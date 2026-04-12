const { config } = require('../config');
const log = require('./logger');
const px = require('./prophetx');
const oddsFeed = require('./odds-feed');
const db = require('./db');
// Lazy require for orderTracker to avoid circular dependency
let orderTracker = null;
function getOrderTracker() {
  if (!orderTracker) orderTracker = require('./order-tracker');
  return orderTracker;
}

// ---------------------------------------------------------------------------
// LINE INDEX — maps PX line_id → metadata + Odds API match
// ---------------------------------------------------------------------------
// { [lineId]: { sport, pxEventId, pxEventName, marketType, selection,
//               teamName, line, homeTeam, awayTeam,
//               oddsApiSport, oddsApiMarket, oddsApiSelection } }
const lineIndex = {};

// Reverse lookup: PX event_id → event metadata
const eventIndex = {};

// Tournament ID → name/sport lookup
const tournamentIndex = {};

// Stats from last seed
let lastSeedStats = null;

// ---------------------------------------------------------------------------
// SPORT-AWARE LINE BOUNDS — reject sub-game/prop totals and spreads
// ---------------------------------------------------------------------------
const TOTAL_BOUNDS_BY_SPORT = {
  'basketball_nba': [180, 300],
  'basketball_ncaab': [100, 200],
  'basketball_wnba': [130, 200],
  'icehockey_nhl': [4, 9],
  'baseball_mlb': [4, 15],
  'soccer': [0.5, 7],
  'soccer_usa_mls': [0.5, 7],
  'soccer_epl': [0.5, 7],
  'soccer_uefa_champs_league': [0.5, 7],
  'soccer_uefa_europa_league': [0.5, 7],
  'soccer_spain_la_liga': [0.5, 7],
  'soccer_italy_serie_a': [0.5, 7],
  'soccer_germany_bundesliga': [0.5, 7],
  'soccer_france_ligue_one': [0.5, 7],
  'soccer_usa_nwsl': [0.5, 7],
  'tennis': [15, 40],
};
// Max plausible alt-spread line per sport. PX bettors commonly play alt
// lines out to ±4 or ±5 for hockey/baseball (e.g. Rangers -3.5 puck line,
// Dodgers -4.5 run line). Previous bounds of 3 for NHL and MLB excluded
// these entirely — ~350 NHL alt-spread RFQs/hour were silently declining
// because the ENTIRE market bundle was rejected whenever its first
// market_line happened to exceed 3. Widened to 5 for those sports.
const MAX_SPREAD_BY_SPORT = {
  'basketball_nba': 30,
  'basketball_ncaab': 40,
  'basketball_wnba': 30,
  'icehockey_nhl': 5,
  'baseball_mlb': 5,
  'soccer': 5,
  'soccer_usa_mls': 5,
  'soccer_epl': 5,
  'soccer_uefa_champs_league': 5,
  'soccer_uefa_europa_league': 5,
  'soccer_spain_la_liga': 5,
  'soccer_italy_serie_a': 5,
  'soccer_germany_bundesliga': 5,
  'soccer_france_ligue_one': 5,
  'soccer_usa_nwsl': 5,
  'tennis': 10,
};

/**
 * Returns true if `line` is within the plausible full-game range for the
 * given sport+markettype. Returns true (accept) for markets/sports without
 * defined bounds. `marketType` = 'total', 'spread', or 'team_total'.
 * F5 markets should be excluded by the caller.
 *
 * team_total lines are always much lower than full-game totals (e.g. MLB
 * team total 3.5-4.5 runs vs full-game 8.5-10) so they get their own
 * lenient bounds. Bypass the check entirely with a permissive range —
 * parseMarketSelections already distinguishes them by name.
 */
function isValidFullGameLine(sport, marketType, line) {
  if (line == null) return true;
  const absLine = Math.abs(line);
  if (marketType === 'team_total') {
    // Team totals are naturally low. Accept 0 to 15 (covers everything from
    // hockey 0.5-goal team totals to NBA 130-point team totals ... wait,
    // NBA team totals are 100+, so widen). Use a very permissive range.
    return absLine >= 0 && absLine <= 200;
  }
  if (marketType === 'total') {
    const bounds = TOTAL_BOUNDS_BY_SPORT[sport];
    if (bounds) return absLine >= bounds[0] && absLine <= bounds[1];
    // Fallback: reject obviously sub-game totals
    return absLine > 2.5;
  }
  if (marketType === 'spread') {
    const max = MAX_SPREAD_BY_SPORT[sport];
    if (max != null) return absLine <= max;
    return true;
  }
  return true;
}

// ---------------------------------------------------------------------------
// TEAM NAME MATCHING
// ---------------------------------------------------------------------------

// Known overrides for team name mismatches between PX and The Odds API
// Add entries here if matching fails for specific teams
const TEAM_NAME_OVERRIDES = {
  // SharpAPI abbreviates some NHL city names
  'washington capitals': 'WAS Capitals',
  'columbus blue jackets': 'CBJ Blue Jackets',
  'montreal canadiens': 'MTL Canadiens',
  'new jersey devils': 'NJ Devils',
  'san jose sharks': 'SJ Sharks',
  'los angeles kings': 'LA Kings',
};

function normalizeTeamName(name) {
  // Strip diacritics first (Godínez → Godinez, São Paulo → Sao Paulo) so
  // ASCII-only feeds can match international/combat-sport names. Without
  // NFD-decomposition + combining-mark removal, every accented fighter name
  // silently drops through the matcher (the í character is not in
  // [a-z0-9 ] so the previous regex would just delete it, corrupting the
  // name to "godnez").
  return (name || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, '')
    .trim();
}

/**
 * Resolve a team_total "hint" (extracted from market name) to home/away side.
 * Team total markets on PX are named things like "SJ: Team Total Goals" or
 * "Philadelphia Phillies Team Total Runs". The prefix before "Team Total" is
 * the team, but it may be an abbreviation, initials, or full name. This
 * function tries several strategies in order of confidence.
 *
 * Returns 'home', 'away', or null.
 */
function resolveTeamTotalSide(hint, homeTeam, awayTeam) {
  if (!hint) return null;
  const normHint = normalizeTeamName(hint);
  if (!normHint) return null;

  // Explicit side labels
  if (normHint === 'home') return 'home';
  if (normHint === 'away') return 'away';

  const normHome = normalizeTeamName(homeTeam);
  const normAway = normalizeTeamName(awayTeam);

  // Exact normalized match
  if (normHint === normHome) return 'home';
  if (normHint === normAway) return 'away';

  // Check TEAM_NAME_OVERRIDES forward and reverse (handles things like
  // "MTL Canadiens" ↔ "Montreal Canadiens").
  for (const [k, v] of Object.entries(TEAM_NAME_OVERRIDES)) {
    const normK = normalizeTeamName(k);
    const normV = normalizeTeamName(v);
    if (normHome === normK || normHome === normV) {
      if (normHint === normK || normHint === normV) return 'home';
      if (normV.includes(normHint) || normK.includes(normHint)) return 'home';
    }
    if (normAway === normK || normAway === normV) {
      if (normHint === normK || normHint === normV) return 'away';
      if (normV.includes(normHint) || normK.includes(normHint)) return 'away';
    }
  }

  // Substring: full team name contains the hint (or vice versa)
  if (normHome.includes(normHint) || normHint.includes(normHome)) return 'home';
  if (normAway.includes(normHint) || normHint.includes(normAway)) return 'away';

  // Abbreviation / initials matching. Sports use three different
  // abbreviation conventions and we need to handle all of them:
  //
  //   1. All-word initials: "SJS" (San Jose Sharks), "CBJ" (Columbus
  //      Blue Jackets), "NYY" (New York Yankees)
  //   2. First-N-word prefix initials: "SJ" (San Jose), "LA" (Los
  //      Angeles), "NY" (New York), "GS" (Golden State)
  //   3. First-N-char chunk: "VAN" (Vancouver), "MON"/"MTL" (Montreal),
  //      "PHI" (Philadelphia)
  function allWordInitials(name) {
    return normalizeTeamName(name).split(/\s+/).map(w => w[0] || '').join('');
  }
  function firstNWordInitials(name, n) {
    return normalizeTeamName(name).split(/\s+/).slice(0, n).map(w => w[0] || '').join('');
  }
  function firstWordChunk(name, n) {
    const norm = normalizeTeamName(name);
    return norm.replace(/\s/g, '').slice(0, n);
  }
  const hintCompact = normHint.replace(/\s/g, '');

  // Strategy 1: all-word initials
  if (hintCompact === allWordInitials(homeTeam)) return 'home';
  if (hintCompact === allWordInitials(awayTeam)) return 'away';

  // Strategy 2: first-N-word prefix initials where N = hint length
  if (hintCompact.length >= 2 && hintCompact.length <= 4) {
    if (firstNWordInitials(homeTeam, hintCompact.length) === hintCompact) return 'home';
    if (firstNWordInitials(awayTeam, hintCompact.length) === hintCompact) return 'away';
  }

  // Strategy 3: first-N-chars of first word
  for (const n of [5, 4, 3]) {
    if (hintCompact.length !== n) continue;
    if (firstWordChunk(homeTeam, n) === hintCompact) return 'home';
    if (firstWordChunk(awayTeam, n) === hintCompact) return 'away';
  }

  // Last-word match (e.g., "Phillies" vs "Philadelphia Phillies")
  const homeLast = normHome.split(/\s+/).pop();
  const awayLast = normAway.split(/\s+/).pop();
  if (normHint === homeLast) return 'home';
  if (normHint === awayLast) return 'away';

  return null;
}

/**
 * Try to match a PX team name to an Odds API team name.
 * Strategies: exact, contains, override map.
 */
function matchTeamName(pxName, oddsApiNames) {
  const norm = normalizeTeamName(pxName);

  // Check override map
  if (TEAM_NAME_OVERRIDES[norm]) {
    const override = TEAM_NAME_OVERRIDES[norm];
    const match = oddsApiNames.find(n => normalizeTeamName(n) === normalizeTeamName(override));
    if (match) return match;
  }

  // Exact normalized match
  const exact = oddsApiNames.find(n => normalizeTeamName(n) === norm);
  if (exact) return exact;

  // Substring: PX name contains Odds API name or vice versa
  for (const oaName of oddsApiNames) {
    const oaNorm = normalizeTeamName(oaName);
    if (norm.includes(oaNorm) || oaNorm.includes(norm)) return oaName;
  }

  // Last N words match (e.g., "Red Sox" matches "Boston Red Sox")
  const pxWords = norm.split(/\s+/);
  // Try last 2 words first (handles "Red Sox" vs "White Sox"), then last 1 word
  for (const n of [2, 1]) {
    if (pxWords.length < n + 1) continue; // Need at least n+1 words (city + name)
    const pxTail = pxWords.slice(-n).join(' ');
    if (pxTail.length < 4) continue;
    const matches = oddsApiNames.filter(name => {
      const words = normalizeTeamName(name).split(/\s+/);
      if (words.length < n) return false;
      return words.slice(-n).join(' ') === pxTail;
    });
    if (matches.length === 1) return matches[0];
  }

  return null;
}

// ---------------------------------------------------------------------------
// MARKET TYPE MAPPING
// ---------------------------------------------------------------------------

// PX market.type → Odds API market key
const MARKET_TYPE_MAP = {
  'moneyline': 'h2h',
  'spread': 'spreads',
  'total': 'totals',
  'team_total': 'team_totals',
  'btts': 'btts',
  'both_teams_to_score': 'btts',
  'double_chance': 'double_chance',
  // First 5 Innings (MLB) — PX market.type guesses; adjust based on decline-audit log
  'first_5_innings_moneyline': 'h2h_f5',
  'first_five_innings_moneyline': 'h2h_f5',
  'first_5_innings_run_line': 'spreads_f5',
  'first_five_innings_run_line': 'spreads_f5',
  'first_5_innings_total': 'totals_f5',
  'first_5_innings_total_runs': 'totals_f5',
  'first_five_innings_total': 'totals_f5',
};

const F5_MARKET_TYPES = [
  'first_5_innings_moneyline',
  'first_five_innings_moneyline',
  'first_5_innings_run_line',
  'first_five_innings_run_line',
  'first_5_innings_total',
  'first_5_innings_total_runs',
  'first_five_innings_total',
];

// ---------------------------------------------------------------------------
// SEEDING
// ---------------------------------------------------------------------------

/**
 * Seed all lines from ProphetX, match to Odds API, register supported lines.
 *
 * Flow:
 * 1. Fetch all PX sport events
 * 2. Filter to supported sports
 * 3. Fetch markets for each event
 * 4. Parse line_ids from markets
 * 5. Match each line to Odds API event/market
 * 6. Register matched lines with PX
 */
async function seedAllLines() {
  log.info('Lines', '=== Starting line seed ===');

  // 1. Fetch PX events
  const allEvents = await px.fetchSportEvents();
  const pxSportNames = Object.values(config.sportNameMap);

  // Build tournament + event index from ALL events (not just supported)
  for (const e of allEvents) {
    if (e.tournament_id) {
      tournamentIndex[e.tournament_id] = {
        name: e.tournament_name || e.tournament?.name || e.sport_name,
        sport: e.sport_name,
      };
    }
    // Store ALL events for name resolution (even ones we don't support)
    if (e.event_id) {
      eventIndex[e.event_id] = eventIndex[e.event_id] || {
        name: e.name,
        sport: null,
        sportName: e.sport_name,
        competitors: e.competitors,
        scheduled: e.scheduled,
      };
    }
  }
  log.info('Lines', `Built indexes: ${Object.keys(tournamentIndex).length} tournaments, ${Object.keys(eventIndex).length} events`);

  // 2. Filter to supported sports (accept any non-settled status)
  const events = allEvents.filter(e =>
    pxSportNames.includes(e.sport_name) &&
    (!e.status || e.status !== 'settled') &&
    e.competitors && e.competitors.length >= 2
  );
  log.info('Lines', `Found ${events.length} supported sport events (of ${allEvents.length} total)`);

  // Get all Odds API cached events for matching
  const oddsApiEvents = oddsFeed.getAllCachedEvents();

  let totalLines = 0;
  let matchedLines = 0;
  let unmatchedEvents = [];

  // 3-4. Fetch markets and parse for each event
  for (const event of events) {
    // Determine sport key(s) — some PX sport names map to multiple keys
    // (e.g., "Basketball" → basketball_nba AND basketball_ncaab)
    const possibleSportKeys = Object.entries(config.sportNameMap)
      .filter(([k, v]) => v === event.sport_name)
      .map(([k]) => k);
    if (possibleSportKeys.length === 0) continue;

    // We'll determine the actual sport key by which one has a matching Odds API event
    let sportKey = possibleSportKeys[0]; // default to first match

    // Store event metadata
    eventIndex[event.event_id] = {
      name: event.name,
      sport: sportKey,
      sportName: event.sport_name,
      competitors: event.competitors,
      scheduled: event.scheduled,
    };

    // Extract home/away from PX event
    // Tennis/soccer may use different side labels or just have 2 competitors without home/away
    let homeComp = event.competitors.find(c => c.side === 'home');
    let awayComp = event.competitors.find(c => c.side === 'away');
    // Fallback: use first two competitors if no home/away labels
    if (!homeComp && !awayComp && event.competitors.length >= 2) {
      homeComp = event.competitors[0];
      awayComp = event.competitors[1];
    }
    if (!homeComp || !awayComp) {
      log.debug('Lines', `Skipping ${event.name}: missing competitors`);
      continue;
    }

    // 5. Try to match to Odds API event — try all possible sport keys
    let matchedHome = null, matchedAway = null, matchedOddsEvent = null;

    for (const tryKey of possibleSportKeys) {
      const allOddsTeams = oddsApiEvents
        .filter(e => e.sport === tryKey)
        .flatMap(e => [e.homeTeam, e.awayTeam]);
      const uniqueTeams = [...new Set(allOddsTeams)];

      const tryHome = matchTeamName(homeComp.name, uniqueTeams);
      const tryAway = matchTeamName(awayComp.name, uniqueTeams);

      if (tryHome && tryAway) {
        // Verify this pair exists — use scheduled time for back-to-back/doubleheader matching
        const pxTime = event.scheduled || null;
        const oddsEvt = oddsFeed.getEventMarkets(tryKey, tryHome, tryAway, pxTime)
          || oddsFeed.getEventMarkets(tryKey, tryAway, tryHome, pxTime);
        if (oddsEvt) {
          matchedHome = tryHome;
          matchedAway = tryAway;
          matchedOddsEvent = oddsEvt;
          sportKey = tryKey; // Use the sport key that matched
          break;
        }
      }
    }

    // Second pass: try SharpAPI /events index (broader team name coverage)
    if (!matchedHome || !matchedAway) {
      for (const tryKey of possibleSportKeys) {
        const sharpEvents = oddsFeed.getSharpEvents(tryKey);
        if (!sharpEvents || sharpEvents.length === 0) continue;
        const sharpTeams = [...new Set(sharpEvents.flatMap(e => [e.homeTeam, e.awayTeam]))];
        const tryHome = matchTeamName(homeComp.name, sharpTeams);
        const tryAway = matchTeamName(awayComp.name, sharpTeams);
        if (tryHome && tryAway) {
          // Look up odds using SharpAPI's canonical team names
          const pxTime = event.scheduled || null;
          const oddsEvt = oddsFeed.getEventMarkets(tryKey, tryHome, tryAway, pxTime)
            || oddsFeed.getEventMarkets(tryKey, tryAway, tryHome, pxTime);
          if (oddsEvt) {
            matchedHome = tryHome;
            matchedAway = tryAway;
            matchedOddsEvent = oddsEvt;
            sportKey = tryKey;
            log.debug('Lines', `Matched via events index: ${homeComp.name} → ${tryHome}, ${awayComp.name} → ${tryAway}`);
            break;
          }
        }
      }
    }

    if (!matchedHome || !matchedAway) {
      unmatchedEvents.push({
        pxEvent: event.name,
        pxHome: homeComp.name,
        pxAway: awayComp.name,
      });
      continue;
    }

    // Verify this home/away pair exists as an actual Odds API event
    const pxScheduled = event.scheduled || null;
    const oddsEvent = matchedOddsEvent || oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway, pxScheduled);
    if (!oddsEvent) {
      const oddsEventReversed = oddsFeed.getEventMarkets(sportKey, matchedAway, matchedHome, pxScheduled);
      if (!oddsEventReversed) {
        unmatchedEvents.push({
          pxEvent: event.name,
          reason: 'Team names matched but no Odds API event found',
          matchedHome,
          matchedAway,
        });
        continue;
      }
      // Swap for correct orientation
      const temp = matchedHome;
      // Note: we'll handle the swap in line indexing below
    }

    // Fetch PX markets
    let markets;
    try {
      markets = await px.fetchMarkets(event.event_id);
    } catch (err) {
      log.error('Lines', `Failed to fetch markets for ${event.name}: ${err.message}`);
      continue;
    }

    // Filter to FULL-GAME main markets only.
    // Exclude: first half, first quarter, period, inning, player props
    const excludePatterns = /first half|1st half|first quarter|1st quarter|2nd half|2nd quarter|3rd quarter|4th quarter|1st period|2nd period|3rd period|1st inning|overtime|player|total hits|total strikeout|total earned|total block|total point[^s]|total rebound|total assist|total steal|total made|total rush|total recei|total passing/i;

    const fullGameNames = {
      moneyline: ['Moneyline', 'Moneyline (2 Way)', 'Moneyline (2-Way)', 'Moneyline (Regulation)', 'Draw No Bet'],
      spread: ['Spread', 'Run Line', 'Puck Line', 'Spread (Regular Time)', 'Game Spread', 'Point Spread'],
      total: ['Total', 'Total Points', 'Points', 'Total Runs', 'Total Goals', 'Total Goals (Regular Time)'],
      team_total: ['Team Total', 'Team Total Points', 'Team Total Runs', 'Team Total Goals', 'Home Total', 'Away Total'],
    };

    // F5 markets (PX uses market.type === 'moneyline'/'spread'/'total' but
    // distinguishes via market.name). Allow these through the filter.
    const f5NamePattern = /1st[-\s]?5th.*inning|first\s*5\s*inning|first\s*five\s*innings/i;

    // Combat sports (MMA, Boxing) only have moneyline in our odds feed —
    // SharpAPI/TheOddsAPI don't publish Total Rounds lines. Restrict to
    // moneyline so we don't register a flood of "Total Rounds" lines that
    // would always decline at price time with 'no odds data'.
    const isCombatSport = sportKey === 'mma_mixed_martial_arts' || sportKey === 'boxing_boxing';

    const mainMarkets = markets.filter(m => {
      const supportedBase = isCombatSport
        ? ['moneyline']
        : ['moneyline', 'spread', 'total', 'team_total', 'btts', 'both_teams_to_score', 'double_chance'];
      if (!supportedBase.includes(m.type) && !F5_MARKET_TYPES.includes(m.type)) return false;
      // Exclude anything matching half/quarter/prop patterns
      if (excludePatterns.test(m.name)) return false;
      // Allow F5 markets by name pattern
      const isF5 = f5NamePattern.test(m.name || '');
      // Name filter: previously required EXACT match against a fixed whitelist
      // which rejected alt-line markets like "Alternate Spread +3.5" — costing
      // us thousands of unknown-leg declines per day. Relaxed to substring
      // match: the market name must CONTAIN one of the canonical full-game
      // names (e.g. "Alternate Spread" contains "Spread"). Player props still
      // fail because their names don't contain "Spread", "Moneyline", etc.
      // Additional safety comes from excludePatterns (above) and sport-aware
      // line bounds (below).
      if (!isF5) {
        const allowed = fullGameNames[m.type];
        if (allowed) {
          const nameL = (m.name || '').toLowerCase();
          const matches = allowed.some(a => nameL.includes(a.toLowerCase()));
          if (!matches) return false;
        }
      }
      // Exclude sub-game totals/spreads and prop markets via sport-aware bounds.
      // F5 markets bypass (MLB F5 totals are ~4-5, spreads ~1.5).
      //
      // IMPORTANT: PX bundles alt lines inside market.market_lines, so a
      // single spread market can contain lines from ±0.5 to ±6.5. Previously
      // this check read parsed[0].line and rejected the entire market if
      // THAT one happened to be out of bounds — silently losing all the
      // reasonable alt lines bundled in the same market. Fixed: accept the
      // market if ANY selection has a line inside the sport's bounds. The
      // individual selection-level bound check (below, inside the
      // registration loop) filters out the out-of-range alt lines one-by-one.
      //
      // Use parsed sel.marketType (not raw m.type) so team_total markets
      // (which PX types as 'total' but parser upgrades to 'team_total') get
      // the correct permissive bounds.
      if ((m.type === 'total' || m.type === 'spread') && !isF5) {
        const parsed = px.parseMarketSelections(m);
        if (parsed.length === 0) return false;
        const anyInBounds = parsed.some(p => isValidFullGameLine(sportKey, p.marketType || m.type, p.line));
        if (!anyInBounds) {
          log.debug('Lines', `Rejecting ${m.type} market (no lines in bounds) for ${sportKey}: ${m.name}`);
          return false;
        }
      }
      return true;
    });

    for (const market of mainMarkets) {
      // Detect PX 3-way moneyline sub-markets like "Arsenal Football Club To Win
      // (90 Min)" which are yes/no propositions on a 3-way outcome. We don't
      // currently support quoting these — skip them entirely so they don't leak
      // into the moneyline path where they'd be mispriced as 2-way team bets.
      // The regular 2-way market is "Moneyline (2 Way)" with team selections.
      if (market.type === 'moneyline' && /\bto win\b.*\(.*min.*\)|^draw\s*\(.*min.*\)/i.test(market.name || '')) {
        log.debug('Lines', `Skipping PX 3-way sub-market at seed: ${market.name}`);
        continue;
      }
      const parsed = px.parseMarketSelections(market);
      // Detect 2-way / Draw No Bet soccer moneylines.
      // PX labels the 2-way soccer ML market as "Moneyline (2 Way)".
      // Also catch explicit "Draw No Bet" / "DNB" / "Moneyline 2W" variants.
      const isDNB = market.type === 'moneyline' && /\b2\s*[\s\-_]?way\b|draw\s*no\s*bet|\bdnb\b|\b2w\b/i.test(market.name || '');

      for (const sel of parsed) {
        totalLines++;

        // Per-selection bounds check for spread/total/team_total alt lines.
        // The market-level filter above accepts the market if ANY selection
        // is in bounds; this check rejects the individual out-of-range ones
        // (e.g. Rangers -6.5 puck line) while keeping sibling in-range
        // alts registered. team_total uses permissive bounds (see
        // isValidFullGameLine) since their lines are naturally low.
        const selMarketType = ['spread', 'total', 'team_total'].includes(sel.marketType) ? sel.marketType : null;
        if (selMarketType && !isValidFullGameLine(sportKey, selMarketType, sel.line)) {
          continue;
        }

        // Determine Odds API selection mapping
        let oddsApiSelection = null;
        let oddsApiMarket = MARKET_TYPE_MAP[sel.marketType];

        if (sel.marketType === 'moneyline') {
          // Match team to home/away
          if (matchTeamName(sel.teamName, [matchedHome])) {
            oddsApiSelection = 'home';
          } else if (matchTeamName(sel.teamName, [matchedAway])) {
            oddsApiSelection = 'away';
          }
        } else if (sel.marketType === 'spread') {
          // Match by team name against home and away explicitly. Do NOT fall
          // back to guessing — previously a loose substring check on the last
          // word of home team name would misclassify e.g. "AS Monaco FC" as
          // home of "Paris FC" (because both contain 'FC'), causing us to
          // price the WRONG team's spread. If neither side matches, leave
          // selection null so the leg is rejected as unresolvable.
          if (matchTeamName(sel.teamName, [matchedHome])) {
            oddsApiSelection = 'home';
          } else if (matchTeamName(sel.teamName, [matchedAway])) {
            oddsApiSelection = 'away';
          }
        } else if (sel.marketType === 'total') {
          oddsApiSelection = sel.selection; // 'over' or 'under'
        } else if (sel.marketType === 'team_total') {
          // Determine home/away from team hint extracted from market name.
          // parseMarketSelections populates sel.teamName with the parsed
          // prefix (e.g. "SJ" from "SJ: Team Total Goals") — not the
          // selection's over/under text. resolveTeamTotalSide handles
          // exact, substring, initials, and first-N-char matching.
          const teamSide = resolveTeamTotalSide(sel.teamName, matchedHome, matchedAway);
          if (!teamSide) continue; // Skip if we can't determine the side
          oddsApiSelection = teamSide + '_' + (sel.selection || 'over'); // "home_over", "away_under", etc.
        } else if (sel.marketType === 'btts' || sel.marketType === 'both_teams_to_score') {
          // Yes/No selection from parseMarketSelections
          oddsApiSelection = (sel.selection || '').toLowerCase();
        } else if (sel.marketType === 'double_chance') {
          // '1X', 'X2', or '12' selection
          oddsApiSelection = sel.selection;
        } else if (F5_MARKET_TYPES.includes(sel.marketType)) {
          // First 5 Innings — same selection logic as full-game for h2h/spreads/totals
          if (sel.marketType.includes('moneyline')) {
            if (matchTeamName(sel.teamName, [matchedHome])) oddsApiSelection = 'home';
            else if (matchTeamName(sel.teamName, [matchedAway])) oddsApiSelection = 'away';
          } else if (sel.marketType.includes('run_line')) {
            // Explicit home/away match only — no substring fallback.
            if (matchTeamName(sel.teamName, [matchedHome])) {
              oddsApiSelection = 'home';
            } else if (matchTeamName(sel.teamName, [matchedAway])) {
              oddsApiSelection = 'away';
            }
          } else if (sel.marketType.includes('total')) {
            oddsApiSelection = sel.selection; // 'over' or 'under'
          }
        }

        if (!oddsApiSelection || !oddsApiMarket) continue;

        // Register line — fair value check happens at RFQ pricing time
        // For moneylines, verify fair value exists now
        // For spreads/totals, register all alternate lines (fair value available for primary line)
        matchedLines++;
        // Get event start time — PX's event.scheduled is authoritative (PX owns
        // the game clock for the games we're quoting on). SharpAPI's
        // event_start_time is unreliable for games that haven't loaded yet —
        // it defaults to midnight UTC which is 8pm ET the PREVIOUS day, causing
        // false "event started" declines all day until SharpAPI loads real
        // tip-off times. Fall back to odds cache only if PX has no scheduled.
        const oddsEvt = oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway, pxScheduled);
        const startTime = event.scheduled || oddsEvt?.commenceTime || null;

        lineIndex[sel.lineId] = {
          sport: sportKey,
          pxEventId: event.event_id,
          pxEventName: event.name,
          marketType: sel.marketType,
          marketName: market.name,
          isDNB,
          selection: oddsApiSelection,
          teamName: sel.teamName,
          line: sel.line,
          homeTeam: matchedHome,
          awayTeam: matchedAway,
          oddsApiSport: sportKey,
          oddsApiMarket,
          oddsApiSelection,
          competitorId: sel.competitorId,
          startTime,
          // Golf-specific metadata from DataGolf (tournament name, round).
          // Undefined for non-golf sports — harmless to always copy.
          tournamentName: oddsEvt?.eventName || null,
          roundNum: oddsEvt?.roundNum || null,
        };
      }
    }

    // Small delay to avoid hammering PX API
    await new Promise(r => setTimeout(r, 100));
  }

  // Log unmatched events
  if (unmatchedEvents.length > 0) {
    log.warn('Lines', `${unmatchedEvents.length} events could not be matched to Odds API:`);
    for (const ue of unmatchedEvents) {
      log.warn('Lines', `  ${ue.pxEvent}: ${ue.reason || `home=${ue.pxHome}→${ue.matchedHome || 'NO MATCH'}, away=${ue.pxAway}→${ue.matchedAway || 'NO MATCH'}`}`);
    }
  }

  // 6. Register matched lines with PX
  const lineIds = Object.keys(lineIndex);
  if (lineIds.length > 0) {
    try {
      await px.registerSupportedLines(lineIds);
      log.info('Lines', `Registered ${lineIds.length} lines with ProphetX`);
    } catch (err) {
      log.error('Lines', `Failed to register lines: ${err.message}`);
    }
  }

  lastSeedStats = {
    timestamp: new Date().toISOString(),
    totalEvents: events.length,
    totalLines,
    matchedLines,
    registeredLines: lineIds.length,
    unmatchedEvents: unmatchedEvents.length,
  };

  log.info('Lines', `=== Seed complete: ${events.length} events, ${totalLines} lines parsed, ${matchedLines} matched, ${lineIds.length} registered ===`);

  // Persist lineIndex to Supabase so historical line_ids survive restarts
  db.saveLineCache(lineIndex).catch(err => {
    log.warn('Lines', `saveLineCache failed: ${err.message}`);
  });

  return lastSeedStats;
}

// ---------------------------------------------------------------------------
// LOOKUPS
// ---------------------------------------------------------------------------

function __debugGetLineIndex() {
  return lineIndex;
}

function lookupLine(lineId) {
  return lineIndex[lineId] || null;
}

/**
 * Async lookupLine that falls back to the persistent Supabase cache
 * when the in-memory lineIndex doesn't have the lineId.
 * Use this for enrichment paths that can await.
 */
async function lookupLineAsync(lineId) {
  if (lineIndex[lineId]) return lineIndex[lineId];
  // Fall back to persistent cache
  const cached = await db.loadLineCacheEntry(lineId);
  if (cached) {
    // Populate in-memory index so subsequent sync lookups hit
    lineIndex[lineId] = cached;
    log.debug('Lines', `lookupLineAsync: resolved ${lineId} from Supabase cache → ${cached.teamName}`);
  }
  return cached;
}

// Track in-flight resolution attempts to avoid duplicate work / rate limiting
const inFlightResolutions = new Map(); // lineId -> Promise
// Cache events we've already fetched markets for to avoid re-fetching
const resolvedEventMarkets = new Map(); // eventId -> { time, markets }
const RESOLVED_MARKET_TTL_MS = 60 * 1000; // 60s

/**
 * On-demand registration: when an RFQ references a line we don't know,
 * attempt to fetch the event's markets from PX, locate the line, build
 * its metadata, and register it. Returns the line info or null on failure.
 *
 * This enables alt-line quoting without pre-registering every possible
 * spread/total during startup seeding.
 */
async function resolveUnknownLine(rfqLeg) {
  const lineId = rfqLeg.line_id || rfqLeg.lineId || rfqLeg;
  if (!lineId) return null;
  if (lineIndex[lineId]) return lineIndex[lineId]; // already resolved

  // Reuse in-flight resolution
  if (inFlightResolutions.has(lineId)) {
    return inFlightResolutions.get(lineId);
  }

  const eventId = rfqLeg.sport_event_id;
  if (!eventId) {
    log.debug('Lines', `Cannot resolve ${lineId}: no sport_event_id in RFQ leg`);
    resolveUnknownLine._lastFailure = { lineId, reason: 'no_event_id' };
    return null;
  }

  const event = eventIndex[eventId];
  if (!event) {
    log.debug('Lines', `Cannot resolve ${lineId}: unknown event ${eventId}`);
    resolveUnknownLine._lastFailure = { lineId, reason: 'unknown_event', eventId };
    return null;
  }

  // Determine sport key
  const possibleSportKeys = Object.entries(config.sportNameMap)
    .filter(([k, v]) => v === event.sportName)
    .map(([k]) => k);
  if (possibleSportKeys.length === 0) return null;

  // Identify home/away teams (reuse same logic as seed)
  let homeComp = (event.competitors || []).find(c => c.side === 'home');
  let awayComp = (event.competitors || []).find(c => c.side === 'away');
  if ((!homeComp || !awayComp) && (event.competitors || []).length >= 2) {
    homeComp = event.competitors[0];
    awayComp = event.competitors[1];
  }
  if (!homeComp || !awayComp) return null;

  // Try to match teams to odds feed for one of the possible sport keys
  const oddsApiEvents = oddsFeed.getAllCachedEvents();
  let matchedHome = null, matchedAway = null, sportKey = possibleSportKeys[0];
  for (const tryKey of possibleSportKeys) {
    const uniqueTeams = [...new Set(oddsApiEvents.filter(e => e.sport === tryKey).flatMap(e => [e.homeTeam, e.awayTeam]))];
    const tryHome = matchTeamName(homeComp.name, uniqueTeams);
    const tryAway = matchTeamName(awayComp.name, uniqueTeams);
    if (tryHome && tryAway) {
      const pxTime = event.scheduled || null;
      const oddsEvt = oddsFeed.getEventMarkets(tryKey, tryHome, tryAway, pxTime) || oddsFeed.getEventMarkets(tryKey, tryAway, tryHome, pxTime);
      if (oddsEvt) {
        matchedHome = tryHome;
        matchedAway = tryAway;
        sportKey = tryKey;
        break;
      }
    }
  }

  // Second pass: try SharpAPI /events index (broader team name coverage)
  // Mirrors the seed-time fallback for events with non-standard team names.
  if (!matchedHome || !matchedAway) {
    for (const tryKey of possibleSportKeys) {
      const sharpEvents = oddsFeed.getSharpEvents(tryKey);
      if (!sharpEvents || sharpEvents.length === 0) continue;
      const sharpTeams = [...new Set(sharpEvents.flatMap(e => [e.homeTeam, e.awayTeam]))];
      const tryHome = matchTeamName(homeComp.name, sharpTeams);
      const tryAway = matchTeamName(awayComp.name, sharpTeams);
      if (tryHome && tryAway) {
        const pxTime = event.scheduled || null;
        const oddsEvt = oddsFeed.getEventMarkets(tryKey, tryHome, tryAway, pxTime)
          || oddsFeed.getEventMarkets(tryKey, tryAway, tryHome, pxTime);
        if (oddsEvt) {
          matchedHome = tryHome;
          matchedAway = tryAway;
          sportKey = tryKey;
          log.debug('Lines', `On-demand matched via events index: ${homeComp.name} → ${tryHome}, ${awayComp.name} → ${tryAway}`);
          break;
        }
      }
    }
  }
  if (!matchedHome || !matchedAway) {
    // Log what we tried to match for debugging
    const sportKeys = possibleSportKeys.join(',');
    const pxHome = homeComp?.name || '?';
    const pxAway = awayComp?.name || '?';
    const oddsApiEvents = oddsFeed.getAllCachedEvents();
    const sportsAvail = possibleSportKeys.map(k => {
      const evts = oddsApiEvents.filter(e => e.sport === k);
      return k + ':' + evts.length;
    }).join(', ');
    log.info('Lines', `Cannot resolve ${lineId}: no odds feed match for "${event.name}" (PX: ${pxHome} vs ${pxAway}, sports: [${sportsAvail}], keys: ${sportKeys})`);
    resolveUnknownLine._lastFailure = { lineId, reason: 'no_odds_match', eventName: event.name, sport: event.sport || event.sportName, pxHome, pxAway, sportKeys, sportsAvail };
    return null;
  }

  const promise = (async () => {
    try {
      // Fetch markets for this event (cached briefly to avoid re-fetching on chains of RFQs)
      const now = Date.now();
      let cached = resolvedEventMarkets.get(eventId);
      let markets;
      if (cached && now - cached.time < RESOLVED_MARKET_TTL_MS) {
        markets = cached.markets;
      } else {
        markets = await px.fetchMarkets(eventId);
        resolvedEventMarkets.set(eventId, { time: now, markets });
      }

      // First pass: find which market contains this line_id (any type)
      // so we can log unsupported market types for diagnostics.
      const SUPPORTED_TYPES = ['moneyline', 'spread', 'total', 'team_total', 'btts', 'both_teams_to_score', 'double_chance', ...F5_MARKET_TYPES];
      let unsupportedMarketInfo = null;
      for (const market of markets || []) {
        if (SUPPORTED_TYPES.includes(market.type)) continue;
        // Walk the market structure generically to find the line_id
        const selections = [];
        if (market.selections) {
          for (const sg of market.selections) for (const s of sg) if (s.line_id) selections.push(s);
        }
        if (market.market_lines) {
          for (const ml of market.market_lines) {
            for (const sg of (ml.selections || [])) for (const s of sg) if (s.line_id) selections.push(s);
          }
        }
        if (selections.some(s => s.line_id === lineId)) {
          unsupportedMarketInfo = {
            marketType: market.type,
            marketName: market.name,
            eventName: event.name,
            sport: sportKey,
          };
          break;
        }
      }
      if (unsupportedMarketInfo) {
        log.info('Lines', `Unsupported market type: ${unsupportedMarketInfo.marketType} / "${unsupportedMarketInfo.marketName}" (${unsupportedMarketInfo.sport}, ${unsupportedMarketInfo.eventName})`);
        getOrderTracker().recordUnsupportedMarket(unsupportedMarketInfo);
        resolveUnknownLine._lastFailure = { lineId, reason: 'unsupported_market_type', ...unsupportedMarketInfo };
        return null;
      }

      // Find the line in the markets
      let foundInfo = null;
      // F5 name pattern — detect F5 markets by name since PX uses
      // market.type='spread'/'total' for them (distinguishes only via name)
      const f5NamePat = /1st[-\s]?5th.*inning|first\s*5\s*inning|first\s*five\s*innings|f5\b/i;
      // Sub-game name pattern — halves, quarters, periods, innings.
      // These markets come through with supported types (spread/total/moneyline)
      // but the market.name identifies them as sub-game. We must NOT register
      // them as full-game markets because their lines can coincidentally match
      // full-game primaries (e.g. NBA 1st-half spread 5.5 vs full-game 5.5),
      // leading to mispriced offers. Mirror the seed-time excludePatterns.
      // F5 is exempt (handled via its own marketType above).
      const subGameNamePat = /first half|1st half|second half|2nd half|first quarter|1st quarter|2nd quarter|3rd quarter|4th quarter|1st period|2nd period|3rd period|1st inning|2nd inning|3rd inning|overtime/i;
      for (const market of markets || []) {
        if (!SUPPORTED_TYPES.includes(market.type)) continue;
        // Reject sub-game markets (halves/quarters/periods) by name BEFORE
        // the bounds check. F5 is exempt because it has its own marketType.
        const isF5ByName = f5NamePat.test(market.name || '');
        if (!isF5ByName && subGameNamePat.test(market.name || '')) {
          // Check if the lineId is actually in this market — if so, we've
          // identified the request as a sub-game bet and should decline
          // cleanly with a specific reason.
          const parsedSub = px.parseMarketSelections(market);
          if (parsedSub.some(s => s.lineId === lineId)) {
            log.info('Lines', `Declined sub-game market: ${market.type} / "${market.name}" (${event.name})`);
            resolveUnknownLine._lastFailure = {
              lineId,
              reason: 'sub_game_market',
              marketType: market.type,
              marketName: market.name,
              sport: sportKey,
              eventName: event.name,
            };
            return null;
          }
          // Otherwise skip this market and keep searching
          continue;
        }
        // Reject sub-game/prop totals and spreads by sport-aware bounds.
        // F5 markets must be exempt — detect by NAME, not type, because PX
        // uses type='spread' / 'total' for F5 (only name distinguishes).
        // IMPORTANT: Previously we checked parsed[0].line and rejected the
        // ENTIRE market if that one line was out of bounds. PX bundles alt
        // lines inside market_lines, so a single spread market can contain
        // lines from -6.5 to +6.5. The first-line check would silently drop
        // the whole bundle — including the in-range alt we were trying to
        // resolve — causing thousands of spurious "line_not_in_markets"
        // failures per day. Fixed: defer the bound check to the specific
        // selection whose lineId matches. The matching branch below checks
        // sel.line against the sport bounds and rejects only that one.
        const isF5Market = f5NamePat.test(market.name || '');
        const parsed = px.parseMarketSelections(market);
        for (const sel of parsed) {
          if (sel.lineId !== lineId) continue;
          // Per-selection bound check: rejects the specific out-of-range
          // alt line the RFQ asked about (e.g. Rangers -6.5) while leaving
          // siblings like Rangers -1.5 intact for future resolves.
          if ((sel.marketType === 'total' || sel.marketType === 'spread') && !isF5Market) {
            if (!isValidFullGameLine(sportKey, sel.marketType, sel.line)) {
              log.debug('Lines', `resolveUnknownLine: rejecting out-of-bounds selection ${sel.marketType} ${sel.line} for ${sportKey}: ${market.name}`);
              resolveUnknownLine._lastFailure = { lineId, reason: 'out_of_bounds_line', sport: sportKey, marketType: sel.marketType, line: sel.line, marketName: market.name };
              continue;
            }
          }
          // Determine oddsApiSelection
          let oddsApiSelection = null;
          const oddsApiMarket = MARKET_TYPE_MAP[sel.marketType];
          if (sel.marketType === 'moneyline') {
            if (matchTeamName(sel.teamName, [matchedHome])) oddsApiSelection = 'home';
            else if (matchTeamName(sel.teamName, [matchedAway])) oddsApiSelection = 'away';
          } else if (sel.marketType === 'spread') {
            // Explicit home/away match only — no substring fallback (see seed path).
            if (matchTeamName(sel.teamName, [matchedHome])) {
              oddsApiSelection = 'home';
            } else if (matchTeamName(sel.teamName, [matchedAway])) {
              oddsApiSelection = 'away';
            }
          } else if (sel.marketType === 'total') {
            oddsApiSelection = sel.selection;
          } else if (sel.marketType === 'team_total') {
            const teamSide = resolveTeamTotalSide(sel.teamName, matchedHome, matchedAway);
            if (!teamSide) continue;
            oddsApiSelection = teamSide + '_' + (sel.selection || 'over');
          } else if (sel.marketType === 'btts' || sel.marketType === 'both_teams_to_score') {
            oddsApiSelection = (sel.selection || '').toLowerCase();
          } else if (sel.marketType === 'double_chance') {
            oddsApiSelection = sel.selection;
          } else if (F5_MARKET_TYPES.includes(sel.marketType)) {
            if (sel.marketType.includes('moneyline')) {
              if (matchTeamName(sel.teamName, [matchedHome])) oddsApiSelection = 'home';
              else if (matchTeamName(sel.teamName, [matchedAway])) oddsApiSelection = 'away';
            } else if (sel.marketType.includes('run_line')) {
              // Explicit home/away match only — no substring fallback.
              if (matchTeamName(sel.teamName, [matchedHome])) {
                oddsApiSelection = 'home';
              } else if (matchTeamName(sel.teamName, [matchedAway])) {
                oddsApiSelection = 'away';
              }
            } else if (sel.marketType.includes('total')) {
              oddsApiSelection = sel.selection;
            }
          }
          if (!oddsApiSelection || !oddsApiMarket) continue;

          const pxTime = event.scheduled || null;
          const oddsEvt = oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway, pxTime);
          // PX scheduled is authoritative; odds cache is unreliable (midnight UTC placeholder).
          const startTime = event.scheduled || oddsEvt?.commenceTime || null;

          // Skip PX 3-way sub-markets ("Arsenal To Win (90 Min)") — we don't
          // support them and the parser would fail to map Yes/No to home/away.
          if (market.type === 'moneyline' && /\bto win\b.*\(.*min.*\)|^draw\s*\(.*min.*\)/i.test(market.name || '')) {
            log.info('Lines', `Skipping PX 3-way sub-market on-demand: ${market.name}`);
            resolveUnknownLine._lastFailure = {
              lineId, reason: 'unsupported_market_type',
              marketType: market.type, marketName: market.name,
              sport: sportKey, eventName: event.name,
            };
            continue;
          }
          const onDemandDNB = market.type === 'moneyline' && /\b2\s*[\s\-_]?way\b|draw\s*no\s*bet|\bdnb\b|\b2w\b/i.test(market.name || '');
          foundInfo = {
            sport: sportKey,
            pxEventId: eventId,
            pxEventName: event.name,
            marketType: sel.marketType,
            marketName: market.name,
            isDNB: onDemandDNB,
            selection: oddsApiSelection,
            teamName: sel.teamName,
            line: sel.line,
            homeTeam: matchedHome,
            awayTeam: matchedAway,
            oddsApiSport: sportKey,
            oddsApiMarket,
            oddsApiSelection,
            competitorId: sel.competitorId,
            startTime,
            onDemand: true,
            // Golf-specific metadata from DataGolf cache
            tournamentName: oddsEvt?.eventName || null,
            roundNum: oddsEvt?.roundNum || null,
          };
          break;
        }
        if (foundInfo) break;
      }

      if (!foundInfo) {
        // Log what market types we DID find for this event (helps diagnose player props etc.)
        const foundTypes = (markets || []).map(m => m.type).filter(Boolean);
        const marketNames = (markets || []).map(m => m.name).filter(Boolean).slice(0, 5);
        log.debug('Lines', `Could not locate line ${lineId} in event ${eventId} markets (types found: ${foundTypes.join(',')}; names: ${marketNames.join(', ')})`);
        resolveUnknownLine._lastFailure = { lineId, reason: 'line_not_in_markets', eventName: event.name, sport: sportKey, marketTypesFound: foundTypes, marketNamesFound: marketNames };
        return null;
      }

      // Add to index locally
      lineIndex[lineId] = foundInfo;
      log.info('Lines', `On-demand registered ${sportKey}/${foundInfo.marketType} line for ${foundInfo.teamName} ${foundInfo.line != null ? foundInfo.line : ''} (${event.name})`);

      // Fire-and-forget PX registration — the RFQ we're responding to already
      // has the line_id, so we don't need to wait for PX to acknowledge
      px.registerSupportedLines([lineId]).catch(err => {
        log.warn('Lines', `PX registration of ${lineId} failed: ${err.message}`);
      });

      return foundInfo;
    } finally {
      inFlightResolutions.delete(lineId);
    }
  })();

  inFlightResolutions.set(lineId, promise);
  return promise;
}

function getRegisteredLineIds() {
  return Object.keys(lineIndex);
}

function getStats() {
  return lastSeedStats;
}

function getLineCount() {
  return Object.keys(lineIndex).length;
}

/**
 * Get a summary of lines by sport and market type.
 */
function getLineSummary() {
  const summary = {};
  for (const [lineId, info] of Object.entries(lineIndex)) {
    const key = `${info.sport}/${info.marketType}`;
    summary[key] = (summary[key] || 0) + 1;
  }
  return summary;
}

// ---------------------------------------------------------------------------
// REFRESH
// ---------------------------------------------------------------------------

/**
 * Diagnostic: trace golf matchup matching step by step.
 * Returns a report showing exactly where each PX golf event matches or fails.
 */
async function debugGolfMatching() {
  const allEvents = await px.fetchSportEvents();
  const golfEvents = allEvents.filter(e =>
    e.sport_name === 'Golf' &&
    e.competitors && e.competitors.length >= 2 &&
    (!e.status || e.status !== 'settled')
  );

  const oddsApiEvents = oddsFeed.getAllCachedEvents();
  const golfOddsEvents = oddsApiEvents.filter(e => e.sport === 'golf_matchups');

  const possibleSportKeys = Object.entries(config.sportNameMap)
    .filter(([k, v]) => v === 'Golf')
    .map(([k]) => k);

  const report = {
    pxGolfEventsTotal: golfEvents.length,
    dataGolfEventsInCache: golfOddsEvents.length,
    possibleSportKeys,
    uniqueDataGolfPlayers: [...new Set(golfOddsEvents.flatMap(e => [e.homeTeam, e.awayTeam]))].sort(),
    eventResults: [],
  };

  for (const event of golfEvents.slice(0, 10)) {
    let homeComp = event.competitors.find(c => c.side === 'home');
    let awayComp = event.competitors.find(c => c.side === 'away');
    if (!homeComp && !awayComp && event.competitors.length >= 2) {
      homeComp = event.competitors[0];
      awayComp = event.competitors[1];
    }

    const result = {
      pxEvent: event.name,
      pxHome: homeComp?.name,
      pxAway: awayComp?.name,
      scheduled: event.scheduled,
      steps: {},
    };

    for (const tryKey of possibleSportKeys) {
      const allOddsTeams = oddsApiEvents
        .filter(e => e.sport === tryKey)
        .flatMap(e => [e.homeTeam, e.awayTeam]);
      const uniqueTeams = [...new Set(allOddsTeams)];

      result.steps[tryKey] = {
        oddsTeamCount: uniqueTeams.length,
        homeMatch: matchTeamName(homeComp?.name, uniqueTeams),
        awayMatch: matchTeamName(awayComp?.name, uniqueTeams),
      };

      const tryHome = result.steps[tryKey].homeMatch;
      const tryAway = result.steps[tryKey].awayMatch;

      if (tryHome && tryAway) {
        const pxTime = event.scheduled || null;
        const oddsEvt = oddsFeed.getEventMarkets(tryKey, tryHome, tryAway, pxTime);
        const oddsEvtRev = oddsFeed.getEventMarkets(tryKey, tryAway, tryHome, pxTime);
        result.steps[tryKey].getEventMarkets = {
          forward: oddsEvt ? { homeTeam: oddsEvt.homeTeam, awayTeam: oddsEvt.awayTeam, markets: Object.keys(oddsEvt.markets || {}) } : null,
          reverse: oddsEvtRev ? { homeTeam: oddsEvtRev.homeTeam, awayTeam: oddsEvtRev.awayTeam, markets: Object.keys(oddsEvtRev.markets || {}) } : null,
        };
        result.steps[tryKey].matched = !!(oddsEvt || oddsEvtRev);
      } else {
        result.steps[tryKey].matched = false;
      }
    }

    // Also try fetching PX markets for this event
    try {
      const markets = await px.fetchMarkets(event.event_id);
      result.pxMarkets = markets.map(m => ({ type: m.type, name: m.name, lineCount: (m.market_lines || []).length }));
    } catch (err) {
      result.pxMarkets = `Error: ${err.message}`;
    }

    report.eventResults.push(result);
  }

  return report;
}

/**
 * Re-seed lines. Clears old index and re-fetches everything.
 */
async function refreshLines() {
  log.info('Lines', 'Refreshing all lines...');
  // Clear existing
  for (const key of Object.keys(lineIndex)) {
    delete lineIndex[key];
  }
  return seedAllLines();
}

/**
 * Resolve a tournament_id to a human-readable name.
 */
function getTournamentName(tournamentId) {
  const t = tournamentIndex[tournamentId];
  return t ? `${t.name} (${t.sport})` : null;
}

/**
 * Resolve a sport_event_id to event name.
 */
function getEventName(eventId) {
  const e = eventIndex[eventId];
  return e ? e.name : null;
}

/**
 * Get full event info for a sport_event_id (sport, name, competitors, scheduled).
 */
function getEventInfo(eventId) {
  return eventIndex[eventId] || null;
}

module.exports = {
  seedAllLines,
  refreshLines,
  lookupLine,
  lookupLineAsync,
  __debugGetLineIndex,
  resolveUnknownLine,
  getRegisteredLineIds,
  getStats,
  getLineCount,
  getLineSummary,
  matchTeamName,
  normalizeTeamName,
  getTournamentName,
  getEventName,
  getEventInfo,
  debugGolfMatching,
};
