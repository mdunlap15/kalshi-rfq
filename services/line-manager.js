const { config } = require('../config');
const log = require('./logger');
const px = require('./prophetx');
const oddsFeed = require('./odds-feed');
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
const MAX_SPREAD_BY_SPORT = {
  'basketball_nba': 30,
  'basketball_ncaab': 40,
  'basketball_wnba': 30,
  'icehockey_nhl': 3,
  'baseball_mlb': 3,
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
 * defined bounds. `marketType` = 'total' or 'spread'. F5 markets should be
 * excluded by the caller.
 */
function isValidFullGameLine(sport, marketType, line) {
  if (line == null) return true;
  const absLine = Math.abs(line);
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
  return (name || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
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

    const mainMarkets = markets.filter(m => {
      const supportedBase = ['moneyline', 'spread', 'total', 'team_total', 'btts', 'both_teams_to_score', 'double_chance'];
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
      if ((m.type === 'total' || m.type === 'spread') && !isF5) {
        const parsed = px.parseMarketSelections(m);
        const line = parsed[0]?.line;
        if (!isValidFullGameLine(sportKey, m.type, line)) {
          log.debug('Lines', `Rejecting out-of-bounds ${m.type} ${line} for ${sportKey}: ${m.name}`);
          return false;
        }
      }
      return true;
    });

    for (const market of mainMarkets) {
      const parsed = px.parseMarketSelections(market);
      // Detect 2-way / Draw No Bet soccer moneylines
      const isDNB = market.type === 'moneyline' && /2.way|draw.no.bet/i.test(market.name);

      for (const sel of parsed) {
        totalLines++;

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
          // Determine home/away from team name, combine with over/under
          const isHome = matchTeamName(sel.teamName, [matchedHome]);
          const teamSide = isHome ? 'home' : 'away';
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
      for (const market of markets || []) {
        if (!SUPPORTED_TYPES.includes(market.type)) continue;
        // Reject sub-game/prop totals and spreads by sport-aware bounds.
        // F5 markets must be exempt — detect by NAME, not type, because PX
        // uses type='spread' / 'total' for F5 (only name distinguishes).
        // Using F5_MARKET_TYPES against market.type was always false, causing
        // MLB F5 total line 3.5 to be rejected against the MLB total bounds
        // [4, 15] and preventing any F5 parlay from being quoted.
        const isF5Market = f5NamePat.test(market.name || '');
        if ((market.type === 'total' || market.type === 'spread') && !isF5Market) {
          const parsedAll = px.parseMarketSelections(market);
          const firstLine = parsedAll[0]?.line;
          if (!isValidFullGameLine(sportKey, market.type, firstLine)) {
            log.debug('Lines', `resolveUnknownLine: rejecting out-of-bounds ${market.type} ${firstLine} for ${sportKey}: ${market.name}`);
            resolveUnknownLine._lastFailure = { lineId, reason: 'out_of_bounds_line', sport: sportKey, marketType: market.type, line: firstLine, marketName: market.name };
            continue;
          }
        }
        const parsed = px.parseMarketSelections(market);
        for (const sel of parsed) {
          if (sel.lineId !== lineId) continue;
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
            const isHome = matchTeamName(sel.teamName, [matchedHome]);
            const teamSide = isHome ? 'home' : 'away';
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

          const onDemandDNB = market.type === 'moneyline' && /2.way|draw.no.bet/i.test(market.name);
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
};
