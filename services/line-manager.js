const { config } = require('../config');

// Memoized websocket module reference. The Phase-2 prop bridge inside
// resolveUnknownLine pulls classifiers + name extractor from websocket
// via require('./websocket'). The require call has lookup overhead on
// every RFQ — caching it here saves ~30-80μs per prop-leg RFQ.
// Initialized lazily on first call to avoid the line-manager ↔
// websocket circular import at module-load time.
let _wsModule = null;
function _getWsModule() {
  if (_wsModule === null) {
    try { _wsModule = require('./websocket'); }
    catch (e) { _wsModule = false; } // sentinel for "tried, failed"
  }
  return _wsModule || null;
}

// Module-level prop-type → TOA market key maps for the bridge. Lifted
// out of the inner closure (was re-created on every prop RFQ) to avoid
// the per-call object construction.
const _NBA_PROP_TO_TOA_MARKET = {
  points: 'player_points',
  rebounds: 'player_rebounds',
  assists: 'player_assists',
  threes_made: 'player_threes',
};
const _NHL_PROP_TO_TOA_MARKET = {
  shots_on_goal: 'player_shots_on_goal',
};
// MLB hitter props. classifyMlbProp's bucket names map to TOA's
// batter_* market keys. Operator chose to enable the high-volume hitter
// markets (44% hits, 36% HR per 24h prop-flow sample) — pitcher_other
// stays unmapped (varies too much: outs recorded vs IP vs walks).
// pitcher_strikeouts has its own bridge (lookupPlayerStrikeoutProp +
// lookupPlayerStrikeoutPropFromTheOddsApi), not routed here.
const _MLB_PROP_TO_TOA_MARKET = {
  hitter_hits: 'batter_hits',
  hitter_hr: 'batter_home_runs',
  hitter_total_bases: 'batter_total_bases',
  hitter_rbi_runs: 'batter_rbis',
};
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

// Cold-start gate for Supabase hydration. Flipped true after the first
// seedAllLines completes. Hydration only runs on cold boot (when
// lineIndex hasn't been authoritatively seeded yet); periodic
// refreshLines cycles skip it so the per-sport stale-event cutoff in
// seed stays authoritative and finished games don't bleed back in
// from line_cache.
let _hasSeededOnce = false;

// O(1) reverse index for getPrimarySpreadHomePoint / getPrimaryTotalLine.
// Without this, those helpers do Object.values(lineIndex) which is O(N=~1200)
// per call. Called per leg in shouldDecline → significant hot-path cost on
// NBA-heavy parlays (Apr 26 latency regression: p50 1.0ms → 1.9ms after
// the NBA alt-spread carve-out shipped). Indexed by pxEventId, stores the
// SHORTEST lineId we saw for each (event, market) primary so lookups are
// constant-time.
//   { [pxEventId]: { spread: lineInfo|null, total: lineInfo|null } }
// Maintained alongside lineIndex via _trackPrimaryForIndex() — every
// insertion into lineIndex flows through this hook.
const primaryByEvent = {};

function _trackPrimaryForIndex(lineInfo) {
  if (!lineInfo) return;
  if (lineInfo.onDemand === true) return;
  const eid = lineInfo.pxEventId;
  if (eid == null) return;
  const mt = lineInfo.marketType;
  if (mt !== 'spread' && mt !== 'total') return;
  if (!primaryByEvent[eid]) primaryByEvent[eid] = { spread: null, total: null };

  // Bug 2026-04-27: previous "first-seen wins" heuristic let alt spreads
  // get locked in as primary if PX seeded them before the main spread —
  // observed Spurs -25.5 + Knicks -20.5 alt parlays passing through
  // isBlockedAltSpread because their false-primary made the +/- 2.0
  // distance check trivially true (alt == "primary").
  //
  // Fix: prefer the SMALLEST-magnitude line as primary. The actual main
  // spread is always the line closest to zero (e.g. NBA -3.5 main, alt
  // ladder runs out to -25.5 in 1-pt increments). Replacing on smaller
  // magnitude means even if PX seeds alts first, the main eventually
  // wins as soon as it's registered.
  //
  // For totals: same logic — though the "smallest" framing is less
  // intuitive (totals are positive numbers, not signed). Use abs(line)
  // for both. NBA totals primary is ~220, alts run 200-240; the "main"
  // is the line with the most book consensus, which is typically the
  // median, not the smallest. So for totals we keep first-seen for now
  // — the spread bug was the operator-observed one.
  const newLine = Number(lineInfo.line);
  if (!Number.isFinite(newLine)) return;
  const newAbs = Math.abs(newLine);
  if (newAbs === 0) return; // line=0 isn't a real spread/total

  if (mt === 'spread') {
    const cur = primaryByEvent[eid].spread;
    const curAbs = cur ? Math.abs(Number(cur.line) || Infinity) : Infinity;
    if (newAbs < curAbs) primaryByEvent[eid].spread = lineInfo;
  }
  if (mt === 'total') {
    // Totals: same bug class as spreads (first-seen could be an alt
    // seeded before the main), but "smallest wins" doesn't translate —
    // totals are positive and the main is near-median, not near-zero.
    //
    // Fix: track ALL seen total lines per event, then getPrimaryTotalLine
    // returns the median. With ≥3 alts seeded, the median converges to
    // the main quickly (alt ladders cluster symmetrically around the
    // main). With <3 known lines, fall back to first-seen.
    if (!primaryByEvent[eid].total) primaryByEvent[eid].total = lineInfo;
    if (!primaryByEvent[eid].seenTotalLines) primaryByEvent[eid].seenTotalLines = new Set();
    primaryByEvent[eid].seenTotalLines.add(newAbs);
  }
}

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
  'baseball_mlb': [6.5, 15],
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
  // MMA total rounds — 3-round prelims have 1.5/2.5 lines, 5-round
  // main events add 3.5/4.5. Range [0.5, 5.5] covers every DK-posted
  // rounds line.
  'mma_mixed_martial_arts': [0.5, 5.5],
  'soccer_mexico_ligamx': [0.5, 7],
  'soccer_brazil_campeonato': [0.5, 7],
  'soccer_conmebol_libertadores': [0.5, 7],
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
  'soccer_mexico_ligamx': 5,
  'soccer_brazil_campeonato': 5,
  'soccer_conmebol_libertadores': 5,
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
 * Resolve the display-friendly team name for a parsed selection.
 *
 * Most market types (moneyline, spread, F5/H1 variants) carry the full
 * team name in `sel.teamName` because parseMarketSelections lifts it
 * from the selection text. team_total is the exception: PX names those
 * markets like "CLE: Team Total Points" or "TOR Team Total Runs", so
 * the parsed teamName is just the abbreviated prefix ("CLE", "TOR").
 *
 * Storing the abbreviation as `lineIndex[lineId].teamName` propagated
 * to the dashboard's parlay-detail "Team / Selection" column, where
 * mid-parlay legs read like "CLE" alongside fully-named legs like
 * "Atlanta Braves -1.5". For team_total legs, swap the abbreviation
 * for the matched event's full home/away team name (we already
 * resolved the side via resolveTeamTotalSide for selection routing).
 *
 * Falls through to sel.teamName for every other market type — those
 * already carry the canonical name.
 */
function resolveDisplayTeamName(sel, matchedHome, matchedAway) {
  if (sel && sel.marketType === 'team_total') {
    const side = resolveTeamTotalSide(sel.teamName, matchedHome, matchedAway);
    if (side === 'home') return matchedHome;
    if (side === 'away') return matchedAway;
  }
  return sel ? sel.teamName : null;
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
  // First Half (NBA) — PX market.type guesses; adjust based on decline-audit log
  'first_half_moneyline': 'h2h_h1',
  '1st_half_moneyline': 'h2h_h1',
  'first_half_spread': 'spreads_h1',
  '1st_half_spread': 'spreads_h1',
  'first_half_total': 'totals_h1',
  '1st_half_total': 'totals_h1',
  'first_half_total_points': 'totals_h1',
  '1st_half_total_points': 'totals_h1',
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

const FIRST_HALF_MARKET_TYPES = [
  'first_half_moneyline',
  '1st_half_moneyline',
  'first_half_spread',
  '1st_half_spread',
  'first_half_total',
  '1st_half_total',
  'first_half_total_points',
  '1st_half_total_points',
];

// Pitcher strikeouts prop detection.
//
// PX uses market.type='total' for these — same as game totals — and
// disambiguates only via market.name like "Dustin May Total Pitching
// Strikeouts". Returns true for pitcher K markets, false for hitter K
// markets ("Batting Strikeouts" — separate market, handled later).
//
// Catches PX's standard "Pitching Strikeouts" form plus less-common
// "K's Thrown" / "Strikeouts Thrown" variants.
function isPitcherStrikeoutMarket(name) {
  if (!name) return false;
  if (/batting\s+strike/i.test(name)) return false; // explicit hitter exclusion
  if (/pitching\s+strike/i.test(name)) return true;
  if (/\bk'?s?\s+thrown\b/i.test(name)) return true;
  if (/strike\s*outs?\s+thrown\b/i.test(name)) return true;
  return false;
}

// Extract pitcher name from a K-prop market name. PX format is typically
// "<Player Name> Total Pitching Strikeouts" — strip the trailing stat
// phrase. Final cleanup pass strips any leftover "Total" word so future
// PX naming variants don't bleed through.
function extractPitcherNameFromKMarket(name) {
  if (!name) return null;
  let stripped = String(name)
    .replace(/\s+(?:total\s+)?pitching\s+strike\s*outs?$/i, '')
    .replace(/\s+(?:total\s+)?strike\s*outs?\s+thrown$/i, '')
    .replace(/\s+(?:total\s+)?k'?s?\s+thrown$/i, '')
    .trim();
  stripped = stripped.replace(/\s+total$/i, '').trim();
  return stripped || null;
}

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
  log.info('Lines', '[golf-debug] seedAllLines starting — golf bypass code v2 is live');

  // 0. Hydrate lineIndex from Supabase BEFORE the slow seed loop runs.
  // The seed itself takes 30-90s (PX fetch + per-event market parse +
  // matching). Without hydration, every Railway redeploy clears the
  // in-memory lineIndex and ~minute of RFQs decline as "unknown legs"
  // until seed completes. Hydration takes <2s and immediately makes
  // every recent-event line in line_cache priceable. Seed then
  // overwrites stale entries with fresh data and adds new entries.
  //
  // Only runs on COLD START (the first seed of the process). On
  // subsequent refreshLines() cycles, the seed is fast (warm odds caches)
  // and there's no gap to bridge — re-hydrating would re-introduce
  // entries from games that just finished but haven't aged out of
  // Supabase yet (line_cache uses 6h cutoff vs the per-sport cutoff
  // applied below in seed). Cold-start gating keeps the seed
  // authoritative on every periodic refresh.
  if (!_hasSeededOnce && Object.keys(lineIndex).length === 0) {
    try {
      const hydrated = await db.loadAllRecentLineCache(1);
      let count = 0;
      for (const [lineId, info] of Object.entries(hydrated)) {
        lineIndex[lineId] = info;
        _trackPrimaryForIndex(info);
        count++;
      }
      if (count > 0) {
        log.info('Lines', `Hydrated ${count} lines from Supabase line_cache before seed (cold start)`);
      }
    } catch (err) {
      log.warn('Lines', `Line cache hydration failed (non-fatal): ${err.message}`);
    }
  }

  // 1. Fetch PX events
  const allEvents = await px.fetchSportEvents();
  const pxSportNames = Object.values(config.sportNameMap);
  const golfEventCount = allEvents.filter(e => e.sport_name === 'Golf').length;
  log.info('Lines', `[golf-debug] PX returned ${allEvents.length} total events, ${golfEventCount} with sport_name="Golf"`);

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

  // 2. Filter to supported sports (accept any non-settled status).
  // Also drop events whose scheduled start is more than the per-sport
  // post-game cutoff in the past. PX can take many hours to mark a
  // finished game as 'settled', and during that window we'd otherwise
  // keep yesterday's F5/spread/total lines in our index, polluting the
  // dashboard and wasting the Supabase line-cache budget.
  //
  // Cutoffs sized to typical game length + buffer for OT / extras / late
  // finishes. Golf is exempt (multi-day tournaments — Round 1 scheduled
  // on Thursday is still relevant Sunday).
  const POST_GAME_CUTOFF_HOURS_BY_SPORT = {
    'Baseball':   5,   // MLB ~3.5hr typical; extras can push to 5+
    'Basketball': 4,   // NBA/WNBA/NCAAB ~2.5hr typical; OT pushes
    'Hockey':     4,   // NHL ~2.5hr; OT/SO buffer
    'Tennis':     6,   // matches occasionally run long
    'Soccer':     3,   // ~2hr typical
    'MMA':        7,   // multi-fight cards
    'Boxing':     7,   // same
    'Football':   4,   // NFL/NCAAF ~3.5hr
    'Golf':       9999, // multi-day tournaments — never filter on scheduled
  };
  const DEFAULT_CUTOFF_HOURS = 6;
  const nowMs = Date.now();
  let droppedAsStale = 0;
  const events = allEvents.filter(e => {
    if (!pxSportNames.includes(e.sport_name)) return false;
    if (e.status && e.status === 'settled') return false;
    if (!e.competitors || e.competitors.length < 2) return false;
    if (e.scheduled) {
      const startMs = new Date(e.scheduled).getTime();
      const cutoffHours = POST_GAME_CUTOFF_HOURS_BY_SPORT[e.sport_name] ?? DEFAULT_CUTOFF_HOURS;
      const cutoffMs = nowMs - cutoffHours * 3600 * 1000;
      if (Number.isFinite(startMs) && startMs < cutoffMs) {
        droppedAsStale++;
        return false;
      }
    }
    return true;
  });
  log.info('Lines', `Found ${events.length} supported sport events (of ${allEvents.length} total; dropped ${droppedAsStale} past per-sport stale cutoff)`);

  // Get all Odds API cached events for matching
  const oddsApiEvents = oddsFeed.getAllCachedEvents();

  let totalLines = 0;
  let matchedLines = 0;
  let unmatchedEvents = [];
  // Golf-seed trace counters. Populated during the main event loop so
  // we can inspect drop points without trawling Railway logs. Exposed
  // in the /refresh-lines response via lastSeedStats.
  const golfTrace = {
    eventsFiltered: 0,       // golf events that passed the outer pxSportNames filter
    bypassFired: 0,          // event-level bypass set matchedHome from competitors
    marketsFound: 0,         // golf markets returned by PX fetchMarkets
    marketsPassedFilter: 0,  // golf markets that survived mainMarkets filter
    selectionsParsed: 0,     // individual selections across golf markets
    selectionsSkipped: 0,    // selections that got `continue` at the oddsApiSelection check
    linesRegistered: 0,      // golf lines that made it into lineIndex
    sampleEventName: null,
    sampleMarketName: null,
    sampleSelectionTeam: null,
  };

  // 3-4. Fetch markets and parse for each event
  for (const event of events) {
    const _isGolfTrace = event.sport_name === 'Golf';
    if (_isGolfTrace) {
      golfTrace.eventsFiltered++;
      if (!golfTrace.sampleEventName) golfTrace.sampleEventName = event.name;
    }
    // Determine sport key(s) — some PX sport names map to multiple keys
    // (e.g., "Basketball" → basketball_nba AND basketball_ncaab,
    // "Soccer" → soccer + soccer_epl + soccer_germany_bundesliga + ...).
    //
    // ORDER MATTERS: the matching loop below breaks on the FIRST sport
    // key whose cache contains a matching team-name pair, which means
    // the generic catch-all key wins over league-specific keys when
    // both have entries for the same match. That mis-registers
    // today's EPL/Bundesliga/Serie A matches under sport='soccer' so
    // they don't appear in the league-specific dashboard filter.
    //
    // Sort to put generic / catch-all keys LAST. Heuristic: keys whose
    // name has no underscore-suffix (e.g. 'soccer', 'tennis') are
    // generic; keys with a suffix ('soccer_epl', 'tennis_atp_madrid')
    // are specific. Specific keys win the matching race.
    const _isGenericKey = (k) => !k.includes('_') || k === 'mma_mixed_martial_arts' || k === 'boxing_boxing';
    const possibleSportKeys = Object.entries(config.sportNameMap)
      .filter(([k, v]) => v === event.sport_name)
      .map(([k]) => k)
      .sort((a, b) => {
        const aGen = _isGenericKey(a) ? 1 : 0;
        const bGen = _isGenericKey(b) ? 1 : 0;
        return aGen - bGen; // generic last
      });
    if (possibleSportKeys.length === 0) continue;

    // We'll determine the actual sport key by which one has a matching Odds API event
    let sportKey = possibleSportKeys[0]; // default to first match (now most-specific)

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

    // Series-winner events (NHL-style: a separate PX event named
    // "Series Winner - X vs Y" rather than a sub-market of the game).
    // We price these against the DK scraper cache, not the odds feed,
    // so skip the odds-api match and use competitor names directly.
    const isSeriesEvent = /^\s*series\s*winner\b/i.test(event.name || '');
    // Golf events where our odds cache didn't cover the pair. DataGolf
    // covers individual 1v1 matchups but NOT team pairs (Zurich Classic
    // is the one PGA event per year that's team-format). BetOnline
    // manual-upload cache supplies the fair, and pricer's cascade hits
    // it via lookupZurichMatchupFairProb. Register from PX competitor
    // names directly so the line lives in our index; if pricing still
    // fails at RFQ time, we decline cleanly — no harm done.
    const isGolfEvent = event.sport_name === 'Golf';
    if (!matchedHome || !matchedAway) {
      if (isSeriesEvent) {
        matchedHome = homeComp.name;
        matchedAway = awayComp.name;
      } else if (isGolfEvent) {
        matchedHome = homeComp.name;
        matchedAway = awayComp.name;
        sportKey = 'golf_matchups';
        golfTrace.bypassFired++;
        log.info('Lines', `[golf-debug] Event-level bypass fired: ${event.name} → home="${matchedHome}" away="${matchedAway}" sportKey="${sportKey}"`);
      } else {
        unmatchedEvents.push({
          pxEvent: event.name,
          pxHome: homeComp.name,
          pxAway: awayComp.name,
        });
        continue;
      }
    }

    // Verify this home/away pair exists as an actual Odds API event
    const pxScheduled = event.scheduled || null;
    const oddsEvent = matchedOddsEvent || oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway, pxScheduled);
    if (!oddsEvent && !isSeriesEvent && !isGolfEvent) {
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

    // Just-in-time alt-line warm. Fire-and-forget so the per-event fetch
    // overlaps with the markets fetch below instead of waiting up to 15s
    // for the periodic warm loop to pick up this event. The JIT function
    // dedupes via altLinesCache TTL + in-flight map, and throttles via
    // its own concurrency queue, so firing unconditionally per event is
    // safe — already-fresh entries return in O(1) with no API call.
    if (matchedHome && matchedAway && sportKey) {
      oddsFeed.warmEventAltLinesJIT({
        sport: sportKey,
        homeTeam: matchedHome,
        awayTeam: matchedAway,
        commenceTime: pxScheduled,
      }).catch(err => {
        log.debug('Lines', `JIT warm (seed) swallowed error: ${err.message}`);
      });
    }

    // Fetch PX markets
    let markets;
    try {
      markets = await px.fetchMarkets(event.event_id);
    } catch (err) {
      log.error('Lines', `Failed to fetch markets for ${event.name}: ${err.message}`);
      continue;
    }
    if (_isGolfTrace) {
      golfTrace.marketsFound += markets.length;
      if (!golfTrace.sampleMarketName && markets[0]) golfTrace.sampleMarketName = `${markets[0].name} (type=${markets[0].type})`;
    }

    // Filter to FULL-GAME main markets only.
    // Exclude: first half, first quarter, period, inning, player props
    // Exclude: sub-game markets (halves/quarters/periods/innings) and player
    // props. Standalone prop keywords (strikeouts, pitching, milestones, etc.)
    // ensure we reject props even when "Total" appears in the name with
    // intervening words (e.g. "Total Pitching Strikeouts Milestones").
    const excludePatterns = /first quarter|1st quarter|2nd half|2nd quarter|3rd quarter|4th quarter|1st period|2nd period|3rd period|1st inning|overtime|player|milestones|strikeouts?|pitching|batting|hits|doubles\b|triples?|errors|walks|stolen bases?|rbis?|home runs?\b|outs recorded|innings pitched|at bats?|put outs?|fouls|cards|bookings|yellow cards?|red cards?|offsides?|crosses|clearances|throw.?ins?|tackles|shots|total earned|total block|total point[^s]|total rebound|total assist|total steal|total made|total rush|total recei|total passing/i;

    const fullGameNames = {
      moneyline: ['Moneyline', 'Moneyline (2 Way)', 'Moneyline (2-Way)', 'Moneyline (Regulation)', 'Draw No Bet'],
      spread: ['Spread', 'Run Line', 'Puck Line', 'Spread (Regular Time)', 'Game Spread', 'Point Spread'],
      total: ['Total', 'Total Points', 'Points', 'Total Runs', 'Total Goals', 'Total Goals (Regular Time)', 'Total Rounds', 'Rounds'],
      team_total: ['Team Total', 'Team Total Points', 'Team Total Runs', 'Team Total Goals', 'Home Total', 'Away Total'],
    };

    // F5 markets (PX uses market.type === 'moneyline'/'spread'/'total' but
    // distinguishes via market.name). Allow these through the filter.
    const f5NamePattern = /1st[-\s]?5th.*inning|first\s*5\s*inning|first\s*five\s*innings/i;
    const h1NamePattern = /first\s*half|1st\s*half/i;

    // Combat sports (MMA, Boxing) historically only had moneyline
    // in our odds feeds. MMA now gets Total Rounds from the DK scraper
    // (services/dk-scraper.fetchMmaFightOdds) merged into the cache —
    // so MMA can register 'total' markets. Boxing still moneyline only.
    const isMmaSport = sportKey === 'mma_mixed_martial_arts';
    const isBoxingSport = sportKey === 'boxing_boxing';
    const isCombatSport = isBoxingSport; // only boxing keeps the ML-only restriction
    // Golf matchups are H2H moneyline only. PX labels the market name
    // "Tournament Matchup" / "Round 1 Matchup" rather than "Moneyline",
    // so the name-allowlist filter below drops them. Flag here so we
    // can bypass the name filter just for this sport without loosening
    // anything else.
    const isGolfSport = sportKey === 'golf_matchups';

    // Series markets (winner/spread/total-games) are priced from the DK
    // scraper cache rather than the odds feed. Allow them through the
    // seed filter when the market name matches. PX uses its standard
    // moneyline/spread/total types for these — we distinguish by name
    // and retag marketType in the per-selection loop below.
    const seriesWinnerNamePat = /\bseries\s*winner\b/i;
    const seriesSpreadNamePat = /\bseries\s*(spread|handicap)\b|\bseries\b[^.]*\bspread\b/i;
    const seriesTotalNamePat  = /\bseries\s*total\b|\btotal\s*games\b|\bseries\b[^.]*\btotal\b/i;

    const mainMarkets = markets.filter(m => {
      const name = m.name || '';
      const isSeriesWinner = seriesWinnerNamePat.test(name);
      const isSeriesSpread = seriesSpreadNamePat.test(name);
      const isSeriesTotal  = !isSeriesSpread && seriesTotalNamePat.test(name);
      const isSeriesMarket = isSeriesWinner || isSeriesSpread || isSeriesTotal;
      const supportedBase = isCombatSport
        ? ['moneyline']
        : isMmaSport
          ? ['moneyline', 'total']
          : isGolfSport
            ? ['moneyline']
            : ['moneyline', 'spread', 'total', 'team_total', 'btts', 'both_teams_to_score', 'double_chance'];
      // Series markets include PX's 'sup_moneyline' type (Series Game Spread,
      // Series Total Games — live probe 2026-04-18). Let those through the
      // supportedBase check; parseMarketSelections retags them to 'spread'
      // or 'total' so selection parsing works.
      const isSupSeries = m.type === 'sup_moneyline' && (isSeriesSpread || isSeriesTotal);
      // Soccer asian-handicap spreads use type='sup_moneyline' with
      // name "Spread (Regular Time)". Verified 2026-05-03: every EPL,
      // UCL, La Liga, Serie A, Bundesliga, etc. spread market on PX
      // rides this combo. Without this carve-out, supportedBase gate
      // rejects every soccer spread → zero spread lines for sub-leagues
      // like EPL/UCL despite ML/total working fine. parseMarketSelections
      // retags marketType='spread' for these so downstream lookup works.
      const isSoccerSupSpread = m.type === 'sup_moneyline'
        && !isSeriesMarket
        && /soccer|fifa/i.test(sportKey || '')
        && /^spread\b/i.test(m.name || '');
      if (!isSupSeries && !isSoccerSupSpread && !supportedBase.includes(m.type) && !F5_MARKET_TYPES.includes(m.type) && !FIRST_HALF_MARKET_TYPES.includes(m.type)) return false;
      // Series markets bypass the sub-game/prop filter and the name-
      // allowlist + bounds checks. Each variant must match one of:
      //   Series Winner      → type='moneyline'
      //   Series Spread      → type='spread' OR 'sup_moneyline'
      //   Series Total Games → type='total'   OR 'sup_moneyline'
      if (isSeriesMarket) {
        if (isSeriesWinner && m.type === 'moneyline') return true;
        if (isSeriesSpread && (m.type === 'spread' || m.type === 'sup_moneyline')) return true;
        if (isSeriesTotal  && (m.type === 'total'  || m.type === 'sup_moneyline')) return true;
        return false;
      }
      // Exclude anything matching half/quarter/prop patterns
      if (excludePatterns.test(m.name)) return false;
      // Allow F5 markets by name pattern
      const isF5 = f5NamePattern.test(m.name || '');
      const isH1 = h1NamePattern.test(m.name || '') || FIRST_HALF_MARKET_TYPES.includes(m.type);
      // Name filter: previously required EXACT match against a fixed whitelist
      // which rejected alt-line markets like "Alternate Spread +3.5" — costing
      // us thousands of unknown-leg declines per day. Relaxed to substring
      // match: the market name must CONTAIN one of the canonical full-game
      // names (e.g. "Alternate Spread" contains "Spread"). Player props still
      // fail because their names don't contain "Spread", "Moneyline", etc.
      // Additional safety comes from excludePatterns (above) and sport-aware
      // line bounds (below).
      // Golf markets on PX use names like "Tournament Matchup" or
      // "Round 1 Matchup" — none contain "Moneyline". Since the sport
      // is already gated to moneyline-only via supportedBase above,
      // no further name check is needed for golf.
      if (!isF5 && !isH1 && !isGolfSport) {
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
      if ((m.type === 'total' || m.type === 'spread') && !isF5 && !isH1) {
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
      // K-prop seed branch — MLB pitcher_strikeouts. PX tags these as
      // type='total' with the player name embedded in market.name (e.g.
      // "Cole Ragans Total Pitching Strikeouts"). Process them BEFORE the
      // standard total path so K-prop lines (4.5–8.5) don't get filtered
      // by the MLB game-total bounds check (which expects 5-15 for game
      // totals), and so they register with marketType='player_strikeouts'
      // instead of 'total'. Sync SharpAPI lookup only — TOA escalation
      // happens via the on-demand resolveUnknownLine path for any K-prop
      // RFQ lineId we miss here. Pre-seeding moves visible K-prop coverage
      // from "only previously RFQ'd" (~23) to "every K-prop SharpAPI knows
      // about" (~60+) so the Lines tab reflects actual quotability.
      if (sportKey === 'baseball_mlb' && market.type === 'total' && isPitcherStrikeoutMarket(market.name || '')) {
        const playerName = extractPitcherNameFromKMarket(market.name);
        if (!playerName) {
          log.debug('Lines', `K-prop seed: name extract failed for "${market.name}"`);
          continue;
        }
        const eventCtx = { homeTeam: matchedHome, awayTeam: matchedAway, startTime: event.scheduled || null };
        const parsedK = px.parseMarketSelections(market);
        let registered = 0;
        for (const sel of parsedK) {
          totalLines++;
          let lookup = oddsFeed.lookupPlayerStrikeoutProp(sportKey, eventCtx, playerName, sel.line);
          // Confidence gate: same as resolveUnknownLine. Skip if SharpAPI
          // returned only a single non-trusted book.
          const trustedSet = (config.pricing && config.pricing.propTrustedSingleBooks) || [];
          const usableFair = (l) => l && l.fairProbOver != null && l.fairProbUnder != null;
          const isHighConfidence = (l) => {
            if (!usableFair(l)) return false;
            const both = l.booksWithBothSides || 0;
            if (both >= 2) return true;
            const books = l.books || [];
            return both === 1 && books.some(b => trustedSet.includes(String(b).toLowerCase()));
          };
          if (!isHighConfidence(lookup)) continue;
          const fairProb = sel.selection === 'over' ? lookup.fairProbOver : lookup.fairProbUnder;
          lineIndex[sel.lineId] = {
            sport: sportKey,
            pxEventId: event.event_id,
            pxEventName: event.name,
            marketType: 'player_strikeouts',
            marketName: market.name,
            selection: sel.selection,
            teamName: playerName, // dashboards display "team" — use pitcher name
            line: sel.line,
            homeTeam: matchedHome,
            awayTeam: matchedAway,
            oddsApiSport: sportKey,
            oddsApiMarket: 'player_strikeouts',
            oddsApiSelection: sel.selection,
            startTime: event.scheduled || null,
            playerName,
            fairProb,
            fairProbOver: lookup.fairProbOver,
            fairProbUnder: lookup.fairProbUnder,
            booksWithBothSides: lookup.booksWithBothSides,
            propBooks: lookup.books,
            propSource: 'sharpapi',
            propFetchedAt: lookup.fetchedAt || Date.now(),
          };
          _trackPrimaryForIndex(lineIndex[sel.lineId]);
          matchedLines++;
          registered++;
        }
        if (registered > 0) {
          log.debug('Lines', `K-prop seed: ${playerName} registered ${registered} lines`);
        }
        continue; // K-prop market done — skip standard processing
      }
      const parsed = px.parseMarketSelections(market);
      if (isGolfSport) {
        log.info('Lines', `[golf-debug] Parsed market "${market.name}" type=${market.type} → ${parsed.length} selections`);
        golfTrace.marketsPassedFilter++;
        golfTrace.selectionsParsed += parsed.length;
        if (!golfTrace.sampleSelectionTeam && parsed[0]) golfTrace.sampleSelectionTeam = parsed[0].teamName;
      }
      // Detect 2-way / Draw No Bet soccer moneylines.
      // PX labels the 2-way soccer ML market as "Moneyline (2 Way)".
      // Also catch explicit "Draw No Bet" / "DNB" / "Moneyline 2W" variants.
      const isDNB = market.type === 'moneyline' && /\b2\s*[\s\-_]?way\b|draw\s*no\s*bet|\bdnb\b|\b2w\b/i.test(market.name || '');
      const mName = market.name || '';
      const isSeriesWinnerMarket = seriesWinnerNamePat.test(mName);
      const isSeriesSpreadMarket = seriesSpreadNamePat.test(mName);
      const isSeriesTotalMarket  = !isSeriesSpreadMarket && seriesTotalNamePat.test(mName);
      const isSeriesMarket = isSeriesWinnerMarket || isSeriesSpreadMarket || isSeriesTotalMarket;

      for (const sel of parsed) {
        totalLines++;
        // Tag series selections so downstream (pricer) routes them to
        // the DK scraper cache instead of oddsFeed. Series markets are
        // structurally identical to moneyline/spread/total but we use a
        // distinct marketType so the pricer takes the DK path.
        if (isSeriesWinnerMarket && sel.marketType === 'moneyline') {
          sel.marketType = 'series_winner';
        } else if (isSeriesSpreadMarket && sel.marketType === 'spread') {
          sel.marketType = 'series_spread';
        } else if (isSeriesTotalMarket && sel.marketType === 'total' && sportKey !== 'tennis') {
          // Tennis "Total Games" markets match seriesTotalNamePat but
          // they are MATCH-LEVEL totals (over/under games in the match),
          // not playoff series totals. Keep them as marketType 'total'
          // so they route to the standard totals-cache lookup path
          // (TOA caches as 'totals'). Without this carve-out, tennis
          // total-games legs get registered as series_total and look
          // for fair probs in a cache key (series_total) that doesn't
          // exist for tennis — every line returns null fair.
          sel.marketType = 'series_total';
        }

        // Per-selection bounds check for spread/total/team_total alt lines.
        // The market-level filter above accepts the market if ANY selection
        // is in bounds; this check rejects the individual out-of-range ones
        // (e.g. Rangers -6.5 puck line) while keeping sibling in-range
        // alts registered. team_total uses permissive bounds (see
        // isValidFullGameLine) since their lines are naturally low.
        // Series markets bypass entirely — series spread lines (±1.5, ±2.5
        // games) are valid by definition, and series totals (5.5-7.5 games)
        // fall far below full-game total bounds but are also valid.
        const selMarketType = ['spread', 'total', 'team_total'].includes(sel.marketType) ? sel.marketType : null;
        if (selMarketType && !isSeriesMarket && !isValidFullGameLine(sportKey, selMarketType, sel.line)) {
          continue;
        }

        // Determine Odds API selection mapping
        let oddsApiSelection = null;
        let oddsApiMarket = MARKET_TYPE_MAP[sel.marketType];

        if (sel.marketType === 'series_winner') {
          // Series-winner: same team→home/away mapping as moneyline.
          // Team names in PX selections sometimes carry a "(Series)"
          // suffix (e.g. "Cleveland Cavaliers (Series)"); strip it
          // before matching. Keep the suffix on the stored teamName
          // so the pricer can recognize the leg type via name, too.
          const cleanTeam = (sel.teamName || '').replace(/\s*\(series\)\s*/ig, '').trim();
          if (matchTeamName(cleanTeam, [matchedHome])) {
            oddsApiSelection = 'home';
          } else if (matchTeamName(cleanTeam, [matchedAway])) {
            oddsApiSelection = 'away';
          }
          // series_winner has no oddsApiMarket (not in MARKET_TYPE_MAP);
          // set a sentinel so the !oddsApiMarket gate below doesn't
          // reject it. Pricer skips oddsFeed for this marketType.
          oddsApiMarket = 'series_winner';
        } else if (sel.marketType === 'series_spread') {
          // Series-spread: team→home/away plus a signed line (PX stores
          // negative for favorite, positive for underdog). Pricer uses
          // teamName + line sign to query the DK scraper cache.
          const cleanTeam = (sel.teamName || '').replace(/\s*\(series\)\s*/ig, '').trim();
          if (matchTeamName(cleanTeam, [matchedHome])) oddsApiSelection = 'home';
          else if (matchTeamName(cleanTeam, [matchedAway])) oddsApiSelection = 'away';
          oddsApiMarket = 'series_spread';
        } else if (sel.marketType === 'series_total') {
          // Series-total: over/under on total games played in the series.
          // Pricer uses home+away team names (from lineInfo) + line +
          // over/under to query DK.
          oddsApiSelection = sel.selection; // 'over' or 'under'
          oddsApiMarket = 'series_total';
        } else if (sel.marketType === 'moneyline') {
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
        } else if (FIRST_HALF_MARKET_TYPES.includes(sel.marketType)) {
          // First Half (NBA) — same selection logic as full-game for h2h/spreads/totals
          if (sel.marketType.includes('moneyline')) {
            if (matchTeamName(sel.teamName, [matchedHome])) oddsApiSelection = 'home';
            else if (matchTeamName(sel.teamName, [matchedAway])) oddsApiSelection = 'away';
          } else if (sel.marketType.includes('spread')) {
            if (matchTeamName(sel.teamName, [matchedHome])) {
              oddsApiSelection = 'home';
            } else if (matchTeamName(sel.teamName, [matchedAway])) {
              oddsApiSelection = 'away';
            }
          } else if (sel.marketType.includes('total')) {
            oddsApiSelection = sel.selection; // 'over' or 'under'
          }
        }

        if (!oddsApiSelection || !oddsApiMarket) {
          if (isGolfSport) {
            log.warn('Lines', `[golf-debug] Skipping selection: team="${sel.teamName}" market=${sel.marketType} oddsApiSelection=${oddsApiSelection} oddsApiMarket=${oddsApiMarket}`);
            golfTrace.selectionsSkipped++;
          }
          continue;
        }

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
        // Golf matchups: parse the round from the PX event name
        // ("R1 RBC Heritage" → round 1; no R-prefix → tournament). Use
        // the round-aware cache accessor so we price a round RFQ against
        // round odds, not against the tournament-long h2h.
        let oddsEvt;
        let golfRoundNum = null;
        let golfMatchupType = null;
        if (sportKey === 'golf_matchups') {
          const nameRoundMatch = /\bR(?:ound\s*)?([1-4])\b/i.exec(event.name || '');
          golfRoundNum = nameRoundMatch ? parseInt(nameRoundMatch[1], 10) : null;
          golfMatchupType = golfRoundNum ? 'round' : 'tournament';
          oddsEvt = oddsFeed.getGolfMatchupEvent(matchedHome, matchedAway, golfRoundNum);
          // If the round-specific lookup failed but a tournament entry
          // exists (or vice versa), don't silently fall through to the
          // wrong-type entry. Leave oddsEvt null so the line is skipped.
        } else {
          oddsEvt = oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway, pxScheduled);
        }
        const startTime = event.scheduled || oddsEvt?.commenceTime || null;

        if (isGolfSport) golfTrace.linesRegistered++;
        lineIndex[sel.lineId] = {
          sport: sportKey,
          pxEventId: event.event_id,
          pxEventName: event.name,
          marketType: sel.marketType,
          marketName: market.name,
          isDNB,
          selection: oddsApiSelection,
          teamName: resolveDisplayTeamName(sel, matchedHome, matchedAway),
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
          roundNum: golfRoundNum ?? oddsEvt?.roundNum ?? null,
          matchupType: golfMatchupType ?? oddsEvt?.matchupType ?? null,
        };
        _trackPrimaryForIndex(lineIndex[sel.lineId]);
      }
    }

    // ----- PRE-SEED PLAYER PROPS -----
    // PX returns prop markets in fetchMarkets, but the mainMarkets filter
    // above excludes them (gametype only). Without pre-seed, props only
    // register via resolveUnknownLine when bettors RFQ specific players —
    // and most RFQs decline as "unknown legs" before that bridge fires
    // (we caught 106K such declines/day). Pre-seeding mirrors the
    // on-demand bridge at seed time so all eligible props live in the
    // index from boot, converting unknown-legs declines into real
    // priced/declined-with-fair-prob outcomes.
    //
    // Cost: ~1 TOA per-event-per-market call per refresh cycle on top of
    // the existing fetch — within Hobby quota at typical volume. Each
    // call's response is cached so multi-player markets only fetch once.
    try {
      const propAllowlist = (config.pricing && config.pricing.propLaunchAllowlist) || new Set();
      if (propAllowlist.size > 0 && (matchedHome && matchedAway)) {
        const ws = _getWsModule();
        const minBooks = (config.pricing && config.pricing.propMinBooksWithBothSides) || 3;
        const trustedSet = (config.pricing && config.pricing.propTrustedSingleBooks) || [];
        for (const market of markets) {
          if (!market || !market.name) continue;
          let propType = null;
          let toaMarketKey = null;
          if (ws) {
            if (sportKey.includes('basketball')) {
              propType = ws._classifyNbaProp(market.name);
              toaMarketKey = _NBA_PROP_TO_TOA_MARKET[propType];
            } else if (sportKey.includes('hockey')) {
              propType = ws._classifyNhlProp(market.name);
              toaMarketKey = _NHL_PROP_TO_TOA_MARKET[propType];
            } else if (sportKey === 'baseball_mlb') {
              propType = ws._classifyMlbProp(market.name);
              toaMarketKey = _MLB_PROP_TO_TOA_MARKET[propType];
            }
          }
          if (!propType || !toaMarketKey) continue;
          if (!propAllowlist.has(sportKey + '.' + propType)) continue;
          const playerName = ws ? ws._extractPlayerNameFromPropMarket(market.name) : null;
          if (!playerName) continue;

          // Parse PX selections (over + under for this player at the line).
          let parsedProp = [];
          try { parsedProp = px.parseMarketSelections(market) || []; } catch { continue; }
          if (parsedProp.length === 0) continue;
          // Use the first selection's line value for the TOA lookup; all
          // sides on a given market share the same line.
          const sampleLine = parsedProp.find(s => s.line != null)?.line ?? null;

          let lookup = null;
          try {
            lookup = await oddsFeed.lookupTheOddsApiPlayerProp(
              sportKey, toaMarketKey,
              { homeTeam: matchedHome, awayTeam: matchedAway, startTime: event.scheduled || null },
              playerName, sampleLine,
            );
          } catch (err) {
            log.debug('Lines', `Pre-seed prop lookup error for ${playerName} ${propType}: ${err.message}`);
            // Fall through to DK scraper — don't continue here
          }

          // DK scraper fallback: when TOA returns no/insufficient data,
          // hit the DK player-prop scraper cache. Operator directive
          // 2026-05-03: every prop type in the allowlist must have a
          // scraper backstop. Same pattern as the MLB F5 DK scraper —
          // single-book DK is treated as authoritative for the prop
          // since DK's player-prop coverage is the broadest in the
          // industry. The DK scraper IS lazy-loaded the first time —
          // first call per refresh cycle takes ~20-30s but every
          // subsequent prop in the same cycle reuses the cached scrape.
          const toaInsufficient = !lookup
            || lookup.fairProbOver == null
            || lookup.fairProbUnder == null
            || ((lookup.booksWithBothSides || 0) < minBooks
                && !((lookup.books || []).some(b => trustedSet.includes(String(b).toLowerCase()))));
          if (toaInsufficient) {
            try {
              const dk = require('./dk-scraper');
              // Trigger a scrape if no cache yet — only on first fallback per cycle
              if (typeof dk.fetchDkPlayerProps === 'function') {
                // Fire-and-await: we want the data this cycle. The 15-min
                // cache TTL inside the scraper means subsequent calls
                // reuse the same scrape result.
                await dk.fetchDkPlayerProps(sportKey).catch((e) => {
                  log.debug('Lines', `DK ${sportKey} player-prop scrape failed: ${e.message}`);
                });
              }
              const dkHit = dk.lookupDkPlayerPropFairProb(sportKey, propType, playerName, sampleLine);
              if (dkHit && dkHit.fairProbOver != null && dkHit.fairProbUnder != null) {
                lookup = dkHit;
              }
            } catch (err) {
              log.debug('Lines', `DK player-prop fallback error for ${playerName} ${propType}: ${err.message}`);
            }
          }
          if (!lookup || lookup.fairProbOver == null || lookup.fairProbUnder == null) continue;
          const both = lookup.booksWithBothSides || 0;
          const trustedAlone = both === 1 && (lookup.books || []).some(b => trustedSet.includes(String(b).toLowerCase()));
          if (both < minBooks && !trustedAlone) continue;

          // Register BOTH sides — bettors will RFQ either over or under
          // and both lineIds need to be in the index ahead of time.
          for (const sel of parsedProp) {
            if (!sel.lineId) continue;
            if (sel.selection !== 'over' && sel.selection !== 'under') continue;
            const fairProb = sel.selection === 'over' ? lookup.fairProbOver : lookup.fairProbUnder;
            lineIndex[sel.lineId] = {
              sport: sportKey,
              pxEventId: event.event_id,
              pxEventName: event.name,
              marketType: 'player_' + propType,
              marketName: market.name,
              selection: sel.selection,
              teamName: playerName,
              line: sel.line,
              homeTeam: matchedHome,
              awayTeam: matchedAway,
              oddsApiSport: sportKey,
              oddsApiMarket: toaMarketKey,
              oddsApiSelection: sel.selection,
              startTime: event.scheduled || null,
              playerName,
              propType,
              fairProb,
              fairProbOver: lookup.fairProbOver,
              fairProbUnder: lookup.fairProbUnder,
              booksWithBothSides: lookup.booksWithBothSides,
              propBooks: lookup.books,
              propSource: 'theoddsapi',
              propFetchedAt: lookup.fetchedAt || Date.now(),
            };
            totalLines++;
            matchedLines++;
          }
        }
      }
    } catch (err) {
      log.warn('Lines', `Pre-seed props pass error for ${event.name}: ${err.message}`);
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
    golfTrace,
  };

  log.info('Lines', `=== Seed complete: ${events.length} events, ${totalLines} lines parsed, ${matchedLines} matched, ${lineIds.length} registered ===`);

  // Persist lineIndex to Supabase so historical line_ids survive restarts
  db.saveLineCache(lineIndex).catch(err => {
    log.warn('Lines', `saveLineCache failed: ${err.message}`);
  });

  // Mark cold start complete — subsequent refreshLines cycles skip
  // Supabase hydration so the per-sport stale-event cutoff in seed
  // stays authoritative.
  _hasSeededOnce = true;

  return lastSeedStats;
}

// ---------------------------------------------------------------------------
// LOOKUPS
// ---------------------------------------------------------------------------

function __debugGetLineIndex() {
  return lineIndex;
}

function lookupLine(lineId) {
  const info = lineIndex[lineId];
  if (!info) return null;
  // Parse startTime once per line lifetime. Hot-path callers (shouldDecline,
  // priceParlay) read startTimeMs directly — avoids re-parsing the ISO string
  // on every RFQ. `undefined` = never computed, `null` = missing startTime,
  // `NaN` = invalid, number = valid ms.
  if (info.startTimeMs === undefined) {
    info.startTimeMs = info.startTime ? Date.parse(info.startTime) : null;
  }
  return info;
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
    _trackPrimaryForIndex(cached);
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

  // Clear stale failure state at the START of each call. _lastFailure is
  // a function-level singleton: it's set on every failure path below but
  // was never reset, so callers reading it after a SUCCESSFUL resolve
  // would see the previous call's failure (potentially from a different
  // parlay's leg). Caused 2026-04-25 cross-attribution where the
  // /decline-audit drill-down showed marketName values from one leg
  // attributed to another. Clearing here makes lineId-guarded reads
  // reliable; readers that don't gate on lineId now also get null
  // instead of stale state.
  resolveUnknownLine._lastFailure = null;

  // Sample log: capture RFQ leg shape (first 20 unknown legs only)
  if (!resolveUnknownLine._sampleCount) resolveUnknownLine._sampleCount = 0;
  if (resolveUnknownLine._sampleCount < 20 && typeof rfqLeg === 'object') {
    resolveUnknownLine._sampleCount++;
    log.debug('Lines', `RFQ leg sample #${resolveUnknownLine._sampleCount}: keys=${Object.keys(rfqLeg).join(',')} line=${rfqLeg.line} origin=${rfqLeg.origin_market_line}`);
  }

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

  // Determine sport key. Generic catch-all keys (e.g. 'soccer') are
  // sorted LAST so league-specific keys (soccer_epl, soccer_germany_*)
  // win the matching race when both have an entry for the same event.
  // See seedAllLines for the full rationale.
  const _isGenericKey = (k) => !k.includes('_') || k === 'mma_mixed_martial_arts' || k === 'boxing_boxing';
  const possibleSportKeys = Object.entries(config.sportNameMap)
    .filter(([k, v]) => v === event.sportName)
    .map(([k]) => k)
    .sort((a, b) => (_isGenericKey(a) ? 1 : 0) - (_isGenericKey(b) ? 1 : 0));
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
    // Series-winner events (NHL-style separate PX event named
    // "Series Winner - X vs Y") won't match odds-feed game events
    // because no game event exists for just the series. Skip the
    // match requirement and use competitor names directly — pricer
    // will route this leg to the DK scraper cache.
    if (/^\s*series\s*winner\b/i.test(event.name || '')) {
      matchedHome = homeComp.name;
      matchedAway = awayComp.name;
      sportKey = possibleSportKeys[0];
    } else {
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
      const SUPPORTED_TYPES = ['moneyline', 'spread', 'total', 'team_total', 'btts', 'both_teams_to_score', 'double_chance', 'series_winner', 'series_spread', 'series_total', 'sup_moneyline', ...F5_MARKET_TYPES, ...FIRST_HALF_MARKET_TYPES];
      // Series markets (winner/spread/total) are structurally
      // moneyline/spread/total but named "Series Winner/Spread/Total
      // Games". resolveUnknownLine accepts the regular PX types and
      // detects by name pattern below.
      const seriesWinnerNamePat = /\bseries\s*winner\b/i;
      const seriesSpreadNamePat = /\bseries\s*(spread|handicap)\b|\bseries\b[^.]*\bspread\b/i;
      const seriesTotalNamePat  = /\bseries\s*total\b|\btotal\s*games\b|\bseries\b[^.]*\btotal\b/i;
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
      // Track whether the line_id was found in ANY PX market (even if we
      // couldn't use it — e.g. out-of-bounds line, player prop total).
      // When set, virtual registration is blocked: PX already told us what
      // this line_id is, and we must not override that with heuristics.
      let lineFoundInPxMarket = false;
      // F5 name pattern — detect F5 markets by name since PX uses
      // market.type='spread'/'total' for them (distinguishes only via name)
      const f5NamePat = /1st[-\s]?5th.*inning|first\s*5\s*inning|first\s*five\s*innings|f5\b/i;
      const h1NamePat = /first\s*half|1st\s*half/i;
      // Sub-game name pattern — halves, quarters, periods, innings.
      // These markets come through with supported types (spread/total/moneyline)
      // but the market.name identifies them as sub-game. We must NOT register
      // them as full-game markets because their lines can coincidentally match
      // full-game primaries (e.g. NBA 1st-half spread 5.5 vs full-game 5.5),
      // leading to mispriced offers. Mirror the seed-time excludePatterns.
      // F5 is exempt (handled via its own marketType above).
      // H1 (first half) is also exempt — handled via FIRST_HALF_MARKET_TYPES.
      const subGameNamePat = /second half|2nd half|first quarter|1st quarter|2nd quarter|3rd quarter|4th quarter|1st period|2nd period|3rd period|1st inning|2nd inning|3rd inning|overtime/i;
      // Player prop name pattern: markets named after a player (e.g.
      // "LeBron James Made Threes", "Patrick Mahomes Passing Yards") that
      // PX tags with a supported type like "total" or "spread". These MUST
      // NOT be treated as full-game markets. Pattern matches common prop
      // keywords that appear alongside player names.
      const playerPropNamePat = /\b(?:made|attempted|assists|rebounds|steals|blocks|turnovers|points|passing|rushing|receiving|tackles|sacks|completions|interceptions|touchdowns|yards|shots|saves|hits|runs|rbis?|strikeouts|walks|home runs|goals|pim|faceoffs?|aces|double faults|games won|milestones|pitching|batting|earned|fantasy|doubles?|triples?|errors|stolen bases?|outs recorded|innings pitched|at bats?|put outs?|fouls|cards|bookings|offsides?|crosses|clearances|throw.?ins?)\b/i;
      for (const market of markets || []) {
        if (!SUPPORTED_TYPES.includes(market.type)) continue;

        // M1: Pitcher strikeouts prop. PX tags these as type='total' but
        // the market name reveals it's a player K prop. Route to the prop
        // fair-prob lookup (services/odds-feed.js lookupPlayerStrikeoutProp)
        // instead of the standard game-total resolver. Falls through to
        // the next market if the lineId isn't in this one.
        //
        // After M1 lands, shouldDecline gates these legs with a temporary
        // 'prop_pricing_not_ready' reason. Vig structure + decline rules
        // come in M2/M3 before live quoting.
        if (market.type === 'total' && isPitcherStrikeoutMarket(market.name || '')) {
          const parsedK = px.parseMarketSelections(market);
          const matchingK = parsedK.find(s => s.lineId === lineId);
          if (matchingK) {
            const playerName = extractPitcherNameFromKMarket(market.name);
            if (!playerName) {
              log.warn('Lines', `K-prop name extract failed: "${market.name}" (lineId ${lineId})`);
              resolveUnknownLine._lastFailure = {
                lineId,
                reason: 'k_prop_name_extract_failed',
                marketType: 'player_strikeouts',
                marketName: market.name,
                sport: sportKey,
                eventName: event.name,
              };
              return null;
            }
            const eventCtx = {
              homeTeam: matchedHome,
              awayTeam: matchedAway,
              startTime: event.scheduled || null,
            };
            // Try SharpAPI first (sync cache hit), fall back to TOA when
            // the SharpAPI result is either missing OR low-confidence.
            // Previously we only escalated on missing-fair (lookup
            // returned null fair_prob_over). That meant alt K-prop lines
            // where SharpAPI had only a single non-trusted book (e.g.
            // BetRivers alone on Suarez 3.5) silently used SharpAPI's
            // narrow result and got rejected downstream by shouldDecline
            // rule (b), even though TOA could have returned 5+ books for
            // the same line. Now we escalate when book count is < 2 AND
            // the single book isn't on the trusted-alone list.
            let lookup = oddsFeed.lookupPlayerStrikeoutProp(
              sportKey, eventCtx, playerName, matchingK.line,
            );
            let propSource = 'sharpapi';
            const usableFair = (l) => l && l.fairProbOver != null && l.fairProbUnder != null;
            const trustedSet = (config.pricing && config.pricing.propTrustedSingleBooks) || [];
            const isHighConfidence = (l) => {
              if (!usableFair(l)) return false;
              const both = l.booksWithBothSides || 0;
              if (both >= 2) return true;
              const books = l.books || [];
              return both === 1 && books.some(b => trustedSet.includes(String(b).toLowerCase()));
            };
            if (!isHighConfidence(lookup)) {
              const toa = await oddsFeed.lookupPlayerStrikeoutPropFromTheOddsApi(
                sportKey, eventCtx, playerName, matchingK.line,
              );
              // Use TOA if it returned anything usable. Special case: if
              // SharpAPI was usable (not high-confidence but has fair
              // probs) AND TOA also returned data, prefer whichever has
              // more books — TOA usually wins for alt lines (5-8 books vs
              // 1) but stick with SharpAPI if TOA somehow gave us less.
              if (toa && usableFair(toa)) {
                const sharpBoth = (lookup && lookup.booksWithBothSides) || 0;
                const toaBoth = toa.booksWithBothSides || 0;
                if (!usableFair(lookup) || toaBoth >= sharpBoth) {
                  lookup = toa;
                  propSource = 'theoddsapi';
                }
              }
            }
            const fairProb = matchingK.selection === 'over'
              ? (lookup && lookup.fairProbOver != null ? lookup.fairProbOver : null)
              : (lookup && lookup.fairProbUnder != null ? lookup.fairProbUnder : null);
            foundInfo = {
              sport: sportKey,
              pxEventId: eventId,
              pxEventName: event.name,
              marketType: 'player_strikeouts',
              marketName: market.name,
              selection: matchingK.selection,
              teamName: playerName, // dashboards display "team" — use pitcher name
              line: matchingK.line,
              homeTeam: matchedHome,
              awayTeam: matchedAway,
              oddsApiSport: sportKey,
              oddsApiMarket: 'player_strikeouts',
              oddsApiSelection: matchingK.selection,
              startTime: event.scheduled || null,
              onDemand: true,
              // Prop-specific metadata
              playerName,
              fairProb,
              fairProbOver: lookup && lookup.fairProbOver != null ? lookup.fairProbOver : null,
              fairProbUnder: lookup && lookup.fairProbUnder != null ? lookup.fairProbUnder : null,
              booksWithBothSides: lookup && lookup.booksWithBothSides != null ? lookup.booksWithBothSides : null,
              propBooks: lookup && lookup.books ? lookup.books : null,
              propSource,
              propFetchedAt: lookup && lookup.fetchedAt ? lookup.fetchedAt : Date.now(),
              propMatchError: lookup && lookup.error ? lookup.error : null,
              propMatchStages: lookup && lookup.stages ? lookup.stages : null,
            };
            lineFoundInPxMarket = true;
            break; // exit markets loop — foundInfo will be stored at line ~1680
          }
          // lineId not in this K-prop market — continue to next market
          continue;
        }

        // Reject sub-game markets (halves/quarters/periods) by name BEFORE
        // the bounds check. F5 is exempt because it has its own marketType.
        const isF5ByName = f5NamePat.test(market.name || '');
        const isH1ByName = h1NamePat.test(market.name || '') || FIRST_HALF_MARKET_TYPES.includes(market.type);
        if (!isF5ByName && !isH1ByName && subGameNamePat.test(market.name || '')) {
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
        // Reject player prop markets — PX often tags these with a supported
        // type (e.g. "total") but the name reveals it's a player stat market.
        // Must check BEFORE the per-selection loop so we don't register a
        // "LeBron James Made Threes O 1.5" as a game total or alt spread.
        //
        // IMPORTANT: Skip this check for known full-game market names like
        // "Total Points", "Total Runs", "Total Goals", "Point Spread", etc.
        // These contain generic words (points, runs, goals) that also appear
        // in playerPropNamePat, causing thousands of false-positive declines
        // on legitimate alt spread/total lines.
        const fullGameNamePat = /^(?:total|spread|moneyline|run line|puck line|point spread|alternate|alt |game spread|team total|draw no bet|both teams|double chance)/i;
        if (playerPropNamePat.test(market.name || '') && !fullGameNamePat.test(market.name || '')) {
          const parsedProp = px.parseMarketSelections(market);
          if (parsedProp.some(s => s.lineId === lineId)) {
            // Phase-2 prop launch bridge: classify the prop, check the
            // launch allowlist, and try the live TOA lookup before
            // falling through to the existing decline path. Empty
            // allowlist = identical behavior to before (decline as
            // player_prop_market). When allowlist is populated, eligible
            // prop legs become quotable here.
            //
            // Use module-cached websocket reference + lifted prop-type
            // maps (see top of file). Saves ~30-80μs per prop RFQ by
            // skipping the per-call require lookup + object literal.
            let propType = null;
            let toaMarketKey = null;
            let nameExtractor = null;
            const ws = _getWsModule();
            if (ws) {
              nameExtractor = ws._extractPlayerNameFromPropMarket;
              if (sportKey.includes('basketball')) {
                propType = ws._classifyNbaProp(market.name);
                toaMarketKey = _NBA_PROP_TO_TOA_MARKET[propType];
              } else if (sportKey.includes('hockey')) {
                propType = ws._classifyNhlProp(market.name);
                toaMarketKey = _NHL_PROP_TO_TOA_MARKET[propType];
              } else if (sportKey === 'baseball_mlb') {
                // pitcher_strikeouts has its own dedicated bridge above
                // (services/odds-feed.js lookupPlayerStrikeoutProp + TOA
                // fallback). Other MLB hitter prop types — hits, home
                // runs, total bases, RBIs — flow through the generic
                // Phase-2 bridge using TOA's batter_* market keys.
                propType = ws._classifyMlbProp(market.name);
                toaMarketKey = _MLB_PROP_TO_TOA_MARKET[propType];
              }
            }
            const allowlist = (config.pricing && config.pricing.propLaunchAllowlist) || new Set();
            const allowKey = sportKey + '.' + propType;
            const allowed = propType && toaMarketKey && allowlist.has(allowKey);

            if (allowed) {
              const matchingProp = parsedProp.find(s => s.lineId === lineId);
              const playerName = nameExtractor ? nameExtractor(market.name) : null;
              if (matchingProp && playerName) {
                const eventCtx = {
                  homeTeam: matchedHome,
                  awayTeam: matchedAway,
                  startTime: event.scheduled || null,
                };
                let lookup = null;
                try {
                  lookup = await oddsFeed.lookupTheOddsApiPlayerProp(
                    sportKey, toaMarketKey, eventCtx, playerName, matchingProp.line,
                  );
                } catch (err) {
                  log.warn('Lines', `Phase-2 prop lookup error for ${playerName} ${propType} ${matchingProp.line}: ${err.message}`);
                }
                const minBooks = (config.pricing && config.pricing.propMinBooksWithBothSides) || 3;
                const usable = lookup
                  && lookup.fairProbOver != null
                  && lookup.fairProbUnder != null
                  && (lookup.booksWithBothSides || 0) >= minBooks;
                if (usable) {
                  const fairProb = matchingProp.selection === 'over'
                    ? lookup.fairProbOver
                    : lookup.fairProbUnder;
                  foundInfo = {
                    sport: sportKey,
                    pxEventId: eventId,
                    pxEventName: event.name,
                    marketType: 'player_' + propType,
                    marketName: market.name,
                    selection: matchingProp.selection,
                    teamName: playerName, // dashboards display "team" — use player name
                    line: matchingProp.line,
                    homeTeam: matchedHome,
                    awayTeam: matchedAway,
                    oddsApiSport: sportKey,
                    oddsApiMarket: toaMarketKey,
                    oddsApiSelection: matchingProp.selection,
                    startTime: event.scheduled || null,
                    onDemand: true,
                    playerName,
                    propType,
                    fairProb,
                    fairProbOver: lookup.fairProbOver,
                    fairProbUnder: lookup.fairProbUnder,
                    booksWithBothSides: lookup.booksWithBothSides,
                    propBooks: lookup.books,
                    propSource: 'theoddsapi',
                    propFetchedAt: lookup.fetchedAt || Date.now(),
                  };
                  lineFoundInPxMarket = true;
                  log.info('Lines', `Phase-2 prop registered: ${playerName} ${propType} ${matchingProp.selection} ${matchingProp.line} (${lookup.booksWithBothSides} books, fair=${fairProb.toFixed(4)})`);
                  break; // exit markets loop — foundInfo will be stored downstream
                }
                // Lookup failed or insufficient books — log + fall through to decline
                const reason = !lookup ? 'lookup_null'
                  : lookup.error ? lookup.error
                  : (lookup.booksWithBothSides || 0) < minBooks ? `insufficient_books(${lookup.booksWithBothSides || 0}<${minBooks})`
                  : 'no_fair_prob';
                log.info('Lines', `Phase-2 prop declined for ${playerName} ${propType} ${matchingProp.line}: ${reason}`);
              }
            }

            log.info('Lines', `Declined player prop market: ${market.type} / "${market.name}" (${event.name})`);
            resolveUnknownLine._lastFailure = {
              lineId,
              reason: 'player_prop_market',
              marketType: market.type,
              marketName: market.name,
              sport: sportKey,
              eventName: event.name,
            };
            return null;
          }
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
        const mName = market.name || '';
        const isSeriesWinnerMarket = seriesWinnerNamePat.test(mName);
        const isSeriesSpreadMarket = seriesSpreadNamePat.test(mName);
        const isSeriesTotalMarket  = !isSeriesSpreadMarket && seriesTotalNamePat.test(mName);
        const isSeriesMarket = isSeriesWinnerMarket || isSeriesSpreadMarket || isSeriesTotalMarket;
        for (const sel of parsed) {
          if (sel.lineId !== lineId) continue;
          // Retag series selections so the pricer routes them to DK.
          if (isSeriesWinnerMarket && sel.marketType === 'moneyline') {
            sel.marketType = 'series_winner';
          } else if (isSeriesSpreadMarket && sel.marketType === 'spread') {
            sel.marketType = 'series_spread';
          } else if (isSeriesTotalMarket && sel.marketType === 'total' && sportKey !== 'tennis') {
            // Tennis "Total Games" → keep as 'total' (match-level). See
            // matching carve-out at line 902 — same reason: tennis match
            // totals route to the standard 'totals' cache, not the series
            // cache (which has no tennis data).
            sel.marketType = 'series_total';
          }
          // Per-selection bound check: rejects the specific out-of-range
          // alt line the RFQ asked about (e.g. Rangers -6.5) while leaving
          // siblings like Rangers -1.5 intact for future resolves. Series
          // markets bypass — series spreads/totals intentionally use lines
          // outside normal full-game bounds.
          if ((sel.marketType === 'total' || sel.marketType === 'spread') && !isF5Market && !isH1ByName && !isSeriesMarket) {
            if (!isValidFullGameLine(sportKey, sel.marketType, sel.line)) {
              log.debug('Lines', `resolveUnknownLine: rejecting out-of-bounds selection ${sel.marketType} ${sel.line} for ${sportKey}: ${market.name}`);
              lineFoundInPxMarket = true; // Line exists in PX — block virtual registration
              resolveUnknownLine._lastFailure = { lineId, reason: 'out_of_bounds_line', sport: sportKey, marketType: sel.marketType, line: sel.line, marketName: market.name };
              continue;
            }
          }
          // Determine oddsApiSelection
          let oddsApiSelection = null;
          let oddsApiMarket = MARKET_TYPE_MAP[sel.marketType];
          if (sel.marketType === 'series_winner') {
            const cleanTeam = (sel.teamName || '').replace(/\s*\(series\)\s*/ig, '').trim();
            if (matchTeamName(cleanTeam, [matchedHome])) oddsApiSelection = 'home';
            else if (matchTeamName(cleanTeam, [matchedAway])) oddsApiSelection = 'away';
            oddsApiMarket = 'series_winner';
          } else if (sel.marketType === 'series_spread') {
            const cleanTeam = (sel.teamName || '').replace(/\s*\(series\)\s*/ig, '').trim();
            if (matchTeamName(cleanTeam, [matchedHome])) oddsApiSelection = 'home';
            else if (matchTeamName(cleanTeam, [matchedAway])) oddsApiSelection = 'away';
            oddsApiMarket = 'series_spread';
          } else if (sel.marketType === 'series_total') {
            oddsApiSelection = sel.selection; // 'over' or 'under'
            oddsApiMarket = 'series_total';
          } else if (sel.marketType === 'moneyline') {
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
          } else if (FIRST_HALF_MARKET_TYPES.includes(sel.marketType)) {
            if (sel.marketType.includes('moneyline')) {
              if (matchTeamName(sel.teamName, [matchedHome])) oddsApiSelection = 'home';
              else if (matchTeamName(sel.teamName, [matchedAway])) oddsApiSelection = 'away';
            } else if (sel.marketType.includes('spread')) {
              if (matchTeamName(sel.teamName, [matchedHome])) {
                oddsApiSelection = 'home';
              } else if (matchTeamName(sel.teamName, [matchedAway])) {
                oddsApiSelection = 'away';
              }
            } else if (sel.marketType.includes('total')) {
              oddsApiSelection = sel.selection;
            }
          }
          if (!oddsApiSelection || !oddsApiMarket) {
            lineFoundInPxMarket = true; // Line exists in PX — block virtual registration
            continue;
          }

          const pxTime = event.scheduled || null;
          // Golf matchups: route through the round-aware accessor so we
          // don't confuse a round RFQ with a tournament matchup.
          let oddsEvt;
          let golfRoundNum = null;
          let golfMatchupType = null;
          if (sportKey === 'golf_matchups') {
            const nameRoundMatch = /\bR(?:ound\s*)?([1-4])\b/i.exec(event.name || '');
            golfRoundNum = nameRoundMatch ? parseInt(nameRoundMatch[1], 10) : null;
            golfMatchupType = golfRoundNum ? 'round' : 'tournament';
            oddsEvt = oddsFeed.getGolfMatchupEvent(matchedHome, matchedAway, golfRoundNum);
          } else {
            oddsEvt = oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway, pxTime);
          }
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
            teamName: resolveDisplayTeamName(sel, matchedHome, matchedAway),
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
            roundNum: golfRoundNum ?? oddsEvt?.roundNum ?? null,
            matchupType: golfMatchupType ?? oddsEvt?.matchupType ?? null,
          };
          break;
        }
        if (foundInfo) break;
      }

      if (!foundInfo) {
        // Log what market types we DID find for this event (helps diagnose player props etc.)
        const foundTypes = (markets || []).map(m => m.type).filter(Boolean);
        const marketNames = (markets || []).map(m => m.name).filter(Boolean).slice(0, 5);

        // If the line_id was found in a PX market but rejected (out-of-bounds,
        // unmappable selection, player prop total, etc.), do NOT fall through
        // to virtual registration. PX already told us what this line is — the
        // heuristic would misidentify it (e.g. player prop O 1.5 → alt spread).
        if (lineFoundInPxMarket) {
          log.info('Lines', `Blocking virtual registration for line ${lineId}: found in PX market but rejected (${resolveUnknownLine._lastFailure?.reason || 'unknown'}). Event: ${event.name}`);
          return null;
        }

        // --- Virtual registration fallback ---
        // PX fetchMarkets often omits the specific alt-line the RFQ referenced.
        // If we have the event matched (home/away teams + sport) AND the RFQ leg
        // carries a numeric line, we can infer market type from context and
        // register a "virtual" entry so the pricer can fetch alt-line odds on
        // demand from The Odds API.
        const rfqLine = rfqLeg.line != null ? Number(rfqLeg.line) : null;
        if (matchedHome && matchedAway && rfqLine != null && !isNaN(rfqLine)) {
          // Determine market type from what we know about the event and the
          // line value. Strategy: look at existing registered lines for this
          // event to decide if this is a spread or total.
          const existingForEvent = Object.values(lineIndex).filter(
            li => li.pxEventId === eventId
          );
          const hasSpread = existingForEvent.some(li => li.marketType === 'spread');
          const hasTotal = existingForEvent.some(li => li.marketType === 'total');
          const primarySpread = existingForEvent.find(li => li.marketType === 'spread');
          const primaryTotal = existingForEvent.find(li => li.marketType === 'total');

          // Heuristic: totals have large abs values (e.g. 8.5 runs, 220.5 pts),
          // spreads have small abs values. Use sport-aware thresholds.
          const absLine = Math.abs(rfqLine);
          let inferredType = null;
          let inferredSelection = null;
          let inferredTeam = null;
          let inferredOddsMarket = null;

          // If we have a primary spread registered, compare magnitude.
          // Require the line to be within a sport-aware distance of the
          // primary spread. Without this, player prop lines (e.g. pitcher
          // K's O/U 4.5) that happen to fall within MAX_SPREAD_BY_SPORT
          // get misidentified as alt spreads. PX fetchMarkets often omits
          // player prop markets entirely, so the name-based check above
          // never fires — virtual registration is the last line of defense.
          if (hasSpread && primarySpread) {
            const primaryAbsSpread = Math.abs(primarySpread.line || 0);
            const maxSpread = MAX_SPREAD_BY_SPORT[sportKey] || 15;
            // Max deviation from primary: sport-aware. NBA/NCAAB alt spreads
            // can deviate ±15+, but MLB/NHL/soccer rarely deviate more than
            // ±3 from the primary run/puck line.
            // Only basketball and football get virtual alt spread registration
            // by *distance*. MLB uses a discrete allowlist instead.
            const MAX_ALT_DEVIATION = {
              'basketball_nba': 20, 'basketball_ncaab': 20, 'basketball_wnba': 20,
              'americanfootball_nfl': 15, 'americanfootball_ncaaf': 15,
            };
            const maxDeviation = MAX_ALT_DEVIATION[sportKey] ?? 0;
            const deviation = Math.abs(absLine - primaryAbsSpread);
            // MLB-specific: allow values from the discrete allowlist
            // (default ±0.5 and ±1.5). Pricer enforces book coverage on
            // non-primary alts; we just need to virtually register so
            // the leg can reach the pricer in the first place.
            let mlbAllowed = false;
            if (sportKey === 'baseball_mlb') {
              const allowed = config.pricing.mlbAllowedRunLines || [0.5, 1.5];
              mlbAllowed = allowed.some(v => Math.abs(absLine - v) < 0.001);
            }
            if ((absLine <= maxSpread && deviation <= maxDeviation) || mlbAllowed) {
              inferredType = 'spread';
              // For spreads: negative line = favorite, positive = underdog.
              // Map to home/away using the primary spread's polarity.
              if (rfqLine < 0) {
                // Favorite side — same team as whoever has negative primary spread
                inferredSelection = primarySpread.oddsApiSelection || 'home';
                inferredTeam = primarySpread.teamName;
              } else {
                // Underdog side — opposite of the favorite
                const oppSel = primarySpread.oddsApiSelection === 'home' ? 'away' : 'home';
                inferredSelection = oppSel;
                inferredTeam = oppSel === 'home' ? matchedHome : matchedAway;
              }
              inferredOddsMarket = 'spreads';
            }
          }

          // If we didn't infer spread, check if it's a total
          if (!inferredType && hasTotal && primaryTotal) {
            const primaryAbsTotal = Math.abs(primaryTotal.line || 0);
            // Sport-aware tolerance:
            //   MLB: ±config.pricing.mlbAltTotalMaxDistance (default 1.5)
            //   Other sports: legacy 0.3x–2.0x heuristic
            let withinTolerance;
            if (sportKey === 'baseball_mlb') {
              const maxDist = config.pricing.mlbAltTotalMaxDistance || 1.5;
              withinTolerance = Math.abs(absLine - primaryAbsTotal) <= maxDist + 0.001;
            } else {
              withinTolerance = absLine >= primaryAbsTotal * 0.3 && absLine <= primaryAbsTotal * 2.0;
            }
            if (withinTolerance) {
              // Determine over/under by walking ALL of PX's markets for this
              // event (not gated by SUPPORTED_TYPES) and finding the lineId.
              // PX may register alt-total markets under a non-supported
              // market.type (e.g. 'alt_total') that the regular seed/walk
              // skips — but parseMarketSelections still extracts the side.
              let altSelection = null;
              for (const market of markets || []) {
                try {
                  const sels = px.parseMarketSelections(market);
                  const match = sels.find(s => s.lineId === lineId);
                  if (match && (match.selection === 'over' || match.selection === 'under')) {
                    altSelection = match.selection;
                    break;
                  }
                } catch (_) { /* skip unparseable market */ }
              }
              if (altSelection) {
                inferredType = 'total';
                inferredSelection = altSelection;
                inferredOddsMarket = 'totals';
              } else {
                log.debug('Lines', `Alt-total virtual reg: cannot determine over/under for ${event.name} line ${rfqLine} — lineId not found in any PX market selection`);
              }
            }
          }

          // Fallback: if no primary data in lineIndex, check odds feed
          // for a primary spread and require proximity to it.
          if (!inferredType && !hasSpread && !hasTotal) {
            const maxSpread = MAX_SPREAD_BY_SPORT[sportKey] || 15;
            if (absLine <= maxSpread) {
              // Without a primary spread in lineIndex, consult odds feed.
              // Require the line to be within sport-aware deviation of the
              // odds feed's primary spread to avoid misidentifying player
              // props as alt spreads.
              const pxTime = event.scheduled || null;
              const oddsEvt = oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway, pxTime);
              if (oddsEvt?.markets?.spreads) {
                const primaryHomePoint = oddsEvt.markets.spreads.home?.point;
                if (primaryHomePoint != null) {
                  const MAX_ALT_DEV_FALLBACK = {
                    'basketball_nba': 20, 'basketball_ncaab': 20, 'basketball_wnba': 20,
                    'americanfootball_nfl': 15, 'americanfootball_ncaaf': 15,
                  };
                  const maxDev = MAX_ALT_DEV_FALLBACK[sportKey] ?? 0;
                  const primaryAbs = Math.abs(primaryHomePoint);
                  const dev = Math.abs(absLine - primaryAbs);
                  if (dev > maxDev) {
                    log.info('Lines', `Virtual registration blocked: line ${rfqLine} deviates ${dev.toFixed(1)} from primary spread ${primaryHomePoint} (max ${maxDev}) for ${sportKey}. Likely player prop.`);
                  } else {
                    inferredType = 'spread';
                    const homeIsFav = primaryHomePoint < 0;
                    if (rfqLine < 0) {
                      inferredSelection = homeIsFav ? 'home' : 'away';
                      inferredTeam = homeIsFav ? matchedHome : matchedAway;
                    } else {
                      inferredSelection = homeIsFav ? 'away' : 'home';
                      inferredTeam = homeIsFav ? matchedAway : matchedHome;
                    }
                    inferredOddsMarket = 'spreads';
                  }
                }
              }
            }
          }

          if (inferredType === 'spread' && inferredSelection && inferredTeam) {
            const pxTime = event.scheduled || null;
            const oddsEvt = oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway, pxTime);
            const startTime = event.scheduled || oddsEvt?.commenceTime || null;

            foundInfo = {
              sport: sportKey,
              pxEventId: eventId,
              pxEventName: event.name,
              marketType: 'spread',
              marketName: `Virtual Alt Spread ${rfqLine}`,
              selection: inferredSelection,
              teamName: inferredTeam,
              line: rfqLine,
              homeTeam: matchedHome,
              awayTeam: matchedAway,
              oddsApiSport: sportKey,
              oddsApiMarket: 'spreads',
              oddsApiSelection: inferredSelection,
              startTime,
              onDemand: true,
              virtualRegistration: true,
            };
            log.info('Lines', `Virtual registration: ${sportKey} spread ${inferredTeam} ${rfqLine} for ${event.name} (line_id ${lineId} not in PX markets)`);
          } else if (inferredType === 'total' && (inferredSelection === 'over' || inferredSelection === 'under')) {
            const pxTime = event.scheduled || null;
            const oddsEvt = oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway, pxTime);
            const startTime = event.scheduled || oddsEvt?.commenceTime || null;

            foundInfo = {
              sport: sportKey,
              pxEventId: eventId,
              pxEventName: event.name,
              marketType: 'total',
              marketName: `Virtual Alt Total ${Math.abs(rfqLine)} ${inferredSelection}`,
              selection: inferredSelection,
              teamName: null,
              line: Math.abs(rfqLine),
              homeTeam: matchedHome,
              awayTeam: matchedAway,
              oddsApiSport: sportKey,
              oddsApiMarket: 'totals',
              oddsApiSelection: inferredSelection,
              startTime,
              onDemand: true,
              virtualRegistration: true,
            };
            log.info('Lines', `Virtual registration: ${sportKey} alt-total ${inferredSelection} ${Math.abs(rfqLine)} for ${event.name}`);
          }
        }

        if (!foundInfo) {
          log.debug('Lines', `Could not locate line ${lineId} in event ${eventId} markets (types found: ${foundTypes.join(',')}; names: ${marketNames.join(', ')}). RFQ leg: line=${rfqLeg.line}, keys=${Object.keys(rfqLeg).join(',')}`);
          resolveUnknownLine._lastFailure = { lineId, reason: 'line_not_in_markets', eventName: event.name, sport: sportKey, marketTypesFound: foundTypes, marketNamesFound: marketNames };
          return null;
        }
      }

      // Add to index locally
      lineIndex[lineId] = foundInfo;
      _trackPrimaryForIndex(foundInfo);
      log.info('Lines', `On-demand registered ${sportKey}/${foundInfo.marketType} line for ${foundInfo.teamName} ${foundInfo.line != null ? foundInfo.line : ''} (${event.name})`);

      // Fire-and-forget PX registration — the RFQ we're responding to already
      // has the line_id, so we don't need to wait for PX to acknowledge
      px.registerSupportedLines([lineId]).catch(err => {
        log.warn('Lines', `PX registration of ${lineId} failed: ${err.message}`);
      });

      // Fire-and-forget JIT alt-line warm. The current RFQ is already being
      // priced (its leg resolved via the primary-cache path or will on-demand
      // fetch inline), but the NEXT RFQ touching this same event will get a
      // warm cache hit instead of paying an on-demand fetch. Deduped internally
      // by the JIT function's in-flight map + TTL check.
      oddsFeed.warmEventAltLinesJIT({
        sport: foundInfo.sport,
        homeTeam: foundInfo.homeTeam,
        awayTeam: foundInfo.awayTeam,
        commenceTime: foundInfo.startTime,
      }).catch(err => {
        log.debug('Lines', `JIT warm (resolveUnknown) swallowed error: ${err.message}`);
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
  for (const key of Object.keys(primaryByEvent)) {
    delete primaryByEvent[key];
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

/**
 * Find the primary total line for a given pxEventId. Returns the
 * absolute over/under value (e.g., 215.5 for an NBA game with O/U
 * 215.5). Used by the alt-total block for NBA: a leg's distance from
 * this value determines whether it's an allowed near-primary alt or
 * a banned far-out alt.
 *
 * "Primary" = the total line the line manager pre-registered from the
 * SharpAPI feed (onDemand=false). Excludes virtually-registered
 * (onDemand=true) entries. Returns null when no primary total is
 * registered for the event.
 *
 * Both over and under selections share the same line value, so we
 * just take the first non-onDemand total leg we find.
 */
function getPrimaryTotalLine(pxEventId) {
  if (pxEventId == null) return null;
  const slot = primaryByEvent[pxEventId];
  if (!slot) return null;
  // Prefer median of all seen totals when we have ≥3 lines for this
  // event. Median converges to the main even when PX seeded alts
  // before the main (the bug we fixed for spreads, applied here too
  // with a different — more robust — heuristic). Fall back to first-
  // seen for sparse events with <3 lines.
  const seenSet = slot.seenTotalLines;
  if (seenSet && seenSet.size >= 3) {
    const sorted = [...seenSet].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 === 0
      ? (sorted[mid - 1] + sorted[mid]) / 2
      : sorted[mid];
  }
  if (!slot.total) return null;
  const li = slot.total;
  if (li.line == null || !Number.isFinite(Number(li.line))) return null;
  return Math.abs(Number(li.line));
}

/**
 * Find the primary spread line for a given pxEventId, expressed in
 * home-team perspective (signed). Used by the alt-spread block for
 * NBA: a leg's distance from this value determines whether it's an
 * allowed near-primary alt or a banned far-out alt.
 *
 * "Primary" = the spread leg the line manager pre-registered from the
 * SharpAPI feed (onDemand=false). Excludes virtually-registered
 * (onDemand=true) entries that came from RFQ-driven on-demand fetches.
 *
 * Returns null when:
 *   - No spread line registered for this event yet
 *   - All registered spreads are onDemand=true (no primary anchor)
 *   - eventId is null/undefined
 *
 * For NBA games we expect exactly one primary spread per event; if
 * multiple are found (unusual), returns the first non-onDemand match.
 */
function getPrimarySpreadHomePoint(pxEventId) {
  if (pxEventId == null) return null;
  const slot = primaryByEvent[pxEventId];
  if (!slot || !slot.spread) return null;
  const li = slot.spread;
  if (li.line == null || !Number.isFinite(Number(li.line))) return null;
  const lineNum = Number(li.line);
  if (li.oddsApiSelection === 'home' || li.selection === 'home') return lineNum;
  if (li.oddsApiSelection === 'away' || li.selection === 'away') return -lineNum;
  log.debug('Lines', `getPrimarySpreadHomePoint: unknown selection on primary spread for event ${pxEventId} — assuming home-perspective. line=${lineNum}`);
  return lineNum;
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
  getPrimarySpreadHomePoint,
  getPrimaryTotalLine,
  debugGolfMatching,
};
