// Uses Node's global fetch (undici). Keep-alive pool, TCP_NODELAY, and HTTP/2
// support come from services/httpClient which configures the global dispatcher
// at process bootstrap. Migrated from node-fetch@2 for S3 of latency plan.
const { config } = require('../config');
const log = require('./logger');
const bovadaAltScraper = require('./bovada-alt-scraper');

// AbortController is a Node.js global — used by abortableFetch below to cancel
// slow Odds API calls instead of just ignoring the promise. This actually
// frees the underlying socket so keep-alive doesn't reuse a hung connection.

// ---------------------------------------------------------------------------
// ODDS API FETCH TIMEOUT HELPER (Option E of latency plan)
// ---------------------------------------------------------------------------
// Bounds the tail on Odds API calls used during RFQ pricing. Without this,
// a stuck request can hang for 10+ seconds (observed live in production),
// blocking the RFQ response well past any useful window. With AbortController
// we actually cancel the underlying socket instead of just ignoring the
// promise, which prevents socket leaks and frees the keep-alive connection.
//
// The timeout is generous (500ms) — enough for normal calls to complete,
// tight enough to kill real hangs.
const ODDS_API_FETCH_TIMEOUT_MS = 500;

async function abortableFetch(url, options, timeoutMs) {
  const t = timeoutMs || ODDS_API_FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), t);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

// ---------------------------------------------------------------------------
// UTF-8-SAFE JSON HELPER
// ---------------------------------------------------------------------------
// `await resp.json()` on the global fetch (undici) has been observed to
// silently mis-decode UTF-8 bodies as Latin-1 in some Node + undici
// version combos when the upstream Content-Type omits an explicit
// charset and the body is gzipped. Empirical case (April 2026, Railway):
// The Odds API returned "Atlético Madrid" as bytes 41 74 6c c3 a9 ...
// (correct UTF-8 for é), but the in-memory cache held two characters
// 'Ã' (U+00C3) + '©' (U+00A9) — the classic UTF-8-as-Latin-1 mojibake.
// Locally on Node 24 the same code path produced clean é (U+00E9),
// so the fault is implementation-version-specific rather than logical.
//
// safeJsonFetch sidesteps the issue by reading raw bytes and explicitly
// decoding as UTF-8 via TextDecoder before JSON.parse. Use this in any
// fetch path that handles upstream-provided strings (team names,
// fighter names, tournament names) where a 1-byte mis-decode silently
// breaks downstream string matching.
async function safeJsonFetch(resp) {
  const buf = await resp.arrayBuffer();
  if (buf.byteLength === 0) return null;
  const text = new TextDecoder('utf-8', { fatal: false }).decode(buf);
  return JSON.parse(text);
}

// ---------------------------------------------------------------------------
// IN-MEMORY CACHE
// ---------------------------------------------------------------------------
// Structure: { [league]: { fetchedAt, events: { [eventKey]: { ... } } } }
const oddsCache = {};
// Live (in-play) odds cache — only populated when in-progress games need refreshing
const liveOddsCache = {};
// Delta tracking — last fetch timestamp per sport for /odds/delta calls
const lastDeltaTimestamp = {};

// When Kalshi is the fallback floor (Pinnacle unavailable), widen by 2%
// because Kalshi trades at razor-thin exchange margins
const KALSHI_BUFFER = 0.02;

// SharpAPI events index — for improved PX-to-odds team name matching
// { [sport]: { fetchedAt, events: [{ eventId, homeTeam, awayTeam, startTime }] } }
const sharpEventsIndex = {};

// Player-prop rows cache — kept SEPARATE from oddsCache so the existing
// moneyline/spread/total de-vig pipeline isn't disturbed. Phase 1 of the
// pitcher-strikeouts shadow-pricing experiment populates this with raw
// SharpAPI rows; lookup is on-demand by event + player + line.
//
// Structure:
//   propRowsCache[sport][marketType] = [
//     { event_id, home_team, away_team, event_start_time,
//       sportsbook, player_name, line, selection ('Over'|'Under'),
//       odds_american, odds_probability, ... }, ...
//   ]
//
// Lookup helper: getPropRows(sport, marketType, filterFn) — returns the
// array filtered, or [] if cache empty. We don't pre-index by event/
// player because prop volume per refresh is small (~hundreds of rows
// per market) and PX RFQ rate is bounded — a linear scan is fine.
const propRowsCache = {};

// Set of SharpAPI market_type values that are PROPS (not core line markets).
// Rows with these market_types are partitioned out of the main rows[] array
// before the de-vig grouping step so the existing pipeline doesn't see them.
// Update when adding a new prop market to fetchOddsForSport's marketTypesList.
const PROP_MARKET_TYPES = new Set([
  'player_strikeouts', // pitcher Ks (Phase 1 shadow target). Player_name
                       // distinguishes pitcher (" Thrown" suffix) vs batter
                       // (" Recorded" suffix) sides within the same market.
  'player_points',     // NBA player points (Phase 1 shadow target).
                       // Single-stat per-player Over/Under. Player_name is
                       // typically the player's full name with no suffix.
]);

// Lineup tracking — MLB starting pitchers, NHL starting goalies.
// SharpAPI appends starter name in parens to team_name: "New York Yankees (Gerrit Cole)"
// We capture these per refresh and diff against the prior refresh to detect
// lineup changes (scratches, late swaps). When a change is detected, the
// event's odds are considered "in motion" for a grace window so the pricer
// can decline until the books re-stabilize.
//
// Structure:
//   lineupCache[sport][lineupKey] = {
//     homeStarter: string|null,
//     awayStarter: string|null,
//     seenAt: timestamp,
//     lastChangeAt: timestamp|null,
//     lastChangeDetail: string|null,
//   }
// lineupKey = `${normalizedEventKey}|${YYYY-MM-DD}` to handle doubleheaders.
const lineupCache = {};
const LINEUP_GRACE_MS = 3 * 60 * 1000; // decline for 3 minutes after a change

// Closing line snapshots. Keyed by normalized event key (home|away). Captured
// once per event when the event's commenceTime crosses into the past. Stores
// the final Pinnacle + consensus per-market fair probs as a snapshot for CLV
// analysis. Persisted only in memory — lost on restart.
// {
//   [eventKey]: {
//     sport, homeTeam, awayTeam, commenceTime, capturedAt,
//     markets: {
//       h2h:     { home, away },  // implied probs
//       spreads: { line, home, away },
//       totals:  { line, over, under },
//     },
//     pinnacle: { ... same structure ... },
//   }
// }
const closingLinesCache = {};

// SharpAPI league/sport keys mapping
const LEAGUE_MAP = {
  'basketball_nba': { param: 'league', value: 'nba' },
  'baseball_mlb': { param: 'league', value: 'mlb' },
  'icehockey_nhl': { param: 'league', value: 'nhl' },
  'soccer': { param: 'sport', value: 'soccer' },
  'mma_mixed_martial_arts': { param: 'league', value: 'ufc' },
  // Tennis: TOA dynamic discovery is primary, SharpAPI is fallback when TOA
  // returns 0 events (e.g. Pinnacle/FD/DK haven't posted Madrid Open prelims).
  // SharpAPI now carries Caesars/DK/FD/BetMGM rows on tennis; comment that
  // SharpAPI had "zero bookmaker odds" is stale as of 2026-05-01.
  'tennis': { param: 'sport', value: 'tennis' },
};

// Bookmakers for The Odds API — Pinnacle (sharpest), DraftKings, FanDuel
const ODDS_API_BOOKMAKERS = 'pinnacle,draftkings,fanduel';

// Expanded bookmakers for alt-line fetching only — more books = more alt line values.
// Primary pricing is NOT affected (uses ODDS_API_BOOKMAKERS via SharpAPI consensus).
// Minimum 2 books required per alt line to ensure de-vig accuracy.
const ALT_LINES_BOOKMAKERS = 'pinnacle,draftkings,fanduel,bovada,betonlineag,betrivers,williamhill_us,unibet_us,superbook,betmgm,espnbet,hardrockbet,fliff,betus,lowvig,pointsbetus,wynnbet';
const ALT_LINES_MIN_BOOKS = 2; // Require at least 2 books for each alt line value
// …unless Pinnacle is the sole book. Pinnacle is sharp enough that we trust
// its line/price alone when no other book has posted that alt. Tennis alt
// totals, for example, only come from Pinnacle among the books we poll.
const ALT_LINES_PINNACLE_ALONE_OK = true;

// Sports that use The Odds API as fallback (SharpAPI free tier doesn't cover them)
const ODDS_API_FALLBACK = {
  'tennis': {
    // Tennis tournaments rotate — discover active ones dynamically
    dynamic: true,
    sportPrefix: 'tennis_',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'basketball_ncaab': {
    oddsApiSport: 'basketball_ncaab',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'americanfootball_nfl': {
    oddsApiSport: 'americanfootball_nfl',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'americanfootball_ncaaf': {
    oddsApiSport: 'americanfootball_ncaaf',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'basketball_wnba': {
    oddsApiSport: 'basketball_wnba',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_usa_mls': {
    oddsApiSport: 'soccer_usa_mls',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_epl': {
    oddsApiSport: 'soccer_epl',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_uefa_champs_league': {
    oddsApiSport: 'soccer_uefa_champs_league',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_uefa_europa_league': {
    oddsApiSport: 'soccer_uefa_europa_league',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_spain_la_liga': {
    oddsApiSport: 'soccer_spain_la_liga',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_italy_serie_a': {
    oddsApiSport: 'soccer_italy_serie_a',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_germany_bundesliga': {
    oddsApiSport: 'soccer_germany_bundesliga',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_france_ligue_one': {
    oddsApiSport: 'soccer_france_ligue_one',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_usa_nwsl': {
    oddsApiSport: 'soccer_usa_nwsl',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_mexico_ligamx': {
    oddsApiSport: 'soccer_mexico_ligamx',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_brazil_campeonato': {
    oddsApiSport: 'soccer_brazil_campeonato',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'soccer_conmebol_libertadores': {
    oddsApiSport: 'soccer_conmebol_libertadores',
    markets: 'h2h,spreads,totals',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  // Golf and combat sports — h2h only (no spreads/totals on these markets)
  'golf_pga_championship': {
    oddsApiSport: 'golf_pga_championship',
    markets: 'h2h,outrights',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  // MMA moved to SharpAPI (league=ufc) — see LEAGUE_MAP. SharpAPI has 3 books
  // (Pinnacle + DK + FD) on our tier vs The Odds API's 3, and posts fighters
  // earlier. Keep entry commented for reference in case of SharpAPI regression.
  // 'mma_mixed_martial_arts': {
  //   oddsApiSport: 'mma_mixed_martial_arts',
  //   markets: 'h2h',
  //   bookmakers: ODDS_API_BOOKMAKERS,
  // },
  'boxing_boxing': {
    oddsApiSport: 'boxing_boxing',
    markets: 'h2h',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
};

// ---------------------------------------------------------------------------
// SHARPAPI CLIENT
// ---------------------------------------------------------------------------

/**
 * Fetch odds for a single league from SharpAPI.
 * Gets moneyline, spread, and total markets from all available books,
 * then de-vigs by averaging across books.
 */
async function fetchOddsForSport(sport, opts) {
  opts = opts || {};
  const liveMode = !!opts.live;
  // DataGolf integration for golf matchups — handled separately
  if (sport === 'golf_matchups') {
    if (liveMode) return null;
    const datagolf = require('./datagolf');
    const result = await datagolf.fetchGolfMatchupsCache();
    oddsCache[sport] = { fetchedAt: result.fetchedAt, events: result.events };
    return result.events;
  }
  // Tennis: TOA dynamic discovery is primary (Pinnacle/FD/DK), but TOA's
  // tournament coverage lags overnight — Madrid Open prelims at 7am ET were
  // returning 0 events from TOA while SharpAPI had Caesars/DK/FD/BetMGM rows.
  // Try TOA first, fall through to SharpAPI when TOA empty.
  if (sport === 'tennis') {
    if (liveMode) return null;
    const toaResult = await fetchFromTheOddsApi(sport);
    if (toaResult && Object.keys(toaResult).length > 0) return toaResult;
    log.info('OddsFeed', 'Tennis TOA returned 0 events — falling through to SharpAPI');
    // fall through to SharpAPI path below
  } else if (ODDS_API_FALLBACK[sport]) {
    if (liveMode) {
      // Live odds for Odds-API-fallback sports not implemented yet
      return null;
    }
    return fetchFromTheOddsApi(sport);
  }

  const mapping = LEAGUE_MAP[sport];
  if (!mapping) throw new Error(`Unknown sport: ${sport}`);

  // Market types vary by sport. SharpAPI's /odds endpoint caps `limit` at
  // 200 per request; we paginate with `cursor` until meta.pagination.has_more
  // is false to drain each market. Splitting by market type still helps
  // because of the tier request-rate limit (Hobby = 120/min) — a single
  // drained fetch per market is much cheaper than a single multi-market
  // fetch that then re-pages through everything.
  //
  // PROP_MARKET_TYPES: any market_type fetched here that should NOT flow
  // through the core de-vig pipeline (which expects moneyline / spread /
  // total shapes). Prop rows are partitioned into propRowsCache for the
  // shadow-pricing path. Update both this set AND marketTypesList when
  // adding a new prop market.
  const marketTypesList = {
    'baseball_mlb': ['moneyline', 'run_line', 'total_runs', 'team_total', '1st_5_innings_moneyline', '1st_5_innings_run_line', 'player_strikeouts'],
    'icehockey_nhl': ['moneyline', 'puck_line', 'total_goals', 'team_total'],
    'basketball_nba': ['moneyline', 'point_spread', 'total_points', 'team_total', 'player_points'],
    'tennis': ['moneyline', 'point_spread', 'total_points'],
    'soccer': ['moneyline', 'point_spread', 'total_goals', 'team_total'],
    'mma_mixed_martial_arts': ['moneyline'],
  }[sport] || ['moneyline', 'point_spread', 'total_points', 'team_total'];

  log.info('OddsFeed', `Fetching ${liveMode ? 'LIVE ' : ''}${mapping.value} odds from SharpAPI (${marketTypesList.length} market types)...`);

  // Fetch each market type separately and paginate with `cursor` until
  // meta.pagination.has_more is false. Hard safety cap on pages so a bad
  // cursor can never cause an infinite loop.
  const PAGE_LIMIT = 200; // SharpAPI /odds max per request
  const MAX_PAGES_PER_MARKET = 50; // safety: 50 × 200 = 10k rows
  const rows = [];
  const marketBreakdown = {};
  for (const mt of marketTypesList) {
    const baseUrl = `${config.oddsApi.baseUrl}/odds`
      + `?${mapping.param}=${mapping.value}`
      + `&market=${mt}`
      + `&live=${liveMode ? 'true' : 'false'}`
      + `&limit=${PAGE_LIMIT}`;
    let cursor = null;
    let pages = 0;
    let mtRowCount = 0;
    const mtEvents = new Set();
    const mtBooks = new Set();
    let errorState = null;
    while (pages < MAX_PAGES_PER_MARKET) {
      const url = cursor ? `${baseUrl}&cursor=${encodeURIComponent(cursor)}` : baseUrl;
      try {
        const resp = await fetch(url, {
          headers: { 'X-API-Key': config.oddsApi.apiKey },
        });
        if (!resp.ok) {
          const text = await resp.text();
          log.warn('OddsFeed', `SharpAPI ${resp.status} for ${mapping.value}/${mt} (page ${pages + 1}): ${text.substring(0, 100)}`);
          errorState = resp.status;
          break;
        }
        const body = await safeJsonFetch(resp);
        const mtRows = body.data || [];
        rows.push(...mtRows);
        mtRowCount += mtRows.length;
        for (const r of mtRows) {
          if (r.event_id) mtEvents.add(r.event_id);
          if (r.sportsbook) mtBooks.add(r.sportsbook);
        }
        pages++;
        const pagination = body.meta && body.meta.pagination;
        if (!pagination || !pagination.has_more || !pagination.next_cursor) break;
        cursor = pagination.next_cursor;
      } catch (err) {
        log.warn('OddsFeed', `Fetch error for ${mapping.value}/${mt} (page ${pages + 1}): ${err.message}`);
        errorState = err.message;
        break;
      }
    }
    if (pages >= MAX_PAGES_PER_MARKET) {
      log.warn('OddsFeed', `Hit ${MAX_PAGES_PER_MARKET}-page safety cap for ${mapping.value}/${mt} — possible pagination loop`);
    }
    marketBreakdown[mt] = {
      rows: mtRowCount,
      events: mtEvents.size,
      books: mtBooks.size,
      pages,
      ...(errorState != null ? { error: errorState } : {}),
    };
    log.debug('OddsFeed', `  ${mt}: ${mtRowCount} rows across ${pages} page(s)`);
  }
  // Compact one-line breakdown: "moneyline=450r/15e/4b/3p, team_total=0r/0e/0b/1p(EMPTY), ..."
  const breakdownStr = Object.entries(marketBreakdown)
    .map(([mt, b]) => {
      const flag = b.error ? `(ERR:${b.error})` : (b.rows === 0 ? '(EMPTY)' : '');
      return `${mt}=${b.rows}r/${b.events}e/${b.books}b/${b.pages}p${flag}`;
    })
    .join(', ');
  log.info('OddsFeed', `SharpAPI ${mapping.value} breakdown: ${breakdownStr}`);
  log.info('OddsFeed', `Got ${rows.length} total odds rows for ${mapping.value} across ${marketTypesList.length} markets`);

  // Partition prop rows out of the main pipeline. The downstream de-vig +
  // line-manager seeding code only knows how to handle moneyline / spread /
  // total / team_total shapes; prop rows would either be silently dropped
  // or cause warnings. Keep them in propRowsCache for the shadow-pricing
  // path (services/websocket.js → odds-feed.lookupPlayerStrikeoutProp).
  const propRows = [];
  const coreRows = [];
  for (const row of rows) {
    if (row && PROP_MARKET_TYPES.has(row.market_type)) propRows.push(row);
    else coreRows.push(row);
  }
  if (propRows.length > 0) {
    if (!propRowsCache[sport]) propRowsCache[sport] = {};
    // Group props by market_type so callers can query "all
    // player_strikeouts rows for sport=X" with one map lookup.
    // Apply cleanTeamName here so SharpAPI's abbreviated names
    // (e.g. "BOS Red Sox") are canonicalized to PX-compatible
    // forms before the cache stores them — avoids needing the
    // matcher to handle both forms.
    const byMt = {};
    for (const r of propRows) {
      if (!byMt[r.market_type]) byMt[r.market_type] = [];
      byMt[r.market_type].push({
        ...r,
        home_team: cleanTeamName(r.home_team),
        away_team: cleanTeamName(r.away_team),
      });
    }
    propRowsCache[sport] = { ...propRowsCache[sport], ...byMt, fetchedAt: Date.now() };
    const mtSummary = Object.entries(byMt)
      .map(([mt, arr]) => `${mt}=${arr.length}`)
      .join(', ');
    log.info('OddsFeed', `Cached ${propRows.length} prop rows for ${mapping.value} (${mtSummary})`);
  }
  // Use coreRows (non-prop) for the existing grouping pipeline.
  const groupingRows = coreRows;

  // Group by event, then by market+selection to de-vig across books
  const eventMap = {};
  // SharpAPI's MLB feed sometimes returns sub-game data as SEPARATE events
  // with team names carrying a sub-game suffix ("Washington first 5
  // innings"). These pollute the cache as phantom entries that don't
  // merge with the proper full-game event, and their data lands in the
  // wrong market slots (e.g. markets.totals instead of markets.totals_f5).
  // Verified 2026-05-03 Milwaukee/Washington: a phantom entry under team
  // name "Washington first 5 innings" carried a `totals` market with all
  // rawOdds=null, while the proper Mil/Was game entry simply had no F5
  // markets. Drop these phantom rows at ingestion — real F5 coverage
  // comes via SharpAPI's own '1st_5_innings_*' market_type rows on the
  // proper event AND the TOA F5 supplement; phantom rows just corrupt
  // the cache and never carry useful data.
  const SUB_GAME_TEAM_SUFFIX = /\b(first\s+5\s+innings|1st\s+5\s+innings|first\s+half|1st\s+half|first\s+quarter|1st\s+quarter|first\s+period|1st\s+period|\d{4}\s+1st\s+round\s+series|\d{4}\s+2nd\s+round\s+series|\d{4}\s+conference\s+(?:semifinals?|finals?))\b/i;
  let phantomDropped = 0;
  for (const row of groupingRows) {
    if (SUB_GAME_TEAM_SUFFIX.test(row.home_team || '') || SUB_GAME_TEAM_SUFFIX.test(row.away_team || '')) {
      phantomDropped++;
      continue;
    }
    const eventId = row.event_id;
    if (!eventMap[eventId]) {
      eventMap[eventId] = {
        homeTeam: cleanTeamName(row.home_team),
        awayTeam: cleanTeamName(row.away_team),
        // Capture raw names for lineup tracking (SharpAPI appends starter
        // info in parens: "New York Yankees (Gerrit Cole)"). First non-null
        // raw name wins — SharpAPI is consistent within a single fetch.
        rawHomeTeam: row.home_team,
        rawAwayTeam: row.away_team,
        commenceTime: row.event_start_time,
        eventId,
        odds: [], // collect all odds rows
      };
    }
    eventMap[eventId].odds.push(row);
  }
  if (phantomDropped > 0) {
    log.info('OddsFeed', `Dropped ${phantomDropped} phantom sub-game rows (team name carrying F5/H1/Q1/P1 suffix) for ${sport}`);
  }

  // Update lineup cache for MLB (pitchers) / NHL (goalies). Runs before
  // consensus building so downstream log output includes any detected change.
  if (!liveMode && (sport === 'baseball_mlb' || sport === 'icehockey_nhl')) {
    for (const ev of Object.values(eventMap)) {
      const homeStarter = extractStarter(ev.rawHomeTeam);
      const awayStarter = extractStarter(ev.rawAwayTeam);
      updateLineupState(sport, ev.homeTeam, ev.awayTeam, ev.commenceTime, homeStarter, awayStarter);
    }
  }

  // Merge single-book events (e.g. Kalshi) into matching multi-book events.
  // SharpAPI sometimes groups Kalshi under a separate event_id because Kalshi uses
  // abbreviated team names (e.g. "Chicago WS" instead of "Chicago White Sox").
  // This step merges those orphan events by fuzzy team name + date matching.
  {
    const eventIds = Object.keys(eventMap);
    // Identify "main" events (2+ books) and "orphan" events (1 book)
    const mainEvents = [];
    const orphanEvents = [];
    for (const eid of eventIds) {
      const ev = eventMap[eid];
      const books = new Set(ev.odds.map(r => r.sportsbook));
      if (books.size >= 2) mainEvents.push(eid);
      else orphanEvents.push(eid);
    }

    if (orphanEvents.length > 0 && mainEvents.length > 0) {
      // Build lookup of main event team names (normalized last-word matching)
      const getLastWords = (name) => {
        const words = normalizeTeamName(name).split(/\s+/);
        return words.slice(-2).join(' '); // last 2 words e.g. "white sox", "blue jays"
      };
      const getLastWord = (name) => {
        const words = normalizeTeamName(name).split(/\s+/);
        return words[words.length - 1]; // last word e.g. "sox", "jays"
      };

      let mergedOrphans = 0;
      for (const orphanId of orphanEvents) {
        const orphan = eventMap[orphanId];
        const orphanDate = orphan.commenceTime ? new Date(orphan.commenceTime).toISOString().substring(0, 10) : '';
        const oHomeLast = getLastWords(orphan.homeTeam);
        const oAwayLast = getLastWords(orphan.awayTeam);
        const oHomeSingle = getLastWord(orphan.homeTeam);
        const oAwaySingle = getLastWord(orphan.awayTeam);

        let bestMatch = null;
        let bestMatchSwapped = false;
        for (const mainId of mainEvents) {
          const main = eventMap[mainId];
          const mainDate = main.commenceTime ? new Date(main.commenceTime).toISOString().substring(0, 10) : '';
          // Date must match (or one is missing)
          if (orphanDate && mainDate && orphanDate !== mainDate) continue;

          const mHomeLast = getLastWords(main.homeTeam);
          const mAwayLast = getLastWords(main.awayTeam);
          const mHomeSingle = getLastWord(main.homeTeam);
          const mAwaySingle = getLastWord(main.awayTeam);

          // Try exact normalized match first
          const exactMatch = normalizeEventKey(orphan.homeTeam, orphan.awayTeam) ===
                             normalizeEventKey(main.homeTeam, main.awayTeam);
          // Try last-2-words match (handles "Chicago White Sox" vs "Chicago WS" where WS doesn't match)
          // Try last-word match (handles "Athletics" vs "A's" — both have last word issues)
          // Try containment (handles "san francisco giants" contains "giants")
          const homeMatch = exactMatch ||
            mHomeLast === oHomeLast ||
            mHomeSingle === oHomeSingle ||
            normalizeTeamName(main.homeTeam).includes(normalizeTeamName(orphan.homeTeam)) ||
            normalizeTeamName(orphan.homeTeam).includes(normalizeTeamName(main.homeTeam));
          const awayMatch = exactMatch ||
            mAwayLast === oAwayLast ||
            mAwaySingle === oAwaySingle ||
            normalizeTeamName(main.awayTeam).includes(normalizeTeamName(orphan.awayTeam)) ||
            normalizeTeamName(orphan.awayTeam).includes(normalizeTeamName(main.awayTeam));
          // Also try swapped home/away (Kalshi sometimes flips them)
          const homeMatchSwap = mHomeLast === oAwayLast || mHomeSingle === oAwaySingle ||
            normalizeTeamName(main.homeTeam).includes(normalizeTeamName(orphan.awayTeam)) ||
            normalizeTeamName(orphan.awayTeam).includes(normalizeTeamName(main.homeTeam));
          const awayMatchSwap = mAwayLast === oHomeLast || mAwaySingle === oHomeSingle ||
            normalizeTeamName(main.awayTeam).includes(normalizeTeamName(orphan.homeTeam)) ||
            normalizeTeamName(orphan.homeTeam).includes(normalizeTeamName(main.awayTeam));

          if (homeMatch && awayMatch) {
            bestMatch = mainId;
            bestMatchSwapped = false;
            break;
          }
          if (homeMatchSwap && awayMatchSwap) {
            bestMatch = mainId;
            bestMatchSwapped = true;
            break;
          }
        }

        if (bestMatch) {
          // Merge orphan odds into main event.
          // If the match was via swapped home/away, flip selection_type
          // so "home"/"away" align with the main event's perspective.
          // Without this, Kalshi's "home" odds (which refer to the orphan's
          // home = main's away) get averaged into the wrong side of the
          // consensus, corrupting fair probabilities.
          const SWAP_MAP = {
            'home': 'away', 'away': 'home',
            'home_over': 'away_over', 'home_under': 'away_under',
            'away_over': 'home_over', 'away_under': 'home_under',
          };
          for (const row of orphan.odds) {
            if (bestMatchSwapped) {
              if (row.selection_type && SWAP_MAP[row.selection_type]) {
                row.selection_type = SWAP_MAP[row.selection_type];
              }
              // Negate spread points when swapping (home -1.5 ↔ away +1.5)
              const isSpread = ['run_line', 'puck_line', 'point_spread'].includes(row.market_type);
              if (isSpread && row.point != null) {
                row.point = -row.point;
              }
            }
            eventMap[bestMatch].odds.push(row);
          }
          delete eventMap[orphanId];
          mergedOrphans++;
          if (bestMatchSwapped) {
            log.info('OddsFeed', `Merged swapped orphan ${orphan.homeTeam}/${orphan.awayTeam} → main ${eventMap[bestMatch].homeTeam}/${eventMap[bestMatch].awayTeam} (flipped selection_type)`);
          }
        }
      }
      if (mergedOrphans > 0) {
        log.info('OddsFeed', `Merged ${mergedOrphans} single-book events into main events for ${mapping.value}`);
      }
    }
  }

  // Supplement with Pinnacle odds from The Odds API
  // Pinnacle events have different IDs, so match by team names and merge.
  // Rows that don't match an existing SharpAPI event become NEW events —
  // this guarantees Pinnacle coverage even when SharpAPI's 50-row cap drops
  // events from the response.
  if (PINNACLE_SPORT_MAP[sport]) {
    const pinnacleRows = await fetchPinnacleRows(sport);
    if (pinnacleRows.length > 0) {
      // Match Odds API events to SharpAPI events by team key + DATE.
      // CRITICAL: do NOT use a dateless fallback. When The Odds API returns
      // multiple events for the same team matchup on different dates (today +
      // tomorrow), a dateless fallback merges tomorrow's odds into today's
      // SharpAPI event, corrupting the cached values. Concrete case: Reds @
      // Angels had a -190 fanduel for today and -132 for tomorrow; both got
      // pushed into the same _rawOdds array, and byBook last-write-wins made
      // tomorrow's (wrong) values overwrite today's (correct) values.
      const teamDateToEventId = {};
      for (const [eid, ev] of Object.entries(eventMap)) {
        const key = normalizeEventKey(ev.homeTeam, ev.awayTeam);
        const date = ev.commenceTime ? new Date(ev.commenceTime).toISOString().substring(0, 10) : '';
        if (date) teamDateToEventId[key + '|' + date] = eid;
      }

      // Group Pinnacle rows by event (key + date) so we can create new events
      // for unmatched groups.
      const pinGroups = {};
      for (const row of pinnacleRows) {
        const home = cleanTeamName(row.home_team);
        const away = cleanTeamName(row.away_team);
        const key = normalizeEventKey(home, away);
        const rowDate = row.event_start_time ? new Date(row.event_start_time).toISOString().substring(0, 10) : '';
        const groupKey = key + '|' + rowDate;
        if (!pinGroups[groupKey]) {
          pinGroups[groupKey] = {
            home, away, key, rowDate,
            commenceTime: row.event_start_time,
            rows: [],
          };
        }
        pinGroups[groupKey].rows.push(row);
      }

      let merged = 0, created = 0;
      for (const [groupKey, group] of Object.entries(pinGroups)) {
        // Strictly require a date-specific match. If SharpAPI doesn't have
        // the same matchup on the same date, create a synthetic event for
        // the Odds API data rather than merging into a different date.
        const matchedId = group.rowDate ? teamDateToEventId[group.key + '|' + group.rowDate] : null;
        if (matchedId && eventMap[matchedId]) {
          // Merge into existing SharpAPI event
          for (const row of group.rows) eventMap[matchedId].odds.push(row);
          merged += group.rows.length;
        } else {
          // No matching SharpAPI event — create a NEW event from Pinnacle data.
          // Use a synthetic event_id prefixed with 'pin_' to avoid collisions.
          const synEventId = 'pin_' + group.key + '_' + group.rowDate;
          eventMap[synEventId] = {
            homeTeam: group.home,
            awayTeam: group.away,
            commenceTime: group.commenceTime,
            eventId: synEventId,
            odds: group.rows,
          };
          created++;
        }
      }
      log.info('OddsFeed', `Pinnacle: merged ${merged} rows, created ${created} new events for ${mapping.value}`);
    }
  }

  // Consolidate events that describe the same physical game. SharpAPI can
  // return the same matchup under different event_ids across its per-
  // market-type queries (e.g. moneyline event_id differs from totals
  // event_id), and the Odds-API supplement may also create a synthetic
  // event if SharpAPI's event_id wasn't the one in teamDateToEventId.
  // Result: same game appears twice in eventMap, with markets split
  // between entries — bettor RFQs miss h2h/totals depending on which
  // entry wins the cache lookup. Merge any events sharing the same
  // (normalizedKey, commence date) into the first one seen.
  {
    const byKeyDate = {};
    const toDelete = [];
    // Use the alias map so abbreviation/full-name pairs like "BOS Red Sox"
    // and "Boston Red Sox" collapse to the same consolidation key.
    const aliasedKey = (home, away) =>
      applyTeamAlias(normalizeTeamName(home)) + '|' + applyTeamAlias(normalizeTeamName(away));
    for (const [eid, ev] of Object.entries(eventMap)) {
      const key = aliasedKey(ev.homeTeam, ev.awayTeam);
      const date = ev.commenceTime ? new Date(ev.commenceTime).toISOString().substring(0, 10) : '';
      if (!date) continue;
      const kd = key + '|' + date;
      if (byKeyDate[kd] == null) {
        byKeyDate[kd] = eid;
      } else {
        const primary = eventMap[byKeyDate[kd]];
        // Merge odds rows from the duplicate into the primary entry,
        // preserving row-level book/market_type data. Downstream parse
        // step de-dups via getBookPairs so identical rows don't inflate
        // consensus counts.
        for (const row of (ev.odds || [])) primary.odds.push(row);
        toDelete.push(eid);
      }
    }
    for (const eid of toDelete) delete eventMap[eid];
    if (toDelete.length > 0) {
      log.info('OddsFeed', `Consolidated ${toDelete.length} duplicate ${mapping.value} events (merged into primary entries)`);
    }
  }

  // Parse into our cache format
  // Store as array per team pair to handle back-to-back series and doubleheaders
  const parsed = {};
  for (const [eventId, event] of Object.entries(eventMap)) {
    const key = normalizeEventKey(event.homeTeam, event.awayTeam);
    const markets = {};

    // Group odds by market_type and sportsbook
    const byMarketAndBook = {};
    for (const row of event.odds) {
      const mk = `${row.market_type}|${row.sportsbook}`;
      if (!byMarketAndBook[mk]) byMarketAndBook[mk] = [];
      byMarketAndBook[mk].push(row);
    }

    // Process moneyline
    const mlBooks = getBookPairs(event.odds, 'moneyline');
    if (mlBooks.length > 0) {
      markets.h2h = buildConsensusMoneyline(mlBooks);
    }

    // Process spread (point_spread / run_line / puck_line)
    const spreadTypes = ['point_spread', 'run_line', 'puck_line'];
    const spreadOdds = event.odds.filter(r => spreadTypes.includes(r.market_type));
    const spreadBooks = getBookPairs(spreadOdds, null);
    if (spreadBooks.length > 0) {
      markets.spreads = buildConsensusSpread(spreadBooks);
    }

    // Process totals (total_points / total_runs / total_goals)
    const totalTypes = ['total_points', 'total_runs', 'total_goals'];
    const totalOdds = event.odds.filter(r => totalTypes.includes(r.market_type));
    const totalBooks = getBookPairsForTotals(totalOdds);
    if (totalBooks.length > 0) {
      markets.totals = buildConsensusTotals(totalBooks);
    }

    // Process team totals
    const teamTotalOdds = event.odds.filter(r => r.market_type === 'team_total');
    if (teamTotalOdds.length > 0) {
      const teamTotalBooks = getBookPairsForTeamTotals(teamTotalOdds);
      if (teamTotalBooks.length > 0) {
        const tt = buildConsensusTeamTotals(teamTotalBooks);
        if (tt) markets.team_totals = tt;
      }
    }

    // F5 markets (MLB only) — SharpAPI returns them under '1st_5_innings_*'
    // naming. Totals aren't populated by SharpAPI currently; those come
    // from The Odds API via supplementMlbF5Markets. We just attach what
    // SharpAPI has here (h2h_f5, spreads_f5) so the primary feed covers
    // moneyline/run-line F5 without waiting for the Odds-API supplement.
    if (sport === 'baseball_mlb') {
      const mlF5Books = getBookPairs(event.odds, '1st_5_innings_moneyline');
      if (mlF5Books.length > 0) {
        const m = buildConsensusMoneyline(mlF5Books);
        if (m) markets.h2h_f5 = m;
      }
      const spreadF5Odds = event.odds.filter(r => r.market_type === '1st_5_innings_run_line');
      const spreadF5Books = getBookPairs(spreadF5Odds, null);
      if (spreadF5Books.length > 0) {
        const s = buildConsensusSpread(spreadF5Books);
        if (s) markets.spreads_f5 = s;
      }
    }

    if (Object.keys(markets).length > 0) {
      if (!parsed[key]) parsed[key] = [];
      parsed[key].push({
        homeTeam: event.homeTeam,
        awayTeam: event.awayTeam,
        commenceTime: event.commenceTime,
        eventId,
        markets,
        _rawOdds: event.odds, // preserved for delta merging
      });
    }
  }

  const targetCache = liveMode ? liveOddsCache : oddsCache;
  targetCache[sport] = {
    fetchedAt: Date.now(),
    events: parsed,
  };

  // Set delta timestamp for incremental updates
  if (!liveMode && LEAGUE_MAP[sport]) {
    lastDeltaTimestamp[sport] = new Date().toISOString();
  }

  // Supplement with First-5-Innings markets for MLB (separate from full-game)
  if (!liveMode && sport === 'baseball_mlb') {
    try {
      await supplementMlbF5Markets(parsed);
    } catch (err) {
      log.warn('OddsFeed', `MLB F5 supplement failed: ${err.message}`);
    }
    _scheduleSupplementRetry(sport, 'MLB F5', supplementMlbF5Markets, parsed);
  }

  // Supplement with 1st-Half markets for NBA (separate from full-game)
  if (!liveMode && sport === 'basketball_nba') {
    try {
      await supplementNbaH1Markets(parsed);
    } catch (err) {
      log.warn('OddsFeed', `NBA H1 supplement failed: ${err.message}`);
    }
    _scheduleSupplementRetry(sport, 'NBA H1', supplementNbaH1Markets, parsed);
  }

  // Supplement with team-total markets for NBA/MLB/NHL. SharpAPI Hobby
  // plan's team_total market currently returns no data for these leagues,
  // so we gap-fill from The Odds API on the same refresh cycle (pre-warmed
  // cache — zero RFQ latency impact, in contrast to on-demand alt-line
  // fetches). Primary-cycle gap-fill is the general pattern for any
  // market SharpAPI doesn't surface for us; see also supplementNbaH1Markets.
  if (!liveMode && ['basketball_nba', 'baseball_mlb', 'icehockey_nhl'].includes(sport)) {
    try {
      await supplementTeamTotals(parsed, sport);
    } catch (err) {
      log.warn('OddsFeed', `${sport} team_totals supplement failed: ${err.message}`);
    }
    _scheduleSupplementRetry(sport, `${sport} team_totals`, supplementTeamTotals, parsed, sport);
  }

  log.info('OddsFeed', `Cached ${Object.keys(parsed).length} ${liveMode ? 'LIVE ' : ''}events for ${mapping.value}`);
  return parsed;
}

// Merge a freshly-supplemented sub-game market (h2h_f5/spreads_f5/
// totals_f5/h2h_h1/spreads_h1/totals_h1) into the existing cache entry
// without losing byLine entries that were populated by an earlier
// build (typically SharpAPI's '1st_5_innings_run_line' rows that the
// main parse stored before the per-event TOA supplement ran).
//
// Without this merge, the supplement OVERWRITES ev.markets.spreads_f5
// with TOA data only — and TOA's per-event response often contains
// only Pinnacle's pick-em (line=0) for thinner-volume games, while
// SharpAPI had BetMGM at the standard ±0.5. Result: PX RFQs at ±0.5
// returned null even though the data was in cache moments earlier.
//
// Strategy: prefer the FRESH (TOA) market as the base — its primary
// home/away/line is more reliable since TOA reaches more sharp books.
// Then union the byLine maps from both, with fresh winning on any
// line both sides happen to cover. SharpAPI-only lines (e.g. BetMGM
// ±0.5 when TOA only had Pinnacle line=0) are preserved.
function _mergeSupplementedMarket(existing, fresh) {
  if (!existing) return fresh;
  if (!fresh) return existing;
  const mergedByLine = { ...(existing.byLine || {}), ...(fresh.byLine || {}) };
  return { ...fresh, byLine: mergedByLine };
}

// Per-event TOA supplements (F5 / H1 / team_totals) are best-effort on
// the primary refresh cycle — if resolveOddsApiEventId misses for an
// event, or TOA returns a transient 4xx/5xx, that event's supplemented
// markets are silently skipped. Until the next full refresh (default
// 10 min), the affected game's H1 / F5 / team_total RFQs decline as
// "no fair value." Operator caught Lakers @ Rockets 2026-05-01: H1 ML
// nulled in the dashboard for the entire window between two refresh
// cycles even though TOA had the data and a manual /refresh-odds
// recovered it instantly.
//
// This helper schedules background retries at 60s / 120s / 240s after
// the initial supplement run. Each retry calls the same supplement
// function on the SAME parsed-events reference. The supplement's
// "skip events that already have all 3 markets" check (inside each
// supplement) makes this idempotent — successful events are skipped
// instantly; only still-missing events get a TOA call.
//
// If the cache is wholesale-replaced before a retry fires (next
// refresh cycle started), the retry is skipped — the new cycle's own
// retry chain takes over for the new events. Stale references are
// detected by checking that oddsCache[sport].events still === the
// captured parsed reference.
//
// Retry telemetry exposed via _supplementRetryStats (read by /status).
const _supplementRetryStats = {
  scheduled: 0,
  fired: 0,
  skippedStale: 0,
  failed: 0,
  succeeded: 0,
};
function _scheduleSupplementRetry(sport, supplementName, supplementFn, ...args) {
  const delaysMs = [60_000, 120_000, 240_000];
  const initialEventsRef = oddsCache[sport]?.events;
  if (!initialEventsRef) return;
  let attempt = 0;
  function next() {
    if (attempt >= delaysMs.length) return;
    _supplementRetryStats.scheduled++;
    setTimeout(async () => {
      _supplementRetryStats.fired++;
      // Stale-reference check: skip if the cache has been replaced by
      // a newer refresh cycle since this retry was scheduled. The new
      // cycle owns its own retry chain.
      if (oddsCache[sport]?.events !== initialEventsRef) {
        _supplementRetryStats.skippedStale++;
        return;
      }
      try {
        await supplementFn(...args);
        _supplementRetryStats.succeeded++;
        log.debug('OddsFeed', `${supplementName} retry attempt ${attempt + 1}/${delaysMs.length} complete`);
      } catch (err) {
        _supplementRetryStats.failed++;
        log.warn('OddsFeed', `${supplementName} retry attempt ${attempt + 1} failed: ${err.message}`);
      }
      attempt++;
      next();
    }, delaysMs[attempt]);
  }
  next();
}

function getSupplementRetryStats() {
  return { ..._supplementRetryStats };
}

/**
 * Fetch First-5-Innings (F5) markets for MLB from The Odds API and attach them
 * to the existing event cache as separate market types: h2h_f5, spreads_f5, totals_f5.
 * These are independent from full-game markets and need their own pricing.
 */
/**
 * Fuzzy lookup into a parsedEvents map (keyed by normalizeEventKey) that
 * falls back to last-word team-name matching. Handles the SharpAPI /
 * Odds-API abbreviation gap (e.g. SharpAPI's "A's", "Chicago WS" vs
 * Odds-API's "Oakland Athletics", "Chicago White Sox") that otherwise
 * silently breaks F5/H1 supplement matching.
 */
function findParsedEntryFuzzy(parsedEvents, home, away) {
  const exact = parsedEvents[normalizeEventKey(cleanTeamName(home), cleanTeamName(away))];
  if (exact) return exact;
  const lw = (name) => (name || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').split(/\s+/).filter(Boolean).pop() || '';
  const homeLW = lw(home), awayLW = lw(away);
  if (!homeLW || !awayLW) return null;
  for (const entry of Object.values(parsedEvents)) {
    const arr = Array.isArray(entry) ? entry : [entry];
    for (const ev of arr) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      const eh = lw(ev.homeTeam), ea = lw(ev.awayTeam);
      if ((eh === homeLW && ea === awayLW) || (eh === awayLW && ea === homeLW)) return entry;
    }
  }
  return null;
}

async function supplementMlbF5Markets(parsedEvents) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return;

  // IMPORTANT: The Odds API's bulk /odds endpoint does NOT support F5
  // market keys (h2h_1st_5_innings etc.) — returns 422 INVALID_MARKET.
  // F5 markets live on the per-event endpoint /events/{id}/odds.
  // Loop all parsed events and fetch F5 per-event (bounded concurrency).
  // Still cheap vs our quota (15-ish MLB games per cycle).

  // Collect candidate events (skip any that already have all 3 F5 markets).
  const candidates = [];
  for (const entry of Object.values(parsedEvents)) {
    const arr = Array.isArray(entry) ? entry : [entry];
    for (const ev of arr) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      if (ev.markets && ev.markets.h2h_f5 && ev.markets.spreads_f5 && ev.markets.totals_f5) continue;
      candidates.push(ev);
    }
  }
  if (candidates.length === 0) {
    log.info('OddsFeed', 'MLB F5 supplement: no candidates');
    return;
  }

  let h2hCount = 0, spreadCount = 0, totalCount = 0, calls = 0, matchFails = 0, apiFails = 0;
  const CONCURRENCY = 3;
  let idx = 0;
  async function worker() {
    while (idx < candidates.length) {
      const ev = candidates[idx++];
      const resolved = await resolveOddsApiEventId('baseball_mlb', ev.homeTeam, ev.awayTeam, ev.commenceTime);
      if (!resolved) { matchFails++; continue; }

      const url = `https://api.the-odds-api.com/v4/sports/baseball_mlb/events/${resolved.eventId}/odds`
        + `?apiKey=${theOddsApiKey}`
        + `&regions=us,eu`
        + `&markets=h2h_1st_5_innings,spreads_1st_5_innings,totals_1st_5_innings`
        + `&bookmakers=pinnacle,draftkings,fanduel`
        + `&oddsFormat=american`;

      try {
        const resp = await fetch(url);
        calls++;
        if (!resp.ok) { apiFails++; continue; }
        const data = await resp.json();

        const h2hPairs = [], spreadPairs = [], totalPairs = [];
        for (const book of (data.bookmakers || [])) {
          for (const m of (book.markets || [])) {
            if (m.key === 'h2h_1st_5_innings') {
              const homeOut = m.outcomes?.find(o => o.name === data.home_team);
              const awayOut = m.outcomes?.find(o => o.name === data.away_team);
              if (homeOut && awayOut) {
                h2hPairs.push({
                  book: book.key,
                  home: { odds_probability: americanToImpliedProb(homeOut.price), odds_american: homeOut.price },
                  away: { odds_probability: americanToImpliedProb(awayOut.price), odds_american: awayOut.price },
                });
              }
            } else if (m.key === 'spreads_1st_5_innings') {
              const homeOut = m.outcomes?.find(o => o.name === data.home_team);
              const awayOut = m.outcomes?.find(o => o.name === data.away_team);
              if (homeOut && awayOut) {
                spreadPairs.push({
                  book: book.key,
                  home: { odds_probability: americanToImpliedProb(homeOut.price), odds_american: homeOut.price, point: homeOut.point, line: homeOut.point },
                  away: { odds_probability: americanToImpliedProb(awayOut.price), odds_american: awayOut.price, point: awayOut.point, line: awayOut.point },
                });
              }
            } else if (m.key === 'totals_1st_5_innings') {
              const over = m.outcomes?.find(o => o.name === 'Over');
              const under = m.outcomes?.find(o => o.name === 'Under');
              if (over && under) {
                totalPairs.push({
                  book: book.key,
                  over: { odds_probability: americanToImpliedProb(over.price), odds_american: over.price, point: over.point, line: over.point },
                  under: { odds_probability: americanToImpliedProb(under.price), odds_american: under.price, point: under.point, line: under.point },
                });
              }
            }
          }
        }

        if (h2hPairs.length > 0) {
          const mk = buildConsensusMoneyline(h2hPairs);
          if (mk) { ev.markets.h2h_f5 = _mergeSupplementedMarket(ev.markets.h2h_f5, mk); h2hCount++; }
        }
        if (spreadPairs.length > 0) {
          const sp = buildConsensusSpread(spreadPairs);
          if (sp) { ev.markets.spreads_f5 = _mergeSupplementedMarket(ev.markets.spreads_f5, sp); spreadCount++; }
        }
        if (totalPairs.length > 0) {
          ev.markets.totals_f5 = _mergeSupplementedMarket(ev.markets.totals_f5, buildConsensusTotals(totalPairs));
          totalCount++;
        }
      } catch (err) {
        apiFails++;
      }
    }
  }
  const workers = [];
  for (let i = 0; i < Math.min(CONCURRENCY, candidates.length); i++) workers.push(worker());
  await Promise.all(workers);
  log.info('OddsFeed', `MLB F5 supplement (per-event): ${calls}/${candidates.length} calls, h2h+${h2hCount} spread+${spreadCount} total+${totalCount}, matchFails=${matchFails} apiFails=${apiFails}`);

  // DK scraper fallback: if any MLB events STILL lack h2h_f5 after the
  // TOA per-event supplement, scrape DK directly. DK posts F5 markets
  // on every MLB game hours before SharpAPI/TOA list them — verified
  // 2026-05-03 with Sunday afternoon games (Tampa@Toronto, Detroit@Boston,
  // SF@SD, Seattle@Atlanta) where SharpAPI returned only Kalshi-only
  // h2h stubs and TOA's events list didn't include them yet, but DK had
  // full F5 markets posted. Operator's directive: 100% F5 coverage on
  // MLB regardless of upstream API gaps.
  const stillMissingF5 = [];
  for (const entry of Object.values(parsedEvents)) {
    const arr = Array.isArray(entry) ? entry : [entry];
    for (const ev of arr) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      if (ev.markets && ev.markets.h2h_f5) continue;
      stillMissingF5.push(ev);
    }
  }
  if (stillMissingF5.length === 0) return;

  log.info('OddsFeed', `MLB F5: ${stillMissingF5.length} events still lack F5 after TOA supplement — invoking DK scraper`);
  let dkScrape;
  try {
    const dk = require('./dk-scraper');
    dkScrape = await dk.fetchMlbF5Odds();
  } catch (err) {
    log.warn('OddsFeed', `DK MLB F5 scrape failed: ${err.message}`);
    return;
  }
  if (!dkScrape || !Array.isArray(dkScrape.games) || dkScrape.games.length === 0) {
    log.warn('OddsFeed', 'DK MLB F5 scrape returned no games');
    return;
  }

  // Index DK games by normalized team-pair for matching
  const lwLast = (s) => (s || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').split(/\s+/).filter(Boolean).pop() || '';
  const dkByPair = {};
  for (const g of dkScrape.games) {
    if (!g.homeTeam || !g.awayTeam) continue;
    const k1 = `${lwLast(g.homeTeam)}|${lwLast(g.awayTeam)}`;
    const k2 = `${lwLast(g.awayTeam)}|${lwLast(g.homeTeam)}`;
    dkByPair[k1] = g;
    dkByPair[k2] = { ...g, _flipped: true };
  }

  let dkApplied = 0;
  for (const ev of stillMissingF5) {
    const key = `${lwLast(ev.homeTeam)}|${lwLast(ev.awayTeam)}`;
    const g = dkByPair[key];
    if (!g) continue;

    if (!ev.markets) ev.markets = {};
    // h2h_f5 — flip home/away if DK orientation is opposite
    const flipMl = !!g._flipped;
    const dkHome = flipMl ? g.h2h.away : g.h2h.home;
    const dkAway = flipMl ? g.h2h.home : g.h2h.away;
    ev.markets.h2h_f5 = {
      home: { rawOdds: dkHome.americanOdds, impliedProb: dkHome.impliedProb, fairProb: dkHome.fairProb, displayFairProb: dkHome.fairProb },
      away: { rawOdds: dkAway.americanOdds, impliedProb: dkAway.impliedProb, fairProb: dkAway.fairProb, displayFairProb: dkAway.fairProb },
      books: 1,
      draftkings: { home: dkHome.americanOdds, away: dkAway.americanOdds },
    };
    if (g.spreads) {
      const spHome = flipMl ? g.spreads.away : g.spreads.home;
      const spAway = flipMl ? g.spreads.home : g.spreads.away;
      ev.markets.spreads_f5 = {
        home: { rawOdds: spHome.americanOdds, point: spHome.line, impliedProb: spHome.impliedProb, fairProb: spHome.fairProb, displayFairProb: spHome.fairProb },
        away: { rawOdds: spAway.americanOdds, point: spAway.line, impliedProb: spAway.impliedProb, fairProb: spAway.fairProb, displayFairProb: spAway.fairProb },
        line: spHome.line,
        books: 1,
        draftkings: { home: spHome.americanOdds, away: spAway.americanOdds },
      };
    }
    const totalLines = Object.keys(g.totalsByLine || {});
    if (totalLines.length > 0) {
      // Pick the line closest to median as primary
      const sorted = totalLines.map(parseFloat).sort((a, b) => a - b);
      const primaryLine = sorted[Math.floor(sorted.length / 2)];
      const primary = g.totalsByLine[String(primaryLine)] || g.totalsByLine[totalLines[0]];
      if (primary) {
        ev.markets.totals_f5 = {
          over: { rawOdds: primary.over.americanOdds, point: primary.line, impliedProb: primary.over.impliedProb, fairProb: primary.over.fairProb, displayFairProb: primary.over.fairProb },
          under: { rawOdds: primary.under.americanOdds, point: primary.line, impliedProb: primary.under.impliedProb, fairProb: primary.under.fairProb, displayFairProb: primary.under.fairProb },
          line: primary.line,
          books: 1,
          draftkings: { over: primary.over.americanOdds, under: primary.under.americanOdds },
        };
      }
    }
    dkApplied++;
  }
  log.info('OddsFeed', `MLB F5 DK scrape applied: ${dkApplied} of ${stillMissingF5.length} missing events filled`);
}

/**
 * Fetch 1st-Half markets for NBA from The Odds API and attach them
 * to the existing event cache as: h2h_h1, spreads_h1, totals_h1.
 *
 * IMPORTANT: Same endpoint gotcha as F5 and team_totals — the bulk
 * /odds endpoint returns 422 INVALID_MARKET for h2h_h1/spreads_h1/
 * totals_h1. Verified via probe 2026-04-22:
 *   "Markets not supported by this endpoint: h2h_h1, spreads_h1, totals_h1"
 * This is why the previous bulk-endpoint implementation produced zero
 * 1H data for months. H1 markets live on the per-event endpoint.
 */
async function supplementNbaH1Markets(parsedEvents) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return;

  // Collect candidates — skip events already populated with H1 data.
  const candidates = [];
  for (const entry of Object.values(parsedEvents)) {
    const arr = Array.isArray(entry) ? entry : [entry];
    for (const ev of arr) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      if (ev.markets && ev.markets.h2h_h1 && ev.markets.spreads_h1 && ev.markets.totals_h1) continue;
      candidates.push(ev);
    }
  }
  if (candidates.length === 0) {
    log.info('OddsFeed', 'NBA H1 supplement: no candidates');
    return;
  }

  let calls = 0, matchFails = 0, apiFails = 0;
  let h2hCount = 0, spreadCount = 0, totalCount = 0;
  const CONCURRENCY = 3;
  let idx = 0;
  async function worker() {
    while (idx < candidates.length) {
      const ev = candidates[idx++];
      const resolved = await resolveOddsApiEventId('basketball_nba', ev.homeTeam, ev.awayTeam, ev.commenceTime);
      if (!resolved) { matchFails++; continue; }

      const url = `https://api.the-odds-api.com/v4/sports/basketball_nba/events/${resolved.eventId}/odds`
        + `?apiKey=${theOddsApiKey}`
        + `&regions=us,eu`
        + `&markets=h2h_h1,spreads_h1,totals_h1`
        + `&bookmakers=pinnacle,draftkings,fanduel`
        + `&oddsFormat=american`;

      try {
        const resp = await fetch(url);
        calls++;
        if (!resp.ok) { apiFails++; continue; }
        const data = await resp.json();

        const mlPairs = [], spreadPairs = [], totalPairs = [];
        for (const book of (data.bookmakers || [])) {
          for (const m of (book.markets || [])) {
            if (m.key === 'h2h_h1') {
              const home = m.outcomes?.find(o => o.name === data.home_team);
              const away = m.outcomes?.find(o => o.name === data.away_team);
              if (home && away) {
                mlPairs.push({
                  book: book.key,
                  home: { odds_probability: americanToImpliedProb(home.price), odds_american: home.price },
                  away: { odds_probability: americanToImpliedProb(away.price), odds_american: away.price },
                });
              }
            } else if (m.key === 'spreads_h1') {
              const home = m.outcomes?.find(o => o.name === data.home_team);
              const away = m.outcomes?.find(o => o.name === data.away_team);
              if (home && away) {
                spreadPairs.push({
                  book: book.key,
                  home: { odds_probability: americanToImpliedProb(home.price), odds_american: home.price, point: home.point, line: home.point },
                  away: { odds_probability: americanToImpliedProb(away.price), odds_american: away.price, point: away.point, line: away.point },
                });
              }
            } else if (m.key === 'totals_h1') {
              const over = m.outcomes?.find(o => o.name === 'Over');
              const under = m.outcomes?.find(o => o.name === 'Under');
              if (over && under) {
                totalPairs.push({
                  book: book.key,
                  over: { odds_probability: americanToImpliedProb(over.price), odds_american: over.price, point: over.point, line: over.point },
                  under: { odds_probability: americanToImpliedProb(under.price), odds_american: under.price, point: under.point, line: under.point },
                });
              }
            }
          }
        }

        if (mlPairs.length > 0) {
          const mk = buildConsensusMoneyline(mlPairs);
          if (mk) { ev.markets.h2h_h1 = _mergeSupplementedMarket(ev.markets.h2h_h1, mk); h2hCount++; }
        }
        if (spreadPairs.length > 0) {
          const sp = buildConsensusSpread(spreadPairs);
          if (sp) { ev.markets.spreads_h1 = _mergeSupplementedMarket(ev.markets.spreads_h1, sp); spreadCount++; }
        }
        if (totalPairs.length > 0) {
          ev.markets.totals_h1 = _mergeSupplementedMarket(ev.markets.totals_h1, buildConsensusTotals(totalPairs));
          totalCount++;
        }
      } catch (err) {
        apiFails++;
      }
    }
  }
  const workers = [];
  for (let i = 0; i < Math.min(CONCURRENCY, candidates.length); i++) workers.push(worker());
  await Promise.all(workers);
  log.info('OddsFeed', `NBA H1 supplement (per-event): ${calls}/${candidates.length} calls, h2h+${h2hCount} spread+${spreadCount} total+${totalCount}, matchFails=${matchFails} apiFails=${apiFails}`);
}

/**
 * Fetch team_totals markets from The Odds API and attach them to the
 * existing event cache as `markets.team_totals`. Used as a gap-fill for
 * SharpAPI's Hobby plan which does not currently return team_total data
 * for NBA/MLB/NHL despite those being requested. Called per-sport on the
 * primary refresh cycle so the data is pre-warmed in the cache; RFQ
 * pricing paths pay zero incremental latency vs. full-game totals.
 *
 * The Odds API team_totals payload shape (per outcome):
 *   { name: 'Over'|'Under', description: '<Team Name>', price: <american>, point: <line> }
 * We group outcomes by description (team) into over/under pairs, then
 * emit one bookPair per (book, teamSide) for buildConsensusTeamTotals.
 */
async function supplementTeamTotals(parsedEvents, sport) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return;

  const OA_SPORT_KEYS = {
    basketball_nba: 'basketball_nba',
    baseball_mlb: 'baseball_mlb',
    icehockey_nhl: 'icehockey_nhl',
  };
  const oaSport = OA_SPORT_KEYS[sport];
  if (!oaSport) return;

  // IMPORTANT: The Odds API's bulk /odds endpoint does NOT support
  // team_totals — returns 422 INVALID_MARKET. Same gotcha as F5.
  // team_totals lives on the per-event endpoint /events/{id}/odds.
  // We resolve each parsed event to its Odds API event ID, then fetch
  // per-event with bounded concurrency (mirrors supplementMlbF5Markets).

  // Collect candidates (skip events that already have team_totals populated
  // from SharpAPI — no need to overwrite with Odds API data).
  const candidates = [];
  for (const entry of Object.values(parsedEvents)) {
    const arr = Array.isArray(entry) ? entry : [entry];
    for (const ev of arr) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      if (ev.markets && ev.markets.team_totals) continue;
      candidates.push(ev);
    }
  }
  if (candidates.length === 0) {
    log.info('OddsFeed', `${sport} team_totals supplement: no candidates`);
    return;
  }

  let calls = 0, matchFails = 0, apiFails = 0, attached = 0, emptyPayload = 0;
  const CONCURRENCY = 3;
  let idx = 0;
  async function worker() {
    while (idx < candidates.length) {
      const ev = candidates[idx++];
      const resolved = await resolveOddsApiEventId(oaSport, ev.homeTeam, ev.awayTeam, ev.commenceTime);
      if (!resolved) { matchFails++; continue; }

      // Fetch BOTH primary team_totals AND alternate_team_totals so the
      // per-line consensus has ±1/±2 alts available. Without alt fetching,
      // PX RFQs for non-primary lines (e.g. Cavaliers +112 when primary
      // is +111.5) returned null fair-prob even when DK/FD posted them.
      const url = `https://api.the-odds-api.com/v4/sports/${oaSport}/events/${resolved.eventId}/odds`
        + `?apiKey=${theOddsApiKey}`
        + `&regions=us,eu`
        + `&markets=team_totals,alternate_team_totals`
        + `&bookmakers=pinnacle,draftkings,fanduel`
        + `&oddsFormat=american`;

      try {
        const resp = await fetch(url);
        calls++;
        if (!resp.ok) { apiFails++; continue; }
        const data = await resp.json();

        // Build bookPairs: one entry per (book × teamSide × line) with
        // { over, under }. Per-event response shape:
        // data.bookmakers[].markets[].outcomes[] where each outcome has
        // { name:'Over'|'Under', description:<team>, price, point }.
        // The alt market posts multiple over/under pairs per team — group
        // by (team, line) so each line gets its own bookPair.
        const bookPairs = [];
        for (const book of (data.bookmakers || [])) {
          for (const m of (book.markets || [])) {
            if (m.key !== 'team_totals' && m.key !== 'alternate_team_totals') continue;
            // Group outcomes by (team, line) — alt markets emit many
            // over/under pairs per team across different lines.
            const byTeamLine = {};
            for (const o of (m.outcomes || [])) {
              const team = o.description;
              if (!team || o.point == null) continue;
              const tlKey = team + '|' + o.point;
              if (!byTeamLine[tlKey]) byTeamLine[tlKey] = { team, line: o.point };
              if (o.name === 'Over') {
                byTeamLine[tlKey].over = {
                  odds_probability: americanToImpliedProb(o.price),
                  odds_american: o.price,
                  line: o.point,
                };
              } else if (o.name === 'Under') {
                byTeamLine[tlKey].under = {
                  odds_probability: americanToImpliedProb(o.price),
                  odds_american: o.price,
                  line: o.point,
                };
              }
            }
            for (const entry of Object.values(byTeamLine)) {
              if (!entry.over || !entry.under) continue;
              let teamSide = null;
              if (entry.team === data.home_team) teamSide = 'home';
              else if (entry.team === data.away_team) teamSide = 'away';
              else continue;
              bookPairs.push({
                book: book.key,
                teamSide,
                over: entry.over,
                under: entry.under,
              });
            }
          }
        }

        if (bookPairs.length === 0) {
          emptyPayload++;
          continue;
        }

        const tt = buildConsensusTeamTotals(bookPairs);
        if (tt) {
          ev.markets.team_totals = tt;
          attached++;
        }
      } catch (err) {
        apiFails++;
      }
    }
  }
  const workers = [];
  for (let i = 0; i < Math.min(CONCURRENCY, candidates.length); i++) workers.push(worker());
  await Promise.all(workers);
  log.info('OddsFeed', `${sport} team_totals supplement (per-event): ${calls}/${candidates.length} calls, ${attached} attached, matchFails=${matchFails} apiFails=${apiFails} emptyPayload=${emptyPayload}`);
}

// ---------------------------------------------------------------------------
// PINNACLE SUPPLEMENT — fetch Pinnacle odds from The Odds API and convert
// to SharpAPI-format rows so they merge into the multi-book de-vig
// ---------------------------------------------------------------------------

// Maps our sport keys to The Odds API sport keys for Pinnacle supplement
const PINNACLE_SPORT_MAP = {
  'basketball_nba': 'basketball_nba',
  'baseball_mlb': 'baseball_mlb',
  'icehockey_nhl': 'icehockey_nhl',
  // Soccer leagues — The Odds API uses per-league sport keys that match our
  // internal ones 1:1. Adds Pinnacle + DK + FD as supplement books on top of
  // SharpAPI's DK/FD-only coverage. Pinnacle is widely considered the sharpest
  // book for soccer, so including it meaningfully tightens fair-prob estimates.
  // If a league has no active Pinnacle coverage for a given cycle, fetchPinnacleRows
  // returns [] and the merge is a no-op — safe to add speculatively.
  'soccer_epl': 'soccer_epl',
  'soccer_spain_la_liga': 'soccer_spain_la_liga',
  'soccer_italy_serie_a': 'soccer_italy_serie_a',
  'soccer_germany_bundesliga': 'soccer_germany_bundesliga',
  'soccer_france_ligue_one': 'soccer_france_ligue_one',
  'soccer_uefa_champs_league': 'soccer_uefa_champs_league',
  'soccer_uefa_europa_league': 'soccer_uefa_europa_league',
  'soccer_usa_mls': 'soccer_usa_mls',
  'soccer_usa_nwsl': 'soccer_usa_nwsl',
  'soccer_mexico_ligamx': 'soccer_mexico_ligamx',
  'soccer_brazil_campeonato': 'soccer_brazil_campeonato',
  'soccer_conmebol_libertadores': 'soccer_conmebol_libertadores',
  // MMA — Pinnacle posts UFC/major MMA events, adds a sharper reference to
  // the existing SharpAPI-only coverage plus the DK scraper total_rounds market.
  'mma_mixed_martial_arts': 'mma_mixed_martial_arts',
  // Tennis intentionally omitted: The Odds API uses per-tournament sport keys
  // (tennis_atp_french_open, tennis_atp_us_open, etc.) rather than a generic
  // "tennis" key, so a 1:1 map doesn't work without tournament-aware routing.
};

// Market key mapping: The Odds API market → SharpAPI market_type (per sport)
function oddsApiToSharpMarket(marketKey, sport) {
  if (marketKey === 'h2h') return 'moneyline';
  if (marketKey === 'spreads') {
    if (sport === 'baseball_mlb') return 'run_line';
    if (sport === 'icehockey_nhl') return 'puck_line';
    return 'point_spread';
  }
  if (marketKey === 'totals') {
    if (sport === 'baseball_mlb') return 'total_runs';
    if (sport === 'icehockey_nhl') return 'total_goals';
    return 'total_points';
  }
  return null;
}

/**
 * Fetch Pinnacle + FanDuel odds from The Odds API and convert to SharpAPI-format rows.
 * Supplements SharpAPI data to ensure Pinnacle and FanDuel coverage for display.
 * Returns array of rows compatible with SharpAPI's format, or empty array on failure.
 */
async function fetchPinnacleRows(sport) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  const oddsApiSport = PINNACLE_SPORT_MAP[sport];
  if (!theOddsApiKey || !oddsApiSport) return [];

  // Fetch Pinnacle + DraftKings + FanDuel from The Odds API — these supplement
  // SharpAPI's (often incomplete) coverage so we have guaranteed book data for
  // all games regardless of SharpAPI's 50-row cap.
  const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/odds`
    + `?apiKey=${theOddsApiKey}`
    + `&regions=us,eu`
    + `&markets=h2h,spreads,totals`
    + `&bookmakers=pinnacle,draftkings,fanduel`
    + `&oddsFormat=american`;

  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      log.warn('OddsFeed', `Odds API supplement fetch failed (${resp.status}) for ${sport}`);
      return [];
    }

    const remaining = resp.headers.get('x-requests-remaining');
    const used = resp.headers.get('x-requests-used');
    if (remaining != null) {
      log.info('OddsFeed', `The Odds API usage (supplement): ${used} used, ${remaining} remaining`);
    }

    const events = await safeJsonFetch(resp);
    const rows = [];

    for (const event of events) {
      // Process ALL bookmakers, not just Pinnacle
      for (const book of (event.bookmakers || [])) {
        const bookKey = book.key; // 'pinnacle', 'draftkings', 'fanduel'
        for (const market of (book.markets || [])) {
          const marketType = oddsApiToSharpMarket(market.key, sport);
          if (!marketType) continue;

          for (const outcome of (market.outcomes || [])) {
            const isHome = outcome.name === event.home_team;
            const isAway = outcome.name === event.away_team;
            const isOver = outcome.name === 'Over';
            const isUnder = outcome.name === 'Under';

            let selectionType;
            if (market.key === 'totals') {
              selectionType = isOver ? 'over' : isUnder ? 'under' : null;
            } else {
              selectionType = isHome ? 'home' : isAway ? 'away' : null;
            }
            if (!selectionType) continue;

            rows.push({
              event_id: event.id,
              home_team: event.home_team,
              away_team: event.away_team,
              event_start_time: event.commence_time,
              sportsbook: bookKey,
              market_type: marketType,
              selection_type: selectionType,
              odds_american: outcome.price,
              odds_probability: americanToImpliedProb(outcome.price),
              line: outcome.point != null ? outcome.point : null,
            });
          }
        }
      }
    }

    // Count rows per book for logging
    const byBook = {};
    for (const r of rows) byBook[r.sportsbook] = (byBook[r.sportsbook] || 0) + 1;
    log.info('OddsFeed', `Odds API supplement: ${rows.length} rows for ${sport} (${events.length} events) — ${JSON.stringify(byBook)}`);
    return rows;
  } catch (err) {
    log.warn('OddsFeed', `Odds API supplement fetch error for ${sport}: ${err.message}`);
    return [];
  }
}

// ---------------------------------------------------------------------------
// THE ODDS API FALLBACK (for sports SharpAPI free tier doesn't cover)
// ---------------------------------------------------------------------------

/**
 * Fetch odds for dynamic sports (e.g., tennis) where tournament keys change.
 * Discovers active tournaments from The Odds API, fetches odds for each,
 * and merges all events into the cache under the generic sport key.
 */
async function fetchDynamicSports(sport, fallback, apiKey) {
  // Step 1: discover active tournaments matching the prefix
  const sportsResp = await fetch(`https://api.the-odds-api.com/v4/sports/?apiKey=${apiKey}`);
  if (!sportsResp.ok) throw new Error(`The Odds API sports list: ${sportsResp.status}`);
  const allSports = await safeJsonFetch(sportsResp);
  const activeTournaments = allSports.filter(s => s.key.startsWith(fallback.sportPrefix) && s.active);

  if (activeTournaments.length === 0) {
    log.warn('OddsFeed', `No active ${sport} tournaments found on The Odds API`);
    return {};
  }
  log.info('OddsFeed', `Found ${activeTournaments.length} active ${sport} tournaments: ${activeTournaments.map(t => t.key).join(', ')}`);

  // Step 2: fetch odds for each active tournament
  const allEvents = [];
  for (const tournament of activeTournaments) {
    const url = `https://api.the-odds-api.com/v4/sports/${tournament.key}/odds`
      + `?apiKey=${apiKey}`
      + `&regions=us,eu`
      + `&markets=${fallback.markets}`
      + `&bookmakers=${fallback.bookmakers}`
      + `&oddsFormat=american`;

    try {
      const resp = await fetch(url);
      if (!resp.ok) {
        log.warn('OddsFeed', `The Odds API ${resp.status} for ${tournament.key}`);
        continue;
      }
      const remaining = resp.headers.get('x-requests-remaining');
      if (remaining != null) log.debug('OddsFeed', `The Odds API: ${remaining} requests remaining`);
      const events = await safeJsonFetch(resp);
      allEvents.push(...events);
      log.info('OddsFeed', `Got ${events.length} events from ${tournament.key}`);
    } catch (err) {
      log.warn('OddsFeed', `Failed to fetch ${tournament.key}: ${err.message}`);
    }
  }

  log.info('OddsFeed', `Total ${sport} events across all tournaments: ${allEvents.length}`);

  // Step 3: parse into cache format (same as regular fetchFromTheOddsApi)
  const parsed = {};
  for (const event of allEvents) {
    const key = normalizeEventKey(event.home_team, event.away_team);
    const allBooks = event.bookmakers || [];
    if (allBooks.length === 0) continue;

    const markets = {};

    // Moneyline (h2h)
    const mlPairs = [];
    for (const book of allBooks) {
      const mlMarket = book.markets?.find(m => m.key === 'h2h');
      if (!mlMarket) continue;
      const home = mlMarket.outcomes?.find(o => o.name === event.home_team);
      const away = mlMarket.outcomes?.find(o => o.name === event.away_team);
      if (home && away) {
        mlPairs.push({
          book: book.key,
          home: { odds_probability: americanToImpliedProb(home.price), odds_american: home.price },
          away: { odds_probability: americanToImpliedProb(away.price), odds_american: away.price },
        });
      }
    }
    if (mlPairs.length > 0) {
      markets.h2h = buildConsensusMoneyline(mlPairs);
    }

    // Spreads (game handicaps for tennis)
    const spreadPairs = [];
    for (const book of allBooks) {
      const sMarket = book.markets?.find(m => m.key === 'spreads');
      if (!sMarket) continue;
      const home = sMarket.outcomes?.find(o => o.name === event.home_team);
      const away = sMarket.outcomes?.find(o => o.name === event.away_team);
      if (home && away) {
        spreadPairs.push({
          book: book.key,
          home: { odds_probability: americanToImpliedProb(home.price), odds_american: home.price, point: home.point, line: home.point },
          away: { odds_probability: americanToImpliedProb(away.price), odds_american: away.price, point: away.point, line: away.point },
        });
      }
    }
    if (spreadPairs.length > 0) {
      markets.spreads = buildConsensusSpread(spreadPairs);
    }

    // Totals (total games for tennis)
    const totalPairs = [];
    for (const book of allBooks) {
      const tMarket = book.markets?.find(m => m.key === 'totals');
      if (!tMarket) continue;
      const over = tMarket.outcomes?.find(o => o.name === 'Over');
      const under = tMarket.outcomes?.find(o => o.name === 'Under');
      if (over && under) {
        totalPairs.push({
          book: book.key,
          over: { odds_probability: americanToImpliedProb(over.price), odds_american: over.price, point: over.point, line: over.point },
          under: { odds_probability: americanToImpliedProb(under.price), odds_american: under.price, point: under.point, line: under.point },
        });
      }
    }
    if (totalPairs.length > 0) {
      markets.totals = buildConsensusTotals(totalPairs);
    }

    if (Object.keys(markets).length > 0) {
      parsed[key] = {
        homeTeam: event.home_team,
        awayTeam: event.away_team,
        commenceTime: event.commence_time,
        markets,
      };
    }
  }

  // Store in cache under the generic sport key
  oddsCache[sport] = {
    events: parsed,
    fetchedAt: Date.now(),
  };

  // Duplicate-event audit: log WARN if two cache entries represent the
  // same game under different team-name strings. This is the signature
  // of the April 2026 Red Sox bug — SharpAPI stored "BOS Red Sox vs
  // New York Yankees" while The Odds API stored "Boston Red Sox vs
  // New York Yankees" for the SAME game, producing two cache keys
  // that the closest-by-time matcher couldn't merge. Catching this at
  // ingest gives us a chance to add an entry to TEAM_ABBREV_TO_CANONICAL
  // before any RFQs get mispriced.
  auditCacheForDuplicateEvents(sport);

  return parsed;
}

// Helpers + main loop for the duplicate-event audit.
function _teamTail(name) {
  // Last 1-2 tokens, lowercased. "BOS Red Sox" and "Boston Red Sox"
  // both → "red sox"; "Chicago White Sox" → "white sox" (not confused
  // with Red Sox). Singletons fall through to just their one token.
  const toks = String(name || '').trim().split(/\s+/).filter(Boolean);
  if (toks.length === 0) return '';
  return toks.slice(-2).join(' ').toLowerCase();
}
function auditCacheForDuplicateEvents(sport) {
  const cache = oddsCache[sport];
  if (!cache || !cache.events) return;
  // Group events by (home-tail, away-tail, date) — collisions under
  // different full names signal a naming-variant bug.
  const groups = {};
  for (const [key, entry] of Object.entries(cache.events)) {
    const list = Array.isArray(entry) ? entry : [entry];
    for (const ev of list) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      const date = ev.commenceTime ? String(ev.commenceTime).substring(0, 10) : 'nodate';
      const groupKey = _teamTail(ev.homeTeam) + '|' + _teamTail(ev.awayTeam) + '|' + date;
      if (!groups[groupKey]) groups[groupKey] = [];
      groups[groupKey].push({ cacheKey: key, homeTeam: ev.homeTeam, awayTeam: ev.awayTeam });
    }
  }
  for (const [gk, members] of Object.entries(groups)) {
    if (members.length < 2) continue;
    // Only warn if at least two members have DIFFERENT full team-name
    // strings (otherwise it's just the same event under one key, fine).
    const distinct = new Set(members.map(m => m.homeTeam + '|' + m.awayTeam));
    if (distinct.size < 2) continue;
    const detail = members.map(m => `"${m.homeTeam}" vs "${m.awayTeam}"`).join(' AND ');
    log.warn('OddsFeed', `Duplicate-event bug detected in ${sport} cache — same game cached under different team-name variants: ${detail}. Add entries to TEAM_ABBREV_TO_CANONICAL to collapse.`);
  }
}

async function fetchFromTheOddsApi(sport) {
  const fallback = ODDS_API_FALLBACK[sport];
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) {
    throw new Error(`No THE_ODDS_API_KEY set for fallback sport ${sport}`);
  }

  // Dynamic sports (e.g., tennis) — discover active tournaments first
  if (fallback.dynamic) {
    return fetchDynamicSports(sport, fallback, theOddsApiKey);
  }

  const url = `https://api.the-odds-api.com/v4/sports/${fallback.oddsApiSport}/odds`
    + `?apiKey=${theOddsApiKey}`
    + `&regions=us,eu`
    + `&markets=${fallback.markets}`
    + `&bookmakers=${fallback.bookmakers}`
    + `&oddsFormat=american`;

  log.info('OddsFeed', `Fetching ${sport} from The Odds API (fallback)...`);

  const resp = await fetch(url);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`The Odds API ${resp.status} for ${sport}: ${text}`);
  }

  const remaining = resp.headers.get('x-requests-remaining');
  const used = resp.headers.get('x-requests-used');
  if (remaining != null) {
    log.info('OddsFeed', `The Odds API usage: ${used} used, ${remaining} remaining`);
  }

  const events = await safeJsonFetch(resp);
  log.info('OddsFeed', `Got ${events.length} events for ${sport} from The Odds API`);

  // Parse into same cache format as SharpAPI
  const parsed = {};
  for (const event of events) {
    const key = normalizeEventKey(event.home_team, event.away_team);
    // Collect all books' odds and build consensus
    const allBooks = event.bookmakers || [];
    if (allBooks.length === 0) continue;

    const markets = {};

    // Moneyline (h2h)
    const mlPairs = [];
    for (const book of allBooks) {
      const mlMarket = book.markets?.find(m => m.key === 'h2h');
      if (!mlMarket) continue;
      const home = mlMarket.outcomes?.find(o => o.name === event.home_team);
      const away = mlMarket.outcomes?.find(o => o.name === event.away_team);
      if (home && away) {
        mlPairs.push({
          book: book.key,
          home: { odds_probability: americanToImpliedProb(home.price), odds_american: home.price },
          away: { odds_probability: americanToImpliedProb(away.price), odds_american: away.price },
        });
      }
    }
    if (mlPairs.length > 0) {
      // Exclude Kalshi from averaging input — prediction-market thinness
      // corrupts consensus. Still populated as a display column below.
      const avgPairs = excludeKalshiFromConsensus(mlPairs);
      const fairHome = [], fairAway = [];
      for (const p of avgPairs) {
        const [fh, fa] = deVig2Way(p.home.odds_probability, p.away.odds_probability);
        fairHome.push(fh);
        fairAway.push(fa);
      }
      // Pinnacle floor only on heavy favorites (>65%) where de-vig over-corrects.
      // Use de-vigged Pinnacle (not raw implied) to avoid double-vig.
      // Kalshi NOT used as fallback floor — operator intent: reference only.
      const pinPair = mlPairs.find(p => p.book === 'pinnacle');
      const pinFairH = pinPair ? deVig2Way(pinPair.home.odds_probability, pinPair.away.odds_probability)[0] : 0;
      const pinFairA = pinPair ? deVig2Way(pinPair.home.odds_probability, pinPair.away.odds_probability)[1] : 0;
      const dvH = avg(fairHome), dvA = avg(fairAway);
      const flrH = pinPair ? pinFairH : 0;
      const flrA = pinPair ? pinFairA : 0;
      const maxHome = dvH >= 0.65 ? Math.max(dvH, flrH) : dvH;
      const maxAway = dvA >= 0.65 ? Math.max(dvA, flrA) : dvA;
      // Find named books for per-book display columns (previously unpopulated
      // in this fallback path — dashboard book columns were always blank).
      const findBook = (name) => mlPairs.find(p => p.book === name);
      const pinBook = findBook('pinnacle');
      const fdBook = findBook('fanduel');
      const dkBook = findBook('draftkings');
      const klBook = findBook('kalshi');
      markets.h2h = {
        home: { rawOdds: mlPairs[0].home.odds_american, impliedProb: mlPairs[0].home.odds_probability, fairProb: maxHome, displayFairProb: avg(fairHome) },
        away: { rawOdds: mlPairs[0].away.odds_american, impliedProb: mlPairs[0].away.odds_probability, fairProb: maxAway, displayFairProb: avg(fairAway) },
        books: mlPairs.length,
        pinnacle: pinBook ? { home: pinBook.home.odds_american, away: pinBook.away.odds_american } : null,
        fanduel: fdBook ? { home: fdBook.home.odds_american, away: fdBook.away.odds_american } : null,
        draftkings: dkBook ? { home: dkBook.home.odds_american, away: dkBook.away.odds_american } : null,
        kalshi: klBook ? { home: klBook.home.odds_american, away: klBook.away.odds_american } : null,
      };
    }

    // Spreads
    const spreadPairs = [];
    for (const book of allBooks) {
      const sMarket = book.markets?.find(m => m.key === 'spreads');
      if (!sMarket) continue;
      const home = sMarket.outcomes?.find(o => o.name === event.home_team);
      const away = sMarket.outcomes?.find(o => o.name === event.away_team);
      if (home && away) {
        spreadPairs.push({
          book: book.key,
          home: { odds_probability: americanToImpliedProb(home.price), odds_american: home.price, point: home.point },
          away: { odds_probability: americanToImpliedProb(away.price), odds_american: away.price, point: away.point },
        });
      }
    }
    if (spreadPairs.length > 0) {
      const avgSpreadPairs = excludeKalshiFromConsensus(spreadPairs);
      const fairHome = [], fairAway = [];
      for (const p of avgSpreadPairs) {
        const [fh, fa] = deVig2Way(p.home.odds_probability, p.away.odds_probability);
        fairHome.push(fh);
        fairAway.push(fa);
      }
      const pinSpread = spreadPairs.find(p => p.book === 'pinnacle');
      const pinFairH = pinSpread ? deVig2Way(pinSpread.home.odds_probability, pinSpread.away.odds_probability)[0] : 0;
      const pinFairA = pinSpread ? deVig2Way(pinSpread.home.odds_probability, pinSpread.away.odds_probability)[1] : 0;
      const dvSHome = avg(fairHome), dvSAway = avg(fairAway);
      const flrSH = pinSpread ? pinFairH : 0;
      const flrSA = pinSpread ? pinFairA : 0;
      const maxSHome = dvSHome >= 0.65 ? Math.max(dvSHome, flrSH) : dvSHome;
      const maxSAway = dvSAway >= 0.65 ? Math.max(dvSAway, flrSA) : dvSAway;
      const findBook = (name) => spreadPairs.find(p => p.book === name);
      const pinBook = findBook('pinnacle');
      const fdBook = findBook('fanduel');
      const dkBook = findBook('draftkings');
      const klBook = findBook('kalshi');
      markets.spreads = {
        home: { rawOdds: spreadPairs[0].home.odds_american, point: spreadPairs[0].home.point, impliedProb: spreadPairs[0].home.odds_probability, fairProb: maxSHome, displayFairProb: avg(fairHome) },
        away: { rawOdds: spreadPairs[0].away.odds_american, point: spreadPairs[0].away.point, impliedProb: spreadPairs[0].away.odds_probability, fairProb: maxSAway, displayFairProb: avg(fairAway) },
        line: spreadPairs[0].home.point,
        books: spreadPairs.length,
        pinnacle: pinBook ? { home: pinBook.home.odds_american, away: pinBook.away.odds_american } : null,
        fanduel: fdBook ? { home: fdBook.home.odds_american, away: fdBook.away.odds_american } : null,
        draftkings: dkBook ? { home: dkBook.home.odds_american, away: dkBook.away.odds_american } : null,
        kalshi: klBook ? { home: klBook.home.odds_american, away: klBook.away.odds_american } : null,
      };
    }

    // Totals
    const totalPairs = [];
    for (const book of allBooks) {
      const tMarket = book.markets?.find(m => m.key === 'totals');
      if (!tMarket) continue;
      const over = tMarket.outcomes?.find(o => o.name === 'Over');
      const under = tMarket.outcomes?.find(o => o.name === 'Under');
      if (over && under) {
        totalPairs.push({
          book: book.key,
          over: { odds_probability: americanToImpliedProb(over.price), odds_american: over.price, point: over.point },
          under: { odds_probability: americanToImpliedProb(under.price), odds_american: under.price, point: under.point },
        });
      }
    }
    if (totalPairs.length > 0) {
      const avgTotalPairs = excludeKalshiFromConsensus(totalPairs);
      const fairOver = [], fairUnder = [];
      for (const p of avgTotalPairs) {
        const [fo, fu] = deVig2Way(p.over.odds_probability, p.under.odds_probability);
        fairOver.push(fo);
        fairUnder.push(fu);
      }
      const pinTotal = totalPairs.find(p => p.book === 'pinnacle');
      const pinFairO = pinTotal ? deVig2Way(pinTotal.over.odds_probability, pinTotal.under.odds_probability)[0] : 0;
      const pinFairU = pinTotal ? deVig2Way(pinTotal.over.odds_probability, pinTotal.under.odds_probability)[1] : 0;
      const dvTOver = avg(fairOver), dvTUnder = avg(fairUnder);
      const flrTO = pinTotal ? pinFairO : 0;
      const flrTU = pinTotal ? pinFairU : 0;
      const maxTOver = dvTOver >= 0.65 ? Math.max(dvTOver, flrTO) : dvTOver;
      const maxTUnder = dvTUnder >= 0.65 ? Math.max(dvTUnder, flrTU) : dvTUnder;
      const findBook = (name) => totalPairs.find(p => p.book === name);
      const pinBook = findBook('pinnacle');
      const fdBook = findBook('fanduel');
      const dkBook = findBook('draftkings');
      const klBook = findBook('kalshi');
      markets.totals = {
        over: { rawOdds: totalPairs[0].over.odds_american, point: totalPairs[0].over.point, impliedProb: totalPairs[0].over.odds_probability, fairProb: maxTOver, displayFairProb: avg(fairOver) },
        under: { rawOdds: totalPairs[0].under.odds_american, point: totalPairs[0].under.point, impliedProb: totalPairs[0].under.odds_probability, fairProb: maxTUnder, displayFairProb: avg(fairUnder) },
        line: totalPairs[0].over.point,
        books: totalPairs.length,
        pinnacle: pinBook ? { over: pinBook.over.odds_american, under: pinBook.under.odds_american } : null,
        fanduel: fdBook ? { over: fdBook.over.odds_american, under: fdBook.under.odds_american } : null,
        draftkings: dkBook ? { over: dkBook.over.odds_american, under: dkBook.under.odds_american } : null,
        kalshi: klBook ? { over: klBook.over.odds_american, under: klBook.under.odds_american } : null,
      };
    }

    // BTTS (Both Teams To Score) — simple 2-way Yes/No
    const bttsPairs = [];
    for (const book of allBooks) {
      const bMarket = book.markets?.find(m => m.key === 'btts');
      if (!bMarket) continue;
      const yes = bMarket.outcomes?.find(o => o.name === 'Yes');
      const no = bMarket.outcomes?.find(o => o.name === 'No');
      if (yes && no) {
        bttsPairs.push({
          yes: { odds_probability: americanToImpliedProb(yes.price), odds_american: yes.price },
          no: { odds_probability: americanToImpliedProb(no.price), odds_american: no.price },
        });
      }
    }
    if (bttsPairs.length > 0) {
      const fairYes = [], fairNo = [];
      for (const p of bttsPairs) {
        const [fy, fn] = deVig2Way(p.yes.odds_probability, p.no.odds_probability);
        fairYes.push(fy);
        fairNo.push(fn);
      }
      const dvYes = avg(fairYes), dvNo = avg(fairNo);
      markets.btts = {
        yes: { rawOdds: bttsPairs[0].yes.odds_american, impliedProb: bttsPairs[0].yes.odds_probability, fairProb: dvYes, displayFairProb: dvYes },
        no: { rawOdds: bttsPairs[0].no.odds_american, impliedProb: bttsPairs[0].no.odds_probability, fairProb: dvNo, displayFairProb: dvNo },
        books: bttsPairs.length,
      };
    }

    // Double Chance — 3-way (1X, X2, 12)
    const dcPairs = [];
    for (const book of allBooks) {
      const dMarket = book.markets?.find(m => m.key === 'double_chance');
      if (!dMarket) continue;
      // Outcome names from The Odds API: "Home/Draw", "Away/Draw", "Home/Away"
      // Some books use: "1X", "X2", "12"
      const outcomes = dMarket.outcomes || [];
      const find = (patterns) => outcomes.find(o => {
        const name = (o.name || '').toLowerCase().replace(/\s+/g, '');
        return patterns.some(p => name === p || name.includes(p));
      });
      const oneX = find(['1x', 'homeordraw', 'home/draw', 'homedraw', event.home_team?.toLowerCase() + '/draw']);
      const xTwo = find(['x2', 'awayordraw', 'away/draw', 'awaydraw', 'draw/' + event.away_team?.toLowerCase()]);
      const oneTwo = find(['12', 'homeoraway', 'home/away', 'homeaway', event.home_team?.toLowerCase() + '/' + event.away_team?.toLowerCase()]);
      if (oneX && xTwo && oneTwo) {
        dcPairs.push({
          oneX: { odds_probability: americanToImpliedProb(oneX.price), odds_american: oneX.price },
          xTwo: { odds_probability: americanToImpliedProb(xTwo.price), odds_american: xTwo.price },
          oneTwo: { odds_probability: americanToImpliedProb(oneTwo.price), odds_american: oneTwo.price },
        });
      }
    }
    if (dcPairs.length > 0) {
      const fair1X = [], fairX2 = [], fair12 = [];
      for (const p of dcPairs) {
        const [f1x, fx2, f12] = deVigDoubleChance(p.oneX.odds_probability, p.xTwo.odds_probability, p.oneTwo.odds_probability);
        fair1X.push(f1x);
        fairX2.push(fx2);
        fair12.push(f12);
      }
      const dv1X = avg(fair1X), dvX2 = avg(fairX2), dv12 = avg(fair12);
      markets.double_chance = {
        '1X': { rawOdds: dcPairs[0].oneX.odds_american, impliedProb: dcPairs[0].oneX.odds_probability, fairProb: dv1X, displayFairProb: dv1X },
        'X2': { rawOdds: dcPairs[0].xTwo.odds_american, impliedProb: dcPairs[0].xTwo.odds_probability, fairProb: dvX2, displayFairProb: dvX2 },
        '12': { rawOdds: dcPairs[0].oneTwo.odds_american, impliedProb: dcPairs[0].oneTwo.odds_probability, fairProb: dv12, displayFairProb: dv12 },
        books: dcPairs.length,
      };
    }

    if (Object.keys(markets).length > 0) {
      if (!parsed[key]) parsed[key] = [];
      parsed[key].push({
        homeTeam: event.home_team,
        awayTeam: event.away_team,
        commenceTime: event.commence_time,
        eventId: event.id,
        markets,
      });
    }
  }

  oddsCache[sport] = { fetchedAt: Date.now(), events: parsed };
  const totalEvents = Object.values(parsed).reduce((s, arr) => s + arr.length, 0);
  log.info('OddsFeed', `Cached ${totalEvents} events (${Object.keys(parsed).length} matchups) for ${sport} (The Odds API fallback)`);
  // Audit: same check as SharpAPI path (see auditCacheForDuplicateEvents).
  auditCacheForDuplicateEvents(sport);
  return parsed;
}

// ---------------------------------------------------------------------------
// CONSENSUS BUILDERS — de-vig each book, then average fair probs
// ---------------------------------------------------------------------------

/**
 * Group odds rows into book-level pairs (home+away for the same book).
 */
function getBookPairs(odds, marketType) {
  const filtered = marketType ? odds.filter(r => r.market_type === marketType) : odds;
  const byBook = {};
  for (const row of filtered) {
    if (!byBook[row.sportsbook]) byBook[row.sportsbook] = {};
    byBook[row.sportsbook][row.selection_type] = row;
  }
  // Only return books that have both sides
  return Object.entries(byBook)
    .filter(([_, sides]) => sides.home && sides.away)
    .map(([book, sides]) => ({ book, home: sides.home, away: sides.away }));
}

function getBookPairsForTotals(odds) {
  // Key by (sportsbook, LINE) so Over at line X only pairs with Under at
  // line X from the same book. Port of the 5ad919f team_total fix — the
  // commit message flagged this function as having the same latent bug.
  //
  // Root-cause fix for 2026-04-24 CLE @ TOR U 8.5 mispricing
  // (parlay 019dc030, our fair 90.36% vs book consensus 55.5% — 35pp error).
  // Prior version keyed by (sportsbook, selection_type) only, so when
  // SharpAPI included alt total rows in the primary feed for one book
  // (observed on MLB totals — a single feed response can include
  // Over/Under at 7.5, 8, 8.5, 9), the last-written `under` row
  // overwrote earlier ones — producing e.g. Over 8.5 paired with
  // Under 6.5, de-vigging to a 90%/10% split when the true primary
  // was ~55%/45%.
  //
  // Rows without a `line` field are dropped — can't pair safely.
  // Books that post only one side at a given line are silently dropped
  // rather than mispaired.
  const byKey = {};
  for (const row of odds) {
    if (row.line == null) continue;
    const dir = row.selection_type;
    if (dir !== 'over' && dir !== 'under') continue;
    const key = `${row.sportsbook}|${row.line}`;
    if (!byKey[key]) byKey[key] = { book: row.sportsbook, line: row.line };
    byKey[key][dir] = row;
  }
  return Object.values(byKey).filter(p => p.over && p.under);
}

/**
 * Group team_total odds by sportsbook and team side (home/away).
 * SharpAPI team_total selection_type: "home_over", "home_under", "away_over", "away_under"
 */
function getBookPairsForTeamTotals(odds) {
  // Key by (book, side, LINE) to guarantee Over/Under pair at the SAME
  // line per book.
  //
  // Root-cause fix for 2026-04-23 ATL Braves mispricing
  // (parlay 019dbae7-632b-7647): prior version keyed by (book, side)
  // only, so when a book posted multiple alt lines for one team
  // (common on FanDuel MLB team_totals — a single feed response can
  // include Under 4.5, 5.5, 6.5 for the same team), the last-written
  // `under` row overwrote earlier ones. Over 4.5 (-136) ended up
  // paired with Under 6.5 (-225), producing a 127% overround.
  // Proportional de-vig then assigned ~45% to the Over side when
  // real FanDuel-fair was ~55% — a ~10pp miss in the losing direction.
  //
  // With line-keyed pairing, each line gets its own over/under pair
  // (matching by same-book same-line). Only pairs where BOTH Over and
  // Under exist at that line flow downstream to buildConsensusTeamTotals.
  const byKey = {};
  for (const row of odds) {
    // Determine team side and direction from selection_type
    const st = row.selection_type || '';
    let side, dir;
    if (st.includes('home') && st.includes('over')) { side = 'home'; dir = 'over'; }
    else if (st.includes('home') && st.includes('under')) { side = 'home'; dir = 'under'; }
    else if (st.includes('away') && st.includes('over')) { side = 'away'; dir = 'over'; }
    else if (st.includes('away') && st.includes('under')) { side = 'away'; dir = 'under'; }
    else {
      // Fallback: try selection field
      const sel = (row.selection || '').toLowerCase();
      if (sel.includes('over')) dir = 'over';
      else if (sel.includes('under')) dir = 'under';
      else continue;
      // Determine side from home/away team name match
      side = row.selection_type === 'home' ? 'home' : row.selection_type === 'away' ? 'away' : null;
      if (!side) continue;
    }
    // Need a line to pair safely. Rows without line would conflate
    // multiple alt lines into one pair (the original bug).
    if (row.line == null) continue;
    const key = `${row.sportsbook}|${side}|${row.line}`;
    if (!byKey[key]) {
      byKey[key] = { book: row.sportsbook, teamSide: side, line: row.line };
    }
    byKey[key][dir] = row;
  }
  // Keep only entries where BOTH sides of the same line exist. Books
  // that post only one side at a line (rare, but possible mid-update)
  // are silently dropped rather than mispaired.
  return Object.values(byKey).filter(p => p.over && p.under);
}

/**
 * Build consensus for team totals — one over/under pair per team side.
 */
function buildConsensusTeamTotals(bookPairs) {
  const result = {};
  for (const side of ['home', 'away']) {
    const sidePairs = bookPairs.filter(bp => bp.teamSide === side);
    if (sidePairs.length === 0) continue;

    // Find primary line
    const lineCounts = {};
    for (const bp of sidePairs) {
      const line = bp.over.line;
      if (line != null) lineCounts[line] = (lineCounts[line] || 0) + 1;
    }
    const primaryLine = parseFloat(Object.entries(lineCounts).sort((a, b) => b[1] - a[1])[0]?.[0]);
    if (isNaN(primaryLine)) continue;
    const matching = sidePairs.filter(bp => bp.over.line === primaryLine);
    if (matching.length === 0) continue;

    // Exclude Kalshi from team-total averaging for the same reason as
    // moneyline/spread/total consensus (prediction-market thinness).
    const avgSet = excludeKalshiFromConsensus(matching);
    const devigged = { over: [], under: [] };
    for (const { over, under } of avgSet) {
      const [fo, fu] = deVig2Way(over.odds_probability, under.odds_probability);
      devigged.over.push(fo);
      devigged.under.push(fu);
    }
    const dvOver = avg(devigged.over);
    const dvUnder = avg(devigged.under);
    const pinBook = matching.find(bp => bp.book === 'pinnacle');
    // Floor at Pinnacle's DE-VIGGED fair prob (not raw) to avoid double-vig.
    const pinFairO = pinBook ? deVig2Way(pinBook.over.odds_probability, pinBook.under.odds_probability)[0] : 0;
    const pinFairU = pinBook ? deVig2Way(pinBook.over.odds_probability, pinBook.under.odds_probability)[1] : 0;

    // Build per-line consensus alongside the primary so getFairProb can
    // resolve alt team_total RFQs (PX often asks for ±1 line off primary
    // — e.g. cached primary 4.5, PX wants 5.5). Without byLine, those
    // RFQs return null fair-prob even when DK/FD posted the alt.
    const byLineEntries = {};
    const linesPresent = [...new Set(sidePairs.map(bp => bp.over.line).filter(l => l != null))];
    for (const altLine of linesPresent) {
      const altMatching = sidePairs.filter(bp => bp.over.line === altLine);
      if (altMatching.length === 0) continue;
      const altAvgSet = excludeKalshiFromConsensus(altMatching);
      const altDevigged = { over: [], under: [] };
      for (const { over, under } of altAvgSet) {
        const [fo, fu] = deVig2Way(over.odds_probability, under.odds_probability);
        altDevigged.over.push(fo);
        altDevigged.under.push(fu);
      }
      const altDvOver = avg(altDevigged.over);
      const altDvUnder = avg(altDevigged.under);
      const altPinBook = altMatching.find(bp => bp.book === 'pinnacle');
      const altPinFairO = altPinBook ? deVig2Way(altPinBook.over.odds_probability, altPinBook.under.odds_probability)[0] : 0;
      const altPinFairU = altPinBook ? deVig2Way(altPinBook.over.odds_probability, altPinBook.under.odds_probability)[1] : 0;
      byLineEntries[String(altLine)] = {
        line: altLine,
        over: {
          rawOdds: altMatching[0].over.odds_american,
          impliedProb: altMatching[0].over.odds_probability,
          fairProb: altDvOver >= 0.65 ? Math.max(altDvOver, altPinFairO) : altDvOver,
          displayFairProb: altDvOver,
        },
        under: {
          rawOdds: altMatching[0].under.odds_american,
          impliedProb: altMatching[0].under.odds_probability,
          fairProb: altDvUnder >= 0.65 ? Math.max(altDvUnder, altPinFairU) : altDvUnder,
          displayFairProb: altDvUnder,
        },
        books: altMatching.length,
      };
    }

    result[side] = {
      over: {
        rawOdds: matching[0].over.odds_american,
        impliedProb: matching[0].over.odds_probability,
        fairProb: dvOver >= 0.65 ? Math.max(dvOver, pinFairO) : dvOver,
        displayFairProb: dvOver,
      },
      under: {
        rawOdds: matching[0].under.odds_american,
        impliedProb: matching[0].under.odds_probability,
        fairProb: dvUnder >= 0.65 ? Math.max(dvUnder, pinFairU) : dvUnder,
        displayFairProb: dvUnder,
      },
      line: primaryLine,
      books: matching.length,
      pinnacle: pinBook ? { over: pinBook.over.odds_american, under: pinBook.under.odds_american } : null,
      // byLine is keyed by stringified line value so getFairProb's lookup
      // (`market[side].byLine[String(absLine)]`) finds it without a Number
      // round-trip. Populated only when the supplement saw multiple lines
      // for this team (alt market_keys returned data) — empty for single-
      // line consensus, which is the common case.
      byLine: byLineEntries,
    };
  }
  return Object.keys(result).length > 0 ? result : null;
}

/**
 * Drop Kalshi from consensus-averaging input. Kalshi is a prediction
 * market with thin volume on sports and often leaves prices orphaned
 * on the wrong side for hours — leading to the Guardians −142 market
 * vs Kalshi +130 disagreement (15pp) that pulled our fair to ~50/50
 * when real market was ~59/41. Operator intent: Kalshi is reference
 * only. Preserved in allBooks so the dashboard's Kalshi column still
 * populates as an eyeball-comparison reference, but NOT used for any
 * pricing input (averaging or fallback floor). Defensive fallback:
 * if Kalshi is the ONLY book in bookPairs, keep it rather than
 * averaging an empty set.
 */
function excludeKalshiFromConsensus(bookPairs) {
  const filtered = bookPairs.filter(bp => bp.book !== 'kalshi');
  return filtered.length > 0 ? filtered : bookPairs;
}

function buildConsensusMoneyline(bookPairs) {
  // Preserve ALL books for display attribution (pinnacle/fd/dk/kalshi fields
  // are always populated from the full list so the dashboard can show every
  // book's raw odds regardless of its vig).
  const allBooks = bookPairs;

  // Filter high-vig books out of the averaging input. Prevents Saba-class
  // Asian bookmaking feeds from corrupting the de-vigged consensus mean
  // via unweighted averaging. See filterSharpBooks for rationale.
  // Also exclude Kalshi — prediction-market thinness, see helper above.
  const sharpBooks = filterSharpBooks(
    excludeKalshiFromConsensus(bookPairs),
    bp => [bp.home.odds_probability, bp.away.odds_probability],
    'moneyline'
  );

  // Compute de-vigged consensus across FILTERED books (for display as "Fair")
  const devigged = { home: [], away: [] };
  for (const { home, away } of sharpBooks) {
    const [fh, fa] = deVig2Way(home.odds_probability, away.odds_probability);
    devigged.home.push(fh);
    devigged.away.push(fa);
  }
  const dvHome = avg(devigged.home);
  const dvAway = avg(devigged.away);

  // For PRICING: use de-vigged consensus as fair value for normal legs.
  // On ANY favorite side (fairProb >= 0.50), floor at Pinnacle's DE-VIGGED
  // fair prob (not raw implied — raw contains Pin's vig, which would double-
  // vig when we apply ours on top). Threshold lowered from 0.65 to 0.50
  // after observing Padres (~60% fair) get dragged to 54% by Saba pollution
  // — the old threshold meant any favorite between 50-65% had no floor
  // protection at all. Extending to all favorites makes Pin an effective
  // lower bound whenever Pin is present in the event.
  const pinBook = allBooks.find(bp => bp.book === 'pinnacle');
  const fdBook = allBooks.find(bp => bp.book === 'fanduel');
  const klBook = allBooks.find(bp => bp.book === 'kalshi');
  const pinFairHome = pinBook ? deVig2Way(pinBook.home.odds_probability, pinBook.away.odds_probability)[0] : 0;
  const pinFairAway = pinBook ? deVig2Way(pinBook.home.odds_probability, pinBook.away.odds_probability)[1] : 0;
  // Pinnacle floor only. Kalshi no longer used as fallback floor —
  // operator intent: reference/display only, not a pricing input.
  const floorHome = pinBook ? pinFairHome : 0;
  const floorAway = pinBook ? pinFairAway : 0;
  const pricingHome = dvHome >= 0.50 ? Math.max(dvHome, floorHome) : dvHome;
  const pricingAway = dvAway >= 0.50 ? Math.max(dvAway, floorAway) : dvAway;

  const pinnacle = pinBook ? {
    home: pinBook.home.odds_american,
    away: pinBook.away.odds_american,
  } : null;
  const fanduel = fdBook ? {
    home: fdBook.home.odds_american,
    away: fdBook.away.odds_american,
  } : null;
  const kalshi = klBook ? {
    home: klBook.home.odds_american,
    away: klBook.away.odds_american,
  } : null;
  return {
    home: {
      rawOdds: bookPairs[0].home.odds_american,
      impliedProb: bookPairs[0].home.odds_probability,
      fairProb: pricingHome,
      displayFairProb: dvHome,    // de-vigged consensus — used for FAIR column
    },
    away: {
      rawOdds: bookPairs[0].away.odds_american,
      impliedProb: bookPairs[0].away.odds_probability,
      fairProb: pricingAway,
      displayFairProb: dvAway,
    },
    books: bookPairs.length,
    pinnacle,
    fanduel,
    kalshi,
    draftkings: (() => {
      const dkBook = bookPairs.find(bp => bp.book === 'draftkings');
      return dkBook ? { home: dkBook.home.odds_american, away: dkBook.away.odds_american } : null;
    })(),
  };
}

function buildConsensusSpread(bookPairs) {
  // Use the most common line across books
  const lineCounts = {};
  for (const { home } of bookPairs) {
    const line = home.line;
    if (line != null) lineCounts[line] = (lineCounts[line] || 0) + 1;
  }
  const primaryLine = Object.entries(lineCounts).sort((a, b) => b[1] - a[1])[0]?.[0];
  const pLine = parseFloat(primaryLine);

  // Filter to books with this line
  const matching = bookPairs.filter(bp => bp.home.line === pLine);
  if (matching.length === 0) return null;

  // Filter high-vig books out of the averaging input (see
  // filterSharpBooks rationale in buildConsensusMoneyline). Kalshi
  // also excluded — see excludeKalshiFromConsensus doc.
  const sharpMatching = filterSharpBooks(
    excludeKalshiFromConsensus(matching),
    bp => [bp.home.odds_probability, bp.away.odds_probability],
    'spread'
  );

  // De-vigged consensus for display
  const devigged = { home: [], away: [] };
  for (const { home, away } of sharpMatching) {
    const [fh, fa] = deVig2Way(home.odds_probability, away.odds_probability);
    devigged.home.push(fh);
    devigged.away.push(fa);
  }
  const dvHome = avg(devigged.home);
  const dvAway = avg(devigged.away);

  const pinBook = matching.find(bp => bp.book === 'pinnacle');
  const fdBook = matching.find(bp => bp.book === 'fanduel');
  // Floor at Pinnacle's DE-VIGGED fair prob (not raw implied) — raw would
  // include Pinnacle's vig and cause double-vig when we apply ours on top.
  // Threshold lowered from 0.65 to 0.50 so any favorite side gets floored
  // against Pin's de-vigged prob whenever Pin is present.
  // Kalshi no longer used as fallback floor — reference only per operator intent.
  const pinFairHomeS = pinBook ? deVig2Way(pinBook.home.odds_probability, pinBook.away.odds_probability)[0] : 0;
  const pinFairAwayS = pinBook ? deVig2Way(pinBook.home.odds_probability, pinBook.away.odds_probability)[1] : 0;
  const floorHomeS = pinBook ? pinFairHomeS : 0;
  const floorAwayS = pinBook ? pinFairAwayS : 0;
  const pricingHome = dvHome >= 0.50 ? Math.max(dvHome, floorHomeS) : dvHome;
  const pricingAway = dvAway >= 0.50 ? Math.max(dvAway, floorAwayS) : dvAway;

  const pinnacle = pinBook ? {
    home: pinBook.home.odds_american,
    away: pinBook.away.odds_american,
  } : null;
  const fanduel = fdBook ? {
    home: fdBook.home.odds_american,
    away: fdBook.away.odds_american,
  } : null;
  const klBook = matching.find(bp => bp.book === 'kalshi');
  const kalshi = klBook ? {
    home: klBook.home.odds_american,
    away: klBook.away.odds_american,
  } : null;
  const dkBookS = matching.find(bp => bp.book === 'draftkings');
  const draftkings = dkBookS ? {
    home: dkBookS.home.odds_american,
    away: dkBookS.away.odds_american,
  } : null;
  // Build per-line consensus alongside the primary so getFairProb can
  // resolve alt spread RFQs (PX often asks for ±0.5 F5 RL when the
  // cached primary is 0 from Pinnacle's pick-em — DK/FD post ±0.5 as
  // their primary). Without byLine, those RFQs fall through to
  // altLinesCache which doesn't include line=0.5 because TOA's
  // alternate_spreads market only posts ±1, ±2, etc. (not ±0.5).
  //
  // Key shape: "home|<signed_line>" / "away|<signed_line>" — matches
  // the lookup convention in getFairProb's spreads byLine fast-path.
  const byLineEntries = {};
  const linesPresent = [...new Set(bookPairs.map(bp => bp.home.line).filter(l => l != null))];
  for (const altLine of linesPresent) {
    const altMatching = bookPairs.filter(bp => bp.home.line === altLine);
    if (altMatching.length === 0) continue;
    const altSharp = filterSharpBooks(
      excludeKalshiFromConsensus(altMatching),
      bp => [bp.home.odds_probability, bp.away.odds_probability],
      'spread'
    );
    if (altSharp.length === 0) continue;
    const altDevigged = { home: [], away: [] };
    for (const { home, away } of altSharp) {
      const [fh, fa] = deVig2Way(home.odds_probability, away.odds_probability);
      altDevigged.home.push(fh);
      altDevigged.away.push(fa);
    }
    const altDvHome = avg(altDevigged.home);
    const altDvAway = avg(altDevigged.away);
    const altPinBook = altMatching.find(bp => bp.book === 'pinnacle');
    const altPinHome = altPinBook ? deVig2Way(altPinBook.home.odds_probability, altPinBook.away.odds_probability)[0] : 0;
    const altPinAway = altPinBook ? deVig2Way(altPinBook.home.odds_probability, altPinBook.away.odds_probability)[1] : 0;
    const altPricingHome = altDvHome >= 0.50 ? Math.max(altDvHome, altPinHome) : altDvHome;
    const altPricingAway = altDvAway >= 0.50 ? Math.max(altDvAway, altPinAway) : altDvAway;
    // Home gets the line as posted; away gets the negated line.
    byLineEntries['home|' + altLine] = {
      line: altLine,
      fairProb: altPricingHome,
      displayFairProb: altDvHome,
      books: altMatching.length,
    };
    byLineEntries['away|' + (-altLine)] = {
      line: -altLine,
      fairProb: altPricingAway,
      displayFairProb: altDvAway,
      books: altMatching.length,
    };
  }

  return {
    home: {
      rawOdds: matching[0].home.odds_american,
      point: pLine,
      impliedProb: matching[0].home.odds_probability,
      fairProb: pricingHome,
      displayFairProb: dvHome,
    },
    away: {
      rawOdds: matching[0].away.odds_american,
      point: -pLine,
      impliedProb: matching[0].away.odds_probability,
      fairProb: pricingAway,
      displayFairProb: dvAway,
    },
    line: pLine,
    books: matching.length,
    pinnacle,
    fanduel,
    kalshi,
    draftkings,
    byLine: byLineEntries,
  };
}

// Compute the de-vigged consensus + book-level details for ONE specific
// totals line. Shared between primary and byLine computation. Returns null
// if no books posted this line.
function buildTotalsForLine(bookPairs, pLine) {
  const matching = bookPairs.filter(bp => bp.over.line === pLine);
  if (matching.length === 0) return null;

  // Exclude Kalshi from averaging (see excludeKalshiFromConsensus doc).
  const sharpMatching = filterSharpBooks(
    excludeKalshiFromConsensus(matching),
    bp => [bp.over.odds_probability, bp.under.odds_probability],
    'total'
  );

  const devigged = { over: [], under: [] };
  for (const { over, under } of sharpMatching) {
    const [fo, fu] = deVig2Way(over.odds_probability, under.odds_probability);
    devigged.over.push(fo);
    devigged.under.push(fu);
  }
  const dvOver = avg(devigged.over);
  const dvUnder = avg(devigged.under);

  const pinBook = matching.find(bp => bp.book === 'pinnacle');
  const klBookT = matching.find(bp => bp.book === 'kalshi');
  const pinFairOver = pinBook ? deVig2Way(pinBook.over.odds_probability, pinBook.under.odds_probability)[0] : 0;
  const pinFairUnder = pinBook ? deVig2Way(pinBook.over.odds_probability, pinBook.under.odds_probability)[1] : 0;
  // Pinnacle floor only. Kalshi no longer used as fallback floor.
  const floorOver = pinBook ? pinFairOver : 0;
  const floorUnder = pinBook ? pinFairUnder : 0;
  const pricingOver = dvOver >= 0.50 ? Math.max(dvOver, floorOver) : dvOver;
  const pricingUnder = dvUnder >= 0.50 ? Math.max(dvUnder, floorUnder) : dvUnder;

  const fdBook = matching.find(bp => bp.book === 'fanduel');
  const dkBook = matching.find(bp => bp.book === 'draftkings');

  return {
    over: {
      rawOdds: matching[0].over.odds_american,
      point: pLine,
      impliedProb: matching[0].over.odds_probability,
      fairProb: pricingOver,
      displayFairProb: dvOver,
    },
    under: {
      rawOdds: matching[0].under.odds_american,
      point: pLine,
      impliedProb: matching[0].under.odds_probability,
      fairProb: pricingUnder,
      displayFairProb: dvUnder,
    },
    line: pLine,
    books: matching.length,
    pinnacle: pinBook ? { over: pinBook.over.odds_american, under: pinBook.under.odds_american } : null,
    fanduel: fdBook ? { over: fdBook.over.odds_american, under: fdBook.under.odds_american } : null,
    kalshi: klBookT ? { over: klBookT.over.odds_american, under: klBookT.under.odds_american } : null,
    draftkings: dkBook ? { over: dkBook.over.odds_american, under: dkBook.under.odds_american } : null,
  };
}

function buildConsensusTotals(bookPairs) {
  // Tally distinct lines. The "primary" is the most-common line across
  // books; but we also preserve consensus for every OTHER line in `byLine`
  // so RFQs that reference a minority line (e.g., Pinnacle's integer 8
  // when the majority is 8.5) can be priced without a network fetch.
  const lineCounts = {};
  for (const { over } of bookPairs) {
    const line = over.line;
    if (line != null) lineCounts[line] = (lineCounts[line] || 0) + 1;
  }
  const entries = Object.entries(lineCounts).sort((a, b) => b[1] - a[1]);
  if (entries.length === 0) return null;

  const primaryLine = parseFloat(entries[0][0]);
  const primary = buildTotalsForLine(bookPairs, primaryLine);
  if (!primary) return null;

  const byLine = {};
  for (const [lineStr] of entries) {
    const ln = parseFloat(lineStr);
    const entry = buildTotalsForLine(bookPairs, ln);
    if (entry) byLine[String(ln)] = entry;
  }

  return { ...primary, byLine };
}

// ---------------------------------------------------------------------------
// DE-VIG
// ---------------------------------------------------------------------------

function deVig2Way(prob1, prob2) {
  const total = prob1 + prob2;
  if (total === 0) return [0.5, 0.5];
  return [prob1 / total, prob2 / total];
}

// Max per-book 2-way vig tolerated in the consensus average. Books above
// this threshold (Asian square-bookmaking feeds like Saba, some retail
// outliers) systematically drag averaged fair probs away from sharp values.
// Pinnacle runs ~2%, FD/DK run 4-5%, anything > 6% is in a different class
// of bookmaking and shouldn't be weighted equally with majors.
const MAX_BOOK_VIG = 0.06;

/**
 * Filter bookPairs to keep only books whose 2-way implied prob sum is
 * within the MAX_BOOK_VIG threshold. The shape of each entry depends on
 * the caller: `getFields` returns `[sideA, sideB]` probability values.
 *
 * If filtering would leave zero books, falls back to the original list
 * (never return an empty set — better to have a noisy fair than no fair
 * at all). Logs dropped books for auditability.
 */
function filterSharpBooks(bookPairs, getFields, label) {
  if (!bookPairs || bookPairs.length === 0) return bookPairs;
  const kept = [];
  const dropped = [];
  for (const bp of bookPairs) {
    const [a, b] = getFields(bp);
    if (a == null || b == null) { kept.push(bp); continue; }
    const vig = (a + b) - 1;
    if (vig > MAX_BOOK_VIG) {
      dropped.push({ book: bp.book, vig: +(vig * 100).toFixed(1) });
    } else {
      kept.push(bp);
    }
  }
  if (kept.length === 0) {
    // All books failed the vig cap — last resort, keep everything rather
    // than fabricate a null. The Pin-floor below will still protect us
    // if Pinnacle is present with any vig level.
    if (dropped.length > 0) {
      log.debug('OddsFeed', `filterSharpBooks(${label}): all ${dropped.length} books exceeded ${(MAX_BOOK_VIG*100).toFixed(0)}% vig — keeping all as fallback`);
    }
    return bookPairs;
  }
  if (dropped.length > 0) {
    log.debug('OddsFeed', `filterSharpBooks(${label}): dropped ${dropped.length} high-vig books: ${dropped.map(d => d.book + '(' + d.vig + '%)').join(', ')}`);
  }
  return kept;
}

/**
 * De-vig a Double Chance 3-way market.
 * Double Chance outcomes are 1X (home or draw), X2 (draw or away), 12 (home or away).
 * Each outcome covers 2 of the 3 possible results, so fair probabilities sum to 2.0.
 * Vig-adjusted: divide each by (sum / 2).
 */
function deVigDoubleChance(p1X, pX2, p12) {
  const total = p1X + pX2 + p12;
  if (total === 0) return [0.5, 0.5, 0.5];
  const scale = total / 2;
  return [p1X / scale, pX2 / scale, p12 / scale];
}

function americanToImpliedProb(odds) {
  if (odds >= 100) return 100 / (odds + 100);
  if (odds <= -100) return Math.abs(odds) / (Math.abs(odds) + 100);
  return 0.5;
}

function avg(arr) {
  if (!arr.length) return 0;
  return arr.reduce((s, v) => s + v, 0) / arr.length;
}

// ---------------------------------------------------------------------------
// ALT LINES CACHE — on-demand fetched from The Odds API event endpoint
// ---------------------------------------------------------------------------
// { 'eventKey': { fetchedAt, altSpreads: { [line]: { home, away } }, altTotals: { [line]: { over, under } } } }
const altLinesCache = {};
// 10 min TTL. Earlier today this was bumped to 30 min as a latency optimization
// (fewer cache misses → fewer on-demand Odds API fetches). Real-world observation
// showed this correlated with a confirmation drought: 0 successful matches in 2
// hours after the bump, vs normal ~4% confirm rate. Stale alt lines beyond 10
// minutes apparently make our offered prices uncompetitive enough that bettors
// consistently pick other SPs. Reverted to 10 min — accepts a latency cost
// (more cache misses) in exchange for fresher prices that stay in the running.
//
// The 60s warm loop and boot pre-warm (both added in 0caa4d3) remain — they
// fetch MORE often, not less. No fill-rate risk from those.
const ALT_LINES_TTL_MS = 10 * 60 * 1000;
// Refresh-ahead window for alt-lines cache. When a cached entry is
// older than this but younger than ALT_LINES_TTL_MS, return the cached
// value AND fire a background refresh (fire-and-forget). Eliminates
// the synchronous block-on-fetch latency tail that was driving the
// pricer's price_phase2 P95 to ~40ms after Phase-C launch.
const ALT_LINES_REFRESH_AHEAD_MS = 7 * 60 * 1000;

/**
 * Fetch alternate spreads and totals for a specific event from The Odds API.
 * Uses the event-specific endpoint which supports alt markets from Pinnacle.
 */
// Cache for The Odds API event ID lookups (sport → { fetchedAt, events: [{id, home, away}] })
const oddsApiEventIdCache = {};
const ODDS_API_EVENT_ID_TTL_MS = 30 * 60 * 1000; // 30 min

// Team-name aliases for SharpAPI's Kalshi-style abbreviations. Keys are
// the NORMALIZED form of the SharpAPI name (lowercased, accent-stripped,
// punctuation removed by normalizeTeamName). Values are the canonical
// full names as they appear in The Odds API events feed (also normalized).
//
// Only add entries here for abbreviations that our generic matcher
// (exact / substring / last-N-words) can't resolve. Observed real-world
// failures from /alt-lines-stats unmatchedSamples.
const ODDS_API_TEAM_ALIASES = {
  // MLB — SharpAPI uses compressed forms when a feed falls back to Kalshi
  'chicago ws': 'chicago white sox',
  'as': 'oakland athletics',         // "A's" → normalized "as" → needs mapping
  'oakland as': 'oakland athletics', // belt-and-suspenders
  'bos red sox': 'boston red sox',   // SharpAPI occasionally uses city-abbreviation form
  // NHL city-abbreviation overrides — mirrors TEAM_NAME_OVERRIDES in
  // line-manager.js for the reverse direction. Kept in sync so warming
  // succeeds even when SharpAPI uses the abbreviation form.
  'was capitals': 'washington capitals',
  'cbj blue jackets': 'columbus blue jackets',
  'mtl canadiens': 'montreal canadiens',
  'nj devils': 'new jersey devils',
  'sj sharks': 'san jose sharks',
  'la kings': 'los angeles kings',
};

function applyTeamAlias(normalizedName) {
  return ODDS_API_TEAM_ALIASES[normalizedName] || normalizedName;
}

/**
 * Resolve The Odds API event ID for a given home/away pair.
 * SharpAPI event IDs are NOT The Odds API event IDs, so we must look up
 * the event list from The Odds API and match by team name.
 *
 * Previously used naive toLowerCase().trim() for matching, which silently
 * failed on accented names ("Montréal Canadiens" vs "Montreal Canadiens"),
 * abbreviations ("LA Clippers" vs "Los Angeles Clippers"), and compressed
 * forms ("NY Yankees" vs "New York Yankees"). Evidence: /alt-lines-stats
 * showed 0-of-10 warm fetches succeeding for NHL/MLB candidates.
 *
 * @param {string} targetTime optional ISO — disambiguates doubleheaders.
 */
async function resolveOddsApiEventId(sport, homeTeam, awayTeam, targetTime) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return null;

  const oddsApiSportMap = {
    'basketball_nba': 'basketball_nba',
    'basketball_ncaab': 'basketball_ncaab',
    'basketball_wnba': 'basketball_wnba',
    'baseball_mlb': 'baseball_mlb',
    'icehockey_nhl': 'icehockey_nhl',
    'soccer_usa_mls': 'soccer_usa_mls',
    'soccer_epl': 'soccer_epl',
    'soccer_spain_la_liga': 'soccer_spain_la_liga',
    'soccer_germany_bundesliga': 'soccer_germany_bundesliga',
    'soccer_italy_serie_a': 'soccer_italy_serie_a',
    'soccer_france_ligue_one': 'soccer_france_ligue_one',
    'soccer_uefa_champs_league': 'soccer_uefa_champions',
    'soccer_uefa_europa_league': 'soccer_uefa_europa_league',
  };
  // Dynamic sports (e.g. tennis) use tournament-specific Odds API keys that
  // rotate over time. For these, we discover active tournaments and fetch
  // events per tournament, tagging each event with its own sport key. Static
  // sports use the map above directly.
  const fallback = ODDS_API_FALLBACK[sport];
  const isDynamic = !!(fallback && fallback.dynamic && fallback.sportPrefix);
  const oddsApiSport = oddsApiSportMap[sport];
  if (!oddsApiSport && !isDynamic) return null;

  // Check cache
  const cached = oddsApiEventIdCache[sport];
  if (!cached || (Date.now() - cached.fetchedAt) > ODDS_API_EVENT_ID_TTL_MS) {
    try {
      let events = [];
      if (isDynamic) {
        // Discover active tournaments, then fetch events for each. Tag each
        // event with its tournament sport key so fetchAltLines can build the
        // correct /v4/sports/{key}/events/{id}/odds URL.
        const sportsResp = await abortableFetch(
          `https://api.the-odds-api.com/v4/sports/?apiKey=${theOddsApiKey}`
        );
        if (!sportsResp.ok) {
          log.warn('OddsFeed', `Odds API sports list failed (${sportsResp.status}) for ${sport}`);
          return null;
        }
        const allSports = await sportsResp.json();
        const active = allSports.filter(s => s.key.startsWith(fallback.sportPrefix) && s.active);
        for (const t of active) {
          try {
            const r = await abortableFetch(
              `https://api.the-odds-api.com/v4/sports/${t.key}/events?apiKey=${theOddsApiKey}`
            );
            if (!r.ok) continue;
            const data = await r.json();
            for (const e of (data || [])) {
              events.push({
                id: e.id,
                home: e.home_team,
                away: e.away_team,
                commence: e.commence_time,
                oddsApiSport: t.key,
              });
            }
          } catch (err) {
            log.warn('OddsFeed', `Odds API events fetch failed for ${t.key}: ${err.message}`);
          }
        }
        log.debug('OddsFeed', `Cached ${events.length} Odds API event IDs across ${active.length} ${sport} tournaments`);
      } else {
        // Static path — single sport key
        const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/events?apiKey=${theOddsApiKey}`;
        const resp = await abortableFetch(url);
        if (!resp.ok) {
          log.warn('OddsFeed', `Odds API events list failed (${resp.status}) for ${sport}`);
          return null;
        }
        const data = await resp.json();
        events = (data || []).map(e => ({
          id: e.id,
          home: e.home_team,
          away: e.away_team,
          commence: e.commence_time,
          oddsApiSport,
        }));
        log.debug('OddsFeed', `Cached ${events.length} Odds API event IDs for ${sport}`);
      }
      oddsApiEventIdCache[sport] = { fetchedAt: Date.now(), events };
    } catch (err) {
      log.warn('OddsFeed', `Odds API events list error for ${sport}: ${err.message}`);
      return null;
    }
  }

  // Robust team-name matcher — handles accents, abbreviations, word-tail
  // equality. Matches the strategy used elsewhere in the codebase
  // (line-manager.js matchTeamName) rather than the naive substring we had.
  function teamsMatch(a, b) {
    if (!a || !b) return false;
    if (a === b) return true;
    // Substring (handles "Rays" vs "Tampa Bay Rays")
    if (a.includes(b) || b.includes(a)) return true;
    const aWords = a.split(/\s+/);
    const bWords = b.split(/\s+/);
    // Last-2-words equality (handles "Red Sox" vs "Boston Red Sox",
    // "White Sox" vs "Chicago White Sox" — disambiguates two Sox teams)
    if (aWords.length >= 2 && bWords.length >= 2) {
      const aT = aWords.slice(-2).join(' ');
      const bT = bWords.slice(-2).join(' ');
      if (aT === bT && aT.length >= 5) return true;
    }
    // Last-word equality (handles "LA Clippers" vs "Los Angeles Clippers",
    // "NY Yankees" vs "New York Yankees"). Require ≥4 chars to avoid "fc"
    // / "sc" / "utd" false positives on soccer clubs.
    const aLast = aWords[aWords.length - 1];
    const bLast = bWords[bWords.length - 1];
    if (aLast && bLast && aLast === bLast && aLast.length >= 4) return true;
    return false;
  }

  // Normalize then apply alias map. Alias maps known SharpAPI abbreviations
  // (e.g. "chicago ws") to canonical Odds API names (e.g. "chicago white sox")
  // so the downstream exact/substring/word-tail matcher can resolve them.
  // Applied to BOTH sides — if either side happens to use the abbreviation,
  // the match still succeeds.
  const normHome = applyTeamAlias(normalizeTeamName(homeTeam));
  const normAway = applyTeamAlias(normalizeTeamName(awayTeam));
  const events = oddsApiEventIdCache[sport]?.events || [];
  const matches = [];
  for (const e of events) {
    const eHome = applyTeamAlias(normalizeTeamName(e.home));
    const eAway = applyTeamAlias(normalizeTeamName(e.away));
    // SharpAPI and The Odds API occasionally disagree on which team is home
    // (observed: Chicago White Sox @ Athletics, Blue Jays @ Diamondbacks —
    // the two feeds flip the designation). Same physical game either way, so
    // match in either orientation. Orientation doesn't affect alt-line data
    // because the alt-line cache is keyed by the event ID we return.
    const straight = teamsMatch(eHome, normHome) && teamsMatch(eAway, normAway);
    const flipped  = teamsMatch(eHome, normAway) && teamsMatch(eAway, normHome);
    if (straight || flipped) {
      matches.push(e);
    }
  }

  if (matches.length === 0) {
    log.debug('OddsFeed', `No Odds API event match for ${homeTeam} vs ${awayTeam} in ${sport} (${events.length} candidates)`);
    return null;
  }

  // Disambiguate by commence time when multiple candidates (doubleheaders
  // or back-to-back same-matchup events). Pick the one closest to the
  // target time; if no target, default to first match.
  let chosen = matches[0];
  if (targetTime && matches.length > 1) {
    const targetMs = new Date(targetTime).getTime();
    if (!isNaN(targetMs)) {
      chosen = matches.reduce((best, e) => {
        const bMs = new Date(best.commence).getTime();
        const eMs = new Date(e.commence).getTime();
        if (isNaN(eMs)) return best;
        if (isNaN(bMs)) return e;
        return Math.abs(eMs - targetMs) < Math.abs(bMs - targetMs) ? e : best;
      }, matches[0]);
    }
  }

  // For dynamic sports, each cached event carries its own tournament sport
  // key. Fall back to the static map value for non-dynamic sports.
  return { eventId: chosen.id, oddsApiSport: chosen.oddsApiSport || oddsApiSport };
}

async function fetchAltLines(sport, homeTeam, awayTeam, targetTime) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return null;

  const key = normalizeEventKey(homeTeam, awayTeam);

  // Refresh-ahead check on cache.
  //   age < REFRESH_AHEAD     → return cached, no refresh
  //   REFRESH_AHEAD ≤ age < TTL → return cached AND fire background refresh
  //   age ≥ TTL                → cache miss; do the fetch synchronously
  // The 'refreshing' bool gates concurrent refreshes for the same key
  // so 5 simultaneous RFQs hitting a stale entry only spawn ONE refresh.
  const cached = altLinesCache[key];
  if (cached) {
    const age = Date.now() - cached.fetchedAt;
    if (age < ALT_LINES_REFRESH_AHEAD_MS) return cached;
    if (age < ALT_LINES_TTL_MS) {
      if (!cached.refreshing) {
        cached.refreshing = true;
        // Fire-and-forget background refresh that bypasses the cache
        // gate via the internal _doAltLinesFetch helper. Result writes
        // back into altLinesCache directly. Refreshing flag clears in
        // finally{} so even on failure we don't deadlock.
        Promise.resolve().then(() =>
          _doAltLinesFetch(sport, homeTeam, awayTeam, targetTime, key)
            .catch(err => log.warn('OddsFeed', `Alt-lines bg refresh failed for ${key}: ${err.message}`))
            .finally(() => {
              const c = altLinesCache[key];
              if (c) c.refreshing = false;
            })
        );
      }
      return cached;
    }
  }
  return _doAltLinesFetch(sport, homeTeam, awayTeam, targetTime, key);
}

// Internal: actual TOA alt-lines fetch + cache write. Called by both
// the synchronous block-on-miss path and the background refresh-ahead
// path inside fetchAltLines. Bypasses any cache gate — caller is
// responsible for deciding when to invoke.
async function _doAltLinesFetch(sport, homeTeam, awayTeam, targetTime, key) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return null;

  // Resolve The Odds API event ID (SharpAPI IDs are different). targetTime
  // disambiguates same-matchup doubleheaders / back-to-backs.
  const resolved = await resolveOddsApiEventId(sport, homeTeam, awayTeam, targetTime);
  if (!resolved) {
    log.debug('OddsFeed', `Cannot fetch alt lines for ${homeTeam} vs ${awayTeam}: no Odds API event ID`);
    return null;
  }
  const { eventId, oddsApiSport } = resolved;

  // MLB events get F5 alt markets appended. PX registers F5 alt total
  // lines (3, 3.5, 4, 5, 5.5) that the bulk supplement doesn't cover
  // — without these, every off-primary F5 total RFQ declines with
  // "no totals_f5 quote".
  const mlbF5Markets = sport === 'baseball_mlb'
    ? ',alternate_spreads_1st_5_innings,alternate_totals_1st_5_innings'
    : '';
  // NBA H1 alt markets. PX RFQs occasionally include integer first-half
  // spreads (e.g. OKC -6 first_half_spread) that no book's PRIMARY h1
  // line carries — main books only quote half-points (-5.5, -6.5) on
  // h1 to avoid pushes. Pinnacle and Bovada DO carry integer h1 alts
  // for select lines via alternate_spreads_h1 / alternate_totals_h1.
  // H2 deliberately NOT included: H2 lines come back on the board at
  // halftime and move very fast live; cache TTL is too long to keep
  // up with halftime volatility, risking stale-quote losses.
  const nbaH1Markets = sport === 'basketball_nba'
    ? ',alternate_spreads_h1,alternate_totals_h1'
    : '';
  // Include the PRIMARY totals market alongside alternate_totals so lines
  // that are a book's primary (e.g. Pinnacle's integer MLB 8) — and thus
  // not listed in alternate_totals — still land in the altTotals cache.
  // Books that skip integer totals in their alt list won't cover Over 8
  // otherwise.
  const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/events/${eventId}/odds`
    + `?apiKey=${theOddsApiKey}`
    + `&regions=us,eu`
    + `&markets=totals,alternate_spreads,alternate_totals${mlbF5Markets}${nbaH1Markets}`
    + `&bookmakers=${ALT_LINES_BOOKMAKERS}`
    + `&oddsFormat=american`;

  log.info('OddsFeed', `Fetching alt lines for ${homeTeam} vs ${awayTeam}...`);

  try {
    const resp = await abortableFetch(url);
    if (!resp.ok) {
      const text = await resp.text();
      log.warn('OddsFeed', `Alt lines fetch failed (${resp.status}): ${text.substring(0, 100)}`);
      return null;
    }

    const data = await resp.json();
    // refreshing:false on every fresh write so the refresh-ahead gate
    // in fetchAltLines treats the new entry as "not currently being
    // refreshed" — without this flag the recursive bg refresh path
    // could see leftover refreshing=true and skip subsequent refreshes.
    const result = { fetchedAt: Date.now(), refreshing: false, altSpreads: {}, altTotals: {}, altSpreadsF5: {}, altTotalsF5: {}, altSpreadsH1: {}, altTotalsH1: {} };

    for (const book of (data.bookmakers || [])) {
      for (const market of (book.markets || [])) {
        if (market.key === 'alternate_spreads_1st_5_innings') {
          // F5 alt spreads. Same signed-home-point keying as full-game
          // alternate_spreads. Reuses the home/away team-name matching
          // block just below via a helper to avoid duplication.
          for (const o of (market.outcomes || [])) {
            const normOutcome = normalizeTeamName(o.name);
            const normHome = normalizeTeamName(homeTeam);
            const normAway = normalizeTeamName(awayTeam);
            const normDataHome = data.home_team ? normalizeTeamName(data.home_team) : '';
            const homeMatch = normOutcome === normHome || normOutcome === normDataHome
              || normHome.includes(normOutcome) || normOutcome.includes(normHome)
              || (normDataHome && (normDataHome.includes(normOutcome) || normOutcome.includes(normDataHome)));
            const awayMatch = normOutcome === normAway
              || normAway.includes(normOutcome) || normOutcome.includes(normAway);
            if (homeMatch === awayMatch) continue;
            const isHome = homeMatch;
            const homePoint = isHome ? o.point : -o.point;
            const lineKey = String(homePoint);
            if (!result.altSpreadsF5[lineKey]) {
              result.altSpreadsF5[lineKey] = { probs: [], books: new Set(), byBook: {}, homePoint };
            }
            const prob = americanToImpliedProb(o.price);
            result.altSpreadsF5[lineKey].probs.push({ isHome, prob, point: o.point });
            result.altSpreadsF5[lineKey].books.add(book.key);
            if (!result.altSpreadsF5[lineKey].byBook[book.key]) result.altSpreadsF5[lineKey].byBook[book.key] = {};
            result.altSpreadsF5[lineKey].byBook[book.key][isHome ? 'home' : 'away'] = o.price;
          }
          continue;
        }
        if (market.key === 'alternate_totals_1st_5_innings') {
          for (const o of (market.outcomes || [])) {
            const lineKey = o.point;
            if (!result.altTotalsF5[lineKey]) {
              result.altTotalsF5[lineKey] = { probs: [], books: new Set(), byBook: {} };
            }
            const isOver = o.name === 'Over';
            const prob = americanToImpliedProb(o.price);
            result.altTotalsF5[lineKey].probs.push({ isOver, prob });
            result.altTotalsF5[lineKey].books.add(book.key);
            if (!result.altTotalsF5[lineKey].byBook[book.key]) result.altTotalsF5[lineKey].byBook[book.key] = {};
            result.altTotalsF5[lineKey].byBook[book.key][isOver ? 'over' : 'under'] = o.price;
          }
          continue;
        }
        if (market.key === 'alternate_spreads_h1') {
          // NBA H1 alt spreads. Same signed-home-point keying as full-game
          // alternate_spreads. Routed to altSpreadsH1 so consumers querying
          // marketType='spreads_h1' don't collide with full-game alts.
          for (const o of (market.outcomes || [])) {
            const normOutcome = normalizeTeamName(o.name);
            const normHome = normalizeTeamName(homeTeam);
            const normAway = normalizeTeamName(awayTeam);
            const normDataHome = data.home_team ? normalizeTeamName(data.home_team) : '';
            const homeMatch = normOutcome === normHome || normOutcome === normDataHome
              || normHome.includes(normOutcome) || normOutcome.includes(normHome)
              || (normDataHome && (normDataHome.includes(normOutcome) || normOutcome.includes(normDataHome)));
            const awayMatch = normOutcome === normAway
              || normAway.includes(normOutcome) || normOutcome.includes(normAway);
            if (homeMatch === awayMatch) continue;
            const isHome = homeMatch;
            const homePoint = isHome ? o.point : -o.point;
            const lineKey = String(homePoint);
            if (!result.altSpreadsH1[lineKey]) {
              result.altSpreadsH1[lineKey] = { probs: [], books: new Set(), byBook: {}, homePoint };
            }
            const prob = americanToImpliedProb(o.price);
            result.altSpreadsH1[lineKey].probs.push({ isHome, prob, point: o.point });
            result.altSpreadsH1[lineKey].books.add(book.key);
            if (!result.altSpreadsH1[lineKey].byBook[book.key]) result.altSpreadsH1[lineKey].byBook[book.key] = {};
            result.altSpreadsH1[lineKey].byBook[book.key][isHome ? 'home' : 'away'] = o.price;
          }
          continue;
        }
        if (market.key === 'alternate_totals_h1') {
          for (const o of (market.outcomes || [])) {
            const lineKey = o.point;
            if (!result.altTotalsH1[lineKey]) {
              result.altTotalsH1[lineKey] = { probs: [], books: new Set(), byBook: {} };
            }
            const isOver = o.name === 'Over';
            const prob = americanToImpliedProb(o.price);
            result.altTotalsH1[lineKey].probs.push({ isOver, prob });
            result.altTotalsH1[lineKey].books.add(book.key);
            if (!result.altTotalsH1[lineKey].byBook[book.key]) result.altTotalsH1[lineKey].byBook[book.key] = {};
            result.altTotalsH1[lineKey].byBook[book.key][isOver ? 'over' : 'under'] = o.price;
          }
          continue;
        }
        if (market.key === 'alternate_spreads') {
          // CRITICAL: key by SIGNED home point, not abs. Otherwise both
          // "home -1.5 / away +1.5" and "home +1.5 / away -1.5" (two
          // distinct bets) collapse into the same bucket[1.5], letting
          // the last-written byBook price overwrite the other direction.
          //
          // The observed-in-production bug: FanDuel's "Bournemouth -1.5"
          // (heavy underdog winning by 2+, +1500) overwrote its
          // "Bournemouth +1.5" (underdog getting 1.5, should be ~-140)
          // in the abs-keyed bucket, dragging consensus fair ~15pp below
          // Pinnacle and producing a +461 quote where the true parlay
          // price was +287. Sign flips on spread alts are dangerous —
          // keep the two directions strictly separate.
          for (const o of (market.outcomes || [])) {
            // Use fuzzy matching for team names — exact match fails when
            // The Odds API returns a slightly different name (e.g.,
            // "LA Clippers" vs "Los Angeles Clippers"). A silent mismatch
            // would flip EVERY home_point sign in the cache, catastrophically
            // swapping home/away probs for all alt spreads in this game.
            const normOutcome = normalizeTeamName(o.name);
            const normHome = normalizeTeamName(homeTeam);
            const normAway = normalizeTeamName(awayTeam);
            const normDataHome = data.home_team ? normalizeTeamName(data.home_team) : '';
            const homeMatch = normOutcome === normHome || normOutcome === normDataHome
              || normHome.includes(normOutcome) || normOutcome.includes(normHome)
              || (normDataHome && (normDataHome.includes(normOutcome) || normOutcome.includes(normDataHome)));
            const awayMatch = normOutcome === normAway
              || normAway.includes(normOutcome) || normOutcome.includes(normAway);
            // If both or neither side matches, skip — ambiguous classification
            // would silently flip signs on every alt spread for this game.
            if (homeMatch === awayMatch) {
              if (homeMatch) log.warn('OddsFeed', `Alt spread ambiguous team: "${o.name}" matches both home "${homeTeam}" and away "${awayTeam}" — skipping outcome`);
              continue;
            }
            const isHome = homeMatch;
            // Compute home_point: the signed spread from the HOME team's
            // perspective. For a home outcome it's just o.point; for an
            // away outcome it's the negation (same bet, opposite side).
            const homePoint = isHome ? o.point : -o.point;
            const lineKey = String(homePoint); // signed string key: "-1.5", "1.5", "0", etc.
            if (!result.altSpreads[lineKey]) {
              result.altSpreads[lineKey] = { probs: [], books: new Set(), byBook: {}, homePoint };
            }
            const prob = americanToImpliedProb(o.price);
            result.altSpreads[lineKey].probs.push({ isHome, prob, point: o.point });
            result.altSpreads[lineKey].books.add(book.key);
            // Per-book raw odds so the dashboard can display actual
            // Pinnacle/DK/FD values for this specific alt line.
            if (!result.altSpreads[lineKey].byBook[book.key]) {
              result.altSpreads[lineKey].byBook[book.key] = {};
            }
            result.altSpreads[lineKey].byBook[book.key][isHome ? 'home' : 'away'] = o.price;
          }
        } else if (market.key === 'alternate_totals' || market.key === 'totals') {
          // `totals` is each book's primary; `alternate_totals` is its alts.
          // Merged into the same altTotals map since consumers (getFairProb)
          // don't care whether a line is a book's primary or alt — only that
          // we have enough book coverage to de-vig. Adding `totals` here is
          // what surfaces integer MLB totals that skip alternate_totals on
          // many US books (e.g. Pinnacle's primary MLB total is integer 8,
          // and it's NOT re-listed in Pinnacle's alternate_totals).
          for (const o of (market.outcomes || [])) {
            const lineKey = o.point;
            if (!result.altTotals[lineKey]) {
              result.altTotals[lineKey] = { probs: [], books: new Set(), byBook: {} };
            }
            const isOver = o.name === 'Over';
            const prob = americanToImpliedProb(o.price);
            result.altTotals[lineKey].probs.push({ isOver, prob });
            result.altTotals[lineKey].books.add(book.key);
            if (!result.altTotals[lineKey].byBook[book.key]) {
              result.altTotals[lineKey].byBook[book.key] = {};
            }
            result.altTotals[lineKey].byBook[book.key][isOver ? 'over' : 'under'] = o.price;
          }
        }
      }
    }

    // De-vig each line — require minimum number of books for accuracy.
    // Preserve byBook raw odds through the consolidation so per-book
    // accessors can look up the exact alt line even when consensus is too
    // thin to de-vig.
    // Accept gate: ≥ MIN_BOOKS, OR Pinnacle alone (sharp enough to trust).
    const hasPinnacle = (byBook) => !!byBook.pinnacle;
    const bookCountOk = (bookCount, byBook) =>
      bookCount >= ALT_LINES_MIN_BOOKS ||
      (ALT_LINES_PINNACLE_ALONE_OK && bookCount >= 1 && hasPinnacle(byBook));

    let skippedThinSpreads = 0, skippedThinTotals = 0;
    for (const [lineKey, lineData] of Object.entries(result.altSpreads)) {
      const bookCount = lineData.books.size;
      const byBook = lineData.byBook;
      const homeProbs = lineData.probs.filter(p => p.isHome).map(p => p.prob);
      const awayProbs = lineData.probs.filter(p => !p.isHome).map(p => p.prob);
      if (homeProbs.length > 0 && awayProbs.length > 0 && bookCountOk(bookCount, byBook)) {
        const [fh, fa] = deVig2Way(avg(homeProbs), avg(awayProbs));
        result.altSpreads[lineKey] = { home: fh, away: fa, books: bookCount, byBook };
      } else {
        if (homeProbs.length > 0 && awayProbs.length > 0) skippedThinSpreads++;
        // Keep a stub with only byBook so accessors can still return per-book
        // raw odds even when the consensus is too thin to de-vig.
        if (Object.keys(byBook).length > 0) {
          result.altSpreads[lineKey] = { home: null, away: null, books: bookCount, byBook };
        } else {
          delete result.altSpreads[lineKey];
        }
      }
    }

    for (const [lineKey, lineData] of Object.entries(result.altTotals)) {
      const bookCount = lineData.books.size;
      const byBook = lineData.byBook;
      const overProbs = lineData.probs.filter(p => p.isOver).map(p => p.prob);
      const underProbs = lineData.probs.filter(p => !p.isOver).map(p => p.prob);
      if (overProbs.length > 0 && underProbs.length > 0 && bookCountOk(bookCount, byBook)) {
        const [fo, fu] = deVig2Way(avg(overProbs), avg(underProbs));
        result.altTotals[lineKey] = { over: fo, under: fu, books: bookCount, byBook };
      } else {
        if (overProbs.length > 0 && underProbs.length > 0) skippedThinTotals++;
        if (Object.keys(byBook).length > 0) {
          result.altTotals[lineKey] = { over: null, under: null, books: bookCount, byBook };
        } else {
          delete result.altTotals[lineKey];
        }
      }
    }

    // F5 alt spreads + totals (MLB only). Identical consolidation as
    // the full-game loops above — de-vig per line with the same
    // min-books + Pinnacle-alone gate, keep byBook stubs for missing
    // consensus so per-book accessors still resolve.
    for (const [lineKey, lineData] of Object.entries(result.altSpreadsF5)) {
      const bookCount = lineData.books.size;
      const byBook = lineData.byBook;
      const homeProbs = lineData.probs.filter(p => p.isHome).map(p => p.prob);
      const awayProbs = lineData.probs.filter(p => !p.isHome).map(p => p.prob);
      if (homeProbs.length > 0 && awayProbs.length > 0 && bookCountOk(bookCount, byBook)) {
        const [fh, fa] = deVig2Way(avg(homeProbs), avg(awayProbs));
        result.altSpreadsF5[lineKey] = { home: fh, away: fa, books: bookCount, byBook };
      } else if (Object.keys(byBook).length > 0) {
        result.altSpreadsF5[lineKey] = { home: null, away: null, books: bookCount, byBook };
      } else {
        delete result.altSpreadsF5[lineKey];
      }
    }
    for (const [lineKey, lineData] of Object.entries(result.altTotalsF5)) {
      const bookCount = lineData.books.size;
      const byBook = lineData.byBook;
      const overProbs = lineData.probs.filter(p => p.isOver).map(p => p.prob);
      const underProbs = lineData.probs.filter(p => !p.isOver).map(p => p.prob);
      if (overProbs.length > 0 && underProbs.length > 0 && bookCountOk(bookCount, byBook)) {
        const [fo, fu] = deVig2Way(avg(overProbs), avg(underProbs));
        result.altTotalsF5[lineKey] = { over: fo, under: fu, books: bookCount, byBook };
      } else if (Object.keys(byBook).length > 0) {
        result.altTotalsF5[lineKey] = { over: null, under: null, books: bookCount, byBook };
      } else {
        delete result.altTotalsF5[lineKey];
      }
    }
    // NBA H1 alt spreads + totals. Same consolidation as the F5 loops —
    // de-vig per line with the same min-books + Pinnacle-alone gate, keep
    // byBook stubs for missing consensus so per-book accessors still resolve.
    for (const [lineKey, lineData] of Object.entries(result.altSpreadsH1)) {
      const bookCount = lineData.books.size;
      const byBook = lineData.byBook;
      const homeProbs = lineData.probs.filter(p => p.isHome).map(p => p.prob);
      const awayProbs = lineData.probs.filter(p => !p.isHome).map(p => p.prob);
      if (homeProbs.length > 0 && awayProbs.length > 0 && bookCountOk(bookCount, byBook)) {
        const [fh, fa] = deVig2Way(avg(homeProbs), avg(awayProbs));
        result.altSpreadsH1[lineKey] = { home: fh, away: fa, books: bookCount, byBook };
      } else if (Object.keys(byBook).length > 0) {
        result.altSpreadsH1[lineKey] = { home: null, away: null, books: bookCount, byBook };
      } else {
        delete result.altSpreadsH1[lineKey];
      }
    }
    for (const [lineKey, lineData] of Object.entries(result.altTotalsH1)) {
      const bookCount = lineData.books.size;
      const byBook = lineData.byBook;
      const overProbs = lineData.probs.filter(p => p.isOver).map(p => p.prob);
      const underProbs = lineData.probs.filter(p => !p.isOver).map(p => p.prob);
      if (overProbs.length > 0 && underProbs.length > 0 && bookCountOk(bookCount, byBook)) {
        const [fo, fu] = deVig2Way(avg(overProbs), avg(underProbs));
        result.altTotalsH1[lineKey] = { over: fo, under: fu, books: bookCount, byBook };
      } else if (Object.keys(byBook).length > 0) {
        result.altTotalsH1[lineKey] = { over: null, under: null, books: bookCount, byBook };
      } else {
        delete result.altTotalsH1[lineKey];
      }
    }

    altLinesCache[key] = result;
    const skippedNote = (skippedThinSpreads + skippedThinTotals) > 0 ? ` (skipped ${skippedThinSpreads} spreads + ${skippedThinTotals} totals with <${ALT_LINES_MIN_BOOKS} books)` : '';
    const f5Note = (Object.keys(result.altSpreadsF5).length || Object.keys(result.altTotalsF5).length) > 0
      ? `, F5: ${Object.keys(result.altSpreadsF5).length} spreads + ${Object.keys(result.altTotalsF5).length} totals`
      : '';
    const h1Note = (Object.keys(result.altSpreadsH1).length || Object.keys(result.altTotalsH1).length) > 0
      ? `, H1: ${Object.keys(result.altSpreadsH1).length} spreads + ${Object.keys(result.altTotalsH1).length} totals`
      : '';
    log.info('OddsFeed', `Cached alt lines: ${Object.keys(result.altSpreads).length} spreads, ${Object.keys(result.altTotals).length} totals${f5Note}${h1Note}${skippedNote}`);
    return result;
  } catch (err) {
    log.error('OddsFeed', `Alt lines error: ${err.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// ALT LINES PRE-WARMING — speculatively fetch alt lines for all registered
// events so on-demand fetch during RFQ is rare. This moves the latency cost
// off the critical path.
// ---------------------------------------------------------------------------

// Sports that publish alt spread/total markets on The Odds API AND
// are safe to aggressively pre-warm at every odds refresh cycle.
// Tennis stays in (thin alt coverage but cheap to fetch; helps RFQs
// on non-primary sets).
//
// 2026-04-22: major soccer leagues added to pre-warm. Soccer alt-line
// on-demand fetches were the single biggest remaining contributor to
// `decline → price` p95 latency (50-500ms on first RFQ per event
// before the on-demand populated the cache). Pre-warming moves that
// cost from the RFQ hot path to the 30s background refresh loop,
// bringing soccer alt RFQs from ~200ms p50 to near-zero.
//
// Minor/niche soccer leagues (Liga MX, Brasileirão, Libertadores,
// NWSL) stay on-demand only to cap API cost — flow there is thin
// and the pre-warm quota would mostly be wasted. Strict-safety
// gating still applies to ALL soccer (see isStrictAltSanitySport).
const SPORTS_WITH_ALT_MARKETS = new Set([
  'basketball_nba', 'basketball_ncaab', 'basketball_wnba',
  'baseball_mlb',
  'icehockey_nhl',
  'americanfootball_nfl', 'americanfootball_ncaaf',
  'tennis',
  // Major soccer leagues — pre-warmed as of 2026-04-22
  'soccer_epl',
  'soccer_spain_la_liga',
  'soccer_italy_serie_a',
  'soccer_germany_bundesliga',
  'soccer_france_ligue_one',
  'soccer_uefa_champs_league',
  'soccer_uefa_europa_league',
  'soccer_usa_mls',
]);

// Sports with strict-safety alt-line handling (tighter lineDiff
// threshold, min-book requirement, reverse sanity). These are sports
// with thin alt-line book coverage where an alt-as-primary mispricing
// would be high-impact. Soccer in particular had a history of alt-
// line confusion incidents that spawned the strict-mode safeguards.
//
// Membership is NOT tied to pre-warm state — soccer is both strict
// AND pre-warmed now. Keep this set explicit rather than deriving
// from other sets so the safety intent stays readable.
//
// Generic 'soccer' key + niche leagues stay on-demand and strict.
const SPORTS_WITH_ONDEMAND_ALT_MARKETS = new Set([
  'soccer',
  'soccer_usa_mls',
  'soccer_epl',
  'soccer_spain_la_liga',
  'soccer_italy_serie_a',
  'soccer_germany_bundesliga',
  'soccer_france_ligue_one',
  'soccer_uefa_champs_league',
  'soccer_uefa_europa_league',
  // Niche leagues: stay on-demand only (not added to pre-warm above)
  'soccer_mexico_ligamx',
  'soccer_brazil_campeonato',
  'soccer_conmebol_libertadores',
  'soccer_usa_nwsl',
]);

// True when either gate (pre-warm or on-demand) applies. Use in
// runtime code paths (getFairProb, fetchAltLines callers); use the
// narrower SPORTS_WITH_ALT_MARKETS for pre-warm scheduling only.
function sportSupportsAltLines(sport) {
  return SPORTS_WITH_ALT_MARKETS.has(sport) || SPORTS_WITH_ONDEMAND_ALT_MARKETS.has(sport);
}

// Safety-sensitive sports get tighter lineDiff sanity gates and a
// minimum-book requirement on alt-line acceptance. Mispricing an
// alt line as primary is one of the worst bug classes we've hit
// (NHL spreads, MLB totals) — soccer has the same risk profile with
// even thinner book coverage on some leagues.
function isStrictAltSanitySport(sport) {
  return SPORTS_WITH_ONDEMAND_ALT_MARKETS.has(sport);
}

// Strict-mode lineDiff threshold: if |alt_line - primary_line| >=
// this value, run the sanity check (direction makes sense, alt
// fair is within plausible range). Default is 2.0; soccer is 1.0
// because EPL goals span a narrow range (2-4 typical) so even a
// 1-goal move between primary and alt is material.
function altSanityLineDiffThreshold(sport) {
  return isStrictAltSanitySport(sport) ? 1.0 : 2.0;
}

// Maximum distance |alt - primary| we'll quote on for strict-mode
// sports. Beyond this, the alt is too far from primary to trust —
// even if the alt cache has data, the de-vig can be wildly off on
// extreme tails. Soccer cap is 3 goals (e.g. primary 2.5 max alt
// 5.5); anything beyond declines.
function altMaxLineDistance(sport) {
  return isStrictAltSanitySport(sport) ? 3.0 : Infinity;
}

// Only pre-warm events starting within this window — avoids wasting API calls
// on events days in the future that the bettor almost certainly won't RFQ.
const WARM_EVENT_MAX_HOURS_AHEAD = 48;

// Concurrency limit: at most N alt-line fetches in flight simultaneously.
// Keeps us from burying The Odds API rate limiter.
const WARM_CONCURRENCY = 2;
// Inter-request delay inside each warm worker. The Odds API rate-limits
// (per-key token bucket) and a startup burst across sports × MLB's new
// F5 alt markets was producing 8+ "Requests are too frequent" 429s per
// warm cycle. 120ms throttle caps each worker at ~8 req/s; with
// WARM_CONCURRENCY=2 per sport and ~4 sports warming, peak is ~65 req/s
// globally — well under the Odds API ~100 req/s ceiling while still
// completing a full sport warm in under 30s.
const WARM_REQUEST_DELAY_MS = 120;

// Last warm stats (for /alt-lines-cache-stats)
let _lastWarmStats = null;

/**
 * Pre-warm alt-line cache for all registered events in a sport.
 * Skips events already fresh in cache (< ALT_LINES_TTL_MS old) to avoid
 * duplicate work. Events with commenceTime more than N hours out are
 * also skipped — bettor demand is concentrated in the next 1-2 days.
 *
 * Runs with bounded concurrency so The Odds API isn't hammered.
 */
// Sports where SharpAPI is the primary feed but occasionally returns
// events without an h2h market (e.g. books haven't posted moneylines
// yet even though spreads/totals are up). We backfill via The Odds
// API on each refresh cycle so the pricer always has a moneyline to
// quote against, at the cost of ~1 Odds API call per sport per cycle.
const H2H_BACKFILL_SPORTS = new Set(['baseball_mlb', 'basketball_nba', 'icehockey_nhl']);

/**
 * Merge DK-scraped MMA fight odds into oddsCache['mma_mixed_martial_arts'].
 * The Odds API typically only carries 2-3 of a UFC Fight Night card's ~12
 * fights; DK carries all of them. We pull the DK scraper's parsed fight
 * list and, for each fight not already in the cache (matched by fighter
 * last-word pairs), inject a new event entry with markets.h2h populated.
 * After this runs, the line-manager seed picks them up and registers
 * moneylines with PX, unlocking RFQ routing for the full card.
 */
/**
 * Pull DK's in-play markets for a sport and write them into liveOddsCache.
 * Replaces anything SharpAPI's live fetch populated (DK is preferred over
 * SharpAPI for in-play because of coverage + speed on top-4 US books).
 * Events keyed by normalized (home, away) pair so getLiveFairProb works.
 */
async function mergeDkLiveOdds(sport) {
  const dk = require('./dk-scraper');
  let live;
  try {
    live = await dk.fetchLiveMarkets(sport);
  } catch (err) {
    log.warn('OddsFeed', `DK live fetch failed for ${sport}: ${err.message}`);
    return { merged: 0, sport, err: err.message };
  }
  if (!live || !Array.isArray(live.events) || live.events.length === 0) {
    return { merged: 0, sport };
  }
  // Build events map keyed by pair, keeping arrays for doubleheaders.
  const events = {};
  for (const ev of live.events) {
    if (!ev.homeTeam || !ev.awayTeam) continue;
    const key = normalizeEventKey(ev.homeTeam, ev.awayTeam);
    // Remap markets from dk-scraper shape into what oddsFeed.getLiveFairProb expects.
    // dk-scraper emits markets: { h2h, totals: { [line]: {...}, _primary } }.
    // liveOddsCache expects the same shape as oddsCache events — we mirror it.
    const entry = {
      homeTeam: ev.homeTeam,
      awayTeam: ev.awayTeam,
      commenceTime: ev.commenceTime,
      eventId: 'dk-live-' + ev.eventId,
      markets: ev.markets || {},
    };
    if (!events[key]) events[key] = [];
    events[key].push(entry);
  }
  if (!liveOddsCache[sport]) liveOddsCache[sport] = {};
  liveOddsCache[sport] = {
    fetchedAt: Date.now(),
    events,
  };
  log.info('OddsFeed', `DK live merge ${sport}: ${live.events.length} in-progress events cached`);
  return { merged: live.events.length, sport };
}

// Odds-API sport keys for the live in-play fetch. Same endpoint as pre-game —
// `commence_time < now` naturally returns in-progress events, at no extra cost.
const ODDS_API_LIVE_SPORTS = {
  basketball_nba: 'basketball_nba',
  baseball_mlb: 'baseball_mlb',
  icehockey_nhl: 'icehockey_nhl',
  americanfootball_nfl: 'americanfootball_nfl',
};

/**
 * Pull in-play markets from The Odds API (Pinnacle + DK + FD) and write them
 * into liveOddsCache in the shape getLiveFairProb expects. Replaces the DK
 * Puppeteer scraper for live odds — same coverage, no Akamai fragility, no
 * browser overhead. Same quota cost as pre-game Odds API calls.
 *
 * Filters events to in-progress (commence_time in past, <6h elapsed) before
 * writing. De-vigs each book's 2-way pair, then averages fair probs across
 * books for each market/line.
 */
async function mergeOddsApiLive(sport) {
  const apiKey = process.env.THE_ODDS_API_KEY;
  const apiSport = ODDS_API_LIVE_SPORTS[sport];
  if (!apiKey || !apiSport) return { merged: 0, sport };

  const url = `https://api.the-odds-api.com/v4/sports/${apiSport}/odds`
    + `?apiKey=${apiKey}`
    + `&regions=us,eu`
    + `&markets=h2h,spreads,totals`
    + `&bookmakers=pinnacle,draftkings,fanduel`
    + `&oddsFormat=american`;

  let events;
  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      log.warn('OddsFeed', `Odds API live fetch failed (${resp.status}) for ${sport}`);
      return { merged: 0, sport };
    }
    const remaining = resp.headers.get('x-requests-remaining');
    const used = resp.headers.get('x-requests-used');
    if (remaining != null) log.debug('OddsFeed', `Odds API live usage: ${used} used, ${remaining} remaining`);
    events = await resp.json();
  } catch (err) {
    log.warn('OddsFeed', `Odds API live fetch error for ${sport}: ${err.message}`);
    return { merged: 0, sport };
  }

  const now = Date.now();
  const SIX_HOURS_MS = 6 * 60 * 60 * 1000;
  const inProgress = (events || []).filter(ev => {
    const t = ev.commence_time ? new Date(ev.commence_time).getTime() : null;
    if (!t || isNaN(t)) return false;
    const elapsed = now - t;
    return elapsed >= 0 && elapsed < SIX_HOURS_MS;
  });

  if (inProgress.length === 0) {
    return { merged: 0, sport };
  }

  const cacheEvents = {};
  for (const ev of inProgress) {
    const home = ev.home_team;
    const away = ev.away_team;
    if (!home || !away) continue;
    const markets = {};

    // --- Moneyline (h2h) ---
    const mlFair = { home: [], away: [] };
    for (const book of (ev.bookmakers || [])) {
      const m = (book.markets || []).find(x => x.key === 'h2h');
      if (!m) continue;
      const h = (m.outcomes || []).find(o => o.name === home);
      const a = (m.outcomes || []).find(o => o.name === away);
      if (!h || !a) continue;
      const hp = americanToImpliedProb(h.price);
      const ap = americanToImpliedProb(a.price);
      if (!hp || !ap) continue;
      const [fh, fa] = deVig2Way(hp, ap);
      mlFair.home.push(fh);
      mlFair.away.push(fa);
    }
    if (mlFair.home.length > 0) {
      const fh = avg(mlFair.home), fa = avg(mlFair.away);
      markets.h2h = {
        home: { fairProb: fh, displayFairProb: fh },
        away: { fairProb: fa, displayFairProb: fa },
        books: mlFair.home.length,
        source: 'odds-api-live',
      };
    }

    // --- Spreads (keyed by line magnitude) ---
    // Books may carry slightly different magnitudes (e.g. Pin -8.5, DK -9).
    // Store each magnitude as its own line entry — the consumer does an
    // exact-line lookup and falls back to `_primary` if the leg's line
    // isn't present. home/away preserve bookmaker's sign-of-home convention.
    const spreadsByLine = {};
    for (const book of (ev.bookmakers || [])) {
      const m = (book.markets || []).find(x => x.key === 'spreads');
      if (!m) continue;
      const h = (m.outcomes || []).find(o => o.name === home);
      const a = (m.outcomes || []).find(o => o.name === away);
      if (!h || !a || h.point == null || a.point == null) continue;
      const line = Math.abs(Number(h.point));
      if (!Number.isFinite(line)) continue;
      const hp = americanToImpliedProb(h.price);
      const ap = americanToImpliedProb(a.price);
      if (!hp || !ap) continue;
      const [fh, fa] = deVig2Way(hp, ap);
      if (!spreadsByLine[line]) spreadsByLine[line] = { home: [], away: [] };
      spreadsByLine[line].home.push(fh);
      spreadsByLine[line].away.push(fa);
    }
    const spreads = {};
    let spreadPrimary = null;
    let spreadPrimaryBooks = 0;
    for (const [line, bucket] of Object.entries(spreadsByLine)) {
      const fh = avg(bucket.home), fa = avg(bucket.away);
      spreads[line] = {
        line: parseFloat(line),
        home: { fairProb: fh, displayFairProb: fh },
        away: { fairProb: fa, displayFairProb: fa },
        books: bucket.home.length,
        source: 'odds-api-live',
      };
      if (bucket.home.length > spreadPrimaryBooks) {
        spreadPrimaryBooks = bucket.home.length;
        spreadPrimary = parseFloat(line);
      }
    }
    if (spreadPrimary != null) {
      spreads._primary = spreadPrimary;
      markets.spreads = spreads;
    }

    // --- Totals (keyed by line) ---
    const totalsByLine = {};
    for (const book of (ev.bookmakers || [])) {
      const m = (book.markets || []).find(x => x.key === 'totals');
      if (!m) continue;
      const ov = (m.outcomes || []).find(o => o.name === 'Over');
      const un = (m.outcomes || []).find(o => o.name === 'Under');
      if (!ov || !un || ov.point == null) continue;
      const line = Number(ov.point);
      if (!Number.isFinite(line)) continue;
      const op = americanToImpliedProb(ov.price);
      const up = americanToImpliedProb(un.price);
      if (!op || !up) continue;
      const [fo, fu] = deVig2Way(op, up);
      if (!totalsByLine[line]) totalsByLine[line] = { over: [], under: [] };
      totalsByLine[line].over.push(fo);
      totalsByLine[line].under.push(fu);
    }
    const totals = {};
    let totalsPrimary = null;
    let totalsPrimaryBooks = 0;
    for (const [line, bucket] of Object.entries(totalsByLine)) {
      const fo = avg(bucket.over), fu = avg(bucket.under);
      totals[line] = {
        line: parseFloat(line),
        over: { fairProb: fo, displayFairProb: fo },
        under: { fairProb: fu, displayFairProb: fu },
        books: bucket.over.length,
        source: 'odds-api-live',
      };
      if (bucket.over.length > totalsPrimaryBooks) {
        totalsPrimaryBooks = bucket.over.length;
        totalsPrimary = parseFloat(line);
      }
    }
    if (totalsPrimary != null) {
      totals._primary = totalsPrimary;
      markets.totals = totals;
    }

    if (!markets.h2h && !markets.spreads && !markets.totals) continue;

    const key = normalizeEventKey(home, away);
    const entry = {
      homeTeam: home,
      awayTeam: away,
      commenceTime: ev.commence_time,
      eventId: 'oddsapi-live-' + ev.id,
      markets,
    };
    if (!cacheEvents[key]) cacheEvents[key] = [];
    cacheEvents[key].push(entry);
  }

  const evCount = Object.values(cacheEvents).reduce((s, arr) => s + arr.length, 0);
  if (evCount === 0) return { merged: 0, sport };

  liveOddsCache[sport] = { fetchedAt: Date.now(), events: cacheEvents };
  const mlN = Object.values(cacheEvents).flat().filter(e => e.markets.h2h).length;
  const spN = Object.values(cacheEvents).flat().filter(e => e.markets.spreads).length;
  const toN = Object.values(cacheEvents).flat().filter(e => e.markets.totals).length;
  log.info('OddsFeed', `Odds API live ${sport}: ${evCount} events (h2h:${mlN} spreads:${spN} totals:${toN})`);
  return { merged: evCount, sport };
}

async function mergeDkMmaFights() {
  const dk = require('./dk-scraper');
  let fightData;
  try {
    fightData = await dk.fetchMmaFightOdds();
  } catch (err) {
    log.warn('OddsFeed', `DK MMA fetch failed: ${err.message}`);
    return { merged: 0, added: 0, err: err.message };
  }
  if (!fightData || !fightData.fights || fightData.fights.length === 0) {
    return { merged: 0, added: 0 };
  }
  const sport = 'mma_mixed_martial_arts';
  if (!oddsCache[sport]) oddsCache[sport] = { fetchedAt: Date.now(), events: {} };
  const cache = oddsCache[sport];

  // Build fuzzy lookup over existing cache events by last-word fighter pair.
  // We also keep a handle to the existing event object so we can graft DK
  // totals onto SharpAPI-seeded h2h entries (SharpAPI MMA feed is moneyline-
  // only, so without enrichment totals would silently 404).
  const lw = (n) => (n || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9 ]/g, '').split(/\s+/).filter(Boolean).pop() || '';
  // CRITICAL: a single fight pair can produce MULTIPLE cache entries when
  // SharpAPI and DK disagree on home/away ordering — they end up as two
  // events under different normalizeEventKey results (e.g. both
  // "Gorimbo vs Micallef" AND "Micallef vs Gorimbo" present in
  // cache.events at the same time). The line-manager binds to whichever
  // the seed picked first — usually the SharpAPI h2h-only one. If we
  // only enrich ONE entry, the other stays bare and getFairProb returns
  // null on totals lookups against the line-manager's chosen orientation.
  // So we collect ALL events per pair (array) and enrich every one.
  const existingByPair = new Map(); // "a|b" → event[]
  for (const entry of Object.values(cache.events || {})) {
    const arr = Array.isArray(entry) ? entry : [entry];
    for (const ev of arr) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      const a = lw(ev.homeTeam), b = lw(ev.awayTeam);
      if (!a || !b) continue;
      const key1 = a + '|' + b;
      const key2 = b + '|' + a;
      if (!existingByPair.has(key1)) existingByPair.set(key1, []);
      if (!existingByPair.has(key2)) existingByPair.set(key2, []);
      const list1 = existingByPair.get(key1);
      const list2 = existingByPair.get(key2);
      if (!list1.includes(ev)) list1.push(ev);
      if (!list2.includes(ev)) list2.push(ev);
    }
  }

  // Helper: build the markets.totals block from a DK fight's totals array.
  function buildTotalsBlock(totals) {
    if (!Array.isArray(totals) || totals.length === 0) return null;
    // Pick a primary line — prefer the middle (median fight duration:
    // 3-round ≈ 2.5, 5-round ≈ 4.5). Remaining lines become alt.
    const sorted = [...totals].sort((a, b) => a.line - b.line);
    const primary = sorted[Math.floor(sorted.length / 2)];
    // byLine map keyed by string-line so getFairProb's fast path
    // (services/odds-feed.js — `if (marketType === 'totals' && market.byLine)`)
    // can resolve PX alt-line requests without depending on altLinesCache,
    // which is empty for MMA (not in SPORTS_WITH_ALT_MARKETS).
    const byLine = {};
    for (const t of sorted) {
      byLine[String(t.line)] = {
        line: t.line,
        over: { rawOdds: t.over.americanOdds, impliedProb: t.over.impliedProb, fairProb: t.over.fairProb, displayFairProb: t.over.fairProb },
        under: { rawOdds: t.under.americanOdds, impliedProb: t.under.impliedProb, fairProb: t.under.fairProb, displayFairProb: t.under.fairProb },
        books: 1,
      };
    }
    return {
      line: primary.line,
      over: {
        rawOdds: primary.over.americanOdds, impliedProb: primary.over.impliedProb,
        fairProb: primary.over.fairProb, displayFairProb: primary.over.fairProb,
      },
      under: {
        rawOdds: primary.under.americanOdds, impliedProb: primary.under.impliedProb,
        fairProb: primary.under.fairProb, displayFairProb: primary.under.fairProb,
      },
      books: 1,
      byLine,
      alt: sorted.map(t => ({
        line: t.line,
        over: { rawOdds: t.over.americanOdds, impliedProb: t.over.impliedProb, fairProb: t.over.fairProb, displayFairProb: t.over.fairProb },
        under: { rawOdds: t.under.americanOdds, impliedProb: t.under.impliedProb, fairProb: t.under.fairProb, displayFairProb: t.under.fairProb },
      })),
      pinnacle: null, fanduel: null,
      draftkings: { line: primary.line, over: primary.over.americanOdds, under: primary.under.americanOdds },
      kalshi: null,
      dkScraped: true,
    };
  }

  let added = 0, enriched = 0, skipped = 0;
  for (const fight of fightData.fights) {
    if (!fight.fighters || fight.fighters.length !== 2) continue;
    const [f1, f2] = fight.fighters;
    const p1 = lw(f1.fighter), p2 = lw(f2.fighter);
    if (!p1 || !p2) continue;
    // existingByPair returns an ARRAY of events for this pair — usually
    // 1, but 2 when SharpAPI + DK ingest produced different home/away
    // orderings (separate cache entries for the same fight). We need
    // to enrich EVERY entry so whichever orientation the line-manager
    // bound to also has totals.
    const existingList = existingByPair.get(p1 + '|' + p2) || existingByPair.get(p2 + '|' + p1) || [];
    if (existingList.length > 0) {
      const block = buildTotalsBlock(fight.totals);
      let didEnrich = false;
      for (const existing of existingList) {
        if (!existing.markets) existing.markets = {};
        if (!existing.markets.totals && block) {
          existing.markets.totals = block;
          didEnrich = true;
        }
      }
      if (didEnrich) enriched++;
      else skipped++;
      continue;
    }
    // DK doesn't label home/away for MMA (it's a neutral-site fight); use
    // the first fighter as 'home' arbitrarily. line-manager's seed matches
    // teamName→competitor by exact/substring anyway.
    const homeTeam = f1.fighter, awayTeam = f2.fighter;
    const key = normalizeEventKey(homeTeam, awayTeam);
    const markets = {
      h2h: {
        home: {
          rawOdds: f1.americanOdds, impliedProb: f1.impliedProb,
          fairProb: f1.fairProb, displayFairProb: f1.fairProb,
        },
        away: {
          rawOdds: f2.americanOdds, impliedProb: f2.impliedProb,
          fairProb: f2.fairProb, displayFairProb: f2.fairProb,
        },
        books: 1,
        pinnacle: null, fanduel: null,
        draftkings: { home: f1.americanOdds, away: f2.americanOdds },
        kalshi: null,
        dkScraped: true,
      },
    };
    const totalsBlock = buildTotalsBlock(fight.totals);
    if (totalsBlock) markets.totals = totalsBlock;
    const newEvent = {
      homeTeam, awayTeam,
      commenceTime: fight.startTime || null,
      eventId: 'dk-mma-' + fight.eventId,
      markets,
    };
    if (!cache.events[key]) cache.events[key] = [];
    if (Array.isArray(cache.events[key])) cache.events[key].push(newEvent);
    else cache.events[key] = [cache.events[key], newEvent];
    added++;
  }
  cache.fetchedAt = Date.now();
  log.info('OddsFeed', `MMA DK merge: added ${added}, enriched-totals ${enriched}, skipped ${skipped} (total DK fights: ${fightData.fights.length})`);
  return { added, enriched, skipped, total: fightData.fights.length };
}

async function backfillMissingH2h(sport) {
  if (!H2H_BACKFILL_SPORTS.has(sport)) return null;
  const cache = oddsCache[sport];
  if (!cache || !cache.events) return null;

  const missing = [];
  for (const [key, entry] of Object.entries(cache.events)) {
    const events = Array.isArray(entry) ? entry : [entry];
    for (const ev of events) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      if (ev.markets && ev.markets.h2h && ev.markets.h2h.home && ev.markets.h2h.away) continue;
      missing.push({ key, ev });
    }
  }
  if (missing.length === 0) return { sport, missing: 0, filled: 0 };

  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return { sport, missing: missing.length, filled: 0, err: 'no THE_ODDS_API_KEY' };

  const oddsApiSport = PINNACLE_SPORT_MAP[sport] || sport;
  const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/odds`
    + `?apiKey=${theOddsApiKey}&regions=us,eu&markets=h2h&oddsFormat=american`;

  let filled = 0;
  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      const text = await resp.text();
      return { sport, missing: missing.length, filled: 0, err: `The Odds API ${resp.status}: ${text.slice(0, 120)}` };
    }
    const apiEvents = await resp.json();

    // Index apiEvents by normalized team-pair key (both directions).
    const byKey = {};
    for (const e of apiEvents) {
      const k = normalizeEventKey(e.home_team, e.away_team);
      const kRev = normalizeEventKey(e.away_team, e.home_team);
      byKey[k] = e;
      byKey[kRev] = e;
    }

    for (const { ev } of missing) {
      const apiEvent = byKey[normalizeEventKey(ev.homeTeam, ev.awayTeam)]
                   || byKey[normalizeEventKey(ev.awayTeam, ev.homeTeam)];
      if (!apiEvent) continue;

      // Collect h2h pairs from every book.
      const mlPairs = [];
      for (const book of (apiEvent.bookmakers || [])) {
        const mk = book.markets?.find(m => m.key === 'h2h');
        if (!mk) continue;
        const home = mk.outcomes?.find(o => o.name === apiEvent.home_team);
        const away = mk.outcomes?.find(o => o.name === apiEvent.away_team);
        if (home && away) {
          mlPairs.push({
            book: book.key,
            home: { odds_probability: americanToImpliedProb(home.price), odds_american: home.price },
            away: { odds_probability: americanToImpliedProb(away.price), odds_american: away.price },
          });
        }
      }
      if (mlPairs.length === 0) continue;

      // Align apiEvent home/away to OUR event's home/away orientation —
      // the odds-api sometimes swaps sides vs SharpAPI. If swapped, flip.
      const apiHomeMatchesOurHome = apiEvent.home_team.toLowerCase().includes(ev.homeTeam.toLowerCase().split(' ').pop())
                                  || ev.homeTeam.toLowerCase().includes(apiEvent.home_team.toLowerCase().split(' ').pop());
      const getOur = (pairSide) => apiHomeMatchesOurHome ? pairSide : (pairSide === 'home' ? 'away' : 'home');

      const fairHome = [], fairAway = [];
      for (const p of mlPairs) {
        const [fh, fa] = deVig2Way(p.home.odds_probability, p.away.odds_probability);
        fairHome.push(apiHomeMatchesOurHome ? fh : fa);
        fairAway.push(apiHomeMatchesOurHome ? fa : fh);
      }
      const pinBook = mlPairs.find(p => p.book === 'pinnacle');
      const fdBook = mlPairs.find(p => p.book === 'fanduel');
      const dkBook = mlPairs.find(p => p.book === 'draftkings');
      const dvH = avg(fairHome), dvA = avg(fairAway);

      ev.markets = ev.markets || {};
      ev.markets.h2h = {
        home: {
          rawOdds: apiHomeMatchesOurHome ? mlPairs[0].home.odds_american : mlPairs[0].away.odds_american,
          impliedProb: apiHomeMatchesOurHome ? mlPairs[0].home.odds_probability : mlPairs[0].away.odds_probability,
          fairProb: dvH,
          displayFairProb: dvH,
        },
        away: {
          rawOdds: apiHomeMatchesOurHome ? mlPairs[0].away.odds_american : mlPairs[0].home.odds_american,
          impliedProb: apiHomeMatchesOurHome ? mlPairs[0].away.odds_probability : mlPairs[0].home.odds_probability,
          fairProb: dvA,
          displayFairProb: dvA,
        },
        books: mlPairs.length,
        pinnacle: pinBook ? { home: apiHomeMatchesOurHome ? pinBook.home.odds_american : pinBook.away.odds_american, away: apiHomeMatchesOurHome ? pinBook.away.odds_american : pinBook.home.odds_american } : null,
        fanduel: fdBook ? { home: apiHomeMatchesOurHome ? fdBook.home.odds_american : fdBook.away.odds_american, away: apiHomeMatchesOurHome ? fdBook.away.odds_american : fdBook.home.odds_american } : null,
        draftkings: dkBook ? { home: apiHomeMatchesOurHome ? dkBook.home.odds_american : dkBook.away.odds_american, away: apiHomeMatchesOurHome ? dkBook.away.odds_american : dkBook.home.odds_american } : null,
        kalshi: null,
        backfilled: true,
      };
      filled++;
    }

    log.info('OddsFeed', `H2H backfill ${sport}: filled ${filled}/${missing.length} missing events from The Odds API`);
    return { sport, missing: missing.length, filled };
  } catch (err) {
    log.warn('OddsFeed', `H2H backfill ${sport} failed: ${err.message}`);
    return { sport, missing: missing.length, filled, err: err.message };
  }
}

async function warmAltLines(sport) {
  if (!SPORTS_WITH_ALT_MARKETS.has(sport)) return { skipped: 'no alt markets' };

  const cache = oddsCache[sport];
  if (!cache || !cache.events) return { skipped: 'no event cache' };

  const now = Date.now();
  const cutoffMs = now + WARM_EVENT_MAX_HOURS_AHEAD * 3600 * 1000;

  // Collect candidate events: home/away pairs with near-term commenceTime
  // and not already fresh in alt-line cache.
  //
  // Skip PX's conditional playoff events ("Game 3: Boston", "Boston 2026
  // 1st Round series", etc.) — these aren't individual books' events and
  // The Odds API has no matching record. Warming would burn API quota
  // noMatch'ing them every cycle.
  const conditionalPlayoffPattern =
    /(^|\s)game\s*\d+\s*:|\d{4}\s+\w+\s+round|\bseries\s*$/i;
  const candidates = [];
  for (const [key, entry] of Object.entries(cache.events)) {
    const events = Array.isArray(entry) ? entry : [entry];
    for (const ev of events) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      if (conditionalPlayoffPattern.test(ev.homeTeam) ||
          conditionalPlayoffPattern.test(ev.awayTeam)) continue;
      const startMs = ev.commenceTime ? new Date(ev.commenceTime).getTime() : null;
      if (startMs && !isNaN(startMs)) {
        if (startMs < now) continue;              // already started
        if (startMs > cutoffMs) continue;         // too far out
      }
      const altKey = normalizeEventKey(ev.homeTeam, ev.awayTeam);
      const altCached = altLinesCache[altKey];
      if (altCached && (now - altCached.fetchedAt) < ALT_LINES_TTL_MS) continue;
      candidates.push({
        homeTeam: ev.homeTeam,
        awayTeam: ev.awayTeam,
        commenceTime: ev.commenceTime || null,
      });
    }
  }

  if (candidates.length === 0) {
    return { sport, candidates: 0, fetched: 0, noMatch: 0, errors: 0 };
  }

  let fetched = 0, errors = 0, noMatch = 0;
  const unmatched = []; // up to 5 samples — used to diagnose matching gaps
  // Bounded-concurrency worker pool. Each worker waits WARM_REQUEST_DELAY_MS
  // between its own fetches to throttle against Odds API's 429 ("Requests
  // are too frequent"). The first iteration skips the sleep.
  let idx = 0;
  async function worker() {
    let iter = 0;
    while (idx < candidates.length) {
      if (iter++ > 0) await new Promise(r => setTimeout(r, WARM_REQUEST_DELAY_MS));
      const i = idx++;
      const c = candidates[i];
      try {
        // Pre-check: does resolveOddsApiEventId find a match? If not, we
        // know the fetch would fail for a matching reason vs an API error.
        // This lets the stats separate "no match" from "API call failed".
        const resolved = await resolveOddsApiEventId(sport, c.homeTeam, c.awayTeam, c.commenceTime);
        if (!resolved) {
          noMatch++;
          if (unmatched.length < 5) unmatched.push(`${c.homeTeam} vs ${c.awayTeam}`);
          continue;
        }
        const r = await fetchAltLines(sport, c.homeTeam, c.awayTeam, c.commenceTime);
        if (r) fetched++;
        else errors++; // resolved but fetch returned nothing (API error, empty response)
      } catch (err) {
        errors++;
      }
    }
  }
  const workers = [];
  for (let i = 0; i < Math.min(WARM_CONCURRENCY, candidates.length); i++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  const stats = {
    sport,
    candidates: candidates.length,
    fetched,
    noMatch,
    errors,
    unmatchedSamples: unmatched,
    completedAt: new Date().toISOString(),
  };
  _lastWarmStats = _lastWarmStats || {};
  _lastWarmStats[sport] = stats;
  log.info('OddsFeed', `Alt-line warm ${sport}: ${fetched}/${candidates.length} fetched (${noMatch} no-match, ${errors} errors)`);
  return stats;
}

/**
 * Warm alt lines for every sport that has alt markets, in parallel.
 * Returns when all sport-level warms complete (or a per-sport error is caught).
 * Used both at boot (pre-WebSocket) and from the 60s periodic loop.
 */
async function warmAllSports() {
  const sports = [...SPORTS_WITH_ALT_MARKETS].filter(s =>
    (config.supportedSports || []).includes(s)
  );
  const settle = await Promise.allSettled(sports.map(s =>
    warmAltLines(s).catch(err => {
      log.warn('OddsFeed', `Warm loop error for ${s}: ${err.message}`);
      return null;
    })
  ));
  return settle.map((r, i) => ({ sport: sports[i], ok: r.status === 'fulfilled', result: r.value }));
}

// Periodic-warm loop handle so callers (and tests) can start/stop it cleanly.
let _warmLoopTimer = null;
// 15s interval (tightened from 30s 2026-04-22). Bounds the window between a
// newly-registered PX event and its first alt-line warm — i.e., the window
// where the first RFQ touching a non-primary line pays on-demand fetch
// latency. TTL gating (ALT_LINES_TTL_MS = 10 min) means events already
// warm get skipped, so the tighter interval doesn't meaningfully multiply
// API quota — it just shortens the new-event coverage gap. With soccer now
// in the pre-warm set (see SPORTS_WITH_ALT_MARKETS), 15s is a better
// match to how quickly soccer RFQ flow can hit a freshly-registered game.
const WARM_LOOP_INTERVAL_MS = 15 * 1000;

/**
 * Start the background warm loop. Safe to call multiple times — second calls
 * are no-ops. Runs warmAllSports every WARM_LOOP_INTERVAL_MS (60s).
 * Deploy-survival: warmAltLines skips events already fresh under ALT_LINES_TTL_MS,
 * so the loop doesn't hammer The Odds API after the initial population.
 */
function startAltLineWarmLoop() {
  if (_warmLoopTimer) return;
  _warmLoopTimer = setInterval(() => {
    warmAllSports().catch(err => {
      log.warn('OddsFeed', `Alt-line warm loop failed: ${err.message}`);
    });
  }, WARM_LOOP_INTERVAL_MS);
  log.info('OddsFeed', `Alt-line warm loop started (every ${WARM_LOOP_INTERVAL_MS / 1000}s)`);
}

// ---------------------------------------------------------------------------
// Pinnacle line-verify cache warmer
// ---------------------------------------------------------------------------
// Pre-warms _pinVerifyCache entries before their 30s TTL expires so RFQs
// with primary spread/total legs never pay the 20-30ms cold-cache fetch
// inline. Sequential across (sport, market) combos with inter-request
// pacing to stay well under Odds API's token bucket.
//
// Without this, the first primary spread/total RFQ per (sport, market)
// per 30s window paid the full verify fetch — manifesting as a p95 spike
// while p50 stayed fast. The pricer's verifyLineWithPinnacle sits behind
// Promise.all alongside getFairProbAsync, so its cost hits every RFQ
// whose line matches the cached primary (which is most primary-line
// RFQs — the common case).
//
// Scope: only the (sport, market) combos we actually serve. Derives from
// PINNACLE_SPORT_MAP ∩ supportedSports × {spreads, totals}.
let _pinVerifyWarmTimer = null;
const PIN_VERIFY_WARM_INTERVAL_MS = 20 * 1000; // 20s inside 30s TTL
const PIN_VERIFY_WARM_DELAY_MS = 120;          // inter-request pacing

// Demand-aware gating. Only warm combos that have been touched by an
// RFQ's verifyLineWithPinnacle call in the last N minutes. Quiet combos
// (e.g. MMA with 73 events but zero RFQs this hour) stop consuming
// Odds API quota. Newly-active combos pay one cold-cache verify (~20ms)
// on the first RFQ after a cold period — acceptable trade vs. the
// ~60-75% quota reduction when most leagues sit idle.
const PIN_VERIFY_ACTIVITY_WINDOW_MS = 5 * 60 * 1000;
const _pinVerifyRfqTouch = {}; // comboKey -> last-RFQ timestamp

// Core sports always stay in the warm rotation regardless of recent
// RFQ activity. These are our primary volume drivers — we accept the
// 6 fetches / 20s cost for them to avoid ever paying cold-verify on a
// primary-line RFQ in an active league. Everything NOT in this set
// goes demand-aware (soccer niche leagues, MMA, etc.). Cost: ~18
// fetches/min always-on; still ~80% below the pre-patch baseline.
const _pinVerifyAlwaysWarmSports = new Set([
  'basketball_nba',
  'baseball_mlb',
  'icehockey_nhl',
]);

// Persistent-error cooldown. If a combo fails N cycles in a row, park
// it for 10 min — typically means Pinnacle doesn't cover that sport/
// market on The Odds API's event-list endpoint (verified for NWSL,
// Libertadores). Saves wasted fetches without permanently blocking
// retries in case coverage returns.
const PIN_VERIFY_ERROR_COOLDOWN_MS = 10 * 60 * 1000;
const PIN_VERIFY_ERROR_THRESHOLD = 3;
const _pinVerifyErrorStreak = {}; // comboKey -> { count, cooldownStartAt }

const _pinVerifyWarmStats = {
  cyclesRun: 0,
  cyclesCompletedAt: null,
  lastCycleMs: null,
  totalFetches: 0,
  totalSkippedFresh: 0,
  totalSkippedInactive: 0,
  totalSkippedErrorCooldown: 0,
  totalErrors: 0,
  perCombo: {}, // `${sport}|${market}` -> { fetched, skippedFresh, skippedInactive, errors, lastFetchedAt, lastRfqAt, errorStreak, cooldownUntil }
};

// Called from verifyLineWithPinnacle on every hot-path invocation so the
// warm loop knows which combos are actually serving RFQs.
function _touchPinVerifyCombo(comboKey) {
  _pinVerifyRfqTouch[comboKey] = Date.now();
}

async function _runPinVerifyWarmCycle() {
  const t0 = Date.now();
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return;

  const supported = new Set(config.supportedSports || []);
  const combos = [];
  for (const [ourSport, apiSport] of Object.entries(PINNACLE_SPORT_MAP)) {
    if (!supported.has(ourSport)) continue;
    combos.push({ apiSport, market: 'spreads' });
    combos.push({ apiSport, market: 'totals' });
  }
  // Sequential with inter-request pacing. Effective rate ~8 req/s per
  // warm cycle, well under Odds API's ~100 req/s ceiling.
  for (let i = 0; i < combos.length; i++) {
    if (i > 0) await new Promise(r => setTimeout(r, PIN_VERIFY_WARM_DELAY_MS));
    const { apiSport, market } = combos[i];
    const comboKey = apiSport + '|' + market;
    const stats = _pinVerifyWarmStats.perCombo[comboKey] || {
      fetched: 0, skippedFresh: 0, skippedInactive: 0, errors: 0,
      lastFetchedAt: null, lastRfqAt: null, errorStreak: 0, cooldownUntil: null,
    };
    const now = Date.now();

    // Gate A: error cooldown. If we've seen N consecutive failures, park
    // this combo for PIN_VERIFY_ERROR_COOLDOWN_MS before trying again.
    const err = _pinVerifyErrorStreak[comboKey];
    if (err && err.cooldownStartAt && (now - err.cooldownStartAt) < PIN_VERIFY_ERROR_COOLDOWN_MS) {
      _pinVerifyWarmStats.totalSkippedErrorCooldown++;
      stats.cooldownUntil = new Date(err.cooldownStartAt + PIN_VERIFY_ERROR_COOLDOWN_MS).toISOString();
      _pinVerifyWarmStats.perCombo[comboKey] = stats;
      continue;
    }
    // Cooldown expired or never triggered — clear any stale state.
    if (err && err.cooldownStartAt && (now - err.cooldownStartAt) >= PIN_VERIFY_ERROR_COOLDOWN_MS) {
      delete _pinVerifyErrorStreak[comboKey];
      stats.cooldownUntil = null;
    }

    // Gate B: demand activity. Skip combos that haven't served an RFQ
    // in the activity window. First-time combos get one grace cycle so
    // the cache is populated before any RFQ lands; thereafter they must
    // earn continued warming via RFQ traffic.
    //
    // Always-warm core sports (NBA/MLB/NHL) bypass this gate — we
    // accept the fetch cost to guarantee no cold-verify tails for
    // primary volume drivers. They still get Gate A (error cooldown)
    // and Gate C (skip-if-very-fresh).
    const lastRfq = _pinVerifyRfqTouch[comboKey];
    stats.lastRfqAt = lastRfq ? new Date(lastRfq).toISOString() : null;
    // `apiSport` value happens to equal our internal sport key for the
    // always-warm trio; Pinnacle mapping is identity for those.
    const isAlwaysWarm = _pinVerifyAlwaysWarmSports.has(apiSport);
    const hasEverWarmed = stats.fetched > 0;
    if (!isAlwaysWarm && hasEverWarmed && (!lastRfq || (now - lastRfq) > PIN_VERIFY_ACTIVITY_WINDOW_MS)) {
      stats.skippedInactive++;
      _pinVerifyWarmStats.totalSkippedInactive++;
      _pinVerifyWarmStats.perCombo[comboKey] = stats;
      continue;
    }

    // Gate C: skip if entry was refreshed very recently. With 20s cycle
    // + 30s TTL, refresh when age >= 10s to keep cache always ≥ 10s
    // from expiry when an RFQ hits.
    const cached = _pinVerifyCache[comboKey];
    if (cached && (now - cached.fetchedAt) < 10 * 1000) {
      stats.skippedFresh++;
      _pinVerifyWarmStats.totalSkippedFresh++;
      _pinVerifyWarmStats.perCombo[comboKey] = stats;
      continue;
    }

    try {
      const events = await _fetchPinVerifyEvents(apiSport, market, theOddsApiKey);
      if (events) {
        stats.fetched++;
        stats.lastFetchedAt = new Date().toISOString();
        stats.errorStreak = 0;
        _pinVerifyWarmStats.totalFetches++;
        delete _pinVerifyErrorStreak[comboKey]; // success clears any streak
      } else {
        stats.errors++;
        stats.errorStreak = (stats.errorStreak || 0) + 1;
        _pinVerifyWarmStats.totalErrors++;
        // Track consecutive errors. Trip cooldown on threshold.
        const streak = (_pinVerifyErrorStreak[comboKey] || { count: 0 }).count + 1;
        _pinVerifyErrorStreak[comboKey] = {
          count: streak,
          cooldownStartAt: streak >= PIN_VERIFY_ERROR_THRESHOLD ? Date.now() : null,
        };
        if (streak >= PIN_VERIFY_ERROR_THRESHOLD) {
          stats.cooldownUntil = new Date(Date.now() + PIN_VERIFY_ERROR_COOLDOWN_MS).toISOString();
          log.info('OddsFeed', `Pin verify cooldown ${comboKey} (${streak} consecutive errors)`);
        }
      }
    } catch (e) {
      stats.errors++;
      stats.errorStreak = (stats.errorStreak || 0) + 1;
      _pinVerifyWarmStats.totalErrors++;
    }
    _pinVerifyWarmStats.perCombo[comboKey] = stats;
  }
  _pinVerifyWarmStats.cyclesRun++;
  _pinVerifyWarmStats.lastCycleMs = Date.now() - t0;
  _pinVerifyWarmStats.cyclesCompletedAt = new Date().toISOString();
}

function startPinVerifyWarmLoop() {
  if (_pinVerifyWarmTimer) return;
  // Fire immediate cycle so cache is populated before any RFQ arrives.
  _runPinVerifyWarmCycle().catch(err => {
    log.warn('OddsFeed', `Pin verify initial warm failed: ${err.message}`);
  });
  _pinVerifyWarmTimer = setInterval(() => {
    _runPinVerifyWarmCycle().catch(err => {
      log.warn('OddsFeed', `Pin verify warm loop failed: ${err.message}`);
    });
  }, PIN_VERIFY_WARM_INTERVAL_MS);
  log.info('OddsFeed', `Pin verify warm loop started (every ${PIN_VERIFY_WARM_INTERVAL_MS / 1000}s)`);
}

function getPinVerifyWarmStats() {
  // Snapshot cache state for visibility
  const now = Date.now();
  const cacheEntries = Object.entries(_pinVerifyCache).map(([k, v]) => ({
    comboKey: k,
    ageSec: Math.round((now - v.fetchedAt) / 1000),
    eventCount: (v.events || []).length,
  }));
  return {
    ..._pinVerifyWarmStats,
    intervalMs: PIN_VERIFY_WARM_INTERVAL_MS,
    cacheSize: Object.keys(_pinVerifyCache).length,
    cacheEntries,
  };
}

// Bovada scraper loop. Runs every 2 min — matches the primary odds
// cycle. Per-event cache TTL inside the scraper (10 min) means most
// calls are cheap skip operations. First run at startup populates
// cache before any alt-line RFQs arrive.
let _bovadaLoopTimer = null;
const BOVADA_LOOP_INTERVAL_MS = 2 * 60 * 1000;
function startBovadaAltLoop() {
  if (_bovadaLoopTimer) return;
  // Initial refresh fire-and-forget — errors logged by the scraper
  bovadaAltScraper.refreshAll().catch(err => {
    log.warn('OddsFeed', `Bovada initial refresh failed: ${err.message}`);
  });
  _bovadaLoopTimer = setInterval(() => {
    bovadaAltScraper.refreshAll().catch(err => {
      log.warn('OddsFeed', `Bovada refresh loop failed: ${err.message}`);
    });
  }, BOVADA_LOOP_INTERVAL_MS);
  log.info('OddsFeed', `Bovada alt-line loop started (every ${BOVADA_LOOP_INTERVAL_MS / 1000}s)`);
}

// ---------------------------------------------------------------------------
// Just-in-time (JIT) single-event warm
// ---------------------------------------------------------------------------
// Called by line-manager when a new PX event is registered (either during
// seed or on-demand via resolveUnknownLine). Fires a single-event alt-line
// fetch immediately rather than waiting up to WARM_LOOP_INTERVAL_MS (15s)
// for the periodic sweep to discover it.
//
// Safety rails:
//   - Dedupes against in-flight warms (per-event key) so repeated calls
//     from seed + resolveUnknownLine + rapid reseeds coalesce.
//   - Skips when altLinesCache has a fresh entry (< ALT_LINES_TTL_MS).
//   - Skips events outside the warm window (already started / too far out).
//   - Throttled by a global concurrency cap (JIT_WARM_CONCURRENCY) so a
//     large seed doesn't burst-call resolveOddsApiEventId + fetchAltLines
//     in parallel and 429 the Odds API.
//
// Fire-and-forget contract: callers don't await — they pass through the
// promise (or ignore it) and let the queue drain in background.
const JIT_WARM_CONCURRENCY = 2;
const _jitInFlight = new Map(); // normalizedKey -> Promise
let _jitRunning = 0;
const _jitPending = []; // [{task, resolve, reject}]
const _jitStats = {
  fired: 0, skippedFresh: 0, skippedNoAltSport: 0,
  skippedStarted: 0, skippedTooFar: 0, skippedMissingFields: 0,
  deduped: 0, fetched: 0, noMatch: 0, errors: 0,
  lastFiredAt: null,
};

function _drainJitQueue() {
  while (_jitRunning < JIT_WARM_CONCURRENCY && _jitPending.length > 0) {
    const { task, resolve, reject } = _jitPending.shift();
    _jitRunning++;
    task()
      .then(resolve, reject)
      .finally(() => {
        _jitRunning--;
        _drainJitQueue();
      });
  }
}

function _runQueuedJit(task) {
  return new Promise((resolve, reject) => {
    _jitPending.push({ task, resolve, reject });
    _drainJitQueue();
  });
}

/**
 * Warm alt-line cache for a single event immediately. Idempotent and
 * safely callable from any registration path. Returns a promise the
 * caller may ignore (fire-and-forget).
 *
 * @param {object} args
 * @param {string} args.sport        Odds API sport key (e.g. 'basketball_nba')
 * @param {string} args.homeTeam     Odds API canonical home team
 * @param {string} args.awayTeam     Odds API canonical away team
 * @param {string|null} args.commenceTime ISO-8601 or null
 */
function warmEventAltLinesJIT({ sport, homeTeam, awayTeam, commenceTime }) {
  if (!sport || !homeTeam || !awayTeam) {
    _jitStats.skippedMissingFields++;
    return Promise.resolve({ status: 'skipped_missing_fields' });
  }
  // Same gate as warmAltLines — only sports we actually pre-warm have
  // meaningful alt-line coverage. On-demand sports (soccer niche leagues)
  // also welcome the JIT since they aren't on the periodic sweep.
  if (!sportSupportsAltLines(sport)) {
    _jitStats.skippedNoAltSport++;
    return Promise.resolve({ status: 'skipped_no_alt_sport', sport });
  }
  const now = Date.now();
  const startMs = commenceTime ? new Date(commenceTime).getTime() : null;
  if (startMs && !isNaN(startMs)) {
    if (startMs < now) {
      _jitStats.skippedStarted++;
      return Promise.resolve({ status: 'skipped_started' });
    }
    if (startMs > now + WARM_EVENT_MAX_HOURS_AHEAD * 3600 * 1000) {
      _jitStats.skippedTooFar++;
      return Promise.resolve({ status: 'skipped_too_far' });
    }
  }
  const key = normalizeEventKey(homeTeam, awayTeam);
  const cached = altLinesCache[key];
  if (cached && (now - cached.fetchedAt) < ALT_LINES_TTL_MS) {
    _jitStats.skippedFresh++;
    return Promise.resolve({ status: 'skipped_fresh', key });
  }
  const pending = _jitInFlight.get(key);
  if (pending) {
    _jitStats.deduped++;
    return pending;
  }

  const promise = _runQueuedJit(async () => {
    _jitStats.fired++;
    _jitStats.lastFiredAt = new Date().toISOString();
    try {
      // Pre-check match so stats can distinguish "Odds API doesn't have
      // this event" from "fetch errored."
      const resolved = await resolveOddsApiEventId(sport, homeTeam, awayTeam, commenceTime);
      if (!resolved) {
        _jitStats.noMatch++;
        return { status: 'no_match', key, sport };
      }
      const r = await fetchAltLines(sport, homeTeam, awayTeam, commenceTime);
      if (r) {
        _jitStats.fetched++;
        log.debug('OddsFeed', `JIT warm: ${sport} ${awayTeam} @ ${homeTeam} fetched (${_jitStats.fetched} total)`);
        return { status: 'fetched', key, sport };
      }
      _jitStats.errors++;
      return { status: 'empty', key, sport };
    } catch (err) {
      _jitStats.errors++;
      log.warn('OddsFeed', `JIT warm failed for ${homeTeam} vs ${awayTeam}: ${err.message}`);
      return { status: 'error', key, sport, error: err.message };
    }
  }).finally(() => {
    _jitInFlight.delete(key);
  });

  _jitInFlight.set(key, promise);
  return promise;
}

function getJitWarmStats() {
  return {
    ..._jitStats,
    concurrencyCap: JIT_WARM_CONCURRENCY,
    inFlight: _jitRunning,
    queued: _jitPending.length,
    inFlightKeys: _jitInFlight.size,
  };
}

function getAltLinesWarmStats() {
  const cacheSize = Object.keys(altLinesCache).length;
  // Compute staleness distribution
  const now = Date.now();
  let fresh = 0, stale = 0;
  for (const entry of Object.values(altLinesCache)) {
    if ((now - entry.fetchedAt) < ALT_LINES_TTL_MS) fresh++;
    else stale++;
  }
  return {
    cacheSize,
    fresh,
    stale,
    ttlMinutes: ALT_LINES_TTL_MS / 60000,
    lastWarmBySport: _lastWarmStats || {},
  };
}

// ---------------------------------------------------------------------------
// CACHE LOOKUP
// ---------------------------------------------------------------------------

/**
 * Get fair probability — sync version, uses cached data only.
 * @param {string} targetTime - optional ISO timestamp for time-aware matching
 */
function getFairProb(sport, homeTeam, awayTeam, marketType, selection, line, targetTime) {
  let event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  // Reversed-orientation fallback — some cache entries (notably DK-scraped
  // MMA fights) are stored under (fighterA, fighterB) while the line-manager
  // registered them as (fighterB, fighterA) based on PX's competitor order.
  // If the forward lookup misses, try reversed and flip home/away semantics
  // so the selection still resolves correctly.
  let orientationFlipped = false;
  if (!event) {
    event = getEventMarkets(sport, awayTeam, homeTeam, targetTime);
    if (event) orientationFlipped = true;
  }
  if (!event) {
    // Last-resort fallback: SharpAPI primary cache doesn't have this
    // game (event not yet cached, team-name mismatch, sport gap), but
    // TOA-populated altLinesCache might. Operator caught 2026-04-27:
    // MLB integer totals like O 8 declining as "no fair value" because
    // SharpAPI Hobby tier doesn't return Pinnacle's primary integer 8,
    // and the early `event == null` exit short-circuited the alt path.
    //
    // Only applies to spreads/totals (where alt cache exists) and
    // requires a non-null line (lookup needs the line value).
    if (line != null && (marketType === 'spreads' || marketType === 'totals')) {
      const altProb = getAltLineFairProb(
        normalizeEventKey(homeTeam, awayTeam), marketType, selection, line
      );
      if (altProb != null && altProb > 0 && altProb < 1) return altProb;
    }
    return null;
  }

  // Flip selection when we found the event under reversed orientation.
  // - h2h / moneyline: home↔away
  // - spreads: home↔away + sign of line flips (home -3 ↔ away +3). We
  //   invert `line` here so downstream alt-line matching stays correct.
  // - totals: over/under are team-agnostic, no change
  // - team_totals: home_over↔away_over, home_under↔away_under
  if (orientationFlipped) {
    if (marketType === 'h2h') {
      selection = selection === 'home' ? 'away' : selection === 'away' ? 'home' : selection;
    } else if (marketType === 'spreads') {
      selection = selection === 'home' ? 'away' : selection === 'away' ? 'home' : selection;
      if (line != null) line = -line;
    } else if (marketType === 'team_totals') {
      if (selection && selection.startsWith('home_')) selection = 'away_' + selection.slice(5);
      else if (selection && selection.startsWith('away_')) selection = 'home_' + selection.slice(5);
    }
  }

  const market = event.markets[marketType];
  if (!market) {
    // Primary cache miss for F5 spread/total. Fall through to the alt
    // cache (may have been populated by fetchAltLines). If alt data
    // isn't there either, getFairProbAsync will trigger the per-event
    // fetch and retry — this path just handles warm-cache alt hits.
    if ((marketType === 'spreads_f5' || marketType === 'totals_f5') && line != null) {
      const altKey = normalizeEventKey(homeTeam, awayTeam);
      return getAltLineFairProb(altKey, marketType, selection, line);
    }
    return null;
  }

  if (marketType === 'spreads' || marketType === 'totals' || marketType === 'spreads_f5' || marketType === 'totals_f5') {
    // CRITICAL: spreads/totals (full-game OR F5) MUST have a line value
    // to price correctly. Without a line, we can't distinguish Over 4.5
    // from Over 5.5 — returning the primary fair prob would be
    // catastrophically wrong for alt lines. (Root cause of +377
    // mispricing on full-game Over 4.5 + Under 5.5 parlay, 2026-04-12;
    // and the current F5 totals_f5 5.5 alt-line decline cluster.)
    if (line == null) {
      log.warn('OddsFeed', `getFairProb: null line for ${marketType} ${selection} ${homeTeam} vs ${awayTeam} — declining to avoid primary-line contamination`);
      return null;
    }

    if (market.line != null) {
      const absLine = Math.abs(line);
      const lineDiff = Math.abs(Math.abs(market.line) - absLine);
      if (lineDiff > 0.01) {
        // Strict-mode distance guard: for soccer (and other ondemand-alt
        // sports), decline outright if the alt line is too far from
        // primary. The further out on the tail, the less reliable the
        // de-vigged fair, and correlation to primary weakens — sanity
        // checks may not catch a bad one.
        const maxDist = altMaxLineDistance(sport);
        if (lineDiff > maxDist) {
          log.warn('OddsFeed', `Alt ${marketType} distance guard: |${line} - ${market.line}| = ${lineDiff.toFixed(1)} > max ${maxDist} for ${sport} ${homeTeam} vs ${awayTeam} — declining`);
          return null;
        }
        // Line magnitude doesn't match primary. First try the per-line
        // consensus in market.byLine — populated by buildConsensusTotals
        // for every distinct line across books, so minority lines (e.g.
        // Pinnacle's integer 8 when majority is 8.5) resolve without a
        // network fetch. Applies to totals AND F5/H1 sub-game totals;
        // the supplement that builds totals_f5/totals_h1 also populates
        // byLine. Spreads have signed home_point bucketing via
        // getAltLineFairProb instead.
        if ((marketType === 'totals' || marketType === 'totals_f5' || marketType === 'totals_h1') && market.byLine) {
          const byLineEntry = market.byLine[String(absLine)];
          if (byLineEntry) {
            const sideProb = selection === 'over' ? byLineEntry.over?.fairProb : byLineEntry.under?.fairProb;
            if (sideProb != null && sideProb > 0 && sideProb < 1) return sideProb;
          }
        }
        // For spreads / spreads_f5 / spreads_h1: check the per-line
        // consensus map populated by buildConsensusSpread. Same pattern
        // as the totals byLine fast-path above. Captures the case where
        // different books post different primary lines (Pinnacle line=0
        // pick-em, DK/FD line=±0.5 standard) and we'd otherwise lose
        // the non-modal-line data when collapsing to a single primary.
        if ((marketType === 'spreads' || marketType === 'spreads_f5' || marketType === 'spreads_h1') && market.byLine) {
          const sideKey = selection + '|' + line; // signed-line key
          const byLineEntry = market.byLine[sideKey];
          if (byLineEntry) {
            const sideProb = byLineEntry.fairProb;
            if (sideProb != null && sideProb > 0 && sideProb < 1) return sideProb;
          }
        }
        // Pass the SIGNED line so getAltLineFairProb can route to the correct
        // signed home_point bucket (critical: sign flips on alt spreads).
        const key = normalizeEventKey(homeTeam, awayTeam);
        const altProb = getAltLineFairProb(key, marketType, selection, line);
        if (altProb != null) {
          // Strict-mode sport check: require ≥ 2 books for the alt line.
          // The Odds API soccer alt coverage is thin on Hobby tier; a
          // single-book alt fair is too noisy to trust. This inspects
          // the raw cache entry, which carries books + byBook from
          // ingestion.
          if (isStrictAltSanitySport(sport)) {
            const altEntry = getAltLineCacheEntry(key, marketType, selection, line);
            const bookCount = altEntry ? altEntry.books : 0;
            if (bookCount < 2) {
              log.warn('OddsFeed', `Alt ${marketType} strict-book check: ${sport} ${selection} ${line} has only ${bookCount} book(s) — declining (min 2)`);
              return null;
            }
          }
          // Sanity: for totals far from primary, verify direction makes sense.
          // Over a low total (e.g. 4.5 when primary is 8.5) should be a heavy
          // favorite (fairProb >= 0.60). Under a low total should be an underdog.
          // Vice versa for high totals.  If violated, the alt line data may be
          // corrupted (swapped over/under, wrong point, stale cache).
          const sanityThreshold = altSanityLineDiffThreshold(sport);
          if (marketType === 'totals' && lineDiff >= sanityThreshold) {
            const expectHigh = (selection === 'over' && line < market.line) || (selection === 'under' && line > market.line);
            if (expectHigh && altProb < 0.55) {
              log.warn('OddsFeed', `Alt total sanity FAIL: ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} — expected heavy favorite, got underdog. Declining.`);
              return null;
            }
            // Strict sports also enforce the reverse: over-a-high-total
            // should be an underdog (fairProb < 0.45) when we're clearly
            // out on the tail. Catches swapped-side bugs where the alt
            // cache returns the fair for the OPPOSITE direction.
            if (isStrictAltSanitySport(sport)) {
              const expectLow = (selection === 'over' && line > market.line) || (selection === 'under' && line < market.line);
              if (expectLow && altProb > 0.55) {
                log.warn('OddsFeed', `Alt total strict sanity FAIL: ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} — expected underdog, got favorite. Declining.`);
                return null;
              }
            }
          }
          // Sanity: for alt spreads far from primary, verify direction makes sense.
          // If primary home spread is -5.5 and we're pricing home -0.5 (easier to
          // cover), the fair prob should be HIGHER than the primary. If it's lower,
          // the alt line data may have sign-flipped home/away probs.
          if (marketType === 'spreads' && lineDiff >= 2.0) {
            const primaryProb = selection === 'home' ? market.home?.fairProb : market.away?.fairProb;
            if (primaryProb != null) {
              // "Easier to cover" = smaller absolute handicap for the team
              const absAlt = Math.abs(line);
              const absPrimary = Math.abs(market.line);
              const easierToCover = (selection === 'home')
                ? (line > market.line) // home -0.5 is easier than home -5.5
                : (line < market.line); // away +0.5 is easier than away +5.5
              if (easierToCover && altProb < primaryProb - 0.05) {
                log.warn('OddsFeed', `Alt spread sanity FAIL: ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} < primary=${primaryProb.toFixed(4)} — easier line should have higher prob. Declining.`);
                return null;
              }
              if (!easierToCover && altProb > primaryProb + 0.05) {
                log.warn('OddsFeed', `Alt spread sanity FAIL: ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} > primary=${primaryProb.toFixed(4)} — harder line should have lower prob. Declining.`);
                return null;
              }
            }
          }
          return altProb;
        }
        return null;
      }

      // Magnitude matches — verify spread DIRECTION is correct.
      // For spreads: if selection is 'home', the cached home point should have the
      // same sign as the requested line. E.g., if home is -1.5 (favorite) but
      // request is +1.5, the bettor wants the alt side, not the primary.
      if (marketType === 'spreads' && line !== 0) {
        const cachedPoint = selection === 'home' ? market.home?.point : market.away?.point;
        if (cachedPoint != null && Math.sign(cachedPoint) !== Math.sign(line)) {
          // Direction mismatch — requested +1.5 but team is -1.5 (or vice versa)
          // Treat as alt line, not primary
          log.info('OddsFeed', `Spread direction mismatch: ${selection} cached ${cachedPoint} vs requested ${line} for ${homeTeam} vs ${awayTeam}`);
          const key = normalizeEventKey(homeTeam, awayTeam);
          const altProb = getAltLineFairProb(key, marketType, selection, line);
          if (altProb != null) return altProb;
          return null; // no alt line data — decline
        }
      }
    } else {
      // market.line is null — can't verify if request matches primary.
      // Route to alt lines; if not cached, decline rather than risk
      // returning an unverified primary fair prob.
      log.warn('OddsFeed', `getFairProb: market.line is null for ${marketType}, requested line=${line} — trying alt lines only`);
      const key = normalizeEventKey(homeTeam, awayTeam);
      const altProb = getAltLineFairProb(key, marketType, selection, line);
      if (altProb != null) return altProb;
      return null;
    }
  }

  if (marketType === 'h2h') {
    if (selection === 'home') return market.home?.fairProb || null;
    if (selection === 'away') return market.away?.fairProb || null;
  } else if (marketType === 'spreads') {
    if (selection === 'home') return market.home?.fairProb || null;
    if (selection === 'away') return market.away?.fairProb || null;
  } else if (marketType === 'totals') {
    if (selection === 'over') return market.over?.fairProb || null;
    if (selection === 'under') return market.under?.fairProb || null;
  } else if (marketType === 'team_totals') {
    // Selection is compound: "home_over", "home_under", "away_over", "away_under"
    const parts = selection.split('_');
    const side = parts[0]; // "home" or "away"
    const dir = parts[1];  // "over" or "under"
    const teamData = market[side];
    if (!teamData) return null;
    // Require a line match against the cached primary OR a per-line
    // entry in byLine. Without this, an alt team-total registered by
    // PX (e.g. Lakers Over 114.5) would receive the primary line's
    // fair prob (e.g. Over 115.5) — wrong enough to leak money
    // systematically. Same safeguard rationale as totals/spreads above.
    // Line 0.01 tolerance for float noise.
    if (line == null) {
      log.warn('OddsFeed', `getFairProb: null line for team_totals ${side}_${dir} ${homeTeam} vs ${awayTeam} — declining`);
      return null;
    }
    if (teamData.line != null && Math.abs(teamData.line - line) > 0.01) {
      // Primary line doesn't match — check per-line consensus map. The
      // supplement now fetches alternate_team_totals from TOA and
      // buildConsensusTeamTotals stores each (team, line) consensus
      // under teamData.byLine[lineStr]. Picks up Cavaliers +112 / +112.5
      // alts that PX RFQs even when our primary is +111.5.
      if (teamData.byLine) {
        const altEntry = teamData.byLine[String(line)];
        if (altEntry) {
          if (dir === 'over') return altEntry.over?.fairProb || null;
          if (dir === 'under') return altEntry.under?.fairProb || null;
        }
      }
      return null;
    }
    if (dir === 'over') return teamData.over?.fairProb || null;
    if (dir === 'under') return teamData.under?.fairProb || null;
  } else if (marketType === 'btts') {
    if (selection === 'yes') return market.yes?.fairProb || null;
    if (selection === 'no') return market.no?.fairProb || null;
  } else if (marketType === 'double_chance') {
    // Selection: '1X' (home or draw), 'X2' (draw or away), '12' (home or away)
    if (market[selection]) return market[selection].fairProb || null;
  } else if (marketType === 'h2h_f5' || marketType === 'spreads_f5') {
    if (selection === 'home') return market.home?.fairProb || null;
    if (selection === 'away') return market.away?.fairProb || null;
  } else if (marketType === 'totals_f5') {
    if (selection === 'over') return market.over?.fairProb || null;
    if (selection === 'under') return market.under?.fairProb || null;
  } else if (marketType === 'h2h_h1' || marketType === 'spreads_h1') {
    if (selection === 'home') return market.home?.fairProb || null;
    if (selection === 'away') return market.away?.fairProb || null;
  } else if (marketType === 'totals_h1') {
    if (selection === 'over') return market.over?.fairProb || null;
    if (selection === 'under') return market.under?.fairProb || null;
  }

  return null;
}

/**
 * Get de-vigged consensus fair prob for display (different from pricing fairProb
 * which uses Pinnacle raw). Returns the displayFairProb or falls back to fairProb.
 */
function getDisplayFairProb(sport, homeTeam, awayTeam, marketType, selection, line, targetTime) {
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event) return null;
  const market = event.markets[marketType];
  if (!market) return null;

  // For spreads/totals, require the requested line to match the cached primary.
  // If it doesn't match (e.g. requested -0.5 but cache has -0.25), return null
  // so the dashboard shows a dash rather than the wrong line's fair value.
  // If line is null for spreads/totals, decline — can't verify match.
  if (marketType === 'spreads' || marketType === 'totals') {
    if (line == null || market.line == null) return null;
    if (Math.abs(Math.abs(market.line) - Math.abs(line)) > 0.01) return null;
  }

  if (marketType === 'h2h') {
    if (selection === 'home') return market.home?.displayFairProb || market.home?.fairProb || null;
    if (selection === 'away') return market.away?.displayFairProb || market.away?.fairProb || null;
  } else if (marketType === 'spreads') {
    if (selection === 'home') return market.home?.displayFairProb || market.home?.fairProb || null;
    if (selection === 'away') return market.away?.displayFairProb || market.away?.fairProb || null;
  } else if (marketType === 'totals') {
    if (selection === 'over') return market.over?.displayFairProb || market.over?.fairProb || null;
    if (selection === 'under') return market.under?.displayFairProb || market.under?.fairProb || null;
  } else if (marketType === 'team_totals') {
    const parts = selection.split('_');
    const teamData = market[parts[0]];
    if (!teamData) return null;
    const dir = parts[1];
    // Line-match check parallels getFairProb (line 4619): if the requested
    // line doesn't match the cached primary, fall back to byLine alts or
    // return null. Without this, a leg with line 103.5 received the primary
    // line's displayFairProb (e.g. 113.5) — wrong number on the FAIR column.
    if (line == null) return null;
    if (teamData.line != null && Math.abs(teamData.line - line) > 0.01) {
      if (teamData.byLine) {
        const altEntry = teamData.byLine[String(line)];
        if (altEntry) {
          if (dir === 'over') return altEntry.over?.displayFairProb || altEntry.over?.fairProb || null;
          if (dir === 'under') return altEntry.under?.displayFairProb || altEntry.under?.fairProb || null;
        }
      }
      return null;
    }
    if (dir === 'over') return teamData.over?.displayFairProb || teamData.over?.fairProb || null;
    if (dir === 'under') return teamData.under?.displayFairProb || teamData.under?.fairProb || null;
  } else if (marketType === 'h2h_h1' || marketType === 'spreads_h1') {
    if (selection === 'home') return market.home?.displayFairProb || market.home?.fairProb || null;
    if (selection === 'away') return market.away?.displayFairProb || market.away?.fairProb || null;
  } else if (marketType === 'totals_h1') {
    if (selection === 'over') return market.over?.displayFairProb || market.over?.fairProb || null;
    if (selection === 'under') return market.under?.displayFairProb || market.under?.fairProb || null;
  }
  return null;
}

/**
 * Get Pinnacle's raw American odds for a specific selection.
 * Returns the odds integer or null if Pinnacle data not available.
 */
/**
 * Returns true if the caller-requested line matches the market's primary
 * cached line. When they don't match, the per-book raw odds stored on the
 * primary line are for a DIFFERENT betting product (e.g. Arsenal -1.25 vs
 * Arsenal -1) and must NOT be reported to the caller — doing so corrupts
 * competitor comparisons. Callers should return null in that case.
 *
 * h2h (moneyline) and team_totals have no line — always match.
 */
function lineMatchesPrimary(market, marketType, requestedLine, selection) {
  if (marketType !== 'spreads' && marketType !== 'totals') return true;
  if (requestedLine == null) return false; // null line → can't verify match, route to alt
  if (market.line == null) return false;

  // Magnitude match first
  const magMatch = Math.abs(Math.abs(market.line) - Math.abs(requestedLine)) < 0.01;
  if (!magMatch) return false;

  // For spreads, also verify DIRECTION. Two spreads with the same magnitude
  // but different signs are different markets (e.g. Arsenal -1.5 vs Arsenal +1.5
  // is the same event but two distinct bets — home at point=-1.5 vs home at
  // point=+1.5). The primary cache holds a specific direction; if the RFQ
  // wants the other one, the per-book odds stored on the primary are for the
  // WRONG side and must not be returned. Route to alt-line cache instead.
  if (marketType === 'spreads' && requestedLine !== 0 && selection) {
    const cachedPoint = selection === 'home' ? market.home?.point : market.away?.point;
    if (cachedPoint != null && Math.sign(cachedPoint) !== Math.sign(requestedLine)) {
      return false;
    }
  }
  return true;
}

function getPinnacleOdds(sport, homeTeam, awayTeam, marketType, selection, targetTime, line) {
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event) return null;

  const market = event.markets[marketType];
  if (!market) return null;

  if (marketType === 'team_totals') {
    const parts = selection.split('_');
    const teamData = market[parts[0]];
    if (!teamData || !teamData.pinnacle) return null;
    if (parts[1] === 'over') return teamData.pinnacle.over || null;
    if (parts[1] === 'under') return teamData.pinnacle.under || null;
    return null;
  }

  if (!lineMatchesPrimary(market, marketType, line, selection)) {
    // Primary line doesn't match — try the alt-line per-book cache.
    return getAltLineBookOdds(homeTeam, awayTeam, marketType, selection, line, 'pinnacle');
  }
  if (!market.pinnacle) return null;
  if (marketType === 'h2h' || marketType === 'spreads'
      || marketType === 'h2h_h1' || marketType === 'spreads_h1'
      || marketType === 'h2h_f5' || marketType === 'spreads_f5') {
    if (selection === 'home') return market.pinnacle.home || null;
    if (selection === 'away') return market.pinnacle.away || null;
  } else if (marketType === 'totals' || marketType === 'totals_h1' || marketType === 'totals_f5') {
    if (selection === 'over') return market.pinnacle.over || null;
    if (selection === 'under') return market.pinnacle.under || null;
  }
  return null;
}

/**
 * Derive Draw No Bet (2-way) fair probability from 3-way h2h odds.
 * Removes the draw and renormalizes: DNB_home = P(home) / (P(home) + P(away))
 */
function getDNBFairProb(sport, homeTeam, awayTeam, selection, targetTime) {
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event) return null;

  const market = event.markets['h2h'];
  if (!market || !market.home?.fairProb || !market.away?.fairProb) return null;

  const pHome = market.home.fairProb;
  const pAway = market.away.fairProb;
  const total = pHome + pAway;
  if (total <= 0) return null;

  if (selection === 'home') return pHome / total;
  if (selection === 'away') return pAway / total;
  return null;
}

function getFanDuelOdds(sport, homeTeam, awayTeam, marketType, selection, targetTime, line) {
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event) return null;

  const market = event.markets[marketType];
  if (!market) return null;

  if (marketType === 'team_totals') {
    // Team totals don't store FanDuel separately in current implementation
    return null;
  }

  if (!lineMatchesPrimary(market, marketType, line, selection)) {
    return getAltLineBookOdds(homeTeam, awayTeam, marketType, selection, line, 'fanduel');
  }
  if (!market.fanduel) return null;
  if (marketType === 'h2h' || marketType === 'spreads'
      || marketType === 'h2h_h1' || marketType === 'spreads_h1'
      || marketType === 'h2h_f5' || marketType === 'spreads_f5') {
    if (selection === 'home') return market.fanduel.home || null;
    if (selection === 'away') return market.fanduel.away || null;
  } else if (marketType === 'totals' || marketType === 'totals_h1' || marketType === 'totals_f5') {
    if (selection === 'over') return market.fanduel.over || null;
    if (selection === 'under') return market.fanduel.under || null;
  }
  return null;
}

function getKalshiOdds(sport, homeTeam, awayTeam, marketType, selection, targetTime, line) {
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event) return null;

  const market = event.markets[marketType];
  if (!market) return null;

  if (marketType === 'team_totals') return null;

  if (!lineMatchesPrimary(market, marketType, line, selection)) {
    return getAltLineBookOdds(homeTeam, awayTeam, marketType, selection, line, 'kalshi');
  }
  if (!market.kalshi) return null;
  if (marketType === 'h2h' || marketType === 'spreads'
      || marketType === 'h2h_h1' || marketType === 'spreads_h1'
      || marketType === 'h2h_f5' || marketType === 'spreads_f5') {
    if (selection === 'home') return market.kalshi.home || null;
    if (selection === 'away') return market.kalshi.away || null;
  } else if (marketType === 'totals' || marketType === 'totals_h1' || marketType === 'totals_f5') {
    if (selection === 'over') return market.kalshi.over || null;
    if (selection === 'under') return market.kalshi.under || null;
  }
  return null;
}

function getDraftKingsOdds(sport, homeTeam, awayTeam, marketType, selection, targetTime, line) {
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event) return null;

  const market = event.markets[marketType];
  if (!market) return null;

  if (marketType === 'team_totals') return null;

  if (!lineMatchesPrimary(market, marketType, line, selection)) {
    return getAltLineBookOdds(homeTeam, awayTeam, marketType, selection, line, 'draftkings');
  }
  if (!market.draftkings) return null;
  if (marketType === 'h2h' || marketType === 'spreads'
      || marketType === 'h2h_h1' || marketType === 'spreads_h1'
      || marketType === 'h2h_f5' || marketType === 'spreads_f5') {
    if (selection === 'home') return market.draftkings.home || null;
    if (selection === 'away') return market.draftkings.away || null;
  } else if (marketType === 'totals' || marketType === 'totals_h1' || marketType === 'totals_f5') {
    if (selection === 'over') return market.draftkings.over || null;
    if (selection === 'under') return market.draftkings.under || null;
  }
  return null;
}

/**
 * Verify a spread/total line hasn't moved by spot-checking Pinnacle's current line.
 * Only called when the requested line matches our cached primary (the dangerous case).
 * Returns { ok: true } if line is confirmed, or { ok: false, currentLine } if moved.
 */
// Per-(sport, market) cache for verifyLineWithPinnacle's full events-list fetch.
// Key: `${oddsApiSport}|${market}` (market is "spreads" or "totals").
// Value: { fetchedAt, events } — events is the raw array from The Odds API.
//
// Why this matters: previously every spread/total RFQ made a fresh HTTPS
// call to The Odds API to verify the primary line. That's 20-30ms added to
// decline→price on every such RFQ. With a modest TTL we answer from cache
// instantly for most RFQs; a few per 30s window still pay the network cost.
//
// TTL calibration: line verifications are catching BIG moves (>1 point
// diff). Primary lines rarely move that much in 30 seconds — the stalePriceMinutes
// guard elsewhere catches slower drift. So 30s stale is safe for this check.
//
// If a single request is in flight, concurrent callers wait on its promise
// (inFlight map) — prevents N simultaneous RFQs from all firing duplicate
// fetches at once.
const _pinVerifyCache = {};
const _pinVerifyInFlight = {};
const PIN_VERIFY_TTL_MS = 30 * 1000;

async function _fetchPinVerifyEvents(oddsApiSport, market, theOddsApiKey) {
  const cacheKey = oddsApiSport + '|' + market;
  const cached = _pinVerifyCache[cacheKey];
  if (cached && (Date.now() - cached.fetchedAt) < PIN_VERIFY_TTL_MS) {
    return cached.events;
  }
  // Coalesce concurrent fetches on the same key.
  if (_pinVerifyInFlight[cacheKey]) {
    return _pinVerifyInFlight[cacheKey];
  }
  const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/odds`
    + `?apiKey=${theOddsApiKey}`
    + `&regions=eu`
    + `&markets=${market}`
    + `&bookmakers=pinnacle`
    + `&oddsFormat=american`;
  const promise = (async () => {
    try {
      const resp = await abortableFetch(url);
      if (!resp.ok) return null;
      const events = await resp.json();
      _pinVerifyCache[cacheKey] = { fetchedAt: Date.now(), events };
      return events;
    } catch (err) {
      log.debug('OddsFeed', `Pin verify events fetch failed: ${err.message}`);
      return null;
    } finally {
      delete _pinVerifyInFlight[cacheKey];
    }
  })();
  _pinVerifyInFlight[cacheKey] = promise;
  return promise;
}

async function verifyLineWithPinnacle(sport, homeTeam, awayTeam, marketType, cachedLine) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  const oddsApiSport = PINNACLE_SPORT_MAP[sport] || ODDS_API_FALLBACK[sport]?.oddsApiSport;
  if (!theOddsApiKey || !oddsApiSport) return { ok: true }; // can't verify, allow

  try {
    const market = marketType === 'spreads' ? 'spreads' : 'totals';
    // Mark this combo as RFQ-active so the warm loop keeps it hot.
    // Quiet combos get demoted out of the warm rotation after
    // PIN_VERIFY_ACTIVITY_WINDOW_MS; this call brings them back in.
    if (typeof _touchPinVerifyCombo === 'function') {
      _touchPinVerifyCombo(oddsApiSport + '|' + market);
    }
    const events = await _fetchPinVerifyEvents(oddsApiSport, market, theOddsApiKey);
    if (!events) return { ok: true }; // fetch failed, allow

    // Find matching event
    const key = normalizeEventKey(homeTeam, awayTeam);
    for (const event of events) {
      const eventKey = normalizeEventKey(event.home_team, event.away_team);
      if (eventKey !== key) continue;

      const pinnacle = event.bookmakers?.find(b => b.key === 'pinnacle');
      if (!pinnacle) return { ok: true }; // no Pinnacle data, allow

      const mkt = pinnacle.markets?.find(m => m.key === market);
      if (!mkt || !mkt.outcomes || mkt.outcomes.length < 2) return { ok: true };

      const pinLine = mkt.outcomes[0]?.point;
      if (pinLine == null) return { ok: true };

      const lineDiff = Math.abs(Math.abs(pinLine) - Math.abs(cachedLine));
      if (lineDiff > 1.0) {
        log.warn('OddsFeed', `Spread line moved! Cached: ${cachedLine}, Pinnacle now: ${pinLine} (diff: ${lineDiff}) for ${homeTeam} vs ${awayTeam}`);
        return { ok: false, currentLine: pinLine, cachedLine, diff: lineDiff };
      }
      return { ok: true, currentLine: pinLine };
    }
    return { ok: true }; // event not found on Pinnacle, allow
  } catch (err) {
    log.debug('OddsFeed', `Pinnacle line verify failed: ${err.message}`);
    return { ok: true }; // error, allow
  }
}

/**
 * Get fair probability — async version. Falls back to on-demand alt line fetch.
 */
/**
 * Sync fast-path for alt-line fair-prob lookups. Returns a fair prob if
 * the alt-lines cache has a fresh entry covering (marketType, selection,
 * line) AND all sanity/strict-mode gates pass. Returns null otherwise —
 * callers must fall through to getFairProbAsync, which handles
 * cache-miss refetch, Bovada fallback, and non-spread/total market types.
 *
 * Why this exists: getFairProbAsync always does `await fetchAltLines(...)`
 * even when altLinesCache has the entry. The await resolves in O(1) but
 * the microtask hop + scheduling still costs 5-50ms under load depending
 * on event-loop pressure. Pricing an alt-line leg on a warm cache was
 * measured at 30-60ms (p95 62ms) before this path; the sync version
 * collapses that to sub-1ms. Primary-line legs already had this via
 * getFairProb — this extends the same treatment to alts.
 *
 * Sanity checks mirror getFairProbAsync exactly so behaviour is
 * identical on the success path. Any failing check returns null (not
 * a decline) so the caller can still try the async path for
 * completeness — e.g., sanity fail on stale sync data might pass once
 * the async refetch brings fresh numbers.
 */
// Per-reason counter for sync alt-line hit/miss paths. Lets us diagnose
// which miss class drives the RFQs that still fall through to the async
// path (cache stale vs line_not_cached vs sanity gates). Each call
// increments exactly one counter. `not_applicable` covers market types
// sync can't answer for (h2h, h1 variants, team_totals) — those are
// expected misses that route to the async Bovada fallback, not
// regressions. `last_miss_*` buckets a rolling sample of recent
// non-hit legs for hands-on debugging without burning log volume.
const _altSyncStats = {
  hit: 0,
  not_applicable: 0,
  cache_empty: 0,
  cache_stale: 0,
  distance_guard: 0,
  line_not_cached: 0,
  min_books_gate: 0,
  totals_sanity_fail: 0,
  spreads_sanity_fail: 0,
  lastHitAt: null,
  lastMissAt: null,
  recentMisses: [], // { reason, sport, home, away, marketType, selection, line, at }
};
function _recordAltSyncMiss(reason, ctx) {
  _altSyncStats[reason]++;
  _altSyncStats.lastMissAt = new Date().toISOString();
  // Keep last 20 misses for quick inspection
  _altSyncStats.recentMisses.push({ reason, ...ctx, at: _altSyncStats.lastMissAt });
  if (_altSyncStats.recentMisses.length > 20) _altSyncStats.recentMisses.shift();
}

function getAltLineFairProbSync(sport, homeTeam, awayTeam, marketType, selection, line, targetTime) {
  // Only spread/total (incl. F5) have alt-line caches. Other market
  // types (h1 variants, team_totals) flow through getFairProbAsync to
  // the Bovada fallback and must not short-circuit here.
  const isSpreadOrTotal = marketType === 'spreads' || marketType === 'totals'
                         || marketType === 'spreads_f5' || marketType === 'totals_f5';
  if (!isSpreadOrTotal || line == null) {
    _altSyncStats.not_applicable++;
    return null;
  }

  const ctx = { sport, home: homeTeam, away: awayTeam, marketType, selection, line };
  const key = normalizeEventKey(homeTeam, awayTeam);
  const cached = altLinesCache[key];
  if (!cached) { _recordAltSyncMiss('cache_empty', ctx); return null; }
  const cacheAge = Date.now() - cached.fetchedAt;
  if (cacheAge >= ALT_LINES_TTL_MS) {
    _recordAltSyncMiss('cache_stale', { ...ctx, ageMs: cacheAge });
    return null;
  }
  // Stale-but-usable: use the cached value AND fire a background refresh.
  // Without this, the cached entry would expire in the background and
  // the next sync caller would miss → fall through to async fetch with
  // 30-60ms HTTP latency. Firing the refresh proactively keeps the sync
  // path cache-warm. Gated by 'refreshing' bool so concurrent callers
  // don't dispatch multiple parallel refreshes for the same key.
  if (cacheAge >= ALT_LINES_REFRESH_AHEAD_MS && !cached.refreshing) {
    cached.refreshing = true;
    Promise.resolve().then(() =>
      _doAltLinesFetch(sport, homeTeam, awayTeam, targetTime, key)
        .catch(err => log.warn('OddsFeed', `Alt-lines bg refresh (sync-path) failed for ${key}: ${err.message}`))
        .finally(() => {
          const c = altLinesCache[key];
          if (c) c.refreshing = false;
        })
    );
  }

  // Distance guard (strict-mode) — mirrors async path line-by-line.
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  const primaryMarket = event ? event.markets[marketType] : null;
  if (primaryMarket?.line != null) {
    const lineDiff0 = Math.abs(Math.abs(primaryMarket.line) - Math.abs(line));
    const maxDist = altMaxLineDistance(sport);
    if (lineDiff0 > maxDist) {
      _recordAltSyncMiss('distance_guard', { ...ctx, primary: primaryMarket.line, diff: lineDiff0 });
      return null;
    }
  }

  const altProb = getAltLineFairProb(key, marketType, selection, line);
  if (altProb == null) { _recordAltSyncMiss('line_not_cached', ctx); return null; }

  // Strict-mode min-book gate
  if (isStrictAltSanitySport(sport)) {
    const altEntry = getAltLineCacheEntry(key, marketType, selection, line);
    const bookCount = altEntry ? altEntry.books : 0;
    if (bookCount < 2) {
      _recordAltSyncMiss('min_books_gate', { ...ctx, books: bookCount });
      return null;
    }
  }

  // Directional sanity checks (mirror async path)
  const market = event ? event.markets[marketType] : null;
  const sanityThreshold = altSanityLineDiffThreshold(sport);
  if (marketType === 'totals' && market?.line != null) {
    const lineDiff = Math.abs(Math.abs(market.line) - Math.abs(line));
    if (lineDiff >= sanityThreshold) {
      const expectHigh = (selection === 'over' && line < market.line) || (selection === 'under' && line > market.line);
      if (expectHigh && altProb < 0.55) {
        _recordAltSyncMiss('totals_sanity_fail', { ...ctx, primary: market.line, altProb });
        return null;
      }
      if (isStrictAltSanitySport(sport)) {
        const expectLow = (selection === 'over' && line > market.line) || (selection === 'under' && line < market.line);
        if (expectLow && altProb > 0.55) {
          _recordAltSyncMiss('totals_sanity_fail', { ...ctx, primary: market.line, altProb, strict: true });
          return null;
        }
      }
    }
  }
  if (marketType === 'spreads' && market?.line != null) {
    const lineDiff = Math.abs(Math.abs(market.line) - Math.abs(line));
    if (lineDiff >= 2.0) {
      const primaryProb = selection === 'home' ? market.home?.fairProb : market.away?.fairProb;
      if (primaryProb != null) {
        const easierToCover = (selection === 'home')
          ? (line > market.line)
          : (line < market.line);
        if (easierToCover && altProb < primaryProb - 0.05) {
          _recordAltSyncMiss('spreads_sanity_fail', { ...ctx, primary: market.line, altProb, primaryProb });
          return null;
        }
        if (!easierToCover && altProb > primaryProb + 0.05) {
          _recordAltSyncMiss('spreads_sanity_fail', { ...ctx, primary: market.line, altProb, primaryProb });
          return null;
        }
      }
    }
  }

  _altSyncStats.hit++;
  _altSyncStats.lastHitAt = new Date().toISOString();
  return altProb;
}

function getAltSyncStats() {
  const totalMisses = _altSyncStats.cache_empty + _altSyncStats.cache_stale
    + _altSyncStats.distance_guard + _altSyncStats.line_not_cached
    + _altSyncStats.min_books_gate + _altSyncStats.totals_sanity_fail
    + _altSyncStats.spreads_sanity_fail;
  const totalCalls = _altSyncStats.hit + _altSyncStats.not_applicable + totalMisses;
  return {
    ..._altSyncStats,
    totalCalls,
    totalMisses,
    hitRateOfApplicable: (_altSyncStats.hit + totalMisses) > 0
      ? _altSyncStats.hit / (_altSyncStats.hit + totalMisses)
      : null,
  };
}

async function getFairProbAsync(sport, homeTeam, awayTeam, marketType, selection, line, targetTime) {
  // Try sync first
  const syncResult = getFairProb(sport, homeTeam, awayTeam, marketType, selection, line, targetTime);
  if (syncResult != null) return syncResult;

  // If it's a spread/total (full-game OR F5) with a line mismatch, try
  // fetching alt lines. For F5, the per-event fetchAltLines call now
  // includes alternate_spreads_1st_5_innings + alternate_totals_1st_5_innings
  // so RFQs for non-primary F5 lines (e.g. O 5.5 when DK primary is 4.5)
  // can still be priced. NOTE: earlier iteration added a 150ms timeout
  // here which regressed p95/p99 and caused +308 price failures because
  // the warm cycle wasn't effectively populating the cache. Reverted to
  // unconditional await until warming is debugged / cache hit rate is
  // high enough for a timeout to be safe.
  const isSpreadOrTotal = marketType === 'spreads' || marketType === 'totals'
                         || marketType === 'spreads_f5' || marketType === 'totals_f5';
  if (isSpreadOrTotal && line != null) {
    const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
    // For F5, an event with NO primary F5 market can still have alts —
    // SharpAPI may skip F5 but the Odds API alt endpoint carries them.
    // So we proceed to fetchAltLines even when the event has no F5
    // primary cached, as long as we have an event record at all.
    if (event || marketType === 'spreads_f5' || marketType === 'totals_f5') {
      // Strict-mode distance guard BEFORE network fetch, so we don't
      // burn an API call on an out-of-range line we'd decline anyway.
      // Only applies when we have the primary market cached (need
      // market.line to compute distance). F5 fallthrough skips this
      // since market can be null.
      const primaryMarket = event ? event.markets[marketType] : null;
      if (primaryMarket?.line != null) {
        const lineDiff0 = Math.abs(Math.abs(primaryMarket.line) - Math.abs(line));
        const maxDist = altMaxLineDistance(sport);
        if (lineDiff0 > maxDist) {
          log.warn('OddsFeed', `Alt ${marketType} distance guard (async): |${line} - ${primaryMarket.line}| = ${lineDiff0.toFixed(1)} > max ${maxDist} for ${sport} — declining`);
          return null;
        }
      }
      await fetchAltLines(sport, homeTeam, awayTeam, targetTime);
      const key = normalizeEventKey(homeTeam, awayTeam);
      // Pass SIGNED line (not abs) so alt-line lookup routes to the correct
      // signed home_point bucket.
      const altProb = getAltLineFairProb(key, marketType, selection, line);
      if (altProb != null) {
        // Strict-mode min-book gate: require ≥ 2 books for sports
        // with thin alt coverage (soccer). Single-book alt fair is
        // too noisy to quote on.
        if (isStrictAltSanitySport(sport)) {
          const altEntry = getAltLineCacheEntry(key, marketType, selection, line);
          const bookCount = altEntry ? altEntry.books : 0;
          if (bookCount < 2) {
            log.warn('OddsFeed', `Alt ${marketType} strict-book check (async): ${sport} ${selection} ${line} has only ${bookCount} book(s) — declining (min 2)`);
            return null;
          }
        }
        // Same directional sanity checks as getFairProb (see comments there).
        // `event` may be null for F5 fallthrough (no primary market cached);
        // skip the sanity block in that case — F5 line ranges are narrow
        // and rarely trigger the lineDiff >= 2.0 guard anyway.
        const market = event ? event.markets[marketType] : null;
        const sanityThreshold = altSanityLineDiffThreshold(sport);
        if (marketType === 'totals' && market?.line != null) {
          const lineDiff = Math.abs(Math.abs(market.line) - Math.abs(line));
          if (lineDiff >= sanityThreshold) {
            const expectHigh = (selection === 'over' && line < market.line) || (selection === 'under' && line > market.line);
            if (expectHigh && altProb < 0.55) {
              log.warn('OddsFeed', `Alt total sanity FAIL (async): ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} — declining`);
              return null;
            }
            // Strict-mode reverse sanity: expect-low directions should
            // actually be underdogs. Catches swapped-side bugs.
            if (isStrictAltSanitySport(sport)) {
              const expectLow = (selection === 'over' && line > market.line) || (selection === 'under' && line < market.line);
              if (expectLow && altProb > 0.55) {
                log.warn('OddsFeed', `Alt total strict sanity FAIL (async): ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} — expected underdog, got favorite. Declining.`);
                return null;
              }
            }
          }
        }
        if (marketType === 'spreads' && market?.line != null) {
          const lineDiff = Math.abs(Math.abs(market.line) - Math.abs(line));
          if (lineDiff >= 2.0) {
            const primaryProb = selection === 'home' ? market.home?.fairProb : market.away?.fairProb;
            if (primaryProb != null) {
              const easierToCover = (selection === 'home')
                ? (line > market.line)
                : (line < market.line);
              if (easierToCover && altProb < primaryProb - 0.05) {
                log.warn('OddsFeed', `Alt spread sanity FAIL (async): ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} < primary=${primaryProb.toFixed(4)} — declining`);
                return null;
              }
              if (!easierToCover && altProb > primaryProb + 0.05) {
                log.warn('OddsFeed', `Alt spread sanity FAIL (async): ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} > primary=${primaryProb.toFixed(4)} — declining`);
                return null;
              }
            }
          }
        }
      }
      return altProb;
    }
  }

  // ---- BOVADA FALLBACK ----
  // Last resort when The Odds API and SharpAPI caches don't cover the
  // RFQ leg. Targets markets Odds API can't serve on its per-event
  // endpoint (verified 422 INVALID_MARKET for alternate_spreads_h1,
  // alternate_totals_h1, alternate_team_totals). Bovada exposes all
  // of these via its public coupon API; scraper maintains a cache
  // refreshed every 2 min.
  //
  // Only consulted after every other cache/fetch path has returned
  // null. Fail-closed: cache miss / stale returns null, which cascades
  // up to a decline at pricer — never a mispriced quote.
  try {
    const bovadaQuery = mapMarketTypeToBovada(marketType, selection, homeTeam, awayTeam);
    if (bovadaQuery) {
      const fair = bovadaAltScraper.lookupFairProb({
        sport, homeTeam, awayTeam,
        period: bovadaQuery.period,
        marketType: bovadaQuery.marketType,
        selection: bovadaQuery.selection,
        line: bovadaQuery.line != null ? bovadaQuery.line : line,
        teamName: bovadaQuery.teamName,
      });
      if (fair != null) {
        log.info('OddsFeed', `Bovada fallback hit: ${sport} ${marketType}/${selection}/line=${line} -> ${(fair*100).toFixed(2)}%`);
        return fair;
      }
    }
  } catch (err) {
    log.warn('OddsFeed', `Bovada fallback error: ${err.message}`);
  }

  return null;
}

/**
 * Translate our internal marketType + selection to the shape
 * bovadaAltScraper.lookupFairProb expects. Returns null for market
 * types Bovada doesn't cover (anything not h1/p1/p2/p3/f5/i1 period
 * and not team_total).
 */
function mapMarketTypeToBovada(marketType, selection, homeTeam, awayTeam) {
  // Full-game (period='game')
  if (marketType === 'h2h')     return { period: 'game', marketType: 'h2h',    selection };
  if (marketType === 'spreads') return { period: 'game', marketType: 'spread', selection };
  if (marketType === 'totals')  return { period: 'game', marketType: 'total',  selection };

  // NBA First Half
  if (marketType === 'h2h_h1')     return { period: 'h1', marketType: 'h2h',    selection };
  if (marketType === 'spreads_h1') return { period: 'h1', marketType: 'spread', selection };
  if (marketType === 'totals_h1')  return { period: 'h1', marketType: 'total',  selection };

  // MLB First 5 Innings
  if (marketType === 'h2h_f5')     return { period: 'f5', marketType: 'h2h',    selection };
  if (marketType === 'spreads_f5') return { period: 'f5', marketType: 'spread', selection };
  if (marketType === 'totals_f5')  return { period: 'f5', marketType: 'total',  selection };

  // team_totals: selection is compound 'home_over' / 'away_under' etc.
  // Decompose into (side, direction) and map side→teamName.
  if (marketType === 'team_totals') {
    const parts = (selection || '').split('_');
    if (parts.length !== 2) return null;
    const [side, direction] = parts;
    if (direction !== 'over' && direction !== 'under') return null;
    const teamName = side === 'home' ? homeTeam : side === 'away' ? awayTeam : null;
    if (!teamName) return null;
    return {
      period: 'game',
      marketType: 'team_total',
      selection: direction,
      teamName,
    };
  }

  // Not a market type Bovada covers
  return null;
}

/**
 * Compute the signed home_point for a spread leg given the bettor's
 * team-perspective line and selection.
 *
 *   Leg: "Arsenal -1.5" (home favored)         → selection=home, line=-1.5 → home_point = -1.5
 *   Leg: "Bournemouth +1.5" (away getting 1.5) → selection=away, line=+1.5 → home_point = -1.5
 *   Leg: "Arsenal +1.5" (home getting 1.5)     → selection=home, line=+1.5 → home_point = +1.5
 *   Leg: "Bournemouth -1.5" (away by 2+)       → selection=away, line=-1.5 → home_point = +1.5
 *
 * The first two legs are opposite sides of the same market and share home_point=-1.5.
 * The last two are opposite sides of a different market at home_point=+1.5. Keying
 * altSpreads by signed home_point keeps these two bets strictly separated.
 */
function spreadHomePoint(line, selection) {
  if (line == null) return null;
  if (selection === 'home') return line;
  if (selection === 'away') return -line;
  return null;
}

/**
 * Look up the raw alt-line cache entry for a (marketType, line) pair.
 * Returns { home|away|over|under (fair), books, byBook, ... } or null
 * when no entry exists. Used by the strict-mode book-count gate so
 * we can reject single-book alts before pricing on them.
 */
function getAltLineCacheEntry(eventKey, marketType, selection, line) {
  const alt = altLinesCache[eventKey];
  if (!alt) return null;
  const isF5 = marketType === 'spreads_f5' || marketType === 'totals_f5';
  const isH1 = marketType === 'spreads_h1' || marketType === 'totals_h1';
  if (marketType === 'spreads' || marketType === 'spreads_f5' || marketType === 'spreads_h1') {
    const homePoint = spreadHomePoint(line, selection);
    if (homePoint == null) return null;
    const bucket = isH1 ? alt.altSpreadsH1 : (isF5 ? alt.altSpreadsF5 : alt.altSpreads);
    return bucket?.[String(homePoint)] || null;
  }
  if (marketType === 'totals' || marketType === 'totals_f5' || marketType === 'totals_h1') {
    const bucket = isH1 ? alt.altTotalsH1 : (isF5 ? alt.altTotalsF5 : alt.altTotals);
    return bucket?.[Math.abs(line)] || null;
  }
  return null;
}

/**
 * Look up a fair prob from the alt lines cache.
 * For spreads, `line` MUST be the signed team-perspective line (not abs) so
 * we can route to the correct signed home_point bucket.
 */
function getAltLineFairProb(eventKey, marketType, selection, line) {
  const alt = altLinesCache[eventKey];
  if (!alt) {
    log.debug('AltLine', `MISS cache: ${eventKey} ${marketType} ${selection} line=${line} — no alt cache entry`);
    return null;
  }

  // Route F5 alt markets to altSpreadsF5 / altTotalsF5 buckets (MLB only).
  // Route H1 alt markets to altSpreadsH1 / altTotalsH1 buckets (NBA only).
  const isF5 = marketType === 'spreads_f5' || marketType === 'totals_f5';
  const isH1 = marketType === 'spreads_h1' || marketType === 'totals_h1';
  if (marketType === 'spreads' || marketType === 'spreads_f5' || marketType === 'spreads_h1') {
    const homePoint = spreadHomePoint(line, selection);
    if (homePoint == null) {
      log.debug('AltLine', `MISS homePoint null: ${eventKey} ${selection} line=${line}`);
      return null;
    }
    const lineKey = String(homePoint);
    const bucket = isH1 ? alt.altSpreadsH1 : (isF5 ? alt.altSpreadsF5 : alt.altSpreads);
    const lineData = bucket?.[lineKey];
    if (!lineData) {
      const availableKeys = Object.keys(bucket || {}).slice(0, 10).join(', ');
      log.debug('AltLine', `MISS ${marketType}: ${eventKey} ${selection} line=${line} homePoint=${lineKey} — not in cache. Available: [${availableKeys}]`);
      return null;
    }
    const fairProb = selection === 'home' ? (lineData.home || null) : (selection === 'away' ? (lineData.away || null) : null);
    log.debug('AltLine', `HIT ${marketType}: ${eventKey} ${selection} line=${line} homePoint=${lineKey} fair=${fairProb?.toFixed(4) ?? 'null'} books=${lineData.books}`);
    return fairProb;
  } else if (marketType === 'totals' || marketType === 'totals_f5' || marketType === 'totals_h1') {
    const bucket = isH1 ? alt.altTotalsH1 : (isF5 ? alt.altTotalsF5 : alt.altTotals);
    const lineData = bucket?.[Math.abs(line)];
    if (!lineData) {
      const availableKeys = Object.keys(bucket || {}).slice(0, 10).join(', ');
      log.debug('AltLine', `MISS ${marketType}: ${eventKey} ${selection} line=${line} — not in cache. Available: [${availableKeys}]`);
      return null;
    }
    const fairProb = selection === 'over' ? (lineData.over || null) : (selection === 'under' ? (lineData.under || null) : null);
    log.debug('AltLine', `HIT ${marketType}: ${eventKey} ${selection} line=${line} fair=${fairProb?.toFixed(4) ?? 'null'} books=${lineData.books}`);
    return fairProb;
  }

  return null;
}

/**
 * Tennis totals fallback: when the requested line isn't cached as an
 * exact match, try to recover a fair prob from nearby cached lines.
 *
 * Two recovery modes (in priority order):
 *   1. INTERPOLATE — bracketing cached lines exist within ±1.0 on both
 *      sides. Linearly interpolates fair prob between them. No vig bump
 *      because the math is sound (small offsets on a near-linear region).
 *   2. SNAP — only one neighbor is available, within ±0.5. Use that
 *      line's fair prob and signal a 3pp vig bump to compensate for
 *      directional drift (true U 23.5 fair > U 23.0 fair, etc.).
 *
 * Returns { fairProb, vigBump } on success, null otherwise. Tennis-only;
 * called by pricer.js as a last-resort fallback after the standard
 * primary + alt-line + byLine paths all return null.
 *
 * Why tennis only: book coverage on tennis totals is unusually sparse
 * (Pinnacle often posts integer totals only to avoid pushes; DK/FD post
 * sporadic half-points). PX is more generous with line offerings than
 * the underlying market is. Other sports have denser book coverage and
 * don't benefit from this approximation.
 */
function getTennisTotalsFallback(homeTeam, awayTeam, selection, line) {
  if (line == null || !Number.isFinite(line)) return null;
  if (selection !== 'over' && selection !== 'under') return null;
  const eventKey = normalizeEventKey(homeTeam, awayTeam);
  const alt = altLinesCache[eventKey];
  if (!alt || !alt.altTotals) return null;
  const requested = Math.abs(line);
  // Build sorted list of (lineValue, fair) pairs for this selection.
  const points = [];
  for (const [lk, ld] of Object.entries(alt.altTotals)) {
    const lv = parseFloat(lk);
    if (!Number.isFinite(lv)) continue;
    const fair = selection === 'over' ? ld.over : ld.under;
    if (fair == null || fair <= 0 || fair >= 1) continue;
    points.push({ line: lv, fair });
  }
  if (points.length === 0) return null;
  points.sort((a, b) => a.line - b.line);

  // Bracket: largest cached line below requested, smallest above.
  let lower = null, upper = null;
  for (const p of points) {
    if (p.line < requested) {
      if (!lower || p.line > lower.line) lower = p;
    } else if (p.line > requested) {
      if (!upper || p.line < upper.line) upper = p;
    } else {
      // Exact match — caller's fast path should have hit, but be safe.
      return { fairProb: p.fair, vigBump: 0 };
    }
  }

  // INTERPOLATE: both neighbors within ±1.0 of requested.
  if (lower && upper && (requested - lower.line) <= 1.0 && (upper.line - requested) <= 1.0) {
    const t = (requested - lower.line) / (upper.line - lower.line);
    const fair = lower.fair + t * (upper.fair - lower.fair);
    if (fair > 0 && fair < 1) {
      log.info('OddsFeed', `Tennis totals INTERP: ${selection} ${requested} ← ${lower.line}(${lower.fair.toFixed(4)})↔${upper.line}(${upper.fair.toFixed(4)}) → ${fair.toFixed(4)}`);
      return { fairProb: fair, vigBump: 0 };
    }
  }

  // SNAP: only one neighbor within ±0.5; bump vig to absorb the gap.
  let snap = null;
  if (lower && (requested - lower.line) <= 0.5) snap = lower;
  if (upper && (upper.line - requested) <= 0.5) {
    if (!snap || (upper.line - requested) < (requested - snap.line)) snap = upper;
  }
  if (snap) {
    log.info('OddsFeed', `Tennis totals SNAP: ${selection} ${requested} ← ${snap.line}(${snap.fair.toFixed(4)}) +3% vig bump`);
    return { fairProb: snap.fair, vigBump: 0.03 };
  }

  return null;
}

/**
 * Look up a specific book's raw American odds for a cached alt line.
 * Returns null if the alt line isn't cached, the book didn't post it,
 * or the requested selection wasn't covered. Used by getPinnacleOdds
 * and siblings to supply accurate competitor comparison values when
 * the PX RFQ line differs from the primary cached line.
 *
 * For spreads, `line` MUST be the signed team-perspective line.
 */
function getAltLineBookOdds(homeTeam, awayTeam, marketType, selection, line, book) {
  if (!homeTeam || !awayTeam || line == null || !book) return null;
  const eventKey = normalizeEventKey(homeTeam, awayTeam);
  const alt = altLinesCache[eventKey];
  if (!alt) return null;

  let lineData;
  if (marketType === 'spreads' || marketType === 'spreads_h1') {
    const homePoint = spreadHomePoint(line, selection);
    if (homePoint == null) return null;
    const bucket = marketType === 'spreads_h1' ? alt.altSpreadsH1 : alt.altSpreads;
    lineData = bucket?.[String(homePoint)];
  } else if (marketType === 'totals' || marketType === 'totals_h1') {
    const bucket = marketType === 'totals_h1' ? alt.altTotalsH1 : alt.altTotals;
    lineData = bucket?.[Math.abs(line)];
  } else {
    return null;
  }

  if (!lineData || !lineData.byBook) return null;
  const bookOdds = lineData.byBook[book];
  if (!bookOdds) return null;

  if (marketType === 'spreads' || marketType === 'spreads_h1') {
    if (selection === 'home') return bookOdds.home != null ? bookOdds.home : null;
    if (selection === 'away') return bookOdds.away != null ? bookOdds.away : null;
  } else if (marketType === 'totals' || marketType === 'totals_h1') {
    if (selection === 'over') return bookOdds.over != null ? bookOdds.over : null;
    if (selection === 'under') return bookOdds.under != null ? bookOdds.under : null;
  }
  return null;
}

/**
 * Get event markets, optionally matching by time for back-to-back/doubleheaders.
 * @param {string} targetTime - ISO timestamp to match closest event (optional)
 */
function getLiveEventMarkets(sport, homeTeam, awayTeam, targetTime) {
  const sportCache = liveOddsCache[sport];
  if (!sportCache || !sportCache.events) return null;
  // Primary: exact pair match.
  const key = normalizeEventKey(homeTeam, awayTeam);
  let events = sportCache.events[key];
  // Fallback 1: flipped orientation (live feed may have stored
  // as away@home while caller passes home/away).
  if (!events || events.length === 0) {
    const flipped = normalizeEventKey(awayTeam, homeTeam);
    events = sportCache.events[flipped];
  }
  // Fallback 2: fuzzy match across all cached events in this sport.
  // Handles abbreviation mismatches (e.g. caller "Oakland Athletics" vs
  // live-cache "Athletics"). Matches on last-word equality which is
  // sufficient for our single-sport context.
  if (!events || events.length === 0) {
    const hNorm = normalizeTeamName(homeTeam);
    const aNorm = normalizeTeamName(awayTeam);
    const hLast = (hNorm.split(' ').pop() || '').toLowerCase();
    const aLast = (aNorm.split(' ').pop() || '').toLowerCase();
    for (const [k, list] of Object.entries(sportCache.events)) {
      for (const ev of (list || [])) {
        const ehLast = (normalizeTeamName(ev.homeTeam || '').split(' ').pop() || '').toLowerCase();
        const eaLast = (normalizeTeamName(ev.awayTeam || '').split(' ').pop() || '').toLowerCase();
        if ((ehLast === hLast && eaLast === aLast)
            || (ehLast === aLast && eaLast === hLast)) {
          events = [ev];
          break;
        }
      }
      if (events && events.length > 0) break;
    }
  }
  if (!events || events.length === 0) return null;
  if (events.length === 1 || !targetTime) return events[0];
  const targetMs = new Date(targetTime).getTime();
  if (isNaN(targetMs)) return events[0];
  let closest = events[0];
  let closestDiff = Infinity;
  for (const ev of events) {
    const evMs = new Date(ev.commenceTime).getTime();
    if (isNaN(evMs)) continue;
    const diff = Math.abs(evMs - targetMs);
    if (diff < closestDiff) { closestDiff = diff; closest = ev; }
  }
  return closest;
}

/**
 * Get LIVE fair prob from liveOddsCache. Returns null if no live data available
 * (caller should fall back to pre-game fair prob).
 */
function getLiveFairProb(sport, homeTeam, awayTeam, marketType, selection, line, targetTime) {
  // Accept both Odds-API naming ('h2h' / 'spreads' / 'totals') and PX
  // naming ('moneyline' / 'spread' / 'total'). refreshLiveOdds pulls
  // from leg.market which uses PX names; other callers use Odds-API
  // names. Translate to the internal h2h/spreads/totals scheme.
  if (marketType === 'moneyline') marketType = 'h2h';
  else if (marketType === 'spread') marketType = 'spreads';
  else if (marketType === 'total') marketType = 'totals';
  const event = getLiveEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event || !event.markets) return null;
  // Detect orientation flip. If the found event's home/away are swapped
  // vs. our caller's args, swap the selection for home/away markets.
  let sel = selection;
  let lookupLine = line;
  const evHomeLast = (normalizeTeamName(event.homeTeam || '').split(' ').pop() || '').toLowerCase();
  const callerHomeLast = (normalizeTeamName(homeTeam || '').split(' ').pop() || '').toLowerCase();
  const flipped = evHomeLast && callerHomeLast && evHomeLast !== callerHomeLast;
  if (flipped) {
    if (marketType === 'h2h' || marketType === 'spreads') {
      sel = selection === 'home' ? 'away' : selection === 'away' ? 'home' : selection;
      if (marketType === 'spreads' && lookupLine != null) lookupLine = -lookupLine;
    }
  }

  const m = event.markets;
  if (marketType === 'h2h' && m.h2h) {
    const pick = sel === 'home' ? m.h2h.home : m.h2h.away;
    return pick && pick.fairProb ? pick.fairProb : null;
  }
  if (marketType === 'spreads' && m.spreads) {
    // Exact-line match first; fall back to the current primary line if the
    // leg's line isn't present. Mirrors the totals behavior below — in-play
    // odds sources typically publish only the current spread, so mid-game
    // the original pre-game line (e.g. -9 when live is -14) isn't there.
    // The primary's fair prob is still more accurate than the stale pre-game
    // one for exposure tracking purposes.
    let group = lookupLine != null ? m.spreads[Math.abs(lookupLine)] : null;
    if (!group && m.spreads._primary != null) group = m.spreads[m.spreads._primary];
    if (!group) return null;
    const pick = sel === 'home' ? group.home : group.away;
    return pick && pick.fairProb ? pick.fairProb : null;
  }
  if (marketType === 'totals' && m.totals) {
    // DK live totals may not have the exact line we registered. Pick
    // the primary (current line) when caller's line is missing from
    // the live cache — still more accurate than pre-game.
    let group = m.totals[line];
    if (!group && m.totals._primary != null) group = m.totals[m.totals._primary];
    if (!group) return null;
    const pick = selection === 'over' ? group.over : group.under;
    return pick && pick.fairProb ? pick.fairProb : null;
  }
  return null;
}

function getLiveCacheStatus() {
  const status = {};
  for (const [sport, cache] of Object.entries(liveOddsCache)) {
    const totalEvents = Object.values(cache.events).reduce((s, arr) => s + arr.length, 0);
    status[sport] = {
      eventCount: totalEvents,
      ageMinutes: Math.round((Date.now() - cache.fetchedAt) / (1000 * 60) * 10) / 10,
    };
  }
  return status;
}

/**
 * Golf matchup lookup. DataGolf publishes BOTH round_matchups (R1/R2/R3/R4
 * specific) and tournament_matchups (full 72-hole head-to-heads) — often
 * for the same two players. These have materially different fair probs,
 * so we cannot price a round RFQ against tournament odds or vice versa.
 *
 * Each cache entry is tagged with matchupType ('round' | 'tournament') and,
 * for rounds, roundNum (1-4). Caller passes the desired roundNum (null =>
 * tournament) and we filter the array to the single matching entry.
 */
function getGolfMatchupEvent(homeTeam, awayTeam, roundNum) {
  const cache = oddsCache['golf_matchups'];
  if (!cache || !cache.events) return null;
  const key = normalizeEventKey(homeTeam, awayTeam);
  const entry = cache.events[key];
  if (!entry) return null;
  const events = Array.isArray(entry) ? entry : [entry];
  if (events.length === 0) return null;
  const isTournament = roundNum == null;
  // Prefer an entry whose matchupType + roundNum match the request.
  const match = events.find(e => {
    if (isTournament) return e.matchupType === 'tournament' || e.roundNum == null;
    return e.matchupType === 'round' && e.roundNum === roundNum;
  });
  return match || null;
}

// Markets attached by per-event TOA supplements (supplementMlbF5Markets,
// supplementNbaH1Markets, supplementTeamTotals). These don't conflict
// across sibling cache entries because they're MARKET TYPES not present
// on the primary feed. When a sport's cache holds multiple entries for
// the same matchup — either same-key siblings (back-to-backs, generic-
// time vs real-time entries) OR reverse-key siblings (SharpAPI feed
// stores home/away reversed from TOA, common for NBA/NHL/MLB) — the
// supplement may write these markets to a sibling entry that PX doesn't
// match against, causing /lines/detail to show null fair-prob even
// though the data is in cache one entry away. Union them across all
// siblings so the consumer sees them regardless of which entry the
// closest-by-time + correct-orientation lookup picks.
const _MERGEABLE_SUPP_MARKETS = [
  'h2h_h1', 'spreads_h1', 'totals_h1',          // NBA / NCAAB H1
  'h2h_f5', 'spreads_f5', 'totals_f5',          // MLB F5
  'team_totals',                                 // NBA / MLB / NHL
];

function getEventMarkets(sport, homeTeam, awayTeam, targetTime) {
  const sportCache = oddsCache[sport];
  if (!sportCache) return null;
  const key = normalizeEventKey(homeTeam, awayTeam);
  const reverseKey = normalizeEventKey(awayTeam, homeTeam);
  const fwdEntry = sportCache.events[key];
  const revEntry = (reverseKey !== key) ? sportCache.events[reverseKey] : null;
  const fwdEvents = fwdEntry ? (Array.isArray(fwdEntry) ? fwdEntry : [fwdEntry]) : [];
  const revEvents = revEntry ? (Array.isArray(revEntry) ? revEntry : [revEntry]) : [];
  if (fwdEvents.length === 0 && revEvents.length === 0) return null;

  // Cross-orientation closest-by-time selection. SharpAPI's MLB feed
  // periodically stores home/away reversed vs PX/TOA on the same matchup.
  // Without considering both buckets when picking `closest`, a line
  // registered with PX's orientation that has no forward-bucket match for
  // the right time will silently fall back to whatever stale or future
  // event happens to be in the forward bucket. Verified 2026-05-02 ATL @
  // COL: forward bucket (COL|ATL) only contained Saturday's afternoon
  // game; tonight's game was in the reverse bucket (ATL|COL), and every
  // RFQ on tonight's game was priced against tomorrow's data.
  //
  // Each candidate is tagged with `flipped: true` when sourced from the
  // reverse bucket so the final market block can be flipped back to the
  // caller's orientation before returning.
  const candidates = [
    ...fwdEvents.map(ev => ({ ev, flipped: false })),
    ...revEvents.map(ev => ({ ev, flipped: true })),
  ];

  let closestC = candidates[0];
  if (candidates.length > 1 && targetTime) {
    const targetMs = new Date(targetTime).getTime();
    if (!isNaN(targetMs)) {
      let closestDiff = Infinity;
      for (const c of candidates) {
        const evMs = new Date(c.ev.commenceTime).getTime();
        if (isNaN(evMs)) continue;
        const diff = Math.abs(evMs - targetMs);
        if (diff < closestDiff) { closestDiff = diff; closestC = c; }
      }
    }
  }

  const closest = closestC.ev;
  const flippedBucket = closestC.flipped;
  const sameBucket = flippedBucket ? revEvents : fwdEvents;
  const oppositeBucket = flippedBucket ? fwdEvents : revEvents;

  // Same-bucket sibling merge (no orientation flip — siblings share the
  // chosen bucket's orientation).
  let merged = _mergeSameKeySiblings(sameBucket, closest);

  // Opposite-bucket supplement merge (orientation flipped). These markets
  // are in the OPPOSITE orientation from `closest`, so flip them once now
  // to align with closest. If we end up flipping the entire result below
  // (because closest came from the reverse bucket), they get flipped a
  // second time — net zero, returning to their cache-native orientation,
  // which equals the caller's orientation by definition.
  if (oppositeBucket.length > 0) {
    const baseMarkets = (merged && merged.markets) || (closest && closest.markets) || {};
    for (const ev of oppositeBucket) {
      if (!ev || !ev.markets) continue;
      for (const k of _MERGEABLE_SUPP_MARKETS) {
        if (!ev.markets[k]) continue;
        if (baseMarkets[k]) continue;
        if (!merged) merged = { ...closest, markets: { ...(closest.markets || {}) } };
        if (merged.markets[k]) continue;
        merged.markets[k] = _flipMarketOrientation(k, ev.markets[k]);
      }
    }
  }

  let result = merged || closest;

  // If `closest` came from the reverse bucket, flip every market block
  // (primary + supplementals) so the caller receives data in the orientation
  // they requested. Updates homeTeam/awayTeam labels to the caller's order
  // and tags the event with _orientationFlipped for downstream debugging.
  if (flippedBucket) {
    const flippedMarkets = {};
    for (const [mk, m] of Object.entries(result.markets || {})) {
      flippedMarkets[mk] = _flipMarketOrientation(mk, m);
    }
    result = {
      ...result,
      homeTeam,
      awayTeam,
      markets: flippedMarkets,
      _orientationFlipped: true,
    };
  }

  return result;
}

// Build a merged event view: keep `closest` as the base, then union in
// any _MERGEABLE_SUPP_MARKETS that same-key sibling entries have but
// `closest` doesn't. Returns the original `closest` (no copy) when no
// merging applies — preserves existing reference semantics. Otherwise
// returns a shallow copy with a new .markets object so we don't mutate
// the cache entry.
function _mergeSameKeySiblings(events, closest) {
  if (!closest || events.length < 2) return null;
  let merged = null;
  for (const ev of events) {
    if (ev === closest || !ev || !ev.markets) continue;
    for (const k of _MERGEABLE_SUPP_MARKETS) {
      if (ev.markets[k] && !(closest.markets && closest.markets[k]) && !(merged && merged.markets[k])) {
        if (!merged) merged = { ...closest, markets: { ...(closest.markets || {}) } };
        merged.markets[k] = ev.markets[k];
      }
    }
  }

  // Special case for team_totals: union byLine maps across all sibling
  // entries even when the closest already has team_totals. NBA / MLB /
  // NHL caches frequently hold multiple sibling entries for the same
  // matchup at different commenceTime stamps (real time + midnight UTC
  // placeholders), each with DIFFERENT primary lines (e.g. closest has
  // Lakers 99.5, sibling has Lakers 98.5). Without this union, a line
  // requested for the sibling's primary lands on the closest entry's
  // byLine + primary check, returns null, and the operator's Lines tab
  // shows fair=null even though the data exists one cache entry away.
  // Verified 2026-05-03 NBA Lakers/OKC: team_total line=98.5 had
  // fair=null because closest entry's primary was 99.5; the 98.5 data
  // sat in a sibling commenceTime entry.
  if (closest.markets && closest.markets.team_totals) {
    const closestTT = closest.markets.team_totals;
    let unionedTT = null;
    for (const side of ['home', 'away']) {
      const closestSide = closestTT[side];
      if (!closestSide) continue;
      // Build the union byLine map: start from closest's byLine, fold in
      // closest's own primary as a byLine entry too, then union sibling
      // entries' byLine + their primaries.
      const unionByLine = { ...(closestSide.byLine || {}) };
      if (closestSide.line != null && !unionByLine[String(closestSide.line)]) {
        unionByLine[String(closestSide.line)] = {
          line: closestSide.line,
          over: closestSide.over,
          under: closestSide.under,
        };
      }
      for (const ev of events) {
        if (ev === closest || !ev || !ev.markets || !ev.markets.team_totals) continue;
        const sibSide = ev.markets.team_totals[side];
        if (!sibSide) continue;
        // Fold sibling's byLine entries
        if (sibSide.byLine) {
          for (const [lk, le] of Object.entries(sibSide.byLine)) {
            if (!unionByLine[lk]) unionByLine[lk] = le;
          }
        }
        // Fold sibling's primary line
        if (sibSide.line != null && !unionByLine[String(sibSide.line)]) {
          unionByLine[String(sibSide.line)] = {
            line: sibSide.line,
            over: sibSide.over,
            under: sibSide.under,
          };
        }
      }
      // Only mutate if we actually expanded
      if (Object.keys(unionByLine).length > Object.keys(closestSide.byLine || {}).length) {
        if (!unionedTT) unionedTT = { ...closestTT };
        unionedTT[side] = { ...closestSide, byLine: unionByLine };
      }
    }
    if (unionedTT) {
      if (!merged) merged = { ...closest, markets: { ...(closest.markets || {}) } };
      merged.markets.team_totals = unionedTT;
    }
  }

  return merged;
}

// Flip a supplemented-market block when it was sourced from a reverse-
// orientation cache entry. h2h/h2h_f5/h2h_h1: swap home<->away.
// spreads/spreads_f5/spreads_h1: swap home<->away AND negate point/line
// on each side (home -1.5 ↔ away +1.5). team_totals: swap home<->away
// (over/under per side stay symmetric). totals/totals_f5/totals_h1:
// over/under are team-agnostic, no flip needed.
//
// Primary h2h/spreads/totals were added 2026-05-02 alongside the cross-
// orientation closest-by-time selection in getEventMarkets — without
// these branches, the orientation flip on a reverse-bucket result would
// leave the primary markets in the wrong orientation.
function _flipMarketOrientation(marketType, market) {
  if (!market) return market;
  if (marketType === 'totals' || marketType === 'totals_h1' || marketType === 'totals_f5') return market;
  if (marketType === 'h2h' || marketType === 'h2h_h1' || marketType === 'h2h_f5') {
    return { ...market, home: market.away, away: market.home };
  }
  if (marketType === 'spreads' || marketType === 'spreads_h1' || marketType === 'spreads_f5') {
    // Side `.point` is the spread for that specific team (Bruins +1.5,
    // Rangers -1.5) — it travels with the team across orientation flips
    // and must NOT be negated. Only `market.line` (canonical, from-home
    // perspective) needs negation since the home team identity changes.
    return {
      ...market,
      home: market.away,
      away: market.home,
      line: market.line != null ? -market.line : null,
    };
  }
  if (marketType === 'team_totals') {
    return { ...market, home: market.away, away: market.home };
  }
  return market;
}

function getCacheAge(sport) {
  const sportCache = oddsCache[sport];
  if (!sportCache) return Infinity;
  return (Date.now() - sportCache.fetchedAt) / 1000 / 60;
}

function isStale(sport) {
  const perSport = config.pricing.stalePriceMinutesBySport || {};
  const threshold = perSport[sport] != null ? perSport[sport] : config.pricing.stalePriceMinutes;
  return getCacheAge(sport) > threshold;
}

function getStaleThreshold(sport) {
  const perSport = config.pricing.stalePriceMinutesBySport || {};
  return perSport[sport] != null ? perSport[sport] : config.pricing.stalePriceMinutes;
}

/**
 * Pre-game closing-line guard. When an event starts within PREGAME_WINDOW_MIN,
 * sportsbooks move the line hard on late news (scratches, weather, scratches).
 * Our cached odds can be stale even when the sport-level cache passes isStale.
 *
 * Returns true if the caller should REFUSE to quote because the cache is too
 * stale for a game that's about to start.
 *
 * Rule: if startTime is within 30 min, require cache age ≤ 2 min.
 * Otherwise falls back to the normal per-sport threshold.
 */
function isEventStalePreGame(sport, startTime) {
  if (!startTime) return false;
  const startMs = new Date(startTime).getTime();
  if (isNaN(startMs)) return false;
  const minsToStart = (startMs - Date.now()) / 60000;
  if (minsToStart < 0 || minsToStart > 10) return false; // not in window
  // Within 10 min of tip-off — tighten to 3 min cache age
  return getCacheAge(sport) > 3;
}

// ---------------------------------------------------------------------------
// DELTA UPDATES — incremental odds changes from SharpAPI /odds/delta
// ---------------------------------------------------------------------------

/**
 * Rebuild consensus markets from raw odds rows for a single event.
 * Used by mergeDeltas to update only affected events.
 */
function rebuildEventConsensus(rawOdds) {
  const markets = {};

  const mlBooks = getBookPairs(rawOdds, 'moneyline');
  if (mlBooks.length > 0) markets.h2h = buildConsensusMoneyline(mlBooks);

  const spreadTypes = ['point_spread', 'run_line', 'puck_line'];
  const spreadOdds = rawOdds.filter(r => spreadTypes.includes(r.market_type));
  const spreadBooks = getBookPairs(spreadOdds, null);
  if (spreadBooks.length > 0) markets.spreads = buildConsensusSpread(spreadBooks);

  const totalTypes = ['total_points', 'total_runs', 'total_goals'];
  const totalOdds = rawOdds.filter(r => totalTypes.includes(r.market_type));
  const totalBooks = getBookPairsForTotals(totalOdds);
  if (totalBooks.length > 0) markets.totals = buildConsensusTotals(totalBooks);

  const teamTotalOdds = rawOdds.filter(r => r.market_type === 'team_total');
  if (teamTotalOdds.length > 0) {
    const teamTotalBooks = getBookPairsForTeamTotals(teamTotalOdds);
    if (teamTotalBooks.length > 0) {
      const tt = buildConsensusTeamTotals(teamTotalBooks);
      if (tt) markets.team_totals = tt;
    }
  }

  return markets;
}

/**
 * Merge delta rows into existing cache for a sport.
 * Finds affected events, updates their raw odds, and rebuilds consensus.
 */
function mergeDeltas(sport, deltaRows) {
  const sportCache = oddsCache[sport];
  if (!sportCache) return 0;

  // Group deltas by event_id
  const deltaByEvent = {};
  for (const row of deltaRows) {
    const eid = row.event_id;
    if (!deltaByEvent[eid]) deltaByEvent[eid] = [];
    deltaByEvent[eid].push(row);
  }

  let updated = 0;
  for (const [eventId, rows] of Object.entries(deltaByEvent)) {
    const homeTeam = cleanTeamName(rows[0].home_team || '');
    const awayTeam = cleanTeamName(rows[0].away_team || '');
    const key = normalizeEventKey(homeTeam, awayTeam);

    // Find existing event in cache
    const entry = sportCache.events[key];
    if (!entry) continue; // new event — skip, next full refresh picks it up

    const events = Array.isArray(entry) ? entry : [entry];
    // Match by eventId AND verify commence time is on the same day
    const deltaDate = rows[0].event_start_time ? new Date(rows[0].event_start_time).toISOString().substring(0, 10) : null;
    const existing = events.find(e => {
      if (e.eventId !== eventId) return false;
      if (!deltaDate) return true; // no date to verify, accept
      const cacheDate = e.commenceTime ? new Date(e.commenceTime).toISOString().substring(0, 10) : null;
      return !cacheDate || cacheDate === deltaDate;
    });
    if (!existing || !existing._rawOdds) continue;

    // Merge: replace/add rows matching by sportsbook + market_type + selection_type
    for (const deltaRow of rows) {
      const idx = existing._rawOdds.findIndex(r =>
        r.sportsbook === deltaRow.sportsbook &&
        r.market_type === deltaRow.market_type &&
        r.selection_type === deltaRow.selection_type
      );
      if (idx >= 0) {
        existing._rawOdds[idx] = deltaRow;
      } else {
        existing._rawOdds.push(deltaRow);
      }
    }

    // Rebuild consensus from updated raw odds
    existing.markets = rebuildEventConsensus(existing._rawOdds);
    updated++;
  }

  if (updated > 0) {
    sportCache.fetchedAt = Date.now();
    log.info('OddsFeed', `Delta merged: ${updated} events updated for ${sport}`);
  }
  return updated;
}

/**
 * Fetch odds changes since last timestamp for a sport.
 * Falls back to full fetch if no previous timestamp or on error.
 */
async function fetchOddsDelta(sport) {
  const mapping = LEAGUE_MAP[sport];
  if (!mapping) return null;

  const since = lastDeltaTimestamp[sport];
  if (!since) {
    // No previous fetch — do a full fetch to establish baseline
    log.debug('OddsFeed', `No delta baseline for ${sport}, doing full fetch`);
    return fetchOddsForSport(sport);
  }

  // Split delta by market type and paginate each until drained. Same
  // reasoning as fetchOddsForSport: /odds/delta caps `limit` at 200 and
  // returns meta.pagination.has_more + next_offset; we loop until empty.
  const marketTypesList = {
    'baseball_mlb': ['moneyline', 'run_line', 'total_runs', 'team_total', '1st_5_innings_moneyline', '1st_5_innings_run_line'],
    'icehockey_nhl': ['moneyline', 'puck_line', 'total_goals', 'team_total'],
    'basketball_nba': ['moneyline', 'point_spread', 'total_points', 'team_total'],
    'soccer': ['moneyline', 'point_spread', 'total_goals', 'team_total'],
  }[sport] || ['moneyline', 'point_spread', 'total_points', 'team_total'];

  const PAGE_LIMIT = 200;
  const MAX_PAGES_PER_MARKET = 50;
  const rows = [];
  let anyFailed = false;
  for (const mt of marketTypesList) {
    const baseUrl = `${config.oddsApi.baseUrl}/odds/delta`
      + `?${mapping.param}=${mapping.value}`
      + `&market=${mt}`
      + `&since=${encodeURIComponent(since)}`
      + `&limit=${PAGE_LIMIT}`;
    let offset = 0;
    let pages = 0;
    while (pages < MAX_PAGES_PER_MARKET) {
      const url = offset === 0 ? baseUrl : `${baseUrl}&offset=${offset}`;
      try {
        const resp = await fetch(url, {
          headers: { 'X-API-Key': config.oddsApi.apiKey },
        });
        if (!resp.ok) {
          log.warn('OddsFeed', `Delta fetch failed (${resp.status}) for ${sport}/${mt} (page ${pages + 1})`);
          anyFailed = true;
          break;
        }
        const body = await safeJsonFetch(resp);
        const mtRows = body.data || [];
        rows.push(...mtRows);
        pages++;
        const pagination = body.meta && body.meta.pagination;
        if (!pagination || !pagination.has_more) break;
        offset = pagination.next_offset != null ? pagination.next_offset : offset + mtRows.length;
        if (mtRows.length === 0) break; // defensive: no progress
      } catch (err) {
        log.warn('OddsFeed', `Delta fetch error for ${sport}/${mt} (page ${pages + 1}): ${err.message}`);
        anyFailed = true;
        break;
      }
    }
    if (pages >= MAX_PAGES_PER_MARKET) {
      log.warn('OddsFeed', `Hit ${MAX_PAGES_PER_MARKET}-page safety cap for ${sport}/${mt} delta — possible pagination loop`);
    }
  }

  // If everything failed, fall back to full fetch
  if (anyFailed && rows.length === 0) {
    log.warn('OddsFeed', `All delta fetches failed for ${sport}, falling back to full`);
    return fetchOddsForSport(sport);
  }

  try {
    lastDeltaTimestamp[sport] = new Date().toISOString();
    // Bump fetchedAt on every successful delta poll, even when zero rows
    // changed. Without this, staleness was measuring "time since last
    // price move" instead of "time since last successful refresh check"
    // — late at night when NBA/MLB lines stop moving for 7-8 minutes,
    // the cache was being classified stale despite the 30s delta loop
    // happily polling and confirming nothing had changed. Verified
    // 2026-05-02: NBA Cavaliers ML and player_hitter_hits parlays
    // declined as "stale 5m / 4m" while delta polls were succeeding
    // every 30s with zero rows.
    if (oddsCache[sport]) {
      oddsCache[sport].fetchedAt = Date.now();
    }
    if (rows.length === 0) {
      log.debug('OddsFeed', `No delta changes for ${sport}`);
      return null;
    }
    log.info('OddsFeed', `Delta: ${rows.length} changed rows for ${mapping.value}`);
    mergeDeltas(sport, rows);
    return oddsCache[sport]?.events;
  } catch (err) {
    log.warn('OddsFeed', `Delta merge error for ${sport}: ${err.message}, falling back to full`);
    return fetchOddsForSport(sport);
  }
}

/**
 * Run delta updates for all SharpAPI sports (not Odds API fallback sports).
 */
async function refreshAllSportsDelta() {
  let mmaTouched = false;
  for (const sport of Object.keys(LEAGUE_MAP)) {
    try {
      await fetchOddsDelta(sport);
      if (sport === 'mma_mixed_martial_arts') mmaTouched = true;
    } catch (err) {
      log.warn('OddsFeed', `Delta refresh failed for ${sport}: ${err.message}`);
    }
  }
  // SharpAPI delta refresh can ADD MMA events under SharpAPI naming
  // ("Steve Erceg vs Tim Elliott") even when the DK-merged variant
  // ("Stephen Erceg vs Tim Elliott") already has totals. Without
  // re-merging, the new SharpAPI-named entry stays h2h-only and the
  // line-manager's lookup against that orientation returns null fair —
  // operator caught Tim Elliott vs Steve Erceg O 2.5 declining as
  // "no totals quote" minutes after the dashboard had shown valid
  // odds. mergeDkMmaFights uses DK's own 15-min cache so re-running
  // is cheap (cache hit, just iterates the events). Fire-and-forget.
  if (mmaTouched) {
    mergeDkMmaFights().catch(err => {
      log.warn('OddsFeed', `Post-delta DK MMA merge failed: ${err.message}`);
    });
  }
}

async function refreshEventsIndex() {
  for (const sport of Object.keys(LEAGUE_MAP)) {
    try {
      const mapping = LEAGUE_MAP[sport];
      const url = `${config.oddsApi.baseUrl}/events`
        + `?${mapping.param}=${mapping.value}`
        + `&live=false&limit=200`;
      const resp = await fetch(url, {
        headers: { 'X-API-Key': config.oddsApi.apiKey },
      });
      if (!resp.ok) continue;
      const body = await safeJsonFetch(resp);
      const events = body.data || [];
      sharpEventsIndex[sport] = {
        fetchedAt: Date.now(),
        events: events.map(e => ({
          eventId: e.event_id || e.id,
          homeTeam: cleanTeamName(e.home_team || ''),
          awayTeam: cleanTeamName(e.away_team || ''),
          startTime: e.event_start_time || e.start_time,
        })).filter(e => e.homeTeam && e.awayTeam),
      };
      log.info('OddsFeed', `Events index: ${sharpEventsIndex[sport].events.length} events for ${sport}`);
    } catch (err) {
      log.warn('OddsFeed', `Events index failed for ${sport}: ${err.message}`);
    }
  }
}

function getSharpEvents(sport) {
  return sharpEventsIndex[sport]?.events || [];
}

async function refreshAllSports() {
  // Refresh events index first — line-manager uses it for matching
  await refreshEventsIndex();

  const results = {};
  // Build the list: configured sports + golf_matchups if DataGolf key is set
  const sportsToRefresh = [...config.supportedSports];
  if (config.dataGolf && config.dataGolf.apiKey && !sportsToRefresh.includes('golf_matchups')) {
    sportsToRefresh.push('golf_matchups');
  }
  for (const sport of sportsToRefresh) {
    try {
      const events = await fetchOddsForSport(sport);
      results[sport] = { ok: true, events: Object.keys(events || {}).length };
    } catch (err) {
      log.error('OddsFeed', `Failed to fetch ${sport}: ${err.message}`);
      results[sport] = { ok: false, error: err.message };
    }
    await new Promise(r => setTimeout(r, 300));
  }

  // Fire-and-forget alt-line pre-warm per sport — don't block the main refresh
  // cycle on this. Keeps the critical RFQ path hitting cache instead of The
  // Odds API network round-trip.
  for (const sport of sportsToRefresh) {
    if (!SPORTS_WITH_ALT_MARKETS.has(sport)) continue;
    warmAltLines(sport).catch(err => {
      log.warn('OddsFeed', `Alt-line warm failed for ${sport}: ${err.message}`);
    });
  }

  // Fire-and-forget h2h backfill for SharpAPI-primary sports where some
  // events lack moneyline data. One bulk Odds API call per sport —
  // cheap relative to the per-event alt-line fetches.
  for (const sport of sportsToRefresh) {
    if (!H2H_BACKFILL_SPORTS.has(sport)) continue;
    backfillMissingH2h(sport).catch(err => {
      log.warn('OddsFeed', `H2H backfill failed for ${sport}: ${err.message}`);
    });
  }

  // Fire-and-forget DK MMA merge — covers UFC Fight Night prelims that
  // The Odds API routinely misses. Adds ~15s of Puppeteer fetch on a
  // 15-min cache, so negligible load.
  if (sportsToRefresh.includes('mma_mixed_martial_arts')) {
    mergeDkMmaFights().catch(err => {
      log.warn('OddsFeed', `DK MMA merge failed: ${err.message}`);
    });
  }

  return results;
}

function getCacheStatus() {
  const status = {};
  // Include golf_matchups (DataGolf) alongside configured sports
  const sports = [...config.supportedSports];
  if (oddsCache['golf_matchups'] && !sports.includes('golf_matchups')) {
    sports.push('golf_matchups');
  }
  for (const sport of sports) {
    const cache = oddsCache[sport];
    const totalEvents = cache ? Object.values(cache.events).reduce((s, entry) => s + (Array.isArray(entry) ? entry.length : 1), 0) : 0;
    status[sport] = cache ? {
      eventCount: totalEvents,
      ageMinutes: Math.round(getCacheAge(sport) * 10) / 10,
      stale: isStale(sport),
    } : { eventCount: 0, ageMinutes: null, stale: true };
  }
  return status;
}

function __debugGetCache(sport) {
  return oddsCache[sport] || null;
}

// ---------------------------------------------------------------------------
// CLOSING LINE CAPTURE — snapshots Pinnacle + consensus fair probs for every
// event the moment its commenceTime crosses into the past. Used for CLV
// analysis at settlement time. Idempotent — only captures a given event once.
// ---------------------------------------------------------------------------

const CLOSING_CAPTURE_WINDOW_MS = 20 * 60 * 1000; // capture within 20min of commence

function captureClosingLines() {
  const now = Date.now();
  let captured = 0;
  for (const sport of Object.keys(oddsCache)) {
    const cache = oddsCache[sport];
    if (!cache || !cache.events) continue;
    for (const [key, entry] of Object.entries(cache.events)) {
      const events = Array.isArray(entry) ? entry : [entry];
      for (const event of events) {
        if (!event || !event.homeTeam || !event.commenceTime) continue;
        const startMs = new Date(event.commenceTime).getTime();
        if (isNaN(startMs)) continue;
        // Only capture events whose commenceTime is in the past but within
        // the capture window. Events more than 20 min past commence are
        // already "closed" enough — don't overwrite.
        const age = now - startMs;
        if (age < 0) continue; // not yet started
        if (age > CLOSING_CAPTURE_WINDOW_MS) continue; // already past window
        const cacheKey = sport + '|' + key + '|' + (event.eventId || '');
        if (closingLinesCache[cacheKey]) continue; // already captured
        // Snapshot the relevant markets
        const m = event.markets || {};
        const snap = {
          sport,
          homeTeam: event.homeTeam,
          awayTeam: event.awayTeam,
          commenceTime: event.commenceTime,
          capturedAt: new Date().toISOString(),
          markets: {},
          pinnacle: {},
        };
        if (m.h2h) {
          snap.markets.h2h = {
            home: m.h2h.home?.fairProb || null,
            away: m.h2h.away?.fairProb || null,
            homeDisplayFair: m.h2h.home?.displayFairProb || null,
            awayDisplayFair: m.h2h.away?.displayFairProb || null,
          };
          if (m.h2h.pinnacle) {
            snap.pinnacle.h2h = {
              home: m.h2h.pinnacle.home,
              away: m.h2h.pinnacle.away,
            };
          }
        }
        if (m.spreads) {
          snap.markets.spreads = {
            line: m.spreads.line,
            home: m.spreads.home?.fairProb || null,
            away: m.spreads.away?.fairProb || null,
          };
          if (m.spreads.pinnacle) {
            snap.pinnacle.spreads = {
              line: m.spreads.line,
              home: m.spreads.pinnacle.home,
              away: m.spreads.pinnacle.away,
            };
          }
        }
        if (m.totals) {
          snap.markets.totals = {
            line: m.totals.line,
            over: m.totals.over?.fairProb || null,
            under: m.totals.under?.fairProb || null,
          };
          if (m.totals.pinnacle) {
            snap.pinnacle.totals = {
              line: m.totals.line,
              over: m.totals.pinnacle.over,
              under: m.totals.pinnacle.under,
            };
          }
        }
        closingLinesCache[cacheKey] = snap;
        captured++;
      }
    }
  }
  if (captured > 0) log.info('CLV', `Captured ${captured} closing line snapshot(s) (total cached: ${Object.keys(closingLinesCache).length})`);
  return { captured, total: Object.keys(closingLinesCache).length };
}

/**
 * Look up a closing line snapshot by event key. Tries primary key first,
 * then falls back to any matching sport + event key.
 */
function getClosingLineSnapshot(sport, homeTeam, awayTeam, pxEventId) {
  const key = normalizeEventKey(homeTeam, awayTeam);
  // Try exact match first
  const exactKey = sport + '|' + key + '|' + (pxEventId || '');
  if (closingLinesCache[exactKey]) return closingLinesCache[exactKey];
  // Fallback: any snapshot matching sport + team key
  const prefix = sport + '|' + key + '|';
  for (const k of Object.keys(closingLinesCache)) {
    if (k.startsWith(prefix)) return closingLinesCache[k];
  }
  return null;
}

function getClosingLinesStatus() {
  return {
    total: Object.keys(closingLinesCache).length,
    sports: (() => {
      const bySport = {};
      for (const snap of Object.values(closingLinesCache)) {
        bySport[snap.sport] = (bySport[snap.sport] || 0) + 1;
      }
      return bySport;
    })(),
  };
}

function getAllCachedEvents() {
  const all = [];
  for (const sport of Object.keys(oddsCache)) {
    const cache = oddsCache[sport];
    if (!cache || !cache.events) continue;
    for (const [key, entry] of Object.entries(cache.events)) {
      // SharpAPI stores arrays (for doubleheaders), Odds API stores single objects
      const events = Array.isArray(entry) ? entry : [entry];
      for (const event of events) {
        if (!event || !event.homeTeam) continue;
        all.push({
          sport,
          homeTeam: event.homeTeam,
          awayTeam: event.awayTeam,
          markets: event.markets ? Object.keys(event.markets) : [],
          commenceTime: event.commenceTime,
        });
      }
    }
  }
  return all;
}

// ---------------------------------------------------------------------------
// HELPERS
// ---------------------------------------------------------------------------

function normalizeEventKey(homeTeam, awayTeam) {
  return `${normalizeTeamName(homeTeam)}|${normalizeTeamName(awayTeam)}`;
}

function normalizeTeamName(name) {
  // NFD-decompose + strip combining marks so diacritics (São, Godínez, Peña)
  // collapse to their ASCII equivalents. Without this, accented characters
  // are deleted outright by the [^a-z0-9 ] filter, corrupting names like
  // "Godínez" → "godnez" and silently breaking every MMA/Soccer matcher
  // that compares against an ASCII-only SharpAPI or TheOddsAPI feed.
  return (name || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, '')
    .trim();
}

// SharpAPI sometimes returns team names with a city abbreviation prefix
// (e.g. "BOS Red Sox") instead of the full city name ("Boston Red Sox")
// that The Odds API, PX, and everything else uses. Without canonicalization
// the same game ends up stored in the cache under TWO different keys —
// "bos red sox|new york yankees" and "boston red sox|new york yankees" —
// and the closest-by-time doubleheader matcher can't work across them.
//
// Observed consequence: April 2026, Red Sox @ Yankees 2-game series.
// SharpAPI cached tonight's game as "BOS Red Sox"; Odds API cached
// tomorrow's game as "Boston Red Sox". Line-manager asked for tonight's
// fair prob with homeTeam="Boston Red Sox"; cache returned tomorrow's
// entry. Red Sox fair came back as 0.421 (tomorrow's pitcher) when the
// correct value for tonight was 0.537. Every parlay with a Red Sox leg
// was priced ~12pp wrong, giving the bettor a ~20% EV edge.
//
// This map lists every team we've observed SharpAPI abbreviate. Extend
// as new offenders appear. Keys + values are raw (pre-lowercase) so
// cleanTeamName runs before normalizeTeamName.
const TEAM_ABBREV_TO_CANONICAL = {
  // MLB
  'BOS Red Sox': 'Boston Red Sox',
  'TOR Blue Jays': 'Toronto Blue Jays',
  // SharpAPI uses "Chicago WS" while TOA + PX use "Chicago White Sox".
  // Without canonicalization, F5 / team_totals supplements run against
  // the SharpAPI-keyed entry (with "Chicago WS") and PX matches against
  // the TOA-keyed entry (with "Chicago White Sox") — null fair-prob on
  // F5 ML / spread / total. Operator-flagged 2026-05-01.
  'Chicago WS': 'Chicago White Sox',
  // Oakland Athletics — three observed variants across feeds:
  //   - SharpAPI: "A's"
  //   - The Odds API: "Oakland Athletics"
  //   - PX (and team's own marketing post-relocation): "Athletics"
  // Without canonicalization, the cache holds three separate entries
  // for tonight's Cleveland @ Athletics game and supplements (F5,
  // team_totals) land on whichever entry resolved the TOA event ID,
  // not the "Athletics"-keyed entry PX matches against. Operator-
  // flagged 2026-05-01: F5 ML / team-total nulls for the A's game.
  // Map all three variants to the PX-side spelling so they collapse
  // into one cache key.
  "A's": 'Athletics',
  'Oakland Athletics': 'Athletics',
  // NHL
  'VGK Golden Knights': 'Vegas Golden Knights',
  'LA Kings': 'Los Angeles Kings',
  // NBA short-name variants. SharpAPI sometimes drops the mascot
  // ("Minnesota" instead of "Minnesota Timberwolves") and sometimes
  // truncates mid-word ("Los Angeles L" → Lakers). PX uses canonical
  // full names. Without canonicalization, the cache holds two-three
  // entries per game and supplements (H1, team_totals, series_*) land
  // on whichever SharpAPI returned, while PX matches the full-name
  // entry. Verified 2026-05-03 via /odds-events scan: Min/SAS,
  // PHI/NYK, LAL/OKC pairs each had 2-3 duplicate cache entries.
  // Only unambiguous cities included here — "Los Angeles" alone could
  // mean Lakers or Clippers, so it's NOT mapped (kept as-is so the
  // matcher uses substring/last-word logic to disambiguate).
  'Minnesota': 'Minnesota Timberwolves',
  'San Antonio': 'San Antonio Spurs',
  'Philadelphia': 'Philadelphia 76ers',
  'Los Angeles L': 'Los Angeles Lakers',

  // Soccer short-vs-full club name variants. Verified 2026-05-03 via
  // /odds-events scan: each pair created two cache entries for the
  // same fixture, dropping fair-prob coverage to zero on whichever
  // entry PX matched against the operator wasn't using.
  //
  // Italy (Serie A):
  'Verona': 'Hellas Verona',
  // Spain (La Liga / Segunda):
  'Oviedo': 'Real Oviedo',
  // Argentina (Primera Division):
  'Rivadavia': 'Independiente Rivadavia',
  'CA Aldosivi': 'Aldosivi',
  'Racing Cordoba': 'Racing de Cordoba',
  // Extend here: add any short-vs-full soccer variants we find in /odds-events
  // duplicate-pair scans. Tennis last-name-only and MMA name-spelling drift
  // need fuzzy matching, not canonicalization — separate effort.
};

/**
 * Clean team names from SharpAPI (removes pitcher info like "(TBD)")
 * and canonicalize known abbreviation-prefixed names so they collide
 * with the Odds API / PX full-name versions in the cache.
 */
function cleanTeamName(name) {
  const stripped = (name || '')
    // Trailing parenthetical (MLB starter, e.g. "Yankees (Cole)"). Existing.
    .replace(/\s*\([^)]*\)\s*$/, '')
    // Leading "Game N:" / "Game N -" prefix that SharpAPI prepends to
    // team names during NBA/NHL playoff series. Without this strip the
    // cached awayTeam ends up as "Game 5: Minnesota Timberwolves",
    // which breaks resolveOddsApiEventId() in the H1 / F5 / team-totals
    // supplements (TOA has the canonical name "Minnesota Timberwolves",
    // not the prefixed form). Result: supplement silently skips the
    // event, markets.h2h_h1 never populated, RFQs for that event's
    // playoff H1/F5/team-total markets decline as "no fair value".
    // Operator-flagged 2026-04-27 (Nuggets H1 ML decline screenshot).
    .replace(/^Game\s+\d+\s*[:\-]\s*/i, '')
    .trim();
  return TEAM_ABBREV_TO_CANONICAL[stripped] || stripped;
}

/**
 * Extract starter name (MLB pitcher / NHL goalie) from a SharpAPI team name.
 * SharpAPI format: "New York Yankees (Gerrit Cole)" → "Gerrit Cole"
 * Returns null if no starter listed or if the starter is "TBD" / blank.
 */
function extractStarter(name) {
  if (!name) return null;
  const m = name.match(/\(([^)]+)\)\s*$/);
  if (!m) return null;
  const s = m[1].trim();
  if (!s || /^tbd$/i.test(s) || /^tba$/i.test(s)) return null;
  return s;
}

/**
 * Build the lineup cache key for a sport + event.
 * Uses normalized team key + date so same-day doubleheaders stay distinct.
 */
function makeLineupKey(homeTeam, awayTeam, commenceTime) {
  const eventKey = normalizeEventKey(homeTeam, awayTeam);
  const date = commenceTime ? new Date(commenceTime).toISOString().substring(0, 10) : '';
  return `${eventKey}|${date}`;
}

/**
 * Update lineup cache for a single event. Detects changes in starting
 * pitcher/goalie and stamps lastChangeAt when one is seen. Called during
 * the odds refresh flow for MLB and NHL only.
 *
 * homeStarter/awayStarter come from extractStarter() on the raw team names.
 * If a starter was previously known and now null (or different), it's
 * treated as a change. Null→null is a no-op.
 */
function updateLineupState(sport, homeTeam, awayTeam, commenceTime, homeStarter, awayStarter) {
  if (!lineupCache[sport]) lineupCache[sport] = {};
  const key = makeLineupKey(homeTeam, awayTeam, commenceTime);
  const now = Date.now();
  const prior = lineupCache[sport][key];

  if (!prior) {
    // First time seeing this event — just stash the baseline (no change event)
    lineupCache[sport][key] = {
      homeStarter,
      awayStarter,
      seenAt: now,
      lastChangeAt: null,
      lastChangeDetail: null,
    };
    return;
  }

  const homeDiff = prior.homeStarter !== homeStarter && (prior.homeStarter || homeStarter);
  const awayDiff = prior.awayStarter !== awayStarter && (prior.awayStarter || awayStarter);

  if (homeDiff || awayDiff) {
    const parts = [];
    if (homeDiff) parts.push(`${homeTeam}: ${prior.homeStarter || 'TBD'} → ${homeStarter || 'TBD'}`);
    if (awayDiff) parts.push(`${awayTeam}: ${prior.awayStarter || 'TBD'} → ${awayStarter || 'TBD'}`);
    const detail = parts.join('; ');
    log.info('Lineup', `${sport} lineup change detected — ${detail}`);
    lineupCache[sport][key] = {
      homeStarter,
      awayStarter,
      seenAt: now,
      lastChangeAt: now,
      lastChangeDetail: detail,
    };
  } else {
    // No change — refresh seenAt but preserve lastChangeAt so the grace
    // window continues to count from the original change time.
    prior.homeStarter = homeStarter;
    prior.awayStarter = awayStarter;
    prior.seenAt = now;
  }
}

/**
 * Check whether an event's lineup recently changed (within grace window).
 * Returns { changed: true, ageMs, detail } if within grace, else null.
 * Used by the pricer to decline MLB/NHL legs for a few minutes after a
 * starter swap so the books have time to re-price.
 *
 * Non-MLB/NHL sports always return null (not tracked).
 */
function checkLineupFreshness(sport, homeTeam, awayTeam, commenceTime) {
  if (sport !== 'baseball_mlb' && sport !== 'icehockey_nhl') return null;
  const bucket = lineupCache[sport];
  if (!bucket) return null;
  const key = makeLineupKey(homeTeam, awayTeam, commenceTime);
  const entry = bucket[key];
  if (!entry || !entry.lastChangeAt) return null;
  const ageMs = Date.now() - entry.lastChangeAt;
  if (ageMs >= LINEUP_GRACE_MS) return null;
  return { changed: true, ageMs, detail: entry.lastChangeDetail };
}

/**
 * Debug accessor — return the full lineup cache for /lineups endpoint.
 */
function getLineupCache() {
  return lineupCache;
}

/**
 * Determine which side of a game a pitcher is on. Returns 'home' or
 * 'away' if the pitcher is the listed starter on that side, else null.
 * Used by the K-prop + ML SGP combo gate to verify the ML leg matches
 * the pitcher's team (allowed) vs the opposing team (blocked — that
 * combo is anti-correlated and a weird bet).
 */
function getPitcherSide(sport, homeTeam, awayTeam, commenceTime, playerName) {
  if (!playerName) return null;
  const bucket = lineupCache[sport];
  if (!bucket) return null;
  const key = makeLineupKey(homeTeam, awayTeam, commenceTime);
  const entry = bucket[key];
  if (!entry) return null;
  // Diacritic-insensitive comparison (PX may send "Randy Vásquez" while
  // SharpAPI lineup has "Randy Vasquez"). Mirror the prop-matcher's
  // normalization.
  const norm = (s) => (s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
  const target = norm(playerName);
  if (entry.homeStarter && norm(entry.homeStarter) === target) return 'home';
  if (entry.awayStarter && norm(entry.awayStarter) === target) return 'away';
  return null;
}

// ---------------------------------------------------------------------------
// SCORES — fetch game results from The Odds API for early win detection
// ---------------------------------------------------------------------------

const scoresCache = {}; // { sport: { fetchedAt, games: [{ homeTeam, awayTeam, commenceTime, completed, homeScore, awayScore }] } }
const SCORES_TTL_MS = 30 * 1000; // 30s cache — pairs with checkLegResults running every 30s. Bounds TOA hit rate while keeping completion latency low.

// Cache of active sport keys discovered from The Odds API's /v4/sports/.
// Used to expand 'soccer' (generic) into per-league fetches. The list
// rarely changes; 6h TTL is plenty.
let _activeSportsCache = { fetchedAt: 0, keys: [] };
const ACTIVE_SPORTS_TTL_MS = 6 * 60 * 60 * 1000;

// Internal-key → Odds-API-key overrides. Most of our sport keys match
// The Odds API 1:1, but a few are wrong / use different naming:
//   - soccer_conmebol_libertadores → soccer_conmebol_copa_libertadores
//     (audited Apr 26: Odds API uses the longer 'copa' form)
// Add new entries here when /audit-scores reveals more drift; missing
// keys silently 404 and cause stuck-pending status circles in the UI.
const SCORES_API_KEY_OVERRIDES = {
  'soccer_conmebol_libertadores': 'soccer_conmebol_copa_libertadores',
};

async function _getActiveSportsList() {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return [];
  if (Date.now() - _activeSportsCache.fetchedAt < ACTIVE_SPORTS_TTL_MS && _activeSportsCache.keys.length > 0) {
    return _activeSportsCache.keys;
  }
  try {
    const resp = await fetch(`https://api.the-odds-api.com/v4/sports/?apiKey=${theOddsApiKey}`);
    if (!resp.ok) return _activeSportsCache.keys;
    const all = await safeJsonFetch(resp);
    const keys = (all || []).filter(s => s.active).map(s => s.key);
    _activeSportsCache = { fetchedAt: Date.now(), keys };
    log.debug('Scores', `Discovered ${keys.length} active sport keys on The Odds API`);
    return keys;
  } catch (err) {
    log.warn('Scores', `Failed to fetch active sports list: ${err.message}`);
    return _activeSportsCache.keys;
  }
}

/**
 * Fetch scores for a sport from The Odds API.
 * Returns array of completed/in-progress games with scores.
 */
async function fetchScores(sport) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return [];

  // Check cache
  const cached = scoresCache[sport];
  if (cached && (Date.now() - cached.fetchedAt) < SCORES_TTL_MS) {
    return cached.games;
  }

  // Generic 'soccer' has no Odds API scores endpoint — only league-
  // specific keys do (soccer_epl, soccer_fa_cup, soccer_uefa_*, etc.).
  // Line-manager tags many soccer legs with 'soccer' (generic) because
  // SharpAPI returns all soccer events under that bucket and it's
  // tried first in sportNameMap order. Without this expansion,
  // fetchScores('soccer') 404s silently and no soccer leg ever resolves
  // a score. Operator-visible (Apr 26):
  //   - EPL Leeds @ Chelsea finished but parlay status stayed grey
  //   - FA Cup match also missed (initially missed by my hardcoded list
  //     because PINNACLE_SPORT_MAP only had 12 of 53 active soccer keys)
  //
  // Use dynamic discovery (active soccer_* keys from /v4/sports/) so
  // the aggregation auto-covers any soccer league The Odds API supports
  // — FA Cup, Coppa Italia, DFB-Pokal, J-League, K-League, etc. — without
  // hardcoding. Same pattern available for any other generic key (e.g.
  // tennis is already handled separately via ODDS_API_FALLBACK.dynamic).
  if (sport === 'soccer') {
    const allActive = await _getActiveSportsList();
    const soccerLeagues = allActive.filter(k => k.startsWith('soccer_'));
    let allGames = [];
    for (const league of soccerLeagues) {
      try {
        const games = await fetchScores(league);
        if (games && games.length > 0) allGames = allGames.concat(games);
      } catch (_) { /* per-league failure shouldn't block others */ }
    }
    scoresCache[sport] = { fetchedAt: Date.now(), games: allGames };
    log.debug('Scores', `Cached ${allGames.length} aggregated soccer scores from ${soccerLeagues.length} active leagues (dynamic)`);
    return allGames;
  }

  const parseGames = (data) => (data || []).map(g => ({
    homeTeam: g.home_team,
    awayTeam: g.away_team,
    commenceTime: g.commence_time,
    completed: g.completed || false,
    homeScore: g.scores?.find(s => s.name === g.home_team)?.score != null ? Number(g.scores.find(s => s.name === g.home_team).score) : null,
    awayScore: g.scores?.find(s => s.name === g.away_team)?.score != null ? Number(g.scores.find(s => s.name === g.away_team).score) : null,
  }));

  try {
    // Dynamic sports (tennis) need tournament discovery — The Odds API
    // doesn't have a generic 'tennis' scores endpoint.
    const fallback = ODDS_API_FALLBACK[sport];
    if (fallback && fallback.dynamic && fallback.sportPrefix) {
      const sportsResp = await fetch(`https://api.the-odds-api.com/v4/sports/?apiKey=${theOddsApiKey}`);
      if (!sportsResp.ok) return cached?.games || [];
      const allSports = await sportsResp.json();
      const active = allSports.filter(s => s.key.startsWith(fallback.sportPrefix) && s.active);
      if (active.length === 0) return cached?.games || [];

      let allGames = [];
      for (const tournament of active) {
        const url = `https://api.the-odds-api.com/v4/sports/${tournament.key}/scores/?apiKey=${theOddsApiKey}&daysFrom=1`;
        const resp = await fetch(url);
        if (resp.ok) {
          const data = await resp.json();
          allGames = allGames.concat(parseGames(data));
        }
      }
      scoresCache[sport] = { fetchedAt: Date.now(), games: allGames };
      log.debug('Scores', `Cached ${allGames.length} scores for ${sport} from ${active.length} tournaments (${allGames.filter(g => g.completed).length} completed)`);
      return allGames;
    }

    // Standard sports — direct fetch. Translate via SCORES_API_KEY_OVERRIDES
    // for any sport key whose Odds API name doesn't match our internal one
    // (e.g. soccer_conmebol_libertadores → soccer_conmebol_copa_libertadores).
    const apiSport = SCORES_API_KEY_OVERRIDES[sport] || sport;
    const url = `https://api.the-odds-api.com/v4/sports/${apiSport}/scores/?apiKey=${theOddsApiKey}&daysFrom=1`;
    const resp = await fetch(url);
    if (!resp.ok) {
      log.debug('Scores', `Failed to fetch scores for ${sport}: ${resp.status}`);
      return cached?.games || [];
    }

    const games = parseGames(await resp.json());
    scoresCache[sport] = { fetchedAt: Date.now(), games };
    log.debug('Scores', `Cached ${games.length} scores for ${sport} (${games.filter(g => g.completed).length} completed)`);
    return games;
  } catch (err) {
    log.error('Scores', `Error fetching scores for ${sport}: ${err.message}`);
    return cached?.games || [];
  }
}

/**
 * Get game result for a specific matchup.
 * Returns { completed, homeScore, awayScore, winner: 'home'|'away'|'tie'|null } or null.
 */
async function getGameResult(sport, homeTeam, awayTeam, startTime) {
  // ESPN scoreboard is the primary score source — covers every supported
  // sport, updates within ~30s of real-time, free, no quota burn. The TOA
  // /scores fallback below catches any sport ESPN doesn't have a league
  // path for (or any single missed game where ESPN's team names didn't
  // match our normalization). Sync read against the in-memory cache the
  // ESPN poller fills in the background — never makes a network call.
  try {
    const espnScores = require('./espn-scores');
    const espnHit = espnScores.getEspnGameResult(sport, homeTeam, awayTeam);
    if (espnHit && espnHit.completed) return espnHit;
    // Hit but not completed yet — fall through to TOA in case TOA has
    // a result ESPN hasn't marked completed yet.
  } catch (_) { /* espn-scores unavailable — fall through */ }

  const games = await fetchScores(sport);
  if (games.length === 0) return null;

  // Match by team names (normalize for comparison)
  const normHome = normalizeTeamName(homeTeam);
  const normAway = normalizeTeamName(awayTeam);
  const targetTime = startTime ? new Date(startTime).getTime() : null;

  let bestMatch = null;
  let bestDiff = Infinity;

  for (const g of games) {
    const gHome = normalizeTeamName(g.homeTeam);
    const gAway = normalizeTeamName(g.awayTeam);

    // Check both orderings
    const match = (gHome.includes(normHome) || normHome.includes(gHome)) &&
                  (gAway.includes(normAway) || normAway.includes(gAway));
    const matchReverse = (gHome.includes(normAway) || normAway.includes(gHome)) &&
                         (gAway.includes(normHome) || normHome.includes(gAway));

    if (!match && !matchReverse) continue;

    // If multiple matches (doubleheader), pick closest by time
    if (targetTime && g.commenceTime) {
      const diff = Math.abs(new Date(g.commenceTime).getTime() - targetTime);
      if (diff < bestDiff) {
        bestDiff = diff;
        bestMatch = g;
      }
    } else {
      bestMatch = g;
    }
  }

  if (!bestMatch) return null;
  if (bestMatch.homeScore == null || bestMatch.awayScore == null) {
    return { completed: bestMatch.completed, homeScore: null, awayScore: null, winner: null };
  }

  let winner = null;
  if (bestMatch.homeScore > bestMatch.awayScore) winner = 'home';
  else if (bestMatch.awayScore > bestMatch.homeScore) winner = 'away';
  else winner = 'tie';

  return {
    completed: bestMatch.completed,
    homeScore: bestMatch.homeScore,
    awayScore: bestMatch.awayScore,
    winner,
  };
}

// ---------------------------------------------------------------------------
// PLAYER-PROP LOOKUP (Phase 1 shadow-pricing)
// ---------------------------------------------------------------------------
// Find SharpAPI player_strikeouts rows that match a PX leg's pitcher name +
// line value. Used by services/websocket.js shadow-pricing hook to log what
// we WOULD have priced — does NOT affect quote/decline behavior.
//
// Inputs:
//   sport          'baseball_mlb'
//   pxEventInfo    { homeTeam, awayTeam, startTime, ... } from line-manager
//                  (used to disambiguate which SharpAPI event_id this PX
//                  leg belongs to — PX and SharpAPI use different event ids)
//   playerName     extracted from PX market name e.g. "Tarik Skubal" parsed
//                  out of "Tarik Skubal Pitching Strikeouts"
//   line           numeric line value (e.g. 6.5)
//
// Returns:
//   {
//     matchedRows:    [...all SharpAPI rows matching player+line for this event],
//     books:          ['draftkings', 'fanduel'],
//     sides:          { over: [...], under: [...] }, // by selection
//     fairProbOver:   de-vigged fair P(Over) across books, or null,
//     fairProbUnder:  de-vigged fair P(Under) across books, or null,
//     resolvedEventId: SharpAPI event_id we matched against,
//   }
//   or null if no match found (for any reason — log the reason via stage).
function lookupPlayerStrikeoutProp(sport, pxEventInfo, playerName, line) {
  const stages = []; // for debug visibility into why a lookup failed
  if (!sport || !pxEventInfo || !playerName) {
    return { error: 'missing_input', stages: ['precondition'] };
  }
  const sportCache = propRowsCache[sport];
  if (!sportCache || !sportCache.player_strikeouts) {
    return { error: 'no_prop_cache', stages: ['cache_empty'] };
  }
  const allRows = sportCache.player_strikeouts;
  stages.push(`cache:${allRows.length}rows`);

  // Step 1: filter by event. SharpAPI event_id won't match PX's
  // sport_event_id, so match by home/away team + start time proximity.
  // Use normalizeTeamName + last-2-words matching to handle the
  // "BOS Red Sox" vs "Boston Red Sox" case (cleanTeamName at cache
  // time should already canonicalize this, but keep last-words
  // fallback for any abbrevs not in TEAM_ABBREV_TO_CANONICAL).
  const lastWords = (name, n = 2) => {
    const words = normalizeTeamName(name).split(/\s+/).filter(Boolean);
    return words.slice(-n).join(' ');
  };
  const pxHomeKey = lastWords(pxEventInfo.homeTeam || '');
  const pxAwayKey = lastWords(pxEventInfo.awayTeam || '');
  const pxStartMs = pxEventInfo.startTime ? Date.parse(pxEventInfo.startTime) : null;
  // Always surface PX teams in stages — makes "no_event_match" failures
  // self-debuggable from the persisted shadow log without needing to
  // cross-reference px_event_id back to the event mapping.
  stages.push(`px:${pxEventInfo.awayTeam || '?'}@${pxEventInfo.homeTeam || '?'}`);
  const teamMatchRows = allRows.filter(r => {
    const rh = lastWords(r.home_team || '');
    const ra = lastWords(r.away_team || '');
    // Bidirectional — SharpAPI sometimes flips home/away.
    return (rh === pxHomeKey && ra === pxAwayKey) ||
           (rh === pxAwayKey && ra === pxHomeKey);
  });
  stages.push(`team_match:${teamMatchRows.length}`);
  if (teamMatchRows.length === 0) {
    // Surface the available SharpAPI events so we can tell at a glance
    // whether the cache simply doesn't have prop data for this game (PX
    // game outside SharpAPI's prop slate) vs a team-name-matching bug.
    const availableEvents = [...new Set(allRows.map(r => `${r.away_team}@${r.home_team}`))];
    stages.push(`available:${availableEvents.join('|')}`);
    return { error: 'no_event_match', stages, availableEvents };
  }

  // If we have multiple events matching (doubleheader), narrow by start time
  let eventRows = teamMatchRows;
  if (pxStartMs) {
    const eventIds = [...new Set(teamMatchRows.map(r => r.event_id))];
    if (eventIds.length > 1) {
      // Pick event whose start time is closest to PX leg's start time
      const eventsByDist = eventIds.map(eid => {
        const sample = teamMatchRows.find(r => r.event_id === eid);
        const eMs = sample.event_start_time ? Date.parse(sample.event_start_time) : 0;
        return { eid, dist: Math.abs(eMs - pxStartMs) };
      }).sort((a, b) => a.dist - b.dist);
      const bestId = eventsByDist[0].eid;
      eventRows = teamMatchRows.filter(r => r.event_id === bestId);
      stages.push(`dh_resolve:${eventsByDist.length}->${bestId}`);
    }
  }
  const resolvedEventId = eventRows[0].event_id;

  // Step 2: filter by player_name. SharpAPI appends side-disambiguation
  // suffixes: "Tarik Skubal Thrown" (pitcher), "Aaron Judge Recorded"
  // (batter K). Strip the suffix before matching. Also strip diacritics
  // — PX sends "Randy Vásquez" but SharpAPI returns "Randy Vasquez".
  const stripDiacritics = (s) => (s || '').normalize('NFD').replace(/[̀-ͯ]/g, '');
  const normPlayer = stripDiacritics(playerName).toLowerCase().trim();
  const matchedRows = eventRows.filter(r => {
    const raw = stripDiacritics(r.player_name || '').toLowerCase();
    const stripped = raw.replace(/\s*-\s*total$/, '').replace(/\s+(thrown|recorded)$/, '').trim();
    // Tolerant match — substring both directions in case of formatting drift
    return stripped === normPlayer || stripped.includes(normPlayer) || normPlayer.includes(stripped);
  });
  stages.push(`player_match:${matchedRows.length}`);
  if (matchedRows.length === 0) {
    return { error: 'no_player_match', stages, resolvedEventId,
             samplePlayers: [...new Set(eventRows.map(r => r.player_name))].slice(0, 5) };
  }

  // Step 3: filter by line value (allow tiny float fuzz)
  const lineRows = line == null ? matchedRows : matchedRows.filter(r => Math.abs((r.line || 0) - line) < 0.01);
  stages.push(`line_match:${lineRows.length}`);
  if (lineRows.length === 0) {
    return { error: 'no_line_match', stages, resolvedEventId,
             sampleLines: [...new Set(matchedRows.map(r => r.line))].slice(0, 8) };
  }

  // Step 4: split by side and compute de-vigged fair probs across books.
  const overRows = lineRows.filter(r => /over/i.test(r.selection || r.selection_type || ''));
  const underRows = lineRows.filter(r => /under/i.test(r.selection || r.selection_type || ''));
  const books = [...new Set(lineRows.map(r => r.sportsbook).filter(Boolean))];
  // Surface side-availability per book so it's easy to see which side is
  // missing when books_with_both_sides=0. Common pattern: low-line K
  // props (Anthony Kay 3.5) only have Over priced because Under is too
  // long-shot to be open.
  stages.push(`sides:over=${overRows.length},under=${underRows.length}`);

  // Per-book de-vig: pair Over/Under from the same book, devig with the
  // existing 2-way helper, then average fair probs across books.
  // NOTE: deVig2Way returns an ARRAY [fair1, fair2], not an object.
  const fairProbsOver = [];
  const fairProbsUnder = [];
  const viggedProbsOver = [];
  const viggedProbsUnder = [];
  for (const book of books) {
    const o = overRows.find(r => r.sportsbook === book);
    const u = underRows.find(r => r.sportsbook === book);
    if (!o || !u) continue;
    const oProb = americanToImpliedProb(o.odds_american);
    const uProb = americanToImpliedProb(u.odds_american);
    if (oProb == null || uProb == null) continue;
    viggedProbsOver.push(oProb);
    viggedProbsUnder.push(uProb);
    const dv = deVig2Way(oProb, uProb);
    if (Array.isArray(dv) && dv.length === 2 && Number.isFinite(dv[0]) && Number.isFinite(dv[1])) {
      fairProbsOver.push(dv[0]);
      fairProbsUnder.push(dv[1]);
    }
  }
  const avg = (arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
  const { fairProbOver, fairProbUnder } = _applyPropHeavyFavFloor(
    avg(fairProbsOver), avg(fairProbsUnder),
    avg(viggedProbsOver), avg(viggedProbsUnder)
  );

  return {
    matchedRows: lineRows,
    books,
    sides: { over: overRows, under: underRows },
    fairProbOver,
    fairProbUnder,
    booksWithBothSides: fairProbsOver.length, // count of books that had both Over+Under
    resolvedEventId,
    fetchedAt: sportCache.fetchedAt || null, // for downstream stale checks
    stages,
  };
}

// Heavy-favorite floor for prop fair probs. Proportional de-vig systematically
// underestimates the true prob on lopsided 2-way prop markets (the books'
// vigged price already captures information the proportional split can't
// recover — e.g. hitter Over 0.5 hits at -200 / +160 vigged-implied
// 67%/38%, summed 105% overround → naive de-vig over = 67/105 = 64%, but
// the true prob is closer to the vigged 67% because the bookmaker's line
// reflects player batting average, lineup spot, weather, etc. that the
// de-vig can't see). Floor each side's fair at avg book vigged minus a
// small buffer when the side is a heavy favorite (>threshold). Symmetric:
// fires for either Over or Under, whichever side is the heavy fav.
//
// Returns { fairProbOver, fairProbUnder } with the floor applied (or
// passes through unchanged on non-heavy-fav legs).
function _applyPropHeavyFavFloor(devigOver, devigUnder, viggedOver, viggedUnder) {
  const cfg = require('../config').config;
  const thresh = (cfg && cfg.pricing && cfg.pricing.propHeavyFavFloorThresh) || 0.60;
  const buffer = (cfg && cfg.pricing && cfg.pricing.propHeavyFavFloorBuffer) || 0.005;
  let outOver = devigOver, outUnder = devigUnder;
  if (devigOver != null && viggedOver != null && devigOver > thresh) {
    const floor = viggedOver - buffer;
    if (floor > devigOver) outOver = floor;
  }
  if (devigUnder != null && viggedUnder != null && devigUnder > thresh) {
    const floor = viggedUnder - buffer;
    if (floor > devigUnder) outUnder = floor;
  }
  return { fairProbOver: outOver, fairProbUnder: outUnder };
}

// ---------------------------------------------------------------------------
// NBA PLAYER POINTS PROP LOOKUP (Phase 1 shadow-pricing target)
// ---------------------------------------------------------------------------
// Mirror of lookupPlayerStrikeoutProp but for SharpAPI's player_points
// market_type. NBA player names don't carry the " Thrown" / " Recorded"
// suffix that K-props have, so the player-name match is straightforward.
// Returns the same shape: { fairProbOver, fairProbUnder, books,
// booksWithBothSides, resolvedEventId, fetchedAt, stages, error?, ... }.
function lookupPlayerPointsProp(sport, pxEventInfo, playerName, line) {
  const stages = [];
  if (!sport || !pxEventInfo || !playerName) {
    return { error: 'missing_input', stages: ['precondition'] };
  }
  const sportCache = propRowsCache[sport];
  if (!sportCache || !sportCache.player_points) {
    return { error: 'no_prop_cache', stages: ['cache_empty'] };
  }
  const allRows = sportCache.player_points;
  stages.push(`cache:${allRows.length}rows`);

  // Step 1: filter by event — match on home/away team last-2-words +
  // start-time proximity for back-to-backs (rare in NBA but possible).
  const lastWords = (name, n = 2) => {
    const words = normalizeTeamName(name).split(/\s+/).filter(Boolean);
    return words.slice(-n).join(' ');
  };
  const pxHomeKey = lastWords(pxEventInfo.homeTeam || '');
  const pxAwayKey = lastWords(pxEventInfo.awayTeam || '');
  const pxStartMs = pxEventInfo.startTime ? Date.parse(pxEventInfo.startTime) : null;
  stages.push(`px:${pxEventInfo.awayTeam || '?'}@${pxEventInfo.homeTeam || '?'}`);
  const teamMatchRows = allRows.filter(r => {
    const rh = lastWords(r.home_team || '');
    const ra = lastWords(r.away_team || '');
    return (rh === pxHomeKey && ra === pxAwayKey) ||
           (rh === pxAwayKey && ra === pxHomeKey);
  });
  stages.push(`team_match:${teamMatchRows.length}`);
  if (teamMatchRows.length === 0) {
    const availableEvents = [...new Set(allRows.map(r => `${r.away_team}@${r.home_team}`))];
    stages.push(`available:${availableEvents.slice(0, 8).join('|')}`);
    return { error: 'no_event_match', stages, availableEvents };
  }

  // Multi-event narrowing (uncommon for NBA single-game, but defensive).
  let eventRows = teamMatchRows;
  if (pxStartMs) {
    const eventIds = [...new Set(teamMatchRows.map(r => r.event_id))];
    if (eventIds.length > 1) {
      const eventsByDist = eventIds.map(eid => {
        const sample = teamMatchRows.find(r => r.event_id === eid);
        const eMs = sample.event_start_time ? Date.parse(sample.event_start_time) : 0;
        return { eid, dist: Math.abs(eMs - pxStartMs) };
      }).sort((a, b) => a.dist - b.dist);
      const bestId = eventsByDist[0].eid;
      eventRows = teamMatchRows.filter(r => r.event_id === bestId);
      stages.push(`multi_event_resolve:${eventsByDist.length}->${bestId}`);
    }
  }
  const resolvedEventId = eventRows[0].event_id;

  // Step 2: filter by player_name. NBA names are typically full + clean
  // (e.g. "LeBron James"). Strip diacritics ("Nikola Jokić" → "Nikola
  // Jokic") since SharpAPI may not preserve them. Tolerant substring
  // match in both directions to handle Jr./III suffix differences.
  const stripDiacritics = (s) => (s || '').normalize('NFD').replace(/[̀-ͯ]/g, '');
  const normPlayer = stripDiacritics(playerName).toLowerCase().trim();
  const matchedRows = eventRows.filter(r => {
    const stripped = stripDiacritics(r.player_name || '').toLowerCase().trim();
    return stripped === normPlayer ||
           stripped.includes(normPlayer) ||
           normPlayer.includes(stripped);
  });
  stages.push(`player_match:${matchedRows.length}`);
  if (matchedRows.length === 0) {
    return { error: 'no_player_match', stages, resolvedEventId,
             samplePlayers: [...new Set(eventRows.map(r => r.player_name))].slice(0, 5) };
  }

  // Step 3: filter by line value (allow tiny float fuzz)
  const lineRows = line == null ? matchedRows : matchedRows.filter(r => Math.abs((r.line || 0) - line) < 0.01);
  stages.push(`line_match:${lineRows.length}`);
  if (lineRows.length === 0) {
    return { error: 'no_line_match', stages, resolvedEventId,
             sampleLines: [...new Set(matchedRows.map(r => r.line))].slice(0, 8) };
  }

  // Step 4: split by side and per-book de-vig
  const overRows = lineRows.filter(r => /over/i.test(r.selection || r.selection_type || ''));
  const underRows = lineRows.filter(r => /under/i.test(r.selection || r.selection_type || ''));
  const books = [...new Set(lineRows.map(r => r.sportsbook).filter(Boolean))];
  stages.push(`sides:over=${overRows.length},under=${underRows.length}`);

  const fairProbsOver = [];
  const fairProbsUnder = [];
  const viggedProbsOver = [];
  const viggedProbsUnder = [];
  for (const book of books) {
    const o = overRows.find(r => r.sportsbook === book);
    const u = underRows.find(r => r.sportsbook === book);
    if (!o || !u) continue;
    const oProb = americanToImpliedProb(o.odds_american);
    const uProb = americanToImpliedProb(u.odds_american);
    if (oProb == null || uProb == null) continue;
    viggedProbsOver.push(oProb);
    viggedProbsUnder.push(uProb);
    const dv = deVig2Way(oProb, uProb);
    if (Array.isArray(dv) && dv.length === 2 && Number.isFinite(dv[0]) && Number.isFinite(dv[1])) {
      fairProbsOver.push(dv[0]);
      fairProbsUnder.push(dv[1]);
    }
  }
  const avg = (arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
  const { fairProbOver, fairProbUnder } = _applyPropHeavyFavFloor(
    avg(fairProbsOver), avg(fairProbsUnder),
    avg(viggedProbsOver), avg(viggedProbsUnder)
  );

  return {
    matchedRows: lineRows,
    books,
    sides: { over: overRows, under: underRows },
    fairProbOver,
    fairProbUnder,
    booksWithBothSides: fairProbsOver.length,
    resolvedEventId,
    fetchedAt: sportCache.fetchedAt || null,
    stages,
  };
}

// ---------------------------------------------------------------------------
// THE ODDS API FALLBACK for player props (Phase 1 supplemental source)
// ---------------------------------------------------------------------------
// SharpAPI Hobby tier exposes pitcher_strikeouts for only ~4 of ~15 MLB
// games per slate (filter logic unclear — possibly top-N by liquidity).
// The Odds API has full coverage from 4-5 books for the games SharpAPI
// misses, so we fall back to it on no_event_match.
//
// Cost: TOA charges ~1 credit per market×region×event. With aggressive
// caching (5min TTL on events list + per-event odds), a typical day's
// MLB slate uses well under 100 credits — fits comfortably in the free
// 500/mo tier or the $30/mo 20K-credit tier.
const TOA_PROP_TTL_MS = 5 * 60 * 1000;
// Refresh-ahead window: when a cached entry is OLDER than this but
// still YOUNGER than TOA_PROP_TTL_MS, return the cached value
// immediately AND fire a background refresh (fire-and-forget). This
// eliminates the synchronous cache-miss latency (100-150ms HTTP RTT)
// for any prop with traffic > 1 hit per (TTL - REFRESH_AHEAD) window.
// Without this, the prop bridge blocks on a fresh fetch every 5min
// per (sport, event, market), driving phase-2 P95 to ~40ms and P99
// past 100ms — measurable in /latency-breakdown.
const TOA_PROP_REFRESH_AHEAD_MS = 3 * 60 * 1000;
const toaEventsCache = {};   // { sportKey: { fetchedAt, events: [...], refreshing: bool } }
const toaPropOddsCache = {}; // { `${sport}:${eventId}:${marketKey}`: { fetchedAt, refreshing: bool, ...respBody } }

// Map our internal sport keys to TOA sport keys. They happen to match
// for MLB but kept explicit for future expansion.
const TOA_SPORT_KEYS = {
  'baseball_mlb': 'baseball_mlb',
  'basketball_nba': 'basketball_nba',
};

// Internal: do the actual TOA events fetch + cache write. Used by both
// the synchronous cache-miss block path and the background refresh-ahead
// path inside _getTheOddsApiEvents.
async function _refreshTheOddsApiEvents(sportKey) {
  const apiKey = process.env.THE_ODDS_API_KEY;
  const url = `https://api.the-odds-api.com/v4/sports/${sportKey}/events?apiKey=${apiKey}`;
  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      log.warn('OddsFeed', `TOA events fetch failed: ${resp.status}`);
      if (toaEventsCache[sportKey]) toaEventsCache[sportKey].refreshing = false;
      return null;
    }
    const events = await resp.json();
    if (!Array.isArray(events)) {
      if (toaEventsCache[sportKey]) toaEventsCache[sportKey].refreshing = false;
      return null;
    }
    toaEventsCache[sportKey] = { fetchedAt: Date.now(), events, refreshing: false };
    return events;
  } catch (err) {
    log.warn('OddsFeed', `TOA events error: ${err.message}`);
    if (toaEventsCache[sportKey]) toaEventsCache[sportKey].refreshing = false;
    return null;
  }
}

async function _getTheOddsApiEvents(sport) {
  const apiKey = process.env.THE_ODDS_API_KEY;
  if (!apiKey) return null;
  const sportKey = TOA_SPORT_KEYS[sport] || sport;
  const now = Date.now();
  const cached = toaEventsCache[sportKey];
  // Fresh: return immediately, no refresh
  if (cached && (now - cached.fetchedAt) < TOA_PROP_REFRESH_AHEAD_MS) return cached.events;
  // Stale-but-usable: return cached AND fire background refresh
  if (cached && (now - cached.fetchedAt) < TOA_PROP_TTL_MS) {
    if (!cached.refreshing) {
      cached.refreshing = true;
      _refreshTheOddsApiEvents(sportKey).catch(() => {
        if (toaEventsCache[sportKey]) toaEventsCache[sportKey].refreshing = false;
      });
    }
    return cached.events;
  }
  // Cache miss or fully expired: block on fetch
  const events = await _refreshTheOddsApiEvents(sportKey);
  return events != null ? events : (cached ? cached.events : null);
}

// Internal: TOA per-event prop-odds fetch + cache write. Same dual-use
// pattern as _refreshTheOddsApiEvents.
async function _refreshTheOddsApiPropOdds(sport, eventId, marketKey) {
  const apiKey = process.env.THE_ODDS_API_KEY;
  const sportKey = TOA_SPORT_KEYS[sport] || sport;
  const cacheKey = `${sportKey}:${eventId}:${marketKey}`;
  const url = `https://api.the-odds-api.com/v4/sports/${sportKey}/events/${eventId}/odds`
    + `?apiKey=${apiKey}&regions=us&markets=${marketKey}&oddsFormat=american`;
  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      log.warn('OddsFeed', `TOA per-event odds failed (${eventId}/${marketKey}): ${resp.status}`);
      if (toaPropOddsCache[cacheKey]) toaPropOddsCache[cacheKey].refreshing = false;
      return null;
    }
    const data = await resp.json();
    toaPropOddsCache[cacheKey] = { fetchedAt: Date.now(), refreshing: false, ...data };
    return toaPropOddsCache[cacheKey];
  } catch (err) {
    log.warn('OddsFeed', `TOA per-event odds error: ${err.message}`);
    if (toaPropOddsCache[cacheKey]) toaPropOddsCache[cacheKey].refreshing = false;
    return null;
  }
}

async function _getTheOddsApiPropOdds(sport, eventId, marketKey) {
  const apiKey = process.env.THE_ODDS_API_KEY;
  if (!apiKey) return null;
  const sportKey = TOA_SPORT_KEYS[sport] || sport;
  const cacheKey = `${sportKey}:${eventId}:${marketKey}`;
  const now = Date.now();
  const cached = toaPropOddsCache[cacheKey];
  // Fresh: return immediately, no refresh
  if (cached && (now - cached.fetchedAt) < TOA_PROP_REFRESH_AHEAD_MS) return cached;
  // Stale-but-usable: return cached AND fire background refresh
  if (cached && (now - cached.fetchedAt) < TOA_PROP_TTL_MS) {
    if (!cached.refreshing) {
      cached.refreshing = true;
      _refreshTheOddsApiPropOdds(sport, eventId, marketKey).catch(() => {
        if (toaPropOddsCache[cacheKey]) toaPropOddsCache[cacheKey].refreshing = false;
      });
    }
    return cached;
  }
  // Cache miss or fully expired: block on fetch
  return await _refreshTheOddsApiPropOdds(sport, eventId, marketKey);
}

// Generic TOA player-prop lookup. Works for any TOA market key with
// player Over/Under outcomes shaped as {description: playerName, name:
// 'Over'|'Under', point: line, price: american}. Used by the wrappers
// below for pitcher_strikeouts, player_points, player_rebounds,
// player_assists, player_threes, etc.
//
// Returns the standard shape:
//   { fairProbOver, fairProbUnder, books, booksWithBothSides,
//     resolvedEventId, matchedRows, stages }
// or { error, stages, ... } on failure.
async function lookupTheOddsApiPlayerProp(sport, marketKey, pxEventInfo, playerName, line) {
  const stages = [];
  if (!sport || !marketKey || !pxEventInfo || !playerName) {
    return { error: 'missing_input', stages: ['precondition'] };
  }
  if (!process.env.THE_ODDS_API_KEY) {
    return { error: 'toa_key_missing', stages: ['no_api_key'] };
  }

  const events = await _getTheOddsApiEvents(sport);
  if (!events) return { error: 'toa_events_fail', stages: ['events_fetch_failed'] };
  stages.push(`toa_events:${events.length}`);

  const lastWords = (name, n = 2) => {
    const words = normalizeTeamName(name).split(/\s+/).filter(Boolean);
    return words.slice(-n).join(' ');
  };
  const pxHomeKey = lastWords(pxEventInfo.homeTeam || '');
  const pxAwayKey = lastWords(pxEventInfo.awayTeam || '');
  stages.push(`px:${pxEventInfo.awayTeam || '?'}@${pxEventInfo.homeTeam || '?'}`);
  const matchingEvents = events.filter(e => {
    const eh = lastWords(e.home_team || '');
    const ea = lastWords(e.away_team || '');
    return (eh === pxHomeKey && ea === pxAwayKey) ||
           (eh === pxAwayKey && ea === pxHomeKey);
  });
  stages.push(`event_match:${matchingEvents.length}`);
  if (matchingEvents.length === 0) {
    return { error: 'no_event_match', stages,
             availableEvents: events.slice(0, 8).map(e => `${e.away_team}@${e.home_team}`) };
  }

  // Disambiguate doubleheaders/back-to-backs by start-time proximity.
  const pxStartMs = pxEventInfo.startTime ? Date.parse(pxEventInfo.startTime) : null;
  let event = matchingEvents[0];
  if (pxStartMs && matchingEvents.length > 1) {
    matchingEvents.sort((a, b) =>
      Math.abs(Date.parse(a.commence_time) - pxStartMs) -
      Math.abs(Date.parse(b.commence_time) - pxStartMs));
    event = matchingEvents[0];
  }

  const odds = await _getTheOddsApiPropOdds(sport, event.id, marketKey);
  if (!odds) return { error: 'toa_odds_fetch_fail', stages, resolvedEventId: event.id };
  const bookmakers = odds.bookmakers || [];
  stages.push(`books_in_resp:${bookmakers.length}`);

  const stripDiacritics = (s) => (s || '').normalize('NFD').replace(/[̀-ͯ]/g, '');
  // Normalize player names: strip diacritics, periods, apostrophes,
  // collapse whitespace. Books vary on "C.J." vs "CJ", "D'Angelo" vs
  // "DAngelo", etc.
  const normPlayerName = (s) => stripDiacritics(s || '').toLowerCase()
    .replace(/[.'`]/g, '').replace(/\s+/g, ' ').trim();
  const normPlayer = normPlayerName(playerName);
  const matched = []; // {book, side, point, price}
  for (const bk of bookmakers) {
    const market = (bk.markets || []).find(m => m.key === marketKey);
    if (!market) continue;
    for (const o of (market.outcomes || [])) {
      const outcomePlayer = normPlayerName(o.description);
      const playerOk = outcomePlayer === normPlayer ||
                       outcomePlayer.includes(normPlayer) ||
                       normPlayer.includes(outcomePlayer);
      const lineOk = line == null || Math.abs((o.point || 0) - line) < 0.01;
      if (playerOk && lineOk) {
        matched.push({ book: bk.key, side: o.name, point: o.point, price: o.price });
      }
    }
  }
  stages.push(`player_line_match:${matched.length}`);

  // Identify the primary line for this player (line with the most book
  // coverage = the consensus anchor) so we can reject deep alts that
  // are exploit-vulnerable. Books only post the primary at ~-110/-110;
  // the deeper the alt the thinner the coverage AND the more sensitive
  // to thin de-vig errors.
  //
  // The cap is propAltLineMaxDistance (default ±2 stat units). Set to
  // a very large value to disable; set to 0 to allow only the primary.
  if (line != null) {
    const cfgConfig = require('../config').config;
    const maxDist = cfgConfig && cfgConfig.pricing && cfgConfig.pricing.propAltLineMaxDistance;
    if (maxDist != null && Number.isFinite(maxDist)) {
      const lineCounts = {};
      for (const bk of bookmakers) {
        const market = (bk.markets || []).find(m => m.key === marketKey);
        if (!market) continue;
        for (const o of (market.outcomes || [])) {
          const outcomePlayer = normPlayerName(o.description);
          const playerOk = outcomePlayer === normPlayer ||
                           outcomePlayer.includes(normPlayer) ||
                           normPlayer.includes(outcomePlayer);
          if (!playerOk || o.point == null) continue;
          const k = String(o.point);
          lineCounts[k] = (lineCounts[k] || 0) + 1;
        }
      }
      let primaryLine = null;
      let primaryCount = 0;
      for (const [k, n] of Object.entries(lineCounts)) {
        if (n > primaryCount) { primaryLine = parseFloat(k); primaryCount = n; }
      }
      if (primaryLine != null) {
        const dist = Math.abs(line - primaryLine);
        stages.push(`primary_line:${primaryLine},dist:${dist.toFixed(1)},max:${maxDist}`);
        if (dist > maxDist) {
          return {
            error: 'alt_line_too_far',
            stages, resolvedEventId: event.id,
            requestedLine: line,
            primaryLine,
            distance: dist,
            maxDistance: maxDist,
          };
        }
      }
    }
  }

  if (matched.length === 0) {
    return { error: 'no_player_or_line_match', stages, resolvedEventId: event.id,
             samplePlayers: [...new Set(
               bookmakers.flatMap(bk =>
                 (bk.markets || []).flatMap(m =>
                   (m.outcomes || []).map(o => o.description))).filter(Boolean))].slice(0, 5) };
  }

  // Per-book Over/Under devig
  const overByBook = {};
  const underByBook = {};
  for (const m of matched) {
    if (/over/i.test(m.side)) overByBook[m.book] = m;
    else if (/under/i.test(m.side)) underByBook[m.book] = m;
  }
  const books = [...new Set(matched.map(m => m.book))];
  stages.push(`sides:over=${Object.keys(overByBook).length},under=${Object.keys(underByBook).length}`);

  const fairProbsOver = [];
  const fairProbsUnder = [];
  const viggedProbsOver = [];
  const viggedProbsUnder = [];
  for (const book of books) {
    const o = overByBook[book];
    const u = underByBook[book];
    if (!o || !u) continue;
    const oProb = americanToImpliedProb(o.price);
    const uProb = americanToImpliedProb(u.price);
    if (oProb == null || uProb == null) continue;
    viggedProbsOver.push(oProb);
    viggedProbsUnder.push(uProb);
    const dv = deVig2Way(oProb, uProb);
    if (Array.isArray(dv) && dv.length === 2 && Number.isFinite(dv[0]) && Number.isFinite(dv[1])) {
      fairProbsOver.push(dv[0]);
      fairProbsUnder.push(dv[1]);
    }
  }
  const avg = (arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
  const { fairProbOver, fairProbUnder } = _applyPropHeavyFavFloor(
    avg(fairProbsOver), avg(fairProbsUnder),
    avg(viggedProbsOver), avg(viggedProbsUnder)
  );

  return {
    matchedRows: matched,
    books,
    fairProbOver,
    fairProbUnder,
    booksWithBothSides: fairProbsOver.length,
    resolvedEventId: event.id,
    fetchedAt: odds.fetchedAt || null,
    stages,
  };
}

// TOA equivalent of lookupPlayerStrikeoutProp. Returns the same shape
// so the websocket caller can swap them transparently. Async because
// TOA requires HTTP calls (cached, but not pre-warmed).
async function lookupPlayerStrikeoutPropFromTheOddsApi(sport, pxEventInfo, playerName, line) {
  const stages = [];
  if (!sport || !pxEventInfo || !playerName) {
    return { error: 'missing_input', stages: ['precondition'] };
  }
  if (!process.env.THE_ODDS_API_KEY) {
    return { error: 'toa_key_missing', stages: ['no_api_key'] };
  }

  const events = await _getTheOddsApiEvents(sport);
  if (!events) return { error: 'toa_events_fail', stages: ['events_fetch_failed'] };
  stages.push(`toa_events:${events.length}`);

  // Match event by team last-words (same approach as SharpAPI helper).
  const lastWords = (name, n = 2) => {
    const words = normalizeTeamName(name).split(/\s+/).filter(Boolean);
    return words.slice(-n).join(' ');
  };
  const pxHomeKey = lastWords(pxEventInfo.homeTeam || '');
  const pxAwayKey = lastWords(pxEventInfo.awayTeam || '');
  stages.push(`px:${pxEventInfo.awayTeam || '?'}@${pxEventInfo.homeTeam || '?'}`);
  const matchingEvents = events.filter(e => {
    const eh = lastWords(e.home_team || '');
    const ea = lastWords(e.away_team || '');
    return (eh === pxHomeKey && ea === pxAwayKey) ||
           (eh === pxAwayKey && ea === pxHomeKey);
  });
  stages.push(`event_match:${matchingEvents.length}`);
  if (matchingEvents.length === 0) {
    return { error: 'no_event_match', stages,
             availableEvents: events.slice(0, 8).map(e => `${e.away_team}@${e.home_team}`) };
  }

  // Disambiguate doubleheaders by start-time proximity.
  const pxStartMs = pxEventInfo.startTime ? Date.parse(pxEventInfo.startTime) : null;
  let event = matchingEvents[0];
  if (pxStartMs && matchingEvents.length > 1) {
    matchingEvents.sort((a, b) =>
      Math.abs(Date.parse(a.commence_time) - pxStartMs) -
      Math.abs(Date.parse(b.commence_time) - pxStartMs));
    event = matchingEvents[0];
  }

  const odds = await _getTheOddsApiPropOdds(sport, event.id, 'pitcher_strikeouts');
  if (!odds) return { error: 'toa_odds_fetch_fail', stages, resolvedEventId: event.id };
  const bookmakers = odds.bookmakers || [];
  stages.push(`books_in_resp:${bookmakers.length}`);

  // Filter outcomes by player name (description field) + line value (point).
  // TOA's "description" is clean — no "Thrown"/"Recorded" suffix like SharpAPI.
  const stripDiacritics = (s) => (s || '').normalize('NFD').replace(/[̀-ͯ]/g, '');
  const normPlayer = stripDiacritics(playerName).toLowerCase().trim();
  const matched = []; // {book, side, point, price}
  for (const bk of bookmakers) {
    const market = (bk.markets || []).find(m => m.key === 'pitcher_strikeouts');
    if (!market) continue;
    for (const o of (market.outcomes || [])) {
      const outcomePlayer = stripDiacritics(o.description || '').toLowerCase().trim();
      const playerOk = outcomePlayer === normPlayer ||
                       outcomePlayer.includes(normPlayer) ||
                       normPlayer.includes(outcomePlayer);
      const lineOk = line == null || Math.abs((o.point || 0) - line) < 0.01;
      if (playerOk && lineOk) {
        matched.push({ book: bk.key, side: o.name, point: o.point, price: o.price });
      }
    }
  }
  stages.push(`player_line_match:${matched.length}`);
  if (matched.length === 0) {
    return { error: 'no_player_or_line_match', stages, resolvedEventId: event.id,
             samplePlayers: [...new Set(
               bookmakers.flatMap(bk =>
                 (bk.markets || []).flatMap(m =>
                   (m.outcomes || []).map(o => o.description))).filter(Boolean))].slice(0, 5) };
  }

  // Per-book Over/Under devig.
  const overByBook = {};
  const underByBook = {};
  for (const m of matched) {
    if (/over/i.test(m.side)) overByBook[m.book] = m;
    else if (/under/i.test(m.side)) underByBook[m.book] = m;
  }
  const books = [...new Set(matched.map(m => m.book))];
  stages.push(`sides:over=${Object.keys(overByBook).length},under=${Object.keys(underByBook).length}`);

  const fairProbsOver = [];
  const fairProbsUnder = [];
  for (const book of books) {
    const o = overByBook[book];
    const u = underByBook[book];
    if (!o || !u) continue;
    const oProb = americanToImpliedProb(o.price);
    const uProb = americanToImpliedProb(u.price);
    if (oProb == null || uProb == null) continue;
    const dv = deVig2Way(oProb, uProb);
    if (Array.isArray(dv) && dv.length === 2 && Number.isFinite(dv[0]) && Number.isFinite(dv[1])) {
      fairProbsOver.push(dv[0]);
      fairProbsUnder.push(dv[1]);
    }
  }
  const avg = (arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;

  return {
    matchedRows: matched,
    books,
    fairProbOver: avg(fairProbsOver),
    fairProbUnder: avg(fairProbsUnder),
    booksWithBothSides: fairProbsOver.length,
    resolvedEventId: event.id,
    fetchedAt: odds.fetchedAt || null, // for downstream stale checks
    stages,
  };
}

// Debug: dump the prop cache for inspection
function getPropRowsCacheStatus() {
  const out = {};
  for (const [sport, mtMap] of Object.entries(propRowsCache)) {
    out[sport] = {};
    for (const [mt, arr] of Object.entries(mtMap)) {
      if (mt === 'fetchedAt') { out[sport].fetchedAt = arr; continue; }
      out[sport][mt] = {
        rowCount: arr.length,
        eventCount: new Set(arr.map(r => r.event_id)).size,
        bookCount: new Set(arr.map(r => r.sportsbook)).size,
        books: [...new Set(arr.map(r => r.sportsbook))],
      };
    }
  }
  return out;
}

module.exports = {
  fetchOddsForSport,
  refreshAllSports,
  getFairProb,
  getFairProbAsync,
  getAltLineFairProbSync,
  getAltSyncStats,
  verifyLineWithPinnacle,
  getPinnacleOdds,
  getDisplayFairProb,
  getFanDuelOdds,
  getKalshiOdds,
  getDraftKingsOdds,
  getDNBFairProb,
  fetchAltLines,
  backfillMissingH2h,
  mergeDkMmaFights,
  mergeDkLiveOdds,
  mergeOddsApiLive,
  warmAltLines,
  warmAllSports,
  warmEventAltLinesJIT,
  startAltLineWarmLoop,
  startBovadaAltLoop,
  startPinVerifyWarmLoop,
  getAltLinesWarmStats,
  getJitWarmStats,
  getSupplementRetryStats,
  getPinVerifyWarmStats,
  getEventMarkets,
  getGolfMatchupEvent,
  getLiveEventMarkets,
  getLiveFairProb,
  getLiveCacheStatus,
  getCacheAge,
  isStale,
  getStaleThreshold,
  isEventStalePreGame,
  getCacheStatus,
  getAllCachedEvents,
  __debugGetCache,
  captureClosingLines,
  getClosingLineSnapshot,
  getClosingLinesStatus,
  getSharpEvents,
  refreshAllSportsDelta,
  normalizeTeamName,
  deVig2Way,
  americanToImpliedProb,
  fetchScores,
  getGameResult,
  checkLineupFreshness,
  getLineupCache,
  getPitcherSide,
  __debugGetAltLinesCache: () => altLinesCache,
  normalizeEventKey,
  getAltLineCacheEntry,
  getTennisTotalsFallback,
  // Phase 1 player-prop shadow pricing
  lookupPlayerStrikeoutProp,
  lookupPlayerStrikeoutPropFromTheOddsApi,
  lookupTheOddsApiPlayerProp,
  lookupPlayerPointsProp,
  getPropRowsCacheStatus,
};
