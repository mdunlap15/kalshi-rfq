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
};
// NOTE: tennis removed from LEAGUE_MAP — SharpAPI returns events but zero bookmaker
// odds for tennis. Routed through The Odds API fallback instead (dynamic tournament discovery).

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
  // Check if this sport uses The Odds API fallback
  if (ODDS_API_FALLBACK[sport]) {
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
  const marketTypesList = {
    'baseball_mlb': ['moneyline', 'run_line', 'total_runs', 'team_total', '1st_5_innings_moneyline', '1st_5_innings_run_line'],
    'icehockey_nhl': ['moneyline', 'puck_line', 'total_goals', 'team_total'],
    'basketball_nba': ['moneyline', 'point_spread', 'total_points', 'team_total'],
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
        const body = await resp.json();
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

  // Group by event, then by market+selection to de-vig across books
  const eventMap = {};
  for (const row of rows) {
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
  }

  // Supplement with 1st-Half markets for NBA (separate from full-game)
  if (!liveMode && sport === 'basketball_nba') {
    try {
      await supplementNbaH1Markets(parsed);
    } catch (err) {
      log.warn('OddsFeed', `NBA H1 supplement failed: ${err.message}`);
    }
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
  }

  log.info('OddsFeed', `Cached ${Object.keys(parsed).length} ${liveMode ? 'LIVE ' : ''}events for ${mapping.value}`);
  return parsed;
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
          if (mk) { ev.markets.h2h_f5 = mk; h2hCount++; }
        }
        if (spreadPairs.length > 0) {
          const sp = buildConsensusSpread(spreadPairs);
          if (sp) { ev.markets.spreads_f5 = sp; spreadCount++; }
        }
        if (totalPairs.length > 0) {
          ev.markets.totals_f5 = buildConsensusTotals(totalPairs);
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
          if (mk) { ev.markets.h2h_h1 = mk; h2hCount++; }
        }
        if (spreadPairs.length > 0) {
          const sp = buildConsensusSpread(spreadPairs);
          if (sp) { ev.markets.spreads_h1 = sp; spreadCount++; }
        }
        if (totalPairs.length > 0) {
          ev.markets.totals_h1 = buildConsensusTotals(totalPairs);
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

      const url = `https://api.the-odds-api.com/v4/sports/${oaSport}/events/${resolved.eventId}/odds`
        + `?apiKey=${theOddsApiKey}`
        + `&regions=us,eu`
        + `&markets=team_totals`
        + `&bookmakers=pinnacle,draftkings,fanduel`
        + `&oddsFormat=american`;

      try {
        const resp = await fetch(url);
        calls++;
        if (!resp.ok) { apiFails++; continue; }
        const data = await resp.json();

        // Build bookPairs: one entry per (book × teamSide) with { over, under }.
        // Per-event response shape: data.bookmakers[].markets[].outcomes[] where
        // each outcome has { name:'Over'|'Under', description:<team>, price, point }.
        const bookPairs = [];
        for (const book of (data.bookmakers || [])) {
          for (const m of (book.markets || [])) {
            if (m.key !== 'team_totals') continue;
            const byTeam = {};
            for (const o of (m.outcomes || [])) {
              const team = o.description;
              if (!team) continue;
              if (!byTeam[team]) byTeam[team] = {};
              if (o.name === 'Over') {
                byTeam[team].over = {
                  odds_probability: americanToImpliedProb(o.price),
                  odds_american: o.price,
                  line: o.point,
                };
              } else if (o.name === 'Under') {
                byTeam[team].under = {
                  odds_probability: americanToImpliedProb(o.price),
                  odds_american: o.price,
                  line: o.point,
                };
              }
            }
            for (const [team, pair] of Object.entries(byTeam)) {
              if (!pair.over || !pair.under) continue;
              let teamSide = null;
              if (team === data.home_team) teamSide = 'home';
              else if (team === data.away_team) teamSide = 'away';
              else continue;
              bookPairs.push({
                book: book.key,
                teamSide,
                over: pair.over,
                under: pair.under,
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

    const events = await resp.json();
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
  const allSports = await sportsResp.json();
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
      const events = await resp.json();
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

  const events = await resp.json();
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
  const byBook = {};
  for (const row of odds) {
    if (!byBook[row.sportsbook]) byBook[row.sportsbook] = {};
    byBook[row.sportsbook][row.selection_type] = row;
  }
  return Object.entries(byBook)
    .filter(([_, sides]) => sides.over && sides.under)
    .map(([book, sides]) => ({ book, over: sides.over, under: sides.under }));
}

/**
 * Group team_total odds by sportsbook and team side (home/away).
 * SharpAPI team_total selection_type: "home_over", "home_under", "away_over", "away_under"
 */
function getBookPairsForTeamTotals(odds) {
  const byBookSide = {};
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
    const key = `${row.sportsbook}|${side}`;
    if (!byBookSide[key]) byBookSide[key] = {};
    byBookSide[key][dir] = row;
  }
  return Object.entries(byBookSide)
    .filter(([_, sides]) => sides.over && sides.under)
    .map(([key, sides]) => {
      const [book, teamSide] = key.split('|');
      return { book, teamSide, over: sides.over, under: sides.under };
    });
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

  // Check cache
  const cached = altLinesCache[key];
  if (cached && (Date.now() - cached.fetchedAt) < ALT_LINES_TTL_MS) {
    return cached;
  }

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
  // Include the PRIMARY totals market alongside alternate_totals so lines
  // that are a book's primary (e.g. Pinnacle's integer MLB 8) — and thus
  // not listed in alternate_totals — still land in the altTotals cache.
  // Books that skip integer totals in their alt list won't cover Over 8
  // otherwise.
  const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/events/${eventId}/odds`
    + `?apiKey=${theOddsApiKey}`
    + `&regions=us,eu`
    + `&markets=totals,alternate_spreads,alternate_totals${mlbF5Markets}`
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
    const result = { fetchedAt: Date.now(), altSpreads: {}, altTotals: {}, altSpreadsF5: {}, altTotalsF5: {} };

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

    altLinesCache[key] = result;
    const skippedNote = (skippedThinSpreads + skippedThinTotals) > 0 ? ` (skipped ${skippedThinSpreads} spreads + ${skippedThinTotals} totals with <${ALT_LINES_MIN_BOOKS} books)` : '';
    const f5Note = (Object.keys(result.altSpreadsF5).length || Object.keys(result.altTotalsF5).length) > 0
      ? `, F5: ${Object.keys(result.altSpreadsF5).length} spreads + ${Object.keys(result.altTotalsF5).length} totals`
      : '';
    log.info('OddsFeed', `Cached alt lines: ${Object.keys(result.altSpreads).length} spreads, ${Object.keys(result.altTotals).length} totals${f5Note}${skippedNote}`);
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
  const existingByPair = new Map(); // "a|b" → event object
  for (const entry of Object.values(cache.events || {})) {
    const arr = Array.isArray(entry) ? entry : [entry];
    for (const ev of arr) {
      if (!ev || !ev.homeTeam || !ev.awayTeam) continue;
      const a = lw(ev.homeTeam), b = lw(ev.awayTeam);
      if (a && b) {
        existingByPair.set(a + '|' + b, ev);
        existingByPair.set(b + '|' + a, ev);
      }
    }
  }

  // Helper: build the markets.totals block from a DK fight's totals array.
  function buildTotalsBlock(totals) {
    if (!Array.isArray(totals) || totals.length === 0) return null;
    // Pick a primary line — prefer the middle (median fight duration:
    // 3-round ≈ 2.5, 5-round ≈ 4.5). Remaining lines become alt.
    const sorted = [...totals].sort((a, b) => a.line - b.line);
    const primary = sorted[Math.floor(sorted.length / 2)];
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
    const existing = existingByPair.get(p1 + '|' + p2) || existingByPair.get(p2 + '|' + p1);
    if (existing) {
      // Fight already in cache (typically from SharpAPI h2h). Graft on
      // DK's Total Rounds if the existing entry lacks totals.
      if (!existing.markets) existing.markets = {};
      if (!existing.markets.totals) {
        const block = buildTotalsBlock(fight.totals);
        if (block) {
          existing.markets.totals = block;
          enriched++;
          continue;
        }
      }
      skipped++;
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
  if (!event) return null;

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
        // network fetch. Only applies to totals; spreads have signed
        // home_point bucketing via getAltLineFairProb.
        if (marketType === 'totals' && market.byLine) {
          const byLineEntry = market.byLine[String(absLine)];
          if (byLineEntry) {
            const sideProb = selection === 'over' ? byLineEntry.over?.fairProb : byLineEntry.under?.fairProb;
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
    // Require a line match against the cached primary. Without this, an
    // alt team-total registered by PX (e.g. Lakers Over 114.5) would
    // receive the primary line's fair prob (e.g. Over 115.5) — wrong
    // enough to leak money systematically. Same safeguard rationale as
    // totals/spreads above. Line 0.01 tolerance for float noise.
    if (line == null) {
      log.warn('OddsFeed', `getFairProb: null line for team_totals ${side}_${dir} ${homeTeam} vs ${awayTeam} — declining`);
      return null;
    }
    if (teamData.line != null && Math.abs(teamData.line - line) > 0.01) {
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
    if (parts[1] === 'over') return teamData.over?.displayFairProb || teamData.over?.fairProb || null;
    if (parts[1] === 'under') return teamData.under?.displayFairProb || teamData.under?.fairProb || null;
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
  if (marketType === 'h2h' || marketType === 'spreads') {
    if (selection === 'home') return market.pinnacle.home || null;
    if (selection === 'away') return market.pinnacle.away || null;
  } else if (marketType === 'totals') {
    if (selection === 'over') return market.pinnacle.over || null;
    if (selection === 'under') return market.pinnacle.under || null;
  } else if (marketType === 'h2h_h1' || marketType === 'spreads_h1') {
    if (selection === 'home') return market.pinnacle.home || null;
    if (selection === 'away') return market.pinnacle.away || null;
  } else if (marketType === 'totals_h1') {
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
  if (marketType === 'h2h' || marketType === 'spreads') {
    if (selection === 'home') return market.fanduel.home || null;
    if (selection === 'away') return market.fanduel.away || null;
  } else if (marketType === 'totals') {
    if (selection === 'over') return market.fanduel.over || null;
    if (selection === 'under') return market.fanduel.under || null;
  } else if (marketType === 'h2h_h1' || marketType === 'spreads_h1') {
    if (selection === 'home') return market.fanduel.home || null;
    if (selection === 'away') return market.fanduel.away || null;
  } else if (marketType === 'totals_h1') {
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
  if (marketType === 'h2h' || marketType === 'spreads') {
    if (selection === 'home') return market.kalshi.home || null;
    if (selection === 'away') return market.kalshi.away || null;
  } else if (marketType === 'totals') {
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
  if (marketType === 'h2h' || marketType === 'spreads') {
    if (selection === 'home') return market.draftkings.home || null;
    if (selection === 'away') return market.draftkings.away || null;
  } else if (marketType === 'totals') {
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
  if ((Date.now() - cached.fetchedAt) >= ALT_LINES_TTL_MS) {
    _recordAltSyncMiss('cache_stale', { ...ctx, ageMs: Date.now() - cached.fetchedAt });
    return null;
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
  if (marketType === 'spreads' || marketType === 'spreads_f5') {
    const homePoint = spreadHomePoint(line, selection);
    if (homePoint == null) return null;
    const bucket = isF5 ? alt.altSpreadsF5 : alt.altSpreads;
    return bucket?.[String(homePoint)] || null;
  }
  if (marketType === 'totals' || marketType === 'totals_f5') {
    const bucket = isF5 ? alt.altTotalsF5 : alt.altTotals;
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
  const isF5 = marketType === 'spreads_f5' || marketType === 'totals_f5';
  if (marketType === 'spreads' || marketType === 'spreads_f5') {
    const homePoint = spreadHomePoint(line, selection);
    if (homePoint == null) {
      log.debug('AltLine', `MISS homePoint null: ${eventKey} ${selection} line=${line}`);
      return null;
    }
    const lineKey = String(homePoint);
    const bucket = isF5 ? alt.altSpreadsF5 : alt.altSpreads;
    const lineData = bucket?.[lineKey];
    if (!lineData) {
      const availableKeys = Object.keys(bucket || {}).slice(0, 10).join(', ');
      log.debug('AltLine', `MISS ${marketType}: ${eventKey} ${selection} line=${line} homePoint=${lineKey} — not in cache. Available: [${availableKeys}]`);
      return null;
    }
    const fairProb = selection === 'home' ? (lineData.home || null) : (selection === 'away' ? (lineData.away || null) : null);
    log.debug('AltLine', `HIT ${marketType}: ${eventKey} ${selection} line=${line} homePoint=${lineKey} fair=${fairProb?.toFixed(4) ?? 'null'} books=${lineData.books}`);
    return fairProb;
  } else if (marketType === 'totals' || marketType === 'totals_f5') {
    const bucket = isF5 ? alt.altTotalsF5 : alt.altTotals;
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
  if (marketType === 'spreads') {
    const homePoint = spreadHomePoint(line, selection);
    if (homePoint == null) return null;
    lineData = alt.altSpreads[String(homePoint)];
  } else if (marketType === 'totals') {
    lineData = alt.altTotals[Math.abs(line)];
  } else {
    return null;
  }

  if (!lineData || !lineData.byBook) return null;
  const bookOdds = lineData.byBook[book];
  if (!bookOdds) return null;

  if (marketType === 'spreads') {
    if (selection === 'home') return bookOdds.home != null ? bookOdds.home : null;
    if (selection === 'away') return bookOdds.away != null ? bookOdds.away : null;
  } else if (marketType === 'totals') {
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

function getEventMarkets(sport, homeTeam, awayTeam, targetTime) {
  const sportCache = oddsCache[sport];
  if (!sportCache) return null;
  const key = normalizeEventKey(homeTeam, awayTeam);
  const entry = sportCache.events[key];
  if (!entry) return null;

  // Normalize: SharpAPI stores arrays, Odds API stores single objects
  const events = Array.isArray(entry) ? entry : [entry];
  if (events.length === 0) return null;

  // If only one event or no target time, return first
  if (events.length === 1 || !targetTime) return events[0];

  // Find closest by time
  const targetMs = new Date(targetTime).getTime();
  if (isNaN(targetMs)) return events[0];

  let closest = events[0];
  let closestDiff = Infinity;
  for (const ev of events) {
    const evMs = new Date(ev.commenceTime).getTime();
    if (isNaN(evMs)) continue;
    const diff = Math.abs(evMs - targetMs);
    if (diff < closestDiff) {
      closestDiff = diff;
      closest = ev;
    }
  }
  return closest;
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
        const body = await resp.json();
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
  for (const sport of Object.keys(LEAGUE_MAP)) {
    try {
      await fetchOddsDelta(sport);
    } catch (err) {
      log.warn('OddsFeed', `Delta refresh failed for ${sport}: ${err.message}`);
    }
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
      const body = await resp.json();
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
  // NHL
  'VGK Golden Knights': 'Vegas Golden Knights',
  'LA Kings': 'Los Angeles Kings',
  // Extend here: add any "<3-char-caps> <mascot>" variants we find in logs
};

/**
 * Clean team names from SharpAPI (removes pitcher info like "(TBD)")
 * and canonicalize known abbreviation-prefixed names so they collide
 * with the Odds API / PX full-name versions in the cache.
 */
function cleanTeamName(name) {
  const stripped = (name || '').replace(/\s*\([^)]*\)\s*$/, '').trim();
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

// ---------------------------------------------------------------------------
// SCORES — fetch game results from The Odds API for early win detection
// ---------------------------------------------------------------------------

const scoresCache = {}; // { sport: { fetchedAt, games: [{ homeTeam, awayTeam, commenceTime, completed, homeScore, awayScore }] } }
const SCORES_TTL_MS = 2 * 60 * 1000; // 2 minute cache

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

    // Standard sports — direct fetch
    const url = `https://api.the-odds-api.com/v4/sports/${sport}/scores/?apiKey=${theOddsApiKey}&daysFrom=1`;
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
  getAltLinesWarmStats,
  getJitWarmStats,
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
  __debugGetAltLinesCache: () => altLinesCache,
};
