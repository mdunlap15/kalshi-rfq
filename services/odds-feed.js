const fetch = require('node-fetch');
const { config } = require('../config');
const log = require('./logger');

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
};
// NOTE: tennis removed from LEAGUE_MAP — SharpAPI returns events but zero bookmaker
// odds for tennis. Routed through The Odds API fallback instead (dynamic tournament discovery).

// Bookmakers for The Odds API — Pinnacle (sharpest), DraftKings, FanDuel
const ODDS_API_BOOKMAKERS = 'pinnacle,draftkings,fanduel';

// Expanded bookmakers for alt-line fetching only — more books = more alt line values.
// Primary pricing is NOT affected (uses ODDS_API_BOOKMAKERS via SharpAPI consensus).
// Minimum 2 books required per alt line to ensure de-vig accuracy.
const ALT_LINES_BOOKMAKERS = 'pinnacle,draftkings,fanduel,bovada,betonlineag,betrivers,williamhill_us,unibet_us,superbook,betmgm';
const ALT_LINES_MIN_BOOKS = 2; // Require at least 2 books for each alt line value

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
  // Golf and combat sports — h2h only (no spreads/totals on these markets)
  'golf_pga_championship': {
    oddsApiSport: 'golf_pga_championship',
    markets: 'h2h,outrights',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
  'mma_mixed_martial_arts': {
    oddsApiSport: 'mma_mixed_martial_arts',
    markets: 'h2h',
    bookmakers: ODDS_API_BOOKMAKERS,
  },
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

  // Market types vary by sport. SharpAPI Hobby tier caps responses at 50 rows
  // per call regardless of the limit parameter, so we split market types into
  // separate API calls to ensure coverage of all market types for all events.
  const marketTypesList = {
    'baseball_mlb': ['moneyline', 'run_line', 'total_runs', 'team_total'],
    'icehockey_nhl': ['moneyline', 'puck_line', 'total_goals', 'team_total'],
    'basketball_nba': ['moneyline', 'point_spread', 'total_points', 'team_total'],
    'tennis': ['moneyline', 'point_spread', 'total_points'],
    'soccer': ['moneyline', 'point_spread', 'total_goals', 'team_total'],
  }[sport] || ['moneyline', 'point_spread', 'total_points', 'team_total'];

  log.info('OddsFeed', `Fetching ${liveMode ? 'LIVE ' : ''}${mapping.value} odds from SharpAPI (${marketTypesList.length} market types)...`);

  // Fetch each market type separately to avoid the Hobby tier row cap
  const rows = [];
  for (const mt of marketTypesList) {
    const url = `${config.oddsApi.baseUrl}/odds`
      + `?${mapping.param}=${mapping.value}`
      + `&market=${mt}`
      + `&live=${liveMode ? 'true' : 'false'}`
      + `&limit=500`;
    try {
      const resp = await fetch(url, {
        headers: { 'X-API-Key': config.oddsApi.apiKey },
      });
      if (!resp.ok) {
        const text = await resp.text();
        log.warn('OddsFeed', `SharpAPI ${resp.status} for ${mapping.value}/${mt}: ${text.substring(0, 100)}`);
        continue;
      }
      const body = await resp.json();
      const mtRows = body.data || [];
      rows.push(...mtRows);
      log.debug('OddsFeed', `  ${mt}: ${mtRows.length} rows`);
    } catch (err) {
      log.warn('OddsFeed', `Fetch error for ${mapping.value}/${mt}: ${err.message}`);
    }
  }
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

          if ((homeMatch && awayMatch) || (homeMatchSwap && awayMatchSwap)) {
            bestMatch = mainId;
            break;
          }
        }

        if (bestMatch) {
          // Merge orphan odds into main event
          for (const row of orphan.odds) {
            eventMap[bestMatch].odds.push(row);
          }
          delete eventMap[orphanId];
          mergedOrphans++;
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

  log.info('OddsFeed', `Cached ${Object.keys(parsed).length} ${liveMode ? 'LIVE ' : ''}events for ${mapping.value}`);
  return parsed;
}

/**
 * Fetch First-5-Innings (F5) markets for MLB from The Odds API and attach them
 * to the existing event cache as separate market types: h2h_f5, spreads_f5, totals_f5.
 * These are independent from full-game markets and need their own pricing.
 */
async function supplementMlbF5Markets(parsedEvents) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return;

  const url = `https://api.the-odds-api.com/v4/sports/baseball_mlb/odds`
    + `?apiKey=${theOddsApiKey}`
    + `&regions=us,eu`
    + `&markets=h2h_1st_5_innings,spreads_1st_5_innings,totals_1st_5_innings`
    + `&bookmakers=pinnacle,draftkings,fanduel`
    + `&oddsFormat=american`;

  const resp = await fetch(url);
  if (!resp.ok) {
    log.warn('OddsFeed', `MLB F5 fetch failed (${resp.status})`);
    return;
  }
  const remaining = resp.headers.get('x-requests-remaining');
  if (remaining != null) log.debug('OddsFeed', `The Odds API usage (MLB F5): ${remaining} remaining`);

  const events = await resp.json();
  let matched = 0;
  for (const event of events) {
    const key = normalizeEventKey(cleanTeamName(event.home_team), cleanTeamName(event.away_team));
    const entry = parsedEvents[key];
    if (!entry) continue;
    const eventArr = Array.isArray(entry) ? entry : [entry];
    // Match by date to handle back-to-back games
    const evDate = event.commence_time ? new Date(event.commence_time).toISOString().substring(0, 10) : '';
    const matchedEv = eventArr.find(e => {
      const d = e.commenceTime ? new Date(e.commenceTime).toISOString().substring(0, 10) : '';
      return !evDate || !d || d === evDate;
    }) || eventArr[0];
    if (!matchedEv) continue;

    // Collect book pairs for each F5 market type
    const mlPairs = [], spreadPairs = [], totalPairs = [];
    for (const book of (event.bookmakers || [])) {
      for (const m of (book.markets || [])) {
        if (m.key === 'h2h_1st_5_innings') {
          const home = m.outcomes?.find(o => o.name === event.home_team);
          const away = m.outcomes?.find(o => o.name === event.away_team);
          if (home && away) {
            mlPairs.push({
              book: book.key,
              home: { odds_probability: americanToImpliedProb(home.price), odds_american: home.price },
              away: { odds_probability: americanToImpliedProb(away.price), odds_american: away.price },
            });
          }
        } else if (m.key === 'spreads_1st_5_innings') {
          const home = m.outcomes?.find(o => o.name === event.home_team);
          const away = m.outcomes?.find(o => o.name === event.away_team);
          if (home && away) {
            spreadPairs.push({
              book: book.key,
              home: { odds_probability: americanToImpliedProb(home.price), odds_american: home.price, point: home.point, line: home.point },
              away: { odds_probability: americanToImpliedProb(away.price), odds_american: away.price, point: away.point, line: away.point },
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

    if (mlPairs.length > 0 || spreadPairs.length > 0 || totalPairs.length > 0) matched++;
    if (mlPairs.length > 0) matchedEv.markets.h2h_f5 = buildConsensusMoneyline(mlPairs);
    if (spreadPairs.length > 0) matchedEv.markets.spreads_f5 = buildConsensusSpread(spreadPairs);
    if (totalPairs.length > 0) matchedEv.markets.totals_f5 = buildConsensusTotals(totalPairs);
  }
  log.info('OddsFeed', `MLB F5: attached to ${matched} events`);
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

  return parsed;
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
      const fairHome = [], fairAway = [];
      for (const p of mlPairs) {
        const [fh, fa] = deVig2Way(p.home.odds_probability, p.away.odds_probability);
        fairHome.push(fh);
        fairAway.push(fa);
      }
      // Pinnacle floor only on heavy favorites (>65%) where de-vig over-corrects.
      // Use de-vigged Pinnacle (not raw implied) to avoid double-vig.
      const pinPair = mlPairs.find(p => p.book === 'pinnacle');
      const klPair = mlPairs.find(p => p.book === 'kalshi');
      const pinFairH = pinPair ? deVig2Way(pinPair.home.odds_probability, pinPair.away.odds_probability)[0] : 0;
      const pinFairA = pinPair ? deVig2Way(pinPair.home.odds_probability, pinPair.away.odds_probability)[1] : 0;
      const dvH = avg(fairHome), dvA = avg(fairAway);
      const flrH = pinPair ? pinFairH : (klPair ? Math.min(klPair.home.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
      const flrA = pinPair ? pinFairA : (klPair ? Math.min(klPair.away.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
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
      const fairHome = [], fairAway = [];
      for (const p of spreadPairs) {
        const [fh, fa] = deVig2Way(p.home.odds_probability, p.away.odds_probability);
        fairHome.push(fh);
        fairAway.push(fa);
      }
      const pinSpread = spreadPairs.find(p => p.book === 'pinnacle');
      const klSpread = spreadPairs.find(p => p.book === 'kalshi');
      const pinFairH = pinSpread ? deVig2Way(pinSpread.home.odds_probability, pinSpread.away.odds_probability)[0] : 0;
      const pinFairA = pinSpread ? deVig2Way(pinSpread.home.odds_probability, pinSpread.away.odds_probability)[1] : 0;
      const dvSHome = avg(fairHome), dvSAway = avg(fairAway);
      const flrSH = pinSpread ? pinFairH : (klSpread ? Math.min(klSpread.home.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
      const flrSA = pinSpread ? pinFairA : (klSpread ? Math.min(klSpread.away.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
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
      const fairOver = [], fairUnder = [];
      for (const p of totalPairs) {
        const [fo, fu] = deVig2Way(p.over.odds_probability, p.under.odds_probability);
        fairOver.push(fo);
        fairUnder.push(fu);
      }
      const pinTotal = totalPairs.find(p => p.book === 'pinnacle');
      const klTotal = totalPairs.find(p => p.book === 'kalshi');
      const pinFairO = pinTotal ? deVig2Way(pinTotal.over.odds_probability, pinTotal.under.odds_probability)[0] : 0;
      const pinFairU = pinTotal ? deVig2Way(pinTotal.over.odds_probability, pinTotal.under.odds_probability)[1] : 0;
      const dvTOver = avg(fairOver), dvTUnder = avg(fairUnder);
      const flrTO = pinTotal ? pinFairO : (klTotal ? Math.min(klTotal.over.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
      const flrTU = pinTotal ? pinFairU : (klTotal ? Math.min(klTotal.under.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
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

    const devigged = { over: [], under: [] };
    for (const { over, under } of matching) {
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

function buildConsensusMoneyline(bookPairs) {
  // Preserve ALL books for display attribution (pinnacle/fd/dk/kalshi fields
  // are always populated from the full list so the dashboard can show every
  // book's raw odds regardless of its vig).
  const allBooks = bookPairs;

  // Filter high-vig books out of the averaging input. Prevents Saba-class
  // Asian bookmaking feeds from corrupting the de-vigged consensus mean
  // via unweighted averaging. See filterSharpBooks for rationale.
  const sharpBooks = filterSharpBooks(
    bookPairs,
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
  const floorHome = pinBook ? pinFairHome : (klBook ? Math.min(klBook.home.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
  const floorAway = pinBook ? pinFairAway : (klBook ? Math.min(klBook.away.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
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
  // filterSharpBooks rationale in buildConsensusMoneyline).
  const sharpMatching = filterSharpBooks(
    matching,
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
  const klBookS = matching.find(bp => bp.book === 'kalshi');
  // Floor at Pinnacle's DE-VIGGED fair prob (not raw implied) — raw would
  // include Pinnacle's vig and cause double-vig when we apply ours on top.
  // Threshold lowered from 0.65 to 0.50 so any favorite side gets floored
  // against Pin's de-vigged prob whenever Pin is present.
  const pinFairHomeS = pinBook ? deVig2Way(pinBook.home.odds_probability, pinBook.away.odds_probability)[0] : 0;
  const pinFairAwayS = pinBook ? deVig2Way(pinBook.home.odds_probability, pinBook.away.odds_probability)[1] : 0;
  const floorHomeS = pinBook ? pinFairHomeS : (klBookS ? klBookS.home.odds_probability : 0);
  const floorAwayS = pinBook ? pinFairAwayS : (klBookS ? klBookS.away.odds_probability : 0);
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

function buildConsensusTotals(bookPairs) {
  // Use the most common line across books
  const lineCounts = {};
  for (const { over } of bookPairs) {
    const line = over.line;
    if (line != null) lineCounts[line] = (lineCounts[line] || 0) + 1;
  }
  const primaryLine = Object.entries(lineCounts).sort((a, b) => b[1] - a[1])[0]?.[0];
  const pLine = parseFloat(primaryLine);

  const matching = bookPairs.filter(bp => bp.over.line === pLine);
  if (matching.length === 0) return null;

  // Filter high-vig books out of the averaging input (see
  // filterSharpBooks rationale in buildConsensusMoneyline).
  const sharpMatching = filterSharpBooks(
    matching,
    bp => [bp.over.odds_probability, bp.under.odds_probability],
    'total'
  );

  // De-vigged consensus for display
  const devigged = { over: [], under: [] };
  for (const { over, under } of sharpMatching) {
    const [fo, fu] = deVig2Way(over.odds_probability, under.odds_probability);
    devigged.over.push(fo);
    devigged.under.push(fu);
  }
  const dvOver = avg(devigged.over);
  const dvUnder = avg(devigged.under);

  const pinBook = matching.find(bp => bp.book === 'pinnacle');
  const fdBook2 = matching.find(bp => bp.book === 'fanduel');
  const klBookT = matching.find(bp => bp.book === 'kalshi');
  // Floor at Pinnacle's DE-VIGGED fair prob (not raw implied) to avoid double-vig.
  // Threshold lowered from 0.65 to 0.50 — see buildConsensusMoneyline.
  const pinFairOver = pinBook ? deVig2Way(pinBook.over.odds_probability, pinBook.under.odds_probability)[0] : 0;
  const pinFairUnder = pinBook ? deVig2Way(pinBook.over.odds_probability, pinBook.under.odds_probability)[1] : 0;
  const floorOver = pinBook ? pinFairOver : (klBookT ? Math.min(klBookT.over.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
  const floorUnder = pinBook ? pinFairUnder : (klBookT ? Math.min(klBookT.under.odds_probability * (1 + KALSHI_BUFFER), 0.99) : 0);
  const pricingOver = dvOver >= 0.50 ? Math.max(dvOver, floorOver) : dvOver;
  const pricingUnder = dvUnder >= 0.50 ? Math.max(dvUnder, floorUnder) : dvUnder;

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
    pinnacle: (() => {
      const pinBook = matching.find(bp => bp.book === 'pinnacle');
      return pinBook ? { over: pinBook.over.odds_american, under: pinBook.under.odds_american } : null;
    })(),
    fanduel: (() => {
      const fdBook = matching.find(bp => bp.book === 'fanduel');
      return fdBook ? { over: fdBook.over.odds_american, under: fdBook.under.odds_american } : null;
    })(),
    kalshi: (() => {
      const klBook = matching.find(bp => bp.book === 'kalshi');
      return klBook ? { over: klBook.over.odds_american, under: klBook.under.odds_american } : null;
    })(),
    draftkings: (() => {
      const dkBook = matching.find(bp => bp.book === 'draftkings');
      return dkBook ? { over: dkBook.over.odds_american, under: dkBook.under.odds_american } : null;
    })(),
  };
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
const ALT_LINES_TTL_MS = 10 * 60 * 1000; // 10 minute cache

/**
 * Fetch alternate spreads and totals for a specific event from The Odds API.
 * Uses the event-specific endpoint which supports alt markets from Pinnacle.
 */
// Cache for The Odds API event ID lookups (sport → { fetchedAt, events: [{id, home, away}] })
const oddsApiEventIdCache = {};
const ODDS_API_EVENT_ID_TTL_MS = 30 * 60 * 1000; // 30 min

/**
 * Resolve The Odds API event ID for a given home/away pair.
 * SharpAPI event IDs are NOT The Odds API event IDs, so we must look up
 * the event list from The Odds API and match by team name.
 */
async function resolveOddsApiEventId(sport, homeTeam, awayTeam) {
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
  const oddsApiSport = oddsApiSportMap[sport];
  if (!oddsApiSport) return null;

  // Check cache
  const cached = oddsApiEventIdCache[sport];
  if (!cached || (Date.now() - cached.fetchedAt) > ODDS_API_EVENT_ID_TTL_MS) {
    // Fetch events list (lightweight — no odds, just event metadata)
    const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/events?apiKey=${theOddsApiKey}`;
    try {
      const resp = await fetch(url);
      if (!resp.ok) {
        log.warn('OddsFeed', `Odds API events list failed (${resp.status}) for ${sport}`);
        return null;
      }
      const data = await resp.json();
      oddsApiEventIdCache[sport] = {
        fetchedAt: Date.now(),
        events: (data || []).map(e => ({
          id: e.id,
          home: e.home_team,
          away: e.away_team,
          commence: e.commence_time,
        })),
      };
      log.debug('OddsFeed', `Cached ${data.length} Odds API event IDs for ${sport}`);
    } catch (err) {
      log.warn('OddsFeed', `Odds API events list error for ${sport}: ${err.message}`);
      return null;
    }
  }

  // Match by team name (normalized)
  const normHome = homeTeam.toLowerCase().trim();
  const normAway = awayTeam.toLowerCase().trim();
  const events = oddsApiEventIdCache[sport]?.events || [];
  for (const e of events) {
    const eHome = e.home.toLowerCase().trim();
    const eAway = e.away.toLowerCase().trim();
    if ((eHome === normHome || eHome.includes(normHome) || normHome.includes(eHome))
        && (eAway === normAway || eAway.includes(normAway) || normAway.includes(eAway))) {
      return { eventId: e.id, oddsApiSport };
    }
  }
  log.debug('OddsFeed', `No Odds API event match for ${homeTeam} vs ${awayTeam} in ${sport}`);
  return null;
}

async function fetchAltLines(sport, homeTeam, awayTeam) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return null;

  const key = normalizeEventKey(homeTeam, awayTeam);

  // Check cache
  const cached = altLinesCache[key];
  if (cached && (Date.now() - cached.fetchedAt) < ALT_LINES_TTL_MS) {
    return cached;
  }

  // Resolve The Odds API event ID (SharpAPI IDs are different)
  const resolved = await resolveOddsApiEventId(sport, homeTeam, awayTeam);
  if (!resolved) {
    log.debug('OddsFeed', `Cannot fetch alt lines for ${homeTeam} vs ${awayTeam}: no Odds API event ID`);
    return null;
  }
  const { eventId, oddsApiSport } = resolved;

  const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/events/${eventId}/odds`
    + `?apiKey=${theOddsApiKey}`
    + `&regions=us,eu`
    + `&markets=alternate_spreads,alternate_totals`
    + `&bookmakers=${ALT_LINES_BOOKMAKERS}`
    + `&oddsFormat=american`;

  log.info('OddsFeed', `Fetching alt lines for ${homeTeam} vs ${awayTeam}...`);

  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      const text = await resp.text();
      log.warn('OddsFeed', `Alt lines fetch failed (${resp.status}): ${text.substring(0, 100)}`);
      return null;
    }

    const data = await resp.json();
    const result = { fetchedAt: Date.now(), altSpreads: {}, altTotals: {} };

    for (const book of (data.bookmakers || [])) {
      for (const market of (book.markets || [])) {
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
            const isHome = o.name === homeTeam || o.name === data.home_team;
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
        } else if (market.key === 'alternate_totals') {
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
    let skippedThinSpreads = 0, skippedThinTotals = 0;
    for (const [lineKey, lineData] of Object.entries(result.altSpreads)) {
      const bookCount = lineData.books.size;
      const byBook = lineData.byBook;
      const homeProbs = lineData.probs.filter(p => p.isHome).map(p => p.prob);
      const awayProbs = lineData.probs.filter(p => !p.isHome).map(p => p.prob);
      if (homeProbs.length > 0 && awayProbs.length > 0 && bookCount >= ALT_LINES_MIN_BOOKS) {
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
      if (overProbs.length > 0 && underProbs.length > 0 && bookCount >= ALT_LINES_MIN_BOOKS) {
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

    altLinesCache[key] = result;
    const skippedNote = (skippedThinSpreads + skippedThinTotals) > 0 ? ` (skipped ${skippedThinSpreads} spreads + ${skippedThinTotals} totals with <${ALT_LINES_MIN_BOOKS} books)` : '';
    log.info('OddsFeed', `Cached alt lines: ${Object.keys(result.altSpreads).length} spreads, ${Object.keys(result.altTotals).length} totals${skippedNote}`);
    return result;
  } catch (err) {
    log.error('OddsFeed', `Alt lines error: ${err.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// CACHE LOOKUP
// ---------------------------------------------------------------------------

/**
 * Get fair probability — sync version, uses cached data only.
 * @param {string} targetTime - optional ISO timestamp for time-aware matching
 */
function getFairProb(sport, homeTeam, awayTeam, marketType, selection, line, targetTime) {
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event) return null;

  const market = event.markets[marketType];
  if (!market) return null;

  if (marketType === 'spreads' || marketType === 'totals') {
    // CRITICAL: spreads/totals MUST have a line value to price correctly.
    // Without a line, we can't distinguish Over 4.5 from Over 8.5 — returning
    // the primary fair prob would be catastrophically wrong for alt lines.
    // (Root cause of +377 mispricing on Over 4.5 + Under 5.5 parlay, 2026-04-12)
    if (line == null) {
      log.warn('OddsFeed', `getFairProb: null line for ${marketType} ${selection} ${homeTeam} vs ${awayTeam} — declining to avoid primary-line contamination`);
      return null;
    }

    if (market.line != null) {
      const absLine = Math.abs(line);
      const lineDiff = Math.abs(Math.abs(market.line) - absLine);
      if (lineDiff > 0.01) {
        // Line magnitude doesn't match primary — check alt lines.
        // Pass the SIGNED line so getAltLineFairProb can route to the correct
        // signed home_point bucket (critical: sign flips on alt spreads).
        const key = normalizeEventKey(homeTeam, awayTeam);
        const altProb = getAltLineFairProb(key, marketType, selection, line);
        if (altProb != null) {
          // Sanity: for totals far from primary, verify direction makes sense.
          // Over a low total (e.g. 4.5 when primary is 8.5) should be a heavy
          // favorite (fairProb >= 0.60). Under a low total should be an underdog.
          // Vice versa for high totals.  If violated, the alt line data may be
          // corrupted (swapped over/under, wrong point, stale cache).
          if (marketType === 'totals' && lineDiff >= 2.0) {
            const expectHigh = (selection === 'over' && line < market.line) || (selection === 'under' && line > market.line);
            if (expectHigh && altProb < 0.55) {
              log.warn('OddsFeed', `Alt total sanity FAIL: ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} — expected heavy favorite, got underdog. Declining.`);
              return null;
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
async function verifyLineWithPinnacle(sport, homeTeam, awayTeam, marketType, cachedLine) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  const oddsApiSport = PINNACLE_SPORT_MAP[sport] || ODDS_API_FALLBACK[sport]?.oddsApiSport;
  if (!theOddsApiKey || !oddsApiSport) return { ok: true }; // can't verify, allow

  try {
    const market = marketType === 'spreads' ? 'spreads' : 'totals';
    const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/odds`
      + `?apiKey=${theOddsApiKey}`
      + `&regions=eu`
      + `&markets=${market}`
      + `&bookmakers=pinnacle`
      + `&oddsFormat=american`;

    const resp = await fetch(url);
    if (!resp.ok) return { ok: true }; // API error, allow

    const events = await resp.json();
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
async function getFairProbAsync(sport, homeTeam, awayTeam, marketType, selection, line, targetTime) {
  // Try sync first
  const syncResult = getFairProb(sport, homeTeam, awayTeam, marketType, selection, line, targetTime);
  if (syncResult != null) return syncResult;

  // If it's a spread/total with a line mismatch, try fetching alt lines
  if ((marketType === 'spreads' || marketType === 'totals') && line != null) {
    const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
    if (event) {
      await fetchAltLines(sport, homeTeam, awayTeam);
      const key = normalizeEventKey(homeTeam, awayTeam);
      // Pass SIGNED line (not abs) so alt-line lookup routes to the correct
      // signed home_point bucket.
      const altProb = getAltLineFairProb(key, marketType, selection, line);
      if (altProb != null) {
        // Same directional sanity check as getFairProb (see comment there).
        const market = event.markets[marketType];
        if (marketType === 'totals' && market?.line != null) {
          const lineDiff = Math.abs(Math.abs(market.line) - Math.abs(line));
          if (lineDiff >= 2.0) {
            const expectHigh = (selection === 'over' && line < market.line) || (selection === 'under' && line > market.line);
            if (expectHigh && altProb < 0.55) {
              log.warn('OddsFeed', `Alt total sanity FAIL (async): ${selection} ${line} (primary ${market.line}) fair=${altProb.toFixed(4)} — declining`);
              return null;
            }
          }
        }
      }
      return altProb;
    }
  }

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
 * Look up a fair prob from the alt lines cache.
 * For spreads, `line` MUST be the signed team-perspective line (not abs) so
 * we can route to the correct signed home_point bucket.
 */
function getAltLineFairProb(eventKey, marketType, selection, line) {
  const alt = altLinesCache[eventKey];
  if (!alt) return null;

  if (marketType === 'spreads') {
    const homePoint = spreadHomePoint(line, selection);
    if (homePoint == null) return null;
    const lineData = alt.altSpreads[String(homePoint)];
    if (!lineData) return null;
    if (selection === 'home') return lineData.home || null;
    if (selection === 'away') return lineData.away || null;
  } else if (marketType === 'totals') {
    // Totals have no sign — over/under don't swap direction.
    const lineData = alt.altTotals[Math.abs(line)];
    if (!lineData) return null;
    if (selection === 'over') return lineData.over || null;
    if (selection === 'under') return lineData.under || null;
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
  if (!sportCache) return null;
  const key = normalizeEventKey(homeTeam, awayTeam);
  const events = sportCache.events[key];
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
  const event = getLiveEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event || !event.markets) return null;
  // Reuse same probe-by-market-type logic as getFairProb — we search the markets object
  // in the live event's payload, same structure as getEventMarkets output
  const m = event.markets;
  if (marketType === 'h2h' && m.h2h) {
    const pick = selection === 'home' ? m.h2h.home : m.h2h.away;
    return pick && pick.fairProb ? pick.fairProb : null;
  }
  if (marketType === 'spreads' && m.spreads) {
    const group = m.spreads[line != null ? Math.abs(line) : (m.spreads._primary || 0)];
    if (!group) return null;
    const pick = selection === 'home' ? group.home : group.away;
    return pick && pick.fairProb ? pick.fairProb : null;
  }
  if (marketType === 'totals' && m.totals) {
    const group = m.totals[line];
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

  // Split delta fetch by market type — SharpAPI Hobby caps at 50 rows per call
  const marketTypesList = {
    'baseball_mlb': ['moneyline', 'run_line', 'total_runs', 'team_total'],
    'icehockey_nhl': ['moneyline', 'puck_line', 'total_goals', 'team_total'],
    'basketball_nba': ['moneyline', 'point_spread', 'total_points', 'team_total'],
    'soccer': ['moneyline', 'point_spread', 'total_goals', 'team_total'],
  }[sport] || ['moneyline', 'point_spread', 'total_points', 'team_total'];

  const rows = [];
  let anyFailed = false;
  for (const mt of marketTypesList) {
    const url = `${config.oddsApi.baseUrl}/odds/delta`
      + `?${mapping.param}=${mapping.value}`
      + `&market=${mt}`
      + `&since=${encodeURIComponent(since)}`
      + `&limit=500`;
    try {
      const resp = await fetch(url, {
        headers: { 'X-API-Key': config.oddsApi.apiKey },
      });
      if (!resp.ok) {
        log.warn('OddsFeed', `Delta fetch failed (${resp.status}) for ${sport}/${mt}`);
        anyFailed = true;
        continue;
      }
      const body = await resp.json();
      const mtRows = body.data || [];
      rows.push(...mtRows);
    } catch (err) {
      log.warn('OddsFeed', `Delta fetch error for ${sport}/${mt}: ${err.message}`);
      anyFailed = true;
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
  return results;
}

function getCacheStatus() {
  const status = {};
  for (const sport of config.supportedSports) {
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

/**
 * Clean team names from SharpAPI (removes pitcher info like "(TBD)").
 */
function cleanTeamName(name) {
  return (name || '').replace(/\s*\([^)]*\)\s*$/, '').trim();
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
  verifyLineWithPinnacle,
  getPinnacleOdds,
  getDisplayFairProb,
  getFanDuelOdds,
  getKalshiOdds,
  getDraftKingsOdds,
  getDNBFairProb,
  fetchAltLines,
  getEventMarkets,
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
