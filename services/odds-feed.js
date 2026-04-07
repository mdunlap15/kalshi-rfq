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

// SharpAPI league/sport keys mapping
const LEAGUE_MAP = {
  'basketball_nba': { param: 'league', value: 'nba' },
  'baseball_mlb': { param: 'league', value: 'mlb' },
  'icehockey_nhl': { param: 'league', value: 'nhl' },
  'tennis': { param: 'sport', value: 'tennis' },
  'soccer': { param: 'sport', value: 'soccer' },
};

// Sports that use The Odds API as fallback (SharpAPI free tier doesn't cover them)
const ODDS_API_FALLBACK = {
  'basketball_ncaab': {
    oddsApiSport: 'basketball_ncaab',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'americanfootball_nfl': {
    oddsApiSport: 'americanfootball_nfl',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'americanfootball_ncaaf': {
    oddsApiSport: 'americanfootball_ncaaf',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'basketball_wnba': {
    oddsApiSport: 'basketball_wnba',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'soccer_usa_mls': {
    oddsApiSport: 'soccer_usa_mls',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'soccer_epl': {
    oddsApiSport: 'soccer_epl',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'soccer_uefa_champs_league': {
    oddsApiSport: 'soccer_uefa_champs_league',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'soccer_uefa_europa_league': {
    oddsApiSport: 'soccer_uefa_europa_league',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'soccer_spain_la_liga': {
    oddsApiSport: 'soccer_spain_la_liga',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'soccer_italy_serie_a': {
    oddsApiSport: 'soccer_italy_serie_a',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'soccer_germany_bundesliga': {
    oddsApiSport: 'soccer_germany_bundesliga',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'soccer_france_ligue_one': {
    oddsApiSport: 'soccer_france_ligue_one',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'soccer_usa_nwsl': {
    oddsApiSport: 'soccer_usa_nwsl',
    markets: 'h2h,spreads,totals',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  // Golf and combat sports — h2h only (no spreads/totals on these markets)
  'golf_pga_championship': {
    oddsApiSport: 'golf_pga_championship',
    markets: 'h2h,outrights',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'mma_mixed_martial_arts': {
    oddsApiSport: 'mma_mixed_martial_arts',
    markets: 'h2h',
    bookmakers: 'pinnacle,draftkings,fanduel',
  },
  'boxing_boxing': {
    oddsApiSport: 'boxing_boxing',
    markets: 'h2h',
    bookmakers: 'pinnacle,draftkings,fanduel',
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

  // Market types vary by sport
  const marketTypes = {
    'baseball_mlb': 'moneyline,run_line,total_runs',
    'icehockey_nhl': 'moneyline,puck_line,total_goals',
    'basketball_nba': 'moneyline,point_spread,total_points',
    'tennis': 'moneyline,point_spread,total_points',
    'soccer': 'moneyline,point_spread,total_goals',
  }[sport] || 'moneyline,point_spread,total_points';

  const url = `${config.oddsApi.baseUrl}/odds`
    + `?${mapping.param}=${mapping.value}`
    + `&market=${marketTypes}`
    + `&live=${liveMode ? 'true' : 'false'}`
    + `&limit=200`;

  log.info('OddsFeed', `Fetching ${liveMode ? 'LIVE ' : ''}${mapping.value} odds from SharpAPI...`);

  const resp = await fetch(url, {
    headers: { 'X-API-Key': config.oddsApi.apiKey },
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`SharpAPI ${resp.status} for ${mapping.value}: ${text}`);
  }

  const body = await resp.json();
  const rows = body.data || [];
  log.info('OddsFeed', `Got ${rows.length} odds rows for ${mapping.value}`);

  // Group by event, then by market+selection to de-vig across books
  const eventMap = {};
  for (const row of rows) {
    const eventId = row.event_id;
    if (!eventMap[eventId]) {
      eventMap[eventId] = {
        homeTeam: cleanTeamName(row.home_team),
        awayTeam: cleanTeamName(row.away_team),
        commenceTime: row.event_start_time,
        eventId,
        odds: [], // collect all odds rows
      };
    }
    eventMap[eventId].odds.push(row);
  }

  // Supplement with Pinnacle odds from The Odds API
  // Pinnacle events have different IDs, so match by team names and merge
  if (PINNACLE_SPORT_MAP[sport]) {
    const pinnacleRows = await fetchPinnacleRows(sport);
    if (pinnacleRows.length > 0) {
      // Build team-name lookup to match Pinnacle events to SharpAPI events
      const teamKeyToEventId = {};
      for (const [eid, ev] of Object.entries(eventMap)) {
        const key = normalizeEventKey(ev.homeTeam, ev.awayTeam);
        teamKeyToEventId[key] = eid;
      }

      let merged = 0;
      for (const row of pinnacleRows) {
        const key = normalizeEventKey(cleanTeamName(row.home_team), cleanTeamName(row.away_team));
        const matchedId = teamKeyToEventId[key];
        if (matchedId && eventMap[matchedId]) {
          eventMap[matchedId].odds.push(row);
          merged++;
        }
        // If no match, Pinnacle has an event SharpAPI doesn't — skip it
      }
      log.info('OddsFeed', `Pinnacle: merged ${merged} of ${pinnacleRows.length} rows into ${mapping.value} events`);
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

    if (Object.keys(markets).length > 0) {
      if (!parsed[key]) parsed[key] = [];
      parsed[key].push({
        homeTeam: event.homeTeam,
        awayTeam: event.awayTeam,
        commenceTime: event.commenceTime,
        eventId,
        markets,
      });
    }
  }

  const targetCache = liveMode ? liveOddsCache : oddsCache;
  targetCache[sport] = {
    fetchedAt: Date.now(),
    events: parsed,
  };

  log.info('OddsFeed', `Cached ${Object.keys(parsed).length} ${liveMode ? 'LIVE ' : ''}events for ${mapping.value}`);
  return parsed;
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
 * Fetch Pinnacle odds from The Odds API and convert to SharpAPI-format rows.
 * Returns array of rows compatible with SharpAPI's format, or empty array on failure.
 */
async function fetchPinnacleRows(sport) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  const oddsApiSport = PINNACLE_SPORT_MAP[sport];
  if (!theOddsApiKey || !oddsApiSport) return [];

  const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/odds`
    + `?apiKey=${theOddsApiKey}`
    + `&regions=us,eu`
    + `&markets=h2h,spreads,totals`
    + `&bookmakers=pinnacle`
    + `&oddsFormat=american`;

  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      log.warn('OddsFeed', `Pinnacle fetch failed (${resp.status}) for ${sport}`);
      return [];
    }

    const remaining = resp.headers.get('x-requests-remaining');
    const used = resp.headers.get('x-requests-used');
    if (remaining != null) {
      log.info('OddsFeed', `The Odds API usage (Pinnacle): ${used} used, ${remaining} remaining`);
    }

    const events = await resp.json();
    const rows = [];

    for (const event of events) {
      const pinnacle = (event.bookmakers || []).find(b => b.key === 'pinnacle');
      if (!pinnacle) continue;

      for (const market of (pinnacle.markets || [])) {
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
            sportsbook: 'pinnacle',
            market_type: marketType,
            selection_type: selectionType,
            odds_american: outcome.price,
            odds_probability: americanToImpliedProb(outcome.price),
            line: outcome.point != null ? outcome.point : null,
          });
        }
      }
    }

    log.info('OddsFeed', `Pinnacle: ${rows.length} odds rows for ${sport} (${events.length} events)`);
    return rows;
  } catch (err) {
    log.warn('OddsFeed', `Pinnacle fetch error for ${sport}: ${err.message}`);
    return [];
  }
}

// ---------------------------------------------------------------------------
// THE ODDS API FALLBACK (for sports SharpAPI free tier doesn't cover)
// ---------------------------------------------------------------------------

async function fetchFromTheOddsApi(sport) {
  const fallback = ODDS_API_FALLBACK[sport];
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) {
    throw new Error(`No THE_ODDS_API_KEY set for fallback sport ${sport}`);
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
      markets.h2h = {
        home: { rawOdds: mlPairs[0].home.odds_american, impliedProb: mlPairs[0].home.odds_probability, fairProb: avg(fairHome) },
        away: { rawOdds: mlPairs[0].away.odds_american, impliedProb: mlPairs[0].away.odds_probability, fairProb: avg(fairAway) },
        books: mlPairs.length,
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
      markets.spreads = {
        home: { rawOdds: spreadPairs[0].home.odds_american, point: spreadPairs[0].home.point, impliedProb: spreadPairs[0].home.odds_probability, fairProb: avg(fairHome) },
        away: { rawOdds: spreadPairs[0].away.odds_american, point: spreadPairs[0].away.point, impliedProb: spreadPairs[0].away.odds_probability, fairProb: avg(fairAway) },
        line: spreadPairs[0].home.point,
        books: spreadPairs.length,
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
      markets.totals = {
        over: { rawOdds: totalPairs[0].over.odds_american, point: totalPairs[0].over.point, impliedProb: totalPairs[0].over.odds_probability, fairProb: avg(fairOver) },
        under: { rawOdds: totalPairs[0].under.odds_american, point: totalPairs[0].under.point, impliedProb: totalPairs[0].under.odds_probability, fairProb: avg(fairUnder) },
        line: totalPairs[0].over.point,
        books: totalPairs.length,
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

function buildConsensusMoneyline(bookPairs) {
  // Compute de-vigged consensus across ALL books (for display as "Fair")
  const devigged = { home: [], away: [] };
  for (const { home, away } of bookPairs) {
    const [fh, fa] = deVig2Way(home.odds_probability, away.odds_probability);
    devigged.home.push(fh);
    devigged.away.push(fa);
  }
  const dvHome = avg(devigged.home);
  const dvAway = avg(devigged.away);

  // For PRICING: use de-vigged consensus (same as display).
  // Combined with odds-based vig (which scales properly on heavy favorites),
  // this produces prices slightly sweeter than Pinnacle — competitive for bids.
  // Previously used Pinnacle raw probs here, but that double-vigged
  // (Pinnacle's ~2% margin + our vig = too tight).
  const pinBook = bookPairs.find(bp => bp.book === 'pinnacle');
  const pricingHome = dvHome;
  const pricingAway = dvAway;

  const pinnacle = pinBook ? {
    home: pinBook.home.odds_american,
    away: pinBook.away.odds_american,
  } : null;
  const fdBook = bookPairs.find(bp => bp.book === 'fanduel');
  const fanduel = fdBook ? {
    home: fdBook.home.odds_american,
    away: fdBook.away.odds_american,
  } : null;
  return {
    home: {
      rawOdds: bookPairs[0].home.odds_american,
      impliedProb: bookPairs[0].home.odds_probability,
      fairProb: pricingHome,      // Pinnacle raw — used for pricing
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

  // De-vigged consensus for display
  const devigged = { home: [], away: [] };
  for (const { home, away } of matching) {
    const [fh, fa] = deVig2Way(home.odds_probability, away.odds_probability);
    devigged.home.push(fh);
    devigged.away.push(fa);
  }
  const dvHome = avg(devigged.home);
  const dvAway = avg(devigged.away);

  // De-vigged consensus for pricing (same as display)
  const pinBook = matching.find(bp => bp.book === 'pinnacle');
  const pricingHome = dvHome;
  const pricingAway = dvAway;

  const pinnacle = pinBook ? {
    home: pinBook.home.odds_american,
    away: pinBook.away.odds_american,
  } : null;
  const fdBook = matching.find(bp => bp.book === 'fanduel');
  const fanduel = fdBook ? {
    home: fdBook.home.odds_american,
    away: fdBook.away.odds_american,
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

  // De-vigged consensus for display
  const devigged = { over: [], under: [] };
  for (const { over, under } of matching) {
    const [fo, fu] = deVig2Way(over.odds_probability, under.odds_probability);
    devigged.over.push(fo);
    devigged.under.push(fu);
  }
  const dvOver = avg(devigged.over);
  const dvUnder = avg(devigged.under);

  // De-vigged consensus for pricing (same as display)
  const pinBook = matching.find(bp => bp.book === 'pinnacle');
  const pricingOver = dvOver;
  const pricingUnder = dvUnder;

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
async function fetchAltLines(sport, homeTeam, awayTeam) {
  const theOddsApiKey = process.env.THE_ODDS_API_KEY;
  if (!theOddsApiKey) return null;

  const key = normalizeEventKey(homeTeam, awayTeam);

  // Check cache
  const cached = altLinesCache[key];
  if (cached && (Date.now() - cached.fetchedAt) < ALT_LINES_TTL_MS) {
    return cached;
  }

  // Need to find the Odds API event ID — look it up from the main cache
  const sportCache = oddsCache[sport];
  if (!sportCache) return null;
  const event = sportCache.events[key];
  if (!event?.eventId) return null;

  // Map our sport key to The Odds API sport key
  const oddsApiSportMap = {
    'basketball_nba': 'basketball_nba',
    'basketball_ncaab': 'basketball_ncaab',
    'baseball_mlb': 'baseball_mlb',
    'icehockey_nhl': 'icehockey_nhl',
    'soccer_usa_mls': 'soccer_usa_mls',
    'soccer_epl': 'soccer_epl',
  };
  const oddsApiSport = oddsApiSportMap[sport];
  if (!oddsApiSport) return null;

  const url = `https://api.the-odds-api.com/v4/sports/${oddsApiSport}/events/${event.eventId}/odds`
    + `?apiKey=${theOddsApiKey}`
    + `&regions=us,eu`
    + `&markets=alternate_spreads,alternate_totals`
    + `&bookmakers=pinnacle,draftkings,fanduel`
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
          // Group by point value, collect home/away
          for (const o of (market.outcomes || [])) {
            const lineKey = Math.abs(o.point);
            if (!result.altSpreads[lineKey]) result.altSpreads[lineKey] = { probs: [] };
            const isHome = o.name === homeTeam || o.name === data.home_team;
            const prob = americanToImpliedProb(o.price);
            result.altSpreads[lineKey].probs.push({ isHome, prob, point: o.point });
          }
        } else if (market.key === 'alternate_totals') {
          for (const o of (market.outcomes || [])) {
            const lineKey = o.point;
            if (!result.altTotals[lineKey]) result.altTotals[lineKey] = { probs: [] };
            const isOver = o.name === 'Over';
            const prob = americanToImpliedProb(o.price);
            result.altTotals[lineKey].probs.push({ isOver, prob });
          }
        }
      }
    }

    // De-vig each line
    for (const [lineKey, data] of Object.entries(result.altSpreads)) {
      const homeProbs = data.probs.filter(p => p.isHome).map(p => p.prob);
      const awayProbs = data.probs.filter(p => !p.isHome).map(p => p.prob);
      if (homeProbs.length > 0 && awayProbs.length > 0) {
        const [fh, fa] = deVig2Way(avg(homeProbs), avg(awayProbs));
        result.altSpreads[lineKey] = { home: fh, away: fa };
      } else {
        delete result.altSpreads[lineKey];
      }
    }

    for (const [lineKey, data] of Object.entries(result.altTotals)) {
      const overProbs = data.probs.filter(p => p.isOver).map(p => p.prob);
      const underProbs = data.probs.filter(p => !p.isOver).map(p => p.prob);
      if (overProbs.length > 0 && underProbs.length > 0) {
        const [fo, fu] = deVig2Way(avg(overProbs), avg(underProbs));
        result.altTotals[lineKey] = { over: fo, under: fu };
      } else {
        delete result.altTotals[lineKey];
      }
    }

    altLinesCache[key] = result;
    log.info('OddsFeed', `Cached alt lines: ${Object.keys(result.altSpreads).length} spreads, ${Object.keys(result.altTotals).length} totals`);
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

  if ((marketType === 'spreads' || marketType === 'totals') && market.line != null && line != null) {
    const lineDiff = Math.abs(Math.abs(market.line) - Math.abs(line));
    if (lineDiff > 0.01) {
      // Check alt lines cache
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

  if (marketType === 'h2h') {
    if (selection === 'home') return market.home?.displayFairProb || market.home?.fairProb || null;
    if (selection === 'away') return market.away?.displayFairProb || market.away?.fairProb || null;
  } else if (marketType === 'spreads') {
    if (selection === 'home') return market.home?.displayFairProb || market.home?.fairProb || null;
    if (selection === 'away') return market.away?.displayFairProb || market.away?.fairProb || null;
  } else if (marketType === 'totals') {
    if (selection === 'over') return market.over?.displayFairProb || market.over?.fairProb || null;
    if (selection === 'under') return market.under?.displayFairProb || market.under?.fairProb || null;
  }
  return null;
}

/**
 * Get Pinnacle's raw American odds for a specific selection.
 * Returns the odds integer or null if Pinnacle data not available.
 */
function getPinnacleOdds(sport, homeTeam, awayTeam, marketType, selection, targetTime) {
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event) return null;

  const market = event.markets[marketType];
  if (!market || !market.pinnacle) return null;

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

function getFanDuelOdds(sport, homeTeam, awayTeam, marketType, selection, targetTime) {
  const event = getEventMarkets(sport, homeTeam, awayTeam, targetTime);
  if (!event) return null;

  const market = event.markets[marketType];
  if (!market || !market.fanduel) return null;

  if (marketType === 'h2h' || marketType === 'spreads') {
    if (selection === 'home') return market.fanduel.home || null;
    if (selection === 'away') return market.fanduel.away || null;
  } else if (marketType === 'totals') {
    if (selection === 'over') return market.fanduel.over || null;
    if (selection === 'under') return market.fanduel.under || null;
  }
  return null;
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
      return getAltLineFairProb(key, marketType, selection, line);
    }
  }

  return null;
}

/**
 * Look up a fair prob from the alt lines cache.
 */
function getAltLineFairProb(eventKey, marketType, selection, line) {
  const alt = altLinesCache[eventKey];
  if (!alt) return null;

  if (marketType === 'spreads') {
    const lineData = alt.altSpreads[Math.abs(line)];
    if (!lineData) return null;
    if (selection === 'home') return lineData.home || null;
    if (selection === 'away') return lineData.away || null;
  } else if (marketType === 'totals') {
    const lineData = alt.altTotals[line];
    if (!lineData) return null;
    if (selection === 'over') return lineData.over || null;
    if (selection === 'under') return lineData.under || null;
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
  const events = sportCache.events[key];
  if (!events || events.length === 0) return null;

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
  return getCacheAge(sport) > config.pricing.stalePriceMinutes;
}

async function refreshAllSports() {
  const results = {};
  for (const sport of config.supportedSports) {
    try {
      const events = await fetchOddsForSport(sport);
      results[sport] = { ok: true, events: Object.keys(events).length };
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
    const totalEvents = cache ? Object.values(cache.events).reduce((s, arr) => s + arr.length, 0) : 0;
    status[sport] = cache ? {
      eventCount: totalEvents,
      ageMinutes: Math.round(getCacheAge(sport) * 10) / 10,
      stale: isStale(sport),
    } : { eventCount: 0, ageMinutes: null, stale: true };
  }
  return status;
}

function getAllCachedEvents() {
  const all = [];
  for (const sport of Object.keys(oddsCache)) {
    for (const [key, events] of Object.entries(oddsCache[sport].events)) {
      for (const event of events) {
        all.push({
          sport,
          homeTeam: event.homeTeam,
          awayTeam: event.awayTeam,
          markets: Object.keys(event.markets),
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
  return (name || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
}

/**
 * Clean team names from SharpAPI (removes pitcher info like "(TBD)").
 */
function cleanTeamName(name) {
  return (name || '').replace(/\s*\([^)]*\)\s*$/, '').trim();
}

module.exports = {
  fetchOddsForSport,
  refreshAllSports,
  getFairProb,
  getFairProbAsync,
  getPinnacleOdds,
  getDisplayFairProb,
  getFanDuelOdds,
  getDNBFairProb,
  fetchAltLines,
  getEventMarkets,
  getLiveEventMarkets,
  getLiveFairProb,
  getLiveCacheStatus,
  getCacheAge,
  isStale,
  getCacheStatus,
  getAllCachedEvents,
  normalizeTeamName,
  deVig2Way,
  americanToImpliedProb,
};
