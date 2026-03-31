const fetch = require('node-fetch');
const { config } = require('../config');
const log = require('./logger');

// ---------------------------------------------------------------------------
// IN-MEMORY CACHE
// ---------------------------------------------------------------------------
// Structure: { [league]: { fetchedAt, events: { [eventKey]: { ... } } } }
const oddsCache = {};

// SharpAPI league keys (also used as our internal sport keys)
const LEAGUE_MAP = {
  'basketball_nba': 'nba',
  'baseball_mlb': 'mlb',
  'icehockey_nhl': 'nhl',
};

// ---------------------------------------------------------------------------
// SHARPAPI CLIENT
// ---------------------------------------------------------------------------

/**
 * Fetch odds for a single league from SharpAPI.
 * Gets moneyline, spread, and total markets from all available books,
 * then de-vigs by averaging across books.
 */
async function fetchOddsForSport(sport) {
  const league = LEAGUE_MAP[sport];
  if (!league) throw new Error(`Unknown sport: ${sport}`);

  // Use explicit market types — the 'main' alias can return empty on some tiers
  const marketTypes = league === 'mlb'
    ? 'moneyline,run_line,total_runs'
    : league === 'nhl'
      ? 'moneyline,puck_line,total_goals'
      : 'moneyline,point_spread,total_points';

  const url = `${config.oddsApi.baseUrl}/odds`
    + `?league=${league}`
    + `&market=${marketTypes}`
    + `&live=false`
    + `&limit=200`;

  log.info('OddsFeed', `Fetching ${league} odds from SharpAPI...`);

  const resp = await fetch(url, {
    headers: { 'X-API-Key': config.oddsApi.apiKey },
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`SharpAPI ${resp.status} for ${league}: ${text}`);
  }

  const body = await resp.json();
  const rows = body.data || [];
  log.info('OddsFeed', `Got ${rows.length} odds rows for ${league}`);

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

  // Parse into our cache format
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
      parsed[key] = {
        homeTeam: event.homeTeam,
        awayTeam: event.awayTeam,
        commenceTime: event.commenceTime,
        eventId,
        markets,
      };
    }
  }

  oddsCache[sport] = {
    fetchedAt: Date.now(),
    events: parsed,
  };

  log.info('OddsFeed', `Cached ${Object.keys(parsed).length} events for ${league}`);
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
  const fairProbs = { home: [], away: [] };
  for (const { home, away } of bookPairs) {
    const [fh, fa] = deVig2Way(home.odds_probability, away.odds_probability);
    fairProbs.home.push(fh);
    fairProbs.away.push(fa);
  }
  return {
    home: {
      rawOdds: bookPairs[0].home.odds_american,
      impliedProb: bookPairs[0].home.odds_probability,
      fairProb: avg(fairProbs.home),
    },
    away: {
      rawOdds: bookPairs[0].away.odds_american,
      impliedProb: bookPairs[0].away.odds_probability,
      fairProb: avg(fairProbs.away),
    },
    books: bookPairs.length,
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

  const fairProbs = { home: [], away: [] };
  for (const { home, away } of matching) {
    const [fh, fa] = deVig2Way(home.odds_probability, away.odds_probability);
    fairProbs.home.push(fh);
    fairProbs.away.push(fa);
  }
  return {
    home: {
      rawOdds: matching[0].home.odds_american,
      point: pLine,
      impliedProb: matching[0].home.odds_probability,
      fairProb: avg(fairProbs.home),
    },
    away: {
      rawOdds: matching[0].away.odds_american,
      point: -pLine,
      impliedProb: matching[0].away.odds_probability,
      fairProb: avg(fairProbs.away),
    },
    line: pLine,
    books: matching.length,
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

  const fairProbs = { over: [], under: [] };
  for (const { over, under } of matching) {
    const [fo, fu] = deVig2Way(over.odds_probability, under.odds_probability);
    fairProbs.over.push(fo);
    fairProbs.under.push(fu);
  }
  return {
    over: {
      rawOdds: matching[0].over.odds_american,
      point: pLine,
      impliedProb: matching[0].over.odds_probability,
      fairProb: avg(fairProbs.over),
    },
    under: {
      rawOdds: matching[0].under.odds_american,
      point: pLine,
      impliedProb: matching[0].under.odds_probability,
      fairProb: avg(fairProbs.under),
    },
    line: pLine,
    books: matching.length,
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
// CACHE LOOKUP (same interface as before — no changes needed downstream)
// ---------------------------------------------------------------------------

function getFairProb(sport, homeTeam, awayTeam, marketType, selection, line) {
  const sportCache = oddsCache[sport];
  if (!sportCache) return null;

  const key = normalizeEventKey(homeTeam, awayTeam);
  const event = sportCache.events[key];
  if (!event) return null;

  const market = event.markets[marketType];
  if (!market) return null;

  if ((marketType === 'spreads' || marketType === 'totals') && market.line != null && line != null) {
    const lineDiff = Math.abs(Math.abs(market.line) - Math.abs(line));
    if (lineDiff > 0.01) {
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

function getEventMarkets(sport, homeTeam, awayTeam) {
  const sportCache = oddsCache[sport];
  if (!sportCache) return null;
  const key = normalizeEventKey(homeTeam, awayTeam);
  return sportCache.events[key] || null;
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
    status[sport] = cache ? {
      eventCount: Object.keys(cache.events).length,
      ageMinutes: Math.round(getCacheAge(sport) * 10) / 10,
      stale: isStale(sport),
    } : { eventCount: 0, ageMinutes: null, stale: true };
  }
  return status;
}

function getAllCachedEvents() {
  const all = [];
  for (const sport of Object.keys(oddsCache)) {
    for (const [key, event] of Object.entries(oddsCache[sport].events)) {
      all.push({
        sport,
        homeTeam: event.homeTeam,
        awayTeam: event.awayTeam,
        markets: Object.keys(event.markets),
        commenceTime: event.commenceTime,
      });
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
  getEventMarkets,
  getCacheAge,
  isStale,
  getCacheStatus,
  getAllCachedEvents,
  normalizeTeamName,
  deVig2Way,
  americanToImpliedProb,
};
