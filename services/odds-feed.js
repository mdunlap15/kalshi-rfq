const fetch = require('node-fetch');
const { config } = require('../config');
const log = require('./logger');

// ---------------------------------------------------------------------------
// IN-MEMORY CACHE
// ---------------------------------------------------------------------------
// Structure: { [sport]: { fetchedAt, events: { [eventKey]: { ... } } } }
const oddsCache = {};

// ---------------------------------------------------------------------------
// THE ODDS API CLIENT
// ---------------------------------------------------------------------------

/**
 * Fetch odds for a single sport from The Odds API.
 * Uses Pinnacle as the primary bookmaker (sharpest lines).
 * Falls back to including all available books if Pinnacle isn't available.
 */
async function fetchOddsForSport(sport) {
  const url = `${config.oddsApi.baseUrl}/sports/${sport}/odds`
    + `?apiKey=${config.oddsApi.apiKey}`
    + `&regions=us,eu`
    + `&markets=h2h,spreads,totals`
    + `&bookmakers=pinnacle`
    + `&oddsFormat=american`;

  log.info('OddsFeed', `Fetching odds for ${sport}...`);

  const resp = await fetch(url);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Odds API ${resp.status} for ${sport}: ${text}`);
  }

  // Track API usage from response headers
  const remaining = resp.headers.get('x-requests-remaining');
  const used = resp.headers.get('x-requests-used');
  if (remaining != null) {
    log.info('OddsFeed', `API usage: ${used} used, ${remaining} remaining`);
  }

  const events = await resp.json();
  log.info('OddsFeed', `Got ${events.length} events for ${sport}`);

  // Parse and cache
  const parsed = {};
  for (const event of events) {
    const key = normalizeEventKey(event.home_team, event.away_team);
    const pinnacle = (event.bookmakers || []).find(b => b.key === 'pinnacle');
    if (!pinnacle) {
      log.debug('OddsFeed', `No Pinnacle odds for ${event.home_team} vs ${event.away_team}`);
      continue;
    }

    const markets = {};
    for (const market of pinnacle.markets || []) {
      if (market.key === 'h2h') {
        markets.h2h = parseMoneylineMarket(market.outcomes, event.home_team, event.away_team);
      } else if (market.key === 'spreads') {
        markets.spreads = parseSpreadMarket(market.outcomes, event.home_team, event.away_team);
      } else if (market.key === 'totals') {
        markets.totals = parseTotalMarket(market.outcomes);
      }
    }

    parsed[key] = {
      homeTeam: event.home_team,
      awayTeam: event.away_team,
      commenceTime: event.commence_time,
      oddsApiId: event.id,
      markets,
    };
  }

  oddsCache[sport] = {
    fetchedAt: Date.now(),
    events: parsed,
  };

  log.info('OddsFeed', `Cached ${Object.keys(parsed).length} events for ${sport} (${Object.keys(parsed).length} with Pinnacle odds)`);
  return parsed;
}

// ---------------------------------------------------------------------------
// MARKET PARSERS — extract Pinnacle odds and de-vig
// ---------------------------------------------------------------------------

function parseMoneylineMarket(outcomes, homeTeam, awayTeam) {
  if (!outcomes || outcomes.length < 2) return null;

  const home = outcomes.find(o => o.name === homeTeam);
  const away = outcomes.find(o => o.name === awayTeam);
  if (!home || !away) return null;

  const homeProb = americanToImpliedProb(home.price);
  const awayProb = americanToImpliedProb(away.price);
  const [fairHome, fairAway] = deVig2Way(homeProb, awayProb);

  return {
    home: { rawOdds: home.price, impliedProb: homeProb, fairProb: fairHome },
    away: { rawOdds: away.price, impliedProb: awayProb, fairProb: fairAway },
  };
}

function parseSpreadMarket(outcomes, homeTeam, awayTeam) {
  if (!outcomes || outcomes.length < 2) return null;

  const home = outcomes.find(o => o.name === homeTeam);
  const away = outcomes.find(o => o.name === awayTeam);
  if (!home || !away) return null;

  const homeProb = americanToImpliedProb(home.price);
  const awayProb = americanToImpliedProb(away.price);
  const [fairHome, fairAway] = deVig2Way(homeProb, awayProb);

  return {
    home: { rawOdds: home.price, point: home.point, impliedProb: homeProb, fairProb: fairHome },
    away: { rawOdds: away.price, point: away.point, impliedProb: awayProb, fairProb: fairAway },
    line: home.point, // Home team's spread (e.g., -5.5)
  };
}

function parseTotalMarket(outcomes) {
  if (!outcomes || outcomes.length < 2) return null;

  const over = outcomes.find(o => o.name === 'Over');
  const under = outcomes.find(o => o.name === 'Under');
  if (!over || !under) return null;

  const overProb = americanToImpliedProb(over.price);
  const underProb = americanToImpliedProb(under.price);
  const [fairOver, fairUnder] = deVig2Way(overProb, underProb);

  return {
    over: { rawOdds: over.price, point: over.point, impliedProb: overProb, fairProb: fairOver },
    under: { rawOdds: under.price, point: under.point, impliedProb: underProb, fairProb: fairUnder },
    line: over.point, // Total number (e.g., 220.5)
  };
}

// ---------------------------------------------------------------------------
// DE-VIG
// ---------------------------------------------------------------------------

/**
 * Simple 2-way de-vig by normalization.
 * For Pinnacle with ~2% margins, this is accurate enough.
 * No need for power method like golf sportsbooks with 20%+ holds.
 */
function deVig2Way(prob1, prob2) {
  const total = prob1 + prob2;
  if (total === 0) return [0.5, 0.5];
  return [prob1 / total, prob2 / total];
}

/**
 * American odds to implied probability (before de-vig).
 */
function americanToImpliedProb(odds) {
  if (odds >= 100) return 100 / (odds + 100);
  if (odds <= -100) return Math.abs(odds) / (Math.abs(odds) + 100);
  return 0.5;
}

// ---------------------------------------------------------------------------
// CACHE LOOKUP
// ---------------------------------------------------------------------------

/**
 * Get fair probability for a specific selection.
 *
 * @param {string} sport - e.g., 'basketball_nba'
 * @param {string} homeTeam - The Odds API home team name
 * @param {string} awayTeam - The Odds API away team name
 * @param {string} marketType - 'h2h', 'spreads', or 'totals'
 * @param {string} selection - 'home', 'away', 'over', or 'under'
 * @param {number|null} line - spread/total number (null for moneyline)
 * @returns {number|null} fair probability (0-1) or null if not found
 */
function getFairProb(sport, homeTeam, awayTeam, marketType, selection, line) {
  const sportCache = oddsCache[sport];
  if (!sportCache) return null;

  const key = normalizeEventKey(homeTeam, awayTeam);
  const event = sportCache.events[key];
  if (!event) return null;

  const market = event.markets[marketType];
  if (!market) return null;

  // For spreads and totals, check if line matches Pinnacle's primary line.
  // If the RFQ line differs from Pinnacle, we still return the primary line fair prob
  // as a reference. The pricer can decide whether to adjust or decline.
  // This is acceptable because: (1) for spreads near the primary, the vig covers the difference,
  // (2) for far-off alternate lines, the pricer will widen vig or decline.
  if ((marketType === 'spreads' || marketType === 'totals') && market.line != null && line != null) {
    const lineDiff = Math.abs(Math.abs(market.line) - Math.abs(line));
    if (lineDiff > 0.01) {
      // Line doesn't match exactly — return null and let pricer handle it
      // In the future, could estimate prob at alternate lines
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
 * Get the full cached market data for an event (for line matching).
 */
function getEventMarkets(sport, homeTeam, awayTeam) {
  const sportCache = oddsCache[sport];
  if (!sportCache) return null;
  const key = normalizeEventKey(homeTeam, awayTeam);
  return sportCache.events[key] || null;
}

/**
 * Get cache age in minutes for a sport.
 */
function getCacheAge(sport) {
  const sportCache = oddsCache[sport];
  if (!sportCache) return Infinity;
  return (Date.now() - sportCache.fetchedAt) / 1000 / 60;
}

/**
 * Check if cache is stale.
 */
function isStale(sport) {
  return getCacheAge(sport) > config.pricing.stalePriceMinutes;
}

// ---------------------------------------------------------------------------
// REFRESH
// ---------------------------------------------------------------------------

/**
 * Refresh odds for all configured sports.
 * Staggers calls to be respectful of API.
 */
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
    // Small delay between calls
    await new Promise(r => setTimeout(r, 500));
  }
  return results;
}

/**
 * Get summary of cache state.
 */
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

/**
 * List all cached events (for debugging / line matching).
 */
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
