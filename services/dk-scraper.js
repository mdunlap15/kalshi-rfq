/**
 * DraftKings scraper for NBA playoff series winner odds.
 *
 * Neither SharpAPI (DK+FD game markets only) nor The Odds API expose
 * per-series moneylines. DK does — but their /sites/*-SB/api/... JSON
 * endpoints are gated by Akamai Bot Manager and return 403 for any
 * vanilla HTTP client. Puppeteer runs headless Chromium so Akamai's
 * JS challenge passes, then we intercept the XHR response the page
 * makes to subcategoryId=18082 (Series Winner).
 *
 * Cached for 15 minutes; a single in-flight promise is shared across
 * concurrent callers to avoid racing multiple Chromium instances.
 */
const log = require('./logger');

const CACHE_TTL_MS = 15 * 60 * 1000;
const NAV_TIMEOUT_MS = 60000;
const POST_NAV_WAIT_MS = 10000;
const TARGET_URL = 'https://sportsbook.draftkings.com/leagues/basketball/nba?category=series-props&subcategory=winner';
const SUBCATEGORY_ID = '18082';

let cache = null; // { at, data }
let inFlight = null;

let _puppeteer = null;
function puppeteer() {
  if (!_puppeteer) _puppeteer = require('puppeteer');
  return _puppeteer;
}

/**
 * Fetch the full parsed payload: an array of series, each with two
 * teams, decimal odds, implied probs, and de-vigged fair probs.
 *
 * Shape:
 *   {
 *     fetchedAt: ISO,
 *     series: [{
 *       eventId, eventName, startTime,
 *       vig,
 *       teams: [{ name, decimalOdds, impliedProb, fairProb }, ...]
 *     }, ...]
 *   }
 */
async function fetchNbaSeriesWinners({ force = false } = {}) {
  if (!force && cache && Date.now() - cache.at < CACHE_TTL_MS) return cache.data;
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const startedAt = Date.now();
    const browser = await puppeteer().launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
      ],
    });
    try {
      const page = await browser.newPage();
      await page.setViewport({ width: 1400, height: 900 });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      const captured = [];
      page.on('response', async (resp) => {
        const url = resp.url();
        if (!url.includes('sportsbook-nash.draftkings.com')) return;
        if (!url.includes(SUBCATEGORY_ID)) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (data && data.selections && data.events && data.markets) captured.push(data);
        } catch { /* ignore */ }
      });

      await page.goto(TARGET_URL, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
      await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS));

      if (captured.length === 0) throw new Error('DK scraper: no series winner payload captured');

      const parsed = parseSeriesData(captured[0]);
      cache = { at: Date.now(), data: parsed };
      log.info('DkScraper', `NBA series winners: ${parsed.series.length} series (${Date.now() - startedAt}ms)`);
      return parsed;
    } finally {
      await browser.close();
      inFlight = null;
    }
  })().catch(err => { inFlight = null; throw err; });
  return inFlight;
}

/**
 * Pure function: parse the raw DK payload into the canonical series shape.
 * Exported for testing.
 */
function parseSeriesData(payload) {
  const eventsById = {};
  for (const e of (payload.events || [])) eventsById[e.id] = e;
  const marketsById = {};
  for (const m of (payload.markets || [])) {
    if (m.marketType?.name === 'Series Winner') marketsById[m.id] = m;
  }

  const seriesByEvent = {};
  for (const sel of (payload.selections || [])) {
    const market = marketsById[sel.marketId];
    if (!market) continue;
    const event = eventsById[market.eventId];
    if (!event) continue;
    if (!seriesByEvent[event.id]) {
      seriesByEvent[event.id] = {
        eventId: event.id,
        eventName: event.name,
        startTime: event.startEventDate,
        marketId: market.id,
        teams: [],
      };
    }
    const decimal = parseFloat(sel.displayOdds?.decimal) || sel.trueOdds || null;
    seriesByEvent[event.id].teams.push({
      name: sel.label,
      decimalOdds: decimal,
      impliedProb: decimal && decimal > 0 ? 1 / decimal : null,
    });
  }

  // De-vig each 2-way series proportionally.
  const series = [];
  for (const s of Object.values(seriesByEvent)) {
    if (s.teams.length !== 2) continue;
    const sumImplied = s.teams.reduce((x, t) => x + (t.impliedProb || 0), 0);
    if (sumImplied <= 0) continue;
    for (const t of s.teams) t.fairProb = (t.impliedProb || 0) / sumImplied;
    s.vig = round(sumImplied - 1, 5);
    series.push(s);
  }
  series.sort((a, b) => (a.startTime || '').localeCompare(b.startTime || ''));
  return { fetchedAt: new Date().toISOString(), series };
}

function round(n, dp) { const f = Math.pow(10, dp); return Math.round(n * f) / f; }

module.exports = { fetchNbaSeriesWinners, parseSeriesData };
