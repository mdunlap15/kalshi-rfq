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

// DK sport-specific config. Both NBA and NHL playoffs use the same
// series-props / winner subcategory layout; only the URL slug differs.
// The subcategoryId in the intercepted XHR is the same value for both
// (DK reuses it across sports) — we key the capture filter on the URL.
const SPORT_CONFIGS = {
  nba: {
    url: 'https://sportsbook.draftkings.com/leagues/basketball/nba?category=series-props&subcategory=winner',
    subcategoryId: '18082',
  },
  nhl: {
    url: 'https://sportsbook.draftkings.com/leagues/hockey/nhl?category=series-props&subcategory=winner',
    // NHL uses a different subcategoryId than NBA (18082) despite the
    // same URL layout. Probed live: 8 series return under 17803.
    subcategoryId: '17803',
  },
};

// Per-sport cache & in-flight dedupe.
const cacheBySport = {}; // sport -> { at, data }
const inFlightBySport = {};

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
async function fetchSeriesWinners(sport, { force = false } = {}) {
  const cfg = SPORT_CONFIGS[sport];
  if (!cfg) throw new Error(`Unknown sport '${sport}' — supported: ${Object.keys(SPORT_CONFIGS).join(', ')}`);
  const cache = cacheBySport[sport];
  if (!force && cache && Date.now() - cache.at < CACHE_TTL_MS) return cache.data;
  if (inFlightBySport[sport]) return inFlightBySport[sport];

  inFlightBySport[sport] = (async () => {
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
        if (!url.includes(cfg.subcategoryId)) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (data && data.selections && data.events && data.markets) captured.push(data);
        } catch { /* ignore */ }
      });

      await page.goto(cfg.url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
      await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS));

      if (captured.length === 0) throw new Error(`DK scraper: no ${sport.toUpperCase()} series winner payload captured`);

      const parsed = parseSeriesData(captured[0]);
      parsed.sport = sport;
      cacheBySport[sport] = { at: Date.now(), data: parsed };
      log.info('DkScraper', `${sport.toUpperCase()} series winners: ${parsed.series.length} series (${Date.now() - startedAt}ms)`);
      return parsed;
    } finally {
      await browser.close();
      delete inFlightBySport[sport];
    }
  })().catch(err => { delete inFlightBySport[sport]; throw err; });
  return inFlightBySport[sport];
}

// Back-compat alias for the existing /nba-series-prices endpoint.
function fetchNbaSeriesWinners(opts) { return fetchSeriesWinners('nba', opts); }
function fetchNhlSeriesWinners(opts) { return fetchSeriesWinners('nhl', opts); }

/**
 * Fetch MMA fight moneylines from DK. Structure differs from series:
 * DK's /leagues/mma/ufc page fires ONE primaryMarkets XHR per event
 * (not a bulk subcategory call), each containing Moneyline + Point
 * Spread + Total Rounds. We intercept every XHR and pick out the
 * Moneyline selections. Covers UFC Fight Night prelims that our
 * Odds API feed misses entirely (~10 of 12 fights on a typical card).
 *
 * Returns { fetchedAt, fights: [{ eventId, eventName, startTime, vig,
 *   fighters: [{ fighter, decimalOdds, americanOdds, impliedProb, fairProb }] }] }
 */
async function fetchMmaFightOdds({ force = false } = {}) {
  if (!force && cacheBySport.mma && Date.now() - cacheBySport.mma.at < CACHE_TTL_MS) {
    return cacheBySport.mma.data;
  }
  if (inFlightBySport.mma) return inFlightBySport.mma;

  inFlightBySport.mma = (async () => {
    const startedAt = Date.now();
    const browser = await puppeteer().launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    try {
      const page = await browser.newPage();
      await page.setViewport({ width: 1400, height: 900 });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      const fightsById = {};
      page.on('response', async (resp) => {
        const url = resp.url();
        if (!url.includes('sportsbook-nash.draftkings.com')) return;
        if (!url.includes('primaryMarkets/v1/markets')) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (!data.events || !data.markets || !data.selections) return;
          for (const ev of data.events) {
            if (!fightsById[ev.id]) {
              fightsById[ev.id] = {
                eventId: ev.id,
                eventName: ev.name,
                startTime: ev.startEventDate || null,
                selections: [],
              };
            }
          }
          const parseSel = (sel) => {
            const trueDec = typeof sel.trueOdds === 'number' ? sel.trueOdds : null;
            const decDisplay = parseFloat(sel.displayOdds?.decimal);
            const decimal = trueDec || decDisplay || null;
            const american = sel.displayOdds?.american
              ? parseInt(String(sel.displayOdds.american).replace(/[−–—]/g, '-').replace(/[^\-0-9]/g, ''), 10)
              : null;
            return {
              decimalOdds: decimal,
              americanOdds: Number.isFinite(american) ? american : null,
              impliedProb: decimal && decimal > 0 ? 1 / decimal : null,
            };
          };
          for (const m of data.markets) {
            const name = m.marketType?.name || m.name || '';
            const ev = fightsById[m.eventId];
            if (!ev) continue;
            if (name === 'Moneyline') {
              for (const sel of data.selections) {
                if (sel.marketId !== m.id) continue;
                const p = parseSel(sel);
                ev.selections.push({ fighter: sel.label, ...p });
              }
            } else if (name === 'Total Rounds' || /total\s*rounds/i.test(name)) {
              // DK Total Rounds: each market line has Over/Under outcomes
              // with a `points` / `label` (e.g. "Over 2.5"). We group by
              // the rounds line and emit one {line, over, under} per.
              const byLine = {};
              for (const sel of data.selections) {
                if (sel.marketId !== m.id) continue;
                const p = parseSel(sel);
                const lineVal = sel.points ?? (typeof sel.label === 'string' ? parseFloat(sel.label.replace(/[^0-9.]/g, '')) : null);
                const side = /^over\b/i.test(sel.label || '') ? 'over'
                           : /^under\b/i.test(sel.label || '') ? 'under' : null;
                if (lineVal == null || !side) continue;
                if (!byLine[lineVal]) byLine[lineVal] = { line: lineVal };
                byLine[lineVal][side] = p;
              }
              if (!ev.totalsByLine) ev.totalsByLine = {};
              for (const [ln, pair] of Object.entries(byLine)) {
                if (pair.over && pair.under) ev.totalsByLine[ln] = pair;
              }
            }
          }
        } catch { /* ignore */ }
      });

      await page.goto('https://sportsbook.draftkings.com/leagues/mma/ufc', {
        waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS,
      });
      // MMA pages fire many per-event XHRs in sequence; give more time.
      await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS + 5000));

      const fights = [];
      for (const ev of Object.values(fightsById)) {
        if (ev.selections.length !== 2) continue;
        const sumImplied = ev.selections.reduce((s, x) => s + (x.impliedProb || 0), 0);
        if (sumImplied <= 0) continue;
        for (const s of ev.selections) s.fairProb = (s.impliedProb || 0) / sumImplied;
        // De-vig each total-rounds line pair (over vs under → fair probs).
        const totals = [];
        for (const ln of Object.keys(ev.totalsByLine || {}).sort((a, b) => parseFloat(a) - parseFloat(b))) {
          const t = ev.totalsByLine[ln];
          const sum = (t.over.impliedProb || 0) + (t.under.impliedProb || 0);
          if (sum <= 0) continue;
          const fairOver = (t.over.impliedProb || 0) / sum;
          const fairUnder = (t.under.impliedProb || 0) / sum;
          totals.push({
            line: parseFloat(ln),
            over: { ...t.over, fairProb: fairOver },
            under: { ...t.under, fairProb: fairUnder },
            vig: round(sum - 1, 5),
          });
        }
        fights.push({
          eventId: ev.eventId,
          eventName: ev.eventName,
          startTime: ev.startTime,
          vig: round(sumImplied - 1, 5),
          fighters: ev.selections,
          totals,
        });
      }
      fights.sort((a, b) => (a.startTime || '').localeCompare(b.startTime || ''));

      const payload = { fetchedAt: new Date().toISOString(), fights };
      cacheBySport.mma = { at: Date.now(), data: payload };
      log.info('DkScraper', `MMA fights: ${fights.length} captured (${Date.now() - startedAt}ms)`);
      return payload;
    } finally {
      await browser.close();
      delete inFlightBySport.mma;
    }
  })().catch(err => { delete inFlightBySport.mma; throw err; });
  return inFlightBySport.mma;
}

/**
 * Look up a fighter's de-vigged fair probability from the DK MMA cache.
 * Returns null if cache is cold or no match found. Uses the same
 * last-word fallback strategy as series lookups to handle diacritics
 * and minor name variants ("Thiago Moisés" vs "Thiago Moises").
 */
function lookupMmaFairProb(fighterName) {
  const cache = cacheBySport.mma;
  if (!cache || !cache.data) return null;
  const target = normalizeTeamName(fighterName);
  if (!target) return null;
  const targetLast = target.split(' ').pop();
  for (const f of cache.data.fights) {
    for (const fighter of f.fighters) {
      const cand = normalizeTeamName(fighter.fighter);
      const base = {
        fairProb: fighter.fairProb,
        decimalOdds: fighter.decimalOdds,
        americanOdds: fighter.americanOdds,
        source: 'dk',
        eventName: f.eventName,
        startTime: f.startTime,
      };
      if (cand === target) return base;
      if (cand.endsWith(' ' + target) || target.endsWith(' ' + cand)) return base;
      const candLast = cand.split(' ').pop();
      if (candLast && candLast === targetLast) return base;
    }
  }
  return null;
}

/**
 * Normalize a team name for robust matching across sources (PX's leg
 * label like "Cleveland Cavaliers (Series)" vs DK's "CLE Cavaliers"
 * vs odds-feed's "Cleveland Cavaliers"). Strips punctuation, casing,
 * the "(Series)" suffix, and common prefix city abbreviations.
 */
function normalizeTeamName(name) {
  if (!name) return '';
  return String(name)
    .replace(/\(series\)/ig, '')
    .replace(/[.,]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

/**
 * Look up the de-vigged fair probability for a team's series winner,
 * given a sport and a team name. Returns null if no series cache is
 * warm or no match is found.
 *
 * Match strategies in order:
 *   1) exact normalized equality
 *   2) DK label (e.g. "CLE Cavaliers") contains the full PX name
 *      (after stripping "(Series)") — rare but covers edge cases
 *   3) last-N-words match (e.g. "Cavaliers" → "CLE Cavaliers")
 */
function lookupSeriesFairProb(sport, teamName) {
  const cache = cacheBySport[sport];
  if (!cache || !cache.data) return null;
  const target = normalizeTeamName(teamName);
  if (!target) return null;
  const targetLast = target.split(' ').pop();
  for (const s of cache.data.series) {
    for (const t of s.teams) {
      const candidate = normalizeTeamName(t.name);
      const base = { fairProb: t.fairProb, decimalOdds: t.decimalOdds, americanOdds: t.americanOdds, source: 'dk', eventName: s.eventName, startTime: s.startTime };
      if (candidate === target) return base;
      if (candidate.endsWith(' ' + target) || target.endsWith(' ' + candidate)) return base;
      const candLast = candidate.split(' ').pop();
      if (candLast && candLast === targetLast) return base;
    }
  }
  return null;
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
    // Prefer trueOdds (full precision) over displayOdds.decimal (rounded
    // to 2dp, which silently shifts american equivalents — e.g. true
    // 1.1818 → DK shows −550, but display "1.18" naively converts to
    // −556). trueOdds matches the american odds DK publishes.
    const trueDec = typeof sel.trueOdds === 'number' ? sel.trueOdds : null;
    const displayDec = parseFloat(sel.displayOdds?.decimal) || null;
    const decimal = trueDec || displayDec;
    const american = sel.displayOdds?.american
      ? parseInt(String(sel.displayOdds.american).replace(/[−–—]/g, '-').replace(/[^\-0-9]/g, ''), 10)
      : null;
    seriesByEvent[event.id].teams.push({
      name: sel.label,
      decimalOdds: decimal,
      americanOdds: Number.isFinite(american) ? american : null,
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

module.exports = {
  fetchSeriesWinners,
  fetchNbaSeriesWinners,
  fetchNhlSeriesWinners,
  fetchMmaFightOdds,
  parseSeriesData,
  lookupSeriesFairProb,
  lookupMmaFairProb,
  normalizeTeamName,
};
