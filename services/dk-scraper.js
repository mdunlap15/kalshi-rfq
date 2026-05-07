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
const { config } = require('../config');

const CACHE_TTL_MS = 15 * 60 * 1000;
const NAV_TIMEOUT_MS = 60000;
const POST_NAV_WAIT_MS = 10000;

// DK sport-specific config. Both NBA and NHL playoffs use the same
// series-props subcategory layout; only the URL league slug differs.
// We navigate through subcategories per sport in a single browser
// session and partition captured XHR payloads by marketType.name —
// robust against DK rotating their internal subcategoryIds in the
// request URL.
const SPORT_CONFIGS = {
  nba: {
    league: 'basketball/nba',
    baseUrl: 'https://sportsbook.draftkings.com/leagues/basketball/nba',
  },
  nhl: {
    league: 'hockey/nhl',
    baseUrl: 'https://sportsbook.draftkings.com/leagues/hockey/nhl',
  },
};

// Market categories we care about, identified by REGEX over
// marketType.name (not exact-string match). DK has historically
// renamed these — the canonical strings were "Series Winner" /
// "Series Spread" / "Series Total Games", but the scraper went silent
// when DK rotated to a new label. Each regex covers the historical
// label PLUS likely rename variants so a small DK relabel doesn't
// break the cache.
//
// `slugs` is the list of subcategory= URL slugs to try, in order, when
// the canonical doesn't surface XHRs containing that market type. We
// stop at the first slug that yields ≥1 matching market.
//
// If a future DK rename breaks ALL regexes, the scraper attaches the
// `seenMarketNames` diagnostic to both the thrown error and the
// payload returned by /nba-series-prices, so the operator can update
// the regex from a single Railway log line.
const MARKET_CATEGORIES = [
  {
    key: 'winner',
    canonicalName: 'Series Winner',
    re: /^(?:series\s+)?(?:winner|outright(?:\s+winner)?|to\s+win\s+series|series\s+price)$/i,
    slugs: ['winner', 'series-winner', 'series-outright', 'outright-winner', 'to-win-series'],
  },
  {
    key: 'spread',
    canonicalName: 'Series Spread',
    re: /^series\s+(?:spread|handicap|games\s+(?:handicap|spread)|margin)$/i,
    slugs: ['spread', 'series-spread', 'handicap', 'series-handicap', 'series-games-handicap'],
  },
  {
    key: 'total',
    canonicalName: 'Series Total Games',
    re: /^series\s+(?:total\s+games?|total|games\s+total|game\s+count(?:\s+over\/under)?)$/i,
    slugs: ['total-games', 'series-total-games', 'series-total', 'total', 'games-total'],
  },
];

// URL category= values to try, in order. DK's playoff series markets
// have historically lived under category=series-props; if that yields
// nothing, fall back to category=futures (the older home for series
// outright pricing) before giving up.
const URL_CATEGORIES = ['series-props', 'futures'];

function categorizeMarketName(name) {
  if (!name) return null;
  for (const c of MARKET_CATEGORIES) {
    if (c.re.test(name)) return c.key;
  }
  return null;
}

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
async function fetchSeriesMarkets(sport, { force = false } = {}) {
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

      // Captured XHR payloads grouped by category key (winner/spread/
      // total). DK fires multiple content XHRs per page load; we retain
      // any payload whose markets include at least one matching the
      // category regex. The same market may appear across multiple
      // captures (e.g. DK re-sends on subscription resume); the parser
      // dedupes by eventId/marketId. `seenMarketNames` records EVERY
      // distinct marketType.name observed across all DK XHRs — surfaced
      // in the response payload + thrown error so a future DK rename
      // is diagnosable from one Railway log line.
      const payloadsByCategoryKey = { winner: [], spread: [], total: [] };
      const seenMarketNames = new Set();

      page.on('response', async (resp) => {
        const url = resp.url();
        if (!url.includes('sportsbook-nash.draftkings.com')) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (!data || !data.selections || !data.events || !data.markets) return;
          const cats = new Set();
          for (const m of data.markets) {
            const name = m.marketType?.name;
            if (!name) continue;
            seenMarketNames.add(name);
            const k = categorizeMarketName(name);
            if (k) cats.add(k);
          }
          for (const k of cats) payloadsByCategoryKey[k].push(data);
        } catch { /* ignore */ }
      });

      // Navigate sequentially in ONE browser session. First navigation
      // pays Akamai cold-start; subsequent ones reuse the already-
      // challenged session and typically resolve in <3s.
      //
      // For each market category we try (URL_CATEGORIES × cat.slugs) in
      // order, but bail out of inner loops as soon as ≥1 matching
      // payload is captured for that category. Happy path (DK
      // unchanged): 3 navigations total, identical to the old behavior.
      // Fallback path (DK renamed slug or category): a few extra
      // navigations bounded to the same browser session.
      for (const cat of MARKET_CATEGORIES) {
        outer:
        for (const urlCat of URL_CATEGORIES) {
          for (const slug of cat.slugs) {
            if (payloadsByCategoryKey[cat.key].length > 0) break outer;
            const url = `${cfg.baseUrl}?category=${urlCat}&subcategory=${slug}`;
            try {
              await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
              await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS));
            } catch (err) {
              log.warn('DkScraper', `${sport} ${urlCat}/${slug} navigation failed: ${err.message}`);
            }
          }
        }
      }

      const winners = parseSeriesWinnerData(payloadsByCategoryKey.winner);
      const spreads = parseSeriesSpreadData(payloadsByCategoryKey.spread);
      const totals = parseSeriesTotalData(payloadsByCategoryKey.total);
      const seenList = [...seenMarketNames].sort();

      const payload = {
        fetchedAt: new Date().toISOString(),
        sport,
        series: winners,   // back-compat: /nba-series-prices reads .series
        winners,
        spreads,
        totals,
        // Empty-result diagnostic: lets the operator see WHICH
        // marketType.name strings DK is actually returning, so a regex
        // miss in MARKET_CATEGORIES is fixable without re-running
        // Puppeteer in debug mode. Only attached when at least one
        // category came up empty — keeps the happy-path response slim.
        seenMarketNames: (winners.length === 0 || spreads.length === 0 || totals.length === 0)
          ? seenList
          : undefined,
      };
      cacheBySport[sport] = { at: Date.now(), data: payload };
      log.info('DkScraper', `${sport.toUpperCase()} series: ${winners.length} winners, ${spreads.length} spreads, ${totals.length} totals (${Date.now() - startedAt}ms)`);

      if (winners.length === 0 && spreads.length === 0 && totals.length === 0) {
        const seenStr = seenList.length ? seenList.slice(0, 25).join(' | ') : '(none)';
        throw new Error(
          `DK scraper: no ${sport.toUpperCase()} series payloads captured across winner/spread/total-games. ` +
          `seenMarketNames=${seenStr}`
        );
      }
      return payload;
    } finally {
      await browser.close();
      delete inFlightBySport[sport];
    }
  })().catch(err => { delete inFlightBySport[sport]; throw err; });
  return inFlightBySport[sport];
}

// Back-compat wrapper. Existing /nba-series-prices endpoint reads
// `.series` from the returned payload; fetchSeriesMarkets populates
// that alongside the new `.spreads` / `.totals` arrays.
async function fetchSeriesWinners(sport, opts) { return fetchSeriesMarkets(sport, opts); }
function fetchNbaSeriesWinners(opts) { return fetchSeriesMarkets('nba', opts); }
function fetchNhlSeriesWinners(opts) { return fetchSeriesMarkets('nhl', opts); }
function fetchNbaSeriesSpreads(opts) { return fetchSeriesMarkets('nba', opts); }
function fetchNhlSeriesSpreads(opts) { return fetchSeriesMarkets('nhl', opts); }
function fetchNbaSeriesTotals(opts) { return fetchSeriesMarkets('nba', opts); }
function fetchNhlSeriesTotals(opts) { return fetchSeriesMarkets('nhl', opts); }

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
        // Accept any DK markets endpoint, not just primaryMarkets — alt
        // Total Rounds lines (Over 1.5, Over 3.5 etc) ride on different
        // sub-category endpoints and we'd miss them with the old strict
        // filter. The shape check below (events/markets/selections) is
        // structural so non-markets endpoints get rejected anyway.
        if (!/markets|subcategory|eventgroup|prematch|inplay/i.test(url)) return;
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
          // Total-rounds market name match. PX offers 1.5 / 2.5 / 3.5 / 4.5
          // depending on fight length, but DK exposes those as a mix of:
          //   - "Total Rounds"            (the primary line, usually the median)
          //   - "Alternate Total Rounds"  (additional Over/Under lines)
          //   - "Total Rounds (Alt)"      (rarer variant)
          //   - "Alt Rounds"
          // Plus Moneyline. Match all variants so alt lines compound into
          // the same totalsByLine map and downstream byLine fast-path can
          // resolve any PX-requested line regardless of which one DK happens
          // to label "primary".
          const isTotalRoundsMarket = (n) => /^(?:alternate\s+|alt\s+)?total\s+rounds/i.test(n)
            || /^total\s+rounds(?:\s*\(alt\))?$/i.test(n)
            || /^alt(?:ernate)?\s+rounds$/i.test(n);
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
            } else if (isTotalRoundsMarket(name)) {
              // DK Total Rounds: each market line has Over/Under outcomes
              // with a `points` / `label` (e.g. "Over 2.5"). We group by
              // the rounds line and emit one {line, over, under} per.
              // Multiple Total-Rounds markets may stream in across
              // separate XHRs (primary + alts) — we MERGE into the same
              // ev.totalsByLine map so all collected lines coexist.
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
                if (pair.over && pair.under) {
                  // First-seen wins per line so a later alt-markets XHR
                  // can't overwrite the primary's prices.
                  if (!ev.totalsByLine[ln]) ev.totalsByLine[ln] = pair;
                }
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

      // DK lazy-loads prelim/lower-card fights below the fold — only the
      // top-of-page (main + co-main) markets fire XHRs on initial render.
      // Without scrolling, prelims silently miss the scraper window and
      // the rest of the card falls back to ML-only (no Total Rounds).
      // Scroll to the bottom in steps so each batch of events triggers
      // its lazy-loaded primaryMarkets/v1/markets XHR; the response
      // listener already in place will pick them up.
      try {
        const SCROLL_STEPS = 12;
        const SCROLL_PAUSE_MS = 600;
        for (let i = 0; i < SCROLL_STEPS; i++) {
          await page.evaluate(() => window.scrollBy(0, window.innerHeight));
          await new Promise(r => setTimeout(r, SCROLL_PAUSE_MS));
        }
        // Scroll back to top so any "above-the-fold rerender" XHRs fire too,
        // then give the listener one more settle window for late XHRs.
        await page.evaluate(() => window.scrollTo(0, 0));
        await new Promise(r => setTimeout(r, 4000));
      } catch (err) {
        log.debug('DKScraper', `MMA scroll loop error (non-fatal): ${err.message}`);
      }

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
 * Fetch DK's MLB First-5-Innings markets (moneyline, run line, total
 * runs) for every MLB game DK has F5 lines posted on. Used as a third
 * source for F5 markets when SharpAPI returns Kalshi-only stubs and
 * TOA's events list doesn't yet include the game. Verified 2026-05-03:
 * Sunday afternoon games like Tampa@Toronto, Seattle@Atlanta,
 * Detroit@Boston had F5 lines on DK hours before SharpAPI/TOA had
 * any F5 data — DK is the most reliable advance source for MLB F5.
 *
 * Returns shape parallel to fetchMmaFightOdds:
 *   {
 *     fetchedAt: ISO,
 *     games: [{
 *       eventId, eventName, startTime,
 *       homeTeam, awayTeam,
 *       h2h: { home: {fairProb,impliedProb,americanOdds}, away: {...}, vig },
 *       spreads: { home: {fairProb,...,point}, away: {...}, line, vig },
 *       totalsByLine: { "5.5": { line, over, under, vig }, ... }
 *     }]
 *   }
 */
async function fetchMlbF5Odds({ force = false } = {}) {
  if (!force && cacheBySport.mlbF5 && Date.now() - cacheBySport.mlbF5.at < CACHE_TTL_MS) {
    return cacheBySport.mlbF5.data;
  }
  if (inFlightBySport.mlbF5) return inFlightBySport.mlbF5;

  inFlightBySport.mlbF5 = (async () => {
    const startedAt = Date.now();
    const browser = await puppeteer().launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    try {
      const page = await browser.newPage();
      await page.setViewport({ width: 1400, height: 900 });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      const gamesById = {};

      page.on('response', async (resp) => {
        const url = resp.url();
        if (!url.includes('sportsbook-nash.draftkings.com')) return;
        if (!/markets|subcategory|eventgroup|prematch|inplay/i.test(url)) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (!data.events || !data.markets || !data.selections) return;

          for (const ev of data.events) {
            if (!gamesById[ev.id]) {
              const teams = (ev.teamShortName1 && ev.teamShortName2)
                ? null  // we'll prefer eventName parsing below
                : null;
              gamesById[ev.id] = {
                eventId: ev.id,
                eventName: ev.name,
                startTime: ev.startEventDate || null,
                // DK event objects sometimes have teams[] with home/away order
                homeTeam: ev.team1 || ev.homeTeam || null,
                awayTeam: ev.team2 || ev.awayTeam || null,
                rawSelections: [],
              };
              // Fallback: parse "Away @ Home" from eventName if structured fields missing
              if ((!gamesById[ev.id].homeTeam || !gamesById[ev.id].awayTeam) && ev.name) {
                const m = ev.name.match(/^(.+?)\s+@\s+(.+)$/);
                if (m) {
                  gamesById[ev.id].awayTeam = gamesById[ev.id].awayTeam || m[1].trim();
                  gamesById[ev.id].homeTeam = gamesById[ev.id].homeTeam || m[2].trim();
                }
              }
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

          // F5 market name variants that DK uses interchangeably:
          //   - "1st 5 Innings" / "1st 5 Innings Moneyline" → h2h_f5
          //   - "1st 5 Innings Run Line" / "1st 5 Innings Spread" → spreads_f5
          //   - "1st 5 Innings Total Runs" / "1st 5 Innings Total" → totals_f5
          const isF5MlMarket = (n) => /^1st\s+5\s+innings(?:\s+moneyline)?$/i.test(n)
            || /^first\s+5\s+innings(?:\s+moneyline)?$/i.test(n);
          const isF5RunLineMarket = (n) => /^1st\s+5\s+innings\s+(?:run\s+line|spread)$/i.test(n)
            || /^first\s+5\s+innings\s+(?:run\s+line|spread)$/i.test(n);
          const isF5TotalMarket = (n) => /^1st\s+5\s+innings\s+(?:total\s+runs?|total)$/i.test(n)
            || /^first\s+5\s+innings\s+(?:total\s+runs?|total)$/i.test(n);

          for (const m of data.markets) {
            const ev = gamesById[m.eventId];
            if (!ev) continue;
            const name = m.marketType?.name || m.name || '';

            if (isF5MlMarket(name)) {
              for (const sel of data.selections) {
                if (sel.marketId !== m.id) continue;
                const p = parseSel(sel);
                ev.rawSelections.push({ market: 'h2h_f5', team: sel.label, ...p });
              }
            } else if (isF5RunLineMarket(name)) {
              for (const sel of data.selections) {
                if (sel.marketId !== m.id) continue;
                const p = parseSel(sel);
                const lineVal = sel.points ?? null;
                ev.rawSelections.push({ market: 'spreads_f5', team: sel.label, line: lineVal, ...p });
              }
            } else if (isF5TotalMarket(name)) {
              for (const sel of data.selections) {
                if (sel.marketId !== m.id) continue;
                const p = parseSel(sel);
                const lineVal = sel.points ?? (typeof sel.label === 'string' ? parseFloat(sel.label.replace(/[^0-9.]/g, '')) : null);
                const side = /^over\b/i.test(sel.label || '') ? 'over' : /^under\b/i.test(sel.label || '') ? 'under' : null;
                if (lineVal == null || !side) continue;
                ev.rawSelections.push({ market: 'totals_f5', side, line: lineVal, ...p });
              }
            }
          }
        } catch { /* ignore */ }
      });

      // Hit the MLB F5 sub-category URL directly. DK's URL pattern places
      // sub-categories as path segments. We try the most common variants
      // since DK has historically rotated between them.
      const F5_URLS = [
        'https://sportsbook.draftkings.com/leagues/baseball/mlb?category=1st-5-innings',
        'https://sportsbook.draftkings.com/leagues/baseball/mlb',
      ];

      for (const url of F5_URLS) {
        try {
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
          await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS));
          // Scroll loop to trigger lazy-loaded XHRs for below-fold games
          for (let i = 0; i < 8; i++) {
            await page.evaluate(() => window.scrollBy(0, window.innerHeight));
            await new Promise(r => setTimeout(r, 500));
          }
          await page.evaluate(() => window.scrollTo(0, 0));
          await new Promise(r => setTimeout(r, 3000));
        } catch (err) {
          log.debug('DKScraper', `MLB F5 nav error (${url}): ${err.message} — non-fatal`);
        }
      }

      // Build per-game structured payload. Only include games where we
      // captured BOTH sides of the moneyline (sumImplied > 0 sanity).
      const games = [];
      for (const ev of Object.values(gamesById)) {
        const mlSels = ev.rawSelections.filter(s => s.market === 'h2h_f5');
        if (mlSels.length !== 2) continue;
        const sumImplied = mlSels.reduce((s, x) => s + (x.impliedProb || 0), 0);
        if (sumImplied <= 0) continue;

        // Map labels back to home/away based on event teams
        const findFor = (teamName) => mlSels.find(s => {
          if (!s.team || !teamName) return false;
          const a = s.team.toLowerCase();
          const b = teamName.toLowerCase();
          return a === b || a.includes(b) || b.includes(a);
        });
        const homeMl = ev.homeTeam ? findFor(ev.homeTeam) : null;
        const awayMl = ev.awayTeam ? findFor(ev.awayTeam) : null;
        if (!homeMl || !awayMl) continue;

        const h2h = {
          home: { ...homeMl, fairProb: (homeMl.impliedProb || 0) / sumImplied },
          away: { ...awayMl, fairProb: (awayMl.impliedProb || 0) / sumImplied },
          vig: round(sumImplied - 1, 5),
        };

        // Spreads: pair home/away by line
        const spreadSels = ev.rawSelections.filter(s => s.market === 'spreads_f5');
        let spreads = null;
        if (spreadSels.length === 2) {
          const homeSp = ev.homeTeam ? findFor(ev.homeTeam) && spreadSels.find(s => {
            const a = (s.team || '').toLowerCase();
            const b = ev.homeTeam.toLowerCase();
            return a === b || a.includes(b) || b.includes(a);
          }) : null;
          const awaySp = ev.awayTeam ? spreadSels.find(s => {
            const a = (s.team || '').toLowerCase();
            const b = ev.awayTeam.toLowerCase();
            return a === b || a.includes(b) || b.includes(a);
          }) : null;
          if (homeSp && awaySp) {
            const spSum = (homeSp.impliedProb || 0) + (awaySp.impliedProb || 0);
            if (spSum > 0) {
              spreads = {
                home: { ...homeSp, fairProb: (homeSp.impliedProb || 0) / spSum },
                away: { ...awaySp, fairProb: (awaySp.impliedProb || 0) / spSum },
                line: homeSp.line,
                vig: round(spSum - 1, 5),
              };
            }
          }
        }

        // Totals: group by line
        const totalSels = ev.rawSelections.filter(s => s.market === 'totals_f5');
        const totalsByLine = {};
        const byLine = {};
        for (const s of totalSels) {
          if (s.line == null || !s.side) continue;
          if (!byLine[s.line]) byLine[s.line] = {};
          byLine[s.line][s.side] = s;
        }
        for (const [ln, pair] of Object.entries(byLine)) {
          if (pair.over && pair.under) {
            const sum = (pair.over.impliedProb || 0) + (pair.under.impliedProb || 0);
            if (sum > 0) {
              totalsByLine[ln] = {
                line: parseFloat(ln),
                over: { ...pair.over, fairProb: (pair.over.impliedProb || 0) / sum },
                under: { ...pair.under, fairProb: (pair.under.impliedProb || 0) / sum },
                vig: round(sum - 1, 5),
              };
            }
          }
        }

        games.push({
          eventId: ev.eventId,
          eventName: ev.eventName,
          startTime: ev.startTime,
          homeTeam: ev.homeTeam,
          awayTeam: ev.awayTeam,
          h2h,
          spreads,
          totalsByLine,
        });
      }

      const payload = { fetchedAt: new Date().toISOString(), games };
      cacheBySport.mlbF5 = { at: Date.now(), data: payload };
      log.info('DkScraper', `MLB F5: ${games.length} games captured (${Date.now() - startedAt}ms)`);
      return payload;
    } finally {
      await browser.close();
      delete inFlightBySport.mlbF5;
    }
  })().catch(err => { delete inFlightBySport.mlbF5; throw err; });
  return inFlightBySport.mlbF5;
}

// ---------------------------------------------------------------------------
// DK PLAYER-PROP SCRAPER (generic across NBA / NHL / MLB)
// ---------------------------------------------------------------------------
// Scrapes DK's player-prop sub-categories for any sport. Used as the
// final fallback when SharpAPI + TOA both lack a player prop. Operator
// directive 2026-05-03: every prop type in PROP_LAUNCH_ALLOWLIST must
// have a scraper backstop — no API gap should produce 0 prop lines.
//
// DK's market name conventions for player props:
//   NBA:  "Points O/U", "Rebounds O/U", "Assists O/U", "Three Pointers Made"
//   NHL:  "Shots on Goal"
//   MLB:  "Hits", "Home Runs", "Total Bases", "RBIs", "Strikeouts Thrown"
//
// Returns shape:
//   {
//     fetchedAt: ISO,
//     props: [{
//       sport, propType, playerName, line,
//       eventName, startTime,
//       over: { fairProb, impliedProb, americanOdds },
//       under: { fairProb, impliedProb, americanOdds },
//       vig,
//     }, ...]
//   }
const PLAYER_PROP_CONFIGS = {
  basketball_nba: {
    leaguePath: 'basketball/nba',
    sportLabel: 'NBA',
    propPatterns: [
      { propType: 'points',       regex: /^points\s*(?:o\/u|over\/under)?$/i },
      { propType: 'rebounds',     regex: /^rebounds\s*(?:o\/u|over\/under)?$/i },
      { propType: 'assists',      regex: /^assists\s*(?:o\/u|over\/under)?$/i },
      { propType: 'threes_made',  regex: /^(?:three\s+pointers\s+made|3\-?point(?:ers)?\s+made)\s*(?:o\/u)?$/i },
    ],
    subCategoryUrls: [
      'https://sportsbook.draftkings.com/leagues/basketball/nba?category=player-points',
      'https://sportsbook.draftkings.com/leagues/basketball/nba?category=player-rebounds',
      'https://sportsbook.draftkings.com/leagues/basketball/nba?category=player-assists',
      'https://sportsbook.draftkings.com/leagues/basketball/nba?category=player-threes',
      'https://sportsbook.draftkings.com/leagues/basketball/nba?category=player-combos',
    ],
  },
  icehockey_nhl: {
    leaguePath: 'hockey/nhl',
    sportLabel: 'NHL',
    propPatterns: [
      { propType: 'shots_on_goal', regex: /^(?:shots\s+on\s+goal|sog)\s*(?:o\/u)?$/i },
    ],
    subCategoryUrls: [
      'https://sportsbook.draftkings.com/leagues/hockey/nhl?category=goal-scorer',
      'https://sportsbook.draftkings.com/leagues/hockey/nhl?category=player-props',
    ],
  },
  baseball_mlb: {
    leaguePath: 'baseball/mlb',
    sportLabel: 'MLB',
    propPatterns: [
      { propType: 'hitter_hits',         regex: /^hits\s*(?:o\/u)?$/i },
      { propType: 'hitter_hr',           regex: /^home\s+runs\s*(?:o\/u)?$/i },
      { propType: 'hitter_total_bases',  regex: /^total\s+bases\s*(?:o\/u)?$/i },
      { propType: 'hitter_rbi_runs',     regex: /^rbis?\s*(?:o\/u)?$/i },
    ],
    subCategoryUrls: [
      'https://sportsbook.draftkings.com/leagues/baseball/mlb?category=batter-props',
      'https://sportsbook.draftkings.com/leagues/baseball/mlb?category=player-hits',
      'https://sportsbook.draftkings.com/leagues/baseball/mlb?category=player-home-runs',
    ],
  },
};

async function fetchDkPlayerProps(sport, { force = false } = {}) {
  const cfg = PLAYER_PROP_CONFIGS[sport];
  if (!cfg) throw new Error(`Unknown sport for player props: ${sport}`);
  const cacheKey = `playerProps_${sport}`;
  if (!force && cacheBySport[cacheKey] && Date.now() - cacheBySport[cacheKey].at < CACHE_TTL_MS) {
    return cacheBySport[cacheKey].data;
  }
  if (inFlightBySport[cacheKey]) return inFlightBySport[cacheKey];

  inFlightBySport[cacheKey] = (async () => {
    const startedAt = Date.now();
    const browser = await puppeteer().launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    try {
      const page = await browser.newPage();
      await page.setViewport({ width: 1400, height: 900 });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      // event metadata + raw selections collected across all sub-category URLs
      const eventsById = {};
      // raw prop selections grouped by (eventId, marketName, marketId)
      const marketRawById = {};

      page.on('response', async (resp) => {
        const url = resp.url();
        if (!url.includes('sportsbook-nash.draftkings.com')) return;
        if (!/markets|subcategory|eventgroup|prematch|inplay/i.test(url)) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (!data.events || !data.markets || !data.selections) return;

          for (const ev of data.events) {
            if (!eventsById[ev.id]) {
              eventsById[ev.id] = {
                eventId: ev.id,
                eventName: ev.name,
                startTime: ev.startEventDate || null,
              };
            }
          }
          for (const m of data.markets) {
            const name = m.marketType?.name || m.name || '';
            // Match against any pattern for this sport
            const propMatch = cfg.propPatterns.find(p => p.regex.test(name));
            if (!propMatch) continue;
            if (!marketRawById[m.id]) {
              marketRawById[m.id] = {
                marketId: m.id,
                eventId: m.eventId,
                propType: propMatch.propType,
                marketName: name,
                selections: [],
              };
            }
          }
          for (const sel of data.selections) {
            const market = marketRawById[sel.marketId];
            if (!market) continue;
            const trueDec = typeof sel.trueOdds === 'number' ? sel.trueOdds : null;
            const decDisplay = parseFloat(sel.displayOdds?.decimal);
            const decimal = trueDec || decDisplay || null;
            const american = sel.displayOdds?.american
              ? parseInt(String(sel.displayOdds.american).replace(/[−–—]/g, '-').replace(/[^\-0-9]/g, ''), 10)
              : null;
            const lineVal = sel.points ?? (typeof sel.label === 'string' ? parseFloat((sel.label.match(/[\d.]+/) || [''])[0]) : null);
            const side = /\bover\b/i.test(sel.label || '') ? 'over' : /\bunder\b/i.test(sel.label || '') ? 'under' : null;
            // playerName comes from sel.outcomeType or sel.player or first line of sel.label before "Over/Under"
            const playerName = sel.participant || sel.player || sel.outcomeType
              || (sel.label || '').replace(/\s*(over|under)\s+[\d.]+.*/i, '').trim() || null;
            market.selections.push({
              playerName,
              line: lineVal,
              side,
              americanOdds: Number.isFinite(american) ? american : null,
              decimalOdds: decimal,
              impliedProb: decimal && decimal > 0 ? 1 / decimal : null,
            });
          }
        } catch { /* ignore */ }
      });

      // Visit each sub-category URL in turn; scroll to trigger lazy loads
      for (const url of cfg.subCategoryUrls) {
        try {
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
          await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS));
          for (let i = 0; i < 6; i++) {
            await page.evaluate(() => window.scrollBy(0, window.innerHeight));
            await new Promise(r => setTimeout(r, 500));
          }
          await page.evaluate(() => window.scrollTo(0, 0));
          await new Promise(r => setTimeout(r, 2500));
        } catch (err) {
          log.debug('DKScraper', `${cfg.sportLabel} player-prop nav (${url}) error: ${err.message}`);
        }
      }

      // Build flat props list — pair Over+Under selections from each market
      const props = [];
      for (const market of Object.values(marketRawById)) {
        // Group selections by (playerName, line) — DK markets can carry
        // multiple players on a single market_id (e.g. all NBA points
        // O/Us in one payload), each as a pair of Over/Under selections.
        const byKey = {};
        for (const sel of market.selections) {
          if (!sel.playerName || sel.line == null || !sel.side) continue;
          const k = `${sel.playerName}|${sel.line}`;
          if (!byKey[k]) byKey[k] = { playerName: sel.playerName, line: sel.line };
          byKey[k][sel.side] = sel;
        }
        for (const pair of Object.values(byKey)) {
          if (!pair.over || !pair.under) continue;
          const sumImplied = (pair.over.impliedProb || 0) + (pair.under.impliedProb || 0);
          if (sumImplied <= 0) continue;
          const ev = eventsById[market.eventId] || {};
          props.push({
            sport,
            propType: market.propType,
            playerName: pair.playerName,
            line: pair.line,
            eventId: market.eventId,
            eventName: ev.eventName || null,
            startTime: ev.startTime || null,
            over: { ...pair.over, fairProb: (pair.over.impliedProb || 0) / sumImplied },
            under: { ...pair.under, fairProb: (pair.under.impliedProb || 0) / sumImplied },
            vig: round(sumImplied - 1, 5),
          });
        }
      }

      const payload = { fetchedAt: new Date().toISOString(), props };
      cacheBySport[cacheKey] = { at: Date.now(), data: payload };
      // Counts per propType for log visibility
      const byType = {};
      for (const p of props) byType[p.propType] = (byType[p.propType] || 0) + 1;
      const summary = Object.entries(byType).map(([k, n]) => `${k}=${n}`).join(', ') || '(none)';
      log.info('DkScraper', `${cfg.sportLabel} player props: ${props.length} captured (${Date.now() - startedAt}ms) [${summary}]`);
      return payload;
    } finally {
      await browser.close();
      delete inFlightBySport[cacheKey];
    }
  })().catch(err => { delete inFlightBySport[cacheKey]; throw err; });
  return inFlightBySport[cacheKey];
}

// ---------------------------------------------------------------------------
// DK GAME-LINE SCRAPER (generic across all team sports)
// ---------------------------------------------------------------------------
// Catches the moneyline / spread / total / team_total markets DK posts on
// every major team-sport league. Used as the third fallback when SharpAPI
// returns Kalshi-only stubs and TOA's events list doesn't include the
// matchup. Operator directive 2026-05-03: every primary-feed gap should
// have a scraper backstop — no upstream API failure should produce 0 lines
// for markets we typically have.
//
// Returns:
//   {
//     fetchedAt: ISO,
//     games: [{
//       eventId, eventName, startTime,
//       homeTeam, awayTeam,
//       h2h: { home: {fairProb,...}, away: {...}, vig },
//       spreads: { home: {...,point}, away: {...}, line, vig },
//       totalsByLine: { "5.5": { line, over, under, vig }, ... },
//       teamTotalsByLine: {
//         home: { "117.5": { line, over, under, vig }, ... },
//         away: { ... },
//       },
//     }, ...]
//   }
const GAME_LINE_CONFIGS = {
  basketball_nba: {
    leaguePath: 'basketball/nba',
    label: 'NBA',
    spreadName: /^point\s+spread$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:point\s+)?spread$/i,
    totalName: /^total(?:\s+points)?$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+points)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+points)?|home\s+team\s+total|away\s+team\s+total)$/i,
  },
  baseball_mlb: {
    leaguePath: 'baseball/mlb',
    label: 'MLB',
    spreadName: /^run\s+line$/i,
    altSpreadName: /^alt(?:ernate)?\s+run\s+line$/i,
    totalName: /^total(?:\s+runs)?$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+runs)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+runs)?|home\s+team\s+total|away\s+team\s+total)$/i,
  },
  icehockey_nhl: {
    leaguePath: 'hockey/nhl',
    label: 'NHL',
    spreadName: /^puck\s+line$/i,
    altSpreadName: /^alt(?:ernate)?\s+puck\s+line$/i,
    totalName: /^total(?:\s+goals)?$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?|home\s+team\s+total|away\s+team\s+total)$/i,
  },
  basketball_wnba: {
    leaguePath: 'basketball/wnba',
    label: 'WNBA',
    spreadName: /^point\s+spread$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:point\s+)?spread$/i,
    totalName: /^total(?:\s+points)?$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+points)?$/i,
    teamTotalName: /^team\s+total(?:\s+points)?$/i,
  },
  tennis: {
    leaguePath: 'tennis',  // multi-tournament; we may need to adapt URL
    label: 'Tennis',
    spreadName: /^game\s+spread$/i,
    altSpreadName: /^alt(?:ernate)?\s+game\s+spread$/i,
    totalName: /^total(?:\s+games)?$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+games)?$/i,
    teamTotalName: null,  // tennis doesn't have team_total
  },

  // Soccer leagues. DK posts moneyline as 3-way (home/draw/away) under
  // "Match Result" or "Three Way Moneyline"; we capture all three sides
  // and renormalize to 2-way DNB downstream (PX/our system uses DNB).
  // Spreads are "Asian Handicap" (1.5/2.5/etc); totals are "Total Goals"
  // or "Match Total Goals".
  soccer_epl: {
    leaguePath: 'soccer/england/premier-league',
    label: 'EPL',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?|home\s+team\s+total|away\s+team\s+total)$/i,
    threeWayMoneyline: true,
  },
  soccer_spain_la_liga: {
    leaguePath: 'soccer/spain/la-liga',
    label: 'La Liga',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
  soccer_italy_serie_a: {
    leaguePath: 'soccer/italy/serie-a',
    label: 'Serie A',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
  soccer_germany_bundesliga: {
    leaguePath: 'soccer/germany/bundesliga',
    label: 'Bundesliga',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
  soccer_france_ligue_one: {
    leaguePath: 'soccer/france/ligue-1',
    label: 'Ligue 1',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
  soccer_usa_mls: {
    leaguePath: 'soccer/usa/mls',
    label: 'MLS',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
  soccer_uefa_champs_league: {
    leaguePath: 'soccer/europe/champions-league',
    label: 'UCL',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
  soccer_uefa_europa_league: {
    leaguePath: 'soccer/europe/europa-league',
    label: 'UEL',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
  soccer_brazil_campeonato: {
    leaguePath: 'soccer/brazil/brasileirao-serie-a',
    label: 'Brasileirão',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
  soccer_mexico_ligamx: {
    leaguePath: 'soccer/mexico/liga-mx',
    label: 'Liga MX',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
  soccer_usa_nwsl: {
    leaguePath: 'soccer/usa/nwsl',
    label: 'NWSL',
    spreadName: /^(?:asian\s+handicap|spread|three\s+way\s+spread)$/i,
    altSpreadName: /^alt(?:ernate)?\s+(?:asian\s+handicap|spread)$/i,
    totalName: /^(?:total(?:\s+goals)?|over\/under)$/i,
    altTotalName: /^alt(?:ernate)?\s+total(?:\s+goals)?$/i,
    teamTotalName: /^(?:team\s+total(?:\s+goals)?)$/i,
    threeWayMoneyline: true,
  },
};

async function fetchDkGameLines(sport, { force = false } = {}) {
  const cfg = GAME_LINE_CONFIGS[sport];
  if (!cfg) throw new Error(`Unknown sport for game-line scrape: ${sport}`);
  const cacheKey = `gameLines_${sport}`;
  if (!force && cacheBySport[cacheKey] && Date.now() - cacheBySport[cacheKey].at < CACHE_TTL_MS) {
    return cacheBySport[cacheKey].data;
  }
  if (inFlightBySport[cacheKey]) return inFlightBySport[cacheKey];

  inFlightBySport[cacheKey] = (async () => {
    const startedAt = Date.now();
    const browser = await puppeteer().launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    try {
      const page = await browser.newPage();
      await page.setViewport({ width: 1400, height: 900 });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      const eventsById = {};
      const marketsByEvent = {}; // eventId → { h2hSels:[], spreadByLine:{}, totalByLine:{}, teamTotalByLine:{home:{}, away:{}} }

      const ensureBucket = (eventId) => {
        if (!marketsByEvent[eventId]) {
          marketsByEvent[eventId] = {
            h2hSels: [],
            spreadByLine: {},
            totalByLine: {},
            teamTotalByLine: { home: {}, away: {} },
          };
        }
        return marketsByEvent[eventId];
      };

      page.on('response', async (resp) => {
        const url = resp.url();
        if (!url.includes('sportsbook-nash.draftkings.com')) return;
        if (!/markets|subcategory|eventgroup|prematch|inplay/i.test(url)) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (!data.events || !data.markets || !data.selections) return;

          for (const ev of data.events) {
            if (!eventsById[ev.id]) {
              eventsById[ev.id] = {
                eventId: ev.id,
                eventName: ev.name,
                startTime: ev.startEventDate || null,
                homeTeam: ev.team1 || ev.homeTeam || null,
                awayTeam: ev.team2 || ev.awayTeam || null,
              };
              if ((!eventsById[ev.id].homeTeam || !eventsById[ev.id].awayTeam) && ev.name) {
                const m = ev.name.match(/^(.+?)\s+@\s+(.+)$/);
                if (m) {
                  eventsById[ev.id].awayTeam = eventsById[ev.id].awayTeam || m[1].trim();
                  eventsById[ev.id].homeTeam = eventsById[ev.id].homeTeam || m[2].trim();
                }
              }
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
            const eventId = m.eventId;
            const ev = eventsById[eventId];
            if (!ev) continue;
            const bucket = ensureBucket(eventId);

            // Moneyline (2-way for major US sports; 3-way for soccer:
            // "Three Way Moneyline" or "Match Result" with home/draw/away)
            if (
              cfg.threeWayMoneyline
                ? /^(?:three\s+way\s+moneyline|match\s+result|match\s+winner|moneyline|1x2)$/i.test(name)
                : /^moneyline$/i.test(name)
            ) {
              for (const sel of data.selections) {
                if (sel.marketId !== m.id) continue;
                bucket.h2hSels.push({ team: sel.label, ...parseSel(sel) });
              }
            }
            // Spread / alt spread
            else if (cfg.spreadName.test(name) || (cfg.altSpreadName && cfg.altSpreadName.test(name))) {
              for (const sel of data.selections) {
                if (sel.marketId !== m.id) continue;
                const lineVal = sel.points;
                if (lineVal == null) continue;
                const teamLabel = sel.label?.replace(/\s*[+\-]?\d+(?:\.\d+)?\s*$/, '').trim() || null;
                if (!teamLabel) continue;
                const lineKey = String(lineVal);
                if (!bucket.spreadByLine[lineKey]) bucket.spreadByLine[lineKey] = {};
                // Detect home vs away by matching team label
                const isHome = ev.homeTeam && (teamLabel.toLowerCase().includes(ev.homeTeam.toLowerCase()) || ev.homeTeam.toLowerCase().includes(teamLabel.toLowerCase()));
                const side = isHome ? 'home' : 'away';
                bucket.spreadByLine[lineKey][side] = { team: teamLabel, line: lineVal, ...parseSel(sel) };
              }
            }
            // Total / alt total
            else if (cfg.totalName.test(name) || (cfg.altTotalName && cfg.altTotalName.test(name))) {
              for (const sel of data.selections) {
                if (sel.marketId !== m.id) continue;
                const lineVal = sel.points ?? (typeof sel.label === 'string' ? parseFloat((sel.label.match(/[\d.]+/) || [''])[0]) : null);
                const sideLabel = /\bover\b/i.test(sel.label || '') ? 'over' : /\bunder\b/i.test(sel.label || '') ? 'under' : null;
                if (lineVal == null || !sideLabel) continue;
                const lineKey = String(lineVal);
                if (!bucket.totalByLine[lineKey]) bucket.totalByLine[lineKey] = { line: lineVal };
                bucket.totalByLine[lineKey][sideLabel] = { line: lineVal, ...parseSel(sel) };
              }
            }
            // Team total (per-team over/under)
            else if (cfg.teamTotalName && cfg.teamTotalName.test(name)) {
              for (const sel of data.selections) {
                if (sel.marketId !== m.id) continue;
                const label = sel.label || '';
                const sideLabel = /\bover\b/i.test(label) ? 'over' : /\bunder\b/i.test(label) ? 'under' : null;
                const lineVal = sel.points ?? (typeof label === 'string' ? parseFloat((label.match(/[\d.]+/) || [''])[0]) : null);
                if (lineVal == null || !sideLabel) continue;
                // Determine home or away by market name (DK often uses
                // "Home Team Total" / "Away Team Total" or includes the
                // team name in the market or selection)
                const homeMatch = ev.homeTeam && (label.toLowerCase().includes(ev.homeTeam.toLowerCase()) || name.toLowerCase().includes(ev.homeTeam.toLowerCase()) || /\bhome\b/i.test(name));
                const awayMatch = ev.awayTeam && (label.toLowerCase().includes(ev.awayTeam.toLowerCase()) || name.toLowerCase().includes(ev.awayTeam.toLowerCase()) || /\baway\b/i.test(name));
                const teamSide = homeMatch ? 'home' : awayMatch ? 'away' : null;
                if (!teamSide) continue;
                const lineKey = String(lineVal);
                if (!bucket.teamTotalByLine[teamSide][lineKey]) {
                  bucket.teamTotalByLine[teamSide][lineKey] = { line: lineVal };
                }
                bucket.teamTotalByLine[teamSide][lineKey][sideLabel] = { line: lineVal, ...parseSel(sel) };
              }
            }
          }
        } catch { /* ignore */ }
      });

      // Navigate to the league page + scroll to trigger lazy XHRs
      const url = `https://sportsbook.draftkings.com/leagues/${cfg.leaguePath}`;
      try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
        await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS));
        for (let i = 0; i < 8; i++) {
          await page.evaluate(() => window.scrollBy(0, window.innerHeight));
          await new Promise(r => setTimeout(r, 500));
        }
        await page.evaluate(() => window.scrollTo(0, 0));
        await new Promise(r => setTimeout(r, 3000));
      } catch (err) {
        log.debug('DKScraper', `${cfg.label} game-line nav error: ${err.message}`);
      }

      // Build per-game payload: de-vig moneyline pairs, spread pairs, total pairs, team-totals.
      const games = [];
      for (const ev of Object.values(eventsById)) {
        const bucket = marketsByEvent[ev.eventId];
        if (!bucket) continue;

        // h2h — 2-way for major US sports; 3-way for soccer (renormalize to
        // 2-way DNB by dropping draw and proportionally splitting home/away).
        // Output shape stays { home, away, vig } in both cases so downstream
        // code is identical.
        let h2h = null;
        if (bucket.h2hSels.length >= 2) {
          const findFor = (tn) => bucket.h2hSels.find(s => s.team && tn && (
            s.team.toLowerCase() === tn.toLowerCase()
            || s.team.toLowerCase().includes(tn.toLowerCase())
            || tn.toLowerCase().includes(s.team.toLowerCase())
          ));
          const homeMl = ev.homeTeam ? findFor(ev.homeTeam) : null;
          const awayMl = ev.awayTeam ? findFor(ev.awayTeam) : null;

          if (cfg.threeWayMoneyline && bucket.h2hSels.length >= 3 && homeMl && awayMl) {
            const sumImplied3 = bucket.h2hSels.reduce((s, x) => s + (x.impliedProb || 0), 0);
            const homeImp = homeMl.impliedProb || 0;
            const awayImp = awayMl.impliedProb || 0;
            const dnbDenom = homeImp + awayImp;
            if (dnbDenom > 0) {
              h2h = {
                home: { ...homeMl, fairProb: homeImp / dnbDenom },
                away: { ...awayMl, fairProb: awayImp / dnbDenom },
                vig: round(sumImplied3 - 1, 5),
                threeWaySource: true,
              };
            }
          } else if (bucket.h2hSels.length === 2 && homeMl && awayMl) {
            const sumImplied = (homeMl.impliedProb || 0) + (awayMl.impliedProb || 0);
            if (sumImplied > 0) {
              h2h = {
                home: { ...homeMl, fairProb: (homeMl.impliedProb || 0) / sumImplied },
                away: { ...awayMl, fairProb: (awayMl.impliedProb || 0) / sumImplied },
                vig: round(sumImplied - 1, 5),
              };
            }
          }
        }

        // spreads (per-line; pair home/away by line)
        const spreadsByLine = {};
        for (const [lineKey, pair] of Object.entries(bucket.spreadByLine)) {
          if (!pair.home || !pair.away) continue;
          const sum = (pair.home.impliedProb || 0) + (pair.away.impliedProb || 0);
          if (sum <= 0) continue;
          spreadsByLine[lineKey] = {
            line: parseFloat(lineKey),
            home: { ...pair.home, fairProb: (pair.home.impliedProb || 0) / sum },
            away: { ...pair.away, fairProb: (pair.away.impliedProb || 0) / sum },
            vig: round(sum - 1, 5),
          };
        }

        // totals (per-line)
        const totalsByLine = {};
        for (const [lineKey, pair] of Object.entries(bucket.totalByLine)) {
          if (!pair.over || !pair.under) continue;
          const sum = (pair.over.impliedProb || 0) + (pair.under.impliedProb || 0);
          if (sum <= 0) continue;
          totalsByLine[lineKey] = {
            line: parseFloat(lineKey),
            over: { ...pair.over, fairProb: (pair.over.impliedProb || 0) / sum },
            under: { ...pair.under, fairProb: (pair.under.impliedProb || 0) / sum },
            vig: round(sum - 1, 5),
          };
        }

        // team totals (home + away separately per line)
        const teamTotalsByLine = { home: {}, away: {} };
        for (const sideKey of ['home', 'away']) {
          for (const [lineKey, pair] of Object.entries(bucket.teamTotalByLine[sideKey] || {})) {
            if (!pair.over || !pair.under) continue;
            const sum = (pair.over.impliedProb || 0) + (pair.under.impliedProb || 0);
            if (sum <= 0) continue;
            teamTotalsByLine[sideKey][lineKey] = {
              line: parseFloat(lineKey),
              over: { ...pair.over, fairProb: (pair.over.impliedProb || 0) / sum },
              under: { ...pair.under, fairProb: (pair.under.impliedProb || 0) / sum },
              vig: round(sum - 1, 5),
            };
          }
        }

        games.push({
          eventId: ev.eventId,
          eventName: ev.eventName,
          startTime: ev.startTime,
          homeTeam: ev.homeTeam,
          awayTeam: ev.awayTeam,
          h2h, spreadsByLine, totalsByLine, teamTotalsByLine,
        });
      }

      const payload = { fetchedAt: new Date().toISOString(), games };
      cacheBySport[cacheKey] = { at: Date.now(), data: payload };
      const summary = `h2h=${games.filter(g => g.h2h).length}, sp_lines=${games.reduce((s, g) => s + Object.keys(g.spreadsByLine || {}).length, 0)}, tot_lines=${games.reduce((s, g) => s + Object.keys(g.totalsByLine || {}).length, 0)}`;
      log.info('DkScraper', `${cfg.label} game-lines: ${games.length} games captured (${Date.now() - startedAt}ms) [${summary}]`);
      return payload;
    } finally {
      await browser.close();
      delete inFlightBySport[cacheKey];
    }
  })().catch(err => { delete inFlightBySport[cacheKey]; throw err; });
  return inFlightBySport[cacheKey];
}

/**
 * Look up game-line data from the DK game-line scraper cache by team-pair.
 * Returns the full game record { h2h, spreadsByLine, totalsByLine,
 * teamTotalsByLine } or null if cache cold or no match. Caller decides
 * which sub-market they need.
 */
function lookupDkGameLines(sport, homeTeam, awayTeam) {
  const cacheKey = `gameLines_${sport}`;
  const cache = cacheBySport[cacheKey];
  if (!cache || !cache.data) return null;
  const targetHome = normalizeTeamName(homeTeam || '');
  const targetAway = normalizeTeamName(awayTeam || '');
  if (!targetHome || !targetAway) return null;
  const lastWord = (s) => (s || '').split(' ').pop();
  for (const g of cache.data.games) {
    if (!g.homeTeam || !g.awayTeam) continue;
    const gHome = normalizeTeamName(g.homeTeam);
    const gAway = normalizeTeamName(g.awayTeam);
    const straight = (gHome === targetHome || gHome.includes(targetHome) || targetHome.includes(gHome))
                  && (gAway === targetAway || gAway.includes(targetAway) || targetAway.includes(gAway));
    const flipped = (gHome === targetAway || gHome.includes(targetAway) || targetAway.includes(gHome))
                 && (gAway === targetHome || gAway.includes(targetHome) || targetHome.includes(gAway));
    if (straight || flipped) {
      return { ...g, _flipped: flipped };
    }
    // Last-word fallback
    if (lastWord(gHome) === lastWord(targetHome) && lastWord(gAway) === lastWord(targetAway)
        && lastWord(targetHome).length >= 4) return { ...g, _flipped: false };
    if (lastWord(gHome) === lastWord(targetAway) && lastWord(gAway) === lastWord(targetHome)
        && lastWord(targetHome).length >= 4) return { ...g, _flipped: true };
  }
  return null;
}

/**
 * Look up a player prop fair prob from the DK player-prop scraper cache.
 * Returns { fairProbOver, fairProbUnder, books:['draftkings'], booksWithBothSides:1, ... }
 * matching the lookupTheOddsApiPlayerProp shape so the prop-bridge caller
 * can swap them transparently. Returns null if cache is cold or no match.
 */
function lookupDkPlayerPropFairProb(sport, propType, playerName, line) {
  const cacheKey = `playerProps_${sport}`;
  const cache = cacheBySport[cacheKey];
  if (!cache || !cache.data) return null;
  const targetPlayer = normalizeTeamName(playerName);
  if (!targetPlayer) return null;
  const targetLast = targetPlayer.split(' ').pop();
  for (const p of cache.data.props) {
    if (p.propType !== propType) continue;
    if (line != null && Math.abs((p.line ?? -1e9) - line) > 0.01) continue;
    const cand = normalizeTeamName(p.playerName || '');
    const candLast = cand.split(' ').pop();
    const playerOk = cand === targetPlayer
      || cand.includes(targetPlayer)
      || targetPlayer.includes(cand)
      || (candLast && targetLast && candLast === targetLast && candLast.length >= 4);
    if (!playerOk) continue;
    return {
      fairProbOver: p.over.fairProb,
      fairProbUnder: p.under.fairProb,
      books: ['draftkings'],
      booksWithBothSides: 1,
      resolvedEventId: p.eventId,
      fetchedAt: cache.data.fetchedAt,
      source: 'dk-scraper',
    };
  }
  return null;
}

/**
 * Look up a fighter's de-vigged fair probability from the DK MMA cache.
 * Returns null if cache is cold or no match found. Uses the same
 * last-word fallback strategy as series lookups to handle diacritics
 * and minor name variants ("Thiago Moisés" vs "Thiago Moises").
 *
 * When `opponentName` is provided, the lookup REQUIRES both fighters in
 * the matched fight to match the (target, opponent) pair. This guards
 * against cross-fight last-name collisions and within-fight orientation
 * flips that produced inverted fair_prob values on the 5/9 UFC card
 * (Stephens, Gomis, Carpenter, Rębecki, Buckley — all matched against
 * the wrong fighter, returning the OPPONENT's fair_prob).
 */
function lookupMmaFairProb(fighterName, opponentName) {
  const cache = cacheBySport.mma;
  if (!cache || !cache.data) return null;
  const target = normalizeTeamName(fighterName);
  if (!target) return null;
  const opponent = opponentName ? normalizeTeamName(opponentName) : '';
  const targetLast = target.split(' ').pop();
  const opponentLast = opponent ? opponent.split(' ').pop() : '';

  // Local single-name matcher mirrors the original cascade so behavior
  // is identical for callers that don't pass opponentName.
  const matches = (cand, t, tLast) => {
    if (!cand || !t) return false;
    if (cand === t) return true;
    if (cand.endsWith(' ' + t) || t.endsWith(' ' + cand)) return true;
    if (cand.startsWith(t + ' ') || t.startsWith(cand + ' ')) return true;
    const candLast = cand.split(' ').pop();
    return !!(candLast && candLast === tLast);
  };

  let fallbackBase = null; // first single-name hit (legacy behavior) for callers without opponent

  for (const f of cache.data.fights) {
    if (!Array.isArray(f.fighters) || f.fighters.length < 2) continue;
    // Try every (target-side, opponent-side) assignment within this fight.
    for (let i = 0; i < f.fighters.length; i++) {
      const fighter = f.fighters[i];
      const cand = normalizeTeamName(fighter.fighter);
      if (!matches(cand, target, targetLast)) continue;
      const base = {
        fairProb: fighter.fairProb,
        decimalOdds: fighter.decimalOdds,
        americanOdds: fighter.americanOdds,
        source: 'dk',
        eventName: f.eventName,
        startTime: f.startTime,
      };
      if (!opponent) {
        // Legacy single-name path — preserve original "first hit wins".
        if (!fallbackBase) fallbackBase = base;
        continue;
      }
      // Verify the OTHER fighter in this fight matches the opponent.
      // Without this check, last-word collisions across different fights
      // (or paired fighters with overlapping surnames) silently return
      // the opponent's fair_prob, inverting the price.
      const otherFighter = f.fighters.find((_, j) => j !== i);
      if (!otherFighter) continue;
      const otherCand = normalizeTeamName(otherFighter.fighter);
      if (matches(otherCand, opponent, opponentLast)) return base;
    }
  }
  return fallbackBase;
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
  for (const s of (cache.data.series || cache.data.winners || [])) {
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
 * Match a candidate DK team name string against a target using our
 * standard cascade: exact → endsWith → last-word. Returns true on match.
 */
function teamNameMatches(candidate, target, targetLast) {
  if (candidate === target) return true;
  if (candidate.endsWith(' ' + target) || target.endsWith(' ' + candidate)) return true;
  // Prefix match — PX series-spread selections come through as bare
  // abbreviations (e.g. 'DEN', 'MIN'), while DK stores the abbrev +
  // mascot ('DEN Nuggets'). Same for '(Series)' selections that got
  // pre-stripped to just an abbreviation.
  if (candidate.startsWith(target + ' ') || target.startsWith(candidate + ' ')) return true;
  const candLast = candidate.split(' ').pop();
  if (candLast && candLast === targetLast) return true;
  // Single-word target that matches the first word of candidate (handles
  // 'DEN' vs 'DEN Nuggets' when neither last-word nor prefix with space
  // applies — shouldn't normally happen after above checks but safety).
  const candFirst = (candidate.split(' ')[0]) || '';
  return !!(candFirst && candFirst === target);
}

/**
 * Look up a team's series-spread fair prob at a specific line + side.
 *   sport   — 'nba' or 'nhl'
 *   teamName — PX team name (will be normalized; "(Series)" suffix stripped)
 *   line     — absolute line magnitude (e.g. 1.5)
 *   side     — '+' for underdog-style (team +1.5), '-' for favorite-style
 * Returns { fairProb, decimalOdds, americanOdds, source, eventName, startTime, line, side } or null.
 */
function lookupSeriesSpreadFairProb(sport, teamName, line, side) {
  const cache = cacheBySport[sport];
  if (!cache || !cache.data || !Array.isArray(cache.data.spreads)) return null;
  const target = normalizeTeamName(teamName);
  if (!target) return null;
  const targetLast = target.split(' ').pop();
  const normalizedSide = /[-−–—]/.test(String(side)) ? '-' : '+';
  const numLine = Number(line);
  if (!Number.isFinite(numLine)) return null;
  for (const s of cache.data.spreads) {
    if (Math.abs(s.line - Math.abs(numLine)) > 1e-6) continue;
    for (const t of s.teams) {
      if (t.side !== normalizedSide) continue;
      const cand = normalizeTeamName(t.name);
      if (!teamNameMatches(cand, target, targetLast)) continue;
      return {
        fairProb: t.fairProb,
        decimalOdds: t.decimalOdds,
        americanOdds: t.americanOdds,
        source: 'dk',
        eventName: s.eventName,
        startTime: s.startTime,
        line: s.line,
        side: t.side,
      };
    }
  }
  return null;
}

/**
 * Look up a series-total-games fair prob at a given line + side. Any
 * participating team name identifies the series (DK's Over/Under labels
 * don't carry team names — we identify the event by matching either
 * home or away against DK's event name).
 */
function lookupSeriesTotalFairProb(sport, homeTeam, awayTeam, line, side) {
  const cache = cacheBySport[sport];
  if (!cache || !cache.data || !Array.isArray(cache.data.totals)) return null;
  const numLine = Number(line);
  if (!Number.isFinite(numLine)) return null;
  const sideKey = String(side || '').toLowerCase().startsWith('o') ? 'over' : 'under';
  const candidates = [homeTeam, awayTeam].filter(Boolean).map(n => {
    const norm = normalizeTeamName(n);
    return { norm, last: norm.split(' ').pop() };
  });
  if (candidates.length === 0) return null;
  for (const s of cache.data.totals) {
    if (Math.abs(s.line - numLine) > 1e-6) continue;
    const eventNorm = normalizeTeamName(s.eventName || '');
    const matched = candidates.some(c => teamNameMatches(eventNorm, c.norm, c.last)
      // Event names like "CLE Cavaliers vs TOR Raptors - Series" won't
      // match cleanly via the usual cascade; also accept a substring
      // containment check, which is safe here because we've already
      // confirmed the line matches and the home/away pair is provided.
      || eventNorm.includes(c.norm)
      || (c.last && eventNorm.includes(c.last)));
    if (!matched) continue;
    const leg = s[sideKey];
    if (!leg) continue;
    return {
      fairProb: leg.fairProb,
      decimalOdds: leg.decimalOdds,
      americanOdds: leg.americanOdds,
      source: 'dk',
      eventName: s.eventName,
      startTime: s.startTime,
      line: s.line,
      side: sideKey,
    };
  }
  return null;
}

// ---------------------------------------------------------------------------
// PARSERS — turn raw DK XHR payloads into canonical {winners|spreads|totals}
// ---------------------------------------------------------------------------

/**
 * De-vig a 2-way pair with a cap on the favorite's share of overround.
 * See config.pricing.devigFavMaxShare (default 0.5 = "additive margin").
 * Mutates the pair in place: sets `fairProb` on each leg, plus vig and
 * devigFavShare fields on the enclosing object. Returns the enclosing
 * object, or null if the pair is malformed.
 *
 *   pair: { a: { impliedProb }, b: { impliedProb }, ...otherProps }
 */
function devigPair(a, b) {
  if (!a || !b || !a.impliedProb || !b.impliedProb) return null;
  const sumImplied = (a.impliedProb || 0) + (b.impliedProb || 0);
  if (sumImplied <= 0) return null;
  const overround = sumImplied - 1;
  const favMaxShare = (config.pricing && config.pricing.devigFavMaxShare != null)
    ? config.pricing.devigFavMaxShare
    : 0.5;
  const [fav, dog] = a.impliedProb >= b.impliedProb ? [a, b] : [b, a];
  const proportionalFavShare = favMaxShare > 0 ? (fav.impliedProb / sumImplied) : 0.5;
  const favShare = Math.min(proportionalFavShare, favMaxShare);
  fav.fairProb = fav.impliedProb - favShare * overround;
  dog.fairProb = dog.impliedProb - (1 - favShare) * overround;
  return { vig: round(overround, 5), devigFavShare: round(favShare, 4) };
}

/**
 * Parse a raw DK selection into our canonical leg shape. DK selection
 * decimals come in two flavors: `trueOdds` (full precision) and
 * `displayOdds.decimal` (rounded to 2dp, which silently shifts the
 * american equivalent — e.g. true 1.1818 → DK shows −550, but "1.18"
 * naively converts to −556). We prefer trueOdds and fall back to
 * displayOdds only when trueOdds is absent.
 */
function parseSelectionOdds(sel) {
  const trueDec = typeof sel.trueOdds === 'number' ? sel.trueOdds : null;
  const displayDec = parseFloat(sel.displayOdds?.decimal) || null;
  const decimal = trueDec || displayDec;
  const american = sel.displayOdds?.american
    ? parseInt(String(sel.displayOdds.american).replace(/[−–—]/g, '-').replace(/[^\-0-9]/g, ''), 10)
    : null;
  return {
    decimalOdds: decimal,
    americanOdds: Number.isFinite(american) ? american : null,
    impliedProb: decimal && decimal > 0 ? 1 / decimal : null,
  };
}

/**
 * Shared ingest: partition events/markets from one OR more captured
 * payloads, filtered by marketType.name. The matcher accepts either
 * a literal string (exact match) or a RegExp (test against the name).
 * Returns:
 *   { eventsById, marketsById, selectionsByMarketId }
 * Markets and events are deduped by id across payloads; selections are
 * grouped by marketId and deduped by selection.id.
 */
function indexPayloads(payloads, matcher) {
  const eventsById = {};
  const marketsById = {};
  const selectionsByMarketId = {};
  const seenSelIds = new Set();
  const matches = (name) => {
    if (!name) return false;
    if (matcher instanceof RegExp) return matcher.test(name);
    return name === matcher;
  };
  for (const payload of (payloads || [])) {
    for (const e of (payload.events || [])) if (!eventsById[e.id]) eventsById[e.id] = e;
    for (const m of (payload.markets || [])) {
      if (!matches(m.marketType?.name)) continue;
      if (!marketsById[m.id]) marketsById[m.id] = m;
    }
    for (const sel of (payload.selections || [])) {
      if (!marketsById[sel.marketId]) continue;
      if (seenSelIds.has(sel.id)) continue;
      seenSelIds.add(sel.id);
      (selectionsByMarketId[sel.marketId] ||= []).push(sel);
    }
  }
  return { eventsById, marketsById, selectionsByMarketId };
}

/**
 * Parse DK Series Winner markets → array of series. Each series has
 * two teams with decimal/american odds and capped-vig fair probs.
 * Accepts an array of captured payloads; dedupes across captures.
 */
function parseSeriesWinnerData(payloads) {
  const re = MARKET_CATEGORIES.find(c => c.key === 'winner').re;
  const { eventsById, marketsById, selectionsByMarketId } = indexPayloads(payloads, re);
  const seriesByEvent = {};
  for (const marketId of Object.keys(selectionsByMarketId)) {
    const market = marketsById[marketId];
    const event = eventsById[market.eventId];
    if (!event) continue;
    const sels = selectionsByMarketId[marketId];
    if (sels.length !== 2) continue;
    const s = {
      eventId: event.id,
      eventName: event.name,
      startTime: event.startEventDate,
      marketId: market.id,
      teams: sels.map(sel => ({ name: sel.label, ...parseSelectionOdds(sel) })),
    };
    const dv = devigPair(s.teams[0], s.teams[1]);
    if (!dv) continue;
    s.vig = dv.vig; s.devigFavShare = dv.devigFavShare;
    seriesByEvent[event.id] = s;
  }
  const series = Object.values(seriesByEvent);
  series.sort((a, b) => (a.startTime || '').localeCompare(b.startTime || ''));
  return series;
}

/**
 * Parse DK Series Spread markets → array of { eventId, eventName,
 * startTime, line, teams: [{ name, side: '+'|'-', line, ... }, ...] }.
 * One entry per (event, line) combination — a single series can have
 * multiple spread lines (±1.5, ±2.5, etc.) and each is de-vigged
 * independently as its own 2-way.
 *
 * DK selection labels look like "CLE Cavaliers +1.5 games" — we parse
 * the team name, sign, and numeric magnitude from the label.
 */
function parseSeriesSpreadData(payloads) {
  const re = MARKET_CATEGORIES.find(c => c.key === 'spread').re;
  const { eventsById, marketsById, selectionsByMarketId } = indexPayloads(payloads, re);
  const labelRe = /^(.+?)\s*([+\-−–—])\s*(\d+(?:\.\d+)?)\s*games?$/i;
  const spreads = [];
  for (const marketId of Object.keys(selectionsByMarketId)) {
    const market = marketsById[marketId];
    const event = eventsById[market.eventId];
    if (!event) continue;
    const sels = selectionsByMarketId[marketId];
    if (sels.length !== 2) continue;
    const parsedSels = [];
    for (const sel of sels) {
      const m = labelRe.exec((sel.label || '').trim());
      if (!m) continue;
      const team = m[1].trim();
      const side = /[-−–—]/.test(m[2]) ? '-' : '+';
      const line = parseFloat(m[3]);
      if (!Number.isFinite(line)) continue;
      parsedSels.push({ name: team, side, line, ...parseSelectionOdds(sel) });
    }
    if (parsedSels.length !== 2) continue;
    // Sibling sanity: both selections in a pair must share the same
    // line magnitude with opposite signs (e.g. +1.5 / -1.5). Skip
    // if DK returned a mismatched pair.
    if (parsedSels[0].line !== parsedSels[1].line) continue;
    if (parsedSels[0].side === parsedSels[1].side) continue;
    const dv = devigPair(parsedSels[0], parsedSels[1]);
    if (!dv) continue;
    spreads.push({
      eventId: event.id,
      eventName: event.name,
      startTime: event.startEventDate,
      marketId: market.id,
      line: parsedSels[0].line,
      teams: parsedSels,
      vig: dv.vig,
      devigFavShare: dv.devigFavShare,
    });
  }
  spreads.sort((a, b) => (a.startTime || '').localeCompare(b.startTime || '') || a.line - b.line);
  return spreads;
}

/**
 * Parse DK Series Total Games markets → array of { eventId, eventName,
 * startTime, line, over, under, vig }. DK pairs one Over and one Under
 * per market at the same line. Labels look like "Over 5.5" / "Under 5.5".
 */
function parseSeriesTotalData(payloads) {
  const re = MARKET_CATEGORIES.find(c => c.key === 'total').re;
  const { eventsById, marketsById, selectionsByMarketId } = indexPayloads(payloads, re);
  const labelRe = /^(over|under)\s*(\d+(?:\.\d+)?)/i;
  const totals = [];
  for (const marketId of Object.keys(selectionsByMarketId)) {
    const market = marketsById[marketId];
    const event = eventsById[market.eventId];
    if (!event) continue;
    const sels = selectionsByMarketId[marketId];
    if (sels.length !== 2) continue;
    let over = null, under = null, line = null;
    for (const sel of sels) {
      const m = labelRe.exec((sel.label || '').trim());
      // Prefer outcomeType discriminator when present (DK sets it on totals).
      const outcome = (sel.outcomeType || (m && m[1]) || '').toLowerCase();
      if (!m) continue;
      const selLine = parseFloat(m[2]);
      if (!Number.isFinite(selLine)) continue;
      line = line == null ? selLine : line;
      if (outcome.startsWith('over')) over = { side: 'over', line: selLine, ...parseSelectionOdds(sel) };
      else if (outcome.startsWith('under')) under = { side: 'under', line: selLine, ...parseSelectionOdds(sel) };
    }
    if (!over || !under || over.line !== under.line) continue;
    const dv = devigPair(over, under);
    if (!dv) continue;
    totals.push({
      eventId: event.id,
      eventName: event.name,
      startTime: event.startEventDate,
      marketId: market.id,
      line: over.line,
      over,
      under,
      vig: dv.vig,
      devigFavShare: dv.devigFavShare,
    });
  }
  totals.sort((a, b) => (a.startTime || '').localeCompare(b.startTime || '') || a.line - b.line);
  return totals;
}

// Back-compat: parseSeriesData(payload) still accepted — wraps a single
// payload into the new array-based parser and returns {fetchedAt, series}.
function parseSeriesData(payload) {
  return { fetchedAt: new Date().toISOString(), series: parseSeriesWinnerData([payload]) };
}

function round(n, dp) { const f = Math.pow(10, dp); return Math.round(n * f) / f; }

// ---------------------------------------------------------------------------
// LIVE IN-PLAY ODDS SCRAPER
//
// Pulls DK's current in-play moneyline + total markets for a sport, parses
// into a shape that merges into oddsFeed.liveOddsCache. Meant to be called
// by orderTracker.refreshLiveOdds every 60s when there are confirmed
// parlays with legs on in-progress games in that sport.
//
// Output shape (per sport):
//   {
//     fetchedAt: ISO,
//     sport,
//     events: [
//       {
//         eventId, eventName, homeTeam, awayTeam, commenceTime,
//         markets: {
//           h2h:    { home: {fairProb, decimalOdds, americanOdds, ...}, away: {...}, books: 1, vig },
//           totals: { [line]: { over: {...}, under: {...}, books: 1, vig }, _primary: line },
//         }
//       },
//       ...
//     ]
//   }
//
// Skipped markets for now: spreads (line matching is fragile for live; we
// can add it once the foundation is proven). MMA/series/F5 have their own
// scrapers.
// ---------------------------------------------------------------------------
// League pages surface in-play markets inline for any game currently live.
// Previous attempt to use /live?category=...&subcategory=... returned no
// usable XHR payloads and also stalled the scrape wall-time, so we're back
// on the league URL — but now with spread parsing enabled (see below), which
// picks up DK's live spread selections whenever they render on the page.
const LIVE_SPORT_URLS = {
  basketball_nba: 'https://sportsbook.draftkings.com/leagues/basketball/nba',
  baseball_mlb:   'https://sportsbook.draftkings.com/leagues/baseball/mlb',
  icehockey_nhl:  'https://sportsbook.draftkings.com/leagues/hockey/nhl',
  americanfootball_nfl: 'https://sportsbook.draftkings.com/leagues/football/nfl',
};
const LIVE_CACHE_TTL_MS = 50 * 1000; // under the 60s refresh cadence
const liveCacheBySport = {};
const liveInFlightBySport = {};

// DK event detail URLs are `/event/<slug>/<eventId>`. The slug portion is
// cosmetic — DK routes by numeric id and the slug only affects the pretty URL
// — but a malformed slug can 404 in some routes, so we construct a sensible
// one from the event name. Example: "BOS Red Sox @ TOR Blue Jays" → "bos-red-sox-at-tor-blue-jays".
function slugifyDkEventName(name) {
  return (name || '')
    .toLowerCase()
    .replace(/@/g, 'at')
    .replace(/&/g, 'and')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'x';
}

function isInProgressEvent(ev) {
  const t = ev.startEventDate ? new Date(ev.startEventDate).getTime() : null;
  if (!t || isNaN(t)) return false;
  const elapsed = Date.now() - t;
  // Game has tipped off (startEventDate is in past) and within 6h — wider
  // than refreshLiveOdds's 4h cutoff so we can cover extras / overtime.
  return elapsed >= 0 && elapsed < 6 * 60 * 60 * 1000;
}

// DK fires many XHRs per live-page load. We grab any payload containing
// moneyline / spread / total markets, then downstream filters to in-progress
// events. We keyword-match against marketType.name rather than using an
// exact-match Set — DK varies naming across league/detail/live contexts
// (e.g. `Moneyline`, `Live Moneyline`, `Puck Line`, `Point Spread`, `Run
// Line`, `Total`, `Total Points`, `Total Runs`, `Total Goals`, `Live Total`)
// and we'd rather catch all variants than maintain an exhaustive list.
//
// Exclusions: sub-game markets (1H/2H/Q1/Period/Inning, etc.) — we only
// want full-game lines. Alternate lines are fine (we store all lines).
const SUB_GAME_RE = /(1st|2nd|3rd|4th|first|second|third|fourth|half|quarter|period|inning|1H|2H|Q1|Q2|Q3|Q4|P1|P2|P3)\b/i;
function classifyLiveMarketName(name) {
  if (!name) return null;
  if (SUB_GAME_RE.test(name)) return null;
  const n = name.toLowerCase();
  if (n.includes('moneyline')) return 'h2h';
  if (n.includes('puck line') || n.includes('run line') || n.includes('point spread') || n.includes('spread')) return 'spreads';
  if (n.includes('total')) return 'totals';
  return null;
}

async function fetchLiveMarkets(sport, { force = false } = {}) {
  const url = LIVE_SPORT_URLS[sport];
  if (!url) return { fetchedAt: new Date().toISOString(), sport, events: [], skipped: 'unsupported sport' };
  const cached = liveCacheBySport[sport];
  if (!force && cached && (Date.now() - cached.at) < LIVE_CACHE_TTL_MS) return cached.data;
  if (liveInFlightBySport[sport]) return liveInFlightBySport[sport];

  liveInFlightBySport[sport] = (async () => {
    const startedAt = Date.now();
    const browser = await puppeteer().launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    try {
      const page = await browser.newPage();
      await page.setViewport({ width: 1400, height: 900 });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      const payloads = [];
      page.on('response', async (resp) => {
        const rurl = resp.url();
        if (!rurl.includes('sportsbook-nash.draftkings.com')) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (!data || !data.selections || !data.events || !data.markets) return;
          const names = (data.markets || []).map(m => m.marketType?.name).filter(Boolean);
          if (!names.some(n => classifyLiveMarketName(n))) return;
          payloads.push(data);
        } catch { /* ignore */ }
      });

      try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
        await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS));
      } catch (err) {
        log.warn('DkScraper', `Live nav failed for ${sport}: ${err.message}`);
      }

      // PHASE 2 — per-event detail pages. The league page only fires moneyline
      // XHRs for live games in its featured strip; spread + total XHRs only
      // load when we navigate to each event's detail URL. We scan the payloads
      // accumulated so far for in-progress events, then hit each detail page
      // sequentially in the same browser session (Akamai challenge already
      // solved, so each nav is fast). Budgeted to ~40s total to stay under the
      // 60s server-side refresh cadence.
      const DETAIL_BUDGET_MS = 40000;
      const DETAIL_NAV_TIMEOUT_MS = 15000;
      const DETAIL_POST_WAIT_MS = 3500;
      const inProgressDetailEvents = [];
      const seenDetailIds = new Set();
      for (const p of payloads) {
        for (const ev of (p.events || [])) {
          if (seenDetailIds.has(ev.id)) continue;
          seenDetailIds.add(ev.id);
          if (isInProgressEvent(ev)) inProgressDetailEvents.push(ev);
        }
      }
      let detailNavs = 0, detailSkipped = 0;
      for (const ev of inProgressDetailEvents) {
        if (Date.now() - startedAt > DETAIL_BUDGET_MS) {
          detailSkipped = inProgressDetailEvents.length - detailNavs;
          break;
        }
        const slug = slugifyDkEventName(ev.name);
        const detailUrl = `https://sportsbook.draftkings.com/event/${slug}/${ev.id}`;
        try {
          await page.goto(detailUrl, { waitUntil: 'domcontentloaded', timeout: DETAIL_NAV_TIMEOUT_MS });
          await new Promise(r => setTimeout(r, DETAIL_POST_WAIT_MS));
          detailNavs++;
        } catch (err) {
          log.warn('DkScraper', `${sport} detail nav ${ev.id} failed: ${err.message}`);
        }
      }

      // Index events / markets / selections across payloads (dedup by id).
      const eventsById = {};
      const marketsById = {};
      const selectionsByMarketId = {};
      const seenSel = new Set();
      for (const p of payloads) {
        for (const e of (p.events || [])) if (!eventsById[e.id]) eventsById[e.id] = e;
        for (const m of (p.markets || [])) {
          const mn = m.marketType?.name;
          if (!mn || !classifyLiveMarketName(mn)) continue;
          if (!marketsById[m.id]) marketsById[m.id] = m;
        }
        for (const sel of (p.selections || [])) {
          if (!marketsById[sel.marketId]) continue;
          if (seenSel.has(sel.id)) continue;
          seenSel.add(sel.id);
          (selectionsByMarketId[sel.marketId] ||= []).push(sel);
        }
      }

      // Group markets by eventId, build per-event {h2h, totals}.
      const liveByEvent = {};
      for (const market of Object.values(marketsById)) {
        const ev = eventsById[market.eventId];
        if (!ev) continue;
        if (!isInProgressEvent(ev)) continue;
        const sels = selectionsByMarketId[market.id] || [];
        if (sels.length < 2) continue;
        if (!liveByEvent[ev.id]) {
          // Team names: DK events have `name` like "BOS Red Sox @ TOR Blue Jays"
          // and also `participants`. Prefer participants when present.
          let home = null, away = null;
          if (Array.isArray(ev.participants) && ev.participants.length >= 2) {
            for (const p of ev.participants) {
              const role = (p.venueRole || '').toLowerCase();
              if (role === 'home') home = p.name;
              else if (role === 'away' || role === 'visitor') away = p.name;
            }
          }
          if (!home || !away) {
            const m = /^(.+?)\s+@\s+(.+)$/.exec((ev.name || '').trim());
            if (m) { away = m[1].trim(); home = m[2].trim(); }
          }
          liveByEvent[ev.id] = {
            eventId: ev.id,
            eventName: ev.name,
            commenceTime: ev.startEventDate,
            homeTeam: home,
            awayTeam: away,
            markets: {},
          };
        }
        const bucket = liveByEvent[ev.id];
        const mname = market.marketType.name;
        const mkind = classifyLiveMarketName(mname);
        // Moneyline → h2h
        if (mkind === 'h2h') {
          // DK sends the two sides in label-order matching participants.
          // Match side by label vs team name.
          const parsedSels = sels.map(sel => ({ label: sel.label, ...parseSelectionOdds(sel) }));
          const homeSel = parsedSels.find(s => s.label && bucket.homeTeam && s.label.includes(bucket.homeTeam.split(' ').pop()));
          const awaySel = parsedSels.find(s => s.label && bucket.awayTeam && s.label.includes(bucket.awayTeam.split(' ').pop()));
          if (homeSel && awaySel && homeSel !== awaySel) {
            const dv = devigPair(homeSel, awaySel);
            if (dv) {
              bucket.markets.h2h = {
                home: { ...homeSel, displayFairProb: homeSel.fairProb },
                away: { ...awaySel, displayFairProb: awaySel.fairProb },
                books: 1,
                vig: dv.vig,
                source: 'dk-live',
              };
            }
          }
        } else if (mkind === 'totals') {
          // Pair Over + Under by line. DK selection labels are "Over N.N" / "Under N.N".
          const byOutcome = {};
          for (const sel of sels) {
            const m = /^\s*(over|under)\s+([0-9]+(?:\.[0-9]+)?)\s*$/i.exec(sel.label || '');
            if (!m) continue;
            const outcome = m[1].toLowerCase();
            const line = parseFloat(m[2]);
            if (!Number.isFinite(line)) continue;
            (byOutcome[line] ||= {})[outcome] = { line, ...parseSelectionOdds(sel) };
          }
          const totals = {};
          let primary = null;
          for (const [line, pair] of Object.entries(byOutcome)) {
            if (!pair.over || !pair.under) continue;
            const dv = devigPair(pair.over, pair.under);
            if (!dv) continue;
            totals[line] = {
              line: parseFloat(line),
              over: { ...pair.over, displayFairProb: pair.over.fairProb },
              under: { ...pair.under, displayFairProb: pair.under.fairProb },
              books: 1,
              vig: dv.vig,
              source: 'dk-live',
            };
            if (primary == null) primary = parseFloat(line);
          }
          if (Object.keys(totals).length > 0) {
            totals._primary = primary;
            bucket.markets.totals = totals;
          }
        } else if (mkind === 'spreads') {
          // DK live spread selection labels look like:
          //   "BOS Red Sox +1.5"  or "Celtics -3.5"
          // Side is encoded by the sign in front of the numeric magnitude.
          // We key the spread by the line magnitude and identify each side
          // by matching the leading team name against bucket.homeTeam /
          // awayTeam (last-word fallback to tolerate abbrev variants).
          const labelRe = /^(.+?)\s*([+\-−–—])\s*(\d+(?:\.\d+)?)\s*$/;
          const homeLast = bucket.homeTeam ? bucket.homeTeam.split(' ').pop() : null;
          const awayLast = bucket.awayTeam ? bucket.awayTeam.split(' ').pop() : null;
          const byLine = {};
          for (const sel of sels) {
            const m = labelRe.exec((sel.label || '').trim());
            if (!m) continue;
            const teamPart = m[1].trim();
            const side = /[-−–—]/.test(m[2]) ? '-' : '+';
            const line = parseFloat(m[3]);
            if (!Number.isFinite(line)) continue;
            let which = null;
            if (homeLast && teamPart.includes(homeLast)) which = 'home';
            else if (awayLast && teamPart.includes(awayLast)) which = 'away';
            if (!which) continue;
            (byLine[line] ||= {})[which] = { side, line, ...parseSelectionOdds(sel) };
          }
          const spreads = {};
          let primary = null;
          for (const [line, pair] of Object.entries(byLine)) {
            if (!pair.home || !pair.away) continue;
            if (pair.home.side === pair.away.side) continue;
            const dv = devigPair(pair.home, pair.away);
            if (!dv) continue;
            spreads[line] = {
              line: parseFloat(line),
              home: { ...pair.home, displayFairProb: pair.home.fairProb },
              away: { ...pair.away, displayFairProb: pair.away.fairProb },
              books: 1,
              vig: dv.vig,
              source: 'dk-live',
            };
            if (primary == null) primary = parseFloat(line);
          }
          if (Object.keys(spreads).length > 0) {
            spreads._primary = primary;
            bucket.markets.spreads = spreads;
          }
        }
      }

      const events = Object.values(liveByEvent).filter(e =>
        e.homeTeam && e.awayTeam && (e.markets.h2h || e.markets.totals || e.markets.spreads)
      );
      const data = {
        fetchedAt: new Date().toISOString(),
        sport,
        events,
        scrapeMs: Date.now() - startedAt,
        payloadCount: payloads.length,
      };
      liveCacheBySport[sport] = { at: Date.now(), data };
      const mlCount = events.filter(e => e.markets.h2h).length;
      const spCount = events.filter(e => e.markets.spreads).length;
      const toCount = events.filter(e => e.markets.totals).length;
      log.info('DkScraper', `${sport} live: ${events.length} events (h2h:${mlCount} spreads:${spCount} totals:${toCount}) detail-navs:${detailNavs}${detailSkipped ? ` (skipped:${detailSkipped} budget)` : ''} ${data.scrapeMs}ms, ${payloads.length} payloads`);
      return data;
    } finally {
      await browser.close();
      delete liveInFlightBySport[sport];
    }
  })().catch(err => { delete liveInFlightBySport[sport]; throw err; });
  return liveInFlightBySport[sport];
}

// ---------------------------------------------------------------------------
// GOLF MATCHUPS SCRAPER
//
// DataGolf covers individual 1v1 player matchups but NOT team pairs (the
// Zurich Classic is the only regular-season 2-man-team PGA event). Also
// covers us for any tour event DataGolf doesn't publish (the odd fringe
// tournaments, team events, silly-season stuff).
//
// DK publishes both tournament-length and per-round matchups under
// /leagues/golf/pga?category=matchups with per-round subcategory slugs.
// We pull them all in one session and tag each matchup with its scope:
//   'tournament' — tournament-length H2H
//   'round_1' … 'round_4' — single-round H2H
//
// Player pair names normalize sort-invariantly so "McIlroy/Lowry" and
// "Lowry/McIlroy" map to the same team. Last-name-only fallback
// tolerates minor name formatting variants (periods, accents).
// ---------------------------------------------------------------------------

// DK's marketType.name for golf matchups includes the round scope in
// the name itself. Pattern covers both "Tournament Matchups" and
// "Round N Matchups" (also "R1 Matchups" / "Rd 1 Matchups" variants).
const GOLF_MATCHUP_MARKET_RE = /^(tournament|round\s*\d+|rd\s*\d+|r\s*\d+)\s*match[-\s]?ups?/i;

// DK's golf league page hosts multiple subcategories under
// /category=matchups. We navigate each to make sure XHRs for every
// scope fire. Empty cells on any individual scope are fine — a
// tournament with no Round 3 lines yet will just produce no payloads
// for that slug.
const GOLF_MATCHUP_BASE = 'https://sportsbook.draftkings.com/leagues/golf/pga';
const GOLF_MATCHUP_SUBCATEGORIES = [
  { slug: 'tournament-matchups', scope: 'tournament' },
  { slug: 'round-1-matchups', scope: 'round_1' },
  { slug: 'round-2-matchups', scope: 'round_2' },
  { slug: 'round-3-matchups', scope: 'round_3' },
  { slug: 'round-4-matchups', scope: 'round_4' },
];

async function fetchGolfMatchups({ force = false } = {}) {
  if (!force && cacheBySport.golf && Date.now() - cacheBySport.golf.at < CACHE_TTL_MS) {
    return cacheBySport.golf.data;
  }
  if (inFlightBySport.golf) return inFlightBySport.golf;

  inFlightBySport.golf = (async () => {
    const startedAt = Date.now();
    const browser = await puppeteer().launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    try {
      const page = await browser.newPage();
      await page.setViewport({ width: 1400, height: 900 });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      const payloads = [];
      // Diagnostics: track EVERY market name seen in XHRs, not just the
      // ones our regex matched. If matchups come back empty we include
      // this in the response so we can tell whether DK is genuinely
      // silent on matchups vs whether our regex is too strict and
      // needs expanding for a new market-name variant.
      const seenMarketNames = new Set();
      page.on('response', async (resp) => {
        const url = resp.url();
        if (!url.includes('sportsbook-nash.draftkings.com')) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (!data || !data.selections || !data.events || !data.markets) return;
          for (const m of (data.markets || [])) {
            const n = m.marketType?.name;
            if (n) seenMarketNames.add(n);
          }
          const hasMatchup = (data.markets || []).some(m => GOLF_MATCHUP_MARKET_RE.test(m.marketType?.name || ''));
          if (hasMatchup) payloads.push(data);
        } catch { /* ignore */ }
      });

      for (const sc of GOLF_MATCHUP_SUBCATEGORIES) {
        const url = `${GOLF_MATCHUP_BASE}?category=matchups&subcategory=${sc.slug}`;
        try {
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
          await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS));
        } catch (err) {
          log.debug('DkScraper', `Golf ${sc.slug} navigation failed: ${err.message}`);
        }
      }

      const matchups = parseGolfMatchupData(payloads);
      const payload = {
        fetchedAt: new Date().toISOString(),
        sport: 'golf',
        matchups,
        scrapeMs: Date.now() - startedAt,
        payloadCount: payloads.length,
        // Only included when matchups is empty — lets operators see
        // which market names DK DID return, so a regex miss is
        // diagnosable without re-running Puppeteer in debug mode.
        seenMarketNames: matchups.length === 0 ? [...seenMarketNames].sort() : undefined,
      };
      cacheBySport.golf = { at: Date.now(), data: payload };
      const byScope = {};
      for (const m of matchups) byScope[m.scope] = (byScope[m.scope] || 0) + 1;
      const scopeStr = Object.entries(byScope).map(([k, v]) => `${k}:${v}`).join(' ');
      log.info('DkScraper', `Golf matchups: ${matchups.length} captured (${scopeStr || 'none'}) ${payload.scrapeMs}ms, ${payloads.length} payloads`);
      return payload;
    } finally {
      await browser.close();
      delete inFlightBySport.golf;
    }
  })().catch(err => { delete inFlightBySport.golf; throw err; });
  return inFlightBySport.golf;
}

/**
 * Parse DK golf matchup payloads. Each market is a 2-way between
 * two entries (single player or team pair). We don't distinguish
 * player-vs-player from team-vs-team in the parse — normalization
 * handles both. Tag each entry with its scope derived from the
 * marketType.name.
 */
function parseGolfMatchupData(payloads) {
  const eventsById = {};
  const marketsById = {};
  const selectionsByMarketId = {};
  const seenSelIds = new Set();
  for (const payload of (payloads || [])) {
    for (const e of (payload.events || [])) if (!eventsById[e.id]) eventsById[e.id] = e;
    for (const m of (payload.markets || [])) {
      if (!GOLF_MATCHUP_MARKET_RE.test(m.marketType?.name || '')) continue;
      if (!marketsById[m.id]) marketsById[m.id] = m;
    }
    for (const sel of (payload.selections || [])) {
      if (!marketsById[sel.marketId]) continue;
      if (seenSelIds.has(sel.id)) continue;
      seenSelIds.add(sel.id);
      (selectionsByMarketId[sel.marketId] ||= []).push(sel);
    }
  }
  const matchups = [];
  for (const marketId of Object.keys(selectionsByMarketId)) {
    const market = marketsById[marketId];
    const event = eventsById[market.eventId];
    if (!event) continue;
    const sels = selectionsByMarketId[marketId];
    // Golf matchups on DK are always 2-way. Ties are listed as separate
    // bets (3-way) on some events — for those the matchup is a 3-outcome
    // which we skip (can't cleanly de-vig 3-way here; would need to
    // handle "tie" outcome explicitly to use the data).
    if (sels.length !== 2) continue;
    const mtypeName = market.marketType?.name || '';
    const roundMatch = /(?:round|rd|r)\s*(\d+)/i.exec(mtypeName);
    const scope = /tournament/i.test(mtypeName)
      ? 'tournament'
      : (roundMatch ? `round_${roundMatch[1]}` : 'unknown');
    const teams = sels.map(sel => ({ name: (sel.label || '').trim(), ...parseSelectionOdds(sel) }));
    const dv = devigPair(teams[0], teams[1]);
    if (!dv) continue;
    matchups.push({
      eventId: event.id,
      eventName: event.name,
      startTime: event.startEventDate,
      marketId: market.id,
      marketName: mtypeName,
      scope,
      roundNum: roundMatch ? parseInt(roundMatch[1], 10) : null,
      teams,
      vig: dv.vig,
      devigFavShare: dv.devigFavShare,
    });
  }
  matchups.sort((a, b) => (a.startTime || '').localeCompare(b.startTime || '') || a.scope.localeCompare(b.scope));
  return matchups;
}

/**
 * Normalize a golf team/player name into a sort-invariant canonical form.
 * "Rory McIlroy / Shane Lowry" and "Shane Lowry / Rory McIlroy" both
 * produce the same key. Tolerates common separators (/ & ,).
 */
function normalizeGolfPairName(name) {
  if (!name) return '';
  return String(name)
    .replace(/&/g, '/')
    .split(/\s*[\/,]\s*/)
    .map(p => p.trim().toLowerCase()
      .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
      .replace(/[.,]/g, '')
      .replace(/\s+/g, ' ')
      .trim())
    .filter(Boolean)
    .sort()
    .join('|');
}

/**
 * Match candidate against target with exact + last-name fallback.
 * Works for both single-player ("Rory McIlroy") and team-pair
 * ("Rory McIlroy / Shane Lowry") names since both normalize into
 * the same sorted-pipe form.
 */
function golfPairNameMatches(candidate, target) {
  if (!candidate || !target) return false;
  if (candidate === target) return true;
  const lastNames = (s) => s.split('|').map(p => p.split(' ').pop()).sort().join('|');
  return lastNames(candidate) === lastNames(target);
}

/**
 * Look up a golf team-matchup fair prob by PX team name + optional round.
 * Returns null if DK cache is cold or the team isn't in a matchup at
 * the requested scope. Round null matches the tournament-length
 * matchup; round 1-4 matches that specific round's H2H.
 *
 * Caller (pricer.js getGolfMatchupFairProb) uses this as a fallback
 * after the DataGolf path returns null — which it will for team
 * events like the Zurich Classic.
 */
function lookupGolfMatchupFairProb(teamName, roundNum) {
  const cache = cacheBySport.golf;
  if (!cache || !cache.data) return null;
  const target = normalizeGolfPairName(teamName);
  if (!target) return null;
  const wantScope = roundNum == null ? 'tournament' : `round_${roundNum}`;
  for (const m of (cache.data.matchups || [])) {
    if (m.scope !== wantScope) continue;
    for (const t of m.teams) {
      const candidate = normalizeGolfPairName(t.name);
      if (golfPairNameMatches(candidate, target)) {
        return {
          fairProb: t.fairProb,
          decimalOdds: t.decimalOdds,
          americanOdds: t.americanOdds,
          source: 'dk',
          eventName: m.eventName,
          startTime: m.startTime,
          scope: m.scope,
          roundNum: m.roundNum,
        };
      }
    }
  }
  return null;
}

/**
 * DISCOVERY PROBE — disposable scraper for unknown DK URLs.
 *
 * Navigates to a given DK URL, optionally follows up with a per-event
 * detail-page visit, captures ALL JSON XHRs to sportsbook-nash.draftkings.com,
 * and returns a summary of captured markets + sample selections.
 *
 * Used during Phase 0 of the alt-lines scraper build to discover:
 *  - Which URL subcategories ("?category=...&subcategory=...") carry
 *    NBA 1H, NHL 1st Period, and team_total markets
 *  - The exact marketType.name strings we'll filter on in the real scraper
 *  - The selection format (label, line, point, price fields)
 *
 * Not production code — after the real scraper is built, this endpoint
 * stays for future reconnaissance of new market types.
 *
 * Example calls (via /debug-dk-probe endpoint):
 *   ?url=https://sportsbook.draftkings.com/leagues/basketball/nba
 *   ?url=https://sportsbook.draftkings.com/leagues/basketball/nba&sub=1st-half
 *   ?url=https://sportsbook.draftkings.com/leagues/basketball/nba&sub=team-totals
 *   ?url=https://sportsbook.draftkings.com/event/<slug>/<id>&sub=1st-half
 */
async function probeDkPage({ url, subcategory = null, postWaitMs = 10000, eventDetailNav = false, maxEventDetails = 3, captureAllDkHosts = false }) {
  const startedAt = Date.now();
  const browser = await puppeteer().launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });
  const capture = {
    navigatedUrl: null,
    elapsedMs: 0,
    xhrCount: 0,
    xhrHostCounts: {},      // when captureAllDkHosts: host → count
    xhrSampleUrls: [],      // when captureAllDkHosts: first N XHR URLs for diagnostics
    payloadShapes: [],      // when captureAllDkHosts: first N payload top-level keys (for shape discovery)
    marketTypesSeen: {},   // marketType.name -> count
    sampleEvents: [],       // first N events for matching reference
    sampleSelections: {},   // marketType.name -> up to 3 sample selections
    eventDetailCaptures: [], // if eventDetailNav: per-event summary
    errors: [],
  };
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1400, height: 900 });
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

    const allPayloads = [];
    page.on('response', async (resp) => {
      const rurl = resp.url();
      // Default behavior: only sportsbook-nash (existing /leagues/* scrapers).
      // Discovery mode (captureAllDkHosts=true): capture XHRs from any DK host
      // and tag the host so we can find which API serves /sports/* pages.
      if (captureAllDkHosts) {
        if (!/draftkings\.com/.test(rurl)) return;
      } else {
        if (!rurl.includes('sportsbook-nash.draftkings.com')) return;
      }
      try {
        const ct = resp.headers()['content-type'] || '';
        if (!ct.includes('json')) return;
        const data = await resp.json();
        if (!data || typeof data !== 'object') return;
        allPayloads.push({ url: rurl, data });
        capture.xhrCount++;
        if (captureAllDkHosts) {
          const host = (rurl.match(/^https?:\/\/([^/]+)/) || [])[1] || rurl;
          capture.xhrHostCounts[host] = (capture.xhrHostCounts[host] || 0) + 1;
          if (capture.xhrSampleUrls.length < 30) {
            // Strip query for readability; keep path for routing insight.
            capture.xhrSampleUrls.push(rurl.split('?')[0]);
          }
          if (capture.payloadShapes.length < 15 && data && typeof data === 'object') {
            capture.payloadShapes.push({
              path: rurl.split('?')[0].replace(/^https?:\/\/[^/]+/, ''),
              keys: Object.keys(data).slice(0, 12),
              sample: JSON.stringify(data).slice(0, 250),
            });
          }
        }
      } catch { /* ignore parse errors */ }
    });

    const navUrl = subcategory
      ? (url.includes('?') ? `${url}&subcategory=${subcategory}` : `${url}?category=odds&subcategory=${subcategory}`)
      : url;
    capture.navigatedUrl = navUrl;

    try {
      await page.goto(navUrl, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
      await new Promise(r => setTimeout(r, postWaitMs));
    } catch (err) {
      capture.errors.push(`primary nav: ${err.message}`);
    }

    // Aggregate market types / events / selections from all captured payloads
    const eventsSeen = new Set();
    for (const { data } of allPayloads) {
      // Events
      for (const ev of (data.events || [])) {
        if (eventsSeen.has(ev.id)) continue;
        eventsSeen.add(ev.id);
        if (capture.sampleEvents.length < 5) {
          capture.sampleEvents.push({
            id: ev.id,
            name: ev.name,
            startTime: ev.startEventDate || ev.startTime,
            slug: slugifyDkEventName(ev.name),
          });
        }
      }
      // Markets
      const selectionsByMarketId = {};
      for (const sel of (data.selections || [])) {
        if (!selectionsByMarketId[sel.marketId]) selectionsByMarketId[sel.marketId] = [];
        selectionsByMarketId[sel.marketId].push(sel);
      }
      for (const m of (data.markets || [])) {
        const mtName = m.marketType?.name;
        if (!mtName) continue;
        capture.marketTypesSeen[mtName] = (capture.marketTypesSeen[mtName] || 0) + 1;
        if (!capture.sampleSelections[mtName] || capture.sampleSelections[mtName].length < 3) {
          const sels = selectionsByMarketId[m.id] || [];
          if (sels.length > 0) {
            if (!capture.sampleSelections[mtName]) capture.sampleSelections[mtName] = [];
            // Only keep the minimal per-selection shape we'd parse in prod
            capture.sampleSelections[mtName].push({
              eventId: m.eventId,
              marketId: m.id,
              samples: sels.slice(0, 4).map(s => ({
                label: s.label,
                displayOdds: s.displayOdds,
                points: s.points,
                participants: s.participants?.map(p => p.name),
              })),
            });
          }
        }
      }
    }

    // Optional per-event detail-page follow-up (for team_totals / alt lines
    // that don't appear at the league level).
    if (eventDetailNav && capture.sampleEvents.length > 0) {
      const before = allPayloads.length;
      for (const ev of capture.sampleEvents.slice(0, maxEventDetails)) {
        const slug = slugifyDkEventName(ev.name);
        const detailUrl = `https://sportsbook.draftkings.com/event/${slug}/${ev.id}`
          + (subcategory ? `?category=odds&subcategory=${subcategory}` : '');
        try {
          const t0 = Date.now();
          await page.goto(detailUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });
          await new Promise(r => setTimeout(r, 3500));
          // Capture new markets that appeared since this nav
          const newMarkets = new Set();
          for (let i = before; i < allPayloads.length; i++) {
            for (const m of (allPayloads[i].data?.markets || [])) {
              if (m.marketType?.name) newMarkets.add(m.marketType.name);
            }
          }
          capture.eventDetailCaptures.push({
            eventId: ev.id,
            eventName: ev.name,
            detailUrl,
            navMs: Date.now() - t0,
            newMarketsFound: [...newMarkets],
          });
        } catch (err) {
          capture.errors.push(`detail nav ${ev.id}: ${err.message}`);
        }
      }
    }

    capture.elapsedMs = Date.now() - startedAt;
    // Sort market types by count desc for easier scanning
    capture.marketTypesSeen = Object.fromEntries(
      Object.entries(capture.marketTypesSeen).sort((a, b) => b[1] - a[1])
    );
    return capture;
  } finally {
    await browser.close();
  }
}

/**
 * Diagnostic-only flow that mirrors fetchMmaFightOdds but additionally
 * captures: every URL the page navigates through, every primaryMarkets
 * XHR seen (with sport hints from the URL path), the final landed URL,
 * the page title, and a sample of visible event-row labels. Goal is
 * to see WHY an empty harvest happened — DK redirected? page is empty?
 * XHR pollution from cross-sport featured panels?
 *
 * Bypasses cache and inFlight gates entirely. Always runs a fresh probe.
 * Doesn't write to cacheBySport.mma so it can't taint live data.
 */
async function debugMmaScraperState({ url = 'https://sportsbook.draftkings.com/leagues/mma/ufc' } = {}) {
  const browser = await puppeteer().launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });
  const xhrs = []; // every primaryMarkets XHR we observed
  const allXhrUrls = []; // every sportsbook-nash URL (broader, for cross-sport detection)
  const navTrail = [];
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1400, height: 900 });
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

    page.on('framenavigated', (frame) => {
      if (frame === page.mainFrame()) navTrail.push(frame.url());
    });

    page.on('response', async (resp) => {
      const u = resp.url();
      if (!u.includes('sportsbook-nash.draftkings.com')) return;
      // Tag broader nash traffic for sanity (cross-sport pollution detection)
      if (allXhrUrls.length < 200) allXhrUrls.push(u);
      if (!u.includes('primaryMarkets/v1/markets')) return;
      try {
        const ct = resp.headers()['content-type'] || '';
        if (!ct.includes('json')) return;
        const data = await resp.json();
        // Sport hint comes from event.eventCategory.sportName when present;
        // fall back to scanning the URL path for common league markers.
        const sportHints = new Set();
        for (const ev of (data.events || [])) {
          const sn = ev.eventCategory?.sportName || ev.competitionName || '';
          if (sn) sportHints.add(String(sn));
        }
        const lower = u.toLowerCase();
        for (const tag of ['mma', 'ufc', 'nba', 'nhl', 'mlb', 'nfl', 'soccer', 'tennis', 'golf']) {
          if (lower.includes(tag)) sportHints.add('url:' + tag);
        }
        xhrs.push({
          url: u,
          eventCount: (data.events || []).length,
          marketCount: (data.markets || []).length,
          sportHints: [...sportHints],
          sampleEventNames: (data.events || []).slice(0, 5).map(e => e.name),
        });
      } catch (err) { /* ignore */ }
    });

    let navError = null;
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    } catch (err) {
      navError = err.message;
    }
    await new Promise(r => setTimeout(r, 8000));
    // Brief scroll to mimic the production scraper, in case scrolling triggers
    // additional XHRs whose origin we want to inspect.
    try {
      for (let i = 0; i < 6; i++) {
        await page.evaluate(() => window.scrollBy(0, window.innerHeight));
        await new Promise(r => setTimeout(r, 600));
      }
      await page.evaluate(() => window.scrollTo(0, 0));
      await new Promise(r => setTimeout(r, 3000));
    } catch { /* ignore */ }

    const finalUrl = page.url();
    let title = null, eventRowSample = [], h1Text = null;
    try { title = await page.title(); } catch {}
    try {
      h1Text = await page.evaluate(() => (document.querySelector('h1')?.innerText || '').trim());
    } catch {}
    try {
      eventRowSample = await page.evaluate(() => {
        const rows = Array.from(document.querySelectorAll('a[href*="/event/"]'));
        return rows.slice(0, 12).map(a => ({ text: (a.innerText || '').trim().substring(0, 100), href: a.getAttribute('href') }));
      });
    } catch {}

    return {
      requestedUrl: url,
      finalUrl,
      navTrail,
      navError,
      title,
      h1Text,
      eventRowSample,
      xhrs,
      xhrCount: xhrs.length,
      crossSportPollution: xhrs.filter(x =>
        x.sportHints.some(h => /url:nba|url:nhl|url:mlb|url:nfl|url:soccer|url:tennis|url:golf/i.test(h))
      ).length,
      mmaXhrs: xhrs.filter(x =>
        x.sportHints.some(h => /url:mma|url:ufc/i.test(h))
        || x.sampleEventNames.some(n => /\bvs\.?\b|@/i.test(n) && !/76ers|celtics|lakers|warriors|nuggets|suns/i.test(n))
      ).length,
      sampleNashUrls: allXhrUrls.slice(0, 30),
    };
  } finally {
    await browser.close();
  }
}

module.exports = {
  fetchSeriesMarkets,
  fetchSeriesWinners,
  fetchNbaSeriesWinners,
  fetchNhlSeriesWinners,
  fetchNbaSeriesSpreads,
  fetchNhlSeriesSpreads,
  fetchNbaSeriesTotals,
  fetchNhlSeriesTotals,
  fetchMmaFightOdds,
  fetchMlbF5Odds,
  fetchDkPlayerProps,
  lookupDkPlayerPropFairProb,
  fetchDkGameLines,
  lookupDkGameLines,
  debugMmaScraperState,
  fetchGolfMatchups,
  fetchLiveMarkets,
  probeDkPage,
  parseSeriesData,
  parseSeriesWinnerData,
  parseSeriesSpreadData,
  parseSeriesTotalData,
  parseGolfMatchupData,
  lookupSeriesFairProb,
  lookupSeriesSpreadFairProb,
  lookupSeriesTotalFairProb,
  lookupMmaFairProb,
  lookupGolfMatchupFairProb,
  normalizeTeamName,
  normalizeGolfPairName,
};
