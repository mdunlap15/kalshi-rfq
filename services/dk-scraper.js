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
// We navigate through three subcategories per sport (winner / spread /
// total-games) in a single browser session and partition captured XHR
// payloads by marketType.name — robust against DK rotating their
// internal subcategoryIds in the request URL.
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

// Subcategory slug → DK marketType.name present in the returned payload.
// We filter captured XHRs by marketType.name (not URL) because DK's new
// template-vars URL format no longer surfaces the subcategoryId.
const SUBCATEGORIES = [
  { slug: 'winner', marketName: 'Series Winner' },
  { slug: 'spread', marketName: 'Series Spread' },
  { slug: 'total-games', marketName: 'Series Total Games' },
];

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

      // Captured XHR payloads grouped by marketType.name. DK fires
      // multiple content XHRs per page load; we retain any payload that
      // carries events/markets/selections AND contains at least one of
      // our target market names. The same marketType may appear across
      // multiple captures (e.g. DK re-sends on subscription resume);
      // we keep them all and the parser dedupes by eventId/marketId.
      const payloadsByMarketName = {};
      for (const sc of SUBCATEGORIES) payloadsByMarketName[sc.marketName] = [];

      page.on('response', async (resp) => {
        const url = resp.url();
        if (!url.includes('sportsbook-nash.draftkings.com')) return;
        try {
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          if (!data || !data.selections || !data.events || !data.markets) return;
          const names = new Set((data.markets || []).map(m => m.marketType?.name).filter(Boolean));
          for (const sc of SUBCATEGORIES) {
            if (names.has(sc.marketName)) payloadsByMarketName[sc.marketName].push(data);
          }
        } catch { /* ignore */ }
      });

      // Navigate the 3 subcategories sequentially in ONE browser session.
      // First navigation pays Akamai cold-start; subsequent ones reuse
      // the already-challenged session and typically resolve in <3s.
      for (const sc of SUBCATEGORIES) {
        const url = `${cfg.baseUrl}?category=series-props&subcategory=${sc.slug}`;
        try {
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
          await new Promise(r => setTimeout(r, POST_NAV_WAIT_MS));
        } catch (err) {
          log.warn('DkScraper', `${sport} ${sc.slug} navigation failed: ${err.message}`);
        }
      }

      const winners = parseSeriesWinnerData(payloadsByMarketName['Series Winner']);
      const spreads = parseSeriesSpreadData(payloadsByMarketName['Series Spread']);
      const totals = parseSeriesTotalData(payloadsByMarketName['Series Total Games']);

      const payload = {
        fetchedAt: new Date().toISOString(),
        sport,
        series: winners,   // back-compat: /nba-series-prices reads .series
        winners,
        spreads,
        totals,
      };
      cacheBySport[sport] = { at: Date.now(), data: payload };
      log.info('DkScraper', `${sport.toUpperCase()} series: ${winners.length} winners, ${spreads.length} spreads, ${totals.length} totals (${Date.now() - startedAt}ms)`);

      if (winners.length === 0 && spreads.length === 0 && totals.length === 0) {
        throw new Error(`DK scraper: no ${sport.toUpperCase()} series payloads captured across winner/spread/total-games`);
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
      // Prefix match — handles PX's "Robert Valentin" vs DK's full
      // three-part "Robert Valentin Frey" (and symmetrically if DK
      // dropped a middle name). Requires a space boundary so "John"
      // doesn't match unrelated "Johnson".
      if (cand.startsWith(target + ' ') || target.startsWith(cand + ' ')) return base;
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
 * payloads, filtered by marketType.name. Returns:
 *   { eventsById, marketsById, selectionsByMarketId }
 * Markets and events are deduped by id across payloads; selections are
 * grouped by marketId and deduped by selection.id.
 */
function indexPayloads(payloads, marketTypeName) {
  const eventsById = {};
  const marketsById = {};
  const selectionsByMarketId = {};
  const seenSelIds = new Set();
  for (const payload of (payloads || [])) {
    for (const e of (payload.events || [])) if (!eventsById[e.id]) eventsById[e.id] = e;
    for (const m of (payload.markets || [])) {
      if (m.marketType?.name !== marketTypeName) continue;
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
  const { eventsById, marketsById, selectionsByMarketId } = indexPayloads(payloads, 'Series Winner');
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
  const { eventsById, marketsById, selectionsByMarketId } = indexPayloads(payloads, 'Series Spread');
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
  const { eventsById, marketsById, selectionsByMarketId } = indexPayloads(payloads, 'Series Total Games');
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
async function probeDkPage({ url, subcategory = null, postWaitMs = 10000, eventDetailNav = false, maxEventDetails = 3 }) {
  const startedAt = Date.now();
  const browser = await puppeteer().launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });
  const capture = {
    navigatedUrl: null,
    elapsedMs: 0,
    xhrCount: 0,
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
      if (!rurl.includes('sportsbook-nash.draftkings.com')) return;
      try {
        const ct = resp.headers()['content-type'] || '';
        if (!ct.includes('json')) return;
        const data = await resp.json();
        if (!data || typeof data !== 'object') return;
        allPayloads.push({ url: rurl, data });
        capture.xhrCount++;
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
