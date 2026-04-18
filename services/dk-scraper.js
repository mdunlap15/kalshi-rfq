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
  const candLast = candidate.split(' ').pop();
  return !!(candLast && candLast === targetLast);
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
  parseSeriesData,
  parseSeriesWinnerData,
  parseSeriesSpreadData,
  parseSeriesTotalData,
  lookupSeriesFairProb,
  lookupSeriesSpreadFairProb,
  lookupSeriesTotalFairProb,
  lookupMmaFairProb,
  normalizeTeamName,
};
