/**
 * Bovada alt-line scraper.
 *
 * Fetches sub-period (1H NBA, 1P/2P/3P NHL, F5/1I MLB) and team-total
 * markets from Bovada's public coupon API. These markets aren't served
 * by SharpAPI or The Odds API (confirmed 422 INVALID_MARKET on per-event
 * endpoint for alternate_spreads_h1 / alternate_totals_h1 /
 * alternate_team_totals), and DK's SPA required complex Puppeteer-based
 * tab clicking to extract. Bovada exposes everything via one JSON GET
 * per event from Railway — no browser, no WAF issues.
 *
 * Architecture:
 *   1. fetchEventsList(sport) — league endpoint returns events list
 *   2. fetchEventDetail(link) — per-event endpoint returns all 12+
 *      displayGroups with full market + outcome data
 *   3. parseAndStore(sport, events, details) — classify markets into
 *      our internal shape, compute fair probs via 2-way de-vig, cache
 *   4. Lookup functions for pricer integration (Phase 2 — not wired yet)
 *
 * Refresh cadence: intended for 2-min loop once Phase 2 integrates,
 * currently manual via /bovada-alt-refresh endpoint.
 *
 * NOT YET INTEGRATED INTO PRICING. Scraper runs in isolation for
 * verification. Pricer cascade hook is a Phase 2 deliverable.
 */
const log = require('./logger');

const CACHE_TTL_MS = 10 * 60 * 1000; // per-event TTL
const FETCH_TIMEOUT_MS = 15000;
const CONCURRENCY = 5;

// Sport → Bovada URL path. Keys are our internal sport keys so callers
// don't need to know Bovada's URL structure.
const SPORT_PATHS = {
  basketball_nba: 'basketball/nba',
  icehockey_nhl: 'hockey/nhl',
  baseball_mlb: 'baseball/mlb',
};

// Period description strings Bovada uses, normalized to canonical keys
// matching our internal marketType cache naming. Anything not in this
// map is treated as an unknown period and skipped (conservative — we
// don't want to surface markets we can't route correctly).
const PERIOD_MAP = {
  'Game':             'game',
  'First Half':       'h1',   // NBA 1H
  '1st Period':       'p1',   // NHL 1P
  '2nd Period':       'p2',
  '3rd Period':       'p3',
  'First 5 Innings':  'f5',   // MLB F5
  '1st Inning':       'i1',   // MLB 1I
};

// Market description → internal marketType classifier. Handles the
// handful of book-specific names per sport. Returns null for markets
// we don't support (score props, combo props, quarter-specific, etc.)
//
// Two-tier match:
//   1. Team-total pattern (regex) — "Total X - <team name>"
//      Returns 'team_total' + captured teamName
//   2. Exact-match lookup — e.g. "Moneyline" → 'h2h'
const TEAM_TOTAL_RE = /^(Total Points|Total Goals O\/U|Total Runs O\/U) - (.+)$/;
const MARKET_TYPE_MAP = {
  'Moneyline':        'h2h',
  'Point Spread':     'spread',         // NBA primary spread
  'Spread':           'spread',         // Bovada "alt" spread uses this name
  'Puck Line':        'spread',         // NHL
  'Runline':          'spread',         // MLB
  'Total':            'total',          // primary totals (all sports)
  'Total Points':     'total',          // NBA alt ladder
  'Total Goals O/U':  'total',          // NHL primary + alt
  'Total Runs O/U':   'total',          // MLB (rare)
};
function classifyMarket(description) {
  const ttMatch = TEAM_TOTAL_RE.exec(description || '');
  if (ttMatch) return { type: 'team_total', teamName: ttMatch[2].trim() };
  const t = MARKET_TYPE_MAP[description];
  return t ? { type: t } : null;
}

// In-memory cache keyed by normalizeEventKey(home, away). Each entry
// carries a periods map — periods contain market-family maps — markets
// contain a primary line + alt ladder.
//
// Example shape:
//   cache['new_york_knicks_atlanta_hawks'] = {
//     fetchedAt: 1714160000000,
//     source: 'bovada',
//     home: 'Atlanta Hawks',
//     away: 'New York Knicks',
//     startTime: '2026-04-23T23:00:00.000Z',
//     periods: {
//       game: {
//         h2h:    { home: {amer,fair}, away: {amer,fair}, vig },
//         spread: { primary: {line,home,away,vig}, alts: [{line,home,away,vig}] },
//         total:  { primary: {line,over,under,vig}, alts: [...] },
//         team_total: {
//           'Atlanta Hawks':   { primary: {line,over,under,vig}, alts: [...] },
//           'New York Knicks': { ... },
//         },
//       },
//       h1: { ... },
//       p1: { ... },
//     },
//   }
const cache = {};

// Diagnostic counters for /bovada-alt-status
const stats = {
  lastFullRefreshAt: null,
  lastFullRefreshMs: 0,
  bySport: {},   // sport -> { eventsTotal, eventsFetched, eventsFailed, marketsKept, marketsDropped }
  lastError: null,
};

// Normalize team+date into a stable cache key — same pattern as
// odds-feed.normalizeEventKey so downstream pricer lookups can reuse
// the identifier they already compute.
function normalizeEventKey(home, away) {
  const clean = (s) => String(s || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '');
  return [clean(home), clean(away)].sort().join('__');
}

// American odds → implied probability. Kept local rather than imported
// from odds-feed to keep this module self-contained.
function amerToProb(amer) {
  if (amer == null || amer === 'EVEN') return 0.5;
  const n = typeof amer === 'string' ? parseInt(amer, 10) : amer;
  if (!Number.isFinite(n) || n === 0) return null;
  if (n > 0) return 100 / (n + 100);
  return -n / (-n + 100);
}

// Two-way de-vig (proportional). Returns fair probs for each side.
function devigPair(a, b) {
  if (a == null || b == null) return { a: null, b: null, vig: null };
  const total = a + b;
  if (total <= 0) return { a: null, b: null, vig: null };
  return { a: a / total, b: b / total, vig: total - 1 };
}

// Safely read a handicap value; Bovada uses strings for these fields.
function toLine(handicap) {
  if (handicap == null) return null;
  const n = typeof handicap === 'string' ? parseFloat(handicap) : handicap;
  return Number.isFinite(n) ? n : null;
}

async function fetchWithTimeout(url, ms = FETCH_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    return await fetch(url, {
      signal: ctrl.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.bovada.lv/sports',
      },
    });
  } finally {
    clearTimeout(t);
  }
}

async function fetchEventsList(sport) {
  const path = SPORT_PATHS[sport];
  if (!path) throw new Error(`Unsupported sport: ${sport}`);
  const url = `https://www.bovada.lv/services/sports/event/coupon/events/A/description/${path}?marketFilterId=def&preMatchOnly=true&eventsLimit=50&lang=en`;
  const r = await fetchWithTimeout(url);
  if (!r.ok) throw new Error(`Events list HTTP ${r.status}`);
  const body = await r.json();
  const root = Array.isArray(body) && body[0] ? body[0] : null;
  const events = (root && root.events) || [];
  // Normalize to minimal shape we need downstream
  return events.map(e => ({
    id: e.id,
    description: e.description,
    link: e.link,
    startTime: e.startTime,
    competitionId: e.competitionId,
  })).filter(e => e.id && e.link);
}

async function fetchEventDetail(eventLink) {
  const url = `https://www.bovada.lv/services/sports/event/coupon/events/A/description${eventLink}?lang=en`;
  const r = await fetchWithTimeout(url);
  if (!r.ok) throw new Error(`Event detail HTTP ${r.status}`);
  const body = await r.json();
  const root = Array.isArray(body) && body[0] ? body[0] : null;
  const event = root && Array.isArray(root.events) && root.events[0] ? root.events[0] : null;
  return event;
}

// Determine home/away from the event's competitors array. Bovada uses
// `home` boolean on each competitor.
function extractTeams(event) {
  const comps = Array.isArray(event.competitors) ? event.competitors : [];
  const home = comps.find(c => c.home === true);
  const away = comps.find(c => c.home === false);
  return {
    homeTeam: home?.name || null,
    awayTeam: away?.name || null,
  };
}

// Bovada's outcome.description for spreads/team-totals carries text
// like "Boston Bruins - 1P" or "Over - 2P 2.0". We need to resolve
// which side (home/away/over/under) the outcome represents. For
// spread/h2h, match against known team names; for totals, match
// against /over|under/i.
function resolveSelection(marketType, outcomeDesc, homeTeam, awayTeam) {
  const d = (outcomeDesc || '').toLowerCase();
  if (marketType === 'h2h' || marketType === 'spread') {
    const h = (homeTeam || '').toLowerCase();
    const a = (awayTeam || '').toLowerCase();
    // Substring match — Bovada sometimes suffixes with period abbreviation
    // ("Boston Bruins - 1P"), so startsWith or includes is required.
    if (h && d.includes(h.split(' ').slice(-1)[0])) return 'home';
    if (a && d.includes(a.split(' ').slice(-1)[0])) return 'away';
    // Try full-name match
    if (h && d.includes(h)) return 'home';
    if (a && d.includes(a)) return 'away';
    return null;
  }
  if (marketType === 'total' || marketType === 'team_total') {
    if (/\bover\b/.test(d)) return 'over';
    if (/\bunder\b/.test(d)) return 'under';
    return null;
  }
  return null;
}

// Parse a Bovada event's displayGroups into our internal cache shape.
// Filters to Game Lines + Alternate Lines groups only (others are
// player props / combos we don't support).
function parseEventMarkets(event, sport) {
  const { homeTeam, awayTeam } = extractTeams(event);
  if (!homeTeam || !awayTeam) return null;

  const groups = Array.isArray(event.displayGroups) ? event.displayGroups : [];
  const targetGroups = new Set(['Game Lines', 'Alternate Lines']);

  // Accumulator: periodKey → marketTypeKey → array of { line, pairs }
  // For h2h/spread: pairs = [{ side: 'home', amer, line }, { side: 'away', amer, line }]
  // For total/team_total: pairs = [{ side: 'over', amer, line }, { side: 'under', amer, line }]
  // team_total additionally nests under teamName.
  const periods = {};
  let kept = 0, dropped = 0;

  for (const g of groups) {
    if (!targetGroups.has(g.description)) continue;
    for (const m of (g.markets || [])) {
      const periodStr = m.period?.description;
      const periodKey = PERIOD_MAP[periodStr];
      if (!periodKey) { dropped++; continue; }

      const cls = classifyMarket(m.description);
      if (!cls) { dropped++; continue; }

      const outcomes = Array.isArray(m.outcomes) ? m.outcomes : [];

      // Group outcomes by line value (for spread/total alt ladders,
      // multiple line values come as separate outcome pairs within the
      // same market). For h2h there's no line — use a single null key.
      const byLine = {};
      for (const o of outcomes) {
        const line = cls.type === 'h2h' ? null : toLine(o.price?.handicap);
        const amer = o.price?.american;
        const prob = amerToProb(amer);
        if (prob == null) continue;
        const side = resolveSelection(cls.type, o.description, homeTeam, awayTeam);
        if (!side) continue;
        const key = line == null ? 'ml' : String(line);
        if (!byLine[key]) byLine[key] = { line, sides: {} };
        byLine[key].sides[side] = { amer, prob };
      }

      // De-vig each line pair
      const entries = [];
      for (const { line, sides } of Object.values(byLine)) {
        let pair;
        if (cls.type === 'h2h' || cls.type === 'spread') {
          if (!sides.home || !sides.away) continue;
          const devig = devigPair(sides.home.prob, sides.away.prob);
          pair = {
            line,
            home: { amer: sides.home.amer, fair: devig.a },
            away: { amer: sides.away.amer, fair: devig.b },
            vig: devig.vig,
          };
        } else {
          if (!sides.over || !sides.under) continue;
          const devig = devigPair(sides.over.prob, sides.under.prob);
          pair = {
            line,
            over: { amer: sides.over.amer, fair: devig.a },
            under: { amer: sides.under.amer, fair: devig.b },
            vig: devig.vig,
          };
        }
        entries.push(pair);
      }
      if (entries.length === 0) { dropped++; continue; }
      kept++;

      // Place into periods[].marketType. For h2h: single entry. For
      // spread/total: choose the entry with the narrowest |vig| as
      // "primary"; others become alts. For team_total: similar, but
      // nested under teamName.
      if (!periods[periodKey]) periods[periodKey] = {};
      const bucket = periods[periodKey];

      if (cls.type === 'h2h') {
        bucket.h2h = entries[0];
      } else if (cls.type === 'spread' || cls.type === 'total') {
        entries.sort((a, b) => Math.abs(a.vig) - Math.abs(b.vig));
        const primary = entries[0];
        const alts = entries.slice(1);
        // Merge with existing if present — Game Lines group may
        // contribute the primary line, Alternate Lines its alts;
        // keep the tightest-vig as primary, append others to alts.
        const existing = bucket[cls.type];
        if (existing) {
          const all = [existing.primary, ...existing.alts, primary, ...alts];
          all.sort((a, b) => Math.abs(a.vig) - Math.abs(b.vig));
          bucket[cls.type] = { primary: all[0], alts: all.slice(1) };
        } else {
          bucket[cls.type] = { primary, alts };
        }
      } else if (cls.type === 'team_total') {
        if (!bucket.team_total) bucket.team_total = {};
        const tt = bucket.team_total;
        entries.sort((a, b) => Math.abs(a.vig) - Math.abs(b.vig));
        const primary = entries[0];
        const alts = entries.slice(1);
        tt[cls.teamName] = { primary, alts };
      }
    }
  }

  return {
    eventId: event.id,
    homeTeam,
    awayTeam,
    startTime: event.startTime ? new Date(event.startTime).toISOString() : null,
    periods,
    _kept: kept,
    _dropped: dropped,
  };
}

// Update cache for one sport. Fetches events list, then per-event
// detail in parallel with bounded concurrency. Returns stats.
async function refreshSport(sport) {
  const t0 = Date.now();
  const out = { sport, eventsTotal: 0, eventsFetched: 0, eventsFailed: 0, marketsKept: 0, marketsDropped: 0, errors: [] };
  let events;
  try {
    events = await fetchEventsList(sport);
  } catch (err) {
    out.errors.push(`eventsList: ${err.message}`);
    stats.bySport[sport] = { ...out, elapsedMs: Date.now() - t0 };
    stats.lastError = `refreshSport(${sport}): ${err.message}`;
    return out;
  }
  out.eventsTotal = events.length;

  let idx = 0;
  const now = Date.now();
  async function worker() {
    while (idx < events.length) {
      const ev = events[idx++];
      const key = normalizeEventKey('', '') && null; // placeholder — real key needs team names
      // Skip if fresh in cache
      const cachedEntry = Object.values(cache).find(c => c.eventId === ev.id);
      if (cachedEntry && (now - cachedEntry.fetchedAt) < CACHE_TTL_MS) continue;
      try {
        const detail = await fetchEventDetail(ev.link);
        if (!detail) { out.eventsFailed++; continue; }
        const parsed = parseEventMarkets(detail, sport);
        if (!parsed) { out.eventsFailed++; continue; }
        const cacheKey = normalizeEventKey(parsed.homeTeam, parsed.awayTeam);
        cache[cacheKey] = {
          fetchedAt: Date.now(),
          source: 'bovada',
          sport,
          ...parsed,
        };
        out.eventsFetched++;
        out.marketsKept += parsed._kept;
        out.marketsDropped += parsed._dropped;
      } catch (err) {
        out.eventsFailed++;
        if (out.errors.length < 5) out.errors.push(`${ev.description}: ${err.message}`);
      }
    }
  }
  const workers = [];
  for (let i = 0; i < Math.min(CONCURRENCY, events.length); i++) workers.push(worker());
  await Promise.all(workers);

  out.elapsedMs = Date.now() - t0;
  stats.bySport[sport] = out;
  log.info('BovadaAlt', `Refresh ${sport}: ${out.eventsFetched}/${out.eventsTotal} events, ${out.marketsKept} markets, ${out.marketsDropped} dropped, ${out.elapsedMs}ms`);
  return out;
}

async function refreshAll() {
  const t0 = Date.now();
  const sports = Object.keys(SPORT_PATHS);
  const results = await Promise.allSettled(sports.map(refreshSport));
  stats.lastFullRefreshAt = new Date().toISOString();
  stats.lastFullRefreshMs = Date.now() - t0;
  return results.map((r, i) => ({ sport: sports[i], ok: r.status === 'fulfilled', value: r.value, reason: r.reason?.message }));
}

/**
 * Lookup API — called by the pricer cascade in Phase 2. NOT YET WIRED.
 *
 * @param {string} sport        — 'basketball_nba', 'icehockey_nhl', 'baseball_mlb'
 * @param {string} homeTeam     — normalized or raw; resolveEventKey handles both
 * @param {string} awayTeam
 * @param {string} period       — 'game' | 'h1' | 'p1' | 'p2' | 'p3' | 'f5' | 'i1'
 * @param {string} marketType   — 'h2h' | 'spread' | 'total' | 'team_total'
 * @param {string} selection    — 'home' | 'away' | 'over' | 'under'
 * @param {number|null} line    — null for h2h, numeric for others
 * @param {string|null} teamName — required for team_total only (exact team name as Bovada reports)
 * @returns {number|null} fair probability, or null if missing/stale
 */
function lookupFairProb({ sport, homeTeam, awayTeam, period, marketType, selection, line = null, teamName = null }) {
  const key = normalizeEventKey(homeTeam, awayTeam);
  const entry = cache[key];
  if (!entry) return null;
  if ((Date.now() - entry.fetchedAt) > CACHE_TTL_MS) return null;
  const periodBucket = entry.periods?.[period];
  if (!periodBucket) return null;

  if (marketType === 'h2h') {
    const pair = periodBucket.h2h;
    if (!pair) return null;
    return selection === 'home' ? pair.home?.fair : pair.away?.fair;
  }

  if (marketType === 'spread' || marketType === 'total') {
    const mkt = periodBucket[marketType];
    if (!mkt) return null;
    const all = [mkt.primary, ...(mkt.alts || [])];
    const match = all.find(p => Math.abs((p.line || 0) - (line || 0)) < 0.01);
    if (!match) return null;
    if (marketType === 'spread') {
      return selection === 'home' ? match.home?.fair : match.away?.fair;
    }
    return selection === 'over' ? match.over?.fair : match.under?.fair;
  }

  if (marketType === 'team_total') {
    if (!teamName) return null;
    const tt = periodBucket.team_total;
    if (!tt || !tt[teamName]) return null;
    const mkt = tt[teamName];
    const all = [mkt.primary, ...(mkt.alts || [])];
    const match = all.find(p => Math.abs((p.line || 0) - (line || 0)) < 0.01);
    if (!match) return null;
    return selection === 'over' ? match.over?.fair : match.under?.fair;
  }

  return null;
}

function getStatus() {
  return {
    ...stats,
    cacheSize: Object.keys(cache).length,
    cachedEvents: Object.entries(cache).slice(0, 10).map(([k, v]) => ({
      key: k,
      home: v.homeTeam,
      away: v.awayTeam,
      sport: v.sport,
      fetchedAt: new Date(v.fetchedAt).toISOString(),
      ageMin: Math.round((Date.now() - v.fetchedAt) / 60000),
      periods: Object.keys(v.periods || {}),
    })),
  };
}

function getCachedEvent(sport, homeTeam, awayTeam) {
  const key = normalizeEventKey(homeTeam, awayTeam);
  return cache[key] || null;
}

module.exports = {
  refreshSport,
  refreshAll,
  lookupFairProb,
  getStatus,
  getCachedEvent,
  normalizeEventKey,
  // Exposed for tests / future tooling; not called from pricing path
  _internal: { parseEventMarkets, classifyMarket, devigPair, amerToProb, PERIOD_MAP, MARKET_TYPE_MAP },
};
