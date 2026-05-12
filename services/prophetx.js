// Uses Node's global fetch (undici under the hood). Keep-alive, pool size,
// and TCP_NODELAY are configured by services/httpClient — see that module.
// Migrated from node-fetch@2 + custom http.Agent for S3 of latency plan.
const { config } = require('../config');
const log = require('./logger');

// Token cache
let tokenCache = { token: null, refreshToken: null, time: 0 };
// Cooldown: after a failed login (especially session_num_exceed), don't retry
// for this many ms. Prevents periodic timers from burning through all 20 sessions.
let loginCooldownUntil = 0;

// ---------------------------------------------------------------------------
// AUTH
// ---------------------------------------------------------------------------

async function login() {
  const age = (Date.now() - tokenCache.time) / 1000 / 60;
  if (tokenCache.token && age < config.px.tokenTtlMinutes) {
    return tokenCache.token;
  }

  // Try refresh first (doesn't create a new session — never blocked by cooldown)
  if (tokenCache.refreshToken) {
    try {
      const refreshed = await refreshSession();
      if (refreshed) return refreshed;
    } catch (err) {
      log.warn('PX-Auth', `Refresh failed: ${err.message}, falling back to login`);
    }
  }

  // If we have a stale token, return it anyway — let PX reject with 401
  // and the caller can retry. Better than throwing and blocking all offers.
  if (tokenCache.token) {
    log.debug('PX-Auth', 'Token expired but returning stale token to avoid blocking');
    return tokenCache.token;
  }

  // Cooldown: if a recent login failed, don't spam PX with more attempts
  if (Date.now() < loginCooldownUntil) {
    const waitSec = Math.round((loginCooldownUntil - Date.now()) / 1000);
    throw new Error(`Login on cooldown (${waitSec}s remaining) — avoiding session_num_exceed`);
  }

  log.info('PX-Auth', 'Logging in to ProphetX...');
  const resp = await fetch(`${config.px.baseUrl}/partner/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
    body: JSON.stringify({
      access_key: config.px.accessKey,
      secret_key: config.px.secretKey,
    }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    // If session limit hit, set a 10-minute cooldown (wait for sessions to expire)
    if (text.includes('session_num_exceed')) {
      loginCooldownUntil = Date.now() + 10 * 60 * 1000;
      log.error('PX-Auth', 'Session limit hit — 10min cooldown before next login attempt');
    }
    throw new Error(`ProphetX login failed (${resp.status}): ${text}`);
  }

  // Success — clear any cooldown
  loginCooldownUntil = 0;

  const data = await resp.json();
  const token = data.access_token || data.data?.access_token;
  if (!token) throw new Error('ProphetX login: no access_token in response');

  const refreshToken = data.refresh_token || data.data?.refresh_token || null;
  tokenCache = { token, refreshToken, time: Date.now() };
  log.info('PX-Auth', `Login successful, token cached${refreshToken ? ' (with refresh token)' : ''}`);
  return token;
}

/**
 * Refresh the access token using the stored refresh token.
 * This does NOT create a new session, avoiding the session limit.
 */
async function refreshSession() {
  if (!tokenCache.refreshToken) return null;

  log.debug('PX-Auth', 'Refreshing session...');
  const resp = await fetch(`${config.px.baseUrl}/partner/auth/refresh`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
    body: JSON.stringify({ refresh_token: tokenCache.refreshToken }),
  });

  if (!resp.ok) {
    tokenCache.refreshToken = null; // clear stale refresh token
    return null;
  }

  const data = await resp.json();
  const token = data.access_token || data.data?.access_token;
  if (!token) return null;

  const refreshToken = data.refresh_token || data.data?.refresh_token || tokenCache.refreshToken;
  tokenCache = { token, refreshToken, time: Date.now() };
  log.info('PX-Auth', 'Session refreshed (no new session created)');
  return token;
}

function invalidateToken() {
  tokenCache = { token: null, refreshToken: tokenCache.refreshToken, time: 0 };
}

// ---------------------------------------------------------------------------
// GENERIC REQUEST WRAPPER
// ---------------------------------------------------------------------------

async function pxFetch(endpoint, method = 'GET', body = null, useBaseUrl = true) {
  const token = await login();
  const url = useBaseUrl ? `${config.px.baseUrl}${endpoint}` : endpoint;

  const options = {
    method,
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    },
  };
  if (body && method !== 'GET') {
    options.body = JSON.stringify(body);
  }

  const resp = await fetch(url, options);

  if (resp.status === 401) {
    // Token expired — try to get a fresh one and retry ONCE.
    // Use refresh token directly (no new session). If refresh fails,
    // try a fresh login but suppress cooldown on failure.
    log.warn('PX-Auth', `401 on ${method} ${endpoint} — re-authenticating`);
    invalidateToken();
    clearCooldown();
    let newToken = null;
    // Try refresh first (no session cost)
    if (tokenCache.refreshToken) {
      try { newToken = await refreshSession(); } catch (e) {}
    }
    // Fall back to login, but suppress cooldown if it fails
    if (!newToken) {
      const savedCooldown = loginCooldownUntil;
      try { newToken = await login(); } catch (e) {
        loginCooldownUntil = savedCooldown; // restore, don't set new cooldown
        throw new Error(`ProphetX API 401 on ${method} ${endpoint} — re-auth failed: ${e.message}`);
      }
    }
    options.headers['Authorization'] = `Bearer ${newToken}`;
    const retryResp = await fetch(url, options);
    if (!retryResp.ok) {
      const text = await retryResp.text();
      throw new Error(`ProphetX API ${retryResp.status} on ${method} ${endpoint} (retry): ${text}`);
    }
    return retryResp.json();
  }
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`ProphetX API ${resp.status} on ${method} ${endpoint}: ${text}`);
  }

  return resp.json();
}

// ---------------------------------------------------------------------------
// SPORT EVENTS & MARKETS
// ---------------------------------------------------------------------------

async function fetchSportEvents() {
  const data = await pxFetch('/partner/mm/get_sport_events');
  return data.data?.sport_events || [];
}

// ---------------------------------------------------------------------------
// fetchMarkets cache + in-flight coalescing
// ---------------------------------------------------------------------------
// Two callers exercise this endpoint heavily:
//   1. seedAllLines — fetches markets for every supported PX event at seed time
//      (one call per event, unique event_id → no cache contention)
//   2. resolveUnknownLine — fetches markets for an event the first time an
//      unknown line_id arrives from that event. The hot path.
//
// Latency instrumentation showed receive_to_resolve p95 ≈ 160ms, traced to
// the PX get_markets round-trip inside resolveUnknownLine. Two classes of
// wasted work:
//   a. Multiple concurrent RFQs for the same fresh event each fire their
//      own fetchMarkets call (line-manager's inFlightResolutions map only
//      dedupes per-lineId, not per-eventId).
//   b. Rapid back-to-back RFQs across different lineIds in the same
//      event re-fetch identical market data.
//
// 30s TTL + in-flight promise map collapses both. Markets metadata (market
// types, lines, selection ids) is structural and changes slowly — the
// odds inside it aren't read here, so a 30s stale window is safe. Seed-
// time calls still hit the network because seed iterates unique events
// sequentially with 100ms spacing between them — TTL won't come into
// play unless the same event is re-seeded inside 30s (refreshLines is
// on a 2-min cadence, so no).
const _marketsCache = {};          // eventId -> { markets, fetchedAt }
const _marketsInFlight = {};       // eventId -> Promise<markets[]>
const MARKETS_CACHE_TTL_MS = 30 * 1000;
const _marketsCacheStats = { hits: 0, coalesced: 0, fetched: 0, errors: 0 };

async function fetchMarkets(eventId, opts = {}) {
  const bypass = opts.bypass === true;
  const now = Date.now();

  // Fast path: fresh cache entry.
  if (!bypass) {
    const cached = _marketsCache[eventId];
    if (cached && (now - cached.fetchedAt) < MARKETS_CACHE_TTL_MS) {
      _marketsCacheStats.hits++;
      return cached.markets;
    }
    // Coalesce: another caller is already fetching this eventId.
    const pending = _marketsInFlight[eventId];
    if (pending) {
      _marketsCacheStats.coalesced++;
      return pending;
    }
  }

  const promise = (async () => {
    try {
      const data = await pxFetch(`/partner/mm/get_markets?event_id=${eventId}`);
      const markets = data.data?.markets || [];
      _marketsCache[eventId] = { markets, fetchedAt: Date.now() };
      _marketsCacheStats.fetched++;
      return markets;
    } catch (err) {
      _marketsCacheStats.errors++;
      throw err;
    } finally {
      delete _marketsInFlight[eventId];
    }
  })();
  _marketsInFlight[eventId] = promise;
  return promise;
}

function getMarketsCacheStats() {
  const total = _marketsCacheStats.hits + _marketsCacheStats.coalesced + _marketsCacheStats.fetched;
  return {
    ..._marketsCacheStats,
    ttlMs: MARKETS_CACHE_TTL_MS,
    cacheSize: Object.keys(_marketsCache).length,
    inFlight: Object.keys(_marketsInFlight).length,
    hitRate: total > 0 ? (_marketsCacheStats.hits + _marketsCacheStats.coalesced) / total : null,
  };
}

// ---------------------------------------------------------------------------
// AFFILIATE API — richer reference endpoints that return team/market names
//
// PX exposes a separate /partner/affiliate/* namespace with three read-only
// endpoints that the /partner/mm/* ones don't match feature-for-feature:
//
//   • get_sport_events?event_ids[]&tournament_id= — bulk event lookup with
//     competitors[] (home/away team names + abbreviations), scheduled start
//     time, tournament_name, sport_name
//   • get_multiple_markets?event_ids[]&get_all_market=true — bulk markets
//     keyed by event_id, same schema as single-event get_markets
//   • get_tournaments — tournament_id → sport/league dictionary
//
// These replace the per-event enrichment loop entirely: 217 serial fetches
// become two bulk calls. They also expose home/away team names (via
// competitors[].side) which the MM namespace does not.
// ---------------------------------------------------------------------------

/**
 * Bulk fetch sport event metadata. Accepts { eventIds?, tournamentId? }.
 * Returns an array of sport-event objects with competitors, scheduled,
 * tournament_name, sport_name, etc.
 *
 * PX appears to accept either `event_ids=1,2,3` (comma-separated) or
 * repeated `event_ids=1&event_ids=2` syntax. We use comma-separated since
 * it's shorter; fall back handled in caller if that doesn't parse.
 */
async function fetchAffiliateSportEvents({ eventIds = null, tournamentId = null } = {}) {
  // NOTE: Our API key has /partner/mm/* access only — /partner/affiliate/*
  // returns 401. The mm namespace exposes the same reference endpoints but
  // only for CURRENT (live/upcoming) events; historical events return 404.
  const params = [];
  if (eventIds && eventIds.length > 0) {
    params.push(`event_ids=${eventIds.join(',')}`);
  }
  if (tournamentId) params.push(`tournament_id=${tournamentId}`);
  const qs = params.length ? `?${params.join('&')}` : '';
  const data = await pxFetch(`/partner/mm/get_sport_events${qs}`);
  return data.data?.sport_events || data.sport_events || [];
}

/**
 * Bulk fetch markets for many events at once. Returns an object keyed by
 * eventId → markets array. Pass `getAllMarket: false` to trim the response
 * to primary markets only (moneyline/spread/total/team_total).
 */
async function fetchAffiliateMultipleMarkets(eventIds, opts = {}) {
  if (!eventIds || eventIds.length === 0) return {};
  const params = [`event_ids=${eventIds.join(',')}`];
  if (opts.getAllMarket !== false) params.push('get_all_market=true');
  const data = await pxFetch(`/partner/mm/get_multiple_markets?${params.join('&')}`);
  // Shape: { data: { <event_id>: [markets] } } OR { <event_id>: [markets] }
  return data.data || data;
}

/**
 * One-shot tournament dictionary. Returns an array of tournaments, each with
 * { id, name, sport: { id, name } }. Small response — cache it on startup
 * and look up tournament_id → sport_name across the whole session.
 */
async function fetchAffiliateTournaments() {
  const data = await pxFetch('/partner/mm/get_tournaments');
  return data.data?.tournaments || data.tournaments || [];
}

// ---------------------------------------------------------------------------
// SUPPORTED LINES
// ---------------------------------------------------------------------------

// PX's /parlay/sp/supported-lines accepts at most 1000 line_ids per call
// (400 invalid_params otherwise). As our line index has grown past 1000
// we'd silently fail to register any lines — forcing every first-RFQ for
// a line to pay a round-trip via on-demand registration. Chunk both
// POST and DELETE to stay under the cap.
const PX_SUPPORTED_LINES_CHUNK_SIZE = 1000;

async function registerSupportedLines(lineIds) {
  if (!lineIds || lineIds.length === 0) return { success: true, count: 0 };
  if (lineIds.length <= PX_SUPPORTED_LINES_CHUNK_SIZE) {
    log.info('PX-Lines', `Registering ${lineIds.length} supported lines`);
    return pxFetch('/parlay/sp/supported-lines', 'POST', { supported_lines: lineIds });
  }
  const chunks = [];
  for (let i = 0; i < lineIds.length; i += PX_SUPPORTED_LINES_CHUNK_SIZE) {
    chunks.push(lineIds.slice(i, i + PX_SUPPORTED_LINES_CHUNK_SIZE));
  }
  log.info('PX-Lines', `Registering ${lineIds.length} supported lines in ${chunks.length} chunks (cap ${PX_SUPPORTED_LINES_CHUNK_SIZE})`);
  for (let ci = 0; ci < chunks.length; ci++) {
    const chunk = chunks[ci];
    await pxFetch('/parlay/sp/supported-lines', 'POST', { supported_lines: chunk });
    log.debug('PX-Lines', `  chunk ${ci + 1}/${chunks.length} registered (${chunk.length} lines)`);
  }
  return { success: true, count: lineIds.length, chunks: chunks.length };
}

async function removeSupportedLines(lineIds) {
  if (!lineIds || lineIds.length === 0) return { success: true };
  if (lineIds.length <= PX_SUPPORTED_LINES_CHUNK_SIZE) {
    log.info('PX-Lines', `Removing ${lineIds.length} supported lines`);
    return pxFetch('/parlay/sp/supported-lines', 'DELETE', { supported_lines: lineIds });
  }
  const chunks = [];
  for (let i = 0; i < lineIds.length; i += PX_SUPPORTED_LINES_CHUNK_SIZE) {
    chunks.push(lineIds.slice(i, i + PX_SUPPORTED_LINES_CHUNK_SIZE));
  }
  log.info('PX-Lines', `Removing ${lineIds.length} supported lines in ${chunks.length} chunks`);
  for (const chunk of chunks) {
    await pxFetch('/parlay/sp/supported-lines', 'DELETE', { supported_lines: chunk });
  }
  return { success: true, count: lineIds.length, chunks: chunks.length };
}

async function getSupportedLines(limit = 100) {
  const data = await pxFetch(`/parlay/sp/supported-lines?limit=${limit}`);
  return data.data?.supported_lines || [];
}

// ---------------------------------------------------------------------------
// WEBSOCKET
// ---------------------------------------------------------------------------

async function getWebSocketConfig() {
  const data = await pxFetch('/parlay/sp/websocket/connection-config');
  return data; // { key, cluster, app_id }
}

async function registerWebSocket(socketId) {
  const data = await pxFetch('/parlay/sp/websocket/register', 'POST', {
    socket_id: socketId,
  });
  return data; // { channels, events }
}

// ---------------------------------------------------------------------------
// OFFERS & CONFIRMATIONS
// ---------------------------------------------------------------------------

// Diagnostic ring buffer for the precision-mode experiment. Each entry
// captures the full submitOffer payload + PX response so we can correlate
// what we sent (decimal vs integer odds) with how PX responded and whether
// that auction later confirmed. Active only when PX_PRECISION_DIAG=true.
// Bounded to 200 entries — operator inspects via GET /admin/precision-diag.
const _precisionDiagBuffer = [];
const _PRECISION_DIAG_MAX = 200;
function _recordPrecisionDiagEntry(entry) {
  _precisionDiagBuffer.unshift({ ...entry, recordedAt: new Date().toISOString() });
  if (_precisionDiagBuffer.length > _PRECISION_DIAG_MAX) _precisionDiagBuffer.pop();
}
function getPrecisionDiagSnapshot() {
  return {
    enabled: process.env.PX_PRECISION_DIAG === 'true' || process.env.PX_PRECISION_DIAG === '1',
    pxOddsPrecision: parseInt(process.env.PX_ODDS_PRECISION || '0', 10),
    bufferSize: _precisionDiagBuffer.length,
    entries: _precisionDiagBuffer,
  };
}

async function submitOffer(callbackUrl, parlayId, offers) {
  // No pre-submit log on the hot path — JSON.stringify(offers) + log.info
  // adds 1-3ms (stdout back-pressure dependent), and the "Offered" log in
  // websocket.js already records this submission. The response log below
  // is off the critical path — the caller fires this promise and doesn't
  // await it.
  //
  // Precision-mode diagnostic: when PX_PRECISION_DIAG=true, capture the
  // full payload + PX response into a ring buffer so we can correlate
  // wire format with PX behavior (silent-reject vs truncate-tie vs full-
  // precision use). Off by default so the hot path stays clean.
  const diagOn = process.env.PX_PRECISION_DIAG === 'true' || process.env.PX_PRECISION_DIAG === '1';
  const submittedAt = diagOn ? Date.now() : null;
  return pxFetch(callbackUrl, 'POST', {
    parlay_id: parlayId,
    offers,
  }, false).then(data => {
    log.info('PX-Offer', `Response for ${parlayId}: ${JSON.stringify(data).substring(0, 300)}`);
    if (diagOn) {
      _recordPrecisionDiagEntry({
        parlayId,
        ok: true,
        pxStatus: data?.status || null,
        pxResponse: data,
        offers,
        submittedAt,
        elapsedMs: Date.now() - submittedAt,
      });
    }
    return data;
  }).catch(err => {
    log.error('PX-Offer', `Failed to submit offer for ${parlayId}: ${err.message}`);
    if (diagOn) {
      _recordPrecisionDiagEntry({
        parlayId,
        ok: false,
        error: err.message,
        errorStatus: err.status || null,
        errorBody: err.body || null,
        offers,
        submittedAt,
        elapsedMs: Date.now() - submittedAt,
      });
    }
    throw err;
  });
}

async function confirmOrder(callbackUrl, orderUuid, action, confirmedOdds, confirmedStake, priceProbability) {
  log.info('PX-Confirm', `${action} order ${orderUuid}`);
  const body = {
    order_uuid: orderUuid,
    action,
  };
  if (confirmedOdds != null) body.confirmed_odds = confirmedOdds;
  if (confirmedStake != null) body.confirmed_stake = confirmedStake;
  if (priceProbability) body.price_probability = priceProbability;

  const data = await pxFetch(callbackUrl, 'POST', body, false);
  return data;
}

// ---------------------------------------------------------------------------
// ORDERS
// ---------------------------------------------------------------------------

async function fetchBalance() {
  const data = await pxFetch('/partner/mm/get_balance');
  return data.data || data;
}

/**
 * Fetch orders from PX. PX caps single pages at 100 orders and returns a
 * base64 `token` for the next page. When limit > 100, we paginate using
 * that token until we reach the limit or exhaust all orders.
 */
/**
 * Fetch a single order by order_uuid from PX REST. Returns the order object
 * with full leg-level settlement data, or null if not found.
 *
 * Used by the WebSocket parlay.settled handler to backfill leg status data
 * before persisting a settlement — without this, orders settled via WebSocket
 * have no leg_status fields, which triggers the loadFromDb revert heuristic
 * on restart and silently destroys settlement records.
 *
 * Implementation: scans the most recent 50 orders for the matching UUID.
 * The just-settled order is nearly always at the top of that window.
 */
async function fetchOrderByUuid(uuid) {
  if (!uuid) return null;
  try {
    const orders = await fetchOrders(50);
    return orders.find(o => o.order_uuid === uuid) || null;
  } catch (err) {
    log.warn('PX-Orders', `fetchOrderByUuid(${uuid}) failed: ${err.message}`);
    return null;
  }
}

async function fetchOrders(limit = 50, status = null) {
  const PAGE_SIZE = 100;
  const all = [];
  let token = null;
  while (all.length < limit) {
    const pageSize = Math.min(PAGE_SIZE, limit - all.length);
    let url = `/parlay/sp/orders/?limit=${pageSize}`;
    if (status) url += `&status=${status}`;
    if (token) url += `&token=${encodeURIComponent(token)}`;
    let data;
    try {
      data = await pxFetch(url);
    } catch (err) {
      log.warn('PX-Orders', `Pagination stopped (offset ${all.length}): ${err.message}`);
      break;
    }
    const orders = data.data?.orders || [];
    if (orders.length === 0) break;
    all.push(...orders);
    token = data.data?.token || null;
    if (!token || orders.length < pageSize) break; // no more pages
  }
  return all;
}

// ---------------------------------------------------------------------------
// MARKET PARSING HELPERS
// ---------------------------------------------------------------------------

/**
 * Parse a ProphetX market response into a flat list of line entries.
 * Handles the nested selections structure for moneyline, spread, and total markets.
 *
 * Returns: [{ lineId, marketType, selection, teamName, line, competitorId, outcomeName }]
 */
function parseMarketSelections(market) {
  const results = [];
  let marketType = market.type; // 'moneyline', 'spread', 'total'

  // Strip trailing odds from team names (e.g., "Kansas City Royals -103" → "Kansas City Royals")
  function cleanSelectionName(name) {
    if (!name) return '';
    // Remove trailing odds pattern: space + optional sign + digits (e.g., " -103", " +275", " 150")
    return name.replace(/\s+[+-]?\d+(\.\d+)?$/, '').trim();
  }

  // PX uses the same market.type ('moneyline', 'spread', 'total') for both
  // full-game and sub-period markets (First 5 Innings for MLB, First Half
  // for NBA), distinguishing them only by market.name. Detect by name and
  // override the marketType so downstream code (line-manager, pricer) routes
  // to the correct cache entry (h2h_f5, h2h_h1, etc).
  //
  // IMPORTANT: the previous F5 regex included `1st\s*half` as an alternate
  // which incorrectly classified NBA 1st-Half markets as MLB F5. H1 and F5
  // are now detected separately with non-overlapping patterns.
  const marketName = market.name || '';
  const isF5ByName = /1st[-\s]?5th.*inning|first\s*5\s*inning|first\s*five\s*innings|\bf5\b/i.test(marketName);
  if (isF5ByName) {
    if (marketType === 'moneyline') marketType = 'first_5_innings_moneyline';
    else if (marketType === 'spread') marketType = 'first_5_innings_run_line';
    else if (marketType === 'total') marketType = 'first_5_innings_total';
  }
  // First-Half (NBA primarily; may apply to other sports if PX posts them).
  // Must run AFTER the F5 check and skip if F5 already matched, so a
  // hypothetical "First Half of 1st 5 Innings" wouldn't double-classify.
  const isH1ByName = !isF5ByName && /first\s*half|1st\s*half/i.test(marketName);
  if (isH1ByName) {
    if (marketType === 'moneyline') marketType = 'first_half_moneyline';
    else if (marketType === 'spread') marketType = 'first_half_spread';
    else if (marketType === 'total') marketType = 'first_half_total';
  }

  // Team totals are also typed as 'total' by PX. The only way to distinguish
  // from full-game totals is the market NAME (e.g. "SJ: Team Total Goals",
  // "Philadelphia Phillies Team Total Runs", "Home Team Total"). Without this
  // override, the line-manager applies full-game total bounds ([4, 9] for NHL)
  // which reject all team totals as sub-game — silently losing hundreds of
  // RFQs per day to mislabeled 'alt_spread' declines. Detect by name and
  // upgrade the marketType so downstream routing uses team_total semantics.
  const isTeamTotalByName = /\bteam\s*total\b|^(home|away|[A-Z]{2,4}):\s*team/i.test(marketName);
  if (!isF5ByName && isTeamTotalByName && marketType === 'total') {
    marketType = 'team_total';
  }

  // F5 moneyline uses same structure as full-game moneyline (selections array)
  const isF5Moneyline = /first_5_innings_moneyline|first_five_innings_moneyline/.test(marketType);
  // F5 spread/total uses same structure as full-game spread/total (market_lines)
  const isF5Spread = /first_5_innings_run_line|first_five_innings_run_line/.test(marketType);
  const isF5Total = /first_5_innings_total|first_five_innings_total/.test(marketType);
  // H1 (First Half) markets use the same structures as full-game — moneyline
  // uses .selections, spread/total uses .market_lines. We need these booleans
  // separate from the string equality checks below (marketType is now
  // 'first_half_moneyline'/'first_half_spread'/'first_half_total' after the
  // name-detection override above).
  const isH1Moneyline = /first_half_moneyline|1st_half_moneyline/.test(marketType);
  const isH1Spread = /first_half_spread|1st_half_spread/.test(marketType);
  const isH1Total = /first_half_total|1st_half_total/.test(marketType);

  // PX uses `type: 'sup_moneyline'` for Series Game Spread + Series Total
  // Games (live probe 2026-04-18). Selections are structured like moneyline
  // (flat selections array) but the 'line' field is zero — the actual
  // line + side are encoded in the selection name (e.g. "MIN +1.5",
  // "Over 5.5"). Detect by market name and retag marketType so the seed's
  // series handling picks it up.
  const isSupSeriesSpread = market.type === 'sup_moneyline'
    && (/\bseries\s*(game\s*)?(spread|handicap)\b/i.test(marketName)
        || /\bseries\b[^.]*\bspread\b/i.test(marketName));
  const isSupSeriesTotal = market.type === 'sup_moneyline'
    && !isSupSeriesSpread
    && (/\bseries\s*total\s*games\b/i.test(marketName)
        || /\btotal\s*games\b/i.test(marketName)
        || /\bseries\b[^.]*\btotal\b/i.test(marketName));
  // Soccer asian-handicap spreads: PX publishes them under type='sup_moneyline'
  // with name "Spread (Regular Time)". Verified 2026-05-03 EPL Tottenham FC at
  // Aston Villa FC and other EPL/UCL/sub-league games — all soccer spreads
  // ride this type-name combo. Without retagging, line-manager's supportedBase
  // gate rejects them and EPL/UCL/etc spread coverage drops to zero.
  const isSupSoccerSpread = market.type === 'sup_moneyline'
    && !isSupSeriesSpread
    && !isSupSeriesTotal
    && /^spread\b/i.test(marketName);
  if (isSupSeriesSpread) marketType = 'spread';      // retagged to 'series_spread' by line-manager
  else if (isSupSeriesTotal) marketType = 'total';   // retagged to 'series_total' by line-manager
  else if (isSupSoccerSpread) marketType = 'spread'; // soccer asian-handicap spread

  if (isSupSeriesSpread && market.selections) {
    for (const selGroup of market.selections) {
      for (const sel of selGroup) {
        if (!sel.line_id) continue;
        const raw = (sel.display_name || sel.name || '').trim();
        // Parse "TEAM +/-N.N" (e.g. "MIN +1.5", "DEN -2.5"). Team portion
        // is everything before the signed number.
        const m = raw.match(/^(.+?)\s+([+-]\d+(?:\.\d+)?)$/);
        if (!m) continue;
        const teamName = m[1].trim();
        const line = parseFloat(m[2]);
        if (!Number.isFinite(line)) continue;
        results.push({
          lineId: sel.line_id,
          marketType: 'spread',
          selection: line < 0 ? 'favorite' : 'underdog',
          teamName,
          line,
          competitorId: sel.competitor_id || null,
          outcomeName: raw,
        });
      }
    }
    return results;
  }

  if (isSupSeriesTotal && market.selections) {
    for (const selGroup of market.selections) {
      for (const sel of selGroup) {
        if (!sel.line_id) continue;
        const raw = (sel.display_name || sel.name || '').trim();
        // Parse "Over N.N" / "Under N.N"
        const m = raw.match(/^(over|under)\s+(\d+(?:\.\d+)?)$/i);
        if (!m) continue;
        const side = m[1].toLowerCase();
        const line = parseFloat(m[2]);
        if (!Number.isFinite(line)) continue;
        results.push({
          lineId: sel.line_id,
          marketType: 'total',
          selection: side,
          teamName: side,
          line,
          competitorId: null,
          outcomeName: raw,
        });
      }
    }
    return results;
  }

  if ((marketType === 'moneyline' || isF5Moneyline || isH1Moneyline) && market.selections) {
    // Moneyline: selections is array of arrays, each inner array has one object
    for (const selGroup of market.selections) {
      for (const sel of selGroup) {
        if (!sel.line_id) continue;
        results.push({
          lineId: sel.line_id,
          marketType, // preserves F5/H1 market type name
          selection: sel.competitor_id ? 'team' : 'unknown',
          teamName: cleanSelectionName(sel.display_name || sel.name || ''),
          line: null,
          competitorId: sel.competitor_id,
          outcomeName: sel.name,
        });
      }
    }
  } else if ((marketType === 'spread' || marketType === 'total' || marketType === 'team_total' || isF5Spread || isF5Total || isH1Spread || isH1Total) && market.market_lines) {
    // Spread/Total: market_lines array, each with selections
    // Include ALL alternate lines so we can respond to any RFQ
    //
    // For team_total markets, the selection display_name is "Over N" / "Under N"
    // and doesn't identify which team. Extract the team hint from the market
    // name (e.g. "SJ: Team Total Goals" → "SJ"). The line-manager matches
    // this hint against home/away team names (via abbreviation maps) to
    // determine the side.
    let teamHint = null;
    if (marketType === 'team_total') {
      // Pattern 1: "ABC: Team Total ..." or "ABC Team Total ..."
      const m1 = marketName.match(/^([^:]+?)(?::|\s+)\s*Team\s*Total/i);
      // Pattern 2: "Team Name Team Total ..." (team name before "Team Total")
      const m2 = marketName.match(/^(.+?)\s+Team\s+Total/i);
      teamHint = (m1 && m1[1].trim()) || (m2 && m2[1].trim()) || null;
    }

    for (const marketLine of market.market_lines) {
      for (const selGroup of (marketLine.selections || [])) {
        for (const sel of selGroup) {
          if (!sel.line_id) continue;

          let selection = 'unknown';
          if (marketType === 'spread' || isF5Spread || isH1Spread) {
            selection = sel.line < 0 ? 'favorite' : 'underdog';
          } else if (marketType === 'total' || marketType === 'team_total' || isF5Total || isH1Total) {
            const nameLC = (sel.name || sel.display_name || '').toLowerCase();
            selection = nameLC.includes('over') ? 'over' : nameLC.includes('under') ? 'under' : 'unknown';
          }

          // For team_total legs, pass the extracted team hint as teamName so
          // line-manager's home/away matching can work. Fall back to the
          // selection display name for non-team-total legs.
          const teamForLeg = marketType === 'team_total' && teamHint
            ? teamHint
            : cleanSelectionName(sel.display_name || sel.name || '');

          results.push({
            lineId: sel.line_id,
            marketType, // preserves F5 market type name
            selection,
            teamName: teamForLeg,
            line: sel.line != null ? sel.line : marketLine.line,
            competitorId: sel.competitor_id,
            outcomeName: sel.name,
            isFavourite: !!marketLine.favourite,
          });
        }
      }
    }
  } else if ((marketType === 'spread' || marketType === 'total' || marketType === 'team_total' || isF5Spread || isF5Total) && market.selections) {
    // Fallback: spread/total market with selections directly (no market_lines
    // wrapper). PX sometimes returns alt lines as SEPARATE market entries,
    // each a flat spread/total market with its own selections array. Without
    // this branch we'd miss thousands of alt spreads/totals per day and
    // decline 'unknown legs' unnecessarily. market.line carries the value.
    const topLine = market.line;
    for (const selGroup of market.selections) {
      for (const sel of selGroup) {
        if (!sel.line_id) continue;
        const legLine = sel.line != null ? sel.line : topLine;
        let selection = 'unknown';
        if (marketType === 'spread' || isF5Spread) {
          selection = (legLine != null && legLine < 0) ? 'favorite' : 'underdog';
        } else if (marketType === 'total' || marketType === 'team_total' || isF5Total) {
          const nameLC = (sel.name || sel.display_name || '').toLowerCase();
          selection = nameLC.includes('over') ? 'over' : nameLC.includes('under') ? 'under' : 'unknown';
        }
        results.push({
          lineId: sel.line_id,
          marketType,
          selection,
          teamName: cleanSelectionName(sel.display_name || sel.name || ''),
          line: legLine,
          competitorId: sel.competitor_id,
          outcomeName: sel.name,
        });
      }
    }
  } else if ((marketType === 'btts' || marketType === 'both_teams_to_score') && market.selections) {
    // BTTS: selections array, Yes/No outcomes
    for (const selGroup of market.selections) {
      for (const sel of selGroup) {
        if (!sel.line_id) continue;
        const nameLC = (sel.name || sel.display_name || '').toLowerCase();
        const selection = nameLC.includes('yes') ? 'yes' : nameLC.includes('no') ? 'no' : 'unknown';
        results.push({
          lineId: sel.line_id,
          marketType: 'btts',
          selection,
          teamName: sel.display_name || sel.name || '',
          line: null,
          competitorId: sel.competitor_id,
          outcomeName: sel.name,
        });
      }
    }
  } else if (marketType === 'double_chance' && market.selections) {
    // Double Chance: 3-way selections — 1X, X2, 12
    for (const selGroup of market.selections) {
      for (const sel of selGroup) {
        if (!sel.line_id) continue;
        const nameLC = (sel.name || sel.display_name || '').toLowerCase().replace(/\s+/g, '');
        let selection = 'unknown';
        if (nameLC.includes('1x') || nameLC.includes('homeordraw') || nameLC.includes('home/draw')) selection = '1X';
        else if (nameLC.includes('x2') || nameLC.includes('awayordraw') || nameLC.includes('draw/away') || nameLC.includes('draworaway')) selection = 'X2';
        else if (nameLC.includes('12') || nameLC.includes('homeoraway') || nameLC.includes('home/away')) selection = '12';
        results.push({
          lineId: sel.line_id,
          marketType: 'double_chance',
          selection,
          teamName: sel.display_name || sel.name || '',
          line: null,
          competitorId: sel.competitor_id,
          outcomeName: sel.name,
        });
      }
    }
  }

  return results;
}

function clearCooldown() {
  loginCooldownUntil = 0;
}

module.exports = {
  login,
  invalidateToken,
  clearCooldown,
  pxFetch,
  fetchSportEvents,
  fetchMarkets,
  getMarketsCacheStats,
  fetchAffiliateSportEvents,
  fetchAffiliateMultipleMarkets,
  fetchAffiliateTournaments,
  registerSupportedLines,
  removeSupportedLines,
  getSupportedLines,
  getWebSocketConfig,
  registerWebSocket,
  submitOffer,
  getPrecisionDiagSnapshot,
  confirmOrder,
  fetchBalance,
  fetchOrders,
  fetchOrderByUuid,
  parseMarketSelections,
};
