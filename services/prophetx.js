const fetch = require('node-fetch');
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

async function fetchMarkets(eventId) {
  const data = await pxFetch(`/partner/mm/get_markets?event_id=${eventId}`);
  return data.data?.markets || [];
}

// ---------------------------------------------------------------------------
// SUPPORTED LINES
// ---------------------------------------------------------------------------

async function registerSupportedLines(lineIds) {
  if (!lineIds || lineIds.length === 0) return { success: true, count: 0 };
  log.info('PX-Lines', `Registering ${lineIds.length} supported lines`);
  const data = await pxFetch('/parlay/sp/supported-lines', 'POST', {
    supported_lines: lineIds,
  });
  return data;
}

async function removeSupportedLines(lineIds) {
  if (!lineIds || lineIds.length === 0) return { success: true };
  log.info('PX-Lines', `Removing ${lineIds.length} supported lines`);
  const data = await pxFetch('/parlay/sp/supported-lines', 'DELETE', {
    supported_lines: lineIds,
  });
  return data;
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

async function submitOffer(callbackUrl, parlayId, offers) {
  log.info('PX-Offer', `Submitting offer for parlay ${parlayId}`, {
    callbackUrl,
    payload: JSON.stringify({ parlay_id: parlayId, offers }).substring(0, 500),
  });
  try {
    const data = await pxFetch(callbackUrl, 'POST', {
      parlay_id: parlayId,
      offers,
    }, false); // useBaseUrl=false — callbackUrl is absolute
    log.info('PX-Offer', `Response for ${parlayId}: ${JSON.stringify(data).substring(0, 300)}`);
    return data;
  } catch (err) {
    log.error('PX-Offer', `Failed to submit offer for ${parlayId}: ${err.message}`);
    throw err;
  }
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
  const marketType = market.type; // 'moneyline', 'spread', 'total'

  // Strip trailing odds from team names (e.g., "Kansas City Royals -103" → "Kansas City Royals")
  function cleanSelectionName(name) {
    if (!name) return '';
    // Remove trailing odds pattern: space + optional sign + digits (e.g., " -103", " +275", " 150")
    return name.replace(/\s+[+-]?\d+(\.\d+)?$/, '').trim();
  }

  // F5 moneyline uses same structure as full-game moneyline (selections array)
  const isF5Moneyline = /first_5_innings_moneyline|first_five_innings_moneyline/.test(marketType);
  // F5 spread/total uses same structure as full-game spread/total (market_lines)
  const isF5Spread = /first_5_innings_run_line|first_five_innings_run_line/.test(marketType);
  const isF5Total = /first_5_innings_total|first_five_innings_total/.test(marketType);

  if ((marketType === 'moneyline' || isF5Moneyline) && market.selections) {
    // Moneyline: selections is array of arrays, each inner array has one object
    for (const selGroup of market.selections) {
      for (const sel of selGroup) {
        if (!sel.line_id) continue;
        results.push({
          lineId: sel.line_id,
          marketType, // preserves F5 market type name
          selection: sel.competitor_id ? 'team' : 'unknown',
          teamName: cleanSelectionName(sel.display_name || sel.name || ''),
          line: null,
          competitorId: sel.competitor_id,
          outcomeName: sel.name,
        });
      }
    }
  } else if ((marketType === 'spread' || marketType === 'total' || marketType === 'team_total' || isF5Spread || isF5Total) && market.market_lines) {
    // Spread/Total: market_lines array, each with selections
    // Include ALL alternate lines so we can respond to any RFQ
    for (const marketLine of market.market_lines) {
      for (const selGroup of (marketLine.selections || [])) {
        for (const sel of selGroup) {
          if (!sel.line_id) continue;

          let selection = 'unknown';
          if (marketType === 'spread' || isF5Spread) {
            selection = sel.line < 0 ? 'favorite' : 'underdog';
          } else if (marketType === 'total' || marketType === 'team_total' || isF5Total) {
            const nameLC = (sel.name || sel.display_name || '').toLowerCase();
            selection = nameLC.includes('over') ? 'over' : nameLC.includes('under') ? 'under' : 'unknown';
          }

          results.push({
            lineId: sel.line_id,
            marketType, // preserves F5 market type name
            selection,
            teamName: cleanSelectionName(sel.display_name || sel.name || ''),
            line: sel.line != null ? sel.line : marketLine.line,
            competitorId: sel.competitor_id,
            outcomeName: sel.name,
            isFavourite: !!marketLine.favourite,
          });
        }
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
  registerSupportedLines,
  removeSupportedLines,
  getSupportedLines,
  getWebSocketConfig,
  registerWebSocket,
  submitOffer,
  confirmOrder,
  fetchBalance,
  fetchOrders,
  parseMarketSelections,
};
