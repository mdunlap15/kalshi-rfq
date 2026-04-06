const fetch = require('node-fetch');
const { config } = require('../config');
const log = require('./logger');

// Token cache
let tokenCache = { token: null, refreshToken: null, time: 0 };

// ---------------------------------------------------------------------------
// AUTH
// ---------------------------------------------------------------------------

async function login() {
  const age = (Date.now() - tokenCache.time) / 1000 / 60;
  if (tokenCache.token && age < config.px.tokenTtlMinutes) {
    return tokenCache.token;
  }

  // Try refresh first (doesn't create a new session)
  if (tokenCache.refreshToken) {
    try {
      const refreshed = await refreshSession();
      if (refreshed) return refreshed;
    } catch (err) {
      log.warn('PX-Auth', `Refresh failed: ${err.message}, falling back to login`);
    }
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
    throw new Error(`ProphetX login failed (${resp.status}): ${text}`);
  }

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
    invalidateToken();
    throw new Error(`ProphetX API 401 on ${method} ${endpoint} — token invalidated`);
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

  if (marketType === 'moneyline' && market.selections) {
    // Moneyline: selections is array of arrays, each inner array has one object
    for (const selGroup of market.selections) {
      for (const sel of selGroup) {
        if (!sel.line_id) continue;
        results.push({
          lineId: sel.line_id,
          marketType: 'moneyline',
          selection: sel.competitor_id ? 'team' : 'unknown',
          teamName: cleanSelectionName(sel.display_name || sel.name || ''),
          line: null,
          competitorId: sel.competitor_id,
          outcomeName: sel.name,
        });
      }
    }
  } else if ((marketType === 'spread' || marketType === 'total') && market.market_lines) {
    // Spread/Total: market_lines array, each with selections
    // Include ALL alternate lines so we can respond to any RFQ
    for (const marketLine of market.market_lines) {
      for (const selGroup of (marketLine.selections || [])) {
        for (const sel of selGroup) {
          if (!sel.line_id) continue;

          let selection = 'unknown';
          if (marketType === 'spread') {
            selection = sel.line < 0 ? 'favorite' : 'underdog';
          } else if (marketType === 'total') {
            const nameLC = (sel.name || sel.display_name || '').toLowerCase();
            selection = nameLC.includes('over') ? 'over' : nameLC.includes('under') ? 'under' : 'unknown';
          }

          results.push({
            lineId: sel.line_id,
            marketType,
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
  }

  return results;
}

module.exports = {
  login,
  invalidateToken,
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
