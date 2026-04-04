const Pusher = require('pusher-js');
const { config } = require('../config');
const log = require('./logger');
const px = require('./prophetx');
const pricer = require('./pricer');
const orderTracker = require('./order-tracker');

// ---------------------------------------------------------------------------
// STATE
// ---------------------------------------------------------------------------

let pusherClient = null;
let broadcastChannel = null;
let privateChannel = null;
let channelNames = { broadcast: null, private: null };
let paused = false;
let connectionState = 'disconnected';
let lastHealthCheck = null;
let healthCheckTimer = null;
let reconnectAttempts = 0;
let channelAuth = {}; // { channelName: authString } from PX registration

// ---------------------------------------------------------------------------
// CONNECT
// ---------------------------------------------------------------------------

/**
 * Full connection sequence:
 * 1. Get Pusher config from PX
 * 2. Create Pusher client
 * 3. Wait for connection
 * 4. Register socket_id with PX
 * 5. Subscribe to broadcast + private channels
 * 6. Bind event handlers
 */
async function connect() {
  log.info('WS', 'Starting WebSocket connection...');

  // 1. Get Pusher config
  const wsConfig = await px.getWebSocketConfig();
  log.info('WS', `Pusher config: key=${wsConfig.key}, cluster=${wsConfig.cluster}`);

  // 2. Create Pusher client
  pusherClient = new Pusher(wsConfig.key, {
    cluster: wsConfig.cluster,
    forceTLS: true,
    // Custom authorizer — uses auth tokens from PX registration response
    authorizer: (channel, options) => ({
      authorize: (socketId, callback) => {
        // Use pre-fetched auth from PX registration
        const auth = channelAuth[channel.name];
        if (auth) {
          log.debug('WS', `Authorizing channel ${channel.name} with cached auth`);
          callback(null, { auth });
          return;
        }

        // Fallback: call PX auth endpoint directly
        log.info('WS', `No cached auth for ${channel.name}, calling PX auth endpoint...`);
        (async () => {
          try {
            const token = await px.login();
            const resp = await require('node-fetch')(
              `${require('../config').config.px.baseUrl}/parlay/sp/websocket/auth`,
              {
                method: 'POST',
                headers: {
                  'Authorization': `Bearer ${token}`,
                  'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                  socket_id: socketId,
                  channel_name: channel.name,
                }),
              }
            );
            const data = await resp.json();
            callback(null, data);
          } catch (err) {
            log.error('WS', `Channel auth failed for ${channel.name}: ${err.message}`);
            callback(err);
          }
        })();
      },
    }),
  });

  // 3. Wait for connection
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Pusher connection timeout (30s)'));
    }, 30000);

    pusherClient.connection.bind('connected', async () => {
      clearTimeout(timeout);
      connectionState = 'connected';
      reconnectAttempts = 0;
      const socketId = pusherClient.connection.socket_id;
      log.info('WS', `Connected! socket_id=${socketId}`);

      try {
        // 4. Register socket with PX
        const regData = await px.registerWebSocket(socketId);
        log.info('WS', 'WebSocket registered with PX', regData);

        // Parse channel info from registration response
        parseChannels(regData);

        // 5-6. Subscribe and bind
        subscribeAndBind();

        // Start health check monitor
        startHealthCheckMonitor();

        resolve();
      } catch (err) {
        log.error('WS', `Post-connect setup failed: ${err.message}`);
        reject(err);
      }
    });

    pusherClient.connection.bind('error', (err) => {
      log.error('WS', `Connection error: ${JSON.stringify(err)}`);
    });

    // Monitor state changes
    pusherClient.connection.bind('state_change', (states) => {
      connectionState = states.current;
      log.info('WS', `State: ${states.previous} → ${states.current}`);

      if (states.current === 'disconnected' || states.current === 'failed') {
        reconnectAttempts++;
        if (reconnectAttempts > 10) {
          log.error('WS', 'Too many reconnect attempts — stopping');
        }
      }

      // On reconnect, re-register
      if (states.current === 'connected' && states.previous !== 'initialized') {
        handleReconnect();
      }
    });
  });
}

/**
 * Parse channel names from the PX registration response.
 *
 * Actual PX response format:
 * {
 *   auth: "...",
 *   data: {
 *     success: true,
 *     status: "CONNECTED",
 *     authorized_channel: [
 *       { channel_name: "private-broadcast-service=4-device_type=5", auth: "...", binding_events: ["price.ask.new", "order.matched"] },
 *       { channel_name: "private-service=4-device_type=5-user=xxx", auth: "...", binding_events: ["price.confirm.new", ...] }
 *     ]
 *   }
 * }
 *
 * Note: Both channels are private- prefixed. The broadcast channel has "broadcast" in the name.
 */
function parseChannels(regData) {
  // Store per-channel auth tokens for the Pusher authorizer
  channelAuth = {};

  const d = regData.data || regData;

  // Handle the authorized_channel array format
  if (d.authorized_channel && Array.isArray(d.authorized_channel)) {
    for (const ch of d.authorized_channel) {
      const name = ch.channel_name;
      if (!name) continue;

      // Store auth for this channel
      channelAuth[name] = ch.auth;

      // Classify channel by name
      if (name.includes('broadcast')) {
        channelNames.broadcast = name;
        log.info('WS', `Broadcast channel: ${name} (events: ${(ch.binding_events || []).join(', ')})`);
      } else {
        channelNames.private = name;
        log.info('WS', `Private channel: ${name} (events: ${(ch.binding_events || []).join(', ')})`);
      }
    }
  }

  // Fallback: try other response formats
  if (!channelNames.broadcast && !channelNames.private) {
    if (d.channels) {
      for (const ch of (Array.isArray(d.channels) ? d.channels : [d.channels])) {
        const name = ch.name || ch.channel_name || ch;
        if (typeof name === 'string') {
          if (name.includes('broadcast')) channelNames.broadcast = name;
          else channelNames.private = name;
        }
      }
    }
  }

  if (!channelNames.broadcast && !channelNames.private) {
    log.error('WS', 'Could not parse channel names from registration response:', JSON.stringify(regData).substring(0, 500));
  }

  log.info('WS', `Channels configured: broadcast=${channelNames.broadcast}, private=${channelNames.private}`);
}

/**
 * Subscribe to channels and bind event handlers.
 */
function subscribeAndBind() {
  // Broadcast channel — RFQ events
  if (channelNames.broadcast) {
    broadcastChannel = pusherClient.subscribe(channelNames.broadcast);
    broadcastChannel.bind('price.ask.new', handleRFQ);
    broadcastChannel.bind('order.matched', handleOrderMatched);
    broadcastChannel.bind('pusher:subscription_succeeded', () => {
      log.info('WS', `Subscribed to broadcast: ${channelNames.broadcast}`);
    });
    broadcastChannel.bind('pusher:subscription_error', (err) => {
      log.error('WS', `Broadcast subscription error: ${JSON.stringify(err)}`);
    });
  }

  // Private channel — confirmations, settlements
  if (channelNames.private) {
    privateChannel = pusherClient.subscribe(channelNames.private);
    privateChannel.bind('price.confirm.new', handleConfirm);
    privateChannel.bind('order.finalized', handleOrderFinalized);
    privateChannel.bind('order.settled', handleLegSettled);
    privateChannel.bind('parlay.settled', handleParlaySettled);
    privateChannel.bind('parlay.processing', (data) => {
      log.info('WS', 'Parlay processing', data);
    });
    privateChannel.bind('parlay.finalized', (data) => {
      log.info('WS', 'Parlay finalized', data);
    });
    privateChannel.bind('refund.processed', (data) => {
      log.info('WS', 'Refund processed', data);
    });
    privateChannel.bind('pusher:subscription_succeeded', () => {
      log.info('WS', `Subscribed to private: ${channelNames.private}`);
    });
    privateChannel.bind('pusher:subscription_error', (err) => {
      log.error('WS', `Private subscription error: ${JSON.stringify(err)}`);
    });
  }

  // Bind health check on all channels
  if (broadcastChannel) {
    broadcastChannel.bind('health_check', handleHealthCheck);
  }
  if (privateChannel) {
    privateChannel.bind('health_check', handleHealthCheck);
  }
}

// ---------------------------------------------------------------------------
// EVENT HANDLERS
// ---------------------------------------------------------------------------

/**
 * Handle incoming RFQ (price.ask.new).
 */
async function handleRFQ(data) {
  const startTime = Date.now();

  if (paused) {
    log.debug('WS', 'Paused — ignoring RFQ');
    return;
  }

  try {
    // RFQ data is nested under data.payload
    const payload = data.payload || data;
    const parlayId = payload.parlay_id || payload.parlayId;
    const legs = payload.market_lines || payload.legs || [];
    const callbackUrl = payload.callback_url || payload.callbackUrl;

    log.info('RFQ', `Received: parlay=${parlayId}, legs=${legs.length}`);

    // Quick decline check
    if (pricer.shouldDecline(legs)) {
      const lineManager = require('./line-manager');
      const knownLegs = [];
      const unknownLegs = [];
      const unknownSports = [];
      for (const l of legs) {
        const lineId = l.line_id || l.lineId || l;
        const info = lineManager.lookupLine(lineId);
        if (info) {
          knownLegs.push({ team: info.teamName, market: info.marketType, sport: info.sport, line: info.line });
        } else {
          unknownLegs.push(lineId);
          // Build specific description with market detail
          const eventName = l.sport_event_id ? lineManager.getEventName(l.sport_event_id) : null;
          const tName = l.tournament_id ? lineManager.getTournamentName(l.tournament_id) : null;
          const baseName = eventName || tName || 'unknown';
          // Add context: is this a known event but unregistered line?
          const isKnownEvent = !!eventName;

          // Build a simple detail string from what we know
          // Don't try to map market_ids — PX reuses them across different market types
          const hasLine = l.line != null;
          const lineNum = hasLine ? l.line : null;
          const origLine = l.origin_market_line != null ? l.origin_market_line : null;

          let detail;
          if (hasLine && origLine != null && lineNum !== origLine) {
            // Alt line — show the line it's based on
            detail = `alt line ${lineNum} (primary: ${origLine})`;
          } else if (hasLine) {
            detail = `line ${lineNum}`;
          } else {
            detail = 'no line (prop or 2-way)';
          }
          const tag = isKnownEvent ? '[unregistered market]' : '[unsupported event]';
          unknownSports.push(`${baseName} ${tag} ${detail}`);
        }
      }
      const reason = unknownLegs.length > 0 ? 'unknown legs' : 'exposure/limit';
      orderTracker.recordDecline(reason, { parlayId, legs, knownLegs, unknownLegs, unknownSports });
      return;
    }

    // Price the parlay
    const result = await pricer.priceParlay(legs);
    if (!result) {
      // Near miss — all legs known but couldn't get fair values
      const lineManager = require('./line-manager');
      const knownLegs = legs.map(l => {
        const info = lineManager.lookupLine(l.line_id || l.lineId || l);
        if (!info) return null;
        // For totals, include game context
        let team = info.teamName;
        if (info.marketType === 'total' && info.homeTeam && info.awayTeam) {
          team = `${team} (${info.awayTeam} @ ${info.homeTeam})`;
        }
        return { team, market: info.marketType, sport: info.sport, line: info.line, homeTeam: info.homeTeam, awayTeam: info.awayTeam };
      }).filter(Boolean);
      orderTracker.recordDecline('no fair value', { parlayId, knownLegs });
      return;
    }

    // Record the quote
    orderTracker.recordQuote(
      parlayId,
      result.meta.legs,
      result.meta.americanOdds, // Store American odds (matches PX format)
      result.meta.maxRisk,
      result.meta.fairParlayProb,
      result.meta
    );

    // Submit offer to PX
    if (callbackUrl) {
      log.info('RFQ', `Submitting: parlay=${parlayId}, decimal=${result.meta.decimalOdds}, american=${result.meta.americanOdds}, offer=${JSON.stringify(result.offer)}`);
      await px.submitOffer(callbackUrl, parlayId, [result.offer]);
      const elapsed = Date.now() - startTime;
      log.info('RFQ', `Offered: parlay=${parlayId}, odds=${result.meta.decimalOdds}, fair=${result.meta.fairParlayProb.toFixed(5)}, vig=${result.meta.vig}, ${elapsed}ms`);
    } else {
      log.warn('RFQ', `No callback URL for parlay ${parlayId}`);
    }
  } catch (err) {
    log.error('RFQ', `Error handling RFQ: ${err.message}`);
  }
}

/**
 * Handle confirmation request (price.confirm.new).
 */
async function handleConfirm(data) {
  try {
    // Confirmation data is nested under data.payload
    const payload = data.payload || data;
    const parlayId = payload.parlay_id || payload.parlayId;
    const orderUuid = payload.order_uuid || payload.orderUuid;
    const callbackUrl = payload.callback_url || payload.callbackUrl;
    const confirmedOdds = payload.odds || payload.confirmed_odds;
    const confirmedStake = payload.stake || payload.confirmed_stake;

    log.info('Confirm', `Received: parlay=${parlayId}, order=${orderUuid}, odds=${confirmedOdds}, stake=$${confirmedStake}`);

    // Find our original quote
    const originalOrder = orderTracker.findByParlayId(parlayId);
    if (!originalOrder) {
      log.warn('Confirm', `No quote found for parlay ${parlayId} — rejecting`);
      if (callbackUrl) {
        await px.confirmOrder(callbackUrl, orderUuid, 'reject');
      }
      return;
    }

    // Check stake/risk limits before accepting
    // confirmedStake = bettor's wager, confirmedOdds = negative (SP side)
    // Our risk = bettor's profit = stake × |odds| / 100 (using bettor's positive odds)
    const bettorOdds = Math.abs(confirmedOdds || 0);
    const ourRisk = bettorOdds >= 100
      ? confirmedStake * bettorOdds / 100
      : confirmedStake;
    const maxRisk = config.pricing.maxRiskPerParlay;
    if (maxRisk > 0 && ourRisk > maxRisk) {
      log.warn('Confirm', `Rejecting: our risk $${ourRisk.toFixed(2)} exceeds max $${maxRisk} (stake=$${confirmedStake}, odds=${confirmedOdds})`);
      orderTracker.recordRejection(parlayId, `risk $${ourRisk.toFixed(0)} > max $${maxRisk}`);
      if (callbackUrl) {
        await px.confirmOrder(callbackUrl, orderUuid, 'reject');
      }
      return;
    }

    // Check portfolio-level drawdown limit
    const maxDrawdown = config.pricing.bankroll * config.pricing.maxDrawdownPct / 100;
    const portfolioCheck = orderTracker.checkPortfolioRisk(ourRisk, maxDrawdown);
    if (!portfolioCheck.allowed) {
      log.warn('Confirm', `Rejecting: portfolio risk $${portfolioCheck.current.toFixed(0)} + $${ourRisk.toFixed(0)} > max drawdown $${maxDrawdown.toFixed(0)}`);
      orderTracker.recordRejection(parlayId, `portfolio risk $${(portfolioCheck.current + ourRisk).toFixed(0)} > $${maxDrawdown.toFixed(0)}`);
      if (callbackUrl) {
        await px.confirmOrder(callbackUrl, orderUuid, 'reject');
      }
      return;
    }

    // Re-validate pricing
    const validation = await pricer.validateForConfirmation(parlayId, originalOrder.meta);
    if (!validation.valid) {
      log.warn('Confirm', `Rejecting: ${validation.reason}`);
      orderTracker.recordRejection(parlayId, validation.reason);
      if (callbackUrl) {
        await px.confirmOrder(callbackUrl, orderUuid, 'reject');
      }
      return;
    }

    // Accept the order
    orderTracker.recordConfirmation(parlayId, orderUuid, confirmedOdds, confirmedStake);

    // Build price_probability for the confirmation
    const priceProbability = [{
      max_risk: originalOrder.maxRisk,
      computed_odds: confirmedOdds,
      vig: config.pricing.defaultVig,
      lines: originalOrder.meta.legs.map(l => ({
        line_id: l.lineId,
        probability: l.fairProb,
      })),
    }];

    if (callbackUrl) {
      await px.confirmOrder(
        callbackUrl,
        orderUuid,
        'accept',
        confirmedOdds,
        confirmedStake,
        priceProbability
      );
      log.info('Confirm', `Accepted: order=${orderUuid}`);
    }
  } catch (err) {
    log.error('Confirm', `Error handling confirmation: ${err.message}`);
  }
}

/**
 * Handle order matched (broadcast — all SPs see this when any parlay gets filled).
 */
function handleOrderMatched(data) {
  const payload = data.payload || data;
  const parlayId = payload.parlay_id || payload.parlayId;
  const matchedOdds = payload.matched_odds;
  const matchedStake = payload.matched_stake;
  const legs = payload.market_lines || [];

  const lineManager = require('./line-manager');
  const entry = orderTracker.recordMatchedParlay(parlayId, matchedOdds, matchedStake, legs, lineManager);

  log.info('Market', `Matched: parlay=${(parlayId||'').substring(0,8)}, odds=${matchedOdds}, stake=$${matchedStake}, legs=${legs.length}, outcome=${entry.outcome}`);
}

/**
 * Handle order finalized (private — our order confirmed by PX).
 */
function handleOrderFinalized(data) {
  const payload = data.payload || data;
  const orderUuid = payload.order_uuid || payload.orderUuid;
  const parlayId = payload.parlay_id || payload.parlayId;
  log.info('WS', `Order finalized: ${orderUuid}`, payload);

  // Store the orderUuid on the order — this is the only event that has it
  if (parlayId && orderUuid) {
    orderTracker.recordFinalized(parlayId, orderUuid, payload);
  }
}

/**
 * Handle individual leg settlement.
 */
function handleLegSettled(data) {
  const payload = data.payload || data;
  const orderUuid = payload.order_uuid || payload.orderUuid;
  const status = payload.status || payload.settlement_status;
  log.info('WS', `Leg settled: order=${orderUuid}, status=${status}`, payload);

  if (orderUuid) {
    orderTracker.recordLegSettlement(orderUuid, payload);
  }
}

/**
 * Handle full parlay settlement.
 */
function handleParlaySettled(data) {
  const payload = data.payload || data;
  const orderUuid = payload.order_uuid || payload.orderUuid;
  const result = payload.result || payload.status;
  const payout = payload.payout || 0;

  log.info('Settle', `Parlay settled: order=${orderUuid}, result=${result}, payout=${payout}`);
  orderTracker.recordSettlement(orderUuid, result, payout);
}

/**
 * Handle health check heartbeat.
 */
function handleHealthCheck(data) {
  lastHealthCheck = Date.now();
  log.debug('WS', 'Health check received');
}

// ---------------------------------------------------------------------------
// HEALTH CHECK MONITOR
// ---------------------------------------------------------------------------

function startHealthCheckMonitor() {
  if (healthCheckTimer) clearInterval(healthCheckTimer);

  healthCheckTimer = setInterval(() => {
    if (lastHealthCheck && (Date.now() - lastHealthCheck) > 60000) {
      log.warn('WS', 'No health check received in 60s — connection may be stale');
      // Pusher-js handles reconnection automatically, but log the warning
    }
  }, 30000);
}

// ---------------------------------------------------------------------------
// RECONNECT
// ---------------------------------------------------------------------------

async function handleReconnect() {
  log.info('WS', 'Reconnected — re-registering socket...');
  try {
    const socketId = pusherClient.connection.socket_id;
    const regData = await px.registerWebSocket(socketId);
    parseChannels(regData);
    // Channels should auto-resubscribe with Pusher-js
    log.info('WS', 'Re-registration complete');
  } catch (err) {
    log.error('WS', `Re-registration failed: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// CONTROL
// ---------------------------------------------------------------------------

function pause() {
  paused = true;
  log.info('WS', 'Paused — will not respond to RFQs');
}

function resume() {
  paused = false;
  log.info('WS', 'Resumed — will respond to RFQs');
}

function disconnect() {
  if (healthCheckTimer) clearInterval(healthCheckTimer);
  if (pusherClient) {
    pusherClient.disconnect();
    log.info('WS', 'Disconnected');
  }
  connectionState = 'disconnected';
}

function getState() {
  return {
    connectionState,
    paused,
    lastHealthCheck: lastHealthCheck ? new Date(lastHealthCheck).toISOString() : null,
    reconnectAttempts,
    channels: channelNames,
  };
}

module.exports = {
  connect,
  disconnect,
  pause,
  resume,
  getState,
};
