const log = require('./logger');
const db = require('./db');

// ---------------------------------------------------------------------------
// IN-MEMORY ORDER STORE (backed by Supabase for persistence)
// ---------------------------------------------------------------------------

const orders = {}; // keyed by parlayId
const ordersByUuid = {}; // secondary index: orderUuid → parlayId

// ---------------------------------------------------------------------------
// MARKET INTELLIGENCE — tracks all matched parlays across all SPs
// ---------------------------------------------------------------------------
const matchedParlays = []; // array of { parlayId, matchedOdds, matchedStake, legs, matchedAt, weQuoted, ourOdds, outcome }
const marketStats = {
  totalMatched: 0,
  weQuoted: 0,
  weWon: 0,
  weLost: 0, // quoted but another SP won
  missedNoQuote: 0, // didn't quote at all
};

// ---------------------------------------------------------------------------
// DECLINE TRACKING
// ---------------------------------------------------------------------------
const declineStats = {
  total: 0,
  reasons: {}, // { 'unknown legs': count, 'no fair value': count, etc. }
  unknownSports: {}, // { 'Soccer': count, 'unknown': count } — sports from unknown legs
  nearMisses: [], // RFQs where all legs were known but couldn't price (no fair value)
  // Rolling log of recent declines for the alert banner
  recent: [], // { reason, detail, time, parlayId }
};
// Limit-related reasons — these are the ones we alert on (user-controllable)
const LIMIT_REASONS = new Set([
  'team exposure limit',
  'game exposure limit',
  'portfolio drawdown limit',
  'too many legs',
]);
// Reject reasons from risk checks at confirmation time
const rejectStats = {
  total: 0,
  reasons: {}, // { 'risk $123 > max $100': count }
  recent: [], // { reason, time, parlayId }
};
// Per-parlay decline lookup — lets us explain "No quote" outcomes in matched parlays
const declinesByParlayId = {}; // { parlayId: { reason, unknownLineIds, unknownDetails, declinedAt } }
const MAX_DECLINE_ENTRIES = 2000;
const declineIdOrder = []; // FIFO to cap memory

// ---------------------------------------------------------------------------
// NET EXPOSURE TRACKING — tracks risk per game, accounting for collected stakes
// ---------------------------------------------------------------------------
// Stores all confirmed parlay legs grouped by pxEventId (game)
// Net exposure = weighted payouts owed - stakes collected from offsetting positions
const gameExposure = {};  // keyed by pxEventId
// Legacy team exposure kept for backward compat with dashboard
const exposure = {};

// Running stats
const stats = {
  totalQuotes: 0,
  totalConfirmations: 0,
  totalRejections: 0,
  totalSettlements: 0,
  totalWins: 0,
  totalLosses: 0,
  runningPnL: 0,
  startedAt: new Date().toISOString(),
};

// ---------------------------------------------------------------------------
// RECORD FUNCTIONS
// ---------------------------------------------------------------------------

function recordQuote(parlayId, legs, offeredOdds, maxRisk, fairParlayProb, meta) {
  stats.totalQuotes++;

  orders[parlayId] = {
    parlayId,
    legs,
    offeredOdds,
    maxRisk,
    fairParlayProb,
    meta,
    status: 'quoted',
    quotedAt: new Date().toISOString(),
    confirmedAt: null,
    confirmedOdds: null,
    confirmedStake: null,
    orderUuid: null,
    settledAt: null,
    settlementResult: null,
    pnl: null,
  };

  log.info('Orders', `Quote #${stats.totalQuotes}: parlay=${parlayId}, legs=${legs.length}, odds=${offeredOdds}, fair=${fairParlayProb.toFixed(5)}`);
  db.saveOrder(orders[parlayId]).catch(() => {});
  return orders[parlayId];
}

function recordConfirmation(parlayId, orderUuid, confirmedOdds, confirmedStake) {
  const order = orders[parlayId];
  if (order) {
    // Never revert a settled order to confirmed (duplicate/late event guard)
    if (order.status && order.status.startsWith('settled_')) {
      log.debug('Orders', `Ignoring late confirmation for already-settled parlay ${parlayId}`);
      if (orderUuid && !ordersByUuid[orderUuid]) ordersByUuid[orderUuid] = parlayId;
      return order;
    }

    // Don't double-count confirmations for the same parlay
    if (order.status !== 'confirmed') {
      stats.totalConfirmations++;
    }

    order.status = 'confirmed';
    order.confirmedAt = new Date().toISOString();
    order.confirmedOdds = confirmedOdds;
    order.confirmedStake = confirmedStake;
    order.orderUuid = orderUuid;

    if (orderUuid) {
      ordersByUuid[orderUuid] = parlayId;
    }

    // Track exposure per team/selection
    addExposure(order);

    log.info('Orders', `Confirmed: parlay=${parlayId}, order=${orderUuid}, odds=${confirmedOdds}, stake=$${confirmedStake}`);
    db.saveOrder(order).catch(err => log.error('DB', `saveOrder(confirmation) failed: ${err.message}`));
  } else {
    log.warn('Orders', `Confirmation for unknown parlay ${parlayId}`);
  }
  return order;
}

function recordRejection(parlayId, reason) {
  stats.totalRejections++;
  rejectStats.total++;

  // Bucket by reason prefix (strip dollar amounts for aggregation)
  const bucket = (reason || 'unknown')
    .replace(/\$[\d,.]+/g, '$')
    .replace(/\s+/g, ' ')
    .trim();
  rejectStats.reasons[bucket] = (rejectStats.reasons[bucket] || 0) + 1;
  rejectStats.recent.unshift({
    reason,
    bucket,
    parlayId,
    time: new Date().toISOString(),
  });
  if (rejectStats.recent.length > 100) rejectStats.recent.pop();

  const order = orders[parlayId];
  if (order) {
    order.status = 'rejected';
    order.rejectedAt = new Date().toISOString();
    order.rejectionReason = reason;
    log.info('Orders', `Rejected: parlay=${parlayId}, reason=${reason}`);
  }
  return order;
}

/**
 * Record order finalization — this is where we get the order_uuid.
 * The price.confirm.new event doesn't include it; order.finalized does.
 */
function recordFinalized(parlayId, orderUuid, payload) {
  const order = orders[parlayId];
  if (!order) {
    log.warn('Orders', `Finalized event for unknown parlay ${parlayId}`);
    return null;
  }

  order.orderUuid = orderUuid;
  ordersByUuid[orderUuid] = parlayId;

  // Also capture confirmed values from finalized event if we missed them
  if (payload) {
    if (payload.confirmed_stake && !order.confirmedStake) order.confirmedStake = payload.confirmed_stake;
    if (payload.confirmed_odds && !order.confirmedOdds) order.confirmedOdds = payload.confirmed_odds;
  }

  log.info('Orders', `Finalized: parlay=${parlayId}, order=${orderUuid}`);
  db.saveOrder(order).catch(() => {});
  return order;
}

/**
 * Record individual leg settlement.
 */
function recordLegSettlement(orderUuid, legPayload) {
  const parlayId = ordersByUuid[orderUuid];
  const order = parlayId ? orders[parlayId] : null;
  if (!order) return;

  const lineId = legPayload.line_id || legPayload.lineId;
  const status = legPayload.settlement_status || legPayload.status;
  if (!lineId || !status) return;

  const legs = order.legs || order.meta?.legs || [];
  for (const leg of legs) {
    if (leg.lineId === lineId || leg.line_id === lineId) {
      leg.settlementStatus = status;
      break;
    }
  }

  db.saveOrder(order).catch(() => {});
}

/**
 * Convert American odds to profit on a given stake.
 * Positive (+500): profit = stake * odds / 100
 * Negative (-200): profit = stake * 100 / |odds|
 */
function americanOddsToProfit(americanOdds, stake) {
  const odds = Number(americanOdds);
  if (!odds || !stake) return 0;
  if (odds >= 100) return stake * odds / 100;
  if (odds <= -100) return stake * 100 / Math.abs(odds);
  return 0;
}

function recordSettlement(orderUuid, result, payout) {
  const parlayId = ordersByUuid[orderUuid];
  const order = parlayId ? orders[parlayId] : null;

  if (order) {
    // Don't re-settle
    if (order.status && order.status.startsWith('settled_')) {
      log.debug('Orders', `Already settled: order=${orderUuid}`);
      return order;
    }

    stats.totalSettlements++;
    order.settledAt = new Date().toISOString();
    order.settlementResult = result; // 'won', 'lost', 'push', 'void'

    // Calculate P&L from SP perspective (house side).
    // `result` is ALWAYS passed as SP-perspective by callers:
    //   'won'  = SP won (bettor's parlay missed ≥1 leg) → +bettor's wager
    //   'lost' = SP lost (all bettor legs hit) → -confirmedStake
    // Callers (WS handler, poll) normalize before calling this function.
    //   confirmedStake = SP's to-win amount = bettor's potential payout
    //   bettor's wager = americanOddsToProfit(confirmedOdds, confirmedStake)
    const bettorWager = americanOddsToProfit(order.confirmedOdds, order.confirmedStake);

    if (result === 'won') {
      // SP won — bettor's parlay lost, we keep their wager
      order.pnl = bettorWager;
      stats.totalWins++;
    } else if (result === 'lost') {
      // SP lost — bettor's parlay won, we pay out our stake
      order.pnl = -(order.confirmedStake || 0);
      stats.totalLosses++;
    } else if (result === 'push' || result === 'void') {
      order.pnl = 0;
    }

    if (order.pnl != null) {
      stats.runningPnL += order.pnl;
    }

    // Release exposure for settled parlay
    removeExposure(order);

    order.status = `settled_${result}`;
    log.info('Orders', `Settled: order=${orderUuid}, result=${result}, pnl=$${order.pnl?.toFixed(2)}, running=$${stats.runningPnL.toFixed(2)}`);
    // Critical — log errors on settlement saves so we never silently lose a settled order
    db.saveOrder(order).catch(err => log.error('DB', `CRITICAL: saveOrder(settlement) failed for ${order.parlayId}: ${err.message}`));
  } else {
    log.warn('Orders', `Settlement for unknown order ${orderUuid}`);
  }

  return order;
}

// ---------------------------------------------------------------------------
// MARKET INTELLIGENCE
// ---------------------------------------------------------------------------

/**
 * Record a matched parlay from the broadcast channel (any SP won it).
 * Compare against our quotes to build competitive intel.
 */
function recordMatchedParlay(parlayId, matchedOdds, matchedStake, legs, lineManager) {
  marketStats.totalMatched++;

  const ourQuote = orders[parlayId] || null;
  const weQuoted = !!ourQuote;

  // Lookup decline info for this parlay so we can flag the problematic leg(s)
  const declineInfo = declinesByParlayId[parlayId] || null;
  const unknownSet = new Set(declineInfo?.unknownLineIds || []);

  // Resolve leg info from line_ids
  const resolvedLegs = (legs || []).map(l => {
    const lineId = l.line_id || l.lineId;
    const info = lineManager ? lineManager.lookupLine(lineId) : null;
    let team = info?.teamName || 'Unknown';
    // For totals, include the game context
    if (info?.marketType === 'total' && info?.homeTeam && info?.awayTeam) {
      team = `${team} (${info.awayTeam} @ ${info.homeTeam})`;
    }
    return {
      lineId,
      team,
      market: info?.marketType || '-',
      line: info?.line ?? l.line ?? null,
      sport: info?.sport || 'unknown',
      // Flag legs that blocked us from quoting
      wasUnregistered: unknownSet.has(lineId) || !info,
    };
  });

  let outcome;
  if (weQuoted && ourQuote.status === 'confirmed') {
    outcome = 'won';
    marketStats.weWon++;
    marketStats.weQuoted++;
  } else if (weQuoted) {
    outcome = 'lost'; // we quoted but didn't win
    marketStats.weLost++;
    marketStats.weQuoted++;
    // Update order status to 'lost' so dashboard reflects it
    ourQuote.status = 'lost';
    ourQuote.lostAt = new Date().toISOString();
    ourQuote.winningOdds = matchedOdds != null ? -matchedOdds : null; // negated to match our format
    ourQuote.winningStake = matchedStake;
    // Persist inside meta so we don't lose these on restart (no DB schema change needed)
    ourQuote.meta = ourQuote.meta || {};
    ourQuote.meta.winningOdds = ourQuote.winningOdds;
    ourQuote.meta.winningStake = ourQuote.winningStake;
    ourQuote.meta.lostAt = ourQuote.lostAt;
    db.saveOrder(ourQuote).catch(err => log.error('DB', `saveOrder(outbid) failed: ${err.message}`));
  } else {
    outcome = 'missed';
    marketStats.missedNoQuote++;
  }

  const entry = {
    parlayId,
    matchedOdds,
    matchedStake,
    legs: resolvedLegs,
    matchedAt: new Date().toISOString(),
    weQuoted,
    ourOdds: ourQuote?.offeredOdds || null,
    ourAmericanOdds: ourQuote?.offeredOdds || null,
    ourDecimalOdds: ourQuote?.meta?.decimalOdds || null, // precise decimal for comparison
    // PX sends matched_odds with opposite sign to our format.
    matchedAmericanOdds: matchedOdds != null ? -matchedOdds : null,
    winDecimalOdds: matchedOdds != null ? (Math.abs(matchedOdds) >= 100 ? 1 + Math.abs(matchedOdds)/100 : null) : null,
    outcome,
    legCount: resolvedLegs.length,
    // If we didn't quote, include why (so the dashboard can explain "No quote")
    declineReason: outcome === 'missed' ? (declineInfo?.reason || 'not seen (service down or pre-startup)') : null,
    declineDetail: declineInfo?.declineDetail || null,
    unknownLegDetails: declineInfo?.unknownDetails || [],
  };

  matchedParlays.unshift(entry); // newest first
  db.saveMatchedParlay(entry).catch(() => {});
  if (matchedParlays.length > 5000) matchedParlays.pop(); // cap memory

  if (weQuoted && outcome === 'lost') {
    log.info('Market', `Lost quote: parlay=${parlayId.substring(0,8)}, our=${entry.ourAmericanOdds}, winning=${matchedOdds}, stake=$${matchedStake}`);
  }

  return entry;
}

function decToAm(dec) {
  if (!dec || dec <= 1) return null;
  if (dec >= 2.0) return '+' + Math.round((dec - 1) * 100);
  return '' + Math.round(-100 / (dec - 1));
}

/**
 * Record a declined RFQ with reason.
 */
/**
 * Record a declined RFQ with detailed info.
 * @param {string} reason - 'unknown legs', 'no fair value', 'exposure/limit'
 * @param {object} detail - { legs, knownLegs, unknownLegs, parlayId }
 */
function recordDecline(reason, detail) {
  declineStats.total++;
  const bucket = reason || 'unknown';
  declineStats.reasons[bucket] = (declineStats.reasons[bucket] || 0) + 1;

  // Track potential volume by reason (leg count as proxy for parlay size)
  if (!declineStats.volumeByReason) declineStats.volumeByReason = {};
  if (!declineStats.volumeByReason[bucket]) declineStats.volumeByReason[bucket] = { count: 0, totalLegs: 0, byLegCount: {} };
  const legCount = (detail?.legs || detail?.knownLegs || []).length + (detail?.unknownLegs || []).length;
  declineStats.volumeByReason[bucket].count++;
  declineStats.volumeByReason[bucket].totalLegs += legCount;
  const lcKey = legCount || 'unknown';
  declineStats.volumeByReason[bucket].byLegCount[lcKey] = (declineStats.volumeByReason[bucket].byLegCount[lcKey] || 0) + 1;
  const declinedAt = new Date().toISOString();
  const isLimit = LIMIT_REASONS.has(bucket);

  // Rolling log (keep last 200, newest first)
  declineStats.recent.unshift({
    reason: bucket,
    detail: detail?.declineDetail || null,
    parlayId: detail?.parlayId || null,
    time: declinedAt,
    isLimit,
  });
  if (declineStats.recent.length > 200) declineStats.recent.pop();

  // Index by parlayId so matched-parlay "No quote" rows can explain which leg caused the miss
  if (detail?.parlayId) {
    declinesByParlayId[detail.parlayId] = {
      reason: bucket,
      unknownLineIds: detail.unknownLegs || [],
      unknownDetails: detail.unknownSports || [],
      declineDetail: detail.declineDetail || null,
      declinedAt,
    };
    declineIdOrder.push(detail.parlayId);
    while (declineIdOrder.length > MAX_DECLINE_ENTRIES) {
      const old = declineIdOrder.shift();
      delete declinesByParlayId[old];
    }
  }

  // Persist to Supabase (fire-and-forget)
  db.saveDecline({
    parlayId: detail?.parlayId || null,
    reason: bucket,
    detail: detail?.declineDetail || null,
    knownLegs: detail?.knownLegs || [],
    unknownLineIds: detail?.unknownLegs || [],
    unknownDetails: detail?.unknownSports || [],
    isLimit,
    declinedAt,
  }).catch(() => {});

  // Track sports from unknown legs
  if (detail?.unknownSports) {
    for (const sport of detail.unknownSports) {
      if (!declineStats.unknownSports[sport]) {
        declineStats.unknownSports[sport] = { count: 0, lastSeen: null, recentDeclines: [] };
      }
      declineStats.unknownSports[sport].count++;
      declineStats.unknownSports[sport].lastSeen = new Date().toISOString();
      // Store recent decline detail for this event
      declineStats.unknownSports[sport].recentDeclines.unshift({
        parlayId: detail.parlayId,
        knownLegs: detail.knownLegs || [],
        unknownLegs: detail.unknownLegs || [],
        time: new Date().toISOString(),
        legCount: (detail.legs || []).length,
      });
      if (declineStats.unknownSports[sport].recentDeclines.length > 5) {
        declineStats.unknownSports[sport].recentDeclines.pop();
      }
    }
  }

  // Track near-misses (all legs known but couldn't price)
  // Near-miss reasons include: 'no fair value', 'stale odds', 'parlay too unlikely', 'odds too high'
  const nearMissReasons = new Set(['no fair value', 'stale odds', 'parlay too unlikely', 'odds too high', 'event started']);
  if (nearMissReasons.has(reason) && detail) {
    declineStats.nearMisses.unshift({
      parlayId: detail.parlayId,
      legs: detail.knownLegs || [],
      time: new Date().toISOString(),
      reason,
      detail: detail.declineDetail || null,
    });
    if (declineStats.nearMisses.length > 500) declineStats.nearMisses.pop();
  }
}

function getMarketIntel(limit = 50) {
  return {
    stats: { ...marketStats },
    declines: {
      total: declineStats.total,
      reasons: { ...declineStats.reasons },
      volumeByReason: declineStats.volumeByReason || {},
      unknownSports: { ...declineStats.unknownSports },
      nearMissCount: declineStats.nearMisses.length,
      recentNearMisses: declineStats.nearMisses.slice(0, 500),
    },
    // Volume analysis: matched parlays we missed, with real stake data
    missedVolume: (() => {
      const missed = matchedParlays.filter(m => !m.weQuoted);
      const byReason = {};
      let totalStake = 0;
      for (const m of missed) {
        const reason = m.declineReason || 'not seen';
        if (!byReason[reason]) byReason[reason] = { count: 0, totalStake: 0, avgStake: 0 };
        byReason[reason].count++;
        byReason[reason].totalStake += (m.matchedStake || 0);
        totalStake += (m.matchedStake || 0);
      }
      for (const r of Object.keys(byReason)) {
        byReason[r].avgStake = byReason[r].count > 0 ? Math.round(byReason[r].totalStake * 100) / 100 : 0;
      }
      return { totalMissed: missed.length, totalStake: Math.round(totalStake * 100) / 100, byReason };
    })(),
    recentMatched: matchedParlays.slice(0, limit),
    quoteWinRate: marketStats.weQuoted > 0 ? (marketStats.weWon / marketStats.weQuoted * 100).toFixed(1) + '%' : '-',
    coverageRate: marketStats.totalMatched > 0 ? (marketStats.weQuoted / marketStats.totalMatched * 100).toFixed(1) + '%' : '-',
    // Sport breakdown of matched parlays
    matchedBySport: (() => {
      const bySport = {};
      for (const m of matchedParlays) {
        const knownSports = [...new Set((m.legs || []).map(l => l.sport).filter(s => s && s !== 'unknown'))];
        let bucket;
        if (knownSports.length === 0) bucket = 'Unknown';
        else if (knownSports.length === 1) bucket = knownSports[0];
        else bucket = 'Multi-league';
        if (!bySport[bucket]) bySport[bucket] = { count: 0, weQuoted: 0, missed: 0, avgStake: 0, totalStake: 0 };
        bySport[bucket].count++;
        bySport[bucket].totalStake += (m.matchedStake || 0);
        if (m.weQuoted) bySport[bucket].weQuoted++;
        else bySport[bucket].missed++;
      }
      for (const s of Object.keys(bySport)) {
        bySport[s].avgStake = bySport[s].count > 0 ? bySport[s].totalStake / bySport[s].count : 0;
      }
      return bySport;
    })(),
    // Competitive analysis — compare our quotes to winning prices
    competitive: (() => {
      const quoted = matchedParlays.filter(m => m.weQuoted && m.ourAmericanOdds != null && m.matchedAmericanOdds != null);
      if (quoted.length === 0) return { entries: [], summary: null };

      const entries = quoted.map(m => {
        const ourOdds = Number(m.ourAmericanOdds);
        const winOdds = Number(m.matchedAmericanOdds);
        // Store precise decimal odds for detailed comparison
        const ourDecimal = m.ourDecimalOdds || (ourOdds >= 100 ? 1 + ourOdds/100 : ourOdds < -100 ? 1 + 100/Math.abs(ourOdds) : null);
        const winDecimal = m.winDecimalOdds || (winOdds >= 100 ? 1 + winOdds/100 : winOdds < -100 ? 1 + 100/Math.abs(winOdds) : null);
        // Convert to implied probability for proper comparison
        const ourProb = americanToProb(ourOdds);
        const winProb = americanToProb(winOdds);
        // Gap in probability points — positive means we were tighter (less generous)
        const gapProb = ourProb - winProb;
        // Gap in odds — how many odds points apart
        const won = m.outcome === 'won';
        return {
          parlayId: m.parlayId,
          teams: (m.legs || []).map(l => l.team).filter(t => t !== 'Unknown').join(', '),
          legs: m.legs || [],
          legCount: m.legCount,
          ourOdds,
          winOdds,
          ourDecimal: ourDecimal ? Math.round(ourDecimal * 1000) / 1000 : null,
          winDecimal: winDecimal ? Math.round(winDecimal * 1000) / 1000 : null,
          ourProb: Math.round(ourProb * 10000) / 100,
          winProb: Math.round(winProb * 10000) / 100,
          gapProb: Math.round(gapProb * 10000) / 100,
          won,
          stake: m.matchedStake,
          time: m.matchedAt,
        };
      }).sort((a, b) => (b.time || '').localeCompare(a.time || ''));

      // Summary stats
      const wins = entries.filter(e => e.won);
      const losses = entries.filter(e => !e.won);
      const avgGapWins = wins.length > 0 ? wins.reduce((s, e) => s + e.gapProb, 0) / wins.length : null;
      const avgGapLosses = losses.length > 0 ? losses.reduce((s, e) => s + e.gapProb, 0) / losses.length : null;
      const avgGapAll = entries.reduce((s, e) => s + e.gapProb, 0) / entries.length;

      return {
        entries,
        summary: {
          totalQuoted: entries.length,
          wins: wins.length,
          losses: losses.length,
          avgGapAll: Math.round(avgGapAll * 100) / 100,
          avgGapWins: avgGapWins != null ? Math.round(avgGapWins * 100) / 100 : null,
          avgGapLosses: avgGapLosses != null ? Math.round(avgGapLosses * 100) / 100 : null,
        },
      };
    })(),
  };
}

function americanToProb(odds) {
  odds = Number(odds);
  if (odds >= 100) return 100 / (odds + 100);
  if (odds <= -100) return Math.abs(odds) / (Math.abs(odds) + 100);
  return 0.5;
}

// ---------------------------------------------------------------------------
// LOOKUPS
// ---------------------------------------------------------------------------

function findByParlayId(parlayId) {
  return orders[parlayId] || null;
}

/**
 * Get total portfolio risk — sum of our max payouts across all confirmed orders.
 * This is the naive worst case (all parlays win simultaneously).
 * Our risk per parlay = americanOddsToProfit(odds, confirmedStake)
 * where confirmedStake = bettor's wager and the profit calc gives our payout.
 */
function getTotalPortfolioRisk() {
  let total = 0;
  for (const order of Object.values(orders)) {
    if (order.status !== 'confirmed') continue;
    // confirmedStake IS our SP risk (verified from PX payload). No multiplication.
    total += (order.confirmedStake || 0);
  }
  return total;
}

/**
 * Check if adding a new parlay would exceed the portfolio drawdown limit.
 * @param {number} additionalRisk - payout of the new parlay
 * @param {number} maxDrawdown - max allowed total portfolio risk
 * @returns {{ allowed: boolean, current: number, limit: number }}
 */
function checkPortfolioRisk(additionalRisk, maxDrawdown) {
  if (!maxDrawdown || maxDrawdown <= 0) return { allowed: true, current: 0, limit: 0 };
  const current = getTotalPortfolioRisk();
  if (current + additionalRisk > maxDrawdown) {
    return { allowed: false, current, additional: additionalRisk, limit: maxDrawdown };
  }
  return { allowed: true, current, limit: maxDrawdown };
}

function findByOrderUuid(uuid) {
  const parlayId = ordersByUuid[uuid];
  return parlayId ? orders[parlayId] : null;
}

function getRecentOrders(limit = 200) {
  const all = Object.values(orders);
  // Always include settled and confirmed orders regardless of limit
  const important = all.filter(o => o.status === 'confirmed' || (o.status && o.status.startsWith('settled_')));
  const rest = all.filter(o => o.status !== 'confirmed' && !(o.status && o.status.startsWith('settled_')))
    .sort((a, b) => (b.quotedAt || '').localeCompare(a.quotedAt || ''))
    .slice(0, limit);
  // Merge, deduplicate, sort
  const merged = [...important, ...rest];
  const seen = new Set();
  return merged.filter(o => {
    if (seen.has(o.parlayId)) return false;
    seen.add(o.parlayId);
    return true;
  }).sort((a, b) => (b.quotedAt || '').localeCompare(a.quotedAt || ''));
}

function getStats() {
  return {
    ...stats,
    activeOrders: Object.values(orders).filter(o => o.status === 'confirmed').length,
    openQuotes: Object.values(orders).filter(o => o.status === 'quoted').length,
    totalOrders: Object.keys(orders).length,
  };
}

/**
 * Get P&L summary grouped by sport.
 */
function getPnLBySport() {
  const bySport = {};
  for (const order of Object.values(orders)) {
    if (order.pnl == null) continue;
    const legs = order.meta?.legs || order.legs || [];
    const knownSports = [...new Set(legs.map(l => l.sport).filter(s => s && s !== 'unknown'))];
    let sport;
    if (knownSports.length === 0) sport = 'Unknown';
    else if (knownSports.length === 1) sport = knownSports[0];
    else sport = 'Multi-league';
    if (!bySport[sport]) bySport[sport] = { pnl: 0, count: 0, wins: 0, losses: 0 };
    bySport[sport].pnl += order.pnl;
    bySport[sport].count++;
    if (order.pnl > 0) bySport[sport].wins++;
    else if (order.pnl < 0) bySport[sport].losses++;
  }
  return bySport;
}

// ---------------------------------------------------------------------------
// PROBABILITY-WEIGHTED EXPOSURE
// ---------------------------------------------------------------------------
// For each team in each confirmed parlay, the weighted risk =
// payout × P(all OTHER legs win | this team's leg wins)
//
// Example: 3-leg parlay, Lakers (60%), Celtics (70%), Over (50%), payout $1000
// Lakers risk: $1000 × 0.70 × 0.50 = $350
// Celtics risk: $1000 × 0.60 × 0.50 = $300
// Over risk: $1000 × 0.60 × 0.70 = $420
//
// A 10-leg $2000 payout parlay at 50% each:
// Each team risk: $2000 × 0.50^9 = $3.91 (negligible)

function normalizeExposureKey(name) {
  return (name || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
}

function getLegsForExposure(order) {
  return order.legs || order.meta?.legs || [];
}

/**
 * Effective probability for a leg — prefers live odds if available.
 * liveFairProb is set by refreshLiveOdds for legs of in-progress games.
 * Falls back to the pre-game fairProb, then 0.5 if neither exists.
 */
function legEffectiveProb(leg) {
  if (leg.liveFairProb != null && leg.liveFairProb > 0 && leg.liveFairProb < 1) return leg.liveFairProb;
  if (leg.fairProb != null && leg.fairProb > 0 && leg.fairProb < 1) return leg.fairProb;
  return 0.5;
}

/**
 * Calculate our payout (= My Risk) for an order.
 * PX's confirmedStake IS our payout liability directly.
 */
function getOrderPayout(order) {
  return order.confirmedStake || order.maxRisk || 0;
}

// ---------------------------------------------------------------------------
// NET EXPOSURE MODEL
// ---------------------------------------------------------------------------
// For each game (pxEventId), we store all parlay legs touching that game.
// Net exposure = weighted payouts owed - stakes collected from offsetting
// positions on the OPPOSITE side of the same game.
//
// Example: Parlay 1 has Lakers -6.5 (+250, $1K stake, $2.5K payout)
//          Parlay 2 has Lakers -6.5 (-350, $3.5K stake, $1K payout)
// If Lakers cover: both legs win, weighted payouts = $2.5K×P1other + $1K×P2other
//                  but no opposite stakes to offset
// If Lakers DON'T cover: both parlays lose, we keep $4.5K
//
// Now add Parlay 3 with Celtics +6.5 (opposite side of Lakers game):
// If Lakers cover: Parlay 3 loses → we keep its stake (offset!)
// Net exposure = payouts owed - stakes from opposite side
// ---------------------------------------------------------------------------

/**
 * Add a confirmed order to exposure tracking.
 */
function addExposure(order) {
  const legs = getLegsForExposure(order);
  const payout = getOrderPayout(order); // = confirmedStake = our max risk
  // "stake" in net-exposure model = amount kept when bettor's parlay fails.
  // That's bettor's original wager = our profit on win = americanOddsToProfit(confirmedOdds, confirmedStake).
  const stake = americanOddsToProfit(order.confirmedOdds || 0, order.confirmedStake || 0);
  if (legs.length === 0) return;

  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    const eventId = leg.pxEventId;
    const name = leg.team || leg.teamName || 'unknown';
    const teamKey = normalizeExposureKey(name);
    // Composite key: team + event so the same team on different games (e.g. back-to-back)
    // is tracked as separate rows in the Team Exposure table
    const key = teamKey + '|' + (eventId || 'noevent');

    // Product of all OTHER legs' effective probs (live if available, else pre-game)
    let otherProb = 1;
    for (let j = 0; j < legs.length; j++) {
      if (j === i) continue;
      otherProb *= legEffectiveProb(legs[j]);
    }

    // Game-level tracking (for net exposure calc)
    if (eventId) {
      if (!gameExposure[eventId]) {
        gameExposure[eventId] = {
          name: (leg.awayTeam || '?') + ' @ ' + (leg.homeTeam || '?'),
          sport: leg.sport,
          startTime: leg.startTime,
          parlays: [],
        };
      }
      gameExposure[eventId].parlays.push({
        parlayId: order.parlayId,
        payout,
        stake,
        legCount: legs.length,
        weightedRisk: payout * otherProb,
        selection: leg.selection,
        market: leg.market || leg.marketType,
        teamKey: key,
      });
    }

    // Team-level tracking (for dashboard display) — keyed by team+event
    if (teamKey) {
      if (!exposure[key]) {
        exposure[key] = {
          risk: 0,
          parlays: 0,
          name,
          teamKey,
          eventId: eventId || null,
          eventName: eventId ? ((leg.awayTeam || '?') + ' @ ' + (leg.homeTeam || '?')) : null,
          startTime: leg.startTime || null,
          sport: leg.sport || null,
          notionalPayout: 0,
          netExposure: 0,
        };
      }
      exposure[key].risk += payout * otherProb;
      exposure[key].parlays += 1;
      exposure[key].notionalPayout += payout;
    }
  }

  // Recalculate net exposure for all affected games
  recalcNetExposure();
}

/**
 * Remove a settled order from exposure tracking.
 */
function removeExposure(order) {
  const legs = getLegsForExposure(order);
  const payout = getOrderPayout(order);

  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    const eventId = leg.pxEventId;
    const teamKey = normalizeExposureKey(leg.team || leg.teamName || '');
    const key = teamKey + '|' + (eventId || 'noevent');

    // Remove from game exposure
    if (eventId && gameExposure[eventId]) {
      gameExposure[eventId].parlays = gameExposure[eventId].parlays.filter(
        p => p.parlayId !== order.parlayId
      );
      if (gameExposure[eventId].parlays.length === 0) {
        delete gameExposure[eventId];
      }
    }

    // Remove from team exposure (composite team+event key)
    if (teamKey && exposure[key]) {
      let otherProb = 1;
      for (let j = 0; j < legs.length; j++) {
        if (j === i) continue;
        otherProb *= legEffectiveProb(legs[j]);
      }
      exposure[key].risk -= payout * otherProb;
      exposure[key].parlays -= 1;
      exposure[key].notionalPayout -= payout;
      if (exposure[key].parlays <= 0) delete exposure[key];
    }
  }

  recalcNetExposure();
}

/**
 * Recalculate net exposure for all games.
 * For each game, for each possible "side" (selection), compute:
 *   netExposure = sum(weighted payouts for winning parlays) - sum(stakes from losing parlays on opposite side)
 * Take the worst case across all sides.
 */
function recalcNetExposure() {
  // Reset team net exposure
  for (const key of Object.keys(exposure)) {
    exposure[key].netExposure = 0;
  }

  for (const [eventId, game] of Object.entries(gameExposure)) {
    // Group parlays by selection within this game
    const bySelection = {};
    for (const p of game.parlays) {
      const sel = (p.market || '') + ':' + (p.selection || '');
      if (!bySelection[sel]) bySelection[sel] = [];
      bySelection[sel].push(p);
    }

    // For each selection (e.g., "moneyline:home"), compute net exposure if that side wins
    for (const [sel, winningParlays] of Object.entries(bySelection)) {
      // Weighted payouts we'd owe if this selection wins
      const weightedPayouts = winningParlays.reduce((s, p) => s + p.weightedRisk, 0);

      // Stakes we'd collect from parlays on the OPPOSITE side of this game
      // (their leg loses, so entire parlay loses, we keep their stake)
      let oppositeStakes = 0;
      for (const [otherSel, otherParlays] of Object.entries(bySelection)) {
        if (otherSel === sel) continue; // same side, skip
        // Check if this is truly opposite (same market, different selection)
        const selMarket = sel.split(':')[0];
        const otherMarket = otherSel.split(':')[0];
        if (selMarket === otherMarket) {
          // Same market type, different selection = opposite side
          oppositeStakes += otherParlays.reduce((s, p) => s + p.stake, 0);
        }
      }

      const netExp = Math.max(0, weightedPayouts - oppositeStakes);

      // Attribute net exposure back to teams involved
      for (const p of winningParlays) {
        if (p.teamKey && exposure[p.teamKey]) {
          exposure[p.teamKey].netExposure = Math.max(
            exposure[p.teamKey].netExposure || 0,
            netExp / winningParlays.length // distribute among teams on this side
          );
        }
      }
    }

    // Store net exposure on the game itself
    game.netExposure = 0;
    for (const [sel, winningParlays] of Object.entries(bySelection)) {
      const wp = winningParlays.reduce((s, p) => s + p.weightedRisk, 0);
      let os = 0;
      for (const [otherSel, otherParlays] of Object.entries(bySelection)) {
        if (otherSel === sel) continue;
        const selMarket = sel.split(':')[0];
        const otherMarket = otherSel.split(':')[0];
        if (selMarket === otherMarket) {
          os += otherParlays.reduce((s, p) => s + p.stake, 0);
        }
      }
      game.netExposure = Math.max(game.netExposure, Math.max(0, wp - os));
    }
  }
}

/**
 * Get game exposure snapshot with net exposure, sorted by worst case.
 */
function getGameExposureSnapshot() {
  return Object.entries(gameExposure).map(([eventId, game]) => {
    const grossRisk = game.parlays.reduce((s, p) => s + p.weightedRisk, 0);
    const totalStakes = game.parlays.reduce((s, p) => s + p.stake, 0);
    // Check if ANY leg in ANY parlay contributing to this game uses live odds
    let hasLiveOdds = false;
    for (const p of game.parlays) {
      const order = orders[p.parlayId];
      if (!order) continue;
      const legs = order.legs || order.meta?.legs || [];
      if (legs.some(l => l.liveFairProb != null)) { hasLiveOdds = true; break; }
    }
    return {
      eventId,
      name: game.name,
      sport: game.sport,
      startTime: game.startTime,
      parlayCount: game.parlays.length,
      grossRisk: Math.round(grossRisk * 100) / 100,
      totalStakes: Math.round(totalStakes * 100) / 100,
      netExposure: Math.round((game.netExposure || 0) * 100) / 100,
      worstCase: Math.round((game.netExposure || grossRisk) * 100) / 100,
      hasLiveOdds,
      parlays: game.parlays,
    };
  }).sort((a, b) => b.netExposure - a.netExposure);
}

/**
 * Check if adding a new parlay would exceed per-game NET exposure limits.
 */
function checkGameExposure(legs, estPayout, maxPerGame) {
  if (!maxPerGame || maxPerGame <= 0) return { allowed: true };

  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    const eventId = leg.lineInfo?.pxEventId || leg.pxEventId;
    if (!eventId) continue;

    let otherProb = 1;
    for (let j = 0; j < legs.length; j++) {
      if (j === i) continue;
      otherProb *= (legs[j].lineInfo?.fairProb || legs[j].fairProb || 0.5);
    }
    const newWeightedRisk = estPayout * otherProb;

    // Current net exposure for this game
    const currentNet = gameExposure[eventId]?.netExposure || 0;

    // Conservative: add full weighted risk (worst case is no offsetting)
    if (currentNet + newWeightedRisk > maxPerGame) {
      const gameName = gameExposure[eventId]?.name || eventId;
      return {
        allowed: false,
        reason: `Game "${gameName}" net exposure $${Math.round(currentNet)} + $${Math.round(newWeightedRisk)} > max $${Math.round(maxPerGame)}`,
      };
    }
  }
  return { allowed: true };
}

/**
 * Check if adding a parlay would exceed per-team NET exposure limits.
 */
function checkExposureLimits(legs, payout, maxNetExposure) {
  if (!maxNetExposure || maxNetExposure <= 0) {
    return { allowed: true, reason: null, violations: [] };
  }

  const violations = [];
  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    const name = leg.team || leg.teamName || leg.lineInfo?.teamName || 'unknown';
    const teamKey = normalizeExposureKey(name);
    if (!teamKey) continue;
    const eventId = leg.lineInfo?.pxEventId || leg.pxEventId;
    const key = teamKey + '|' + (eventId || 'noevent');

    let otherProb = 1;
    for (let j = 0; j < legs.length; j++) {
      if (j === i) continue;
      otherProb *= (legs[j].fairProb || legs[j].lineInfo?.fairProb || 0.5);
    }

    const newRisk = payout * otherProb;
    const currentNet = exposure[key]?.netExposure || 0;
    const afterAdd = currentNet + newRisk;

    if (afterAdd > maxNetExposure) {
      violations.push({
        team: name,
        currentExposure: Math.round(currentNet * 100) / 100,
        newRisk: Math.round(newRisk * 100) / 100,
        wouldBe: Math.round(afterAdd * 100) / 100,
        limit: maxNetExposure,
      });
    }
  }

  if (violations.length > 0) {
    const names = violations.map(v => `${v.team} ($${v.wouldBe}/$${v.limit})`).join(', ');
    return {
      allowed: false,
      reason: `Net exposure limit exceeded: ${names}`,
      violations,
    };
  }
  return { allowed: true, reason: null, violations: [] };
}

function getExposureForTeam(teamName) {
  const key = normalizeExposureKey(teamName);
  return exposure[key] || null;
}

/**
 * Get full exposure snapshot — all teams with net exposure.
 */
function getExposureSnapshot() {
  return Object.entries(exposure)
    .map(([key, val]) => ({
      key,
      ...val,
      risk: Math.round(val.risk * 100) / 100,
      netExposure: Math.round((val.netExposure || 0) * 100) / 100,
      notionalPayout: Math.round((val.notionalPayout || 0) * 100) / 100,
    }))
    .sort((a, b) => (b.netExposure || b.risk) - (a.netExposure || a.risk));
}

/**
 * Poll PX orders API for settlement updates.
 * Catches settlements that WebSocket events may have missed.
 */
/**
 * Clear all exposure state and rebuild from scratch by re-adding every
 * confirmed order. Picks up any updated liveFairProb values on legs.
 */
function rebuildAllExposure() {
  // Clear state
  for (const k of Object.keys(exposure)) delete exposure[k];
  for (const k of Object.keys(gameExposure)) delete gameExposure[k];
  // Re-add all confirmed orders
  for (const order of Object.values(orders)) {
    if (order.status === 'confirmed') {
      addExposure(order);
    }
  }
}

/**
 * Refresh live odds for in-progress games and update weighted risk
 * calculations. Pulls live odds from the odds feed (live=true) for each
 * sport that has any in-progress game in a confirmed parlay, updates
 * leg.liveFairProb on affected legs, then rebuilds exposure.
 *
 * Returns a summary: { sportsRefreshed, legsUpdated, inProgressGames }.
 */
async function refreshLiveOdds(oddsFeed) {
  const now = Date.now();
  // Find all in-progress legs across confirmed orders
  const inProgressLegsBySport = {}; // sport -> list of { order, leg }
  let inProgressGames = 0;
  const seenEvents = new Set();
  for (const order of Object.values(orders)) {
    if (order.status !== 'confirmed') continue;
    const legs = order.legs || order.meta?.legs || [];
    for (const leg of legs) {
      const startMs = leg.startTime ? new Date(leg.startTime).getTime() : null;
      if (!startMs || isNaN(startMs)) continue;
      if (startMs > now) continue; // not started yet
      // Skip legs where game is likely over (>4h since start)
      if (now - startMs > 4 * 60 * 60 * 1000) continue;
      const sport = leg.sport || leg.oddsApiSport;
      if (!sport) continue;
      if (!inProgressLegsBySport[sport]) inProgressLegsBySport[sport] = [];
      inProgressLegsBySport[sport].push({ order, leg });
      const eventKey = sport + '|' + (leg.pxEventId || (leg.homeTeam + '@' + leg.awayTeam));
      if (!seenEvents.has(eventKey)) { seenEvents.add(eventKey); inProgressGames++; }
    }
  }

  const sports = Object.keys(inProgressLegsBySport);
  if (sports.length === 0) {
    return { sportsRefreshed: 0, legsUpdated: 0, inProgressGames: 0 };
  }

  // Fetch live odds for each sport with in-progress games
  let sportsRefreshed = 0;
  for (const sport of sports) {
    try {
      const result = await oddsFeed.fetchOddsForSport(sport, { live: true });
      if (result != null) sportsRefreshed++;
    } catch (err) {
      log.warn('LiveOdds', `Failed to fetch live odds for ${sport}: ${err.message}`);
    }
  }

  // Update liveFairProb on each in-progress leg
  let legsUpdated = 0;
  for (const [sport, legRecords] of Object.entries(inProgressLegsBySport)) {
    for (const { leg } of legRecords) {
      const prob = oddsFeed.getLiveFairProb(
        leg.oddsApiSport || sport,
        leg.homeTeam,
        leg.awayTeam,
        leg.oddsApiMarket || leg.market || leg.marketType,
        leg.oddsApiSelection || leg.selection,
        leg.line != null ? Math.abs(leg.line) : null,
        leg.startTime
      );
      if (prob != null && prob > 0 && prob < 1) {
        leg.liveFairProb = prob;
        leg.liveFairProbUpdatedAt = new Date().toISOString();
        legsUpdated++;
      }
    }
  }

  // Rebuild exposure with new probs (if any legs were updated)
  if (legsUpdated > 0) {
    rebuildAllExposure();
  }

  log.info('LiveOdds', `Refreshed ${sportsRefreshed}/${sports.length} sports, updated ${legsUpdated} legs across ${inProgressGames} in-progress games`);
  return { sportsRefreshed, legsUpdated, inProgressGames, sportsWithInProgress: sports };
}

async function pollOrderSettlements(px) {
  const confirmed = Object.values(orders).filter(o => o.status === 'confirmed' && o.orderUuid);
  if (confirmed.length === 0) {
    log.debug('Poll', 'No confirmed orders to check');
    return { checked: 0, settled: 0 };
  }

  log.info('Poll', `Checking ${confirmed.length} confirmed orders for settlement...`);

  try {
    // Fetch all our orders from PX (high limit to catch older settlements)
    const pxOrders = await px.fetchOrders(500);
    let settled = 0;

    for (const pxOrder of pxOrders) {
      const uuid = pxOrder.order_uuid;
      if (!uuid) continue;

      // Try UUID index first, then fallback to parlay_id match
      let parlayId = ordersByUuid[uuid];
      let order = parlayId ? orders[parlayId] : null;

      // Fallback: match by parlay_id. PX uses `p_id` as the canonical field name.
      const pxParlayId = pxOrder.p_id || pxOrder.parlay_id;
      if (!order && pxParlayId && orders[pxParlayId]) {
        order = orders[pxParlayId];
        parlayId = pxParlayId;
        // Backfill the UUID so future lookups work
        order.orderUuid = uuid;
        ordersByUuid[uuid] = parlayId;
        log.info('Poll', `Backfilled UUID for parlay ${parlayId}: ${uuid}`);
        db.saveOrder(order).catch(() => {});
      }

      // If still no match: reconstruct the order from PX data so P&L is captured.
      // This handles cases where we missed the confirmation WS event entirely
      // (e.g., service was down) but PX knows about the settled order.
      // Disabled when SKIP_RECONSTRUCTION=true (production clean start).
      if (!order && pxParlayId) {
        if (process.env.SKIP_RECONSTRUCTION === 'true' || process.env.SKIP_RECONSTRUCTION === '1') {
          continue; // skip all reconstruction
        }
        const settlementStatus = pxOrder.settlement_status;
        if (settlementStatus && !['tbd','requested'].includes(settlementStatus)) {
          log.info('Poll', `Reconstructing missing settled order ${pxParlayId} (uuid=${uuid})`);
          // Enrich legs from lineManager where possible
          const lineManager = require('./line-manager');
          const enrichedLegs = (pxOrder.legs || []).map(l => {
            const info = lineManager.lookupLine(l.line_id);
            const eventName = l.sport_event_id ? lineManager.getEventName(l.sport_event_id) : null;
            let team = info?.teamName || '?';
            // If we only have the event name, use that as team context for totals/spreads
            if (!info && eventName) team = eventName;
            // Totals display prefix (over/under) when we know the market type
            if (info?.marketType === 'total' && info?.homeTeam && info?.awayTeam) {
              team = `${team} (${info.awayTeam} @ ${info.homeTeam})`;
            }
            return {
              lineId: l.line_id,
              sport: info?.sport || 'unknown',
              team,
              teamName: info?.teamName || team,
              market: info?.marketType || null,
              marketType: info?.marketType || null,
              line: l.line,
              selection: info?.selection || null,
              homeTeam: info?.homeTeam || null,
              awayTeam: info?.awayTeam || null,
              pxEventId: l.sport_event_id,
              pxEventName: eventName || null,
              startTime: info?.startTime || null,
              settlementStatus: l.settlement_status,
              settlement_status: l.settlement_status,
            };
          });
          order = {
            parlayId: pxParlayId,
            status: 'confirmed', // will be set to settled_* below by recordSettlement
            legs: enrichedLegs,
            offeredOdds: pxOrder.confirmed_odds != null ? -pxOrder.confirmed_odds : null,
            fairParlayProb: null,
            maxRisk: null,
            vig: null,
            confirmedOdds: pxOrder.confirmed_odds != null ? Number(pxOrder.confirmed_odds) : null,
            confirmedStake: pxOrder.confirmed_stake != null ? Number(pxOrder.confirmed_stake) : null,
            orderUuid: uuid,
            quotedAt: null,
            confirmedAt: new Date((pxOrder.updated_at || 0) * 1000).toISOString(),
            settledAt: null,
            pnl: null,
            settlementResult: null,
            meta: { reconstructed: true, legs: enrichedLegs },
          };
          orders[pxParlayId] = order;
          ordersByUuid[uuid] = pxParlayId;
          parlayId = pxParlayId;
          // Persist to DB
          db.saveOrder(order).catch(err => log.error('DB', `saveOrder(reconstructed) failed: ${err.message}`));
        }
      }

      if (!order) continue;
      // Skip if already settled with matching status
      // Skip if already settled — compare against SP-perspective status
      const pxSpResult = pxOrder.settlement_status === 'won' ? 'lost'
                       : pxOrder.settlement_status === 'lost' ? 'won'
                       : pxOrder.settlement_status;
      if (order.status === `settled_${pxSpResult}`) continue;

      const settlementStatus = pxOrder.settlement_status;
      if (!settlementStatus || settlementStatus === 'tbd' || settlementStatus === 'requested') continue;

      // If order was somehow reverted to non-settled status, fix it
      // by temporarily clearing the settled status so recordSettlement will run
      if (order.status && order.status.startsWith('settled_')) {
        log.warn('Orders', `Fixing stale settlement for ${order.parlayId}: was ${order.status}, PX says ${settlementStatus}`);
        // Reverse the old P&L before re-settling
        if (order.pnl != null) stats.runningPnL -= order.pnl;
        if (order.pnl > 0) stats.totalWins--;
        else if (order.pnl < 0) stats.totalLosses--;
        stats.totalSettlements--;
        order.status = 'confirmed';
        order.pnl = null;
      }

      // Backfill stake/odds from PX if missing (critical for recovering lost settlements)
      if (!order.confirmedStake && pxOrder.stake != null) order.confirmedStake = Number(pxOrder.stake);
      if (!order.confirmedOdds && pxOrder.odds != null) order.confirmedOdds = Number(pxOrder.odds);
      if (!order.confirmedStake && pxOrder.confirmed_stake != null) order.confirmedStake = Number(pxOrder.confirmed_stake);
      if (!order.confirmedOdds && pxOrder.confirmed_odds != null) order.confirmedOdds = Number(pxOrder.confirmed_odds);

      // PX REST API settlement_status is BETTOR-perspective:
      //   'won'  = bettor's parlay won  = SP LOST
      //   'lost' = bettor's parlay lost = SP WON
      // Flip to SP-perspective before calling recordSettlement.
      const spResult = settlementStatus === 'won' ? 'lost'
                     : settlementStatus === 'lost' ? 'won'
                     : settlementStatus; // push/void stay as-is
      recordSettlement(uuid, spResult, pxOrder.profit || 0);
      settled++;

      // Update per-leg settlement from PX response
      if (pxOrder.legs && Array.isArray(pxOrder.legs)) {
        for (const pxLeg of pxOrder.legs) {
          if (pxLeg.line_id && pxLeg.settlement_status) {
            recordLegSettlement(uuid, pxLeg);
          }
        }
      }
    }

    log.info('Poll', `Settlement poll: checked ${pxOrders.length} PX orders, settled ${settled}`);
    return { checked: pxOrders.length, settled };
  } catch (err) {
    log.error('Poll', `Settlement poll failed: ${err.message}`);
    return { checked: 0, settled: 0, error: err.message };
  }
}

/**
 * Delete settled orders whose legs are all unresolved ('?' team names).
 * Removes from in-memory store, reverses P&L stats, and deletes from DB.
 */
async function deleteUnknownSettledOrders() {
  let deleted = 0;
  const parlayIds = [];
  for (const [parlayId, order] of Object.entries(orders)) {
    if (!order.status?.startsWith('settled_')) continue;
    const legs = order.legs || order.meta?.legs || [];
    const allUnknown = legs.length > 0 && legs.every(l => !l.team || l.team === '?' || l.team === 'unknown');
    if (!allUnknown) continue;

    // Reverse stats
    if (order.pnl != null) {
      stats.runningPnL -= order.pnl;
      if (order.pnl > 0) stats.totalWins--;
      else if (order.pnl < 0) stats.totalLosses--;
    }
    stats.totalSettlements--;

    // Remove from in-memory stores
    if (order.orderUuid) delete ordersByUuid[order.orderUuid];
    delete orders[parlayId];
    parlayIds.push(parlayId);
    deleted++;
  }

  // Delete from Supabase
  if (parlayIds.length > 0) {
    const supabase = db.getClient();
    if (supabase) {
      for (const pid of parlayIds) {
        const { error } = await supabase.from('parlay_orders').delete().eq('parlay_id', pid);
        if (error) log.error('DB', `Failed to delete ${pid}: ${error.message}`);
      }
    }
  }

  log.info('Orders', `Deleted ${deleted} unknown settled orders (P&L now: $${stats.runningPnL.toFixed(2)})`);
  return { deleted, parlayIds };
}

module.exports = {
  recordQuote,
  recordConfirmation,
  recordRejection,
  recordFinalized,
  recordSettlement,
  recordLegSettlement,
  pollOrderSettlements,
  deleteUnknownSettledOrders,
  findByParlayId,
  findByOrderUuid,
  getTotalPortfolioRisk,
  checkPortfolioRisk,
  getGameExposureSnapshot,
  checkGameExposure,
  getRecentOrders,
  getStats,
  getPnLBySport,
  getExposureForTeam,
  checkExposureLimits,
  getExposureSnapshot,
  recordMatchedParlay,
  recordDecline,
  getMarketIntel,
  getAlerts,
  refreshLiveOdds,
  rebuildAllExposure,
  enrichReconstructedOrders,
  enrichReconstructedFromPx,
  loadFromDb,
};

/**
 * Walk all in-memory orders, find ones flagged meta.reconstructed=true with
 * missing team names, and enrich them from the current lineManager index.
 * Persists enriched versions back to DB.
 */
function enrichReconstructedOrders() {
  const lineManager = require('./line-manager');
  let enriched = 0;
  let scanned = 0;
  for (const order of Object.values(orders)) {
    const legs = order.legs || order.meta?.legs || [];
    // Check if any leg has '?' or null team (reconstructed signature)
    const needsEnrichment = legs.some(l => !l.team || l.team === '?' || l.team === 'unknown');
    if (!needsEnrichment) continue;
    scanned++;
    let changed = false;
    const newLegs = legs.map(l => {
      const info = l.lineId ? lineManager.lookupLine(l.lineId) : null;
      const eventName = l.pxEventId ? lineManager.getEventName(l.pxEventId) : null;
      if (!info && !eventName) return l; // nothing new to add
      let team = info?.teamName || l.team;
      if (!info && eventName) team = eventName;
      if (info?.marketType === 'total' && info?.homeTeam && info?.awayTeam) {
        team = `${team} (${info.awayTeam} @ ${info.homeTeam})`;
      }
      if (team && team !== '?' && team !== l.team) changed = true;
      return {
        ...l,
        team: team || l.team,
        teamName: info?.teamName || l.teamName || team,
        sport: info?.sport || l.sport || 'unknown',
        market: info?.marketType || l.market,
        marketType: info?.marketType || l.marketType,
        selection: info?.selection || l.selection,
        homeTeam: info?.homeTeam || l.homeTeam,
        awayTeam: info?.awayTeam || l.awayTeam,
        pxEventName: eventName || l.pxEventName,
        startTime: info?.startTime || l.startTime,
      };
    });
    if (changed) {
      order.legs = newLegs;
      if (order.meta) order.meta.legs = newLegs;
      enriched++;
      db.saveOrder(order).catch(() => {});
    }
  }
  log.info('Orders', `Enrichment: scanned ${scanned} orders with unresolved legs, enriched ${enriched}`);
  return { scanned, enriched };
}

/**
 * Deep-enrich reconstructed orders by fetching historical markets from PX for
 * each unique pxEventId referenced by unresolved legs. Writes team/market/line
 * info directly onto order legs and persists to DB. Does NOT register lines
 * back into lineManager (these events are historical and should not affect
 * live quoting).
 */
async function enrichReconstructedFromPx() {
  const px = require('./prophetx');
  let scanned = 0, enriched = 0, eventsFetched = 0, eventsFailed = 0;

  // Collect unique pxEventIds from orders that need enrichment
  const eventIdToOrders = new Map(); // eventId -> Set of parlayIds
  for (const order of Object.values(orders)) {
    const legs = order.legs || order.meta?.legs || [];
    const needsEnrichment = legs.some(l => !l.team || l.team === '?' || l.team === 'unknown');
    if (!needsEnrichment) continue;
    scanned++;
    for (const l of legs) {
      const eid = l.pxEventId || l.sport_event_id;
      if (!eid) continue;
      if (!eventIdToOrders.has(eid)) eventIdToOrders.set(eid, new Set());
      eventIdToOrders.get(eid).add(order.parlayId);
    }
  }

  // Fetch markets for each unique event and build a lineId -> {team, market, line, ...} map
  const lineIdInfo = {}; // lineId -> { teamName, marketType, line, ... }
  const eventNames = {}; // eventId -> name (best-effort)
  for (const [eventId, parlayIds] of eventIdToOrders.entries()) {
    try {
      const markets = await px.fetchMarkets(eventId);
      eventsFetched++;
      for (const market of markets || []) {
        if (!['moneyline', 'spread', 'total'].includes(market.type)) continue;
        if (market.event_name && !eventNames[eventId]) eventNames[eventId] = market.event_name;
        const parsed = px.parseMarketSelections(market);
        for (const sel of parsed) {
          if (!sel.lineId) continue;
          lineIdInfo[sel.lineId] = {
            teamName: sel.teamName,
            marketType: sel.marketType,
            selection: sel.selection,
            line: sel.line,
            competitorId: sel.competitorId,
          };
        }
      }
    } catch (err) {
      eventsFailed++;
      log.debug('Orders', `enrichReconstructedFromPx: fetchMarkets(${eventId}) failed: ${err.message}`);
    }
  }

  // Apply enrichment
  for (const order of Object.values(orders)) {
    const legs = order.legs || order.meta?.legs || [];
    const needsEnrichment = legs.some(l => !l.team || l.team === '?' || l.team === 'unknown');
    if (!needsEnrichment) continue;
    let changed = false;
    const newLegs = legs.map(l => {
      const info = l.lineId ? lineIdInfo[l.lineId] : null;
      if (!info) return l;
      const team = info.teamName || l.team;
      if (team && team !== '?' && team !== l.team) changed = true;
      return {
        ...l,
        team: team || l.team,
        teamName: info.teamName || l.teamName,
        market: info.marketType || l.market,
        marketType: info.marketType || l.marketType,
        selection: info.selection || l.selection,
        line: info.line != null ? info.line : l.line,
        pxEventName: eventNames[l.pxEventId] || l.pxEventName,
      };
    });
    if (changed) {
      order.legs = newLegs;
      if (order.meta) order.meta.legs = newLegs;
      enriched++;
      db.saveOrder(order).catch(() => {});
    }
  }

  log.info('Orders', `Deep enrichment: ${scanned} orders scanned, ${eventsFetched} events fetched (${eventsFailed} failed), ${enriched} orders enriched`);
  return { scanned, enriched, eventsFetched, eventsFailed, uniqueEvents: eventIdToOrders.size };
}

/**
 * Return alert-relevant data: counts of limit-based declines + rejections
 * over the last 15 minutes. Used by the dashboard to show a warning banner
 * when the SP is missing quotes because it's hitting its own risk limits.
 */
function getAlerts() {
  const now = Date.now();
  const windowMs = 15 * 60 * 1000;
  const cutoff = now - windowMs;

  // Recent limit-hit declines (within window)
  const recentLimitDeclines = declineStats.recent.filter(d => {
    if (!d.isLimit) return false;
    const t = new Date(d.time).getTime();
    return !isNaN(t) && t >= cutoff;
  });

  // Recent rejections (all reasons — these are limit hits at confirm time)
  const recentRejects = rejectStats.recent.filter(r => {
    const t = new Date(r.time).getTime();
    return !isNaN(t) && t >= cutoff;
  });

  // Group declines by reason
  const byReason = {};
  for (const d of recentLimitDeclines) {
    if (!byReason[d.reason]) byReason[d.reason] = { count: 0, lastDetail: null, lastTime: null };
    byReason[d.reason].count++;
    if (!byReason[d.reason].lastDetail) byReason[d.reason].lastDetail = d.detail;
    if (!byReason[d.reason].lastTime) byReason[d.reason].lastTime = d.time;
  }

  // Group rejections by bucket
  const rejectByBucket = {};
  for (const r of recentRejects) {
    if (!rejectByBucket[r.bucket]) rejectByBucket[r.bucket] = { count: 0, lastReason: null, lastTime: null };
    rejectByBucket[r.bucket].count++;
    if (!rejectByBucket[r.bucket].lastReason) rejectByBucket[r.bucket].lastReason = r.reason;
    if (!rejectByBucket[r.bucket].lastTime) rejectByBucket[r.bucket].lastTime = r.time;
  }

  return {
    windowMinutes: 15,
    limitDeclineCount: recentLimitDeclines.length,
    rejectCount: recentRejects.length,
    declineByReason: byReason,
    rejectByBucket,
    allTimeLimitDeclines: declineStats.recent.filter(d => d.isLimit).length,
    allTimeRejects: rejectStats.total,
  };
}

/**
 * Load historical data from Supabase on startup.
 * Restores orders and matched parlays into in-memory stores.
 */
async function loadFromDb() {
  if (!db.isEnabled()) {
    log.info('DB', 'Supabase not configured — running in memory-only mode');
    return;
  }

  log.info('DB', 'Loading historical data from Supabase...');

  // Load orders (very high limit to pull ALL history; loadOrders paginates)
  const dbOrders = await db.loadOrders(20000);
  for (const o of dbOrders) {
    // Hoist winning-quote info out of meta (stored there to avoid DB schema change)
    if (o.meta) {
      if (o.meta.winningOdds != null && o.winningOdds == null) o.winningOdds = o.meta.winningOdds;
      if (o.meta.winningStake != null && o.winningStake == null) o.winningStake = o.meta.winningStake;
      if (o.meta.lostAt && !o.lostAt) o.lostAt = o.meta.lostAt;
    }
    orders[o.parlayId] = o;
    if (o.orderUuid) ordersByUuid[o.orderUuid] = o.parlayId;

    // Restore stats
    stats.totalQuotes++;
    if (o.status === 'confirmed') stats.totalConfirmations++;
    if (o.status === 'rejected') stats.totalRejections++;
    if (o.status?.startsWith('settled_')) {
      stats.totalSettlements++;

      // Determine correct SP result from LEG-LEVEL settlement data.
      // Leg settlement_status is bettor-perspective:
      //   'won' = bettor's selection hit, 'lost' = bettor's selection missed
      // If ANY leg has 'lost' → bettor's parlay lost → SP WON
      // If ALL legs have 'won' → bettor's parlay hit → SP LOST
      // This is idempotent — doesn't depend on how status was previously stored.
      const legs = o.legs || o.meta?.legs || [];
      const legStatuses = legs.map(l => l.settlementStatus || l.settlement_status).filter(Boolean);

      let spResult;
      if (legStatuses.length > 0) {
        // We have leg data — derive SP result definitively
        const anyLegLost = legStatuses.some(s => s === 'lost');
        const anyPush = legStatuses.some(s => s === 'push' || s === 'void');
        if (anyLegLost) {
          spResult = 'won'; // bettor's parlay missed ≥1 leg → SP won
        } else if (anyPush && legStatuses.every(s => s === 'won' || s === 'push' || s === 'void')) {
          spResult = 'push';
        } else {
          spResult = 'lost'; // all legs hit → bettor won → SP lost
        }
      } else {
        // No leg data — use stored status but mark as needing verification
        // Can't determine direction without legs, keep whatever is stored
        spResult = o.status.replace('settled_', '');
      }

      o.status = `settled_${spResult}`;
      o.settlementResult = spResult;

      // Recalculate P&L on load. Status is SP-perspective:
      //   settled_won  = SP won (bettor's parlay lost) → +bettor's wager
      //   settled_lost = SP lost (bettor's parlay won) → -confirmedStake
      const bettorWager = americanOddsToProfit(o.confirmedOdds, o.confirmedStake);
      if (spResult === 'won') {
        o.pnl = bettorWager;              // SP won
      } else if (spResult === 'lost') {
        o.pnl = -(o.confirmedStake || 0); // SP lost
      } else {
        o.pnl = 0;
      }
      db.saveOrder(o).catch(() => {}); // persist corrected status + P&L
      if (o.pnl != null) {
        stats.runningPnL += o.pnl;
        if (o.pnl > 0) stats.totalWins++;
        else if (o.pnl < 0) stats.totalLosses++;
      }
    }

    // Restore exposure for confirmed (unsettled) orders
    if (o.status === 'confirmed') {
      addExposure(o);
    }
  }
  log.info('DB', `Loaded ${dbOrders.length} orders (P&L: $${stats.runningPnL.toFixed(2)})`);

  // Load matched parlays (paginated in loadMatchedParlays)
  const dbMatched = await db.loadMatchedParlays(10000);
  for (const m of dbMatched) {
    matchedParlays.push(m);
    marketStats.totalMatched++;
    if (m.weQuoted) {
      marketStats.weQuoted++;
      if (m.outcome === 'won') marketStats.weWon++;
      else if (m.outcome === 'lost') marketStats.weLost++;
    } else {
      marketStats.missedNoQuote++;
    }
  }
  log.info('DB', `Loaded ${dbMatched.length} matched parlays`);

  // Load declines (restores declineStats, declinesByParlayId, nearMisses)
  const dbDeclines = await db.loadDeclines(2000);
  const nearMissReasons = new Set(['no fair value', 'stale odds', 'parlay too unlikely', 'odds too high', 'event started']);
  // dbDeclines are newest-first; iterate oldest-first to rebuild counters
  for (let i = dbDeclines.length - 1; i >= 0; i--) {
    const d = dbDeclines[i];
    declineStats.total++;
    declineStats.reasons[d.reason] = (declineStats.reasons[d.reason] || 0) + 1;
    declineStats.recent.unshift({
      reason: d.reason,
      detail: d.detail,
      parlayId: d.parlayId,
      time: d.declinedAt,
      isLimit: d.isLimit,
    });
    if (declineStats.recent.length > 200) declineStats.recent.pop();

    if (d.parlayId) {
      declinesByParlayId[d.parlayId] = {
        reason: d.reason,
        unknownLineIds: d.unknownLineIds || [],
        unknownDetails: d.unknownDetails || [],
        declineDetail: d.detail,
        declinedAt: d.declinedAt,
      };
      declineIdOrder.push(d.parlayId);
      while (declineIdOrder.length > MAX_DECLINE_ENTRIES) {
        const old = declineIdOrder.shift();
        delete declinesByParlayId[old];
      }
    }

    // Reconstruct unknownSports aggregations
    for (const ud of (d.unknownDetails || [])) {
      if (!declineStats.unknownSports[ud]) {
        declineStats.unknownSports[ud] = { count: 0, lastSeen: null, recentDeclines: [] };
      }
      declineStats.unknownSports[ud].count++;
      declineStats.unknownSports[ud].lastSeen = d.declinedAt;
      declineStats.unknownSports[ud].recentDeclines.unshift({
        parlayId: d.parlayId,
        knownLegs: d.knownLegs || [],
        unknownLegs: d.unknownLineIds || [],
        time: d.declinedAt,
        legCount: (d.knownLegs || []).length + (d.unknownLineIds || []).length,
      });
      if (declineStats.unknownSports[ud].recentDeclines.length > 5) {
        declineStats.unknownSports[ud].recentDeclines.pop();
      }
    }

    // Near misses
    if (nearMissReasons.has(d.reason)) {
      declineStats.nearMisses.unshift({
        parlayId: d.parlayId,
        legs: d.knownLegs || [],
        time: d.declinedAt,
        reason: d.reason,
        detail: d.detail,
      });
      if (declineStats.nearMisses.length > 500) declineStats.nearMisses.pop();
    }
  }
  log.info('DB', `Loaded ${dbDeclines.length} declines`);
}
