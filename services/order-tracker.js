const log = require('./logger');

// ---------------------------------------------------------------------------
// IN-MEMORY ORDER STORE
// ---------------------------------------------------------------------------

const orders = {}; // keyed by parlayId
const ordersByUuid = {}; // secondary index: orderUuid → parlayId

// ---------------------------------------------------------------------------
// EXPOSURE TRACKING — team/selection level risk across confirmed parlays
// ---------------------------------------------------------------------------
// { 'normalized_team_or_selection': { risk: totalMaxRisk, parlays: count, name: displayName } }
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
  return orders[parlayId];
}

function recordConfirmation(parlayId, orderUuid, confirmedOdds, confirmedStake) {
  stats.totalConfirmations++;

  const order = orders[parlayId];
  if (order) {
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
  } else {
    log.warn('Orders', `Confirmation for unknown parlay ${parlayId}`);
  }
  return order;
}

function recordRejection(parlayId, reason) {
  stats.totalRejections++;

  const order = orders[parlayId];
  if (order) {
    order.status = 'rejected';
    order.rejectedAt = new Date().toISOString();
    order.rejectionReason = reason;
    log.info('Orders', `Rejected: parlay=${parlayId}, reason=${reason}`);
  }
  return order;
}

function recordSettlement(orderUuid, result, payout) {
  stats.totalSettlements++;

  const parlayId = ordersByUuid[orderUuid];
  const order = parlayId ? orders[parlayId] : null;

  if (order) {
    order.settledAt = new Date().toISOString();
    order.settlementResult = result; // 'won', 'lost', 'push', 'void'

    // Calculate P&L from SP perspective (house side)
    // If parlay wins (bettor wins): we lose payout - stake collected
    // If parlay loses (bettor loses): we win the stake
    if (result === 'won') {
      // Bettor won — we pay out
      const payoutAmount = order.confirmedStake * (order.confirmedOdds - 1);
      order.pnl = -payoutAmount;
      stats.totalLosses++;
    } else if (result === 'lost') {
      // Bettor lost — we keep the stake
      order.pnl = order.confirmedStake;
      stats.totalWins++;
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
  } else {
    log.warn('Orders', `Settlement for unknown order ${orderUuid}`);
  }

  return order;
}

// ---------------------------------------------------------------------------
// LOOKUPS
// ---------------------------------------------------------------------------

function findByParlayId(parlayId) {
  return orders[parlayId] || null;
}

function findByOrderUuid(uuid) {
  const parlayId = ordersByUuid[uuid];
  return parlayId ? orders[parlayId] : null;
}

function getRecentOrders(limit = 20) {
  return Object.values(orders)
    .sort((a, b) => (b.quotedAt || '').localeCompare(a.quotedAt || ''))
    .slice(0, limit);
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
    const sport = order.meta?.legs?.[0]?.sport || 'unknown';
    if (!bySport[sport]) bySport[sport] = { pnl: 0, count: 0, wins: 0, losses: 0 };
    bySport[sport].pnl += order.pnl;
    bySport[sport].count++;
    if (order.pnl > 0) bySport[sport].wins++;
    else if (order.pnl < 0) bySport[sport].losses++;
  }
  return bySport;
}

// ---------------------------------------------------------------------------
// EXPOSURE HELPERS
// ---------------------------------------------------------------------------

function normalizeExposureKey(name) {
  return (name || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
}

function getLegsForExposure(order) {
  return order.legs || order.meta?.legs || [];
}

function addExposure(order) {
  const legs = getLegsForExposure(order);
  const risk = order.maxRisk || 0;
  for (const leg of legs) {
    const name = leg.team || leg.teamName || 'unknown';
    const key = normalizeExposureKey(name);
    if (!key) continue;
    if (!exposure[key]) {
      exposure[key] = { risk: 0, parlays: 0, name };
    }
    exposure[key].risk += risk;
    exposure[key].parlays += 1;
  }
}

function removeExposure(order) {
  const legs = getLegsForExposure(order);
  const risk = order.maxRisk || 0;
  for (const leg of legs) {
    const name = leg.team || leg.teamName || 'unknown';
    const key = normalizeExposureKey(name);
    if (!key || !exposure[key]) continue;
    exposure[key].risk -= risk;
    exposure[key].parlays -= 1;
    if (exposure[key].parlays <= 0) {
      delete exposure[key];
    }
  }
}

/**
 * Get current exposure for a specific team/selection.
 * Returns { risk, parlays, name } or null.
 */
function getExposureForTeam(teamName) {
  const key = normalizeExposureKey(teamName);
  return exposure[key] || null;
}

/**
 * Check if adding a parlay with these legs would exceed exposure limits.
 * Returns { allowed, reason, violations[] }.
 */
function checkExposureLimits(legs, maxRiskPerParlay, maxExposurePerTeam) {
  if (!maxExposurePerTeam || maxExposurePerTeam <= 0) {
    return { allowed: true, reason: null, violations: [] };
  }

  const violations = [];
  for (const leg of legs) {
    const name = leg.team || leg.teamName || leg.lineInfo?.teamName || 'unknown';
    const key = normalizeExposureKey(name);
    if (!key) continue;

    const current = exposure[key]?.risk || 0;
    const afterAdd = current + maxRiskPerParlay;

    if (afterAdd > maxExposurePerTeam) {
      violations.push({
        team: name,
        currentExposure: current,
        wouldBe: afterAdd,
        limit: maxExposurePerTeam,
      });
    }
  }

  if (violations.length > 0) {
    const names = violations.map(v => v.team).join(', ');
    return {
      allowed: false,
      reason: `Exposure limit exceeded for: ${names}`,
      violations,
    };
  }

  return { allowed: true, reason: null, violations: [] };
}

/**
 * Get full exposure snapshot — all teams with active exposure.
 */
function getExposureSnapshot() {
  return Object.entries(exposure)
    .map(([key, val]) => ({ key, ...val }))
    .sort((a, b) => b.risk - a.risk);
}

module.exports = {
  recordQuote,
  recordConfirmation,
  recordRejection,
  recordSettlement,
  findByParlayId,
  findByOrderUuid,
  getRecentOrders,
  getStats,
  getPnLBySport,
  getExposureForTeam,
  checkExposureLimits,
  getExposureSnapshot,
};
