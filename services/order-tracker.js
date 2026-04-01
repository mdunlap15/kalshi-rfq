const log = require('./logger');

// ---------------------------------------------------------------------------
// IN-MEMORY ORDER STORE
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
};

// ---------------------------------------------------------------------------
// EXPOSURE TRACKING — team/selection level risk across confirmed parlays
// ---------------------------------------------------------------------------
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
    ourAmericanOdds: ourQuote?.offeredOdds || null, // Already stored as American
    // PX sends matched_odds with opposite sign to our format.
    // Evidence: when we win at +18297, PX broadcasts -18297.
    // Negate PX value so both are in the same format for comparison.
    matchedAmericanOdds: matchedOdds != null ? -matchedOdds : null,
    outcome,
    legCount: resolvedLegs.length,
  };

  matchedParlays.unshift(entry); // newest first
  if (matchedParlays.length > 200) matchedParlays.pop(); // cap memory

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

  // Track sports from unknown legs
  if (detail?.unknownSports) {
    for (const sport of detail.unknownSports) {
      if (!declineStats.unknownSports[sport]) {
        declineStats.unknownSports[sport] = { count: 0, lastSeen: null };
      }
      declineStats.unknownSports[sport].count++;
      declineStats.unknownSports[sport].lastSeen = new Date().toISOString();
    }
  }

  // Track near-misses (all legs known but couldn't price)
  if (reason === 'no fair value' && detail) {
    declineStats.nearMisses.unshift({
      parlayId: detail.parlayId,
      legs: detail.knownLegs || [],
      time: new Date().toISOString(),
      reason: 'no fair value for one or more legs',
    });
    if (declineStats.nearMisses.length > 50) declineStats.nearMisses.pop();
  }
}

function getMarketIntel(limit = 50) {
  return {
    stats: { ...marketStats },
    declines: {
      total: declineStats.total,
      reasons: { ...declineStats.reasons },
      unknownSports: { ...declineStats.unknownSports },
      nearMissCount: declineStats.nearMisses.length,
      recentNearMisses: declineStats.nearMisses.slice(0, 10),
    },
    recentMatched: matchedParlays.slice(0, limit),
    quoteWinRate: marketStats.weQuoted > 0 ? (marketStats.weWon / marketStats.weQuoted * 100).toFixed(1) + '%' : '-',
    coverageRate: marketStats.totalMatched > 0 ? (marketStats.weQuoted / marketStats.totalMatched * 100).toFixed(1) + '%' : '-',
    // Sport breakdown of matched parlays
    matchedBySport: (() => {
      const bySport = {};
      for (const m of matchedParlays) {
        const sports = [...new Set((m.legs || []).map(l => l.sport).filter(Boolean))];
        for (const s of sports) {
          if (!bySport[s]) bySport[s] = { count: 0, weQuoted: 0, missed: 0, avgStake: 0, totalStake: 0 };
          bySport[s].count++;
          bySport[s].totalStake += (m.matchedStake || 0);
          if (m.weQuoted) bySport[s].weQuoted++;
          else bySport[s].missed++;
        }
        if (sports.length === 0) {
          if (!bySport['unknown']) bySport['unknown'] = { count: 0, weQuoted: 0, missed: 0, avgStake: 0, totalStake: 0 };
          bySport['unknown'].count++;
          bySport['unknown'].totalStake += (m.matchedStake || 0);
          bySport['unknown'].missed++;
        }
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
          legCount: m.legCount,
          ourOdds,
          winOdds,
          ourProb: Math.round(ourProb * 10000) / 100,
          winProb: Math.round(winProb * 10000) / 100,
          gapProb: Math.round(gapProb * 10000) / 100, // in percentage points
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
  recordMatchedParlay,
  recordDecline,
  getMarketIntel,
};
