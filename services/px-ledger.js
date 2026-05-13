/**
 * PX-native P&L ledger. Pulls full order history from ProphetX and
 * aggregates realized P&L, open exposure, and net balance impact
 * directly from PX's settlement_status + profit fields.
 *
 * This is the authoritative source of truth for P&L. The in-memory
 * order tracker misses silent losses (PX only emits `order.matched`
 * for SP wins, per Alec's event model), so tracker-derived P&L over-
 * states by the unsettled-loss count × avg stake.
 *
 * Cached because a full paginated fetch (up to ~2100 orders) takes
 * several seconds; dashboard refreshes every minute.
 */
const px = require('./prophetx');
const log = require('./logger');

let cache = null; // { at: ms, summary, ledger }
const CACHE_TTL_MS = 60 * 1000; // 1 min — fresh enough for dashboard polling

async function fetchLedger({ limit = 5000, force = false } = {}) {
  const now = Date.now();
  if (!force && cache && (now - cache.at) < CACHE_TTL_MS) return cache;

  const startedAt = now;
  const orders = await px.fetchOrders(limit);
  const summary = summarize(orders);
  cache = { at: now, summary, ledger: orders, fetchMs: Date.now() - startedAt };
  log.info('PxLedger', `Fetched ${orders.length} PX orders in ${cache.fetchMs}ms — realized $${summary.realizedPnL.toFixed(2)}, open $${summary.openExposure.toFixed(2)}`);
  return cache;
}

/**
 * Aggregate a raw PX orders array into the canonical P&L shape.
 * Pure function (no network) so it's reusable by tests.
 */
function summarize(orders) {
  const byStatus = {};
  const bySettlementStatus = {};
  let realizedPnL = 0;
  let openExposure = 0;  // stakes on unsettled orders that actually debited balance
  let stakesOnWins = 0, stakesOnLosses = 0, stakesOnPushes = 0;
  let profitOnWins = 0, profitOnLosses = 0;
  let countWins = 0, countLosses = 0, countPushes = 0;
  let openCount = 0;

  for (const po of orders) {
    const st = (po.status || '').toLowerCase();
    byStatus[st] = (byStatus[st] || 0) + 1;

    // 'rejected' and 'failed' never debited balance — skip entirely.
    if (st === 'rejected' || st === 'failed') continue;

    const settlementStatus = (po.settlement_status || '').toLowerCase();
    const stake = Number(po.confirmed_stake ?? po.stake ?? po.matched_stake ?? 0);
    const profit = po.profit != null ? Number(po.profit) : null;

    if (st === 'settled') {
      bySettlementStatus[settlementStatus || '(none)'] = (bySettlementStatus[settlementStatus || '(none)'] || 0) + 1;
      if (settlementStatus === 'won') {
        countWins++; stakesOnWins += stake;
        // PX populates profit > 0 on wins (= bettor's wager kept).
        if (profit != null) { profitOnWins += profit; realizedPnL += profit; }
      } else if (settlementStatus === 'lost') {
        countLosses++; stakesOnLosses += stake;
        // PX populates profit < 0 on losses. Fall back to -stake if missing.
        const loss = profit != null ? profit : -stake;
        profitOnLosses += loss; realizedPnL += loss;
      } else if (settlementStatus === 'push') {
        countPushes++; stakesOnPushes += stake;
        // Push = net zero (stake returned).
      }
    } else {
      // finalized (waiting for settlement) or any other non-rejected non-settled
      // status = money locked up in open parlays.
      openCount++;
      openExposure += stake;
    }
  }

  return {
    realizedPnL: round(realizedPnL),
    openExposure: round(openExposure),
    // Net balance impact vs starting bankroll = realized minus stakes still
    // locked in open parlays (those stakes are currently debited).
    netBalanceImpact: round(realizedPnL - openExposure),
    counts: { wins: countWins, losses: countLosses, pushes: countPushes, open: openCount, totalActive: countWins + countLosses + countPushes + openCount },
    stakes: {
      wins: round(stakesOnWins),
      losses: round(stakesOnLosses),
      pushes: round(stakesOnPushes),
      open: round(openExposure),
      totalSettled: round(stakesOnWins + stakesOnLosses + stakesOnPushes),
    },
    profit: { wins: round(profitOnWins), losses: round(profitOnLosses) },
    statusBreakdown: byStatus,
    settlementStatusBreakdown: bySettlementStatus,
  };
}

function round(n) { return Math.round(n * 100) / 100; }

async function getSummary({ force = false } = {}) {
  const c = await fetchLedger({ force });
  return {
    ...c.summary,
    fetchedAt: new Date(c.at).toISOString(),
    fetchMs: c.fetchMs,
    cacheTtlMs: CACHE_TTL_MS,
    cached: !force && (Date.now() - c.at) < CACHE_TTL_MS,
  };
}

/**
 * Sync, never-fetching accessor — returns the cached PX-native open
 * exposure if a fetch has populated the cache, otherwise null. Used by
 * the /status hot path which can't afford a multi-second blocking PX
 * fetch. Caller is responsible for falling back to a local estimate
 * when this returns null (cold-start scenario).
 */
function getCachedOpenExposure() {
  if (!cache) return null;
  return cache.summary?.openExposure ?? null;
}

module.exports = { fetchLedger, summarize, getSummary, getCachedOpenExposure };
