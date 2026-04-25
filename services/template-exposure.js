/**
 * Template-exposure ramp
 *
 * Tracks confirmed bets grouped by PARLAY SIGNATURE (canonical leg tuple)
 * and applies a graduated vig ramp / hard decline when the same template
 * accumulates multiple confirmations inside a rolling window.
 *
 * Motivation — April 18 post-mortem (Apr 14-22 dataset):
 *   - 8 of 9 days had at least one repeated template
 *   - 15-23% of confirmed bets sat inside a repeated template every day
 *   - April 18 cliff: 6 bettors stacked "Rockies ML + Under 11" for
 *     -$5,769 in a single parlay signature, 9 bettors stacked the same
 *     MMA 6-leg for -$2,520. Two templates = 80% of the day's gross
 *     losses.
 *   - Counterfactual: blocking 4th+ bet on any signature across Apr 14-22
 *     would have avoided $5,443 in losses at a cost of $64 in foregone
 *     wins — 84:1 asymmetry.
 *
 * Existing team/event exposure caps don't catch this because the losses
 * come from ONE parlay copied across many counterparties. Template is a
 * first-class exposure dimension alongside team/event/sport.
 *
 * Mechanism:
 *   1. When a bet confirms (orderUuid arrives), record its canonical
 *      signature + stake against a rolling window.
 *   2. At price time, compute the RFQ's signature, look up current
 *      exposure (count + totalStake). Return a ramp decision:
 *        - extraVig: ADDITIVE to the existing vig rate (same units,
 *          stacks additively with longshotAdd, capped at 0.20 downstream)
 *        - decline: boolean, true when count has hit the hard cap
 *        - reason: decline reason for the pricer failure record
 *   3. Pricer adds extraVig to its vig rate, or bails with the reason.
 *
 * Ramp tiers are configurable. Defaults calibrated from the 9-day
 * counterfactual analysis:
 *   count=0 (1st bet) → 0 extra
 *   count=1 (2nd bet) → +0.25pp
 *   count=2 (3rd bet) → +1.0pp
 *   count=3 (4th bet) → +3.0pp
 *   count=4 (5th bet) → DECLINE
 *
 * Window: 24h rolling by default. Signatures with no confirmations in
 * the window get pruned periodically.
 *
 * Persistence: in-memory only. Boot reconstructs from recently-loaded
 * order history (rebuildFromOrders). No new DB table required.
 */

const { config } = require('../config');
const log = require('./logger');

const WINDOW_MS = (config.pricing.templateRampWindowHours || 24) * 60 * 60 * 1000;
const ENABLED = config.pricing.templateRampEnabled !== false;

// signature -> { confirmations: [{ parlayId, stake, confirmedAt (ms epoch) }] }
const _exposure = {};

let _stats = {
  recordedConfirmations: 0,
  signaturesActive: 0,
  lastPrunedAt: null,
  rampHits: { tier2: 0, tier3: 0, tier4: 0, decline: 0 },
};

// --------------------------------------------------------------------
// Signature canonicalization
// --------------------------------------------------------------------

function normalizeTeam(s) {
  if (s == null) return '?';
  return String(s).trim().toLowerCase().slice(0, 60);
}

/**
 * Build a canonical JSON-string key from a parlay's legs. Order-
 * independent so bettors can't evade by reordering. Uses
 * (team, market, line) tuples — the same primitives the dashboard
 * uses to describe a parlay to humans.
 *
 * For SPREAD and TOTAL markets the line value is intentionally
 * COLLAPSED to a single bucket per (team, market). Apr 25 forensic
 * review of the recurring "Rockies + Under (Rockies @ Mets)" probe
 * showed bettors evading the ramp by submitting near-identical parlays
 * across 2-3 alt-lines (Under 8 vs 8.5 vs 9 on the same total leg,
 * Rockies +1.5 vs +2.5 on the same spread leg). Substantively the
 * same thesis but each landed on a distinct signature, so the ramp
 * never accumulated a count > 0. Dropping the line value at the
 * canonicalization step closes that evasion in one line.
 *
 * Spread SIDE (Rockies +1.5 vs Mets -1.5) is still preserved because
 * those are mathematically opposite bets (different theses, even
 * when paired with the same total leg). Same for Over vs Under.
 *
 * Moneyline legs already carry no line value, so they're unaffected.
 */
function canonicalSignature(legs) {
  if (!Array.isArray(legs) || legs.length === 0) return null;
  const tuples = legs.map(l => {
    const team = normalizeTeam(l.team || l.teamName || '?');
    const market = (l.market || l.marketType || '?').toLowerCase();
    // Collapse alt-line probing on spread/total markets. See header above.
    const isLineMarket = market === 'spread' || market === 'total' ||
                         market === 'team_total' || market === 'run_line' ||
                         market === 'puck_line' || market === 'alt_spread' ||
                         market === 'alt_total';
    const line = isLineMarket
      ? null
      : ((l.line != null && !isNaN(Number(l.line))) ? Number(l.line) : null);
    return [team, market, line];
  });
  // Stable sort to make ordering irrelevant
  tuples.sort((a, b) => {
    if (a[0] !== b[0]) return a[0] < b[0] ? -1 : 1;
    if (a[1] !== b[1]) return a[1] < b[1] ? -1 : 1;
    const la = a[2] == null ? -Infinity : a[2];
    const lb = b[2] == null ? -Infinity : b[2];
    return la - lb;
  });
  return JSON.stringify(tuples);
}

// --------------------------------------------------------------------
// Recording
// --------------------------------------------------------------------

/**
 * Called from order-tracker.recordConfirmation when a real fill lands
 * (orderUuid first arrival). Idempotent: duplicate parlayIds for the
 * same signature are ignored so replays / order-matched storms don't
 * double-count.
 */
function recordConfirmation(legs, parlayId, stake, confirmedAt = null) {
  if (!ENABLED) return;
  const sig = canonicalSignature(legs);
  if (!sig || !parlayId || !(stake > 0)) return;
  const ts = confirmedAt ? new Date(confirmedAt).getTime() : Date.now();
  if (isNaN(ts)) return;
  if (!_exposure[sig]) _exposure[sig] = { confirmations: [] };
  // Dedupe by parlayId
  if (_exposure[sig].confirmations.some(c => c.parlayId === parlayId)) return;
  _exposure[sig].confirmations.push({ parlayId, stake, confirmedAt: ts });
  _stats.recordedConfirmations++;
}

/**
 * Return current in-window exposure for a given parlay signature.
 * Also prunes expired entries for that signature lazily.
 */
function getExposure(legs, nowMs = null) {
  const sig = canonicalSignature(legs);
  if (!sig || !_exposure[sig]) return { signature: sig, count: 0, totalStake: 0, firstAt: null, lastAt: null };
  const now = nowMs || Date.now();
  const cutoff = now - WINDOW_MS;
  const entry = _exposure[sig];
  entry.confirmations = entry.confirmations.filter(c => c.confirmedAt >= cutoff);
  if (entry.confirmations.length === 0) {
    delete _exposure[sig];
    return { signature: sig, count: 0, totalStake: 0, firstAt: null, lastAt: null };
  }
  const totalStake = entry.confirmations.reduce((s, c) => s + c.stake, 0);
  const firstAt = Math.min(...entry.confirmations.map(c => c.confirmedAt));
  const lastAt = Math.max(...entry.confirmations.map(c => c.confirmedAt));
  return {
    signature: sig,
    count: entry.confirmations.length,
    totalStake,
    firstAt: new Date(firstAt).toISOString(),
    lastAt: new Date(lastAt).toISOString(),
  };
}

// --------------------------------------------------------------------
// Ramp decision
// --------------------------------------------------------------------

/**
 * Given a parlay's legs, return the ramp decision:
 *   { extraVig: number, decline: boolean, reason: string|null,
 *     count: number, totalStake: number }
 *
 * count is the number of prior confirmations on this signature inside
 * the window. The current RFQ is NOT counted — the ramp applies to the
 * nth bet based on how many identical bets have ALREADY confirmed.
 */
function getRampDecision(legs) {
  if (!ENABLED) return { extraVig: 0, decline: false, reason: null, count: 0, totalStake: 0 };
  const exp = getExposure(legs);
  const priorCount = exp.count;

  // Tiered defaults; all knobs Railway-tunable.
  const declineAt = config.pricing.templateRampDeclineAt;      // e.g. 4 → decline 5th+ bet
  const tier2Add  = config.pricing.templateRampTier2Add;       // added for 2nd bet (priorCount==1)
  const tier3Add  = config.pricing.templateRampTier3Add;       // added for 3rd bet (priorCount==2)
  const tier4Add  = config.pricing.templateRampTier4Add;       // added for 4th bet (priorCount==3)

  if (declineAt > 0 && priorCount >= declineAt) {
    _stats.rampHits.decline++;
    return {
      extraVig: 0, decline: true,
      reason: `template_cap: ${priorCount} prior confirmations on this signature in ${WINDOW_MS / 3600000}h window`,
      count: priorCount, totalStake: exp.totalStake,
    };
  }

  let extraVig = 0;
  if (priorCount === 1)      { extraVig = tier2Add; _stats.rampHits.tier2++; }
  else if (priorCount === 2) { extraVig = tier3Add; _stats.rampHits.tier3++; }
  else if (priorCount >= 3)  { extraVig = tier4Add; _stats.rampHits.tier4++; }

  return { extraVig, decline: false, reason: null, count: priorCount, totalStake: exp.totalStake };
}

// --------------------------------------------------------------------
// Maintenance
// --------------------------------------------------------------------

function prune(nowMs = null) {
  const now = nowMs || Date.now();
  const cutoff = now - WINDOW_MS;
  let pruned = 0;
  for (const sig of Object.keys(_exposure)) {
    const entry = _exposure[sig];
    entry.confirmations = entry.confirmations.filter(c => c.confirmedAt >= cutoff);
    if (entry.confirmations.length === 0) { delete _exposure[sig]; pruned++; }
  }
  _stats.lastPrunedAt = new Date(now).toISOString();
  _stats.signaturesActive = Object.keys(_exposure).length;
  return pruned;
}

let _pruneTimer = null;
function startPruneLoop(intervalMs = 5 * 60 * 1000) {
  if (_pruneTimer) return;
  _pruneTimer = setInterval(() => {
    try { prune(); } catch (err) { log.warn('TemplateExposure', `prune failed: ${err.message}`); }
  }, intervalMs);
}

/**
 * Rebuild the in-memory exposure map from an array of already-loaded
 * orders. Called at service boot once order-tracker has hydrated from
 * Supabase. Only orders with orderUuid + confirmedStake + confirmedAt
 * within the window contribute.
 */
function rebuildFromOrders(orders) {
  const cutoff = Date.now() - WINDOW_MS;
  let added = 0;
  for (const o of orders || []) {
    if (!o.orderUuid) continue;
    if (!(o.confirmedStake > 0)) continue;
    if (!o.confirmedAt) continue;
    const ts = new Date(o.confirmedAt).getTime();
    if (isNaN(ts) || ts < cutoff) continue;
    const legs = o.legs || (o.meta && o.meta.legs) || [];
    if (legs.length === 0) continue;
    recordConfirmation(legs, o.parlayId, o.confirmedStake, o.confirmedAt);
    added++;
  }
  _stats.signaturesActive = Object.keys(_exposure).length;
  log.info('TemplateExposure', `Rebuilt from history: ${added} confirmations across ${_stats.signaturesActive} signatures (window ${WINDOW_MS / 3600000}h)`);
  return added;
}

// --------------------------------------------------------------------
// Stats
// --------------------------------------------------------------------

function getStats() {
  // Snapshot — include top-N active signatures by count for observability
  const top = Object.entries(_exposure)
    .map(([sig, e]) => ({
      signature: sig.slice(0, 100) + (sig.length > 100 ? '…' : ''),
      count: e.confirmations.length,
      totalStake: e.confirmations.reduce((s, c) => s + c.stake, 0),
    }))
    .sort((a, b) => b.count - a.count || b.totalStake - a.totalStake)
    .slice(0, 10);
  return {
    enabled: ENABLED,
    windowHours: WINDOW_MS / 3600000,
    ..._stats,
    // Override _stats.signaturesActive with a live count (prune/rebuild
    // only update the stale field on their own schedules).
    signaturesActive: Object.keys(_exposure).length,
    tiers: {
      tier2Add: config.pricing.templateRampTier2Add,
      tier3Add: config.pricing.templateRampTier3Add,
      tier4Add: config.pricing.templateRampTier4Add,
      declineAt: config.pricing.templateRampDeclineAt,
    },
    topActive: top,
  };
}

module.exports = {
  canonicalSignature,
  recordConfirmation,
  getExposure,
  getRampDecision,
  prune,
  startPruneLoop,
  rebuildFromOrders,
  getStats,
};
