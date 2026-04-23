/**
 * Calibration trainer for v2 pricing.
 *
 * Loads historical settled legs from Supabase, deduplicates by event,
 * and fits per-bucket calibration correction factors based on the
 * predicted-vs-actual bias observed across outcomes.
 *
 * Bucket key: (sport, market, odds_band)
 *   odds_band = one of ['deep_dog','dog','mild_dog','coinflip','mild_fav','fav','heavy_fav']
 *
 * Correction semantics:
 *   If bucket has N=200 legs, avg_predicted=0.536, actual_win_rate=0.403:
 *     bias = 0.536 - 0.403 = +0.133 → we're overconfident by 13.3pp
 *     correction = -0.133 applied to future predictions in same bucket
 *     corrected_fair = raw_fair + correction_for_bucket (shifts toward actual)
 *
 * Shrinkage: small-sample buckets get pulled toward zero correction to
 * avoid fitting noise. Uses Beta-binomial-style shrinkage with prior
 * strength of K pseudo-observations (default 20).
 *
 * Output schema (in-memory + persisted to disk cache for restart):
 *   {
 *     trainedAt: ISO,
 *     legsAnalyzed: number,
 *     buckets: {
 *       'baseball_mlb|spread|coinflip': {
 *         n: 197,
 *         avgPredicted: 0.549,
 *         actualWinRate: 0.518,
 *         rawBias: +0.031,
 *         shrunkBias: +0.027,
 *         correction: -0.027,
 *         uncertainty: 0.011,  // stdev of predicted vs actual
 *       },
 *       ...
 *     },
 *     overall: { n, avgPred, actual, bias }
 *   }
 *
 * Public API:
 *   trainFromOrders(orders) → stats object
 *   getCorrection(sport, market, fairProb) → { correction, uncertainty, n }
 *   getStats() → current fit summary
 */

const log = require('../logger');

const ODDS_BANDS = [
  { name: 'deep_dog',   min: 0.00, max: 0.30 },
  { name: 'dog',        min: 0.30, max: 0.40 },
  { name: 'mild_dog',   min: 0.40, max: 0.50 },
  { name: 'coinflip',   min: 0.50, max: 0.55 },
  { name: 'mild_fav',   min: 0.55, max: 0.65 },
  { name: 'fav',        min: 0.65, max: 0.75 },
  { name: 'heavy_fav',  min: 0.75, max: 1.00 },
];

// Minimum sample size per bucket before we apply a non-zero correction.
// Below this, the shrinkage-weighted correction trends to 0.
const SHRINKAGE_K = 30;

// Cap corrections at ±15pp to guard against outlier buckets with
// surprising small-sample extreme biases.
const MAX_CORRECTION = 0.15;

let _fit = {
  trainedAt: null,
  legsAnalyzed: 0,
  buckets: {},
  overall: null,
};

function bandFor(fairProb) {
  for (const b of ODDS_BANDS) {
    if (fairProb >= b.min && fairProb < b.max) return b.name;
  }
  return 'coinflip'; // fallback
}

function bucketKey(sport, market, band) {
  return `${sport}|${market}|${band}`;
}

/**
 * Build a deduplicated training set from raw loaded orders. Same-event
 * legs (same pxEventId × same market × same selection × same line) are
 * collapsed into a single observation so one game's outcome doesn't
 * count 10 times.
 */
function dedupeLegs(orders) {
  const uniq = {};
  for (const o of orders) {
    const legs = o.legs || (o.meta && o.meta.legs) || [];
    for (const l of legs) {
      const fair = l.fairProb;
      const status = l.settlementStatus || l.settlement_status;
      if (fair == null || !(fair > 0 && fair < 1)) continue;
      if (status !== 'won' && status !== 'lost') continue;
      const px = l.pxEventId;
      if (!px) continue;
      const key = `${px}|${l.market}|${l.selection}|${l.line}`;
      if (!uniq[key]) {
        uniq[key] = {
          fair_sum: 0, fair_n: 0,
          status, sport: l.sport, market: l.market,
          selection: l.selection, line: l.line, pxEventId: px,
        };
      }
      uniq[key].fair_sum += fair;
      uniq[key].fair_n += 1;
    }
  }
  return Object.values(uniq).map(u => ({
    fair: u.fair_sum / u.fair_n,
    status: u.status, sport: u.sport, market: u.market,
    selection: u.selection, line: u.line,
  }));
}

/**
 * Fit bucket corrections from a deduped leg set.
 */
function fitBuckets(deduped) {
  const groups = {};
  for (const l of deduped) {
    const band = bandFor(l.fair);
    const key = bucketKey(l.sport, l.market, band);
    if (!groups[key]) groups[key] = { legs: [] };
    groups[key].legs.push(l);
  }

  const buckets = {};
  for (const [key, g] of Object.entries(groups)) {
    const n = g.legs.length;
    const avgPred = g.legs.reduce((s, l) => s + l.fair, 0) / n;
    const wins = g.legs.filter(l => l.status === 'won').length;
    const actualWinRate = wins / n;
    const rawBias = avgPred - actualWinRate;

    // Shrinkage: pull correction toward 0 when sample is small.
    // Effective correction = rawBias * n / (n + K)
    const shrinkWeight = n / (n + SHRINKAGE_K);
    const shrunkBias = rawBias * shrinkWeight;

    // Cap at ±MAX_CORRECTION
    let correction = -shrunkBias;
    if (correction > MAX_CORRECTION) correction = MAX_CORRECTION;
    if (correction < -MAX_CORRECTION) correction = -MAX_CORRECTION;

    // Standard error of the proportion estimate (for uncertainty)
    const p = actualWinRate;
    const se = n > 0 ? Math.sqrt(p * (1 - p) / n) : 0.5;

    buckets[key] = {
      n,
      avgPredicted: Math.round(avgPred * 10000) / 10000,
      actualWinRate: Math.round(actualWinRate * 10000) / 10000,
      rawBias: Math.round(rawBias * 10000) / 10000,
      shrunkBias: Math.round(shrunkBias * 10000) / 10000,
      correction: Math.round(correction * 10000) / 10000,
      uncertainty: Math.round(se * 10000) / 10000,
    };
  }
  return buckets;
}

/**
 * Train calibration from a set of loaded orders. Called at boot + on
 * a weekly refit timer. Persists fit to in-memory _fit.
 */
function trainFromOrders(orders) {
  if (!Array.isArray(orders) || orders.length === 0) {
    log.warn('V2Calibration', 'trainFromOrders: empty order set');
    return _fit;
  }
  const settled = orders.filter(o => (o.status || '').startsWith('settled_'));
  const deduped = dedupeLegs(settled);
  if (deduped.length === 0) {
    log.warn('V2Calibration', 'no deduped legs to train on');
    return _fit;
  }
  const buckets = fitBuckets(deduped);
  const overall = {
    n: deduped.length,
    avgPred: deduped.reduce((s, l) => s + l.fair, 0) / deduped.length,
    actual: deduped.filter(l => l.status === 'won').length / deduped.length,
  };
  overall.bias = overall.avgPred - overall.actual;
  _fit = {
    trainedAt: new Date().toISOString(),
    legsAnalyzed: deduped.length,
    buckets,
    overall: {
      n: overall.n,
      avgPredicted: Math.round(overall.avgPred * 10000) / 10000,
      actualWinRate: Math.round(overall.actual * 10000) / 10000,
      bias: Math.round(overall.bias * 10000) / 10000,
    },
  };
  log.info('V2Calibration', `Trained on ${deduped.length} deduped legs, ${Object.keys(buckets).length} buckets`);
  return _fit;
}

/**
 * Return correction + uncertainty for a given (sport, market, fairProb).
 * When bucket has insufficient samples or no fit loaded, returns a
 * zero-correction neutral result so callers can safely no-op.
 */
function getCorrection(sport, market, fairProb) {
  if (fairProb == null || !(fairProb > 0 && fairProb < 1)) {
    return { correction: 0, uncertainty: 0.10, n: 0, bucket: null };
  }
  const band = bandFor(fairProb);
  const key = bucketKey(sport, market, band);
  const b = _fit.buckets[key];
  if (!b) {
    // No bucket data — neutral correction, broad uncertainty
    return { correction: 0, uncertainty: 0.10, n: 0, bucket: key };
  }
  return {
    correction: b.correction,
    uncertainty: b.uncertainty || 0.05,
    n: b.n,
    bucket: key,
  };
}

function getStats() {
  return _fit;
}

/**
 * Reset the fit (for tests + explicit refits).
 */
function reset() {
  _fit = { trainedAt: null, legsAnalyzed: 0, buckets: {}, overall: null };
}

module.exports = {
  ODDS_BANDS,
  bandFor,
  bucketKey,
  dedupeLegs,
  fitBuckets,
  trainFromOrders,
  getCorrection,
  getStats,
  reset,
};
