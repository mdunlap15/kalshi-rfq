/**
 * FairProbEstimator — v2 fair-prob pipeline.
 *
 * Takes a single leg (already de-vigged via existing oddsFeed.getFairProb
 * or equivalent) and applies:
 *   1. Calibration correction from calibration-trainer
 *   2. Uncertainty estimate derived from:
 *      - Book count (fewer books = more uncertainty)
 *      - Book disagreement (spread of book-implied probs)
 *      - Calibration sample size for the bucket
 *
 * Returns `{ fairProb, uncertainty, nBooks, bucket, corrections }` so
 * downstream stages can both use the corrected prob AND widen vig when
 * uncertainty is high.
 *
 * Does NOT replace the existing de-vig code — sits on top of it. The
 * v1 path still computes `raw_fair` from book de-vig; this module
 * corrects + assesses confidence.
 */

const calibration = require('./calibration-trainer');

// Uncertainty floor + ceiling. 0.01 = our tightest confidence on a
// clean multi-book market; 0.15 = very wide for thin markets or
// sparse-calibration buckets.
const UNCERTAINTY_FLOOR = 0.01;
const UNCERTAINTY_CEILING = 0.15;

/**
 * Combine calibration-sample uncertainty with book-level uncertainty
 * in quadrature (assuming rough independence of sources).
 */
function combineUncertainty(calibUncertainty, bookUncertainty) {
  const combined = Math.sqrt(
    (calibUncertainty || 0) ** 2 + (bookUncertainty || 0) ** 2
  );
  return Math.max(UNCERTAINTY_FLOOR, Math.min(UNCERTAINTY_CEILING, combined));
}

/**
 * Book-level uncertainty from the disagreement of book-implied probs.
 * Standard deviation of book_implied_probs across the books that
 * quoted this leg. Single-book markets get a penalty.
 */
function bookUncertainty(bookProbs) {
  if (!bookProbs || bookProbs.length === 0) return 0.10;
  if (bookProbs.length === 1) return 0.08; // single book → broad uncertainty
  const mean = bookProbs.reduce((s, p) => s + p, 0) / bookProbs.length;
  const variance = bookProbs.reduce((s, p) => s + (p - mean) ** 2, 0) / bookProbs.length;
  const stdev = Math.sqrt(variance);
  // Lower bound 0.005 (very tight agreement doesn't mean zero uncertainty —
  // books can all be wrong together).
  return Math.max(0.005, stdev);
}

/**
 * Main API. Given a leg's raw fair-prob (from current de-vig pipeline)
 * plus optional book-implied probs + sport/market metadata, return
 * the corrected fair prob and its uncertainty.
 */
function estimate({ rawFairProb, sport, marketType, bookImpliedProbs }) {
  if (rawFairProb == null || !(rawFairProb > 0 && rawFairProb < 1)) {
    return {
      fairProb: null,
      uncertainty: UNCERTAINTY_CEILING,
      nBooks: (bookImpliedProbs || []).length,
      bucket: null,
      corrections: { calibration: 0 },
      rawFairProb,
    };
  }

  const calib = calibration.getCorrection(sport, marketType, rawFairProb);
  const calibCorrection = calib.correction || 0;
  const calibUncertainty = calib.uncertainty || 0.05;
  const bookU = bookUncertainty(bookImpliedProbs);

  // Apply correction, then clamp to (0.005, 0.995) — at the extremes
  // vig math gets unstable and real markets rarely have truly-certain
  // outcomes pre-settlement.
  let corrected = rawFairProb + calibCorrection;
  corrected = Math.max(0.005, Math.min(0.995, corrected));

  const uncertainty = combineUncertainty(calibUncertainty, bookU);

  return {
    fairProb: Math.round(corrected * 100000) / 100000,
    uncertainty: Math.round(uncertainty * 10000) / 10000,
    nBooks: (bookImpliedProbs || []).length,
    bucket: calib.bucket,
    corrections: {
      calibration: Math.round(calibCorrection * 10000) / 10000,
    },
    rawFairProb: Math.round(rawFairProb * 100000) / 100000,
  };
}

module.exports = {
  estimate,
  bookUncertainty,
  combineUncertainty,
  UNCERTAINTY_FLOOR,
  UNCERTAINTY_CEILING,
};
