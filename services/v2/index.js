/**
 * v2 pricing engine — unified orchestration.
 *
 * Single entry point: `priceParlayV2(pricedLegs, opts)` → { offer, meta }
 *
 * `pricedLegs` is already a validated list of legs with raw fair-prob
 * (from existing v1 pipeline: de-vig, series/MMA/golf special-cases,
 * alt-line lookup). This module does the v2-specific work on top:
 *   1. Calibration correction per leg (FairProbEstimator)
 *   2. Correlation-aware parlay prob + uncertainty propagation
 *   3. EV-targeted vig
 *
 * Shadow mode: computes alongside v1 and logs a comparison record but
 * doesn't affect the live offer. Flag: `config.pricing.pricingV2Enabled`
 * (read here, not imported so v1 is unaffected when flag off).
 */

const log = require('../logger');
const calibration = require('./calibration-trainer');
const estimator = require('./fair-prob-estimator');
const correlation = require('./correlation');
const evVig = require('./ev-vig');

// Comparison log for shadow mode. In-memory ring buffer, last 500 parlays.
const _shadowLog = [];
const SHADOW_LOG_CAP = 500;

function recordShadow(record) {
  _shadowLog.push(record);
  if (_shadowLog.length > SHADOW_LOG_CAP) _shadowLog.shift();
}

function americanFromImplied(implied) {
  if (implied == null || implied <= 0 || implied >= 1) return null;
  return implied >= 0.5
    ? -Math.round((implied / (1 - implied)) * 100)
    : Math.round(100 / implied - 100);
}

function americanToImplied(a) {
  if (a == null) return null;
  if (a > 0) return 100 / (a + 100);
  return -a / (-a + 100);
}

/**
 * Run V2 pricing on a set of already-priced legs. Returns the v2
 * offered American odds + full breakdown, or null on invalid input.
 *
 * `pricedLegs` expected shape (per leg):
 *   { fairProb, lineInfo: { sport, marketType, homeTeam, awayTeam,
 *     pxEventId, line, selection }, pinnacleOdds, fanduelOdds,
 *     draftkingsOdds }
 *
 * `opts`: { targetEdge, kSigma, templateRampAdd, vigLegCount }
 */
function priceParlayV2(pricedLegs, opts = {}) {
  if (!pricedLegs || pricedLegs.length === 0) return null;

  const targetEdge = opts.targetEdge != null ? opts.targetEdge : 0.02;
  const kSigma = opts.kSigma != null ? opts.kSigma : 0.5;
  const templateRampAdd = opts.templateRampAdd || 0;

  // Step 1: Per-leg calibration + uncertainty
  const legsV2 = pricedLegs.map(l => {
    const raw = l.fairProb;
    const sport = l.lineInfo?.sport;
    const marketType = l.lineInfo?.marketType;
    const books = [
      americanToImplied(l.pinnacleOdds),
      americanToImplied(l.fanduelOdds),
      americanToImplied(l.draftkingsOdds),
    ].filter(p => p != null && p > 0 && p < 1);
    const est = estimator.estimate({
      rawFairProb: raw,
      sport,
      marketType,
      bookImpliedProbs: books,
    });
    return {
      rawFairProb: raw,
      fairProb: est.fairProb,
      uncertainty: est.uncertainty,
      nBooks: est.nBooks,
      pxEventId: l.lineInfo?.pxEventId,
      marketType,
      sport,
      // Passed through for cross-event correlation (same-sport same-night totals)
      selection: l.lineInfo?.selection,
      startTime: l.lineInfo?.startTime,
      calibrationBucket: est.bucket,
      calibrationCorrection: est.corrections.calibration,
    };
  });

  // Abort if any leg failed estimation
  if (legsV2.some(l => l.fairProb == null)) {
    return null;
  }

  // Step 2: Correlation-aware parlay prob + uncertainty
  const comb = correlation.combineProbs(legsV2);
  const parlayUncertainty = correlation.propagateUncertainty(legsV2);
  // Convert log-std back to prob-space approximate std:
  //   σ_p ≈ σ_logp * p   (first-order)
  const parlayProbStd = parlayUncertainty * comb.parlayProb;

  // Step 3: EV-targeted vig
  const vig = evVig.solveVig({
    parlayFairProb: comb.parlayProb,
    parlayUncertainty: parlayProbStd,
    targetEdge,
    templateRampAdd,
    kSigma,
  });

  if (vig.offeredImpliedProb == null) return null;

  const americanOdds = americanFromImplied(vig.offeredImpliedProb);

  return {
    offeredImpliedProb: vig.offeredImpliedProb,
    offeredAmericanOdds: americanOdds,
    parlayFairProb: comb.parlayProb,
    parlayIndependentProb: comb.independent,
    correlationLift: comb.correlationLift,
    parlayUncertainty: parlayProbStd,
    effectiveEdge: vig.effectiveEdge,
    vigRateUsed: vig.vigRateUsed,
    legs: legsV2,
    breakdown: vig.breakdown,
    targetEdge,
  };
}

/**
 * Shadow-mode helper: given v1's priced parlay result + the same legs,
 * run v2 and log the comparison for later analysis. Never affects the
 * v1 offer.
 */
function shadowCompare({ parlayId, pricedLegs, v1OfferedAmericanOdds, v1FairParlayProb, opts }) {
  try {
    const v2 = priceParlayV2(pricedLegs, opts || {});
    if (!v2) return null;
    const record = {
      parlayId,
      at: new Date().toISOString(),
      v1: {
        offeredAmericanOdds: v1OfferedAmericanOdds,
        fairParlayProb: v1FairParlayProb,
      },
      v2: {
        offeredAmericanOdds: v2.offeredAmericanOdds,
        offeredImpliedProb: v2.offeredImpliedProb,
        parlayFairProb: v2.parlayFairProb,
        parlayIndependentProb: v2.parlayIndependentProb,
        correlationLift: v2.correlationLift,
        parlayUncertainty: v2.parlayUncertainty,
        effectiveEdge: v2.effectiveEdge,
        vigRateUsed: v2.vigRateUsed,
        calibrationApplied: v2.legs.some(l => Math.abs(l.calibrationCorrection || 0) > 0.001),
      },
      delta: {
        americanOddsDelta: v2.offeredAmericanOdds != null && v1OfferedAmericanOdds != null
          ? v2.offeredAmericanOdds - v1OfferedAmericanOdds
          : null,
      },
    };
    recordShadow(record);
    return record;
  } catch (err) {
    log.warn('V2Pricing', `shadowCompare failed: ${err.message}`);
    return null;
  }
}

/**
 * Report shadow-mode stats for the /v2-shadow-stats endpoint.
 */
function getShadowStats() {
  const n = _shadowLog.length;
  if (n === 0) return { n: 0 };
  const deltas = _shadowLog.map(r => r.delta.americanOddsDelta).filter(d => d != null);
  const v1Fair = _shadowLog.map(r => r.v1.fairParlayProb).filter(x => x != null);
  const v2Fair = _shadowLog.map(r => r.v2.parlayFairProb).filter(x => x != null);
  const calibApplied = _shadowLog.filter(r => r.v2.calibrationApplied).length;
  const corrLifts = _shadowLog.map(r => r.v2.correlationLift).filter(x => x != null && x !== 0);

  function median(arr) {
    if (!arr.length) return null;
    const s = [...arr].sort((a, b) => a - b);
    return s[Math.floor(s.length / 2)];
  }

  return {
    n,
    calibrationApplied,
    correlationTouched: corrLifts.length,
    medianAmericanOddsDelta: median(deltas),
    medianV1FairParlay: median(v1Fair),
    medianV2FairParlay: median(v2Fair),
    medianCorrelationLift: median(corrLifts),
    recent: _shadowLog.slice(-10),
  };
}

module.exports = {
  priceParlayV2,
  shadowCompare,
  getShadowStats,
  // Re-exports for testing + endpoints
  calibration,
  estimator,
  correlation,
  evVig,
};
