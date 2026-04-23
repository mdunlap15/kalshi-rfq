/**
 * EV-targeted vig solver.
 *
 * Replaces the v1 vig stack (DEFAULT_VIG + VIG_BY_SPORT +
 * VIG_FAVORITE_* + VIG_LONGSHOT_* + SGP_VIG_MULTIPLIER + series/MMA
 * floors) with a single knob: TARGET_EDGE. Everything else is derived.
 *
 * Semantics:
 *   TARGET_EDGE = expected EV per $1 of bettor stake.
 *   e.g. 0.02 = we want to net 2¢ per $1 they risk on average.
 *
 * Vig formula (closed form):
 *   Given parlay_prob (our corrected estimate), target_edge, and
 *   parlay_std (our uncertainty), we want to offer odds such that:
 *     EV_SP = (1 - p) * wager_collected - p * stake_paid_out
 *
 *   For a bettor stake S at offered American odds O, wager_collected
 *   is S (their stake becomes ours on loss), stake_paid_out is
 *   S * decimal_payout (where decimal_payout = (1/offered_implied) - 1)
 *   on their win.
 *
 *   EV_SP per $1 of stake:
 *     = (1 - p) * 1 - p * ((1/q) - 1)    where q = offered_implied
 *     = (1 - p) - p/q + p
 *     = 1 - p/q
 *
 *   We want EV_SP >= TARGET_EDGE:
 *     1 - p/q >= TARGET_EDGE
 *     q >= p / (1 - TARGET_EDGE)
 *
 *   So offered_implied = p / (1 - TARGET_EDGE)
 *
 * Uncertainty adjustment:
 *   Our p is an estimate with std. Worst-case sensitivity favors
 *   slightly inflating the effective p for quoting, so we price against
 *   p_quote = p + k * std (where k is tunable, default 0.5 = half-sigma
 *   conservative shift). This naturally widens vig when uncertainty
 *   is high (thin markets, small calibration samples, heavy favorites
 *   with sparse books).
 *
 * Template adjustment (additive to final vig):
 *   templateRampAdd from template-exposure.js is ADDED to the
 *   computed vig as a structural exposure widening. Kept separate
 *   because it's about position concentration, not pricing uncertainty.
 *
 * Return: `{ offeredImpliedProb, effectiveEdge, vigRateUsed, breakdown }`.
 */

/**
 * Conservative-shift p: inflate fair prob by a fraction of its
 * uncertainty to price against the worst-case-for-us estimate.
 */
function conservativeP(fairProb, uncertainty, k = 0.5) {
  if (fairProb == null || !(fairProb > 0 && fairProb < 1)) return fairProb;
  const shifted = fairProb + k * (uncertainty || 0);
  return Math.max(0.01, Math.min(0.99, shifted));
}

/**
 * Solve offered implied prob from a target edge.
 */
function solveVig({ parlayFairProb, parlayUncertainty, targetEdge, templateRampAdd = 0, kSigma = 0.5 }) {
  if (parlayFairProb == null || !(parlayFairProb > 0 && parlayFairProb < 1)) {
    return {
      offeredImpliedProb: null,
      error: 'invalid parlayFairProb',
    };
  }
  if (targetEdge < 0 || targetEdge >= 1) {
    return {
      offeredImpliedProb: null,
      error: 'invalid targetEdge',
    };
  }

  // Conservative p with uncertainty shift
  const pConservative = conservativeP(parlayFairProb, parlayUncertainty, kSigma);

  // Base vig from target edge
  // offered_implied = p / (1 - targetEdge)
  let offered = pConservative / (1 - targetEdge);

  // Add template ramp as additive vig widening (applied multiplicatively
  // on the payout, same as v1 applyOddsVig)
  if (templateRampAdd > 0) {
    // Convert: offered_with_template = 1 / (1 + payout * (1 - templateRampAdd))
    // where payout = (1/offered) - 1
    const payout = (1 / offered) - 1;
    const widenedPayout = payout * (1 - templateRampAdd);
    offered = 1 / (1 + widenedPayout);
  }

  // Clamp to sensible range
  offered = Math.max(0.01, Math.min(0.99, offered));

  // Compute effective realized edge (from fairProb perspective, not conservative)
  // effectiveEdge = 1 - fairProb / offered
  const effectiveEdge = 1 - parlayFairProb / offered;

  // Nominal vig rate (for analytics)
  // vigRateUsed = (offered - fair) / fair in prob-space, roughly
  const vigRateUsed = offered > 0 ? (offered - parlayFairProb) / parlayFairProb : 0;

  return {
    offeredImpliedProb: Math.round(offered * 100000) / 100000,
    effectiveEdge: Math.round(effectiveEdge * 10000) / 10000,
    vigRateUsed: Math.round(vigRateUsed * 10000) / 10000,
    breakdown: {
      parlayFairProb: Math.round(parlayFairProb * 100000) / 100000,
      pConservative: Math.round(pConservative * 100000) / 100000,
      parlayUncertainty: Math.round((parlayUncertainty || 0) * 10000) / 10000,
      kSigma,
      targetEdge,
      templateRampAdd: Math.round(templateRampAdd * 10000) / 10000,
    },
  };
}

module.exports = {
  solveVig,
  conservativeP,
};
