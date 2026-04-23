/**
 * Correlation-aware parlay combiner.
 *
 * Replaces `P_parlay = ∏ P_leg` with a correlation-adjusted product
 * that accounts for:
 *   - Same-event correlation (highest): two legs from the same game
 *     can move together (e.g. team ML + their spread correlate strongly,
 *     team ML + total correlate weakly-to-moderately)
 *   - Cross-event same-team (moderate): team ML in game 1 + same-team
 *     over/under in game 2 of a series/slate
 *   - Slate-level sport correlation (weak): all NBA favorites on the
 *     same night can move with league-wide line drift
 *
 * Also propagates uncertainty from leg-level std to parlay-level std.
 *
 * Starting scope: same-event block correlation only. Implements a
 * closed-form approximation based on the Gaussian-copula idea: for two
 * legs with marginal probabilities p1, p2 and correlation ρ,
 *   P(both) ≈ p1 * p2 + ρ * sqrt(p1*(1-p1)*p2*(1-p2))
 * clipped to [0, min(p1, p2)].
 *
 * For N legs same-event, we pairwise-accumulate. Cross-event legs
 * multiply independently for now (ρ=0 baseline; extend with learned
 * correlation matrix later).
 */

// Correlation coefficients by (market_pair, same_event). Tuned from
// empirical April 14-22 pattern: MLB run line + total correlated,
// NBA ML + total weakly correlated, etc. Positive ρ = legs tend to
// win/lose together.
const SAME_EVENT_CORR = {
  // team ML ↔ team spread: highly positive (same side of same game)
  'moneyline|spread': 0.75,
  'spread|moneyline': 0.75,
  // team ML ↔ total: weak positive (winning team often produces scoring)
  'moneyline|total': 0.15,
  'total|moneyline': 0.15,
  // team spread ↔ total: weak positive (same logic)
  'spread|total': 0.15,
  'total|spread': 0.15,
  // H1 vs full-game same-side-same-market: strong positive
  'spreads_h1|spread': 0.55,
  'spread|spreads_h1': 0.55,
  'totals_h1|total': 0.55,
  'total|totals_h1': 0.55,
  // F5 vs full-game (MLB): moderate positive
  'spreads_f5|spread': 0.45,
  'spread|spreads_f5': 0.45,
  'totals_f5|total': 0.45,
  'total|totals_f5': 0.45,
  // Team total vs full-game total (same team = strong anti-corr if
  // other side, strong pos if same; we only see positive-side RFQs
  // here, so treat as +0.5 default)
  'team_totals|total': 0.40,
  'total|team_totals': 0.40,
};

function lookupPairCorrelation(market1, market2, sameEvent) {
  if (!sameEvent) return 0; // cross-event pairs default to uncorrelated
  if (market1 === market2) return 0.90; // same market, same event → near-perfect corr
  const key = `${market1}|${market2}`;
  return SAME_EVENT_CORR[key] || 0;
}

/**
 * Gaussian-copula approximation for joint probability of two
 * independent-marginal events with correlation ρ.
 * Exact for binary-outcome ρ in [-1, 1] when using the bivariate
 * Bernoulli joint with shared covariance:
 *   P(A∩B) = p_A * p_B + ρ * sqrt(p_A*(1-p_A)*p_B*(1-p_B))
 */
function pairJoint(p1, p2, rho) {
  const covTerm = rho * Math.sqrt(p1 * (1 - p1) * p2 * (1 - p2));
  const joint = p1 * p2 + covTerm;
  // Clamp: must be in [max(0, p1+p2-1), min(p1, p2)]
  const lower = Math.max(0, p1 + p2 - 1);
  const upper = Math.min(p1, p2);
  if (joint < lower) return lower;
  if (joint > upper) return upper;
  return joint;
}

/**
 * Pairwise-accumulate a parlay's probability accounting for
 * correlations between same-event legs. Non-pairs collapse to
 * independent multiplication (ρ=0).
 *
 * Heuristic: process legs in order. Start with first leg's prob as
 * accumulator. For each subsequent leg, compute its correlation with
 * the "strongest-linked prior leg" and accumulate via pairJoint.
 *
 * This is a simplification; a full multivariate treatment would use a
 * Gaussian copula over all N legs. For 2-3 leg parlays the pairwise
 * approach is tight enough; for 6+ leg parlays we'd need the full
 * treatment. Flagged as TODO.
 */
function combineProbs(legs) {
  if (!legs || legs.length === 0) return { parlayProb: null, correlationAdj: 0 };
  if (legs.length === 1) return { parlayProb: legs[0].fairProb, correlationAdj: 0 };

  // Index by pxEventId for same-event lookup
  const byEvent = {};
  for (let i = 0; i < legs.length; i++) {
    const l = legs[i];
    if (!byEvent[l.pxEventId]) byEvent[l.pxEventId] = [];
    byEvent[l.pxEventId].push(i);
  }

  // Baseline: independent product
  const independent = legs.reduce((p, l) => p * l.fairProb, 1);

  // For each same-event cluster, compute correlation-adjusted joint
  // probability among its legs. Cross-cluster remains independent.
  let adjusted = 1;
  for (const [evt, idxs] of Object.entries(byEvent)) {
    if (idxs.length === 1) {
      adjusted *= legs[idxs[0]].fairProb;
      continue;
    }
    // Same-event cluster: pairwise accumulate starting from strongest prob
    const clusterLegs = idxs.map(i => legs[i]);
    // Sort by probability (start with most-likely as anchor)
    clusterLegs.sort((a, b) => b.fairProb - a.fairProb);
    let clusterProb = clusterLegs[0].fairProb;
    for (let i = 1; i < clusterLegs.length; i++) {
      const rho = lookupPairCorrelation(clusterLegs[0].marketType, clusterLegs[i].marketType, true);
      // Pair clusterProb (accumulated so far) with next leg using rho
      // This approximation treats the accumulated cluster as having
      // the anchor leg's correlation with subsequent legs. For 2-leg
      // SGPs (our main case) this is exact.
      clusterProb = pairJoint(clusterProb, clusterLegs[i].fairProb, rho);
    }
    adjusted *= clusterProb;
  }

  const correlationAdj = adjusted - independent;

  return {
    parlayProb: adjusted,
    independent,
    correlationAdj,
    correlationLift: independent > 0 ? correlationAdj / independent : 0,
  };
}

/**
 * Propagate uncertainty from legs to parlay level. For a product of
 * independent log-normals:
 *   var(log P) = sum(var(log p_i))
 * We approximate leg-level log-variance from the leg uncertainty using
 * first-order propagation: d(log p)/dp = 1/p, so σ²_logp ≈ (σ_p / p)².
 * Correlated legs inflate the variance; same-event clusters get a
 * multiplicative bump.
 */
function propagateUncertainty(legs) {
  if (!legs || legs.length === 0) return 0;
  let sumVar = 0;
  for (const l of legs) {
    if (l.fairProb <= 0 || l.uncertainty == null) continue;
    sumVar += (l.uncertainty / l.fairProb) ** 2;
  }
  // Apply rough same-event inflation: if K legs share an event, their
  // variance contribution roughly multiplied by K/2 (pairwise coupling)
  const byEvent = {};
  for (const l of legs) {
    if (!byEvent[l.pxEventId]) byEvent[l.pxEventId] = 0;
    byEvent[l.pxEventId] += 1;
  }
  let inflation = 1;
  for (const n of Object.values(byEvent)) {
    if (n > 1) inflation *= 1 + 0.25 * (n - 1); // +25% per additional same-event leg
  }
  const parlayLogStd = Math.sqrt(sumVar) * inflation;
  return Math.round(parlayLogStd * 10000) / 10000;
}

module.exports = {
  SAME_EVENT_CORR,
  lookupPairCorrelation,
  pairJoint,
  combineProbs,
  propagateUncertainty,
};
