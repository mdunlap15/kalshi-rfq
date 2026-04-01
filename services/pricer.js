const { config } = require('../config');
const log = require('./logger');
const lineManager = require('./line-manager');
const oddsFeed = require('./odds-feed');
const orderTracker = require('./order-tracker');

/**
 * Convert decimal odds to American odds (integer).
 * PX uses American odds throughout its API.
 */
function decimalToAmerican(dec) {
  if (!dec || dec <= 1) return 0;
  if (dec >= 2.0) return Math.round((dec - 1) * 100);  // +150, +286, etc.
  return Math.round(-100 / (dec - 1));                   // -200, -150, etc.
}

// ---------------------------------------------------------------------------
// PRICING ENGINE
// ---------------------------------------------------------------------------

/**
 * Price a parlay given an array of legs (line_ids from the RFQ).
 *
 * Returns null if the parlay should be declined.
 * Returns an offer object if we can price it.
 */
async function priceParlay(legs) {
  // Validate leg count
  if (!legs || legs.length === 0) {
    log.debug('Pricing', 'Declined: no legs');
    return null;
  }
  if (legs.length > config.pricing.maxLegs) {
    log.debug('Pricing', `Declined: ${legs.length} legs exceeds max ${config.pricing.maxLegs}`);
    return null;
  }

  // Look up and price each leg
  const pricedLegs = [];
  let fairParlayProb = 1.0;

  for (const leg of legs) {
    const lineId = leg.line_id || leg.lineId || leg;
    const lineInfo = lineManager.lookupLine(lineId);

    if (!lineInfo) {
      log.debug('Pricing', `Declined: unknown line_id ${lineId}`);
      return null;
    }

    // Check if event has already started — don't quote with stale pre-game odds
    if (lineInfo.startTime) {
      const startMs = new Date(lineInfo.startTime).getTime();
      if (!isNaN(startMs) && Date.now() > startMs) {
        log.debug('Pricing', `Declined: event already started (${lineInfo.teamName}, started ${lineInfo.startTime})`);
        return null;
      }
    }

    // Check if prices are stale for this sport
    if (oddsFeed.isStale(lineInfo.sport)) {
      log.debug('Pricing', `Declined: stale prices for ${lineInfo.sport} (${Math.round(oddsFeed.getCacheAge(lineInfo.sport))}min old)`);
      return null;
    }

    // Get fair probability — tries cache first, then on-demand alt lines fetch
    const fairProb = await oddsFeed.getFairProbAsync(
      lineInfo.oddsApiSport,
      lineInfo.homeTeam,
      lineInfo.awayTeam,
      lineInfo.oddsApiMarket,
      lineInfo.oddsApiSelection,
      lineInfo.line != null ? Math.abs(lineInfo.line) : null
    );

    if (fairProb == null || fairProb <= 0 || fairProb >= 1) {
      log.debug('Pricing', `Declined: no fair value for ${lineInfo.teamName} ${lineInfo.marketType}`);
      return null;
    }

    pricedLegs.push({
      lineId,
      lineInfo,
      fairProb,
    });

    fairParlayProb *= fairProb;
  }

  // Sanity check — if fair parlay prob is extremely small, decline
  if (fairParlayProb < 0.001) {
    log.debug('Pricing', `Declined: fair parlay prob too small (${fairParlayProb.toFixed(6)})`);
    return null;
  }

  // Apply vig — this makes the price worse for the bettor (higher implied prob)
  const vig = config.pricing.defaultVig;
  const offeredImpliedProb = fairParlayProb * (1 + vig);

  // Cap at 0.99 (can't offer 100%+ implied)
  const cappedProb = Math.min(offeredImpliedProb, 0.99);

  // Convert to decimal odds
  const decimalOdds = 1 / cappedProb;

  // Determine max risk
  const maxRisk = config.pricing.maxRiskPerParlay;

  // Convert to American odds (PX uses American integers throughout)
  const americanOdds = decimalToAmerican(decimalOdds);

  // Build estimated_price (per-leg breakdown) — also in American
  const estimatedPrice = pricedLegs.map(leg => ({
    line_id: leg.lineId,
    odds: decimalToAmerican(1 / leg.fairProb),
  }));

  // valid_until in nanoseconds
  const validUntil = Math.floor((Date.now() / 1000 + config.pricing.offerValidSeconds) * 1e9);

  return {
    offer: {
      valid_until: validUntil,
      odds: americanOdds,
      max_risk: maxRisk,
      estimated_price: estimatedPrice,
    },
    meta: {
      legs: pricedLegs.map(l => {
        // For totals/spreads, build a descriptive label: "Over 6.5 (NYM vs CHC)"
        let team = l.lineInfo.teamName;
        const event = l.lineInfo.pxEventName || '';
        if (l.lineInfo.marketType === 'total' && l.lineInfo.homeTeam && l.lineInfo.awayTeam) {
          team = `${team} (${l.lineInfo.awayTeam} @ ${l.lineInfo.homeTeam})`;
        }
        return {
          lineId: l.lineId,
          team,
          market: l.lineInfo.marketType,
          selection: l.lineInfo.oddsApiSelection,
          line: l.lineInfo.line,
          fairProb: Math.round(l.fairProb * 10000) / 10000,
          sport: l.lineInfo.sport,
          homeTeam: l.lineInfo.homeTeam,
          awayTeam: l.lineInfo.awayTeam,
        };
      }),
      fairParlayProb: Math.round(fairParlayProb * 100000) / 100000,
      offeredImpliedProb: Math.round(cappedProb * 100000) / 100000,
      decimalOdds: Math.round(decimalOdds * 100) / 100,
      americanOdds,
      vig,
      maxRisk,
    },
  };
}

/**
 * Build multiple tier offers for a parlay.
 * Tighter odds at low risk, wider at high risk.
 */
async function buildOffers(legs) {
  const base = await priceParlay(legs);
  if (!base) return null;

  const tiers = [
    { vigMultiplier: 1.0,  riskMultiplier: 0.5, label: 'tight' },
    { vigMultiplier: 1.2,  riskMultiplier: 1.0, label: 'medium' },
    { vigMultiplier: 1.5,  riskMultiplier: 2.0, label: 'wide' },
  ];

  const offers = [];
  for (const tier of tiers) {
    const vig = config.pricing.defaultVig * tier.vigMultiplier;
    const offeredProb = Math.min(base.meta.fairParlayProb * (1 + vig), 0.99);
    const decOdds = 1 / offeredProb;
    const maxRisk = config.pricing.maxRiskPerParlay * tier.riskMultiplier;

    const validUntil = Math.floor((Date.now() / 1000 + config.pricing.offerValidSeconds) * 1e9);

    offers.push({
      valid_until: validUntil,
      odds: Math.round(decOdds * 100) / 100,
      max_risk: maxRisk,
      estimated_price: base.offer.estimated_price,
    });
  }

  return {
    offers,
    meta: base.meta,
  };
}

/**
 * Quick check if we should even attempt to price this parlay.
 */
function shouldDecline(legs) {
  if (!legs || legs.length === 0) return true;
  if (legs.length > config.pricing.maxLegs) return true;

  // Check all legs are known and events haven't started
  const resolvedLegs = [];
  for (const leg of legs) {
    const lineId = leg.line_id || leg.lineId || leg;
    const lineInfo = lineManager.lookupLine(lineId);
    if (!lineInfo) return true;

    // Reject if event has already started
    if (lineInfo.startTime) {
      const startMs = new Date(lineInfo.startTime).getTime();
      if (!isNaN(startMs) && Date.now() > startMs) return true;
    }

    resolvedLegs.push({ lineId, lineInfo });
  }

  // Reject same-game parlays — legs from the same event are correlated,
  // independent probability multiplication gives wrong fair value
  const eventIds = resolvedLegs.map(l => l.lineInfo.pxEventId).filter(Boolean);
  if (eventIds.length !== new Set(eventIds).size) {
    log.info('Pricing', 'Declined: same-game parlay detected (correlated legs)');
    return true;
  }

  // Check team-level exposure limits
  const exposureCheck = orderTracker.checkExposureLimits(
    resolvedLegs.map(l => ({ team: l.lineInfo.teamName })),
    config.pricing.maxRiskPerParlay,
    config.pricing.maxExposurePerTeam
  );
  if (!exposureCheck.allowed) {
    log.info('Pricing', `Exposure limit: ${exposureCheck.reason}`);
    return true;
  }

  return false;
}

/**
 * Re-validate pricing at confirmation time.
 * Check if fair values have moved significantly since we quoted.
 */
async function validateForConfirmation(parlayId, originalMeta) {
  if (!originalMeta || !originalMeta.legs) return { valid: false, reason: 'no original meta' };

  const legs = originalMeta.legs.map(l => l.lineId);
  const currentPricing = await priceParlay(legs);
  if (!currentPricing) return { valid: false, reason: 'cannot reprice — missing data' };

  // Check if fair value has moved more than 5% against us
  const originalProb = originalMeta.fairParlayProb;
  const currentProb = currentPricing.meta.fairParlayProb;
  const drift = Math.abs(currentProb - originalProb) / originalProb;

  if (drift > 0.05) {
    log.warn('Pricing', `Price drift of ${(drift * 100).toFixed(1)}% since quote — rejecting confirmation`);
    return { valid: false, reason: `price drift ${(drift * 100).toFixed(1)}%`, currentPricing };
  }

  return { valid: true, currentPricing };
}

module.exports = {
  priceParlay,
  buildOffers,
  shouldDecline,
  validateForConfirmation,
};
