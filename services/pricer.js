const { config, getBankroll } = require('../config');
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
  priceParlay._lastFailure = null; // clear any prior failure
  // Validate leg count
  if (!legs || legs.length === 0) {
    log.debug('Pricing', 'Declined: no legs');
    priceParlay._lastFailure = { reason: 'empty', detail: null, blockerLeg: null };
    return null;
  }
  if (legs.length > config.pricing.maxLegs) {
    log.debug('Pricing', `Declined: ${legs.length} legs exceeds max ${config.pricing.maxLegs}`);
    priceParlay._lastFailure = { reason: 'too many legs', detail: `${legs.length} > max ${config.pricing.maxLegs}`, blockerLeg: null };
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
      priceParlay._lastFailure = { reason: 'unknown line', detail: null, blockerLeg: null };
      return null;
    }

    const legLabel = `${lineInfo.teamName || '?'} (${lineInfo.marketType || '?'}${lineInfo.line != null ? ' ' + lineInfo.line : ''})`;
    const legDescriptor = {
      team: lineInfo.teamName,
      market: lineInfo.marketType,
      line: lineInfo.line,
      sport: lineInfo.sport,
      homeTeam: lineInfo.homeTeam,
      awayTeam: lineInfo.awayTeam,
    };

    // Check if event has already started — don't quote with stale pre-game odds
    if (lineInfo.startTime) {
      const startMs = new Date(lineInfo.startTime).getTime();
      if (!isNaN(startMs) && Date.now() > startMs) {
        log.debug('Pricing', `Declined: event already started (${lineInfo.teamName}, started ${lineInfo.startTime})`);
        priceParlay._lastFailure = { reason: 'event started', detail: `${legLabel} already in progress`, blockerLeg: legDescriptor };
        return null;
      }
    }

    // Check if prices are stale for this sport
    if (oddsFeed.isStale(lineInfo.sport)) {
      const ageMin = Math.round(oddsFeed.getCacheAge(lineInfo.sport));
      log.debug('Pricing', `Declined: stale prices for ${lineInfo.sport} (${ageMin}min old)`);
      priceParlay._lastFailure = { reason: 'stale odds', detail: `${lineInfo.sport} odds ${ageMin}m old`, blockerLeg: legDescriptor };
      return null;
    }

    // Get fair probability — tries cache first, then on-demand alt lines fetch
    const fairProb = await oddsFeed.getFairProbAsync(
      lineInfo.oddsApiSport,
      lineInfo.homeTeam,
      lineInfo.awayTeam,
      lineInfo.oddsApiMarket,
      lineInfo.oddsApiSelection,
      lineInfo.line != null ? Math.abs(lineInfo.line) : null,
      lineInfo.startTime // for back-to-back/doubleheader matching
    );

    if (fairProb == null || fairProb <= 0 || fairProb >= 1) {
      log.debug('Pricing', `Declined: no fair value for ${lineInfo.teamName} ${lineInfo.marketType}`);
      priceParlay._lastFailure = {
        reason: 'no fair value',
        detail: `no ${lineInfo.oddsApiMarket || lineInfo.marketType} quote for ${legLabel} in our odds feed`,
        blockerLeg: legDescriptor,
      };
      return null;
    }

    // Look up sportsbook raw odds for this leg
    const pinnacleOdds = oddsFeed.getPinnacleOdds(
      lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
      lineInfo.oddsApiMarket, lineInfo.oddsApiSelection, lineInfo.startTime
    );
    const fanduelOdds = oddsFeed.getFanDuelOdds(
      lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
      lineInfo.oddsApiMarket, lineInfo.oddsApiSelection, lineInfo.startTime
    );

    pricedLegs.push({
      lineId,
      lineInfo,
      fairProb,
      pinnacleOdds,
      fanduelOdds,
    });

    fairParlayProb *= fairProb;
  }

  // Sanity check — if fair parlay prob is extremely small, decline
  if (fairParlayProb < 0.001) {
    log.debug('Pricing', `Declined: fair parlay prob too small (${fairParlayProb.toFixed(6)})`);
    priceParlay._lastFailure = {
      reason: 'parlay too unlikely',
      detail: `combined fair prob ${(fairParlayProb * 100).toFixed(3)}% < 0.1% threshold`,
      blockerLeg: null,
    };
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

  // PX rejects decimal odds with "invalid odds" 400 error.
  // PX web UI and matched orders all use American format.
  const americanOdds = decimalToAmerican(decimalOdds);

  // Don't quote on very high odds parlays — even small stakes create huge payouts
  const maxOdds = config.pricing.maxOdds || 1000;
  if (americanOdds > maxOdds) {
    log.debug('Pricing', `Declined: odds +${americanOdds} exceed max +${maxOdds}`);
    priceParlay._lastFailure = {
      reason: 'odds too high',
      detail: `offered +${americanOdds} > max +${maxOdds}`,
      blockerLeg: null,
    };
    return null;
  }

  // Build estimated_price (per-leg breakdown) — American odds per leg
  const estimatedPrice = pricedLegs.map(leg => ({
    line_id: leg.lineId,
    odds: decimalToAmerican(1 / leg.fairProb),
  }));

  // valid_until in nanoseconds
  const validUntil = Math.floor((Date.now() / 1000 + config.pricing.offerValidSeconds) * 1e9);

  return {
    offer: {
      valid_until: validUntil,
      odds: americanOdds, // American odds for PX
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
          pinnacleOdds: l.pinnacleOdds || null,
          fanduelOdds: l.fanduelOdds || null,
          sport: l.lineInfo.sport,
          homeTeam: l.lineInfo.homeTeam,
          awayTeam: l.lineInfo.awayTeam,
          startTime: l.lineInfo.startTime || null,
          pxEventId: l.lineInfo.pxEventId || null,
        };
      }),
      fairParlayProb: Math.round(fairParlayProb * 100000) / 100000,
      offeredImpliedProb: Math.round(cappedProb * 100000) / 100000,
      decimalOdds: Math.round(decimalOdds * 100) / 100,
      americanOdds,
      // Compute Pinnacle parlay odds from per-leg Pinnacle implied probs
      pinnacleParlay: (() => {
        const pinLegs = pricedLegs.filter(l => l.pinnacleOdds != null);
        if (pinLegs.length !== pricedLegs.length) return null; // need all legs
        let pinProb = 1;
        for (const l of pinLegs) {
          pinProb *= oddsFeed.americanToImpliedProb(l.pinnacleOdds);
        }
        if (pinProb <= 0 || pinProb >= 1) return null;
        return decimalToAmerican(1 / pinProb);
      })(),
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
      odds: decimalToAmerican(decOdds),
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
  if (!legs || legs.length === 0) return { declined: true, reason: 'empty parlay', detail: null };
  if (legs.length > config.pricing.maxLegs) {
    return { declined: true, reason: 'too many legs', detail: `${legs.length} legs > max ${config.pricing.maxLegs}` };
  }

  // Check all legs are known and events haven't started
  const resolvedLegs = [];
  for (const leg of legs) {
    const lineId = leg.line_id || leg.lineId || leg;
    const lineInfo = lineManager.lookupLine(lineId);
    if (!lineInfo) return { declined: true, reason: 'unknown legs', detail: null };

    // Reject if event has already started
    if (lineInfo.startTime) {
      const startMs = new Date(lineInfo.startTime).getTime();
      if (!isNaN(startMs) && Date.now() > startMs) {
        return { declined: true, reason: 'event started', detail: `${lineInfo.teamName || '?'} (${lineInfo.sport || '?'}) already in progress` };
      }
    }

    resolvedLegs.push({ lineId, lineInfo });
  }

  // Check for correlated same-game legs.
  // Allowed: spread/moneyline + total on same game (low correlation)
  // Blocked: spread + moneyline on same game (high correlation)
  // Blocked: two of the same market type on same game
  const byEvent = {};
  for (const l of resolvedLegs) {
    const eid = l.lineInfo.pxEventId;
    if (!eid) continue;
    if (!byEvent[eid]) byEvent[eid] = [];
    byEvent[eid].push({ market: l.lineInfo.marketType, team: l.lineInfo.teamName, home: l.lineInfo.homeTeam, away: l.lineInfo.awayTeam });
  }
  for (const [eid, entries] of Object.entries(byEvent)) {
    if (entries.length <= 1) continue;
    const types = entries.map(e => e.market);
    const gameLabel = entries[0].away && entries[0].home ? `${entries[0].away} @ ${entries[0].home}` : `event ${eid}`;
    const hasSpread = types.includes('spread');
    const hasMoneyline = types.includes('moneyline');
    const uniqueTypes = new Set(types);
    // Block: two of the same type on same game
    if (uniqueTypes.size < types.length) {
      const dup = types.find((t, i) => types.indexOf(t) !== i);
      log.info('Pricing', `Declined: duplicate ${dup} on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `two ${dup} legs on same game: ${gameLabel}` };
    }
    // Block: spread + moneyline (highly correlated)
    if (hasSpread && hasMoneyline) {
      log.info('Pricing', `Declined: spread + moneyline on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `spread + moneyline on same game: ${gameLabel}` };
    }
    // Allow: spread/moneyline + total (acceptable correlation)
  }

  // Check team-level exposure limits
  // Estimate payout for exposure check (max_risk is our max payout)
  const estPayout = config.pricing.maxRiskPerParlay;
  // Pass legs with fairProb info for weighted calculation
  const legsWithProb = resolvedLegs.map(l => {
    const fp = oddsFeed.getFairProb(
      l.lineInfo.oddsApiSport, l.lineInfo.homeTeam, l.lineInfo.awayTeam,
      l.lineInfo.oddsApiMarket, l.lineInfo.oddsApiSelection,
      l.lineInfo.line != null ? Math.abs(l.lineInfo.line) : null, l.lineInfo.startTime
    );
    return { team: l.lineInfo.teamName, fairProb: fp || 0.5 };
  });
  const exposureCheck = orderTracker.checkExposureLimits(
    legsWithProb, estPayout, config.pricing.maxExposurePerTeam
  );
  if (!exposureCheck.allowed) {
    log.info('Pricing', `Exposure limit: ${exposureCheck.reason}`);
    return { declined: true, reason: 'team exposure limit', detail: exposureCheck.reason };
  }

  // Check game-level exposure limit
  const maxPerGame = getBankroll() * config.pricing.maxExposurePerGamePct / 100;
  const gameCheck = orderTracker.checkGameExposure(resolvedLegs, estPayout, maxPerGame);
  if (!gameCheck.allowed) {
    log.info('Pricing', `Game exposure limit: ${gameCheck.reason}`);
    return { declined: true, reason: 'game exposure limit', detail: gameCheck.reason };
  }

  // Check portfolio-level drawdown limit
  const maxDrawdown = getBankroll() * config.pricing.maxDrawdownPct / 100;
  const portfolioCheck = orderTracker.checkPortfolioRisk(estPayout, maxDrawdown);
  if (!portfolioCheck.allowed) {
    log.info('Pricing', `Portfolio risk limit: $${portfolioCheck.current.toFixed(0)} + $${estPayout.toFixed(0)} > max $${portfolioCheck.limit.toFixed(0)}`);
    return {
      declined: true,
      reason: 'portfolio drawdown limit',
      detail: `$${portfolioCheck.current.toFixed(0)} current + $${estPayout.toFixed(0)} new > $${portfolioCheck.limit.toFixed(0)} max`,
    };
  }

  return { declined: false };
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

function getLastPriceFailure() {
  return priceParlay._lastFailure || null;
}

module.exports = {
  priceParlay,
  buildOffers,
  shouldDecline,
  validateForConfirmation,
  getLastPriceFailure,
};
