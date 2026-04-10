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

    // Check if event has already started — don't quote with stale pre-game odds.
    // Golf matchups exempt (no commenceTime from DataGolf; isStale check covers).
    const isGolfMatchup = lineInfo.sport === 'golf_matchups' || lineInfo.oddsApiSport === 'golf_matchups';
    if (lineInfo.startTime) {
      const startMs = new Date(lineInfo.startTime).getTime();
      if (isNaN(startMs)) {
        log.debug('Pricing', `Declined: invalid startTime for ${lineInfo.teamName} (${lineInfo.startTime})`);
        priceParlay._lastFailure = { reason: 'unknown start time', detail: `${legLabel} has invalid startTime ${lineInfo.startTime}`, blockerLeg: legDescriptor };
        return null;
      }
      if (Date.now() > startMs) {
        log.debug('Pricing', `Declined: event already started (${lineInfo.teamName}, started ${lineInfo.startTime})`);
        priceParlay._lastFailure = { reason: 'event started', detail: `${legLabel} already in progress`, blockerLeg: legDescriptor };
        return null;
      }
    } else if (!isGolfMatchup) {
      log.debug('Pricing', `Declined: null startTime for ${lineInfo.teamName} (${lineInfo.sport})`);
      priceParlay._lastFailure = { reason: 'unknown start time', detail: `${legLabel} has no startTime — cannot verify game hasn't started`, blockerLeg: legDescriptor };
      return null;
    }

    // Check if prices are stale for this sport
    if (oddsFeed.isStale(lineInfo.sport)) {
      const ageMin = Math.round(oddsFeed.getCacheAge(lineInfo.sport));
      log.debug('Pricing', `Declined: stale prices for ${lineInfo.sport} (${ageMin}min old)`);
      priceParlay._lastFailure = { reason: 'stale odds', detail: `${lineInfo.sport} odds ${ageMin}m old`, blockerLeg: legDescriptor };
      return null;
    }

    // Get fair probability — tries cache first, then on-demand alt lines fetch.
    // For Draw No Bet (2-way soccer moneyline), derive from 3-way h2h by
    // removing the draw probability and renormalizing.
    let fairProb;
    if (lineInfo.isDNB) {
      fairProb = oddsFeed.getDNBFairProb(
        lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
        lineInfo.oddsApiSelection, lineInfo.startTime
      );
      if (fairProb != null) {
        log.debug('Pricing', `DNB derived fair prob ${fairProb.toFixed(4)} for ${legLabel}`);
      }
    } else {
      fairProb = await oddsFeed.getFairProbAsync(
        lineInfo.oddsApiSport,
        lineInfo.homeTeam,
        lineInfo.awayTeam,
        lineInfo.oddsApiMarket,
        lineInfo.oddsApiSelection,
        lineInfo.line,
        lineInfo.startTime
      );
    }

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
    const kalshiOdds = oddsFeed.getKalshiOdds(
      lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
      lineInfo.oddsApiMarket, lineInfo.oddsApiSelection, lineInfo.startTime
    );
    const draftkingsOdds = oddsFeed.getDraftKingsOdds(
      lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
      lineInfo.oddsApiMarket, lineInfo.oddsApiSelection, lineInfo.startTime
    );

    // Require a valid fair probability — if we have one, sportsbook data exists
    // (fair prob is built from de-vigged consensus of all available books).
    // Previously required specifically Pinnacle or FanDuel, but with 25 books
    // from SharpAPI, any book's data in the consensus is sufficient.
    // The fairProb null check above (line 107) already catches truly blind legs.

    // Spread/total line verification: when the requested line matches our cached
    // primary, spot-check Pinnacle to confirm the line hasn't moved. Prevents
    // pricing stale alt-spreads at primary-spread odds.
    if ((lineInfo.oddsApiMarket === 'spreads' || lineInfo.oddsApiMarket === 'totals') && lineInfo.line != null) {
      const event = oddsFeed.getEventMarkets(
        lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam, lineInfo.startTime
      );
      const cachedLine = event?.markets?.[lineInfo.oddsApiMarket]?.line;
      if (cachedLine != null && Math.abs(Math.abs(cachedLine) - Math.abs(lineInfo.line)) < 0.01) {
        // Requested line matches our cached primary — verify it hasn't moved
        const verify = await oddsFeed.verifyLineWithPinnacle(
          lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
          lineInfo.oddsApiMarket, cachedLine
        );
        if (!verify.ok) {
          log.info('Pricing', `Declined: spread line moved for ${legLabel} (cached ${cachedLine}, Pinnacle now ${verify.currentLine})`);
          priceParlay._lastFailure = {
            reason: 'spread line moved',
            detail: `${legLabel}: cached line ${cachedLine} but Pinnacle now ${verify.currentLine} (moved ${verify.diff.toFixed(1)}pts)`,
            blockerLeg: legDescriptor,
          };
          return null;
        }
      }
    }

    // Get de-vigged consensus fair prob for display (separate from pricing fairProb)
    const displayFairProb = oddsFeed.getDisplayFairProb(
      lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
      lineInfo.oddsApiMarket, lineInfo.oddsApiSelection,
      lineInfo.line != null ? Math.abs(lineInfo.line) : null,
      lineInfo.startTime
    );

    pricedLegs.push({
      lineId,
      lineInfo,
      fairProb,
      displayFairProb,
      pinnacleOdds,
      fanduelOdds,
      kalshiOdds,
      draftkingsOdds,
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

  // Apply vig to ODDS (multiplicative) rather than probability (additive).
  // This scales naturally: a 3% vig on -700 reduces the payout by 3%,
  // not by a fixed probability shift that distorts at extreme odds.
  //
  // Method: for each leg, compute fair decimal odds (1/fairProb), then
  // reduce the payout portion by (1 - vig). Compound across legs.
  //
  // Example at 3% vig:
  //   Fair -700 (decimal 1.143): payout = 0.143, vigged = 0.143 × 0.97 = 0.139 → decimal 1.139 → -718
  //   Fair +150 (decimal 2.50):  payout = 1.50,  vigged = 1.50  × 0.97 = 1.455 → decimal 2.455 → +146
  //   Consistent % reduction in payout regardless of odds level.
  const baseVig = config.pricing.defaultVig;

  // Tiered vig: scaled proportionally above base.
  // Gentle bumps on heavy favorites to prevent compounding leaks
  // without killing competitiveness on normal parlays.
  function getEffectiveVig(fairProb) {
    if (fairProb >= 0.80) return Math.max(baseVig, 0.03);  // extreme favorites (-400+)
    if (fairProb >= 0.70) return Math.max(baseVig, 0.025); // heavy favorites (-230+)
    return baseVig;                                         // everything else keeps base vig
  }

  function applyOddsVig(fairProb) {
    const vig = getEffectiveVig(fairProb);
    const fairDecimal = 1 / fairProb;
    const payout = fairDecimal - 1; // the profit portion
    const viggedPayout = payout * (1 - vig); // reduce payout by vig %
    return 1 / (1 + viggedPayout); // convert back to implied prob
  }

  let offeredImpliedProb = 1;
  for (const leg of pricedLegs) {
    offeredImpliedProb *= applyOddsVig(leg.fairProb);
  }

  // Correlation adjustment for same-game multi-leg parlays. Legs on the same
  // game are positively correlated (e.g. favorite winning & game going over),
  // so the true joint probability is higher than the product of per-leg fair
  // probs. Applied as a multiplicative boost on the parlay's fair prob, then
  // re-vigged as a single combined bet. We take max(per-leg vig result,
  // correlation-adjusted-with-vig) so we never price BELOW the uncorrelated
  // baseline — correlation only pushes prices tighter, never looser.
  const correlationBoost = computeCorrelationBoost(pricedLegs);
  if (correlationBoost > 1.0) {
    const adjustedFair = Math.min(fairParlayProb * correlationBoost, 0.99);
    const adjustedFairDecimal = 1 / adjustedFair;
    // Apply a conservative 3% vig on the correlation-adjusted fair as a
    // single combined bet (matching the per-leg vig method).
    const corrVig = Math.max(baseVig, 0.03);
    const adjustedPayout = (adjustedFairDecimal - 1) * (1 - corrVig);
    const correlatedOfferedProb = 1 / (1 + adjustedPayout);
    if (correlatedOfferedProb > offeredImpliedProb) {
      log.info('Pricing', `Correlation penalty: boost ${correlationBoost.toFixed(2)}x raised offered implied ${(offeredImpliedProb*100).toFixed(2)}% → ${(correlatedOfferedProb*100).toFixed(2)}% for ${pricedLegs.length}-leg parlay`);
      offeredImpliedProb = correlatedOfferedProb;
    }
  }

  // Cap at 0.99 (can't offer 100%+ implied)
  const cappedProb = Math.min(offeredImpliedProb, 0.99);

  // Convert to decimal odds
  const decimalOdds = 1 / cappedProb;

  // Determine max risk
  const maxRisk = config.pricing.maxRiskPerParlay;

  // PX expects positive bettor-side American odds in offers (e.g., +215).
  // PX converts to negative SP-side for storage (confirmed_odds = -215).
  // PX cannot handle negative bettor-side odds — it flips the sign and overpays.
  if (decimalOdds < 2.0) {
    log.debug('Pricing', `Declined: negative bettor odds (decimal ${decimalOdds.toFixed(3)}, prob ${(cappedProb*100).toFixed(1)}%) — PX cannot process`);
    priceParlay._lastFailure = {
      reason: 'negative odds',
      detail: `parlay prob ${(cappedProb*100).toFixed(1)}% > 50% → negative bettor odds not supported by PX`,
      blockerLeg: null,
    };
    return null;
  }

  const americanOdds = decimalToAmerican(decimalOdds);

  // Decline heavy favorite moneyline legs — PX sign-flip bug causes overpayment.
  // NBA: no moneyline favorites beyond -180 (fairProb > 0.6429)
  // Tennis: no moneyline favorites beyond -300 (fairProb > 0.75)
  for (const leg of pricedLegs) {
    if (leg.lineInfo.marketType !== 'moneyline') continue;
    const impliedOdds = leg.fairProb >= 0.5 ? Math.round(-100 * leg.fairProb / (1 - leg.fairProb)) : Math.round(100 * (1 - leg.fairProb) / leg.fairProb);
    if (leg.lineInfo.sport === 'basketball_nba' && leg.fairProb > 0.6429) {
      log.debug('Pricing', `Declined: NBA moneyline ${leg.lineInfo.teamName} is heavy favorite (${impliedOdds})`);
      priceParlay._lastFailure = {
        reason: 'NBA heavy favorite',
        detail: `${leg.lineInfo.teamName} at ${impliedOdds} exceeds -180 limit`,
        blockerLeg: { team: leg.lineInfo.teamName, sport: 'basketball_nba', market: 'moneyline' },
      };
      return null;
    }
    if (leg.lineInfo.sport === 'tennis' && leg.fairProb > 0.75) {
      log.debug('Pricing', `Declined: Tennis moneyline ${leg.lineInfo.teamName} is heavy favorite (${impliedOdds})`);
      priceParlay._lastFailure = {
        reason: 'tennis heavy favorite',
        detail: `${leg.lineInfo.teamName} at ${impliedOdds} exceeds -300 limit`,
        blockerLeg: { team: leg.lineInfo.teamName, sport: 'tennis', market: 'moneyline' },
      };
      return null;
    }
  }

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

  // Build estimated_price (per-leg breakdown) — bettor-side American odds per leg.
  // Apply same odds-based vig so PX's recomputed parlay matches our intended price.
  const estimatedPrice = pricedLegs.map(leg => ({
    line_id: leg.lineId,
    odds: decimalToAmerican(1 / applyOddsVig(leg.fairProb)),
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
        // Capitalize first letter (PX sends "over"/"under" lowercase for totals)
        if (team) team = team.charAt(0).toUpperCase() + team.slice(1);
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
          legVig: Math.round(getEffectiveVig(l.fairProb) * 10000) / 10000,
          displayFairProb: l.displayFairProb ? Math.round(l.displayFairProb * 10000) / 10000 : null,
          pinnacleOdds: l.pinnacleOdds || null,
          fanduelOdds: l.fanduelOdds || null,
          kalshiOdds: l.kalshiOdds || null,
          draftkingsOdds: l.draftkingsOdds || null,
          sport: l.lineInfo.sport,
          homeTeam: l.lineInfo.homeTeam,
          awayTeam: l.lineInfo.awayTeam,
          startTime: l.lineInfo.startTime || null,
          pxEventId: l.lineInfo.pxEventId || null,
          onDemand: l.lineInfo.onDemand || false,
        };
      }),
      vig: Math.round(pricedLegs.reduce((s, l) => s + getEffectiveVig(l.fairProb), 0) / pricedLegs.length * 10000) / 10000,
      fairParlayProb: Math.round(fairParlayProb * 100000) / 100000,
      correlationBoost: correlationBoost > 1 ? Math.round(correlationBoost * 1000) / 1000 : undefined,
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
      kalshiParlay: (() => {
        const klLegs = pricedLegs.filter(l => l.kalshiOdds != null);
        if (klLegs.length !== pricedLegs.length) return null;
        let klProb = 1;
        for (const l of klLegs) {
          klProb *= oddsFeed.americanToImpliedProb(l.kalshiOdds);
        }
        if (klProb <= 0 || klProb >= 1) return null;
        return decimalToAmerican(1 / klProb);
      })(),
      draftkingsParlay: (() => {
        const dkLegs = pricedLegs.filter(l => l.draftkingsOdds != null);
        if (dkLegs.length !== pricedLegs.length) return null;
        let dkProb = 1;
        for (const l of dkLegs) {
          dkProb *= oddsFeed.americanToImpliedProb(l.draftkingsOdds);
        }
        if (dkProb <= 0 || dkProb >= 1) return null;
        return decimalToAmerican(1 / dkProb);
      })(),
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
 * Compute a multiplicative boost to the parlay's fair probability to account
 * for correlation between same-game legs. Returns 1.0 if no correlated pairs.
 *
 * Factors are conservative estimates of the positive correlation between:
 *   - moneyline/spread + total on the same game (both depend on game flow)
 *   - double_chance + total similarly
 *
 * For same-team spread + total, the correlation is stronger when the bettor
 * takes the favorite and the over (winning teams tend to score more).
 *
 * Each correlated pair multiplies the boost independently; a 3-leg same-game
 * SGP therefore compounds multiple pair penalties.
 */
function computeCorrelationBoost(pricedLegs) {
  // Group by event id
  const byEvent = {};
  for (const pl of pricedLegs) {
    const eid = pl.lineInfo.pxEventId;
    if (!eid) continue;
    if (!byEvent[eid]) byEvent[eid] = [];
    byEvent[eid].push(pl);
  }
  let boost = 1.0;
  for (const legs of Object.values(byEvent)) {
    if (legs.length < 2) continue;
    // Find moneyline, spread, and total legs in this event
    const ml = legs.find(l => l.lineInfo.marketType === 'moneyline');
    const spread = legs.find(l => l.lineInfo.marketType === 'spread');
    const total = legs.find(l => l.lineInfo.marketType === 'total');
    const dc = legs.find(l => l.lineInfo.marketType === 'double_chance');
    const btts = legs.find(l => l.lineInfo.marketType === 'btts' || l.lineInfo.marketType === 'both_teams_to_score');
    // Spread + total: strongest correlation among allowed combinations
    if (spread && total) boost *= 1.22;
    // Moneyline + total: moderately correlated
    if (ml && total) boost *= 1.15;
    // Double-chance + total: weaker but positive
    if (dc && total) boost *= 1.12;
    // BTTS + moneyline/spread: BTTS yes requires both teams to score,
    // which is correlated with a close game (dog ML / cover scenario)
    if (btts && (ml || spread)) boost *= 1.10;
  }
  return boost;
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

    // Reject if event has already started.
    // Golf matchups legitimately have no commenceTime (DataGolf doesn't expose
    // per-matchup tee times); we rely on isStale() instead for those.
    const isGolfMatchup = lineInfo.sport === 'golf_matchups' || lineInfo.oddsApiSport === 'golf_matchups';
    if (lineInfo.startTime) {
      const startMs = new Date(lineInfo.startTime).getTime();
      if (isNaN(startMs)) {
        return { declined: true, reason: 'unknown start time', detail: `${lineInfo.teamName || '?'} (${lineInfo.sport || '?'}) has invalid startTime ${lineInfo.startTime} — cannot verify game hasn't started` };
      }
      if (Date.now() > startMs) {
        return { declined: true, reason: 'event started', detail: `${lineInfo.teamName || '?'} (${lineInfo.sport || '?'}) already in progress` };
      }
    } else if (!isGolfMatchup) {
      // Null startTime is a risk — game could already be live. Decline rather than
      // silently skip the check. Golf matchups exempt (see comment above).
      return { declined: true, reason: 'unknown start time', detail: `${lineInfo.teamName || '?'} (${lineInfo.sport || '?'}) has no startTime — cannot verify game hasn't started` };
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
    const hasDoubleChance = types.includes('double_chance');
    const hasBtts = types.includes('btts') || types.includes('both_teams_to_score');
    const hasTotal = types.includes('total');
    const hasF5Moneyline = types.some(t => /first_5_innings_moneyline|first_five_innings_moneyline/.test(t));
    const hasF5RunLine = types.some(t => /first_5_innings_run_line|first_five_innings_run_line/.test(t));
    const hasF5Total = types.some(t => /first_5_innings_total|first_five_innings_total/.test(t));
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
    // Block: double_chance + moneyline (double chance is derived from moneyline, perfectly correlated)
    if (hasDoubleChance && hasMoneyline) {
      log.info('Pricing', `Declined: double_chance + moneyline on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `double_chance + moneyline on same game: ${gameLabel}` };
    }
    // Block: double_chance + spread (both tied to match result)
    if (hasDoubleChance && hasSpread) {
      log.info('Pricing', `Declined: double_chance + spread on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `double_chance + spread on same game: ${gameLabel}` };
    }
    // Block: BTTS + total (both depend on goal count, strongly correlated)
    if (hasBtts && hasTotal) {
      log.info('Pricing', `Declined: BTTS + total on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `BTTS + total on same game: ${gameLabel}` };
    }
    // Block: F5 + full-game on same market type (F5 is a subset of the full game)
    if (hasF5Moneyline && hasMoneyline) {
      log.info('Pricing', `Declined: F5 moneyline + full-game moneyline on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `F5 + full-game moneyline on same game: ${gameLabel}` };
    }
    if (hasF5RunLine && hasSpread) {
      log.info('Pricing', `Declined: F5 run line + full-game spread on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `F5 + full-game spread on same game: ${gameLabel}` };
    }
    if (hasF5Total && hasTotal) {
      log.info('Pricing', `Declined: F5 total + full-game total on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `F5 + full-game total on same game: ${gameLabel}` };
    }
    // Block: team_total combined with any other leg FOR THE SAME TEAM on the
    // same game. team_total for the home team is highly correlated with home
    // moneyline, home spread cover, and total goes over. A team_total leg
    // standalone (no other legs on this game) OR combined with legs for the
    // OPPOSING team is allowed.
    const teamTotalEntries = entries.filter(e => e.market === 'team_total');
    if (teamTotalEntries.length > 0 && entries.length > 1) {
      // Figure out which team each leg is for: team_total's .team is the team
      // side; other legs' .team is also the team (for ML/spread) or uses home
      // for totals (which are event-level). Block if ANY other leg is for the
      // SAME team as a team_total leg.
      for (const tt of teamTotalEntries) {
        const ttTeam = tt.team;
        const sameTeamOther = entries.find(e => e !== tt && e.team === ttTeam);
        if (sameTeamOther) {
          log.info('Pricing', `Declined: team_total + ${sameTeamOther.market} on ${ttTeam} (same game ${gameLabel})`);
          return { declined: true, reason: 'correlated legs', detail: `team_total + ${sameTeamOther.market} for ${ttTeam}: ${gameLabel}` };
        }
        // Also block team_total + full-game total (both depend on same goals)
        if (entries.some(e => e !== tt && e.market === 'total')) {
          log.info('Pricing', `Declined: team_total + full-game total on ${gameLabel}`);
          return { declined: true, reason: 'correlated legs', detail: `team_total + full-game total on same game: ${gameLabel}` };
        }
      }
    }
    // Note: same-game spread+total and moneyline+total are NOT blocked here
    // because they're still reasonable to quote — but they receive a correlation
    // penalty at pricing time (see computeCorrelationBoost in priceParlay).
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
