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

    // Check if prices are stale for this sport. Uses per-sport threshold:
    // MMA/boxing/NFL have tighter windows because they move on news;
    // NCAAB/tennis have looser windows because they only refresh on full cycles.
    if (oddsFeed.isStale(lineInfo.sport)) {
      const ageMin = Math.round(oddsFeed.getCacheAge(lineInfo.sport) * 10) / 10;
      const threshold = oddsFeed.getStaleThreshold(lineInfo.sport);
      log.debug('Pricing', `Declined: stale prices for ${lineInfo.sport} (${ageMin}min old, threshold ${threshold}m)`);
      priceParlay._lastFailure = {
        reason: 'stale odds',
        detail: `${lineInfo.sport} odds ${ageMin}m old (threshold ${threshold}m)`,
        blockerLeg: legDescriptor,
      };
      return null;
    }

    // Pre-game closing-line guard: within 30 min of tip-off, sportsbooks
    // move the line hard on late news. Require much fresher cache (≤ 2 min)
    // for events in the final pre-game window. Catches scenarios where the
    // sport-level cache passes isStale but the line has moved since refresh
    // (this is how the Rockies +190/+194 stale FD/DK quote slipped through).
    if (oddsFeed.isEventStalePreGame(lineInfo.sport, lineInfo.startTime)) {
      const ageMin = Math.round(oddsFeed.getCacheAge(lineInfo.sport) * 10) / 10;
      log.info('Pricing', `Declined pre-game: ${legLabel} starts soon, cache ${ageMin}m old (limit 2m)`);
      priceParlay._lastFailure = {
        reason: 'stale odds (pre-game)',
        detail: `${legLabel} starts within 30m, ${lineInfo.sport} cache ${ageMin}m old (pre-game limit 2m)`,
        blockerLeg: legDescriptor,
      };
      return null;
    }

    // Lineup-change guard: if the MLB starting pitcher or NHL starting goalie
    // has swapped within the last few minutes, the sportsbooks are still
    // re-pricing and our cached odds are likely stale even if the cache
    // timestamp is fresh. Decline briefly to avoid getting picked off on
    // lineup news. Only MLB/NHL — other sports return null from this check.
    const lineupStatus = oddsFeed.checkLineupFreshness(
      lineInfo.sport, lineInfo.homeTeam, lineInfo.awayTeam, lineInfo.startTime
    );
    if (lineupStatus) {
      const ageSec = Math.round(lineupStatus.ageMs / 1000);
      log.info('Pricing', `Declined: lineup change ${ageSec}s ago for ${legLabel} — ${lineupStatus.detail}`);
      priceParlay._lastFailure = {
        reason: 'lineup change',
        detail: `${legLabel}: ${lineupStatus.detail} (${ageSec}s ago, grace window active)`,
        blockerLeg: legDescriptor,
      };
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

    // Look up sportsbook raw odds for this leg. Pass lineInfo.line so the
    // accessor can reject cached values that belong to a different line
    // (e.g. Arsenal -1.25 when the PX RFQ wanted Arsenal -1). Returning null
    // on mismatch is safer than reporting the primary-line book odds and
    // corrupting the dashboard's competitor comparison.
    const pinnacleOdds = oddsFeed.getPinnacleOdds(
      lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
      lineInfo.oddsApiMarket, lineInfo.oddsApiSelection, lineInfo.startTime, lineInfo.line
    );
    const fanduelOdds = oddsFeed.getFanDuelOdds(
      lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
      lineInfo.oddsApiMarket, lineInfo.oddsApiSelection, lineInfo.startTime, lineInfo.line
    );
    const kalshiOdds = oddsFeed.getKalshiOdds(
      lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
      lineInfo.oddsApiMarket, lineInfo.oddsApiSelection, lineInfo.startTime, lineInfo.line
    );
    const draftkingsOdds = oddsFeed.getDraftKingsOdds(
      lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
      lineInfo.oddsApiMarket, lineInfo.oddsApiSelection, lineInfo.startTime, lineInfo.line
    );

    // For DNB (Draw No Bet) legs, also fetch the opposite-side book odds so
    // we can compute DNB-adjusted per-book implied probs. The raw book home
    // implied from a 3-way soccer market is NOT the DNB implied — it must be
    // renormalized as pinHome / (pinHome + pinAway). Without this, the
    // dashboard's Pinnacle/DK parlay columns compound raw 3-way home probs,
    // producing a misleading apples-to-oranges comparison with our DNB offer.
    let pinnacleDNBProb = null;
    let fanduelDNBProb = null;
    let kalshiDNBProb = null;
    let draftkingsDNBProb = null;
    if (lineInfo.isDNB && lineInfo.oddsApiMarket === 'h2h') {
      const oppSel = lineInfo.oddsApiSelection === 'home' ? 'away' : 'home';
      function calcDNB(sameOdds, oppOdds) {
        if (sameOdds == null || oppOdds == null) return null;
        const pSame = oddsFeed.americanToImpliedProb(sameOdds);
        const pOpp = oddsFeed.americanToImpliedProb(oppOdds);
        const sum = pSame + pOpp;
        return sum > 0 ? pSame / sum : null;
      }
      const pinOpp = oddsFeed.getPinnacleOdds(
        lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
        'h2h', oppSel, lineInfo.startTime
      );
      const fdOpp = oddsFeed.getFanDuelOdds(
        lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
        'h2h', oppSel, lineInfo.startTime
      );
      const klOpp = oddsFeed.getKalshiOdds(
        lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
        'h2h', oppSel, lineInfo.startTime
      );
      const dkOpp = oddsFeed.getDraftKingsOdds(
        lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam,
        'h2h', oppSel, lineInfo.startTime
      );
      pinnacleDNBProb = calcDNB(pinnacleOdds, pinOpp);
      fanduelDNBProb = calcDNB(fanduelOdds, fdOpp);
      kalshiDNBProb = calcDNB(kalshiOdds, klOpp);
      draftkingsDNBProb = calcDNB(draftkingsOdds, dkOpp);
    }

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
      pinnacleDNBProb,
      fanduelDNBProb,
      kalshiDNBProb,
      draftkingsDNBProb,
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
  const globalBaseVig = config.pricing.defaultVig;
  const vigBySport = config.pricing.vigBySport || {};

  // Per-sport base vig: uses sport-specific override if set, else global default.
  function getBaseVigForSport(sport) {
    if (sport && vigBySport[sport] != null) return vigBySport[sport];
    return globalBaseVig;
  }

  // Tiered vig: scaled proportionally above base.
  // Gentle bumps on heavy favorites to prevent compounding leaks
  // without killing competitiveness on normal parlays.
  function getEffectiveVig(fairProb, sport) {
    const baseVig = getBaseVigForSport(sport);
    if (fairProb >= 0.80) return Math.max(baseVig, 0.03);  // extreme favorites (-400+)
    if (fairProb >= 0.70) return Math.max(baseVig, 0.025); // heavy favorites (-230+)
    return baseVig;                                         // everything else keeps base vig
  }

  function applyOddsVig(fairProb, sport) {
    const vig = getEffectiveVig(fairProb, sport);
    const fairDecimal = 1 / fairProb;
    const payout = fairDecimal - 1; // the profit portion
    const viggedPayout = payout * (1 - vig); // reduce payout by vig %
    return 1 / (1 + viggedPayout); // convert back to implied prob
  }

  let offeredImpliedProb = 1;
  for (const leg of pricedLegs) {
    offeredImpliedProb *= applyOddsVig(leg.fairProb, leg.lineInfo.sport);
  }

  // -------------------------------------------------------------------
  // PRICING SAFETY NET: cross-check fair against Pinnacle raw compound.
  //
  // Sign-flip bugs on alt spreads have historically been the most dangerous
  // pricing error (see altSpreads signed-home_point fix). If our fair prob
  // diverges too far optimistically from Pinnacle's raw compound on a
  // non-SGP parlay, decline rather than price. Pinnacle raw is looser than
  // our consensus on average (it includes vig) so OUR fair should almost
  // always be SLIGHTLY LOWER (tighter) than Pin raw compound. A 25%+ gap
  // in the bettor-favorable direction is a strong signal that our
  // consensus was corrupted (wrong line side, wrong book, wrong direction).
  //
  // Only runs when Pinnacle has all legs and for non-SGP cross-game
  // parlays (SGPs have valid correlation-driven deltas and are handled
  // via the Pin-match path below).
  // -------------------------------------------------------------------
  {
    const pinLegs = pricedLegs.filter(l => l.pinnacleOdds != null);
    const havePinAll = pinLegs.length === pricedLegs.length && pinLegs.length > 0;
    // Detect cross-game (no two legs share a pxEventId)
    const eventIds = pricedLegs.map(l => l.lineInfo.pxEventId).filter(Boolean);
    const uniqueEventIds = new Set(eventIds);
    const isCrossGame = uniqueEventIds.size === eventIds.length && eventIds.length > 0;
    if (havePinAll && isCrossGame) {
      let pinRawCross = 1;
      for (const l of pricedLegs) {
        const legImpl = l.lineInfo.isDNB && l.pinnacleDNBProb != null
          ? l.pinnacleDNBProb
          : oddsFeed.americanToImpliedProb(l.pinnacleOdds);
        pinRawCross *= legImpl;
      }
      if (pinRawCross > 0 && pinRawCross < 1) {
        // Our fair should be at or below Pin raw (we use de-vigged consensus
        // which is typically tighter). Guard against wildly optimistic fair
        // that would produce bettor-favorable prices way above Pin.
        const pinToOurs = fairParlayProb / pinRawCross;
        const SAFETY_THRESHOLD = 0.75; // block if our fair is < 75% of Pin raw
        if (pinToOurs < SAFETY_THRESHOLD) {
          log.warn('Pricing', `SAFETY: fair ${(fairParlayProb*100).toFixed(2)}% vs Pin raw ${(pinRawCross*100).toFixed(2)}% ratio=${pinToOurs.toFixed(3)} — declining to avoid sign-flip / wrong-line mispricing`);
          priceParlay._lastFailure = {
            reason: 'pricing safety guard',
            detail: `our fair ${(fairParlayProb*100).toFixed(2)}% is ${((1-pinToOurs)*100).toFixed(0)}% below Pinnacle raw ${(pinRawCross*100).toFixed(2)}% — possible sign-flip / wrong-line corruption`,
            blockerLeg: null,
          };
          return null;
        }
      }
    }
  }

  // ---------------------------------------------------------------------
  // SGP correlation handling — PIN-MATCH approach
  //
  // Prior approach used hard-coded multiplicative boosts (1.15x ML+total,
  // 1.22x spread+total) which consistently priced SGPs far below Pinnacle,
  // crushing fill rate. New approach: target Pinnacle's raw compound parlay
  // price (which already embeds Pin's own correlation model) and offer the
  // bettor a small 0.5% edge below that. Pinnacle's SGP pricing is the
  // sharpest public signal — matching it ensures we're competitive without
  // overpaying on correlation.
  //
  // Formula: pinMatchTarget = pinRawCompound * (1 - 0.005)
  //   offered = max(perLegVigResult, pinMatchTarget)
  //
  // max() ensures we never loosen below per-leg vig baseline. If Pin data
  // missing for any leg, fall back to a modest 1.05x boost re-vigged.
  // ---------------------------------------------------------------------
  let pricingMethod = 'perLegVig';
  let pinRawCompound = null;
  let pinMatchTarget = null;

  // Detect SGP: any event id shared by 2+ legs
  const eventCounts = {};
  for (const pl of pricedLegs) {
    const eid = pl.lineInfo.pxEventId;
    if (!eid) continue;
    eventCounts[eid] = (eventCounts[eid] || 0) + 1;
  }
  const isSGP = Object.values(eventCounts).some(c => c >= 2);

  if (isSGP) {
    // Compute Pinnacle raw compound implied prob using DNB-adjusted values
    // where applicable (soccer 2-way moneylines).
    const pinLegs = pricedLegs.filter(l => l.pinnacleOdds != null);
    const havePinAll = pinLegs.length === pricedLegs.length;

    if (havePinAll) {
      let pinProb = 1;
      for (const l of pricedLegs) {
        const legImpl = l.lineInfo.isDNB && l.pinnacleDNBProb != null
          ? l.pinnacleDNBProb
          : oddsFeed.americanToImpliedProb(l.pinnacleOdds);
        pinProb *= legImpl;
      }
      if (pinProb > 0 && pinProb < 1) {
        pinRawCompound = pinProb;
        // Target 0.5% bettor edge below Pin raw compound
        pinMatchTarget = pinRawCompound * (1 - 0.005);
        if (pinMatchTarget > offeredImpliedProb) {
          log.info('Pricing', `SGP pin-match: ${pricedLegs.length}-leg parlay offered ${(offeredImpliedProb*100).toFixed(2)}% → ${(pinMatchTarget*100).toFixed(2)}% (Pin raw ${(pinRawCompound*100).toFixed(2)}%)`);
          offeredImpliedProb = pinMatchTarget;
          pricingMethod = 'sgp_pin_match';
        } else {
          // Per-leg vig already gives bettor better value than Pin — keep it
          pricingMethod = 'sgp_pin_match_keep_baseline';
        }
      }
    }

    // Fallback: Pin data missing — use modest 1.05x boost on fair prob re-vigged
    if (pinRawCompound == null) {
      const fallbackBoost = 1.05;
      const adjustedFair = Math.min(fairParlayProb * fallbackBoost, 0.99);
      const adjustedFairDecimal = 1 / adjustedFair;
      const corrVig = Math.max(config.pricing.defaultVig, 0.03);
      const adjustedPayout = (adjustedFairDecimal - 1) * (1 - corrVig);
      const fallbackOfferedProb = 1 / (1 + adjustedPayout);
      if (fallbackOfferedProb > offeredImpliedProb) {
        log.info('Pricing', `SGP fallback boost ${fallbackBoost.toFixed(2)}x (Pin missing): ${(offeredImpliedProb*100).toFixed(2)}% → ${(fallbackOfferedProb*100).toFixed(2)}% for ${pricedLegs.length}-leg parlay`);
        offeredImpliedProb = fallbackOfferedProb;
        pricingMethod = 'sgp_fallback_boost';
      }
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
    odds: decimalToAmerican(1 / applyOddsVig(leg.fairProb, leg.lineInfo.sport)),
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
          marketName: l.lineInfo.marketName || null,
          isDNB: l.lineInfo.isDNB || false,
          selection: l.lineInfo.oddsApiSelection,
          line: l.lineInfo.line,
          fairProb: Math.round(l.fairProb * 10000) / 10000,
          legVig: Math.round(getEffectiveVig(l.fairProb, l.lineInfo.sport) * 10000) / 10000,
          displayFairProb: l.displayFairProb ? Math.round(l.displayFairProb * 10000) / 10000 : null,
          pinnacleOdds: l.pinnacleOdds || null,
          fanduelOdds: l.fanduelOdds || null,
          kalshiOdds: l.kalshiOdds || null,
          draftkingsOdds: l.draftkingsOdds || null,
          // DNB-adjusted per-book implied probs for soccer 2-way parlay compound.
          // Null for non-DNB legs (use standard americanToProb(odds) instead).
          pinnacleDNBProb: l.pinnacleDNBProb != null ? Math.round(l.pinnacleDNBProb * 10000) / 10000 : null,
          fanduelDNBProb: l.fanduelDNBProb != null ? Math.round(l.fanduelDNBProb * 10000) / 10000 : null,
          kalshiDNBProb: l.kalshiDNBProb != null ? Math.round(l.kalshiDNBProb * 10000) / 10000 : null,
          draftkingsDNBProb: l.draftkingsDNBProb != null ? Math.round(l.draftkingsDNBProb * 10000) / 10000 : null,
          sport: l.lineInfo.sport,
          homeTeam: l.lineInfo.homeTeam,
          awayTeam: l.lineInfo.awayTeam,
          startTime: l.lineInfo.startTime || null,
          pxEventId: l.lineInfo.pxEventId || null,
          pxEventName: l.lineInfo.pxEventName || null,
          onDemand: l.lineInfo.onDemand || false,
          // Golf-specific: tournament name + round number for display
          tournamentName: l.lineInfo.tournamentName || null,
          roundNum: l.lineInfo.roundNum || null,
        };
      }),
      vig: Math.round(pricedLegs.reduce((s, l) => s + getEffectiveVig(l.fairProb, l.lineInfo.sport), 0) / pricedLegs.length * 10000) / 10000,
      fairParlayProb: Math.round(fairParlayProb * 100000) / 100000,
      pricingMethod,
      isSGP,
      pinRawCompound: pinRawCompound != null ? Math.round(pinRawCompound * 100000) / 100000 : null,
      pinMatchTarget: pinMatchTarget != null ? Math.round(pinMatchTarget * 100000) / 100000 : null,
      offeredImpliedProb: Math.round(cappedProb * 100000) / 100000,
      decimalOdds: Math.round(decimalOdds * 100) / 100,
      americanOdds,
      // Compute competitor book parlay odds from per-leg book implied probs.
      // For DNB legs (soccer 2-way moneyline), use the DNB-adjusted per-book
      // implied (pinHome / (pinHome + pinAway)) instead of the raw 3-way home
      // implied. This makes the comparison apples-to-apples: our DNB offer
      // vs. the book's DNB-equivalent parlay. Previously we compounded raw
      // 3-way implied, producing a misleadingly loose "Pinnacle parlay" number
      // that looked ~200 American points better than our DNB offer.
      pinnacleParlay: (() => {
        const pinLegs = pricedLegs.filter(l => l.pinnacleOdds != null);
        if (pinLegs.length !== pricedLegs.length) return null;
        let pinProb = 1;
        for (const l of pinLegs) {
          const legImpl = l.lineInfo.isDNB && l.pinnacleDNBProb != null
            ? l.pinnacleDNBProb
            : oddsFeed.americanToImpliedProb(l.pinnacleOdds);
          pinProb *= legImpl;
        }
        if (pinProb <= 0 || pinProb >= 1) return null;
        return decimalToAmerican(1 / pinProb);
      })(),
      kalshiParlay: (() => {
        const klLegs = pricedLegs.filter(l => l.kalshiOdds != null);
        if (klLegs.length !== pricedLegs.length) return null;
        let klProb = 1;
        for (const l of klLegs) {
          const legImpl = l.lineInfo.isDNB && l.kalshiDNBProb != null
            ? l.kalshiDNBProb
            : oddsFeed.americanToImpliedProb(l.kalshiOdds);
          klProb *= legImpl;
        }
        if (klProb <= 0 || klProb >= 1) return null;
        return decimalToAmerican(1 / klProb);
      })(),
      draftkingsParlay: (() => {
        const dkLegs = pricedLegs.filter(l => l.draftkingsOdds != null);
        if (dkLegs.length !== pricedLegs.length) return null;
        let dkProb = 1;
        for (const l of dkLegs) {
          const legImpl = l.lineInfo.isDNB && l.draftkingsDNBProb != null
            ? l.draftkingsDNBProb
            : oddsFeed.americanToImpliedProb(l.draftkingsOdds);
          dkProb *= legImpl;
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

  // Dedup: decline if we just quoted this exact leg-set within the window.
  // Bettors can submit the same parlay repeatedly faster than our exposure
  // state updates (race between quote and confirm). Say no on the repeats.
  const dup = orderTracker.checkRecentDuplicate(legs);
  if (dup) {
    return {
      declined: true,
      reason: 'duplicate parlay',
      detail: `identical leg-set quoted ${Math.round(dup.ageMs / 1000)}s ago (60s dedup window)`,
    };
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
    // ---- TEAM TOTAL CORRELATION BLOCKS (same game) ----
    // team_total is a quantity bet on ONE team's scoring in a game. It's
    // strongly correlated with many other markets on the same game:
    //
    //   team_total + full-game total  → both depend on the same pool of goals
    //   team_total + game spread      → team scoring rate drives cover margin
    //   team_total + game moneyline   → team scoring drives win prob
    //   team_total (home) + team_total (away) → both tied to game pace
    //
    // Rules (in order of strictness):
    //   1. team_total + ANY game-level total → BLOCK unconditionally
    //      (the user-requested rule: "team totals and game totals cannot
    //      be combined"). Applies even if the team_total is the OPPOSING
    //      team — the joint distribution is still highly correlated.
    //   2. team_total + any other leg for the SAME TEAM → BLOCK
    //      (e.g. home team_total + home moneyline).
    //   3. Two team_total legs (home + away same game) → already caught
    //      upstream by the duplicate-market-type rule.
    const teamTotalEntries = entries.filter(e => e.market === 'team_total');
    if (teamTotalEntries.length > 0 && entries.length > 1) {
      // Rule 1 (unconditional): team_total + full-game total on same game
      const hasGameTotal = entries.some(e => e.market === 'total');
      if (hasGameTotal) {
        log.info('Pricing', `Declined: team_total + game total on ${gameLabel}`);
        return { declined: true, reason: 'correlated legs', detail: `team_total + game total on same game: ${gameLabel}` };
      }
      // Rule 2: team_total + any other leg for the SAME team
      for (const tt of teamTotalEntries) {
        const ttTeam = tt.team;
        const sameTeamOther = entries.find(e => e !== tt && e.team === ttTeam);
        if (sameTeamOther) {
          log.info('Pricing', `Declined: team_total + ${sameTeamOther.market} on ${ttTeam} (same game ${gameLabel})`);
          return { declined: true, reason: 'correlated legs', detail: `team_total + ${sameTeamOther.market} for ${ttTeam}: ${gameLabel}` };
        }
      }
    }
    // Note: same-game spread+total and moneyline+total are NOT blocked here
    // because they're still reasonable to quote — SGP correlation is handled
    // at pricing time via Pin-match logic (pinMatchTarget in priceParlay).
  }

  // Check team-level exposure limits.
  // Use maxRiskPerParlay as the estimate. The confirm-time re-check in
  // handleConfirm is the real safety net (checks actual stake against both
  // per-team and per-game caps with pending reservations). Keeping
  // shouldDecline lightweight here avoids over-restricting quotes — the
  // original 3x estPayout inflation was declining too many RFQs.
  const estPayout = config.pricing.maxRiskPerParlay || 500;
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
    return { declined: true, reason: 'team exposure limit', detail: exposureCheck.reason, violations: exposureCheck.violations, estPayout };
  }

  // Check game-level exposure limit
  const maxPerGame = getBankroll() * config.pricing.maxExposurePerGamePct / 100;
  const gameCheck = orderTracker.checkGameExposure(resolvedLegs, estPayout, maxPerGame);
  if (!gameCheck.allowed) {
    log.info('Pricing', `Game exposure limit: ${gameCheck.reason}`);
    return { declined: true, reason: 'game exposure limit', detail: gameCheck.reason, violations: [{ team: 'game-level', wouldBe: gameCheck.wouldBe || 0, limit: gameCheck.limit || 0 }], estPayout };
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
      violations: [{ team: 'portfolio', wouldBe: portfolioCheck.current + estPayout, limit: portfolioCheck.limit }],
      estPayout,
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

  // Check if fair value has moved significantly against us since quote.
  // Threshold configurable via CONFIRMATION_DRIFT_THRESHOLD env var (default 3%).
  const originalProb = originalMeta.fairParlayProb;
  const currentProb = currentPricing.meta.fairParlayProb;
  const drift = Math.abs(currentProb - originalProb) / originalProb;
  const driftThreshold = config.pricing.confirmationDriftThreshold;

  if (drift > driftThreshold) {
    log.warn('Pricing', `Price drift of ${(drift * 100).toFixed(1)}% since quote (threshold ${(driftThreshold * 100).toFixed(1)}%) — rejecting confirmation`);
    return { valid: false, reason: `price drift ${(drift * 100).toFixed(1)}% > ${(driftThreshold * 100).toFixed(1)}%`, currentPricing };
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
