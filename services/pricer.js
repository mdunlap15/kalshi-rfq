const { config, getBankroll } = require('../config');
const log = require('./logger');
const lineManager = require('./line-manager');
const oddsFeed = require('./odds-feed');
const orderTracker = require('./order-tracker');
const dkScraper = require('./dk-scraper');

/**
 * Detect a series-winner leg and look up its fair probability from the
 * DK scraper cache. PX sends these as moneyline legs with team names
 * like "Cleveland Cavaliers (Series)" / "Edmonton Oilers (Series)",
 * which don't match any odds-feed h2h market. Returns null if the leg
 * isn't a series bet or the DK cache doesn't contain the team.
 */
// Decline whenever DK's series event startTime is in the past. That
// field is the NEXT game's tipoff — as long as it's still showing a
// past time, the most recent game has already started and DK hasn't
// relisted the series market yet. A fixed 6h cutoff would incorrectly
// re-enable us for marathon games (multiple OT, rain delays, etc.)
// where DK is NOT going to have relisted by then. The safe signal is
// simply "startTime is still in the past = DK has not moved on to the
// next game yet = we should not quote".

function getSeriesFairProb(lineInfo) {
  // Detect which series market this leg belongs to. marketType carries
  // the line-manager tag for winner/spread/total; oddsApiMarket is a
  // mirror. We also accept raw PX moneyline legs whose team name still
  // carries a "(Series)" suffix (legacy path for any that slipped
  // through the line-manager retag).
  const teamName = lineInfo?.teamName || '';
  const mt = lineInfo?.marketType;
  const am = lineInfo?.oddsApiMarket;
  const hasSeriesTeamSuffix = /\(series\)/i.test(teamName);
  const isSeriesWinner = mt === 'series_winner' || am === 'series_winner' || (hasSeriesTeamSuffix && (mt === 'moneyline' || !mt));
  const isSeriesSpread = mt === 'series_spread' || am === 'series_spread';
  const isSeriesTotal  = mt === 'series_total'  || am === 'series_total';
  if (!isSeriesWinner && !isSeriesSpread && !isSeriesTotal) return null;

  const sport = (lineInfo.oddsApiSport || lineInfo.sport || '').toLowerCase();
  const sportKey = sport.includes('nba') || sport.includes('basketball') ? 'nba'
                 : sport.includes('nhl') || sport.includes('icehockey') || sport.includes('hockey') ? 'nhl'
                 : null;
  if (!sportKey) return null;
  const bareTeam = teamName.replace(/\s*\(series\)\s*/ig, '').trim();

  let hit = null;
  if (isSeriesWinner) {
    hit = dkScraper.lookupSeriesFairProb(sportKey, bareTeam || teamName);
  } else if (isSeriesSpread) {
    // PX stores spread line as signed (negative for favorite side).
    // DK cache keys each team's leg by (team, |line|, '+'|'-').
    const rawLine = lineInfo.line;
    if (rawLine == null || !Number.isFinite(Number(rawLine))) return null;
    const absLine = Math.abs(Number(rawLine));
    const side = Number(rawLine) < 0 ? '-' : '+';
    hit = dkScraper.lookupSeriesSpreadFairProb(sportKey, bareTeam || teamName, absLine, side);
  } else if (isSeriesTotal) {
    const rawLine = lineInfo.line;
    if (rawLine == null || !Number.isFinite(Number(rawLine))) return null;
    const side = lineInfo.oddsApiSelection || lineInfo.selection;
    hit = dkScraper.lookupSeriesTotalFairProb(sportKey, lineInfo.homeTeam, lineInfo.awayTeam, Number(rawLine), side);
  }
  if (!hit) return null;

  // In-play / cooldown guard: decline if the next game in this series
  // has already started and is still within the cooldown window. Our
  // cached DK odds are pre-game and would give a bettor material edge
  // if the in-game team took an early lead. Once DK's scraper refresh
  // moves startTime forward (post-game to next game), we resume.
  if (hit.startTime) {
    const t = new Date(hit.startTime).getTime();
    if (Number.isFinite(t) && t <= Date.now()) {
      log.debug('Pricing', `Series in-play decline: ${teamName} ${mt || ''} (startTime ${hit.startTime}, ${Math.round((Date.now()-t)/60000)}min ago — DK has not relisted for next game)`);
      return null;
    }
  }

  // NBA series heavy-favorite: beyond FV of -500, quote DK's offered
  // price directly (no vig) instead of our de-vigged-plus-vig number.
  // Avoids drifting out of market on extreme favorites where our ramp
  // would produce an uncompetitive line. Applies to series_winner and
  // series_spread; totals pass through (no "favorite" concept).
  if (sportKey === 'nba' && (isSeriesWinner || isSeriesSpread)) {
    const threshProb = 500 / 600; // FV of -500
    if (hit.fairProb > threshProb) {
      let bookDec = hit.decimalOdds;
      if ((!bookDec || bookDec <= 1) && hit.americanOdds != null) {
        bookDec = hit.americanOdds >= 0
          ? 1 + hit.americanOdds / 100
          : 1 + 100 / Math.abs(hit.americanOdds);
      }
      if (bookDec && bookDec > 1) {
        const bookImplied = 1 / bookDec;
        log.info('Pricing', `NBA series heavy fav ${teamName} ${mt || ''} fair ${hit.fairProb.toFixed(4)} > -500 cutoff — using DK book price ${hit.americanOdds} (implied ${bookImplied.toFixed(4)})`);
        return { fairProb: hit.fairProb, bookPriceOverride: bookImplied };
      }
    }
  }
  return hit.fairProb;
}

/**
 * Golf matchup legs: route through the round-aware DataGolf accessor so
 * a round RFQ ("R1 RBC Heritage") is priced against round_matchups odds
 * and a tournament RFQ is priced against tournament_matchups odds. The
 * generic oddsFeed path keys purely on player-pair and would pick
 * whichever entry was pushed to the cache first when both exist — a
 * material mispricing risk.
 *
 * Returns null for non-golf legs; caller falls back to the normal path.
 */
function getGolfMatchupFairProb(lineInfo) {
  const sport = (lineInfo?.oddsApiSport || lineInfo?.sport || '').toLowerCase();
  if (!sport.includes('golf')) return null;
  const mt = lineInfo?.marketType || '';
  if (mt !== 'moneyline') return null; // golf matchups are h2h only
  const event = oddsFeed.getGolfMatchupEvent(
    lineInfo.homeTeam, lineInfo.awayTeam, lineInfo.roundNum ?? null
  );
  if (!event) return null;
  const h2h = event.markets?.h2h;
  if (!h2h) return null;
  const sel = lineInfo.oddsApiSelection;
  const side = sel === 'home' ? h2h.home : sel === 'away' ? h2h.away : null;
  if (!side || side.fairProb == null) return null;
  return side.fairProb;
}

/**
 * MMA moneyline legs: route directly to the DK scraper cache rather than
 * the shared odds-feed cache. Context: SharpAPI rarely covers MMA h2h
 * reliably, so the DK scraper is the source of truth. DK data IS merged
 * into oddsCache['mma_mixed_martial_arts'] at startup/refresh, but that
 * cache appears to get wiped when SharpAPI's UFC refresh runs with zero
 * matching rows — producing spurious "no h2h quote for X" declines on
 * fights DK clearly has. Bypassing to dkScraper.lookupMmaFairProb makes
 * us resilient to that wipe regardless of whether the merge ran, and
 * mirrors the getSeriesFairProb pattern.
 *
 * Returns null when the leg isn't MMA, isn't moneyline, or the fighter
 * isn't in the DK cache — caller falls back to the oddsFeed path.
 */
function getMmaFairProb(lineInfo) {
  const sport = (lineInfo?.oddsApiSport || lineInfo?.sport || '').toLowerCase();
  if (!sport.includes('mma')) return null;
  // DK cache only covers moneyline (h2h) for MMA — total rounds go via
  // the separate oddsFeed path that the merge ALSO populates.
  const mt = lineInfo?.marketType || '';
  const am = lineInfo?.oddsApiMarket || '';
  if (mt !== 'moneyline' && am !== 'h2h') return null;
  const fighter = lineInfo?.teamName || '';
  if (!fighter) return null;
  const hit = dkScraper.lookupMmaFairProb(fighter);
  if (!hit) return null;
  // In-play guard: decline if the fight has already started. DK's odds
  // are pre-fight and would give a bettor material edge mid-round.
  if (hit.startTime) {
    const t = new Date(hit.startTime).getTime();
    if (Number.isFinite(t) && t <= Date.now()) {
      log.debug('Pricing', `MMA in-play decline: ${fighter} (startTime ${hit.startTime})`);
      return null;
    }
  }
  return hit.fairProb;
}

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
async function priceParlay(legs, opts = {}) {
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
  // Optional: caller (handleRFQ via shouldDecline) passes a Map of lineId →
  // lineInfo so we skip redundant lookupLine() calls. shouldDecline already
  // validated "unknown legs" and "event started" for these, but we keep the
  // startTime check below as a belt-and-suspenders guard (cheap with
  // startTimeMs pre-computed).
  const preResolved = opts.resolvedLineInfos; // Map<lineId, lineInfo> | undefined

  // Look up and price each leg.
  //
  // --- PARALLELIZED STRUCTURE (Phase A of latency plan) ---
  // Previously every leg ran sequentially — each getFairProbAsync await blocked
  // the next. For N-leg parlays where M legs need alt-line fetches, worst-case
  // pricing time was M × (fetch round-trip). Now:
  //
  //   Phase 1: All cheap sync validation per leg. Bail early on first failure.
  //   Phase 2: Fire getFairProbAsync + verifyLineWithPinnacle for ALL surviving
  //            legs in parallel via Promise.all. Worst-case pricing time caps at
  //            a single fetch round-trip regardless of leg count.
  //   Phase 3: Sync post-processing — check async results, compute book odds,
  //            build pricedLegs array, accumulate fair parlay prob.
  //
  // All original validation, decline reasons, and blocker-leg reporting are
  // preserved — just the two network calls are now parallel instead of serial.
  const pricedLegs = [];
  let fairParlayProb = 1.0;

  // ------------------------- PHASE 1: Sync validation -------------------------
  const legStates = [];
  const nowMs = Date.now();
  // Per-request cache: isStale / getCacheAge / getStaleThreshold repeat for
  // every leg of a single-sport parlay. Memoize once here.
  const staleCache = {};
  const isStaleCached = (sport) => {
    if (staleCache[sport] === undefined) staleCache[sport] = oddsFeed.isStale(sport);
    return staleCache[sport];
  };
  for (const leg of legs) {
    const lineId = leg.line_id || leg.lineId || leg;
    const lineInfo = (preResolved && preResolved.get(lineId)) || lineManager.lookupLine(lineId);

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
    // Uses pre-computed lineInfo.startTimeMs (cached by lookupLine).
    const isGolfMatchup = lineInfo.sport === 'golf_matchups' || lineInfo.oddsApiSport === 'golf_matchups';
    const startMs = lineInfo.startTimeMs;
    if (startMs != null) {
      if (isNaN(startMs)) {
        log.debug('Pricing', `Declined: invalid startTime for ${lineInfo.teamName} (${lineInfo.startTime})`);
        priceParlay._lastFailure = { reason: 'unknown start time', detail: `${legLabel} has invalid startTime ${lineInfo.startTime}`, blockerLeg: legDescriptor };
        return null;
      }
      if (nowMs > startMs) {
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
    if (isStaleCached(lineInfo.sport)) {
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

    // Pre-compute whether we need Pinnacle line verification for this leg.
    // (The conditional that used to run after getFairProbAsync — moved here so
    // we can fire it in parallel with the fair-prob fetch.)
    let needsVerify = false;
    let verifyCachedLine = null;
    if ((lineInfo.oddsApiMarket === 'spreads' || lineInfo.oddsApiMarket === 'totals') && lineInfo.line != null) {
      const event = oddsFeed.getEventMarkets(
        lineInfo.oddsApiSport, lineInfo.homeTeam, lineInfo.awayTeam, lineInfo.startTime
      );
      const cachedLine = event?.markets?.[lineInfo.oddsApiMarket]?.line;
      if (cachedLine != null && Math.abs(Math.abs(cachedLine) - Math.abs(lineInfo.line)) < 0.01) {
        needsVerify = true;
        verifyCachedLine = cachedLine;
      }
    }

    legStates.push({ lineId, lineInfo, legLabel, legDescriptor, needsVerify, verifyCachedLine });
  }

  // ------------------- PHASE 2: Parallel async fetches per leg ------------------
  // getFairProbAsync can trigger an alt-line fetch (expensive). verifyLineWithPinnacle
  // hits the Pinnacle supplement cache / Odds API. Running these in parallel caps
  // pricing time at a single worst-case fetch regardless of leg count.
  const fairProbPromises = legStates.map(s => {
    // Series-winner legs: PX sends them as moneyline with team="Team (Series)".
    // Our odds feeds don't carry series markets, so bypass oddsFeed and
    // look up from the DK scraper cache (fetched out-of-band).
    const seriesFair = getSeriesFairProb(s.lineInfo);
    if (seriesFair != null) {
      if (typeof seriesFair === 'object') {
        s.bookPriceOverride = seriesFair.bookPriceOverride;
        return Promise.resolve(seriesFair.fairProb);
      }
      return Promise.resolve(seriesFair);
    }

    // MMA moneyline legs: DK scraper is the source of truth and the
    // merged oddsFeed cache is prone to wipe by unrelated SharpAPI
    // refreshes. Route directly to dkScraper (see getMmaFairProb).
    const mmaFair = getMmaFairProb(s.lineInfo);
    if (mmaFair != null) return Promise.resolve(mmaFair);

    // Golf matchup legs: route through the round-aware DataGolf lookup
    // so we price a round RFQ against round odds (not tournament h2h).
    const golfFair = getGolfMatchupFairProb(s.lineInfo);
    if (golfFair != null) return Promise.resolve(golfFair);

    if (s.lineInfo.isDNB) {
      // Draw-No-Bet is sync (derives from cached 3-way h2h).
      return Promise.resolve(oddsFeed.getDNBFairProb(
        s.lineInfo.oddsApiSport, s.lineInfo.homeTeam, s.lineInfo.awayTeam,
        s.lineInfo.oddsApiSelection, s.lineInfo.startTime
      ));
    }
    return oddsFeed.getFairProbAsync(
      s.lineInfo.oddsApiSport,
      s.lineInfo.homeTeam,
      s.lineInfo.awayTeam,
      s.lineInfo.oddsApiMarket,
      s.lineInfo.oddsApiSelection,
      s.lineInfo.line,
      s.lineInfo.startTime
    );
  });
  const verifyPromises = legStates.map(s => {
    if (!s.needsVerify) return Promise.resolve(null);
    return oddsFeed.verifyLineWithPinnacle(
      s.lineInfo.oddsApiSport, s.lineInfo.homeTeam, s.lineInfo.awayTeam,
      s.lineInfo.oddsApiMarket, s.verifyCachedLine
    );
  });
  const [fairProbs, verifyResults] = await Promise.all([
    Promise.all(fairProbPromises),
    Promise.all(verifyPromises),
  ]);

  // -------------------- PHASE 3: Post-process results per leg -------------------
  for (let legIdx = 0; legIdx < legStates.length; legIdx++) {
    const { lineId, lineInfo, legLabel, legDescriptor, bookPriceOverride } = legStates[legIdx];
    const fairProb = fairProbs[legIdx];
    const verifyResult = verifyResults[legIdx];

    if (lineInfo.isDNB && fairProb != null) {
      log.debug('Pricing', `DNB derived fair prob ${fairProb.toFixed(4)} for ${legLabel}`);
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
    // The fairProb null check above already catches truly blind legs.

    // Spread/total line verification: check result from Phase 2. When the
    // requested line matches our cached primary, verifyLineWithPinnacle
    // confirms the line hasn't moved. Prevents pricing stale alt-spreads
    // at primary-spread odds.
    if (verifyResult && !verifyResult.ok) {
      log.info('Pricing', `Declined: spread line moved for ${legLabel} (cached ${legStates[legIdx].verifyCachedLine}, Pinnacle now ${verifyResult.currentLine})`);
      priceParlay._lastFailure = {
        reason: 'spread line moved',
        detail: `${legLabel}: cached line ${legStates[legIdx].verifyCachedLine} but Pinnacle now ${verifyResult.currentLine} (moved ${verifyResult.diff.toFixed(1)}pts)`,
        blockerLeg: legDescriptor,
      };
      return null;
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
      bookPriceOverride: bookPriceOverride != null ? bookPriceOverride : null,
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

  // Smoothly scaling vig: linear ramp above fairProb 0.50.
  // Replaces the previous 2-step floor (2.5% @ 0.70, 3% @ 0.80) — the
  // step function gave identical vig at p=0.81 and p=0.95 even though
  // the long tail needs more bite to prevent compounding leaks on
  // heavy favorites in multi-leg parlays.
  //
  // Formula: vig = max(floor, baseVig + slope * (fairProb - 0.5))
  // Slope and floor are Railway-tunable via VIG_FAVORITE_SLOPE and
  // VIG_FAVORITE_FLOOR env vars.
  //
  // Default slope 0.075 sample (base 1%):
  //   p=0.60: 1.75%   p=0.70: 2.50%   p=0.75: 2.875%
  //   p=0.80: 3.25%   p=0.85: 3.625%  p=0.90: 4.00%
  //   p=0.95: 4.375%  p=1.00: 4.75%
  // Hits the old 2.5% floor exactly at p=0.70 (no regression at the
  // standard heavy-favorite band), exceeds the old 3% at p≥0.80, and
  // grows ~1.4pp into the long tail where the old step was bleeding.
  const favSlope = config.pricing.vigFavoriteSlope;
  const favFloor = config.pricing.vigFavoriteFloor;
  const seriesMinVig = config.pricing.vigSeriesMin || 0;
  function getEffectiveVig(fairProb, sport, marketType) {
    const baseVig = getBaseVigForSport(sport);
    let vig;
    if (fairProb <= 0.5) {
      vig = baseVig;
    } else {
      const ramp = baseVig + favSlope * (fairProb - 0.5);
      vig = favFloor > 0 ? Math.max(favFloor, ramp) : ramp;
    }
    // Series-winner legs get a configurable floor on top of the
    // normal vig — typically the only SP quoting these on PX, so we
    // can widen the spread without losing order flow.
    if (marketType === 'series_winner' && seriesMinVig > 0) {
      vig = Math.max(vig, seriesMinVig);
    }
    // MMA legs (moneyline + total rounds) — same logic: low-comp
    // market on PX, we can widen without losing flow.
    const mmaMinVig = config.pricing.vigMmaMin || 0;
    if (sport === 'mma_mixed_martial_arts' && mmaMinVig > 0) {
      vig = Math.max(vig, mmaMinVig);
    }
    return vig;
  }

  function applyOddsVig(fairProb, sport, marketType) {
    const vig = getEffectiveVig(fairProb, sport, marketType);
    const fairDecimal = 1 / fairProb;
    const payout = fairDecimal - 1; // the profit portion
    const viggedPayout = payout * (1 - vig); // reduce payout by vig %
    return 1 / (1 + viggedPayout); // convert back to implied prob
  }

  // ---------------------------------------------------------------------
  // VIG APPLICATION — two modes, A/B-testable via config.pricing.parlayLevelVig
  //
  // PER-LEG MODE (default, legacy):
  //   Apply vig to each leg independently, multiply the vigged per-leg
  //   probs. This COMPOUNDS the vig. For a 5-leg parlay at 2% per leg,
  //   effective parlay vig ≈ 4.2% (meaningfully uncompetitive).
  //
  // PARLAY-LEVEL MODE (experimental):
  //   Apply vig ONCE at the parlay level using the MAX per-leg effective
  //   rate. The max preserves sport-aware pricing (highest sport wins)
  //   and favorite-ramp protection (any leg triggering the ramp pulls
  //   the whole parlay's vig up). Eliminates multi-leg compounding.
  //
  // Observed data: per-leg win rate collapses at 4+ legs (28%→14%→9%)
  // suggesting competitors don't compound. A/B test via:
  //   POST /config/vig {parlayLevelVig:true}  — enable
  //   POST /config/vig {parlayLevelVig:false} — disable
  // Watch win rate by leg count before/after.
  // ---------------------------------------------------------------------
  let offeredImpliedProb;
  let vigMode;
  let vigRateUsed; // informational — the rate applied (for debugging/analytics)
  // Legs flagged with bookPriceOverride (e.g. NBA series heavy favorites
  // past -500 FV) bypass vig entirely — we quote DK's offered number for
  // those legs. Vig logic below applies only to the remaining legs.
  const overrideLegs = pricedLegs.filter(l => l.bookPriceOverride != null);
  const vigLegs = pricedLegs.filter(l => l.bookPriceOverride == null);
  const overrideProduct = overrideLegs.reduce((p, l) => p * l.bookPriceOverride, 1);

  if (config.pricing.parlayLevelVig) {
    // Parlay-level: single vig application using max per-leg rate (over vig legs only).
    const perLegVigs = vigLegs.map(l => getEffectiveVig(l.fairProb, l.lineInfo.sport, l.lineInfo.marketType));
    const maxVig = perLegVigs.length > 0 ? Math.max(...perLegVigs) : config.pricing.defaultVig;
    const vigFair = vigLegs.reduce((p, l) => p * l.fairProb, 1);
    if (vigLegs.length > 0) {
      const fairDecimal = 1 / vigFair;
      const payout = fairDecimal - 1;
      const viggedPayout = payout * (1 - maxVig);
      offeredImpliedProb = (1 / (1 + viggedPayout)) * overrideProduct;
    } else {
      offeredImpliedProb = overrideProduct;
    }
    vigMode = 'parlay-level';
    vigRateUsed = maxVig;
  } else {
    // Per-leg: vig applied to each leg's odds then compounded (legacy).
    offeredImpliedProb = overrideProduct;
    for (const leg of vigLegs) {
      offeredImpliedProb *= applyOddsVig(leg.fairProb, leg.lineInfo.sport, leg.lineInfo.marketType);
    }
    vigMode = 'per-leg';
    // For per-leg, expose the AVERAGE per-leg rate as the "used" value (vig legs only).
    const avgVig = vigLegs.length > 0
      ? vigLegs.reduce((s, l) => s + getEffectiveVig(l.fairProb, l.lineInfo.sport, l.lineInfo.marketType), 0) / vigLegs.length
      : 0;
    vigRateUsed = avgVig;
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
  // SGP correlation handling
  //
  // Same-game parlays have correlated legs that independent multiplication
  // doesn't capture. ML/spread + total on the same game are positively
  // correlated (favorites covering → more scoring → pushes over).
  //
  // Two-layer approach:
  //   1. Correlation discount: 3% implied prob boost on ML/spread + total
  //      SGP pairs. Calibrated from DK (+240 on Rangers ML + Over 9) vs
  //      independent (+253). DK discounts ~4%, Bookmaker ~0%, we split at 3%.
  //   2. Pin-match floor: if Pinnacle data available, ensure we never
  //      offer better than Pinnacle raw compound minus 0.5% edge.
  //      max(correlation-adjusted, pin-match) ensures we take the tighter
  //      of the two signals.
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
    // PX rejects any SGP offer priced above its internal correlation model
    // with "invalid estimated prices". Apr 2026 test: any boost → rejection,
    // zero boost → ~99% acceptance. We let PX's own correlation handling
    // price the dependency and only enforce a Pin-match floor below.

    // --- Pin-match floor ---
    // Compute Pinnacle raw compound implied prob using DNB-adjusted values
    // where applicable (soccer 2-way moneylines). If Pin data available,
    // ensure we don't offer better than Pin raw minus 0.5%.
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
          log.info('Pricing', `SGP pin-match tightening: ${(offeredImpliedProb*100).toFixed(2)}% → ${(pinMatchTarget*100).toFixed(2)}% (Pin raw ${(pinRawCompound*100).toFixed(2)}%)`);
          offeredImpliedProb = pinMatchTarget;
          pricingMethod = 'sgp_pin_match';
        } else {
          pricingMethod = 'sgp_pin_match_keep_baseline';
        }
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
  // NBA: no moneyline favorites beyond -300 (fairProb > 0.75)
  // Tennis: no moneyline favorites beyond -300 (fairProb > 0.75)
  for (const leg of pricedLegs) {
    if (leg.lineInfo.marketType !== 'moneyline') continue;
    const impliedOdds = leg.fairProb >= 0.5 ? Math.round(-100 * leg.fairProb / (1 - leg.fairProb)) : Math.round(100 * (1 - leg.fairProb) / leg.fairProb);
    if (leg.lineInfo.sport === 'basketball_nba' && leg.fairProb > 0.75) {
      log.debug('Pricing', `Declined: NBA moneyline ${leg.lineInfo.teamName} is heavy favorite (${impliedOdds})`);
      priceParlay._lastFailure = {
        reason: 'NBA heavy favorite',
        detail: `${leg.lineInfo.teamName} at ${impliedOdds} exceeds -300 limit`,
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
  const estimatedPrice = pricedLegs.map(leg => {
    const legImplied = leg.bookPriceOverride != null
      ? leg.bookPriceOverride
      : applyOddsVig(leg.fairProb, leg.lineInfo.sport, leg.lineInfo.marketType);
    return { line_id: leg.lineId, odds: decimalToAmerican(1 / legImplied) };
  });

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
          legVig: l.bookPriceOverride != null ? 0 : Math.round(getEffectiveVig(l.fairProb, l.lineInfo.sport, l.lineInfo.marketType) * 10000) / 10000,
          bookPriceOverride: l.bookPriceOverride != null ? Math.round(l.bookPriceOverride * 10000) / 10000 : null,
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
          // Golf matchup metadata — drives the R1/R2/Tournament tag in the
          // dashboard. Null on non-golf legs.
          roundNum: l.lineInfo.roundNum ?? null,
          matchupType: l.lineInfo.matchupType ?? null,
          tournamentName: l.lineInfo.tournamentName ?? null,
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
      vig: Math.round(pricedLegs.reduce((s, l) => s + getEffectiveVig(l.fairProb, l.lineInfo.sport, l.lineInfo.marketType), 0) / pricedLegs.length * 10000) / 10000,
      // Which vig application mode was used for this quote. Recorded per-quote
      // so /market-intel can split win rate by mode for A/B analysis.
      vigMode,
      vigRateUsed: Math.round((vigRateUsed || 0) * 10000) / 10000,
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

  // Check all legs are known and events haven't started.
  // Uses pre-computed lineInfo.startTimeMs (parsed lazily on first lookupLine
  // and cached on the object) to avoid re-parsing the ISO string per RFQ.
  const resolvedLegs = [];
  const nowMs = Date.now();
  for (const leg of legs) {
    const lineId = leg.line_id || leg.lineId || leg;
    const lineInfo = lineManager.lookupLine(lineId);
    if (!lineInfo) return { declined: true, reason: 'unknown legs', detail: null };

    const isGolfMatchup = lineInfo.sport === 'golf_matchups' || lineInfo.oddsApiSport === 'golf_matchups';
    const startMs = lineInfo.startTimeMs;
    if (startMs != null) {
      if (isNaN(startMs)) {
        return { declined: true, reason: 'unknown start time', detail: `${lineInfo.teamName || '?'} (${lineInfo.sport || '?'}) has invalid startTime ${lineInfo.startTime} — cannot verify game hasn't started` };
      }
      if (nowMs > startMs) {
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
    // Block: ANY sub-game market (F5 / half / quarter / period / inning)
    // paired with ANY full-game market on the same event. A sub-game
    // market is a strict subset of the full game, so the two are always
    // correlated — whatever happens in the first 5 innings / first half
    // affects the full-game outcome mechanically. Previously only
    // matched-type pairs (F5 ML + full ML, etc.) were blocked; the
    // cross pairs (F5 ML + full total, F5 total + full ML, etc.) leaked
    // through and landed in the SGP "other" correlation bucket, which
    // doesn't capture the true mechanical dependence.
    //
    // Regex covers:
    //   F5 baseball:   first_5_innings_*, first_five_innings_*, *_f5
    //   Halves:        first_half_*, 1st_half_*, 2nd_half_*, *_h1, *_h2
    //   Quarters:      first_quarter_*, 1st_quarter_*..4th_quarter_*, *_q1..*_q4
    //   Periods (NHL): first_period_*, 1st_period_*..3rd_period_*, *_p1..*_p3
    //   Innings:       1st_inning_*..9th_inning_*
    const subGamePattern =
      /first_5_innings|first_five_innings|\b_f5\b|_f5_|\bf5_|(first|1st|2nd|3rd|4th)_half|_h[12]\b|(first|1st|2nd|3rd|4th)_quarter|_q[1-4]\b|(first|1st|2nd|3rd)_period|_p[1-3]\b|(1st|2nd|3rd|4th|5th|6th|7th|8th|9th)_inning/i;
    const fullGameTypes = new Set(['moneyline', 'spread', 'total', 'team_total', 'btts', 'both_teams_to_score', 'double_chance']);
    const hasSubGame = entries.some(e => subGamePattern.test(String(e.market || '')));
    const hasFullGame = entries.some(e => fullGameTypes.has(String(e.market || '').toLowerCase()));
    if (hasSubGame && hasFullGame) {
      const subLeg = entries.find(e => subGamePattern.test(String(e.market || '')));
      const fullLeg = entries.find(e => fullGameTypes.has(String(e.market || '').toLowerCase()));
      log.info('Pricing', `Declined: sub-game ${subLeg.market} + full-game ${fullLeg.market} on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `sub-game (${subLeg.market}) + full-game (${fullLeg.market}) on same event: ${gameLabel}` };
    }
    // Also block two sub-game markets of the same period on the same
    // event (e.g. F5 ML + F5 total) — same mechanical correlation
    // between score and who's winning within that narrower window.
    const subGameLegs = entries.filter(e => subGamePattern.test(String(e.market || '')));
    if (subGameLegs.length >= 2) {
      log.info('Pricing', `Declined: multiple sub-game legs on ${gameLabel}: ${subGameLegs.map(e => e.market).join(' + ')}`);
      return { declined: true, reason: 'correlated legs', detail: `multiple sub-game legs on same event: ${gameLabel} (${subGameLegs.map(e => e.market).join(', ')})` };
    }
    // ---- TEAM TOTAL: block ALL same-game combinations ----
    // team_total is strongly correlated with every other market on the same
    // game (game total, spread, moneyline, other team's total). Block
    // unconditionally whenever team_total appears with any other leg.
    const hasTeamTotal = types.includes('team_total');
    if (hasTeamTotal && entries.length > 1) {
      const otherMarket = entries.find(e => e.market !== 'team_total')?.market || 'other';
      log.info('Pricing', `Declined: team_total + ${otherMarket} on same game ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `team_total + ${otherMarket} on same game: ${gameLabel}` };
    }

    // ---- SERIES: mirror single-game correlation rules ----
    // Block highly-correlated pairs on the same series event. The
    // duplicate-types check above already handles two series_winner /
    // two series_spread / two series_total. Here we only need the
    // cross-type cases.
    //   BLOCK: series_winner + series_spread (both tied to outcome)
    //   ALLOW: series_winner OR series_spread + series_total
    const hasSeriesWinner = types.includes('series_winner');
    const hasSeriesSpread = types.includes('series_spread');
    if (hasSeriesWinner && hasSeriesSpread) {
      log.info('Pricing', `Declined: series_winner + series_spread on ${gameLabel}`);
      return { declined: true, reason: 'correlated legs', detail: `series winner + series spread on same series: ${gameLabel}` };
    }

    // ---- 1ST HALF: block any H1 + full-game on same game ----
    // 1st half outcomes are a subset of full-game outcomes — heavily correlated.
    const h1Pattern = /^(first_half_|1st_half_)/;
    const hasH1 = types.some(t => h1Pattern.test(t));
    if (hasH1) {
      const hasFullGame = types.some(t => !h1Pattern.test(t) && !/^first_5_innings|^first_five_innings/.test(t));
      if (hasFullGame) {
        log.info('Pricing', `Declined: 1st half + full-game on ${gameLabel}`);
        return { declined: true, reason: 'correlated legs', detail: `1st half + full-game on same game: ${gameLabel}` };
      }
    }
    // Note: same-game spread+total and moneyline+total are NOT blocked here
    // because they're still reasonable to quote — SGP correlation is handled
    // at pricing time via Pin-match logic (pinMatchTarget in priceParlay).
  }

  // ---- CROSS-EVENT SERIES CORRELATION ----
  // Series events and the underlying game events have DIFFERENT pxEventIds
  // (e.g. Series Winner - DEN vs MIN is event 1500006589, Game 1 MIN @ DEN
  // is a separate event). The per-pxEventId grouping above can't catch them
  // paired. Group again by normalized home/away team pair across all legs:
  //   - series_spread + any game leg (moneyline/spread/total/team_total)
  //     between the same two teams: blocked (series outcome is tied to each
  //     game's outcome, high correlation)
  //   - multiple series_spread legs on the same series (alt lines on same
  //     matchup): blocked (same bet at different breakpoints)
  {
    const normName = (s) => (s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/\s*\(series\)\s*/g, '').replace(/[^a-z0-9 ]/g, '').trim();
    const pairKey = (h, a) => {
      const nh = normName(h), na = normName(a);
      if (!nh || !na) return null;
      return [nh, na].sort().join('|');
    };
    const byPair = {};
    for (const l of resolvedLegs) {
      const key = pairKey(l.lineInfo.homeTeam, l.lineInfo.awayTeam);
      if (!key) continue;
      if (!byPair[key]) byPair[key] = [];
      byPair[key].push({ market: l.lineInfo.marketType, team: l.lineInfo.teamName, line: l.lineInfo.line });
    }
    for (const [key, entries] of Object.entries(byPair)) {
      if (entries.length <= 1) continue;
      const seriesSpreadLegs = entries.filter(e => e.market === 'series_spread');
      if (seriesSpreadLegs.length === 0) continue;
      if (seriesSpreadLegs.length >= 2) {
        log.info('Pricing', `Declined: multiple series_spread legs on same series (${key})`);
        return { declined: true, reason: 'correlated legs', detail: `multiple series spread lines on same series: ${key}` };
      }
      const gameTypes = ['moneyline', 'spread', 'total', 'team_total'];
      const hasGameLeg = entries.some(e => gameTypes.includes(e.market));
      if (hasGameLeg) {
        log.info('Pricing', `Declined: series_spread + game leg on same matchup (${key})`);
        return { declined: true, reason: 'correlated legs', detail: `series spread + individual game market on same matchup: ${key}` };
      }
    }
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

  // On success, surface the resolved lineInfos keyed by lineId so the caller
  // can pass them to priceParlay and skip redundant lookupLine() calls.
  const resolvedLineInfos = new Map();
  for (const r of resolvedLegs) resolvedLineInfos.set(r.lineId, r.lineInfo);
  return { declined: false, resolvedLineInfos };
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
