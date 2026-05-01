const { config, getBankroll } = require('../config');
const log = require('./logger');
const lineManager = require('./line-manager');
const oddsFeed = require('./odds-feed');
const orderTracker = require('./order-tracker');
const dkScraper = require('./dk-scraper');
const templateExposure = require('./template-exposure');
const { performance } = require('perf_hooks');
const crypto = require('crypto');
const v2 = require('./v2');

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

/**
 * Detect whether a leg is an "alt spread" on a sport that's currently
 * blocked. The block list comes from config.pricing.blockAltSpreadSports
 * (env BLOCK_ALT_SPREAD_SPORTS, default "baseball_mlb,icehockey_nhl,
 * basketball_nba"). Returns null if not blocked, or a human-readable
 * reason string if it should be declined.
 *
 * Detection rules per sport:
 *
 *   MLB / NHL — primary run/puck line is always ±1.5. Allow any leg
 *   with |line| === 1.5 (intentionally includes the OPPOSITE-symbol
 *   alt: if primary is Team A −1.5, also allow Team A +1.5 — the
 *   heavy-favorite alt where the same fav gets a head-start instead).
 *   Block any other magnitude (±0.5, ±2.5, etc.).
 *
 *   The opposite-symbol alts route through odds-feed's signed-key
 *   altSpreads cache (keyed by signed home-perspective home_point), so
 *   "Team A +1.5 (away)" lands in bucket "−1.5" and "Team A −1.5
 *   (away)" lands in bucket "+1.5" — strictly different buckets, no
 *   collision risk. See odds-feed.js getAltLineFairProb() and
 *   spreadHomePoint() for the routing.
 *
 *   NBA — primary spread varies per game. Allow legs whose line is
 *   within ±config.pricing.nbaAltSpreadMaxDistance points of the
 *   primary (in home-team perspective), AND which have coverage in
 *   our alt-lines cache (i.e., at least one book reported odds for
 *   this exact line). Block legs farther than the threshold OR with
 *   no book coverage (we'd have to derive odds ourselves — operator
 *   doesn't want that). Primary lines (onDemand=false) always pass.
 *
 *   Other blocked sports — fall back to the onDemand=true proxy
 *   (RFQ asked for a line we hadn't pre-registered → block).
 *
 * Why this exists: Apr 25 forensic showed alt-spread legs concentrated
 * in the low-fair-prob "red box" region where realized EV/$ was -6%;
 * one recurring 2-leg MLB template (Rockies +1.5 + Under) drove ~$5.8k
 * of loss alone. Block is sport-list driven so we can re-enable per
 * sport without a code change.
 */
function isBlockedAltSpread(lineInfo) {
  if (!lineInfo) return null;
  if (lineInfo.marketType !== 'spread') return null;
  const sport = lineInfo.sport || '';
  const blocked = config.pricing.blockAltSpreadSports || [];
  if (!blocked.includes(sport)) return null;
  // Spread legs with no line value are anomalous (probably won't price either).
  // Don't claim "alt" without an actual line to compare against — let the
  // downstream pricing path reject them on its own terms.
  if (lineInfo.line == null) return null;
  const lineNum = Number(lineInfo.line);
  if (!Number.isFinite(lineNum)) return null;
  const absLine = Math.abs(lineNum);
  // NHL: discrete allowlist of |line| values (default ±0.5, ±1.0, ±1.5
  // per Mike 2026-05-01). Mirrors MLB pattern — primary ±1.5 passes
  // without coverage check; non-primary alts require book coverage in
  // the alt-spread cache so we never derive a line ourselves.
  if (sport === 'icehockey_nhl') {
    const allowed = config.pricing.nhlAllowedPuckLines || [0.5, 1.0, 1.5];
    const isAllowed = allowed.some(v => Math.abs(absLine - v) < 0.001);
    if (!isAllowed) {
      return `NHL puck line ${absLine} not in allowed set [${allowed.join(', ')}]`;
    }
    // Primary ±1.5 — no coverage check needed
    if (Math.abs(absLine - 1.5) < 0.001) return null;
    // Non-primary alt — require book coverage
    const sel = lineInfo.oddsApiSelection || lineInfo.selection;
    if (lineInfo.homeTeam && lineInfo.awayTeam) {
      const eventKey = oddsFeed.normalizeEventKey(lineInfo.homeTeam, lineInfo.awayTeam);
      const cacheEntry = oddsFeed.getAltLineCacheEntry(eventKey, 'spreads', sel, lineNum);
      if (!cacheEntry || !cacheEntry.books || cacheEntry.books === 0) {
        return `NHL alt puck line: no book coverage for line ${absLine} (cache miss or 0 books)`;
      }
    } else {
      return `NHL alt puck line: missing team names, can't verify book coverage`;
    }
    return null;
  }
  // MLB: discrete allowlist of |line| values (default ±0.5 and ±1.5).
  // Each non-primary value also requires alt-spread cache book coverage —
  // mirrors the NBA carve-out's coverage gate so we never derive a line
  // ourselves. Primary ±1.5 passes without coverage check.
  if (sport === 'baseball_mlb') {
    const allowed = config.pricing.mlbAllowedRunLines || [0.5, 1.5];
    const isAllowed = allowed.some(v => Math.abs(absLine - v) < 0.001);
    if (!isAllowed) {
      return `MLB run line ${absLine} not in allowed set [${allowed.join(', ')}]`;
    }
    // Primary ±1.5 — no coverage check needed
    if (Math.abs(absLine - 1.5) < 0.001) return null;
    // Non-primary alt — require book coverage
    const sel = lineInfo.oddsApiSelection || lineInfo.selection;
    if (lineInfo.homeTeam && lineInfo.awayTeam) {
      const eventKey = oddsFeed.normalizeEventKey(lineInfo.homeTeam, lineInfo.awayTeam);
      const cacheEntry = oddsFeed.getAltLineCacheEntry(eventKey, 'spreads', sel, lineNum);
      if (!cacheEntry || !cacheEntry.books || cacheEntry.books === 0) {
        return `MLB alt run line: no book coverage for line ${absLine} (cache miss or 0 books)`;
      }
    } else {
      return `MLB alt run line: missing team names, can't verify book coverage`;
    }
    return null;
  }
  // NBA: within-±N alts with book coverage allowed.
  if (sport === 'basketball_nba') {
    // Find primary spread for this event in home-team perspective.
    const primaryHomePoint = lineManager.getPrimarySpreadHomePoint(lineInfo.pxEventId);
    if (primaryHomePoint == null) {
      return `NBA alt: no primary spread registered for event ${lineInfo.pxEventId}`;
    }
    // Convert this leg's line to home-team-signed perspective.
    const sel = lineInfo.oddsApiSelection || lineInfo.selection;
    const legHomePoint = sel === 'home' ? lineNum : -lineNum;
    // If THIS leg IS the primary line, allow without further checks.
    // Same fix class as isBlockedAltTotal (operator-caught Apr 26):
    // `onDemand !== true` was being used as a proxy for "is primary",
    // but PX seeds many alt-spreads as non-on-demand entries during
    // regular seeding — they bypassed the distance check. Use the
    // line-value match against primary as the actual truth signal.
    if (Math.abs(legHomePoint - primaryHomePoint) < 0.001) return null;
    const dist = Math.abs(legHomePoint - primaryHomePoint);
    const maxDist = config.pricing.nbaAltSpreadMaxDistance || 2.0;
    // Tiny floating-point tolerance (NBA spreads come in 0.5 increments
    // so values are exact, but be defensive).
    if (dist > maxDist + 0.001) {
      return `NBA alt outside ±${maxDist} of primary (alt=${legHomePoint}, primary=${primaryHomePoint}, dist=${dist})`;
    }
    // Verify book coverage: only allow alts that came from real books.
    // No derived/inferred lines. Cache miss OR books=0 → block.
    if (lineInfo.homeTeam && lineInfo.awayTeam) {
      const eventKey = oddsFeed.normalizeEventKey(lineInfo.homeTeam, lineInfo.awayTeam);
      const cacheEntry = oddsFeed.getAltLineCacheEntry(eventKey, 'spreads', sel, lineNum);
      if (!cacheEntry || !cacheEntry.books || cacheEntry.books === 0) {
        return `NBA alt: no book coverage for line ${legHomePoint} (cache miss or 0 books)`;
      }
    } else {
      // Without home/away team names we can't look up the cache —
      // conservatively block so we never derive an alt line ourselves.
      return `NBA alt: missing team names, can't verify book coverage`;
    }
    return null; // within range + has book coverage
  }
  // Other blocked sports: fall back to onDemand proxy.
  if (lineInfo.onDemand === true) {
    return `${sport} alt spread (virtually-registered line ${lineNum})`;
  }
  return null;
}

/**
 * Alt-totals carve-out check. Allows alt totals within a sport-specific
 * distance of the primary O/U line AND only if the line has real book
 * coverage in our alt-totals cache (no derived/inferred lines).
 *
 * Sports gated:
 *   - NBA: ±config.pricing.nbaAltTotalMaxDistance (default 2.0)
 *   - MLB: ±config.pricing.mlbAltTotalMaxDistance (default 1.5)
 * Other sports pass through unrestricted (subject to other rules).
 *
 * Returns null when the leg is allowed, or a human-readable reason
 * string when it should be declined.
 */
function isBlockedAltTotal(lineInfo) {
  if (!lineInfo) return null;
  if (lineInfo.marketType !== 'total') return null;
  const sport = lineInfo.sport || '';
  let maxDist;
  let label;
  if (sport === 'basketball_nba') {
    maxDist = config.pricing.nbaAltTotalMaxDistance || 2.0;
    label = 'NBA';
  } else if (sport === 'baseball_mlb') {
    maxDist = config.pricing.mlbAltTotalMaxDistance || 1.5;
    label = 'MLB';
  } else {
    return null;
  }
  // Need a line value to compare
  if (lineInfo.line == null) return null;
  const lineNum = Number(lineInfo.line);
  if (!Number.isFinite(lineNum)) return null;
  const absLine = Math.abs(lineNum);
  // Find primary total for this event
  const primaryTotal = lineManager.getPrimaryTotalLine(lineInfo.pxEventId);
  if (primaryTotal == null) {
    return `${label} alt-total: no primary total registered for event ${lineInfo.pxEventId}`;
  }
  // If THIS leg IS the primary line, allow without further checks.
  if (Math.abs(absLine - primaryTotal) < 0.001) return null;
  // Alt: must be within ±N of primary
  const dist = Math.abs(absLine - primaryTotal);
  if (dist > maxDist + 0.001) {
    return `${label} alt-total outside ±${maxDist} of primary (alt=${absLine}, primary=${primaryTotal}, dist=${dist})`;
  }
  // Verify book coverage exists in alt-totals cache (no derived lines)
  if (lineInfo.homeTeam && lineInfo.awayTeam) {
    const eventKey = oddsFeed.normalizeEventKey(lineInfo.homeTeam, lineInfo.awayTeam);
    const cacheEntry = oddsFeed.getAltLineCacheEntry(eventKey, 'totals', lineInfo.oddsApiSelection || lineInfo.selection, lineNum);
    if (!cacheEntry || !cacheEntry.books || cacheEntry.books === 0) {
      return `${label} alt-total: no book coverage for line ${absLine} (cache miss or 0 books)`;
    }
  } else {
    return `${label} alt-total: missing team names, can't verify book coverage`;
  }
  return null;
}

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

  // NBA series heavy-favorite: beyond the config'd FV cap (default -250),
  // quote DK's offered price directly (no vig) instead of our
  // de-vigged-plus-vig number. Avoids drifting out of market on extreme
  // favorites where our ramp would produce an uncompetitive line.
  // Applies ONLY to series_winner (the "moneyline" of series betting).
  // Series spread and series total pass through normally — those lines
  // are calibrated to ~50/50 and the fav-cap concept doesn't apply.
  if (sportKey === 'nba' && isSeriesWinner) {
    // Convert config'd American odds cap → implied probability threshold.
    // Negative American odds -N → N / (N + 100). Positive odds +N → 100 / (N + 100).
    const capAm = config.pricing.nbaSeriesFavoriteCapAmericanOdds || -250;
    const absCap = Math.abs(capAm);
    const threshProb = capAm < 0 ? absCap / (absCap + 100) : 100 / (absCap + 100);
    if (hit.fairProb > threshProb) {
      let bookDec = hit.decimalOdds;
      if ((!bookDec || bookDec <= 1) && hit.americanOdds != null) {
        bookDec = hit.americanOdds >= 0
          ? 1 + hit.americanOdds / 100
          : 1 + 100 / Math.abs(hit.americanOdds);
      }
      if (bookDec && bookDec > 1) {
        const bookImplied = 1 / bookDec;
        log.info('Pricing', `NBA series heavy fav ${teamName} ${mt || ''} fair ${hit.fairProb.toFixed(4)} > ${capAm} cutoff — using DK book price ${hit.americanOdds} (implied ${bookImplied.toFixed(4)})`);
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

  // PRIORITY 1: Manual book upload (operator-supplied Bookmaker / BetOnline
  // odds via /betonline-zurich/upload). When present, this is the
  // operator's authoritative quoting price — overrides DataGolf and DK
  // even when those have coverage. Without this priority order, DataGolf's
  // de-vigged fair would silently win on any regular-tour event where
  // DataGolf has h2h coverage, and the manual upload would be a no-op.
  //
  // Return shape: { fairProb, bookPriceOverride } so priceParlay quotes
  // AT the raw odds (no de-vig + re-add vig). Mirrors the NBA series
  // heavy-favorite pattern (pricer.js:~290).
  const teamName = lineInfo?.teamName || '';
  if (teamName) {
    try {
      const betonlineScraper = require('./betonline-scraper');
      const boHit = betonlineScraper.lookupZurichMatchupFairProb(teamName, lineInfo.roundNum ?? null);
      if (boHit && boHit.fairProb != null) {
        const rawAm = boHit.americanOdds;
        let rawImplied = boHit.impliedProb;
        if (rawImplied == null && Number.isFinite(rawAm)) {
          rawImplied = rawAm >= 0 ? 100 / (rawAm + 100) : -rawAm / (-rawAm + 100);
        }
        if (rawImplied != null && rawImplied > 0 && rawImplied < 1) {
          return { fairProb: boHit.fairProb, bookPriceOverride: rawImplied };
        }
        return boHit.fairProb;
      }
    } catch (_) { /* scraper unavailable — fall through */ }
  }

  // PRIORITY 2: DataGolf via oddsFeed.getGolfMatchupEvent. Covers
  // individual 1v1 player matchups for regular tour events.
  const event = oddsFeed.getGolfMatchupEvent(
    lineInfo.homeTeam, lineInfo.awayTeam, lineInfo.roundNum ?? null
  );
  if (event) {
    const h2h = event.markets?.h2h;
    if (h2h) {
      const sel = lineInfo.oddsApiSelection;
      const side = sel === 'home' ? h2h.home : sel === 'away' ? h2h.away : null;
      if (side && side.fairProb != null) return side.fairProb;
    }
  }

  // PRIORITY 3: DK scraper. Last-resort fallback for matchups DataGolf
  // happens to miss. Looked up by the full team-pair name so the
  // pairing-name match works regardless of home/away orientation.
  if (teamName) {
    const dkHit = dkScraper.lookupGolfMatchupFairProb(teamName, lineInfo.roundNum ?? null);
    if (dkHit && dkHit.fairProb != null) return dkHit.fairProb;
  }
  return null;
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

/**
 * Standalone (not-inside-priceParlay) vig computation for a single leg.
 * Mirrors the closures inside priceParlay: base sport vig → favorite ramp
 * → series/MMA floors. Used by the /lines/detail endpoint to show "what
 * would I quote on this single leg if someone asked for it alone?"
 * Not used in the actual pricing path — that stays in priceParlay.
 */
function computeSingleLegVig(fairProb, sport, marketType) {
  if (!(fairProb > 0 && fairProb < 1)) return null;
  const vigBySport = config.pricing.vigBySport || {};
  const baseVig = (sport && vigBySport[sport] != null) ? vigBySport[sport] : config.pricing.defaultVig;
  // Player props: flat floor at vigPropFloor (default 3%). Mirrors the
  // priceParlay-internal getEffectiveVig logic for player_<type> markets.
  if (marketType && /^player_/.test(marketType)) {
    return Math.max(config.pricing.vigPropFloor || 0, baseVig);
  }
  const favSlope = config.pricing.vigFavoriteSlope;
  const favFloor = config.pricing.vigFavoriteFloor;
  const seriesMinVig = config.pricing.vigSeriesMin || 0;
  const mmaMinVig = config.pricing.vigMmaMin || 0;
  let vig;
  if (fairProb <= 0.5) {
    vig = baseVig;
  } else {
    const ramp = baseVig + favSlope * (fairProb - 0.5);
    vig = favFloor > 0 ? Math.max(favFloor, ramp) : ramp;
  }
  if (marketType === 'series_winner' && seriesMinVig > 0) vig = Math.max(vig, seriesMinVig);
  if (sport === 'mma_mixed_martial_arts' && mmaMinVig > 0) vig = Math.max(vig, mmaMinVig);
  return vig;
}

/**
 * Given a fair probability + leg metadata, return what we'd quote if this
 * were the ONLY leg of a 1-leg "parlay". Returns { vig, impliedProb,
 * americanOdds } or null if inputs are invalid.
 */
function computeSingleLegQuote(fairProb, sport, marketType) {
  const vig = computeSingleLegVig(fairProb, sport, marketType);
  if (vig == null) return null;
  const fairDecimal = 1 / fairProb;
  const payout = fairDecimal - 1;
  const viggedPayout = payout * (1 - vig);
  const impliedProb = 1 / (1 + viggedPayout);
  const decimalOdds = 1 / impliedProb;
  return {
    vig,
    impliedProb,
    americanOdds: decimalToAmerican(decimalOdds),
  };
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
// NOTE: priceParlay is intentionally NOT declared `async`. An `async`
// function always returns a Promise, which forces the websocket caller
// to `await` it — and `await` always pays one V8 microtask hop (~0.32ms
// p50 measured Apr 26). On the cache-warm fast path (most RFQs), no
// async work is needed inside priceParlay, so returning the result
// SYNCHRONOUSLY skips that microtask cost entirely. The caller does:
//   const r = pricer.priceParlay(...);
//   const result = (r && typeof r.then === 'function') ? await r : r;
// On RFQs that DO need async work (alt-line cache miss or Pinnacle
// verify), priceParlay returns a Promise that resolves to the result.
function priceParlay(legs, opts = {}) {
  priceParlay._lastFailure = null; // clear any prior failure
  // Latency diagnostic — captures function entry to surface in _timings.
  // Lets the websocket layer compute "entry-to-phase1" gap (work before
  // sync validation begins: opts unpacking, top-level config reads,
  // team_total decline loop). Apr 26: 1.27ms p50 total had ~0.31ms of
  // unaccounted time inside priceParlay despite phase1+2+3 only summing
  // to 0.37ms — these markers find where it lives.
  const entryMs = performance.now();
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

  // Phase-timing markers. We emit these on the happy path via the
  // result.meta._timings field so the websocket layer can decompose the
  // composite "price" stage. Failure paths (early returns) skip this —
  // only successful offers produce timings.
  const phaseStartMs = performance.now();
  let phase1EndMs = null;
  let phase2EndMs = null;

  // ---------------------------------------------------------------------
  // DEFENSIVE DECLINE: team_total legs
  //
  // Verified 2026-04-23: a confirmed 4-leg parlay (019dbae7-632b-7647)
  // contained an "ATL team_total Over 4.5" leg priced at our fair 45.4%
  // while FanDuel quoted -136 (~55% fair after de-vig). The primary
  // cache for that event held team_total Over at -136 paired with
  // Under at -225 — a 26.9% overround that indicates the consensus
  // builder paired an Over 4.5 with a DIFFERENT-line Under (probably
  // Under 5.5 or 6.5). Proportional de-vig then produced a 45/55
  // split when the real fair was ~55/45, a ~10pp systematic bias on
  // the losing side. Estimated ~7-8pp bettor EV on that parlay.
  //
  // The root cause lives in getBookPairsForTeamTotals /
  // buildConsensusTeamTotals and requires an audit to ensure Over/Under
  // pairs are always same-line same-book. Until that audit lands,
  // decline all team_total legs defensively — no quotes on this market
  // class instead of potentially mispriced ones.
  //
  // Gated behind config flag so we can flip it off once the audit
  // completes without another deploy.
  // ---------------------------------------------------------------------
  if (config.pricing.declineTeamTotals !== false) {
    for (const leg of legs) {
      const lineId = leg.line_id || leg.lineId || leg;
      const lineInfo = (opts.resolvedLineInfos && opts.resolvedLineInfos.get(lineId)) || lineManager.lookupLine(lineId);
      if (lineInfo && (lineInfo.marketType === 'team_total' || lineInfo.oddsApiMarket === 'team_totals')) {
        log.info('Pricing', `Declined: team_total leg on defensive block (${lineInfo.teamName} ${lineInfo.line} — audit pending on consensus pairing)`);
        priceParlay._lastFailure = {
          reason: 'team_total declined',
          detail: `team_total legs declined pending consensus-builder audit (${lineInfo.teamName || '?'} ${lineInfo.line || '?'})`,
          blockerLeg: {
            team: lineInfo.teamName,
            market: lineInfo.marketType,
            line: lineInfo.line,
            sport: lineInfo.sport,
          },
        };
        return null;
      }
    }
  }

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

  // Phase 1 complete — captured before Phase 2 dispatches any async work.
  phase1EndMs = performance.now();

  // ------------------- PHASE 2: Per-leg fair probs + line verify ----------------
  // Try sync resolution first. All fair-prob paths EXCEPT getFairProbAsync's
  // alt-line fallback are actually synchronous under the hood (series / MMA /
  // golf / DNB / primary-cache). When every leg resolves sync, we skip the
  // Promise.all scheduling entirely — saves N microtasks per RFQ on the
  // cache-warm happy path. Only fall into async mode if any leg requires
  // an alt-line fetch or a verify-against-Pinnacle call.
  const fairProbs = new Array(legStates.length);
  const verifyResults = new Array(legStates.length).fill(null);
  let pendingAsyncFair = null;     // array of [idx, Promise] for legs needing async fair
  let pendingVerifyIdx = null;     // array of idx for legs needing async verify

  for (let i = 0; i < legStates.length; i++) {
    const s = legStates[i];
    // Sync-resolvable fair paths
    const seriesFair = getSeriesFairProb(s.lineInfo);
    if (seriesFair != null) {
      if (typeof seriesFair === 'object') {
        s.bookPriceOverride = seriesFair.bookPriceOverride;
        fairProbs[i] = seriesFair.fairProb;
      } else {
        fairProbs[i] = seriesFair;
      }
      continue;
    }
    const mmaFair = getMmaFairProb(s.lineInfo);
    if (mmaFair != null) { fairProbs[i] = mmaFair; continue; }
    const golfFair = getGolfMatchupFairProb(s.lineInfo);
    if (golfFair != null) {
      // Handle object-form return (carries bookPriceOverride when
      // fair came from BetOnline manual upload). See NBA series
      // pattern above for the same object-vs-number convention.
      if (typeof golfFair === 'object') {
        s.bookPriceOverride = golfFair.bookPriceOverride;
        fairProbs[i] = golfFair.fairProb;
      } else {
        fairProbs[i] = golfFair;
      }
      continue;
    }
    if (s.lineInfo.isDNB) {
      fairProbs[i] = oddsFeed.getDNBFairProb(
        s.lineInfo.oddsApiSport, s.lineInfo.homeTeam, s.lineInfo.awayTeam,
        s.lineInfo.oddsApiSelection, s.lineInfo.startTime
      );
      continue;
    }
    // Player strikeouts (Phase 2). Fair prob was captured at registration
    // time by line-manager via oddsFeed.lookupPlayerStrikeoutProp (sync
    // SharpAPI cache) or lookupPlayerStrikeoutPropFromTheOddsApi (async
    // TOA fallback) and stored on lineInfo.fairProb keyed to selection.
    // The generic getFairProb() below is keyed on home/away game-line
    // markets and returns null for player props, which previously caused
    // Phase-3 to decline with "no player_strikeouts quote in our odds
    // feed" even though lineInfo.fairProb was already populated. Use the
    // cached value directly.
    //
    // Staleness is guarded upstream in shouldDecline rule (c) — anything
    // older than STALE_MS already declined before reaching priceParlay.
    // Player-prop legs (Phase-2 launch types: player_points,
    // player_rebounds, player_assists, player_threes,
    // player_shots_on_goal) AND the original player_strikeouts. All
    // store fair prob on lineInfo at registration time (line-manager
    // calls oddsFeed.lookupTheOddsApiPlayerProp / lookupPlayerStrikeoutProp
    // with the leg's selection-specific over/under value). The generic
    // getFairProb() below is keyed on home/away game-line markets and
    // returns null for player props, so use the cached value directly.
    //
    // Staleness is guarded upstream in shouldDecline — anything older
    // than STALE_MS already declined before reaching priceParlay.
    if (s.lineInfo.marketType && /^player_/.test(s.lineInfo.marketType)) {
      if (s.lineInfo.fairProb != null) {
        fairProbs[i] = s.lineInfo.fairProb;
        continue;
      }
      // fairProb missing — leave fairProbs[i] undefined so Phase 3's
      // null-check fires the standard 'no fair value' decline path.
    }
    // Primary cache fast path — try sync before dispatching async.
    const syncPrimary = oddsFeed.getFairProb(
      s.lineInfo.oddsApiSport,
      s.lineInfo.homeTeam,
      s.lineInfo.awayTeam,
      s.lineInfo.oddsApiMarket,
      s.lineInfo.oddsApiSelection,
      s.lineInfo.line,
      s.lineInfo.startTime
    );
    if (syncPrimary != null) { fairProbs[i] = syncPrimary; continue; }

    // Alt-line sync fast path. When altLinesCache is fresh for this
    // event, fair + sanity checks resolve synchronously — no await, no
    // microtask hop. This restores sub-1ms pricing for cache-hit alt
    // legs. Falls through to async on cache miss/stale so network
    // refetch + non-spread/total market types (h1, team_totals via
    // Bovada) stay covered by getFairProbAsync.
    const syncAlt = oddsFeed.getAltLineFairProbSync(
      s.lineInfo.oddsApiSport,
      s.lineInfo.homeTeam,
      s.lineInfo.awayTeam,
      s.lineInfo.oddsApiMarket,
      s.lineInfo.oddsApiSelection,
      s.lineInfo.line,
      s.lineInfo.startTime
    );
    if (syncAlt != null) { fairProbs[i] = syncAlt; continue; }

    // Missing from both sync paths — fall back to the async path which can fetch
    // alt lines / do event-id resolution / consult Bovada fallback.
    if (!pendingAsyncFair) pendingAsyncFair = [];
    pendingAsyncFair.push([i, oddsFeed.getFairProbAsync(
      s.lineInfo.oddsApiSport,
      s.lineInfo.homeTeam,
      s.lineInfo.awayTeam,
      s.lineInfo.oddsApiMarket,
      s.lineInfo.oddsApiSelection,
      s.lineInfo.line,
      s.lineInfo.startTime
    )]);
  }
  for (let i = 0; i < legStates.length; i++) {
    if (legStates[i].needsVerify) {
      if (!pendingVerifyIdx) pendingVerifyIdx = [];
      pendingVerifyIdx.push(i);
    }
  }

  // ASYNC PATH: only enter when something actually needs async work.
  // Wraps the post-await wire-up + phase-3 call in a .then() so the
  // function still returns the same shape (Promise resolves to result).
  if (pendingAsyncFair || pendingVerifyIdx) {
    const proms = [];
    if (pendingAsyncFair) {
      for (const [, p] of pendingAsyncFair) proms.push(p);
    }
    if (pendingVerifyIdx) {
      for (const i of pendingVerifyIdx) {
        const s = legStates[i];
        proms.push(oddsFeed.verifyLineWithPinnacle(
          s.lineInfo.oddsApiSport, s.lineInfo.homeTeam, s.lineInfo.awayTeam,
          s.lineInfo.oddsApiMarket, s.verifyCachedLine
        ));
      }
    }
    return Promise.all(proms).then(settled => {
      let k = 0;
      if (pendingAsyncFair) {
        for (const [idx] of pendingAsyncFair) fairProbs[idx] = settled[k++];
      }
      if (pendingVerifyIdx) {
        for (const idx of pendingVerifyIdx) verifyResults[idx] = settled[k++];
      }
      return _doPhase3();
    });
  }
  // SYNC FAST PATH: all leg fair-probs already populated. Call _doPhase3
  // directly and return the result object — NOT a Promise. Caller's
  // sync-or-await branch returns it without microtask overhead.
  return _doPhase3();

  // _doPhase3 is a hoisted function declaration so it can be CALLED above
  // even though it's DECLARED below. JS hoists `function` declarations
  // (not arrow expressions) to the top of the containing scope. Captures
  // every priceParlay local via closure scope (legStates, fairProbs,
  // verifyResults, opts, timing markers, etc.). Body kept at indent
  // level 2 to minimize the diff vs the original phase-3 layout.
  function _doPhase3() {
  // Phase 2 complete — await(s) have resolved; everything below is sync.
  phase2EndMs = performance.now();

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

    // Anomalous-fair guard. Safety net in case a feed-shape edge case
    // sneaks past getBookPairsForTotals. If our de-vigged fair sits far
    // from the simple book consensus (mean of Pin/DK/FD implied probs),
    // decline rather than risk a mispriced offer. Scoped to total /
    // run_line markets — where the Apr-24 CLE @ TOR bug lived — and
    // skipped when fewer than 2 books are available (consensus isn't
    // meaningful). Not applied to DNB legs: raw 3-way americanToProb
    // isn't directly comparable to our DNB-adjusted fair.
    if (
      config.pricing.declineAnomalousTotalsEnabled &&
      bookPriceOverride == null && // overrides are intentional and trusted
      !lineInfo.isDNB &&
      (lineInfo.marketType === 'total' || lineInfo.marketType === 'run_line')
    ) {
      const books = [pinnacleOdds, fanduelOdds, draftkingsOdds]
        .map(o => (o != null ? oddsFeed.americanToImpliedProb(o) : null))
        .filter(p => p != null && p > 0 && p < 1);
      if (books.length >= 2) {
        const consensus = books.reduce((s, p) => s + p, 0) / books.length;
        const dev = Math.abs(fairProb - consensus);
        const threshold = config.pricing.declineAnomalousTotalsThreshold || 0.10;
        if (dev > threshold) {
          log.warn('Pricing', `Declined: anomalous fair ${(fairProb * 100).toFixed(1)}% vs book consensus ${(consensus * 100).toFixed(1)}% (dev ${(dev * 100).toFixed(1)}pp > ${(threshold * 100).toFixed(1)}pp) — ${legLabel}`);
          priceParlay._lastFailure = {
            reason: 'anomalous fair vs book consensus',
            detail: `${legLabel}: fair ${(fairProb * 100).toFixed(1)}% deviates ${(dev * 100).toFixed(1)}pp from book consensus ${(consensus * 100).toFixed(1)}% (${books.length} books)`,
            blockerLeg: legDescriptor,
          };
          return null;
        }
      }
    }

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

  // Detect SGP early (any pxEventId shared by 2+ legs). Used below to
  // widen per-leg vig for same-game parlays (compensates for positive
  // correlation that independent-multiplication fair ignores). SGP is
  // permitted here only for the combos that shouldDecline already
  // allow-listed — this is just the pricing-side amplifier.
  const _eventCounts = {};
  for (const pl of pricedLegs) {
    const eid = pl.lineInfo.pxEventId;
    if (!eid) continue;
    _eventCounts[eid] = (_eventCounts[eid] || 0) + 1;
  }
  const isSGPParlay = Object.values(_eventCounts).some(c => c >= 2);
  // Opposing-pitcher K-prop SGPs (kprop_kprop) are functionally
  // independent — two pitchers in the same game throw to different
  // batters and their K-counts barely correlate. Books (DK in
  // particular) treat them as independent and apply only normal vig,
  // not SGP-widened vig. Without this carve-out we offer ~80 American
  // odds points tighter than DK on the same combo (operator-flagged
  // 2026-04-27, Boyd + Vasquez K-prop SGP example: DK +395 vs ours
  // +314). Suppress sgpVigMult for kprop_kprop so per-leg vig stays at
  // the normal prop floor (3%) instead of widening to 4.2%.
  const skipSgpVig = (opts.sgpCombo === 'kprop_kprop');
  const sgpVigMult = (isSGPParlay && !skipSgpVig)
    ? Math.max(1, config.pricing.sgpVigMultiplier || 1)
    : 1;

  // ---- SGP CORRELATION ADJUSTMENT ----
  // The naive product of fair leg probs understates the true joint prob
  // for positively-correlated spread+total pairs. We apply an UPWARD
  // multiplier to fairParlayProb BEFORE vig is computed, which shortens
  // offered odds — matching how real books price SGPs.
  //
  // POLICY CHANGE 2026-04-22: previously we had a direction classifier
  // that applied ×1.15 for "positive" combos (minus+over, plus+under)
  // and ×0.90 for "negative" combos (minus+under, plus+over). Empirical
  // check against a DK SGP reference (OKC -16.5 + Under 216.5) showed
  // the direction classifier gets the sign WRONG for large-spread NBA
  // blowouts: the "fav + under" case is actually POSITIVELY correlated
  // there (starter rest in 4th quarter → pace slows → under hits), yet
  // our code was shrinking fair by 0.90, pushing offered odds longer
  // and leaking EV to bettors. DK's +228 vs our naive-compound-derived
  // +263 on that parlay was a ~35-cent underpricing in the bettor's
  // favor.
  //
  // New policy: treat ALL spread_total SGPs as positively correlated
  // (always widen fair, always shorten offered). Empirically books
  // almost always charge more on SGPs than naive product, regardless
  // of theoretical sign. Safer default — worst case we're slightly
  // tight on the small minority of truly negatively-correlated pairs
  // and lose those fills, which is strictly better than settling
  // EV-negative on the positively-correlated ones we've been mispricing.
  //
  // sgpCorrelationNegative is no longer consulted but kept in config
  // for potential future use (e.g., restoring direction logic once
  // per-sport/per-spread-magnitude rules are calibrated).
  //
  // Only applies to 2-leg spread_total SGPs — the only combo currently
  // in SGP_ALLOWED_COMBOS. ml_total was briefly added + reverted on
  // 2026-04-22 (operator concern that ml_total correlation is stronger
  // than our flat 1.08 factor captures — would risk EV leak until we
  // add per-combo tuning). If ml_total is re-enabled later, extend the
  // combo check here and consider a dedicated correlation factor.
  let sgpCorrelationFactor = 1;
  const sgpCorrelationSign = (function() {
    if (!isSGPParlay) return null;
    if ((opts.sgpCombo || null) !== 'spread_total') return null;
    if (pricedLegs.length !== 2) return null;
    // Verify structure — both a spread leg and a total leg — before
    // applying. If the combo key said spread_total but we can't find
    // one of each, bail safely rather than mis-apply.
    let spreadLeg = null, totalLeg = null;
    for (const l of pricedLegs) {
      const mt = l.lineInfo.marketType;
      if (mt === 'spread') spreadLeg = l;
      else if (mt === 'total') totalLeg = l;
    }
    if (!spreadLeg || !totalLeg) return null;
    // Always positive — see comment block above.
    return 'positive';
  })();
  if (sgpCorrelationSign === 'positive') {
    sgpCorrelationFactor = config.pricing.sgpCorrelationPositive || 1;
  }

  // K-prop + same-team ML SGP carve-out (M3 carve-out, operator-approved
  // 2026-04-27). When the parlay is exactly 2 legs (one player_strikeouts
  // + one moneyline on the same game) AND the ML team matches the
  // pitcher's team per lineup cache, apply a positive-correlation boost.
  // Empirically calibrated from DK SGP pricing: ~14.5% avg discount on
  // 3 sample combos. Default 0.15. Stacks multiplicatively with the
  // 2-way spread_total factor above (won't fire simultaneously since
  // those are different combos).
  let isKpropMlSameTeamSGP = false;
  if (isSGPParlay && pricedLegs.length === 2) {
    const propLeg = pricedLegs.find(l => l.lineInfo.marketType === 'player_strikeouts');
    const mlLeg = pricedLegs.find(l => l.lineInfo.marketType === 'moneyline');
    if (propLeg && mlLeg && propLeg.lineInfo.pxEventId === mlLeg.lineInfo.pxEventId) {
      const pi = propLeg.lineInfo;
      const pitcherSide = oddsFeed.getPitcherSide(
        pi.sport, pi.homeTeam, pi.awayTeam, pi.startTime, pi.playerName,
      );
      const mlSide = mlLeg.lineInfo.oddsApiSelection || mlLeg.lineInfo.selection;
      if (pitcherSide && mlSide && pitcherSide === mlSide) {
        isKpropMlSameTeamSGP = true;
        const boost = 1 + (config.pricing.sgpPropMlCorrBoost || 0);
        const before = fairParlayProb;
        fairParlayProb = Math.max(0.001, Math.min(0.99, fairParlayProb * boost));
        log.debug('Pricing', `SGP K-prop+ML same-team correlation boost — fair ${(before*100).toFixed(2)}% × ${boost} = ${(fairParlayProb*100).toFixed(2)}% (pitcher ${pi.playerName}, side ${pitcherSide})`);
      }
    }
  }

  if (sgpCorrelationFactor !== 1) {
    const before = fairParlayProb;
    fairParlayProb = Math.max(0.001, Math.min(0.99, fairParlayProb * sgpCorrelationFactor));
    log.debug('Pricing', `SGP correlation ${sgpCorrelationSign} — fair ${(before*100).toFixed(2)}% × ${sgpCorrelationFactor} = ${(fairParlayProb*100).toFixed(2)}%`);
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
  //
  // SGP amplifier: when the parlay is a same-game parlay, multiply the
  // effective per-leg vig by config.pricing.sgpVigMultiplier (default 2×).
  // This widens the price to compensate for positive same-game correlation
  // without triggering PX's "invalid estimated prices" rejection that
  // multiplicative correlation boosts used to hit.
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
    // Player props (Phase-2 launch types + K-prop): flat per-leg vig at
    // the VIG_PROP_FLOOR floor (default 3%). Skips the favorite-slope
    // ramp because props don't have favorites in the team-line sense —
    // a player with fair Over=0.55 isn't a "favorite" the way a -350
    // moneyline is. Still subject to the SGP multiplier and 20% cap
    // below so multi-prop parlays don't compound runaway vig.
    if (marketType && /^player_/.test(marketType)) {
      let vig = Math.max(config.pricing.vigPropFloor || 0, baseVig);
      if (sgpVigMult > 1) {
        vig = Math.min(0.20, vig * sgpVigMult);
      }
      return vig;
    }
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
    // SGP amplifier — compensate for same-game correlation. Capped at
    // 20% to avoid runaway vig if favorite ramp + SGP multiplier stack
    // on an extreme favorite; PX could still reject at absurd vig.
    if (sgpVigMult > 1) {
      vig = Math.min(0.20, vig * sgpVigMult);
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

  // Parlay fair prob over vig legs — reused by both modes and by the
  // longshot ramp below.
  const vigFair = vigLegs.reduce((p, l) => p * l.fairProb, 1);

  // ---------------------------------------------------------------------
  // LONGSHOT VIG WIDENING (parlay-level)
  //
  // The per-leg favorite ramp only fires above fairProb 0.5, so multi-leg
  // parlays built out of dog legs never trigger it — their parlay-product
  // fair prob lands low (5-20%) but each leg uses base vig. That's where
  // our chart showed the biggest gap vs Pinnacle. This ramp closes that
  // gap by adding vig linearly as parlay fair prob approaches 0.
  //
  // Applied as an ADDITIVE increment to the vig rate used below. Capped
  // at 0.20 downstream (same ceiling as the SGP multiplier) so we can't
  // stack runaway vig on extreme long shots.
  // ---------------------------------------------------------------------
  const lsThreshold = config.pricing.vigLongshotThreshold || 0;
  const lsMaxAdd = config.pricing.vigLongshotMaxAdd || 0;
  let longshotAdd = 0;
  if (lsMaxAdd > 0 && lsThreshold > 0 && vigFair > 0 && vigFair < lsThreshold) {
    longshotAdd = lsMaxAdd * (1 - vigFair / lsThreshold);
  }

  // ---------------------------------------------------------------------
  // TEMPLATE-EXPOSURE RAMP
  //
  // Checks how many identical parlays (same canonical signature) have
  // already confirmed inside the rolling window (default 24h). Returns
  // a graduated vig add, or a decline signal when concentration exceeds
  // the hard cap. Empirical: April 18 cliff was 80%+ driven by two
  // parlay signatures each copied 6-9 times — see template-exposure.js
  // for the full counterfactual derivation.
  //
  // ADDITIVE with longshotAdd (both in vig rate units); capped at 0.20
  // downstream so runaway stacking can't happen.
  // ---------------------------------------------------------------------
  const templateLegsForSig = pricedLegs.map(l => ({
    team: l.lineInfo.teamName,
    market: l.lineInfo.marketType,
    line: l.lineInfo.line,
  }));
  // Pass parlayId so getRampDecision can atomically reserve a pending
  // slot on non-decline. Closes the timing race where multiple RFQs on
  // the same signature land within seconds (faster than the confirm
  // cycle) and all see priorCount=0 because none had confirmed yet.
  // estStake uses the RFQ's max_risk if known so /template-exposure-stats
  // shows realistic in-flight totals; the real confirmedStake replaces it
  // when recordConfirmation graduates the entry.
  const templateRfqMaxRisk = (opts && Number.isFinite(+opts.maxRisk)) ? +opts.maxRisk : 0;
  const templateDecision = templateExposure.getRampDecision(templateLegsForSig, {
    parlayId: opts ? opts.parlayId : null,
    estStake: templateRfqMaxRisk,
  });
  if (templateDecision.decline) {
    log.info('Pricing', `Declined: ${templateDecision.reason} (stake so far $${templateDecision.totalStake.toFixed(2)})`);
    priceParlay._lastFailure = {
      reason: 'template exposure cap',
      detail: templateDecision.reason,
      blockerLeg: null,
    };
    return null;
  }
  const templateRampAdd = templateDecision.extraVig || 0;
  const templatePriorCount = templateDecision.count || 0;

  if (config.pricing.parlayLevelVig) {
    // Parlay-level: single vig application using max per-leg rate (over vig legs only).
    const perLegVigs = vigLegs.map(l => getEffectiveVig(l.fairProb, l.lineInfo.sport, l.lineInfo.marketType));
    const maxVig = perLegVigs.length > 0 ? Math.max(...perLegVigs) : config.pricing.defaultVig;
    const effectiveVig = Math.min(0.20, maxVig + longshotAdd + templateRampAdd);
    if (vigLegs.length > 0) {
      const fairDecimal = 1 / vigFair;
      const payout = fairDecimal - 1;
      const viggedPayout = payout * (1 - effectiveVig);
      offeredImpliedProb = (1 / (1 + viggedPayout)) * overrideProduct;
    } else {
      offeredImpliedProb = overrideProduct;
    }
    vigMode = 'parlay-level';
    vigRateUsed = effectiveVig;
  } else {
    // Per-leg: vig applied to each leg's odds then compounded (legacy).
    offeredImpliedProb = overrideProduct;
    for (const leg of vigLegs) {
      offeredImpliedProb *= applyOddsVig(leg.fairProb, leg.lineInfo.sport, leg.lineInfo.marketType);
    }
    // Longshot widening + template ramp applied as a final parlay-level
    // haircut on the post-compounding offered prob so the sensitivity
    // matches parlay-level mode's behavior for low-prob parlays.
    const totalParlayAdd = longshotAdd + templateRampAdd;
    if (totalParlayAdd > 0 && vigLegs.length > 0 && offeredImpliedProb > 0) {
      const payout = (1 / offeredImpliedProb) - 1;
      const cappedAdd = Math.min(totalParlayAdd, 0.20);
      const adjustedPayout = payout * (1 - cappedAdd);
      offeredImpliedProb = (1 / (1 + adjustedPayout));
    }
    vigMode = 'per-leg';
    // For per-leg, expose the AVERAGE per-leg rate as the "used" value (vig legs only).
    const avgVig = vigLegs.length > 0
      ? vigLegs.reduce((s, l) => s + getEffectiveVig(l.fairProb, l.lineInfo.sport, l.lineInfo.marketType), 0) / vigLegs.length
      : 0;
    vigRateUsed = Math.min(0.20, avgVig + longshotAdd + templateRampAdd);
  }

  // Fair-prob multiplier floor. Mirrors how Pinnacle / DK / FD price —
  // markup applied as a fraction of fair, producing pp-distance that
  // grows with fair prob. Our payout-vig formula above produces a flat
  // (or slightly decreasing) pp-distance curve; this knob gives the
  // slope a books-shaped lift without disrupting low-fair behavior
  // where the longshot ramp already dominates.
  //
  // Take the MAX of (current offered, fair × (1 + multiplier)). At low
  // fair the existing payout formula wins; at high fair the multiplier
  // floor kicks in. Default 0 = disabled.
  const fairMult = config.pricing.vigFairMultiplier || 0;
  if (fairMult > 0 && vigLegs.length > 0 && vigFair > 0) {
    const multiplierOffered = vigFair * (1 + fairMult) * overrideProduct;
    if (multiplierOffered > offeredImpliedProb) {
      offeredImpliedProb = Math.min(0.99, multiplierOffered);
    }
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

  // SGP flag — already computed above as isSGPParlay. Kept here for
  // backwards-compat naming in the blocks below (Pin-match floor etc.).
  const isSGP = isSGPParlay;

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

  // ---------------------------------------------------------------------
  // CATASTROPHIC-MISPRICING CIRCUIT BREAKER
  //
  // After all pricing logic has run, cross-check the OFFERED implied prob
  // against the naive book-consensus compound (no de-vig, just raw book
  // implied products). If we're selling at a dramatically lower implied
  // prob than the books collectively suggest, something is structurally
  // broken — a compounding bug, a sign-flip, a misread line, etc.
  //
  // Motivation: Delphi Apr-2026 case study where a maker offered $0.10 on
  // a 4-leg NHL parlay whose book-raw compound was ~$0.80+ and kept
  // offering it for 95 minutes across 142 fills losing $170k. The fair
  // sanity check (v1 "PRICING SAFETY NET" above) runs BEFORE vig is
  // applied and would not catch a post-vig error that blows up the
  // offered price.
  //
  // Threshold 0.60: under normal operation our offered ≈ 0.95-1.10× book
  // raw (we add slightly more vig than books do on average, but same
  // order of magnitude). Anything below 60% is a strong signal of
  // structural bug. Declines with 'suspected mispricing' reason and
  // logs loudly so the operator can investigate.
  //
  // Runs only when we have book data for every leg. Single-book legs
  // still count — correlated bias in one book is rare enough that
  // a dramatic undercut relative to even a single book is worth
  // flagging.
  // ---------------------------------------------------------------------
  {
    let bookCompound = 1;
    let allLegsHaveBooks = true;
    for (const l of pricedLegs) {
      const probs = [l.pinnacleOdds, l.fanduelOdds, l.draftkingsOdds]
        .filter(o => o != null)
        .map(o => oddsFeed.americanToImpliedProb(o))
        .filter(p => p != null && p > 0 && p < 1);
      if (probs.length === 0) { allLegsHaveBooks = false; break; }
      const legAvg = probs.reduce((s, p) => s + p, 0) / probs.length;
      bookCompound *= legAvg;
    }
    if (allLegsHaveBooks && bookCompound > 0 && offeredImpliedProb > 0) {
      const ratio = offeredImpliedProb / bookCompound;
      const SANITY_FLOOR = 0.60;
      if (ratio < SANITY_FLOOR) {
        log.error('Pricing', `CATASTROPHIC MISPRICE BLOCK: offered ${(offeredImpliedProb*100).toFixed(2)}% vs book-raw compound ${(bookCompound*100).toFixed(2)}% (ratio ${ratio.toFixed(3)}) — declining. Legs: ${pricedLegs.map(l => `${l.lineInfo.teamName}/${l.lineInfo.marketType}${l.lineInfo.line!=null?' '+l.lineInfo.line:''}`).join(' + ')}`);
        priceParlay._lastFailure = {
          reason: 'suspected mispricing',
          detail: `offered ${(offeredImpliedProb*100).toFixed(2)}% is ${((1-ratio)*100).toFixed(0)}% below book-raw compound ${(bookCompound*100).toFixed(2)}% — possible compounding/sign-flip bug`,
          blockerLeg: null,
        };
        return null;
      }
    }
  }

  // Cap at 0.99 (can't offer 100%+ implied)
  const cappedProb = Math.min(offeredImpliedProb, 0.99);

  // Convert to decimal odds
  const decimalOdds = 1 / cappedProb;

  // Determine max risk. Series-containing parlays get a tighter cap
  // because they tie up bankroll until the series settles (can be weeks).
  // Prop-containing parlays (Phase 2 player_strikeouts) get an even
  // tighter cap because we're still proving +EV at small size and
  // single-pitcher concentration risk is unknown. The smallest cap
  // applicable wins (series cap doesn't compound with prop cap).
  // max_risk on the offer is what PX uses to bound bettor wagers, so
  // setting it here enforces the per-parlay limit at the exchange level.
  const parlayHasSeries = pricedLegs.some(l =>
    typeof l.lineInfo.marketType === 'string' &&
    l.lineInfo.marketType.startsWith('series_')
  );
  // Detect any player_<type> leg, not just player_strikeouts. Earlier
  // version of this check only matched K-prop, so the new Phase-2 prop
  // types (player_points/rebounds/assists/threes_made/shots_on_goal)
  // were going out with the standard maxRiskPerParlay cap instead of
  // the much smaller prop-aware maxRiskPerParlayWithProp cap. Caused
  // the first confirmed prop fill (Hart REB + Tatum AST cross-game)
  // to land at $90.4 SP risk despite a configured $50 prop cap.
  const parlayHasProp = pricedLegs.some(l =>
    typeof l.lineInfo.marketType === 'string' &&
    /^player_/.test(l.lineInfo.marketType)
  );
  const candidateCaps = [config.pricing.maxRiskPerParlay];
  if (parlayHasSeries) candidateCaps.push(config.pricing.maxSeriesRiskPerParlay || 500);
  if (parlayHasProp) candidateCaps.push(config.pricing.maxRiskPerParlayWithProp || 50);
  const maxRisk = Math.min(...candidateCaps);

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
  // NBA: no moneyline favorites beyond -250 (fairProb > 0.7143)
  // Tennis: no moneyline favorites beyond -300 (fairProb > 0.75)
  for (const leg of pricedLegs) {
    if (leg.lineInfo.marketType !== 'moneyline') continue;
    const impliedOdds = leg.fairProb >= 0.5 ? Math.round(-100 * leg.fairProb / (1 - leg.fairProb)) : Math.round(100 * (1 - leg.fairProb) / leg.fairProb);
    if (leg.lineInfo.sport === 'basketball_nba' && leg.fairProb > (250 / 350)) {
      log.debug('Pricing', `Declined: NBA moneyline ${leg.lineInfo.teamName} is heavy favorite (${impliedOdds})`);
      priceParlay._lastFailure = {
        reason: 'NBA heavy favorite',
        detail: `${leg.lineInfo.teamName} at ${impliedOdds} exceeds -250 limit`,
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

  // Compose phase-level durations for latency instrumentation. Rounded
  // to 0.01ms — sub-microsecond noise isn't useful here.
  const phase3EndMs = performance.now();
  const _timings = {
    // entryToPhase1Ms: time from priceParlay entry to start of sync
    // validation (phaseStartMs). Captures team_total decline loop +
    // any pre-phase1 setup. Should be <0.05ms in steady state.
    entryToPhase1Ms: Math.round((phaseStartMs - entryMs) * 100) / 100,
    phase1Ms: phase1EndMs != null ? Math.round((phase1EndMs - phaseStartMs) * 100) / 100 : null,
    phase2Ms: (phase1EndMs != null && phase2EndMs != null) ? Math.round((phase2EndMs - phase1EndMs) * 100) / 100 : null,
    phase3Ms: phase2EndMs != null ? Math.round((phase3EndMs - phase2EndMs) * 100) / 100 : null,
    // totalInternalMs: total time from entry to end of phase3 marker.
    // Difference vs the websocket-measured priceParlay duration (which
    // includes the await microtask hop on entry + the result-construction/
    // return path + post-return microtask hop) tells us the V8 overhead.
    totalInternalMs: Math.round((phase3EndMs - entryMs) * 100) / 100,
  };

  // ---------------------------------------------------------------------
  // V2 SHADOW MODE
  //
  // When pricingV2Enabled, run the v2 pipeline alongside v1 and log a
  // side-by-side comparison record. Never affects the live offer (v1
  // number still goes to PX). Gate in template-ramp + longshot-add is
  // passed through so v2 sees the same exposure context as v1.
  //
  // When pricingV2Live is also true, OVERRIDE the v1 americanOdds with
  // v2's computed value. Ships code-dark until Mike flips the flag.
  //
  // Hot-path cost: the v2 pipeline (calibration + correlation +
  // ev-vig) adds ~500-1000µs per quote. That cost is unavoidable when
  // pricingV2Live is true (we need v2's price before returning), but
  // when it's false (pure shadow) we can defer with setImmediate so
  // the offer returns to the WS handler first and v2 computes on the
  // next event loop tick. Observed regression 2026-04-24: p50 went
  // from <1.0ms to ~1.6ms once v2 shadow went wide. This defer path
  // recovers that — the shadow log still populates, just milliseconds
  // later, and nothing downstream reads meta._v2Shadow today.
  // ---------------------------------------------------------------------
  const _v2ShadowArgs = config.pricing.pricingV2Enabled ? {
    parlayId: opts.parlayId || '(unknown)',
    pricedLegs,
    v1OfferedAmericanOdds: americanOdds,
    v1FairParlayProb: fairParlayProb,
    opts: {
      targetEdge: config.pricing.pricingV2TargetEdge,
      kSigma: config.pricing.pricingV2KSigma,
      templateRampAdd: templateRampAdd || 0,
    },
  } : null;
  let _v2Shadow = null;
  if (_v2ShadowArgs) {
    if (config.pricing.pricingV2Live) {
      // Sync path: v2's offered odds may override v1's below. Must block.
      try {
        _v2Shadow = v2.shadowCompare(_v2ShadowArgs);
      } catch (err) {
        log.warn('V2Pricing', `shadow failed: ${err.message}`);
      }
    } else {
      // Async path: pure-shadow, no override possible. Defer so the offer
      // ships first. setImmediate runs on the next event loop tick —
      // after this function returns and the offer is handed to the WS
      // handler for HTTP submission to PX.
      setImmediate(() => {
        try {
          v2.shadowCompare(_v2ShadowArgs);
        } catch (err) {
          log.warn('V2Pricing', `deferred shadow failed: ${err.message}`);
        }
      });
    }
  }

  // A/B arm assignment. Deterministic by parlayId-hash so the same
  // parlay always lands in the same arm (important for retry semantics
  // and settlement attribution). Recorded in meta.abArm REGARDLESS of
  // whether v2 actually overrides — keeps shadow records attributable
  // to arms even before we flip pricingV2Live on.
  //   - pricingV2LivePercent 0  → every parlay is 'v1'
  //   - pricingV2LivePercent 50 → 50/50 split
  //   - pricingV2LivePercent 100 → every parlay is 'v2'
  // Override only actually fires when pricingV2Live is ALSO true —
  // that's the master kill-switch.
  //
  // Hash uses md5 over the full parlayId rather than parseInt on the
  // first 8 hex chars. Why: parlayIds are UUIDv7, where the first 12
  // hex chars encode the timestamp in ms. First-8-hex only advances
  // ~1 per 65s, so at 5% target the hash spends ~104 min in the
  // >=5 band before cycling back — yielding long "freeze" windows
  // where no parlay gets assigned v2. md5 mixes all bits uniformly
  // so assignment is per-parlay-random, not per-65s-bucket.
  const _livePct = config.pricing.pricingV2LivePercent || 0;
  let abArm = 'v1';
  if (opts.parlayId && _livePct > 0) {
    if (_livePct >= 100) {
      abArm = 'v2';
    } else {
      const h = crypto.createHash('md5').update(String(opts.parlayId)).digest();
      const n = h.readUInt32BE(0);
      if ((n % 100) < _livePct) abArm = 'v2';
    }
  }

  // If v2 is LIVE and this parlay is in the v2 arm, replace the offer's
  // odds with v2's result. Records v2Used in meta so A/B metrics can
  // distinguish "assigned to v2 arm" from "actually got v2 price"
  // (they differ when pricingV2Live is false but assignment still runs).
  let finalAmericanOdds = americanOdds;
  let finalOfferedImpliedProb = offeredImpliedProb;
  let v2Used = false;
  if (config.pricing.pricingV2Live && abArm === 'v2' && _v2Shadow && _v2Shadow.v2.offeredAmericanOdds != null) {
    finalAmericanOdds = _v2Shadow.v2.offeredAmericanOdds;
    finalOfferedImpliedProb = _v2Shadow.v2.offeredImpliedProb;
    v2Used = true;
    log.info('V2Pricing', `live[${abArm}]: v1=${americanOdds} → v2=${finalAmericanOdds} (Δ ${finalAmericanOdds - americanOdds})`);
  }

  return {
    offer: {
      valid_until: validUntil,
      odds: finalAmericanOdds, // American odds for PX
      max_risk: maxRisk,
      estimated_price: estimatedPrice,
    },
    meta: {
      _timings,
      abArm,
      v2Used,
      _v2Shadow: _v2Shadow ? {
        v1American: _v2Shadow.v1.offeredAmericanOdds,
        v2American: _v2Shadow.v2.offeredAmericanOdds,
        americanDelta: _v2Shadow.delta.americanOddsDelta,
        v2FairParlay: _v2Shadow.v2.parlayFairProb,
        v2CorrelationLift: _v2Shadow.v2.correlationLift,
        v2Uncertainty: _v2Shadow.v2.parlayUncertainty,
        v2EffectiveEdge: _v2Shadow.v2.effectiveEdge,
        calibrationApplied: _v2Shadow.v2.calibrationApplied,
        liveMode: !!config.pricing.pricingV2Live,
      } : null,
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
      // Additional vig added by longshot ramp (parlay-level, applied when
      // parlay fair prob < vigLongshotThreshold). 0 when not triggered.
      // Exposed so dashboard can flag which quotes used the ramp and we
      // can A/B measure acceptance-rate impact.
      longshotAdd: Math.round((longshotAdd || 0) * 10000) / 10000,
      // Template-exposure ramp contribution + prior-confirmation count
      // for the quote's canonical parlay signature. 0 / 0 when ramp
      // didn't fire. Lets the dashboard flag quotes penalized for
      // template stacking and retroactively measure whether we actually
      // lost volume on the 2nd/3rd/4th-of-identical bets.
      templateRampAdd: Math.round((templateRampAdd || 0) * 10000) / 10000,
      templatePriorCount: templatePriorCount || 0,
      fairParlayProb: Math.round(fairParlayProb * 100000) / 100000,
      pricingMethod,
      isSGP,
      // Same-game parlay tracking: which combo (spread_total, ml_total, etc.)
      // and how much we amplified vig. `sgpCombo` is set upstream by
      // shouldDecline when the parlay is a 2-leg same-event combo we
      // allow-listed; it flows through opts so priceParlay can record it.
      sgpCombo: opts.sgpCombo || null,
      sgpVigMultiplier: isSGP ? sgpVigMult : 1,
      // Correlation adjustment applied to the joint fair prob for this SGP
      // before vig. Null if not an SGP or not a recognized combo. Stored
      // so /sgp-stats + order audits can split acceptance + ROI by
      // correlation sign as we tune the factors.
      sgpCorrelationSign: sgpCorrelationSign,
      sgpCorrelationFactor: sgpCorrelationFactor,
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
      fanduelParlay: (() => {
        const fdLegs = pricedLegs.filter(l => l.fanduelOdds != null);
        if (fdLegs.length !== pricedLegs.length) return null;
        let fdProb = 1;
        for (const l of fdLegs) {
          const legImpl = l.lineInfo.isDNB && l.fanduelDNBProb != null
            ? l.fanduelDNBProb
            : oddsFeed.americanToImpliedProb(l.fanduelOdds);
          fdProb *= legImpl;
        }
        if (fdProb <= 0 || fdProb >= 1) return null;
        return decimalToAmerican(1 / fdProb);
      })(),
      maxRisk,
    },
  };
  } // end of _doPhase3
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

  // Pre-pass for STRUCTURAL prop-correlation rules that should outrank
  // per-leg quality reasons (no_fair_value, low_confidence, stale). A
  // parlay containing two K-prop legs on the same pitcher is correlated
  // by construction — declining for "BetRivers-alone" instead of
  // "two legs on the same pitcher" misnames the actual problem and would
  // also approve the parlay if BetRivers happened to be FD/DK.
  //
  // Only handles the same-pitcher case here; same-game correlation rule
  // (d) requires the full carve-out logic and stays in its post-loop
  // position so we don't double-implement the K+ML SGP allowance.
  {
    const seen = {};
    for (const leg of legs) {
      const lineId = leg.line_id || leg.lineId || leg;
      const lineInfo = lineManager.lookupLine(lineId);
      if (!lineInfo) continue; // 'unknown legs' will fire in the main loop
      if (lineInfo.marketType !== 'player_strikeouts') continue;
      const player = (lineInfo.playerName || '').toLowerCase().trim();
      if (!player) continue;
      if (seen[player]) {
        return {
          declined: true,
          reason: 'prop_correlation_same_pitcher',
          detail: `Two legs on pitcher "${lineInfo.playerName}" in same parlay (lines ${seen[player]} + ${lineInfo.line})`,
        };
      }
      seen[player] = lineInfo.line;
    }
  }

  // Phase-2 prop launch — same-game correlation block. Rule: a prop leg
  // can never share a game with any other leg in the same parlay (team
  // line OR another prop, same player or different player). The
  // mechanical correlation between a player's stat output and his
  // team's game-level outcome (or another teammate's stat output) is
  // strong and not modeled by the SGP correlation factors. Cleaner to
  // block outright than to risk underpricing.
  //
  // Applies to all player_<type> marketTypes including player_strikeouts.
  // The same-pitcher rule above is the narrower variant; this catches
  // the cross-market-type case (e.g. McCollum points + Pelicans ML, or
  // McCollum points + Murray rebounds in DEN@NOP).
  //
  // Only fires when the parlay actually contains a prop leg, so
  // game-line-only parlays are unaffected.
  {
    const isPropLeg = (li) => li && /^player_/.test(li.marketType || '');
    const legsByEvent = {};
    let anyPropLeg = false;
    for (const leg of legs) {
      const lineId = leg.line_id || leg.lineId || leg;
      const lineInfo = lineManager.lookupLine(lineId);
      if (!lineInfo) continue;
      const eid = lineInfo.pxEventId;
      if (!eid) continue;
      if (!legsByEvent[eid]) legsByEvent[eid] = { props: [], others: [] };
      if (isPropLeg(lineInfo)) {
        legsByEvent[eid].props.push(lineInfo);
        anyPropLeg = true;
      } else {
        legsByEvent[eid].others.push(lineInfo);
      }
    }
    if (anyPropLeg) {
      for (const [eid, group] of Object.entries(legsByEvent)) {
        // Block: prop + any other leg (team market or another prop) on
        // same pxEventId.
        if (group.props.length > 0 && (group.props.length + group.others.length) > 1) {
          const propLabels = group.props.map(li =>
            `${li.playerName || li.teamName || '?'} ${li.propType || li.marketType} ${li.selection || ''} ${li.line ?? ''}`.trim());
          const otherLabels = group.others.map(li =>
            `${li.teamName || '?'} ${li.marketType || ''}`.trim());
          const detail = `pxEventId=${eid}: ${group.props.length} prop leg(s) [${propLabels.join('; ')}]`
            + (group.others.length ? ` + ${group.others.length} same-game leg(s) [${otherLabels.join('; ')}]` : '');
          return {
            declined: true,
            reason: 'prop_correlation_same_game',
            detail,
          };
        }
      }
    }
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

    // Prop-specific decline rules. Applied per-leg as we iterate.
    // Cross-leg correlation rules checked separately above.
    //
    // Applies to ALL player_<type> markets — player_strikeouts (K-prop),
    // player_points, player_rebounds, player_assists, player_threes,
    // player_shots_on_goal, etc. Phase-2 launch types share the same
    // confidence-gate semantics as K-prop.
    if (lineInfo.marketType && /^player_/.test(lineInfo.marketType)) {
      const isKProp = lineInfo.marketType === 'player_strikeouts';
      const propMinBooks = config.pricing.propMinBooksWithBothSides || 3;
      // Min-books floor: K-prop keeps its 2-book + trusted-alone exception
      // for backward compat (built around the SharpAPI single-book pattern).
      // New Phase-2 props use the stricter propMinBooksWithBothSides (default
      // 3) with no trusted-alone carve-out — TOA's broader coverage means
      // we should always have ≥3 books for legitimate prop quotes.
      const playerLabel = lineInfo.playerName || lineInfo.teamName || '?';
      const propLabel = isKProp ? 'K' : (lineInfo.propType || lineInfo.marketType.replace(/^player_/, ''));

      // (a) Missing fair value — prop matcher returned no usable consensus.
      if (lineInfo.fairProb == null) {
        return {
          declined: true,
          reason: 'prop_no_fair_value',
          detail: `${playerLabel} ${propLabel} ${lineInfo.line} (${lineInfo.selection}) — prop matcher returned no fair_prob`,
        };
      }
      // (b) Min-books gate — branches by prop family.
      const both = lineInfo.booksWithBothSides || 0;
      const propBooks = lineInfo.propBooks || [];
      if (isKProp) {
        const trustedSet = config.pricing.propTrustedSingleBooks || [];
        const trustedAlone = both === 1 && propBooks.some(b => trustedSet.includes(String(b).toLowerCase()));
        if (both < 2 && !trustedAlone) {
          return {
            declined: true,
            reason: 'prop_low_confidence',
            detail: `${playerLabel} K ${lineInfo.line}: books_with_both_sides=${both}, books=[${propBooks.join(',')}] — need ≥2 books OR one of [${trustedSet.join(',')}]`,
          };
        }
      } else {
        // Phase-2 launch props: stricter ≥N-book floor.
        if (both < propMinBooks) {
          return {
            declined: true,
            reason: 'prop_low_confidence',
            detail: `${playerLabel} ${propLabel} ${lineInfo.line}: books_with_both_sides=${both}, books=[${propBooks.join(',')}] — need ≥${propMinBooks}`,
          };
        }
      }
      // (c) Stale prop data (>15 min old).
      const STALE_MS = 15 * 60 * 1000;
      if (lineInfo.propFetchedAt && (Date.now() - lineInfo.propFetchedAt) > STALE_MS) {
        const ageMin = Math.round((Date.now() - lineInfo.propFetchedAt) / 60000);
        return {
          declined: true,
          reason: 'prop_stale',
          detail: `${playerLabel} ${propLabel} ${lineInfo.line}: prop data ${ageMin} min old (>15 min)`,
        };
      }
    }

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

    // Alt-spread block (NBA/MLB/NHL by default — see isBlockedAltSpread).
    const altReason = isBlockedAltSpread(lineInfo);
    if (altReason) {
      return {
        declined: true,
        reason: 'alt-spread blocked',
        detail: `${lineInfo.teamName || '?'} ${lineInfo.marketType} ${lineInfo.line}: ${altReason}`,
      };
    }
    // Alt-total block (NBA only — see isBlockedAltTotal).
    const altTotalReason = isBlockedAltTotal(lineInfo);
    if (altTotalReason) {
      return {
        declined: true,
        reason: 'alt-total blocked',
        detail: `${lineInfo.teamName || '?'} ${lineInfo.marketType} ${lineInfo.line}: ${altTotalReason}`,
      };
    }

    resolvedLegs.push({ lineId, lineInfo });
  }

  // M3 cross-leg prop correlation rules. Run after the per-leg loop
  // because they need the resolvedLegs array to compare across legs.
  // ALL-OR-NOTHING decline: if any rule fires, decline the entire parlay.
  const propLegs = resolvedLegs.filter(l => l.lineInfo.marketType === 'player_strikeouts');
  if (propLegs.length > 0) {
    // (d) Same-game correlation: K-prop leg + any other leg sharing
    // pxEventId is heavily correlated. The pitcher's K count and his
    // team's run total / win probability move together. Decline rather
    // than try to model the correlation.
    //
    // CARVE-OUT (operator-approved 2026-04-27): exactly-2-leg parlays
    // of (K-prop + same-team ML) are allowed. The pitcher's team
    // moneyline IS positively correlated with their K total, but books
    // (DK, FD) have stable empirical SGP pricing for this combo (DK
    // ~14.5% discount, FD ~24.3% across sample). We mirror DK-style
    // pricing via sgpPropMlCorrBoost (default 0.15) applied to
    // fairParlayProb in priceParlay. Other K-prop SGP combos (K + total,
    // K + run-line, K + opposite-team-ML) remain blocked.
    for (const propLeg of propLegs) {
      const eid = propLeg.lineInfo.pxEventId;
      if (!eid) continue;
      const sameGameOthers = resolvedLegs.filter(l =>
        l !== propLeg && l.lineInfo.pxEventId === eid
      );
      if (sameGameOthers.length === 0) continue;

      // CARVE-OUT 1: K-prop + same-team ML (2-leg parlay).
      // Positively correlated — pricing applies sgpPropMlCorrBoost in
      // priceParlay. Operator-approved 2026-04-27.
      const isMlCarveOutCandidate = resolvedLegs.length === 2
        && sameGameOthers.length === 1
        && sameGameOthers[0].lineInfo.marketType === 'moneyline';
      if (isMlCarveOutCandidate) {
        const pi = propLeg.lineInfo;
        const pitcherSide = oddsFeed.getPitcherSide(
          pi.sport, pi.homeTeam, pi.awayTeam, pi.startTime, pi.playerName
        );
        const mlSide = sameGameOthers[0].lineInfo.oddsApiSelection || sameGameOthers[0].lineInfo.selection;
        if (pitcherSide && mlSide && pitcherSide === mlSide) {
          // Allow — fall through to pricing. Marker on resolvedLegs
          // so priceParlay can apply the correlation boost.
          for (const r of resolvedLegs) r.sgpKpropMlSameTeam = true;
          continue; // process next propLeg
        }
      }

      // CARVE-OUT 2: All same-game-other legs are K-props on different
      // pitchers (opposing starters). Two pitchers in the same MLB game
      // pitch to different batters; their K-counts are largely
      // independent. Books (DK, FD) routinely offer this combo with
      // little to no correlation discount. Same-pitcher × 2 already
      // blocked upstream by the same-pitcher pre-pass, so by the time
      // we reach here any same-game K-prop pairing is opposing-pitcher
      // by construction. Operator-approved 2026-04-27.
      if (sameGameOthers.every(l => l.lineInfo.marketType === 'player_strikeouts')) {
        // Allow — fall through to pricing as effectively independent.
        // Marker so downstream code can identify the combo if needed.
        for (const r of resolvedLegs) r.sgpKpropOpposing = true;
        continue;
      }

      // No carve-out applied — decline. Show first same-game-other leg
      // in the detail for diagnostic clarity.
      const sample = sameGameOthers[0];
      return {
        declined: true,
        reason: 'prop_correlation_same_game',
        detail: `${propLeg.lineInfo.playerName || '?'} K ${propLeg.lineInfo.line} + ${sample.lineInfo.teamName || sample.lineInfo.playerName || '?'} ${sample.lineInfo.marketType || '?'} on same event ${eid}`,
      };
    }
    // (e) Multi-line same-pitcher: two K-prop legs on the same pitcher
    // are perfectly anti-correlated (if Over/Under) or perfectly
    // correlated (if same side, different lines). Either way, decline.
    const seen = {};
    for (const propLeg of propLegs) {
      const player = (propLeg.lineInfo.playerName || '').toLowerCase().trim();
      if (!player) continue;
      if (seen[player]) {
        return {
          declined: true,
          reason: 'prop_correlation_same_pitcher',
          detail: `Two legs on pitcher "${propLeg.lineInfo.playerName}" in same parlay`,
        };
      }
      seen[player] = true;
    }
  }

  // Same-game parlay gating. Previously blanket-declined because
  // multiplicative correlation boosts triggered PX "invalid estimated
  // prices" rejection. Now: allow configured market-pair combos only;
  // pricing path applies a wider vig multiplier (config.pricing.
  // sgpVigMultiplier) instead of a boost to compensate for positive
  // same-game correlation in a way PX accepts.
  const byEvent = {};
  for (const l of resolvedLegs) {
    const eid = l.lineInfo.pxEventId;
    if (!eid) continue;
    if (!byEvent[eid]) byEvent[eid] = [];
    byEvent[eid].push({ market: l.lineInfo.marketType, team: l.lineInfo.teamName, home: l.lineInfo.homeTeam, away: l.lineInfo.awayTeam });
  }
  // Classify a 2-leg SGP group into a stable combo key. Returns null if
  // the group is 3+ legs or the market pair isn't a recognized combo
  // (e.g. spread+spread — blocked by duplicate rules anyway).
  const classifySgpCombo = (entries) => {
    if (entries.length !== 2) return null;
    const markets = entries.map(e => e.market).sort();
    const key = markets.join('_');
    // Only named combos users can opt in/out of via SGP_ALLOWED_COMBOS:
    if (key === 'moneyline_total') return 'ml_total';
    if (key === 'spread_total') return 'spread_total';
    if (key === 'moneyline_spread') return 'ml_spread'; // also blocked below as correlated
    if (key === 'moneyline_player_strikeouts') return 'kprop_ml'; // 2026-04-27: K-prop + ML same-team SGP
    if (key === 'player_strikeouts_player_strikeouts') return 'kprop_kprop'; // 2026-04-27: 2 K-props on same game (opposing pitchers — same-pitcher dup blocked upstream)
    return null; // any other pair: reject by default
  };
  // Auto-include 'kprop_ml' and 'kprop_kprop' in the allowed-combos set
  // so the prop carve-outs (handled in the prop_correlation_same_game
  // block above) don't get re-blocked here. The carve-outs already
  // enforce the same-team check (kprop_ml) and the same-pitcher pre-pass
  // ensures kprop_kprop is opposing pitchers; this gate just needs to
  // recognize the combos.
  const allowedCombos = new Set([...(config.pricing.sgpAllowedCombos || []), 'kprop_ml', 'kprop_kprop']);
  // `sgpCombo` is captured on the whole parlay for downstream pricing +
  // order-tracking. Currently we only support single-event SGP legs
  // (length==2 on one pxEventId); multi-event SGPs not supported yet.
  let sgpEventId = null;
  let sgpCombo = null;
  for (const [eid, entries] of Object.entries(byEvent)) {
    if (entries.length <= 1) continue;
    const gameLabel0 = entries[0].away && entries[0].home ? `${entries[0].away} @ ${entries[0].home}` : `event ${eid}`;
    const combo = classifySgpCombo(entries);
    if (!combo || !allowedCombos.has(combo)) {
      log.info('Pricing', `Declined SGP: ${entries.length} legs on ${gameLabel0} (combo=${combo || 'unclassified'}, not in allowed list)`);
      return {
        declined: true,
        reason: 'SGP not allowed',
        detail: `${entries.length} legs on same game: ${gameLabel0} (combo=${combo || 'unclassified'} not in SGP_ALLOWED_COMBOS=${[...allowedCombos].join(',') || 'empty'})`,
      };
    }
    // More than one SGP pair on the same parlay (e.g. two distinct
    // same-game pairs on different events) — don't try to price.
    if (sgpEventId != null) {
      log.info('Pricing', `Declined SGP: multiple same-game pairs across events`);
      return { declined: true, reason: 'SGP not allowed', detail: `multiple same-game pairs not supported` };
    }
    sgpEventId = eid;
    sgpCombo = combo;
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
  //
  // Fast-path: skip this pass entirely when no leg is a series market.
  // 97%+ of RFQs don't touch series markets at all, so the byPair build
  // and loop were pure waste on the hot path. Guard saves ~50-150μs per
  // RFQ depending on leg count.
  const hasAnySeriesLeg = resolvedLegs.some(r => {
    const mt = r.lineInfo.marketType;
    return typeof mt === 'string' && mt.startsWith('series_');
  });
  if (hasAnySeriesLeg) {
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
  // Pass legs with fairProb info for weighted calculation. CRITICAL:
  // include the full lineInfo so checkExposureLimits and checkGameExposure
  // can build the right key (teamKey|pxEventId|date for team check,
  // pxEventId|date for game check). Previously only {team, fairProb} was
  // passed — the key builders fell back to '|noevent' which never matched
  // the real exposure[] entries (which use eventId+date). The team-cap
  // check was effectively a no-op at quote time; only confirm-time caught
  // overflows. Now the quote-time check is a real gate, eliminating
  // redundant quote→confirm→reject cycles and closing the PX-ignores-
  // reject leak path entirely (we never quote in the first place).
  const legsWithProb = resolvedLegs.map(l => {
    const fp = oddsFeed.getFairProb(
      l.lineInfo.oddsApiSport, l.lineInfo.homeTeam, l.lineInfo.awayTeam,
      l.lineInfo.oddsApiMarket, l.lineInfo.oddsApiSelection,
      l.lineInfo.line != null ? Math.abs(l.lineInfo.line) : null, l.lineInfo.startTime
    );
    return {
      team: l.lineInfo.teamName,
      fairProb: fp || 0.5,
      lineInfo: l.lineInfo,           // exposure-key builders read pxEventId, startTime, homeTeam, awayTeam from here
      pxEventId: l.lineInfo.pxEventId, // explicit fallback fields if a checker reads them at top level
      startTime: l.lineInfo.startTime,
      homeTeam: l.lineInfo.homeTeam,
      awayTeam: l.lineInfo.awayTeam,
    };
  });
  const exposureCheck = orderTracker.checkExposureLimits(
    legsWithProb, estPayout, config.pricing.maxExposurePerTeam
  );
  if (!exposureCheck.allowed) {
    log.info('Pricing', `Exposure limit: ${exposureCheck.reason}`);
    return { declined: true, reason: 'team exposure limit', detail: exposureCheck.reason, violations: exposureCheck.violations, estPayout };
  }

  // Per-event aggregate cap (quote-time gate). Catches the same risk
  // as the team check but at the EVENT level — a parlay with Lakers
  // spread on one leg and Hawks total on another (same LAL@ATL game)
  // ends up adding to BOTH team buckets but ALSO to the single
  // pxEventId bucket. Without this, repeated parlays touching opposite
  // sides of the same game can build event-level concentration that
  // no team-level cap sees.
  const gameCheck = orderTracker.checkGameExposure(
    legsWithProb, estPayout, config.pricing.maxExposurePerGame
  );
  if (!gameCheck.allowed) {
    log.info('Pricing', `Game exposure limit: ${gameCheck.reason}`);
    return {
      declined: true,
      reason: 'game exposure limit',
      detail: gameCheck.reason,
      violations: [{ team: 'game-event', wouldBe: gameCheck.wouldBe || 0, limit: gameCheck.limit || 0 }],
      estPayout,
    };
  }

  // Series-specific gross exposure cap. Uses the tighter per-parlay
  // cap (config.pricing.maxSeriesRiskPerParlay) as the worst-case add
  // since that's what max_risk on the offer will be set to for series
  // parlays — PX cannot confirm an amount above it.
  const hasSeriesLeg = resolvedLegs.some(l =>
    typeof l.lineInfo.marketType === 'string' &&
    l.lineInfo.marketType.startsWith('series_')
  );
  if (hasSeriesLeg) {
    const seriesParlayCap = config.pricing.maxSeriesRiskPerParlay || 500;
    const seriesCheck = orderTracker.checkSeriesExposure(
      resolvedLegs, seriesParlayCap, config.pricing.maxSeriesGrossExposure
    );
    if (!seriesCheck.allowed) {
      log.info('Pricing', `Series exposure limit: ${seriesCheck.reason}`);
      return {
        declined: true,
        reason: 'series exposure limit',
        detail: seriesCheck.reason,
        violations: [{ team: 'series-event', wouldBe: seriesCheck.wouldBe || 0, limit: seriesCheck.limit || 0 }],
        estPayout: seriesParlayCap,
      };
    }
  }

  // M4 per-player exposure cap. Run AFTER the series check so we have
  // the right estPayout — for prop-containing parlays, the per-parlay
  // cap is config.pricing.maxRiskPerParlayWithProp (smaller than the
  // generic estPayout). Use that as the additional-risk worst case.
  //
  // Two parallel caps:
  //   - K-prop (player_strikeouts): pitcherExposure (legacy, per-event)
  //   - Phase-2 (player_points/rebounds/assists/threes/shots_on_goal):
  //     playerExposure (per-(sport, player), cross-prop)
  // Both fire when a parlay contains the matching leg type.
  const hasKPropLeg = resolvedLegs.some(l => l.lineInfo.marketType === 'player_strikeouts');
  const hasNewPropLeg = resolvedLegs.some(l =>
    l.lineInfo.marketType
    && /^player_/.test(l.lineInfo.marketType)
    && l.lineInfo.marketType !== 'player_strikeouts'
  );
  const hasAnyPropLeg = hasKPropLeg || hasNewPropLeg;
  if (hasAnyPropLeg) {
    const propParlayCap = config.pricing.maxRiskPerParlayWithProp || 50;

    if (hasKPropLeg) {
      const pitcherCheck = orderTracker.checkPitcherExposure(
        resolvedLegs, propParlayCap, config.pricing.maxExposurePerPitcher,
      );
      if (pitcherCheck && pitcherCheck.exceeded) {
        const pendingTxt = pitcherCheck.pending ? ` + pending $${pitcherCheck.pending}` : '';
        log.info('Pricing', `Pitcher exposure cap: ${pitcherCheck.pitcher} would be $${pitcherCheck.wouldBe} (max $${pitcherCheck.max})`);
        return {
          declined: true,
          reason: 'pitcher_exposure_cap',
          detail: `${pitcherCheck.pitcher}: current $${pitcherCheck.current}${pendingTxt} + this parlay $${propParlayCap} = $${pitcherCheck.wouldBe} > cap $${pitcherCheck.max}`,
          violations: [{ team: pitcherCheck.pitcher, wouldBe: pitcherCheck.wouldBe, limit: pitcherCheck.max }],
          estPayout: propParlayCap,
        };
      }
    }

    if (hasNewPropLeg) {
      const playerCheck = orderTracker.checkPlayerExposure(
        resolvedLegs,
        propParlayCap,
        config.pricing.maxExposurePerPlayerBySport || {},
        config.pricing.maxExposurePerPlayerDefault,
      );
      if (playerCheck && playerCheck.exceeded) {
        const pendingTxt = playerCheck.pending ? ` + pending $${playerCheck.pending}` : '';
        log.info('Pricing', `Player exposure cap: ${playerCheck.player} (${playerCheck.sport}) would be $${playerCheck.wouldBe} (max $${playerCheck.max})`);
        return {
          declined: true,
          reason: 'player_exposure_cap',
          detail: `${playerCheck.player} (${playerCheck.sport}): current $${playerCheck.current}${pendingTxt} + this parlay $${propParlayCap} = $${playerCheck.wouldBe} > cap $${playerCheck.max}`,
          violations: [{ team: playerCheck.player, wouldBe: playerCheck.wouldBe, limit: playerCheck.max }],
          estPayout: propParlayCap,
        };
      }
    }
  }

  // On success, surface the resolved lineInfos keyed by lineId so the caller
  // can pass them to priceParlay and skip redundant lookupLine() calls.
  // Also surface the classified SGP combo (if any) so priceParlay can stamp
  // it on meta for order-tracking and per-combo analytics.
  const resolvedLineInfos = new Map();
  for (const r of resolvedLegs) resolvedLineInfos.set(r.lineId, r.lineInfo);
  return { declined: false, resolvedLineInfos, sgpCombo, sgpEventId };
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
  computeSingleLegQuote,
  decimalToAmerican,
  // Exposed so /lines/detail can resolve golf fair probs through the
  // same DataGolf → DK → BetOnline cascade that the live pricing
  // path uses. Without this, the Lines tab shows no fair for any
  // golf matchup line even when pricing would succeed at RFQ time.
  getGolfMatchupFairProb,
};
