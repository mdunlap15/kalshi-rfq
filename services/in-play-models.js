/**
 * In-play (live) win-probability models per sport.
 *
 * Given a parlay leg's static metadata + the current live game state from
 * ESPN, return a live fair-prob estimate from the BETTOR's perspective
 * (i.e. P(bettor wins the leg | current state)) — matches the convention
 * of the static `fairProb` field on each leg. Dashboards convert to
 * SP-perspective as `1 - liveFairProb` for display.
 *
 * These are NOT trading-grade models — they're calibration-light
 * approximations meant to give the operator a live "is this leg alive?"
 * read on open positions, not to inform live pricing. For trading-grade
 * live odds we'd subscribe to a market-data feed (TOA / SharpAPI live
 * tier); these models bridge the gap when that's unavailable.
 *
 * All models accept:
 *   leg       — { market, selection, line, sport, homeTeam, awayTeam, fairProb (pregame) }
 *   gameState — { homeScore, awayScore, period, displayClock, shortDetail,
 *                 homeLinescores, awayLinescores, state, completed,
 *                 homeRunsThru5, awayRunsThru5, f5Completed }
 *
 * Return:
 *   { liveFairProb, model, confidence: 'high'|'medium'|'low'|'pregame'|'final',
 *     reason: string|null }
 *
 *   liveFairProb is P(SP wins this leg | current game state).
 *   When confidence='final', the leg's outcome is determined (settled
 *   prob is 0 or 1).
 */

// --------------------------------------------------------------------
// Helpers — math, clock parsing, sign conventions
// --------------------------------------------------------------------

/**
 * Standard normal CDF Φ(z). Abramowitz & Stegun 7.1.26 — the formula
 * computes Q(|z|) = P(Z > |z|), the UPPER tail. So Φ(z) = 1 - Q(z) for
 * z >= 0 and Φ(z) = Q(|z|) for z < 0. Pure-JS, ~5 digit precision, fast.
 */
function normalCdf(z) {
  if (!Number.isFinite(z)) return 0.5;
  const sign = z < 0 ? -1 : 1;
  const x = Math.abs(z);
  const t = 1 / (1 + 0.2316419 * x);
  const d = 0.3989423 * Math.exp(-x * x / 2);
  const q = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  // For z >= 0: Φ(z) = 1 - Q(z). For z < 0: Φ(z) = Q(|z|).
  return sign > 0 ? (1 - q) : q;
}

/**
 * Parse a "M:SS" or "MM:SS" clock string to seconds. Returns null on
 * unparseable input (which can happen for "End 3rd", "Halftime", etc.).
 */
function parseClockToSeconds(clockStr) {
  if (typeof clockStr !== 'string') return null;
  const m = clockStr.match(/^(\d{1,2}):(\d{2})(?:\.\d+)?$/);
  if (!m) return null;
  const mins = parseInt(m[1], 10);
  const secs = parseInt(m[2], 10);
  if (!Number.isFinite(mins) || !Number.isFinite(secs)) return null;
  return mins * 60 + secs;
}

/**
 * For a sport with N timed periods of P minutes each, compute total
 * minutes remaining given current period (1-indexed) and clock-remaining-
 * in-period (seconds). period > N means OT.
 */
function minutesRemainingTimedGame(period, clockSec, periodLengthMin, totalPeriods) {
  if (!Number.isFinite(period) || !Number.isFinite(clockSec)) return null;
  if (period < 1) return totalPeriods * periodLengthMin;
  if (period > totalPeriods) {
    // OT — assume current OT period is in progress; remaining = clock only.
    // Not strictly correct (OT can extend further) but a usable approximation.
    return clockSec / 60;
  }
  const periodsRemaining = totalPeriods - period; // full periods after this one
  return periodsRemaining * periodLengthMin + (clockSec / 60);
}

/**
 * Compute SP-perspective from leg's selection + market. Returns:
 *   { rooting: 'home'|'away'|'over'|'under'|null, spreadOrTotalValue: number|null }
 *
 * `rooting` is which side the BETTOR took — SP wins when the OTHER side
 * wins. So flip to derive SP-perspective probs.
 */
function decodeLegSide(leg) {
  if (!leg) return { rooting: null };
  const market = (leg.market || leg.marketType || '').toLowerCase();
  const selection = (leg.selection || '').toLowerCase();
  if (market === 'moneyline') {
    return { rooting: selection === 'home' ? 'home' : 'away', spreadOrTotalValue: null };
  }
  if (market === 'spread' || market === 'run_line' || market === 'puck_line' || market === 'alt_spread') {
    return { rooting: selection === 'home' ? 'home' : 'away', spreadOrTotalValue: Number(leg.line) || 0 };
  }
  if (market === 'total' || market === 'alt_total') {
    return { rooting: selection === 'over' ? 'over' : 'under', spreadOrTotalValue: Number(leg.line) || null };
  }
  if (market === 'team_total') {
    return { rooting: selection === 'over' ? 'over' : 'under', spreadOrTotalValue: Number(leg.line) || null, isTeamTotal: true };
  }
  return { rooting: null };
}

// --------------------------------------------------------------------
// BASKETBALL — NBA / WNBA / NCAAB (and others)
// --------------------------------------------------------------------

// Per-minute scoring SD per team (empirical). Used to compute the SD of
// the margin between two teams over the remaining minutes:
//   SD_margin_remaining ≈ sqrt(2) * perMinuteSD * sqrt(minutesRemaining)
// Tuned slightly higher for WNBA (smaller league sample, higher per-
// possession variance) and lower for NCAAB (longer possessions).
const BASKETBALL_PER_MINUTE_SD = {
  basketball_nba:   1.10,
  basketball_wnba:  1.15,
  basketball_ncaab: 0.95,
};
const BASKETBALL_PERIOD_LENGTH_MIN = {
  basketball_nba:   12,   // 4×12
  basketball_wnba:  10,   // 4×10
  basketball_ncaab: 20,   // 2×20
};
const BASKETBALL_TOTAL_PERIODS = {
  basketball_nba:   4,
  basketball_wnba:  4,
  basketball_ncaab: 2,
};

function basketballSdMarginRemaining(sport, minRemaining) {
  const perMin = BASKETBALL_PER_MINUTE_SD[sport] || 1.10;
  return Math.sqrt(2) * perMin * Math.sqrt(Math.max(0, minRemaining));
}

function basketballLiveProb(leg, state) {
  const sport = leg.sport || leg.oddsApiSport;
  const periodLen = BASKETBALL_PERIOD_LENGTH_MIN[sport];
  const totalPeriods = BASKETBALL_TOTAL_PERIODS[sport];
  if (!periodLen || !totalPeriods) {
    return { liveFairProb: leg.fairProb || null, model: 'basketball_unsupported', confidence: 'low', reason: 'unrecognized basketball sport key' };
  }
  if (state.completed || state.state === 'post') {
    const settled = settledLegProb(leg, state);
    if (settled != null) return { liveFairProb: settled, model: 'basketball', confidence: 'final', reason: 'game completed' };
  }
  if (!state.period || state.state === 'pre') {
    return { liveFairProb: leg.fairProb || null, model: 'basketball', confidence: 'pregame', reason: 'not started' };
  }
  const clockSec = parseClockToSeconds(state.displayClock);
  if (clockSec == null) {
    // End of quarter / halftime / etc. Assume the period has just ended
    // → minutesRemaining = (totalPeriods - period) * periodLen.
    const minsLeft = Math.max(0, (totalPeriods - state.period) * periodLen);
    return basketballProbFromState(leg, state, minsLeft, 'period-boundary');
  }
  const minsLeft = minutesRemainingTimedGame(state.period, clockSec, periodLen, totalPeriods);
  return basketballProbFromState(leg, state, minsLeft, 'live');
}

function basketballProbFromState(leg, state, minsRemaining, phase) {
  const sport = leg.sport || leg.oddsApiSport;
  const decoded = decodeLegSide(leg);
  const home = state.homeScore || 0;
  const away = state.awayScore || 0;
  const margin = home - away; // positive = home leading

  // Floor on remaining minutes to avoid divide-by-zero (e.g. clock=0:00.
  // Treat 0 minutes as "essentially over" — return 0/1 based on lead.
  if (!(minsRemaining > 0.05)) {
    const settled = settledLegProb(leg, state);
    if (settled != null) return { liveFairProb: settled, model: 'basketball', confidence: 'final', reason: '~0 min remaining' };
  }
  const sd = basketballSdMarginRemaining(sport, minsRemaining);
  const confidence = phase === 'period-boundary' ? 'medium' : (minsRemaining < 8 ? 'high' : 'medium');

  // Returns bettor-perspective P(bettor wins this leg | current state).
  if (decoded.rooting === 'home' && (leg.market || leg.marketType) === 'moneyline') {
    // P(home wins) = P(margin_final > 0) = 1 - Φ(-margin/sd)
    const pBettor = 1 - normalCdf(-margin / sd);
    return { liveFairProb: pBettor, model: 'basketball-ml', confidence, reason: `margin=${margin}, ${minsRemaining.toFixed(1)}min left` };
  }
  if (decoded.rooting === 'away' && (leg.market || leg.marketType) === 'moneyline') {
    const pBettor = normalCdf(-margin / sd);
    return { liveFairProb: pBettor, model: 'basketball-ml', confidence, reason: `margin=${margin}, ${minsRemaining.toFixed(1)}min left` };
  }
  if ((leg.market || leg.marketType) === 'spread' || (leg.market || leg.marketType) === 'alt_spread') {
    // PX stores the line value from the bettor's perspective for spread
    // legs: selection=home with line=+3.5 means "home +3.5" (home is the
    // underdog by 3.5 → bettor wins if home_margin_final > -3.5).
    const line = Number(leg.line) || 0;
    if (decoded.rooting === 'home') {
      // home covers if margin_final > -line
      const pBettor = 1 - normalCdf((-line - margin) / sd);
      return { liveFairProb: pBettor, model: 'basketball-spread', confidence, reason: `home ${line >= 0 ? '+' : ''}${line}, margin=${margin}, ${minsRemaining.toFixed(1)}min left` };
    } else {
      // away covers if -margin_final > -line → margin_final < line
      const pBettor = normalCdf((line - margin) / sd);
      return { liveFairProb: pBettor, model: 'basketball-spread', confidence, reason: `away ${line >= 0 ? '+' : ''}${line}, margin=${margin}, ${minsRemaining.toFixed(1)}min left` };
    }
  }
  if ((leg.market || leg.marketType) === 'total' || (leg.market || leg.marketType) === 'alt_total') {
    // Live total: project remaining points from current pace.
    //   pace = current_total / minutesPlayed (combined points-per-minute)
    //   expectedFinal = total + pace * minsRemaining
    //   SD of remaining total ~ marginSD × 1.4 (BOTH teams' variance adds)
    const total = home + away;
    const periodLen = BASKETBALL_PERIOD_LENGTH_MIN[sport];
    const totalPeriods = BASKETBALL_TOTAL_PERIODS[sport];
    const fullGameMin = periodLen * totalPeriods;
    const minutesPlayed = Math.max(0.5, fullGameMin - minsRemaining);
    const pace = total / minutesPlayed;
    const expectedFinal = total + pace * minsRemaining;
    const sdTotal = basketballSdMarginRemaining(sport, minsRemaining) * 1.4;
    const line = Number(leg.line) || 0;
    if (!sdTotal || sdTotal <= 0) {
      const settled = settledLegProb(leg, state);
      return { liveFairProb: settled != null ? settled : (leg.fairProb || null), model: 'basketball-total', confidence: 'low', reason: 'sd=0' };
    }
    if (decoded.rooting === 'over') {
      const pBettor = 1 - normalCdf((line - expectedFinal) / sdTotal);
      return { liveFairProb: pBettor, model: 'basketball-total', confidence, reason: `over ${line}, current=${total}, exp_final=${expectedFinal.toFixed(0)}, ${minsRemaining.toFixed(1)}min left` };
    } else {
      const pBettor = normalCdf((line - expectedFinal) / sdTotal);
      return { liveFairProb: pBettor, model: 'basketball-total', confidence, reason: `under ${line}, current=${total}, exp_final=${expectedFinal.toFixed(0)}, ${minsRemaining.toFixed(1)}min left` };
    }
  }
  return { liveFairProb: leg.fairProb || null, model: 'basketball', confidence: 'low', reason: `unhandled market: ${leg.market || leg.marketType}` };
}

// --------------------------------------------------------------------
// HOCKEY — NHL
// --------------------------------------------------------------------

// Hockey SD per minute (per-team goals SD). Lower scoring than basketball.
// SD of margin between two teams over remaining min ≈ sqrt(2) * 0.35 * sqrt(min)
const HOCKEY_PER_MINUTE_SD = 0.35;
const HOCKEY_PERIOD_LENGTH_MIN = 20;
const HOCKEY_TOTAL_PERIODS = 3;

function hockeyLiveProb(leg, state) {
  if (state.completed || state.state === 'post') {
    const settled = settledLegProb(leg, state);
    if (settled != null) return { liveFairProb: settled, model: 'hockey', confidence: 'final', reason: 'game completed' };
  }
  if (!state.period || state.state === 'pre') {
    return { liveFairProb: leg.fairProb || null, model: 'hockey', confidence: 'pregame', reason: 'not started' };
  }
  const clockSec = parseClockToSeconds(state.displayClock);
  const minsLeft = clockSec != null
    ? minutesRemainingTimedGame(state.period, clockSec, HOCKEY_PERIOD_LENGTH_MIN, HOCKEY_TOTAL_PERIODS)
    : Math.max(0, (HOCKEY_TOTAL_PERIODS - state.period) * HOCKEY_PERIOD_LENGTH_MIN);
  if (!(minsLeft > 0.05)) {
    const settled = settledLegProb(leg, state);
    if (settled != null) return { liveFairProb: settled, model: 'hockey', confidence: 'final', reason: '~0 min remaining' };
  }
  const sd = Math.sqrt(2) * HOCKEY_PER_MINUTE_SD * Math.sqrt(minsLeft);
  const home = state.homeScore || 0;
  const away = state.awayScore || 0;
  const margin = home - away;
  const decoded = decodeLegSide(leg);
  const conf = minsLeft < 6 ? 'high' : 'medium';
  if ((leg.market || leg.marketType) === 'moneyline') {
    if (decoded.rooting === 'home') {
      const pBettor = 1 - normalCdf(-margin / sd);
      return { liveFairProb: pBettor, model: 'hockey-ml', confidence: conf, reason: `margin=${margin}, ${minsLeft.toFixed(1)}min left` };
    } else if (decoded.rooting === 'away') {
      const pBettor = normalCdf(-margin / sd);
      return { liveFairProb: pBettor, model: 'hockey-ml', confidence: conf, reason: `margin=${margin}, ${minsLeft.toFixed(1)}min left` };
    }
  }
  if ((leg.market || leg.marketType) === 'spread' || (leg.market || leg.marketType) === 'puck_line' || (leg.market || leg.marketType) === 'alt_spread') {
    const line = Number(leg.line) || 0;
    if (decoded.rooting === 'home') {
      const pBettor = 1 - normalCdf((-line - margin) / sd);
      return { liveFairProb: pBettor, model: 'hockey-spread', confidence: conf, reason: `home ${line >= 0 ? '+' : ''}${line}, margin=${margin}, ${minsLeft.toFixed(1)}min left` };
    } else {
      const pBettor = normalCdf((line - margin) / sd);
      return { liveFairProb: pBettor, model: 'hockey-spread', confidence: conf, reason: `away ${line >= 0 ? '+' : ''}${line}, margin=${margin}, ${minsLeft.toFixed(1)}min left` };
    }
  }
  if ((leg.market || leg.marketType) === 'total' || (leg.market || leg.marketType) === 'alt_total') {
    const total = home + away;
    const fullGameMin = HOCKEY_PERIOD_LENGTH_MIN * HOCKEY_TOTAL_PERIODS;
    const minutesPlayed = Math.max(0.5, fullGameMin - minsLeft);
    const pace = total / minutesPlayed;
    const expectedFinal = total + pace * minsLeft;
    const sdTotal = sd * 1.4;
    const line = Number(leg.line) || 0;
    if (decoded.rooting === 'over') {
      const pBettor = 1 - normalCdf((line - expectedFinal) / sdTotal);
      return { liveFairProb: pBettor, model: 'hockey-total', confidence: conf, reason: `over ${line}, current=${total}, exp_final=${expectedFinal.toFixed(1)}, ${minsLeft.toFixed(1)}min left` };
    } else {
      const pBettor = normalCdf((line - expectedFinal) / sdTotal);
      return { liveFairProb: pBettor, model: 'hockey-total', confidence: conf, reason: `under ${line}, current=${total}, exp_final=${expectedFinal.toFixed(1)}, ${minsLeft.toFixed(1)}min left` };
    }
  }
  return { liveFairProb: leg.fairProb || null, model: 'hockey', confidence: 'low', reason: `unhandled market: ${leg.market || leg.marketType}` };
}

// --------------------------------------------------------------------
// BASEBALL — MLB
// --------------------------------------------------------------------

// Win-prob approximation given run differential + outs remaining. We
// don't have base-runner state from ESPN, so this is the cruder
// "score-and-inning" model. SD of remaining-game run margin scales with
// sqrt(half-innings-remaining).
const MLB_RUNS_SD_PER_HALF_INNING = 0.85;

function mlbLiveProb(leg, state) {
  if (state.completed || state.state === 'post') {
    const settled = settledLegProb(leg, state);
    if (settled != null) return { liveFairProb: settled, model: 'mlb', confidence: 'final', reason: 'game completed' };
  }
  if (!state.period || state.state === 'pre') {
    return { liveFairProb: leg.fairProb || null, model: 'mlb', confidence: 'pregame', reason: 'not started' };
  }
  // ESPN shortDetail "Top 7th" / "Bottom 5th" tells us half-inning;
  // status.period is the inning number (1-9+).
  const inning = state.period;
  const shortDetail = (state.shortDetail || '').toLowerCase();
  const isTopHalf = /\btop\b/.test(shortDetail);
  const isBottomHalf = /\bbot/.test(shortDetail);
  // Half-innings remaining: 9 innings × 2 halves = 18 total. If we're in
  // top of inning N, N-1 innings fully complete + the top half is in
  // progress → (9-N+1) + 1 = 10-N half-innings remain. Bottom half: (9-N).
  // Defensive: cap at ≥ 1 half-inning remaining (extras possible but rare
  // in the calibration set).
  let halfInningsRemaining;
  if (inning > 9) halfInningsRemaining = 1; // extras — minimum 1 half
  else if (isTopHalf) halfInningsRemaining = (9 - inning) * 2 + 1.5;
  else if (isBottomHalf) halfInningsRemaining = (9 - inning) * 2 + 0.5;
  else halfInningsRemaining = (9 - inning) * 2;
  if (halfInningsRemaining < 0.5) {
    const settled = settledLegProb(leg, state);
    if (settled != null) return { liveFairProb: settled, model: 'mlb', confidence: 'final', reason: 'game effectively over' };
  }
  const sd = MLB_RUNS_SD_PER_HALF_INNING * Math.sqrt(halfInningsRemaining);
  const home = state.homeScore || 0;
  const away = state.awayScore || 0;
  const margin = home - away;
  const decoded = decodeLegSide(leg);
  const conf = halfInningsRemaining < 4 ? 'high' : 'medium';
  if ((leg.market || leg.marketType) === 'moneyline') {
    if (decoded.rooting === 'home') {
      const pBettor = 1 - normalCdf(-margin / sd);
      return { liveFairProb: pBettor, model: 'mlb-ml', confidence: conf, reason: `margin=${margin}, ${halfInningsRemaining.toFixed(1)} half-innings left` };
    } else if (decoded.rooting === 'away') {
      const pBettor = normalCdf(-margin / sd);
      return { liveFairProb: pBettor, model: 'mlb-ml', confidence: conf, reason: `margin=${margin}, ${halfInningsRemaining.toFixed(1)} half-innings left` };
    }
  }
  if ((leg.market || leg.marketType) === 'spread' || (leg.market || leg.marketType) === 'run_line' || (leg.market || leg.marketType) === 'alt_spread') {
    const line = Number(leg.line) || 0;
    if (decoded.rooting === 'home') {
      const pBettor = 1 - normalCdf((-line - margin) / sd);
      return { liveFairProb: pBettor, model: 'mlb-spread', confidence: conf, reason: `home ${line >= 0 ? '+' : ''}${line}, margin=${margin}, ${halfInningsRemaining.toFixed(1)} half-innings left` };
    } else {
      const pBettor = normalCdf((line - margin) / sd);
      return { liveFairProb: pBettor, model: 'mlb-spread', confidence: conf, reason: `away ${line >= 0 ? '+' : ''}${line}, margin=${margin}, ${halfInningsRemaining.toFixed(1)} half-innings left` };
    }
  }
  if ((leg.market || leg.marketType) === 'total' || (leg.market || leg.marketType) === 'alt_total') {
    const total = home + away;
    const totalHalves = 18;
    const halvesPlayed = Math.max(0.5, totalHalves - halfInningsRemaining);
    const pace = total / halvesPlayed;
    const expectedFinal = total + pace * halfInningsRemaining;
    const sdTotal = MLB_RUNS_SD_PER_HALF_INNING * 1.5 * Math.sqrt(halfInningsRemaining);
    const line = Number(leg.line) || 0;
    if (decoded.rooting === 'over') {
      const pBettor = 1 - normalCdf((line - expectedFinal) / sdTotal);
      return { liveFairProb: pBettor, model: 'mlb-total', confidence: conf, reason: `over ${line}, current=${total}, exp_final=${expectedFinal.toFixed(1)}` };
    } else {
      const pBettor = normalCdf((line - expectedFinal) / sdTotal);
      return { liveFairProb: pBettor, model: 'mlb-total', confidence: conf, reason: `under ${line}, current=${total}, exp_final=${expectedFinal.toFixed(1)}` };
    }
  }
  return { liveFairProb: leg.fairProb || null, model: 'mlb', confidence: 'low', reason: `unhandled market: ${leg.market || leg.marketType}` };
}

// --------------------------------------------------------------------
// SETTLED-LEG PROB — final-state lookup for completed games
// --------------------------------------------------------------------

/**
 * When the game is final, return P(bettor wins this leg) ∈ {0, 0.5, 1}
 * (0.5 only on a push). Bettor-perspective to match liveFairProb's
 * convention everywhere else in this module.
 */
function settledLegProb(leg, state) {
  if (state.homeScore == null || state.awayScore == null) return null;
  const home = state.homeScore;
  const away = state.awayScore;
  const margin = home - away; // home perspective
  const decoded = decodeLegSide(leg);
  const market = (leg.market || leg.marketType || '').toLowerCase();
  if (market === 'moneyline') {
    if (decoded.rooting === 'home') return margin > 0 ? 1 : (margin === 0 ? 0.5 : 0);
    if (decoded.rooting === 'away') return margin < 0 ? 1 : (margin === 0 ? 0.5 : 0);
  }
  if (market === 'spread' || market === 'run_line' || market === 'puck_line' || market === 'alt_spread') {
    const line = Number(leg.line) || 0;
    if (decoded.rooting === 'home') {
      const adjMargin = margin + line;
      return adjMargin > 0 ? 1 : (adjMargin === 0 ? 0.5 : 0);
    } else {
      const adjMargin = -margin + line;
      return adjMargin > 0 ? 1 : (adjMargin === 0 ? 0.5 : 0);
    }
  }
  if (market === 'total' || market === 'alt_total') {
    const total = home + away;
    const line = Number(leg.line) || 0;
    if (decoded.rooting === 'over')  return total > line ? 1 : (total === line ? 0.5 : 0);
    if (decoded.rooting === 'under') return total < line ? 1 : (total === line ? 0.5 : 0);
  }
  return null;
}

// --------------------------------------------------------------------
// ROUTER + parlay-level aggregation
// --------------------------------------------------------------------

const SPORT_HANDLERS = {
  basketball_nba:   basketballLiveProb,
  basketball_wnba:  basketballLiveProb,
  basketball_ncaab: basketballLiveProb,
  baseball_mlb:     mlbLiveProb,
  icehockey_nhl:    hockeyLiveProb,
};

/**
 * Public entry: given a leg + current game state, return live-prob estimate.
 * For sports without a model (tennis/soccer/MMA/golf), returns null with
 * pregame confidence — the operator will see the static pre-game prob
 * unchanged on those rows.
 */
function computeLiveLegProb(leg, gameState) {
  if (!leg || !gameState) {
    return { liveFairProb: leg && leg.fairProb || null, model: null, confidence: 'pregame', reason: 'no game state' };
  }
  const sport = leg.sport || leg.oddsApiSport;
  const handler = SPORT_HANDLERS[sport];
  if (!handler) {
    return { liveFairProb: leg.fairProb || null, model: null, confidence: 'pregame', reason: `no in-play model for ${sport}` };
  }
  return handler(leg, gameState);
}

/**
 * Compute live parlay-level fair prob from per-leg live probs. Treats
 * legs as independent (same as the static parlay-prob calc). Returns
 * bettor-perspective liveFairProb plus operator-facing flags:
 *
 *   anyLegLost — any leg's bettor prob is ~0 → parlay is dead for bettor
 *                → SP has effectively won. Good news; operator may want
 *                to highlight (green).
 *   anyLegWon  — any leg's bettor prob is ~1 → that leg's locked in for
 *                bettor. SP-side of THAT leg is settled against us, but
 *                the parlay is still live if other legs are unresolved.
 *   allFinal   — every leg's confidence is 'final' (game completed).
 */
function computeLiveParlayProb(legResults) {
  if (!Array.isArray(legResults) || legResults.length === 0) {
    return { liveFairProb: null, perLeg: [], allFinal: false, anyLegLost: false, anyLegWon: false };
  }
  let prob = 1;
  let allFinal = true;
  let anyLegLost = false; // bettor cannot win this leg (SP wins)
  let anyLegWon = false;  // bettor has locked this leg (SP loses this leg)
  let anyMissing = false;
  for (const r of legResults) {
    if (r.liveFairProb == null || !Number.isFinite(r.liveFairProb)) {
      anyMissing = true;
      allFinal = false;
      continue;
    }
    if (r.confidence !== 'final') allFinal = false;
    if (r.liveFairProb <= 0.001) anyLegLost = true;
    if (r.liveFairProb >= 0.999) anyLegWon = true;
    prob *= r.liveFairProb;
  }
  if (anyMissing) return { liveFairProb: null, perLeg: legResults, allFinal: false, anyLegLost, anyLegWon };
  return { liveFairProb: prob, perLeg: legResults, allFinal, anyLegLost, anyLegWon };
}

module.exports = {
  computeLiveLegProb,
  computeLiveParlayProb,
  // exported for tests
  _internal: {
    normalCdf,
    parseClockToSeconds,
    minutesRemainingTimedGame,
    decodeLegSide,
    basketballLiveProb,
    hockeyLiveProb,
    mlbLiveProb,
    settledLegProb,
  },
};
