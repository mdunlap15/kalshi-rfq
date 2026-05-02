require('dotenv').config({ path: __dirname + '/.env' });

const config = {
  px: {
    baseUrl: process.env.PX_BASE_URL || 'https://cash.api.prophetx.co',
    accessKey: process.env.PX_ACCESS_KEY,
    secretKey: process.env.PX_SECRET_KEY,
    tokenTtlMinutes: 9,
  },
  oddsApi: {
    baseUrl: 'https://api.sharpapi.io/api/v1',
    apiKey: process.env.SHARP_ODDS_API_KEY || process.env.ODDS_API_KEY,
    cacheTtlMinutes: parseInt(process.env.ODDS_CACHE_TTL_MINUTES) || 5,
  },
  dataGolf: {
    apiKey: process.env.DATAGOLF_API_KEY,
    baseUrl: 'https://feeds.datagolf.com',
  },
  pricing: {
    defaultVig: parseFloat(process.env.DEFAULT_VIG) || 0.015,
    // Per-sport vig overrides. Keyed by odds-feed sport key.
    // Falls back to defaultVig if sport not listed.
    // Bootstrapped from VIG_BY_SPORT env var (JSON-encoded map) so values
    // survive Railway redeploys. Still adjustable at runtime via POST
    // /config/vig — runtime POSTs override the env-var defaults until the
    // next restart, at which point the env-var values take over again.
    vigBySport: (() => {
      if (!process.env.VIG_BY_SPORT) return {};
      try {
        const parsed = JSON.parse(process.env.VIG_BY_SPORT);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed;
        console.warn('VIG_BY_SPORT must be a JSON object — got', typeof parsed, '— ignoring');
        return {};
      } catch (e) {
        console.warn('Invalid VIG_BY_SPORT JSON, ignoring:', e.message);
        return {};
      }
    })(),
    // Heavy-favorite vig ramp. For legs with fairProb > 0.5, vig is computed as:
    //   vig = max(vigFavoriteFloor, baseVig + vigFavoriteSlope * (fairProb - 0.5))
    // Tunable at runtime via Railway env vars without code changes.
    // Slope 0.075 default: matches old 2.5% step at p=0.70, exceeds it everywhere
    // above, and adds meaningful bite in the long tail (4% at p=0.90, 4.4% at p=0.95).
    // Floor 0 (off) by default — set e.g. 0.02 to enforce a 2% minimum on favorite legs.
    vigFavoriteSlope: parseFloat(process.env.VIG_FAVORITE_SLOPE) || 0.075,
    vigFavoriteFloor: parseFloat(process.env.VIG_FAVORITE_FLOOR) || 0,
    // Minimum per-leg vig for series_winner legs (NBA/NHL playoff
    // series). DK charges ~4-5% per-leg on these and we're typically
    // the only SP quoting them on PX — so we can widen our spread
    // without losing flow. Applied as a floor on top of the normal
    // baseVig + favorite ramp, so extreme favorites still pay more.
    // Default 0.05 (5%); tunable via VIG_SERIES_MIN env var.
    vigSeriesMin: parseFloat(process.env.VIG_SERIES_MIN) || 0.05,
    // Pitcher strikeouts prop floor — minimum per-leg vig applied to
    // marketType='player_strikeouts' legs. Skips the favorite-slope
    // ramp that game-line legs use because props don't have favorites
    // in the team-line sense. Conservative floor for MVP; can lower
    // once Phase 2 proves +EV at scale.
    vigPropFloor: parseFloat(process.env.VIG_PROP_FLOOR) || 0.03,
    // Threshold on NBA series_winner favorite pricing. If our fair prob
    // for an NBA series_winner favorite exceeds this cutoff (default
    // -250 = 250/350 = 0.7143 fair prob), we quote at DK's posted book
    // price directly instead of our de-vigged-plus-vig number — avoids
    // drifting out of market on extreme favorites where our ramp would
    // produce an uncompetitive line. Applies to series_winner only;
    // series_spread and series_total pass through normally.
    // Tightened -1000 → -500 → -250 over iterations as we measured
    // heavy favorites as a meaningful chunk of NBA series bleed.
    // Tunable via NBA_SERIES_FAV_CAP_ODDS env var.
    nbaSeriesFavoriteCapAmericanOdds: parseInt(process.env.NBA_SERIES_FAV_CAP_ODDS) || -250,
    // Cap the favorite side's share of the book's overround during
    // 2-way de-vig. Proportional de-vig (share = favImplied/sumImplied)
    // over-corrects heavy favorites — on DK -3000/+1300 it strips ~4pp
    // off the favorite and leaves our fair ~15pp looser than DK posts.
    // Capping at 0.5 is the standard "additive margin" method: each side
    // absorbs at most half the overround. Only binds once favorite
    // implied share exceeds the cap (i.e., any 2-way with a meaningful
    // favorite); coinflips are unaffected. Tunable via
    // DEVIG_FAV_MAX_SHARE env var — lower values (0.3-0.4) bias harder
    // toward DK's posted on heavy favs.
    devigFavMaxShare: parseFloat(process.env.DEVIG_FAV_MAX_SHARE) || 0.5,
    // Minimum per-leg vig for MMA legs (moneyline + total rounds).
    // MMA is a low-competition market on PX and DK's per-leg vig is
    // ~4-5%; we can widen without losing flow. Applied as a floor on
    // top of the normal baseVig + favorite ramp. Tunable via
    // VIG_MMA_MIN env var.
    vigMmaMin: parseFloat(process.env.VIG_MMA_MIN) || 0.03,
    // Longshot vig widening: add extra vig on low-PARLAY-fair-prob quotes
    // (long odds). Per-leg favorite ramp only fires above fairProb 0.5 —
    // it doesn't help multi-leg parlays made of dog legs, which hit a low
    // parlay-product fair prob without any single leg triggering the ramp.
    // Observed (2026-04-23): our parlay offer avg sits +0.76pp above fair
    // while Pinnacle averages +1.11pp on comparable parlays, with the
    // biggest gap in the low-prob region. Bettors are less price-sensitive
    // on long odds — an extra 20¢ on a +500 offer is invisible to them
    // but is meaningful EV for us.
    //
    // Formula: if parlayFairProb < threshold, add a linear ramp that
    // peaks at maxAdd when parlayFairProb → 0:
    //   ramp = maxAdd * (1 - parlayFairProb / threshold)
    //
    // Sample with threshold=0.25, maxAdd=0.010:
    //   parlayFair=0.05 → +0.8pp vig
    //   parlayFair=0.10 → +0.6pp
    //   parlayFair=0.15 → +0.4pp
    //   parlayFair=0.20 → +0.2pp
    //   parlayFair≥0.25 → 0 (no change)
    //
    // Applied in both parlay-level and per-leg modes. Set maxAdd=0 to
    // disable. Tunable via VIG_LONGSHOT_THRESHOLD and VIG_LONGSHOT_MAX_ADD.
    vigLongshotThreshold: parseFloat(process.env.VIG_LONGSHOT_THRESHOLD) || 0.25,
    vigLongshotMaxAdd: parseFloat(process.env.VIG_LONGSHOT_MAX_ADD) || 0.010,
    // Fair-prob multiplier markup. Mirrors how Pinnacle / DK / FD price
    // parlays — the pp distance from fair grows linearly with fair_prob
    // because their markup is applied as a fraction of fair_prob rather
    // than (1 - vig) on payout. Our existing payout-based vig formula
    // produces a roughly FLAT pp-distance curve across fair (or even
    // slightly decreasing); books slope upward.
    //
    // When vigFairMultiplier > 0, after computing offeredImpliedProb via
    // the existing payout formula, we compute a candidate offered prob
    // as fair × (1 + vigFairMultiplier) and take the MAX of the two.
    // Means at LOW fair the existing longshot ramp still dominates;
    // at HIGH fair (where the payout formula gives a tiny pp gap) the
    // multiplier kicks in and produces a Pinnacle-shaped curve.
    //
    // Sample with vigFairMultiplier=0.04:
    //   fair=10% → multiplier offered = 10.4% → +0.4pp
    //   fair=20% → multiplier offered = 20.8% → +0.8pp
    //   fair=40% → multiplier offered = 41.6% → +1.6pp
    //
    // Default 0 = disabled (current behavior). Tunable via
    // VIG_FAIR_MULTIPLIER env var.
    vigFairMultiplier: parseFloat(process.env.VIG_FAIR_MULTIPLIER) || 0,
    // Heavy-favorite fair markup. Per-leg fair-shaped widening that
    // fires only when a leg's fair_prob exceeds vigHeavyFavThreshold.
    // Applied as MAX(payout-vig offered, fair × (1 + markup)) on
    // qualifying legs. Mirrors the VIG_FAIR_MULTIPLIER MAX gate but is
    // per-leg and gated to chalk; gives DK-retail-like markup on
    // -300+ favorite legs without affecting coinflip or longshot legs.
    //
    // Why exists: payout-vig markup on chalky legs is microscopic
    // because payout is small (-400 fav has payout 0.25, so 5% vig
    // = 1.25pp shift). Books apply markup as fraction of fair_prob
    // instead, which scales properly. Default 0 = disabled.
    vigHeavyFavFairMarkup: parseFloat(process.env.VIG_HEAVY_FAV_FAIR_MARKUP) || 0,
    vigHeavyFavThreshold: parseFloat(process.env.VIG_HEAVY_FAV_THRESHOLD) || 0.70,
    // Chalk-stack parlay surcharge. Parlay-level fair-shaped widening
    // that fires only when EVERY leg of a multi-leg parlay is a
    // favorite (fair_prob > vigChalkStackLegThreshold) AND the parlay's
    // combined fair exceeds vigChalkStackParlayThreshold (parlay isn't
    // a longshot). Applied via MAX gate after VIG_FAIR_MULTIPLIER.
    //
    // Why exists: stacking 3-4 heavy favorites compounds to a
    // near-coinflip parlay; bettors love this shape and books charge
    // an outsized chalk-stack premium (DK +101 where our fair-driven
    // pricing produces +120). This knob lets us approach DK-style
    // pricing on chalk stacks without touching single-leg quotes or
    // mixed parlays. Default 0 = disabled.
    vigChalkStackSurcharge: parseFloat(process.env.VIG_CHALK_STACK_SURCHARGE) || 0,
    vigChalkStackLegThreshold: parseFloat(process.env.VIG_CHALK_STACK_LEG_THRESHOLD) || 0.60,
    vigChalkStackParlayThreshold: parseFloat(process.env.VIG_CHALK_STACK_PARLAY_THRESHOLD) || 0.25,
    // Template-exposure ramp: penalizes bets whose canonical parlay signature
    // (sorted team+market+line tuple) has already confirmed N times inside a
    // rolling window. Catches the April 18 failure mode: multiple bettors
    // stacking the IDENTICAL parlay — a hidden correlation dimension the
    // existing team/event exposure caps can't see. See
    // services/template-exposure.js for mechanism + empirical derivation.
    //
    // Defaults calibrated from 9-day (Apr 14-22) counterfactual analysis:
    // blocking the 4th+ same-template bet would have avoided $5,443 in
    // losses at a cost of $64 in foregone wins across the window.
    //
    // Tier adds are ADDITIVE to the base vig (same units as vigLongshotMaxAdd).
    // Capped downstream in the pricer at 0.20 so they can't stack runaway.
    templateRampEnabled:
      process.env.TEMPLATE_RAMP_ENABLED !== 'false' && process.env.TEMPLATE_RAMP_ENABLED !== '0',
    templateRampWindowHours: parseFloat(process.env.TEMPLATE_RAMP_WINDOW_HOURS) || 24,
    templateRampTier2Add: parseFloat(process.env.TEMPLATE_RAMP_TIER2_ADD) || 0.0025,  // +0.25pp on 2nd bet
    templateRampTier3Add: parseFloat(process.env.TEMPLATE_RAMP_TIER3_ADD) || 0.010,   // +1.00pp on 3rd
    templateRampTier4Add: parseFloat(process.env.TEMPLATE_RAMP_TIER4_ADD) || 0.030,   // +3.00pp on 4th
    templateRampDeclineAt: parseInt(process.env.TEMPLATE_RAMP_DECLINE_AT) || 4,       // decline 5th+ bet (priorCount >= 4)
    // Block alt-spread quoting on listed sports. An "alt spread" is any
    // spread leg whose line value differs from the primary line:
    //   - MLB:  primary run line is always ±1.5 → anything else is alt
    //   - NHL:  primary puck line is always ±1.5 → anything else is alt
    //   - NBA:  primary spread varies per game → use lineInfo.onDemand=true
    //           (PX RFQ asked for a line that wasn't pre-registered, virtually
    //           registered by the line-manager — strong proxy for "alt")
    //
    // Comma-separated list of sport keys. Default blocks NBA/MLB/NHL based on
    // Apr 25 forensic review showing red-box (low-fair-prob) parlays —
    // disproportionately built from alt-spread legs — were the entire
    // P&L drag (-$4.9k of the -$84% red-box bleed). Set to empty string
    // ("") to disable the block, or change the list to widen / narrow it.
    blockAltSpreadSports: (process.env.BLOCK_ALT_SPREAD_SPORTS == null
      ? 'baseball_mlb,icehockey_nhl,basketball_nba'
      : process.env.BLOCK_ALT_SPREAD_SPORTS
    ).split(',').map(s => s.trim()).filter(Boolean),
    // NBA-specific carve-out within the alt-spread block: even when NBA is in
    // blockAltSpreadSports, allow alt-spread legs whose line is within
    // ±N points of the primary spread (in home-team perspective) AND has
    // book coverage in our alt-lines cache. Default 2.0 — operator wants
    // "if main is Team A −5, allow Team A −3..−7 (and equivalent dog
    // sides)" but block anything farther OR anything we'd have to derive
    // ourselves (no books reported it).
    nbaAltSpreadMaxDistance: parseFloat(process.env.NBA_ALT_SPREAD_MAX_DISTANCE) || 2.0,
    // Same idea as nbaAltSpreadMaxDistance but for the totals market.
    // If primary NBA total is O/U 215.5, allow alt totals 213.5 / 214 /
    // 214.5 / 215 / 216 / 216.5 / 217 / 217.5 (within ±2). Block farther
    // alts. Like the spread carve-out, also requires book coverage in
    // our altLines cache — no derived/inferred lines.
    nbaAltTotalMaxDistance: parseFloat(process.env.NBA_ALT_TOTAL_MAX_DISTANCE) || 2.0,
    // MLB alt run-line allowed |line| values. Discrete allowlist (not a
    // distance from primary) because a distance check of 1.0 would also
    // pull in 2.5, which is too aggressive.
    // Comma-separated env override; values are absolute (sign-agnostic).
    // 2026-05-01: Mike expanded to include 1.0 ("MLB and NHL spreads of
    // ±0.5, ±1.0, ±1.5"). Non-primary alts still require book coverage
    // in the alt-spread cache; primary ±1.5 passes without coverage check.
    mlbAllowedRunLines: (process.env.MLB_ALLOWED_RUN_LINES || '0.5,1.0,1.5')
      .split(',').map(s => parseFloat(s.trim())).filter(n => Number.isFinite(n)),
    // NHL alt puck-line allowed |line| values. Same pattern as MLB run
    // lines — primary is ±1.5; ±0.5 and ±1.0 are alts with book-coverage
    // gating. Without this allowlist, all NHL alt puck-lines decline as
    // 'icehockey_nhl alt spread' (a hard block from blockAltSpreadSports).
    // 2026-05-01 unblocked NHL alt-spreads in this range per Mike's request
    // — was previously the dominant decline category (~4,500/day).
    nhlAllowedPuckLines: (process.env.NHL_ALLOWED_PUCK_LINES || '0.5,1.0,1.5')
      .split(',').map(s => parseFloat(s.trim())).filter(n => Number.isFinite(n)),
    // MLB alt-total max distance from primary (default ±1.5 in any 0.5
    // step). E.g. primary 7.5 → allow 6.0/6.5/7.0/7.5/8.0/8.5/9.0.
    // Also requires book coverage in the altTotals cache.
    mlbAltTotalMaxDistance: parseFloat(process.env.MLB_ALT_TOTAL_MAX_DISTANCE) || 1.5,
    // v2 pricing engine: shadow-mode by default. When enabled, runs the
    // unified calibration-corrected + correlation-aware + EV-targeted
    // pipeline alongside v1 and logs the comparison. Does NOT affect
    // live offers until pricingV2Live is true.
    //
    // Two flags so we can ship code without behavior change:
    //   pricingV2Enabled — compute v2 alongside v1, log deltas (observation mode)
    //   pricingV2Live    — use v2 as the authoritative offer (A/B or cutover)
    //
    // Knobs:
    //   pricingV2TargetEdge — single vig parameter replacing the v1 stack
    //   pricingV2KSigma     — conservative uncertainty shift (0.5 = half-sigma)
    pricingV2Enabled:
      process.env.PRICING_V2_ENABLED === 'true' || process.env.PRICING_V2_ENABLED === '1',
    pricingV2Live:
      process.env.PRICING_V2_LIVE === 'true' || process.env.PRICING_V2_LIVE === '1',
    pricingV2TargetEdge: parseFloat(process.env.PRICING_V2_TARGET_EDGE) || 0.02,
    pricingV2KSigma: parseFloat(process.env.PRICING_V2_K_SIGMA) || 0.5,
    // A/B split control. pricingV2Live is the master kill-switch (false =
    // v2 never overrides v1, regardless of arm). pricingV2LivePercent is
    // the fraction of parlays (0-100) whose parlayId-hash falls in the
    // v2 arm. At 0, the master flag is a no-op; at 100, every parlay is
    // v2-arm. Assignment is ALWAYS recorded in meta.abArm even when
    // master is off, so analytics can attribute shadow records by arm.
    pricingV2LivePercent: (() => {
      const v = parseInt(process.env.PRICING_V2_LIVE_PERCENT);
      if (!Number.isFinite(v) || v < 0) return 0;
      if (v > 100) return 100;
      return v;
    })(),
    // Safety net: decline any total leg where our de-vigged fair diverges
    // from the simple book consensus (mean of Pin/DK/FD implied probs) by
    // more than the threshold. Backstop for the getBookPairsForTotals fix
    // in case another feed-shape edge case slips through. Limited to
    // 'total' and 'run_line' market types — the scope where the Apr-24
    // CLE @ TOR U 8.5 bug was observed (our fair 90.36% vs books 55.5%).
    // Enabled by default so fresh deploys are protected. Set
    // DECLINE_ANOMALOUS_TOTALS=false to disable; tune threshold via
    // DECLINE_ANOMALOUS_TOTALS_THRESHOLD (default 0.10 = 10pp).
    declineAnomalousTotalsEnabled:
      process.env.DECLINE_ANOMALOUS_TOTALS !== 'false' && process.env.DECLINE_ANOMALOUS_TOTALS !== '0',
    declineAnomalousTotalsThreshold: parseFloat(process.env.DECLINE_ANOMALOUS_TOTALS_THRESHOLD) || 0.10,
    // Defensive decline on team_total legs. Original bug (2026-04-23
    // ATL Over 4.5 mispricing via buildConsensusTeamTotals pairing
    // mismatched Over/Under lines) was fixed at the root in commit
    // 5ad919f — getBookPairsForTeamTotals now keys on (book, side, line)
    // so Over/Under can only pair at matching lines.
    //
    // External validation (2026-04-23, 4 sides across NYY/BOS and LAD/SF):
    // our de-vigged fair now sits within ±2pp of FanDuel's fair on every
    // tested market — normal book-consensus noise, down from the pre-fix
    // ~10pp bias.
    //
    // Default flipped to FALSE here (re-enable serving) after verification.
    // Leaving the env var as an opt-in circuit breaker — set
    // DECLINE_TEAM_TOTALS=true to re-enable the defensive decline if we
    // discover a new team_total bug class.
    declineTeamTotals:
      process.env.DECLINE_TEAM_TOTALS === 'true' || process.env.DECLINE_TEAM_TOTALS === '1',
    // A/B-testable pricing mode for parlays. When true, vig is applied
    // ONCE at the parlay level using the MAX per-leg effective rate, rather
    // than compounded per-leg. Per-leg compounding penalizes multi-leg
    // parlays (a 5-leg at 2% per leg = 4.2% effective parlay vig), which
    // shows up in our data as a sharp win-rate drop at 4+ legs (28%→14%→9%).
    // Parlay-level application preserves sport-aware pricing + favorite
    // ramp (via the MAX leg's rate) while eliminating the compounding tax.
    // Toggle at runtime via POST /config/vig {parlayLevelVig:true|false}.
    parlayLevelVig: process.env.PARLAY_LEVEL_VIG === 'true' || process.env.PARLAY_LEVEL_VIG === '1',
    maxRiskPerParlay: parseFloat(process.env.MAX_RISK_PER_PARLAY) || 500,
    // Quote-time exposure checks use max_risk × otherProb as the "pending"
    // risk estimate per outstanding RFQ — but bettors essentially never
    // wager the full max. Historical fills on this cluster: median 1.7% of
    // max_risk, p90 ~14%, p99 ~62%. Without a discount, 2-3 simultaneous
    // quotes on the same team can fill up a $4k team limit in pending
    // reservations alone and block further RFQs whose actual expected risk
    // would be trivially small. This factor scales the pending + new-risk
    // numbers at check time only; confirmed exposure (real stakes) is
    // never discounted. Default 0.20 covers the p90 of historical fill
    // sizes with modest margin. 1.0 disables the discount (pre-existing
    // behavior). Tunable via PENDING_RESERVATION_DISCOUNT env var.
    pendingReservationDiscount: (() => {
      const v = parseFloat(process.env.PENDING_RESERVATION_DISCOUNT);
      if (!Number.isFinite(v) || v <= 0 || v > 1) return 0.20;
      return v;
    })(),
    maxLegs: parseInt(process.env.MAX_LEGS) || 8,
    stalePriceMinutes: parseInt(process.env.STALE_PRICE_MINUTES) || 5,
    // Per-sport override for stale threshold (minutes). Tighter for fast-moving
    // markets (MMA/boxing move on news; NFL moves on injury reports), looser
    // for slow Odds-API fallback sports that refresh less often.
    // Falls back to stalePriceMinutes if sport not listed.
    stalePriceMinutesBySport: {
      'mma_mixed_martial_arts': 3,
      'boxing_boxing': 3,
      'americanfootball_nfl': 4,
      'americanfootball_ncaaf': 4,
      'basketball_ncaab': 5,
      'tennis': 4,
      'basketball_wnba': 5,
      'golf_pga_championship': 5,
      // Golf matchups come from DataGolf and only refresh on the main 10-min
      // cycle (not in the SharpAPI delta or Odds-API fast-refresh loops), so
      // the effective worst-case cache age is ~10 min + fetch time. A 25-min
      // threshold gives a 15-min buffer over the refresh interval — matchup
      // lines between comparable golfers are stable enough that a somewhat
      // older consensus is still tradeable.
      'golf_matchups': 25,
    },
    // Confirmation-time re-price drift threshold. If current fair prob drifts
    // by more than this fraction from the original quote, reject the confirm.
    confirmationDriftThreshold: parseFloat(process.env.CONFIRMATION_DRIFT_THRESHOLD) || 0.03,
    offerValidSeconds: parseInt(process.env.OFFER_VALID_SECONDS) || 60,
    maxExposurePerTeam: parseFloat(process.env.MAX_EXPOSURE_PER_TEAM) || 5000,
    // Per-team exposure overrides. JSON map of team/fighter name → cap dollars.
    // Looked up FIRST during exposure checks; falls back to maxExposurePerTeam
    // when a team has no entry. Use this to tighten exposure on specific
    // teams/fighters (e.g. a few MMA chalk favorites already in multiple
    // parlays) without lowering the global cap that protects every other
    // team in every other sport.
    //
    // Lookup is case-insensitive after the same normalizeExposureKey
    // canonicalization the exposure map itself uses, so spelling
    // variations resolve consistently. Names not normalizable (empty
    // strings) are ignored.
    //
    // Example:
    //   EXPOSURE_OVERRIDES_PER_TEAM={"Islam Makhachev":500,"Alex Pereira":500}
    //
    // Set/edit on Railway without a code push.
    exposureOverridesPerTeam: (() => {
      const raw = process.env.EXPOSURE_OVERRIDES_PER_TEAM;
      if (!raw || !raw.trim()) return {};
      try {
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object') return {};
        const out = {};
        for (const [name, cap] of Object.entries(parsed)) {
          const num = parseFloat(cap);
          if (Number.isFinite(num) && num > 0) out[name] = num;
        }
        return out;
      } catch (e) {
        // Bad JSON — log via console (logger not available at config-load time)
        console.warn(`[config] EXPOSURE_OVERRIDES_PER_TEAM is not valid JSON: ${e.message}`);
        return {};
      }
    })(),
    // Phase 2 prop quoting caps — applies to ANY parlay containing one
    // or more player_prop legs (NBA points/rebounds/assists/threes,
    // NHL shots_on_goal, MLB pitcher_strikeouts, etc.). Game-line-only
    // parlays use the standard MAX_RISK_PER_PARLAY ($4000). Tunable via
    // env vars.
    maxRiskPerParlayWithProp: parseFloat(process.env.MAX_RISK_PER_PARLAY_WITH_PROP) || 50,
    // DEPRECATED 2026-05-01: pitcher_strikeouts is now governed by the
    // unified MAX_EXPOSURE_PER_PLAYER_* system. This var is kept for
    // backward-compat reads (some legacy logs / instrumentation still
    // reference it) but does NOT drive quote-time gating anymore.
    // Configure MLB pitcher caps via MAX_EXPOSURE_PER_PLAYER_BY_SPORT
    // (e.g. {"baseball_mlb": 2000}) or MAX_EXPOSURE_PER_PLAYER_DEFAULT.
    maxExposurePerPitcher: parseFloat(process.env.MAX_EXPOSURE_PER_PITCHER) || 500,
    // Per-player aggregate exposure cap, keyed by sport. Sums SP-risk
    // across ALL parlays containing ANY prop leg featuring that player,
    // regardless of prop type — so CJ McCollum points + rebounds +
    // threes parlays all roll up to one McCollum line. Critical for
    // cross-prop concentration where one star anchors many tickets.
    // Tunable via MAX_EXPOSURE_PER_PLAYER_BY_SPORT (JSON map). Falls
    // back to MAX_EXPOSURE_PER_PLAYER_DEFAULT for sports not listed.
    maxExposurePerPlayerBySport: (() => {
      if (!process.env.MAX_EXPOSURE_PER_PLAYER_BY_SPORT) {
        return { 'basketball_nba': 200, 'icehockey_nhl': 200 };
      }
      try {
        const parsed = JSON.parse(process.env.MAX_EXPOSURE_PER_PLAYER_BY_SPORT);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed;
        return { 'basketball_nba': 200, 'icehockey_nhl': 200 };
      } catch (e) {
        return { 'basketball_nba': 200, 'icehockey_nhl': 200 };
      }
    })(),
    maxExposurePerPlayerDefault: parseFloat(process.env.MAX_EXPOSURE_PER_PLAYER_DEFAULT) || 200,
    // Minimum number of books with both sides required for a prop leg
    // to be quotable. Below this, decline the parlay (insufficient
    // de-vig confidence — single-book or near-single-book pricing is
    // just re-quoting that book's vigged line).
    propMinBooksWithBothSides: parseInt(process.env.PROP_MIN_BOOKS_WITH_BOTH_SIDES) || 3,
    // Max distance (in stat units) the requested prop line can sit
    // from the primary line for that (player, propType) before we
    // decline. Default ±2 — restricts quoting to near-primary alts
    // where book coverage is dense and bettor edge from "deep alt"
    // mispricing is bounded. Set 0 to allow only primary; set very
    // large (e.g. 99) to disable the cap.
    //
    // Primary line is determined by the line value with the most
    // bookmaker coverage in TOA's per-event response (most books
    // posting both sides → that's the line they all anchor on).
    propAltLineMaxDistance: parseFloat(process.env.PROP_ALT_LINE_MAX_DISTANCE) || 2.0,
    // Master allowlist for live prop quoting. Comma-separated list of
    // "${sport}.${propType}" pairs. Only props in this allowlist are
    // resolved live and quoted; everything else falls into the existing
    // shadow / decline-as-unknown path. Empty allowlist = current
    // behavior (no live prop quoting). Examples:
    //   PROP_LAUNCH_ALLOWLIST="basketball_nba.points"
    //   PROP_LAUNCH_ALLOWLIST="basketball_nba.points,basketball_nba.rebounds,basketball_nba.assists,basketball_nba.threes_made,icehockey_nhl.shots_on_goal"
    propLaunchAllowlist: new Set(
      (process.env.PROP_LAUNCH_ALLOWLIST || '')
        .split(',').map(s => s.trim()).filter(Boolean)
    ),
    // Books we trust as a single source for prop pricing. When a prop
    // lookup returns exactly 1 book with both sides AND that book is on
    // this list, shouldDecline rule (b) accepts the leg instead of
    // declining for low confidence. Applies to BOTH K-prop AND the
    // Phase-2 launch props (player_points/rebounds/assists/threes/
    // shots_on_goal/hitter_*). Default list: Pinnacle (sharpest book
    // overall), FanDuel + DraftKings (US prop pricing leaders), BetMGM
    // (large US book, generally sharp), BetRivers (smaller but a
    // frequent sole-book on alt lines DK/FD don't post). Tunable via
    // PROP_TRUSTED_SINGLE_BOOKS (comma-separated, lowercase book keys).
    propTrustedSingleBooks: (process.env.PROP_TRUSTED_SINGLE_BOOKS || 'pinnacle,fanduel,draftkings,betmgm,betrivers')
      .split(',').map(s => s.trim().toLowerCase()).filter(Boolean),
    // Per-event aggregate cap. Sums SP-risk across ALL legs touching one
    // pxEventId (regardless of team or market), preventing two-sided
    // event stacking that the per-team cap can't see — e.g. Lakers spread
    // on parlay 1 + Hawks spread on parlay 2 + Over total on parlay 3,
    // each below team cap but together overconcentrated on the LAL @ ATL
    // game. Critical as alt-spread coverage expands: more breakpoints =
    // more ways to load up on one event.
    // Tunable via MAX_EXPOSURE_PER_GAME env var. Set 0 to disable.
    maxExposurePerGame: parseFloat(process.env.MAX_EXPOSURE_PER_GAME) || 5000,
    // Tighter risk caps for parlays containing series_* markets. Series
    // bets tie up bankroll for weeks until the series settles, so we
    // limit both per-parlay SP risk and aggregate per-series-event
    // exposure. Applied only when at least one leg is a series market.
    maxSeriesRiskPerParlay: parseFloat(process.env.MAX_SERIES_RISK_PER_PARLAY) || 500,
    maxSeriesGrossExposure: parseFloat(process.env.MAX_SERIES_GROSS_EXPOSURE) || 1000,

    // Same-game parlay (SGP) handling. Historically all SGPs were blocked
    // because a multiplicative correlation "boost" (+3-15%) caused PX to
    // reject with "invalid estimated prices" on any offer we pushed above
    // their internal SGP model. Now: allow specific market-pair combos,
    // applying a wider per-leg vig (sgpVigMultiplier × normal vig) instead
    // of a boost. PX accepts wider vig; it doesn't accept upward price
    // corrections vs their model.
    //
    // SGP_ALLOWED_COMBOS: comma-separated list of combo keys.
    //   'spread_total'  — spread + total on same game (moderate correlation)
    //   'ml_total'      — moneyline + total (strong correlation, −37% ROI historically)
    //   'ml_spread'     — still blocked by correlation rules regardless (highly correlated)
    //   empty string    — no SGPs allowed (safe default-ish; matches pre-this-change)
    sgpAllowedCombos: (process.env.SGP_ALLOWED_COMBOS || 'spread_total')
      .split(',').map(s => s.trim()).filter(Boolean),
    // Multiplier applied to per-leg effective vig when pricing an SGP.
    // 2.0 = double the normal vig on each leg of the SGP. Tunable while
    // we gather acceptance + ROI data on re-enabled SGPs.
    sgpVigMultiplier: parseFloat(process.env.SGP_VIG_MULTIPLIER) || 2.0,
    // Phase 2 K-prop + same-team ML SGP correlation boost. Empirically
    // calibrated from DK SGP pricing on 3 MLB combos (Guardians/Cardinals/
    // Rays + their pitcher's K-Over): DK applies 10-19% discount (avg
    // 14.5%); FD applies 17-33% (avg 24.3%). Default 0.15 splits the
    // difference toward DK-side (less aggressive correlation cost). The
    // boost MULTIPLIES fairParlayProb upward — bettor gets shorter odds,
    // matching how books charge the bettor for positive correlation.
    sgpPropMlCorrBoost: parseFloat(process.env.SGP_PROP_ML_CORR_BOOST) || 0.15,
    // SGP correlation adjustment factors. The naive product of leg fair
    // probs understates the true joint probability for positively-
    // correlated combos (spread-fav + over, or spread-dog + under) and
    // overstates for negatively-correlated combos (fav + under, dog +
    // over). Applied as a multiplier to the joint fair prob BEFORE vig.
    // Unlike a post-vig offered-price boost (which PX rejected with
    // "invalid estimated prices"), this adjusts the INPUT fair prob —
    // mathematically identical to what every major book does internally,
    // and within PX's accepted pricing model.
    //
    // Empirical FD SGP discount is ~25-30% on spread+total pairs we've
    // observed; start conservative at 1.15 / 0.90 and tune up based on
    // acceptance + ROI data. Set POSITIVE=1 and NEGATIVE=1 to disable.
    sgpCorrelationPositive: parseFloat(process.env.SGP_CORRELATION_POSITIVE) || 1.15,
    sgpCorrelationNegative: parseFloat(process.env.SGP_CORRELATION_NEGATIVE) || 0.90,
    // Per-combo correlation factors. The legacy single sgpCorrelationPositive
    // applied only to spread_total. Operator caught SGP fill rate at 0%
    // across all combo types, including ml_total which gets sgpVigMultiplier
    // applied on TOP of zero correlation discount — pricing every ml+total
    // SGP looser than fair AND wider with vig, double-disadvantage.
    //
    // Map keys are sorted alphabetically: 'ml_total' (not 'total_ml').
    // Defaults are conservative — calibrated below empirical book discounts
    // so we don't over-boost fair on weak data. Tune up gradually with
    // acceptance/ROI data via SGP_CORRELATION_BY_COMBO env JSON map.
    //
    //   spread_total : 1.15 (existing default — moderate, see comment above)
    //   ml_total     : 1.10 (smaller — strongest when team wins AND scores;
    //                        weaker when team wins low-scoring or blowout
    //                        with under)
    //   ml_spread    : not listed; correlation rules already block this
    //                  combo (anti-arb) regardless of pricing
    //   3+leg combos : not yet supported; use 1.0 (no boost) until calibrated
    sgpCorrelationByCombo: (() => {
      const defaults = { spread_total: 1.15, ml_total: 1.10 };
      const raw = process.env.SGP_CORRELATION_BY_COMBO;
      if (!raw || !raw.trim()) return defaults;
      try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === 'object') {
          const out = { ...defaults };
          for (const [k, v] of Object.entries(parsed)) {
            const num = parseFloat(v);
            if (Number.isFinite(num) && num > 0) out[k] = num;
          }
          return out;
        }
      } catch (e) { /* fall through to defaults */ }
      return defaults;
    })(),
    // startingBankroll anchors the account-based P&L calculation
    // (balance − starting). If env var is NOT set, leave as null so the
    // dashboard falls back to the tracker's runningPnL (derived from
    // real settled outcomes, not an arbitrary anchor).
    startingBankroll: process.env.STARTING_BANKROLL != null && process.env.STARTING_BANKROLL !== ''
      ? parseFloat(process.env.STARTING_BANKROLL)
      : null,
    maxOdds: parseInt(process.env.MAX_ODDS) || 1500,
  },
  supportedSports: (process.env.SUPPORTED_SPORTS || 'basketball_nba,basketball_ncaab,basketball_wnba,baseball_mlb,icehockey_nhl,tennis,soccer,soccer_usa_mls,soccer_epl,soccer_mexico_ligamx,soccer_brazil_campeonato,soccer_conmebol_libertadores')
    .split(',').map(s => s.trim()),
  // Maps our sport keys to ProphetX sport_name values
  // Note: NBA and NCAAB both map to 'Basketball' — line manager handles both
  // Note: MLS and EPL both map to 'Soccer' — line manager tries all matching keys
  sportNameMap: {
    'basketball_nba': 'Basketball',
    'basketball_ncaab': 'Basketball',
    'basketball_wnba': 'Basketball',
    'baseball_mlb': 'Baseball',
    'icehockey_nhl': 'Ice Hockey',
    'tennis': 'Tennis',
    'americanfootball_nfl': 'American Football',
    'americanfootball_ncaaf': 'American Football',
    'soccer': 'Soccer',
    'soccer_usa_mls': 'Soccer',
    'soccer_epl': 'Soccer',
    'soccer_uefa_champs_league': 'Soccer',
    'soccer_uefa_europa_league': 'Soccer',
    'soccer_spain_la_liga': 'Soccer',
    'soccer_italy_serie_a': 'Soccer',
    'soccer_germany_bundesliga': 'Soccer',
    'soccer_france_ligue_one': 'Soccer',
    'soccer_usa_nwsl': 'Soccer',
    'soccer_mexico_ligamx': 'Soccer',
    'soccer_brazil_campeonato': 'Soccer',
    'soccer_conmebol_libertadores': 'Soccer',
    'golf_pga_championship': 'Golf',
    'golf_matchups': 'Golf',
    // PX uses 'MMA' (short form) as sport_name, not 'Mixed Martial Arts'.
    // Getting this wrong silently blocks every MMA event in seedAllLines
    // because pxSportNames.includes(event.sport_name) returns false.
    'mma_mixed_martial_arts': 'MMA',
    'boxing_boxing': 'Boxing',
  },
  server: {
    port: parseInt(process.env.PORT) || 3001,
  },
  logLevel: process.env.LOG_LEVEL || 'info',
  refreshIntervalMinutes: parseInt(process.env.REFRESH_INTERVAL_MINUTES) || 10,
};

/**
 * Get effective bankroll — auto-populated from live PX balance at startup
 * and every refresh cycle. Used only for display/P&L anchoring now that the
 * percent-based exposure caps have been removed.
 */
function getBankroll() {
  return config.pricing.liveBankroll || 0;
}

// Validate required config
function validate() {
  const missing = [];
  if (!config.px.accessKey) missing.push('PX_ACCESS_KEY');
  if (!config.px.secretKey) missing.push('PX_SECRET_KEY');
  if (!config.oddsApi.apiKey) missing.push('SHARP_ODDS_API_KEY');
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
  // Sanity-check env values for the "name leaked into value" typo class:
  // operator types `VAR=value1,value2` as the VALUE in Railway, not just
  // `value1,value2`. The leaked prefix breaks any parser that splits on
  // delimiter and expects clean tokens. Caught operator-side 2026-05-01:
  // PROP_LAUNCH_ALLOWLIST first entry was the literal string
  // 'PROP_LAUNCH_ALLOWLIST=basketball_nba.points' instead of just
  // 'basketball_nba.points', silently breaking the points-prop allowlist
  // gate. Walk all process.env keys and warn on any value starting with
  // its own key followed by '='. Also catches surrounding-quote and
  // leading-equals typos as a side effect.
  const warnings = [];
  for (const [key, val] of Object.entries(process.env)) {
    if (typeof val !== 'string' || val.length === 0) continue;
    // Common shape: "VAR=foo,bar" pasted as the value of VAR
    if (val.startsWith(key + '=')) {
      const cleaned = val.slice(key.length + 1);
      warnings.push({
        type: 'name_leaked',
        key,
        rawValue: val.length > 80 ? val.slice(0, 77) + '...' : val,
        suggestedValue: cleaned.length > 80 ? cleaned.slice(0, 77) + '...' : cleaned,
      });
    }
    // Surrounding single/double quotes — Railway often does NOT strip
    // these (some platforms do). Warn so operator can decide.
    else if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      // Ignore short legitimate values like '0', and obvious empty strings
      if (val.length > 2) {
        warnings.push({
          type: 'wrapping_quotes',
          key,
          rawValue: val.length > 80 ? val.slice(0, 77) + '...' : val,
          suggestedValue: val.slice(1, -1),
        });
      }
    }
    // Leading '=' — operator pasted "=value" by accident
    else if (val.startsWith('=')) {
      warnings.push({
        type: 'leading_equals',
        key,
        rawValue: val.length > 80 ? val.slice(0, 77) + '...' : val,
        suggestedValue: val.slice(1),
      });
    }
  }
  if (warnings.length > 0) {
    // Log via console (logger may not be ready at config-load time)
    for (const w of warnings) {
      console.warn(
        `[config] ENV TYPO WARNING (${w.type}) for ${w.key}: value starts with "${w.key}=" or is wrapped in quotes. ` +
        `Got: ${JSON.stringify(w.rawValue)}. Suggested fix: set value to ${JSON.stringify(w.suggestedValue)}`
      );
    }
  }
  return { warnings };
}

module.exports = { config, validate, getBankroll };
