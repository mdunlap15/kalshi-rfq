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
}

module.exports = { config, validate, getBankroll };
