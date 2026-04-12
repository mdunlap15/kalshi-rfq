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
    defaultVig: parseFloat(process.env.DEFAULT_VIG) || 0.001,
    // Per-sport vig overrides. Keyed by odds-feed sport key.
    // Falls back to defaultVig if sport not listed.
    // Adjustable at runtime via POST /config/vig.
    vigBySport: {},
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
      'basketball_ncaab': 10,
      'tennis': 10,
    },
    // Confirmation-time re-price drift threshold. If current fair prob drifts
    // by more than this fraction from the original quote, reject the confirm.
    confirmationDriftThreshold: parseFloat(process.env.CONFIRMATION_DRIFT_THRESHOLD) || 0.03,
    offerValidSeconds: 120,
    maxExposurePerTeam: parseFloat(process.env.MAX_EXPOSURE_PER_TEAM) || 5000,
    bankroll: parseFloat(process.env.BANKROLL) || 0,
    // Override live PX balance with a fixed amount. Set to 0 (or unset) to use live balance.
    assumedBankroll: parseFloat(process.env.ASSUMED_BANKROLL) || 0,
    maxDrawdownPct: parseFloat(process.env.MAX_DRAWDOWN_PCT) || 100,
    maxRiskPerParlayPct: parseFloat(process.env.MAX_RISK_PER_PARLAY_PCT) || 5,
    maxExposurePerGamePct: parseFloat(process.env.MAX_EXPOSURE_PER_GAME_PCT) || 10,
    maxOdds: parseInt(process.env.MAX_ODDS) || 1500,
  },
  supportedSports: (process.env.SUPPORTED_SPORTS || 'basketball_nba,basketball_ncaab,baseball_mlb,icehockey_nhl,tennis,soccer,soccer_usa_mls,soccer_epl')
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
 * Get effective bankroll — assumed override (sandbox testing) takes priority,
 * then live PX balance, then fallback to env var.
 */
function getBankroll() {
  if (config.pricing.assumedBankroll && config.pricing.assumedBankroll > 0) {
    return config.pricing.assumedBankroll;
  }
  return config.pricing.liveBankroll || config.pricing.bankroll;
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
