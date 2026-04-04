require('dotenv').config({ path: __dirname + '/.env' });

const config = {
  px: {
    baseUrl: process.env.PX_BASE_URL || 'https://api-ss-sandbox.betprophet.co',
    accessKey: process.env.PX_ACCESS_KEY,
    secretKey: process.env.PX_SECRET_KEY,
    tokenTtlMinutes: 8,
  },
  oddsApi: {
    baseUrl: 'https://api.sharpapi.io/api/v1',
    apiKey: process.env.SHARP_ODDS_API_KEY || process.env.ODDS_API_KEY,
    cacheTtlMinutes: parseInt(process.env.ODDS_CACHE_TTL_MINUTES) || 5,
  },
  pricing: {
    defaultVig: parseFloat(process.env.DEFAULT_VIG) || 0.001,
    maxRiskPerParlay: parseFloat(process.env.MAX_RISK_PER_PARLAY) || 500,
    maxLegs: parseInt(process.env.MAX_LEGS) || 8,
    stalePriceMinutes: parseInt(process.env.STALE_PRICE_MINUTES) || 15,
    offerValidSeconds: 120,
    maxExposurePerTeam: parseFloat(process.env.MAX_EXPOSURE_PER_TEAM) || 5000,
    bankroll: parseFloat(process.env.BANKROLL) || 100000,
    // Sandbox testing override — when set, ignore live PX balance and use this fixed amount
    assumedBankroll: parseFloat(process.env.ASSUMED_BANKROLL) || 600000,
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
    'baseball_mlb': 'Baseball',
    'icehockey_nhl': 'Ice Hockey',
    'tennis': 'Tennis',
    'soccer': 'Soccer',
    'soccer_usa_mls': 'Soccer',
    'soccer_epl': 'Soccer',
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
