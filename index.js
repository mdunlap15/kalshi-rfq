// =============================================================================
// ProphetX Parlay Service Provider
// MLB / NBA / NHL / Tennis / Soccer — Spreads, Moneylines, Totals
// =============================================================================
console.log('[BOOT] Process starting, NODE_ENV=' + process.env.NODE_ENV + ', PORT=' + process.env.PORT);

// Configure HTTP transport FIRST — sets global dispatcher for every fetch()
// call everywhere in the app (keep-alive pooling, TCP_NODELAY, HTTP/2 when
// negotiated). Must run before any module that imports fetch or node-fetch.
require('./services/httpClient');

const { config, validate, getBankroll } = require('./config');
const log = require('./services/logger');
const px = require('./services/prophetx');
const oddsFeed = require('./services/odds-feed');
const lineManager = require('./services/line-manager');
const websocket = require('./services/websocket');
const orderTracker = require('./services/order-tracker');
const pricer = require('./services/pricer');
const pxLedger = require('./services/px-ledger');
const dkScraper = require('./services/dk-scraper');
const bovadaAltScraper = require('./services/bovada-alt-scraper');
const db = require('./services/db');
const express = require('express');
const path = require('path');

// ---------------------------------------------------------------------------
// STARTUP
// ---------------------------------------------------------------------------

const startTime = Date.now();
let oddsRefreshTimer = null;
let lineRefreshTimer = null;
let settlementPollTimer = null;
let serviceReady = false;

// Cached Supabase total P&L — refreshed periodically, used by /status
let cachedDbPnL = null;
async function refreshDbPnL() {
  try {
    const total = await db.getTotalPnL();
    if (total != null) cachedDbPnL = Math.round(total * 100) / 100;
  } catch (_) { /* best effort */ }
}

/** Run an async fn with a timeout. Rejects with a clear message on expiry. */
function withTimeout(fn, ms, label) {
  return Promise.race([
    fn(),
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timed out after ${ms / 1000}s`)), ms)),
  ]);
}

async function startup() {
  log.setLevel(config.logLevel);

  console.log('');
  console.log('  ╔═══════════════════════════════════════════════╗');
  console.log('  ║   ProphetX Parlay Service Provider            ║');
  console.log('  ║   MLB · NBA · NHL                             ║');
  console.log('  ╚═══════════════════════════════════════════════╝');
  console.log('');

  // Start Express FIRST so Railway health check passes while we initialize
  log.info('Startup', '0/5 Starting status server...');
  startStatusServer();

  // Validate config (don't exit — keep Express alive for health check)
  try {
    validate();
  } catch (err) {
    log.error('Startup', `Config validation failed: ${err.message}`);
    log.error('Startup', 'Service will stay running but cannot process RFQs until env vars are set');
    return; // Stop startup but keep Express alive
  }

  log.info('Startup', `Config: vig=${config.pricing.defaultVig}, maxRisk=$${config.pricing.maxRiskPerParlay}, maxLegs=${config.pricing.maxLegs}`);
  log.info('Startup', `Sports: ${config.supportedSports.join(', ')}`);
  log.info('Startup', `PX Base URL: ${config.px.baseUrl}`);

  // Step 1: Auth with ProphetX
  // On session_num_exceed, wait for old sessions to expire WITHOUT creating
  // new ones. Test with a single login attempt every 2 min (not every 60s,
  // to avoid burning through the 20-session limit).
  log.info('Startup', '1/5 Authenticating with ProphetX...');
  let authOk = false;
  px.clearCooldown();
  try {
    await px.login();
    log.info('Startup', '    ✓ ProphetX auth OK');
    authOk = true;
  } catch (err) {
    if (err.message.includes('session_num_exceed')) {
      // Wait 10 min (PX session TTL), then try ONCE more.
      // Do NOT retry in a loop — each login() creates a new session.
      log.warn('Startup', '    ⚠ Session limit hit. Waiting 10min for sessions to expire...');
      await new Promise(r => setTimeout(r, 10 * 60 * 1000));
      try {
        px.clearCooldown();
        await px.login();
        log.info('Startup', '    ✓ ProphetX auth OK (after 10min wait)');
        authOk = true;
      } catch (retryErr) {
        log.error('Startup', `    ✗ Auth retry failed: ${retryErr.message}`);
      }
    } else {
      log.error('Startup', `    ✗ ProphetX auth failed: ${err.message}`);
    }
  }
  if (!authOk) {
    log.warn('Startup', 'Continuing without PX auth — click Reconnect when ready');
  }

  // Step 1b: Load historical data from Supabase, then ALWAYS reconcile
  // against PX REST. Supabase often has stale status (orders stored as
  // 'confirmed' even after PX settled them) because settlement was only
  // applied in-memory by fullPxReconcile and not always persisted. PX REST
  // is the authoritative source — reconcile on every startup to ensure
  // settlement status, P&L, and stats are accurate.
  // DB load + PX reconcile run in background so they never block startup.
  // Positions will populate within ~30s after odds/lines are ready.
  const dbAndReconcile = (async () => {
    try {
      await orderTracker.loadFromDb();
    } catch (err) {
      log.warn('Startup', `    ⚠ DB load failed: ${err.message}`);
    }
    if (authOk) {
      try {
        const result = await orderTracker.fullPxReconcile(px);
        log.info('Startup', `    ✓ PX reconcile: imported ${result.imported}, settled ${result.settled}, P&L $${result.after.runningPnL.toFixed(2)}`);
      } catch (err) {
        log.warn('Startup', `    ✗ PX reconcile failed: ${err.message}`);
      }
      // Ghost-confirmed reconcile runs after the full reconcile has
      // established the order baseline. Initial pass immediately, then
      // every 2 minutes. Flags orders our tracker shows 'confirmed' but
      // PX doesn't know about (or has already closed) as phantoms so
      // they stop inflating the Deployed number. Tightened from 5min to
      // 2min after seeing 400+ ghosts accumulate between cycles during
      // normal operation — a stray order.matched without a follow-up
      // order.finalized stays a ghost until the next reconcile, so
      // shorter interval → tighter Deployed accuracy.
      try {
        const ghost = await orderTracker.reconcileGhostConfirmed(px);
        if (ghost.ghostsFound > 0 || ghost.orderUuidFilledIn > 0 || ghost.settledFound > 0) {
          log.info('Startup', `    ✓ Ghost reconcile: ${ghost.ghostsFound} phantoms, ${ghost.orderUuidFilledIn} uuids filled in, ${ghost.settledFound} PX-settled`);
        }
      } catch (err) {
        log.warn('Startup', `    ✗ Ghost reconcile failed: ${err.message}`);
      }
      setInterval(async () => {
        try {
          await orderTracker.reconcileGhostConfirmed(px);
        } catch (err) {
          log.warn('GhostReconcile', `Periodic reconcile failed: ${err.message}`);
        }
      }, 2 * 60 * 1000);
    }
  })();
  // Don't await — let startup continue to odds/lines/WS immediately

  // Step 2: Fetch odds from The Odds API
  log.info('Startup', '2/5 Fetching fair values from The Odds API...');
  try {
    const oddsResults = await oddsFeed.refreshAllSports();
    for (const [sport, result] of Object.entries(oddsResults)) {
      if (result.ok) {
        log.info('Startup', `    ✓ ${sport}: ${result.events} events`);
      } else {
        log.warn('Startup', `    ✗ ${sport}: ${result.error}`);
      }
    }
  } catch (err) {
    log.warn('Startup', `    ✗ Odds fetch failed: ${err.message}`);
    log.warn('Startup', '    Continuing without odds — will decline all RFQs until odds are loaded');
  }

  // Step 3: Seed lines and register with PX
  log.info('Startup', '3/5 Seeding lines and registering with ProphetX...');
  try {
    const seedStats = await lineManager.seedAllLines();
    log.info('Startup', `    ✓ ${seedStats.registeredLines} lines registered (${seedStats.matchedLines} matched of ${seedStats.totalLines} parsed)`);
  } catch (err) {
    log.warn('Startup', `    ✗ Line seeding failed: ${err.message}`);
  }

  // Step 3b: Now that the line index is populated, enrich any reconstructed
  // orders whose legs still have team='?' by looking up their line_id in the
  // fresh lineIndex. Persists enriched legs to Supabase so the data survives
  // the next restart. Then rebuild exposure so Team / Game Exposure tables
  // reflect the newly-enriched legs. Without this step, every deploy wiped
  // the Team Exposure table because loadFromDb ran addExposure BEFORE the
  // line index existed, dropping every leg whose team resolved to '?'.
  // Timeout: 30s — enrichment is best-effort; next refresh cycle catches up.
  try {
    await withTimeout(async () => {
      const enrich = await orderTracker.enrichReconstructedOrders();
      if (enrich.enriched > 0) {
        log.info('Startup', `    ✓ Enriched ${enrich.enriched}/${enrich.scanned} reconstructed orders from lineIndex (persisted ${enrich.persisted})`);
      }
      const diag = orderTracker.rebuildAllExposure();
      log.info('Startup', `    ✓ Exposure rebuilt: ${diag.exposureKeysAfter} team keys, ${diag.gameKeysAfter} games (${diag.legsWithTeamKey}/${diag.legsTotal} legs contributed)`);
    }, 30000, 'Post-seed enrichment');
  } catch (err) {
    log.warn('Startup', `    ✗ Post-seed enrichment/exposure rebuild failed: ${err.message}`);
  }

  // Step 3c: For any orders still unresolved (legs not in the current lineIndex
  // because the event has already started or aged out), resolve them via PX's
  // /partner/affiliate/* bulk endpoints. This is how we recover real team names
  // for confirmed historical parlays instead of showing "Event 10077494".
  // Timeout: 30s — affiliate API can be slow; team names fill in on next cycle.
  try {
    const affil = await withTimeout(() => orderTracker.enrichOpenPositionsFromAffiliate(), 30000, 'Affiliate enrichment');
    log.info('Startup', `    ✓ Affiliate enrichment: ${JSON.stringify(affil)}`);
  } catch (err) {
    log.warn('Startup', `    ✗ Affiliate enrichment failed: ${err.message}`);
  }

  // Step 3c: Pre-warm alt-line cache before WebSocket connect. The odds-refresh
  // in Step 2 already kicked off a fire-and-forget warm, but we await here with
  // a bounded deadline so the first RFQs hit warm cache instead of paying cold
  // decline→price cost (~30ms per unwarmed event). Deadline keeps boot from
  // stalling if The Odds API is slow.
  log.info('Startup', '3c/5 Pre-warming alt-line cache...');
  try {
    const WARM_BOOT_DEADLINE_MS = 15000;
    const result = await Promise.race([
      oddsFeed.warmAllSports(),
      new Promise(r => setTimeout(() => r('__timeout__'), WARM_BOOT_DEADLINE_MS)),
    ]);
    if (result === '__timeout__') {
      log.warn('Startup', `    ⚠ Alt-line warm exceeded ${WARM_BOOT_DEADLINE_MS}ms deadline — continuing; warm loop will finish in background`);
    } else {
      const fetched = (result || []).reduce((s, r) => s + (r.result?.fetched || 0), 0);
      const candidates = (result || []).reduce((s, r) => s + (r.result?.candidates || 0), 0);
      log.info('Startup', `    ✓ Alt-line warm: ${fetched}/${candidates} events cached`);
    }
  } catch (err) {
    log.warn('Startup', `    ✗ Alt-line warm failed: ${err.message} — continuing; warm loop will retry`);
  }

  // Step 4: Connect WebSocket
  log.info('Startup', '4/5 Connecting to ProphetX WebSocket...');
  // Load persisted pause state BEFORE connecting. Boot defaults to
  // paused=true; this overrides with the last-saved state from Supabase
  // so an operator who explicitly resumed before a restart sees the
  // service come back up in the resumed state. Without this, every
  // restart would come up paused until someone manually POST /resume.
  try {
    const pauseLoad = await websocket.loadPausedStateFromDb();
    log.info('Startup', `    ✓ Pause state loaded: paused=${pauseLoad.paused}${pauseLoad.loaded ? '' : ' (fresh / no persisted state)'}`);
  } catch (err) {
    log.warn('Startup', `    ⚠ Pause-state load failed: ${err.message} — defaulting to paused=true`);
  }
  try {
    await websocket.connect();
    log.info('Startup', '    ✓ WebSocket connected');
  } catch (err) {
    log.error('Startup', `    ✗ WebSocket connection failed: ${err.message}`);
    log.warn('Startup', '    Service will run without WebSocket — use /status to check state');
  }

  // Start the periodic alt-line warm loop (every 60s). Keeps the cache fresh
  // continuously, not just on odds-refresh cycles. With 30-min TTL, most calls
  // return cache hits quickly — the loop only re-fetches aging entries.
  oddsFeed.startAltLineWarmLoop();

  // Start Bovada scraper loop — serves alt-line markets The Odds API
  // doesn't cover (NBA H1 alt spreads/totals, team_total alt ladders,
  // NHL periods). Fail-closed: cache misses cascade to decline, never
  // misprice. Refreshes every 2 min.
  oddsFeed.startBovadaAltLoop();

  // Pre-warm Pinnacle line-verify cache every 20s (inside 30s TTL).
  // Closes the cold-cache p95 tail on primary spread/total RFQs where
  // verifyLineWithPinnacle would otherwise block the RFQ on a 20-30ms
  // Odds API fetch. With the loop running, the hot path always hits
  // a warm cache entry.
  oddsFeed.startPinVerifyWarmLoop();

  // Template-exposure prune loop: sweeps out signatures whose in-window
  // confirmations have all aged past the TTL (default 24h). Keeps the
  // in-memory map bounded even over multi-day sessions.
  require('./services/template-exposure').startPruneLoop();

  // v2 pricing engine: train calibration from the loaded historical
  // orders once (non-blocking). Later refits run on a weekly timer.
  // Non-fatal if training data is sparse — calibration just returns
  // neutral corrections until the refit accumulates enough history.
  try {
    const v2 = require('./services/v2');
    const allOrders = orderTracker.getRecentOrders ? orderTracker.getRecentOrders(100000) : [];
    if (allOrders.length > 0) {
      v2.calibration.trainFromOrders(allOrders);
      const stats = v2.calibration.getStats();
      log.info('V2Pricing', `Boot-time calibration: ${stats.legsAnalyzed} legs across ${Object.keys(stats.buckets || {}).length} buckets`);
    } else {
      log.info('V2Pricing', 'No loaded orders yet for calibration; will refit on /v2-refit');
    }
    // Weekly refit timer (7 days)
    setInterval(() => {
      try {
        const orders = orderTracker.getRecentOrders(100000);
        v2.calibration.trainFromOrders(orders);
        log.info('V2Pricing', `Weekly refit: ${v2.calibration.getStats().legsAnalyzed} legs`);
      } catch (err) {
        log.warn('V2Pricing', `weekly refit failed: ${err.message}`);
      }
    }, 7 * 24 * 60 * 60 * 1000);
  } catch (err) {
    log.warn('V2Pricing', `boot-time setup failed: ${err.message}`);
  }

  // Start periodic timers
  const refreshMs = config.refreshIntervalMinutes * 60 * 1000;
  oddsRefreshTimer = setInterval(async () => {
    try {
      await oddsFeed.refreshAllSports();
    } catch (err) {
      log.error('Refresh', `Odds refresh failed: ${err.message}`);
    }
  }, refreshMs);

  // Fast delta updates for SharpAPI sports (every 30s)
  // Catches line movements quickly without full re-fetch
  setInterval(async () => {
    try {
      await oddsFeed.refreshAllSportsDelta();
    } catch (err) {
      log.debug('Refresh', `Delta refresh failed: ${err.message}`);
    }
  }, 30 * 1000);

  // Fast refresh for Odds API sports not covered by SharpAPI delta loop.
  // Full re-fetch every ~2.5 min keeps per-sport stale thresholds honest.
  // Cost: ~12 extra Odds-API calls per cycle, negligible against daily quota.
  const FAST_REFRESH_SPORTS = [
    'mma_mixed_martial_arts', 'boxing_boxing',
    'americanfootball_nfl', 'americanfootball_ncaaf',
    'tennis', 'basketball_wnba', 'basketball_ncaab',
    'soccer_usa_mls', 'soccer_epl', 'soccer_uefa_champs_league',
    'soccer_uefa_europa_league', 'soccer_spain_la_liga',
    'soccer_italy_serie_a', 'soccer_germany_bundesliga',
    'soccer_france_ligue_one', 'soccer_usa_nwsl',
    'soccer_mexico_ligamx', 'soccer_brazil_campeonato',
    'soccer_conmebol_libertadores',
    'golf_pga_championship',
  ];
  setInterval(async () => {
    for (const sport of FAST_REFRESH_SPORTS) {
      if (!config.supportedSports.includes(sport)) continue;
      try {
        await oddsFeed.fetchOddsForSport(sport);
      } catch (err) {
        log.debug('Refresh', `Fast refresh ${sport} failed: ${err.message}`);
      }
    }
  }, 150 * 1000);

  // Closing line capture for CLV analysis — runs every 60s, snapshots
  // Pinnacle + consensus fair probs for events whose commenceTime has just
  // crossed into the past. Idempotent.
  setInterval(() => {
    try {
      oddsFeed.captureClosingLines();
    } catch (err) {
      log.debug('CLV', `Closing line capture failed: ${err.message}`);
    }
  }, 60 * 1000);

  lineRefreshTimer = setInterval(async () => {
    try {
      await lineManager.refreshLines();
    } catch (err) {
      log.error('Refresh', `Line refresh failed: ${err.message}`);
    }
  }, refreshMs);

  // Poll PX for settlement updates and refresh balance
  settlementPollTimer = setInterval(async () => {
    try {
      await orderTracker.pollOrderSettlements(px);
    } catch (err) {
      log.error('Refresh', `Settlement poll failed: ${err.message}`);
    }
    try {
      const bal = await px.fetchBalance();
      const amount = bal?.balance ?? bal?.available ?? (typeof bal === 'number' ? bal : null);
      if (amount != null && amount > 0) {
        config.pricing.liveBankroll = amount;
        log.debug('Balance', `PX balance: $${amount}`);
      }
    } catch (err) {
      log.debug('Balance', `Balance fetch failed: ${err.message}`);
    }
  }, refreshMs);

  // Check game results every 30s for early win detection + fix bogus
  // settlements. checkLegResults already early-exits when there are no
  // in-progress legs to check, and the underlying TOA /scores fetch is
  // bounded by scoresCache TTL — so the cost during quiet windows is
  // ~zero. During live games this cuts dashboard staleness on completed
  // games from up to 4 min (2-min interval + 2-min cache) to under 1 min.
  setInterval(async () => {
    try {
      orderTracker.revertBogusSettlements();
      const resultDiag = await orderTracker.checkLegResults();
      orderTracker.reconcileSettlements();
      // Rebuild exposure so dead parlays (any leg lost → guaranteed-win
      // for SP, but PX hasn't settled yet) drop out of Team / Game
      // Exposure totals immediately. Without this, capacity stays
      // tied up by parlays we can't lose until PX settles them.
      try { orderTracker.rebuildAllExposure(); } catch (err) {
        log.debug('Results', `post-check rebuildAllExposure failed: ${err.message}`);
      }
    } catch (err) {
      log.debug('Results', `Result check failed: ${err.message}`);
    }
  }, 30 * 1000);

  // Settlement drift monitor — every 5 min scan PX settled orders and flag
  // any divergence from local state. Runs independently of the settlement
  // poll so it catches drift that might otherwise go silent. Details
  // available via GET /drift-status.
  setInterval(async () => {
    try {
      await orderTracker.checkSettlementDrift(px);
    } catch (err) {
      log.debug('Drift', `Drift check failed: ${err.message}`);
    }
  }, 5 * 60 * 1000);

  // Refresh DB P&L on startup and every 2 minutes
  refreshDbPnL();
  setInterval(refreshDbPnL, 2 * 60 * 1000);

  // Server-side live-odds refresh. The client dashboard fires a 60s
  // timer too, but if nobody has the dashboard open (mobile-only
  // viewing, page closed, overnight), nothing would refresh and
  // Risk Simulation / Exposure would silently fall back to pre-game
  // fair probs. Run unconditionally on the server at 60s cadence;
  // refreshLiveOdds itself cheaply early-exits when there are no
  // in-progress legs in confirmed parlays.
  setInterval(async () => {
    try {
      await orderTracker.refreshLiveOdds(oddsFeed);
    } catch (err) {
      log.debug('LiveOdds', `Refresh failed: ${err.message}`);
    }
  }, 60 * 1000);

  // ESPN live-scores poller. Provides primary score / completion data
  // across every sport we quote on, replacing TOA's /scores as the fast
  // path. Drives leg.inferredResult ('won'/'lost'/'push') in
  // order-tracker.checkLegResults via oddsFeed.getGameResult, which now
  // tries the ESPN cache first.
  try {
    const espnScores = require('./services/espn-scores');
    espnScores.startPoller();
  } catch (err) {
    log.warn('Startup', `ESPN scores poller failed to start (non-fatal): ${err.message}`);
  }

  // Pre-warm DK series prices (NBA + NHL). Puppeteer takes ~15s per
  // sport — too slow to run inline when an RFQ arrives — so fetch at
  // boot and refresh every 10 min. Pricer's getSeriesFairProb() reads
  // from this cache synchronously via dkScraper.lookupSeriesFairProb().
  (async () => {
    // Run NBA + NHL series pre-warm in parallel to halve the cold-cache
    // window at boot. Previously serial — when NBA's Puppeteer fetch ran
    // long, NHL series RFQs arriving early got a "no fair value" decline.
    await Promise.all(['nba', 'nhl'].map(sport =>
      dkScraper.fetchSeriesWinners(sport).catch(err => {
        log.warn('DkScraper', `Initial ${sport.toUpperCase()} fetch failed: ${err.message}`);
      })
    ));
    // Prime MMA cache + merge into oddsCache so line-manager picks up
    // UFC Fight Night fights The Odds API doesn't carry.
    try {
      await dkScraper.fetchMmaFightOdds();
      await oddsFeed.mergeDkMmaFights();
    } catch (err) {
      log.warn('DkScraper', `Initial MMA prime failed: ${err.message}`);
    }
    // Prime golf matchups cache. DataGolf covers individual 1v1 player
    // matchups; this DK path covers team matchups (Zurich Classic) and
    // any other PGA event DataGolf misses. A cold call is only ~10-15s
    // and between-tournament runs return an empty set harmlessly.
    try {
      await dkScraper.fetchGolfMatchups();
    } catch (err) {
      log.warn('DkScraper', `Initial golf matchups prime failed: ${err.message}`);
    }
    // Prime BetOnline Zurich matchups (temporary, this-week scraper).
    // DataGolf + DK don't cover PX's Zurich Classic team-matchup
    // pairings; BetOnline does. Fails harmlessly when not tournament
    // week (no data to scrape, empty matchups).
    //
    // Order matters: restore from Supabase KV FIRST so manually-uploaded
    // matchups survive Railway redeploys. Only then attempt the live
    // scrape — it usually fails (BetOnline blocks our data-center IP)
    // but the restored cache remains authoritative either way.
    try {
      const betonlineScraper = require('./services/betonline-scraper');
      await betonlineScraper.restoreFromPersistence();
      await betonlineScraper.fetchZurichMatchups();
    } catch (err) {
      log.warn('BetOnlineScraper', `Initial Zurich prime failed: ${err.message}`);
    }
  })();
  setInterval(async () => {
    await Promise.all(['nba', 'nhl'].map(sport =>
      dkScraper.fetchSeriesWinners(sport, { force: true }).catch(err => {
        log.warn('DkScraper', `Periodic ${sport.toUpperCase()} refresh failed: ${err.message}`);
      })
    ));
    try {
      await dkScraper.fetchMmaFightOdds({ force: true });
      await oddsFeed.mergeDkMmaFights();
    } catch (err) {
      log.warn('DkScraper', `Periodic MMA refresh failed: ${err.message}`);
    }
    try {
      await dkScraper.fetchGolfMatchups({ force: true });
    } catch (err) {
      log.warn('DkScraper', `Periodic golf matchups refresh failed: ${err.message}`);
    }
    try {
      const betonlineScraper = require('./services/betonline-scraper');
      await betonlineScraper.fetchZurichMatchups({ force: true });
    } catch (err) {
      log.warn('BetOnlineScraper', `Periodic Zurich refresh failed: ${err.message}`);
    }
  }, 10 * 60 * 1000);

  // Initial balance fetch — timeout: 10s
  try {
    const bal = await withTimeout(() => px.fetchBalance(), 10000, 'Balance fetch');
    const amount = bal?.balance ?? bal?.available ?? (typeof bal === 'number' ? bal : null);
    if (amount != null && amount > 0) {
      config.pricing.liveBankroll = amount;
      log.info('Startup', `    ✓ PX balance: $${amount}`);
    }
  } catch (err) {
    log.warn('Startup', `    ⚠ Balance fetch failed: ${err.message}`);
  }

  serviceReady = true;
  log.info('Startup', `=== Service ready! Refreshing every ${config.refreshIntervalMinutes}min ===`);
  console.log('');
}

// ---------------------------------------------------------------------------
// EXPRESS STATUS SERVER
// ---------------------------------------------------------------------------

function startStatusServer() {
  const app = express();
  app.use(express.json());

  // ---------------------------------------------------------------------
  // HTTP Basic Auth — gates the entire app behind a username/password.
  // Enabled only when AUTH_PASSWORD env var is set (back-compat: an
  // un-set password leaves the server publicly accessible just like
  // before, with a loud warning in logs so the operator knows). Works
  // for both the dashboard and the mobile PWA — modern browsers cache
  // credentials for the session after the first prompt. /health remains
  // public so Railway's deployment health probe keeps working.
  //
  // Two roles:
  //   admin:   AUTH_USERNAME / AUTH_PASSWORD — full dashboard access
  //   viewer:  AUTH_VIEWERS=user1:pass1,user2:pass2 — restricted to
  //            AUTH_VIEWER_PATHS only (default: /edge-vs-fair.html).
  //            Lets the operator share a specific report page without
  //            exposing orders, market intel, admin endpoints, etc.
  const AUTH_USERNAME = process.env.AUTH_USERNAME || 'mike';
  const AUTH_PASSWORD = process.env.AUTH_PASSWORD || '';
  const AUTH_ENABLED = AUTH_PASSWORD.length > 0;
  const AUTH_PUBLIC_PATHS = new Set(['/health']);
  // Viewers: comma-separated user:pass list. A user with a colon in
  // its name would break parsing; document this is intentional and
  // disallow such names below.
  const AUTH_VIEWERS = (() => {
    const raw = process.env.AUTH_VIEWERS || '';
    const map = new Map();
    for (const pair of raw.split(',').map(s => s.trim()).filter(Boolean)) {
      const idx = pair.indexOf(':');
      if (idx <= 0 || idx === pair.length - 1) {
        log.warn('Auth', `Skipping malformed AUTH_VIEWERS entry: "${pair}" (expected "user:pass")`);
        continue;
      }
      const user = pair.slice(0, idx);
      const pass = pair.slice(idx + 1);
      if (user === AUTH_USERNAME) {
        log.warn('Auth', `Skipping viewer entry that collides with AUTH_USERNAME: "${user}"`);
        continue;
      }
      map.set(user, pass);
    }
    return map;
  })();
  // Paths a viewer is permitted to load. Default covers the shareable
  // report and the static asset paths it implicitly resolves to. /health
  // is already public via AUTH_PUBLIC_PATHS so viewers don't need it.
  const AUTH_VIEWER_PATHS = new Set(
    (process.env.AUTH_VIEWER_PATHS || '/edge-vs-fair.html')
      .split(',').map(s => s.trim()).filter(Boolean)
  );
  if (AUTH_ENABLED) {
    log.info('Auth', `HTTP Basic Auth enabled (admin: "${AUTH_USERNAME}", viewers: ${AUTH_VIEWERS.size}, viewer paths: ${Array.from(AUTH_VIEWER_PATHS).join(', ')}, public paths: ${Array.from(AUTH_PUBLIC_PATHS).join(', ')})`);
  } else {
    log.warn('Auth', 'AUTH_PASSWORD env var not set — server is PUBLICLY ACCESSIBLE. Set AUTH_PASSWORD on Railway to lock down access.');
  }
  // Constant-time-ish string compare. Length-mismatch short-circuits
  // before any byte compare; otherwise XORs all bytes so the running
  // time depends on length only.
  function safeEqual(a, b) {
    if (a.length !== b.length) return false;
    let diff = 0;
    for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
    return diff === 0;
  }
  app.use((req, res, next) => {
    if (!AUTH_ENABLED) return next();
    if (AUTH_PUBLIC_PATHS.has(req.path)) return next();
    const header = req.headers.authorization || '';
    if (header.startsWith('Basic ')) {
      try {
        const decoded = Buffer.from(header.slice(6), 'base64').toString('utf8');
        const idx = decoded.indexOf(':');
        if (idx > -1) {
          const user = decoded.slice(0, idx);
          const pass = decoded.slice(idx + 1);
          // Admin: full access to every non-public path.
          if (user === AUTH_USERNAME && safeEqual(pass, AUTH_PASSWORD)) {
            return next();
          }
          // Viewer: only the allowlisted paths.
          if (AUTH_VIEWERS.has(user)) {
            const expected = AUTH_VIEWERS.get(user);
            if (safeEqual(pass, expected)) {
              if (AUTH_VIEWER_PATHS.has(req.path)) return next();
              // Authenticated as viewer but path is admin-only.
              return res.status(403).send('Forbidden');
            }
          }
        }
      } catch (_) { /* fall through to 401 */ }
    }
    res.set('WWW-Authenticate', 'Basic realm="ProphetX SP", charset="UTF-8"');
    return res.status(401).send('Authentication required');
  });

  // No cache for HTML so deploys are picked up immediately
  app.use(express.static(path.join(__dirname, 'client'), {
    setHeaders: (res, filePath) => {
      if (filePath.endsWith('.html')) {
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      }
    }
  }));

  // Health check — always returns 200 so Railway deployment succeeds
  app.get('/health', (req, res) => {
    const ws = websocket.getState();
    res.json({
      ok: true,
      ready: serviceReady,
      uptime: Math.round((Date.now() - startTime) / 1000),
      wsState: ws.connectionState,
      paused: ws.paused,
      lineCount: lineManager.getLineCount(),
      oddsCacheStatus: oddsFeed.getCacheStatus(),
    });
  });

  // Full status dashboard
  app.get('/status', (req, res) => {
    res.json({
      service: {
        ready: serviceReady,
        uptime: Math.round((Date.now() - startTime) / 1000),
        startedAt: new Date(startTime).toISOString(),
      },
      config: {
        vig: config.pricing.defaultVig,
        vigBySport: config.pricing.vigBySport || {},
        maxRisk: config.pricing.maxRiskPerParlay,
        maxLegs: config.pricing.maxLegs,
        maxExposurePerTeam: config.pricing.maxExposurePerTeam,
        stalePriceMinutes: config.pricing.stalePriceMinutes,
        pendingReservationDiscount: config.pricing.pendingReservationDiscount,
        parlayLevelVig: !!config.pricing.parlayLevelVig,
        sports: config.supportedSports,
        baseUrl: config.px.baseUrl,
        // Phase-2 prop launch knobs
        propLaunchAllowlist: [...(config.pricing.propLaunchAllowlist || new Set())],
        maxRiskPerParlayWithProp: config.pricing.maxRiskPerParlayWithProp,
        propMinBooksWithBothSides: config.pricing.propMinBooksWithBothSides,
        maxExposurePerPlayerBySport: config.pricing.maxExposurePerPlayerBySport || {},
        maxExposurePerPlayerDefault: config.pricing.maxExposurePerPlayerDefault,
      },
      websocket: websocket.getState(),
      lines: {
        registered: lineManager.getLineCount(),
        bySportAndMarket: lineManager.getLineSummary(),
        lastSeed: lineManager.getStats(),
      },
      odds: oddsFeed.getCacheStatus(),
      orders: { ...orderTracker.getStats(), dbPnL: cachedDbPnL },
      exposure: {
        maxPerTeam: config.pricing.maxExposurePerTeam,
        maxPerGame: config.pricing.maxExposurePerGame,
        teams: orderTracker.getExposureSnapshot(),
        games: orderTracker.getGameExposureSnapshot(),
        // Phase-2 prop launch concentration. Empty until prop legs
        // start landing (post-bridge). Cap config surfaced for visibility.
        playersByPropExposure: orderTracker.getPlayerExposureSnapshot(),
        maxPerPlayerBySport: config.pricing.maxExposurePerPlayerBySport || {},
        maxPerPlayerDefault: config.pricing.maxExposurePerPlayerDefault,
      },
      portfolio: (() => {
        // Account-based P&L is the SOURCE OF TRUTH for the dashboard.
        // PX's /balance endpoint returns the TOTAL account balance —
        // matched_wager_balance and unmatched_wager_balance both return
        // 0 in the /balance payload, confirming PX lumps matched stakes
        // into the top-level `balance` field rather than reporting them
        // as a separate locked bucket. So:
        //   accountValue  == liveBalance   (NOT liveBalance + currentRisk)
        //   accountPnL    == liveBalance - startingBankroll
        // The previous formula added currentRisk back on top, which
        // double-counted the matched stakes and produced a misleading
        // P&L (operator reported +$17,067 when real P&L was ~+$2,731).
        const liveBal = config.pricing.liveBankroll;
        const currentRisk = orderTracker.getTotalPortfolioRisk();
        const startingBankroll = config.pricing.startingBankroll;
        const accountValue = (liveBal && liveBal > 0) ? liveBal : null;
        // Only compute account-based P&L when startingBankroll was
        // explicitly set (env var present). Otherwise leave null so
        // the dashboard falls back to the tracker's runningPnL —
        // avoids the sandbox-era $20K default silently anchoring
        // production P&L to the wrong baseline.
        const accountPnL = (accountValue != null && startingBankroll != null)
          ? (accountValue - startingBankroll)
          : null;
        return {
          bankroll: getBankroll(),
          balance: liveBal || getBankroll(),
          accountValue,
          startingBankroll,
          accountPnL,
          totalRisk: orderTracker.getTotalPortfolioRisk(),
          currentRisk: orderTracker.getTotalPortfolioRisk(),
          totalToWin: orderTracker.getTotalToWin(),
          maxRiskPerParlay: config.pricing.maxRiskPerParlay,
        };
      })(),
      alerts: orderTracker.getAlerts(),
      exposureLimits: orderTracker.getExposureLimitStats(),
    });
  });

  // Balance
  app.get('/balance', async (req, res) => {
    try {
      const balance = await px.fetchBalance();
      res.json({ ok: true, balance });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Recent orders
  app.get('/order/:id', (req, res) => {
    const id = req.params.id;
    const order = orderTracker.findByParlayId(id) || orderTracker.findByOrderUuid(id);
    if (!order) return res.status(404).json({ error: 'Order not found', id });
    res.json(order);
  });

  // Decline stats from Supabase — grouped by reason
  app.get('/recent-rejects', (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    res.json({ rejects: orderTracker.getRecentRejects(limit) });
  });

  app.get('/decline-stats', async (req, res) => {
    try {
      const days = parseInt(req.query.days) || 7;
      const reasonFilter = req.query.reason || null;
      const client = db.getClient();
      if (!client) return res.json({ error: 'No Supabase client' });

      // Count-only queries (head:true) — no row transfer, much faster.
      // Process one day at a time to avoid overwhelming Supabase connection pool.
      const KNOWN_REASONS = [
        'unknown legs', 'portfolio drawdown limit', 'stale odds',
        'correlated legs', 'no fair value', 'duplicate parlay',
        'odds too high', 'NBA heavy favorite', 'max exposure per game',
        'max risk exceeded', 'too many legs', 'event started',
      ];
      const reasons = reasonFilter ? [reasonFilter] : KNOWN_REASONS;

      // Build all (day × reason) count queries and run them ALL in parallel.
      // With indexes on declined_at and reason, each count is fast (<50ms).
      const allQueries = [];
      for (let d = 0; d < days; d++) {
        const dayStart = new Date(Date.now() - (d + 1) * 24 * 3600 * 1000).toISOString();
        const dayEnd = new Date(Date.now() - d * 24 * 3600 * 1000).toISOString();
        const dayLabel = new Date(Date.now() - d * 24 * 3600 * 1000).toLocaleDateString('en-CA');
        for (const reason of reasons) {
          allQueries.push({ dayLabel, reason, exec: client.from('declines')
            .select('*', { count: 'exact', head: true })
            .gte('declined_at', dayStart)
            .lt('declined_at', dayEnd)
            .eq('reason', reason)
          });
        }
      }

      const results = await Promise.all(allQueries.map(async q => {
        const { count, error } = await q.exec;
        if (error) {
          log.warn('DeclineStats', `Count failed ${q.reason} ${q.dayLabel}: ${error.message || JSON.stringify(error)}`);
          return { ...q, count: 0 };
        }
        return { ...q, count: count || 0 };
      }));

      const byReason = {};
      const byReasonByDay = {};
      let total = 0;
      for (const { dayLabel, reason, count } of results) {
        if (count === 0) continue;
        byReason[reason] = (byReason[reason] || 0) + count;
        if (!byReasonByDay[reason]) byReasonByDay[reason] = {};
        byReasonByDay[reason][dayLabel] = count;
        total += count;
      }

      const sorted = Object.entries(byReason).sort((a, b) => b[1] - a[1]);
      res.json({ days, total, byReason: Object.fromEntries(sorted), byReasonByDay });
    } catch (err) {
      res.status(500).json({ error: err.message || JSON.stringify(err) });
    }
  });

  // Phase 0 instrumentation for the player-prop opportunity sizing.
  // Returns a breakdown of what % of player_prop unknown legs are
  // pitcher_strikeouts vs other MLB prop types — the gating metric for
  // whether to subscribe to a paid prop feed.
  //
  // Two data sources, each with caveats:
  //   - in-memory (declineStats.unknownLegCategories): exact propType
  //     bucket counts but resets on service restart
  //   - Supabase declines table: persists across restarts; we substring-
  //     count the [propType:X] tags we now embed in unknown_details
  //     strings. Coarser but durable.
  //
  // Operator polls this every few hours over a 24-48h window. Phase 1
  // proceeds only if pitcher_strikeouts ≥10% of total player_prop volume.
  // TTL cache for /prop-opportunity. The Supabase scan is the slow
  // part — 250k rows × 1000-row pagination = ~250 sequential round-
  // trips to Supabase, ~30s for a 1d window. Dashboard refreshes hit
  // this endpoint repeatedly; without a cache every refresh pays the
  // full scan cost. 60s TTL is fine — the data is a rolling window of
  // declines, sub-minute freshness isn't material.
  //
  // Cache key includes `days` so different windows don't share entries.
  // `nocache=1` query param bypasses (for the rare case operator wants
  // a forced fresh read).
  const propOppCache = new Map(); // days -> { ts, payload }
  const PROP_OPP_TTL_MS = 60_000;
  app.get('/prop-opportunity', async (req, res) => {
    try {
      const days = Math.min(7, Math.max(1, parseInt(req.query.days) || 2));
      const noCache = req.query.nocache === '1';
      if (!noCache) {
        const hit = propOppCache.get(days);
        if (hit && (Date.now() - hit.ts) < PROP_OPP_TTL_MS) {
          return res.json({
            ...hit.payload,
            cache: { hit: true, ageMs: Date.now() - hit.ts, ttlMs: PROP_OPP_TTL_MS },
          });
        }
      }

      // ---- In-memory snapshot ----
      const memBucket = (orderTracker.getDeclineStatsSnapshot
        ? orderTracker.getDeclineStatsSnapshot()
        : { unknownLegCategories: {} });
      const playerPropBucket = (memBucket.unknownLegCategories || {}).player_prop || { count: 0, byPropType: {}, bySport: {}, sampleLegs: [] };
      const memPropTypes = { ...(playerPropBucket.byPropType || {}) };
      delete memPropTypes._lastSeen;
      // The classifier now runs for BOTH baseball (classifyMlbProp) and
      // basketball (classifyNbaProp). The byPropType rollup mixes both
      // sports' bucket names, so compute MLB- and NBA-only sub-totals
      // by enumerated bucket name. Anything we don't recognize is
      // reported under `unclassifiedBuckets` so a new bucket added to
      // either classifier later can't silently inflate the wrong total.
      const MLB_BUCKETS = new Set([
        'pitcher_strikeouts', 'pitcher_other',
        'hitter_strikeouts', 'hitter_total_bases', 'hitter_hr',
        'hitter_rbi_runs', 'hitter_hits', 'hitter_other',
        'mlb_prop_ambiguous', 'other_mlb_prop',
      ]);
      const NBA_BUCKETS = new Set([
        'points', 'rebounds', 'assists', 'threes_made', 'blocks',
        'steals', 'steals_blocks', 'pra_combo', 'double_double',
        'triple_double', 'turnovers', 'first_basket',
        'nba_prop_ambiguous', 'other_nba_prop',
      ]);
      const sumByBuckets = (counts, allowed) => {
        let total = 0;
        for (const [k, v] of Object.entries(counts)) {
          if (allowed.has(k)) total += v;
        }
        return total;
      };
      const unclassifiedBuckets = (counts) => {
        const out = {};
        for (const [k, v] of Object.entries(counts)) {
          if (!MLB_BUCKETS.has(k) && !NBA_BUCKETS.has(k)) out[k] = v;
        }
        return out;
      };
      const memMlbClassified = sumByBuckets(memPropTypes, MLB_BUCKETS);
      const memNbaClassified = sumByBuckets(memPropTypes, NBA_BUCKETS);
      const memUnclassified = unclassifiedBuckets(memPropTypes);
      const memAllSports = playerPropBucket.count || 0;
      const memPctPitcherKOfMlb = memMlbClassified > 0
        ? Math.round((memPropTypes.pitcher_strikeouts || 0) / memMlbClassified * 10000) / 100
        : null;
      // bySport breakdown so the operator can see how MLB prop volume
      // compares to NBA/NHL/etc. — useful for prioritizing future
      // expansion beyond just pitcher strikeouts.
      const memBySport = { ...(playerPropBucket.bySport || {}) };

      // ---- DB-backed: substring-count [propType:X] in unknown_details ----
      // Paginate via .range() so we get past Supabase's server-side
      // 1000-row default cap. Without pagination the .limit(50000)
      // request was silently truncated to 1000, sampling only ~1% of
      // a typical 2-day window (~93k declines) — the response showed
      // declineRowsScanned: 1000 even when far more existed.
      // Mirrors the loadOrdersInDateRange paging pattern in db.js.
      let dbBreakdown = null;
      try {
        const client = db.getClient();
        if (client) {
          const since = new Date(Date.now() - days * 24 * 3600 * 1000).toISOString();
          const PAGE_SIZE = 1000;
          const MAX_ROWS = 250000; // hard ceiling for runaway windows
          const re = /\[propType:([a-z_]+)\]/g;
          const counts = {};
          let totalScanned = 0;
          let offset = 0;
          let pages = 0;
          let lastError = null;
          while (totalScanned < MAX_ROWS) {
            const { data, error } = await client
              .from('declines')
              .select('unknown_details')
              .eq('reason', 'unknown legs')
              .gte('declined_at', since)
              .order('declined_at', { ascending: false })
              .range(offset, offset + PAGE_SIZE - 1);
            if (error) { lastError = error; break; }
            if (!Array.isArray(data) || data.length === 0) break;
            for (const row of data) {
              const arr = row.unknown_details || [];
              for (const s of arr) {
                if (typeof s !== 'string') continue;
                let m;
                while ((m = re.exec(s)) !== null) {
                  counts[m[1]] = (counts[m[1]] || 0) + 1;
                }
              }
            }
            totalScanned += data.length;
            pages++;
            if (data.length < PAGE_SIZE) break; // last page
            offset += PAGE_SIZE;
          }
          const total = Object.values(counts).reduce((a, b) => a + b, 0);
          const dbMlb = sumByBuckets(counts, MLB_BUCKETS);
          const dbNba = sumByBuckets(counts, NBA_BUCKETS);
          const dbUnclassified = unclassifiedBuckets(counts);
          dbBreakdown = {
            windowDays: days,
            declineRowsScanned: totalScanned,
            pagesFetched: pages,
            hitMaxRowsCap: totalScanned >= MAX_ROWS,
            propLegsTagged: total,
            mlbPropLegsClassified: dbMlb,
            nbaPropLegsClassified: dbNba,
            byPropType: counts,
            unclassifiedBuckets: dbUnclassified,
            // Denominator is MLB-only buckets — Phase 2 gate decision
            // depends on pitcher-K share of MLB-classified prop volume,
            // not of all-sports prop volume.
            pctPitcherStrikeoutsOfMlb: dbMlb > 0
              ? Math.round((counts.pitcher_strikeouts || 0) / dbMlb * 10000) / 100
              : null,
            ...(lastError ? { warning: `pagination aborted at offset ${offset}: ${lastError.message}` } : {}),
          };
        }
      } catch (err) {
        log.warn('PropOpportunity', `DB scan failed: ${err.message}`);
      }

      const payload = {
        ok: true,
        generatedAt: new Date().toISOString(),
        gatingThreshold: '≥10% pitcher_strikeouts of MLB-classified player_prop volume → proceed to Phase 1',
        inMemorySinceBoot: {
          // True total across all sports (NBA + NHL + MLB + ...)
          totalPlayerPropLegsAllSports: memAllSports,
          // Breakdown of where the player_prop volume lives by sport.
          // Useful for sizing future non-MLB expansions.
          bySport: memBySport,
          // Sub-totals by classifier sport. The byPropType rollup mixes
          // MLB and NBA bucket names; these scalars partition them so
          // pctPitcherStrikeoutsOfMlb has the correct denominator.
          mlbPropLegsClassified: memMlbClassified,
          nbaPropLegsClassified: memNbaClassified,
          byPropType: memPropTypes,
          // Any byPropType keys we didn't recognize as MLB or NBA
          // buckets (should always be empty; surfaces classifier drift
          // if a new bucket name is added without updating MLB_BUCKETS
          // / NBA_BUCKETS here).
          unclassifiedBuckets: memUnclassified,
          // Denominator is mlbPropLegsClassified (NOT the all-sports
          // total, NOT the all-classified total) — Phase 2 gate is
          // pitcher-K share of MLB-classified prop volume.
          pctPitcherStrikeoutsOfMlb: memPctPitcherKOfMlb,
          sampleLegs: (playerPropBucket.sampleLegs || []).slice(0, 10),
        },
        persistedWindow: dbBreakdown || { error: 'DB unavailable or no data' },
      };
      propOppCache.set(days, { ts: Date.now(), payload });
      res.json({ ...payload, cache: { hit: false, ttlMs: PROP_OPP_TTL_MS } });
    } catch (err) {
      log.error('API', `/prop-opportunity failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Live feed of recently-declined player-prop legs. Each entry is ONE
  // leg (a single parlay can produce multiple entries). Used by the
  // Player Prop Flow dashboard card so the operator can eyeball what
  // prop RFQs PX is sending us — even though we're not quoting them
  // yet — to spot patterns, naming surprises, and validate the
  // classifier on real flow.
  //
  // Query params:
  //   sport=baseball_mlb | basketball_nba | ...    (default: all)
  //   propType=pitcher_strikeouts | hitter_hits | ... (default: all)
  //   limit=N (default 200, max 2000)
  //   sinceMinutes=N (default unset = full in-memory window, ~5000 events)
  app.get('/prop-flow', (req, res) => {
    try {
      const opts = {
        sport: req.query.sport || null,
        propType: req.query.propType || null,
        limit: req.query.limit ? parseInt(req.query.limit) : 200,
      };
      if (req.query.sinceMinutes) {
        opts.sinceMs = Date.now() - parseInt(req.query.sinceMinutes) * 60 * 1000;
      }
      const flow = orderTracker.getRecentPropFlow
        ? orderTracker.getRecentPropFlow(opts)
        : [];
      // Counts by sport / propType for quick header-line context
      const bySport = {};
      const byPropType = {};
      for (const e of flow) {
        if (e.sport) bySport[e.sport] = (bySport[e.sport] || 0) + 1;
        const pt = e.propType || '(unclassified)';
        byPropType[pt] = (byPropType[pt] || 0) + 1;
      }
      res.json({
        ok: true,
        count: flow.length,
        filter: opts,
        bySport,
        byPropType,
        flow,
        note: 'In-memory rolling log capped at ~5000 decline events; restart-volatile. Use /prop-opportunity for restart-resilient aggregates.',
      });
    } catch (err) {
      log.error('API', `/prop-flow failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // One-off probe to measure The Odds API's prop coverage for a sport.
  // Hits TOA's events endpoint (free) to see what games it knows about,
  // then fetches per-event odds for the specified markets to count
  // outcomes and books per game. Used to evaluate whether a prop type
  // is viable for quoting before wiring it into the lookup pipeline.
  //
  // Cost: 1 events call (free) + N event×market credits (e.g. NBA with
  // 10 games × 4 markets = 40 credits). Cap with the maxEvents param.
  //
  // Query params:
  //   sport      (default basketball_nba) — TOA sport key
  //   markets    (default player_points,player_rebounds,player_assists,player_threes)
  //   maxEvents  (default 3) — cap to limit credit burn
  // Trace the Phase-2 prop bridge end-to-end for a specific player + market.
  // Mirrors what services/line-manager.js calls inside resolveUnknownLine —
  // useful when /decline-events shows player_prop legs failing with no
  // visible reason. Returns the full stages + error/result from
  // lookupTheOddsApiPlayerProp so the operator can see exactly which gate
  // (event match, player match, line match, alt-distance, books-with-both)
  // is dropping the leg.
  //
  // Usage:
  //   /debug/prop-bridge?sport=basketball_nba&market=player_points&player=Cade%20Cunningham&line=24.5&home=Orlando%20Magic&away=Detroit%20Pistons
  app.get('/debug/prop-bridge', async (req, res) => {
    try {
      const sport = String(req.query.sport || '').trim();
      const market = String(req.query.market || '').trim();
      const player = String(req.query.player || '').trim();
      const home = String(req.query.home || '').trim();
      const away = String(req.query.away || '').trim();
      const line = req.query.line != null ? parseFloat(req.query.line) : null;
      const startTime = req.query.startTime || null;
      if (!sport || !market || !player || !home || !away) {
        return res.status(400).json({
          ok: false,
          error: 'required: sport, market, player, home, away (line + startTime optional)',
          example: '/debug/prop-bridge?sport=basketball_nba&market=player_points&player=Cade%20Cunningham&line=24.5&home=Orlando%20Magic&away=Detroit%20Pistons',
        });
      }
      const eventCtx = { homeTeam: home, awayTeam: away, startTime };
      const result = await oddsFeed.lookupTheOddsApiPlayerProp(sport, market, eventCtx, player, line);
      // Also report config thresholds the bridge applies post-lookup
      const minBooks = (config.pricing && config.pricing.propMinBooksWithBothSides) || 3;
      const maxDist = (config.pricing && config.pricing.propAltLineMaxDistance);
      const usable = result
        && result.fairProbOver != null
        && result.fairProbUnder != null
        && (result.booksWithBothSides || 0) >= minBooks;
      res.json({
        ok: true,
        input: { sport, market, player, line, home, away, startTime },
        result,
        bridgeGate: {
          propMinBooksWithBothSides: minBooks,
          propAltLineMaxDistance: maxDist,
          usable,
          wouldRegister: usable,
          declineReason: !result ? 'lookup_null'
            : result.error ? result.error
            : (result.booksWithBothSides || 0) < minBooks ? `insufficient_books(${result.booksWithBothSides || 0}<${minBooks})`
            : (result.fairProbOver == null || result.fairProbUnder == null) ? 'no_fair_prob'
            : null,
        },
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack?.split('\n').slice(0, 5) });
    }
  });

  app.get('/probe-toa-prop-coverage', async (req, res) => {
    try {
      const apiKey = process.env.THE_ODDS_API_KEY;
      if (!apiKey) return res.status(500).json({ ok: false, error: 'THE_ODDS_API_KEY not set' });
      const sport = String(req.query.sport || 'basketball_nba').trim();
      const marketsParam = String(req.query.markets ||
        'player_points,player_rebounds,player_assists,player_threes').trim();
      const markets = marketsParam.split(',').map(s => s.trim()).filter(Boolean);
      const maxEvents = Math.max(1, Math.min(20, parseInt(req.query.maxEvents) || 3));

      const fetch = require('node-fetch');
      const evtUrl = `https://api.the-odds-api.com/v4/sports/${sport}/events?apiKey=${apiKey}`;
      const evtResp = await fetch(evtUrl);
      if (!evtResp.ok) {
        return res.status(502).json({ ok: false, error: `TOA events ${evtResp.status}`, body: await evtResp.text() });
      }
      const events = await evtResp.json();
      if (!Array.isArray(events)) return res.json({ ok: true, sport, eventsCount: 0, events: [] });

      const probed = events.slice(0, maxEvents);
      const probeResults = [];
      for (const ev of probed) {
        const url = `https://api.the-odds-api.com/v4/sports/${sport}/events/${ev.id}/odds`
          + `?apiKey=${apiKey}&regions=us,eu&markets=${markets.join(',')}&oddsFormat=american`;
        let odds = null, status = 0, errBody = null;
        try {
          const r = await fetch(url);
          status = r.status;
          if (r.ok) odds = await r.json();
          else errBody = await r.text();
        } catch (e) {
          errBody = String(e.message);
        }
        const perMarket = {};
        const allBooks = new Set();
        for (const m of markets) perMarket[m] = { books: new Set(), outcomes: 0, samplePlayers: new Set(), sampleLines: new Set() };
        for (const bk of (odds?.bookmakers || [])) {
          allBooks.add(bk.key);
          for (const m of (bk.markets || [])) {
            if (!perMarket[m.key]) continue;
            perMarket[m.key].books.add(bk.key);
            for (const o of (m.outcomes || [])) {
              perMarket[m.key].outcomes++;
              if (o.description && perMarket[m.key].samplePlayers.size < 6) perMarket[m.key].samplePlayers.add(o.description);
              if (o.point != null && perMarket[m.key].sampleLines.size < 8) perMarket[m.key].sampleLines.add(o.point);
            }
          }
        }
        probeResults.push({
          eventId: ev.id,
          matchup: `${ev.away_team} @ ${ev.home_team}`,
          commenceTime: ev.commence_time,
          httpStatus: status,
          errBody: errBody && errBody.slice(0, 240),
          totalBooks: allBooks.size,
          perMarket: Object.fromEntries(Object.entries(perMarket).map(([k, v]) => [k, {
            books: [...v.books],
            bookCount: v.books.size,
            outcomes: v.outcomes,
            samplePlayers: [...v.samplePlayers],
            sampleLines: [...v.sampleLines].sort((a, b) => a - b),
          }])),
        });
      }

      // Summary: avg books per market across probed events
      const marketSummary = {};
      for (const m of markets) {
        const eventsWithMarket = probeResults.filter(p => p.perMarket[m].bookCount > 0);
        const totalBooks = probeResults.reduce((s, p) => s + p.perMarket[m].bookCount, 0);
        marketSummary[m] = {
          eventsWithCoverage: eventsWithMarket.length,
          eventsProbed: probeResults.length,
          coveragePct: probeResults.length ? Math.round((eventsWithMarket.length / probeResults.length) * 100) : 0,
          avgBooksPerEvent: probeResults.length ? Math.round((totalBooks / probeResults.length) * 10) / 10 : 0,
          allBooksSeen: [...new Set(probeResults.flatMap(p => p.perMarket[m].books))],
        };
      }

      res.json({
        ok: true,
        generatedAt: new Date().toISOString(),
        sport,
        markets,
        eventsAvailable: events.length,
        eventsProbed: probeResults.length,
        marketSummary,
        probeResults,
      });
    } catch (err) {
      log.error('API', `/probe-toa-prop-coverage failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Phase-1 prop shadow inspector. Pulls rows we logged into
  // prop_shadow_quotes for a given propType (e.g. 'points',
  // 'pitcher_strikeouts') and reports counts + a few sample rows so
  // we can verify the matching pipeline is working before flipping a
  // prop type to live quoting.
  //
  // Query params:
  //   propType (required) — prop_type column to filter on
  //   days     (default 7) — lookback window
  //   sample   (default 25) — how many newest rows to include verbatim
  app.get('/prop-shadow', async (req, res) => {
    try {
      const propType = String(req.query.propType || '').trim();
      if (!propType) return res.status(400).json({ ok: false, error: 'propType query param required' });
      const days = Math.max(1, Math.min(30, parseInt(req.query.days) || 7));
      const sample = Math.max(1, Math.min(200, parseInt(req.query.sample) || 25));
      const fromIso = new Date(Date.now() - days * 86400000).toISOString();
      const rows = await db.loadPropShadowQuotes({ propType, fromIso, limit: 5000 });
      let matched = 0, unmatched = 0;
      const errorsByType = {};
      const bySource = {};
      const playerCounts = {};
      for (const r of rows) {
        const ok = r.fair_prob_over != null && r.fair_prob_under != null;
        if (ok) matched++; else unmatched++;
        if (r.match_error) errorsByType[r.match_error] = (errorsByType[r.match_error] || 0) + 1;
        if (r.source) bySource[r.source] = (bySource[r.source] || 0) + 1;
        if (r.player_name) playerCounts[r.player_name] = (playerCounts[r.player_name] || 0) + 1;
      }
      const topPlayers = Object.entries(playerCounts)
        .sort((a, b) => b[1] - a[1]).slice(0, 10)
        .map(([name, n]) => ({ name, n }));
      res.json({
        ok: true,
        generatedAt: new Date().toISOString(),
        propType, windowDays: days,
        totalRows: rows.length,
        matched, unmatched,
        matchRate: rows.length ? Math.round((matched / rows.length) * 1000) / 10 : null,
        errorsByType, bySource, topPlayers,
        sample: rows.slice(0, sample).map(r => ({
          recordedAt: r.recorded_at,
          parlayId: r.parlay_id,
          pxEventId: r.px_event_id,
          marketName: r.market_name,
          playerName: r.player_name,
          line: r.line,
          source: r.source,
          fairProbOver: r.fair_prob_over,
          fairProbUnder: r.fair_prob_under,
          booksWithBothSides: r.books_with_both_sides,
          books: r.books,
          matchError: r.match_error,
          matchStages: r.match_stages,
        })),
      });
    } catch (err) {
      log.error('API', `/prop-shadow failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Phase 2 K-prop performance dashboard data. One-stop view of:
  //   - Funnel (RFQs → declines by reason → quotes → fills → settled)
  //   - Competitiveness (avg pp distance from fair, won/lost bid breakdown)
  //   - Modeled EV per dollar wagered on settled K-prop fills
  //   - Live per-pitcher exposure
  //
  // Query params:
  //   days (default 7) — window for orders + declines lookback
  app.get('/prop-performance', async (req, res) => {
    try {
      const days = Math.max(1, Math.min(30, parseInt(req.query.days) || 7));
      const fromIso = new Date(Date.now() - days * 86400000).toISOString();
      const toIso = new Date().toISOString();

      // K-prop heuristic on a parlay row: any leg has marketType / market
      // === 'player_strikeouts'. Uses raw-DB-shape (snake_case) since
      // loadOrdersInDateRange returns Supabase rows directly.
      const isKPropOrder = (o) => {
        const legs = o.legs || (o.meta && o.meta.legs) || [];
        return Array.isArray(legs) && legs.some(l =>
          l && (l.marketType === 'player_strikeouts' || l.market === 'player_strikeouts')
        );
      };

      // K-prop heuristic on a decline row: either the reason is a
      // prop-specific reason we now emit, OR the unknown_details array
      // contains a [propType:pitcher_strikeouts] tag (Phase 0 marker).
      const PROP_REASONS = new Set([
        'prop_pricing_not_ready', 'prop_no_fair_value', 'prop_low_confidence',
        'prop_stale', 'prop_correlation_same_game', 'prop_correlation_same_pitcher',
        'pitcher_exposure_cap',
      ]);
      // Near-miss subset: pricing/data issues we WANTED to quote but
      // couldn't. Mirrors order-tracker.js:1512 nearMissReasons for K
      // props. Excludes intentional blocks (correlation, exposure caps)
      // because those aren't "near misses" in any meaningful sense —
      // we never wanted to quote them.
      const K_PROP_NEAR_MISS_REASONS = new Set([
        'prop_no_fair_value', 'prop_low_confidence', 'prop_stale',
      ]);
      const isKPropDecline = (d) => {
        if (PROP_REASONS.has(d.reason)) return true;
        const details = d.unknown_details || [];
        return Array.isArray(details) && details.some(s =>
          typeof s === 'string' && /\[propType:pitcher_strikeouts\]/.test(s)
        );
      };

      // ---- Pull data ----
      // Server-side narrow to K-prop orders via the new legMarketEquals
      // filter — avoids loading 25,000+ rows just to client-side-filter
      // down to ~150 K-prop parlays. Previously timed out the endpoint
      // at 60s+ on busy days; now resolves in 1-2s.
      const kOrders = await db.loadOrdersInDateRange(fromIso, toIso, {
        legMarketEquals: 'player_strikeouts',
      });
      // Declines path: still pulls the full window since the K-prop
      // signal lives in mixed fields (reason or unknown_details) and
      // there's no clean server-side JSONB filter for both. Bounded by
      // fromIso to avoid the historical timeout. Most decline reasons
      // are short strings; 7d × ~471 K-prop declines/day plus other
      // reasons stays under the 20k cap.
      const recentDeclines = await db.loadDeclines(20000, { fromIso });
      const kDeclines = recentDeclines.filter(isKPropDecline);

      // ---- Funnel ----
      const declinesByReason = {};
      const nearMissesByReason = {};
      const recentNearMisses = [];
      for (const d of kDeclines) {
        declinesByReason[d.reason] = (declinesByReason[d.reason] || 0) + 1;
        if (K_PROP_NEAR_MISS_REASONS.has(d.reason)) {
          nearMissesByReason[d.reason] = (nearMissesByReason[d.reason] || 0) + 1;
          if (recentNearMisses.length < 20) {
            recentNearMisses.push({
              parlayId: d.parlayId,
              reason: d.reason,
              detail: d.detail,
              declinedAt: d.declinedAt,
              knownLegs: d.knownLegs || [],
            });
          }
        }
      }
      // Sort recent near misses by time descending
      recentNearMisses.sort((a, b) => (b.declinedAt || '').localeCompare(a.declinedAt || ''));

      const byStatus = {};
      for (const o of kOrders) {
        const s = o.status || 'unknown';
        byStatus[s] = (byStatus[s] || 0) + 1;
      }
      const isBidWon = (o) => o.status === 'confirmed'
        || (typeof o.status === 'string' && o.status.startsWith('settled_'));
      const isFill = (o) => o.confirmed_stake != null && Number(o.confirmed_stake) > 0
        && (o.status === 'confirmed' || (typeof o.status === 'string' && o.status.startsWith('settled_')));
      const settledOrders = kOrders.filter(o => (o.status || '').startsWith('settled_'));

      const funnel = {
        windowDays: days,
        rfqsWithKProp: kOrders.length + kDeclines.length, // rough — orders we quoted + declines that mentioned K props
        declined: kDeclines.length,
        declinedByReason: declinesByReason,
        // Near-miss subset: declines where we wanted to quote but
        // couldn't (no fair value / low confidence / stale data).
        // Excludes intentional blocks like correlation rules and
        // exposure caps. These are the K-prop analog of game-line
        // Near Misses surfaced in the dashboard table.
        nearMissCount: Object.values(nearMissesByReason).reduce((a, b) => a + b, 0),
        nearMissesByReason,
        recentNearMisses,
        quoted: kOrders.length,
        bidsWon: kOrders.filter(isBidWon).length,
        filled: kOrders.filter(isFill).length,
        settled: settledOrders.length,
      };

      // ---- Competitiveness (vs Pinnacle/DK/FD on fair-prob distance) ----
      // For each K-prop quote, compare offered_implied_prob to fair_parlay_prob
      // to compute distance in pp. Aggregate by status (won bid vs lost).
      const americanToImplied = (a) => {
        if (a == null) return null;
        const n = Number(a);
        if (!Number.isFinite(n) || n === 0) return null;
        return n > 0 ? 100 / (n + 100) : Math.abs(n) / (Math.abs(n) + 100);
      };
      const distances = [];
      for (const o of kOrders) {
        const fair = o.fair_parlay_prob != null ? Number(o.fair_parlay_prob) : null;
        const offered = americanToImplied(o.offered_odds);
        if (fair == null || offered == null) continue;
        distances.push({
          parlayId: o.parlay_id,
          status: o.status,
          fairProb: fair,
          offeredProb: offered,
          distancePp: (offered - fair) * 100,
          wonBid: isBidWon(o),
        });
      }
      const avg = (arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
      const competitiveness = {
        sample: distances.length,
        avgDistancePp: avg(distances.map(d => d.distancePp)),
        wonBidAvgDistancePp: avg(distances.filter(d => d.wonBid).map(d => d.distancePp)),
        lostBidAvgDistancePp: avg(distances.filter(d => !d.wonBid).map(d => d.distancePp)),
      };

      // ---- Modeled EV on settled K-prop fills ----
      // SP perspective: positive = we expected to profit
      const americanToDecimal = (a) => {
        if (a == null) return null;
        const n = Number(a);
        if (!Number.isFinite(n) || n === 0) return null;
        return n > 0 ? 1 + n / 100 : 1 + 100 / Math.abs(n);
      };
      let totalStake = 0, totalModeledEV = 0, totalRealizedPnL = 0, evN = 0;
      for (const o of settledOrders) {
        const fp = o.fair_parlay_prob != null ? Number(o.fair_parlay_prob) : null;
        const stake = o.confirmed_stake != null ? Number(o.confirmed_stake) : 0;
        const dec = americanToDecimal(o.offered_odds);
        if (fp == null || dec == null || stake <= 0) continue;
        const profitIfWin = stake * (dec - 1);
        const modeledEV = (1 - fp) * stake - fp * profitIfWin;
        totalStake += stake;
        totalModeledEV += modeledEV;
        totalRealizedPnL += o.pnl != null ? Number(o.pnl) : 0;
        evN++;
      }
      const evSection = {
        settledN: evN,
        totalStake: Math.round(totalStake * 100) / 100,
        modeledEvPerDollar: totalStake > 0 ? Math.round((totalModeledEV / totalStake) * 10000) / 10000 : null,
        realizedPnLPerDollar: totalStake > 0 ? Math.round((totalRealizedPnL / totalStake) * 10000) / 10000 : null,
        modeledEvDollars: Math.round(totalModeledEV * 100) / 100,
        realizedPnLDollars: Math.round(totalRealizedPnL * 100) / 100,
        note: evN < 30 ? `Sample too small for confident EV estimation (n=${evN}). Wait for n≥30 settled K-prop fills.` : null,
      };

      // ---- Live per-pitcher exposure ----
      const pitcherExposure = orderTracker.getPitcherExposureSnapshot();

      res.json({
        ok: true,
        generatedAt: new Date().toISOString(),
        funnel,
        competitiveness,
        ev: evSection,
        pitcherExposure,
      });
    } catch (err) {
      log.error('API', `/prop-performance failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Aggregated breakdown of "unknown legs" declines by sport, category,
  // propType, and specific market. The dominant decline reason today is
  // "unknown legs" (~95% of all 24h declines = ~168k/day). This endpoint
  // tells you WHAT specifically those unknown legs are so you can
  // prioritize what scope to add next.
  //
  // Reads two data sources per row and merges:
  //   1. unknown_categories JSONB (structured: sport, category, propType,
  //      playerName, marketName, line) — present on rows declined after
  //      the column was added; empty otherwise
  //   2. unknown_details TEXT[] (legacy text format, includes [tag] hints
  //      like [propType:X], [unsupported event], [unregistered market])
  //
  // Query params:
  //   days  (default 1)  — lookback window
  //   sport (optional)   — filter to specific sport key
  //   limit (default 5000) — max declines to scan
  app.get('/unknown-legs-breakdown', async (req, res) => {
    try {
      const days = Math.max(1, Math.min(30, parseInt(req.query.days) || 1));
      const sportFilter = String(req.query.sport || '').trim();
      const limit = Math.max(100, Math.min(50000, parseInt(req.query.limit) || 5000));
      const fromIso = new Date(Date.now() - days * 86400000).toISOString();

      const declines = await db.loadDeclines(limit, { fromIso });
      const unknownLegDeclines = declines.filter(d => d.reason === 'unknown legs');

      const bySport = {};
      const byCategory = {};
      const byPropType = {};
      const byTag = { 'unsupported_event': 0, 'unregistered_market': 0, 'other': 0 };
      const byMarketName = {}; // top market names per sport
      const byPropTypeBySport = {}; // sport → propType → count
      let parsedRows = 0;
      let structuredRows = 0;
      let totalUnknownLegs = 0;

      // Parser for the legacy text format: extracts tag + propType from
      // bracketed hints in unknown_details strings.
      const parseTextString = (s) => {
        if (typeof s !== 'string') return null;
        const tagMatch = s.match(/\[(unsupported event|unregistered market)\]/);
        const propMatch = s.match(/\[propType:([^\]]+)\]/);
        return {
          tag: tagMatch ? tagMatch[1] : null,
          propType: propMatch ? propMatch[1] : null,
        };
      };

      for (const d of unknownLegDeclines) {
        parsedRows++;
        const cats = Array.isArray(d.unknownCategories) ? d.unknownCategories : [];
        const txts = Array.isArray(d.unknownDetails) ? d.unknownDetails : [];
        const usingStructured = cats.length > 0;
        if (usingStructured) structuredRows++;

        // Choose source: prefer structured if present, else parse text
        const legCount = Math.max(cats.length, txts.length);
        for (let i = 0; i < legCount; i++) {
          totalUnknownLegs++;
          const cat = cats[i];
          const txt = txts[i];
          const parsed = txt ? parseTextString(txt) : null;

          let sport = (cat && (cat.sport || cat.eventSport)) || 'unknown_sport';
          if (sportFilter && sport !== sportFilter) continue;
          let category = (cat && cat.category) || 'unknown';
          let propType = (cat && cat.propType) || (parsed && parsed.propType) || null;
          const marketName = (cat && cat.marketName) || null;
          const tagFromText = parsed && parsed.tag;
          const tagBucket = tagFromText === 'unsupported event' ? 'unsupported_event'
            : tagFromText === 'unregistered market' ? 'unregistered_market'
            : 'other';

          bySport[sport] = (bySport[sport] || 0) + 1;
          byCategory[category] = (byCategory[category] || 0) + 1;
          if (propType) byPropType[propType] = (byPropType[propType] || 0) + 1;
          byTag[tagBucket] = (byTag[tagBucket] || 0) + 1;
          if (propType) {
            if (!byPropTypeBySport[sport]) byPropTypeBySport[sport] = {};
            byPropTypeBySport[sport][propType] = (byPropTypeBySport[sport][propType] || 0) + 1;
          }
          if (marketName) {
            if (!byMarketName[sport]) byMarketName[sport] = {};
            byMarketName[sport][marketName] = (byMarketName[sport][marketName] || 0) + 1;
          }
        }
      }

      // Sort + slice top-N for the larger maps so the response stays
      // readable
      const topN = (obj, n) => Object.entries(obj)
        .sort((a, b) => b[1] - a[1]).slice(0, n)
        .map(([k, v]) => ({ key: k, count: v }));

      const topMarketsBySport = {};
      for (const [sp, mks] of Object.entries(byMarketName)) {
        topMarketsBySport[sp] = topN(mks, 15);
      }

      res.json({
        ok: true,
        generatedAt: new Date().toISOString(),
        windowDays: days,
        sportFilter: sportFilter || null,
        declinesScanned: declines.length,
        unknownLegDeclines: unknownLegDeclines.length,
        parsedRows,
        structuredRows,
        textOnlyRows: parsedRows - structuredRows,
        totalUnknownLegs,
        bySport: topN(bySport, 30),
        byCategory: topN(byCategory, 20),
        byPropType: topN(byPropType, 30),
        byTag,
        byPropTypeBySport, // full nested map for sport-specific drill-down
        topMarketsBySport, // top 15 raw market names per sport
        note: structuredRows === 0
          ? 'No structured unknown_categories yet — add the column then redeploy. Until then everything is parsed from text.'
          : structuredRows < parsedRows
          ? `${parsedRows - structuredRows}/${parsedRows} rows pre-date the structured-categories column; sport attribution may be missing for those.`
          : null,
      });
    } catch (err) {
      log.error('API', `/unknown-legs-breakdown failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Live per-(sport,player) exposure snapshot for Phase-2 prop launch
  // types (NBA points/rebounds/assists/threes_made, NHL shots_on_goal,
  // etc.). Aggregates SP-risk across ALL parlays containing ANY prop
  // leg featuring that player, regardless of prop type — so cross-prop
  // concentration on one star (e.g. CJ McCollum points + rebounds +
  // threes) rolls up to a single line.
  //
  // Distinct from /prop-performance.pitcherExposure which stays scoped
  // to MLB player_strikeouts.
  app.get('/player-exposure', (req, res) => {
    try {
      const sport = String(req.query.sport || '').trim();
      let snap = orderTracker.getPlayerExposureSnapshot();
      if (sport) snap = snap.filter(e => e.sport === sport);
      res.json({
        ok: true,
        generatedAt: new Date().toISOString(),
        sport: sport || 'all',
        capsBySport: config.pricing.maxExposurePerPlayerBySport || {},
        capDefault: config.pricing.maxExposurePerPlayerDefault,
        playerCount: snap.length,
        players: snap,
      });
    } catch (err) {
      log.error('API', `/player-exposure failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Detailed bid-comparison report. For every matched parlay in the
  // window (bettor's parlay got filled by SOME SP), surface our price
  // alongside the winning price so the operator can calibrate vig.
  // Cross-references matched_parlays.matched_odds against parlay_orders.
  // offered_odds (matched_parlays.our_odds is often null because the
  // in-memory orders[] map gets reset on restart before order.matched
  // arrives — so we look it up by parlay_id from the persisted table).
  //
  // Query params:
  //   days  (default 1)  — window for matched-parlay scan
  //   sport (default '') — empty for all, 'baseball_mlb' / etc. to filter
  //   propsOnly (default 0) — '1' to restrict to K-prop containing parlays
  app.get('/bid-comparison', async (req, res) => {
    try {
      const days = Math.max(1, Math.min(7, parseInt(req.query.days) || 1));
      const propsOnly = req.query.propsOnly === '1' || req.query.propsOnly === 'true';
      const sportFilter = (req.query.sport || '').trim();
      const fromIso = new Date(Date.now() - days * 86400000).toISOString();

      const sb = db.getClient();
      if (!sb) return res.status(500).json({ ok: false, error: 'no DB' });

      // 1. Pull matched parlays in window. Apply K-prop server-side filter
      //    when propsOnly so we don't drag down every match for nothing.
      let query = sb.from('matched_parlays')
        .select('parlay_id, matched_at, matched_odds, matched_stake, our_odds, outcome, legs')
        .gte('matched_at', fromIso)
        .order('matched_at', { ascending: false });
      if (propsOnly) {
        query = query.or('legs.cs.[{"market":"player_strikeouts"}],legs.cs.[{"marketType":"player_strikeouts"}]');
      }
      const { data: matches, error: mErr } = await query.limit(2000);
      if (mErr) return res.status(500).json({ ok: false, error: mErr.message });

      // 2. Batch-lookup our parlay_orders for these parlay IDs to backfill
      //    our_odds when matched_parlays.our_odds is null (in-memory state
      //    lost on restart).
      const ids = (matches || []).map(m => m.parlay_id).filter(Boolean);
      const orderById = {};
      for (let i = 0; i < ids.length; i += 100) {
        const chunk = ids.slice(i, i + 100);
        const { data } = await sb.from('parlay_orders')
          .select('parlay_id, status, offered_odds, fair_parlay_prob, confirmed_odds, confirmed_stake')
          .in('parlay_id', chunk);
        for (const r of (data || [])) orderById[r.parlay_id] = r;
      }

      // 3. Helper: American → bettor implied probability.
      const amToImplied = (a) => {
        if (a == null) return null;
        const n = Number(a);
        if (!Number.isFinite(n) || n === 0) return null;
        return n > 0 ? 100 / (n + 100) : Math.abs(n) / (Math.abs(n) + 100);
      };

      // 4. Build comparison rows + aggregate.
      const rows = [];
      const summary = {
        totalMatches: 0,
        weQuoted: 0,
        weMissed: 0,
        weWon: 0,
        weLost: 0,
        avgGapPpWhenQuoted: null,
        avgGapPpWhenLost: null,
        ourWinRateWhenQuoted: null,
      };
      const gapsWhenQuoted = [];
      const gapsWhenLost = [];

      for (const m of (matches || [])) {
        // Sport filter: any leg matches the sport string
        if (sportFilter) {
          const legs = m.legs || [];
          const hasMatch = legs.some(l => (l.sport || l.oddsApiSport) === sportFilter);
          if (!hasMatch) continue;
        }
        summary.totalMatches++;
        const order = orderById[m.parlay_id] || null;
        const ourOdds = (m.our_odds != null ? m.our_odds
          : order && order.offered_odds != null ? Number(order.offered_odds)
          : null);
        const matched = m.matched_odds != null ? Number(m.matched_odds) : null;
        const ourImpl = amToImplied(ourOdds);
        const matchedImpl = amToImplied(matched);
        const gapPp = (ourImpl != null && matchedImpl != null)
          ? Math.round((matchedImpl - ourImpl) * 1000) / 10  // pp, 1 decimal
          : null;

        const weQuoted = (ourOdds != null);
        if (weQuoted) summary.weQuoted++;
        else summary.weMissed++;
        if (m.outcome === 'won') summary.weWon++;
        else if (weQuoted) summary.weLost++;

        if (gapPp != null && weQuoted) gapsWhenQuoted.push(gapPp);
        if (gapPp != null && weQuoted && m.outcome !== 'won') gapsWhenLost.push(gapPp);

        rows.push({
          matchedAt: m.matched_at,
          parlayId: m.parlay_id,
          legCount: (m.legs || []).length,
          legs: (m.legs || []).map(l => {
            const team = l.team || l.teamName || l.selection || l.playerName || '?';
            const sel = l.selection && /^(over|under)$/i.test(l.selection)
              ? ' ' + l.selection.charAt(0).toUpperCase() + l.selection.slice(1).toLowerCase()
              : '';
            const lineStr = l.line != null ? ' ' + l.line : '';
            return team + sel + lineStr;
          }),
          stake: m.matched_stake,
          matchedOdds: matched,
          ourOdds,
          ourStatus: order ? order.status : null,
          gapPp,
          outcome: m.outcome,
          ourQuoted: weQuoted,
        });
      }

      const avg = (a) => a.length ? Math.round(a.reduce((x, y) => x + y, 0) / a.length * 10) / 10 : null;
      summary.avgGapPpWhenQuoted = avg(gapsWhenQuoted);
      summary.avgGapPpWhenLost = avg(gapsWhenLost);
      summary.ourWinRateWhenQuoted = summary.weQuoted > 0
        ? Math.round((summary.weWon / summary.weQuoted) * 10000) / 100
        : null;

      res.json({
        ok: true,
        generatedAt: new Date().toISOString(),
        windowDays: days,
        filters: { sport: sportFilter || null, propsOnly },
        summary,
        // Most recent first; cap at 200 in response to keep payload sane
        rows: rows.slice(0, 200),
        rowsTruncated: rows.length > 200,
      });
    } catch (err) {
      log.error('API', `/bid-comparison failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.get('/orders', (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    // Stamp each order with isStalePhantom computed server-side so the
    // client has a single truth-flag for "is this a real open position?"
    // Previously the client only checked meta.phantom, which misses
    // two failure modes isOrderStalePhantom catches:
    //   (1) 'confirmed' with no orderUuid after 10min (finalize never came)
    //   (2) all legs started >12h ago (stuck settlement)
    // Without this flag the Risk Simulation was counting ~120 no-UUID
    // ghosts as real positions, inflating Total SP risk to $22k when
    // actual deployed capital was $5-7k.
    const stamp = (o) => ({ ...o, isStalePhantom: orderTracker.isOrderStalePhantom(o) });
    res.json({
      stats: orderTracker.getStats(),
      pnlBySport: orderTracker.getPnLBySport(),
      recentOrders: orderTracker.getRecentOrders(limit).map(stamp),
    });
  });

  // Daily P&L from Supabase (settled orders grouped by date).
  // Optional ?groupBy=quoted_at|settled_at — defaults to settled_at.
  // The dashboard's "Daily Volume & P&L" chart groups by quoted_at, so
  // use that param to match what's visualized when investigating a day.
  // Paginates past Supabase's 1,000-row limit so multi-week windows
  // don't silently truncate.
  app.get('/daily-pnl', async (req, res) => {
    const days = parseInt(req.query.days) || 30;
    const groupBy = req.query.groupBy === 'quoted_at' ? 'quoted_at' : 'settled_at';
    try {
      const daily = await db.getDailyPnL(days, { groupBy });
      const totalPnL = await db.getTotalPnL();
      res.json({ daily, totalPnL, groupBy });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Forensic orders pull by date range — hits Supabase directly with
  // pagination, returns fully-hydrated rows (legs, odds, P&L, meta).
  // Query params:
  //   from=YYYY-MM-DD (required)
  //   to=YYYY-MM-DD   (required)
  //   groupBy=quoted_at|settled_at (default quoted_at)
  //   status=settled_lost (optional filter)
  //   maxRows=10000 (cap; default 10000, ceiling 50000)
  // Use for ad-hoc forensic investigations of specific days.
  app.get('/orders-by-date', async (req, res) => {
    const from = req.query.from;
    const to = req.query.to;
    if (!from || !to) {
      return res.status(400).json({ error: 'need ?from=YYYY-MM-DD&to=YYYY-MM-DD' });
    }
    const groupBy = req.query.groupBy === 'settled_at' ? 'settled_at' : 'quoted_at';
    const status = req.query.status || null;
    const maxRows = Math.min(parseInt(req.query.maxRows) || 10000, 50000);
    // Date-only inputs become full-day ISO ranges. "to" gets a 23:59:59.999
    // end so a same-day request (from=to=2026-04-18) spans the full day.
    const fromIso = from.length === 10 ? `${from}T00:00:00.000Z` : from;
    const toIso = to.length === 10 ? `${to}T23:59:59.999Z` : to;
    try {
      const rows = await db.loadOrdersInDateRange(fromIso, toIso, { groupBy, status, maxRows });
      res.json({
        from: fromIso, to: toIso, groupBy, status, maxRows,
        count: rows.length,
        truncated: rows.length >= maxRows,
        orders: rows,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Sport-filtered orders — e.g. /orders/golf, /orders/mma, /orders/nba
  // Matches if ANY leg's sport field contains the filter string.
  app.get('/orders/:sportFilter', (req, res) => {
    const filter = req.params.sportFilter.toLowerCase();
    const limit = parseInt(req.query.limit) || 500;
    const statusFilter = req.query.status; // e.g. ?status=settled
    const allOrders = orderTracker.getRecentOrders(limit);
    const matched = allOrders.filter(o => {
      const legs = o.meta?.legs || o.legs || [];
      const hasSport = legs.some(l => (l.sport || '').toLowerCase().includes(filter));
      if (!hasSport) return false;
      if (statusFilter === 'settled') return o.status && o.status.startsWith('settled_');
      if (statusFilter === 'confirmed') return o.status === 'confirmed';
      if (statusFilter) return o.status === statusFilter;
      return true;
    });
    const settled = matched.filter(o => o.status && o.status.startsWith('settled_'));
    const totalPnL = settled.reduce((s, o) => s + (o.pnl || 0), 0);
    const wins = settled.filter(o => o.status === 'settled_won').length;
    const losses = settled.filter(o => o.status === 'settled_lost').length;
    // Stamp with isStalePhantom so the client has one flag for "is this
    // a real open position?" — see /orders endpoint above for rationale.
    const stamp = (o) => ({ ...o, isStalePhantom: orderTracker.isOrderStalePhantom(o) });
    res.json({
      filter,
      total: matched.length,
      settled: settled.length,
      wins,
      losses,
      pushVoid: settled.length - wins - losses,
      totalPnL: Math.round(totalPnL * 100) / 100,
      orders: matched.map(stamp),
    });
  });

  // Market intelligence
  app.get('/market-intel', (req, res) => {
    const limit = parseInt(req.query.limit) || 50;
    res.json(orderTracker.getMarketIntel(limit));
  });

  // Latency breakdown — per-stage percentiles + win-rate-by-bucket
  // For measuring before/after effects of latency optimizations.
  app.get('/latency-breakdown', (req, res) => {
    res.json(websocket.getLatencyBreakdown());
  });

  // Alt-line pre-warming stats + manual trigger
  app.get('/alt-lines-stats', (req, res) => {
    res.json(oddsFeed.getAltLinesWarmStats());
  });

  // Just-in-time warm stats — fire counts, skips, cache hits, queue depth.
  // Use this to verify JIT is triggering on new-event registrations and
  // shortening the new-event coverage gap that the 15s periodic loop misses.
  app.get('/jit-warm-stats', (req, res) => {
    res.json(oddsFeed.getJitWarmStats());
  });

  // Sync alt-line fast-path stats — per-reason miss breakdown for RFQs
  // that fell through to the async fetchAltLines path instead of being
  // resolved sync. Use this to distinguish stale-cache misses (warm loop
  // gap) from line-not-cached misses (alt line outside cached range) or
  // sanity-gate rejections.
  app.get('/sync-alt-stats', (req, res) => {
    res.json(oddsFeed.getAltSyncStats());
  });

  // Pinnacle line-verify warm-loop stats — per-combo fetch counts, cache
  // ages, last cycle duration. Use to confirm the loop is keeping the
  // 30s-TTL verify cache fresh so RFQs with primary spread/total legs
  // never pay the inline fetch cost.
  app.get('/pin-verify-stats', (req, res) => {
    res.json(oddsFeed.getPinVerifyWarmStats());
  });

  // PX fetchMarkets cache hit-rate. Watch hitRate and coalesced to
  // confirm the cache is collapsing concurrent on-demand resolutions
  // for the same event. Tail latency on the "resolve" stage should
  // drop as this climbs.
  app.get('/px-markets-cache-stats', (req, res) => {
    res.json(px.getMarketsCacheStats());
  });

  // Template-exposure ramp: per-parlay-signature counts + stakes
  // inside the rolling window, plus ramp-firing stats. Use this to
  // verify the ramp is catching template stacking as intended and to
  // see top-active signatures on any given day.
  app.get('/template-exposure-stats', (req, res) => {
    const templateExposure = require('./services/template-exposure');
    res.json(templateExposure.getStats());
  });

  // ---- v2 pricing engine diagnostics (shadow-mode data) ----
  // Calibration fit summary: per-bucket bias/correction/sample-size.
  // Use to inspect where the calibration layer is actively shifting
  // v2 predictions vs raw de-vig.
  app.get('/v2-calibration-stats', (req, res) => {
    res.json(require('./services/v2').calibration.getStats());
  });
  // Shadow-mode comparison log: last N v1-vs-v2 records, median deltas,
  // correlation lifts. Populates only when PRICING_V2_ENABLED=true.
  app.get('/v2-shadow-stats', (req, res) => {
    res.json(require('./services/v2').getShadowStats());
  });
  // Manual refit trigger. Normally runs automatically on a weekly
  // timer and at boot; call this after loading fresh history or
  // adjusting the calibration trainer.
  app.post('/v2-refit', (req, res) => {
    try {
      const v2 = require('./services/v2');
      const orders = orderTracker.getRecentOrders(100000);
      v2.calibration.trainFromOrders(orders);
      const stats = v2.calibration.getStats();
      res.json({ ok: true, legsAnalyzed: stats.legsAnalyzed, buckets: Object.keys(stats.buckets || {}).length, overall: stats.overall });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // A/B metrics — splits orders by meta.abArm (set by pricer) and
  // reports per-arm quote/fill/pnl stats over a time window.
  // Reads straight from the orders table so the numbers are derived
  // from real confirmedAt/settledAt timestamps, not from the
  // reconciliation-blind session counters.
  //
  // Query: ?from=ISO&to=ISO  (defaults: last 24h, using quoted_at)
  //        ?sport=...        (optional filter on meta.sport per order)
  //
  // Returns { window, v1: stats, v2: stats, overall: stats } where
  // stats = {quotes, fills, fillRate, avgStake, settledN, pnl,
  //          evPerDollarWagered, avgOfferedOdds}.
  app.get('/v2-ab-metrics', async (req, res) => {
    try {
      const now = Date.now();
      // Time window: explicit from/to wins. windowMinutes is a convenience
      // alias (now − N min → now). Without either, defaults to last 24h.
      // Apr 25 bug: the scheduled trigger and operator queries had been
      // passing windowMinutes which the endpoint silently ignored,
      // returning the default 24h aggregate every call. windowMinutes is
      // now first-class so callers can't accidentally measure the wrong
      // window after a setting change.
      const windowMinutes = req.query.windowMinutes != null
        ? Math.max(1, Number(req.query.windowMinutes))
        : null;
      const to = req.query.to || new Date(now).toISOString();
      const from = req.query.from || (windowMinutes != null
        ? new Date(now - windowMinutes * 60 * 1000).toISOString()
        : new Date(now - 24 * 60 * 60 * 1000).toISOString());
      const sportFilter = req.query.sport || null;
      // Optional time-bucketing for trend analysis. bucketMinutes=30
      // returns a `series` array of {bucketStart, quotes, fills, fillRate,
      // avgStake, ...} per arm so a setting change is visible as an
      // inflection point, not just folded into one aggregate. Capped at
      // 200 buckets to keep the response bounded; finer granularity
      // requires a tighter window.
      const bucketMinutes = req.query.bucketMinutes != null
        ? Math.max(1, Number(req.query.bucketMinutes))
        : null;
      // Optional red-box subdivision. When includeRedBox=1, each summary
      // also reports {redBox: {…}} for the fair_parlay_prob<redBoxThreshold
      // subset (default 0.17 — matches the dashboard chart's red box).
      const includeRedBox = req.query.includeRedBox === '1' || req.query.includeRedBox === 'true';
      const redBoxThreshold = req.query.redBoxThreshold != null
        ? Number(req.query.redBoxThreshold)
        : 0.17;

      // Parlay-shape filter — narrows results to a specific parlay
      // structure for monitoring SGP / prop fill rates separately from
      // game-only flow. Values: 'sgp', 'prop', 'game_only' (no value =
      // no shape filter).
      const parlayShape = req.query.parlayShape || null;
      function classifyShape(legs) {
        if (!Array.isArray(legs) || legs.length < 1) return 'game_only';
        const hasProp = legs.some(l => /^player_/.test(l.market || l.marketType || ''));
        if (hasProp) return 'prop';
        // SGP: 2+ legs all on the same pxEventId
        if (legs.length < 2) return 'game_only';
        const eids = new Set(legs.map(l => l.pxEventId).filter(Boolean));
        if (eids.size === 1) return 'sgp';
        return 'game_only';
      }

      const rows = await db.loadOrdersInDateRange(from, to, { groupBy: 'quoted_at', maxRows: 50000 });

      const buckets = { v1: [], v2: [], overall: [] };
      for (const r of rows) {
        const arm = (r.meta && r.meta.abArm) || 'v1';
        const legs = r.legs || (r.meta && r.meta.legs) || [];
        if (sportFilter) {
          const sports = new Set(legs.map(l => l.sport).filter(Boolean));
          if (!sports.has(sportFilter)) continue;
        }
        if (parlayShape) {
          if (classifyShape(legs) !== parlayShape) continue;
        }
        buckets.overall.push(r);
        if (arm === 'v2') buckets.v2.push(r);
        else buckets.v1.push(r);
      }

      // Real-fill filter (post-Apr-25 false-fill discovery): a row is
      // a true fill ONLY if it has an order_uuid (recorded on the
      // order.finalized event) OR is already settled. Pre-fix code
      // counted every status='confirmed' row, which inflated v1+v2 fill
      // counts ~30× because the order.matched broadcast was promoting
      // 97% of quotes that some other SP actually won. Use the same
      // gate everywhere fillRate is computed.
      const isRealFill = (r) =>
        (r.status === 'confirmed' && r.order_uuid != null) ||
        (typeof r.status === 'string' && r.status.startsWith('settled_'));

      // "Best bidder" = PX selected our quote as the winning offer.
      // status='confirmed' fires from recordMatchedParlay (gated post
      // Apr 25 to require sign-flip on matched_odds = our offered_odds,
      // so the broadcast is genuinely OUR win). Includes both the
      // 'Offered' state (no orderUuid yet, awaiting bettor accept) AND
      // the 'Accepted' state (bettor confirmed). Settled_* rows imply
      // accepted, which implies bid won.
      //
      // bestBidderRate = bidsWon / quotes — how often we win the bid.
      // fillRate       = fills   / quotes — how often that converts to
      //                                      a booked parlay on PX.
      // conversion     = fills / bidsWon  — bettor follow-through rate
      //                                      after we win their bid.
      // The gap between bestBidderRate and fillRate is the
      // "bettor walked during final review" cohort.
      const isBestBidder = (r) =>
        r.status === 'confirmed' ||
        (typeof r.status === 'string' && r.status.startsWith('settled_'));

      function summarize(list) {
        const quotes = list.length;
        const fills = list.filter(isRealFill).length;
        const bidsWon = list.filter(isBestBidder).length;
        const stakes = list.filter(r => r.confirmed_stake != null).map(r => Number(r.confirmed_stake));
        const avgStake = stakes.length ? stakes.reduce((a, b) => a + b, 0) / stakes.length : null;
        const totalStake = stakes.reduce((a, b) => a + b, 0);
        const settled = list.filter(r => (r.status || '').startsWith('settled_'));
        const pnls = settled.map(r => r.pnl != null ? Number(r.pnl) : 0);
        const pnl = pnls.reduce((a, b) => a + b, 0);
        const totalStakeSettled = settled.reduce((s, r) => s + (r.confirmed_stake ? Number(r.confirmed_stake) : 0), 0);
        const evPerDollar = totalStakeSettled > 0 ? pnl / totalStakeSettled : null;
        const offeredOdds = list.filter(r => r.offered_odds != null).map(r => Number(r.offered_odds));
        const avgOfferedOdds = offeredOdds.length ? offeredOdds.reduce((a, b) => a + b, 0) / offeredOdds.length : null;
        const out = {
          quotes,
          bidsWon,
          fills,
          bestBidderRate: quotes > 0 ? Math.round((bidsWon / quotes) * 10000) / 100 : null, // %
          fillRate: quotes > 0 ? Math.round((fills / quotes) * 10000) / 100 : null, // %
          conversion: bidsWon > 0 ? Math.round((fills / bidsWon) * 10000) / 100 : null, // % of best-bidder wins that the bettor accepted
          avgStake: avgStake != null ? Math.round(avgStake * 100) / 100 : null,
          totalStake: Math.round(totalStake * 100) / 100,
          settledN: settled.length,
          pnl: Math.round(pnl * 100) / 100,
          evPerDollarWagered: evPerDollar != null ? Math.round(evPerDollar * 10000) / 10000 : null,
          avgOfferedOdds: avgOfferedOdds != null ? Math.round(avgOfferedOdds) : null,
        };
        if (includeRedBox) {
          const rb = list.filter(r => {
            const fp = r.fair_parlay_prob != null
              ? Number(r.fair_parlay_prob)
              : (r.meta && r.meta.fairParlayProb != null ? Number(r.meta.fairParlayProb) : null);
            return fp != null && fp < redBoxThreshold;
          });
          out.redBox = summarizeNoRedBox(rb);
          out.redBox.threshold = redBoxThreshold;
        }
        return out;
      }
      // Re-entrant summary without the redBox sub-recursion to avoid
      // infinite nesting. Same metrics, one level deep.
      function summarizeNoRedBox(list) {
        const quotes = list.length;
        const fills = list.filter(isRealFill).length;
        const bidsWon = list.filter(isBestBidder).length;
        const stakes = list.filter(r => r.confirmed_stake != null).map(r => Number(r.confirmed_stake));
        const avgStake = stakes.length ? stakes.reduce((a, b) => a + b, 0) / stakes.length : null;
        const totalStake = stakes.reduce((a, b) => a + b, 0);
        const settled = list.filter(r => (r.status || '').startsWith('settled_'));
        const pnls = settled.map(r => r.pnl != null ? Number(r.pnl) : 0);
        const pnl = pnls.reduce((a, b) => a + b, 0);
        const totalStakeSettled = settled.reduce((s, r) => s + (r.confirmed_stake ? Number(r.confirmed_stake) : 0), 0);
        const evPerDollar = totalStakeSettled > 0 ? pnl / totalStakeSettled : null;
        const offeredOdds = list.filter(r => r.offered_odds != null).map(r => Number(r.offered_odds));
        const avgOfferedOdds = offeredOdds.length ? offeredOdds.reduce((a, b) => a + b, 0) / offeredOdds.length : null;
        return {
          quotes,
          bidsWon,
          fills,
          bestBidderRate: quotes > 0 ? Math.round((bidsWon / quotes) * 10000) / 100 : null,
          fillRate: quotes > 0 ? Math.round((fills / quotes) * 10000) / 100 : null,
          conversion: bidsWon > 0 ? Math.round((fills / bidsWon) * 10000) / 100 : null,
          avgStake: avgStake != null ? Math.round(avgStake * 100) / 100 : null,
          totalStake: Math.round(totalStake * 100) / 100,
          settledN: settled.length,
          pnl: Math.round(pnl * 100) / 100,
          evPerDollarWagered: evPerDollar != null ? Math.round(evPerDollar * 10000) / 10000 : null,
          avgOfferedOdds: avgOfferedOdds != null ? Math.round(avgOfferedOdds) : null,
        };
      }

      const response = {
        window: { from, to, windowMinutes },
        sportFilter,
        v1: summarize(buckets.v1),
        v2: summarize(buckets.v2),
        overall: summarize(buckets.overall),
      };

      // Time-bucketed series for trend visualization. Walks each row into
      // its bucket by quotedAt, then summarizes per bucket per arm. Useful
      // for spotting "did fill rate drop after the env-var change at
      // 13:00 UTC" without eyeballing aggregates from two separate calls.
      if (bucketMinutes != null) {
        const fromMs = new Date(from).getTime();
        const toMs = new Date(to).getTime();
        const bucketMs = bucketMinutes * 60 * 1000;
        const totalBuckets = Math.ceil((toMs - fromMs) / bucketMs);
        if (totalBuckets > 200) {
          response.warning = `bucketMinutes=${bucketMinutes} would produce ${totalBuckets} buckets (cap=200) — series omitted; use a tighter window or larger bucketMinutes`;
        } else {
          const seriesByArm = { v1: [], v2: [], overall: [] };
          // Pre-create empty buckets so gaps in traffic still show up.
          for (let i = 0; i < totalBuckets; i++) {
            const startMs = fromMs + i * bucketMs;
            const endMs = Math.min(toMs, startMs + bucketMs);
            for (const arm of ['v1', 'v2', 'overall']) {
              seriesByArm[arm].push({
                bucketStart: new Date(startMs).toISOString(),
                bucketEnd: new Date(endMs).toISOString(),
                rows: [],
              });
            }
          }
          for (const r of buckets.overall) {
            const ts = r.quoted_at ? new Date(r.quoted_at).getTime() : null;
            if (ts == null || isNaN(ts)) continue;
            const idx = Math.floor((ts - fromMs) / bucketMs);
            if (idx < 0 || idx >= totalBuckets) continue;
            seriesByArm.overall[idx].rows.push(r);
            const arm = (r.meta && r.meta.abArm) === 'v2' ? 'v2' : 'v1';
            seriesByArm[arm][idx].rows.push(r);
          }
          for (const arm of ['v1', 'v2', 'overall']) {
            response[arm].series = seriesByArm[arm].map(b => ({
              bucketStart: b.bucketStart,
              bucketEnd: b.bucketEnd,
              ...summarizeNoRedBox(b.rows),
            }));
          }
          response.bucketMinutes = bucketMinutes;
        }
      }

      res.json(response);
    } catch (err) {
      log.error('API', `/v2-ab-metrics failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });
  app.post('/warm-alt-lines', async (req, res) => {
    const sport = req.query.sport || req.body?.sport;
    try {
      if (sport) {
        const result = await oddsFeed.warmAltLines(sport);
        res.json({ ok: true, result });
      } else {
        // Warm all configured sports
        const results = {};
        for (const s of config.supportedSports || []) {
          try {
            results[s] = await oddsFeed.warmAltLines(s);
          } catch (err) {
            results[s] = { error: err.message };
          }
        }
        res.json({ ok: true, results });
      }
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Competitiveness analytics. For every matched parlay where we ALSO
  // offered (weQuoted=true) but didn't win, computes the gap between our
  // offered odds and the winning SP's matched_odds. Surfaces how far
  // off-market our pricing is on RFQs we lose, broken down by sport
  // and parlay shape so we can dial vig at the right granularity.
  //
  // Query params:
  //   windowMinutes=N    Look-back window (default 180 = last 3h, max 10080 = 7d)
  //   sport=key          Filter by sport (any leg in that sport)
  //   maxGapCents=N      Cap gap-cents tail in the histogram (default 200)
  //
  // Per-RFQ output: parlayId, ourOdds, winnerOdds, ourImplied, winnerImplied,
  // gapPp (our_implied − winner_implied; positive = we were tighter),
  // gapCents (payout per $100 stake difference at the winner's offer),
  // legCount, sports.
  //
  // Aggregate output: count, avgGapCents, p50/p90 gap, withinFiveCents,
  // withinTenCents, wayOutOfMarket (>50c gap), per-sport breakdown.
  app.get('/competitiveness', async (req, res) => {
    try {
      const windowMinutes = Math.min(10080, Math.max(1, parseInt(req.query.windowMinutes) || 180));
      const sportFilter = req.query.sport || null;
      const maxGapCents = Math.min(1000, Math.max(20, parseInt(req.query.maxGapCents) || 200));

      // Load both tables and join in JS. parlay_orders is the
      // PERSISTENT source of our offered odds — survives every Railway
      // restart. matched_parlays stores the winning SP's matched_odds.
      // The previous read off matched_parlays.our_odds depended on the
      // in-memory orders[] being populated at match time, which clears
      // on every redeploy → weQuoted=false flooded the table whenever
      // the operator was tuning env vars. Join-via-parlayId fixes that.
      const fromIso = new Date(Date.now() - windowMinutes * 60 * 1000).toISOString();
      const toIso = new Date().toISOString();
      const [matched, ourOrders] = await Promise.all([
        db.loadMatchedParlays(10000),
        db.loadOrdersInDateRange(fromIso, toIso, { groupBy: 'quoted_at', maxRows: 50000 }),
      ]);
      const cutoff = Date.now() - windowMinutes * 60 * 1000;

      // Index our orders by parlayId for fast lookup. Our offered_odds
      // is bettor-side American (positive=longshot for bettor).
      const ourByParlayId = {};
      for (const r of ourOrders) {
        if (!r.parlay_id) continue;
        if (r.offered_odds == null) continue;
        ourByParlayId[r.parlay_id] = {
          ourOdds: Number(r.offered_odds),
          legs: r.legs || (r.meta && r.meta.legs) || [],
          quotedAt: r.quoted_at,
          status: r.status,
        };
      }

      // American odds → implied probability
      function americanToImplied(am) {
        if (am == null || !Number.isFinite(am) || am === 0) return null;
        if (am > 0) return 100 / (am + 100);
        return -am / (-am + 100);
      }
      function americanToPayoutPer100(am) {
        if (am == null || !Number.isFinite(am) || am === 0) return null;
        if (am > 0) return am;
        return Math.round(100 * (100 / Math.abs(am)));
      }

      const lostRfqs = [];
      for (const m of matched) {
        if (!m.parlayId) continue;
        if (m.matchedAmericanOdds == null) continue;
        const matchedAtMs = m.matchedAt ? new Date(m.matchedAt).getTime() : 0;
        if (matchedAtMs < cutoff) continue;
        const our = ourByParlayId[m.parlayId];
        if (!our) continue; // we didn't quote this parlay
        if (our.ourOdds === m.matchedAmericanOdds) continue; // tied / our quote won
        if (sportFilter) {
          const sports = (m.legs || our.legs || []).map(l => l.sport).filter(Boolean);
          if (!sports.includes(sportFilter)) continue;
        }
        const ourImpl = americanToImplied(our.ourOdds);
        const winImpl = americanToImplied(m.matchedAmericanOdds);
        if (ourImpl == null || winImpl == null) continue;
        const gapPp = (ourImpl - winImpl) * 100;
        const ourPayout = americanToPayoutPer100(our.ourOdds);
        const winPayout = americanToPayoutPer100(m.matchedAmericanOdds);
        const gapCents = (ourPayout != null && winPayout != null) ? (winPayout - ourPayout) : null;
        const sports = [...new Set((m.legs || our.legs || []).map(l => l.sport).filter(Boolean))];
        lostRfqs.push({
          parlayId: m.parlayId,
          ourOdds: our.ourOdds,
          winnerOdds: m.matchedAmericanOdds,
          ourImpliedPct: Math.round(ourImpl * 10000) / 100,
          winnerImpliedPct: Math.round(winImpl * 10000) / 100,
          gapPp: Math.round(gapPp * 100) / 100,
          gapCents,
          legCount: (m.legs || our.legs || []).length,
          sports,
          matchedAt: m.matchedAt,
        });
      }

      // Aggregate
      function p(arr, q) {
        if (arr.length === 0) return null;
        const sorted = [...arr].sort((a, b) => a - b);
        return sorted[Math.min(sorted.length - 1, Math.max(0, Math.floor(q * sorted.length)))];
      }
      const gaps = lostRfqs.map(r => r.gapCents).filter(g => g != null);
      const gapsPp = lostRfqs.map(r => r.gapPp).filter(g => g != null);

      // Per-sport summary
      const bySport = {};
      for (const r of lostRfqs) {
        for (const s of r.sports) {
          if (!bySport[s]) bySport[s] = { count: 0, gaps: [], gapsPp: [] };
          bySport[s].count++;
          if (r.gapCents != null) bySport[s].gaps.push(r.gapCents);
          if (r.gapPp != null) bySport[s].gapsPp.push(r.gapPp);
        }
      }
      const sportSummary = {};
      for (const [s, v] of Object.entries(bySport)) {
        sportSummary[s] = {
          count: v.count,
          avgGapCents: v.gaps.length ? Math.round(v.gaps.reduce((a, b) => a + b, 0) / v.gaps.length * 10) / 10 : null,
          medianGapCents: p(v.gaps, 0.5),
          p90GapCents: p(v.gaps, 0.9),
          avgGapPp: v.gapsPp.length ? Math.round(v.gapsPp.reduce((a, b) => a + b, 0) / v.gapsPp.length * 100) / 100 : null,
        };
      }

      // Closest losses — RFQs we lost by the smallest gap (highest signal
      // for tuning: these are the ones we'd capture with a small dial-back).
      const closest = lostRfqs
        .filter(r => r.gapCents != null && r.gapCents > 0)
        .sort((a, b) => a.gapCents - b.gapCents)
        .slice(0, 25);

      // Histogram buckets in 5-cent steps up to maxGapCents
      const histo = {};
      for (let b = 0; b < maxGapCents; b += 5) histo[`${b}-${b + 5}`] = 0;
      histo[`${maxGapCents}+`] = 0;
      for (const g of gaps) {
        if (g < 0) continue; // we were wider (won't usually happen if we lost)
        if (g >= maxGapCents) histo[`${maxGapCents}+`]++;
        else histo[`${Math.floor(g / 5) * 5}-${Math.floor(g / 5) * 5 + 5}`]++;
      }

      res.json({
        ok: true,
        window: { minutes: windowMinutes, from: new Date(cutoff).toISOString(), to: new Date().toISOString() },
        sportFilter,
        summary: {
          totalLost: lostRfqs.length,
          avgGapCents: gaps.length ? Math.round(gaps.reduce((a, b) => a + b, 0) / gaps.length * 10) / 10 : null,
          medianGapCents: p(gaps, 0.5),
          p90GapCents: p(gaps, 0.9),
          p99GapCents: p(gaps, 0.99),
          avgGapPp: gapsPp.length ? Math.round(gapsPp.reduce((a, b) => a + b, 0) / gapsPp.length * 100) / 100 : null,
          medianGapPp: p(gapsPp, 0.5),
          withinFiveCents: gaps.filter(g => g <= 5 && g >= 0).length,
          withinTenCents: gaps.filter(g => g <= 10 && g >= 0).length,
          withinTwentyCents: gaps.filter(g => g <= 20 && g >= 0).length,
          wayOutOfMarket: gaps.filter(g => g > 50).length,
        },
        bySport: sportSummary,
        closestLosses: closest,
        histogram: histo,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Hour-of-week analytics. Buckets historical parlays by ET day-of-week
  // and ET hour to surface time-window-specific patterns the global
  // aggregates hide. Originally built for Saturday-morning vig tuning:
  // "what's our actual fill rate + realized margin in the 12am-9am ET
  // Saturday window, broken down by sport, so we can pick a base vig
  // that actually reflects that window's competitive landscape?"
  //
  // Query params:
  //   days=N             Look-back window in days (default 60, max 365)
  //   sport=key          Filter to parlays containing this sport (any leg)
  //   dayOfWeek=N        Filter to one ET day-of-week (0=Sun ... 6=Sat).
  //                      When set, returns 24 hour buckets (one row per
  //                      ET hour 00-23). When omitted, returns 168
  //                      hour-of-week buckets (full week, hour 0..167
  //                      = Sun 00:00 ET .. Sat 23:00 ET).
  //   includeSportBreakdown=1   Also report per-sport stats inside each
  //                             bucket (only meaningful when no sport
  //                             filter is set).
  //
  // Per bucket fields mirror /v2-ab-metrics summarize():
  //   quotes, bidsWon, fills, bestBidderRate, fillRate, conversion,
  //   avgStake, totalStake, settledN, pnl, evPerDollarWagered,
  //   avgOfferedOdds, avgPairVigPct (estimated from fair vs offered)
  app.get('/analytics/hour-of-week', async (req, res) => {
    try {
      const days = Math.min(365, Math.max(1, parseInt(req.query.days) || 60));
      const sportFilter = req.query.sport || null;
      const dayOfWeekFilter = req.query.dayOfWeek != null
        ? Math.max(0, Math.min(6, parseInt(req.query.dayOfWeek)))
        : null;
      const includeSportBreakdown = req.query.includeSportBreakdown === '1' || req.query.includeSportBreakdown === 'true';

      const now = Date.now();
      const fromIso = new Date(now - days * 24 * 60 * 60 * 1000).toISOString();
      const toIso = new Date(now).toISOString();

      const rows = await db.loadOrdersInDateRange(fromIso, toIso, { groupBy: 'quoted_at', maxRows: 50000 });

      // ET conversion. Intl.DateTimeFormat with timeZone='America/New_York'
      // yields ET-local hour and weekday handling DST automatically — far
      // safer than hardcoding a -5/-4 offset.
      const dtf = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        weekday: 'short',
        hour: 'numeric',
        hour12: false,
      });
      const weekdayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
      function etDayHour(iso) {
        if (!iso) return null;
        const t = new Date(iso);
        if (isNaN(t.getTime())) return null;
        const parts = dtf.formatToParts(t);
        const wd = parts.find(p => p.type === 'weekday')?.value;
        const hr = parseInt(parts.find(p => p.type === 'hour')?.value);
        if (wd == null || isNaN(hr)) return null;
        const dow = weekdayMap[wd];
        if (dow == null) return null;
        // Intl returns 24 for midnight in some locales; normalize.
        const hour = hr === 24 ? 0 : hr;
        return { dow, hour };
      }

      const isRealFill = (r) =>
        (r.status === 'confirmed' && r.order_uuid != null) ||
        (typeof r.status === 'string' && r.status.startsWith('settled_'));
      const isBestBidder = (r) =>
        r.status === 'confirmed' ||
        (typeof r.status === 'string' && r.status.startsWith('settled_'));

      // Apply filters and bucket
      // Bucket key: when dayOfWeekFilter is set, key = hour (0..23). Else
      // key = dow*24 + hour (0..167).
      const bucketCount = dayOfWeekFilter != null ? 24 : 168;
      const buckets = new Array(bucketCount).fill(null).map(() => ({ rows: [], bySport: {} }));

      let filtered = 0;
      for (const r of rows) {
        const dh = etDayHour(r.quoted_at);
        if (!dh) continue;
        if (dayOfWeekFilter != null && dh.dow !== dayOfWeekFilter) continue;
        if (sportFilter) {
          const legs = r.legs || (r.meta && r.meta.legs) || [];
          if (!legs.some(l => l.sport === sportFilter)) continue;
        }
        const idx = dayOfWeekFilter != null ? dh.hour : (dh.dow * 24 + dh.hour);
        buckets[idx].rows.push(r);
        if (includeSportBreakdown) {
          const legs = r.legs || (r.meta && r.meta.legs) || [];
          const sports = new Set(legs.map(l => l.sport).filter(Boolean));
          for (const s of sports) {
            if (!buckets[idx].bySport[s]) buckets[idx].bySport[s] = [];
            buckets[idx].bySport[s].push(r);
          }
        }
        filtered++;
      }

      function summarize(list) {
        if (list.length === 0) {
          return { quotes: 0 };
        }
        const quotes = list.length;
        const fills = list.filter(isRealFill).length;
        const bidsWon = list.filter(isBestBidder).length;
        const stakes = list.filter(r => r.confirmed_stake != null).map(r => Number(r.confirmed_stake));
        const avgStake = stakes.length ? stakes.reduce((a, b) => a + b, 0) / stakes.length : null;
        const totalStake = stakes.reduce((a, b) => a + b, 0);
        const settled = list.filter(r => (r.status || '').startsWith('settled_'));
        const pnl = settled.reduce((s, r) => s + (r.pnl != null ? Number(r.pnl) : 0), 0);
        const totalStakeSettled = settled.reduce((s, r) => s + (r.confirmed_stake ? Number(r.confirmed_stake) : 0), 0);
        const evPerDollar = totalStakeSettled > 0 ? pnl / totalStakeSettled : null;
        const offeredOdds = list.filter(r => r.offered_odds != null).map(r => Number(r.offered_odds));
        const avgOfferedOdds = offeredOdds.length ? offeredOdds.reduce((a, b) => a + b, 0) / offeredOdds.length : null;
        // Estimated pair vig: 1 - fairProb × decimalOffered. Decimal from
        // American: pos → 1+am/100; neg → 1+100/|am|.
        const vigSamples = [];
        for (const r of list) {
          const fp = r.fair_parlay_prob != null ? Number(r.fair_parlay_prob)
            : (r.meta && r.meta.fairParlayProb != null ? Number(r.meta.fairParlayProb) : null);
          const am = r.offered_odds != null ? Number(r.offered_odds) : null;
          if (fp == null || am == null || fp <= 0 || fp >= 1) continue;
          const dec = am >= 0 ? 1 + am / 100 : 1 + 100 / Math.abs(am);
          const v = 1 - fp * dec; // bettor disadvantage as decimal
          if (Number.isFinite(v)) vigSamples.push(v);
        }
        const avgVig = vigSamples.length
          ? vigSamples.reduce((a, b) => a + b, 0) / vigSamples.length
          : null;
        return {
          quotes,
          bidsWon,
          fills,
          bestBidderRate: quotes > 0 ? Math.round((bidsWon / quotes) * 10000) / 100 : null,
          fillRate: quotes > 0 ? Math.round((fills / quotes) * 10000) / 100 : null,
          conversion: bidsWon > 0 ? Math.round((fills / bidsWon) * 10000) / 100 : null,
          avgStake: avgStake != null ? Math.round(avgStake * 100) / 100 : null,
          totalStake: Math.round(totalStake * 100) / 100,
          settledN: settled.length,
          pnl: Math.round(pnl * 100) / 100,
          evPerDollarWagered: evPerDollar != null ? Math.round(evPerDollar * 10000) / 10000 : null,
          avgOfferedOdds: avgOfferedOdds != null ? Math.round(avgOfferedOdds) : null,
          avgVigPct: avgVig != null ? Math.round(avgVig * 10000) / 100 : null, // % bettor disadvantage
        };
      }

      const dayLabels = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
      const series = buckets.map((b, idx) => {
        const dow = dayOfWeekFilter != null ? dayOfWeekFilter : Math.floor(idx / 24);
        const hour = dayOfWeekFilter != null ? idx : (idx % 24);
        const entry = {
          bucketIndex: idx,
          etDay: dayLabels[dow],
          etHour: hour,
          label: `${dayLabels[dow]} ${String(hour).padStart(2, '0')}:00 ET`,
          ...summarize(b.rows),
        };
        if (includeSportBreakdown && b.rows.length > 0) {
          entry.bySport = {};
          for (const [sp, lst] of Object.entries(b.bySport)) {
            entry.bySport[sp] = summarize(lst);
          }
        }
        return entry;
      });

      res.json({
        ok: true,
        window: { from: fromIso, to: toIso, days },
        sportFilter,
        dayOfWeekFilter,
        includeSportBreakdown,
        totalRowsScanned: rows.length,
        totalRowsBucketed: filtered,
        series,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Freeze a baseline snapshot for before/after comparison.
  // POST /latency-baseline/capture → freeze current stats as baseline
  // GET  /latency-baseline         → return frozen baseline + current delta
  app.post('/latency-baseline/capture', (req, res) => {
    const snap = websocket.captureBaseline();
    res.json({ ok: true, baseline: snap });
  });
  app.get('/latency-baseline', (req, res) => {
    const baseline = websocket.getBaseline();
    const current = websocket.getLatencyBreakdown();
    // Compute deltas on end-to-end percentiles for quick comparison
    let delta = null;
    if (baseline && baseline.endToEnd && current.endToEnd) {
      const pct = (key) => {
        const b = baseline.endToEnd[key];
        const c = current.endToEnd[key];
        if (b == null || c == null) return null;
        return { baseline: b, current: c, delta: c - b, pctChange: b > 0 ? Math.round((c - b) / b * 1000) / 10 + '%' : null };
      };
      delta = {
        p50: pct('p50'),
        p75: pct('p75'),
        p90: pct('p90'),
        p95: pct('p95'),
        p99: pct('p99'),
        avg: pct('avg'),
      };
    }
    res.json({ baseline, current, delta });
  });

  // Manual refresh odds
  app.post('/refresh-odds', async (req, res) => {
    try {
      const results = await oddsFeed.refreshAllSports();
      res.json({ ok: true, results });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Manual refresh lines
  app.post('/refresh-lines', async (req, res) => {
    try {
      const stats = await lineManager.refreshLines();
      res.json({ ok: true, stats });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Coverage audit: for each PX event in the next N hours, compare PX-published
  // markets against what we've registered + what's in our odds cache. Surfaces
  // the kind of silent gap where lines exist on PX with full market data but
  // we're not registering them (e.g. wrong sport-key match, late market
  // publication, team-name mismatch, market-name allowlist miss).
  //
  // Response per event:
  //   pxMarketTypes        — set of normalized market types PX returned
  //   registeredCounts     — count of registered lines per market type
  //   missingMarketTypes   — PX has it, we don't
  //   cachePresence        — which oddsCache sport keys hold this event
  //   registeredSportKey   — what sport key we stored lines under
  //   gap                  — coarse boolean flag: any missing markets?
  //
  // Use ?windowHours=24 to control the look-ahead. Default 24h.
  // Health-coverage matrix — compact sport × marketType summary suitable
  // for dashboard alerting. Lightweight (no PX call): operates entirely
  // on the in-memory line index. Returns a status flag per sport based
  // on whether expected markets (per a static EXPECTED_MARKETS_BY_SPORT
  // map) have at least one line registered. Intended as a structural
  // backstop so coverage regressions surface without manual scanning.
  app.get('/health/coverage', (req, res) => {
    try {
      const idx = lineManager.__debugGetLineIndex();
      const sportMatrix = {};
      const sportEvents = {};
      for (const lineId of Object.keys(idx)) {
        const info = idx[lineId];
        const sport = info.sport || '?';
        const mkt = info.marketType || '?';
        if (!sportMatrix[sport]) sportMatrix[sport] = {};
        sportMatrix[sport][mkt] = (sportMatrix[sport][mkt] || 0) + 1;
        if (!sportEvents[sport]) sportEvents[sport] = new Set();
        if (info.pxEventId) sportEvents[sport].add(info.pxEventId);
      }

      // Expected markets per sport (operator's coverage scope).
      // Sport with zero events today is considered "no schedule"
      // (status=idle), not a coverage gap. Sport with events but
      // missing one or more expected markets is status=gap.
      const EXPECTED_MARKETS_BY_SPORT = {
        baseball_mlb: ['moneyline', 'spread', 'total', 'team_total',
          'first_5_innings_moneyline', 'first_5_innings_run_line', 'first_5_innings_total',
          'player_hitter_hits', 'player_hitter_total_bases', 'player_hitter_rbi_runs'],
        basketball_nba: ['moneyline', 'spread', 'total', 'team_total',
          'first_half_moneyline', 'first_half_spread', 'first_half_total',
          'player_points', 'player_rebounds', 'player_assists', 'player_threes_made'],
        basketball_wnba: ['moneyline', 'spread', 'total'],
        icehockey_nhl: ['moneyline', 'spread', 'total', 'team_total', 'player_shots_on_goal'],
        tennis: ['moneyline'],
        soccer_epl: ['moneyline', 'spread', 'total'],
        soccer_uefa_champs_league: ['moneyline', 'spread', 'total'],
        soccer_spain_la_liga: ['moneyline', 'spread', 'total'],
        soccer_italy_serie_a: ['moneyline', 'spread', 'total'],
        soccer_germany_bundesliga: ['moneyline', 'spread', 'total'],
        soccer_france_ligue_one: ['moneyline', 'spread', 'total'],
        soccer_usa_mls: ['moneyline', 'spread', 'total'],
        soccer_brazil_campeonato: ['moneyline', 'spread', 'total'],
        mma_mixed_martial_arts: ['moneyline', 'total'],
        boxing_boxing: ['moneyline'],
        golf_matchups: ['moneyline'],
      };

      const sports = [];
      let totalGaps = 0;
      for (const sport of Object.keys(EXPECTED_MARKETS_BY_SPORT)) {
        const expected = EXPECTED_MARKETS_BY_SPORT[sport];
        const matrix = sportMatrix[sport] || {};
        const eventCount = (sportEvents[sport] && sportEvents[sport].size) || 0;
        const totalLines = Object.values(matrix).reduce((s, n) => s + n, 0);
        const missing = expected.filter(m => !(matrix[m] > 0));
        let status;
        if (eventCount === 0) status = 'idle';            // no schedule, not a gap
        else if (missing.length === 0) status = 'ok';      // all expected markets registered
        else status = 'gap';                               // events exist but expected markets missing
        if (status === 'gap') totalGaps += missing.length;
        sports.push({
          sport,
          status,
          eventCount,
          totalLines,
          marketCounts: matrix,
          missing,
        });
      }

      // Surface any sports we have lines for but isn't in our expected map
      // — discovered ad-hoc through PX RFQs. Worth knowing about so we
      // can decide to add to the expected set.
      for (const sport of Object.keys(sportMatrix)) {
        if (EXPECTED_MARKETS_BY_SPORT[sport]) continue;
        const matrix = sportMatrix[sport];
        const eventCount = (sportEvents[sport] && sportEvents[sport].size) || 0;
        const totalLines = Object.values(matrix).reduce((s, n) => s + n, 0);
        sports.push({
          sport, status: 'unexpected',
          eventCount, totalLines, marketCounts: matrix, missing: [],
        });
      }

      sports.sort((a, b) => a.sport.localeCompare(b.sport));
      res.json({
        ok: true,
        timestamp: new Date().toISOString(),
        summary: {
          sportsTracked: Object.keys(EXPECTED_MARKETS_BY_SPORT).length,
          sportsOk: sports.filter(s => s.status === 'ok').length,
          sportsIdle: sports.filter(s => s.status === 'idle').length,
          sportsWithGaps: sports.filter(s => s.status === 'gap').length,
          totalMissingMarkets: totalGaps,
        },
        sports,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.get('/coverage-audit', async (req, res) => {
    try {
      const windowHours = Math.min(168, Math.max(1, parseInt(req.query.windowHours) || 24));
      const sportFilter = req.query.sport || null; // optional PX sport_name filter (e.g. "Soccer")
      const onlyGaps = req.query.onlyGaps !== '0';

      const allEvents = await px.fetchSportEvents();
      const pxSportNames = Object.values(config.sportNameMap);
      const nowMs = Date.now();
      const windowEndMs = nowMs + windowHours * 3600 * 1000;

      // Filter to supported, non-settled, in-window
      const events = allEvents.filter(e => {
        if (!pxSportNames.includes(e.sport_name)) return false;
        if (e.status === 'settled') return false;
        if (sportFilter && e.sport_name !== sportFilter) return false;
        if (!e.scheduled) return true; // include events without scheduled
        const t = new Date(e.scheduled).getTime();
        return Number.isFinite(t) && t >= nowMs && t <= windowEndMs;
      });

      const idx = lineManager.__debugGetLineIndex();
      // Group lineIndex by pxEventId for O(1) lookup
      const linesByPxEventId = {};
      for (const lineId of Object.keys(idx)) {
        const info = idx[lineId];
        const eid = info.pxEventId;
        if (!eid) continue;
        if (!linesByPxEventId[eid]) linesByPxEventId[eid] = [];
        linesByPxEventId[eid].push(info);
      }

      // Map PX market.type → normalized base type we register under
      const normalizeMarketType = (mtype, mname) => {
        const t = (mtype || '').toLowerCase();
        const n = (mname || '').toLowerCase();
        // Sub-game markets we don't register on the full-game side
        if (/first\s*5|1st.5|f5/.test(n)) return null;
        if (/first\s*half|1st\s*half|h1\b/.test(n)) return null;
        if (/quarter|period|inning/.test(n)) return null;
        if (/player|prop|home runs|strikeouts|points|rebounds|assists/.test(n)) return null;
        if (t === 'moneyline' || /\bmoneyline\b|\bdraw\s*no\s*bet\b|\bdnb\b/.test(n)) return 'moneyline';
        if (t === 'spread' || /\bspread\b|run line|puck line|game spread|point spread/.test(n)) return 'spread';
        if (t === 'total' || /\btotal\b/.test(n)) {
          if (/team total/.test(n)) return 'team_total';
          return 'total';
        }
        return null;
      };

      const oddsCacheKeys = Object.entries(config.sportNameMap)
        .filter(([, v]) => true)
        .map(([k]) => k);

      const _isGenericKey = (k) => !k.includes('_') || k === 'mma_mixed_martial_arts' || k === 'boxing_boxing';

      const results = [];
      let processed = 0;
      const maxToProbe = Math.min(events.length, 200); // cap to avoid runaway PX calls

      for (const ev of events.slice(0, maxToProbe)) {
        processed++;
        // Fetch PX markets per event
        let pxMarkets = [];
        try {
          pxMarkets = await px.fetchMarkets(ev.event_id);
        } catch (err) {
          results.push({
            pxEventId: ev.event_id,
            name: ev.name,
            sportName: ev.sport_name,
            scheduled: ev.scheduled,
            error: `fetchMarkets: ${err.message}`,
            gap: true,
          });
          continue;
        }

        const pxMarketTypes = new Set();
        const pxMarketDetail = [];
        for (const m of (pxMarkets || [])) {
          const norm = normalizeMarketType(m.type, m.name);
          pxMarketDetail.push({ type: m.type, name: m.name, normalized: norm });
          if (norm) pxMarketTypes.add(norm);
        }

        // Registered lines (lineIndex)
        const registered = linesByPxEventId[ev.event_id] || [];
        const registeredCounts = {};
        const registeredSportKeys = new Set();
        for (const r of registered) {
          const mt = r.marketType || 'unknown';
          registeredCounts[mt] = (registeredCounts[mt] || 0) + 1;
          if (r.sport) registeredSportKeys.add(r.sport);
        }

        const registeredMarketTypes = new Set(Object.keys(registeredCounts));
        const missingMarketTypes = [...pxMarketTypes].filter(t => !registeredMarketTypes.has(t));

        // Cache presence per candidate sport key (for the configured PX sport_name)
        const possibleSportKeys = Object.entries(config.sportNameMap)
          .filter(([, v]) => v === ev.sport_name)
          .map(([k]) => k);
        const homeComp = (ev.competitors || []).find(c => c.side === 'home') || (ev.competitors || [])[0];
        const awayComp = (ev.competitors || []).find(c => c.side === 'away') || (ev.competitors || [])[1];
        const cachePresence = [];
        for (const sk of possibleSportKeys) {
          const evt = oddsFeed.getEventMarkets(sk, homeComp?.name, awayComp?.name, ev.scheduled)
            || oddsFeed.getEventMarkets(sk, awayComp?.name, homeComp?.name, ev.scheduled);
          if (evt) {
            cachePresence.push({
              sportKey: sk,
              isGeneric: _isGenericKey(sk),
              cachedHome: evt.homeTeam,
              cachedAway: evt.awayTeam,
              commenceTime: evt.commenceTime,
              markets: Object.keys(evt.markets || {}),
            });
          }
        }

        const gap = missingMarketTypes.length > 0
          || (registered.length === 0 && pxMarketTypes.size > 0)
          || ([...registeredSportKeys].some(_isGenericKey) && cachePresence.some(c => !c.isGeneric));

        const entry = {
          pxEventId: ev.event_id,
          sportName: ev.sport_name,
          name: ev.name,
          scheduled: ev.scheduled,
          hoursUntilStart: ev.scheduled
            ? Math.round((new Date(ev.scheduled).getTime() - nowMs) / 36e5 * 10) / 10
            : null,
          pxMarketTypes: [...pxMarketTypes].sort(),
          pxMarketDetail,
          registeredCounts,
          registeredSportKeys: [...registeredSportKeys].sort(),
          missingMarketTypes,
          cachePresence,
          gap,
        };
        if (!onlyGaps || gap) results.push(entry);
        // Light throttle so we don't burst PX
        await new Promise(r => setTimeout(r, 50));
      }

      const summary = {
        totalEventsInWindow: events.length,
        eventsProbed: processed,
        eventsWithGaps: results.filter(r => r.gap).length,
        eventsClean: results.filter(r => !r.gap).length,
      };

      res.json({
        ok: true,
        windowHours,
        sportFilter,
        onlyGaps,
        now: new Date(nowMs).toISOString(),
        summary,
        events: results,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Refresh live odds for in-progress games and update weighted exposure
  app.post('/refresh-live-odds', async (req, res) => {
    try {
      const result = await orderTracker.refreshLiveOdds(oddsFeed);
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Diagnostic: per-sport live-cache status + a sample lookup on an
  // in-progress leg, to debug why liveFairProb may not be hitting.
  app.get('/debug/live-odds-coverage', (req, res) => {
    try {
      const status = oddsFeed.getLiveCacheStatus();
      const now = Date.now();
      const legsByStatus = { covered: 0, uncovered: 0, noSport: 0, notInProgress: 0 };
      const uncoveredSamples = [];
      for (const o of Object.values(orderTracker.getRecentOrders(5000))) {
        if (o.status !== 'confirmed') continue;
        if (o.meta && o.meta.phantom) continue;
        const legs = o.legs || (o.meta && o.meta.legs) || [];
        for (const l of legs) {
          const sport = l.sport || l.oddsApiSport;
          if (!sport) { legsByStatus.noSport++; continue; }
          const st = l.startTime ? new Date(l.startTime).getTime() : null;
          if (!st || isNaN(st)) { legsByStatus.noSport++; continue; }
          const elapsed = now - st;
          if (elapsed < 0 || elapsed > 6 * 60 * 60 * 1000) { legsByStatus.notInProgress++; continue; }
          if (l.liveFairProb != null) { legsByStatus.covered++; continue; }
          legsByStatus.uncovered++;
          if (uncoveredSamples.length < 10) {
            const probeMarket = l.oddsApiMarket || (l.market === 'moneyline' ? 'h2h' : l.market === 'total' ? 'totals' : l.market === 'spread' ? 'spreads' : l.market);
            const probed = oddsFeed.getLiveFairProb(
              sport, l.homeTeam, l.awayTeam, probeMarket, l.oddsApiSelection || l.selection,
              l.line != null ? Math.abs(l.line) : null, l.startTime
            );
            uncoveredSamples.push({
              team: l.team || l.teamName || '?', market: l.market, line: l.line,
              sport, home: l.homeTeam, away: l.awayTeam, startTime: l.startTime,
              probeResult: probed != null ? 'MATCH ' + probed.toFixed(4) : 'no-match',
            });
          }
        }
      }
      res.json({
        ok: true,
        liveCacheStatus: status,
        legsByStatus,
        coveragePct: legsByStatus.covered + legsByStatus.uncovered > 0
          ? Math.round(legsByStatus.covered / (legsByStatus.covered + legsByStatus.uncovered) * 100)
          : 0,
        uncoveredSamples,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Enrich reconstructed orders by looking up lineIds in the current lineManager.
  // Reconstructed orders are ones rebuilt from PX settlement data when we missed
  // the WS confirmation event — their legs initially have team='?' etc.
  app.post('/enrich-reconstructed', async (req, res) => {
    try {
      const result = await orderTracker.enrichReconstructedOrders();
      // Rebuild exposure now that legs have team data — restores Team
      // Exposure rows that were collapsed because the reconstructed legs
      // had team='?'.
      orderTracker.rebuildAllExposure();
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Deep enrichment: fetch historical markets from PX for each pxEventId on
  // reconstructed orders and resolve team names directly from the PX API.
  app.post('/enrich-reconstructed-deep', async (req, res) => {
    try {
      const result = await orderTracker.enrichReconstructedFromPx();
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Delete settled orders with all-unknown ('?') selections
  app.post('/delete-unknown-settled', async (req, res) => {
    try {
      const result = await orderTracker.deleteUnknownSettledOrders();
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // One-time cleanup of false-fill 'confirmed' rows produced by the
  // pre-Apr-25 recordMatchedParlay bug (promoted on every order.matched
  // broadcast, including other-SP wins). Defaults to dry-run; pass
  // ?apply=1 to actually rewrite the rows. Use ?days=N to widen window
  // (default 30).
  app.post('/clean-false-confirms', async (req, res) => {
    try {
      const dryRun = !(req.query.apply === '1' || req.query.apply === 'true');
      const days = req.query.days != null ? Math.max(1, Number(req.query.days)) : 30;
      const fromIso = new Date(Date.now() - days * 24 * 3600 * 1000).toISOString();
      const result = await orderTracker.cleanFalseConfirms({ dryRun, fromIso });
      res.json({ ok: true, ...result });
    } catch (err) {
      log.error('API', `/clean-false-confirms failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Nuclear reset: truncate all tables and clear in-memory state
  app.post('/reset-all', async (req, res) => {
    try {
      const db = require('./services/db');
      const client = db.getClient();
      if (client) {
        const r1 = await client.from('parlay_orders').delete().neq('parlay_id', '');
        if (r1.error) log.warn('Reset', `parlay_orders: ${r1.error.message}`);
        const r2 = await client.from('matched_parlays').delete().neq('id', 0);
        if (r2.error) log.warn('Reset', `matched_parlays: ${r2.error.message}`);
        const r3 = await client.from('declines').delete().neq('id', 0);
        if (r3.error) log.warn('Reset', `declines: ${r3.error.message}`);
      }
      const result = { dbCleared: true, note: 'Restart service for full in-memory reset' };
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // DB vs in-memory diagnostic: shows DB row counts alongside loaded counts
  app.get('/db-diag', async (req, res) => {
    try {
      const db = require('./services/db');
      const dbCounts = await db.countOrders();
      const s = orderTracker.getStats();
      res.json({
        db: dbCounts,
        inMemory: {
          totalOrders: s.totalOrders,
          activeOrders: s.activeOrders,
          settlements: s.totalSettlements,
          confirmations: s.totalConfirmations,
          wins: s.totalWins,
          losses: s.totalLosses,
        },
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Manual settlement poll
  // Diagnostic: raw PX order data by UUID
  app.get('/debug/px-order', async (req, res) => {
    try {
      const uuid = req.query.uuid;
      if (!uuid) return res.status(400).json({ error: 'uuid query param required' });
      // Use the full cached pxLedger (up to 5000) so we catch older
      // parlays that aren't in the first 500 returned by fetchOrders.
      const ledger = await pxLedger.fetchLedger();
      const orders = ledger.ledger || [];
      const match = orders.find(o =>
        o.order_uuid === uuid ||
        o.p_id === uuid ||
        (o.order_uuid && o.order_uuid.startsWith(uuid)) ||
        (o.p_id && o.p_id.startsWith(uuid))
      );
      if (!match) return res.json({ error: 'Order not found in PX', searched: orders.length });
      res.json({ ok: true, pxOrder: match });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // PX-native ledger: pulls the full order history from PX and computes
  // net settlement P&L using ONLY PX's own status/stake/profit fields —
  // no tracker interpretation. Use this to reconcile against our
  // runningPnL when account-balance vs tracker-P&L disagree.
  // DK-scraped playoff series winner odds. Bypasses Akamai via
  // Puppeteer (our feeds don't carry per-series markets).
  app.get('/nba-series-prices', async (req, res) => {
    try {
      const force = req.query.force === '1' || req.query.force === 'true';
      const data = await dkScraper.fetchNbaSeriesWinners({ force });
      res.json({ ok: true, ...data });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });
  app.get('/nhl-series-prices', async (req, res) => {
    try {
      const force = req.query.force === '1' || req.query.force === 'true';
      const data = await dkScraper.fetchNhlSeriesWinners({ force });
      res.json({ ok: true, ...data });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });
  // Series spread + total-games cache views. fetchSeriesMarkets warms
  // all three (winner / spread / total-games) in one Puppeteer run, so
  // these endpoints share the same cache and Puppeteer cost.
  app.get('/nba-series-spreads', async (req, res) => {
    try {
      const force = req.query.force === '1' || req.query.force === 'true';
      const data = await dkScraper.fetchSeriesMarkets('nba', { force });
      res.json({ ok: true, fetchedAt: data.fetchedAt, sport: data.sport, spreads: data.spreads });
    } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
  });
  app.get('/nhl-series-spreads', async (req, res) => {
    try {
      const force = req.query.force === '1' || req.query.force === 'true';
      const data = await dkScraper.fetchSeriesMarkets('nhl', { force });
      res.json({ ok: true, fetchedAt: data.fetchedAt, sport: data.sport, spreads: data.spreads });
    } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
  });
  app.get('/nba-series-totals', async (req, res) => {
    try {
      const force = req.query.force === '1' || req.query.force === 'true';
      const data = await dkScraper.fetchSeriesMarkets('nba', { force });
      res.json({ ok: true, fetchedAt: data.fetchedAt, sport: data.sport, totals: data.totals });
    } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
  });
  app.get('/nhl-series-totals', async (req, res) => {
    try {
      const force = req.query.force === '1' || req.query.force === 'true';
      const data = await dkScraper.fetchSeriesMarkets('nhl', { force });
      res.json({ ok: true, fetchedAt: data.fetchedAt, sport: data.sport, totals: data.totals });
    } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
  });
  app.get('/mma-fight-odds', async (req, res) => {
    try {
      const force = req.query.force === '1' || req.query.force === 'true';
      const data = await dkScraper.fetchMmaFightOdds({ force });
      res.json({ ok: true, ...data });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });
  // ESPN scoreboard cache snapshot — diagnostic. Shows every sport key,
  // its game count, completed-game count, fetched timestamp, and the
  // first 50 games' raw shape (homeTeam, awayTeam, completed, scores,
  // status). Use to verify ESPN coverage matches expectations after
  // deploy and confirm a specific fight / match was picked up.
  app.get('/debug-espn-scores', (req, res) => {
    try {
      const espnScores = require('./services/espn-scores');
      const sport = req.query.sport;
      const dump = espnScores.__debugDump();
      res.json({ ok: true, ...(sport ? { [sport]: dump[sport] } : { sports: dump }) });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Diagnostic-only mirror of the MMA scraper. Captures nav trail, final
  // URL, page title, visible event-row sample, every primaryMarkets XHR
  // URL with sport hints, and counts of MMA-vs-cross-sport XHR pollution.
  // Use ?url=... to override the navigation target (e.g. probe a card-
  // specific page like /event/UFC-Macao-2026).
  app.get('/debug-dk-mma-state', async (req, res) => {
    try {
      const url = req.query.url || undefined;
      const state = await dkScraper.debugMmaScraperState({ url });
      res.json({ ok: true, ...state });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });
  // DK golf matchups (PGA tour team + individual H2H). Covers Zurich
  // Classic team pairs and any PGA event DataGolf doesn't publish.
  // `?force=1` bypasses the 15-min scraper cache and hits DK fresh.
  app.get('/golf-matchups', async (req, res) => {
    try {
      const force = req.query.force === '1' || req.query.force === 'true';
      const data = await dkScraper.fetchGolfMatchups({ force });
      res.json({ ok: true, ...data });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });
  // BetOnline Zurich Classic matchups (this-week-only scraper).
  // DataGolf and DK don't publish the team pairings PX uses for Zurich;
  // BetOnline mirrors PX's pairings. `?force=1` busts the 15-min cache.
  app.get('/betonline-zurich', async (req, res) => {
    try {
      const betonlineScraper = require('./services/betonline-scraper');
      const force = req.query.force === '1' || req.query.force === 'true';
      const data = await betonlineScraper.fetchZurichMatchups({ force });
      res.json({ ok: true, ...data });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });
  // Manual upload path. Scraper is blocked (anti-bot); operator
  // supplies odds by hand as a one-shot static snapshot. Prices
  // don't move much between Wed posting and Thu tee-off for a
  // team event so a snapshot is good enough. POST body:
  //   { scope: 'tournament'|'round_1', text: "<raw paste>" } OR
  //   { scope: ..., matchups: [{teamA,oddsA,teamB,oddsB}, ...] }
  app.post('/betonline-zurich/upload', (req, res) => {
    try {
      const betonlineScraper = require('./services/betonline-scraper');
      const result = betonlineScraper.loadManualMatchups(req.body || {});
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(400).json({ ok: false, error: err.message });
    }
  });

  // Authoritative PX-native P&L. Pulls full order history from PX and
  // aggregates realized P&L, open exposure, and net balance impact
  // directly from PX's profit + settlement_status fields. This is the
  // source of truth — the in-memory tracker misses silent losses.
  app.get('/px-pnl', async (req, res) => {
    try {
      const force = req.query.force === '1' || req.query.force === 'true';
      const summary = await pxLedger.getSummary({ force });
      res.json({ ok: true, ...summary });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Authoritative open-positions list. Pulls PX's finalized (unsettled)
  // orders and joins with tracker leg data. Replaces the old path of
  // filtering /orders by status==='confirmed' which includes silent-loss
  // ghosts that reconcile hasn't swept yet.
  app.get('/px-positions', async (req, res) => {
    try {
      const ledger = await pxLedger.fetchLedger();
      const pxOpen = (ledger.ledger || []).filter(po => {
        const st = (po.status || '').toLowerCase();
        const ss = (po.settlement_status || '').toLowerCase();
        if (st === 'finalized') return true;
        if (st === 'settled' && !['won', 'lost', 'push'].includes(ss)) return true;
        return false;
      });
      // Track which parlays PX already surfaced so we can avoid
      // double-counting when we union with the tracker's confirmed set.
      const coveredParlayIds = new Set();
      const coveredUuids = new Set();
      // Also build a set of PX UUIDs in terminal states (settled won/
      // lost/push, rejected, failed) so we never re-surface a stale
      // tracker-confirmed ghost that's already resolved on PX side.
      const terminalUuids = new Set();
      for (const po of (ledger.ledger || [])) {
        const st = (po.status || '').toLowerCase();
        const ss = (po.settlement_status || '').toLowerCase();
        const isTerminal = (st === 'settled' && ['won','lost','push'].includes(ss))
                        || st === 'rejected' || st === 'failed';
        if (isTerminal && po.order_uuid) terminalUuids.add(po.order_uuid);
      }

      const positions = pxOpen.map(po => {
        const uuid = po.order_uuid || po.orderUuid;
        const tracked = uuid ? orderTracker.getOrderByUuid(uuid) : null;
        if (tracked?.parlayId) coveredParlayIds.add(tracked.parlayId);
        if (uuid) coveredUuids.add(uuid);
        return {
          parlayId: tracked?.parlayId || uuid,
          orderUuid: uuid,
          status: 'confirmed',
          confirmedStake: Number(po.confirmed_stake ?? po.stake ?? 0),
          confirmedOdds: tracked?.confirmedOdds ?? null,
          offeredOdds: tracked?.offeredOdds ?? null,
          fairParlayProb: tracked?.fairParlayProb ?? null,
          maxRisk: tracked?.maxRisk ?? null,
          confirmedAt: tracked?.confirmedAt || po.created_at || po.updated_at || null,
          quotedAt: tracked?.quotedAt || null,
          legs: tracked?.legs || tracked?.meta?.legs || [],
          meta: tracked?.meta || {},
          pxSource: 'px',
        };
      });

      // Union with tracker-confirmed orders PX hasn't surfaced via its
      // orders endpoint. Observed live: a freshly-confirmed parlay
      // (e.g. Fulham/Newcastle, 9s confirm latency) was present in our
      // tracker but absent from PX's /parlay/sp/orders response. Those
      // positions are real and should show in Open Positions alongside
      // PX-sourced ones. We gate on the tracker's own guard rails:
      //   - status === 'confirmed'
      //   - not flagged phantom via ghost-reconcile
      //   - orderUuid (if present) not in PX's terminal set
      // Sorted tracker-only positions appear at the top so newest are
      // visible first.
      const trackerAdds = [];
      for (const o of orderTracker.getRecentOrders(500)) {
        if (o.status !== 'confirmed') continue;
        // Require orderUuid — same gate as the client-side trackerConfirmed
        // filter (client/index.html line 3192) and the truth model
        // established Apr 25 (orderUuid only set on order.finalized, i.e.
        // after the bettor accepts on PX). Without this check, the
        // 'Offered' transient state (status='confirmed', no UUID, awaiting
        // accept) would surface in Open Positions for ~10 min then vanish
        // when isOrderStalePhantom started catching them — operator-visible
        // flicker reported on Apr 26. The isOrderStalePhantom check below
        // only kicks in AFTER 10 min, so it can't catch fresh no-UUID
        // entries on its own.
        if (!o.orderUuid) continue;
        // Apply the same stale/phantom filter getTotalPortfolioRisk uses.
        // isOrderStalePhantom catches three failure modes that plain
        // meta.phantom misses:
        //   1. Explicit ghost-reconcile tag (meta.phantom=true)
        //   2. 'confirmed' with no orderUuid >10min — finalize event
        //      never arrived, fill almost certainly not placed on PX
        //   3. Every leg started >12h ago — stuck settlement / zombie
        // Without this filter, Open Positions was showing hundreds of
        // stale tracker-only ghosts totaling $14k+ that /status's
        // Deployed figure (already filtered) correctly excluded. Result:
        // sum(My Risk) in the table was ~3x Deployed, which it shouldn't
        // be — both are meant to represent the same thing.
        if (orderTracker.isOrderStalePhantom(o)) continue;
        if (o.orderUuid && terminalUuids.has(o.orderUuid)) continue;
        if (coveredParlayIds.has(o.parlayId)) continue;
        if (o.orderUuid && coveredUuids.has(o.orderUuid)) continue;
        trackerAdds.push({
          parlayId: o.parlayId,
          orderUuid: o.orderUuid || null,
          status: 'confirmed',
          confirmedStake: Number(o.confirmedStake || 0),
          confirmedOdds: o.confirmedOdds ?? null,
          offeredOdds: o.offeredOdds ?? null,
          fairParlayProb: o.fairParlayProb ?? null,
          maxRisk: o.maxRisk ?? null,
          confirmedAt: o.confirmedAt || null,
          quotedAt: o.quotedAt || null,
          legs: o.legs || o.meta?.legs || [],
          meta: o.meta || {},
          pxSource: 'tracker',
        });
      }
      const merged = [...positions, ...trackerAdds];
      res.json({
        ok: true,
        count: merged.length,
        pxCount: positions.length,
        trackerOnlyCount: trackerAdds.length,
        totalStake: merged.reduce((s, p) => s + (p.confirmedStake || 0), 0),
        positions: merged,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.get('/debug/px-ledger', async (req, res) => {
    const limit = parseInt(req.query.limit) || 50000;
    try {
      const pxOrders = await px.fetchOrders(limit);
      const byStatus = {};
      const bySettlementStatus = {};
      let totalStakeIn = 0;
      let totalPxProfit = 0;
      let sumPositive = 0;  // sum of profit on wins only
      let sumNegative = 0;  // sum of profit on losses only (how much we paid out net)
      let sumWinStakes = 0, sumLossStakes = 0, sumPushStakes = 0;
      let countWin = 0, countLoss = 0, countPush = 0, countOther = 0;
      let unsettled = 0;
      let samples = { wins: [], losses: [], pushes: [], other: [] };
      for (const po of pxOrders) {
        const st = (po.status || 'unknown').toLowerCase();
        byStatus[st] = (byStatus[st] || 0) + 1;
        const pxProfit = po.profit ?? po.net_profit ?? po.settlement_profit ?? po.payout ?? null;
        const stake = po.stake ?? po.confirmed_stake ?? po.matched_stake ?? 0;
        const settlementStatus = (po.settlement_status || po.settlementStatus || '').toLowerCase();
        const isSettled = /settled|won|lost|push/.test(st);
        if (isSettled) {
          bySettlementStatus[settlementStatus || '(none)'] = (bySettlementStatus[settlementStatus || '(none)'] || 0) + 1;
          const profit = pxProfit != null ? Number(pxProfit) : 0;
          totalPxProfit += profit;
          totalStakeIn += Number(stake);
          if (/won|win/.test(settlementStatus)) { countWin++; sumWinStakes += Number(stake); if (profit > 0) sumPositive += profit; }
          else if (/lost|loss/.test(settlementStatus)) { countLoss++; sumLossStakes += Number(stake); if (profit < 0) sumNegative += profit; }
          else if (/push/.test(settlementStatus)) { countPush++; sumPushStakes += Number(stake); }
          else { countOther++; }

          const group = /won|win/.test(settlementStatus) ? 'wins'
                      : /lost|loss/.test(settlementStatus) ? 'losses'
                      : /push/.test(settlementStatus) ? 'pushes'
                      : 'other';
          if (samples[group].length < 5) samples[group].push({
            uuid: po.order_uuid?.substring(0, 8),
            status: st,
            settlementStatus,
            stake,
            profit: pxProfit,
            availableFields: Object.keys(po).filter(k => /profit|payout|settle|pnl|return|stake/i.test(k)),
          });
        } else {
          unsettled++;
        }
      }
      res.json({
        ok: true,
        pxTotalOrders: pxOrders.length,
        statusBreakdown: byStatus,
        settlementStatusBreakdown: bySettlementStatus,
        unsettledCount: unsettled,
        counts: { wins: countWin, losses: countLoss, pushes: countPush, other: countOther },
        totalStakeIn: Math.round(totalStakeIn * 100) / 100,
        totalPxProfit: Math.round(totalPxProfit * 100) / 100,
        sumProfitOnWins: Math.round(sumPositive * 100) / 100,
        sumProfitOnLosses: Math.round(sumNegative * 100) / 100,
        sumStakesByOutcome: {
          wins: Math.round(sumWinStakes * 100) / 100,
          losses: Math.round(sumLossStakes * 100) / 100,
          pushes: Math.round(sumPushStakes * 100) / 100,
        },
        samples,
        note: 'totalPxProfit = sum of all profit fields. If settlementStatusBreakdown shows only "won" and no "lost", PX\'s profit field may only be populated on wins and losses subtract from balance separately — in which case net P&L = sumProfitOnWins - sumLossStakes.',
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Diagnostic: force a full fetchOddsForSport for a specific sport
  app.get('/debug/force-fetch', async (req, res) => {
    try {
      const sport = req.query.sport;
      if (!sport) return res.status(400).json({ ok: false, error: 'sport query param required' });
      const result = await oddsFeed.fetchOddsForSport(sport);
      const eventCount = result ? Object.keys(result).length : 0;
      // Sample events
      const sample = [];
      if (result) {
        for (const [key, entry] of Object.entries(result).slice(0, 5)) {
          const events = Array.isArray(entry) ? entry : [entry];
          for (const e of events) {
            sample.push({
              home: e.homeTeam,
              away: e.awayTeam,
              markets: Object.keys(e.markets || {}),
              commenceTime: e.commenceTime,
            });
          }
        }
      }
      res.json({ ok: true, sport, eventCount, sample });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack?.split('\n').slice(0, 5) });
    }
  });

  // Diagnostic: fetch any SharpAPI sport+market combo using the production query
  app.get('/debug/raw-fetch', async (req, res) => {
    try {
      const fetch = require('node-fetch');
      const cfg = config.config || config;
      const sport = req.query.sport || 'baseball_mlb';
      const market = req.query.market || 'moneyline';
      // Map our sport key to SharpAPI params
      const mapping = {
        'basketball_nba': { param: 'league', value: 'nba' },
        'basketball_ncaab': { param: 'league', value: 'ncaab' },
        'baseball_mlb': { param: 'league', value: 'mlb' },
        'icehockey_nhl': { param: 'league', value: 'nhl' },
        'soccer': { param: 'sport', value: 'soccer' },
        'tennis': { param: 'sport', value: 'tennis' },
      }[sport] || { param: 'league', value: sport };
      const url = `${cfg.oddsApi.baseUrl}/odds?${mapping.param}=${mapping.value}&market=${market}&live=false&limit=500`;
      const resp = await fetch(url, { headers: { 'X-API-Key': cfg.oddsApi.apiKey } });
      const body = await resp.json();
      const rows = body.data || [];
      // Summarize
      const byEvent = {};
      const bySelType = {};
      const byBook = {};
      for (const r of rows) {
        byBook[r.sportsbook] = (byBook[r.sportsbook] || 0) + 1;
        bySelType[r.selection_type] = (bySelType[r.selection_type] || 0) + 1;
        const key = (r.home_team || '?') + ' vs ' + (r.away_team || '?');
        if (!byEvent[key]) byEvent[key] = new Set();
        byEvent[key].add(r.sportsbook);
      }
      res.json({
        url,
        totalRows: rows.length,
        totalEvents: Object.keys(byEvent).length,
        byBook,
        bySelType,
        sampleEvents: Object.entries(byEvent).slice(0, 20).map(([k, v]) => ({ event: k, books: [...v] })),
        sampleRows: rows.slice(0, 3),
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Dump the raw cached odds for a specific event. Shows every row we've
  // accumulated from all sources (SharpAPI + Odds API supplement + deltas).
  app.get('/debug/event-raw', (req, res) => {
    try {
      const sport = req.query.sport || 'baseball_mlb';
      const team = (req.query.team || '').toLowerCase();
      if (!team) return res.status(400).json({ error: 'team required' });
      const cache = oddsFeed.getRawCache?.();
      // Access internal cache via the module — use require to get fresh reference
      const of = require('./services/odds-feed');
      const sportCache = of.__debugGetCache ? of.__debugGetCache(sport) : null;
      if (!sportCache) return res.status(500).json({ error: 'cache accessor missing' });
      const events = Object.values(sportCache.events || {}).flat();
      const ev = events.find(e => (e.homeTeam || '').toLowerCase().includes(team) || (e.awayTeam || '').toLowerCase().includes(team));
      if (!ev) return res.status(404).json({ error: 'not found' });
      // Summarize _rawOdds: count per book + market_type, plus all rows
      const byBookMarket = {};
      const mlRows = [];
      for (const r of (ev._rawOdds || [])) {
        const k = r.sportsbook + '|' + r.market_type;
        byBookMarket[k] = (byBookMarket[k] || 0) + 1;
        if (r.market_type === 'moneyline') {
          mlRows.push({ sb: r.sportsbook, sel: r.selection_type, odds: r.odds_american, line: r.line, updated: r.odds_changed_at || r.last_seen_at });
        }
      }
      res.json({
        homeTeam: ev.homeTeam,
        awayTeam: ev.awayTeam,
        commenceTime: ev.commenceTime,
        eventId: ev.eventId,
        byBookMarket,
        markets: ev.markets,
        moneylineRows: mlRows.sort((a, b) => a.sb.localeCompare(b.sb)),
      });
    } catch (err) {
      res.status(500).json({ error: err.message, stack: err.stack });
    }
  });

  // Bulk lookup of line_ids against the current line-manager index.
  // POST body: { lineIds: ['abc...', 'def...'] }
  // Response: { [lineId]: { marketType, marketName, line, sport, teamName } | null }
  // Return a sample of registered lines filtered by market type substring.
  // Useful for debugging seed results: /debug/lineindex-sample?marketType=first_5&limit=5
  app.get('/debug/lineindex-sample', (req, res) => {
    try {
      const marketFilter = (req.query.marketType || '').toLowerCase();
      const limit = Number(req.query.limit) || 10;
      const idx = lineManager.__debugGetLineIndex ? lineManager.__debugGetLineIndex() : null;
      if (!idx) return res.status(500).json({ error: 'lineIndex accessor missing' });
      const matching = [];
      for (const [lineId, info] of Object.entries(idx)) {
        if (marketFilter && !(info.marketType || '').toLowerCase().includes(marketFilter)) continue;
        matching.push({ lineId, ...info });
        if (matching.length >= limit) break;
      }
      res.json({ ok: true, count: matching.length, sample: matching });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Audit a single parlay by fetching PX's authoritative market data
  // for each leg's sport_event_id, finding the line_id, and returning
  // the actual market.type and market.name from PX. Used to verify that
  // our locally-stored marketType matches PX reality.
  app.get('/debug/audit-parlay/:id', async (req, res) => {
    try {
      const parlayId = req.params.id;
      const local = orderTracker.findByParlayId(parlayId);
      if (!local) return res.status(404).json({ error: 'local parlay not found' });
      // Resolve each leg via PX fetchMarkets
      const auditedLegs = [];
      for (const leg of (local.legs || local.meta?.legs || [])) {
        const lineId = leg.lineId || leg.line_id;
        const eventId = leg.pxEventId;
        if (!lineId || !eventId) {
          auditedLegs.push({ team: leg.team, storedMarket: leg.market, note: 'missing line_id or pxEventId' });
          continue;
        }
        let markets;
        try {
          markets = await px.fetchMarkets(eventId);
        } catch (err) {
          auditedLegs.push({ team: leg.team, storedMarket: leg.market, note: 'fetchMarkets failed: ' + err.message });
          continue;
        }
        // Find which market contains this line_id by walking all markets
        let foundMarket = null;
        let foundSel = null;
        for (const m of markets || []) {
          // Check flat selections
          for (const sg of (m.selections || [])) {
            for (const s of (sg || [])) {
              if (s.line_id === lineId) { foundMarket = m; foundSel = s; break; }
            }
            if (foundMarket) break;
          }
          if (foundMarket) break;
          // Check nested market_lines
          for (const ml of (m.market_lines || [])) {
            for (const sg of (ml.selections || [])) {
              for (const s of (sg || [])) {
                if (s.line_id === lineId) { foundMarket = m; foundSel = s; break; }
              }
              if (foundMarket) break;
            }
            if (foundMarket) break;
          }
          if (foundMarket) break;
        }
        auditedLegs.push({
          team: leg.team,
          storedMarket: leg.market,
          storedLine: leg.line,
          pxMarketType: foundMarket?.type || null,
          pxMarketName: foundMarket?.name || null,
          pxSelName: foundSel?.name || null,
          pxSelLine: foundSel?.line ?? null,
          matches: foundMarket ? (
            (leg.market === 'moneyline' && foundMarket.type === 'moneyline' && !/1st.*5|first.*5|f5\b/i.test(foundMarket.name || '')) ||
            (leg.market === 'spread' && foundMarket.type === 'spread' && !/1st.*5|first.*5|f5\b/i.test(foundMarket.name || '')) ||
            (leg.market === 'total' && foundMarket.type === 'total' && !/1st.*5|first.*5|f5\b/i.test(foundMarket.name || ''))
          ) : 'line not found in PX markets',
        });
      }
      res.json({ ok: true, parlayId, legs: auditedLegs });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  app.post('/debug/lookup-lines', (req, res) => {
    try {
      const ids = Array.isArray(req.body?.lineIds) ? req.body.lineIds : [];
      const out = {};
      for (const id of ids) {
        const info = lineManager.lookupLine(id);
        out[id] = info ? {
          marketType: info.marketType,
          marketName: info.marketName,
          line: info.line,
          sport: info.sport,
          teamName: info.teamName,
          selection: info.selection,
          pxEventId: info.pxEventId,
          pxEventName: info.pxEventName,
        } : null;
      }
      res.json({ ok: true, results: out });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Query The Odds API directly (not SharpAPI) — used to debug what
  // specific books actually return. sport defaults to baseball_mlb.
  app.get('/debug/odds-api-raw', async (req, res) => {
    try {
      const fetch = require('node-fetch');
      const theOddsApiKey = process.env.THE_ODDS_API_KEY;
      if (!theOddsApiKey) return res.status(500).json({ ok: false, error: 'THE_ODDS_API_KEY not set' });
      const sport = req.query.sport || 'baseball_mlb';
      const markets = req.query.markets || 'h2h';
      const bookmakers = req.query.bookmakers || 'pinnacle,fanduel,draftkings';
      const team = req.query.team;
      const url = `https://api.the-odds-api.com/v4/sports/${sport}/odds`
        + `?apiKey=${theOddsApiKey}&regions=us,eu&markets=${markets}&bookmakers=${bookmakers}&oddsFormat=american`;
      const resp = await fetch(url);
      if (!resp.ok) {
        const txt = await resp.text();
        return res.status(500).json({ ok: false, status: resp.status, body: txt.substring(0, 500) });
      }
      const data = await resp.json();
      const filtered = team
        ? data.filter(e => (e.home_team || '').toLowerCase().includes(team.toLowerCase()) || (e.away_team || '').toLowerCase().includes(team.toLowerCase()))
        : data;
      // Summarize per event: book -> {home, away}
      const out = [];
      for (const ev of filtered.slice(0, 20)) {
        const byBook = {};
        for (const b of (ev.bookmakers || [])) {
          const mkt = (b.markets || []).find(m => m.key === 'h2h');
          if (!mkt) continue;
          const home = (mkt.outcomes || []).find(o => o.name === ev.home_team);
          const away = (mkt.outcomes || []).find(o => o.name === ev.away_team);
          byBook[b.key] = {
            home: home?.price,
            away: away?.price,
            last_update: b.last_update,
          };
        }
        out.push({
          home: ev.home_team,
          away: ev.away_team,
          commence_time: ev.commence_time,
          books: byBook,
        });
      }
      res.json({ ok: true, url: url.replace(theOddsApiKey, 'REDACTED'), count: filtered.length, events: out });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Diagnostic: mirror the exact fetch that fetchOddsForSport uses
  app.get('/debug/raw-mlb-fetch', async (req, res) => {
    try {
      const fetch = require('node-fetch');
      const cfg = config.config || config;
      // Exactly mirror fetchOddsForSport for baseball_mlb
      const url = `${cfg.oddsApi.baseUrl}/odds?league=mlb&market=moneyline,run_line,total_runs,team_total&live=false&limit=500`;
      const resp = await fetch(url, { headers: { 'X-API-Key': cfg.oddsApi.apiKey } });
      const body = await resp.json();
      const rows = body.data || [];
      // Group by event
      const byEvent = {};
      for (const row of rows) {
        const key = row.event_id;
        if (!byEvent[key]) {
          byEvent[key] = {
            event_id: key,
            home: row.home_team,
            away: row.away_team,
            start: row.event_start_time,
            rows: 0,
            books: new Set(),
            markets: new Set(),
            selections: new Set(),
          };
        }
        byEvent[key].rows++;
        byEvent[key].books.add(row.sportsbook);
        byEvent[key].markets.add(row.market_type);
        byEvent[key].selections.add(row.selection_type);
      }
      const events = Object.values(byEvent).map(e => ({
        event_id: e.event_id,
        home: e.home, away: e.away, start: e.start,
        rows: e.rows,
        books: [...e.books],
        markets: [...e.markets],
        selections: [...e.selections],
      })).sort((a, b) => (a.start || '').localeCompare(b.start || ''));
      res.json({
        totalRows: rows.length,
        totalEvents: events.length,
        url,
        events,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Diagnostic: show raw sportsbook names from SharpAPI
  app.get('/debug/sportsbooks', async (req, res) => {
    try {
      const fetch = require('node-fetch');
      const league = req.query.league || 'nba';
      const market = req.query.market || 'moneyline';
      const cfg = config.config || config;
      const url = `${cfg.oddsApi.baseUrl}/odds?league=${league}&market=${market}&limit=50`;
      const resp = await fetch(url, {
        headers: { 'X-API-Key': cfg.oddsApi.apiKey },
      });
      const body = await resp.json();
      const rows = body.data || [];
      const books = {};
      for (const r of rows) {
        if (!books[r.sportsbook]) books[r.sportsbook] = 0;
        books[r.sportsbook]++;
      }
      const events = {};
      for (const r of rows) {
        const key = (r.home_team || '') + ' vs ' + (r.away_team || '');
        if (!events[key]) events[key] = new Set();
        events[key].add(r.sportsbook);
      }
      // Show selection_type values per sportsbook
      const selTypes = {};
      for (const r of rows) {
        const key = r.sportsbook;
        if (!selTypes[key]) selTypes[key] = new Set();
        selTypes[key].add(r.selection_type);
      }
      // Show sample raw rows for first sportsbook
      const sampleRows = rows.slice(0, 3).map(r => ({
        sportsbook: r.sportsbook, selection_type: r.selection_type, selection: r.selection,
        home_team: r.home_team, away_team: r.away_team, odds_american: r.odds_american,
      }));
      res.json({
        league, market,
        totalRows: rows.length,
        sportsbooks: Object.entries(books).sort((a, b) => b[1] - a[1]).map(([book, count]) => ({ book, count })),
        selectionTypes: Object.entries(selTypes).map(([book, types]) => ({ book, types: [...types] })),
        sampleRows,
        sampleEvents: Object.entries(events).slice(0, 5).map(([event, booksSet]) => ({ event, books: [...booksSet] })),
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Check game results for early win detection
  app.post('/check-results', async (req, res) => {
    try {
      const result = await orderTracker.checkLegResults();
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.post('/fix-settlements', async (req, res) => {
    try {
      const result = orderTracker.revertBogusSettlements();
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.post('/poll-settlements', async (req, res) => {
    try {
      const result = await orderTracker.pollOrderSettlements(px);
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Debug: enumerate every confirmed leg matching a given team name and
  // group by the composite key the client uses in teamParlayMap. Lets us
  // see whether the Team Exposure drop-down is over-matching because
  // legs share a pxEventId they shouldn't, or because the fallback key
  // (name|opponent|date) collapses across dates.
  // Usage: GET /debug/team-exposure-legs?team=Houston%20Rockets
  app.get('/debug/team-exposure-legs', (req, res) => {
    try {
      const target = (req.query.team || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
      if (!target) return res.status(400).json({ ok: false, error: 'team query param required' });
      const all = orderTracker.getRecentOrders(5000) || [];
      const confirmed = all.filter(o => o.status === 'confirmed');
      const byKey = {};
      const unmatched = [];
      for (const o of confirmed) {
        const legs = o.legs || (o.meta && o.meta.legs) || [];
        for (const l of legs) {
          const name = String(l.team || l.teamName || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
          if (!name || !name.includes(target)) continue;
          const gameDate = l.startTime ? new Date(l.startTime).toISOString().substring(0, 10) : '';
          const eventId = l.pxEventId || null;
          let compositeKey;
          if (eventId) {
            compositeKey = name + '|' + eventId + '|' + gameDate;
          } else {
            const opp = String((l.homeTeam || '') + (l.awayTeam || '')).toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
            compositeKey = name + '|' + (opp || '') + '|' + (gameDate || 'noevent');
          }
          if (!byKey[compositeKey]) byKey[compositeKey] = [];
          byKey[compositeKey].push({
            parlayId: o.parlayId,
            confirmedAt: o.confirmedAt,
            market: l.market || l.marketType || null,
            marketName: l.marketName || null,
            pxEventName: l.pxEventName || null,
            pxEventId: eventId,
            startTime: l.startTime || null,
            homeTeam: l.homeTeam || null,
            awayTeam: l.awayTeam || null,
            line: l.line != null ? l.line : null,
            lineId: l.lineId || null,
            confirmedStake: o.confirmedStake,
            legTeam: l.team || l.teamName,
          });
        }
      }
      // Summarize
      const groups = Object.entries(byKey).map(([key, legs]) => ({
        compositeKey: key,
        legCount: legs.length,
        distinctMarkets: [...new Set(legs.map(x => x.market))],
        distinctEventNames: [...new Set(legs.map(x => x.pxEventName).filter(Boolean))],
        sampleLegs: legs.slice(0, 5),
      })).sort((a, b) => b.legCount - a.legCount);
      res.json({
        ok: true,
        target,
        totalConfirmedScanned: confirmed.length,
        matchingLegCount: Object.values(byKey).reduce((s, a) => s + a.length, 0),
        distinctCompositeKeys: groups.length,
        groups,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Debug: trace golf matchup matching step by step
  // Debug: pull DataGolf's raw per-book odds for a specific matchup so we
  // can see which books actually offered the pairing and confirm the
  // consensus isn't averaging across books that never offered it.
  // Usage: GET /debug/datagolf-raw?p1=Scheffler&p2=Fitzpatrick&market=round_matchups&round=4
  app.get('/debug/datagolf-raw', async (req, res) => {
    try {
      const p1q = (req.query.p1 || '').toLowerCase();
      const p2q = (req.query.p2 || '').toLowerCase();
      const market = req.query.market || 'round_matchups';
      const tour = req.query.tour || 'pga';
      const apiKey = process.env.DATAGOLF_API_KEY;
      if (!apiKey) return res.status(400).json({ ok: false, error: 'DATAGOLF_API_KEY not set' });
      const url = `https://feeds.datagolf.com/betting-tools/matchups`
        + `?tour=${tour}&market=${market}&odds_format=american&file_format=json&key=${apiKey}`;
      const resp = await fetch(url);
      if (!resp.ok) return res.status(resp.status).json({ ok: false, error: `DG ${resp.status}` });
      const data = await resp.json();
      if (typeof data.match_list === 'string') {
        return res.json({ ok: true, note: 'DataGolf returned string (no matchups offered)', value: data.match_list });
      }
      const matches = (data.match_list || []).filter(m => {
        const n1 = (m.p1_player_name || '').toLowerCase();
        const n2 = (m.p2_player_name || '').toLowerCase();
        if (!p1q && !p2q) return true;
        const bothInOne = (p1q && (n1.includes(p1q) || n2.includes(p1q))) && (p2q && (n1.includes(p2q) || n2.includes(p2q)));
        return bothInOne;
      });
      res.json({
        ok: true,
        event_name: data.event_name,
        round_num: data.round_num,
        last_updated: data.last_updated,
        matchCount: matches.length,
        matches,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.get('/debug/golf-matching', async (req, res) => {
    try {
      const report = await lineManager.debugGolfMatching();
      res.json({ ok: true, ...report });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Debug: show DataGolf matchup cache
  app.get('/debug/golf-matchups', async (req, res) => {
    try {
      const events = oddsFeed.getAllCachedEvents().filter(e => e.sport === 'golf_matchups');
      // Deduplicate — cache has mirror entries for order-independence
      const seen = new Set();
      const unique = [];
      for (const e of events) {
        const key = [e.homeTeam, e.awayTeam].sort().join('|');
        if (seen.has(key)) continue;
        seen.add(key);
        unique.push(e);
      }
      res.json({
        ok: true,
        totalCacheEntries: events.length,
        uniqueMatchups: unique.length,
        sample: unique.slice(0, 50).map(e => ({ home: e.homeTeam, away: e.awayTeam, markets: e.markets })),
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Debug: test alt-line fetch for a specific event
  app.get('/debug/alt-lines', async (req, res) => {
    try {
      const sport = req.query.sport || 'basketball_nba';
      const home = req.query.home;
      const away = req.query.away;
      if (!home || !away) {
        // List available events
        const events = oddsFeed.getAllCachedEvents().filter(e => e.sport === sport);
        return res.json({ ok: true, hint: 'Pass ?home=TeamA&away=TeamB&sport=xxx', events: events.map(e => ({ home: e.homeTeam, away: e.awayTeam, sport: e.sport })) });
      }
      const result = await oddsFeed.fetchAltLines(sport, home, away);
      if (!result) return res.json({ ok: false, error: 'No alt lines returned (no event match or API error)' });
      const spreads = Object.entries(result.altSpreads || {}).map(([line, data]) => ({ line: parseFloat(line), home: data.home?.toFixed(4), away: data.away?.toFixed(4), books: data.books }));
      const totals = Object.entries(result.altTotals || {}).map(([line, data]) => ({ line: parseFloat(line), over: data.over?.toFixed(4), under: data.under?.toFixed(4), books: data.books }));
      res.json({ ok: true, sport, home, away, fetchedAt: new Date(result.fetchedAt).toISOString(), altSpreads: spreads.sort((a, b) => a.line - b.line), altTotals: totals.sort((a, b) => a.line - b.line), spreadCount: spreads.length, totalCount: totals.length });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Debug: check if a specific parlayId was received via WebSocket
  app.get('/debug/rfq-receipt', (req, res) => {
    try {
      const parlayId = req.query.parlayId || req.query.id;
      if (!parlayId) {
        // No ID → return summary stats
        return res.json({ ok: true, hint: 'Pass ?parlayId=xxx to check a specific parlay', stats: websocket.getReceivedRfqStats() });
      }
      const entry = websocket.wasRfqReceived(parlayId);
      if (!entry) {
        return res.json({ ok: true, parlayId, received: false, note: 'This parlayId is NOT in our receipt log. Either we never received it from PX, or it was received before our receipt tracking started (after a restart).' });
      }
      res.json({ ok: true, parlayId, received: true, ...entry });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Debug: bulk check — given a list of parlayIds (from market-intel missed list),
  // return which ones we received vs didn't
  app.post('/debug/rfq-receipt-bulk', (req, res) => {
    try {
      const ids = (req.body && req.body.parlayIds) || [];
      if (!Array.isArray(ids) || ids.length === 0) {
        return res.json({ ok: false, error: 'POST { parlayIds: [...] } required' });
      }
      const results = { received: [], notReceived: [] };
      for (const id of ids) {
        const entry = websocket.wasRfqReceived(id);
        if (entry) results.received.push({ parlayId: id, ...entry });
        else results.notReceived.push(id);
      }
      res.json({
        ok: true,
        totalChecked: ids.length,
        receivedCount: results.received.length,
        notReceivedCount: results.notReceived.length,
        receiptRate: Math.round(results.received.length / ids.length * 1000) / 10 + '%',
        ...results,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Quote coverage: real-time breakdown of why RFQs are being declined or failing to price.
  app.get('/quote-coverage', (req, res) => {
    try {
      const coverage = websocket.getQuoteCoverageStats();
      const total = coverage.rfqStages.received || 1;
      res.json({
        ok: true,
        summary: {
          received: coverage.rfqStages.received,
          submitted: coverage.rfqStages.submitted,
          declined: coverage.rfqStages.declined,
          priceFailed: coverage.rfqStages.priceFailed,
          submissionRate: coverage.submissionRate,
        },
        priceFailureReasons: coverage.priceFailureReasons,
        declineReasons: coverage.declineReasons,
        recentFailures: coverage.recentFailures,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Decline audit: rank unknown events/sports by how often they're declining parlays.
  // Optional ?window=5m|15m|30m|1h|2h|6h|24h — when provided, stats are computed from
  // the rolling event log filtered to the window rather than all-session cumulative.
  // Drill-down: recent decline events filtered by reason. Backs the
  // mobile app's expandable decline rows. Returns up to ?limit raw
  // events from the rolling event log for an exact ?reason match,
  // newest first. Defaults to 24h window, limit 50.
  app.get('/decline-events', (req, res) => {
    try {
      const reason = req.query.reason;
      if (!reason) return res.status(400).json({ ok: false, error: 'reason query param required' });
      const limit = Math.min(Math.max(parseInt(req.query.limit) || 50, 1), 500);
      const windowMs = (() => {
        const w = req.query.window || '24h';
        const m = /^(\d+)\s*(s|m|h|d)?$/i.exec(w);
        if (!m) return 24 * 60 * 60 * 1000;
        const mult = { s: 1000, m: 60000, h: 3600000, d: 86400000 }[(m[2] || 'h').toLowerCase()] || 3600000;
        return Number(m[1]) * mult;
      })();
      const intel = orderTracker.getMarketIntel(5000);
      const events = (intel.declines && intel.declines.recentDeclineEvents) || [];
      const cutoff = Date.now() - windowMs;
      const matches = [];
      // Walk newest first
      for (let i = events.length - 1; i >= 0; i--) {
        const ev = events[i];
        if (new Date(ev.time).getTime() < cutoff) break;
        if (ev.reason !== reason) continue;
        // Trim each event to the fields the mobile actually uses, keeps
        // payload small over cellular.
        matches.push({
          time: ev.time,
          parlayId: ev.parlayId,
          legCount: (ev.knownLegs || []).length + (ev.unknownCategories || []).length,
          knownLegs: (ev.knownLegs || []).slice(0, 8).map(l => ({
            sport: l.sport, marketType: l.marketType, teamName: l.teamName,
            line: l.line, eventName: l.eventName,
          })),
          unknownCategories: (ev.unknownCategories || []).slice(0, 8).map(uc => ({
            category: uc.category, sport: uc.sport, eventName: uc.eventName,
            marketName: uc.marketName, propType: uc.propType, line: uc.line,
            isKnownEvent: uc.isKnownEvent, resolveReason: uc.resolveReason,
          })),
          detail: ev.detail,
        });
        if (matches.length >= limit) break;
      }
      res.json({
        ok: true, reason, window: req.query.window || '24h', limit,
        cutoff: new Date(cutoff).toISOString(),
        count: matches.length, events: matches,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.get('/decline-audit', (req, res) => {
    try {
      const intel = orderTracker.getMarketIntel(1000);
      const declines = intel.declines || {};

      // Parse the window parameter into milliseconds. Accepts e.g. '5m', '1h', '24h'.
      function parseWindow(w) {
        if (!w) return null;
        const m = /^(\d+)\s*(s|m|h|d)?$/i.exec(w);
        if (!m) return null;
        const n = Number(m[1]);
        const unit = (m[2] || 'm').toLowerCase();
        const mult = unit === 's' ? 1000
                   : unit === 'm' ? 60 * 1000
                   : unit === 'h' ? 60 * 60 * 1000
                   : unit === 'd' ? 24 * 60 * 60 * 1000
                   : 60 * 1000;
        return n * mult;
      }
      const windowMs = parseWindow(req.query.window);

      // If a window is requested, compute stats by filtering the rolling event log.
      if (windowMs != null) {
        const events = declines.recentDeclineEvents || [];
        const cutoff = Date.now() - windowMs;
        const inWindow = events.filter(e => new Date(e.time).getTime() >= cutoff);

        const byReason = {};
        const categoryAgg = {}; // category -> { count, bySport: {} }
        let quotableCount = 0;
        let unquotableCount = 0;
        const QUOTABLE_CATS = new Set(['alt_line', 'alt_spread', 'alt_total', 'team_total']);
        // Derive per-sport decline counts from leg data in the window
        const bySport = {};
        // Per-event counts for ranking
        const byEvent = {};

        for (const ev of inWindow) {
          byReason[ev.reason] = (byReason[ev.reason] || 0) + 1;
          // Count sports from known legs
          for (const l of (ev.knownLegs || [])) {
            if (l.sport) bySport[l.sport] = (bySport[l.sport] || 0) + 1;
          }
          // Count unknown categories
          for (const uc of (ev.unknownCategories || [])) {
            const cat = uc.category || 'unknown';
            if (!categoryAgg[cat]) categoryAgg[cat] = { count: 0, bySport: {}, sampleLegs: [] };
            categoryAgg[cat].count++;
            if (uc.sport) categoryAgg[cat].bySport[uc.sport] = (categoryAgg[cat].bySport[uc.sport] || 0) + 1;
            if (categoryAgg[cat].sampleLegs.length < 5) categoryAgg[cat].sampleLegs.push(uc);
            if (QUOTABLE_CATS.has(cat)) quotableCount++;
            else unquotableCount++;
            if (uc.eventName) {
              const key = uc.eventName + (uc.isKnownEvent ? ' [unregistered market]' : ' [unsupported event]');
              byEvent[key] = (byEvent[key] || 0) + 1;
            }
          }
        }
        // Build topUnknowns ranked by count
        const topUnknowns = Object.entries(byEvent)
          .map(([raw, count]) => {
            const tagMatch = raw.match(/\[([^\]]+)\]/);
            return { eventName: raw.split('[')[0].trim(), tag: tagMatch ? tagMatch[1] : 'unknown', count };
          })
          .sort((a, b) => b.count - a.count)
          .slice(0, 50);

        return res.json({
          ok: true,
          window: req.query.window,
          windowMs,
          cutoff: new Date(cutoff).toISOString(),
          totalDeclines: inWindow.length,
          byReason,
          bySport,
          topUnknowns,
          unknownLegDrillDown: {
            totalUnknownLegs: quotableCount + unquotableCount,
            quotable: quotableCount,
            unquotable: unquotableCount,
            categories: categoryAgg,
          },
          note: 'Windowed stats from rolling event log (max 5000 events retained). Omit ?window to get all-session cumulative stats.',
        });
      }

      // Group the unknownSports entries by the inferred sport/league
      const byKey = {};
      for (const [raw, val] of Object.entries(declines.unknownSports || {})) {
        const count = typeof val === 'object' ? val.count : val;
        const lastSeen = typeof val === 'object' ? val.lastSeen : null;
        const tagMatch = raw.match(/\[([^\]]+)\]/);
        const tag = tagMatch ? tagMatch[1] : 'unknown';
        const eventName = raw.split('[')[0].trim();
        byKey[raw] = { eventName, tag, count, lastSeen };
      }
      // Sort by count desc
      const ranked = Object.values(byKey).sort((a, b) => b.count - a.count);
      // Aggregate by tag
      const byTag = {};
      for (const r of ranked) {
        if (!byTag[r.tag]) byTag[r.tag] = { count: 0, distinctEvents: 0 };
        byTag[r.tag].count += r.count;
        byTag[r.tag].distinctEvents++;
      }
      // Unknown leg categories (granular drill-down)
      const categories = declines.unknownLegCategories || {};
      const categorySummary = {};
      for (const [cat, info] of Object.entries(categories)) {
        categorySummary[cat] = {
          count: info.count,
          bySport: info.bySport,
          byResolveReason: info.byResolveReason,
          quotable: ['alt_line', 'alt_spread', 'alt_total', 'team_total'].includes(cat),
          sampleLegs: info.sampleLegs || [],
        };
      }
      // Compute actionable summary
      const quotableCount = Object.entries(categorySummary)
        .filter(([, v]) => v.quotable)
        .reduce((s, [, v]) => s + v.count, 0);
      const unquotableCount = Object.entries(categorySummary)
        .filter(([, v]) => !v.quotable)
        .reduce((s, [, v]) => s + v.count, 0);

      // Unsupported PX market types — bettor is trying to price these
      // but our code doesn't recognize the market.type
      const unsupportedMarkets = declines.unsupportedMarkets || {};
      const sortedUnsupported = Object.values(unsupportedMarkets)
        .sort((a, b) => b.count - a.count)
        .slice(0, 100);

      res.json({
        ok: true,
        totalDeclines: declines.total,
        byReason: declines.reasons,
        byTag,
        topUnknowns: ranked.slice(0, 50),
        // New: granular unknown leg categories
        unknownLegDrillDown: {
          totalUnknownLegs: quotableCount + unquotableCount,
          quotable: quotableCount,
          unquotable: unquotableCount,
          categories: categorySummary,
        },
        // Unsupported PX market types — what bettors are trying to price
        // that we decline wholesale (e.g., F5 innings, quarters, etc.)
        unsupportedMarketTypes: {
          uniqueTypes: Object.keys(unsupportedMarkets).length,
          totalOccurrences: Object.values(unsupportedMarkets).reduce((s, v) => s + v.count, 0),
          top: sortedUnsupported,
        },
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Debug: list PX sport events with sport_name grouping (diagnose sport name mismatches)
  // Dump raw PX market names/types for an event, looked up by a team
  // substring match against event name + competitors. Primary use: see
  // what series markets (Series Winner, Series Spread, Series Total
  // Games, etc.) actually exist on PX so we can confirm our detection
  // regexes match the exact labels.
  app.get('/debug-px-event-markets', async (req, res) => {
    try {
      const q = (req.query.q || '').toLowerCase().trim();
      if (!q) return res.status(400).json({ ok: false, error: 'q required' });
      const allEvents = await px.fetchSportEvents();
      const candidates = allEvents.filter(e => {
        const hay = ((e.name || '') + ' ' + (e.competitors || []).map(c => c.name).join(' ')).toLowerCase();
        return hay.includes(q);
      });
      if (candidates.length === 0) return res.status(404).json({ ok: false, error: 'no event matches q' });
      const results = [];
      for (const ev of candidates.slice(0, 10)) {
        let markets = [];
        try { markets = await px.fetchMarkets(ev.event_id); } catch (e) { markets = [{ error: e.message }]; }
        results.push({
          event_id: ev.event_id,
          name: ev.name,
          sport_name: ev.sport_name,
          scheduled: ev.scheduled,
          competitors: (ev.competitors || []).map(c => ({ name: c.name, side: c.side })),
          markets: markets.map(m => ({
            type: m.type,
            name: m.name,
            // sample a few selection labels so we can see spread/total line structure
            sampleSelections: (() => {
              const labels = [];
              if (m.selections) {
                for (const g of m.selections) for (const s of g) if (s.display_name || s.name) labels.push(`${s.display_name || s.name}${s.line != null ? ` (line=${s.line})` : ''}`);
              }
              if (m.market_lines) {
                for (const ml of m.market_lines) {
                  for (const g of (ml.selections || [])) for (const s of g) if (s.display_name || s.name) labels.push(`${s.display_name || s.name}${s.line != null ? ` (line=${s.line})` : (ml.line != null ? ` (line=${ml.line})` : '')}`);
                }
              }
              return labels.slice(0, 6);
            })(),
          })),
        });
      }
      res.json({ ok: true, matchedCount: candidates.length, results });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  app.get('/px-events-debug', async (req, res) => {
    try {
      const allEvents = await px.fetchSportEvents();
      const filter = req.query.sport_name;
      const sampleLimit = parseInt(req.query.limit || '3');
      const bySportName = {};
      for (const e of allEvents) {
        const sn = e.sport_name || '(none)';
        if (filter && sn !== filter) continue;
        if (!bySportName[sn]) bySportName[sn] = { count: 0, sample: [] };
        bySportName[sn].count++;
        if (bySportName[sn].sample.length < sampleLimit) {
          bySportName[sn].sample.push({
            name: e.name,
            event_id: e.event_id,
            competitors: (e.competitors || []).map(c => ({ name: c.name, side: c.side })),
            scheduled: e.scheduled,
            status: e.status,
          });
        }
      }
      res.json({ ok: true, totalEvents: allEvents.length, bySportName });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.get('/px-orders', async (req, res) => {
    try {
      const limit = Number(req.query.limit) || 500;
      const status = req.query.status || null;
      const pxOrders = await px.fetchOrders(limit, status);
      // Summarize by settlement_status
      const byStatus = {};
      for (const o of pxOrders) {
        const s = o.settlement_status || 'none';
        byStatus[s] = (byStatus[s] || 0) + 1;
      }
      // Return settled by default; add ?include=all or ?include=tbd to also return pending
      const include = req.query.include || 'settled';
      const settled = pxOrders.filter(o => o.settlement_status && !['tbd','requested','none'].includes(o.settlement_status));
      const tbd = pxOrders.filter(o => o.settlement_status === 'tbd' || o.status === 'tbd');
      const body = { ok: true, total: pxOrders.length, byStatus };
      if (include === 'all' || include === 'settled') body.settled = settled;
      if (include === 'all' || include === 'tbd') body.tbd = tbd;
      res.json(body);
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Debug: return raw PX response including pagination metadata
  app.get('/px-orders-raw', async (req, res) => {
    try {
      const limit = Number(req.query.limit) || 100;
      const status = req.query.status || null;
      // Pass through any query param PX might support for pagination
      const extraParams = [];
      for (const k of Object.keys(req.query)) {
        if (['limit','status'].includes(k)) continue;
        extraParams.push(`${k}=${encodeURIComponent(req.query[k])}`);
      }
      let url = `/parlay/sp/orders/?limit=${limit}`;
      if (status) url += `&status=${status}`;
      if (extraParams.length) url += '&' + extraParams.join('&');
      const raw = await px.pxFetch(url);
      res.json({ ok: true, url, firstUuid: raw?.data?.orders?.[0]?.order_uuid, orderCount: raw?.data?.orders?.length, token: raw?.data?.token });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Reconcile a single order between PX and local state.
  // :id can be a parlay_id OR an order_uuid — we scan recent PX orders for either.
  app.get('/reconcile-order/:id', async (req, res) => {
    try {
      const id = req.params.id;
      const limit = Number(req.query.limit) || 1000;
      // Pull recent PX orders and scan for a match on either parlay_id or order_uuid
      const pxOrders = await px.fetchOrders(limit);
      const pxMatch = pxOrders.find(o =>
        o.order_uuid === id || o.p_id === id || o.parlay_id === id
      );
      // Find local order (try both ID types)
      const localByParlay = orderTracker.findByParlayId(id);
      const localByUuid = orderTracker.findByOrderUuid(id);
      const local = localByParlay || localByUuid || null;

      // Build a compact diff summary if we found both
      const diff = {};
      if (pxMatch && local) {
        const pxStatus = pxMatch.settlement_status || null;
        const localStatus = local.status && local.status.startsWith('settled_')
          ? local.status.substring('settled_'.length)
          : local.status;
        if (pxStatus !== localStatus) diff.settlementStatus = { px: pxStatus, local: localStatus };

        const pxStake = pxMatch.stake != null ? Number(pxMatch.stake) : (pxMatch.confirmed_stake != null ? Number(pxMatch.confirmed_stake) : null);
        if (pxStake != null && local.confirmedStake != null && Math.abs(pxStake - local.confirmedStake) > 0.01) {
          diff.stake = { px: pxStake, local: local.confirmedStake };
        }

        const pxOdds = pxMatch.confirmed_odds != null ? Number(pxMatch.confirmed_odds) : (pxMatch.odds != null ? Number(pxMatch.odds) : null);
        if (pxOdds != null && local.confirmedOdds != null && pxOdds !== local.confirmedOdds) {
          diff.confirmedOdds = { px: pxOdds, local: local.confirmedOdds };
        }

        const pxProfit = pxMatch.profit != null ? Number(pxMatch.profit) : null;
        if (pxProfit != null && local.pnl != null && Math.abs(pxProfit - local.pnl) > 0.01) {
          diff.pnl = { px: pxProfit, local: local.pnl };
        }

        const pxLegCount = (pxMatch.legs || []).length;
        const localLegs = local.legs || local.meta?.legs || [];
        if (pxLegCount !== localLegs.length) {
          diff.legCount = { px: pxLegCount, local: localLegs.length };
        }

        // Per-leg settlement comparison (by line_id)
        const legDiffs = [];
        for (const pxLeg of pxMatch.legs || []) {
          const localLeg = localLegs.find(l => l.lineId === pxLeg.line_id || l.line_id === pxLeg.line_id);
          const localLegStatus = localLeg && (localLeg.settlementStatus || localLeg.settlement_status || localLeg.inferredResult);
          if (pxLeg.settlement_status && localLegStatus && pxLeg.settlement_status !== localLegStatus) {
            legDiffs.push({
              line_id: pxLeg.line_id,
              team: localLeg?.teamName || localLeg?.team || '?',
              px: pxLeg.settlement_status,
              local: localLegStatus,
            });
          } else if (pxLeg.settlement_status && !localLeg) {
            legDiffs.push({ line_id: pxLeg.line_id, team: '?', px: pxLeg.settlement_status, local: 'MISSING' });
          }
        }
        if (legDiffs.length > 0) diff.legs = legDiffs;
      }

      res.json({
        ok: true,
        id,
        found: { px: !!pxMatch, local: !!local },
        match: pxMatch && local ? (Object.keys(diff).length === 0 ? 'identical' : 'differs') : 'incomplete',
        diff,
        px: pxMatch || null,
        local: local || null,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Settlement drift status — returns most recent PX-vs-local comparison
  // from the periodic drift check (runs every 5min). Used by the dashboard
  // drift banner and ops monitoring to catch silent reconciliation errors.
  app.get('/drift-status', (req, res) => {
    try {
      res.json({ ok: true, ...orderTracker.getDriftState() });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Manually run the drift check on demand instead of waiting for the
  // 5-minute interval.
  app.post('/check-drift', async (req, res) => {
    try {
      const result = await orderTracker.checkSettlementDrift(px);
      res.json({ ok: true, ...result, state: orderTracker.getDriftState() });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Manually trigger golf metadata backfill. Walks stored orders and
  // populates tournamentName/roundNum on golf legs by looking up the
  // line_id in the current line-manager index. Idempotent.
  app.post('/backfill-golf-metadata', (req, res) => {
    try {
      const result = orderTracker.backfillGolfMetadata();
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Manually trigger a reconcileSettlements pass. Returns number corrected.
  // Use after deploy to clean up historical mismatches without waiting for
  // the 2-min polling loop.
  app.post('/force-reconcile', async (req, res) => {
    try {
      const result = orderTracker.reconcileSettlements();
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Immediately sweep locally-'confirmed' orders against PX. Orders PX
  // can't locate AND that satisfy one of the phantom triggers (age,
  // all legs finished, any leg with a known result) get flagged
  // meta.phantom=true so the Open Positions view, CSV export, Deployed
  // figure, and exposure tables stop counting them. Use when the
  // Open Positions table is obviously polluted with stuck confirmeds
  // and you don't want to wait for the 5-min periodic reconcile.
  app.post('/purge-phantoms', async (req, res) => {
    try {
      const result = await orderTracker.reconcileGhostConfirmed(px);
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Force a fresh rebuild of the in-memory exposure tables from the current
  // orders map. Use this when Team Exposure / Game Exposure look stale or
  // empty after a reload path that mutated legs without re-running exposure.
  app.post('/rebuild-exposure', (req, res) => {
    try {
      const before = {
        teams: orderTracker.getExposureSnapshot().length,
        games: orderTracker.getGameExposureSnapshot().length,
      };
      const diag = orderTracker.rebuildAllExposure();
      const after = {
        teams: orderTracker.getExposureSnapshot().length,
        games: orderTracker.getGameExposureSnapshot().length,
      };
      res.json({ ok: true, before, after, diag });
    } catch (err) {
      log.error('Exposure', `rebuild-exposure failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Resolve open/unresolved orders using PX /partner/affiliate/* endpoints.
  // This is the bulk path for recovering team names on confirmed historical
  // parlays whose line_ids have aged out of the current lineIndex.
  app.post('/enrich-from-affiliate', async (req, res) => {
    try {
      const before = {
        teams: orderTracker.getExposureSnapshot().length,
        games: orderTracker.getGameExposureSnapshot().length,
      };
      const result = await orderTracker.enrichOpenPositionsFromAffiliate();
      const after = {
        teams: orderTracker.getExposureSnapshot().length,
        games: orderTracker.getGameExposureSnapshot().length,
      };
      res.json({ ok: true, before, after, result });
    } catch (err) {
      log.error('Affiliate', `enrich-from-affiliate failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Targeted repair for the accept-POST-failed drift. PX has booked
  // bets that our local DB records as rejected. Scans PX's open (TBD)
  // order list and flips any local-rejected/missing-status rows back
  // to confirmed, rebuilds team exposure, and persists.
  //
  // Dry-run by default — responds with a plan showing what would change.
  // Pass ?commit=true to actually write. Safe to re-run; only flips
  // rejected → confirmed, never the other direction.
  //
  // Observed 2026-04-24: 228 orders ($13,904 SP-risk) booked on PX
  // but locally rejected, hidden from Team Exposure. Root cause fix
  // still needed in websocket.handleConfirm accept-POST error path.
  app.post('/px-status-repair', async (req, res) => {
    const commit = req.query.commit === 'true' || req.query.commit === '1';
    try {
      const pxOrders = await px.fetchOrders(Number(req.query.limit) || 3000);
      const tbd = pxOrders.filter(o => o.settlement_status === 'tbd');

      const toPromote = [];
      const alreadyConfirmed = [];
      const missingLocal = [];

      for (const pxOrder of tbd) {
        const pid = pxOrder.p_id;
        const localOrder = pid ? orderTracker.findByParlayId(pid) : null;
        if (!localOrder) {
          missingLocal.push({
            parlayId: pid,
            stake: parseFloat(pxOrder.confirmed_stake || 0),
            orderUuid: pxOrder.order_uuid,
          });
          continue;
        }
        if (localOrder.status === 'confirmed') {
          alreadyConfirmed.push(pid);
          continue;
        }
        toPromote.push({
          parlayId: pid,
          localStatus: localOrder.status,
          confirmedStake: parseFloat(pxOrder.confirmed_stake || 0),
          confirmedOdds: parseInt(pxOrder.confirmed_odds || 0),
          orderUuid: pxOrder.order_uuid,
        });
      }

      const sumRisk = arr => arr.reduce((s, o) => s + (parseFloat(o.confirmedStake ?? o.stake ?? 0) || 0), 0);
      const toPromoteRisk = sumRisk(toPromote);
      const missingLocalRisk = sumRisk(missingLocal);

      if (!commit) {
        return res.json({
          ok: true,
          mode: 'dry-run',
          pxTbdCount: tbd.length,
          alreadyConfirmedCount: alreadyConfirmed.length,
          toPromoteCount: toPromote.length,
          toPromoteRisk: Math.round(toPromoteRisk * 100) / 100,
          missingLocalCount: missingLocal.length,
          missingLocalRisk: Math.round(missingLocalRisk * 100) / 100,
          sample: toPromote.slice(0, 10),
          note: 'Pass ?commit=true to actually flip statuses and add exposure.',
        });
      }

      // Execute promotions.
      let promoted = 0;
      const failures = [];
      for (const p of toPromote) {
        const result = orderTracker.importPxBookedOrder(
          p.parlayId, p.orderUuid, p.confirmedStake, p.confirmedOdds
        );
        if (result.ok) promoted++;
        else failures.push({ parlayId: p.parlayId, reason: result.reason });
      }

      res.json({
        ok: true,
        mode: 'committed',
        pxTbdCount: tbd.length,
        promoted,
        failures: failures.length,
        failureSample: failures.slice(0, 5),
        toPromoteRisk: Math.round(toPromoteRisk * 100) / 100,
        missingLocalCount: missingLocal.length,
        missingLocalRisk: Math.round(missingLocalRisk * 100) / 100,
        note: 'Status flips persisted to DB and exposure rebuilt. Missing-local orders still need separate import via /full-px-reconcile.',
      });
    } catch (err) {
      log.error('Repair', `/px-status-repair failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Ghost-sweep. Settles or phantoms local orders still marked
  // 'confirmed' whose games started long enough ago that settlement
  // MUST have happened on PX. Separate from /full-px-reconcile:
  // reconcile only processes orders PX still returns in its feed;
  // this sweep works backwards from local ghosts and either closes
  // them (via PX match) or hides them (phantom flag) so they stop
  // polluting stats and exposure.
  //
  // Query params:
  //   ?olderThanHours=24   (default) — how far past game start to treat as settled
  //   ?commit=true         — actually write; default is dry-run plan
  //
  // Observed 2026-04-24: 953 ghost orders from 2026-04-17 onward,
  // status=confirmed locally, games long over, PX settlement never
  // applied. Sweep is safe to re-run.
  app.post('/ghost-sweep', async (req, res) => {
    const commit = req.query.commit === 'true' || req.query.commit === '1';
    const olderThanHours = Math.max(1, Number(req.query.olderThanHours) || 24);
    try {
      // Pull PX's full order list so we can match ghosts to real settlements
      // where possible. 10K covers multi-week history and still fits in
      // memory comfortably.
      const pxOrders = await px.fetchOrders(10000);
      const result = await orderTracker.sweepGhostOrders({
        olderThanHours,
        commit,
        pxOrders,
      });
      res.json({
        ok: true,
        mode: commit ? 'committed' : 'dry-run',
        olderThanHours,
        pxOrdersFetched: pxOrders.length,
        ...result,
        note: commit
          ? 'Ghosts settled where PX match found; remainder phantomed. Stats rebuilt.'
          : 'Pass ?commit=true to actually settle/phantom. Sample actions shown.',
      });
    } catch (err) {
      log.error('GhostSweep', `/ghost-sweep failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Full reconcile against PX REST: exhaust PX order history, import/update
  // all orders locally, then rebuild stats from scratch. Recovery path for
  // in-memory drift against PX ground truth.
  app.post('/full-px-reconcile', async (req, res) => {
    try {
      const result = await orderTracker.fullPxReconcile(px);
      res.json({ ok: true, ...result });
    } catch (err) {
      log.error('Reconcile', `full-px-reconcile failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Dump the FULL raw PX order response, no field filtering. Reveals
  // every field PX actually sends per order and per leg, so we can see
  // whether team names or other useful metadata exist that we've been
  // silently dropping.
  app.get('/debug-px-raw-dump', async (req, res) => {
    try {
      const pxSvc = require('./services/prophetx');
      const limit = parseInt(req.query.limit) || 3;
      const orders = await pxSvc.fetchOrders(limit);
      // Also include the full Object.keys so even nested structures are visible
      const keyStructure = orders.slice(0, 1).map(o => ({
        topKeys: Object.keys(o),
        firstLegKeys: Array.isArray(o.legs) && o.legs[0] ? Object.keys(o.legs[0]) : [],
      }));
      res.json({ ok: true, count: orders.length, keyStructure, samples: orders.slice(0, limit) });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Backfill team/sport from a user-uploaded parlay export CSV. The CSV
  // has parlayId + Selections (comma-separated team names) + Sports per
  // row. This is the most reliable source for historical orders whose
  // events PX no longer serves via fetchMarkets.
  //
  // Expects body = raw CSV text (Content-Type: text/plain). Parses the
  // first row as headers, finds Parlay ID / Selections / Sports / Legs
  // columns, and builds a row array to hand to backfillFromExport.
  app.post('/backfill-from-csv', express.text({ limit: '10mb', type: ['text/*', 'application/octet-stream'] }), (req, res) => {
    try {
      const csv = req.body;
      if (!csv || typeof csv !== 'string') {
        return res.status(400).json({ ok: false, error: 'expected text/plain body with CSV content' });
      }
      // Simple CSV parser that handles quoted fields with commas.
      function parseLine(line) {
        const out = [];
        let cur = '';
        let inQ = false;
        for (let i = 0; i < line.length; i++) {
          const c = line[i];
          if (c === '"') {
            if (inQ && line[i + 1] === '"') { cur += '"'; i++; } // escaped quote
            else inQ = !inQ;
          } else if (c === ',' && !inQ) {
            out.push(cur);
            cur = '';
          } else {
            cur += c;
          }
        }
        out.push(cur);
        return out;
      }
      const lines = csv.split(/\r?\n/).filter(l => l.trim().length > 0);
      if (lines.length < 2) return res.status(400).json({ ok: false, error: 'CSV has no data rows' });
      const headers = parseLine(lines[0]).map(h => h.trim());
      const idxParlay = headers.findIndex(h => /parlay.*id/i.test(h));
      const idxLegs = headers.findIndex(h => /^legs$/i.test(h));
      const idxSelections = headers.findIndex(h => /selections/i.test(h));
      const idxSports = headers.findIndex(h => /sports/i.test(h));
      if (idxParlay < 0 || idxSelections < 0) {
        return res.status(400).json({ ok: false, error: 'CSV missing required columns (Parlay ID, Selections)', foundHeaders: headers });
      }
      const rows = [];
      for (let i = 1; i < lines.length; i++) {
        const cells = parseLine(lines[i]);
        const parlayId = cells[idxParlay]?.trim();
        if (!parlayId) continue;
        const selectionsStr = cells[idxSelections]?.trim() || '';
        const sportsStr = idxSports >= 0 ? (cells[idxSports]?.trim() || '') : '';
        // Selections field is a comma-joined string that was already parsed
        // by the quote-aware parser, so it arrives as a single cell. Split
        // on ", " to recover the individual team labels.
        const selections = selectionsStr.split(',').map(s => s.trim()).filter(Boolean);
        const sports = sportsStr ? sportsStr.split(/\s*\/\s*/).map(s => s.trim()).filter(Boolean) : [];
        const legCount = idxLegs >= 0 ? parseInt(cells[idxLegs], 10) : null;
        rows.push({ parlayId, legCount, selections, sports });
      }
      const result = orderTracker.backfillFromExport(rows);
      res.json({ ok: true, ...result });
    } catch (err) {
      log.error('Backfill', `backfill-from-csv failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Backfill sport metadata on orders currently tagged as 'unknown' sport.
  // Uses three-tier inference: pxEventId→eventIndex, team→self-built sport
  // map, else leave as unknown. Only mutates leg.sport fields; does not
  // touch P&L or stats counters.
  app.post('/backfill-sports', async (req, res) => {
    try {
      const result = orderTracker.backfillUnknownSports();
      res.json({ ok: true, ...result });
    } catch (err) {
      log.error('Backfill', `backfill-sports failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Fill-rate breakdown: how often does a submitted quote become a
  // confirmed order, sliced by sport / leg count / odds tier. Low fill
  // rate = we're too tight (outbid) or not competitive. High fill rate
  // on negative-EV slices = we're getting picked off by sharps.
  app.get('/fill-rate-report', (req, res) => {
    try {
      const all = orderTracker.getRecentOrders(20000);
      // Consider all orders that reached the submit stage — these have
      // status 'quoted' (submitted, not confirmed), 'confirmed', or
      // 'settled_*'. Decline/priceFailed ones aren't in the orders map.
      const candidates = all.filter(o =>
        o.status === 'quoted' || o.status === 'confirmed' || (o.status && o.status.startsWith('settled_'))
      );

      function bucketLegs(n) {
        if (n <= 2) return '2';
        if (n === 3) return '3';
        if (n === 4) return '4';
        if (n === 5) return '5';
        return '6+';
      }
      function bucketOdds(am) {
        if (am == null) return '?';
        const a = Math.abs(am);
        if (a < 150) return '+100 to +150';
        if (a < 250) return '+150 to +250';
        if (a < 400) return '+250 to +400';
        if (a < 700) return '+400 to +700';
        return '+700+';
      }
      function primarySport(o) {
        const legs = o.meta?.legs || o.legs || [];
        const sports = [...new Set(legs.map(l => l.sport).filter(Boolean))];
        if (sports.length === 1) return sports[0];
        if (sports.length > 1) return 'multi';
        return 'unknown';
      }
      // A parlay is a Same-Game Parlay if 2+ legs share a pxEventId.
      function isSGP(o) {
        const legs = o.meta?.legs || o.legs || [];
        if (legs.length < 2) return false;
        const seen = new Set();
        for (const l of legs) {
          const eid = l.pxEventId;
          if (!eid) continue;
          if (seen.has(eid)) return true;
          seen.add(eid);
        }
        return false;
      }
      // Did this SGP hit the correlation penalty? Post-pin-match refactor,
      // the marker is meta.pricingMethod being an SGP variant. Legacy orders
      // fall back to the old meta.correlationBoost field for historical data.
      function wasBoosted(o) {
        if (o.meta?.pricingMethod && o.meta.pricingMethod.startsWith('sgp_')) return true;
        return o.meta?.correlationBoost != null && o.meta.correlationBoost > 1;
      }

      const byKey = (grouper, label) => {
        const buckets = {};
        for (const o of candidates) {
          const k = grouper(o);
          if (!buckets[k]) buckets[k] = { submitted: 0, confirmed: 0, filled: 0 };
          buckets[k].submitted++;
          if (o.status === 'confirmed' || (o.status && o.status.startsWith('settled_'))) {
            buckets[k].confirmed++;
          }
        }
        for (const k of Object.keys(buckets)) {
          const b = buckets[k];
          b.fillPct = b.submitted > 0 ? Math.round(b.confirmed / b.submitted * 1000) / 10 : 0;
        }
        return Object.fromEntries(
          Object.entries(buckets).sort((a, b) => b[1].submitted - a[1].submitted)
        );
      };

      // SGP split: is the correlation penalty killing fill volume?
      // Compare SGP fill rate vs cross-game fill rate overall AND per-sport.
      const sgpCandidates = candidates.filter(isSGP);
      const nonSgpCandidates = candidates.filter(o => !isSGP(o));
      const boostedCandidates = candidates.filter(wasBoosted);
      function fillPct(arr) {
        const confirmed = arr.filter(o => o.status === 'confirmed' || (o.status && o.status.startsWith('settled_'))).length;
        return {
          submitted: arr.length,
          confirmed,
          fillPct: arr.length > 0 ? Math.round(confirmed / arr.length * 1000) / 10 : 0,
        };
      }
      // SGP fill rate broken down by sport so we can see if it's a specific
      // sport (e.g. NHL SGPs) that's failing to fill vs across-the-board.
      const sgpBySport = {};
      const nonSgpBySport = {};
      for (const o of sgpCandidates) {
        const s = primarySport(o);
        if (!sgpBySport[s]) sgpBySport[s] = [];
        sgpBySport[s].push(o);
      }
      for (const o of nonSgpCandidates) {
        const s = primarySport(o);
        if (!nonSgpBySport[s]) nonSgpBySport[s] = [];
        nonSgpBySport[s].push(o);
      }
      const sgpVsNonSgpBySport = {};
      const allSports = new Set([...Object.keys(sgpBySport), ...Object.keys(nonSgpBySport)]);
      for (const s of allSports) {
        const sgp = fillPct(sgpBySport[s] || []);
        const nonSgp = fillPct(nonSgpBySport[s] || []);
        sgpVsNonSgpBySport[s] = {
          sgp,
          nonSgp,
          // Delta: positive = SGPs are filling HIGHER (correlation penalty isn't biting);
          //        negative = SGPs are filling LOWER (penalty might be too aggressive)
          fillDelta: sgp.fillPct - nonSgp.fillPct,
        };
      }

      res.json({
        ok: true,
        totalCandidates: candidates.length,
        overall: {
          submitted: candidates.length,
          confirmed: candidates.filter(o => o.status === 'confirmed' || (o.status && o.status.startsWith('settled_'))).length,
        },
        // NEW: SGP vs cross-game split. If SGPs fill much worse than cross-game
        // parlays, the correlation penalty is killing volume. If SGPs fill at
        // a similar rate, the penalty is sized about right and the "below Pin"
        // optics are safe to ignore.
        sgpVsNonSgp: {
          sgp: fillPct(sgpCandidates),
          nonSgp: fillPct(nonSgpCandidates),
          withCorrelationBoost: fillPct(boostedCandidates),
          bySport: Object.fromEntries(
            Object.entries(sgpVsNonSgpBySport)
              .filter(([, v]) => v.sgp.submitted + v.nonSgp.submitted >= 10)
              .sort((a, b) => (b[1].sgp.submitted + b[1].nonSgp.submitted) - (a[1].sgp.submitted + a[1].nonSgp.submitted))
          ),
          note: 'fillDelta = sgp.fillPct - nonSgp.fillPct. Strongly negative = correlation penalty may be too aggressive for this sport.',
        },
        bySport: byKey(primarySport, 'sport'),
        byLegCount: byKey(o => bucketLegs((o.meta?.legs || o.legs || []).length), 'legs'),
        byOddsTier: byKey(o => bucketOdds(o.offeredOdds), 'odds'),
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Competitor comparison report: for every parlay where we have both our
  // offered American odds and the corresponding Pinnacle (or DK / FD)
  // compound parlay odds, measure the delta. Positive delta (ours > Pin)
  // means we offered a LARGER bettor payout than Pinnacle would — we're
  // leaking edge to bettors. Negative delta means we're tighter than
  // Pinnacle — we're uncompetitive and losing fills.
  //
  // This is a proxy for CLV (Closing Line Value). A proper CLV uses the
  // line AT EVENT START; this uses the line AT QUOTE TIME. Directionally
  // it's still the single best indicator of whether we're sharper than
  // the market.
  app.get('/competitor-report', (req, res) => {
    try {
      const all = orderTracker.getRecentOrders(20000);
      // Only consider confirmed or settled parlays (places where we actually
      // locked in an offer) where the parlay had enough book data to
      // compound a Pinnacle/DK/FD parlay value.
      const withData = all.filter(o =>
        (o.status === 'confirmed' || (o.status && o.status.startsWith('settled_')))
        && o.offeredOdds != null
        && o.meta
      );

      function summarize(getCompetitorOdds, name) {
        const pts = withData
          .map(o => ({
            ours: Number(o.offeredOdds),
            comp: getCompetitorOdds(o),
            parlayId: o.parlayId,
          }))
          .filter(p => p.comp != null && !isNaN(p.comp));
        if (pts.length === 0) return { name, count: 0 };
        const deltas = pts.map(p => p.ours - p.comp);
        const avgDelta = deltas.reduce((a, b) => a + b, 0) / deltas.length;
        const positive = deltas.filter(d => d > 0).length;
        const negative = deltas.filter(d => d < 0).length;
        const zero = deltas.filter(d => d === 0).length;
        // Convert avg American delta to implied prob delta for context
        function amToImpl(am) { return am >= 0 ? 100 / (am + 100) : Math.abs(am) / (Math.abs(am) + 100); }
        const avgOurs = pts.reduce((s, p) => s + p.ours, 0) / pts.length;
        const avgComp = pts.reduce((s, p) => s + p.comp, 0) / pts.length;
        return {
          name,
          count: pts.length,
          avgDeltaAmerican: Math.round(avgDelta),
          avgOursAmerican: Math.round(avgOurs),
          avgCompAmerican: Math.round(avgComp),
          oursHigher: positive,   // we offered better payout than competitor
          oursLower: negative,    // we offered worse payout (tighter) than competitor
          equal: zero,
          pctHigher: Math.round(positive / pts.length * 1000) / 10,
          // Median delta is more robust to outliers
          medianDelta: deltas.sort((a, b) => a - b)[Math.floor(deltas.length / 2)],
        };
      }

      res.json({
        ok: true,
        note: 'delta = (our American) - (competitor American). Positive = we gave bettor more than competitor (leaked edge). Negative = we were tighter (may be outbid).',
        pinnacle: summarize(o => o.meta?.pinnacleParlay, 'Pinnacle'),
        draftkings: summarize(o => o.meta?.draftkingsParlay, 'DraftKings'),
        fanduel: summarize(o => o.meta?.fanduelParlay, 'FanDuel'),
        kalshi: summarize(o => o.meta?.kalshiParlay, 'Kalshi'),
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Calibration report — are our fair probabilities accurate?
  //
  // Bucketizes settled parlays (and individually settled legs) by our
  // predicted probability and compares to the realized frequency the bettor
  // side actually hit. Also reports Brier score (lower = better), mean bias
  // (actual − expected; positive means we underestimate bettor wins),
  // sample count, and Wilson 95% confidence intervals for each bucket.
  app.get('/calibration-report', (req, res) => {
    try {
      const all = orderTracker.getRecentOrders(20000);
      const settled = all.filter(o => o.status && o.status.startsWith('settled_'));

      // Wilson score interval for a proportion (95% CI)
      function wilson(k, n) {
        if (n === 0) return { lo: 0, hi: 0 };
        const z = 1.96;
        const p = k / n;
        const denom = 1 + z * z / n;
        const centre = (p + z * z / (2 * n)) / denom;
        const half = z * Math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom;
        return { lo: Math.max(0, centre - half), hi: Math.min(1, centre + half) };
      }

      // Finer buckets than the existing client chart (10 instead of 5)
      const parlayBuckets = [
        { lo: 0.00, hi: 0.05, label: '0-5%' },
        { lo: 0.05, hi: 0.10, label: '5-10%' },
        { lo: 0.10, hi: 0.15, label: '10-15%' },
        { lo: 0.15, hi: 0.20, label: '15-20%' },
        { lo: 0.20, hi: 0.25, label: '20-25%' },
        { lo: 0.25, hi: 0.30, label: '25-30%' },
        { lo: 0.30, hi: 0.40, label: '30-40%' },
        { lo: 0.40, hi: 0.50, label: '40-50%' },
        { lo: 0.50, hi: 0.70, label: '50-70%' },
        { lo: 0.70, hi: 1.01, label: '70%+' },
      ];

      // Settled-lost = SP lost = bettor's parlay HIT
      function computeParlayBuckets(orders) {
        let totalBrier = 0;
        let brierCount = 0;
        let totalExpected = 0;
        let totalActual = 0;
        const bins = parlayBuckets.map(b => ({ ...b, n: 0, bettorHit: 0, expectedSum: 0 }));
        for (const o of orders) {
          const p = o.fairParlayProb;
          if (p == null || p <= 0 || p >= 1) continue;
          const hit = o.settlementResult === 'lost' ? 1 : (o.settlementResult === 'won' ? 0 : null);
          if (hit == null) continue; // skip pushes/voids — no calibration signal
          totalBrier += (hit - p) * (hit - p);
          brierCount++;
          totalExpected += p;
          totalActual += hit;
          const bin = bins.find(b => p >= b.lo && p < b.hi);
          if (bin) { bin.n++; bin.bettorHit += hit; bin.expectedSum += p; }
        }
        const detail = bins.filter(b => b.n > 0).map(b => {
          const actual = b.bettorHit / b.n;
          const expected = b.expectedSum / b.n;
          const ci = wilson(b.bettorHit, b.n);
          return {
            label: b.label, n: b.n,
            expected: Math.round(expected * 10000) / 10000,
            actual: Math.round(actual * 10000) / 10000,
            bias: Math.round((actual - expected) * 10000) / 10000,
            ci95Lo: Math.round(ci.lo * 10000) / 10000,
            ci95Hi: Math.round(ci.hi * 10000) / 10000,
          };
        });
        return {
          n: brierCount,
          brierScore: brierCount > 0 ? Math.round(totalBrier / brierCount * 100000) / 100000 : null,
          meanExpected: brierCount > 0 ? Math.round(totalExpected / brierCount * 10000) / 10000 : null,
          meanActual: brierCount > 0 ? Math.round(totalActual / brierCount * 10000) / 10000 : null,
          meanBias: brierCount > 0 ? Math.round((totalActual - totalExpected) / brierCount * 10000) / 10000 : null,
          buckets: detail,
        };
      }

      const overall = computeParlayBuckets(settled);

      // Per-sport parlay calibration
      const bySport = {};
      for (const o of settled) {
        const legs = o.meta?.legs || o.legs || [];
        const sports = [...new Set(legs.map(l => l.sport).filter(Boolean))];
        const k = sports.length === 1 ? sports[0] : 'multi';
        if (!bySport[k]) bySport[k] = [];
        bySport[k].push(o);
      }
      const bySportSummary = {};
      for (const [k, orders] of Object.entries(bySport)) {
        bySportSummary[k] = computeParlayBuckets(orders);
      }

      // Leg-level calibration — vastly more data (avg 3 legs per parlay)
      // A leg is a "hit" when settlementStatus === 'won' (bettor's side hit).
      const legBuckets = [
        { lo: 0.30, hi: 0.40, label: '30-40%' },
        { lo: 0.40, hi: 0.45, label: '40-45%' },
        { lo: 0.45, hi: 0.50, label: '45-50%' },
        { lo: 0.50, hi: 0.55, label: '50-55%' },
        { lo: 0.55, hi: 0.60, label: '55-60%' },
        { lo: 0.60, hi: 0.65, label: '60-65%' },
        { lo: 0.65, hi: 0.70, label: '65-70%' },
        { lo: 0.70, hi: 0.80, label: '70-80%' },
        { lo: 0.80, hi: 1.01, label: '80%+' },
      ];
      let legBrier = 0, legN = 0, legExpected = 0, legActual = 0;
      const legBins = legBuckets.map(b => ({ ...b, n: 0, hit: 0, expectedSum: 0 }));
      const legBySport = {};
      for (const o of settled) {
        const legs = o.legs || o.meta?.legs || [];
        for (const l of legs) {
          const p = l.fairProb;
          if (p == null || p <= 0 || p >= 1) continue;
          const ss = l.settlementStatus || l.settlement_status;
          const hit = ss === 'won' ? 1 : ss === 'lost' ? 0 : null;
          if (hit == null) continue;
          legBrier += (hit - p) * (hit - p);
          legN++;
          legExpected += p;
          legActual += hit;
          const bin = legBins.find(b => p >= b.lo && p < b.hi);
          if (bin) { bin.n++; bin.hit += hit; bin.expectedSum += p; }
          if (l.sport) {
            if (!legBySport[l.sport]) legBySport[l.sport] = { n: 0, hit: 0, expSum: 0, brier: 0 };
            legBySport[l.sport].n++;
            legBySport[l.sport].hit += hit;
            legBySport[l.sport].expSum += p;
            legBySport[l.sport].brier += (hit - p) * (hit - p);
          }
        }
      }
      const legDetail = legBins.filter(b => b.n > 0).map(b => {
        const actual = b.hit / b.n;
        const expected = b.expectedSum / b.n;
        const ci = wilson(b.hit, b.n);
        return {
          label: b.label, n: b.n,
          expected: Math.round(expected * 10000) / 10000,
          actual: Math.round(actual * 10000) / 10000,
          bias: Math.round((actual - expected) * 10000) / 10000,
          ci95Lo: Math.round(ci.lo * 10000) / 10000,
          ci95Hi: Math.round(ci.hi * 10000) / 10000,
        };
      });
      const legBySportSummary = {};
      for (const [k, v] of Object.entries(legBySport)) {
        legBySportSummary[k] = {
          n: v.n,
          meanExpected: Math.round(v.expSum / v.n * 10000) / 10000,
          meanActual: Math.round(v.hit / v.n * 10000) / 10000,
          meanBias: Math.round((v.hit / v.n - v.expSum / v.n) * 10000) / 10000,
          brierScore: Math.round(v.brier / v.n * 100000) / 100000,
        };
      }

      res.json({
        ok: true,
        note: 'bias = actual - expected (positive = we UNDERestimate bettor win rate; negative = we OVERestimate). Brier score: lower is better (0 = perfect, 0.25 = coin flip).',
        parlay: {
          overall,
          bySport: bySportSummary,
        },
        leg: {
          overall: {
            n: legN,
            brierScore: legN > 0 ? Math.round(legBrier / legN * 100000) / 100000 : null,
            meanExpected: legN > 0 ? Math.round(legExpected / legN * 10000) / 10000 : null,
            meanActual: legN > 0 ? Math.round(legActual / legN * 10000) / 10000 : null,
            meanBias: legN > 0 ? Math.round((legActual - legExpected) / legN * 10000) / 10000 : null,
            buckets: legDetail,
          },
          bySport: legBySportSummary,
        },
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // CLV (Closing Line Value) report — aggregates clvDelta across settled
  // parlays that had closing-line snapshots captured at event start. This
  // is the definitive "are we sharper than the market" metric.
  //
  // clvDelta = ourOfferedImplied - closingParlayImplied (SP perspective)
  //   positive → we priced LOOSER than close; bettor got positive CLV; bad
  //   negative → we priced TIGHTER than close; we captured edge; good
  //
  // Over a large sample, negative avg clvDelta correlates strongly with
  // positive long-run P&L. Positive avg clvDelta means sharps are picking
  // us off.
  app.get('/clv-report', (req, res) => {
    try {
      const all = orderTracker.getRecentOrders(20000);
      const settled = all.filter(o =>
        o.status && o.status.startsWith('settled_') && o.clvDelta != null
      );
      const withCoverage = all.filter(o => o.status && o.status.startsWith('settled_'));
      const sumDelta = settled.reduce((s, o) => s + (o.clvDelta || 0), 0);
      const avgDelta = settled.length > 0 ? sumDelta / settled.length : 0;

      function summarize(orders) {
        if (orders.length === 0) return { count: 0 };
        const deltas = orders.map(o => o.clvDelta).sort((a, b) => a - b);
        const median = deltas[Math.floor(deltas.length / 2)];
        const winningForSp = orders.filter(o => o.clvDelta < 0).length;
        const sumPnl = orders.reduce((s, o) => s + (o.pnl || 0), 0);
        return {
          count: orders.length,
          avgClvDelta: Math.round(avgDelta * 100000) / 100000,
          medianClvDelta: Math.round(median * 100000) / 100000,
          spSharpPct: Math.round(winningForSp / orders.length * 1000) / 10, // % where we priced tighter than close
          sumPnl: Math.round(sumPnl * 100) / 100,
        };
      }

      // Break down by sport
      const bySport = {};
      for (const o of settled) {
        const legs = o.meta?.legs || o.legs || [];
        const sports = [...new Set(legs.map(l => l.sport).filter(Boolean))];
        const k = sports.length === 1 ? sports[0] : 'multi';
        if (!bySport[k]) bySport[k] = [];
        bySport[k].push(o);
      }
      const bySportSummary = {};
      for (const [k, orders] of Object.entries(bySport)) {
        const deltas = orders.map(o => o.clvDelta).sort((a, b) => a - b);
        const avg = deltas.reduce((s, d) => s + d, 0) / deltas.length;
        const median = deltas[Math.floor(deltas.length / 2)];
        const sumPnl = orders.reduce((s, o) => s + (o.pnl || 0), 0);
        bySportSummary[k] = {
          count: orders.length,
          avgClvDelta: Math.round(avg * 100000) / 100000,
          medianClvDelta: Math.round(median * 100000) / 100000,
          sumPnl: Math.round(sumPnl * 100) / 100,
        };
      }

      // Current closing-line cache status
      const closingStatus = oddsFeed.getClosingLinesStatus
        ? oddsFeed.getClosingLinesStatus()
        : null;

      res.json({
        ok: true,
        note: 'clvDelta = offeredImplied - closingImplied (SP view). negative = SP priced tighter than close (good). positive = SP priced looser than close (bad, sharps picking us off).',
        overall: {
          ...summarize(settled),
          coveragePct: withCoverage.length > 0 ? Math.round(settled.length / withCoverage.length * 1000) / 10 : 0,
          totalSettled: withCoverage.length,
        },
        bySport: bySportSummary,
        closingLinesCache: closingStatus,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Expected Value report: sum Σ EV across confirmed & settled parlays and
  // compare to actual P&L. Over a large enough sample, Σ EV should converge
  // toward realized P&L. Persistent gaps indicate miscalibrated fair prob.
  app.get('/ev-report', (req, res) => {
    try {
      const all = orderTracker.getRecentOrders(20000);
      // Only include orders with both EV and a known outcome (settled) for
      // the main comparison; also report open positions' pending EV.
      const settled = all.filter(o => o.status && o.status.startsWith('settled_'));
      const open = all.filter(o => o.status === 'confirmed');
      const withEv = settled.filter(o => o.expectedValue != null);
      const openWithEv = open.filter(o => o.expectedValue != null);

      const sumEv = withEv.reduce((s, o) => s + (o.expectedValue || 0), 0);
      const sumPnl = withEv.reduce((s, o) => s + (o.pnl || 0), 0);
      const sumOpenEv = openWithEv.reduce((s, o) => s + (o.expectedValue || 0), 0);

      // By sport breakdown
      const bySport = {};
      for (const o of withEv) {
        const legs = o.meta?.legs || o.legs || [];
        const sports = [...new Set(legs.map(l => l.sport).filter(Boolean))];
        const key = sports.length === 1 ? sports[0] : (sports.length > 1 ? 'multi' : 'unknown');
        if (!bySport[key]) bySport[key] = { count: 0, sumEv: 0, sumPnl: 0 };
        bySport[key].count++;
        bySport[key].sumEv += o.expectedValue || 0;
        bySport[key].sumPnl += o.pnl || 0;
      }
      for (const k of Object.keys(bySport)) {
        bySport[k].delta = Math.round((bySport[k].sumPnl - bySport[k].sumEv) * 100) / 100;
        bySport[k].sumEv = Math.round(bySport[k].sumEv * 100) / 100;
        bySport[k].sumPnl = Math.round(bySport[k].sumPnl * 100) / 100;
      }

      res.json({
        ok: true,
        settled: {
          count: withEv.length,
          totalSettled: settled.length,
          withEvPct: settled.length > 0 ? Math.round(withEv.length / settled.length * 1000) / 10 : 0,
          sumExpectedValue: Math.round(sumEv * 100) / 100,
          sumActualPnl: Math.round(sumPnl * 100) / 100,
          delta: Math.round((sumPnl - sumEv) * 100) / 100,
          note: 'delta = actualPnL - expectedValue; positive = variance in our favor, negative = fair prob underestimating bettor wins',
        },
        open: {
          count: openWithEv.length,
          totalOpen: open.length,
          sumExpectedValue: Math.round(sumOpenEv * 100) / 100,
        },
        bySport,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Reconcile all settled orders between PX and local. Returns only mismatches.
  app.get('/reconcile-settlements', async (req, res) => {
    try {
      const limit = Number(req.query.limit) || 1000;
      const pxOrders = await px.fetchOrders(limit);

      const mismatches = [];
      const missingLocal = []; // PX has settled, we don't
      const missingPx = [];    // we have settled, PX doesn't see it
      const matched = [];

      // Build a PX lookup by parlay_id for fast pairing
      const pxByParlayId = {};
      const pxByUuid = {};
      for (const o of pxOrders) {
        const pid = o.p_id || o.parlay_id;
        if (pid) pxByParlayId[pid] = o;
        if (o.order_uuid) pxByUuid[o.order_uuid] = o;
      }

      // Check every settled PX order against local
      for (const pxOrder of pxOrders) {
        const pxStatus = pxOrder.settlement_status;
        if (!pxStatus || ['tbd','requested','none',''].includes(pxStatus)) continue;
        const pid = pxOrder.p_id || pxOrder.parlay_id;
        const local = (pid && orderTracker.findByParlayId(pid))
          || (pxOrder.order_uuid && orderTracker.findByOrderUuid(pxOrder.order_uuid))
          || null;

        if (!local) {
          missingLocal.push({
            parlayId: pid,
            orderUuid: pxOrder.order_uuid,
            pxStatus,
            pxStake: pxOrder.stake != null ? Number(pxOrder.stake) : null,
            pxProfit: pxOrder.profit != null ? Number(pxOrder.profit) : null,
          });
          continue;
        }

        const localStatus = local.status && local.status.startsWith('settled_')
          ? local.status.substring('settled_'.length)
          : local.status;
        const pxProfit = pxOrder.profit != null ? Number(pxOrder.profit) : null;
        const pxStake = pxOrder.stake != null ? Number(pxOrder.stake) : null;

        const statusMismatch = pxStatus !== localStatus;
        const pnlMismatch = pxProfit != null && local.pnl != null && Math.abs(pxProfit - local.pnl) > 0.01;
        const stakeMismatch = pxStake != null && local.confirmedStake != null && Math.abs(pxStake - local.confirmedStake) > 0.01;

        if (statusMismatch || pnlMismatch || stakeMismatch) {
          mismatches.push({
            parlayId: pid,
            orderUuid: pxOrder.order_uuid,
            status: statusMismatch ? { px: pxStatus, local: localStatus } : undefined,
            pnl: pnlMismatch ? { px: pxProfit, local: local.pnl } : undefined,
            stake: stakeMismatch ? { px: pxStake, local: local.confirmedStake } : undefined,
          });
        } else {
          matched.push({ parlayId: pid, status: pxStatus, pnl: pxProfit });
        }
      }

      // Find local settled orders that PX doesn't know about
      const allLocal = orderTracker.getRecentOrders(10000);
      for (const local of allLocal) {
        if (!local.status || !local.status.startsWith('settled_')) continue;
        const pxMatch = (local.parlayId && pxByParlayId[local.parlayId])
          || (local.orderUuid && pxByUuid[local.orderUuid]);
        if (!pxMatch) {
          missingPx.push({
            parlayId: local.parlayId,
            orderUuid: local.orderUuid,
            localStatus: local.status.substring('settled_'.length),
            localPnl: local.pnl,
          });
        }
      }

      res.json({
        ok: true,
        summary: {
          pxSettledTotal: pxOrders.filter(o => o.settlement_status && !['tbd','requested','none',''].includes(o.settlement_status)).length,
          matched: matched.length,
          mismatches: mismatches.length,
          missingLocal: missingLocal.length,
          missingPx: missingPx.length,
        },
        mismatches,
        missingLocal,
        missingPx,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Pause/resume RFQ handling
  app.post('/pause', (req, res) => {
    websocket.pause();
    try { require('./services/push').notifyConnectionState('paused', 'RFQ handling paused via /pause'); } catch (_) {}
    res.json({ ok: true, paused: true });
  });

  app.post('/resume', (req, res) => {
    websocket.resume();
    try { require('./services/push').notifyConnectionState('resumed', 'RFQ handling resumed via /resume'); } catch (_) {}
    res.json({ ok: true, paused: false });
  });

  // Admin: send a Telegram message to the operator's chat. Useful for
  // background agents to surface findings without polling the dashboard,
  // or one-shot triage notifications. No-ops if TELEGRAM_BOT_TOKEN /
  // TELEGRAM_CHAT_ID env vars aren't set.
  // Body: { message: string, parseMode?: 'Markdown'|'HTML' }
  app.post('/admin/notify', async (req, res) => {
    try {
      const telegram = require('./services/telegram');
      const { message, parseMode } = req.body || {};
      if (!message || typeof message !== 'string') {
        return res.status(400).json({ ok: false, error: 'message (string) required' });
      }
      const out = await telegram.sendMessage(message, { parseMode });
      res.status(out.ok ? 200 : 400).json(out);
    } catch (err) {
      log.error('API', `/admin/notify failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Admin: manually disable / enable specific lines or whole events.
  // When a sportsbook pulls lines on a delayed game and our cache shows
  // stale prices, the operator clicks Disable in the Lines table to
  // make this line/event auto-decline at the pricer level instead of
  // pausing the whole service. State is in-memory and clears on restart.
  // Body: { lineId } or { pxEventId }
  app.post('/admin/disable-line', (req, res) => {
    const { lineId } = req.body || {};
    if (!lineId) return res.status(400).json({ ok: false, error: 'lineId required' });
    lineManager.disableLine(String(lineId));
    log.warn('AdminDisable', `Line ${lineId} disabled`);
    res.json({ ok: true, lineId, disabled: true });
  });
  app.post('/admin/enable-line', (req, res) => {
    const { lineId } = req.body || {};
    if (!lineId) return res.status(400).json({ ok: false, error: 'lineId required' });
    lineManager.enableLine(String(lineId));
    log.warn('AdminDisable', `Line ${lineId} enabled`);
    res.json({ ok: true, lineId, disabled: false });
  });
  app.post('/admin/disable-event', (req, res) => {
    const { pxEventId } = req.body || {};
    if (pxEventId == null) return res.status(400).json({ ok: false, error: 'pxEventId required' });
    lineManager.disablePxEvent(String(pxEventId));
    log.warn('AdminDisable', `Event ${pxEventId} disabled (all lines)`);
    res.json({ ok: true, pxEventId, disabled: true });
  });
  app.post('/admin/enable-event', (req, res) => {
    const { pxEventId } = req.body || {};
    if (pxEventId == null) return res.status(400).json({ ok: false, error: 'pxEventId required' });
    lineManager.enablePxEvent(String(pxEventId));
    log.warn('AdminDisable', `Event ${pxEventId} enabled (all lines)`);
    res.json({ ok: true, pxEventId, disabled: false });
  });
  app.get('/admin/disabled', (req, res) => {
    res.json(lineManager.getDisabledSnapshot());
  });

  // Admin: manually override a leg's inferredResult. For cases where the
  // automatic re-validation can't heal a wrongly-set leg (e.g. the score
  // source no longer carries the historical day's game).
  // Body: { parlayId, team, market?, inferredResult: 'won' | 'lost' | 'push' | null }
  app.post('/admin/override-leg-result', async (req, res) => {
    try {
      const { parlayId, team, market, inferredResult } = req.body || {};
      const out = await orderTracker.overrideLegResult(parlayId, team, market, inferredResult);
      res.status(out.ok ? 200 : 400).json(out);
    } catch (err) {
      log.error('API', `/admin/override-leg-result failed: ${err.message}`);
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // ---------------------------------------------------------------------------
  // VIG CONFIGURATION — per-sport vig overrides
  // ---------------------------------------------------------------------------

  // GET current vig settings
  app.get('/config/vig', (req, res) => {
    const vigBySport = config.pricing.vigBySport || {};
    const defaultVig = config.pricing.defaultVig;
    // Build a display map showing effective vig per known sport
    const sports = [
      'basketball_nba', 'basketball_ncaab', 'basketball_wnba',
      'baseball_mlb', 'icehockey_nhl', 'tennis',
      'soccer', 'soccer_usa_mls', 'soccer_epl',
      'soccer_uefa_champs_league', 'soccer_uefa_europa_league',
      'soccer_spain_la_liga', 'soccer_italy_serie_a',
      'soccer_germany_bundesliga', 'soccer_france_ligue_one',
      'golf_matchups', 'mma_mixed_martial_arts', 'boxing_boxing',
    ];
    const effective = {};
    for (const s of sports) {
      effective[s] = {
        vig: vigBySport[s] != null ? vigBySport[s] : defaultVig,
        isOverride: vigBySport[s] != null,
        pct: ((vigBySport[s] != null ? vigBySport[s] : defaultVig) * 100).toFixed(2) + '%',
      };
    }
    res.json({
      defaultVig,
      defaultVigPct: (defaultVig * 100).toFixed(2) + '%',
      overrides: vigBySport,
      effective,
      parlayLevelVig: !!config.pricing.parlayLevelVig,
    });
  });

  // POST update vig — body: { sport: "basketball_nba", vig: 0.005 }
  // or { sport: "basketball_nba", vigPct: 0.5 } (0.5%)
  // or { defaultVig: 0.002 } to change the global default
  // or { sport: "basketball_nba", reset: true } to remove override
  app.post('/config/vig', (req, res) => {
    const body = req.body || {};

    // Update global default
    if (body.defaultVig != null) {
      const val = parseFloat(body.defaultVig);
      if (isNaN(val) || val < 0 || val > 0.20) {
        return res.status(400).json({ ok: false, error: 'defaultVig must be 0-0.20 (0-20%)' });
      }
      config.pricing.defaultVig = val;
      log.info('Config', `Default vig updated to ${(val * 100).toFixed(2)}%`);
    }

    // Update per-sport vig
    if (body.sport) {
      if (!config.pricing.vigBySport) config.pricing.vigBySport = {};

      if (body.reset) {
        delete config.pricing.vigBySport[body.sport];
        log.info('Config', `Vig override removed for ${body.sport} — falls back to default ${(config.pricing.defaultVig * 100).toFixed(2)}%`);
      } else {
        let val = body.vig != null ? parseFloat(body.vig) : null;
        if (val == null && body.vigPct != null) val = parseFloat(body.vigPct) / 100;
        if (val == null || isNaN(val) || val < 0 || val > 0.20) {
          return res.status(400).json({ ok: false, error: 'vig must be 0-0.20, or vigPct must be 0-20' });
        }
        config.pricing.vigBySport[body.sport] = val;
        log.info('Config', `Vig for ${body.sport} set to ${(val * 100).toFixed(2)}%`);
      }
    }

    // Toggle parlay-level-vig A/B flag at runtime.
    // Body: {parlayLevelVig: true|false}
    if (body.parlayLevelVig != null) {
      const on = !!body.parlayLevelVig;
      config.pricing.parlayLevelVig = on;
      log.info('Config', `Parlay-level vig mode: ${on ? 'ON (max-per-leg applied once)' : 'OFF (per-leg compounded)'}`);
    }

    res.json({
      ok: true,
      defaultVig: config.pricing.defaultVig,
      defaultVigPct: (config.pricing.defaultVig * 100).toFixed(2) + '%',
      overrides: config.pricing.vigBySport,
      parlayLevelVig: !!config.pricing.parlayLevelVig,
    });
  });

  // v2 pricing-engine config: view + tune knobs without a redeploy.
  // Works because pricer.js reads config.pricing.pricingV2* on every call,
  // so live writes here take effect on the very next RFQ.
  app.get('/config/v2', (req, res) => {
    res.json({
      pricingV2Enabled: !!config.pricing.pricingV2Enabled,
      pricingV2Live: !!config.pricing.pricingV2Live,
      pricingV2LivePercent: config.pricing.pricingV2LivePercent || 0,
      pricingV2TargetEdge: config.pricing.pricingV2TargetEdge,
      pricingV2KSigma: config.pricing.pricingV2KSigma,
    });
  });

  app.post('/config/v2', (req, res) => {
    const body = req.body || {};
    const changed = {};

    if (body.pricingV2Enabled != null) {
      config.pricing.pricingV2Enabled = !!body.pricingV2Enabled;
      changed.pricingV2Enabled = config.pricing.pricingV2Enabled;
    }
    if (body.pricingV2Live != null) {
      config.pricing.pricingV2Live = !!body.pricingV2Live;
      changed.pricingV2Live = config.pricing.pricingV2Live;
    }
    if (body.pricingV2LivePercent != null) {
      const v = parseInt(body.pricingV2LivePercent);
      if (!Number.isFinite(v) || v < 0 || v > 100) {
        return res.status(400).json({ ok: false, error: 'pricingV2LivePercent must be 0 to 100 (integer)' });
      }
      config.pricing.pricingV2LivePercent = v;
      changed.pricingV2LivePercent = v;
    }
    if (body.pricingV2TargetEdge != null) {
      const v = parseFloat(body.pricingV2TargetEdge);
      if (!Number.isFinite(v) || v < 0 || v >= 0.20) {
        return res.status(400).json({ ok: false, error: 'pricingV2TargetEdge must be 0 to 0.20 (0-20%)' });
      }
      config.pricing.pricingV2TargetEdge = v;
      changed.pricingV2TargetEdge = v;
    }
    if (body.pricingV2KSigma != null) {
      const v = parseFloat(body.pricingV2KSigma);
      if (!Number.isFinite(v) || v < 0 || v > 2) {
        return res.status(400).json({ ok: false, error: 'pricingV2KSigma must be 0 to 2' });
      }
      config.pricing.pricingV2KSigma = v;
      changed.pricingV2KSigma = v;
    }

    if (Object.keys(changed).length === 0) {
      return res.status(400).json({ ok: false, error: 'no recognized fields (pricingV2Enabled|pricingV2Live|pricingV2LivePercent|pricingV2TargetEdge|pricingV2KSigma)' });
    }

    log.info('Config', `v2 updated: ${JSON.stringify(changed)}`);
    res.json({
      ok: true,
      changed,
      current: {
        pricingV2Enabled: !!config.pricing.pricingV2Enabled,
        pricingV2Live: !!config.pricing.pricingV2Live,
        pricingV2LivePercent: config.pricing.pricingV2LivePercent || 0,
        pricingV2TargetEdge: config.pricing.pricingV2TargetEdge,
        pricingV2KSigma: config.pricing.pricingV2KSigma,
      },
    });
  });

  // Force WebSocket reconnect
  app.post('/reconnect', async (req, res) => {
    try {
      log.info('API', 'Manual WebSocket reconnect requested');
      px.clearCooldown(); // allow login even if cooldown is active
      websocket.disconnect();
      await websocket.connect();
      res.json({ ok: true, state: websocket.getState().connectionState });
    } catch (err) {
      res.json({ ok: false, error: err.message });
    }
  });

  // Response time stats + offer errors for diagnosing auction issues
  app.get('/response-times', (req, res) => {
    res.json(websocket.getResponseTimeStats());
  });

  // List cached odds events (debugging)
  app.get('/odds-events', (req, res) => {
    res.json({ events: oddsFeed.getAllCachedEvents() });
  });

  // TEMPORARY: diagnose why tennis lines aren't registering despite
  // tennis events being in the odds cache. Returns PX's tennis event
  // list + current odds-cache teams + per-event registered status.
  // TEMPORARY: probe The Odds API for MLB F5 markets to see which
  // games actually have F5 h2h / spreads / totals vs missing.
  // Generic probe — pass ?markets=... to test arbitrary Odds API market
  // keys. Defaults to 1st-inning markets since that's the current
  // question. Returns per-event per-market book coverage so we can
  // see which books report what.
  app.get('/debug-oapi-mlb-probe', async (req, res) => {
    const key = process.env.THE_ODDS_API_KEY;
    if (!key) return res.status(500).json({ error: 'no key' });
    const markets = req.query.markets ||
      'h2h_1st_1_innings,spreads_1st_1_innings,totals_1st_1_innings';
    const url = 'https://api.the-odds-api.com/v4/sports/baseball_mlb/odds'
      + '?apiKey=' + key
      + '&regions=us,eu'
      + '&markets=' + markets
      + '&bookmakers=pinnacle,draftkings,fanduel,betonlineag,bovada,betmgm'
      + '&oddsFormat=american';
    try {
      const r = await fetch(url);
      const text = await r.text();
      let body;
      try { body = JSON.parse(text); } catch { body = null; }
      if (!Array.isArray(body)) {
        return res.json({ ok: false, status: r.status, markets, rawBody: text.slice(0, 800) });
      }
      // Aggregate: how many events had each market, and which books posted it
      const marketCoverage = {};
      for (const e of body) {
        for (const b of (e.bookmakers || [])) {
          for (const m of (b.markets || [])) {
            if (!marketCoverage[m.key]) marketCoverage[m.key] = { events: 0, books: new Set() };
            marketCoverage[m.key].events++;
            marketCoverage[m.key].books.add(b.key);
          }
        }
      }
      // Sample output: first 3 events with their market breakdown
      const sample = body.slice(0, 3).map(e => {
        const mkTypes = {};
        for (const b of (e.bookmakers || [])) {
          for (const m of (b.markets || [])) {
            if (!mkTypes[m.key]) mkTypes[m.key] = new Set();
            mkTypes[m.key].add(b.key);
          }
        }
        return {
          home: e.home_team, away: e.away_team,
          commenceTime: e.commence_time,
          markets: Object.fromEntries(Object.entries(mkTypes).map(([k, v]) => [k, [...v]])),
        };
      });
      res.json({
        ok: true,
        status: r.status,
        markets,
        eventCount: body.length,
        marketCoverage: Object.fromEntries(
          Object.entries(marketCoverage).map(([k, v]) => [k, { events: v.events, books: [...v.books] }])
        ),
        sample,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Bovada alt-line scraper — status + manual refresh triggers.
  // Phase 1 scaffolding: scraper runs in isolation, NOT yet wired into
  // the pricer cascade. Use these endpoints to verify it works before
  // integration (Phase 2).
  app.get('/bovada-alt-status', (req, res) => {
    res.json({ ok: true, status: bovadaAltScraper.getStatus() });
  });

  // Force a refresh. ?sport=basketball_nba|icehockey_nhl|baseball_mlb
  // or no query to refresh all three.
  app.post('/bovada-alt-refresh', async (req, res) => {
    try {
      if (req.query.sport) {
        const result = await bovadaAltScraper.refreshSport(req.query.sport);
        res.json({ ok: true, sport: req.query.sport, result });
      } else {
        const results = await bovadaAltScraper.refreshAll();
        res.json({ ok: true, results });
      }
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Inspect a single cached event's parsed market data. Useful for
  // spot-checking that the scraper produces what we expect before
  // Phase 2 pricer integration goes live.
  //   /bovada-alt-event?home=Atlanta+Hawks&away=New+York+Knicks
  app.get('/bovada-alt-event', (req, res) => {
    const { home, away } = req.query;
    if (!home || !away) return res.status(400).json({ error: 'need ?home=...&away=...' });
    const entry = bovadaAltScraper.getCachedEvent('', home, away);
    if (!entry) return res.json({ ok: false, reason: 'no cache entry', searchedKey: bovadaAltScraper.normalizeEventKey(home, away) });
    res.json({ ok: true, event: entry });
  });

  // Bovada API discovery probe — tests Bovada's public coupon API.
  // Bovada is offshore + minimal anti-bot, so this usually works with
  // vanilla fetch. If it returns JSON, we have a scraper-free path to
  // alt-line markets for NBA/NHL halves and team totals.
  //
  //   /debug-bovada-probe              — tries NBA default
  //   /debug-bovada-probe?sport=nhl    — hockey
  //   /debug-bovada-probe?sport=mlb    — baseball
  app.get('/debug-bovada-probe', async (req, res) => {
    const userUrl = req.query.url;
    const sport = (req.query.sport || 'nba').toLowerCase();
    const SPORT_PATHS = {
      nba: 'basketball/nba',
      nhl: 'hockey/nhl',
      mlb: 'baseball/mlb',
      ncaab: 'basketball/ncaab-mens-basketball',
    };
    const path = SPORT_PATHS[sport] || SPORT_PATHS.nba;
    const defaultProbes = [
      // Coupon API — events + primary markets
      `https://www.bovada.lv/services/sports/event/coupon/events/A/description/${path}?marketFilterId=def&preMatchOnly=true&eventsLimit=50&lang=en`,
      // Sometimes the public v2 endpoint carries alt line markets too
      `https://www.bovada.lv/services/sports/event/v2/events/A/description/${path}?marketFilterId=def&preMatchOnly=true&eventsLimit=50&lang=en`,
      // Category listings for a league — tells us what props are available
      `https://www.bovada.lv/services/sports/category/${path}?preMatchOnly=true&lang=en`,
    ];
    const probes = userUrl ? [userUrl] : defaultProbes;
    const results = [];
    for (const url of probes) {
      const t0 = Date.now();
      try {
        const r = await fetch(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.bovada.lv/sports',
          },
        });
        const elapsedMs = Date.now() - t0;
        const ct = r.headers.get('content-type') || '';
        const text = await r.text();
        let body = null;
        if (ct.includes('json')) {
          try { body = JSON.parse(text); } catch { /* ignore */ }
        }
        // Bovada responses are typically [{ events: [...] }] arrays
        let summary = null;
        if (Array.isArray(body) && body[0]) {
          const root = body[0];
          const events = root.events || root.categories || [];
          summary = {
            rootKeys: Object.keys(root).slice(0, 10),
            eventCount: Array.isArray(events) ? events.length : null,
            sampleEvent: Array.isArray(events) && events[0] ? {
              id: events[0].id,
              description: events[0].description,
              link: events[0].link,
              competitionId: events[0].competitionId,
              startTime: events[0].startTime,
              displayGroupCount: Array.isArray(events[0].displayGroups) ? events[0].displayGroups.length : null,
              displayGroups: Array.isArray(events[0].displayGroups)
                ? events[0].displayGroups.slice(0, 20).map(dg => ({
                    description: dg.description,
                    marketCount: Array.isArray(dg.markets) ? dg.markets.length : null,
                    // Raised from 8 to 50 so we can see complete market lists
                    // in groups like Alternate Lines (18 markets — 8-limit
                    // was truncating exactly what we need to identify).
                    sampleMarkets: Array.isArray(dg.markets)
                      ? dg.markets.slice(0, 50).map(m => ({
                          description: m.description,
                          period: m.period?.description,
                          periodId: m.period?.id,
                          outcomeCount: Array.isArray(m.outcomes) ? m.outcomes.length : null,
                          sampleOutcomes: Array.isArray(m.outcomes)
                            ? m.outcomes.slice(0, 2).map(o => ({
                                description: o.description,
                                american: o.price?.american,
                                handicap: o.price?.handicap,
                              }))
                            : null,
                        }))
                      : null,
                  }))
                : null,
            } : null,
          };
        } else if (body && typeof body === 'object') {
          summary = {
            topLevelKeys: Object.keys(body).slice(0, 10),
          };
        }
        results.push({
          url,
          status: r.status,
          elapsedMs,
          contentType: ct,
          bodyLength: text.length,
          bodyPreview: text.slice(0, 300),
          summary,
        });
      } catch (err) {
        results.push({ url, error: err.message });
      }
    }
    res.json({ ok: true, sport, results });
  });

  // BetMGM API discovery probe — tries to fetch JSON directly from MGM's
  // public cds-api endpoints. Unlike DK, BetMGM exposes market data via
  // a REST API that typically doesn't require full browser rendering.
  // If these endpoints work with vanilla fetch, we skip Puppeteer entirely
  // for the MGM scraper — much faster and more reliable than SPA scraping.
  //
  //   /debug-mgm-probe                     — tries a handful of default URLs
  //   /debug-mgm-probe?url=<full_url>      — probe a specific URL
  //   /debug-mgm-probe?subdivision=US-NJ   — try a specific state subdomain
  app.get('/debug-mgm-probe', async (req, res) => {
    const userUrl = req.query.url;
    // MGM has state-specific subdomains (NJ, MI, PA, etc.). Some data is
    // accessible across states; some is gated. Try a few defaults.
    const subdivision = req.query.subdivision || 'US-NJ';
    const state = subdivision.split('-')[1]?.toLowerCase() || 'nj';

    // Common MGM API URL patterns to probe. Each tests a different endpoint
    // shape; we want to find one that returns JSON without requiring
    // auth tokens beyond basic query-string params.
    const defaultProbes = [
      // Public fixtures list (most-useful if accessible)
      `https://sports.${state}.betmgm.com/cds-api/bettingoffer/fixtures?x-bwin-accessid=aW50Om1vYjo&lang=en-us&country=US&userCountry=US&subdivision=${subdivision}&sportIds=7&state=Upcoming`,
      // Alternative: competitions (NBA)
      `https://sports.${state}.betmgm.com/cds-api/bettingoffer/competitions?x-bwin-accessid=aW50Om1vYjo&lang=en-us&country=US&userCountry=US&subdivision=${subdivision}&sportIds=7`,
      // Nested fixtures under NBA league id (103 is NBA on MGM typically)
      `https://sports.${state}.betmgm.com/cds-api/bettingoffer/fixtures?x-bwin-accessid=aW50Om1vYjo&lang=en-us&country=US&userCountry=US&subdivision=${subdivision}&sportIds=7&competitionIds=103`,
    ];
    const probes = userUrl ? [userUrl] : defaultProbes;
    const results = [];
    for (const url of probes) {
      const t0 = Date.now();
      try {
        const r = await fetch(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
          },
        });
        const elapsedMs = Date.now() - t0;
        const ct = r.headers.get('content-type') || '';
        const bodyText = await r.text();
        let bodyJson = null, parseErr = null;
        if (ct.includes('json')) {
          try { bodyJson = JSON.parse(bodyText); } catch (e) { parseErr = e.message; }
        }
        // Extract useful summary from the JSON if present
        let summary = null;
        if (bodyJson) {
          const keys = Object.keys(bodyJson).slice(0, 10);
          // Common MGM shapes
          const fixtures = bodyJson.fixtures || bodyJson.Fixtures || bodyJson.events || bodyJson.payload?.fixtures;
          const competitions = bodyJson.competitions || bodyJson.Competitions;
          summary = {
            topLevelKeys: keys,
            fixtureCount: Array.isArray(fixtures) ? fixtures.length : null,
            competitionCount: Array.isArray(competitions) ? competitions.length : null,
            sampleFixture: Array.isArray(fixtures) && fixtures[0] ? {
              id: fixtures[0].id,
              name: fixtures[0].name?.value || fixtures[0].name,
              startDate: fixtures[0].startDate || fixtures[0].startTime,
              hasGames: !!fixtures[0].games,
              gameCount: Array.isArray(fixtures[0].games) ? fixtures[0].games.length : null,
            } : null,
          };
        }
        results.push({
          url,
          status: r.status,
          elapsedMs,
          contentType: ct,
          parseErr,
          bodyLength: bodyText.length,
          bodyPreview: bodyText.slice(0, 300),
          summary,
        });
      } catch (err) {
        results.push({ url, error: err.message });
      }
    }
    res.json({ ok: true, subdivision, probeCount: probes.length, results });
  });

  // DK scraper discovery probe — navigates to a DK URL, optionally follows
  // up with a per-event detail-page visit, and returns a summary of every
  // marketType.name captured from XHRs plus sample selection shapes.
  //
  // Used during Phase 0 of the DK alt-line scraper build to identify:
  //  - Which DK subcategory slugs carry NBA 1H, NHL 1st Period, team totals
  //  - Exact marketType.name strings for parser filters
  //  - Per-event detail-page timing (validates the 2-min cycle budget)
  //
  // Example calls:
  //   /debug-dk-probe?url=https://sportsbook.draftkings.com/leagues/basketball/nba&sub=1st-half
  //   /debug-dk-probe?url=https://sportsbook.draftkings.com/leagues/basketball/nba&sub=team-totals&eventDetail=1
  //   /debug-dk-probe?url=https://sportsbook.draftkings.com/leagues/hockey/nhl&sub=1st-period
  //
  // Expect 15-90s for a primary-page-only probe, 30-120s with event detail.
  app.get('/debug-dk-probe', async (req, res) => {
    const url = req.query.url;
    if (!url) return res.status(400).json({ error: 'need ?url=...' });
    if (!url.startsWith('https://sportsbook.draftkings.com/')) {
      return res.status(400).json({ error: 'url must start with https://sportsbook.draftkings.com/' });
    }
    const subcategory = req.query.sub || null;
    const postWaitMs = parseInt(req.query.waitMs) || 10000;
    const eventDetailNav = req.query.eventDetail === '1' || req.query.eventDetail === 'true';
    const maxEventDetails = parseInt(req.query.maxDetails) || 3;
    try {
      const capture = await dkScraper.probeDkPage({ url, subcategory, postWaitMs, eventDetailNav, maxEventDetails });
      res.json({ ok: true, ...capture });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Probe PX's raw market catalog for an event. Useful for diagnosing why
  // a market type we "think" we support doesn't end up in the line index —
  // usually because PX's actual `m.type` string doesn't match our allowlist.
  //   /debug-px-markets?eventId=30025304
  //   /debug-px-markets?sport=basketball_nba   (shows first 2 events' data)
  app.get('/debug-px-markets', async (req, res) => {
    try {
      let eventIds = [];
      if (req.query.eventId) {
        eventIds = [req.query.eventId];
      } else if (req.query.sport) {
        // Fetch sport events, filter to requested sport, take first few
        const sportKey = req.query.sport;
        const all = await px.fetchSportEvents();
        const matches = (all || []).filter(e => {
          const sn = (e.sport_name || '').toLowerCase();
          const tn = (e.tournament_name || '').toLowerCase();
          if (sportKey === 'basketball_nba') return /nba/.test(sn + tn);
          if (sportKey === 'baseball_mlb') return /mlb/.test(sn + tn);
          if (sportKey === 'icehockey_nhl') return /nhl/.test(sn + tn);
          return false;
        });
        eventIds = matches.slice(0, 2).map(e => e.event_id);
      } else {
        return res.status(400).json({ error: 'need ?eventId=... or ?sport=...' });
      }

      const results = [];
      for (const eid of eventIds) {
        const markets = await px.fetchMarkets(eid);
        const typeCounts = {};
        for (const m of markets) typeCounts[m.type] = (typeCounts[m.type] || 0) + 1;
        const h1Candidates = markets.filter(m =>
          /first\s*half|1st\s*half|h1\b/i.test(m.name || '') ||
          /first_half|1st_half|_h1\b|_half|half_/i.test(m.type || '')
        ).map(m => ({
          type: m.type,
          name: m.name,
          status: m.status,
          lineCount: (m.market_lines || []).length,
          sampleLine: (m.market_lines || [])[0] ? {
            line: m.market_lines[0].line,
            selection_name: m.market_lines[0].selection_name,
          } : null,
        }));
        results.push({
          eventId: eid,
          totalMarkets: markets.length,
          typeCounts,
          h1Candidates,
          sampleMarketNames: markets.slice(0, 20).map(m => ({ type: m.type, name: m.name })),
        });
      }
      res.json({ ok: true, eventIds, results });
    } catch (err) {
      res.status(500).json({ error: err.message, stack: err.stack });
    }
  });

  // Generic Odds API probe — used to diagnose coverage for any sport+market
  // combination. Pre-canned URLs for team_totals and NBA 1H below.
  //   /debug-odds-market-probe?sport=basketball_nba&markets=team_totals
  //   /debug-odds-market-probe?sport=basketball_nba&markets=h2h_h1,spreads_h1,totals_h1
  //   /debug-odds-market-probe?sport=baseball_mlb&markets=team_totals
  //   /debug-odds-market-probe?sport=icehockey_nhl&markets=team_totals
  app.get('/debug-odds-market-probe', async (req, res) => {
    const key = process.env.THE_ODDS_API_KEY;
    if (!key) return res.status(500).json({ error: 'no key' });
    const sport = req.query.sport;
    const markets = req.query.markets;
    if (!sport || !markets) {
      return res.status(400).json({ error: 'need ?sport=...&markets=...' });
    }
    const url = `https://api.the-odds-api.com/v4/sports/${sport}/odds`
      + `?apiKey=${key}&regions=us,eu&markets=${markets}`
      + `&bookmakers=pinnacle,draftkings,fanduel`
      + `&oddsFormat=american`;
    try {
      const r = await fetch(url);
      const text = await r.text();
      let body;
      try { body = JSON.parse(text); } catch { body = null; }
      if (!Array.isArray(body)) {
        return res.json({ ok: false, status: r.status, sport, markets, rawBody: text.slice(0, 800) });
      }
      // Aggregate coverage across events
      const marketCoverage = {};
      let outcomeExemplar = null;
      for (const e of body) {
        for (const b of (e.bookmakers || [])) {
          for (const m of (b.markets || [])) {
            if (!marketCoverage[m.key]) marketCoverage[m.key] = { events: 0, books: new Set(), outcomes: 0 };
            marketCoverage[m.key].events++;
            marketCoverage[m.key].books.add(b.key);
            marketCoverage[m.key].outcomes += (m.outcomes || []).length;
            if (!outcomeExemplar && (m.outcomes || []).length > 0) {
              outcomeExemplar = { market: m.key, book: b.key, outcomes: m.outcomes.slice(0, 4) };
            }
          }
        }
      }
      res.json({
        ok: true,
        status: r.status,
        sport, markets,
        eventCount: body.length,
        remainingCalls: r.headers.get('x-requests-remaining'),
        marketCoverage: Object.fromEntries(
          Object.entries(marketCoverage).map(([k, v]) => [k, { events: v.events, books: [...v.books], outcomes: v.outcomes }])
        ),
        outcomeExemplar,
        sampleEvents: body.slice(0, 2).map(e => ({
          home: e.home_team,
          away: e.away_team,
          commenceTime: e.commence_time,
          bookCount: (e.bookmakers || []).length,
          marketsPresent: [...new Set((e.bookmakers || []).flatMap(b => (b.markets || []).map(m => m.key)))],
        })),
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/debug-oapi-mlb-f5', async (req, res) => {
    const key = process.env.THE_ODDS_API_KEY;
    if (!key) return res.status(500).json({ error: 'no key' });
    const url = 'https://api.the-odds-api.com/v4/sports/baseball_mlb/odds'
      + '?apiKey=' + key
      + '&regions=us,eu'
      + '&markets=h2h_1st_5_innings,spreads_1st_5_innings,totals_1st_5_innings'
      + '&bookmakers=pinnacle,draftkings,fanduel'
      + '&oddsFormat=american';
    try {
      const r = await fetch(url);
      const text = await r.text();
      let body;
      try { body = JSON.parse(text); } catch { body = null; }
      if (!Array.isArray(body)) {
        return res.json({ ok: false, status: r.status, rawBody: text.slice(0, 800) });
      }
      const summary = body.map(e => {
        const mkTypes = {};
        for (const b of (e.bookmakers || [])) {
          for (const m of (b.markets || [])) {
            if (!mkTypes[m.key]) mkTypes[m.key] = new Set();
            mkTypes[m.key].add(b.key);
          }
        }
        return {
          home: e.home_team, away: e.away_team,
          commenceTime: e.commence_time,
          markets: Object.fromEntries(Object.entries(mkTypes).map(([k, v]) => [k, [...v]])),
        };
      });
      res.json({ ok: true, status: r.status, eventCount: body.length, events: summary });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // TEMPORARY: dump raw PX market names/types for a given event so we
  // can see what PX actually publishes for series spread markets.
  app.get('/debug-px-markets', async (req, res) => {
    try {
      const eventName = (req.query.event || '').toLowerCase();
      if (!eventName) return res.status(400).json({ error: 'event query param required (partial match)' });
      const pxEvents = await px.fetchSportEvents();
      const match = pxEvents.find(e => (e.name || '').toLowerCase().includes(eventName));
      if (!match) return res.json({ error: 'no event found', searched: pxEvents.length });
      const markets = await px.fetchMarkets(match.event_id);
      const summary = (markets || []).map(m => ({
        id: m.id, type: m.type, name: m.name,
        selectionCount: (m.selections || []).reduce((n, sg) => n + (sg || []).length, 0)
          + (m.market_lines || []).reduce((n, ml) => n + (ml.selections || []).reduce((mm, sg) => mm + (sg || []).length, 0), 0),
        sampleSelection: (() => {
          if (m.selections) for (const sg of m.selections) for (const s of sg || []) if (s.line_id) return { name: s.name, line_id: s.line_id, line: s.line };
          if (m.market_lines) for (const ml of m.market_lines) for (const sg of ml.selections || []) for (const s of sg || []) if (s.line_id) return { name: s.name, line_id: s.line_id, line: ml.line ?? s.line };
          return null;
        })(),
      }));
      res.json({ ok: true, event: { id: match.event_id, name: match.name }, marketCount: markets.length, markets: summary });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.get('/debug-tennis-match', async (req, res) => {
    try {
      const pxEvents = await px.fetchSportEvents();
      // Dump all distinct sport_name values PX returned so we can see
      // if tennis is actually empty vs tagged under a different name.
      const sportNameCounts = {};
      for (const e of pxEvents) {
        const n = e.sport_name || '(null)';
        sportNameCounts[n] = (sportNameCounts[n] || 0) + 1;
      }
      const tennisEvents = pxEvents.filter(e => /tennis/i.test(e.sport_name || ''));
      const idx = lineManager.__debugGetLineIndex ? lineManager.__debugGetLineIndex() : {};
      const registeredByEvent = {};
      for (const v of Object.values(idx)) {
        if (v.sport === 'tennis' && v.pxEventId) {
          registeredByEvent[String(v.pxEventId)] = (registeredByEvent[String(v.pxEventId)] || 0) + 1;
        }
      }
      const oddsCached = oddsFeed.getAllCachedEvents().filter(e => e.sport === 'tennis');
      const summary = tennisEvents.map(e => ({
        pxEventId: e.event_id,
        pxEventName: e.name,
        tournament: e.tournament_name || e.tournament?.name,
        scheduled: e.scheduled,
        competitors: (e.competitors || []).map(c => c.name),
        registeredLines: registeredByEvent[String(e.event_id)] || 0,
      }));
      res.json({
        ok: true,
        pxTotalEvents: pxEvents.length,
        pxSportNameCounts: sportNameCounts,
        pxTennisEventCount: tennisEvents.length,
        registeredEvents: Object.keys(registeredByEvent).length,
        oddsCachedCount: oddsCached.length,
        oddsCachedSample: oddsCached.slice(0, 30).map(e => ({
          home: e.homeTeam, away: e.awayTeam, commenceTime: e.commenceTime,
        })),
        pxEvents: summary,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // TEMPORARY: probe DK's MMA/UFC page to find the right URL + XHR
  // subcategory for fight-winner moneylines. Loads the league page in
  // headless Chromium and reports all nash-API XHRs + category tabs.
  // Remove after dk-scraper MMA support ships.
  app.get('/debug-dk-mma-probe', async (req, res) => {
    const path = req.query.path || '/leagues/mma/ufc';
    let puppeteer;
    try { puppeteer = require('puppeteer'); } catch { return res.status(500).json({ error: 'puppeteer not installed' }); }
    const browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    const hits = [];
    let tabs = [];
    let seoEvents = [];
    try {
      const page = await browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
      page.on('response', async (resp) => {
        const url = resp.url();
        if (!url.includes('sportsbook-nash.draftkings.com')) return;
        const subMatch = url.match(/subCategoryId%20eq%20%27(\d+)%27/);
        const entry = { url, subcategoryId: subMatch ? subMatch[1] : null };
        try {
          const ct = resp.headers()['content-type'] || '';
          if (ct.includes('json')) {
            const data = await resp.json();
            entry.eventCount = (data.events || []).length;
            entry.selectionCount = (data.selections || []).length;
            entry.marketNamesSample = [...new Set((data.markets || []).map(m => m.name || m.marketType?.name).filter(Boolean))].slice(0, 8);
            entry.firstEvent = data.events?.[0]?.name || null;
          }
        } catch {}
        hits.push(entry);
      });
      await page.goto('https://sportsbook.draftkings.com' + path, { waitUntil: 'domcontentloaded', timeout: 45000 });
      await new Promise(r => setTimeout(r, 10000));
      tabs = await page.evaluate(() => {
        const out = [];
        for (const a of document.querySelectorAll('a')) {
          const h = a.getAttribute('href') || '';
          const x = (a.textContent || '').trim();
          if (/category|leagues\//.test(h) && x) out.push({ href: h, txt: x });
        }
        return out.slice(0, 40);
      });
      // Also collect visible event names on the page
      seoEvents = await page.evaluate(() => {
        const out = [];
        for (const a of document.querySelectorAll('a[href*="/event/"]')) {
          const h = a.getAttribute('href') || '';
          const x = (a.textContent || '').trim();
          if (x) out.push({ href: h, txt: x });
        }
        return out.slice(0, 20);
      });
    } finally {
      await browser.close();
    }
    res.json({ ok: true, probedPath: path, hits, tabs, seoEventsSample: seoEvents });
  });

  // TEMPORARY: diagnose why most MMA fights aren't being registered.
  // Pulls PX's full MMA event list, cross-checks against the line index
  // and odds-feed events, and reports which ones matched vs didn't.
  app.get('/debug-mma-match', async (req, res) => {
    try {
      const pxEvents = await px.fetchSportEvents();
      const mmaEvents = pxEvents.filter(e => (e.sport_name || '').toLowerCase().includes('mma'));
      const idx = lineManager.__debugGetLineIndex ? lineManager.__debugGetLineIndex() : {};
      const registeredEventIds = new Set();
      for (const v of Object.values(idx)) {
        if (v.sport === 'mma_mixed_martial_arts' && v.pxEventId) registeredEventIds.add(String(v.pxEventId));
      }
      const oddsCached = oddsFeed.getAllCachedEvents().filter(e => e.sport === 'mma_mixed_martial_arts');
      const oddsNames = oddsCached.map(e => `${e.homeTeam} vs ${e.awayTeam}`);

      const summary = mmaEvents.map(e => ({
        pxEventId: e.event_id,
        pxEventName: e.name,
        sport_name: e.sport_name,
        scheduled: e.scheduled,
        competitors: (e.competitors || []).map(c => c.name),
        registered: registeredEventIds.has(String(e.event_id)),
      }));

      res.json({
        ok: true,
        pxMmaEventCount: mmaEvents.length,
        registeredCount: summary.filter(s => s.registered).length,
        oddsCachedCount: oddsCached.length,
        oddsCachedSample: oddsNames.slice(0, 30),
        pxEvents: summary,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack?.split('\n').slice(0, 5) });
    }
  });

  // Inspect the actual shape of SharpAPI's 1st_inning_moneyline market.
  // Confirmed it has data; this tells us whether it's standard home/away
  // moneyline or a YRFI/NRFI-style yes/no binary prop. Shows distinct
  // selection + selection_type values across 50 sampled rows, plus the
  // book coverage.
  app.get('/debug-sharp-1st-inning-detail', async (req, res) => {
    const fetch = require('node-fetch');
    const key = process.env.SHARP_ODDS_API_KEY || process.env.ODDS_API_KEY;
    const baseUrl = 'https://api.sharpapi.io/api/v1';
    if (!key) return res.status(500).json({ ok: false, error: 'no SHARP_ODDS_API_KEY' });
    const url = `${baseUrl}/odds?league=mlb&market=1st_inning_moneyline&live=false&limit=50`;
    try {
      const resp = await fetch(url, { headers: { 'X-API-Key': key } });
      const body = await resp.json();
      const rows = body.data || [];
      // Tally distinct selections + selection_types + books, and
      // pair up per-event to show what both-sides of a market look like.
      const selections = new Set();
      const selectionTypes = new Set();
      const books = new Set();
      const byEvent = {};
      for (const r of rows) {
        selections.add(r.selection);
        selectionTypes.add(r.selection_type);
        books.add(r.sportsbook);
        const k = r.event_id;
        if (!byEvent[k]) byEvent[k] = { teams: `${r.away_team} @ ${r.home_team}`, entries: [] };
        byEvent[k].entries.push({
          book: r.sportsbook,
          selection: r.selection,
          selection_type: r.selection_type,
          odds_american: r.odds_american,
          line: r.line,
        });
      }
      // First event with both sides, for human inspection
      const sampleEvent = Object.values(byEvent).find(ev => ev.entries.length >= 2) || Object.values(byEvent)[0];
      res.json({
        ok: true,
        rowCount: rows.length,
        distinctSelections: [...selections],
        distinctSelectionTypes: [...selectionTypes],
        books: [...books],
        eventCount: Object.keys(byEvent).length,
        sampleEvent,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Probe SharpAPI with candidate 1st-inning market-type names.
  // YRFI/NRFI (Yes/No Run First Inning) is a popular bet but The
  // Odds API doesn't support any 1st-inning markets. SharpAPI may
  // expose it via a different naming convention — test which (if
  // any) return data. Also tries common variants for 1st inning
  // totals, moneyline, and team totals.
  app.get('/debug-sharp-1st-inning', async (req, res) => {
    const fetch = require('node-fetch');
    const key = process.env.SHARP_ODDS_API_KEY || process.env.ODDS_API_KEY;
    const baseUrl = 'https://api.sharpapi.io/api/v1';
    if (!key) return res.status(500).json({ ok: false, error: 'no SHARP_ODDS_API_KEY' });
    const candidates = [
      // Full naming per SharpAPI F5 convention
      '1st_inning_moneyline', '1st_inning_total', '1st_inning_run_line',
      'first_inning_moneyline', 'first_inning_total', 'first_inning_run_line',
      'moneyline_1st_inning', 'total_1st_inning',
      'moneyline_first_inning', 'total_first_inning',
      // YRFI / NRFI binary prop
      'yrfi', 'nrfi', 'yrfi_nrfi',
      'yes_run_first_inning', 'no_run_first_inning',
      'first_inning_run', 'run_first_inning',
      'run_in_1st_inning', 'run_in_first_inning',
      '1st_inning_run_scored', 'first_inning_run_scored',
      // Team-specific 1st inning
      '1st_inning_team_total', 'first_inning_team_total',
    ];
    const results = [];
    for (const mt of candidates) {
      const url = `${baseUrl}/odds?league=mlb&market=${mt}&live=false&limit=5`;
      try {
        const resp = await fetch(url, { headers: { 'X-API-Key': key } });
        const text = await resp.text();
        let rowCount = 0;
        let sample = null;
        try {
          const body = JSON.parse(text);
          rowCount = (body.data || []).length;
          sample = body.data && body.data[0] ? body.data[0] : null;
        } catch (_) {}
        results.push({
          market: mt,
          status: resp.status,
          rows: rowCount,
          sampleKeys: sample ? Object.keys(sample) : null,
          bodySnippet: rowCount === 0 ? text.slice(0, 200) : null,
        });
      } catch (err) {
        results.push({ market: mt, error: err.message });
      }
      await new Promise(r => setTimeout(r, 200));
    }
    // Compact summary: which variants actually returned rows
    const hits = results.filter(r => r.rows > 0);
    res.json({ ok: true, baseUrl, hitCount: hits.length, hits, allResults: results });
  });

  // TEMPORARY: probe SharpAPI with candidate F5 market-type names to see
  // which (if any) return data. Answers whether we can drop the Odds-API
  // F5 supplement in favor of SharpAPI as primary. Remove after confirming.
  app.get('/debug-sharp-f5', async (req, res) => {
    const fetch = require('node-fetch');
    const key = process.env.SHARP_ODDS_API_KEY || process.env.ODDS_API_KEY;
    const base = process.env.PX_BASE_URL ? undefined : 'https://api.sharpapi.io/api/v1';
    const baseUrl = 'https://api.sharpapi.io/api/v1';
    if (!key) return res.status(500).json({ ok: false, error: 'no SHARP_ODDS_API_KEY' });
    const candidates = [
      'first_5_innings_moneyline',
      'first_5_innings_total',
      'first_5_innings_run_line',
      'first_5_innings_total_runs',
      'first_five_innings_moneyline',
      'first_five_innings_total',
      'first_five_innings_run_line',
      '1st_5_innings_moneyline',
      '1st_5_innings_total',
      '1st_5_innings_run_line',
      'f5_moneyline',
      'f5_total',
      'f5_run_line',
      'moneyline_f5',
      'total_f5',
      'run_line_f5',
      'moneyline_first_5_innings',
      'total_first_5_innings',
    ];
    const results = [];
    for (const mt of candidates) {
      const url = `${baseUrl}/odds?league=mlb&market=${mt}&live=false&limit=5`;
      try {
        const resp = await fetch(url, { headers: { 'X-API-Key': key } });
        const text = await resp.text();
        let rowCount = 0;
        let sample = null;
        try {
          const body = JSON.parse(text);
          rowCount = (body.data || []).length;
          sample = body.data && body.data[0] ? body.data[0] : null;
        } catch (_) {}
        results.push({
          market: mt,
          status: resp.status,
          rows: rowCount,
          sampleKeys: sample ? Object.keys(sample) : null,
          bodySnippet: rowCount === 0 ? text.slice(0, 200) : null,
        });
      } catch (err) {
        results.push({ market: mt, error: err.message });
      }
      await new Promise(r => setTimeout(r, 200));
    }
    res.json({ ok: true, baseUrl, results });
  });

  // TEMPORARY: probe The Odds API's raw MLB response to see which games it
  // currently has available. Lets us diagnose why the Odds-API supplement
  // isn't filling SharpAPI's gaps (esp. for games >24h out). Remove after
  // MLB coverage fix is shipped.
  app.get('/debug-odds-api-mlb', async (req, res) => {
    const key = process.env.THE_ODDS_API_KEY;
    if (!key) return res.status(500).json({ ok: false, error: 'THE_ODDS_API_KEY not set' });
    try {
      const url = `https://api.the-odds-api.com/v4/sports/baseball_mlb/odds`
        + `?apiKey=${key}`
        + `&regions=us,eu`
        + `&markets=h2h,spreads,totals`
        + `&bookmakers=pinnacle,draftkings,fanduel`
        + `&oddsFormat=american`;
      const resp = await fetch(url);
      const status = resp.status;
      if (!resp.ok) {
        const text = await resp.text();
        return res.json({ ok: false, status, body: text.slice(0, 500) });
      }
      const events = await resp.json();
      // Trim to the info we actually need: matchup + commence time + which books have h2h
      const summary = events.map(e => ({
        home: e.home_team,
        away: e.away_team,
        commenceTime: e.commence_time,
        books: (e.bookmakers || []).map(b => b.key),
        hasH2h: (e.bookmakers || []).some(b => (b.markets || []).some(m => m.key === 'h2h')),
        hasSpreads: (e.bookmakers || []).some(b => (b.markets || []).some(m => m.key === 'spreads')),
        hasTotals: (e.bookmakers || []).some(b => (b.markets || []).some(m => m.key === 'totals')),
      }));
      res.json({
        ok: true,
        requestsRemaining: resp.headers.get('x-requests-remaining'),
        totalEvents: events.length,
        events: summary,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // TEMPORARY: probe SharpAPI reference endpoints to discover which sports
  // and leagues are supported on our current tier. Used to determine if we
  // can route MMA through SharpAPI (currently goes to The Odds API fallback).
  // Remove after MMA routing decision is made.
  app.get('/debug-sharp-leagues', async (req, res) => {
    const apiKey = process.env.SHARP_ODDS_API_KEY || process.env.ODDS_API_KEY;
    if (!apiKey) return res.status(500).json({ ok: false, error: 'no api key' });
    const base = 'https://api.sharpapi.io/api/v1';
    const out = {};
    for (const path of ['/sports', '/leagues']) {
      try {
        const r = await fetch(base + path, { headers: { 'X-API-Key': apiKey } });
        const status = r.status;
        const body = status === 200 ? await r.json() : await r.text();
        out[path] = { status, body };
      } catch (err) {
        out[path] = { error: err.message };
      }
    }
    // Also try a direct MMA/UFC probe — guess common league keys
    const guesses = ['ufc', 'mma', 'mma_ufc', 'ufc_mma'];
    out.leagueGuesses = {};
    for (const g of guesses) {
      try {
        const url = `${base}/odds?league=${g}&market=moneyline&limit=5`;
        const r = await fetch(url, { headers: { 'X-API-Key': apiKey } });
        const status = r.status;
        const body = status === 200 ? await r.json() : await r.text();
        const rows = (body && body.data) ? body.data.length : null;
        out.leagueGuesses[g] = { status, rows, sampleText: typeof body === 'string' ? body.slice(0, 200) : null };
      } catch (err) {
        out.leagueGuesses[g] = { error: err.message };
      }
    }
    res.json({ ok: true, out });
  });

  // Full event detail — returns cached markets (all books) for a specific
  // event by team-name substring match. Used to audit stale/wrong Pinnacle
  // values in quoted parlays.
  app.get('/odds-event-detail', (req, res) => {
    const q = (req.query.q || '').toLowerCase();
    const sportFilter = req.query.sport || null;
    if (!q) return res.status(400).json({ ok: false, error: 'missing q param' });
    const sports = sportFilter ? [sportFilter] : [
      'basketball_nba', 'baseball_mlb', 'icehockey_nhl', 'tennis',
      'soccer', 'soccer_epl', 'soccer_usa_mls', 'soccer_uefa_champs_league',
      'soccer_uefa_europa_league', 'soccer_spain_la_liga',
      'soccer_italy_serie_a', 'soccer_germany_bundesliga',
      'soccer_france_ligue_one', 'soccer_usa_nwsl', 'basketball_wnba',
    ];
    const hits = [];
    for (const sport of sports) {
      const cache = oddsFeed.__debugGetCache(sport);
      if (!cache || !cache.events) continue;
      for (const entry of Object.values(cache.events)) {
        const evList = Array.isArray(entry) ? entry : [entry];
        for (const ev of evList) {
          if (!ev) continue;
          const hay = ((ev.homeTeam || '') + ' ' + (ev.awayTeam || '')).toLowerCase();
          if (!hay.includes(q)) continue;
          // Return full market structure + first few raw rows per book
          const rawBySb = {};
          for (const r of (ev._rawOdds || [])) {
            if (!rawBySb[r.sportsbook]) rawBySb[r.sportsbook] = [];
            rawBySb[r.sportsbook].push({
              mt: r.market_type,
              st: r.selection_type,
              line: r.line,
              american: r.odds_american,
            });
          }
          hits.push({
            sport,
            homeTeam: ev.homeTeam,
            awayTeam: ev.awayTeam,
            commenceTime: ev.commenceTime,
            fetchedAt: new Date(cache.fetchedAt).toISOString(),
            cacheAgeMin: Math.round((Date.now() - cache.fetchedAt) / 60000),
            markets: ev.markets,
            rawBySb,
          });
        }
      }
    }
    res.json({ hits });
  });

  // Book coverage diagnostic — counts how many events per sport have each
  // sportsbook present (pinnacle, fanduel, draftkings, kalshi). Answers
  // "why don't my quotes show Kalshi odds?" — it's almost always because
  // SharpAPI returned zero Kalshi rows for that sport/event.
  // Dump altLinesCache entries for an event (by normalized team key).
  // Used to audit per-book alt line odds vs primary line values.
  app.get('/debug-alt-lines', (req, res) => {
    const home = req.query.home || '';
    const away = req.query.away || '';
    if (!home || !away) return res.status(400).json({ ok: false, error: 'need home + away' });
    // Access private altLinesCache via eval against the odds-feed module.
    // We don't expose it through __debugGetCache, so read via fs hack.
    try {
      const of = require('./services/odds-feed');
      // Try to reach the normalizer + cache
      const key = of.normalizeTeamName(home) + '|' + of.normalizeTeamName(away);
      // altLinesCache is module-scoped; we need to add an accessor.
      // Fall back: require module and look at its exports for a debug hook.
      const dump = (of.__debugGetAltLinesCache && of.__debugGetAltLinesCache()) || null;
      if (!dump) return res.json({ ok: false, error: '__debugGetAltLinesCache not exported' });
      res.json({ key, entry: dump[key] || null, availableKeys: Object.keys(dump).slice(0, 20) });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Inspect line index entries matching a query. Shows exactly what the
  // line index has registered for an event + market type.
  // For every registered line (filtered by sport/market), resolve the
  // fair probability the pricer would actually use and classify as
  // quotable vs not-quotable. Primary use: after /debug-line-index
  // shows which lines we'll RECOGNIZE on an RFQ, this shows which of
  // those will ACTUALLY PRICE vs. decline for "no fair value".
  //
  // Series markets route to the DK scraper cache (same as pricer's
  // getSeriesFairProb). Non-series markets use getFairProbAsync so
  // alt-line lookups resolve accurately — may trigger Odds API
  // fetches for uncached alt lines, so keep `limit` modest.
  app.get('/debug-quotable', async (req, res) => {
    try {
      const sport = req.query.sport || null;
      const market = req.query.market || null;
      const limit = Math.min(parseInt(req.query.limit) || 200, 500);
      const idx = lineManager.__debugGetLineIndex ? lineManager.__debugGetLineIndex() : {};
      const entries = Object.entries(idx).filter(([, info]) => {
        if (sport && info.sport !== sport) return false;
        if (market && info.marketType !== market) return false;
        return true;
      }).slice(0, limit);

      function americanFromProb(p) {
        if (p == null || p <= 0 || p >= 1) return null;
        return p >= 0.5
          ? -Math.round((p / (1 - p)) * 100)
          : Math.round(100 / p - 100);
      }

      function sportKeyFor(info) {
        const s = (info.oddsApiSport || info.sport || '').toLowerCase();
        return s.includes('nba') || s.includes('basketball') ? 'nba'
             : s.includes('nhl') || s.includes('icehockey') || s.includes('hockey') ? 'nhl'
             : null;
      }

      const quotable = [];
      const notQuotable = [];
      for (const [lineId, info] of entries) {
        const mt = info.marketType;
        let fairProb = null;
        let reason = null;

        try {
          if (mt === 'series_winner') {
            const sk = sportKeyFor(info);
            const bare = (info.teamName || '').replace(/\s*\(series\)\s*/ig, '').trim();
            const hit = sk ? dkScraper.lookupSeriesFairProb(sk, bare || info.teamName) : null;
            fairProb = hit?.fairProb ?? null;
          } else if (mt === 'series_spread') {
            const sk = sportKeyFor(info);
            const n = Number(info.line);
            if (sk && Number.isFinite(n)) {
              const bare = (info.teamName || '').replace(/\s*\(series\)\s*/ig, '').trim();
              const hit = dkScraper.lookupSeriesSpreadFairProb(sk, bare || info.teamName, Math.abs(n), n < 0 ? '-' : '+');
              fairProb = hit?.fairProb ?? null;
            }
          } else if (mt === 'series_total') {
            const sk = sportKeyFor(info);
            const n = Number(info.line);
            if (sk && Number.isFinite(n)) {
              const side = info.oddsApiSelection || info.selection;
              const hit = dkScraper.lookupSeriesTotalFairProb(sk, info.homeTeam, info.awayTeam, n, side);
              fairProb = hit?.fairProb ?? null;
            }
          } else {
            // Normal markets — use async path so alt lines resolve.
            fairProb = await oddsFeed.getFairProbAsync(
              info.oddsApiSport, info.homeTeam, info.awayTeam,
              info.oddsApiMarket, info.oddsApiSelection, info.line, info.startTime
            );
          }
        } catch (e) {
          reason = 'lookup_error: ' + e.message;
        }

        const row = {
          lineId,
          sport: info.sport,
          marketType: mt,
          marketName: info.marketName,
          team: info.teamName,
          selection: info.oddsApiSelection,
          line: info.line,
          event: info.pxEventName,
          home: info.homeTeam,
          away: info.awayTeam,
        };
        if (fairProb != null && fairProb > 0 && fairProb < 1) {
          quotable.push({ ...row, fairProb: +fairProb.toFixed(4), fairAmerican: americanFromProb(fairProb) });
        } else {
          notQuotable.push({ ...row, reason: reason || 'no_fair_prob' });
        }
      }

      // Group notQuotable by marketType so gaps are easy to spot at a glance.
      const notQuotableByMarket = {};
      for (const r of notQuotable) {
        if (!notQuotableByMarket[r.marketType]) notQuotableByMarket[r.marketType] = 0;
        notQuotableByMarket[r.marketType]++;
      }
      res.json({
        summary: {
          total: entries.length,
          quotable: quotable.length,
          notQuotable: notQuotable.length,
          notQuotableByMarket,
        },
        quotable,
        notQuotable,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  app.get('/debug-line-index', (req, res) => {
    const q = (req.query.q || '').toLowerCase();
    const market = req.query.market || null;
    const sport = req.query.sport || null;
    const idx = lineManager.__debugGetLineIndex ? lineManager.__debugGetLineIndex() : {};
    const hits = [];
    for (const [lineId, info] of Object.entries(idx)) {
      if (sport && info.sport !== sport) continue;
      if (market && info.marketType !== market) continue;
      const haystack = ((info.pxEventName || '') + ' ' + (info.homeTeam || '') + ' ' + (info.awayTeam || '') + ' ' + (info.teamName || '')).toLowerCase();
      if (q && !haystack.includes(q)) continue;
      hits.push({
        lineId,
        sport: info.sport,
        marketType: info.marketType,
        marketName: info.marketName,
        team: info.teamName,
        selection: info.oddsApiSelection,
        line: info.line,
        event: info.pxEventName,
        home: info.homeTeam,
        away: info.awayTeam,
      });
    }
    res.json({ count: hits.length, hits: hits.slice(0, 50) });
  });

  // Query PX API directly for ALL orders and return settled ones with full data.
  // Used to reconcile our in-memory P&L counter against ground truth from PX REST.
  // Pure read: no DB writes, no in-memory mutations, no pause/unpause.
  app.get('/debug-px-settled-full', async (req, res) => {
    try {
      const pxSvc = require('./services/prophetx');
      const startMs = Date.now();
      // PX paginates via cursor tokens; 10000 limit ensures we traverse the
      // full history (fetchOrders stops early when PX returns no next token).
      const all = await pxSvc.fetchOrders(10000);
      const fetchedMs = Date.now() - startMs;

      const SETTLED_STATUSES = new Set(['won', 'lost', 'push', 'void']);
      const settled = all.filter(o => o.settlement_status && SETTLED_STATUSES.has(o.settlement_status));

      // Aggregate stats directly from PX's `profit` field (authoritative)
      let sumProfit = 0;
      let wins = 0, losses = 0, pushes = 0, voids = 0;
      let missingProfit = 0;
      const byStatus = {};
      const byMonth = {};
      // Also group by inferred sport from first leg's event metadata if available
      const bySportGuess = {};

      for (const o of settled) {
        const status = o.settlement_status;
        byStatus[status] = (byStatus[status] || 0) + 1;

        const profit = o.profit != null ? Number(o.profit) : null;
        if (profit == null) missingProfit++;
        else sumProfit += profit;

        if (status === 'won') wins++;
        else if (status === 'lost') losses++;
        else if (status === 'push') pushes++;
        else if (status === 'void') voids++;

        const updatedAt = o.updated_at || o.settled_at || 0;
        const date = updatedAt ? new Date(Number(updatedAt) * 1000).toISOString().substring(0, 7) : 'unknown';
        byMonth[date] = (byMonth[date] || 0) + (profit || 0);
      }

      // Also compute from stake/odds where profit is missing, as a sanity check
      function americanToProfit(odds, stake) {
        const o = Number(odds);
        if (!o || !stake) return 0;
        if (o >= 100) return stake * o / 100;
        return stake * 100 / Math.abs(o);
      }
      let computedProfit = 0;
      for (const o of settled) {
        const stake = Number(o.confirmed_stake || o.stake || 0);
        const odds = Number(o.confirmed_odds || o.odds || 0);
        const status = o.settlement_status;
        if (status === 'won') {
          // SP won — bettor's parlay missed. SP keeps bettor's wager (= their stake × 100/|odds|).
          computedProfit += americanToProfit(odds, stake);
        } else if (status === 'lost') {
          // SP lost — pays out confirmedStake.
          computedProfit -= stake;
        }
        // push/void: 0
      }

      res.json({
        ok: true,
        fetchedMs,
        totalOrdersFromPx: all.length,
        settledCount: settled.length,
        byStatus,
        wins, losses, pushes, voids,
        missingProfit,
        sumProfitFromPxField: Math.round(sumProfit * 100) / 100,
        computedProfitFromStakeOdds: Math.round(computedProfit * 100) / 100,
        pnlByMonth: Object.fromEntries(
          Object.entries(byMonth).sort().map(([k, v]) => [k, Math.round(v * 100) / 100])
        ),
        // Include first 5 settled as samples for verification
        samples: settled.slice(0, 5).map(o => ({
          uuid: o.order_uuid,
          parlayId: o.p_id || o.parlay_id,
          status: o.settlement_status,
          stake: o.confirmed_stake || o.stake,
          odds: o.confirmed_odds || o.odds,
          profit: o.profit,
          updatedAt: o.updated_at,
        })),
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  // Dump raw PX markets for a specific event by team substring match.
  // Used to see whether PX is even returning alt puck lines / alt spread
  // markets for NHL / MLB so we can tell coverage from parsing bugs.
  // Probe raw PX reference endpoints to find ones that return team/market
  // names for events we can't currently resolve. Pass ?tournament_id=234 to
  // try the tournament-scoped sport_events call, or ?event_ids=1,2,3 to
  // try get_multiple_markets.
  // Probe PX /partner/affiliate/* directly and return the raw response shape
  // so we can see what parseable fields the endpoints actually expose.
  app.get('/debug-affiliate-probe', async (req, res) => {
    try {
      const pxSvc = require('./services/prophetx');
      const out = {};
      const paths = [
        '/partner/affiliate/get_tournaments',
        '/partner/v2/affiliate/get_tournaments',
        '/partner/affiliate/tournaments',
        '/partner/mm/get_tournaments',
      ];
      out.tournamentProbes = [];
      for (const p of paths) {
        try {
          const raw = await pxSvc.pxFetch(p);
          out.tournamentProbes.push({ path: p, ok: true, topKeys: Object.keys(raw || {}), sample: JSON.stringify(raw).slice(0, 600) });
        } catch (err) {
          out.tournamentProbes.push({ path: p, ok: false, error: err.message });
        }
      }
      if (req.query.event_ids) {
        const ids = req.query.event_ids;
        const sePaths = [
          `/partner/affiliate/get_sport_events?event_ids=${ids}`,
          `/partner/v2/affiliate/get_sport_events?event_ids=${ids}`,
          `/partner/mm/get_sport_events?event_ids=${ids}`,
        ];
        out.sportEventProbes = [];
        for (const p of sePaths) {
          try {
            const raw = await pxSvc.pxFetch(p);
            out.sportEventProbes.push({ path: p, ok: true, topKeys: Object.keys(raw || {}), sample: JSON.stringify(raw).slice(0, 800) });
          } catch (err) {
            out.sportEventProbes.push({ path: p, ok: false, error: err.message });
          }
        }
        const mmPaths = [
          `/partner/affiliate/get_multiple_markets?event_ids=${ids}`,
          `/partner/v2/affiliate/get_multiple_markets?event_ids=${ids}`,
          `/partner/mm/get_multiple_markets?event_ids=${ids}`,
        ];
        out.multipleMarketsProbes = [];
        for (const p of mmPaths) {
          try {
            const raw = await pxSvc.pxFetch(p);
            // Extract the market list for the first event and print each
            // market's outer keys so we can see if 'type' is actually set.
            const inner = raw?.data || {};
            const firstKey = Object.keys(inner)[0];
            const list = firstKey ? inner[firstKey] : [];
            const marketSummary = Array.isArray(list) ? list.map(m => ({
              outerKeys: Object.keys(m || {}),
              type: m?.type,
              name: m?.name,
              hasSelections: Array.isArray(m?.selections),
              selectionsCount: Array.isArray(m?.selections) ? m.selections.length : 0,
              hasMarketLines: Array.isArray(m?.market_lines),
              marketLinesCount: Array.isArray(m?.market_lines) ? m.market_lines.length : 0,
            })) : [];
            out.multipleMarketsProbes.push({ path: p, ok: true, topKeys: Object.keys(raw || {}), firstEventId: firstKey, marketSummary });
          } catch (err) {
            out.multipleMarketsProbes.push({ path: p, ok: false, error: err.message });
          }
        }
      }
      res.json({ ok: true, ...out });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Run the same fetch path enrichOpenPositionsFromAffiliate uses, but on
  // an arbitrary set of event_ids. Returns eventInfo + lineIdInfo so we can
  // verify the bulk markets parser works.
  app.get('/debug-affiliate-enrich-dry', async (req, res) => {
    try {
      const pxSvc = require('./services/prophetx');
      const ids = (req.query.event_ids || '').split(',').map(s => s.trim()).filter(Boolean);
      if (ids.length === 0) return res.status(400).json({ ok: false, error: 'missing event_ids' });
      const seList = await pxSvc.fetchAffiliateSportEvents({ eventIds: ids });
      const bulkMarkets = await pxSvc.fetchAffiliateMultipleMarkets(ids);
      const lineIdInfo = {};
      let marketsSeen = 0, marketsParsed = 0;
      for (const [eid, marketList] of Object.entries(bulkMarkets || {})) {
        if (!Array.isArray(marketList)) continue;
        for (const market of marketList) {
          marketsSeen++;
          if (!['moneyline', 'spread', 'total', 'team_total'].includes(market.type)) continue;
          let parsed;
          try { parsed = pxSvc.parseMarketSelections(market); }
          catch (e) { continue; }
          marketsParsed++;
          for (const sel of parsed || []) {
            if (!sel.lineId) continue;
            lineIdInfo[sel.lineId] = { teamName: sel.teamName, marketType: sel.marketType, selection: sel.selection, line: sel.line };
          }
        }
      }
      res.json({
        ok: true,
        requested: ids.length,
        sportEventsResolved: (seList || []).length,
        sportEventsSample: (seList || []).slice(0, 3).map(se => ({ event_id: se.event_id, name: se.name, competitors: (se.competitors || []).map(c => ({ side: c.side, display_name: c.display_name })), scheduled: se.scheduled, sport_name: se.sport_name })),
        bulkMarketsTopKeys: Object.keys(bulkMarkets || {}),
        marketsSeen, marketsParsed,
        lineIdCount: Object.keys(lineIdInfo).length,
        lineIdSample: Object.entries(lineIdInfo).slice(0, 5),
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  app.get('/debug-px-probe', async (req, res) => {
    try {
      const pxSvc = require('./services/prophetx');
      const out = {};
      if (req.query.tournament_id) {
        try {
          const raw = await pxSvc.pxFetch(`/partner/mm/get_sport_events?tournament_id=${req.query.tournament_id}`);
          out.tournamentEvents = {
            ok: true,
            topKeys: raw && raw.data ? Object.keys(raw.data) : Object.keys(raw || {}),
            count: (raw?.data?.sport_events || raw?.sport_events || []).length,
            sample: (raw?.data?.sport_events || raw?.sport_events || []).slice(0, 3),
          };
        } catch (err) { out.tournamentEvents = { ok: false, error: err.message }; }
      }
      if (req.query.event_ids) {
        const ids = req.query.event_ids.split(',').map(s => s.trim()).filter(Boolean);
        // Try several URL shapes for "multiple markets"
        const shapes = [
          `/partner/mm/get_multiple_markets?event_ids=${ids.join(',')}`,
          `/partner/mm/get_multiple_markets?event_id=${ids.join('&event_id=')}`,
          `/partner/mm/get_markets?event_ids=${ids.join(',')}`,
          `/partner/mm/get_markets?event_id=${ids.join('&event_id=')}`,
        ];
        out.multipleMarkets = [];
        for (const shape of shapes) {
          try {
            const raw = await pxSvc.pxFetch(shape);
            out.multipleMarkets.push({
              url: shape,
              ok: true,
              topKeys: raw && raw.data ? Object.keys(raw.data) : Object.keys(raw || {}),
              sample: JSON.stringify(raw).slice(0, 800),
            });
          } catch (err) {
            out.multipleMarkets.push({ url: shape, ok: false, error: err.message });
          }
        }
      }
      if (req.query.event_id) {
        // Single event resolution for comparison
        try {
          const markets = await pxSvc.fetchMarkets(req.query.event_id);
          out.singleEvent = { ok: true, count: markets.length, marketTypes: [...new Set(markets.map(m => m.type))], sample: markets[0] };
        } catch (err) { out.singleEvent = { ok: false, error: err.message }; }
      }
      res.json({ ok: true, ...out });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Return all PX sport events grouped by sport_name so we can see exactly
  // which sport labels PX uses (e.g. "Mixed Martial Arts" vs "MMA") and what
  // the competitor shape looks like for non-team sports.
  app.get('/debug-px-events-by-sport', async (req, res) => {
    try {
      const pxSvc = require('./services/prophetx');
      const all = await pxSvc.fetchSportEvents();
      const groups = {};
      for (const e of all) {
        const key = e.sport_name || 'UNKNOWN';
        if (!groups[key]) groups[key] = { count: 0, sample: [] };
        groups[key].count++;
        if (groups[key].sample.length < 3) {
          groups[key].sample.push({
            event_id: e.event_id,
            name: e.name,
            status: e.status,
            scheduled: e.scheduled,
            competitors: (e.competitors || []).map(c => ({
              side: c.side, name: c.name, display_name: c.display_name,
            })),
          });
        }
      }
      res.json({ ok: true, total: all.length, sports: groups });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Dump PX markets for one MMA event so we can see what market.type and
  // market.name strings it actually uses.
  app.get('/debug-px-mma-markets', async (req, res) => {
    try {
      const pxSvc = require('./services/prophetx');
      const all = await pxSvc.fetchSportEvents();
      const mmaEvents = all.filter(e => /martial|mma/i.test(e.sport_name || ''));
      if (mmaEvents.length === 0) return res.json({ ok: false, error: 'no MMA events in PX feed', total: all.length });
      const pick = mmaEvents[0];
      const markets = await pxSvc.fetchMarkets(pick.event_id);
      const parsed = markets.map(m => {
        let p;
        try { p = pxSvc.parseMarketSelections(m); } catch (e) { p = [{ err: e.message }]; }
        return { type: m.type, name: m.name, parsedCount: p.length, parsedSample: p.slice(0, 4) };
      });
      res.json({
        ok: true,
        mmaEventCount: mmaEvents.length,
        picked: { event_id: pick.event_id, name: pick.name, scheduled: pick.scheduled, sport_name: pick.sport_name, competitors: pick.competitors },
        markets: parsed,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message, stack: err.stack });
    }
  });

  app.get('/debug-px-markets', async (req, res) => {
    const q = (req.query.q || '').toLowerCase();
    const sportFilter = req.query.sport || 'Hockey';
    if (!q) return res.status(400).json({ ok: false, error: 'missing q param' });
    try {
      const pxSvc = require('./services/prophetx');
      const allEvents = await pxSvc.fetchSportEvents();
      const hit = allEvents.find(e => (e.name || '').toLowerCase().includes(q) && (e.sport_name || '') === sportFilter);
      if (!hit) return res.json({ ok: false, error: 'no event matched', searched: allEvents.length });
      const markets = await pxSvc.fetchMarkets(hit.event_id);
      const filtered = markets.map(m => {
        const parsed = (() => {
          try { return pxSvc.parseMarketSelections(m); }
          catch (e) { return [{ error: e.message }]; }
        })();
        return {
          type: m.type,
          name: m.name,
          hasMarketLines: !!m.market_lines,
          marketLineCount: (m.market_lines || []).length,
          marketLineSample: (m.market_lines || []).slice(0, 3).map(ml => ({
            line: ml.line,
            favourite: ml.favourite,
            selCount: (ml.selections || []).length,
          })),
          hasSelections: !!m.selections,
          selCount: (m.selections || []).length,
          parsedCount: parsed.length,
          parsedMarketType: parsed[0]?.marketType,
          parsedSample: parsed.slice(0, 3).map(p => ({
            lineId: p.lineId,
            marketType: p.marketType,
            selection: p.selection,
            teamName: p.teamName,
            line: p.line,
          })),
        };
      });
      res.json({
        ok: true,
        event: { id: hit.event_id, name: hit.name, sport: hit.sport_name },
        marketCount: markets.length,
        markets: req.query.all ? filtered : filtered.filter(m => /spread|puck|line|run/i.test(m.name || '') || m.type === 'spread'),
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Direct Odds API probe — fetches a sport's Pinnacle supplement feed
  // and returns a book count / sample so we can tell whether zero Pinnacle
  // rows indicate API coverage gap or a merge bug on our side.
  app.get('/probe-odds-api', async (req, res) => {
    const sport = req.query.sport || 'basketball_nba';
    const apiKey = process.env.THE_ODDS_API_KEY;
    if (!apiKey) return res.status(400).json({ ok: false, error: 'THE_ODDS_API_KEY not set' });
    const fetch = require('node-fetch');
    const url = `https://api.the-odds-api.com/v4/sports/${sport}/odds`
      + `?apiKey=${apiKey}&regions=us,eu&markets=h2h,spreads,totals`
      + `&bookmakers=pinnacle,draftkings,fanduel&oddsFormat=american`;
    try {
      const resp = await fetch(url);
      const status = resp.status;
      const remaining = resp.headers.get('x-requests-remaining');
      if (!resp.ok) {
        const text = await resp.text();
        return res.json({ ok: false, status, remaining, errorBody: text.substring(0, 500) });
      }
      const events = await resp.json();
      const bookCounts = {};
      const sampleEvents = [];
      for (const ev of events) {
        for (const bm of (ev.bookmakers || [])) {
          bookCounts[bm.key] = (bookCounts[bm.key] || 0) + 1;
        }
        if (sampleEvents.length < 5) {
          sampleEvents.push({
            home: ev.home_team,
            away: ev.away_team,
            commence: ev.commence_time,
            books: (ev.bookmakers || []).map(b => b.key),
          });
        }
      }
      res.json({ ok: true, status, remaining, eventCount: events.length, bookCounts, sampleEvents });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  app.get('/book-coverage', (req, res) => {
    const sports = ['basketball_nba', 'baseball_mlb', 'icehockey_nhl', 'tennis', 'soccer', 'soccer_epl', 'soccer_usa_mls', 'soccer_uefa_champs_league', 'basketball_wnba'];
    const out = { sports: {} };
    for (const sport of sports) {
      const cache = oddsFeed.__debugGetCache(sport);
      if (!cache || !cache.events) continue;
      const stats = {
        totalEvents: 0,
        eventsWithH2H: 0,
        eventsWithSpread: 0,
        eventsWithTotals: 0,
        bookCounts: { pinnacle: 0, fanduel: 0, draftkings: 0, kalshi: 0 },
        rawBookKeys: {},
      };
      for (const entry of Object.values(cache.events)) {
        const evList = Array.isArray(entry) ? entry : [entry];
        for (const ev of evList) {
          if (!ev) continue;
          stats.totalEvents++;
          const m = ev.markets || {};
          const addBookCounts = (mkt) => {
            if (!mkt) return;
            if (mkt.pinnacle) stats.bookCounts.pinnacle++;
            if (mkt.fanduel) stats.bookCounts.fanduel++;
            if (mkt.draftkings) stats.bookCounts.draftkings++;
            if (mkt.kalshi) stats.bookCounts.kalshi++;
          };
          if (m.h2h) { stats.eventsWithH2H++; addBookCounts(m.h2h); }
          if (m.spreads) stats.eventsWithSpread++;
          if (m.totals) stats.eventsWithTotals++;
          for (const r of (ev._rawOdds || [])) {
            const sb = r.sportsbook || '(none)';
            stats.rawBookKeys[sb] = (stats.rawBookKeys[sb] || 0) + 1;
          }
        }
      }
      if (stats.totalEvents > 0) out.sports[sport] = stats;
    }
    res.json(out);
  });

  // List line index (debugging)
  app.get('/lines', (req, res) => {
    const summary = lineManager.getLineSummary();
    res.json({
      count: lineManager.getLineCount(),
      bySportAndMarket: summary,
    });
  });

  // Per-line detail — what lines we're actually registered to quote on.
  // Filter via query params to narrow down:
  //   ?sport=basketball_nba
  //   ?market=series_winner        (exact match)
  //   ?market=series_              (prefix match if trailing underscore)
  //   ?team=Lakers                 (substring match on teamName/home/away)
  //   ?event=1500006697            (pxEventId exact)
  //   ?limit=500                   (default 200, max 10000)
  // Returns: [{ lineId, sport, pxEventId, pxEventName, marketType,
  //             teamName, selection, line, homeTeam, awayTeam, startTime }]
  // Cap raised from 2000 → 10000 on 2026-05-05: with 4.7k+ lines registered,
  // the prior cap silently dropped low-volume sports (MMA/Boxing/soccer
  // sub-leagues) from the unfiltered Lines tab because Object.entries
  // iterates in insertion order and high-volume sports come first.
  app.get('/lines/detail', (req, res) => {
    const idx = lineManager.__debugGetLineIndex();
    const q = req.query || {};
    const sportFilter = q.sport ? String(q.sport).toLowerCase() : null;
    const marketFilter = q.market ? String(q.market).toLowerCase() : null;
    const marketIsPrefix = marketFilter && marketFilter.endsWith('_');
    const teamFilter = q.team ? String(q.team).toLowerCase() : null;
    const eventFilter = q.event ? String(q.event) : null;
    const limit = Math.min(parseInt(q.limit) || 200, 10000);

    const out = [];
    for (const [lineId, info] of Object.entries(idx)) {
      if (!info) continue;
      if (sportFilter && String(info.sport || '').toLowerCase() !== sportFilter) continue;
      if (marketFilter) {
        const mt = String(info.marketType || '').toLowerCase();
        if (marketIsPrefix ? !mt.startsWith(marketFilter) : mt !== marketFilter) continue;
      }
      if (teamFilter) {
        const hay = [info.teamName, info.homeTeam, info.awayTeam, info.pxEventName]
          .filter(Boolean).join(' ').toLowerCase();
        if (!hay.includes(teamFilter)) continue;
      }
      if (eventFilter && String(info.pxEventId || '') !== eventFilter) continue;
      // Best-effort fair prob + quote preview. Uses oddsFeed.getFairProb
      // (synchronous; no on-demand alt-line fetch here — we don't want to
      // spam the Odds API on a Lines tab browse). For series/MMA/golf
      // lines that need their own fair-prob paths, fairProb will be null
      // and myOdds won't show until the user gets an actual RFQ.
      let fairProb = null;
      // bookPriceOverride surfaces when the fair comes from a source
      // that includes the raw book-offered price we want to quote AT
      // (e.g. BetOnline manual upload for Zurich — operator preference
      // is quote-as-book, not undercut). Non-null value is the raw
      // implied prob from the source book; we display it as myOdds
      // rather than applying our default vig on top of fair.
      let bookPriceOverride = null;
      try {
        // Golf matchups: route through pricer.getGolfMatchupFairProb
        // FIRST so the manual book upload (operator-supplied Bookmaker
        // odds) wins over DataGolf, matching what priceParlay does at
        // RFQ time. Otherwise oddsFeed.getFairProb would hit DataGolf
        // first and the dashboard would show DataGolf's de-vigged fair
        // even with a manual override sitting in cache.
        if (info.sport === 'golf_matchups') {
          const golfRes = pricer.getGolfMatchupFairProb(info);
          if (golfRes != null && typeof golfRes === 'object') {
            fairProb = golfRes.fairProb;
            bookPriceOverride = golfRes.bookPriceOverride;
          } else if (golfRes != null) {
            fairProb = golfRes;
          }
        }
        // Player-prop lines (K-prop + Phase-2 NBA/NHL props) carry their
        // resolved fair prob directly on lineInfo because the seed-time
        // bridge already paid the SharpAPI / TOA lookup cost. Read it
        // first; the generic oddsFeed.getFairProb path doesn't know how
        // to recover prop fair-prob (sync SharpAPI returns null when the
        // pitcher/player isn't in the SharpAPI cache, and there's no
        // sync TOA fallback at lookup time). Without this preference,
        // /lines/detail shows null fair-prob for every K-prop / Phase-2
        // prop line, even though the prop bridge populated it correctly
        // at seed and the actual quote-time pricer uses it just fine.
        if (fairProb == null && info.marketType && /^player_/.test(info.marketType)) {
          if (info.fairProb != null) {
            fairProb = info.fairProb;
          } else if (info.fairProbOver != null && info.fairProbUnder != null) {
            fairProb = info.selection === 'over' ? info.fairProbOver : info.fairProbUnder;
          }
        }
        // Series markets (series_winner / series_spread / series_total —
        // NBA + NHL playoff series). These price through the DK scraper
        // via pricer.getSeriesFairProb at RFQ time, but the generic
        // oddsFeed.getFairProb path doesn't know about that source. Without
        // this branch, every series line in /lines/detail surfaces with
        // null fair even though the actual quote-time pricer fills them
        // correctly. Verified 2026-05-03 NHL series_spread / series_total
        // for COL/MIN, CAR/PHI, VEG/ANA — all fair=null in Lines tab,
        // populated correctly via DK scraper at RFQ time.
        if (fairProb == null && info.marketType
            && (info.marketType.startsWith('series_')
                || info.oddsApiMarket === 'series_winner'
                || info.oddsApiMarket === 'series_spread'
                || info.oddsApiMarket === 'series_total')) {
          const seriesFair = pricer.getSeriesFairProb(info);
          if (seriesFair != null && typeof seriesFair === 'object') {
            fairProb = seriesFair.fairProb;
            bookPriceOverride = seriesFair.bookPriceOverride;
          } else if (seriesFair != null) {
            fairProb = seriesFair;
          }
        }
        // Non-golf, OR golf matchup with no manual/scraper hit: fall
        // through to the generic oddsFeed.getFairProb. Pass the SIGNED
        // line — critical for spreads where home -1.5 and home +1.5
        // are different bets. getFairProb applies Math.abs internally
        // for totals, so signed input is safe for both markets.
        if (fairProb == null) {
          fairProb = oddsFeed.getFairProb(
            info.oddsApiSport || info.sport,
            info.homeTeam,
            info.awayTeam,
            info.oddsApiMarket || info.marketType,
            info.oddsApiSelection || info.selection,
            info.line != null ? info.line : null,
            info.startTime
          );
        }
        // Alt-line cache fallback. getFairProb returns null when the
        // primary cache has no markets[marketType] for an event (typical
        // for MMA totals: SharpAPI seeded h2h-only, DK didn't capture
        // Total Rounds for that fight). The pricer's sync fast path
        // already consults getAltLineFairProbSync — extending the same
        // treatment here lets /lines/detail reflect quotability for
        // lines whose fair prob lives in altLinesCache (warmed by
        // mergeDkMmaFights TOA backstop or any prior RFQ).
        if (fairProb == null) {
          try {
            fairProb = oddsFeed.getAltLineFairProbSync(
              info.oddsApiSport || info.sport,
              info.homeTeam,
              info.awayTeam,
              info.oddsApiMarket || info.marketType,
              info.oddsApiSelection || info.selection,
              info.line != null ? info.line : null,
              info.startTime
            );
          } catch (_) { /* ignore */ }
        }
        // Final fallback: lineInfo carries a stored fairProb when registration
        // resolved one at seed-time (player props pre-seed path, tennis
        // on-demand registration via TOA per-event fetch, golf manual
        // upload, series scraper hit cached on the line). Without this
        // fallback, the Lines tab shows null fair for any line whose fair
        // came from a source the synchronous accessors don't replay.
        // Verified 2026-05-03 tennis Zverev/Sinner: registered via
        // resolveUnknownLine but oddsFeed.getFairProb returns null since
        // tennis cache holds only minor-tour matches; pricing-path RFQs
        // worked because they re-fetched TOA per-event. Lines tab needs
        // the seed-time-resolved value preserved on lineInfo.
        if (fairProb == null && info.fairProb != null) {
          fairProb = info.fairProb;
        }
      } catch (_) { /* ignore */ }
      // If bookPriceOverride is set, quote at that raw implied
      // instead of fair + our vig. Mirrors the priceParlay behavior
      // so /lines/detail display matches what we'd quote on an RFQ.
      let quote = null;
      if (bookPriceOverride != null && bookPriceOverride > 0 && bookPriceOverride < 1) {
        quote = {
          vig: fairProb != null ? (bookPriceOverride - fairProb) / fairProb : null,
          impliedProb: bookPriceOverride,
          americanOdds: pricer.decimalToAmerican(1 / bookPriceOverride),
        };
      } else if (fairProb != null && fairProb > 0 && fairProb < 1) {
        quote = pricer.computeSingleLegQuote(fairProb, info.sport, info.marketType);
      }

      // Per-book raw odds for this selection. Must be LINE-AWARE for
      // spreads/totals — the previous implementation looked up
      // market[book][selection] in the primary cache without passing
      // the registered line, which returned the MAIN-line odds for
      // every row including 20-point alt lines. That made e.g. NBA
      // "Under 200.5" (true book price ≈ +500) display as -108 (the
      // main-line 220.5 price), producing nonsense -37pp vsConsensus
      // deltas on every alt row.
      //
      // Use the proper line-aware accessors which fall through to
      // altLinesCache via getAltLineBookOdds when the registered line
      // differs from the primary. Moneylines still need the line=null
      // call since lineMatchesPrimary special-cases h2h.
      let bookOdds = { pinnacle: null, fanduel: null, draftkings: null, kalshi: null };
      try {
        const oaSport = info.oddsApiSport || info.sport;
        const oaMarket = info.oddsApiMarket || info.marketType;
        const sel = info.oddsApiSelection || info.selection;
        if (sel) {
          bookOdds.pinnacle   = oddsFeed.getPinnacleOdds(oaSport, info.homeTeam, info.awayTeam, oaMarket, sel, info.startTime, info.line);
          bookOdds.fanduel    = oddsFeed.getFanDuelOdds(oaSport, info.homeTeam, info.awayTeam, oaMarket, sel, info.startTime, info.line);
          bookOdds.draftkings = oddsFeed.getDraftKingsOdds(oaSport, info.homeTeam, info.awayTeam, oaMarket, sel, info.startTime, info.line);
          bookOdds.kalshi     = oddsFeed.getKalshiOdds(oaSport, info.homeTeam, info.awayTeam, oaMarket, sel, info.startTime, info.line);
        }
      } catch (_) { /* ignore */ }

      // Consensus = average implied prob across Pin / FD / DK (exclude Kalshi).
      // Convert back to American odds for display. Diff vs my odds is in pp.
      const consBooks = ['pinnacle', 'fanduel', 'draftkings']
        .map(b => bookOdds[b])
        .filter(o => o != null && Number.isFinite(o));
      let consensusAmerican = null, consensusImplied = null;
      if (consBooks.length > 0) {
        const implied = consBooks.map(o => (o >= 0 ? 100 / (o + 100) : -o / (-o + 100)));
        consensusImplied = implied.reduce((a, b) => a + b, 0) / implied.length;
        if (consensusImplied > 0 && consensusImplied < 1) {
          consensusAmerican = pricer.decimalToAmerican(1 / consensusImplied);
        }
      }
      const vsConsensusPp = (consensusImplied != null && quote)
        ? Math.round((quote.impliedProb - consensusImplied) * 10000) / 100
        : null;

      out.push({
        lineId,
        sport: info.sport,
        pxEventId: info.pxEventId,
        pxEventName: info.pxEventName,
        marketType: info.marketType,
        marketName: info.marketName,
        teamName: info.teamName,
        selection: info.selection,
        line: info.line,
        homeTeam: info.homeTeam,
        awayTeam: info.awayTeam,
        startTime: info.startTime,
        fairProb: fairProb != null ? Math.round(fairProb * 10000) / 10000 : null,
        fairAmerican: (fairProb != null && fairProb > 0 && fairProb < 1)
          ? pricer.decimalToAmerican(1 / fairProb) : null,
        myOddsAmerican: quote ? quote.americanOdds : null,
        myVigPct: quote ? Math.round(quote.vig * 10000) / 100 : null,
        pinnacleOdds: bookOdds.pinnacle,
        fanduelOdds: bookOdds.fanduel,
        draftkingsOdds: bookOdds.draftkings,
        kalshiOdds: bookOdds.kalshi,
        consensusAmerican,
        vsConsensusPp,
        oddsApiSport: info.oddsApiSport || null,
        oddsApiMarket: info.oddsApiMarket || null,
        oddsApiSelection: info.oddsApiSelection || null,
        isDNB: !!info.isDNB,
        // Operator-disabled flags. lineDisabled = this specific lineId is
        // blocklisted; eventDisabled = the entire pxEventId is blocklisted
        // (cascades to every line under it). The pricer treats either as
        // a decline reason 'manually_disabled'.
        lineDisabled: lineManager.isLineDisabled(lineId) && !(info.pxEventId != null && lineManager.isPxEventDisabled(info.pxEventId)),
        eventDisabled: info.pxEventId != null && lineManager.isPxEventDisabled(info.pxEventId),
      });
      if (out.length >= limit) break;
    }
    // Sort for easy scanning: sport → market → event → team
    out.sort((a, b) =>
      (a.sport || '').localeCompare(b.sport || '') ||
      (a.marketType || '').localeCompare(b.marketType || '') ||
      (a.pxEventName || '').localeCompare(b.pxEventName || '') ||
      (a.teamName || '').localeCompare(b.teamName || '')
    );
    res.json({ count: out.length, lines: out });
  });

  // SGP tracking — acceptance rate + ROI per combo. Built from the
  // in-memory orders store rather than Supabase so it's always current.
  app.get('/sgp-stats', (req, res) => {
    const orders = orderTracker.getRecentOrders(5000);
    const bycombo = {};
    const overall = {
      quoted: 0, confirmed: 0, rejected: 0,
      settled: 0, wins: 0, losses: 0, pushes: 0,
      stakeConfirmed: 0, stakeSettled: 0, pnl: 0,
    };
    const bucketOf = (k) => (bycombo[k] = bycombo[k] || {
      quoted: 0, confirmed: 0, rejected: 0,
      settled: 0, wins: 0, losses: 0, pushes: 0,
      stakeConfirmed: 0, stakeSettled: 0, pnl: 0,
      samples: [],
    });
    for (const o of orders) {
      const meta = o.meta || {};
      if (!meta.isSGP) continue;
      const combo = meta.sgpCombo || 'unclassified';
      const b = bucketOf(combo);
      // Count every quoted row (quoted / confirmed / rejected / settled_*)
      b.quoted++;
      overall.quoted++;
      const status = o.status || '';
      if (status === 'rejected') { b.rejected++; overall.rejected++; }
      if (status === 'confirmed' || status.startsWith('settled_')) {
        b.confirmed++;
        overall.confirmed++;
        b.stakeConfirmed += Number(o.confirmedStake || 0);
        overall.stakeConfirmed += Number(o.confirmedStake || 0);
      }
      if (status.startsWith('settled_')) {
        b.settled++;
        overall.settled++;
        b.stakeSettled += Number(o.confirmedStake || 0);
        overall.stakeSettled += Number(o.confirmedStake || 0);
        const pnl = Number(o.pnl || 0);
        b.pnl += pnl;
        overall.pnl += pnl;
        if (o.settlementResult === 'won')  { b.wins++;   overall.wins++; }
        if (o.settlementResult === 'lost') { b.losses++; overall.losses++; }
        if (o.settlementResult === 'push') { b.pushes++; overall.pushes++; }
      }
      // Keep a small sample for ad-hoc inspection
      if (b.samples.length < 10) {
        b.samples.push({
          parlayId: o.parlayId,
          status,
          offeredOdds: o.offeredOdds,
          fairParlayProb: meta.fairParlayProb,
          vigRateUsed: meta.vigRateUsed,
          sgpVigMultiplier: meta.sgpVigMultiplier,
          confirmedStake: o.confirmedStake,
          settlementResult: o.settlementResult,
          pnl: o.pnl,
          quotedAt: o.quotedAt,
          confirmedAt: o.confirmedAt,
          settledAt: o.settledAt,
        });
      }
    }
    // Compute derived metrics per bucket + overall
    const derive = (b) => ({
      ...b,
      acceptanceRate: b.quoted > 0 ? b.confirmed / b.quoted : null,
      winRate: (b.wins + b.losses) > 0 ? b.wins / (b.wins + b.losses) : null,
      roi: b.stakeSettled > 0 ? b.pnl / b.stakeSettled : null,
    });
    res.json({
      config: {
        allowedCombos: config.pricing.sgpAllowedCombos || [],
        vigMultiplier: config.pricing.sgpVigMultiplier,
      },
      overall: derive(overall),
      byCombo: Object.fromEntries(Object.entries(bycombo).map(([k, v]) => [k, derive(v)])),
    });
  });

  // Lineup tracker — MLB pitchers + NHL goalies, including recent changes.
  // Shows grace-window activity so we can audit declines labeled "lineup change".
  app.get('/lineups', (req, res) => {
    const cache = oddsFeed.getLineupCache();
    const now = Date.now();
    const GRACE_MS = 3 * 60 * 1000;
    const out = { sports: {}, recentChanges: [] };
    for (const [sport, bucket] of Object.entries(cache)) {
      out.sports[sport] = { total: 0, withStarters: 0, recentlyChanged: 0, entries: [] };
      for (const [key, entry] of Object.entries(bucket)) {
        out.sports[sport].total++;
        if (entry.homeStarter || entry.awayStarter) out.sports[sport].withStarters++;
        const changeAgeMs = entry.lastChangeAt ? (now - entry.lastChangeAt) : null;
        const inGrace = changeAgeMs != null && changeAgeMs < GRACE_MS;
        if (inGrace) out.sports[sport].recentlyChanged++;
        const rec = {
          key,
          homeStarter: entry.homeStarter,
          awayStarter: entry.awayStarter,
          lastChangeAt: entry.lastChangeAt ? new Date(entry.lastChangeAt).toISOString() : null,
          lastChangeDetail: entry.lastChangeDetail,
          changeAgeSec: changeAgeMs != null ? Math.round(changeAgeMs / 1000) : null,
          inGrace,
        };
        out.sports[sport].entries.push(rec);
        if (entry.lastChangeAt) out.recentChanges.push({ sport, ...rec });
      }
    }
    out.recentChanges.sort((a, b) => (b.lastChangeAt || '').localeCompare(a.lastChangeAt || ''));
    out.recentChanges = out.recentChanges.slice(0, 50);
    res.json(out);
  });

  // --- PWA / Mobile App routes ---
  const push = require('./services/push');
  // Hydrate push subscriptions from Supabase so notifications survive
  // Railway redeploys. Fire-and-forget — failures are non-fatal.
  push.hydrateFromDb().catch(err =>
    log.warn('Push', `hydrate failed: ${err.message}`));

  app.get('/app', (req, res) => {
    push.resetBadge();
    res.sendFile(path.join(__dirname, 'client', 'app.html'));
  });
  app.get('/app/manifest.json', (req, res) => {
    res.sendFile(path.join(__dirname, 'client', 'manifest.json'));
  });
  app.get('/app/sw.js', (req, res) => {
    res.setHeader('Service-Worker-Allowed', '/');
    res.setHeader('Content-Type', 'application/javascript');
    res.sendFile(path.join(__dirname, 'client', 'sw.js'));
  });
  // SVG icon for PWA
  app.get('/app/icon-192.svg', (req, res) => {
    res.setHeader('Content-Type', 'image/svg+xml');
    res.send(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 192 192">
      <rect width="192" height="192" rx="32" fill="#0d1117"/>
      <text x="96" y="120" text-anchor="middle" font-size="100" font-family="sans-serif" font-weight="bold" fill="#58a6ff">P</text>
    </svg>`);
  });
  app.get('/app/icon-512.svg', (req, res) => {
    res.setHeader('Content-Type', 'image/svg+xml');
    res.send(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
      <rect width="512" height="512" rx="80" fill="#0d1117"/>
      <text x="256" y="320" text-anchor="middle" font-size="280" font-family="sans-serif" font-weight="bold" fill="#58a6ff">P</text>
    </svg>`);
  });
  app.get('/push/vapid-key', (req, res) => {
    res.json({ publicKey: push.getVapidPublicKey() });
  });
  app.post('/push/subscribe', (req, res) => {
    const body = req.body || {};
    push.addSubscription(body);
    // Optional: client may send `mutedCategories` in the same payload
    // so we don't need a follow-up round trip on first install.
    if (Array.isArray(body.mutedCategories)) {
      push.setMutedCategories(body.endpoint, body.mutedCategories);
    }
    res.json({ ok: true, subscriptions: push.getSubscriptionCount() });
  });

  // Update muted categories for a subscription. Server-side gate for
  // per-category notification suppression — authoritative across SW
  // versions and PWA install states. Body: {endpoint, mutedCategories[]}.
  app.post('/push/mute-prefs', (req, res) => {
    const body = req.body || {};
    if (!body.endpoint) return res.status(400).json({ ok: false, error: 'endpoint required' });
    push.setMutedCategories(body.endpoint, body.mutedCategories || []);
    res.json({ ok: true, endpoint: body.endpoint, muted: push.getMutedCategories(body.endpoint) });
  });
  // Debug: how many push subscriptions does the server currently hold?
  app.get('/push/status', (req, res) => {
    res.json({
      subscriptions: push.getSubscriptionCount(),
      vapidConfigured: !!push.getVapidPublicKey(),
      lastDailySummary: _lastDailySummaryAt || null,
    });
  });

  // Operator-triggered test notification. Hit /push/test?category=settlement
  // (or any category name) to verify SW handlers, browser permissions, and
  // network reach. Safe to call any time.
  app.post('/push/test', (req, res) => {
    const cat = (req.body && req.body.category) || (req.query && req.query.category) || 'test';
    push.sendTestNotification(cat);
    res.json({ ok: true, category: cat, subscriptions: push.getSubscriptionCount() });
  });
  app.get('/push/test', (req, res) => {
    const cat = (req.query && req.query.category) || 'test';
    push.sendTestNotification(cat);
    res.json({ ok: true, category: cat, subscriptions: push.getSubscriptionCount() });
  });

  // Notify on service start so the operator knows when the service comes
  // back from a Railway redeploy. Skip the very first boot's wakeup if no
  // subscriptions are hydrated yet.
  setTimeout(() => {
    try {
      if (push.getSubscriptionCount() > 0) {
        push.notifyConnectionState('restarted', `Parlay SP online · ${new Date().toLocaleString('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit' })} ET`);
      }
    } catch (_) { /* fire-and-forget */ }
  }, 5000);

  // End-of-day summary notification — fires once per ET day at ~23:50 ET.
  // Composes a P&L + fill summary from the in-memory order tracker and
  // pushes it. Skipped if no subscriptions or if already sent for the day.
  let _lastDailySummaryAt = null; // ISO date string (YYYY-MM-DD ET) of last send
  function maybeSendDailySummary() {
    try {
      const nowEt = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', hour12: false });
      const [datePart, timePart] = nowEt.split(', ');
      const [hh, mm] = (timePart || '').split(':').map(Number);
      // Window: 23:45 — 23:59 ET. Send once per day in that window.
      if (hh !== 23 || mm < 45) return;
      const todayEt = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
      if (_lastDailySummaryAt === todayEt) return;
      if (push.getSubscriptionCount() === 0) return;
      // Build summary from order tracker — fills, wins, losses, P&L since 10am ET today
      const orderTrackerMod = require('./services/order-tracker');
      const recentOrders = orderTrackerMod.getRecentOrders(2000);
      const cutoff = new Date(todayEt + 'T10:00:00-04:00').getTime();
      let fills = 0, wins = 0, losses = 0, pnl = 0;
      for (const o of (recentOrders || [])) {
        const t = o.settledAt ? new Date(o.settledAt).getTime() : 0;
        if (t < cutoff) continue;
        if (o.status === 'settled_won') { wins++; fills++; pnl += (o.pnl || 0); }
        else if (o.status === 'settled_lost') { losses++; fills++; pnl += (o.pnl || 0); }
        else if (o.status === 'settled_push' || o.status === 'settled_void') { fills++; pnl += (o.pnl || 0); }
      }
      push.notifyDailySummary({ fills, wins, losses, pnl });
      _lastDailySummaryAt = todayEt;
      log.info('Push', `Daily summary sent for ${todayEt}: ${fills} fills, ${wins}W/${losses}L, $${pnl.toFixed(2)}`);
    } catch (err) {
      log.debug('Push', `Daily summary check failed: ${err.message}`);
    }
  }
  // Check every 5 minutes — cheap, no-op outside the 23:45-23:59 window.
  setInterval(maybeSendDailySummary, 5 * 60 * 1000);

  // SPA fallback
  app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'client', 'index.html'));
  });

  const server = app.listen(config.server.port, () => {
    log.info('Startup', `    ✓ Status server on port ${config.server.port}`);
  });
  server.on('error', (err) => {
    log.error('Startup', `Status server failed: ${err.message}`);
  });
}

// ---------------------------------------------------------------------------
// GRACEFUL SHUTDOWN
// ---------------------------------------------------------------------------

function shutdown(signal) {
  log.info('Shutdown', `Received ${signal} — shutting down...`);

  if (oddsRefreshTimer) clearInterval(oddsRefreshTimer);
  if (lineRefreshTimer) clearInterval(lineRefreshTimer);
  if (settlementPollTimer) clearInterval(settlementPollTimer);
  websocket.disconnect();

  log.info('Shutdown', 'Final stats:', orderTracker.getStats());
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Handle uncaught errors
process.on('uncaughtException', (err) => {
  log.error('Fatal', `Uncaught exception: ${err.message}`, err.stack);
  shutdown('uncaughtException');
});

process.on('unhandledRejection', (reason) => {
  log.error('Fatal', `Unhandled rejection: ${reason}`);
});

// ---------------------------------------------------------------------------
// RUN
// ---------------------------------------------------------------------------

console.log(`[Boot] PORT=${process.env.PORT}, PX_ACCESS_KEY=${process.env.PX_ACCESS_KEY ? 'set' : 'MISSING'}, ODDS_API_KEY=${process.env.ODDS_API_KEY ? 'set' : 'MISSING'}`);

startup().catch(err => {
  log.error('Startup', `Fatal startup error: ${err.message}`);
  log.error('Startup', err.stack);
});
