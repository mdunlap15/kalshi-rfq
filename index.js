// =============================================================================
// ProphetX Parlay Service Provider
// MLB / NBA / NHL — Spreads, Moneylines, Totals
// =============================================================================
console.log('[BOOT] Process starting, NODE_ENV=' + process.env.NODE_ENV + ', PORT=' + process.env.PORT);

const { config, validate, getBankroll } = require('./config');
const log = require('./services/logger');
const px = require('./services/prophetx');
const oddsFeed = require('./services/odds-feed');
const lineManager = require('./services/line-manager');
const websocket = require('./services/websocket');
const orderTracker = require('./services/order-tracker');
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

  // Step 1b: Load historical data from Supabase
  try {
    await orderTracker.loadFromDb();
  } catch (err) {
    log.warn('Startup', `    ⚠ DB load failed: ${err.message} — continuing with empty state`);
  }

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

  // Step 4: Connect WebSocket
  log.info('Startup', '4/5 Connecting to ProphetX WebSocket...');
  try {
    await websocket.connect();
    log.info('Startup', '    ✓ WebSocket connected');
  } catch (err) {
    log.error('Startup', `    ✗ WebSocket connection failed: ${err.message}`);
    log.warn('Startup', '    Service will run without WebSocket — use /status to check state');
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

  // Check game results every 2 minutes for early win detection + fix bogus settlements
  setInterval(async () => {
    try {
      orderTracker.revertBogusSettlements();
      await orderTracker.checkLegResults();
      orderTracker.reconcileSettlements();
    } catch (err) {
      log.debug('Results', `Result check failed: ${err.message}`);
    }
  }, 2 * 60 * 1000);

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

  // Initial balance fetch
  try {
    const bal = await px.fetchBalance();
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
        maxRisk: config.pricing.maxRiskPerParlay,
        maxLegs: config.pricing.maxLegs,
        maxExposurePerTeam: config.pricing.maxExposurePerTeam,
        stalePriceMinutes: config.pricing.stalePriceMinutes,
        sports: config.supportedSports,
        baseUrl: config.px.baseUrl,
      },
      websocket: websocket.getState(),
      lines: {
        registered: lineManager.getLineCount(),
        bySportAndMarket: lineManager.getLineSummary(),
        lastSeed: lineManager.getStats(),
      },
      odds: oddsFeed.getCacheStatus(),
      orders: orderTracker.getStats(),
      exposure: {
        maxPerTeam: config.pricing.maxExposurePerTeam,
        teams: orderTracker.getExposureSnapshot(),
        maxPerGamePct: config.pricing.maxExposurePerGamePct,
        maxPerGame: getBankroll() * config.pricing.maxExposurePerGamePct / 100,
        games: orderTracker.getGameExposureSnapshot(),
      },
      portfolio: {
        bankroll: getBankroll(),
        maxDrawdownPct: config.pricing.maxDrawdownPct,
        maxDrawdown: getBankroll() * config.pricing.maxDrawdownPct / 100,
        currentRisk: orderTracker.getTotalPortfolioRisk(),
        maxRiskPerParlay: config.pricing.maxRiskPerParlay,
        maxRiskPerParlayPct: config.pricing.maxRiskPerParlayPct,
        maxRiskPerParlayFromPct: config.pricing.maxRiskPerParlayPct > 0
          ? getBankroll() * config.pricing.maxRiskPerParlayPct / 100
          : null,
      },
      alerts: orderTracker.getAlerts(),
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
  app.get('/orders', (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    res.json({
      stats: orderTracker.getStats(),
      pnlBySport: orderTracker.getPnLBySport(),
      recentOrders: orderTracker.getRecentOrders(limit),
    });
  });

  // Market intelligence
  app.get('/market-intel', (req, res) => {
    const limit = parseInt(req.query.limit) || 50;
    res.json(orderTracker.getMarketIntel(limit));
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

  // Refresh live odds for in-progress games and update weighted exposure
  app.post('/refresh-live-odds', async (req, res) => {
    try {
      const result = await orderTracker.refreshLiveOdds(oddsFeed);
      res.json({ ok: true, ...result });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Enrich reconstructed orders by looking up lineIds in the current lineManager.
  // Reconstructed orders are ones rebuilt from PX settlement data when we missed
  // the WS confirmation event — their legs initially have team='?' etc.
  app.post('/enrich-reconstructed', (req, res) => {
    try {
      const result = orderTracker.enrichReconstructedOrders();
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
      const orders = await px.fetchOrders(500);
      const match = orders.find(o => o.order_uuid === uuid || o.p_id === uuid || (o.order_uuid && o.order_uuid.startsWith(uuid)));
      if (!match) return res.json({ error: 'Order not found in PX', searched: orders.length });
      res.json({ ok: true, pxOrder: match });
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

  // Decline audit: rank unknown events/sports by how often they're declining parlays.
  // Optional ?window=5m|15m|30m|1h|2h|6h|24h — when provided, stats are computed from
  // the rolling event log filtered to the window rather than all-session cumulative.
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
    res.json({ ok: true, paused: true });
  });

  app.post('/resume', (req, res) => {
    websocket.resume();
    res.json({ ok: true, paused: false });
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
