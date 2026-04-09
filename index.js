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

  // Decline audit: rank unknown events/sports by how often they're declining parlays
  app.get('/decline-audit', (req, res) => {
    try {
      const intel = orderTracker.getMarketIntel(1000);
      const declines = intel.declines || {};
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
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: err.message });
    }
  });

  // Debug: list PX sport events with sport_name grouping (diagnose sport name mismatches)
  app.get('/px-events-debug', async (req, res) => {
    try {
      const allEvents = await px.fetchSportEvents();
      const bySportName = {};
      for (const e of allEvents) {
        const sn = e.sport_name || '(none)';
        if (!bySportName[sn]) bySportName[sn] = { count: 0, sample: [] };
        bySportName[sn].count++;
        if (bySportName[sn].sample.length < 3) {
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
      // Only return settled ones in detail + summary
      const settled = pxOrders.filter(o => o.settlement_status && !['tbd','requested','none'].includes(o.settlement_status));
      res.json({ ok: true, total: pxOrders.length, byStatus, settled });
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

  // List line index (debugging)
  app.get('/lines', (req, res) => {
    const summary = lineManager.getLineSummary();
    res.json({
      count: lineManager.getLineCount(),
      bySportAndMarket: summary,
    });
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
