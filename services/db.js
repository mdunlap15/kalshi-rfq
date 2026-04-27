const { createClient } = require('@supabase/supabase-js');
const log = require('./logger');

let supabase = null;

function getClient() {
  if (!supabase) {
    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_KEY;
    if (url && key) {
      supabase = createClient(url, key);
      log.info('DB', 'Supabase client initialized');
    }
  }
  return supabase;
}

function isEnabled() {
  return !!(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_KEY);
}

// ---------------------------------------------------------------------------
// PARLAY ORDERS
// ---------------------------------------------------------------------------

async function saveOrder(order) {
  const db = getClient();
  if (!db) return;

  try {
    // Stash pxProfit, expectedValue, and CLV fields inside meta (no dedicated
    // columns — keeps schema stable while persisting these derived fields).
    const metaWithExtras = { ...(order.meta || {}) };
    if (order.pxProfit != null) metaWithExtras.pxProfit = order.pxProfit;
    if (order.expectedValue != null) metaWithExtras.expectedValue = order.expectedValue;
    if (order.closingImpliedProb != null) metaWithExtras.closingImpliedProb = order.closingImpliedProb;
    if (order.clvDelta != null) metaWithExtras.clvDelta = order.clvDelta;

    const row = {
      parlay_id: order.parlayId,
      status: order.status,
      legs: order.legs || order.meta?.legs || [],
      offered_odds: order.offeredOdds,
      fair_parlay_prob: order.fairParlayProb,
      max_risk: order.maxRisk,
      vig: order.meta?.vig || order.vig,
      confirmed_odds: order.confirmedOdds,
      confirmed_stake: order.confirmedStake,
      order_uuid: order.orderUuid,
      pnl: order.pnl,
      settlement_result: order.settlementResult,
      quoted_at: order.quotedAt,
      confirmed_at: order.confirmedAt,
      settled_at: order.settledAt,
      meta: metaWithExtras,
    };

    // Guard: never let a reconstructed order overwrite a pxBackfill record.
    // The PX backfill is verified ground-truth data from the PX team's export.
    // Reconstructed orders are skeleton records from PX REST with incomplete data.
    if (metaWithExtras.reconstructed) {
      const { data: existing } = await db
        .from('parlay_orders')
        .select('meta')
        .eq('parlay_id', order.parlayId)
        .maybeSingle();
      if (existing?.meta?.pxBackfill) {
        log.debug('DB', `Blocked saveOrder for ${order.parlayId} — reconstructed cannot overwrite pxBackfill`);
        return;
      }
    }

    const { error } = await db
      .from('parlay_orders')
      .upsert(row, { onConflict: 'parlay_id' });

    if (error) {
      log.error('DB', `Failed to save order ${order.parlayId}: ${error.message}`);
    }
  } catch (err) {
    log.error('DB', `saveOrder error: ${err.message}`);
  }
}

async function loadOrders(limit = 100) {
  const db = getClient();
  if (!db) {
    log.warn('DB', 'loadOrders: no Supabase client available');
    return [];
  }

  // Load only settled + confirmed orders on startup. The 50K+ "quoted" rows
  // (unfilled RFQs) don't affect P&L, exposure, or positions and cause
  // Supabase free-tier timeouts when sorting/paginating the full table.
  // New quotes from the current session are tracked in memory.
  const PAGE_SIZE = 1000;
  const MAX_PAGE_RETRIES = 4;
  const all = [];
  const startMs = Date.now();
  let pagesFetched = 0;
  // Per-status row counts so we can compare to a head count and detect
  // silent partial loads (e.g. Supabase timeouts mid-pagination).
  const perStatus = {};
  const STATUSES = ['confirmed', 'settled_won', 'settled_lost', 'settled_push', 'rejected'];
  try {
    for (const status of STATUSES) {
      // Get authoritative count first so we know if pagination got truncated.
      let expected = null;
      try {
        const { count, error: cErr } = await db
          .from('parlay_orders')
          .select('*', { count: 'exact', head: true })
          .eq('status', status);
        if (!cErr) expected = count;
      } catch (_) { /* count is best-effort */ }

      let offset = 0;
      let loaded = 0;
      while (offset < limit - all.length) {
        const pageSize = Math.min(PAGE_SIZE, limit - all.length - offset);

        // Retry loop: Supabase free-tier occasionally times out individual
        // page queries. Previously a single failure would `break` out of the
        // while loop and silently drop the rest of this status's rows,
        // producing a biased subset (e.g. all losses + some wins → fake
        // negative P&L on restart). Retry with exponential backoff and only
        // give up after MAX_PAGE_RETRIES.
        let data = null;
        let lastError = null;
        for (let attempt = 0; attempt < MAX_PAGE_RETRIES; attempt++) {
          const result = await db
            .from('parlay_orders')
            .select('*')
            .eq('status', status)
            .order('parlay_id', { ascending: true })
            .range(offset, offset + pageSize - 1);
          if (!result.error) {
            data = result.data;
            lastError = null;
            break;
          }
          lastError = result.error;
          log.warn('DB', `loadOrders ${status} offset ${offset} attempt ${attempt + 1}/${MAX_PAGE_RETRIES} failed: ${result.error.message}`);
          // Exponential backoff: 250ms, 500ms, 1s, 2s
          await new Promise(r => setTimeout(r, 250 * Math.pow(2, attempt)));
        }
        if (lastError) {
          // After exhausting retries, log loudly. Do NOT break — try the
          // next page anyway. A single bad page shouldn't poison the
          // whole status. Worst case we still log the gap below.
          log.error('DB', `loadOrders ${status} offset ${offset}: gave up after ${MAX_PAGE_RETRIES} retries (${lastError.message})`);
          offset += pageSize;
          continue;
        }
        if (!data || data.length === 0) break;
        all.push(...data);
        loaded += data.length;
        pagesFetched++;
        if (data.length < pageSize) break;
        offset += pageSize;
      }
      perStatus[status] = loaded;

      // Drift detection: if we know the expected count and loaded fewer
      // rows, that's a partial load. Log loudly so this doesn't silently
      // corrupt P&L the way it did 2026-04-15 (loaded 180/675 wins, P&L
      // showed -$8,871 instead of +$7,536).
      if (expected != null && loaded < expected) {
        log.error('DB', `loadOrders PARTIAL LOAD for ${status}: got ${loaded} rows, expected ${expected} (missing ${expected - loaded})`);
      }
    }
    log.info('DB', `loadOrders: ${all.length} rows in ${pagesFetched} pages (${Date.now() - startMs}ms) — ${JSON.stringify(perStatus)}`);

    // Convert DB rows back to order format
    return all.map(row => ({
      parlayId: row.parlay_id,
      status: row.status,
      legs: row.legs,
      offeredOdds: row.offered_odds,
      fairParlayProb: row.fair_parlay_prob ? Number(row.fair_parlay_prob) : null,
      maxRisk: row.max_risk ? Number(row.max_risk) : null,
      vig: row.vig ? Number(row.vig) : null,
      confirmedOdds: row.confirmed_odds ? Number(row.confirmed_odds) : null,
      confirmedStake: row.confirmed_stake ? Number(row.confirmed_stake) : null,
      orderUuid: row.order_uuid,
      pnl: row.pnl != null ? Number(row.pnl) : null,
      settlementResult: row.settlement_result,
      quotedAt: row.quoted_at,
      confirmedAt: row.confirmed_at,
      settledAt: row.settled_at,
      meta: row.meta || {},
    }));
  } catch (err) {
    log.error('DB', `loadOrders error: ${err.message}`);
    return [];
  }
}

/**
 * Load specific orders by parlay_id from Supabase.
 * Returns a map { parlayId: orderObject } for orders that exist in the DB.
 * Used by fullPxReconcile to preserve pricing data (offeredOdds, fairParlayProb,
 * etc.) that would otherwise be lost when reconstructing from PX REST.
 */
async function loadOrdersByParlayIds(parlayIds) {
  const client = getClient();
  if (!client || !parlayIds || parlayIds.length === 0) return {};

  const result = {};
  // Supabase IN filter has practical limits; chunk to 500
  const CHUNK = 500;
  try {
    for (let i = 0; i < parlayIds.length; i += CHUNK) {
      const chunk = parlayIds.slice(i, i + CHUNK);
      const { data, error } = await client
        .from('parlay_orders')
        .select('*')
        .in('parlay_id', chunk);
      if (error) {
        log.warn('DB', `loadOrdersByParlayIds chunk failed: ${error.message}`);
        continue;
      }
      for (const row of (data || [])) {
        result[row.parlay_id] = {
          parlayId: row.parlay_id,
          status: row.status,
          legs: row.legs,
          offeredOdds: row.offered_odds,
          fairParlayProb: row.fair_parlay_prob ? Number(row.fair_parlay_prob) : null,
          maxRisk: row.max_risk ? Number(row.max_risk) : null,
          vig: row.vig ? Number(row.vig) : null,
          confirmedOdds: row.confirmed_odds ? Number(row.confirmed_odds) : null,
          confirmedStake: row.confirmed_stake ? Number(row.confirmed_stake) : null,
          orderUuid: row.order_uuid,
          pnl: row.pnl != null ? Number(row.pnl) : null,
          settlementResult: row.settlement_result,
          quotedAt: row.quoted_at,
          confirmedAt: row.confirmed_at,
          settledAt: row.settled_at,
          meta: row.meta || {},
        };
      }
    }
  } catch (err) {
    log.warn('DB', `loadOrdersByParlayIds error: ${err.message}`);
  }
  return result;
}

// ---------------------------------------------------------------------------
// MATCHED PARLAYS
// ---------------------------------------------------------------------------

async function saveMatchedParlay(entry) {
  const db = getClient();
  if (!db) return;

  try {
    const row = {
      parlay_id: entry.parlayId,
      matched_odds: entry.matchedAmericanOdds,
      matched_stake: entry.matchedStake,
      legs: entry.legs || [],
      we_quoted: entry.weQuoted || false,
      our_odds: entry.ourAmericanOdds,
      outcome: entry.outcome,
      matched_at: entry.matchedAt,
    };

    const { error } = await db
      .from('matched_parlays')
      .insert(row);

    if (error) {
      log.error('DB', `Failed to save matched parlay: ${error.message}`);
    }
  } catch (err) {
    log.error('DB', `saveMatchedParlay error: ${err.message}`);
  }
}

async function loadMatchedParlays(limit = 200) {
  const db = getClient();
  if (!db) return [];
  const PAGE_SIZE = 1000;
  const all = [];
  try {
    let offset = 0;
    while (offset < limit) {
      const pageSize = Math.min(PAGE_SIZE, limit - offset);
      const { data, error } = await db
        .from('matched_parlays')
        .select('*')
        .order('matched_at', { ascending: false, nullsFirst: false })
        .order('parlay_id', { ascending: true })
        .range(offset, offset + pageSize - 1);
      if (error) {
        log.error('DB', `Failed to load matched parlays (page at offset ${offset}): ${error.message}`);
        break;
      }
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < pageSize) break;
      offset += pageSize;
    }
    return all.map(row => ({
      parlayId: row.parlay_id,
      matchedAmericanOdds: row.matched_odds,
      matchedStake: row.matched_stake ? Number(row.matched_stake) : null,
      legs: row.legs || [],
      weQuoted: row.we_quoted,
      ourAmericanOdds: row.our_odds,
      outcome: row.outcome,
      matchedAt: row.matched_at,
      legCount: (row.legs || []).length,
    }));
  } catch (err) {
    log.error('DB', `loadMatchedParlays error: ${err.message}`);
    return [];
  }
}

// ---------------------------------------------------------------------------
// DECLINES — persistent record of every declined RFQ
// ---------------------------------------------------------------------------

async function saveDecline(entry) {
  const db = getClient();
  if (!db) return;
  try {
    const row = {
      parlay_id: entry.parlayId || null,
      reason: entry.reason || 'unknown',
      detail: entry.detail || null,
      known_legs: entry.knownLegs || [],
      unknown_line_ids: entry.unknownLineIds || [],
      unknown_details: entry.unknownDetails || [],
      is_limit: !!entry.isLimit,
      declined_at: entry.declinedAt || new Date().toISOString(),
    };
    const { error } = await db.from('declines').insert(row);
    if (error) {
      // Table may not exist — log once and keep going
      if (!saveDecline._warned) {
        log.error('DB', `saveDecline failed (run the SQL migration to create 'declines' table): ${error.message}`);
        saveDecline._warned = true;
      }
    }
  } catch (err) {
    if (!saveDecline._warned) {
      log.error('DB', `saveDecline error: ${err.message}`);
      saveDecline._warned = true;
    }
  }
}

async function loadDeclines(limit = 2000) {
  const db = getClient();
  if (!db) return [];
  const PAGE_SIZE = 1000;
  const all = [];
  try {
    let offset = 0;
    while (offset < limit) {
      const pageSize = Math.min(PAGE_SIZE, limit - offset);
      const { data, error } = await db
        .from('declines')
        .select('*')
        .order('declined_at', { ascending: false, nullsFirst: false })
        .order('id', { ascending: true })
        .range(offset, offset + pageSize - 1);
      if (error) {
        log.warn('DB', `loadDeclines failed (table may not exist yet): ${error.message}`);
        return [];
      }
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < pageSize) break;
      offset += pageSize;
    }
    return all.map(row => ({
      parlayId: row.parlay_id,
      reason: row.reason,
      detail: row.detail,
      knownLegs: row.known_legs || [],
      unknownLineIds: row.unknown_line_ids || [],
      unknownDetails: row.unknown_details || [],
      isLimit: !!row.is_limit,
      declinedAt: row.declined_at,
    }));
  } catch (err) {
    log.error('DB', `loadDeclines error: ${err.message}`);
    return [];
  }
}

async function lookupDecline(parlayId) {
  const db = getClient();
  if (!db) return null;
  try {
    const { data, error } = await db
      .from('declines')
      .select('reason, detail')
      .eq('parlay_id', parlayId)
      .limit(1);
    if (error || !data || data.length === 0) return null;
    return { reason: data[0].reason, detail: data[0].detail };
  } catch (err) {
    return null;
  }
}

async function countOrders() {
  const db = getClient();
  if (!db) return null;
  try {
    // Total rows
    const { count: total, error: e1 } = await db
      .from('parlay_orders')
      .select('*', { count: 'exact', head: true });
    if (e1) { log.error('DB', `countOrders total failed: ${e1.message}`); return null; }
    // Settled rows
    const { count: settled, error: e2 } = await db
      .from('parlay_orders')
      .select('*', { count: 'exact', head: true })
      .like('status', 'settled_%');
    if (e2) { log.error('DB', `countOrders settled failed: ${e2.message}`); return null; }
    // Confirmed rows
    const { count: confirmed, error: e3 } = await db
      .from('parlay_orders')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'confirmed');
    if (e3) { log.error('DB', `countOrders confirmed failed: ${e3.message}`); return null; }
    // Breakdown by settled_won/lost/push/void
    const breakdown = {};
    for (const s of ['settled_won', 'settled_lost', 'settled_push', 'settled_void']) {
      const { count } = await db
        .from('parlay_orders')
        .select('*', { count: 'exact', head: true })
        .eq('status', s);
      breakdown[s] = count || 0;
    }
    return { total, settled, confirmed, breakdown };
  } catch (err) {
    log.error('DB', `countOrders error: ${err.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// LINE CACHE — persistent lineId → team/market mapping
// ---------------------------------------------------------------------------

/**
 * Bulk-upsert the entire lineIndex to Supabase so historical line_ids survive
 * restarts even after PX purges events from the mm namespace.
 *
 * @param {Object} lineIndex - { lineId: { sport, pxEventId, teamName, ... } }
 */
async function saveLineCache(lineIndex) {
  const db = getClient();
  if (!db) return;

  const entries = Object.entries(lineIndex);
  if (entries.length === 0) return;

  const now = new Date().toISOString();
  const rows = entries.map(([lineId, info]) => ({
    line_id: lineId,
    sport: info.sport || null,
    px_event_id: info.pxEventId || null,
    px_event_name: info.pxEventName || null,
    market_type: info.marketType || null,
    market_name: info.marketName || null,
    is_dnb: !!info.isDNB,
    selection: info.selection || info.oddsApiSelection || null,
    team_name: info.teamName || null,
    line: info.line != null ? info.line : null,
    home_team: info.homeTeam || null,
    away_team: info.awayTeam || null,
    odds_api_sport: info.oddsApiSport || info.sport || null,
    odds_api_market: info.oddsApiMarket || null,
    odds_api_selection: info.oddsApiSelection || null,
    competitor_id: info.competitorId || null,
    start_time: info.startTime || null,
    updated_at: now,
  }));

  // Supabase upsert in chunks of 500 to stay within payload limits
  const CHUNK = 500;
  let saved = 0;
  try {
    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      const { error } = await db
        .from('line_cache')
        .upsert(chunk, { onConflict: 'line_id' });
      if (error) {
        if (!saveLineCache._warned) {
          log.error('DB', `saveLineCache failed (run the SQL migration to create 'line_cache' table): ${error.message}`);
          saveLineCache._warned = true;
        }
        return;
      }
      saved += chunk.length;
    }
    log.info('DB', `saveLineCache: upserted ${saved} lines`);
  } catch (err) {
    if (!saveLineCache._warned) {
      log.error('DB', `saveLineCache error: ${err.message}`);
      saveLineCache._warned = true;
    }
  }
}

/**
 * Look up a single lineId from the persistent cache.
 * Returns the same shape as lineIndex entries, or null.
 */
async function loadLineCacheEntry(lineId) {
  const db = getClient();
  if (!db) return null;
  try {
    const { data, error } = await db
      .from('line_cache')
      .select('*')
      .eq('line_id', lineId)
      .limit(1);
    if (error || !data || data.length === 0) return null;
    const row = data[0];
    return {
      sport: row.sport,
      pxEventId: row.px_event_id,
      pxEventName: row.px_event_name,
      marketType: row.market_type,
      marketName: row.market_name,
      isDNB: !!row.is_dnb,
      selection: row.selection,
      teamName: row.team_name,
      line: row.line != null ? Number(row.line) : null,
      homeTeam: row.home_team,
      awayTeam: row.away_team,
      oddsApiSport: row.odds_api_sport,
      oddsApiMarket: row.odds_api_market,
      oddsApiSelection: row.odds_api_selection,
      competitorId: row.competitor_id,
      startTime: row.start_time,
    };
  } catch (err) {
    return null;
  }
}

/**
 * Bulk-load multiple lineIds from the persistent cache.
 * Returns a map { lineId: info }.
 */
async function loadLineCacheBulk(lineIds) {
  const db = getClient();
  if (!db) return {};
  if (!lineIds || lineIds.length === 0) return {};

  const result = {};
  const CHUNK = 200;
  try {
    for (let i = 0; i < lineIds.length; i += CHUNK) {
      const chunk = lineIds.slice(i, i + CHUNK);
      const { data, error } = await db
        .from('line_cache')
        .select('*')
        .in('line_id', chunk);
      if (error) {
        log.warn('DB', `loadLineCacheBulk failed: ${error.message}`);
        break;
      }
      for (const row of data || []) {
        result[row.line_id] = {
          sport: row.sport,
          pxEventId: row.px_event_id,
          pxEventName: row.px_event_name,
          marketType: row.market_type,
          marketName: row.market_name,
          isDNB: !!row.is_dnb,
          selection: row.selection,
          teamName: row.team_name,
          line: row.line != null ? Number(row.line) : null,
          homeTeam: row.home_team,
          awayTeam: row.away_team,
          oddsApiSport: row.odds_api_sport,
          oddsApiMarket: row.odds_api_market,
          oddsApiSelection: row.odds_api_selection,
          competitorId: row.competitor_id,
          startTime: row.start_time,
        };
      }
    }
  } catch (err) {
    log.warn('DB', `loadLineCacheBulk error: ${err.message}`);
  }
  return result;
}

/**
 * Look up line_cache entries by px_event_id. Returns one representative
 * entry per event (any line for that event — we just need homeTeam/awayTeam).
 * Returns { eventId: info }.
 */
async function loadLineCacheByEventIds(eventIds) {
  const db = getClient();
  if (!db) return {};
  if (!eventIds || eventIds.length === 0) return {};

  const result = {};
  const CHUNK = 200;
  try {
    // Convert to strings since px_event_id may be stored as text
    const ids = eventIds.map(String);
    for (let i = 0; i < ids.length; i += CHUNK) {
      const chunk = ids.slice(i, i + CHUNK);
      const { data, error } = await db
        .from('line_cache')
        .select('*')
        .in('px_event_id', chunk);
      if (error) {
        log.warn('DB', `loadLineCacheByEventIds failed: ${error.message}`);
        break;
      }
      for (const row of data || []) {
        const eid = row.px_event_id;
        // Keep first match per event (we just need home/away team)
        if (result[eid]) continue;
        result[eid] = {
          sport: row.sport,
          pxEventId: row.px_event_id,
          pxEventName: row.px_event_name,
          marketType: row.market_type,
          teamName: row.team_name,
          homeTeam: row.home_team,
          awayTeam: row.away_team,
          startTime: row.start_time,
          oddsApiSport: row.odds_api_sport,
        };
      }
    }
  } catch (err) {
    log.warn('DB', `loadLineCacheByEventIds error: ${err.message}`);
  }
  return result;
}

// ---------------------------------------------------------------------------
// DAILY P&L — query settled orders grouped by settlement date
// ---------------------------------------------------------------------------

async function getDailyPnL(days = 30, opts = {}) {
  const db = getClient();
  if (!db) return [];

  // groupBy selects which column buckets a row into a day:
  //   - 'settled_at' (default): date the outcome landed — matches the
  //     prior behaviour and the settlement-centric P&L view.
  //   - 'quoted_at': date the offer was made — matches the dashboard's
  //     Daily Volume & P&L chart, which groups by quote date so the
  //     forensic "what happened on April 18" question lines up with
  //     what the operator sees.
  const groupBy = opts.groupBy === 'quoted_at' ? 'quoted_at' : 'settled_at';

  try {
    const cutoff = new Date(Date.now() - days * 24 * 3600 * 1000).toISOString();
    // Paginate past Supabase's default 1,000-row ceiling. At ~150 fills/day
    // of confirmed volume, ~30 days of settled rows easily exceeds that
    // limit; prior behaviour silently truncated at whichever 10-ish days
    // filled the first page.
    const PAGE_SIZE = 1000;
    const MAX_PAGE_RETRIES = 4;
    const MAX_ROWS = 50000;
    const rows = [];
    let offset = 0;
    while (rows.length < MAX_ROWS) {
      const pageSize = Math.min(PAGE_SIZE, MAX_ROWS - rows.length);
      let pageData = null;
      let lastError = null;
      for (let attempt = 0; attempt < MAX_PAGE_RETRIES; attempt++) {
        const result = await db.from('parlay_orders')
          .select('parlay_id, status, pnl, confirmed_stake, offered_odds, settled_at, quoted_at')
          .like('status', 'settled_%')
          .gte(groupBy, cutoff)
          .order(groupBy, { ascending: true })
          .range(offset, offset + pageSize - 1);
        if (!result.error) { pageData = result.data; lastError = null; break; }
        lastError = result.error;
        await new Promise(r => setTimeout(r, 250 * Math.pow(2, attempt)));
      }
      if (lastError) {
        log.warn('DB', `getDailyPnL offset ${offset}: gave up after ${MAX_PAGE_RETRIES} retries: ${lastError.message}`);
        break;
      }
      if (!pageData || pageData.length === 0) break;
      for (const row of pageData) rows.push(row);
      if (pageData.length < pageSize) break;
      offset += pageSize;
    }
    if (rows.length === 0) return [];

    // Group by date (YYYY-MM-DD in local timezone) using the selected column.
    const byDay = {};
    for (const row of rows) {
      const bucketTs = row[groupBy];
      if (!bucketTs) continue;
      const day = new Date(bucketTs).toLocaleDateString('en-CA'); // YYYY-MM-DD
      if (!byDay[day]) byDay[day] = { date: day, pnl: 0, wins: 0, losses: 0, pushes: 0, risk: 0, fills: 0 };
      const d = byDay[day];
      d.pnl += (row.pnl || 0);
      d.fills++;
      if (row.status === 'settled_won') d.wins++;    // SP won (bettor lost parlay)
      else if (row.status === 'settled_lost') d.losses++;  // SP lost (bettor won parlay)
      else d.pushes++;
      d.risk += (row.confirmed_stake || 0);
    }

    return Object.values(byDay).sort((a, b) => a.date.localeCompare(b.date));
  } catch (err) {
    log.warn('DB', `getDailyPnL error: ${err.message}`);
    return [];
  }
}

/**
 * Load fully-hydrated orders for a date range. Read-only forensic
 * endpoint — pulls everything needed to decompose a single day's P&L
 * by sport, parlay structure, shared legs, counterparty, etc. Paginated
 * with retries matching loadFillBucketRowsSince's pattern so it can pull
 * days that blow past Supabase's default 1,000-row ceiling.
 *
 * groupBy: 'quoted_at' (default) or 'settled_at' — which timestamp
 * column drives the range filter.
 */
async function loadOrdersInDateRange(fromIso, toIso, opts = {}) {
  const db = getClient();
  if (!db) return [];
  const groupBy = opts.groupBy === 'settled_at' ? 'settled_at' : 'quoted_at';
  const statusFilter = opts.status; // optional — e.g. 'settled_lost'
  const PAGE_SIZE = 1000;
  const MAX_PAGE_RETRIES = 4;
  const MAX_ROWS = opts.maxRows || 10000;
  const rows = [];
  let offset = 0;
  const startMs = Date.now();
  try {
    while (rows.length < MAX_ROWS) {
      const pageSize = Math.min(PAGE_SIZE, MAX_ROWS - rows.length);
      let pageData = null;
      let lastError = null;
      for (let attempt = 0; attempt < MAX_PAGE_RETRIES; attempt++) {
        let query = db.from('parlay_orders')
          .select('parlay_id, status, legs, offered_odds, confirmed_odds, confirmed_stake, max_risk, fair_parlay_prob, pnl, quoted_at, confirmed_at, settled_at, settlement_result, order_uuid, meta')
          .gte(groupBy, fromIso)
          .lte(groupBy, toIso);
        if (statusFilter) query = query.eq('status', statusFilter);
        const result = await query.order(groupBy, { ascending: true }).range(offset, offset + pageSize - 1);
        if (!result.error) { pageData = result.data; lastError = null; break; }
        lastError = result.error;
        await new Promise(r => setTimeout(r, 250 * Math.pow(2, attempt)));
      }
      if (lastError) {
        log.warn('DB', `loadOrdersInDateRange offset ${offset}: gave up after ${MAX_PAGE_RETRIES} retries: ${lastError.message}`);
        break;
      }
      if (!pageData || pageData.length === 0) break;
      for (const row of pageData) rows.push(row);
      if (pageData.length < pageSize) break;
      offset += pageSize;
    }
    log.info('DB', `loadOrdersInDateRange ${fromIso}..${toIso} (${groupBy}${statusFilter ? ',' + statusFilter : ''}): ${rows.length} rows (${Date.now() - startMs}ms)`);
    return rows;
  } catch (err) {
    log.warn('DB', `loadOrdersInDateRange error: ${err.message}`);
    return rows;
  }
}

/**
 * Load minimal order fields needed to reconstruct fill-bucket events
 * over a historical window. Pulls every parlay_orders row with
 * quoted_at >= cutoff, so it includes 'quoted' rows (unfilled RFQs)
 * that loadOrders() intentionally skips.
 *
 * Returns rows shaped as { parlayId, quotedAt, confirmedAt, status, legs }.
 * Paginated with retries; caps at 'cap' rows to bound memory / boot time.
 */
async function loadFillBucketRowsSince(cutoffIso, cap = 200000) {
  const db = getClient();
  if (!db) return [];
  const PAGE_SIZE = 1000;
  const MAX_PAGE_RETRIES = 4;
  const all = [];
  let offset = 0;
  const startMs = Date.now();
  try {
    while (all.length < cap) {
      const pageSize = Math.min(PAGE_SIZE, cap - all.length);
      let data = null;
      let lastError = null;
      for (let attempt = 0; attempt < MAX_PAGE_RETRIES; attempt++) {
        const result = await db
          .from('parlay_orders')
          .select('parlay_id, status, legs, quoted_at, confirmed_at')
          .gte('quoted_at', cutoffIso)
          .order('quoted_at', { ascending: true })
          .range(offset, offset + pageSize - 1);
        if (!result.error) {
          data = result.data;
          lastError = null;
          break;
        }
        lastError = result.error;
        log.warn('DB', `loadFillBucketRowsSince offset ${offset} attempt ${attempt + 1}/${MAX_PAGE_RETRIES} failed: ${result.error.message}`);
        await new Promise(r => setTimeout(r, 250 * Math.pow(2, attempt)));
      }
      if (lastError) {
        log.error('DB', `loadFillBucketRowsSince offset ${offset}: gave up after ${MAX_PAGE_RETRIES} retries`);
        break;
      }
      if (!data || data.length === 0) break;
      for (const row of data) {
        all.push({
          parlayId: row.parlay_id,
          status: row.status,
          legs: row.legs || [],
          quotedAt: row.quoted_at,
          confirmedAt: row.confirmed_at,
        });
      }
      if (data.length < pageSize) break;
      offset += pageSize;
    }
    log.info('DB', `loadFillBucketRowsSince: ${all.length} rows (${Date.now() - startMs}ms)`);
    if (all.length >= cap) {
      log.warn('DB', `loadFillBucketRowsSince hit cap ${cap} — fill-bucket history may be incomplete`);
    }
    return all;
  } catch (err) {
    log.error('DB', `loadFillBucketRowsSince error: ${err.message}`);
    return all;
  }
}

/**
 * Get the total P&L sum directly from Supabase (source of truth).
 * Sums the pnl column for all settled orders — no in-memory drift.
 */
async function getTotalPnL() {
  const db = getClient();
  if (!db) return null;

  try {
    const { data, error } = await db.from('parlay_orders')
      .select('pnl')
      .like('status', 'settled_%')
      .not('pnl', 'is', null);

    if (error) {
      log.warn('DB', `getTotalPnL error: ${error.message}`);
      return null;
    }
    if (!data) return 0;
    return data.reduce((sum, row) => sum + (Number(row.pnl) || 0), 0);
  } catch (err) {
    log.warn('DB', `getTotalPnL error: ${err.message}`);
    return null;
  }
}

/**
 * Persist a web-push subscription so it survives Railway redeploys.
 * Subscriptions were previously in-memory only (services/push.js), so
 * every redeploy silently dropped every subscription and notifications
 * stopped firing until the operator re-enabled them in the browser.
 *
 * Table schema (run in Supabase SQL editor once):
 *
 *   create table if not exists push_subscriptions (
 *     endpoint text primary key,
 *     subscription jsonb not null,
 *     created_at timestamptz default now()
 *   );
 *   alter table push_subscriptions enable row level security;
 */
async function savePushSubscription(sub) {
  if (!isEnabled() || !sub || !sub.endpoint) return;
  const db = getClient();
  try {
    const { error } = await db.from('push_subscriptions').upsert({
      endpoint: sub.endpoint,
      subscription: sub,
    }, { onConflict: 'endpoint' });
    if (error) log.warn('DB', `savePushSubscription error: ${error.message}`);
  } catch (err) {
    log.warn('DB', `savePushSubscription exception: ${err.message}`);
  }
}

async function loadPushSubscriptions() {
  if (!isEnabled()) return [];
  const db = getClient();
  try {
    const { data, error } = await db.from('push_subscriptions').select('subscription');
    if (error) {
      log.warn('DB', `loadPushSubscriptions error: ${error.message}`);
      return [];
    }
    return (data || []).map(r => r.subscription).filter(s => s && s.endpoint);
  } catch (err) {
    log.warn('DB', `loadPushSubscriptions exception: ${err.message}`);
    return [];
  }
}

async function deletePushSubscription(endpoint) {
  if (!isEnabled() || !endpoint) return;
  const db = getClient();
  try {
    const { error } = await db.from('push_subscriptions').delete().eq('endpoint', endpoint);
    if (error) log.warn('DB', `deletePushSubscription error: ${error.message}`);
  } catch (err) {
    log.warn('DB', `deletePushSubscription exception: ${err.message}`);
  }
}

// ---------------------------------------------------------------------------
// KEY-VALUE STORE
// ---------------------------------------------------------------------------
// Generic JSONB store for small pieces of state that need to survive
// service restarts but don't warrant a dedicated table. One row per key.
//
// Schema expected:
//   create table if not exists kv_store (
//     key         text primary key,
//     value       jsonb not null,
//     updated_at  timestamptz default now()
//   );
//
// First used for the BetOnline Zurich manual-upload cache — operator
// uploads once, cache survives Railway redeploys without re-posting.

async function saveKV(key, value) {
  const db = getClient();
  if (!db) return;
  try {
    const { error } = await db
      .from('kv_store')
      .upsert({ key, value, updated_at: new Date().toISOString() });
    if (error) log.warn('DB', `saveKV(${key}) error: ${error.message}`);
  } catch (err) {
    log.warn('DB', `saveKV(${key}) exception: ${err.message}`);
  }
}

async function loadKV(key) {
  const db = getClient();
  if (!db) return null;
  try {
    const { data, error } = await db
      .from('kv_store')
      .select('value')
      .eq('key', key)
      .maybeSingle();
    if (error) {
      log.warn('DB', `loadKV(${key}) error: ${error.message}`);
      return null;
    }
    return data?.value || null;
  } catch (err) {
    log.warn('DB', `loadKV(${key}) exception: ${err.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// PLAYER-PROP SHADOW QUOTES (Phase 1 — observation-only logging)
// ---------------------------------------------------------------------------
// Persists what we WOULD have priced for pitcher_strikeouts legs that
// arrived in PX RFQs. Used to validate the prop matching pipeline + book
// coverage before flipping to real quoting in Phase 2.
//
// Schema (run manually in Supabase SQL editor before this writes):
//   CREATE TABLE prop_shadow_quotes (
//     id BIGSERIAL PRIMARY KEY,
//     parlay_id TEXT,
//     line_id TEXT,
//     px_event_id TEXT,
//     market_name TEXT,
//     player_name TEXT,
//     line NUMERIC,
//     prop_type TEXT,
//     fair_prob_over NUMERIC,
//     fair_prob_under NUMERIC,
//     books_with_both_sides INT,
//     books TEXT[],
//     resolved_event_id TEXT,
//     match_error TEXT,
//     match_stages TEXT[],
//     recorded_at TIMESTAMPTZ DEFAULT now()
//   );
async function savePropShadowQuote(entry) {
  const db = getClient();
  if (!db) return;
  try {
    const row = {
      parlay_id: entry.parlayId || null,
      line_id: entry.lineId || null,
      px_event_id: entry.pxEventId || null,
      market_name: entry.marketName || null,
      player_name: entry.playerName || null,
      line: entry.line != null ? entry.line : null,
      prop_type: entry.propType || null,
      fair_prob_over: entry.fairProbOver != null ? entry.fairProbOver : null,
      fair_prob_under: entry.fairProbUnder != null ? entry.fairProbUnder : null,
      books_with_both_sides: entry.booksWithBothSides != null ? entry.booksWithBothSides : null,
      books: entry.books || null,
      resolved_event_id: entry.resolvedEventId || null,
      match_error: entry.matchError || null,
      match_stages: entry.matchStages || null,
      recorded_at: entry.recordedAt || new Date().toISOString(),
    };
    const { error } = await db.from('prop_shadow_quotes').insert(row);
    if (error && !savePropShadowQuote._warned) {
      log.error('DB', `savePropShadowQuote failed (run the SQL migration to create 'prop_shadow_quotes' table): ${error.message}`);
      savePropShadowQuote._warned = true;
    }
  } catch (err) {
    if (!savePropShadowQuote._warned) {
      log.error('DB', `savePropShadowQuote error: ${err.message}`);
      savePropShadowQuote._warned = true;
    }
  }
}

module.exports = {
  getClient,
  isEnabled,
  saveOrder,
  loadOrders,
  loadOrdersByParlayIds,
  loadFillBucketRowsSince,
  countOrders,
  saveMatchedParlay,
  loadMatchedParlays,
  saveDecline,
  loadDeclines,
  lookupDecline,
  savePropShadowQuote,
  saveKV,
  loadKV,
  saveLineCache,
  loadLineCacheEntry,
  loadLineCacheBulk,
  loadLineCacheByEventIds,
  getDailyPnL,
  getTotalPnL,
  loadOrdersInDateRange,
  savePushSubscription,
  loadPushSubscriptions,
  deletePushSubscription,
};
