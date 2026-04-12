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

  // Supabase caps single queries at 1000 rows by default. Paginate via .range()
  // to fetch all requested orders beyond that cap.
  const PAGE_SIZE = 1000;
  const all = [];
  const startMs = Date.now();
  let pagesFetched = 0;
  try {
    let offset = 0;
    while (offset < limit) {
      const pageSize = Math.min(PAGE_SIZE, limit - offset);
      // Stable pagination: primary sort on quoted_at (may be NULL for
      // reconstructed orders) + secondary sort on parlay_id so .range()
      // is guaranteed deterministic across pages.
      const { data, error } = await db
        .from('parlay_orders')
        .select('*')
        .order('quoted_at', { ascending: false, nullsFirst: false })
        .order('parlay_id', { ascending: true })
        .range(offset, offset + pageSize - 1);
      if (error) {
        log.error('DB', `loadOrders page ${pagesFetched} at offset ${offset} failed: ${error.message} (code=${error.code} details=${error.details})`);
        break;
      }
      if (!data || data.length === 0) {
        if (pagesFetched === 0) {
          log.warn('DB', `loadOrders first page returned empty — table may be empty or the query returned no rows`);
        }
        break;
      }
      all.push(...data);
      pagesFetched++;
      if (data.length < pageSize) break; // reached end
      offset += pageSize;
    }
    log.info('DB', `loadOrders: ${all.length} rows in ${pagesFetched} pages (${Date.now() - startMs}ms)`);

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

module.exports = {
  getClient,
  isEnabled,
  saveOrder,
  loadOrders,
  loadOrdersByParlayIds,
  countOrders,
  saveMatchedParlay,
  loadMatchedParlays,
  saveDecline,
  loadDeclines,
  lookupDecline,
  saveLineCache,
  loadLineCacheEntry,
  loadLineCacheBulk,
  loadLineCacheByEventIds,
};
