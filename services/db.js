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
      meta: order.meta || {},
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
  if (!db) return [];

  try {
    const { data, error } = await db
      .from('parlay_orders')
      .select('*')
      .order('quoted_at', { ascending: false })
      .limit(limit);

    if (error) {
      log.error('DB', `Failed to load orders: ${error.message}`);
      return [];
    }

    // Convert DB rows back to order format
    return (data || []).map(row => ({
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

  try {
    const { data, error } = await db
      .from('matched_parlays')
      .select('*')
      .order('matched_at', { ascending: false })
      .limit(limit);

    if (error) {
      log.error('DB', `Failed to load matched parlays: ${error.message}`);
      return [];
    }

    return (data || []).map(row => ({
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
  try {
    const { data, error } = await db
      .from('declines')
      .select('*')
      .order('declined_at', { ascending: false })
      .limit(limit);
    if (error) {
      log.warn('DB', `loadDeclines failed (table may not exist yet): ${error.message}`);
      return [];
    }
    return (data || []).map(row => ({
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

module.exports = {
  getClient,
  isEnabled,
  saveOrder,
  loadOrders,
  saveMatchedParlay,
  loadMatchedParlays,
  saveDecline,
  loadDeclines,
};
