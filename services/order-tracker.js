const log = require('./logger');
const db = require('./db');
const templateExposure = require('./template-exposure');

// ---------------------------------------------------------------------------
// IN-MEMORY ORDER STORE (backed by Supabase for persistence)
// ---------------------------------------------------------------------------

const orders = {}; // keyed by parlayId
const ordersByUuid = {}; // secondary index: orderUuid → parlayId

// ---------------------------------------------------------------------------
// MARKET INTELLIGENCE — tracks all matched parlays across all SPs
// ---------------------------------------------------------------------------
const matchedParlays = []; // array of { parlayId, matchedOdds, matchedStake, legs, matchedAt, weQuoted, ourOdds, outcome }
const marketStats = {
  totalMatched: 0,
  weQuoted: 0,
  weWon: 0,
  // weLost: kept for back-compat with historical DB rows, but no longer
  // incremented. Per Alec (PX): order.matched events are private and only
  // fire on our own wins; we cannot observe losses via this path. Any
  // legacy 'lost' entries loaded from DB are misclassified wins from the
  // pre-fix era. Downstream aggregations filter them out.
  weLost: 0,
  missedNoQuote: 0, // matched event received for a parlay we didn't quote (rare)
};

// Session-only fill-rate counters. The orders map is NOT a reliable
// denominator for fill rate: loadFromDb skips the 50K+ unfilled 'quoted'
// rows, so the orders map is heavily biased toward historical fills →
// every fill-rate computation over it rounds near 100%.
//
// These counters track only quotes submitted and fills received THIS
// SESSION (reset on each boot). Keyed by rawSport|legCount so the
// frontend can canonicalize + bucket however it wants.
//
// Shape: sessionFillBuckets['basketball_nba|2'] = { submitted: 5, filled: 2 }
//        sessionFillBuckets['Multi|3']           = { submitted: 10, filled: 1 }
const sessionFillBuckets = {};
// Rolling timestamped event log so the dashboard can aggregate fill
// rates over windows wider than the current session (24h / 7d / 30d).
// Entries: { t: ms, key: 'rawSport|legCount', kind: 'submit' | 'fill' }.
// Capped at MAX_FILL_BUCKET_EVENTS and pruned to FILL_BUCKET_WINDOW_MS
// on each write so memory stays bounded (~60B * 300k ≈ 18MB worst case).
// Restarts wipe this — windows that cross a restart will be partial.
const fillBucketEvents = [];
const FILL_BUCKET_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_FILL_BUCKET_EVENTS = 300000;
function pruneFillBucketEvents() {
  const cutoff = Date.now() - FILL_BUCKET_WINDOW_MS;
  let dropTo = 0;
  while (dropTo < fillBucketEvents.length && fillBucketEvents[dropTo].t < cutoff) dropTo++;
  if (dropTo > 0) fillBucketEvents.splice(0, dropTo);
  if (fillBucketEvents.length > MAX_FILL_BUCKET_EVENTS) {
    fillBucketEvents.splice(0, fillBucketEvents.length - MAX_FILL_BUCKET_EVENTS);
  }
}
function fillBucketKeys(legs) {
  const rawSports = [...new Set((legs || []).map(l => l.sport).filter(Boolean))];
  const key = rawSports.length === 0 ? 'Unknown'
    : rawSports.length === 1 ? rawSports[0]
    : 'Multi';
  const legCount = (legs || []).length;
  return [key + '|' + legCount];
}
function recordFillBucketSubmission(legs) {
  const now = Date.now();
  for (const k of fillBucketKeys(legs)) {
    if (!sessionFillBuckets[k]) sessionFillBuckets[k] = { submitted: 0, filled: 0 };
    sessionFillBuckets[k].submitted++;
    fillBucketEvents.push({ t: now, key: k, kind: 'submit' });
  }
  pruneFillBucketEvents();
}
function recordFillBucketFill(legs) {
  const now = Date.now();
  for (const k of fillBucketKeys(legs)) {
    if (!sessionFillBuckets[k]) sessionFillBuckets[k] = { submitted: 0, filled: 0 };
    sessionFillBuckets[k].filled++;
    fillBucketEvents.push({ t: now, key: k, kind: 'fill' });
  }
  pruneFillBucketEvents();
}

/**
 * Back-fill fillBucketEvents from Supabase on boot so the 24h/7d/30d
 * heatmap timeframes work across restarts. Pulls every parlay_orders
 * row with quoted_at in the window (includes unfilled 'quoted' rows
 * that loadFromDb skips) and replays them as synthetic submit + fill
 * events. Does NOT touch sessionFillBuckets — that stays session-scoped.
 */
async function backfillFillBucketEvents() {
  if (!db.isEnabled()) return;
  const cutoffIso = new Date(Date.now() - FILL_BUCKET_WINDOW_MS).toISOString();
  const rows = await db.loadFillBucketRowsSince(cutoffIso);
  if (!rows || rows.length === 0) return;
  const FILLED_STATUSES = new Set(['confirmed', 'settled_won', 'settled_lost', 'settled_push', 'settled_void']);
  let submits = 0, fills = 0;
  const historical = [];
  for (const row of rows) {
    const keys = fillBucketKeys(row.legs);
    const qt = row.quotedAt ? new Date(row.quotedAt).getTime() : null;
    if (qt && !Number.isNaN(qt)) {
      for (const k of keys) { historical.push({ t: qt, key: k, kind: 'submit' }); submits++; }
    }
    if (FILLED_STATUSES.has(row.status)) {
      const ft = row.confirmedAt ? new Date(row.confirmedAt).getTime() : qt;
      if (ft && !Number.isNaN(ft)) {
        for (const k of keys) { historical.push({ t: ft, key: k, kind: 'fill' }); fills++; }
      }
    }
  }
  historical.sort((a, b) => a.t - b.t);
  // Prepend historical, keep any session events that already accumulated
  // during boot (unlikely but guard against races).
  fillBucketEvents.unshift(...historical);
  fillBucketEvents.sort((a, b) => a.t - b.t);
  pruneFillBucketEvents();
  log.info('Tracker', `Backfilled fill-bucket events: ${submits} submits + ${fills} fills from ${rows.length} rows`);
}

// ---------------------------------------------------------------------------
// DECLINE TRACKING
// ---------------------------------------------------------------------------
const declineStats = {
  total: 0,
  reasons: {}, // { 'unknown legs': count, 'no fair value': count, etc. }
  unknownSports: {}, // { 'Soccer': count, 'unknown': count } — sports from unknown legs
  nearMisses: [], // RFQs where all legs were known but couldn't price (no fair value)
  // Rolling log of recent declines for the alert banner
  recent: [], // { reason, detail, time, parlayId }
  // Granular unknown leg categorization
  unknownLegCategories: {
    // category -> { count, bySport: { sport: count }, byResolveReason: { reason: count }, sampleLegs: [] }
    // Categories: player_prop, alt_line, alt_spread, alt_total, team_total, other_line, unknown
  },
  // PX market types we don't support — logged when a line_id is found in an
  // unsupported market.type during resolveUnknownLine. Lets us see exactly what
  // bettors are trying to price that we're declining wholesale.
  // key = marketType + '|' + marketName
  unsupportedMarkets: {},
  // Rolling log of full decline events with timestamps so /decline-audit can
  // filter by time window (last 5 min, last hour, etc) rather than only
  // returning all-session cumulative stats. Max 5000 entries keeps memory
  // bounded at ~2.5 MB.
  recentDeclineEvents: [],
};
const MAX_DECLINE_EVENTS = 5000;

function recordUnsupportedMarket(info) {
  if (!info || !info.marketType) return;
  const key = info.marketType + '|' + (info.marketName || '');
  if (!declineStats.unsupportedMarkets[key]) {
    declineStats.unsupportedMarkets[key] = {
      marketType: info.marketType,
      marketName: info.marketName || null,
      count: 0,
      sports: {}, // sport → count
      sampleEvents: [], // up to 5 event names
      firstSeen: new Date().toISOString(),
      lastSeen: null,
    };
  }
  const entry = declineStats.unsupportedMarkets[key];
  entry.count++;
  entry.lastSeen = new Date().toISOString();
  if (info.sport) entry.sports[info.sport] = (entry.sports[info.sport] || 0) + 1;
  if (info.eventName && !entry.sampleEvents.includes(info.eventName) && entry.sampleEvents.length < 5) {
    entry.sampleEvents.push(info.eventName);
  }
}
// Limit-related reasons — these are the ones we alert on (user-controllable)
const LIMIT_REASONS = new Set([
  'team exposure limit',
  'game exposure limit',
  'portfolio drawdown limit',
  'too many legs',
]);
// ---------------------------------------------------------------------------
// EXPOSURE-LIMIT DECLINE TRACKING — structured data about parlays declined
// because they'd exceed team/game/portfolio exposure limits. Lets us see how
// often large-payout parlays are coming through PX that we can't take on.
// ---------------------------------------------------------------------------
const exposureLimitStats = {
  total: 0,
  byReason: {}, // { 'team exposure limit': count, 'game exposure limit': count, ... }
  // Size buckets based on the estimated payout (estPayout × leg probs)
  // At quote time we only have estPayout (maxRiskPerParlay); at confirm time
  // we have the actual stake. We track both scenarios.
  bySizeBucket: {
    'under_1k': 0,
    '1k_5k': 0,
    '5k_10k': 0,
    '10k_50k': 0,
    '50k_plus': 0,
  },
  // Recent entries with full detail (cap at 200)
  recent: [],
  // Confirm-time rejections have actual stakes — tracked separately
  confirmTimeRejections: {
    total: 0,
    bySizeBucket: { 'under_1k': 0, '1k_5k': 0, '5k_10k': 0, '10k_50k': 0, '50k_plus': 0 },
    recent: [], // { parlayId, stake, reason, violations, time }
  },
};

function getSizeBucket(amount) {
  if (amount < 1000) return 'under_1k';
  if (amount < 5000) return '1k_5k';
  if (amount < 10000) return '5k_10k';
  if (amount < 50000) return '10k_50k';
  return '50k_plus';
}

// Reject reasons from risk checks at confirmation time
const rejectStats = {
  total: 0,
  reasons: {}, // { 'risk $123 > max $100': count }
  recent: [], // { reason, time, parlayId }
};
// Per-parlay decline lookup — lets us explain "No quote" outcomes in matched parlays
const declinesByParlayId = {}; // { parlayId: { reason, unknownLineIds, unknownDetails, declinedAt } }
const MAX_DECLINE_ENTRIES = 50000;
const declineIdOrder = []; // FIFO to cap memory

// ---------------------------------------------------------------------------
// NET EXPOSURE TRACKING — tracks risk per game, accounting for collected stakes
// ---------------------------------------------------------------------------
// Stores all confirmed parlay legs grouped by pxEventId (game)
// Net exposure = weighted payouts owed - stakes collected from offsetting positions
const gameExposure = {};  // keyed by pxEventId
// Legacy team exposure kept for backward compat with dashboard
const exposure = {};
// Phase 2 prop exposure — per-pitcher aggregate risk across all confirmed
// parlays containing a player_strikeouts leg for that pitcher's game.
// Key: `${pxEventId}|${normalizedPlayerName}`. Risk = sum of full
// confirmedStake from each containing parlay (overly cautious; refined
// later if needed). Used by checkPitcherExposure to gate new RFQs against
// the per-pitcher cap. Parallel to gameExposure / exposure — does NOT
// participate in the netExposure offsetting model.
const pitcherExposure = {};

// Per-player aggregate exposure for the new Phase-2 prop launch types
// (NBA points/rebounds/assists/threes_made, NHL shots_on_goal, etc.).
// Keyed by `${sport}|${normalizedPlayerName}` so the same player's
// exposure across multiple prop types within a sport rolls up to one
// number. Critical for cross-prop concentration: CJ McCollum points +
// rebounds + threes parlays all sum into one McCollum line.
//
// Parallel to pitcherExposure (which stays narrowly scoped to MLB
// player_strikeouts) — the K-prop pattern is preserved for backward
// compat. Eventually consolidate; for now they coexist cleanly.
const playerExposure = {};

// ---------------------------------------------------------------------------
// PENDING EXPOSURE — reservations for quotes not yet confirmed.
// Closes the race window where N identical RFQs all pass shouldDecline
// concurrently because none of them has been confirmed (and so added to
// `exposure`) yet. Each quote immediately reserves its worst-case risk
// against the per-team and per-game buckets; the reservation is released
// when the quote is confirmed (and replaced by real exposure), rejected,
// or expires (offerValidSeconds).
// Shape: { [parlayId]: { expiresAt, teamKeys:[{key, risk}], gameKeys:[{key, risk}] } }
// ---------------------------------------------------------------------------
const pendingExposure = {};

// ---------------------------------------------------------------------------
// RECENT LEG SIGNATURES — dedup identical parlay structures from the same
// bettor pool within a short window. Bettors can farm a correlated parlay
// by re-submitting it faster than our exposure state updates; we just say
// no on the second one.
// Shape: { [sigKey]: lastSeenMs }
// ---------------------------------------------------------------------------
const recentParlaySignatures = {};
const DEDUP_WINDOW_MS = 5 * 1000; // 5s: catch rapid re-submits only; pending exposure handles race window

function parlayLegSignature(legs) {
  if (!legs || legs.length === 0) return null;
  const ids = legs
    .map(l => String(l.line_id || l.lineId || l))
    .sort();
  return ids.join('|');
}

function checkRecentDuplicate(legs) {
  const sig = parlayLegSignature(legs);
  if (!sig) return null;
  const now = Date.now();
  // Opportunistic cleanup
  for (const [k, ts] of Object.entries(recentParlaySignatures)) {
    if (now - ts > DEDUP_WINDOW_MS) delete recentParlaySignatures[k];
  }
  const last = recentParlaySignatures[sig];
  if (last && now - last < DEDUP_WINDOW_MS) {
    return { ageMs: now - last };
  }
  return null;
}

function recordParlaySignature(legs) {
  const sig = parlayLegSignature(legs);
  if (sig) recentParlaySignatures[sig] = Date.now();
}

// Running stats
const stats = {
  totalQuotes: 0,
  totalConfirmations: 0,
  totalRejections: 0,
  totalSettlements: 0,
  totalWins: 0,
  totalLosses: 0,
  runningPnL: 0,
  startedAt: new Date().toISOString(),
  // Session-only counters: only incremented by live WS events, never by
  // reconciliation or DB rebuild.  Gives an accurate fill rate for this uptime window.
  sessionQuotes: 0,
  sessionFills: 0,
};

// ---------------------------------------------------------------------------
// RECORD FUNCTIONS
// ---------------------------------------------------------------------------

/**
 * Compute per-team + per-game risk reservations for a quote.
 * Uses the SAME key construction as addExposure (team+event+date) so that
 * checkExposureLimits sees consistent totals across real + pending.
 */
function buildPendingReservation(legs, worstCaseRisk, offerValidSeconds) {
  if (!legs || legs.length === 0) return null;
  const teamKeys = [];
  const gameKeys = [];
  const pitcherKeys = [];
  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    const li = leg.lineInfo || leg;
    const eventId = li.pxEventId;
    const gameDate = li.startTime ? new Date(li.startTime).toISOString().substring(0, 10) : '';
    let eventSuffix = eventId ? (eventId + '|' + gameDate) : null;
    if (!eventSuffix) {
      const opp = normalizeExposureKey((li.homeTeam || '') + (li.awayTeam || ''));
      eventSuffix = (opp || '') + '|' + (gameDate || 'noevent');
    }
    // Weighted by other-legs fair prob, matching addExposure
    let otherProb = 1;
    for (let j = 0; j < legs.length; j++) {
      if (j === i) continue;
      const ol = legs[j];
      const oli = ol.lineInfo || ol;
      otherProb *= (ol.fairProb || oli.fairProb || 0.5);
    }
    const weightedRisk = worstCaseRisk * otherProb;

    // Per-team key
    const teamName = li.teamName || li.team || li.homeTeam || li.awayTeam || 'unknown';
    const teamKey = normalizeExposureKey(teamName);
    if (teamKey) {
      teamKeys.push({ key: teamKey + '|' + eventSuffix, risk: weightedRisk });
    }
    // Per-game key
    const gameKey = eventId ? (eventId + '|' + gameDate) : ('syn_' + eventSuffix);
    gameKeys.push({ key: gameKey, risk: weightedRisk });
    // Per-pitcher key (only player_strikeouts legs). Use FULL worstCaseRisk
    // — not the otherProb-weighted version — to match addExposure's
    // pitcher-tracking semantics (which uses raw payout, not weighted).
    // checkPitcherExposure compares wouldBe against maxPerPitcher with
    // unweighted risk, so the pending side must match.
    if (li.marketType === 'player_strikeouts') {
      const pkey = pitcherKeyForLeg(li);
      if (pkey) {
        pitcherKeys.push({ key: pkey, risk: worstCaseRisk });
      }
    }
  }
  return {
    expiresAt: Date.now() + (offerValidSeconds || 120) * 1000,
    teamKeys,
    gameKeys,
    pitcherKeys,
  };
}

// Reverse indices for O(1) lookup of pending risk by team or game key.
// Maintained in sync with pendingExposure mutations (reservePending,
// releasePending, opportunistic expiry cleanup). Keeps getPendingTeamRisk
// and getPendingGameRisk constant-time instead of O(P × K) scans.
//
// Pre-fix (2026-04-25): per-RFQ exposure check called these helpers N×2
// times (per leg, per cap), each scanning all pending reservations. With
// 50 in-flight quotes × 4 keys each × 4 legs × 2 caps = 1,600 string
// comparisons per quote — 1ms+ on the hot path. Indices flatten this to
// O(N×2) hash lookups regardless of pending volume.
const pendingTeamRiskByKey = new Map(); // key → running risk total
const pendingGameRiskByKey = new Map(); // key → running risk total
// Per-pitcher pending index. Mirror of the team/game indices, added
// 2026-04-27 to close the K-prop quote-time race window where N
// concurrent RFQs all read pitcherExposure[key].risk == 0 (because none
// have confirmed yet) and all pass the per-pitcher cap. With this index
// the first reservation increments it; the next checkPitcherExposure
// reads real + pending and declines correctly.
const pendingPitcherRiskByKey = new Map(); // key → running risk total
// Same race-protection lane for the generic playerExposure (Phase-2
// prop launch types). Mirrors pitcher pending logic exactly — see
// reservePending / checkPlayerExposure for the read side.
const pendingPlayerRiskByKey = new Map(); // key → running risk total

function _addToIndex(idx, key, risk) {
  idx.set(key, (idx.get(key) || 0) + risk);
}
function _subFromIndex(idx, key, risk) {
  const v = (idx.get(key) || 0) - risk;
  if (v <= 1e-6) idx.delete(key);
  else idx.set(key, v);
}
function _applyReservationToIndices(reservation, sign) {
  if (!reservation) return;
  for (const tk of reservation.teamKeys || []) {
    if (sign > 0) _addToIndex(pendingTeamRiskByKey, tk.key, tk.risk);
    else _subFromIndex(pendingTeamRiskByKey, tk.key, tk.risk);
  }
  for (const gk of reservation.gameKeys || []) {
    if (sign > 0) _addToIndex(pendingGameRiskByKey, gk.key, gk.risk);
    else _subFromIndex(pendingGameRiskByKey, gk.key, gk.risk);
  }
  for (const pk of reservation.pitcherKeys || []) {
    if (sign > 0) _addToIndex(pendingPitcherRiskByKey, pk.key, pk.risk);
    else _subFromIndex(pendingPitcherRiskByKey, pk.key, pk.risk);
  }
  for (const pk of reservation.playerKeys || []) {
    if (sign > 0) _addToIndex(pendingPlayerRiskByKey, pk.key, pk.risk);
    else _subFromIndex(pendingPlayerRiskByKey, pk.key, pk.risk);
  }
}

function reservePending(parlayId, reservation) {
  if (!reservation) return;
  // If parlayId already had a reservation, retire it first so indices
  // don't double-count (defensive — caller shouldn't re-reserve).
  if (pendingExposure[parlayId]) {
    _applyReservationToIndices(pendingExposure[parlayId], -1);
  }
  pendingExposure[parlayId] = reservation;
  _applyReservationToIndices(reservation, +1);
  // Opportunistic cleanup of expired reservations — also updates indices.
  const now = Date.now();
  for (const [pid, res] of Object.entries(pendingExposure)) {
    if (res.expiresAt < now) {
      _applyReservationToIndices(res, -1);
      delete pendingExposure[pid];
    }
  }
}

function releasePending(parlayId) {
  const res = pendingExposure[parlayId];
  if (res) _applyReservationToIndices(res, -1);
  delete pendingExposure[parlayId];
}

/**
 * Sum of all in-flight (non-expired) pending risk for a given team+event key.
 * Called by checkExposureLimits to include pending reservations in the total.
 * O(1) via reverse index. Expired entries are pruned lazily on the next
 * reservePending call — small over-count window is acceptable (next quote
 * triggers cleanup; expiry windows are typically 60-120s).
 */
function getPendingTeamRisk(teamEventKey) {
  return pendingTeamRiskByKey.get(teamEventKey) || 0;
}

function getPendingGameRisk(gameKey) {
  return pendingGameRiskByKey.get(gameKey) || 0;
}

/**
 * Sum of in-flight pending risk against a pitcher key. Used by
 * checkPitcherExposure to close the quote-time race window. Same lazy-
 * expiry contract as the team/game variants.
 */
function getPendingPitcherRisk(pitcherKey) {
  return pendingPitcherRiskByKey.get(pitcherKey) || 0;
}

/**
 * Sum of in-flight pending risk against a player key. Generic player
 * version of getPendingPitcherRisk; used by checkPlayerExposure to
 * close the quote-time race window for the new Phase-2 prop types.
 */
function getPendingPlayerRisk(playerKey) {
  return pendingPlayerRiskByKey.get(playerKey) || 0;
}

function recordQuote(parlayId, legs, offeredOdds, maxRisk, fairParlayProb, meta) {
  stats.totalQuotes++;
  stats.sessionQuotes++;
  // Session-accurate submission count for fill-rate tracking. Must be
  // before the orders[] mutation so double-quotes for the same parlayId
  // (shouldn't happen but just in case) are each counted once.
  recordFillBucketSubmission(legs);

  orders[parlayId] = {
    parlayId,
    legs,
    offeredOdds,
    maxRisk,
    fairParlayProb,
    meta,
    status: 'quoted',
    quotedAt: new Date().toISOString(),
    confirmedAt: null,
    confirmedOdds: null,
    confirmedStake: null,
    orderUuid: null,
    settledAt: null,
    settlementResult: null,
    pnl: null,
  };

  // Downgraded from info to debug — fires on every RFQ we quote, which is
  // high-volume. On Railway, synchronous stdout writes under back-pressure can
  // add 1-5ms each. Keep the audit trail in the "Offered" log in websocket.js.
  log.debug('Orders', `Quote #${stats.totalQuotes}: parlay=${parlayId}, legs=${legs.length}, odds=${offeredOdds}, fair=${fairParlayProb.toFixed(5)}`);
  db.saveOrder(orders[parlayId]).catch(() => {});
  return orders[parlayId];
}

/**
 * Attach end-to-end submit latency + per-stage timing to an existing quote.
 * Called from websocket.js after submitOffer returns. Persists to Supabase so
 * the data survives service restarts and can be joined with matched-outcome
 * data later for a real latency × win-rate analysis.
 */
function updateOrderLatency(parlayId, submitLatencyMs, stageTimings) {
  const order = orders[parlayId];
  if (!order) return; // already cleaned up or never stored
  if (!order.meta) order.meta = {};
  order.meta.submitLatencyMs = submitLatencyMs;
  if (stageTimings && typeof stageTimings === 'object') {
    order.meta.stageTimings = { ...stageTimings };
  }
  db.saveOrder(order).catch(() => {});
}

/**
 * Return the most recent N orders that carry persisted submit-latency data,
 * shaped for the websocket responseTimes buffer. Used at boot to seed the
 * Latency Monitor so its data survives restarts/redeploys.
 */
function getRecentLatencyRecords(limit = 500) {
  const records = [];
  for (const o of Object.values(orders)) {
    const elapsed = o.meta && o.meta.submitLatencyMs;
    if (elapsed == null) continue;
    const time = o.quotedAt || o.confirmedAt || (o.meta && o.meta.quotedAt) || null;
    if (!time) continue;
    records.push({
      parlayId: o.parlayId,
      elapsed,
      offeredOdds: o.offeredOdds || null,
      time,
      stages: (o.meta && o.meta.stageTimings) || null,
    });
  }
  // Newest first, then trim to limit
  records.sort((a, b) => (b.time || '').localeCompare(a.time || ''));
  return records.slice(0, limit);
}

function recordConfirmation(parlayId, orderUuid, confirmedOdds, confirmedStake) {
  const order = orders[parlayId];
  if (order) {
    // Never revert a settled order to confirmed (duplicate/late event guard)
    if (order.status && order.status.startsWith('settled_')) {
      log.debug('Orders', `Ignoring late confirmation for already-settled parlay ${parlayId}`);
      if (orderUuid && !ordersByUuid[orderUuid]) ordersByUuid[orderUuid] = parlayId;
      return order;
    }

    // Count as a real fill only when an orderUuid arrives — i.e., when
    // order.finalized fires, not on the tentative order.matched. PX's
    // event model emits order.matched when the bettor initially selects
    // our quote, but the bettor still has a final-terms review step
    // where they can back out. Only order.finalized (which carries
    // orderUuid) indicates the bettor actually committed.
    //
    // Previous logic incremented on any status change to 'confirmed',
    // which inflated sessionFills with bettor-backout "phantoms"
    // (~76% of matched events on 2026-04-22). Status still promotes to
    // 'confirmed' on order.matched for exposure tracking during the
    // review window — the 10-min isStalePhantom sweep cleans up orphans.
    const wasCountedAsFill = order.orderUuid != null;
    if (!wasCountedAsFill && orderUuid != null) {
      stats.totalConfirmations++;
      stats.sessionFills++;
    }

    order.status = 'confirmed';
    order.confirmedAt = new Date().toISOString();
    order.confirmedOdds = confirmedOdds;
    order.confirmedStake = confirmedStake;
    order.orderUuid = orderUuid;

    // Compute Expected Value at confirmation time (SP perspective):
    //   EV = P(bettor_loses) * bettor_wager   - P(bettor_wins) * confirmedStake
    //      = (1 - fairParlayProb) * bettorWager - fairParlayProb * confirmedStake
    // Positive EV = we expect to profit on this parlay over many repetitions.
    // Negative EV = we expect to lose money on it.
    // This is stored at confirm time (freezes fair prob and stake together).
    if (order.fairParlayProb != null && order.fairParlayProb > 0 && order.fairParlayProb < 1 && confirmedOdds != null && confirmedStake != null) {
      const bettorWager = americanOddsToProfit(confirmedOdds, confirmedStake);
      const fair = order.fairParlayProb;
      order.expectedValue = (1 - fair) * bettorWager - fair * confirmedStake;
      if (order.meta) order.meta.expectedValue = order.expectedValue;
    }

    if (orderUuid) {
      ordersByUuid[orderUuid] = parlayId;
    }

    // Track exposure per team/selection — ONLY on real fills (orderUuid
    // first arrival). Previously addExposure fired on order.matched too,
    // which inflated Team/Game Exposure with tentative matches the
    // bettor later walked away from. Surfaced as a visible bug: the
    // Team Exposure table would show a team with $1,465 net exposure
    // across 4 parlays, but clicking the drill-down found "No matching
    // confirmed orders" because the filter there requires real fills.
    // Ties exposure accounting to the same commit signal as sessionFills.
    if (!wasCountedAsFill && orderUuid != null) {
      addExposure(order);
      // Template-exposure dimension: hash legs into a canonical signature
      // and record this confirmed bet against the rolling window. Feeds
      // the pricer's template ramp on subsequent RFQs with the same
      // signature. See services/template-exposure.js.
      try {
        const legs = order.meta?.legs || order.legs || [];
        templateExposure.recordConfirmation(legs, parlayId, confirmedStake, order.confirmedAt);
      } catch (err) {
        log.warn('TemplateExposure', `recordConfirmation failed: ${err.message}`);
      }
    }
    releasePending(parlayId);

    log.info('Orders', `Confirmed: parlay=${parlayId}, order=${orderUuid}, odds=${confirmedOdds}, stake=$${confirmedStake}`);
    db.saveOrder(order).catch(err => log.error('DB', `saveOrder(confirmation) failed: ${err.message}`));
  } else {
    log.warn('Orders', `Confirmation for unknown parlay ${parlayId}`);
  }
  return order;
}

/**
 * Ghost-sweep. Walks confirmed-locally orders whose games started
 * long enough ago that settlement MUST have happened on PX by now,
 * reconciles each against PX, and either:
 *   - Settles it (if PX has a matching settlement)
 *   - Marks it phantom (if PX has no record — we can't verify and
 *     we're tired of these corrupting stats)
 *
 * Why this is separate from fullPxReconcile: reconcile only processes
 * orders PX RETURNS in its feed. Our 953 ghost orders from 2026-04-17
 * onward aren't all being reached — PX's order endpoint caps at ~3000
 * records and the oldest ghosts fall off the tail. This sweep takes
 * the LOCAL set as the ground truth for "candidates that should be
 * closed," then tries to match into whatever PX returns. Anything
 * unmatched after 24h+ past game time is flagged phantom and hidden.
 *
 * opts: { olderThanHours = 24, commit = false, pxOrders = [] }
 * Returns: { candidates, settled, phantomed, stillOpen, ambiguous }
 */
async function sweepGhostOrders(opts = {}) {
  const olderThanHours = opts.olderThanHours != null ? opts.olderThanHours : 24;
  const commit = !!opts.commit;
  const pxOrders = opts.pxOrders || [];
  const cutoff = Date.now() - olderThanHours * 3600 * 1000;

  // Build PX lookup maps
  const pxByUuid = {};
  const pxByParlayId = {};
  for (const p of pxOrders) {
    if (p.order_uuid) pxByUuid[p.order_uuid] = p;
    const pid = p.p_id || p.parlay_id;
    if (pid) pxByParlayId[pid] = p;
  }

  // Identify candidates: confirmed locally, earliest leg started before cutoff
  const candidates = [];
  for (const order of Object.values(orders)) {
    if (order.status !== 'confirmed') continue;
    // Skip already-phantom — they're not polluting stats anymore.
    if (order.meta?.phantom) continue;
    const legs = order.legs || (order.meta && order.meta.legs) || [];
    let earliest = null;
    for (const l of legs) {
      const st = l.startTime || l.start_time;
      if (!st) continue;
      const t = new Date(st).getTime();
      if (Number.isFinite(t) && (earliest == null || t < earliest)) earliest = t;
    }
    if (earliest == null || earliest > cutoff) continue;
    candidates.push(order);
  }

  const result = {
    candidates: candidates.length,
    settled: 0,
    phantomed: 0,
    stillOpenOnPx: 0,
    actions: [], // sample of what would/did happen
  };

  for (const order of candidates) {
    const pid = order.parlayId;
    const uuid = order.orderUuid;
    const pxOrder = (uuid && pxByUuid[uuid]) || (pid && pxByParlayId[pid]) || null;
    let action;

    if (pxOrder) {
      const pxStatus = pxOrder.settlement_status;
      if (pxStatus === 'tbd' || pxStatus === 'requested' || !pxStatus) {
        // PX still has it open. Unusual for something >24h past game time,
        // but defer to PX — don't force-settle.
        result.stillOpenOnPx++;
        action = { parlayId: pid, action: 'left_open_per_px' };
      } else {
        // PX says won/lost/push. Settle locally.
        if (commit) {
          try {
            recordSettlement(pxOrder.order_uuid || uuid, pxStatus, Number(pxOrder.profit || 0));
            result.settled++;
            action = { parlayId: pid, action: 'settled', pxStatus, profit: pxOrder.profit };
          } catch (err) {
            action = { parlayId: pid, action: 'settle_failed', error: err.message };
            log.warn('GhostSweep', `Settle failed for ${pid}: ${err.message}`);
          }
        } else {
          result.settled++;
          action = { parlayId: pid, action: 'would_settle', pxStatus, profit: pxOrder.profit };
        }
      }
    } else {
      // PX has no record. Mark phantom so the order stops polluting stats/exposure.
      if (commit) {
        if (!order.meta) order.meta = {};
        order.meta.phantom = true;
        order.meta.phantomReason = `ghost-sweep: no PX match, game started ${olderThanHours}h+ ago`;
        order.meta.phantomFlaggedAt = new Date().toISOString();
        // Release any residual exposure
        try { releasePending(pid); } catch (_) {}
        db.saveOrder(order).catch(err => log.warn('GhostSweep', `saveOrder failed for ${pid}: ${err.message}`));
        result.phantomed++;
        action = { parlayId: pid, action: 'phantomed' };
      } else {
        result.phantomed++;
        action = { parlayId: pid, action: 'would_phantom' };
      }
    }

    if (result.actions.length < 20) result.actions.push(action);
  }

  // After settlements, rebuild exposure so team/game totals reflect
  // the now-closed bets. Only if we actually did work.
  if (commit && (result.settled > 0 || result.phantomed > 0)) {
    try { rebuildAllExposure(); } catch (err) { log.warn('GhostSweep', `rebuildAllExposure failed: ${err.message}`); }
  }

  return result;
}

/**
 * Mark an order as "accept-unknown" — the accept POST to PX errored
 * ambiguously (timeout, 5xx, 400 with unclear body). Does NOT flip
 * status to rejected and does NOT run addExposure. Caller is expected
 * to follow up with a PX REST verification (see verifyAcceptUnknown
 * in websocket.js) to determine ground truth.
 *
 * Stores enough context on the order that, even if the service
 * restarts before verification runs, an operator can see the
 * unresolved state via /orders and manually resolve via
 * /px-status-repair.
 */
function markAcceptUnknown(parlayId, orderUuid, confirmedOdds, confirmedStake, errMsg) {
  const order = orders[parlayId];
  if (!order) return { ok: false, reason: 'not-found' };
  if (!order.meta) order.meta = {};
  order.meta.acceptUnknown = true;
  order.meta.acceptUnknownAt = new Date().toISOString();
  order.meta.acceptUnknownError = errMsg || 'unknown';
  order.meta.acceptUnknownUuid = orderUuid || null;
  order.meta.acceptUnknownOdds = confirmedOdds != null ? +confirmedOdds : null;
  order.meta.acceptUnknownStake = confirmedStake != null ? +confirmedStake : null;
  db.saveOrder(order).catch(err => log.warn('AcceptUnknown', `saveOrder failed for ${parlayId}: ${err.message}`));
  return { ok: true };
}

/**
 * Import a PX-booked order that our local state has as rejected (or
 * has no status for). Used by the /px-status-repair endpoint to patch
 * the accept-POST-failed drift: PX booked the bet, we marked it
 * rejected, our Team Exposure under-reports the true position.
 *
 * Non-destructive:
 *   - Only flips rejected/missing → confirmed (never confirmed → anything)
 *   - Idempotent (safe to re-run; no-ops on already-confirmed orders)
 *   - Rebuilds exposure for the order so Team/Game tables catch up
 *
 * Caller: POST /px-status-repair (see index.js). Not on the hot path;
 * safe to run during live traffic (reads + writes are per-parlay).
 */
function importPxBookedOrder(parlayId, orderUuid, confirmedStake, confirmedOdds) {
  const order = orders[parlayId];
  if (!order) return { ok: false, reason: 'not-found' };
  if (order.status === 'confirmed') return { ok: false, reason: 'already-confirmed' };
  if ((order.status || '').startsWith('settled_')) return { ok: false, reason: 'already-settled' };

  const prevStatus = order.status;
  order.status = 'confirmed';
  order.confirmedAt = order.confirmedAt || new Date().toISOString();
  if (orderUuid) order.orderUuid = orderUuid;
  if (confirmedStake != null && Number.isFinite(+confirmedStake)) order.confirmedStake = +confirmedStake;
  if (confirmedOdds != null && Number.isFinite(+confirmedOdds)) order.confirmedOdds = +confirmedOdds;

  // Clear rejection metadata so the startup self-heal at loadFromDb()
  // doesn't demote this order back to rejected on the next restart.
  // The self-heal triggers on rejectedAt being set with status !== 'rejected',
  // which would otherwise undo every successful drift recovery on redeploy.
  delete order.rejectedAt;
  delete order.rejectionReason;

  // Stats: count this as a confirmation, back off the prior rejection.
  stats.totalConfirmations++;
  if (prevStatus === 'rejected' && stats.totalRejections > 0) stats.totalRejections--;

  // Uuid secondary index.
  if (orderUuid) ordersByUuid[orderUuid] = parlayId;

  // Release any pending reservation left over from when we'd quoted it.
  try { releasePending(parlayId); } catch (_) { /* best-effort */ }

  // Team/game exposure. Wrap in try so a single bad order doesn't
  // break the repair loop — we'd rather have the status flipped and
  // miss exposure on one row than fail the entire repair.
  try { addExposure(order); } catch (err) {
    log.warn('Repair', `addExposure failed for ${parlayId}: ${err.message}`);
  }

  // Persist. Fire-and-forget; failures are logged but don't block.
  db.saveOrder(order).catch(err => log.warn('Repair', `saveOrder failed for ${parlayId}: ${err.message}`));

  log.info('Repair', `Promoted ${parlayId} ${prevStatus || '(no status)'} → confirmed (uuid=${orderUuid}, stake=$${confirmedStake}, odds=${confirmedOdds})`);
  return { ok: true, parlayId, previousStatus: prevStatus };
}

// Classify a raw rejection reason string into a short, human-readable
// category so the dashboard banner doesn't dump URLs and JSON into the
// UI. Falls back to the leading clause of the reason when no pattern
// matches. The RAW reason stays in rejectStats.recent[].reason for
// debugging; only the `bucket` field gets normalized here.
function classifyRejectionReason(reason) {
  const r = (reason || '').trim();
  if (!r) return 'unknown';
  // PX API errors bubbled up from the confirm/accept HTTP call
  if (r.startsWith('accept-POST-failed:') || r.includes('ProphetX API')) {
    const errM = r.match(/"error"\s*:\s*"([^"]+)"/);
    if (errM) return `PX confirm rejected (${errM[1]})`;
    const statusM = r.match(/ProphetX API (\d+)/);
    if (statusM) return `PX confirm rejected (HTTP ${statusM[1]})`;
    return 'PX confirm rejected';
  }
  // Local exposure / risk limits
  if (/team exposure/i.test(r)) return 'team exposure limit';
  if (/game exposure/i.test(r)) return 'game exposure limit';
  if (/portfolio (risk|drawdown)/i.test(r)) return 'portfolio drawdown limit';
  if (/per-parlay risk|risk \$[\d,.]+ > max/i.test(r)) return 'per-parlay risk limit';
  // Generic: take the clause before the first colon (dropping any
  // dollar values), capped at 60 chars
  const before = r.split(':')[0].replace(/\$[\d,.]+/g, '$').trim();
  if (before.length > 0 && before.length <= 60) return before;
  return before.substring(0, 60) + '…';
}

function recordRejection(parlayId, reason) {
  // Guard against clobbering a confirmed/settled order. The verify-accept
  // retry chain and other late-arriving rejection signals can fire AFTER
  // a parallel order.matched / order.finalized event has already promoted
  // the order to confirmed (or PX has already settled it). Without this
  // check, the late rejection overwrites the authoritative status and the
  // parlay shows as "Rejected" in All Quotes despite being live in Open
  // Positions. Mirrors the late-event guard in recordConfirmation.
  const order = orders[parlayId];
  if (order && (order.status === 'confirmed' || (order.status || '').startsWith('settled_'))) {
    log.debug('Orders', `Skipping late rejection for ${parlayId} — already ${order.status} (reason ignored: ${reason})`);
    return order;
  }

  stats.totalRejections++;
  rejectStats.total++;

  const bucket = classifyRejectionReason(reason);
  rejectStats.reasons[bucket] = (rejectStats.reasons[bucket] || 0) + 1;
  rejectStats.recent.unshift({
    reason,
    bucket,
    parlayId,
    time: new Date().toISOString(),
  });
  if (rejectStats.recent.length > 100) rejectStats.recent.pop();

  if (order) {
    order.status = 'rejected';
    order.rejectedAt = new Date().toISOString();
    order.rejectionReason = reason;
    log.info('Orders', `Rejected: parlay=${parlayId}, reason=${reason}`);
  }
  // Release any pending reservation — rejection means we won't take this risk
  releasePending(parlayId);
  // Also release the template-exposure pending lane so this RFQ stops
  // counting toward future ramp decisions on the same signature.
  // Confirmation path (recordConfirmation → templateExposure.recordConfirmation)
  // already removes pending on graduation; this covers the reject path.
  try { templateExposure.releasePending(parlayId); } catch (_) { /* best-effort */ }
  return order;
}

/**
 * Record order finalization — this is where we get the order_uuid.
 * The price.confirm.new event doesn't include it; order.finalized does.
 */
function recordFinalized(parlayId, orderUuid, payload) {
  const order = orders[parlayId];
  if (!order) {
    log.warn('Orders', `Finalized event for unknown parlay ${parlayId}`);
    return null;
  }

  order.orderUuid = orderUuid;
  ordersByUuid[orderUuid] = parlayId;

  // Also capture confirmed values from finalized event if we missed them
  if (payload) {
    if (payload.confirmed_stake && !order.confirmedStake) order.confirmedStake = payload.confirmed_stake;
    if (payload.confirmed_odds && !order.confirmedOdds) order.confirmedOdds = payload.confirmed_odds;
  }

  log.info('Orders', `Finalized: parlay=${parlayId}, order=${orderUuid}`);
  db.saveOrder(order).catch(() => {});
  return order;
}

/**
 * Record individual leg settlement.
 */
function recordLegSettlement(orderUuid, legPayload) {
  const parlayId = ordersByUuid[orderUuid];
  const order = parlayId ? orders[parlayId] : null;
  if (!order) return;

  const lineId = legPayload.line_id || legPayload.lineId;
  const status = legPayload.settlement_status || legPayload.status;
  if (!lineId || !status) return;

  // Update ALL matching legs in BOTH sources (o.legs and o.meta.legs) —
  // previously we broke after finding the first match in order.legs, leaving
  // stale settlementStatus on meta.legs.
  //
  // IMPORTANT: Do NOT overwrite inferredResult with PX's settlement_status.
  // PX leg settlement_status is bettor-perspective and can be premature or
  // wrong for alt-line legs (e.g., PX said "won" for Over 4.5 when the game
  // total was 1). Our scraper's inferredResult uses actual game scores and
  // is more reliable. Keep both fields so the dashboard can compare.
  const sources = [order.legs, order.meta?.legs].filter(Boolean);
  for (const src of sources) {
    for (const leg of src) {
      if (leg.lineId === lineId || leg.line_id === lineId) {
        leg.settlementStatus = status;
        leg.settlement_status = status;
        // Only set inferredResult from PX if scraper hasn't resolved this leg yet
        if (!leg.inferredResult) {
          leg.inferredResult = status;
        }
      }
    }
  }

  db.saveOrder(order).catch(() => {});
}

/**
 * Convert American odds to profit on a given stake.
 * Positive (+500): profit = stake * odds / 100
 * Negative (-200): profit = stake * 100 / |odds|
 */
function americanOddsToProfit(americanOdds, stake) {
  const odds = Number(americanOdds);
  if (!odds || !stake) return 0;
  if (odds >= 100) return stake * odds / 100;
  if (odds <= -100) return stake * 100 / Math.abs(odds);
  return 0;
}

function recordSettlement(orderUuid, result, payout, opts = {}) {
  const parlayId = ordersByUuid[orderUuid];
  const order = parlayId ? orders[parlayId] : null;

  if (order) {
    // Don't re-settle
    if (order.status && order.status.startsWith('settled_')) {
      log.debug('Orders', `Already settled: order=${orderUuid}`);
      return order;
    }

    // Reject bogus settlements where any leg hasn't finished. SP 'lost'
    // requires every leg to have played. SP 'won' can legitimately settle
    // early if a leg lost (the parlay is dead). When the caller doesn't
    // pass leg-level context, we can't distinguish the two — be strict for
    // 'lost' only and permissive for 'won' (callers that DO have PX leg
    // data should filter there, e.g. pollOrderSettlements does).
    //
    // Callers that have already verified the settlement via PX's leg-level
    // data (pollOrderSettlements) pass { trusted: true } to bypass this
    // defensive recheck, otherwise our 4-hour start-time heuristic would
    // silently re-reject settlements the upstream validator just approved.
    if (result === 'lost' && !opts.trusted) {
      const legs = order.legs || order.meta?.legs || [];
      const now = Date.now();
      const anyUnfinished = legs.some(l => {
        const st = l.startTime || l.start_time;
        if (!st) return false;
        const startMs = new Date(st).getTime();
        if (startMs > now) return true;
        if ((now - startMs) < 4 * 3600 * 1000) return true;
        return false;
      });
      if (anyUnfinished) {
        log.warn('Settle', `Rejecting bogus lost settlement for ${order.parlayId}: leg(s) not yet finished`);
        return order;
      }
    }

    stats.totalSettlements++;
    // Preserve the ORIGINAL settlement timestamp on re-settle. If a bug
    // somewhere triggers recordSettlement a second time on an already-
    // settled order (the guard at line 656 is bypassed via the
    // "Fixing stale settlement" revert path in pollOrderSettlements),
    // we don't want the timestamp to drift forward and make the parlay
    // reappear at the top of the Settled Positions list each poll cycle.
    if (!order.settledAt) order.settledAt = new Date().toISOString();
    order.settlementResult = result; // 'won', 'lost', 'push', 'void'

    // Calculate P&L from SP perspective (house side).
    // `result` is ALWAYS passed as SP-perspective by callers:
    //   'won'  = SP won (bettor's parlay missed ≥1 leg) → +bettor's wager
    //   'lost' = SP lost (all bettor legs hit) → -confirmedStake
    // Callers (WS handler, poll) normalize before calling this function.
    //   confirmedStake = SP's to-win amount = bettor's potential payout
    //   bettor's wager = americanOddsToProfit(confirmedOdds, confirmedStake)
    const bettorWager = americanOddsToProfit(order.confirmedOdds, order.confirmedStake);

    // If PX provided the actual profit/loss amount, prefer it over our
    // computed value. This correctly handles push-reduced parlays where a
    // pushed leg shrinks the payout (actual loss < full confirmedStake).
    // Stored on the order for later reference.
    const pxProfit = (payout != null && payout !== 0) ? Number(payout) : null;
    if (pxProfit != null) order.pxProfit = pxProfit;

    if (result === 'won') {
      // SP won — bettor's parlay lost, we keep their wager
      order.pnl = pxProfit != null && pxProfit > 0 ? pxProfit : bettorWager;
      stats.totalWins++;
    } else if (result === 'lost') {
      // SP lost — bettor's parlay won, we pay out the actual reduced payout.
      // Prefer PX's profit field (negative) when provided; fall back to -stake
      // for cases where PX didn't include profit.
      order.pnl = pxProfit != null && pxProfit < 0 ? pxProfit : -(order.confirmedStake || 0);
      stats.totalLosses++;
    } else if (result === 'push' || result === 'void') {
      order.pnl = 0;
    }

    if (order.pnl != null) {
      stats.runningPnL += order.pnl;
    }

    // CLV capture: for each leg, look up the closing line snapshot captured
    // when the event started. Compute the closing implied prob per leg and
    // the parlay-level closing implied prob. Store on the order so the
    // /clv-report endpoint can aggregate across settled parlays.
    try {
      const oddsFeed = require('./odds-feed');
      const legs = order.legs || order.meta?.legs || [];
      let closingParlayImplied = 1;
      let allLegsHaveClose = true;
      for (const leg of legs) {
        const snap = oddsFeed.getClosingLineSnapshot(
          leg.sport, leg.homeTeam, leg.awayTeam, leg.pxEventId
        );
        if (!snap) { allLegsHaveClose = false; continue; }
        let closeImpl = null;
        const mt = leg.market;
        const sel = leg.selection;
        if (mt === 'moneyline' && snap.markets?.h2h) {
          closeImpl = sel === 'home' ? snap.markets.h2h.home : snap.markets.h2h.away;
        } else if (mt === 'spread' && snap.markets?.spreads) {
          // Only count CLV when the leg's line matches the snapshot's primary
          if (leg.line != null && snap.markets.spreads.line != null
              && Math.abs(Math.abs(leg.line) - Math.abs(snap.markets.spreads.line)) < 0.01) {
            closeImpl = sel === 'home' ? snap.markets.spreads.home : snap.markets.spreads.away;
          }
        } else if (mt === 'total' && snap.markets?.totals) {
          if (leg.line != null && snap.markets.totals.line != null
              && Math.abs(leg.line - snap.markets.totals.line) < 0.01) {
            closeImpl = sel === 'over' ? snap.markets.totals.over : snap.markets.totals.under;
          }
        }
        if (closeImpl != null && closeImpl > 0 && closeImpl < 1) {
          leg.closingImpliedProb = Math.round(closeImpl * 10000) / 10000;
          closingParlayImplied *= closeImpl;
        } else {
          allLegsHaveClose = false;
        }
      }
      if (allLegsHaveClose && legs.length > 0) {
        order.closingImpliedProb = Math.round(closingParlayImplied * 100000) / 100000;
        // CLV from SP perspective:
        //   ourOfferedImplied - closingImplied
        //   > 0: we priced looser than close (bettor got positive CLV; bad for SP)
        //   < 0: we priced tighter than close (SP captured edge)
        const ourOffered = order.meta?.offeredImpliedProb;
        if (ourOffered != null) {
          order.clvDelta = Math.round((ourOffered - closingParlayImplied) * 100000) / 100000;
        }
      }
    } catch (err) {
      log.debug('CLV', `CLV capture failed for ${order.parlayId}: ${err.message}`);
    }

    // Release exposure for settled parlay
    removeExposure(order);

    order.status = `settled_${result}`;
    log.info('Orders', `Settled: order=${orderUuid}, result=${result}, pnl=$${order.pnl?.toFixed(2)}, running=$${stats.runningPnL.toFixed(2)}`);
    // Critical — log errors on settlement saves so we never silently lose a settled order
    db.saveOrder(order).catch(err => log.error('DB', `CRITICAL: saveOrder(settlement) failed for ${order.parlayId}: ${err.message}`));
    // Push settlement notification to mobile subscribers. Fire-and-forget;
    // failures here must never disrupt settlement bookkeeping.
    try {
      const push = require('./push');
      push.notifySettlement(order);
    } catch (pushErr) {
      log.debug('Push', `Settlement notification failed: ${pushErr.message}`);
    }
  } else {
    log.warn('Orders', `Settlement for unknown order ${orderUuid}`);
  }

  return order;
}

// ---------------------------------------------------------------------------
// MARKET INTELLIGENCE
// ---------------------------------------------------------------------------

/**
 * Record a matched parlay from the broadcast channel (any SP won it).
 * Compare against our quotes to build competitive intel.
 */
// Track which parlay_ids we've already recorded as "won" to avoid double-counting
// when PX broadcasts multiple fills for the same parlay (split across SPs).
const matchedWonIds = new Set();

function recordMatchedParlay(parlayId, matchedOdds, matchedStake, legs, lineManager) {
  const ourQuote = orders[parlayId] || null;
  const weQuoted = !!ourQuote;

  // If we already recorded a "won" for this parlay, this is another SP's fill
  // on the same parlay (PX splits across SPs). Record as market intel only.
  if (matchedWonIds.has(parlayId)) {
    // Don't double-count stats, just log for intel
    log.debug('Market', `Duplicate matched broadcast for won parlay ${parlayId} — other SP fill at odds=${matchedOdds}, stake=$${matchedStake}`);
    return { outcome: 'duplicate', parlayId };
  }

  marketStats.totalMatched++;

  // Lookup decline info for this parlay so we can flag the problematic leg(s)
  const declineInfo = declinesByParlayId[parlayId] || null;
  const unknownSet = new Set(declineInfo?.unknownLineIds || []);
  // Build a lineId → unknownCategory lookup so we can enrich unregistered
  // legs with marketName / propType / playerName captured at decline time.
  const unknownCatsByLineId = {};
  for (const uc of (declineInfo?.unknownCategories || [])) {
    if (uc && uc.lineId) unknownCatsByLineId[uc.lineId] = uc;
  }

  // Resolve leg info from line_ids
  const resolvedLegs = (legs || []).map(l => {
    const lineId = l.line_id || l.lineId;
    const info = lineManager ? lineManager.lookupLine(lineId) : null;
    let team = info?.teamName || 'Unknown';
    // For totals, include the game context
    if (info?.marketType === 'total' && info?.homeTeam && info?.awayTeam) {
      team = `${team} (${info.awayTeam} @ ${info.homeTeam})`;
    }
    const wasUnreg = unknownSet.has(lineId) || !info;
    const enriched = wasUnreg ? unknownCatsByLineId[lineId] : null;
    // For unregistered legs we know about, prefer the captured player
    // name (e.g. "Wendell Carter Jr.") over the literal "Unknown". Fall
    // back to the full PX market name when we couldn't extract a player.
    let displayTeam = team;
    let displayMarket = info?.marketType || '-';
    let displayLine = info?.line ?? l.line ?? null;
    let displaySport = info?.sport || 'unknown';
    if (enriched) {
      if (enriched.playerName) displayTeam = enriched.playerName;
      else if (enriched.marketName) displayTeam = enriched.marketName;
      // propType ('pra_combo', 'hitter_hits', 'pitcher_strikeouts', ...)
      // is a more useful market label than the bare 'player_prop'
      // category, but fall through to category if propType wasn't set.
      if (enriched.propType) displayMarket = enriched.propType;
      else if (enriched.category) displayMarket = enriched.category;
      if (enriched.line != null) displayLine = enriched.line;
      if (enriched.sport) displaySport = enriched.sport;
    }
    return {
      lineId,
      team: displayTeam,
      market: displayMarket,
      line: displayLine,
      sport: displaySport,
      // Surface the full PX market name as a tooltip for unregistered
      // legs so the operator can hover-read the literal book wording
      // without losing the cleaner playerName-first display.
      marketName: enriched?.marketName || null,
      propType: enriched?.propType || null,
      // Flag legs that blocked us from quoting
      wasUnregistered: wasUnreg,
    };
  });

  // Classification under PX's order.matched broadcast model (corrected
  // Apr 25, 2026):
  //   PX broadcasts order.matched to ALL SPs that quoted on the RFQ, not
  //   just the winner. Alec's earlier "private only" claim was wrong (or
  //   PX changed behavior). Smoking gun: operator screenshotted a parlay
  //   marked "Confirmed" on the dashboard that he had not accepted on PX.
  //   Audit on 1,302 confirmed-no-orderUuid orders: 1,259 (97%) had
  //   |confirmedOdds + offeredOdds| ≠ 0 — i.e. matched_odds was the
  //   WINNING SP's price, not ours.
  //
  // Detection: real wins have matched ≈ offered (both in PX's bettor-side
  // American-odds convention). Other-SP wins have matched ≠ offered.
  // Real wins ALSO flow through handleConfirm → recordConfirmation
  // independently — that's the canonical "we won" signal. This branch is
  // now an opportunistic backup for the rare case where order.matched
  // races ahead of price.confirm.new for our own win.
  let outcome;
  if (weQuoted) {
    const offered = ourQuote.offeredOdds;
    const ODDS_TOL = 5; // American odds — generous to absorb minor PX drift
    const isOurWin = (matchedOdds != null && offered != null &&
      Math.abs(matchedOdds - offered) <= ODDS_TOL);

    if (!isOurWin) {
      // Another SP won this RFQ at a different price. Don't promote our
      // quote, don't bump win counters, don't record as a fill. Just
      // store diagnostic metadata so the dashboard can show we were
      // beaten on this one.
      outcome = 'other_sp';
      marketStats.otherSpMatched = (marketStats.otherSpMatched || 0) + 1;
      if (ourQuote.status === 'quoted') {
        ourQuote.meta = ourQuote.meta || {};
        ourQuote.meta.matchedByOtherSp = {
          observedAt: new Date().toISOString(),
          matchedOdds, matchedStake,
          ourOfferedOdds: offered,
          oddsDelta: (matchedOdds != null && offered != null) ? (matchedOdds - offered) : null,
        };
        db.saveOrder(ourQuote).catch(err => log.error('DB', `saveOrder(matched-other-sp) failed: ${err.message}`));
      }
      // Skip the rest of the won-branch side effects (matched_parlays
      // entry is also skipped further down via the outcome === 'other_sp'
      // early-return).
    } else {
    outcome = 'won';
    matchedWonIds.add(parlayId);
    // Session-accurate fill count for fill-rate tracking. Uses the legs
    // from our original quote so sport/leg-count match the submission entry.
    recordFillBucketFill(ourQuote.legs || []);
    // weQuoted here is a misnamed legacy counter — it's really "matched
    // events we received that we won". True quote-submission count lives
    // in rfqStages.submitted on the websocket side. Keep incrementing for
    // back-compat but only on real wins (post Apr 25 false-fill fix).
    marketStats.weWon++;
    marketStats.weQuoted++;
    // Promote quoted → confirmed only. Never promote from 'rejected' —
    // that would silently override our own limit checks. Previously the
    // check was `status !== 'confirmed'`, which treated 'rejected' as
    // "promotable" and flipped any rejected parlay to confirmed when
    // PX matched it despite our reject. Observed: \$7,410 stake parlay
    // correctly rejected by our max_risk check (max \$2,500), then PX
    // ignored the reject and broadcast order.matched, and this branch
    // resurrected the parlay as confirmed with the over-limit stake.
    //
    // PX ignoring our reject is a PX-side bug (known; Alec aware). Our
    // job is to keep our local accounting honest: once we've rejected,
    // we stay rejected. Log the mismatch so it's visible but do not
    // override.
    if (ourQuote.status === 'quoted') {
      // Exposure check — handleConfirm has team/series/per-parlay gates,
      // but order.matched arrives independently and historically bypassed
      // all of them. Observed 2026-04-24: 5 SAS bets booked totaling
      // $1,243 while SAS exposure already sat at $4,980 against a $3,000
      // team cap.
      //
      // Policy (Option B — soft accept + alert): PROMOTE anyway so our
      // local accounting matches what PX has on the books (hard-rejecting
      // here creates the exact drift class we just spent all day cleaning
      // up). But flag the override loudly via meta.exposureOverrideOnMatch
      // so the operator sees the violation, and record an exposure
      // rejection in the stats bucket so /alerts surfaces it.
      let exposureOverride = null;
      try {
        const cfg = require('../config').config;
        const legsForCheck = (ourQuote.legs || ourQuote.meta?.legs || []).map(l => ({
          ...l, lineInfo: l, team: l.team || l.teamName, fairProb: l.fairProb,
        }));
        const spRisk = Number(matchedStake || ourQuote.maxRisk || 0);
        if (spRisk > 0 && legsForCheck.length > 0) {
          // Per-parlay cap
          const parlayHasSeries = legsForCheck.some(l =>
            typeof (l.market || l.marketType) === 'string' &&
            (l.market || l.marketType).startsWith('series_')
          );
          const perParlayCap = parlayHasSeries
            ? (cfg.pricing.maxSeriesRiskPerParlay || cfg.pricing.maxRiskPerParlay)
            : cfg.pricing.maxRiskPerParlay;
          if (perParlayCap > 0 && spRisk > perParlayCap) {
            exposureOverride = { reason: 'per-parlay cap', detail: `risk $${spRisk} > cap $${perParlayCap}`, violations: [] };
          }
          // Team cap
          if (!exposureOverride) {
            const teamCheck = checkExposureLimits(legsForCheck, spRisk, cfg.pricing.maxExposurePerTeam);
            if (!teamCheck.allowed) {
              exposureOverride = { reason: teamCheck.reason || 'team exposure limit', detail: teamCheck.reason, violations: teamCheck.violations || [] };
            }
          }
          // Series gross exposure
          if (!exposureOverride && parlayHasSeries) {
            const seriesCheck = checkSeriesExposure(legsForCheck, spRisk, cfg.pricing.maxSeriesGrossExposure);
            if (!seriesCheck.allowed) {
              exposureOverride = { reason: seriesCheck.reason || 'series exposure limit', detail: seriesCheck.reason, violations: [{ team: 'series-event', wouldBe: seriesCheck.wouldBe, limit: seriesCheck.limit }] };
            }
          }
        }
      } catch (err) {
        log.warn('Orders', `order.matched exposure-check threw: ${err.message} — proceeding with promotion`);
      }

      ourQuote.status = 'confirmed';
      if (matchedOdds != null) {
        // PX sends matched_odds in bettor-side convention; our format is
        // SP-side (negated). Store the confirmed price in our format.
        ourQuote.confirmedOdds = -matchedOdds;
      } else {
        // offeredOdds is stored in BETTOR-side convention (decimalToAmerican
        // of the parlay's bettor decimal; positive for longshots). Our
        // confirmedOdds contract is SP-side, so negate when falling back.
        // Previously we stored offeredOdds directly, which meant every
        // affected parlay had confirmedOdds with the wrong sign — and
        // americanOddsToProfit(confirmedOdds, confirmedStake) used to
        // derive bettor-wager flipped from its true value, inflating the
        // "Stakes Held" column in Game/Team Exposure by a factor of
        // roughly odds/100. Observed in production as $62k Stakes Held
        // on games where actual bettor wagers summed to under $5k.
        ourQuote.confirmedOdds = ourQuote.offeredOdds != null ? -ourQuote.offeredOdds : null;
      }
      if (matchedStake != null) ourQuote.confirmedStake = matchedStake;
      ourQuote.confirmedAt = ourQuote.confirmedAt || new Date().toISOString();

      if (exposureOverride) {
        ourQuote.meta = ourQuote.meta || {};
        ourQuote.meta.exposureOverrideOnMatch = {
          reason: exposureOverride.reason,
          detail: exposureOverride.detail,
          violations: exposureOverride.violations,
          flaggedAt: new Date().toISOString(),
        };
        log.warn('Orders', `[EXPOSURE OVERRIDE] order.matched forced confirm past cap for ${parlayId} — ${exposureOverride.reason}: ${exposureOverride.detail}. PX has booked; local promotion proceeded to preserve state accuracy. Reduce caps or rebalance manually.`);
        try {
          recordExposureRejection(parlayId, Number(matchedStake || 0), exposureOverride.reason, exposureOverride.violations);
        } catch (_) { /* best-effort stats bump */ }
      }

      db.saveOrder(ourQuote).catch(err => log.error('DB', `saveOrder(won via matched) failed: ${err.message}`));
    } else if (ourQuote.status === 'rejected') {
      log.warn('Orders', `PX matched a parlay we rejected: ${parlayId} (reject reason=${ourQuote.rejectionReason || 'unknown'}, px stake=${matchedStake}, px odds=${matchedOdds}). Keeping status=rejected.`);
      ourQuote.meta = ourQuote.meta || {};
      ourQuote.meta.pxMatchedAfterReject = {
        matchedAt: new Date().toISOString(),
        matchedOdds, matchedStake,
        originalRejectReason: ourQuote.rejectionReason || null,
      };
      db.saveOrder(ourQuote).catch(() => {});
    }
    } // end isOurWin
  } else {
    // order.matched without a corresponding quote on our side — should not
    // happen under Alec's model but handle gracefully as "missed" so we
    // don't crash. Downstream consumers treat this as a market-intel entry
    // for a parlay we didn't participate in.
    outcome = 'missed';
    marketStats.missedNoQuote++;
  }

  const entry = {
    parlayId,
    matchedOdds,
    matchedStake,
    legs: resolvedLegs,
    matchedAt: new Date().toISOString(),
    weQuoted,
    ourOdds: ourQuote?.offeredOdds || null,
    ourAmericanOdds: ourQuote?.offeredOdds || null,
    ourDecimalOdds: ourQuote?.meta?.decimalOdds || null, // precise decimal for comparison
    // PX sends matched_odds with opposite sign to our format.
    matchedAmericanOdds: matchedOdds != null ? -matchedOdds : null,
    winDecimalOdds: matchedOdds != null ? (Math.abs(matchedOdds) >= 100 ? 1 + Math.abs(matchedOdds)/100 : null) : null,
    outcome,
    legCount: resolvedLegs.length,
    // If we didn't quote, include why (so the dashboard can explain "No quote")
    declineReason: outcome === 'missed' ? (declineInfo?.reason || 'not seen') : null,
    declineDetail: declineInfo?.declineDetail || null,
    unknownLegDetails: declineInfo?.unknownDetails || [],
  };

  // Don't store other_sp fills as separate matched entries
  if (outcome === 'other_sp') return entry;

  matchedParlays.unshift(entry); // newest first
  db.saveMatchedParlay(entry).catch(() => {});
  if (matchedParlays.length > 5000) matchedParlays.pop(); // cap memory

  // If decline reason is "not seen", try DB lookup to backfill the real reason
  if (entry.declineReason === 'not seen') {
    db.lookupDecline(parlayId).then(dbDecline => {
      if (dbDecline) {
        entry.declineReason = dbDecline.reason;
        entry.declineDetail = dbDecline.detail;
      }
    }).catch(() => {});
  }

  // Async backfill: when in-memory orders[] missed (very common after a
  // Railway restart wiped the map between our quote and order.matched
  // arriving), look up parlay_orders by parlay_id and populate ourOdds.
  // Without this, the dashboard's OUR PRICE column shows '-' on
  // every matched parlay we did quote on but the in-memory record had
  // already been evicted / never reloaded. Mutates the entry object in
  // place — matchedParlays array holds the same reference.
  if (!weQuoted && parlayId) {
    backfillOurOddsFromDb(entry).catch(err =>
      log.debug('Market', `backfill ourOdds failed for ${parlayId}: ${err.message}`)
    );
  }

  if (weQuoted && outcome === 'lost') {
    log.info('Market', `Lost quote: parlay=${parlayId.substring(0,8)}, our=${entry.ourAmericanOdds}, winning=${matchedOdds}, stake=$${matchedStake}`);
  }

  return entry;
}

/**
 * Look up our offeredOdds from parlay_orders for a matched parlay whose
 * in-memory orders[] entry was missing at recordMatchedParlay time. On
 * success: populates entry.ourOdds / ourAmericanOdds, flips weQuoted to
 * true, and demotes outcome from 'missed' → 'lost' (we DID quote, we
 * just lost the bid). Re-saves the updated record so future restarts
 * preserve the backfilled state. Idempotent — safe to call multiple
 * times on the same entry; only mutates if a new value is found.
 */
async function backfillOurOddsFromDb(entry) {
  if (!entry || !entry.parlayId) return;
  if (entry.ourAmericanOdds != null) return; // already populated
  if (entry._backfillAttempted) return; // don't repeat lookup
  entry._backfillAttempted = true;
  const map = await db.loadOrdersByParlayIds([entry.parlayId]);
  const row = map && map[entry.parlayId];
  if (!row || row.offeredOdds == null) return;
  entry.ourOdds = row.offeredOdds;
  entry.ourAmericanOdds = row.offeredOdds;
  entry.weQuoted = true;
  // 'missed' meant "we never quoted." Now we know we did quote — flip to
  // 'lost' so the dashboard renders the row as "Outbid" not "No Quote".
  if (entry.outcome === 'missed') entry.outcome = 'lost';
  // Persist the updated record so the next /market-intel fetch (or the
  // next restart's reload) carries the backfilled values.
  db.saveMatchedParlay(entry).catch(err =>
    log.debug('Market', `saveMatchedParlay (backfill) failed for ${entry.parlayId}: ${err.message}`)
  );
}

function decToAm(dec) {
  if (!dec || dec <= 1) return null;
  if (dec >= 2.0) return '+' + Math.round((dec - 1) * 100);
  return '' + Math.round(-100 / (dec - 1));
}

/**
 * Record a declined RFQ with reason.
 */
/**
 * Record a declined RFQ with detailed info.
 * @param {string} reason - 'unknown legs', 'no fair value', 'exposure/limit'
 * @param {object} detail - { legs, knownLegs, unknownLegs, parlayId }
 */
function recordDecline(reason, detail) {
  declineStats.total++;
  const bucket = reason || 'unknown';
  declineStats.reasons[bucket] = (declineStats.reasons[bucket] || 0) + 1;

  // Track potential volume by reason (leg count as proxy for parlay size)
  if (!declineStats.volumeByReason) declineStats.volumeByReason = {};
  if (!declineStats.volumeByReason[bucket]) declineStats.volumeByReason[bucket] = { count: 0, totalLegs: 0, byLegCount: {} };
  const legCount = (detail?.legs || detail?.knownLegs || []).length + (detail?.unknownLegs || []).length;
  declineStats.volumeByReason[bucket].count++;
  declineStats.volumeByReason[bucket].totalLegs += legCount;
  const lcKey = legCount || 'unknown';
  declineStats.volumeByReason[bucket].byLegCount[lcKey] = (declineStats.volumeByReason[bucket].byLegCount[lcKey] || 0) + 1;
  const declinedAt = new Date().toISOString();
  const isLimit = LIMIT_REASONS.has(bucket);

  // Rolling log (keep last 200, newest first) — used by the limit-alert banner
  declineStats.recent.unshift({
    reason: bucket,
    detail: detail?.declineDetail || null,
    parlayId: detail?.parlayId || null,
    time: declinedAt,
    isLimit,
  });
  if (declineStats.recent.length > 200) declineStats.recent.pop();

  // Full-event rolling log with timestamps for /decline-audit windowing.
  // Capture enough context per entry so time-filtered stats can be recomputed
  // on demand without re-summarizing all-session counters.
  declineStats.recentDeclineEvents.push({
    time: declinedAt,
    reason: bucket,
    parlayId: detail?.parlayId || null,
    legCount,
    knownLegs: (detail?.knownLegs || []).map(l => ({ sport: l.sport, market: l.market })),
    unknownCategories: detail?.unknownCategories || [],
  });
  if (declineStats.recentDeclineEvents.length > MAX_DECLINE_EVENTS) {
    declineStats.recentDeclineEvents.shift();
  }

  // Index by parlayId so matched-parlay "No quote" rows can explain which leg caused the miss
  if (detail?.parlayId) {
    declinesByParlayId[detail.parlayId] = {
      reason: bucket,
      unknownLineIds: detail.unknownLegs || [],
      unknownDetails: detail.unknownSports || [],
      // Full unknownCategories entries (with marketName, propType,
      // playerName, line, etc.) keyed by lineId for the matched-parlay
      // leg-row enrichment. Lets the dashboard render "Wendell Carter Jr.
      // (pra_combo, 9.5)" instead of "Unknown / - / -" when an unknown
      // leg blocks a quote.
      unknownCategories: detail.unknownCategories || [],
      declineDetail: detail.declineDetail || null,
      declinedAt,
    };
    declineIdOrder.push(detail.parlayId);
    while (declineIdOrder.length > MAX_DECLINE_ENTRIES) {
      const old = declineIdOrder.shift();
      delete declinesByParlayId[old];
    }
  }

  // Persist to Supabase (fire-and-forget)
  db.saveDecline({
    parlayId: detail?.parlayId || null,
    reason: bucket,
    detail: detail?.declineDetail || null,
    knownLegs: detail?.knownLegs || [],
    unknownLineIds: detail?.unknownLegs || [],
    unknownDetails: detail?.unknownSports || [],
    // Structured per-leg breakdown (sport, category, propType, playerName,
    // marketName, line, eventName) — was built in memory above but never
    // persisted before. Required by /unknown-legs-breakdown to aggregate
    // unknown-leg volume by sport and market category.
    unknownCategories: detail?.unknownCategories || [],
    isLimit,
    declinedAt,
  }).catch(() => {});

  // Track sports from unknown legs
  if (detail?.unknownSports) {
    for (const sport of detail.unknownSports) {
      if (!declineStats.unknownSports[sport]) {
        declineStats.unknownSports[sport] = { count: 0, lastSeen: null, recentDeclines: [] };
      }
      declineStats.unknownSports[sport].count++;
      declineStats.unknownSports[sport].lastSeen = new Date().toISOString();
      // Store recent decline detail for this event
      declineStats.unknownSports[sport].recentDeclines.unshift({
        parlayId: detail.parlayId,
        knownLegs: detail.knownLegs || [],
        unknownLegs: detail.unknownLegs || [],
        // Granular per-leg categorization (player_prop, alt_spread, team_total,
        // sub_game, etc.) for the legs we couldn't register. Lets the dashboard
        // render meaningful "why" info when knownLegs is empty.
        unknownCategories: detail.unknownCategories || [],
        time: new Date().toISOString(),
        legCount: (detail.legs || []).length,
      });
      if (declineStats.unknownSports[sport].recentDeclines.length > 5) {
        declineStats.unknownSports[sport].recentDeclines.pop();
      }
    }
  }

  // Track granular unknown leg categories
  if (detail?.unknownCategories) {
    for (const cat of detail.unknownCategories) {
      const c = cat.category || 'unknown';
      if (!declineStats.unknownLegCategories[c]) {
        declineStats.unknownLegCategories[c] = { count: 0, bySport: {}, byResolveReason: {}, byPropType: {}, sampleLegs: [] };
      }
      const bucket2 = declineStats.unknownLegCategories[c];
      bucket2.count++;
      const sp = cat.sport || 'unknown';
      bucket2.bySport[sp] = (bucket2.bySport[sp] || 0) + 1;
      if (cat.resolveReason) {
        bucket2.byResolveReason[cat.resolveReason] = (bucket2.byResolveReason[cat.resolveReason] || 0) + 1;
      }
      // Phase 0 prop-opportunity instrumentation (Apr 26): when an
      // unknown leg was classified as MLB player_prop in websocket.js,
      // it carries a propType sub-bucket ('pitcher_strikeouts',
      // 'hitter_total_bases', etc.). Roll those up here so
      // /prop-opportunity can report what % of player_prop volume is
      // pitcher Ks — the gating metric for whether to subscribe to a
      // paid feed and ship Phase 2.
      if (cat.propType) {
        bucket2.byPropType[cat.propType] = (bucket2.byPropType[cat.propType] || 0) + 1;
        if (!bucket2.byPropType._lastSeen) bucket2.byPropType._lastSeen = {};
        bucket2.byPropType._lastSeen[cat.propType] = new Date().toISOString();
      }
      if (bucket2.sampleLegs.length < 10) {
        bucket2.sampleLegs.push({
          eventName: cat.eventName,
          sport: sp,
          line: cat.line,
          origLine: cat.origLine,
          isKnownEvent: cat.isKnownEvent,
          resolveReason: cat.resolveReason,
          resolveDetail: cat.resolveDetail,
          propType: cat.propType || null,
          marketName: cat.marketName || null,
        });
      }
    }
  }

  // Track exposure-limit declines with structured data
  if (isLimit && detail?.violations) {
    exposureLimitStats.total++;
    exposureLimitStats.byReason[bucket] = (exposureLimitStats.byReason[bucket] || 0) + 1;
    // Use the max violation amount as the representative size
    const maxViolationAmount = Math.max(...detail.violations.map(v => v.wouldBe || 0), 0);
    const sizeBucket = getSizeBucket(maxViolationAmount);
    exposureLimitStats.bySizeBucket[sizeBucket] = (exposureLimitStats.bySizeBucket[sizeBucket] || 0) + 1;
    exposureLimitStats.recent.unshift({
      parlayId: detail.parlayId || null,
      reason: bucket,
      violations: detail.violations,
      estPayout: detail.estPayout || null,
      legCount,
      maxViolationAmount,
      sizeBucket,
      time: declinedAt,
      teams: detail.violations.map(v => v.team),
    });
    if (exposureLimitStats.recent.length > 200) exposureLimitStats.recent.pop();
  }

  // Track near-misses (all legs known but couldn't price)
  // Near-miss reasons include: 'no fair value', 'stale odds', 'parlay too unlikely', 'odds too high'
  // Phase 2 K-prop pricing/data issues are also Near Misses — we tried
  // to price but couldn't (low book coverage, stale cache, no consensus).
  // Intentional blocks (correlation, exposure caps) are NOT included
  // because we never wanted to quote them.
  const nearMissReasons = new Set([
    'no fair value', 'stale odds', 'parlay too unlikely', 'odds too high', 'event started',
    'prop_no_fair_value', 'prop_low_confidence', 'prop_stale',
  ]);
  if (nearMissReasons.has(reason) && detail) {
    declineStats.nearMisses.unshift({
      parlayId: detail.parlayId,
      legs: detail.knownLegs || [],
      time: new Date().toISOString(),
      reason,
      detail: detail.declineDetail || null,
    });
    if (declineStats.nearMisses.length > 500) declineStats.nearMisses.pop();
  }
}

/**
 * Split the "not seen" missed-volume bucket into sub-categories so the
 * operator can tell which kind of coverage gap is producing the volume.
 *
 * Categories (mutually exclusive, first match wins):
 *   1. had_unknown_legs       — at least one leg.wasUnregistered = true
 *                               (should have been classified as 'unknown
 *                               legs' but decline reason was lost)
 *   2. unsupported_sport      — at least one leg has a sport outside
 *                               config.supportedSports (or sport unknown)
 *   3. historical_pre_service — matchedAt before the current process
 *                               booted (prior session, downtime, or
 *                               pre-fix historical data)
 *   4. session_active_gap     — matchedAt within current session but we
 *                               still never saw it (real WS delivery gap
 *                               or sub-second startup window)
 *   5. other                  — unclassified
 *
 * Returns per-bucket totals plus a dailyByBucket map so the dashboard
 * can show "missed $X/day in category Y".
 */
function buildNotSeenBreakdown(missed) {
  let config;
  try { ({ config } = require('../config')); } catch { config = { supportedSports: [] }; }
  const supported = new Set((config.supportedSports || []).map(s => s.toLowerCase()));
  const sessionStart = stats.startedAt;

  const notSeen = missed.filter(m => !m.declineReason);
  const buckets = {
    had_unknown_legs:       { count: 0, totalStake: 0 },
    unsupported_sport:      { count: 0, totalStake: 0 },
    historical_pre_service: { count: 0, totalStake: 0 },
    session_active_gap:     { count: 0, totalStake: 0 },
    other:                  { count: 0, totalStake: 0 },
  };
  // dailyByBucket[bucket][YYYY-MM-DD] = { count, stake }
  const dailyByBucket = {};
  const ensureDay = (bucket, day) => {
    if (!dailyByBucket[bucket]) dailyByBucket[bucket] = {};
    if (!dailyByBucket[bucket][day]) dailyByBucket[bucket][day] = { count: 0, stake: 0 };
    return dailyByBucket[bucket][day];
  };

  for (const m of notSeen) {
    const legs = m.legs || [];
    const stake = m.matchedStake || 0;
    const day = (m.matchedAt || '').substring(0, 10) || 'unknown';

    let bucket;
    const hasUnregistered = legs.some(l => l.wasUnregistered);
    const legSports = legs.map(l => (l.sport || '').toLowerCase()).filter(Boolean);
    const hasUnsupportedSport = legSports.length > 0 && legSports.some(s => !supported.has(s) && s !== 'unknown');
    const anyUnknownSport = legSports.length === 0 || legSports.some(s => s === 'unknown' || s === '');

    if (hasUnregistered) {
      bucket = 'had_unknown_legs';
    } else if (hasUnsupportedSport || anyUnknownSport) {
      bucket = 'unsupported_sport';
    } else if (m.matchedAt && m.matchedAt < sessionStart) {
      bucket = 'historical_pre_service';
    } else if (m.matchedAt && m.matchedAt >= sessionStart) {
      bucket = 'session_active_gap';
    } else {
      bucket = 'other';
    }

    buckets[bucket].count++;
    buckets[bucket].totalStake += stake;
    const d = ensureDay(bucket, day);
    d.count++;
    d.stake += stake;
  }

  // Round and compute averages
  for (const k of Object.keys(buckets)) {
    const b = buckets[k];
    b.totalStake = Math.round(b.totalStake * 100) / 100;
    b.avgStake = b.count > 0 ? Math.round((b.totalStake / b.count) * 100) / 100 : 0;
  }
  for (const bucket of Object.keys(dailyByBucket)) {
    for (const day of Object.keys(dailyByBucket[bucket])) {
      dailyByBucket[bucket][day].stake = Math.round(dailyByBucket[bucket][day].stake * 100) / 100;
    }
  }

  // Also compute combined daily totals across all not-seen parlays
  // so the dashboard can show an overall daily missed-volume chart.
  const dailyTotals = {};
  for (const bucket of Object.keys(dailyByBucket)) {
    for (const [day, stats2] of Object.entries(dailyByBucket[bucket])) {
      if (!dailyTotals[day]) dailyTotals[day] = { count: 0, stake: 0 };
      dailyTotals[day].count += stats2.count;
      dailyTotals[day].stake += stats2.stake;
    }
  }
  for (const day of Object.keys(dailyTotals)) {
    dailyTotals[day].stake = Math.round(dailyTotals[day].stake * 100) / 100;
  }

  return {
    totalCount: notSeen.length,
    totalStake: Math.round(notSeen.reduce((s, m) => s + (m.matchedStake || 0), 0) * 100) / 100,
    buckets,
    dailyByBucket,
    dailyTotals,
    sessionStart,
  };
}

/**
 * Breakdown of RFQs we declined (and potentially matched parlays we missed)
 * because the request exceeded one of our risk thresholds. Powers the
 * Analytics tab "Missed Volume from Risk Limits" chart.
 *
 * Two data sources:
 *  - declineStats.recentDeclineEvents: every decline with timestamp + reason.
 *    Gives us the REQUEST count per day (all RFQs, not just ones that
 *    matched to another SP).
 *  - matchedParlays with declineReason in RISK_LIMIT_REASONS: gives us the
 *    actual STAKE amounts on the subset that another SP won. This is the
 *    real dollar volume we left on the table.
 *
 * Daily table is keyed by YYYY-MM-DD with counts + stake per reason.
 */
const RISK_LIMIT_REASONS = new Set([
  'team exposure limit',
  'game exposure limit',
  'portfolio drawdown limit',
  'odds too high',
]);

function buildRiskLimitMissedVolume() {
  // Init empty daily rollup
  const byDay = {}; // day → { totalCount, totalStake, byReason: { reason: { count, stake } } }
  const ensureDay = (day) => {
    if (!byDay[day]) byDay[day] = { totalCount: 0, totalStake: 0, byReason: {} };
    return byDay[day];
  };
  const ensureReason = (day, reason) => {
    const d = ensureDay(day);
    if (!d.byReason[reason]) d.byReason[reason] = { count: 0, stake: 0 };
    return d.byReason[reason];
  };

  // 1. Request counts — walk all decline events tagged with a risk reason
  for (const ev of declineStats.recentDeclineEvents || []) {
    if (!RISK_LIMIT_REASONS.has(ev.reason)) continue;
    const day = (ev.time || '').substring(0, 10);
    if (!day) continue;
    const d = ensureDay(day);
    d.totalCount++;
    const r = ensureReason(day, ev.reason);
    r.count++;
  }

  // 2. Real stake amounts — walk matched parlays where we declined for risk
  for (const m of matchedParlays) {
    if (m.weQuoted) continue;
    if (!RISK_LIMIT_REASONS.has(m.declineReason)) continue;
    const day = (m.matchedAt || '').substring(0, 10);
    if (!day) continue;
    const stake = m.matchedStake || 0;
    const d = ensureDay(day);
    d.totalStake += stake;
    const r = ensureReason(day, m.declineReason);
    r.stake += stake;
    // Note: the request count was already incremented from
    // recentDeclineEvents above, so we don't double-count here.
  }

  // Round stakes
  for (const day of Object.keys(byDay)) {
    byDay[day].totalStake = Math.round(byDay[day].totalStake * 100) / 100;
    for (const r of Object.keys(byDay[day].byReason)) {
      byDay[day].byReason[r].stake = Math.round(byDay[day].byReason[r].stake * 100) / 100;
    }
  }

  // Reason totals across all days
  const byReason = {};
  let grandCount = 0;
  let grandStake = 0;
  for (const day of Object.keys(byDay)) {
    grandCount += byDay[day].totalCount;
    grandStake += byDay[day].totalStake;
    for (const [reason, r] of Object.entries(byDay[day].byReason)) {
      if (!byReason[reason]) byReason[reason] = { count: 0, stake: 0 };
      byReason[reason].count += r.count;
      byReason[reason].stake += r.stake;
    }
  }
  for (const r of Object.keys(byReason)) {
    byReason[r].stake = Math.round(byReason[r].stake * 100) / 100;
  }

  return {
    byDay,
    byReason,
    grandTotal: { count: grandCount, stake: Math.round(grandStake * 100) / 100 },
    reasons: [...RISK_LIMIT_REASONS],
  };
}

function getMarketIntel(limit = 50) {
  // Lazy backfill: scan recent matched parlays for entries missing
  // ourAmericanOdds and trigger async DB lookup for them. Each entry
  // sets _backfillAttempted on first call so we don't re-query on every
  // /market-intel poll. Mutations land on the entries before the next
  // dashboard render. Bounded to the rendered window (limit) plus a
  // small lookahead so we don't scan the entire 5000-entry buffer.
  const backfillScan = matchedParlays.slice(0, Math.min(limit + 50, matchedParlays.length));
  for (const m of backfillScan) {
    if (m && m.ourAmericanOdds == null && m.parlayId && !m._backfillAttempted) {
      backfillOurOddsFromDb(m).catch(() => {});
    }
  }

  return {
    stats: { ...marketStats },
    // Session-scoped fill-rate counters keyed by rawSport|legCount.
    // Authoritative denominator for fill rate — orders map can't be used
    // because loadFromDb skips unfilled 'quoted' rows. Resets on boot;
    // sessionStartedAt tells the dashboard what window this covers.
    sessionFillBuckets: { ...sessionFillBuckets },
    // Pre-aggregated fill-rate buckets for each dashboard timeframe
    // selector. Computed from the rolling fillBucketEvents log so the
    // client doesn't need to ship/parse the raw events (can be hundreds
    // of thousands of entries). Shape mirrors sessionFillBuckets:
    //   { 'rawSport|legCount': { submitted, filled } }
    fillBucketsByWindow: (() => {
      pruneFillBucketEvents();
      const now = Date.now();
      const cutoffs = {
        '24h': now - 24 * 60 * 60 * 1000,
        '7d':  now - 7 * 24 * 60 * 60 * 1000,
        '30d': now - 30 * 24 * 60 * 60 * 1000,
      };
      const out = { '24h': {}, '7d': {}, '30d': {} };
      // Single backward pass — stop once we're past 30d (array is
      // chronological by insertion).
      for (let i = fillBucketEvents.length - 1; i >= 0; i--) {
        const ev = fillBucketEvents[i];
        if (ev.t < cutoffs['30d']) break;
        for (const label of ['30d', '7d', '24h']) {
          if (ev.t < cutoffs[label]) continue;
          const b = out[label];
          if (!b[ev.key]) b[ev.key] = { submitted: 0, filled: 0 };
          if (ev.kind === 'submit') b[ev.key].submitted++;
          else if (ev.kind === 'fill') b[ev.key].filled++;
        }
      }
      return out;
    })(),
    sessionStartedAt: stats.startedAt,
    declines: {
      total: declineStats.total,
      reasons: { ...declineStats.reasons },
      volumeByReason: declineStats.volumeByReason || {},
      unknownSports: { ...declineStats.unknownSports },
      unknownLegCategories: declineStats.unknownLegCategories || {},
      unsupportedMarkets: declineStats.unsupportedMarkets || {},
      nearMissCount: declineStats.nearMisses.length,
      recentNearMisses: declineStats.nearMisses.slice(0, 500),
      // Rolling event log for /decline-audit?window= time filtering.
      // Reference (not a copy) — consumers should treat as read-only.
      recentDeclineEvents: declineStats.recentDeclineEvents || [],
    },
    // Volume analysis: matched parlays we missed, with real stake data
    missedVolume: (() => {
      const missed = matchedParlays.filter(m => !m.weQuoted);
      const byReason = {};
      let totalStake = 0;
      for (const m of missed) {
        const reason = m.declineReason || 'not seen';
        if (!byReason[reason]) byReason[reason] = { count: 0, totalStake: 0, avgStake: 0 };
        byReason[reason].count++;
        byReason[reason].totalStake += (m.matchedStake || 0);
        totalStake += (m.matchedStake || 0);
      }
      for (const r of Object.keys(byReason)) {
        byReason[r].avgStake = byReason[r].count > 0 ? Math.round(byReason[r].totalStake * 100) / 100 : 0;
      }
      return {
        totalMissed: missed.length,
        totalStake: Math.round(totalStake * 100) / 100,
        byReason,
        notSeenBreakdown: buildNotSeenBreakdown(missed),
      };
    })(),
    // Risk-limit decline breakdown for the Analytics tab
    riskLimitMissed: buildRiskLimitMissedVolume(),
    recentMatched: matchedParlays.slice(0, limit),
    quoteWinRate: marketStats.weQuoted > 0 ? (marketStats.weWon / marketStats.weQuoted * 100).toFixed(1) + '%' : '-',
    coverageRate: marketStats.totalMatched > 0 ? (marketStats.weQuoted / marketStats.totalMatched * 100).toFixed(1) + '%' : '-',
    // Sport breakdown of matched parlays
    matchedBySport: (() => {
      const bySport = {};
      for (const m of matchedParlays) {
        const knownSports = [...new Set((m.legs || []).map(l => l.sport).filter(s => s && s !== 'unknown'))];
        let bucket;
        if (knownSports.length === 0) bucket = 'Unknown';
        else if (knownSports.length === 1) bucket = knownSports[0];
        else bucket = 'Multi-league';
        if (!bySport[bucket]) bySport[bucket] = { count: 0, weQuoted: 0, missed: 0, avgStake: 0, totalStake: 0 };
        bySport[bucket].count++;
        bySport[bucket].totalStake += (m.matchedStake || 0);
        if (m.weQuoted) bySport[bucket].weQuoted++;
        else bySport[bucket].missed++;
      }
      for (const s of Object.keys(bySport)) {
        bySport[s].avgStake = bySport[s].count > 0 ? bySport[s].totalStake / bySport[s].count : 0;
      }
      return bySport;
    })(),
    // Competitive/drift entries — every matched event is a WIN under Alec's
    // confirmed event model. The "gap" metric that used to compare our
    // odds to a competitor's is actually drift (originalOdds vs final
    // confirmed odds after any re-confirmation). We keep the field name
    // for UI compatibility but its meaning is narrower.
    //
    // Historical entries with outcome='lost' or outcome='other_sp' are
    // misclassified from the pre-fix era — exclude them from the live
    // view so fill-rate and drift numbers aren't polluted. A separate
    // DB migration can retroactively relabel those rows, but filtering
    // here means the operator sees clean numbers immediately.
    competitive: (() => {
      const quoted = matchedParlays.filter(m =>
        m.weQuoted
        && m.ourAmericanOdds != null
        && m.matchedAmericanOdds != null
        && m.outcome !== 'lost'  // legacy misclassification
        && m.outcome !== 'other_sp'  // ditto
      );
      if (quoted.length === 0) return { entries: [], summary: null };

      const entries = quoted.map(m => {
        const ourOdds = Number(m.ourAmericanOdds);
        const winOdds = Number(m.matchedAmericanOdds);
        const ourDecimal = m.ourDecimalOdds || (ourOdds >= 100 ? 1 + ourOdds/100 : ourOdds < -100 ? 1 + 100/Math.abs(ourOdds) : null);
        const winDecimal = m.winDecimalOdds || (winOdds >= 100 ? 1 + winOdds/100 : winOdds < -100 ? 1 + 100/Math.abs(winOdds) : null);
        const ourProb = americanToProb(ourOdds);
        const winProb = americanToProb(winOdds);
        // gapProb now represents DRIFT: positive = final confirmed price
        // was tighter than what we originally offered.
        const gapProb = ourProb - winProb;
        return {
          parlayId: m.parlayId,
          teams: (m.legs || []).map(l => l.team).filter(t => t !== 'Unknown').join(', '),
          legs: m.legs || [],
          legCount: m.legCount,
          ourOdds,
          winOdds,
          ourDecimal: ourDecimal ? Math.round(ourDecimal * 1000) / 1000 : null,
          winDecimal: winDecimal ? Math.round(winDecimal * 1000) / 1000 : null,
          ourProb: Math.round(ourProb * 10000) / 100,
          winProb: Math.round(winProb * 10000) / 100,
          gapProb: Math.round(gapProb * 10000) / 100,
          won: true,  // every entry is a win under Alec's model
          stake: m.matchedStake,
          time: m.matchedAt,
        };
      }).sort((a, b) => (b.time || '').localeCompare(a.time || ''));

      const avgDrift = entries.length > 0 ? entries.reduce((s, e) => s + e.gapProb, 0) / entries.length : 0;

      return {
        entries,
        summary: {
          totalQuoted: entries.length,
          wins: entries.length,
          losses: 0,  // not observable via this event path
          avgGapAll: Math.round(avgDrift * 100) / 100,
          avgGapWins: Math.round(avgDrift * 100) / 100,
          avgGapLosses: null,
        },
      };
    })(),
  };
}

function americanToProb(odds) {
  odds = Number(odds);
  if (odds >= 100) return 100 / (odds + 100);
  if (odds <= -100) return Math.abs(odds) / (Math.abs(odds) + 100);
  return 0.5;
}

// ---------------------------------------------------------------------------
// LOOKUPS
// ---------------------------------------------------------------------------

function findByParlayId(parlayId) {
  return orders[parlayId] || null;
}

/**
 * Returns true if all legs of an order have already started (game is
 * finished or in-progress). These orders are awaiting settlement from PX
 * but no longer represent live risk — the outcome is already determined.
 * Used to exclude phantom exposure from stale reconstructed orders.
 */
function isOrderFinished(order) {
  const legs = order.legs || order.meta?.legs || [];
  if (legs.length === 0) return false;
  const now = Date.now();
  // Every leg with a startTime must have started. Legs without a startTime
  // (reconstructed with team='?') are assumed stale if confirmedAt is old.
  let hasAnyTime = false;
  for (const leg of legs) {
    const st = leg.startTime || leg.start_time;
    if (st) {
      hasAnyTime = true;
      if (new Date(st).getTime() > now) return false; // future game
    }
  }
  if (hasAnyTime) return true;
  // No startTime on any leg — use confirmedAt as a proxy. If confirmed
  // more than 8 hours ago, almost certainly finished.
  const ca = order.confirmedAt;
  if (ca && (now - new Date(ca).getTime()) > 8 * 3600 * 1000) return true;
  return false;
}

// ~12h covers any single game (even longest MLB extra innings + cross-country
// travel padding) plus PX settlement lag. Anything older than this with all
// legs started is either a stuck settlement or a reconstructed phantom — not
// live risk we should show on the dashboard.
const STALE_PHANTOM_HOURS = 12;

/**
 * Returns true only for orders that are almost certainly phantom/stuck:
 * every leg started more than STALE_PHANTOM_HOURS ago (or no startTime on
 * any leg and confirmedAt is that old). In-play games that started minutes
 * or hours ago still count as live risk.
 *
 * This is the narrow exclusion used for dashboard "deployed" accounting.
 * isOrderFinished (above) is the broader exclusion used for exposure
 * rebuilds and pre-trade checks where "game has started" is the right
 * semantic regardless of how recently.
 */
function isOrderStalePhantom(order) {
  // Active PX cross-check (reconcileGhostConfirmed) sets meta.phantom=true
  // on orders PX doesn't recognize. Respect that first — it's the most
  // authoritative signal we have.
  if (order.meta && order.meta.phantom) return true;

  const legs = order.legs || order.meta?.legs || [];
  const now = Date.now();
  const cutoff = STALE_PHANTOM_HOURS * 3600 * 1000;

  // Treat 'confirmed' orders with no orderUuid older than 10 min as phantoms. Under Alec's confirmed PX event model, a real fill
  // produces BOTH order.matched (without orderUuid) AND order.finalized
  // (with orderUuid). If 10 min have passed since we promoted to
  // 'confirmed' via the matched path but order.finalized never showed
  // up, the fill was almost certainly not actually placed on PX and
  // it's inflating our Deployed number. Exclude from risk.
  //
  // 10 min is generous — in practice finalize arrives within seconds.
  if (order.status === 'confirmed' && !order.orderUuid) {
    const ca = order.confirmedAt;
    if (ca && (now - new Date(ca).getTime()) > 10 * 60 * 1000) return true;
  }

  let latestStart = 0;
  let hasAnyTime = false;
  for (const leg of legs) {
    const st = leg.startTime || leg.start_time;
    if (st) {
      hasAnyTime = true;
      const t = new Date(st).getTime();
      if (t > latestStart) latestStart = t;
    }
  }
  if (hasAnyTime) {
    // Still live if any leg is future or latest start is within cutoff.
    return (now - latestStart) > cutoff;
  }
  // No leg start times at all — fall back to confirmedAt.
  const ca = order.confirmedAt;
  if (ca && (now - new Date(ca).getTime()) > cutoff) return true;
  return false;
}

/**
 * Get total portfolio risk — sum of SP stakes across all confirmed orders
 * that still represent live exposure (pre-game or in-play, not yet settled).
 *
 * Excludes only stale phantom orders (all legs started >12h ago or no start
 * time and confirmed >12h ago) — those are either stuck settlements or
 * reconstructed-from-PX orders from previous restarts. Without that filter,
 * phantoms inflated this number by tens of thousands. With too broad a filter
 * (e.g. "any leg has started"), in-play games disappear from the dashboard
 * the moment they tip off — hiding real live risk.
 */
function getTotalPortfolioRisk() {
  let total = 0;
  for (const order of Object.values(orders)) {
    if (order.status !== 'confirmed') continue;
    if (isOrderStalePhantom(order)) continue;
    // confirmedStake IS our SP risk (verified from PX payload). No multiplication.
    total += (order.confirmedStake || 0);
  }
  return total;
}

/**
 * Sum of SP profit across all live (non-phantom) confirmed orders.
 * SP profit = bettor's wager = confirmedStake * 100 / |odds|.
 * This is what we keep if every active parlay settles in our favor.
 */
function getTotalToWin() {
  let total = 0;
  for (const order of Object.values(orders)) {
    if (order.status !== 'confirmed') continue;
    if (isOrderStalePhantom(order)) continue;
    const stake = order.confirmedStake || 0;
    const odds = Math.abs(order.confirmedOdds || order.offeredOdds || 0);
    if (stake > 0 && odds >= 100) total += stake * 100 / odds;
  }
  return total;
}

/**
 * Check if adding a new parlay would exceed the portfolio drawdown limit.
 * @param {number} additionalRisk - payout of the new parlay
 * @param {number} maxDrawdown - max allowed total portfolio risk
 * @returns {{ allowed: boolean, current: number, limit: number }}
 */
function checkPortfolioRisk(additionalRisk, maxDrawdown) {
  if (!maxDrawdown || maxDrawdown <= 0) return { allowed: true, current: 0, limit: 0 };
  const current = getTotalPortfolioRisk();
  if (current + additionalRisk > maxDrawdown) {
    return { allowed: false, current, additional: additionalRisk, limit: maxDrawdown };
  }
  return { allowed: true, current, limit: maxDrawdown };
}

function findByOrderUuid(uuid) {
  const parlayId = ordersByUuid[uuid];
  return parlayId ? orders[parlayId] : null;
}

function getRecentOrders(limit = 200) {
  const all = Object.values(orders);
  // Always include settled and confirmed orders regardless of limit
  const important = all.filter(o => o.status === 'confirmed' || (o.status && o.status.startsWith('settled_')));
  const rest = all.filter(o => o.status !== 'confirmed' && !(o.status && o.status.startsWith('settled_')))
    .sort((a, b) => (b.quotedAt || '').localeCompare(a.quotedAt || ''))
    .slice(0, limit);
  // Merge, deduplicate, sort
  const merged = [...important, ...rest];
  const seen = new Set();
  return merged.filter(o => {
    if (seen.has(o.parlayId)) return false;
    seen.add(o.parlayId);
    return true;
  }).sort((a, b) => (b.quotedAt || '').localeCompare(a.quotedAt || ''));
}

function getStats() {
  return {
    ...stats,
    // activeOrders is the "real open positions" count the dashboard shows.
    // Match getTotalPortfolioRisk()'s filter so Active and Deployed always
    // agree — both use isOrderStalePhantom to catch no-UUID ghosts and
    // stale confirmeds that plain meta.phantom misses.
    activeOrders: Object.values(orders).filter(o =>
      o.status === 'confirmed' && !isOrderStalePhantom(o)
    ).length,
    openQuotes: Object.values(orders).filter(o => o.status === 'quoted').length,
    totalOrders: Object.keys(orders).length,
    sessionFillRate: stats.sessionQuotes > 0
      ? Number((stats.sessionFills / stats.sessionQuotes * 100).toFixed(1))
      : null,
  };
}

/**
 * Get P&L summary grouped by sport.
 */
function getPnLBySport() {
  const bySport = {};
  for (const order of Object.values(orders)) {
    if (order.pnl == null) continue;
    const legs = order.meta?.legs || order.legs || [];
    const knownSports = [...new Set(legs.map(l => l.sport).filter(s => s && s !== 'unknown'))];
    let sport;
    if (knownSports.length === 0) sport = 'Unknown';
    else if (knownSports.length === 1) sport = knownSports[0];
    else sport = 'Multi-league';
    if (!bySport[sport]) bySport[sport] = { pnl: 0, count: 0, wins: 0, losses: 0 };
    bySport[sport].pnl += order.pnl;
    bySport[sport].count++;
    if (order.pnl > 0) bySport[sport].wins++;
    else if (order.pnl < 0) bySport[sport].losses++;
  }
  return bySport;
}

// ---------------------------------------------------------------------------
// PROBABILITY-WEIGHTED EXPOSURE
// ---------------------------------------------------------------------------
// For each team in each confirmed parlay, the weighted risk =
// payout × P(all OTHER legs win | this team's leg wins)
//
// Example: 3-leg parlay, Lakers (60%), Celtics (70%), Over (50%), payout $1000
// Lakers risk: $1000 × 0.70 × 0.50 = $350
// Celtics risk: $1000 × 0.60 × 0.50 = $300
// Over risk: $1000 × 0.60 × 0.70 = $420
//
// A 10-leg $2000 payout parlay at 50% each:
// Each team risk: $2000 × 0.50^9 = $3.91 (negligible)

function normalizeExposureKey(name) {
  return (name || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
}

function getLegsForExposure(order) {
  return order.legs || order.meta?.legs || [];
}

/**
 * Effective probability for a leg — prefers live odds if available.
 * liveFairProb is set by refreshLiveOdds for legs of in-progress games.
 * Falls back to the pre-game fairProb, then 0.5 if neither exists.
 */
function legEffectiveProb(leg) {
  if (leg.liveFairProb != null && leg.liveFairProb > 0 && leg.liveFairProb < 1) return leg.liveFairProb;
  if (leg.fairProb != null && leg.fairProb > 0 && leg.fairProb < 1) return leg.fairProb;
  return 0.5;
}

/**
 * Calculate our payout (= My Risk) for an order.
 * PX's confirmedStake IS our payout liability directly.
 */
function getOrderPayout(order) {
  return order.confirmedStake || order.maxRisk || 0;
}

// ---------------------------------------------------------------------------
// NET EXPOSURE MODEL
// ---------------------------------------------------------------------------
// For each game (pxEventId), we store all parlay legs touching that game.
// Net exposure = weighted payouts owed - stakes collected from offsetting
// positions on the OPPOSITE side of the same game.
//
// Example: Parlay 1 has Lakers -6.5 (+250, $1K stake, $2.5K payout)
//          Parlay 2 has Lakers -6.5 (-350, $3.5K stake, $1K payout)
// If Lakers cover: both legs win, weighted payouts = $2.5K×P1other + $1K×P2other
//                  but no opposite stakes to offset
// If Lakers DON'T cover: both parlays lose, we keep $4.5K
//
// Now add Parlay 3 with Celtics +6.5 (opposite side of Lakers game):
// If Lakers cover: Parlay 3 loses → we keep its stake (offset!)
// Net exposure = payouts owed - stakes from opposite side
// ---------------------------------------------------------------------------

/**
 * Add a confirmed order to exposure tracking.
 */
/**
 * True when ANY leg has already resolved as 'lost' — the parlay is dead
 * (guaranteed bettor loss / guaranteed SP win) but PX hasn't moved it to
 * settled_* yet because remaining legs are still in progress. Exposure
 * tables should treat these as zero risk: we already "have" the bettor's
 * stake; the remaining open legs can't add liability for us.
 *
 * 'push' does NOT make the parlay dead — pushed legs just drop out of
 * the parlay leaving the rest live. Only 'lost' triggers the skip.
 */
function isParlayAlreadyDead(legs) {
  if (!Array.isArray(legs)) return false;
  for (const l of legs) {
    const r = (l && (l.inferredResult || l.settlementStatus || l.settlement_status)) || null;
    if (r === 'lost') return true;
  }
  return false;
}

function addExposure(order) {
  const legs = getLegsForExposure(order);
  const payout = getOrderPayout(order); // = confirmedStake = our max risk
  // "stake" in net-exposure model = amount kept when bettor's parlay fails.
  // That's bettor's original wager = our profit on win = americanOddsToProfit(confirmedOdds, confirmedStake).
  const stake = americanOddsToProfit(order.confirmedOdds || 0, order.confirmedStake || 0);
  if (legs.length === 0) return;
  // Skip already-dead parlays — at least one leg has resolved as 'lost'
  // so we're guaranteed to win; remaining open legs carry no SP risk.
  // PX hasn't settled the parlay yet (waiting on all legs to complete),
  // but the financial outcome is locked in. Operator request 2026-04-30:
  // exposure tables should reflect this immediately so capacity isn't
  // tied up by parlays we can't lose.
  if (isParlayAlreadyDead(legs)) return;

  // Precompute per-leg effective prob + gameKey so we can compute the
  // CORRECT per-parlay-per-game weighted risk for SGPs. Without this,
  // a parlay with 2 legs on the same game would double-push into that
  // game's parlays[] array and inflate Stakes Held + parlayCount +
  // grossRisk in the dashboard totals (observed: $45k+ stakes held on
  // games where actual bettor wagers were much lower).
  const legProbs = legs.map(legEffectiveProb);
  const legGameKeys = legs.map(l => {
    const eid = l.pxEventId;
    const gd = l.startTime ? new Date(l.startTime).toISOString().substring(0, 10) : '';
    if (eid) return eid + '|' + gd;
    const opp = normalizeExposureKey((l.homeTeam || '') + (l.awayTeam || ''));
    return 'syn_' + (opp || '') + '|' + (gd || 'noevent');
  });
  // Per-game correct weighted risk = payout × product(probs of OFF-game legs).
  const offGameProbByGame = {};
  for (const gk of new Set(legGameKeys)) {
    let p = 1;
    for (let j = 0; j < legs.length; j++) {
      if (legGameKeys[j] !== gk) p *= legProbs[j];
    }
    offGameProbByGame[gk] = p;
  }

  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    const eventId = leg.pxEventId;
    // Prefer explicit team/teamName. If missing (reconstructed-from-PX legs
    // that never got enriched), fall back progressively so the leg still
    // shows up in Team Exposure rather than being silently dropped:
    //   1) homeTeam or awayTeam if known
    //   2) "Event {pxEventId}" as a last-resort placeholder
    //   3) 'unknown'
    // Without this fallback, `normalizeExposureKey('?')` returns '' and the
    // whole leg is skipped, collapsing the entire Team Exposure table after
    // every restart (reconstructed orders come back from DB with team='?').
    let name = leg.team || leg.teamName;
    if (!name || name === '?' || name === 'unknown') {
      name = leg.homeTeam || leg.awayTeam
        || (eventId ? `Event ${eventId}` : null)
        || 'unknown';
    }
    const teamKey = normalizeExposureKey(name);
    // Composite key: team + event + date so the same team on different games
    // is tracked as separate rows. Appends game date even when pxEventId exists,
    // because PX can reuse event IDs across different days.
    const gameDate = leg.startTime ? new Date(leg.startTime).toISOString().substring(0, 10) : '';
    let eventSuffix = eventId ? (eventId + '|' + gameDate) : null;
    if (!eventSuffix) {
      const opp = normalizeExposureKey((leg.homeTeam || '') + (leg.awayTeam || ''));
      eventSuffix = (opp || '') + '|' + (gameDate || 'noevent');
    }
    const key = teamKey + '|' + eventSuffix;

    // Product of all OTHER legs' effective probs (live if available, else pre-game)
    let otherProb = 1;
    for (let j = 0; j < legs.length; j++) {
      if (j === i) continue;
      otherProb *= legEffectiveProb(legs[j]);
    }

    // Game-level tracking (for net exposure calc)
    // Include date in gameKey to separate same-eventId across different days.
    const gameKey = eventId ? (eventId + '|' + gameDate) : ('syn_' + eventSuffix);
    if (!gameExposure[gameKey]) {
      gameExposure[gameKey] = {
        name: (leg.awayTeam || '?') + ' @ ' + (leg.homeTeam || '?'),
        sport: leg.sport,
        startTime: leg.startTime,
        parlays: [],
      };
    }
    gameExposure[gameKey].parlays.push({
      parlayId: order.parlayId,
      payout,
      stake,
      // weightedStake mirrors weightedRisk: the bettor's wager portion
      // we actually keep when THIS leg kills the parlay, weighted by
      // the prob of every OTHER leg hitting. Used in recalcNetExposure
      // to avoid crediting full parlay stakes as 100% offsetting — a
      // 5-leg parlay w/ Dodgers-ML as one leg only reliably dies at
      // the Dodgers leg when the other 4 all hit, so the stake we'd
      // keep in that scenario is stake * product(other-leg probs).
      weightedStake: stake * otherProb,
      legCount: legs.length,
      // Per-leg weightedRisk — kept for the detail drop-down so each
      // leg row shows its own marginal weighted contribution.
      weightedRisk: payout * otherProb,
      // Per-parlay-per-game weightedRisk — used by getGameExposureSnapshot
      // when deduping by parlayId so SGPs don't double-count. Equals
      // payout * prod(probs of legs NOT on this game). Identical on
      // every entry that shares (parlayId, gameKey) so aggregation can
      // safely pick the first seen.
      parlayGameWeightedRisk: payout * (offGameProbByGame[gameKey] || 1),
      selection: leg.selection,
      market: leg.market || leg.marketType,
      teamKey: key,
      // Passed through so the dashboard can render the exact
      // "<Team> <±Spread>" / "Over <Total>" label instead of the
      // ambiguous "spread away" / "total over" shape.
      teamName: leg.team || leg.teamName || name,
      line: leg.line,
      homeTeam: leg.homeTeam,
      awayTeam: leg.awayTeam,
    });

    // Team-level tracking (for dashboard display) — keyed by team+event
    if (teamKey) {
      if (!exposure[key]) {
        exposure[key] = {
          risk: 0,
          parlays: 0,
          name,
          teamKey,
          eventId: eventId || null,
          eventName: eventId ? ((leg.awayTeam || '?') + ' @ ' + (leg.homeTeam || '?')) : null,
          startTime: leg.startTime || null,
          sport: leg.sport || null,
          notionalPayout: 0,
          netExposure: 0,
        };
      }
      exposure[key].risk += payout * otherProb;
      exposure[key].parlays += 1;
      exposure[key].notionalPayout += payout;
    }
  }

  // Phase 2: per-pitcher exposure for player_strikeouts legs. Add full
  // confirmedStake against each pitcher in the parlay (overly cautious
  // — could refine to leg-conditional risk, but flat full-stake math is
  // easier to reason about and bounds risk safely for MVP).
  for (const leg of legs) {
    if (leg.marketType !== 'player_strikeouts') continue;
    const key = pitcherKeyForLeg(leg);
    if (!key) continue;
    if (!pitcherExposure[key]) {
      pitcherExposure[key] = {
        risk: 0,
        parlays: new Set(),
        playerName: leg.playerName || leg.teamName || null,
        pxEventId: leg.pxEventId || null,
      };
    }
    pitcherExposure[key].risk += payout;
    pitcherExposure[key].parlays.add(order.parlayId);
  }

  // Phase 2 prop launch: per-(sport,player) generic exposure for the
  // new player_<type> markets. No-op for parlays without prop legs.
  addPlayerExposure(order);

  // Recalculate net exposure for all affected games
  recalcNetExposure();
}

/**
 * Compute the pitcherExposure key for a leg. Returns null if the leg
 * isn't a player_strikeouts leg or is missing required fields.
 */
function pitcherKeyForLeg(leg) {
  if (!leg || leg.marketType !== 'player_strikeouts') return null;
  const player = leg.playerName || leg.teamName;
  if (!player) return null;
  const eid = leg.pxEventId || 'unknown';
  return `${eid}|${normalizeExposureKey(player)}`;
}

/**
 * Remove a settled order from exposure tracking.
 */
function removeExposure(order) {
  const legs = getLegsForExposure(order);
  const payout = getOrderPayout(order);

  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    const eventId = leg.pxEventId;
    const teamKey = normalizeExposureKey(leg.team || leg.teamName || '');
    // Must match the key logic in addExposure
    const gameDate = leg.startTime ? new Date(leg.startTime).toISOString().substring(0, 10) : '';
    let eventSuffix = eventId ? (eventId + '|' + gameDate) : null;
    if (!eventSuffix) {
      const opp = normalizeExposureKey((leg.homeTeam || '') + (leg.awayTeam || ''));
      eventSuffix = (opp || '') + '|' + (gameDate || 'noevent');
    }
    const key = teamKey + '|' + eventSuffix;

    // Remove from game exposure
    const gameKey = eventId ? (eventId + '|' + gameDate) : ('syn_' + eventSuffix);
    if (gameExposure[gameKey]) {
      gameExposure[gameKey].parlays = gameExposure[gameKey].parlays.filter(
        p => p.parlayId !== order.parlayId
      );
      if (gameExposure[gameKey].parlays.length === 0) {
        delete gameExposure[gameKey];
      }
    }

    // Remove from team exposure (composite team+event key)
    if (teamKey && exposure[key]) {
      let otherProb = 1;
      for (let j = 0; j < legs.length; j++) {
        if (j === i) continue;
        otherProb *= legEffectiveProb(legs[j]);
      }
      exposure[key].risk -= payout * otherProb;
      exposure[key].parlays -= 1;
      exposure[key].notionalPayout -= payout;
      if (exposure[key].parlays <= 0) delete exposure[key];
    }
  }

  // Phase 2: mirror the per-pitcher exposure removal.
  for (const leg of legs) {
    if (leg.marketType !== 'player_strikeouts') continue;
    const pkey = pitcherKeyForLeg(leg);
    if (!pkey || !pitcherExposure[pkey]) continue;
    pitcherExposure[pkey].risk -= payout;
    pitcherExposure[pkey].parlays.delete(order.parlayId);
    if (pitcherExposure[pkey].parlays.size === 0 || pitcherExposure[pkey].risk <= 0) {
      delete pitcherExposure[pkey];
    }
  }

  // Phase 2 prop launch: mirror generic per-player removal. No-op for
  // parlays without player_<type> prop legs.
  removePlayerExposure(order);

  recalcNetExposure();
}

/**
 * Check if accepting a new parlay containing one or more player_strikeouts
 * legs would push the per-pitcher exposure over the cap. Called from
 * pricer.shouldDecline. Returns null if all clear, otherwise an object
 * with details for the decline reason.
 */
/**
 * Snapshot of all live per-pitcher exposure entries. Used by the
 * /prop-performance endpoint to surface concentration risk.
 */
function getPitcherExposureSnapshot() {
  const out = [];
  // Union of confirmed + pending keys so a pitcher with only in-flight
  // quotes still shows up.
  const seen = new Set();
  for (const [key, v] of Object.entries(pitcherExposure)) {
    seen.add(key);
    const pending = getPendingPitcherRisk(key);
    out.push({
      key,
      pxEventId: v.pxEventId,
      playerName: v.playerName,
      risk: Math.round((v.risk || 0) * 100) / 100,
      pending: Math.round(pending * 100) / 100,
      total: Math.round(((v.risk || 0) + pending) * 100) / 100,
      parlayCount: v.parlays ? v.parlays.size : 0,
      parlayIds: v.parlays ? [...v.parlays] : [],
    });
  }
  for (const [key, pending] of pendingPitcherRiskByKey.entries()) {
    if (seen.has(key)) continue;
    out.push({
      key,
      pxEventId: null,
      playerName: key.split('|')[1] || null,
      risk: 0,
      pending: Math.round(pending * 100) / 100,
      total: Math.round(pending * 100) / 100,
      parlayCount: 0,
      parlayIds: [],
    });
  }
  return out.sort((a, b) => (b.total || b.risk) - (a.total || a.risk));
}

/**
 * Compute the playerExposure key for any player_<type> prop leg.
 * Recognizes Phase-2 marketTypes (player_points, player_rebounds,
 * player_assists, player_threes, player_shots_on_goal) AND the legacy
 * player_strikeouts (MLB pitchers). As of 2026-05-01 we consolidated
 * the legacy MAX_EXPOSURE_PER_PITCHER cap into the unified per-player
 * system — pitcher_strikeouts now respects MAX_EXPOSURE_PER_PLAYER_*
 * env vars instead. The pitcherExposure map is still populated for
 * instrumentation/visibility but no longer drives quote-time gating.
 *
 * Player name normalization: strip diacritics, periods, apostrophes,
 * lowercase, collapse whitespace. Same canonicalization as the TOA
 * lookup so "C.J. McCollum" and "CJ McCollum" produce the same key.
 */
function playerKeyForLeg(leg) {
  if (!leg) return null;
  const mt = leg.marketType || '';
  if (!/^player_/.test(mt)) return null;
  const sport = leg.sport || leg.oddsApiSport;
  const player = leg.playerName;
  if (!sport || !player) return null;
  const norm = String(player)
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[.'`]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!norm) return null;
  return `${sport}|${norm}`;
}

/**
 * Add a confirmed parlay's exposure against playerExposure for each
 * prop leg with a player_<type> marketType. Mirrors the addPitcher
 * loop in addExposure but for the new generic-prop key. Safe to call
 * unconditionally — leg-level filtering happens via playerKeyForLeg.
 */
function addPlayerExposure(order) {
  const legs = getLegsForExposure(order);
  const payout = getOrderPayout(order);
  for (const leg of legs) {
    const key = playerKeyForLeg(leg);
    if (!key) continue;
    if (!playerExposure[key]) {
      playerExposure[key] = {
        risk: 0,
        parlays: new Set(),
        playerName: leg.playerName,
        sport: leg.sport || leg.oddsApiSport,
        propTypes: new Set(),
      };
    }
    playerExposure[key].risk += payout;
    playerExposure[key].parlays.add(order.parlayId);
    if (leg.marketType) playerExposure[key].propTypes.add(leg.marketType);
  }
}

/**
 * Mirror of addPlayerExposure for settlement / removal flows.
 */
function removePlayerExposure(order) {
  const legs = getLegsForExposure(order);
  const payout = getOrderPayout(order);
  for (const leg of legs) {
    const key = playerKeyForLeg(leg);
    if (!key || !playerExposure[key]) continue;
    playerExposure[key].risk -= payout;
    playerExposure[key].parlays.delete(order.parlayId);
    if (playerExposure[key].parlays.size === 0 || playerExposure[key].risk <= 0) {
      delete playerExposure[key];
    }
  }
}

/**
 * Quote-time check: would accepting a new parlay's prop legs push any
 * (sport, player) pair past its cap? Caps may be per-sport via
 * capBySport map; falls back to defaultCap. Returns null if all clear,
 * otherwise the first violating entry with details.
 */
function checkPlayerExposure(legs, additionalRisk, capBySport, defaultCap) {
  if (!Array.isArray(legs) || legs.length === 0) return null;
  if (!(additionalRisk > 0)) return null;
  for (const leg of legs) {
    const li = leg.lineInfo || leg;
    const key = playerKeyForLeg(li);
    if (!key) continue;
    const sport = li.sport || li.oddsApiSport;
    const cap = (capBySport && capBySport[sport] != null) ? capBySport[sport] : defaultCap;
    if (!(cap > 0)) continue;
    const current = playerExposure[key]?.risk || 0;
    const pending = getPendingPlayerRisk(key);
    const wouldBe = current + pending + additionalRisk;
    if (wouldBe > cap) {
      return {
        exceeded: true,
        player: li.playerName,
        sport,
        current: Math.round(current * 100) / 100,
        pending: Math.round(pending * 100) / 100,
        wouldBe: Math.round(wouldBe * 100) / 100,
        max: cap,
      };
    }
  }
  return null;
}

/**
 * Snapshot of all live per-player exposure entries (Phase-2 props).
 * Used by /status and /player-exposure to surface concentration risk.
 */
function getPlayerExposureSnapshot() {
  const out = [];
  const seen = new Set();
  for (const [key, v] of Object.entries(playerExposure)) {
    seen.add(key);
    const pending = getPendingPlayerRisk(key);
    out.push({
      key,
      sport: v.sport,
      playerName: v.playerName,
      propTypes: v.propTypes ? [...v.propTypes] : [],
      risk: Math.round((v.risk || 0) * 100) / 100,
      pending: Math.round(pending * 100) / 100,
      total: Math.round(((v.risk || 0) + pending) * 100) / 100,
      parlayCount: v.parlays ? v.parlays.size : 0,
      parlayIds: v.parlays ? [...v.parlays] : [],
    });
  }
  for (const [key, pending] of pendingPlayerRiskByKey.entries()) {
    if (seen.has(key)) continue;
    const [sport, name] = key.split('|');
    out.push({
      key, sport, playerName: name, propTypes: [],
      risk: 0, pending: Math.round(pending * 100) / 100,
      total: Math.round(pending * 100) / 100,
      parlayCount: 0, parlayIds: [],
    });
  }
  return out.sort((a, b) => (b.total || b.risk) - (a.total || a.risk));
}

function checkPitcherExposure(legs, additionalRisk, maxPerPitcher) {
  if (!Array.isArray(legs) || legs.length === 0) return null;
  if (!(additionalRisk > 0)) return null;
  for (const leg of legs) {
    // pricer.js shouldDecline iterates legs as { lineInfo, ... } objects;
    // accept either shape so this works whether called with raw legs or
    // resolved-leg wrappers.
    const li = leg.lineInfo || leg;
    if (li.marketType !== 'player_strikeouts') continue;
    const key = pitcherKeyForLeg(li);
    if (!key) continue;
    const current = pitcherExposure[key]?.risk || 0;
    // Include in-flight (quoted but not yet confirmed) reservations
    // against this pitcher. Closes the race where N concurrent RFQs all
    // pass because none has confirmed yet. websocket.js calls
    // releasePending(parlayId) before the per-team confirm-time recheck;
    // we don't have a confirm-time pitcher recheck, but the pending
    // index still drains correctly when each quote releases or expires.
    const pending = getPendingPitcherRisk(key);
    const wouldBe = current + pending + additionalRisk;
    if (wouldBe > maxPerPitcher) {
      return {
        exceeded: true,
        pitcher: li.playerName || li.teamName,
        pxEventId: li.pxEventId,
        current: Math.round(current * 100) / 100,
        pending: Math.round(pending * 100) / 100,
        wouldBe: Math.round(wouldBe * 100) / 100,
        max: maxPerPitcher,
      };
    }
  }
  return null;
}

/**
 * Recalculate net exposure for all games.
 * For each game, for each possible "side" (selection), compute:
 *   netExposure = sum(weighted payouts for winning parlays) - sum(stakes from losing parlays on opposite side)
 * Take the worst case across all sides.
 */
function recalcNetExposure() {
  // Reset team net exposure
  for (const key of Object.keys(exposure)) {
    exposure[key].netExposure = 0;
  }

  for (const [eventId, game] of Object.entries(gameExposure)) {
    // Group parlays by selection within this game
    const bySelection = {};
    for (const p of game.parlays) {
      const sel = (p.market || '') + ':' + (p.selection || '');
      if (!bySelection[sel]) bySelection[sel] = [];
      bySelection[sel].push(p);
    }

    // For each selection (e.g., "moneyline:home"), compute net exposure if that side wins
    for (const [sel, winningParlays] of Object.entries(bySelection)) {
      // Weighted payouts we'd owe if this selection wins
      const weightedPayouts = winningParlays.reduce((s, p) => s + p.weightedRisk, 0);

      // Stakes we'd collect from parlays on the OPPOSITE side of this game
      // (their leg loses, so entire parlay loses, we keep their stake)
      let oppositeStakes = 0;
      for (const [otherSel, otherParlays] of Object.entries(bySelection)) {
        if (otherSel === sel) continue; // same side, skip
        // Check if this is truly opposite (same market, different selection)
        const selMarket = sel.split(':')[0];
        const otherMarket = otherSel.split(':')[0];
        if (selMarket === otherMarket) {
          // Same market type, different selection = opposite side
          // Use weightedStake (= stake × other-legs-prob) not raw
          // stake. A 5-leg parlay with one opposite-side leg only
          // "actually dies at this leg" (us keeping the wager in the
          // scenario being evaluated) when the other 4 legs have
          // resolved in the bettor's favor. Counting full stake
          // over-credited offsets and drove legit large exposures to
          // $0 Net (observed: Rockies row showed $0 despite $1,592
          // gross on 3 large Rockies+Under SGPs).
          oppositeStakes += otherParlays.reduce(
            (s, p) => s + (p.weightedStake != null ? p.weightedStake : p.stake),
            0
          );
        }
      }

      const netExp = Math.max(0, weightedPayouts - oppositeStakes);

      // Attribute net exposure back to teams involved
      // netExp is the TOTAL net exposure for this selection — each team key
      // gets the full amount (not divided by parlay count)
      for (const p of winningParlays) {
        if (p.teamKey && exposure[p.teamKey]) {
          exposure[p.teamKey].netExposure = Math.max(
            exposure[p.teamKey].netExposure || 0,
            netExp
          );
        }
      }
    }

    // Store net exposure on the game itself
    game.netExposure = 0;
    for (const [sel, winningParlays] of Object.entries(bySelection)) {
      const wp = winningParlays.reduce((s, p) => s + p.weightedRisk, 0);
      let os = 0;
      for (const [otherSel, otherParlays] of Object.entries(bySelection)) {
        if (otherSel === sel) continue;
        const selMarket = sel.split(':')[0];
        const otherMarket = otherSel.split(':')[0];
        if (selMarket === otherMarket) {
          os += otherParlays.reduce((s, p) => s + p.stake, 0);
        }
      }
      game.netExposure = Math.max(game.netExposure, Math.max(0, wp - os));
    }
  }
}

/**
 * Get game exposure snapshot with net exposure, sorted by worst case.
 */
function getGameExposureSnapshot() {
  return Object.entries(gameExposure).map(([eventId, game]) => {
    // Dedupe by parlayId: a SGP with 2+ legs on the same game gets
    // pushed into parlays[] once per leg so the detail drop-down can
    // render each selection. Aggregating blindly double-counted stake
    // and risk (observed: $45k+ Stakes Held on games where actual
    // bettor wagers were a fraction of that).
    const seen = new Set();
    let grossRisk = 0;
    let totalStakes = 0;
    for (const p of game.parlays) {
      if (seen.has(p.parlayId)) continue;
      seen.add(p.parlayId);
      // Prefer the parlay-level weightedRisk which is computed as
      // payout × prod(off-game leg probs). Fallback to per-leg value for
      // orders persisted before this field existed.
      grossRisk += (p.parlayGameWeightedRisk != null ? p.parlayGameWeightedRisk : p.weightedRisk) || 0;
      totalStakes += p.stake || 0;
    }
    const distinctParlayCount = seen.size;
    // hasLiveOdds drives the green LIVE badge in the dashboard's Game
    // Exposure table. Pre-Apr-26 it fired whenever ANY leg in ANY parlay
    // touching this game had liveFairProb — which made multi-game parlays
    // bleed the LIVE flag onto unrelated games. Operator-visible:
    // Lakers @ Rockets at 9:30 PM ET was tagged LIVE at ~1 PM ET (8.5h
    // before tip-off) because a parlay touching the Lakers game also
    // contained a Rockies @ Mets leg that was actually in-progress.
    //
    // Two-layer gate:
    //   1) THIS game's own startTime must be in the past — a not-yet-
    //      started game can never legitimately have live odds, no matter
    //      what other legs exist.
    //   2) The leg carrying liveFairProb must belong to THIS game (match
    //      on home/away pair — pxEventId isn't always populated on
    //      reconstructed legs, so don't rely on it as the only key).
    let hasLiveOdds = false;
    const gameStartMs = game.startTime ? new Date(game.startTime).getTime() : null;
    if (gameStartMs && gameStartMs <= Date.now()) {
      for (const p of game.parlays) {
        const order = orders[p.parlayId];
        if (!order) continue;
        const legs = order.legs || order.meta?.legs || [];
        if (legs.some(l => l.liveFairProb != null && (
          (l.homeTeam === p.homeTeam && l.awayTeam === p.awayTeam) ||
          (l.homeTeam === p.awayTeam && l.awayTeam === p.homeTeam)
        ))) {
          hasLiveOdds = true;
          break;
        }
      }
    }
    return {
      eventId,
      name: game.name,
      sport: game.sport,
      startTime: game.startTime,
      parlayCount: distinctParlayCount,
      grossRisk: Math.round(grossRisk * 100) / 100,
      totalStakes: Math.round(totalStakes * 100) / 100,
      netExposure: Math.round((game.netExposure || 0) * 100) / 100,
      worstCase: Math.round((game.netExposure || grossRisk) * 100) / 100,
      hasLiveOdds,
      parlays: game.parlays,
    };
  }).sort((a, b) => b.netExposure - a.netExposure);
}

/**
 * Sum SP risk (confirmedStake) across all open/confirmed parlays that
 * touch a given series pxEventId with a series_* market. Used for the
 * series-specific gross exposure cap — series events have their own
 * pxEventIds distinct from underlying game events.
 */
function getSeriesEventRisk(pxEventId) {
  if (!pxEventId) return 0;
  let total = 0;
  for (const o of Object.values(orders)) {
    if (o.status !== 'confirmed') continue;
    if (o.meta && o.meta.phantom) continue;
    const legs = o.legs || (o.meta && o.meta.legs) || [];
    const touches = legs.some(l =>
      l.pxEventId === pxEventId &&
      typeof (l.market || l.marketType) === 'string' &&
      (l.market || l.marketType).startsWith('series_')
    );
    if (touches) total += (o.confirmedStake || 0);
  }
  return total;
}

/**
 * Check whether adding a new parlay would exceed the per-series-event
 * SP risk cap. Only fires for parlays containing ≥1 series_* leg.
 * Enforces at quote-time with worstCaseRisk (typically the max_risk
 * set on the offer) and at confirm-time with actual ourRisk.
 */
function checkSeriesExposure(legs, additionalRisk, maxPerSeries) {
  if (!maxPerSeries || maxPerSeries <= 0) return { allowed: true };
  const seen = new Set();
  for (const leg of legs) {
    const li = leg.lineInfo || leg;
    const mt = li.marketType || leg.market;
    if (typeof mt !== 'string' || !mt.startsWith('series_')) continue;
    const eid = li.pxEventId || leg.pxEventId;
    if (!eid || seen.has(eid)) continue;
    seen.add(eid);
    const current = getSeriesEventRisk(eid);
    const wouldBe = current + additionalRisk;
    if (wouldBe > maxPerSeries) {
      return {
        allowed: false,
        reason: `Series event ${eid}: current $${Math.round(current)} + new $${Math.round(additionalRisk)} > max $${Math.round(maxPerSeries)}`,
        eventId: eid,
        current,
        additionalRisk,
        wouldBe,
        limit: maxPerSeries,
      };
    }
  }
  return { allowed: true };
}

/**
 * Check if adding a new parlay would exceed per-game NET exposure limits.
 */
function checkGameExposure(legs, estPayout, maxPerGame) {
  if (!maxPerGame || maxPerGame <= 0) return { allowed: true };
  // Same fill-fraction discount as checkExposureLimits — scales quote-time
  // projections only. Real confirmed exposure stays raw.
  let discount = 1.0;
  try {
    const { config } = require('../config');
    const d = config?.pricing?.pendingReservationDiscount;
    if (Number.isFinite(d) && d > 0 && d <= 1) discount = d;
  } catch { /* ignore */ }

  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    const li = leg.lineInfo || leg;
    const eventId = li.pxEventId || leg.pxEventId;
    const gameDate = li.startTime ? new Date(li.startTime).toISOString().substring(0, 10) : '';
    // Build game key matching addExposure logic — include date to separate same eventId across days
    let gameKey = eventId ? (eventId + '|' + gameDate) : null;
    if (!gameKey) {
      const opp = normalizeExposureKey((li.homeTeam || '') + (li.awayTeam || ''));
      gameKey = 'syn_' + (opp || '') + '|' + (gameDate || 'noevent');
    }

    let otherProb = 1;
    for (let j = 0; j < legs.length; j++) {
      if (j === i) continue;
      otherProb *= (legs[j].lineInfo?.fairProb || legs[j].fairProb || 0.5);
    }
    const newWeightedRiskRaw = estPayout * otherProb;
    const newWeightedRiskEff = newWeightedRiskRaw * discount;

    // Current net exposure for this game (real confirmations) + pending quotes
    const currentNet = gameExposure[gameKey]?.netExposure || 0;
    const pendingNetRaw = getPendingGameRisk(gameKey);
    const pendingNetEff = pendingNetRaw * discount;

    // Conservative: add full weighted risk (worst case is no offsetting)
    if (currentNet + pendingNetEff + newWeightedRiskEff > maxPerGame) {
      const gameName = gameExposure[gameKey]?.name || gameKey;
      const wouldBe = currentNet + pendingNetEff + newWeightedRiskEff;
      const discTag = discount < 1 ? ` (discount ${Math.round(discount * 100)}%)` : '';
      return {
        allowed: false,
        reason: `Game "${gameName}" net $${Math.round(currentNet)} + pending $${Math.round(pendingNetEff)} + new $${Math.round(newWeightedRiskEff)} > max $${Math.round(maxPerGame)}${discTag}`,
        wouldBe,
        limit: maxPerGame,
        pendingRaw: Math.round(pendingNetRaw * 100) / 100,
        pendingEffective: Math.round(pendingNetEff * 100) / 100,
        newRiskRaw: Math.round(newWeightedRiskRaw * 100) / 100,
        newRiskEffective: Math.round(newWeightedRiskEff * 100) / 100,
        reservationDiscount: discount,
      };
    }
  }
  return { allowed: true };
}

/**
 * Check if adding a parlay would exceed per-team NET exposure limits.
 *
 * The `maxNetExposure` arg is the GLOBAL default cap. Per-team overrides
 * (config.pricing.exposureOverridesPerTeam) are applied first when a
 * leg's team name matches; only legs with no override use the global.
 * Lets you tighten exposure on a few specific fighters/teams without
 * lowering the cap for everyone else.
 */
function checkExposureLimits(legs, payout, maxNetExposure) {
  if (!maxNetExposure || maxNetExposure <= 0) {
    return { allowed: true, reason: null, violations: [] };
  }
  // Lazy-require config so this module stays decoupled from circular import
  // risk during bootstrap. Default to 1.0 (no discount) if config is missing.
  let discount = 1.0;
  let overridesByKey = {};
  try {
    const { config } = require('../config');
    const d = config?.pricing?.pendingReservationDiscount;
    if (Number.isFinite(d) && d > 0 && d <= 1) discount = d;
    // Pre-normalize override keys with the SAME function the exposure
    // map uses — without this, "Islam Makhachev" in env wouldn't match
    // legs that arrive with whitespace/case variations.
    const ovs = config?.pricing?.exposureOverridesPerTeam || {};
    for (const [name, cap] of Object.entries(ovs)) {
      const k = normalizeExposureKey(name);
      if (k && Number.isFinite(cap) && cap > 0) overridesByKey[k] = cap;
    }
  } catch { /* ignore */ }

  const violations = [];
  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    const name = leg.team || leg.teamName || leg.lineInfo?.teamName || 'unknown';
    const teamKey = normalizeExposureKey(name);
    if (!teamKey) continue;
    const eventId = leg.lineInfo?.pxEventId || leg.pxEventId;
    const li = leg.lineInfo || leg;
    const gameDate = li.startTime ? new Date(li.startTime).toISOString().substring(0, 10) : '';
    // Must match the key logic in addExposure
    let eventSuffix = eventId ? (eventId + '|' + gameDate) : null;
    if (!eventSuffix) {
      const opp = normalizeExposureKey((li.homeTeam || '') + (li.awayTeam || ''));
      eventSuffix = (opp || '') + '|' + (gameDate || 'noevent');
    }
    const key = teamKey + '|' + eventSuffix;

    let otherProb = 1;
    for (let j = 0; j < legs.length; j++) {
      if (j === i) continue;
      otherProb *= (legs[j].fairProb || legs[j].lineInfo?.fairProb || 0.5);
    }

    // Resolve effective cap: per-team override wins over global default.
    const effectiveLimit = overridesByKey[teamKey] != null
      ? overridesByKey[teamKey]
      : maxNetExposure;
    const overrideApplied = overridesByKey[teamKey] != null;

    // Raw values (for logging + UI transparency) and effective values (the
    // ones actually compared to the limit). Discount applies only to
    // quote-time projections — confirmed exposure (currentNet) stays raw.
    const newRiskRaw = payout * otherProb;
    const newRiskEff = newRiskRaw * discount;
    const currentNet = exposure[key]?.netExposure || 0;
    // Pending = in-flight reservations from quotes not yet confirmed.
    // Including this closes the race window where N concurrent RFQs for the
    // same team all pass because none has been confirmed yet.
    const pendingNetRaw = getPendingTeamRisk(key);
    const pendingNetEff = pendingNetRaw * discount;
    const afterAdd = currentNet + pendingNetEff + newRiskEff;

    if (afterAdd > effectiveLimit) {
      if (overrideApplied) {
        log.info('Exposure', `Per-team override BLOCKED ${name}: would-be $${Math.round(afterAdd*100)/100} > override $${effectiveLimit} (global $${maxNetExposure})`);
      }
      violations.push({
        team: name,
        currentExposure: Math.round(currentNet * 100) / 100,
        pendingExposure: Math.round(pendingNetRaw * 100) / 100,
        pendingEffective: Math.round(pendingNetEff * 100) / 100,
        newRisk: Math.round(newRiskRaw * 100) / 100,
        newRiskEffective: Math.round(newRiskEff * 100) / 100,
        wouldBe: Math.round(afterAdd * 100) / 100,
        limit: effectiveLimit,
        globalLimit: maxNetExposure,
        overrideApplied,
        reservationDiscount: discount,
      });
    }
  }

  if (violations.length > 0) {
    const names = violations.map(v => {
      const tag = v.overrideApplied ? ' [override]' : '';
      return `${v.team} ($${v.wouldBe}/$${v.limit}${tag})`;
    }).join(', ');
    return {
      allowed: false,
      reason: `Net exposure limit exceeded: ${names}`,
      violations,
    };
  }
  return { allowed: true, reason: null, violations: [] };
}

function getExposureForTeam(teamName) {
  const key = normalizeExposureKey(teamName);
  return exposure[key] || null;
}

/**
 * Get full exposure snapshot — all teams with net exposure.
 */
function getExposureSnapshot() {
  return Object.entries(exposure)
    .map(([key, val]) => ({
      key,
      ...val,
      risk: Math.round(val.risk * 100) / 100,
      netExposure: Math.round((val.netExposure || 0) * 100) / 100,
      notionalPayout: Math.round((val.notionalPayout || 0) * 100) / 100,
    }))
    .sort((a, b) => (b.netExposure || b.risk) - (a.netExposure || a.risk));
}

/**
 * Poll PX orders API for settlement updates.
 * Catches settlements that WebSocket events may have missed.
 */
/**
 * Clear all exposure state and rebuild from scratch by re-adding every
 * confirmed order. Picks up any updated liveFairProb values on legs.
 */
function rebuildAllExposure() {
  // Clear state
  for (const k of Object.keys(exposure)) delete exposure[k];
  for (const k of Object.keys(gameExposure)) delete gameExposure[k];
  // Diagnostic counters so we can tell WHY exposure comes out small.
  const diag = {
    totalOrders: 0,
    confirmedOrders: 0,
    confirmedWithLegs: 0,
    legsTotal: 0,
    legsWithTeamKey: 0,
    legsSkippedNoTeam: 0,
    legsSkippedNoPayout: 0,
    uniqueTeamKeys: new Set(),
  };
  // Re-add all confirmed orders. We DO NOT skip orders whose games have
  // already started — they still represent real risk on our books until
  // they actually move to settled_*. Zombie/stuck confirmed orders are
  // cleaned by the settlement poller + drift reconcile, not by hiding
  // them from exposure. Previous isOrderFinished filter was silently
  // dropping 30+ parlays per hot game after every live-odds refresh.
  //
  // BUT skip orders explicitly flagged meta.phantom — the ghost
  // reconciler already determined PX can't locate these and we treat
  // them as not-really-open. Exposure tables and Deployed counters
  // should not include them, matching Open Positions view which
  // already filters phantoms.
  let skippedFinished = 0;
  let skippedPhantom = 0;
  let skippedNoUuid = 0;
  for (const order of Object.values(orders)) {
    diag.totalOrders++;
    if (order.status !== 'confirmed') continue;
    if (order.meta && order.meta.phantom) { skippedPhantom++; continue; }
    // Also skip computed stale phantoms (confirmed>10min ago without an
    // orderUuid, or all legs started >12h ago) — same policy as Open
    // Positions. Without this, phantom "matches" that PX never finalized
    // keep inflating Team/Game Exposure with tentative bookings the
    // bettor walked away from.
    if (isOrderStalePhantom(order)) { skippedPhantom++; continue; }
    // Belt-and-suspenders: orders missing orderUuid AFTER phantom-sweep
    // means order.finalized never fired. Don't count them — same logic
    // as the exposure gate in recordConfirmation.
    if (!order.orderUuid) { skippedNoUuid++; continue; }
    diag.confirmedOrders++;
    const legs = order.legs || order.meta?.legs || [];
    if (legs.length === 0) continue;
    diag.confirmedWithLegs++;
    const payout = getOrderPayout(order);
    for (const leg of legs) {
      diag.legsTotal++;
      const name = leg.team || leg.teamName || 'unknown';
      const teamKey = normalizeExposureKey(name);
      if (!teamKey) { diag.legsSkippedNoTeam++; continue; }
      if (!payout) diag.legsSkippedNoPayout++;
      diag.legsWithTeamKey++;
      diag.uniqueTeamKeys.add(teamKey);
    }
    addExposure(order);
  }
  if (skippedFinished > 0) log.info('Exposure', `Skipped ${skippedFinished} finished orders during rebuild`);
  if (skippedPhantom > 0) log.info('Exposure', `Skipped ${skippedPhantom} phantom-flagged orders during rebuild`);
  if (skippedNoUuid > 0) log.info('Exposure', `Skipped ${skippedNoUuid} no-orderUuid (unfinalized) orders during rebuild`);
  // Also rebuild template-exposure from the same order set so the ramp
  // sees in-window signature counts after a restart. The module's
  // rebuildFromOrders skips anything older than its window (24h default)
  // and anything without orderUuid + stake + confirmedAt.
  try {
    templateExposure.rebuildFromOrders(Object.values(orders));
  } catch (err) {
    log.warn('TemplateExposure', `rebuildFromOrders failed: ${err.message}`);
  }
  const result = {
    ...diag,
    uniqueTeamKeys: diag.uniqueTeamKeys.size,
    exposureKeysAfter: Object.keys(exposure).length,
    gameKeysAfter: Object.keys(gameExposure).length,
    skippedPhantom,
    skippedNoUuid,
  };
  log.info('Exposure', `rebuildAllExposure: ${JSON.stringify(result)}`);
  return result;
}

/**
 * Refresh live odds for in-progress games and update weighted risk
 * calculations. Pulls live odds from the odds feed (live=true) for each
 * sport that has any in-progress game in a confirmed parlay, updates
 * leg.liveFairProb on affected legs, then rebuilds exposure.
 *
 * Returns a summary: { sportsRefreshed, legsUpdated, inProgressGames }.
 */
async function refreshLiveOdds(oddsFeed) {
  const now = Date.now();
  // Find all in-progress legs across confirmed orders
  const inProgressLegsBySport = {}; // sport -> list of { order, leg }
  let inProgressGames = 0;
  const seenEvents = new Set();
  for (const order of Object.values(orders)) {
    if (order.status !== 'confirmed') continue;
    const legs = order.legs || order.meta?.legs || [];
    for (const leg of legs) {
      const startMs = leg.startTime ? new Date(leg.startTime).getTime() : null;
      if (!startMs || isNaN(startMs)) continue;
      if (startMs > now) continue; // not started yet
      // Skip legs where game is very likely over. Bumped from 4h → 5h
      // to cover extra-innings MLB, NBA/NHL OT, and ceremonial-end
      // padding — these were freezing on the dashboard at the 4h mark
      // because the live-odds refresher abandoned them while the game
      // was still in progress.
      if (now - startMs > 5 * 60 * 60 * 1000) continue;
      const sport = leg.sport || leg.oddsApiSport;
      if (!sport) continue;
      if (!inProgressLegsBySport[sport]) inProgressLegsBySport[sport] = [];
      inProgressLegsBySport[sport].push({ order, leg });
      const eventKey = sport + '|' + (leg.pxEventId || (leg.homeTeam + '@' + leg.awayTeam));
      if (!seenEvents.has(eventKey)) { seenEvents.add(eventKey); inProgressGames++; }
    }
  }

  const sports = Object.keys(inProgressLegsBySport);
  if (sports.length === 0) {
    return { sportsRefreshed: 0, legsUpdated: 0, inProgressGames: 0 };
  }

  // Fetch live odds for each sport with in-progress games. Strategy:
  //   1) SharpAPI live (free, thin coverage — moneyline mostly, best-effort)
  //   2) The Odds API in-play (Pinnacle+DK+FD across h2h/spreads/totals) —
  //      writes last so it wins over SharpAPI where both are present.
  //
  // Previously used DK Puppeteer scraping as the primary — that approach only
  // yielded moneyline XHRs (spreads/totals were rendered from client-side
  // state, not interceptable) and incurred 16-40s of headless-Chromium load
  // per sport per cycle. The Odds API covers moneyline+spreads+totals cleanly
  // at the same quota cost as pre-game, no fragility. DK remains the scraper
  // of record for NBA/NHL series winners (separate code path).
  let sportsRefreshed = 0;
  const LIVE_ODDS_API_SPORTS = new Set(['basketball_nba', 'baseball_mlb', 'icehockey_nhl', 'americanfootball_nfl']);
  for (const sport of sports) {
    try {
      // SharpAPI live (best-effort — may return empty for some sports).
      await oddsFeed.fetchOddsForSport(sport, { live: true }).catch(() => null);
      // The Odds API live — replaces DK scraper for in-play h2h/spreads/totals.
      if (LIVE_ODDS_API_SPORTS.has(sport)) {
        const result = await oddsFeed.mergeOddsApiLive(sport).catch(err => {
          log.warn('LiveOdds', `Odds API live merge failed for ${sport}: ${err.message}`);
          return null;
        });
        if (result && result.merged > 0) sportsRefreshed++;
      }
    } catch (err) {
      log.warn('LiveOdds', `Failed to fetch live odds for ${sport}: ${err.message}`);
    }
  }

  // Refresh DataGolf live in-play stats once per cycle if any golf legs
  // are in-progress. Golf doesn't have an in-play book-odds source the way
  // NBA/MLB/NHL/NFL do — DataGolf's per-player live win prob (renormalized
  // 2-way for matchups) plus a stroke-diff heuristic for round matchups
  // are the best available signals.
  const hasGolfInProgress = inProgressLegsBySport['golf_matchups']
    || inProgressLegsBySport['golf_pga_championship'];
  if (hasGolfInProgress) {
    try {
      const datagolf = require('./datagolf');
      await datagolf.refreshLiveStats();
    } catch (err) {
      log.debug('LiveOdds', `DataGolf live refresh failed: ${err.message}`);
    }
  }

  // Update liveFairProb on each in-progress leg
  let legsUpdated = 0;
  for (const [sport, legRecords] of Object.entries(inProgressLegsBySport)) {
    for (const { leg } of legRecords) {
      let prob = null;
      // Golf: DataGolf live in-play (renormalized 2-way for matchups)
      if (sport === 'golf_matchups' || (leg.oddsApiSport || '').startsWith('golf')) {
        try {
          const datagolf = require('./datagolf');
          prob = datagolf.getGolfLiveMatchupProb(
            leg.homeTeam,
            leg.awayTeam,
            leg.teamName || leg.team,
            leg.matchupType || 'round'
          );
        } catch (_) { /* fall through */ }
      }
      // All other sports: SharpAPI live + TOA in-play (existing path)
      if (prob == null) {
        prob = oddsFeed.getLiveFairProb(
          leg.oddsApiSport || sport,
          leg.homeTeam,
          leg.awayTeam,
          leg.oddsApiMarket || leg.market || leg.marketType,
          leg.oddsApiSelection || leg.selection,
          leg.line != null ? Math.abs(leg.line) : null,
          leg.startTime
        );
      }
      if (prob != null && prob > 0 && prob < 1) {
        leg.liveFairProb = prob;
        leg.liveFairProbUpdatedAt = new Date().toISOString();
        legsUpdated++;
      }
    }
  }

  // Rebuild exposure with new probs (if any legs were updated)
  if (legsUpdated > 0) {
    rebuildAllExposure();
  }

  log.info('LiveOdds', `Refreshed ${sportsRefreshed}/${sports.length} sports, updated ${legsUpdated} legs across ${inProgressGames} in-progress games`);
  return { sportsRefreshed, legsUpdated, inProgressGames, sportsWithInProgress: sports };
}

// ---------------------------------------------------------------------------
// SETTLEMENT DRIFT MONITOR — periodic PX vs local reconciliation that logs
// warnings on mismatch and exposes the most recent drift state via /drift-status.
// Runs independently of the settlement poll so it catches drift even when
// the poll is working correctly (e.g. a late WS event that contradicts
// an already-polled settlement).
// ---------------------------------------------------------------------------
const driftState = {
  lastCheckedAt: null,
  lastCheckDurationMs: null,
  lastError: null,
  pxSettledTotal: 0,
  matched: 0,
  mismatches: [],
  missingLocal: [],
  missingPx: [],
  history: [], // rolling log of drift events with parlayId + discovered-at timestamp
};

async function checkSettlementDrift(px) {
  const started = Date.now();
  try {
    // Reuse existing fetchOrders path — paginates to 1000 orders.
    const pxOrders = await px.fetchOrders(1000);
    const pxByParlayId = {};
    const pxByUuid = {};
    for (const o of pxOrders) {
      const pid = o.p_id || o.parlay_id;
      if (pid) pxByParlayId[pid] = o;
      if (o.order_uuid) pxByUuid[o.order_uuid] = o;
    }

    const mismatches = [];
    const missingLocal = [];
    const missingPx = [];
    let matched = 0;
    let pxSettledTotal = 0;

    // Scan PX-settled orders for mismatches and missing-local
    for (const pxOrder of pxOrders) {
      const pxStatus = pxOrder.settlement_status;
      if (!pxStatus || ['tbd', 'requested', 'none', ''].includes(pxStatus)) continue;
      pxSettledTotal++;
      const pid = pxOrder.p_id || pxOrder.parlay_id;
      const local = (pid && orders[pid]) || (pxOrder.order_uuid && ordersByUuid[pxOrder.order_uuid] ? orders[ordersByUuid[pxOrder.order_uuid]] : null);
      if (!local) {
        missingLocal.push({
          parlayId: pid,
          orderUuid: pxOrder.order_uuid,
          pxStatus,
          pxProfit: pxOrder.profit != null ? Number(pxOrder.profit) : null,
          pxStake: pxOrder.stake != null ? Number(pxOrder.stake) : null,
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
        matched++;
      }
    }

    // Scan locally-settled orders that PX doesn't see
    for (const local of Object.values(orders)) {
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

    const anyDrift = mismatches.length + missingLocal.length + missingPx.length;

    // Log only when drift is detected (don't spam green checks)
    if (anyDrift > 0) {
      log.warn('Drift', `PX vs local: ${mismatches.length} mismatches, ${missingLocal.length} missing-local, ${missingPx.length} missing-px (matched: ${matched}/${pxSettledTotal})`);
      // Log up to 3 samples per bucket for diagnostic clarity
      for (const m of mismatches.slice(0, 3)) {
        log.warn('Drift', `  mismatch ${m.parlayId}: ${JSON.stringify({ status: m.status, pnl: m.pnl, stake: m.stake })}`);
      }
      for (const m of missingLocal.slice(0, 3)) {
        log.warn('Drift', `  missing-local ${m.parlayId}: px=${m.pxStatus} profit=${m.pxProfit}`);
      }
      for (const m of missingPx.slice(0, 3)) {
        log.warn('Drift', `  missing-px ${m.parlayId}: local=${m.localStatus} pnl=${m.localPnl}`);
      }
      // Record novel drift events in history for UI visibility
      const seen = new Set((driftState.history || []).map(h => h.key));
      for (const m of mismatches) {
        const key = 'mm:' + m.parlayId;
        if (!seen.has(key)) {
          driftState.history.push({
            key,
            type: 'mismatch',
            parlayId: m.parlayId,
            detail: m,
            detectedAt: new Date().toISOString(),
          });
        }
      }
      for (const m of missingLocal) {
        const key = 'ml:' + m.parlayId;
        if (!seen.has(key)) {
          driftState.history.push({ key, type: 'missing_local', parlayId: m.parlayId, detail: m, detectedAt: new Date().toISOString() });
        }
      }
      for (const m of missingPx) {
        const key = 'mp:' + m.parlayId;
        if (!seen.has(key)) {
          driftState.history.push({ key, type: 'missing_px', parlayId: m.parlayId, detail: m, detectedAt: new Date().toISOString() });
        }
      }
      // Cap history at 200 entries (drop oldest)
      if (driftState.history.length > 200) {
        driftState.history = driftState.history.slice(-200);
      }
    } else {
      log.debug('Drift', `PX vs local clean (${matched}/${pxSettledTotal} matched)`);
    }

    driftState.lastCheckedAt = new Date().toISOString();
    driftState.lastCheckDurationMs = Date.now() - started;
    driftState.lastError = null;
    driftState.pxSettledTotal = pxSettledTotal;
    driftState.matched = matched;
    driftState.mismatches = mismatches;
    driftState.missingLocal = missingLocal;
    driftState.missingPx = missingPx;

    return {
      pxSettledTotal,
      matched,
      mismatches: mismatches.length,
      missingLocal: missingLocal.length,
      missingPx: missingPx.length,
      totalDrift: anyDrift,
    };
  } catch (err) {
    driftState.lastError = err.message;
    driftState.lastCheckedAt = new Date().toISOString();
    driftState.lastCheckDurationMs = Date.now() - started;
    log.warn('Drift', `Drift check failed: ${err.message}`);
    return { error: err.message };
  }
}

function getDriftState() {
  return {
    ...driftState,
    // Summarize counts at the top level for quick dashboard read
    mismatchCount: driftState.mismatches?.length || 0,
    missingLocalCount: driftState.missingLocal?.length || 0,
    missingPxCount: driftState.missingPx?.length || 0,
  };
}

async function pollOrderSettlements(px) {
  const confirmed = Object.values(orders).filter(o => o.status === 'confirmed' && o.orderUuid);
  if (confirmed.length === 0) {
    log.debug('Poll', 'No confirmed orders to check');
    return { checked: 0, settled: 0 };
  }

  log.info('Poll', `Checking ${confirmed.length} confirmed orders for settlement...`);

  try {
    // Fetch all our orders from PX (high limit to catch older settlements)
    const pxOrders = await px.fetchOrders(500);
    let settled = 0;

    // Pre-fetch orders from Supabase so reconstructed orders preserve pricing data
    const dbFallback = {};
    try {
      const missingIds = [];
      for (const pxOrder of pxOrders) {
        const pid = pxOrder.p_id || pxOrder.parlay_id;
        if (!pid) continue;
        const existing = orders[pid] || (pxOrder.order_uuid && ordersByUuid[pxOrder.order_uuid] ? orders[ordersByUuid[pxOrder.order_uuid]] : null);
        if (!existing) missingIds.push(pid);
      }
      if (missingIds.length > 0) {
        const dbRows = await db.loadOrdersByParlayIds(missingIds);
        Object.assign(dbFallback, dbRows);
        log.info('Poll', `Pre-fetched ${Object.keys(dbRows).length}/${missingIds.length} orders from Supabase for pricing preservation`);
      }
    } catch (err) {
      log.warn('Poll', `Supabase pricing pre-fetch failed: ${err.message}`);
    }

    for (const pxOrder of pxOrders) {
      const uuid = pxOrder.order_uuid;
      if (!uuid) continue;

      // Try UUID index first, then fallback to parlay_id match
      let parlayId = ordersByUuid[uuid];
      let order = parlayId ? orders[parlayId] : null;

      // Fallback: match by parlay_id. PX uses `p_id` as the canonical field name.
      const pxParlayId = pxOrder.p_id || pxOrder.parlay_id;
      if (!order && pxParlayId && orders[pxParlayId]) {
        order = orders[pxParlayId];
        parlayId = pxParlayId;
        // Backfill the UUID so future lookups work
        order.orderUuid = uuid;
        ordersByUuid[uuid] = parlayId;
        log.info('Poll', `Backfilled UUID for parlay ${parlayId}: ${uuid}`);
        db.saveOrder(order).catch(() => {});
      }

      // If still no match: reconstruct the order from PX data so P&L is captured.
      // This handles cases where we missed the confirmation WS event entirely
      // (e.g., service was down) but PX knows about the settled order.
      // Only reconstruct if we have a Supabase record (i.e., we actually quoted
      // it). Without this guard, every PX order we never quoted gets imported as
      // a skeleton on each poll cycle, corrupting P&L.
      if (!order && pxParlayId) {
        const dbOrder = dbFallback[pxParlayId];
        if (!dbOrder) {
          log.debug('Poll', `Skipping PX order ${pxParlayId} — no Supabase record (never quoted by us)`);
          continue;
        }
        const settlementStatus = pxOrder.settlement_status;
        if (settlementStatus && !['tbd','requested'].includes(settlementStatus)) {
          log.info('Poll', `Reconstructing missing settled order ${pxParlayId} (uuid=${uuid})`);
          // Enrich legs from lineManager where possible
          const lineManager = require('./line-manager');
          const enrichedLegs = (pxOrder.legs || []).map(l => {
            const info = lineManager.lookupLine(l.line_id);
            const eventName = l.sport_event_id ? lineManager.getEventName(l.sport_event_id) : null;
            let team = info?.teamName || '?';
            // If we only have the event name, use that as team context for totals/spreads
            if (!info && eventName) team = eventName;
            // Totals display prefix (over/under) when we know the market type
            if (info?.marketType === 'total' && info?.homeTeam && info?.awayTeam) {
              team = `${team} (${info.awayTeam} @ ${info.homeTeam})`;
            }
            return {
              lineId: l.line_id,
              sport: info?.sport || 'unknown',
              team,
              teamName: info?.teamName || team,
              market: info?.marketType || null,
              marketType: info?.marketType || null,
              line: l.line,
              selection: info?.selection || null,
              homeTeam: info?.homeTeam || null,
              awayTeam: info?.awayTeam || null,
              pxEventId: l.sport_event_id,
              pxEventName: eventName || null,
              startTime: info?.startTime || null,
              settlementStatus: l.settlement_status,
              settlement_status: l.settlement_status,
            };
          });
          const dbOrder = dbFallback[pxParlayId];
          const pxOdds = pxOrder.confirmed_odds != null ? Number(pxOrder.confirmed_odds) : null;
          const pxStake = pxOrder.confirmed_stake != null ? Number(pxOrder.confirmed_stake) : null;

          // Prefer DB legs when they have richer data (same logic as fullPxReconcile)
          const dbLegs = dbOrder?.legs;
          const dbLegsRicher = Array.isArray(dbLegs) && dbLegs.length > 0
            && dbLegs.some(l => l.fairProb != null || l.market != null || (l.sport && l.sport !== 'unknown'));
          let mergedLegs;
          if (dbLegsRicher) {
            mergedLegs = dbLegs.map(dbLeg => {
              const pxLeg = enrichedLegs.find(el => el.lineId === dbLeg.lineId);
              if (pxLeg) {
                return {
                  ...dbLeg,
                  settlementStatus: pxLeg.settlementStatus || dbLeg.settlementStatus,
                  settlement_status: pxLeg.settlement_status || dbLeg.settlement_status,
                  inferredResult: pxLeg.inferredResult || dbLeg.inferredResult,
                };
              }
              return dbLeg;
            });
          } else {
            mergedLegs = enrichedLegs;
          }

          order = {
            parlayId: pxParlayId,
            status: dbOrder?.status || 'confirmed', // will be set to settled_* below by recordSettlement
            legs: mergedLegs,
            offeredOdds: dbOrder?.offeredOdds ?? (pxOdds != null ? -pxOdds : null),
            fairParlayProb: dbOrder?.fairParlayProb ?? null,
            maxRisk: dbOrder?.maxRisk ?? null,
            vig: dbOrder?.vig ?? null,
            confirmedOdds: dbOrder?.confirmedOdds ?? pxOdds,
            confirmedStake: dbOrder?.confirmedStake ?? pxStake,
            orderUuid: dbOrder?.orderUuid || uuid,
            quotedAt: dbOrder?.quotedAt || null,
            confirmedAt: dbOrder?.confirmedAt || new Date((pxOrder.updated_at || 0) * 1000).toISOString(),
            settledAt: dbOrder?.settledAt || null,
            pnl: dbOrder?.pnl ?? null,
            settlementResult: dbOrder?.settlementResult || null,
            meta: dbOrder?.meta && Object.keys(dbOrder.meta).length > 1
              ? { ...dbOrder.meta, legs: mergedLegs }
              : { reconstructed: true, legs: mergedLegs },
          };
          orders[pxParlayId] = order;
          ordersByUuid[uuid] = pxParlayId;
          parlayId = pxParlayId;
          // Persist to DB
          db.saveOrder(order).catch(err => log.error('DB', `saveOrder(reconstructed) failed: ${err.message}`));
        }
      }

      if (!order) continue;
      // If already settled with matching status, still check if PX's profit
      // field disagrees with our stored pnl — this catches push-reduced
      // parlays where our derivation produced the wrong magnitude.
      if (order.status === `settled_${pxOrder.settlement_status}`) {
        const pxProfit = pxOrder.profit != null ? Number(pxOrder.profit) : null;
        if (pxProfit != null && order.pnl != null && Math.abs(pxProfit - order.pnl) > 0.01) {
          log.info('Poll', `Fixing stale pnl for ${order.parlayId}: was ${order.pnl}, PX profit ${pxProfit}`);
          stats.runningPnL -= order.pnl;
          order.pnl = pxProfit;
          order.pxProfit = pxProfit;
          stats.runningPnL += pxProfit;
          db.saveOrder(order).catch(() => {});
        }
        continue;
      }

      const settlementStatus = pxOrder.settlement_status;
      if (!settlementStatus || settlementStatus === 'tbd' || settlementStatus === 'requested') continue;

      // If order was somehow reverted to non-settled status, fix it
      // by temporarily clearing the settled status so recordSettlement will run.
      // Enriched log captures the exact mismatch (stored vs expected) plus
      // whether this is a reconstructed order, so recurring hits can be
      // traced to a specific root cause (casing drift, parlayId mismatch,
      // restart-induced reconstruction, etc.) via Railway logs.
      if (order.status && order.status.startsWith('settled_')) {
        const expected = `settled_${settlementStatus}`;
        const reconstructed = !!(order.meta && order.meta.reconstructed);
        log.warn('Orders', `Fixing stale settlement for ${order.parlayId} uuid=${uuid}: stored=${order.status} expected=${expected} pxRaw=${JSON.stringify(settlementStatus)} reconstructed=${reconstructed} prevSettledAt=${order.settledAt || 'null'} prevPnl=${order.pnl}`);
        // Reverse the old P&L before re-settling
        if (order.pnl != null) stats.runningPnL -= order.pnl;
        if (order.pnl > 0) stats.totalWins--;
        else if (order.pnl < 0) stats.totalLosses--;
        stats.totalSettlements--;
        order.status = 'confirmed';
        order.pnl = null;
      }

      // Backfill stake/odds from PX if missing (critical for recovering lost settlements)
      if (!order.confirmedStake && pxOrder.stake != null) order.confirmedStake = Number(pxOrder.stake);
      if (!order.confirmedOdds && pxOrder.odds != null) order.confirmedOdds = Number(pxOrder.odds);
      if (!order.confirmedStake && pxOrder.confirmed_stake != null) order.confirmedStake = Number(pxOrder.confirmed_stake);
      if (!order.confirmedOdds && pxOrder.confirmed_odds != null) order.confirmedOdds = Number(pxOrder.confirmed_odds);

      // Reject bogus settlements where any leg hasn't finished. This matters
      // only for SP LOST (bettor's parlay hit) — SP can't legitimately lose
      // a parlay until every leg has played. For SP WON (bettor's parlay
      // busted), PX can settle early as soon as ANY leg losses — the parlay
      // is dead and PX's early settlement is correct. Verify via PX leg-level
      // settlement data: if the SP-won order contains at least one lost leg
      // per PX, accept the settlement immediately.
      if (settlementStatus === 'lost') {
        // Trust PX's leg-level settlement first. If every PX leg has a
        // terminal settlement_status (won/lost/push/void), the parlay
        // is genuinely resolved and our 4-hour start-time heuristic
        // should not veto it. The heuristic only exists to guard
        // against the historical case where PX returned 'lost' before
        // the game actually finished — which cannot be true when PX's
        // own leg resolutions are complete.
        const terminalLegStatuses = new Set(['won', 'lost', 'push', 'void']);
        const pxLegs = Array.isArray(pxOrder.legs) ? pxOrder.legs : [];
        const allPxLegsTerminal = pxLegs.length > 0 &&
          pxLegs.every(l => terminalLegStatuses.has(l.settlement_status));
        if (!allPxLegsTerminal) {
          const orderLegs = order.legs || order.meta?.legs || [];
          const now = Date.now();
          const anyUnfinished = orderLegs.some(l => {
            const st = l.startTime || l.start_time;
            if (!st) return false;
            const startMs = new Date(st).getTime();
            if (startMs > now) return true;
            if ((now - startMs) < 4 * 3600 * 1000) return true;
            return false;
          });
          if (anyUnfinished) {
            log.warn('Poll', `Skipping bogus lost settlement for ${order.parlayId}: leg(s) not yet finished and PX legs incomplete`);
            continue;
          }
        }
      } else if (settlementStatus === 'won') {
        // For 'won', only accept if PX has at least one leg marked lost.
        // If no PX leg is marked lost but we'd still have an unfinished leg,
        // something is off — skip to avoid recording a fake win.
        const pxHasLostLeg = (pxOrder.legs || []).some(l => l.settlement_status === 'lost');
        if (!pxHasLostLeg) {
          const orderLegs = order.legs || order.meta?.legs || [];
          const now = Date.now();
          const anyUnfinished = orderLegs.some(l => {
            const st = l.startTime || l.start_time;
            if (!st) return false;
            const startMs = new Date(st).getTime();
            if (startMs > now) return true;
            if ((now - startMs) < 4 * 3600 * 1000) return true;
            return false;
          });
          if (anyUnfinished) {
            log.warn('Poll', `Skipping suspicious won settlement for ${order.parlayId}: no PX lost leg + unfinished game`);
            continue;
          }
        }
      }

      // PX REST API settlement_status is SP-perspective — use directly.
      // Pass trusted:true because we already validated the settlement via
      // pxOrder.legs above; recordSettlement should not re-apply its own
      // defensive 4-hour startTime recheck.
      recordSettlement(uuid, settlementStatus, pxOrder.profit || 0, { trusted: true });
      settled++;

      // Update per-leg settlement from PX response
      if (pxOrder.legs && Array.isArray(pxOrder.legs)) {
        for (const pxLeg of pxOrder.legs) {
          if (pxLeg.line_id && pxLeg.settlement_status) {
            recordLegSettlement(uuid, pxLeg);
          }
        }
      }
    }

    log.info('Poll', `Settlement poll: checked ${pxOrders.length} PX orders, settled ${settled}`);
    return { checked: pxOrders.length, settled };
  } catch (err) {
    log.error('Poll', `Settlement poll failed: ${err.message}`);
    return { checked: 0, settled: 0, error: err.message };
  }
}

/**
 * Check game results for all active positions and infer leg outcomes.
 * Uses The Odds API scores endpoint to detect wins/losses before PX settles.
 * Sets leg.inferredResult = 'won' | 'lost' on each resolved leg.
 */

/**
 * Find and revert any settled orders where NO leg could possibly have resolved.
 *
 * CRITICAL: previous version used legs.some(unfinished) which destroyed
 * legitimate early settlements (e.g. 3-leg parlay where leg 1 busted bettor-
 * side, making SP winner before legs 2 and 3 play out). That bug has been
 * running every 2 minutes in a background loop, accumulating drift against
 * PX REST ground truth. Observed impact: our in-memory P&L was off from
 * PX by ~$4,700 with 151 settled parlays missing.
 *
 * Fixed: only revert when EVERY leg is still unfinished (future or within
 * 4h of start). If any leg has had time to resolve, the parlay could
 * legitimately have settled via early-win logic.
 */
function revertBogusSettlements() {
  const now = Date.now();
  let reverted = 0;
  for (const o of Object.values(orders)) {
    if (!o.status || !o.status.startsWith('settled_')) continue;
    const legs = o.legs || o.meta?.legs || [];
    if (legs.length === 0) continue;
    const allUnfinished = legs.every(l => {
      const st = l.startTime || l.start_time;
      if (!st) return false; // unknown start time — don't vote unfinished
      const startMs = new Date(st).getTime();
      if (startMs > now) return true; // future — hasn't started
      if ((now - startMs) < 4 * 3600 * 1000) return true; // started < 4h ago — likely in progress
      return false;
    });
    if (allUnfinished) {
      log.warn('Orders', `Reverting bogus settlement: ${o.parlayId} (was ${o.status}, pnl=${o.pnl}) — ALL legs still unfinished`);
      // Reverse stats
      if (o.pnl != null) stats.runningPnL -= o.pnl;
      if (o.pnl > 0) stats.totalWins--;
      else if (o.pnl < 0) stats.totalLosses--;
      stats.totalSettlements--;
      // Revert to confirmed
      o.status = 'confirmed';
      o.settlementResult = null;
      o.pnl = null;
      o.settledAt = null;
      stats.totalConfirmations++;
      addExposure(o);
      db.saveOrder(o).catch(() => {});
      reverted++;
    }
  }
  if (reverted > 0) log.info('Orders', `Reverted ${reverted} bogus settlements`);
  return { reverted };
}

/**
 * Reconcile settled orders: if leg-level data (inferredResult / settlementStatus)
 * derives a DIFFERENT SP result than what's stored, correct it.
 * This catches cases where PX settlement was wrong or score detection updated legs
 * after the initial settlement was recorded.
 */
function reconcileSettlements() {
  let corrected = 0;
  for (const o of Object.values(orders)) {
    if (!o.status || !o.status.startsWith('settled_')) continue;

    // Collect leg statuses. PX settlementStatus is authoritative — check it in
    // EVERY source before falling back to inferredResult in any source.
    // Previously we broke on the first source that had ANY status, which meant
    // if meta.legs had only inferredResult and o.legs had settlementStatus from
    // a late WS event, we'd use the stale inferredResult and skip the correction.
    const legSources = [o.legs, o.meta?.legs].filter(Boolean);
    const legStatuses = [];
    const primaryLegs = legSources.reduce((a, b) => (a || []).length >= (b || []).length ? a : b, []) || [];
    for (let li = 0; li < primaryLegs.length; li++) {
      let st = null;
      const pLeg = primaryLegs[li];
      const pTeam = pLeg?.team || pLeg?.teamName;
      const pLineId = pLeg?.lineId || pLeg?.line_id;

      // Helper: find the matching leg in a source by index first, then lineId, then team
      const findMatching = (src) => {
        const byIdx = src[li];
        if (byIdx && ((byIdx.lineId || byIdx.line_id) === pLineId || (byIdx.team || byIdx.teamName) === pTeam)) return byIdx;
        if (pLineId) {
          const byLineId = src.find(l => (l.lineId || l.line_id) === pLineId);
          if (byLineId) return byLineId;
        }
        if (pTeam) {
          const byTeam = src.find(l => (l.team || l.teamName) === pTeam);
          if (byTeam) return byTeam;
        }
        return byIdx;
      };

      // Pass 1: prefer PX settlementStatus from ANY source
      for (const src of legSources) {
        const l = findMatching(src);
        if (l?.settlementStatus || l?.settlement_status) {
          st = l.settlementStatus || l.settlement_status;
          break;
        }
      }
      // Pass 2: fall back to our scraped inferredResult only if no PX status anywhere
      if (!st) {
        for (const src of legSources) {
          const l = findMatching(src);
          if (l?.inferredResult) { st = l.inferredResult; break; }
        }
      }
      if (st) legStatuses.push(st);
    }
    if (legStatuses.length === 0) continue; // no leg data to reconcile against

    // Derive correct SP result from legs (bettor-perspective leg data).
    // Only derive when the leg pattern is unambiguous. PX has inconsistent
    // handling of won+push mixed parlays (sometimes 'lost', sometimes 'push')
    // so we don't try to guess — leave those to pollOrderSettlements which
    // fetches PX's authoritative order-level decision directly.
    //   - Any leg LOST → bettor's parlay busted → SP WON (always, even
    //     with other legs missing status — one loss is enough to kill)
    //   - ALL legs have status AND all WON (no pushes) → bettor hit
    //     everything → SP LOST
    //   - ALL legs have status AND all PUSHED → stake refunded → PUSH
    //   - Otherwise → don't derive. Critically: if any leg's status is
    //     MISSING we cannot assume it won just because the known legs
    //     won. Observed bug: a 4-leg parlay where the Royals +1.5 leg's
    //     status was missing while the other 3 were 'won' triggered a
    //     false "allWon → SP lost" derivation, overriding PX's correct
    //     SP-won settlement (pxProfit=+\$200) with pnl=-\$896.
    const anyLegLost = legStatuses.some(s => s === 'lost');
    const haveAllLegStatuses = legStatuses.length === primaryLegs.length;
    const allWon = haveAllLegStatuses
      && legStatuses.length > 0
      && legStatuses.every(s => s === 'won');
    const allPushed = haveAllLegStatuses
      && legStatuses.length > 0
      && legStatuses.every(s => s === 'push' || s === 'void');
    let derivedResult;
    if (anyLegLost) {
      derivedResult = 'won';
    } else if (allWon) {
      derivedResult = 'lost';
    } else if (allPushed) {
      derivedResult = 'push';
    } else {
      continue; // incomplete coverage OR mixed won+push — let PX poll decide
    }

    const storedResult = o.settlementResult || o.status.replace('settled_', '');
    if (derivedResult === storedResult) continue; // already correct

    log.warn('Reconcile', `Correcting settlement for ${o.parlayId}: was ${storedResult}, derived ${derivedResult} from ${legStatuses.length} legs`);

    // Reverse old stats
    if (o.pnl != null) stats.runningPnL -= o.pnl;
    if (o.pnl > 0) stats.totalWins--;
    else if (o.pnl < 0) stats.totalLosses--;

    // Apply corrected result
    o.status = `settled_${derivedResult}`;
    o.settlementResult = derivedResult;
    const bettorWager = americanOddsToProfit(o.confirmedOdds, o.confirmedStake);
    if (derivedResult === 'won') {
      o.pnl = bettorWager;
    } else if (derivedResult === 'lost') {
      o.pnl = -(o.confirmedStake || 0);
    } else {
      o.pnl = 0;
    }

    // Update stats
    stats.runningPnL += o.pnl;
    if (o.pnl > 0) stats.totalWins++;
    else if (o.pnl < 0) stats.totalLosses++;

    db.saveOrder(o).catch(() => {});
    corrected++;
  }
  if (corrected > 0) log.info('Reconcile', `Corrected ${corrected} settlements from leg data`);
  return { corrected };
}

async function checkLegResults() {
  const oddsFeed = require('./odds-feed');
  // Check both confirmed and settled orders — settled may have legs without inferredResult
  const toCheck = Object.values(orders).filter(o => o.status === 'confirmed' || o.status?.startsWith('settled_'));
  if (toCheck.length === 0) return { checked: 0, resolved: 0 };
  const confirmed = toCheck;

  // Collect unique sports that have active legs
  const activeSports = new Set();
  for (const o of confirmed) {
    const legs = o.meta?.legs || o.legs || [];
    for (const l of legs) {
      if (l.sport && !l.inferredResult) activeSports.add(l.sport);
    }
  }

  // Pre-fetch scores for all active sports
  for (const sport of activeSports) {
    await oddsFeed.fetchScores(sport);
  }

  let checked = 0, resolved = 0;

  for (const o of confirmed) {
    const legs = o.meta?.legs || o.legs || [];
    for (const l of legs) {
      // Re-validation strategy:
      //  1. If the game started <4h ago and inferredResult is set, clear it
      //     and recompute — protects against yesterday's value lingering
      //     when same-team teams play back-to-back days.
      //  2. Otherwise keep the existing value, BUT still run the fresh
      //     lookup. If the new score-based result definitively
      //     contradicts the stored inferredResult, override + log. This
      //     heals legs set wrong by an older buggy lookup (the ESPN
      //     time-disambiguation fix in commit 3448105 — pre-fix, a 5/2
      //     leg could be overwritten with 5/3's result for the same
      //     team-pair).
      //  3. If no existing inferredResult, run the lookup as usual.
      if (l.inferredResult) {
        const st = l.startTime || l.start_time;
        if (st) {
          const startMs = new Date(st).getTime();
          const now = Date.now();
          if (startMs > 0 && (now - startMs) < 4 * 3600 * 1000) {
            log.debug('Results', `Clearing stale inferredResult for ${l.team} (game started ${Math.round((now-startMs)/60000)}min ago)`);
            l.inferredResult = null;
          }
        }
      }
      if (!l.sport || !l.homeTeam || !l.awayTeam) continue;

      const result = await oddsFeed.getGameResult(l.sport, l.homeTeam, l.awayTeam, l.startTime);
      if (!result || !result.completed) continue;
      if (result.homeScore == null || result.awayScore == null) continue;

      checked++;
      const market = l.market || l.marketType;
      const selection = l.selection || l.oddsApiSelection;

      // Compute the fresh result from this score-based lookup.
      let freshResult = null;
      if (market === 'moneyline') {
        // Only set a result if we have a definitive winner. Unknown/missing winner
        // must NOT default to 'push' — moneylines in NBA/NHL/MLB can't push (OT,
        // shootout, extra innings). A silent push default caused us to record
        // pushed parlays as $0 P&L when they were actually losses.
        if (result.winner === 'home' || result.winner === 'away') {
          if (selection === 'home') freshResult = result.winner === 'home' ? 'won' : 'lost';
          else if (selection === 'away') freshResult = result.winner === 'away' ? 'won' : 'lost';
        }
      } else if (market === 'spread') {
        const line = l.line != null ? Number(l.line) : null;
        if (line != null) {
          const homeMargin = result.homeScore - result.awayScore;
          if (selection === 'home') {
            const adjusted = homeMargin + line;
            freshResult = adjusted > 0 ? 'won' : adjusted < 0 ? 'lost' : 'push';
          } else {
            const awayMargin = result.awayScore - result.homeScore;
            const adjusted = awayMargin + Math.abs(line);
            freshResult = adjusted > 0 ? 'won' : adjusted < 0 ? 'lost' : 'push';
          }
        }
      } else if (market === 'total') {
        const line = l.line != null ? Number(l.line) : null;
        if (line != null) {
          const total = result.homeScore + result.awayScore;
          if (selection === 'over') freshResult = total > line ? 'won' : total < line ? 'lost' : 'push';
          else if (selection === 'under') freshResult = total < line ? 'won' : total > line ? 'lost' : 'push';
        }
      }

      // Decide whether to apply freshResult.
      if (freshResult == null) continue; // inconclusive — leave whatever's there
      if (l.inferredResult && l.inferredResult !== freshResult) {
        log.warn('Results', `Overriding inferredResult for ${l.team} ${market}: stored=${l.inferredResult} → fresh=${freshResult} (${result.homeScore}-${result.awayScore}, startTime=${l.startTime})`);
      }
      l.inferredResult = freshResult;

      if (l.inferredResult) {
        resolved++;
        // Clear liveFairProb so the dashboard stops rendering a frozen
        // in-game probability next to a resolved leg. The dashboard
        // should display the result chip (won/lost/push) instead.
        l.liveFairProb = null;
        l.liveFairProbUpdatedAt = null;
        // Sync inferredResult + cleared live prob to o.legs as well (frontend may read either source)
        if (o.legs) {
          const matchingLeg = o.legs.find(ol => ol.lineId === l.lineId || ((ol.team || ol.teamName) === (l.team || l.teamName) && (ol.market || ol.marketType) === market));
          if (matchingLeg) {
            matchingLeg.inferredResult = l.inferredResult;
            matchingLeg.liveFairProb = null;
            matchingLeg.liveFairProbUpdatedAt = null;
          }
        }
        log.info('Results', `Leg resolved: ${l.team} ${market} → ${l.inferredResult} (${result.homeScore}-${result.awayScore})`);
      }
    }
  }

  log.info('Results', `Checked ${checked} legs, resolved ${resolved}`);

  // Immediately reconcile any settled orders whose leg data now disagrees with stored result
  if (resolved > 0) {
    reconcileSettlements();
  }

  return { checked, resolved };
}

/**
 * Full reconcile against PX REST: pull the ENTIRE order history from PX,
 * import/update each order in local state, and rebuild stats from scratch.
 *
 * This is the authoritative recovery path when local state has drifted
 * from PX (e.g. from the revertBogusSettlements bug that was silently
 * destroying legitimate settlements every 2 minutes).
 *
 * Flow:
 *   1. Fetch all PX orders via fetchOrders with a large cap
 *   2. For each PX order:
 *      a. If we already have it locally, update status/stake/odds to match PX
 *      b. If we don't have it, reconstruct a skeleton order from PX data
 *      c. If PX has it settled, call recordLegSettlement per leg then
 *         recordSettlement for the parlay (so leg context is preserved)
 *   3. After upserting everything, reset stats counters and rebuild them
 *      from the current in-memory `orders` map (ground truth)
 *
 * Returns a summary object with before/after counts and P&L.
 */
async function fullPxReconcile(px) {
  const before = {
    totalQuotes: stats.totalQuotes,
    totalConfirmations: stats.totalConfirmations,
    totalSettlements: stats.totalSettlements,
    totalWins: stats.totalWins,
    totalLosses: stats.totalLosses,
    runningPnL: stats.runningPnL,
    ordersInMemory: Object.keys(orders).length,
  };

  log.info('Reconcile', 'Starting full PX reconcile — fetching all PX orders...');
  const pxOrders = await px.fetchOrders(10000); // exhaust PX history
  log.info('Reconcile', `Fetched ${pxOrders.length} PX orders`);

  let imported = 0;
  let updated = 0;
  let settled = 0;

  const lineManager = require('./line-manager');

  // Pre-fetch line_cache for any line_ids not in the live lineIndex.
  // Historical events get purged from the in-memory index, but line_cache
  // in Supabase retains team names/sport/homeTeam/awayTeam from when the
  // line was last registered. Without this, reconstructed orders for expired
  // events get team='?' and show "Event XXXXXXX" in the exposure table.
  const lineCacheFallback = {};
  try {
    const allLineIds = new Set();
    for (const pxOrder of pxOrders) {
      const pid = pxOrder.p_id || pxOrder.parlay_id;
      if (!pid) continue;
      // Only need line_cache for NEW orders (not already in memory)
      const existing = orders[pid] || (pxOrder.order_uuid && ordersByUuid[pxOrder.order_uuid] ? orders[ordersByUuid[pxOrder.order_uuid]] : null);
      if (existing) continue;
      for (const l of pxOrder.legs || []) {
        if (l.line_id && !lineManager.lookupLine(l.line_id)) {
          allLineIds.add(l.line_id);
        }
      }
    }
    if (allLineIds.size > 0) {
      const cached = await db.loadLineCacheBulk([...allLineIds]);
      Object.assign(lineCacheFallback, cached);
      log.info('Reconcile', `Pre-fetched ${Object.keys(cached).length}/${allLineIds.size} unresolved line_ids from line_cache`);
    }
  } catch (err) {
    log.warn('Reconcile', `line_cache pre-fetch failed: ${err.message}`);
  }

  // Pre-fetch orders from Supabase for PX orders not in memory.
  // This preserves pricing data (offeredOdds, fairParlayProb, vig, etc.)
  // that was saved at quote time but lost from memory on restart.
  // Without this, reconstructed orders overwrite Supabase rows with nulls.
  const dbFallback = {};
  try {
    const missingIds = [];
    for (const pxOrder of pxOrders) {
      const pid = pxOrder.p_id || pxOrder.parlay_id;
      if (!pid) continue;
      const existing = orders[pid] || (pxOrder.order_uuid && ordersByUuid[pxOrder.order_uuid] ? orders[ordersByUuid[pxOrder.order_uuid]] : null);
      if (!existing) missingIds.push(pid);
    }
    if (missingIds.length > 0) {
      const dbRows = await db.loadOrdersByParlayIds(missingIds);
      Object.assign(dbFallback, dbRows);
      log.info('Reconcile', `Pre-fetched ${Object.keys(dbRows).length}/${missingIds.length} orders from Supabase for pricing preservation`);
    }
  } catch (err) {
    log.warn('Reconcile', `Supabase pricing pre-fetch failed: ${err.message}`);
  }

  for (const pxOrder of pxOrders) {
    const uuid = pxOrder.order_uuid;
    if (!uuid) continue;
    const pxParlayId = pxOrder.p_id || pxOrder.parlay_id;
    if (!pxParlayId) continue;

    // Locate or construct the local order
    let order = orders[pxParlayId] || (ordersByUuid[uuid] ? orders[ordersByUuid[uuid]] : null);
    const wasNew = !order;

    if (!order) {
      // Check Supabase for a previously-saved version with pricing data.
      // If found, use it as the base and only backfill missing fields from PX.
      // If NOT found, skip — this is a PX order we never quoted, so there's
      // no pricing data to reconstruct. Importing it would create a skeleton
      // with null odds/stake/fair that pollutes the dashboard and inflates
      // the active order count.
      const dbOrder = dbFallback[pxParlayId];
      if (!dbOrder) {
        log.debug('Reconcile', `Skipping PX order ${pxParlayId} — no Supabase record (never quoted by us)`);
        continue;
      }

      // Reconstruct skeleton from PX data (mirrors pollOrderSettlements logic).
      // Uses lineManager (live index) first, then lineCacheFallback (Supabase)
      // for expired events no longer in the live index.
      const enrichedLegs = (pxOrder.legs || []).map(l => {
        const info = lineManager.lookupLine(l.line_id) || lineCacheFallback[l.line_id] || null;
        const eventName = l.sport_event_id ? lineManager.getEventName(l.sport_event_id) : null;
        let team = info?.teamName || '?';
        if (!info && eventName) team = eventName;
        if (info?.marketType === 'total' && info?.homeTeam && info?.awayTeam) {
          team = `${team} (${info.awayTeam} @ ${info.homeTeam})`;
        }
        return {
          lineId: l.line_id,
          sport: info?.sport || info?.oddsApiSport || 'unknown',
          team,
          teamName: info?.teamName || team,
          market: info?.marketType || null,
          marketType: info?.marketType || null,
          line: l.line,
          selection: info?.selection || info?.oddsApiSelection || null,
          homeTeam: info?.homeTeam || null,
          awayTeam: info?.awayTeam || null,
          pxEventId: l.sport_event_id,
          pxEventName: eventName || info?.pxEventName || null,
          startTime: info?.startTime || null,
          settlementStatus: l.settlement_status,
          settlement_status: l.settlement_status,
        };
      });

      // Merge: Supabase pricing data takes priority (saved at quote time),
      // PX REST fills in confirmation fields if Supabase is missing them.
      const pxOdds = pxOrder.confirmed_odds != null ? Number(pxOrder.confirmed_odds) : null;
      const pxStake = pxOrder.confirmed_stake != null ? Number(pxOrder.confirmed_stake) : null;

      // Prefer DB legs when they have richer data (fairProb, market type,
      // team names, book odds). The enrichedLegs skeleton from PX REST only
      // has lineId, line, and settlement_status — it lacks all pricing data.
      // Without this, every restart destroys the pricing detail for all
      // historical orders, leaving team='?', market=null, fairProb=null.
      const dbLegs = dbOrder?.legs;
      const dbLegsAreRicher = Array.isArray(dbLegs) && dbLegs.length > 0
        && dbLegs.some(l => l.fairProb != null || l.market != null || (l.sport && l.sport !== 'unknown'));
      let mergedLegs;
      if (dbLegsAreRicher) {
        // Start from DB legs (which have full pricing data), then backfill
        // settlement status from PX REST (which has the authoritative result).
        mergedLegs = dbLegs.map(dbLeg => {
          const pxLeg = enrichedLegs.find(el => el.lineId === dbLeg.lineId);
          if (pxLeg) {
            return {
              ...dbLeg,
              settlementStatus: pxLeg.settlementStatus || dbLeg.settlementStatus,
              settlement_status: pxLeg.settlement_status || dbLeg.settlement_status,
              inferredResult: pxLeg.inferredResult || dbLeg.inferredResult,
            };
          }
          return dbLeg;
        });
      } else {
        mergedLegs = enrichedLegs;
      }

      order = {
        parlayId: pxParlayId,
        status: dbOrder?.status || 'confirmed',
        legs: mergedLegs,
        offeredOdds: dbOrder?.offeredOdds ?? (pxOdds != null ? -pxOdds : null),
        fairParlayProb: dbOrder?.fairParlayProb ?? null,
        maxRisk: dbOrder?.maxRisk ?? null,
        vig: dbOrder?.vig ?? null,
        confirmedOdds: dbOrder?.confirmedOdds ?? pxOdds,
        confirmedStake: dbOrder?.confirmedStake ?? pxStake,
        orderUuid: dbOrder?.orderUuid || uuid,
        quotedAt: dbOrder?.quotedAt || null,
        confirmedAt: dbOrder?.confirmedAt || new Date((pxOrder.updated_at || 0) * 1000).toISOString(),
        settledAt: dbOrder?.settledAt || null,
        pnl: dbOrder?.pnl ?? null,
        settlementResult: dbOrder?.settlementResult || null,
        meta: dbOrder?.meta && Object.keys(dbOrder.meta).length > 1
          ? { ...dbOrder.meta, legs: mergedLegs }
          : { reconstructed: true, legs: mergedLegs },
      };
      orders[pxParlayId] = order;
      ordersByUuid[uuid] = pxParlayId;
      imported++;
    } else {
      // Update stake/odds/uuid from PX if missing
      if (!order.orderUuid && uuid) {
        order.orderUuid = uuid;
        ordersByUuid[uuid] = pxParlayId;
      }
      if (!order.confirmedStake && pxOrder.confirmed_stake != null) {
        order.confirmedStake = Number(pxOrder.confirmed_stake);
      }
      if (!order.confirmedOdds && pxOrder.confirmed_odds != null) {
        order.confirmedOdds = Number(pxOrder.confirmed_odds);
      }
      updated++;
    }

    // Apply settlement if PX says this order is settled
    const pxStatus = pxOrder.settlement_status;
    const isSettledOnPx = pxStatus && ['won', 'lost', 'push', 'void'].includes(pxStatus);
    if (!isSettledOnPx) {
      // PX has it confirmed/pending — ensure our status is 'confirmed' and
      // PERSIST to DB. Without this, reconstructed confirmed orders only
      // exist in memory and vanish on next restart, recreating the drift.
      if (!order.status?.startsWith('settled_')) {
        order.status = order.status || 'confirmed';
      }
      if (wasNew && !isOrderFinished(order)) {
        // Register exposure for newly-reconstructed confirmed orders so the
        // exposure tracker includes them in portfolio calculations.
        // Skip finished orders (all legs started) — they inflate exposure
        // with phantom risk from games that are already over.
        addExposure(order);
      }
      db.saveOrder(order).catch(err => log.error('Reconcile', `saveOrder(unsettled) failed for ${pxParlayId}: ${err.message}`));
      continue;
    }

    // Backfill leg settlement data from PX before calling recordSettlement
    // so loadFromDb's revert heuristic (which looks at leg statuses) has
    // complete data and won't re-revert on next reload.
    if (pxOrder.legs && Array.isArray(pxOrder.legs)) {
      for (const pxLeg of pxOrder.legs) {
        if (pxLeg.line_id && pxLeg.settlement_status) {
          recordLegSettlement(uuid, pxLeg);
        }
      }
    }

    // If already marked settled with matching status, fix pnl if PX disagrees
    if (order.status === `settled_${pxStatus}`) {
      const pxProfit = pxOrder.profit != null ? Number(pxOrder.profit) : null;
      if (pxProfit != null && order.pnl != null && Math.abs(pxProfit - order.pnl) > 0.01) {
        log.info('Reconcile', `Fixing stale pnl for ${pxParlayId}: was ${order.pnl}, PX says ${pxProfit}`);
        order.pnl = pxProfit;
        order.pxProfit = pxProfit;
        db.saveOrder(order).catch(() => {});
      }
      continue;
    }

    // Clear any stale settled_* status so recordSettlement re-runs cleanly
    if (order.status?.startsWith('settled_')) {
      order.status = 'confirmed';
      order.pnl = null;
    }

    // Record the settlement. recordSettlement will update status, pnl,
    // stats counters, and persist to DB.
    recordSettlement(uuid, pxStatus, pxOrder.profit || 0);
    settled++;
  }

  // After all upserts, REBUILD stats counters from scratch to eliminate drift.
  // The recordSettlement path has been incrementing stats throughout the loop,
  // but those increments were on top of whatever drifted stats existed before.
  // Resetting + rebuilding guarantees stats match what's actually in `orders`.
  log.info('Reconcile', 'Rebuilding stats from in-memory orders...');
  stats.totalQuotes = 0;
  stats.totalConfirmations = 0;
  stats.totalRejections = 0;
  stats.totalSettlements = 0;
  stats.totalWins = 0;
  stats.totalLosses = 0;
  stats.runningPnL = 0;
  for (const o of Object.values(orders)) {
    stats.totalQuotes++;
    // Phantom-flagged orders never produced a real confirm on PX; treat
    // them as rejections for the lifetime-count stats so totalConfirmations
    // reflects real fills only.
    if (o.meta && o.meta.phantom) {
      stats.totalRejections++;
      continue;
    }
    if (o.status === 'confirmed') stats.totalConfirmations++;
    else if (o.status === 'rejected') stats.totalRejections++;
    else if (o.status?.startsWith('settled_')) {
      stats.totalSettlements++;
      if (o.pnl != null) {
        stats.runningPnL += o.pnl;
        if (o.pnl > 0) stats.totalWins++;
        else if (o.pnl < 0) stats.totalLosses++;
      }
    }
  }

  const after = {
    totalQuotes: stats.totalQuotes,
    totalConfirmations: stats.totalConfirmations,
    totalSettlements: stats.totalSettlements,
    totalWins: stats.totalWins,
    totalLosses: stats.totalLosses,
    runningPnL: Math.round(stats.runningPnL * 100) / 100,
    ordersInMemory: Object.keys(orders).length,
  };

  log.info('Reconcile', `Done: ${imported} imported, ${updated} updated, ${settled} settled. New P&L: $${after.runningPnL}`);

  return {
    pxOrdersFetched: pxOrders.length,
    imported,
    updated,
    settled,
    before,
    after,
  };
}

/**
 * Backfill `sport` on orders whose legs are tagged as 'unknown'.
 *
 * When fullPxReconcile reconstructs orders from PX REST, old line_ids
 * aren't in the current lineManager index, so `lineManager.lookupLine`
 * returns null and sport defaults to 'unknown'. This bunches otherwise-
 * categorizable P&L into an "Unknown" pnlBySport bucket — hiding actual
 * sport-level performance.
 *
 * Three-tier inference (first success wins):
 *   1. pxEventId → line_manager.eventIndex[id].sport (or sportName mapped
 *      to our sport key). This is the most reliable signal.
 *   2. Team name lookup against a self-built map constructed from THIS
 *      run's orders that already have a non-'unknown' sport. The same
 *      team on PX (e.g. "New York Yankees") should always map to the
 *      same sport key ("baseball_mlb") — we just need one known example.
 *   3. If neither works, leave the leg tagged 'unknown' and count it
 *      in the unresolved bucket of the return value.
 *
 * Also updates the top-level o.legs and o.meta.legs in place so the
 * pnlBySport aggregation (which iterates both leg sources) picks up
 * the corrected sport on the next render.
 *
 * Persists each mutated order via db.saveOrder. Does NOT touch stats
 * counters (pnl is unchanged, only sport tagging changes).
 */
function backfillUnknownSports() {
  const lineManager = require('./line-manager');

  // STEP 1a: Build a team-name → sport map from orders that already have
  // a known sport. Lowercased keys so matching is case-insensitive.
  const teamToSport = {};
  // STEP 1b: Build a pxEventId-prefix → sport map. PX namespaces event IDs
  // by sport (MLB 10077xxx, NHL 30025xxx, etc). For reconstructed orders
  // with no team/event name at all, the pxEventId is the only signal left.
  const prefixToSport = {};  // '10077' → 'baseball_mlb'
  const prefixCounts = {};   // for debug logging: how many hits per prefix

  for (const o of Object.values(orders)) {
    const legSources = [o.legs, o.meta?.legs].filter(Boolean);
    for (const legs of legSources) {
      for (const leg of legs) {
        const sport = leg.sport;
        if (!sport || sport === 'unknown') continue;

        // Team name → sport
        const team = (leg.team || leg.teamName || '').toLowerCase().trim();
        if (team && team !== '?' && team !== 'unknown') {
          const cleaned = team.replace(/^(over|under)\s*\(/i, '').replace(/\)$/, '').trim();
          if (cleaned.length >= 3) teamToSport[cleaned] = sport;
          if (team.length >= 3) teamToSport[team] = sport;
        }

        // pxEventId prefix → sport (first 5 chars captures the sport namespace)
        const eid = leg.pxEventId || leg.sport_event_id;
        if (eid != null) {
          const prefix = String(eid).substring(0, 5);
          if (prefix.length >= 4) {
            // Only set the prefix if there's no conflict (different sports
            // sharing the same prefix would make this inference unsafe).
            if (!prefixToSport[prefix]) {
              prefixToSport[prefix] = sport;
              prefixCounts[prefix] = (prefixCounts[prefix] || 0) + 1;
            } else if (prefixToSport[prefix] === sport) {
              prefixCounts[prefix]++;
            } else {
              // Conflict — mark this prefix unsafe
              prefixToSport[prefix] = '__CONFLICT__';
            }
          }
        }
      }
    }
  }
  // Drop conflicted prefixes
  for (const [p, s] of Object.entries(prefixToSport)) {
    if (s === '__CONFLICT__') delete prefixToSport[p];
  }
  log.info('Backfill', `Built team→sport map (${Object.keys(teamToSport).length} entries) and ` +
    `prefix→sport map (${Object.keys(prefixToSport).length} entries): ` +
    Object.entries(prefixToSport).map(([p, s]) => `${p}→${s}(${prefixCounts[p] || 0})`).join(', '));

  // STEP 2: Walk all orders and try to resolve any 'unknown' legs.
  let ordersScanned = 0;
  let ordersUpdated = 0;
  let legsResolvedByEvent = 0;
  let legsResolvedByTeam = 0;
  let legsResolvedByPrefix = 0;
  let legsStillUnresolved = 0;

  for (const o of Object.values(orders)) {
    ordersScanned++;
    let mutated = false;
    const legSources = [o.legs, o.meta?.legs].filter(Boolean);
    for (const legs of legSources) {
      for (const leg of legs) {
        if (leg.sport && leg.sport !== 'unknown') continue;

        // Tier 1: pxEventId → eventIndex
        const eid = leg.pxEventId || leg.sport_event_id;
        if (eid) {
          const info = lineManager.getEventInfo ? lineManager.getEventInfo(eid) : null;
          if (info && info.sport) {
            leg.sport = info.sport;
            legsResolvedByEvent++;
            mutated = true;
            continue;
          }
          // eventIndex entries built from fetchSportEvents only have sportName
          // (PX's string like "Baseball"), not our sport key. Map sportName to
          // our sport keys via config.sportNameMap if possible.
          if (info && info.sportName) {
            // Reverse lookup: find our sport key from sportName
            try {
              const { config } = require('../config');
              for (const [sportKey, pxName] of Object.entries(config.sportNameMap || {})) {
                if (pxName === info.sportName) {
                  leg.sport = sportKey;
                  legsResolvedByEvent++;
                  mutated = true;
                  break;
                }
              }
              if (leg.sport && leg.sport !== 'unknown') continue;
            } catch (err) { /* fall through to team match */ }
          }
        }

        // Tier 2: team name lookup in self-built map
        const team = (leg.team || leg.teamName || '').toLowerCase().trim();
        if (team && team !== '?' && team !== 'unknown') {
          const cleaned = team.replace(/^(over|under)\s*\(/i, '').replace(/\)$/, '').trim();
          const match = teamToSport[team] || teamToSport[cleaned] || null;
          if (match) {
            leg.sport = match;
            legsResolvedByTeam++;
            mutated = true;
            continue;
          }
        }

        // Tier 3: pxEventId prefix → sport. PX namespaces event IDs by
        // sport, so '10077xxx' is always MLB and '30025xxx' is always NHL.
        // This catches reconstructed orders that have zero team/event data.
        if (eid != null) {
          const prefix = String(eid).substring(0, 5);
          if (prefix.length >= 4 && prefixToSport[prefix]) {
            leg.sport = prefixToSport[prefix];
            legsResolvedByPrefix++;
            mutated = true;
            continue;
          }
        }

        legsStillUnresolved++;
      }
    }
    if (mutated) {
      ordersUpdated++;
      db.saveOrder(o).catch(() => {});
    }
  }

  log.info('Backfill', `Scanned ${ordersScanned} orders, updated ${ordersUpdated}. ` +
    `Resolved ${legsResolvedByEvent} by eventIndex + ${legsResolvedByTeam} by team map + ${legsResolvedByPrefix} by prefix. ` +
    `${legsStillUnresolved} legs still unresolved.`);

  return {
    ordersScanned,
    ordersUpdated,
    legsResolvedByEvent,
    legsResolvedByTeam,
    legsResolvedByPrefix,
    legsStillUnresolved,
    teamMapSize: Object.keys(teamToSport).length,
    prefixMapSize: Object.keys(prefixToSport).length,
    prefixMap: prefixToSport,
  };
}

/**
 * Backfill team/sport/market data on reconstructed orders using a user-
 * provided export CSV. The export was generated earlier when the orders
 * had full metadata; we parse rows, match by parlayId, and populate the
 * otherwise-unresolvable `?` legs.
 *
 * Input: array of { parlayId, legCount, selections: string[], sports: string[] }
 * (pre-parsed from the CSV by the caller).
 *
 * Returns { matched, updated, skipped, selectionMismatch } counts.
 */
function backfillFromExport(rows) {
  // Sport display label → our internal sport key
  const SPORT_LABEL_MAP = {
    'MLB': 'baseball_mlb',
    'NBA': 'basketball_nba',
    'NHL': 'icehockey_nhl',
    'NCAAB': 'basketball_ncaab',
    'Tennis': 'tennis',
    'EPL': 'soccer_epl',
    'MLS': 'soccer_usa_mls',
    'La Liga': 'soccer_spain_la_liga',
    'Bundesliga': 'soccer_germany_bundesliga',
    'Serie A': 'soccer_italy_serie_a',
    'Ligue 1': 'soccer_france_ligue_one',
    'UCL': 'soccer_uefa_champs_league',
    'Europa': 'soccer_uefa_europa_league',
    'UEFA Europa': 'soccer_uefa_europa_league',
    'NWSL': 'soccer_usa_nwsl',
    'WNBA': 'basketball_wnba',
    'Boxing': 'boxing_boxing',
    'MMA': 'mma_mixed_martial_arts',
    'Golf': 'golf_matchups',
    'PGA': 'golf_matchups',
  };
  function labelToSport(label) {
    if (!label) return null;
    const trimmed = String(label).trim();
    if (SPORT_LABEL_MAP[trimmed]) return SPORT_LABEL_MAP[trimmed];
    // Try case-insensitive fallback
    for (const [k, v] of Object.entries(SPORT_LABEL_MAP)) {
      if (k.toLowerCase() === trimmed.toLowerCase()) return v;
    }
    return null;
  }

  let matched = 0;
  let updated = 0;
  let skipped = 0;
  let selectionMismatch = 0;

  for (const row of rows) {
    const order = orders[row.parlayId];
    if (!order) { skipped++; continue; }
    matched++;

    const legs = order.legs || order.meta?.legs || [];
    if (legs.length === 0) { skipped++; continue; }

    // CSV selections array must match the leg count to safely position-map.
    if (!Array.isArray(row.selections) || row.selections.length !== legs.length) {
      selectionMismatch++;
      continue;
    }

    // Build per-leg sport assignment. If the CSV has a single sport for the
    // row, apply it to every leg. If it has as many sports as legs, zip in
    // order. Otherwise, leave sport untouched.
    let perLegSport = null;
    if (Array.isArray(row.sports)) {
      if (row.sports.length === 1 && row.sports[0]) {
        const s = labelToSport(row.sports[0]);
        if (s) perLegSport = legs.map(() => s);
      } else if (row.sports.length === legs.length) {
        perLegSport = row.sports.map(label => labelToSport(label));
      }
    }

    let changed = false;
    const updateLegs = (legSource) => {
      for (let i = 0; i < legSource.length; i++) {
        const leg = legSource[i];
        const prevTeam = leg.team;
        const newTeam = row.selections[i];
        if (newTeam && newTeam !== '?' && newTeam !== prevTeam) {
          leg.team = newTeam;
          if (!leg.teamName || leg.teamName === '?') leg.teamName = newTeam;
          changed = true;
        }
        if (perLegSport && perLegSport[i] && (!leg.sport || leg.sport === 'unknown')) {
          leg.sport = perLegSport[i];
          changed = true;
        }
      }
    };
    updateLegs(legs);
    // Mirror onto both o.legs and o.meta.legs for consistency with display
    if (order.meta?.legs && order.meta.legs !== legs) updateLegs(order.meta.legs);

    if (changed) {
      updated++;
      db.saveOrder(order).catch(err => log.warn('Backfill', `saveOrder failed for ${order.parlayId}: ${err.message}`));
    }
  }

  log.info('Backfill', `CSV backfill: matched ${matched}, updated ${updated}, skipped ${skipped}, selectionMismatch ${selectionMismatch}`);
  return { matched, updated, skipped, selectionMismatch, rowsReceived: rows.length };
}

/**
 * Delete settled orders whose legs are all unresolved ('?' team names).
 * Removes from in-memory store, reverses P&L stats, and deletes from DB.
 */
async function deleteUnknownSettledOrders() {
  let deleted = 0;
  const parlayIds = [];
  for (const [parlayId, order] of Object.entries(orders)) {
    if (!order.status?.startsWith('settled_')) continue;
    const legs = order.legs || order.meta?.legs || [];
    const allUnknown = legs.length > 0 && legs.every(l => !l.team || l.team === '?' || l.team === 'unknown');
    if (!allUnknown) continue;

    // Reverse stats
    if (order.pnl != null) {
      stats.runningPnL -= order.pnl;
      if (order.pnl > 0) stats.totalWins--;
      else if (order.pnl < 0) stats.totalLosses--;
    }
    stats.totalSettlements--;

    // Remove from in-memory stores
    if (order.orderUuid) delete ordersByUuid[order.orderUuid];
    delete orders[parlayId];
    parlayIds.push(parlayId);
    deleted++;
  }

  // Delete from Supabase
  if (parlayIds.length > 0) {
    const supabase = db.getClient();
    if (supabase) {
      for (const pid of parlayIds) {
        const { error } = await supabase.from('parlay_orders').delete().eq('parlay_id', pid);
        if (error) log.error('DB', `Failed to delete ${pid}: ${error.message}`);
      }
    }
  }

  log.info('Orders', `Deleted ${deleted} unknown settled orders (P&L now: $${stats.runningPnL.toFixed(2)})`);
  return { deleted, parlayIds };
}

/**
 * Lightweight periodic reconcile specifically for ghost 'confirmed' orders.
 * Our in-memory tracker can accumulate ghosts in two ways:
 *   1) order.matched arrived and promoted status to 'confirmed' (per the
 *      Alec event-model fix) but order.finalized never followed, so we
 *      have no orderUuid and no way to verify PX actually placed the bet.
 *   2) order.finalized arrived, but PX later settled/voided the bet and
 *      we missed the settlement event — so we still show 'confirmed'
 *      while PX has it closed.
 *
 * This function cross-checks each tracker-confirmed order against PX's
 * current order list and:
 *   - If an order has an orderUuid and PX shows it settled → promote to
 *     settled_* locally (triggers P&L recording).
 *   - If an order has an orderUuid and PX doesn't have it at all → mark
 *     as phantom (excluded from deployed risk).
 *   - If an order has no orderUuid and PX's list has no matching parlay_id
 *     after 10 min → mark as phantom.
 *
 * Designed to run every ~5 min. px.fetchOrders is paginated but capped
 * at 1000 recent orders — sufficient since stale-phantom filter already
 * excludes anything with game-start >12h old.
 */
async function reconcileGhostConfirmed(px) {
  const startedAt = Date.now();
  let pxOrders;
  try {
    // Pull all-history. PX's total order count in prod reached 2,151+
    // (most 'rejected' — counter-offers we didn't take). limit=1000 only
    // returned the 1000 most-recent, which cut off most of our older
    // still-confirmed parlays so they never got reconciled. 10,000 covers
    // growth headroom; fetchOrders short-circuits when PX returns fewer.
    pxOrders = await px.fetchOrders(10000);
  } catch (err) {
    log.warn('GhostReconcile', `Could not fetch PX orders: ${err.message}`);
    return { checked: 0, ghostsFound: 0, settledFound: 0, err: err.message };
  }
  // Index PX orders two ways for fast lookup.
  const pxByUuid = {};
  const pxByParlayId = {};
  for (const po of pxOrders) {
    const uuid = po.order_uuid || po.orderUuid;
    const pid = po.p_id || po.parlay_id || po.parlayId;
    if (uuid) pxByUuid[uuid] = po;
    if (pid) {
      if (!pxByParlayId[pid]) pxByParlayId[pid] = [];
      pxByParlayId[pid].push(po);
    }
  }

  let checked = 0, ghostsFound = 0, settledFound = 0, orderUuidFilledIn = 0;
  // Phantom-flag age threshold. PX's /parlay/sp/orders/ is paginated and
  // server-side-capped (observed: ~475 rows returned for limit=1000), so
  // absence from a single fetch is NOT reliable evidence of a ghost —
  // recent real confirmations were being misclassified, hiding $20K+ of
  // real deployed risk. Only flag phantom when the order is genuinely
  // old: any real fill should have settled (and been removed from the
  // 'confirmed' set) well within this window.
  const PHANTOM_MIN_AGE_MS = 48 * 60 * 60 * 1000; // 48h
  // Supplementary rule: regardless of confirmedAt age, an order whose
  // every leg's game started > this many ms ago is almost certainly
  // settled or failed on PX. If we still can't find it in PX's paginated
  // list, safe to phantom-flag early. Without this, null-UUID orders
  // that fall outside PX's scan window (orderUuid was never captured
  // because price.confirm.new sometimes omits it) sit in "confirmed"
  // state for hours or days after the games finished.
  // Lowered from 24h → 12h because the previous threshold was missing
  // orders where games finished 16-20h ago (e.g. Minneapolis-based
  // games that tip off ~8pm ET finish ~11pm and the order still sat
  // "confirmed" at 4pm the next day).
  const LEGS_DONE_CUTOFF_MS = 12 * 60 * 60 * 1000; // 12h
  function allLegsDoneOver(order, cutoffMs) {
    const legs = order.legs || (order.meta && order.meta.legs) || [];
    if (legs.length === 0) return false;
    const now = Date.now();
    let latest = null;
    for (const leg of legs) {
      const st = leg.startTime || leg.start_time;
      if (!st) return false; // unknown — don't vote done
      const t = new Date(st).getTime();
      if (isNaN(t)) return false;
      if (latest == null || t > latest) latest = t;
    }
    return latest != null && (now - latest) > cutoffMs;
  }
  // Additional signal: if our game-results checker has already set
  // inferredResult on any leg, that game is DEFINITIVELY over — there's
  // no time-based uncertainty. If that's true AND PX can't find the
  // order, it's a phantom regardless of confirmedAt age.
  function anyLegHasKnownResult(order) {
    const legs = order.legs || (order.meta && order.meta.legs) || [];
    for (const leg of legs) {
      const r = leg.inferredResult
        || leg.settlementStatus
        || leg.settlement_status;
      if (r && r !== 'pending' && r !== 'unknown') return true;
    }
    return false;
  }
  const phantomIds = [];
  let autoCleared = 0;
  for (const order of Object.values(orders)) {
    if (order.status !== 'confirmed') continue;
    // Skip very-fresh confirmations — give the finalize event time to arrive.
    if (order.confirmedAt && (Date.now() - new Date(order.confirmedAt).getTime()) < 60 * 1000) continue;
    checked++;
    const ageMs = order.confirmedAt ? (Date.now() - new Date(order.confirmedAt).getTime()) : Infinity;
    const wasPhantom = !!(order.meta && order.meta.phantom);
    // Un-flag any previously-phantom order that is still within the
    // grace window. The prior version of this function over-flagged
    // under truncated PX responses; this step recovers DB rows that
    // were incorrectly marked in past runs. Preserve phantoms that
    // were set for legs-finished OR leg-result-known reasons — those
    // don't care about confirmedAt age, only about whether games are
    // done.
    const legsDone = allLegsDoneOver(order, LEGS_DONE_CUTOFF_MS);
    const legResultKnown = anyLegHasKnownResult(order);
    if (wasPhantom && ageMs < PHANTOM_MIN_AGE_MS && !legsDone && !legResultKnown) {
      delete order.meta.phantom;
      delete order.meta.phantomReason;
      delete order.meta.phantomMarkedAt;
      autoCleared++;
      // Restore the lifetime-confirmation count — we'd decremented it
      // when flagging; unclearing undoes that.
      if (order.status === 'confirmed') stats.totalConfirmations++;
      db.saveOrder(order).catch(() => {});
    }

    if (order.orderUuid) {
      const px = pxByUuid[order.orderUuid];
      if (px) {
        // PX knows this order. Auto-clear any prior phantom flag.
        if (wasPhantom) {
          delete order.meta.phantom;
          delete order.meta.phantomReason;
          delete order.meta.phantomMarkedAt;
          autoCleared++;
          if (order.status === 'confirmed') stats.totalConfirmations++;
          db.saveOrder(order).catch(() => {});
        }
        // If PX shows it's settled/voided, demote our copy and — critically —
        // run recordSettlement so tracker P&L, runningPnL, and the confirmed
        // list all reflect reality. Previously we only set a flag, so
        // losses silently accumulated as 'confirmed' (PX never emits
        // order.matched for SP losses per Alec's event model).
        const pxStatus = (px.status || '').toLowerCase();
        const pxSettlement = (px.settlement_status || px.settlementStatus || '').toLowerCase();
        if (pxStatus === 'settled' && (pxSettlement === 'won' || pxSettlement === 'lost' || pxSettlement === 'push')) {
          const result = pxSettlement === 'push' ? 'push' : pxSettlement;
          const pxProfit = px.profit != null ? Number(px.profit) : 0;
          log.info('GhostReconcile', `PX settled ${order.parlayId.substring(0,8)} as ${result} (profit=${pxProfit}) — promoting tracker status`);
          recordSettlement(order.orderUuid, result, pxProfit);
          settledFound++;
        } else if (pxStatus === 'rejected' || pxStatus === 'failed') {
          // Not a real fill on PX — scrub our confirmed status so it stops
          // inflating Deployed and the Open Positions list.
          log.info('GhostReconcile', `PX ${pxStatus} ${order.parlayId.substring(0,8)} — demoting from confirmed (never a real fill)`);
          order.status = 'rejected';
          order.meta = order.meta || {};
          order.meta.pxRejectedOrFailed = pxStatus;
          order.meta.pxRejectedAt = new Date().toISOString();
          // Strip confirmedStake so Deployed and Open Positions ignore it
          // (status='rejected' is filtered out of those views already, but
          // clearing the stake makes the intent explicit).
          db.saveOrder(order).catch(() => {});
          settledFound++;
        } else if (pxStatus && pxStatus !== 'confirmed' && pxStatus !== 'matched' && pxStatus !== 'pending' && pxStatus !== 'finalized') {
          // Unknown PX status (e.g. 'voided' variants we don't handle) —
          // flag for investigation but don't auto-demote.
          log.info('GhostReconcile', `PX says ${order.parlayId.substring(0,8)} is '${pxStatus}' (settlement=${pxSettlement}) — flagging`);
          order.meta = order.meta || {};
          order.meta.pxStatusMismatch = pxStatus;
          order.meta.pxStatusMismatchAt = new Date().toISOString();
          db.saveOrder(order).catch(() => {});
        }
        // pxStatus === 'finalized' WITHOUT settlement_status = still waiting
        // for outcome → leave as 'confirmed' in our tracker.
      } else if (ageMs >= PHANTOM_MIN_AGE_MS
                 || allLegsDoneOver(order, LEGS_DONE_CUTOFF_MS)
                 || anyLegHasKnownResult(order)) {
        // PX's paginated list didn't return this order AND any of:
        //   (a) order is older than the 48h confirmedAt grace window, OR
        //   (b) every leg's game started >12h ago (so the parlay is long
        //       resolved upstream regardless of confirmedAt age), OR
        //   (c) at least one leg already has a known game result
        //       (inferredResult / settlement_status) — game is over,
        //       further uncertainty is about PX not us.
        const wasAlreadyPhantom = order.meta && order.meta.phantom;
        order.meta = order.meta || {};
        order.meta.phantom = true;
        order.meta.phantomReason = ageMs >= PHANTOM_MIN_AGE_MS
          ? 'orderUuid-not-in-px'
          : anyLegHasKnownResult(order)
          ? 'orderUuid-not-in-px-leg-result-known'
          : 'orderUuid-not-in-px-legs-finished';
        order.meta.phantomMarkedAt = new Date().toISOString();
        phantomIds.push(order.parlayId);
        ghostsFound++;
        // Decrement the lifetime confirmations stat — this order was
        // counted as confirmed on the way in but we now know it never
        // really was. Matches what fullPxReconcile will do on next run.
        if (!wasAlreadyPhantom && stats.totalConfirmations > 0) {
          stats.totalConfirmations--;
        }
        db.saveOrder(order).catch(() => {});
      }
      // else: absent from PX list but still within grace window — do
      // nothing. Don't flag, don't clear an existing flag.
    } else {
      // No orderUuid yet — try to fill in from PX by parlayId match.
      const candidates = pxByParlayId[order.parlayId] || [];
      if (candidates.length > 0) {
        const pxMatch = candidates[0];
        if (pxMatch.order_uuid) {
          order.orderUuid = pxMatch.order_uuid;
          ordersByUuid[pxMatch.order_uuid] = order.parlayId;
          orderUuidFilledIn++;
          // Found in PX — also auto-clear any stale phantom flag.
          if (wasPhantom) {
            delete order.meta.phantom;
            delete order.meta.phantomReason;
            delete order.meta.phantomMarkedAt;
            autoCleared++;
            if (order.status === 'confirmed') stats.totalConfirmations++;
          }
          db.saveOrder(order).catch(() => {});
        }
        continue;
      }
      // No match at all. Flag phantom on any of:
      //   (a) confirmedAt is beyond the 48h grace window, OR
      //   (b) all legs' games have concluded (>12h past their start), OR
      //   (c) at least one leg already has a known game result — games
      //       are done regardless of time, and the order isn't on PX.
      if (ageMs >= PHANTOM_MIN_AGE_MS
          || allLegsDoneOver(order, LEGS_DONE_CUTOFF_MS)
          || anyLegHasKnownResult(order)) {
        const wasAlreadyPhantom = order.meta && order.meta.phantom;
        order.meta = order.meta || {};
        order.meta.phantom = true;
        if (!wasAlreadyPhantom && stats.totalConfirmations > 0) {
          stats.totalConfirmations--;
        }
        order.meta.phantomReason = ageMs >= PHANTOM_MIN_AGE_MS
          ? 'no-uuid-and-no-px-match'
          : anyLegHasKnownResult(order)
          ? 'no-uuid-leg-result-known'
          : 'no-uuid-legs-finished';
        order.meta.phantomMarkedAt = new Date().toISOString();
        phantomIds.push(order.parlayId);
        ghostsFound++;
        db.saveOrder(order).catch(() => {});
      }
    }
  }
  // Second pass: rejected → confirmed recovery (the accept-POST-failed
  // drift backstop). Walks orders our local state has as 'rejected' and
  // checks whether PX actually has them booked. If so, call
  // importPxBookedOrder to flip status back to 'confirmed' and rebuild
  // exposure. Catches the case where verifyAcceptUnknown's retry window
  // (3s/15s/60s) timed out before PX finalized — without this loop those
  // parlays sit forever as locally-rejected while PX reports them in
  // /px-positions, causing the All Quotes vs Open Positions mismatch.
  //
  // Scope guards:
  //   - Only consider rejections from the accept/confirm path (rejection
  //     reason starts with 'accept-POST-failed:' or contains 'PX state').
  //     We do NOT auto-import rejections from local risk/correlation/etc.
  //     declines — those were intentional and must stay rejected even if
  //     PX somehow booked them in a sandbox/race scenario.
  //   - Only flip if PX shows the order as actually booked (tbd/finalized
  //     or settled won/lost/push) — never on 'requested', 'cancelled', etc.
  //   - Skip if rejectedAt is younger than the verify-retry window so we
  //     don't race verifyAcceptUnknown's own retries.
  let rejectedRecovered = 0;
  const RECOVERY_MIN_AGE_MS = 90 * 1000; // > final 60s verify retry, with margin
  const ACCEPT_REJECT_PREFIX = /^accept-POST-failed:/i;
  for (const order of Object.values(orders)) {
    if (order.status !== 'rejected') continue;
    if (!order.rejectedAt) continue;
    if ((Date.now() - new Date(order.rejectedAt).getTime()) < RECOVERY_MIN_AGE_MS) continue;
    const reason = order.rejectionReason || '';
    if (!ACCEPT_REJECT_PREFIX.test(reason) && !/PX state/i.test(reason)) continue;
    // Find PX's record. Prefer orderUuid if we have one, fall back to parlayId.
    let pxMatch = null;
    if (order.orderUuid && pxByUuid[order.orderUuid]) {
      pxMatch = pxByUuid[order.orderUuid];
    } else if (pxByParlayId[order.parlayId] && pxByParlayId[order.parlayId].length > 0) {
      pxMatch = pxByParlayId[order.parlayId][0];
    }
    if (!pxMatch) continue;
    const pxStatus = (pxMatch.status || '').toLowerCase();
    const pxSettlement = (pxMatch.settlement_status || pxMatch.settlementStatus || '').toLowerCase();
    const isBooked = pxStatus === 'finalized'
      || pxStatus === 'matched'
      || pxStatus === 'confirmed'
      || pxSettlement === 'tbd'
      || pxSettlement === 'won'
      || pxSettlement === 'lost'
      || pxSettlement === 'push';
    if (!isBooked) continue;
    const uuid = order.orderUuid || pxMatch.order_uuid || pxMatch.orderUuid;
    const stake = pxMatch.confirmed_stake != null ? Number(pxMatch.confirmed_stake)
      : (order.confirmedStake != null ? Number(order.confirmedStake) : null);
    const odds = pxMatch.confirmed_odds != null ? Number(pxMatch.confirmed_odds)
      : (order.confirmedOdds != null ? Number(order.confirmedOdds) : null);
    log.warn('GhostReconcile', `Rejected→Confirmed recovery: ${order.parlayId.substring(0,8)} (reason="${reason}", PX status=${pxStatus}/${pxSettlement}) — importing`);
    const result = importPxBookedOrder(order.parlayId, uuid, stake, odds);
    if (result && result.ok) {
      rejectedRecovered++;
      // If PX has it settled too, record the settlement so P&L is captured.
      if (pxSettlement === 'won' || pxSettlement === 'lost' || pxSettlement === 'push') {
        const profit = pxMatch.profit != null ? Number(pxMatch.profit) : 0;
        recordSettlement(uuid, pxSettlement, profit);
      }
    }
  }

  const elapsedMs = Date.now() - startedAt;
  log.info('GhostReconcile', `Checked ${checked} confirmed orders in ${elapsedMs}ms — ghosts: ${ghostsFound}, auto-cleared phantoms: ${autoCleared}, orderUuids filled in: ${orderUuidFilledIn}, PX-status-mismatches: ${settledFound}, rejected-recovered: ${rejectedRecovered}`);
  return { checked, ghostsFound, autoCleared, orderUuidFilledIn, settledFound, rejectedRecovered, phantomIds, elapsedMs };
}

/**
 * One-time historical cleanup for the Apr 25, 2026 false-fill bug
 * (recordMatchedParlay was promoting status='confirmed' on every
 * order.matched broadcast, including ones where another SP won at a
 * different price). Walks all DB rows with status='confirmed' and no
 * order_uuid, and reverts to status='rejected' the ones whose
 * confirmed_odds prove the broadcast was for someone else's price.
 *
 * Test: real wins have confirmed_odds = -offered_odds (sign-flipped,
 * same magnitude — handleConfirm path or self-matched). False fills
 * have confirmed_odds = -matched_odds where matched_odds was the
 * WINNING SP's price, so |confirmed + offered| > tolerance.
 *
 * Conservative: skips rows missing either offered_odds or
 * confirmed_odds, and rows where the sign-flip test passes (real
 * wins). Marks reverted rows with meta.matchedByOtherSp and
 * meta.falseConfirmCleanedAt for audit.
 *
 * @param {object} opts
 * @param {boolean} opts.dryRun - default true; if false, writes to DB
 * @param {string} opts.fromIso - default 30 days ago
 * @param {string} opts.toIso - default now
 * @returns {Promise<object>} { dryRun, scanned, candidates, reverted, skipped, samples }
 */
async function cleanFalseConfirms(opts = {}) {
  const dryRun = opts.dryRun !== false; // default true — must explicitly pass false to write
  const toIso = opts.toIso || new Date().toISOString();
  const fromIso = opts.fromIso || new Date(Date.now() - 30 * 24 * 3600 * 1000).toISOString();
  const ODDS_TOL = 5;

  const dbClient = db.getClient ? db.getClient() : null;
  if (!dbClient) {
    return { error: 'no DB client', dryRun, scanned: 0, candidates: 0, reverted: 0 };
  }

  // Pull confirmed-no-uuid rows in window. Use loadOrdersInDateRange
  // (paginated, retried) and filter in memory — keeps SQL simple.
  const rows = await db.loadOrdersInDateRange(fromIso, toIso, { groupBy: 'confirmed_at', maxRows: 50000 });
  let scanned = 0, candidates = 0, reverted = 0, skipped = 0;
  const samples = [];
  const skipReasons = { missingOdds: 0, hasOrderUuid: 0, notConfirmed: 0, isRealWin: 0 };

  for (const row of rows) {
    scanned++;
    if (row.status !== 'confirmed') { skipReasons.notConfirmed++; skipped++; continue; }
    if (row.order_uuid) { skipReasons.hasOrderUuid++; skipped++; continue; }
    if (row.offered_odds == null || row.confirmed_odds == null) {
      skipReasons.missingOdds++; skipped++; continue;
    }
    const sumAbs = Math.abs(Number(row.offered_odds) + Number(row.confirmed_odds));
    if (sumAbs <= ODDS_TOL) {
      // Sign-flipped within tolerance — real win, leave alone.
      skipReasons.isRealWin++; skipped++; continue;
    }

    // False fill: confirmed_odds is some other SP's (negated) price.
    candidates++;
    if (samples.length < 10) {
      samples.push({
        parlayId: row.parlay_id,
        offered: Number(row.offered_odds),
        confirmed: Number(row.confirmed_odds),
        sumAbs,
        confirmedAt: row.confirmed_at,
      });
    }

    if (!dryRun) {
      const newMeta = {
        ...(row.meta || {}),
        matchedByOtherSp: {
          observedAt: row.confirmed_at || new Date().toISOString(),
          matchedOdds: -Number(row.confirmed_odds), // back-derive bettor-side
          ourOfferedOdds: Number(row.offered_odds),
          oddsDelta: (-Number(row.confirmed_odds)) - Number(row.offered_odds),
        },
        falseConfirmCleanedAt: new Date().toISOString(),
        falseConfirmCleanupReason: `confirmed_odds (${row.confirmed_odds}) + offered_odds (${row.offered_odds}) = ${(Number(row.confirmed_odds) + Number(row.offered_odds))} — not sign-flipped, was another SP's price`,
      };
      const { error } = await dbClient
        .from('parlay_orders')
        .update({
          status: 'rejected',
          confirmed_odds: null,
          confirmed_stake: null,
          confirmed_at: null,
          meta: newMeta,
        })
        .eq('parlay_id', row.parlay_id);
      if (error) {
        log.warn('CleanFalseConfirms', `update ${row.parlay_id} failed: ${error.message}`);
      } else {
        reverted++;
        // Sync in-memory tracker if present
        const memOrder = orders[row.parlay_id];
        if (memOrder) {
          memOrder.status = 'rejected';
          memOrder.confirmedOdds = null;
          memOrder.confirmedStake = null;
          memOrder.confirmedAt = null;
          memOrder.meta = newMeta;
        }
      }
    }
  }

  log.info('CleanFalseConfirms', `${dryRun ? '[DRY-RUN] ' : ''}scanned=${scanned} candidates=${candidates} reverted=${reverted} skipped=${skipped} (realWins=${skipReasons.isRealWin}, missingOdds=${skipReasons.missingOdds}, hasUuid=${skipReasons.hasOrderUuid}, notConfirmed=${skipReasons.notConfirmed})`);

  return {
    dryRun, fromIso, toIso, scanned, candidates, reverted, skipped, skipReasons, samples,
  };
}

module.exports = {
  recordQuote,
  updateOrderLatency,
  getRecentLatencyRecords,
  recordConfirmation,
  recordRejection,
  recordFinalized,
  recordSettlement,
  recordLegSettlement,
  pollOrderSettlements,
  checkSettlementDrift,
  getDriftState,
  checkLegResults,
  revertBogusSettlements,
  reconcileSettlements,
  fullPxReconcile,
  reconcileGhostConfirmed,
  cleanFalseConfirms,
  // Read-only snapshot of the in-memory declineStats counters. Used by
  // /prop-opportunity to surface the unknownLegCategories.player_prop
  // byPropType breakdown without exporting the mutable object directly.
  getDeclineStatsSnapshot: () => ({
    total: declineStats.total,
    reasons: { ...declineStats.reasons },
    unknownLegCategories: declineStats.unknownLegCategories,
  }),
  // Pull a flat, time-ordered list of player_prop legs from the rolling
  // recentDeclineEvents log for the dashboard's Player Prop Flow card.
  // Each entry is one prop leg (a single declined parlay can produce
  // multiple entries when several of its legs are props). Filterable by
  // sport and propType so the operator can drill into MLB-only or
  // pitcher-K-only flow without scanning the whole stream client-side.
  //
  // opts: { sport?: string, propType?: string, limit?: number, sinceMs?: number }
  // Returns newest first (matches recentDeclineEvents push order in reverse).
  getRecentPropFlow: (opts = {}) => {
    const limit = Math.min(opts.limit || 200, 2000);
    const events = declineStats.recentDeclineEvents || [];
    const out = [];
    // Walk newest-first by iterating in reverse
    for (let i = events.length - 1; i >= 0; i--) {
      const ev = events[i];
      if (opts.sinceMs && new Date(ev.time).getTime() < opts.sinceMs) break;
      const cats = ev.unknownCategories || [];
      for (const c of cats) {
        if (c.category !== 'player_prop') continue;
        if (opts.sport && c.sport !== opts.sport) continue;
        if (opts.propType && c.propType !== opts.propType) continue;
        out.push({
          time: ev.time,
          parlayId: ev.parlayId,
          sport: c.sport,
          eventName: c.eventName,
          marketName: c.marketName,
          propType: c.propType, // null for non-MLB sports
          line: c.line,
          isKnownEvent: c.isKnownEvent,
          resolveReason: c.resolveReason,
        });
        if (out.length >= limit) return out;
      }
    }
    return out;
  },
  backfillUnknownSports,
  backfillFromExport,
  deleteUnknownSettledOrders,
  findByParlayId,
  findByOrderUuid,
  getTotalPortfolioRisk,
  getTotalToWin,
  // Exposed so /px-positions can apply the same stale/phantom filter
  // that getTotalPortfolioRisk uses — keeps the Open Positions table's
  // sum of My Risk aligned with the Deployed figure on the dashboard.
  isOrderStalePhantom,
  checkPortfolioRisk,
  getGameExposureSnapshot,
  checkGameExposure,
  getSeriesEventRisk,
  checkSeriesExposure,
  checkPitcherExposure,
  getPitcherExposureSnapshot,
  checkPlayerExposure,
  getPlayerExposureSnapshot,
  playerKeyForLeg,
  getRecentOrders,
  getStats,
  getPnLBySport,
  getExposureForTeam,
  checkExposureLimits,
  getExposureSnapshot,
  buildPendingReservation,
  reservePending,
  releasePending,
  getPendingTeamRisk,
  getPendingGameRisk,
  checkRecentDuplicate,
  recordParlaySignature,
  recordMatchedParlay,
  getMatchedParlays: () => matchedParlays,
  recordDecline,
  recordUnsupportedMarket,
  importPxBookedOrder,
  markAcceptUnknown,
  sweepGhostOrders,
  getMarketIntel,
  getAlerts,
  getRecentRejects: (limit = 100) => rejectStats.recent.slice(0, Math.max(1, Math.min(limit, 100))),
  getExposureLimitStats,
  recordExposureRejection,
  refreshLiveOdds,
  rebuildAllExposure,
  enrichReconstructedOrders,
  enrichReconstructedFromPx,
  enrichOpenPositionsFromAffiliate,
  loadFromDb,
  backfillFillBucketEvents,
  // Exposed for /px-positions endpoint — lets it enrich PX-open orders
  // with tracker-held leg data without duplicating the uuid lookup.
  getOrderByUuid: (uuid) => {
    const pid = ordersByUuid[uuid];
    return pid ? orders[pid] : null;
  },
  backfillGolfMetadata,
};

/**
 * Walk all in-memory orders, find ones flagged meta.reconstructed=true with
 * missing team names, and enrich them from the current lineManager index.
 * Persists enriched versions back to DB.
 */
async function enrichReconstructedOrders() {
  const lineManager = require('./line-manager');
  let enriched = 0;
  let scanned = 0;
  const pending = []; // awaited saves so callers know the DB has actually been updated

  // Pre-fetch line_cache for unresolved lineIds (same pattern as fullPxReconcile).
  // The lineManager only has CURRENT lines; line_cache has historical data.
  const unresolvedLineIds = new Set();
  for (const order of Object.values(orders)) {
    const legs = order.legs || order.meta?.legs || [];
    const needsEnrichment = legs.some(l => !l.team || l.team === '?' || l.team === 'unknown');
    if (!needsEnrichment) continue;
    for (const l of legs) {
      if (l.lineId && !lineManager.lookupLine(l.lineId)) unresolvedLineIds.add(l.lineId);
    }
  }
  let lineCacheFallback = {};
  if (unresolvedLineIds.size > 0) {
    try {
      lineCacheFallback = await db.loadLineCacheBulk([...unresolvedLineIds]);
      if (Object.keys(lineCacheFallback).length > 0) {
        log.info('Orders', `enrichReconstructedOrders: pre-fetched ${Object.keys(lineCacheFallback).length}/${unresolvedLineIds.size} from line_cache`);
      }
    } catch (err) {
      log.warn('Orders', `enrichReconstructedOrders: line_cache pre-fetch failed: ${err.message}`);
    }
  }

  for (const order of Object.values(orders)) {
    const legs = order.legs || order.meta?.legs || [];
    // Check if any leg has '?' or null team (reconstructed signature)
    const needsEnrichment = legs.some(l => !l.team || l.team === '?' || l.team === 'unknown');
    if (!needsEnrichment) continue;
    scanned++;
    let changed = false;
    const newLegs = legs.map(l => {
      const info = (l.lineId ? lineManager.lookupLine(l.lineId) : null) || lineCacheFallback[l.lineId] || null;
      const eventName = l.pxEventId ? lineManager.getEventName(l.pxEventId) : null;
      if (!info && !eventName) return l; // nothing new to add
      let team = info?.teamName || l.team;
      if (!info && eventName) team = eventName;
      if (info?.marketType === 'total' && info?.homeTeam && info?.awayTeam) {
        team = `${team} (${info.awayTeam} @ ${info.homeTeam})`;
      }
      if (team && team !== '?' && team !== l.team) changed = true;
      return {
        ...l,
        team: team || l.team,
        teamName: info?.teamName || l.teamName || team,
        sport: info?.sport || info?.oddsApiSport || l.sport || 'unknown',
        market: info?.marketType || l.market,
        marketType: info?.marketType || l.marketType,
        selection: info?.selection || info?.oddsApiSelection || l.selection,
        homeTeam: info?.homeTeam || l.homeTeam,
        awayTeam: info?.awayTeam || l.awayTeam,
        pxEventName: eventName || info?.pxEventName || l.pxEventName,
        startTime: info?.startTime || l.startTime,
      };
    });
    if (changed) {
      order.legs = newLegs;
      if (order.meta) order.meta.legs = newLegs;
      enriched++;
      // Queue the save and actually await it later. Using fire-and-forget
      // here meant restarts could discard enrichment before it hit Supabase,
      // which is exactly how the Team Exposure table kept ending up empty.
      pending.push(db.saveOrder(order).catch(err => {
        log.warn('Orders', `enrichReconstructedOrders: saveOrder failed for ${order.parlayId}: ${err.message}`);
      }));
    }
  }
  // Wait for every queued save to settle before returning so callers
  // (e.g. startup) can trust the DB reflects the enriched in-memory state.
  if (pending.length > 0) await Promise.all(pending);
  log.info('Orders', `Enrichment: scanned ${scanned} orders with unresolved legs, enriched ${enriched}, persisted ${pending.length}`);
  return { scanned, enriched, persisted: pending.length };
}

/**
 * Resolve open/in-progress orders via PX's /partner/affiliate/* endpoints.
 *
 * This is the PRIMARY enrichment path for confirmed orders. It replaces the
 * per-event enrichReconstructedFromPx loop with two bulk calls:
 *
 *   1) GET /partner/affiliate/get_tournaments              (cached dictionary)
 *   2) GET /partner/affiliate/get_sport_events?event_ids=… (home/away teams,
 *      start time, sport/tournament names — all keyed by sport_event_id)
 *   3) GET /partner/affiliate/get_multiple_markets?event_ids=…
 *      (market display_name + selections[] with line_id → team/selection
 *      mapping)
 *
 * For each confirmed order whose legs have team='?', it:
 *   • Resolves pxEventId → home/away team + start time + sport
 *   • Resolves lineId → team + market type + selection + line via
 *     parseMarketSelections
 *   • Awaits the DB save so the enrichment survives restarts
 *   • Calls rebuildAllExposure() so the Team/Game Exposure tables reflect
 *     the newly-resolved data without requiring a separate manual step
 *
 * Returns counters for visibility into how well the affiliate endpoints
 * cover the current order book.
 */
async function enrichOpenPositionsFromAffiliate() {
  const px = require('./prophetx');

  // ---- 1) collect work set ----
  const eventIds = new Set();
  const targetOrders = [];
  for (const order of Object.values(orders)) {
    const legs = order.legs || order.meta?.legs || [];
    const needsEnrichment = legs.some(
      l => !l.team || l.team === '?' || l.team === 'unknown' || !l.homeTeam || !l.startTime
    );
    if (!needsEnrichment) continue;
    targetOrders.push(order);
    for (const l of legs) {
      const eid = l.pxEventId || l.sport_event_id;
      if (eid) eventIds.add(eid);
    }
  }
  if (eventIds.size === 0) {
    return { targetOrders: 0, eventsRequested: 0, eventsResolved: 0, lineIdsResolved: 0, enriched: 0 };
  }

  // ---- 2) load tournament dictionary (small, one call) ----
  const tournaments = {}; // tournament_id → { name, sportName }
  try {
    const list = await px.fetchAffiliateTournaments();
    for (const t of list || []) {
      tournaments[t.id] = { name: t.name, sportName: t.sport?.name };
    }
    log.info('Affiliate', `Loaded ${Object.keys(tournaments).length} tournaments`);
  } catch (err) {
    log.warn('Affiliate', `get_tournaments failed: ${err.message} — continuing without tournament dict`);
  }

  // ---- 3) bulk-fetch sport events (home/away, start time, sport/tournament) ----
  const idList = [...eventIds];
  const CHUNK = 100; // generous — URL length for 100 ids is ~900 bytes
  const eventInfo = {}; // eventId → { name, homeTeam, awayTeam, scheduled, sportName, tournamentName, tournamentId }
  let sportEventsFetched = 0;
  for (let i = 0; i < idList.length; i += CHUNK) {
    const chunk = idList.slice(i, i + CHUNK);
    try {
      const seList = await px.fetchAffiliateSportEvents({ eventIds: chunk });
      for (const se of seList || []) {
        const home = (se.competitors || []).find(c => c.side === 'home');
        const away = (se.competitors || []).find(c => c.side === 'away');
        eventInfo[se.event_id] = {
          name: se.name || se.display_name,
          homeTeam: home?.display_name || home?.name || null,
          awayTeam: away?.display_name || away?.name || null,
          scheduled: se.scheduled,
          sportName: se.sport_name,
          tournamentName: se.tournament_name,
          tournamentId: se.tournament_id,
        };
        sportEventsFetched++;
      }
    } catch (err) {
      log.warn('Affiliate', `get_sport_events chunk ${i}-${i + chunk.length} failed: ${err.message}`);
    }
  }
  log.info('Affiliate', `Resolved ${sportEventsFetched}/${idList.length} sport events`);

  // ---- 4) bulk-fetch markets (line_id → team/selection/market type) ----
  const lineIdInfo = {}; // lineId → { teamName, marketType, selection, line }
  let marketsChunks = 0;
  for (let i = 0; i < idList.length; i += CHUNK) {
    const chunk = idList.slice(i, i + CHUNK);
    try {
      const bulk = await px.fetchAffiliateMultipleMarkets(chunk);
      // Shape is { event_id: [market, ...] }. Keys are stringified ids.
      for (const [eid, marketList] of Object.entries(bulk || {})) {
        if (!Array.isArray(marketList)) continue;
        for (const market of marketList) {
          if (!['moneyline', 'spread', 'total', 'team_total'].includes(market.type)) continue;
          let parsed;
          try { parsed = px.parseMarketSelections(market); }
          catch (e) { continue; }
          for (const sel of parsed || []) {
            if (!sel.lineId) continue;
            lineIdInfo[sel.lineId] = {
              teamName: sel.teamName,
              marketType: sel.marketType,
              selection: sel.selection,
              line: sel.line,
              competitorId: sel.competitorId,
            };
          }
        }
      }
      marketsChunks++;
    } catch (err) {
      log.warn('Affiliate', `get_multiple_markets chunk ${i}-${i + chunk.length} failed: ${err.message}`);
    }
  }
  log.info('Affiliate', `Resolved ${Object.keys(lineIdInfo).length} line_ids across ${marketsChunks} chunks`);

  // ---- 4b) fallback: check persistent line_cache for any unresolved line_ids ----
  const unresolvedLineIds = [];
  for (const order of targetOrders) {
    const legs = order.legs || order.meta?.legs || [];
    for (const l of legs) {
      if (l.lineId && !lineIdInfo[l.lineId]) unresolvedLineIds.push(l.lineId);
    }
  }
  if (unresolvedLineIds.length > 0) {
    const unique = [...new Set(unresolvedLineIds)];
    try {
      const cached = await db.loadLineCacheBulk(unique);
      const cacheHits = Object.keys(cached).length;
      if (cacheHits > 0) {
        for (const [lid, info] of Object.entries(cached)) {
          lineIdInfo[lid] = {
            teamName: info.teamName,
            marketType: info.marketType,
            selection: info.selection || info.oddsApiSelection,
            line: info.line,
            competitorId: info.competitorId,
            // Carry homeTeam/awayTeam/sport/startTime from line_cache so
            // step 5 can use them when the PX affiliate endpoint doesn't
            // return data for expired/historical events (the primary cause
            // of "? @ ?" and "Event XXXXXXX" in the exposure tables).
            homeTeam: info.homeTeam || null,
            awayTeam: info.awayTeam || null,
            sport: info.sport || info.oddsApiSport || null,
            startTime: info.startTime || null,
            pxEventName: info.pxEventName || null,
          };
        }
        log.info('Affiliate', `Resolved ${cacheHits}/${unique.length} additional line_ids from Supabase line_cache`);
      }
    } catch (err) {
      log.warn('Affiliate', `loadLineCacheBulk failed: ${err.message}`);
    }
  }

  // ---- 4c) backfill eventInfo from line_cache hits ----
  // When the PX affiliate endpoint doesn't return data for historical events
  // (returns 404 for expired events), the eventInfo map has gaps. Line_cache
  // stores homeTeam/awayTeam/startTime per line, so we can reconstruct event
  // info from any resolved line that references the event.
  for (const [, info] of Object.entries(lineIdInfo)) {
    if (!info.homeTeam && !info.awayTeam) continue;
    // Find which event(s) this line belongs to and backfill eventInfo
    for (const order of targetOrders) {
      const legs = order.legs || order.meta?.legs || [];
      for (const l of legs) {
        if (l.lineId !== undefined && lineIdInfo[l.lineId] === info) {
          const eid = l.pxEventId || l.sport_event_id;
          if (eid && !eventInfo[eid] && (info.homeTeam || info.awayTeam)) {
            eventInfo[eid] = {
              name: info.pxEventName || `${info.awayTeam || '?'} @ ${info.homeTeam || '?'}`,
              homeTeam: info.homeTeam,
              awayTeam: info.awayTeam,
              scheduled: info.startTime,
              sportName: info.sport,
            };
          }
        }
      }
    }
  }

  // ---- 4d) direct event-ID lookup in line_cache ----
  // For events STILL unresolved (lineId not in line_cache either — e.g.
  // the specific line was never registered but another line for the same
  // event was), look up by px_event_id directly.
  const stillUnresolvedEventIds = [];
  for (const order of targetOrders) {
    const legs = order.legs || order.meta?.legs || [];
    for (const l of legs) {
      const eid = l.pxEventId || l.sport_event_id;
      if (eid && !eventInfo[eid]) stillUnresolvedEventIds.push(eid);
    }
  }
  if (stillUnresolvedEventIds.length > 0) {
    const uniqueEids = [...new Set(stillUnresolvedEventIds)];
    try {
      const evByEid = await db.loadLineCacheByEventIds(uniqueEids);
      const hits = Object.keys(evByEid).length;
      if (hits > 0) {
        for (const [eid, info] of Object.entries(evByEid)) {
          if (!eventInfo[eid]) {
            eventInfo[eid] = {
              name: info.pxEventName || `${info.awayTeam || '?'} @ ${info.homeTeam || '?'}`,
              homeTeam: info.homeTeam,
              awayTeam: info.awayTeam,
              scheduled: info.startTime,
              sportName: info.sport || info.oddsApiSport,
            };
          }
        }
        log.info('Affiliate', `Resolved ${hits}/${uniqueEids.length} events from line_cache by px_event_id`);
      }
    } catch (err) {
      log.warn('Affiliate', `loadLineCacheByEventIds failed: ${err.message}`);
    }
  }

  // ---- 5) apply enrichment to target orders + persist ----
  let enriched = 0;
  const pending = [];
  for (const order of targetOrders) {
    const legs = order.legs || order.meta?.legs || [];
    let changed = false;
    const newLegs = legs.map(l => {
      const lineInfo = l.lineId ? lineIdInfo[l.lineId] : null;
      const eid = l.pxEventId || l.sport_event_id;
      const ev = eid ? eventInfo[eid] : null;
      if (!lineInfo && !ev) return l;

      // Team precedence: explicit line match → existing → home/away from event
      // Use lineInfo (which may come from line_cache) as fallback for event
      // data when the PX affiliate endpoint doesn't cover historical events.
      const resolvedHome = ev?.homeTeam || lineInfo?.homeTeam || l.homeTeam;
      const resolvedAway = ev?.awayTeam || lineInfo?.awayTeam || l.awayTeam;

      let team = lineInfo?.teamName || l.team;
      if (!team || team === '?' || team === 'unknown') {
        if (ev) {
          // For moneyline/spread we can't pick home vs away without the lineInfo,
          // but for total markets we can label as "Over/Under (Away @ Home)".
          if (lineInfo?.marketType === 'total' && resolvedAway && resolvedHome) {
            team = `${lineInfo.selection || 'Total'} (${resolvedAway} @ ${resolvedHome})`;
          } else {
            // Fallback to the event display name so the row at least says
            // "Yankees @ Red Sox" instead of "Event 10077494".
            team = ev.name || `${resolvedAway || '?'} @ ${resolvedHome || '?'}`;
          }
        } else if (resolvedHome || resolvedAway) {
          // No event info from PX, but line_cache gave us team names
          team = `${resolvedAway || '?'} @ ${resolvedHome || '?'}`;
        }
      }

      const sportName = ev?.sportName
        || lineInfo?.sport
        || tournaments[l.tournamentId || l.tournament_id]?.sportName
        || l.sport;

      if ((team && team !== l.team) || (resolvedHome && !l.homeTeam) || (sportName && sportName !== l.sport)) {
        changed = true;
      }

      return {
        ...l,
        team: team || l.team,
        teamName: lineInfo?.teamName || l.teamName || team,
        market: lineInfo?.marketType || l.market,
        marketType: lineInfo?.marketType || l.marketType,
        selection: lineInfo?.selection || l.selection,
        line: lineInfo?.line != null ? lineInfo.line : l.line,
        homeTeam: resolvedHome || null,
        awayTeam: resolvedAway || null,
        startTime: ev?.scheduled || lineInfo?.startTime || l.startTime,
        sport: sportName || 'unknown',
        pxEventName: ev?.name || lineInfo?.pxEventName || l.pxEventName,
        tournamentName: ev?.tournamentName || l.tournamentName,
      };
    });

    if (changed) {
      order.legs = newLegs;
      if (order.meta) order.meta.legs = newLegs;
      enriched++;
      pending.push(db.saveOrder(order).catch(err => {
        log.warn('Affiliate', `saveOrder failed for ${order.parlayId}: ${err.message}`);
      }));
    }
  }

  if (pending.length > 0) await Promise.all(pending);

  // Rebuild exposure so the Team/Game Exposure tables reflect the new names
  // without the caller having to know to trigger it separately.
  rebuildAllExposure();

  const result = {
    targetOrders: targetOrders.length,
    eventsRequested: idList.length,
    eventsResolved: sportEventsFetched,
    lineIdsResolved: Object.keys(lineIdInfo).length,
    tournamentsLoaded: Object.keys(tournaments).length,
    enriched,
    persisted: pending.length,
  };
  log.info('Affiliate', `enrichOpenPositionsFromAffiliate done: ${JSON.stringify(result)}`);
  return result;
}

/**
 * Deep-enrich reconstructed orders by fetching historical markets from PX for
 * each unique pxEventId referenced by unresolved legs. Writes team/market/line
 * info directly onto order legs and persists to DB. Does NOT register lines
 * back into lineManager (these events are historical and should not affect
 * live quoting).
 */
async function enrichReconstructedFromPx() {
  const px = require('./prophetx');
  let scanned = 0, enriched = 0, eventsFetched = 0, eventsFailed = 0;

  // Collect unique pxEventIds from orders that need enrichment
  const eventIdToOrders = new Map(); // eventId -> Set of parlayIds
  for (const order of Object.values(orders)) {
    const legs = order.legs || order.meta?.legs || [];
    const needsEnrichment = legs.some(l => !l.team || l.team === '?' || l.team === 'unknown');
    if (!needsEnrichment) continue;
    scanned++;
    for (const l of legs) {
      const eid = l.pxEventId || l.sport_event_id;
      if (!eid) continue;
      if (!eventIdToOrders.has(eid)) eventIdToOrders.set(eid, new Set());
      eventIdToOrders.get(eid).add(order.parlayId);
    }
  }

  // Fetch markets for each unique event and build a lineId -> {team, market, line, ...} map
  const lineIdInfo = {}; // lineId -> { teamName, marketType, line, ... }
  const eventNames = {}; // eventId -> name (best-effort)
  for (const [eventId, parlayIds] of eventIdToOrders.entries()) {
    try {
      const markets = await px.fetchMarkets(eventId);
      eventsFetched++;
      for (const market of markets || []) {
        if (!['moneyline', 'spread', 'total'].includes(market.type)) continue;
        if (market.event_name && !eventNames[eventId]) eventNames[eventId] = market.event_name;
        const parsed = px.parseMarketSelections(market);
        for (const sel of parsed) {
          if (!sel.lineId) continue;
          lineIdInfo[sel.lineId] = {
            teamName: sel.teamName,
            marketType: sel.marketType,
            selection: sel.selection,
            line: sel.line,
            competitorId: sel.competitorId,
          };
        }
      }
    } catch (err) {
      eventsFailed++;
      log.debug('Orders', `enrichReconstructedFromPx: fetchMarkets(${eventId}) failed: ${err.message}`);
    }
  }

  // Fallback: check persistent line_cache for unresolved line_ids
  const unresolvedLineIds = [];
  for (const order of Object.values(orders)) {
    const legs = order.legs || order.meta?.legs || [];
    for (const l of legs) {
      if (l.lineId && !lineIdInfo[l.lineId] && (!l.team || l.team === '?' || l.team === 'unknown')) {
        unresolvedLineIds.push(l.lineId);
      }
    }
  }
  if (unresolvedLineIds.length > 0) {
    const unique = [...new Set(unresolvedLineIds)];
    try {
      const cached = await db.loadLineCacheBulk(unique);
      const cacheHits = Object.keys(cached).length;
      if (cacheHits > 0) {
        for (const [lid, info] of Object.entries(cached)) {
          lineIdInfo[lid] = {
            teamName: info.teamName,
            marketType: info.marketType,
            selection: info.selection || info.oddsApiSelection,
            line: info.line,
            competitorId: info.competitorId,
          };
        }
        log.info('Orders', `Deep enrichment: resolved ${cacheHits}/${unique.length} line_ids from Supabase line_cache`);
      }
    } catch (err) {
      log.warn('Orders', `loadLineCacheBulk failed in deep enrichment: ${err.message}`);
    }
  }

  // Apply enrichment
  for (const order of Object.values(orders)) {
    const legs = order.legs || order.meta?.legs || [];
    const needsEnrichment = legs.some(l => !l.team || l.team === '?' || l.team === 'unknown');
    if (!needsEnrichment) continue;
    let changed = false;
    const newLegs = legs.map(l => {
      const info = l.lineId ? lineIdInfo[l.lineId] : null;
      if (!info) return l;
      const team = info.teamName || l.team;
      if (team && team !== '?' && team !== l.team) changed = true;
      return {
        ...l,
        team: team || l.team,
        teamName: info.teamName || l.teamName,
        market: info.marketType || l.market,
        marketType: info.marketType || l.marketType,
        selection: info.selection || l.selection,
        line: info.line != null ? info.line : l.line,
        pxEventName: eventNames[l.pxEventId] || l.pxEventName,
      };
    });
    if (changed) {
      order.legs = newLegs;
      if (order.meta) order.meta.legs = newLegs;
      enriched++;
      db.saveOrder(order).catch(() => {});
    }
  }

  log.info('Orders', `Deep enrichment: ${scanned} orders scanned, ${eventsFetched} events fetched (${eventsFailed} failed), ${enriched} orders enriched`);
  return { scanned, enriched, eventsFetched, eventsFailed, uniqueEvents: eventIdToOrders.size };
}

/**
 * Record a confirm-time exposure rejection with actual stake data.
 * Called from websocket.js when a confirmation is rejected for exposure limits.
 */
function recordExposureRejection(parlayId, stake, reason, violations) {
  const time = new Date().toISOString();
  const sizeBucket = getSizeBucket(stake);
  exposureLimitStats.confirmTimeRejections.total++;
  exposureLimitStats.confirmTimeRejections.bySizeBucket[sizeBucket] =
    (exposureLimitStats.confirmTimeRejections.bySizeBucket[sizeBucket] || 0) + 1;
  exposureLimitStats.confirmTimeRejections.recent.unshift({
    parlayId,
    stake: Math.round(stake * 100) / 100,
    reason,
    violations: violations || null,
    sizeBucket,
    time,
  });
  if (exposureLimitStats.confirmTimeRejections.recent.length > 100) {
    exposureLimitStats.confirmTimeRejections.recent.pop();
  }
}

/**
 * Get exposure-limit rejection stats — both quote-time (estimated) and
 * confirm-time (actual stakes). Used by /status and dashboard.
 */
function getExposureLimitStats() {
  return {
    quoteTime: {
      total: exposureLimitStats.total,
      byReason: { ...exposureLimitStats.byReason },
      bySizeBucket: { ...exposureLimitStats.bySizeBucket },
      recent: exposureLimitStats.recent.slice(0, 50),
    },
    confirmTime: {
      total: exposureLimitStats.confirmTimeRejections.total,
      bySizeBucket: { ...exposureLimitStats.confirmTimeRejections.bySizeBucket },
      recent: exposureLimitStats.confirmTimeRejections.recent.slice(0, 50),
    },
  };
}

/**
 * Return alert-relevant data: counts of limit-based declines + rejections
 * over the last 15 minutes. Used by the dashboard to show a warning banner
 * when the SP is missing quotes because it's hitting its own risk limits.
 */
function getAlerts() {
  const now = Date.now();
  const windowMs = 15 * 60 * 1000;
  const cutoff = now - windowMs;

  // Recent limit-hit declines (within window)
  const recentLimitDeclines = declineStats.recent.filter(d => {
    if (!d.isLimit) return false;
    const t = new Date(d.time).getTime();
    return !isNaN(t) && t >= cutoff;
  });

  // Recent rejections (all reasons — these are limit hits at confirm time)
  // Filters out two flavors of false-positive that would otherwise inflate
  // the alarm counter:
  //
  //   (a) `accept-POST-failed: no orderUuid` rejections younger than 5s.
  //       These are transient — handleConfirm queues verifyAcceptUnknown
  //       3s after the failed accept POST, and most of them flip back to
  //       'confirmed' via importPxBookedOrder (PX booked the bet, just
  //       didn't return order_uuid in the synchronous response). The 5s
  //       grace period gives the verify task time to land before we count
  //       the parlay as a real rejection.
  //
  //   (b) Any reject whose parlayId now has status === 'confirmed' in
  //       orders[] — that's the verify path having flipped it back. The
  //       rejectStats.recent log entry stays for audit but we don't
  //       count it as a current rejection, since it isn't one anymore.
  //
  // Without this filter the dashboard cried wolf on every accept-POST
  // hiccup even though the SP had taken the bet correctly.
  const VERIFY_GRACE_MS = 5000;
  const recentRejects = rejectStats.recent.filter(r => {
    const t = new Date(r.time).getTime();
    if (isNaN(t) || t < cutoff) return false;
    // (b) Parlay flipped back to confirmed by verifyAcceptUnknown. Exclude.
    const order = r.parlayId ? orders[r.parlayId] : null;
    if (order && order.status === 'confirmed') return false;
    // (a) accept-POST-failed: no orderUuid in the verify-pending window.
    const reason = r.reason || '';
    if (reason.includes('no orderUuid') && (now - t) < VERIFY_GRACE_MS) return false;
    return true;
  });

  // Group declines by reason
  const byReason = {};
  for (const d of recentLimitDeclines) {
    if (!byReason[d.reason]) byReason[d.reason] = { count: 0, lastDetail: null, lastTime: null };
    byReason[d.reason].count++;
    if (!byReason[d.reason].lastDetail) byReason[d.reason].lastDetail = d.detail;
    if (!byReason[d.reason].lastTime) byReason[d.reason].lastTime = d.time;
  }

  // Group rejections by bucket
  const rejectByBucket = {};
  for (const r of recentRejects) {
    if (!rejectByBucket[r.bucket]) rejectByBucket[r.bucket] = { count: 0, lastReason: null, lastTime: null };
    rejectByBucket[r.bucket].count++;
    if (!rejectByBucket[r.bucket].lastReason) rejectByBucket[r.bucket].lastReason = r.reason;
    if (!rejectByBucket[r.bucket].lastTime) rejectByBucket[r.bucket].lastTime = r.time;
  }

  return {
    windowMinutes: 15,
    limitDeclineCount: recentLimitDeclines.length,
    rejectCount: recentRejects.length,
    declineByReason: byReason,
    rejectByBucket,
    allTimeLimitDeclines: declineStats.recent.filter(d => d.isLimit).length,
    allTimeRejects: rejectStats.total,
  };
}

/**
 * Load historical data from Supabase on startup.
 * Restores orders and matched parlays into in-memory stores.
 */
/**
 * Backfill golf metadata (tournamentName, roundNum) onto existing stored
 * golf legs by looking up each leg's line_id in the current line-manager
 * index. Only legs from lines still registered in the current lineIndex
 * can be backfilled (that's always the currently-active tournament).
 *
 * Walks both o.legs and o.meta.legs. Saves touched orders to DB.
 *
 * Returns: { ordersTouched, updated, skipped, goldLegsTotal }
 */
function backfillGolfMetadata() {
  const lineManager = require('./line-manager');
  let ordersTouched = 0;
  let updated = 0;
  let skipped = 0;
  let goldLegsTotal = 0;
  for (const o of Object.values(orders)) {
    let orderDirty = false;
    for (const src of [o.legs, o.meta?.legs]) {
      if (!Array.isArray(src)) continue;
      for (const leg of src) {
        if (leg.sport !== 'golf_matchups') continue;
        goldLegsTotal++;
        // Skip if already populated
        if (leg.tournamentName != null && leg.roundNum != null) continue;
        const lid = leg.lineId || leg.line_id;
        if (!lid) { skipped++; continue; }
        const info = lineManager.lookupLine(lid);
        if (!info) { skipped++; continue; }
        if (info.tournamentName != null && leg.tournamentName == null) {
          leg.tournamentName = info.tournamentName;
          orderDirty = true;
        }
        if (info.roundNum != null && leg.roundNum == null) {
          leg.roundNum = info.roundNum;
          orderDirty = true;
        }
        if (orderDirty) updated++;
      }
    }
    if (orderDirty) {
      ordersTouched++;
      db.saveOrder(o).catch(() => {});
    }
  }
  return { ordersTouched, updated, skipped, goldLegsTotal };
}

function hydrateDeclinesInMemory(dbDeclines) {
  // Mirror the live-decline near-miss set (services/order-tracker.js:1512)
  // so restart-rebuilt declines surface in the Near Misses table the same
  // way live ones do. Includes Phase 2 K-prop pricing/data reasons.
  const nearMissReasons = new Set([
    'no fair value', 'stale odds', 'parlay too unlikely', 'odds too high', 'event started',
    'prop_no_fair_value', 'prop_low_confidence', 'prop_stale',
  ]);
  for (let i = dbDeclines.length - 1; i >= 0; i--) {
    const d = dbDeclines[i];
    declineStats.total++;
    declineStats.reasons[d.reason] = (declineStats.reasons[d.reason] || 0) + 1;
    declineStats.recent.unshift({ reason: d.reason, detail: d.detail, parlayId: d.parlayId, time: d.declinedAt, isLimit: d.isLimit });
    if (declineStats.recent.length > 200) declineStats.recent.pop();
    if (d.parlayId) {
      declinesByParlayId[d.parlayId] = { reason: d.reason, unknownLineIds: d.unknownLineIds || [], unknownDetails: d.unknownDetails || [], declineDetail: d.detail, declinedAt: d.declinedAt };
      declineIdOrder.push(d.parlayId);
      while (declineIdOrder.length > MAX_DECLINE_ENTRIES) { const old = declineIdOrder.shift(); delete declinesByParlayId[old]; }
    }
    for (const ud of (d.unknownDetails || [])) {
      if (!declineStats.unknownSports[ud]) declineStats.unknownSports[ud] = { count: 0, lastSeen: null, recentDeclines: [] };
      declineStats.unknownSports[ud].count++;
      declineStats.unknownSports[ud].lastSeen = d.declinedAt;
    }
    if (nearMissReasons.has(d.reason)) {
      declineStats.nearMisses.unshift({ parlayId: d.parlayId, legs: d.knownLegs || [], time: d.declinedAt, reason: d.reason, detail: d.detail });
      if (declineStats.nearMisses.length > 500) declineStats.nearMisses.pop();
    }
  }
}

async function loadFromDb() {
  if (!db.isEnabled()) {
    log.info('DB', 'Supabase not configured — running in memory-only mode');
    return;
  }

  log.info('DB', 'Loading historical data from Supabase...');

  // Load orders (very high cap to pull ALL history; loadOrders paginates).
  // Bumped from 20,000 to 200,000 after observing the original cap drop
  // reconstructed orders added by fullPxReconcile: those orders have
  // null quoted_at and sort to the end with nullsFirst:false, so any
  // growth beyond 20k was silently lost on restart. 200k covers weeks of
  // production scale.
  const LOAD_CAP = 200000;
  const dbOrders = await db.loadOrders(LOAD_CAP);
  if (dbOrders.length >= LOAD_CAP) {
    log.warn('DB', `loadFromDb hit cap ${LOAD_CAP} — may be truncating history. Raise LOAD_CAP.`);
  }
  for (const o of dbOrders) {
    // Hoist winning-quote info out of meta (stored there to avoid DB schema change)
    if (o.meta) {
      if (o.meta.winningOdds != null && o.winningOdds == null) o.winningOdds = o.meta.winningOdds;
      if (o.meta.winningStake != null && o.winningStake == null) o.winningStake = o.meta.winningStake;
      if (o.meta.lostAt && !o.lostAt) o.lostAt = o.meta.lostAt;
      if (o.meta.pxProfit != null && o.pxProfit == null) o.pxProfit = o.meta.pxProfit;
      if (o.meta.expectedValue != null && o.expectedValue == null) o.expectedValue = o.meta.expectedValue;
      if (o.meta.closingImpliedProb != null && o.closingImpliedProb == null) o.closingImpliedProb = o.meta.closingImpliedProb;
      if (o.meta.clvDelta != null && o.clvDelta == null) o.clvDelta = o.meta.clvDelta;
    }

    // Normalize leg data across both sources (o.legs and o.meta.legs).
    // Historical records have PX settlementStatus on o.legs only (set by
    // recordLegSettlement at the time) but NOT on meta.legs (which was
    // populated at quote time with scraper inferredResult). The client
    // often reads `order.meta?.legs || order.legs`, preferring meta.legs
    // first, so we need to mirror the PX truth onto meta.legs too.
    // Strategy:
    //   1) Build a map from o.legs by lineId (and fall back to team name)
    //      of the authoritative settlementStatus.
    //   2) For every leg in both sources: if settlementStatus is missing,
    //      copy it from the authoritative map. Then sync inferredResult to
    //      match the settlementStatus whenever present.
    const legsA = Array.isArray(o.legs) ? o.legs : [];
    const legsB = Array.isArray(o.meta?.legs) ? o.meta.legs : [];
    const authMap = {};
    for (const src of [legsA, legsB]) {
      for (const leg of src) {
        const ss = leg.settlementStatus || leg.settlement_status;
        if (!ss) continue;
        const lid = leg.lineId || leg.line_id;
        const tm = leg.team || leg.teamName;
        if (lid) authMap['l:' + lid] = ss;
        if (tm) authMap['t:' + tm] = ss;
      }
    }
    for (const src of [legsA, legsB]) {
      for (const leg of src) {
        const lid = leg.lineId || leg.line_id;
        const tm = leg.team || leg.teamName;
        const lookedUp = (lid && authMap['l:' + lid]) || (tm && authMap['t:' + tm]) || null;
        if (lookedUp && !leg.settlementStatus) leg.settlementStatus = lookedUp;
        const px = leg.settlementStatus || leg.settlement_status;
        if (px && leg.inferredResult !== px) leg.inferredResult = px;
      }
    }
    // Skip reconstructed orders — they're skeleton records imported from PX REST
    // that we never actually quoted. Real settled orders come from either the PX
    // backfill (meta.pxBackfill) or our live quoting pipeline (have quotedAt).
    if (o.meta?.reconstructed && !o.meta?.pxBackfill) {
      log.debug('DB', `Skipping reconstructed order ${o.parlayId} on load`);
      continue;
    }

    orders[o.parlayId] = o;
    if (o.orderUuid) ordersByUuid[o.orderUuid] = o.parlayId;

    // Self-heal: confirmedOdds stored in bettor-side convention instead
    // of SP-side. Triggered by the recordMatchedParlay fallback path
    // that was setting `confirmedOdds = offeredOdds` (no negation) when
    // matched_odds was missing from an order.matched broadcast. Since
    // offeredOdds is bettor-side and confirmedOdds is SP-side, healthy
    // orders NEVER have the two fields exactly equal — and exact
    // equality is the crisp bug signature (same-sign after drift from
    // a legitimate handleConfirm could coincidentally occur, but exact
    // equality of the raw values cannot). Fix BEFORE addExposure runs
    // below so the healed value flows into Stakes Held / P&L
    // calculations immediately. Persist to DB so the heal is permanent.
    if (
      o.confirmedOdds != null &&
      o.offeredOdds != null &&
      o.confirmedOdds === o.offeredOdds &&
      o.confirmedOdds !== 0
    ) {
      const old = o.confirmedOdds;
      o.confirmedOdds = -o.confirmedOdds;
      o.meta = o.meta || {};
      o.meta.confirmedOddsSignHealed = { from: old, to: o.confirmedOdds, at: new Date().toISOString() };
      db.saveOrder(o).catch(() => {});
      log.warn('Orders', `Self-heal: negated bettor-side confirmedOdds for ${o.parlayId} (was ${old}, now ${o.confirmedOdds}; offered=${o.offeredOdds})`);
    }

    // PURE HYDRATION: trust what's stored in the DB. No pattern matching,
    // no revert heuristics, no pnl recomputation.
    //
    // Prior versions of this code recomputed spResult from leg data and
    // recomputed pnl from stake/odds on every restart. The re-derivation
    // was brittle (nullable leg.settlement_status caused allWon → lost
    // flips; nullable confirmedStake caused SP losses to recompute to $0)
    // and was the primary source of in-memory ↔ DB drift observed all day.
    //
    // Correct settlement logic lives in exactly ONE place: recordSettlement,
    // which runs when a WebSocket parlay.settled event arrives or during
    // pollOrderSettlements / fullPxReconcile. That code path persists
    // status + pnl + pxProfit to the DB. On reload we just read those
    // fields. If the DB is wrong, run /full-px-reconcile to fix it —
    // don't let the reload path silently mutate data.
    stats.totalQuotes++;
    // Phantom-flagged orders don't count toward lifetime confirmations
    // — classify as rejections so stats match reality.
    if (o.meta && o.meta.phantom) {
      stats.totalRejections++;
      continue;
    }
    if (o.status === 'confirmed') {
      stats.totalConfirmations++;
      // Always track exposure for confirmed orders regardless of whether
      // their legs have started — we still hold the risk until the order
      // actually moves to settled_*. The previous isOrderFinished filter
      // silently dropped confirmed orders whose games had tipped off at
      // startup, causing Team/Game Exposure rows to drastically
      // under-count parlays after every redeploy. Zombie / stuck
      // confirmed orders are cleaned by settlement polling + drift
      // reconcile, not by hiding them from exposure.
      addExposure(o);
    } else if (o.status === 'rejected') {
      stats.totalRejections++;
    } else if (o.status?.startsWith('settled_')) {
      stats.totalSettlements++;
      if (o.pnl != null) {
        stats.runningPnL += o.pnl;
        if (o.pnl > 0) stats.totalWins++;
        else if (o.pnl < 0) stats.totalLosses++;
      }
    }
  }
  log.info('DB', `Loaded ${dbOrders.length} orders (P&L: $${stats.runningPnL.toFixed(2)})`);

  // Self-heal: any order with rejectedAt set but status !== 'rejected'
  // is a historical victim of the recordMatchedParlay bug that was
  // promoting rejected quotes to 'confirmed' when PX ignored our reject
  // and broadcast order.matched anyway. Demote back to rejected so our
  // accounting is honest about which parlays we actually accepted.
  // This matters especially for max_risk violations where PX confirmed
  // stakes exceeding our local cap.
  let resurrectedHealed = 0;
  for (const o of Object.values(orders)) {
    if (!o || !o.rejectedAt) continue;
    if (o.status === 'rejected') continue;
    if (typeof o.status === 'string' && o.status.startsWith('settled_')) continue;
    log.warn('Orders', `Self-heal: demoting ${o.parlayId} from ${o.status} back to rejected (rejectedAt=${o.rejectedAt}, reason=${o.rejectionReason || 'unknown'})`);
    // Correct any counter drift
    if (o.status === 'confirmed') {
      if (stats.totalConfirmations > 0) stats.totalConfirmations--;
      stats.totalRejections++;
    }
    o.status = 'rejected';
    db.saveOrder(o).catch(() => {});
    resurrectedHealed++;
  }
  if (resurrectedHealed > 0) {
    log.info('DB', `Self-heal: demoted ${resurrectedHealed} resurrected-rejection orders back to rejected`);
  }

  // One-time golf metadata backfill on startup. For existing stored golf legs
  // missing tournamentName / roundNum, look up the lineId against the current
  // lineIndex (which was seeded before loadFromDb runs in the service bootstrap
  // sequence) and copy those fields over if the line is still registered.
  // Legs from past tournaments where the lineId is no longer indexed will be
  // skipped — the client's display fallback still shows the opponent from
  // homeTeam/awayTeam regardless.
  try {
    const result = backfillGolfMetadata();
    if (result.updated > 0) {
      log.info('DB', `Golf backfill: ${result.updated} legs enriched with tournamentName/roundNum across ${result.ordersTouched} orders (${result.skipped} legs not in current lineIndex)`);
    }
  } catch (err) {
    log.warn('DB', `Golf backfill failed: ${err.message}`);
  }

  // Load matched parlays (paginated in loadMatchedParlays).
  // Per Alec (PX): order.matched events are private — only fire on our own
  // wins. Legacy DB rows with outcome='lost' or 'other_sp' are
  // misclassified wins from before this was clarified. Count them in
  // totalMatched (they really did happen) but don't inflate weLost or
  // miscategorize the win counter.
  const dbMatched = await db.loadMatchedParlays(10000);
  let legacyMisclassified = 0;
  for (const m of dbMatched) {
    matchedParlays.push(m);
    marketStats.totalMatched++;
    if (m.weQuoted) {
      marketStats.weQuoted++;
      if (m.outcome === 'won') {
        marketStats.weWon++;
      } else if (m.outcome === 'lost' || m.outcome === 'other_sp') {
        // Legacy misclassified — treat as won for the counter so fill-rate
        // math reflects reality. weLost stays at 0.
        marketStats.weWon++;
        legacyMisclassified++;
      }
    } else {
      marketStats.missedNoQuote++;
    }
  }
  log.info('DB', `Loaded ${dbMatched.length} matched parlays${legacyMisclassified > 0 ? ` (${legacyMisclassified} legacy 'lost'/'other_sp' rows re-counted as wins per PX event-model clarification)` : ''}`);

  // Load declines in background — the declines table is too large for a
  // synchronous startup query. Positions and P&L don't depend on it.
  db.loadDeclines(2000).then(dbDeclines => {
    if (!dbDeclines || dbDeclines.length === 0) return;
    log.info('DB', `Background-loaded ${dbDeclines.length} declines`);
    hydrateDeclinesInMemory(dbDeclines);
  }).catch(err => {
    log.warn('DB', `Background loadDeclines failed: ${err.message}`);
  });
  log.info('DB', 'Decline loading deferred to background');

  // Back-fill fillBucketEvents for 24h/7d/30d heatmap windows. Runs in
  // the background — the heatmap will start empty and populate once the
  // Supabase query returns.
  backfillFillBucketEvents().catch(err => {
    log.warn('Tracker', `backfillFillBucketEvents failed: ${err.message}`);
  });
}
