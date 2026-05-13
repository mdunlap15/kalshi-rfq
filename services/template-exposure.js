/**
 * Template-exposure ramp
 *
 * Tracks confirmed bets grouped by PARLAY SIGNATURE (canonical leg tuple)
 * and applies a graduated vig ramp / hard decline when the same template
 * accumulates multiple confirmations inside a rolling window.
 *
 * Motivation — April 18 post-mortem (Apr 14-22 dataset):
 *   - 8 of 9 days had at least one repeated template
 *   - 15-23% of confirmed bets sat inside a repeated template every day
 *   - April 18 cliff: 6 bettors stacked "Rockies ML + Under 11" for
 *     -$5,769 in a single parlay signature, 9 bettors stacked the same
 *     MMA 6-leg for -$2,520. Two templates = 80% of the day's gross
 *     losses.
 *   - Counterfactual: blocking 4th+ bet on any signature across Apr 14-22
 *     would have avoided $5,443 in losses at a cost of $64 in foregone
 *     wins — 84:1 asymmetry.
 *
 * Existing team/event exposure caps don't catch this because the losses
 * come from ONE parlay copied across many counterparties. Template is a
 * first-class exposure dimension alongside team/event/sport.
 *
 * Mechanism:
 *   1. When a bet confirms (orderUuid arrives), record its canonical
 *      signature + stake against a rolling window.
 *   2. At price time, compute the RFQ's signature, look up current
 *      exposure (count + totalStake). Return a ramp decision:
 *        - extraVig: ADDITIVE to the existing vig rate (same units,
 *          stacks additively with longshotAdd, capped at 0.20 downstream)
 *        - decline: boolean, true when count has hit the hard cap
 *        - reason: decline reason for the pricer failure record
 *   3. Pricer adds extraVig to its vig rate, or bails with the reason.
 *
 * Ramp tiers are configurable. Defaults calibrated from the 9-day
 * counterfactual analysis:
 *   count=0 (1st bet) → 0 extra
 *   count=1 (2nd bet) → +0.25pp
 *   count=2 (3rd bet) → +1.0pp
 *   count=3 (4th bet) → +3.0pp
 *   count=4 (5th bet) → DECLINE
 *
 * Window: 24h rolling by default. Signatures with no confirmations in
 * the window get pruned periodically.
 *
 * Persistence: in-memory only. Boot reconstructs from recently-loaded
 * order history (rebuildFromOrders). No new DB table required.
 */

const { config } = require('../config');
const log = require('./logger');

const WINDOW_MS = (config.pricing.templateRampWindowHours || 24) * 60 * 60 * 1000;
const ENABLED = config.pricing.templateRampEnabled !== false;

// Pending reservation TTL — covers in-flight RFQs that haven't yet
// resolved to confirm/reject. Closes the timing race where multiple
// RFQs on the same signature land in seconds (faster than the
// confirm cycle) and all see priorCount=0. Operator caught this
// 2026-04-29 when 4 identical parlays cleared in 24s with no ramp.
//
// 5 minutes is long enough to span any reasonable RFQ→confirm cycle
// (typical 3-30s) plus PX auction timeouts (~60s) plus a safety
// buffer. Lost reservations expire on their own without leaking.
const PENDING_TTL_MS = 5 * 60 * 1000;

// signature -> {
//   confirmations: [{ parlayId, stake, confirmedAt (ms epoch) }],
//   pending: [{ parlayId, stake, reservedAt (ms epoch) }],
// }
const _exposure = {};

let _stats = {
  recordedConfirmations: 0,
  recordedPending: 0,
  releasedPending: 0,
  signaturesActive: 0,
  lastPrunedAt: null,
  rampHits: { tier2: 0, tier3: 0, tier4: 0, decline: 0, cooldown: 0, team_cooldown: 0 },
  // Confirm-time gate hits — bumped by checkConfirmCooldown / checkTeamCooldown
  // callers in websocket.handleConfirm when a race-window block fires.
  // Separate from rampHits so we can distinguish quote-time declines (no
  // commitment made) from confirm-time walk-aways (we already quoted and
  // PX wanted to book it, but we backed out).
  confirmHits: { template_cooldown: 0, team_cooldown: 0 },
  lastTeamCooldownTeam: null,
  lastTeamCooldownAt: null,
};

// --------------------------------------------------------------------
// Signature canonicalization
// --------------------------------------------------------------------

function normalizeTeam(s) {
  if (s == null) return '?';
  return String(s).trim().toLowerCase().slice(0, 60);
}

/**
 * Build a canonical JSON-string key from a parlay's legs. Order-
 * independent so bettors can't evade by reordering. Uses
 * (team, market, line) tuples — the same primitives the dashboard
 * uses to describe a parlay to humans.
 *
 * For SPREAD and TOTAL markets the line value is intentionally
 * COLLAPSED to a single bucket per (team, market). Apr 25 forensic
 * review of the recurring "Rockies + Under (Rockies @ Mets)" probe
 * showed bettors evading the ramp by submitting near-identical parlays
 * across 2-3 alt-lines (Under 8 vs 8.5 vs 9 on the same total leg,
 * Rockies +1.5 vs +2.5 on the same spread leg). Substantively the
 * same thesis but each landed on a distinct signature, so the ramp
 * never accumulated a count > 0. Dropping the line value at the
 * canonicalization step closes that evasion in one line.
 *
 * Spread SIDE (Rockies +1.5 vs Mets -1.5) is still preserved because
 * those are mathematically opposite bets (different theses, even
 * when paired with the same total leg). Same for Over vs Under.
 *
 * Moneyline legs already carry no line value, so they're unaffected.
 */
function canonicalSignature(legs) {
  if (!Array.isArray(legs) || legs.length === 0) return null;
  const tuples = legs.map(l => {
    const team = normalizeTeam(l.team || l.teamName || '?');
    const market = (l.market || l.marketType || '?').toLowerCase();
    // Collapse alt-line probing on spread/total markets. See header above.
    const isLineMarket = market === 'spread' || market === 'total' ||
                         market === 'team_total' || market === 'run_line' ||
                         market === 'puck_line' || market === 'alt_spread' ||
                         market === 'alt_total';
    const line = isLineMarket
      ? null
      : ((l.line != null && !isNaN(Number(l.line))) ? Number(l.line) : null);
    return [team, market, line];
  });
  // Stable sort to make ordering irrelevant
  tuples.sort((a, b) => {
    if (a[0] !== b[0]) return a[0] < b[0] ? -1 : 1;
    if (a[1] !== b[1]) return a[1] < b[1] ? -1 : 1;
    const la = a[2] == null ? -Infinity : a[2];
    const lb = b[2] == null ? -Infinity : b[2];
    return la - lb;
  });
  return JSON.stringify(tuples);
}

// --------------------------------------------------------------------
// Recording
// --------------------------------------------------------------------

/**
 * Called from order-tracker.recordConfirmation when a real fill lands
 * (orderUuid first arrival). Idempotent: duplicate parlayIds for the
 * same signature are ignored so replays / order-matched storms don't
 * double-count.
 */
function recordConfirmation(legs, parlayId, stake, confirmedAt = null) {
  if (!ENABLED) return;
  const sig = canonicalSignature(legs);
  if (!sig || !parlayId || !(stake > 0)) return;
  const ts = confirmedAt ? new Date(confirmedAt).getTime() : Date.now();
  if (isNaN(ts)) return;
  if (!_exposure[sig]) _exposure[sig] = { confirmations: [], pending: [] };
  if (!_exposure[sig].pending) _exposure[sig].pending = [];
  // Dedupe by parlayId on the confirmed lane.
  if (_exposure[sig].confirmations.some(c => c.parlayId === parlayId)) {
    // Already confirmed — but make sure it's not also lingering as pending.
    _exposure[sig].pending = _exposure[sig].pending.filter(p => p.parlayId !== parlayId);
    return;
  }
  _exposure[sig].confirmations.push({ parlayId, stake, confirmedAt: ts });
  // Graduate from pending → confirmed: remove any matching pending entry
  // so the parlay isn't counted twice on next exposure read.
  _exposure[sig].pending = _exposure[sig].pending.filter(p => p.parlayId !== parlayId);
  _stats.recordedConfirmations++;

  // Update per-TEAM last-confirmed map. The signature-level confirmations[]
  // above only catches exact same-leg duplicates; the team map below catches
  // the broader bot pattern where the same target team is rotated through
  // different secondary legs (Mike caught 2026-05-13: Seattle Storm in
  // S+Det, S+Det, S+Cle within 30s). One entry per team in the parlay.
  for (const leg of legs) {
    const tk = normalizeTeam(leg.team || leg.teamName || '');
    if (!tk || tk === '?') continue;
    const existing = _lastConfirmedAtByTeam.get(tk);
    if (!existing || existing.confirmedAt < ts) {
      _lastConfirmedAtByTeam.set(tk, { confirmedAt: ts, parlayId });
    }
  }
}

// Per-team last-confirmed map. Independent dimension from the signature-
// keyed _exposure store: tracks the wall-clock timestamp of the most
// recent confirmed parlay that contained each team. Used by
// checkTeamCooldown to gate new RFQs on rapid-succession bot patterns.
//
// Memory bounded by the universe of distinct team names we ever quote,
// which is small (a few thousand teams + players across all sports/
// seasons). No pruning needed — stale entries are cheap and the
// cooldown check naturally ignores anything past cooldownSec.
const _lastConfirmedAtByTeam = new Map(); // teamKey -> { confirmedAt, parlayId }

/**
 * Per-team cooldown check. Returns { block: true, ... } if any team in
 * `legs` was present in another parlay that confirmed within
 * teamCooldownSeconds. Excludes self by parlayId so confirm-time
 * idempotency doesn't self-block.
 *
 * Designed to fire at BOTH quote time (in getRampDecision) and confirm
 * time (called from handleConfirm). Quote-time blocks make us appear
 * unresponsive to the bot's templated rotations; confirm-time blocks
 * close the race window where the first parlay hasn't confirmed yet
 * when later RFQs land.
 *
 * `legs` shape: array of { team | teamName, market | marketType, line }.
 */
function checkTeamCooldown(legs, parlayId, nowMs = null, opts = {}) {
  if (!ENABLED) return { block: false, reason: null };
  const cooldownSec = config.pricing.teamCooldownSeconds;
  if (!cooldownSec || cooldownSec <= 0) return { block: false, reason: null };
  if (!Array.isArray(legs) || legs.length === 0) return { block: false, reason: null };
  const now = nowMs || Date.now();
  for (const leg of legs) {
    const team = leg.team || leg.teamName || '';
    const tk = normalizeTeam(team);
    if (!tk || tk === '?') continue;
    const last = _lastConfirmedAtByTeam.get(tk);
    if (!last) continue;
    if (last.parlayId === parlayId) continue; // self-exclusion
    const sinceMs = now - last.confirmedAt;
    if (sinceMs < cooldownSec * 1000) {
      const remainSec = Math.ceil((cooldownSec * 1000 - sinceMs) / 1000);
      // Bump the confirm-time counter only for confirm-stage callers.
      // The quote-time caller (getRampDecision) bumps its own rampHits
      // counter and would double-count if we bumped here too.
      if (opts.source === 'confirm') {
        _stats.confirmHits.team_cooldown++;
        _stats.lastTeamCooldownTeam = team;
        _stats.lastTeamCooldownAt = new Date(now).toISOString();
      }
      return {
        block: true,
        team,
        reason: `team_cooldown: "${team}" confirmed in another parlay ${Math.round(sinceMs / 1000)}s ago — cooldown ${cooldownSec}s, ${remainSec}s remaining`,
        sinceMs,
      };
    }
  }
  return { block: false, reason: null };
}

/**
 * Reserve a pending slot for an in-flight RFQ. Called from the pricer
 * once we've decided to quote (i.e. ramp decision was non-decline).
 * Subsequent RFQs on the same signature will see this reservation in
 * priorCount, closing the timing race where 4 RFQs in 24s all see
 * count=0 because none had confirmed yet.
 *
 * Idempotent: same parlayId reserving twice is a no-op (e.g. retried
 * pricing on the same RFQ shouldn't double-count).
 *
 * Stake is approximate at reservation time (RFQ's max_risk or 0). The
 * actual confirmedStake is recorded later via recordConfirmation, which
 * also removes the pending entry as part of the graduation.
 */
function reservePending(legs, parlayId, stake = 0) {
  if (!ENABLED) return;
  const sig = canonicalSignature(legs);
  if (!sig || !parlayId) return;
  if (!_exposure[sig]) _exposure[sig] = { confirmations: [], pending: [] };
  if (!_exposure[sig].pending) _exposure[sig].pending = [];
  // Dedupe across BOTH lanes — already confirmed shouldn't get a pending too.
  if (_exposure[sig].confirmations.some(c => c.parlayId === parlayId)) return;
  if (_exposure[sig].pending.some(p => p.parlayId === parlayId)) return;
  _exposure[sig].pending.push({
    parlayId,
    stake: Number.isFinite(+stake) && +stake > 0 ? +stake : 0,
    reservedAt: Date.now(),
  });
  _stats.recordedPending++;
}

/**
 * Release a pending reservation. Called when a quote is rejected,
 * declined post-pricing, or otherwise definitively will not become a
 * confirmation. Safe to call on a parlayId that was never reserved
 * or has already graduated to confirmed (no-op in those cases).
 *
 * Searches all signatures because the caller (rejection handler)
 * typically has parlayId but not the legs at that point. Cheap because
 * pending is small per signature and most signatures have zero pending.
 */
function releasePending(parlayId) {
  if (!ENABLED) return;
  if (!parlayId) return;
  let removed = 0;
  for (const sig of Object.keys(_exposure)) {
    const entry = _exposure[sig];
    if (!entry.pending || entry.pending.length === 0) continue;
    const before = entry.pending.length;
    entry.pending = entry.pending.filter(p => p.parlayId !== parlayId);
    removed += (before - entry.pending.length);
  }
  if (removed > 0) _stats.releasedPending += removed;
}

/**
 * Return current in-window exposure for a given parlay signature.
 * Also prunes expired entries for that signature lazily.
 */
function getExposure(legs, nowMs = null) {
  const sig = canonicalSignature(legs);
  if (!sig || !_exposure[sig]) {
    return {
      signature: sig, count: 0, confirmedCount: 0, pendingCount: 0,
      totalStake: 0, firstAt: null, lastAt: null,
    };
  }
  const now = nowMs || Date.now();
  const confirmedCutoff = now - WINDOW_MS;
  const pendingCutoff = now - PENDING_TTL_MS;
  const entry = _exposure[sig];
  entry.confirmations = entry.confirmations.filter(c => c.confirmedAt >= confirmedCutoff);
  entry.pending = (entry.pending || []).filter(p => p.reservedAt >= pendingCutoff);
  // Combined count is what drives the ramp decision — confirmed plus
  // in-flight reservations. This closes the timing race where multiple
  // RFQs on the same signature land before any confirms.
  const confirmedCount = entry.confirmations.length;
  const pendingCount = entry.pending.length;
  const totalCount = confirmedCount + pendingCount;
  if (totalCount === 0) {
    delete _exposure[sig];
    return {
      signature: sig, count: 0, confirmedCount: 0, pendingCount: 0,
      totalStake: 0, firstAt: null, lastAt: null,
    };
  }
  const confirmedStake = entry.confirmations.reduce((s, c) => s + c.stake, 0);
  const pendingStake = entry.pending.reduce((s, p) => s + p.stake, 0);
  const totalStake = confirmedStake + pendingStake;
  const allTimes = [
    ...entry.confirmations.map(c => c.confirmedAt),
    ...entry.pending.map(p => p.reservedAt),
  ];
  const firstAt = Math.min(...allTimes);
  const lastAt = Math.max(...allTimes);
  return {
    signature: sig,
    count: totalCount,
    confirmedCount,
    pendingCount,
    totalStake,
    confirmedStake,
    pendingStake,
    firstAt: new Date(firstAt).toISOString(),
    lastAt: new Date(lastAt).toISOString(),
  };
}

// --------------------------------------------------------------------
// Ramp decision
// --------------------------------------------------------------------

/**
 * Given a parlay's legs, return the ramp decision:
 *   { extraVig: number, decline: boolean, reason: string|null,
 *     count: number, confirmedCount: number, pendingCount: number,
 *     totalStake: number }
 *
 * count is the number of prior bets on this signature inside the window —
 * sum of CONFIRMED bets and in-flight PENDING reservations. The current
 * RFQ is NOT counted yet; if the decision is non-decline AND opts.parlayId
 * is supplied, this call will atomically reserve a pending slot for it
 * so the next concurrent RFQ on the same signature sees an incremented
 * count.
 *
 * Closes the timing race observed 2026-04-29 where 4 identical parlays
 * cleared in 24s with no ramp because none had confirmed yet.
 *
 * @param {Array} legs - parlay legs (team/market/line tuples)
 * @param {object} [opts]
 * @param {string} [opts.parlayId] - if provided, auto-reserves on non-decline
 * @param {number} [opts.estStake] - estimated stake at quote time (max_risk)
 */
function getRampDecision(legs, opts = {}) {
  if (!ENABLED) {
    return {
      extraVig: 0, decline: false, reason: null,
      count: 0, confirmedCount: 0, pendingCount: 0, totalStake: 0,
    };
  }
  const exp = getExposure(legs);
  const priorCount = exp.count;

  // Tiered defaults; all knobs Railway-tunable.
  const declineAt = config.pricing.templateRampDeclineAt;      // e.g. 4 → decline 5th+ bet
  const tier2Add  = config.pricing.templateRampTier2Add;       // added for 2nd bet (priorCount==1)
  const tier3Add  = config.pricing.templateRampTier3Add;       // added for 3rd bet (priorCount==2)
  const tier4Add  = config.pricing.templateRampTier4Add;       // added for 4th bet (priorCount==3)
  const cooldownSec = config.pricing.templateRampCooldownSeconds;

  // Short-window cooldown: any RFQ on a signature whose most recent
  // CONFIRMED bet landed within the cooldown window declines, regardless
  // of count or counterparty. Closes the timing race where multiple
  // bettors copy the same parlay before the per-template confirm
  // feedback can register through the longer 24h ramp tiers. Operator-
  // requested 2026-05-06 after watching same-template clusters slip
  // through within seconds. Pending reservations are NOT considered
  // here — only confirmed bets — because pending RFQs can still be
  // rejected by PX downstream (e.g. confirmation drift) and we don't
  // want to reject a bettor's first attempt because their own RFQ is
  // sitting in-flight.
  if (cooldownSec > 0 && exp.confirmedCount > 0) {
    const sig = exp.signature;
    const entry = _exposure[sig];
    if (entry && entry.confirmations && entry.confirmations.length > 0) {
      const lastConfirmedMs = Math.max(...entry.confirmations.map(c => c.confirmedAt));
      const sinceMs = Date.now() - lastConfirmedMs;
      if (sinceMs < cooldownSec * 1000) {
        _stats.rampHits.cooldown = (_stats.rampHits.cooldown || 0) + 1;
        const remainSec = Math.ceil((cooldownSec * 1000 - sinceMs) / 1000);
        return {
          extraVig: 0, decline: true,
          reason: `template_cooldown: same parlay confirmed ${Math.round(sinceMs / 1000)}s ago — cooldown is ${cooldownSec}s, ${remainSec}s remaining`,
          count: priorCount,
          confirmedCount: exp.confirmedCount,
          pendingCount: exp.pendingCount,
          totalStake: exp.totalStake,
          cooldownActive: true,
          cooldownRemainingSec: remainSec,
        };
      }
    }
  }

  // Per-team cooldown — broader than signature-level above. Catches bot
  // rotations where the same target team appears with different 2nd legs.
  // Returns early before the ramp-tier vig add, so a rotation hit is
  // declined entirely rather than just priced wider.
  const teamCd = checkTeamCooldown(legs, opts ? opts.parlayId : null);
  if (teamCd.block) {
    _stats.rampHits.team_cooldown = (_stats.rampHits.team_cooldown || 0) + 1;
    return {
      extraVig: 0, decline: true,
      reason: teamCd.reason,
      count: priorCount,
      confirmedCount: exp.confirmedCount,
      pendingCount: exp.pendingCount,
      totalStake: exp.totalStake,
      teamCooldownActive: true,
      teamCooldownTeam: teamCd.team,
    };
  }

  if (declineAt > 0 && priorCount >= declineAt) {
    _stats.rampHits.decline++;
    return {
      extraVig: 0, decline: true,
      reason: `template_cap: ${priorCount} prior bets on this signature (${exp.confirmedCount} confirmed + ${exp.pendingCount} pending) in ${WINDOW_MS / 3600000}h window`,
      count: priorCount,
      confirmedCount: exp.confirmedCount,
      pendingCount: exp.pendingCount,
      totalStake: exp.totalStake,
    };
  }

  let extraVig = 0;
  if (priorCount === 1)      { extraVig = tier2Add; _stats.rampHits.tier2++; }
  else if (priorCount === 2) { extraVig = tier3Add; _stats.rampHits.tier3++; }
  else if (priorCount >= 3)  { extraVig = tier4Add; _stats.rampHits.tier4++; }

  // Atomically reserve a pending slot so the NEXT concurrent RFQ on
  // this signature sees an incremented count. This is the timing-race
  // fix: 4 RFQs in 24s used to all see priorCount=0 because none had
  // confirmed yet. Now the 2nd sees count=1, 3rd sees count=2, 4th
  // sees count=3, and each gets the matching ramp tier.
  if (opts && opts.parlayId) {
    reservePending(legs, opts.parlayId, opts.estStake);
  }

  return {
    extraVig, decline: false, reason: null,
    count: priorCount,
    confirmedCount: exp.confirmedCount,
    pendingCount: exp.pendingCount,
    totalStake: exp.totalStake,
  };
}

/**
 * Confirm-time template-cooldown gate. The quote-time check in
 * getRampDecision intentionally ignores pending reservations (so a
 * bettor's retry isn't blocked by their own in-flight quote). That
 * trade-off opens a race window: multiple RFQs on the same signature
 * can quote out before any of them confirm, and PX can then book all
 * of them — exactly the rapid-duplicate pattern Mike caught
 * (2026-05-13: three Seattle Storm +3.5 + Detroit Pistons -4 confirms
 * within 30 seconds at +278 each).
 *
 * This gate runs in handleConfirm BEFORE recordConfirmation. If
 * another parlay on the same signature has already confirmed within
 * cooldownSec, we walk away from the new one. The first parlay still
 * fills; only the rapid copies decline. Bettor sees a rejected
 * confirm — annoying but the correct outcome since they shouldn't
 * have been able to fill that fast in the first place.
 *
 * Excludes the parlay being confirmed itself from the lookup so
 * recordConfirmation idempotency (re-running on the same parlayId)
 * doesn't accidentally self-block.
 *
 * Returns { block: bool, reason: string|null, sinceMs: number|null }.
 */
function checkConfirmCooldown(legs, parlayId, nowMs = null) {
  if (!ENABLED) return { block: false, reason: null, sinceMs: null };
  const cooldownSec = config.pricing.templateRampCooldownSeconds;
  if (!cooldownSec || cooldownSec <= 0) return { block: false, reason: null, sinceMs: null };
  const sig = canonicalSignature(legs);
  if (!sig) return { block: false, reason: null, sinceMs: null };
  const entry = _exposure[sig];
  if (!entry || !entry.confirmations || entry.confirmations.length === 0) {
    return { block: false, reason: null, sinceMs: null };
  }
  const now = nowMs || Date.now();
  // Exclude self if this parlay's confirmation has already been recorded
  // (paranoid — confirmer should call this BEFORE recordConfirmation).
  const others = entry.confirmations.filter(c => c.parlayId !== parlayId);
  if (others.length === 0) return { block: false, reason: null, sinceMs: null };
  const lastConfirmedMs = Math.max(...others.map(c => c.confirmedAt));
  const sinceMs = now - lastConfirmedMs;
  if (sinceMs < cooldownSec * 1000) {
    const remainSec = Math.ceil((cooldownSec * 1000 - sinceMs) / 1000);
    _stats.confirmHits.template_cooldown++;
    return {
      block: true,
      reason: `template_cooldown_at_confirm: same parlay confirmed ${Math.round(sinceMs / 1000)}s ago — cooldown ${cooldownSec}s, ${remainSec}s remaining`,
      sinceMs,
    };
  }
  return { block: false, reason: null, sinceMs };
}

// --------------------------------------------------------------------
// Maintenance
// --------------------------------------------------------------------

function prune(nowMs = null) {
  const now = nowMs || Date.now();
  const confirmedCutoff = now - WINDOW_MS;
  const pendingCutoff = now - PENDING_TTL_MS;
  let pruned = 0;
  let pendingDropped = 0;
  for (const sig of Object.keys(_exposure)) {
    const entry = _exposure[sig];
    entry.confirmations = entry.confirmations.filter(c => c.confirmedAt >= confirmedCutoff);
    if (entry.pending && entry.pending.length > 0) {
      const before = entry.pending.length;
      entry.pending = entry.pending.filter(p => p.reservedAt >= pendingCutoff);
      pendingDropped += (before - entry.pending.length);
    }
    if (entry.confirmations.length === 0 && (!entry.pending || entry.pending.length === 0)) {
      delete _exposure[sig];
      pruned++;
    }
  }
  _stats.lastPrunedAt = new Date(now).toISOString();
  _stats.signaturesActive = Object.keys(_exposure).length;
  if (pendingDropped > 0) _stats.releasedPending += pendingDropped;
  return pruned;
}

let _pruneTimer = null;
function startPruneLoop(intervalMs = 5 * 60 * 1000) {
  if (_pruneTimer) return;
  _pruneTimer = setInterval(() => {
    try { prune(); } catch (err) { log.warn('TemplateExposure', `prune failed: ${err.message}`); }
  }, intervalMs);
}

/**
 * Rebuild the in-memory exposure map from an array of already-loaded
 * orders. Called at service boot once order-tracker has hydrated from
 * Supabase. Only orders with orderUuid + confirmedStake + confirmedAt
 * within the window contribute.
 */
function rebuildFromOrders(orders) {
  const cutoff = Date.now() - WINDOW_MS;
  let added = 0;
  for (const o of orders || []) {
    if (!o.orderUuid) continue;
    if (!(o.confirmedStake > 0)) continue;
    if (!o.confirmedAt) continue;
    const ts = new Date(o.confirmedAt).getTime();
    if (isNaN(ts) || ts < cutoff) continue;
    const legs = o.legs || (o.meta && o.meta.legs) || [];
    if (legs.length === 0) continue;
    recordConfirmation(legs, o.parlayId, o.confirmedStake, o.confirmedAt);
    added++;
  }
  _stats.signaturesActive = Object.keys(_exposure).length;
  log.info('TemplateExposure', `Rebuilt from history: ${added} confirmations across ${_stats.signaturesActive} signatures (window ${WINDOW_MS / 3600000}h)`);
  return added;
}

// --------------------------------------------------------------------
// Stats
// --------------------------------------------------------------------

function getStats() {
  // Snapshot — include top-N active signatures by combined count
  // (confirmed + pending) for observability.
  const top = Object.entries(_exposure)
    .map(([sig, e]) => {
      const confirmedCount = (e.confirmations || []).length;
      const pendingCount = (e.pending || []).length;
      const stake = (e.confirmations || []).reduce((s, c) => s + c.stake, 0)
        + (e.pending || []).reduce((s, p) => s + p.stake, 0);
      return {
        signature: sig.slice(0, 100) + (sig.length > 100 ? '…' : ''),
        count: confirmedCount + pendingCount,
        confirmedCount,
        pendingCount,
        totalStake: stake,
      };
    })
    .sort((a, b) => b.count - a.count || b.totalStake - a.totalStake)
    .slice(0, 10);
  return {
    enabled: ENABLED,
    windowHours: WINDOW_MS / 3600000,
    pendingTtlSeconds: PENDING_TTL_MS / 1000,
    ..._stats,
    // Override _stats.signaturesActive with a live count (prune/rebuild
    // only update the stale field on their own schedules).
    signaturesActive: Object.keys(_exposure).length,
    tiers: {
      tier2Add: config.pricing.templateRampTier2Add,
      tier3Add: config.pricing.templateRampTier3Add,
      tier4Add: config.pricing.templateRampTier4Add,
      declineAt: config.pricing.templateRampDeclineAt,
      cooldownSeconds: config.pricing.templateRampCooldownSeconds,
    },
    topActive: top,
  };
}

module.exports = {
  canonicalSignature,
  recordConfirmation,
  reservePending,
  releasePending,
  getExposure,
  getRampDecision,
  checkConfirmCooldown,
  checkTeamCooldown,
  prune,
  startPruneLoop,
  rebuildFromOrders,
  getStats,
};
