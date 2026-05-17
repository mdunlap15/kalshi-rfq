/**
 * Signature cooldown lock — minimalist backstop for "block same parlay
 * for N seconds after it confirms."
 *
 * Why this exists separately from template-exposure.js:
 *   The existing TEMPLATE_RAMP_COOLDOWN check depends on
 *   templateExposure.recordConfirmation() being called when a parlay
 *   confirms. We discovered (2026-05-17 forensics on the back-to-back
 *   Atlanta+3.5/Detroit ML pair) that the order.matched side-channel
 *   path sets order.status='confirmed' WITHOUT calling recordConfirmation,
 *   so the cooldown map stays empty between order.matched and the
 *   subsequent order.finalized event. A second RFQ on the same signature
 *   landing in that window passes every confirmed-count-based check.
 *
 *   This module is the belt-and-suspenders. Its lockSignature() is
 *   called from BOTH status='confirmed' sites (the matched side-channel
 *   AND recordConfirmation), so the moment any code path knows a parlay
 *   is confirmed, the cooldown is armed regardless of which other state
 *   updates ran. checkSignatureCooldown() is called as the FIRST check
 *   in pricer.shouldDecline so it's authoritative — no other logic can
 *   silently bypass it.
 *
 * Configurable via SIGNATURE_COOLDOWN_SECONDS env var (default 120).
 * Set to 0 to disable. In-memory only — a restart wipes locks (acceptable
 * given the 2-min default window: only ~2 min of bot pattern flow is
 * exposed during restart, which is rare).
 */

const log = require('./logger');

const COOLDOWN_MS = (parseInt(process.env.SIGNATURE_COOLDOWN_SECONDS, 10) || 120) * 1000;
const ENABLED = COOLDOWN_MS > 0;

// signature key → confirmedAt timestamp (ms epoch)
const _locks = new Map();

/**
 * Build the canonical signature for cooldown matching. Order-independent;
 * collapses spread/total line values like template-exposure's canonical-
 * Signature so a bettor can't evade by switching to ±0.5 / ±2.5.
 *
 * Kept separate from template-exposure.canonicalSignature to make this
 * module standalone (no cross-module dependency that could break the
 * backstop). If you change one, change both.
 */
function _normTeam(s) {
  return s == null ? '?' : String(s).trim().toLowerCase().slice(0, 60);
}
function buildSigKey(legs) {
  if (!Array.isArray(legs) || legs.length === 0) return null;
  const tuples = legs.map(l => {
    const team = _normTeam(l.team || l.teamName || '?');
    const market = (l.market || l.marketType || '?').toLowerCase();
    const isLineMarket = market === 'spread' || market === 'total' ||
                         market === 'team_total' || market === 'run_line' ||
                         market === 'puck_line' || market === 'alt_spread' ||
                         market === 'alt_total';
    // For spread/total, collapse the line value so ±0.5 vs ±2.5 dodging
    // is foiled. Selection still preserved (home vs away, over vs under).
    const sel = (l.selection || '').toLowerCase() || null;
    const line = isLineMarket ? null
      : ((l.line != null && !isNaN(Number(l.line))) ? Number(l.line) : null);
    return [team, market, sel, line];
  });
  tuples.sort((a, b) => {
    if (a[0] !== b[0]) return a[0] < b[0] ? -1 : 1;
    if (a[1] !== b[1]) return a[1] < b[1] ? -1 : 1;
    if (a[2] !== b[2]) return (a[2] || '') < (b[2] || '') ? -1 : 1;
    const la = a[3] == null ? -Infinity : a[3];
    const lb = b[3] == null ? -Infinity : b[3];
    return la - lb;
  });
  return JSON.stringify(tuples);
}

/**
 * Arm the cooldown for this parlay's signature. Idempotent — re-locking
 * the same signature within the window just refreshes the timestamp.
 * Called from EVERY code path that sets order.status='confirmed' so the
 * lock is set as soon as any path knows the parlay confirmed.
 *
 * No-op when disabled (COOLDOWN_MS=0).
 */
function lockSignature(legs, parlayId) {
  if (!ENABLED) return false;
  const sig = buildSigKey(legs);
  if (!sig) return false;
  const now = Date.now();
  const prev = _locks.get(sig);
  _locks.set(sig, now);
  // Light logging so the operator can see this firing in Railway logs.
  // Only logs on FIRST lock for a signature in the window (not refreshes).
  if (!prev || (now - prev) >= COOLDOWN_MS) {
    log.info('SigCooldown', `Locked signature for ${COOLDOWN_MS/1000}s (parlay=${parlayId || '?'})`);
  }
  return true;
}

/**
 * Check the cooldown for this parlay's signature. Returns:
 *   { block: true, ageMs, remainingMs } if a same-sig confirmed within
 *     COOLDOWN_MS — the caller should decline the RFQ.
 *   null otherwise (no lock or expired).
 *
 * Lazy expiry: an expired lock is removed from the map on access so the
 * Map doesn't grow unbounded.
 */
function checkSignatureCooldown(legs) {
  if (!ENABLED) return null;
  const sig = buildSigKey(legs);
  if (!sig) return null;
  const lastMs = _locks.get(sig);
  if (!lastMs) return null;
  const ageMs = Date.now() - lastMs;
  if (ageMs >= COOLDOWN_MS) {
    _locks.delete(sig); // expired — clean up
    return null;
  }
  return { block: true, ageMs, remainingMs: COOLDOWN_MS - ageMs };
}

/**
 * Operator override — clears the lock for a specific signature so the
 * next same-sig RFQ passes. Used by /admin/sig-cooldown-clear when the
 * operator decides they're OK with another bet on this shape (analogous
 * to the template-cap override pattern).
 */
function clearSignature(legs) {
  const sig = buildSigKey(legs);
  if (!sig) return false;
  return _locks.delete(sig);
}

function getActiveLocks() {
  const now = Date.now();
  const out = [];
  for (const [sig, ts] of _locks.entries()) {
    const ageMs = now - ts;
    if (ageMs >= COOLDOWN_MS) continue;
    out.push({ sigPreview: sig.slice(0, 120), ageMs, remainingMs: COOLDOWN_MS - ageMs });
  }
  return out;
}

function getStats() {
  return {
    enabled: ENABLED,
    cooldownSeconds: COOLDOWN_MS / 1000,
    activeLocks: _locks.size,
  };
}

module.exports = {
  lockSignature,
  checkSignatureCooldown,
  clearSignature,
  getActiveLocks,
  getStats,
  buildSigKey, // exported for tests
};
