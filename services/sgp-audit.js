/**
 * SGP (Same-Game Parlay) shadow logging — Phase 0.
 *
 * Pure observability. When pricer.shouldDecline returns
 * 'prop_correlation_same_game' (and the audit gate is ON), this module
 * captures the parlay's leg composition into Supabase so we can analyze
 * the actual distribution of declined SGP shapes BEFORE designing
 * Phase 1 pricing.
 *
 * Gated by SGP_SHADOW_LOGGING env var (default OFF). When off, every
 * code path here is a no-op. Operator flips to "true" on Railway when
 * ready to start data collection. Fire-and-forget DB writes — RFQ
 * latency stays unchanged.
 *
 * Required Supabase table (run once in SQL editor):
 *
 *   CREATE TABLE IF NOT EXISTS sgp_audit (
 *     parlay_id TEXT PRIMARY KEY,
 *     seen_at TIMESTAMPTZ DEFAULT NOW(),
 *     decline_reason TEXT NOT NULL,
 *     px_event_id TEXT,
 *     leg_count INT,
 *     prop_count INT,
 *     other_count INT,
 *     combo_signature TEXT,
 *     legs JSONB
 *   );
 *   CREATE INDEX IF NOT EXISTS idx_sgp_audit_seen_at ON sgp_audit (seen_at DESC);
 *   CREATE INDEX IF NOT EXISTS idx_sgp_audit_combo_signature ON sgp_audit (combo_signature);
 *
 * Without the table the helper silently no-ops (warn-once at boot).
 */
const log = require('./logger');
const db = require('./db');

function isEnabled() {
  return process.env.SGP_SHADOW_LOGGING === 'true';
}

/**
 * Build a compact one-line shape descriptor for the parlay. Lets us
 * GROUP BY combo_signature in SQL to see what shapes dominate.
 *
 * Examples:
 *   "2props_0other_mlb"        (two MLB hitter props, no game-line legs)
 *   "1prop_1total_nba"         (one NBA player prop + game total)
 *   "1prop_1moneyline_mlb"     (one MLB HR prop + moneyline)
 *   "3props_1spread_nba"       (three NBA props + a spread)
 */
function buildComboSignature(props, others) {
  const otherTypes = others
    .map(li => (li.marketType || 'unknown').replace(/^player_/, ''))
    .sort();
  // Sport — take the first prop's sport (props always present in SGP decline).
  // Use the LAST segment of the sport key so "basketball_nba" → "nba",
  // "basketball_wnba" → "wnba", "baseball_mlb" → "mlb". Distinguishes the
  // common cases for combo analysis. Falls back to the whole key if no _.
  const sport = (props[0] && props[0].sport) || (others[0] && others[0].sport) || 'unknown';
  const sportShort = sport.includes('_') ? sport.split('_').pop() : sport;
  // Tally other-type counts
  const otherTally = {};
  for (const t of otherTypes) otherTally[t] = (otherTally[t] || 0) + 1;
  const otherStr = Object.entries(otherTally)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([t, n]) => n === 1 ? t : `${n}${t}`)
    .join('+');
  const propPart = props.length === 1 ? '1prop' : `${props.length}props`;
  const otherPart = otherStr || '0other';
  return `${propPart}_${otherPart}_${sportShort}`;
}

/**
 * Extract minimal lineInfo into a flat object for JSONB storage.
 * Dropping fields we don't need for SGP analysis to keep row size down.
 */
function legSnapshot(lineInfo, isProp) {
  return {
    isProp,
    sport: lineInfo.sport || null,
    pxEventId: lineInfo.pxEventId || null,
    marketType: lineInfo.marketType || null,
    propType: lineInfo.propType || null,
    playerName: lineInfo.playerName || null,
    teamName: lineInfo.teamName || null,
    line: lineInfo.line != null ? lineInfo.line : null,
    selection: lineInfo.selection || null,
    fairProb: lineInfo.fairProb != null ? lineInfo.fairProb : null,
    homeTeam: lineInfo.homeTeam || null,
    awayTeam: lineInfo.awayTeam || null,
  };
}

/**
 * Log a same-game-correlation decline. Fire-and-forget — never throws,
 * never blocks the RFQ path. Caller passes the parlayId, the offending
 * pxEventId, and the props + others arrays from the SGP-block detection.
 */
function logSgpDecline(parlayId, pxEventId, props, others) {
  if (!isEnabled()) return;
  if (!parlayId) return; // can't dedupe without a key
  try {
    const propSnaps = (props || []).map(li => legSnapshot(li, true));
    const otherSnaps = (others || []).map(li => legSnapshot(li, false));
    const row = {
      parlay_id: parlayId,
      decline_reason: 'prop_correlation_same_game',
      px_event_id: pxEventId != null ? String(pxEventId) : null,
      leg_count: propSnaps.length + otherSnaps.length,
      prop_count: propSnaps.length,
      other_count: otherSnaps.length,
      combo_signature: buildComboSignature(props || [], others || []),
      legs: [...propSnaps, ...otherSnaps],
    };
    // Async — never await, never block
    db.saveSgpAudit(row).catch(err => {
      // Silent unless it's a structural failure
      if (!logSgpDecline._warned) {
        log.warn('SGP-Audit', `saveSgpAudit failed (logged once): ${err.message}`);
        logSgpDecline._warned = true;
      }
    });
  } catch (err) {
    // Never let the audit path break the RFQ path
    if (!logSgpDecline._exWarned) {
      log.warn('SGP-Audit', `logSgpDecline exception (logged once): ${err.message}`);
      logSgpDecline._exWarned = true;
    }
  }
}

module.exports = {
  logSgpDecline,
  isEnabled,
  buildComboSignature, // exported for tests / verification scripts
};
