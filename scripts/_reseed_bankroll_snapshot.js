// Re-seed the bankroll snapshot for the corrected Mike→Rick transfer.
//
// 2026-05-15 (revised): Mike is sending Rick $5,000 from his own separate
// account (not from PX). Trading-account total equity is unchanged. Mike's
// trading-account claim increases by $5K (he's "buying" Rick's ownership
// share); Rick's decreases by $5K. The pre-cohort is refreshed to capture
// all parlays open at this revised redistribution point.
//
// Differs from the original seed:
//   - Amount $2K → $5K
//   - Money path: PX withdrawal → external personal-account transfer
//     (so no PX cash movement; live TE unchanged at $54,460.36)
//   - Pre-cohort refreshed to include parlays placed since the original
//     (incorrect) snapshot
//   - pendingWithdrawal removed (nothing pending on PX)

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const SNAPSHOT_AT = new Date().toISOString();

// Pre-transfer state captured earlier from PX:
//   cash $39,986.71 + open exposure $14,473.65 = TE $54,460.36
//   Mike $31,768.54 / Rick $22,691.82 (58.333% / 41.667%)
//
// After Mike pays Rick $5K externally (no PX cash movement):
//   Mike: $31,768.54 + $5,000 = $36,768.54
//   Rick: $22,691.82 - $5,000 = $17,691.82
//   Total: $54,460.36 (unchanged)
const SNAPSHOT_EQUITY = 54460.36;
const MIKE_POST       = 36768.54;
const RICK_POST       = 17691.82;

const PRE_RATIO  = { mike: 7 / 12, rick: 5 / 12 };
const POST_RATIO = {
  mike: MIKE_POST / SNAPSHOT_EQUITY,
  rick: RICK_POST / SNAPSHOT_EQUITY,
};

(async () => {
  const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

  // Fetch the LIVE open positions so the pre-cohort includes any parlays
  // placed between the original (incorrect) snapshot and now. Mike's
  // direction: "leave the redistribution point at what the parlays prior to
  // this" → cohort = all currently-open parlays.
  const auth = 'Basic ' + Buffer.from(`${process.env.AUTH_USERNAME}:${process.env.AUTH_PASSWORD}`).toString('base64');
  const PROD = process.env.PROD_URL || 'https://prophetx-rfq-production-6781.up.railway.app';
  console.log('Fetching /px-positions (fresh)...');
  const resp = await fetch(PROD + '/px-positions', { headers: { Authorization: auth } });
  if (!resp.ok) { console.error('px-positions HTTP', resp.status); process.exit(1); }
  const body = await resp.json();
  if (!body.ok || !Array.isArray(body.positions)) { console.error('Bad response'); process.exit(1); }
  const preCohortIds = body.positions.map(p => p.parlayId).filter(Boolean);
  console.log(`Captured ${preCohortIds.length} open positions, total stake $${body.totalStake.toFixed(2)}`);

  // Also pull live TE to verify the unchanged-equity assumption.
  const [bal, pnl] = await Promise.all([
    fetch(PROD + '/balance', { headers: { Authorization: auth } }).then(r => r.json()),
    fetch(PROD + '/px-pnl',  { headers: { Authorization: auth } }).then(r => r.json()),
  ]);
  const liveTE = (bal?.balance?.balance || 0) + (pnl?.openExposure || 0);
  console.log(`Live TE now: $${liveTE.toFixed(2)}  (snapshot expects $${SNAPSHOT_EQUITY.toFixed(2)})`);
  const teDrift = liveTE - SNAPSHOT_EQUITY;
  if (Math.abs(teDrift) > 50) {
    console.warn(`⚠️  TE drift $${teDrift.toFixed(2)} > $50.`);
    console.warn('    Parlays settling since the pre-transfer measurement have shifted TE.');
    console.warn('    Snapshot constants are based on the earlier reading. Drift will show in /bankroll.');
  } else {
    console.log('✓ TE drift within $50 — snapshot constants align with live PX.');
  }

  const snapshot = {
    version: 2,  // bumped from 1 — supersedes the prior snapshot
    snapshotAt: SNAPSHOT_AT,
    snapshotEquity: SNAPSHOT_EQUITY,
    snapshotMike: MIKE_POST,
    snapshotRick: RICK_POST,
    preCohortIds,
    preCohortStakeAtSnapshot: body.totalStake,
    preRatio: PRE_RATIO,
    postRatio: POST_RATIO,
    // No pending withdrawal — the $5K moves between Mike's and Rick's
    // personal accounts, not in/out of PX.
    pendingWithdrawal: null,
    capitalEvents: [
      {
        type: 'external_transfer',
        from: 'mike',
        to: 'rick',
        amount: 5000,
        at: SNAPSHOT_AT,
        note: 'Mike paid Rick $5K from a separate personal account. Trading-account total equity unchanged; Mike picks up $5K of Rick\'s trading-account ownership.',
      },
    ],
    supersedes: { version: 1, snapshotAt: '2026-05-15T16:09:55.423Z' },
    notes: 'Re-seed 2026-05-15: $5K external Mike→Rick transfer (corrects earlier $2K-PX-withdrawal misreading). Pre-cohort = currently-open parlays as of this re-seed.',
  };

  console.log('\nRevised snapshot:');
  console.log('  at:', snapshot.snapshotAt);
  console.log('  equity:', snapshot.snapshotEquity);
  console.log('  mike:', snapshot.snapshotMike, `(${(snapshot.postRatio.mike*100).toFixed(3)}%)`);
  console.log('  rick:', snapshot.snapshotRick, `(${(snapshot.postRatio.rick*100).toFixed(3)}%)`);
  console.log('  preCohort:', snapshot.preCohortIds.length, 'ids, stake $' + snapshot.preCohortStakeAtSnapshot.toFixed(2));

  const { error } = await sb.from('kv_store').upsert({
    key: 'bankroll_state',
    value: snapshot,
    updated_at: SNAPSHOT_AT,
  }, { onConflict: 'key' });
  if (error) { console.error('Upsert error:', error.message); process.exit(1); }
  console.log('\n✓ Saved to kv_store["bankroll_state"]');

  const { data: check } = await sb.from('kv_store').select('value').eq('key', 'bankroll_state').single();
  console.log('  verified version from DB:', check.value.version);
  console.log('  verified preCohortIds count:', check.value.preCohortIds.length);
})();
