// Re-seed the bankroll snapshot for the corrected Mike→Rick transfer.
//
// 2026-05-15 (revised twice):
//   Rev 1 (16:09:55Z): captured 64 open parlays as pre-cohort when Mike
//     first mentioned a Rick withdrawal. Original snapshotMike/Rick
//     assumed a $2K PX withdrawal.
//   Rev 2: corrected the cash path — $5K external transfer from Mike's
//     personal account, not a PX withdrawal. Re-pulled positions and
//     captured 65 (one new parlay had landed between seeds).
//   Rev 3 (THIS FILE): restores the redistribution point to the ORIGINAL
//     timestamp + 64 preCohortIds — per Mike's clarification that "RIGHT
//     NOW should be what it was when I first mentioned that above". The
//     1 parlay that landed between snapshots therefore becomes part of
//     the POST-cohort (settles under the new 67.514% / 32.486% ratios).
//
// Pre-cohort source: positions.json — the /px-positions response captured
// at original snapshot time (16:09:55Z, 64 parlays, $14,473.65 stake).

require('dotenv').config();
const fs = require('fs');
const { createClient } = require('@supabase/supabase-js');

// Pin the redistribution point to the original snapshot time — when Mike
// first raised the withdrawal question. This is the moment the partnership
// agreement effectively changed.
const SNAPSHOT_AT = '2026-05-15T16:09:55.423Z';

// Corrected post-transfer state for $5K external Mike→Rick:
//   Trading-account TE unchanged: $54,460.36
//   Mike: $31,768.54 + $5,000 = $36,768.54  (he "bought" Rick's $5K stake)
//   Rick: $22,691.82 - $5,000 = $17,691.82
const SNAPSHOT_EQUITY = 54460.36;
const MIKE_POST       = 36768.54;
const RICK_POST       = 17691.82;

const PRE_RATIO  = { mike: 7 / 12, rick: 5 / 12 };
const POST_RATIO = {
  mike: MIKE_POST / SNAPSHOT_EQUITY,   // ≈ 0.67514
  rick: RICK_POST / SNAPSHOT_EQUITY,   // ≈ 0.32486
};

(async () => {
  const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

  // Load the original 64-parlay open set captured at 16:09:55Z. This
  // positions.json was saved at the original snapshot moment — it's the
  // authoritative pre-cohort for Mike's intent.
  const positionsRaw = JSON.parse(fs.readFileSync('positions.json', 'utf8'));
  const preCohortIds = positionsRaw.positions.map(p => p.parlayId).filter(Boolean);
  const preCohortStake = positionsRaw.totalStake;
  console.log(`Pre-cohort restored from positions.json:`);
  console.log(`  ${preCohortIds.length} parlays, total stake $${preCohortStake.toFixed(2)}`);

  // Verify live TE is still ~$54,460.36 (sanity check that the assumed
  // snapshot equity is still correct from the partnership's perspective —
  // realized P&L since the original snapshot should already be captured in
  // /bankroll's "post-cohort realized" if any parlays settled).
  const auth = 'Basic ' + Buffer.from(`${process.env.AUTH_USERNAME}:${process.env.AUTH_PASSWORD}`).toString('base64');
  const PROD = process.env.PROD_URL || 'https://prophetx-rfq-production-6781.up.railway.app';
  const [bal, pnl] = await Promise.all([
    fetch(PROD + '/balance', { headers: { Authorization: auth } }).then(r => r.json()),
    fetch(PROD + '/px-pnl',  { headers: { Authorization: auth } }).then(r => r.json()),
  ]);
  const liveTE = (bal?.balance?.balance || 0) + (pnl?.openExposure || 0);
  console.log(`Live TE now: $${liveTE.toFixed(2)}  (snapshot anchor: $${SNAPSHOT_EQUITY.toFixed(2)})`);

  const snapshot = {
    version: 3,
    snapshotAt: SNAPSHOT_AT,
    snapshotEquity: SNAPSHOT_EQUITY,
    snapshotMike: MIKE_POST,
    snapshotRick: RICK_POST,
    preCohortIds,
    preCohortStakeAtSnapshot: preCohortStake,
    preRatio: PRE_RATIO,
    postRatio: POST_RATIO,
    pendingWithdrawal: null,
    capitalEvents: [
      {
        type: 'external_transfer',
        from: 'mike',
        to: 'rick',
        amount: 5000,
        at: SNAPSHOT_AT,
        note: 'Mike paid Rick $5K from a separate personal account. PX untouched; ownership shifts in-account.',
      },
    ],
    supersedes: [
      { version: 1, snapshotAt: '2026-05-15T16:09:55.423Z', note: 'misread as $2K PX withdrawal' },
      { version: 2, snapshotAt: '2026-05-15T16:31:31.422Z', note: '$5K external, but pre-cohort had drifted to 65' },
    ],
    notes: 'Rev 3: redistribution point pinned to when Mike first raised the question (16:09:55Z). Pre-cohort = the 64 parlays open at that moment.',
  };

  console.log('\nRev 3 snapshot:');
  console.log('  at:', snapshot.snapshotAt);
  console.log('  equity:', snapshot.snapshotEquity);
  console.log('  mike:', snapshot.snapshotMike, `(${(snapshot.postRatio.mike*100).toFixed(3)}%)`);
  console.log('  rick:', snapshot.snapshotRick, `(${(snapshot.postRatio.rick*100).toFixed(3)}%)`);
  console.log('  preCohort:', snapshot.preCohortIds.length, 'ids, stake $' + snapshot.preCohortStakeAtSnapshot.toFixed(2));

  const { error } = await sb.from('kv_store').upsert({
    key: 'bankroll_state',
    value: snapshot,
    updated_at: new Date().toISOString(),
  }, { onConflict: 'key' });
  if (error) { console.error('Upsert error:', error.message); process.exit(1); }
  console.log('\n✓ Saved to kv_store["bankroll_state"]');

  const { data: check } = await sb.from('kv_store').select('value').eq('key', 'bankroll_state').single();
  console.log('  verified version:', check.value.version);
  console.log('  verified snapshotAt:', check.value.snapshotAt);
  console.log('  verified preCohortIds count:', check.value.preCohortIds.length);
})();
