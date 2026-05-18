// Rev 5: Mike's second personal $5K distribution to Rick.
//
// 2026-05-18T03:59:00Z (23:59 ET on 2026-05-17): Mike paid Rick another
// $5,000 from a personal account (cumulative $7,500 since rev 4's $2,500).
// PX cash untouched; ownership shifts inside the partnership account.
// Mike's stake in PX goes UP by $5K, Rick's goes DOWN by $5K.
//
// Pre-cohort (parlays open at rev-5 cutoff that were quoted under rev-4's
// regime) = the 14 parlays with confirmed_at between rev-4 and rev-5
// cutoffs, still status='confirmed' as of the rev-5 cutoff. All 64 from
// rev-4's pre-cohort have settled (verified), so no 3-tier ratio needed.
//
// Baseline (snapshotMike/Rick): pulled from the live rev-4 books — i.e.
// snapshotMike(rev4) + preRatio.mike × prePnl + postRatio.mike × postPnl
// across all settled parlays since 2026-05-15T16:09:55Z. This respects
// the path-dependent rev-4 attribution (64 pre-cohort split at 7/12, rest
// at 0.62924) more accurately than naively applying E × 0.62924.
//
// E5 (partnership equity) = $69,800.59 — confirmed by Mike. Live PX TE is
// ~$178K but ~$108K of that is unaccounted drift (likely ghost-settlement
// sync gap from ~1,000 stale 'confirmed' April rows). The drift sits
// outside partnership equity and is a separate reconciliation concern.

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const SNAPSHOT_AT = '2026-05-18T03:59:00.000Z';   // 23:59 ET on 2026-05-17
const REV4_AT     = '2026-05-15T16:09:55.423Z';
const TRANSFER    = 5000;                          // Mike -> Rick personal

(async () => {
  const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

  // 1. Load rev-4 snapshot
  const { data: kv } = await sb.from('kv_store').select('value').eq('key', 'bankroll_state').single();
  const rev4 = kv.value;
  if (rev4.version !== 4) { console.error('Expected rev-4, got rev-' + rev4.version); process.exit(1); }
  console.log('Rev-4 baseline:');
  console.log('  snapshotMike:', rev4.snapshotMike);
  console.log('  snapshotRick:', rev4.snapshotRick);
  console.log('  preRatio :', rev4.preRatio);
  console.log('  postRatio:', rev4.postRatio);

  // 2. Replay rev-4 books to the rev-5 cutoff (sum settled P&L between rev-4 and rev-5, split by rev-4 cohort).
  const rev4PreIds = new Set(rev4.preCohortIds);
  const settled = [];
  {
    let offset = 0;
    while (true) {
      const { data, error } = await sb.from('parlay_orders')
        .select('parlay_id, pnl, status, settled_at')
        .gte('settled_at', REV4_AT)
        .lte('settled_at', SNAPSHOT_AT)
        .in('status', ['settled_won','settled_lost','settled_push'])
        .order('settled_at', { ascending: true })
        .range(offset, offset + 999);
      if (error) { console.error('settled fetch err:', error.message); process.exit(1); }
      if (!data || !data.length) break;
      settled.push(...data);
      if (data.length < 1000) break;
      offset += 1000;
    }
  }
  let prePnl = 0, postPnl = 0, preN = 0, postN = 0;
  for (const r of settled) {
    const p = Number(r.pnl) || 0;
    if (rev4PreIds.has(r.parlay_id)) { prePnl += p; preN++; } else { postPnl += p; postN++; }
  }
  const mikeAtCutoff = rev4.snapshotMike + rev4.preRatio.mike * prePnl + rev4.postRatio.mike * postPnl;
  const rickAtCutoff = rev4.snapshotRick + rev4.preRatio.rick * prePnl + rev4.postRatio.rick * postPnl;
  console.log('\nReplay between rev-4 and rev-5 cutoffs:');
  console.log('  settled parlays:', settled.length, '(rev-4-pre:', preN, ' rev-4-post:', postN, ')');
  console.log('  prePnl: $' + prePnl.toFixed(2));
  console.log('  postPnl: $' + postPnl.toFixed(2));
  console.log('  Mike at cutoff (pre-$5K-transfer):', mikeAtCutoff.toFixed(2));
  console.log('  Rick at cutoff (pre-$5K-transfer):', rickAtCutoff.toFixed(2));

  // 3. Apply the $5K Mike→Rick transfer (Mike's share UP, Rick's DOWN).
  const MIKE_POST = mikeAtCutoff + TRANSFER;
  const RICK_POST = rickAtCutoff - TRANSFER;
  const SNAPSHOT_EQUITY = MIKE_POST + RICK_POST;

  console.log('\nAfter $5K transfer:');
  console.log('  Mike post:', MIKE_POST.toFixed(2));
  console.log('  Rick post:', RICK_POST.toFixed(2));
  console.log('  Total:    ', SNAPSHOT_EQUITY.toFixed(2));

  // 4. Freeze rev-5 pre-cohort: parlays with confirmed_at <= cutoff that are
  //    still open at the cutoff. Restrict to confirmed_at >= rev-4 cutoff so
  //    we exclude the ~1,000 stale April 'confirmed' ghosts.
  const { data: between } = await sb.from('parlay_orders')
    .select('parlay_id, confirmed_at, status, settled_at, confirmed_stake')
    .gte('confirmed_at', REV4_AT)
    .lte('confirmed_at', SNAPSHOT_AT);
  const openAtCutoff = (between||[]).filter(r => r.status === 'confirmed' || (r.settled_at && r.settled_at > SNAPSHOT_AT));
  const preCohortIds = openAtCutoff.map(r => r.parlay_id);
  const preCohortStake = openAtCutoff.reduce((s,r) => s + (Number(r.confirmed_stake)||0), 0);
  console.log('\nRev-5 pre-cohort:', preCohortIds.length, 'parlays, stake $' + preCohortStake.toFixed(2));

  // 5. Build rev-5 snapshot. preRatio carries forward rev-4's postRatio (the
  // 14 pre-cohort parlays were quoted under rev-4's regime). postRatio is
  // derived from the post-transfer Mike/Rick split.
  const PRE_RATIO  = { mike: rev4.postRatio.mike, rick: rev4.postRatio.rick };
  const POST_RATIO = {
    mike: MIKE_POST / SNAPSHOT_EQUITY,
    rick: RICK_POST / SNAPSHOT_EQUITY,
  };

  const snapshot = {
    version: 5,
    snapshotAt: SNAPSHOT_AT,
    snapshotEquity: Math.round(SNAPSHOT_EQUITY * 100) / 100,
    snapshotMike:   Math.round(MIKE_POST       * 100) / 100,
    snapshotRick:   Math.round(RICK_POST       * 100) / 100,
    preCohortIds,
    preCohortStakeAtSnapshot: Math.round(preCohortStake * 100) / 100,
    preRatio:  PRE_RATIO,
    postRatio: POST_RATIO,
    pendingWithdrawal: null,
    capitalEvents: [
      ...(rev4.capitalEvents || []),
      {
        type: 'external_transfer',
        from: 'mike',
        to: 'rick',
        amount: TRANSFER,
        at: SNAPSHOT_AT,
        note: 'Mike paid Rick $5,000 from a personal account. PX untouched; ownership shifts in-account. Cumulative Mike→Rick personal = $7,500.',
      },
    ],
    supersedes: [
      ...(rev4.supersedes || []),
      { version: 4, note: 'rev-4 $2,500 transfer; superseded by rev-5 $5,000 retro to 23:59 ET 5/17' },
    ],
    notes: 'Rev 5: Mike\'s second $5K personal distribution to Rick, retroactive to 23:59 ET on 2026-05-17. Baseline (snapshotMike/Rick) computed by replaying rev-4 books to the cutoff (path-dependent split of 64 rev-4 pre-cohort + 286 rev-4 post-cohort settlements). E5 = $69,800.59 partnership equity; ~$108K live-PX drift treated as out-of-partnership ghost-settlement sync gap (separate concern).',
  };

  console.log('\nRev-5 snapshot:');
  console.log(JSON.stringify(snapshot, null, 2));

  if (process.argv.includes('--dry-run')) {
    console.log('\n[dry-run] not writing to kv_store');
    return;
  }

  const { error } = await sb.from('kv_store').upsert({
    key: 'bankroll_state',
    value: snapshot,
    updated_at: new Date().toISOString(),
  }, { onConflict: 'key' });
  if (error) { console.error('Upsert error:', error.message); process.exit(1); }
  console.log('\n✓ Saved rev-5 to kv_store["bankroll_state"]');

  const { data: check } = await sb.from('kv_store').select('value').eq('key', 'bankroll_state').single();
  console.log('  verified version:', check.value.version);
  console.log('  verified snapshotAt:', check.value.snapshotAt);
  console.log('  verified preCohortIds count:', check.value.preCohortIds.length);
})();
