// One-shot: write the initial bankroll snapshot to kv_store.
//
// Triggered on 2026-05-15 when Rick withdrew $2K from the partnership
// account. From this point forward, the bankroll accounting uses a
// dual-cohort model:
//   - Parlays open at snapshotAt   → "pre" cohort   → P&L splits 7/12 ÷ 5/12
//   - Parlays placed after snapshotAt → "post" cohort → P&L splits
//                                                       60.557% ÷ 39.443%
//
// Re-running this script overwrites the snapshot, which is generally NOT
// what you want once accounting is live. Intended as a one-shot seed.

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const SNAPSHOT_AT = new Date().toISOString();

// Pre-withdrawal state (from live PX): Mike $31,768.54 / Rick $22,691.82
// Post Rick's $2K withdrawal: Mike unchanged, Rick = $20,691.82
const MIKE_POST = 31768.54;
const RICK_POST = 20691.82;
const SNAPSHOT_EQUITY = MIKE_POST + RICK_POST;  // $52,460.36

const PRE_RATIO  = { mike: 7 / 12,                rick: 5 / 12 };
const POST_RATIO = { mike: MIKE_POST / SNAPSHOT_EQUITY,
                     rick: RICK_POST / SNAPSHOT_EQUITY };

(async () => {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_KEY;
  if (!url || !key) { console.error('Missing Supabase creds'); process.exit(1); }
  const sb = createClient(url, key);

  // Pull the live open-position set from PX via the production /px-positions
  // endpoint. We don't trust Supabase's status='confirmed' alone because
  // it includes ~1,300 stale rows (phantoms PX has long forgotten); PX's
  // own view of open positions is the source of truth.
  const auth = 'Basic ' + Buffer.from(`${process.env.AUTH_USERNAME}:${process.env.AUTH_PASSWORD}`).toString('base64');
  const PROD = process.env.PROD_URL || 'https://prophetx-rfq-production-6781.up.railway.app';
  console.log('Fetching /px-positions from', PROD, '...');
  const resp = await fetch(PROD + '/px-positions', { headers: { Authorization: auth } });
  if (!resp.ok) { console.error('px-positions HTTP', resp.status); process.exit(1); }
  const body = await resp.json();
  if (!body.ok || !Array.isArray(body.positions)) { console.error('Bad response:', JSON.stringify(body).slice(0,200)); process.exit(1); }
  const preCohortIds = body.positions.map(p => p.parlayId).filter(Boolean);
  console.log(`Captured ${preCohortIds.length} open positions, total stake $${body.totalStake.toFixed(2)}`);

  const snapshot = {
    version: 1,
    snapshotAt: SNAPSHOT_AT,
    snapshotEquity: SNAPSHOT_EQUITY,
    snapshotMike: MIKE_POST,
    snapshotRick: RICK_POST,
    preCohortIds,
    preCohortStakeAtSnapshot: body.totalStake,
    preRatio: PRE_RATIO,
    postRatio: POST_RATIO,
    // Pending Rick withdrawal. Until cash physically moves out of PX, live
    // TE will sit $2,000 above the accounting figure. The /bankroll endpoint
    // surfaces this as drift so the operator can see the un-executed leg.
    pendingWithdrawal: {
      partner: 'rick',
      amount: 2000,
      noticedAt: SNAPSHOT_AT,
    },
    // Forward log of recorded capital events. Each entry: { partner, type,
    // amount, at }. Withdrawals/deposits adjust the partner's running equity
    // when the /bankroll endpoint detects the cash movement (or we add an
    // explicit /admin/bankroll-record endpoint to confirm).
    capitalEvents: [],
    notes: 'Initial seed: 2026-05-15 Rick $2K withdrawal. Pre-cohort = open at snapshot.',
  };

  console.log('\nSnapshot:');
  console.log('  at:', snapshot.snapshotAt);
  console.log('  equity:', snapshot.snapshotEquity);
  console.log('  mike:', snapshot.snapshotMike, `(${(snapshot.postRatio.mike*100).toFixed(3)}%)`);
  console.log('  rick:', snapshot.snapshotRick, `(${(snapshot.postRatio.rick*100).toFixed(3)}%)`);
  console.log('  preCohortIds count:', snapshot.preCohortIds.length);
  console.log('  preCohortStake:', snapshot.preCohortStakeAtSnapshot);
  console.log('  pendingWithdrawal:', snapshot.pendingWithdrawal);

  // Upsert into kv_store
  const { error } = await sb.from('kv_store').upsert({
    key: 'bankroll_state',
    value: snapshot,
    updated_at: SNAPSHOT_AT,
  }, { onConflict: 'key' });

  if (error) { console.error('Upsert error:', error.message); process.exit(1); }
  console.log('\n✓ Saved to kv_store["bankroll_state"]');

  // Verify by reading back
  const { data: check, error: readErr } = await sb.from('kv_store').select('value, updated_at').eq('key', 'bankroll_state').single();
  if (readErr) { console.error('Verify read error:', readErr.message); process.exit(1); }
  console.log('  verified preCohortIds count from DB:', check.value.preCohortIds.length);
})();
