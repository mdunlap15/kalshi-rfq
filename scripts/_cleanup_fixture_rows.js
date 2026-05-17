// Remove any parlay_orders rows with fixture-test parlayIds that
// leaked from scripts/_verify_sig_cooldown.js. The verify script
// wrote a fake confirmed order to test Layer 2 of the signature
// cooldown but didn't delete the row after.
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

(async () => {
  // First, list what we'd delete
  const { data: rows, error: e1 } = await sb.from('parlay_orders')
    .select('parlay_id, status, confirmed_at, confirmed_stake, legs')
    .like('parlay_id', 'fixture-saveorder-test-%');
  if (e1) { console.error('list error:', e1); process.exit(1); }
  console.log(`Found ${rows.length} fixture rows:`);
  for (const r of rows) {
    console.log(`  ${r.parlay_id}  status=${r.status}  confirmed_at=${r.confirmed_at}  stake=${r.confirmed_stake}`);
  }
  if (rows.length === 0) {
    console.log('Nothing to delete.');
    return;
  }
  // Delete
  const { error: e2 } = await sb.from('parlay_orders')
    .delete()
    .like('parlay_id', 'fixture-saveorder-test-%');
  if (e2) { console.error('delete error:', e2); process.exit(1); }
  console.log(`✓ Deleted ${rows.length} fixture rows.`);

  // Also check if any of these contaminated matched_parlays
  const fixtureIds = rows.map(r => r.parlay_id);
  const { data: matched } = await sb.from('matched_parlays')
    .select('parlay_id')
    .in('parlay_id', fixtureIds);
  if (matched && matched.length > 0) {
    console.log(`Also deleting ${matched.length} matched_parlays rows...`);
    await sb.from('matched_parlays').delete().in('parlay_id', fixtureIds);
  } else {
    console.log('No matched_parlays rows to delete.');
  }
})().catch(e => { console.error(e); process.exit(1); });
