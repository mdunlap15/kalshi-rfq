require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
(async () => {
  const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
  const start = '2026-05-15T04:00:00.000Z';
  const end   = '2026-05-16T04:00:00.000Z';

  console.log('=== exact, head:true ===');
  for (const tab of ['parlay_orders', 'declines', 'matched_parlays']) {
    const col = tab === 'parlay_orders' ? 'quoted_at'
              : tab === 'declines' ? 'declined_at' : 'matched_at';
    const t0 = Date.now();
    try {
      const r = await sb.from(tab).select('*', { count: 'exact', head: true })
        .gte(col, start).lt(col, end);
      console.log(`  ${tab} (${col}): ${r.count} rows in ${Date.now() - t0}ms (err: ${r.error ? JSON.stringify(r.error) : 'none'})`);
    } catch (e) {
      console.log(`  ${tab}: THREW after ${Date.now() - t0}ms: ${e.message}`);
    }
  }

  console.log('\n=== estimated ===');
  for (const tab of ['parlay_orders', 'declines']) {
    const col = tab === 'parlay_orders' ? 'quoted_at' : 'declined_at';
    const t0 = Date.now();
    try {
      const r = await sb.from(tab).select('*', { count: 'estimated', head: true })
        .gte(col, start).lt(col, end);
      console.log(`  ${tab} (${col}): ${r.count} rows in ${Date.now() - t0}ms`);
    } catch (e) {
      console.log(`  ${tab}: THREW: ${e.message}`);
    }
  }

  console.log('\n=== planned ===');
  for (const tab of ['parlay_orders', 'declines']) {
    const col = tab === 'parlay_orders' ? 'quoted_at' : 'declined_at';
    const t0 = Date.now();
    try {
      const r = await sb.from(tab).select('*', { count: 'planned', head: true })
        .gte(col, start).lt(col, end);
      console.log(`  ${tab} (${col}): ${r.count} rows in ${Date.now() - t0}ms`);
    } catch (e) {
      console.log(`  ${tab}: THREW: ${e.message}`);
    }
  }
})();
