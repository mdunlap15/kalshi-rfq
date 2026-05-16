// Smoke-test the hourly endpoint logic against live Supabase.
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

(async () => {
  const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
  const hours = 24;
  const rolling = 6;
  const now = Date.now();
  const endMs = Math.floor(now / 3_600_000) * 3_600_000;
  const startMs = endMs - hours * 3_600_000;
  const startIso = new Date(startMs).toISOString();
  const endIso = new Date(endMs).toISOString();
  console.log(`Window: ${startIso} → ${endIso}  (${hours}h)`);

  const buckets = [];
  for (let i = 0; i < hours; i++) {
    const ms = startMs + i * 3_600_000;
    buckets.push({ hourMs: ms, myQuotes: 0, myFills: 0, weDeclined: 0, networkMatched: 0, networkWeBidOn: 0 });
  }
  const bucketForMs = (ms) => {
    if (ms < startMs || ms >= endMs) return null;
    return buckets[Math.floor((ms - startMs) / 3_600_000)];
  };

  // parlay_orders
  const ourIds = new Set();
  const t1 = Date.now();
  {
    let offset = 0;
    while (true) {
      const { data, error } = await sb.from('parlay_orders')
        .select('parlay_id, status, quoted_at, confirmed_at, meta')
        .or(`and(quoted_at.gte.${startIso},quoted_at.lt.${endIso}),and(confirmed_at.gte.${startIso},confirmed_at.lt.${endIso})`)
        .order('quoted_at', { ascending: true })
        .range(offset, offset + 999);
      if (error) { console.error('po err:', error.message); break; }
      if (!data || !data.length) break;
      for (const r of data) {
        if (r.meta?.phantom) continue;
        if (r.meta?.reconstructed && !r.quoted_at) continue;
        ourIds.add(r.parlay_id);
        if (r.quoted_at) {
          const b = bucketForMs(new Date(r.quoted_at).getTime());
          if (b) b.myQuotes++;
        }
        const isWon = r.status === 'confirmed' || (typeof r.status === 'string' && r.status.startsWith('settled_'));
        if (isWon && r.confirmed_at) {
          const b = bucketForMs(new Date(r.confirmed_at).getTime());
          if (b) b.myFills++;
        }
      }
      if (data.length < 1000) break;
      offset += 1000;
    }
  }
  console.log(`  parlay_orders: ${Date.now() - t1}ms`);

  // declines
  const t2 = Date.now();
  {
    let offset = 0;
    while (true) {
      const { data, error } = await sb.from('declines')
        .select('declined_at')
        .gte('declined_at', startIso).lt('declined_at', endIso)
        .order('declined_at', { ascending: true })
        .range(offset, offset + 999);
      if (error) { console.error('dec err:', error.message); break; }
      if (!data || !data.length) break;
      for (const r of data) {
        const b = bucketForMs(new Date(r.declined_at).getTime());
        if (b) b.weDeclined++;
      }
      if (data.length < 1000) break;
      offset += 1000;
    }
  }
  console.log(`  declines: ${Date.now() - t2}ms`);

  // matched_parlays
  const seen = new Map();
  const t3 = Date.now();
  {
    let offset = 0;
    while (true) {
      const { data, error } = await sb.from('matched_parlays')
        .select('parlay_id, matched_at, we_quoted, our_odds')
        .gte('matched_at', startIso).lt('matched_at', endIso)
        .order('matched_at', { ascending: true })
        .range(offset, offset + 999);
      if (error) { console.error('mp err:', error.message); break; }
      if (!data || !data.length) break;
      for (const r of data) {
        const p = seen.get(r.parlay_id);
        if (!p) seen.set(r.parlay_id, { matched_at: r.matched_at, we_quoted: !!r.we_quoted, our_odds: r.our_odds });
        else { if (r.we_quoted) p.we_quoted = true; if (p.our_odds == null && r.our_odds != null) p.our_odds = r.our_odds; }
      }
      if (data.length < 1000) break;
      offset += 1000;
    }
    for (const [pid, r] of seen.entries()) {
      const b = bucketForMs(new Date(r.matched_at).getTime());
      if (!b) continue;
      b.networkMatched++;
      if (r.we_quoted || r.our_odds != null || ourIds.has(pid)) b.networkWeBidOn++;
    }
  }
  console.log(`  matched_parlays: ${Date.now() - t3}ms`);

  // Per-hour print
  const fmtET = (ms) => new Intl.DateTimeFormat('en-US', { timeZone:'America/New_York', hour:'2-digit', month:'numeric', day:'numeric', hour12: false }).format(new Date(ms));
  console.log('\\nPer-hour (last 24h ET):');
  console.log('hour ET        | quotes | fills | decl   | netMatch | netBidOn | share%  | bidWin%');
  for (const b of buckets) {
    const share = b.networkMatched > 0 ? (b.myFills/b.networkMatched*100).toFixed(2) + '%' : '—';
    const bw = b.networkWeBidOn > 0 ? (b.myFills/b.networkWeBidOn*100).toFixed(2) + '%' : '—';
    console.log(`  ${fmtET(b.hourMs).padEnd(13)} | ${String(b.myQuotes).padStart(6)} | ${String(b.myFills).padStart(5)} | ${String(b.weDeclined).padStart(6)} | ${String(b.networkMatched).padStart(8)} | ${String(b.networkWeBidOn).padStart(8)} | ${share.padStart(7)} | ${bw.padStart(7)}`);
  }
  const tot = buckets.reduce((a,b) => { a.q+=b.myQuotes; a.f+=b.myFills; a.d+=b.weDeclined; a.m+=b.networkMatched; a.bid+=b.networkWeBidOn; return a; }, { q:0, f:0, d:0, m:0, bid:0 });
  console.log(`\\nTotals: quotes ${tot.q} · fills ${tot.f} · declines ${tot.d} · netMatched ${tot.m} · bidOn ${tot.bid} · share ${(tot.f/tot.m*100).toFixed(2)}% · bidWin ${(tot.f/tot.bid*100).toFixed(2)}%`);
})();
