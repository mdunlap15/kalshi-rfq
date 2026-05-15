// Test harness for the refactored /network-share-daily endpoint logic.
// Mirrors the head-count strategy in index.js exactly so we can validate it
// against the live Supabase data without booting the full express app.

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

async function main() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_KEY;
  if (!url || !key) { console.error('No Supabase creds in env'); process.exit(1); }
  const sb = createClient(url, key);

  const days = parseInt(process.argv[2]) || 14;
  const startedAt = Date.now();

  const TZ = 'America/New_York';
  const ET_FMT = new Intl.DateTimeFormat('en-CA', {
    timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit',
  });
  const ET_OFFSET_FMT = new Intl.DateTimeFormat('en-US', {
    timeZone: TZ, timeZoneName: 'shortOffset', year: 'numeric',
  });
  const isoToEt = (iso) => {
    if (!iso) return null;
    const d = new Date(iso);
    if (isNaN(d.getTime())) return null;
    return ET_FMT.format(d);
  };
  function etDayBoundsUtc(yyyymmdd) {
    const [y, mo, d] = yyyymmdd.split('-').map(Number);
    const noonUtc = new Date(Date.UTC(y, mo - 1, d, 16, 0, 0));
    const fmtOut = ET_OFFSET_FMT.format(noonUtc);
    const m = fmtOut.match(/GMT([+-]\d+)(?::(\d+))?/);
    const offsetHrs = m ? parseInt(m[1]) : -5;
    const startUtc = Date.UTC(y, mo - 1, d) - offsetHrs * 3600 * 1000;
    return [new Date(startUtc).toISOString(), new Date(startUtc + 86400000).toISOString()];
  }

  const todayEt = isoToEt(new Date().toISOString());
  const [tyr, tmo, tday] = todayEt.split('-').map(Number);
  const todayNoonMs = Date.UTC(tyr, tmo - 1, tday, 12, 0, 0);
  const dayList = [];
  for (let i = days - 1; i >= 0; i--) {
    const dayStr = ET_FMT.format(new Date(todayNoonMs - i * 86400000));
    const [s, e] = etDayBoundsUtc(dayStr);
    dayList.push({ date: dayStr, startIso: s, endIso: e });
  }

  console.log(`Window: ${dayList[0].startIso} → ${dayList[dayList.length-1].endIso} (${days} days)`);
  console.log(`First day: ${dayList[0].date}, Last day: ${dayList[dayList.length-1].date}`);

  async function headCount(table, col, s, e, mode, statusIn) {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        let q = sb.from(table).select('*', { count: mode, head: true }).gte(col, s).lt(col, e);
        if (statusIn) q = q.in('status', statusIn);
        const r = await q;
        if (!r.error) return r.count || 0;
        if (attempt === 0) { await new Promise(rs => setTimeout(rs, 200)); continue; }
        console.error(`  ${table}/${col} error:`, JSON.stringify(r.error));
        return null;
      } catch (err) {
        if (attempt === 0) { await new Promise(rs => setTimeout(rs, 200)); continue; }
        console.error(`  ${table}/${col} threw:`, err.message); return null;
      }
    }
    return null;
  }
  const countQuotes   = (s, e) => headCount('parlay_orders', 'quoted_at',    s, e, 'estimated');
  const countFills    = (s, e) => headCount('parlay_orders', 'confirmed_at', s, e, 'estimated', ['confirmed','settled_won','settled_lost','settled_push']);
  const countDeclines = (s, e) => headCount('declines',      'declined_at',  s, e, 'exact');

  // Run counts in chunks of 4 parallel queries (matches index.js endpoint)
  const CHUNK = 4;
  const dailyCounts = new Map();
  const t0 = Date.now();
  for (let i = 0; i < dayList.length; i += CHUNK) {
    const slice = dayList.slice(i, i + CHUNK);
    const results = await Promise.all(slice.flatMap(d => [
      countQuotes(d.startIso, d.endIso),
      countFills(d.startIso, d.endIso),
      countDeclines(d.startIso, d.endIso),
    ]));
    for (let j = 0; j < slice.length; j++) {
      dailyCounts.set(slice[j].date, {
        myQuotes:   results[j*3]     ?? 0,
        myFills:    results[j*3 + 1] ?? 0,
        weDeclined: results[j*3 + 2] ?? 0,
      });
    }
  }
  console.log(`  daily head-counts: ${Date.now() - t0}ms (${dayList.length * 3} queries)`);

  // Stake totals + our fills set (canonical "we won")
  const ourFills = new Map();
  const fillStakeByDay = new Map();
  const t1 = Date.now();
  {
    const ws = dayList[0].startIso, we = dayList[dayList.length-1].endIso;
    let offset = 0;
    while (true) {
      const { data, error } = await sb.from('parlay_orders')
        .select('parlay_id, confirmed_at, confirmed_stake')
        .gte('confirmed_at', ws).lt('confirmed_at', we)
        .in('status', ['confirmed','settled_won','settled_lost','settled_push'])
        .order('confirmed_at', { ascending: true })
        .range(offset, offset + 999);
      if (error) { console.error('fill stake error:', error.message); break; }
      if (!data || data.length === 0) break;
      for (const r of data) {
        const d = isoToEt(r.confirmed_at);
        if (!d) continue;
        ourFills.set(r.parlay_id, { confirmed_at: r.confirmed_at, confirmed_stake: r.confirmed_stake });
        fillStakeByDay.set(d, (fillStakeByDay.get(d) || 0) + (Number(r.confirmed_stake) || 0));
      }
      if (data.length < 1000) break;
      offset += 1000;
    }
  }
  console.log(`  fill stake pull: ${Date.now() - t1}ms (${ourFills.size} wins)`);

  // matched_parlays FIRST so we have the parlay_id set to cross-ref.
  const matchedSeen = new Map();
  const t3 = Date.now();
  {
    const ws = dayList[0].startIso, we = dayList[dayList.length-1].endIso;
    let offset = 0;
    while (true) {
      const { data, error } = await sb.from('matched_parlays')
        .select('parlay_id, matched_at, we_quoted, our_odds')
        .gte('matched_at', ws).lt('matched_at', we)
        .order('matched_at', { ascending: true })
        .range(offset, offset + 999);
      if (error) { console.error('matched_parlays error:', error.message); break; }
      if (!data || data.length === 0) break;
      for (const r of data) {
        const prev = matchedSeen.get(r.parlay_id);
        if (!prev) matchedSeen.set(r.parlay_id, { matched_at: r.matched_at, we_quoted: !!r.we_quoted, our_odds: r.our_odds });
        else {
          if (r.matched_at && r.matched_at < prev.matched_at) prev.matched_at = r.matched_at;
          if (r.we_quoted) prev.we_quoted = true;
          if (prev.our_odds == null && r.our_odds != null) prev.our_odds = r.our_odds;
        }
      }
      if (data.length < 1000) break;
      offset += 1000;
    }
  }
  console.log(`  matched_parlays pull: ${Date.now() - t3}ms (${matchedSeen.size} unique)`);

  // Cross-reference via .in('parlay_id', [chunk]) — uses PK index, fast.
  // Parallel batches of 4 to keep the request pipeline warm.
  const quotedParlayIds = new Set();
  const t2 = Date.now();
  {
    const ids = Array.from(matchedSeen.keys());
    const BATCH = 200;
    const PARALLEL = 4;
    const tasks = [];
    for (let i = 0; i < ids.length; i += BATCH) tasks.push(ids.slice(i, i + BATCH));
    for (let i = 0; i < tasks.length; i += PARALLEL) {
      const wave = tasks.slice(i, i + PARALLEL);
      const results = await Promise.all(wave.map(chunk =>
        sb.from('parlay_orders').select('parlay_id').in('parlay_id', chunk)
          .then(r => r, e => ({ error: e }))
      ));
      for (const r of results) {
        if (r.error) { console.error(`  cross-ref err:`, r.error.message); continue; }
        for (const row of (r.data || [])) quotedParlayIds.add(row.parlay_id);
      }
    }
  }
  console.log(`  parlay_id cross-ref (in-chunks): ${Date.now() - t2}ms (${quotedParlayIds.size} ids)`);

  // Union our fills into matched-parlay universe (PX broadcasts are unreliable
  // for our own wins — synthesize entries bucketed by confirmed_at).
  let synthesized = 0;
  for (const [pid, fill] of ourFills.entries()) {
    if (matchedSeen.has(pid)) continue;
    matchedSeen.set(pid, { matched_at: fill.confirmed_at, we_quoted: true, our_odds: null, _synth: true });
    synthesized++;
  }
  console.log(`  synthesized ${synthesized} entries from fills not in matched_parlays`);

  const matchedByDay = new Map();
  for (const [pid, r] of matchedSeen.entries()) {
    const d = isoToEt(r.matched_at);
    if (!d) continue;
    let bucket = matchedByDay.get(d);
    if (!bucket) { bucket = { networkMatched: 0, networkWeBidOn: 0 }; matchedByDay.set(d, bucket); }
    bucket.networkMatched++;
    const weBid = r.we_quoted || r.our_odds != null || quotedParlayIds.has(pid) || ourFills.has(pid);
    if (weBid) bucket.networkWeBidOn++;
  }

  const byDayArr = dayList.map(d => {
    const counts  = dailyCounts.get(d.date) || { myQuotes:0, myFills:0, weDeclined:0 };
    const matched = matchedByDay.get(d.date) || { networkMatched:0, networkWeBidOn:0 };
    const sharePct = matched.networkMatched > 0 ? counts.myFills / matched.networkMatched : null;
    const bidWin   = matched.networkWeBidOn > 0 ? Math.min(1, counts.myFills / matched.networkWeBidOn) : null;
    return {
      date: d.date,
      myQuotes: counts.myQuotes, myFills: counts.myFills, weDeclined: counts.weDeclined,
      networkDemand: counts.myQuotes + counts.weDeclined,
      networkMatched: matched.networkMatched, networkWeBidOn: matched.networkWeBidOn,
      shareOfMatched: sharePct, bidWinRate: bidWin,
    };
  });

  console.log('\nPer-day:');
  for (const d of byDayArr) {
    const share = d.shareOfMatched != null ? (d.shareOfMatched * 100).toFixed(2) + '%' : '—';
    const bw = d.bidWinRate != null ? (d.bidWinRate * 100).toFixed(1) + '%' : '—';
    console.log(`  ${d.date}: quotes=${String(d.myQuotes).padStart(5)} fills=${String(d.myFills).padStart(4)} decl=${String(d.weDeclined).padStart(5)} | netMatched=${String(d.networkMatched).padStart(5)} netBidOn=${String(d.networkWeBidOn).padStart(5)} | share=${share.padStart(7)} bidWin=${bw.padStart(7)}`);
  }

  const totals = byDayArr.reduce((acc, d) => {
    acc.myQuotes += d.myQuotes; acc.myFills += d.myFills; acc.weDeclined += d.weDeclined;
    acc.networkDemand += d.networkDemand;
    acc.networkMatched += d.networkMatched; acc.networkWeBidOn += d.networkWeBidOn;
    return acc;
  }, { myQuotes:0, myFills:0, weDeclined:0, networkDemand:0, networkMatched:0, networkWeBidOn:0 });
  totals.shareOfMatched = totals.networkMatched > 0 ? totals.myFills / totals.networkMatched : null;
  totals.bidWinRate = totals.networkWeBidOn > 0 ? Math.min(1, totals.myFills / totals.networkWeBidOn) : null;

  console.log('\nTotals:');
  console.log(`  myQuotes:       ${totals.myQuotes.toLocaleString()}`);
  console.log(`  myFills:        ${totals.myFills.toLocaleString()}`);
  console.log(`  weDeclined:     ${totals.weDeclined.toLocaleString()}`);
  console.log(`  networkDemand:  ${totals.networkDemand.toLocaleString()}`);
  console.log(`  networkMatched: ${totals.networkMatched.toLocaleString()}`);
  console.log(`  networkWeBidOn: ${totals.networkWeBidOn.toLocaleString()}`);
  console.log(`  shareOfMatched: ${totals.shareOfMatched != null ? (totals.shareOfMatched * 100).toFixed(2) + '%' : '—'}`);
  console.log(`  bidWinRate:     ${totals.bidWinRate != null ? (totals.bidWinRate * 100).toFixed(1) + '%' : '—'}`);
  console.log(`\nTOTAL ELAPSED: ${Date.now() - startedAt}ms`);
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
