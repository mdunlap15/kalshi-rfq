// Drill into "tighter-but-lost" auction losses — parlays where we
// offered a WORSE bettor payout than the winning SP (we underbid) and
// lost the auction on price.
//
// (Previously named "_looser_losses_drill.js" with inverted sign
// convention — fixed 2026-05-13 after Alec confirmed PX's ranking
// algorithm is odds-first → max_risk → timestamp. The cases this script
// filters on are gap < -0.01, which represent us OFFERING LESS PAYOUT
// than the winner. The "we beat the winner on price and still lost"
// case would be gap > 0.01 — which should be essentially zero per
// Alec's mechanic; if it appears that's worth its own investigation.)
//
// gap = our_SP_implied − winner_SP_implied (SP-side amToProb).
//   gap < 0 ⇒ we offered LESS payout (TIGHTER, underbid) — what we drill here
//   gap > 0 ⇒ we offered MORE payout (LOOSER) and lost anyway — rare
//   gap = 0 ⇒ exact tie, lost on max_risk / timestamp tiebreaker

require('dotenv').config();
const SUPA_URL = process.env.SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_SERVICE_KEY;

async function fetchAll(table, query) {
  const all = [];
  let offset = 0;
  while (true) {
    const url = SUPA_URL + '/rest/v1/' + table + '?' + query + '&limit=1000&offset=' + offset;
    const r = await fetch(url, { headers: { apikey: SUPA_KEY, Authorization: 'Bearer ' + SUPA_KEY } });
    if (!r.ok) break;
    const page = await r.json();
    all.push(...page);
    if (page.length < 1000) break;
    offset += 1000;
    if (offset > 200000) break;
  }
  return all;
}

function amToProb(am) {
  const n = Number(am);
  if (!Number.isFinite(n) || n === 0) return null;
  return n > 0 ? 100 / (n + 100) : -n / (-n + 100);
}

(async () => {
  const now = Date.now();
  const fromIso = new Date(now - 10 * 3600000).toISOString();

  console.log('=== LOOSER-BUT-LOST DRILL (last 10h) ===');
  console.log('Window: since', fromIso);
  console.log();

  // Pull all matched_parlays we reached auction on
  const matched = await fetchAll('matched_parlays',
    'select=parlay_id,outcome,our_odds,matched_odds,matched_stake,matched_at,legs'
    + '&matched_at=gte.' + encodeURIComponent(fromIso)
    + '&outcome=in.(other_sp,lost)'
  );
  // Dedupe
  const mpUniq = new Map();
  for (const m of matched) if (!mpUniq.has(m.parlay_id)) mpUniq.set(m.parlay_id, m);
  const mp = [...mpUniq.values()];

  // Filter to TIGHTER losses (we underbid winner — offered less payout).
  // Keeping variable name `tighterLosses` to match the corrected semantics.
  const tighterLosses = [];
  for (const m of mp) {
    if (m.our_odds == null || m.matched_odds == null) continue;
    const wi = amToProb(-Number(m.matched_odds));
    const oi = amToProb(-Number(m.our_odds));
    if (wi == null || oi == null) continue;
    const gap = (oi - wi) * 100; // negative = we underbid (offered less payout)
    if (gap < -0.01) {
      tighterLosses.push({ ...m, gapPp: gap });
    }
  }
  // Alias kept for the rest of the script body — same data, corrected name.
  const looser = tighterLosses;

  console.log('Total auction losses: ' + mp.length);
  console.log('Tighter-but-lost (we underbid winner): ' + looser.length + ' (' + (looser.length / mp.length * 100).toFixed(1) + '%)');
  console.log();

  if (looser.length === 0) { console.log('Nothing to drill.'); return; }

  // For each looser loss, pull the parlay_order to inspect valid_until,
  // offered_odds, confirmed flow, etc.
  console.log('=== Pulling parlay_order metadata for each tighter-loss ===');
  const orderUrl = SUPA_URL + '/rest/v1/parlay_orders'
    + '?select=parlay_id,quoted_at,status,offered_odds,confirmed_stake,fair_parlay_prob,meta'
    + '&parlay_id=in.(' + looser.map(l => l.parlay_id).join(',') + ')';
  const r = await fetch(orderUrl, { headers: { apikey: SUPA_KEY, Authorization: 'Bearer ' + SUPA_KEY } });
  const orders = await r.json();
  const orderByPid = new Map();
  for (const o of orders) orderByPid.set(o.parlay_id, o);

  // ----- 1. Time-since-quote distribution -----
  console.log();
  console.log('=== Time from quote → match (latency / lifetime indicator) ===');
  const latencies = [];
  for (const l of looser) {
    const o = orderByPid.get(l.parlay_id);
    if (!o || !o.quoted_at || !l.matched_at) continue;
    const ms = new Date(l.matched_at).getTime() - new Date(o.quoted_at).getTime();
    if (ms < 0 || ms > 600000) continue; // sanity cap at 10min
    latencies.push({ parlayId: l.parlay_id, ms, status: o.status, gap: l.gapPp });
  }
  latencies.sort((a, b) => a.ms - b.ms);
  if (latencies.length === 0) {
    console.log('No parlay_order matches found — possibly broadcast-only RFQs we never recorded.');
  } else {
    const pct = (arr, p) => arr.length ? arr[Math.min(arr.length - 1, Math.floor(arr.length * p / 100))].ms : null;
    console.log('  n=' + latencies.length);
    console.log('  p10:', pct(latencies, 10), 'ms');
    console.log('  p50:', pct(latencies, 50), 'ms');
    console.log('  p90:', pct(latencies, 90), 'ms');
    console.log('  p99:', pct(latencies, 99), 'ms');
    console.log('  Distribution:');
    const buckets = { '<1s': 0, '1-5s': 0, '5-30s': 0, '30-60s': 0, '60-120s': 0, '>120s': 0 };
    for (const l of latencies) {
      if (l.ms < 1000) buckets['<1s']++;
      else if (l.ms < 5000) buckets['1-5s']++;
      else if (l.ms < 30000) buckets['5-30s']++;
      else if (l.ms < 60000) buckets['30-60s']++;
      else if (l.ms < 120000) buckets['60-120s']++;
      else buckets['>120s']++;
    }
    for (const [k, v] of Object.entries(buckets)) {
      console.log('    ' + k.padEnd(10), v, '(' + (v / latencies.length * 100).toFixed(0) + '%)');
    }
  }

  // ----- 2. Order status of looser parlays -----
  console.log();
  console.log('=== Status of our parlay_order at time of analysis ===');
  const statusCounts = {};
  let weConfirmedToo = 0; // did we ALSO get a fill on this parlay?
  for (const l of looser) {
    const o = orderByPid.get(l.parlay_id);
    const st = o?.status || '(no_order_record)';
    statusCounts[st] = (statusCounts[st] || 0) + 1;
    if (st === 'confirmed' || (st && st.startsWith('settled_'))) weConfirmedToo++;
  }
  for (const [k, v] of Object.entries(statusCounts).sort((a, b) => b[1] - a[1])) {
    console.log('  ' + k.padEnd(25), v);
  }
  console.log();
  console.log('  We ALSO got a confirm on ' + weConfirmedToo + ' of ' + looser.length + ' tighter losses');
  console.log('  (Per Alec\'s mechanic, this is expected: we underbid the winner so PX matched their better price, not ours.)');

  // ----- 3. Sport / market / leg-count distribution -----
  console.log();
  console.log('=== Sport / market / leg-count distribution ===');
  const bySport = {}, byLegCount = {}, byMarketMix = {};
  for (const l of looser) {
    const legs = l.legs || [];
    const sports = [...new Set(legs.map(g => g.sport).filter(Boolean))];
    const primarySport = sports.length === 1 ? sports[0] : (sports.length > 1 ? 'MULTI' : 'unknown');
    bySport[primarySport] = (bySport[primarySport] || 0) + 1;
    byLegCount[legs.length] = (byLegCount[legs.length] || 0) + 1;
    const ms = [...new Set(legs.map(g => {
      const m = String(g.market || g.marketType || '').toLowerCase();
      if (m.startsWith('player_')) return 'prop';
      if (m.includes('moneyline')) return 'ml';
      if (m.includes('spread') || m.includes('run_line')) return 'spread';
      if (m.includes('total')) return 'total';
      return m;
    }).filter(Boolean))].sort();
    const cat = ms.length === 1 ? ms[0] : (ms.length > 1 ? 'mixed' : 'unknown');
    byMarketMix[cat] = (byMarketMix[cat] || 0) + 1;
  }
  console.log('  Sport:');
  for (const [k, v] of Object.entries(bySport).sort((a, b) => b[1] - a[1])) console.log('    ' + k.padEnd(20), v);
  console.log('  Leg count:');
  for (const [k, v] of Object.entries(byLegCount).sort((a, b) => Number(a[0]) - Number(b[0]))) console.log('    ' + k + ' legs:', v);
  console.log('  Market mix:');
  for (const [k, v] of Object.entries(byMarketMix).sort((a, b) => b[1] - a[1])) console.log('    ' + k.padEnd(20), v);

  // ----- 4. Gap distribution (HOW MUCH better did we offer?) -----
  console.log();
  console.log('=== Gap magnitude distribution (how much better were we?) ===');
  const gaps = looser.map(l => Math.abs(l.gapPp));
  gaps.sort((a, b) => a - b);
  const pctGap = (p) => gaps.length ? gaps[Math.min(gaps.length - 1, Math.floor(gaps.length * p / 100))] : null;
  console.log('  p25:', pctGap(25)?.toFixed(2), 'pp');
  console.log('  p50:', pctGap(50)?.toFixed(2), 'pp');
  console.log('  p75:', pctGap(75)?.toFixed(2), 'pp');
  console.log('  p95:', pctGap(95)?.toFixed(2), 'pp');
  console.log('  max:', gaps[gaps.length - 1]?.toFixed(2), 'pp');

  // ----- 5. Sample 10 most extreme cases -----
  console.log();
  console.log('=== Top 10 most extreme tighter losses (largest underbid) ===');
  const top = looser.slice().sort((a, b) => a.gapPp - b.gapPp).slice(0, 10);
  console.log('TIME              | OUR ODDS | WINNER  | GAP PP  | STAKE   | STATUS    | LEGS');
  console.log('------------------+----------+---------+---------+---------+-----------+---------');
  for (const l of top) {
    const o = orderByPid.get(l.parlay_id);
    const ourBet = -Number(l.our_odds);
    const winBet = -Number(l.matched_odds);
    const legs = (l.legs || []).slice(0, 2).map(g =>
      (g.sport || '?').replace(/^.*_/, '').slice(0, 4) + ':' + (g.team || g.selection || '?').slice(0, 12)
    ).join('/');
    console.log(
      (l.matched_at || '').slice(5, 16).replace('T', ' ').padEnd(17),
      '|', ((ourBet >= 0 ? '+' : '') + ourBet).padStart(8),
      '|', ((winBet >= 0 ? '+' : '') + winBet).padStart(7),
      '|', (l.gapPp.toFixed(2) + 'pp').padStart(7),
      '|', ('$' + Math.round(l.matched_stake || 0)).padStart(7),
      '|', (o?.status || '?').padEnd(9),
      '|', legs
    );
  }
})().catch(e => { console.error(e); process.exit(1); });
