require('dotenv').config();
const SUPA_URL = process.env.SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_SERVICE_KEY;

async function fetchAll(table, query) {
  const all = [];
  let offset = 0;
  while (true) {
    const url = SUPA_URL + '/rest/v1/' + table + '?' + query + '&limit=1000&offset=' + offset;
    const r = await fetch(url, { headers: { apikey: SUPA_KEY, Authorization: 'Bearer ' + SUPA_KEY } });
    if (!r.ok) { console.error('fetch failed', r.status); break; }
    const page = await r.json();
    all.push(...page);
    if (page.length < 1000) break;
    offset += 1000;
    if (offset > 100000) break;
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
  // Last 90 minutes
  const fromIso = new Date(now - 90 * 60 * 1000).toISOString();
  console.log('Window: last 90 min since', fromIso);

  const orders = await fetchAll('parlay_orders',
    'select=parlay_id,quoted_at,confirmed_at,status,offered_odds,confirmed_stake,fair_parlay_prob,meta'
    + '&quoted_at=gte.' + encodeURIComponent(fromIso)
    + '&order=quoted_at.desc'
  );

  const byStatus = {};
  for (const o of orders) byStatus[o.status] = (byStatus[o.status] || 0) + 1;
  console.log('\n=== ORDERS (last 90 min) ===');
  console.log('Total:', orders.length);
  for (const [s, c] of Object.entries(byStatus).sort((a, b) => b[1] - a[1])) {
    console.log('  ' + s.padEnd(20), c);
  }

  // Confirms timeline (last 90 min)
  const confirmed = orders.filter(o => o.status === 'confirmed' || (o.status && o.status.startsWith('settled_')));
  const sortedConf = confirmed.slice().sort((a, b) => (b.confirmed_at || '').localeCompare(a.confirmed_at || ''));
  console.log('\n=== CONFIRMS (last 90 min) ===');
  console.log('Count:', sortedConf.length);
  for (const o of sortedConf.slice(0, 10)) {
    console.log('  ' + o.confirmed_at + ' | $' + o.confirmed_stake + ' | ' + o.parlay_id.slice(0, 8));
  }
  if (sortedConf.length > 0) {
    const last = new Date(sortedConf[0].confirmed_at);
    console.log('  Last fill:', Math.round((now - last.getTime()) / 60000), 'min ago');
  }

  // Rejections
  const rejected = orders.filter(o => o.status === 'rejected');
  console.log('\n=== REJECTIONS (last 90 min) ===');
  console.log('Count:', rejected.length);
  const reasonCounts = {};
  for (const o of rejected) {
    const r = o.meta?.rejectionReason || 'unknown';
    let bucket;
    if (/drift/i.test(r)) bucket = 'drift';
    else if (/per-parlay risk/i.test(r)) bucket = 'per-parlay risk cap';
    else if (/team exposure/i.test(r)) bucket = 'team exposure';
    else if (/game exposure/i.test(r)) bucket = 'game exposure';
    else if (/portfolio gross/i.test(r)) bucket = 'portfolio gross cap';
    else if (/series exposure/i.test(r)) bucket = 'series exposure';
    else if (/paused/i.test(r)) bucket = 'paused';
    else if (/cannot reprice/i.test(r)) bucket = 'reprice failed';
    else bucket = r.slice(0, 60);
    reasonCounts[bucket] = (reasonCounts[bucket] || 0) + 1;
  }
  for (const [b, c] of Object.entries(reasonCounts).sort((a, b) => b[1] - a[1])) {
    console.log('  ' + b.padEnd(30), c);
  }

  // matched_parlays
  const matched = await fetchAll('matched_parlays',
    'select=parlay_id,outcome,matched_at,matched_stake,our_odds,matched_odds'
    + '&matched_at=gte.' + encodeURIComponent(fromIso)
  );
  const mpUniq = new Map();
  for (const r of matched) if (!mpUniq.has(r.parlay_id)) mpUniq.set(r.parlay_id, r);
  const mp = [...mpUniq.values()];

  console.log('\n=== MATCHED_PARLAYS (last 90 min) ===');
  console.log('Total:', mp.length);
  const byOutcome = {};
  for (const m of mp) byOutcome[m.outcome] = (byOutcome[m.outcome] || 0) + 1;
  for (const [o, c] of Object.entries(byOutcome).sort((a, b) => b[1] - a[1])) {
    console.log('  ' + o.padEnd(15), c);
  }

  // Gap analysis on losses
  const losses = mp.filter(m => (m.outcome === 'lost' || m.outcome === 'other_sp') && m.our_odds != null && m.matched_odds != null);
  const gaps = losses.map(m => {
    const winBet = -Number(m.matched_odds);
    const ourBet = -Number(m.our_odds);
    const wi = amToProb(winBet);
    const oi = amToProb(ourBet);
    if (wi == null || oi == null) return null;
    return (oi - wi) * 100;
  }).filter(g => g != null);

  console.log('\n=== GAP ANALYSIS (losses last 90 min) ===');
  if (gaps.length > 0) {
    const sorted = gaps.slice().sort((a, b) => a - b);
    const median = sorted[Math.floor(sorted.length / 2)];
    const mean = gaps.reduce((s, x) => s + x, 0) / gaps.length;
    console.log('Loss count w/ gap data:', gaps.length);
    console.log('Median gap pp:', median.toFixed(3));
    console.log('Mean gap pp:', mean.toFixed(3));
    // gap = our_SP_implied − winner_SP_implied (SP-side amToProb).
    // gap > 0 = we offered MORE payout (looser), lost — rare per Alec's
    //          odds-first ranking; almost zero in practice
    // gap < 0 = we offered LESS payout (tighter, underbid winner) — common
    // gap = 0 = exact integer tie, lost on PX tiebreaker (max_risk, ts)
    const tied = gaps.filter(g => Math.abs(g) < 0.01).length;
    const looser = gaps.filter(g => g > 0.01).length;     // we overbid, lost
    const tighter = gaps.filter(g => g < -0.01).length;   // we underbid winner
    console.log('Tied (=0):', tied, '· tighter/underbid (<0):', tighter, '· looser/overbid (>0):', looser);
  } else {
    console.log('No losses with gap data');
  }

  // Recent 10 lost auctions detail
  console.log('\n=== 10 MOST RECENT LOSSES (last 90 min) ===');
  const recentLosses = mp
    .filter(m => m.outcome === 'lost' || m.outcome === 'other_sp')
    .sort((a, b) => (b.matched_at || '').localeCompare(a.matched_at || ''))
    .slice(0, 10);
  for (const m of recentLosses) {
    const winBet = -Number(m.matched_odds);
    const ourBet = -Number(m.our_odds);
    const wi = amToProb(winBet);
    const oi = amToProb(ourBet);
    const gap = (wi != null && oi != null) ? ((oi - wi) * 100).toFixed(2) : '?';
    console.log('  ' + m.matched_at.slice(11, 19), 'our=' + (ourBet >= 0 ? '+' : '') + ourBet, 'winner=' + (winBet >= 0 ? '+' : '') + winBet, 'gap=' + gap + 'pp', '$' + m.matched_stake);
  }
})().catch(e => { console.error(e); process.exit(1); });
