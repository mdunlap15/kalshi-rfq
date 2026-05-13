require('dotenv').config();
const SUPA_URL = process.env.SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_SERVICE_KEY;

async function fetchAll(table, query) {
  const all = [];
  let offset = 0;
  while (true) {
    const url = SUPA_URL + '/rest/v1/' + table + '?' + query + '&limit=1000&offset=' + offset;
    const r = await fetch(url, { headers: { apikey: SUPA_KEY, Authorization: 'Bearer ' + SUPA_KEY } });
    if (!r.ok) { console.error('fetch failed', r.status, await r.text()); break; }
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
  // Window: since last fill at 17:57 UTC
  const fromIso = '2026-05-12T17:58:00Z';
  console.log('Window: since 17:58 UTC (after last fill at 17:57 UTC)');
  console.log();

  const orders = await fetchAll('parlay_orders',
    'select=parlay_id,quoted_at,status,offered_odds,fair_parlay_prob,meta'
    + '&quoted_at=gte.' + encodeURIComponent(fromIso)
    + '&order=quoted_at.desc'
  );

  console.log('=== ORDERS POST-DROUGHT-START ===');
  const byStatus = {};
  for (const o of orders) {
    const s = o.status || 'unknown';
    byStatus[s] = (byStatus[s] || 0) + 1;
  }
  console.log('Total orders:', orders.length);
  for (const [s, c] of Object.entries(byStatus).sort((a, b) => b[1] - a[1])) {
    console.log('  ' + s.padEnd(20), c);
  }
  console.log();

  // Pull matched_parlays in window
  const matched = await fetchAll('matched_parlays',
    'select=parlay_id,matched_odds,our_odds,matched_stake,matched_at,outcome'
    + '&matched_at=gte.' + encodeURIComponent(fromIso)
  );
  const mpUniq = new Map();
  for (const r of matched) if (!mpUniq.has(r.parlay_id)) mpUniq.set(r.parlay_id, r);
  const mpDedup = [...mpUniq.values()];

  console.log('=== MATCHED_PARLAYS POST-DROUGHT-START ===');
  const mpByOutcome = {};
  for (const m of mpDedup) {
    const o = m.outcome || 'unknown';
    mpByOutcome[o] = (mpByOutcome[o] || 0) + 1;
  }
  console.log('Total matched (deduped):', mpDedup.length);
  for (const [o, c] of Object.entries(mpByOutcome).sort((a, b) => b[1] - a[1])) {
    console.log('  ' + o.padEnd(20), c);
  }
  console.log();

  // Gap analysis on other_sp losses — are we close or way off?
  const otherSpLosses = mpDedup.filter(m => m.outcome === 'other_sp' || m.outcome === 'lost');
  const gaps = [];
  const offeredCompare = [];
  for (const m of otherSpLosses) {
    if (m.our_odds == null || m.matched_odds == null) continue;
    const winBet = -Number(m.matched_odds);
    const ourBet = -Number(m.our_odds);
    const wi = amToProb(winBet);
    const oi = amToProb(ourBet);
    if (wi != null && oi != null) {
      const gapPp = (oi - wi) * 100;
      gaps.push(gapPp);
      offeredCompare.push({
        ourBet, winBet, gapPp,
        ourTighter: oi > wi,
        stake: Number(m.matched_stake) || 0,
        time: m.matched_at,
      });
    }
  }
  console.log('=== LOST-AUCTION GAP ANALYSIS ===');
  console.log('Auctions lost where we have both prices:', gaps.length);
  if (gaps.length > 0) {
    const sortedGaps = gaps.slice().sort((a, b) => a - b);
    const mean = gaps.reduce((s, x) => s + x, 0) / gaps.length;
    const median = sortedGaps[Math.floor(sortedGaps.length / 2)];
    const p90 = sortedGaps[Math.floor(sortedGaps.length * 0.9)];
    console.log('Median gap pp:', median.toFixed(3));
    console.log('Mean gap pp:  ', mean.toFixed(3));
    console.log('P90 gap pp:   ', p90.toFixed(3));
    console.log('  (negative = we underbid winner [common]; ~0 = integer tie, lost on tiebreak; positive = we offered better, lost anyway [rare per Alec])');
    console.log();
    // Tied vs not
    // gap = our_SP_implied − winner_SP_implied. See sign-convention notes
    // in _peak_check.js / _pricing_review_10h.js for the full breakdown.
    const tied = offeredCompare.filter(x => Math.abs(x.gapPp) < 0.01);
    const tighterButLost = offeredCompare.filter(x => x.gapPp < -0.01);    // we underbid winner
    const looserButLost = offeredCompare.filter(x => x.gapPp > 0.01);      // we offered better, lost
    console.log('Tied at integer (lost on tiebreak):', tied.length);
    console.log('We were tighter — underbid winner on price:', tighterButLost.length);
    console.log('We were looser — offered better, lost anyway:', looserButLost.length);

    console.log('\nLast 5 lost auctions:');
    for (const x of offeredCompare.slice(0, 5)) {
      console.log('  ', x.time.slice(11, 19), 'our=' + (x.ourBet >= 0 ? '+' : '') + x.ourBet, 'winner=' + (x.winBet >= 0 ? '+' : '') + x.winBet, 'gapPp=' + x.gapPp.toFixed(2), '$' + x.stake.toFixed(0));
    }
  }

  // Most recent declined orders — what reason?
  console.log('\n=== RECENT REJECTIONS ===');
  const rejected = orders.filter(o => o.status === 'rejected').slice(0, 10);
  for (const r of rejected) {
    console.log('  ' + r.quoted_at.slice(11, 19) + ' | ' + (r.meta?.rejectionReason || 'no reason'));
  }
  if (rejected.length === 0) console.log('  (no rejections in window)');

  // 5 most recent quotes — what's the offered vs fair
  console.log('\n=== 5 MOST RECENT QUOTES ===');
  for (const o of orders.slice(0, 5)) {
    const fairProb = Number(o.fair_parlay_prob);
    const fairAm = fairProb > 0 && fairProb < 1
      ? (fairProb >= 0.5 ? -Math.round(100 * fairProb / (1 - fairProb)) : Math.round(100 * (1 - fairProb) / fairProb))
      : '?';
    console.log('  ' + o.quoted_at.slice(11, 19), 'offered=' + (o.offered_odds >= 0 ? '+' : '') + o.offered_odds, 'fair=' + (fairAm > 0 ? '+' + fairAm : fairAm));
  }
})().catch(e => { console.error(e); process.exit(1); });
