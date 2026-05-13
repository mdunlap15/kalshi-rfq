// Pricing review — compare last 10h to historical baselines.
// Pulls parlay_orders + matched_parlays and computes:
//   - Quote / confirm / fill rate
//   - Gap-to-winner distribution (tighter / tied / looser)
//   - Median + mean gap pp
//   - Auction win rate (matched / reached-auction)
//   - Walked-away count
//   - Average offered-vs-fair vig
//
// Baselines: same 10h window for each of the prior 7 days, plus the
// 30-day rolling average for any-time-of-day. Lets the operator see
// whether recent pricing has gotten tighter or looser vs. their own
// historical norm.

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
function pct(arr, p) {
  if (arr.length === 0) return null;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(s.length * p / 100))];
}
function mean(arr) {
  if (arr.length === 0) return null;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

// Compute pricing stats for a given window [fromMs, toMs).
async function computeWindow(label, fromMs, toMs) {
  const fromIso = new Date(fromMs).toISOString();
  const toIso = new Date(toMs).toISOString();

  const orders = await fetchAll('parlay_orders',
    'select=parlay_id,quoted_at,status,offered_odds,confirmed_stake,fair_parlay_prob'
    + '&quoted_at=gte.' + encodeURIComponent(fromIso)
    + '&quoted_at=lt.' + encodeURIComponent(toIso)
  );

  const matched = await fetchAll('matched_parlays',
    'select=parlay_id,outcome,our_odds,matched_odds,matched_stake'
    + '&matched_at=gte.' + encodeURIComponent(fromIso)
    + '&matched_at=lt.' + encodeURIComponent(toIso)
  );
  // Dedupe matched by parlay_id
  const mpUniq = new Map();
  for (const m of matched) if (!mpUniq.has(m.parlay_id)) mpUniq.set(m.parlay_id, m);
  const mp = [...mpUniq.values()];

  // Quote / confirm split
  const quoted = orders.length;
  const confirmed = orders.filter(o => o.status === 'confirmed' || (o.status && o.status.startsWith('settled_'))).length;
  const rejected = orders.filter(o => o.status === 'rejected').length;
  const confirmedStakeSum = orders
    .filter(o => o.status === 'confirmed' || (o.status && o.status.startsWith('settled_')))
    .reduce((s, o) => s + (Number(o.confirmed_stake) || 0), 0);

  // Pricing distance: offered_prob - fair_prob (positive = we offered worse than fair)
  const pricingDeltas = [];
  for (const o of orders) {
    const oddsAm = Number(o.offered_odds);
    const fairProb = Number(o.fair_parlay_prob);
    if (!Number.isFinite(oddsAm) || !Number.isFinite(fairProb) || fairProb <= 0 || fairProb >= 1) continue;
    // o.offered_odds is bettor-side American. amToProb gives bettor-side implied prob.
    const offeredProb = amToProb(oddsAm);
    if (offeredProb == null) continue;
    pricingDeltas.push((fairProb - offeredProb) * 100); // pp where positive = we charged vig
  }

  // Auction outcomes
  const byOutcome = {};
  for (const m of mp) byOutcome[m.outcome] = (byOutcome[m.outcome] || 0) + 1;

  // Gap analysis on losses (we reached auction but other SP won)
  const losses = mp.filter(m => (m.outcome === 'other_sp' || m.outcome === 'lost')
    && m.our_odds != null && m.matched_odds != null);
  const gaps = [];
  for (const m of losses) {
    const wi = amToProb(-Number(m.matched_odds));
    const oi = amToProb(-Number(m.our_odds));
    if (wi == null || oi == null) continue;
    gaps.push((oi - wi) * 100); // negative = we offered better (looser), positive = worse (tighter)
  }
  const tied = gaps.filter(g => Math.abs(g) < 0.01).length;
  const tighter = gaps.filter(g => g > 0.01).length;
  const looser = gaps.filter(g => g < -0.01).length;

  return {
    label,
    from: fromIso, to: toIso,
    hours: (toMs - fromMs) / 3600000,
    quoted,
    confirmed,
    rejected,
    fillRatePct: quoted > 0 ? (confirmed / quoted * 100) : 0,
    confirmedStakeSum,
    pricingVig: {
      n: pricingDeltas.length,
      p50: pct(pricingDeltas, 50),
      p25: pct(pricingDeltas, 25),
      p75: pct(pricingDeltas, 75),
      mean: mean(pricingDeltas),
    },
    auction: {
      reached: losses.length + (byOutcome.won || 0),
      won: byOutcome.won || 0,
      lost: losses.length,
      missed: byOutcome.missed || 0,
      otherSp: byOutcome.other_sp || 0,
    },
    gap: {
      n: gaps.length,
      p50: pct(gaps, 50),
      mean: mean(gaps),
      tighter, tied, looser,
      tighterPct: gaps.length > 0 ? tighter / gaps.length * 100 : 0,
      tiedPct: gaps.length > 0 ? tied / gaps.length * 100 : 0,
      looserPct: gaps.length > 0 ? looser / gaps.length * 100 : 0,
    },
  };
}

function fmtMs(ms) {
  if (ms == null) return '   -  ';
  return (ms >= 0 ? '+' : '') + ms.toFixed(2) + 'pp';
}
function fmtPct(p) {
  if (p == null) return '  -  ';
  return p.toFixed(1) + '%';
}
function fmt$(n) {
  if (n == null) return '$0';
  if (Math.abs(n) >= 1000) return '$' + Math.round(n).toLocaleString();
  return '$' + n.toFixed(0);
}

(async () => {
  const now = Date.now();
  const TEN_H = 10 * 3600000;

  console.log('========================================================');
  console.log('         PRICING REVIEW — last 10 hours vs baselines');
  console.log('         Generated: ' + new Date().toISOString());
  console.log('========================================================');

  // Target window: last 10 hours
  const targetFrom = now - TEN_H;
  const targetTo = now;

  // Baseline windows: same 10h window for each of past 7 days
  const baselines = [];
  for (let d = 1; d <= 7; d++) {
    const fromMs = targetFrom - d * 86400000;
    const toMs = targetTo - d * 86400000;
    baselines.push({ label: `D-${d}`, fromMs, toMs });
  }

  const target = await computeWindow('LAST 10H', targetFrom, targetTo);
  const baselineResults = [];
  for (const b of baselines) {
    const r = await computeWindow(b.label, b.fromMs, b.toMs);
    baselineResults.push(r);
  }

  // Aggregate baseline (mean of last 7 same-windows, excluding zero-quote periods)
  const valid = baselineResults.filter(b => b.quoted > 0);
  const agg = {
    quoted: mean(valid.map(b => b.quoted)),
    confirmed: mean(valid.map(b => b.confirmed)),
    fillRatePct: mean(valid.map(b => b.fillRatePct)),
    confirmedStakeSum: mean(valid.map(b => b.confirmedStakeSum)),
    vigP50: mean(valid.map(b => b.pricingVig.p50).filter(v => v != null)),
    gapP50: mean(valid.map(b => b.gap.p50).filter(v => v != null)),
    tighterPct: mean(valid.map(b => b.gap.tighterPct)),
    tiedPct: mean(valid.map(b => b.gap.tiedPct)),
    looserPct: mean(valid.map(b => b.gap.looserPct)),
  };

  // -------- Section: Volume --------
  console.log();
  console.log('=== VOLUME ===');
  console.log('Window        | Quotes  | Confirms | Fill %  | Stake $');
  console.log('--------------+---------+----------+---------+----------');
  const row = (r) => console.log(
    r.label.padEnd(13) + ' | ' +
    String(r.quoted).padStart(7) + ' | ' +
    String(r.confirmed).padStart(8) + ' | ' +
    fmtPct(r.fillRatePct).padStart(7) + ' | ' +
    fmt$(r.confirmedStakeSum).padStart(9)
  );
  row(target);
  console.log('--- baselines (same 10h window, prior days) ---');
  for (const b of baselineResults) row(b);
  console.log('--------------+---------+----------+---------+----------');
  console.log(
    'BASELINE AVG  | ' +
    String(Math.round(agg.quoted || 0)).padStart(7) + ' | ' +
    String(Math.round(agg.confirmed || 0)).padStart(8) + ' | ' +
    fmtPct(agg.fillRatePct).padStart(7) + ' | ' +
    fmt$(agg.confirmedStakeSum).padStart(9)
  );

  // -------- Section: Pricing distance from fair --------
  console.log();
  console.log('=== PRICING vs FAIR (offered_implied − fair_implied, pp; +ve = we charged vig) ===');
  console.log('Window        | n      | p25     | p50    | p75    | mean');
  console.log('--------------+--------+---------+--------+--------+--------');
  const vigRow = (r) => console.log(
    r.label.padEnd(13) + ' | ' +
    String(r.pricingVig.n).padStart(6) + ' | ' +
    fmtMs(r.pricingVig.p25).padStart(7) + ' | ' +
    fmtMs(r.pricingVig.p50).padStart(6) + ' | ' +
    fmtMs(r.pricingVig.p75).padStart(6) + ' | ' +
    fmtMs(r.pricingVig.mean).padStart(6)
  );
  vigRow(target);
  for (const b of baselineResults) vigRow(b);

  // -------- Section: Auction outcomes --------
  console.log();
  console.log('=== AUCTION OUTCOMES (only includes parlays we reached auction on) ===');
  console.log('Window        | Reached | Won     | Lost   | Win %');
  console.log('--------------+---------+---------+--------+--------');
  const aucRow = (r) => console.log(
    r.label.padEnd(13) + ' | ' +
    String(r.auction.reached).padStart(7) + ' | ' +
    String(r.auction.won).padStart(7) + ' | ' +
    String(r.auction.lost).padStart(6) + ' | ' +
    fmtPct(r.auction.reached > 0 ? r.auction.won / r.auction.reached * 100 : 0).padStart(6)
  );
  aucRow(target);
  for (const b of baselineResults) aucRow(b);

  // -------- Section: Loss gap split --------
  console.log();
  console.log('=== LOSS GAP SPLIT (when we lost auction: did we offer worse, match, or beat?) ===');
  console.log('Window        | n     | Tighter% | Tied%   | Looser% | Median gap');
  console.log('--------------+-------+----------+---------+---------+----------');
  console.log('  (tighter = we underbid; tied = same price; looser = we beat winner but still lost)');
  const gapRow = (r) => console.log(
    r.label.padEnd(13) + ' | ' +
    String(r.gap.n).padStart(5) + ' | ' +
    fmtPct(r.gap.tighterPct).padStart(8) + ' | ' +
    fmtPct(r.gap.tiedPct).padStart(7) + ' | ' +
    fmtPct(r.gap.looserPct).padStart(7) + ' | ' +
    fmtMs(r.gap.p50).padStart(7)
  );
  gapRow(target);
  for (const b of baselineResults) gapRow(b);
  console.log('--------------+-------+----------+---------+---------+----------');
  console.log('BASELINE AVG  | -     | ' +
    fmtPct(agg.tighterPct).padStart(8) + ' | ' +
    fmtPct(agg.tiedPct).padStart(7) + ' | ' +
    fmtPct(agg.looserPct).padStart(7) + ' | ' +
    fmtMs(agg.gapP50).padStart(7)
  );

  // -------- Section: Deltas / interpretation --------
  console.log();
  console.log('=== INTERPRETATION ===');
  const deltaFill = target.fillRatePct - agg.fillRatePct;
  const deltaVig = (target.pricingVig.p50 || 0) - (agg.vigP50 || 0);
  const deltaTighter = target.gap.tighterPct - agg.tighterPct;
  const deltaLooser = target.gap.looserPct - agg.looserPct;
  console.log('  Fill rate: ' + fmtPct(target.fillRatePct) + ' (baseline ' + fmtPct(agg.fillRatePct) + ', Δ ' + (deltaFill >= 0 ? '+' : '') + deltaFill.toFixed(1) + ' pts)');
  console.log('  Median vig over fair: ' + fmtMs(target.pricingVig.p50) + ' (baseline ' + fmtMs(agg.vigP50) + ', Δ ' + fmtMs(deltaVig) + ')');
  console.log('  Tighter losses (we underbid): ' + fmtPct(target.gap.tighterPct) + ' (baseline ' + fmtPct(agg.tighterPct) + ', Δ ' + (deltaTighter >= 0 ? '+' : '') + deltaTighter.toFixed(1) + ' pts)');
  console.log('  Looser losses (latency-tax): ' + fmtPct(target.gap.looserPct) + ' (baseline ' + fmtPct(agg.looserPct) + ', Δ ' + (deltaLooser >= 0 ? '+' : '') + deltaLooser.toFixed(1) + ' pts)');
})().catch(e => { console.error(e); process.exit(1); });
