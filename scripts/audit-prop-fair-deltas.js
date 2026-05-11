// Audit player-prop fair probabilities against a fresh TOA re-lookup.
//
// Built 2026-05-11 after discovering the line-manager pre-seed was
// propagating one line's fair probabilities to every alt line on the
// same market — so quotes on Mike Trout's 1.5 TB Under were running at
// the 0.5-line's fair (~0.43) instead of the true 1.5-line fair (~0.64).
// The fix scopes the TOA lookup per line; this script proves the fix
// is holding by comparing what we CAPTURED on each prop quote against
// what TOA returns NOW for that exact (player, line, side).
//
// Run periodically (cron / manual sanity check):
//   node scripts/audit-prop-fair-deltas.js
//   node scripts/audit-prop-fair-deltas.js --hours 48 --min-delta 0.03
//
// Output is sorted by abs delta. Anything > 5pp deserves an explanation
// (could be a real market move, but typically it's a pricing bug).
// Steady-state expectation post-fix: handful of < 2pp entries from
// natural market drift; zero entries > 10pp.
require('dotenv').config();
const oddsFeed = require('../services/odds-feed');

const SUPA_URL = process.env.SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_SERVICE_KEY;

const args = process.argv.slice(2);
const argHours = (() => { const i = args.indexOf('--hours'); return i >= 0 ? Number(args[i + 1]) : 24; })();
const argMinDelta = (() => { const i = args.indexOf('--min-delta'); return i >= 0 ? Number(args[i + 1]) : 0.05; })();

// Map internal prop market types to (TOA sport, TOA market key).
const TOA_MARKET = {
  player_hitter_total_bases: 'batter_total_bases',
  player_hitter_hits: 'batter_hits',
  player_hitter_hr: 'batter_home_runs',
  player_hitter_rbi_runs: 'batter_rbis',
  player_strikeouts: 'pitcher_strikeouts',
  player_points: 'player_points',
  player_rebounds: 'player_rebounds',
  player_assists: 'player_assists',
  player_threes_made: 'player_threes',
  player_shots_on_goal: 'player_shots_on_goal',
};
const TOA_SPORT = {
  player_hitter_total_bases: 'baseball_mlb',
  player_hitter_hits: 'baseball_mlb',
  player_hitter_hr: 'baseball_mlb',
  player_hitter_rbi_runs: 'baseball_mlb',
  player_strikeouts: 'baseball_mlb',
  player_points: 'basketball_nba',
  player_rebounds: 'basketball_nba',
  player_assists: 'basketball_nba',
  player_threes_made: 'basketball_nba',
  player_shots_on_goal: 'icehockey_nhl',
};

async function fetchJson(url) {
  const r = await fetch(url, { headers: { apikey: SUPA_KEY, Authorization: 'Bearer ' + SUPA_KEY } });
  if (!r.ok) { console.error('fetch failed:', r.status, await r.text()); return null; }
  return r.json();
}

(async () => {
  const fromIso = new Date(Date.now() - argHours * 3600 * 1000).toISOString();
  console.log(`Pulling parlay_orders with player-prop legs from the last ${argHours}h...`);

  let all = [];
  for (let offset = 0; offset < 200000; offset += 1000) {
    const url = SUPA_URL + '/rest/v1/parlay_orders?select=parlay_id,quoted_at,status,legs'
      + '&quoted_at=gte.' + encodeURIComponent(fromIso)
      + '&order=quoted_at.desc&limit=1000&offset=' + offset;
    const page = await fetchJson(url);
    if (!page) break;
    all.push(...page);
    if (page.length < 1000) break;
  }
  console.log('Orders pulled:', all.length);

  // Dedupe to unique (propType, player, line, side). For each unique
  // combination, capture the most recent fair seen. We only re-lookup
  // ONCE per unique combination to avoid burning TOA quota on identical
  // quotes — the captured fairs are stable across repeat quotes anyway
  // (line-manager refreshes every 2 min, fair is bucketed at that grain).
  const unique = new Map();
  for (const o of all) {
    for (const l of (o.legs || [])) {
      if (!l.market || !l.market.startsWith('player_')) continue;
      if (l.line == null || l.fairProb == null) continue;
      if (!l.homeTeam || !l.awayTeam) continue;
      const player = l.team || l.playerName;
      if (!player) continue;
      const key = l.market + '|' + player + '|' + l.line + '|' + l.selection;
      if (!unique.has(key)) {
        unique.set(key, {
          propType: l.market,
          player,
          line: l.line,
          selection: l.selection,
          fair: l.fairProb,
          homeTeam: l.homeTeam,
          awayTeam: l.awayTeam,
          startTime: l.startTime,
          quotedAt: o.quoted_at,
          parlayId: o.parlay_id,
        });
      }
    }
  }
  console.log('Unique (propType, player, line, side):', unique.size);

  const results = [];
  let i = 0;
  for (const entry of unique.values()) {
    i++;
    const toaMarket = TOA_MARKET[entry.propType];
    const toaSport = TOA_SPORT[entry.propType];
    if (!toaMarket || !toaSport) continue;
    try {
      const live = await oddsFeed.lookupTheOddsApiPlayerProp(
        toaSport, toaMarket,
        { homeTeam: entry.homeTeam, awayTeam: entry.awayTeam, startTime: entry.startTime },
        entry.player, entry.line,
      );
      if (!live || live.error || live.fairProbOver == null || live.fairProbUnder == null) continue;
      const liveFair = entry.selection === 'over' ? live.fairProbOver : live.fairProbUnder;
      const delta = entry.fair - liveFair;
      results.push({ ...entry, liveFair, liveBooks: live.booksWithBothSides, delta, absDelta: Math.abs(delta) });
    } catch (err) {
      console.error('lookup failed for', entry.player, entry.propType, entry.line, ':', err.message);
    }
    // Light throttle to stay under TOA rate limit on cold cache
    if (i % 10 === 0) await new Promise(r => setTimeout(r, 200));
  }

  results.sort((a, b) => b.absDelta - a.absDelta);
  const flagged = results.filter(r => r.absDelta >= argMinDelta);

  console.log(`\n=== Fair-prob deltas ≥ ${(argMinDelta * 100).toFixed(1)}pp ===`);
  if (flagged.length === 0) {
    console.log('  (none — fix is holding)');
  } else {
    console.log('|delta| pp | propType                   | player                | line | side  | captured | live   | live_books');
    for (const r of flagged) {
      console.log(
        '  ' + (r.absDelta * 100).toFixed(1).padStart(5) + 'pp',
        r.propType.padEnd(28),
        r.player.padEnd(22),
        String(r.line).padStart(4),
        r.selection.padEnd(5),
        r.fair.toFixed(4).padStart(8),
        r.liveFair.toFixed(4).padStart(8),
        String(r.liveBooks).padStart(3),
      );
    }
  }

  console.log('\n=== Summary ===');
  console.log('  Quotes audited:', results.length);
  console.log('  Delta > 10pp:', results.filter(x => x.absDelta > 0.10).length);
  console.log('  Delta > 5pp:', results.filter(x => x.absDelta > 0.05).length);
  console.log('  Delta > 2pp:', results.filter(x => x.absDelta > 0.02).length);
})().catch(e => { console.error(e); process.exit(1); });
