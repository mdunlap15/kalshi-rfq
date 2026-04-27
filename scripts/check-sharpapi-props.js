// Probe whether the configured SharpAPI tier exposes player-prop markets.
// Reads SHARP_ODDS_API_KEY from .env via the existing config.js — the
// key never leaves the repo. Run with: node scripts/check-sharpapi-props.js
//
// Strategy: query the /odds endpoint with a list of candidate prop
// `market=` values. Report which return data, which return empty, and
// which return tier-gate errors (403/4xx). The presence of even ONE
// 200-with-data response confirms Hobby tier exposes props.

const { config } = require('../config');

const BASE = config.oddsApi.baseUrl; // https://api.sharpapi.io/api/v1
const KEY = config.oddsApi.apiKey;

if (!KEY) {
  console.error('SHARP_ODDS_API_KEY missing from .env — abort');
  process.exit(1);
}

// SharpAPI documents `market_type: player_prop` as the response field
// but the query param `market=` likely uses specific market slugs (e.g.
// `pitcher_strikeouts`). We don't know the exact slugs, so probe the
// most likely candidates from common sportsbook prop naming.
// SharpAPI uses short league slugs (mlb, nba) — see LEAGUE_MAP in
// services/odds-feed.js. Probes use the same names.
const PROBES = [
  // MLB pitcher / hitter props
  { league: 'mlb', market: 'pitcher_strikeouts' },
  { league: 'mlb', market: 'pitcher_outs' },
  { league: 'mlb', market: 'strikeouts' },
  { league: 'mlb', market: 'batter_total_bases' },
  { league: 'mlb', market: 'batter_hits' },
  { league: 'mlb', market: 'home_runs' },
  // NBA player props
  { league: 'nba', market: 'player_points' },
  { league: 'nba', market: 'player_rebounds' },
  { league: 'nba', market: 'player_assists' },
  { league: 'nba', market: 'player_threes' },
  // Generic fallbacks
  { league: 'mlb', market: 'player_prop' },
  { league: 'nba', market: 'player_prop' },
];

// First, hit /markets (or /leagues) to enumerate the canonical valid
// values — beats guessing.
async function discover() {
  const endpoints = ['/markets', '/leagues'];
  for (const ep of endpoints) {
    try {
      const resp = await fetch(`${BASE}${ep}`, { headers: { 'X-API-Key': KEY } });
      if (!resp.ok) {
        console.log(`  ${ep} → HTTP ${resp.status}`);
        continue;
      }
      const body = await resp.json();
      const data = body.data || body.markets || body.leagues || body;
      console.log(`  ${ep} → 200, ${Array.isArray(data) ? data.length : '?'} entries`);
      if (Array.isArray(data) && data.length) {
        // Show the first ~30 raw entries to see the schema
        console.log(`     first entries: ${JSON.stringify(data.slice(0, 30))}`);
      }
    } catch (err) {
      console.log(`  ${ep} → ${err.message}`);
    }
  }
}

async function probe({ league, market }) {
  const url = `${BASE}/odds?league=${league}&market=${market}&limit=5`;
  try {
    const resp = await fetch(url, { headers: { 'X-API-Key': KEY } });
    if (!resp.ok) {
      const text = await resp.text();
      return { league, market, status: resp.status, error: text.slice(0, 200) };
    }
    const body = await resp.json();
    const rows = body.data || [];
    const sampleBooks = [...new Set(rows.map(r => r.sportsbook).filter(Boolean))].slice(0, 5);
    const sampleMarketTypes = [...new Set(rows.map(r => r.market_type).filter(Boolean))];
    const sampleMarketNames = [...new Set(rows.map(r => r.market_name || r.name).filter(Boolean))].slice(0, 3);
    return {
      league, market,
      status: 200,
      rowCount: rows.length,
      sampleBooks,
      sampleMarketTypes,
      sampleMarketNames,
      sampleRow: rows[0] || null,
    };
  } catch (err) {
    return { league, market, error: err.message };
  }
}

(async () => {
  console.log(`Probing SharpAPI ${BASE} with key ${KEY.slice(0, 8)}…\n`);

  console.log('=== Discovery: enumerate valid leagues / markets ===');
  await discover();

  console.log(`\n=== Probing ${PROBES.length} (league × market) combinations ===`);

  const results = [];
  for (const p of PROBES) {
    process.stdout.write(`  ${p.league.padEnd(20)} market=${p.market.padEnd(22)} → `);
    const r = await probe(p);
    results.push(r);
    if (r.status === 200) {
      console.log(`200 (${r.rowCount} rows${r.sampleBooks.length ? ', books: ' + r.sampleBooks.join(',') : ''})`);
    } else if (r.status) {
      console.log(`HTTP ${r.status}: ${r.error}`);
    } else {
      console.log(`ERROR: ${r.error}`);
    }
  }

  console.log('\n=== Summary ===');
  const ok = results.filter(r => r.status === 200 && r.rowCount > 0);
  const empty = results.filter(r => r.status === 200 && r.rowCount === 0);
  const errors = results.filter(r => r.status && r.status !== 200);

  console.log(`Returned data:       ${ok.length}`);
  console.log(`Returned empty (200): ${empty.length}`);
  console.log(`Returned error:      ${errors.length}`);

  if (ok.length > 0) {
    console.log('\n✅ Player props ARE accessible on this tier. Markets that returned data:');
    for (const r of ok) {
      console.log(`   - ${r.league}/${r.market}: ${r.rowCount} rows from ${r.sampleBooks.join(',')}`);
      if (r.sampleMarketNames.length) console.log(`       sample names: ${r.sampleMarketNames.join(' | ')}`);
    }
    console.log('\nFull sample row:');
    console.log(JSON.stringify(ok[0].sampleRow, null, 2));
  } else if (empty.length === results.length) {
    console.log('\n⚠ All probes returned 200 with zero rows. Two possibilities:');
    console.log('   1. No live events right now for those markets (expected late at night).');
    console.log('   2. The market slugs above are wrong. Check SharpAPI docs for the exact');
    console.log('      `market=` values they use for props.');
  } else if (errors.some(r => r.status === 403)) {
    console.log('\n❌ Got 403 on at least one probe — Hobby tier likely does NOT expose player props.');
    console.log('   Pro tier ($229/mo) is the documented upgrade path.');
    console.log('   First 403:', errors.find(r => r.status === 403));
  } else if (errors.some(r => r.status === 400)) {
    console.log('\n⚠ Got 400 errors — likely invalid market= slugs. Need to find the correct prop slug names.');
    console.log('   Try the SharpAPI docs or contact support for the canonical list.');
    console.log('   First 400:', errors.find(r => r.status === 400));
  } else {
    console.log('\n❓ Mixed results, no clear answer. Raw error sample:');
    console.log(errors[0] || empty[0]);
  }
})();
