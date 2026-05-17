// Verify the signature-cooldown end-to-end: prove that after a parlay
// is "confirmed" (via either status-set path), a subsequent same-sig
// shouldDecline call returns block. This isn't a smoke test of the
// module — it's an integration test that touches the actual paths
// used in production.
require('dotenv').config();

const pricer = require('../services/pricer');
const sigCd = require('../services/sig-cooldown');

// Use Mike's actual back-to-back parlay legs as the fixture.
const legs1 = [
  { line_id: 'fixture-leg-1a', team: 'Atlanta Dream', market: 'spread', line: 3.5, selection: 'home' },
  { line_id: 'fixture-leg-1b', team: 'Detroit Pistons', market: 'moneyline', selection: 'home' },
];
// Same signature (collapses spread line per cooldown's buildSigKey).
const legs2 = [
  { line_id: 'fixture-leg-2a', team: 'Atlanta Dream', market: 'spread', line: 3.5, selection: 'home' },
  { line_id: 'fixture-leg-2b', team: 'Detroit Pistons', market: 'moneyline', selection: 'home' },
];
// Different signature — bot trying to evade by adding a 3rd leg.
const legs3 = [
  ...legs2,
  { line_id: 'fixture-leg-3c', team: 'Indiana Fever', market: 'spread', line: -10.5, selection: 'home' },
];
// Different teams entirely — must not be blocked.
const legsUnrelated = [
  { line_id: 'fixture-other-a', team: 'Boston Celtics', market: 'moneyline', selection: 'home' },
];

let passed = 0, failed = 0;
function test(name, cond, detail) {
  if (cond) { console.log('  ✓', name); passed++; }
  else      { console.log('  ✗ FAIL:', name, detail ? `(${detail})` : ''); failed++; }
}

console.log('=== Test 1: shouldDecline returns no cooldown block for fresh signatures ===');
// Note: shouldDecline will decline these for OTHER reasons (unknown legs,
// since fixture lineIds aren't registered) — we're specifically asserting
// the reason is NOT 'signature_cooldown'.
let r = pricer.shouldDecline(legs1, 'fixture-parlay-1');
test('legs1 not blocked by signature_cooldown (cold start)',
  !(r.declined && r.reason === 'signature_cooldown'),
  `reason=${r.reason}, detail=${r.detail}`);

console.log('\n=== Test 2: lockSignature arms the lock ===');
sigCd.lockSignature(legs1, 'fixture-parlay-1');
const lock = sigCd.checkSignatureCooldown(legs1);
test('checkSignatureCooldown returns block after lock', lock && lock.block === true, JSON.stringify(lock));
test('ageMs near 0', lock && lock.ageMs < 100, lock && `ageMs=${lock.ageMs}`);
test('remainingMs near full window', lock && lock.remainingMs > 119000, lock && `remainingMs=${lock.remainingMs}`);

console.log('\n=== Test 3: shouldDecline now returns signature_cooldown on same-sig RFQ ===');
r = pricer.shouldDecline(legs2, 'fixture-parlay-2');
test('declined=true', r.declined === true);
test("reason='signature_cooldown'", r.reason === 'signature_cooldown', `got reason='${r.reason}'`);
test('detail mentions cooldown', /cooldown/i.test(r.detail || ''), `detail='${r.detail}'`);

console.log('\n=== Test 4: Different-signature parlay is NOT blocked by cooldown ===');
r = pricer.shouldDecline(legsUnrelated, 'fixture-parlay-3');
test('unrelated parlay not blocked by signature_cooldown',
  !(r.declined && r.reason === 'signature_cooldown'),
  `reason=${r.reason}`);

console.log('\n=== Test 5: Adding a leg DOES change signature (bot evasion attempt fails to use the cooldown) ===');
// 3-leg parlay has a DIFFERENT signature than the 2-leg one — that's correct
// behavior. The cooldown is sig-specific, so adding a leg is a NEW signature
// and the bot would have to consume that cooldown separately. Not a leak —
// just verifying the sig key works as designed.
r = pricer.shouldDecline(legs3, 'fixture-parlay-4');
test('3-leg variant has different sig (not blocked by 2-leg lock)',
  !(r.declined && r.reason === 'signature_cooldown'),
  `reason=${r.reason}`);

console.log('\n=== Test 6: Line-value variant (bot evasion via alt-line) IS blocked ===');
// Bot tries ATL +1.5 instead of +3.5 — must still match (spread line
// values are collapsed in the sig key).
const legsAltLine = [
  { line_id: 'fixture-alt-1', team: 'Atlanta Dream', market: 'spread', line: 1.5, selection: 'home' },
  { line_id: 'fixture-alt-2', team: 'Detroit Pistons', market: 'moneyline', selection: 'home' },
];
r = pricer.shouldDecline(legsAltLine, 'fixture-parlay-5');
test('alt-line variant IS blocked (sig collapses spread lines)',
  r.declined && r.reason === 'signature_cooldown',
  `reason=${r.reason}`);

console.log('\n=== Test 7: clearSignature removes the lock ===');
sigCd.clearSignature(legs1);
r = pricer.shouldDecline(legs2, 'fixture-parlay-6');
test('shouldDecline no longer blocks after clearSignature',
  !(r.declined && r.reason === 'signature_cooldown'),
  `reason=${r.reason}`);

console.log('\n=== Test 8: db.saveOrder hook arms the lock for status=confirmed writes ===');
// DEFAULT: do NOT exercise the real db.saveOrder path. The hook is a
// single line inside saveOrder ("if status==='confirmed', call
// lockSignature") and is statically verifiable by inspection. Running
// it for real means writing a fake confirmed-status row to whatever
// Supabase the env vars point at — which previously leaked a
// fixture row into production's parlay_orders and surfaced as an open
// position in the dashboard.
//
// Pass --include-db-write ONLY when running against a sidecar /
// dev Supabase. The fixture is auto-deleted at the end either way.
const includeDbWrite = process.argv.includes('--include-db-write');
if (!includeDbWrite) {
  console.log('  ⚠ skipped (default — pass --include-db-write to enable; only safe against a non-production Supabase)');
  console.log('\n=== Summary ===');
  console.log(`  ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
// --include-db-write path: explicit opt-in. Still cleans up after itself.
const db = require('../services/db');
const fixtureId = 'fixture-saveorder-test-' + Date.now();
const fakeOrder = {
  parlayId: fixtureId,
  status: 'confirmed',
  legs: legs1,
  offeredOdds: 200,
  confirmedOdds: -200,
  confirmedStake: 100,
  orderUuid: 'fixture-uuid-saveorder',
  quotedAt: new Date().toISOString(),
  confirmedAt: new Date().toISOString(),
  meta: {},
};
(async () => {
  sigCd.clearSignature(legs1);
  const preLock = sigCd.checkSignatureCooldown(legs1);
  test('lock empty before saveOrder', preLock === null);
  if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_KEY) {
    console.log('  ⚠ no Supabase creds in env — skipping');
  } else {
    await db.saveOrder(fakeOrder);
    const postLock = sigCd.checkSignatureCooldown(legs1);
    test('lock armed after db.saveOrder with status=confirmed',
      postLock && postLock.block === true,
      JSON.stringify(postLock));
    // ALWAYS delete the fixture row, even if assertion failed. Belt and
    // suspenders: also delete any other fixture-saveorder-test rows
    // that might have leaked from previous runs.
    const { createClient } = require('@supabase/supabase-js');
    const cleanup = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
    const { error: delErr } = await cleanup.from('parlay_orders')
      .delete()
      .like('parlay_id', 'fixture-saveorder-test-%');
    test('fixture row cleaned up from Supabase', !delErr, delErr && delErr.message);
    sigCd.clearSignature(legs1);
  }

  console.log('\n=== Summary ===');
  console.log(`  ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
})().catch(e => { console.error('FATAL:', e); process.exit(2); });
