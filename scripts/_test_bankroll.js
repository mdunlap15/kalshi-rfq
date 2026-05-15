// Mini server that mounts JUST the /bankroll endpoint so we can validate
// the dual-cohort logic against live Supabase data.

require('dotenv').config();
const express = require('express');
const db = require('./../services/db');
const config = require('./../config').config;
const log = require('./../services/logger');

const app = express();

const _bankrollCache = { at: 0, data: null };
app.get('/bankroll', async (req, res) => {
  try {
    const now = Date.now();
    if (req.query.refresh !== '1' && _bankrollCache.data && (now - _bankrollCache.at) < 60_000) {
      res.set('X-Cache', 'hit'); return res.json(_bankrollCache.data);
    }
    const sb = db.getClient();
    if (!sb) return res.status(500).json({ ok: false, error: 'no DB' });

    const { data: kvRow, error: kvErr } = await sb.from('kv_store').select('value, updated_at').eq('key', 'bankroll_state').maybeSingle();
    if (kvErr) return res.status(500).json({ ok: false, error: kvErr.message });
    if (!kvRow || !kvRow.value) return res.status(404).json({ ok: false, error: 'no snapshot' });
    const snap = kvRow.value;
    const preIds = new Set(snap.preCohortIds || []);

    const settled = [];
    let offset = 0;
    while (true) {
      const { data, error } = await sb.from('parlay_orders')
        .select('parlay_id, pnl, status')
        .gte('settled_at', snap.snapshotAt)
        .in('status', ['settled_won', 'settled_lost', 'settled_push'])
        .order('settled_at', { ascending: true })
        .range(offset, offset + 999);
      if (error) { console.error('settled err', error.message); break; }
      if (!data || !data.length) break;
      settled.push(...data);
      if (data.length < 1000) break;
      offset += 1000;
    }

    let prePnl = 0, postPnl = 0, preCount = 0, postCount = 0;
    for (const r of settled) {
      const pnl = Number(r.pnl); if (!Number.isFinite(pnl)) continue;
      if (preIds.has(r.parlay_id)) { prePnl += pnl; preCount++; }
      else { postPnl += pnl; postCount++; }
    }

    const mikeBooks = snap.snapshotMike + snap.preRatio.mike * prePnl + snap.postRatio.mike * postPnl;
    const rickBooks = snap.snapshotRick + snap.preRatio.rick * prePnl + snap.postRatio.rick * postPnl;

    // Live TE — query PX prod via fetch since we don't boot the trader here
    let liveTE = null;
    try {
      const auth = 'Basic ' + Buffer.from(`${process.env.AUTH_USERNAME}:${process.env.AUTH_PASSWORD}`).toString('base64');
      const PROD = process.env.PROD_URL || 'https://prophetx-rfq-production-6781.up.railway.app';
      const [bal, pnl] = await Promise.all([
        fetch(PROD + '/balance', { headers: { Authorization: auth } }).then(r => r.json()),
        fetch(PROD + '/px-pnl',  { headers: { Authorization: auth } }).then(r => r.json()),
      ]);
      const cash = bal?.balance?.balance;
      const exp  = pnl?.openExposure;
      if (cash != null && exp != null) liveTE = cash + exp;
    } catch (e) { console.warn('live TE fetch fail', e.message); }

    const total = mikeBooks + rickBooks;
    const drift = liveTE != null ? liveTE - total : null;
    const pendingStatus = snap.pendingWithdrawal && drift != null ? {
      partner: snap.pendingWithdrawal.partner,
      amount: snap.pendingWithdrawal.amount,
      noticedAt: snap.pendingWithdrawal.noticedAt,
      cashMoved: Math.abs(drift - snap.pendingWithdrawal.amount) > 50,
      driftDelta: drift,
    } : null;

    const payload = {
      ok: true,
      snapshotAt: snap.snapshotAt,
      snapshotEquity: snap.snapshotEquity,
      snapshotMike: snap.snapshotMike,
      snapshotRick: snap.snapshotRick,
      preCohort: {
        ids: snap.preCohortIds.length,
        stakeAtSnapshot: snap.preCohortStakeAtSnapshot,
        settled: preCount, stillOpen: snap.preCohortIds.length - preCount,
        realizedPnl: prePnl, ratio: snap.preRatio,
      },
      postCohort: { settled: postCount, realizedPnl: postPnl, ratio: snap.postRatio },
      mike: mikeBooks, rick: rickBooks, total, liveTotalEquity: liveTE, drift,
      pending: pendingStatus,
      asOfUtc: new Date().toISOString(),
    };
    _bankrollCache.at = now; _bankrollCache.data = payload;
    res.set('X-Cache', 'miss');
    res.json(payload);
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

const PORT = 4100;
const srv = app.listen(PORT, async () => {
  console.log(`Bankroll test server on :${PORT}`);
  // Self-test
  const http = require('http');
  http.get(`http://localhost:${PORT}/bankroll`, r => {
    let buf = ''; r.on('data', c => buf += c); r.on('end', () => {
      console.log('Status:', r.statusCode);
      try {
        const j = JSON.parse(buf);
        console.log(JSON.stringify(j, null, 2));
      } catch (e) { console.log('Parse err:', buf.slice(0, 300)); }
      srv.close(); process.exit(0);
    });
  });
});
