// Minimal express harness that mounts JUST the /network-share-daily endpoint
// so we can verify the HTTP wiring (cache, error path, JSON shape) without
// booting the full PX trader. Hits the endpoint over loopback, prints summary.

require('dotenv').config();
const express = require('express');
const db = require('../services/db');
const log = require('../services/logger');

const app = express();
app.use(express.json());

// Mount just the netshare endpoint (copy/paste the handler — easier than
// trying to extract it from index.js's all-in-one startup function).
const _netShareCache = { key: null, at: 0, data: null };
app.get('/network-share-daily', async (req, res) => {
  // ... same as index.js
  try {
    const days = Math.max(1, Math.min(90, parseInt(req.query.days) || 30));
    const cacheKey = `d${days}`;
    const now = Date.now();
    if (_netShareCache.key === cacheKey && (now - _netShareCache.at) < 60_000) {
      res.set('X-Cache', 'hit');
      return res.json(_netShareCache.data);
    }
    const sb = db.getClient();
    if (!sb) return res.status(500).json({ ok: false, error: 'no DB' });
    const startedAt = Date.now();
    const TZ = 'America/New_York';
    const ET_FMT = new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year:'numeric', month:'2-digit', day:'2-digit' });
    const ET_OFFSET_FMT = new Intl.DateTimeFormat('en-US', { timeZone: TZ, timeZoneName: 'shortOffset', year:'numeric' });
    function isoToEt(iso) { if (!iso) return null; const d = new Date(iso); if (isNaN(d.getTime())) return null; return ET_FMT.format(d); }
    function etDayBoundsUtc(yyyymmdd) {
      const [y, mo, d] = yyyymmdd.split('-').map(Number);
      const noonUtc = new Date(Date.UTC(y, mo - 1, d, 16, 0, 0));
      const m = ET_OFFSET_FMT.format(noonUtc).match(/GMT([+-]\d+)/);
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
    async function headCount(opts) {
      const { table, col, start, end, statusIn, mode } = opts;
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          let q = sb.from(table).select('*', { count: mode||'exact', head: true }).gte(col, start).lt(col, end);
          if (statusIn) q = q.in('status', statusIn);
          const { count, error } = await q;
          if (!error) return count || 0;
          if (attempt === 0) { await new Promise(r=>setTimeout(r,200)); continue; }
          return null;
        } catch (e) {
          if (attempt === 0) { await new Promise(r=>setTimeout(r,200)); continue; }
          return null;
        }
      }
      return null;
    }
    const countQuotes   = (s,e) => headCount({table:'parlay_orders', col:'quoted_at',    start:s, end:e, mode:'estimated'});
    const countFills    = (s,e) => headCount({table:'parlay_orders', col:'confirmed_at', start:s, end:e, mode:'estimated', statusIn:['confirmed','settled_won','settled_lost','settled_push']});
    const countDeclines = (s,e) => headCount({table:'declines',      col:'declined_at',  start:s, end:e, mode:'exact'});
    const CHUNK = 4;
    const dailyCounts = new Map();
    for (let i = 0; i < dayList.length; i += CHUNK) {
      const slice = dayList.slice(i, i + CHUNK);
      const results = await Promise.all(slice.flatMap(d => [
        countQuotes(d.startIso, d.endIso),
        countFills(d.startIso, d.endIso),
        countDeclines(d.startIso, d.endIso),
      ]));
      for (let j = 0; j < slice.length; j++) {
        dailyCounts.set(slice[j].date, { myQuotes: results[j*3] ?? 0, myFills: results[j*3+1] ?? 0, weDeclined: results[j*3+2] ?? 0 });
      }
    }
    const ourFills = new Map();
    const fillStakeByDay = new Map();
    {
      const ws = dayList[0].startIso, we = dayList[dayList.length-1].endIso;
      let offset = 0;
      while (true) {
        const { data, error } = await sb.from('parlay_orders').select('parlay_id, confirmed_at, confirmed_stake')
          .gte('confirmed_at', ws).lt('confirmed_at', we)
          .in('status', ['confirmed','settled_won','settled_lost','settled_push'])
          .order('confirmed_at', { ascending: true })
          .range(offset, offset+999);
        if (error || !data || data.length === 0) break;
        for (const r of data) {
          const d = isoToEt(r.confirmed_at);
          if (!d) continue;
          ourFills.set(r.parlay_id, { confirmed_at: r.confirmed_at, confirmed_stake: r.confirmed_stake });
          fillStakeByDay.set(d, (fillStakeByDay.get(d)||0) + (Number(r.confirmed_stake)||0));
        }
        if (data.length < 1000) break; offset += 1000;
      }
    }
    const matchedSeen = new Map();
    {
      const ws = dayList[0].startIso, we = dayList[dayList.length-1].endIso;
      let offset = 0;
      while (true) {
        const { data, error } = await sb.from('matched_parlays').select('parlay_id, matched_at, we_quoted, our_odds')
          .gte('matched_at', ws).lt('matched_at', we).order('matched_at', {ascending: true}).range(offset, offset+999);
        if (error || !data || data.length === 0) break;
        for (const r of data) {
          const prev = matchedSeen.get(r.parlay_id);
          if (!prev) matchedSeen.set(r.parlay_id, { matched_at: r.matched_at, we_quoted: !!r.we_quoted, our_odds: r.our_odds });
          else { if (r.matched_at && r.matched_at < prev.matched_at) prev.matched_at = r.matched_at; if (r.we_quoted) prev.we_quoted = true; if (prev.our_odds == null && r.our_odds != null) prev.our_odds = r.our_odds; }
        }
        if (data.length < 1000) break; offset += 1000;
      }
    }
    const quotedParlayIds = new Set();
    {
      const ids = Array.from(matchedSeen.keys());
      const BATCH = 200; const PARALLEL = 4;
      const tasks = [];
      for (let i = 0; i < ids.length; i += BATCH) tasks.push(ids.slice(i, i+BATCH));
      for (let i = 0; i < tasks.length; i += PARALLEL) {
        const wave = tasks.slice(i, i+PARALLEL);
        const results = await Promise.all(wave.map(c => sb.from('parlay_orders').select('parlay_id').in('parlay_id', c).then(r=>r, e=>({error:e}))));
        for (const r of results) { if (r.error) continue; for (const row of (r.data||[])) quotedParlayIds.add(row.parlay_id); }
      }
    }
    for (const [pid, fill] of ourFills.entries()) { if (!matchedSeen.has(pid)) matchedSeen.set(pid, { matched_at: fill.confirmed_at, we_quoted: true, our_odds: null, _synth: true }); }
    const matchedByDay = new Map();
    for (const [pid, r] of matchedSeen.entries()) {
      const d = isoToEt(r.matched_at);
      if (!d) continue;
      let b = matchedByDay.get(d); if (!b) { b = {networkMatched:0, networkWeBidOn:0}; matchedByDay.set(d, b); }
      b.networkMatched++;
      if (r.we_quoted || r.our_odds != null || quotedParlayIds.has(pid) || ourFills.has(pid)) b.networkWeBidOn++;
    }
    const byDayArr = dayList.map(d => {
      const c = dailyCounts.get(d.date) || {myQuotes:0, myFills:0, weDeclined:0};
      const m = matchedByDay.get(d.date) || {networkMatched:0, networkWeBidOn:0};
      const stake = fillStakeByDay.get(d.date) || 0;
      return {
        date: d.date, myQuotes: c.myQuotes, myFills: c.myFills, myFillStake: stake, weDeclined: c.weDeclined,
        networkDemand: c.myQuotes + c.weDeclined,
        networkMatched: m.networkMatched, networkWeBidOn: m.networkWeBidOn,
        shareOfMatched: m.networkMatched > 0 ? c.myFills / m.networkMatched : null,
        bidWinRate: m.networkWeBidOn > 0 ? Math.min(1, c.myFills / m.networkWeBidOn) : null,
      };
    });
    const totals = byDayArr.reduce((a,d) => {
      a.myQuotes+=d.myQuotes; a.myFills+=d.myFills; a.myFillStake+=d.myFillStake;
      a.weDeclined+=d.weDeclined; a.networkDemand+=d.networkDemand;
      a.networkMatched+=d.networkMatched; a.networkWeBidOn+=d.networkWeBidOn; return a;
    }, {myQuotes:0, myFills:0, myFillStake:0, weDeclined:0, networkDemand:0, networkMatched:0, networkWeBidOn:0});
    totals.shareOfMatched = totals.networkMatched > 0 ? totals.myFills / totals.networkMatched : null;
    totals.bidWinRate = totals.networkWeBidOn > 0 ? Math.min(1, totals.myFills / totals.networkWeBidOn) : null;
    const payload = { ok: true, days, windowStart: dayList[0].startIso, windowEnd: dayList[dayList.length-1].endIso, asOfUtc: new Date().toISOString(), elapsedMs: Date.now() - startedAt, byDay: byDayArr, totals };
    _netShareCache.key = cacheKey; _netShareCache.at = now; _netShareCache.data = payload;
    res.set('X-Cache', 'miss');
    res.json(payload);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

const PORT = 3099;
const server = app.listen(PORT, async () => {
  console.log(`Test server listening on :${PORT}`);
  // Self-test: hit the endpoint with 7d
  const http = require('http');
  console.log('Hitting /network-share-daily?days=7 ...');
  http.get(`http://localhost:${PORT}/network-share-daily?days=7`, res => {
    let buf = '';
    res.on('data', c => buf += c);
    res.on('end', () => {
      console.log('Status:', res.statusCode, 'X-Cache:', res.headers['x-cache']);
      try {
        const j = JSON.parse(buf);
        console.log('ok:', j.ok, 'days:', j.days, 'rows:', j.byDay?.length, 'elapsedMs:', j.elapsedMs);
        console.log('Totals:', JSON.stringify(j.totals));
        console.log('Last 3 days:');
        for (const d of (j.byDay||[]).slice(-3)) console.log(' ', d.date, JSON.stringify(d));
        // Also test cache hit
        console.log('\n--- 2nd request (cache hit) ---');
        http.get(`http://localhost:${PORT}/network-share-daily?days=7`, res2 => {
          let buf2 = '';
          res2.on('data', c => buf2 += c);
          res2.on('end', () => {
            console.log('Status:', res2.statusCode, 'X-Cache:', res2.headers['x-cache']);
            server.close();
            process.exit(0);
          });
        });
      } catch (e) {
        console.log('Failed to parse JSON:', buf.slice(0, 200));
        server.close();
        process.exit(1);
      }
    });
  }).on('error', e => { console.log('Request error:', e.message); server.close(); process.exit(1); });
});
