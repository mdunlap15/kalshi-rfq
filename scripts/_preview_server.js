// Minimal server for previewing the new Analytics card visually.
// Serves client/index.html at / and mounts the netshare endpoint stub.
// Bypass auth — this is local dev only.

require('dotenv').config();
const path = require('path');
const fs = require('fs');
const express = require('express');
const db = require('../services/db');

const app = express();

// Serve dashboard
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'client', 'index.html'));
});
app.use(express.static(path.join(__dirname, '..', 'client')));

// Mock /me — viewer with full access for preview
app.get('/me', (req, res) => res.json({ ok: true, role: 'admin', username: 'preview' }));

// Mock /status — return enough to keep the dashboard happy
app.get('/status', (req, res) => res.json({
  ok: true, running: true, paused: false, isPaused: false,
  config: {}, marketStats: {}, stats: {},
  matchedParlays: 0, ordersCount: 0,
  latency: { p50: 0, p95: 0, max: 0 },
  startTime: new Date().toISOString(),
}));

// Catch-all stub for any dashboard polling endpoint — return empty data so
// the polling storm doesn't generate 404s and suspend the browser.
// The wildcard fallback is mounted at the BOTTOM (after /network-share-daily).
const stubResponse = { ok: true, data: [], items: [], orders: [], byDay: {}, byReason: {}, byHour: {}, grandTotal: { count: 0, stake: 0 }, balance: 0, p50: 0, p95: 0, max: 0 };
const stubEndpoints = ['/orders', '/market-intel', '/exposure', '/team-exposure',
  '/series-exposure', '/game-exposure', '/risk-declines', '/today-stats',
  '/cooldown-activity', '/prop-flow', '/lines', '/lost-analysis',
  '/configured-knobs', '/portfolio', '/pitcher-exposure', '/player-exposure',
  '/balance', '/drift-status', '/health/coverage', '/px-pnl', '/px-positions',
  '/odds-events', '/health', '/limits', '/team-cooldowns', '/template-cooldowns',
  '/recent-declines', '/lost-bid-scatter', '/prop-performance', '/win-rate-heatmap'];
for (const r of stubEndpoints) {
  app.get(r, (_, res) => res.json(stubResponse));
}

// /network-share-daily — same handler as in index.js (inlined). Validated.
const _cache = { key: null, at: 0, data: null };
app.get('/network-share-daily', async (req, res) => {
  try {
    const days = Math.max(1, Math.min(90, parseInt(req.query.days) || 30));
    if (_cache.key === `d${days}` && (Date.now() - _cache.at) < 60_000) {
      res.set('X-Cache', 'hit'); return res.json(_cache.data);
    }
    const sb = db.getClient();
    if (!sb) return res.status(500).json({ ok: false, error: 'no DB' });
    const startedAt = Date.now();
    const ET_FMT = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year:'numeric', month:'2-digit', day:'2-digit' });
    const ET_OFFSET_FMT = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', timeZoneName: 'shortOffset', year:'numeric' });
    const isoToEt = iso => { if (!iso) return null; const d = new Date(iso); return isNaN(d) ? null : ET_FMT.format(d); };
    function etBounds(y) { const [yy,mm,dd] = y.split('-').map(Number); const m = ET_OFFSET_FMT.format(new Date(Date.UTC(yy,mm-1,dd,16))).match(/GMT([+-]\d+)/); const off = m ? parseInt(m[1]) : -5; const s = Date.UTC(yy,mm-1,dd) - off*3600000; return [new Date(s).toISOString(), new Date(s+86400000).toISOString()]; }
    const todayEt = isoToEt(new Date().toISOString());
    const [ty,tm,td] = todayEt.split('-').map(Number);
    const noon = Date.UTC(ty,tm-1,td,12);
    const dayList = [];
    for (let i = days-1; i >= 0; i--) { const ds = ET_FMT.format(new Date(noon - i*86400000)); const [s,e] = etBounds(ds); dayList.push({date:ds, startIso:s, endIso:e}); }
    async function hc(table, col, s, e, mode, sIn) { for (let a = 0; a < 2; a++) { try { let q = sb.from(table).select('*',{count:mode||'exact',head:true}).gte(col,s).lt(col,e); if (sIn) q = q.in('status',sIn); const r = await q; if (!r.error) return r.count||0; if (a===0) { await new Promise(rs=>setTimeout(rs,200)); continue; } return null; } catch(_) { if (a===0) { await new Promise(rs=>setTimeout(rs,200)); continue; } return null; } } return null; }
    const cq = (s,e) => hc('parlay_orders','quoted_at',s,e,'estimated');
    const cf = (s,e) => hc('parlay_orders','confirmed_at',s,e,'estimated',['confirmed','settled_won','settled_lost','settled_push']);
    const cd = (s,e) => hc('declines','declined_at',s,e,'exact');
    const dailyCounts = new Map();
    for (let i = 0; i < dayList.length; i += 4) {
      const slice = dayList.slice(i, i+4);
      const r = await Promise.all(slice.flatMap(d => [cq(d.startIso,d.endIso), cf(d.startIso,d.endIso), cd(d.startIso,d.endIso)]));
      for (let j = 0; j < slice.length; j++) dailyCounts.set(slice[j].date, { myQuotes: r[j*3]??0, myFills: r[j*3+1]??0, weDeclined: r[j*3+2]??0 });
    }
    const ourFills = new Map(); const fillStakeByDay = new Map();
    { const ws=dayList[0].startIso, we=dayList[dayList.length-1].endIso; let off=0;
      while (true) { const {data,error} = await sb.from('parlay_orders').select('parlay_id, confirmed_at, confirmed_stake').gte('confirmed_at',ws).lt('confirmed_at',we).in('status',['confirmed','settled_won','settled_lost','settled_push']).order('confirmed_at',{ascending:true}).range(off,off+999); if (error||!data||!data.length) break; for (const r of data) { const d = isoToEt(r.confirmed_at); if (!d) continue; ourFills.set(r.parlay_id, {confirmed_at:r.confirmed_at, confirmed_stake:r.confirmed_stake}); fillStakeByDay.set(d,(fillStakeByDay.get(d)||0)+(Number(r.confirmed_stake)||0)); } if (data.length<1000) break; off+=1000; } }
    const matchedSeen = new Map();
    { const ws=dayList[0].startIso, we=dayList[dayList.length-1].endIso; let off=0;
      while (true) { const {data,error} = await sb.from('matched_parlays').select('parlay_id, matched_at, we_quoted, our_odds').gte('matched_at',ws).lt('matched_at',we).order('matched_at',{ascending:true}).range(off,off+999); if (error||!data||!data.length) break; for (const r of data) { const p = matchedSeen.get(r.parlay_id); if (!p) matchedSeen.set(r.parlay_id, {matched_at:r.matched_at, we_quoted:!!r.we_quoted, our_odds:r.our_odds}); else { if (r.matched_at && r.matched_at < p.matched_at) p.matched_at = r.matched_at; if (r.we_quoted) p.we_quoted = true; if (p.our_odds == null && r.our_odds != null) p.our_odds = r.our_odds; } } if (data.length<1000) break; off+=1000; } }
    const quotedParlayIds = new Set();
    { const ids = Array.from(matchedSeen.keys()); const tasks = []; for (let i=0;i<ids.length;i+=200) tasks.push(ids.slice(i,i+200));
      for (let i=0;i<tasks.length;i+=4) { const wv = tasks.slice(i,i+4); const r = await Promise.all(wv.map(c=>sb.from('parlay_orders').select('parlay_id').in('parlay_id',c).then(r=>r,e=>({error:e})))); for (const x of r) { if (x.error) continue; for (const row of (x.data||[])) quotedParlayIds.add(row.parlay_id); } } }
    for (const [pid,f] of ourFills) if (!matchedSeen.has(pid)) matchedSeen.set(pid, {matched_at:f.confirmed_at, we_quoted:true, our_odds:null});
    const mbd = new Map();
    for (const [pid,r] of matchedSeen) { const d = isoToEt(r.matched_at); if (!d) continue; let b = mbd.get(d); if (!b) { b = {networkMatched:0, networkWeBidOn:0}; mbd.set(d,b); } b.networkMatched++; if (r.we_quoted || r.our_odds != null || quotedParlayIds.has(pid) || ourFills.has(pid)) b.networkWeBidOn++; }
    const BID_WIN_RATE_RELIABLE_FROM = '2026-05-12';
    const byDay = dayList.map(d => {
      const c = dailyCounts.get(d.date)||{myQuotes:0,myFills:0,weDeclined:0};
      const m = mbd.get(d.date)||{networkMatched:0,networkWeBidOn:0};
      const st = fillStakeByDay.get(d.date)||0;
      const reliable = d.date >= BID_WIN_RATE_RELIABLE_FROM;
      return {
        date:d.date, myQuotes:c.myQuotes, myFills:c.myFills, myFillStake:st, weDeclined:c.weDeclined,
        networkDemand:c.myQuotes+c.weDeclined,
        networkMatched:m.networkMatched, networkWeBidOn:m.networkWeBidOn,
        shareOfMatched: m.networkMatched > 0 ? c.myFills/m.networkMatched : null,
        bidWinRate: (m.networkWeBidOn > 0 && reliable) ? Math.min(1, c.myFills/m.networkWeBidOn) : null,
        bidWinRateReliable: reliable,
      };
    });
    const tot = byDay.reduce((a,d) => { a.myQuotes+=d.myQuotes; a.myFills+=d.myFills; a.myFillStake+=d.myFillStake; a.weDeclined+=d.weDeclined; a.networkDemand+=d.networkDemand; a.networkMatched+=d.networkMatched; a.networkWeBidOn+=d.networkWeBidOn; return a; }, {myQuotes:0,myFills:0,myFillStake:0,weDeclined:0,networkDemand:0,networkMatched:0,networkWeBidOn:0});
    tot.shareOfMatched = tot.networkMatched > 0 ? tot.myFills/tot.networkMatched : null;
    tot.bidWinRate = tot.networkWeBidOn > 0 ? Math.min(1, tot.myFills/tot.networkWeBidOn) : null;
    const reliableTotals = byDay.filter(d => d.bidWinRateReliable).reduce((a,d) => { a.myFills += d.myFills; a.networkWeBidOn += d.networkWeBidOn; return a; }, { myFills:0, networkWeBidOn:0 });
    reliableTotals.bidWinRate = reliableTotals.networkWeBidOn > 0 ? Math.min(1, reliableTotals.myFills / reliableTotals.networkWeBidOn) : null;
    reliableTotals.days = byDay.filter(d => d.bidWinRateReliable).length;
    const payload = { ok:true, days, windowStart:dayList[0].startIso, windowEnd:dayList[dayList.length-1].endIso, bidWinRateReliableFrom: BID_WIN_RATE_RELIABLE_FROM, asOfUtc:new Date().toISOString(), elapsedMs:Date.now()-startedAt, byDay, totals:tot, reliableTotals };
    _cache.key = `d${days}`; _cache.at = Date.now(); _cache.data = payload;
    res.set('X-Cache', 'miss'); res.json(payload);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ---- /network-share-hourly stub for the preview server ----
const _hourlyCache = { key: null, at: 0, data: null };
app.get('/network-share-hourly', async (req, res) => {
  try {
    const hours = Math.max(1, Math.min(48, parseInt(req.query.hours) || 24));
    const rolling = Math.max(2, Math.min(12, parseInt(req.query.rolling) || 6));
    const cacheKey = `h${hours}_r${rolling}`;
    const now = Date.now();
    if (_hourlyCache.key === cacheKey && (now - _hourlyCache.at) < 30000) {
      res.set('X-Cache', 'hit'); return res.json(_hourlyCache.data);
    }
    const sb = db.getClient();
    if (!sb) return res.status(500).json({ ok: false, error: 'no DB' });
    const endMs = Math.floor(now / 3_600_000) * 3_600_000;
    const startMs = endMs - hours * 3_600_000;
    const startIso = new Date(startMs).toISOString();
    const endIso = new Date(endMs).toISOString();
    const ET_FMT = new Intl.DateTimeFormat('en-US', { timeZone:'America/New_York', hour:'2-digit', minute:'2-digit', month:'numeric', day:'numeric', hour12: false });
    const buckets = [];
    for (let i = 0; i < hours; i++) { const ms = startMs + i * 3600000; buckets.push({ hourMs: ms, hourIso: new Date(ms).toISOString(), etLabel: ET_FMT.format(new Date(ms)), myQuotes:0, myFills:0, weDeclined:0, networkMatched:0, networkWeBidOn:0, bySport: {} }); }
    const bucketForMs = (ms) => { if (ms < startMs || ms >= endMs) return null; return buckets[Math.floor((ms - startMs) / 3600000)]; };
    const sportOf = (legs) => { if (!Array.isArray(legs) || !legs.length) return 'unknown'; const seen = new Set(); for (const l of legs) if (l && l.sport) seen.add(l.sport); if (seen.size === 0) return 'unknown'; if (seen.size === 1) return [...seen][0]; return 'multi'; };
    const bumpSport = (b, sp, field) => { if (!b.bySport[sp]) b.bySport[sp] = { networkMatched: 0, networkWeBidOn: 0, myFills: 0 }; b.bySport[sp][field]++; };
    const ourOrderIds = new Set();
    const ourFills = new Map();
    async function pull(col, handler) {
      let offset = 0;
      while (true) {
        const { data, error } = await sb.from('parlay_orders').select('parlay_id, status, quoted_at, confirmed_at, meta').gte(col, startIso).lt(col, endIso).order(col, { ascending: true }).range(offset, offset+999);
        if (error || !data || !data.length) break;
        for (const r of data) handler(r);
        if (data.length < 1000) break; offset += 1000;
      }
    }
    await pull('quoted_at', (r) => { if (r.meta?.phantom) return; if (r.meta?.reconstructed && !r.quoted_at) return; ourOrderIds.add(r.parlay_id); if (r.quoted_at) { const b = bucketForMs(new Date(r.quoted_at).getTime()); if (b) b.myQuotes++; } });
    await pull('confirmed_at', (r) => { if (r.meta?.phantom) return; ourOrderIds.add(r.parlay_id); const isWon = r.status === 'confirmed' || (typeof r.status === 'string' && r.status.startsWith('settled_')); if (isWon && r.confirmed_at) { const b = bucketForMs(new Date(r.confirmed_at).getTime()); if (b) { b.myFills++; ourFills.set(r.parlay_id, true); } } });
    {
      let offset = 0;
      while (true) {
        const { data, error } = await sb.from('declines').select('declined_at').gte('declined_at', startIso).lt('declined_at', endIso).order('declined_at', { ascending: true }).range(offset, offset+999);
        if (error || !data || !data.length) break;
        for (const r of data) { const b = bucketForMs(new Date(r.declined_at).getTime()); if (b) b.weDeclined++; }
        if (data.length < 1000) break; offset += 1000;
      }
    }
    const seen = new Map();
    {
      let offset = 0;
      while (true) {
        const { data, error } = await sb.from('matched_parlays').select('parlay_id, matched_at, we_quoted, our_odds, legs').gte('matched_at', startIso).lt('matched_at', endIso).order('matched_at', { ascending: true }).range(offset, offset+999);
        if (error || !data || !data.length) break;
        for (const r of data) {
          const p = seen.get(r.parlay_id);
          if (!p) seen.set(r.parlay_id, { matched_at: r.matched_at, we_quoted: !!r.we_quoted, our_odds: r.our_odds, legs: r.legs });
          else { if (r.we_quoted) p.we_quoted = true; if (p.our_odds == null && r.our_odds != null) p.our_odds = r.our_odds; if (!p.legs && r.legs) p.legs = r.legs; }
        }
        if (data.length < 1000) break; offset += 1000;
      }
      for (const [pid, r] of seen.entries()) {
        const b = bucketForMs(new Date(r.matched_at).getTime());
        if (!b) continue;
        const sp = sportOf(r.legs);
        const weBidOn = r.we_quoted || r.our_odds != null || ourOrderIds.has(pid);
        const weWon = ourFills.has(pid);
        b.networkMatched++;
        bumpSport(b, sp, 'networkMatched');
        if (weBidOn) { b.networkWeBidOn++; bumpSport(b, sp, 'networkWeBidOn'); }
        if (weWon) bumpSport(b, sp, 'myFills');
      }
    }
    const BID_RELIABLE_FROM = '2026-05-12';
    for (const b of buckets) {
      b.networkDemand = b.myQuotes + b.weDeclined;
      b.shareOfMatched = b.networkMatched > 0 ? b.myFills / b.networkMatched : null;
      const dayKey = new Intl.DateTimeFormat('en-CA', { timeZone:'America/New_York', year:'numeric', month:'2-digit', day:'2-digit' }).format(new Date(b.hourMs));
      b.bidWinRateReliable = dayKey >= BID_RELIABLE_FROM;
      b.bidWinRate = (b.networkWeBidOn > 0 && b.bidWinRateReliable) ? Math.min(1, b.myFills / b.networkWeBidOn) : null;
    }
    function rrRatio(numA, denA, w) {
      const o = new Array(numA.length).fill(null);
      for (let i = 0; i < numA.length; i++) {
        let n = 0, d = 0;
        for (let j = Math.max(0, i - w + 1); j <= i; j++) { n += numA[j] || 0; d += denA[j] || 0; }
        o[i] = d > 0 ? n / d : null;
      }
      return o;
    }
    const rs = rrRatio(buckets.map(b => b.myFills), buckets.map(b => b.networkMatched), rolling);
    const rb = rrRatio(buckets.map(b => b.bidWinRateReliable ? b.myFills : 0), buckets.map(b => b.bidWinRateReliable ? b.networkWeBidOn : 0), rolling);
    for (let i = 0; i < buckets.length; i++) { buckets[i].rollingShare = rs[i]; buckets[i].rollingBidWin = rb[i] != null ? Math.min(1, rb[i]) : null; }
    const tot = buckets.reduce((a, b) => { a.myQuotes+=b.myQuotes; a.myFills+=b.myFills; a.weDeclined+=b.weDeclined; a.networkDemand+=b.networkDemand; a.networkMatched+=b.networkMatched; a.networkWeBidOn+=b.networkWeBidOn; return a; }, {myQuotes:0, myFills:0, weDeclined:0, networkDemand:0, networkMatched:0, networkWeBidOn:0});
    tot.shareOfMatched = tot.networkMatched > 0 ? tot.myFills / tot.networkMatched : null;
    tot.bidWinRate = tot.networkWeBidOn > 0 ? Math.min(1, tot.myFills / tot.networkWeBidOn) : null;
    const headlineFor = (n) => { const s = buckets.slice(Math.max(0, buckets.length - n)).reduce((a,b) => { a.myFills+=b.myFills; a.networkMatched+=b.networkMatched; a.networkWeBidOn+=b.networkWeBidOn; a.myQuotes+=b.myQuotes; a.weDeclined+=b.weDeclined; return a; }, {myFills:0, networkMatched:0, networkWeBidOn:0, myQuotes:0, weDeclined:0}); s.shareOfMatched = s.networkMatched > 0 ? s.myFills / s.networkMatched : null; s.bidWinRate = s.networkWeBidOn > 0 ? Math.min(1, s.myFills / s.networkWeBidOn) : null; s.hours = Math.min(n, buckets.length); return s; };
    const sportTotals = new Map();
    for (const b of buckets) for (const [sp, s] of Object.entries(b.bySport)) sportTotals.set(sp, (sportTotals.get(sp) || 0) + (s.networkMatched || 0));
    const sports = [...sportTotals.entries()].sort((a, b) => b[1] - a[1]).map(([sp, n]) => ({ key: sp, networkMatched: n }));
    const payload = { ok:true, hours, rollingWindow: rolling, startIso, endIso, bidWinRateReliableFrom: BID_RELIABLE_FROM, asOfUtc: new Date().toISOString(), elapsedMs: 0, buckets, totals: tot, headline: { last1h: headlineFor(1), last6h: headlineFor(6), last24h: headlineFor(Math.min(24, hours)), full: { ...tot, hours } }, sports };
    _hourlyCache.key = cacheKey; _hourlyCache.at = now; _hourlyCache.data = payload;
    res.set('X-Cache', 'miss'); res.json(payload);
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Wildcard fallback — mounted LAST so all specific routes (especially
// /network-share-daily and /network-share-hourly) take precedence.
app.get(/^\/[a-z0-9_/-]+$/i, (_, res) => res.json(stubResponse));

const PORT = 4099;
app.listen(PORT, () => console.log(`Preview server on http://localhost:${PORT}`));
