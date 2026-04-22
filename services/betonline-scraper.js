/**
 * BetOnline scraper for Zurich Classic team matchups.
 *
 * Context: DataGolf publishes 1v1 player matchups for regular tour
 * events but NOT team pairs. DK publishes team matchups but their
 * pairings don't overlap PX's pairings. BetOnline's matchup listings
 * mirror PX's pairings (operator-confirmed) — this is the one week
 * per year where BetOnline is our best source.
 *
 * Scope: narrow and temporary. Zurich Classic only. Deleted or
 * repurposed for future PGA team events (Ryder Cup, Presidents Cup)
 * when they show up on PX.
 *
 * URL layout on BetOnline:
 *   tournament-length matchups:
 *     /sportsbook/golf/fed-ex-events/zurich-classic-of-new-orleans
 *   Round 1 matchups:
 *     /sportsbook/golf/fed-ex-round-1/zurich-classic-of-new-orleans
 *
 * DOM pattern per matchup (visible on BetOnline page):
 *   ID-prefixed team cell:  "7017 - Bauchou/Stevens"
 *   Odds cell:              "-110" | "+105" | etc.
 *   Two such pairs per matchup row.
 *
 * We pull both URLs in a single Puppeteer session, walk the rendered
 * DOM for text matching "<4 digits> - Name/Name" + adjacent American
 * odds, and pair them sequentially.
 */
const log = require('./logger');
const { config } = require('../config');

const CACHE_TTL_MS = 15 * 60 * 1000;
const NAV_TIMEOUT_MS = 60000;
const POST_NAV_WAIT_MS = 10000;

let _puppeteer = null;
function puppeteer() {
  if (!_puppeteer) _puppeteer = require('puppeteer');
  return _puppeteer;
}

const cache = {};
const inFlight = {};

const BETONLINE_URLS = [
  {
    scope: 'tournament',
    url: 'https://www.betonline.ag/sportsbook/golf/fed-ex-events/zurich-classic-of-new-orleans',
  },
  {
    scope: 'round_1',
    url: 'https://www.betonline.ag/sportsbook/golf/fed-ex-round-1/zurich-classic-of-new-orleans',
  },
];

async function fetchZurichMatchups({ force = false } = {}) {
  const key = 'zurich';
  if (!force && cache[key] && Date.now() - cache[key].at < CACHE_TTL_MS) {
    return cache[key].data;
  }
  if (inFlight[key]) return inFlight[key];

  inFlight[key] = (async () => {
    const startedAt = Date.now();
    const browser = await puppeteer().launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    try {
      const page = await browser.newPage();
      await page.setViewport({ width: 1400, height: 900 });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      // Diagnostic: collect every JSON XHR — no domain filter, because
      // BetOnline uses Kambi (or similar third-party provider) as its
      // sportsbook widget. Provider XHRs are on OTHER domains (kambi,
      // sas, bv-linesfeed, etc.). We want to see them all so we can
      // spot matchup-shaped payloads for targeted parsing later.
      const xhrDiag = [];
      page.on('response', async (resp) => {
        try {
          const url = resp.url();
          const ct = resp.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const data = await resp.json();
          // Only keep non-trivial responses (not manifests, tiny configs).
          const jsonSize = JSON.stringify(data).length;
          if (jsonSize < 500) return;
          xhrDiag.push({
            url: url.slice(0, 240),
            topKeys: data && typeof data === 'object' ? Object.keys(data).slice(0, 12) : [],
            sampleSize: jsonSize,
          });
        } catch { /* ignore */ }
      });

      const allMatchups = [];
      const perScopeDiag = {};
      for (const { scope, url } of BETONLINE_URLS) {
        try {
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS });
          // Wait significantly longer — BetOnline's sportsbook widget is a
          // React SPA that hydrates after the initial HTML, and inside a
          // web component / iframe. 10s was not enough. 25s is over-kill
          // but this scraper runs every 10 min in the background, not on
          // any hot path, so cost is irrelevant.
          await new Promise(r => setTimeout(r, 25000));
          // Walk the main frame AND every iframe — Kambi-style widgets
          // typically mount in an iframe whose DOM our main-frame walker
          // can't reach.
          const res = await parseAllFrames(page, scope);
          allMatchups.push(...res.matchups);
          perScopeDiag[scope] = {
            matchups: res.matchups.length,
            teamCellsFound: res.teamCellsFound,
            oddsCellsFound: res.oddsCellsFound,
            sampleText: res.sampleText,
            frameCount: res.frameCount,
            // Record landing URL + title so we can detect auth redirects
            // or challenge pages that silently replace the target page.
            landingUrl: page.url(),
            title: await page.title(),
          };
        } catch (err) {
          log.warn('BetOnlineScraper', `${scope} nav failed: ${err.message}`);
          perScopeDiag[scope] = { error: err.message };
        }
      }

      const payload = {
        fetchedAt: new Date().toISOString(),
        source: 'betonline',
        matchups: allMatchups,
        scrapeMs: Date.now() - startedAt,
        // Diagnostics only when parsing came up empty — helps pinpoint
        // whether the issue is anti-bot block, selector change, or
        // BetOnline genuinely hasn't posted the lines yet.
        diagnostics: allMatchups.length === 0 ? {
          perScope: perScopeDiag,
          xhrCount: xhrDiag.length,
          sampleXhrs: xhrDiag.slice(0, 8),
        } : undefined,
      };
      // Only overwrite cache when the scrape ACTUALLY found data. An
      // empty result shouldn't clobber a previously-persisted manual
      // upload — scraper is blocked most of the time, manual upload
      // is the authoritative source for the Zurich week.
      if (allMatchups.length > 0) {
        cache[key] = { at: Date.now(), data: payload };
        log.info('BetOnlineScraper', `Zurich matchups: ${allMatchups.length} captured in ${payload.scrapeMs}ms — cache updated`);
      } else {
        log.info('BetOnlineScraper', `Zurich scrape returned 0 matchups in ${payload.scrapeMs}ms — keeping prior cache (${(cache[key]?.data?.matchups?.length) || 0} matchups, source=${cache[key]?.data?.source || 'none'})`);
      }
      return payload;
    } finally {
      await browser.close();
      delete inFlight[key];
    }
  })().catch(err => { delete inFlight[key]; throw err; });
  return inFlight[key];
}

/**
 * Walk the main frame + every iframe and run parseScope on each.
 * Kambi-style sportsbook widgets (BetOnline, Bet365, etc.) mount
 * inside an iframe whose DOM is invisible to a main-frame walker.
 */
async function parseAllFrames(page, scope) {
  const frames = page.frames();
  let aggMatchups = [];
  let aggTeamCells = 0, aggOddsCells = 0;
  let aggSample = [];
  for (const frame of frames) {
    try {
      const res = await parseScope(frame, scope);
      aggMatchups = aggMatchups.concat(res.matchups);
      aggTeamCells += res.teamCellsFound;
      aggOddsCells += res.oddsCellsFound;
      if (aggSample.length < 5) {
        aggSample = aggSample.concat(res.sampleText.slice(0, 5 - aggSample.length));
      }
    } catch (_) {
      // Cross-origin iframes (Google tag manager, ad networks, etc.)
      // throw on page.evaluate; swallow and continue.
    }
  }
  return {
    matchups: aggMatchups,
    teamCellsFound: aggTeamCells,
    oddsCellsFound: aggOddsCells,
    sampleText: aggSample,
    frameCount: frames.length,
  };
}

/**
 * Scan the current frame DOM for matchup data. BetOnline renders team
 * names as "<4-digit id> - <Lastname>/<Lastname>" and odds as
 * standalone "+NNN" / "-NNN" text. We find all matches in DOM order
 * and pair them sequentially (team, odds, team, odds → one matchup).
 *
 * Returns { matchups, teamCellsFound, oddsCellsFound, sampleText }.
 * The diag counts + sample help debug parse failures without needing
 * to re-run Puppeteer manually.
 */
function parseScope(page, scope) {
  return page.evaluate((scopeArg) => {
    // Pattern: "7017 - Bauchou/Stevens" (allow en-dash or em-dash,
    // tolerate whitespace). Captures the team-pair part only.
    const teamRe = /^\s*\d{3,5}\s*[-\u2013\u2014]\s*([A-Za-zÀ-ÿ'.\- ]+\/[A-Za-zÀ-ÿ'.\- ]+)\s*$/;
    // American odds standalone: "+105", "-125", "+2400", etc.
    const oddsRe = /^\s*([+-]\d{3,5})\s*$/;

    const teams = [];
    const odds = [];

    // Walk a shallow subtree — BetOnline nests team/odds a few levels
    // deep but we just care about the leaf text nodes. Use a
    // TreeWalker on TEXT_NODE for speed + completeness.
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
    let i = 0;
    while (walker.nextNode()) {
      const t = (walker.currentNode.nodeValue || '').trim();
      if (!t) continue;
      const tm = teamRe.exec(t);
      if (tm) {
        teams.push({ text: tm[1].trim(), domOrder: i });
      } else {
        const om = oddsRe.exec(t);
        if (om) odds.push({ text: om[1].trim(), domOrder: i });
      }
      i++;
    }

    // Pair team + odds in DOM order. Expected layout per matchup:
    //   team1, odds1, team2, odds2
    // We walk sequentially by position in the combined sorted list.
    const combined = [
      ...teams.map(t => ({ kind: 'team', ...t })),
      ...odds.map(o => ({ kind: 'odds', ...o })),
    ].sort((a, b) => a.domOrder - b.domOrder);

    const matchups = [];
    let buffer = [];
    for (const item of combined) {
      // Expected cycle: team odds team odds
      const expectedKind = ['team', 'odds', 'team', 'odds'][buffer.length];
      if (item.kind !== expectedKind) {
        // Cycle got desynced — reset buffer. Happens if page has extra
        // odds cells (e.g. cashout pricing, parlay boost) mixed in.
        if (item.kind === 'team') buffer = [item];
        else buffer = [];
        continue;
      }
      buffer.push(item);
      if (buffer.length === 4) {
        matchups.push({
          scope: scopeArg,
          teams: [
            { team: buffer[0].text, odds: parseInt(buffer[1].text, 10) },
            { team: buffer[2].text, odds: parseInt(buffer[3].text, 10) },
          ],
        });
        buffer = [];
      }
    }

    return {
      matchups,
      teamCellsFound: teams.length,
      oddsCellsFound: odds.length,
      sampleText: teams.slice(0, 3).map(t => t.text),
    };
  }, scope);
}

/**
 * De-vig a 2-way pair and set fair probs on both teams. Returns the
 * mutated matchup with fairProb on each team entry. Dropped if odds
 * are missing or invalid.
 */
function devigMatchup(m) {
  const [a, b] = m.teams;
  if (!Number.isFinite(a?.odds) || !Number.isFinite(b?.odds)) return null;
  const impA = a.odds >= 0 ? 100 / (a.odds + 100) : -a.odds / (-a.odds + 100);
  const impB = b.odds >= 0 ? 100 / (b.odds + 100) : -b.odds / (-b.odds + 100);
  const sum = impA + impB;
  if (sum <= 0) return null;
  // Proportional de-vig with the same favMaxShare cap as other scrapers.
  const favMaxShare = (config.pricing && config.pricing.devigFavMaxShare != null)
    ? config.pricing.devigFavMaxShare : 0.5;
  const [fav, dog] = impA >= impB ? [a, b] : [b, a];
  const favImp = Math.max(impA, impB);
  const overround = sum - 1;
  const favShare = Math.min(favImp / sum, favMaxShare);
  fav.impliedProb = fav.odds >= 0 ? 100 / (fav.odds + 100) : -fav.odds / (-fav.odds + 100);
  dog.impliedProb = dog.odds >= 0 ? 100 / (dog.odds + 100) : -dog.odds / (-dog.odds + 100);
  fav.fairProb = fav.impliedProb - favShare * overround;
  dog.fairProb = dog.impliedProb - (1 - favShare) * overround;
  m.vig = Math.round(overround * 10000) / 10000;
  return m;
}

/**
 * Normalize a team-pair name into a sort-invariant canonical form.
 * Works for last-name-only ("Bauchou/Stevens") and full-name
 * ("Hayden Bauchou / Sam Stevens") formats since normalization
 * splits on / and sorts alphabetically.
 */
function normalizePairName(name) {
  if (!name) return '';
  return String(name)
    .replace(/&/g, '/')
    .split(/\s*[\/,]\s*/)
    .map(p => p.trim().toLowerCase()
      .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
      .replace(/[.,']/g, '')
      // Hyphens (and en/em dashes) → spaces so "Neergaard-Petersen"
      // and "Neergaard Petersen" normalize to the same form. Different
      // data sources use different conventions — BetOnline/Bookmaker
      // typed "Neergaard Petersen" while PX returns "Neergaard-Petersen".
      .replace(/[-\u2013\u2014]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim())
    .filter(Boolean)
    .sort()
    .join('|');
}

/**
 * Match a candidate vs target with exact normalized match OR
 * last-name-only fallback. BetOnline uses last names only; PX may
 * send full names. Last-name fallback bridges that gap.
 */
function pairNameMatches(candidate, target) {
  if (!candidate || !target) return false;
  if (candidate === target) return true;
  const lastOnly = (s) => s.split('|').map(p => p.split(' ').pop()).sort().join('|');
  return lastOnly(candidate) === lastOnly(target);
}

/**
 * Look up a Zurich matchup fair prob. Matches both tournament-length
 * (roundNum=null) and per-round (roundNum=1..4) markets. Returns
 * { fairProb, americanOdds, source, scope } or null.
 */
function lookupZurichMatchupFairProb(teamName, roundNum) {
  const c = cache.zurich;
  if (!c || !c.data) return null;
  // De-vig every matchup lazily on first lookup (parser intentionally
  // separates DOM-extract from fair-prob math so parse bugs don't
  // corrupt cached fair values).
  for (const m of (c.data.matchups || [])) {
    if (m.vig == null) devigMatchup(m);
  }
  const target = normalizePairName(teamName);
  if (!target) return null;
  const wantScope = roundNum == null ? 'tournament' : `round_${roundNum}`;
  for (const m of (c.data.matchups || [])) {
    if (m.scope !== wantScope) continue;
    for (const t of m.teams) {
      const cand = normalizePairName(t.team);
      if (pairNameMatches(cand, target) && t.fairProb != null) {
        return {
          fairProb: t.fairProb,
          americanOdds: t.odds,
          source: 'betonline',
          scope: m.scope,
        };
      }
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// MANUAL UPLOAD PATH
//
// BetOnline blocks our Puppeteer session (data-center IP, unauthenticated).
// Operator supplied odds by hand as a one-time manual upload — prices
// move little between Tuesday/Wednesday and Thursday tee-off for a
// team event like Zurich Classic, so a static snapshot is good enough.
//
// Accepts two input shapes:
//   { scope: 'tournament'|'round_1', text: "<raw paste from BetOnline>" }
//     Parsed with a regex — flexible, tolerates the copy-paste text
//     layout BetOnline produces (id - Name/Name + odds nearby).
//   { scope: 'tournament'|'round_1', matchups: [{teamA,oddsA,teamB,oddsB},...] }
//     Direct JSON — for scripted bulk upload.
//
// Upload REPLACES any existing matchups with the same scope; matchups
// in other scopes stay. Cache flagged source='betonline-manual' so we
// can tell manually-uploaded data apart from scraped data in logs.
// ---------------------------------------------------------------------------
function loadManualMatchups(body) {
  const scope = (body && body.scope) || 'tournament';
  if (scope !== 'tournament' && !/^round_\d+$/.test(scope)) {
    throw new Error(`Invalid scope "${scope}" — use "tournament" or "round_N"`);
  }
  let newMatchups;
  if (body && typeof body.text === 'string' && body.text.trim()) {
    newMatchups = parseManualText(body.text, scope);
    if (newMatchups.length === 0) {
      throw new Error('Text parse yielded 0 matchups — check format (expected "<id> - Name/Name" with American odds nearby)');
    }
  } else if (body && Array.isArray(body.matchups)) {
    newMatchups = body.matchups.map(m => ({
      scope,
      teams: [
        { team: String(m.teamA || m.team_a || '').trim(), odds: Number(m.oddsA ?? m.odds_a) },
        { team: String(m.teamB || m.team_b || '').trim(), odds: Number(m.oddsB ?? m.odds_b) },
      ],
    })).filter(m => m.teams[0].team && m.teams[1].team && Number.isFinite(m.teams[0].odds) && Number.isFinite(m.teams[1].odds));
    if (newMatchups.length === 0) throw new Error('No valid matchups in array — require teamA, teamB, oddsA, oddsB on each entry');
  } else {
    throw new Error('Payload must include either `text` (raw paste) or `matchups` (array)');
  }

  // Merge: keep matchups from OTHER scopes, replace the uploaded scope.
  const existing = (cache.zurich && cache.zurich.data && cache.zurich.data.matchups) || [];
  const kept = existing.filter(m => m.scope !== scope);
  const merged = [...kept, ...newMatchups];

  cache.zurich = {
    at: Date.now(),
    data: {
      fetchedAt: new Date().toISOString(),
      source: 'betonline-manual',
      matchups: merged,
      scrapeMs: 0,
    },
  };
  // Persist to Supabase KV so the manual upload survives Railway
  // redeploys. In-memory cache is ephemeral; without this, every
  // code push forces a re-upload. Fire-and-forget — if Supabase is
  // down we still serve the in-memory copy for this session.
  try {
    const db = require('./db');
    db.saveKV('betonline_zurich', cache.zurich.data).catch(e => {
      log.warn('BetOnlineScraper', `KV persist failed: ${e.message}`);
    });
  } catch (e) { /* db module load failed — skip */ }
  // Invalidate fair probs so the lookup code recomputes (devigMatchup
  // runs lazily; by not setting .vig on the new matchups it'll fire
  // on first lookup with the fresh odds).
  log.info('BetOnlineScraper', `Manual upload: ${newMatchups.length} ${scope} matchups loaded (total cache: ${merged.length})`);
  return {
    loadedScope: scope,
    loadedCount: newMatchups.length,
    totalInCache: merged.length,
    sampleLoaded: newMatchups.slice(0, 3).map(m => ({
      scope: m.scope,
      teamA: m.teams[0].team, oddsA: m.teams[0].odds,
      teamB: m.teams[1].team, oddsB: m.teams[1].odds,
    })),
  };
}

/**
 * On startup, restore previously-uploaded matchups from Supabase KV.
 * Called once during boot so the in-memory cache is warm before the
 * first RFQ arrives. No-ops cleanly if Supabase isn't configured or
 * there's no prior upload.
 */
async function restoreFromPersistence() {
  try {
    const db = require('./db');
    const stored = await db.loadKV('betonline_zurich');
    if (!stored || !Array.isArray(stored.matchups) || stored.matchups.length === 0) {
      log.info('BetOnlineScraper', 'No persisted Zurich matchups to restore');
      return;
    }
    cache.zurich = { at: Date.now(), data: stored };
    log.info('BetOnlineScraper', `Restored ${stored.matchups.length} Zurich matchups from Supabase KV (originally uploaded ${stored.fetchedAt})`);
  } catch (err) {
    log.warn('BetOnlineScraper', `Restore from persistence failed: ${err.message}`);
  }
}

/**
 * Regex-parse a block of text copy-pasted from BetOnline's matchups
 * page. Looks for "<3-5 digit id> - <Name>/<Name>" rows followed by
 * an American odds value on the same OR the next few lines, and
 * pairs them sequentially.
 */
function parseManualText(text, scope) {
  const lines = String(text).split(/\r?\n/);
  // Team row pattern — tolerates en/em dash and accented chars.
  const teamRe = /(\d{3,5})\s*[-\u2013\u2014]\s*([A-Za-zÀ-ÿ'.\- ]+\/[A-Za-zÀ-ÿ'.\- ]+)/;
  // American odds standalone anywhere on the line.
  const oddsRe = /([+-]\d{3,5})/;

  const entries = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const tm = teamRe.exec(line);
    if (!tm) continue;
    const team = tm[2].trim();
    // Try to find the odds on the SAME line first (BetOnline's text
    // export typically has odds on the same row); fall back to the
    // next 3 lines if not found.
    let odds = null;
    const sameLine = oddsRe.exec(line.slice(line.indexOf(team) + team.length));
    if (sameLine) {
      odds = parseInt(sameLine[1], 10);
    } else {
      for (let j = i + 1; j <= Math.min(i + 3, lines.length - 1); j++) {
        const nxt = oddsRe.exec(lines[j]);
        if (nxt) { odds = parseInt(nxt[1], 10); break; }
      }
    }
    if (team && Number.isFinite(odds)) entries.push({ team, odds });
  }

  // Pair sequentially: entries[0]+entries[1] = matchup, entries[2]+entries[3] = matchup, etc.
  const results = [];
  for (let i = 0; i + 1 < entries.length; i += 2) {
    results.push({ scope, teams: [entries[i], entries[i + 1]] });
  }
  return results;
}

module.exports = {
  fetchZurichMatchups,
  lookupZurichMatchupFairProb,
  loadManualMatchups,
  restoreFromPersistence,
  parseManualText,
  normalizePairName,
};
