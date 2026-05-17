// ESPN scoreboard scraper. Single source for live game scores + status
// across every sport we quote on. Replaces TOA's /sports/{key}/scores/
// as the primary completion-detection feed because TOA's MMA prelim
// coverage is hours-late and tennis/some soccer leagues are missing.
//
// Endpoint: https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard
// Free, public, no key required. Updates within ~30s of real-time.
// Generally tolerated at modest cadence; we poll at 60s/sport.
//
// Exposes a SYNC getter that order-tracker.checkLegResults reads
// against the cache — never blocks the leg-resolution loop on a
// network call. The poller fills the cache in the background.

const fetch = require('node-fetch');
const log = require('./logger');

// Map our internal sport keys to ESPN's `(sport, league)` URL components.
// Some keys (tennis, MMA non-UFC promotions) span multiple ESPN league
// paths; we list each as a separate entry so the poller hits all of them
// and the resulting cache contains every possible match.
const ESPN_LEAGUES = {
  basketball_nba:           [{ sport: 'basketball', league: 'nba' }],
  basketball_ncaab:         [{ sport: 'basketball', league: 'mens-college-basketball' }],
  basketball_wnba:          [{ sport: 'basketball', league: 'wnba' }],
  baseball_mlb:             [{ sport: 'baseball',   league: 'mlb' }],
  icehockey_nhl:            [{ sport: 'hockey',     league: 'nhl' }],
  americanfootball_nfl:     [{ sport: 'football',   league: 'nfl' }],
  americanfootball_ncaaf:   [{ sport: 'football',   league: 'college-football' }],
  // MMA: hit UFC + Bellator + PFL + ONE so prelim coverage isn't UFC-only.
  // ESPN aggregates them under separate league paths.
  mma_mixed_martial_arts:   [
    { sport: 'mma', league: 'ufc' },
    { sport: 'mma', league: 'pfl' },
    { sport: 'mma', league: 'bellator' },
    { sport: 'mma', league: 'one' },
  ],
  boxing_boxing:            [{ sport: 'boxing', league: 'top-rank' }],
  // Major soccer leagues + cup competitions
  soccer_epl:                  [{ sport: 'soccer', league: 'eng.1' }],
  soccer_germany_bundesliga:   [{ sport: 'soccer', league: 'ger.1' }],
  soccer_italy_serie_a:        [{ sport: 'soccer', league: 'ita.1' }],
  soccer_spain_la_liga:        [{ sport: 'soccer', league: 'esp.1' }],
  soccer_france_ligue_one:     [{ sport: 'soccer', league: 'fra.1' }],
  soccer_usa_mls:              [{ sport: 'soccer', league: 'usa.1' }],
  soccer_usa_nwsl:             [{ sport: 'soccer', league: 'usa.nwsl' }],
  soccer_mexico_ligamx:        [{ sport: 'soccer', league: 'mex.1' }],
  soccer_brazil_campeonato:    [{ sport: 'soccer', league: 'bra.1' }],
  soccer_uefa_champs_league:   [{ sport: 'soccer', league: 'uefa.champions' }],
  soccer_uefa_europa_league:   [{ sport: 'soccer', league: 'uefa.europa' }],
  // Tennis: ESPN uses /tennis/atp + /tennis/wta. Both polled so any
  // match across either tour is in the cache. The dynamic-tournament
  // routing in odds-feed normalizes both onto our generic 'tennis' key.
  tennis: [
    { sport: 'tennis', league: 'atp' },
    { sport: 'tennis', league: 'wta' },
  ],
  // Golf scoreboard (PGA Tour). Per-player live probabilities for matchups
  // come from DataGolf, not ESPN — see services/datagolf.js for that path.
  // ESPN gives us tournament status + per-player current scores as a fallback.
  golf_pga_championship: [{ sport: 'golf', league: 'pga' }],
};

// 60s for active sports = sports with any non-pre game in last poll.
// 5min for inactive sports — keeps quota / network noise low when nothing
// is happening on a given tour at a given hour.
const ACTIVE_TTL_MS = 60 * 1000;
const INACTIVE_TTL_MS = 5 * 60 * 1000;

// In-memory cache: sport → { fetchedAt, hasActive, games[] }
// games[] = [{ homeTeam, awayTeam, commenceTime, completed, homeScore,
//              awayScore, status, league }]
const cache = {};

// Last-poll times so the poller can decide which sports are due.
const lastPolledAt = {}; // sport → ms

function _normalizeTeam(name) {
  return String(name || '')
    .toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9 ]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// ESPN's `competition.status.type.state` values:
//   'pre'  → not started
//   'in'   → in progress (clock active or between periods)
//   'post' → final (or canceled — see status.type.completed)
function _parseGame(event, leagueLabel) {
  try {
    const comp = event?.competitions?.[0];
    if (!comp) return null;
    const home = comp.competitors?.find(c => c.homeAway === 'home');
    const away = comp.competitors?.find(c => c.homeAway === 'away');
    if (!home || !away) return null;
    const status = comp.status?.type;
    const state = status?.state || 'pre';
    // Only treat truly final games as completed. ESPN sets type.completed=true
    // on STATUS_FINAL, plus various canceled / forfeit / postponed states.
    // We require completed=true AND state='post' to be conservative.
    const completed = !!status?.completed && state === 'post';
    const homeScore = home.score != null ? Number(home.score) : null;
    const awayScore = away.score != null ? Number(away.score) : null;
    // Linescores: per-period (per-inning for MLB) numeric values. ESPN
    // returns these in `linescores: [{value: 2}, {value: 0}, ...]` on
    // each competitor when the game has started. Used by the F5 leg
    // resolver to compute first-5-innings totals — full-game score
    // alone can't tell us where the F5 ended. May be empty/missing for
    // pre-game events; downstream code handles null gracefully.
    const homeLinescores = Array.isArray(home.linescores)
      ? home.linescores.map(s => s && s.value != null ? Number(s.value) : null)
      : null;
    const awayLinescores = Array.isArray(away.linescores)
      ? away.linescores.map(s => s && s.value != null ? Number(s.value) : null)
      : null;
    // F5 (top of 5th onwards): a game has "completed 5 innings" once both
    // teams have batted 5 times. With ESPN's linescore array we treat
    // 5+ entries with non-null values as the gate. If only 4 entries
    // exist, the game is still in progress through the 4th inning and
    // F5 hasn't resolved yet.
    const homeRunsThru5 = (homeLinescores && homeLinescores.length >= 5)
      ? homeLinescores.slice(0, 5).reduce((s, v) => s + (v || 0), 0) : null;
    const awayRunsThru5 = (awayLinescores && awayLinescores.length >= 5)
      ? awayLinescores.slice(0, 5).reduce((s, v) => s + (v || 0), 0) : null;
    const f5Completed = homeRunsThru5 != null && awayRunsThru5 != null;
    // Period (quarter / inning / set) and clock string. ESPN exposes these
    // under status.period (1-based int) and status.displayClock ("5:23" /
    // "Top 7th"-style for MLB). Used by the in-play win-prob models to
    // compute time-remaining for basketball/hockey and inning state for
    // MLB. shortDetail is a free-form string ("End 3rd Quarter", "5:23
    // - 4th Quarter") good as a fallback display when period/clock parse
    // ambiguously.
    const period = (status && status.period != null) ? Number(status.period) : null;
    const displayClock = (status && status.displayClock) || null;
    const shortDetail = (status && status.shortDetail) || null;
    return {
      homeTeam: home.team?.displayName || home.team?.name || '',
      awayTeam: away.team?.displayName || away.team?.name || '',
      // Aliases that some sports/leagues use (player names for tennis/MMA/golf
      // tournaments) — populated from `athlete` if displayName missing.
      homeAlt: home.team?.shortDisplayName || home.athlete?.displayName || null,
      awayAlt: away.team?.shortDisplayName || away.athlete?.displayName || null,
      commenceTime: event.date || comp.date || null,
      completed,
      state,
      statusName: status?.name || status?.shortDetail || null,
      homeScore,
      awayScore,
      // Live-play state: period (1-based) + remaining clock string. Both null
      // pre-game / post-game. Models pull from these. Sport-specific
      // semantics: basketball/football period = quarter, hockey = period,
      // MLB = inning (note: status.shortDetail also encodes top/bottom).
      period,
      displayClock,
      shortDetail,
      // Per-inning runs (MLB) and F5 (first-5-innings) summed runs.
      // f5Completed=true when both teams have batted 5+ times.
      homeLinescores,
      awayLinescores,
      homeRunsThru5,
      awayRunsThru5,
      f5Completed,
      league: leagueLabel,
    };
  } catch (_) {
    return null;
  }
}

// Tennis matches live in event.groupings[].competitions[] (one grouping per
// draw — singles winners side, singles losers side, doubles, etc.) — NOT
// in the standard event.competitions[] that other sports use. Each match's
// competitor is an athlete (no team), and the score comes from sets won
// (count of linescores[].winner === true) rather than a single score field.
function _parseTennisMatches(event, leagueLabel) {
  const out = [];
  const groupings = Array.isArray(event?.groupings) ? event.groupings : [];
  for (const grouping of groupings) {
    const comps = Array.isArray(grouping?.competitions) ? grouping.competitions : [];
    for (const comp of comps) {
      try {
        const home = (comp.competitors || []).find(c => c.homeAway === 'home');
        const away = (comp.competitors || []).find(c => c.homeAway === 'away');
        if (!home || !away) continue;
        const homeName = home.athlete?.displayName || home.team?.displayName || '';
        const awayName = away.athlete?.displayName || away.team?.displayName || '';
        // Skip TBD slots (future matches in the bracket where the bettor's
        // opponent hasn't been determined yet — competitor name is "TBD").
        if (!homeName || !awayName || homeName === 'TBD' || awayName === 'TBD') continue;
        const status = comp.status?.type;
        const state = status?.state || 'pre';
        const completed = !!status?.completed && state === 'post';
        // Tennis "score" is sets won, not a single number. Count
        // linescores[].winner=true. linescores may be missing pre-match.
        const homeLinescores = Array.isArray(home.linescores) ? home.linescores : [];
        const awayLinescores = Array.isArray(away.linescores) ? away.linescores : [];
        const homeSetsWon = homeLinescores.filter(s => s && s.winner === true).length;
        const awaySetsWon = awayLinescores.filter(s => s && s.winner === true).length;
        out.push({
          homeTeam: homeName,
          awayTeam: awayName,
          // Alt slots — homeAlt set so the matcher's secondary lookup works
          // even if the SharpAPI / TOA name varies (accents, hyphenated last
          // names). For tennis we don't have a separate shortName, so leave
          // both null — the primary athlete.displayName matches PX directly.
          homeAlt: null,
          awayAlt: null,
          commenceTime: comp.date || comp.startDate || null,
          completed,
          state,
          statusName: status?.name || status?.shortDetail || null,
          homeScore: homeSetsWon,
          awayScore: awaySetsWon,
          // Tennis has no innings/halves — leave linescores fields null so
          // the F5 / H1 paths short-circuit cleanly.
          homeLinescores: null,
          awayLinescores: null,
          homeRunsThru5: null,
          awayRunsThru5: null,
          f5Completed: false,
          league: leagueLabel,
        });
      } catch (_) { /* per-match isolation — keep parsing remaining matches */ }
    }
  }
  return out;
}

function _ymdUTC(offsetDays) {
  const d = new Date(Date.now() + offsetDays * 86400 * 1000);
  return d.getUTCFullYear().toString()
    + String(d.getUTCMonth() + 1).padStart(2, '0')
    + String(d.getUTCDate()).padStart(2, '0');
}

async function _fetchOneLeagueDay(sport, league, dateStr) {
  // dateStr is YYYYMMDD or '' for the default (today) endpoint.
  const url = dateStr
    ? `https://site.api.espn.com/apis/site/v2/sports/${sport}/${league}/scoreboard?dates=${dateStr}`
    : `https://site.api.espn.com/apis/site/v2/sports/${sport}/${league}/scoreboard`;
  try {
    const resp = await fetch(url, { timeout: 10000 });
    if (!resp.ok) {
      log.debug('EspnScores', `Fetch failed for ${sport}/${league} dates=${dateStr || 'today'}: ${resp.status}`);
      return [];
    }
    const data = await resp.json();
    const events = Array.isArray(data?.events) ? data.events : [];
    const out = [];
    for (const e of events) {
      if (sport === 'tennis') {
        const matches = _parseTennisMatches(e, `${sport}/${league}`);
        for (const m of matches) out.push(m);
      } else {
        const g = _parseGame(e, `${sport}/${league}`);
        if (g) out.push(g);
      }
    }
    return out;
  } catch (err) {
    log.debug('EspnScores', `Fetch error for ${sport}/${league} dates=${dateStr || 'today'}: ${err.message}`);
    return [];
  }
}

// Fetch today + yesterday in parallel and dedupe. ESPN's default
// scoreboard endpoint returns only games for "today" in their UTC
// reckoning — so once UTC midnight passes, yesterday's afternoon games
// silently disappear from the cache. A Saturday 3pm ET / 19:00 UTC NBA
// playoff game falls off ~5 hours after kickoff, well before the
// leg-resolution loop next runs. Verified 2026-05-10 against parlay
// 019e0ced — Lakers (8:30pm ET = 5/10 00:30 UTC) resolved, Pistons
// (3pm ET = 5/9 19:00 UTC) didn't.
//
// The explicit ?dates=YYYYMMDD lookup is the documented way to fetch
// a historical day's scoreboard. Yesterday's slice + today's default
// covers a 48-hour window with no overlap (ESPN's "today" is keyed by
// its own server's UTC interpretation, so we just trust it for the
// recent window and rely on the explicit date for what fell off).
async function _fetchOneLeague(sport, league) {
  const [todayGames, yesterdayGames] = await Promise.all([
    _fetchOneLeagueDay(sport, league, ''),
    _fetchOneLeagueDay(sport, league, _ymdUTC(-1)),
  ]);
  const seen = new Set();
  const merged = [];
  for (const list of [todayGames, yesterdayGames]) {
    for (const g of list) {
      // Dedupe on team-pair + commenceTime so a game showing up in
      // both windows (rare but possible near the UTC boundary) only
      // lands once in the cache.
      const key = `${g.homeTeam || ''}|${g.awayTeam || ''}|${g.commenceTime || ''}`;
      if (seen.has(key)) continue;
      seen.add(key);
      merged.push(g);
    }
  }
  return merged;
}

async function refreshSport(sportKey) {
  const leagues = ESPN_LEAGUES[sportKey];
  if (!leagues || leagues.length === 0) return null;
  const lists = await Promise.all(leagues.map(({ sport, league }) =>
    _fetchOneLeague(sport, league)
  ));
  const games = [].concat(...lists);
  const hasActive = games.some(g => g.state === 'in' || g.state === 'pre');
  cache[sportKey] = { fetchedAt: Date.now(), hasActive, games };
  lastPolledAt[sportKey] = Date.now();
  log.debug('EspnScores', `${sportKey}: ${games.length} games (${games.filter(g => g.completed).length} final)`);
  return cache[sportKey];
}

async function refreshAll() {
  const sports = Object.keys(ESPN_LEAGUES);
  // Sequential to keep concurrent connection count low; ESPN is fast
  // enough that 12 sports finishes in <5s total.
  for (const s of sports) {
    try { await refreshSport(s); } catch (_) { /* per-sport isolation */ }
  }
}

// Sync getter — never makes a network call. Order-tracker calls this
// in its 30s leg-resolution loop. Returns the same shape as TOA's
// odds-feed.getGameResult so callers can swap.
//
// Match by normalized team-pair, with displayName / shortDisplayName /
// athlete alias fallbacks (tennis/golf use player names; UFC uses fighter
// names). When startTime is provided AND multiple games match the same
// team-pair (back-to-back days, doubleheader, mid-week + weekend matchups),
// pick the one whose commenceTime is closest to startTime — otherwise
// the cache may carry yesterday's win + today's loss for the same teams
// and return whichever it sees first, flipping a resolved leg's status.
function getEspnGameResult(sportKey, homeTeam, awayTeam, startTime) {
  const entry = cache[sportKey];
  if (!entry) return null;
  const nHome = _normalizeTeam(homeTeam);
  const nAway = _normalizeTeam(awayTeam);
  if (!nHome || !nAway) return null;
  const targetMs = startTime ? new Date(startTime).getTime() : null;
  let bestMatch = null;
  let bestFlipped = false;
  let bestDiffMs = Infinity;
  for (const g of entry.games) {
    const candidates = [
      [_normalizeTeam(g.homeTeam), _normalizeTeam(g.awayTeam)],
      [_normalizeTeam(g.awayTeam), _normalizeTeam(g.homeTeam)],
    ];
    if (g.homeAlt) candidates.push([_normalizeTeam(g.homeAlt), _normalizeTeam(g.awayTeam)]);
    if (g.awayAlt) candidates.push([_normalizeTeam(g.homeTeam), _normalizeTeam(g.awayAlt)]);
    if (g.homeAlt && g.awayAlt) {
      candidates.push([_normalizeTeam(g.homeAlt), _normalizeTeam(g.awayAlt)]);
      candidates.push([_normalizeTeam(g.awayAlt), _normalizeTeam(g.homeAlt)]);
    }
    let matched = false;
    let flipped = false;
    for (const [ch, ca] of candidates) {
      const exact = (ch === nHome && ca === nAway) || (ch.includes(nHome) && ca.includes(nAway))
        || (nHome.includes(ch) && nAway.includes(ca));
      if (exact) { matched = true; break; }
      const flip = (ch === nAway && ca === nHome) || (ch.includes(nAway) && ca.includes(nHome))
        || (nAway.includes(ch) && nHome.includes(ca));
      if (flip) { matched = true; flipped = true; break; }
    }
    if (!matched) continue;
    if (targetMs && g.commenceTime) {
      const gMs = new Date(g.commenceTime).getTime();
      if (Number.isFinite(gMs)) {
        const diff = Math.abs(gMs - targetMs);
        if (diff < bestDiffMs) {
          bestDiffMs = diff;
          bestMatch = g;
          bestFlipped = flipped;
        }
        continue;
      }
    }
    // No targetMs OR no commenceTime on this game — keep first match
    // (preserves prior behavior) but only if nothing time-matched yet.
    if (bestMatch === null) {
      bestMatch = g;
      bestFlipped = flipped;
    }
  }
  if (!bestMatch) return null;
  // Refuse to use a far-off match when a startTime was provided. If the
  // closest match is more than 24h away from the leg's startTime, we're
  // almost certainly looking at the wrong day's game — return null and
  // let the caller fall through to TOA, which has its own time match.
  if (targetMs && bestDiffMs > 24 * 60 * 60 * 1000) return null;
  let winner = null;
  if (bestMatch.completed && bestMatch.homeScore != null && bestMatch.awayScore != null) {
    if (bestMatch.homeScore > bestMatch.awayScore) winner = bestFlipped ? 'away' : 'home';
    else if (bestMatch.awayScore > bestMatch.homeScore) winner = bestFlipped ? 'home' : 'away';
    else winner = 'tie';
  }
  return {
    completed: bestMatch.completed,
    homeScore: bestFlipped ? bestMatch.awayScore : bestMatch.homeScore,
    awayScore: bestFlipped ? bestMatch.homeScore : bestMatch.awayScore,
    winner,
    state: bestMatch.state,
    statusName: bestMatch.statusName,
    // Live-play state — used by in-play-models.js to compute live win
    // probabilities. period is sport-specific (quarter / inning / period),
    // displayClock is the per-period remaining clock ("5:23"), shortDetail
    // is a free-form string like "Bot 7th" or "End 3rd Quarter".
    period: bestMatch.period != null ? bestMatch.period : null,
    displayClock: bestMatch.displayClock || null,
    shortDetail: bestMatch.shortDetail || null,
    // For team-perspective consumers: indicate whether we flipped home/away
    // during matching so they can correctly map "home"/"away" semantics.
    homeAwayFlipped: bestFlipped,
    league: bestMatch.league,
    source: 'espn',
  };
}

/**
 * Sync getter for first-5-innings (F5) result. Returns the same kind of
 * object as getEspnGameResult but scoped to runs through the bottom of
 * the 5th. Used by order-tracker to resolve first_5_innings_moneyline /
 * first_5_innings_run_line / first_5_innings_total legs without waiting
 * for the full game to finish.
 *
 * Returns:
 *   {
 *     completed:       boolean — both teams have batted 5+ times
 *     homeRunsThru5:   number or null
 *     awayRunsThru5:   number or null
 *     winner:          'home' | 'away' | 'tie' | null   (F5 ML winner)
 *     state, statusName, league, source: same as getEspnGameResult
 *   }
 *   or null if no team-pair match in cache.
 */
function getEspnF5Result(sportKey, homeTeam, awayTeam, startTime) {
  const entry = cache[sportKey];
  if (!entry) return null;
  const nHome = _normalizeTeam(homeTeam);
  const nAway = _normalizeTeam(awayTeam);
  if (!nHome || !nAway) return null;
  const targetMs = startTime ? new Date(startTime).getTime() : null;
  let bestMatch = null;
  let bestFlipped = false;
  let bestDiffMs = Infinity;
  for (const g of entry.games) {
    const candidates = [
      [_normalizeTeam(g.homeTeam), _normalizeTeam(g.awayTeam)],
      [_normalizeTeam(g.awayTeam), _normalizeTeam(g.homeTeam)],
    ];
    if (g.homeAlt) candidates.push([_normalizeTeam(g.homeAlt), _normalizeTeam(g.awayTeam)]);
    if (g.awayAlt) candidates.push([_normalizeTeam(g.homeTeam), _normalizeTeam(g.awayAlt)]);
    let matched = false;
    let flipped = false;
    for (const [ch, ca] of candidates) {
      const exact = (ch === nHome && ca === nAway) || (ch.includes(nHome) && ca.includes(nAway))
        || (nHome.includes(ch) && nAway.includes(ca));
      if (exact) { matched = true; break; }
      const flip = (ch === nAway && ca === nHome) || (ch.includes(nAway) && ca.includes(nHome))
        || (nAway.includes(ch) && nHome.includes(ca));
      if (flip) { matched = true; flipped = true; break; }
    }
    if (!matched) continue;
    if (targetMs && g.commenceTime) {
      const gMs = new Date(g.commenceTime).getTime();
      if (Number.isFinite(gMs)) {
        const diff = Math.abs(gMs - targetMs);
        if (diff < bestDiffMs) {
          bestDiffMs = diff;
          bestMatch = g;
          bestFlipped = flipped;
        }
        continue;
      }
    }
    if (bestMatch === null) {
      bestMatch = g;
      bestFlipped = flipped;
    }
  }
  if (!bestMatch) return null;
  if (targetMs && bestDiffMs > 24 * 60 * 60 * 1000) return null;
  if (!bestMatch.f5Completed) {
    // Game in progress but hasn't reached the bottom of the 5th yet.
    return {
      completed: false,
      homeRunsThru5: bestMatch.homeRunsThru5,
      awayRunsThru5: bestMatch.awayRunsThru5,
      winner: null,
      state: bestMatch.state,
      statusName: bestMatch.statusName,
      league: bestMatch.league,
      source: 'espn-f5',
    };
  }
  // Apply orientation flip if leg's home/away differs from ESPN's
  const homeF5 = bestFlipped ? bestMatch.awayRunsThru5 : bestMatch.homeRunsThru5;
  const awayF5 = bestFlipped ? bestMatch.homeRunsThru5 : bestMatch.awayRunsThru5;
  let winner = null;
  if (homeF5 > awayF5) winner = 'home';
  else if (awayF5 > homeF5) winner = 'away';
  else winner = 'tie';
  return {
    completed: true,
    homeRunsThru5: homeF5,
    awayRunsThru5: awayF5,
    winner,
    state: bestMatch.state,
    statusName: bestMatch.statusName,
    league: bestMatch.league,
    source: 'espn-f5',
  };
}

// Periodic loop. Caller (index.js) starts this on boot.
function startPoller() {
  // Initial warm-up
  refreshAll().catch(err => log.warn('EspnScores', `Initial refresh failed: ${err.message}`));

  // 60s heartbeat — refresh sports whose cache is past their TTL.
  setInterval(async () => {
    const now = Date.now();
    for (const sport of Object.keys(ESPN_LEAGUES)) {
      const entry = cache[sport];
      const ttl = entry?.hasActive ? ACTIVE_TTL_MS : INACTIVE_TTL_MS;
      const age = now - (lastPolledAt[sport] || 0);
      if (age >= ttl) {
        refreshSport(sport).catch(err =>
          log.debug('EspnScores', `${sport} refresh error: ${err.message}`));
      }
    }
  }, 60 * 1000);

  log.info('EspnScores', `Started poller for ${Object.keys(ESPN_LEAGUES).length} sport keys`);
}

// Diagnostic: full cache snapshot for /debug-espn-scores endpoint
function __debugDump() {
  const out = {};
  for (const [sport, entry] of Object.entries(cache)) {
    out[sport] = {
      fetchedAt: entry.fetchedAt,
      hasActive: entry.hasActive,
      gameCount: entry.games.length,
      finalCount: entry.games.filter(g => g.completed).length,
      games: entry.games.slice(0, 50),
    };
  }
  return out;
}

module.exports = {
  refreshSport,
  refreshAll,
  getEspnGameResult,
  getEspnF5Result,
  startPoller,
  __debugDump,
};
