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
      league: leagueLabel,
    };
  } catch (_) {
    return null;
  }
}

async function _fetchOneLeague(sport, league) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/${sport}/${league}/scoreboard`;
  try {
    const resp = await fetch(url, { timeout: 10000 });
    if (!resp.ok) {
      log.debug('EspnScores', `Fetch failed for ${sport}/${league}: ${resp.status}`);
      return [];
    }
    const data = await resp.json();
    const events = Array.isArray(data?.events) ? data.events : [];
    const out = [];
    for (const e of events) {
      const g = _parseGame(e, `${sport}/${league}`);
      if (g) out.push(g);
    }
    return out;
  } catch (err) {
    log.debug('EspnScores', `Fetch error for ${sport}/${league}: ${err.message}`);
    return [];
  }
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
// names). targetTime is unused for now — ESPN's scoreboard only carries
// today + adjacent days, so doubleheader collisions are rare. If needed
// we can disambiguate by closest-by-time later.
function getEspnGameResult(sportKey, homeTeam, awayTeam) {
  const entry = cache[sportKey];
  if (!entry) return null;
  const nHome = _normalizeTeam(homeTeam);
  const nAway = _normalizeTeam(awayTeam);
  if (!nHome || !nAway) return null;
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
    let winner = null;
    if (g.completed && g.homeScore != null && g.awayScore != null) {
      if (g.homeScore > g.awayScore) winner = flipped ? 'away' : 'home';
      else if (g.awayScore > g.homeScore) winner = flipped ? 'home' : 'away';
      else winner = 'tie';
    }
    return {
      completed: g.completed,
      homeScore: flipped ? g.awayScore : g.homeScore,
      awayScore: flipped ? g.homeScore : g.awayScore,
      winner,
      state: g.state,
      statusName: g.statusName,
      league: g.league,
      source: 'espn',
    };
  }
  return null;
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
  startPoller,
  __debugDump,
};
