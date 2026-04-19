// DataGolf API integration — fetches head-to-head matchup odds from DataGolf's
// betting-tools endpoint. Provides golf matchup coverage for PX parlay quoting.
//
// API key: DATAGOLF_API_KEY env var
// Endpoint: https://feeds.datagolf.com/betting-tools/matchups
// Markets: round_matchups (2-way), tournament_matchups (2-way), 3_balls (3-way)
//
// Data is parsed into odds cache entries under sport key 'golf_matchups', where
// each matchup is a pseudo-event with homeTeam/awayTeam being the two players.

const fetch = require('node-fetch');
const { config } = require('../config');
const log = require('./logger');

// Books to use for de-vig consensus. Excludes 'datagolf' which is their model
// prediction (not a tradeable market quote).
// Pinnacle added — consistently the sharpest book in DataGolf's feed for
// golf matchups and was being silently excluded from the consensus. Its
// inclusion pulls fair values closer to true market consensus.
const CONSENSUS_BOOKS = ['bet365', 'betmgm', 'betonline', 'bovada', 'caesars', 'draftkings', 'fanduel', 'unibet', 'betcris', 'pinnacle'];
const TOURS = ['pga', 'euro', 'alt']; // alt includes LIV

function americanToImpliedProb(odds) {
  if (odds == null) return null;
  const o = Number(odds);
  if (isNaN(o)) return null;
  if (o > 0) return 100 / (o + 100);
  return Math.abs(o) / (Math.abs(o) + 100);
}

function deVig2Way(p1, p2) {
  const total = p1 + p2;
  if (total === 0) return [0.5, 0.5];
  return [p1 / total, p2 / total];
}

function avg(arr) {
  if (arr.length === 0) return 0;
  return arr.reduce((s, v) => s + v, 0) / arr.length;
}

/**
 * DataGolf occasionally returns MULTIPLE match_list entries for the same
 * pairing (different internal source rollups). If we process them
 * independently, one may end up with only a single soft book (e.g.
 * bet365 alone) while another holds the full 7-book spread — and our
 * lookup returns the first entry for a given pair, which can be the
 * thin one. Real-world miss: Scheffler vs Fitzpatrick R4 showed two
 * DG entries, first had bet365-only at Scheffler -138, second had 7
 * books averaging Scheffler ≈ -170. We were quoting off the bet365
 * number (FV -145, ~25 cents softer than market).
 *
 * Fix: merge all match_list entries that refer to the same pairing
 * (by normalized player pair, regardless of p1/p2 ordering) into a
 * single entry whose `odds` map contains every book from every
 * contributing entry. First-seen wins per book so later soft entries
 * can't overwrite a sharper one.
 */
function mergeDuplicatePairings(matchList) {
  if (!Array.isArray(matchList)) return [];
  const merged = new Map(); // canonical-pair-key → merged entry
  for (const entry of matchList) {
    const p1n = normalizeDgPlayerName(entry.p1_player_name);
    const p2n = normalizeDgPlayerName(entry.p2_player_name);
    if (!p1n || !p2n) continue;
    // Canonical key: alphabetically ordered player pair, case-insensitive.
    const [a, b] = [p1n, p2n].map(s => s.toLowerCase()).sort();
    const key = a + '|' + b;
    const existing = merged.get(key);
    if (!existing) {
      merged.set(key, {
        p1_player_name: entry.p1_player_name,
        p2_player_name: entry.p2_player_name,
        ties: entry.ties,
        odds: { ...(entry.odds || {}) },
      });
      continue;
    }
    // Another entry for the same pairing — merge its odds in. If the
    // new entry has the players in opposite p1/p2 order, swap before
    // merging so p1/p2 remain consistent with the canonical entry.
    const existingP1Norm = normalizeDgPlayerName(existing.p1_player_name).toLowerCase();
    const thisP1Norm = p1n.toLowerCase();
    const swapped = thisP1Norm !== existingP1Norm;
    for (const [book, o] of Object.entries(entry.odds || {})) {
      if (!o || typeof o !== 'object') continue;
      if (existing.odds[book]) continue; // first-seen wins
      existing.odds[book] = swapped ? { p1: o.p2, p2: o.p1 } : { p1: o.p1, p2: o.p2 };
    }
  }
  return Array.from(merged.values());
}

/**
 * Normalize a DataGolf player name: "Im, Sungjae" → "Sungjae Im".
 * Handles edge cases: suffixes (Jr., III), hyphens, accents, single names.
 */
function normalizeDgPlayerName(name) {
  if (!name) return '';
  const s = name.trim();
  if (!s.includes(',')) return s; // already "Firstname Lastname"
  const parts = s.split(',').map(p => p.trim());
  if (parts.length === 2) {
    return parts[1] + ' ' + parts[0]; // "Firstname Lastname"
  }
  // Handle "Lastname, Firstname, Jr." or similar
  return parts.slice(1).join(' ') + ' ' + parts[0];
}

/**
 * Fetch matchups for a specific tour and market type from DataGolf.
 */
async function fetchDgMatchups(tour, market) {
  const apiKey = config.dataGolf.apiKey;
  if (!apiKey) {
    log.warn('DataGolf', 'DATAGOLF_API_KEY not set — skipping fetch');
    return null;
  }
  const url = `${config.dataGolf.baseUrl}/betting-tools/matchups`
    + `?tour=${tour}`
    + `&market=${market}`
    + `&odds_format=american`
    + `&file_format=json`
    + `&key=${apiKey}`;

  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      log.warn('DataGolf', `Fetch failed (${resp.status}) for ${tour}/${market}`);
      return null;
    }
    const data = await resp.json();
    if (!data || typeof data.match_list === 'string') {
      // DataGolf returns a string message when no matchups are offered
      log.debug('DataGolf', `${tour}/${market}: ${data && data.match_list}`);
      return null;
    }
    return data; // { event_name, last_updated, market, match_list, round_num }
  } catch (err) {
    log.warn('DataGolf', `Fetch error for ${tour}/${market}: ${err.message}`);
    return null;
  }
}

/**
 * Parse a single 2-way matchup into our cache event format.
 * Returns { homeTeam, awayTeam, eventName, roundNum, markets: { h2h: {home, away, books} } }
 * or null if not enough book data.
 */
function parseMatchup(entry, eventName, roundNum) {
  const p1Name = normalizeDgPlayerName(entry.p1_player_name);
  const p2Name = normalizeDgPlayerName(entry.p2_player_name);
  if (!p1Name || !p2Name) return null;

  // Collect book pairs (excluding datagolf's own model)
  const p1Probs = [], p2Probs = [];
  const p1Raw = [], p2Raw = [];
  for (const [book, odds] of Object.entries(entry.odds || {})) {
    if (!CONSENSUS_BOOKS.includes(book)) continue;
    const p1Prob = americanToImpliedProb(odds.p1);
    const p2Prob = americanToImpliedProb(odds.p2);
    if (p1Prob == null || p2Prob == null) continue;
    // ties: void — de-vig as 2-way
    const [fp1, fp2] = deVig2Way(p1Prob, p2Prob);
    p1Probs.push(fp1);
    p2Probs.push(fp2);
    p1Raw.push(Number(odds.p1));
    p2Raw.push(Number(odds.p2));
  }
  if (p1Probs.length === 0) return null; // no tradeable book data

  // Fair probability = de-vigged consensus across sportsbooks only.
  // DataGolf's model predictions are NOT used for pricing — only real
  // tradeable book odds feed into the consensus.
  const dvP1 = avg(p1Probs);
  const dvP2 = avg(p2Probs);

  return {
    homeTeam: p2Name, // p2 → home (consistent arbitrary choice)
    awayTeam: p1Name, // p1 → away
    eventName,
    roundNum,
    p1Name,
    p2Name,
    markets: {
      h2h: {
        home: {
          rawOdds: p2Raw[0] || null,
          impliedProb: avg(p2Raw.map(americanToImpliedProb)),
          fairProb: dvP2,
          displayFairProb: dvP2,
        },
        away: {
          rawOdds: p1Raw[0] || null,
          impliedProb: avg(p1Raw.map(americanToImpliedProb)),
          fairProb: dvP1,
          displayFairProb: dvP1,
        },
        books: p1Probs.length,
      },
    },
  };
}

/**
 * Main entry point: fetch golf matchups from DataGolf and build a cache object
 * compatible with our odds cache format.
 * Returns { events: { [normalizedKey]: [matchupEvent] }, fetchedAt }
 */
async function fetchGolfMatchupsCache() {
  const apiKey = config.dataGolf.apiKey;
  if (!apiKey) return { events: {}, fetchedAt: Date.now() };

  const normalizeTeamName = (n) => (n || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
  const normalizeEventKey = (h, a) => `${normalizeTeamName(h)}|${normalizeTeamName(a)}`;

  const parsed = {};
  let totalMatchups = 0;

  for (const tour of TOURS) {
    // Fetch round_matchups (active tournament rounds)
    const roundData = await fetchDgMatchups(tour, 'round_matchups');
    if (roundData && Array.isArray(roundData.match_list)) {
      const eventName = roundData.event_name || '';
      const roundNum = roundData.round_num || null;
      const mergedRound = mergeDuplicatePairings(roundData.match_list);
      for (const entry of mergedRound) {
        const matchup = parseMatchup(entry, eventName, roundNum);
        if (!matchup) continue;

        // Store in BOTH orderings so order-insensitive lookups work
        const keyAB = normalizeEventKey(matchup.homeTeam, matchup.awayTeam);
        const keyBA = normalizeEventKey(matchup.awayTeam, matchup.homeTeam);

        // Primary entry (p2 as home, p1 as away)
        if (!parsed[keyAB]) parsed[keyAB] = [];
        parsed[keyAB].push({
          homeTeam: matchup.homeTeam,
          awayTeam: matchup.awayTeam,
          commenceTime: null,
          eventName: matchup.eventName,
          roundNum: matchup.roundNum,
          matchupType: 'round',
          markets: matchup.markets,
        });

        // Mirror entry (p1 as home, p2 as away) — swap home/away in h2h
        if (!parsed[keyBA]) parsed[keyBA] = [];
        parsed[keyBA].push({
          homeTeam: matchup.awayTeam,
          awayTeam: matchup.homeTeam,
          commenceTime: null,
          eventName: matchup.eventName,
          roundNum: matchup.roundNum,
          matchupType: 'round',
          markets: {
            h2h: {
              home: matchup.markets.h2h.away, // swapped
              away: matchup.markets.h2h.home, // swapped
              books: matchup.markets.h2h.books,
            },
          },
        });

        totalMatchups++;
      }
    }

    // Fetch tournament_matchups (if active)
    const tournData = await fetchDgMatchups(tour, 'tournament_matchups');
    if (tournData && Array.isArray(tournData.match_list)) {
      const eventName = tournData.event_name || '';
      const mergedTourn = mergeDuplicatePairings(tournData.match_list);
      for (const entry of mergedTourn) {
        const matchup = parseMatchup(entry, eventName, null);
        if (!matchup) continue;
        const keyAB = normalizeEventKey(matchup.homeTeam, matchup.awayTeam);
        const keyBA = normalizeEventKey(matchup.awayTeam, matchup.homeTeam);
        if (!parsed[keyAB]) parsed[keyAB] = [];
        parsed[keyAB].push({
          homeTeam: matchup.homeTeam,
          awayTeam: matchup.awayTeam,
          commenceTime: null,
          eventName: matchup.eventName,
          roundNum: null,
          matchupType: 'tournament',
          markets: matchup.markets,
        });
        if (!parsed[keyBA]) parsed[keyBA] = [];
        parsed[keyBA].push({
          homeTeam: matchup.awayTeam,
          awayTeam: matchup.homeTeam,
          commenceTime: null,
          eventName: matchup.eventName,
          roundNum: null,
          matchupType: 'tournament',
          markets: {
            h2h: {
              home: matchup.markets.h2h.away,
              away: matchup.markets.h2h.home,
              books: matchup.markets.h2h.books,
            },
          },
        });
        totalMatchups++;
      }
    }
  }

  log.info('DataGolf', `Fetched ${totalMatchups} matchups across ${TOURS.length} tours (${Object.keys(parsed).length} cache keys incl mirrors)`);
  return { events: parsed, fetchedAt: Date.now() };
}

module.exports = {
  fetchGolfMatchupsCache,
  normalizeDgPlayerName,
};
