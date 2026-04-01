const { config } = require('../config');
const log = require('./logger');
const px = require('./prophetx');
const oddsFeed = require('./odds-feed');

// ---------------------------------------------------------------------------
// LINE INDEX — maps PX line_id → metadata + Odds API match
// ---------------------------------------------------------------------------
// { [lineId]: { sport, pxEventId, pxEventName, marketType, selection,
//               teamName, line, homeTeam, awayTeam,
//               oddsApiSport, oddsApiMarket, oddsApiSelection } }
const lineIndex = {};

// Reverse lookup: PX event_id → event metadata
const eventIndex = {};

// Tournament ID → name/sport lookup
const tournamentIndex = {};

// Stats from last seed
let lastSeedStats = null;

// ---------------------------------------------------------------------------
// TEAM NAME MATCHING
// ---------------------------------------------------------------------------

// Known overrides for team name mismatches between PX and The Odds API
// Add entries here if matching fails for specific teams
const TEAM_NAME_OVERRIDES = {
  // SharpAPI abbreviates some NHL city names
  'washington capitals': 'WAS Capitals',
  'columbus blue jackets': 'CBJ Blue Jackets',
  'montreal canadiens': 'MTL Canadiens',
  'new jersey devils': 'NJ Devils',
  'san jose sharks': 'SJ Sharks',
  'los angeles kings': 'LA Kings',
};

function normalizeTeamName(name) {
  return (name || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').trim();
}

/**
 * Try to match a PX team name to an Odds API team name.
 * Strategies: exact, contains, override map.
 */
function matchTeamName(pxName, oddsApiNames) {
  const norm = normalizeTeamName(pxName);

  // Check override map
  if (TEAM_NAME_OVERRIDES[norm]) {
    const override = TEAM_NAME_OVERRIDES[norm];
    const match = oddsApiNames.find(n => normalizeTeamName(n) === normalizeTeamName(override));
    if (match) return match;
  }

  // Exact normalized match
  const exact = oddsApiNames.find(n => normalizeTeamName(n) === norm);
  if (exact) return exact;

  // Substring: PX name contains Odds API name or vice versa
  for (const oaName of oddsApiNames) {
    const oaNorm = normalizeTeamName(oaName);
    if (norm.includes(oaNorm) || oaNorm.includes(norm)) return oaName;
  }

  // Last N words match (e.g., "Red Sox" matches "Boston Red Sox")
  const pxWords = norm.split(/\s+/);
  // Try last 2 words first (handles "Red Sox" vs "White Sox"), then last 1 word
  for (const n of [2, 1]) {
    if (pxWords.length < n + 1) continue; // Need at least n+1 words (city + name)
    const pxTail = pxWords.slice(-n).join(' ');
    if (pxTail.length < 4) continue;
    const matches = oddsApiNames.filter(name => {
      const words = normalizeTeamName(name).split(/\s+/);
      if (words.length < n) return false;
      return words.slice(-n).join(' ') === pxTail;
    });
    if (matches.length === 1) return matches[0];
  }

  return null;
}

// ---------------------------------------------------------------------------
// MARKET TYPE MAPPING
// ---------------------------------------------------------------------------

// PX market.type → Odds API market key
const MARKET_TYPE_MAP = {
  'moneyline': 'h2h',
  'spread': 'spreads',
  'total': 'totals',
};

// ---------------------------------------------------------------------------
// SEEDING
// ---------------------------------------------------------------------------

/**
 * Seed all lines from ProphetX, match to Odds API, register supported lines.
 *
 * Flow:
 * 1. Fetch all PX sport events
 * 2. Filter to supported sports
 * 3. Fetch markets for each event
 * 4. Parse line_ids from markets
 * 5. Match each line to Odds API event/market
 * 6. Register matched lines with PX
 */
async function seedAllLines() {
  log.info('Lines', '=== Starting line seed ===');

  // 1. Fetch PX events
  const allEvents = await px.fetchSportEvents();
  const pxSportNames = Object.values(config.sportNameMap);

  // Build tournament + event index from ALL events (not just supported)
  for (const e of allEvents) {
    if (e.tournament_id) {
      tournamentIndex[e.tournament_id] = {
        name: e.tournament_name || e.tournament?.name || e.sport_name,
        sport: e.sport_name,
      };
    }
    // Store ALL events for name resolution (even ones we don't support)
    if (e.event_id) {
      eventIndex[e.event_id] = eventIndex[e.event_id] || {
        name: e.name,
        sport: null,
        sportName: e.sport_name,
        competitors: e.competitors,
        scheduled: e.scheduled,
      };
    }
  }
  log.info('Lines', `Built indexes: ${Object.keys(tournamentIndex).length} tournaments, ${Object.keys(eventIndex).length} events`);

  // 2. Filter to supported sports (accept any non-settled status)
  const events = allEvents.filter(e =>
    pxSportNames.includes(e.sport_name) &&
    (!e.status || e.status !== 'settled') &&
    e.competitors && e.competitors.length >= 2
  );
  log.info('Lines', `Found ${events.length} supported sport events (of ${allEvents.length} total)`);

  // Get all Odds API cached events for matching
  const oddsApiEvents = oddsFeed.getAllCachedEvents();

  let totalLines = 0;
  let matchedLines = 0;
  let unmatchedEvents = [];

  // 3-4. Fetch markets and parse for each event
  for (const event of events) {
    // Determine sport key(s) — some PX sport names map to multiple keys
    // (e.g., "Basketball" → basketball_nba AND basketball_ncaab)
    const possibleSportKeys = Object.entries(config.sportNameMap)
      .filter(([k, v]) => v === event.sport_name)
      .map(([k]) => k);
    if (possibleSportKeys.length === 0) continue;

    // We'll determine the actual sport key by which one has a matching Odds API event
    let sportKey = possibleSportKeys[0]; // default to first match

    // Store event metadata
    eventIndex[event.event_id] = {
      name: event.name,
      sport: sportKey,
      sportName: event.sport_name,
      competitors: event.competitors,
      scheduled: event.scheduled,
    };

    // Extract home/away from PX event
    // Tennis/soccer may use different side labels or just have 2 competitors without home/away
    let homeComp = event.competitors.find(c => c.side === 'home');
    let awayComp = event.competitors.find(c => c.side === 'away');
    // Fallback: use first two competitors if no home/away labels
    if (!homeComp && !awayComp && event.competitors.length >= 2) {
      homeComp = event.competitors[0];
      awayComp = event.competitors[1];
    }
    if (!homeComp || !awayComp) {
      log.debug('Lines', `Skipping ${event.name}: missing competitors`);
      continue;
    }

    // 5. Try to match to Odds API event — try all possible sport keys
    let matchedHome = null, matchedAway = null, matchedOddsEvent = null;

    for (const tryKey of possibleSportKeys) {
      const allOddsTeams = oddsApiEvents
        .filter(e => e.sport === tryKey)
        .flatMap(e => [e.homeTeam, e.awayTeam]);
      const uniqueTeams = [...new Set(allOddsTeams)];

      const tryHome = matchTeamName(homeComp.name, uniqueTeams);
      const tryAway = matchTeamName(awayComp.name, uniqueTeams);

      if (tryHome && tryAway) {
        // Verify this pair exists as an actual Odds API event
        const oddsEvt = oddsFeed.getEventMarkets(tryKey, tryHome, tryAway)
          || oddsFeed.getEventMarkets(tryKey, tryAway, tryHome);
        if (oddsEvt) {
          matchedHome = tryHome;
          matchedAway = tryAway;
          matchedOddsEvent = oddsEvt;
          sportKey = tryKey; // Use the sport key that matched
          break;
        }
      }
    }

    if (!matchedHome || !matchedAway) {
      unmatchedEvents.push({
        pxEvent: event.name,
        pxHome: homeComp.name,
        pxAway: awayComp.name,
      });
      continue;
    }

    // Verify this home/away pair exists as an actual Odds API event
    const oddsEvent = matchedOddsEvent || oddsFeed.getEventMarkets(sportKey, matchedHome, matchedAway);
    if (!oddsEvent) {
      const oddsEventReversed = oddsFeed.getEventMarkets(sportKey, matchedAway, matchedHome);
      if (!oddsEventReversed) {
        unmatchedEvents.push({
          pxEvent: event.name,
          reason: 'Team names matched but no Odds API event found',
          matchedHome,
          matchedAway,
        });
        continue;
      }
      // Swap for correct orientation
      const temp = matchedHome;
      // Note: we'll handle the swap in line indexing below
    }

    // Fetch PX markets
    let markets;
    try {
      markets = await px.fetchMarkets(event.event_id);
    } catch (err) {
      log.error('Lines', `Failed to fetch markets for ${event.name}: ${err.message}`);
      continue;
    }

    // Filter to main markets only (moneyline, spread, total for the game)
    const mainMarkets = markets.filter(m =>
      ['moneyline', 'spread', 'total'].includes(m.type) &&
      // Exclude player props that also have type 'total'
      (m.type !== 'total' || m.name === 'Total' || m.name === 'Total Points' || m.name === 'Total Runs' || m.name === 'Total Goals')
    );

    for (const market of mainMarkets) {
      const parsed = px.parseMarketSelections(market);
      for (const sel of parsed) {
        totalLines++;

        // Determine Odds API selection mapping
        let oddsApiSelection = null;
        let oddsApiMarket = MARKET_TYPE_MAP[sel.marketType];

        if (sel.marketType === 'moneyline') {
          // Match team to home/away
          if (matchTeamName(sel.teamName, [matchedHome])) {
            oddsApiSelection = 'home';
          } else if (matchTeamName(sel.teamName, [matchedAway])) {
            oddsApiSelection = 'away';
          }
        } else if (sel.marketType === 'spread') {
          // Match by team name
          if (matchTeamName(sel.teamName, [matchedHome]) ||
              (sel.teamName.toLowerCase().includes(normalizeTeamName(matchedHome).split(' ').pop()))) {
            oddsApiSelection = 'home';
          } else {
            oddsApiSelection = 'away';
          }
        } else if (sel.marketType === 'total') {
          oddsApiSelection = sel.selection; // 'over' or 'under'
        }

        if (!oddsApiSelection || !oddsApiMarket) continue;

        // Register line — fair value check happens at RFQ pricing time
        // For moneylines, verify fair value exists now
        // For spreads/totals, register all alternate lines (fair value available for primary line)
        matchedLines++;
        lineIndex[sel.lineId] = {
          sport: sportKey,
          pxEventId: event.event_id,
          pxEventName: event.name,
          marketType: sel.marketType,
          selection: oddsApiSelection,
          teamName: sel.teamName,
          line: sel.line,
          homeTeam: matchedHome,
          awayTeam: matchedAway,
          oddsApiSport: sportKey,
          oddsApiMarket,
          oddsApiSelection,
          competitorId: sel.competitorId,
        };
      }
    }

    // Small delay to avoid hammering PX API
    await new Promise(r => setTimeout(r, 100));
  }

  // Log unmatched events
  if (unmatchedEvents.length > 0) {
    log.warn('Lines', `${unmatchedEvents.length} events could not be matched to Odds API:`);
    for (const ue of unmatchedEvents) {
      log.warn('Lines', `  ${ue.pxEvent}: ${ue.reason || `home=${ue.pxHome}→${ue.matchedHome || 'NO MATCH'}, away=${ue.pxAway}→${ue.matchedAway || 'NO MATCH'}`}`);
    }
  }

  // 6. Register matched lines with PX
  const lineIds = Object.keys(lineIndex);
  if (lineIds.length > 0) {
    try {
      await px.registerSupportedLines(lineIds);
      log.info('Lines', `Registered ${lineIds.length} lines with ProphetX`);
    } catch (err) {
      log.error('Lines', `Failed to register lines: ${err.message}`);
    }
  }

  lastSeedStats = {
    timestamp: new Date().toISOString(),
    totalEvents: events.length,
    totalLines,
    matchedLines,
    registeredLines: lineIds.length,
    unmatchedEvents: unmatchedEvents.length,
  };

  log.info('Lines', `=== Seed complete: ${events.length} events, ${totalLines} lines parsed, ${matchedLines} matched, ${lineIds.length} registered ===`);
  return lastSeedStats;
}

// ---------------------------------------------------------------------------
// LOOKUPS
// ---------------------------------------------------------------------------

function lookupLine(lineId) {
  return lineIndex[lineId] || null;
}

function getRegisteredLineIds() {
  return Object.keys(lineIndex);
}

function getStats() {
  return lastSeedStats;
}

function getLineCount() {
  return Object.keys(lineIndex).length;
}

/**
 * Get a summary of lines by sport and market type.
 */
function getLineSummary() {
  const summary = {};
  for (const [lineId, info] of Object.entries(lineIndex)) {
    const key = `${info.sport}/${info.marketType}`;
    summary[key] = (summary[key] || 0) + 1;
  }
  return summary;
}

// ---------------------------------------------------------------------------
// REFRESH
// ---------------------------------------------------------------------------

/**
 * Re-seed lines. Clears old index and re-fetches everything.
 */
async function refreshLines() {
  log.info('Lines', 'Refreshing all lines...');
  // Clear existing
  for (const key of Object.keys(lineIndex)) {
    delete lineIndex[key];
  }
  return seedAllLines();
}

/**
 * Resolve a tournament_id to a human-readable name.
 */
function getTournamentName(tournamentId) {
  const t = tournamentIndex[tournamentId];
  return t ? `${t.name} (${t.sport})` : null;
}

/**
 * Resolve a sport_event_id to event name.
 */
function getEventName(eventId) {
  const e = eventIndex[eventId];
  return e ? e.name : null;
}

module.exports = {
  seedAllLines,
  refreshLines,
  lookupLine,
  getRegisteredLineIds,
  getStats,
  getLineCount,
  getLineSummary,
  matchTeamName,
  normalizeTeamName,
  getTournamentName,
  getEventName,
};
