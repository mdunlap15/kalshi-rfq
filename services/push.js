/**
 * Web Push notification service.
 * Sends push notifications to subscribed devices for parlay confirmations,
 * settlements, exposure cap hits, system events (disconnects/restarts),
 * and end-of-day summaries.
 *
 * Each notification category can be debounced to avoid spam — cap-hit
 * notifications in particular fire many times per minute under load.
 */
const webpush = require('web-push');
const log = require('./logger');
const db = require('./db');

// VAPID keys — set in env vars or use defaults
const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY || 'BMwBpGMMSuF0Jf6OaYukUM6zPZp3XTEiW4mAomeuQpZxMvtSab3Eh45pLrmbIrHpzhOmpuMipHscIhVC6cbP0iw';
const VAPID_PRIVATE = process.env.VAPID_PRIVATE_KEY || 'tKAsKAiXPBw8pl8_fyc06zLkbE2dLZz0UZnXBKuJRUU';

let configured = false;
try {
  webpush.setVapidDetails('mailto:noreply@prophetx-sp.com', VAPID_PUBLIC, VAPID_PRIVATE);
  configured = true;
} catch (err) {
  log.warn('Push', `VAPID setup failed: ${err.message}`);
}

// In-memory subscription store, hydrated from Supabase at boot so
// notifications survive Railway redeploys. Previously pure in-memory,
// meaning every deploy silently wiped every subscription and operators
// had to re-enable notifications in their browser.
const subscriptions = new Map();
// Per-endpoint mute state. The PWA POSTs the user's per-category mute
// prefs to /push/subscribe alongside the subscription itself; we store
// them here and use them to skip sends in sendNotification(). This is
// the authoritative gate — relying on the SW's IndexedDB read inside
// the push event was unreliable across SW versions and iOS PWA update
// cadences. The SW-side check stays as defense-in-depth.
const mutedByEndpoint = new Map(); // endpoint -> Set<category>
let unreadCount = 0;
let hydrated = false;

// Per-category debounce timestamps (Map<category, lastSentMs>) so cap-hit
// floods don't push 50 notifications/min. notifyConfirmation/notifySettlement
// are NOT debounced — those are 1-per-event by nature.
const lastSentByCategory = new Map();
const DEBOUNCE_MS = {
  cap_hit: 5 * 60 * 1000,        // 5 min between same-category cap-hit pushes
  connection: 60 * 1000,          // 1 min between connection-state pushes
  daily_summary: 12 * 3600 * 1000,// 12h between summary pushes (1/day)
};

async function hydrateFromDb() {
  if (hydrated) return;
  hydrated = true;
  try {
    const saved = await db.loadPushSubscriptions();
    for (const sub of saved) {
      if (sub && sub.endpoint) subscriptions.set(sub.endpoint, sub);
    }
    if (saved.length > 0) {
      log.info('Push', `Hydrated ${saved.length} subscriptions from Supabase`);
    }
  } catch (err) {
    log.warn('Push', `Subscription hydrate failed: ${err.message}`);
  }
}

function addSubscription(sub) {
  const key = sub.endpoint;
  subscriptions.set(key, sub);
  log.info('Push', `Subscription added (${subscriptions.size} total)`);
  // Persist to Supabase so it survives Railway redeploys. Fire and
  // forget — the subscription is already in memory and will work even
  // if the DB write fails.
  db.savePushSubscription(sub).catch(err =>
    log.warn('Push', `savePushSubscription failed: ${err.message}`));
}

function removeSubscription(endpoint) {
  subscriptions.delete(endpoint);
  mutedByEndpoint.delete(endpoint);
  db.deletePushSubscription(endpoint).catch(err =>
    log.warn('Push', `deletePushSubscription failed: ${err.message}`));
}

/**
 * Update the muted-categories list for a subscription endpoint. Called
 * from /push/mute-prefs whenever the operator flips a toggle in the
 * Settings tab. Categories not in the array are considered enabled.
 * Pass null/undefined for categories to clear all mutes for that
 * endpoint (re-enable everything).
 */
function setMutedCategories(endpoint, categories) {
  if (!endpoint) return;
  if (!Array.isArray(categories) || categories.length === 0) {
    mutedByEndpoint.delete(endpoint);
    return;
  }
  mutedByEndpoint.set(endpoint, new Set(categories));
}

function getMutedCategories(endpoint) {
  const s = mutedByEndpoint.get(endpoint);
  return s ? [...s] : [];
}

/**
 * Send a push notification to all subscribed devices.
 * `category` allows the SW + client to filter by user prefs (the SW
 * receives the category in payload.category and respects per-category
 * mute toggles persisted in IndexedDB).
 */
async function sendNotification(payload) {
  if (!configured || subscriptions.size === 0) return;

  // Bump the unread badge count IF at least one subscriber will actually
  // receive this push. If all subscribers have muted this category, skip
  // the bump too — otherwise the badge would inch up forever for muted
  // notifications, which is the same UX problem as showing the toast.
  const category = payload.category || null;
  let anyEligible = false;
  for (const [endpoint] of subscriptions) {
    const muted = mutedByEndpoint.get(endpoint);
    if (!category || !muted || !muted.has(category)) { anyEligible = true; break; }
  }
  if (!anyEligible) {
    log.debug('Push', `All subscribers have muted category "${category}" — skipping send`);
    return;
  }

  unreadCount++;
  const data = JSON.stringify({
    ...payload,
    badgeCount: unreadCount,
    sentAt: new Date().toISOString(),
  });

  const stale = [];
  for (const [endpoint, sub] of subscriptions) {
    // Per-endpoint mute gate. Authoritative — independent of whatever
    // SW version the client has installed.
    const muted = mutedByEndpoint.get(endpoint);
    if (category && muted && muted.has(category)) {
      log.debug('Push', `Endpoint muted "${category}" — skipping`);
      continue;
    }
    try {
      await webpush.sendNotification(sub, data);
    } catch (err) {
      if (err.statusCode === 410 || err.statusCode === 404) {
        // Subscription expired
        stale.push(endpoint);
      } else {
        log.warn('Push', `Send failed (${err.statusCode}): ${err.message}`);
      }
    }
  }
  for (const ep of stale) removeSubscription(ep);
}

// Debounced wrapper — drops the call when the same category fired within
// the configured window. Returns true if sent, false if dropped.
function maybeSend(category, payload) {
  const window = DEBOUNCE_MS[category];
  if (window) {
    const last = lastSentByCategory.get(category) || 0;
    if (Date.now() - last < window) return false;
    lastSentByCategory.set(category, Date.now());
  }
  sendNotification({ ...payload, category });
  return true;
}

/**
 * Format a single leg as a short human-readable description.
 * Examples:
 *   moneyline   → "Atlanta Braves ML"
 *   spread      → "Atlanta Braves -3.5"
 *   total       → "Over 8.5"
 *   player prop → "Mookie Betts Hits Over 1.5"
 */
const PROP_TYPE_LABEL = {
  player_hitter_hits: 'Hits',
  player_hitter_hr: 'HR',
  player_hitter_total_bases: 'TB',
  player_hitter_rbi_runs: 'RBI',
  player_strikeouts: 'Ks',
  player_points: 'Pts',
  player_rebounds: 'Reb',
  player_assists: 'Ast',
  player_threes_made: '3PM',
  player_shots_on_goal: 'SOG',
  player_goals: 'G',
};
function _formatLeg(l) {
  if (!l) return '?';
  const team = l.teamName || l.team || '';
  const market = l.market || l.marketType || '';
  const selection = (l.selection || '').toLowerCase();
  const line = l.line;
  // Player props: team holds the player name; market like player_hitter_hits.
  if (market.startsWith('player_')) {
    const stat = PROP_TYPE_LABEL[market] || market.replace(/^player_/, '').replace(/_/g, ' ');
    const side = selection === 'over' ? 'Over' : selection === 'under' ? 'Under' : '';
    const lineStr = line != null ? ' ' + line : '';
    return `${team} ${stat} ${side}${lineStr}`.trim();
  }
  // Moneyline (full match / DNB)
  if (market === 'moneyline') {
    return `${team} ML`.trim();
  }
  // Spread — selection determines sign of the displayed line. PX legs store
  // the leg's line from the selection's perspective so usually `line` is
  // already signed correctly. Show with explicit + or -.
  if (market === 'spread') {
    if (line == null) return `${team} spread`.trim();
    const signed = line > 0 ? `+${line}` : `${line}`;
    return `${team} ${signed}`.trim();
  }
  // Total — over/under + line. Team field is usually null; if it is set
  // (team_total), include it.
  if (market === 'total' || market === 'team_total') {
    const side = selection === 'over' ? 'Over' : selection === 'under' ? 'Under' : '';
    const lineStr = line != null ? ` ${line}` : '';
    const teamPrefix = team ? `${team} ` : '';
    return `${teamPrefix}${side}${lineStr}`.trim();
  }
  // Series winner / first-half / fallthrough — keep it simple.
  return [team, market, selection, line].filter(x => x != null && x !== '').join(' ');
}

/**
 * Send notification for a confirmed parlay.
 */
function notifyConfirmation(order) {
  const legs = order.legs || order.meta?.legs || [];
  const legCount = legs.length;
  const odds = order.confirmedOdds || order.offeredOdds;
  const oddsStr = odds ? (odds > 0 ? '+' + odds : '' + odds) : '';
  const stake = order.confirmedStake ? '$' + order.confirmedStake.toFixed(2) : '';
  // List every leg with full detail (player/team + market + selection + line).
  // Long parlays get one leg per line so the operator can see what was
  // taken without opening the dashboard. iOS/Android auto-truncate as
  // needed; the full string is still attached to the notification.
  const legLines = legs.map(_formatLeg);
  const body = `${stake} stake\n` + legLines.map(s => '• ' + s).join('\n');

  sendNotification({
    title: `Parlay Confirmed: ${legCount}-leg ${oddsStr}`,
    body,
    tag: 'confirm-' + order.parlayId,
    parlayId: order.parlayId,
    category: 'confirmation',
    // Open the PWA, not the /order/:id JSON endpoint. Previously tapping
    // a confirmation notification dumped raw JSON into the browser.
    url: '/app',
  });
}

/**
 * Send notification when a confirmed parlay settles.
 * Pushes per settlement so the operator gets immediate feedback on
 * wins/losses without watching the dashboard.
 */
function notifySettlement(order) {
  if (!order || !order.settlementResult) return;
  const result = order.settlementResult; // 'won' | 'lost' | 'push' | 'void'
  const pnl = order.pnl || 0;
  const pnlStr = pnl >= 0 ? '+$' + pnl.toFixed(2) : '-$' + Math.abs(pnl).toFixed(2);
  const legs = order.legs || order.meta?.legs || [];
  const legCount = legs.length;
  const odds = order.confirmedOdds || order.offeredOdds;
  const oddsStr = odds ? (odds > 0 ? '+' + odds : '' + odds) : '';
  // SP perspective: 'won' = we kept bettor's wager (good); 'lost' = we paid out (bad)
  const emoji = result === 'won' ? '✓' : result === 'lost' ? '✗' : '◯';
  const titlePrefix = result === 'won' ? 'Won' : result === 'lost' ? 'Lost' : result === 'push' ? 'Push' : 'Void';

  sendNotification({
    title: `${emoji} ${titlePrefix}: ${pnlStr}`,
    body: `${legCount}-leg ${oddsStr} settled ${result}`,
    tag: 'settle-' + order.parlayId,
    parlayId: order.parlayId,
    category: 'settlement',
    url: '/app',
  });
}

/**
 * Send notification when an exposure cap blocks an RFQ.
 * Categories: 'team', 'player', 'game', 'parlay_risk', 'portfolio'.
 * Debounced 5 min per (cap_type) so a fast RFQ flow doesn't spam.
 */
function notifyCapHit(capType, details) {
  const subj = details.subject || '';
  const limit = details.limit ? '$' + Math.round(details.limit) : '';
  const current = details.current != null ? '$' + Math.round(details.current) : '';
  let title = '';
  let body = '';
  switch (capType) {
    case 'team':
      title = `Team Cap: ${subj}`;
      body = `Exposure ${current} ≥ ${limit} — blocking ${subj} RFQs`;
      break;
    case 'player':
      title = `Player Cap: ${subj}`;
      body = `Exposure ${current} ≥ ${limit} — blocking ${subj} prop RFQs`;
      break;
    case 'game':
      title = `Game Cap: ${subj}`;
      body = `Game exposure ${current} ≥ ${limit}`;
      break;
    case 'parlay_risk':
      title = `Parlay Risk Cap`;
      body = `Single-parlay risk ${current} > ${limit}`;
      break;
    case 'portfolio':
      title = `Portfolio Drawdown`;
      body = `Open exposure hit drawdown limit ${limit}`;
      break;
    default:
      title = `Cap Hit: ${capType}`;
      body = `${subj} ${current}/${limit}`;
  }
  // Debounce key includes cap type + subject so same-team repeat hits dedupe
  // but different teams get separate notifications.
  const dbKey = `cap_hit_${capType}_${subj}`;
  const last = lastSentByCategory.get(dbKey) || 0;
  if (Date.now() - last < (DEBOUNCE_MS.cap_hit || 0)) return false;
  lastSentByCategory.set(dbKey, Date.now());
  sendNotification({
    title,
    body,
    tag: dbKey,
    category: 'cap_hit',
    capType,
    subject: subj,
    url: '/app#exposure',
  });
  return true;
}

/**
 * Send notification on connection state changes (websocket disconnect,
 * reconnect, restart). Debounced 1min so flapping doesn't spam.
 */
function notifyConnectionState(state, reason) {
  let title = '';
  let body = '';
  switch (state) {
    case 'disconnected':
      title = '⚠ WebSocket Disconnected';
      body = reason || 'PX RFQ stream lost — service paused';
      break;
    case 'reconnected':
      title = '✓ WebSocket Reconnected';
      body = reason || 'PX RFQ stream restored';
      break;
    case 'restarted':
      title = 'Service Restarted';
      body = reason || 'Parlay SP restarted';
      break;
    case 'paused':
      title = '⏸ Service Paused';
      body = reason || 'RFQ handling paused (manual)';
      break;
    case 'resumed':
      title = '▶ Service Resumed';
      body = reason || 'RFQ handling resumed';
      break;
    default:
      title = `Connection: ${state}`;
      body = reason || '';
  }
  return maybeSend('connection', {
    title, body, tag: `conn-${state}`, url: '/app',
  });
}

/**
 * Send end-of-day summary notification with fills + P&L.
 */
function notifyDailySummary(summary) {
  if (!summary) return;
  const pnl = summary.pnl || 0;
  const pnlStr = pnl >= 0 ? '+$' + pnl.toFixed(0) : '-$' + Math.abs(pnl).toFixed(0);
  const fills = summary.fills || 0;
  const wins = summary.wins || 0;
  const losses = summary.losses || 0;
  const winRate = fills > 0 ? Math.round((wins / (wins + losses)) * 100) : 0;
  return maybeSend('daily_summary', {
    title: `Day Wrap: ${pnlStr} (${fills} fills)`,
    body: `${wins}W / ${losses}L · ${winRate}% SP win rate`,
    tag: 'daily-summary',
    url: '/app',
  });
}

function resetBadge() {
  unreadCount = 0;
}

function getVapidPublicKey() {
  return VAPID_PUBLIC;
}

function getSubscriptionCount() {
  return subscriptions.size;
}

// Diagnostic — operator can hit /push/test to send a sample of each
// category and verify that the SW handlers + iOS install + browser
// permissions all work. Safe to call at any time.
function sendTestNotification(category) {
  const cat = category || 'test';
  sendNotification({
    title: `Test Notification (${cat})`,
    body: `Triggered at ${new Date().toLocaleTimeString('en-US', { timeZone: 'America/New_York' })} ET`,
    tag: 'test-' + Date.now(),
    category: cat,
    url: '/app',
  });
}

module.exports = {
  addSubscription,
  removeSubscription,
  setMutedCategories,
  getMutedCategories,
  notifyConfirmation,
  notifySettlement,
  notifyCapHit,
  notifyConnectionState,
  notifyDailySummary,
  sendTestNotification,
  resetBadge,
  getVapidPublicKey,
  getSubscriptionCount,
  hydrateFromDb,
};
