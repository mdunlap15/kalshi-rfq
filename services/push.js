/**
 * Web Push notification service.
 * Sends push notifications to subscribed devices when parlays are confirmed.
 */
const webpush = require('web-push');
const log = require('./logger');

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

// In-memory subscription store (survives as long as process runs)
const subscriptions = new Map();
let unreadCount = 0;

function addSubscription(sub) {
  const key = sub.endpoint;
  subscriptions.set(key, sub);
  log.info('Push', `Subscription added (${subscriptions.size} total)`);
}

function removeSubscription(endpoint) {
  subscriptions.delete(endpoint);
}

/**
 * Send a push notification to all subscribed devices.
 */
async function sendNotification(payload) {
  if (!configured || subscriptions.size === 0) return;

  unreadCount++;
  const data = JSON.stringify({
    ...payload,
    badgeCount: unreadCount,
  });

  const stale = [];
  for (const [endpoint, sub] of subscriptions) {
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

/**
 * Send notification for a confirmed parlay.
 */
function notifyConfirmation(order) {
  const legs = order.legs || order.meta?.legs || [];
  const legCount = legs.length;
  const odds = order.confirmedOdds || order.offeredOdds;
  const oddsStr = odds ? (odds > 0 ? '+' + odds : '' + odds) : '';
  const stake = order.confirmedStake ? '$' + order.confirmedStake.toFixed(2) : '';
  const teams = legs.map(l => l.teamName || l.team || '?').slice(0, 3).join(', ');
  const more = legCount > 3 ? ` +${legCount - 3} more` : '';

  sendNotification({
    title: `Parlay Confirmed: ${legCount}-leg ${oddsStr}`,
    body: `${stake} stake | ${teams}${more}`,
    tag: 'confirm-' + order.parlayId,
    parlayId: order.parlayId,
    url: '/order/' + order.parlayId,
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

module.exports = {
  addSubscription,
  notifyConfirmation,
  resetBadge,
  getVapidPublicKey,
  getSubscriptionCount,
};
