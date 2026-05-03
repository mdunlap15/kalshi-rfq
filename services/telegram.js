/**
 * Telegram bot notification service.
 *
 * Setup (one-time, ~3 minutes):
 *   1. In Telegram, search @BotFather and start a chat
 *   2. Send /newbot, follow prompts, choose a name + username
 *   3. BotFather replies with a token like "1234567890:ABCdef..."
 *      → set as TELEGRAM_BOT_TOKEN env var (Railway)
 *   4. Open a chat with your new bot, send any message ("hi")
 *   5. Visit https://api.telegram.org/bot<TOKEN>/getUpdates
 *      → find "chat":{"id":123456789} — that's your chat_id
 *      → set as TELEGRAM_CHAT_ID env var (Railway)
 *
 * If either env var is missing, sendMessage no-ops gracefully (logs once
 * at boot, then silent) so the rest of the service runs unchanged.
 */
const fetch = require('node-fetch');
const log = require('./logger');

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || null;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID || null;

let warned = false;
function _warnIfUnconfigured() {
  if (warned) return false;
  warned = true;
  if (!BOT_TOKEN || !CHAT_ID) {
    log.warn('Telegram', `Not configured — set TELEGRAM_BOT_TOKEN${!CHAT_ID ? ' and TELEGRAM_CHAT_ID' : ''} to enable`);
    return false;
  }
  return true;
}

/**
 * Send a Telegram message. Markdown supported (set parse_mode).
 * Returns { ok, messageId } on success, { ok: false, error } on failure.
 * Never throws — caller can fire-and-forget.
 */
async function sendMessage(text, opts = {}) {
  if (!BOT_TOKEN || !CHAT_ID) {
    _warnIfUnconfigured();
    return { ok: false, error: 'not_configured' };
  }
  if (!text || typeof text !== 'string') {
    return { ok: false, error: 'text required' };
  }
  // Telegram caps at 4096 chars per message; truncate with a marker.
  const MAX = 4000;
  const body = text.length > MAX ? text.slice(0, MAX) + '\n…[truncated]' : text;
  try {
    const resp = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: CHAT_ID,
        text: body,
        parse_mode: opts.parseMode || 'Markdown',
        disable_web_page_preview: opts.disableWebPreview !== false,
      }),
      timeout: 10000,
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok || !data.ok) {
      log.warn('Telegram', `sendMessage failed: ${resp.status} ${JSON.stringify(data).slice(0, 200)}`);
      return { ok: false, error: data.description || `HTTP ${resp.status}` };
    }
    return { ok: true, messageId: data.result?.message_id };
  } catch (err) {
    log.warn('Telegram', `sendMessage error: ${err.message}`);
    return { ok: false, error: err.message };
  }
}

function isConfigured() {
  return Boolean(BOT_TOKEN && CHAT_ID);
}

module.exports = { sendMessage, isConfigured };
