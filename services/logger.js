/**
 * Async-buffered logger.
 *
 * Prior version: each log call synchronously invoked console.log/warn/error,
 * which writes to stdout/stderr pipes. On Railway (and most hosting platforms)
 * those pipes are consumed by a log aggregator that can back-pressure under
 * load, causing synchronous write() calls to block the caller — 1-5ms per
 * call when it happens. For a service that fires log.info on every RFQ
 * submit, that's real hot-path latency.
 *
 * Current version: append to an in-process buffer, flush via setImmediate
 * (runs after the current JS tick completes but before I/O polling). The
 * caller never waits for stdout. Under steady state: near-zero hot-path
 * cost. Under burst load: messages coalesce into fewer, larger writes.
 *
 * API is unchanged — all existing log.info/debug/warn/error callers work.
 *
 * Trade-offs:
 *   - If the process is killed (SIGKILL) with queued messages, those logs
 *     are lost. SIGTERM flushes via the beforeExit handler.
 *   - Order is preserved (single FIFO queue, single flush path).
 *   - Errors (stderr) and other levels (stdout) are batched separately so
 *     they emit to the correct stream.
 */

const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };

let currentLevel = LEVELS.info;

function setLevel(level) {
  currentLevel = LEVELS[level] || LEVELS.info;
}

function timestamp() {
  return new Date().toISOString();
}

// Separate queues for stdout and stderr so we don't mix streams. When
// either has pending data we schedule a flush on the next microtask.
const stdoutQueue = [];
const stderrQueue = [];
let flushScheduled = false;

function scheduleFlush() {
  if (flushScheduled) return;
  flushScheduled = true;
  // setImmediate runs after the current tick's synchronous work finishes
  // but before the next poll phase. Effectively off the hot path.
  setImmediate(flushNow);
}

function flushNow() {
  flushScheduled = false;
  if (stdoutQueue.length > 0) {
    // Single write per flush — fewer syscalls than one-per-message.
    const chunk = stdoutQueue.join('\n') + '\n';
    stdoutQueue.length = 0;
    try { process.stdout.write(chunk); } catch (e) { /* best-effort */ }
  }
  if (stderrQueue.length > 0) {
    const chunk = stderrQueue.join('\n') + '\n';
    stderrQueue.length = 0;
    try { process.stderr.write(chunk); } catch (e) { /* best-effort */ }
  }
}

// Flush any pending logs before the process exits so we don't drop the
// final messages (e.g. crash stack traces). SIGKILL still loses messages
// — nothing we can do about that.
process.on('beforeExit', flushNow);
process.on('SIGTERM', () => { flushNow(); process.exit(0); });
process.on('SIGINT',  () => { flushNow(); process.exit(0); });

function log(level, category, message, data) {
  if (LEVELS[level] < currentLevel) return;

  // Build the line lazily; if we filtered above, we don't pay this cost.
  const prefix = `${timestamp()} [${category}]`;
  const line = data !== undefined
    ? `${prefix} ${message} ${typeof data === 'object' ? JSON.stringify(data) : data}`
    : `${prefix} ${message}`;

  // Errors go to stderr; everything else stdout. Matches the prior behavior
  // of using console.error vs console.log/warn.
  if (level === 'error') {
    stderrQueue.push(line);
  } else {
    stdoutQueue.push(line);
  }
  scheduleFlush();
}

module.exports = {
  setLevel,
  debug: (cat, msg, data) => log('debug', cat, msg, data),
  info:  (cat, msg, data) => log('info', cat, msg, data),
  warn:  (cat, msg, data) => log('warn', cat, msg, data),
  error: (cat, msg, data) => log('error', cat, msg, data),
  // Exposed for tests + emergency manual flushing
  _flushNow: flushNow,
};
