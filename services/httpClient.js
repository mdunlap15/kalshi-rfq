/**
 * Global HTTP client configuration.
 *
 * Switches Node's global `fetch` onto an undici Agent with:
 *   - Keep-alive enabled (socket reuse across calls — no TLS handshake per request)
 *   - Large-enough connection pool for bursty RFQ traffic
 *   - TCP_NODELAY (default — disables Nagle's algorithm, critical for small RFQ payloads
 *     where a 40ms delay waiting to coalesce writes would blow latency)
 *   - HTTP/2 negotiated automatically when the server (e.g. CloudFront) supports it
 *
 * Must be required ONCE at process bootstrap, before any module that makes fetch() calls.
 * After this runs, every `fetch(...)` anywhere in the app uses these settings.
 *
 * Replaces the old per-module http.Agent / https.Agent keep-alive plumbing in
 * prophetx.js (applied uniformly now to odds-feed and any other fetch caller too).
 *
 * Part of Phase-1 latency plan: S3 (undici) + S7 (keep-alive on Odds API) + S9 (TCP_NODELAY).
 */

const { Agent, setGlobalDispatcher } = require('undici');

const dispatcher = new Agent({
  // Keep idle sockets alive for 30s. After that they close naturally.
  keepAliveTimeout: 30_000,
  // Hard ceiling on a single socket's lifetime: 10 min. Stops a stale socket
  // from being reused forever if the upstream silently stops responding.
  keepAliveMaxTimeout: 600_000,
  // Max concurrent sockets per origin. RFQ traffic is bursty; PX may
  // occasionally issue several simultaneous confirms.
  connections: 50,
  // 1 = standard HTTP/1.1 keep-alive semantics. Works fine with CloudFront
  // and doesn't require negotiating pipelining.
  pipelining: 1,
  // TCP_NODELAY is enabled by default in undici's connect options; documenting
  // here so future-us doesn't forget: small request bodies (our offer submissions)
  // must not wait for Nagle's algorithm to batch. Do NOT override.
  // connect: { noDelay: true }  ← already the default
});

setGlobalDispatcher(dispatcher);

module.exports = { dispatcher };
