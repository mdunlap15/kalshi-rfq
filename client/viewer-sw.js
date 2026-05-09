// Service Worker for the Parlay SP Viewer PWA.
//
// Minimal — viewer accounts don't subscribe to push notifications and
// don't need offline caching beyond what the browser already does. The
// SW exists so iOS/Android treat the page as an installable PWA.
//
// Scope is set to /viewer/ via Service-Worker-Allowed header (viewer.html
// is at /viewer; the SW lives at /viewer/sw.js so it controls /viewer
// and any /viewer/* paths).

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

// Pass-through fetch handler. Chrome's PWA install eligibility check
// requires the SW to have a `fetch` listener that calls respondWith()
// (a no-op listener isn't always enough). We forward the request to
// the network unchanged — viewer.html doesn't need offline caching
// and live data must always hit the server.
self.addEventListener('fetch', (event) => {
  event.respondWith(fetch(event.request));
});
