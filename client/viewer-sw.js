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

// Pass-through fetch handler. Required for some browsers to consider the
// SW "valid" for PWA install eligibility, but doesn't intercept anything.
self.addEventListener('fetch', () => { /* network handles it */ });
