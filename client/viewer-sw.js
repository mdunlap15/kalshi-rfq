// Service Worker for the Parlay SP Viewer PWA.
//
// Handles:
//   - PWA install eligibility (fetch listener with respondWith)
//   - Push notifications (parlay confirmations / settlements broadcast
//     by the server's /push/* endpoints)
//   - App icon badge count via navigator.setAppBadge
//   - Notification clicks → focus the viewer if open, otherwise open it

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

// Pass-through fetch handler — required for Chrome's PWA install
// eligibility check. Doesn't intercept anything.
self.addEventListener('fetch', (event) => {
  event.respondWith(fetch(event.request));
});

// Server push payload shape (set by services/push.js):
//   { title, body, tag, parlayId, category, url, badgeCount, sentAt }
self.addEventListener('push', (event) => {
  const data = event.data ? event.data.json() : {};
  const title = data.title || 'Parlay SP';

  event.waitUntil((async () => {
    // Update the home-screen icon badge regardless of category. Viewers
    // see the unread count grow on the icon until they open the app.
    if (navigator.setAppBadge && data.badgeCount != null) {
      try { await navigator.setAppBadge(data.badgeCount); } catch (_) { /* ignore */ }
    }

    // Vibration: longer pattern for settlement / cap-hit so they stand
    // out from routine confirmations.
    const category = data.category || 'unknown';
    const vibrate = (category === 'settlement' || category === 'cap_hit')
      ? [200, 100, 200, 100, 200]
      : [200, 100, 200];

    const options = {
      body: data.body || 'New parlay activity',
      icon: '/viewer/icon-192.svg',
      badge: '/viewer/icon-192.svg',
      tag: data.tag || (category + '-' + Date.now()),
      data: {
        url: data.url || '/viewer',
        parlayId: data.parlayId,
        category,
      },
      vibrate,
    };
    return self.registration.showNotification(title, options);
  })());
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  // Always navigate to /viewer for viewer-PWA notifications. Server
  // sends url:'/app' for admin pushes; we override here so taps from
  // the viewer don't try to load an admin-only path (which 403s for
  // viewer accounts and yields a confusing error page).
  const targetUrl = '/viewer';

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
      // Reuse an existing viewer window if open.
      for (const client of clients) {
        if (client.url.includes('/viewer') && 'focus' in client) {
          try { client.postMessage({ type: 'navigate', url: targetUrl }); } catch (_) { /* ignore */ }
          return client.focus();
        }
      }
      return self.clients.openWindow(targetUrl);
    })
  );
});

// When a viewer client comes to the foreground, clear the icon badge.
// Fires when viewer.html sends 'badge-clear' via postMessage on tab focus.
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'badge-clear') {
    if (navigator.clearAppBadge) {
      navigator.clearAppBadge().catch(() => { /* ignore */ });
    } else if (navigator.setAppBadge) {
      navigator.setAppBadge(0).catch(() => { /* ignore */ });
    }
  }
});
