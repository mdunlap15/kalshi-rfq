// Service Worker for Parlay SP PWA
// Handles push notifications and badge count

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

// Push notification received
self.addEventListener('push', (event) => {
  const data = event.data ? event.data.json() : {};
  const title = data.title || 'Parlay SP';
  const options = {
    body: data.body || 'New parlay confirmed',
    icon: '/app/icon-192.svg',
    badge: '/app/icon-192.svg',
    tag: data.tag || 'parlay-' + Date.now(),
    data: {
      url: data.url || '/app',
      parlayId: data.parlayId,
    },
    vibrate: [200, 100, 200],
  };

  // Update badge count
  if (navigator.setAppBadge && data.badgeCount != null) {
    navigator.setAppBadge(data.badgeCount);
  }

  event.waitUntil(self.registration.showNotification(title, options));
});

// Notification clicked — open the app or the specific parlay
self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  const url = event.notification.data?.url || '/app';

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
      // If app is already open, focus it
      for (const client of clients) {
        if (client.url.includes('/app') && 'focus' in client) {
          return client.focus();
        }
      }
      // Otherwise open new window
      return self.clients.openWindow(url);
    })
  );
});
