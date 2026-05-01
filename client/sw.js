// Service Worker for Parlay SP PWA
// Handles push notifications, badge count, and per-category mute prefs.
//
// Categories from the server: 'confirmation', 'settlement', 'cap_hit',
// 'connection', 'daily_summary', 'test'. The PWA stores per-category
// mute toggles in IndexedDB under the 'sw-prefs' DB; if the category is
// muted, we suppress the user-visible notification but still update the
// badge count. Operator can flip toggles in Settings tab without re-
// installing the SW.

const PREF_DB = 'sw-prefs';
const PREF_STORE = 'mutes';

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

function openPrefDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(PREF_DB, 1);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(PREF_STORE)) {
        db.createObjectStore(PREF_STORE);
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function isCategoryMuted(category) {
  if (!category) return false;
  try {
    const db = await openPrefDb();
    return await new Promise((resolve) => {
      const tx = db.transaction(PREF_STORE, 'readonly');
      const req = tx.objectStore(PREF_STORE).get(category);
      req.onsuccess = () => resolve(req.result === true);
      req.onerror = () => resolve(false);
    });
  } catch {
    return false;
  }
}

// Push notification received
self.addEventListener('push', (event) => {
  const data = event.data ? event.data.json() : {};
  const title = data.title || 'Parlay SP';
  const category = data.category || 'unknown';

  event.waitUntil((async () => {
    // Update badge count regardless of mute (operator still wants to see
    // the count of unread events when they open the app).
    if (navigator.setAppBadge && data.badgeCount != null) {
      try { await navigator.setAppBadge(data.badgeCount); } catch {}
    }

    // Honor per-category mute prefs. Muted categories still update the
    // badge but skip the user-visible notification toast.
    const muted = await isCategoryMuted(category);
    if (muted) return;

    // Vibration pattern varies by category — settlements and cap-hits
    // get a longer pattern so they stand out from confirmations.
    const vibrate = (category === 'settlement' || category === 'cap_hit')
      ? [200, 100, 200, 100, 200]
      : [200, 100, 200];

    const options = {
      body: data.body || 'New parlay confirmed',
      icon: '/app/icon-192.svg',
      badge: '/app/icon-192.svg',
      tag: data.tag || (category + '-' + Date.now()),
      data: {
        url: data.url || '/app',
        parlayId: data.parlayId,
        category,
      },
      vibrate,
    };
    return self.registration.showNotification(title, options);
  })());
});

// Notification clicked — open the app or the specific parlay
self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  const url = event.notification.data?.url || '/app';

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
      // If app is already open, focus it and post a navigation message
      // so the SPA can switch to the right tab (e.g. #exposure).
      for (const client of clients) {
        if (client.url.includes('/app') && 'focus' in client) {
          try { client.postMessage({ type: 'navigate', url }); } catch {}
          return client.focus();
        }
      }
      // Otherwise open new window
      return self.clients.openWindow(url);
    })
  );
});
