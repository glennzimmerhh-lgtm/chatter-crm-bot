// ZF CRM Service Worker — PWA + Web Push
self.addEventListener('install', (e) => { self.skipWaiting(); });
self.addEventListener('activate', (e) => { e.waitUntil(self.clients.claim()); });

self.addEventListener('push', (e) => {
  let d = { title: 'ZF CRM', body: '', url: '/', tag: 'zf' };
  try { d = Object.assign(d, e.data.json()); }
  catch (_) { if (e.data) { try { d.body = e.data.text(); } catch (__) {} } }
  e.waitUntil(self.registration.showNotification(d.title, {
    body: d.body,
    icon: '/icon-192.png',
    badge: '/icon-192.png',
    tag: d.tag || 'zf',
    renotify: true,
    vibrate: [80, 40, 80],
    data: { url: d.url || '/' }
  }));
});

self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const url = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((cl) => {
    for (const c of cl) { if ('focus' in c) { try { c.navigate(url); } catch (_) {} return c.focus(); } }
    if (self.clients.openWindow) return self.clients.openWindow(url);
  }));
});
