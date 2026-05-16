// Screenshot the Network Volume & Share card via puppeteer.
// Loads http://localhost:4099 (preview server must be running), waits for
// the dashboard to settle, expands the card, captures a tight crop.

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900, deviceScaleFactor: 2 });

  page.on('console', m => { if (m.type() === 'error') console.error('console:', m.text()); });

  console.log('Loading dashboard...');
  await page.goto('http://localhost:4099/', { waitUntil: 'networkidle2', timeout: 60000 });
  console.log('Page loaded');

  // Kill polling so screenshot is stable
  await page.evaluate(() => {
    for (let i = 1; i < 99999; i++) { try { clearInterval(i); } catch(_){} }
  });

  // Switch to Analytics tab
  await page.evaluate(() => {
    const nav = [...document.querySelectorAll('.nav-item, [onclick*=section]')].find(el => el.textContent.includes('Analytics'));
    if (nav) nav.click();
  });

  // Expand the netshare card
  await page.evaluate(() => {
    const card = document.getElementById('netshare-card');
    if (card?.classList.contains('collapsed')) {
      const header = card.previousElementSibling;
      if (header) header.click();
    }
  });

  // Wait for the charts to render (loadNetworkShareDaily takes ~30s on cold cache)
  console.log('Waiting for charts to render...');
  await page.waitForFunction(() => {
    return !!document.querySelector('#chart-netshare-quotes svg')
        && !!document.querySelector('#chart-netshare-matched svg')
        && !!document.querySelector('#chart-netshare-share svg')
        && !!document.querySelector('#netshare-table table')
        && !!document.querySelector('#chart-netshare-hourly-vol svg')
        && !!document.querySelector('#chart-netshare-hourly-trend svg');
  }, { timeout: 180000 });
  console.log('Charts rendered');

  // Scroll the card into view
  await page.evaluate(() => {
    document.getElementById('netshare-card').scrollIntoView({ block: 'start' });
  });
  await new Promise(r => setTimeout(r, 500));

  // Get the card's bounding box for a tight crop
  const cardHandle = await page.$('#netshare-card');
  const cardHeader = await page.$('xpath/.//div[contains(@class,"card-header") and contains(., "Network Volume")]');
  let clip = null;
  if (cardHandle) {
    const b1 = await cardHandle.boundingBox();
    const b2 = cardHeader ? await cardHeader.boundingBox() : null;
    if (b1) {
      clip = {
        x: Math.max(0, (b2?.x || b1.x) - 8),
        y: Math.max(0, (b2?.y || b1.y) - 8),
        width: Math.min(1280, b1.width + 16),
        height: Math.min(2400, b1.height + (b2?.height || 0) + 16),
      };
    }
  }
  console.log('Crop:', JSON.stringify(clip));

  const outPath = path.join(__dirname, '..', 'netshare_preview.png');
  await page.screenshot({ path: outPath, clip: clip || { x: 0, y: 0, width: 1280, height: 1800 } });
  console.log('Screenshot saved:', outPath);

  await browser.close();
})().catch(e => { console.error('FATAL:', e); process.exit(1); });
