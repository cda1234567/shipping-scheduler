import { chromium } from 'playwright';
const browser = await chromium.launch({ headless: true });
const page = await (await browser.newContext()).newPage();
await page.goto('http://127.0.0.1:8765', { waitUntil: 'networkidle', timeout: 60000 });
await page.click('[data-tab="main-preview-v2"]');
await page.waitForFunction(() => document.querySelector('#main-preview-v2-stage canvas') !== null, null, { timeout: 60000 });
await page.waitForTimeout(2000);

const before = await page.evaluate(() => {
  const el = document.querySelector('#main-preview-v2-stage .luckysheet-scrollbar-x');
  return { scrollLeft: el?.scrollLeft || 0, scrollWidth: el?.scrollWidth || 0, clientWidth: el?.clientWidth || 0 };
});
console.log('BEFORE:', before);

await page.click('#btn-main-preview-v2-scroll-right');
await page.waitForTimeout(800);

const after = await page.evaluate(() => {
  const el = document.querySelector('#main-preview-v2-stage .luckysheet-scrollbar-x');
  return { scrollLeft: el?.scrollLeft || 0, scrollWidth: el?.scrollWidth || 0, clientWidth: el?.clientWidth || 0 };
});
console.log('AFTER:', after);
console.log('scrolled?', after.scrollLeft > before.scrollLeft, 'maxed?', after.scrollLeft + after.clientWidth >= after.scrollWidth - 5);
await browser.close();
