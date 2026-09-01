import { chromium } from 'playwright';
const browser = await chromium.launch({ headless: true });
const page = await (await browser.newContext()).newPage();
await page.goto('http://127.0.0.1:8765', { waitUntil: 'networkidle', timeout: 60000 });
await page.waitForTimeout(3000);
const result = await page.evaluate(async () => {
  // 直接執行模組內的 builder
  const mod = await import('/static/modules/schedule.js');
  return {
    keys: Object.keys(mod),
  };
});
console.log('module exports:', result.keys);
// 切到「補料」tab，然後計算實際渲染的 shortage card 數量
await page.click('[data-right-panel-tab="shortages"]').catch(() => {});
await page.waitForTimeout(1000);
const items = await page.evaluate(() => {
  const cards = document.querySelectorAll('#right-scroll .shortage-item');
  const list = [];
  cards.forEach(c => {
    const part = c.getAttribute('data-part') || c.querySelector('.part')?.textContent?.trim();
    list.push(part);
  });
  return { count: cards.length, parts: list.slice(0, 50) };
});
console.log('rendered shortage cards:', items.count);
console.log('parts:', items.parts);

// 抓 main-file/data response 看 _stock
const stockData = await page.evaluate(async () => {
  const r = await fetch('/api/main-file/data');
  const d = await r.json();
  return { stockSize: Object.keys(d.stock || {}).length, sample: Object.entries(d.stock || {}).slice(0,3) };
});
console.log('main-file/data stock:', stockData);
await browser.close();
