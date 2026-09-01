import { chromium } from 'playwright';
const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext();
const page = await ctx.newPage();
await page.goto('http://127.0.0.1:8765', { waitUntil: 'networkidle', timeout: 60000 });
await page.click('[data-tab="main-preview-v2"]');
await page.waitForTimeout(15000);
const scrollbarHtml = await page.evaluate(() => {
  const items = [];
  document.querySelectorAll('[class*="scrollbar"], [class*="scroll-bar"], [class*="scroll_bar"]').forEach(el => {
    items.push({ class: el.className, tag: el.tagName, parent: el.parentElement?.className || '' });
  });
  return items.slice(0, 40);
});
console.log(JSON.stringify(scrollbarHtml, null, 2));
await browser.close();
