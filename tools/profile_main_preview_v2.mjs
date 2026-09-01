import { chromium } from 'playwright';

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext();
const page = await ctx.newPage();

const t0 = Date.now();
await page.goto('http://127.0.0.1:8765', { waitUntil: 'networkidle', timeout: 60000 });
const tAfterPageLoad = Date.now() - t0;
console.log(`page initial load (networkidle): ${tAfterPageLoad}ms`);

const tab = Date.now();
await page.click('[data-tab="main-preview-v2"]');
await page.waitForFunction(() => {
  const stage = document.querySelector('#main-preview-v2-stage canvas');
  return stage !== null;
}, null, { timeout: 60000 });
const tAfterTabRender = Date.now() - tab;
console.log(`v2 tab click → canvas appears: ${tAfterTabRender}ms`);

await page.waitForTimeout(2000);

const resources = await page.evaluate(() => {
  return performance.getEntriesByType('resource').map(e => ({
    name: e.name,
    duration: Math.round(e.duration),
    transferSize: e.transferSize,
    encodedBodySize: e.encodedBodySize,
    initiatorType: e.initiatorType,
  }));
});

console.log('\n=== top 20 slowest resources ===');
resources.sort((a, b) => b.duration - a.duration);
resources.slice(0, 20).forEach(r => {
  const url = r.name.length > 100 ? r.name.slice(0, 97) + '...' : r.name;
  console.log(`${String(r.duration).padStart(5)}ms  ${String(r.transferSize).padStart(8)}b  ${r.initiatorType.padEnd(10)}  ${url}`);
});

console.log('\n=== Luckysheet+CDN resources ===');
const cdn = resources.filter(r => r.name.includes('luckysheet') || r.name.includes('jsdelivr') || r.name.includes('unpkg'));
cdn.forEach(r => {
  const url = r.name.length > 100 ? r.name.slice(0, 97) + '...' : r.name;
  console.log(`${String(r.duration).padStart(5)}ms  ${String(r.transferSize).padStart(8)}b  ${url}`);
});
const cdnTotalBytes = cdn.reduce((s, r) => s + r.transferSize, 0);
console.log(`Luckysheet+CDN total: ${cdn.length} requests, ${(cdnTotalBytes/1024).toFixed(0)}KB`);

console.log('\n=== /api/main-file/preview ===');
const previewReqs = resources.filter(r => r.name.includes('/api/main-file/preview'));
previewReqs.forEach(r => console.log(`${r.duration}ms ${r.transferSize}b ${r.name}`));

// === 第二輪：模擬 F5 重新整理（backend cache warm 後）===
console.log('\n\n=== ROUND 2: F5 reload (warm backend cache) ===');
const t1 = Date.now();
await page.reload({ waitUntil: 'networkidle', timeout: 60000 });
console.log(`reload (networkidle): ${Date.now() - t1}ms`);

const tab2 = Date.now();
await page.click('[data-tab="main-preview-v2"]');
await page.waitForFunction(() => {
  const stage = document.querySelector('#main-preview-v2-stage canvas');
  return stage !== null;
}, null, { timeout: 60000 });
console.log(`v2 tab click → canvas: ${Date.now() - tab2}ms`);

const reload2Resources = await page.evaluate(() => {
  return performance.getEntriesByType('resource').map(e => ({
    name: e.name,
    duration: Math.round(e.duration),
    transferSize: e.transferSize,
  }));
});
console.log('\n=== ROUND 2 top 10 slowest ===');
reload2Resources.sort((a, b) => b.duration - a.duration);
reload2Resources.slice(0, 10).forEach(r => {
  const url = r.name.length > 100 ? r.name.slice(0, 97) + '...' : r.name;
  console.log(`${String(r.duration).padStart(5)}ms  ${String(r.transferSize).padStart(8)}b  ${url}`);
});

await browser.close();
