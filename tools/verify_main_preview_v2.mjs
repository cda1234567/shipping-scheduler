import { chromium } from 'playwright';

const BASE_URL = 'http://127.0.0.1:8765';
const errors = [];
const networkFailures = [];
const consoleMessages = [];

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext();
const page = await context.newPage();

page.on('console', msg => {
  const text = msg.text();
  consoleMessages.push(`[${msg.type()}] ${text}`);
  if (msg.type() === 'error') errors.push(text);
});
page.on('pageerror', err => errors.push(`PAGEERROR: ${err.message}`));
page.on('requestfailed', req => {
  networkFailures.push(`${req.failure()?.errorText} ${req.url()}`);
});

console.log('opening', BASE_URL);
await page.goto(BASE_URL, { waitUntil: 'networkidle', timeout: 60000 });
console.log('initial load OK');

const v2BtnVisible = await page.locator('[data-tab="main-preview-v2"]').isVisible();
console.log('v2 tab button visible:', v2BtnVisible);

if (!v2BtnVisible) {
  console.log('FAIL: v2 tab button not found');
  await browser.close();
  process.exit(1);
}

await page.click('[data-tab="main-preview-v2"]');
console.log('clicked v2 tab');

// Wait a bit for Univer to load + render
await page.waitForTimeout(15000);

const stage = page.locator('#main-preview-v2-stage');
const stageHTML = await stage.innerHTML();
console.log('stage HTML length:', stageHTML.length);
console.log('stage HTML preview (first 400):', stageHTML.slice(0, 400));

const univerCanvas = await page.locator('#main-preview-v2-stage canvas').count();
console.log('canvas count in v2 stage:', univerCanvas);

const sheetSelectOptions = await page.locator('#main-preview-v2-sheet-select option').count();
console.log('sheet select options count:', sheetSelectOptions);

console.log('\n=== console messages (last 30) ===');
consoleMessages.slice(-30).forEach(m => console.log(m));

console.log('\n=== page errors ===');
errors.forEach(e => console.log(e));

console.log('\n=== network failures ===');
networkFailures.forEach(f => console.log(f));

// v1 已移除，不再切回舊 tab
await browser.close();
console.log('\n=== SUMMARY ===');
console.log('errors:', errors.length);
console.log('network failures:', networkFailures.length);
console.log('univer canvas rendered:', univerCanvas > 0);
process.exit(errors.length === 0 && univerCanvas > 0 ? 0 : 1);
