import { chromium } from 'playwright';

const URL = 'http://127.0.0.1:8765/';

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext();
const page = await ctx.newPage();

const results = { errors: [], findings: {} };
page.on('pageerror', e => results.errors.push(`pageerror: ${e.message}`));
page.on('console', m => { if (m.type() === 'error') results.errors.push(`console.error: ${m.text()}`); });

try {
  await page.goto(URL, { waitUntil: 'networkidle', timeout: 30000 });

  // 點 ST 庫存頁籤
  await page.waitForSelector('button[data-tab="st-inventory-tab"], button[data-tab="tab-st-inventory"], .tab-btn:has-text("ST 庫存")', { timeout: 10000 });
  const stTab = await page.locator('.tab-btn').filter({ hasText: 'ST 庫存' }).first();
  await stTab.click();

  // 等表格載入
  await page.waitForSelector('#st-inventory-table-body tr', { timeout: 15000 });

  // 檢查表頭欄位
  const headers = await page.locator('.st-inventory-table thead th').allTextContents();
  results.findings.headers = headers;
  results.findings.has_moq_header = headers.includes('MOQ');

  // 抓第一筆 row 的 cell 數
  const firstRowCells = await page.locator('#st-inventory-table-body tr.st-inventory-data-row').first().locator('td').count();
  results.findings.first_row_td_count = firstRowCells;

  // 找 moq badge
  const badgeCount = await page.locator('.moq-badge-editable').count();
  results.findings.moq_badge_count = badgeCount;

  // 抓前 3 個 badge 的內容
  const sampleBadges = [];
  const badges = await page.locator('.moq-badge-editable').all();
  for (let i = 0; i < Math.min(3, badges.length); i++) {
    sampleBadges.push({
      text: (await badges[i].textContent())?.trim(),
      part: await badges[i].getAttribute('data-part'),
      moq: await badges[i].getAttribute('data-moq'),
    });
  }
  results.findings.sample_badges = sampleBadges;

} catch (e) {
  results.errors.push(`navigation_error: ${e.message}`);
} finally {
  await browser.close();
}

console.log(JSON.stringify(results, null, 2));
