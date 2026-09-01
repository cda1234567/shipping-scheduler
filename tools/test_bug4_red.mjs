import { chromium } from 'playwright';

const TARGET = 'IC-M24C02-WMN6TP-TAB';
const PAGE_URL = 'http://localhost:8765';

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1600, height: 1000 } });
const page = await context.newPage();

const errors = [];
page.on('pageerror', (err) => errors.push(`pageerror: ${err.message}`));
page.on('console', (msg) => {
  if (msg.type() === 'error') errors.push(`console.error: ${msg.text()}`);
});

try {
  await page.goto(PAGE_URL, { waitUntil: 'networkidle', timeout: 45000 });
  await page.waitForSelector('#right-panel', { timeout: 30000 });
  await page.waitForTimeout(3000);

  // 全選 6-X (任何 schedule row checkbox)
  // 直接用 evaluate 勾全部
  const checkedCount = await page.evaluate(() => {
    const boxes = document.querySelectorAll('#schedule-tbody input[type="checkbox"]');
    let n = 0;
    boxes.forEach(b => { if (!b.disabled) { b.checked = true; b.dispatchEvent(new Event('change', { bubbles: true })); n++; } });
    return n;
  });
  console.log(`checked ${checkedCount} rows`);

  await page.waitForTimeout(3000);

  const panelText = await page.locator('#right-panel').innerText();
  const hasTarget = panelText.includes(TARGET);
  const targetCount = (panelText.match(new RegExp(TARGET, 'g')) || []).length;
  const ec30009 = panelText.includes('EC-30009A');

  console.log('---');
  console.log(`right-panel 是否還顯示 ${TARGET}? ${hasTarget ? `是 (${targetCount} 次) ← bug 4 沒修好` : '否 ← bug 4 PASS'}`);
  console.log(`right-panel 是否顯示 EC-30009A? ${ec30009 ? '是 (可看 16000 補料數字)' : '否'}`);

  await page.screenshot({ path: 'C:/Users/Andy-STNB/bug4_panel.png', fullPage: false });
  console.log('截圖存到 C:/Users/Andy-STNB/bug4_panel.png');

  if (errors.length) {
    console.log('\n--- page errors ---');
    errors.forEach(e => console.log('  ' + e));
  }
} catch (e) {
  console.log('ERROR:', e.message);
} finally {
  await browser.close();
}
