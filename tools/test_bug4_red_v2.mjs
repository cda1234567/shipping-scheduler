import { chromium } from 'playwright';

const PAGE_URL = 'http://localhost:8765';
const EDIT_PASSWORD = '123';
const TARGETS = ['IC-M24C02-WMN6TP-TAB', 'EC-30009A'];

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1700, height: 1100 } });
const page = await ctx.newPage();
const errors = [];
page.on('pageerror', e => errors.push(`pageerror: ${e.message}`));
page.on('console', m => { if (m.type() === 'error') errors.push(`console.error: ${m.text()}`); });

try {
  await page.goto(PAGE_URL, { waitUntil: 'networkidle', timeout: 45000 });
  await page.waitForSelector('#right-panel', { timeout: 30000 });
  await page.waitForTimeout(2000);

  // 開 modal 並登入
  const modalVisible = await page.evaluate(() => {
    const m = document.getElementById('edit-auth-modal');
    return m && getComputedStyle(m).display !== 'none';
  });
  if (!modalVisible) {
    await page.click('text=登入編輯');
  }
  await page.waitForSelector('#edit-auth-password', { state: 'visible', timeout: 10000 });
  await page.fill('#edit-auth-password', EDIT_PASSWORD);
  await page.click('#edit-auth-submit');
  await page.waitForSelector('#edit-auth-modal', { state: 'hidden', timeout: 10000 });
  await page.waitForTimeout(2500);

  // 全勾(可動的 checkbox)
  const checked = await page.evaluate(() => {
    const boxes = document.querySelectorAll('input[type="checkbox"]');
    let n = 0;
    boxes.forEach(b => {
      if (b.disabled) return;
      const inSchedule = b.closest('[id*="schedule"], .schedule-row, tr, .row, .order-row');
      if (b.id && b.id.startsWith('chk-') || inSchedule) {
        b.checked = true;
        b.dispatchEvent(new Event('change', { bubbles: true }));
        n++;
      }
    });
    return n;
  });
  console.log(`checked ${checked} checkboxes after login`);

  await page.waitForTimeout(4000);

  const panelText = await page.locator('#right-panel').innerText();
  console.log('---');
  for (const t of TARGETS) {
    const cnt = (panelText.match(new RegExp(t.replace(/[-]/g, '\\-'), 'g')) || []).length;
    console.log(`  ${t}: 出現 ${cnt} 次 ${cnt === 0 ? '← 無紅字 (PASS)' : '← 有紅字 (檢視 16000?)'}`);
  }

  // 撈每個顯示卡的補料數字
  const cards = await page.evaluate(() => {
    return Array.from(document.querySelectorAll('#right-panel .shortage-item, #right-panel [class*="shortage"]'))
      .slice(0, 20)
      .map(el => el.innerText.replace(/\s+/g, ' ').slice(0, 200));
  });
  console.log('\n--- 右側面板補料卡 (前 20 個) ---');
  cards.forEach((c, i) => console.log(`  [${i}] ${c}`));

  await page.screenshot({ path: 'C:/Users/Andy-STNB/bug4_after_login.png', fullPage: false });
  console.log('\n截圖: C:/Users/Andy-STNB/bug4_after_login.png');

  if (errors.length) {
    console.log('\n--- page errors ---');
    errors.slice(0, 10).forEach(e => console.log('  ' + e));
  }
} catch (e) {
  console.log('ERROR:', e.message);
  await page.screenshot({ path: 'C:/Users/Andy-STNB/bug4_error.png' }).catch(() => {});
} finally {
  await browser.close();
}
