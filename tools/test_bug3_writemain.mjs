// Bug 3: 批次 Merge + 寫主檔流程,確認 modal 後真的有寫入主檔
import { chromium } from 'playwright';

const PAGE_URL = 'http://localhost:8765';
const PASSWORD = '123';
const TARGET = 'IC-M24C02-WMN6TP-TAB';

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1700, height: 1100 } });
const page = await ctx.newPage();
page.on('pageerror', e => console.log('pageerror:', e.message));
page.on('dialog', async d => {
  console.log(`[dialog: ${d.type()}] ${d.message().slice(0, 200)}`);
  await d.accept();
});

const log = (...a) => console.log(...a);

try {
  await page.goto(PAGE_URL, { waitUntil: 'networkidle', timeout: 45000 });
  await page.waitForSelector('#right-panel', { timeout: 30000 });
  await page.waitForTimeout(2000);

  // 登入
  const visible = await page.evaluate(() => {
    const m = document.getElementById('edit-auth-modal');
    return m && getComputedStyle(m).display !== 'none';
  });
  if (!visible) await page.click('text=登入編輯');
  await page.waitForSelector('#edit-auth-password', { state: 'visible', timeout: 10000 });
  await page.fill('#edit-auth-password', PASSWORD);
  await page.click('#edit-auth-submit');
  await page.waitForSelector('#edit-auth-modal', { state: 'hidden', timeout: 10000 });
  await page.waitForTimeout(2500);
  log('登入完成');

  // 撈寫入前狀態
  log('--- 寫入前 ---');
  const before = await page.evaluate(async () => {
    const r = await fetch('/api/main-file/stock');
    return r.ok ? await r.json() : { error: r.status };
  });
  log(`寫入前 ${TARGET} 主檔庫存 = ${before?.stock?.[TARGET] ?? before?.[TARGET] ?? 'unknown'}`);

  // 勾全部
  const checked = await page.evaluate(() => {
    let n = 0;
    document.querySelectorAll('input[type="checkbox"]').forEach(b => {
      if (b.disabled) return;
      const row = b.closest('tr, .schedule-row, [data-order-id]');
      if (!row) return;
      b.checked = true;
      b.dispatchEvent(new Event('change', { bubbles: true }));
      n++;
    });
    return n;
  });
  log(`勾選 ${checked} 筆`);
  await page.waitForTimeout(2000);

  // ==== 按「批次 Merge + 寫主檔」 ====
  log('\n=== 按批次 Merge + 寫主檔 ===');
  await page.click('#btn-batch-merge-commit');
  await page.waitForSelector('#shortage-modal', { state: 'visible', timeout: 30000 });
  await page.waitForTimeout(6000); // 等 modal 載完

  // 看 modal save button(應該叫「確認補料並寫主檔」或 modal-save-draft)
  const buttons = await page.evaluate(() => {
    return Array.from(document.querySelectorAll('#shortage-modal button')).map(b => ({
      id: b.id, text: (b.textContent || '').trim().slice(0, 40), classes: b.className,
    }));
  });
  log('modal buttons:', JSON.stringify(buttons, null, 2));

  // 找「儲存副檔」或「確認」按鈕(在 mergeCommit 模式下文案可能是「確認補料並寫主檔」)
  // 從 source code 看 button id 是 modal-save-draft
  await page.click('#modal-save-draft');
  log('按下 modal save,等 confirm dialog 跟主檔寫入...');

  // dialog handler 會自動 accept "確認要強制寫入主檔...?" 跟其他 confirm
  // 等寫入完成 — toast 出現或 modal 關掉
  await page.waitForSelector('#shortage-modal', { state: 'hidden', timeout: 90000 });
  log('modal 關閉,等 refresh...');
  await page.waitForTimeout(8000);

  // ==== 寫入後驗證 ====
  log('\n--- 寫入後 ---');
  const after = await page.evaluate(async () => {
    const r = await fetch('/api/main-file/stock');
    return r.ok ? await r.json() : { error: r.status };
  });
  log(`寫入後 ${TARGET} 主檔庫存 = ${after?.stock?.[TARGET] ?? after?.[TARGET] ?? 'unknown'}`);

  // 撈 6-X order status
  const orders = await page.evaluate(async () => {
    const r = await fetch('/api/schedule/rows');
    const d = await r.json();
    return (d.rows || []).filter(o => /^6-\d+$/.test(String(o.code || '')));
  });
  log(`\n寫入後 pending+merged 中還剩幾筆 6-X: ${orders.length}`);

  const completed = await page.evaluate(async () => {
    const r = await fetch('/api/schedule/completed');
    const d = await r.json();
    return (d.rows || []).filter(o => /^6-\d+$/.test(String(o.code || ''))).length;
  });
  log(`寫入後 dispatched/completed 中有幾筆 6-X: ${completed}`);

  await page.screenshot({ path: 'C:/Users/Andy-STNB/bug3_after_write.png' });
  log('\n截圖: C:/Users/Andy-STNB/bug3_after_write.png');

  log(`\n============`);
  log(`Bug 3 判定: ${orders.length === 0 && completed >= 10 ? '✅ PASS (6-X 都寫入主檔)' : '❌ 寫入沒成功 (還剩 ' + orders.length + ' 筆在 pending/merged)'}`);
} catch (e) {
  log('ERROR:', e.message);
  await page.screenshot({ path: 'C:/Users/Andy-STNB/bug3_error.png' }).catch(() => {});
} finally {
  await browser.close();
}
