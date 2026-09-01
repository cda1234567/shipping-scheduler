// Bug 7 v2: db.supplement 全清,modal 還是顯示 16000?抓 modal 內真實 input value
import { chromium } from 'playwright';

const PAGE_URL = 'http://localhost:8765';
const PASSWORD = '123';

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
  await page.waitForTimeout(2000);
  console.log('登入完成');

  // 全勾
  const checked = await page.evaluate(() => {
    let n = 0;
    document.querySelectorAll('input[type="checkbox"]').forEach(b => {
      if (b.disabled) return;
      if (!b.closest('tr, .schedule-row, [data-order-id]')) return;
      b.checked = true;
      b.dispatchEvent(new Event('change', { bubbles: true }));
      n++;
    });
    return n;
  });
  console.log(`勾選 ${checked} 筆`);
  await page.waitForTimeout(1500);

  // 按綠色「批次 Merge」(reset_stored=true 路徑)
  console.log('按綠色 批次 Merge...');
  await page.click('#btn-batch-merge');
  await page.waitForSelector('#shortage-modal', { state: 'visible', timeout: 60000 });
  await page.waitForTimeout(8000); // 等 rebuild + render

  // 抓 EC-30009A 那行的 supplement input 跟周邊 metadata
  const ec = await page.evaluate(() => {
    const list = document.getElementById('modal-shortage-list');
    if (!list) return { found: false, reason: 'no modal-shortage-list' };
    const items = Array.from(list.querySelectorAll('.shortage-item, [data-part]'));
    const out = [];
    for (const item of items) {
      const text = item.innerText || '';
      if (!text.includes('EC-30009A')) continue;
      const input = item.querySelector('.supplement-input, input.modal-shortage-supplement, input[type="number"]');
      out.push({
        supplement_value: input ? input.value : null,
        default_supplement: input ? input.dataset?.defaultSupplement : null,
        snippet: text.replace(/\s+/g, ' ').slice(0, 400),
      });
    }
    return { found: out.length > 0, items: out };
  });

  console.log('\n=== EC-30009A 在 modal 內 ===');
  console.log(JSON.stringify(ec, null, 2));

  // 撈 server 算出來的 shortages,看 lookahead 算什麼
  const serverCalc = await page.evaluate(async () => {
    const r = await fetch('/api/schedule/drafts?' + new URLSearchParams({ order_ids: '95,96,97,98,99,100,101,102,103,104' }));
    if (!r.ok) return { error: r.status };
    const d = await r.json();
    // 撈 EC-30009A 在每個 draft 的 shortage_amount / supplement_qty
    const drafts = d.drafts || {};
    const result = [];
    for (const [oid, draft] of Object.entries(drafts)) {
      if (!draft) continue;
      const shortages = draft.shortages || [];
      const ec = shortages.find(s => (s.part_number || '').toUpperCase() === 'EC-30009A');
      if (ec) {
        result.push({
          order_id: oid,
          shortage_amount: ec.shortage_amount,
          suggested_qty: ec.suggested_qty,
          supplement_qty: ec.supplement_qty,
          current_stock: ec.current_stock,
          needed: ec.needed,
          moq: ec.moq,
          st_available: ec.st_available_qty,
          purchase_needed: ec.purchase_needed_qty,
        });
      }
    }
    return result;
  });
  console.log('\n=== Server draft.shortages 對 EC-30009A ===');
  console.log(JSON.stringify(serverCalc, null, 2));

  await page.screenshot({ path: 'C:/Users/Andy-STNB/bug7_v2_modal.png', fullPage: false });

  if (errors.length) {
    console.log('\n--- page errors ---');
    errors.slice(0, 10).forEach(e => console.log('  ' + e));
  }
} catch (e) {
  console.log('ERROR:', e.message);
  await page.screenshot({ path: 'C:/Users/Andy-STNB/bug7_v2_error.png' }).catch(() => {});
} finally {
  await browser.close();
}
