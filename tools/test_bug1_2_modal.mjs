// Bug 1 (16000) + Bug 2 (改 MOQ 不丟失補量) — 都需要打開 modal
import { chromium } from 'playwright';

const PAGE_URL = 'http://localhost:8765';
const PASSWORD = '123';

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1700, height: 1100 } });
const page = await ctx.newPage();
const errors = [];
page.on('pageerror', e => errors.push(`pageerror: ${e.message}`));
page.on('console', m => { if (m.type() === 'error') errors.push(`console.error: ${m.text()}`); });

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

  // 只勾 6-1 (含 EC-30009A 的 T356789IU)
  const checked = await page.evaluate(() => {
    let n = 0;
    document.querySelectorAll('input[type="checkbox"]').forEach(b => {
      if (b.disabled) return;
      // 勾選 schedule 列的 checkbox (table 內 row checkbox)
      const row = b.closest('tr, .schedule-row, [data-order-id]');
      if (!row) return;
      // 把所有 schedule row checkbox 都勾(只勾 6-1 那行較麻煩,先全勾測 modal)
      b.checked = true;
      b.dispatchEvent(new Event('change', { bubbles: true }));
      n++;
    });
    return n;
  });
  log(`勾選 ${checked} 筆`);
  await page.waitForTimeout(2000);

  // ============ Bug 1: 打開「批次 Merge」 modal 看 EC-30009A 預設補量 ============
  log('\n=== Bug 1: 開批次 Merge modal ===');
  await page.click('#btn-batch-merge');
  await page.waitForSelector('#shortage-modal', { state: 'visible', timeout: 30000 });
  await page.waitForTimeout(5000); // 等 modal 內容載入完

  // 撈 modal 內 EC-30009A 那行的 supplement input value
  const ec30009Info = await page.evaluate(() => {
    const list = document.getElementById('modal-shortage-list');
    if (!list) return { found: false, reason: 'no modal-shortage-list' };
    const items = Array.from(list.querySelectorAll('.shortage-item, [data-part]'));
    for (const item of items) {
      const partText = item.innerText || '';
      if (partText.includes('EC-30009A')) {
        const input = item.querySelector('.supplement-input, input.modal-shortage-supplement, input[type="number"]');
        return {
          found: true,
          supplement_value: input ? input.value : null,
          snippet: partText.replace(/\s+/g, ' ').slice(0, 300),
        };
      }
    }
    return { found: false, items_count: items.length, sample_text: items.slice(0, 3).map(i => i.innerText.slice(0, 80)) };
  });
  log('EC-30009A 在 modal:', JSON.stringify(ec30009Info, null, 2));

  // ============ Bug 2: 試 modal 內 supplement 保留 ============
  log('\n=== Bug 2: 改 MOQ 後 supplement 是否保留 ===');

  // step 1: 在某顆「未寫 MOQ」料的 input 填數字補量
  // 找一個 modal 內的 supplement-input,填 999 進去
  const fillResult = await page.evaluate(() => {
    const inputs = document.querySelectorAll('#modal-shortage-list .supplement-input');
    if (!inputs.length) return { error: 'no supplement-input' };
    const target = inputs[0];
    const before = target.value;
    target.value = '999';
    target.dispatchEvent(new Event('input', { bubbles: true }));
    target.dispatchEvent(new Event('change', { bubbles: true }));
    const part = target.closest('[data-part]')?.dataset?.part || target.dataset?.part || 'unknown';
    return { part, before, set_to: '999' };
  });
  log('填補量結果:', fillResult);

  // step 2: 找「未寫 MOQ」的料,在 .moq-input 填 100,按 .save-moq-btn(記住 MOQ)
  const moqEditResult = await page.evaluate(() => {
    const editor = document.querySelector('#modal-shortage-list .moq-editor');
    if (!editor) return { error: 'no moq-editor (沒有未寫 MOQ 的料)' };
    const input = editor.querySelector('.moq-input');
    const btn = editor.querySelector('.save-moq-btn');
    if (!input || !btn) return { error: 'incomplete editor' };
    const part = input.dataset.part;
    input.value = '100';
    return { part, set_moq: 100, ready_to_click: true };
  });
  log('MOQ editor 狀態:', moqEditResult);

  if (moqEditResult.ready_to_click) {
    await page.click('#modal-shortage-list .moq-editor .save-moq-btn');
    await page.waitForTimeout(5000); // 等 re-render

    // step 3: 看剛剛填的 999 是否還在
    const after = await page.evaluate(() => {
      const inputs = document.querySelectorAll('#modal-shortage-list .supplement-input');
      const vals = Array.from(inputs).slice(0, 5).map(i => ({ value: i.value, part: i.closest('[data-part]')?.dataset?.part }));
      return { sample: vals, has_999: vals.some(v => v.value === '999') };
    });
    log('MOQ 存完後補量狀態:', JSON.stringify(after, null, 2));
    console.log(`Bug 2: ${after.has_999 ? '✅ PASS (999 保留)' : '❌ FAIL (999 不見了)'}`);
  } else {
    log('Bug 2: SKIP (modal 內沒未寫 MOQ 的料)');
  }

  await page.screenshot({ path: 'C:/Users/Andy-STNB/bug1_2_modal.png' });
  log('\n截圖: C:/Users/Andy-STNB/bug1_2_modal.png');

  if (errors.length) {
    log('\n--- page errors ---');
    errors.slice(0, 10).forEach(e => log('  ' + e));
  }
} catch (e) {
  log('ERROR:', e.message);
  await page.screenshot({ path: 'C:/Users/Andy-STNB/bug1_2_error.png' }).catch(() => {});
} finally {
  await browser.close();
}
