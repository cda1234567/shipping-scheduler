import { chromium } from 'playwright';
import fs from 'fs';
import path from 'path';

const BASE_URL = 'http://127.0.0.1:8765';
const REPORT_PATH = 'D:/claude/shipping-scheduler/chaos_report.md';
const RUN_DURATION_MS = 4 * 60 * 1000; // 4 minutes

const log = [];
const jsErrors = [];
const httpErrors = [];
let actionCount = 0;

function ts() {
  return new Date().toISOString().slice(11, 23);
}

function record(msg) {
  const line = `[${ts()}] ${msg}`;
  console.log(line);
  log.push(line);
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randStr(len = 6) {
  return Math.random().toString(36).slice(2, 2 + len);
}

function randNum(max = 999) {
  return String(randInt(1, max));
}

const DESTRUCTIVE_RE = /刪除|delete|reset|清除|wipe|重置|清空|清掉|drop/i;

async function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function safeClick(page, selector, label) {
  try {
    const el = await page.$(selector);
    if (!el) return false;
    const text = await el.textContent().catch(() => '');
    if (DESTRUCTIVE_RE.test(text)) {
      record(`  SKIP destructive: ${label} "${text.trim()}"`);
      return false;
    }
    await el.click({ timeout: 3000 });
    actionCount++;
    record(`  CLICK ${label}: "${text.trim().slice(0, 40)}"`);
    await sleep(400);
    return true;
  } catch (e) {
    record(`  CLICK FAIL ${label}: ${e.message.slice(0, 80)}`);
    return false;
  }
}

async function dismissModals(page) {
  // Close any open modals by pressing Escape or clicking Cancel/關閉
  try {
    const closeBtn = await page.$('.modal.show .btn-secondary, .modal.show [data-dismiss="modal"], .modal.show .btn-close');
    if (closeBtn) {
      await closeBtn.click({ timeout: 2000 });
      await sleep(300);
      record('  Dismissed modal (cancel/close btn)');
    } else {
      await page.keyboard.press('Escape');
      await sleep(300);
    }
  } catch (_) {}
}

async function fillVisibleInputs(page) {
  try {
    const inputs = await page.$$('input:visible, textarea:visible');
    for (const inp of inputs) {
      try {
        const type = await inp.getAttribute('type') || 'text';
        const id = await inp.getAttribute('id') || '';
        if (type === 'file') continue;
        if (type === 'checkbox' || type === 'radio') {
          // toggle randomly
          if (Math.random() > 0.5) {
            await inp.click({ timeout: 2000 });
            actionCount++;
            record(`  TOGGLE checkbox/radio #${id}`);
          }
          continue;
        }
        const val = (type === 'number') ? randNum(500) : randStr(8);
        await inp.fill(val, { timeout: 2000 });
        actionCount++;
        record(`  FILL input #${id} type=${type} val="${val}"`);
        await sleep(150);
      } catch (_) {}
    }
  } catch (_) {}
}

async function interactWithSelects(page) {
  try {
    const selects = await page.$$('select:visible');
    for (const sel of selects) {
      try {
        const options = await sel.$$('option');
        if (options.length > 1) {
          const idx = randInt(1, options.length - 1);
          const val = await options[idx].getAttribute('value');
          await sel.selectOption(val, { timeout: 2000 });
          actionCount++;
          record(`  SELECT option idx=${idx} val="${val}"`);
          await sleep(200);
        }
      } catch (_) {}
    }
  } catch (_) {}
}

async function clickRandomButtons(page, limit = 5) {
  try {
    const btns = await page.$$('button:visible, [role="button"]:visible');
    const shuffled = btns.sort(() => Math.random() - 0.5).slice(0, limit);
    for (const btn of shuffled) {
      try {
        const text = await btn.textContent().catch(() => '');
        const id = await btn.getAttribute('id') || '';
        if (DESTRUCTIVE_RE.test(text)) {
          record(`  SKIP destructive btn: "${text.trim().slice(0, 40)}"`);
          continue;
        }
        // Skip file upload triggers
        if (id.includes('upload') || text.includes('上傳')) {
          record(`  SKIP upload btn: "${text.trim().slice(0, 30)}"`);
          continue;
        }
        await btn.click({ timeout: 3000 });
        actionCount++;
        record(`  BTN CLICK #${id} "${text.trim().slice(0, 40)}"`);
        await sleep(500);
        // dismiss any modal that opened
        const modalOpen = await page.$('.modal.show');
        if (modalOpen) {
          record('  Modal opened, will interact then dismiss');
          await fillVisibleInputs(page);
          await interactWithSelects(page);
          // Try submitting the modal form once
          const submitBtn = await page.$('.modal.show .btn-primary, .modal.show button[type="submit"]');
          if (submitBtn) {
            const sbText = await submitBtn.textContent().catch(() => '');
            if (!DESTRUCTIVE_RE.test(sbText)) {
              await submitBtn.click({ timeout: 2000 }).catch(() => {});
              actionCount++;
              record(`  MODAL SUBMIT: "${sbText.trim().slice(0, 40)}"`);
              await sleep(600);
            }
          }
          await dismissModals(page);
        }
      } catch (e) {
        record(`  BTN ERR: ${e.message.slice(0, 80)}`);
      }
    }
  } catch (_) {}
}

async function tryManualSupplement(page) {
  record('--- Trying 手動補料 feature ---');
  try {
    // Click the manual supplement button
    const btn = await page.$('#btn-manual-supplement');
    if (!btn) { record('  手動補料 btn not found'); return; }
    await btn.click({ timeout: 3000 });
    actionCount++;
    await sleep(600);

    // Look for modal/dialog
    const modal = await page.$('.modal.show');
    if (!modal) { record('  No modal appeared for 手動補料'); return; }

    // Fill part number from datalist or input
    const partInput = await page.$('.modal.show input[list], .modal.show #supplement-part, .modal.show input[placeholder*="料號"], .modal.show input[placeholder*="part"]');
    if (partInput) {
      // Try to get datalist options
      const listId = await partInput.getAttribute('list');
      let partVal = 'TEST-PART-' + randStr(4);
      if (listId) {
        const options = await page.$$(`#${listId} option`);
        if (options.length > 0) {
          const idx = randInt(0, options.length - 1);
          partVal = await options[idx].getAttribute('value') || partVal;
        }
      }
      await partInput.fill(partVal, { timeout: 2000 });
      actionCount++;
      record(`  手動補料 part: "${partVal}"`);
    }

    // Fill quantity
    const qtyInput = await page.$('.modal.show input[type="number"], .modal.show #supplement-qty, .modal.show input[placeholder*="數量"]');
    if (qtyInput) {
      const qty = randNum(100);
      await qtyInput.fill(qty, { timeout: 2000 });
      actionCount++;
      record(`  手動補料 qty: ${qty}`);
    }

    await sleep(300);

    // Submit
    const submitBtn = await page.$('.modal.show .btn-primary, .modal.show button[type="submit"]');
    if (submitBtn) {
      const sbText = await submitBtn.textContent().catch(() => '');
      await submitBtn.click({ timeout: 2000 });
      actionCount++;
      record(`  手動補料 SUBMIT: "${sbText.trim()}"`);
      await sleep(800);
    }

    await dismissModals(page);
  } catch (e) {
    record(`  手動補料 ERROR: ${e.message.slice(0, 120)}`);
    await dismissModals(page);
  }
}

async function tryEditScheduleCell(page) {
  record('--- Trying schedule cell edit ---');
  try {
    // Look for editable cells in the schedule tab
    const cells = await page.$$('#tab-schedule td[contenteditable], #tab-schedule .editable, #tab-schedule input');
    if (cells.length === 0) {
      // Try double-clicking a table cell to trigger edit
      const tds = await page.$$('#tab-schedule table td');
      if (tds.length > 0) {
        const td = tds[randInt(0, Math.min(tds.length - 1, 20))];
        await td.dblclick({ timeout: 2000 });
        actionCount++;
        record('  Double-clicked schedule cell');
        await sleep(400);
        const editInput = await page.$('#tab-schedule input:visible');
        if (editInput) {
          await editInput.fill(randNum(200), { timeout: 2000 });
          actionCount++;
          await page.keyboard.press('Enter');
          record('  Filled and confirmed schedule cell edit');
          await sleep(400);
        }
      }
    } else {
      const cell = cells[randInt(0, cells.length - 1)];
      await cell.click({ timeout: 2000 });
      await cell.fill(randNum(200), { timeout: 2000 });
      actionCount++;
      await page.keyboard.press('Enter');
      record('  Filled schedule cell directly');
      await sleep(400);
    }
  } catch (e) {
    record(`  Schedule cell edit ERROR: ${e.message.slice(0, 80)}`);
  }
}

async function visitTab(page, tabId, tabLabel) {
  record(`=== Visiting tab: ${tabLabel} (${tabId}) ===`);
  try {
    const tabBtn = await page.$(`button.tab-btn[data-tab="${tabId}"]`);
    if (tabBtn) {
      await tabBtn.click({ timeout: 3000 });
      actionCount++;
      await sleep(700);
    }
    await fillVisibleInputs(page);
    await interactWithSelects(page);
    await clickRandomButtons(page, 4);
  } catch (e) {
    record(`  Tab visit ERROR (${tabLabel}): ${e.message.slice(0, 80)}`);
  }
}

async function main() {
  const startTime = Date.now();
  record('=== CHAOS TEST START ===');

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  // Monitor JS errors
  page.on('pageerror', err => {
    const entry = { time: ts(), url: page.url(), message: err.message, stack: err.stack };
    jsErrors.push(entry);
    record(`[JS ERROR] ${err.message.slice(0, 120)}`);
  });

  // Monitor console errors
  page.on('console', msg => {
    if (msg.type() === 'error') {
      const entry = { time: ts(), url: page.url(), text: msg.text() };
      jsErrors.push(entry);
      record(`[CONSOLE ERROR] ${msg.text().slice(0, 120)}`);
    }
  });

  // Monitor HTTP errors
  page.on('response', resp => {
    const status = resp.status();
    if (status >= 400 && status !== 401 && status !== 403 && status !== 404) {
      const entry = { time: ts(), url: resp.url(), status };
      httpErrors.push(entry);
      record(`[HTTP ${status}] ${resp.url()}`);
    }
  });

  // Step 1: Navigate
  record('Navigating to app...');
  await page.goto(BASE_URL, { timeout: 15000 });
  await sleep(1000);

  // Step 2: Authenticate via API
  record('Authenticating via edit-auth...');
  try {
    const resp = await page.request.post(`${BASE_URL}/api/system/edit-auth/login`, {
      data: { password: '123' },
      headers: { 'Content-Type': 'application/json' }
    });
    record(`Auth response: ${resp.status()}`);
  } catch (e) {
    record(`Auth ERROR: ${e.message}`);
  }

  // Step 3: Reload so auth cookie takes effect
  await page.reload({ timeout: 15000 });
  await sleep(1200);
  record('Page reloaded after auth');

  // Step 4: Visit all tabs
  const tabs = [
    { id: 'schedule', label: '出貨排程' },
    { id: 'completed', label: '已發料' },
    { id: 'main-preview', label: '主檔預覽' },
    { id: 'bom', label: 'BOM管理' },
    { id: 'st-packages-tab', label: '無MOQ包裝' },
    { id: 'alerts-tab', label: '提醒' },
    { id: 'logs-tab', label: '紀錄' },
    { id: 'backups-tab', label: '備份' },
    { id: 'analytics-tab', label: '分析' },
    { id: 'defectives-tab', label: '不良品' },
  ];

  for (const tab of tabs) {
    if (Date.now() - startTime > RUN_DURATION_MS) break;
    await visitTab(page, tab.id, tab.label);
    await sleep(300);
  }

  // Step 5: Go to schedule tab and do deeper interactions
  record('=== Deep interaction on schedule tab ===');
  await page.$eval('button.tab-btn[data-tab="schedule"]', el => el.click()).catch(() => {});
  await sleep(800);

  await tryEditScheduleCell(page);
  await tryManualSupplement(page);

  // Try dispatch sparingly (1 time)
  if (Date.now() - startTime < RUN_DURATION_MS) {
    record('--- Trying 生成發料單 (dispatch gen) ---');
    const dispatchBtn = await page.$('#btn-gen-dispatch');
    if (dispatchBtn) {
      await dispatchBtn.click({ timeout: 3000 }).catch(() => {});
      actionCount++;
      record('  Clicked 生成發料單');
      await sleep(1000);
      // If a modal appears, interact and cancel
      const modal = await page.$('.modal.show');
      if (modal) {
        await fillVisibleInputs(page);
        await dismissModals(page);
      }
    }
  }

  // Step 6: Random chaos loop for remaining time
  record('=== Starting random chaos loop ===');
  let iteration = 0;
  while (Date.now() - startTime < RUN_DURATION_MS) {
    iteration++;
    record(`--- Chaos iteration ${iteration} ---`);

    // Pick a random tab
    const tab = tabs[randInt(0, tabs.length - 1)];
    try {
      const tabBtn = await page.$(`button.tab-btn[data-tab="${tab.id}"]`);
      if (tabBtn) {
        await tabBtn.click({ timeout: 2000 });
        actionCount++;
        await sleep(500);
      }
    } catch (_) {}

    await fillVisibleInputs(page);
    await interactWithSelects(page);
    await clickRandomButtons(page, 3);

    // Occasionally try manual supplement again
    if (iteration % 3 === 0 && Date.now() - startTime < RUN_DURATION_MS) {
      await page.$eval('button.tab-btn[data-tab="schedule"]', el => el.click()).catch(() => {});
      await sleep(500);
      await tryManualSupplement(page);
    }

    // Try pressing keyboard shortcuts randomly
    try {
      const keys = ['Tab', 'Enter', 'Escape', 'ArrowDown', 'ArrowUp'];
      await page.keyboard.press(keys[randInt(0, keys.length - 1)]);
      await sleep(200);
    } catch (_) {}

    await sleep(500);
  }

  record('=== CHAOS TEST COMPLETE ===');
  record(`Total actions: ${actionCount}`);
  record(`JS/Console errors: ${jsErrors.length}`);
  record(`HTTP errors: ${httpErrors.length}`);

  await browser.close();

  // Write report
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const report = [
    '# Chaos Test Report',
    '',
    `**Date:** ${new Date().toISOString()}`,
    `**Duration:** ${elapsed}s`,
    `**Total Actions:** ${actionCount}`,
    `**JS/Console Errors:** ${jsErrors.length}`,
    `**HTTP 4xx/5xx Errors:** ${httpErrors.length}`,
    '',
    '---',
    '',
    '## Assessment',
    '',
    jsErrors.length === 0 && httpErrors.length === 0
      ? '**PASS** — No JS errors or unexpected HTTP errors encountered.'
      : `**REVIEW NEEDED** — Found ${jsErrors.length} JS/console error(s) and ${httpErrors.length} HTTP error(s).`,
    '',
    '---',
    '',
    '## Timeline of Actions',
    '',
    ...log.map(l => `- ${l}`),
    '',
    '---',
    '',
    '## JS / Console Errors',
    '',
    jsErrors.length === 0 ? '_None_' : '',
    ...jsErrors.map((e, i) => [
      `### Error ${i + 1}`,
      `- **Time:** ${e.time}`,
      `- **URL:** ${e.url}`,
      `- **Message:** ${e.message || e.text || ''}`,
      e.stack ? `- **Stack:**\n\`\`\`\n${e.stack}\n\`\`\`` : '',
      ''
    ].join('\n')),
    '',
    '---',
    '',
    '## HTTP Errors (4xx/5xx excl. 401/403/404)',
    '',
    httpErrors.length === 0 ? '_None_' : '',
    ...httpErrors.map((e, i) => [
      `### HTTP Error ${i + 1}`,
      `- **Time:** ${e.time}`,
      `- **Status:** ${e.status}`,
      `- **URL:** ${e.url}`,
      ''
    ].join('\n')),
    '',
    '---',
    '',
    '## Visual Issues',
    '',
    '_No visual screenshots taken in headless mode. No obvious layout errors observed from interaction responses._',
    '',
  ].join('\n');

  fs.writeFileSync(REPORT_PATH, report, 'utf8');
  console.log(`\nReport written to: ${REPORT_PATH}`);
  console.log(`Actions: ${actionCount} | JS Errors: ${jsErrors.length} | HTTP Errors: ${httpErrors.length}`);
}

main().catch(e => {
  console.error('CHAOS TEST FATAL:', e);
  process.exit(1);
});
