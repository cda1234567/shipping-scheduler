// Probe ArrowLeft auto-scroll behavior on main-preview-v2.
// Run: node tools/probe_arrow_left_scroll.mjs
import { chromium } from "playwright";

const URL = "http://127.0.0.1:8765/";

(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1600, height: 900 } });
  const page = await ctx.newPage();
  page.on("console", msg => console.log(`[browser:${msg.type()}]`, msg.text()));
  page.on("pageerror", err => console.log("[pageerror]", err.message));

  await page.goto(URL, { waitUntil: "networkidle", timeout: 30000 });

  await page.click('button[data-tab="main-preview-v2"]');
  await page.waitForSelector("#main-preview-v2-stage .luckysheet", { timeout: 30000 });
  await page.waitForFunction(() => !!window.luckysheet?.getAllSheets?.()?.[0], null, { timeout: 30000 });
  await page.waitForTimeout(800);

  // Push scrollLeft to far right
  const initial = await page.evaluate(() => {
    const stage = document.getElementById("main-preview-v2-stage");
    const scrollX = stage.querySelector(".luckysheet-scrollbar-x");
    const sheet = window.luckysheet.getAllSheets()[0];
    const colCount = sheet.column || (sheet.config?.columnlen ? Math.max(...Object.keys(sheet.config.columnlen).map(Number)) + 1 : 100);
    let totalWidth = 0;
    for (let i = 0; i < colCount; i++) {
      const cw = sheet.config?.columnlen?.[i];
      totalWidth += Number.isFinite(Number(cw)) && Number(cw) > 0 ? Number(cw) : (sheet.defaultColWidth || 73);
    }
    scrollX.scrollLeft = scrollX.scrollWidth;
    scrollX.dispatchEvent(new Event("scroll", { bubbles: true }));
    return {
      scrollLeft: scrollX.scrollLeft,
      scrollWidth: scrollX.scrollWidth,
      clientWidth: scrollX.clientWidth,
      colCount,
      totalWidth,
      frozen: sheet.frozen,
    };
  });
  console.log("[after-scroll-right]", JSON.stringify(initial));
  await page.waitForTimeout(400);

  // Click on a cell roughly in middle of the visible area (after scrolling right)
  // Use luckysheet API to set selection on a far-right cell
  await page.evaluate(() => {
    const ls = window.luckysheet;
    const sheet = ls.getAllSheets()[0];
    const colCount = sheet.column || 100;
    const targetCol = Math.min(colCount - 1, 80); // far right
    ls.setRangeShow({ row: [3, 3], column: [targetCol, targetCol] });
  });
  await page.waitForTimeout(300);

  const beforeKey = await page.evaluate(() => {
    const sheet = window.luckysheet.getAllSheets()[0];
    const stage = document.getElementById("main-preview-v2-stage");
    const scrollX = stage.querySelector(".luckysheet-scrollbar-x");
    const sel = window.luckysheet.getluckysheet_select_save?.()?.[0] || sheet.luckysheet_select_save?.[0];
    return {
      column_focus: sel?.column_focus,
      column: sel?.column,
      scrollLeft: scrollX?.scrollLeft,
      frozen: sheet.frozen,
    };
  });
  console.log("[before-arrows]", JSON.stringify(beforeKey));

  // Focus the stage so keydown lands inside it
  await page.click("#main-preview-v2-stage", { position: { x: 600, y: 400 } });
  await page.waitForTimeout(200);

  // Reset selection after click (click may have moved it)
  await page.evaluate(() => {
    const ls = window.luckysheet;
    const sheet = ls.getAllSheets()[0];
    const colCount = sheet.column || 100;
    ls.setRangeShow({ row: [3, 3], column: [Math.min(colCount - 1, 80), Math.min(colCount - 1, 80)] });
  });
  await page.waitForTimeout(200);

  // Press ArrowLeft repeatedly, snapshot each time
  const snaps = [];
  for (let i = 0; i < 90; i++) {
    await page.keyboard.press("ArrowLeft");
    await page.waitForTimeout(60);
    const snap = await page.evaluate(() => {
      const sheet = window.luckysheet.getAllSheets()[0];
      const stage = document.getElementById("main-preview-v2-stage");
      const scrollX = stage.querySelector(".luckysheet-scrollbar-x");
      const sel = window.luckysheet.getluckysheet_select_save?.()?.[0] || sheet.luckysheet_select_save?.[0];
      const col = sel?.column_focus ?? (Array.isArray(sel?.column) ? sel.column[0] : sel?.column);
      const colWidth = (idx) => {
        const cw = sheet.config?.columnlen?.[idx];
        return Number.isFinite(Number(cw)) && Number(cw) > 0 ? Number(cw) : (sheet.defaultColWidth || 73);
      };
      let selLeft = 0;
      for (let i = 0; i < col; i++) selLeft += colWidth(i);
      const frozenCol = Number(sheet.frozen?.range?.column_focus ?? -1);
      let frozenWidth = 0;
      for (let i = 0; i <= frozenCol; i++) frozenWidth += colWidth(i);
      const visibleLeft = selLeft - (scrollX?.scrollLeft || 0);
      return {
        col,
        scrollLeft: scrollX?.scrollLeft,
        selLeft,
        frozenWidth,
        visibleLeft,
        coveredByFrozen: visibleLeft < frozenWidth,
      };
    });
    snaps.push(snap);
  }
  console.log("[arrow-snapshots]");
  snaps.forEach((s, i) => console.log(`  #${i}`, JSON.stringify(s)));

  const coveredCount = snaps.filter(s => s.coveredByFrozen).length;
  console.log(`[summary] coveredByFrozen ratio = ${coveredCount}/${snaps.length}`);

  await browser.close();
})().catch(err => { console.error(err); process.exit(1); });
