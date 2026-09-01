import { chromium } from "playwright";
const URL = "http://127.0.0.1:8765/";
const b = await chromium.launch({ headless: true });
const p = await (await b.newContext()).newPage();
await p.goto(URL, { waitUntil: "networkidle" });
await p.click('button[data-tab="main-preview-v2"]');
await p.waitForSelector("#main-preview-v2-stage .luckysheet", { timeout: 30000 });
await p.waitForFunction(() => !!window.luckysheet?.getAllSheets?.()?.[0], null, { timeout: 30000 });
await p.waitForTimeout(800);
await p.screenshot({ path: "D:\\claude\\shipping-scheduler\\tools\\wrap_screenshot.png", fullPage: false, clip: { x: 200, y: 100, width: 1200, height: 200 } });
console.log("[saved] wrap_screenshot.png");
const out = await p.evaluate(() => {
  const sh = window.luckysheet.getAllSheets()[0];
  const stage = document.getElementById("main-preview-v2-stage");
  const row0DOM = stage.querySelector(".luckysheet-cell-main")?.querySelector?.("canvas");
  // Sample a few cells from sheet.data (post-init)
  const dataRow0 = (sh.data?.[0] || []).slice(0, 8).map((c, idx) => c ? ({
    c: idx,
    tb: c.tb,
    m: c.m,
    keys: Object.keys(c),
  }) : null);
  // Compute actual rendered row height from luckysheet visibledatarow
  const luckysheetfile = window.luckysheet.getluckysheetfile?.();
  const file0 = luckysheetfile?.[0] || {};
  return {
    rowlen0: sh.config?.rowlen?.[0],
    visibledatarow_0_1: file0.visibledatarow?.slice?.(0, 3),
    defaultRowHeight: file0.defaultRowHeight,
    dataRow0Sample: dataRow0,
    dataRow1Sample: (sh.data?.[1] || []).slice(0, 5).map((c, idx) => c ? ({c: idx, tb: c.tb, m: c.m}) : null),
  };
});
console.log(JSON.stringify(out, null, 2));
await b.close();
