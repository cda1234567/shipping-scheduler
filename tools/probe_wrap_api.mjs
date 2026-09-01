import { chromium } from "playwright";
const URL = "http://127.0.0.1:8765/";
const b = await chromium.launch({ headless: true });
const p = await (await b.newContext({ viewport:{width:1600,height:900} })).newPage();
await p.goto(URL, { waitUntil: "networkidle" });
await p.click('button[data-tab="main-preview-v2"]');
await p.waitForSelector("#main-preview-v2-stage .luckysheet", { timeout: 30000 });
await p.waitForFunction(() => !!window.luckysheet?.getAllSheets?.()?.[0], null, { timeout: 30000 });
await p.waitForTimeout(800);

// Find a cell in row 0 with long text and narrow column
const cellInfo = await p.evaluate(() => {
  const sh = window.luckysheet.getAllSheets()[0];
  const cells = (sh.data?.[0] || []).map((c, idx) => c ? ({ c: idx, m: c.m, len: String(c.m||"").length, tb: c.tb }) : null).filter(Boolean);
  // pick a cell with longest text
  cells.sort((a, b) => b.len - a.len);
  const apiKeys = Object.keys(window.luckysheet).filter(k => /format|wrap|range|cell|refresh/i.test(k));
  return { topCells: cells.slice(0, 5), apiKeys };
});
console.log("[apis available]", JSON.stringify(cellInfo.apiKeys));
console.log("[longest row0 cells]", JSON.stringify(cellInfo.topCells));

// Try setCellFormat on (0, longestCol)
const tryResults = await p.evaluate(({ targetCol }) => {
  const ls = window.luckysheet;
  const out = {};
  // Approach 1: setCellFormat
  try {
    ls.setCellFormat?.(0, targetCol, "tb", "2");
    const sh = ls.getAllSheets()[0];
    out.afterSetCellFormat_tb = sh.data[0][targetCol]?.tb;
  } catch (e) { out.setCellFormat_err = String(e); }
  // Approach 2: setRangeFormat
  try {
    ls.setRangeFormat?.("tb", "2", { range: { row: [0, 0], column: [0, 100] } });
    const sh = ls.getAllSheets()[0];
    out.afterSetRangeFormat_tb = sh.data[0][targetCol]?.tb;
  } catch (e) { out.setRangeFormat_err = String(e); }
  // Approach 3: direct mutate + refresh
  try {
    const sh = ls.getAllSheets()[0];
    sh.data[0][targetCol].tb = "2";
    ls.jfrefreshgrid?.(sh.data, [{ row:[0,0], column:[targetCol, targetCol] }], null, false);
    out.afterDirectMutate = sh.data[0][targetCol]?.tb;
  } catch (e) { out.directMutate_err = String(e); }
  return out;
}, { targetCol: cellInfo.topCells[0].c });
console.log("[try-results]", JSON.stringify(tryResults));

await p.waitForTimeout(500);
await p.screenshot({ path: "D:\\claude\\shipping-scheduler\\tools\\wrap_after_api.png", clip:{ x:50, y:200, width: 1500, height: 200 } });
console.log("[screenshot saved]");

// Last attempt: run setRangeWordWrap if exists, also dump all available format methods
const more = await p.evaluate(() => {
  const ls = window.luckysheet;
  const all = Object.keys(ls);
  return {
    setBatch: typeof ls.setCellsFormat,
    wordWrapApi: typeof ls.setRangeWordWrap,
    refreshGrid: typeof ls.jfrefreshgrid,
    refreshGridDirect: typeof ls.luckysheetrefreshgrid,
    flat: all.filter(k => /set|wrap|format/i.test(k)),
  };
});
console.log("[more]", JSON.stringify(more));
await b.close();
