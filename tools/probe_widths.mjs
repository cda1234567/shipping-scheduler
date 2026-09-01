import { chromium } from "playwright";
const URL = "http://127.0.0.1:8765/";
const b = await chromium.launch({ headless: true });
const p = await (await b.newContext({viewport:{width:1600,height:900}})).newPage();
await p.goto(URL, { waitUntil: "networkidle" });
await p.click('button[data-tab="main-preview-v2"]');
await p.waitForSelector("#main-preview-v2-stage .luckysheet", { timeout: 30000 });
await p.waitForFunction(() => !!window.luckysheet?.getAllSheets?.()?.[0], null, { timeout: 30000 });
await p.waitForTimeout(1500);

const out = await p.evaluate(() => {
  const sh = window.luckysheet.getAllSheets()[0];
  const file0 = window.luckysheet.getluckysheetfile?.()?.[0];
  const vdc = file0.visibledatacolumn || [];
  const cols = [188, 189, 190, 191, 192, 193, 196, 199];
  return cols.map(idx => ({
    idx,
    columnlenSet: sh.config?.columnlen?.[idx],
    visibleEnd: vdc[idx],
    actualWidth: vdc[idx] - (idx > 0 ? vdc[idx-1] : 0),
    text: sh.data?.[0]?.[idx]?.m,
  }));
});
console.log(JSON.stringify(out, null, 2));
await b.close();
