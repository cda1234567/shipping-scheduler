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
  function letterToIdx(s) { let n=0; for (const c of s) n=n*26+(c.charCodeAt(0)-64); return n-1; }
  const cols = ["GG","GH","GI","GJ","GK","GL","GO","GR"].map(c => ({ letter: c, idx: letterToIdx(c) }));
  const sh = window.luckysheet.getAllSheets()[0];
  const rowlen = sh.config?.rowlen?.[0];
  const result = cols.map(({letter, idx}) => {
    const cell = sh.data?.[0]?.[idx];
    const colWidth = sh.config?.columnlen?.[idx];
    return { letter, idx, m: cell?.m, tb: cell?.tb, ct: cell?.ct, colWidth, has_v: !!cell?.v };
  });
  return { rowlen, cells: result };
});
console.log(JSON.stringify(out, null, 2));
await b.close();
