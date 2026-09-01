import { chromium } from "playwright";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(__filename), "..");
const port = Number(process.env.PORT || 8766);

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
};

function serveFile(req, res) {
  const url = new URL(req.url || "/", `http://127.0.0.1:${port}`);
  if (url.pathname.startsWith("/api/")) {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ detail: "stubbed in verification" }));
    return;
  }

  const relativePath = url.pathname === "/" ? "/static/index.html" : url.pathname;
  const filePath = path.resolve(repoRoot, `.${decodeURIComponent(relativePath)}`);
  if (!filePath.startsWith(repoRoot) || !fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    res.end("not found");
    return;
  }

  res.writeHead(200, { "content-type": mimeTypes[path.extname(filePath)] || "application/octet-stream" });
  fs.createReadStream(filePath).pipe(res);
}

const server = http.createServer(serveFile);
await new Promise(resolve => server.listen(port, "127.0.0.1", resolve));

const browser = await chromium.launch();
const page = await browser.newPage();
const errors = [];
page.on("pageerror", error => errors.push(error.message));

try {
  await page.goto(`http://127.0.0.1:${port}/static/index.html`, { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForFunction(() => Boolean(window.__scheduleEcCollectionHotfix), null, { timeout: 10000 });

  const result = await page.evaluate(() => {
    const hook = window.__scheduleEcCollectionHotfix;
    const list = document.getElementById("modal-shortage-list");
    if (!list) throw new Error("#modal-shortage-list not found");

    function setRows(html) {
      list.innerHTML = html;
    }

    setRows(`
      <div class="shortage-item" data-part="EC-30009A" data-flow-hidden="0">
        <input class="supplement-input" data-part="EC-30009A" value="100">
        <input type="checkbox" class="shortage-mark" data-part="EC-30009A">
      </div>
      <div class="shortage-item" data-part="EC-30009A" data-flow-hidden="0">
        <input class="supplement-input" data-part="EC-30009A" value="0">
        <input type="checkbox" class="shortage-mark" data-part="EC-30009A">
      </div>
    `);
    const noCheckedSupplements = hook.collectSupplements();
    const noCheckedConflicts = hook.findConflicts();

    setRows(`
      <div class="shortage-item" data-part="EC-30009A" data-flow-hidden="0">
        <input class="supplement-input" data-part="EC-30009A" value="0">
        <input type="checkbox" class="shortage-mark" data-part="EC-30009A" checked>
      </div>
      <div class="shortage-item" data-part="EC-30009A" data-flow-hidden="0">
        <input class="supplement-input" data-part="EC-30009A" value="100">
        <input type="checkbox" class="shortage-mark" data-part="EC-30009A">
      </div>
    `);
    const checkedSupplements = hook.collectSupplements();
    const checkedDecisions = hook.collectDecisions();
    const conflicts = hook.findConflicts();
    const warning = hook.buildConflictMessage(conflicts);

    return {
      noCheckedSupplements,
      noCheckedConflicts,
      checkedSupplements,
      checkedDecisions,
      conflicts,
      warning,
      autoNegativeChecked: hook.shouldAutoShortageCheck({ part_number: "EC-30009A", current_stock: -1 }),
      storedShortageChecked: hook.shouldAutoShortageCheck({ part_number: "EC-30009A", decision: "Shortage", current_stock: -1 }),
      hasNegativeRunningStock: hook.hasNegativeRunningStock({ part_number: "EC-30009A", current_stock: -1 }),
    };
  });

  const failures = [];
  if (result.noCheckedSupplements["EC-30009A"] !== 100) failures.push("無人勾缺料時，補料未被收集為 100");
  if (result.noCheckedConflicts.length !== 0) failures.push("無人勾缺料時，不應觸發衝突警告");
  if (Object.keys(result.checkedSupplements).length !== 0) failures.push("跨列勾缺料時，既有 .some() 行為應仍會丟棄同料號補料");
  if (result.checkedDecisions["EC-30009A"] !== "Shortage") failures.push("跨列勾缺料時，decision 應維持 Shortage");
  if (result.conflicts.length !== 1 || result.conflicts[0].part !== "EC-30009A" || result.conflicts[0].rows !== 1) {
    failures.push("衝突警告條件未正確抓到 EC-30009A 的 1 列補料");
  }
  if (!result.warning.includes("料號 EC-30009A 有 1 列填了補料")) failures.push("警告文案缺少料號與列數");
  if (result.autoNegativeChecked !== false) failures.push("負結存不應再自動勾缺料");
  if (result.storedShortageChecked !== true) failures.push("既有 Shortage 決策應維持勾選");
  if (result.hasNegativeRunningStock !== true) failures.push("負結存提示條件應可被辨識");
  if (errors.length) failures.push(`頁面 JS 錯誤：${errors.join(" | ")}`);

  console.log(JSON.stringify(result, null, 2));
  if (failures.length) {
    console.error(`verify_ec_collection_fix failed:\n- ${failures.join("\n- ")}`);
    process.exitCode = 1;
  } else {
    console.log("verify_ec_collection_fix: OK");
  }
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}
