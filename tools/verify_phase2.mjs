import { chromium } from "playwright";
const browser = await chromium.launch();
const page = await browser.newPage();
const errors = [];
page.on("pageerror", e => errors.push(e.message));
try {
  await page.goto("http://127.0.0.1:8765", { waitUntil: "networkidle", timeout: 30000 });
  // calc-preview API 探針（純計算端點，不寫入）
  const probe = await page.evaluate(async () => {
    const sched = await fetch("/api/schedule/completed").then(r => r.json());
    const ids = (sched.rows || []).slice(0, 2).map(r => r.id);
    if (!ids.length) return { skip: "no orders" };
    const res = await fetch("/api/schedule/calc-preview", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ order_ids: ids, decisions: {}, supplements: {}, sample_order_ids: [], reset_stored: false }),
    });
    const data = await res.json().catch(() => ({}));
    return { status: res.status, hasScopes: Array.isArray(data.scopes), hasShared: Array.isArray(data.shared_parts), blocking: data.blocking_count };
  });
  console.log("calc-preview probe:", JSON.stringify(probe));
  await page.click('.tab-btn[data-tab="calc-workspace"]');
  await page.waitForTimeout(600);
  const wsOk = await page.$eval("#tab-calc-workspace", el => getComputedStyle(el).display !== "none").catch(() => false);
  console.log("workspace tab opens:", wsOk);
  console.log("JS errors:", errors.length ? errors.join(" | ") : "none");
} finally { await browser.close(); }
