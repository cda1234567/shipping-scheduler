import { chromium } from "playwright";
const browser = await chromium.launch();
const page = await browser.newPage();
const errors = [];
page.on("pageerror", e => errors.push(e.message));
try {
  await page.goto("http://127.0.0.1:8765", { waitUntil: "networkidle", timeout: 30000 });
  const tabBtn = await page.$('.tab-btn[data-tab="calc-workspace"]');
  console.log("workspace tab button:", tabBtn ? "FOUND" : "MISSING");
  if (tabBtn) {
    await tabBtn.click();
    await page.waitForTimeout(800);
    const visible = await page.$eval("#tab-calc-workspace", el => getComputedStyle(el).display !== "none").catch(() => false);
    const emptyText = await page.$eval("#tab-calc-workspace", el => el.textContent.slice(0, 80)).catch(() => "?");
    console.log("workspace tab visible:", visible, "| content head:", emptyText.trim().slice(0, 50));
    const overlayShown = await page.$eval("#shortage-modal", el => getComputedStyle(el).display !== "none").catch(() => false);
    console.log("old overlay visible (should be false):", overlayShown);
    await page.click('.tab-btn[data-tab="schedule"]');
    await page.waitForTimeout(500);
    await page.click('.tab-btn[data-tab="calc-workspace"]');
    await page.waitForTimeout(500);
    console.log("tab switching round-trip: OK");
  }
  console.log("JS errors:", errors.length ? errors.join(" | ") : "none");
} finally { await browser.close(); }
