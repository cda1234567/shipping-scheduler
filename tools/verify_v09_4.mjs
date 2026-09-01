import { chromium } from "playwright";
const browser = await chromium.launch();
const page = await browser.newPage();
const errors = [];
page.on("pageerror", e => errors.push(e.message));
try {
  await page.goto("http://127.0.0.1:8765", { waitUntil: "networkidle", timeout: 30000 });
  await page.click('.tab-btn[data-tab="completed"]');
  await page.waitForTimeout(1500);
  const folderSelects = await page.$$eval(".completed-folder-header .folder-select", els => els.length).catch(() => 0);
  const addChildBtns = await page.$$eval(".btn-folder-add-child", els => els.length).catch(() => 0);
  console.log("folder parent dropdowns:", folderSelects, "| removed +buttons (should be 0):", addChildBtns);
  const dupInfo = await page.evaluate(() => {
    const ids = [...document.querySelectorAll(".completed-order-check")].map(b => b.dataset.orderId);
    return { total: ids.length, unique: new Set(ids).size };
  });
  console.log("order cards:", JSON.stringify(dupInfo));
  console.log("JS errors:", errors.length ? errors.join(" | ") : "none");
} finally { await browser.close(); }
