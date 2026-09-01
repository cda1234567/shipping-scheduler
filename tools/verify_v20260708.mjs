import { chromium } from "playwright";

const browser = await chromium.launch();
const page = await browser.newPage();
const errors = [];
page.on("pageerror", e => errors.push("pageerror: " + e.message));

try {
  await page.goto("http://127.0.0.1:8765", { waitUntil: "networkidle", timeout: 30000 });

  // 切到已發料分頁
  await page.click('.tab-btn[data-tab="completed"]');
  await page.waitForTimeout(1500);

  const selectAll = await page.$("#completed-select-all");
  console.log("select-all checkbox:", selectAll ? "FOUND" : "MISSING");

  const checkCount = await page.$$eval(".completed-order-check", els => els.length);
  console.log("order checkboxes rendered:", checkCount);

  const folderSections = await page.$$eval(".completed-folder-section", els => els.length);
  console.log("folder sections rendered:", folderSections);

  // 全選 → 數量顯示
  if (selectAll && checkCount > 0) {
    await page.click("#completed-select-all");
    await page.waitForTimeout(300);
    const countText = await page.$eval("#completed-selected-count", el => el.textContent).catch(() => "(no count el)");
    const checkedNow = await page.$$eval(".completed-order-check:checked", els => els.length);
    console.log("after select-all:", checkedNow, "checked; count label:", countText);
    await page.click("#completed-select-all"); // 還原
    await page.waitForTimeout(200);
    const checkedAfter = await page.$$eval(".completed-order-check:checked", els => els.length);
    console.log("after deselect-all:", checkedAfter, "checked");
  }

  // 新資料夾輸入框支援巢狀提示（存在即可）
  const newFolderInput = await page.$("#new-folder-name");
  console.log("new-folder input:", newFolderInput ? "FOUND" : "MISSING");

  console.log("JS errors:", errors.length ? errors.join(" | ") : "none");
} finally {
  await browser.close();
}
