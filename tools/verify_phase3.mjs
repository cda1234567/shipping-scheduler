import { chromium } from "playwright";
const browser = await chromium.launch();
const page = await browser.newPage();
const errors = [];
page.on("pageerror", e => errors.push(e.message));
try {
  await page.goto("http://127.0.0.1:8765", { waitUntil: "networkidle", timeout: 30000 });
  const mergeBtn = await page.$("#btn-batch-merge");
  const oldCommitBtn = await page.$("#btn-batch-merge-commit");
  const checkboxes = await page.evaluate(() => {
    const reset = document.querySelector("#batch-merge-reset-stored, [data-role='batch-merge-reset']");
    const commit = document.querySelector("#batch-merge-commit-main, [data-role='batch-merge-commit']");
    const labels = [...document.querySelectorAll("label")].map(l => l.textContent.trim()).filter(t => t.includes("重算補料") || t.includes("同時寫主檔"));
    return { resetFound: !!reset, commitFound: !!commit, labels };
  });
  console.log("batch merge button:", mergeBtn ? "FOUND" : "MISSING", "| old red button (should be null):", oldCommitBtn ? "STILL THERE" : "removed");
  console.log("checkboxes:", JSON.stringify(checkboxes));
  console.log("JS errors:", errors.length ? errors.join(" | ") : "none");
} finally { await browser.close(); }
