import { chromium } from "playwright";
const browser = await chromium.launch();
const page = await browser.newPage();
try {
  await page.goto("http://127.0.0.1:8765", { waitUntil: "networkidle", timeout: 30000 });
  await page.click('.tab-btn[data-tab="completed"]');
  await page.waitForTimeout(1500);
  const info = await page.evaluate(() => {
    const boxes = [...document.querySelectorAll(".completed-order-check")];
    const ids = boxes.map(b => b.dataset.orderId);
    const seen = {}, dups = new Set();
    ids.forEach(id => { seen[id] = (seen[id] || 0) + 1; if (seen[id] > 1) dups.add(id); });
    const dupList = [...dups].slice(0, 5).map(id => {
      const els = [...document.querySelectorAll(`.completed-order-check[data-order-id="${id}"]`)];
      return { id, count: els.length, sections: els.map(e => e.closest(".completed-folder-section")?.querySelector(".folder-name")?.textContent || "?") };
    });
    return { total: ids.length, unique: new Set(ids).size, dupCount: dups.size, dupList };
  });
  console.log(JSON.stringify(info, null, 1));
} finally { await browser.close(); }
