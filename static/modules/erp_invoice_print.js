// 開 Invoice 列印：仿正航「KPM5-INVOICE」格式（A4，window.print）。
// renderInvoicePrint(data, root) 是純函式（只動 root），離線測試可先設 window.__INVOICE_PRINT_DATA__ 再載入本模組，就不會 fetch。

export const BILL_RE = /^ST-\d{7}$/i;

function esc(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

/** 千分位 + 固定小數位：fmtNum(5000, 4) = "5,000.0000"。 */
export function fmtNum(value, places) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  return n.toLocaleString("en-US", { minimumFractionDigits: places, maximumFractionDigits: places });
}

/** 品名切段：英數字與空白（窄字距）/ 其他標點與中文（正常字距，例 "---" 才不會黏成一條長線）。 */
export function descRuns(text) {
  return (String(text ?? "").match(/[A-Za-z0-9\s]+|[^A-Za-z0-9\s]+/g) || [])
    .map(t => ({ text: t, narrow: /^[A-Za-z0-9\s]/.test(t) }));
}

export function descHtml(text) {
  return descRuns(text).map(r => (r.narrow ? esc(r.text) : `<span class="inv-ls">${esc(r.text)}</span>`)).join("");
}

function companyBlock(c = {}) {
  const contact = [c.tel ? `TEL: ${esc(c.tel)}` : "", c.fax ? `FAX: ${esc(c.fax)}` : ""].filter(Boolean).join(" ");
  return `<header class="inv-company">
      <div class="inv-company-name">${esc(c.name)}</div>
      <div class="inv-company-line">${esc(c.address)}</div>
      <div class="inv-company-line">${contact}</div>
    </header>`;
}

function lineRow(line) {
  return `<div class="inv-cols inv-line">
      <div class="c-qty">${esc(line.qty_display ?? fmtNum(line.qty, 4))}</div><div></div>
      <div class="c-desc"><div class="inv-clamp">${descHtml(line.print_description)}</div></div>
      <div class="c-price">${esc(line.price_display ?? fmtNum(line.price, 3))}</div>
      <div class="c-amt">${esc(line.amount_display ?? fmtNum(line.amount, 3))}</div>
    </div>`;
}

function poRow(po) {
  return `<div class="inv-cols inv-po"><div class="c-qty"></div><div></div><div class="c-desc">PO#${esc(po)}</div><div></div><div></div></div>`;
}

/** 明細依序切成不可拆的單位：PO 換了的那一列帶 PO 標題（標題與第一列綁在一起）。 */
export function buildUnits(lines) {
  let prev = null;
  return (lines || []).map((l, i) => {
    const po = l.po || "";
    const unit = { line: i, po, header: Boolean(po) && (i === 0 || po !== prev) };
    prev = po;
    return unit;
  });
}

/**
 * 依實際高度分頁（純函式）。heights.po / heights.line[i] = PO 標題列 / 第 i 列明細的高度；
 * capacity = 每頁明細區可用高度；reserve = 最後一頁合計 + 簽核要多佔的高度。
 * 新頁從同一 PO 的中間開始時重印 PO 標題；最後一頁放不下合計 + 簽核時，把最後一個單位移到下一頁（最後一頁不會只剩合計）。
 */
export function paginateUnits(units, heights, capacity, reserve) {
  const unitHeight = (u, header) => (header ? heights.po : 0) + heights.line[u.line];
  const pages = [];
  let cur = [];
  let used = 0;
  for (const u of units) {
    const header = u.header || (cur.length === 0 && Boolean(u.po));
    const h = unitHeight(u, header);
    if (cur.length && used + h > capacity) {
      pages.push(cur);
      const again = Boolean(u.po);
      cur = [{ ...u, header: again }];
      used = unitHeight(u, again);
    } else {
      cur.push({ ...u, header });
      used += h;
    }
  }
  if (cur.length > 1 && used + reserve > capacity) {
    const last = cur.pop();
    pages.push(cur);
    cur = [{ ...last, header: Boolean(last.po) }];
  }
  pages.push(cur);
  return pages.map((p, n) => ({
    no: n + 1, of: pages.length, last: n === pages.length - 1,
    rows: p.flatMap(u => (u.header ? [{ type: "po", po: u.po }, { type: "line", line: u.line }] : [{ type: "line", line: u.line }])),
  }));
}

// 沒有 DOM 可量（例 Node 測試）時的估計：以「行」為單位，品名每 50 個半形字算 1 行、最多 3 行
const EST_LINES_PER_PAGE = 38;
const EST_RESERVE_LINES = 6;

function estimateHeights(lines) {
  const width = t => [...String(t || "")].reduce((n, ch) => n + (/[ᄀ-ᅟ⺀-꓏가-힣豈-﫿︰-﹏＀-｠￠-￦]/.test(ch) ? 2 : 1), 0);
  return { po: 1, line: (lines || []).map(l => Math.max(1, Math.min(3, Math.ceil(width(l.print_description) / 50)))) };
}

const PAGE_HEIGHT_MM = 277;   // A4 297mm − 上下邊界各 10mm
const PAGE_SAFETY_MM = 3;     // 量測與列印的捨入誤差

/** 在 root 裡放一張看不見的量測頁（列印時的寬度 190mm、同字型），量 PO 標題 / 每列明細 / 合計 + 簽核的實際高度。 */
function measureLayout(data, root) {
  const doc = root.ownerDocument;
  const probe = doc.createElement("div");
  probe.style.cssText = "position:absolute;left:-10000px;top:0;visibility:hidden;width:190mm;";
  root.appendChild(probe);
  try {
    const units = buildUnits(data.lines);
    const allRows = units.flatMap(u => [{ type: "po", po: u.po || "-" }, { type: "line", line: u.line }]);
    probe.innerHTML = sheet(data, { no: 1, rows: [], last: false }, 1);
    const s = probe.firstElementChild;
    s.style.cssText = "width:190mm;min-height:0;margin:0;padding:0;box-shadow:none;";
    const mm = doc.createElement("div");
    mm.style.cssText = "height:100mm;";
    probe.appendChild(mm);
    const pxPerMm = mm.getBoundingClientRect().height / 100;
    const base = s.getBoundingClientRect().height;
    probe.innerHTML = sheet(data, { no: 1, rows: allRows, last: true }, 1);
    const full = probe.firstElementChild;
    full.style.cssText = s.style.cssText;
    const rowEls = full.querySelectorAll(".inv-body > div");
    const heights = { po: 0, line: [] };
    allRows.forEach((r, i) => {
      const h = rowEls[i].getBoundingClientRect().height;
      if (r.type === "po") heights.po = Math.max(heights.po, h);
      else heights.line[r.line] = h;
    });
    const tail = el => (el ? el.getBoundingClientRect().height + parseFloat(getComputedStyle(el).marginTop || 0) : 0);
    const reserve = tail(full.querySelector(".inv-totals")) + tail(full.querySelector(".inv-sign"));
    const capacity = (PAGE_HEIGHT_MM - PAGE_SAFETY_MM) * pxPerMm - base;
    return { heights, capacity, reserve, pxPerMm };
  } finally {
    probe.remove();
  }
}

function layoutPages(data, root) {
  const units = buildUnits(data.lines);
  const canMeasure = root && root.ownerDocument && typeof root.appendChild === "function" && typeof getComputedStyle === "function";
  if (canMeasure) {
    const m = measureLayout(data, root);
    return paginateUnits(units, m.heights, m.capacity, m.reserve);
  }
  return paginateUnits(units, estimateHeights(data.lines), EST_LINES_PER_PAGE, EST_RESERVE_LINES);
}

function sheet(data, page, total) {
  const t = data.totals || {};
  const body = (page.rows || []).map(r => (r.type === "po" ? poRow(r.po) : lineRow(data.lines[r.line]))).join("");
  const totals = page.last ? `<div class="inv-cols inv-totals">
      <div class="c-qty"><span class="inv-dbl">${esc(t.qty_display ?? fmtNum(t.qty, 4))}</span></div><div></div><div></div>
      <div class="c-sum"><span class="inv-dbl">${esc(data.currency)} ${esc(t.amount_display ?? fmtNum(t.amount, 3))}</span></div>
    </div>` : "";
  const signs = page.last ? `<div class="inv-sign">${(data.signatures || []).map(s => `<span class="inv-sign-item">${esc(s)} <span class="inv-sign-line"></span></span>`).join("")}</div>` : "";
  return `<section class="inv-sheet">
    ${companyBlock(data.company)}
    <div class="inv-title"><span>${esc(data.title)}</span></div>
    <div class="inv-pageno">Page ${page.no} of ${total}</div>
    <div class="inv-box">
      <div class="inv-row-a"><div>INVOICE:&nbsp;&nbsp;${esc(data.bill_no)}</div><div>DATE:&nbsp;&nbsp;${esc(data.date_display)}</div></div>
      <div class="inv-row-b">客戶名稱:&nbsp;${esc(data.customer?.name)}</div>
      <div class="inv-cols inv-head">
        <div class="c-qty-head">QTY</div><div></div><div class="c-desc">DESCRIPTION</div>
        <div class="c-price"><div class="inv-price-head">UNIT PRICE${data.trade_term ? `<div class="inv-term"><span>${esc(data.trade_term)}</span></div>` : ""}</div></div>
        <div class="c-amt">TTL AMOUNT</div>
      </div>
      <div class="inv-body">${body}</div>
      ${totals}
      ${signs}
    </div>
  </section>`;
}

/** 純函式：把列印資料畫進 root（每頁一張 A4，依實際高度分頁、Page n of N 用實際頁數）。 */
export function renderInvoicePrint(data, root) {
  // 一律在瀏覽器依實際高度分頁；後端回的 pages 只是估計，不用
  const pages = layoutPages(data, root);
  root.innerHTML = pages.map(p => sheet(data, p, pages.length)).join("");
  return pages.length;
}

export function renderError(message, root) {
  root.innerHTML = `<div class="inv-error">無法載入 Invoice 列印資料：${esc(message)}</div>`;
}

async function loadData(bill) {
  if (!BILL_RE.test(bill)) throw new Error(`單號格式不正確（需為 ST-2026xxx）：${bill || "未指定"}`);
  const { apiJson } = await import("/static/modules/api.js");
  return apiJson(`/api/erp-invoice/print-data?bill_no=${encodeURIComponent(bill.toUpperCase())}`);
}

async function main() {
  const root = document.getElementById("invoice-root");
  if (!root) return;
  document.getElementById("btn-print")?.addEventListener("click", () => window.print());
  document.getElementById("lnk-close")?.addEventListener("click", event => { event.preventDefault(); window.close(); });
  const params = new URLSearchParams(window.location.search);
  const bill = (params.get("bill") || "").trim();
  const status = document.getElementById("print-status");
  try {
    const data = window.__INVOICE_PRINT_DATA__ !== undefined ? window.__INVOICE_PRINT_DATA__ : await loadData(bill);
    if (document.fonts?.ready) await document.fonts.ready;  // 字型到位才量高度
    renderInvoicePrint(data, root);
    document.title = `${data.bill_no || bill} 銷貨單`;
    if (status && data.excluded_tab_rows?.length) {
      status.textContent = `已排除 ${data.excluded_tab_rows.length} 列 -TAB 料號（數量 ${fmtNum(data.excluded_tab_qty, 4)}、金額 ${fmtNum(data.excluded_tab_amount, 3)}），未列印`;
    }
    if (params.get("auto") === "1") setTimeout(() => window.print(), 300);
  } catch (error) {
    renderError(error.message || String(error), root);
  }
}

if (typeof document !== "undefined") void main();
