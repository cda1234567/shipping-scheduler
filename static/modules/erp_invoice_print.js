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
      <div class="c-desc"><div class="inv-clamp">${esc(line.print_description)}</div></div>
      <div class="c-price">${esc(line.price_display ?? fmtNum(line.price, 3))}</div>
      <div class="c-amt">${esc(line.amount_display ?? fmtNum(line.amount, 3))}</div>
    </div>`;
}

function poRow(po) {
  return `<div class="inv-cols inv-po"><div class="c-qty"></div><div></div><div class="c-desc">PO#${esc(po)}</div><div></div><div></div></div>`;
}

function pagesOf(data) {
  if (Array.isArray(data.pages) && data.pages.length) return data.pages;
  // 後端沒給排版時：全部放一頁（PO 換了才印 PO#）
  const rows = [];
  let prev = null;
  (data.lines || []).forEach((l, i) => {
    if ((i === 0 || l.po !== prev) && l.po) rows.push({ type: "po", po: l.po });
    prev = l.po;
    rows.push({ type: "line", line: i });
  });
  return [{ no: 1, of: 1, rows, last: true }];
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
      <div class="inv-spacer"></div>
      ${signs}
    </div>
  </section>`;
}

/** 純函式：把列印資料畫進 root（每頁一張 A4）。 */
export function renderInvoicePrint(data, root) {
  const pages = pagesOf(data);
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
