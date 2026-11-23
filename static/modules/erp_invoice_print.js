// 開 Invoice 列印：仿正航「KPM5-INVOICE」格式（A4，window.print）。
// 分頁、品名折行、所有座標都照後端 print-data 的 print_pages / print_geometry（= 伺服器 PDF invoice_print_pdf.layout），
// 每頁一張 210×297mm 的 SVG（單位 mm、y 為文字基線），不自己估高度。
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

const DEFAULT_SIGNATURES = ["簽核", "審核", "製單"];

/** SVG 文字：anchor = start / middle / end；fit = 最大寬（mm），超過時 fitTexts() 水平壓縮（同 PDF）。 */
function text(x, y, s, { size, anchor = "start", bold = false, fit = null, cls = "" } = {}) {
  if (s === "" || s == null) return "";
  const attrs = [`x="${x}"`, `y="${y}"`, `font-size="${size}"`];
  if (anchor !== "start") attrs.push(`text-anchor="${anchor}"`);
  if (bold) attrs.push('font-weight="bold"');
  if (fit) attrs.push(`data-fit="${fit}"`);
  if (cls) attrs.push(`class="${cls}"`);
  return `<text ${attrs.join(" ")}>${esc(s)}</text>`;
}

function hline(x0, x1, y) {
  return `<line x1="${x0}" y1="${y}" x2="${x1}" y2="${y}" stroke="#000"/>`;
}

function vline(x, y0, y1) {
  return `<line x1="${x}" y1="${y0}" x2="${x}" y2="${y1}" stroke="#000"/>`;
}

function rect(x0, x1, y0, y1, w) {
  return `<rect x="${x0}" y="${y0}" width="${x1 - x0}" height="${y1 - y0}" fill="none" stroke="#000" stroke-width="${w}"/>`;
}

/** 一頁：表頭、外粗框 + 內細框（內框左右細線一路到底）、明細、合計（最後一頁）、簽核（每一頁），同 PDF _header / _body / _footer。 */
function sheet(data, page, g) {
  const c = data.company || {};
  const t = data.totals || {};
  const S = g.size;
  const out = [];
  // 表頭
  out.push(text(...g.company, c.name, { size: g.company_size, anchor: "middle" }));
  out.push(text(...g.address, c.address, { size: S, anchor: "middle" }));
  out.push(text(...g.tel, `TEL: ${c.tel || ""} FAX: ${c.fax || ""}`, { size: S, anchor: "middle" }));
  out.push(text(...g.title, data.title || "銷貨單", { size: g.title_size, anchor: "middle" }));
  out.push(hline(...g.title_ul));
  out.push(text(...g.page_no, `Page ${page.no} of ${page.of}`, { size: S, anchor: "end" }));
  // 框與格線
  out.push(rect(g.frame_x[0], g.frame_x[1], g.frame_top, page.frame_bottom, g.frame_line_w));
  out.push(rect(g.inner_x[0], g.inner_x[1], g.inner_top, page.frame_bottom - g.inner_bottom_pad, g.line_w));
  for (const y of [g.row_a_bottom, g.row_b_bottom, g.head_bottom]) out.push(hline(g.inner_x[0], g.inner_x[1], y));
  out.push(vline(g.row_a_div, g.inner_top, g.row_a_bottom));
  for (const x of g.head_vlines) out.push(vline(x, g.row_b_bottom, g.head_bottom));
  out.push(text(g.text_x, g.row_a_base, "INVOICE:", { size: S }));
  out.push(text(g.invoice_value_x, g.row_a_base, data.bill_no, { size: S }));
  out.push(text(g.date_x, g.row_a_base, "DATE:", { size: S }));
  out.push(text(g.date_value_x, g.row_a_base, data.date_display, { size: S }));
  out.push(text(g.text_x, g.row_b_base, `客戶名稱: ${data.customer?.name || ""}`, { size: S, fit: g.customer_max_w }));
  out.push(text(g.text_x, g.head_base, "QTY", { size: S }));
  out.push(text(g.head_desc_x, g.head_base, "DESCRIPTION", { size: S }));
  out.push(text(g.head_price_right, g.head_base, "UNIT PRICE", { size: S, anchor: "end" }));
  out.push(text(g.head_amount_cx, g.head_base, "TTL AMOUNT", { size: S, anchor: "middle" }));
  if (data.trade_term && g.term_x != null) {
    out.push(text(g.term_x, g.term_base, data.trade_term, { size: S }));
    out.push(hline(g.term_x - 0.9, g.head_vlines[2], g.term_ul_y));
  }
  // 明細：PO 非空的每一列都印粗體 PO#，品名照 PDF 折好的行
  for (const l of page.lines) {
    if (l.po_base != null) out.push(text(g.po_x, l.po_base, `PO#${l.po}`, { size: S, bold: true, cls: "inv-po" }));
    out.push(text(g.qty_right, l.base, l.qty, { size: S, anchor: "end", cls: "inv-qty" }));
    out.push(text(g.price_right, l.base, l.price, { size: S, anchor: "end", cls: "inv-price" }));
    out.push(text(g.amount_x, l.base, l.amount, { size: S, cls: "inv-amt" }));  // TTL AMOUNT 靠左（同正航）
    l.desc.forEach((s, i) => out.push(text(g.desc_x, l.base + i * g.desc_pitch, s, { size: S, fit: g.desc_max_w, cls: "inv-desc" })));
  }
  // 合計（最後一頁）：細線、數量 / 金額雙底線，幣別在 166.8–175.8 置中、金額靠左
  if (page.last) {
    out.push(hline(g.inner_x[0], g.inner_x[1], page.totals_rule));
    const base = page.totals_base;
    out.push(text(g.qty_right, base, t.qty_display ?? fmtNum(t.qty, 4), { size: S, anchor: "end", cls: "inv-total-qty" }));
    for (const dy of g.double_ul) out.push(hline(g.qty_ul[0], g.qty_ul[1], base + dy));
    const amountBase = base + g.amount_base_dy;
    out.push(text(g.currency_cx, amountBase, data.currency, { size: S, anchor: "middle" }));
    out.push(text(g.amount_x, amountBase, t.amount_display ?? fmtNum(t.amount, 3), { size: S, cls: "inv-total-amt" }));
    for (const dy of g.amount_double_ul) out.push(hline(g.amount_ul[0], g.amount_ul[1], amountBase + dy));
  }
  // 簽核每一頁都印（非最後一頁在頁尾、最後一頁緊接合計）：標籤 + 空白 + 底線字元，同正航
  const labels = (data.signatures?.length ? data.signatures : DEFAULT_SIGNATURES).slice(0, 3);
  labels.forEach((label, i) => out.push(text(g.sign_x[i], page.sign_base, `${label} ${"_".repeat(g.sign_underscores)}`,
    { size: S, cls: "inv-sign" })));
  return `<section class="inv-sheet" data-page="${page.no}" data-last="${page.last}">
    <svg class="inv-svg" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 210 297" width="210mm" height="297mm" fill="#000" stroke-width="${g.line_w}">
      ${out.join("\n      ")}
    </svg>
  </section>`;
}

/** 超過最大寬的文字整段水平壓縮（同 PDF Tz）；沒有 DOM（Node 測試）就略過。 */
function fitTexts(root) {
  if (typeof root.querySelectorAll !== "function") return;
  for (const el of root.querySelectorAll("text[data-fit]")) {
    const max = Number(el.getAttribute("data-fit"));
    if (typeof el.getComputedTextLength === "function" && el.getComputedTextLength() > max) {
      el.setAttribute("textLength", String(max));
      el.setAttribute("lengthAdjust", "spacingAndGlyphs");
    }
  }
}

/** 純函式：把列印資料畫進 root，頁數 = print_pages（= 伺服器 PDF 頁數）。 */
export function renderInvoicePrint(data, root) {
  const pages = data.print_pages;
  const g = data.print_geometry;
  if (!Array.isArray(pages) || !pages.length || !g) throw new Error("列印資料缺少 print_pages / print_geometry（伺服器版本太舊）");
  root.innerHTML = pages.map(p => sheet(data, p, g)).join("");
  fitTexts(root);
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
    if (document.fonts?.ready) await document.fonts.ready;  // 字型到位才量字寬（超寬壓縮）
    renderInvoicePrint(data, root);
    document.title = `${data.bill_no || bill} 銷貨單`;
    const notes = [data.print_form === "half" ? "A4 上半張" : `A4 ${data.print_pages.length} 頁`];
    if (data.excluded_tab_rows?.length) {
      notes.push(`已排除 ${data.excluded_tab_rows.length} 列 -TAB 料號（數量 ${fmtNum(data.excluded_tab_qty, 4)}、金額 ${fmtNum(data.excluded_tab_amount, 3)}），未列印`);
    }
    if (status) status.textContent = notes.join("；");
    if (params.get("auto") === "1") setTimeout(() => window.print(), 300);
  } catch (error) {
    renderError(error.message || String(error), root);
  }
}

if (typeof document !== "undefined") void main();
