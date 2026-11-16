// 出貨單：正航 S/C / Invoice / 客戶 PO 帶品項，填箱號箱數後匯出 OPENTEXT 出貨單 xlsx
import { apiPost, esc, showToast } from "./api.js";
import { desktopDownload, showDownloadToast } from "./desktop_bridge.js";

let _items = [];
let _searching = false;

const BOX_FIELDS = [
  "carton_no", "box_count", "per_box_qty", "net_weight", "gross_weight", "volume",
  "tail_carton_no", "tail_qty", "tail_net_weight", "tail_gross_weight", "tail_volume",
];
const TEXT_FIELDS = new Set(["carton_no", "tail_carton_no"]);

function _todayIso() {
  const d = new Date();
  const pad = n => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

function _num(value) {
  const n = Number(String(value ?? "").replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : 0;
}

function _round(n, digits = 4) {
  const f = 10 ** digits;
  return Math.round(n * f) / f;
}

function _money(n) {
  return _num(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function _isBilling() {
  return document.getElementById("shipnote-type")?.value === "billing";
}

export function initShipnote() {
  const dateInput = document.getElementById("shipnote-date");
  if (dateInput && !dateInput.value) dateInput.value = _todayIso();
  document.getElementById("btn-shipnote-search")?.addEventListener("click", () => void searchItems());
  document.getElementById("shipnote-keywords")?.addEventListener("keydown", event => {
    if (event.key === "Enter") {
      event.preventDefault();
      void searchItems();
    }
  });
  document.getElementById("shipnote-type")?.addEventListener("change", _applyTypeLayout);
  document.getElementById("shipnote-check-all")?.addEventListener("change", event => {
    _items.forEach(item => { item.checked = event.target.checked; });
    _renderItems();
  });
  document.getElementById("shipnote-body")?.addEventListener("input", _onCellInput);
  document.getElementById("shipnote-body")?.addEventListener("change", _onCellChange);
  document.getElementById("btn-shipnote-export")?.addEventListener("click", () => void exportShipnote());
  _applyTypeLayout();
}

export function onShipnoteTabActivated() {
  const dateInput = document.getElementById("shipnote-date");
  if (dateInput && !dateInput.value) dateInput.value = _todayIso();
  document.getElementById("shipnote-keywords")?.focus();
}

function _applyTypeLayout() {
  document.getElementById("shipnote-table")?.classList.toggle("shipnote-billing", _isBilling());
}

// 依每箱數量帶整箱數與尾箱；尾箱 N.W. 照比例、材積同整箱，尾箱 G.W. 留給人填
function _fillBoxes(item) {
  const perBox = _num(item.per_box_qty);
  const qty = _num(item.qty);
  if (!(perBox > 0) || !(qty > 0)) return;
  const full = Math.floor(qty / perBox);
  const tail = _round(qty - full * perBox);
  item.box_count = full || "";
  if (tail > 0) {
    item.tail_qty = tail;
    item.tail_net_weight = _num(item.net_weight) ? _round(_num(item.net_weight) * tail / perBox, 2) : "";
    item.tail_volume = _num(item.volume) || "";
  } else {
    item.tail_qty = "";
    item.tail_net_weight = "";
    item.tail_volume = "";
    item.tail_gross_weight = "";
    item.tail_carton_no = "";
  }
}

export async function searchItems() {
  if (_searching) return;
  const keywords = document.getElementById("shipnote-keywords")?.value.trim() || "";
  const status = document.getElementById("shipnote-status");
  if (!keywords) {
    showToast("請輸入 S/C、Invoice 單號或客戶 PO");
    return;
  }
  _searching = true;
  if (status) status.innerHTML = "<span>查詢正航中...</span>";
  try {
    const data = await apiPost("/api/shipnote/search", { keywords });
    _items = (data.items || []).map(item => {
      const row = { ...item, checked: true };
      BOX_FIELDS.forEach(key => { if (row[key] == null) row[key] = ""; });
      _fillBoxes(row);
      return row;
    });
    _renderItems();
    const parts = [`<span>${_items.length} 筆品項</span>`];
    const missing = _items.filter(item => !item.spec_matched).length;
    if (missing) parts.push(`<span class="sea-warn">${missing} 筆淨重主檔沒有，箱重請自行填</span>`);
    if ((data.not_found || []).length) {
      parts.push(`<span class="sea-warn">查無：${esc(data.not_found.join("、"))}</span>`);
    }
    if (status) status.innerHTML = parts.join("");
  } catch (error) {
    if (status) status.innerHTML = "";
    showToast(`查詢失敗：${error.message}`, { tone: "error", sticky: true });
  } finally {
    _searching = false;
  }
}

function _input(index, key, value, cls = "") {
  const type = TEXT_FIELDS.has(key) ? "text" : "number";
  const step = type === "number" ? ' step="any"' : "";
  return `<input class="shipnote-input ${cls}" type="${type}"${step} data-index="${index}" data-key="${key}" value="${esc(value ?? "")}">`;
}

function _renderItems() {
  const body = document.getElementById("shipnote-body");
  if (!body) return;
  if (!_items.length) {
    body.innerHTML = '<tr><td colspan="20" class="shipnote-empty">查無品項</td></tr>';
    return;
  }
  body.innerHTML = _items.map((item, index) => `
    <tr class="${item.checked ? "" : "shipnote-unchecked"}">
      <td><input type="checkbox" data-index="${index}" data-key="checked" ${item.checked ? "checked" : ""}></td>
      <td class="shipnote-po">${esc(item.cust_po || "")}</td>
      <td title="${esc(item.source)}${item.source === "Invoice" && item.sc_no ? `（S/C ${esc(item.sc_no)}）` : ""}">${esc(item.bill_no)}</td>
      <td>${esc(item.row_no)}</td>
      <td>${esc(item.item_no)}</td>
      <td class="shipnote-name" title="${esc(item.erp_description || "")}">${esc(item.name || "")}</td>
      <td>${_input(index, "qty", item.qty, "shipnote-qty")}</td>
      <td class="shipnote-num">${esc(item.price)}</td>
      <td class="shipnote-num" data-amount="${index}">${_money(_num(item.qty) * _num(item.price))}</td>
      ${BOX_FIELDS.map(key => `<td class="shipnote-box">${_input(index, key, item[key])}</td>`).join("")}
    </tr>`).join("");
  const all = document.getElementById("shipnote-check-all");
  if (all) all.checked = _items.every(item => item.checked);
}

function _onCellInput(event) {
  const el = event.target;
  const index = Number(el.dataset.index);
  const key = el.dataset.key;
  if (!key || !_items[index] || key === "checked") return;
  _items[index][key] = el.value;
  if (key === "qty") {
    const cell = document.querySelector(`[data-amount="${index}"]`);
    if (cell) cell.textContent = _money(_num(el.value) * _num(_items[index].price));
  }
}

function _onCellChange(event) {
  const el = event.target;
  const index = Number(el.dataset.index);
  const key = el.dataset.key;
  if (!_items[index]) return;
  if (key === "checked") {
    _items[index].checked = el.checked;
    el.closest("tr")?.classList.toggle("shipnote-unchecked", !el.checked);
    const all = document.getElementById("shipnote-check-all");
    if (all) all.checked = _items.every(item => item.checked);
    return;
  }
  if (key === "qty" || key === "per_box_qty") {
    _fillBoxes(_items[index]);
    _renderItems();
  }
}

export async function exportShipnote() {
  const selected = _items.filter(item => item.checked);
  if (!selected.length) {
    showToast("請先勾選要匯出的品項");
    return;
  }
  if (selected.some(item => !(_num(item.qty) > 0))) {
    showToast("勾選的品項數量需大於 0", { tone: "error" });
    return;
  }
  const body = {
    type: document.getElementById("shipnote-type")?.value || "general",
    date: document.getElementById("shipnote-date")?.value || _todayIso(),
    invoice_no: document.getElementById("shipnote-invoice-no")?.value.trim() || "",
    maker: document.getElementById("shipnote-maker")?.value.trim() || "Andy",
    mark: document.getElementById("shipnote-mark")?.value || "HILLIARD",
    remark: document.getElementById("shipnote-remark")?.value.trim() || "",
    items: selected.map(item => {
      const row = {
        cust_po: item.cust_po,
        item_no: item.item_no,
        name: item.name,
        erp_description: item.erp_description,
        harmonized_code: item.harmonized_code,
        qty: _num(item.qty),
        price: _num(item.price),
      };
      BOX_FIELDS.forEach(key => { row[key] = TEXT_FIELDS.has(key) ? String(item[key] ?? "").trim() : _num(item[key]); });
      return row;
    }),
  };
  try {
    const result = await desktopDownload({ path: "/api/shipnote/export", method: "POST", body });
    showDownloadToast(result, "出貨單");
  } catch (error) {
    showToast(`匯出失敗：${error.message}`, { tone: "error", sticky: true });
  }
}
