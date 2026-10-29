import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";

const LEAD_DAYS = 14;

let _config = null;
let _supplier = null; // { id, name }
let _lines = [];
let _busy = false;
let _initialized = false;
let _searchTimer = null;

function pad(n) { return String(n).padStart(2, "0"); }

function defaultDate() {
  const d = new Date();
  d.setDate(d.getDate() + LEAD_DAYS);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

function newLine(part = "") {
  return { part, name: "", qty: "", price: "", date: defaultDate() };
}

function fmtMoney(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toLocaleString("zh-TW", { maximumFractionDigits: 2 }) : "—";
}

function requireEdit() {
  if (isEditAuthenticated()) return true;
  showToast("目前為唯讀模式，請先登入編輯。");
  openEditAuthModal();
  return false;
}

// ── 目標公司徽章 ────────────────────────────────────────────────────────────
function renderBadge() {
  const el = document.getElementById("erp-po-badge");
  const previewBtn = document.getElementById("btn-erp-po-preview");
  if (!el) return;
  if (!_config) {
    el.className = "erp-po-badge is-unavailable";
    el.textContent = "無法使用：讀取設定失敗";
  } else if (!_config.available) {
    el.className = "erp-po-badge is-unavailable";
    el.textContent = `無法使用　${_config.reason || ""}`;
  } else {
    el.className = `erp-po-badge ${_config.is_test ? "is-test" : "is-prod"}`;
    el.textContent = `寫入：${_config.label}`;
  }
  if (previewBtn) previewBtn.disabled = !_config?.available;
}

async function loadConfig() {
  try {
    _config = await apiJson("/api/erp/po/config");
    const salesman = document.getElementById("erp-po-salesman");
    if (salesman && !salesman.value.trim()) salesman.value = _config.default_salesman || "017";
  } catch (_) {
    _config = null;
  }
  renderBadge();
}

// ── 供應商搜尋 ──────────────────────────────────────────────────────────────
function renderSupplier() {
  const el = document.getElementById("erp-po-supplier-selected");
  if (!el) return;
  el.textContent = _supplier ? `已選：${_supplier.id} ${_supplier.name}` : "尚未選擇供應商";
  el.classList.toggle("is-selected", Boolean(_supplier));
}

function selectSupplier(id, name) {
  _supplier = { id, name };
  const input = document.getElementById("erp-po-supplier-input");
  if (input) input.value = "";
  hideSupplierResults();
  renderSupplier();
}

function hideSupplierResults() {
  const list = document.getElementById("erp-po-supplier-results");
  if (list) { list.hidden = true; list.innerHTML = ""; }
}

async function searchSuppliers() {
  const input = document.getElementById("erp-po-supplier-input");
  const list = document.getElementById("erp-po-supplier-results");
  const q = input?.value.trim() || "";
  if (!list) return;
  if (!q) { hideSupplierResults(); return; }
  try {
    const data = await apiJson(`/api/erp/po/suppliers?q=${encodeURIComponent(q)}`);
    const rows = data.suppliers || [];
    list.hidden = false;
    list.innerHTML = rows.length
      ? rows.map(r => `<li data-id="${esc(r.id)}" data-name="${esc(r.short_name || r.full_name)}">
          <span class="mono">${esc(r.id)}</span> ${esc(r.short_name)} <span class="erp-po-hint">${esc(r.full_name)}</span></li>`).join("")
      : '<li class="is-empty">找不到符合的供應商</li>';
  } catch (error) {
    list.hidden = false;
    list.innerHTML = `<li class="is-empty">搜尋失敗：${esc(error.message)}</li>`;
  }
}

// ── 明細 ────────────────────────────────────────────────────────────────────
function renderLines() {
  const body = document.getElementById("erp-po-lines");
  if (!body) return;
  body.innerHTML = _lines.map((l, i) => `<tr data-idx="${i}">
    <td>${i + 1}</td>
    <td><input type="text" data-field="part" value="${esc(l.part)}" class="mono" autocomplete="off"></td>
    <td class="erp-po-name">${esc(l.name) || '<span class="erp-po-hint">—</span>'}</td>
    <td><input type="number" data-field="qty" value="${esc(l.qty)}" min="0" step="any" class="num"></td>
    <td><input type="number" data-field="price" value="${esc(l.price)}" min="0" step="any" class="num"></td>
    <td><input type="date" data-field="date" value="${esc(l.date)}"></td>
    <td><button class="btn btn-danger btn-sm" data-act="remove" type="button">移除</button></td>
  </tr>`).join("");
}

async function fillDefaults(idx) {
  const line = _lines[idx];
  const part = line?.part.trim();
  if (!part) return;
  try {
    const d = await apiJson(`/api/erp/po/part-defaults?part=${encodeURIComponent(part)}`);
    if (_lines[idx] !== line || line.part.trim() !== part) return; // 使用者已改掉
    line.name = d.found ? d.product_name : "（正航查無此料號）";
    if (d.found) line.part = d.part_number || line.part;
    if ((line.price === "" || line.price === null) && d.last_price !== null && d.last_price !== undefined) line.price = String(d.last_price);
    if ((line.qty === "" || line.qty === null) && d.suggested_qty !== null && d.suggested_qty !== undefined) line.qty = String(Math.max(1, d.suggested_qty));
    if (idx === 0 && !_supplier && d.supplier_id) _supplier = { id: d.supplier_id, name: d.supplier_name };
    renderSupplier();
    renderLines();
  } catch (error) {
    showToast(`讀取料號資料失敗：${error.message}`, { tone: "error" });
  }
}

function onLinesInput(event) {
  const field = event.target.dataset?.field;
  const tr = event.target.closest("tr[data-idx]");
  if (!field || !tr) return;
  _lines[Number(tr.dataset.idx)][field] = event.target.value;
}

function onLinesChange(event) {
  if (event.target.dataset?.field !== "part") return;
  const tr = event.target.closest("tr[data-idx]");
  if (tr) void fillDefaults(Number(tr.dataset.idx));
}

function onLinesClick(event) {
  const btn = event.target.closest('[data-act="remove"]');
  const tr = event.target.closest("tr[data-idx]");
  if (!btn || !tr) return;
  _lines.splice(Number(tr.dataset.idx), 1);
  if (!_lines.length) _lines.push(newLine());
  renderLines();
}

// 供低庫存提醒呼叫：帶入料號與建議數量
export function prefillPurchaseOrder(part, qty) {
  const empty = _lines.length === 1 && !_lines[0].part.trim();
  if (empty) _lines = [];
  const line = newLine(String(part || "").toUpperCase());
  line.qty = qty ? String(qty) : "";
  _lines.push(line);
  renderLines();
  void fillDefaults(_lines.length - 1);
}

// ── 預覽 / 寫入 ─────────────────────────────────────────────────────────────
function buildBody() {
  if (!_supplier) throw new Error("請先選擇供應商");
  const lines = _lines.filter(l => l.part.trim() || l.qty !== "" || l.price !== "");
  if (!lines.length) throw new Error("請至少輸入一行明細");
  return {
    supplier_id: _supplier.id,
    remark: document.getElementById("erp-po-remark")?.value.trim() || "",
    salesman: document.getElementById("erp-po-salesman")?.value.trim() || null,
    lines: lines.map(l => ({
      part_number: l.part.trim(),
      qty: Number(l.qty),
      price: l.price === "" ? NaN : Number(l.price),
      pre_in_date: l.date,
    })),
  };
}

function setBusy(busy) {
  _busy = busy;
  const previewBtn = document.getElementById("btn-erp-po-preview");
  if (previewBtn) previewBtn.disabled = busy || !_config?.available;
  document.getElementById("btn-erp-po-create")?.toggleAttribute("disabled", busy);
}

function hideResult() {
  const el = document.getElementById("erp-po-result");
  if (el) { el.hidden = true; el.innerHTML = ""; }
}

function renderPreview(p) {
  const el = document.getElementById("erp-po-preview");
  if (!el) return;
  const masters = p.masters_to_copy?.length
    ? `<div class="erp-po-warn">將先從來源公司複製以下主檔到 ${esc(p.target_label)}：${p.masters_to_copy.map(m => `${esc(m.label)} ${esc(m.key)}`).join("、")}</div>`
    : "";
  el.innerHTML = `
    <div class="erp-po-badge ${p.is_test ? "is-test" : "is-prod"} erp-po-badge-small">寫入：${esc(p.target_label)}</div>
    <div class="erp-po-preview-head">預計單號 <strong class="mono">${esc(p.bill_no)}</strong>（實際單號以寫入結果為準）　供應商 ${esc(p.supplier.id)} ${esc(p.supplier.name)}　採購員 ${esc(p.salesman)}</div>
    <div class="erp-po-hint">付款條件 / 地址取自範本單 ${esc(p.template_bill_no)}${p.remark ? `　備註：${esc(p.remark)}` : ""}</div>
    ${masters}
    <table class="erp-po-table">
      <thead><tr><th>#</th><th>料號</th><th>品名</th><th class="num">數量</th><th class="num">單價</th><th class="num">未稅金額</th><th class="num">稅額</th><th>預計進貨日</th></tr></thead>
      <tbody>${p.lines.map(l => `<tr><td>${l.row_no}</td><td class="mono">${esc(l.part_number)}</td><td>${esc(l.product_name)}</td>
        <td class="num">${fmtMoney(l.qty)}</td><td class="num">${fmtMoney(l.price)}</td><td class="num">${fmtMoney(l.amount)}</td>
        <td class="num">${fmtMoney(l.tax_amt)}</td><td>${esc(l.pre_in_date)}</td></tr>`).join("")}</tbody>
    </table>
    <div class="erp-po-totals">未稅 <strong>${fmtMoney(p.totals.sum_amount)}</strong>　稅額 <strong>${fmtMoney(p.totals.sum_tax)}</strong>　合計 <strong>${fmtMoney(p.totals.total)}</strong></div>
    <div class="erp-po-actions"><span class="spacer"></span>
      <button class="btn btn-danger" id="btn-erp-po-create" type="button">確認寫入正航</button></div>`;
  el.hidden = false;
  el.dataset.target = p.target_label;
  document.getElementById("btn-erp-po-create")?.addEventListener("click", handleCreate);
}

async function handlePreview() {
  if (_busy || !requireEdit()) return;
  hideResult();
  let body;
  try { body = buildBody(); } catch (error) { showToast(error.message, { tone: "error" }); return; }
  if (body.lines.some(l => !Number.isFinite(l.qty) || !Number.isFinite(l.price))) {
    showToast("請填寫每一行的數量與單價", { tone: "error" });
    return;
  }
  setBusy(true);
  try {
    renderPreview(await apiPost("/api/erp/po/preview", body));
  } catch (error) {
    const el = document.getElementById("erp-po-preview");
    if (el) { el.hidden = true; el.innerHTML = ""; }
    showToast(`預覽失敗：${error.message}`, { tone: "error", sticky: true });
  } finally {
    setBusy(false);
  }
}

async function handleCreate() {
  if (_busy || !requireEdit()) return;
  const label = document.getElementById("erp-po-preview")?.dataset.target || _config?.label || "";
  if (!window.confirm(`確定要在「${label}」建立這張採購單嗎？\n寫入後無法從本系統修改或刪除。`)) return;
  let body;
  try { body = buildBody(); } catch (error) { showToast(error.message, { tone: "error" }); return; }
  setBusy(true);
  const result = document.getElementById("erp-po-result");
  try {
    const r = await apiPost("/api/erp/po/create", { ...body, confirm: true });
    if (result) {
      result.className = "erp-po-result is-ok";
      result.innerHTML = `✅ 已寫入 ${esc(r.target_label)}：採購單 <strong class="mono">${esc(r.bill_no)}</strong>，含稅合計 ${fmtMoney(r.totals.total)}`;
      result.hidden = false;
    }
    const preview = document.getElementById("erp-po-preview");
    if (preview) { preview.hidden = true; preview.innerHTML = ""; }
    _lines = [newLine()];
    renderLines();
    showToast(`已建立採購單 ${r.bill_no}`, { tone: "success" });
  } catch (error) {
    if (result) {
      result.className = "erp-po-result is-error";
      result.textContent = `❌ 寫入失敗：${error.message}`;
      result.hidden = false;
    }
  } finally {
    setBusy(false);
    void refreshHistory();
  }
}

// ── 歷史 ────────────────────────────────────────────────────────────────────
async function refreshHistory() {
  const body = document.getElementById("erp-po-history");
  if (!body) return;
  try {
    const data = await apiJson("/api/erp/po/history");
    const rows = data.orders || [];
    body.innerHTML = rows.length ? rows.map(o => {
      let lines = [];
      try { lines = JSON.parse(o.lines_json || "[]"); } catch (_) { /* 舊資料格式異常則略過 */ }
      const first = lines.slice(0, 2).map(l => `${esc(l.part_number)}×${fmtMoney(l.qty)}`).join("、");
      const more = lines.length > 2 ? ` 等 ${lines.length} 行` : "";
      const ok = o.status === "ok";
      return `<tr class="${ok ? "" : "erp-po-row-error"}">
        <td>${esc(String(o.created_at).slice(0, 16).replace("T", " "))}</td>
        <td class="mono">${esc(o.bill_no) || "—"}</td>
        <td>${esc(o.target_db)}</td>
        <td>${esc(o.supplier_id)} ${esc(o.supplier_name)}</td>
        <td>${first}${more}</td>
        <td class="num">${ok ? fmtMoney(o.total) : "—"}</td>
        <td>${ok ? "成功" : `<span title="${esc(o.error)}">失敗：${esc(String(o.error).slice(0, 40))}</span>`}</td></tr>`;
    }).join("") : '<tr><td colspan="7" class="erp-po-hint">尚無紀錄</td></tr>';
  } catch (error) {
    body.innerHTML = `<tr><td colspan="7" class="erp-po-hint">讀取失敗：${esc(error.message)}</td></tr>`;
  }
}

// 供缺料採購在寫入後更新開採購單的歷史紀錄
export function refreshErpPurchaseHistory() {
  return refreshHistory();
}

export async function onErpPurchaseTabActivated() {
  await Promise.all([loadConfig(), refreshHistory()]);
}

export async function initErpPurchase() {
  if (_initialized) return;
  _initialized = true;
  _lines = [newLine()];
  renderLines();
  renderSupplier();
  const body = document.getElementById("erp-po-lines");
  body?.addEventListener("input", onLinesInput);
  body?.addEventListener("change", onLinesChange);
  body?.addEventListener("click", onLinesClick);
  document.getElementById("btn-erp-po-add-line")?.addEventListener("click", () => {
    if (_lines.length >= 50) { showToast("最多 50 行", { tone: "error" }); return; }
    _lines.push(newLine());
    renderLines();
  });
  document.getElementById("btn-erp-po-preview")?.addEventListener("click", handlePreview);
  document.getElementById("erp-po-supplier-input")?.addEventListener("input", () => {
    clearTimeout(_searchTimer);
    _searchTimer = setTimeout(() => { void searchSuppliers(); }, 250);
  });
  document.getElementById("erp-po-supplier-results")?.addEventListener("click", event => {
    const li = event.target.closest("li[data-id]");
    if (li) selectSupplier(li.dataset.id, li.dataset.name);
  });
  await loadConfig();
}
