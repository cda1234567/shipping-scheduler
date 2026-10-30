import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";
import { allocationText, refreshErpPurchaseHistory } from "/static/modules/erp_purchase.js";

const CATEGORY_LABELS = { smt: "打件", backend: "後段" };
const SOURCE_LABELS = { master: "主檔", last_po: "最近採購", none: "未設定", manual: "手動" };
const MAX_GROUPS = 20;

let _config = null;
let _orders = [];
let _checkedOrders = new Set();
let _ordersLoaded = false;
let _data = null; // /api/shortage-po/list 回應
let _listOrderIds = []; // 產生目前缺料清單時勾選的訂單，預覽 / 寫入時送給伺服器重算訂單分配
let _rows = [];
let _remarks = {}; // supplierId → 使用者改過的備註
let _showAll = false;
let _busy = false;
let _initialized = false;
let _pendingIds = null;
let _previewSig = "";
let _previewResults = null;

function fmtQty(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toLocaleString("zh-TW", { maximumFractionDigits: 2 }) : "—";
}

function requireEdit() {
  if (isEditAuthenticated()) return true;
  showToast("目前為唯讀模式，請先登入編輯。");
  openEditAuthModal();
  return false;
}

function el(id) { return document.getElementById(id); }

// ── 目標公司徽章（與開採購單相同）──────────────────────────────────────────
function renderBadge() {
  const badge = el("shortage-po-badge");
  if (!badge) return;
  if (!_config) {
    badge.className = "erp-po-badge is-unavailable";
    badge.textContent = "無法使用：讀取設定失敗";
  } else if (!_config.available) {
    badge.className = "erp-po-badge is-unavailable";
    badge.textContent = `無法使用　${_config.reason || ""}`;
  } else {
    badge.className = `erp-po-badge ${_config.is_test ? "is-test" : "is-prod"}`;
    badge.textContent = `寫入：${_config.label}`;
  }
  updateButtons();
}

async function loadConfig() {
  try {
    _config = await apiJson("/api/erp/po/config");
  } catch (_) {
    _config = null;
  }
  renderBadge();
}

// ── 訂單選擇 ────────────────────────────────────────────────────────────────
function renderOrders() {
  const box = el("shortage-po-orders");
  if (!box) return;
  if (!_orders.length) {
    box.innerHTML = '<div class="erp-po-hint">尚無已發料訂單</div>';
    return;
  }
  box.innerHTML = _orders.map(o => `<label class="shortage-po-order">
    <input type="checkbox" data-order="${esc(o.id)}" ${_checkedOrders.has(o.id) ? "checked" : ""}>
    <span class="mono">${esc(o.code)}</span>
    <span>${esc(o.model)}</span>
    <span class="erp-po-hint">PO ${esc(o.po_number)}</span>
    <span class="erp-po-hint">出貨 ${esc(o.ship_date) || "—"}</span>
    <span class="erp-po-hint">${esc(String(o.dispatched_at || "").slice(0, 16).replace("T", " "))}</span>
  </label>`).join("");
}

async function loadOrders() {
  try {
    const data = await apiJson("/api/shortage-po/orders");
    _orders = data.orders || [];
    if (!_ordersLoaded) _checkedOrders = new Set(data.latest_batch_ids || []);
    _ordersLoaded = true;
  } catch (error) {
    _orders = [];
    showToast(`讀取訂單失敗：${error.message}`, { tone: "error" });
  }
  renderOrders();
}

// ── 缺料清單 ────────────────────────────────────────────────────────────────
function toRow(p) {
  return {
    part: p.part_number,
    desc: p.description || "",
    category: p.category,
    categorySource: p.category_source,
    supplierId: p.supplier_id || "",
    supplierName: p.supplier_name || "",
    supplierSource: p.supplier_source,
    need: p.need,
    st: p.st,
    mergedFrom: p.merged_from || [],
    onOrder: p.on_order,
    shortage: p.shortage,
    needsPurchase: p.needs_purchase,
    qty: p.suggested_qty > 0 ? String(p.suggested_qty) : "",
    price: String(p.price ?? 0),
    date: p.pre_in_date || "",
    dates: p.pre_in_dates || {},
    originalDates: p.original_pre_in_dates || {},
    datesAdjusted: p.dates_adjusted || {},
    dateEdited: false,
    earliest: p.earliest_order,
    orders: p.orders || [],
    checked: false,
    doneBill: "",
  };
}

async function buildList() {
  if (_busy) return;
  const ids = [..._checkedOrders];
  if (!ids.length) { showToast("請先勾選訂單", { tone: "error" }); return; }
  setBusy(true);
  resetPreview();
  el("shortage-po-result").hidden = true;
  try {
    _data = await apiJson(`/api/shortage-po/list?order_ids=${ids.join(",")}`);
    _listOrderIds = ids;
    _config = _data.config || _config;
    renderBadge();
    _rows = (_data.parts || []).map(toRow);
    _remarks = {};
    renderWarnings();
    el("shortage-po-list").hidden = false;
    renderGroups();
    renderCustomerSupplied();
  } catch (error) {
    showToast(`產生缺料清單失敗：${error.message}`, { tone: "error", sticky: true });
  } finally {
    setBusy(false);
  }
}

function renderWarnings() {
  const box = el("shortage-po-warnings");
  const warnings = _data?.warnings || [];
  box.innerHTML = warnings.map(w => `<div class="erp-po-warn">${esc(w)}</div>`).join("");
}

// ── 客供料（向客戶要料）：唯讀，不可勾選、不開採購單 ──────────────────────────
function renderCustomerSupplied() {
  const box = el("shortage-po-cs");
  if (!box) return;
  const items = _data?.customer_supplied || [];
  if (!items.length) { box.hidden = true; box.innerHTML = ""; return; }
  const wares = (_data.customer_supplied_ware_ids || []).join("、");
  const rows = items.map(c => {
    const e = c.earliest_order || {};
    const tip = (c.orders || []).map(o => `${o.code} ${o.model}　出貨 ${o.ship_date || "—"}　需求 ${fmtQty(o.qty)}`).join("\n");
    const stock = c.stock_known ? fmtQty(c.tab_stock) : `<span class="shortage-po-adjusted" title="${esc(c.stock_error)}">未知</span>`;
    const short = c.stock_known ? fmtQty(c.shortage) : "—";
    return `<tr>
      <td class="mono">${esc(c.part_number)}</td>
      <td>${esc(c.description)}</td>
      <td class="num">${fmtQty(c.need)}</td>
      <td class="num">${stock}</td>
      <td class="num ${c.stock_known && c.shortage > 0 ? "shortage-po-short" : ""}">${short}</td>
      <td title="${esc(tip)}">${esc(e.code)} ${esc(e.model)}<div class="erp-po-hint">出貨 ${esc(e.ship_date) || "—"}${(c.orders || []).length > 1 ? `（共 ${c.orders.length} 張）` : ""}</div></td>
    </tr>`;
  }).join("");
  const errors = [...new Set(items.filter(c => !c.stock_known).map(c => c.stock_error))];
  box.hidden = false;
  box.innerHTML = `<div class="shortage-po-group-head"><strong>客供料（向客戶要料）</strong>
      <span class="erp-po-hint">${items.length} 種；料號 -TAB 結尾或正航類別 003。僅供查看，不可勾選、不開採購單；庫存為正航倉庫 ${esc(wares)}（非 ST）</span></div>
    ${errors.map(e => `<div class="erp-po-warn">${esc(e)}，庫存暫顯示為未知</div>`).join("")}
    <div class="erp-po-table-wrap"><table class="erp-po-table"><thead><tr>
      <th>料號</th><th>品名</th><th class="num">需求</th><th class="num">${esc(wares)} 庫存</th><th class="num">缺（向客戶要）</th><th>最早訂單</th>
    </tr></thead><tbody>${rows}</tbody></table></div>`;
}

function visibleRows() {
  return _rows.filter(r => r.needsPurchase || _showAll || r.doneBill);
}

function checkedRows() {
  return _rows.filter(r => r.checked && !r.doneBill);
}

function groupBySupplier(rows) {
  const map = new Map();
  for (const r of rows) {
    const key = r.supplierId.trim();
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(r);
  }
  return [...map.entries()].sort(([a], [b]) => (a === "" ? 1 : b === "" ? -1 : a.localeCompare(b)));
}

function defaultRemark(rows) {
  const picked = rows.filter(r => r.checked && !r.doneBill);
  const source = picked.length ? picked : rows;
  const ordered = [...source].sort((a, b) => String(a.earliest?.ship_date || "9999").localeCompare(String(b.earliest?.ship_date || "9999")));
  const codes = [...new Set(ordered.map(r => r.earliest?.code).filter(Boolean))].slice(0, 3);
  return `缺料採購：${codes.join("、")}`;
}

function remarkFor(supplierId, rows) {
  return supplierId in _remarks ? _remarks[supplierId] : defaultRemark(rows);
}

function subtotal(rows) {
  return rows.filter(r => r.checked && !r.doneBill).reduce((sum, r) => {
    const q = Number(r.qty);
    const p = Number(r.price);
    return sum + (Number.isFinite(q) && Number.isFinite(p) ? q * p : 0);
  }, 0);
}

// 推算日已過、改用今天時的提示（使用者手動改日期後不再顯示）
function adjustedNote(r) {
  if (r.dateEdited || !r.datesAdjusted[r.category]) return "";
  const [, m, d] = String(r.originalDates[r.category] || "").split("-");
  return m && d ? `原推算 ${Number(m)}/${Number(d)} 已過，改今天` : "";
}

function rowHtml(r) {
  const note = adjustedNote(r);
  const tip = r.orders.map(o => `${o.code} ${o.model} 出貨 ${o.ship_date || "—"} 需 ${fmtQty(o.qty)}`).join("\n");
  const e = r.earliest || {};
  const disabled = !r.supplierId.trim() || r.doneBill;
  return `<tr data-part="${esc(r.part)}" class="${r.doneBill ? "shortage-po-done" : ""}">
    <td><input type="checkbox" data-act="check" ${r.checked ? "checked" : ""} ${disabled ? "disabled" : ""}
      title="${r.supplierId.trim() ? "" : "請先填供應商"}"></td>
    <td class="mono">${esc(r.part)}${r.doneBill ? `<div class="erp-po-hint">已開 ${esc(r.doneBill)}</div>` : ""}</td>
    <td>${esc(r.desc)}</td>
    <td><select data-act="category">
      ${Object.entries(CATEGORY_LABELS).map(([k, label]) => `<option value="${k}" ${r.category === k ? "selected" : ""}>${label}</option>`).join("")}
    </select><div class="erp-po-hint">${r.categorySource === "manual" ? "手動" : "自動"}</div></td>
    <td><input type="text" data-act="supplier" class="mono" value="${esc(r.supplierId)}" autocomplete="off" ${r.doneBill ? "disabled" : ""}>
      <div class="erp-po-hint">${esc(r.supplierName)} ${esc(SOURCE_LABELS[r.supplierSource] || "")}</div></td>
    <td class="num">${fmtQty(r.need)}</td>
    <td class="num">${fmtQty(r.st)}${r.mergedFrom.length ? `<div class="erp-po-hint" title="轉換料：B 有效庫存 = B + A">含 ${r.mergedFrom.map(m => `${esc(m.part)} ${fmtQty(m.stock)}`).join("、")}</div>` : ""}</td>
    <td class="num">${fmtQty(r.onOrder)}</td>
    <td class="num ${r.needsPurchase ? "shortage-po-short" : ""}">${fmtQty(r.shortage)}</td>
    <td><input type="number" data-act="qty" class="num" min="0" step="any" value="${esc(r.qty)}" ${r.doneBill ? "disabled" : ""}></td>
    <td><input type="number" data-act="price" class="num" min="0" step="any" value="${esc(r.price)}" ${r.doneBill ? "disabled" : ""}></td>
    <td><input type="date" data-act="date" value="${esc(r.date)}" ${r.doneBill ? "disabled" : ""}>
      ${note ? `<div class="erp-po-hint shortage-po-adjusted">${esc(note)}</div>` : ""}</td>
    <td title="${esc(tip)}">${esc(e.code)} ${esc(e.model)}<div class="erp-po-hint">出貨 ${esc(e.ship_date) || "—"}${r.orders.length > 1 ? `（共 ${r.orders.length} 張）` : ""}</div></td>
  </tr>`;
}

function renderGroups() {
  const box = el("shortage-po-groups");
  if (!box) return;
  const rows = visibleRows();
  const needCount = _rows.filter(r => r.needsPurchase).length;
  el("shortage-po-summary").textContent = `共 ${_rows.length} 種料，需採購 ${needCount} 種，已勾選 ${checkedRows().length} 種`;
  if (!rows.length) {
    box.innerHTML = `<div class="erp-po-hint">${_rows.length ? "沒有缺的料（可勾選「顯示不缺的料」查看全部）" : "沒有補料 / 缺料內容"}</div>`;
    updateButtons();
    return;
  }
  box.innerHTML = groupBySupplier(rows).map(([sid, list]) => {
    const selectable = list.filter(r => r.supplierId.trim() && !r.doneBill);
    const allChecked = selectable.length > 0 && selectable.every(r => r.checked);
    const name = list.find(r => r.supplierName)?.supplierName || "";
    return `<div class="shortage-po-group" data-supplier="${esc(sid)}">
      <div class="shortage-po-group-head">
        <input type="checkbox" data-act="check-all" ${allChecked ? "checked" : ""} ${selectable.length ? "" : "disabled"}>
        <strong class="mono">${sid ? esc(sid) : "（未設定供應商）"}</strong>
        <span>${esc(name)}</span>
        <span class="erp-po-hint">${list.length} 行</span>
        <span class="erp-po-hint">已勾小計（未稅）<strong data-role="subtotal">${fmtQty(subtotal(list))}</strong></span>
        <span class="spacer"></span>
        ${sid ? `<label class="shortage-po-remark">備註 <input type="text" data-act="remark" value="${esc(remarkFor(sid, list))}" autocomplete="off"></label>` : ""}
      </div>
      <div class="erp-po-table-wrap"><table class="erp-po-table shortage-po-table">
        <thead><tr><th></th><th>料號</th><th>品名</th><th>類別</th><th>供應商</th><th class="num">需求</th><th class="num">ST 現有</th>
          <th class="num" title="僅供參考，不從缺料量扣除（缺 = 需求 − ST）">在途（僅參考）</th><th class="num">缺</th><th>數量</th><th>單價</th><th>預計進貨日</th><th>來源訂單</th></tr></thead>
        <tbody>${list.map(rowHtml).join("")}</tbody>
      </table></div>
    </div>`;
  }).join("");
  updateButtons();
}

function updateSubtotals() {
  document.querySelectorAll("#shortage-po-groups .shortage-po-group").forEach(g => {
    const sid = g.dataset.supplier;
    const list = visibleRows().filter(r => r.supplierId.trim() === sid);
    const out = g.querySelector('[data-role="subtotal"]');
    if (out) out.textContent = fmtQty(subtotal(list));
  });
  el("shortage-po-summary").textContent =
    `共 ${_rows.length} 種料，需採購 ${_rows.filter(r => r.needsPurchase).length} 種，已勾選 ${checkedRows().length} 種`;
}

// ── 事件 ────────────────────────────────────────────────────────────────────
function rowOf(target) {
  const tr = target.closest("tr[data-part]");
  return tr ? _rows.find(r => r.part === tr.dataset.part) : null;
}

async function setCategory(row, category) {
  const before = { category: row.category, source: row.categorySource };
  row.category = category;
  row.categorySource = "manual";
  if (!row.dateEdited && row.dates[category]) row.date = row.dates[category];
  renderGroups();
  try {
    await apiPost("/api/shortage-po/category", { part_number: row.part, category });
  } catch (error) {
    row.category = before.category;
    row.categorySource = before.source;
    if (!row.dateEdited && row.dates[before.category]) row.date = row.dates[before.category];
    renderGroups();
    showToast(`儲存類別失敗：${error.message}`, { tone: "error" });
  }
}

async function setSupplier(row, text) {
  const id = text.trim();
  if (id === row.supplierId) return;
  row.supplierId = id;
  row.supplierSource = id ? "manual" : "none";
  row.supplierName = "";
  if (!id) row.checked = false;
  invalidatePreview();
  renderGroups();
  if (!id) return;
  try {
    const data = await apiJson(`/api/erp/po/suppliers?q=${encodeURIComponent(id)}`);
    if (row.supplierId !== id) return; // 使用者又改了
    const hit = (data.suppliers || []).find(s => s.id.toLowerCase() === id.toLowerCase());
    if (hit) {
      row.supplierId = hit.id;
      row.supplierName = hit.short_name || hit.full_name;
    } else {
      row.supplierName = "（正航查無此供應商）";
    }
    renderGroups();
  } catch (_) { /* 名稱查不到不影響，預覽時會驗證 */ }
}

function onGroupsInput(event) {
  const act = event.target.dataset?.act;
  if (act === "remark") {
    const sid = event.target.closest(".shortage-po-group")?.dataset.supplier;
    if (sid !== undefined) _remarks[sid] = event.target.value;
    invalidatePreview();
    return;
  }
  const row = rowOf(event.target);
  if (!row) return;
  if (act === "qty") row.qty = event.target.value;
  else if (act === "price") row.price = event.target.value;
  else return;
  invalidatePreview();
  updateSubtotals();
}

function onGroupsChange(event) {
  const act = event.target.dataset?.act;
  if (act === "check-all") {
    const sid = event.target.closest(".shortage-po-group")?.dataset.supplier;
    visibleRows().filter(r => r.supplierId.trim() === sid && !r.doneBill).forEach(r => { r.checked = event.target.checked; });
    invalidatePreview();
    renderGroups();
    return;
  }
  const row = rowOf(event.target);
  if (!row) return;
  if (act === "check") {
    row.checked = event.target.checked;
    invalidatePreview();
    renderGroups();
  } else if (act === "category") {
    invalidatePreview();
    void setCategory(row, event.target.value);
  } else if (act === "supplier") {
    void setSupplier(row, event.target.value);
  } else if (act === "date") {
    row.date = event.target.value;
    row.dateEdited = true;
    invalidatePreview();
    renderGroups();
  }
}

// ── 預覽 / 寫入 ─────────────────────────────────────────────────────────────
function setBusy(busy) {
  _busy = busy;
  updateButtons();
}

function updateButtons() {
  const build = el("btn-shortage-po-build");
  if (build) { build.disabled = _busy; build.textContent = _busy ? "處理中..." : "產生缺料清單"; }
  const preview = el("btn-shortage-po-preview");
  if (preview) preview.disabled = _busy || !_config?.available || checkedRows().length === 0;
  const create = el("btn-shortage-po-create");
  if (create) create.disabled = _busy || !_config?.available || !previewIsCurrent() || !okPreviewCount();
}

function buildGroups() {
  const picked = checkedRows();
  const groups = [];
  for (const [sid, list] of groupBySupplier(picked)) {
    if (!sid) throw new Error("有勾選的料尚未指定供應商");
    const remark = (remarkFor(sid, visibleRows().filter(r => r.supplierId.trim() === sid)) || "").trim();
    groups.push({
      supplier_id: sid,
      remark,
      lines: list.map(r => {
        const qty = Number(r.qty);
        const price = r.price === "" ? NaN : Number(r.price);
        if (!(qty > 0)) throw new Error(`${r.part}：請填大於 0 的數量`);
        if (!Number.isFinite(price) || price < 0) throw new Error(`${r.part}：請填單價`);
        if (!r.date) throw new Error(`${r.part}：請填預計進貨日`);
        return { part_number: r.part, qty, price, pre_in_date: r.date };
      }),
    });
  }
  if (groups.length > MAX_GROUPS) throw new Error(`一次最多 ${MAX_GROUPS} 張採購單，請分批勾選`);
  return groups;
}

function resetPreview() {
  _previewSig = "";
  _previewResults = null;
  const box = el("shortage-po-preview");
  if (box) { box.hidden = true; box.innerHTML = ""; }
}

function previewIsCurrent() {
  if (!_previewResults) return false;
  try { return JSON.stringify(buildGroups()) === _previewSig; } catch (_) { return false; }
}

function okPreviewCount() {
  return (_previewResults || []).filter(r => r.ok).length;
}

function invalidatePreview() {
  if (!_previewResults) { updateButtons(); return; }
  updateButtons();
  const note = el("shortage-po-preview")?.querySelector('[data-role="stale"]');
  if (note) note.hidden = previewIsCurrent();
}

function cardHtml(r) {
  if (!r.ok) {
    return `<div class="shortage-po-card is-error"><strong class="mono">${esc(r.supplier_id)}</strong>　❌ ${esc(r.error)}</div>`;
  }
  const masters = r.missing_masters?.length
    ? `<div class="erp-po-warn">將先從來源公司複製以下主檔到 ${esc(r.target_label)}：${r.missing_masters.map(esc).join("、")}</div>`
    : "";
  return `<div class="shortage-po-card">
    <div class="erp-po-preview-head">預估單號 <strong class="mono">${esc(r.bill_no)}</strong>（預估，實際以寫入結果為準）　供應商 ${esc(r.supplier.id)} ${esc(r.supplier.name)}</div>
    ${r.remark ? `<div class="erp-po-hint">備註：${esc(r.remark)}</div>` : ""}
    ${masters}
    <table class="erp-po-table"><thead><tr><th>料號</th><th>品名</th><th class="num">數量</th><th class="num">單價</th><th class="num">未稅金額</th><th>預計進貨日</th></tr></thead>
      <tbody>${r.lines.map(l => `<tr><td class="mono">${esc(l.part_number)}</td><td>${esc(l.product_name)}</td>
        <td class="num">${fmtQty(l.qty)}</td><td class="num">${fmtQty(l.price)}</td><td class="num">${fmtQty(l.amount)}</td>
        <td>${esc(l.pre_in_date)}</td></tr>
        <tr class="shortage-po-alloc"><td colspan="6" class="erp-po-hint">${esc(allocationText(l.allocations))}</td></tr>`).join("")}</tbody></table>
    <div class="erp-po-totals">未稅 <strong>${fmtQty(r.totals.sum_amount)}</strong>　稅額 <strong>${fmtQty(r.totals.sum_tax)}</strong>　合計 <strong>${fmtQty(r.totals.total)}</strong></div>
  </div>`;
}

function renderPreview() {
  const box = el("shortage-po-preview");
  const results = _previewResults || [];
  const label = results.find(r => r.ok)?.target_label || _config?.label || "";
  const isTest = results.find(r => r.ok)?.is_test ?? _config?.is_test;
  box.innerHTML = `
    <div class="erp-po-badge ${isTest ? "is-test" : "is-prod"} erp-po-badge-small">寫入：${esc(label)}</div>
    <div class="erp-po-warn" data-role="stale" hidden>內容已修改，請重新預覽後才能寫入</div>
    ${results.map(cardHtml).join("")}
    <div class="erp-po-actions"><span class="spacer"></span>
      <button class="btn btn-danger" id="btn-shortage-po-create" type="button">確認寫入正航</button></div>`;
  box.hidden = false;
  el("btn-shortage-po-create").addEventListener("click", handleCreate);
  updateButtons();
}

async function handlePreview() {
  if (_busy || !requireEdit()) return;
  el("shortage-po-result").hidden = true;
  let groups;
  try { groups = buildGroups(); } catch (error) { showToast(error.message, { tone: "error" }); return; }
  if (!groups.length) { showToast("請先勾選要採購的料", { tone: "error" }); return; }
  setBusy(true);
  try {
    const data = await apiPost("/api/shortage-po/preview", { groups, order_ids: _listOrderIds });
    _previewResults = data.results || [];
    _previewSig = JSON.stringify(groups);
    renderPreview();
  } catch (error) {
    resetPreview();
    showToast(`預覽失敗：${error.message}`, { tone: "error", sticky: true });
  } finally {
    setBusy(false);
  }
}

function resultHtml(results) {
  return results.map(r => r.ok
    ? `<div class="is-ok">✅ ${esc(r.supplier?.id || r.supplier_id)} ${esc(r.supplier?.name || "")}：成功 單號 <strong class="mono">${esc(r.bill_no)}</strong>，含稅合計 ${fmtQty(r.totals?.total)}</div>`
    : `<div class="is-error">❌ ${esc(r.supplier_id)}：失敗 ${esc(r.error)}</div>`).join("");
}

async function handleCreate() {
  if (_busy || !requireEdit()) return;
  if (!previewIsCurrent()) { showToast("內容已修改，請重新預覽", { tone: "error" }); return; }
  const okSuppliers = new Set((_previewResults || []).filter(r => r.ok).map(r => r.supplier_id.toLowerCase()));
  const groups = buildGroups().filter(g => okSuppliers.has(g.supplier_id.toLowerCase()));
  if (!groups.length) return;
  const label = _previewResults.find(r => r.ok)?.target_label || _config?.label || "";
  if (!window.confirm(`確定要在「${label}」建立 ${groups.length} 張採購單嗎？\n寫入後無法從本系統修改或刪除。`)) return;
  setBusy(true);
  const out = el("shortage-po-result");
  try {
    const data = await apiPost("/api/shortage-po/create", { groups, order_ids: _listOrderIds, confirm: true });
    const results = data.results || [];
    out.innerHTML = resultHtml(results);
    out.className = `shortage-po-result ${results.every(r => r.ok) ? "is-ok" : "is-error"}`;
    out.hidden = false;
    const bills = new Map(results.filter(r => r.ok).map(r => [r.supplier_id, r.bill_no]));
    for (const row of checkedRows()) {
      const bill = bills.get(row.supplierId.trim());
      if (bill) { row.doneBill = bill; row.checked = false; }
    }
    resetPreview();
    renderGroups();
    const okN = results.filter(r => r.ok).length;
    showToast(`已建立 ${okN} 張採購單${okN < results.length ? `，${results.length - okN} 張失敗` : ""}`,
      { tone: okN === results.length ? "success" : "error", sticky: okN < results.length });
  } catch (error) {
    out.className = "shortage-po-result is-error";
    out.textContent = `❌ 寫入失敗：${error.message}`;
    out.hidden = false;
  } finally {
    setBusy(false);
    void refreshErpPurchaseHistory();
  }
}

// ── 對外 ────────────────────────────────────────────────────────────────────
export async function onShortagePurchaseTabActivated() {
  await Promise.all([loadConfig(), loadOrders()]);
  if (_pendingIds) {
    const ids = _pendingIds;
    _pendingIds = null;
    const known = new Set(_orders.map(o => o.id));
    const usable = ids.filter(id => known.has(id));
    if (usable.length < ids.length) showToast("部分訂單不在可選清單內（需為已發料 / 已完成）", { tone: "error" });
    if (!usable.length) return;
    _checkedOrders = new Set(usable);
    renderOrders();
    await buildList();
  }
}

/** 切到「缺料採購」頁籤，勾選指定訂單並產生缺料清單。 */
export async function openShortagePurchase(orderIds) {
  _pendingIds = [...new Set((orderIds || []).map(Number).filter(Number.isInteger))];
  if (!_pendingIds.length) { _pendingIds = null; return; }
  const tab = document.querySelector('.tab-btn[data-tab="shortage-po-tab"]');
  if (tab) tab.click();
  else await onShortagePurchaseTabActivated();
}

export async function initShortagePurchase() {
  if (_initialized) return;
  _initialized = true;
  el("shortage-po-orders")?.addEventListener("change", event => {
    const id = Number(event.target.dataset?.order);
    if (!Number.isInteger(id)) return;
    if (event.target.checked) _checkedOrders.add(id); else _checkedOrders.delete(id);
  });
  el("btn-shortage-po-build")?.addEventListener("click", () => { void buildList(); });
  el("btn-shortage-po-preview")?.addEventListener("click", handlePreview);
  el("shortage-po-show-all")?.addEventListener("change", event => {
    _showAll = event.target.checked;
    renderGroups();
  });
  const groups = el("shortage-po-groups");
  groups?.addEventListener("input", onGroupsInput);
  groups?.addEventListener("change", onGroupsChange);
  renderOrders();
  await loadConfig();
}
