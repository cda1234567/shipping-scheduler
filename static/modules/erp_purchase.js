import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";
import { decoratePoMailBadges, poMailFailedHint, poMailSlot } from "/static/modules/po_mail_status.js";

const LEAD_DAYS = 14;

let _config = null;
let _supplier = null; // { id, name }
let _lines = [];
let _busy = false;
let _initialized = false;
let _searchTimer = null;
let _defaultDue = null; // /api/holidays/default-due：{ date, reason }（今天 + LEAD_DAYS 再套用假日規則）
let _companions = []; // 後段加工（另開一張採購單）的明細

function pad(n) { return String(n).padStart(2, "0"); }

function defaultDate() {
  const d = new Date();
  d.setDate(d.getDate() + LEAD_DAYS);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

function newLine(part = "") {
  return {
    part, name: "", qty: "", price: "", date: _defaultDue?.date || defaultDate(), dateHint: _defaultDue?.reason || "",
    hint: null, qtyAuto: false, pp: [],
  };
}

/** 日期旁的小提示：預設日期遇假日已提前、或使用者選到非上班日。 */
function shiftHintText(r, userPicked) {
  if (!r?.shifted) return "";
  if (userPicked) {
    if (r.reason_code !== "holiday") return "";
    const [, m, d] = String(r.date).split("-");
    return `所選日期非上班日，建議 ${Number(m)}/${Number(d)}`;
  }
  return r.reason;
}

async function loadDefaultDue() {
  try {
    const r = await apiJson(`/api/holidays/default-due?days=${LEAD_DAYS}`);
    _defaultDue = { date: r.date, reason: shiftHintText(r, false) };
    // 還沒動過的空白行也套用
    for (const l of _lines) {
      if (!l.part.trim() && l.qty === "" && l.price === "") { l.date = _defaultDue.date; l.dateHint = _defaultDue.reason; }
    }
    renderLines();
  } catch (_) {
    _defaultDue = null;
  }
}

async function hintLineDate(line) {
  try {
    const r = await apiJson(`/api/holidays/shift?date=${encodeURIComponent(line.date)}`);
    line.dateHint = shiftHintText(r, true);
    renderLines();
    renderCompanions();
  } catch (_) { /* 提示失敗不影響 */ }
}

/** 加工件數量 = 基礎件數量 × 比例，無條件進位成整數。 */
export function linkedQty(qty, ratio) {
  const v = Number(qty) * Number(ratio);
  return Number.isFinite(v) && v > 0 ? Math.ceil(Math.round(v * 1e6) / 1e6) : 0;
}

/** 'YYYY-MM-DD' → 'YYYY/MM/DD'。 */
function slashDate(text) {
  return /^\d{4}-\d{2}-\d{2}/.test(String(text || "")) ? String(text).slice(0, 10).replaceAll("-", "/") : "";
}

/** 數量欄下方的提示（MOQ 以「最近一次進貨量」估算）：
 *  「需 1,234，上次進貨 6,000（2026/09/15）」；改用其他來源時註明：「需 X，MOQ Y（主檔）」/「（正航最小採購量）」；
 *  手動行沒有缺料需求（需求缺漏或 ≤ 1 的佔位值）就不顯示「需」；完全沒有資料顯示「無 MOQ 資料」。
 *  source 為空字串 = 沒有查過 MOQ，不顯示 MOQ 部分。receipt = { qty, date }。 */
export function moqHintText(need, moq, source, receipt = null) {
  const n = Number(need);
  const hasNeed = Number.isFinite(n) && n > 1;
  const m = Number(moq);
  const hasMoq = Number.isFinite(m) && m > 0;
  let part = "";
  if (hasMoq && source === "last_receipt") {
    const d = slashDate(receipt?.date);
    part = `上次進貨 ${fmtMoney(m)}${d ? `（${d}）` : ""}`;
  } else if (hasMoq && source === "erp_minpurch") {
    part = `MOQ ${fmtMoney(m)}（正航最小採購量）`;
  } else if (hasMoq && source === "site_master") {
    part = `MOQ ${fmtMoney(m)}（主檔）`;
  } else if (hasMoq) {
    part = `MOQ ${fmtMoney(m)}`;
  } else if (source === "none") {
    part = "無 MOQ 資料";
  }
  return [hasNeed ? `需 ${fmtMoney(n)}` : "", part].filter(Boolean).join("，");
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
    <td><input type="text" data-field="part" value="${esc(l.part)}" class="mono" autocomplete="off">${
      (l.pp || []).filter(pp => !_companions.some(c => c.part === pp.process_part)).map(pp =>
        `<div class="erp-po-hint pp-linked">有後段加工 ${esc(pp.process_part)}　<button class="btn btn-secondary btn-sm" data-act="add-pp" data-pp="${esc(pp.process_part)}" type="button">加入後段加工（×${esc(pp.ratio)}）</button></div>`).join("")}</td>
    <td class="erp-po-name">${esc(l.name) || '<span class="erp-po-hint">—</span>'}</td>
    <td><input type="number" data-field="qty" value="${esc(l.qty)}" min="0" step="any" class="num">${
      l.hint ? `<div class="erp-po-hint erp-po-moq-hint">${esc(moqHintText(l.hint.need, l.hint.moq, l.hint.moq_source, l.hint.receipt))}</div>` : ""}</td>
    <td><input type="number" data-field="price" value="${esc(l.price)}" min="0" step="any" class="num"></td>
    <td><input type="date" data-field="date" value="${esc(l.date)}">${l.dateHint ? `<div class="erp-po-hint shortage-po-adjusted">${esc(l.dateHint)}</div>` : ""}</td>
    <td><button class="btn btn-danger btn-sm" data-act="remove" type="button">移除</button></td>
  </tr>`).join("");
}

function receiptOf(d) {
  return { qty: d.last_receipt_qty, date: d.last_receipt_date };
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
    if ((line.qty === "" || line.qty === null) && d.suggested_qty !== null && d.suggested_qty !== undefined) {
      line.qty = String(Math.max(1, d.suggested_qty));
      line.qtyAuto = true;
      if (d.need_qty) line.hint = { need: d.need_qty, moq: d.moq, moq_source: d.moq_source, receipt: receiptOf(d) };
    } else if (line.qtyAuto && Number(d.moq) > 0) {
      // 從低庫存帶入的數量尚未被改過：補上 ERP 最低採購量（本站 MOQ 已在帶入時處理）
      const need = Number(line.hint?.need || line.qty);
      line.qty = String(Math.max(Number(line.qty) || 0, Math.ceil(Number(d.moq))));
      line.hint = { need, moq: d.moq, moq_source: d.moq_source, receipt: receiptOf(d) };
    }
    // 其餘情況（手動行沒有缺料需求）：只顯示 MOQ 估算來源，不顯示「需」
    if (!d.found) line.hint = null;
    else if (!line.hint) line.hint = { need: null, moq: d.moq, moq_source: d.moq_source, receipt: receiptOf(d) };
    else line.hint = { ...line.hint, moq: d.moq, moq_source: d.moq_source, receipt: receiptOf(d) };
    if (idx === 0 && !_supplier && d.supplier_id) _supplier = { id: d.supplier_id, name: d.supplier_name };
    line.pp = [];
    try {
      const key = (d.part_number || part).toUpperCase();
      const chk = await apiJson(`/api/postprocess/check?parts=${encodeURIComponent(key)}`);
      if (_lines[idx] === line) line.pp = (chk.links || {})[key] || [];
    } catch (_) { /* 後段加工提示失敗不影響開單 */ }
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
  const line = _lines[Number(tr.dataset.idx)];
  line[field] = event.target.value;
  if (field === "qty") line.qtyAuto = false; // 使用者自行改量後不再自動調整
}

function onLinesChange(event) {
  const field = event.target.dataset?.field;
  const tr = event.target.closest("tr[data-idx]");
  if (!tr) return;
  if (field === "date") { void hintLineDate(_lines[Number(tr.dataset.idx)]); return; }
  if (field === "part") void fillDefaults(Number(tr.dataset.idx));
}

/** 使用者確認 → 加入後段加工明細（另開一張採購單，供應商 / 單價取自加工件的正航資料）。 */
async function addCompanion(line, processPart) {
  const pp = (line.pp || []).find(x => x.process_part === processPart);
  const qty = linkedQty(line.qty, pp?.ratio);
  if (!pp || !qty) { showToast("請先填鈞洋件數量", { tone: "error" }); return; }
  if (_companions.some(c => c.part === processPart)) return;
  try {
    const d = await apiJson(`/api/erp/po/part-defaults?part=${encodeURIComponent(processPart)}`);
    // 加工件預交日 = 鈞洋件預交日 + 4 天（鈞洋件比加工件早 4 天到），再套用假日規則
    const base = new Date(`${line.date || _defaultDue?.date}T00:00:00`);
    base.setDate(base.getDate() + 4);
    const raw = `${base.getFullYear()}-${pad(base.getMonth() + 1)}-${pad(base.getDate())}`;
    const shifted = await apiJson(`/api/holidays/shift?date=${raw}`);
    _companions.push({
      part: d.part_number || processPart, name: d.found ? d.product_name : "（正航查無此料號）", qty: String(qty),
      price: d.last_price === null || d.last_price === undefined ? "" : String(d.last_price),
      date: shifted.date, dateHint: shiftHintText(shifted, false),
      supplierId: d.supplier_id || "", supplierName: d.supplier_name || "", from: line.part, ratio: pp.ratio,
    });
    renderLines();
    renderCompanions();
  } catch (error) {
    showToast(`讀取加工件資料失敗：${error.message}`, { tone: "error" });
  }
}

function renderCompanions() {
  const box = document.getElementById("erp-po-companions");
  if (!box) return;
  if (!_companions.length) { box.hidden = true; box.innerHTML = ""; return; }
  box.hidden = false;
  box.innerHTML = `<h4 class="erp-po-history-title">後段加工（另開一張採購單，依供應商分開）</h4>
    <div class="erp-po-table-wrap"><table class="erp-po-table"><thead><tr>
      <th>料號</th><th>品名</th><th>供應商</th><th>數量</th><th>單價（未稅）</th><th>預計進貨日</th><th></th></tr></thead><tbody>
    ${_companions.map((c, i) => `<tr data-cidx="${i}">
      <td class="mono">${esc(c.part)}<div class="erp-po-hint pp-linked">← ${esc(c.from)} ×${esc(c.ratio)}</div></td>
      <td>${esc(c.name)}</td>
      <td><input type="text" class="mono" data-cfield="supplierId" value="${esc(c.supplierId)}" autocomplete="off"><div class="erp-po-hint">${esc(c.supplierName)}</div></td>
      <td><input type="number" class="num" data-cfield="qty" value="${esc(c.qty)}" min="0" step="any"></td>
      <td><input type="number" class="num" data-cfield="price" value="${esc(c.price)}" min="0" step="any"></td>
      <td><input type="date" data-cfield="date" value="${esc(c.date)}">${c.dateHint ? `<div class="erp-po-hint shortage-po-adjusted">${esc(c.dateHint)}</div>` : ""}</td>
      <td><button class="btn btn-danger btn-sm" data-act="remove-companion" type="button">移除</button></td></tr>`).join("")}
    </tbody></table></div>`;
}

function onCompanionsInput(event) {
  const field = event.target.dataset?.cfield;
  const tr = event.target.closest("tr[data-cidx]");
  if (!field || !tr) return;
  const c = _companions[Number(tr.dataset.cidx)];
  c[field] = event.target.value;
  if (field === "supplierId") c.supplierName = "";
}

function onCompanionsChange(event) {
  const field = event.target.dataset?.cfield;
  const tr = event.target.closest("tr[data-cidx]");
  if (!tr) return;
  const c = _companions[Number(tr.dataset.cidx)];
  if (field === "date") void (async () => {
    try {
      const r = await apiJson(`/api/holidays/shift?date=${encodeURIComponent(c.date)}`);
      c.dateHint = shiftHintText(r, true);
      renderCompanions();
    } catch (_) { /* 提示失敗不影響 */ }
  })();
}

function onCompanionsClick(event) {
  const btn = event.target.closest('[data-act="remove-companion"]');
  const tr = event.target.closest("tr[data-cidx]");
  if (!btn || !tr) return;
  _companions.splice(Number(tr.dataset.cidx), 1);
  renderCompanions();
  renderLines();
}

function onLinesClick(event) {
  const add = event.target.closest('[data-act="add-pp"]');
  if (add) {
    const addTr = add.closest("tr[data-idx]");
    if (addTr && requireEdit()) void addCompanion(_lines[Number(addTr.dataset.idx)], add.dataset.pp);
    return;
  }
  const btn = event.target.closest('[data-act="remove"]');
  const tr = event.target.closest("tr[data-idx]");
  if (!btn || !tr) return;
  _lines.splice(Number(tr.dataset.idx), 1);
  if (!_lines.length) _lines.push(newLine());
  renderLines();
}

// 供低庫存提醒呼叫：帶入料號與建議數量（已含 MOQ）；meta = { need, moq, moq_source, receipt } 用來顯示取整提示
export function prefillPurchaseOrder(part, qty, meta = null) {
  const empty = _lines.length === 1 && !_lines[0].part.trim();
  if (empty) _lines = [];
  const line = newLine(String(part || "").toUpperCase());
  line.qty = qty ? String(qty) : "";
  line.qtyAuto = Boolean(qty);
  if (meta && meta.need) line.hint = { need: meta.need, moq: meta.moq, moq_source: meta.moq_source, receipt: meta.receipt };
  _lines.push(line);
  renderLines();
  void fillDefaults(_lines.length - 1);
  if (!_defaultDue) {
    // 預設預交日尚未載入（假日規則）：載入後補套到這一行
    const rawDate = line.date;
    void loadDefaultDue().then(() => {
      if (_defaultDue && line.date === rawDate) { line.date = _defaultDue.date; line.dateHint = _defaultDue.reason; renderLines(); }
    });
  }
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

/** 後段加工明細 → 依供應商分組的採購單請求（每個供應商一張）。 */
function buildCompanionBodies() {
  const groups = new Map();
  for (const c of _companions) {
    const sid = c.supplierId.trim();
    if (!sid) throw new Error(`後段加工 ${c.part}：請填供應商`);
    const qty = Number(c.qty);
    const price = c.price === "" ? NaN : Number(c.price);
    if (!(qty > 0) || !Number.isFinite(price) || price < 0) throw new Error(`後段加工 ${c.part}：請填數量與單價`);
    if (!groups.has(sid)) groups.set(sid, []);
    groups.get(sid).push({ part_number: c.part, qty, price, pre_in_date: c.date });
  }
  const salesman = document.getElementById("erp-po-salesman")?.value.trim() || null;
  const remark = document.getElementById("erp-po-remark")?.value.trim() || "";
  return [...groups.entries()].map(([supplier_id, lines]) => ({
    supplier_id, remark: remark ? `${remark}（後段加工）` : "後段加工", salesman, lines,
  }));
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

function previewCard(p, withBadge) {
  const masters = p.masters_to_copy?.length
    ? `<div class="erp-po-warn">將先從來源公司複製以下主檔到 ${esc(p.target_label)}：${p.masters_to_copy.map(m => `${esc(m.label)} ${esc(m.key)}`).join("、")}</div>`
    : "";
  return `
    ${withBadge ? `<div class="erp-po-badge ${p.is_test ? "is-test" : "is-prod"} erp-po-badge-small">寫入：${esc(p.target_label)}</div>` : ""}
    <div class="erp-po-preview-head">預計單號 <strong class="mono">${esc(p.bill_no)}</strong>（實際單號以寫入結果為準）　供應商 ${esc(p.supplier.id)} ${esc(p.supplier.name)}　採購員 ${esc(p.salesman)}</div>
    <div class="erp-po-hint">付款條件 / 地址取自範本單 ${esc(p.template_bill_no)}${p.remark ? `　備註：${esc(p.remark)}` : ""}</div>
    ${masters}
    <table class="erp-po-table">
      <thead><tr><th>#</th><th>料號</th><th>品名</th><th class="num">數量</th><th class="num">單價</th><th class="num">未稅金額</th><th class="num">稅額</th><th>預計進貨日</th></tr></thead>
      <tbody>${p.lines.map(l => `<tr><td>${l.row_no}</td><td class="mono">${esc(l.part_number)}</td><td>${esc(l.product_name)}</td>
        <td class="num">${fmtMoney(l.qty)}</td><td class="num">${fmtMoney(l.price)}</td><td class="num">${fmtMoney(l.amount)}</td>
        <td class="num">${fmtMoney(l.tax_amt)}</td><td>${esc(l.pre_in_date)}</td></tr>`).join("")}</tbody>
    </table>
    <div class="erp-po-totals">未稅 <strong>${fmtMoney(p.totals.sum_amount)}</strong>　稅額 <strong>${fmtMoney(p.totals.sum_tax)}</strong>　合計 <strong>${fmtMoney(p.totals.total)}</strong></div>`;
}

/** 後端 postprocess_warning（鈞洋件有後段加工對應但加工件不在同一批）轉成提示文字。 */
export function postprocessWarningText(items) {
  const lines = (items || []).map(i => `・${i.base_part} 需搭配後段加工 ${i.process_part}（建議數量 ${fmtMoney(i.suggested_qty)}，比例 ×${fmtMoney(i.ratio)}）`);
  return `以下鈞洋件有後段加工對應，但加工件不在這批採購單中：\n${lines.join("\n")}`;
}

function renderPreview(p, companions = []) {
  const el = document.getElementById("erp-po-preview");
  if (!el) return;
  const covered = new Set(_companions.map(c => c.part));
  const missing = (p.postprocess_warning || []).filter(w => !covered.has(w.process_part));
  el.innerHTML = `
    ${previewCard(p, true)}
    ${missing.length ? `<div class="erp-po-warn">${esc(postprocessWarningText(missing))}</div>` : ""}
    ${companions.map(c => `<hr>${previewCard(c, false)}`).join("")}
    <div class="erp-po-actions"><span class="spacer"></span>
      <button class="btn btn-danger" id="btn-erp-po-create" type="button">確認寫入正航${companions.length ? `（共 ${1 + companions.length} 張）` : ""}</button></div>`;
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
    const main = await apiPost("/api/erp/po/preview", body);
    const companions = [];
    for (const c of buildCompanionBodies()) companions.push(await apiPost("/api/erp/po/preview", c));
    renderPreview(main, companions);
  } catch (error) {
    const el = document.getElementById("erp-po-preview");
    if (el) { el.hidden = true; el.innerHTML = ""; }
    showToast(`預覽失敗：${error.message}`, { tone: "error", sticky: true });
  } finally {
    setBusy(false);
  }
}

class CancelledError extends Error {}

/** 建立一張採購單；後端回 postprocess_warning / duplicate_warning 時先確認再重送。使用者取消丟 CancelledError。 */
async function createOne(body, coveredParts) {
  let payload = { ...body, confirm: true };
  let r = await apiPost("/api/erp/po/create", payload);
  if (r.postprocess_warning?.length) {
    const uncovered = r.postprocess_warning.filter(w => !coveredParts.has(w.process_part));
    if (uncovered.length && !window.confirm(`${postprocessWarningText(uncovered)}\n\n仍要只開這張採購單嗎？`)) {
      throw new CancelledError("已取消：鈞洋件的後段加工件不在這批採購單中，未寫入。");
    }
    payload = { ...payload, confirm_missing_postprocess: true };
    r = await apiPost("/api/erp/po/create", payload);
  }
  if (r.duplicate_warning) {
    // 後端沒有寫入任何東西；使用者確認後才帶 allow_duplicate 重送
    if (!window.confirm(duplicateWarningText(r.duplicate_warning))) {
      throw new CancelledError("已取消：明細在本站已開過採購單，未寫入。");
    }
    r = await apiPost("/api/erp/po/create", { ...payload, allow_duplicate: true });
  }
  return r;
}

async function handleCreate() {
  if (_busy || !requireEdit()) return;
  const label = document.getElementById("erp-po-preview")?.dataset.target || _config?.label || "";
  let body;
  let companionBodies;
  try { body = buildBody(); companionBodies = buildCompanionBodies(); } catch (error) { showToast(error.message, { tone: "error" }); return; }
  const count = 1 + companionBodies.length;
  if (!window.confirm(`確定要在「${label}」建立${count > 1 ? ` ${count} 張` : "這張"}採購單嗎？\n寫入後無法從本系統修改或刪除。`)) return;
  setBusy(true);
  const result = document.getElementById("erp-po-result");
  const done = [];
  try {
    const covered = new Set(_companions.map(c => c.part));
    done.push(await createOne(body, covered));
    for (const c of companionBodies) done.push(await createOne(c, new Set()));
    if (result) {
      result.className = "erp-po-result is-ok";
      result.innerHTML = done.map(r =>
        `<div>✅ 已寫入 ${esc(r.target_label)}：採購單 <strong class="mono">${esc(r.bill_no)}</strong> ${poMailSlot(r.bill_no)}（${esc(r.supplier.id)}），含稅合計 ${fmtMoney(r.totals.total)}</div>`).join("");
      result.hidden = false;
      void decoratePoMailBadges(result);
    }
    const preview = document.getElementById("erp-po-preview");
    if (preview) { preview.hidden = true; preview.innerHTML = ""; }
    _lines = [newLine()];
    _companions = [];
    renderLines();
    renderCompanions();
    showToast(`已建立採購單 ${done.map(r => r.bill_no).join("、")}`, { tone: "success" });
  } catch (error) {
    if (result) {
      result.className = "erp-po-result is-error";
      const written = done.length ? `（已寫入 ${done.map(r => r.bill_no).join("、")}，其餘未寫入）` : "";
      result.textContent = error instanceof CancelledError ? `${error.message}${written}` : `❌ 寫入失敗：${error.message}${written}`;
      result.hidden = false;
    }
  } finally {
    setBusy(false);
    void refreshHistory();
  }
}

/** 重複開單警告（後端 duplicate_warning）轉成確認視窗文字：每行列出料號與已開過的單號 / 日期 / 數量。 */
export function duplicateWarningText(items) {
  const lines = (items || []).map(d => {
    const prior = (d.matches || []).map(m => `${m.bill_no}（${m.date}，數量 ${fmtMoney(m.qty)}）`).join("、");
    return `・${d.part_number}${d.supplier_id ? `（供應商 ${d.supplier_id}）` : ""}：已開 ${prior}`;
  });
  return `以下明細本站已經開過採購單：\n${lines.join("\n")}\n\n仍要再開一次嗎？`;
}

// ── 歷史 ────────────────────────────────────────────────────────────────────
/** 單行「為哪些訂單而買」的文字；空陣列 / 未提供代表舊單沒有紀錄。 */
export function allocationText(allocs) {
  if (!allocs || !allocs.length) return "—（舊單無紀錄）";
  if (allocs.every(a => a.kind === "manual")) return "手動（未綁訂單）";
  const parts = allocs.map(a => {
    if (a.kind === "extra") return `備品 ×${fmtMoney(a.qty)}`;
    if (a.kind === "manual") return `未綁訂單 ×${fmtMoney(a.qty)}`;
    const po = a.customer_po ? `（PO ${a.customer_po}）` : "";
    return `${[a.order_code, a.model].filter(Boolean).join(" ") || `訂單 ${a.order_id}`}${po} ×${fmtMoney(a.qty)}`;
  });
  return `用於：${parts.join("；")}`;
}

function historyLinesHtml(o, lines) {
  if (o.status !== "ok") return "";
  const byRow = new Map();
  for (const a of o.allocations || []) {
    if (!byRow.has(a.row_no)) byRow.set(a.row_no, []);
    byRow.get(a.row_no).push(a);
  }
  return lines.map((l, i) => `<div class="erp-po-hint"><span class="mono">${esc(l.part_number)}</span> ${esc(allocationText(byRow.get(l.row_no ?? i + 1)))}</div>`).join("");
}

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
        <td class="mono">${esc(o.bill_no) || "—"}${ok && o.bill_no ? ` ${poMailSlot(o.bill_no)}` : ""}</td>
        <td>${esc(o.target_db)}</td>
        <td>${esc(o.supplier_id)} ${esc(o.supplier_name)}</td>
        <td>${first}${more}${historyLinesHtml(o, lines)}</td>
        <td class="num">${ok ? fmtMoney(o.total) : "—"}</td>
        <td>${ok ? "成功" : `<span title="${esc(o.error)}">失敗：${esc(String(o.error).slice(0, 40))}</span>`}</td></tr>`;
    }).join("") : '<tr><td colspan="7" class="erp-po-hint">尚無紀錄</td></tr>';
  } catch (error) {
    body.innerHTML = `<tr><td colspan="7" class="erp-po-hint">讀取失敗：${esc(error.message)}</td></tr>`;
    return;
  }
  void decorateHistoryMail(false);
}

async function decorateHistoryMail(force) {
  const body = document.getElementById("erp-po-history");
  if (body) await decoratePoMailBadges(body, { force });
  const hint = document.getElementById("erp-po-mail-hint");
  if (hint) hint.textContent = await poMailFailedHint();
}

// 供缺料採購在寫入後更新開採購單的歷史紀錄
export function refreshErpPurchaseHistory() {
  return refreshHistory();
}

export async function onErpPurchaseTabActivated() {
  await Promise.all([loadConfig(), refreshHistory(), loadDefaultDue()]);
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
  const comp = document.getElementById("erp-po-companions");
  comp?.addEventListener("input", onCompanionsInput);
  comp?.addEventListener("change", onCompanionsChange);
  comp?.addEventListener("click", onCompanionsClick);
  document.getElementById("btn-erp-po-add-line")?.addEventListener("click", () => {
    if (_lines.length >= 50) { showToast("最多 50 行", { tone: "error" }); return; }
    _lines.push(newLine());
    renderLines();
  });
  document.getElementById("btn-erp-po-preview")?.addEventListener("click", handlePreview);
  document.getElementById("btn-erp-po-mail-refresh")?.addEventListener("click", () => { void decorateHistoryMail(true); });
  document.getElementById("erp-po-supplier-input")?.addEventListener("input", () => {
    clearTimeout(_searchTimer);
    _searchTimer = setTimeout(() => { void searchSuppliers(); }, 250);
  });
  document.getElementById("erp-po-supplier-results")?.addEventListener("click", event => {
    const li = event.target.closest("li[data-id]");
    if (li) selectSupplier(li.dataset.id, li.dataset.name);
  });
  await Promise.all([loadConfig(), loadDefaultDue()]);
}
