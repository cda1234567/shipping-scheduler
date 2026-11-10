import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";
import { createOpenLinePicker } from "/static/modules/open_line_picker.js";

// 開 Invoice：由 S/C（外銷訂單）開立 ST- Invoice。依客戶鎖定，數量預帶 S/C 未完成量，可改數量 / 單價。

let _config = null;
let _picker = null;
let _initialized = false;
let _busy = false;
let _preview = null;
let _token = "";
let _rePreviewPending = false;
let _costTouched = false;
let _rate = "";            // 預覽畫面輸入的發票匯率；空 = 用正航幣別檔匯率
let _rateCustomer = null;  // 匯率是哪個客戶時輸入的；換客戶就不沿用
let _lastShown = null;     // 最後一次成功畫出的預覽（預覽失敗時保留給使用者對照）

const fmt = v => (Number.isFinite(Number(v)) ? Number(v).toLocaleString("zh-TW", { maximumFractionDigits: 4 }) : "—");
const el = id => document.getElementById(id);

function pad(n) { return String(n).padStart(2, "0"); }
function todayText() { const d = new Date(); return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`; }
function ymd(n) { const s = String(n || ""); return s.length === 8 ? `${s.slice(0, 4)}/${s.slice(4, 6)}/${s.slice(6)}` : "—"; }
function newToken() { return (crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`).toString(); }

/** 勾選明細 → 請求明細（純函式）；數量需 > 0、單價需 >= 0，否則丟出錯誤。 */
export function invoiceLines(selection) {
  if (!selection.length) throw new Error("請先勾選要開 Invoice 的 S/C 明細");
  return selection.map(s => {
    if (!Number.isFinite(s.qty) || s.qty <= 0) throw new Error(`${s.line.sc_no} #${s.line.row_no} 的數量需大於 0`);
    if (!Number.isFinite(s.price) || s.price < 0) throw new Error(`${s.line.sc_no} #${s.line.row_no} 的單價不正確`);
    return { sc_no: s.line.sc_no, row_no: s.line.row_no, qty: s.qty, price: s.price };
  });
}

/** 匯率輸入值 → 數字；空白或不合法回 null（= 後端用正航幣別檔匯率）。 */
export function parseRate(value) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  const n = Number(text);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function requireEdit() {
  if (isEditAuthenticated()) return true;
  showToast("目前為唯讀模式，請先登入編輯。");
  openEditAuthModal();
  return false;
}

// label 已帶「（正式）」/「（測試）」，例如「公司13（測試）」
function targetText(label, isProduction) {
  const text = label || "";
  if (isProduction && !text.includes("正式")) return `${text}（正式）`;
  return text;
}

function renderBadge() {
  const badge = el("erp-invoice-badge");
  if (!badge) return;
  if (!_config) { badge.className = "erp-po-badge is-unavailable"; badge.textContent = "無法使用：讀取設定失敗"; return; }
  if (!_config.available) { badge.className = "erp-po-badge is-unavailable"; badge.textContent = `無法使用　${_config.reason || ""}`; return; }
  badge.className = `erp-po-badge ${_config.is_production ? "is-prod" : "is-test"}`;
  badge.textContent = `寫入：${targetText(_config.label, _config.is_production)}　預設出貨倉 ${_config.warehouse}`;
}

function updateCostChecked() {
  const box = el("erp-invoice-update-cost");
  return box ? box.checked : true;
}

function syncCostDefault() {
  const box = el("erp-invoice-update-cost");
  if (box && _config && !_costTouched) box.checked = _config.update_cost_default !== false;
}

function currentRate() {
  if (!_rate || _picker.lockedGroup() !== _rateCustomer) return null;
  return parseRate(_rate);
}

function currentInvDate() {
  const date = el("erp-invoice-date")?.value || "";
  return date ? Number(date.replaceAll("-", "")) : null;
}

function buildBody(confirm = false) {
  const lines = invoiceLines(_picker.getSelection());
  const invDate = currentInvDate();
  if (!invDate) throw new Error("請先填發票日期");
  return {
    target: _config?.target || null,
    inv_date: invDate,
    lines,
    confirm,
    client_token: _token,
    update_cost: updateCostChecked(),
    exch_rate: currentRate(),
  };
}

// 匯率輸入框：change（離開欄位）或按 Enter 都重新預覽；預覽中再改會排隊，不會被吃掉
function applyRate(value) {
  const n = parseRate(value);
  if (n === null) { showToast("發票匯率必須大於 0", { tone: "error" }); return; }
  const same = _rate === String(n) && _rateCustomer === _picker.lockedGroup();
  _rate = String(n);
  _rateCustomer = _picker.lockedGroup();
  if (_busy) { if (!same) _rePreviewPending = true; return; }
  if (same && _preview && Number(_preview.exch_rate) === n) return;  // Enter 後又 blur：不重複預覽
  void handlePreview();
}

function bindRateInput() {
  const input = el("erp-invoice-rate");
  if (!input) return;
  input.addEventListener("input", () => {
    // 匯率改了但還沒重新預覽：先鎖住確認鈕，免得照舊金額寫入
    const btn = el("btn-erp-invoice-create");
    if (btn) { btn.disabled = true; btn.title = "匯率已修改，按 Enter 或點別處重新計算"; }
    const tip = el("erp-invoice-rate-tip");
    if (tip) tip.hidden = false;
  });
  input.addEventListener("change", event => applyRate(event.target.value));
  input.addEventListener("keydown", event => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    applyRate(event.target.value);
  });
}

function rateInput(r) {
  if (r.rate_source === "local") return "";
  const h = r.rate_hint || {};
  const hint = [h.sc ? `S/C ${h.sc}` : "", h.currency ? `正航幣別檔 ${h.currency}` : ""].filter(Boolean).join(" / ");
  // 使用者輸入過的匯率優先（預覽失敗重畫時也不會把剛打的數字清掉）
  const typed = currentRate();
  const val = typed !== null ? esc(typed) : (r.rate_required ? "" : esc(r.exch_rate));
  return `　<label class="erp-receive-rate-label"><strong>發票匯率</strong>（${esc(r.currency)}）<input type="number" id="erp-invoice-rate" class="num" min="0" step="any" style="width:7em" placeholder="照發票填" value="${val}"></label> <span id="erp-invoice-rate-tip" class="erp-po-hint" style="display:inline" hidden>按 Enter 重新計算</span>${hint ? ` <span style="color:#999">參考：${esc(hint)}</span>` : ""}${r.rate_required ? ' <strong class="erp-po-warn" style="display:inline">請輸入發票上的匯率</strong>' : ""}`;
}

function hidePreview() {
  _preview = null;
  _lastShown = null;
  _token = "";
  const p = el("erp-invoice-preview");
  if (p) { p.hidden = true; p.innerHTML = ""; }
}

function renderPreview(r, error = "") {
  const p = el("erp-invoice-preview");
  const warn = (r.warnings || []).map(w => `<div class="erp-po-warn">${esc(w.message)}</div>`).join("");
  const cur = esc(r.currency || "NTD");
  const costText = r.update_cost === false ? '<span class="erp-receive-cost-off">不扣總成本</span>' : "扣總成本";
  const errorBox = error ? `<div id="erp-invoice-preview-error" class="erp-po-result is-error">❌ 預覽失敗：${esc(error)}（下方是上一次的預覽，不能照這份開立；修正後按 Enter 或「預覽」重試）</div>` : "";
  const blocked = Boolean(error) || r.rate_required;
  p.innerHTML = `${errorBox}
    <div class="erp-po-badge ${r.is_production ? "is-prod" : "is-test"} erp-po-badge-small">寫入：${esc(targetText(r.target_label, r.is_production))}　${costText}</div>
    <div class="erp-po-preview-head">預估單號（寫入當下才會確定）：<strong class="mono">${esc(r.bill_no)}</strong>　客戶 ${esc(r.customer.id)} ${esc(r.customer.name)}　來源 S/C <span class="mono">${esc((r.sc_nos || []).join("、"))}</span></div>
    <div class="erp-po-hint">發票日期 ${ymd(r.inv_date)}　幣別 ${cur}${rateInput(r)}</div>
    ${warn}
    <div class="erp-po-table-wrap"><table class="erp-po-table">
      <thead><tr><th>#</th><th>S/C</th><th>料號</th><th>品名</th><th class="num">未完成量</th><th class="num">數量</th><th class="num">單價（${cur}）</th><th class="num">金額（${cur}）</th><th class="num">本幣金額</th><th>倉別</th><th class="num">庫存</th></tr></thead>
      <tbody>${r.lines.map(l => `<tr><td>${l.row_no}</td><td class="mono">${esc(l.sc_no)} #${l.sc_row}</td><td class="mono">${esc(l.prod_id)}</td><td>${esc(l.prod_name)}</td><td class="num">${fmt(l.qty_remain)}</td><td class="num">${fmt(l.qty)}</td><td class="num">${fmt(l.price)}</td><td class="num">${fmt(l.amount)}</td><td class="num">${fmt(l.ml_amount)}</td><td class="mono">${esc(l.warehouse)}</td><td class="num">${fmt(l.stock)}</td></tr>`).join("")}</tbody>
    </table></div>
    <div class="erp-po-totals">數量 ${fmt(r.totals.sum_qty)}　<strong>合計 ${cur} ${fmt(r.totals.total)}</strong>　本幣 ${fmt(r.totals.local_total)}（匯率 ${fmt(r.exch_rate)}）</div>
    <div class="erp-po-actions"><span class="spacer"></span><button class="btn btn-primary" id="btn-erp-invoice-create" type="button"${blocked ? ` disabled title="${error ? "預覽失敗，請重新預覽" : "請先輸入發票匯率"}"` : ""}>確認開立</button></div>`;
  p.hidden = false;
  el("btn-erp-invoice-create").addEventListener("click", handleCreate);
  bindRateInput();
}

function showPreviewError(message, last) {
  _preview = null;
  _token = "";
  const p = el("erp-invoice-preview");
  if (!p) return;
  if (last) {
    try { renderPreview(last, message); return; } catch (_) { /* 舊資料也畫不出來就只顯示錯誤 */ }
  }
  p.innerHTML = `<div id="erp-invoice-preview-error" class="erp-po-result is-error">❌ 預覽失敗：${esc(message)}</div>`;
  p.hidden = false;
}

function markPreviewBusy() {
  const btn = el("btn-erp-invoice-create");
  if (btn) { btn.disabled = true; btn.textContent = "重新預覽中…"; }
}

// 發票日期一改就重新預覽（單號年份、帳款月份跟著變）
async function handleDateChange() {
  if (!currentInvDate()) { hidePreview(); return; }
  if (!_preview && !el("erp-invoice-preview")?.innerHTML) return;  // 還沒預覽過就不自動跑
  if (_busy) { _rePreviewPending = true; return; }
  await handlePreview();
}

async function handlePreview() {
  if (_busy || !requireEdit()) return;
  hideResult();
  let body;
  try { body = buildBody(); } catch (error) { showToast(error.message, { tone: "error" }); return; }
  _busy = true;
  const last = _preview || _lastShown;
  markPreviewBusy();
  try {
    _token = newToken();
    const r = await apiPost("/api/erp-invoice/preview", body);
    renderPreview(r);
    _preview = r;
    _lastShown = r;
  } catch (error) {
    console.error("erp-invoice preview failed", error);
    showPreviewError(error.message || String(error), last);
    showToast(`預覽失敗：${error.message || error}`, { tone: "error", sticky: true });
  } finally {
    _busy = false;
    if (_rePreviewPending) { _rePreviewPending = false; void handlePreview(); }
  }
}

function hideResult() {
  const r = el("erp-invoice-result");
  if (r) { r.hidden = true; r.innerHTML = ""; }
}

function confirmText(items) {
  return `${items.map(o => (o.kind === "stock_short"
    ? `・${o.part_number} 倉 ${o.warehouse}：現有 ${fmt(o.stock)}，需 ${fmt(o.qty)}（庫存會變負）`
    : `・${o.sc_no} #${o.row_no} ${o.part_number}：未完成 ${fmt(o.qty_remain)}，開 ${fmt(o.qty)}（剩餘量會變負）`)).join("\n")}\n\n仍要開立嗎？`;
}

async function handleCreate() {
  if (_busy || !requireEdit()) return;
  let body;
  try { body = buildBody(); } catch (error) { showToast(error.message, { tone: "error" }); return; }
  if (!_preview || Number(_preview.inv_date) !== Number(body.inv_date)) {
    showToast(_preview ? "發票日期已變更，已重新預覽，請確認後再按「確認開立」" : "已重新預覽，請確認後再按「確認開立」");
    await handlePreview();
    return;
  }
  if (_preview.update_cost !== body.update_cost) {
    showToast("「扣總成本」已變更，已重新預覽，請確認後再按「確認開立」");
    await handlePreview();
    return;
  }
  if (_preview.rate_required && body.exch_rate === null) {
    showToast("請先輸入發票匯率（大於 0）", { tone: "error" });
    return;
  }
  if (body.exch_rate !== null && Number(_preview.exch_rate) !== body.exch_rate) {
    showToast("匯率已變更，已重新預覽，請確認後再按「確認開立」");
    await handlePreview();
    return;
  }
  // 寫入時帶預覽用的匯率，確保寫入金額與畫面一致
  body.exch_rate = Number(_preview.exch_rate);
  const tgt = targetText(_preview.target_label || _config?.label, _preview.is_production);
  if (!window.confirm(`寫入：${tgt}${_preview.is_production ? "\n⚠ 這是正式公司（正航正式帳），會直接寫入正式資料。" : ""}\n確定要在「${tgt}」開立 Invoice 嗎？\n客戶 ${_preview.customer.id} ${_preview.customer.name}　發票日期 ${ymd(body.inv_date)}\n幣別 ${_preview.currency}　匯率 ${_preview.exch_rate}　合計 ${fmt(_preview.totals?.total)}　本幣 ${fmt(_preview.totals?.local_total)}\n寫入後無法從本系統修改或刪除。`)) return;
  _busy = true;
  const btn = el("btn-erp-invoice-create");
  if (btn) btn.disabled = true;
  const result = el("erp-invoice-result");
  try {
    let r = await apiPost("/api/erp-invoice/create", body);
    if (r.confirm_warning) {
      if (!window.confirm(confirmText(r.confirm_warning))) { showToast("已取消，未寫入"); return; }
      r = await apiPost("/api/erp-invoice/create", { ...body, confirm: true });
    }
    result.className = "erp-po-result is-ok";
    result.innerHTML = `✅ 已寫入 ${esc(r.target_label)}：Invoice <strong class="mono">${esc(r.bill_no)}</strong>，客戶 ${esc(r.customer.id)}，${esc(r.currency)} ${fmt(r.totals.total)}（本幣 ${fmt(r.totals.local_total)}）${r.closed_scs?.length ? `　已結案 S/C：${esc(r.closed_scs.join("、"))}` : ""}`;
    result.hidden = false;
    hidePreview();
    _picker.clear();
    await _picker.reload();
    showToast(`已開立 Invoice ${r.bill_no}`, { tone: "success" });
  } catch (error) {
    result.className = "erp-po-result is-error";
    result.textContent = `❌ 寫入失敗：${error.message}`;
    result.hidden = false;
  } finally {
    _busy = false;
    if (btn) btn.disabled = false;
    void refreshHistory();
  }
}

async function refreshHistory() {
  const body = el("erp-invoice-history");
  if (!body) return;
  try {
    const rows = (await apiJson("/api/erp-invoice/history")).invoices || [];
    body.innerHTML = rows.length ? rows.map(o => {
      const ok = o.status === "ok";
      return `<tr class="${ok ? "" : "erp-po-row-error"}"><td>${esc(String(o.created_at).slice(0, 16).replace("T", " "))}</td>
        <td class="mono">${ok ? esc(o.bill_no) : "—"}</td><td>${esc(o.target_db)}</td>
        <td>${esc(o.customer_id)} ${esc(o.customer_name)}</td>
        <td class="num">${ok ? `${esc(o.currency)} ${fmt(o.total)}` : "—"}</td><td class="num">${ok ? fmt(o.exch_rate) : "—"}</td>
        <td>${ok ? "成功" : `<span title="${esc(o.error)}">失敗：${esc(String(o.error).slice(0, 40))}</span>`}</td></tr>`;
    }).join("") : '<tr><td colspan="7" class="erp-po-hint">尚無紀錄</td></tr>';
  } catch (error) {
    body.innerHTML = `<tr><td colspan="7" class="erp-po-hint">讀取失敗：${esc(error.message)}</td></tr>`;
  }
}

async function loadConfig() {
  try { _config = await apiJson("/api/erp-invoice/config"); } catch (_) { _config = null; }
  renderBadge();
  syncCostDefault();
}

export async function onErpInvoiceTabActivated() {
  await loadConfig();
  if (_config?.available) {
    try { await _picker.reload(); } catch (error) { showToast(`讀取 S/C 明細失敗：${error.message}`, { tone: "error" }); }
  }
  void refreshHistory();
}

export async function initErpInvoice() {
  if (_initialized) return;
  _initialized = true;
  el("erp-invoice-date").value = todayText();
  _picker = createOpenLinePicker(el("erp-invoice-picker"), {
    // 已鎖定客戶時只抓該客戶（勾選跨搜尋保留，且不會被 300 筆上限截掉）
    fetchLines: async ({ q, group }) => apiJson(`/api/erp-invoice/open-lines?q=${encodeURIComponent(q)}${group ? `&customer=${encodeURIComponent(group)}` : ""}`),
    lineKey: l => `${l.sc_no}#${l.row_no}`,
    groupKey: l => l.customer_id,
    groupLabel: l => `${l.customer_id} ${l.customer_name}`,
    columns: [
      { label: "S/C 單號", render: l => `<span class="mono">${esc(l.sc_no)}</span>` },
      { label: "列", align: "num", render: l => esc(l.row_no) },
      { label: "客戶", render: l => esc(`${l.customer_id} ${l.customer_name}`) },
      { label: "料號", render: l => `<span class="mono">${esc(l.part_number)}</span>` },
      { label: "品名", render: l => esc(l.part_name) },
      { label: "未完成量", align: "num", render: l => fmt(l.qty_remain) },
      { label: "S/C 單價", align: "num", render: l => fmt(l.price) },
      { label: "幣別", render: l => esc(l.currency) },
    ],
    qtyOf: l => l.qty_remain,
    priceOf: l => l.price,
    searchPlaceholder: "搜尋客戶 / 料號 / S/C 單號 / 品名",
    emptyText: "沒有未完成的 S/C 明細",
    otherGroupText: "已隱藏其他客戶 {n} 筆",
    multiGroupText: "目前顯示多個客戶，請先在搜尋框輸入客戶 / 單號縮小到單一客戶再全選（不可混客戶）",
    onChange: () => hidePreview(),
  });
  el("btn-erp-invoice-preview").addEventListener("click", handlePreview);
  el("erp-invoice-date").addEventListener("change", () => { void handleDateChange(); });
  el("erp-invoice-update-cost")?.addEventListener("change", () => { _costTouched = true; void handleDateChange(); });
  await loadConfig();
}
