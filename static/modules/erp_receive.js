import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";
import { decoratePoMailBadges, poMailSlot } from "/static/modules/po_mail_status.js";
import { createOpenLinePicker } from "/static/modules/open_line_picker.js";

let _config = null;
let _picker = null;
let _initialized = false;
let _busy = false;
let _preview = null;
let _token = "";
let _rePreviewPending = false;
let _costTouched = false;

const fmt = v => (Number.isFinite(Number(v)) ? Number(v).toLocaleString("zh-TW", { maximumFractionDigits: 4 }) : "—");
const el = id => document.getElementById(id);

function pad(n) { return String(n).padStart(2, "0"); }
function todayText() { const d = new Date(); return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`; }
function ymd(n) { const s = String(n || ""); return s.length === 8 ? `${s.slice(0, 4)}/${s.slice(4, 6)}/${s.slice(6)}` : "—"; }
function newToken() { return (crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`).toString(); }

function requireEdit() {
  if (isEditAuthenticated()) return true;
  showToast("目前為唯讀模式，請先登入編輯。");
  openEditAuthModal();
  return false;
}

function renderBadge() {
  const badge = el("erp-receive-badge");
  if (!badge) return;
  if (!_config) { badge.className = "erp-po-badge is-unavailable"; badge.textContent = "無法使用：讀取設定失敗"; return; }
  if (!_config.available) { badge.className = "erp-po-badge is-unavailable"; badge.textContent = `無法使用　${_config.reason || ""}`; return; }
  badge.className = `erp-po-badge ${_config.is_production ? "is-prod" : "is-test"}`;
  badge.textContent = `寫入：${targetText(_config.label, _config.is_production)}　倉庫 ${_config.warehouse}`;
}

// label 已帶「（正式）」/「（測試）」，例如「公司3（正式）」
function targetText(label, isProduction) {
  const text = label || "";
  if (isProduction && !text.includes("正式")) return `${text}（正式）`;
  return text;
}

function updateCostChecked() {
  const box = el("erp-receive-update-cost");
  return box ? box.checked : true;
}

function syncCostDefault() {
  const box = el("erp-receive-update-cost");
  if (box && _config && !_costTouched) box.checked = _config.update_cost_default !== false;
}

function buildBody(confirm = false) {
  const sel = _picker.getSelection();
  if (!sel.length) throw new Error("請先勾選要收料的明細");
  for (const s of sel) {
    if (!Number.isFinite(s.qty) || s.qty <= 0) throw new Error(`${s.line.po_no} #${s.line.row_no} 的數量需大於 0`);
    if (!Number.isFinite(s.price) || s.price < 0) throw new Error(`${s.line.po_no} #${s.line.row_no} 的單價不正確`);
  }
  const date = el("erp-receive-date").value;
  if (!date) throw new Error("請先填發票日期（進貨日）");
  return {
    target: _config?.target || null,
    rm_date: date ? Number(date.replaceAll("-", "")) : null,
    lines: sel.map(s => ({ po_no: s.line.po_no, row_no: s.line.row_no, qty: s.qty, price: s.price })),
    confirm,
    client_token: _token,
    update_cost: updateCostChecked(),
  };
}

function hidePreview() {
  _preview = null;
  _token = "";
  const p = el("erp-receive-preview");
  if (p) { p.hidden = true; p.innerHTML = ""; }
}

function renderPreview(r) {
  const p = el("erp-receive-preview");
  const warn = (r.warnings || []).map(w => `<div class="erp-po-warn">${esc(w.message)}</div>`).join("");
  const costText = r.update_cost === false
    ? '<span class="erp-receive-cost-off">不更新平均成本</span>'
    : "更新平均成本";
  p.innerHTML = `
    <div class="erp-po-badge ${r.is_production ? "is-prod" : "is-test"} erp-po-badge-small">寫入：${esc(targetText(r.target_label, r.is_production))}　${costText}</div>
    <div class="erp-po-preview-head">預估單號（寫入當下才會確定）：收料 <strong class="mono">${esc(r.im_no)}</strong>　驗收 <strong class="mono">${esc(r.qm_no)}</strong>　進貨 <strong class="mono">${esc(r.rm_no)}</strong>　廠商 ${esc(r.vendor.id)} ${esc(r.vendor.name)}</div>
    <div class="erp-po-hint">收料 / 驗收日 ${ymd(r.today)}　<span class="erp-receive-preview-date">發票日期（進貨日）${ymd(r.rm_date)}</span>　帳款月份 ${esc(r.terms.AccMonth)}　預計付款日 ${ymd(r.terms.PrepayDay)}　兌現日 ${ymd(r.terms.EncashDay)}</div>
    ${warn}
    <div class="erp-po-table-wrap"><table class="erp-po-table">
      <thead><tr><th>#</th><th>採購單</th><th>料號</th><th class="num">數量</th><th class="num">單價</th><th class="num">未稅金額</th><th class="num">稅額</th></tr></thead>
      <tbody>${r.lines.map(l => `<tr><td>${l.row_no}</td><td class="mono">${esc(l.po_no)} #${l.po_row}</td><td class="mono">${esc(l.prod_id)}</td><td class="num">${fmt(l.qty)}</td><td class="num">${fmt(l.price)}</td><td class="num">${fmt(l.amount)}</td><td class="num">${fmt(l.tax)}</td></tr>`).join("")}</tbody>
    </table></div>
    <div class="erp-po-totals">數量 ${fmt(r.totals.sum_qty)}　未稅 ${fmt(r.totals.sum_amount)}　稅 ${fmt(r.totals.sum_tax)}　<strong>含稅合計 ${fmt(r.totals.total)}</strong></div>
    <div class="erp-po-actions"><span class="spacer"></span><button class="btn btn-primary" id="btn-erp-receive-create" type="button">確認收料</button></div>`;
  p.hidden = false;
  el("btn-erp-receive-create").addEventListener("click", handleCreate);
}

function currentRmDate() {
  const date = el("erp-receive-date")?.value || "";
  return date ? Number(date.replaceAll("-", "")) : null;
}

// 發票日期（= 進貨日）一改就重新預覽，讓帳款月份 / 付款日 / 兌現日跟著更新
async function handleDateChange() {
  if (!currentRmDate()) { hidePreview(); return; }
  if (!_preview && !el("erp-receive-preview")?.innerHTML) return;  // 還沒預覽過就不自動跑
  if (_busy) { _rePreviewPending = true; return; }
  await handlePreview();
}

async function handlePreview() {
  if (_busy || !requireEdit()) return;
  hideResult();
  let body;
  try { body = buildBody(); } catch (error) { showToast(error.message, { tone: "error" }); return; }
  _busy = true;
  try {
    _token = newToken();
    _preview = await apiPost("/api/erp-receive/preview", body);
    renderPreview(_preview);
  } catch (error) {
    hidePreview();
    showToast(`預覽失敗：${error.message}`, { tone: "error", sticky: true });
  } finally {
    _busy = false;
    if (_rePreviewPending) { _rePreviewPending = false; void handlePreview(); }
  }
}

function hideResult() {
  const r = el("erp-receive-result");
  if (r) { r.hidden = true; r.innerHTML = ""; }
}

function overText(items) {
  return `以下明細收料量超過採購單剩餘量（剩餘量最多扣到 0）：\n${items.map(o => `・${o.po_no} #${o.row_no} ${o.part_number}：剩 ${fmt(o.qty_remain)}，收 ${fmt(o.qty)}`).join("\n")}\n\n仍要收料嗎？`;
}

async function handleCreate() {
  if (_busy || !requireEdit()) return;
  let body;
  try { body = buildBody(); } catch (error) { showToast(error.message, { tone: "error" }); return; }
  if (!_preview || Number(_preview.rm_date) !== Number(body.rm_date)) {
    // 預覽與畫面上的發票日期不一致：先重新預覽，讓使用者看過新的帳款月份再確認
    showToast("發票日期已變更，已重新預覽，請確認後再按「確認收料」");
    await handlePreview();
    return;
  }
  if (_preview.update_cost !== body.update_cost) {
    showToast("「更新平均成本」已變更，已重新預覽，請確認後再按「確認收料」");
    await handlePreview();
    return;
  }
  const tgt = targetText(_preview.target_label || _config?.label, _preview.is_production);
  if (!window.confirm(`寫入：${tgt}${_preview.is_production ? "\n⚠ 這是正式公司（正航正式帳），會直接寫入正式資料。" : ""}\n平均成本：${body.update_cost ? "更新" : "不更新"}\n確定要在「${tgt}」建立收料 / 驗收 / 進貨嗎？\n發票日期（進貨日）：${ymd(body.rm_date)}　帳款月份 ${_preview.terms?.AccMonth ?? ""}\n寫入後無法從本系統修改或刪除。`)) return;
  _busy = true;
  const btn = el("btn-erp-receive-create");
  if (btn) btn.disabled = true;
  const result = el("erp-receive-result");
  try {
    let r = await apiPost("/api/erp-receive/create", body);
    if (r.over_receive_warning) {
      if (!window.confirm(overText(r.over_receive_warning))) { showToast("已取消，未寫入"); return; }
      r = await apiPost("/api/erp-receive/create", { ...body, confirm: true });
    }
    result.className = "erp-po-result is-ok";
    result.innerHTML = `✅ 已寫入 ${esc(r.target_label)}：收料 <strong class="mono">${esc(r.im_no)}</strong>　驗收 <strong class="mono">${esc(r.qm_no)}</strong>　進貨 <strong class="mono">${esc(r.rm_no)}</strong>，含稅合計 ${fmt(r.totals.total)}${r.closed_pos?.length ? `　已結案採購單：${esc(r.closed_pos.join("、"))}` : ""}`;
    result.hidden = false;
    hidePreview();
    _picker.clear();
    await _picker.reload();
    showToast(`已建立進貨單 ${r.rm_no}`, { tone: "success" });
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
  const body = el("erp-receive-history");
  if (!body) return;
  try {
    const rows = (await apiJson("/api/erp-receive/history")).receipts || [];
    body.innerHTML = rows.length ? rows.map(o => {
      const ok = o.status === "ok";
      return `<tr class="${ok ? "" : "erp-po-row-error"}"><td>${esc(String(o.created_at).slice(0, 16).replace("T", " "))}</td>
        <td class="mono">${ok ? `${esc(o.im_no)} / ${esc(o.qm_no)} / ${esc(o.rm_no)}` : "—"}</td><td>${esc(o.target_db)}</td>
        <td>${esc(o.vendor_id)} ${esc(o.vendor_name)}</td><td class="num">${ok ? fmt(o.total) : "—"}</td>
        <td>${ok ? "成功" : `<span title="${esc(o.error)}">失敗：${esc(String(o.error).slice(0, 40))}</span>`}</td></tr>`;
    }).join("") : '<tr><td colspan="6" class="erp-po-hint">尚無紀錄</td></tr>';
  } catch (error) {
    body.innerHTML = `<tr><td colspan="6" class="erp-po-hint">讀取失敗：${esc(error.message)}</td></tr>`;
  }
}

async function loadConfig() {
  try { _config = await apiJson("/api/erp-receive/config"); } catch (_) { _config = null; }
  renderBadge();
  syncCostDefault();
}

export async function onErpReceiveTabActivated() {
  await loadConfig();
  if (_config?.available) {
    try { await _picker.reload(); } catch (error) { showToast(`讀取明細失敗：${error.message}`, { tone: "error" }); }
  }
  void refreshHistory();
}

export async function initErpReceive() {
  if (_initialized) return;
  _initialized = true;
  el("erp-receive-date").value = todayText();
  _picker = createOpenLinePicker(el("erp-receive-picker"), {
    fetchLines: async ({ q }) => apiJson(`/api/erp-receive/open-lines?q=${encodeURIComponent(q)}`),
    lineKey: l => `${l.po_no}#${l.row_no}`,
    groupKey: l => l.vendor_id,
    groupLabel: l => `${l.vendor_id} ${l.vendor_name}`,
    columns: [
      { label: "採購單", render: l => `<span class="mono">${esc(l.po_no)} #${l.row_no}</span> ${poMailSlot(l.po_no)}` },
      { label: "日期", render: l => esc(l.date_text) },
      { label: "廠商", render: l => esc(`${l.vendor_id} ${l.vendor_name}`) },
      { label: "料號", render: l => `<span class="mono">${esc(l.part_number)}</span>` },
      { label: "品名", render: l => esc(l.part_name) },
      { label: "未收量", align: "num", render: l => fmt(l.qty_remain) },
    ],
    qtyOf: l => l.qty_remain,
    priceOf: l => l.price,
    otherGroupText: "已隱藏其他廠商 {n} 筆",
    multiGroupText: "目前顯示多家廠商，請先在搜尋框輸入廠商 / 單號縮小到單一廠商再全選（不可混廠商）",
    onRender: root => { void decoratePoMailBadges(root); },
    onChange: () => hidePreview(),
  });
  el("btn-erp-receive-preview").addEventListener("click", handlePreview);
  el("erp-receive-date").addEventListener("change", () => { void handleDateChange(); });
  el("erp-receive-update-cost")?.addEventListener("change", () => { _costTouched = true; void handleDateChange(); });
  await loadConfig();
}
