import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";
import { createOpenLinePicker } from "/static/modules/open_line_picker.js";

let _config = null;
let _picker = null;
let _initialized = false;
let _busy = false;
let _preview = null;
let _token = "";

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
  badge.className = "erp-po-badge is-test";
  badge.textContent = `寫入：${_config.label}（測試庫）　倉庫 ${_config.warehouse}`;
}

function buildBody(confirm = false) {
  const sel = _picker.getSelection();
  if (!sel.length) throw new Error("請先勾選要收料的明細");
  for (const s of sel) {
    if (!Number.isFinite(s.qty) || s.qty <= 0) throw new Error(`${s.line.po_no} #${s.line.row_no} 的數量需大於 0`);
    if (!Number.isFinite(s.price) || s.price < 0) throw new Error(`${s.line.po_no} #${s.line.row_no} 的單價不正確`);
  }
  const date = el("erp-receive-date").value;
  return {
    target: _config?.target || null,
    rm_date: date ? Number(date.replaceAll("-", "")) : null,
    lines: sel.map(s => ({ po_no: s.line.po_no, row_no: s.line.row_no, qty: s.qty, price: s.price })),
    confirm,
    client_token: _token,
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
  p.innerHTML = `
    <div class="erp-po-preview-head">預估單號（寫入當下才會確定）：收料 <strong class="mono">${esc(r.im_no)}</strong>　驗收 <strong class="mono">${esc(r.qm_no)}</strong>　進貨 <strong class="mono">${esc(r.rm_no)}</strong>　廠商 ${esc(r.vendor.id)} ${esc(r.vendor.name)}</div>
    <div class="erp-po-hint">收料 / 驗收日 ${ymd(r.today)}　進貨日 ${ymd(r.rm_date)}　帳款月份 ${esc(r.terms.AccMonth)}　預計付款日 ${ymd(r.terms.PrepayDay)}　兌現日 ${ymd(r.terms.EncashDay)}</div>
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
  if (!window.confirm(`確定要在「${_config?.label || ""}」建立收料 / 驗收 / 進貨嗎？\n寫入後無法從本系統修改或刪除。`)) return;
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
      { label: "採購單", render: l => `<span class="mono">${esc(l.po_no)} #${l.row_no}</span>` },
      { label: "日期", render: l => esc(l.date_text) },
      { label: "廠商", render: l => esc(`${l.vendor_id} ${l.vendor_name}`) },
      { label: "料號", render: l => `<span class="mono">${esc(l.part_number)}</span>` },
      { label: "品名", render: l => esc(l.part_name) },
      { label: "未收量", align: "num", render: l => fmt(l.qty_remain) },
    ],
    qtyOf: l => l.qty_remain,
    priceOf: l => l.price,
    onChange: () => hidePreview(),
  });
  el("btn-erp-receive-preview").addEventListener("click", handlePreview);
  el("erp-receive-date").addEventListener("change", hidePreview);
  await loadConfig();
}
