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
let _rate = "";          // 預覽畫面輸入的發票匯率（外幣採購單）；空 = 尚未輸入，不能寫入
let _rateVendor = null;  // 匯率是哪家廠商時輸入的；換廠商就不沿用

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
    exch_rate: currentRate(),
  };
}

function currentRate() {
  if (!_rate || _picker.lockedGroup() !== _rateVendor) return null;
  const n = Number(_rate);
  return Number.isFinite(n) && n > 0 ? n : null;
}


function currencyBlock(r) {
  const tax = Number(r.tax_type || 0) === 0 ? esc(r.tax_label || "應稅") : `<strong>${esc(r.tax_label || "")}</strong>`;
  if (!r.mode || r.mode === "local") return `<div class="erp-po-hint">幣別 ${esc(r.currency)}　課稅別：${tax}</div>`;
  const what = r.mode === "foreign"
    ? `<strong>${esc(r.currency)} 進貨單</strong>（單價 / 金額為 ${esc(r.currency)}，台幣 = 金額 × 匯率）`
    : `採購單幣別 <strong>${esc(r.po_currency)}</strong> → 開<strong>台幣</strong>進貨單（單價 = ${esc(r.po_currency)} 單價 × 匯率）`;
  return `<div class="erp-po-hint erp-receive-currency">${what}　課稅別：${tax}</div>`;
}

function rateInput(r) {
  if (!r.mode || r.mode === "local") return "";
  const h = r.rate_hint || {};
  const hint = [h.po ? `採購單 ${h.po}` : "", h.currency ? `正航幣別檔 ${h.currency}` : ""].filter(Boolean).join(" / ");
  const val = r.rate_required ? "" : esc(r.exch_rate);
  return `　<label class="erp-receive-rate-label"><strong>發票匯率</strong>（${esc(r.po_currency)}）<input type="number" id="erp-receive-rate" class="num" min="0" step="any" style="width:7em" placeholder="照發票填" value="${val}"></label>${hint ? ` <span style="color:#999">參考：${esc(hint)}</span>` : ""}${r.rate_required ? ' <strong class="erp-po-warn" style="display:inline">請輸入發票上的匯率</strong>' : ""}`;
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
  const cur = esc(r.currency || "NTD");
  const foreign = r.mode === "foreign";
  const conv = r.mode === "convert";
  const costText = r.update_cost === false
    ? '<span class="erp-receive-cost-off">不更新平均成本</span>'
    : "更新平均成本";
  p.innerHTML = `
    <div class="erp-po-badge ${r.is_production ? "is-prod" : "is-test"} erp-po-badge-small">寫入：${esc(targetText(r.target_label, r.is_production))}　${costText}</div>
    <div class="erp-po-preview-head">預估單號（寫入當下才會確定）：收料 <strong class="mono">${esc(r.im_no)}</strong>　驗收 <strong class="mono">${esc(r.qm_no)}</strong>　進貨 <strong class="mono">${esc(r.rm_no)}</strong>　廠商 ${esc(r.vendor.id)} ${esc(r.vendor.name)}</div>
    <div class="erp-po-hint">單號照正航各類分開編號（收料 / 驗收 / 進貨各自當日最大號 + 1，三個號碼可能不同）；寫入時若撞號會自動重選。</div>
    <div class="erp-po-hint">收料 / 驗收日 ${ymd(r.today)}　<span class="erp-receive-preview-date">發票日期（進貨日）${ymd(r.rm_date)}</span>${rateInput(r)}　帳款月份 ${esc(r.terms.AccMonth)}　預計付款日 ${ymd(r.terms.PrepayDay)}　兌現日 ${ymd(r.terms.EncashDay)}</div>
    ${currencyBlock(r)}
    ${warn}
    <div class="erp-po-table-wrap"><table class="erp-po-table">
      <thead><tr><th>#</th><th>採購單</th><th>料號</th><th class="num">數量</th>${conv ? `<th class="num">原單價（${esc(r.po_currency)}）</th>` : ""}<th class="num">單價（${cur}）</th><th class="num">未稅金額（${cur}）</th><th class="num">稅額</th>${foreign ? '<th class="num">台幣金額</th>' : ""}</tr></thead>
      <tbody>${r.lines.map(l => `<tr><td>${l.row_no}</td><td class="mono">${esc(l.po_no)} #${l.po_row}</td><td class="mono">${esc(l.prod_id)}</td><td class="num">${fmt(l.qty)}</td>${conv ? `<td class="num">${fmt(l.src_price)}</td>` : ""}<td class="num">${fmt(l.price)}</td><td class="num">${fmt(l.amount)}</td><td class="num">${fmt(l.tax)}</td>${foreign ? `<td class="num">${fmt(l.ml_amount)}</td>` : ""}</tr>`).join("")}</tbody>
    </table></div>
    <div class="erp-po-totals">幣別 ${cur}　數量 ${fmt(r.totals.sum_qty)}　未稅 ${fmt(r.totals.sum_amount)}　稅 ${fmt(r.totals.sum_tax)}${Number(r.tax_type || 0) !== 0 ? "（免稅）" : ""}　<strong>${Number(r.tax_type || 0) !== 0 ? "合計" : "含稅合計"} ${cur} ${fmt(r.totals.total)}</strong>${foreign ? `　台幣 ${fmt(r.totals.local_total)}` : ""}</div>
    <div class="erp-po-actions"><span class="spacer"></span><button class="btn btn-primary" id="btn-erp-receive-create" type="button"${r.rate_required ? ' disabled title="請先輸入發票匯率"' : ""}>確認收料</button></div>`;
  p.hidden = false;
  el("btn-erp-receive-create").addEventListener("click", handleCreate);
  el("erp-receive-rate")?.addEventListener("change", event => {
    const n = Number(event.target.value);
    if (!Number.isFinite(n) || n <= 0) { showToast("發票匯率必須大於 0", { tone: "error" }); return; }
    _rate = String(n);
    _rateVendor = _picker.lockedGroup();
    void handlePreview();
  });
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
  if (_preview.mode && _preview.mode !== "local" && (body.exch_rate === null || _preview.rate_required)) {
    showToast("請先輸入發票匯率（大於 0）", { tone: "error" });
    return;
  }
  if (body.exch_rate !== null && Number(_preview.exch_rate) !== body.exch_rate) {
    showToast("匯率已變更，已重新預覽，請確認後再按「確認收料」");
    await handlePreview();
    return;
  }
  const tgt = targetText(_preview.target_label || _config?.label, _preview.is_production);
  const curLine = `幣別 ${_preview.currency || "NTD"}${_preview.mode && _preview.mode !== "local" ? `　發票匯率 ${_preview.exch_rate}` : ""}　${_preview.tax_label || ""}　合計 ${fmt(_preview.totals?.total)}`;
  if (!window.confirm(`寫入：${tgt}${_preview.is_production ? "\n⚠ 這是正式公司（正航正式帳），會直接寫入正式資料。" : ""}\n平均成本：${body.update_cost ? "更新" : "不更新"}\n確定要在「${tgt}」建立收料 / 驗收 / 進貨嗎？\n發票日期（進貨日）：${ymd(body.rm_date)}　帳款月份 ${_preview.terms?.AccMonth ?? ""}\n${curLine}\n寫入後無法從本系統修改或刪除。`)) return;
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
    result.innerHTML = `✅ 已寫入 ${esc(r.target_label)}：收料 <strong class="mono">${esc(r.im_no)}</strong>　驗收 <strong class="mono">${esc(r.qm_no)}</strong>　進貨 <strong class="mono">${esc(r.rm_no)}</strong>，合計 ${esc(r.currency || "NTD")} ${fmt(r.totals.total)}${r.closed_pos?.length ? `　已結案採購單：${esc(r.closed_pos.join("、"))}` : ""}${r.receipt_id ? `　<button type="button" class="btn btn-sm" data-rm-print="${esc(r.receipt_id)}">🖨 列印進貨單</button>` : ""}`;
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
        <td>${ok ? "成功" : `<span title="${esc(o.error)}">失敗：${esc(String(o.error).slice(0, 40))}</span>`}</td>
        <td>${ok && o.rm_no ? `<button type="button" class="btn btn-sm" data-rm-print="${esc(o.id)}" title="列印進貨單 ${esc(o.rm_no)}">🖨 列印</button>` : ""}</td></tr>`;
    }).join("") : '<tr><td colspan="7" class="erp-po-hint">尚無紀錄</td></tr>';
  } catch (error) {
    body.innerHTML = `<tr><td colspan="7" class="erp-po-hint">讀取失敗：${esc(error.message)}</td></tr>`;
  }
}

// 列印進貨單（正航進貨憑單版面 PDF）。自動送印已啟用就直接送到選定的印表機；
// 否則（預設）開 PDF 分頁，並提示要選哪台印表機（半張 → M111w 半張紙；超過 → M652 A4）。
async function printReceipt(id, btn) {
  if (btn) btn.disabled = true;
  // 先同步開分頁（避免被瀏覽器擋彈出視窗），之後再決定內容
  const win = window.open("", "_blank");
  try {
    const info = await apiJson(`/api/erp-receive/receipts/${encodeURIComponent(id)}/print-info`);
    const where = `${info.fits_half_page ? "半張（上半頁）" : `A4${info.page_count > 1 ? ` ${info.page_count} 頁` : ""}`} → 印表機「${info.printer}」`;
    if (info.warning) showToast(info.warning, { tone: "warning" });
    if (info.helper_enabled) {
      if (win) win.close();
      if (!isEditAuthenticated()) { openEditAuthModal(); return; }
      const r = await apiPost(`/api/erp-receive/receipts/${encodeURIComponent(id)}/print`, {});
      showToast(r.sent ? `已送印 ${info.rm_no}：${where}` : (r.message || "未送印"), { tone: r.sent ? "success" : "warning" });
      return;
    }
    if (win) win.location.href = info.pdf_url; else window.open(info.pdf_url, "_blank");
    showToast(`進貨單 ${info.rm_no}：${where}（自動送印未啟用，請在 PDF 視窗列印時選這台；縮放選「實際大小」）`, { tone: "info", duration: 12000 });
  } catch (error) {
    if (win) win.close();
    showToast(`列印失敗：${error.message}`, { tone: "error", duration: 10000 });
  } finally {
    if (btn) btn.disabled = false;
  }
}

function bindPrintButtons() {
  for (const id of ["erp-receive-history", "erp-receive-result"]) {
    el(id)?.addEventListener("click", event => {
      const btn = event.target.closest?.("[data-rm-print]");
      if (btn) void printReceipt(btn.dataset.rmPrint, btn);
    });
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
    // 已鎖定廠商時只抓該廠商（勾選跨搜尋保留，且不會被 300 筆上限截掉）
    fetchLines: async ({ q, group }) => apiJson(`/api/erp-receive/open-lines?q=${encodeURIComponent(q)}${group ? `&vendor=${encodeURIComponent(group)}` : ""}`),
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
  bindPrintButtons();
  el("erp-receive-date").addEventListener("change", () => { void handleDateChange(); });
  el("erp-receive-update-cost")?.addEventListener("change", () => { _costTouched = true; void handleDateChange(); });
  await loadConfig();
}
