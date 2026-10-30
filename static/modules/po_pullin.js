import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";

const STATUSES = ["未詢問", "已詢問", "可提前", "不能提前"];
const ROW_STATUS_LABELS = { pull_in: "需提前", on_time: "交期 OK", unordered: "未下單", unverified: "未驗證（正航失敗）" };

let _orders = [];
let _checked = new Set();
let _data = null; // /api/po-pullin/preview 回應
let _busy = false;
let _initialized = false;

function el(id) { return document.getElementById(id); }

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

// ── 提醒徽章（側邊 tab 上的未處理數）────────────────────────────────────────
export function setPullinBadge(count) {
  const badge = el("po-pullin-badge");
  if (!badge) return;
  badge.textContent = String(count);
  badge.style.display = count > 0 ? "inline" : "none";
}

export async function refreshPullinBadge() {
  try {
    const r = await apiJson("/api/po-pullin/badge");
    setPullinBadge(Number(r.unresolved) || 0);
  } catch (_) { /* 徽章失敗不影響其他功能 */ }
}

// ── 訂單選擇（沿用排程 / 已發料的既有端點）──────────────────────────────────
function renderOrders() {
  const box = el("pp-orders");
  if (!box) return;
  if (!_orders.length) { box.innerHTML = '<div class="erp-po-hint">尚無訂單</div>'; return; }
  box.innerHTML = _orders.map(o => `<label class="shortage-po-order">
    <input type="checkbox" data-order="${esc(o.id)}" ${_checked.has(o.id) ? "checked" : ""}>
    <span class="mono">${esc(o.code)}</span>
    <span>${esc(o.model)}</span>
    <span class="erp-po-hint">PO ${esc(o.po_number)}</span>
    <span class="erp-po-hint">出貨 ${esc(o.ship_date || o.delivery_date) || "—"}</span>
    <span class="erp-po-hint">${esc(o.status)}</span>
  </label>`).join("");
}

async function loadOrders() {
  try {
    const [open, done] = await Promise.all([
      apiJson("/api/schedule/rows"),
      apiJson("/api/schedule/completed"),
    ]);
    _orders = [...(open.rows || []), ...(done.rows || [])]
      .filter(o => ["pending", "merged", "dispatched", "completed"].includes(o.status));
  } catch (error) {
    _orders = [];
    showToast(`讀取訂單失敗：${error.message}`, { tone: "error" });
  }
  renderOrders();
}

// ── 結果呈現 ────────────────────────────────────────────────────────────────
function statusSelect(line) {
  return `<select data-act="status" data-key="${esc(line.line_key)}">${
    STATUSES.map(s => `<option value="${s}" ${s === line.status ? "selected" : ""}>${s}</option>`).join("")
  }</select>`;
}

function allocText(line) {
  const a = line.allocations || [];
  return a.length ? a.map(x => `${x.order_code || x.customer_po || "手動"}×${fmtQty(x.qty)}`).join("、") : "—";
}

function renderSupplier(s, idx) {
  const rows = s.lines.map(l => `<tr>
    <td class="mono">${esc(l.bill_no)}</td>
    <td class="num">${l.row_no}</td>
    <td class="mono">${esc(l.part_number)}</td>
    <td class="num">${fmtQty(l.qty)}</td>
    <td>${esc(l.po_date) || "未定"}</td>
    <td>${esc(l.requested_date)}</td>
    <td class="num shortage-po-short">${l.days_earlier}</td>
    <td class="erp-po-hint">${esc(allocText(l))}</td>
    <td>${statusSelect(l)}</td>
  </tr>`).join("");
  return `<div class="shortage-po-group" data-supplier-idx="${idx}">
    <div class="shortage-po-group-head">
      <strong>${esc(s.supplier_name)}</strong><span class="erp-po-hint mono">${esc(s.supplier_id)}</span>
      <span class="erp-po-hint">${s.lines.length} 行，未處理 ${s.unresolved}</span>
      <span class="spacer"></span>
      <button class="btn btn-secondary btn-sm" data-act="mark-asked" data-idx="${idx}" type="button">全部標為已詢問</button>
      <button class="btn btn-primary btn-sm" data-act="copy" data-idx="${idx}" type="button" ${s.draft_text ? "" : "disabled"}>複製詢問信</button>
    </div>
    <div class="erp-po-table-wrap"><table class="erp-po-table"><thead><tr>
      <th>採購單</th><th class="num">項次</th><th>料號</th><th class="num">數量</th><th>原交期</th><th>需要到貨日</th><th class="num">提前天數</th><th>為哪些訂單買</th><th>狀態</th>
    </tr></thead><tbody>${rows}</tbody></table></div>
    <textarea class="pp-draft" readonly rows="${Math.min(14, 6 + s.lines.length)}">${esc(s.draft_text || "（此供應商的項目都已有結論，無需再詢問）")}</textarea>
  </div>`;
}

// 需要到貨日的調整說明：後端給 date_shift_reason（遇假日提前 / 早於今天）；舊資料退回「改用今天」
function dateHint(p) {
  const text = p.date_shift_reason || (p.date_adjusted ? "改用今天" : "");
  return text ? ` <span class="shortage-po-adjusted" title="原推算 ${esc(p.original_required_date || "")}">（${esc(text)}）</span>` : "";
}

function renderResult() {
  const box = el("pp-result");
  if (!_data) { box.hidden = true; return; }
  const d = _data;
  box.hidden = false;
  const warn = (d.warnings || []).map(w => `<div class="erp-po-warn">${esc(w)}</div>`).join("");
  const sm = d.summary;
  const unordered = d.unordered.map(p => `<tr>
    <td class="mono">${esc(p.part_number)}</td><td>${esc(p.description)}</td>
    <td class="num">${fmtQty(p.shortage)}</td><td class="num shortage-po-short">${fmtQty(p.unordered_qty)}</td>
    <td>${esc(p.required_date)}${dateHint(p)}</td>
  </tr>`).join("");
  const partRows = d.parts.map(p => `<tr>
    <td class="mono">${esc(p.part_number)}</td><td>${esc(p.category_label)}</td>
    <td class="num">${fmtQty(p.need)}</td>
    <td class="num">${fmtQty(p.st)}${(p.merged_from || []).length ? ` <span class="erp-po-hint">(含 ${p.merged_from.map(m => `${esc(m.part)} ${fmtQty(m.stock)}`).join("、")})</span>` : ""}</td>
    <td class="num">${fmtQty(p.shortage)}</td>
    <td>${esc(p.required_date)}${dateHint(p)}</td>
    <td>${esc(ROW_STATUS_LABELS[p.status] || p.status)}</td>
  </tr>`).join("");
  box.innerHTML = `
    ${warn}
    <div class="erp-po-hint">新出貨日 ${esc(d.new_ship_date)}｜缺料料號 ${sm.shortage_parts}｜需提前 ${sm.pull_in_lines} 行（${sm.suppliers} 家）｜未下單 ${sm.unordered_parts}｜未處理 ${sm.unresolved}</div>
    <h4 class="erp-po-history-title">需請廠商提前（依供應商）</h4>
    ${d.suppliers.length ? d.suppliers.map(renderSupplier).join("") : '<div class="erp-po-hint">沒有需要提前的採購單</div>'}
    <h4 class="erp-po-history-title">未下單（缺料但沒有足夠的採購單）</h4>
    ${d.unordered.length ? `<div class="erp-po-table-wrap"><table class="erp-po-table"><thead><tr>
      <th>料號</th><th>品名</th><th class="num">缺料</th><th class="num">未下單量</th><th>需要到貨日</th></tr></thead><tbody>${unordered}</tbody></table></div>`
      : '<div class="erp-po-hint">無</div>'}
    ${(d.customer_supplied || []).length ? `<h4 class="erp-po-history-title">客供料（向客戶要料）</h4>
    <div class="erp-po-hint">料號 -TAB 結尾：不採購，不列入未下單 / 詢問信 / 徽章。</div>
    <div class="erp-po-table-wrap"><table class="erp-po-table"><thead><tr>
      <th>料號</th><th>品名</th><th class="num">需求</th><th class="num">倉庫庫存</th><th class="num">缺（向客戶要）</th><th>需要到貨日</th></tr></thead><tbody>${
      d.customer_supplied.map(c => `<tr><td class="mono">${esc(c.part_number)}</td><td>${esc(c.description)}</td>
        <td class="num">${fmtQty(c.need)}</td>
        <td class="num">${c.stock_known ? fmtQty(c.tab_stock) : `<span title="${esc(c.stock_error)}">未知</span>`}</td>
        <td class="num">${c.stock_known ? fmtQty(c.shortage) : "—"}</td>
        <td>${esc(c.required_date)}</td></tr>`).join("")}</tbody></table></div>` : ""}
    <h4 class="erp-po-history-title">缺料料號一覽</h4>
    <div class="erp-po-table-wrap"><table class="erp-po-table"><thead><tr>
      <th>料號</th><th>類別</th><th class="num">需求</th><th class="num">ST</th><th class="num">缺料</th><th>需要到貨日</th><th>結果</th>
    </tr></thead><tbody>${partRows || '<tr><td colspan="7" class="erp-po-hint">選取的訂單沒有缺料</td></tr>'}</tbody></table></div>`;
}

function readInputs() {
  return { ids: [..._checked], date: el("pp-new-date").value };
}

async function runPreview() {
  const { ids, date } = readInputs();
  if (!ids.length) { showToast("請先選擇訂單"); return; }
  if (!date) { showToast("請輸入新出貨日"); return; }
  _busy = true; updateButtons();
  try {
    _data = await apiJson(`/api/po-pullin/preview?order_ids=${ids.join(",")}&new_ship_date=${encodeURIComponent(date)}`);
  } catch (error) {
    _data = null;
    showToast(error.message, { tone: "error" });
  } finally {
    _busy = false;
    renderResult();
    updateButtons();
  }
}

async function runConfirm() {
  if (!requireEdit() || !_data) return;
  const { ids, date } = readInputs();
  const update = el("pp-update-ship").checked;
  const msg = update
    ? `確定保存提前清單，並把 ${ids.length} 張訂單的出貨日改為 ${date} 嗎？`
    : "確定保存提前清單嗎？（不會更動訂單出貨日）";
  if (!confirm(msg)) return;
  _busy = true; updateButtons();
  try {
    const r = await apiPost("/api/po-pullin/confirm", {
      order_ids: ids, new_ship_date: date, update_order_ship_date: update,
    });
    _data = r;
    renderResult();
    showToast(`已保存 ${r.saved_lines} 行${update ? `，更新 ${(r.updated_orders || []).length} 張訂單出貨日` : ""}`);
    await refreshPullinBadge();
    if (update) await loadOrders();
  } catch (error) {
    showToast(`保存失敗：${error.message}`, { tone: "error" });
  } finally {
    _busy = false;
    updateButtons();
  }
}

async function saveStatus(lines, status) {
  if (!requireEdit()) { renderResult(); return; }
  try {
    const r = await apiPost("/api/po-pullin/status", { lines, status });
    setPullinBadge(Number(r.unresolved) || 0);
    await runPreview(); // 重新計算並帶出已保存的狀態
  } catch (error) {
    showToast(`更新狀態失敗：${error.message}`, { tone: "error" });
    renderResult();
  }
}

function findLine(key) {
  for (const s of _data?.suppliers || []) {
    const l = s.lines.find(x => x.line_key === key);
    if (l) return l;
  }
  return null;
}

async function copyDraft(idx) {
  const s = _data?.suppliers?.[idx];
  if (!s?.draft_text) return;
  try {
    await navigator.clipboard.writeText(s.draft_text);
  } catch (_) {
    const ta = document.querySelector(`[data-supplier-idx="${idx}"] .pp-draft`);
    ta?.focus(); ta?.select(); document.execCommand("copy");
  }
  showToast(`已複製「${s.supplier_name}」的詢問信（僅複製，不會寄出）`);
}

function updateButtons() {
  el("btn-pp-preview").disabled = _busy;
  el("btn-pp-confirm").disabled = _busy || !_data;
}

export async function initPoPullin() {
  if (_initialized) return;
  _initialized = true;
  el("pp-orders")?.addEventListener("change", ev => {
    const cb = ev.target.closest("[data-order]");
    if (!cb) return;
    const id = Number(cb.dataset.order);
    if (cb.checked) _checked.add(id); else _checked.delete(id);
  });
  el("btn-pp-preview")?.addEventListener("click", runPreview);
  el("btn-pp-confirm")?.addEventListener("click", runConfirm);
  el("pp-result")?.addEventListener("click", ev => {
    const btn = ev.target.closest("button[data-act]");
    if (!btn) return;
    const idx = Number(btn.dataset.idx);
    if (btn.dataset.act === "copy") void copyDraft(idx);
    if (btn.dataset.act === "mark-asked") void saveStatus(_data.suppliers[idx].lines, "已詢問");
  });
  el("pp-result")?.addEventListener("change", ev => {
    const sel = ev.target.closest('select[data-act="status"]');
    if (!sel) return;
    const line = findLine(sel.dataset.key);
    if (line) void saveStatus([line], sel.value);
  });
  updateButtons();
  void refreshPullinBadge();
}

export async function onPoPullinTabActivated() {
  await loadOrders();
  await refreshPullinBadge();
}
