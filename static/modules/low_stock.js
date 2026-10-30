import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";
import { prefillPurchaseOrder } from "/static/modules/erp_purchase.js";

const REFRESH_MS = 5 * 60 * 1000;
const HIDE_ON_ORDER_KEY = "low-stock-hide-on-order";

let _rows = [];
let _settings = {};
let _initialized = false;
let _busy = false;

function fmtNum(value, digits = 1) {
  if (value === null || value === undefined || value === "") return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return Number.isInteger(n) ? String(n) : n.toFixed(digits);
}

function requireEdit() {
  if (isEditAuthenticated()) return true;
  showToast("目前為唯讀模式，請先登入編輯。");
  openEditAuthModal();
  return false;
}

// ── 徽章與側邊欄提醒（只讀 /alerts，不連 ERP） ─────────────────────────────
export async function refreshLowStockAlerts() {
  let data = null;
  try {
    data = await apiJson("/api/low-stock/alerts");
  } catch (_) {
    return;
  }
  const count = Number(data?.count || 0);
  setMainFileHint(data);
  const badge = document.getElementById("low-stock-badge");
  if (badge) {
    badge.textContent = String(count);
    badge.style.display = count > 0 ? "inline" : "none";
  }
  const notice = document.getElementById("low-stock-notice");
  const btn = document.getElementById("low-stock-notice-btn");
  const meta = document.getElementById("low-stock-notice-meta");
  if (notice) notice.style.display = count > 0 ? "" : "none";
  if (btn) btn.textContent = `⚠ ${count} 個常用料號低於門檻`;
  if (meta) {
    const alerts = data.alerts || [];
    const worst = alerts.slice(0, 3).map(a => a.part_number).join("、");
    const withOrder = alerts.filter(a => Number(a.on_order_qty) > 0).length;
    const orderText = withOrder > 0 ? `；其中 ${withOrder} 個已有在途量` : "";
    meta.textContent = worst ? `最急：${worst}${orderText}` : "";
  }
}

// 主檔讀不到、未套用料號過濾時，在狀態列下方顯示一行提示
function setMainFileHint(data) {
  const el = document.getElementById("low-stock-main-hint");
  if (!el) return;
  const show = data && data.main_file_filter_applied === false;
  el.textContent = show ? (data.main_file_note || "") : "";
  el.style.display = show ? "" : "none";
}

export async function onLowStockTabActivated() {
  await refreshLowStock();
}

// ── 清單 ────────────────────────────────────────────────────────────────────
export async function refreshLowStock() {
  const status = document.getElementById("low-stock-status");
  try {
    const data = await apiJson("/api/low-stock/watchlist");
    _rows = data.rows || [];
    _settings = data.settings || {};
    setMainFileHint(data);
    renderSettings();
    renderTable();
    if (status) {
      const suggested = data.last_suggested_at ? `；上次建議：${data.last_suggested_at}` : "；尚未執行過建議";
      const loaded = data.st_inventory_loaded_at ? `；ST 庫存更新：${String(data.st_inventory_loaded_at).slice(0, 16).replace("T", " ")}` : "";
      status.className = "file-status" + (data.low_count > 0 ? " warn" : " ok");
      status.textContent = `共 ${_rows.length} 個料號，${data.low_count} 個低於門檻${suggested}${loaded}`;
    }
  } catch (error) {
    if (status) {
      status.className = "file-status warn";
      status.textContent = `讀取失敗：${error.message}`;
    }
  }
  void refreshLowStockAlerts();
}

function renderSettings() {
  const map = {
    "low-stock-set-lookback": "lookback_days",
    "low-stock-set-suggest-lookback": "suggest_lookback_days",
    "low-stock-set-min-bills": "min_bills",
    "low-stock-set-safety": "safety_factor",
    "low-stock-set-min-cover": "min_cover_days",
    "low-stock-set-default-lead": "default_lead_days",
    "low-stock-set-actual-ratio": "actual_lead_ratio",
    "low-stock-set-actual-gap": "actual_lead_min_gap_days",
    "low-stock-set-on-order-overdue": "on_order_max_overdue_days",
  };
  const panel = document.getElementById("low-stock-settings-panel");
  if (panel && !panel.hidden) return; // 編輯中不覆蓋輸入
  Object.entries(map).forEach(([id, key]) => {
    const el = document.getElementById(id);
    if (el && _settings[key] !== undefined) el.value = _settings[key];
  });
}

function delayHtml(days) {
  if (days === null || days === undefined) return "—";
  const n = Number(days);
  if (n > 0) return `<span class="low-stock-delay-late">+${fmtNum(n)} 天 延遲</span>`;
  if (n < 0) return `<span class="low-stock-delay-early">提早 ${fmtNum(-n)} 天</span>`;
  return "準時";
}

function onOrderHtml(r) {
  const qty = Number(r.on_order_qty || 0);
  if (qty > 0) {
    const date = r.next_expected_date ? `，預計 ${esc(r.next_expected_date)}` : "";
    return `<span class="low-stock-on-order-info">在途 ${fmtNum(qty)}${date}</span>`;
  }
  if (r.on_order_note) return `<span class="low-stock-hint" title="${esc(r.on_order_note)}">${esc(r.on_order_note)}</span>`;
  return "—";
}

function statusHtml(status) {
  if (status === "on_order") return '<span class="low-stock-status on_order">已下單</span>';
  if (status === "low") return '<span class="low-stock-status low">低於門檻</span>';
  if (status === "disabled") return '<span class="low-stock-status disabled">停用</span>';
  return '<span class="low-stock-status ok">正常</span>';
}

function renderTable() {
  const body = document.getElementById("low-stock-table-body");
  if (!body) return;
  const onlyLow = Boolean(document.getElementById("low-stock-only-low")?.checked);
  const hideOnOrder = Boolean(document.getElementById("low-stock-hide-on-order")?.checked);
  const rows = _rows.filter(r => (!onlyLow || r.status === "low") && !(hideOnOrder && r.status === "on_order"));
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="15" class="low-stock-empty">${onlyLow ? "目前沒有低於門檻的料號。" : "清單是空的，請按「重新建議」或手動新增料號。"}</td></tr>`;
    return;
  }
  body.innerHTML = rows.map(r => {
    const part = esc(r.part_number);
    const manualThreshold = r.threshold_source === "manual" ? ' <span class="low-stock-tag">手動</span>' : "";
    return `<tr class="low-stock-row ${r.status}" data-part="${part}">
      <td class="mono">${part}</td>
      <td>${esc(r.name)}</td>
      <td class="num">${fmtNum(r.qty)}</td>
      <td>${onOrderHtml(r)}</td>
      <td class="num">${fmtNum(r.avg_monthly_usage)}</td>
      <td>${fmtNum(r.lead_time_days)} <span class="low-stock-tag src-${esc(r.lead_time_source)}">${esc(r.lead_time_label)}</span>
        <button class="low-stock-link" data-act="edit-lead" title="修改交期（留空=恢復自動）">編輯</button></td>
      <td class="num">${fmtNum(r.planned_lead_days)}</td>
      <td class="num">${fmtNum(r.actual_lead_days)}</td>
      <td>${delayHtml(r.delay_days)}</td>
      <td>${fmtNum(r.threshold)}${manualThreshold}
        <button class="low-stock-link" data-act="edit-threshold" title="修改門檻（留空=恢復自動）">編輯</button></td>
      <td class="num">${fmtNum(r.days_of_cover)}</td>
      <td>${statusHtml(r.status)}</td>
      <td><input type="checkbox" data-act="toggle" ${r.enabled ? "checked" : ""} aria-label="啟用 ${part}"></td>
      <td>${esc(r.note)} <button class="low-stock-link" data-act="edit-note">編輯</button></td>
      <td><button class="btn btn-secondary btn-sm" data-act="open-po" title="到「開採購單」頁籤，帶入此料號與建議數量">開採購單</button>
        <button class="btn btn-danger btn-sm" data-act="remove">移除</button></td>
    </tr>`;
  }).join("");
}

async function saveField(part, fields, okMessage) {
  if (!requireEdit()) return false;
  try {
    await apiPost("/api/low-stock/items/update", { part_number: part, ...fields });
    if (okMessage) showToast(okMessage, { tone: "success" });
    await refreshLowStock();
    return true;
  } catch (error) {
    showToast(`儲存失敗：${error.message}`, { tone: "error" });
    await refreshLowStock();
    return false;
  }
}

function promptNumber(message, current) {
  const raw = window.prompt(message, current === null || current === undefined ? "" : String(current));
  if (raw === null) return undefined; // 取消
  const text = raw.trim();
  if (text === "") return null; // 恢復自動
  const n = Number(text);
  if (!Number.isFinite(n)) {
    showToast("請輸入數字", { tone: "error" });
    return undefined;
  }
  return n;
}

async function onTableClick(event) {
  const target = event.target.closest("[data-act]");
  const tr = event.target.closest("tr[data-part]");
  if (!target || !tr) return;
  const part = tr.dataset.part;
  const row = _rows.find(r => r.part_number === part);
  if (!row) return;
  const act = target.dataset.act;
  if (act === "toggle") return; // 由 change 事件處理
  if (act === "open-po") {
    // 建議量 = max(門檻 - 現有量 - 在途量（至少 1）, MOQ)；後端已算好，缺值時在這裡補算
    const gap = row.threshold === null || row.threshold === undefined
      ? 1
      : Math.max(1, Math.ceil(Number(row.threshold) - Number(row.qty || 0) - Number(row.on_order_qty || 0)));
    const moq = Number(row.moq) > 0 ? Number(row.moq) : 0;
    const qty = row.suggested_po_qty ?? Math.max(gap, Math.ceil(moq));
    document.querySelector('.tab-btn[data-tab="erp-po-tab"]')?.click();
    prefillPurchaseOrder(part, qty, {
      need: gap, moq, moq_source: row.moq_source || "",
      receipt: { qty: row.last_receipt_qty, date: row.last_receipt_date },
    });
    return;
  }
  if (act === "edit-threshold") {
    if (!requireEdit()) return;
    const value = promptNumber(`${part} 門檻（留空 = 恢復自動建議）`, row.threshold);
    if (value !== undefined) await saveField(part, { threshold_qty: value }, value === null ? "已恢復自動門檻" : "門檻已更新");
  } else if (act === "edit-lead") {
    if (!requireEdit()) return;
    const value = promptNumber(`${part} 交期天數（留空 = 恢復自動）`, row.lead_time_days);
    if (value !== undefined) await saveField(part, { lead_time_days: value }, value === null ? "已恢復自動交期" : "交期已更新");
  } else if (act === "edit-note") {
    if (!requireEdit()) return;
    const note = window.prompt(`${part} 備註`, row.note || "");
    if (note !== null) await saveField(part, { note }, "備註已更新");
  } else if (act === "remove") {
    if (!requireEdit()) return;
    if (!window.confirm(`確定將 ${part} 從低庫存關注清單移除？（之後「重新建議」不會自動加回）`)) return;
    try {
      await apiPost("/api/low-stock/items/remove", { part_number: part });
      showToast("已移除", { tone: "success" });
    } catch (error) {
      showToast(`移除失敗：${error.message}`, { tone: "error" });
    }
    await refreshLowStock();
  }
}

async function onTableChange(event) {
  const target = event.target.closest('[data-act="toggle"]');
  const tr = event.target.closest("tr[data-part]");
  if (!target || !tr) return;
  const ok = await saveField(tr.dataset.part, { enabled: target.checked });
  if (!ok) target.checked = !target.checked;
}

async function handleSuggest() {
  if (_busy || !requireEdit()) return;
  _busy = true;
  const btn = document.getElementById("btn-low-stock-suggest");
  if (btn) { btn.disabled = true; btn.textContent = "建議計算中..."; }
  try {
    const s = await apiPost("/api/low-stock/suggest", {});
    if (s.main_file_filter_applied === false && s.main_file_note) showToast(s.main_file_note, { duration: 6000 });
    showToast(
      `建議完成：候選 ${s.candidates} 個，新增 ${s.added}、更新 ${s.updated}；` +
      `保留手動門檻 ${s.kept_manual_threshold}、手動交期 ${s.kept_manual_lead}；清單共 ${s.total_rows} 筆`,
      { tone: "success", duration: 6000 },
    );
    await refreshLowStock();
  } catch (error) {
    showToast(error.message, { tone: "error", sticky: true });
  } finally {
    _busy = false;
    if (btn) { btn.disabled = !isEditAuthenticated(); btn.textContent = "重新建議"; }
  }
}

async function handleAdd(event) {
  event.preventDefault();
  if (!requireEdit()) return;
  const part = document.getElementById("low-stock-add-part").value.trim();
  if (!part) return;
  const numOrNull = id => {
    const v = document.getElementById(id).value.trim();
    return v === "" ? null : Number(v);
  };
  try {
    await apiPost("/api/low-stock/items", {
      part_number: part,
      threshold_qty: numOrNull("low-stock-add-threshold"),
      lead_time_days: numOrNull("low-stock-add-lead"),
      note: document.getElementById("low-stock-add-note").value.trim(),
    });
    event.target.reset();
    showToast(`已新增 ${part.toUpperCase()}`, { tone: "success" });
    await refreshLowStock();
  } catch (error) {
    showToast(`新增失敗：${error.message}`, { tone: "error" });
  }
}

async function handleSaveSettings() {
  if (!requireEdit()) return;
  const val = id => Number(document.getElementById(id).value);
  try {
    _settings = await apiPost("/api/low-stock/settings", {
      lookback_days: val("low-stock-set-lookback"),
      suggest_lookback_days: val("low-stock-set-suggest-lookback"),
      min_bills: val("low-stock-set-min-bills"),
      safety_factor: val("low-stock-set-safety"),
      min_cover_days: val("low-stock-set-min-cover"),
      default_lead_days: val("low-stock-set-default-lead"),
      actual_lead_ratio: val("low-stock-set-actual-ratio"),
      actual_lead_min_gap_days: val("low-stock-set-actual-gap"),
      on_order_max_overdue_days: val("low-stock-set-on-order-overdue"),
    });
    showToast("設定已儲存，自動門檻已重算", { tone: "success" });
    const panel = document.getElementById("low-stock-settings-panel");
    if (panel) panel.hidden = true;
    await refreshLowStock();
  } catch (error) {
    showToast(`儲存設定失敗：${error.message}`, { tone: "error" });
  }
}

export async function initLowStock({ autoLoad = true } = {}) {
  if (!_initialized) {
    document.getElementById("btn-low-stock-suggest")?.addEventListener("click", handleSuggest);
    document.getElementById("low-stock-add-form")?.addEventListener("submit", handleAdd);
    document.getElementById("btn-low-stock-save-settings")?.addEventListener("click", handleSaveSettings);
    document.getElementById("btn-low-stock-settings-toggle")?.addEventListener("click", () => {
      const panel = document.getElementById("low-stock-settings-panel");
      if (!panel) return;
      panel.hidden = !panel.hidden;
      if (panel.hidden) renderSettings();
    });
    document.getElementById("low-stock-only-low")?.addEventListener("change", renderTable);
    const hideOnOrder = document.getElementById("low-stock-hide-on-order");
    if (hideOnOrder) {
      let saved = null;
      try { saved = localStorage.getItem(HIDE_ON_ORDER_KEY); } catch (_) { /* 無 localStorage 則維持預設勾選 */ }
      if (saved !== null) hideOnOrder.checked = saved === "1";
      hideOnOrder.addEventListener("change", () => {
        try { localStorage.setItem(HIDE_ON_ORDER_KEY, hideOnOrder.checked ? "1" : "0"); } catch (_) { /* ignore */ }
        renderTable();
      });
    }
    const body = document.getElementById("low-stock-table-body");
    body?.addEventListener("click", onTableClick);
    body?.addEventListener("change", onTableChange);
    document.getElementById("low-stock-notice-btn")?.addEventListener("click", () => {
      document.querySelector('.tab-btn[data-tab="low-stock-tab"]')?.click();
    });
    // ST 庫存同步（ERP 或 Excel）後立即重算徽章
    window.addEventListener("st-inventory-synced", () => { void refreshLowStockAlerts(); });
    setInterval(() => { void refreshLowStockAlerts(); }, REFRESH_MS);
    _initialized = true;
  }
  if (autoLoad) await refreshLowStock();
  else await refreshLowStockAlerts();
}
