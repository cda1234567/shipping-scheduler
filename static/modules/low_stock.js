// 低庫存提醒（v2026.11.11.7）：用廠商交期設定判斷、新可用量、推定廠商、待確認區。
// 畫面只讀 /api/low-stock/plan（本機快照，不連正航）；「重新抓正航」才會唯讀查正航。
import { apiJson, apiPost, esc, showToast } from "/static/modules/api.js";
import { isEditAuthenticated, openEditAuthModal } from "/static/modules/edit_auth.js";

const REFRESH_MS = 5 * 60 * 1000;
const HIDE_ORDERED_KEY = "low-stock-plan-hide-ordered";

let _plan = null;
let _initialized = false;
let _busy = false;
const _openDetail = new Set();

function el(id) {
  return document.getElementById(id);
}

function fmtNum(value, digits = 1) {
  if (value === null || value === undefined || value === "") return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return Number.isInteger(n) ? n.toLocaleString("en-US") : n.toLocaleString("en-US", { maximumFractionDigits: digits });
}

// '2026-11-11 23:50:01' → '11/11 23:50'（R-J4：日期寫 10/07）
function fmtTime(text) {
  const m = String(text || "").match(/^\d{4}-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/);
  return m ? `${m[1]}/${m[2]} ${m[3]}:${m[4]}` : (text || "");
}

function requireEdit() {
  if (isEditAuthenticated()) return true;
  showToast("目前為唯讀模式，請先登入編輯。");
  openEditAuthModal();
  return false;
}

// ── 徽章與側邊欄提醒（主清單列數，只讀本機） ──────────────────────────────
export async function refreshLowStockAlerts() {
  let count = 0;
  let top = [];
  try {
    if (_plan) {
      count = (_plan.main || []).length;
      top = (_plan.main || []).slice(0, 3).map(r => r.part);
    } else {
      count = Number((await apiJson("/api/low-stock/plan/count"))?.count || 0);
    }
  } catch (_) {
    return;
  }
  const badge = el("low-stock-badge");
  if (badge) {
    badge.textContent = String(count);
    badge.style.display = count > 0 ? "inline" : "none";
  }
  const notice = el("low-stock-notice");
  if (notice) notice.style.display = count > 0 ? "" : "none";
  const btn = el("low-stock-notice-btn");
  if (btn) btn.textContent = `⚠ 低庫存 ${count} 顆`;
  const meta = el("low-stock-notice-meta");
  if (meta) meta.textContent = top.length ? `最急：${top.join("、")}` : "";
}

export async function onLowStockTabActivated() {
  await refreshLowStock();
}

// ── 清單 ────────────────────────────────────────────────────────────────────
export async function refreshLowStock() {
  const status = el("low-stock-status");
  try {
    _plan = await apiJson("/api/low-stock/plan");
    renderStatus();
    renderSuppliers();
    renderTables();
  } catch (error) {
    if (status) {
      status.className = "file-status warn";
      status.textContent = `讀取失敗：${error.message}`;
    }
  }
  void refreshLowStockAlerts();
}

function renderStatus() {
  const status = el("low-stock-status");
  const hint = el("low-stock-main-hint");
  if (!_plan) return;
  const c = _plan.counts || {};
  const cut = _plan.cutoff || {};
  if (status) {
    const parts = [
      `母體 ${c.population || 0}`,
      `主清單 ${c.main || 0}`,
      `待確認 ${c.check || 0}`,
      `可隨時下單 ${c.anytime || 0}`,
      `有交期且撐得到 ${c.ok_with_lead || 0}`,
    ];
    const cutText = cut.code ? `待調撥截止批次 ${cut.code}（${cut.text || cut.source}）` : "待調撥截止批次：找不到";
    const time = _plan.refreshed_at ? `資料時間 ${fmtTime(_plan.refreshed_at)}` : "尚未抓過正航，請按「重新抓正航」";
    status.className = "file-status" + ((c.main || 0) > 0 ? " warn" : " ok");
    status.textContent = `${parts.join("、")}；${cutText}；${time}`;
  }
  if (hint) {
    const lines = [];
    if (_plan.error) lines.push(_plan.error);
    (_plan.warnings || []).forEach(w => lines.push(w));
    const noBom = _plan.no_bom_orders || [];
    if (noBom.length) {
      lines.push(`缺 BOM 的已發料訂單（資料待確認）：${noBom.map(o => `${o.code || "—"} ${o.model}`).join("、")}`);
    }
    hint.textContent = lines.join("；");
    hint.style.display = lines.length ? "" : "none";
  }
}

function supplierHtml(r) {
  if (!r.supplier_id) return '<span class="low-stock-hint">無廠商</span>';
  const kind = r.supplier_kind === "inferred"
    ? ' <span class="low-stock-tag" title="正航沒設主要廠商，用最近一張進貨的廠商">推定廠商</span>'
    : (r.supplier_kind === "manual" ? ' <span class="low-stock-tag src-manual">手動</span>' : "");
  return `<span class="low-stock-supplier" title="${esc(r.supplier_id)}">${esc(r.supplier_name)}</span>${kind}`;
}

function leadHtml(r) {
  const ref = Number(r.perdays) > 0 ? `<div class="low-stock-hint">參考 PerDays ${fmtNum(r.perdays)}（僅參考，非交期）</div>` : "";
  if (r.anytime) return `<span class="low-stock-tag">可隨時下單</span><div class="low-stock-hint">${esc(r.lead_text)}</div>${ref}`;
  if (r.lead_days === null || r.lead_days === undefined) return `<span class="low-stock-hint">無交期</span>${ref}`;
  return `${fmtNum(r.lead_days)} 天<div class="low-stock-hint">${esc(r.lead_text)}</div>${ref}`;
}

function reasonHtml(r) {
  const reasons = (r.reasons || []).map(t => `<div>${esc(t)}</div>`).join("");
  const tags = (r.tags || []).map(t => {
    const cls = t.startsWith("萬晉/崧嵐") ? "low-stock-priority-tag" : (t.startsWith("已下單") ? "low-stock-tag src-manual" : "low-stock-tag");
    return `<span class="${cls}">${esc(t)}</span>`;
  }).join(" ");
  return reasons + (tags ? `<div class="low-stock-tags">${tags}</div>` : "");
}

function settingHtml(r) {
  const lead = r.part_lead_days === null || r.part_lead_days === undefined ? "" : String(r.part_lead_days);
  return `<input type="number" class="low-stock-lead-input" data-act="part-lead" min="0" max="730" step="1" value="${esc(lead)}" placeholder="交期" title="料號覆蓋交期（天，空白 = 用廠商交期）">
    <label class="low-stock-filter" title="可隨時下單：不需要交期"><input type="checkbox" data-act="part-anytime" ${r.part_anytime ? "checked" : ""}> 隨時</label>
    <button class="low-stock-link" data-act="part-supplier" title="改廠商（空白 = 自動判斷）">改廠商</button>`;
}

function detailHtml(r) {
  const rows = (r.unissued_detail || []).map(d => `<tr>
      <td class="mono">${esc(d.po)}</td><td>${esc((d.codes || []).join("、"))}</td>
      <td class="num">${fmtNum(d.need)}</td><td class="num">${d.bom_need === null ? "缺 BOM" : fmtNum(d.bom_need)}</td>
      <td class="num">${fmtNum(d.issued)}</td><td class="num">${fmtNum(d.unissued)}</td></tr>`).join("");
  const table = rows
    ? `<table class="low-stock-detail-table"><thead><tr><th>PO</th><th>批次</th><th class="num">需求（派工紀錄）</th><th class="num">BOM 版</th><th class="num">015 已領</th><th class="num">未領</th></tr></thead><tbody>${rows}</tbody></table>`
    : '<div class="low-stock-hint">沒有已發料未領的需求</div>';
  return `<div class="low-stock-detail-sum">ST ${fmtNum(r.st)}、待調撥 ${fmtNum(r.pending)}${r.pending_over_st ? `（原 ${fmtNum(r.pending_raw)}，超過 ST）` : ""}、自由 ST ${fmtNum(r.free_st)}、015 ${fmtNum(r.gl015)}、未領需求 ${fmtNum(r.unissued)}
    → 可用 ${fmtNum(r.available)}（不含自由 ST：${fmtNum(r.available_without_free_st)}）</div>${table}`;
}

function rowHtml(r) {
  const part = esc(r.part);
  const cls = ["low-stock-row", r.priority_supplier ? "priority-supplier" : "", r.ordered ? "on_order" : (r.area === "main" ? "low" : "")].filter(Boolean).join(" ");
  const open = _openDetail.has(r.part);
  const main = `<tr class="${cls}" data-part="${part}">
    <td class="mono">${part}<div class="low-stock-hint">${esc(r.name || "")}</div></td>
    <td>${supplierHtml(r)}</td>
    <td class="num"><button class="low-stock-link" data-act="detail" title="看 ST、待調撥、015、各 PO 未領">${fmtNum(r.available)}</button></td>
    <td class="num">${fmtNum(r.free_st)}</td>
    <td class="num">${fmtNum(r.need)}</td>
    <td class="num">${fmtNum(r.daily, 2)}</td>
    <td class="num">${fmtNum(r.cover_days)}</td>
    <td>${leadHtml(r)}</td>
    <td>${reasonHtml(r)}</td>
    <td class="low-stock-setting">${settingHtml(r)}</td>
  </tr>`;
  return open ? main + `<tr class="low-stock-detail-row" data-detail="${part}"><td colspan="10">${detailHtml(r)}</td></tr>` : main;
}

function renderTables() {
  if (!_plan) return;
  const hideOrdered = Boolean(el("low-stock-hide-ordered")?.checked);
  const keep = r => !(hideOrdered && r.ordered);
  const main = (_plan.main || []).filter(keep);
  const check = (_plan.check || []).filter(keep);
  const mainBody = el("low-stock-main-body");
  if (mainBody) {
    mainBody.innerHTML = main.length
      ? main.map(rowHtml).join("")
      : `<tr><td colspan="10" class="low-stock-empty">${_plan.refreshed_at ? "主清單沒有料號。" : "尚未抓過正航，請按「重新抓正航」。"}</td></tr>`;
  }
  const checkBody = el("low-stock-check-body");
  if (checkBody) {
    checkBody.innerHTML = check.length ? check.map(rowHtml).join("") : '<tr><td colspan="10" class="low-stock-empty">沒有待確認的料號。</td></tr>';
  }
  const summary = el("low-stock-check-summary");
  if (summary) summary.textContent = `資料待確認 ${(_plan.check || []).length} 顆（無廠商交期，展開看）`;
}

function renderSuppliers() {
  const body = el("low-stock-supplier-body");
  if (!body || !_plan) return;
  const rows = _plan.suppliers || [];
  const noSupplier = Number(_plan.no_supplier_count || 0);
  body.innerHTML = rows.map(s => {
    const lead = s.is_default || s.lead_days === null || s.lead_days === undefined ? "" : String(s.lead_days);
    const now = s.anytime ? "可隨時下單" : (s.lead_days === null || s.lead_days === undefined ? "無交期" : `${s.lead_days} 天`);
    return `<tr data-supplier="${esc(s.supplier_id)}">
      <td class="mono">${esc(s.supplier_id)}</td><td>${esc(s.supplier_name)}</td><td class="num">${fmtNum(s.part_count)}</td>
      <td>${esc(now)}<div class="low-stock-hint">${esc(s.text)}</div></td>
      <td><input type="number" class="low-stock-lead-input" data-act="sup-lead" min="0" max="730" step="1" value="${esc(lead)}" placeholder="天"></td>
      <td><input type="checkbox" data-act="sup-anytime" ${!s.is_default && s.anytime ? "checked" : ""}></td>
      <td><button class="btn btn-secondary btn-sm" data-act="sup-save">存</button></td>
    </tr>`;
  }).join("") + (noSupplier ? `<tr><td colspan="7" class="low-stock-hint">無廠商 ${noSupplier} 顆：只能在料號列設定交期或改廠商。</td></tr>` : "");
}

// ── 編輯 ───────────────────────────────────────────────────────────────────
function findRow(part) {
  return [...(_plan?.main || []), ...(_plan?.check || [])].find(r => r.part === part);
}

async function savePart(part, fields, okMessage) {
  if (!requireEdit()) { renderTables(); return; }
  try {
    await apiPost("/api/low-stock/lead-settings/part", { part, ...fields });
    showToast(okMessage, { tone: "success" });
  } catch (error) {
    showToast(`儲存失敗：${error.message}`, { tone: "error" });
  }
  await refreshLowStock();
}

async function onTableClick(event) {
  const target = event.target.closest("[data-act]");
  const tr = event.target.closest("tr[data-part]");
  if (!target || !tr) return;
  const part = tr.dataset.part;
  const act = target.dataset.act;
  if (act === "detail") {
    if (_openDetail.has(part)) _openDetail.delete(part); else _openDetail.add(part);
    renderTables();
  } else if (act === "part-supplier") {
    if (!requireEdit()) return;
    const row = findRow(part);
    const raw = window.prompt(`${part} 廠商代碼（例如 P0175；空白 = 自動判斷）`, row?.supplier_override || "");
    if (raw === null) return;
    await savePart(part, { supplier_override: raw.trim() }, raw.trim() ? "廠商已改" : "已改回自動判斷");
  }
}

async function onTableChange(event) {
  const target = event.target.closest("[data-act]");
  const tr = event.target.closest("tr[data-part]");
  if (!target || !tr) return;
  const part = tr.dataset.part;
  if (target.dataset.act === "part-lead") {
    const text = target.value.trim();
    await savePart(part, { lead_days: text === "" ? null : text }, text === "" ? "已清除料號交期" : "料號交期已存");
  } else if (target.dataset.act === "part-anytime") {
    await savePart(part, { anytime: target.checked }, target.checked ? "已設可隨時下單" : "已取消可隨時下單");
  }
}

async function onSupplierClick(event) {
  const target = event.target.closest('[data-act="sup-save"]');
  const tr = event.target.closest("tr[data-supplier]");
  if (!target || !tr || !requireEdit()) return;
  const text = tr.querySelector('[data-act="sup-lead"]')?.value.trim() || "";
  const anytime = Boolean(tr.querySelector('[data-act="sup-anytime"]')?.checked);
  try {
    await apiPost("/api/low-stock/lead-settings/supplier", {
      supplier_id: tr.dataset.supplier, lead_days: text === "" ? null : text, anytime,
    });
    showToast(text === "" && !anytime ? "已回到預設" : "廠商交期已存", { tone: "success" });
  } catch (error) {
    showToast(`儲存失敗：${error.message}`, { tone: "error" });
  }
  await refreshLowStock();
}

async function handleRefresh() {
  if (_busy || !requireEdit()) return;
  _busy = true;
  const btn = el("btn-low-stock-refresh");
  if (btn) { btn.disabled = true; btn.textContent = "抓正航中..."; }
  try {
    const res = await apiPost("/api/low-stock/plan/refresh", {});
    showToast(`已更新，主檔料號 ${res.parts} 顆`, { tone: "success" });
  } catch (error) {
    showToast(error.message, { tone: "error", sticky: true });
  } finally {
    _busy = false;
    if (btn) { btn.disabled = false; btn.textContent = "重新抓正航"; }
  }
  await refreshLowStock();
}

export async function initLowStock({ autoLoad = true } = {}) {
  if (!_initialized) {
    el("btn-low-stock-refresh")?.addEventListener("click", handleRefresh);
    el("btn-low-stock-suppliers-toggle")?.addEventListener("click", () => {
      const panel = el("low-stock-supplier-panel");
      if (panel) panel.hidden = !panel.hidden;
    });
    const hide = el("low-stock-hide-ordered");
    if (hide) {
      try { hide.checked = localStorage.getItem(HIDE_ORDERED_KEY) === "1"; } catch (_) { /* 無 localStorage：預設不勾 */ }
      hide.addEventListener("change", () => {
        try { localStorage.setItem(HIDE_ORDERED_KEY, hide.checked ? "1" : "0"); } catch (_) { /* ignore */ }
        renderTables();
      });
    }
    for (const id of ["low-stock-main-body", "low-stock-check-body"]) {
      el(id)?.addEventListener("click", onTableClick);
      el(id)?.addEventListener("change", onTableChange);
    }
    el("low-stock-supplier-body")?.addEventListener("click", onSupplierClick);
    el("low-stock-notice-btn")?.addEventListener("click", () => {
      document.querySelector('.tab-btn[data-tab="low-stock-tab"]')?.click();
    });
    // ST 庫存同步後（會一併重算快照）更新徽章
    window.addEventListener("st-inventory-synced", () => { _plan = null; void refreshLowStockAlerts(); });
    setInterval(() => { _plan = null; void refreshLowStockAlerts(); }, REFRESH_MS);
    _initialized = true;
  }
  if (autoLoad) await refreshLowStock();
  else await refreshLowStockAlerts();
}
