import { apiJson, apiFetch, apiPost, apiPatch, apiPut, showToast, hideToast, esc, fmt } from "./api.js";
import { calculate } from "./calculator.js";
import { desktopDownload, showDownloadToast } from "./desktop_bridge.js";

// ── State ─────────────────────────────────────────────────────────────────────
let _rows = [];
let _bomData = {};
let _stock = {};
let _liveStock = {};
let _moq = {};
let _stStock = {};
let _dispatchedConsumption = {};
let _calcResults = [];
let _decisions = {};
let _draftsByOrderId = {};
let _orderSupplementsByOrderId = {};
let _orderSupplementDetailsByOrderId = {};
let _completedRows = [];
let _completedFolders = [];
let _scheduleMeta = { filename: "", loaded_at: "", row_count: 0 };
let _onRefreshMain = null;
let _checkedIds = new Set();
let _scheduleInitialized = false;
let _batchMergeInFlight = false;
let _modalProgressTimer = null;
let _modalProgressValue = 0;
let _completedFolderCollapsedState = loadCompletedFolderCollapsedState();
let _draftPanelCollapsedState = loadDraftPanelCollapsedState();
let _draftFileSelectionState = loadDraftFileSelectionState();
let _modalDraftId = null;
let _modalDraftReadOnly = false;
let _modalDraftBaseDecisions = {};
let _modalDraftBaseSupplements = {};
let _modalDraftVisibleParts = [];
let _globalBusyDepth = 0;
const ORDER_SCOPED_PART_PREFIXES = ["IC-STM", "IC-M24", "IC-XC2C32"];

// ── Public ────────────────────────────────────────────────────────────────────
export async function initSchedule(onRefreshMain) {
  _onRefreshMain = onRefreshMain || null;
  if (!_scheduleInitialized) {
    document.getElementById("btn-auto-sort").addEventListener("click", handleAutoSort);
    document.getElementById("btn-save-order").addEventListener("click", handleSaveOrder);
    document.getElementById("btn-batch-merge")?.addEventListener("click", handleBatchMerge);
    document.getElementById("btn-batch-dispatch")?.addEventListener("click", handleBatchDispatch);
    document.getElementById("btn-dedup-schedule")?.addEventListener("click", handleDedupSchedule);
    document.getElementById("btn-create-folder")?.addEventListener("click", handleCreateFolder);
    document.getElementById("schedule-scroll")?.addEventListener("click", handleDraftPanelToggleClick);
    _loadPostDispatchShortages();
    _scheduleInitialized = true;
  }
  await refresh();
}

export async function refresh() {
  await Promise.all([loadMainData(), loadStInventoryData(), loadScheduleRows(), loadBomData()]);
  recalculate();
  renderSchedule();
}

function waitForNextFrame() {
  return new Promise(resolve => window.requestAnimationFrame(() => resolve()));
}

export async function refreshCompleted() {
  await loadCompletedRows();
  renderCompletedTab();
}

export function getDecisions() {
  return { ..._decisions };
}

export function getCheckedOrderIds() {
  return _rows
    .filter(row => _checkedIds.has(row.id))
    .map(row => row.id);
}

export function clearCheckedOrderIds(orderIds) {
  for (const id of orderIds) _checkedIds.delete(id);
  updateStatusOnly();
  renderSchedule();
}

export function getScheduleMeta() {
  return { ..._scheduleMeta };
}

function normalizePartKey(partNumber) {
  return String(partNumber || "").trim().toUpperCase();
}

function isOrderScopedPart(partNumber) {
  const key = normalizePartKey(partNumber);
  return ORDER_SCOPED_PART_PREFIXES.some(prefix => key.startsWith(prefix));
}

function normalizeOrderId(value) {
  const id = Number.parseInt(value, 10);
  return Number.isInteger(id) ? id : null;
}

function buildOrderPartKey(orderId, partNumber) {
  const normalizedOrderId = normalizeOrderId(orderId);
  const key = normalizePartKey(partNumber);
  if (!Number.isInteger(normalizedOrderId) || !key) return "";
  return `${normalizedOrderId}::${key}`;
}

function completedFolderStateKey(folderName) {
  return String(folderName || "__unsorted__");
}

function loadCompletedFolderCollapsedState() {
  try {
    const raw = window.localStorage?.getItem("completed-folder-collapsed-state");
    const parsed = raw ? JSON.parse(raw) : {};
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_) {
    return {};
  }
}

function saveCompletedFolderCollapsedState() {
  try {
    window.localStorage?.setItem(
      "completed-folder-collapsed-state",
      JSON.stringify(_completedFolderCollapsedState || {}),
    );
  } catch (_) {}
}

function isCompletedFolderCollapsed(folderName) {
  const key = completedFolderStateKey(folderName);
  if (Object.prototype.hasOwnProperty.call(_completedFolderCollapsedState, key)) {
    return Boolean(_completedFolderCollapsedState[key]);
  }
  return Boolean(folderName);
}

function setCompletedFolderCollapsed(folderName, collapsed) {
  _completedFolderCollapsedState[completedFolderStateKey(folderName)] = Boolean(collapsed);
  saveCompletedFolderCollapsedState();
}

function loadDraftPanelCollapsedState() {
  try {
    const raw = window.localStorage?.getItem("merge-draft-panel-collapsed-state");
    const parsed = raw ? JSON.parse(raw) : {};
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_) {
    return {};
  }
}

function saveDraftPanelCollapsedState() {
  try {
    window.localStorage?.setItem(
      "merge-draft-panel-collapsed-state",
      JSON.stringify(_draftPanelCollapsedState || {}),
    );
  } catch (_) {}
}

function isDraftPanelCollapsed(orderId) {
  const key = String(orderId || "");
  if (!key) return true;
  if (Object.prototype.hasOwnProperty.call(_draftPanelCollapsedState, key)) {
    return Boolean(_draftPanelCollapsedState[key]);
  }
  return true;
}

function setDraftPanelCollapsed(orderId, collapsed) {
  const key = String(orderId || "");
  if (!key) return;
  _draftPanelCollapsedState[key] = Boolean(collapsed);
  saveDraftPanelCollapsedState();
}

function loadDraftFileSelectionState() {
  try {
    const raw = window.localStorage?.getItem("merge-draft-file-selection-state");
    const parsed = raw ? JSON.parse(raw) : {};
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_) {
    return {};
  }
}

function saveDraftFileSelectionState() {
  try {
    window.localStorage?.setItem(
      "merge-draft-file-selection-state",
      JSON.stringify(_draftFileSelectionState || {}),
    );
  } catch (_) {}
}

function getSelectedDraftFileId(draftId, files = []) {
  const key = String(draftId || "");
  if (!key) return null;

  const selected = String(_draftFileSelectionState[key] || "").trim();
  if (!selected) return null;

  const exists = Array.isArray(files) && files.some(file => String(file?.id || "").trim() === selected);
  if (!exists) {
    delete _draftFileSelectionState[key];
    saveDraftFileSelectionState();
    return null;
  }

  return selected;
}

function setSelectedDraftFileId(draftId, fileId) {
  const key = String(draftId || "");
  if (!key) return;

  const normalized = String(fileId || "").trim();
  if (!normalized) {
    delete _draftFileSelectionState[key];
  } else {
    _draftFileSelectionState[key] = normalized;
  }
  saveDraftFileSelectionState();
}

function setGlobalBusyState(active, { title = "系統正在處理中", detail = "大型批次可能需要幾秒鐘，請稍候，不用重複點擊。" } = {}) {
  const overlay = document.getElementById("action-busy-overlay");
  if (!overlay) return;
  const titleEl = document.getElementById("busy-overlay-title");
  const detailEl = document.getElementById("busy-overlay-detail");

  if (active) {
    _globalBusyDepth += 1;
    if (titleEl) titleEl.textContent = title;
    if (detailEl) detailEl.textContent = detail;
    overlay.style.display = "flex";
    return;
  }

  _globalBusyDepth = Math.max(0, _globalBusyDepth - 1);
  if (_globalBusyDepth === 0) {
    overlay.style.display = "none";
  }
}

async function withGlobalBusy(task, options = {}) {
  const timeoutMs = options.timeout || 60000;
  hideToast();
  setGlobalBusyState(true, options);
  try {
    const timeoutPromise = new Promise((_, reject) =>
      setTimeout(() => reject(new Error("操作逾時，請重新整理頁面後再試")), timeoutMs),
    );
    return await Promise.race([task(), timeoutPromise]);
  } finally {
    setGlobalBusyState(false);
  }
}

function normalizeDecisionMap(decisions = {}) {
  const normalized = {};
  for (const [part, decision] of Object.entries(decisions || {})) {
    const key = normalizePartKey(part);
    if (!key || !decision || decision === "None") continue;
    normalized[key] = decision;
  }
  return normalized;
}

function normalizeSupplementMap(supplements = {}) {
  const normalized = {};
  for (const [part, qty] of Object.entries(supplements || {})) {
    const key = normalizePartKey(part);
    const amount = Number(qty || 0);
    if (!key || !Number.isFinite(amount) || amount <= 0) continue;
    normalized[key] = amount;
  }
  return normalized;
}

function normalizeOrderSupplementState(orderSupplements = {}) {
  const normalized = {};
  for (const [rawOrderId, supplements] of Object.entries(orderSupplements || {})) {
    const orderId = normalizeOrderId(rawOrderId);
    if (!Number.isInteger(orderId)) continue;
    normalized[orderId] = normalizeSupplementMap(supplements || {});
  }
  return normalized;
}

function normalizeOrderSupplementDetailState(orderSupplementDetails = {}) {
  const normalized = {};
  for (const [rawOrderId, supplements] of Object.entries(orderSupplementDetails || {})) {
    const orderId = normalizeOrderId(rawOrderId);
    if (!Number.isInteger(orderId)) continue;
    normalized[orderId] = {};
    for (const [rawPart, detail] of Object.entries(supplements || {})) {
      const part = normalizePartKey(rawPart);
      if (!part) continue;
      normalized[orderId][part] = {
        supplement_qty: Number(detail?.supplement_qty || detail?.qty || 0) || 0,
        note: String(detail?.note || "").trim(),
        updated_at: String(detail?.updated_at || "").trim(),
      };
    }
  }
  return normalized;
}

function getStoredOrderSupplementQty(orderId, partNumber) {
  const normalizedOrderId = normalizeOrderId(orderId);
  const key = normalizePartKey(partNumber);
  if (!Number.isInteger(normalizedOrderId) || !key) return 0;
  return Number(_orderSupplementsByOrderId?.[normalizedOrderId]?.[key] || 0) || 0;
}

function getStoredOrderSupplementDetail(orderId, partNumber) {
  const normalizedOrderId = normalizeOrderId(orderId);
  const key = normalizePartKey(partNumber);
  if (!Number.isInteger(normalizedOrderId) || !key) {
    return { supplement_qty: 0, note: "", updated_at: "" };
  }
  const detail = _orderSupplementDetailsByOrderId?.[normalizedOrderId]?.[key];
  return detail
    ? {
        supplement_qty: Number(detail.supplement_qty || 0) || 0,
        note: String(detail.note || "").trim(),
        updated_at: String(detail.updated_at || "").trim(),
      }
    : { supplement_qty: 0, note: "", updated_at: "" };
}

function isEcPart(partNumber) {
  return normalizePartKey(partNumber).startsWith("EC-");
}

function isMainWriteBlockingShortage(shortage) {
  const shortageAmount = Number(shortage?.shortage_amount || 0);
  if (!Number.isFinite(shortageAmount) || shortageAmount <= 0) return false;
  if (String(shortage?.decision || "") === "Shortage") return true;
  if (!isEcPart(shortage?.part_number)) return true;

  const resultingStock = Number(shortage?.resulting_stock);
  return !Number.isFinite(resultingStock) || resultingStock < 0;
}

function getMainWriteBlockingShortages(shortages = []) {
  return (Array.isArray(shortages) ? shortages : []).filter(isMainWriteBlockingShortage);
}

function buildMainWriteBlockedMessage(shortages = [], model = "") {
  const prefix = model ? `${model}\n` : "";
  const lines = [...new Map(
    (shortages || [])
      .filter(item => String(item?.part_number || "").trim())
      .map(item => [String(item.part_number).trim(), item]),
  ).values()].slice(0, 6).map(item => {
    const part = String(item?.part_number || "").trim();
    const shortageAmount = Number(item?.shortage_amount || 0);
    if (isEcPart(part)) {
      const resultingStock = Number(item?.resulting_stock);
      if (Number.isFinite(resultingStock)) {
        return `- ${part}: 寫入後結存 ${fmt(resultingStock)}，EC 料不能為負數`;
      }
      return `- ${part}: EC 料結存無法判定，暫時不能寫入主檔`;
    }
    if (Number.isFinite(shortageAmount) && shortageAmount > 0) {
      return `- ${part}: 仍缺 ${fmt(shortageAmount)}`;
    }
    return `- ${part}: 仍有缺料`;
  });

  const hiddenCount = Math.max(0, getMainWriteBlockingShortages(shortages).length - lines.length);
  if (hiddenCount) lines.push(`- 另有 ${hiddenCount} 項未展開`);
  return `${prefix}以下料號仍不能寫入主檔，請先補料或調整決策：\n${lines.join("\n")}`;
}

function isMainWriteBlockedMessage(message) {
  return String(message || "").includes("不能寫入主檔");
}

function showMainWriteBlockedNotice(message) {
  showToast(message, { sticky: true, tone: "error" });
}

function setLocalDecision(partNumber, decision) {
  const key = normalizePartKey(partNumber);
  if (!key) return;
  if (!decision || decision === "None") {
    delete _decisions[key];
    return;
  }
  _decisions[key] = decision;
}

function getAffectedOrderIdsForPart(partNumber) {
  const key = normalizePartKey(partNumber);
  if (!key) return [];
  const ids = new Set();
  _calcResults.forEach((result, index) => {
    if (!result) return;
    const items = [...(result.shortages || []), ...(result.customer_material_shortages || [])];
    if (items.some(item => normalizePartKey(item.part_number) === key)) {
      const orderId = _rows[index]?.id;
      if (Number.isInteger(orderId)) ids.add(orderId);
    }
  });
  return [...ids];
}

async function persistDecisionsForOrders(decisions, orderIds, orderDecisions = {}) {
  const targetIds = [...new Set((orderIds || []).filter(id => Number.isInteger(id)))];
  if (!targetIds.length) return;

  const targetIdSet = new Set(targetIds);
  const decisionsByOrder = new Map();

  for (const [part, rawDecision] of Object.entries(decisions || {})) {
    const key = normalizePartKey(part);
    if (!key) continue;

    let relevantIds = getAffectedOrderIdsForPart(key).filter(id => targetIdSet.has(id));
    if (!relevantIds.length && targetIds.length === 1) {
      relevantIds = [...targetIds];
    }
    if (!relevantIds.length) continue;

    const decision = rawDecision && rawDecision !== "None" ? rawDecision : "None";
    relevantIds.forEach(orderId => {
      if (!decisionsByOrder.has(orderId)) decisionsByOrder.set(orderId, {});
      decisionsByOrder.get(orderId)[key] = decision;
    });
  }

  for (const [rawOrderId, rawDecisions] of Object.entries(orderDecisions || {})) {
    const orderId = normalizeOrderId(rawOrderId);
    if (!targetIdSet.has(orderId)) continue;
    const normalized = {};
    for (const [part, decision] of Object.entries(rawDecisions || {})) {
      const key = normalizePartKey(part);
      if (!key) continue;
      normalized[key] = decision && decision !== "None" ? decision : "None";
    }
    if (!Object.keys(normalized).length) continue;
    if (!decisionsByOrder.has(orderId)) decisionsByOrder.set(orderId, {});
    Object.assign(decisionsByOrder.get(orderId), normalized);
  }

  if (!decisionsByOrder.size) return;
  await Promise.all(
    [...decisionsByOrder.entries()].map(([orderId, payload]) =>
      apiPost(`/api/schedule/orders/${orderId}/decisions`, { decisions: payload })
    )
  );
}

// ── Data loading ──────────────────────────────────────────────────────────────
async function loadMainData() {
  try {
    const d = await apiJson("/api/main-file/data");
    _stock = d.stock || {};
    _liveStock = d.live_stock || {};
    _moq = d.moq || {};
  } catch (_) { _stock = {}; _liveStock = {}; _moq = {}; }
}

async function loadStInventoryData() {
  try {
    const d = await apiJson("/api/system/st-inventory/data");
    _stStock = d.stock || {};
  } catch (_) { _stStock = {}; }
}

async function loadScheduleRows() {
  try {
    const d = await apiJson("/api/schedule/rows");
    _rows = d.rows || [];
      _dispatchedConsumption = d.dispatched_consumption || {};
      _decisions = normalizeDecisionMap(d.decisions || {});
      _draftsByOrderId = d.merge_drafts || {};
      _orderSupplementsByOrderId = normalizeOrderSupplementState(d.order_supplements || {});
      _orderSupplementDetailsByOrderId = normalizeOrderSupplementDetailState(d.order_supplement_details || {});
      _scheduleMeta = {
      filename: String(d.filename || ""),
      loaded_at: String(d.loaded_at || ""),
      row_count: Array.isArray(d.rows) ? d.rows.length : 0,
    };
    const completedBadge = document.getElementById("completed-count");
    if (completedBadge && d.completed_count > 0) {
      completedBadge.textContent = d.completed_count;
      completedBadge.style.display = "inline";
    } else if (completedBadge) {
      completedBadge.style.display = "none";
    }
  } catch (_) {
    _rows = [];
      _dispatchedConsumption = {};
      _decisions = {};
      _draftsByOrderId = {};
      _orderSupplementsByOrderId = {};
      _orderSupplementDetailsByOrderId = {};
      _scheduleMeta = { filename: "", loaded_at: "", row_count: 0 };
  }
}

async function loadBomData() {
  try {
    _bomData = await apiJson("/api/bom/data");
  } catch (_) { _bomData = {}; }
}

async function loadCompletedRows() {
  try {
    const d = await apiJson("/api/schedule/completed");
    _completedRows = d.rows || [];
    _completedFolders = d.folders || [];
  } catch (_) { _completedRows = []; _completedFolders = []; }
}

// ── Calculation ───────────────────────────────────────────────────────────────
function recalculate() {
  if (!_rows.length) { _calcResults = []; return; }
  // 只計算勾選的訂單
  const checkedOrders = _rows.filter(r => _checkedIds.has(r.id));
  const checkedResults = checkedOrders.length
    ? calculate(checkedOrders, _bomData, _stock, _moq, _dispatchedConsumption, _stStock, _orderSupplementsByOrderId)
    : [];
  // 建立以 order id 為 key 的結果 map
  const resultById = new Map();
  checkedOrders.forEach((r, i) => resultById.set(r.id, checkedResults[i]));
  // _calcResults 保持與 _rows 同長度，未勾選的為 null
  _calcResults = _rows.map(r => resultById.get(r.id) || null);
}

// ── Render schedule ───────────────────────────────────────────────────────────
function renderScheduleLegacyBase() {
  const container = document.getElementById("schedule-scroll");
  container.innerHTML = "";

  if (!_rows.length) {
    container.innerHTML = '<div class="empty-state">尚未上傳排程表，或排程為空</div>';
    renderShortagePanel([], [], buildMainFileDeficitItems());
    return;
  }

  const resultMap = {};
  _calcResults.forEach((r, i) => { resultMap[_rows[i]?.id] = r; });

  for (const r of _rows) {
    container.appendChild(buildRowCard(r, resultMap));
  }

  initSortable(container);

  const allShortages = [];
  const allCSShortages = [];
  _calcResults.forEach((r, i) => {
    if (!r) return;
    const code = _rows[i]?.code || "";
    const model = _rows[i]?.model || "";
    (r.shortages || []).forEach(s => allShortages.push({ ...s, _row_code: code, _row_model: model }));
    (r.customer_material_shortages || []).forEach(s => allCSShortages.push({ ...s, _row_code: code, _row_model: model }));
  });
  renderShortagePanel(allShortages, allCSShortages, buildMainFileDeficitItems());
}

function formatDraftTime(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.slice(5, 16).replace("T", " ");
}

function buildDraftPanelHtmlLegacyBase(draft) {
  const files = draft?.files || [];
  const shortages = draft?.shortages || [];
  const updatedAt = formatDraftTime(draft?.updated_at);
  const fileHtml = files.length
    ? files.map(file => `<span class="merge-draft-file" title="${esc(file.filename || "")}">${esc(file.filename || "")}</span>`).join("")
    : '<span class="merge-draft-file merge-draft-file-empty">副檔尚未生成</span>';

  return `
    <div class="merge-draft-panel">
      <div class="merge-draft-summary">
        <span class="merge-draft-pill">副檔 ${files.length} 份</span>
        <span class="merge-draft-meta">缺料 ${shortages.length} 筆</span>
        ${updatedAt ? `<span class="merge-draft-meta">更新 ${esc(updatedAt)}</span>` : ""}
      </div>
      <div class="merge-draft-files">${fileHtml}</div>
      <div class="merge-draft-actions">
        <button class="btn btn-secondary btn-sm btn-draft-preview" data-draft-id="${draft.id}">預覽</button>
        <button class="btn btn-secondary btn-sm btn-draft-edit" data-draft-id="${draft.id}">修改</button>
        <button class="btn btn-secondary btn-sm btn-draft-download" data-draft-id="${draft.id}">下載</button>
        <button class="btn btn-secondary btn-sm btn-draft-delete" data-draft-id="${draft.id}">刪除</button>
      </div>
    </div>`;
}

// ── Build single-row card ─────────────────────────────────────────────────────
function buildRowCard(r, resultMap, visibleShortageTotals = null) {
  const div = document.createElement("div");
  div.className = "po-group";
  div.dataset.orderId = r.id;

  const res = resultMap[r.id];
  const draft = _draftsByOrderId?.[r.id] || null;
  const badge = buildOrderBadge(r, res, visibleShortageTotals);
  const date = (r.delivery_date || r.ship_date) ? (r.delivery_date || r.ship_date).slice(5).replace("-", "/") : "—";
  const qty = r.order_qty != null ? r.order_qty : "—";
  const code = esc(r.code || "");
  const isChecked = _checkedIds.has(r.id);
  const statusTag = r.status === "dispatched"
    ? '<span class="tag tag-dispatched">已扣帳</span>'
    : r.status === "merged"
      ? '<span class="tag tag-merged">已merge</span>'
      : '<span class="tag tag-pending">待排程</span>';
  const remarkSpan = r.remark
    ? `<span class="po-ship-date" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.remark)}">${esc(r.remark.slice(0, 24))}${r.remark.length > 24 ? "..." : ""}</span>`
    : `<span></span>`;
  const completeTitle = draft
    ? "照副檔寫入主檔"
    : r.status === "merged"
      ? "請先重新 merge 生成副檔"
      : "直接發料";
  const draftHtml = draft ? buildDraftPanelHtml(draft) : "";
  const draftToggleHtml = draft
    ? `<button
        class="btn-draft-toggle row-draft-toggle ${isDraftPanelCollapsed(r.id) ? "" : "is-expanded"}"
        type="button"
        data-order-id="${r.id}"
        aria-expanded="${isDraftPanelCollapsed(r.id) ? "false" : "true"}"
        title="${isDraftPanelCollapsed(r.id) ? "展開副檔工作台" : "收起副檔工作台"}"
      >${isDraftPanelCollapsed(r.id) ? "▶" : "▼"}</button>`
    : "";

  div.innerHTML = `
    <div class="po-group-header">
      <input type="checkbox" class="row-check" data-order-id="${r.id}" ${isChecked ? "checked" : ""}
             style="width:16px;height:16px;margin:0;cursor:pointer;accent-color:#34c759">
      <span class="drag-handle" title="拖曳調整順序">⠿</span>
      <span class="po-model-wrap">
        <span class="po-number model-editable" data-order-id="${r.id}" title="雙擊編輯機種名稱">${esc(r.model)}</span>${draftToggleHtml}
      </span>
      <span style="color:#c7c7cc;font-size:13px;text-align:center">|</span>
      <span class="po-number" style="color:#6b7280;font-weight:500">${r.po_number}</span>
      <span class="tag tag-pcb pcb-chip">${esc(r.pcb)}</span>
      <span style="font-size:13px;color:#3c3c43;font-weight:500;white-space:nowrap">${qty}<span style="font-size:11px;color:#8e8e93;font-weight:400">pcs</span></span>
      <span class="po-ship-date">${date}</span>
      <input class="code-input" type="text" value="${code}"
             data-order-id="${r.id}" placeholder="編號"
             style="width:100%;box-sizing:border-box;border:1px solid #e5e5ea;border-radius:4px;padding:2px 6px;font-size:12px;text-align:center;background:transparent">
      ${remarkSpan}
      <span class="po-status-badge ${badge.cls}">${badge.text}</span>
      <div class="row-actions">
        <button class="btn-edit-date" data-order-id="${r.id}" title="改交期">📅</button>
        <button class="btn-cancel-order" data-order-id="${r.id}" title="取消訂單">✕</button>
      </div>
    </div>
    ${draftHtml}`;

  // 勾選控制
  div.querySelector(".row-check").addEventListener("change", e => {
    const id = parseInt(e.target.dataset.orderId);
    if (e.target.checked) _checkedIds.add(id);
    else _checkedIds.delete(id);
    recalculate();
    updateStatusOnly();
  });

  // 雙擊編輯機種名稱
  div.querySelector(".model-editable").addEventListener("dblclick", e => {
    const span = e.target;
    const currentModel = span.textContent;
    const input = document.createElement("input");
    input.type = "text";
    input.value = currentModel;
    input.className = "model-edit-input";
    input.style.cssText = "width:100%;box-sizing:border-box;border:1px solid #007aff;border-radius:4px;padding:2px 6px;font-size:14px;font-weight:600;outline:none;background:#fff";
    span.replaceWith(input);
    input.focus();
    input.select();

    const save = async () => {
      const newModel = input.value.trim();
      if (newModel && newModel !== currentModel) {
        try {
          await apiPatch(`/api/schedule/orders/${r.id}/model`, { model: newModel });
          showToast("機種已更新");
          await refresh();
          return;
        } catch (err) { showToast("失敗：" + err.message); }
      }
      const newSpan = document.createElement("span");
      newSpan.className = "po-number model-editable";
      newSpan.dataset.orderId = r.id;
      newSpan.title = "雙擊編輯機種名稱";
      newSpan.textContent = newModel || currentModel;
      input.replaceWith(newSpan);
      newSpan.addEventListener("dblclick", e => {
        div.querySelector(".model-editable")?.dispatchEvent(new MouseEvent("dblclick"));
      });
    };

    input.addEventListener("blur", save);
    input.addEventListener("keydown", e => {
      if (e.key === "Enter") { e.preventDefault(); input.blur(); }
      if (e.key === "Escape") { input.value = currentModel; input.blur(); }
    });
  });

  // 編號輸入
  div.querySelector(".code-input").addEventListener("change", async e => {
    const orderId = parseInt(e.target.dataset.orderId);
    const codeVal = e.target.value.trim();
    try {
      await apiPatch(`/api/schedule/orders/${orderId}/code`, { code: codeVal });
    } catch (_) {}
  });

  // 發料（直接鎖定）
  const completeButton = div.querySelector(".btn-complete");
  if (completeButton) {
    completeButton.title = completeTitle;
    completeButton.innerHTML = "&#10003;";
  }
  completeButton?.addEventListener("click", () => {
    if (draft) {
      void handleCommitDraft(draft.id, r.model);
      return;
    }
    if (r.status === "merged") {
      showToast("這筆還沒有副檔，請先重新 merge");
      return;
    }
    void handleDispatch(r.id, r.model);
  });

  // 改交期
  div.querySelector(".btn-edit-date").addEventListener("click", () => {
    handleEditDelivery(r.id, r.delivery_date || r.ship_date || "");
  });

  // 取消
  div.querySelector(".btn-cancel-order").addEventListener("click", () => {
    handleCancel(r.id, r.model);
  });

  if (draft) {
    div.querySelector(".btn-draft-commit")?.addEventListener("click", () => {
      void handleCommitDraft(draft.id, r.model);
    });
    div.querySelector(".btn-draft-edit")?.addEventListener("click", () => {
      const selectedFileId = getSelectedDraftFileId(draft.id, draft.files || []);
      void showDraftModal(draft.id, { readOnly: false, fileId: selectedFileId });
    });
    div.querySelector(".btn-draft-download")?.addEventListener("click", () => {
      const selectedFileId = getSelectedDraftFileId(draft.id, draft.files || []);
      void downloadDraft(draft.id, selectedFileId);
    });
    div.querySelector(".btn-draft-delete")?.addEventListener("click", () => {
      void handleDeleteDraft(draft.id, r.model);
    });
  }

  return div;
}

function handleDraftPanelToggleClick(event) {
  const fileButton = event.target.closest(".merge-draft-file-select");
  if (fileButton) {
    const draftId = parseInt(fileButton.dataset.draftId || "", 10);
    if (!Number.isInteger(draftId)) return;

    const nextFileId = String(fileButton.dataset.fileId || "").trim();
    const panel = fileButton.closest(".merge-draft-panel");
    setSelectedDraftFileId(draftId, nextFileId);

    panel?.querySelectorAll(".merge-draft-file-select").forEach(button => {
      const buttonFileId = String(button.dataset.fileId || "").trim();
      const isActive = nextFileId ? buttonFileId === nextFileId : !buttonFileId;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
    return;
  }

  const button = event.target.closest(".btn-draft-toggle");
  if (!button) return;

  const orderId = parseInt(button.dataset.orderId || "", 10);
  if (!Number.isInteger(orderId)) return;

  const row = button.closest(".po-group");
  const panel = row?.querySelector(".merge-draft-panel");
  if (!panel) return;

  const nextCollapsed = !panel.classList.contains("is-collapsed");
  panel.classList.toggle("is-collapsed", nextCollapsed);
  button.setAttribute("aria-expanded", nextCollapsed ? "false" : "true");
  button.textContent = nextCollapsed ? "▶" : "▼";
  button.classList.toggle("is-expanded", !nextCollapsed);
  button.title = nextCollapsed ? "展開副檔工作台" : "收起副檔工作台";
  setDraftPanelCollapsed(orderId, nextCollapsed);
}

function cardBadge(res, orderId) {
  if (!res && orderId !== undefined && !_checkedIds.has(orderId)) return { cls: "badge-unchecked", text: "—" };
  if (!res) return { cls: "badge-no-bom", text: "BOM未上傳" };
  if (res.status === "ok") return { cls: "badge-ok", text: "OK" };
  if (res.status === "shortage") {
    const total = [...(res.shortages || []), ...(res.customer_material_shortages || [])]
      .reduce((s, x) => s + (x.shortage_amount || 0), 0);
    return { cls: "badge-shortage", text: `缺 ${fmt(roundShortageUiValue(total))}` };
  }
  return { cls: "badge-no-bom", text: "BOM未上傳" };
}

function formatShortageScopeLabel(model, code) {
  const modelText = String(model || "").trim() || "未分類機種";
  const codeText = String(code || "").trim();
  if (!codeText || codeText === modelText) return modelText;
  return `${modelText} ${codeText}`;
}

function buildShortageGroupMeta(row = {}, item = {}) {
  const orderId = normalizeOrderId(item?._order_id ?? item?.order_id ?? row?.id);
  const model = String(item?._row_model || item?.model || row?.model || "").trim() || "未分類機種";
  const rawCode = String(item?._row_code || item?.batch_code || row?.code || "").trim();
  const poNumber = String(item?._po_number || item?.po_number || row?.po_number || "").trim();
  const code = rawCode || model;
  const explicitOrderIndex = Number(item?._row_order_index);
  let orderIndex = Number.isFinite(explicitOrderIndex) ? explicitOrderIndex : Number.MAX_SAFE_INTEGER;

  if (Number.isInteger(orderId)) {
    const rowIndex = _rows.findIndex(candidate => normalizeOrderId(candidate?.id) === orderId);
    if (rowIndex >= 0) {
      orderIndex = rowIndex;
    }
  } else {
    const sortOrder = Number(row?.sort_order ?? item?.sort_order);
    if (Number.isFinite(sortOrder)) orderIndex = sortOrder;
  }

  return {
    _row_code: code,
    _row_model: model,
    _row_group_key: Number.isInteger(orderId)
      ? `order:${orderId}`
      : `scope:${normalizePartKey(model)}:${normalizePartKey(rawCode || model)}`,
    _row_group_label: formatShortageScopeLabel(model, rawCode),
    _row_order_index: orderIndex,
    _order_id: orderId,
    _po_number: poNumber,
  };
}

function buildShortageScopeList(rows = []) {
  const scopes = [];
  const seen = new Set();
  for (const row of rows || []) {
    const meta = buildShortageGroupMeta(row);
    if (!meta._row_group_key || seen.has(meta._row_group_key)) continue;
    seen.add(meta._row_group_key);
    scopes.push({
      key: meta._row_group_key,
      label: meta._row_group_label,
      po_number: meta._po_number,
    });
  }
  return scopes;
}

function shortageGroupHeadingHtml(label, poNumber = "", { compact = false } = {}) {
  const safeLabel = esc(label || "未指定機種");
  const poText = String(poNumber || "").trim();
  const poHtml = poText
    ? `<span style="font-size:${compact ? "10px" : "11px"};font-weight:500;color:#6b7280">PO ${esc(poText)}</span>`
    : "";

  if (compact) {
    return `<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin:8px 0 4px;font-size:11px;font-weight:600;color:#6b7280"><span>${safeLabel}</span>${poHtml}</div>`;
  }

  return `<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;margin:12px 0 8px;padding:6px 10px;background:#f3f4f6;border-radius:6px;font-weight:600;font-size:13px;color:#1f2937"><span>${safeLabel}</span>${poHtml}</div>`;
}

function buildShortageItemsForRow(row, items = []) {
  return (items || []).map(item => {
    const meta = buildShortageGroupMeta(row, item);
    const orderId = meta._order_id;
    return {
      ...item,
      ...meta,
      supplement_qty: Number(item?.supplement_qty || 0) > 0
        ? Number(item.supplement_qty || 0)
        : getStoredOrderSupplementQty(orderId, item?.part_number),
      default_supplement: Number(item?.default_supplement || 0) > 0
        ? Number(item.default_supplement || 0)
        : getStoredOrderSupplementQty(orderId, item?.part_number),
    };
  });
}

function getRightPanelSupplementQty(item) {
  if (Number(item?.default_supplement || 0) > 0) return Number(item?.default_supplement || 0);
  return Number(item?.supplement_qty || 0);
}

function applyRightPanelSupplementState(item) {
  const supplementQty = getRightPanelSupplementQty(item);
  const resultingStock = computeShortageResultingStock(item, supplementQty);
  return {
    ...item,
    supplement_qty: supplementQty,
    default_supplement: supplementQty,
    resulting_stock: resultingStock,
  };
}

function getRightPanelResultingStock(item) {
  const enriched = applyRightPanelSupplementState(item);
  return Number(enriched?.resulting_stock);
}

function shouldRenderRightPanelShortageItem(item) {
  const shortageAmount = Number(item?.shortage_amount || 0);
  if (!Number.isFinite(shortageAmount) || shortageAmount <= 0) return false;
  const resultingStock = getRightPanelResultingStock(item);
  return Number.isFinite(resultingStock) ? resultingStock < 0 : shortageAmount > 0;
}

function removeRightPanelShortageRowIfResolved(row) {
  const scroll = document.getElementById("right-scroll");
  if (!row || !scroll) return;

  const header = row.previousElementSibling && !row.previousElementSibling.classList.contains("shortage-item")
    ? row.previousElementSibling
    : null;
  row.remove();

  if (header) {
    const next = header.nextElementSibling;
    if (!next || !next.classList.contains("shortage-item")) {
      header.remove();
    }
  }

  const remainingRows = scroll.querySelectorAll(".shortage-item").length;
  const badge = document.getElementById("shortage-count");
  if (badge) {
    if (remainingRows > 0) {
      badge.style.display = "inline";
      badge.textContent = String(remainingRows);
    } else {
      badge.style.display = "none";
    }
  }
  if (!remainingRows) {
    scroll.innerHTML = '<div class="no-shortage-msg">無缺料</div>';
  }
}

function getEffectiveShortageState(row, res = null) {
  const draft = row ? _draftsByOrderId?.[row.id] || null : null;
  if (draft) {
    return {
      hasDraft: true,
      shortages: buildShortageItemsForRow(row, draft.shortages || []),
      customer_material_shortages: [],
      status: Array.isArray(draft.shortages) && draft.shortages.length ? "shortage" : "ok",
    };
  }

  return {
    hasDraft: false,
    shortages: buildShortageItemsForRow(row, res?.shortages || []),
    customer_material_shortages: buildShortageItemsForRow(row, res?.customer_material_shortages || []),
    status: res?.status || (!res ? "no_bom" : "ok"),
  };
}

function buildOrderBadge(row, res, visibleShortageTotals = null) {
  if (!res && row?.id !== undefined && !_checkedIds.has(row.id)) {
    return { cls: "badge-unchecked", text: "—" };
  }

  const orderId = normalizeOrderId(row?.id);
  const effective = getEffectiveShortageState(row, res);
  if (Number.isInteger(orderId) && _checkedIds.has(orderId) && visibleShortageTotals instanceof Map && (effective.hasDraft || res)) {
    const total = Number(visibleShortageTotals.get(orderId) || 0);
    return total > 0
      ? { cls: "badge-shortage", text: `缺 ${fmt(roundShortageUiValue(total))}` }
      : effective.status === "no_bom"
        ? { cls: "badge-no-bom", text: "BOM未上傳" }
        : { cls: "badge-ok", text: "OK" };
  }

  if (effective.hasDraft) {
    const total = [...effective.shortages, ...effective.customer_material_shortages]
      .reduce((sum, item) => sum + (item.shortage_amount || 0), 0);
    return total > 0
      ? { cls: "badge-shortage", text: `缺 ${fmt(roundShortageUiValue(total))}` }
      : { cls: "badge-ok", text: "OK" };
  }

  return cardBadge(res, row?.id);
}

function buildRightPanelShortageData() {
  const shortagesByScope = {};
  const csShortagesByScope = {};
  const checkedRows = _rows.filter(row => _checkedIds.has(row.id));
  const orderedScopes = buildShortageScopeList(checkedRows);

  checkedRows.forEach(row => {
    const index = _rows.findIndex(item => item.id === row.id);
    if (index < 0) return;

    const scopeKey = buildShortageGroupMeta(row)._row_group_key;
    const effective = getEffectiveShortageState(row, _calcResults[index]);
    if (!shortagesByScope[scopeKey]) shortagesByScope[scopeKey] = [];
    if (!csShortagesByScope[scopeKey]) csShortagesByScope[scopeKey] = [];

    for (const item of (effective.shortages || [])) {
      const partKey = normalizePartKey(item?.part_number);
      shortagesByScope[scopeKey].push(applyRightPanelSupplementState({
        ...item,
        decision: _decisions[partKey] || item?.decision || "None",
      }));
    }
    for (const item of (effective.customer_material_shortages || [])) {
      const partKey = normalizePartKey(item?.part_number);
      csShortagesByScope[scopeKey].push(applyRightPanelSupplementState({
        ...item,
        decision: _decisions[partKey] || item?.decision || "None",
      }));
    }
  });

  for (const items of Object.values(shortagesByScope)) items.sort(compareShortageItems);
  for (const items of Object.values(csShortagesByScope)) items.sort(compareShortageItems);

  const shortages = [];
  const csShortages = [];
  for (const scope of orderedScopes) {
    shortages.push(...(shortagesByScope[scope.key] || []).filter(item => shouldRenderRightPanelShortageItem(item)));
    csShortages.push(...(csShortagesByScope[scope.key] || []).filter(item => shouldRenderRightPanelShortageItem(item)));
  }

  return { shortages, csShortages };
}

function buildCheckedOrderVisibleShortageBadgeMap() {
  const badgeMap = new Map();
  const { shortages, csShortages } = buildRightPanelShortageData();
  for (const item of [...shortages, ...csShortages]) {
    const orderId = normalizeOrderId(item?._order_id);
    if (!Number.isInteger(orderId)) continue;
    badgeMap.set(orderId, (badgeMap.get(orderId) || 0) + Number(item?.shortage_amount || 0));
  }
  return badgeMap;
}

function buildRawModalShortageGroups(targets) {
  const targetIds = new Set(
    (targets || [])
      .map(target => normalizeOrderId(target?.id))
      .filter(Number.isInteger)
  );
  const targetRows = _rows.filter(row => targetIds.has(row.id));
  const rawResults = targetRows.length
    ? calculate(targetRows, _bomData, _stock, _moq, _dispatchedConsumption, _stStock, {})
    : [];

  const shortagesByScope = {};
  const csShortagesByScope = {};
  const orderedScopes = buildShortageScopeList(targetRows);
  rawResults.forEach((result, index) => {
    if (!result) return;
    const row = targetRows[index];
    if (!row) return;

    const meta = buildShortageGroupMeta(row);
    const scopeKey = meta._row_group_key;

    (result.shortages || []).forEach(item => {
      if (!shortagesByScope[scopeKey]) shortagesByScope[scopeKey] = [];
      shortagesByScope[scopeKey].push({ ...item, ...meta });
    });
    (result.customer_material_shortages || []).forEach(item => {
      if (!csShortagesByScope[scopeKey]) csShortagesByScope[scopeKey] = [];
      csShortagesByScope[scopeKey].push({ ...item, ...meta });
    });
  });

  return { shortagesByScope, csShortagesByScope, orderedScopes };
}

/** 從主檔庫存中找出已經確定缺料的料號（庫存 < 安全水位）。 */
function buildMainFileDeficitItems() {
  if (!_liveStock || !Object.keys(_liveStock).length) return [];

  // 建立 BOM 料號描述對照表
  const descLookup = {};
  for (const entry of Object.values(_bomData || {})) {
    for (const comp of (entry?.components || [])) {
      if (!comp?.part_number) continue;
      const key = normalizePartKey(comp.part_number);
      if (key && comp.description && !descLookup[key]) {
        descLookup[key] = comp.description;
      }
    }
  }

  const deficits = [];
  for (const [part, stockQty] of Object.entries(_liveStock)) {
    const key = normalizePartKey(part);
    if (!key) continue;
    const currentStock = Number(stockQty || 0);
    if (currentStock >= 0) continue;

    deficits.push({
      part_number: part,
      description: descLookup[key] || "",
      current_stock: currentStock,
      shortage_amount: Math.abs(currentStock),
      moq: _moq?.[key] || 0,
    });
  }

  deficits.sort((a, b) => b.shortage_amount - a.shortage_amount);
  return deficits;
}

// ── Actions ───────────────────────────────────────────────────────────────────
async function handleDispatch(orderId, model) {
  if (!confirm(`確定要將「${model}」標記為已發料並鎖定？`)) return;

  const btn = document.querySelector(`.btn-complete[data-order-id="${orderId}"]`);
  if (btn) { btn.disabled = true; btn.textContent = "..."; }

  try {
    const result = await apiPost(`/api/schedule/orders/${orderId}/dispatch`, { decisions: _decisions });
    showToast(`已發料，${result.merged_parts} 筆料號已 Merge`);
    _checkedIds.delete(orderId);
    await refresh();
    if (_onRefreshMain) await _onRefreshMain();
  } catch (e) {
    showToast("失敗：" + e.message);
    if (btn) { btn.disabled = false; btn.textContent = "✓"; }
  }
}

async function handleEditDelivery(orderId, currentDate) {
  const newDate = prompt("請輸入新交期（格式: YYYY-MM-DD）：", currentDate);
  if (!newDate) return;
  try {
    const result = await apiPatch(`/api/schedule/orders/${orderId}/delivery`, { delivery_date: newDate });
    if (result.alert) {
      alert(result.message);
    } else {
      showToast("交期已更新");
    }
    await refresh();
  } catch (e) { showToast("失敗：" + e.message); }
}

async function handleCancel(orderId, model) {
  if (!confirm(`確定要取消訂單「${model}」？`)) return;
  try {
    const result = await apiPost(`/api/schedule/orders/${orderId}/cancel`);
    if (result.alert) {
      alert(result.message);
    } else {
      showToast("訂單已取消");
    }
    await refresh();
  } catch (e) { showToast("失敗：" + e.message); }
}

// ── Shortage Modal ────────────────────────────────────────────────────────────
let _modalTargets = [];
let _modalBomFiles = [];
let _modalCarryOversByModel = {};
let _modalMode = "download";
let _modalPreviewShortages = [];

function getModalTargetForBomFile(bomFile) {
  const keys = new Set();
  const primary = normalizePartKey(bomFile?.model);
  if (primary) keys.add(primary);
  String(bomFile?.group_model || "")
    .split(",")
    .map(normalizePartKey)
    .filter(Boolean)
    .forEach(key => keys.add(key));
  return _modalTargets.find(target => keys.has(normalizePartKey(target?.model))) || null;
}

function buildModalHeaderOverrides() {
  const overrides = {};
  _modalBomFiles.forEach(bomFile => {
    const target = getModalTargetForBomFile(bomFile);
    const poNumber = String(target?.po_number || "").trim();
    if (!poNumber) return;
    overrides[bomFile.id] = { po_number: poNumber };
  });
  return overrides;
}

function buildModalCarryOversByModel(targets) {
  const carryOvers = {};
  const running = {};

  Object.entries(_stock || {}).forEach(([part, qty]) => {
    const key = normalizePartKey(part);
    if (!key) return;
    running[key] = Number(qty || 0);
  });

  Object.entries(_dispatchedConsumption || {}).forEach(([part, consumed]) => {
    const key = normalizePartKey(part);
    running[key] = Number(running[key] || 0) - Number(consumed || 0);
  });

  const normalizedBomMap = {};
  Object.entries(_bomData || {}).forEach(([model, entry]) => {
    normalizedBomMap[normalizePartKey(model)] = entry?.components || [];
  });

  (targets || []).forEach(target => {
    const modelKey = normalizePartKey(target?.model);
    if (!modelKey) return;

    const components = normalizedBomMap[modelKey] || [];
    const partMap = {};

    components.forEach(component => {
      if (component?.is_dash || Number(component?.needed_qty || 0) <= 0) return;

      const part = normalizePartKey(component?.part_number);
      if (!part) return;

      if (!(part in partMap)) {
        partMap[part] = roundShortageUiValue(running[part] ?? 0);
      }

      const currentStock = Number(running[part] || 0);
      const neededQty = Number(component?.needed_qty || 0);
      const prevQty = Number(component?.prev_qty_cs || 0);
      running[part] = currentStock + prevQty - neededQty;
    });

    carryOvers[modelKey] = partMap;
  });

  return carryOvers;
}

function buildModalCarryOverOverrides() {
  const overrides = {};
  _modalBomFiles.forEach(bomFile => {
    const target = getModalTargetForBomFile(bomFile);
    const modelKey = normalizePartKey(target?.model);
    if (!modelKey) return;
    overrides[bomFile.id] = { ...(_modalCarryOversByModel[modelKey] || {}) };
  });
  return overrides;
}

async function saveCurrentDraftFromModal({ silent = false } = {}) {
  if (!_modalDraftId) return null;
  const visibleParts = new Set((_modalDraftVisibleParts || []).map(normalizePartKey).filter(Boolean));
  const supplements = { ...(_modalDraftBaseSupplements || {}) };
  const decisions = { ...(_modalDraftBaseDecisions || {}) };

  visibleParts.forEach(part => {
    delete supplements[part];
    delete decisions[part];
  });

  Object.assign(supplements, _collectModalSupplements());
  Object.entries(_collectModalDecisions()).forEach(([part, decision]) => {
    const key = normalizePartKey(part);
    if (!key || !decision || decision === "None") return;
    decisions[key] = decision;
  });
  const response = await apiPut(`/api/schedule/drafts/${_modalDraftId}`, { decisions, supplements });
  if (!silent) showToast("副檔已更新");
  await refresh();
  return response?.draft || null;
}

function draftDecisionLabel(decision) {
  const labels = {
    CreateRequirement: "建立需求",
    MarkHasPO: "已有 PO",
    IgnoreOnce: "忽略一次",
    Shortage: "保留缺料",
    None: "一般",
  };
  return labels[decision] || "一般";
}

function draftInlineStatHtml(label, value, cls = "") {
  return `<span class="draft-preview-inline-stat ${cls}"><span>${esc(label)}</span><strong>${fmt(roundShortageUiValue(value))}</strong></span>`;
}

function syncDraftPartControls(list, part, { qty = null, shortageChecked = null } = {}) {
  const partKey = normalizePartKey(part);
  if (!partKey || !list) return;

  list.querySelectorAll(".supplement-input").forEach(input => {
    if (normalizePartKey(input.dataset.part) !== partKey) return;
    if (qty !== null && document.activeElement !== input) {
      input.value = String(qty);
    }
    if (shortageChecked !== null) {
      input.disabled = Boolean(shortageChecked);
      if (shortageChecked) input.value = "0";
    }
  });

  list.querySelectorAll(".shortage-mark").forEach(checkbox => {
    if (normalizePartKey(checkbox.dataset.part) !== partKey) return;
    if (shortageChecked !== null && document.activeElement !== checkbox) {
      checkbox.checked = Boolean(shortageChecked);
    }
  });

  refreshDraftPartTone(list, partKey);
}

function bindDraftPreviewEditors(list) {
  if (!list) return;

  list.querySelectorAll(".shortage-mark").forEach(checkbox => {
    checkbox.addEventListener("change", () => {
      syncDraftPartControls(list, checkbox.dataset.part, {
        shortageChecked: checkbox.checked,
      });
    });
  });

  list.querySelectorAll(".supplement-input").forEach(input => {
    input.addEventListener("input", () => {
      syncDraftPartControls(list, input.dataset.part, {
        qty: parseFloat(input.value) || 0,
      });
    });
  });
}

function buildDraftPreviewRowHtml(row, { editable = false } = {}) {
  const partNumber = String(row.part_number || "");
  const shortageChecked = shouldAutoShortageCheck(row);
  const supplementQty = roundShortageUiValue(row.supplement_qty || 0);
  const shortageAmount = roundShortageUiValue(row.shortage_amount || 0);
  const searchPrimary = [partNumber, row.model || "", row.bom_model || ""].join(" ");
  const searchSecondary = row.description || "";
  const badges = [
    shortageAmount > 0 ? `<span class="draft-preview-badge is-shortage">缺 ${fmt(shortageAmount)}</span>` : "",
    row.decision && !["None", "CreateRequirement"].includes(row.decision)
      ? `<span class="draft-preview-badge is-decision">${esc(draftDecisionLabel(row.decision))}</span>`
      : "",
  ].filter(Boolean).join("");

  return `
    <div
      class="draft-preview-row ${editable ? "is-editable" : ""}"
      data-search="${esc([searchPrimary, searchSecondary].join(" "))}"
      data-search-primary="${esc(searchPrimary)}"
      data-search-secondary="${esc(searchSecondary)}"
    >
      <div class="draft-preview-top">
        <div class="draft-preview-part">${esc(partNumber)}</div>
        ${badges ? `<div class="draft-preview-badges">${badges}</div>` : ""}
      </div>
      <div class="draft-preview-desc">${esc(row.description || "未填說明")}</div>
      <div class="draft-preview-inline-stats">
        ${draftInlineStatHtml("需求", row.needed)}
        ${draftInlineStatHtml("上批餘料", row.carry_over, "is-carry")}
        ${draftInlineStatHtml("補料", supplementQty, "is-supplement")}
      </div>
      ${editable ? `
        <div class="draft-preview-editors">
          <label class="draft-preview-editor-label">補料</label>
          <input
            type="number"
            class="supplement-input"
            data-part="${esc(partNumber)}"
            value="${supplementQty}"
            min="0"
            ${shortageChecked ? "disabled" : ""}
          >
          <label class="draft-preview-check">
            <input
              type="checkbox"
              class="shortage-mark"
              data-part="${esc(partNumber)}"
              ${shortageChecked ? "checked" : ""}
            >
            保留缺料
          </label>
        </div>` : ""}
    </div>`;
}

function buildDraftFileSectionHtml(file, { editable = false } = {}) {
  const rows = Array.isArray(file?.preview_rows) ? file.preview_rows : [];
  const body = rows.length
    ? rows.map(row => buildDraftPreviewRowHtml(row, { editable })).join("")
    : '<div class="merge-draft-empty-note">這份副檔目前沒有可顯示的寫入明細。</div>';
  const sectionSearch = file?.filename || "";

  return `
    <section class="draft-preview-section" data-search="${esc(sectionSearch)}">
      <div class="draft-preview-section-head">
        <div class="draft-preview-section-title">${esc(file.filename || "未命名副檔")}</div>
        <div class="draft-preview-section-meta">寫入列數 ${rows.length}</div>
      </div>
      <div class="draft-preview-section-body">${body}</div>
    </section>`;
}

function buildDraftFileListHtml(files, {
  label = "副檔清單",
  selectable = false,
  draftId = null,
  selectedFileId = null,
} = {}) {
  if (!Array.isArray(files) || !files.length) {
    return '<div class="merge-draft-empty-note">副檔尚未生成</div>';
  }

  const items = selectable
    ? [
      `<button class="merge-draft-file merge-draft-file-select merge-draft-file-all ${selectedFileId ? "" : "is-active"}" type="button" data-draft-id="${Number.isInteger(draftId) ? draftId : ""}" data-file-id="" aria-pressed="${selectedFileId ? "false" : "true"}">全部</button>`,
      ...files.map(file => {
        const fileId = String(file?.id || "").trim();
        const isActive = Boolean(selectedFileId) && fileId === selectedFileId;
        return `<button class="merge-draft-file merge-draft-file-select ${isActive ? "is-active" : ""}" type="button" data-draft-id="${Number.isInteger(draftId) ? draftId : ""}" data-file-id="${esc(fileId)}" aria-pressed="${isActive ? "true" : "false"}" title="${esc(file.filename || "")}">${esc(file.filename || "")}</button>`;
      }),
    ].join("")
    : files.map(file => `<span class="merge-draft-file" title="${esc(file.filename || "")}">${esc(file.filename || "")}</span>`).join("");

  return `
    <div class="${selectable ? "merge-draft-files" : "merge-draft-inline-files"}">
      <div class="merge-draft-files-label">${esc(label)}</div>
      <div class="merge-draft-file-strip">${items}</div>
    </div>`;
}

function normalizeModalSearchQuery(value) {
  return String(value || "").trim().toLowerCase();
}

function getModalSearchableText(node) {
  return normalizeModalSearchQuery(node?.dataset?.search || node?.textContent || "");
}

function tokenizeModalSearchText(value) {
  return normalizeModalSearchQuery(value)
    .split(/[^0-9a-z\u4e00-\u9fff]+/i)
    .map(token => token.trim())
    .filter(Boolean);
}

function matchesModalSearchQuery(node, rawQuery = "") {
  const query = normalizeModalSearchQuery(rawQuery);
  if (!query) return true;

  const primary = normalizeModalSearchQuery(node?.dataset?.searchPrimary || "");
  const secondary = normalizeModalSearchQuery(node?.dataset?.searchSecondary || "");
  const fallback = getModalSearchableText(node);

  if (primary && primary.includes(query)) return true;
  if (!primary && fallback.includes(query)) return true;

  if (!secondary) {
    return primary ? fallback.includes(query) : false;
  }

  const hasAsciiLetter = /[a-z]/i.test(query);
  if (hasAsciiLetter) {
    return secondary.includes(query);
  }

  const secondaryTokens = tokenizeModalSearchText(secondary);
  return secondaryTokens.some(token => token.startsWith(query));
}

function setModalSearchMetaText(text = "") {
  const meta = document.getElementById("modal-search-meta");
  if (meta) meta.textContent = text;
}

function applyModalSearchFilter(rawQuery = "") {
  const list = document.getElementById("modal-shortage-list");
  if (!list) return;

  const query = normalizeModalSearchQuery(rawQuery);
  list.querySelectorAll(".modal-search-empty").forEach(node => node.remove());

  const sectionNodes = [
    ...list.querySelectorAll(".modal-shortage-section"),
    ...list.querySelectorAll(".draft-preview-section"),
  ];

  if (!sectionNodes.length) {
    setModalSearchMetaText("");
    return;
  }

  let totalRows = 0;
  let visibleRows = 0;

  sectionNodes.forEach(section => {
    const rowNodes = [...section.querySelectorAll(".shortage-item, .draft-preview-row")];
    const sectionMatches = query && getModalSearchableText(section).includes(query);
    let sectionVisibleRows = 0;
    totalRows += rowNodes.length;

    rowNodes.forEach(row => {
      const visible = !query || sectionMatches || matchesModalSearchQuery(row, query);
      row.style.display = visible ? "" : "none";
      if (visible) {
        visibleRows += 1;
        sectionVisibleRows += 1;
      }
    });

    section.style.display = (!query || sectionMatches || sectionVisibleRows > 0) ? "" : "none";
  });

  if (query && totalRows > 0 && visibleRows === 0) {
    list.insertAdjacentHTML("beforeend", '<div class="modal-search-empty">找不到符合的料號、說明或機種。</div>');
  }

  if (!totalRows) {
    setModalSearchMetaText(query ? "無可搜尋資料" : "");
    return;
  }
  setModalSearchMetaText(query ? `符合 ${visibleRows} / ${totalRows} 筆` : `共 ${totalRows} 筆`);
}

function configureModalSearch({
  enabled = true,
  placeholder = "搜尋料號 / 說明 / 機種",
} = {}) {
  const wrap = document.getElementById("modal-search-wrap");
  const input = document.getElementById("modal-search-input");
  const clearBtn = document.getElementById("modal-search-clear");
  if (!wrap || !input || !clearBtn) return;

  if (!enabled) {
    wrap.style.display = "none";
    input.value = "";
    input.oninput = null;
    clearBtn.onclick = null;
    setModalSearchMetaText("");
    return;
  }

  wrap.style.display = "flex";
  input.placeholder = placeholder;
  input.value = "";
  input.oninput = () => applyModalSearchFilter(input.value);
  clearBtn.onclick = () => {
    input.value = "";
    applyModalSearchFilter("");
    input.focus();
  };
  applyModalSearchFilter("");
}

async function showDraftModal(draftId, { readOnly = false, fileId = null } = {}) {
  const modal = document.getElementById("shortage-modal");
  const list = document.getElementById("modal-shortage-list");
  const footer = document.getElementById("modal-footer");
  const detail = await apiJson(`/api/schedule/drafts/${draftId}`);
  const draft = detail.draft || {};
  const order = detail.order || {};

  _modalDraftId = draftId;
  _modalDraftReadOnly = Boolean(readOnly);
  _modalTargets = order?.id ? [order] : [];
  _modalCarryOversByModel = {};
  _modalMode = readOnly ? "draft-preview" : "draft-edit";
  _modalPreviewShortages = draft.shortages || [];
  _modalDraftBaseDecisions = normalizeDecisionMap(draft.decisions || {});
  _modalDraftBaseSupplements = normalizeSupplementMap(draft.supplements || {});

  const allFiles = Array.isArray(draft.files) ? draft.files : [];
  const selectedFileId = String(fileId || "").trim();
  const selectedFile = selectedFileId
    ? allFiles.find(file => String(file?.id || "").trim() === selectedFileId) || null
    : null;
  const files = selectedFile ? [selectedFile] : allFiles;
  _modalBomFiles = files;
  _modalDraftVisibleParts = files.flatMap(file => (file?.preview_rows || []).map(row => normalizePartKey(row?.part_number)));
  const fileSummary = files.length
    ? buildDraftFileListHtml(files, { label: selectedFile ? "目前預覽" : "副檔清單" })
    : '<div class="merge-draft-empty-note">這份副檔目前沒有可用檔案。</div>';
  const previewSections = files.length
    ? files.map(file => buildDraftFileSectionHtml(file, { editable: !readOnly })).join("")
    : '<div class="merge-draft-empty-note">目前沒有可預覽的副檔內容。</div>';
  const modalMeta = selectedFile
    ? `指定副檔預覽 1 / 全部 ${allFiles.length} 份，更新 ${esc(formatDraftTime(draft.updated_at) || "--")}`
    : `副檔 ${files.length} 份，更新 ${esc(formatDraftTime(draft.updated_at) || "--")}`;
  const downloadLabel = selectedFile && readOnly ? "下載全部副檔" : "下載副檔";

  list.innerHTML = `
    <div class="merge-draft-modal-head">
      <div class="merge-draft-modal-title">${esc(order.po_number || "")} ${esc(order.model || "")}</div>
      <div class="merge-draft-modal-meta">${modalMeta}</div>
      ${fileSummary}
    </div>
    <section class="draft-preview-wrap">
      <div class="draft-adjust-title">${readOnly ? "副檔寫入明細" : "副檔寫入明細 / 補料調整"}</div>
      ${previewSections}
    </section>
  `;
  configureModalSearch({ placeholder: "搜尋料號 / 說明 / 副檔名稱" });

  if (!readOnly) {
    bindDraftPreviewEditors(list);
  } else {
    list.querySelectorAll(".supplement-input, .shortage-mark").forEach(el => {
      el.disabled = true;
    });
  }
  bindMoqEditors(list);
  bindShortageMoqBadgeEditors(list);

  footer.innerHTML = readOnly
    ? `
      <button id="modal-download-bom" class="btn btn-primary btn-sm">${downloadLabel}</button>
      <button id="modal-cancel" class="btn btn-secondary btn-sm">關閉</button>`
    : `
      <div id="modal-download-progress" class="modal-progress-shell" style="display:none">
        <div class="modal-progress-head">
          <div id="modal-download-status" class="modal-progress-label">正在儲存副檔...</div>
          <div id="modal-download-percent" class="modal-progress-percent">0%</div>
        </div>
        <div id="modal-download-detail" class="modal-progress-detail">會把目前編輯的缺料與補料內容寫回副檔。</div>
        <div class="modal-progress-bar">
          <div id="modal-download-progress-fill" class="modal-progress-fill"></div>
        </div>
      </div>
      <button id="modal-save-draft" class="btn btn-success btn-sm">儲存副檔</button>
      <button id="modal-download-bom" class="btn btn-primary btn-sm">${downloadLabel}</button>
      <button id="modal-cancel" class="btn btn-secondary btn-sm">取消</button>`;

  document.getElementById("modal-save-draft")?.addEventListener("click", async () => {
    try {
      setModalDownloadProgress(true, "正在儲存副檔...", "會把目前編輯的缺料與補料內容寫回副檔。", 14);
      startModalProgressAnimation(92, 180);
      await saveCurrentDraftFromModal({ silent: true });
      setModalDownloadProgress(true, "副檔已更新", "這份副檔的補料調整已經保存。", 100, { tone: "success" });
      await new Promise(resolve => setTimeout(resolve, 180));
      closeShortageModal();
      showToast("副檔已更新");
    } catch (error) {
      setModalDownloadProgress(false, "", "", 0);
      showToast("副檔儲存失敗: " + error.message);
    }
  });
  document.getElementById("modal-download-bom")?.addEventListener("click", async () => {
    try {
      if (!_modalDraftReadOnly) {
        await saveCurrentDraftFromModal({ silent: true });
      }
      await downloadDraft(draftId);
      if (!_modalDraftReadOnly) closeShortageModal();
    } catch (error) {
      showToast("副檔下載失敗: " + error.message);
    }
  });
  document.getElementById("modal-cancel")?.addEventListener("click", closeShortageModal);
  document.getElementById("modal-close").onclick = closeShortageModal;
  modal.style.display = "flex";
}

async function showShortageModal(targets) {
  _modalTargets = targets;
  _modalMode = "download";
  _modalPreviewShortages = [];
  const modal = document.getElementById("shortage-modal");
  const list = document.getElementById("modal-shortage-list");
  const footer = document.getElementById("modal-footer");

  // 查詢對應的 BOM 檔案
  const models = [...new Set(targets.map(t => t.model).filter(Boolean))];
  _modalBomFiles = [];
  if (models.length) {
    try {
      const lookup = await apiPost("/api/bom/lookup", { models });
      _modalBomFiles = lookup.files || [];
    } catch (_) {}
  }

  // 收集勾選訂單的缺料，按機種分組
  const {
    shortagesByScope,
    csShortagesByScope,
    orderedScopes,
  } = buildRawModalShortageGroups(targets);

  // 組內按料號排序
  _modalCarryOversByModel = buildModalCarryOversByModel(targets);

  for (const items of Object.values(shortagesByScope))
    items.sort(compareShortageItems);
  for (const items of Object.values(csShortagesByScope))
    items.sort(compareShortageItems);

  // 從已存的副檔草稿還原上次的 decisions / supplements
  const storedDecisions = {};
  const storedSupplements = {};
  const storedOrderScopedDecisions = {};
  const storedOrderScopedSupplements = {};
  for (const t of targets) {
    const draft = _draftsByOrderId[t.id];
    if (!draft) continue;
    for (const [part, decision] of Object.entries(draft.decisions || {})) {
      const pk = normalizePartKey(part);
      if (!pk || !decision) continue;
      if (isOrderScopedPart(pk)) {
        storedOrderScopedDecisions[buildOrderPartKey(t.id, pk)] = decision;
      } else {
        storedDecisions[pk] = decision;
      }
    }
    for (const [part, qty] of Object.entries(draft.supplements || {})) {
      const pk = normalizePartKey(part);
      const val = Number(qty) || 0;
      if (!pk || val <= 0) continue;
      if (isOrderScopedPart(pk)) {
        storedOrderScopedSupplements[buildOrderPartKey(t.id, pk)] = val;
      } else {
        storedSupplements[pk] = (storedSupplements[pk] || 0) + val;
      }
    }
  }
  _applyStoredToShortages(
    shortagesByScope,
    storedDecisions,
    storedSupplements,
    storedOrderScopedDecisions,
    storedOrderScopedSupplements,
  );
  _applyStoredToShortages(
    csShortagesByScope,
    storedDecisions,
    storedSupplements,
    storedOrderScopedDecisions,
    storedOrderScopedSupplements,
  );

  const visibleScopes = orderedScopes.filter(scope =>
    (shortagesByScope[scope.key] || []).length > 0 || (csShortagesByScope[scope.key] || []).length > 0
  );
  const hasAny = visibleScopes.length > 0;

  let html = "";

  if (!hasAny) {
    html += `<div style="text-align:center;padding:24px;color:#16a34a;font-weight:600">
      全部 OK，無缺料！可直接扣帳。</div>`;
  } else {
    for (const scope of visibleScopes) {
      html += `<section class="modal-shortage-section" data-search="${esc([scope.label, scope.po_number || ""].join(" "))}">`;
      html += shortageGroupHeadingHtml(scope.label, scope.po_number);
      const csItems = csShortagesByScope[scope.key] || [];
      const items = shortagesByScope[scope.key] || [];
      if (csItems.length) {
        html += '<div style="margin-bottom:8px"><h4 style="font-size:12px;color:#ca8a04;margin:4px 0">客供料</h4>';
        html += csItems.map(s => modalShortageItem(s, true)).join("");
        html += '</div>';
      }
      if (items.length) {
        html += '<h4 style="font-size:12px;color:#dc2626;margin:4px 0">採購缺料</h4>';
        html += items.map(s => modalShortageItem(s, false)).join("");
      }
      html += "</section>";
    }
  }

  list.innerHTML = html;
  configureModalSearch({ placeholder: "搜尋料號 / 說明 / 機種" });
  bindShortageEditors(list);

  // 重設 footer
  bindMoqEditors(list);
  bindShortageMoqBadgeEditors(list);

  footer.innerHTML = `
    <div id="modal-download-progress" class="modal-progress-shell" style="display:none">
      <div class="modal-progress-head">
        <div id="modal-download-status" class="modal-progress-label">正在整理補料資料...</div>
        <div id="modal-download-percent" class="modal-progress-percent">0%</div>
      </div>
      <div id="modal-download-detail" class="modal-progress-detail">大型 BOM 會需要幾秒鐘，請稍候。</div>
      <div class="modal-progress-bar">
        <div id="modal-download-progress-fill" class="modal-progress-fill"></div>
      </div>
    </div>
    <button id="modal-download-bom" class="btn btn-primary btn-sm">確認補料並下載 BOM</button>
    <button id="modal-cancel" class="btn btn-secondary btn-sm">取消</button>`;
  document.getElementById("modal-download-bom").onclick = handleModalDownloadBom;
  document.getElementById("modal-cancel").onclick = closeShortageModal;
  document.getElementById("modal-close").onclick = closeShortageModal;
  modal.style.display = "flex";
}

async function saveBatchDraftsFromModal({ silent = false } = {}) {
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  if (!targetOrderIds.length) return null;

  const supplements = _collectModalSupplements();
  const decisions = _collectModalDecisions();
  const orderSupplements = _collectModalOrderSupplements();
  const orderDecisions = _collectModalOrderDecisions();
  await persistDecisionsForOrders(decisions, targetOrderIds, orderDecisions);
  Object.entries(decisions).forEach(([part, decision]) => {
    setLocalDecision(part, decision);
  });

  const response = await apiPut("/api/schedule/drafts", {
    order_ids: targetOrderIds,
    decisions,
    supplements,
    order_decisions: orderDecisions,
    order_supplements: orderSupplements,
  });
  await refresh();
  if (!silent) showToast("補料已寫入副檔");
  return response?.drafts || null;
}

async function handleModalSaveDrafts() {
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  if (!targetOrderIds.length) {
    showToast("找不到要儲存補料的訂單");
    return;
  }

  try {
    setModalDownloadProgress(true, "正在保存補料到副檔...", "補料完成後，副檔會顯示在該機種下方，之後再按發料即可。", 14);
    startModalProgressAnimation(94, 180);
    await saveBatchDraftsFromModal({ silent: true });
    setModalDownloadProgress(true, "副檔已更新", "現在可以在機種下方預覽、編輯、下載，或最後再發料。", 100);
    await new Promise(resolve => setTimeout(resolve, 180));
    closeShortageModal();
    showToast("補料已寫入副檔，請在訂單下方確認後再發料");
  } catch (error) {
    showToast("保存補料失敗: " + error.message);
    setModalDownloadProgress(false, "", "", 0);
  }
}

async function handleModalDownloadDrafts() {
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  if (!targetOrderIds.length) {
    showToast("找不到可下載副檔的訂單");
    return;
  }

  try {
    setModalDownloadProgress(true, "正在保存補料到副檔...", "會把這次補料與缺料決策正式寫進副檔。", 12);
    startModalProgressAnimation(46, 140);
    await saveBatchDraftsFromModal({ silent: true });

    setModalDownloadProgress(true, "正在下載副檔...", `共 ${targetOrderIds.length} 筆訂單，會依目前副檔內容輸出。`, 58);
    startModalProgressAnimation(92, 220);
    const result = await desktopDownload({
      path: "/api/schedule/drafts/download",
      method: "POST",
      body: { order_ids: targetOrderIds },
    });
    showDownloadToast(result, "副檔");

    setModalDownloadProgress(true, "副檔下載完成", "補料內容已保存，後續也能在機種下方繼續預覽與編輯。", 100);
    await new Promise(resolve => setTimeout(resolve, 220));
    closeShortageModal();
  } catch (error) {
    showToast("副檔下載失敗: " + error.message);
    setModalDownloadProgress(false, "", "", 0);
  }
}

async function showBatchMergeDraftModal(targets) {
  _modalTargets = targets;
  _modalBomFiles = [];
  _modalCarryOversByModel = {};
  _modalMode = "download";
  _modalPreviewShortages = [];
  _modalDraftId = null;
  _modalDraftReadOnly = false;
  _modalDraftBaseDecisions = {};
  _modalDraftBaseSupplements = {};
  _modalDraftVisibleParts = [];

  const modal = document.getElementById("shortage-modal");
  const list = document.getElementById("modal-shortage-list");
  const footer = document.getElementById("modal-footer");
  if (!modal || !list || !footer) {
    console.error("[showBatchMergeDraftModal] DOM elements missing:", { modal: !!modal, list: !!list, footer: !!footer });
    throw new Error("補料 modal DOM 元素遺失");
  }
  const {
    shortagesByScope,
    csShortagesByScope,
    orderedScopes,
  } = buildRawModalShortageGroups(targets);

  for (const items of Object.values(shortagesByScope)) items.sort(compareShortageItems);
  for (const items of Object.values(csShortagesByScope)) items.sort(compareShortageItems);

  // 從已存的副檔草稿載入上次的 decisions / supplements，下次開 modal 直接還原
  const storedDecisions = {};
  const storedSupplements = {};
  const storedOrderScopedDecisions = {};
  const storedOrderScopedSupplements = {};
  for (const t of targets) {
    const draft = _draftsByOrderId[t.id];
    if (!draft) continue;
    for (const [part, decision] of Object.entries(draft.decisions || {})) {
      const pk = normalizePartKey(part);
      if (!pk || !decision) continue;
      if (isOrderScopedPart(pk)) {
        storedOrderScopedDecisions[buildOrderPartKey(t.id, pk)] = decision;
      } else {
        storedDecisions[pk] = decision;
      }
    }
    for (const [part, qty] of Object.entries(draft.supplements || {})) {
      const pk = normalizePartKey(part);
      const val = Number(qty) || 0;
      if (!pk || val <= 0) continue;
      if (isOrderScopedPart(pk)) {
        storedOrderScopedSupplements[buildOrderPartKey(t.id, pk)] = val;
      } else {
        storedSupplements[pk] = (storedSupplements[pk] || 0) + val;
      }
    }
  }
  _applyStoredToShortages(
    shortagesByScope,
    storedDecisions,
    storedSupplements,
    storedOrderScopedDecisions,
    storedOrderScopedSupplements,
  );
  _applyStoredToShortages(
    csShortagesByScope,
    storedDecisions,
    storedSupplements,
    storedOrderScopedDecisions,
    storedOrderScopedSupplements,
  );
  const visibleScopes = orderedScopes.filter(scope =>
    (shortagesByScope[scope.key] || []).length > 0 || (csShortagesByScope[scope.key] || []).length > 0
  );

  let html = "";
  if (!visibleScopes.length) {
    html = `<div style="text-align:center;padding:24px;color:#16a34a;font-weight:600">
      目前沒有缺料，確認後仍會建立可預覽、可編輯的副檔。
    </div>`;
  } else {
    for (const scope of visibleScopes) {
      html += `<section class="modal-shortage-section" data-search="${esc([scope.label, scope.po_number || ""].join(" "))}">`;
      html += shortageGroupHeadingHtml(scope.label, scope.po_number);
      const csItems = csShortagesByScope[scope.key] || [];
      const items = shortagesByScope[scope.key] || [];
      if (csItems.length) {
        html += '<div style="margin-bottom:8px"><h4 style="font-size:12px;color:#ca8a04;margin:4px 0">客供料</h4>';
        html += csItems.map(item => modalShortageItem(item, true)).join("");
        html += "</div>";
      }
      if (items.length) {
        html += '<h4 style="font-size:12px;color:#dc2626;margin:4px 0">採購缺料</h4>';
        html += items.map(item => modalShortageItem(item, false)).join("");
      }
      html += "</section>";
    }
  }

  list.innerHTML = html;
  configureModalSearch({ placeholder: "搜尋料號 / 說明 / 機種" });
  bindShortageEditors(list);

  bindMoqEditors(list);
  bindShortageMoqBadgeEditors(list);

  footer.innerHTML = `
    <div id="modal-download-progress" class="modal-progress-shell" style="display:none">
      <div class="modal-progress-head">
        <div id="modal-download-status" class="modal-progress-label">正在準備副檔...</div>
        <div id="modal-download-percent" class="modal-progress-percent">0%</div>
      </div>
      <div id="modal-download-detail" class="modal-progress-detail">補料確認後，副檔會顯示在該機種下方。</div>
      <div class="modal-progress-bar">
        <div id="modal-download-progress-fill" class="modal-progress-fill"></div>
      </div>
    </div>
    <button id="modal-save-draft" class="btn btn-success btn-sm">確認補料</button>
    <button id="modal-download-bom" class="btn btn-primary btn-sm">確認補料並下載副檔</button>
    <button id="modal-cancel" class="btn btn-secondary btn-sm">取消</button>`;
  document.getElementById("modal-save-draft").onclick = handleModalSaveDrafts;
  document.getElementById("modal-download-bom").onclick = handleModalDownloadDrafts;
  document.getElementById("modal-cancel").onclick = closeShortageModal;
  document.getElementById("modal-close").onclick = closeShortageModal;
  modal.style.display = "flex";
}

async function showWriteToMainModal(targets) {
  _modalTargets = targets;
  _modalMode = "write";

  const modal = document.getElementById("shortage-modal");
  const list = document.getElementById("modal-shortage-list");
  const footer = document.getElementById("modal-footer");
  const targetOrderIds = (targets || []).map(item => item.id).filter(Number.isInteger);
  const models = [...new Set((targets || []).map(item => item.model).filter(Boolean))];

  _modalBomFiles = [];
  if (models.length) {
    try {
      const lookup = await apiPost("/api/bom/lookup", { models });
      _modalBomFiles = lookup.files || [];
    } catch (_) {}
  }

  _modalCarryOversByModel = buildModalCarryOversByModel(targets);

  const preview = await apiPost("/api/schedule/main-write-preview", {
    order_ids: targetOrderIds,
    decisions: _decisions,
  });
  _modalPreviewShortages = preview.shortages || [];

  const shortagesByScope = {};
  const orderedScopes = buildShortageScopeList(targets);
  const targetByOrderId = new Map(
    (targets || [])
      .map(target => [normalizeOrderId(target?.id), target])
      .filter(([orderId]) => Number.isInteger(orderId))
  );
  _modalPreviewShortages.forEach(item => {
    const target = targetByOrderId.get(normalizeOrderId(item?.order_id)) || {};
    const meta = buildShortageGroupMeta(target, {
      ...item,
      model: item.model || item.bom_model || target?.model || "未指定機種",
      _row_code: item.batch_code || target?.code || item.model || item.bom_model || "未指定機種",
      _po_number: target?.po_number || item?.po_number || "",
    });
    if (!shortagesByScope[meta._row_group_key]) shortagesByScope[meta._row_group_key] = [];
    shortagesByScope[meta._row_group_key].push({
      ...item,
      ...meta,
    });
  });
  for (const items of Object.values(shortagesByScope)) items.sort(compareShortageItems);

  const visibleScopes = orderedScopes.filter(scope => (shortagesByScope[scope.key] || []).length > 0);
  const totalShortageCount = _modalPreviewShortages.length;
  let html = "";
  if (!visibleScopes.length) {
    html = `<div style="text-align:center;padding:24px;color:#16a34a;font-weight:600">
      模擬寫入主檔後沒有剩餘缺料，可以直接寫入主檔。</div>`;
  } else {
    html += `<div style="padding:10px 14px;margin-bottom:8px;background:#fef2f2;border:1px solid #fecaca;border-radius:8px;color:#dc2626;font-weight:600;font-size:13px">
      ⚠ 寫入後將有 ${totalShortageCount} 筆料號缺料，需在右側面板手動補料</div>`;
    for (const scope of visibleScopes) {
      html += `<section class="modal-shortage-section" data-search="${esc([scope.label, scope.po_number || ""].join(" "))}">`;
      html += shortageGroupHeadingHtml(scope.label, scope.po_number);
      html += '<h4 style="font-size:12px;color:#dc2626;margin:4px 0">寫入主檔後仍缺料</h4>';
      html += (shortagesByScope[scope.key] || []).map(item => modalShortageItem(item, false)).join("");
      html += "</section>";
    }
  }

  list.innerHTML = html;
  configureModalSearch({ placeholder: "搜尋料號 / 說明 / 機種" });
  bindShortageEditors(list);

  bindMoqEditors(list);
  bindShortageMoqBadgeEditors(list);

  footer.innerHTML = `
    <div id="modal-download-progress" class="modal-progress-shell" style="display:none">
      <div class="modal-progress-head">
        <div id="modal-download-status" class="modal-progress-label">正在準備寫入主檔...</div>
        <div id="modal-download-percent" class="modal-progress-percent">0%</div>
      </div>
      <div id="modal-download-detail" class="modal-progress-detail">這份明細是模擬寫入主檔後，仍然會缺料的項目。</div>
      <div class="modal-progress-bar">
        <div id="modal-download-progress-fill" class="modal-progress-fill"></div>
      </div>
    </div>
    <button id="modal-write-main" class="btn btn-success btn-sm">寫入主檔</button>
    <button id="modal-cancel" class="btn btn-secondary btn-sm">取消</button>`;
  document.getElementById("modal-write-main").onclick = handleModalWriteMain;
  document.getElementById("modal-cancel").onclick = closeShortageModal;
  document.getElementById("modal-close").onclick = closeShortageModal;
  modal.style.display = "flex";
}

function computeShortageResultingStock(shortage, supplementQty = undefined) {
  const currentStock = Number(shortage?.current_stock);
  const prevQtyCs = Number(shortage?.prev_qty_cs || 0);
  const neededQty = Number(shortage?.needed);
  const effectiveSupplement = supplementQty !== undefined
    ? Number(supplementQty)
    : Number(
      Number(shortage?.default_supplement) > 0
        ? shortage?.default_supplement
        : shortage?.supplement_qty || 0
    );

  if ([currentStock, prevQtyCs, neededQty, effectiveSupplement].every(Number.isFinite)) {
    return currentStock + prevQtyCs + effectiveSupplement - neededQty;
  }

  const explicitResulting = Number(shortage?.resulting_stock);
  return Number.isFinite(explicitResulting) ? explicitResulting : NaN;
}

function isShortageStillNegative(shortage) {
  const resultingStock = computeShortageResultingStock(shortage);
  return Number.isFinite(resultingStock) && resultingStock < 0;
}

function shortageToneClass(shortage) {
  const classNames = ["shortage-item", "modal-shortage-item"];
  if (isShortageStillNegative(shortage)) {
    classNames.push("is-negative-after-supplement");
  } else {
    classNames.push("is-resolved-after-supplement");
  }
  return classNames.join(" ");
}

function shouldAutoShortageCheck(item) {
  const decision = String(item?.decision || "").trim();
  if (decision === "Shortage") return true;
  if (decision && decision !== "None") return false;
  if (isOrderScopedPart(item?.part_number)) return false;

  const carryOver = Number(item?.carry_over);
  if (Number.isFinite(carryOver) && carryOver < 0) return true;

  const currentStock = Number(item?.current_stock);
  return Number.isFinite(currentStock) && currentStock < 0;
}

function modalShortageItem(s, isCS) {
  const codeTag = s._row_code ? `<span class="tag tag-pcb" style="font-size:10px;padding:1px 6px;margin-left:4px">${esc(s._row_code)}</span>` : "";
  const csTag = isCS ? '<span class="tag tag-cs">客供</span>' : "";
  const orderIdAttr = Number.isInteger(normalizeOrderId(s._order_id)) ? ` data-order-id="${normalizeOrderId(s._order_id)}"` : "";
  const searchText = [
    s.part_number || "",
    s.description || "",
    s._row_code || "",
    s._row_model || "",
    s._row_group_label || "",
    s._po_number ? `PO ${s._po_number}` : "",
  ].join(" ");
  const shortageChecked = shouldAutoShortageCheck(s);
  const hasStoredSupplement = Number(s.default_supplement) > 0 || Number(s.supplement_qty) > 0;
  const defaultQty = shortageChecked && !hasStoredSupplement
    ? 0
    : roundShortageUiValue(
      Number(s.default_supplement) > 0
        ? s.default_supplement
        : Number(s.supplement_qty) > 0
          ? s.supplement_qty
          : Number(s.suggested_qty) > 0
            ? s.suggested_qty
            : s.shortage_amount ?? 0
    );
  const shortageAmount = roundShortageUiValue(s.shortage_amount);
  const currentStock = roundShortageUiValue(s.current_stock);
  const neededQty = roundShortageUiValue(s.needed);
  const stAvailableQty = roundShortageUiValue(s.st_available_qty || 0);
  const purchaseNeededQty = roundShortageUiValue(s.purchase_needed_qty || 0);
  const resultingStock = computeShortageResultingStock(s, defaultQty);
  s = {
    ...s,
    shortage_amount: shortageAmount,
    current_stock: currentStock,
    needed: neededQty,
    st_available_qty: stAvailableQty,
    purchase_needed_qty: purchaseNeededQty,
    default_supplement: defaultQty,
    supplement_qty: defaultQty,
    resulting_stock: resultingStock,
  };

  return `<div class="${shortageToneClass(s)}" style="margin-bottom:8px" data-part="${esc(s.part_number)}" data-current-stock="${esc(s.current_stock)}" data-prev-qty-cs="${esc(s.prev_qty_cs || 0)}" data-needed="${esc(s.needed)}" data-search="${esc(searchText)}"${orderIdAttr}>
    <div style="display:flex;align-items:center;gap:6px;font-weight:600;font-size:13px">${s.part_number}${codeTag}${csTag}</div>
    <div style="font-size:11px;color:#6b7280">${s.description || "—"}</div>
    <div style="font-size:12px;display:flex;gap:10px;margin:4px 0">
      <span style="color:#dc2626">缺 ${fmt(s.shortage_amount)}</span>
      <span style="color:#16a34a">庫存 ${fmt(s.current_stock)}</span>
      <span>需 ${fmt(s.needed)}</span>
      ${stSupplySummaryHtml(s)}
      ${moqBadgeHtml(s)}
      ${moqEditTriggerHtml(s)}
    </div>
    ${missingMoqEditorHtml(s)}
    ${isCS ? '<div style="font-size:11px;color:#ca8a04">請通知客戶提供此料</div>' : `
    <div style="display:flex;align-items:center;gap:8px;margin-top:4px">
      <label style="font-size:12px;color:#374151;white-space:nowrap">補料:</label>
      <input type="number" class="supplement-input" data-part="${s.part_number}"${orderIdAttr} value="${defaultQty}" min="0" ${shortageChecked ? "disabled" : ""}
             style="width:80px;padding:2px 6px;border:1px solid #d1d5db;border-radius:4px;font-size:12px;text-align:right">
      <label style="font-size:12px;display:flex;align-items:center;gap:4px;color:#dc2626;cursor:pointer;white-space:nowrap">
        <input type="checkbox" class="shortage-mark" data-part="${s.part_number}"${orderIdAttr} ${shortageChecked ? "checked" : ""}> 缺料
      </label>
    </div>`}
  </div>`;
}

function computeModalCardResultingStock(card) {
  if (!card) return Number.NaN;
  const currentStock = Number(card.dataset.currentStock);
  const prevQtyCs = Number(card.dataset.prevQtyCs || 0);
  const neededQty = Number(card.dataset.needed);
  const input = card.querySelector(".supplement-input");
  const checkbox = card.querySelector(".shortage-mark");
  const supplementQty = checkbox?.checked ? 0 : Number(input?.value || 0);
  if ([currentStock, prevQtyCs, neededQty, supplementQty].every(Number.isFinite)) {
    return currentStock + prevQtyCs + supplementQty - neededQty;
  }
  return Number.NaN;
}

function updateModalShortageTone(card) {
  if (!card) return;
  const resultingStock = computeModalCardResultingStock(card);
  const isNegative = Number.isFinite(resultingStock) && resultingStock < 0;
  card.classList.toggle("is-negative-after-supplement", isNegative);
  card.classList.toggle("is-resolved-after-supplement", !isNegative);
}

function refreshDraftPartTone(list, part) {
  const partKey = normalizePartKey(part);
  if (!partKey || !list) return;
  list.querySelectorAll(".shortage-item[data-part]").forEach(card => {
    if (normalizePartKey(card.dataset.part) !== partKey) return;
    updateModalShortageTone(card);
  });
}

function closeShortageModal() {
  stopModalProgressAnimation();
  setModalDownloadProgress(false, "", "", 0);
  const modal = document.getElementById("shortage-modal");
  const list = document.getElementById("modal-shortage-list");
  const footer = document.getElementById("modal-footer");
  if (modal) modal.style.display = "none";
  if (list) list.innerHTML = "";
  if (footer) footer.innerHTML = "";
  configureModalSearch({ enabled: false });
  _modalTargets = [];
  _modalBomFiles = [];
  _modalCarryOversByModel = {};
  _modalMode = "download";
  _modalPreviewShortages = [];
  _modalDraftId = null;
  _modalDraftReadOnly = false;
  _modalDraftBaseDecisions = {};
  _modalDraftBaseSupplements = {};
  _modalDraftVisibleParts = [];
}

function hasMoqValue(shortage) {
  return Number(shortage?.moq || 0) > 0;
}

function roundShortageUiValue(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return Math.round(num);
}

function moqBadgeHtml(shortage) {
  const partNumber = esc(shortage?.part_number || "");
  const rawMoq = Number(shortage?.moq || 0);
  if (hasMoqValue(shortage)) {
    return `<span class="moq-badge moq-badge-present moq-badge-editable" data-part="${partNumber}" data-moq="${rawMoq}" title="雙擊可編輯 MOQ">MOQ ${fmt(roundShortageUiValue(shortage.moq))}</span>`;
  }
  return `<span class="moq-badge moq-badge-missing moq-badge-editable" data-part="${partNumber}" data-moq="0" title="雙擊可編輯 MOQ">未寫 MOQ</span>`;
}

function moqEditTriggerHtml(shortage) {
  const partNumber = esc(shortage?.part_number || "");
  const rawMoq = Number(shortage?.moq || 0);
  return `<button type="button" class="moq-edit-trigger" data-part="${partNumber}" data-moq="${rawMoq}" title="編輯 MOQ" aria-label="編輯 MOQ">編</button>`;
}

function suggestedQtyHtml(shortage) {
  shortage = { ...shortage, moq: roundShortageUiValue(shortage.moq) };
  const suggested = roundShortageUiValue(shortage.suggested_qty || shortage.shortage_amount || 0);
  const purchaseNeeded = roundShortageUiValue(
    shortage.purchase_suggested_qty || shortage.purchase_needed_qty || 0
  );
  if (purchaseNeeded > 0) {
    if (hasMoqValue(shortage)) {
      return `<span class="amber">建議補 ${fmt(suggested)}（其中需買 ${fmt(purchaseNeeded)}，MOQ ${fmt(shortage.moq)}）</span>`;
    }
    return `<span class="amber">建議補 ${fmt(suggested)}（其中需買 ${fmt(purchaseNeeded)}，未寫 MOQ）</span>`;
  }
  if (hasMoqValue(shortage)) {
    return `<span class="blue">建議補 ${fmt(suggested)}（MOQ ${fmt(shortage.moq)}）</span>`;
  }
  return `<span class="amber">建議補 ${fmt(suggested)}（未寫 MOQ）</span>`;
}

function stSupplySummaryHtml(shortage) {
  const stAvailable = roundShortageUiValue(shortage?.st_available_qty || 0);
  const purchaseNeeded = roundShortageUiValue(
    shortage?.purchase_suggested_qty || shortage?.purchase_needed_qty || 0
  );
  const summary = [];
  if (stAvailable > 0) {
    summary.push(`<span class="blue">ST 可補 ${fmt(stAvailable)}</span>`);
  }
  if (purchaseNeeded > 0) {
    summary.push(`<span class="amber">需買 ${fmt(purchaseNeeded)}</span>`);
  }
  return summary.join("");
}

function missingMoqEditorHtml(shortage) {
  if (hasMoqValue(shortage)) return "";
  return `
    <div class="moq-editor" style="display:flex;align-items:center;gap:8px;margin-top:6px">
      <label style="font-size:12px;color:#fbbf24;white-space:nowrap">MOQ:</label>
      <input type="number" class="moq-input" data-part="${shortage.part_number}" min="0" step="0.01"
             placeholder="輸入 MOQ"
             style="width:96px;padding:2px 6px;border:1px solid #f59e0b;border-radius:4px;font-size:12px;text-align:right;background:#2a2a2a;color:#fff">
      <button type="button" class="save-moq-btn"
              style="border:1px solid #f59e0b;background:#78350f;color:#fef3c7;border-radius:4px;padding:3px 8px;font-size:12px;cursor:pointer;white-space:nowrap">
        記住 MOQ
      </button>
    </div>`;
}

async function saveManualMoq(partNumber, input, button) {
  const key = normalizePartKey(partNumber);
  const moqValue = parseFloat(input?.value ?? "");
  if (!key) {
    showToast("料號不可空白");
    return;
  }
  if (!Number.isFinite(moqValue) || moqValue <= 0) {
    showToast("MOQ 請輸入大於 0 的數字");
    input?.focus();
    input?.select();
    return;
  }

  if (button) {
    button.disabled = true;
    button.textContent = "儲存中...";
  }

  try {
    await apiPatch("/api/main-file/moq", { part_number: key, moq: moqValue });
    _moq[key] = moqValue;
    recalculate();
    updateStatusOnly();
    if (_modalTargets.length && document.getElementById("shortage-modal")?.style.display === "flex") {
      await showShortageModal(_modalTargets);
    }
    showToast(`${key} MOQ 已儲存`);
  } catch (e) {
    showToast("MOQ 儲存失敗: " + e.message);
    if (button) {
      button.disabled = false;
      button.textContent = "記住 MOQ";
    }
  }
}

async function handleCommitDraft(draftId, model) {
  if (!confirm(`確認要依照 ${model} 的副檔寫入主檔嗎？`)) return;

  try {
    const result = await apiPost(`/api/schedule/drafts/${draftId}/commit`);
    showToast(`已依副檔寫入主檔，merge ${result.merged_parts} 筆`, { tone: "success" });
    _checkedIds.delete(result.order_id);
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
    if (result.shortages?.length) {
      showPostDispatchShortages(result.shortages);
    }
  } catch (error) {
    showToast("副檔提交失敗: " + error.message, { tone: "error" });
  }
}

async function downloadDraft(draftId, fileId = null) {
  try {
    const query = fileId ? `?file_id=${encodeURIComponent(fileId)}` : "";
    const result = await desktopDownload({ path: `/api/schedule/drafts/${draftId}/download${query}` });
    showDownloadToast(result, "副檔");
  } catch (error) {
    showToast("副檔下載失敗: " + error.message);
  }
}

async function handleDeleteDraftLegacyBroken(draftId, model) {
  if (!confirm(`確認要刪除 ${model} 的副檔嗎？`)) return;
  try {
    await apiFetch(`/api/schedule/drafts/${draftId}`, { method: "DELETE" });
    showToast("副檔已刪除");
    await refresh();
    showToast(`已建立 ${result.draft_count || 0} 份副檔，請在訂單下方確認後再按勾寫入主檔`);
  } catch (error) {
    showToast("副檔刪除失敗: " + error.message);
  }
}

function buildDraftPanelHtmlLegacyV2(draft) {
  const files = draft?.files || [];
  const shortages = draft?.shortages || [];
  const updatedAt = formatDraftTime(draft?.updated_at);
  const fileHtml = files.length
    ? files.map(file => `<span class="merge-draft-file" title="${esc(file.filename || "")}">${esc(file.filename || "")}</span>`).join("")
    : '<span class="merge-draft-file merge-draft-file-empty">副檔尚未生成</span>';

  return `
    <div class="merge-draft-panel">
      <div class="merge-draft-summary">
        <span class="merge-draft-pill">副檔 ${files.length} 份</span>
        <span class="merge-draft-meta">缺料 ${shortages.length} 筆</span>
        ${updatedAt ? `<span class="merge-draft-meta">更新 ${esc(updatedAt)}</span>` : ""}
      </div>
      <div class="merge-draft-files">${fileHtml}</div>
      <div class="merge-draft-actions">
        <button class="btn btn-secondary btn-sm btn-draft-preview" data-draft-id="${draft.id}">預覽</button>
        <button class="btn btn-secondary btn-sm btn-draft-edit" data-draft-id="${draft.id}">修改</button>
        <button class="btn btn-secondary btn-sm btn-draft-download" data-draft-id="${draft.id}">下載</button>
        <button class="btn btn-secondary btn-sm btn-draft-delete" data-draft-id="${draft.id}">刪除</button>
      </div>
    </div>`;
}

async function handleDeleteDraftLegacyV2(draftId, model) {
  if (!confirm(`確認要刪除 ${model} 的副檔嗎？`)) return;
  try {
    await apiFetch(`/api/schedule/drafts/${draftId}`, { method: "DELETE" });
    showToast("副檔已刪除");
    await refresh();
  } catch (error) {
    showToast("副檔刪除失敗: " + error.message);
  }
}

async function handleBatchDispatchLegacyV1() {
  const targets = _rows.filter(r => _checkedIds.has(r.id) && (r.status === "pending" || r.status === "merged"));
  if (!_checkedIds.size) { showToast("請先勾選要發料的訂單"); return; }
  if (!targets.length) { showToast("勾選的訂單中沒有可發料的"); return; }

  const button = document.getElementById("btn-batch-dispatch");
  const originalText = button?.textContent || "批次發料";
  const preview = targets.slice(0, 6).map(item => `${item.po_number} ${item.model}`).join("\n");
  const extra = targets.length > 6 ? `\n... 另 ${targets.length - 6} 筆` : "";
  const confirmed = confirm(`確定要批次發料 ${targets.length} 筆訂單嗎？\n${preview}${extra}`);
  if (!confirmed) return;
  await showWriteToMainModal(targets);
  return;

  try {
    if (button) {
      button.disabled = true;
      button.textContent = "發料中...";
    }
    const result = await apiPost("/api/schedule/batch-dispatch", {
      order_ids: targets.map(item => item.id),
      decisions: _decisions,
    });
    targets.forEach(item => _checkedIds.delete(item.id));
    showToast(`已批次發料 ${result.count} 筆\n共 Merge ${result.merged_parts} 筆料號`);
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
  } catch (error) {
    showToast("批次發料失敗：" + error.message);
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
}

async function handleShortageBadgeMoqEdit(badge) {
  const partNumber = normalizePartKey(badge?.dataset.part);
  if (!partNumber) {
    showToast("料號不可空白");
    return;
  }

  if (badge.dataset.editing === "1") return;

  const currentMoq = Number(badge?.dataset.moq || 0);
  badge.dataset.editing = "1";
  const originalHtml = badge.innerHTML;
  badge.innerHTML = `<input type="number" class="moq-inline-input" min="0" step="0.01" value="${currentMoq > 0 ? currentMoq : ""}" placeholder="MOQ">`;

  const input = badge.querySelector(".moq-inline-input");
  if (!input) {
    delete badge.dataset.editing;
    badge.innerHTML = originalHtml;
    return;
  }

  requestAnimationFrame(() => {
    input.focus();
    input.select();
  });

  let finished = false;
  const cancel = () => {
    if (finished) return;
    finished = true;
    delete badge.dataset.editing;
    badge.innerHTML = originalHtml;
  };

  const save = async () => {
    if (finished) return;
    const rawValue = String(input.value || "").trim();
    if (!rawValue) {
      cancel();
      return;
    }

    const moqValue = parseFloat(rawValue);
    if (!Number.isFinite(moqValue) || moqValue <= 0) {
      showToast("MOQ 請輸入大於 0 的數字");
      input.focus();
      input.select();
      return;
    }

    finished = true;
    try {
      await apiPatch("/api/main-file/moq", { part_number: partNumber, moq: moqValue });
      _moq[partNumber] = moqValue;
      recalculate();
      updateStatusOnly();
      showToast(`${partNumber} MOQ 已儲存`);
    } catch (e) {
      delete badge.dataset.editing;
      badge.innerHTML = originalHtml;
      showToast("MOQ 儲存失敗: " + e.message);
    }
  };

  input.addEventListener("click", event => event.stopPropagation());
  input.addEventListener("dblclick", event => event.stopPropagation());
  input.addEventListener("keydown", event => {
    if (event.key === "Enter") {
      event.preventDefault();
      void save();
    } else if (event.key === "Escape") {
      event.preventDefault();
      cancel();
    }
  });
  input.addEventListener("blur", () => { void save(); });
}

function bindShortageMoqBadgeEditors(root) {
  if (!root) return;
  root.querySelectorAll(".moq-badge-editable").forEach(badge => {
    if (badge.dataset.moqBadgeBound === "1") return;
    badge.dataset.moqBadgeBound = "1";
    badge.addEventListener("dblclick", event => {
      event.preventDefault();
      event.stopPropagation();
      void handleShortageBadgeMoqEdit(badge);
    });
  });
  root.querySelectorAll(".moq-edit-trigger").forEach(button => {
    if (button.dataset.moqButtonBound === "1") return;
    button.dataset.moqButtonBound = "1";
    button.addEventListener("click", event => {
      event.preventDefault();
      event.stopPropagation();
      const badge = button.parentElement?.querySelector(".moq-badge-editable");
      if (!badge) return;
      void handleShortageBadgeMoqEdit(badge);
    });
  });
}

function bindMoqEditors(root) {
  if (!root) return;

  root.querySelectorAll(".save-moq-btn").forEach(button => {
    button.addEventListener("click", async () => {
      const wrapper = button.closest(".moq-editor");
      const input = wrapper?.querySelector(".moq-input");
      await saveManualMoq(input?.dataset.part, input, button);
    });
  });

  root.querySelectorAll(".moq-input").forEach(input => {
    input.addEventListener("keydown", event => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      input.closest(".moq-editor")?.querySelector(".save-moq-btn")?.click();
    });
  });
}

function _getModalRowMeta(element) {
  const container = element?.closest(".shortage-item");
  const orderId = normalizeOrderId(
    element?.dataset.orderId
    || container?.dataset.orderId
  );
  const part = normalizePartKey(element?.dataset.part || container?.querySelector(".supplement-input")?.dataset.part);
  return { container, orderId, part };
}

function _collectModalDecisions() {
  const list = document.getElementById("modal-shortage-list");
  if (!list) return {};
  const decisions = {};

  list.querySelectorAll(".supplement-input").forEach(input => {
    const { part } = _getModalRowMeta(input);
    if (!part || isOrderScopedPart(part)) return;

    const qty = parseFloat(input.value) || 0;
    const isShortage = input.closest(".draft-preview-row, .shortage-item")?.querySelector(".shortage-mark")?.checked
      || Array.from(list.querySelectorAll(".shortage-mark")).some(checkbox => normalizePartKey(checkbox.dataset.part) === part && checkbox.checked);

    if (isShortage) {
      decisions[part] = "Shortage";
    } else if (qty > 0) {
      decisions[part] = "CreateRequirement";
    } else {
      decisions[part] = "None";
    }
  });

  return decisions;
}

function _collectModalSupplements() {
  const supplements = {};
  const list = document.getElementById("modal-shortage-list");
  if (!list) return supplements;

  list.querySelectorAll(".supplement-input").forEach(input => {
    const { part } = _getModalRowMeta(input);
    if (!part) return;

    const qty = parseFloat(input.value) || 0;
    const isShortage = input.closest(".draft-preview-row, .shortage-item")?.querySelector(".shortage-mark")?.checked
      || Array.from(list.querySelectorAll(".shortage-mark")).some(checkbox => normalizePartKey(checkbox.dataset.part) === part && checkbox.checked);
    if (!isShortage && qty > 0) supplements[part] = (supplements[part] || 0) + qty;
  });
  return supplements;
}

function _collectModalOrderDecisions() {
  const list = document.getElementById("modal-shortage-list");
  if (!list) return {};
  const decisionsByOrder = {};

  list.querySelectorAll(".supplement-input").forEach(input => {
    const { orderId, part } = _getModalRowMeta(input);
    if (!Number.isInteger(orderId) || !part) return;
    const qty = parseFloat(input.value) || 0;
    const isShortage = input.closest(".draft-preview-row, .shortage-item")?.querySelector(".shortage-mark")?.checked;
    const decision = isShortage
      ? "Shortage"
      : qty > 0
        ? "CreateRequirement"
        : "None";
    if (!decisionsByOrder[orderId]) decisionsByOrder[orderId] = {};
    decisionsByOrder[orderId][part] = decision;
  });

  return decisionsByOrder;
}

function _collectModalOrderSupplements() {
  const list = document.getElementById("modal-shortage-list");
  if (!list) return {};
  const supplementsByOrder = {};

  list.querySelectorAll(".supplement-input").forEach(input => {
    const { orderId, part } = _getModalRowMeta(input);
    if (!Number.isInteger(orderId) || !part) return;
    const qty = parseFloat(input.value) || 0;
    const isShortage = input.closest(".draft-preview-row, .shortage-item")?.querySelector(".shortage-mark")?.checked;
    if (!supplementsByOrder[orderId]) supplementsByOrder[orderId] = {};
    if (!isShortage && qty > 0) {
      supplementsByOrder[orderId][part] = qty;
    } else {
      delete supplementsByOrder[orderId][part];
    }
  });

  return supplementsByOrder;
}

function stopModalProgressAnimation() {
  if (_modalProgressTimer) {
    clearInterval(_modalProgressTimer);
    _modalProgressTimer = null;
  }
}

function setModalProgressPercent(percent) {
  const value = Math.max(0, Math.min(100, Math.round(Number(percent) || 0)));
  const fill = document.getElementById("modal-download-progress-fill");
  const percentLabel = document.getElementById("modal-download-percent");
  _modalProgressValue = value;
  if (fill) fill.style.width = `${value}%`;
  if (percentLabel) percentLabel.textContent = `${value}%`;
}

function startModalProgressAnimation(targetPercent, intervalMs = 180) {
  stopModalProgressAnimation();
  _modalProgressTimer = setInterval(() => {
    if (_modalProgressValue >= targetPercent) {
      stopModalProgressAnimation();
      return;
    }
    const remaining = targetPercent - _modalProgressValue;
    const step = remaining > 18 ? 4 : remaining > 8 ? 2 : 1;
    setModalProgressPercent(_modalProgressValue + step);
  }, intervalMs);
}

function setModalDownloadProgress(active, statusText = "", detailText = "", percent = null, options = {}) {
  const progress = document.getElementById("modal-download-progress");
  const status = document.getElementById("modal-download-status");
  const detail = document.getElementById("modal-download-detail");
  const saveBtn = document.getElementById("modal-save-draft");
  const downloadBtn = document.getElementById("modal-download-bom");
  const writeBtn = document.getElementById("modal-write-main");
  const cancelBtn = document.getElementById("modal-cancel");
  const closeBtn = document.getElementById("modal-close");
  const config = {
    lockUi: active,
    tone: "default",
    ...options,
  };

  if (progress) progress.style.display = active ? "block" : "none";
  if (status && statusText) status.textContent = statusText;
  if (detail && detailText) detail.textContent = detailText;
  if (progress) {
    progress.classList.remove("is-error", "is-success");
    if (config.tone === "error") progress.classList.add("is-error");
    if (config.tone === "success") progress.classList.add("is-success");
  }
  if (percent != null) setModalProgressPercent(percent);

  if (saveBtn) {
    if (!saveBtn.dataset.idleText) saveBtn.dataset.idleText = saveBtn.textContent || "儲存";
    if (!saveBtn.dataset.busyText) saveBtn.dataset.busyText = "儲存中...";
    saveBtn.disabled = Boolean(config.lockUi);
    saveBtn.textContent = active ? saveBtn.dataset.busyText : saveBtn.dataset.idleText;
  }
  if (downloadBtn) {
    if (!downloadBtn.dataset.idleText) downloadBtn.dataset.idleText = downloadBtn.textContent || "下載";
    if (!downloadBtn.dataset.busyText) downloadBtn.dataset.busyText = "下載中...";
    downloadBtn.disabled = Boolean(config.lockUi);
    downloadBtn.textContent = active ? downloadBtn.dataset.busyText : downloadBtn.dataset.idleText;
  }
  if (writeBtn) {
    writeBtn.disabled = Boolean(config.lockUi);
    writeBtn.textContent = active ? "寫入中..." : "寫入主檔";
  }
  if (cancelBtn) cancelBtn.disabled = Boolean(config.lockUi);
  if (closeBtn) closeBtn.disabled = Boolean(config.lockUi);
  if (!active) {
    stopModalProgressAnimation();
    setModalProgressPercent(percent ?? 0);
    if (progress) progress.classList.remove("is-error", "is-success");
  }
}

async function handleModalWriteMain() {
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  if (!targetOrderIds.length) {
    showToast("沒有可寫入主檔的訂單");
    return;
  }

  const supplements = _collectModalSupplements();
  const modalDecisions = _collectModalDecisions();
  const orderSupplements = _collectModalOrderSupplements();
  const orderDecisions = _collectModalOrderDecisions();

  try {
    setModalDownloadProgress(true, "正在保存缺料決策...", "先保存這次要寫入主檔的缺料與補料內容。", 10);
    startModalProgressAnimation(35, 140);
    await persistDecisionsForOrders(modalDecisions, targetOrderIds, orderDecisions);
    Object.entries(modalDecisions).forEach(([part, decision]) => {
      setLocalDecision(part, decision);
    });

    setModalDownloadProgress(true, "正在寫入主檔...", "系統會依目前選取訂單順序，將內容寫入 live 主檔。", 46);
    startModalProgressAnimation(92, 220);
    const result = await apiPost("/api/schedule/batch-dispatch", {
      order_ids: targetOrderIds,
      decisions: modalDecisions,
      supplements,
      order_decisions: orderDecisions,
      order_supplements: orderSupplements,
    });

    _modalTargets.forEach(item => _checkedIds.delete(item.id));
    setModalDownloadProgress(true, "寫入完成", "主檔與已發料清單正在刷新。", 100);
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
    await new Promise(resolve => setTimeout(resolve, 200));
    closeShortageModal();
    const shortageCount = (result.shortages || []).length;
    if (shortageCount > 0) {
      showToast(`已寫入主檔 ${result.count} 筆，merge ${result.merged_parts} 個料件，${shortageCount} 筆缺料待補`, { tone: "success", duration: 5000 });
      showPostDispatchShortages(result.shortages);
    } else {
      showToast(`已寫入主檔 ${result.count} 筆，merge ${result.merged_parts} 個料件`, { tone: "success" });
    }
  } catch (error) {
    stopModalProgressAnimation();
    setModalDownloadProgress(
      true,
      "寫入主檔失敗",
      error.message,
      100,
      { tone: "error", lockUi: false },
    );
    showToast("寫入主檔失敗: " + error.message, { sticky: true, tone: "error" });
  }
}

async function handleModalDownloadBom() {
  if (!_modalBomFiles.length) { showToast("找不到對應的 BOM 檔案"); return; }

  const supplements = _collectModalSupplements();
  const modalDecisions = _collectModalDecisions();
  const orderSupplements = _collectModalOrderSupplements();
  const orderDecisions = _collectModalOrderDecisions();
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  const headerOverrides = buildModalHeaderOverrides();

  try {
    setModalDownloadProgress(true, "正在保存補料決策...", "會把這次 merge 的補料內容記進系統。", 12);
    startModalProgressAnimation(32, 140);
    await persistDecisionsForOrders(modalDecisions, targetOrderIds, orderDecisions);
    Object.entries(modalDecisions).forEach(([part, decision]) => {
      setLocalDecision(part, decision);
    });

    const bomIds = _modalBomFiles.map(f => f.id);
    setModalDownloadProgress(true, "正在產生並下載 BOM...", `共 ${bomIds.length} 份 BOM，請稍候。`, 42);
    startModalProgressAnimation(92, 220);
    const result = await desktopDownload({
      path: "/api/bom/dispatch-download",
      method: "POST",
      body: {
        bom_ids: bomIds,
        order_ids: targetOrderIds,
        supplements,
        order_supplements: orderSupplements,
        header_overrides: headerOverrides,
      },
    });
    showDownloadToast(result, "BOM");

    setModalDownloadProgress(true, "下載完成", "BOM 已經下載完成。", 100);
    await new Promise(resolve => setTimeout(resolve, 220));
    updateStatusOnly();
    closeShortageModal();
  } catch (e) {
    showToast("BOM 下載失敗：" + e.message);
    setModalDownloadProgress(false, "", "", 0);
  }
}

// ── Completed tab ─────────────────────────────────────────────────────────────
function renderCompletedTab() {
  const container = document.getElementById("completed-scroll");
  if (!container) return;

  if (!_completedRows.length) {
    container.innerHTML = '<div class="empty-state">尚無已發料的排程列</div>';
    return;
  }

  // 按 folder 分組
  const grouped = {};
  for (const r of _completedRows) {
    const folder = r.folder || "";
    if (!grouped[folder]) grouped[folder] = [];
    grouped[folder].push(r);
  }

  // 所有資料夾選項（給下拉用）
  const allFolders = _completedFolders.slice();

  container.innerHTML = "";

  // 先渲染有名字的資料夾
  for (const folderName of allFolders) {
    if (!grouped[folderName]) continue;
    container.appendChild(buildFolderSection(folderName, grouped[folderName], allFolders));
  }

  // 最後渲染未歸檔
  if (grouped[""]) {
    container.appendChild(buildFolderSection("", grouped[""], allFolders));
  }
}

function buildFolderSection(folderName, rows, allFolders) {
  const section = document.createElement("div");
  section.className = "completed-folder-section";

  const isUnsorted = !folderName;
  const label = isUnsorted ? "未歸檔" : folderName;
  const isCollapsed = isCompletedFolderCollapsed(folderName);

  // 標題列
  const header = document.createElement("div");
  header.className = "completed-folder-header";
  header.innerHTML = `
    <span class="folder-toggle" style="cursor:pointer;user-select:none">${isCollapsed ? "▶" : "▼"}</span>
    <span class="folder-name">${esc(label)}</span>
    <span style="font-size:11px;color:#8e8e93;margin-left:4px">(${rows.length})</span>
    ${!isUnsorted ? `<button class="btn-folder-delete" title="刪除資料夾（訂單移回未歸檔）" style="margin-left:auto;background:none;border:none;color:#dc2626;font-size:14px;cursor:pointer;padding:2px 6px">✕</button>` : ""}`;
  section.appendChild(header);

  // 卡片容器
  const body = document.createElement("div");
  body.className = "completed-folder-body";
  if (isCollapsed) body.style.display = "none";
  for (const r of rows) {
    body.appendChild(buildCompletedCard(r, allFolders));
  }
  section.appendChild(body);

  // 收合
  header.querySelector(".folder-toggle").addEventListener("click", () => {
    const isOpen = body.style.display !== "none";
    const nextCollapsed = isOpen;
    body.style.display = nextCollapsed ? "none" : "";
    header.querySelector(".folder-toggle").textContent = nextCollapsed ? "▶" : "▼";
    setCompletedFolderCollapsed(folderName, nextCollapsed);
  });

  // 刪除資料夾
  const delBtn = header.querySelector(".btn-folder-delete");
  if (delBtn) {
    delBtn.addEventListener("click", async () => {
      if (!confirm(`確定刪除資料夾「${folderName}」？訂單會移回未歸檔。`)) return;
      try {
        delete _completedFolderCollapsedState[completedFolderStateKey(folderName)];
        saveCompletedFolderCollapsedState();
        await apiFetch(`/api/schedule/folders/${encodeURIComponent(folderName)}`, { method: "DELETE" });
        await refreshCompleted();
      } catch (e) { showToast("失敗：" + e.message); }
    });
  }

  return section;
}

function buildCompletedCard(r, allFolders) {
  const div = document.createElement("div");
  div.className = "po-group completed-card";
  const date = (r.delivery_date || r.ship_date) ? (r.delivery_date || r.ship_date).slice(5).replace("-", "/") : "—";
  const qty = r.order_qty != null ? r.order_qty : "—";
  const code = r.code ? `<span class="tag tag-pcb" style="font-size:10px;padding:1px 4px">${esc(r.code)}</span>` : "";

  // 資料夾下拉選項
  const currentFolder = r.folder || "";
  let folderOptions = `<option value=""${currentFolder === "" ? " selected" : ""}>未歸檔</option>`;
  for (const f of allFolders) {
    folderOptions += `<option value="${esc(f)}"${currentFolder === f ? " selected" : ""}>${esc(f)}</option>`;
  }

  div.innerHTML = `
    <div class="completed-card-header">
      <span class="po-number">${esc(r.model)}</span>
      <span style="color:#c7c7cc;font-size:13px">|</span>
      <span style="color:#6b7280;font-weight:500;font-size:14px;font-family:monospace">${r.po_number}</span>
      <span class="tag tag-pcb pcb-chip">${esc(r.pcb)}</span>
      ${code}
      <span style="font-size:13px;color:#3c3c43;font-weight:500">${qty}<span style="font-size:11px;color:#8e8e93;font-weight:400">pcs</span></span>
      <span class="po-ship-date">${date}</span>
      <div class="completed-card-actions">
        <button class="btn btn-secondary btn-sm btn-rollback-order" data-order-id="${r.id}" title="反悔此筆與之後的已發料訂單">反悔</button>
        <select class="folder-select" data-order-id="${r.id}" style="font-size:11px;padding:2px 4px;border:1px solid #e5e5ea;border-radius:4px;max-width:100px">${folderOptions}</select>
      </div>
    </div>`;

  // 移動資料夾
  div.querySelector(".folder-select").addEventListener("change", async (e) => {
    const newFolder = e.target.value;
    try {
      await apiPost("/api/schedule/orders/move-folder", { order_ids: [r.id], folder: newFolder });
      await refreshCompleted();
    } catch (err) { showToast("移動失敗：" + err.message); }
  });
  div.querySelector(".btn-rollback-order").addEventListener("click", event => {
    void handleRollbackDispatch(r.id, event.currentTarget);
  });

  return div;
}

async function handleCreateFolder() {
  const input = document.getElementById("new-folder-name");
  const name = (input?.value || "").trim();
  if (!name) { showToast("請輸入資料夾名稱"); return; }
  if (_completedFolders.includes(name)) { showToast("資料夾已存在"); return; }

  // 直接建立：把一筆假訂單移過去再移回來太蠢，直接在前端記住
  // 實際上建立資料夾 = 有訂單被移進去才會出現
  // 所以先提示使用者建立後要把訂單移過去
  _completedFolders.push(name);
  input.value = "";
  showToast(`資料夾「${name}」已建立，請將訂單移入`);
  renderCompletedTab();
}

async function handleRollbackDispatch(orderId, trigger) {
  if (!Number.isInteger(orderId)) return;

  const button = trigger || document.querySelector(`.btn-rollback-order[data-order-id="${orderId}"]`);
  const originalText = button?.textContent || "反悔";

  try {
    if (button) {
      button.disabled = true;
      button.textContent = "讀取中...";
    }
    const preview = await apiJson(`/api/schedule/orders/${orderId}/rollback-preview`);
    const orderLines = (preview.orders || []).map((item, index) => `${index + 1}. ${item.po_number} ${item.model}`).join("\n");
    const confirmed = confirm(
      `這會從所選訂單開始反悔，共 ${preview.count} 筆已發料訂單：\n${orderLines}\n\n主檔會一併還原到當時備份。確定繼續嗎？`
    );
    if (!confirmed) return;

    if (button) button.textContent = "反悔中...";
    const result = await apiPost(`/api/schedule/orders/${orderId}/rollback`);
    const draftNote = Number(result.restored_draft_count || 0) > 0
      ? `\n已恢復 ${result.restored_draft_count} 筆副檔工作台，可直接接續修改。`
      : "";
    showToast(`已反悔 ${result.count} 筆訂單\n主檔已同步還原${draftNote}`, { sticky: Boolean(draftNote), tone: "success" });
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
  } catch (error) {
    showToast("反悔失敗：" + error.message, { tone: "error" });
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
}

// ── SortableJS ────────────────────────────────────────────────────────────────
function buildRowCardLegacyBase(r, resultMap) {
  const div = document.createElement("div");
  div.className = "po-group";
  div.dataset.orderId = r.id;

  const res = resultMap[r.id];
  const draft = _draftsByOrderId?.[r.id] || null;
  const badge = cardBadge(res, r.id);
  const date = (r.delivery_date || r.ship_date) ? (r.delivery_date || r.ship_date).slice(5).replace("-", "/") : "--";
  const qty = r.order_qty != null ? r.order_qty : "--";
  const code = esc(r.code || "");
  const isChecked = _checkedIds.has(r.id);
  const remarkSpan = r.remark
    ? `<span class="po-ship-date" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.remark)}">${esc(r.remark.slice(0, 24))}${r.remark.length > 24 ? "..." : ""}</span>`
    : "<span></span>";
  const completeTitle = draft
    ? "照副檔寫入主檔"
    : r.status === "merged"
      ? "請先重新 merge 生成副檔"
      : "直接發料";
  const draftHtml = draft ? buildDraftPanelHtml(draft) : "";

  div.innerHTML = `
    <div class="po-group-header">
      <input type="checkbox" class="row-check" data-order-id="${r.id}" ${isChecked ? "checked" : ""}
             style="width:16px;height:16px;margin:0;cursor:pointer;accent-color:#34c759">
      <span class="drag-handle" title="拖曳排序">⋮</span>
      <span class="po-number model-editable" data-order-id="${r.id}" title="雙擊可改機種">${esc(r.model)}</span>
      <span style="color:#c7c7cc;font-size:13px;text-align:center">|</span>
      <span class="po-number" style="color:#6b7280;font-weight:500">${esc(r.po_number || "")}</span>
      <span class="tag tag-pcb pcb-chip">${esc(r.pcb)}</span>
      <span style="font-size:13px;color:#3c3c43;font-weight:500;white-space:nowrap">${qty}<span style="font-size:11px;color:#8e8e93;font-weight:400">pcs</span></span>
      <span class="po-ship-date">${date}</span>
      <input class="code-input" type="text" value="${code}"
             data-order-id="${r.id}" placeholder="代碼"
             style="width:100%;box-sizing:border-box;border:1px solid #e5e5ea;border-radius:4px;padding:2px 6px;font-size:12px;text-align:center;background:transparent">
      ${remarkSpan}
      <span class="po-status-badge ${badge.cls}">${badge.text}</span>
      <div class="row-actions">
        <button class="btn-edit-date" data-order-id="${r.id}" title="修改日期">✎</button>
        <button class="btn-cancel-order" data-order-id="${r.id}" title="取消訂單">×</button>
      </div>
    </div>
    ${draftHtml}`;

  div.querySelector(".row-check")?.addEventListener("change", e => {
    const id = parseInt(e.target.dataset.orderId, 10);
    if (e.target.checked) _checkedIds.add(id);
    else _checkedIds.delete(id);
    recalculate();
    updateStatusOnly();
  });

  div.querySelector(".model-editable")?.addEventListener("dblclick", e => {
    const span = e.currentTarget;
    const currentModel = span.textContent;
    const input = document.createElement("input");
    input.type = "text";
    input.value = currentModel;
    input.className = "model-edit-input";
    input.style.cssText = "width:100%;box-sizing:border-box;border:1px solid #007aff;border-radius:4px;padding:2px 6px;font-size:14px;font-weight:600;outline:none;background:#fff";
    span.replaceWith(input);
    input.focus();
    input.select();

    const save = async () => {
      const newModel = input.value.trim();
      if (newModel && newModel !== currentModel) {
        try {
          await apiPatch(`/api/schedule/orders/${r.id}/model`, { model: newModel });
          showToast("機種已更新");
          await refresh();
          return;
        } catch (err) {
          showToast("機種更新失敗: " + err.message);
        }
      }
      const newSpan = document.createElement("span");
      newSpan.className = "po-number model-editable";
      newSpan.dataset.orderId = r.id;
      newSpan.title = "雙擊可改機種";
      newSpan.textContent = newModel || currentModel;
      input.replaceWith(newSpan);
      newSpan.addEventListener("dblclick", () => {
        div.querySelector(".model-editable")?.dispatchEvent(new MouseEvent("dblclick"));
      });
    };

    input.addEventListener("blur", save);
    input.addEventListener("keydown", event => {
      if (event.key === "Enter") { event.preventDefault(); input.blur(); }
      if (event.key === "Escape") { input.value = currentModel; input.blur(); }
    });
  });

  div.querySelector(".code-input")?.addEventListener("change", async e => {
    const orderId = parseInt(e.target.dataset.orderId, 10);
    const codeVal = e.target.value.trim();
    try {
      await apiPatch(`/api/schedule/orders/${orderId}/code`, { code: codeVal });
    } catch (_) {}
  });

  div.querySelector(".btn-complete")?.addEventListener("click", () => {
    if (draft) {
      void handleCommitDraft(draft.id, r.model);
      return;
    }
    if (r.status === "merged") {
      showToast("這筆還沒有副檔，請先重新 merge");
      return;
    }
    void handleDispatch(r.id, r.model);
  });
  div.querySelector(".btn-edit-date")?.addEventListener("click", () => {
    void handleEditDelivery(r.id, r.delivery_date || r.ship_date || "");
  });
  div.querySelector(".btn-cancel-order")?.addEventListener("click", () => {
    void handleCancel(r.id, r.model);
  });

  if (draft) {
    div.querySelector(".btn-draft-edit")?.addEventListener("click", () => {
      void showDraftModal(draft.id, { readOnly: false });
    });
    div.querySelector(".btn-draft-download")?.addEventListener("click", () => {
      const selectedFileId = getSelectedDraftFileId(draft.id, draft.files || []);
      void downloadDraft(draft.id, selectedFileId);
    });
    div.querySelector(".btn-draft-delete")?.addEventListener("click", () => {
      void handleDeleteDraft(draft.id, r.model);
    });
  }

  return div;
}

async function handleBatchMergeLegacyFlow() {
  const targets = _rows.filter(r => _checkedIds.has(r.id) && (r.status === "pending" || r.status === "merged"));
  if (!_checkedIds.size) { showToast("請先勾選要 merge 的訂單"); return; }
  if (!targets.length) { showToast("目前勾選的訂單不能 merge"); return; }
  try {
    const result = await apiPost("/api/schedule/batch-merge", { order_ids: targets.map(r => r.id) });
    await refresh();
    showToast(`已建立 ${result.draft_count || 0} 份副檔，請在訂單下方確認後再按勾寫入主檔`);
  } catch (error) {
    showToast("批次 merge 失敗: " + error.message);
  }
}

async function handleBatchDispatch() {
  const targets = _rows.filter(r => _checkedIds.has(r.id) && (r.status === "pending" || r.status === "merged"));
  if (!_checkedIds.size) { showToast("請先勾選要寫入主檔的訂單"); return; }
  if (!targets.length) { showToast("目前勾選的訂單沒有可寫入主檔的項目"); return; }

  const button = document.getElementById("btn-batch-dispatch");
  const originalText = button?.textContent || "寫入主檔";
  const confirmed = confirm(`確定要直接寫入主檔 ${targets.length} 筆訂單嗎？`);
  if (!confirmed) return;
  try {
    if (button) {
      button.disabled = true;
      button.textContent = "寫入中...";
    }
    await withGlobalBusy(
      async () => {
        const result = await apiPost("/api/schedule/batch-dispatch", {
          order_ids: targets.map(item => item.id),
          decisions: _decisions,
          order_supplements: _orderSupplementsByOrderId,
        });
        targets.forEach(item => _checkedIds.delete(item.id));
        await Promise.all([refresh(), refreshCompleted()]);
        if (_onRefreshMain) await _onRefreshMain();
        const shortageCount = (result.shortages || []).length;
        if (shortageCount > 0) {
          showToast(`已寫入主檔 ${result.count} 筆，merge ${result.merged_parts} 個料件，${shortageCount} 筆缺料待補`, { tone: "success", duration: 5000 });
          showPostDispatchShortages(result.shortages);
        } else {
          showToast(`已寫入主檔 ${result.count} 筆，merge ${result.merged_parts} 個料件`, { tone: "success" });
        }
      },
      {
        title: "正在寫入主檔",
        detail: `共 ${targets.length} 筆訂單，系統會依目前順序直接寫入主檔。`,
      },
    );
  } catch (error) {
    showToast("寫入主檔失敗: " + error.message, { sticky: true, tone: "error" });
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
}

async function handleDeleteDraftLegacyV3(draftId, model) {
  if (!confirm(`確認要刪除 ${model} 的副檔嗎？`)) return;
  try {
    await apiFetch(`/api/schedule/drafts/${draftId}`, { method: "DELETE" });
    showToast("副檔已刪除");
    await refresh();
  } catch (error) {
    showToast("副檔刪除失敗: " + error.message);
  }
}

function initSortable(container) {
  if (typeof Sortable === "undefined") return;
  Sortable.create(container, {
    animation: 180,
    handle: ".drag-handle",
    ghostClass: "sortable-ghost",
    dragClass: "sortable-drag",
    async onEnd() {
      const newOrder = Array.from(container.children).map(el => parseInt(el.dataset.orderId));
      const rowMap = new Map(_rows.map(r => [r.id, r]));
      _rows = newOrder.map(id => rowMap.get(id)).filter(Boolean);
      recalculate();
      updateStatusOnly();
      try { await apiPost("/api/schedule/reorder", { order_ids: newOrder }); } catch (_) {}
    },
  });
}

function updateStatusOnly() {
  const resultMap = {};
  _calcResults.forEach((r, i) => { resultMap[_rows[i]?.id] = r; });
  const visibleShortageTotals = buildCheckedOrderVisibleShortageBadgeMap();

  document.querySelectorAll("#schedule-scroll .po-group[data-order-id]").forEach(div => {
    const orderId = parseInt(div.dataset.orderId);
    const row = _rows.find(item => item.id === orderId) || null;
    const res = resultMap[orderId];
    const badge = div.querySelector(".po-status-badge");
    if (badge) {
      const b = buildOrderBadge(row, res, visibleShortageTotals);
      badge.className = `po-status-badge ${b.cls}`;
      badge.textContent = b.text;
    }
  });

  const { shortages, csShortages } = buildRightPanelShortageData();
  renderShortagePanel(shortages, csShortages, buildMainFileDeficitItems());
}

// ── 合併跨機種同料號缺料 ─────────────────────────────────────────────────────
/**
 * Running balance 是累積的，後面機種對同一料號的 shortage_amount 已包含前面的，
 * 因此同一料號只保留在「第一個出現的機種」，補料量取最大值（= 最終累積缺量）。
 */
function _consolidateShortagesAcrossModels(
  byModel,
  orderedModels,
  { preserveOrderScopedParts = false, preserveShortageDecisions = false } = {},
) {
  const bestByPart = {}; // PART_UPPER → { item (reference in first model's array) }
  const preservedParts = new Set();
  for (const model of orderedModels) {
    const items = byModel[model] || [];
    for (const item of items) {
      const pk = (item.part_number || "").toUpperCase();
      if (preserveOrderScopedParts && isOrderScopedPart(pk)) continue;
      if (preserveShortageDecisions && String(item?.decision || "").trim() === "Shortage") {
        preservedParts.add(pk);
        continue;
      }
      if (preservedParts.has(pk)) continue;
      if (!bestByPart[pk]) {
        bestByPart[pk] = { item };
      } else {
        const first = bestByPart[pk].item;
        // 累積需求量
        first.needed = (first.needed || 0) + (item.needed || 0);
        // 取最大缺量（= 最後一個機種的累積缺量）
        if (item.shortage_amount > first.shortage_amount) {
          first.shortage_amount = item.shortage_amount;
          const moq = first.moq || 0;
          first.suggested_qty = moq > 0
            ? Math.ceil(first.shortage_amount / moq) * moq
            : first.shortage_amount;
          const stAvail = Math.min(first.suggested_qty, first.st_stock_qty || 0);
          first.st_available_qty = stAvail;
          first.purchase_needed_qty = Math.max(0, first.shortage_amount - stAvail);
          first.purchase_suggested_qty = first.purchase_needed_qty > 0
            ? (moq > 0 ? Math.ceil(first.purchase_needed_qty / moq) * moq : first.purchase_needed_qty)
            : 0;
          first.needs_purchase = first.purchase_needed_qty > 0;
        }
        item._consolidated = true;
      }
    }
  }
  // 移除已合併到前面機種的項目
  for (const model of orderedModels) {
    if (!byModel[model]) continue;
    byModel[model] = byModel[model].filter(item => !item._consolidated);
  }
}

/**
 * 從已存的副檔草稿還原 decisions / supplements 到缺料項目。
 * 讓 modal 重新開啟時自動帶入上次使用者的輸入，不需重填。
 */
function _applyStoredToShortages(
  byModel,
  storedDecisions,
  storedSupplements,
  storedOrderScopedDecisions = {},
  storedOrderScopedSupplements = {},
) {
  for (const items of Object.values(byModel)) {
    for (const item of items) {
      const pk = normalizePartKey(item.part_number);
      if (!pk) continue;
      const orderPartKey = buildOrderPartKey(item._order_id, pk);
      if (orderPartKey && storedOrderScopedDecisions[orderPartKey]) {
        item.decision = storedOrderScopedDecisions[orderPartKey];
      } else if (storedDecisions[pk]) {
        item.decision = storedDecisions[pk];
      }
      if (orderPartKey && Number(storedOrderScopedSupplements[orderPartKey]) > 0) {
        item.default_supplement = storedOrderScopedSupplements[orderPartKey];
      } else if (storedSupplements[pk] > 0) {
        item.default_supplement = storedSupplements[pk];
      }
    }
  }
}

// ── Shortage panel ────────────────────────────────────────────────────────────
function compareText(a, b, locale = "en") {
  return String(a || "").localeCompare(String(b || ""), locale, { numeric: true, sensitivity: "base" });
}

function compareShortageItems(a, b) {
  const orderIndexA = Number.isFinite(Number(a?._row_order_index)) ? Number(a._row_order_index) : Number.MAX_SAFE_INTEGER;
  const orderIndexB = Number.isFinite(Number(b?._row_order_index)) ? Number(b._row_order_index) : Number.MAX_SAFE_INTEGER;
  if (orderIndexA !== orderIndexB) return orderIndexA - orderIndexB;
  const groupCmp = compareText(a._row_group_label, b._row_group_label, "zh-Hant");
  if (groupCmp !== 0) return groupCmp;
  const partCmp = compareText(a.part_number, b.part_number);
  if (partCmp !== 0) return partCmp;
  return compareText(a._row_code, b._row_code);
}

function renderShortageGroupHtml(items, isCS) {
  const sorted = [...items].sort(compareShortageItems);
  let html = "";
  let currentGroupKey = null;
  for (const item of sorted) {
    const groupKey = item._row_group_key || item._row_model || "未指定機種";
    if (groupKey !== currentGroupKey) {
      currentGroupKey = groupKey;
      html += shortageGroupHeadingHtml(
        item._row_group_label || item._row_model || "未指定機種",
        item._po_number || "",
        { compact: true },
      );
    }
    html += shortageItemHtml(item, isCS);
  }
  return html;
}

function renderShortagePanel(shortages, csShortages = [], mainDeficits = []) {
  const scroll = document.getElementById("right-scroll");
  const badge = document.getElementById("shortage-count");
  const orderShortageCount = shortages.length + csShortages.length;
  const totalCount = orderShortageCount + mainDeficits.length;

  if (!totalCount) {
    scroll.innerHTML = '<div class="no-shortage-msg">無缺料</div>';
    badge.style.display = "none";
    return;
  }

  badge.style.display = "inline";
  badge.textContent = totalCount;

  let html = "";

  // 客供料缺料（黃色區塊）
  if (csShortages.length) {
    html += '<div class="cs-shortage-section"><h4 class="cs-title">客供料缺料</h4>';
    html += renderShortageGroupHtml(csShortages, true);
    html += '</div>';
  }

  // 一般缺料
  if (shortages.length) {
    if (csShortages.length) html += '<h4 style="font-size:12px;color:#dc2626;margin:8px 0 4px;font-weight:600">採購缺料</h4>';
    html += renderShortageGroupHtml(shortages, false);
  }

  // 主檔已缺料（不需勾選訂單也會顯示）
  if (mainDeficits.length) {
    html += renderMainDeficitSectionHtml(mainDeficits);
  }

  scroll.innerHTML = html;

  scroll.querySelectorAll(".dec-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      const part = normalizePartKey(btn.dataset.part);
      const dec = btn.dataset.dec;
      const prev = _decisions[part] || "None";
      const next = prev === dec ? "None" : dec;
      setLocalDecision(part, next);
      renderShortagePanel(shortages, csShortages, mainDeficits);
      try {
        await persistDecisionsForOrders({ [part]: next }, getAffectedOrderIdsForPart(part));
      } catch (e) {
        setLocalDecision(part, prev);
        renderShortagePanel(shortages, csShortages, mainDeficits);
        showToast("決策儲存失敗：" + e.message);
      }
    });
  });
  scroll.querySelectorAll(".right-panel-supplement-save").forEach(btn => {
    btn.addEventListener("click", () => {
      void saveRightPanelSupplement(btn);
    });
  });
  scroll.querySelectorAll(".right-panel-supplement-input").forEach(input => {
    input.addEventListener("keydown", event => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      input.closest(".right-panel-supplement-row")?.querySelector(".right-panel-supplement-save")?.click();
    });
  });
  scroll.querySelectorAll(".right-panel-supplement-note-input").forEach(input => {
    input.addEventListener("keydown", event => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      input.closest(".shortage-item")?.querySelector(".right-panel-supplement-save")?.click();
    });
  });
  bindMoqEditors(scroll);
  bindShortageMoqBadgeEditors(scroll);
}

async function saveRightPanelSupplement(button) {
  const row = button?.closest(".shortage-item");
  const input = row?.querySelector(".right-panel-supplement-input");
  const noteInput = row?.querySelector(".right-panel-supplement-note-input");
  const orderId = normalizeOrderId(button?.dataset.orderId || input?.dataset.orderId);
  const part = normalizePartKey(button?.dataset.part || input?.dataset.part);
  const isMainSupplement = String(button?.dataset.mainSupplement || input?.dataset.mainSupplement || "").trim() === "true";
  const qty = Number(input?.value || 0);
  const note = String(noteInput?.value || "").trim();

  if ((!Number.isInteger(orderId) && !isMainSupplement) || !part) {
    showToast("找不到要保存的補料項目");
    return;
  }
  if (!Number.isFinite(qty) || qty < 0 || (isMainSupplement && qty <= 0)) {
    showToast(isMainSupplement ? "補料數量必須大於 0" : "請輸入正確的補料數量");
    input?.focus();
    return;
  }

  const originalText = button.textContent;
  if (button) {
    button.disabled = true;
    button.textContent = "保存中...";
  }
  if (input) input.disabled = true;
  if (noteInput) noteInput.disabled = true;

  try {
    if (isMainSupplement) {
      const result = await apiPost("/api/schedule/supplement-part", {
        part_number: part,
        supplement_qty: qty,
      });
      _postDispatchShortages = _postDispatchShortages.filter(item => normalizePartKey(item?.part_number) !== part);
      _savePostDispatchShortages();
      showToast(
        `${result.part_number} 已補料 ${fmt(qty)}，庫存 ${fmt(result.stock_before)} → ${fmt(result.stock_after)}`,
        { tone: "success", duration: 4000 },
      );
      await refresh();
      if (_onRefreshMain) await _onRefreshMain();
      return;
    }

    const response = await apiPut("/api/schedule/shortage-settings", {
      order_ids: [orderId],
      order_supplements: {
        [String(orderId)]: {
          [part]: qty,
        },
      },
      order_supplement_notes: {
        [String(orderId)]: {
          [part]: note,
        },
      },
    });
    if (!_orderSupplementsByOrderId[orderId]) _orderSupplementsByOrderId[orderId] = {};
    if (!_orderSupplementDetailsByOrderId[orderId]) _orderSupplementDetailsByOrderId[orderId] = {};
    if (qty > 0) {
      _orderSupplementsByOrderId[orderId][part] = qty;
      const detail = response?.order_supplement_details?.[String(orderId)]?.[part]
        || response?.order_supplement_details?.[orderId]?.[part]
        || {
          supplement_qty: qty,
          note,
          updated_at: new Date().toISOString(),
        };
      _orderSupplementDetailsByOrderId[orderId][part] = {
        supplement_qty: Number(detail?.supplement_qty || qty) || qty,
        note: String(detail?.note || note).trim(),
        updated_at: String(detail?.updated_at || new Date().toISOString()).trim(),
      };
    } else {
      delete _orderSupplementsByOrderId[orderId][part];
      delete _orderSupplementDetailsByOrderId[orderId][part];
    }
    const currentStock = Number(input?.dataset.currentStock || row?.dataset.currentStock || 0);
    const prevQtyCs = Number(input?.dataset.prevQtyCs || row?.dataset.prevQtyCs || 0);
    const neededQty = Number(input?.dataset.needed || row?.dataset.needed || 0);
    const nextResultingStock = currentStock + prevQtyCs + qty - neededQty;
    if (Number.isFinite(nextResultingStock) && nextResultingStock >= 0) {
      removeRightPanelShortageRowIfResolved(row);
    }
    showToast(qty > 0 ? `已保存 ${part} 補料 ${fmt(qty)}` : `已清除 ${part} 補料`);
    await refresh();
  } catch (e) {
    showToast("補料儲存失敗：" + e.message);
    if (button?.isConnected) {
      button.disabled = false;
      button.textContent = originalText;
    }
    if (input?.isConnected) input.disabled = false;
    if (noteInput?.isConnected) noteInput.disabled = false;
  }
}

function renderMainDeficitSectionHtml(deficits) {
  let html = `<div style="margin-top:12px;border-top:1px solid var(--border, #e5e7eb);padding-top:10px">
    <h4 style="font-size:12px;color:#f59e0b;margin:0 0 6px;font-weight:600">主檔已缺料 (${deficits.length})</h4>`;
  for (const item of deficits) {
    const deficitAmt = fmt(roundShortageUiValue(item.shortage_amount));
    const stockAmt = fmt(roundShortageUiValue(item.current_stock));
    const moqVal = Number(item.moq || 0);
    const suggestedQty = roundShortageUiValue(item.suggested_qty || item.shortage_amount || 0);
    html += `<div class="shortage-item" style="opacity:0.85">
      <div class="part">${esc(item.part_number)}</div>
      <div class="desc">${esc(item.description || "—")}</div>
      <div class="amounts">
        <span class="red">缺 ${deficitAmt}</span>
        <span style="color:#6b7280">結存 ${stockAmt}</span>
        ${moqVal > 0 ? `<span>MOQ ${fmt(moqVal)}</span>` : ""}
      </div>
      <div class="right-panel-supplement-row" style="display:flex;gap:6px;align-items:center;margin-top:8px">
        <label style="font-size:11px;color:#6b7280;white-space:nowrap">補主檔</label>
        <input
          type="number"
          class="right-panel-supplement-input"
          data-part="${esc(item.part_number)}"
          data-main-supplement="true"
          value="${suggestedQty}"
          min="0"
          step="any"
          style="flex:1;min-width:0;padding:6px 8px;border:1px solid #d1d5db;border-radius:8px;font-size:12px"
        >
        <button
          class="btn btn-secondary btn-xs right-panel-supplement-save"
          data-part="${esc(item.part_number)}"
          data-main-supplement="true"
        >補主檔</button>
      </div>
      <div style="font-size:10px;color:#6b7280;margin-top:4px">直接補到主檔，後續同料號欄位會一起同步加回</div>
    </div>`;
  }
  html += "</div>";
  return html;
}

function shortageItemHtml(s, isCS) {
  const shortageAmount = roundShortageUiValue(s.shortage_amount);
  const currentStock = roundShortageUiValue(s.current_stock);
  const neededQty = roundShortageUiValue(s.needed);
  const stAvailableQty = roundShortageUiValue(s.st_available_qty || 0);
  const purchaseNeededQty = roundShortageUiValue(s.purchase_needed_qty || 0);
  const supplementQty = roundShortageUiValue(
    Number(s.default_supplement) > 0
      ? s.default_supplement
      : Number(s.supplement_qty) > 0
        ? s.supplement_qty
        : getStoredOrderSupplementQty(s._order_id, s.part_number),
  );
  const orderId = normalizeOrderId(s._order_id);
  const supplementDetail = getStoredOrderSupplementDetail(orderId, s.part_number);
  const supplementNote = supplementDetail.note;
  const supplementUpdatedAt = formatDraftTime(supplementDetail.updated_at);
  s = {
    ...s,
    shortage_amount: shortageAmount,
    current_stock: currentStock,
    needed: neededQty,
    st_available_qty: stAvailableQty,
    purchase_needed_qty: purchaseNeededQty,
    supplement_qty: supplementQty,
    _order_id: orderId,
  };
  const dec = _decisions[normalizePartKey(s.part_number)] || "None";
  const codeTag = s._row_code
    ? `<span class="tag tag-pcb" style="font-size:10px;padding:1px 6px;margin-left:6px">${esc(s._row_code)}</span>`
    : "";
  const csTag = isCS ? '<span class="tag tag-cs">客供</span>' : "";
  const orderIdAttr = Number.isInteger(orderId) ? ` data-order-id="${orderId}"` : "";
  const searchPrimary = [
    s.part_number || "",
    s._row_code || "",
    s._row_model || "",
    s._row_group_label || "",
    s._po_number ? `PO ${s._po_number}` : "",
  ].join(" ");
  const searchSecondary = s.description || "";
  const supplementEditorHtml = !isCS && Number.isInteger(orderId)
    ? `<div class="right-panel-supplement-row" style="display:flex;gap:6px;align-items:center;margin-top:8px">
        <label style="font-size:11px;color:#6b7280;white-space:nowrap">補這筆</label>
        <input
          type="number"
          class="right-panel-supplement-input"
          data-part="${esc(s.part_number)}"${orderIdAttr}
          data-current-stock="${esc(s.current_stock)}"
          data-prev-qty-cs="${esc(s.prev_qty_cs || 0)}"
          data-needed="${esc(s.needed)}"
          value="${s.supplement_qty}"
          min="0"
          step="any"
          style="flex:1;min-width:0;padding:6px 8px;border:1px solid #d1d5db;border-radius:8px;font-size:12px"
        >
        <button class="btn btn-secondary btn-xs right-panel-supplement-save" data-part="${esc(s.part_number)}"${orderIdAttr}>保存補料</button>
      </div>
      <input
        type="text"
        class="right-panel-supplement-note-input"
        data-part="${esc(s.part_number)}"${orderIdAttr}
        value="${esc(supplementNote)}"
        placeholder="備註（選填）"
        style="width:100%;margin-top:6px;padding:6px 8px;border:1px solid #d1d5db;border-radius:8px;font-size:12px"
      >
      <div class="right-panel-supplement-meta" style="font-size:10px;color:#6b7280;margin-top:4px">
        ${supplementUpdatedAt ? `最後修改 ${esc(supplementUpdatedAt)}` : "尚未保存"}
      </div>
      <div style="font-size:10px;color:#6b7280;margin-top:4px">只補這筆，後面機種會沿用剩餘量繼續扣帳</div>`
    : "";

  return `<div class="${shortageToneClass(s, isCS)}" data-search="${esc([searchPrimary, searchSecondary].join(" "))}" data-search-primary="${esc(searchPrimary)}" data-search-secondary="${esc(searchSecondary)}">
    <div class="part">${s.part_number}${codeTag}${csTag}</div>
    <div class="desc">${s.description || "—"}</div>
    <div class="amounts">
      <span class="red">缺 ${fmt(s.shortage_amount)}</span>
      <span class="green">庫存 ${fmt(s.current_stock)}</span>
      <span>需 ${fmt(s.needed)}</span>
      ${stSupplySummaryHtml(s)}
      ${suggestedQtyHtml(s)}
    </div>
    ${missingMoqEditorHtml(s)}
    ${supplementEditorHtml}
    ${isCS ? '<div style="font-size:11px;color:#ca8a04;margin-top:4px">請通知客戶提供此料</div>' : `
    <div class="decision-btns">
      <button class="dec-btn ${dec === "CreateRequirement" ? "active-create" : ""}" data-dec="CreateRequirement" data-part="${s.part_number}">需採購</button>
      <button class="dec-btn ${dec === "MarkHasPO" ? "active-has-po" : ""}" data-dec="MarkHasPO" data-part="${s.part_number}">已有PO</button>
      <button class="dec-btn ${dec === "IgnoreOnce" ? "active-ignore" : ""}" data-dec="IgnoreOnce" data-part="${s.part_number}">忽略</button>
      <button class="dec-btn ${dec === "Shortage" ? "active-shortage" : ""}" data-dec="Shortage" data-part="${s.part_number}">缺料</button>
    </div>`}
  </div>`;
}

// ── Post-dispatch shortage panel + single-part supplement modal ───────────────

const POST_DISPATCH_STORAGE_KEY = "post-dispatch-shortages";
let _postDispatchShortages = [];

function _loadPostDispatchShortages() {
  try {
    const raw = window.localStorage?.getItem(POST_DISPATCH_STORAGE_KEY);
    if (raw) _postDispatchShortages = JSON.parse(raw);
  } catch (_) {}
}

function _savePostDispatchShortages() {
  try {
    if (_postDispatchShortages.length) {
      window.localStorage?.setItem(POST_DISPATCH_STORAGE_KEY, JSON.stringify(_postDispatchShortages));
    } else {
      window.localStorage?.removeItem(POST_DISPATCH_STORAGE_KEY);
    }
  } catch (_) {}
}

function showPostDispatchShortages(shortages) {
  // 追加新的缺料（合併，同料號取最新）
  const newItems = (shortages || []).filter(s => Number(s.shortage_amount || 0) > 0);
  const merged = new Map();
  for (const s of _postDispatchShortages) merged.set(s.part_number, s);
  for (const s of newItems) merged.set(s.part_number, s);
  _postDispatchShortages = [...merged.values()];
  _savePostDispatchShortages();
  renderPostDispatchPanel();
}

function renderPostDispatchPanel() {
  const scroll = document.getElementById("right-scroll");
  const badge = document.getElementById("shortage-count");
  if (!_postDispatchShortages.length) {
    scroll.innerHTML = '<div class="no-shortage-msg">無缺料</div>';
    badge.style.display = "none";
    return;
  }

  badge.style.display = "inline";
  badge.textContent = _postDispatchShortages.length;

  const grouped = {};
  for (const s of _postDispatchShortages) {
    const model = s.model || s.bom_model || "未指定機種";
    if (!grouped[model]) grouped[model] = [];
    grouped[model].push(s);
  }

  let html = '<div style="padding:6px 10px;font-size:12px;font-weight:600;color:#dc2626;border-bottom:1px solid #fee2e2;margin-bottom:4px">寫入主檔後仍缺料（可直接在右側補主檔）</div>';
  for (const [model, items] of Object.entries(grouped).sort((a, b) => a[0].localeCompare(b[0], "zh-Hant"))) {
    html += `<div style="font-size:11px;font-weight:600;color:#6b7280;margin:8px 10px 4px">${esc(model)}</div>`;
    for (const s of items) {
      const shortageAmt = roundShortageUiValue(s.shortage_amount);
      const resultingStock = roundShortageUiValue(s.resulting_stock ?? s.current_stock);
      const moqVal = roundShortageUiValue(s.moq || 0);
      const suggestedQty = roundShortageUiValue(s.suggested_qty || shortageAmt);
      html += `<div class="shortage-item is-negative-after-supplement" data-part="${esc(s.part_number)}" data-shortage="${shortageAmt}" data-moq="${moqVal}" data-suggested="${suggestedQty}" data-stock="${resultingStock}" data-desc="${esc(s.description || "")}">
        <div class="part">${esc(s.part_number)}</div>
        <div class="desc">${esc(s.description || "—")}</div>
        <div class="amounts">
          <span class="red">缺 ${fmt(shortageAmt)}</span>
          <span style="color:#6b7280">結存 ${fmt(resultingStock)}</span>
          ${moqVal > 0 ? `<span style="color:#8b5cf6">MOQ ${fmt(moqVal)}</span>` : ""}
        </div>
        <div class="right-panel-supplement-row" style="display:flex;gap:6px;align-items:center;margin-top:8px">
          <label style="font-size:11px;color:#6b7280;white-space:nowrap">補主檔</label>
          <input
            type="number"
            class="right-panel-supplement-input"
            data-part="${esc(s.part_number)}"
            data-main-supplement="true"
            value="${suggestedQty}"
            min="0"
            step="any"
            style="flex:1;min-width:0;padding:6px 8px;border:1px solid #d1d5db;border-radius:8px;font-size:12px"
          >
          <button class="btn btn-primary btn-xs right-panel-supplement-save" data-part="${esc(s.part_number)}" data-main-supplement="true">補主檔</button>
        </div>
        <div style="font-size:10px;color:#6b7280;margin-top:4px">直接補到主檔，後續同料號欄位會一起同步加回</div>
      </div>`;
    }
  }
  scroll.innerHTML = html;

  scroll.querySelectorAll(".right-panel-supplement-save").forEach(btn => {
    btn.addEventListener("click", () => {
      void saveRightPanelSupplement(btn);
    });
  });
  scroll.querySelectorAll(".right-panel-supplement-input").forEach(input => {
    input.addEventListener("keydown", event => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      input.closest(".right-panel-supplement-row")?.querySelector(".right-panel-supplement-save")?.click();
    });
  });
}

function bindShortageEditors(list) {
  if (!list) return;

  list.querySelectorAll(".shortage-mark").forEach(checkbox => {
    checkbox.addEventListener("change", () => {
      const partKey = normalizePartKey(checkbox.dataset.part);
      if (!partKey) return;

      if (!isOrderScopedPart(partKey)) {
        syncDraftPartControls(list, partKey, {
          shortageChecked: checkbox.checked,
        });
        return;
      }

      const input = checkbox.closest(".shortage-item")?.querySelector(".supplement-input");
      if (!input) return;
      input.disabled = checkbox.checked;
      if (checkbox.checked) input.value = "0";
      updateModalShortageTone(checkbox.closest(".shortage-item"));
    });
  });

  list.querySelectorAll(".supplement-input").forEach(input => {
    input.addEventListener("input", () => {
      const partKey = normalizePartKey(input.dataset.part);
      if (!partKey) return;
      if (isOrderScopedPart(partKey)) {
        updateModalShortageTone(input.closest(".shortage-item"));
        return;
      }
      syncDraftPartControls(list, partKey, {
        qty: parseFloat(input.value) || 0,
      });
    });
  });

  list.querySelectorAll(".shortage-item[data-part]").forEach(card => updateModalShortageTone(card));
}

// ── Toolbar actions ───────────────────────────────────────────────────────────
async function handleAutoSort() {
  try {
    await apiFetch("/api/schedule/auto-sort", { method: "POST" });
    await loadScheduleRows();
    recalculate();
    renderSchedule();
    showToast("已恢復依出貨日自動排序");
  } catch (e) { showToast("錯誤：" + e.message); }
}

async function handleDedupSchedule() {
  const btn = document.getElementById("btn-dedup-schedule");
  if (btn) { btn.disabled = true; btn.textContent = "比對中..."; }
  try {
    const resp = await apiFetch("/api/schedule/dedup", { method: "POST" });
    const data = await resp.json();
    if (data.removed > 0) {
      const poList = data.duplicates.map(d => d.po_number).join("、");
      showToast(`已移除 ${data.removed} 筆已發料重複 PO：${poList}`, { tone: "success", duration: 5000 });
      await loadScheduleRows();
      recalculate();
      renderSchedule();
    } else {
      showToast("沒有重複的 PO，排程表已是最新", { tone: "success" });
    }
  } catch (e) {
    showToast("清理失敗：" + e.message);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "清理重複"; }
  }
}

// Safe overrides for draft workbench rendering.
async function handleBatchMerge() {
  if (_batchMergeInFlight) {
    showToast("批次 merge 進行中，請稍候");
    return;
  }
  closeShortageModal();
  await waitForNextFrame();
  const selectedRows = _rows.filter(row => _checkedIds.has(row.id));
  const targets = selectedRows.filter(row => row.status === "pending" || row.status === "merged");
  if (!_checkedIds.size) {
    showToast("請先勾選要 merge 的訂單");
    return;
  }
  if (!targets.length) {
    showToast("勾選的訂單中沒有可 merge 的");
    return;
  }

  const button = document.getElementById("btn-batch-merge");
  const originalText = button?.textContent || "批次 Merge";
  _batchMergeInFlight = true;
  try {
    if (button) {
      button.disabled = true;
      button.textContent = "建立中...";
    }
    const targetIds = targets.map(row => row.id);
    const currentOrderIds = _rows.map(row => row.id).filter(Number.isInteger);

    // 先把目前畫面順序寫回後端，再依這個順序重建副檔
    const result = await withGlobalBusy(
      async () => {
        if (currentOrderIds.length) {
          await apiPost("/api/schedule/reorder", { order_ids: currentOrderIds });
        }
        return apiPost("/api/schedule/batch-merge", { order_ids: targetIds });
      },
      {
        title: "正在批次建立副檔",
        detail: `共 ${targets.length} 筆訂單，系統正在整理 BOM 與補料資料，請稍候。`,
      },
    );

    // overlay 已關閉，背景刷新資料
    await refresh();

    showToast(`已建立 ${result.draft_count || 0} 份副檔，請先確認補料`, { tone: "success" });
    try {
      await openBatchMergeDraftModalStable(targetIds, targets);
    } catch (modalError) {
      console.error("[handleBatchMerge] showBatchMergeDraftModal failed:", modalError);
      showToast("補料 modal 開啟失敗: " + modalError.message, { sticky: true, tone: "error" });
    }
  } catch (error) {
    console.error("[handleBatchMerge] batch merge failed:", error);
    showToast("批次 merge 失敗: " + error.message, { sticky: true, tone: "error" });
  } finally {
    _batchMergeInFlight = false;
    if (button) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
}

function buildBatchMergeModalTargets(targetIds, fallbackTargets = []) {
  const targetOrderIndex = new Map((targetIds || []).map((id, index) => [id, index]));
  const refreshedTargets = _rows
    .filter(row => targetOrderIndex.has(row.id))
    .sort((a, b) => (targetOrderIndex.get(a.id) ?? 0) - (targetOrderIndex.get(b.id) ?? 0));
  if (refreshedTargets.length) return refreshedTargets;
  return (fallbackTargets || [])
    .filter(row => targetOrderIndex.has(row?.id))
    .sort((a, b) => (targetOrderIndex.get(a.id) ?? 0) - (targetOrderIndex.get(b.id) ?? 0));
}

async function openBatchMergeDraftModalStable(targetIds, fallbackTargets = []) {
  await waitForNextFrame();
  await waitForNextFrame();

  let modalTargets = buildBatchMergeModalTargets(targetIds, fallbackTargets);
  await showBatchMergeDraftModal(modalTargets);
  await waitForNextFrame();

  const modal = document.getElementById("shortage-modal");
  if (modal?.style.display === "flex") return;

  closeShortageModal();
  await waitForNextFrame();
  modalTargets = buildBatchMergeModalTargets(targetIds, fallbackTargets);
  await showBatchMergeDraftModal(modalTargets);
  await waitForNextFrame();

  if (modal?.style.display !== "flex") {
    throw new Error("補料 modal 沒有成功顯示");
  }
}

function buildDraftPanelHtml(draft) {
  const files = Array.isArray(draft?.files) ? draft.files : [];
  const shortages = Array.isArray(draft?.shortages) ? draft.shortages : [];
  const updatedAt = formatDraftTime(draft?.updated_at) || "--";
  const draftId = Number.parseInt(String(draft?.id || ""), 10);
  const orderId = Number.parseInt(String(draft?.order_id || ""), 10);
  const collapsed = Number.isInteger(orderId) ? isDraftPanelCollapsed(orderId) : false;
  const selectedFileId = Number.isInteger(draftId) ? getSelectedDraftFileId(draftId, files) : null;
  const fileHtml = buildDraftFileListHtml(files, {
    label: "副檔目標",
    selectable: true,
    draftId,
    selectedFileId,
  });

  return `
    <div class="merge-draft-panel ${collapsed ? "is-collapsed" : ""}">
      <div class="merge-draft-head">
        <div class="merge-draft-head-label">副檔工作台</div>
      </div>
      <div class="merge-draft-body">
        <div class="merge-draft-summary">
          <span class="merge-draft-pill">副檔 ${files.length} 份</span>
          <span class="merge-draft-meta">缺料 ${shortages.length} 筆</span>
          <span class="merge-draft-meta">更新 ${esc(updatedAt)}</span>
        </div>
        ${fileHtml}
        <div class="merge-draft-actions">
          <button class="btn btn-success btn-sm btn-draft-commit" data-draft-id="${draft.id}">寫入主檔</button>
          <button class="btn btn-secondary btn-sm btn-draft-edit" data-draft-id="${draft.id}">修改</button>
          <button class="btn btn-secondary btn-sm btn-draft-download" data-draft-id="${draft.id}">下載</button>
          <button class="btn btn-secondary btn-sm btn-draft-delete" data-draft-id="${draft.id}">刪除</button>
        </div>
      </div>
    </div>`;
}

async function handleDeleteDraft(draftId, model) {
  if (!confirm(`確認要刪除 ${model} 的副檔嗎？`)) return;
  try {
    await apiFetch(`/api/schedule/drafts/${draftId}`, { method: "DELETE" });
    showToast("副檔已刪除");
    await refresh();
  } catch (error) {
    showToast("副檔刪除失敗: " + error.message);
  }
}

function buildRowCardLegacyOriginal(r, resultMap) {
  const div = document.createElement("div");
  div.className = "po-group";
  div.dataset.orderId = r.id;

  const res = resultMap[r.id];
  const draft = _draftsByOrderId?.[r.id] || null;
  const badge = cardBadge(res, r.id);
  const date = (r.delivery_date || r.ship_date) ? (r.delivery_date || r.ship_date).slice(5).replace("-", "/") : "--";
  const qty = r.order_qty != null ? r.order_qty : "--";
  const code = esc(r.code || "");
  const isChecked = _checkedIds.has(r.id);
  const remarkSpan = r.remark
    ? `<span class="po-ship-date" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.remark)}">${esc(r.remark.slice(0, 24))}${r.remark.length > 24 ? "..." : ""}</span>`
    : "<span></span>";
  const completeTitle = draft ? "依副檔寫入主檔" : (r.status === "merged" ? "請先確認副檔後再提交" : "發料");
  const draftHtml = draft ? buildDraftPanelHtml(draft) : "";

  div.innerHTML = `
    <div class="po-group-header">
      <input type="checkbox" class="row-check" data-order-id="${r.id}" ${isChecked ? "checked" : ""}
             style="width:16px;height:16px;margin:0;cursor:pointer;accent-color:#34c759">
      <span class="drag-handle" title="拖曳排序">⋮</span>
      <span class="po-number model-editable" data-order-id="${r.id}" title="雙擊可改機種">${esc(r.model)}</span>
      <span style="color:#c7c7cc;font-size:13px;text-align:center">|</span>
      <span class="po-number" style="color:#6b7280;font-weight:500">${esc(r.po_number || "")}</span>
      <span class="tag tag-pcb pcb-chip">${esc(r.pcb || "")}</span>
      <span style="font-size:13px;color:#3c3c43;font-weight:500;white-space:nowrap">${qty}<span style="font-size:11px;color:#8e8e93;font-weight:400">pcs</span></span>
      <span class="po-ship-date">${date}</span>
      <input class="code-input" type="text" value="${code}"
             data-order-id="${r.id}" placeholder="代碼"
             style="width:100%;box-sizing:border-box;border:1px solid #e5e5ea;border-radius:4px;padding:2px 6px;font-size:12px;text-align:center;background:transparent">
      ${remarkSpan}
      <span class="po-status-badge ${badge.cls}">${badge.text}</span>
      <div class="row-actions">
        <button class="btn-edit-date" data-order-id="${r.id}" title="修改日期">✎</button>
        <button class="btn-cancel-order" data-order-id="${r.id}" title="取消訂單">×</button>
      </div>
    </div>
    ${draftHtml}`;

  div.querySelector(".row-check")?.addEventListener("change", e => {
    const id = parseInt(e.target.dataset.orderId, 10);
    if (e.target.checked) _checkedIds.add(id);
    else _checkedIds.delete(id);
    recalculate();
    updateStatusOnly();
  });

  div.querySelector(".model-editable")?.addEventListener("dblclick", e => {
    const span = e.currentTarget;
    const currentModel = span.textContent;
    const input = document.createElement("input");
    input.type = "text";
    input.value = currentModel;
    input.className = "model-edit-input";
    input.style.cssText = "width:100%;box-sizing:border-box;border:1px solid #007aff;border-radius:4px;padding:2px 6px;font-size:14px;font-weight:600;outline:none;background:#fff";
    span.replaceWith(input);
    input.focus();
    input.select();

    const save = async () => {
      const newModel = input.value.trim();
      if (newModel && newModel !== currentModel) {
        try {
          await apiPatch(`/api/schedule/orders/${r.id}/model`, { model: newModel });
          showToast("機種已更新");
          await refresh();
          return;
        } catch (err) {
          showToast("機種更新失敗: " + err.message);
        }
      }
      const newSpan = document.createElement("span");
      newSpan.className = "po-number model-editable";
      newSpan.dataset.orderId = r.id;
      newSpan.title = "雙擊可改機種";
      newSpan.textContent = newModel || currentModel;
      input.replaceWith(newSpan);
      newSpan.addEventListener("dblclick", () => {
        div.querySelector(".model-editable")?.dispatchEvent(new MouseEvent("dblclick"));
      });
    };

    input.addEventListener("blur", save);
    input.addEventListener("keydown", event => {
      if (event.key === "Enter") { event.preventDefault(); input.blur(); }
      if (event.key === "Escape") { input.value = currentModel; input.blur(); }
    });
  });

  div.querySelector(".code-input")?.addEventListener("change", async e => {
    const orderId = parseInt(e.target.dataset.orderId, 10);
    const codeVal = e.target.value.trim();
    try {
      await apiPatch(`/api/schedule/orders/${orderId}/code`, { code: codeVal });
    } catch (_) {}
  });

  div.querySelector(".btn-complete")?.addEventListener("click", () => {
    if (draft) {
      void handleCommitDraft(draft.id, r.model);
      return;
    }
    if (r.status === "merged") {
      showToast("請先確認副檔後再提交");
      return;
    }
    void handleDispatch(r.id, r.model);
  });
  div.querySelector(".btn-edit-date")?.addEventListener("click", () => {
    void handleEditDelivery(r.id, r.delivery_date || r.ship_date || "");
  });
  div.querySelector(".btn-cancel-order")?.addEventListener("click", () => {
    void handleCancel(r.id, r.model);
  });

  if (draft) {
    div.querySelector(".btn-draft-edit")?.addEventListener("click", () => {
      void showDraftModal(draft.id, { readOnly: false });
    });
    div.querySelector(".btn-draft-download")?.addEventListener("click", () => {
      const selectedFileId = getSelectedDraftFileId(draft.id, draft.files || []);
      void downloadDraft(draft.id, selectedFileId);
    });
    div.querySelector(".btn-draft-delete")?.addEventListener("click", () => {
      void handleDeleteDraft(draft.id, r.model);
    });
  }

  return div;
}

function renderSchedule() {
  const container = document.getElementById("schedule-scroll");
  if (!container) return;
  container.innerHTML = "";

  if (!_rows.length) {
    container.innerHTML = '<div class="empty-state">尚未上傳排程表，或排程為空</div>';
    renderShortagePanel([], [], buildMainFileDeficitItems());
    return;
  }

  try {
    const resultMap = {};
    _calcResults.forEach((r, i) => { resultMap[_rows[i]?.id] = r; });
    const visibleShortageTotals = buildCheckedOrderVisibleShortageBadgeMap();

    _rows.forEach(row => {
      container.appendChild(buildRowCard(row, resultMap, visibleShortageTotals));
    });

    initSortable(container);

    const { shortages, csShortages } = buildRightPanelShortageData();
    const mainDeficits = buildMainFileDeficitItems();
    renderShortagePanel(shortages, csShortages, mainDeficits);
  } catch (error) {
    console.error("renderSchedule failed", error);
    container.innerHTML = `<div class="empty-state">畫面載入失敗：${esc(error?.message || "未知錯誤")}</div>`;
  }
}

async function handleSaveOrder() {
  const order_ids = [];
  document.querySelectorAll("#schedule-scroll .po-group[data-order-id]").forEach(div => {
    order_ids.push(parseInt(div.dataset.orderId));
  });
  try {
    await apiPost("/api/schedule/reorder", { order_ids });
    showToast("順序已儲存");
  } catch (e) { showToast("錯誤：" + e.message); }
}
