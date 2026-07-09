import { apiJson, apiFetch, apiPost, apiPatch, apiPut, showToast, hideToast, esc, fmt } from "./api.js";
import { calculate } from "./calculator.js";
import { desktopDownload, showDownloadToast } from "./desktop_bridge.js";

// ── State ─────────────────────────────────────────────────────────────────────
let _rows = [];
let _bomData = {};
let _stock = {};
let _liveStock = {};
let _moq = {};
let _vendors = {};
let _purchaseReminderStatuses = {};
let _partFirstOrder = {};
let _purchaseReminderCollapsed = { pending: false, notified: false, ignored: true };
let _purchaseReminderShowAll = false;
let _stStock = {};
let _stDescriptions = {};
let _dispatchedConsumption = {};
let _calcResults = [];
let _decisions = {};
let _draftsByOrderId = {};
let _orderSupplementsByOrderId = {};
let _orderSupplementDetailsByOrderId = {};
let _completedRows = [];
let _completedFolders = [];
let _completedDraftsByOrderId = {};
let _scheduleMeta = { filename: "", loaded_at: "", row_count: 0 };
let _onRefreshMain = null;
let _checkedIds = new Set();
let _completedCheckedIds = new Set();
let _completedLastCheckedId = null;
let _scheduleInitialized = false;
let _batchMergeInFlight = false;
let _modalProgressTimer = null;
let _modalProgressValue = 0;
let _completedFolderCollapsedState = loadCompletedFolderCollapsedState();
let _completedDraftPanelCollapsedState = loadCompletedDraftPanelCollapsedState();
let _draftPanelCollapsedState = {};
let _draftFileSelectionState = loadDraftFileSelectionState();
let _modalDraftId = null;
let _modalDraftReadOnly = false;
let _modalDraftBaseDecisions = {};
let _modalDraftBaseSupplements = {};
let _modalDraftVisibleParts = [];
let _modalCommitAfterSave = false;
let _modalResetStored = false;
let _modalPreviewAbortController = null;
let _modalPreviewDebounceTimer = null;
let _modalPreviewRequestSeq = 0;
let _globalBusyDepth = 0;
let _globalBusyProgressTimer = null;
let _globalBusyPercent = 0;
let _rightPanelMode = "shortages";
let _rightPanelActiveTab = "shortages";
let _lastShortagePanelData = { shortages: [], csShortages: [], mainDeficits: [] };
const ORDER_SCOPED_PART_PREFIXES = ["IC-STM", "IC-XC2C32", "IC-M24"];
const PURCHASE_REMINDER_PREFIXES = ["IC-", "OC-", "UC-"];
const PURCHASE_REMINDER_FALLBACK_THRESHOLD = 100;
const BATCH_MERGE_RESET_STORAGE_KEY = "shippingScheduler.batchMerge.resetStored";
const BATCH_MERGE_COMMIT_STORAGE_KEY = "shippingScheduler.batchMerge.commit";

// ── Public ────────────────────────────────────────────────────────────────────
export async function initSchedule(onRefreshMain) {
  _onRefreshMain = onRefreshMain || null;
  if (!_scheduleInitialized) {
    document.getElementById("btn-auto-sort").addEventListener("click", handleAutoSort);
    document.getElementById("btn-save-order").addEventListener("click", handleSaveOrder);
    document.getElementById("btn-batch-merge")?.addEventListener("click", handleBatchMerge);
    initBatchMergeOptions();
    document.getElementById("btn-dedup-schedule")?.addEventListener("click", handleDedupSchedule);
    document.getElementById("btn-manual-supplement")?.addEventListener("click", openManualSupplementModal);
    document.getElementById("btn-create-folder")?.addEventListener("click", handleCreateFolder);
    document.getElementById("btn-completed-download-drafts")?.addEventListener("click", handleCompletedDownloadDrafts);
    document.getElementById("btn-completed-gen-dispatch")?.addEventListener("click", handleCompletedGenerateDispatch);
    document.getElementById("completed-select-all")?.addEventListener("change", handleCompletedSelectAllChange);
    document.getElementById("schedule-scroll")?.addEventListener("click", handleDraftPanelToggleClick);
    document.querySelectorAll("[data-right-panel-tab]").forEach(btn => {
      btn.addEventListener("click", () => activateRightPanelTab(btn.dataset.rightPanelTab));
    });
    document.getElementById("right-scroll")?.addEventListener("dblclick", async (e) => {
      const card = e.target.closest(".shortage-item");
      if (!card) return;
      const part = card.dataset.part || card.querySelector("[data-part]")?.dataset.part;
      if (!part) return;
      const codeTagEl = card.querySelector(".tag-pcb");
      const batchCode = codeTagEl?.textContent?.trim() || card.dataset.rowCode || "";
      document.querySelector('[data-tab="main-preview-v2"]')?.click();
      try {
        const mod = await import("./main_preview_v2.js");
        await new Promise(r => setTimeout(r, 350));
        const ok = await mod.navigateToPart(part, batchCode);
        if (!ok) showToast(`主檔找不到 ${part}`, { tone: "error" });
      } catch (err) {
        console.error("[shortage dblclick] navigate failed", err);
      }
    });
    _scheduleInitialized = true;
  }
  await refresh();
}

export async function refresh() {
  await Promise.all([loadMainData(), loadStInventoryData(), loadScheduleRows(), loadBomData(), loadCompletedRows()]);
  recalculate();
  renderSchedule();
}

export async function refreshScheduleOnly() {
  await loadScheduleRows();
  recalculate();
  renderSchedule();
}

function waitForNextFrame() {
  return new Promise(resolve => window.requestAnimationFrame(() => resolve()));
}

export async function refreshCompleted() {
  await loadCompletedRows();
  syncPostDispatchShortagesFromCompletedDrafts();
  if (_rightPanelMode === "postDispatch") renderPostDispatchPanel();
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

function getCompletedCheckedOrderIds() {
  const validIds = new Set(_completedRows.map(row => normalizeOrderId(row?.id)).filter(Number.isInteger));
  return [..._completedCheckedIds].filter(id => validIds.has(id));
}

function getRenderedCompletedCheckControls() {
  return Array.from(document.querySelectorAll("#completed-scroll .completed-order-check"))
    .map(checkbox => ({
      checkbox,
      id: normalizeOrderId(checkbox.dataset.orderId),
    }))
    .filter(item => Number.isInteger(item.id));
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

function normalizeCompletedFolderPath(value) {
  return String(value || "")
    .split("/")
    .map(part => part.trim())
    .filter(Boolean)
    .join("/");
}

function validateCompletedFolderPath(folderName) {
  const raw = String(folderName || "").trim();
  if (!raw) return "請輸入資料夾名稱";
  const parts = raw.split("/").map(part => part.trim());
  if (parts.some(part => !part)) return "資料夾路徑不可有空白層級";
  if (parts.length > 3) return "資料夾最多 3 層";
  return "";
}

function compareCompletedFolderName(a, b) {
  return String(a || "").localeCompare(String(b || ""), "zh-Hant", { numeric: true, sensitivity: "base" });
}

function buildCompletedFolderTree(rows = _completedRows, folders = _completedFolders) {
  const root = { name: "", path: "", rows: [], children: new Map(), totalCount: 0, depth: 0 };
  const seenFolderPaths = new Set();
  const seenOrderKeys = new Set();
  const ensureNode = (folderPath) => {
    const normalized = normalizeCompletedFolderPath(folderPath);
    if (!normalized) return root;
    let current = root;
    const parts = normalized.split("/");
    let path = "";
    for (const part of parts) {
      path = path ? `${path}/${part}` : part;
      if (!current.children.has(part)) {
        current.children.set(part, { name: part, path, rows: [], children: new Map(), totalCount: 0, depth: current.depth + 1 });
      }
      current = current.children.get(part);
    }
    return current;
  };

  for (const folder of folders || []) {
    const normalized = normalizeCompletedFolderPath(folder);
    if (!normalized || seenFolderPaths.has(normalized)) continue;
    seenFolderPaths.add(normalized);
    ensureNode(normalized);
  }
  for (const row of rows || []) {
    const id = normalizeOrderId(row?.id);
    const key = Number.isInteger(id)
      ? `id:${id}`
      : `row:${String(row?.code || "")}|${String(row?.po_number || "")}|${String(row?.model || "")}`;
    if (seenOrderKeys.has(key)) continue;
    seenOrderKeys.add(key);
    const folder = normalizeCompletedFolderPath(row?.folder);
    ensureNode(folder).rows.push(row);
  }

  const updateTotals = (node) => {
    node.totalCount = node.rows.length;
    node.sortedChildren = [...node.children.values()].sort((a, b) => compareCompletedFolderName(a.name, b.name));
    for (const child of node.sortedChildren) {
      node.totalCount += updateTotals(child);
    }
    return node.totalCount;
  };
  updateTotals(root);
  return root;
}

function flattenCompletedFolderTree(node) {
  const folders = [];
  const visit = (current) => {
    for (const child of current.sortedChildren || []) {
      folders.push({ path: child.path, depth: child.depth });
      visit(child);
    }
  };
  visit(node);
  return folders;
}

function getCompletedFolderParent(folderPath) {
  const normalized = normalizeCompletedFolderPath(folderPath);
  if (!normalized || !normalized.includes("/")) return "";
  return normalized.split("/").slice(0, -1).join("/");
}

function getCompletedFolderName(folderPath) {
  const normalized = normalizeCompletedFolderPath(folderPath);
  return normalized ? normalized.split("/").pop() : "";
}

function buildCompletedFolderParentOptions(folderName, allFolders) {
  const currentParent = getCompletedFolderParent(folderName);
  let options = `<option value=""${currentParent === "" ? " selected" : ""}>(最上層)</option>`;
  for (const f of allFolders || []) {
    const path = normalizeCompletedFolderPath(f.path);
    if (!path || path === folderName || path.startsWith(`${folderName}/`)) continue;
    const indent = "　".repeat(Math.max(0, (f.depth || 1) - 1));
    options += `<option value="${esc(path)}"${currentParent === path ? " selected" : ""}>${indent}${esc(path)}</option>`;
  }
  return options;
}

function moveCompletedEmptyFolderLocal(folderName, newParent) {
  const source = normalizeCompletedFolderPath(folderName);
  const parent = normalizeCompletedFolderPath(newParent);
  const targetRoot = [parent, getCompletedFolderName(source)].filter(Boolean).join("/");
  const sourcePrefix = `${source}/`;
  _completedFolders = (_completedFolders || []).map(item => {
    const path = normalizeCompletedFolderPath(item);
    if (path === source) return targetRoot;
    if (path.startsWith(sourcePrefix)) return `${targetRoot}/${path.slice(sourcePrefix.length)}`;
    return path;
  });
}

function getCompletedFolderMoveDepth(folderName, newParent, allFolders) {
  const source = normalizeCompletedFolderPath(folderName);
  const parent = normalizeCompletedFolderPath(newParent);
  const targetDepth = (parent ? parent.split("/").length : 0) + 1;
  const sourceDepth = source ? source.split("/").length : 0;
  let maxExtraDepth = 0;
  for (const f of allFolders || []) {
    const path = normalizeCompletedFolderPath(f.path);
    if (path === source || path.startsWith(`${source}/`)) {
      maxExtraDepth = Math.max(maxExtraDepth, path.split("/").length - sourceDepth);
    }
  }
  return targetDepth + maxExtraDepth;
}

function appendCompletedFolderSection(container, folderNode, allFolders, folderTree, renderedFolderPaths) {
  const folderName = normalizeCompletedFolderPath(folderNode?.path);
  if (folderName) {
    if (renderedFolderPaths.has(folderName)) return;
    renderedFolderPaths.add(folderName);
  }
  container.appendChild(buildFolderSection(folderNode, allFolders, folderTree, renderedFolderPaths));
}

function completedFolderHasChildren(folderName, tree) {
  const normalized = normalizeCompletedFolderPath(folderName);
  if (!normalized) return false;
  let current = tree;
  for (const part of normalized.split("/")) {
    current = current?.children?.get(part);
    if (!current) return false;
  }
  return Boolean(current.children?.size);
}

function loadCompletedDraftPanelCollapsedState() {
  try {
    const raw = window.localStorage?.getItem("completed-draft-panel-collapsed-state");
    const parsed = raw ? JSON.parse(raw) : {};
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_) {
    return {};
  }
}

function saveCompletedDraftPanelCollapsedState() {
  try {
    window.localStorage?.setItem(
      "completed-draft-panel-collapsed-state",
      JSON.stringify(_completedDraftPanelCollapsedState || {}),
    );
  } catch (_) {}
}

function isCompletedDraftPanelCollapsed(orderId) {
  const key = String(orderId || "");
  if (!key) return true;
  if (Object.prototype.hasOwnProperty.call(_completedDraftPanelCollapsedState, key)) {
    return Boolean(_completedDraftPanelCollapsedState[key]);
  }
  return true;
}

function setCompletedDraftPanelCollapsed(orderId, collapsed) {
  const key = String(orderId || "");
  if (!key) return;
  _completedDraftPanelCollapsedState[key] = Boolean(collapsed);
  saveCompletedDraftPanelCollapsedState();
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

function setGlobalBusyPercent(percent, phase = "") {
  const value = Math.max(0, Math.min(100, Math.round(Number(percent) || 0)));
  const fill = document.querySelector("#action-busy-overlay .busy-bar-fill");
  const percentEl = document.getElementById("busy-overlay-percent");
  const phaseEl = document.getElementById("busy-overlay-phase");
  _globalBusyPercent = value;
  if (fill) fill.style.width = `${value}%`;
  if (percentEl) percentEl.textContent = `${value}%`;
  if (phaseEl && phase) phaseEl.textContent = phase;
}

function stopGlobalBusyProgress() {
  if (_globalBusyProgressTimer) {
    clearInterval(_globalBusyProgressTimer);
    _globalBusyProgressTimer = null;
  }
}

function startGlobalBusyProgress({ initialPercent = 6, ceilingPercent = 92, phase = "處理中" } = {}) {
  stopGlobalBusyProgress();
  setGlobalBusyPercent(initialPercent, phase);
  _globalBusyProgressTimer = setInterval(() => {
    if (_globalBusyPercent >= ceilingPercent) return;
    const remaining = ceilingPercent - _globalBusyPercent;
    const step = remaining > 35 ? 4 : remaining > 18 ? 2 : 1;
    setGlobalBusyPercent(Math.min(ceilingPercent, _globalBusyPercent + step));
  }, 700);
}

function setGlobalBusyState(active, {
  title = "系統正在處理中",
  detail = "大型批次可能需要幾秒鐘，請稍候，不用重複點擊。",
  phase = "處理中",
  initialPercent = 6,
  ceilingPercent = 92,
} = {}) {
  const overlay = document.getElementById("action-busy-overlay");
  if (!overlay) return;
  const titleEl = document.getElementById("busy-overlay-title");
  const detailEl = document.getElementById("busy-overlay-detail");

  if (active) {
    _globalBusyDepth += 1;
    if (titleEl) titleEl.textContent = title;
    if (detailEl) detailEl.textContent = detail;
    if (_globalBusyDepth === 1) {
      startGlobalBusyProgress({ initialPercent, ceilingPercent, phase });
    }
    overlay.style.display = "flex";
    return;
  }

  _globalBusyDepth = Math.max(0, _globalBusyDepth - 1);
  if (_globalBusyDepth === 0) {
    stopGlobalBusyProgress();
    setGlobalBusyPercent(0, "處理中");
    overlay.style.display = "none";
  }
}

export async function withGlobalBusy(task, options = {}) {
  const timeoutMs = options.timeout || 600000;
  hideToast();
  setGlobalBusyState(true, options);
  let succeeded = false;
  try {
    const timeoutPromise = new Promise((_, reject) =>
      setTimeout(() => reject(new Error("操作逾時，請重新整理頁面後再試")), timeoutMs),
    );
    const result = await Promise.race([task(), timeoutPromise]);
    succeeded = true;
    setGlobalBusyPercent(100, "完成");
    await new Promise(resolve => setTimeout(resolve, 160));
    return result;
  } finally {
    if (!succeeded) setGlobalBusyPercent(100, "中止");
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

function readStoredBoolean(key, fallback = false) {
  try {
    const value = localStorage.getItem(key);
    if (value === null) return fallback;
    return value === "1" || value === "true";
  } catch (_) {
    return fallback;
  }
}

function writeStoredBoolean(key, value) {
  try {
    localStorage.setItem(key, value ? "1" : "0");
  } catch (_) {}
}

function getBatchMergeOptions() {
  return {
    resetStored: Boolean(document.getElementById("batch-merge-reset-stored")?.checked),
    commitAfterModal: Boolean(document.getElementById("batch-merge-commit")?.checked),
  };
}

function formatBatchMergeMode({ resetStored = false, commitAfterModal = false } = {}) {
  const resetText = resetStored ? "重算補料" : "沿用補料";
  const commitText = commitAfterModal ? "寫主檔" : "只建副檔";
  return `模式：${resetText}＋${commitText}`;
}

function updateBatchMergeModeLabel() {
  const label = document.getElementById("batch-merge-mode");
  if (label) label.textContent = formatBatchMergeMode(getBatchMergeOptions());
}

function initBatchMergeOptions() {
  const resetInput = document.getElementById("batch-merge-reset-stored");
  const commitInput = document.getElementById("batch-merge-commit");
  if (resetInput) resetInput.checked = readStoredBoolean(BATCH_MERGE_RESET_STORAGE_KEY, false);
  if (commitInput) commitInput.checked = readStoredBoolean(BATCH_MERGE_COMMIT_STORAGE_KEY, false);
  resetInput?.addEventListener("change", () => {
    writeStoredBoolean(BATCH_MERGE_RESET_STORAGE_KEY, resetInput.checked);
    updateBatchMergeModeLabel();
  });
  commitInput?.addEventListener("change", () => {
    writeStoredBoolean(BATCH_MERGE_COMMIT_STORAGE_KEY, commitInput.checked);
    updateBatchMergeModeLabel();
  });
  updateBatchMergeModeLabel();
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
    _vendors = d.vendors || {};
    _purchaseReminderStatuses = d.purchase_reminder_statuses || {};
    _partFirstOrder = d.part_first_order || {};
  } catch (_) { _stock = {}; _liveStock = {}; _moq = {}; _vendors = {}; _purchaseReminderStatuses = {}; _partFirstOrder = {}; }
}

async function loadStInventoryData() {
  try {
    const d = await apiJson("/api/system/st-inventory/data");
    _stStock = d.stock || {};
    _stDescriptions = d.descriptions || {};
  } catch (_) { _stStock = {}; _stDescriptions = {}; }
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
    _completedDraftsByOrderId = d.committed_merge_drafts || {};
    const validIds = new Set(_completedRows.map(row => normalizeOrderId(row?.id)).filter(Number.isInteger));
    _completedCheckedIds = new Set([..._completedCheckedIds].filter(id => validIds.has(id)));
    if (!validIds.has(_completedLastCheckedId)) _completedLastCheckedId = null;
  } catch (_) {
    _completedRows = [];
    _completedFolders = [];
    _completedDraftsByOrderId = {};
    _completedLastCheckedId = null;
  }
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
    renderShortagePanel(buildMainStockNegativeItems(), [], []);
    return;
  }

  const resultMap = {};
  _calcResults.forEach((r, i) => { resultMap[_rows[i]?.id] = r; });

  for (const r of _rows) {
    container.appendChild(buildRowCard(r, resultMap));
  }

  initSortable(container);

  const { shortages: rpShortages, csShortages: rpCSShortages } = buildRightPanelShortageData();
  renderShortagePanel(rpShortages, rpCSShortages, []);
}

function formatDraftTime(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.slice(5, 16).replace("T", " ");
}

function hasUsableDraftFiles(draft) {
  return Array.isArray(draft?.files) && draft.files.some(file => (
    String(file?.id || "").trim() || String(file?.filename || "").trim()
  ));
}

function formatOrderAlertLabel(row) {
  const scopeLabel = formatShortageScopeLabel(row?.model, row?.code);
  const poNumber = String(row?.po_number || "").trim();
  return poNumber ? `${scopeLabel}（PO ${poNumber}）` : scopeLabel;
}

function summarizeOrderAlertLabels(rows = [], limit = 3) {
  const labels = (rows || [])
    .map(formatOrderAlertLabel)
    .filter(Boolean);
  if (!labels.length) return "";
  if (labels.length <= limit) return labels.join("、");
  return `${labels.slice(0, limit).join("、")}，另 ${labels.length - limit} 筆`;
}

function syncRowBadge(div, badgeState = { cls: "", text: "" }) {
  if (!div) return;
  const header = div.querySelector(".po-group-header");
  const actions = div.querySelector(".row-actions");
  if (!header || !actions) return;

  let badge = div.querySelector(".po-status-badge");
  if (!badgeState?.text) {
    badge?.remove();
    return;
  }

  if (!badge) {
    badge = document.createElement("span");
    header.insertBefore(badge, actions);
  }

  badge.className = `po-status-badge ${badgeState.cls || ""}`.trim();
  badge.textContent = badgeState.text;
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
  const rawDraft = _draftsByOrderId?.[r.id] || null;
  const draft = hasUsableDraftFiles(rawDraft) ? rawDraft : null;
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
  const badgeHtml = badge?.text
    ? `<span class="po-status-badge ${badge.cls}">${badge.text}</span>`
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
      ${badgeHtml}
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
          await refreshScheduleOnly();
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
  if (!res && orderId !== undefined && !_checkedIds.has(orderId)) return { cls: "", text: "" };
  if (!res) return { cls: "badge-no-bom", text: "BOM未上傳" };
  if (res.status === "ok") return { cls: "", text: "" };
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
      order_id: meta._order_id,
    });
  }
  return scopes;
}

function shortageGroupHeadingHtml(label, poNumber = "", { compact = false, sampleControlHtml = "" } = {}) {
  const safeLabel = esc(label || "未指定機種");
  const poText = String(poNumber || "").trim();
  const poHtml = poText
    ? `<span style="font-size:${compact ? "10px" : "11px"};font-weight:500;color:#6b7280">PO ${esc(poText)}</span>`
    : "";

  if (compact) {
    return `<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin:8px 0 4px;font-size:11px;font-weight:600;color:#6b7280"><span>${safeLabel}</span>${poHtml}</div>`;
  }

  return `<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;margin:12px 0 8px;padding:6px 10px;background:#f3f4f6;border-radius:6px;font-weight:600;font-size:13px;color:#1f2937"><span>${safeLabel}</span><span style="display:flex;align-items:center;gap:10px">${poHtml}${sampleControlHtml}</span></div>`;
}

function sampleOrderCheckboxHtml(scope = {}, checked = false) {
  const orderId = normalizeOrderId(scope.order_id);
  if (!Number.isInteger(orderId)) return "";
  return `<label style="display:flex;align-items:center;gap:4px;font-size:12px;font-weight:500;color:#374151;white-space:nowrap">
    <input type="checkbox" class="sample-order-flag" data-order-id="${orderId}" ${checked ? "checked" : ""}>
    打樣（此單 EC 料不強制補到 100）
  </label>`;
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

function buildShortageSupplySuggestion(partNumber, shortageAmount, moq = 0, stStockQty = 0) {
  const normalizedShortage = Math.max(0, Number(shortageAmount || 0) || 0);
  const normalizedMoq = Math.max(0, Number(moq || 0) || 0);
  const normalizedStStock = Math.max(0, Number(stStockQty || 0) || 0);
  const isOrderScoped = isOrderScopedPart(partNumber);
  const suggestedQty = isOrderScoped
    ? normalizedShortage
    : normalizedMoq > 0
      ? Math.ceil(normalizedShortage / normalizedMoq) * normalizedMoq
      : normalizedShortage;
  const stAvailableQty = isOrderScoped
    ? Math.min(normalizedShortage, normalizedStStock)
    : Math.min(suggestedQty, normalizedStStock);
  const purchaseNeededQty = Math.max(0, normalizedShortage - stAvailableQty);
  const purchaseSuggestedQty = isOrderScoped
    ? purchaseNeededQty
    : purchaseNeededQty > 0
      ? (normalizedMoq > 0 ? Math.ceil(purchaseNeededQty / normalizedMoq) * normalizedMoq : purchaseNeededQty)
      : 0;
  return {
    suggested_qty: normalizedShortage > 0 ? suggestedQty : 0,
    st_available_qty: stAvailableQty,
    purchase_needed_qty: purchaseNeededQty,
    purchase_suggested_qty: purchaseSuggestedQty,
    needs_purchase: purchaseNeededQty > 0,
  };
}

function getRightPanelSupplementQty(item, storedSupplementsByPart = {}) {
  if (Number(item?.default_supplement || 0) > 0) return Number(item?.default_supplement || 0);
  if (Number(item?.supplement_qty || 0) > 0) return Number(item?.supplement_qty || 0);
  const partKey = normalizePartKey(item?.part_number);
  const storedQty = Number(storedSupplementsByPart?.[partKey] || 0) || 0;
  const lookaheadSuggestedQty = Number(item?._lookahead_suggested_qty || 0) || 0;
  if (storedQty > 0 || lookaheadSuggestedQty > 0) return Math.max(storedQty, lookaheadSuggestedQty);
  return Number(item?.suggested_qty || 0) || 0;
}

function applyRightPanelSupplementState(item, storedSupplementsByPart = {}) {
  const supplementQty = getRightPanelSupplementQty(item, storedSupplementsByPart);
  const resultingStock = computeShortageResultingStock(item, supplementQty);
  return {
    ...item,
    supplement_qty: supplementQty,
    default_supplement: supplementQty,
    resulting_stock: resultingStock,
  };
}

function getRightPanelResultingStock(item, storedSupplementsByPart = {}) {
  const enriched = applyRightPanelSupplementState(item, storedSupplementsByPart);
  return Number(enriched?.resulting_stock);
}

function shouldRenderRightPanelShortageItem(item, storedSupplementsByPart = {}) {
  const shortageAmount = Number(item?.shortage_amount || 0);
  if (!Number.isFinite(shortageAmount) || shortageAmount <= 0) return false;
  const resultingStock = getRightPanelResultingStock(item, storedSupplementsByPart);
  return shouldRenderRightPanelActionableShortage(item?.part_number, resultingStock, shortageAmount);
}

function shouldRenderRightPanelActionableShortage(partNumber, resultingStock, shortageAmount = 0) {
  const amount = Number(shortageAmount || 0);
  if (!Number.isFinite(amount) || amount <= 0) return false;

  const stock = Number(resultingStock);
  if (isEcPart(partNumber) && Number.isFinite(stock)) {
    return stock < 0;
  }

  return Number.isFinite(stock)
    ? hasRemainingShortageForResultingStock(partNumber, stock)
    : amount > 0;
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
  setRightPanelBadge(remainingRows);
  updateRightPanelTabs(remainingRows);
  if (!remainingRows) {
    scroll.innerHTML = '<div class="no-shortage-msg">無缺料</div>';
  }
}

function getEffectiveShortageState(row, res = null) {
  const rawDraft = row ? _draftsByOrderId?.[row.id] || null : null;
  const draft = hasUsableDraftFiles(rawDraft) ? rawDraft : null;
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
    return { cls: "", text: "" };
  }

  const orderId = normalizeOrderId(row?.id);
  const effective = getEffectiveShortageState(row, res);
  if (Number.isInteger(orderId) && _checkedIds.has(orderId) && visibleShortageTotals instanceof Map && (effective.hasDraft || res)) {
    const total = Number(visibleShortageTotals.get(orderId) || 0);
    return total > 0
      ? { cls: "badge-shortage", text: `缺 ${fmt(roundShortageUiValue(total))}` }
      : effective.status === "no_bom"
        ? { cls: "badge-no-bom", text: "BOM未上傳" }
        : { cls: "", text: "" };
  }

  if (effective.hasDraft) {
    const total = [...effective.shortages, ...effective.customer_material_shortages]
      .reduce((sum, item) => sum + (item.shortage_amount || 0), 0);
    return total > 0
      ? { cls: "badge-shortage", text: `缺 ${fmt(roundShortageUiValue(total))}` }
      : { cls: "", text: "" };
  }

  return cardBadge(res, row?.id);
}

function buildRightPanelShortageData() {
  const shortagesByModel = {};
  const csShortagesByModel = {};
  const checkedRows = _rows.filter(row => _checkedIds.has(row.id));
  const orderedScopes = buildShortageScopeList(checkedRows);
  const allModels = orderedScopes.map(scope => scope.key);
  const storedSupplementsByPart = {};

  checkedRows.forEach(row => {
    const orderId = normalizeOrderId(row?.id);
    if (!Number.isInteger(orderId)) return;
    Object.entries(_orderSupplementsByOrderId?.[orderId] || {}).forEach(([rawPart, rawQty]) => {
      const partKey = normalizePartKey(rawPart);
      const qty = Number(rawQty || 0) || 0;
      if (!partKey || qty <= 0) return;
      if (isOrderScopedPart(partKey)) {
        // ORDER_SCOPED 料每筆 order 各自配 ST，跨 order 應累加成 batch 總補量。
        storedSupplementsByPart[partKey] = (storedSupplementsByPart[partKey] || 0) + qty;
      } else {
        // EC 等共享 pool 料：跨 order 共享 stock，避免單一 supplement 被算多次。
        storedSupplementsByPart[partKey] = Math.max(storedSupplementsByPart[partKey] || 0, qty);
      }
    });
  });

  checkedRows.forEach(row => {
    const index = _rows.findIndex(item => item.id === row.id);
    if (index < 0) return;

    const model = buildShortageGroupMeta(row)._row_group_key;
    const effective = getEffectiveShortageState(row, _calcResults[index]);
    if (!shortagesByModel[model]) shortagesByModel[model] = [];
    if (!csShortagesByModel[model]) csShortagesByModel[model] = [];
    if (!allModels.includes(model)) allModels.push(model);

    for (const item of (effective.shortages || [])) {
      const partKey = normalizePartKey(item?.part_number);
      shortagesByModel[model].push(applyRightPanelSupplementState({
        ...item,
        decision: _decisions[partKey] || item?.decision || "None",
      }, storedSupplementsByPart));
    }
    for (const item of (effective.customer_material_shortages || [])) {
      const partKey = normalizePartKey(item?.part_number);
      csShortagesByModel[model].push(applyRightPanelSupplementState({
        ...item,
        decision: _decisions[partKey] || item?.decision || "None",
      }, storedSupplementsByPart));
    }
  });

  _consolidateShortagesAcrossModels(shortagesByModel, allModels, {
    preserveOrderScopedParts: true,
    preserveShortageDecisions: true,
  });

  for (const [model, items] of Object.entries(shortagesByModel)) {
    shortagesByModel[model] = items.map(item => applyRightPanelSupplementState({
      ...item,
      default_supplement: 0,
      supplement_qty: 0,
    }, storedSupplementsByPart));
  }
  for (const items of Object.values(shortagesByModel)) items.sort(compareShortageItems);
  for (const items of Object.values(csShortagesByModel)) items.sort(compareShortageItems);

  const shortages = [];
  const csShortages = [];
  for (const model of allModels) {
    shortages.push(...(shortagesByModel[model] || []).filter(item => shouldRenderRightPanelShortageItem(item, storedSupplementsByPart)));
    csShortages.push(...(csShortagesByModel[model] || []).filter(item => shouldRenderRightPanelShortageItem(item, storedSupplementsByPart)));
  }

  const shortagePartKeys = new Set(shortages.map(item => normalizePartKey(item?.part_number)).filter(Boolean));
  const mainStockItems = buildMainStockNegativeItems()
    .filter(item => !shortagePartKeys.has(normalizePartKey(item?.part_number)));
  shortages.push(...mainStockItems);

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

function toSchedOrderQty(row) {
  const v = Number(row?.order_qty || 0);
  return Number.isFinite(v) && v > 0 ? v : 0;
}

function effectiveNeededFromBomComp(comp, scheduleQty) {
  const qpb = Number(comp.qty_per_board || 0) || 0;
  if (scheduleQty > 0 && qpb > 0) {
    const scrap = Math.max(0, Number(comp.scrap_factor || 0) || 0);
    return qpb * scheduleQty * (1 + scrap);
  }
  return Number(comp.needed_qty || 0) || 0;
}

function buildPartDescriptionLookup() {
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
  return descLookup;
}

function getDisplayMinStock(partNumber) {
  const normalized = normalizePartKey(partNumber);
  if (normalized.startsWith("EC-6")) return 0;
  if (normalized.startsWith("EC-")) return 100;
  if (normalized.startsWith("PK-")) return normalized.startsWith("PK-50070") ? 0 : 1;
  return 0;
}

function calculateDisplayShortageAmount(partNumber, endingStock) {
  return Math.max(0, getDisplayMinStock(partNumber) - Number(endingStock || 0));
}

/** 從主檔目前庫存找出已違反 shortage rule 門檻的料號。 */
function buildMainStockNegativeItems() {
  if (!_stock || !Object.keys(_stock).length) return [];

  const descLookup = buildPartDescriptionLookup();
  const items = [];
  for (const [part, stockQty] of Object.entries(_stock)) {
    const key = normalizePartKey(part);
    if (!key) continue;

    const currentStock = Number(stockQty || 0);
    if (!Number.isFinite(currentStock)) continue;
    if (currentStock >= 0) continue;

    const threshold = getDisplayMinStock(key);
    const shortageAmount = calculateDisplayShortageAmount(key, currentStock);
    if (shortageAmount <= 0) continue;

    const moq = Math.max(0, Number(_moq?.[key] || 0) || 0);
    const stStockQty = Math.max(0, Number(_stStock?.[key] ?? 0) || 0);
    const suggestion = buildShortageSupplySuggestion(key, shortageAmount, moq, stStockQty);
    const firstOrderCode = String(_partFirstOrder?.[key] || "").trim() || "主檔";
    items.push({
      part_number: key,
      description: descLookup[key] || "",
      vendor: normalizeVendorName(_vendors?.[key]),
      current_stock: currentStock,
      resulting_stock: currentStock,
      shortage_amount: shortageAmount,
      threshold,
      needed: threshold,
      prev_qty_cs: 0,
      moq,
      suggested_qty: suggestion.suggested_qty,
      st_stock_qty: stStockQty,
      st_available_qty: suggestion.st_available_qty,
      purchase_needed_qty: suggestion.purchase_needed_qty,
      purchase_suggested_qty: suggestion.purchase_suggested_qty,
      needs_purchase: suggestion.needs_purchase,
      decision: "None",
      _row_code: firstOrderCode,
      _row_model: "",
      _row_group_key: "主檔",
      _row_group_label: "主檔層級缺料",
      _row_order_index: Number.MAX_SAFE_INTEGER,
      _po_number: "",
      _main_stock_level: true,
    });
  }

  items.sort(compareShortageItems);
  return items;
}

/** 從主檔庫存中找出已經確定缺料的料號（庫存 < 安全水位）。 */
function buildMainFileDeficitItems() {
  if (!_liveStock || !Object.keys(_liveStock).length) return [];

  const descLookup = buildPartDescriptionLookup();

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

function isPurchaseReminderPart(partNumber) {
  const key = normalizePartKey(partNumber);
  return PURCHASE_REMINDER_PREFIXES.some(prefix => key.startsWith(prefix)) && !isOpenTextPart(key);
}

function isOpenTextPart(partNumber) {
  return normalizePartKey(partNumber).endsWith("-TAB");
}

function getPurchaseReminderThreshold(partNumber) {
  const key = normalizePartKey(partNumber);
  const moq = Math.max(0, Number(_moq?.[key] || 0) || 0);
  return moq > 0 ? moq : PURCHASE_REMINDER_FALLBACK_THRESHOLD;
}

function normalizeVendorName(value) {
  return String(value || "").trim() || "未分類廠商";
}

function buildMainPurchaseReminderPartKeys() {
  const keys = new Set();
  [_vendors, _liveStock, _stock, _moq].forEach(source => {
    Object.keys(source || {}).forEach(part => {
      const key = normalizePartKey(part);
      if (key && isPurchaseReminderPart(key)) keys.add(key);
    });
  });
  return [...keys].sort((a, b) => compareText(a, b));
}

function buildActivePurchaseReminderPartKeys() {
  const keys = new Set();
  for (const row of Array.isArray(_rows) ? _rows : []) {
    const modelKey = String(row?.model || "").trim().toUpperCase();
    if (!modelKey) continue;
    const bomEntry = _bomData?.[modelKey];
    for (const comp of (bomEntry?.components || [])) {
      const partKey = normalizePartKey(comp?.part_number);
      if (partKey && isPurchaseReminderPart(partKey)) keys.add(partKey);
    }
  }
  return keys;
}

function buildActivePurchaseReminderUsageMap() {
  const usageMap = new Map();
  for (const row of Array.isArray(_rows) ? _rows : []) {
    const model = String(row?.model || "").trim();
    const modelKey = model.toUpperCase();
    if (!modelKey) continue;
    const bomEntry = _bomData?.[modelKey];
    const orderQty = toSchedOrderQty(row);
    const usageByPart = new Map();
    for (const comp of (bomEntry?.components || [])) {
      const partKey = normalizePartKey(comp?.part_number);
      if (!partKey || !isPurchaseReminderPart(partKey) || comp?.is_dash) continue;
      const usedQty = effectiveNeededFromBomComp(comp, orderQty);
      if (usedQty <= 0) continue;
      usageByPart.set(partKey, roundShortageUiValue((usageByPart.get(partKey) || 0) + usedQty));
    }
    if (!usageByPart.size) continue;

    const orderId = normalizeOrderId(row?.id);
    const code = String(row?.code || "").trim();
    const po = String(row?.po_number || "").trim();
    const shipDate = String(row?.ship_date || row?.delivery_date || "").trim();
    for (const [partKey, usedQty] of usageByPart.entries()) {
      if (!usageMap.has(partKey)) usageMap.set(partKey, []);
      usageMap.get(partKey).push({
        order_id: Number.isInteger(orderId) ? orderId : null,
        model,
        code,
        po_number: po,
        ship_date: shipDate,
        used_qty: usedQty,
      });
    }
  }
  return usageMap;
}

function buildPurchaseReminderItems(options = {}) {
  if (!_stStock || !Object.keys(_stStock).length) return [];

  const includeInactive = Boolean(options.includeInactive);
  const descLookup = buildPartDescriptionLookup();
  const items = [];
  const mainPartKeys = buildMainPurchaseReminderPartKeys();
  const activePartKeys = buildActivePurchaseReminderPartKeys();
  const activeUsageMap = buildActivePurchaseReminderUsageMap();
  for (const key of mainPartKeys) {
    const activeDemand = activePartKeys.has(key);
    if (!includeInactive && !_purchaseReminderShowAll && !activeDemand) continue;

    const currentStock = Number(_stStock?.[key] ?? 0);
    if (!Number.isFinite(currentStock)) continue;

    const moq = Math.max(0, Number(_moq?.[key] || 0) || 0);
    const threshold = getPurchaseReminderThreshold(key);
    const mainStock = Number.isFinite(Number(_liveStock?.[key]))
      ? Number(_liveStock?.[key])
      : (Number.isFinite(Number(_stock?.[key])) ? Number(_stock?.[key]) : 0);
    const activeUsedQty = (activeUsageMap.get(key) || []).reduce(
      (sum, row) => sum + (Number(row?.used_qty || 0) || 0),
      0,
    );
    const combinedStock = currentStock + Math.max(0, mainStock);
    const projectedAvailable = combinedStock - activeUsedQty;
    if (projectedAvailable >= threshold) continue;

    const neededToThreshold = Math.max(0, threshold - projectedAvailable);
    const suggestedQty = moq > 0
      ? Math.max(moq, Math.ceil(Math.max(neededToThreshold, 1) / moq) * moq)
      : Math.max(PURCHASE_REMINDER_FALLBACK_THRESHOLD, neededToThreshold);
    const status = combinedStock <= 0
      ? "可用庫存已見底"
      : (combinedStock < threshold ? "可用庫存低於安全線" : "排程後低於安全線");

    items.push({
      part_number: key,
      vendor: normalizeVendorName(_vendors?.[key]),
      description: _stDescriptions?.[key] || descLookup[key] || "",
      current_stock: currentStock,
      main_stock: mainStock,
      active_used_qty: activeUsedQty,
      projected_available: projectedAvailable,
      threshold,
      moq,
      suggested_qty: suggestedQty,
      status,
      active_demand: activeDemand,
      notified: Boolean(_purchaseReminderStatuses?.[key]?.notified),
      notified_at: _purchaseReminderStatuses?.[key]?.notified_at || "",
      notification_note: _purchaseReminderStatuses?.[key]?.note || "",
      ignored: Boolean(_purchaseReminderStatuses?.[key]?.ignored),
      ignored_at: _purchaseReminderStatuses?.[key]?.ignored_at || "",
      used_by: activeUsageMap.get(key) || [],
    });
  }

  items.sort((a, b) => (
    Number(a.ignored) - Number(b.ignored)
    || Number(a.notified) - Number(b.notified)
    || compareText(a.vendor, b.vendor, "zh-Hant")
    || Number(a.current_stock || 0) - Number(b.current_stock || 0)
    || compareText(a.part_number, b.part_number)
  ));
  return items;
}

function buildPostDispatchShortagesFromCompletedDrafts() {
  const completedRows = Array.isArray(_completedRows) ? _completedRows : [];
  if (!completedRows.length) return [];

  const merged = new Map();
  for (const row of completedRows) {
    const orderId = normalizeOrderId(row?.id);
    const draft = Number.isInteger(orderId)
      ? _completedDraftsByOrderId?.[orderId]
      : null;
    const shortages = Array.isArray(draft?.shortages) ? draft.shortages : [];
    if (!shortages.length) continue;

    for (const shortage of shortages) {
      const partKey = normalizePartKey(shortage?.part_number);
      if (!partKey || merged.has(partKey)) continue;

      const fallbackShortageAmount = Math.max(0, Number(shortage?.shortage_amount || 0) || 0);
      const liveStock = Number(_liveStock?.[partKey]);
      const useLiveStock = Number.isFinite(liveStock) && !isOrderScopedPart(partKey);
      const currentStock = useLiveStock
        ? liveStock
        : Number(shortage?.resulting_stock ?? shortage?.current_stock ?? 0);
      const shortageAmount = useLiveStock
        ? calculateDisplayShortageAmount(partKey, currentStock)
        : fallbackShortageAmount;
      if (!shouldRenderRightPanelActionableShortage(partKey, currentStock, shortageAmount)) continue;

      const moq = Math.max(0, Number(_moq?.[partKey] || shortage?.moq || 0) || 0);
      const stStockQty = Math.max(0, Number(_stStock?.[partKey] ?? shortage?.st_stock_qty ?? 0) || 0);
      const suggestion = buildShortageSupplySuggestion(partKey, shortageAmount, moq, stStockQty);
      const meta = buildShortageGroupMeta(row, shortage);

      merged.set(partKey, {
        ...shortage,
        ...meta,
        model: meta._row_model || shortage?.model || draft?.model || row?.model || "未指定機種",
        current_stock: currentStock,
        resulting_stock: currentStock,
        shortage_amount: shortageAmount,
        moq,
        st_stock_qty: stStockQty,
        suggested_qty: suggestion.suggested_qty,
        st_available_qty: suggestion.st_available_qty,
        purchase_needed_qty: suggestion.purchase_needed_qty,
        purchase_suggested_qty: suggestion.purchase_suggested_qty,
        needs_purchase: suggestion.needs_purchase,
      });
    }
  }

  return [...merged.values()];
}

function findPostDispatchShortageOrderRow(shortage = {}) {
  const orderId = normalizeOrderId(shortage?._order_id ?? shortage?.order_id);
  if (!Number.isInteger(orderId)) return {};

  const completedRows = Array.isArray(_completedRows) ? _completedRows : [];
  const activeRows = Array.isArray(_rows) ? _rows : [];
  return completedRows.find(row => normalizeOrderId(row?.id) === orderId)
    || activeRows.find(row => normalizeOrderId(row?.id) === orderId)
    || {};
}

function normalizePostDispatchShortageForPanel(shortage = {}, row = {}) {
  const partKey = normalizePartKey(shortage?.part_number);
  if (!partKey) return null;

  const fallbackShortageAmount = Math.max(0, Number(shortage?.shortage_amount || 0) || 0);
  const liveStock = Number(_liveStock?.[partKey]);
  const useLiveStock = Number.isFinite(liveStock) && !isOrderScopedPart(partKey);
  const fallbackStock = Number(shortage?.resulting_stock ?? shortage?.current_stock);
  const currentStock = useLiveStock
    ? liveStock
    : (Number.isFinite(fallbackStock) ? fallbackStock : -fallbackShortageAmount);
  const shortageAmount = useLiveStock
    ? calculateDisplayShortageAmount(partKey, currentStock)
    : fallbackShortageAmount;
  if (!shouldRenderRightPanelActionableShortage(partKey, currentStock, shortageAmount)) return null;

  const moq = Math.max(0, Number(_moq?.[partKey] || shortage?.moq || 0) || 0);
  const stStockQty = Math.max(0, Number(_stStock?.[partKey] ?? shortage?.st_stock_qty ?? 0) || 0);
  const suggestedQty = Math.max(0, Number(shortage?.suggested_qty || 0) || 0);
  const stAvailableQty = Math.max(0, Number(shortage?.st_available_qty || 0) || 0);
  const purchaseNeededQty = Math.max(0, Number(shortage?.purchase_needed_qty || 0) || 0);
  const purchaseSuggestedQty = Math.max(0, Number(shortage?.purchase_suggested_qty || 0) || 0);
  const suggestion = buildShortageSupplySuggestion(partKey, shortageAmount, moq, stStockQty);
  const meta = buildShortageGroupMeta(row, shortage);

  return {
    ...shortage,
    ...meta,
    part_number: partKey,
    model: meta._row_model || shortage?.model || shortage?.bom_model || row?.model || "未指定機種",
    current_stock: currentStock,
    resulting_stock: currentStock,
    shortage_amount: shortageAmount,
    moq,
    st_stock_qty: stStockQty,
    suggested_qty: suggestedQty || suggestion.suggested_qty,
    st_available_qty: stAvailableQty || suggestion.st_available_qty,
    purchase_needed_qty: purchaseNeededQty || suggestion.purchase_needed_qty,
    purchase_suggested_qty: purchaseSuggestedQty || suggestion.purchase_suggested_qty,
    needs_purchase: Boolean(shortage?.needs_purchase || suggestion.needs_purchase),
  };
}

function buildPostDispatchShortagesFromResponse(shortages = []) {
  if (!Array.isArray(shortages) || !shortages.length) return [];
  return shortages
    .map(shortage => normalizePostDispatchShortageForPanel(
      shortage,
      findPostDispatchShortageOrderRow(shortage),
    ))
    .filter(Boolean);
}

function getPostDispatchShortageKey(shortage = {}) {
  const partKey = normalizePartKey(shortage?.part_number);
  if (!partKey) return "";
  const orderId = normalizeOrderId(shortage?._order_id ?? shortage?.order_id);
  if (isOrderScopedPart(partKey) && Number.isInteger(orderId)) {
    return buildOrderPartKey(orderId, partKey);
  }
  return partKey;
}

function mergePostDispatchShortageLists(...lists) {
  const merged = new Map();
  for (const list of lists) {
    for (const shortage of (Array.isArray(list) ? list : [])) {
      const key = getPostDispatchShortageKey(shortage);
      if (!key || merged.has(key)) continue;
      merged.set(key, shortage);
    }
  }
  return [...merged.values()];
}

function syncPostDispatchShortagesFromCompletedDrafts() {
  _postDispatchShortages = mergePostDispatchShortageLists(
    buildPostDispatchShortagesFromCompletedDrafts(),
  );
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
    await refreshScheduleOnly();
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
    await refreshScheduleOnly();
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
  await refreshScheduleOnly();
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

function syncDraftPartControls(list, part, { qty = null, shortageChecked = null, orderId = null } = {}) {
  const partKey = normalizePartKey(part);
  if (!partKey || !list) return;

  list.querySelectorAll(".supplement-input").forEach(input => {
    if (normalizePartKey(input.dataset.part) !== partKey) return;
    if (orderId !== null && normalizeOrderId(input.dataset.orderId) !== orderId) return;
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
    if (orderId !== null && normalizeOrderId(checkbox.dataset.orderId) !== orderId) return;
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
  const rawResultingStock = Number(row.resulting_stock);
  const resultingStock = Number.isFinite(rawResultingStock)
    ? roundShortageUiValue(rawResultingStock)
    : Number.NaN;
  const negativeRunningWarning = hasNegativeRunningStock(row)
    ? '<div style="font-size:12px;color:#dc2626;font-weight:700;margin-top:4px">⚠ 結存為負，請確認補料或手動勾缺料</div>'
    : "";
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
        ${Number.isFinite(resultingStock) ? draftInlineStatHtml("結存", resultingStock, resultingStock < 0 ? "is-carry" : "") : ""}
      </div>
      ${negativeRunningWarning}
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
    const sampleHidden = section.dataset.sampleHidden === "1";
    let sectionVisibleRows = 0;
    totalRows += rowNodes.length;

    rowNodes.forEach(row => {
      const flowHidden = row.dataset.flowHidden === "1";
      const visible = !flowHidden && (!query || sectionMatches || matchesModalSearchQuery(row, query));
      row.style.display = visible ? "" : "none";
      if (visible) {
        visibleRows += 1;
        sectionVisibleRows += 1;
      }
    });

    section.style.display = !sampleHidden && (!query || sectionMatches || sectionVisibleRows > 0) ? "" : "none";
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

function activateCalcWorkspace() {
  const tabButton = document.querySelector('.tab-btn[data-tab="calc-workspace"]');
  if (tabButton) {
    tabButton.click();
    return;
  }
  document.querySelectorAll(".tab-btn").forEach(btn => btn.classList.remove("active"));
  document.querySelectorAll(".tab-content").forEach(content => content.classList.remove("active"));
  document.getElementById("tab-calc-workspace")?.classList.add("active");
}

function isCalcWorkspaceActive() {
  return document.getElementById("tab-calc-workspace")?.classList.contains("active");
}

function setCalcWorkspaceTitle(title, subtitle = "") {
  const titleEl = document.getElementById("modal-title");
  const subtitleEl = document.getElementById("modal-subtitle");
  if (titleEl) titleEl.textContent = title || "算料工作區";
  if (subtitleEl) subtitleEl.textContent = subtitle || "從出貨排程按「批次 Merge」或「生成發料單」開始";
}

function ensureCalcWorkspaceReady(title, subtitle = "") {
  activateCalcWorkspace();
  setCalcWorkspaceTitle(title, subtitle);
  const closeBtn = document.getElementById("modal-close");
  const clearBtn = document.getElementById("modal-clear-workspace");
  if (closeBtn) closeBtn.onclick = closeShortageModal;
  if (clearBtn) clearBtn.onclick = clearCalcWorkspace;
  return {
    list: document.getElementById("modal-shortage-list"),
    footer: document.getElementById("modal-footer"),
  };
}

async function showDraftModal(draftId, { readOnly = false, fileId = null } = {}) {
  const { list, footer } = ensureCalcWorkspaceReady(
    readOnly ? "副檔預覽" : "副檔補料工作區",
    readOnly ? "檢視已存副檔內容，可下載副檔。" : "編輯副檔補料內容，可儲存或下載副檔。",
  );
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
}

async function showShortageModal(targets) {
  _modalDraftId = null;
  _modalTargets = targets;
  _modalMode = "download";
  _modalPreviewShortages = [];
  _modalResetStored = false;
  const { list, footer } = ensureCalcWorkspaceReady(
    "確認補料並下載 BOM",
    `共 ${targets.length} 筆訂單，補料內容會寫入下載的 BOM 副本。`,
  );

  // 查詢對應的 BOM 檔案
  const models = [...new Set(targets.map(t => t.model).filter(Boolean))];
  _modalBomFiles = [];
  if (models.length) {
    try {
      const lookup = await apiPost("/api/bom/lookup", { models });
      _modalBomFiles = lookup.files || [];
    } catch (_) {}
  }

  _modalCarryOversByModel = {};
  await refreshModalCalcPreview({ immediate: true, resetStored: false });
  configureModalSearch({ placeholder: "搜尋料號 / 說明 / 機種" });

  // 重設 footer
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
}

async function saveBatchDraftsFromModal({ silent = false } = {}) {
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  if (!targetOrderIds.length) return null;

  const supplements = _collectModalSupplements();
  const decisions = _collectModalDecisions();
  const orderSupplements = _collectModalOrderSupplements();
  const orderDecisions = _collectModalOrderDecisions();
  const sampleOrderIds = collectModalSampleOrderIds();
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
    sample_order_ids: sampleOrderIds,
  });
  await refreshScheduleOnly();
  if (!silent) showToast("補料已寫入副檔");
  return response?.drafts || null;
}

async function updateAndCommitBatchDraftsFromModal() {
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  if (!targetOrderIds.length) return null;

  const supplements = _collectModalSupplements();
  const decisions = _collectModalDecisions();
  const orderSupplements = _collectModalOrderSupplements();
  const orderDecisions = _collectModalOrderDecisions();
  const sampleOrderIds = collectModalSampleOrderIds();
  await persistDecisionsForOrders(decisions, targetOrderIds, orderDecisions);
  Object.entries(decisions).forEach(([part, decision]) => {
    setLocalDecision(part, decision);
  });

  return apiPost("/api/schedule/update-and-commit-drafts", {
    order_ids: targetOrderIds,
    decisions,
    supplements,
    order_decisions: orderDecisions,
    order_supplements: orderSupplements,
    sample_order_ids: sampleOrderIds,
  });
}

async function handleModalSaveDrafts() {
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  if (!targetOrderIds.length) {
    showToast("找不到要儲存補料的訂單");
    return;
  }

  if (_modalCommitAfterSave) {
    await handleModalUpdateAndCommitDrafts();
    return;
  }

  if (!confirmModalShortageSupplementConflicts()) return;

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

async function handleModalUpdateAndCommitDrafts() {
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  if (!targetOrderIds.length) {
    showToast("找不到要寫入主檔的訂單");
    return;
  }
  if (!confirm(`確認要強制寫入主檔 ${targetOrderIds.length} 筆訂單嗎？缺料不會擋停，主檔可能出現負庫存。`)) {
    return;
  }
  if (!confirmModalShortageSupplementConflicts()) return;

  try {
    setModalDownloadProgress(true, "正在保存補料並寫入主檔...", `共 ${targetOrderIds.length} 筆訂單，系統會先重建副檔再寫入 live 主檔；可先切到其他分頁。`, 12);
    startModalProgressAnimation(92, 260);
    const result = await updateAndCommitBatchDraftsFromModal();
    targetOrderIds.forEach(id => _checkedIds.delete(id));
    const negativeCount = (result?.negative_shortages || []).length;
    const failureCount = Number(result?.failure_count || 0);
    if (failureCount > 0) {
      setModalDownloadProgress(
        true,
        "寫入主檔有失敗項目",
        formatWorkspaceFailureDetail(result),
        100,
        { tone: "error", lockUi: false },
      );
      showToast(`強制寫入完成但有 ${failureCount} 筆失敗，請回算料工作區查看明細`, { sticky: true, tone: "error" });
      await Promise.all([refresh(), refreshCompleted()]);
      return;
    }

    setModalDownloadProgress(true, "寫入主檔完成，正在重新整理...", "正在重新整理排程與主檔資料。", 100);
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
    await new Promise(resolve => setTimeout(resolve, 200));

    const message = `已強制寫入 ${result?.success_count || result?.count || 0} 筆，失敗 ${failureCount} 筆，負庫存 ${negativeCount} 項`;
    showToast(message, { tone: failureCount ? "error" : "success", sticky: failureCount > 0, duration: 6000 });
    if (negativeCount > 0) {
      showPostDispatchShortages(result.negative_shortages);
    } else if (result?.shortages?.length) {
      showPostDispatchShortages(result.shortages);
    }
    if (isCalcWorkspaceActive()) closeShortageModal();
  } catch (error) {
    showToast("強制寫入主檔失敗: " + error.message, { sticky: true, tone: "error" });
    setModalDownloadProgress(
      true,
      "強制寫入主檔失敗",
      formatWorkspaceFailureDetail(error),
      100,
      { tone: "error", lockUi: false },
    );
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
  const commitAfterSave = Boolean(_modalCommitAfterSave);
  _modalTargets = targets;
  _modalBomFiles = [];
  _modalCarryOversByModel = {};
  _modalMode = commitAfterSave ? "mergeCommit" : "download";
  _modalPreviewShortages = [];
  _modalDraftId = null;
  _modalDraftReadOnly = false;
  _modalDraftBaseDecisions = {};
  _modalDraftBaseSupplements = {};
  _modalDraftVisibleParts = [];
  _modalResetStored = Boolean(_modalResetStored);
  const modeText = formatBatchMergeMode({
    resetStored: _modalResetStored,
    commitAfterModal: commitAfterSave,
  });

  const { list, footer } = ensureCalcWorkspaceReady(
    _modalCommitAfterSave ? "批次 Merge 並寫入主檔" : "批次 Merge 補料確認",
    `共 ${targets.length} 筆訂單，可先確認補料、儲存副檔或下載副檔。`,
  );
  if (!list || !footer) {
    console.error("[showBatchMergeDraftModal] DOM elements missing:", { list: !!list, footer: !!footer });
    throw new Error("算料工作區 DOM 元素遺失");
  }
  await refreshModalCalcPreview({ immediate: true, resetStored: _modalResetStored });
  configureModalSearch({ placeholder: "搜尋料號 / 說明 / 機種" });

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
    <div class="batch-merge-mode">${modeText}</div>
    <button id="modal-save-draft" class="btn btn-success btn-sm">${commitAfterSave ? "確認補料並寫主檔" : "確認補料"}</button>
    <button id="modal-download-bom" class="btn btn-primary btn-sm">確認補料並下載副檔</button>
    <button id="modal-cancel" class="btn btn-secondary btn-sm">取消</button>`;
  document.getElementById("modal-save-draft").onclick = handleModalSaveDrafts;
  document.getElementById("modal-download-bom").onclick = handleModalDownloadDrafts;
  document.getElementById("modal-cancel").onclick = closeShortageModal;
}

async function showWriteToMainModal(targets) {
  _modalDraftId = null;
  _modalTargets = targets;
  _modalMode = "write";
  _modalResetStored = false;

  const { list, footer } = ensureCalcWorkspaceReady(
    "寫入主檔前確認",
    `共 ${targets.length} 筆訂單，寫入期間可自由切換分頁。`,
  );
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

  await refreshModalCalcPreview({ immediate: true, resetStored: false });
  configureModalSearch({ placeholder: "搜尋料號 / 說明 / 機種" });

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
}

function buildModalCalcPreviewPayload({ resetStored = _modalResetStored } = {}) {
  const targetOrderIds = (_modalTargets || [])
    .map(target => normalizeOrderId(target?.id))
    .filter(Number.isInteger);
  return {
    order_ids: targetOrderIds,
    decisions: _modalMode === "write" ? { ..._decisions, ..._collectModalDecisions() } : _collectModalDecisions(),
    supplements: _collectModalSupplements(),
    order_decisions: _collectModalOrderDecisions(),
    order_supplements: _collectModalOrderSupplements(),
    sample_order_ids: collectModalSampleOrderIds(),
    reset_stored: Boolean(resetStored),
  };
}

function captureFocusedModalField() {
  const active = document.activeElement;
  if (!active || !active.closest?.("#modal-shortage-list")) return null;
  if (!active.matches("input, textarea, select")) return null;
  const meta = _getModalRowMeta(active);
  return {
    selector: active.classList.contains("sample-order-flag")
      ? `.sample-order-flag[data-order-id="${esc(active.dataset.orderId || "")}"]`
      : active.classList.contains("shortage-mark")
        ? `.shortage-mark[data-part="${esc(active.dataset.part || "")}"][data-order-id="${esc(active.dataset.orderId || "")}"]`
        : active.classList.contains("supplement-input")
          ? `.supplement-input[data-part="${esc(active.dataset.part || "")}"][data-order-id="${esc(active.dataset.orderId || "")}"]`
          : "",
    className: active.className,
    part: meta.part,
    orderId: meta.orderId,
    value: active.value,
    checked: Boolean(active.checked),
    selectionStart: active.selectionStart,
    selectionEnd: active.selectionEnd,
  };
}

function restoreFocusedModalField(focusState) {
  if (!focusState) return;
  const list = document.getElementById("modal-shortage-list");
  if (!list) return;
  let target = focusState.selector ? list.querySelector(focusState.selector) : null;
  if (!target && focusState.part) {
    const orderSelector = Number.isInteger(focusState.orderId) ? `[data-order-id="${focusState.orderId}"]` : "";
    const cls = String(focusState.className || "").includes("shortage-mark") ? "shortage-mark" : "supplement-input";
    target = list.querySelector(`.${cls}[data-part="${CSS.escape(focusState.part)}"]${orderSelector}`);
  }
  if (!target) return;
  if ("checked" in target) target.checked = focusState.checked;
  if ("value" in target) target.value = focusState.value;
  target.focus({ preventScroll: true });
  if (Number.isInteger(focusState.selectionStart) && target.setSelectionRange) {
    target.setSelectionRange(focusState.selectionStart, focusState.selectionEnd ?? focusState.selectionStart);
  }
}

async function fetchModalCalcPreview({ resetStored = _modalResetStored, signal = null } = {}) {
  const payload = buildModalCalcPreviewPayload({ resetStored });
  if (!payload.order_ids.length) return null;
  return apiJson("/api/schedule/calc-preview", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal,
  });
}

async function refreshModalCalcPreview({ immediate = false, resetStored = _modalResetStored } = {}) {
  const run = async () => {
    const seq = ++_modalPreviewRequestSeq;
    if (_modalPreviewAbortController) _modalPreviewAbortController.abort();
    _modalPreviewAbortController = new AbortController();
    const focusState = captureFocusedModalField();
    try {
      const preview = await fetchModalCalcPreview({
        resetStored,
        signal: _modalPreviewAbortController.signal,
      });
      if (!preview || seq !== _modalPreviewRequestSeq || _modalPreviewDebounceTimer) return;
      renderModalCalcPreview(preview, { focusState });
    } catch (error) {
      if (error?.name === "AbortError") return;
      showToast("缺料預覽更新失敗: " + error.message, { tone: "error" });
    }
  };

  if (immediate) {
    if (_modalPreviewDebounceTimer) clearTimeout(_modalPreviewDebounceTimer);
    _modalPreviewDebounceTimer = null;
    await run();
    return;
  }
  if (_modalPreviewDebounceTimer) clearTimeout(_modalPreviewDebounceTimer);
  _modalPreviewDebounceTimer = setTimeout(() => {
    _modalPreviewDebounceTimer = null;
    void run();
  }, 250);
}

function normalizePreviewScope(scope = {}) {
  const orderId = normalizeOrderId(scope.order_id);
  const target = (_modalTargets || []).find(item => normalizeOrderId(item?.id) === orderId) || {};
  const meta = buildShortageGroupMeta(target, {
    order_id: orderId,
    model: scope.model || target.model,
    batch_code: scope.batch_code || target.code,
    po_number: scope.po_number || target.po_number,
  });
  return {
    key: meta._row_group_key,
    label: meta._row_group_label,
    po_number: meta._po_number,
    order_id: orderId,
    batch_code: scope.batch_code || target.code || "",
    is_sample: Boolean(scope.is_sample),
  };
}

function normalizePreviewShortageItem(item = {}, scope = {}) {
  const meta = buildShortageGroupMeta(
    (_modalTargets || []).find(target => normalizeOrderId(target?.id) === normalizeOrderId(scope.order_id)) || {},
    {
      ...item,
      order_id: item.order_id ?? scope.order_id,
      model: item.model || scope.model,
      batch_code: item.batch_code || scope.batch_code,
      po_number: item.po_number || scope.po_number,
    },
  );
  return {
    ...item,
    ...meta,
    _lookahead_shortage_amount: item.lookahead_shortage_amount ?? item._lookahead_shortage_amount ?? item.shortage_amount,
    _lookahead_suggested_qty: item.lookahead_suggested_qty ?? item._lookahead_suggested_qty ?? item.suggested_qty,
    _lookahead_st_available_qty: item.lookahead_st_available_qty ?? item.st_available_qty,
    _lookahead_purchase_needed_qty: item.lookahead_purchase_needed_qty ?? item.purchase_needed_qty,
    _lookahead_purchase_suggested_qty: item.lookahead_purchase_suggested_qty ?? item.purchase_suggested_qty,
    _lookahead_needs_purchase: item.lookahead_needs_purchase ?? item.needs_purchase,
  };
}

function renderSharedPreviewParts(sharedParts = []) {
  const items = (sharedParts || [])
    .map(item => ({
      ...item,
      _row_code: (item.batch_codes || []).join("、"),
      _row_model: "共用料",
      _row_group_label: "共用料",
      _row_group_key: "shared",
      _order_id: undefined,
      _lookahead_shortage_amount: item.lookahead_shortage_amount ?? item.shortage_amount,
      _lookahead_suggested_qty: item.lookahead_suggested_qty ?? item.suggested_qty,
      _lookahead_st_available_qty: item.lookahead_st_available_qty ?? item.st_available_qty,
      _lookahead_purchase_needed_qty: item.lookahead_purchase_needed_qty ?? item.purchase_needed_qty,
      _lookahead_purchase_suggested_qty: item.lookahead_purchase_suggested_qty ?? item.purchase_suggested_qty,
    }))
    .filter(item => Number(item.shortage_amount || 0) > 0 || Number(item.supplement_qty || item.default_supplement || 0) > 0 || item.decision === "Shortage")
    .sort(compareShortageItems);

  if (!items.length) return "";
  return `<section class="modal-shortage-section" data-fixed-scope="1" data-search="共用料 ${esc(items.map(item => item.part_number).join(" "))}">
    ${shortageGroupHeadingHtml("共用料（整批只補一次）", "")}
    <h4 style="font-size:12px;color:#dc2626;margin:4px 0">採購缺料</h4>
    ${items.map(item => modalShortageItem(item, Boolean(item.is_customer_material))).join("")}
  </section>`;
}

function renderModalCalcPreview(preview, { focusState = null } = {}) {
  const list = document.getElementById("modal-shortage-list");
  if (!list) return;

  _modalPreviewShortages = preview?.shortages || [];
  const scopes = Array.isArray(preview?.scopes) ? preview.scopes : [];
  const sharedPartKeys = new Set((preview?.shared_parts || []).map(item => normalizePartKey(item?.part_number)).filter(Boolean));
  let html = "";

  if (_modalMode === "write" && Number(preview?.blocking_count || 0) > 0) {
    html += `<div style="padding:10px 14px;margin-bottom:8px;background:#fef2f2;border:1px solid #fecaca;border-radius:8px;color:#dc2626;font-weight:600;font-size:13px">
      寫入後將有 ${Number(preview.blocking_count || 0)} 筆料號缺料，需補料或保留缺料</div>`;
  }

  html += renderSharedPreviewParts(preview?.shared_parts || []);

  if (!scopes.length && !html) {
    html = `<div style="text-align:center;padding:24px;color:#16a34a;font-weight:600">目前沒有可顯示的缺料。</div>`;
  } else {
    for (const rawScope of scopes) {
      const scope = normalizePreviewScope(rawScope);
      const items = (rawScope.shortages || [])
        .map(item => normalizePreviewShortageItem(item, rawScope))
        .filter(item => !sharedPartKeys.has(normalizePartKey(item.part_number)))
        .sort(compareShortageItems);
      const csItems = items.filter(item => Boolean(item.is_customer_material));
      const purchaseItems = items.filter(item => !item.is_customer_material);
      html += `<section class="modal-shortage-section" data-fixed-scope="1" data-search="${esc([scope.label, scope.po_number || ""].join(" "))}">`;
      html += shortageGroupHeadingHtml(scope.label, scope.po_number, {
        sampleControlHtml: sampleOrderCheckboxHtml(scope, scope.is_sample),
      });
      if (csItems.length) {
        html += '<div style="margin-bottom:8px"><h4 style="font-size:12px;color:#ca8a04;margin:4px 0">客供料</h4>';
        html += csItems.map(item => modalShortageItem(item, true)).join("");
        html += "</div>";
      }
      if (purchaseItems.length) {
        html += `<h4 style="font-size:12px;color:#dc2626;margin:4px 0">${_modalMode === "write" ? "寫入主檔後仍缺料" : "訂單專屬缺料"}</h4>`;
        html += purchaseItems.map(item => modalShortageItem(item, false)).join("");
      }
      if (!csItems.length && !purchaseItems.length) {
        html += '<div style="font-size:12px;color:#16a34a;font-weight:600;padding:6px 2px">此單無訂單專屬缺料</div>';
      }
      html += "</section>";
    }
  }

  if (!preview?.shared_parts?.length && scopes.every(scope => !(scope.shortages || []).length)) {
    html = `<div style="text-align:center;padding:24px;color:#16a34a;font-weight:600">
      ${_modalMode === "write" ? "模擬寫入主檔後沒有剩餘缺料，可以直接寫入主檔。" : "全部 OK，無缺料！"}</div>`;
  }

  list.innerHTML = html;
  bindShortageEditors(list);
  bindSampleOrderFlags(list);
  bindMoqEditors(list);
  bindShortageMoqBadgeEditors(list);
  restoreFocusedModalField(focusState);
  const searchInput = document.getElementById("modal-search-input");
  applyModalSearchFilter(searchInput?.value || "");
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

function hasRemainingShortageForResultingStock(partNumber, resultingStock) {
  return Number.isFinite(resultingStock) && resultingStock < 0;
}

function isShortageStillNegative(shortage) {
  const resultingStock = computeShortageResultingStock(shortage);
  return hasRemainingShortageForResultingStock(shortage?.part_number, resultingStock);
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
  return false;
}

function hasNegativeRunningStock(item) {
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
          : Number(s._lookahead_suggested_qty) > 0
            ? s._lookahead_suggested_qty
          : Number(s.suggested_qty) > 0
            ? s.suggested_qty
            : s.shortage_amount ?? 0
    );
  const shortageAmount = roundShortageUiValue(s.shortage_amount);
  const currentStock = roundShortageUiValue(s.current_stock);
  const neededQty = roundShortageUiValue(s.needed);
  const stAvailableQty = roundShortageUiValue(s.st_available_qty || 0);
  const purchaseNeededQty = roundShortageUiValue(s.purchase_needed_qty || 0);
  const negativeRunningWarning = hasNegativeRunningStock(s)
    ? '<div style="font-size:12px;color:#dc2626;font-weight:700;margin:2px 0">⚠ 結存為負，請確認補料或手動勾缺料</div>'
    : "";
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
  const amountsHtml = buildModalShortageAmountsHtml(s);

  return `<div class="${shortageToneClass(s)}" style="margin-bottom:8px" data-part="${esc(s.part_number)}" data-base-current-stock="${esc(s.current_stock)}" data-current-stock="${esc(s.current_stock)}" data-prev-qty-cs="${esc(s.prev_qty_cs || 0)}" data-needed="${esc(s.needed)}" data-moq="${esc(s.moq || 0)}" data-st-stock-qty="${esc(s.st_stock_qty || 0)}" data-order-index="${esc(s._row_order_index ?? "")}" data-flow-hidden="0" data-search="${esc(searchText)}"${orderIdAttr}>
    <div style="display:flex;align-items:center;gap:6px;font-weight:600;font-size:13px">${s.part_number}${codeTag}${csTag}</div>
    <div style="font-size:11px;color:#6b7280">${s.description || "—"}</div>
    <div class="modal-shortage-amounts" style="font-size:12px;display:flex;gap:10px;margin:4px 0">
      ${amountsHtml}
    </div>
    ${negativeRunningWarning}
    ${missingMoqEditorHtml(s)}
    ${isCS ? '<div style="font-size:11px;color:#ca8a04">請通知客戶提供此料</div>' : `
    <div style="display:flex;align-items:center;gap:8px;margin-top:4px">
      <label style="font-size:12px;color:#374151;white-space:nowrap">補料:</label>
      <input type="number" class="supplement-input" data-part="${s.part_number}"${orderIdAttr} value="${defaultQty}" min="0" ${shortageChecked ? "disabled" : ""}
             style="width:80px;padding:2px 6px;border:1px solid #d1d5db;border-radius:4px;font-size:12px;text-align:right">
      <label style="font-size:12px;display:flex;align-items:center;gap:4px;color:#dc2626;cursor:pointer;white-space:nowrap">
        <input type="checkbox" class="shortage-mark" data-part="${s.part_number}" data-manual="0"${orderIdAttr} ${shortageChecked ? "checked" : ""}> 缺料
      </label>
    </div>`}
  </div>`;
}

function buildModalShortageAmountsHtml(shortage) {
  const ownShortage = roundShortageUiValue(shortage.shortage_amount || 0);
  const lookaheadShortage = roundShortageUiValue(shortage._lookahead_shortage_amount || 0);
  const lookaheadSuggested = roundShortageUiValue(shortage._lookahead_suggested_qty || 0);
  const showLookahead = lookaheadShortage > ownShortage;
  return `
      <span class="modal-shortage-amount modal-shortage-amount-shortage" style="color:#dc2626">缺 ${fmt(ownShortage)}</span>
      <span class="modal-shortage-amount modal-shortage-amount-stock" style="color:#16a34a">庫存 ${fmt(roundShortageUiValue(shortage.current_stock || 0))}</span>
      <span class="modal-shortage-amount modal-shortage-amount-needed">需 ${fmt(roundShortageUiValue(shortage.needed || 0))}</span>
      ${stSupplySummaryHtml(shortage)}
      ${showLookahead ? `<span class="modal-shortage-amount modal-shortage-amount-lookahead" style="color:#7c3aed">總共缺 ${fmt(lookaheadShortage)}（建議補 ${fmt(lookaheadSuggested)}）</span>` : ""}
      ${moqBadgeHtml(shortage)}
      ${moqEditTriggerHtml(shortage)}
  `;
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
  const isNegative = hasRemainingShortageForResultingStock(card?.dataset.part, resultingStock);
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
  document.querySelector('.tab-btn[data-tab="schedule"]')?.click();
}

function clearCalcWorkspace() {
  stopModalProgressAnimation();
  setModalDownloadProgress(false, "", "", 0);
  const modal = document.getElementById("shortage-modal");
  const list = document.getElementById("modal-shortage-list");
  const footer = document.getElementById("modal-footer");
  if (modal) modal.style.display = "none";
  if (list) list.innerHTML = '<div class="calc-workspace-empty">從出貨排程按「批次 Merge」或「生成發料單」開始</div>';
  if (footer) footer.innerHTML = "";
  configureModalSearch({ enabled: false });
  setCalcWorkspaceTitle("算料工作區", "從出貨排程按「批次 Merge」或「生成發料單」開始");
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
  _modalCommitAfterSave = false;
  _modalResetStored = false;
  if (_modalPreviewAbortController) _modalPreviewAbortController.abort();
  if (_modalPreviewDebounceTimer) clearTimeout(_modalPreviewDebounceTimer);
  _modalPreviewAbortController = null;
  _modalPreviewDebounceTimer = null;
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
  return `<button type="button" class="moq-edit-trigger" data-part="${partNumber}" data-moq="${rawMoq}" title="儲存 MOQ" aria-label="儲存 MOQ">存</button>`;
}

function suggestedQtyHtml(shortage) {
  shortage = { ...shortage, moq: roundShortageUiValue(shortage.moq) };
  const suggested = roundShortageUiValue(
    shortage._lookahead_suggested_qty || shortage.suggested_qty || shortage.shortage_amount || 0
  );
  const purchaseNeeded = roundShortageUiValue(
    shortage._lookahead_purchase_suggested_qty || shortage.purchase_suggested_qty || shortage.purchase_needed_qty || 0
  );
  if (purchaseNeeded > 0) {
    if (hasMoqValue(shortage)) {
      return `<span class="amber">建議補 ${fmt(suggested)}（其中這串需要買 ${fmt(purchaseNeeded)}，MOQ ${fmt(shortage.moq)}）</span>`;
    }
    return `<span class="amber">建議補 ${fmt(suggested)}（其中這串需要買 ${fmt(purchaseNeeded)}，未寫 MOQ）</span>`;
  }
  if (hasMoqValue(shortage)) {
    return `<span class="blue">建議補 ${fmt(suggested)}（MOQ ${fmt(shortage.moq)}）</span>`;
  }
  return `<span class="amber">建議補 ${fmt(suggested)}（未寫 MOQ）</span>`;
}

function stSupplySummaryHtml(shortage) {
  const stAvailable = roundShortageUiValue(
    shortage?._lookahead_st_available_qty || shortage?.st_available_qty || 0
  );
  const purchaseNeeded = roundShortageUiValue(
    shortage?._lookahead_purchase_suggested_qty || shortage?.purchase_suggested_qty || shortage?.purchase_needed_qty || 0
  );
  const summary = [];
  if (stAvailable > 0) {
    summary.push(`<span class="blue">ST 可補 ${fmt(stAvailable)}</span>`);
  }
  if (purchaseNeeded > 0) {
    summary.push(`<span class="amber">這串需要買 ${fmt(purchaseNeeded)}</span>`);
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
    if (_modalTargets.length && document.getElementById("modal-shortage-list")) {
      const inFlightSupplements = _collectModalSupplements();
      _modalDraftBaseSupplements = { ..._modalDraftBaseSupplements, ...inFlightSupplements };
      // Bug 2 root fix: 把當前 modal 內容 silent save 到 server，避免 re-render 時被 server draft 預設值蓋掉
      try { await saveBatchDraftsFromModal({ silent: true }); } catch (_) {}
      if (_modalMode === "write") {
        await showWriteToMainModal(_modalTargets);
      } else if (_modalDraftId) {
        await showDraftModal(_modalDraftId, { readOnly: _modalDraftReadOnly });
      } else if (_modalMode === "mergeCommit" || _modalCommitAfterSave) {
        await showBatchMergeDraftModal(_modalTargets);
      } else {
        await showShortageModal(_modalTargets);
      }
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
    const result = await withGlobalBusy(
      () => desktopDownload({ path: `/api/schedule/drafts/${draftId}/download${query}` }),
      {
        title: "正在產生副檔",
        detail: "系統會讀取主檔與 BOM 重新整理 Excel，檔案較大時請稍候。",
        phase: "讀取主檔",
        timeout: 600000,
      },
    );
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
    await refreshScheduleOnly();
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
    await refreshScheduleOnly();
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
      // 編輯中：blur input 觸發 save；不在編輯：進入編輯模式
      const input = badge.querySelector(".moq-inline-input");
      if (input) {
        input.blur();
      } else {
        void handleShortageBadgeMoqEdit(badge);
      }
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
    if (input.closest(".shortage-item")?.dataset.flowHidden === "1") return;
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
    if (input.closest(".shortage-item")?.dataset.flowHidden === "1") return;
    const { orderId, part } = _getModalRowMeta(input);
    if (!part) return;
    if (isOrderScopedPart(part) && Number.isInteger(orderId)) return;

    const qty = parseFloat(input.value) || 0;
    const isShortage = input.closest(".draft-preview-row, .shortage-item")?.querySelector(".shortage-mark")?.checked
      || Array.from(list.querySelectorAll(".shortage-mark")).some(checkbox => normalizePartKey(checkbox.dataset.part) === part && checkbox.checked);
    if (!isShortage && qty > 0) supplements[part] = (supplements[part] || 0) + qty;
  });
  return supplements;
}

const MODAL_SHORTAGE_SUPPLEMENT_WARNING_PREFIX = "以下料號有補料數量，但同料號某列已勾缺料，這些補料將不會送出：";

function findModalShortageSupplementConflicts() {
  const list = document.getElementById("modal-shortage-list");
  if (!list) return [];

  const checkedParts = new Set();
  list.querySelectorAll(".shortage-mark").forEach(checkbox => {
    if (!checkbox.checked) return;
    const part = normalizePartKey(checkbox.dataset.part || checkbox.closest(".shortage-item, .draft-preview-row")?.querySelector(".supplement-input")?.dataset.part);
    if (part && !isOrderScopedPart(part)) checkedParts.add(part);
  });

  const conflicts = new Map();
  list.querySelectorAll(".supplement-input").forEach(input => {
    if (input.closest(".shortage-item")?.dataset.flowHidden === "1") return;
    const part = normalizePartKey(input.dataset.part || input.closest(".shortage-item, .draft-preview-row")?.dataset.part);
    const qty = parseFloat(input.value) || 0;
    if (!part || isOrderScopedPart(part) || qty <= 0 || !checkedParts.has(part)) return;
    const current = conflicts.get(part) || { part, rows: 0, totalQty: 0 };
    current.rows += 1;
    current.totalQty += qty;
    conflicts.set(part, current);
  });

  return Array.from(conflicts.values());
}

function buildModalShortageSupplementConflictMessage(conflicts) {
  const lines = conflicts.slice(0, 8).map(item => (
    `料號 ${item.part} 有 ${item.rows} 列填了補料但因為某列勾了缺料，補料將不會送出`
  ));
  if (conflicts.length > lines.length) lines.push(`另有 ${conflicts.length - lines.length} 個料號也有相同衝突`);
  lines.push("確定要繼續？");
  return [MODAL_SHORTAGE_SUPPLEMENT_WARNING_PREFIX, ...lines].join("\n");
}

function confirmModalShortageSupplementConflicts() {
  const conflicts = findModalShortageSupplementConflicts();
  if (!conflicts.length) return true;
  return confirm(buildModalShortageSupplementConflictMessage(conflicts));
}

if (typeof window !== "undefined") {
  window.__scheduleEcCollectionHotfix = {
    collectSupplements: _collectModalSupplements,
    collectDecisions: _collectModalDecisions,
    findConflicts: findModalShortageSupplementConflicts,
    buildConflictMessage: buildModalShortageSupplementConflictMessage,
    shouldAutoShortageCheck,
    hasNegativeRunningStock,
  };
}

function _collectModalOrderDecisions() {
  const list = document.getElementById("modal-shortage-list");
  if (!list) return {};
  const decisionsByOrder = {};

  list.querySelectorAll(".supplement-input").forEach(input => {
    if (input.closest(".shortage-item")?.dataset.flowHidden === "1") return;
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
    if (input.closest(".shortage-item")?.dataset.flowHidden === "1") return;
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
  const progress = document.getElementById("modal-download-progress");
  const fill = document.getElementById("modal-download-progress-fill");
  const percentLabel = document.getElementById("modal-download-percent");
  _modalProgressValue = value;
  if (fill && !progress?.classList.contains("is-indeterminate")) fill.style.width = `${value}%`;
  if (percentLabel) percentLabel.textContent = value >= 100 ? "完成" : "處理中";
}

function startModalProgressAnimation(targetPercent, intervalMs = 180) {
  stopModalProgressAnimation();
  setModalProgressPercent(Math.min(Number(targetPercent) || 0, 99));
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
    progress.classList.remove("is-error", "is-success", "is-indeterminate");
    if (config.tone === "error") progress.classList.add("is-error");
    if (config.tone === "success") progress.classList.add("is-success");
    if (active && config.tone !== "error" && config.tone !== "success") progress.classList.add("is-indeterminate");
  }
  if (percent != null) setModalProgressPercent(percent);
  const percentLabel = document.getElementById("modal-download-percent");
  if (percentLabel && config.tone === "error") percentLabel.textContent = "失敗";
  if (percentLabel && config.tone === "success") percentLabel.textContent = "完成";

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
  if (!active) {
    stopModalProgressAnimation();
    setModalProgressPercent(percent ?? 0);
    if (progress) progress.classList.remove("is-error", "is-success", "is-indeterminate");
  }
}

function formatWorkspaceFailureDetail(resultOrError) {
  const result = resultOrError || {};
  const lines = [];
  const failures = Array.isArray(result.failures) ? result.failures : [];
  const negativeShortages = Array.isArray(result.negative_shortages) ? result.negative_shortages : [];
  failures.slice(0, 12).forEach(item => {
    const label = item?.order_label || item?.order_id || item?.po_number || "訂單";
    const message = item?.error || item?.message || item?.detail || "寫入失敗";
    lines.push(`${label}: ${message}`);
  });
  negativeShortages.slice(0, 12).forEach(item => {
    const part = item?.part_number || item?.part || "料號";
    const order = item?.batch_code || item?.order_code || item?.order_id || "";
    const shortage = item?.shortage_amount ?? item?.shortage ?? item?.resulting_stock ?? "";
    lines.push(`${order ? `${order} ` : ""}${part}: 負庫存/缺料 ${shortage}`);
  });
  if (result?.message && !lines.length) lines.push(result.message);
  if (resultOrError instanceof Error && !lines.length) lines.push(resultOrError.message);
  if (!lines.length) lines.push("後端未回傳明細，請查看操作紀錄或伺服器日誌。");
  const omitted = failures.length + negativeShortages.length - lines.length;
  if (omitted > 0) lines.push(`另有 ${omitted} 筆未列出。`);
  return lines.join("\n");
}

async function handleModalWriteMain() {
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  if (!targetOrderIds.length) {
    showToast("沒有可寫入主檔的訂單");
    return;
  }
  if (!confirmModalShortageSupplementConflicts()) return;

  const supplements = _collectModalSupplements();
  const modalDecisions = _collectModalDecisions();
  const orderSupplements = _collectModalOrderSupplements();
  const orderDecisions = _collectModalOrderDecisions();
  const sampleOrderIds = collectModalSampleOrderIds();

  try {
    setModalDownloadProgress(true, "正在保存缺料決策...", "先保存這次要寫入主檔的缺料與補料內容。", 10);
    startModalProgressAnimation(35, 140);
    await persistDecisionsForOrders(modalDecisions, targetOrderIds, orderDecisions);
    Object.entries(modalDecisions).forEach(([part, decision]) => {
      setLocalDecision(part, decision);
    });

    setModalDownloadProgress(true, "正在寫入主檔...", `共 ${targetOrderIds.length} 筆訂單，可能需要一段時間；可先切到其他分頁，完成後會通知。`, 46);
    startModalProgressAnimation(92, 220);
    const result = await apiPost("/api/schedule/batch-dispatch", {
      order_ids: targetOrderIds,
      decisions: modalDecisions,
      supplements,
      order_decisions: orderDecisions,
      order_supplements: orderSupplements,
      sample_order_ids: sampleOrderIds,
    });

    _modalTargets.forEach(item => _checkedIds.delete(item.id));
    setModalDownloadProgress(true, "寫入完成", "主檔與已發料清單正在刷新。", 100);
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
    await new Promise(resolve => setTimeout(resolve, 200));
    const shortageCount = (result.shortages || []).length;
    if (shortageCount > 0) {
      showToast(`已寫入主檔 ${result.count} 筆，merge ${result.merged_parts} 個料件，${shortageCount} 筆缺料待補`, { tone: "success", duration: 5000 });
      showPostDispatchShortages(result.shortages);
    } else {
      showToast(`已寫入主檔 ${result.count} 筆，merge ${result.merged_parts} 個料件`, { tone: "success" });
    }
    if (isCalcWorkspaceActive()) closeShortageModal();
  } catch (error) {
    stopModalProgressAnimation();
    setModalDownloadProgress(
      true,
      "寫入主檔失敗",
      formatWorkspaceFailureDetail(error),
      100,
      { tone: "error", lockUi: false },
    );
    showToast("寫入主檔失敗: " + error.message, { sticky: true, tone: "error" });
  }
}

async function handleModalDownloadBom() {
  if (!_modalBomFiles.length) { showToast("找不到對應的 BOM 檔案"); return; }
  if (!confirmModalShortageSupplementConflicts()) return;

  const supplements = _collectModalSupplements();
  const modalDecisions = _collectModalDecisions();
  const orderSupplements = _collectModalOrderSupplements();
  const orderDecisions = _collectModalOrderDecisions();
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  const sampleOrderIds = collectModalSampleOrderIds();
  const headerOverrides = buildModalHeaderOverrides();

  try {
    setModalDownloadProgress(true, "正在保存補料決策...", "會把這次 merge 的補料內容記進系統。", 12);
    startModalProgressAnimation(32, 140);
    await persistDecisionsForOrders(modalDecisions, targetOrderIds, orderDecisions);
    Object.entries(modalDecisions).forEach(([part, decision]) => {
      setLocalDecision(part, decision);
    });

    const bomIds = _modalBomFiles.map(f => f.id);
    setModalDownloadProgress(true, "正在產生並下載 BOM...", `共 ${bomIds.length} 份 BOM，正在寫入補料量並打包；可先切到其他分頁。`, 42);
    startModalProgressAnimation(92, 220);
    const result = await desktopDownload({
      path: "/api/bom/dispatch-download",
      method: "POST",
      body: {
        bom_ids: bomIds,
        order_ids: targetOrderIds,
        supplements,
        order_supplements: orderSupplements,
        sample_order_ids: sampleOrderIds,
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

  if (!_completedRows.length && !_completedFolders.length) {
    container.innerHTML = '<div class="empty-state">尚無已發料的排程列</div>';
    updateCompletedToolbarState();
    return;
  }

  // 每個資料夾的直屬訂單依 code (X-X) 自然排序
  const parseCodeSort = (code) => {
    if (!code) return [Number.MAX_SAFE_INTEGER];
    return String(code).split("-").map(seg => {
      const n = parseInt(seg, 10);
      return Number.isFinite(n) ? n : Number.MAX_SAFE_INTEGER;
    });
  };
  const folderTree = buildCompletedFolderTree();
  const sortRowsByCode = (rows) => {
    rows.sort((a, b) => {
      const ac = parseCodeSort(a.code);
      const bc = parseCodeSort(b.code);
      const len = Math.max(ac.length, bc.length);
      for (let i = 0; i < len; i += 1) {
        const av = ac[i] ?? Number.MAX_SAFE_INTEGER;
        const bv = bc[i] ?? Number.MAX_SAFE_INTEGER;
        if (av !== bv) return av - bv;
      }
      return 0;
    });
  };
  const sortNodeRows = (node) => {
    sortRowsByCode(node.rows || []);
    for (const child of node.sortedChildren || []) sortNodeRows(child);
  };
  sortNodeRows(folderTree);

  // 所有資料夾選項（給下拉用）
  const allFolders = flattenCompletedFolderTree(folderTree);

  container.innerHTML = "";
  const renderedFolderPaths = new Set();

  // 先渲染有名字的資料夾樹
  for (const node of folderTree.sortedChildren || []) {
    appendCompletedFolderSection(container, node, allFolders, folderTree, renderedFolderPaths);
  }

  // 最後渲染未歸檔
  if (folderTree.rows.length) {
    container.appendChild(buildFolderSection(folderTree, allFolders, folderTree, renderedFolderPaths));
  }
  updateCompletedToolbarState();
}

function updateCompletedToolbarState() {
  const count = getCompletedCheckedOrderIds().length;
  const countEl = document.getElementById("completed-selected-count");
  const selectAll = document.getElementById("completed-select-all");
  const renderedChecks = getRenderedCompletedCheckControls();
  const renderedCheckedCount = renderedChecks.filter(item => _completedCheckedIds.has(item.id)).length;
  if (countEl) countEl.textContent = count ? `已選 ${count} 筆` : "";
  if (selectAll) {
    selectAll.checked = renderedChecks.length > 0 && renderedCheckedCount === renderedChecks.length;
    selectAll.indeterminate = renderedCheckedCount > 0 && renderedCheckedCount < renderedChecks.length;
    selectAll.disabled = renderedChecks.length === 0;
  }
  document.getElementById("btn-completed-download-drafts")?.toggleAttribute("disabled", count === 0);
  document.getElementById("btn-completed-gen-dispatch")?.toggleAttribute("disabled", count === 0);
}

function handleCompletedSelectAllChange(event) {
  const checked = Boolean(event.currentTarget?.checked);
  const renderedChecks = getRenderedCompletedCheckControls();
  for (const item of renderedChecks) {
    item.checkbox.checked = checked;
    if (checked) {
      _completedCheckedIds.add(item.id);
    } else {
      _completedCheckedIds.delete(item.id);
    }
  }
  if (!checked) _completedLastCheckedId = null;
  updateCompletedToolbarState();
}

function buildFolderSection(folderNode, allFolders, folderTree, renderedFolderPaths = new Set()) {
  const section = document.createElement("div");
  section.className = "completed-folder-section";

  const folderName = folderNode.path || "";
  const rows = folderNode.rows || [];
  const isUnsorted = !folderName;
  const label = isUnsorted ? "未歸檔" : folderNode.name;
  const isCollapsed = isCompletedFolderCollapsed(folderName);
  const totalCount = isUnsorted ? rows.length : folderNode.totalCount;
  const depth = isUnsorted ? 0 : Math.max(0, (folderNode.depth || 1) - 1);
  const parentOptions = !isUnsorted ? buildCompletedFolderParentOptions(folderName, allFolders) : "";

  // 標題列
  const header = document.createElement("div");
  header.className = "completed-folder-header";
  if (depth) header.style.paddingLeft = `${14 + depth * 18}px`;
  header.innerHTML = `
    <span class="folder-toggle" style="cursor:pointer;user-select:none">${isCollapsed ? "▶" : "▼"}</span>
    <span class="folder-name">${esc(label)}</span>
    <span style="font-size:11px;color:#8e8e93;margin-left:4px">(${totalCount})</span>
    ${!isUnsorted ? `<select class="folder-select completed-folder-parent-select" data-folder="${esc(folderName)}" title="移動資料夾到" style="margin-left:auto;font-size:11px;padding:2px 4px;border:1px solid #e5e5ea;border-radius:4px;max-width:150px">${parentOptions}</select><button class="btn-folder-delete" title="刪除資料夾（訂單移回未歸檔）" style="background:none;border:none;color:#dc2626;font-size:14px;cursor:pointer;padding:2px 6px">✕</button>` : ""}`;
  section.appendChild(header);

  // 卡片容器
  const body = document.createElement("div");
  body.className = "completed-folder-body";
  if (isCollapsed) body.style.display = "none";
  for (const r of rows) {
    body.appendChild(buildCompletedCard(r, allFolders));
  }
  for (const child of folderNode.sortedChildren || []) {
    appendCompletedFolderSection(body, child, allFolders, folderTree, renderedFolderPaths);
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

  header.querySelector(".completed-folder-parent-select")?.addEventListener("change", async (event) => {
    const select = event.currentTarget;
    const newParent = normalizeCompletedFolderPath(select.value);
    if (getCompletedFolderMoveDepth(folderName, newParent, allFolders) > 3) {
      showToast("搬移後資料夾會超過 3 層，請選擇較上層的位置");
      renderCompletedTab();
      return;
    }
    try {
      if (!folderNode.totalCount) {
        moveCompletedEmptyFolderLocal(folderName, newParent);
        renderCompletedTab();
        return;
      }
      await apiPost("/api/schedule/completed/folders/move", { folder: folderName, new_parent: newParent });
      await refreshCompleted();
    } catch (err) {
      showToast("資料夾搬移失敗：" + err.message);
      renderCompletedTab();
    }
  });

  // 刪除資料夾
  const delBtn = header.querySelector(".btn-folder-delete");
  if (delBtn) {
    delBtn.addEventListener("click", async () => {
      if (completedFolderHasChildren(folderName, folderTree)) {
        showToast("請先刪除或清空子資料夾");
        return;
      }
      if (!confirm(`確定刪除資料夾「${folderName}」？訂單會移回未歸檔。`)) return;
      try {
        delete _completedFolderCollapsedState[completedFolderStateKey(folderName)];
        saveCompletedFolderCollapsedState();
        if (!rows.length) {
          _completedFolders = _completedFolders.filter(item => normalizeCompletedFolderPath(item) !== folderName);
          renderCompletedTab();
          return;
        }
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
  const draft = _completedDraftsByOrderId?.[r.id] || null;
  const draftCollapsed = draft ? isCompletedDraftPanelCollapsed(r.id) : true;
  const date = (r.delivery_date || r.ship_date) ? (r.delivery_date || r.ship_date).slice(5).replace("-", "/") : "—";
  const qty = r.order_qty != null ? r.order_qty : "—";
  const code = r.code ? `<span class="tag tag-pcb" style="font-size:10px;padding:1px 4px">${esc(r.code)}</span>` : "";
  const orderId = normalizeOrderId(r.id);
  const checked = Number.isInteger(orderId) && _completedCheckedIds.has(orderId);

  // 資料夾下拉選項
  const currentFolder = normalizeCompletedFolderPath(r.folder);
  let folderOptions = `<option value=""${currentFolder === "" ? " selected" : ""}>未歸檔</option>`;
  for (const f of allFolders) {
    const indent = "　".repeat(Math.max(0, (f.depth || 1) - 1));
    folderOptions += `<option value="${esc(f.path)}"${currentFolder === f.path ? " selected" : ""}>${indent}${esc(f.path)}</option>`;
  }

  const draftToggleHtml = draft
    ? `<button
        class="btn-draft-toggle row-draft-toggle btn-completed-draft-toggle ${draftCollapsed ? "" : "is-expanded"}"
        type="button"
        data-order-id="${r.id}"
        aria-expanded="${draftCollapsed ? "false" : "true"}"
        title="${draftCollapsed ? "展開已發料副檔" : "收起已發料副檔"}"
      >${draftCollapsed ? "▶" : "▼"}</button>`
    : "";
  const draftHtml = draft ? buildCompletedDraftPanelHtml(draft, { collapsed: draftCollapsed }) : "";

  div.innerHTML = `
    <div class="completed-card-header">
      <input type="checkbox" class="completed-order-check" data-order-id="${r.id}" ${checked ? "checked" : ""} aria-label="選取已發料訂單">
      <span class="po-model-wrap">
        <span class="po-number">${esc(r.model)}</span>${draftToggleHtml}
      </span>
      <span style="color:#c7c7cc;font-size:13px">|</span>
      <span style="color:#6b7280;font-weight:500;font-size:14px;font-family:monospace">${r.po_number}</span>
      <span class="tag tag-pcb pcb-chip">${esc(r.pcb)}</span>
      ${code}
      <span style="font-size:13px;color:#3c3c43;font-weight:500">${qty}<span style="font-size:11px;color:#8e8e93;font-weight:400">pcs</span></span>
      <span class="po-ship-date">${date}</span>
      <div class="completed-card-actions">
        <button class="btn btn-danger btn-sm btn-rollback-order" data-order-id="${r.id}" title="退回此筆已發料並恢復可重扣；若後面還有已發料，會一起往後還原">退回已發料</button>
        <select class="folder-select" data-order-id="${r.id}" style="font-size:11px;padding:2px 4px;border:1px solid #e5e5ea;border-radius:4px;max-width:100px">${folderOptions}</select>
      </div>
    </div>
    ${draftHtml}`;

  div.querySelector(".completed-order-check")?.addEventListener("click", event => {
    const checkbox = event.currentTarget;
    const id = normalizeOrderId(checkbox?.dataset.orderId);
    if (!Number.isInteger(id)) return;
    const renderedChecks = getRenderedCompletedCheckControls();
    const currentIndex = renderedChecks.findIndex(item => item.id === id);
    const lastIndex = renderedChecks.findIndex(item => item.id === _completedLastCheckedId);
    if (event.shiftKey && currentIndex >= 0 && lastIndex >= 0) {
      const start = Math.min(currentIndex, lastIndex);
      const end = Math.max(currentIndex, lastIndex);
      for (const item of renderedChecks.slice(start, end + 1)) {
        item.checkbox.checked = checkbox.checked;
        if (checkbox.checked) {
          _completedCheckedIds.add(item.id);
        } else {
          _completedCheckedIds.delete(item.id);
        }
      }
    } else if (checkbox.checked) {
      _completedCheckedIds.add(id);
    } else {
      _completedCheckedIds.delete(id);
    }
    _completedLastCheckedId = id;
    updateCompletedToolbarState();
  });

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
  div.querySelector(".btn-completed-draft-view")?.addEventListener("click", () => {
    void showDraftModal(draft.id, { readOnly: true });
  });
  div.querySelector(".btn-completed-draft-download")?.addEventListener("click", () => {
    void downloadDraft(draft.id);
  });
  div.querySelector(".btn-completed-draft-toggle")?.addEventListener("click", event => {
    const button = event.currentTarget;
    const panel = div.querySelector(".completed-draft-panel");
    if (!button || !panel) return;
    const nextCollapsed = !panel.classList.contains("is-collapsed");
    panel.classList.toggle("is-collapsed", nextCollapsed);
    button.setAttribute("aria-expanded", nextCollapsed ? "false" : "true");
    button.textContent = nextCollapsed ? "▶" : "▼";
    button.classList.toggle("is-expanded", !nextCollapsed);
    button.title = nextCollapsed ? "展開已發料副檔" : "收起已發料副檔";
    setCompletedDraftPanelCollapsed(r.id, nextCollapsed);
  });

  return div;
}

function buildCompletedDraftPanelHtml(draft, { collapsed = false } = {}) {
  const files = Array.isArray(draft?.files) ? draft.files : [];
  const committedAt = formatDraftTime(draft?.committed_at || draft?.updated_at) || "--";
  return `
    <div class="completed-draft-panel ${collapsed ? "is-collapsed" : ""}">
      <div class="completed-draft-summary">
        <span class="merge-draft-pill">副檔 ${files.length} 份</span>
        <span class="merge-draft-meta">存檔 ${esc(committedAt)}</span>
      </div>
      ${buildDraftFileListHtml(files, { label: "已發料副檔" })}
      <div class="completed-draft-actions">
        <button class="btn btn-secondary btn-sm btn-completed-draft-view" data-draft-id="${draft.id}">查看副檔</button>
        <button class="btn btn-secondary btn-sm btn-completed-draft-download" data-draft-id="${draft.id}">下載副檔</button>
      </div>
    </div>`;
}

async function handleCompletedDownloadDrafts() {
  const orderIds = getCompletedCheckedOrderIds();
  if (!orderIds.length) {
    showToast("請先勾選要下載副檔的已發料訂單");
    return;
  }

  try {
    const result = await withGlobalBusy(
      () => desktopDownload({
        path: "/api/schedule/completed/drafts/download",
        method: "POST",
        body: { order_ids: orderIds },
      }),
      {
        title: "正在重新產生已發料副檔",
        detail: `共 ${orderIds.length} 筆訂單，會依目前主檔批次欄重新抓上批餘料與補料。`,
        phase: "讀取主檔",
        timeout: 600000,
      },
    );
    showDownloadToast(result, "已發料副檔");
  } catch (error) {
    showToast("已發料副檔下載失敗：" + error.message, { tone: "error", sticky: true });
  }
}

async function handleCompletedGenerateDispatch() {
  const orderIds = getCompletedCheckedOrderIds();
  if (!orderIds.length) {
    showToast("請先勾選要生成發料單的已發料訂單");
    return;
  }

  try {
    const result = await withGlobalBusy(
      () => desktopDownload({
        path: "/api/dispatch/generate",
        method: "POST",
        body: { order_ids: orderIds, decisions: {} },
      }),
      {
        title: "正在生成發料單",
        detail: `共 ${orderIds.length} 筆訂單，系統正在彙整補料與缺料資料。`,
        phase: "彙整資料",
        timeout: 600000,
      },
    );
    showDownloadToast(result, "發料單");
  } catch (error) {
    showToast("已發料發料單生成失敗：" + error.message, { tone: "error", sticky: true });
  }
}

async function handleCreateFolder() {
  const input = document.getElementById("new-folder-name");
  const validationMessage = validateCompletedFolderPath(input?.value || "");
  if (validationMessage) { showToast(validationMessage); return; }
  const name = normalizeCompletedFolderPath(input?.value || "");
  const existingFolders = new Set(flattenCompletedFolderTree(buildCompletedFolderTree()).map(item => item.path));
  if (existingFolders.has(name)) { showToast("資料夾已存在"); return; }

  // 直接建立：把一筆假訂單移過去再移回來太蠢，直接在前端記住
  // 實際上建立資料夾 = 有訂單被移進去才會出現
  // 所以先提示使用者建立後要把訂單移過去
  _completedFolders.push(name);
  input.value = "";
  showToast(`資料夾「${name}」已建立，請將訂單移入`);
  renderCompletedTab();
}

function isRollbackBlockedMessage(message) {
  return String(message || "").includes("後面已有其他庫存異動");
}

function flattenWipedDefectives(wipedDefectives) {
  const rows = [];
  for (const batch of (wipedDefectives || [])) {
    for (const item of (batch.items || [])) {
      rows.push({
        batchId: batch.batch_id,
        filename: batch.filename || "",
        importedAt: batch.imported_at || "",
        actionTaken: batch.action_taken || "不良品扣帳",
        partNumber: item.part_number || "",
        defectiveQty: Number(item.defective_qty || 0),
      });
    }
  }
  return rows;
}

function buildWipedDefectivesWarning(wipedDefectives) {
  const rows = flattenWipedDefectives(wipedDefectives);
  if (!rows.length) return "";
  const lines = rows.map((item, index) => {
    const date = item.importedAt ? item.importedAt.slice(0, 10) : "時間未知";
    const type = item.actionTaken === "加工多打扣帳" ? "加工多打" : "不良品";
    return `${index + 1}. ${item.partNumber} × ${fmt(item.defectiveQty)}（${date} 匯入，${type}）`;
  });
  return `\n\n⚠ 這次退回會沖掉以下不良品/加工多打扣帳，共 ${rows.length} 筆：\n${lines.join("\n")}`;
}

async function replayDefectivesAfterRollback(cutoff, expectedCount, actionButton = null) {
  const originalText = actionButton?.textContent || "";
  if (actionButton) {
    actionButton.disabled = true;
    actionButton.textContent = "補回中…";
  }
  try {
    const result = await apiPost("/api/defectives/replay-after-rollback", { cutoff });
    hideToast();
    showToast(
      `已補回 ${result.replayed_records || 0} 筆不良品/加工多打扣帳，共 ${result.replayed_batches || 0} 批`,
      { tone: "success", duration: 5000 }
    );
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
  } catch (error) {
    const retryLabel = originalText || `補回 ${expectedCount || 0} 筆不良品`;
    if (actionButton) {
      actionButton.disabled = false;
      actionButton.textContent = retryLabel;
    }
    showToast(`補回 ${expectedCount || 0} 筆不良品失敗：${error.message}`, {
      tone: "error",
      sticky: true,
      action: {
        label: retryLabel,
        onClick: (event) => replayDefectivesAfterRollback(cutoff, expectedCount, event.currentTarget),
      },
    });
  }
}

async function handleRollbackDispatch(orderId, trigger) {
  if (!Number.isInteger(orderId)) return;

  const button = trigger || document.querySelector(`.btn-rollback-order[data-order-id="${orderId}"]`);
  const originalText = button?.textContent || "退回已發料";

  try {
    if (button) {
      button.disabled = true;
      button.textContent = "確認中...";
    }
    let preview = null;
    let forceDelete = false;

    try {
      preview = await apiJson(`/api/schedule/orders/${orderId}/rollback-preview`);
    } catch (error) {
      if (!isRollbackBlockedMessage(error.message)) throw error;
      preview = await apiJson(`/api/schedule/orders/${orderId}/rollback-preview?force=1`);
      const forceOrderLines = (preview.orders || []).map((item, index) => `${index + 1}. ${item.po_number} ${item.model}`).join("\n");
      const forceAffectedHint = Number(preview.count || 0) > 1
        ? "\n\n這次強制退回會連同後面已發料的訂單一起退回。"
        : "";
      const wipedWarning = buildWipedDefectivesWarning(preview.wiped_defectives || []);
      const forcedConfirmed = confirm(
        `這筆目前被安全機制擋住，因為後面已有其他庫存異動。\n\n如果仍要強制退回，系統會直接用當時備份覆蓋目前主檔，後面新增的主檔異動也會一起被還原。${forceAffectedHint}${wipedWarning}\n\n共 ${preview.count} 筆：\n${forceOrderLines}\n\n確定仍要強制退回嗎？`
      );
      if (!forcedConfirmed) return;
      forceDelete = true;
    }

    const orderLines = (preview.orders || []).map((item, index) => `${index + 1}. ${item.po_number} ${item.model}`).join("\n");
    const affectedHint = Number(preview.count || 0) > 1
      ? "\n\n因為主檔是照順序連續扣帳，後面已發料的訂單也會一起退回。"
      : "";

    if (!forceDelete) {
      const confirmed = confirm(
        `這會把所選訂單從已發料退回，恢復可重扣狀態，共 ${preview.count} 筆：\n${orderLines}${affectedHint}\n\n主檔也會一併還原到當時備份。確定繼續嗎？`
      );
      if (!confirmed) return;
    }

    if (button) button.textContent = forceDelete ? "強制退回中..." : "退回中...";
    const result = await apiPost(`/api/schedule/orders/${orderId}/rollback${forceDelete ? "?force=1" : ""}`);
    const draftNote = Number(result.restored_draft_count || 0) > 0
      ? `\n已恢復 ${result.restored_draft_count} 筆副檔工作台，可直接接續修改。`
      : "";
    const actionLabel = result.forced ? "已強制退回" : "已退回";
    const replayRows = flattenWipedDefectives(result.wiped_defectives || []);
    if (result.replay_cutoff) {
      showToast(`${actionLabel} ${result.count} 筆已發料\n主檔已同步還原，可重新扣帳${draftNote}`, {
        sticky: true,
        tone: "success",
        action: {
          label: `補回 ${replayRows.length} 筆不良品`,
          onClick: (event) => replayDefectivesAfterRollback(result.replay_cutoff, replayRows.length, event.currentTarget),
        },
      });
    } else {
      showToast(`${actionLabel} ${result.count} 筆已發料\n主檔已同步還原，可重新扣帳${draftNote}`, { sticky: Boolean(draftNote), tone: "success" });
    }
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
  } catch (error) {
    showToast("退回已發料失敗：" + error.message, { tone: "error" });
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
          await refreshScheduleOnly();
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
    await refreshScheduleOnly();
    showToast(`已建立 ${result.draft_count || 0} 份副檔，請在訂單下方確認後再按勾寫入主檔`);
  } catch (error) {
    showToast("批次 merge 失敗: " + error.message);
  }
}

async function handleDeleteDraftLegacyV3(draftId, model) {
  if (!confirm(`確認要刪除 ${model} 的副檔嗎？`)) return;
  try {
    await apiFetch(`/api/schedule/drafts/${draftId}`, { method: "DELETE" });
    showToast("副檔已刪除");
    await refreshScheduleOnly();
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
    const badgeState = buildOrderBadge(row, res, visibleShortageTotals);
    syncRowBadge(div, badgeState);
  });

  const { shortages, csShortages } = buildRightPanelShortageData();
  renderShortagePanel(shortages, csShortages, []);
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

// ── Shortage panel ────────────────────────────────────────────────────────────
function compareText(a, b, locale = "en") {
  return String(a || "").localeCompare(String(b || ""), locale, { numeric: true, sensitivity: "base" });
}

function _parseCodeSegments(code) {
  return String(code || "")
    .split(/[-_]/)
    .map(seg => {
      const num = parseInt(seg, 10);
      return Number.isFinite(num) ? num : Number.MAX_SAFE_INTEGER;
    });
}

function compareShortageItems(a, b) {
  const codeA = _parseCodeSegments(a?._row_code);
  const codeB = _parseCodeSegments(b?._row_code);
  const len = Math.max(codeA.length, codeB.length);
  for (let i = 0; i < len; i += 1) {
    const av = codeA[i] ?? Number.MAX_SAFE_INTEGER;
    const bv = codeB[i] ?? Number.MAX_SAFE_INTEGER;
    if (av !== bv) return av - bv;
  }
  const partCmp = compareText(a?.part_number, b?.part_number);
  if (partCmp !== 0) return partCmp;
  const groupCmp = compareText(a?._row_group_label, b?._row_group_label, "zh-Hant");
  return groupCmp;
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

function setRightPanelBadge(count, { purchase = false } = {}) {
  const badge = document.getElementById("shortage-count");
  if (!badge) return;
  badge.classList.toggle("is-purchase-alert", Boolean(purchase));
  if (count > 0) {
    badge.style.display = "inline";
    badge.textContent = String(count);
  } else {
    badge.style.display = "none";
    badge.textContent = "";
  }
}

function setRightPanelTabCount(id, count) {
  const badge = document.getElementById(id);
  if (!badge) return;
  badge.textContent = count > 0 ? String(count) : "";
  badge.classList.toggle("has-items", count > 0);
}

function getActivePurchaseReminderCount(items = buildPurchaseReminderItems()) {
  return (Array.isArray(items) ? items : []).filter(item => !item.notified && !item.ignored).length;
}

function updateRightPanelTabs(shortageCount = 0, purchaseCount = getActivePurchaseReminderCount()) {
  document.querySelectorAll("[data-right-panel-tab]").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.rightPanelTab === _rightPanelActiveTab);
  });
  const title = document.querySelector("#right-panel-header h3");
  if (title) title.textContent = _rightPanelActiveTab === "purchase" ? "買料提醒" : "補料明細";
  setRightPanelTabCount("right-panel-shortage-tab-count", shortageCount);
  setRightPanelTabCount("right-panel-purchase-tab-count", purchaseCount);
}

function activateRightPanelTab(tabName) {
  const nextTab = tabName === "purchase" ? "purchase" : "shortages";
  if (nextTab === "purchase") {
    renderPurchaseReminderPanel();
    return;
  }

  _rightPanelActiveTab = "shortages";
  if (_rightPanelMode === "postDispatch") {
    renderPostDispatchPanel();
    return;
  }
  renderShortagePanel(
    _lastShortagePanelData.shortages,
    _lastShortagePanelData.csShortages,
    _lastShortagePanelData.mainDeficits,
  );
}

function renderPurchaseReminderPanel() {
  _rightPanelActiveTab = "purchase";
  const scroll = document.getElementById("right-scroll");
  const allItems = buildPurchaseReminderItems({ includeInactive: true });
  const hiddenInactiveCount = allItems.filter(item => !item.active_demand).length;
  const items = _purchaseReminderShowAll
    ? allItems
    : allItems.filter(item => item.active_demand);
  const activeCount = getActivePurchaseReminderCount(items);
  const shortageCount = _rightPanelMode === "postDispatch"
    ? _postDispatchShortages.length
    : (_lastShortagePanelData.shortages.length
      + _lastShortagePanelData.csShortages.length
      + _lastShortagePanelData.mainDeficits.length);
  updateRightPanelTabs(shortageCount, activeCount);
  setRightPanelBadge(activeCount, { purchase: true });

  if (!scroll) return;
  if (!items.length) {
    const hiddenHtml = hiddenInactiveCount
      ? `<div style="margin-top:8px"><button class="btn btn-secondary btn-xs purchase-reminder-toggle-scope" type="button">顯示全部 ${hiddenInactiveCount} 筆舊安全庫存提醒</button></div>`
      : "";
    scroll.innerHTML = `<div class="no-shortage-msg">目前排程沒有 IC / OC / UC ST 買料提醒${hiddenHtml}</div>`;
    bindPurchaseReminderPanelActions(scroll, items);
    return;
  }

  const pendingItems = items.filter(item => !item.notified && !item.ignored);
  const notifiedItems = items.filter(item => item.notified && !item.ignored);
  const ignoredItems = items.filter(item => item.ignored);
  const notifiedCount = notifiedItems.length;
  const ignoredCount = ignoredItems.length;
  let html = `<div class="purchase-reminder-toolbar">
    <div class="purchase-reminder-toolbar-main">
      <div class="purchase-reminder-toolbar-title">IC/OC/UC ST 買料提醒</div>
      <div class="purchase-reminder-toolbar-meta">待通知 ${activeCount}，已通知 ${notifiedCount}，已忽略 ${ignoredCount}${!_purchaseReminderShowAll && hiddenInactiveCount ? `，已隱藏非目前排程 ${hiddenInactiveCount}` : ""}</div>
    </div>
    <div class="purchase-reminder-toolbar-actions">
      <button class="btn btn-secondary btn-xs purchase-reminder-toggle-scope" type="button">${_purchaseReminderShowAll ? "只看目前排程" : `顯示全部${hiddenInactiveCount ? ` ${hiddenInactiveCount}` : ""}`}</button>
      <button class="btn btn-primary btn-xs purchase-reminder-export" type="button">匯出 Excel</button>
    </div>
  </div>`;
  if (pendingItems.length) {
    html += renderPurchaseReminderSection("待通知採購", pendingItems, { key: "pending", collapsible: true });
  }
  if (notifiedItems.length) {
    html += renderPurchaseReminderSection("已通知採購", notifiedItems, { key: "notified", collapsible: true });
  }
  if (ignoredItems.length) {
    html += renderPurchaseReminderSection("已忽略", ignoredItems, { key: "ignored", collapsible: true, collapsed: true });
  }
  scroll.innerHTML = html;
  bindPurchaseReminderPanelActions(scroll, items);
}

function renderPurchaseReminderSection(title, items, options = {}) {
  const grouped = {};
  for (const item of items) {
    const vendor = normalizeVendorName(item.vendor);
    if (!grouped[vendor]) grouped[vendor] = [];
    grouped[vendor].push(item);
  }

  const key = options.key || title;
  const collapsible = Boolean(options.collapsible);
  const collapsed = collapsible
    ? Boolean(_purchaseReminderCollapsed[key] ?? options.collapsed)
    : false;
  let html = `<div class="purchase-reminder-section" data-section="${esc(key)}" data-collapsed="${collapsed ? "true" : "false"}">`;
  html += `<h4 class="purchase-reminder-section-title ${collapsible ? "is-collapsible" : ""} ${collapsed ? "is-collapsed" : ""}" ${collapsible ? `data-section="${esc(key)}"` : ""}>${esc(title)} <span>${items.length}</span></h4>`;
  for (const [vendor, vendorItems] of Object.entries(grouped).sort((a, b) => compareText(a[0], b[0], "zh-Hant"))) {
    html += `<div class="purchase-reminder-vendor-heading">${esc(vendor)} <span>${vendorItems.length}</span></div>`;
    for (const item of vendorItems) {
      html += purchaseReminderItemHtml(item);
    }
  }
  html += "</div>";
  return html;
}

function purchaseReminderItemHtml(item) {
  const currentStock = roundShortageUiValue(item.current_stock);
  const mainStock = roundShortageUiValue(item.main_stock || 0);
  const activeUsedQty = roundShortageUiValue(item.active_used_qty || 0);
  const projectedAvailable = roundShortageUiValue(item.projected_available ?? ((item.current_stock || 0) + Math.max(0, item.main_stock || 0) - activeUsedQty));
  const threshold = roundShortageUiValue(item.threshold);
  const moq = roundShortageUiValue(item.moq || 0);
  const suggestedQty = roundShortageUiValue(item.suggested_qty || 0);
  const notifiedTag = item.notified ? '<span class="purchase-reminder-tag is-notified">已通知採購</span>' : "";
  const ignoredTag = item.ignored ? '<span class="purchase-reminder-tag is-ignored">已忽略</span>' : "";
  const inactiveTag = item.active_demand ? "" : '<span class="purchase-reminder-tag">非目前排程</span>';
  const noteHtml = item.notification_note
    ? `<div class="purchase-reminder-note">備註：${esc(item.notification_note)}</div>`
    : "";
  const notifiedAtHtml = item.notified_at
    ? `<span style="color:#6b7280">通知 ${esc(String(item.notified_at).replace("T", " ").slice(0, 16))}</span>`
    : "";
  const ignoredAtHtml = item.ignored_at
    ? `<span style="color:#6b7280">忽略 ${esc(String(item.ignored_at).replace("T", " ").slice(0, 16))}</span>`
    : "";
  const usedByHtml = purchaseReminderUsedByHtml(item.used_by);

  return `<div class="shortage-item purchase-reminder-item ${item.notified ? "is-notified" : ""} ${item.ignored ? "is-ignored" : ""}" data-part="${esc(item.part_number)}">
    <div class="part">${esc(item.part_number)} <span class="purchase-reminder-tag">${esc(item.status)}</span>${inactiveTag}${notifiedTag}${ignoredTag}</div>
    <div class="desc">${esc(item.description || "—")}</div>
    ${usedByHtml}
    <div class="purchase-reminder-vendor-line">
      <span>廠商：<strong>${esc(normalizeVendorName(item.vendor))}</strong></span>
      <button class="btn btn-secondary btn-xs purchase-reminder-vendor-edit" type="button" data-part="${esc(item.part_number)}">改廠商</button>
    </div>
    <div class="amounts">
      <span class="amber">ST 庫存 ${fmt(currentStock)}</span>
      <span class="green">主檔庫存 ${fmt(mainStock)}</span>
      ${activeUsedQty > 0 ? `<span style="color:#6b7280">排程後可用 ${fmt(projectedAvailable)}</span>` : ""}
      <span style="color:#6b7280">安全線 ${fmt(threshold)}</span>
      ${moq > 0 ? `<span style="color:#8b5cf6">MOQ ${fmt(moq)}</span>` : ""}
      <span class="blue">建議買 ${fmt(suggestedQty)}</span>
      ${notifiedAtHtml}
      ${ignoredAtHtml}
    </div>
    ${noteHtml}
    <div class="purchase-reminder-actions">
      <button class="btn btn-secondary btn-xs purchase-reminder-copy" type="button" data-part="${esc(item.part_number)}">複製料號</button>
      <button class="btn ${item.notified ? "btn-secondary" : "btn-primary"} btn-xs purchase-reminder-notify" type="button" data-part="${esc(item.part_number)}" data-notified="${item.notified ? "0" : "1"}">
        ${item.notified ? "取消通知" : "已通知採購"}
      </button>
      <button class="btn btn-secondary btn-xs purchase-reminder-ignore" type="button" data-part="${esc(item.part_number)}" data-ignored="${item.ignored ? "0" : "1"}">
        ${item.ignored ? "取消忽略" : "忽略"}
      </button>
    </div>
  </div>`;
}

function purchaseReminderUsedByHtml(usedBy) {
  const rows = Array.isArray(usedBy) ? usedBy : [];
  if (!rows.length) {
    return '<div class="purchase-reminder-used-by is-empty">目前排程沒有使用中的機種</div>';
  }
  const totalUsedQty = roundShortageUiValue(rows.reduce(
    (sum, row) => sum + (Number(row?.used_qty || 0) || 0),
    0,
  ));
  const displayRows = rows.slice(0, 4);
  const chips = displayRows.map(row => {
    const model = String(row?.model || "").trim() || "未指定機種";
    const code = String(row?.code || "").trim();
    const po = String(row?.po_number || "").trim();
    const date = String(row?.ship_date || "").trim();
    const usedQty = roundShortageUiValue(row?.used_qty || 0);
    const meta = [code, po ? `PO ${po}` : "", date].filter(Boolean).join(" / ");
    return `<span class="purchase-reminder-used-chip">${esc(model)} <strong>用量 ${fmt(usedQty)}</strong>${meta ? ` <em>${esc(meta)}</em>` : ""}</span>`;
  }).join("");
  const more = rows.length > displayRows.length
    ? `<span class="purchase-reminder-used-more">另 ${rows.length - displayRows.length} 筆</span>`
    : "";
  return `<div class="purchase-reminder-used-by"><span class="purchase-reminder-used-label">用到：</span>${chips}${more}<span class="purchase-reminder-used-total">合計 ${fmt(totalUsedQty)}</span></div>`;
}

function bindPurchaseReminderPanelActions(scroll, items) {
  scroll.querySelectorAll(".purchase-reminder-section-title.is-collapsible").forEach(title => {
    title.addEventListener("click", () => {
      const key = title.dataset.section || "";
      const section = title.closest(".purchase-reminder-section");
      if (!key || !section) return;
      const collapsed = section.dataset.collapsed !== "true";
      _purchaseReminderCollapsed[key] = collapsed;
      section.dataset.collapsed = collapsed ? "true" : "false";
      title.classList.toggle("is-collapsed", collapsed);
    });
  });
  scroll.querySelector(".purchase-reminder-export")?.addEventListener("click", () => {
    void exportPurchaseReminders(items);
  });
  scroll.querySelector(".purchase-reminder-toggle-scope")?.addEventListener("click", () => {
    _purchaseReminderShowAll = !_purchaseReminderShowAll;
    renderPurchaseReminderPanel();
  });
  scroll.querySelectorAll(".purchase-reminder-copy").forEach(btn => {
    btn.addEventListener("click", () => {
      void copyPurchaseReminderPart(btn);
    });
  });
  scroll.querySelectorAll(".purchase-reminder-notify").forEach(btn => {
    btn.addEventListener("click", () => {
      void savePurchaseReminderNotification(btn);
    });
  });
  scroll.querySelectorAll(".purchase-reminder-ignore").forEach(btn => {
    btn.addEventListener("click", () => {
      void savePurchaseReminderIgnore(btn);
    });
  });
  scroll.querySelectorAll(".purchase-reminder-vendor-edit").forEach(btn => {
    btn.addEventListener("click", () => {
      void editPurchaseReminderVendor(btn);
    });
  });
}

async function copyPurchaseReminderPart(button) {
  const part = normalizePartKey(button?.dataset.part);
  try {
    if (!navigator.clipboard?.writeText) throw new Error("clipboard unavailable");
    await navigator.clipboard.writeText(part);
    showToast(`已複製 ${part}`);
  } catch (_) {
    showToast(part);
  }
}

async function savePurchaseReminderNotification(button) {
  const part = normalizePartKey(button?.dataset.part);
  const notified = String(button?.dataset.notified || "") === "1";
  if (!part) return;

  let note = "";
  if (notified) {
    const currentNote = _purchaseReminderStatuses?.[part]?.note || "";
    const entered = prompt(`標記 ${part} 已通知採購，可輸入備註：`, currentNote);
    if (entered === null) return;
    note = entered;
  }

  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "保存中...";
  try {
    const result = await apiPatch("/api/main-file/purchase-reminder-status", {
      part_number: part,
      notified,
      note,
    });
    const status = result.status || {};
    await loadMainData();
    if (!_purchaseReminderStatuses?.[part] && (status.notified || status.ignored)) {
      _purchaseReminderStatuses[part] = status;
    }
    showToast(notified ? `${part} 已標記通知採購` : `${part} 已取消通知`, { tone: "success" });
    renderPurchaseReminderPanel();
  } catch (error) {
    showToast("通知狀態保存失敗：" + error.message, { tone: "error" });
    if (button.isConnected) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
}

async function savePurchaseReminderIgnore(button) {
  const part = normalizePartKey(button?.dataset.part);
  const ignored = String(button?.dataset.ignored || "") === "1";
  if (!part) return;

  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "保存中...";
  try {
    const result = await apiPatch("/api/main-file/purchase-reminder-status", {
      part_number: part,
      ignored,
    });
    const status = result.status || {};
    await loadMainData();
    if (!_purchaseReminderStatuses?.[part] && (status.notified || status.ignored)) {
      _purchaseReminderStatuses[part] = status;
    }
    showToast(ignored ? `${part} 已忽略買料提醒` : `${part} 已取消忽略`, { tone: "success" });
    renderPurchaseReminderPanel();
  } catch (error) {
    showToast("忽略狀態保存失敗：" + error.message, { tone: "error" });
    if (button.isConnected) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
}

async function editPurchaseReminderVendor(button) {
  const part = normalizePartKey(button?.dataset.part);
  if (!part) return;

  const currentVendor = normalizeVendorName(_vendors?.[part]);
  const entered = prompt(`修改 ${part} 的主檔 B 欄廠商：`, currentVendor === "未分類廠商" ? "" : currentVendor);
  if (entered === null) return;

  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "保存中...";
  try {
    const result = await apiPatch("/api/main-file/vendor", {
      part_number: part,
      vendor: entered,
    });
    _vendors[part] = result.vendor || "";
    showToast(`已更新 ${part} 廠商：${normalizeVendorName(result.vendor)}`, { tone: "success" });
    renderPurchaseReminderPanel();
    if (_onRefreshMain) void _onRefreshMain();
  } catch (error) {
    showToast("廠商更新失敗：" + error.message, { tone: "error" });
    if (button.isConnected) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
}

async function exportPurchaseReminders(items) {
  const payload = (Array.isArray(items) ? items : buildPurchaseReminderItems()).filter(item => !item.ignored).map(item => ({
    vendor: normalizeVendorName(item.vendor),
    part_number: item.part_number,
    description: item.description || "",
    current_stock: Number(item.current_stock || 0) || 0,
    main_stock: Number(item.main_stock || 0) || 0,
    active_used_qty: Number(item.active_used_qty || 0) || 0,
    projected_available: Number(item.projected_available || 0) || 0,
    threshold: Number(item.threshold || 0) || 0,
    moq: Number(item.moq || 0) || 0,
    suggested_qty: Number(item.suggested_qty || 0) || 0,
    notified: Boolean(item.notified),
    notified_at: item.notified_at || "",
    note: item.notification_note || "",
  }));
  try {
    const result = await desktopDownload({
      path: "/api/main-file/purchase-reminders/export",
      method: "POST",
      body: { items: payload },
    });
    showDownloadToast(result, "買料提醒");
  } catch (error) {
    showToast("買料提醒匯出失敗：" + error.message, { tone: "error" });
  }
}

function renderShortagePanel(shortages, csShortages = [], mainDeficits = []) {
  _rightPanelMode = "shortages";
  _lastShortagePanelData = {
    shortages: Array.isArray(shortages) ? shortages : [],
    csShortages: Array.isArray(csShortages) ? csShortages : [],
    mainDeficits: Array.isArray(mainDeficits) ? mainDeficits : [],
  };
  const scroll = document.getElementById("right-scroll");
  const orderShortageCount = shortages.length + csShortages.length;
  const totalCount = orderShortageCount + mainDeficits.length;
  updateRightPanelTabs(totalCount);

  if (_rightPanelActiveTab === "purchase") {
    renderPurchaseReminderPanel();
    return;
  }

  _rightPanelActiveTab = "shortages";
  updateRightPanelTabs(totalCount);

  if (!totalCount) {
    scroll.innerHTML = '<div class="no-shortage-msg">無缺料</div>';
    setRightPanelBadge(0);
    return;
  }

  setRightPanelBadge(totalCount);

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
      showToast(
        `${result.part_number} 已補料 ${fmt(qty)}，庫存 ${fmt(result.stock_before)} → ${fmt(result.stock_after)}`,
        { tone: "success", duration: 4000 },
      );
      await refresh();
      await refreshCompleted();
      showPostDispatchShortages();
      if (_onRefreshMain) await _onRefreshMain();
      return;
    }

    const response = await apiPut("/api/schedule/shortage-settings", {
      order_ids: [orderId],
      order_decisions: {
        [String(orderId)]: {
          [part]: qty > 0 ? "CreateRequirement" : "None",
        },
      },
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
    if (!hasRemainingShortageForResultingStock(part, nextResultingStock)) {
      removeRightPanelShortageRowIfResolved(row);
    }
    showToast(qty > 0 ? `已保存 ${part} 補料 ${fmt(qty)}` : `已清除 ${part} 補料`);
    await refreshScheduleOnly();
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
  const isMainStockItem = Boolean(s?._main_stock_level);
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
  const _orderByCode = (code) => {
    if (!code) return null;
    return _rows.find(r => r.code === code) || _completedRows.find(r => r.code === code) || null;
  };
  const _matchedOrder = _orderByCode(s._row_code);
  const _displayModel = s._row_model || _matchedOrder?.model || "";
  const _displayPo = s._po_number || _matchedOrder?.po_number || "";
  const _displayShip = _matchedOrder?.ship_date || _matchedOrder?.delivery_date || "";
  const codeTag = s._row_code
    ? `<span class="tag tag-pcb" style="font-size:10px;padding:1px 6px">${esc(s._row_code)}</span>`
    : "";
  const metaParts = [];
  if (codeTag) metaParts.push(codeTag);
  if (_displayModel) metaParts.push(`<span>${esc(_displayModel)}</span>`);
  if (_displayPo) metaParts.push(`<span>PO ${esc(_displayPo)}</span>`);
  if (_displayShip) metaParts.push(`<span>出貨 ${esc(String(_displayShip).slice(0, 10))}</span>`);
  const metaLineHtml = metaParts.length
    ? `<div class="shortage-meta" style="display:flex;flex-wrap:wrap;gap:6px;align-items:center;font-size:11px;color:#3b82f6;margin-top:2px">${metaParts.join("")}</div>`
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
  const supplementEditorHtml = isMainStockItem
    ? `<div class="right-panel-supplement-row" style="display:flex;gap:6px;align-items:center;margin-top:8px">
        <label style="font-size:11px;color:#6b7280;white-space:nowrap">補主檔</label>
        <input
          type="number"
          class="right-panel-supplement-input"
          data-part="${esc(s.part_number)}"
          data-main-supplement="true"
          value="${roundShortageUiValue(s.suggested_qty || s.shortage_amount || 0)}"
          min="0"
          step="any"
          style="flex:1;min-width:0;padding:6px 8px;border:1px solid #d1d5db;border-radius:8px;font-size:12px"
        >
        <button class="btn btn-secondary btn-xs right-panel-supplement-save" data-part="${esc(s.part_number)}" data-main-supplement="true">補主檔</button>
      </div>
      <div style="font-size:10px;color:#6b7280;margin-top:4px">直接補到主檔，後續同料號欄位會一起同步加回</div>`
    : !isCS && Number.isInteger(orderId)
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
    <div class="part">${s.part_number}${csTag}</div>
    ${metaLineHtml}
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
    ${isCS ? '<div style="font-size:11px;color:#ca8a04;margin-top:4px">請通知客戶提供此料</div>' : isMainStockItem ? "" : `
    <div class="decision-btns">
      <button class="dec-btn ${dec === "CreateRequirement" ? "active-create" : ""}" data-dec="CreateRequirement" data-part="${s.part_number}">需採購</button>
      <button class="dec-btn ${dec === "MarkHasPO" ? "active-has-po" : ""}" data-dec="MarkHasPO" data-part="${s.part_number}">已有PO</button>
      <button class="dec-btn ${dec === "IgnoreOnce" ? "active-ignore" : ""}" data-dec="IgnoreOnce" data-part="${s.part_number}">忽略</button>
      <button class="dec-btn ${dec === "Shortage" ? "active-shortage" : ""}" data-dec="Shortage" data-part="${s.part_number}">缺料</button>
    </div>`}
  </div>`;
}

// ── Post-dispatch shortage panel + single-part supplement modal ───────────────

let _postDispatchShortages = [];

function showPostDispatchShortages(shortages) {
  _postDispatchShortages = mergePostDispatchShortageLists(
    buildPostDispatchShortagesFromResponse(shortages),
    buildPostDispatchShortagesFromCompletedDrafts(),
  );
  renderPostDispatchPanel();
}

function renderPostDispatchPanel() {
  _rightPanelMode = "postDispatch";
  _rightPanelActiveTab = "shortages";
  updateRightPanelTabs(_postDispatchShortages.length);
  const scroll = document.getElementById("right-scroll");
  if (!_postDispatchShortages.length) {
    scroll.innerHTML = '<div class="no-shortage-msg">無缺料</div>';
    setRightPanelBadge(0);
    return;
  }

  setRightPanelBadge(_postDispatchShortages.length);

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
      const orderId = normalizeOrderId(checkbox.dataset.orderId);
      checkbox.dataset.manual = "1";
      syncDraftPartControls(list, partKey, {
        shortageChecked: checkbox.checked,
        orderId,
      });
      const input = checkbox.closest(".shortage-item")?.querySelector(".supplement-input");
      if (input) {
        input.disabled = checkbox.checked;
        if (checkbox.checked) input.value = "0";
      }
      void refreshModalCalcPreview();
    });
  });

  list.querySelectorAll(".supplement-input").forEach(input => {
    input.addEventListener("input", () => {
      const partKey = normalizePartKey(input.dataset.part);
      if (!partKey) return;
      const orderId = normalizeOrderId(input.dataset.orderId);
      const checkbox = input.closest(".shortage-item")?.querySelector(".shortage-mark");
      if (checkbox?.checked) {
        checkbox.checked = false;
        checkbox.dataset.manual = "1";
      }
      syncDraftPartControls(list, partKey, {
        qty: parseFloat(input.value) || 0,
        shortageChecked: false,
        orderId,
      });
      void refreshModalCalcPreview();
    });
  });
}

function bindSampleOrderFlags(list) {
  if (!list) return;
  list.querySelectorAll(".sample-order-flag").forEach(checkbox => {
    checkbox.addEventListener("change", () => {
      void refreshModalCalcPreview();
    });
  });
}

function collectModalSampleOrderIds() {
  const list = document.getElementById("modal-shortage-list");
  if (!list) return [];
  return [...list.querySelectorAll(".sample-order-flag:checked")]
    .map(checkbox => normalizeOrderId(checkbox.dataset.orderId))
    .filter(Number.isInteger);
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

// ── Manual supplement modal ──────────────────────────────────────────────────

function populateManualSupplementPartOptions() {
  const datalist = document.getElementById("manual-supplement-parts");
  if (!datalist) return;
  const parts = Object.keys(_stock || {}).sort((a, b) => a.localeCompare(b));
  datalist.innerHTML = parts.map(part => {
    const qty = _stock[part];
    const label = `庫存 ${fmt(qty)}`;
    return `<option value="${esc(part)}" label="${esc(label)}"></option>`;
  }).join("");
}

function updateManualSupplementPartHint() {
  const input = document.getElementById("manual-supplement-part");
  const hint = document.getElementById("manual-supplement-part-hint");
  if (!input || !hint) return;
  const part = (input.value || "").trim().toUpperCase();
  if (!part) { hint.textContent = ""; return; }
  if (_stock && Object.prototype.hasOwnProperty.call(_stock, part)) {
    hint.textContent = `目前主檔庫存：${fmt(_stock[part])}`;
    hint.style.color = "#6b7280";
  } else {
    hint.textContent = "主檔中找不到此料號";
    hint.style.color = "#dc2626";
  }
}

async function openManualSupplementModal() {
  const modal = document.getElementById("manual-supplement-modal");
  if (!modal) return;

  document.getElementById("manual-supplement-part").value = "";
  document.getElementById("manual-supplement-qty").value = "";
  document.getElementById("manual-supplement-note").value = "";
  const hintEl = document.getElementById("manual-supplement-part-hint");
  if (hintEl) hintEl.textContent = "";
  document.getElementById("manual-supplement-logs").innerHTML = "<div style='text-align:center;color:#9ca3af;font-size:12px;padding:8px'>載入紀錄中...</div>";

  populateManualSupplementPartOptions();

  modal.style.display = "flex";
  modal.setAttribute("aria-hidden", "false");
  document.getElementById("manual-supplement-part").focus();

  document.getElementById("manual-supplement-close").onclick = closeManualSupplementModal;
  document.getElementById("manual-supplement-cancel").onclick = closeManualSupplementModal;
  document.getElementById("manual-supplement-submit").onclick = submitManualSupplement;
  const partInput = document.getElementById("manual-supplement-part");
  if (partInput) partInput.oninput = updateManualSupplementPartHint;

  try {
    const data = await apiJson("/api/schedule/supplement-logs?limit=30");
    renderSupplementLogs(data.logs || []);
  } catch (_) {
    document.getElementById("manual-supplement-logs").innerHTML = "";
  }
}

function closeManualSupplementModal() {
  const modal = document.getElementById("manual-supplement-modal");
  if (modal) {
    modal.style.display = "none";
    modal.setAttribute("aria-hidden", "true");
  }
}

function renderSupplementLogs(logs) {
  const container = document.getElementById("manual-supplement-logs");
  if (!container) return;
  if (!logs.length) {
    container.innerHTML = "<div style='text-align:center;color:#9ca3af;font-size:12px;padding:8px'>尚無補料紀錄</div>";
    return;
  }
  const rows = logs.map(log => {
    const time = (log.created_at || "").replace("T", " ").slice(0, 16);
    return `<div style="display:flex;justify-content:space-between;align-items:baseline;padding:4px 0;border-bottom:1px solid #f3f4f6;font-size:12px">
      <span style="color:#374151;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(log.detail || "")}</span>
      <span style="color:#9ca3af;white-space:nowrap;margin-left:8px">${esc(time)}</span>
    </div>`;
  }).join("");
  container.innerHTML = `
    <div style="margin-bottom:6px;font-size:13px;font-weight:600;color:#374151">補料紀錄</div>
    <div style="max-height:200px;overflow-y:auto">${rows}</div>`;
}

async function submitManualSupplement() {
  const partInput = document.getElementById("manual-supplement-part");
  const qtyInput = document.getElementById("manual-supplement-qty");
  const noteInput = document.getElementById("manual-supplement-note");
  const part = (partInput?.value || "").trim();
  const qty = Number(qtyInput?.value || 0);
  const note = (noteInput?.value || "").trim();

  if (!part) { showToast("請輸入料號"); partInput?.focus(); return; }
  if (!qty || qty <= 0) { showToast("補料數量必須大於 0"); qtyInput?.focus(); return; }

  const btn = document.getElementById("manual-supplement-submit");
  const originalText = btn?.textContent || "確認補料";
  if (btn) { btn.disabled = true; btn.textContent = "補料中..."; }

  try {
    const result = await apiPost("/api/schedule/supplement-part", {
      part_number: part,
      supplement_qty: qty,
      note,
    });
    showToast(
      `${result.part_number} 已補料 ${fmt(qty)}，庫存 ${fmt(result.stock_before)} → ${fmt(result.stock_after)}`,
      { tone: "success", duration: 4000 },
    );
    partInput.value = "";
    qtyInput.value = "";
    noteInput.value = "";
    partInput.focus();

    // refresh logs in modal
    try {
      const data = await apiJson("/api/schedule/supplement-logs?limit=30");
      renderSupplementLogs(data.logs || []);
    } catch (_) {}

    // refresh main data
    await refresh();
    if (_onRefreshMain) await _onRefreshMain();
  } catch (e) {
    showToast("補料失敗: " + e.message, { tone: "error" });
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = originalText; }
  }
}

// Safe overrides for draft workbench rendering.
async function handleBatchMerge() {
  await runBatchMergeWorkflow();
}

function countStoredDraftInputsForTargets(targets = []) {
  const seen = new Set();
  for (const row of targets || []) {
    const orderId = normalizeOrderId(row?.id);
    if (!Number.isInteger(orderId)) continue;
    const draft = _draftsByOrderId?.[orderId] || {};
    Object.keys(normalizeDecisionMap(draft.decisions || {})).forEach(part => seen.add(`${orderId}:${part}:d`));
    Object.keys(normalizeSupplementMap(draft.supplements || {})).forEach(part => seen.add(`${orderId}:${part}:s`));
    Object.keys(normalizeSupplementMap(_orderSupplementsByOrderId?.[orderId] || {})).forEach(part => seen.add(`${orderId}:${part}:os`));
  }
  return seen.size;
}

async function runBatchMergeWorkflow() {
  const { resetStored, commitAfterModal } = getBatchMergeOptions();
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

  const resultMap = new Map();
  _calcResults.forEach((result, index) => {
    const orderId = normalizeOrderId(_rows[index]?.id);
    if (Number.isInteger(orderId)) resultMap.set(orderId, result);
  });

  const missingBomTargets = targets.filter(row => {
    const result = resultMap.get(row.id);
    return !result || result.status === "no_bom";
  });
  const missingBomIds = new Set(missingBomTargets.map(row => row.id));
  const mergeableTargets = targets.filter(row => !missingBomIds.has(row.id));

  if (missingBomTargets.length && !mergeableTargets.length) {
    showToast(`以下訂單尚未上傳 BOM，這次不會建立副檔：${summarizeOrderAlertLabels(missingBomTargets)}`, {
      sticky: true,
      tone: "error",
    });
    return;
  }
  if (resetStored) {
    const storedCount = countStoredDraftInputsForTargets(mergeableTargets);
    if (storedCount > 0 && !confirm(`重算會清掉你手填的 ${storedCount} 筆補料，確定？`)) {
      return;
    }
  }

  const button = document.getElementById("btn-batch-merge");
  const originalText = button?.textContent || "批次 Merge";
  _batchMergeInFlight = true;
  try {
    if (button) {
      button.disabled = true;
      button.textContent = "建立中...";
    }
    const targetIds = mergeableTargets.map(row => row.id);
    const currentOrderIds = _rows.map(row => row.id).filter(Number.isInteger);

    // 先把目前畫面順序寫回後端，再依這個順序重建副檔
    const result = await withGlobalBusy(
      async () => {
        if (currentOrderIds.length) {
          await apiPost("/api/schedule/reorder", { order_ids: currentOrderIds });
        }
        return apiPost("/api/schedule/batch-merge", {
          order_ids: targetIds,
          reset_stored: resetStored,
        });
      },
      {
        title: "正在批次建立副檔",
        detail: `共 ${mergeableTargets.length} 筆訂單，系統正在整理 BOM 與補料資料，請稍候。`,
      },
    );

    // overlay 已關閉，背景刷新資料
    await refreshScheduleOnly();

    if (missingBomTargets.length) {
      showToast(`已建立 ${result.draft_count || 0} 份副檔；以下訂單尚未上傳 BOM，這次已跳過：${summarizeOrderAlertLabels(missingBomTargets)}`, {
        sticky: true,
        tone: "error",
      });
    } else {
      showToast(
        commitAfterModal
          ? `已建立 ${result.draft_count || 0} 份副檔，確認補料後會強制寫入主檔`
          : `已建立 ${result.draft_count || 0} 份副檔，請先確認補料`,
        { tone: "success" },
      );
    }
    try {
      _modalCommitAfterSave = commitAfterModal;
      _modalResetStored = resetStored;
      await openBatchMergeDraftModalStable(targetIds, targets);
    } catch (modalError) {
      console.error("[handleBatchMerge] showBatchMergeDraftModal failed:", modalError);
      showToast("算料工作區開啟失敗: " + modalError.message, { sticky: true, tone: "error" });
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
  const modalTargets = buildBatchMergeModalTargets(targetIds, fallbackTargets);
  await showBatchMergeDraftModal(modalTargets);
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
    await refreshScheduleOnly();
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
          await refreshScheduleOnly();
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
    renderShortagePanel(buildMainStockNegativeItems(), [], []);
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
    renderShortagePanel(shortages, csShortages, []);
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
