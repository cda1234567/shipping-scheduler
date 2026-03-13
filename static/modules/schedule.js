import { apiJson, apiFetch, apiPost, apiPatch, apiPut, showToast, esc, fmt } from "./api.js";
import { calculate } from "./calculator.js";
import { desktopDownload, showDownloadToast } from "./desktop_bridge.js";

// ── State ─────────────────────────────────────────────────────────────────────
let _rows = [];
let _bomData = {};
let _stock = {};
let _moq = {};
let _dispatchedConsumption = {};
let _calcResults = [];
let _decisions = {};
let _draftsByOrderId = {};
let _completedRows = [];
let _completedFolders = [];
let _onRefreshMain = null;
let _checkedIds = new Set();
let _modalProgressTimer = null;
let _modalProgressValue = 0;
let _completedFolderCollapsedState = loadCompletedFolderCollapsedState();
let _modalDraftId = null;
let _modalDraftReadOnly = false;

// ── Public ────────────────────────────────────────────────────────────────────
export async function initSchedule(onRefreshMain) {
  _onRefreshMain = onRefreshMain || null;
  document.getElementById("btn-auto-sort").addEventListener("click", handleAutoSort);
  document.getElementById("btn-save-order").addEventListener("click", handleSaveOrder);
  document.getElementById("btn-batch-merge")?.addEventListener("click", handleBatchMerge);
  document.getElementById("btn-batch-dispatch")?.addEventListener("click", handleBatchDispatch);
  document.getElementById("btn-create-folder")?.addEventListener("click", handleCreateFolder);
  await refresh();
}

export async function refresh() {
  await Promise.all([loadMainData(), loadScheduleRows(), loadBomData()]);
  recalculate();
  renderSchedule();
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

function normalizePartKey(partNumber) {
  return String(partNumber || "").trim().toUpperCase();
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

function normalizeDecisionMap(decisions = {}) {
  const normalized = {};
  for (const [part, decision] of Object.entries(decisions || {})) {
    const key = normalizePartKey(part);
    if (!key || !decision || decision === "None") continue;
    normalized[key] = decision;
  }
  return normalized;
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

async function persistDecisionsForOrders(decisions, orderIds) {
  const normalizedDecisions = {};
  for (const [part, decision] of Object.entries(decisions || {})) {
    const key = normalizePartKey(part);
    if (!key || !decision) continue;
    normalizedDecisions[key] = decision;
  }
  const targetIds = [...new Set((orderIds || []).filter(id => Number.isInteger(id)))];
  if (!targetIds.length || !Object.keys(normalizedDecisions).length) return;
  await Promise.all(targetIds.map(orderId =>
    apiPost(`/api/schedule/orders/${orderId}/decisions`, { decisions: normalizedDecisions })
  ));
}

// ── Data loading ──────────────────────────────────────────────────────────────
async function loadMainData() {
  try {
    const d = await apiJson("/api/main-file/data");
    _stock = d.stock || {};
    _moq = d.moq || {};
  } catch (_) { _stock = {}; _moq = {}; }
}

async function loadScheduleRows() {
  try {
    const d = await apiJson("/api/schedule/rows");
    _rows = d.rows || [];
    _dispatchedConsumption = d.dispatched_consumption || {};
    _decisions = normalizeDecisionMap(d.decisions || {});
    _draftsByOrderId = d.merge_drafts || {};
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
    ? calculate(checkedOrders, _bomData, _stock, _moq, _dispatchedConsumption)
    : [];
  // 建立以 order id 為 key 的結果 map
  const resultById = new Map();
  checkedOrders.forEach((r, i) => resultById.set(r.id, checkedResults[i]));
  // _calcResults 保持與 _rows 同長度，未勾選的為 null
  _calcResults = _rows.map(r => resultById.get(r.id) || null);
}

// ── Render schedule ───────────────────────────────────────────────────────────
function renderSchedule() {
  const container = document.getElementById("schedule-scroll");
  container.innerHTML = "";

  if (!_rows.length) {
    container.innerHTML = '<div class="empty-state">尚未上傳排程表，或排程為空</div>';
    renderShortagePanel([], []);
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
  renderShortagePanel(allShortages, allCSShortages);
}

function formatDraftTime(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.slice(5, 16).replace("T", " ");
}

function buildDraftPanelHtml(draft) {
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
    </div>
    ${draftHtml}`;
}

// ── Build single-row card ─────────────────────────────────────────────────────
function buildRowCard(r, resultMap) {
  const div = document.createElement("div");
  div.className = "po-group";
  div.dataset.orderId = r.id;

  const res = resultMap[r.id];
  const draft = _draftsByOrderId?.[r.id] || null;
  const badge = cardBadge(res, r.id);
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

  div.innerHTML = `
    <div class="po-group-header">
      <input type="checkbox" class="row-check" data-order-id="${r.id}" ${isChecked ? "checked" : ""}
             style="width:16px;height:16px;margin:0;cursor:pointer;accent-color:#34c759">
      <span class="drag-handle" title="拖曳調整順序">⠿</span>
      <span class="po-number model-editable" data-order-id="${r.id}" title="雙擊編輯機種名稱">${esc(r.model)}</span>
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
        <button class="btn-complete" data-order-id="${r.id}" title="標記已發料並 Merge">✓</button>
        <button class="btn-edit-date" data-order-id="${r.id}" title="改交期">📅</button>
        <button class="btn-cancel-order" data-order-id="${r.id}" title="取消訂單">✕</button>
      </div>
    </div>`;

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
    div.querySelector(".btn-draft-preview")?.addEventListener("click", () => {
      void showDraftModal(draft.id, { readOnly: true });
    });
    div.querySelector(".btn-draft-edit")?.addEventListener("click", () => {
      void showDraftModal(draft.id, { readOnly: false });
    });
    div.querySelector(".btn-draft-download")?.addEventListener("click", () => {
      void downloadDraft(draft.id);
    });
    div.querySelector(".btn-draft-delete")?.addEventListener("click", () => {
      void handleDeleteDraft(draft.id, r.model);
    });
  }

  return div;
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

async function handleBatchMerge() {
  const targets = _rows.filter(r => _checkedIds.has(r.id) && (r.status === "pending" || r.status === "merged"));
  if (!_checkedIds.size) { showToast("請先勾選要 merge 的訂單"); return; }
  if (!targets.length) { showToast("勾選的訂單中沒有可 merge 的"); return; }
  try {
    const result = await apiPost("/api/schedule/batch-merge", { order_ids: targets.map(r => r.id) });
    await refresh();
    const shouldWriteMain = confirm(`已完成 ${targets.length} 筆 merge。\n要接著寫入主檔嗎？`);
    if (shouldWriteMain) {
      await showWriteToMainModal(targets);
    } else {
      await showShortageModal(targets);
    }
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
  const supplements = _collectModalSupplements();
  const decisions = _collectModalDecisions();
  const response = await apiPut(`/api/schedule/drafts/${_modalDraftId}`, { decisions, supplements });
  if (!silent) showToast("副檔已更新");
  await refresh();
  return response?.draft || null;
}

async function showDraftModal(draftId, { readOnly = false } = {}) {
  const modal = document.getElementById("shortage-modal");
  const list = document.getElementById("modal-shortage-list");
  const footer = document.getElementById("modal-footer");
  const detail = await apiJson(`/api/schedule/drafts/${draftId}`);
  const draft = detail.draft || {};
  const order = detail.order || {};

  _modalDraftId = draftId;
  _modalDraftReadOnly = Boolean(readOnly);
  _modalTargets = order?.id ? [order] : [];
  _modalBomFiles = draft.files || [];
  _modalCarryOversByModel = {};
  _modalMode = readOnly ? "draft-preview" : "draft-edit";
  _modalPreviewShortages = draft.shortages || [];

  const fileSummary = (draft.files || []).length
    ? `<div class="merge-draft-inline-files">${draft.files.map(file => `<span class="merge-draft-file" title="${esc(file.filename || "")}">${esc(file.filename || "")}</span>`).join("")}</div>`
    : '<div class="merge-draft-empty-note">這份副檔目前沒有可用檔案。</div>';

  const grouped = {};
  (draft.shortages || []).forEach(item => {
    const modelKey = item._row_model || order.model || "副檔";
    if (!grouped[modelKey]) grouped[modelKey] = [];
    grouped[modelKey].push(item);
  });

  let html = `
    <div class="merge-draft-modal-head">
      <div class="merge-draft-modal-title">${esc(order.po_number || "")} ${esc(order.model || "")}</div>
      <div class="merge-draft-modal-meta">副檔 ${draft.files?.length || 0} 份，更新 ${esc(formatDraftTime(draft.updated_at) || "--")}</div>
      ${fileSummary}
    </div>

  const models = Object.keys(grouped);
  if (!models.length) {
    html += '<div class="merge-draft-empty-note">目前這份副檔沒有掛缺料，可直接下載或按勾提交。</div>';
  } else {
    models.sort((a, b) => compareText(a, b, "zh-Hant"));
    models.forEach(modelKey => {
      html += `<div style="margin:12px 0 8px;padding:6px 10px;background:#f3f4f6;border-radius:6px;font-weight:600;font-size:13px;color:#1f2937">${esc(modelKey)}</div>`;
      html += grouped[modelKey].map(item => modalShortageItem(item, false)).join("");
    });
  }

  list.innerHTML = html;
  list.querySelectorAll(".shortage-item").forEach(itemEl => {
    const input = itemEl.querySelector(".supplement-input");
    const checkbox = itemEl.querySelector(".shortage-mark");
    const part = normalizePartKey(input?.dataset.part || checkbox?.dataset.part);
    const decision = draft.decisions?.[part] || "None";
    if (checkbox) checkbox.checked = decision === "Shortage";
    if (input && checkbox?.checked) input.disabled = true;
  });
  list.querySelectorAll(".supplement-input, .shortage-mark").forEach(el => {
    el.disabled = readOnly;
  });
  bindMoqEditors(list);
  bindShortageMoqBadgeEditors(list);

  footer.innerHTML = readOnly
    ? `
      <button id="modal-download-bom" class="btn btn-primary btn-sm">下載副檔</button>
      <button id="modal-cancel" class="btn btn-secondary btn-sm">關閉</button>`
    : `
      <button id="modal-save-draft" class="btn btn-success btn-sm">儲存副檔</button>
      <button id="modal-download-bom" class="btn btn-primary btn-sm">下載副檔</button>
      <button id="modal-cancel" class="btn btn-secondary btn-sm">取消</button>`;

  document.getElementById("modal-save-draft")?.addEventListener("click", async () => {
    try {
      await saveCurrentDraftFromModal();
      closeShortageModal();
    } catch (error) {
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
  const targetIds = new Set(targets.map(t => t.id));

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
  const shortagesByModel = {};
  const csShortagesByModel = {};
  _calcResults.forEach((r, i) => {
    if (!r) return;
    if (!targetIds.has(_rows[i]?.id)) return;
    const model = _rows[i]?.model || "未知機種";
    const code = _rows[i]?.code || model;
    (r.shortages || []).forEach(s => {
      if (!shortagesByModel[model]) shortagesByModel[model] = [];
      shortagesByModel[model].push({ ...s, _row_code: code, _row_model: model });
    });
    (r.customer_material_shortages || []).forEach(s => {
      if (!csShortagesByModel[model]) csShortagesByModel[model] = [];
      csShortagesByModel[model].push({ ...s, _row_code: code, _row_model: model });
    });
  });

  // 組內按料號排序
  _modalCarryOversByModel = buildModalCarryOversByModel(targets);

  for (const items of Object.values(shortagesByModel))
    items.sort(compareShortageItems);
  for (const items of Object.values(csShortagesByModel))
    items.sort(compareShortageItems);

  const allModels = [...new Set([...Object.keys(csShortagesByModel), ...Object.keys(shortagesByModel)])]
    .sort((a, b) => compareText(a, b, "zh-Hant"));
  const hasAny = allModels.length > 0;

  let html = "";

  if (!hasAny) {
    html += `<div style="text-align:center;padding:24px;color:#16a34a;font-weight:600">
      全部 OK，無缺料！可直接扣帳。</div>`;
  } else {
    for (const model of allModels) {
      html += `<div style="margin:12px 0 8px;padding:6px 10px;background:#f3f4f6;border-radius:6px;font-weight:600;font-size:13px;color:#1f2937">${esc(model)}</div>`;
      const csItems = csShortagesByModel[model] || [];
      const items = shortagesByModel[model] || [];
      if (csItems.length) {
        html += '<div style="margin-bottom:8px"><h4 style="font-size:12px;color:#ca8a04;margin:4px 0">客供料</h4>';
        html += csItems.map(s => modalShortageItem(s, true)).join("");
        html += '</div>';
      }
      if (items.length) {
        html += '<h4 style="font-size:12px;color:#dc2626;margin:4px 0">採購缺料</h4>';
        html += items.map(s => modalShortageItem(s, false)).join("");
      }
    }
  }

  list.innerHTML = html;

  // 綁定缺料 checkbox — 勾選時停用輸入框
  list.querySelectorAll(".shortage-mark").forEach(chk => {
    chk.addEventListener("change", () => {
      const input = chk.closest(".shortage-item").querySelector(".supplement-input");
      if (input) {
        input.disabled = chk.checked;
        if (chk.checked) input.value = "0";
      }
    });
  });

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

  const shortagesByModel = {};
  _modalPreviewShortages.forEach(item => {
    const model = item.model || item.bom_model || "未指定機種";
    if (!shortagesByModel[model]) shortagesByModel[model] = [];
    shortagesByModel[model].push({
      ...item,
      _row_code: item.batch_code || item.model || model,
      _row_model: model,
    });
  });
  for (const items of Object.values(shortagesByModel)) items.sort(compareShortageItems);

  const allModels = Object.keys(shortagesByModel).sort((a, b) => compareText(a, b, "zh-Hant"));
  let html = "";
  if (!allModels.length) {
    html = `<div style="text-align:center;padding:24px;color:#16a34a;font-weight:600">
      模擬寫入主檔後沒有剩餘缺料，可以直接寫入主檔。</div>`;
  } else {
    for (const model of allModels) {
      html += `<div style="margin:12px 0 8px;padding:6px 10px;background:#f3f4f6;border-radius:6px;font-weight:600;font-size:13px;color:#1f2937">${esc(model)}</div>`;
      html += '<h4 style="font-size:12px;color:#dc2626;margin:4px 0">寫入主檔後仍缺料</h4>';
      html += (shortagesByModel[model] || []).map(item => modalShortageItem(item, false)).join("");
    }
  }

  list.innerHTML = html;
  list.querySelectorAll(".shortage-mark").forEach(chk => {
    chk.addEventListener("change", () => {
      const input = chk.closest(".shortage-item")?.querySelector(".supplement-input");
      if (!input) return;
      input.disabled = chk.checked;
      if (chk.checked) input.value = "0";
    });
  });

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
    <button id="modal-download-bom" class="btn btn-primary btn-sm">下載 BOM</button>
    <button id="modal-cancel" class="btn btn-secondary btn-sm">取消</button>`;
  document.getElementById("modal-write-main").onclick = handleModalWriteMain;
  document.getElementById("modal-download-bom").onclick = handleModalDownloadBom;
  document.getElementById("modal-cancel").onclick = closeShortageModal;
  document.getElementById("modal-close").onclick = closeShortageModal;
  modal.style.display = "flex";
}

function modalShortageItem(s, isCS) {
  const codeTag = s._row_code ? `<span class="tag tag-pcb" style="font-size:10px;padding:1px 6px;margin-left:4px">${esc(s._row_code)}</span>` : "";
  const csTag = isCS ? '<span class="tag tag-cs">客供</span>' : "";
  const defaultQty = roundShortageUiValue(s.default_supplement ?? s.supplement_qty ?? s.suggested_qty || s.shortage_amount || 0);
  const shortageAmount = roundShortageUiValue(s.shortage_amount);
  const currentStock = roundShortageUiValue(s.current_stock);
  const neededQty = roundShortageUiValue(s.needed);
  const shortageChecked = s.decision === "Shortage";
  s = { ...s, shortage_amount: shortageAmount, current_stock: currentStock, needed: neededQty };

  return `<div class="shortage-item ${isCS ? "cs-item" : ""}" style="margin-bottom:8px">
    <div style="display:flex;align-items:center;gap:6px;font-weight:600;font-size:13px">${s.part_number}${codeTag}${csTag}</div>
    <div style="font-size:11px;color:#6b7280">${s.description || "—"}</div>
    <div style="font-size:12px;display:flex;gap:10px;margin:4px 0">
      <span style="color:#dc2626">缺 ${fmt(s.shortage_amount)}</span>
      <span style="color:#16a34a">庫存 ${fmt(s.current_stock)}</span>
      <span>需 ${fmt(s.needed)}</span>
      ${moqBadgeHtml(s)}
      ${moqEditTriggerHtml(s)}
    </div>
    ${missingMoqEditorHtml(s)}
    ${isCS ? '<div style="font-size:11px;color:#ca8a04">請通知客戶提供此料</div>' : `
    <div style="display:flex;align-items:center;gap:8px;margin-top:4px">
      <label style="font-size:12px;color:#374151;white-space:nowrap">補料:</label>
      <input type="number" class="supplement-input" data-part="${s.part_number}" value="${defaultQty}" min="0" ${shortageChecked ? "disabled" : ""}
             style="width:80px;padding:2px 6px;border:1px solid #d1d5db;border-radius:4px;font-size:12px;text-align:right">
      <label style="font-size:12px;display:flex;align-items:center;gap:4px;color:#dc2626;cursor:pointer;white-space:nowrap">
        <input type="checkbox" class="shortage-mark" data-part="${s.part_number}"> 缺料
      </label>
    </div>`}
  </div>`;
}

function closeShortageModal() {
  stopModalProgressAnimation();
  setModalDownloadProgress(false, "", "", 0);
  document.getElementById("shortage-modal").style.display = "none";
  _modalTargets = [];
  _modalBomFiles = [];
  _modalCarryOversByModel = {};
  _modalMode = "download";
  _modalPreviewShortages = [];
  _modalDraftId = null;
  _modalDraftReadOnly = false;
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
  if (hasMoqValue(shortage)) {
    return `<span class="blue">建議補 ${fmt(suggested)}（MOQ ${fmt(shortage.moq)}）</span>`;
  }
  return `<span class="amber">建議補 ${fmt(suggested)}（未寫 MOQ）</span>`;
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
    showToast(`已依副檔寫入主檔，merge ${result.merged_parts} 筆`);
    _checkedIds.delete(result.order_id);
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
  } catch (error) {
    showToast("副檔提交失敗: " + error.message);
  }
}

async function downloadDraft(draftId) {
  try {
    const result = await desktopDownload({ path: `/api/schedule/drafts/${draftId}/download` });
    showDownloadToast(result, "副檔");
  } catch (error) {
    showToast("副檔下載失敗: " + error.message);
  }
}

async function handleDeleteDraft(draftId, model) {
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

function buildDraftPanelHtml(draft) {
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

async function handleBatchDispatch() {
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

function _collectModalDecisions() {
  const list = document.getElementById("modal-shortage-list");
  if (!list) return {};
  const decisions = {};

  list.querySelectorAll(".supplement-input").forEach(input => {
    const part = normalizePartKey(input.dataset.part);
    const qty = parseFloat(input.value) || 0;
    const isShortage = input.closest(".shortage-item")?.querySelector(".shortage-mark")?.checked;

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
    const part = normalizePartKey(input.dataset.part);
    const qty = parseFloat(input.value) || 0;
    if (qty > 0) supplements[part] = qty;
  });
  return supplements;
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

function setModalDownloadProgress(active, statusText = "", detailText = "", percent = null) {
  const progress = document.getElementById("modal-download-progress");
  const status = document.getElementById("modal-download-status");
  const detail = document.getElementById("modal-download-detail");
  const downloadBtn = document.getElementById("modal-download-bom");
  const writeBtn = document.getElementById("modal-write-main");
  const cancelBtn = document.getElementById("modal-cancel");
  const closeBtn = document.getElementById("modal-close");

  if (progress) progress.style.display = active ? "block" : "none";
  if (status && statusText) status.textContent = statusText;
  if (detail && detailText) detail.textContent = detailText;
  if (percent != null) setModalProgressPercent(percent);

  if (downloadBtn) {
    downloadBtn.disabled = active;
    downloadBtn.textContent = active ? "下載中..." : "下載 BOM";
    downloadBtn.textContent = active ? "下載中..." : "確認補料並下載 BOM";
  }
  if (writeBtn) {
    writeBtn.disabled = active;
    writeBtn.textContent = active ? "寫入中..." : "寫入主檔";
  }
  if (cancelBtn) cancelBtn.disabled = active;
  if (closeBtn) closeBtn.disabled = active;
  if (!active) {
    stopModalProgressAnimation();
    setModalProgressPercent(percent ?? 0);
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

  try {
    setModalDownloadProgress(true, "正在保存缺料決策...", "先保存這次要寫入主檔的缺料與補料內容。", 10);
    startModalProgressAnimation(35, 140);
    await persistDecisionsForOrders(modalDecisions, targetOrderIds);
    Object.entries(modalDecisions).forEach(([part, decision]) => {
      setLocalDecision(part, decision);
    });

    setModalDownloadProgress(true, "正在寫入主檔...", "系統會依目前選取訂單順序，將內容寫入 live 主檔。", 46);
    startModalProgressAnimation(92, 220);
    const result = await apiPost("/api/schedule/batch-dispatch", {
      order_ids: targetOrderIds,
      decisions: modalDecisions,
      supplements,
    });

    _modalTargets.forEach(item => _checkedIds.delete(item.id));
    setModalDownloadProgress(true, "寫入完成", "主檔與已發料清單正在刷新。", 100);
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
    await new Promise(resolve => setTimeout(resolve, 200));
    closeShortageModal();
    showToast(`已寫入主檔 ${result.count} 筆，merge ${result.merged_parts} 個料件`);
  } catch (error) {
    showToast("寫入主檔失敗: " + error.message);
    setModalDownloadProgress(false, "", "", 0);
  }
}

async function handleModalDownloadBom() {
  if (!_modalBomFiles.length) { showToast("找不到對應的 BOM 檔案"); return; }

  const supplements = _collectModalSupplements();
  const modalDecisions = _collectModalDecisions();
  const targetOrderIds = _modalTargets.map(target => target.id).filter(id => Number.isInteger(id));
  const headerOverrides = buildModalHeaderOverrides();

  try {
    setModalDownloadProgress(true, "正在保存補料決策...", "會把這次 merge 的補料內容記進系統。", 12);
    startModalProgressAnimation(32, 140);
    await persistDecisionsForOrders(modalDecisions, targetOrderIds);
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
    showToast(`已反悔 ${result.count} 筆訂單\n主檔已同步還原`);
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
  } catch (error) {
    showToast("反悔失敗：" + error.message);
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
}

// ── SortableJS ────────────────────────────────────────────────────────────────
function buildRowCard(r, resultMap) {
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
        <button class="btn-complete" data-order-id="${r.id}" title="${esc(completeTitle)}">&#10003;</button>
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
    div.querySelector(".btn-draft-preview")?.addEventListener("click", () => {
      void showDraftModal(draft.id, { readOnly: true });
    });
    div.querySelector(".btn-draft-edit")?.addEventListener("click", () => {
      void showDraftModal(draft.id, { readOnly: false });
    });
    div.querySelector(".btn-draft-download")?.addEventListener("click", () => {
      void downloadDraft(draft.id);
    });
    div.querySelector(".btn-draft-delete")?.addEventListener("click", () => {
      void handleDeleteDraft(draft.id, r.model);
    });
  }

  return div;
}

async function handleBatchMerge() {
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
  if (!_checkedIds.size) { showToast("請先勾選要發料的訂單"); return; }
  if (!targets.length) { showToast("目前勾選的訂單沒有可發料項目"); return; }

  const preview = targets.slice(0, 6).map(item => `${item.po_number} ${item.model}`).join("\n");
  const extra = targets.length > 6 ? `\n... 另 ${targets.length - 6} 筆` : "";
  if (!confirm(`確認要依副檔批次寫入主檔嗎？\n${preview}${extra}`)) return;

  const button = document.getElementById("btn-batch-dispatch");
  const originalText = button?.textContent || "批次發料";
  try {
    if (button) {
      button.disabled = true;
      button.textContent = "發料中...";
    }
    const result = await apiPost("/api/schedule/batch-dispatch", { order_ids: targets.map(item => item.id) });
    targets.forEach(item => _checkedIds.delete(item.id));
    showToast(`已批次寫入 ${result.count} 筆訂單，merge ${result.merged_parts} 筆`);
    await Promise.all([refresh(), refreshCompleted()]);
    if (_onRefreshMain) await _onRefreshMain();
  } catch (error) {
    showToast("批次發料失敗: " + error.message);
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalText;
    }
  }
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

  document.querySelectorAll("#schedule-scroll .po-group[data-order-id]").forEach(div => {
    const orderId = parseInt(div.dataset.orderId);
    const res = resultMap[orderId];
    const badge = div.querySelector(".po-status-badge");
    if (badge) {
      const b = cardBadge(res, orderId);
      badge.className = `po-status-badge ${b.cls}`;
      badge.textContent = b.text;
    }
  });

  const allShortages = [];
  const allCSShortages = [];
  _calcResults.forEach((r, i) => {
    if (!r) return;
    const code = _rows[i]?.code || "";
    const model = _rows[i]?.model || "";
    (r.shortages || []).forEach(s => allShortages.push({ ...s, _row_code: code, _row_model: model }));
    (r.customer_material_shortages || []).forEach(s => allCSShortages.push({ ...s, _row_code: code, _row_model: model }));
  });
  renderShortagePanel(allShortages, allCSShortages);
}

// ── Shortage panel ────────────────────────────────────────────────────────────
function compareText(a, b, locale = "en") {
  return String(a || "").localeCompare(String(b || ""), locale, { numeric: true, sensitivity: "base" });
}

function compareShortageItems(a, b) {
  const modelCmp = compareText(a._row_model, b._row_model, "zh-Hant");
  if (modelCmp !== 0) return modelCmp;
  const partCmp = compareText(a.part_number, b.part_number);
  if (partCmp !== 0) return partCmp;
  return compareText(a._row_code, b._row_code);
}

function renderShortageGroupHtml(items, isCS) {
  const sorted = [...items].sort(compareShortageItems);
  let html = "";
  let currentModel = null;
  for (const item of sorted) {
    const model = item._row_model || "未指定機種";
    if (model !== currentModel) {
      currentModel = model;
      html += `<div style="font-size:11px;font-weight:600;color:#6b7280;margin:8px 0 4px">${esc(model)}</div>`;
    }
    html += shortageItemHtml(item, isCS);
  }
  return html;
}

function renderShortagePanel(shortages, csShortages = []) {
  const scroll = document.getElementById("right-scroll");
  const badge = document.getElementById("shortage-count");
  const totalCount = shortages.length + csShortages.length;

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

  scroll.innerHTML = html;

  scroll.querySelectorAll(".dec-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      const part = normalizePartKey(btn.dataset.part);
      const dec = btn.dataset.dec;
      const prev = _decisions[part] || "None";
      const next = prev === dec ? "None" : dec;
      setLocalDecision(part, next);
      renderShortagePanel(shortages, csShortages);
      try {
        await persistDecisionsForOrders({ [part]: next }, getAffectedOrderIdsForPart(part));
      } catch (e) {
        setLocalDecision(part, prev);
        renderShortagePanel(shortages, csShortages);
        showToast("決策儲存失敗：" + e.message);
      }
    });
  });
  bindMoqEditors(scroll);
  bindShortageMoqBadgeEditors(scroll);
}

function shortageItemHtml(s, isCS) {
  const shortageAmount = roundShortageUiValue(s.shortage_amount);
  const currentStock = roundShortageUiValue(s.current_stock);
  const neededQty = roundShortageUiValue(s.needed);
  s = { ...s, shortage_amount: shortageAmount, current_stock: currentStock, needed: neededQty };
  const dec = _decisions[normalizePartKey(s.part_number)] || "None";
  const codeTag = s._row_code
    ? `<span class="tag tag-pcb" style="font-size:10px;padding:1px 6px;margin-left:6px">${esc(s._row_code)}</span>`
    : "";
  const csTag = isCS ? '<span class="tag tag-cs">客供</span>' : "";

  return `<div class="shortage-item ${isCS ? "cs-item" : ""}">
    <div class="part">${s.part_number}${codeTag}${csTag}</div>
    <div class="desc">${s.description || "—"}</div>
    <div class="amounts">
      <span class="red">缺 ${fmt(s.shortage_amount)}</span>
      <span class="green">庫存 ${fmt(s.current_stock)}</span>
      <span>需 ${fmt(s.needed)}</span>
      ${suggestedQtyHtml(s)}
    </div>
    ${missingMoqEditorHtml(s)}
    ${isCS ? '<div style="font-size:11px;color:#ca8a04;margin-top:4px">請通知客戶提供此料</div>' : `
    <div class="decision-btns">
      <button class="dec-btn ${dec === "CreateRequirement" ? "active-create" : ""}" data-dec="CreateRequirement" data-part="${s.part_number}">需採購</button>
      <button class="dec-btn ${dec === "MarkHasPO" ? "active-has-po" : ""}" data-dec="MarkHasPO" data-part="${s.part_number}">已有PO</button>
      <button class="dec-btn ${dec === "IgnoreOnce" ? "active-ignore" : ""}" data-dec="IgnoreOnce" data-part="${s.part_number}">忽略</button>
      <button class="dec-btn ${dec === "Shortage" ? "active-shortage" : ""}" data-dec="Shortage" data-part="${s.part_number}">缺料</button>
    </div>`}
  </div>`;
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

// Safe overrides for draft workbench rendering.
function buildDraftPanelHtml(draft) {
  const files = Array.isArray(draft?.files) ? draft.files : [];
  const shortages = Array.isArray(draft?.shortages) ? draft.shortages : [];
  const updatedAt = formatDraftTime(draft?.updated_at) || "--";
  const fileHtml = files.length
    ? files.map(file => `<span class="merge-draft-file" title="${esc(file.filename || "")}">${esc(file.filename || "")}</span>`).join("")
    : '<span class="merge-draft-file merge-draft-file-empty">副檔尚未生成</span>';

  return `
    <div class="merge-draft-panel">
      <div class="merge-draft-summary">
        <span class="merge-draft-pill">副檔 ${files.length} 份</span>
        <span class="merge-draft-meta">缺料 ${shortages.length} 筆</span>
        <span class="merge-draft-meta">更新 ${esc(updatedAt)}</span>
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

function buildRowCard(r, resultMap) {
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
        <button class="btn-complete" data-order-id="${r.id}" title="${esc(completeTitle)}">&#10003;</button>
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
    div.querySelector(".btn-draft-preview")?.addEventListener("click", () => {
      void showDraftModal(draft.id, { readOnly: true });
    });
    div.querySelector(".btn-draft-edit")?.addEventListener("click", () => {
      void showDraftModal(draft.id, { readOnly: false });
    });
    div.querySelector(".btn-draft-download")?.addEventListener("click", () => {
      void downloadDraft(draft.id);
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
    renderShortagePanel([], []);
    return;
  }

  try {
    const resultMap = {};
    _calcResults.forEach((r, i) => { resultMap[_rows[i]?.id] = r; });

    _rows.forEach(row => {
      container.appendChild(buildRowCard(row, resultMap));
    });

    initSortable(container);

    const allShortages = [];
    const allCSShortages = [];
    _calcResults.forEach((r, i) => {
      if (!r) return;
      const rowCode = _rows[i]?.code || "";
      const rowModel = _rows[i]?.model || "";
      (r.shortages || []).forEach(s => allShortages.push({ ...s, _row_code: rowCode, _row_model: rowModel }));
      (r.customer_material_shortages || []).forEach(s => allCSShortages.push({ ...s, _row_code: rowCode, _row_model: rowModel }));
    });
    renderShortagePanel(allShortages, allCSShortages);
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
