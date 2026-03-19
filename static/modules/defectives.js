import { apiJson, apiFetch, apiPost, showToast, esc, fmt } from "./api.js";

let _batches = [];
let _overrunPreview = null;
let _overrunPreviewSignature = "";
let _resolutionContext = null;
const _collapsed = new Set();
const _skippedMap = new Map();  // batch_id → [part_number, ...]

export async function initDefectives() {
  document.getElementById("btn-import-defective")?.addEventListener("click", handleImport);
  document.getElementById("btn-overrun-import")?.addEventListener("click", handleOverrunImportPicker);
  document.getElementById("btn-overrun-preview")?.addEventListener("click", () => void handleOverrunPreview());
  document.getElementById("btn-overrun-submit")?.addEventListener("click", () => void handleOverrunSubmit());

  document.getElementById("overrun-resolution-close")?.addEventListener("click", closeResolutionModal);
  document.getElementById("overrun-resolution-cancel")?.addEventListener("click", closeResolutionModal);
  document.getElementById("overrun-resolution-confirm")?.addEventListener("click", () => void handleResolutionConfirm());
  document.getElementById("overrun-resolution-list")?.addEventListener("change", handleResolutionListChange);
  document.getElementById("overrun-resolution-list")?.addEventListener("click", handleResolutionListClick);

  [
    "overrun-model",
    "overrun-extra-pcs",
    "overrun-reason",
    "overrun-note",
    "overrun-reported-by",
  ].forEach(id => {
    document.getElementById(id)?.addEventListener("input", invalidateOverrunPreview);
  });

  await loadOverrunModelOptions();
  renderOverrunPreview();
}

export async function refreshDefectives() {
  try {
    const d = await apiJson("/api/defectives/batches");
    _batches = d.batches || [];
  } catch (_) {
    _batches = [];
  }
  renderBatches();
}

// ── Excel 匯入 ──────────────────────────────────────────────────────────────

function handleImport() {
  const input = document.getElementById("defective-import-input");
  if (!input) return;
  input.value = "";
  input.onchange = async () => {
    const file = input.files?.[0];
    if (!file) return;
    await doImport(file);
  };
  input.click();
}

async function doImport(file, batchId) {
  const btn = document.getElementById("btn-import-defective");
  if (btn) {
    btn.disabled = true;
    btn.textContent = "預覽中...";
  }
  showToast(`正在讀取 ${file.name}，先整理扣帳明細...`, { duration: 30000 });

  const formData = new FormData();
  formData.append("file", file);
  const url = batchId ? `/api/defectives/batches/${batchId}/add-preview` : "/api/defectives/import-preview";

  try {
    const resp = await apiFetch(url, { method: "POST", body: formData });
    const preview = await resp.json();
    _resolutionContext = {
      kind: "defective",
      preview,
      batchId: batchId || Number(preview.batch_id || 0) || null,
      sourceFilename: preview.source_filename || file.name,
    };
    openResolutionModal(_resolutionContext);
    if (Number(preview.missing_count || 0) > 0) {
      showToast(`這份不良品有 ${preview.missing_count} 筆料號抓不到，請先確認後再扣帳`, {
        tone: "error",
        duration: 4000,
      });
    } else {
      showToast(`已載入 ${fmt(preview.deducted_count || 0)} 筆不良品，請確認後再扣帳`, {
        tone: "success",
        duration: 3000,
      });
    }
  } catch (e) {
    _resolutionContext = null;
    showToast("不良品預覽失敗：" + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = "匯入副檔扣帳";
    }
  }
}

// ── 加工多打明細匯入 ─────────────────────────────────────────────────────────

function handleOverrunImportPicker() {
  const input = document.getElementById("overrun-import-input");
  if (!input) return;
  input.value = "";
  input.onchange = async () => {
    const file = input.files?.[0];
    if (!file) return;
    await doOverrunImport(file);
  };
  input.click();
}

async function doOverrunImport(file) {
  const formData = new FormData();
  formData.append("file", file);

  const button = document.getElementById("btn-overrun-import");
  if (button) {
    button.disabled = true;
    button.textContent = "預覽中...";
  }
  showToast(`正在讀取 ${file.name}，先檢查抓不到的料號...`, { duration: 30000 });

  try {
    const resp = await apiFetch("/api/defectives/overrun/import-preview", { method: "POST", body: formData });
    const preview = await resp.json();
    _resolutionContext = {
      kind: "overrun",
      preview,
      batchId: null,
      sourceFilename: preview.source_filename || file.name,
    };
    openResolutionModal(_resolutionContext);

    if (Number(preview.missing_count || 0) > 0) {
      showToast(`這份明細有 ${preview.missing_count} 筆料號抓不到，請先確認後再扣帳`, {
        tone: "error",
        duration: 4000,
      });
    } else {
      showToast(`已載入 ${fmt(preview.deducted_count || 0)} 筆多打明細，請確認後再扣帳`, {
        tone: "success",
        duration: 3000,
      });
    }
  } catch (error) {
    _resolutionContext = null;
    showToast("匯入多打明細失敗：" + error.message);
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = "匯入多打明細";
    }
  }
}

function buildImportConfirmItems(preview, resolutionsByRow = new Map()) {
  return (preview?.items || []).map(item => {
    const rowKey = String(item.source_row || "");
    const resolution = resolutionsByRow.get(rowKey);
    if (item.found_in_main) {
      return {
        source_row: Number(item.source_row || 0),
        part_number: item.part_number || "",
        defective_qty: Number(item.defective_qty || 0),
        description: item.description || "",
        action: "deduct",
        target_part_number: "",
      };
    }
    return {
      source_row: Number(item.source_row || 0),
      part_number: item.part_number || "",
      defective_qty: Number(item.defective_qty || 0),
      description: item.description || "",
      action: resolution?.action || "skip",
      target_part_number: resolution?.target_part_number || "",
    };
  });
}

async function submitImportConfirm(context, items) {
  const preview = context?.preview || {};
  const isDefective = context?.kind === "defective";
  const button = document.getElementById(isDefective ? "btn-import-defective" : "btn-overrun-import");
  const modalConfirm = document.getElementById("overrun-resolution-confirm");

  if (button) button.disabled = true;
  if (modalConfirm) {
    modalConfirm.disabled = true;
    modalConfirm.textContent = "扣帳中...";
  }

  try {
    const payload = isDefective
      ? {
          batch_id: context.batchId || null,
          source_filename: context.sourceFilename || preview.source_filename || "",
          items,
        }
      : {
          source_filename: preview.source_filename || "",
          title: preview.title || "",
          mo_info: preview.mo_info || "",
          items,
        };
    const url = isDefective ? "/api/defectives/import-confirm" : "/api/defectives/overrun/import-confirm";
    const result = await apiPost(url, payload);
    const skipped = result.skipped_parts || [];
    if (skipped.length) {
      _skippedMap.set(result.batch_id, skipped);
    } else {
      _skippedMap.delete(result.batch_id);
    }
    closeResolutionModal({ preserveContext: false });
    const actionText = isDefective
      ? (context.batchId ? "已追加不良品扣帳" : "已完成不良品扣帳")
      : "已完成多打明細扣帳";
    showToast(`${actionText} ${result.deducted_count} 筆`
      + (result.replaced_count ? `，改正 ${result.replaced_count} 筆` : "")
      + (result.skipped_count ? `，不扣 ${result.skipped_count} 筆` : ""), {
      tone: "success",
      duration: 3500,
    });
    await refreshDefectives();
  } catch (error) {
    showToast("確認扣帳失敗：" + error.message, { tone: "error" });
  } finally {
    if (button) button.disabled = false;
    if (modalConfirm) {
      modalConfirm.disabled = false;
      modalConfirm.textContent = isDefective && context?.batchId ? "確認後追加扣帳" : "確認後扣帳";
    }
  }
}

function openResolutionModal(context) {
  const modal = document.getElementById("overrun-resolution-modal");
  if (!modal) return;
  renderResolutionModal(context);
  modal.style.display = "flex";
  modal.setAttribute("aria-hidden", "false");
}

function closeResolutionModal(options = {}) {
  const preserveContext = Boolean(options.preserveContext);
  const modal = document.getElementById("overrun-resolution-modal");
  if (modal) {
    modal.style.display = "none";
    modal.setAttribute("aria-hidden", "true");
  }
  if (!preserveContext) {
    _resolutionContext = null;
  }
}

function renderResolutionModal(context) {
  const preview = context?.preview || {};
  const summary = document.getElementById("overrun-resolution-summary");
  const list = document.getElementById("overrun-resolution-list");
  const title = document.getElementById("resolution-modal-title");
  const subtitle = document.getElementById("resolution-modal-subtitle");
  const confirm = document.getElementById("overrun-resolution-confirm");
  if (!summary || !list || !title || !subtitle || !confirm) return;

  const isDefective = context?.kind === "defective";
  const missingCount = Number(preview.missing_count || 0);
  const foundCount = Number(preview.item_count || 0) - missingCount;
  const missingItems = preview.missing_items || [];
  const targetBatch = isDefective && context?.batchId
    ? _batches.find(item => Number(item.id || 0) === Number(context.batchId || 0))
    : null;

  title.textContent = isDefective ? "不良品扣帳確認" : "加工多打料號確認";
  subtitle.textContent = missingCount > 0
    ? "抓不到的料號不能直接扣，請選擇不扣，或改成主檔裡的正確料號後再扣。"
    : "確認後才會真正寫入主檔，請先檢查這次要扣的明細。";
  confirm.textContent = isDefective && context?.batchId ? "確認後追加扣帳" : "確認後扣帳";

  summary.innerHTML = [
    isDefective && targetBatch ? `追加批次：${esc(targetBatch.filename || `#${context.batchId}`)}` : "",
    `來源檔案：${esc(preview.source_filename || "")}`,
    !isDefective && preview.title ? `明細標題：${esc(preview.title)}` : "",
    !isDefective && preview.mo_info ? `M/O：${esc(preview.mo_info)}` : "",
    `已抓到 ${fmt(foundCount)} 筆，可直接扣帳 ${fmt(preview.deducted_count || 0)} 筆；抓不到 ${fmt(missingCount)} 筆需要你確認。`,
  ].filter(Boolean).join("<br>");

  if (missingItems.length) {
    list.innerHTML = missingItems.map(item => {
      const suggestions = item.suggestions || [];
      return `<div class="defective-resolution-item" data-source-row="${item.source_row}">
        <div class="defective-resolution-top">
          <span class="defective-resolution-row-label">第 ${fmt(item.source_row || 0)} 列</span>
          <span class="defective-resolution-part">${esc(item.part_number || "")}</span>
          <span class="defective-resolution-qty">${fmt(item.defective_qty || 0)} pcs</span>
        </div>
        <div class="defective-resolution-actions">
          <select class="js-resolution-action">
            <option value="skip" selected>不扣</option>
            <option value="replace">改正料號後扣</option>
          </select>
          <input class="js-resolution-target" type="text" placeholder="輸入正確料號" disabled>
        </div>
        ${suggestions.length ? `
          <div class="defective-resolution-suggestions">
            ${suggestions.map(suggestion =>
              `<button class="defective-resolution-suggestion" type="button" data-part="${esc(suggestion.part_number || "")}">
                ${esc(suggestion.part_number || "")} (${fmt(suggestion.stock_qty || 0)})
              </button>`
            ).join("")}
          </div>
        ` : ""}
        <div class="defective-resolution-help">找不到原料號時，不能直接扣原料號；要嘛不扣，要嘛改成主檔裡存在的正確料號。</div>
      </div>`;
    }).join("");
    return;
  }

  const rows = preview.results || [];
  if (!rows.length) {
    list.innerHTML = '<div class="no-shortage-msg">這次沒有任何可扣帳的料號。</div>';
    return;
  }

  list.innerHTML = `<div class="defective-overrun-table-wrap"><table class="analytics-table defective-batch-table"><thead><tr>
    <th>料號</th><th>說明</th><th>扣帳數量</th><th>扣帳前庫存</th><th>扣帳後庫存</th>
  </tr></thead><tbody>${
    rows.map(row => {
      const stockClass = row.stock_after < 0 ? ' class="stock-negative"' : "";
      return `<tr>
        <td>${esc(row.part_number || "")}</td>
        <td>${esc(row.description || "")}</td>
        <td>${fmt(row.defective_qty || 0)}</td>
        <td>${fmt(row.stock_before || 0)}</td>
        <td${stockClass}>${fmt(row.stock_after || 0)}</td>
      </tr>`;
    }).join("")
  }</tbody></table></div>`;
}

function handleResolutionListChange(event) {
  const row = event.target.closest(".defective-resolution-item");
  if (!row) return;

  const actionSelect = row.querySelector(".js-resolution-action");
  const targetInput = row.querySelector(".js-resolution-target");
  if (!actionSelect || !targetInput) return;

  const shouldEnable = actionSelect.value === "replace";
  targetInput.disabled = !shouldEnable;
  if (!shouldEnable) {
    targetInput.value = "";
  }
}

function handleResolutionListClick(event) {
  const button = event.target.closest(".defective-resolution-suggestion");
  if (!button) return;

  const row = button.closest(".defective-resolution-item");
  if (!row) return;

  const actionSelect = row.querySelector(".js-resolution-action");
  const targetInput = row.querySelector(".js-resolution-target");
  if (!actionSelect || !targetInput) return;

  actionSelect.value = "replace";
  targetInput.disabled = false;
  targetInput.value = button.dataset.part || "";
}

function collectResolutionChoices() {
  const resolutions = new Map();
  document.querySelectorAll(".defective-resolution-item").forEach(row => {
    const sourceRow = String(row.dataset.sourceRow || "");
    const action = row.querySelector(".js-resolution-action")?.value || "skip";
    const target = row.querySelector(".js-resolution-target")?.value?.trim() || "";
    resolutions.set(sourceRow, {
      action,
      target_part_number: target,
    });
  });
  return resolutions;
}

async function handleResolutionConfirm() {
  if (!_resolutionContext) {
    closeResolutionModal();
    return;
  }

  const resolutions = collectResolutionChoices();
  for (const [rowKey, resolution] of resolutions.entries()) {
    if (resolution.action === "replace" && !resolution.target_part_number) {
      showToast(`第 ${rowKey} 列請輸入正確料號，或改成不扣`);
      return;
    }
  }

  const items = buildImportConfirmItems(_resolutionContext.preview, resolutions);
  await submitImportConfirm(_resolutionContext, items);
}

// ── 加工多打 ────────────────────────────────────────────────────────────────

async function loadOverrunModelOptions() {
  const datalist = document.getElementById("defective-overrun-models");
  if (!datalist) return;

  try {
    const data = await apiJson("/api/bom/list");
    const models = new Set();
    for (const group of data.groups || []) {
      for (const part of String(group.model || "").split(",")) {
        const value = String(part || "").trim();
        if (value) models.add(value);
      }
      for (const item of group.items || []) {
        const model = String(item.model || "").trim();
        if (model) models.add(model);
      }
    }
    datalist.innerHTML = Array.from(models).sort((a, b) => a.localeCompare(b)).map(model =>
      `<option value="${esc(model)}"></option>`
    ).join("");
  } catch (_) {
    datalist.innerHTML = "";
  }
}

function getOverrunPayload() {
  return {
    model: document.getElementById("overrun-model")?.value?.trim() || "",
    extra_pcs: document.getElementById("overrun-extra-pcs")?.value?.trim() || "",
    reason: document.getElementById("overrun-reason")?.value?.trim() || "",
    note: document.getElementById("overrun-note")?.value?.trim() || "",
    reported_by: document.getElementById("overrun-reported-by")?.value?.trim() || "",
  };
}

function getValidatedOverrunPayload() {
  const raw = getOverrunPayload();
  const extraPcs = Number(raw.extra_pcs);
  if (!raw.model) throw new Error("請先輸入機種");
  if (!Number.isFinite(extraPcs) || extraPcs <= 0) throw new Error("多打 pcs 必須大於 0");
  return {
    model: raw.model,
    extra_pcs: extraPcs,
    reason: raw.reason,
    note: raw.note,
    reported_by: raw.reported_by,
  };
}

function buildOverrunSignature(payload) {
  return JSON.stringify(payload);
}

function invalidateOverrunPreview() {
  _overrunPreview = null;
  _overrunPreviewSignature = "";
  const submitBtn = document.getElementById("btn-overrun-submit");
  if (submitBtn) submitBtn.disabled = true;
  renderOverrunPreview();
}

async function handleOverrunPreview() {
  let payload;
  try {
    payload = getValidatedOverrunPayload();
  } catch (error) {
    showToast(error.message);
    return;
  }

  const button = document.getElementById("btn-overrun-preview");
  if (button) {
    button.disabled = true;
    button.textContent = "預覽中...";
  }

  try {
    const preview = await apiPost("/api/defectives/overrun/preview", payload);
    _overrunPreview = preview;
    _overrunPreviewSignature = buildOverrunSignature(payload);
    renderOverrunPreview(preview);
    document.getElementById("btn-overrun-submit")?.removeAttribute("disabled");
  } catch (error) {
    _overrunPreview = null;
    _overrunPreviewSignature = "";
    renderOverrunPreview();
    showToast("預覽失敗：" + error.message);
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = "預覽扣帳";
    }
  }
}

async function handleOverrunSubmit() {
  let payload;
  try {
    payload = getValidatedOverrunPayload();
  } catch (error) {
    showToast(error.message);
    return;
  }

  if (!_overrunPreview || _overrunPreviewSignature !== buildOverrunSignature(payload)) {
    showToast("內容已變更，請先重新預覽後再確認扣帳");
    return;
  }

  const targetModel = _overrunPreview.requested_model || _overrunPreview.model || payload.model;
  const itemCount = Number(_overrunPreview.deducted_count || 0);
  if (!confirm(`確定要對機種「${targetModel}」做加工多打扣帳？\n將依 BOM 扣帳 ${itemCount} 筆料號。`)) {
    return;
  }

  const previewBtn = document.getElementById("btn-overrun-preview");
  const submitBtn = document.getElementById("btn-overrun-submit");
  if (previewBtn) previewBtn.disabled = true;
  if (submitBtn) {
    submitBtn.disabled = true;
    submitBtn.textContent = "扣帳中...";
  }

  try {
    const result = await apiPost("/api/defectives/overrun", payload);
    const skipped = result.skipped_parts || [];
    if (skipped.length) {
      _skippedMap.set(result.batch_id, skipped);
    } else {
      _skippedMap.delete(result.batch_id);
    }
    showToast(`已建立加工多打扣帳批次，扣帳 ${result.deducted_count} 筆`, { tone: "success", duration: 3000 });
    clearOverrunForm();
    invalidateOverrunPreview();
    await refreshDefectives();
  } catch (error) {
    showToast("加工多打扣帳失敗：" + error.message);
  } finally {
    if (previewBtn) previewBtn.disabled = false;
    if (submitBtn) {
      submitBtn.textContent = "確認扣帳";
      submitBtn.disabled = !_overrunPreview;
    }
  }
}

function clearOverrunForm() {
  ["overrun-model", "overrun-extra-pcs", "overrun-reason", "overrun-note", "overrun-reported-by"].forEach(id => {
    const element = document.getElementById(id);
    if (element) element.value = "";
  });
}

function renderOverrunPreview(preview = _overrunPreview) {
  const container = document.getElementById("defective-overrun-preview");
  if (!container) return;

  if (!preview) {
    container.innerHTML = '<div class="no-shortage-msg">輸入機種與多打 pcs 後，按「預覽扣帳」即可先確認會扣哪些料號。</div>';
    return;
  }

  const requestedModel = esc(preview.requested_model || preview.model || "");
  const matchedModels = (preview.matched_models || []).map(item => esc(item)).join("、");
  const skipped = preview.skipped_parts || [];
  const rows = preview.results || [];

  let html = '<div class="defective-overrun-summary">';
  html += `<span class="defective-overrun-pill">機種 ${requestedModel}</span>`;
  html += `<span class="defective-overrun-pill">多打 ${fmt(preview.extra_pcs || 0)} pcs</span>`;
  html += `<span class="defective-overrun-pill">扣帳 ${fmt(preview.deducted_count || 0)} 筆</span>`;
  if (Number(preview.negative_count || 0) > 0) {
    html += `<span class="defective-overrun-pill is-warn">負庫存 ${fmt(preview.negative_count || 0)} 筆</span>`;
  }
  if (skipped.length) {
    html += `<span class="defective-overrun-pill is-warn">主檔找不到 ${fmt(skipped.length)} 筆</span>`;
  }
  html += "</div>";

  if (matchedModels) {
    html += `<div class="defective-overrun-meta">BOM 對應：${matchedModels}</div>`;
  }
  html += `<div class="defective-overrun-meta">預計總扣帳數量：${fmt(preview.total_deduction_qty || 0)} pcs</div>`;

  if (skipped.length) {
    html += `<div class="defective-overrun-meta" style="color:#b91c1c">主檔找不到：${skipped.map(part => esc(part)).join("、")}</div>`;
  }

  if (rows.length) {
    html += '<div class="defective-overrun-table-wrap"><table class="analytics-table defective-batch-table"><thead><tr>';
    html += "<th>料號</th><th>說明</th><th>扣帳數量</th><th>扣帳前庫存</th><th>扣帳後庫存</th>";
    html += "</tr></thead><tbody>";
    for (const row of rows) {
      const stockClass = row.stock_after < 0 ? ' class="stock-negative"' : "";
      html += `<tr>
        <td>${esc(row.part_number)}</td>
        <td>${esc(row.description || "")}</td>
        <td>${fmt(row.defective_qty || 0)}</td>
        <td>${fmt(row.stock_before || 0)}</td>
        <td${stockClass}>${fmt(row.stock_after || 0)}</td>
      </tr>`;
    }
    html += "</tbody></table></div>";
  } else {
    html += '<div class="no-shortage-msg" style="margin-top:10px">這次預覽沒有任何可扣帳的料號。</div>';
  }

  container.innerHTML = html;
}

// ── 批次渲染 ─────────────────────────────────────────────────────────────────

function renderBatches() {
  const container = document.getElementById("defective-list");
  if (!container) return;

  if (!_batches.length) {
    container.innerHTML = '<div class="no-shortage-msg">目前沒有扣帳紀錄</div>';
    return;
  }

  container.innerHTML = _batches.map(batch => {
    const isCollapsed = _collapsed.has(batch.id);
    const items = batch.items || [];
    const dateStr = (batch.imported_at || "").slice(0, 16).replace("T", " ");
    const totalQty = items.reduce((sum, item) => sum + Number(item.defective_qty || 0), 0);
    const skipped = _skippedMap.get(batch.id) || [];
    const batchType = String(batch.batch_type || "defective");
    const typeLabel = batchType === "overrun" ? "加工多打" : "不良品";
    const typeClass = batchType === "overrun" ? "is-overrun" : "is-defective";
    const noteHtml = batch.note ? `<div class="defective-batch-note">${esc(batch.note)}</div>` : "";
    const addButton = batch.can_add_file
      ? `<button class="btn btn-secondary btn-xs" onclick="event.stopPropagation(); window._defAddToBatch(${batch.id})">追加</button>`
      : "";

    let html = `<div class="defective-batch" data-batch-id="${batch.id}">
      <div class="defective-batch-header" onclick="window._defToggleBatch(${batch.id})">
        <span class="defective-batch-toggle">${isCollapsed ? "▶" : "▼"}</span>
        <span class="defective-batch-type ${typeClass}">${typeLabel}</span>
        <span class="defective-batch-name">${esc(batch.filename)}</span>
        <span class="defective-batch-date">${dateStr}</span>
        <span class="defective-batch-count">${items.length} 筆 / ${fmt(totalQty)} pcs</span>
        <span class="spacer"></span>
        ${addButton}
        <button class="btn btn-danger btn-xs" onclick="event.stopPropagation(); window._defDeleteBatch(${batch.id})">刪除</button>
      </div>`;

    if (!isCollapsed) {
      html += noteHtml;
    }

    if (!isCollapsed && items.length) {
      html += '<table class="analytics-table defective-batch-table"><thead><tr>';
      html += "<th>料號</th><th>說明</th><th>扣帳數量</th><th>扣帳前庫存</th><th>扣帳後庫存</th>";
      html += "</tr></thead><tbody>";
      for (const item of items) {
        const stockClass = item.stock_after < 0 ? ' class="stock-negative"' : "";
        html += `<tr>
          <td>${esc(item.part_number)}</td>
          <td>${esc(item.description || "")}</td>
          <td>${fmt(item.defective_qty || 0)}</td>
          <td>${fmt(item.stock_before || 0)}</td>
          <td${stockClass}>${fmt(item.stock_after || 0)}</td>
        </tr>`;
      }
      html += "</tbody></table>";
    } else if (!isCollapsed && !items.length) {
      html += '<div class="defective-batch-empty">無項目</div>';
    }

    if (!isCollapsed && skipped.length) {
      html += `<div class="defective-skipped">主檔找不到（${skipped.length} 筆）：${skipped.map(part => esc(part)).join("、")}</div>`;
    }

    html += "</div>";
    return html;
  }).join("");
}

// ── Global handlers ─────────────────────────────────────────────────────────

window._defToggleBatch = (batchId) => {
  if (_collapsed.has(batchId)) {
    _collapsed.delete(batchId);
  } else {
    _collapsed.add(batchId);
  }
  renderBatches();
};

window._defAddToBatch = (batchId) => {
  const batch = _batches.find(item => item.id === batchId);
  if (!batch || !batch.can_add_file) return;

  _skippedMap.delete(batchId);
  const input = document.getElementById("defective-import-input");
  if (!input) return;
  input.value = "";
  input.onchange = async () => {
    const file = input.files?.[0];
    if (!file) return;
    await doImport(file, batchId);
  };
  input.click();
};

window._defDeleteBatch = async (batchId) => {
  const batch = _batches.find(item => item.id === batchId);
  const name = batch?.filename || `#${batchId}`;
  if (!confirm(`確定要刪除批次「${name}」？\n已扣帳的庫存會自動加回主檔。`)) return;
  try {
    const resp = await apiFetch(`/api/defectives/batches/${batchId}`, { method: "DELETE" });
    const result = await resp.json();
    _skippedMap.delete(batchId);
    const reversedCount = result.reversed_count || 0;
    if (result.main_file_changed) {
      showToast("已刪除批次（主檔已更換，未回寫庫存）", { tone: "success" });
    } else {
      showToast(`已刪除批次，回復 ${reversedCount} 筆庫存`, { tone: "success" });
    }
    await refreshDefectives();
  } catch (e) {
    showToast("刪除失敗：" + e.message);
  }
};
