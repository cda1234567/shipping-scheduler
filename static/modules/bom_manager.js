import { apiFetch, apiJson, apiPut, showToast, esc } from "./api.js";

let _onRefresh = null;
let _outerSortable = null;
const _innerSortables = [];
let _editorBom = null;

export function initBomManager(onRefreshCallback) {
  _onRefresh = onRefreshCallback;

  const uploadArea = document.getElementById("bom-upload-area");
  const bomInput = document.getElementById("bom-file-input");

  uploadArea.addEventListener("click", () => bomInput.click());
  uploadArea.addEventListener("dragover", event => {
    event.preventDefault();
    uploadArea.style.borderColor = "#007aff";
  });
  uploadArea.addEventListener("dragleave", () => {
    uploadArea.style.borderColor = "";
  });
  uploadArea.addEventListener("drop", async event => {
    event.preventDefault();
    uploadArea.style.borderColor = "";
    if (event.dataTransfer.files.length) {
      await uploadBom(event.dataTransfer.files);
    }
  });
  bomInput.addEventListener("change", async event => {
    if (event.target.files.length) {
      await uploadBom(event.target.files);
    }
    bomInput.value = "";
  });

  bindEditorModal();
  renderBomGroups();
}

async function uploadBom(files) {
  const fd = new FormData();
  for (const file of files) fd.append("files", file);
  const groupModel = (document.getElementById("bom-group-model")?.value || "").trim();
  if (groupModel) fd.append("group_model", groupModel);

  try {
    const response = await fetch("/api/bom/upload", { method: "POST", body: fd });
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);

    const saved = data.saved || [];
    const errors = data.errors || [];
    const converted = saved.filter(item => item.is_converted).length;
    let message = `已上傳 ${saved.length} 份 BOM`;
    if (converted) message += `，${converted} 份 xls 已轉成 xlsx`;
    if (errors.length) {
      message += `，${errors.length} 份失敗`;
      alert(`BOM 上傳失敗：\n${errors.join("\n")}`);
    }
    showToast(message);
    await renderBomGroups();
    if (_onRefresh) await _onRefresh();
  } catch (error) {
    showToast(`上傳 BOM 失敗：${error.message}`);
  }
}

export async function renderBomGroups() {
  const container = document.getElementById("bom-group-list");
  if (!container) return;

  if (_outerSortable) {
    _outerSortable.destroy();
    _outerSortable = null;
  }
  _innerSortables.forEach(sortable => sortable.destroy());
  _innerSortables.length = 0;

  try {
    const data = await apiJson("/api/bom/list");
    const groups = data.groups || [];

    if (!groups.length) {
      container.innerHTML = '<div class="empty-state">尚未上傳 BOM 檔案</div>';
      return;
    }

    container.innerHTML = groups.map(group => `
      <div class="bom-model-group" data-model="${esc(group.model)}">
        <div class="bom-model-header">
          <span class="drag-handle-model" title="拖曳排序機種">⋮⋮</span>
          <span class="bom-model-name">${esc(group.model)}</span>
          <span class="bom-model-count">${group.items.length} 份 BOM</span>
        </div>
        <ul class="bom-items-list">
          ${group.items.map(item => itemHtml(item)).join("")}
        </ul>
      </div>`).join("");

    if (typeof Sortable !== "undefined") {
      _outerSortable = Sortable.create(container, {
        animation: 150,
        handle: ".drag-handle-model",
        ghostClass: "sortable-ghost",
      });

      container.querySelectorAll(".bom-items-list").forEach(list => {
        const sortable = Sortable.create(list, {
          animation: 150,
          handle: ".drag-handle-item",
          ghostClass: "sortable-ghost",
          group: "bom-items",
        });
        _innerSortables.push(sortable);
      });
    }

    container.querySelectorAll(".bom-edit").forEach(button => {
      button.addEventListener("click", () => openEditor(button.dataset.id));
    });
    container.querySelectorAll(".bom-download").forEach(button => {
      button.addEventListener("click", () => {
        window.location.href = `/api/bom/${button.dataset.id}/file`;
      });
    });
    container.querySelectorAll(".bom-del").forEach(button => {
      button.addEventListener("click", () => deleteBom(button.dataset.id));
    });
  } catch (error) {
    container.innerHTML = `<div class="empty-state">載入 BOM 失敗：${esc(error.message)}</div>`;
  }
}

function itemHtml(item) {
  const date = (item.uploaded_at || "").slice(5, 10);
  const sourceName = item.source_filename || item.filename;
  const convertedTag = item.is_converted
    ? '<span class="tag tag-converted">XLS→XLSX</span>'
    : "";
  const csTag = item.customer_supplied_count
    ? `<span class="tag tag-cs">${item.customer_supplied_count} 客供</span>`
    : "";

  return `<li class="bom-item-row" data-id="${item.id}">
    <span class="drag-handle-item" title="拖曳排序檔案">⋮⋮</span>
    <div class="bom-item-main">
      <div class="bom-item-top">
        <span class="tag tag-pcb">${esc(item.pcb)}</span>
        <span class="bom-item-filename" title="${esc(item.filename)}">${esc(item.filename)}</span>
        ${convertedTag}
        ${csTag}
      </div>
      <div class="bom-item-sub" title="${esc(sourceName)}">
        ${item.is_converted ? `原始 ${esc(sourceName)}，已轉為正式 xlsx` : `來源 ${esc(sourceName)}`}
      </div>
    </div>
    <span class="bom-item-info">${item.components} 列${date ? ` · ${date}` : ""}</span>
    <div class="bom-item-actions">
      <button class="btn btn-secondary btn-sm bom-download" data-id="${item.id}" title="下載正式 BOM">下載</button>
      <button class="btn btn-primary btn-sm bom-edit" data-id="${item.id}" title="線上編輯">編輯</button>
      <button class="bom-del" data-id="${item.id}" title="刪除">✕</button>
    </div>
  </li>`;
}

async function deleteBom(id) {
  if (!confirm("確定要刪除這份 BOM 嗎？")) return;
  try {
    await apiFetch(`/api/bom/${id}`, { method: "DELETE" });
    await renderBomGroups();
    if (_onRefresh) await _onRefresh();
    showToast("BOM 已刪除");
  } catch (error) {
    showToast(`刪除失敗：${error.message}`);
  }
}

function bindEditorModal() {
  document.getElementById("bom-editor-close")?.addEventListener("click", closeEditor);
  document.getElementById("bom-editor-cancel")?.addEventListener("click", closeEditor);
  document.getElementById("bom-editor-save")?.addEventListener("click", saveEditor);
  document.getElementById("bom-editor-modal")?.addEventListener("click", event => {
    if (event.target.id === "bom-editor-modal") closeEditor();
  });
}

async function openEditor(bomId) {
  try {
    const bom = await apiJson(`/api/bom/${bomId}/editor`);
    _editorBom = bom;
    renderEditor(bom);
    document.getElementById("bom-editor-modal").style.display = "flex";
  } catch (error) {
    showToast(`載入 BOM 失敗：${error.message}`);
  }
}

function closeEditor() {
  document.getElementById("bom-editor-modal").style.display = "none";
  document.getElementById("bom-editor-form").reset();
  document.getElementById("bom-editor-table-body").innerHTML = "";
  _editorBom = null;
}

function renderEditor(bom) {
  document.getElementById("bom-editor-title").textContent = `編輯 BOM：${bom.filename}`;
  document.getElementById("bom-editor-source").textContent = bom.is_converted
    ? `原始檔 ${bom.source_filename || bom.filename} 已轉為正式 xlsx，同步修改都會寫回 ${bom.filename}`
    : `正式 BOM 檔：${bom.filename}`;
  document.getElementById("bom-editor-convert-note").style.display = bom.is_converted ? "block" : "none";
  document.getElementById("bom-editor-source-format").textContent = (bom.source_format || "").toUpperCase() || "-";
  document.getElementById("bom-editor-po").value = bom.po_number ?? 0;
  document.getElementById("bom-editor-order-qty").value = bom.order_qty ?? 0;
  document.getElementById("bom-editor-model").value = bom.model || "";
  document.getElementById("bom-editor-pcb").value = bom.pcb || "";
  document.getElementById("bom-editor-group-model").value = bom.group_model || "";

  const body = document.getElementById("bom-editor-table-body");
  body.innerHTML = (bom.components || []).map(component => `
    <tr data-row="${component.source_row}">
      <td>${component.source_row || ""}</td>
      <td><input type="text" class="bom-cell-input" data-field="part_number" value="${esc(component.part_number)}"></td>
      <td><input type="text" class="bom-cell-input" data-field="description" value="${esc(component.description || "")}"></td>
      <td><input type="number" class="bom-cell-input" data-field="qty_per_board" step="0.01" value="${component.qty_per_board ?? 0}"></td>
      <td><input type="number" class="bom-cell-input" data-field="needed_qty" step="0.01" value="${component.needed_qty ?? 0}"></td>
      <td><input type="number" class="bom-cell-input" data-field="prev_qty_cs" step="0.01" value="${component.prev_qty_cs ?? 0}"></td>
      <td>
        <label class="bom-check-label">
          <input type="checkbox" class="bom-cell-check" data-field="is_dash" ${component.is_dash ? "checked" : ""}>
          跳過
        </label>
      </td>
      <td>${component.is_customer_supplied ? '<span class="tag tag-cs">客供</span>' : '<span class="bom-auto-tag">自動判斷</span>'}</td>
    </tr>`).join("");
}

function readNumberValue(id) {
  return parseFloat(document.getElementById(id).value || "0") || 0;
}

function readEditorPayload() {
  const rows = Array.from(document.querySelectorAll("#bom-editor-table-body tr"));
  const components = rows.map(row => {
    const sourceRow = parseInt(row.dataset.row || "0", 10);
    const getValue = field => row.querySelector(`[data-field="${field}"]`)?.value ?? "";
    const getNumber = field => parseFloat(getValue(field) || "0") || 0;
    const isDash = !!row.querySelector('[data-field="is_dash"]')?.checked;
    const partNumber = String(getValue("part_number") || "").trim();
    if (!partNumber) {
      throw new Error(`第 ${sourceRow} 列料號不可為空白`);
    }
    return {
      source_row: sourceRow,
      part_number: partNumber,
      description: String(getValue("description") || "").trim(),
      qty_per_board: getNumber("qty_per_board"),
      needed_qty: getNumber("needed_qty"),
      prev_qty_cs: getNumber("prev_qty_cs"),
      is_dash: isDash,
    };
  });

  return {
    po_number: Math.round(readNumberValue("bom-editor-po")),
    order_qty: readNumberValue("bom-editor-order-qty"),
    model: document.getElementById("bom-editor-model").value.trim(),
    pcb: document.getElementById("bom-editor-pcb").value.trim(),
    group_model: document.getElementById("bom-editor-group-model").value.trim(),
    components,
  };
}

async function saveEditor() {
  if (!_editorBom) return;

  const saveButton = document.getElementById("bom-editor-save");
  try {
    const payload = readEditorPayload();
    if (!payload.model) throw new Error("機種不可為空白");

    saveButton.disabled = true;
    saveButton.textContent = "儲存中...";
    const result = await apiPut(`/api/bom/${_editorBom.id}/editor`, payload);
    showToast(`BOM 已同步儲存，${result.components} 列已更新`);
    closeEditor();
    await renderBomGroups();
    if (_onRefresh) await _onRefresh();
  } catch (error) {
    showToast(`儲存失敗：${error.message}`);
  } finally {
    saveButton.disabled = false;
    saveButton.textContent = "儲存並同步正式 BOM";
  }
}

export async function renderBomSidebar() {
  // 目前無側欄內容
}
