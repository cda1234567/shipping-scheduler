import { apiJson, apiPost, apiPatch, showToast, esc, fmt } from "./api.js";

let _records = [];
let _statusFilter = "all";

const ACTION_LABELS = {
  rework: "重工",
  scrap: "報廢",
  return_supplier: "退回供應商",
  replace: "換料",
  other: "其他",
};

const STATUS_LABELS = {
  open: "待處理",
  confirmed: "已確認",
  closed: "已結案",
};

const STATUS_CLS = {
  open: "badge-shortage",
  confirmed: "badge-merged",
  closed: "badge-ok",
};

export async function initDefectives() {
  document.getElementById("btn-add-defective")?.addEventListener("click", openCreateModal);
  document.getElementById("defective-status-filter")?.addEventListener("change", (e) => {
    _statusFilter = e.target.value;
    renderDefectives();
  });
  document.getElementById("defective-modal-cancel")?.addEventListener("click", closeModal);
  document.getElementById("defective-modal-save")?.addEventListener("click", handleSave);
}

export async function refreshDefectives() {
  try {
    const d = await apiJson(`/api/defectives?status=${_statusFilter}`);
    _records = d.records || [];
  } catch (_) {
    _records = [];
  }
  renderDefectives();
}

function renderDefectives() {
  const container = document.getElementById("defective-list");
  if (!container) return;

  const filtered = _statusFilter === "all"
    ? _records
    : _records.filter(r => r.status === _statusFilter);

  if (!filtered.length) {
    container.innerHTML = '<div class="no-shortage-msg">目前沒有不良品紀錄</div>';
    return;
  }

  container.innerHTML = filtered.map(r => {
    const statusBadge = `<span class="po-status-badge ${STATUS_CLS[r.status] || ""}">${STATUS_LABELS[r.status] || r.status}</span>`;
    const actionLabel = ACTION_LABELS[r.action_taken] || r.action_taken || "—";
    const poInfo = r.po_number ? `PO ${esc(String(r.po_number))}` : "";
    const modelInfo = r.model ? esc(r.model) : "";
    const orderInfo = [poInfo, modelInfo].filter(Boolean).join(" / ");
    const dateStr = (r.created_at || "").slice(0, 16).replace("T", " ");

    return `<div class="defective-card">
      <div class="defective-header">
        <span class="defective-part">${esc(r.part_number)}</span>
        ${statusBadge}
      </div>
      ${r.description ? `<div class="defective-desc">${esc(r.description)}</div>` : ""}
      <div class="defective-meta">
        <span>數量: <strong>${fmt(r.defective_qty)}</strong></span>
        <span>處理: <strong>${esc(actionLabel)}</strong></span>
        ${orderInfo ? `<span>${orderInfo}</span>` : ""}
        <span>${dateStr}</span>
      </div>
      ${r.action_note ? `<div class="defective-note">${esc(r.action_note)}</div>` : ""}
      <div class="defective-actions">
        ${r.status === "open" ? `
          <button class="btn btn-secondary btn-sm" onclick="window._defectiveEdit(${r.id})">編輯</button>
          <button class="btn btn-primary btn-sm" onclick="window._defectiveConfirm(${r.id})">確認</button>
          <button class="btn btn-danger btn-sm" onclick="window._defectiveClose(${r.id})">結案</button>
        ` : ""}
        ${r.status === "confirmed" ? `
          <button class="btn btn-danger btn-sm" onclick="window._defectiveClose(${r.id})">結案</button>
        ` : ""}
      </div>
    </div>`;
  }).join("");
}

// Global handlers for inline onclick
window._defectiveEdit = async (id) => {
  const record = _records.find(r => r.id === id);
  if (!record) return;
  openEditModal(record);
};

window._defectiveConfirm = async (id) => {
  if (!confirm("確定要確認此不良品紀錄？")) return;
  try {
    await apiPost(`/api/defectives/${id}/confirm`);
    showToast("已確認", { tone: "success" });
    await refreshDefectives();
  } catch (e) {
    showToast("確認失敗：" + e.message);
  }
};

window._defectiveClose = async (id) => {
  if (!confirm("確定要結案此不良品紀錄？")) return;
  try {
    await apiPost(`/api/defectives/${id}/close`);
    showToast("已結案", { tone: "success" });
    await refreshDefectives();
  } catch (e) {
    showToast("結案失敗：" + e.message);
  }
};

let _editingId = null;

function openCreateModal() {
  _editingId = null;
  const modal = document.getElementById("defective-modal");
  document.getElementById("defective-modal-title").textContent = "新增不良品";
  document.getElementById("def-part-number").value = "";
  document.getElementById("def-description").value = "";
  document.getElementById("def-qty").value = "";
  document.getElementById("def-action").value = "";
  document.getElementById("def-note").value = "";
  document.getElementById("def-reported-by").value = "";
  modal.style.display = "flex";
}

function openEditModal(record) {
  _editingId = record.id;
  const modal = document.getElementById("defective-modal");
  document.getElementById("defective-modal-title").textContent = "編輯不良品";
  document.getElementById("def-part-number").value = record.part_number || "";
  document.getElementById("def-description").value = record.description || "";
  document.getElementById("def-qty").value = record.defective_qty || "";
  document.getElementById("def-action").value = record.action_taken || "";
  document.getElementById("def-note").value = record.action_note || "";
  document.getElementById("def-reported-by").value = record.reported_by || "";
  modal.style.display = "flex";
}

function closeModal() {
  document.getElementById("defective-modal").style.display = "none";
  _editingId = null;
}

async function handleSave() {
  const partNumber = document.getElementById("def-part-number").value.trim();
  const qty = parseFloat(document.getElementById("def-qty").value);
  if (!partNumber) { showToast("料號不可為空"); return; }
  if (!qty || qty <= 0) { showToast("數量必須大於 0"); return; }

  const body = {
    part_number: partNumber,
    description: document.getElementById("def-description").value.trim(),
    defective_qty: qty,
    action_taken: document.getElementById("def-action").value,
    action_note: document.getElementById("def-note").value.trim(),
    reported_by: document.getElementById("def-reported-by").value.trim(),
  };

  try {
    if (_editingId) {
      await apiPatch(`/api/defectives/${_editingId}`, body);
      showToast("已更新", { tone: "success" });
    } else {
      await apiPost("/api/defectives", body);
      showToast("已新增", { tone: "success" });
    }
    closeModal();
    await refreshDefectives();
  } catch (e) {
    showToast("儲存失敗：" + e.message);
  }
}
