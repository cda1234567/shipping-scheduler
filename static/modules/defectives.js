import { apiJson, apiFetch, showToast, esc, fmt } from "./api.js";

let _batches = [];
const _collapsed = new Set();
const _skippedMap = new Map();  // batch_id → [part_number, ...]

export async function initDefectives() {
  document.getElementById("btn-import-defective")?.addEventListener("click", handleImport);
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
  const formData = new FormData();
  formData.append("file", file);

  const btn = document.getElementById("btn-import-defective");
  if (btn) { btn.disabled = true; btn.textContent = "匯入中..."; }
  showToast(`正在匯入 ${file.name}，解析並扣帳主檔中...`, { duration: 30000 });

  const url = batchId ? `/api/defectives/batches/${batchId}/add` : "/api/defectives/import";

  try {
    const resp = await apiFetch(url, { method: "POST", body: formData });
    const result = await resp.json();
    const skipped = result.skipped_parts || [];
    const targetBatchId = batchId || result.batch_id;

    if (skipped.length) {
      _skippedMap.set(targetBatchId, skipped);
    } else {
      _skippedMap.delete(targetBatchId);
    }

    showToast(`已匯入 ${result.deducted_count} 筆不良品並扣帳`, { tone: "success", duration: 3000 });
    await refreshDefectives();
  } catch (e) {
    showToast("匯入失敗：" + e.message);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "匯入副檔扣帳"; }
  }
}

// ── 批次渲染 ─────────────────────────────────────────────────────────────────

function renderBatches() {
  const container = document.getElementById("defective-list");
  if (!container) return;

  if (!_batches.length) {
    container.innerHTML = '<div class="no-shortage-msg">目前沒有不良品紀錄</div>';
    return;
  }

  container.innerHTML = _batches.map(batch => {
    const isCollapsed = _collapsed.has(batch.id);
    const items = batch.items || [];
    const dateStr = (batch.imported_at || "").slice(0, 16).replace("T", " ");
    const totalQty = items.reduce((s, i) => s + (i.defective_qty || 0), 0);
    const skipped = _skippedMap.get(batch.id) || [];

    let html = `<div class="defective-batch" data-batch-id="${batch.id}">
      <div class="defective-batch-header" onclick="window._defToggleBatch(${batch.id})">
        <span class="defective-batch-toggle">${isCollapsed ? "▶" : "▼"}</span>
        <span class="defective-batch-name">${esc(batch.filename)}</span>
        <span class="defective-batch-date">${dateStr}</span>
        <span class="defective-batch-count">${items.length} 筆 / ${fmt(totalQty)} pcs</span>
        <span class="spacer"></span>
        <button class="btn btn-secondary btn-xs" onclick="event.stopPropagation(); window._defAddToBatch(${batch.id})">追加</button>
        <button class="btn btn-danger btn-xs" onclick="event.stopPropagation(); window._defDeleteBatch(${batch.id})">刪除</button>
      </div>`;

    if (!isCollapsed && items.length) {
      html += '<table class="analytics-table defective-batch-table"><thead><tr>';
      html += "<th>料號</th><th>說明</th><th>不良數量</th><th>扣帳前庫存</th><th>扣帳後庫存</th>";
      html += "</tr></thead><tbody>";
      for (const item of items) {
        const stockClass = item.stock_after < 0 ? ' class="stock-negative"' : "";
        html += `<tr>
          <td>${esc(item.part_number)}</td>
          <td>${esc(item.description || "")}</td>
          <td>${fmt(item.defective_qty)}</td>
          <td>${fmt(item.stock_before)}</td>
          <td${stockClass}>${fmt(item.stock_after)}</td>
        </tr>`;
      }
      html += "</tbody></table>";
    } else if (!isCollapsed && !items.length) {
      html += '<div class="defective-batch-empty">無項目</div>';
    }

    // 略過的料號 — 紅字列在最下面
    if (!isCollapsed && skipped.length) {
      html += `<div class="defective-skipped">主檔找不到（${skipped.length} 筆）：${skipped.map(p => esc(p)).join("、")}</div>`;
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
  // 追加時清掉該批次的 skipped 記錄
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
  const batch = _batches.find(b => b.id === batchId);
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
