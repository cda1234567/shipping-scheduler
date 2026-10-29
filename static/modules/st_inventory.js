import { apiFetch, apiJson, apiPatch, apiPost, showToast, esc } from "./api.js";

let _initialized = false;
let _busy = false;
let _onChanged = null;
let _inMainRows = [];
let _inMainInitialized = false;
let _erpMeta = { fetchedAt: "", wareId: "", wareName: "" };
let _expandedPartNumber = "";
let _auditRowsByPart = {};
let _auditLoadingPart = "";
let _auditRequestToken = 0;

export async function initStInventory({ onChanged, autoLoad = true } = {}) {
  _onChanged = typeof onChanged === "function" ? onChanged : null;
  if (_initialized) {
    if (autoLoad) await refreshStInventoryPanel();
    return;
  }

  document.getElementById("btn-upload-st-inventory")?.addEventListener("click", () => {
    if (_busy) return;
    document.getElementById("st-inventory-file-input")?.click();
  });
  document.getElementById("btn-erp-sync-st-inventory")?.addEventListener("click", () => {
    if (_busy) return;
    void handleErpSync();
  });
  document.getElementById("btn-refresh-st-inventory")?.addEventListener("click", () => {
    if (_busy) return;
    void refreshStInventoryPanel();
  });
  document.getElementById("st-inventory-file-input")?.addEventListener("change", event => {
    void handleUpload(event);
  });

  _initialized = true;
  if (autoLoad) {
    await refreshStInventoryPanel();
  }
}

export async function refreshStInventoryPanel() {
  try {
    const info = await apiJson("/api/system/st-inventory/info");
    renderInfo(info);
    if (document.getElementById("tab-st-inventory")?.classList.contains("active")) {
      await refreshStInventoryInMain({ silent: true });
    }
  } catch (error) {
    renderError(error);
  }
}

export async function initStInventoryInMain({ autoLoad = false } = {}) {
  if (!_inMainInitialized) {
    document.getElementById("st-inventory-search")?.addEventListener("input", () => {
      renderInMainRows();
    });
    document.getElementById("st-inventory-only-main")?.addEventListener("change", () => {
      renderInMainRows();
    });
    document.getElementById("btn-st-inventory-tab-refresh")?.addEventListener("click", () => {
      void refreshStInventoryInMain({ force: true });
    });
    _inMainInitialized = true;
  }
  if (autoLoad) await refreshStInventoryInMain();
}

// ST 庫存頁籤的料號 / 品名 / 庫存改讀正航 ERP（唯讀）；MOQ 與主檔交集由後端用主檔補上。
// 缺料 / 發料計算仍用 Excel 快照，不受此處影響。
export async function refreshStInventoryInMain({ silent = false, force = false } = {}) {
  const statusEl = document.getElementById("st-inventory-tab-status");
  const errorEl = document.getElementById("st-inventory-erp-error");
  const refreshBtn = document.getElementById("btn-st-inventory-tab-refresh");
  if (!silent && statusEl) {
    statusEl.className = "file-status";
    statusEl.innerHTML = "<span>讀取 ERP ST 庫存中...</span>";
  }
  if (refreshBtn) refreshBtn.disabled = true;
  try {
    const payload = await apiJson(`/api/erp/st-stock${force ? "?refresh=true" : ""}`);
    _inMainRows = Array.isArray(payload?.rows) ? payload.rows : [];
    _erpMeta = { fetchedAt: payload?.fetched_at || "", wareId: payload?.ware_id || "", wareName: payload?.ware_name || "" };
    _expandedPartNumber = "";
    if (errorEl) errorEl.hidden = true;
    renderInMainRows();
  } catch (error) {
    // 失敗時保留畫面上既有資料，只顯示錯誤，不動本機任何資料
    if (errorEl) {
      errorEl.textContent = `ERP 讀取失敗：${error.message}`;
      errorEl.hidden = false;
    }
    if (statusEl) {
      statusEl.className = "file-status warn";
      statusEl.innerHTML = "<span>目前無法取得 ERP 即時庫存</span>";
    }
  } finally {
    if (refreshBtn) refreshBtn.disabled = false;
  }
}

function renderInMainRows() {
  const statusEl = document.getElementById("st-inventory-tab-status");
  const body = document.getElementById("st-inventory-table-body");
  const errorEl = document.getElementById("st-inventory-erp-error");
  const query = String(document.getElementById("st-inventory-search")?.value || "").trim().toUpperCase();
  if (!body) return;

  const onlyMain = document.getElementById("st-inventory-only-main")?.checked !== false;
  const rows = _inMainRows
    .filter(row => !onlyMain || row.in_main)
    .filter(row => !query || String(row.part_number || "").toUpperCase().includes(query))
    .sort((a, b) => String(a.part_number || "").localeCompare(String(b.part_number || ""), "en"));

  if (statusEl && !(errorEl && !errorEl.hidden)) {
    statusEl.className = "file-status ok";
    const ware = _erpMeta.wareName ? `${_erpMeta.wareId} ${_erpMeta.wareName}` : _erpMeta.wareId;
    const suffix = query ? `，搜尋結果 ${rows.length} 筆` : "";
    const scope = onlyMain ? `主檔交集 ${_inMainRows.filter(row => row.in_main).length}` : `全部 ${_inMainRows.length}`;
    statusEl.innerHTML = `<span>來源：正航 ERP 倉庫 ${esc(ware)}｜更新時間 ${esc(formatDateTime(_erpMeta.fetchedAt))}｜${scope} 筆${suffix}</span>`;
  }

  if (!rows.length) {
    body.innerHTML = `
      <tr>
        <td colspan="4" class="st-inventory-empty">沒有符合的 ST 庫存料號。</td>
      </tr>
    `;
    return;
  }

  body.innerHTML = rows.map(row => renderInventoryRow(row)).join("");
  bindInventoryRowClicks(body);
  bindMoqBadgeEditors(body);
}

function renderInventoryRow(row) {
  const partNumber = String(row.part_number || "");
  const isExpanded = partNumber && partNumber === _expandedPartNumber;
  const arrow = isExpanded ? "▼" : "▶";
  const mainRow = `
    <tr class="st-inventory-data-row${isExpanded ? " expanded" : ""}" data-part-number="${esc(partNumber)}">
      <td>
        <button class="st-inventory-expand-btn" type="button" aria-label="${isExpanded ? "收起" : "展開"} ${esc(partNumber)} 歷史紀錄">
          <span class="st-inventory-arrow">${arrow}</span>
          <span>${esc(partNumber)}</span>
        </button>
      </td>
      <td>${esc(row.description || "")}</td>
      <td class="num">${formatQty(row.stock_qty)}</td>
      <td>${moqBadgeHtml(row)}</td>
    </tr>
  `;
  if (!isExpanded) return mainRow;
  return mainRow + renderAuditDetailRow(partNumber);
}

function renderAuditDetailRow(partNumber) {
  const rows = _auditRowsByPart[partNumber];
  let content = "";
  if (_auditLoadingPart === partNumber) {
    content = `<div class="st-inventory-audit-empty">讀取變動紀錄中...</div>`;
  } else if (!Array.isArray(rows)) {
    content = `<div class="st-inventory-audit-empty">讀取變動紀錄中...</div>`;
  } else if (!rows.length) {
    content = `<div class="st-inventory-audit-empty">沒有變動紀錄</div>`;
  } else {
    content = `
      <table class="st-inventory-audit-table">
        <thead>
          <tr>
            <th>時間</th>
            <th>舊值</th>
            <th>新值</th>
            <th>差異</th>
            <th>reason</th>
            <th>actor</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map(log => `
            <tr>
              <td>${esc(formatDateTime(log.changed_at))}</td>
              <td class="num">${formatAuditQty(log.old_qty)}</td>
              <td class="num">${formatAuditQty(log.new_qty)}</td>
              <td class="num ${Number(log.delta || 0) < 0 ? "neg" : "pos"}">${formatSignedQty(log.delta)}</td>
              <td>${esc(log.reason || "")}</td>
              <td>${esc(log.actor || "")}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    `;
  }
  return `
    <tr class="st-inventory-audit-row">
      <td colspan="4">
        <div class="st-inventory-audit-panel">${content}</div>
      </td>
    </tr>
  `;
}

function bindInventoryRowClicks(body) {
  body.querySelectorAll(".st-inventory-data-row").forEach(row => {
    row.addEventListener("click", () => {
      const partNumber = String(row.dataset.partNumber || "");
      if (!partNumber) return;
      void toggleAuditRow(partNumber);
    });
  });
}

function moqBadgeHtml(row) {
  const partNumber = esc(row?.part_number || "");
  const moq = Number(row?.moq || 0);
  if (Number.isFinite(moq) && moq > 0) {
    return `<span class="moq-badge moq-badge-present moq-badge-editable" data-part="${partNumber}" data-moq="${moq}" title="雙擊可編輯 MOQ">MOQ ${formatQty(moq)}</span>`;
  }
  return `<span class="moq-badge moq-badge-missing moq-badge-editable" data-part="${partNumber}" data-moq="0" title="雙擊可編輯 MOQ">未寫 MOQ</span>`;
}

function bindMoqBadgeEditors(root) {
  if (!root) return;
  root.querySelectorAll(".moq-badge-editable").forEach(badge => {
    if (badge.dataset.moqBadgeBound === "1") return;
    badge.dataset.moqBadgeBound = "1";
    badge.addEventListener("click", event => {
      event.stopPropagation();
    });
    badge.addEventListener("dblclick", event => {
      event.preventDefault();
      event.stopPropagation();
      void handleMoqBadgeEdit(badge);
    });
  });
}

async function handleMoqBadgeEdit(badge) {
  const partNumber = String(badge?.dataset.part || "").trim().toUpperCase();
  if (!partNumber) {
    showToast("料號不可空白");
    return;
  }
  if (badge.dataset.editing === "1") return;

  const currentMoq = Number(badge?.dataset.moq || 0);
  const originalHtml = badge.innerHTML;
  badge.dataset.editing = "1";
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
    if (!Number.isFinite(moqValue) || moqValue < 0) {
      showToast("MOQ 請輸入 0 或大於 0 的數字");
      input.focus();
      input.select();
      return;
    }

    finished = true;
    try {
      const result = await apiPatch("/api/main-file/moq", { part_number: partNumber, moq: moqValue });
      const savedPart = String(result?.part_number || partNumber).trim().toUpperCase();
      _inMainRows = _inMainRows.map(row => {
        if (String(row.part_number || "").trim().toUpperCase() !== savedPart) return row;
        return { ...row, moq: moqValue };
      });
      renderInMainRows();
      showToast(`${savedPart} MOQ 已儲存`, { tone: "success" });
    } catch (error) {
      delete badge.dataset.editing;
      badge.innerHTML = originalHtml;
      showToast(`MOQ 儲存失敗：${error.message}`, { tone: "error" });
    }
  };

  input.addEventListener("click", event => event.stopPropagation());
  input.addEventListener("dblclick", event => event.stopPropagation());
  input.addEventListener("keydown", event => {
    event.stopPropagation();
    if (event.key === "Enter") {
      event.preventDefault();
      void save();
    } else if (event.key === "Escape") {
      event.preventDefault();
      cancel();
    }
  });
  input.addEventListener("blur", event => {
    event.stopPropagation();
    void save();
  });
}

async function toggleAuditRow(partNumber) {
  if (_expandedPartNumber === partNumber) {
    _expandedPartNumber = "";
    renderInMainRows();
    return;
  }

  _expandedPartNumber = partNumber;
  renderInMainRows();
  if (Array.isArray(_auditRowsByPart[partNumber])) return;

  const token = ++_auditRequestToken;
  _auditLoadingPart = partNumber;
  renderInMainRows();
  try {
    const payload = await apiJson(`/api/system/st-inventory/audit?part_number=${encodeURIComponent(partNumber)}&limit=200`);
    if (token !== _auditRequestToken) return;
    _auditRowsByPart[partNumber] = Array.isArray(payload?.rows) ? payload.rows : [];
  } catch (error) {
    if (token !== _auditRequestToken) return;
    _auditRowsByPart[partNumber] = [{
      changed_at: "",
      old_qty: null,
      new_qty: 0,
      delta: null,
      reason: `讀取失敗：${error.message}`,
      actor: "",
    }];
  } finally {
    if (token === _auditRequestToken) {
      _auditLoadingPart = "";
      renderInMainRows();
    }
  }
}

function formatQty(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return "0";
  return Number.isInteger(num) ? String(num) : String(Math.round(num * 1000000) / 1000000);
}

function formatAuditQty(value) {
  if (value === null || value === undefined || value === "") return "-";
  return formatQty(value);
}

function formatSignedQty(value) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return "-";
  const formatted = formatQty(num);
  return num > 0 ? `+${formatted}` : formatted;
}

function setBusy(nextBusy) {
  _busy = Boolean(nextBusy);
  [
    "btn-erp-sync-st-inventory",
    "btn-upload-st-inventory",
    "btn-refresh-st-inventory",
  ].forEach(id => {
    const element = document.getElementById(id);
    if (element) element.disabled = _busy;
  });
}

function renderInfo(info) {
  const statusEl = document.getElementById("st-inventory-status");
  const metaEl = document.getElementById("st-inventory-meta");
  if (!statusEl || !metaEl) return;

  const syncError = String(info?.last_sync_error || "").trim();
  const errorLine = syncError
    ? `<br><span class="st-inventory-sync-error">${esc(`正航同步失敗：${syncError}`)}</span>`
    : "";

  if (!info?.loaded) {
    statusEl.className = syncError ? "file-status warn" : "file-status";
    statusEl.innerHTML = `<span>尚未載入 ST 庫存</span>${errorLine}`;
    metaEl.innerHTML = '<span>請按「從正航更新」（備援可上傳 .xlsx / .xls / .xlsm）</span>';
    return;
  }

  const isErp = info.source === "erp";
  const lines = [`來源：${esc(isErp ? "正航 ERP" : (info.filename || "Excel 上傳"))}`];
  if (Number(info.part_count || 0) > 0) lines.push(`${Number(info.part_count)} 筆料號`);
  statusEl.className = syncError ? "file-status warn" : "file-status ok";
  statusEl.innerHTML = lines.map(line => `<span>${line}</span>`).join("<br>") + errorLine;

  const meta = [];
  if (info.loaded_at) meta.push(`${isErp ? "最後同步" : "最後匯入"} ${formatDateTime(info.loaded_at)}`);
  meta.push("供缺料 / 補料判斷使用");
  metaEl.innerHTML = meta.map(line => `<span>${esc(line)}</span>`).join("<br>");
}

function renderError(error) {
  const statusEl = document.getElementById("st-inventory-status");
  const metaEl = document.getElementById("st-inventory-meta");
  const message = `讀取 ST 庫存狀態失敗：${error.message}`;
  if (statusEl) {
    statusEl.className = "file-status warn";
    statusEl.innerHTML = `<span>${esc(message)}</span>`;
  }
  if (metaEl) {
    metaEl.innerHTML = '<span>請重新整理，或按「從正航更新」</span>';
  }
}

async function handleErpSync() {
  setBusy(true);
  const statusEl = document.getElementById("st-inventory-status");
  if (statusEl) {
    statusEl.className = "file-status";
    statusEl.innerHTML = "<span>從正航更新 ST 庫存中...</span>";
  }
  try {
    const result = await apiPost("/api/system/st-inventory/erp-sync", {});
    await refreshStInventoryPanel();
    showToast(`ST 庫存已從正航更新（${result.part_count} 筆）`, { tone: "success" });
    if (_onChanged) {
      try {
        await _onChanged(result);
      } catch (error) {
        showToast(`ST 庫存已更新，但排程刷新失敗：${error.message}`, { tone: "error", sticky: true });
      }
    }
  } catch (error) {
    // 後端失敗時本機快照不變；重新讀 info 讓卡片顯示來源與錯誤
    await refreshStInventoryPanel();
    showToast(`從正航更新失敗：${error.message}`, { tone: "error", sticky: true });
  } finally {
    setBusy(false);
  }
}

async function handleUpload(event) {
  const input = event?.target;
  const file = input?.files?.[0];
  if (!file) return;

  setBusy(true);
  const statusEl = document.getElementById("st-inventory-status");
  const metaEl = document.getElementById("st-inventory-meta");
  if (statusEl) {
    statusEl.className = "file-status";
    statusEl.innerHTML = "<span>上傳 ST 庫存中...</span>";
  }
  if (metaEl) {
    metaEl.innerHTML = `<span>${esc(file.name)}</span>`;
  }

  try {
    const formData = new FormData();
    formData.append("file", file);
    const response = await apiFetch("/api/system/st-inventory/upload", {
      method: "POST",
      body: formData,
    });
    const result = await response.json();
    renderInfo({
      loaded: true,
      filename: result.filename,
      part_count: result.part_count,
      loaded_at: result.loaded_at,
    });
    await refreshStInventoryInMain({ silent: true });
    showToast(`ST 庫存已匯入：${result.filename}（${result.part_count} 筆）`, { tone: "success" });
    if (_onChanged) {
      try {
        await _onChanged(result);
      } catch (error) {
        showToast(`ST 庫存已匯入，但排程刷新失敗：${error.message}`, { tone: "error", sticky: true });
      }
    }
  } catch (error) {
    renderError(error);
    showToast(`ST 庫存匯入失敗：${error.message}`, { tone: "error", sticky: true });
  } finally {
    setBusy(false);
    if (input) input.value = "";
  }
}

function formatDateTime(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  const normalized = text.includes("T") ? text : text.replace(" ", "T");
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return text.replace("T", " ").slice(0, 16);
  const y = parsed.getFullYear();
  const m = String(parsed.getMonth() + 1).padStart(2, "0");
  const d = String(parsed.getDate()).padStart(2, "0");
  const hh = String(parsed.getHours()).padStart(2, "0");
  const mm = String(parsed.getMinutes()).padStart(2, "0");
  return `${y}/${m}/${d} ${hh}:${mm}`;
}
