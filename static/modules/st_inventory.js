import { apiFetch, apiJson, showToast, esc } from "./api.js";

let _initialized = false;
let _busy = false;
let _onChanged = null;
let _inMainRows = [];
let _inMainInitialized = false;

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
    document.getElementById("btn-st-inventory-tab-refresh")?.addEventListener("click", () => {
      void refreshStInventoryInMain();
    });
    _inMainInitialized = true;
  }
  if (autoLoad) await refreshStInventoryInMain();
}

export async function refreshStInventoryInMain({ silent = false } = {}) {
  const statusEl = document.getElementById("st-inventory-tab-status");
  if (!silent && statusEl) {
    statusEl.className = "file-status";
    statusEl.innerHTML = "<span>讀取 ST 庫存中...</span>";
  }
  try {
    const rows = await apiJson("/api/st-inventory/in-main");
    _inMainRows = Array.isArray(rows) ? rows : [];
    renderInMainRows();
  } catch (error) {
    _inMainRows = [];
    if (statusEl) {
      statusEl.className = "file-status warn";
      statusEl.innerHTML = `<span>${esc(error.message)}</span>`;
    }
    const body = document.getElementById("st-inventory-table-body");
    if (body) body.innerHTML = "";
  }
}

function renderInMainRows() {
  const statusEl = document.getElementById("st-inventory-tab-status");
  const body = document.getElementById("st-inventory-table-body");
  const query = String(document.getElementById("st-inventory-search")?.value || "").trim().toUpperCase();
  if (!body) return;

  const rows = _inMainRows
    .filter(row => !query || String(row.part_number || "").toUpperCase().includes(query))
    .sort((a, b) => String(a.part_number || "").localeCompare(String(b.part_number || ""), "en"));

  if (statusEl) {
    statusEl.className = "file-status ok";
    const suffix = query ? `，搜尋結果 ${rows.length} 筆` : "";
    statusEl.innerHTML = `<span>主檔交集 ${_inMainRows.length} 筆${suffix}</span>`;
  }

  if (!rows.length) {
    body.innerHTML = `
      <tr>
        <td colspan="3" class="st-inventory-empty">沒有符合的 ST 庫存料號。</td>
      </tr>
    `;
    return;
  }

  body.innerHTML = rows.map(row => `
    <tr>
      <td>${esc(row.part_number || "")}</td>
      <td>${esc(row.description || "")}</td>
      <td class="num">${formatQty(row.stock_qty)}</td>
    </tr>
  `).join("");
}

function formatQty(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return "0";
  return Number.isInteger(num) ? String(num) : String(Math.round(num * 1000000) / 1000000);
}

function setBusy(nextBusy) {
  _busy = Boolean(nextBusy);
  [
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

  if (!info?.loaded) {
    statusEl.className = "file-status";
    statusEl.innerHTML = "<span>尚未載入 ST 庫存</span>";
    metaEl.innerHTML = '<span>支援 .xlsx / .xls / .xlsm</span>';
    return;
  }

  const lines = [];
  if (info.filename) lines.push(esc(info.filename));
  if (Number(info.part_count || 0) > 0) lines.push(`${Number(info.part_count)} 筆料號`);
  statusEl.className = "file-status ok";
  statusEl.innerHTML = lines.map(line => `<span>${line}</span>`).join("<br>");

  const meta = [];
  if (info.loaded_at) meta.push(`最後匯入 ${formatDateTime(info.loaded_at)}`);
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
    metaEl.innerHTML = '<span>請重新整理或重新上傳 ST 庫存檔</span>';
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
