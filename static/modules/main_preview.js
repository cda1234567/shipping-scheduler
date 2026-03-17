import { apiJson, apiFetch, esc, showToast } from "./api.js";

let _sheetCache = new Map();
let _sheetNames = [];
let _activeSheet = "";
let _workbookMeta = { filename: "", loaded_at: "", style_preserved: true };
let _searchMatches = [];
let _searchCursor = -1;
let _currentQuery = "";
let _initialized = false;
const MAIN_PREVIEW_ROW_HEADER_WIDTH = 56;
const MAIN_PREVIEW_COLUMN_HEADER_HEIGHT = 30;
const MAIN_PREVIEW_FROZEN_COLUMN_COUNT = 3;

export async function initMainPreview() {
  if (_initialized) return;
  _initialized = true;

  hydratePreviewLabels();

  document.getElementById("btn-main-preview-refresh")?.addEventListener("click", () => {
    void refreshMainPreview({ force: true, eager: true });
  });
  document.getElementById("btn-main-preview-search")?.addEventListener("click", runSheetSearch);
  document.getElementById("btn-main-preview-clear")?.addEventListener("click", clearSheetSearch);
  document.getElementById("main-preview-search")?.addEventListener("keydown", event => {
    if (event.key === "Enter") {
      event.preventDefault();
      runSheetSearch();
    }
    if (event.key === "Escape") {
      event.preventDefault();
      clearSheetSearch();
    }
  });

  // 雙擊儲存格 → 進入編輯模式
  document.getElementById("main-preview-stage")?.addEventListener("dblclick", event => {
    const td = event.target.closest("td.main-preview-cell");
    if (!td) return;
    startCellEdit(td);
  });
}

export async function onMainPreviewTabActivated() {
  hydratePreviewLabels();
  await refreshMainPreview({ eager: true });
}

export async function refreshMainPreview({ force = false, sheet = "", eager = false } = {}) {
  if (force) {
    _sheetCache.clear();
    if (sheet) _activeSheet = sheet;
  }

  const panel = document.getElementById("tab-main-preview");
  const isActive = panel?.classList.contains("active");
  if (!isActive && !eager) return;

  renderLoadingState("正在載入目前 live 主檔...");

  try {
    const requestedSheet = sheet || _activeSheet;
    let payload = requestedSheet ? _sheetCache.get(requestedSheet) : null;

    if (!payload) {
      const url = new URL("/api/main-file/preview", window.location.origin);
      if (requestedSheet) url.searchParams.set("sheet", requestedSheet);
      payload = await apiJson(`${url.pathname}${url.search}`);
      _workbookMeta = {
        filename: payload.filename || "",
        loaded_at: payload.loaded_at || "",
        style_preserved: payload.style_preserved !== false,
      };
      _sheetNames = payload.sheet_names || [];
      _activeSheet = payload.selected_sheet || requestedSheet || "";
      if (_activeSheet) _sheetCache.set(_activeSheet, payload);
    } else {
      _activeSheet = requestedSheet;
    }

    renderWorkbookMeta();
    renderSheetTabs();
    renderActiveSheet();
  } catch (error) {
    _sheetNames = [];
    _activeSheet = "";
    _sheetCache.clear();
    renderEmptyState(error.message || "無法讀取目前主檔預覽。");
  }
}

function hydratePreviewLabels() {
  const tabButton = document.querySelector('.tab-btn[data-tab="main-preview"]');
  if (tabButton) tabButton.textContent = "主檔預覽";

  const title = document.getElementById("main-preview-title");
  if (title && !_workbookMeta.filename) {
    title.textContent = "主檔預覽";
  }

  const searchInput = document.getElementById("main-preview-search");
  if (searchInput) searchInput.setAttribute("placeholder", "搜尋料號 / 內容");

  const searchBtn = document.getElementById("btn-main-preview-search");
  if (searchBtn) searchBtn.textContent = "搜尋";

  const clearBtn = document.getElementById("btn-main-preview-clear");
  if (clearBtn) clearBtn.textContent = "清除";

  const refreshBtn = document.getElementById("btn-main-preview-refresh");
  if (refreshBtn) refreshBtn.textContent = "重新整理";

  const stage = document.getElementById("main-preview-stage");
  if (stage?.querySelector(".main-preview-empty")) {
    stage.innerHTML = '<div class="main-preview-empty">主檔已載入後，可在這裡預覽目前的 live 主檔。</div>';
  }
}

function renderLoadingState(message) {
  const stage = document.getElementById("main-preview-stage");
  if (!stage) return;
  stage.innerHTML = `<div class="main-preview-empty">${esc(message)}</div>`;
}

function renderEmptyState(message) {
  updateWorkbookMetaUi("主檔預覽", message, "");
  const tabs = document.getElementById("main-preview-sheet-tabs");
  if (tabs) tabs.innerHTML = "";
  const stage = document.getElementById("main-preview-stage");
  if (stage) stage.innerHTML = `<div class="main-preview-empty">${esc(message)}</div>`;
}

function renderWorkbookMeta() {
  const metaLine = _workbookMeta.loaded_at
    ? `目前顯示的是 live 主檔，載入時間 ${formatLoadedAt(_workbookMeta.loaded_at)}`
    : "目前顯示的是 live 主檔";
  const hintLine = _workbookMeta.style_preserved
    ? "畫面會保留欄寬、合併儲存格與框線，但不套用原始底色，方便直接核對內容。"
    : "這份主檔來自 .xls 來源，畫面會保留結構與欄位，但不保留原始底色。";
  updateWorkbookMetaUi(_workbookMeta.filename || "主檔", metaLine, hintLine);
}

function updateWorkbookMetaUi(title, meta, hint) {
  const titleEl = document.getElementById("main-preview-title");
  const metaEl = document.getElementById("main-preview-meta");
  const hintEl = document.getElementById("main-preview-hint");
  if (titleEl) titleEl.textContent = title || "";
  if (metaEl) metaEl.textContent = meta || "";
  if (hintEl) hintEl.textContent = hint || "";
}

function renderSheetTabs() {
  const container = document.getElementById("main-preview-sheet-tabs");
  if (!container) return;
  container.innerHTML = "";

  _sheetNames.forEach(sheetName => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `main-preview-sheet-tab${sheetName === _activeSheet ? " active" : ""}`;
    button.textContent = sheetName;
    button.addEventListener("click", () => {
      void activateSheet(sheetName);
    });
    container.appendChild(button);
  });
}

async function activateSheet(sheetName) {
  if (!sheetName) return;
  if (_sheetCache.has(sheetName)) {
    _activeSheet = sheetName;
    renderSheetTabs();
    renderActiveSheet();
    return;
  }
  await refreshMainPreview({ sheet: sheetName, eager: true });
}

function renderActiveSheet() {
  const payload = _sheetCache.get(_activeSheet);
  const sheet = payload?.sheet;
  if (!sheet) {
    renderEmptyState("目前沒有可預覽的工作表。");
    return;
  }

  const query = (_currentQuery || "").trim().toLowerCase();
  _searchMatches = query ? collectMatchesForActiveSheet(query) : [];
  if (!_searchMatches.length) {
    _searchCursor = -1;
  } else if (_searchCursor < 0 || _searchCursor >= _searchMatches.length) {
    _searchCursor = 0;
  }

  const matchMap = buildMatchMap(sheet, query);
  const currentMatch = getCurrentMatch();
  const styles = sheet.styles || [];
  const columns = sheet.columns || [];
  const frozenColumnOffsets = buildFrozenColumnOffsets(columns);

  let html = `
    <div class="main-preview-sheet-card">
      <div class="main-preview-sheet-head">
        <div>
          <div class="main-preview-sheet-name">${esc(sheet.name)}</div>
          <div class="main-preview-sheet-meta">${sheet.row_count} 列 × ${sheet.col_count} 欄</div>
        </div>
        <span class="spacer"></span>
        <div class="main-preview-search-status">${esc(buildSearchStatusText())}</div>
      </div>
      <div class="main-preview-grid-wrap">
        <table class="main-preview-grid">
          <colgroup>
            <col style="width:${MAIN_PREVIEW_ROW_HEADER_WIDTH}px">
            ${columns.map(column => `<col style="width:${column.width_px}px">`).join("")}
          </colgroup>
          <thead>
            <tr>
              <th class="main-preview-corner"></th>
              ${columns.map((column, index) => buildColumnHeaderHtml(column, index, frozenColumnOffsets)).join("")}
            </tr>
          </thead>
          <tbody>
  `;

  (sheet.rows || []).forEach(row => {
    const rowClass = matchMap.rowSet.has(row.index) ? "main-preview-row-match" : "";
    html += `<tr class="${rowClass}" data-row-index="${row.index}" style="height:${row.height_px}px">`;
    html += buildRowHeaderHtml(row.index);
    (row.cells || []).forEach(cell => {
      const cellKey = `${row.index}:${cell.col}`;
      const isMatched = matchMap.cellSet.has(cellKey);
      const isFocused = currentMatch && currentMatch.row === row.index && currentMatch.col === cell.col;
      html += buildCellHtml(cell, styles[cell.style_id] || {}, row.index, isMatched, isFocused, frozenColumnOffsets);
    });
    html += "</tr>";
  });

  html += `
          </tbody>
        </table>
      </div>
    </div>
  `;

  const stage = document.getElementById("main-preview-stage");
  if (stage) stage.innerHTML = html;
  scrollToCurrentMatch();
}

function buildFrozenColumnOffsets(columns) {
  const offsets = [];
  let left = MAIN_PREVIEW_ROW_HEADER_WIDTH;

  columns.slice(0, MAIN_PREVIEW_FROZEN_COLUMN_COUNT).forEach(column => {
    offsets.push(left);
    left += Number(column?.width_px || 0);
  });

  return offsets;
}

function buildColumnHeaderHtml(column, index, frozenColumnOffsets) {
  const columnIndex = index + 1;
  const classes = ["main-preview-col-header"];
  const styles = [];

  if (columnIndex <= MAIN_PREVIEW_FROZEN_COLUMN_COUNT) {
    classes.push("is-frozen-col");
    styles.push(`left:${frozenColumnOffsets[columnIndex - 1]}px`);
  }

  return `<th class="${classes.join(" ")}" ${styles.length ? `style="${styles.join(";")}"` : ""}>${column.letter}</th>`;
}

function buildRowHeaderHtml(rowIndex) {
  const classes = ["main-preview-row-header"];
  const styles = [];

  if (rowIndex === 1) {
    classes.push("is-frozen-row", "is-frozen-corner");
    styles.push(`top:${MAIN_PREVIEW_COLUMN_HEADER_HEIGHT}px`);
  }

  return `<th class="${classes.join(" ")}" ${styles.length ? `style="${styles.join(";")}"` : ""}>${rowIndex}</th>`;
}

function buildCellHtml(cell, style, rowIndex, isMatched, isFocused, frozenColumnOffsets = []) {
  const attrs = [
    `data-row-index="${rowIndex}"`,
    `data-col-index="${cell.col}"`,
  ];
  if (cell.rowspan && cell.rowspan > 1) attrs.push(`rowspan="${cell.rowspan}"`);
  if (cell.colspan && cell.colspan > 1) attrs.push(`colspan="${cell.colspan}"`);

  const classes = ["main-preview-cell"];
  const inlineStyles = [styleToCss(style)];
  if (isMatched) classes.push("main-preview-cell-match");
  if (isFocused) classes.push("main-preview-cell-current");
  if (cell.col <= MAIN_PREVIEW_FROZEN_COLUMN_COUNT) {
    classes.push("is-frozen-col");
    inlineStyles.push(`left:${frozenColumnOffsets[cell.col - 1] || MAIN_PREVIEW_ROW_HEADER_WIDTH}px`);
  }
  if (rowIndex === 1) {
    classes.push("is-frozen-row");
    inlineStyles.push(`top:${MAIN_PREVIEW_COLUMN_HEADER_HEIGHT}px`);
  }
  if (rowIndex === 1 && cell.col <= MAIN_PREVIEW_FROZEN_COLUMN_COUNT) {
    classes.push("is-frozen-corner");
  }

  const text = String(cell.value ?? "");
  return `<td class="${classes.join(" ")}" ${attrs.join(" ")} style="${inlineStyles.filter(Boolean).join(";")}" title="${esc(text)}">${renderCellContent(text, style)}</td>`;
}

function renderCellContent(text, style) {
  const safeText = esc(text);
  if (!safeText) return "&nbsp;";
  if (style.wrap) return safeText.replace(/\n/g, "<br>");
  return safeText;
}

function styleToCss(style = {}) {
  const css = [];
  if (style.font_name) css.push(`font-family:'${String(style.font_name).replace(/'/g, "\\'")}', 'Calibri', sans-serif`);
  if (style.font_size) css.push(`font-size:${style.font_size}pt`);
  if (style.bold) css.push("font-weight:700");
  if (style.italic) css.push("font-style:italic");
  if (style.underline) css.push("text-decoration:underline");
  if (style.align) css.push(`text-align:${normalizeAlign(style.align)}`);
  if (style.valign) css.push(`vertical-align:${normalizeVerticalAlign(style.valign)}`);
  if (style.wrap) css.push("white-space:pre-wrap");
  if (style.border_top) css.push(`border-top:${neutralizeBorder(style.border_top)}`);
  if (style.border_right) css.push(`border-right:${neutralizeBorder(style.border_right)}`);
  if (style.border_bottom) css.push(`border-bottom:${neutralizeBorder(style.border_bottom)}`);
  if (style.border_left) css.push(`border-left:${neutralizeBorder(style.border_left)}`);
  return css.join(";");
}

function neutralizeBorder(borderCss = "") {
  return String(borderCss || "").replace(/#[0-9a-fA-F]{6}/g, "#cbd5e1");
}

function normalizeAlign(value) {
  if (value === "centerContinuous" || value === "distributed") return "center";
  if (value === "justify") return "left";
  return value || "left";
}

function normalizeVerticalAlign(value) {
  if (value === "center" || value === "distributed" || value === "justify") return "middle";
  return value || "middle";
}

function runSheetSearch() {
  const input = document.getElementById("main-preview-search");
  const query = (input?.value || "").trim();
  if (!query) {
    clearSheetSearch();
    return;
  }

  const normalized = query.toLowerCase();
  const sameQuery = normalized === _currentQuery;
  _currentQuery = normalized;
  _searchMatches = collectMatchesForActiveSheet(normalized);
  if (!_searchMatches.length) {
    _searchCursor = -1;
    renderActiveSheet();
    showToast(`找不到「${query}」`);
    return;
  }

  _searchCursor = sameQuery && _searchCursor >= 0
    ? (_searchCursor + 1) % _searchMatches.length
    : 0;
  renderActiveSheet();
}

function clearSheetSearch() {
  const input = document.getElementById("main-preview-search");
  if (input) input.value = "";
  _currentQuery = "";
  _searchMatches = [];
  _searchCursor = -1;
  renderActiveSheet();
}

function collectMatchesForActiveSheet(query) {
  const payload = _sheetCache.get(_activeSheet);
  const sheet = payload?.sheet;
  if (!sheet || !query) return [];

  const matches = [];
  (sheet.rows || []).forEach(row => {
    (row.cells || []).forEach(cell => {
      if (String(cell.value || "").toLowerCase().includes(query)) {
        matches.push({ row: row.index, col: cell.col });
      }
    });
  });
  return matches;
}

function buildMatchMap(sheet, query) {
  if (!query) return { rowSet: new Set(), cellSet: new Set() };

  const rowSet = new Set();
  const cellSet = new Set();
  (sheet.rows || []).forEach(row => {
    (row.cells || []).forEach(cell => {
      if (String(cell.value || "").toLowerCase().includes(query)) {
        rowSet.add(row.index);
        cellSet.add(`${row.index}:${cell.col}`);
      }
    });
  });
  return { rowSet, cellSet };
}

function getCurrentMatch() {
  if (_searchCursor < 0 || !_searchMatches.length) return null;
  return _searchMatches[_searchCursor] || null;
}

function scrollToCurrentMatch() {
  const current = getCurrentMatch();
  if (!current) return;
  const selector = `.main-preview-cell[data-row-index="${current.row}"][data-col-index="${current.col}"]`;
  document.querySelector(selector)?.scrollIntoView({ block: "center", inline: "center", behavior: "smooth" });
}

function buildSearchStatusText() {
  if (!_currentQuery) return "可搜尋料號、內容或備註。";
  if (!_searchMatches.length) return "沒有符合的結果。";
  return `搜尋結果 ${_searchCursor + 1} / ${_searchMatches.length}`;
}

function formatLoadedAt(value) {
  if (!value) return "";
  return String(value).replace("T", " ").slice(0, 16);
}

/* ── 雙擊編輯儲存格 ── */

let _editingCell = null;

function startCellEdit(td) {
  if (_editingCell) return;  // 已經在編輯中

  const rowIndex = parseInt(td.dataset.rowIndex, 10);
  const colIndex = parseInt(td.dataset.colIndex, 10);
  if (!rowIndex || !colIndex) return;

  _editingCell = td;
  const originalText = td.title || "";

  // 保留原本尺寸，替換成 input
  const rect = td.getBoundingClientRect();
  const input = document.createElement("input");
  input.type = "text";
  input.value = originalText;
  input.className = "main-preview-cell-input";
  input.style.cssText = `width:${Math.max(rect.width - 4, 40)}px;height:${Math.max(rect.height - 4, 20)}px;`;

  td.textContent = "";
  td.appendChild(input);
  input.focus();
  input.select();

  const commit = async () => {
    const newValue = input.value.trim();
    cleanup();

    if (newValue === originalText.trim()) {
      // 沒改，還原顯示
      restoreCellDisplay(td, originalText);
      return;
    }

    td.textContent = "⏳";
    try {
      const res = await apiFetch("/api/main-file/cell", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sheet: _activeSheet,
          row: rowIndex,
          col: colIndex,
          value: newValue,
        }),
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.detail || "儲存失敗");

      showToast(`已修改 R${rowIndex}C${colIndex}: ${json.old_value} → ${json.new_value}`);
      // 清快取，重新載入當前 sheet
      _sheetCache.delete(_activeSheet);
      await refreshMainPreview({ force: false, sheet: _activeSheet, eager: true });
    } catch (err) {
      showToast(`編輯失敗：${err.message}`, "error");
      restoreCellDisplay(td, originalText);
    }
  };

  const cancel = () => {
    cleanup();
    restoreCellDisplay(td, originalText);
  };

  const cleanup = () => {
    _editingCell = null;
    input.removeEventListener("blur", onBlur);
    input.removeEventListener("keydown", onKeydown);
  };

  const onKeydown = (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      commit();
    } else if (e.key === "Escape") {
      e.preventDefault();
      cancel();
    }
  };

  const onBlur = () => {
    // 短暫延遲避免與 Enter 事件衝突
    setTimeout(() => {
      if (_editingCell === td) commit();
    }, 100);
  };

  input.addEventListener("keydown", onKeydown);
  input.addEventListener("blur", onBlur);
}

function restoreCellDisplay(td, text) {
  td.textContent = text || "\u00a0";
  td.title = text;
}
