import { apiJson, esc, showToast } from "./api.js";

let _sheetCache = new Map();
let _sheetNames = [];
let _activeSheet = "";
let _workbookMeta = { filename: "", loaded_at: "", style_preserved: true };
let _searchMatches = [];
let _searchCursor = -1;
let _currentQuery = "";
let _initialized = false;

export async function initMainPreview() {
  if (_initialized) return;
  _initialized = true;

  document.getElementById("btn-main-preview-refresh")?.addEventListener("click", () => {
    void refreshMainPreview({ force: true, eager: true });
  });
  document.getElementById("btn-main-preview-search")?.addEventListener("click", () => {
    runSheetSearch();
  });
  document.getElementById("btn-main-preview-clear")?.addEventListener("click", () => {
    clearSheetSearch();
  });
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
}

export async function onMainPreviewTabActivated() {
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

  renderLoadingState("正在讀取 live 主檔...");

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
    renderEmptyState(error.message || "尚未載入主檔");
  }
}

function renderLoadingState(message) {
  const stage = document.getElementById("main-preview-stage");
  if (!stage) return;
  stage.innerHTML = `<div class="main-preview-empty">${esc(message)}</div>`;
}

function renderEmptyState(message) {
  updateWorkbookMetaUi("尚未載入主檔", message, "");
  const tabs = document.getElementById("main-preview-sheet-tabs");
  if (tabs) tabs.innerHTML = "";
  const stage = document.getElementById("main-preview-stage");
  if (stage) {
    stage.innerHTML = `<div class="main-preview-empty">${esc(message)}</div>`;
  }
}

function renderWorkbookMeta() {
  const metaLine = _workbookMeta.loaded_at
    ? `目前預覽的是 live 主檔，載入時間 ${formatLoadedAt(_workbookMeta.loaded_at)}`
    : "目前預覽的是 live 主檔";
  const hintLine = _workbookMeta.style_preserved
    ? "樣式會盡量保留原始 Excel 的欄寬、底色、粗體與合併儲存格。"
    : "這份主檔是 .xls，預覽會保留內容與結構，但樣式可能不會完全等同原始 Excel。";
  updateWorkbookMetaUi(_workbookMeta.filename || "主檔預覽", metaLine, hintLine);
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
    renderEmptyState("讀不到主檔工作表");
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
            <col style="width:56px">
            ${columns.map(column => `<col style="width:${column.width_px}px">`).join("")}
          </colgroup>
          <thead>
            <tr>
              <th class="main-preview-corner"></th>
              ${columns.map(column => `<th class="main-preview-col-header">${column.letter}</th>`).join("")}
            </tr>
          </thead>
          <tbody>
  `;

  (sheet.rows || []).forEach(row => {
    const rowClass = matchMap.rowSet.has(row.index) ? "main-preview-row-match" : "";
    html += `<tr class="${rowClass}" data-row-index="${row.index}" style="height:${row.height_px}px">`;
    html += `<th class="main-preview-row-header">${row.index}</th>`;
    (row.cells || []).forEach(cell => {
      const cellKey = `${row.index}:${cell.col}`;
      const isMatched = matchMap.cellSet.has(cellKey);
      const isFocused = currentMatch && currentMatch.row === row.index && currentMatch.col === cell.col;
      html += buildCellHtml(cell, styles[cell.style_id] || {}, row.index, isMatched, isFocused);
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

function buildCellHtml(cell, style, rowIndex, isMatched, isFocused) {
  const attrs = [
    `data-row-index="${rowIndex}"`,
    `data-col-index="${cell.col}"`,
  ];
  if (cell.rowspan && cell.rowspan > 1) attrs.push(`rowspan="${cell.rowspan}"`);
  if (cell.colspan && cell.colspan > 1) attrs.push(`colspan="${cell.colspan}"`);
  const classes = ["main-preview-cell"];
  if (isMatched) classes.push("main-preview-cell-match");
  if (isFocused) classes.push("main-preview-cell-current");
  const text = String(cell.value ?? "");
  return `<td class="${classes.join(" ")}" ${attrs.join(" ")} style="${styleToCss(style)}" title="${esc(text)}">${renderCellContent(text, style)}</td>`;
}

function renderCellContent(text, style) {
  const safeText = esc(text);
  if (!safeText) return "&nbsp;";
  if (style.wrap) return safeText.replace(/\n/g, "<br>");
  return safeText;
}

function styleToCss(style = {}) {
  const css = [];
  if (style.background) css.push(`background:${style.background}`);
  if (style.color) css.push(`color:${style.color}`);
  if (style.font_name) css.push(`font-family:'${String(style.font_name).replace(/'/g, "\\'")}', 'Calibri', sans-serif`);
  if (style.font_size) css.push(`font-size:${style.font_size}pt`);
  if (style.bold) css.push("font-weight:700");
  if (style.italic) css.push("font-style:italic");
  if (style.underline) css.push("text-decoration:underline");
  if (style.align) css.push(`text-align:${normalizeAlign(style.align)}`);
  if (style.valign) css.push(`vertical-align:${normalizeVerticalAlign(style.valign)}`);
  if (style.wrap) css.push("white-space:pre-wrap");
  if (style.border_top) css.push(`border-top:${style.border_top}`);
  if (style.border_right) css.push(`border-right:${style.border_right}`);
  if (style.border_bottom) css.push(`border-bottom:${style.border_bottom}`);
  if (style.border_left) css.push(`border-left:${style.border_left}`);
  return css.join(";");
}

function normalizeAlign(value) {
  if (value === "centerContinuous" || value === "distributed") return "center";
  if (value === "justify") return "left";
  return value || "left";
}

function normalizeVerticalAlign(value) {
  if (value === "center") return "middle";
  if (value === "distributed" || value === "justify") return "middle";
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
    showToast(`主檔內找不到 ${query}`);
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
  if (!_currentQuery) return "可直接搜尋料號、MOQ 或任一儲存格內容";
  if (!_searchMatches.length) return "找不到符合內容";
  return `搜尋結果 ${_searchCursor + 1} / ${_searchMatches.length}`;
}

function formatLoadedAt(value) {
  if (!value) return "";
  return String(value).replace("T", " ").slice(0, 16);
}
