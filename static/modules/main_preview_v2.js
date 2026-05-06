import { apiJson, apiPatch, showToast } from "./api.js";

let _initialized = false;
let _activeSheet = "";
let _sheetNames = [];
let _workbookMeta = { filename: "", loaded_at: "" };
let _isLoading = false;
let _loadToken = 0;
let _luckyMounted = false;
let _suppressNextHook = false;

export function initMainPreviewV2() {
  if (_initialized) return;
  _initialized = true;

  document.getElementById("btn-main-preview-v2-refresh")?.addEventListener("click", () => {
    void refreshMainPreviewV2({ force: true, eager: true });
  });

  document.getElementById("main-preview-v2-sheet-select")?.addEventListener("change", event => {
    const sheet = event.target.value || "";
    if (!sheet || sheet === _activeSheet) return;
    void refreshMainPreviewV2({ force: true, sheet, eager: true });
  });

  document.getElementById("btn-main-preview-v2-scroll-right")?.addEventListener("click", () => {
    if (!_luckyMounted || !window.luckysheet) return;
    try {
      const ls = window.luckysheet;
      const sheets = ls.getAllSheets ? ls.getAllSheets() : [];
      const active = sheets.find(s => s.status === 1) || sheets[0];
      const colCount = active?.column || active?.celldata?.reduce((m, c) => Math.max(m, (c.c ?? 0) + 1), 0) || 0;
      if (colCount > 0 && ls.setRangeShow) {
        ls.setRangeShow({ row: [0, 0], column: [colCount - 1, colCount - 1] });
      } else if (ls.scroll) {
        ls.scroll({ scrollLeft: Number.MAX_SAFE_INTEGER });
      }
    } catch (err) {
      console.error("[main_preview_v2] scrollToRight failed", err);
    }
  });
}

export async function onMainPreviewV2TabActivated() {
  await refreshMainPreviewV2({ eager: true });
}

export async function refreshMainPreviewV2({ force = false, sheet = "", eager = false } = {}) {
  const panel = document.getElementById("tab-main-preview-v2");
  const isActive = panel?.classList.contains("active");
  if (!isActive && !eager) return;
  if (_isLoading && !force) return;

  const stage = document.getElementById("main-preview-v2-stage");
  if (!stage) return;

  if (typeof window.luckysheet === "undefined") {
    renderStatus("Luckysheet 尚未載入完成，請稍候再試。");
    return;
  }

  const token = ++_loadToken;
  _isLoading = true;
  renderStatus("正在載入目前 live 主檔...");

  try {
    const requestedSheet = sheet || _activeSheet || "";
    const url = new URL("/api/main-file/preview", window.location.origin);
    if (requestedSheet) url.searchParams.set("sheet", requestedSheet);

    const payload = await apiJson(`${url.pathname}${url.search}`);
    if (token !== _loadToken) return;

    _workbookMeta = {
      filename: payload.filename || "",
      loaded_at: payload.loaded_at || "",
    };
    _sheetNames = payload.sheet_names || [];
    _activeSheet = payload.selected_sheet || payload.sheet?.name || requestedSheet || _sheetNames[0] || "";

    renderSheetSelect();
    mountLuckysheet(payload);
    renderMeta(payload.sheet);
  } catch (error) {
    if (token !== _loadToken) return;
    renderStatus(error.message || "讀取主檔失敗");
  } finally {
    if (token === _loadToken) _isLoading = false;
  }
}

function mountLuckysheet(payload) {
  const stage = document.getElementById("main-preview-v2-stage");
  if (!stage) return;

  const sheet = payload.sheet || {};
  const celldata = buildCelldata(sheet);
  const rowCount = Math.max(sheet.row_count || sheet.rows?.length || 100, 100);
  const colCount = Math.max(sheet.col_count || (sheet.columns?.length) || 26, 26);

  if (_luckyMounted && window.luckysheet?.destroy) {
    try { window.luckysheet.destroy(); } catch (_) {}
  }

  stage.innerHTML = "";
  stage.style.minHeight = "600px";

  _suppressNextHook = true;

  window.luckysheet.create({
    container: "main-preview-v2-stage",
    title: _workbookMeta.filename || "主檔預覽 v2",
    lang: "zh",
    showinfobar: false,
    showsheetbar: false,
    showstatisticBar: false,
    enableAddRow: false,
    enableAddBackTop: false,
    showtoolbarConfig: {
      undoRedo: true,
      paintFormat: false,
      currencyFormat: false,
      percentageFormat: false,
      numberDecrease: false,
      numberIncrease: false,
      moreFormats: false,
      font: false,
      fontSize: false,
      bold: false,
      italic: false,
      strikethrough: false,
      underline: false,
      textColor: false,
      fillColor: false,
      border: false,
      mergeCell: false,
      horizontalAlignMode: false,
      verticalAlignMode: false,
      textWrapMode: false,
      textRotateMode: false,
      image: false,
      link: false,
      chart: false,
      postil: false,
      pivotTable: false,
      function: false,
      frozenMode: true,
      sortAndFilter: true,
      conditionalFormat: false,
      dataVerification: false,
      splitColumn: false,
      screenshot: false,
      findAndReplace: true,
      protection: false,
      print: false,
    },
    data: [{
      name: sheet.name || _activeSheet || "Sheet1",
      celldata,
      row: rowCount,
      column: colCount,
      config: {
        rowlen: buildRowConfig(sheet),
      },
      status: 1,
      order: 0,
      frozen: {
        type: "rangeBoth",
        range: { row_focus: 0, column_focus: 1 },
      },
    }],
    hook: {
      cellUpdated: function (r, c, oldValue, newValue) {
        if (_suppressNextHook) {
          _suppressNextHook = false;
          return;
        }
        const row = r + 1;
        const col = c + 1;
        let value = "";
        if (newValue && typeof newValue === "object") {
          value = (newValue.v ?? newValue.m ?? "").toString();
        } else if (newValue !== null && newValue !== undefined) {
          value = String(newValue);
        }
        apiPatch("/api/main-file/cell", {
          sheet: _activeSheet,
          row,
          col,
          value,
        }).then(json => {
          if (json?.ok) {
            showToast(`已修改 R${row}C${col}: ${json.old_value ?? ""} → ${json.new_value ?? value}`);
          } else {
            showToast(`儲存失敗：${json?.detail || ""}`, "error");
          }
        }).catch(err => {
          showToast(`儲存失敗：${err.message}`, "error");
        });
      },
      workbookCreateAfter: function () {
        _luckyMounted = true;
        _suppressNextHook = false;
      },
    },
  });
}

function buildCelldata(sheet) {
  const rows = sheet?.rows || [];
  const result = [];
  for (const rowEntry of rows) {
    const rowIndex = (rowEntry?.index || 1) - 1;
    const cells = rowEntry?.cells || [];
    for (const cell of cells) {
      const colIndex = (cell?.col || 1) - 1;
      const rawValue = cell?.value;
      if (rawValue === null || rawValue === undefined || rawValue === "") continue;
      const cellValue = {
        v: rawValue,
        m: String(rawValue),
      };
      if (rowIndex === 0) {
        cellValue.tb = "2";
      }
      result.push({
        r: rowIndex,
        c: colIndex,
        v: cellValue,
      });
    }
  }
  return result;
}

function buildRowConfig(sheet) {
  const rows = sheet?.rows || [];
  const rowlen = {};
  if (rows.length > 0) {
    rowlen[0] = 80;
  }
  return rowlen;
}

function renderSheetSelect() {
  const sel = document.getElementById("main-preview-v2-sheet-select");
  if (!sel) return;
  sel.innerHTML = "";
  for (const name of _sheetNames) {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    if (name === _activeSheet) opt.selected = true;
    sel.appendChild(opt);
  }
}

function renderMeta(sheet) {
  const meta = document.getElementById("main-preview-v2-meta");
  if (!meta) return;
  const rows = sheet?.row_count ?? sheet?.rows?.length ?? 0;
  const cols = sheet?.col_count ?? sheet?.columns?.length ?? 0;
  const parts = [];
  if (_workbookMeta.filename) parts.push(_workbookMeta.filename);
  parts.push(`共 ${rows} 列 × ${cols} 欄`);
  if (_workbookMeta.loaded_at) parts.push(`載入於 ${_workbookMeta.loaded_at}`);
  meta.textContent = parts.join("　");

  const titleEl = document.getElementById("main-preview-v2-title");
  if (titleEl) {
    titleEl.textContent = _workbookMeta.filename || "主檔預覽 v2";
  }
}

function renderStatus(text) {
  const stage = document.getElementById("main-preview-v2-stage");
  if (!stage) return;
  stage.innerHTML = `<div class="main-preview-v2-empty">${text || ""}</div>`;
}
