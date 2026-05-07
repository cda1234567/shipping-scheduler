import { apiJson, apiPatch, showToast } from "./api.js";
import { refresh as refreshSchedule } from "./schedule.js";

let _initialized = false;
let _activeSheet = "";
let _sheetNames = [];
let _workbookMeta = { filename: "", loaded_at: "" };
let _isLoading = false;
let _loadToken = 0;
let _luckyMounted = false;
let _suppressNextHook = false;
let _suppressProgrammaticUpdates = 0;
let _lastLoadedAt = "";
let _partLocations = {};
let _batchLocations = {};

export async function navigateToPart(partNumber, batchCode = "") {
  const key = String(partNumber || "").trim().toUpperCase();
  if (!key) return false;
  if (!_luckyMounted || !window.luckysheet) {
    await refreshMainPreviewV2({ eager: true, force: false });
  }
  const rowIndex = _partLocations[key];
  if (rowIndex === undefined) return false;
  try {
    const ls = window.luckysheet;
    const sheet = ls.getAllSheets ? ls.getAllSheets()[0] : null;
    const cells = sheet?.celldata || [];
    let lastCol = 0;
    const code = String(batchCode || "").trim();
    if (code && _batchLocations[code] !== undefined) {
      lastCol = _batchLocations[code] + 2;
    } else {
      for (const cell of cells) {
        if ((cell.r ?? -1) === rowIndex && (cell.c ?? -1) > lastCol && cell.v?.v != null && cell.v?.v !== "") {
          lastCol = cell.c;
        }
      }
    }
    if (ls.setRangeShow) {
      ls.setRangeShow({ row: [rowIndex, rowIndex], column: [lastCol, lastCol] });
    }
    // setRangeShow 不會自動捲到位置，手動設 DOM scrollTop/Left
    const stage = document.getElementById("main-preview-v2-stage");
    const scrollY = stage?.querySelector(".luckysheet-scrollbar-y");
    const scrollX = stage?.querySelector(".luckysheet-scrollbar-x");
    if (scrollY) {
      const rowConfig = sheet?.config?.rowlen || {};
      let top = 0;
      for (let i = 0; i < rowIndex; i++) top += Number(rowConfig[i] || 19);
      scrollY.scrollTop = Math.max(0, top - 100);
    }
    if (scrollX && lastCol > 0) {
      const colConfig = sheet?.config?.columnlen || {};
      let left = 0;
      for (let i = 0; i < lastCol; i++) left += Number(colConfig[i] || 73);
      scrollX.scrollLeft = Math.max(0, left - 200);
    }
    return true;
  } catch (err) {
    console.error("[main_preview_v2] navigateToPart failed", err);
    return false;
  }
}

const LS_WRAP_KEY = "mainPreviewV2.headerWrap";
const LS_COLLEN_PREFIX = "mainPreviewV2.columnlen.";
const LS_ROWLEN_PREFIX = "mainPreviewV2.rowlen.";
let _wrapEnabled = (() => {
  try { return localStorage.getItem(LS_WRAP_KEY) !== "0"; } catch (_) { return true; }
})();

function loadLayout(sheetName) {
  const safe = (key, fallback) => {
    try { return JSON.parse(localStorage.getItem(key) || "null") ?? fallback; } catch (_) { return fallback; }
  };
  return {
    columnlen: safe(LS_COLLEN_PREFIX + sheetName, {}),
    rowlen: safe(LS_ROWLEN_PREFIX + sheetName, {}),
  };
}

function saveLayoutPart(prefix, sheetName, idx, value) {
  if (!sheetName) return;
  const key = prefix + sheetName;
  let obj = {};
  try { obj = JSON.parse(localStorage.getItem(key) || "{}") || {}; } catch (_) { obj = {}; }
  obj[idx] = value;
  try { localStorage.setItem(key, JSON.stringify(obj)); } catch (_) {}
}

function updateWrapButtonLabel() {
  const btn = document.getElementById("btn-main-preview-v2-toggle-wrap");
  if (btn) btn.textContent = `標題列換行: ${_wrapEnabled ? "開" : "關"}`;
}

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

  updateWrapButtonLabel();
  document.getElementById("btn-main-preview-v2-toggle-wrap")?.addEventListener("click", () => {
    _wrapEnabled = !_wrapEnabled;
    try { localStorage.setItem(LS_WRAP_KEY, _wrapEnabled ? "1" : "0"); } catch (_) {}
    updateWrapButtonLabel();
    void refreshMainPreviewV2({ force: true, eager: true });
  });

  document.getElementById("btn-main-preview-v2-scroll-right")?.addEventListener("click", () => {
    if (!_luckyMounted || !window.luckysheet) return;
    try {
      const stage = document.getElementById("main-preview-v2-stage");
      const scrollEl = stage?.querySelector(".luckysheet-scrollbar-x");
      if (scrollEl) {
        scrollEl.scrollLeft = scrollEl.scrollWidth;
      }
      if (window.luckysheet?.scroll) {
        window.luckysheet.scroll({ scrollLeft: Number.MAX_SAFE_INTEGER });
      }
    } catch (err) {
      console.error("[main_preview_v2] scrollToRight failed", err);
    }
  });
}

export async function onMainPreviewV2TabActivated() {
  // 已 mount 過 → 快速 check 主檔是否變動，沒變就跳過重 mount
  if (_luckyMounted && _lastLoadedAt) {
    try {
      const url = new URL("/api/main-file/preview", window.location.origin);
      if (_activeSheet) url.searchParams.set("sheet", _activeSheet);
      const payload = await apiJson(`${url.pathname}${url.search}`);
      if ((payload?.loaded_at || "") === _lastLoadedAt) return;
    } catch (_) {}
  }
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
    _lastLoadedAt = payload.loaded_at || "";
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

  _partLocations = {};
  _batchLocations = {};
  const batchCodeRe = /^\d+-\d+$/;
  for (const rowEntry of (sheet?.rows || [])) {
    const rowIndex = (rowEntry?.index || 1) - 1;
    if (rowIndex === 0) {
      // 第 1 列：找批次 code → col 起點
      for (const cell of (rowEntry?.cells || [])) {
        const value = String(cell?.value || "").trim();
        if (batchCodeRe.test(value)) {
          const col0 = (cell.col || 1) - 1;
          if (!(value in _batchLocations)) _batchLocations[value] = col0;
        }
      }
      continue;
    }
    const partCell = (rowEntry?.cells || []).find(c => (c?.col || 0) === 1);
    const partKey = String(partCell?.value || "").trim().toUpperCase();
    if (!partKey || partKey in _partLocations) continue;
    _partLocations[partKey] = rowIndex;
  }
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
      config: (() => {
        const layout = loadLayout(_activeSheet);
        const rowlen = { ...buildRowConfig(sheet), ...(layout.rowlen || {}) };
        return {
          rowlen,
          columnlen: layout.columnlen || {},
        };
      })(),
      status: 1,
      order: 0,
      frozen: {
        type: "rangeBoth",
        range: { row_focus: 0, column_focus: 1 },
      },
    }],
    hook: {
      cellUpdated: function (r, c, oldValue, newValue) {
        if (_suppressProgrammaticUpdates > 0) {
          _suppressProgrammaticUpdates -= 1;
          return;
        }
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
        }).then(async json => {
          if (json?.ok) {
            const affectedCells = Array.isArray(json.affected_cells) ? json.affected_cells : [];
            if (affectedCells.length > 0 && window.luckysheet?.setCellValue) {
              _suppressProgrammaticUpdates += affectedCells.length;
              for (const cell of affectedCells) {
                window.luckysheet.setCellValue(
                  Number(cell.row || 1) - 1,
                  Number(cell.col || 1) - 1,
                  cell.value ?? "",
                );
              }
            }
            let scheduleRefreshMessage = "";
            if (json.schedule_refresh_required === true) {
              try {
                await refreshSchedule();
                scheduleRefreshMessage = "，排程缺料已重新計算";
              } catch (err) {
                console.error("[main_preview_v2] refresh schedule failed", err);
                scheduleRefreshMessage = "，但排程重新整理失敗";
              }
            }
            if (affectedCells.length > 0) {
              showToast(`已寫入並重算 ${affectedCells.length} 個結餘 cell${scheduleRefreshMessage}`);
            } else {
              showToast(`已修改 R${row}C${col}: ${json.old_value ?? ""} → ${json.new_value ?? value}${scheduleRefreshMessage}`);
            }
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
        // 鍵盤左推到凍結邊界時，畫面跟著往左捲（避免 cursor 被凍結 A/B 欄壓住）
        const stage2 = document.getElementById("main-preview-v2-stage");
        if (stage2 && !stage2.dataset.keyboardScrollBound) {
          stage2.dataset.keyboardScrollBound = "1";
          stage2.addEventListener("keydown", (e) => {
            if (e.key !== "ArrowLeft" || e.ctrlKey || e.metaKey || e.altKey || e.shiftKey) return;
            setTimeout(() => {
              try {
                const ls = window.luckysheet;
                const sh = ls?.getAllSheets?.()[0];
                if (!sh) return;
                const sel = (sh.luckysheet_select_save || [])[0];
                if (!sel) return;
                const colSelected = Array.isArray(sel.column) ? sel.column[0] : sel.column;
                const frozenCol = sh.frozen?.range?.column_focus ?? -1;
                if (colSelected <= frozenCol + 1) {
                  // 進入凍結邊界，往左捲一格
                  const scrollX = stage2.querySelector(".luckysheet-scrollbar-x");
                  if (scrollX && scrollX.scrollLeft > 0) {
                    const colConfig = sh.config?.columnlen || {};
                    const step = Number(colConfig[colSelected] || 73);
                    scrollX.scrollLeft = Math.max(0, scrollX.scrollLeft - step);
                  }
                }
              } catch (_) {}
            }, 30);
          });
        }
        // Luckysheet 沒 column/row resize hook，改用 mouseup 後 dump 整個 sheet
        // 的 columnlen / rowlen 到 localStorage（debounce 200ms）
        const stage = document.getElementById("main-preview-v2-stage");
        if (stage && !stage.dataset.layoutListenerBound) {
          stage.dataset.layoutListenerBound = "1";
          let timer = null;
          stage.addEventListener("mouseup", () => {
            if (timer) clearTimeout(timer);
            timer = setTimeout(() => {
              try {
                const ls = window.luckysheet;
                const sh = ls?.getAllSheets?.()[0];
                if (!sh || !_activeSheet) return;
                if (sh.config?.columnlen) {
                  localStorage.setItem(LS_COLLEN_PREFIX + _activeSheet, JSON.stringify(sh.config.columnlen));
                }
                if (sh.config?.rowlen) {
                  localStorage.setItem(LS_ROWLEN_PREFIX + _activeSheet, JSON.stringify(sh.config.rowlen));
                }
              } catch (_) {}
            }, 200);
          });
        }
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
      if (rowIndex === 0 && _wrapEnabled) {
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
  if (rows.length > 0 && _wrapEnabled) {
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
