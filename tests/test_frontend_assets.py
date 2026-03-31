from __future__ import annotations

import json
import re
import subprocess
import unittest
from pathlib import Path


class FrontendAssetTests(unittest.TestCase):
    def test_schedule_module_has_no_duplicate_top_level_function_declarations(self):
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        pattern = re.compile(r"^(?:export\s+)?(?:async\s+)?function\s+([A-Za-z0-9_]+)\s*\(", re.M)
        seen: dict[str, int] = {}
        duplicates: dict[str, list[int]] = {}

        for match in pattern.finditer(text):
            name = match.group(1)
            line = text.count("\n", 0, match.start()) + 1
            if name in seen:
                duplicates.setdefault(name, [seen[name]]).append(line)
                continue
            seen[name] = line

        self.assertEqual(duplicates, {})

    def test_active_build_row_card_renders_draft_panel_html(self):
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        match = re.search(
            r"function\s+buildRowCard\s*\(r,\s*resultMap\)\s*\{(?P<body>.*?)\n\}",
            text,
            re.S,
        )

        self.assertIsNotNone(match)
        self.assertIn("${draftHtml}", match.group("body"))
        self.assertNotIn('<button class="btn-complete"', match.group("body"))
        self.assertIn("const selectedFileId = getSelectedDraftFileId(draft.id, draft.files || []);", match.group("body"))
        self.assertIn('void showDraftModal(draft.id, { readOnly: false, fileId: selectedFileId });', match.group("body"))

    def test_schedule_module_supports_collapsible_draft_panel_and_preview_rows(self):
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        self.assertIn("btn-draft-toggle", text)
        self.assertIn("preview_rows", text)
        self.assertIn("draft-preview-inline-stat", text)
        self.assertIn("draft-preview-editors", text)
        self.assertIn('closest(".draft-preview-row, .shortage-item")', text)
        self.assertIn("merge-draft-file-select", text)
        self.assertIn("getSelectedDraftFileId", text)
        self.assertIn("指定副檔預覽", text)
        self.assertIn("buildDraftFileListHtml", text)
        self.assertIn("merge-draft-file-strip", text)
        self.assertIn("btn-draft-commit", text)
        self.assertIn("getEffectiveShortageState", text)
        self.assertIn("buildRightPanelShortageData", text)
        self.assertIn("draft.shortages || []", text)
        self.assertIn("withGlobalBusy(", text)
        self.assertIn("showMainWriteBlockedNotice", text)
        self.assertIn("action-busy-overlay", text)
        self.assertIn('setModalDownloadProgress(', text)
        self.assertIn('{ tone: "error", lockUi: false }', text)
        self.assertIn("Object.prototype.hasOwnProperty.call(_draftPanelCollapsedState, key)", text)
        self.assertIn("return true;", text)
        self.assertNotIn("const selectedFile = readOnly && selectedFileId", text)
        self.assertIn("_modalDraftBaseSupplements = normalizeSupplementMap(draft.supplements || {});", text)
        self.assertIn("_modalBomFiles = files;", text)
        self.assertIn('setModalDownloadProgress(true, "正在儲存副檔..."', text)
        self.assertIn('showToast("副檔已更新");', text)
        self.assertIn('saveBtn.dataset.idleText', text)
        self.assertIn('saveBtn.dataset.busyText', text)
        self.assertIn('id="modal-download-progress"', text)

        preview_match = re.search(
            r"function\s+buildDraftPreviewRowHtml\s*\(row,\s*\{\s*editable\s*=\s*false\s*\}\s*=\s*\{\}\)\s*\{(?P<body>.*?)\n\}",
            text,
            re.S,
        )

        self.assertIsNotNone(preview_match)
        self.assertNotIn('draftInlineStatHtml("CS"', preview_match.group("body"))
        self.assertNotIn("s.default_supplement ?? s.supplement_qty ?? s.suggested_qty ?? s.shortage_amount ?? 0", text)
        self.assertIn("Number(s.default_supplement) > 0", text)
        self.assertIn("Number(s.suggested_qty) > 0", text)

    def test_collapsed_draft_panel_uses_compact_half_width_layout(self):
        stylesheet = (Path(__file__).resolve().parents[1] / "static" / "style.css").read_text(encoding="utf-8")

        self.assertIn(".merge-draft-panel.is-collapsed {", stylesheet)
        self.assertIn("width: min(21%, 215px);", stylesheet)
        self.assertIn("padding: 5px 8px;", stylesheet)
        self.assertIn(".merge-draft-panel.is-collapsed .btn-draft-toggle {", stylesheet)
        self.assertIn("min-height: 24px;", stylesheet)
        self.assertIn("font-size: 10px;", stylesheet)

    def test_schedule_module_marks_st_purchase_state_with_dedicated_visuals(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")
        stylesheet = (root / "static" / "style.css").read_text(encoding="utf-8")

        self.assertIn('apiJson("/api/system/st-inventory/data")', schedule_module)
        self.assertIn("function loadStInventoryData()", schedule_module)
        self.assertIn("function shortageToneClass(", schedule_module)
        self.assertIn("function stSupplySummaryHtml(", schedule_module)
        self.assertIn("purchase_needed_qty", schedule_module)
        self.assertIn("purchase_suggested_qty", schedule_module)
        self.assertIn("st_available_qty", schedule_module)
        self.assertIn("is-st-purchase", schedule_module)
        self.assertIn("ST 可補", schedule_module)
        self.assertIn("需買", schedule_module)
        self.assertNotIn("s.purchase_needed_qty ?? s.shortage_amount ?? 0", schedule_module)

        self.assertIn(".shortage-item.is-st-purchase", stylesheet)
        self.assertIn("body.desktop-dark .shortage-item.is-st-purchase", stylesheet)

    def test_schedule_module_allows_main_file_deficits_to_supplement_from_right_panel(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("function renderMainDeficitSectionHtml(deficits)", schedule_module)
        self.assertIn('data-main-supplement="true"', schedule_module)
        self.assertIn("直接補到主檔，後續同料號欄位會一起同步加回", schedule_module)
        self.assertIn("right-panel-supplement-save", schedule_module)
        self.assertIn("right-panel-supplement-input", schedule_module)
        self.assertIn('const isMainSupplement = String(button?.dataset.mainSupplement || input?.dataset.mainSupplement || "").trim() === "true";', schedule_module)
        self.assertIn('await apiPost("/api/schedule/supplement-part"', schedule_module)
        self.assertIn('_postDispatchShortages = _postDispatchShortages.filter(item => normalizePartKey(item?.part_number) !== part);', schedule_module)

    def test_schedule_module_auto_checks_shortage_for_negative_carry_over(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("function shouldAutoShortageCheck(item)", schedule_module)
        self.assertIn("const carryOver = Number(item?.carry_over);", schedule_module)
        self.assertIn("const currentStock = Number(item?.current_stock);", schedule_module)
        self.assertIn("const shortageChecked = shouldAutoShortageCheck(row);", schedule_module)
        self.assertIn("const shortageChecked = shouldAutoShortageCheck(s);", schedule_module)

    def test_batch_merge_modal_rebuilds_raw_shortages_before_reapplying_stored_inputs(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("function buildRawModalShortageGroups(targets)", schedule_module)
        self.assertIn("calculate(targetRows, _bomData, _stock, _moq, _dispatchedConsumption, _stStock, {})", schedule_module)

        batch_modal_match = re.search(
            r"async function showBatchMergeDraftModal\(targets\) \{(?P<body>.*?)\n\}",
            schedule_module,
            re.S,
        )
        self.assertIsNotNone(batch_modal_match)
        batch_body = batch_modal_match.group("body")
        self.assertIn("buildRawModalShortageGroups(targets)", batch_body)
        self.assertNotIn("_calcResults.forEach", batch_body)

        shortage_modal_match = re.search(
            r"async function showShortageModal\(targets\) \{(?P<body>.*?)\n\}",
            schedule_module,
            re.S,
        )
        self.assertIsNotNone(shortage_modal_match)
        shortage_body = shortage_modal_match.group("body")
        self.assertIn("buildRawModalShortageGroups(targets)", shortage_body)
        self.assertNotIn("_calcResults.forEach", shortage_body)

    def test_batch_merge_rebuilds_all_checked_pending_or_merged_targets_in_current_order(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        match = re.search(
            r"async function handleBatchMerge\(\) \{(?P<body>.*?)\n\}",
            schedule_module,
            re.S,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn("if (_batchMergeInFlight) {", body)
        self.assertIn('showToast("批次 merge 進行中，請稍候");', body)
        self.assertIn("closeShortageModal();", body)
        self.assertIn("await waitForNextFrame();", body)
        self.assertIn("const selectedRows = _rows.filter(row => _checkedIds.has(row.id));", body)
        self.assertIn('const targets = selectedRows.filter(row => row.status === "pending" || row.status === "merged");', body)
        self.assertNotIn("勾選的訂單已經有副檔，請直接在訂單下方副檔工作台修改", body)
        self.assertIn("const currentOrderIds = _rows.map(row => row.id).filter(Number.isInteger);", body)
        self.assertIn('await apiPost("/api/schedule/reorder", { order_ids: currentOrderIds });', body)
        self.assertIn("await openBatchMergeDraftModalStable(targetIds, targets);", body)
        self.assertIn("function waitForNextFrame()", schedule_module)
        self.assertIn("function buildBatchMergeModalTargets(targetIds, fallbackTargets = [])", schedule_module)
        self.assertIn("async function openBatchMergeDraftModalStable(targetIds, fallbackTargets = [])", schedule_module)
        self.assertIn("await waitForNextFrame();", schedule_module)
        self.assertIn('if (modal?.style.display !== "flex") {', schedule_module)
        self.assertIn('throw new Error("補料 modal 沒有成功顯示");', schedule_module)
        self.assertIn("_modalDraftBaseDecisions = {};", schedule_module)
        self.assertIn("_modalDraftBaseSupplements = {};", schedule_module)
        self.assertIn("_modalDraftVisibleParts = [];", schedule_module)
        self.assertIn('if (list) list.innerHTML = "";', schedule_module)
        self.assertIn('if (footer) footer.innerHTML = "";', schedule_module)

    def test_frontend_calculator_keeps_order_scoped_ic_shortages_per_current_order(self):
        root = Path(__file__).resolve().parents[1]
        script = """
import { calculate } from './static/modules/calculator.js';

const results = calculate(
  [
    { id: 1, po_number: 2001, pcb: 'F', model: 'MODEL-F1' },
    { id: 2, po_number: 2002, pcb: 'G', model: 'MODEL-F2' },
  ],
  {
    'MODEL-F1': {
      components: [
        { part_number: 'IC-STM32F', description: 'STM MCU', needed_qty: 100, prev_qty_cs: 0, is_dash: false },
      ],
    },
    'MODEL-F2': {
      components: [
        { part_number: 'IC-STM32F', description: 'STM MCU', needed_qty: 50, prev_qty_cs: 0, is_dash: false },
      ],
    },
  },
  { 'IC-STM32F': 0 },
  { 'IC-STM32F': 100 },
  {},
  { 'IC-STM32F': 80 },
  {},
);

console.log(JSON.stringify(results));
"""
        completed = subprocess.run(
            ["node", "--input-type=module", "-e", script],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        results = json.loads(completed.stdout)

        first_shortage = results[0]["shortages"][0]
        second_shortage = results[1]["shortages"][0]
        self.assertEqual(first_shortage["shortage_amount"], 100)
        self.assertEqual(first_shortage["suggested_qty"], 100)
        self.assertEqual(first_shortage["purchase_suggested_qty"], 20)
        self.assertEqual(second_shortage["shortage_amount"], 50)
        self.assertEqual(second_shortage["suggested_qty"], 50)
        self.assertEqual(second_shortage["purchase_suggested_qty"], 0)

    def test_schedule_module_does_not_auto_mark_order_scoped_ic_parts_as_shortage_from_prior_negative(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("if (isOrderScopedPart(item?.part_number)) return false;", schedule_module)
        self.assertIn("const hasStoredSupplement = Number(s.default_supplement) > 0 || Number(s.supplement_qty) > 0;", schedule_module)
        self.assertIn("const defaultQty = shortageChecked && !hasStoredSupplement", schedule_module)

    def test_defectives_tab_auto_collapses_batch_history_on_activation(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        defectives_module = (root / "static" / "modules" / "defectives.js").read_text(encoding="utf-8")

        self.assertIn('import { initDefectives, refreshDefectives, onDefectivesTabActivated } from "/static/modules/defectives.js";', index_html)
        self.assertIn('if (btn.dataset.tab === "defectives-tab") void onDefectivesTabActivated();', index_html)
        self.assertIn("export async function onDefectivesTabActivated()", defectives_module)
        self.assertIn("await refreshDefectives({ collapseAll: true });", defectives_module)
        self.assertIn("const collapseAll = Boolean(options?.collapseAll);", defectives_module)
        self.assertIn("_collapsed.clear();", defectives_module)
        self.assertIn("_batches.forEach(batch => _collapsed.add(batch.id));", defectives_module)

    def test_desktop_bridge_uses_regular_browser_downloads_for_web_mode(self):
        root = Path(__file__).resolve().parents[1]
        bridge_module = (root / "static" / "modules" / "desktop_bridge.js").read_text(encoding="utf-8")
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")

        self.assertIn('id="desktop-choose-download-dir"', index_html)
        self.assertIn('id="desktop-clear-download-dir"', index_html)
        self.assertIn('id="desktop-download-folder"', index_html)
        self.assertIn('id="desktop-download-note"', index_html)
        self.assertIn('id="desktop-modal-title"', index_html)
        self.assertIn('id="desktop-modal-subtitle"', index_html)
        self.assertIn("handleChooseDownloadDirectory()", bridge_module)
        self.assertIn("handleChooseDesktopDownloadDirectory()", bridge_module)
        self.assertIn("handleClearDownloadDirectory()", bridge_module)
        self.assertIn("網頁版下載會交給瀏覽器處理；如要變更位置，請使用瀏覽器本身的下載設定。", bridge_module)
        self.assertIn('desktopChooseBtn.style.display = desktopAvailable ? "inline-flex" : "none";', bridge_module)
        self.assertIn('desktopClearBtn.style.display = desktopAvailable ? "inline-flex" : "none";', bridge_module)
        self.assertIn('folderEl.textContent = "使用瀏覽器預設下載位置";', bridge_module)
        self.assertIn('folderNoteEl.textContent = "如要每次自行選位置，請在瀏覽器開啟「下載前一律詢問儲存位置」。";', bridge_module)
        self.assertIn("網頁版下載交由瀏覽器處理；如要每次選位置，請在瀏覽器開啟「下載前一律詢問儲存位置」。", bridge_module)
        self.assertIn("網頁版沒有系統內的下載資料夾設定可清除。", bridge_module)
        self.assertIn("document.body.appendChild(anchor);", bridge_module)
        self.assertIn("window.setTimeout(() => {", bridge_module)
        self.assertIn("URL.revokeObjectURL(blobUrl);", bridge_module)
        self.assertIn("anchor.remove();", bridge_module)
        self.assertIn("browser_download_started: true", bridge_module)
        self.assertNotIn('id="browser-choose-download-dir"', index_html)

    def test_schedule_init_only_binds_listeners_once(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("let _scheduleInitialized = false;", schedule_module)
        self.assertIn("if (!_scheduleInitialized) {", schedule_module)
        self.assertIn("_scheduleInitialized = true;", schedule_module)

    def test_st_inventory_upload_assets_exist_for_sidebar_panel(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        st_module = (root / "static" / "modules" / "st_inventory.js").read_text(encoding="utf-8")
        stylesheet = (root / "static" / "style.css").read_text(encoding="utf-8")

        self.assertIn('id="st-inventory-status"', index_html)
        self.assertIn('id="st-inventory-meta"', index_html)
        self.assertIn('id="btn-upload-st-inventory"', index_html)
        self.assertIn('id="btn-refresh-st-inventory"', index_html)
        self.assertIn('id="st-inventory-file-input"', index_html)
        self.assertIn('import { initStInventory } from "/static/modules/st_inventory.js";', index_html)
        self.assertIn("await initStInventory({ onChanged: async () => { await refreshSchedule(); } });", index_html)

        self.assertIn('apiJson("/api/system/st-inventory/info")', st_module)
        self.assertIn('apiFetch("/api/system/st-inventory/upload"', st_module)
        self.assertIn('document.getElementById("btn-upload-st-inventory")', st_module)
        self.assertIn('document.getElementById("st-inventory-file-input")', st_module)
        self.assertIn("ST 庫存已匯入", st_module)

        self.assertIn(".sidebar-meta", stylesheet)
        self.assertIn("body.desktop-dark .sidebar-meta", stylesheet)

    def test_main_preview_supports_freeze_panes_for_row_one_and_columns_abc(self):
        root = Path(__file__).resolve().parents[1]
        preview_module = (root / "static" / "modules" / "main_preview.js").read_text(encoding="utf-8")
        stylesheet = (root / "static" / "style.css").read_text(encoding="utf-8")

        self.assertIn("MAIN_PREVIEW_FROZEN_COLUMN_COUNT = 3", preview_module)
        self.assertIn("MAIN_PREVIEW_COLUMN_HEADER_HEIGHT = 30", preview_module)
        self.assertIn("buildFrozenColumnOffsets", preview_module)
        self.assertIn("is-frozen-col", preview_module)
        self.assertIn("is-frozen-row", preview_module)
        self.assertIn("top:${MAIN_PREVIEW_COLUMN_HEADER_HEIGHT}px", preview_module)

        self.assertIn(".main-preview-col-header.is-frozen-col", stylesheet)
        self.assertIn(".main-preview-row-header.is-frozen-row", stylesheet)
        self.assertIn(".main-preview-cell.is-frozen-col", stylesheet)
        self.assertIn(".main-preview-cell.is-frozen-row", stylesheet)

    def test_database_backup_assets_exist_for_sidebar_and_restore_modal(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        backup_module = (root / "static" / "modules" / "db_backup.js").read_text(encoding="utf-8")
        stylesheet = (root / "static" / "style.css").read_text(encoding="utf-8")
        api_module = (root / "static" / "modules" / "api.js").read_text(encoding="utf-8")
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn('data-tab="backups-tab"', index_html)
        self.assertIn('id="tab-backups-tab"', index_html)
        self.assertIn('id="db-backup-status"', index_html)
        self.assertIn('id="btn-run-db-backup"', index_html)
        self.assertIn('id="btn-db-backup-refresh"', index_html)
        self.assertIn('import { initDbBackup, refreshDbBackupPanel } from "/static/modules/db_backup.js";', index_html)
        self.assertIn('if (btn.dataset.tab === "backups-tab") refreshDbBackupPanel();', index_html)
        self.assertIn(
            "await initDbBackup({ reloadApp: () => refreshCurrentView({ skipDirtyConfirm: true }), autoLoad: false });",
            index_html,
        )

        self.assertIn('apiJson("/api/system/db-backups")', backup_module)
        self.assertIn('apiPost("/api/system/db-backups/run")', backup_module)
        self.assertIn('apiPut("/api/system/db-backups/settings"', backup_module)
        self.assertIn('apiPost("/api/system/db-backups/restore"', backup_module)
        self.assertIn("let _initialized = false;", backup_module)
        self.assertIn("資料庫還原完成，系統將重新整理", backup_module)

        self.assertIn(".db-backup-tab-shell", stylesheet)
        self.assertIn(".tab-btn[data-tab=\"backups-tab\"]", stylesheet)
        self.assertIn(".db-backup-toggle", stylesheet)
        self.assertIn(".db-backup-item", stylesheet)
        self.assertIn(".db-backup-empty", stylesheet)
        self.assertIn('id="action-busy-overlay"', index_html)
        self.assertIn('class="toast-message"', index_html)
        self.assertIn('class="toast-close"', index_html)
        self.assertIn(".busy-overlay", stylesheet)
        self.assertIn(".busy-spinner", stylesheet)
        self.assertIn(".toast-close", stylesheet)
        self.assertIn(".modal-progress-shell.is-error", stylesheet)
        self.assertIn("export function hideToast()", api_module)
        self.assertIn("sticky: false", api_module)
        self.assertIn("tone-error", api_module)
        self.assertIn('id="btn-batch-dispatch">寫入主檔</button>', index_html)
        self.assertIn("await showWriteToMainModal(targets);", schedule_module)
        self.assertIn('button.textContent = "整理中...";', schedule_module)
        self.assertIn('title: "正在整理寫入主檔預覽"', schedule_module)

    def test_desktop_icon_assets_are_wired_into_shell_and_html(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        desktop_app = (root / "desktop_app.py").read_text(encoding="utf-8")
        icon_png = root / "static" / "assets" / "dispatch_app_icon.png"
        icon_ico = root / "static" / "assets" / "dispatch_app_icon.ico"

        self.assertIn('/static/assets/dispatch_app_icon.png', index_html)
        self.assertIn('/static/assets/dispatch_app_icon.ico', index_html)
        self.assertIn("from app.runtime_paths import get_app_base_dir, get_resource_base_dir", desktop_app)
        self.assertIn('APP_ICON_PATH = RESOURCE_DIR / "static" / "assets" / "dispatch_app_icon.ico"', desktop_app)
        self.assertIn("get_desktop_app_icon_path", desktop_app)
        self.assertIn("icon=str(APP_ICON_PATH) if APP_ICON_PATH.exists() else None", desktop_app)
        self.assertIn("def apply_windows_window_icon(window: webview.Window):", desktop_app)
        self.assertIn("SendMessageW", desktop_app)
        self.assertTrue(icon_png.exists())
        self.assertTrue(icon_ico.exists())

    def test_desktop_bridge_supports_remote_server_status(self):
        root = Path(__file__).resolve().parents[1]
        desktop_bridge = (root / "static" / "modules" / "desktop_bridge.js").read_text(encoding="utf-8")
        desktop_app = (root / "desktop_app.py").read_text(encoding="utf-8")

        self.assertIn("桌面版已連到遠端服務", desktop_bridge)
        self.assertIn("remote_server", desktop_bridge)
        self.assertIn("from app.services.desktop_connection import resolve_remote_server_url", desktop_app)
        self.assertIn('parser.add_argument("--server-url"', desktop_app)
        self.assertIn('remote_server": self.server.is_remote', desktop_app)

    def test_version_info_assets_exist_for_header_chip_and_release_notes_modal(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        version_module = (root / "static" / "modules" / "version_info.js").read_text(encoding="utf-8")
        stylesheet = (root / "static" / "style.css").read_text(encoding="utf-8")

        self.assertIn('id="btn-app-version"', index_html)
        self.assertIn('id="app-version-label"', index_html)
        self.assertIn('id="version-info-modal"', index_html)
        self.assertIn('id="version-info-content"', index_html)
        self.assertIn('import { initVersionInfo } from "/static/modules/version_info.js";', index_html)
        self.assertIn("await initVersionInfo();", index_html)

        self.assertIn('apiJson("/api/system/app-meta")', version_module)
        self.assertIn('document.getElementById("btn-app-version")', version_module)
        self.assertIn('document.getElementById("version-info-modal")', version_module)
        self.assertIn("document.title = `${appName} ${version}`;", version_module)

        self.assertIn(".app-version-chip", stylesheet)
        self.assertIn(".version-modal-box", stylesheet)
        self.assertIn(".version-section", stylesheet)
