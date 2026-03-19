from __future__ import annotations

import re
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

    def test_schedule_module_auto_checks_shortage_for_negative_carry_over(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("function shouldAutoShortageCheck(item)", schedule_module)
        self.assertIn("const carryOver = Number(item?.carry_over);", schedule_module)
        self.assertIn("const currentStock = Number(item?.current_stock);", schedule_module)
        self.assertIn("const shortageChecked = shouldAutoShortageCheck(row);", schedule_module)
        self.assertIn("const shortageChecked = shouldAutoShortageCheck(s);", schedule_module)

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
        icon_png = root / "static" / "assets" / "opentext_app_icon.png"
        icon_ico = root / "static" / "assets" / "opentext_app_icon.ico"

        self.assertIn('/static/assets/opentext_app_icon.png', index_html)
        self.assertIn('/static/assets/opentext_app_icon.ico', index_html)
        self.assertIn('APP_ICON_PATH = BASE_DIR / "static" / "assets" / "opentext_app_icon.ico"', desktop_app)
        self.assertIn("get_desktop_app_icon_path", desktop_app)
        self.assertIn("icon=str(APP_ICON_PATH) if APP_ICON_PATH.exists() else None", desktop_app)
        self.assertIn("def apply_windows_window_icon(window: webview.Window):", desktop_app)
        self.assertIn("SendMessageW", desktop_app)
        self.assertTrue(icon_png.exists())
        self.assertTrue(icon_ico.exists())

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
