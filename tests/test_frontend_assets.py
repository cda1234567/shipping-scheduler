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
            r"function\s+buildRowCard\s*\(r,\s*resultMap(?:,\s*visibleShortageTotals\s*=\s*null)?\)\s*\{(?P<body>.*?)\n\}",
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
        self.assertIn("row-draft-toggle", text)
        self.assertIn("po-model-wrap", text)
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
        self.assertIn('draftInlineStatHtml("結存"', text)
        self.assertIn("draft.shortages || []", text)
        self.assertIn("withGlobalBusy(", text)
        self.assertIn("showMainWriteBlockedNotice", text)
        self.assertIn("action-busy-overlay", text)
        self.assertIn("busy-overlay-percent", text)
        self.assertIn("setGlobalBusyPercent", text)
        self.assertIn("startGlobalBusyProgress", text)
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
        self.assertNotIn("if (_postDispatchShortages.length) renderPostDispatchPanel();", text)
        self.assertIn("function configureModalSearch(", text)
        self.assertIn("function applyModalSearchFilter(", text)
        self.assertIn("function matchesModalSearchQuery(", text)
        self.assertIn("function tokenizeModalSearchText(", text)
        self.assertIn("modal-shortage-section", text)
        self.assertIn("modal-search-empty", text)
        self.assertIn('const sectionSearch = file?.filename || "";', text)
        self.assertNotIn('const sectionSearch = [file?.filename || "", ...rows.map(row => `${row?.part_number || ""} ${row?.description || ""}`)].join(" ");', text)
        self.assertIn('data-search-primary="${esc(searchPrimary)}"', text)
        self.assertIn('data-search-secondary="${esc(searchSecondary)}"', text)
        self.assertIn('const hasAsciiLetter = /[a-z]/i.test(query);', text)
        self.assertIn("return secondaryTokens.some(token => token.startsWith(query));", text)

        panel_match = re.search(
            r"function\s+buildDraftPanelHtml\s*\(draft\)\s*\{(?P<body>.*?)\n\}",
            text,
            re.S,
        )
        self.assertIsNotNone(panel_match)
        self.assertNotIn("btn-draft-preview", panel_match.group("body"))

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

    def test_collapsed_draft_panel_hides_panel_and_uses_row_triangle_toggle(self):
        stylesheet = (Path(__file__).resolve().parents[1] / "static" / "style.css").read_text(encoding="utf-8")
        schedule_module = (Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn(".merge-draft-panel.is-collapsed {", stylesheet)
        self.assertIn("display: none;", stylesheet)
        self.assertIn(".po-model-wrap {", stylesheet)
        self.assertIn(".po-model-wrap .model-editable {", stylesheet)
        self.assertIn(".row-draft-toggle {", stylesheet)
        self.assertIn(".row-draft-toggle.is-expanded {", stylesheet)
        self.assertIn('>${isDraftPanelCollapsed(r.id) ? "▶" : "▼"}</button>`', schedule_module)
        self.assertIn('<span class="po-model-wrap">', schedule_module)
        self.assertIn('button.textContent = nextCollapsed ? "▶" : "▼";', schedule_module)
        self.assertIn('const row = button.closest(".po-group");', schedule_module)

    def test_schedule_module_marks_negative_after_supplement_state_with_dedicated_visuals(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")
        stylesheet = (root / "static" / "style.css").read_text(encoding="utf-8")

        self.assertIn('apiJson("/api/system/st-inventory/data")', schedule_module)
        self.assertIn("function loadStInventoryData()", schedule_module)
        self.assertIn("function computeShortageResultingStock(", schedule_module)
        self.assertIn("function isShortageStillNegative(", schedule_module)
        self.assertIn("function shortageToneClass(", schedule_module)
        self.assertIn("function computeModalCardResultingStock(", schedule_module)
        self.assertIn("function updateModalShortageTone(", schedule_module)
        self.assertIn("function refreshDraftPartTone(", schedule_module)
        self.assertIn("function stSupplySummaryHtml(", schedule_module)
        self.assertIn("function hasRemainingShortageForResultingStock(", schedule_module)
        self.assertIn("purchase_needed_qty", schedule_module)
        self.assertIn("purchase_suggested_qty", schedule_module)
        self.assertIn("st_available_qty", schedule_module)
        self.assertIn("is-negative-after-supplement", schedule_module)
        self.assertIn("is-resolved-after-supplement", schedule_module)
        self.assertIn('card.classList.toggle("is-negative-after-supplement"', schedule_module)
        self.assertIn('card.classList.toggle("is-resolved-after-supplement"', schedule_module)
        self.assertIn('const classNames = ["shortage-item", "modal-shortage-item"];', schedule_module)
        self.assertIn('data-current-stock="${esc(s.current_stock)}"', schedule_module)
        self.assertIn("ST 可補", schedule_module)
        self.assertIn("這串需要買", schedule_module)
        self.assertIn('aria-label="儲存 MOQ">存</button>', schedule_module)
        self.assertNotIn('aria-label="編輯 MOQ">編</button>', schedule_module)
        self.assertIn("shortage?._lookahead_purchase_suggested_qty", schedule_module)
        self.assertIn("shortage?._lookahead_st_available_qty", schedule_module)
        self.assertIn("function hasRemainingShortageForResultingStock(partNumber, resultingStock) {", schedule_module)
        self.assertIn("return Number.isFinite(resultingStock) && resultingStock < 0;", schedule_module)
        self.assertIn("return hasRemainingShortageForResultingStock(shortage?.part_number, resultingStock);", schedule_module)
        self.assertIn("const isNegative = hasRemainingShortageForResultingStock(card?.dataset.part, resultingStock);", schedule_module)
        self.assertNotIn("s.purchase_needed_qty ?? s.shortage_amount ?? 0", schedule_module)
        self.assertNotIn("is-st-purchase", schedule_module)

        self.assertIn(".modal-shortage-item.is-negative-after-supplement", stylesheet)
        self.assertIn(".modal-shortage-item.is-resolved-after-supplement", stylesheet)
        self.assertIn("body.desktop-dark .modal-shortage-item.is-negative-after-supplement", stylesheet)
        self.assertIn("body.desktop-dark .modal-shortage-item.is-resolved-after-supplement", stylesheet)
        self.assertNotIn(".shortage-item.cs-item", stylesheet)
        self.assertNotIn(".shortage-item.is-st-purchase", stylesheet)

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
        self.assertIn("function buildPostDispatchShortagesFromCompletedDrafts()", schedule_module)
        self.assertIn("syncPostDispatchShortagesFromCompletedDrafts();", schedule_module)
        self.assertIn("await refreshCompleted();", schedule_module)
        self.assertIn("showPostDispatchShortages();", schedule_module)
        self.assertGreaterEqual(
            schedule_module.count("const useLiveStock = Number.isFinite(liveStock);"),
            2,
        )
        self.assertNotIn("Number.isFinite(liveStock) && !isOrderScopedPart(partKey)", schedule_module)
        self.assertNotIn("POST_DISPATCH_STORAGE_KEY", schedule_module)
        self.assertNotIn("post-dispatch-shortages", schedule_module)

    def test_completed_tab_exposes_delete_dispatched_action(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("btn-rollback-order", schedule_module)
        self.assertIn("退回已發料", schedule_module)
        self.assertIn("仍要強制退回", schedule_module)
        self.assertIn("force=1", schedule_module)
        self.assertIn("/rollback-preview", schedule_module)
        self.assertIn('/rollback${forceDelete ? "?force=1" : ""}', schedule_module)
        self.assertIn("已退回", schedule_module)

    def test_completed_tab_exposes_batch_download_and_dispatch_actions(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn('id="btn-completed-download-drafts"', index_html)
        self.assertIn('id="btn-completed-gen-dispatch"', index_html)
        self.assertIn("completed-order-check", schedule_module)
        self.assertIn("function getCompletedCheckedOrderIds()", schedule_module)
        self.assertIn("handleCompletedDownloadDrafts", schedule_module)
        self.assertIn('path: "/api/schedule/completed/drafts/download"', schedule_module)
        self.assertIn("handleCompletedGenerateDispatch", schedule_module)
        self.assertIn('path: "/api/dispatch/generate"', schedule_module)
        self.assertIn("請先勾選要下載副檔的已發料訂單", schedule_module)
        self.assertIn("請先勾選要生成發料單的已發料訂單", schedule_module)
        self.assertIn("正在重新產生已發料副檔", schedule_module)
        self.assertIn("正在生成發料單", schedule_module)
        self.assertIn("timeout: 600000", schedule_module)

    def test_schedule_module_auto_checks_shortage_for_negative_carry_over(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("function shouldAutoShortageCheck(item)", schedule_module)
        self.assertIn("const carryOver = Number(item?.carry_over);", schedule_module)
        self.assertIn("const currentStock = Number(item?.current_stock);", schedule_module)
        self.assertIn("const shortageChecked = shouldAutoShortageCheck(row);", schedule_module)
        self.assertIn("const shortageChecked = shouldAutoShortageCheck(s);", schedule_module)

    def test_frontend_calculator_skips_ec_6_low_stock_warning_threshold(self):
        root = Path(__file__).resolve().parents[1]
        calculator_module = (root / "static" / "modules" / "calculator.js").read_text(encoding="utf-8")
        shortage_rules = (root / "static" / "modules" / "shortage_rules.js").read_text(encoding="utf-8")

        self.assertNotIn("export function getRequiredMinStock", calculator_module)
        self.assertNotIn("export function calculateShortageAmount", calculator_module)
        self.assertIn('if (normalized.startsWith("EC-6")) requiredMin = 0;', shortage_rules)
        self.assertIn('else if (normalized.startsWith("EC-")) requiredMin = 100;', shortage_rules)
        self.assertIn('else if (normalized.startsWith("PK-")) requiredMin = normalized.startsWith("PK-50070") ? 0 : 1;', shortage_rules)

    def test_right_panel_includes_main_stock_shortage_rule_items_without_duplicates(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn('import { calculate } from "./calculator.js";', schedule_module)
        self.assertIn("function buildMainStockNegativeItems()", schedule_module)
        self.assertIn("for (const [part, stockQty] of Object.entries(_stock))", schedule_module)
        self.assertIn("const threshold = getDisplayMinStock(key);", schedule_module)
        self.assertIn("const shortageAmount = calculateDisplayShortageAmount(key, currentStock);", schedule_module)
        self.assertIn('vendor: normalizeVendorName(_vendors?.[key]),', schedule_module)
        self.assertIn("_row_code: firstOrderCode", schedule_module)
        self.assertIn('_row_group_label: "主檔層級缺料"', schedule_module)
        self.assertIn("_main_stock_level: true", schedule_module)

        self.assertIn("const shortagePartKeys = new Set(shortages.map(item => normalizePartKey(item?.part_number)).filter(Boolean));", schedule_module)
        self.assertIn("const mainStockItems = buildMainStockNegativeItems()", schedule_module)
        self.assertIn(".filter(item => !shortagePartKeys.has(normalizePartKey(item?.part_number)));", schedule_module)
        self.assertIn("shortages.push(...mainStockItems);", schedule_module)
        self.assertIn("renderShortagePanel(shortages, csShortages, []);", schedule_module)
        self.assertNotIn("renderShortagePanel(shortages, csShortages, buildMainFileDeficitItems())", schedule_module)

        self.assertIn("const isMainStockItem = Boolean(s?._main_stock_level);", schedule_module)
        self.assertIn('data-main-supplement="true"', schedule_module)
        self.assertIn('isMainStockItem ? "" : `', schedule_module)

    def test_right_panel_shortages_reuse_cross_model_consolidation_for_normal_parts(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        match = re.search(
            r"function buildRightPanelShortageData\(\) \{(?P<body>.*?)\n\}",
            schedule_module,
            re.S,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn("const shortagesByModel = {};", body)
        self.assertIn("const checkedRows = _rows.filter(row => _checkedIds.has(row.id));", body)
        self.assertIn("const storedSupplementsByPart = {};", body)
        self.assertIn("shortagesByModel[model].push(applyRightPanelSupplementState({", body)
        self.assertIn("csShortagesByModel[model].push(applyRightPanelSupplementState({", body)
        self.assertIn("_consolidateShortagesAcrossModels(shortagesByModel, allModels, {", body)
        self.assertIn("preserveOrderScopedParts: true", body)
        self.assertIn("preserveShortageDecisions: true", body)
        self.assertIn("storedSupplementsByPart[partKey] = Math.max(storedSupplementsByPart[partKey] || 0, qty);", body)
        self.assertIn("storedSupplementsByPart[partKey] = (storedSupplementsByPart[partKey] || 0) + qty;", body)
        self.assertIn("if (isOrderScopedPart(partKey)) {", body)
        self.assertIn("shortages.push(...(shortagesByModel[model] || []).filter(item => shouldRenderRightPanelShortageItem(item, storedSupplementsByPart)));", body)
        self.assertIn("csShortages.push(...(csShortagesByModel[model] || []).filter(item => shouldRenderRightPanelShortageItem(item, storedSupplementsByPart)));", body)
        self.assertNotIn("for (const item of (effective.shortages || []).filter(shouldRenderRightPanelShortageItem))", body)

    def test_right_panel_only_renders_items_still_negative_after_merge(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("function getRightPanelSupplementQty(item, storedSupplementsByPart = {})", schedule_module)
        self.assertIn("function applyRightPanelSupplementState(item, storedSupplementsByPart = {})", schedule_module)
        self.assertIn("function getRightPanelResultingStock(item, storedSupplementsByPart = {})", schedule_module)
        self.assertIn("function shouldRenderRightPanelShortageItem(item, storedSupplementsByPart = {})", schedule_module)
        self.assertIn("function shouldRenderRightPanelActionableShortage(partNumber, resultingStock, shortageAmount = 0)", schedule_module)
        self.assertIn("if (isEcPart(partNumber) && Number.isFinite(stock)) {", schedule_module)
        self.assertIn("return stock < 0;", schedule_module)
        self.assertIn("return shouldRenderRightPanelActionableShortage(item?.part_number, resultingStock, shortageAmount);", schedule_module)
        self.assertIn("? hasRemainingShortageForResultingStock(partNumber, stock)", schedule_module)
        self.assertIn(": amount > 0;", schedule_module)
        self.assertIn("const supplementQty = getRightPanelSupplementQty(item, storedSupplementsByPart);", schedule_module)
        self.assertIn("supplement_qty: supplementQty,", schedule_module)
        self.assertIn("default_supplement: supplementQty,", schedule_module)
        self.assertIn("if (!shouldRenderRightPanelActionableShortage(partKey, currentStock, shortageAmount)) continue;", schedule_module)

    def test_order_badge_reuses_visible_right_panel_shortages_for_checked_rows(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("function buildCheckedOrderVisibleShortageBadgeMap()", schedule_module)
        self.assertIn("const { shortages, csShortages } = buildRightPanelShortageData();", schedule_module)
        self.assertIn("badgeMap.set(orderId, (badgeMap.get(orderId) || 0) + Number(item?.shortage_amount || 0));", schedule_module)
        self.assertIn("const visibleShortageTotals = buildCheckedOrderVisibleShortageBadgeMap();", schedule_module)
        self.assertIn("const total = Number(visibleShortageTotals.get(orderId) || 0);", schedule_module)
        self.assertIn("effective.status === \"no_bom\"", schedule_module)

    def test_right_panel_save_removes_row_when_supplement_turns_balance_positive(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("function removeRightPanelShortageRowIfResolved(row)", schedule_module)
        self.assertIn("const nextResultingStock = currentStock + prevQtyCs + qty - neededQty;", schedule_module)
        self.assertIn("if (!hasRemainingShortageForResultingStock(part, nextResultingStock)) {", schedule_module)
        self.assertIn("removeRightPanelShortageRowIfResolved(row);", schedule_module)
        self.assertIn("if (!_orderSupplementsByOrderId[orderId]) _orderSupplementsByOrderId[orderId] = {};", schedule_module)
        self.assertIn("_orderSupplementsByOrderId[orderId][part] = qty;", schedule_module)
        self.assertIn("let _orderSupplementDetailsByOrderId = {};", schedule_module)
        self.assertIn("function normalizeOrderSupplementDetailState(orderSupplementDetails = {})", schedule_module)
        self.assertIn("function getStoredOrderSupplementDetail(orderId, partNumber)", schedule_module)
        self.assertIn('const noteInput = row?.querySelector(".right-panel-supplement-note-input");', schedule_module)
        self.assertIn("order_decisions", schedule_module)
        self.assertIn('[part]: qty > 0 ? "CreateRequirement" : "None"', schedule_module)
        self.assertIn("order_supplement_notes", schedule_module)
        self.assertIn('_orderSupplementDetailsByOrderId[orderId][part] = {', schedule_module)
        self.assertIn('class="right-panel-supplement-note-input"', schedule_module)
        self.assertIn("最後修改", schedule_module)
        self.assertIn('data-current-stock="${esc(s.current_stock)}"', schedule_module)
        self.assertIn('data-prev-qty-cs="${esc(s.prev_qty_cs || 0)}"', schedule_module)
        self.assertIn('data-needed="${esc(s.needed)}"', schedule_module)

    def test_desktop_download_settings_support_prompting_for_location(self):
        root = Path(__file__).resolve().parents[1]
        desktop_bridge = (root / "static" / "modules" / "desktop_bridge.js").read_text(encoding="utf-8")
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")

        self.assertIn("desktop-download-ask-each-time", index_html)
        self.assertIn("每次下載前詢問位置", index_html)
        self.assertIn("set_download_mode", desktop_bridge)
        self.assertIn("handleDownloadModeChange", desktop_bridge)
        self.assertIn("choose_location", desktop_bridge)
        self.assertIn("saveBlobWithBrowserPicker", desktop_bridge)
        self.assertIn("每次下載前都會先跳出另存新檔", desktop_bridge)
        self.assertIn("之後每次下載都會先詢問位置", desktop_bridge)

    def test_calc_workspace_uses_backend_preview_instead_of_frontend_raw_modal_calc(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertNotIn("function buildRawModalShortageGroups(targets)", schedule_module)
        self.assertIn('apiJson("/api/schedule/calc-preview"', schedule_module)
        self.assertNotIn("function renderSharedPreviewParts(sharedParts = [])", schedule_module)
        self.assertNotIn("共用料（整批只補一次）", schedule_module)

        batch_modal_match = re.search(
            r"async function showBatchMergeDraftModal\(targets\) \{(?P<body>.*?)\n\}",
            schedule_module,
            re.S,
        )
        self.assertIsNotNone(batch_modal_match)
        batch_body = batch_modal_match.group("body")
        self.assertIn("refreshModalCalcPreview({ immediate: true, resetStored: _modalResetStored })", batch_body)
        self.assertNotIn("_calcResults.forEach", batch_body)

        shortage_modal_match = re.search(
            r"async function showShortageModal\(targets\) \{(?P<body>.*?)\n\}",
            schedule_module,
            re.S,
        )
        self.assertIsNotNone(shortage_modal_match)
        shortage_body = shortage_modal_match.group("body")
        self.assertIn("refreshModalCalcPreview({ immediate: true, resetStored: false })", shortage_body)
        self.assertNotIn("_calcResults.forEach", shortage_body)
        self.assertIn("function bindShortageEditors(list)", schedule_module)
        self.assertIn("async function refreshModalCalcPreview({ immediate = false, resetStored = _modalResetStored } = {})", schedule_module)
        self.assertIn("const preview = await fetchModalCalcPreview({", schedule_module)
        self.assertIn("renderModalCalcPreview(preview, { focusState });", schedule_module)
        self.assertIn("_modalPreviewDebounceTimer = setTimeout(() => {", schedule_module)
        self.assertIn("_modalPreviewDebounceTimer = null;", schedule_module)
        self.assertIn("void run();", schedule_module)
        self.assertIn("}, 600);", schedule_module)
        self.assertIn("if (_modalPreviewAbortController) _modalPreviewAbortController.abort();", schedule_module)
        self.assertNotIn("buildRawModalShortageGroups", schedule_module)
        self.assertIn("syncDraftPartControls(list, partKey, {", schedule_module)

    def test_batch_merge_rebuilds_all_checked_pending_or_merged_targets_in_current_order(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        match = re.search(
            r"async function runBatchMergeWorkflow\([^)]*\) \{(?P<body>.*?)\n\}",
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
        self.assertIn("function ensureCalcWorkspaceReady(title, subtitle = \"\")", schedule_module)
        self.assertIn("activateCalcWorkspace();", schedule_module)
        self.assertNotIn('if (modal?.style.display !== "flex") {', schedule_module)
        self.assertNotIn('throw new Error("補料 modal 沒有成功顯示");', schedule_module)
        self.assertIn("_modalDraftBaseDecisions = {};", schedule_module)
        self.assertIn("_modalDraftBaseSupplements = {};", schedule_module)
        self.assertIn("_modalDraftVisibleParts = [];", schedule_module)
        self.assertIn("function clearCalcWorkspace()", schedule_module)
        self.assertIn("calc-workspace-empty", schedule_module)
        self.assertIn('if (footer) footer.innerHTML = "";', schedule_module)

    def test_batch_dispatch_button_and_handler_are_removed_from_frontend(self):
        # 「寫入主檔」按鈕已拿掉：HTML 沒按鈕、schedule.js 沒 listener、edit_auth.js 沒鎖定 selector
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")
        edit_auth_module = (root / "static" / "modules" / "edit_auth.js").read_text(encoding="utf-8")

        self.assertNotIn('id="btn-batch-dispatch"', index_html)
        self.assertNotIn('getElementById("btn-batch-dispatch")?.addEventListener', schedule_module)
        self.assertNotIn('"#btn-batch-dispatch"', edit_auth_module)
        # handleBatchDispatch 函式本體不存在
        self.assertIsNone(re.search(
            r"^async function handleBatchDispatch\(\) \{",
            schedule_module,
            re.M,
        ))

    def test_batch_merge_button_uses_checkbox_options_for_reset_and_commit(self):
        # 批次 Merge 只剩一顆按鈕，reset_stored 與寫主檔由兩個 checkbox 控制。
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn('id="batch-merge-reset-stored"', index_html)
        self.assertIn('id="batch-merge-commit"', index_html)
        self.assertNotIn('id="btn-batch-merge-commit"', index_html)
        match = re.search(
            r"async function runBatchMergeWorkflow\([^)]*\) \{(?P<body>.*?)\n\}",
            schedule_module,
            re.S,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn('apiPost("/api/schedule/batch-merge", {', body)
        self.assertIn("const { resetStored, commitAfterModal } = getBatchMergeOptions();", body)
        self.assertIn("reset_stored: resetStored,", body)
        self.assertIn("countStoredDraftInputsForTargets", body)

    def test_write_main_modal_footer_no_longer_offers_download_bom(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        match = re.search(
            r"async function showWriteToMainModal\(targets\) \{(?P<body>.*?)\n\}",
            schedule_module,
            re.S,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn('<button id="modal-write-main" class="btn btn-success btn-sm">寫入主檔</button>', body)
        self.assertNotIn('id="modal-download-bom"', body)

    def test_dispatch_download_keeps_checked_orders_selected(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")

        match = re.search(
            r'document\.getElementById\("btn-gen-dispatch"\)\.addEventListener\("click", async \(\) => \{(?P<body>.*?)\n\}\);',
            index_html,
            re.S,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn('const orderIds = getCheckedOrderIds();', body)
        self.assertIn('path: "/api/dispatch/generate",', body)
        self.assertIn('showDownloadToast(result, "發料單");', body)
        self.assertNotIn("clearCheckedOrderIds(orderIds);", body)

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

    def test_frontend_calculator_treats_m24_as_order_scoped_with_supplement_carryover(self):
        root = Path(__file__).resolve().parents[1]
        script = """
import { calculate } from './static/modules/calculator.js';

const results = calculate(
  [
    { id: 1, po_number: 2101, pcb: 'M', model: 'MODEL-M1' },
    { id: 2, po_number: 2102, pcb: 'N', model: 'MODEL-M2' },
  ],
  {
    'MODEL-M1': {
      components: [
        { part_number: 'IC-M24C02-WMN6TP-TAB', description: 'EEPROM', needed_qty: 200, prev_qty_cs: 0, is_dash: false },
      ],
    },
    'MODEL-M2': {
      components: [
        { part_number: 'IC-M24C02-WMN6TP-TAB', description: 'EEPROM', needed_qty: 200, prev_qty_cs: 0, is_dash: false },
      ],
    },
  },
  { 'IC-M24C02-WMN6TP-TAB': 34 },
  { 'IC-M24C02-WMN6TP-TAB': 100 },
  {},
  {},
  { 1: { 'IC-M24C02-WMN6TP-TAB': 200 } },
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
        self.assertEqual(first_shortage["current_stock"], 34)
        self.assertEqual(first_shortage["shortage_amount"], 166)
        self.assertEqual(first_shortage["supplement_qty"], 200)
        self.assertEqual(second_shortage["current_stock"], 34)
        self.assertEqual(second_shortage["shortage_amount"], 166)

    def test_frontend_calculator_uses_qty_per_board_with_scrap_over_bom_needed_qty(self):
        # 新公式：needed = qty_per_board × schedule × (1 + scrap_factor)
        # 忽略 BOM F 欄的 needed_qty（即使 BOM 存錯值也不影響）
        root = Path(__file__).resolve().parents[1]
        script = """
import { calculate } from './static/modules/calculator.js';

const results = calculate(
  [{ id: 1, po_number: 9002, pcb: 'PCB-BD9', model: 'TA7-3', order_qty: 300 }],
  {
    'TA7-3': {
      components: [
        {
          part_number: 'IC-BD9327EFJ',
          description: 'IC, VOLTAGE REGULATOR',
          needed_qty: 99999,
          qty_per_board: 1,
          scrap_factor: 0.1,
          bom_order_qty: 200,
          prev_qty_cs: 0,
          is_dash: false,
        },
      ],
    },
  },
  { 'IC-BD9327EFJ': 100 },
  { 'IC-BD9327EFJ': 2500 },
  {},
  {},
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

        shortages = results[0]["shortages"]
        self.assertEqual(len(shortages), 1)
        self.assertEqual(shortages[0]["needed"], 330)  # 1 × 300 × 1.1
        self.assertEqual(shortages[0]["current_stock"], 100)
        self.assertEqual(shortages[0]["shortage_amount"], 230)

    def test_schedule_module_does_not_auto_mark_negative_running_stock_as_shortage(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn('if (decision === "Shortage") return true;', schedule_module)
        self.assertIn("return false;\n}\n\nfunction hasNegativeRunningStock(item)", schedule_module)
        self.assertIn("⚠ 結存為負，請確認補料或手動勾缺料", schedule_module)
        self.assertIn("const hasStoredSupplement = Number(s.default_supplement) > 0 || Number(s.supplement_qty) > 0;", schedule_module)
        self.assertIn("const defaultQty = shortageChecked && !hasStoredSupplement", schedule_module)

    def test_defectives_tab_auto_collapses_batch_history_on_activation(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        defectives_module = (root / "static" / "modules" / "defectives.js").read_text(encoding="utf-8")

        self.assertIn('import { initDefectives, refreshDefectives, onDefectivesTabActivated } from "/static/modules/defectives.js";', index_html)
        self.assertIn('if (btn.dataset.tab === "defectives-tab") void onDefectivesTabActivated();', index_html)
        self.assertIn("export async function onDefectivesTabActivated()", defectives_module)
        self.assertIn("await Promise.all([", defectives_module)
        self.assertIn("refreshDefectives({ collapseAll: true }),", defectives_module)
        self.assertIn("const collapseAll = Boolean(options?.collapseAll);", defectives_module)
        self.assertIn("_collapsed.clear();", defectives_module)
        self.assertIn("_batches.forEach(batch => _collapsed.add(batch.id));", defectives_module)
        self.assertIn("loadOverrunModelOptions(),", defectives_module)

    def test_boot_does_not_wait_for_secondary_tab_data(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        bom_module = (root / "static" / "modules" / "bom_manager.js").read_text(encoding="utf-8")
        alerts_module = (root / "static" / "modules" / "alerts.js").read_text(encoding="utf-8")
        defectives_module = (root / "static" / "modules" / "defectives.js").read_text(encoding="utf-8")

        init_body = index_html.split("async function init() {", 1)[1].split("\nfunction setStatus", 1)[0]
        before_finish, after_finish = init_body.split("await finishBoot();", 1)

        self.assertNotIn('apiJson("/api/schedule/rows")', before_finish)
        self.assertIn("initBomManager(async () => { await refreshSchedule(); }, { autoLoad: false });", before_finish)
        self.assertIn("await initAlerts({ autoLoad: false });", before_finish)
        self.assertIn("await initDefectives({ autoLoad: false });", before_finish)
        self.assertNotIn("await renderBomGroups();", before_finish)
        self.assertIn('runBootBackground("renderBomGroups", renderBomGroups);', after_finish)
        self.assertIn('runBootBackground("refreshAlerts", refreshAlerts);', after_finish)
        self.assertIn("export function initBomManager(onRefreshCallback, { autoLoad = true } = {})", bom_module)
        self.assertIn("export async function initAlerts({ autoLoad = true } = {})", alerts_module)
        self.assertIn("export async function initDefectives({ autoLoad = true } = {})", defectives_module)

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
        self.assertIn("使用瀏覽器預設下載位置", bridge_module)
        self.assertIn("如要每次自行選位置，請在瀏覽器開啟「下載前一律詢問儲存位置」。", bridge_module)
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

    def test_purchase_reminder_uses_main_file_parts_and_st_stock_lookup(self):
        root = Path(__file__).resolve().parents[1]
        schedule_module = (root / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")

        self.assertIn("function buildMainPurchaseReminderPartKeys()", schedule_module)
        self.assertIn("function buildActivePurchaseReminderPartKeys()", schedule_module)
        self.assertIn("[_vendors, _liveStock, _stock, _moq].forEach(source => {", schedule_module)
        self.assertIn("const mainPartKeys = buildMainPurchaseReminderPartKeys();", schedule_module)
        self.assertIn("const activePartKeys = buildActivePurchaseReminderPartKeys();", schedule_module)
        self.assertIn("function buildActivePurchaseReminderUsageMap()", schedule_module)
        self.assertIn("const activeUsageMap = buildActivePurchaseReminderUsageMap();", schedule_module)
        self.assertIn("if (!includeInactive && !_purchaseReminderShowAll && !activeDemand) continue;", schedule_module)
        self.assertIn("已隱藏非目前排程", schedule_module)
        self.assertIn("非目前排程", schedule_module)
        self.assertIn("used_by: activeUsageMap.get(key) || [],", schedule_module)
        self.assertIn("function purchaseReminderUsedByHtml(usedBy)", schedule_module)
        self.assertIn("用到：", schedule_module)
        self.assertIn("const usageByPart = new Map();", schedule_module)
        self.assertIn("const usedQty = effectiveNeededFromBomComp(comp, orderQty);", schedule_module)
        self.assertIn("used_qty: usedQty,", schedule_module)
        self.assertIn("用量 ${fmt(usedQty)}", schedule_module)
        self.assertIn("合計 ${fmt(totalUsedQty)}", schedule_module)
        self.assertIn("main_stock:", schedule_module)
        self.assertIn("主檔庫存 ${fmt(mainStock)}", schedule_module)
        self.assertIn("function isOpenTextPart(partNumber)", schedule_module)
        self.assertIn('normalizePartKey(partNumber).endsWith("-TAB")', schedule_module)
        self.assertIn("&& !isOpenTextPart(key)", schedule_module)
        self.assertIn("const combinedStock = currentStock + Math.max(0, mainStock);", schedule_module)
        self.assertIn("const projectedAvailable = combinedStock - activeUsedQty;", schedule_module)
        self.assertIn("if (projectedAvailable >= threshold) continue;", schedule_module)
        self.assertIn("排程後可用 ${fmt(projectedAvailable)}", schedule_module)
        self.assertIn("active_used_qty:", schedule_module)
        self.assertIn("projected_available:", schedule_module)
        self.assertIn("purchase-reminder-toggle-scope", schedule_module)
        self.assertIn("for (const key of mainPartKeys) {", schedule_module)
        self.assertIn("const currentStock = Number(_stStock?.[key] ?? 0);", schedule_module)

        match = re.search(
            r"function\s+buildPurchaseReminderItems\(options = \{\}\)\s*\{(?P<body>.*?)\n\}",
            schedule_module,
            re.S,
        )
        self.assertIsNotNone(match)
        self.assertNotIn("Object.entries(_stStock)", match.group("body"))

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
        self.assertIn(
            "await initStInventory({ onChanged: async () => { await refreshSchedule(); await refreshStPackages(); } });",
            index_html,
        )

        self.assertIn('apiJson("/api/system/st-inventory/info")', st_module)
        self.assertIn('apiFetch("/api/system/st-inventory/upload"', st_module)
        self.assertIn('document.getElementById("btn-upload-st-inventory")', st_module)
        self.assertIn('document.getElementById("st-inventory-file-input")', st_module)
        self.assertIn("ST 庫存已匯入", st_module)

        self.assertIn(".sidebar-meta", stylesheet)
        self.assertIn("body.desktop-dark .sidebar-meta", stylesheet)

    def test_st_package_management_assets_exist_for_missing_moq_page(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        module = (root / "static" / "modules" / "st_packages.js").read_text(encoding="utf-8")
        stylesheet = (root / "static" / "style.css").read_text(encoding="utf-8")

        self.assertIn('data-tab="st-packages-tab"', index_html)
        self.assertIn('id="tab-st-packages-tab"', index_html)
        self.assertIn('id="btn-st-packages-refresh"', index_html)
        self.assertIn('id="st-packages-status"', index_html)
        self.assertIn('id="st-packages-list"', index_html)
        self.assertIn('import { initStPackages, refreshStPackages } from "/static/modules/st_packages.js";', index_html)
        self.assertIn('if (btn.dataset.tab === "st-packages-tab") refreshStPackages();', index_html)
        self.assertIn('await initStPackages({ autoLoad: false });', index_html)
        self.assertIn('await initStInventory({ onChanged: async () => { await refreshSchedule(); await refreshStPackages(); } });', index_html)

        self.assertIn('apiJson("/api/system/st-packages/missing-moq")', module)
        self.assertIn('apiPut(`/api/system/st-packages/${encodeURIComponent(partNumber)}`', module)
        self.assertIn("例如：200,300,500", module)
        self.assertIn("ST 庫存", module)
        self.assertIn("料號清單來自主檔 MOQ 空白料", module)
        self.assertIn("差額", module)
        self.assertIn("先找整包相等的數量扣除", module)
        self.assertIn("function parsePackageInput(", module)
        self.assertIn("function applyRowState(", module)

        self.assertIn(".st-package-shell", stylesheet)
        self.assertIn(".st-package-card.is-match", stylesheet)
        self.assertIn(".st-package-card.is-mismatch", stylesheet)
        self.assertIn(".st-package-input", stylesheet)

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
        self.assertIn('id="busy-overlay-percent"', index_html)
        self.assertIn('class="toast-message"', index_html)
        self.assertIn('class="toast-close"', index_html)
        self.assertIn(".busy-overlay", stylesheet)
        self.assertIn(".busy-progress-head", stylesheet)
        self.assertIn("#busy-overlay-percent", stylesheet)
        self.assertIn(".busy-spinner", stylesheet)
        self.assertIn(".toast-close", stylesheet)
        self.assertIn(".modal-progress-shell.is-error", stylesheet)
        self.assertIn("export function hideToast()", api_module)
        self.assertIn("sticky: false", api_module)
        self.assertIn("tone-error", api_module)

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
        self.assertIn("urllib.request.urlopen(request, timeout=600)", desktop_app)

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

    def test_right_panel_stored_supplements_do_not_sum_across_orders(self):
        # Bug 1: EC 等共享 pool 料在 N 個 order 都缺料時，storedSupplementsByPart 累加會把
        # 同一份 ST 補量重複計 N 次 → 補料欄位顯示 N × MOQ 而不是一個 MOQ。修正後非 order-scoped
        # 料應走 Math.max 分支（ORDER_SCOPED 料另以累加分支處理 bug 4）。
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        # 非 order-scoped 分支必須保留 Math.max
        self.assertIn(
            "storedSupplementsByPart[partKey] = Math.max(storedSupplementsByPart[partKey] || 0, qty);",
            text,
        )
        # 累加分支必須被 isOrderScopedPart 守衛包住，不能無條件作用
        match = re.search(
            r"if \(isOrderScopedPart\(partKey\)\) \{\s*(?://[^\n]*\n\s*)*"
            r"storedSupplementsByPart\[partKey\] = \(storedSupplementsByPart\[partKey\] \|\| 0\) \+ qty;",
            text,
        )
        self.assertIsNotNone(match, "ORDER_SCOPED 累加必須在 isOrderScopedPart 分支內")

    def test_right_panel_stored_supplements_sum_for_order_scoped_parts(self):
        # Bug 4: ORDER_SCOPED 料（IC-M24 / IC-STM / IC-XC2C32）每筆 order 各自配 ST。
        # 6-4~6-9 共補 1900 顆 IC-M24C02 時，若 storedSupplementsByPart 用 Math.max 只記 400，
        # 右側補料明細會把剩 1500 顆判為缺料。修正後 ORDER_SCOPED 分支必須累加。
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        match = re.search(
            r"function buildRightPanelShortageData\(\) \{(?P<body>.*?)\n\}",
            text,
            re.S,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        # ORDER_SCOPED 分支累加
        self.assertIn("if (isOrderScopedPart(partKey)) {", body)
        self.assertIn(
            "storedSupplementsByPart[partKey] = (storedSupplementsByPart[partKey] || 0) + qty;",
            body,
        )
        # else 分支必須仍是 Math.max（避免 bug 1 復發）
        self.assertIn(
            "storedSupplementsByPart[partKey] = Math.max(storedSupplementsByPart[partKey] || 0, qty);",
            body,
        )

        # 模擬 6-4~6-9 IC-M24C02 補 400/400/400/200/300/200 → 累加 = 1900
        # 確認分流邏輯能對 ORDER_SCOPED prefix 正確認 + 走累加路徑。
        shortage_rules = (
            Path(__file__).resolve().parents[1] / "static" / "modules" / "shortage_rules.js"
        ).read_text(encoding="utf-8")
        self.assertIn('ORDER_SCOPED_PART_PREFIXES = ["IC-STM", "IC-XC2C32", "IC-M24"]', shortage_rules)
        self.assertIn("export function isOrderScopedPart(partNumber)", shortage_rules)

    def test_modal_stored_supplements_do_not_sum_across_orders_for_shared_pool_parts(self):
        # Bug 7 的正確性現在由後端 calc-preview/分配邏輯保證，API 測試已有覆蓋。
        # 這裡只守前端資產：不能恢復已刪除的 stored modal draft 聚合路徑，
        # 避免 EC 等共享 pool 料再次在前端跨 order 累加成 N 倍。
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        self.assertNotIn("function buildStoredModalDraftState", text)
        self.assertNotIn("function _applyStoredToShortages", text)
        self.assertNotIn("buildStoredModalDraftState(", text)
        self.assertNotIn("_applyStoredToShortages(", text)
        self.assertNotIn("storedSupplements[pk] = (storedSupplements[pk] || 0) + val;", text)
        self.assertNotIn("storedOrderScopedSupplements[orderPartKey] = val;", text)

        self.assertIn('apiJson("/api/schedule/calc-preview"', text)
        self.assertIn("function buildModalCalcPreviewPayload({ resetStored = _modalResetStored } = {})", text)
        self.assertIn("order_supplements: _collectModalOrderSupplements(),", text)
        self.assertIn("function renderModalCalcPreview(preview, { focusState = null } = {})", text)
        self.assertNotIn("renderSharedPreviewParts(preview?.shared_parts || [])", text)
        self.assertIn("if (!part || Number.isInteger(orderId)) return;", text)

    def test_right_panel_supplement_qty_takes_max_of_stored_and_lookahead(self):
        # Bug: 4 個 order 各缺 2000 (總缺 8000) 時，原本 stored>0 直接 return 4000，
        # 漏掉 lookahead 累積需求 8000 → under-supply。
        # 修正後 stored 與 lookahead 應該取較大者，確保補滿真正的跨 order 需求。
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        self.assertIn(
            "if (storedQty > 0 || lookaheadSuggestedQty > 0) return Math.max(storedQty, lookaheadSuggestedQty);",
            text,
        )

    def test_modal_live_recalculation_preserves_original_shortage_and_lookahead(self):
        # 輸入補料後 calc-preview 會回傳剩餘缺口 0；畫面仍須沿用第一次預覽的
        # 原始缺口與整批 lookahead，否則「總共缺 XXX」會在打字時突然消失。
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        self.assertIn("let _modalShortageDisplayBaselines = new Map();", text)
        self.assertIn("function preserveModalShortageDisplayBaseline(item = {})", text)
        self.assertIn("_remaining_shortage_amount:", text)
        self.assertIn("_lookahead_shortage_amount: Math.max(", text)
        self.assertIn("return preserveModalShortageDisplayBaseline({", text)
        self.assertIn("gap:10px;flex-wrap:wrap", text)

    def test_save_manual_moq_recalculates_without_saving_or_reopening_drafts(self):
        # 改 MOQ 只能用目前記憶體狀態重算；不能先重建副檔再重開整個工作區，
        # 否則其他尚未儲存的手動輸入會被舊副檔值覆蓋。
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        match = re.search(
            r"async function saveManualMoq\([^)]*\) \{(?P<body>.*?)\n\}",
            text,
            re.S,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn("refreshModalCalcPreview({ immediate: true", body)
        self.assertNotIn("saveBatchDraftsFromModal", body)
        self.assertNotIn("showShortageModal(_modalTargets)", body)
        self.assertNotIn("showBatchMergeDraftModal(_modalTargets)", body)

    def test_modal_manual_editor_state_survives_calc_preview_rerenders(self):
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        self.assertIn("let _modalManualEditorState = createEmptyModalManualEditorState();", text)
        self.assertIn("function rememberModalManualEditorControl(control)", text)
        self.assertIn("function mergeModalManualEditorState(payload)", text)
        self.assertIn("function collectModalManualEditorPayload()", text)
        self.assertIn("function applyModalManualEditorState(list)", text)
        self.assertIn("const editorPayload = collectModalManualEditorPayload();", text)
        self.assertIn("applyModalManualEditorState(list);", text)
        self.assertIn("rememberModalManualEditorControl(checkbox);", text)
        self.assertIn("rememberModalManualEditorControl(input);", text)

        # 預覽重算後即使某列暫時不再顯示，儲存、下載與寫主檔也要從
        # 記憶狀態組 payload，不能只讀當下仍在 DOM 裡的列。
        self.assertGreaterEqual(text.count("} = collectModalManualEditorPayload();"), 4)

    def test_open_batch_merge_modal_stable_renders_workspace_once(self):
        # 算料已搬到工作區分頁，不再使用 overlay 掛載 retry；
        # openBatchMergeDraftModalStable 只依目前順序重建 target 並渲染一次。
        schedule_module = Path(__file__).resolve().parents[1] / "static" / "modules" / "schedule.js"
        text = schedule_module.read_text(encoding="utf-8")

        match = re.search(
            r"async function openBatchMergeDraftModalStable\([^)]*\) \{(?P<body>.*?)\n\}",
            text,
            re.S,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn("const modalTargets = buildBatchMergeModalTargets(targetIds, fallbackTargets);", body)
        self.assertIn("await showBatchMergeDraftModal(modalTargets);", body)
        self.assertNotIn("closeShortageModal();", body)
        self.assertNotIn("preservedCommitAfterSave", body)

    def test_edit_auth_assets_exist(self):
        root = Path(__file__).resolve().parents[1]
        index_html = (root / "static" / "index.html").read_text(encoding="utf-8")
        api_module = (root / "static" / "modules" / "api.js").read_text(encoding="utf-8")
        auth_module = (root / "static" / "modules" / "edit_auth.js").read_text(encoding="utf-8")
        stylesheet = (root / "static" / "style.css").read_text(encoding="utf-8")

        self.assertIn('id="btn-edit-auth"', index_html)
        self.assertIn('id="edit-auth-status-chip"', index_html)
        self.assertIn('id="edit-auth-modal"', index_html)
        self.assertIn('import { initEditAuth } from "/static/modules/edit_auth.js";', index_html)
        self.assertIn("await initEditAuth();", index_html)

        self.assertIn("function setApiAuthRequiredHandler(handler)", api_module)
        self.assertIn('payload?.code === "edit_auth_required"', api_module)

        self.assertIn('apiJson("/api/system/edit-auth/status")', auth_module)
        self.assertIn('apiPost("/api/system/edit-auth/login"', auth_module)
        self.assertIn('apiPost("/api/system/edit-auth/logout")', auth_module)
        self.assertIn('document.body.classList.toggle("edit-auth-readonly"', auth_module)
        self.assertIn('setApiAuthRequiredHandler(() => {', auth_module)

        self.assertIn(".app-readonly-chip", stylesheet)
        self.assertIn(".edit-auth-locked", stylesheet)
