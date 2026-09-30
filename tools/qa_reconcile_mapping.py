"""隔離驗證盤點選料畫面；使用真實前端片段，API 全部模擬，不接觸庫存。"""
from __future__ import annotations

import copy
import re
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


ROOT = Path(__file__).resolve().parents[1]


def run() -> None:
    html = (ROOT / "static/index.html").read_text(encoding="utf-8")
    section = re.search(r'<section class="db-backup-card st-reconcile-card">.*?</section>', html, re.S)
    assert section is not None
    script = html.split("// ── ST reconcile", 1)[1].split("// ── Main file", 1)[0]
    script = script.split("\n", 1)[1].replace("void loadStReconcileCutoffOptions().then(loadStInventoryCountSession);", "")
    mock = """
window.mockRequests = [];
window.mockToasts = [];
function esc(value) { return String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;'); }
function showToast(message) { window.mockToasts.push(message); }
function apiJson(url, options) {
  return new Promise((resolve, reject) => window.mockRequests.push({ url, values: Object.fromEntries(options.body.entries()), parts: options.body.getAll('part_numbers'), resolve, reject }));
}
async function apiPost() { return {}; }
async function handleMainMutation() {}
async function refreshStInventoryInMain() {}
window.confirm = () => true;
"""
    hooks = """
window.mappingTest = {
  render(report) { _lastStReconcilePreview = report; _stReconcilePartMappings = { ...(report.part_mappings || {}) }; renderStReconcilePreview(report); },
  active(session) { _activeStInventoryCount = session; renderStInventoryCountSession(); },
  snapshot() { return { mappings: _stReconcilePartMappings, preview: _lastStReconcilePreview, revision: _stReconcilePreviewRevision }; }
};
"""
    exact = {"part_number": "EC-EXACT-TAB", "source_part_numbers": ["EC-EXACT-TAB"], "physical_qty": 10,
             "book_qty": 10, "cutoff_main": 10, "current_main": 10, "target_main": 10,
             "expected_count": 10, "main_adjustment": 0, "category": "無差異"}
    report = {"mode": "stop_loss", "cutoff_batch_code": "9-12", "count_date": "2026-09-23",
              "preview_token": "old-token", "part_mappings": {}, "parts": [exact], "uncovered_parts": [
                  {"part_number": "IC-MISSING", "source_part_number": "IC-MISSING", "source_part_numbers": ["IC-MISSING"],
                   "physical_qty": 20, "reason": "主檔找不到料號", "can_map": True,
                   "suggestions": [{"part_number": f"IC-TARGET-{n}", "stock_qty": 20} for n in range(6)]},
                  {"part_number": "IC-BAD-BALANCE", "source_part_numbers": ["IC-BAD-BALANCE"],
                   "physical_qty": 30, "reason": "主檔結存無效", "can_map": False}]}
    mapped = copy.deepcopy(report)
    mapped["preview_token"] = "mapped-token"
    mapped["part_mappings"] = {"IC-MISSING": "IC-TARGET-0"}
    mapped["uncovered_parts"] = report["uncovered_parts"][1:]
    mapped["parts"].append({**exact, "part_number": "IC-TARGET-0", "source_part_numbers": ["IC-MISSING"], "manual_mapping": True})
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={"width": 1500, "height": 1000})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.set_content(section.group(0))
            page.add_style_tag(content=(ROOT / "static/style.css").read_text(encoding="utf-8"))
            page.add_script_tag(content=mock + script + hooks)
            page.wait_for_load_state("networkidle")
            page.locator("#st-reconcile-file").set_input_files({"name": "count.xlsx", "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "buffer": b"mock"})
            page.evaluate("mappingTest.active({id: 1, cutoff_code: '9-12', cutoff_at: '2026-09-12'})")
            page.evaluate("report => mappingTest.render(report)", report)
            assert page.locator(".st-reconcile-mapping-suggestion").count() == 5
            assert page.locator(".st-reconcile-mapping-target").count() == 1
            expect(page.locator("#btn-st-reconcile-commit")).to_be_enabled()

            page.locator(".st-reconcile-mapping-suggestion").first.click()
            expect(page.locator(".st-reconcile-mapping-target")).to_have_value("IC-TARGET-0")
            expect(page.locator("#btn-st-reconcile-commit")).to_be_disabled()
            assert page.evaluate("mappingTest.snapshot().preview") is None
            assert page.evaluate("mockRequests.length") == 0
            page.locator(".st-reconcile-mapping-skip").click()
            target = page.locator(".st-reconcile-mapping-target")
            target.click()
            target.press_sequentially("ic-target-0", delay=10)
            expect(target).to_have_value("ic-target-0")
            expect(target).to_be_focused()
            assert page.evaluate("mappingTest.snapshot().mappings") == {"IC-MISSING": "IC-TARGET-0"}

            page.locator(".st-reconcile-mapping-preview").click()
            assert page.evaluate("JSON.parse(mockRequests[0].values.part_mappings)") == {"IC-MISSING": "IC-TARGET-0"}
            target.fill("IC-TARGET-1")
            page.evaluate("report => mockRequests[0].resolve(report)", mapped)
            expect(page.locator("#btn-st-reconcile-commit")).to_be_disabled()
            assert page.evaluate("mappingTest.snapshot().preview") is None
            expect(target).to_have_value("IC-TARGET-1")

            target.fill("IC-TARGET-0")
            page.locator(".st-reconcile-mapping-preview").click()
            page.evaluate("report => mockRequests[1].resolve(report)", mapped)
            mapped_check = page.locator('.st-reconcile-part-check[data-part="IC-TARGET-0"]')
            expect(mapped_check).not_to_be_checked()
            page.locator("#btn-st-reconcile-select-all").click()
            expect(mapped_check).not_to_be_checked()
            expect(page.locator("#st-reconcile-result")).to_contain_text("IC-MISSING → IC-TARGET-0")
            mapped_check.check()
            page.locator("#btn-st-reconcile-commit").click()
            assert page.evaluate("mockRequests[2].url") == "/api/reconcile/st/commit"
            assert page.evaluate("mockRequests[2].values.preview_token") == "mapped-token"
            assert page.evaluate("JSON.parse(mockRequests[2].values.part_mappings)") == {"IC-MISSING": "IC-TARGET-0"}
            assert set(page.evaluate("mockRequests[2].parts")) == {"EC-EXACT-TAB", "IC-TARGET-0"}
            page.evaluate("mockRequests[2].resolve({summary: {session_id: 1, part_count: 2, adjusted_count: 0}})")
            expect(page.locator("#st-reconcile-result")).to_be_empty()
            assert page.evaluate("mappingTest.snapshot().mappings") == {}

            page.evaluate("mappingTest.active({id: 2, cutoff_code: '9-12', cutoff_at: '2026-09-12'})")
            page.evaluate("report => mappingTest.render(report)", report)
            page.locator("#btn-st-reconcile-preview").click()
            page.locator("#st-reconcile-file").set_input_files({"name": "other.xlsx", "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "buffer": b"mock"})
            page.evaluate("report => mockRequests[3].resolve(report)", report)
            expect(page.locator("#st-reconcile-result")).to_be_empty()
            assert page.evaluate("mappingTest.snapshot().preview") is None

            page.evaluate("mappingTest.active({id: 3, cutoff_code: '', cutoff_at: '2026-09-12'})")
            legacy = {"mode": "stop_loss", "parts": [{"part_number": "LEGACY", "book_qty": 10, "physical_qty": 10, "theoretical": 10, "category": "無差異"}], "uncovered_parts": []}
            page.evaluate("report => mappingTest.render(report)", legacy)
            assert page.locator(".st-reconcile-mapping-target").count() == 0
            expect(page.locator("#btn-st-reconcile-commit")).to_contain_text("ST 停損點")
            page.locator("#btn-st-reconcile-preview").click()
            assert page.evaluate("mockRequests[4].values.part_mappings ?? null") is None
            page.evaluate("report => mockRequests[4].resolve(report)", legacy)
            expect(page.locator("#btn-st-reconcile-commit")).to_be_enabled()
            assert not errors, errors
            print("PASS: 推薦上限、不自動選料、連續打字不失焦、選料重算、舊回應失效、人工料不預勾、提交同份 mapping/token、完成與換檔清除、日期相容。")
        finally:
            browser.close()


if __name__ == "__main__":
    run()
