"""隔離驗證盤點選料畫面；使用真實前端片段，API 全部模擬，不接觸庫存。"""
from __future__ import annotations

import copy
import re
import subprocess
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


ROOT = Path(__file__).resolve().parents[1]


def layout_report() -> dict:
    examples = [
        ("EXPRESS-ID7-D-1713NT/M8G-TAB", -233, [("EXPRESS-ID7-D-1713NT/M16G-TAB", -238)]),
        ("GHR-04V-S-TAB", 24, [("PB-20134A-TAB", 490), ("PB-20140A-TAB", 45),
                                ("PB-20141A-TAB", 937), ("PB-20142A-TAB", 448), ("PB-20143A-TAB", -4)]),
        ("IC-ADM1032ARZ-1REEL-TAB", 2951, [("IC-ADM1032ARMZ-TAB", 2438), ("IC-APX809-TAB", 1569),
                                            ("OC-10839B-TAB", 0), ("PB-20103A-TAB", 193), ("IC-ASM4064-TAB", 4824)]),
        ("IC-APX809-29SAG-7-TAB", 1567, [("IC-APX809-29SAG-7", 1569)]),
        ("IC-PMM-8620AU-0-TAB", 35, [("IC-PMM-8620AU-TAB", 70)]),
        ("IC-SZESD7104MTWTAG-TAB", None, [("IC-SZESD7104MTWTAG", 0)]),
    ]
    return {"mode": "stop_loss", "cutoff_batch_code": "9-12", "count_date": "2026-09-23",
            "preview_token": "layout-token", "part_mappings": {}, "parts": [], "uncovered_parts": [
                {"part_number": source, "source_part_number": source, "source_part_numbers": [source],
                 "physical_qty": quantity, "can_map": True,
                 "reason": f"主檔找不到料號 {source}，可人工選擇正確料號後重新試算",
                 "suggestions": [{"part_number": target, "stock_qty": stock} for target, stock in suggestions]}
                for source, quantity, suggestions in examples]}


def prepare_layout(page) -> None:
    # 僅模擬主內容區的寬度與捲動環境；選料本身全部使用正式 CSS。
    page.add_style_tag(content="""
      body { display: block; overflow: auto; height: auto; padding: 16px; }
      @media (min-width: 700px) { body { padding-left: 256px; } }
      .st-reconcile-card > :not(#st-reconcile-result) { display: none !important; }
      .st-reconcile-group > :not(.st-reconcile-mapping-panel) { display: none !important; }
    """)


def verify_layout(page, output: Path) -> None:
    report = layout_report()
    for theme in ("dark", "light"):
        page.evaluate("theme => document.body.classList.toggle('desktop-dark', theme === 'dark')", theme)
        for width in (1920, 1366, 900, 390):
            page.set_viewport_size({"width": width, "height": 1000})
            page.evaluate("report => mappingTest.render(report)", report)
            expect(page.locator(".st-reconcile-mapping-card")).to_have_count(6)
            expect(page.locator(".st-reconcile-mapping-quantity").first.locator("span")).to_have_text("庚霖實盤")
            expect(page.locator(".st-reconcile-mapping-quantity").first.locator("strong")).to_have_text("-233")
            expect(page.locator(".st-reconcile-mapping-quantity").last).to_contain_text("未填")
            # 同一個來源不重複列出；數量、原因與選料各有獨立區域。
            assert page.locator(".st-reconcile-mapping-card").first.inner_text().count("EXPRESS-ID7-D-1713NT/M8G-TAB") == 1
            assert page.locator(".st-reconcile-mapping-note").first.inner_text() == "主檔找不到此料號，未納入對帳。"
            assert page.locator(".st-reconcile-mapping-stock").first.inner_text() == "主檔 -238"
            measurements = page.evaluate("""() => {
              const list = document.querySelector('.st-reconcile-mapping-list');
              const cards = [...list.querySelectorAll('.st-reconcile-mapping-card')];
              return { overflow: list.scrollWidth > list.clientWidth + 1,
                cardOverflow: cards.some(card => card.scrollWidth > card.clientWidth + 1),
                pageOverflow: document.documentElement.scrollWidth > window.innerWidth,
                visibleCards: cards.filter(card => card.getBoundingClientRect().bottom <= list.getBoundingClientRect().bottom + 1).length,
                columns: getComputedStyle(cards[0]).gridTemplateColumns.split(' ').length,
                inputWidth: cards[0].querySelector('input').getBoundingClientRect().width };
            }""")
            assert not measurements["overflow"], (theme, width, measurements)
            assert not measurements["cardOverflow"], (theme, width, measurements)
            assert not measurements["pageOverflow"], (theme, width, measurements)
            assert measurements["inputWidth"] >= 180, (theme, width, measurements)
            assert measurements["columns"] == (2 if width > 900 else 1), (theme, width, measurements)
            if width == 1920:
                assert measurements["visibleCards"] >= 4, (theme, width, measurements)
            page.locator(".st-reconcile-mapping-panel").screenshot(path=str(output / f"after-{theme}-{width}.png"))
    mapped = copy.deepcopy(report)
    source = mapped["uncovered_parts"].pop(0)["part_number"]
    mapped["part_mappings"] = {source: "EXPRESS-ID7-D-1713NT/M16G-TAB"}
    page.set_viewport_size({"width": 1366, "height": 1000})
    page.evaluate("document.body.classList.add('desktop-dark')")
    page.evaluate("report => mappingTest.render(report)", mapped)
    expect(page.locator(".st-reconcile-mapping-card.is-mapped")).to_contain_text(f"{source} → EXPRESS-ID7-D-1713NT/M16G-TAB")
    page.locator(".st-reconcile-mapping-panel").screenshot(path=str(output / "after-mapped-dark-1366.png"))
    print("PASS: 真實 CSS 深淺主題 1920/1366/900/390、長料號、負數/未填、無水平溢出、桌面可見至少 4 支。")


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
  return new Promise((resolve, reject) => window.mockRequests.push({ url, values: options?.body ? Object.fromEntries(options.body.entries()) : {}, parts: options?.body?.getAll('part_numbers') || [], resolve, reject }));
}
window.mockSessionRequests = [];
async function apiPost(url) { window.mockSessionRequests.push(url); return {}; }
async function handleMainMutation() {}
async function refreshStInventoryInMain() {}
window.confirm = () => true;
"""
    hooks = """
window.mappingTest = {
  render(report) { _lastStReconcilePreview = report; _stReconcilePartMappings = { ...(report.part_mappings || {}) }; renderStReconcilePreview(report); },
  active(session) { _activeStInventoryCount = session; renderStInventoryCountSession(); },
  snapshot() { return { mappings: _stReconcilePartMappings, preview: _lastStReconcilePreview, revision: _stReconcilePreviewRevision, activeSession: _activeStInventoryCount }; }
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
            # 慢回應期間雙擊、重新試算、取消與改選都不能建立競爭請求。
            for selector in ("#btn-st-reconcile-commit", "#btn-st-reconcile-preview", "#btn-st-reconcile-cancel",
                             "#st-reconcile-file", ".st-reconcile-mapping-target", "#btn-st-reconcile-select-all"):
                expect(page.locator(selector)).to_be_disabled()
            page.evaluate("""() => {
              for (const id of ['btn-st-reconcile-commit', 'btn-st-reconcile-preview', 'btn-st-reconcile-cancel']) {
                document.getElementById(id).dispatchEvent(new MouseEvent('click', {bubbles: true}));
              }
              document.querySelector('.st-reconcile-mapping-skip').dispatchEvent(new MouseEvent('click', {bubbles: true}));
            }""")
            assert page.evaluate("mockRequests.length") == 3
            assert page.evaluate("mockSessionRequests.length") == 0
            assert page.evaluate("mappingTest.snapshot().mappings") == {"IC-MISSING": "IC-TARGET-0"}
            page.evaluate("mockRequests[2].resolve({summary: {session_id: 1, part_count: 2, adjusted_count: 0}})")
            expect(page.locator("#st-reconcile-result")).to_be_empty()
            assert page.evaluate("mappingTest.snapshot().mappings") == {}
            expect(page.locator("#btn-st-reconcile-start")).to_be_enabled()
            expect(page.locator("#st-reconcile-file")).to_be_enabled()

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
            # 取消確認與網路失敗都會解鎖；重試只送一次並可完成。
            page.evaluate("window.confirm = () => false")
            page.locator("#btn-st-reconcile-commit").click()
            expect(page.locator("#btn-st-reconcile-commit")).to_be_enabled()
            assert page.evaluate("mockRequests.length") == 5
            page.evaluate("window.confirm = () => true")
            page.locator("#btn-st-reconcile-commit").click()
            page.evaluate("mockRequests[5].reject(new Error('連線中斷'))")
            expect(page.locator("#btn-st-reconcile-commit")).to_be_enabled()
            expect(page.locator("#st-reconcile-commit-status")).to_contain_text("連線中斷")
            page.locator("#btn-st-reconcile-commit").click()
            assert page.evaluate("mockRequests.length") == 7
            assert page.evaluate("mockRequests[6].values.cutoff_date") == "2026-09-12"
            page.evaluate("mockRequests[6].resolve({summary: {alignment_id: 2, part_count: 1, adjusted_count: 0}})")
            expect(page.locator("#st-reconcile-result")).to_be_empty()
            expect(page.locator("#btn-st-reconcile-start")).to_be_enabled()

            # 有多來源即使合計恰好無差異也要人工核對；部分未填不得選取。
            combined = copy.deepcopy(report)
            combined["parts"] = [
                {**exact, "part_number": "EC-MULTI", "source_part_numbers": ["EC-MULTI", "EC-MULTI-TAB"],
                 "source_rows": [{"part_number": "EC-MULTI", "physical_qty": 4},
                                 {"part_number": "EC-MULTI-TAB", "physical_qty": 6}]},
                {**exact, "part_number": "EC-PARTIAL", "physical_qty": None, "blocked_reason": "部分來源未填實盤，請補齊後再試算",
                 "source_part_numbers": ["EC-PARTIAL", "EC-PARTIAL-TAB"],
                 "source_rows": [{"part_number": "EC-PARTIAL", "physical_qty": 4},
                                 {"part_number": "EC-PARTIAL-TAB", "physical_qty": None}]}]
            page.evaluate("mappingTest.active({id: 4, cutoff_code: '9-12', cutoff_at: '2026-09-12'})")
            page.evaluate("report => mappingTest.render(report)", combined)
            multi_check = page.locator('.st-reconcile-part-check[data-part="EC-MULTI"]')
            expect(multi_check).not_to_be_checked()
            page.locator("#btn-st-reconcile-select-all").click()
            expect(multi_check).not_to_be_checked()
            expect(page.locator('.st-reconcile-part-check[data-part="EC-PARTIAL"]')).to_have_count(0)
            expect(page.locator("#st-reconcile-result")).to_contain_text("EC-MULTI-TAB（實盤 6）")
            expect(page.locator("#st-reconcile-result")).to_contain_text("EC-PARTIAL-TAB（實盤 未填）")
            expect(page.locator("#st-reconcile-result")).to_contain_text("部分來源未填實盤，請補齊後再試算")
            # 提交前已送出的舊狀態查詢，不能在成功後把已結束盤點復活。
            page.evaluate("void loadStInventoryCountSession()")
            multi_check.check()
            page.locator("#btn-st-reconcile-commit").click()
            page.evaluate("mockRequests[8].resolve({summary: {session_id: 4, part_count: 1, adjusted_count: 0}})")
            expect(page.locator("#st-reconcile-result")).to_be_empty()
            page.evaluate("mockRequests[7].resolve({active: {id: 4, cutoff_code: '9-12'}})")
            assert page.evaluate("mappingTest.snapshot().activeSession") is None
            expect(page.locator("#btn-st-reconcile-start")).to_be_enabled()
            assert not errors, errors
            print("PASS: 選料與舊回應回歸、提交防重入/衝突操作鎖定、取消與失敗可重試、日期相容、多來源明細不預勾/未填不可選。")
            output = ROOT / ".omc/artifacts/reconcile-layout-2026-09-30"
            output.mkdir(parents=True, exist_ok=True)
            before_html = subprocess.check_output(["git", "show", "HEAD:static/index.html"], cwd=ROOT).decode("utf-8")
            before_css = subprocess.check_output(["git", "show", "HEAD:static/style.css"], cwd=ROOT).decode("utf-8")
            before_section = re.search(r'<section class="db-backup-card st-reconcile-card">.*?</section>', before_html, re.S)
            assert before_section is not None
            before_script = before_html.split("// ── ST reconcile", 1)[1].split("// ── Main file", 1)[0]
            before_script = before_script.split("\n", 1)[1].replace("void loadStReconcileCutoffOptions().then(loadStInventoryCountSession);", "")
            before = browser.new_page(viewport={"width": 1920, "height": 1000})
            before.set_content(before_section.group(0))
            before.add_style_tag(content=before_css)
            before.add_script_tag(content=mock + before_script + hooks)
            before.wait_for_load_state("networkidle")
            prepare_layout(before)
            before.evaluate("document.body.classList.add('desktop-dark')")
            before.evaluate("report => mappingTest.render(report)", layout_report())
            before.locator(".st-reconcile-mapping-panel").screenshot(path=str(output / "before-dark-1920.png"))
            before.close()
            prepare_layout(page)
            verify_layout(page, output)
            assert not errors, errors
            print(f"Screenshots: {output}")
        finally:
            browser.close()


if __name__ == "__main__":
    run()
