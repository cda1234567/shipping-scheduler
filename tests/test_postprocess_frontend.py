from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MOD = ROOT / "static" / "modules"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class FrontendWiringTests(unittest.TestCase):
    def test_moq_hint_only_shows_moq_without_real_need(self):
        js = read(MOD / "erp_purchase.js")
        self.assertIn("const hasNeed = Number.isFinite(n) && n > 1;", js)
        self.assertIn("上次進貨 ${fmtMoney(m)}", js)
        self.assertIn("無 MOQ 資料", js)
        self.assertIn("（正航最小採購量）", js)

    def test_shortage_po_links_and_warning_flow(self):
        js = read(MOD / "shortage_po.js")
        for needle in ("function syncLinked()", "confirm_missing_postprocess: true", "後段加工 ←", "data-act=\"unlink\"",
                       "dueReasons", "export function linkedQty"):
            self.assertIn(needle, js)
        self.assertIn("postprocessWarningText", read(MOD / "erp_purchase.js"))

    def test_manual_tab_companion_and_default_due(self):
        js = read(MOD / "erp_purchase.js")
        for needle in ("/api/holidays/default-due", "/api/holidays/shift", "/api/postprocess/check", "add-pp", "confirm_missing_postprocess"):
            self.assertIn(needle, js)
        self.assertIn('id="erp-po-companions"', read(ROOT / "static" / "index.html"))

    def test_links_panel_uses_api_helpers_and_edit_auth(self):
        js = read(MOD / "postprocess_links.js")
        self.assertNotIn(" fetch(", js)
        self.assertIn("isEditAuthenticated", js)
        html = read(ROOT / "static" / "index.html")
        self.assertIn('id="pp-links-panel"', html)
        self.assertIn("initPostprocessLinks", html)

    def test_pullin_ui_uses_shift_reason(self):
        self.assertIn("date_shift_reason", read(MOD / "po_pullin.js"))

    def test_holiday_data_is_packaged(self):
        self.assertIn("COPY app /app/app", read(ROOT / "Dockerfile"))
        self.assertIn("app/resources", read(ROOT / "desktop_app.spec"))
        self.assertTrue((ROOT / "app" / "resources" / "holidays" / "tw_2026.csv").exists())
        self.assertFalse((ROOT / ".dockerignore").read_text().count("app/resources"))


if __name__ == "__main__":
    unittest.main()
