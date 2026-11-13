from __future__ import annotations

import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import openpyxl
from fastapi.testclient import TestClient
from openpyxl.comments import Comment
from openpyxl.formatting.rule import CellIsRule
from openpyxl.styles import Font, PatternFill
from openpyxl.worksheet.datavalidation import DataValidation

from app.services.main_negative_format import (
    apply_main_negative_red,
    find_negative_red_ranges,
    save_main_workbook,
)
from main import app


def _build_main(path: Path) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "2026"
    ws.append(["料號", "廠商", "MOQ", "期初", "12-4", "4500059032", "T356"])
    ws.append(["EC-1", "Vendor", -1, 10, -5, 0, 3])
    ws.append(["EC-2", "Vendor", 100, -2.5, "-7", None, 8])
    ws["E2"].fill = PatternFill(fill_type="solid", fgColor="FFC7CE")
    ws["E2"].font = Font(bold=True)
    ws["A2"].comment = Comment("inventory-count-boundary", "system")
    ws["E3"].comment = Comment("dispatch-precise-rollback", "system")
    ws.freeze_panes = "D2"
    ws.column_dimensions["A"].width = 22
    ws.row_dimensions[2].height = 30
    ws.conditional_formatting.add(
        "G2:G3",
        CellIsRule(operator="greaterThan", formula=["5"], fill=PatternFill(fill_type="solid", fgColor="FFFF00")),
    )
    dv = DataValidation(type="list", formula1='"A,B"')
    dv.add("B2:B3")
    ws.add_data_validation(dv)
    second = wb.create_sheet("第二頁")
    second["A1"] = "保留"
    wb.save(path)
    wb.close()


def _cf_summary(ws) -> list[tuple[str, str, str | None]]:
    return [
        (str(cf.sqref), rule.type, getattr(rule, "operator", None))
        for cf in ws.conditional_formatting
        for rule in cf.rules
    ]


class MainNegativeRedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        cls.client = TestClient(app)

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.main_path = Path(self._tmp.name) / "main.xlsx"
        _build_main(self.main_path)

    def tearDown(self):
        self._tmp.cleanup()

    def _fake_setting(self, key, default=""):
        return {
            "main_file_path": str(self.main_path),
            "main_filename": "main.xlsx",
            "main_loaded_at": "2026-11-13T12:00:00",
        }.get(key, default)

    def _save_round(self, mutate=None) -> None:
        wb = openpyxl.load_workbook(self.main_path)
        if mutate:
            mutate(wb.worksheets[0])
        save_main_workbook(wb, self.main_path)
        wb.close()

    def test_save_adds_single_rule_covering_d2_to_last_cell(self):
        self._save_round()
        wb = openpyxl.load_workbook(self.main_path)
        ws = wb.worksheets[0]
        self.assertEqual(find_negative_red_ranges(ws), ["D2:G3"])
        rule = next(r for cf in ws.conditional_formatting for r in cf.rules if r.type == "cellIs" and r.operator == "lessThan")
        self.assertEqual(rule.formula, ["0"])
        self.assertTrue(str(rule.dxf.font.color.rgb).upper().endswith("FF0000"))
        # 儲存格自身樣式不變
        self.assertEqual(ws["E2"].fill.fgColor.rgb, "00FFC7CE")
        self.assertTrue(ws["E2"].font.bold)
        self.assertNotEqual(getattr(ws["D3"].font.color, "rgb", None), "00FF0000")
        wb.close()

    def test_repeated_saves_do_not_accumulate_and_range_follows_columns(self):
        for _ in range(3):
            self._save_round()
        wb = openpyxl.load_workbook(self.main_path)
        self.assertEqual(find_negative_red_ranges(wb.worksheets[0]), ["D2:G3"])
        dxf_count = len(wb._differential_styles.styles)
        wb.close()

        def grow(ws):
            ws.cell(row=5, column=10, value=-1)

        self._save_round(grow)
        self._save_round()
        wb = openpyxl.load_workbook(self.main_path)
        ws = wb.worksheets[0]
        self.assertEqual(find_negative_red_ranges(ws), ["D2:J5"])
        # 既有的其他條件式格式保留，且只多我們這一條
        self.assertEqual(_cf_summary(ws), [("G2:G3", "cellIs", "greaterThan"), ("D2:J5", "cellIs", "lessThan")])
        self.assertEqual(len(wb._differential_styles.styles), dxf_count)
        wb.close()

    def test_save_preserves_comments_freeze_validation_dimensions_and_second_sheet(self):
        self._save_round()
        self._save_round()
        wb = openpyxl.load_workbook(self.main_path)
        ws = wb.worksheets[0]
        self.assertEqual(ws["A2"].comment.text, "inventory-count-boundary")
        self.assertEqual(ws["E3"].comment.text, "dispatch-precise-rollback")
        self.assertEqual(ws.freeze_panes, "D2")
        self.assertEqual(ws.column_dimensions["A"].width, 22)
        self.assertEqual(ws.row_dimensions[2].height, 30)
        self.assertEqual(len(ws.data_validations.dataValidation), 1)
        self.assertEqual(str(ws.data_validations.dataValidation[0].sqref), "B2:B3")
        self.assertEqual(wb.sheetnames, ["2026", "第二頁"])
        self.assertEqual(wb["第二頁"]["A1"].value, "保留")
        self.assertEqual(find_negative_red_ranges(wb["第二頁"]), [])
        wb.close()

    def test_apply_is_noop_when_rule_is_current(self):
        wb = openpyxl.load_workbook(self.main_path)
        self.assertTrue(apply_main_negative_red(wb))
        self.assertFalse(apply_main_negative_red(wb))
        self.assertEqual(find_negative_red_ranges(wb.worksheets[0]), ["D2:G3"])
        wb.close()

    def test_edit_main_cell_saves_with_rule(self):
        with patch("app.routers.main_file.db.get_setting", side_effect=self._fake_setting), \
             patch("app.routers.main_file.backup_main_file"):
            response = self.client.patch("/api/main-file/cell", json={"row": 3, "col": 7, "value": "-9"})
        self.assertEqual(response.status_code, 200, response.text)
        wb = openpyxl.load_workbook(self.main_path)
        self.assertEqual(wb.worksheets[0]["G3"].value, -9)
        self.assertEqual(find_negative_red_ranges(wb.worksheets[0]), ["D2:G3"])
        self.assertEqual(wb.worksheets[0]["A2"].comment.text, "inventory-count-boundary")
        wb.close()

    def test_download_includes_rule_without_touching_disk_file(self):
        before = self.main_path.read_bytes()
        with patch("app.routers.main_file.db.get_setting", side_effect=self._fake_setting):
            response = self.client.get("/api/main-file/download")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.main_path.read_bytes(), before)

        wb = openpyxl.load_workbook(io.BytesIO(response.content))
        ws = wb.worksheets[0]
        self.assertEqual(find_negative_red_ranges(ws), ["D2:G3"])
        self.assertEqual(ws["A2"].comment.text, "inventory-count-boundary")
        self.assertEqual(ws.freeze_panes, "D2")
        self.assertEqual(wb.sheetnames, ["2026", "第二頁"])
        wb.close()

    def test_download_of_already_formatted_file_sends_original_bytes(self):
        self._save_round()
        saved = self.main_path.read_bytes()
        with patch("app.routers.main_file.db.get_setting", side_effect=self._fake_setting):
            response = self.client.get("/api/main-file/download")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, saved)

    def test_preview_marks_only_negative_numbers(self):
        with patch("app.routers.main_file.db.get_setting", side_effect=self._fake_setting), \
             patch("app.services.main_preview.MAIN_PREVIEW_CACHE_DIR", Path(self._tmp.name) / "cache"):
            response = self.client.get("/api/main-file/preview")
        self.assertEqual(response.status_code, 200)
        sheet = response.json()["sheet"]
        flagged = {
            (row["index"], cell["col"])
            for row in sheet["rows"]
            for cell in row["cells"]
            if cell.get("negative")
        }
        # C2=-1、E2=-5、D3=-2.5 是負數；0、正數、文字 "-7" 不標
        self.assertEqual(flagged, {(2, 3), (2, 5), (3, 4)})
        # 原本的背景與粗體樣式不受影響
        e2 = next(cell for cell in sheet["rows"][1]["cells"] if cell["col"] == 5)
        style = sheet["styles"][e2["style_id"]]
        self.assertEqual(style.get("background"), "#FFC7CE")
        self.assertTrue(style.get("bold"))
        self.assertNotIn("color", style)

    def test_frontend_applies_red_font_for_negative_cells(self):
        source = (Path(__file__).resolve().parents[1] / "static" / "modules" / "main_preview_v2.js").read_text(encoding="utf-8")
        self.assertIn('const NEGATIVE_FONT_COLOR = "#FF0000";', source)
        self.assertIn("if (cell?.negative) cellValue.fc = NEGATIVE_FONT_COLOR;", source)


if __name__ == "__main__":
    unittest.main()


class MainPreviewSpeedupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        cls.client = TestClient(app)

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.main_path = Path(self._tmp.name) / "main.xlsx"
        _build_main(self.main_path)
        self._patches = [
            patch("app.routers.main_file.db.get_setting", side_effect=self._fake_setting),
            patch("app.services.main_preview.MAIN_PREVIEW_CACHE_DIR", Path(self._tmp.name) / "cache"),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        self._tmp.cleanup()

    def _fake_setting(self, key, default=""):
        return {
            "main_file_path": str(self.main_path),
            "main_filename": "main.xlsx",
            "main_loaded_at": "2026-11-13T12:00:00",
        }.get(key, default)

    def test_compact_drops_only_empty_cells(self):
        full = self.client.get("/api/main-file/preview").json()
        compact = self.client.get("/api/main-file/preview?compact=1").json()
        self.assertNotIn("compact", full)
        self.assertTrue(compact.pop("compact"))
        expected = {k: v for k, v in full.items() if k != "sheet"}
        self.assertEqual({k: v for k, v in compact.items() if k != "sheet"}, expected)
        for full_row, compact_row in zip(full["sheet"]["rows"], compact["sheet"]["rows"]):
            self.assertEqual(
                compact_row["cells"],
                [c for c in full_row["cells"] if c["value"] != "" or "rowspan" in c or "colspan" in c],
            )
        self.assertEqual(full["sheet"]["columns"], compact["sheet"]["columns"])

    def test_meta_returns_only_loaded_at(self):
        response = self.client.get("/api/main-file/preview?meta=1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"filename": "main.xlsx", "loaded_at": "2026-11-13T12:00:00"})

    def test_cached_bytes_reused_until_main_file_changes(self):
        first = self.client.get("/api/main-file/preview")
        with patch("app.services.main_preview.read_live_main_preview") as reader:
            again = self.client.get("/api/main-file/preview")
            reader.assert_not_called()
        self.assertEqual(again.content, first.content)

        wb = openpyxl.load_workbook(self.main_path)
        wb.worksheets[0]["A3"] = "EC-9"
        wb.save(self.main_path)
        wb.close()
        os.utime(self.main_path, ns=(1, self.main_path.stat().st_mtime_ns + 10**9))
        changed = self.client.get("/api/main-file/preview").json()
        a3 = next(c for c in changed["sheet"]["rows"][2]["cells"] if c["col"] == 1)
        self.assertEqual(a3["value"], "EC-9")

    def test_etag_304_still_works_for_compact(self):
        first = self.client.get("/api/main-file/preview?compact=1")
        etag = first.headers["etag"]
        self.assertNotEqual(etag, self.client.get("/api/main-file/preview").headers["etag"])
        second = self.client.get("/api/main-file/preview?compact=1", headers={"If-None-Match": etag})
        self.assertEqual(second.status_code, 304)
