from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from xml.etree import ElementTree as ET

from openpyxl import Workbook

from app.services.main_reader import read_stock


class MainStockReaderTests(unittest.TestCase):
    def _workbook(self, path: Path, rows: list[list]) -> None:
        wb = Workbook()
        ws = wb.active
        ws.append(["料號", "廠商", "MOQ", "說明", None, None, None, "期初庫存",
                   "10-1", "用量", "結存", "10-2", "用量", "結存"])
        for row in rows:
            ws.append(row)
        wb.save(path)
        wb.close()

    def _read(self, row: list) -> dict[str, float]:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "main.xlsx"
            self._workbook(path, [row])
            return read_stock(str(path))

    def test_uncached_rightmost_formula_is_not_previous_usage(self):
        self.assertEqual(self._read(["PART", "", 100, "", None, None, None,
                                     100, 0, 125, "=H2+I2-J2"]), {"PART": -25})

    def test_formula_chain_resolves_references_and_blank_supplement(self):
        self.assertEqual(self._read(["PART", "", 100, "", None, None, None,
                                     100, 0, 125, "=H2+I2-J2", None, 25,
                                     "=$K$2+L2-M2"]), {"PART": -50})

    def test_thousands_text_is_read_and_can_be_formula_reference(self):
        self.assertEqual(self._read(["PART", "", 100, "", None, None, None,
                                     100, 0, 1350, " -1,250 "]), {"PART": -1250})
        self.assertEqual(self._read(["PART", "", 100, "", None, None, None,
                                     100, 0, 1350, " -1,250 ", 0, 25,
                                     "=K2+L2-M2"]), {"PART": -1275})

    def test_empty_trailing_batch_is_skipped_and_zero_is_authoritative(self):
        self.assertEqual(self._read(["PART", "", 100, "", None, None, None,
                                     100, 0, 125, -25, None, None, None]), {"PART": -25})
        self.assertEqual(self._read(["PART", "", 100, "", None, None, None,
                                     100, 0, 125, -25, None, None, 0]), {"PART": 0})

    def test_uncalculable_or_cyclic_formula_reports_part_and_cell(self):
        for formula in ["=SUM(H2:J2)", "=K2+1", "=1/0"]:
            with self.subTest(formula=formula), self.assertRaisesRegex(ValueError, r"PART.*K2"):
                self._read(["PART", "", 100, "", None, None, None, 100, 0, 125, formula])

    def test_nonfinite_balance_is_not_silently_replaced_by_usage(self):
        with self.assertRaisesRegex(ValueError, r"PART.*K2"):
            self._read(["PART", "", 100, "", None, None, None, 100, 0, 125, "NaN"])

    def test_excel_error_balance_is_not_silently_replaced_by_usage(self):
        for value in ["#DIV/0!", "#VALUE!", "#REF!", "#NUM!", "#NAME?", "#N/A", "#NULL!"]:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, r"PART.*K2"):
                self._read(["PART", "", 100, "", None, None, None, 100, 0, 125, value])

    def test_legacy_balance_without_explicit_header_preserves_rightmost_value(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "main.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.append(["料號", "廠商", "MOQ", None, None, None, None,
                       "期初庫存", None, 59220, "TTUSBC-REV-A"])
            ws.append(["PART", "", 100, None, None, None, None, None, 1123, 80, 1043])
            wb.save(path)
            wb.close()
            self.assertEqual(read_stock(str(path)), {"PART": 1043})

    def test_unsupported_formula_uses_existing_numeric_cache_without_modifying_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "main.xlsx"
            self._workbook(path, [["PART", "", 100, "", None, None, None,
                                   100, 0, 125, "=SUM(H2:J2)"]])
            namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
            with zipfile.ZipFile(path) as source:
                entries = [(item, source.read(item.filename)) for item in source.infolist()]
            with zipfile.ZipFile(path, "w") as target:
                for item, data in entries:
                    if item.filename == "xl/worksheets/sheet1.xml":
                        root = ET.fromstring(data)
                        cell = root.find(f".//{{{namespace}}}c[@r='K2']")
                        cell.find(f"{{{namespace}}}v").text = "-25"
                        data = ET.tostring(root)
                    target.writestr(item, data)
            original_bytes = path.read_bytes()
            self.assertEqual(read_stock(str(path)), {"PART": -25})
            self.assertEqual(path.read_bytes(), original_bytes)

    def test_main_data_reports_unreadable_stock_instead_of_using_snapshot(self):
        import asyncio
        from fastapi import HTTPException
        from app.routers import main_file

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "main.xlsx"
            self._workbook(path, [["PART", "", 100, "", None, None, None,
                                   100, 0, 125, "=SUM(H2:J2)"]])
            with patch.object(main_file, "_main_data_cache", None), \
                 patch.object(main_file.db, "get_setting", return_value=str(path)), \
                 patch.object(main_file.db, "get_snapshot", return_value={
                     "PART": {"stock_qty": 100, "moq": 100},
                 }) as get_snapshot:
                with self.assertRaises(HTTPException) as raised:
                    asyncio.run(main_file.get_main_data())
            self.assertEqual(raised.exception.status_code, 422)
            self.assertIn("主檔庫存讀取失敗", raised.exception.detail)
            self.assertIn("PART", raised.exception.detail)
            self.assertIn("K2", raised.exception.detail)
            get_snapshot.assert_not_called()
