from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook, load_workbook

from app.services.main_reader import find_legacy_snapshot_stock_fixes, read_stock
from app.services.merge_to_main import merge_row_to_main


class ExcelLogicTests(unittest.TestCase):
    def _build_main_workbook(self, path: Path) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = "2026"
        ws.append(["料號", "廠商", "MOQ", "期初", "盤點", "", "M/O", "結存"])
        ws.append(["PART-A", "Vendor", 1000, None, None, None, None, None])
        ws.append(["PART-B", "Vendor", 500, 10, 20, None, 0, 20])
        wb.save(path)
        wb.close()

    def test_read_stock_does_not_treat_moq_as_inventory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "main.xlsx"
            self._build_main_workbook(path)

            stock = read_stock(str(path))

        self.assertEqual(stock["PART-A"], 0.0)
        self.assertEqual(stock["PART-B"], 20.0)

    def test_find_legacy_snapshot_stock_fixes_detects_moq_only_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "main.xlsx"
            self._build_main_workbook(path)

            fixes = find_legacy_snapshot_stock_fixes(
                str(path),
                {
                    "PART-A": {"stock_qty": 1000, "moq": 1000},
                    "PART-B": {"stock_qty": 20, "moq": 500},
                },
            )

        self.assertEqual(fixes, {"PART-A": 0.0})

    def test_merge_to_main_uses_zero_stock_when_only_moq_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "main.xlsx"
            backup_dir = Path(temp_dir) / "backups"
            self._build_main_workbook(path)

            result = merge_row_to_main(
                main_path=str(path),
                groups=[{
                    "batch_code": "1-1",
                    "po_number": "12345",
                    "bom_model": "MODEL-A",
                    "components": [{
                        "part_number": "PART-A",
                        "is_dash": False,
                        "needed_qty": 50,
                        "prev_qty_cs": 0,
                    }],
                }],
                decisions={},
                backup_dir=str(backup_dir),
            )

            wb = load_workbook(path, data_only=True)
            ws = wb.active
            self.assertEqual(result["merged_parts"], 1)
            self.assertEqual(ws.cell(row=2, column=9).value, 0)
            self.assertEqual(ws.cell(row=2, column=10).value, 50)
            self.assertEqual(ws.cell(row=2, column=11).value, -50)
            wb.close()
