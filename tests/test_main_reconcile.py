from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import openpyxl

from app.services.main_reconcile import verify_main_write
from app.services.merge_to_main import merge_row_to_main


def _create_main(path: Path) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["料號", "廠商", "MOQ", "說明", "庫存"])
    ws.append(["PART-A", "", 1, "Part A", 100])
    wb.save(path)
    wb.close()


def _groups() -> list[dict]:
    return [{
        "batch_code": "1-1",
        "po_number": "PO-1",
        "bom_model": "MODEL-A",
        "components": [{
            "part_number": "PART-A",
            "description": "Part A",
            "needed_qty": 30,
            "prev_qty_cs": 5,
            "is_dash": 0,
        }],
    }]


class MainReconcileTests(unittest.TestCase):
    def test_verify_main_write_matches_merge_plan(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            _create_main(main_path)

            result = merge_row_to_main(str(main_path), _groups(), {}, backup_dir=None)
            reconcile = verify_main_write(str(main_path), result["plan_rows"])

        self.assertTrue(reconcile["ok"])
        self.assertEqual(reconcile["checked_parts"], 1)
        self.assertEqual(reconcile["mismatches"], [])

    def test_verify_main_write_detects_bad_written_cell(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            _create_main(main_path)
            result = merge_row_to_main(str(main_path), _groups(), {}, backup_dir=None)
            first_row = result["plan_rows"][0]

            wb = openpyxl.load_workbook(main_path)
            ws = wb.active
            ws.cell(row=first_row["row_idx"], column=first_row["col_j"]).value = 999
            wb.save(main_path)
            wb.close()

            reconcile = verify_main_write(str(main_path), result["plan_rows"])

        self.assertFalse(reconcile["ok"])
        self.assertTrue(any(item["kind"] == "cell" for item in reconcile["mismatches"]))


if __name__ == "__main__":
    unittest.main()
