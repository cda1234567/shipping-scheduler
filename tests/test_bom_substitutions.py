from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from openpyxl import Workbook

from app.services.bom_substitutions import allocate_substitution, find_rule
from app.services.calculator import run
from app.services.merge_to_main import preview_order_batches
from app.services import merge_drafts


RULE = {
    "id": 7,
    "model": "MODEL-A",
    "old_part_number": "OLD-1",
    "new_part_number": "NEW-1",
    "new_per_old_ratio": 1,
    "strategy": "old_first",
    "effective_from_code": "2-1",
    "status": "active",
}


class BomSubstitutionTests(unittest.TestCase):
    def test_rule_starts_from_selected_batch(self):
        self.assertIsNone(find_rule([RULE], model="MODEL-A", part_number="OLD-1", batch_code="1-9"))
        self.assertEqual(
            find_rule([RULE], model="MODEL-A", part_number="OLD-1", batch_code="2-1")["id"],
            7,
        )

    def test_old_first_allocates_demand_once(self):
        rows = allocate_substitution(
            {"part_number": "OLD-1", "description": "零件", "needed_qty": 10, "prev_qty_cs": 0},
            RULE,
            old_available=4,
            new_available=100,
        )
        self.assertEqual([(row["part_number"], row["needed_qty"]) for row in rows], [("OLD-1", 4), ("NEW-1", 6)])
        self.assertEqual(sum(row["needed_qty"] for row in rows), 10)

    def test_calculator_reports_shortage_on_actual_replacement_part(self):
        results = run(
            orders=[{"id": 1, "model": "MODEL-A", "code": "2-1", "order_qty": 10}],
            bom_map={"MODEL-A": [{
                "part_number": "OLD-1",
                "description": "零件",
                "qty_per_board": 1,
                "needed_qty": 10,
                "prev_qty_cs": 0,
                "is_dash": False,
            }]},
            snapshot_stock={"OLD-1": 4, "NEW-1": 3},
            moq={"OLD-1": 1, "NEW-1": 1},
            substitution_rules=[RULE],
        )
        self.assertEqual(results[0]["status"], "shortage")
        self.assertEqual(results[0]["shortages"][0]["part_number"], "NEW-1")
        self.assertEqual(results[0]["shortages"][0]["needed"], 6)
        self.assertEqual(results[0]["shortages"][0]["shortage_amount"], 3)

    def test_main_preview_writes_old_and_new_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "main.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.append(["料號", "廠商", "MOQ", "期初", "盤點", "", "M/O", "結存"])
            sheet.append(["OLD-1", "", 1, None, None, None, 0, 4])
            sheet.append(["NEW-1", "", 1, None, None, None, 0, 20])
            workbook.save(path)
            workbook.close()

            preview = preview_order_batches(
                str(path),
                [{
                    "order_id": 1,
                    "model": "MODEL-A",
                    "groups": [{
                        "batch_code": "2-1",
                        "po_number": "PO-1",
                        "bom_model": "MODEL-A",
                        "components": [{
                            "part_number": "OLD-1",
                            "description": "零件",
                            "needed_qty": 10,
                            "prev_qty_cs": 0,
                            "is_dash": False,
                        }],
                        "substitution_rules": [RULE],
                    }],
                }],
            )
            rows = preview["batches"][0]["groups"][0]["rows"]
            self.assertEqual([(row["part_number"], row["needed_qty"]) for row in rows], [("OLD-1", 4), ("NEW-1", 6)])
            self.assertEqual(preview["merged_parts"], 2)

    def test_merge_draft_preview_uses_actual_old_and_new_parts(self):
        component = {
            "id": 12,
            "part_number": "OLD-1",
            "description": "零件",
            "needed_qty": 10,
            "prev_qty_cs": 0,
            "is_dash": False,
        }
        with (
            patch.object(merge_drafts.db, "get_bom_components", return_value=[component]),
            patch.object(merge_drafts.db, "list_bom_substitution_rules", return_value=[RULE]),
            patch.object(merge_drafts.db, "get_order_substitution_allocations", return_value={1: {}}),
        ):
            plan = merge_drafts._plan_order_draft(
                {"id": 1, "model": "MODEL-A", "code": "2-1", "order_qty": 0},
                {},
                [{"id": "bom-1", "model": "MODEL-A", "order_qty": 0}],
                {"OLD-1": 4, "NEW-1": 20},
                {"OLD-1": 1, "NEW-1": 1},
            )

        actual = plan["file_plans"][0]["effective_components"]
        self.assertEqual([(row["part_number"], row["needed_qty"]) for row in actual], [("OLD-1", 4), ("NEW-1", 6)])
        self.assertEqual(plan["shortages"], [])


if __name__ == "__main__":
    unittest.main()
