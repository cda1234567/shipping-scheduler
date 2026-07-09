from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.st_reconcile import (
    CATEGORY_HAVE_OURS_NOT_THEIRS,
    CATEGORY_HAVE_THEIRS_NOT_OURS,
    CATEGORY_MATCHED,
    CATEGORY_UNATTRIBUTED,
    build_st_reconcile_preview,
    parse_st_reconcile_file,
)


class StReconcileParserTests(unittest.TestCase):
    def test_parse_real_chenshang_file_extracts_parts_and_forward_filled_groups(self):
        sample_path = Path("templates") / "辰尚庫存狀況20260610_辰尚填寫.xlsx"

        parsed = parse_st_reconcile_file(str(sample_path))

        self.assertEqual(parsed["sheet_name"], "辰尚庫存表20260610")
        self.assertGreater(parsed["part_count"], 0)
        rows = parsed["rows"]
        self.assertTrue(any(row["part_number"] and row["physical"] is not None for row in rows))
        self.assertTrue(all(row["part_number"] == row["part_number"].upper() for row in rows))
        self.assertTrue(any(row.get("customer_code") for row in rows))
        manual_split_rows = [row for row in rows if row.get("needs_manual_split")]
        if manual_split_rows:
            row = manual_split_rows[0]
            self.assertIsNone(row["physical"])
            self.assertGreater(row["group_physical"], 0)
            self.assertGreater(row["group_part_count"], 1)
            self.assertIn(row["part_number"], row["group_parts"])


class StReconcileAttributionTests(unittest.TestCase):
    def test_preview_classifies_four_readonly_attribution_buckets(self):
        parsed = {
            "sheet_name": "盤點",
            "rows": [
                {
                    "part_number": "PART-A",
                    "description": "A",
                    "physical": 12,
                    "customer_code": "C1",
                    "needs_manual_split": False,
                },
                {
                    "part_number": "PART-B",
                    "description": "B",
                    "physical": 5,
                    "customer_code": "C2",
                    "needs_manual_split": False,
                },
                {
                    "part_number": "PART-C",
                    "description": "C",
                    "physical": 7,
                    "customer_code": "C3",
                    "needs_manual_split": False,
                },
                {
                    "part_number": "PART-D",
                    "description": "D",
                    "physical": None,
                    "group_physical": 20,
                    "customer_code": "C4",
                    "group_part_count": 2,
                    "group_parts": ["PART-D", "PART-E"],
                    "needs_manual_split": True,
                },
            ],
        }
        theoretical = {
            "stock": {
                "PART-A": 10,
                "PART-B": 8,
                "PART-C": 7,
                "PART-D": 1,
            },
            "order_details": {
                "PART-A": [{"order_id": 101, "used_qty": 2}],
                "PART-C": [{"order_id": 102, "used_qty": 1}],
            },
        }

        with patch("app.services.st_reconcile.parse_st_reconcile_file", return_value=parsed), \
             patch("app.services.st_reconcile.theoretical_stock_with_details", return_value=theoretical):
            report = build_st_reconcile_preview("ignored.xlsx", "2026-06-10")

        by_part = {row["part_number"]: row for row in report["parts"]}
        self.assertEqual(by_part["PART-A"]["category"], CATEGORY_HAVE_OURS_NOT_THEIRS)
        self.assertEqual(by_part["PART-B"]["category"], CATEGORY_HAVE_THEIRS_NOT_OURS)
        self.assertEqual(by_part["PART-C"]["category"], CATEGORY_MATCHED)
        self.assertEqual(by_part["PART-D"]["category"], CATEGORY_UNATTRIBUTED)
        self.assertIn("群組多料號需人工拆分", by_part["PART-D"]["notes"])
        self.assertEqual(report["summary"][CATEGORY_HAVE_OURS_NOT_THEIRS], 1)
        self.assertEqual(report["summary"][CATEGORY_HAVE_THEIRS_NOT_OURS], 1)
        self.assertEqual(report["summary"][CATEGORY_MATCHED], 1)
        self.assertEqual(report["summary"][CATEGORY_UNATTRIBUTED], 1)


if __name__ == "__main__":
    unittest.main()
