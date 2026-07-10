from __future__ import annotations

import unittest
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from openpyxl import Workbook

import app.database as db
from app.services.reconcile_core import theoretical_stock
from app.services.st_reconcile import (
    CATEGORY_GENLIN_BLANK_PHYSICAL,
    CATEGORY_HAVE_OURS_NOT_THEIRS,
    CATEGORY_HAVE_THEIRS_NOT_OURS,
    CATEGORY_MATCHED,
    CATEGORY_STOP_LOSS,
    CATEGORY_UNATTRIBUTED,
    build_st_reconcile_preview,
    commit_st_reconcile_stop_loss,
    parse_st_reconcile_file,
)


class StReconcileParserTests(unittest.TestCase):
    def test_parse_real_genlin_file_extracts_book_physical_and_strips_tab_suffix(self):
        sample_path = Path("templates") / "庚霖實際庫存2026Q1_2026-6-29.xlsx"

        parsed = parse_st_reconcile_file(str(sample_path))

        self.assertEqual(parsed["format"], "genlin")
        self.assertEqual(parsed["sheet_name"], "實際庫存-生產結餘")
        rows = {row["part_number"]: row for row in parsed["rows"]}
        self.assertIn("OC-10935B", rows)
        self.assertNotIn("OC-10935B-TAB", rows)
        self.assertEqual(rows["OC-10935B"]["book_qty"], 8100)
        self.assertEqual(rows["OC-10935B"]["physical_qty"], 687)

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


class StReconcileCommitTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(db._CREATE_SQL)
        self.conn.execute("ALTER TABLE orders ADD COLUMN folder TEXT NOT NULL DEFAULT ''")

        @contextmanager
        def temp_conn():
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

        self.get_conn_patcher = patch.object(db, "get_conn", temp_conn)
        self.group_patcher = patch.object(db, "_get_bom_model_groups", lambda: {})
        self.get_conn_patcher.start()
        self.group_patcher.start()

    def tearDown(self):
        self.group_patcher.stop()
        self.get_conn_patcher.stop()
        self.conn.close()

    def _make_genlin_file(self, folder: str, physical_qty: float | None, book_qty: float = 10) -> str:
        wb = Workbook()
        ws = wb.active
        ws.title = "實際庫存-生產結餘"
        ws.cell(row=3, column=3, value="Parts No/Description")
        ws.cell(row=3, column=4, value="consign invoice NO")
        ws.cell(row=3, column=6, value="辰尚庫存")
        ws.cell(row=3, column=7, value="庚霖庫存 當下實際")
        ws.cell(row=5, column=3, value="測試料")
        ws.cell(row=5, column=4, value="PART-1-TAB")
        ws.cell(row=5, column=6, value=book_qty)
        ws.cell(row=5, column=7, value=physical_qty)
        suffix = "blank" if physical_qty is None else f"{physical_qty:g}"
        path = Path(folder) / f"genlin_{suffix}.xlsx"
        wb.save(path)
        return str(path)

    def _insert_snapshot(self, qty: float) -> None:
        self.conn.execute(
            """
            INSERT INTO st_inventory_snapshot(part_number, stock_qty, description, loaded_at)
            VALUES('PART-1', ?, '', '2026-06-01T08:00:00')
            """,
            (qty,),
        )

    def _insert_audit(self, old_qty: float, new_qty: float, delta: float, reason: str, changed_at: str) -> None:
        self.conn.execute(
            """
            INSERT INTO st_inventory_audit_log(part_number, old_qty, new_qty, delta, reason, actor, changed_at)
            VALUES('PART-1', ?, ?, ?, ?, 'test', ?)
            """,
            (old_qty, new_qty, delta, reason, changed_at),
        )

    def test_commit_sets_stop_loss_anchor_writes_audit_and_second_alignment_reanchors(self):
        self._insert_snapshot(10)
        with TemporaryDirectory() as tmp:
            first_path = self._make_genlin_file(tmp, physical_qty=7, book_qty=10)
            result = commit_st_reconcile_stop_loss(first_path, "2026-06-29", source_filename="first.xlsx")

            self.assertTrue(result["ok"])
            self.assertEqual(result["summary"]["part_count"], 1)
            self.assertEqual(result["preview_summary"][CATEGORY_STOP_LOSS], 1)
            audit_rows = db.get_st_inventory_audit_log("PART-1", limit=10)
            self.assertTrue(any(row["reason"] == "st_reconcile_adjustment" and row["actor"] == "reconcile" for row in audit_rows))

            self._insert_audit(7, 5, -2, "st_consume", "2026-06-30T09:00:00")
            self.assertEqual(theoretical_stock("2026-07-01T00:00:00", part_numbers=["PART-1"])["PART-1"], 5)

            second_path = self._make_genlin_file(tmp, physical_qty=9, book_qty=9)
            second = commit_st_reconcile_stop_loss(second_path, "2026-07-02", source_filename="second.xlsx")
            self.assertEqual(second["preview_summary"][CATEGORY_MATCHED], 1)

            self._insert_audit(9, 8, -1, "st_consume", "2026-07-03T09:00:00")
            self.assertEqual(theoretical_stock("2026-07-04T00:00:00", part_numbers=["PART-1"])["PART-1"], 8)

    def test_blank_genlin_physical_is_previewed_and_skipped_by_commit(self):
        self._insert_snapshot(500)
        with TemporaryDirectory() as tmp:
            path = self._make_genlin_file(tmp, physical_qty=None, book_qty=15)

            preview = build_st_reconcile_preview(path, "2026-06-29")
            by_part = {row["part_number"]: row for row in preview["parts"]}
            self.assertIsNone(by_part["PART-1"]["physical_qty"])
            self.assertEqual(by_part["PART-1"]["category"], CATEGORY_GENLIN_BLANK_PHYSICAL)

            result = commit_st_reconcile_stop_loss(path, "2026-06-29", source_filename="blank.xlsx")

            self.assertEqual(result["summary"]["part_count"], 0)
            self.assertEqual(result["summary"]["updated_count"], 0)
            self.assertEqual(result["adjustments"], [])
            self.assertEqual(db.get_st_inventory_stock()["PART-1"], 500)
            anchor = db.get_latest_st_reconcile_anchor("2026-06-30T00:00:00", ["PART-1"])
            self.assertEqual(anchor["baseline_qty"], {})

    def test_commit_same_genlin_file_twice_records_zero_second_adjustment(self):
        self._insert_snapshot(10)
        with TemporaryDirectory() as tmp:
            path = self._make_genlin_file(tmp, physical_qty=7, book_qty=10)

            first = commit_st_reconcile_stop_loss(path, "2026-06-29", source_filename="same.xlsx")
            second = commit_st_reconcile_stop_loss(path, "2026-06-29", source_filename="same.xlsx")

            self.assertEqual(first["summary"]["adjusted_count"], 1)
            self.assertEqual(second["summary"]["adjusted_count"], 0)
            self.assertEqual(second["adjustments"][0]["adjust_qty"], 0)
            self.assertEqual(db.get_st_inventory_stock()["PART-1"], 7)


if __name__ == "__main__":
    unittest.main()
