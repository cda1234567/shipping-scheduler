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
    resolve_cutoff_batch,
)


class StReconcileParserTests(unittest.TestCase):
    def test_parse_detail_format_prefers_fg_and_combines_exact_duplicate_parts(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "total-format.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "實際庫存-生產結餘"
            header_row = 12
            ws.cell(row=header_row, column=3, value="Parts No/\nDescription")
            ws.cell(row=header_row, column=4, value="consign invoic")
            ws.cell(row=header_row, column=6, value="辰尚庫存")
            ws.cell(row=header_row, column=7, value="庚霖庫存\n當下實際")
            ws.cell(row=header_row, column=21, value="實際庫存\n總和 (辰尚+庚霖)")
            ws.cell(row=header_row, column=22, value="生產\n結餘(12/30/2025) Remaining")
            ws.cell(row=header_row, column=25, value="給客人資料已扣NG")

            ws.cell(row=14, column=3, value="測試料 1")
            ws.cell(row=14, column=4, value="part-1-tab")
            ws.cell(row=14, column=6, value=900)
            ws.cell(row=14, column=7, value=800)
            ws.cell(row=14, column=21, value=12)
            ws.cell(row=14, column=22, value=15)
            ws.cell(row=14, column=25, value=7)

            ws.cell(row=15, column=3, value="測試料 1 重複")
            ws.cell(row=15, column=4, value="PART-1-TAB")
            ws.cell(row=15, column=6, value=90)
            ws.cell(row=15, column=7, value=80)
            ws.cell(row=15, column=21, value=-2)
            ws.cell(row=15, column=22, value=-1)
            ws.cell(row=15, column=25, value=999)

            ws.cell(row=16, column=21, value=123)
            ws.cell(row=16, column=22, value=456)
            ws.cell(row=25, column=1, value="")
            wb.save(path)
            wb.close()

            parsed = parse_st_reconcile_file(str(path))
            with patch(
                "app.services.st_reconcile.theoretical_stock_with_details",
                return_value={"stock": {"PART-1": 20}, "order_details": {}},
            ), patch(
                "app.services.st_reconcile.db.get_latest_st_reconcile_anchor",
                return_value=None,
            ), patch(
                "app.services.st_reconcile.db.get_defective_part_totals",
                return_value=[],
            ), patch(
                "app.services.st_reconcile.db.get_st_inventory_stock", return_value={},
            ), patch(
                "app.services.st_reconcile.db.get_setting", return_value='',
            ):
                preview = build_st_reconcile_preview(str(path), "2026-09-23")

        self.assertEqual(parsed["source_columns"], {"book": "F", "physical": "G"})
        self.assertEqual(len(parsed["rows"]), 2)
        self.assertEqual(parsed["rows"][0]["part_number"], "PART-1-TAB")
        self.assertEqual(parsed["rows"][0]["book_qty"], 900)
        self.assertEqual(parsed["rows"][0]["physical_qty"], 800)
        self.assertEqual(parsed["rows"][1]["book_qty"], 90)
        self.assertEqual(parsed["rows"][1]["physical_qty"], 80)
        self.assertEqual(preview["source_columns"], {"book": "F", "physical": "G"})
        self.assertEqual(preview["parts"][0]["book_qty"], 990)
        self.assertEqual(preview["parts"][0]["physical_qty"], 880)
        self.assertEqual(preview["parts"][0]["book_vs_physical_diff"], 110)
        self.assertIn("F 欄為我方帳面，G 欄為庚霖實盤", preview["assumptions"][0])

    def test_parse_legacy_format_keeps_fg_with_short_and_multiline_headers(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy-format.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "實際庫存-生產結餘 "
            ws.cell(row=3, column=3, value="Parts No/Description")
            ws.cell(row=3, column=4, value="consign invoic")
            ws.cell(row=3, column=6, value="辰尚庫存")
            ws.cell(row=3, column=7, value="庚霖庫存\n當下實際")
            ws.cell(row=5, column=3, value="舊格式料")
            ws.cell(row=5, column=4, value="legacy-tab")
            ws.cell(row=5, column=6, value=30)
            ws.cell(row=5, column=7, value=25)
            ws.cell(row=5, column=25, value=1)
            ws.cell(row=10, column=1, value="")
            wb.save(path)
            wb.close()

            parsed = parse_st_reconcile_file(str(path))

        self.assertEqual(parsed["source_columns"], {"book": "F", "physical": "G"})
        self.assertEqual(parsed["part_count"], 1)
        self.assertEqual(parsed["rows"][0]["part_number"], "LEGACY-TAB")
        self.assertEqual(parsed["rows"][0]["book_qty"], 30)
        self.assertEqual(parsed["rows"][0]["physical_qty"], 25)

    def test_parse_real_chenshang_file_extracts_parts_and_forward_filled_groups(self):
        sample_path = Path("templates") / "辰尚庫存狀況20260610_辰尚填寫.xlsx"
        if not sample_path.exists():
            self.skipTest("本機實際辰尚盤點樣本未納入版本庫")

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
        return self._make_genlin_file_with_rows(
            folder,
            [{"part": "PART-1-TAB", "description": "測試料", "book_qty": book_qty, "physical_qty": physical_qty}],
        )

    def _make_genlin_file_with_rows(self, folder: str, rows: list[dict]) -> str:
        wb = Workbook()
        ws = wb.active
        ws.title = "實際庫存-生產結餘"
        ws.cell(row=3, column=3, value="Parts No/Description")
        ws.cell(row=3, column=4, value="consign invoice NO")
        ws.cell(row=3, column=6, value="辰尚庫存")
        ws.cell(row=3, column=7, value="庚霖庫存 當下實際")
        for offset, row in enumerate(rows, start=5):
            ws.cell(row=offset, column=3, value=row.get("description") or "")
            ws.cell(row=offset, column=4, value=row.get("part") or "")
            ws.cell(row=offset, column=6, value=row.get("book_qty"))
            ws.cell(row=offset, column=7, value=row.get("physical_qty"))
        path = Path(folder) / f"genlin_{len(rows)}_rows.xlsx"
        wb.save(path)
        return str(path)

    def _insert_snapshot(self, qty: float, part_number: str = "PART-1") -> None:
        self.conn.execute(
            """
            INSERT INTO st_inventory_snapshot(part_number, stock_qty, description, loaded_at)
            VALUES(?, ?, '', '2026-06-01T08:00:00')
            """,
            (part_number, qty),
        )

    def _insert_audit(self, old_qty: float, new_qty: float, delta: float, reason: str, changed_at: str) -> None:
        self.conn.execute(
            "UPDATE st_inventory_snapshot SET stock_qty=?, loaded_at=? WHERE part_number='PART-1'",
            (new_qty, changed_at),
        )
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
            self.assertEqual(db.get_st_inventory_stock()["PART-1"], 9)

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

    def test_commit_with_part_number_subset_updates_only_selected_parts(self):
        self._insert_snapshot(10, "PART-1")
        self._insert_snapshot(20, "PART-2")
        with TemporaryDirectory() as tmp:
            path = self._make_genlin_file_with_rows(tmp, [
                {"part": "PART-1-TAB", "description": "測試料 1", "book_qty": 10, "physical_qty": 7},
                {"part": "PART-2-TAB", "description": "測試料 2", "book_qty": 20, "physical_qty": 19},
            ])

            result = commit_st_reconcile_stop_loss(
                path,
                "2026-06-29",
                source_filename="subset.xlsx",
                part_numbers=[" part-2 "],
            )

            self.assertEqual(result["summary"]["part_count"], 1)
            self.assertEqual(result["summary"]["adjusted_count"], 1)
            self.assertEqual(db.get_st_inventory_stock()["PART-1"], 10)
            self.assertEqual(db.get_st_inventory_stock()["PART-2"], 19)

            alignment = self.conn.execute(
                "SELECT part_count, diff_count FROM st_reconcile_alignments WHERE id=?",
                (result["summary"]["alignment_id"],),
            ).fetchone()
            self.assertEqual(alignment["part_count"], 1)
            self.assertEqual(alignment["diff_count"], 1)
            parts = self.conn.execute(
                "SELECT part_number FROM st_reconcile_alignment_parts WHERE alignment_id=?",
                (result["summary"]["alignment_id"],),
            ).fetchall()
            self.assertEqual([row["part_number"] for row in parts], ["PART-2"])

    def test_commit_with_empty_part_number_list_returns_plain_error(self):
        self._insert_snapshot(10)
        with TemporaryDirectory() as tmp:
            path = self._make_genlin_file(tmp, physical_qty=7, book_qty=10)

            with self.assertRaisesRegex(ValueError, "請至少勾選 1 支料號再建立停損點"):
                commit_st_reconcile_stop_loss(path, "2026-06-29", part_numbers=[])

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

    def test_commit_preserves_post_cutoff_consumption_and_second_commit_is_idempotent(self):
        self._insert_snapshot(100)
        self._insert_audit(100, 70, -30, "st_consume", "2026-06-30T09:00:00")
        with TemporaryDirectory() as tmp:
            path = self._make_genlin_file(tmp, physical_qty=80, book_qty=100)

            first = commit_st_reconcile_stop_loss(path, "2026-06-29", source_filename="post-cutoff.xlsx")

            self.assertEqual(first["summary"]["adjusted_count"], 1)
            self.assertEqual(first["adjustments"][0]["adjust_qty"], -20)
            self.assertEqual(db.get_st_inventory_stock()["PART-1"], 50)

            second = commit_st_reconcile_stop_loss(path, "2026-06-29", source_filename="post-cutoff.xlsx")

            self.assertEqual(second["summary"]["adjusted_count"], 0)
            self.assertEqual(second["adjustments"][0]["adjust_qty"], 0)
            self.assertEqual(db.get_st_inventory_stock()["PART-1"], 50)

    def test_preview_lists_defective_parts_not_covered_by_stocktake(self):
        self._insert_snapshot(10)
        for part, qty, created in (
            ("PART-1", 5, "2026-06-21T09:00:00"),
            ("SELF-9", 12, "2026-06-22T09:00:00"),
            ("SELF-9", 3, "2026-06-25T09:00:00"),
            ("SELF-LATE", 4, "2026-07-05T09:00:00"),
        ):
            self.conn.execute(
                "INSERT INTO defective_records(part_number, defective_qty, created_at) VALUES(?, ?, ?)",
                (part, qty, created),
            )

        with TemporaryDirectory() as tmp:
            path = self._make_genlin_file(tmp, physical_qty=7, book_qty=10)
            preview = build_st_reconcile_preview(path, "2026-06-29")

        uncovered = {item["part_number"]: item for item in preview["uncovered_parts"]}
        self.assertNotIn("PART-1", uncovered)          # 檔內有的料不列
        self.assertNotIn("SELF-LATE", uncovered)       # 截止點之後的不算本期
        self.assertIn("SELF-9", uncovered)
        self.assertEqual(uncovered["SELF-9"]["total_qty"], 15)
        self.assertEqual(uncovered["SELF-9"]["record_count"], 2)

    def test_batch_cutoff_requires_explicit_main_selection_and_never_falls_back_to_st_write(self):
        self._insert_snapshot(100)
        order_cursor = self.conn.execute(
            "INSERT INTO orders(po_number, model, status, code) VALUES('PO-6-1', 'MODEL-A', 'dispatched', '6-1')"
        )
        session_cursor = self.conn.execute(
            "INSERT INTO dispatch_sessions(order_id, dispatched_at) VALUES(?, '2026-06-28T14:00:00')",
            (order_cursor.lastrowid,),
        )
        self._insert_audit(100, 70, -30, "st_consume", "2026-06-28T14:00:05")
        self.conn.execute(
            """
            INSERT INTO st_dispatch_consumptions(dispatch_session_id, order_id, part_number, used_qty, consumed_at)
            VALUES(?, ?, 'PART-1', 30, '2026-06-28T14:00:06')
            """,
            (session_cursor.lastrowid, order_cursor.lastrowid),
        )
        self._insert_audit(70, 50, -20, "st_consume", "2026-06-30T09:00:05")

        resolved = resolve_cutoff_batch("6-1")
        self.assertEqual(resolved["cutoff_at"], "2026-06-28T14:00:06")

        with TemporaryDirectory() as tmp:
            path = self._make_genlin_file(tmp, physical_qty=68, book_qty=70)
            wb = Workbook()
            ws = wb.active
            ws.append(['料號', '廠商', 'MOQ', '6-1', None, None, '6-2', None, None])
            ws.append(['PART-1', '', 1000, 0, 30, 70, 0, 20, 50])
            main_path = Path(tmp) / 'main.xlsx'
            wb.save(main_path)
            wb.close()
            db.set_setting('main_file_path', str(main_path))
            db.start_inventory_count_session(cutoff_at=resolved['cutoff_at'], cutoff_code='6-1')
            st_before = db.get_st_inventory_stock()["PART-1"]
            with self.assertRaisesRegex(ValueError, "至少勾選"):
                commit_st_reconcile_stop_loss(
                    path,
                    resolved["cutoff_at"],
                    source_filename="batch-cutoff_2026-06-29.xlsx",
                    cutoff_label=resolved["code"],
                )

            self.assertEqual(db.get_st_inventory_stock()["PART-1"], st_before)
            self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM st_reconcile_alignments").fetchone()[0], 0)


class StReconcileCutoffBatchTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(db._CREATE_SQL)

        @contextmanager
        def temp_conn():
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

        self.get_conn_patcher = patch.object(db, "get_conn", temp_conn)
        self.get_conn_patcher.start()

    def tearDown(self):
        self.get_conn_patcher.stop()
        self.conn.close()

    def _insert_order_with_session(self, code: str, dispatched_at: str, rolled_back_at: str = "") -> int:
        cursor = self.conn.execute(
            "INSERT INTO orders(po_number, model, status, code) VALUES(?, ?, 'dispatched', ?)",
            (f"PO-{code}", f"MODEL-{code}", code),
        )
        order_id = cursor.lastrowid
        session_cursor = self.conn.execute(
            "INSERT INTO dispatch_sessions(order_id, dispatched_at, rolled_back_at) VALUES(?, ?, ?)",
            (order_id, dispatched_at, rolled_back_at),
        )
        return int(session_cursor.lastrowid)

    def _insert_consumption(self, session_id: int, consumed_at: str, part_number: str = "PART-1", used_qty: float = 1) -> None:
        self.conn.execute(
            """
            INSERT INTO st_dispatch_consumptions(dispatch_session_id, order_id, part_number, used_qty, consumed_at)
            SELECT ?, order_id, ?, ?, ? FROM dispatch_sessions WHERE id=?
            """,
            (session_id, part_number, used_qty, consumed_at, session_id),
        )

    def test_cutoff_options_exclude_rolled_back_and_sort_newest_first(self):
        self._insert_order_with_session("1-3", "2026-06-11T09:32:08")
        self._insert_order_with_session("6-3", "2026-06-28T14:05:00")
        self._insert_order_with_session("9-9", "2026-07-01T10:00:00", rolled_back_at="2026-07-02T08:00:00")

        options = db.get_st_reconcile_cutoff_batch_options()

        codes = [option["code"] for option in options]
        self.assertEqual(codes, ["6-3", "1-3"])
        self.assertNotIn("9-9", codes)
        self.assertEqual(options[0]["dispatched_at"], "2026-06-28T14:05:00")

    def test_resolve_cutoff_batch_uses_last_consumption_time_to_absorb_own_batch(self):
        session_id = self._insert_order_with_session("6-1", "2026-06-28T14:32:08")
        self._insert_consumption(session_id, "2026-06-28T14:32:15")

        resolved = resolve_cutoff_batch("6-1")

        self.assertEqual(resolved["dispatched_at"], "2026-06-28T14:32:08")
        self.assertEqual(resolved["cutoff_at"], "2026-06-28T14:32:15")
        with self.assertRaises(ValueError):
            resolve_cutoff_batch("99-99")
        with self.assertRaises(ValueError):
            resolve_cutoff_batch("")

    def test_resolve_cutoff_batch_without_consumption_falls_back_to_dispatched_at(self):
        self._insert_order_with_session("7-1", "2026-07-01T09:00:00")

        resolved = resolve_cutoff_batch("7-1")

        self.assertEqual(resolved["cutoff_at"], "2026-07-01T09:00:00")


if __name__ == "__main__":
    unittest.main()
