from __future__ import annotations

import sqlite3
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import app.database as db
from app.services.reconcile_core import theoretical_stock, theoretical_stock_with_details


class InMemoryDbTestCase(unittest.TestCase):
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

    def insert_audit(
        self,
        part_number: str,
        old_qty: float | None,
        new_qty: float,
        delta: float | None,
        reason: str,
        changed_at: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO st_inventory_audit_log(part_number, old_qty, new_qty, delta, reason, actor, changed_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (part_number, old_qty, new_qty, delta, reason, "test", changed_at),
        )

    def insert_consumption(
        self,
        *,
        row_id: int,
        order_id: int,
        part_number: str,
        used_qty: float,
        consumed_at: str,
        rolled_back_at: str = "",
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO orders(id, status, created_at, updated_at)
            VALUES(?, 'dispatched', ?, ?)
            """,
            (order_id, consumed_at, consumed_at),
        )
        self.conn.execute(
            """
            INSERT INTO dispatch_sessions(id, order_id, previous_status, backup_path, main_file_path, dispatched_at)
            VALUES(?, ?, 'merged', '', '', ?)
            """,
            (row_id, order_id, consumed_at),
        )
        self.conn.execute(
            """
            INSERT INTO st_dispatch_consumptions(
                id, dispatch_session_id, order_id, part_number, used_qty,
                stock_before, stock_after, package_before, package_after,
                consumed_at, rolled_back_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                row_id,
                row_id,
                order_id,
                part_number,
                used_qty,
                0,
                0,
                "",
                "",
                consumed_at,
                rolled_back_at,
            ),
        )


class TheoreticalStockTests(InMemoryDbTestCase):
    def test_as_of_stock_handles_rollback_and_reconsume_across_cutoff(self):
        self.insert_audit("PART-1", None, 100, None, "st_inventory_upload", "2026-04-01T08:00:00")
        self.insert_audit("PART-1", 100, 70, -30, "st_consume", "2026-04-03T09:00:00")
        self.insert_audit("PART-1", 70, 100, 30, "st_rollback", "2026-04-05T10:00:00")
        self.insert_audit("PART-1", 100, 65, -35, "st_consume", "2026-04-07T11:00:00")

        before_rollback = theoretical_stock("2026-04-04T23:59:59")
        after_rollback = theoretical_stock("2026-04-06T23:59:59")
        after_reconsume = theoretical_stock("2026-04-08T00:00:00")

        self.assertEqual(before_rollback["PART-1"], 70.0)
        self.assertEqual(after_rollback["PART-1"], 100.0)
        self.assertEqual(after_reconsume["PART-1"], 65.0)

    def test_reconcile_adjustment_reason_is_excluded(self):
        self.insert_audit("PART-1", None, 100, None, "st_inventory_upload", "2026-04-01T08:00:00")
        self.insert_audit("PART-1", 100, 90, -10, "st_consume", "2026-04-02T09:00:00")
        self.insert_audit(
            "PART-1",
            90,
            120,
            30,
            "st_reconcile_adjustment",
            "2026-04-03T09:00:00",
        )
        self.insert_audit("PART-1", 120, 115, -5, "manual_edit", "2026-04-04T09:00:00")

        result = theoretical_stock("2026-04-05T00:00:00")

        self.assertEqual(result["PART-1"], 85.0)

    def test_anchor_baseline_uses_anchor_time_and_quantities(self):
        self.insert_audit("PART-1", None, 100, None, "st_inventory_upload", "2026-04-01T08:00:00")
        self.insert_audit("PART-1", 100, 80, -20, "st_consume", "2026-04-02T09:00:00")
        self.insert_audit("PART-1", 80, 70, -10, "st_consume", "2026-04-05T09:00:00")

        result = theoretical_stock(
            "2026-04-06T00:00:00",
            anchor={"aligned_at": "2026-04-03T00:00:00", "baseline_qty": {"PART-1": 82}},
        )

        self.assertEqual(result["PART-1"], 72.0)

    def test_anchor_missing_part_falls_back_to_upload_baseline(self):
        self.insert_audit("PART-1", None, 100, None, "st_inventory_upload", "2026-04-01T08:00:00")
        self.insert_audit("PART-1", 100, 95, -5, "st_consume", "2026-04-04T09:00:00")

        result = theoretical_stock(
            "2026-04-06T00:00:00",
            anchor={"aligned_at": "2026-04-03T00:00:00", "baseline_qty": {"PART-2": 50}},
            part_numbers=["PART-1"],
        )

        self.assertEqual(result["PART-1"], 95.0)

    def test_order_details_are_filtered_as_of_cutoff(self):
        self.insert_audit("PART-1", None, 100, None, "st_inventory_upload", "2026-04-01T08:00:00")
        self.insert_consumption(
            row_id=1,
            order_id=101,
            part_number="PART-1",
            used_qty=10,
            consumed_at="2026-04-02T09:00:00",
        )
        self.insert_consumption(
            row_id=2,
            order_id=102,
            part_number="PART-1",
            used_qty=20,
            consumed_at="2026-04-03T09:00:00",
            rolled_back_at="2026-04-04T09:00:00",
        )
        self.insert_consumption(
            row_id=3,
            order_id=103,
            part_number="PART-1",
            used_qty=30,
            consumed_at="2026-04-05T09:00:00",
        )
        self.insert_consumption(
            row_id=4,
            order_id=104,
            part_number="PART-2",
            used_qty=40,
            consumed_at="2026-04-03T09:00:00",
            rolled_back_at="2026-04-06T09:00:00",
        )

        result = theoretical_stock_with_details("2026-04-04T12:00:00")

        self.assertEqual([row["order_id"] for row in result["order_details"]["PART-1"]], [101])
        self.assertEqual([row["order_id"] for row in result["order_details"]["PART-2"]], [104])
