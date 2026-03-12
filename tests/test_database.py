from __future__ import annotations

import sqlite3
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import app.database as db


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


class SnapshotTests(InMemoryDbTestCase):
    def test_save_snapshot_keeps_parts_that_only_have_moq(self):
        db.save_snapshot({"AAA": 5}, {"AAA": 8, "BBB": 12})

        snapshot = db.get_snapshot()

        self.assertEqual(snapshot["AAA"]["stock_qty"], 5)
        self.assertEqual(snapshot["AAA"]["moq"], 8)
        self.assertEqual(snapshot["BBB"]["stock_qty"], 0)
        self.assertEqual(snapshot["BBB"]["moq"], 12)

    def test_update_snapshot_stock_only_changes_target_parts(self):
        db.save_snapshot({"AAA": 8, "BBB": 5}, {"AAA": 8, "BBB": 12})

        updated = db.update_snapshot_stock({"AAA": 0})
        snapshot = db.get_snapshot()

        self.assertEqual(updated, 1)
        self.assertEqual(snapshot["AAA"]["stock_qty"], 0)
        self.assertEqual(snapshot["BBB"]["stock_qty"], 5)


class OrderReloadTests(InMemoryDbTestCase):
    def test_upsert_orders_clears_pending_decisions_before_rebuild(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('1', 'OLD', 'PCB', 1, 'pending', 0, 1, 'n', 'n', '')"
        )
        order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]
        db.save_decision(order_id, "part-1", "CreateRequirement")

        db.upsert_orders_from_schedule([
            {
                "po_number": "2",
                "model": "NEW",
                "pcb": "PCB2",
                "order_qty": 2,
                "balance_qty": None,
                "ship_date": "2026-03-12",
                "remark": "",
                "row_index": 2,
                "code": "",
            }
        ])

        order_count = self.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        decision_count = self.conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

        self.assertEqual(order_count, 1)
        self.assertEqual(decision_count, 0)

    def test_get_all_decisions_returns_latest_pending_decision_in_uppercase(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('1', 'M1', 'PCB', 1, 'pending', 0, 1, 'n', 'n', '')"
        )
        order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]

        db.save_decision(order_id, "part-2", "IgnoreOnce")
        self.conn.execute(
            "UPDATE decisions SET decided_at='2026-03-12T10:00:00' WHERE order_id=? AND part_number='PART-2'",
            (order_id,),
        )
        db.save_decision(order_id, "part-2", "Shortage")

        decisions = db.get_all_decisions()

        self.assertEqual(decisions["PART-2"], "Shortage")
