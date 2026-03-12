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
        self.assertFalse(snapshot["AAA"]["moq_manual"])
        self.assertEqual(snapshot["BBB"]["stock_qty"], 0)
        self.assertEqual(snapshot["BBB"]["moq"], 12)
        self.assertFalse(snapshot["BBB"]["moq_manual"])

    def test_update_snapshot_stock_only_changes_target_parts(self):
        db.save_snapshot({"AAA": 8, "BBB": 5}, {"AAA": 8, "BBB": 12})

        updated = db.update_snapshot_stock({"AAA": 0})
        snapshot = db.get_snapshot()

        self.assertEqual(updated, 1)
        self.assertEqual(snapshot["AAA"]["stock_qty"], 0)
        self.assertEqual(snapshot["BBB"]["stock_qty"], 5)

    def test_upsert_snapshot_moq_updates_existing_part_without_changing_cutoff(self):
        db.save_snapshot({"AAA": 8}, {"AAA": 0})
        self.conn.execute(
            "UPDATE inventory_snapshot SET snapshot_at='2026-03-12T11:05:45.000000' WHERE part_number='AAA'"
        )

        saved_part = db.upsert_snapshot_moq("aaa", 1200)
        row = self.conn.execute(
            "SELECT moq, snapshot_at FROM inventory_snapshot WHERE part_number='AAA'"
        ).fetchone()

        self.assertEqual(saved_part, "AAA")
        self.assertEqual(row["moq"], 1200)
        self.assertEqual(row["snapshot_at"], "2026-03-12T11:05:45.000000")
        self.assertEqual(
            self.conn.execute("SELECT moq_manual FROM inventory_snapshot WHERE part_number='AAA'").fetchone()["moq_manual"],
            1,
        )

    def test_upsert_snapshot_moq_inserts_missing_part_with_existing_snapshot_cutoff(self):
        db.save_snapshot({"AAA": 8}, {"AAA": 500})
        self.conn.execute(
            "UPDATE inventory_snapshot SET snapshot_at='2026-03-12T11:05:45.000000' WHERE part_number='AAA'"
        )

        saved_part = db.upsert_snapshot_moq("bbb", 3000)
        row = self.conn.execute(
            "SELECT stock_qty, moq, snapshot_at FROM inventory_snapshot WHERE part_number='BBB'"
        ).fetchone()

        self.assertEqual(saved_part, "BBB")
        self.assertEqual(row["stock_qty"], 0)
        self.assertEqual(row["moq"], 3000)
        self.assertEqual(row["snapshot_at"], "2026-03-12T11:05:45.000000")
        self.assertEqual(
            self.conn.execute("SELECT moq_manual FROM inventory_snapshot WHERE part_number='BBB'").fetchone()["moq_manual"],
            1,
        )

    def test_save_snapshot_preserves_manual_moq_flags_for_selected_parts(self):
        db.save_snapshot({"AAA": 8, "BBB": 5}, {"AAA": 500, "BBB": 1200}, manual_moq_parts={"BBB"})

        snapshot = db.get_snapshot()
        manual_moq = db.get_manual_snapshot_moq()

        self.assertFalse(snapshot["AAA"]["moq_manual"])
        self.assertTrue(snapshot["BBB"]["moq_manual"])
        self.assertEqual(manual_moq, {"BBB": 1200})

    def test_save_bom_file_keeps_source_metadata(self):
        db.save_bom_file({
            "id": "bom-1",
            "filename": "formal.xlsx",
            "filepath": "C:/formal.xlsx",
            "source_filename": "legacy.xls",
            "source_format": ".xls",
            "is_converted": True,
            "po_number": "123",
            "model": "MODEL-A",
            "pcb": "PCB-A",
            "group_model": "MODEL-A",
            "order_qty": 10,
            "uploaded_at": "2026-03-12T10:00:00",
            "components": [],
        })

        bom = db.get_bom_file("bom-1")

        self.assertEqual(bom["source_filename"], "legacy.xls")
        self.assertEqual(bom["source_format"], ".xls")
        self.assertEqual(bom["is_converted"], 1)

    def test_save_bom_file_preserves_existing_revisions(self):
        db.save_bom_file({
            "id": "bom-1",
            "filename": "formal.xlsx",
            "filepath": "C:/formal.xlsx",
            "source_filename": "legacy.xls",
            "source_format": ".xls",
            "is_converted": True,
            "po_number": "123",
            "model": "MODEL-A",
            "pcb": "PCB-A",
            "group_model": "MODEL-A",
            "order_qty": 10,
            "uploaded_at": "2026-03-12T10:00:00",
            "components": [],
        })
        db.save_bom_revision({
            "bom_file_id": "bom-1",
            "filename": "formal.xlsx",
            "filepath": "C:/history/formal_v001.xlsx",
            "source_action": "upload",
            "note": "上傳 BOM",
        })

        db.save_bom_file({
            "id": "bom-1",
            "filename": "formal.xlsx",
            "filepath": "C:/formal.xlsx",
            "source_filename": "legacy.xls",
            "source_format": ".xls",
            "is_converted": True,
            "po_number": "456",
            "model": "MODEL-A",
            "pcb": "PCB-B",
            "group_model": "MODEL-A",
            "order_qty": 20,
            "uploaded_at": "2026-03-12T11:00:00",
            "components": [],
        })

        revisions = db.get_bom_revisions("bom-1")
        bom = db.get_bom_file("bom-1")

        self.assertEqual(len(revisions), 1)
        self.assertEqual(revisions[0]["source_action"], "upload")
        self.assertEqual(bom["pcb"], "PCB-B")


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

    def test_upsert_orders_clears_pending_supplements_before_rebuild(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('1', 'OLD', 'PCB', 1, 'pending', 0, 1, 'n', 'n', '')"
        )
        order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]
        db.replace_order_supplements([order_id], {order_id: {"PART-1": 3000}})

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

        supplement_count = self.conn.execute("SELECT COUNT(*) FROM order_supplements").fetchone()[0]
        self.assertEqual(supplement_count, 0)

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

    def test_replace_and_get_order_supplements_normalize_parts(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('1', 'M1', 'PCB', 1, 'merged', 0, 1, 'n', 'n', '')"
        )
        order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]

        db.replace_order_supplements([order_id], {order_id: {" part-1 ": 3000, "PART-2": 0}})

        supplements = db.get_order_supplements([order_id])

        self.assertEqual(supplements, {order_id: {"PART-1": 3000.0}})


class DispatchConsumptionTests(InMemoryDbTestCase):
    def test_get_all_dispatched_consumption_ignores_records_at_or_before_snapshot(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('1', 'M1', 'PCB', 1, 'dispatched', 0, 1, 'n', 'n', '')"
        )
        order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]

        db.save_snapshot({"PART-1": 100}, {"PART-1": 25})
        self.conn.execute(
            "UPDATE inventory_snapshot SET snapshot_at='2026-03-12T11:05:45.000000'"
        )

        self.conn.execute(
            "INSERT INTO dispatch_records(order_id, part_number, needed_qty, prev_qty_cs, decision, dispatched_at) "
            "VALUES(?,?,?,?,?,?)",
            (order_id, "PART-1", 40, 0, "None", "2026-03-12T10:00:00.000000"),
        )
        self.conn.execute(
            "INSERT INTO dispatch_records(order_id, part_number, needed_qty, prev_qty_cs, decision, dispatched_at) "
            "VALUES(?,?,?,?,?,?)",
            (order_id, "PART-1", 15, 0, "None", "2026-03-12T12:00:00.000000"),
        )
        self.conn.execute(
            "INSERT INTO dispatch_records(order_id, part_number, needed_qty, prev_qty_cs, decision, dispatched_at) "
            "VALUES(?,?,?,?,?,?)",
            (order_id, "PART-1", 99, 0, "Shortage", "2026-03-12T12:30:00.000000"),
        )

        consumption = db.get_all_dispatched_consumption(db.get_snapshot_taken_at())

        self.assertEqual(consumption, {"PART-1": 15.0})


class BomOrderTests(InMemoryDbTestCase):
    def test_save_bom_order_updates_group_and_sort_order(self):
        db.save_bom_file({
            "id": "bom-1",
            "filename": "one.xlsx",
            "filepath": "C:/one.xlsx",
            "po_number": "1",
            "model": "MODEL-1",
            "pcb": "PCB-1",
            "group_model": "GROUP-A",
            "order_qty": 10,
            "uploaded_at": "2026-03-12T10:00:00",
            "components": [],
        })
        db.save_bom_file({
            "id": "bom-2",
            "filename": "two.xlsx",
            "filepath": "C:/two.xlsx",
            "po_number": "2",
            "model": "MODEL-2",
            "pcb": "PCB-2",
            "group_model": "GROUP-B",
            "order_qty": 20,
            "uploaded_at": "2026-03-12T10:01:00",
            "components": [],
        })

        updated = db.save_bom_order([
            {"model": "GROUP-B", "item_ids": ["bom-2", "bom-1"]},
        ])
        bom_files = db.get_bom_files()

        self.assertEqual(updated, 2)
        self.assertEqual([bom["id"] for bom in bom_files], ["bom-2", "bom-1"])
        self.assertEqual(bom_files[0]["group_model"], "GROUP-B")
        self.assertEqual(bom_files[1]["group_model"], "GROUP-B")
