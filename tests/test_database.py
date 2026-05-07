from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
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

    def test_save_st_inventory_snapshot_normalizes_part_numbers(self):
        db.save_st_inventory_snapshot(
            {" part-1 ": 8, "PART-2": 0},
            {"part-1": "Capacitor", " part-2 ": "Resistor"},
        )

        snapshot = db.get_st_inventory_snapshot()
        stock = db.get_st_inventory_stock()
        loaded_at = db.get_st_inventory_taken_at()

        self.assertEqual(snapshot["PART-1"]["stock_qty"], 8.0)
        self.assertEqual(snapshot["PART-1"]["description"], "Capacitor")
        self.assertEqual(snapshot["PART-2"]["stock_qty"], 0.0)
        self.assertEqual(snapshot["PART-2"]["description"], "Resistor")
        self.assertEqual(stock, {"PART-1": 8.0, "PART-2": 0.0})
        self.assertTrue(loaded_at)

    def test_update_st_inventory_stock_writes_audit_log(self):
        db.save_st_inventory_snapshot({"PART-1": 100}, {"PART-1": "Capacitor"})

        updated = db.update_st_inventory_stock(
            {"part-1": 75, "part-2": 20},
            reason="manual_edit",
            actor="system",
        )
        logs = db.get_st_inventory_audit_log(limit=10)
        part_logs = {row["part_number"]: row for row in logs if row["reason"] == "manual_edit"}

        self.assertEqual(updated, 2)
        self.assertEqual(part_logs["PART-1"]["old_qty"], 100.0)
        self.assertEqual(part_logs["PART-1"]["new_qty"], 75.0)
        self.assertEqual(part_logs["PART-1"]["delta"], -25.0)
        self.assertEqual(part_logs["PART-1"]["actor"], "system")
        self.assertIsNone(part_logs["PART-2"]["old_qty"])
        self.assertEqual(part_logs["PART-2"]["new_qty"], 20.0)
        self.assertIsNone(part_logs["PART-2"]["delta"])

    def test_save_st_inventory_snapshot_writes_audit_for_changed_parts(self):
        db.save_st_inventory_snapshot({"PART-1": 100, "PART-2": 50})
        self.conn.execute("DELETE FROM st_inventory_audit_log")

        db.save_st_inventory_snapshot({"PART-1": 80, "PART-3": 25})
        logs = db.get_st_inventory_audit_log(limit=10)
        logs_by_part = {row["part_number"]: row for row in logs}

        self.assertEqual(set(logs_by_part), {"PART-1", "PART-2", "PART-3"})
        self.assertEqual(logs_by_part["PART-1"]["old_qty"], 100.0)
        self.assertEqual(logs_by_part["PART-1"]["new_qty"], 80.0)
        self.assertEqual(logs_by_part["PART-1"]["delta"], -20.0)
        self.assertEqual(logs_by_part["PART-2"]["old_qty"], 50.0)
        self.assertEqual(logs_by_part["PART-2"]["new_qty"], 0.0)
        self.assertEqual(logs_by_part["PART-2"]["delta"], -50.0)
        self.assertIsNone(logs_by_part["PART-3"]["old_qty"])
        self.assertEqual(logs_by_part["PART-3"]["new_qty"], 25.0)
        self.assertEqual({row["reason"] for row in logs}, {"st_inventory_upload"})
        self.assertEqual({row["actor"] for row in logs}, {"system"})

    def test_get_st_inventory_audit_log_filters_and_sorts(self):
        db.save_st_inventory_snapshot({"PART-1": 100, "PART-2": 50})
        self.conn.execute("DELETE FROM st_inventory_audit_log")
        db.update_st_inventory_stock({"PART-1": 90}, reason="first")
        db.update_st_inventory_stock({"PART-2": 45}, reason="other")
        db.update_st_inventory_stock({"PART-1": 80}, reason="second")
        rows = self.conn.execute("SELECT id FROM st_inventory_audit_log ORDER BY id").fetchall()
        self.conn.execute(
            "UPDATE st_inventory_audit_log SET changed_at='2026-03-12T10:00:00' WHERE id=?",
            (rows[0]["id"],),
        )
        self.conn.execute(
            "UPDATE st_inventory_audit_log SET changed_at='2026-03-12T10:05:00' WHERE id=?",
            (rows[1]["id"],),
        )
        self.conn.execute(
            "UPDATE st_inventory_audit_log SET changed_at='2026-03-12T10:10:00' WHERE id=?",
            (rows[2]["id"],),
        )

        part_logs = db.get_st_inventory_audit_log(part_number="part-1", limit=10)
        limited_logs = db.get_st_inventory_audit_log(limit=2)

        self.assertEqual([row["reason"] for row in part_logs], ["second", "first"])
        self.assertEqual({row["part_number"] for row in part_logs}, {"PART-1"})
        self.assertEqual([row["reason"] for row in limited_logs], ["second", "other"])

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


class PurchaseReminderStatusTests(InMemoryDbTestCase):
    def test_set_purchase_reminder_status_persists_notified_note(self):
        with patch.object(db, "_now", side_effect=["2026-04-23T10:00:00", "2026-04-23T11:00:00"]):
            first = db.set_purchase_reminder_status(" ic-100 ", True, "先通知")
            second = db.set_purchase_reminder_status("IC-100", True, "改備註")

        statuses = db.get_purchase_reminder_statuses()

        self.assertEqual(first["part_number"], "IC-100")
        self.assertTrue(second["notified"])
        self.assertEqual(second["notified_at"], "2026-04-23T10:00:00")
        self.assertEqual(second["updated_at"], "2026-04-23T11:00:00")
        self.assertEqual(statuses["IC-100"]["note"], "改備註")

    def test_set_purchase_reminder_status_false_clears_record(self):
        with patch.object(db, "_now", side_effect=["2026-04-23T10:00:00", "2026-04-23T11:00:00"]):
            db.set_purchase_reminder_status("OC-1", True, "已通知")
            cleared = db.set_purchase_reminder_status("oc-1", False)

        self.assertFalse(cleared["notified"])
        self.assertEqual(cleared["part_number"], "OC-1")
        self.assertNotIn("OC-1", db.get_purchase_reminder_statuses())


class OrderSupplementTests(InMemoryDbTestCase):
    def test_replace_order_supplements_preserves_existing_timestamp_when_unchanged(self):
        self.conn.execute(
            "INSERT INTO orders(id, po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES(1, '1', 'MODEL', 'PCB', 1, 'pending', 0, 1, 'n', 'n', '')"
        )
        self.conn.execute(
            "INSERT INTO order_supplements(order_id, part_number, supplement_qty, note, updated_at) "
            "VALUES(1, 'PART-1', 100, '原備註', '2026-04-02T09:00:00')"
        )

        db.replace_order_supplements([1], {1: {"PART-1": 100}})
        row = self.conn.execute(
            "SELECT supplement_qty, note, updated_at FROM order_supplements WHERE order_id=1 AND part_number='PART-1'"
        ).fetchone()

        self.assertEqual(row["supplement_qty"], 100)
        self.assertEqual(row["note"], "原備註")
        self.assertEqual(row["updated_at"], "2026-04-02T09:00:00")

    def test_replace_order_supplements_updates_note_and_timestamp_when_changed(self):
        self.conn.execute(
            "INSERT INTO orders(id, po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES(1, '1', 'MODEL', 'PCB', 1, 'pending', 0, 1, 'n', 'n', '')"
        )
        self.conn.execute(
            "INSERT INTO order_supplements(order_id, part_number, supplement_qty, note, updated_at) "
            "VALUES(1, 'PART-1', 100, '原備註', '2026-04-02T09:00:00')"
        )

        db.replace_order_supplements([1], {1: {"PART-1": 200}}, {1: {"PART-1": "新備註"}})
        row = self.conn.execute(
            "SELECT supplement_qty, note, updated_at FROM order_supplements WHERE order_id=1 AND part_number='PART-1'"
        ).fetchone()

        self.assertEqual(row["supplement_qty"], 200)
        self.assertEqual(row["note"], "新備註")
        self.assertNotEqual(row["updated_at"], "2026-04-02T09:00:00")


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


class ManagedPathMigrationTests(InMemoryDbTestCase):
    def test_get_setting_repairs_stale_main_file_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            managed_dir = Path(temp_dir)
            candidate = managed_dir / "main.xlsx"
            candidate.write_bytes(b"main")

            with patch.dict(db._MANAGED_PATH_FALLBACKS, {"main_file_path": managed_dir}, clear=False):
                db.set_setting("main_file_path", r"Z:\Andy\Job\code\-\shipping-scheduler\data\main_file\main.xlsx")

                repaired = db.get_setting("main_file_path")

        stored = self.conn.execute(
            "SELECT value FROM settings WHERE key='main_file_path'"
        ).fetchone()["value"]
        self.assertEqual(repaired, str(candidate))
        self.assertEqual(stored, str(candidate))

    def test_get_setting_repairs_unc_main_file_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            managed_dir = Path(temp_dir)
            candidate = managed_dir / "main.xlsx"
            candidate.write_bytes(b"main")

            with patch.dict(db._MANAGED_PATH_FALLBACKS, {"main_file_path": managed_dir}, clear=False):
                db.set_setting("main_file_path", r"\\St-nas\個人資料夾\Andy\Job\code\-\shipping-scheduler\data\main_file\main.xlsx")

                repaired = db.get_setting("main_file_path")

        stored = self.conn.execute(
            "SELECT value FROM settings WHERE key='main_file_path'"
        ).fetchone()["value"]
        self.assertEqual(repaired, str(candidate))
        self.assertEqual(stored, str(candidate))

    def test_resolve_managed_path_can_repair_dispatch_session_path_without_setting_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            managed_dir = Path(temp_dir)
            candidate = managed_dir / "main.xlsx"
            candidate.write_bytes(b"main")

            with patch.dict(db._MANAGED_PATH_FALLBACKS, {"main_file_path": managed_dir}, clear=False):
                repaired = db.resolve_managed_path(
                    r"Z:\Andy\Job\code\-\shipping-scheduler\data\main_file\main.xlsx"
                )

        self.assertEqual(repaired, str(candidate))

    def test_get_bom_file_repairs_stale_bom_filepath(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_dir = Path(temp_dir) / "bom"
            bom_dir.mkdir()
            candidate = bom_dir / "bom-1.xlsx"
            candidate.write_bytes(b"bom")

            db.save_bom_file({
                "id": "bom-1",
                "filename": "bom-1.xlsx",
                "filepath": r"Z:\Andy\Job\code\-\shipping-scheduler\data\bom\bom-1.xlsx",
                "source_filename": "bom-1.xls",
                "source_format": ".xls",
                "is_converted": True,
                "po_number": "4500059162",
                "model": "TDA3-3",
                "pcb": "PCB-A1109",
                "group_model": "TDA3-3",
                "order_qty": 10,
                "uploaded_at": "2026-03-13T14:40:00",
                "components": [],
            })

            with patch.object(db, "_BOM_FILE_FALLBACK_DIRS", (bom_dir,)):
                bom = db.get_bom_file("bom-1")
                matched = db.get_bom_files_by_models(["TDA3-3"])

        stored = self.conn.execute(
            "SELECT filepath FROM bom_files WHERE id='bom-1'"
        ).fetchone()["filepath"]
        self.assertEqual(bom["filepath"], str(candidate))
        self.assertEqual(matched[0]["filepath"], str(candidate))
        self.assertEqual(stored, str(candidate))

    def test_get_bom_file_repairs_unc_bom_filepath(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_dir = Path(temp_dir) / "bom"
            bom_dir.mkdir()
            candidate = bom_dir / "bom-1.xlsx"
            candidate.write_bytes(b"bom")

            db.save_bom_file({
                "id": "bom-1",
                "filename": "bom-1.xlsx",
                "filepath": r"\\St-nas\個人資料夾\Andy\Job\code\-\shipping-scheduler\data\bom\bom-1.xlsx",
                "source_filename": "bom-1.xls",
                "source_format": ".xls",
                "is_converted": True,
                "po_number": "4500059162",
                "model": "TDA3-3",
                "pcb": "PCB-A1109",
                "group_model": "TDA3-3",
                "order_qty": 10,
                "uploaded_at": "2026-03-13T14:40:00",
                "components": [],
            })

            with patch.object(db, "_BOM_FILE_FALLBACK_DIRS", (bom_dir,)):
                bom = db.get_bom_file("bom-1")

        stored = self.conn.execute(
            "SELECT filepath FROM bom_files WHERE id='bom-1'"
        ).fetchone()["filepath"]
        self.assertEqual(bom["filepath"], str(candidate))
        self.assertEqual(stored, str(candidate))


class MergeDraftTests(InMemoryDbTestCase):
    def test_replace_merge_draft_round_trip_and_commit(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('4500059234', 'MODEL-A', 'PCB-A', 10, 'merged', 0, 1, 'n', 'n', '')"
        )
        order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]

        draft = db.replace_merge_draft(
            order_id=order_id,
            main_file_path="C:/main.xlsx",
            main_file_mtime_ns="123456",
            main_loaded_at="2026-03-13T09:00:00",
            decisions={"PART-1": "Shortage"},
            supplements={"PART-1": 3000},
            shortages=[{"part_number": "PART-1", "shortage_amount": 12}],
        )
        db.replace_merge_draft_files(
            draft["id"],
            [
                {
                    "bom_file_id": "bom-1",
                    "filename": "draft-a.xlsx",
                    "filepath": "C:/draft-a.xlsx",
                    "source_filename": "source-a.xlsx",
                    "source_format": ".xlsx",
                    "model": "MODEL-A",
                    "group_model": "MODEL-A",
                    "carry_overs": {"PART-1": 100},
                    "supplements": {"PART-1": 3000},
                }
            ],
        )

        active = db.get_active_merge_draft_for_order(order_id)
        active_list = db.get_active_merge_drafts([order_id])
        mapping = db.get_active_merge_draft_ids_by_order_ids([order_id])

        self.assertEqual(active["main_file_mtime_ns"], "123456")
        self.assertEqual(active["decisions"], {"PART-1": "Shortage"})
        self.assertEqual(active["supplements"], {"PART-1": 3000})
        self.assertEqual(active["shortages"], [{"part_number": "PART-1", "shortage_amount": 12}])
        self.assertEqual(mapping, {order_id: draft["id"]})
        self.assertEqual(len(active_list), 1)
        self.assertEqual(active_list[0]["files"][0]["carry_overs"], {"PART-1": 100})
        self.assertEqual(active_list[0]["files"][0]["supplements"], {"PART-1": 3000})

        marked = db.mark_merge_draft_committed(draft["id"])

        self.assertEqual(marked, 1)
        self.assertIsNone(db.get_active_merge_draft_for_order(order_id))

    def test_reactivate_merge_draft_restores_last_committed_record(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('4500059235', 'MODEL-B', 'PCB-B', 8, 'merged', 0, 1, 'n', 'n', '')"
        )
        order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]

        draft = db.replace_merge_draft(
            order_id=order_id,
            main_file_path="C:/main.xlsx",
            main_file_mtime_ns="123456",
            main_loaded_at="2026-03-13T09:00:00",
            decisions={"PART-2": "CreateRequirement"},
            supplements={"PART-2": 1200},
            shortages=[{"part_number": "PART-2", "shortage_amount": 12}],
        )
        db.mark_merge_draft_committed(draft["id"])

        committed = db.get_latest_committed_merge_draft_for_order(order_id)
        reactivated = db.reactivate_merge_draft(draft["id"])
        active = db.get_active_merge_draft_for_order(order_id)

        self.assertIsNotNone(committed)
        self.assertEqual(committed["id"], draft["id"])
        self.assertEqual(reactivated, 1)
        self.assertIsNotNone(active)
        self.assertEqual(active["id"], draft["id"])
        self.assertEqual(active["supplements"], {"PART-2": 1200})

    def test_get_expired_committed_merge_drafts_only_returns_records_older_than_retention(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('4500059236', 'MODEL-C', 'PCB-C', 5, 'merged', 0, 1, 'n', 'n', '')"
        )
        order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]

        self.conn.execute(
            "INSERT INTO merge_drafts(order_id, status, main_file_path, main_file_mtime_ns, main_loaded_at, decisions_json, supplements_json, shortages_json, created_at, updated_at, committed_at, deleted_at) "
            "VALUES(?, 'committed', 'C:/main.xlsx', '', '', '{}', '{}', '[]', '2026-01-01T00:00:00', '2026-01-01T00:00:00', '2026-01-02T00:00:00', '')",
            (order_id,),
        )
        self.conn.execute(
            "INSERT INTO merge_drafts(order_id, status, main_file_path, main_file_mtime_ns, main_loaded_at, decisions_json, supplements_json, shortages_json, created_at, updated_at, committed_at, deleted_at) "
            "VALUES(?, 'committed', 'C:/main.xlsx', '', '', '{}', '{}', '[]', '2099-01-01T00:00:00', '2099-01-01T00:00:00', '2099-01-02T00:00:00', '')",
            (order_id,),
        )

        expired = db.get_expired_committed_merge_drafts(retention_days=30)

        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0]["committed_at"], "2026-01-02T00:00:00")

    def test_get_expired_committed_merge_drafts_defaults_to_365_days(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('4500059237', 'MODEL-D', 'PCB-D', 5, 'merged', 0, 1, 'n', 'n', '')"
        )
        order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]
        recent_committed_at = (datetime.now() - timedelta(days=200)).isoformat()
        old_committed_at = (datetime.now() - timedelta(days=400)).isoformat()

        for committed_at in (recent_committed_at, old_committed_at):
            self.conn.execute(
                "INSERT INTO merge_drafts(order_id, status, main_file_path, main_file_mtime_ns, main_loaded_at, decisions_json, supplements_json, shortages_json, created_at, updated_at, committed_at, deleted_at) "
                "VALUES(?, 'committed', 'C:/main.xlsx', '', '', '{}', '{}', '[]', ?, ?, ?, '')",
                (order_id, committed_at, committed_at, committed_at),
            )

        expired = db.get_expired_committed_merge_drafts()

        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0]["committed_at"], old_committed_at)


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


class DispatchSessionTests(InMemoryDbTestCase):
    def test_get_dispatch_session_tail_returns_active_sessions_from_target(self):
        for index, po in enumerate(("A", "B", "C"), start=1):
            self.conn.execute(
                "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
                "VALUES(?, 'MODEL', 'PCB', 1, 'dispatched', ?, ?, 'n', 'n', '')",
                (po, index, index),
            )

        order_ids = [row["id"] for row in self.conn.execute("SELECT id FROM orders ORDER BY id").fetchall()]
        first = db.save_dispatch_session(order_ids[0], "merged", "C:/b1.xlsx", "C:/main.xlsx", "2026-03-12T10:00:00")
        second = db.save_dispatch_session(order_ids[1], "merged", "C:/b2.xlsx", "C:/main.xlsx", "2026-03-12T10:05:00")
        third = db.save_dispatch_session(order_ids[2], "pending", "C:/b3.xlsx", "C:/main.xlsx", "2026-03-12T10:10:00")
        db.mark_dispatch_sessions_rolled_back([third["id"]])

        tail = db.get_dispatch_session_tail(second["id"])

        self.assertEqual([row["order_id"] for row in tail], order_ids[:2][1:])
        self.assertEqual([row["id"] for row in tail], [second["id"]])
        self.assertEqual(db.get_active_dispatch_session(order_ids[0])["id"], first["id"])

    def test_delete_dispatch_records_for_orders_removes_only_target_rows(self):
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('1', 'M1', 'PCB', 1, 'dispatched', 0, 1, 'n', 'n', '')"
        )
        self.conn.execute(
            "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
            "VALUES('2', 'M2', 'PCB', 1, 'dispatched', 1, 2, 'n', 'n', '')"
        )
        ids = [row["id"] for row in self.conn.execute("SELECT id FROM orders ORDER BY id").fetchall()]
        db.save_dispatch_records(ids[0], [{"part_number": "PART-1", "needed_qty": 10}])
        db.save_dispatch_records(ids[1], [{"part_number": "PART-2", "needed_qty": 20}])

        deleted = db.delete_dispatch_records_for_orders([ids[1]])
        remaining = db.get_dispatch_records()

        self.assertEqual(deleted, 1)
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["order_id"], ids[0])


class BomOrderTests(InMemoryDbTestCase):
    def test_get_all_bom_components_by_model_dedupes_repeated_group_aliases(self):
        db.save_bom_file({
            "id": "bom-1",
            "filename": "one.xlsx",
            "filepath": "C:/one.xlsx",
            "po_number": "1",
            "model": "MODEL-A",
            "pcb": "PCB-A",
            "group_model": "MODEL-A, model-a , MODEL-B",
            "order_qty": 10,
            "uploaded_at": "2026-03-12T10:00:00",
            "components": [
                {
                    "part_number": "PB-20138A-TAB",
                    "description": "Board",
                    "needed_qty": 200,
                    "prev_qty_cs": 48,
                    "is_dash": False,
                    "is_customer_supplied": False,
                },
            ],
        })

        bom_map = db.get_all_bom_components_by_model()

        self.assertEqual(len(bom_map["MODEL-A"]), 1)
        self.assertEqual(len(bom_map["MODEL-B"]), 1)
        self.assertEqual(bom_map["MODEL-A"][0]["needed_qty"], 200)

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


class ManagedPathRepairTests(InMemoryDbTestCase):
    def test_get_setting_repairs_managed_main_file_path_in_place(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_dir = Path(temp_dir) / "main_file"
            schedule_dir = Path(temp_dir) / "schedule"
            main_dir.mkdir()
            schedule_dir.mkdir()
            repaired_main = main_dir / "main.xlsx"
            repaired_main.write_bytes(b"main")

            self.conn.execute(
                "INSERT INTO settings(key, value) VALUES('main_file_path', 'Z:/legacy/main.xlsx')"
            )

            with patch.dict(
                db._MANAGED_PATH_FALLBACKS,
                {"main_file_path": main_dir, "schedule_file_path": schedule_dir},
                clear=True,
            ):
                value = db.get_setting("main_file_path")

            stored = self.conn.execute(
                "SELECT value FROM settings WHERE key='main_file_path'"
            ).fetchone()["value"]
            self.assertEqual(value, str(repaired_main))
            self.assertEqual(stored, str(repaired_main))

    def test_repair_managed_paths_updates_draft_and_dispatch_main_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_dir = Path(temp_dir) / "main_file"
            schedule_dir = Path(temp_dir) / "schedule"
            main_dir.mkdir()
            schedule_dir.mkdir()
            repaired_main = main_dir / "main.xlsx"
            repaired_main.write_bytes(b"main")

            self.conn.execute(
                "INSERT INTO orders(po_number, model, pcb, order_qty, status, sort_order, row_index, created_at, updated_at, folder) "
                "VALUES('4500059234', 'MODEL-A', 'PCB-A', 1, 'merged', 0, 1, 'n', 'n', '')"
            )
            order_id = self.conn.execute("SELECT id FROM orders").fetchone()["id"]
            self.conn.execute(
                "INSERT INTO settings(key, value) VALUES('main_file_path', 'Z:/legacy/main.xlsx')"
            )
            self.conn.execute(
                "INSERT INTO dispatch_sessions(order_id, previous_status, backup_path, main_file_path, dispatched_at, rolled_back_at) "
                "VALUES(?, 'merged', 'backup.xlsx', 'Z:/legacy/main.xlsx', '2026-03-13T10:00:00', '')",
                (order_id,),
            )
            self.conn.execute(
                "INSERT INTO merge_drafts(order_id, status, main_file_path, main_file_mtime_ns, main_loaded_at, decisions_json, supplements_json, shortages_json, created_at, updated_at, committed_at, deleted_at) "
                "VALUES(?, 'active', 'Z:/legacy/main.xlsx', '', '', '{}', '{}', '[]', 'n', 'n', '', '')",
                (order_id,),
            )

            with patch.dict(
                db._MANAGED_PATH_FALLBACKS,
                {"main_file_path": main_dir, "schedule_file_path": schedule_dir},
                clear=True,
            ):
                repaired_count = db._repair_managed_paths(self.conn)

            self.assertEqual(repaired_count, 3)
            setting_value = self.conn.execute(
                "SELECT value FROM settings WHERE key='main_file_path'"
            ).fetchone()["value"]
            dispatch_value = self.conn.execute(
                "SELECT main_file_path FROM dispatch_sessions WHERE order_id=?",
                (order_id,),
            ).fetchone()["main_file_path"]
            draft_value = self.conn.execute(
                "SELECT main_file_path FROM merge_drafts WHERE order_id=?",
                (order_id,),
            ).fetchone()["main_file_path"]
            self.assertEqual(setting_value, str(repaired_main))
            self.assertEqual(dispatch_value, str(repaired_main))
            self.assertEqual(draft_value, str(repaired_main))
