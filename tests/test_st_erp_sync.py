from __future__ import annotations

import sqlite3
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.database as db
from app.routers import system as system_router
from app.services import erp_stock, st_erp_sync
from app.services import st_package_breakdowns as pkg
from app.services.inventory_count_lock import is_inventory_mutation_request
from app.services.st_inventory import ST_SOURCE_EXCEL, apply_st_inventory_snapshot


def erp_data(rows, ware_id="001", ware_name="ST倉"):
    return {
        "source": "erp",
        "ware_id": ware_id,
        "ware_name": ware_name,
        "fetched_at": "2026-10-29 10:00:00",
        "rows": [{"part_number": p, "name": n, "quantity": q} for p, n, q in rows],
    }


class ErpSyncBase(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
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

        self.patchers = [
            patch.object(db, "get_conn", temp_conn),
            patch.object(db, "_get_bom_model_groups", lambda: {}),
        ]
        for p in self.patchers:
            p.start()

    def tearDown(self):
        for p in reversed(self.patchers):
            p.stop()
        self.conn.close()

    def sync_with(self, rows):
        with patch.object(erp_stock, "get_st_stock", return_value=erp_data(rows)):
            return st_erp_sync.sync_st_inventory_from_erp()


class SyncWriteTests(ErpSyncBase):
    def test_success_writes_snapshot_audit_and_settings(self):
        db.save_st_inventory_snapshot({"A-1": 10, "B-1": 5})
        self.conn.execute("DELETE FROM st_inventory_audit_log")

        result = self.sync_with([("a-1", "電容", 12), ("C-1", "電阻", 3), ("B-1", "", 5)])

        self.assertTrue(result["ok"])
        self.assertEqual(result["part_count"], 3)
        snapshot = db.get_st_inventory_snapshot()
        self.assertEqual(snapshot["A-1"], {"stock_qty": 12.0, "description": "電容"})
        self.assertEqual(snapshot["C-1"]["stock_qty"], 3.0)
        logs = {row["part_number"]: row for row in db.get_st_inventory_audit_log()}
        self.assertEqual(set(logs), {"A-1", "C-1"})  # B-1 未變動不寫 audit（與上傳相同）
        self.assertEqual({row["reason"] for row in logs.values()}, {"st_inventory_upload"})
        self.assertEqual({row["actor"] for row in logs.values()}, {"erp_sync"})
        self.assertEqual(db.get_setting("st_inventory_source"), "erp")
        self.assertEqual(db.get_setting("st_inventory_part_count"), "3")
        self.assertIn("正航 ERP", db.get_setting("st_inventory_filename"))
        self.assertEqual(db.get_setting("st_inventory_file_path"), "")
        self.assertTrue(db.get_setting("st_inventory_loaded_at"))
        self.assertEqual(db.get_setting("st_inventory_last_sync_error"), "")
        acts = self.conn.execute("SELECT * FROM activity_logs WHERE action='st_inventory_upload'").fetchall()
        self.assertEqual(len(acts), 1)

    def test_erp_baseline_is_recognised_by_reconcile(self):
        self.sync_with([("A-1", "", 40)])
        baselines = db.get_st_inventory_upload_baselines("2999-01-01T00:00:00")
        self.assertEqual(baselines["A-1"]["baseline_qty"], 40.0)

    def test_duplicate_parts_are_summed(self):
        self.sync_with([("A-1", "x", 4), ("a-1", "", 6)])
        self.assertEqual(db.get_st_inventory_stock(), {"A-1": 10.0})


class SyncFailureTests(ErpSyncBase):
    def seed(self):
        apply_st_inventory_snapshot(
            {f"P-{i}": float(i) for i in range(10)}, {"P-1": "d"},
            source=ST_SOURCE_EXCEL, filename="old.xlsx", file_path="/x/old.xlsx",
        )
        self.before = (
            db.get_st_inventory_snapshot(),
            {k: db.get_setting(k) for k in (
                "st_inventory_filename", "st_inventory_loaded_at", "st_inventory_part_count",
                "st_inventory_file_path", "st_inventory_source")},
            self.conn.execute("SELECT COUNT(*) FROM st_inventory_audit_log").fetchone()[0],
        )

    def assert_untouched(self):
        after = (
            db.get_st_inventory_snapshot(),
            {k: db.get_setting(k) for k in self.before[1]},
            self.conn.execute("SELECT COUNT(*) FROM st_inventory_audit_log").fetchone()[0],
        )
        self.assertEqual(after, self.before)

    def test_erp_unavailable_keeps_everything(self):
        self.seed()
        with patch.object(erp_stock, "get_st_stock", side_effect=erp_stock.ErpUnavailable("無法連線 ERP")):
            with self.assertRaises(st_erp_sync.StErpSyncError) as ctx:
                st_erp_sync.sync_st_inventory_from_erp()
        self.assertIn("無法連線 ERP", str(ctx.exception))
        self.assert_untouched()
        self.assertIn("無法連線 ERP", db.get_setting("st_inventory_last_sync_error"))

    def test_unexpected_exception_keeps_everything(self):
        self.seed()
        with patch.object(erp_stock, "get_st_stock", side_effect=RuntimeError("boom")):
            with self.assertRaises(st_erp_sync.StErpSyncError):
                st_erp_sync.sync_st_inventory_from_erp()
        self.assert_untouched()

    def test_empty_result_keeps_everything(self):
        self.seed()
        with patch.object(erp_stock, "get_st_stock", return_value=erp_data([("  ", "x", 1)])):
            with self.assertRaises(st_erp_sync.StErpSyncError) as ctx:
                st_erp_sync.sync_st_inventory_from_erp()
        self.assertIn("0 筆", str(ctx.exception))
        self.assert_untouched()

    def test_too_few_rows_keeps_everything(self):
        self.seed()  # 現有 10 筆；4 筆 < 50%
        with patch.object(erp_stock, "get_st_stock", return_value=erp_data([(f"P-{i}", "", 1) for i in range(4)])):
            with self.assertRaises(st_erp_sync.StErpSyncError):
                st_erp_sync.sync_st_inventory_from_erp()
        self.assert_untouched()

    def test_exactly_half_is_accepted(self):
        self.seed()
        self.sync_with([(f"P-{i}", "", 1) for i in range(5)])
        self.assertEqual(len(db.get_st_inventory_snapshot()), 5)

    def test_first_sync_on_empty_snapshot_has_no_ratio_guard(self):
        self.sync_with([("A-1", "", 1)])
        self.assertEqual(db.get_st_inventory_stock(), {"A-1": 1.0})

    def test_success_clears_previous_error(self):
        self.seed()
        with patch.object(erp_stock, "get_st_stock", side_effect=erp_stock.ErpUnavailable("down")):
            with self.assertRaises(st_erp_sync.StErpSyncError):
                st_erp_sync.sync_st_inventory_from_erp()
        self.sync_with([(f"P-{i}", "", 1) for i in range(10)])
        self.assertEqual(db.get_setting("st_inventory_last_sync_error"), "")


class DispatchInteractionTests(ErpSyncBase):
    """同步後回滾「同步前的發料」：行為必須與上傳新 Excel 完全一致。"""

    def dispatch_300(self):
        self.conn.execute("INSERT INTO orders(id, status) VALUES(1, 'dispatched')")
        session = db.save_dispatch_session(
            order_id=1, previous_status="merged", backup_path="b.xlsx", main_file_path="m.xlsx",
        )
        result = pkg.consume_st_package_breakdowns({1: {"PART-1": 300}})
        db.save_st_dispatch_consumptions([
            {"dispatch_session_id": session["id"], "order_id": 1, **result["details"][0]}
        ])
        return session["id"]

    def run_flow(self, replace):
        db.save_st_inventory_snapshot({"PART-1": 1000})
        sid = self.dispatch_300()
        self.assertEqual(db.get_st_inventory_stock()["PART-1"], 700.0)
        replace()  # 以 700 取代（ERP / 新 Excel 已含該次發料）
        after_replace = db.get_st_inventory_stock()["PART-1"]
        pkg.restore_st_package_consumptions([sid], [1])
        return after_replace, db.get_st_inventory_stock()["PART-1"]

    def test_sync_then_rollback_matches_excel_upload(self):
        erp_result = self.run_flow(lambda: self.sync_with([("PART-1", "", 700)]))
        self.tearDown()
        self.setUp()
        excel_result = self.run_flow(lambda: apply_st_inventory_snapshot(
            {"PART-1": 700.0}, {}, source=ST_SOURCE_EXCEL, filename="new.xlsx"))
        self.assertEqual(erp_result, excel_result)
        # 同步是「取代」不是「再扣」：不會 700-300；回滾則照現有語意加回 used_qty
        self.assertEqual(erp_result, (700.0, 1000.0))

    def test_sync_does_not_deduct_again_and_later_dispatch_uses_erp_value(self):
        db.save_st_inventory_snapshot({"PART-1": 1000})
        self.dispatch_300()
        self.sync_with([("PART-1", "", 650)])
        self.assertEqual(db.get_st_inventory_stock()["PART-1"], 650.0)
        result = pkg.consume_st_package_breakdowns({1: {"PART-1": 50}})
        self.assertEqual(result["details"][0]["stock_before"], 650.0)
        self.assertEqual(db.get_st_inventory_stock()["PART-1"], 600.0)


class AutoSyncTests(ErpSyncBase):
    def test_parse_auto_sync_minutes(self):
        parse = st_erp_sync.parse_auto_sync_minutes
        self.assertEqual(parse(""), 60)
        self.assertEqual(parse("   "), 60)
        self.assertEqual(parse("30"), 30)
        self.assertEqual(parse("0"), 0)
        self.assertEqual(parse("-5"), 0)
        self.assertEqual(parse("abc"), 0)
        self.assertEqual(parse("1"), st_erp_sync.AUTO_SYNC_MIN_MINUTES)
        with patch.dict("os.environ", {"ERP_ST_AUTO_SYNC_MINUTES": "15"}):
            self.assertEqual(parse(), 15)
        with patch.dict("os.environ", {}, clear=False) as env:
            env.pop("ERP_ST_AUTO_SYNC_MINUTES", None)
            self.assertEqual(parse(), 60)

    def test_scheduler_disabled_when_zero(self):
        scheduler = st_erp_sync.StErpAutoSyncScheduler()
        self.assertFalse(scheduler.start(0))
        self.assertIsNone(scheduler._thread)

    def test_run_once_skips_when_unconfigured_or_counting(self):
        with patch.object(erp_stock, "is_configured", return_value=False), \
             patch.object(st_erp_sync, "sync_st_inventory_from_erp") as sync:
            self.assertFalse(st_erp_sync.run_auto_sync_once())
            sync.assert_not_called()
        with patch.object(erp_stock, "is_configured", return_value=True), \
             patch.object(st_erp_sync, "get_inventory_count_lock", return_value={"id": 1}), \
             patch.object(st_erp_sync, "sync_st_inventory_from_erp") as sync:
            self.assertFalse(st_erp_sync.run_auto_sync_once())
            sync.assert_not_called()

    def test_run_once_never_raises(self):
        with patch.object(erp_stock, "is_configured", return_value=True), \
             patch.object(st_erp_sync, "get_inventory_count_lock", return_value=None):
            with patch.object(st_erp_sync, "sync_st_inventory_from_erp",
                              side_effect=st_erp_sync.StErpSyncError("x")):
                self.assertFalse(st_erp_sync.run_auto_sync_once())
            with patch.object(st_erp_sync, "sync_st_inventory_from_erp", side_effect=RuntimeError("x")):
                self.assertFalse(st_erp_sync.run_auto_sync_once())
            with patch.object(st_erp_sync, "sync_st_inventory_from_erp", return_value={"ok": True}):
                self.assertTrue(st_erp_sync.run_auto_sync_once())


class EndpointTests(ErpSyncBase):
    def setUp(self):
        super().setUp()
        app = FastAPI()
        app.include_router(system_router.router, prefix="/api")
        self.client = TestClient(app)

    def test_erp_sync_endpoint_success_and_info(self):
        with patch.object(erp_stock, "get_st_stock", return_value=erp_data([("A-1", "電容", 5)])):
            res = self.client.post("/api/system/st-inventory/erp-sync")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["part_count"], 1)
        info = self.client.get("/api/system/st-inventory/info").json()
        self.assertTrue(info["loaded"])
        self.assertEqual(info["source"], "erp")
        self.assertEqual(info["source_label"], "正航 ERP")
        self.assertEqual(info["last_sync_error"], "")

    def test_erp_sync_endpoint_failure_is_503_and_info_shows_error(self):
        db.save_st_inventory_snapshot({"A-1": 5})
        db.set_setting("st_inventory_filename", "old.xlsx")
        with patch.object(erp_stock, "get_st_stock", side_effect=erp_stock.ErpUnavailable("無法連線 ERP")):
            res = self.client.post("/api/system/st-inventory/erp-sync")
        self.assertEqual(res.status_code, 503)
        self.assertIn("無法連線 ERP", res.json()["detail"])
        self.assertEqual(db.get_st_inventory_stock(), {"A-1": 5.0})
        info = self.client.get("/api/system/st-inventory/info").json()
        self.assertEqual(info["source"], "excel")
        self.assertIn("無法連線 ERP", info["last_sync_error"])

    def test_erp_sync_is_blocked_during_inventory_count(self):
        self.assertTrue(is_inventory_mutation_request("POST", "/api/system/st-inventory/erp-sync"))


if __name__ == "__main__":
    unittest.main()
