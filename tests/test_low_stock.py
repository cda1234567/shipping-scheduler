from __future__ import annotations

import sqlite3
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request

import app.database as db
from app.routers import low_stock as low_stock_router
from app.services import erp_stock, low_stock as ls
from app.services.edit_auth import request_requires_edit_auth


def make_request(method: str, path: str) -> Request:
    return Request({"type": "http", "method": method, "path": path, "headers": [], "query_string": b""})


class PureFormulaTests(unittest.TestCase):
    def test_threshold_uses_safety_factor_and_ceil(self):
        # 2.5/日 × 10 天 × 1.5 = 37.5 → 38
        self.assertEqual(ls.calc_threshold(2.5, 10, 1.5, 7), 38)

    def test_threshold_min_cover_floor(self):
        # 交期短：1/日 × 2 × 1.5 = 3，但最低可撐 7 天 → 7
        self.assertEqual(ls.calc_threshold(1, 2, 1.5, 7), 7)

    def test_threshold_float_noise_not_rounded_up(self):
        self.assertEqual(ls.calc_threshold(0.1, 10, 1.0, 0), 1)

    def test_threshold_none_without_usage(self):
        self.assertIsNone(ls.calc_threshold(None, 7, 1.5, 7))

    def test_days_of_cover(self):
        self.assertEqual(ls.calc_days_of_cover(100, 3), 33.3)
        self.assertIsNone(ls.calc_days_of_cover(100, 0))
        self.assertIsNone(ls.calc_days_of_cover(100, None))

    def test_lead_planned_adds_only_positive_delay(self):
        self.assertEqual(ls.calc_lead_time(10, 4, 20, 7), (14.0, "planned"))
        self.assertEqual(ls.calc_lead_time(10, -3, 20, 7), (10.0, "planned"))
        self.assertEqual(ls.calc_lead_time(10, None, None, 7), (10.0, "planned"))

    def test_lead_fallback_actual_then_default(self):
        self.assertEqual(ls.calc_lead_time(None, 5, 18, 7), (18.0, "actual"))
        self.assertEqual(ls.calc_lead_time(None, None, None, 7), (7.0, "default"))

    def test_lead_labels(self):
        self.assertEqual(ls.lead_label("planned", 0), "預定交期")
        self.assertEqual(ls.lead_label("planned", None), "預定交期")
        self.assertEqual(ls.lead_label("planned", -2), "預定交期")
        self.assertEqual(ls.lead_label("planned", 3), "預定+延遲")
        self.assertEqual(ls.lead_label("actual"), "實際交期")
        self.assertEqual(ls.lead_label("default"), "預設")
        self.assertEqual(ls.lead_label("manual"), "手動")

    def test_median(self):
        self.assertIsNone(ls.median_int([]))
        self.assertEqual(ls.median_int([3, 1, 2]), 2)
        self.assertEqual(ls.median_int([1, 2]), 2)  # 1.5 → 2
        self.assertEqual(ls.median_int([-3, 5, 10, 4]), 5)  # 4.5 → 5
        self.assertEqual(ls.median_int([5, 5, 20, 20, -2]), 5)

    def test_status(self):
        self.assertEqual(ls.calc_status(False, 0, 10), "disabled")
        self.assertEqual(ls.calc_status(True, 5, 10), "low")
        self.assertEqual(ls.calc_status(True, 10, 10), "ok")
        self.assertEqual(ls.calc_status(True, 0, None), "ok")


class DbCase(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db._CREATE_SQL)

        @contextmanager
        def temp_conn():
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

        @contextmanager
        def fake_session():
            yield object()

        self.patchers = [
            patch.object(db, "get_conn", temp_conn),
            patch.object(ls, "_erp_session", fake_session),
            patch.object(erp_stock, "get_settings", lambda: {"ware_id": "001"}),
        ]
        for p in self.patchers:
            p.start()
        self.usage = {}
        self.info = {}
        self.add_patchers = [
            patch.object(ls, "_query_usage", lambda conn, ware, start, end, parts=None: {
                k: v for k, v in self.usage.items() if parts is None or k in parts}),
            patch.object(ls, "_query_part_info", lambda conn, parts, ware, today, cfg: {
                p: dict(self.info.get(p, {})) for p in parts}),
        ]
        for p in self.add_patchers:
            p.start()

    def tearDown(self):
        for p in reversed(self.add_patchers + self.patchers):
            p.stop()
        self.conn.close()

    def set_stock(self, part, qty, desc=""):
        self.conn.execute(
            "INSERT OR REPLACE INTO st_inventory_snapshot(part_number, stock_qty, description, loaded_at) VALUES(?,?,?,?)",
            (part, qty, desc, "2026-10-29T10:00:00"),
        )
        self.conn.commit()

    def row(self, part):
        r = self.conn.execute("SELECT * FROM low_stock_watch WHERE part_number=?", (part,)).fetchone()
        return dict(r) if r else None


class RunSuggestionTests(DbCase):
    def test_adds_candidates_meeting_min_bills(self):
        self.usage = {"A-1": (900.0, 5), "B-1": (900.0, 2), "C-1": (-5.0, 9)}
        self.info = {"A-1": {"planned_lead_days": 10, "delay_days": 4, "delay_samples": 3,
                             "actual_lead_days": 20, "name": "電阻", "last_purchase_date": "2026-10-01",
                             "last_purchase_supplier": "甲廠"}}
        summary = ls.run_suggestion(actor="tester")
        self.assertEqual(summary["candidates"], 1)
        self.assertEqual(summary["added"], 1)
        self.assertEqual(summary["planned_lead_parts"], 1)
        self.assertEqual(summary["delay_parts"], 1)
        self.assertEqual(summary["total_rows"], 1)
        r = self.row("A-1")
        self.assertEqual(r["source"], "auto")
        self.assertEqual(r["lead_time_days"], 14)
        self.assertEqual(r["lead_time_source"], "planned")
        # 日均 10 → 10*14*1.5 = 210
        self.assertEqual(r["threshold_qty"], 210)
        self.assertEqual(r["last_purchase_supplier"], "甲廠")
        self.assertEqual(r["updated_by"], "tester")
        self.assertIsNone(self.row("B-1"))
        self.assertIsNone(self.row("C-1"))
        self.assertTrue(db.get_setting(ls.LAST_SUGGESTED_KEY))

    def test_preserves_manual_values_and_never_deletes(self):
        self.usage = {"A-1": (900.0, 5)}
        ls.run_suggestion()
        ls.update_part("A-1", threshold_qty=77, lead_time_days=30, note="重要", enabled=False)
        self.usage = {"A-1": (1800.0, 6), "Z-9": (10.0, 1)}
        self.conn.execute(
            "INSERT INTO low_stock_watch(part_number, source) VALUES('Z-9','manual')")
        self.conn.commit()
        summary = ls.run_suggestion()
        r = self.row("A-1")
        self.assertEqual((r["threshold_qty"], r["threshold_source"]), (77, "manual"))
        self.assertEqual((r["lead_time_days"], r["lead_time_source"]), (30, "manual"))
        self.assertEqual(r["note"], "重要")
        self.assertEqual(r["enabled"], 0)
        self.assertEqual(r["usage_qty"], 1800.0)  # 統計仍會刷新
        self.assertEqual(summary["kept_manual_threshold"], 1)
        self.assertEqual(summary["kept_manual_lead"], 1)
        # 不合格的既有列保留且統計被刷新
        z = self.row("Z-9")
        self.assertIsNotNone(z)
        self.assertEqual(z["bill_count"], 1)
        # 沒有任何用量的既有列也保留
        self.usage = {}
        ls.run_suggestion()
        self.assertIsNotNone(self.row("A-1"))
        self.assertIsNotNone(self.row("Z-9"))

    def test_auto_threshold_recomputed_on_existing(self):
        self.usage = {"A-1": (900.0, 5)}
        ls.run_suggestion()
        self.usage = {"A-1": (1800.0, 5)}
        ls.run_suggestion()
        r = self.row("A-1")
        # 日均 20，預設交期 7：max(20*7*1.5, 20*7)=210
        self.assertEqual(r["threshold_qty"], 210)

    def test_dismissed_not_readded(self):
        self.usage = {"A-1": (900.0, 5)}
        ls.run_suggestion()
        self.assertTrue(ls.remove_part("A-1"))
        summary = ls.run_suggestion()
        self.assertEqual(summary["added"], 0)
        self.assertEqual(self.row("A-1")["dismissed"], 1)
        self.assertEqual(summary["total_rows"], 0)

    def test_erp_error_writes_nothing(self):
        self.usage = {"A-1": (900.0, 5)}
        with patch.object(ls, "_query_usage", side_effect=erp_stock.ErpUnavailable("ERP 查詢失敗")):
            with self.assertRaises(erp_stock.ErpError):
                ls.run_suggestion()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM low_stock_watch").fetchone()[0], 0)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM activity_logs").fetchone()[0], 0)
        self.assertEqual(db.get_setting(ls.LAST_SUGGESTED_KEY), "")

    def test_info_error_writes_nothing(self):
        self.usage = {"A-1": (900.0, 5)}
        with patch.object(ls, "_query_part_info", side_effect=erp_stock.ErpUnavailable("x")):
            with self.assertRaises(erp_stock.ErpError):
                ls.run_suggestion()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM low_stock_watch").fetchone()[0], 0)

    def test_activity_logged(self):
        self.usage = {"A-1": (900.0, 5)}
        ls.run_suggestion(actor="admin_oneoff")
        log = self.conn.execute("SELECT detail FROM activity_logs").fetchone()
        self.assertIn("admin_oneoff", log["detail"])


class EditRuleTests(DbCase):
    def setUp(self):
        super().setUp()
        self.usage = {"A-1": (900.0, 5)}  # 日均 10
        self.info = {"A-1": {"planned_lead_days": 10, "delay_days": 0, "delay_samples": 1}}
        ls.run_suggestion()

    def test_manual_threshold_then_clear(self):
        ls.update_part("a-1 ", threshold_qty=500)
        r = self.row("A-1")
        self.assertEqual((r["threshold_qty"], r["threshold_source"]), (500, "manual"))
        ls.update_part("A-1", threshold_qty=None)
        r = self.row("A-1")
        self.assertEqual(r["threshold_source"], "auto")
        self.assertEqual(r["threshold_qty"], 150)  # 10*10*1.5

    def test_manual_lead_recomputes_auto_threshold(self):
        ls.update_part("A-1", lead_time_days=20)
        r = self.row("A-1")
        self.assertEqual((r["lead_time_days"], r["lead_time_source"]), (20, "manual"))
        self.assertEqual(r["threshold_qty"], 300)
        ls.update_part("A-1", lead_time_days=None)
        r = self.row("A-1")
        self.assertEqual((r["lead_time_days"], r["lead_time_source"]), (10, "planned"))
        self.assertEqual(r["threshold_qty"], 150)

    def test_manual_lead_keeps_manual_threshold(self):
        ls.update_part("A-1", threshold_qty=42)
        ls.update_part("A-1", lead_time_days=60)
        self.assertEqual(self.row("A-1")["threshold_qty"], 42)

    def test_validation_and_missing(self):
        with self.assertRaises(ls.LowStockError):
            ls.update_part("A-1", threshold_qty=-1)
        with self.assertRaises(ls.LowStockError):
            ls.update_part("A-1", lead_time_days=0)
        with self.assertRaises(ls.LowStockNotFound):
            ls.update_part("NOPE", note="x")

    def test_add_manual_without_stats_and_readd_dismissed(self):
        part = ls.add_part("x/y #1", note="手動")
        self.assertEqual(part["part_number"], "X/Y #1")
        r = self.row("X/Y #1")
        self.assertEqual((r["source"], r["lead_time_source"]), ("manual", "default"))
        self.assertEqual(r["lead_time_days"], 7)
        self.assertIsNone(r["threshold_qty"])
        with self.assertRaises(ls.LowStockExists):
            ls.add_part("X/Y #1")
        ls.remove_part("X/Y #1")
        ls.add_part("X/Y #1", threshold=5)
        r = self.row("X/Y #1")
        self.assertEqual((r["dismissed"], r["threshold_qty"], r["threshold_source"]), (0, 5, "manual"))

    def test_add_pulls_stats_when_erp_available(self):
        self.usage["N-1"] = (300.0, 4)
        ls.add_part("N-1")
        r = self.row("N-1")
        self.assertEqual(r["avg_daily_usage"], 300 / 90)
        self.assertIsNotNone(r["threshold_qty"])

    def test_add_survives_erp_failure(self):
        with patch.object(ls, "_query_usage", side_effect=erp_stock.ErpUnavailable("x")):
            ls.add_part("N-2")
        self.assertIsNotNone(self.row("N-2"))

    def test_save_config_recomputes_and_validates(self):
        ls.save_config({"safety_factor": 2.0})
        self.assertEqual(self.row("A-1")["threshold_qty"], 200)  # 10*10*2
        for bad in ({"lookback_days": 3}, {"min_bills": 0}, {"safety_factor": 11},
                    {"min_cover_days": -1}, {"default_lead_days": 0}, {"lookback_days": "abc"}):
            with self.assertRaises(ls.LowStockError):
                ls.save_config(bad)


class ApiTests(DbCase):
    def setUp(self):
        super().setUp()
        app = FastAPI()
        app.include_router(low_stock_router.router, prefix="/api")
        self.client = TestClient(app)
        self.usage = {"A-1": (900.0, 5), "B-1": (90.0, 4), "C-1": (900.0, 5)}
        ls.run_suggestion()
        self.set_stock("A-1", 10, "電阻")   # 門檻 105 → low
        self.set_stock("B-1", 1000)         # ok
        self.set_stock("C-1", 5)            # low，之後停用
        ls.update_part("C-1", enabled=False)
        db.set_setting("st_inventory_loaded_at", "2026-10-29T10:00:00")
        db.set_setting("st_inventory_source", "erp")

    def test_alerts_shape_and_filter(self):
        data = self.client.get("/api/low-stock/alerts").json()
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["st_inventory_source"], "erp")
        self.assertEqual(data["st_inventory_loaded_at"], "2026-10-29T10:00:00")
        self.assertRegex(data["generated_at"], r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$")
        alert = data["alerts"][0]
        self.assertEqual(alert["part_number"], "A-1")
        self.assertEqual(alert["qty"], 10)
        self.assertEqual(alert["threshold"], 105)
        self.assertEqual(alert["lead_time_label"], "預設")
        self.assertEqual(alert["days_of_cover"], 1.0)
        self.assertEqual(alert["avg_monthly_usage"], 300.0)
        for key in ("name", "threshold_source", "avg_daily_usage", "lead_time_days", "lead_time_source",
                    "planned_lead_days", "actual_lead_days", "delay_days", "last_purchase_date",
                    "last_purchase_supplier", "note"):
            self.assertIn(key, alert)

    def test_alerts_never_calls_erp(self):
        with patch.object(ls, "_erp_session", side_effect=AssertionError("no ERP")):
            self.assertEqual(self.client.get("/api/low-stock/alerts").status_code, 200)

    def test_alerts_sorted_by_cover(self):
        ls.update_part("C-1", enabled=True)
        self.set_stock("C-1", 1)
        parts = [a["part_number"] for a in self.client.get("/api/low-stock/alerts").json()["alerts"]]
        self.assertEqual(parts, ["C-1", "A-1"])

    def test_watchlist(self):
        data = self.client.get("/api/low-stock/watchlist").json()
        self.assertEqual(data["low_count"], 1)
        self.assertEqual(data["settings"]["lookback_days"], 90)
        status = {r["part_number"]: r["status"] for r in data["rows"]}
        self.assertEqual(status, {"A-1": "low", "B-1": "ok", "C-1": "disabled"})
        self.assertEqual(data["rows"][0]["part_number"], "A-1")
        self.assertTrue(data["last_suggested_at"])
        ls.remove_part("B-1")
        rows = self.client.get("/api/low-stock/watchlist").json()["rows"]
        self.assertNotIn("B-1", [r["part_number"] for r in rows])

    def test_settings_validation(self):
        self.assertEqual(self.client.get("/api/low-stock/settings").json()["min_bills"], 3)
        self.assertEqual(self.client.post("/api/low-stock/settings", json={"lookback_days": 3}).status_code, 400)
        self.assertEqual(self.client.post("/api/low-stock/settings", json={"safety_factor": 99}).status_code, 400)
        ok = self.client.post("/api/low-stock/settings", json={"safety_factor": 2, "min_bills": 2})
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(ok.json()["safety_factor"], 2)

    def test_suggest_and_erp_failure(self):
        res = self.client.post("/api/low-stock/suggest")
        self.assertEqual(res.status_code, 200)
        self.assertIn("total_rows", res.json())
        with patch.object(ls, "_query_usage", side_effect=erp_stock.ErpNotConfigured("尚未設定 ERP 連線")):
            res = self.client.post("/api/low-stock/suggest")
        self.assertEqual(res.status_code, 503)
        self.assertIn("尚未設定 ERP 連線", res.json()["detail"])

    def test_item_endpoints(self):
        res = self.client.post("/api/low-stock/items", json={"part_number": "p/q #9", "threshold_qty": 3})
        self.assertEqual(res.json()["part_number"], "P/Q #9")
        self.assertEqual(self.client.post("/api/low-stock/items", json={"part_number": "P/Q #9"}).status_code, 409)
        res = self.client.post("/api/low-stock/items/update", json={"part_number": "P/Q #9", "threshold_qty": None})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(self.row("P/Q #9")["threshold_source"], "auto")
        res = self.client.post("/api/low-stock/items/update", json={"part_number": "P/Q #9", "note": "n"})
        self.assertEqual(self.row("P/Q #9")["note"], "n")
        self.assertEqual(self.client.post("/api/low-stock/items/update", json={"part_number": "ZZ", "note": "n"}).status_code, 404)
        self.assertEqual(self.client.post("/api/low-stock/items/remove", json={"part_number": "P/Q #9"}).status_code, 200)
        self.assertEqual(self.client.post("/api/low-stock/items/remove", json={"part_number": "P/Q #9"}).status_code, 404)

    def test_edit_auth_requirements(self):
        self.assertFalse(request_requires_edit_auth(make_request("GET", "/api/low-stock/alerts")))
        self.assertFalse(request_requires_edit_auth(make_request("GET", "/api/low-stock/watchlist")))
        for path in ("/api/low-stock/suggest", "/api/low-stock/items", "/api/low-stock/items/update",
                     "/api/low-stock/items/remove", "/api/low-stock/settings"):
            self.assertTrue(request_requires_edit_auth(make_request("POST", path)), path)


class FrontendWiringTests(unittest.TestCase):
    def test_tab_module_and_notice_wired(self):
        root = Path(__file__).resolve().parents[1]
        index = (root / "static" / "index.html").read_text(encoding="utf-8")
        module = (root / "static" / "modules" / "low_stock.js").read_text(encoding="utf-8")
        self.assertIn('data-tab="low-stock-tab"', index)
        self.assertIn('id="tab-low-stock-tab"', index)
        self.assertIn('id="low-stock-badge"', index)
        self.assertIn('id="low-stock-notice"', index)
        self.assertIn('from "/static/modules/low_stock.js"', index)
        self.assertIn('runBootBackground("refreshLowStockAlerts", refreshLowStockAlerts);', index)
        st_module = (root / "static" / "modules" / "st_inventory.js").read_text(encoding="utf-8")
        self.assertIn('"st-inventory-synced"', st_module)
        self.assertIn('"st-inventory-synced"', module)
        self.assertIn("export async function initLowStock({ autoLoad = true } = {})", module)
        self.assertIn("/api/low-stock/alerts", module)
        self.assertNotIn("fetch(", module.replace("apiFetch", ""))


if __name__ == "__main__":
    unittest.main()
