from __future__ import annotations

import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date, timedelta
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

    def test_lead_actual_when_planned_too_long(self):
        self.assertEqual(ls.calc_lead_time(42, 0, 9, 7), (9.0, "actual"))
        self.assertEqual(ls.calc_lead_time(42, 0, 10, 7), (10.0, "actual"))
        self.assertEqual(ls.calc_lead_time(89, 3, 39, 7), (39.0, "actual"))
        self.assertEqual(ls.calc_lead_time(42, 5, 12, 7), (12.0, "actual"))

    def test_lead_actual_boundaries(self):
        self.assertEqual(ls.calc_lead_time(20, 0, 12, 7), (12.0, "actual"))  # 12<=12 且差 8
        self.assertEqual(ls.calc_lead_time(10, 0, 5, 7), (10.0, "planned"))  # 差 5 < 7
        self.assertEqual(ls.calc_lead_time(20, 2, 13, 7), (22.0, "planned"))  # 13 > 12
        self.assertEqual(ls.calc_lead_time(30, 0, 23, 7, 1.0, 7), (23.0, "actual"))  # 差剛好 7

    def test_lead_actual_custom_settings(self):
        self.assertEqual(ls.calc_lead_time(42, 0, 20, 7), (20.0, "actual"))  # 預設值即成立
        self.assertEqual(ls.calc_lead_time(42, 0, 30, 7), (42.0, "planned"))  # 30 > 25.2
        self.assertEqual(ls.calc_lead_time(42, 0, 30, 7, 0.8, 7), (30.0, "actual"))  # 30 <= 33.6
        self.assertEqual(ls.calc_lead_time(10, 0, 5, 7, 0.6, 5), (5.0, "actual"))
        self.assertEqual(ls.calc_lead_time(42, 0, 9, 7, 0.1, 7), (42.0, "planned"))

    def test_lead_labels(self):
        self.assertEqual(ls.lead_label("planned", 0), "預定交期")
        self.assertEqual(ls.lead_label("planned", None), "預定交期")
        self.assertEqual(ls.lead_label("planned", -2), "預定交期")
        self.assertEqual(ls.lead_label("planned", 3), "預定+延遲")
        self.assertEqual(ls.lead_label("actual"), "實際交期")
        self.assertEqual(ls.lead_label("actual", None, None), "實際交期")
        self.assertEqual(ls.lead_label("actual", 0, 42), "實際交期（預定過長）")
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
        self.on_order = {}
        # 重點廠商（015）用：正航廠商、最近進貨廠商、015 領用 / 庫存
        self.erp_suppliers = {}
        self.erp_last_purchase = {}
        self.gl_usage = {}
        self.gl_qty = {}
        self.add_patchers = [
            patch.object(ls, "_query_on_order", lambda conn, parts, *a, **k: {
                p: v for p, v in self.on_order.items() if p in parts}),
            patch.object(ls, "_query_usage", lambda conn, ware, start, end, parts=None: {
                k: v for k, v in self.usage.items() if parts is None or k in parts}),
            patch.object(ls, "_query_part_info", lambda conn, parts, ware, today, cfg: {
                p: dict(self.info.get(p, {})) for p in parts}),
            patch.object(ls, "_query_priority_suppliers", lambda conn, keywords: {
                cid: names for cid, names in self.erp_suppliers.items()
                if ls.match_priority_supplier(names[0], keywords) or ls.match_priority_supplier(names[1], keywords)}),
            patch.object(ls, "_query_priority_parts", lambda conn, ids: {
                p: dict(v) for p, v in self.erp_last_purchase.items() if v["supplier_id"] in ids}),
            patch.object(ls, "_query_gl_usage", lambda conn, ware, start, end, parts: {
                p: v for p, v in self.gl_usage.items() if p in parts}),
            patch.object(ls, "_query_ware_qty", lambda conn, ware, parts: {
                p: v for p, v in self.gl_qty.items() if p in parts}),
        ]
        for p in self.add_patchers:
            p.start()
        # 既有案例的數字以 90 天窗口計（日均 = 用量 / 90）；預設 365 另有專屬案例
        db.set_setting("low_stock_lookback_days", "90")

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


class OnOrderTests(DbCase):
    def test_calc_status_on_order(self):
        self.assertEqual(ls.calc_status(True, 10, 100, 90), "on_order")
        self.assertEqual(ls.calc_status(True, 10, 100, 89), "low")
        self.assertEqual(ls.calc_status(True, 10, 100), "low")
        self.assertEqual(ls.calc_status(True, 100, 100, 50), "ok")
        self.assertEqual(ls.calc_status(False, 10, 100, 500), "disabled")

    def test_run_suggestion_writes_on_order(self):
        self.usage = {"A-1": (900.0, 5)}
        self.on_order = {"A-1": (500.0, "2026-11-05")}
        summary = ls.run_suggestion()
        self.assertTrue(summary["on_order_ok"])
        r = self.row("A-1")
        self.assertEqual((r["on_order_qty"], r["next_expected_date"], r["on_order_note"]), (500.0, "2026-11-05", ""))
        self.assertTrue(r["on_order_synced_at"])

    def test_on_order_failure_counts_zero_with_note(self):
        self.usage = {"A-1": (900.0, 5)}
        self.on_order = {"A-1": (500.0, "2026-11-05")}
        ls.run_suggestion()
        with patch.object(ls, "_query_on_order", side_effect=RuntimeError("boom")):
            with self.assertLogs("low_stock", level="WARNING"):
                summary = ls.run_suggestion()
        self.assertFalse(summary["on_order_ok"])
        r = self.row("A-1")
        self.assertEqual(r["on_order_qty"], 0)
        self.assertEqual(r["next_expected_date"], "")
        self.assertIn("在途量查詢失敗", r["on_order_note"])
        self.assertIsNotNone(r["threshold_qty"])  # 其他部分照常寫入
        self.set_stock("A-1", 1)
        self.assertEqual(ls.list_rows()[0]["status"], "low")

    def test_list_rows_and_alerts(self):
        self.usage = {"A-1": (900.0, 5), "B-1": (900.0, 5), "C-1": (900.0, 5)}
        self.on_order = {"A-1": (1000.0, "2026-11-05"), "B-1": (5.0, "2026-11-09")}
        ls.run_suggestion()  # 門檻皆 105
        self.set_stock("A-1", 10)
        self.set_stock("B-1", 10)
        self.set_stock("C-1", 500)
        rows = ls.list_rows()
        self.assertEqual([(r["part_number"], r["status"]) for r in rows],
                         [("B-1", "low"), ("A-1", "on_order"), ("C-1", "ok")])
        self.assertEqual(rows[0]["on_order_qty"], 5.0)
        self.assertEqual(rows[2]["on_order_qty"], 0)
        self.assertEqual(rows[2]["next_expected_date"], "")
        self.assertEqual([a["part_number"] for a in ls.list_alerts()], ["B-1"])

    def test_suggest_window_only_for_candidates_usage_uses_lookback(self):
        db.set_setting("low_stock_lookback_days", "")
        calls = []
        long_usage = {"A-1": (3650.0, 5), "B-1": (730.0, 9)}  # 365 天窗口：A、B 都有量
        short_usage = {"A-1": (900.0, 5)}  # 90 天窗口：只有 A 符合最少單據數

        def fake(conn, ware, start, end, parts=None):
            days = (end - start).days
            calls.append(days)
            return dict(long_usage if days == 365 else short_usage)

        with patch.object(ls, "_query_usage", fake):
            summary = ls.run_suggestion()
        self.assertEqual(sorted(calls), [90, 365])
        self.assertEqual(summary["candidates"], 1)  # 只有 A-1 在 90 天內符合
        self.assertEqual(self.row("A-1")["avg_daily_usage"], 3650 / 365)  # 日均用 365 天窗口
        self.assertIsNone(self.conn.execute("SELECT 1 FROM low_stock_watch WHERE part_number='B-1'").fetchone())

    def test_same_windows_query_once(self):
        calls = []

        def fake(conn, ware, start, end, parts=None):
            calls.append((end - start).days)
            return {}

        db.set_setting("low_stock_lookback_days", "")
        db.set_setting("low_stock_suggest_lookback_days", "365")
        with patch.object(ls, "_query_usage", fake):
            ls.run_suggestion()
        self.assertEqual(calls, [365])

    def test_config_defaults_and_env_override(self):
        db.set_setting("low_stock_lookback_days", "")
        cfg = ls.get_config()
        self.assertEqual((cfg["lookback_days"], cfg["suggest_lookback_days"]), (365, 90))
        with patch.dict("os.environ", {"LOW_STOCK_LOOKBACK_DAYS": "180"}):
            self.assertEqual(ls.get_config()["lookback_days"], 180)
            db.set_setting("low_stock_lookback_days", "100")  # 設定表優先於環境變數
            self.assertEqual(ls.get_config()["lookback_days"], 100)
        with patch.dict("os.environ", {"LOW_STOCK_LOOKBACK_DAYS": "abc"}):
            db.set_setting("low_stock_lookback_days", "")
            self.assertEqual(ls.get_config()["lookback_days"], 365)
        with self.assertRaises(ls.LowStockError):
            ls.validate_config({"suggest_lookback_days": 3})

    def test_suggested_po_qty_respects_moq(self):
        from app.services import moq as moq_service

        self.usage = {"A-1": (900.0, 5), "B-1": (900.0, 5), "C-1": (900.0, 5)}
        ls.run_suggestion()  # 門檻皆 105
        self.set_stock("A-1", 10)  # 缺口 95 < MOQ 500 → 500
        self.set_stock("B-1", 10)  # 缺口 95 > MOQ 50 → 95
        self.set_stock("C-1", 10)  # 沒有 MOQ → 95
        self.set_stock("A-1", 10)
        with patch.object(moq_service, "site_moq_map", lambda: {"A-1": 500.0, "B-1": 50.0, "C-1": 0.0}):
            rows = {r["part_number"]: r for r in ls.list_rows()}
            alerts = {a["part_number"]: a for a in ls.list_alerts()}
        self.assertEqual((rows["A-1"]["suggested_po_qty"], rows["A-1"]["moq"], rows["A-1"]["moq_source"]), (500, 500, "site_master"))
        self.assertEqual(rows["B-1"]["suggested_po_qty"], 95)
        self.assertEqual((rows["C-1"]["suggested_po_qty"], rows["C-1"]["moq"], rows["C-1"]["moq_source"]), (95, 0, "none"))
        self.assertEqual(alerts["A-1"]["suggested_po_qty"], 500)

    def test_suggested_po_gap_minimum_one(self):
        self.assertEqual(ls.suggested_po_gap({"threshold": 100, "qty": 10, "on_order_qty": 0}), 90)
        self.assertEqual(ls.suggested_po_gap({"threshold": 100, "qty": 200, "on_order_qty": 0}), 1)
        self.assertEqual(ls.suggested_po_gap({"threshold": 100, "qty": 10, "on_order_qty": 95}), 1)
        self.assertEqual(ls.suggested_po_gap({"threshold": None, "qty": 10}), 1)

    def test_migration_adds_columns_to_old_table(self):
        self.conn.execute("DROP TABLE low_stock_watch")
        self.conn.execute("CREATE TABLE low_stock_watch (part_number TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '')")
        db.ensure_low_stock_table()
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(low_stock_watch)").fetchall()}
        self.assertTrue({"on_order_qty", "next_expected_date", "on_order_synced_at", "on_order_note"} <= cols)

    def test_config_rules(self):
        self.assertEqual(ls.get_config()["on_order_max_overdue_days"], 90)
        self.assertEqual(ls.validate_config({"on_order_max_overdue_days": 0}), {"on_order_max_overdue_days": 0})
        for bad in (-1, 3651, 1.5):
            with self.assertRaises(ls.LowStockError):
                ls.validate_config({"on_order_max_overdue_days": bad})
        self.assertEqual(ls.CONFIG_LABELS["on_order_max_overdue_days"], "在途逾期排除天數")

    def test_refresh_on_order_updates_and_swallows_failure(self):
        self.usage = {"A-1": (900.0, 5)}
        ls.run_suggestion()
        self.on_order = {"A-1": (7.0, "2026-11-01")}
        self.assertEqual(ls.refresh_on_order(), 1)
        self.assertEqual(self.row("A-1")["on_order_qty"], 7.0)
        with patch.object(ls, "_query_on_order", side_effect=RuntimeError("x")):
            self.assertEqual(ls.refresh_on_order(), 1)
        self.assertEqual(self.row("A-1")["on_order_qty"], 0)
        with patch.object(ls, "_erp_session", side_effect=erp_stock.ErpUnavailable("down")):
            self.assertEqual(ls.refresh_on_order(), 1)
        self.assertIn("在途量查詢失敗", self.row("A-1")["on_order_note"])

    def test_st_sync_refreshes_on_order_independently(self):
        from app.services import st_erp_sync
        with patch.object(ls, "refresh_on_order", side_effect=RuntimeError("boom")) as refresh, \
                patch.object(erp_stock, "get_st_stock", return_value={
                    "rows": [{"part_number": "A-1", "quantity": 5, "name": "x"}], "ware_id": "001"}), \
                patch.object(st_erp_sync, "apply_st_inventory_snapshot",
                             return_value={"part_count": 1, "loaded_at": "t"}):
            result = st_erp_sync.sync_st_inventory_from_erp()
        refresh.assert_called_once()
        self.assertTrue(result["ok"])


class QueryOnOrderTests(unittest.TestCase):
    class FakeCursor:
        def __init__(self, rows):
            self.rows, self.calls = rows, []

        def execute(self, sql, params=()):
            self.calls.append((sql, params))

        def fetchall(self):
            return self.rows

    class FakeConn:
        def __init__(self, cursor):
            self._cursor = cursor

        def cursor(self):
            return self._cursor

    def query(self, rows, **kw):
        cur = self.FakeCursor(rows)
        out = ls._query_on_order(self.FakeConn(cur), ["a-1", "B-2"], date(2026, 10, 29), 90)
        return out, cur

    def test_sums_open_and_earliest_date(self):
        rows = [
            ("a-1", "PO1", 1, 100.0, 20261120),
            ("A-1", "PO2", 1, 50.0, 20261105),
            ("A-1", "PO3", 2, 0.0, 20261001),    # 已全數進貨
            ("A-1", "PO4", 1, -5.0, 20261001),   # 超收
            ("B-2", "PO5", 1, 30.0, 0),          # 無效交期 → 不計日期
        ]
        out, _ = self.query(rows)
        self.assertEqual(out, {"A-1": (150.0, "2026-11-05"), "B-2": (30.0, "")})

    def test_no_open_rows_means_absent(self):
        out, _ = self.query([("A-1", "PO1", 1, 0, 20261105)])
        self.assertEqual(out, {})

    def test_sql_parameterized_with_cutoff(self):
        _, cur = self.query([])
        sql, params = cur.calls[0]
        self.assertNotIn("A-1", sql)
        self.assertIn("s.PreInDate >= %s", sql)
        self.assertIn("h.BillStatus = 0", sql)
        self.assertIn("h.EndacaseDate = 0", sql)
        self.assertEqual(params[0], 20260731)  # 2026-10-29 減 90 天
        self.assertEqual(params[1:], ("A-1", "B-2", "A-1", "B-2"))

    def test_batches_large_part_lists(self):
        cur = self.FakeCursor([])
        parts = [f"P{i}" for i in range(ls.IN_CHUNK + 1)]
        ls._query_on_order(self.FakeConn(cur), parts, date(2026, 10, 29), 90)
        self.assertEqual(len(cur.calls), 2)


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
        self.assertEqual(r["avg_daily_usage"], 300 / 90)  # 單一料號用 lookback_days
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


class ActualLeadConfigTests(DbCase):
    def setUp(self):
        super().setUp()
        self.usage = {"A-1": (900.0, 5), "M-1": (900.0, 5)}  # 日均 10
        stats = {"planned_lead_days": 42, "actual_lead_days": 30, "delay_days": 0, "delay_samples": 1}
        self.info = {"A-1": dict(stats), "M-1": dict(stats)}
        ls.run_suggestion()
        ls.update_part("M-1", lead_time_days=30)

    def test_defaults(self):
        cfg = ls.get_config()
        self.assertEqual((cfg["actual_lead_ratio"], cfg["actual_lead_min_gap_days"], cfg["min_cover_days"]), (0.6, 7, 7))

    def test_validation(self):
        for bad in ({"actual_lead_ratio": 1.1}, {"actual_lead_ratio": -0.1}, {"actual_lead_min_gap_days": 366},
                    {"actual_lead_min_gap_days": -1}, {"actual_lead_ratio": "x"}):
            with self.assertRaises(ls.LowStockError):
                ls.save_config(bad)
        self.assertEqual(ls.save_config({"actual_lead_ratio": 0.5})["actual_lead_ratio"], 0.5)

    def test_save_config_recomputes_auto_but_not_manual(self):
        r = self.row("A-1")
        self.assertEqual((r["lead_time_days"], r["lead_time_source"]), (42, "planned"))
        ls.save_config({"actual_lead_ratio": 0.8})  # 30 <= 33.6 且差 12
        r = self.row("A-1")
        self.assertEqual((r["lead_time_days"], r["lead_time_source"], r["threshold_qty"]), (30, "actual", 450))
        m = self.row("M-1")
        self.assertEqual((m["lead_time_days"], m["lead_time_source"]), (30, "manual"))
        ls.save_config({"actual_lead_min_gap_days": 13})  # 差 12 < 13 → 回原公式
        self.assertEqual(self.row("A-1")["lead_time_source"], "planned")

    def test_min_cover_days_configurable(self):
        ls.save_config({"actual_lead_ratio": 0.8})  # 交期 30 → 30*10*1.5 = 450
        ls.save_config({"min_cover_days": 60})  # 10*60 = 600 > 450
        self.assertEqual(ls.get_config()["min_cover_days"], 60)
        self.assertEqual(self.row("A-1")["threshold_qty"], 600)


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

    def test_on_order_fields_and_status(self):
        self.conn.execute(
            "UPDATE low_stock_watch SET on_order_qty=?, next_expected_date=?, on_order_note=? WHERE part_number='A-1'",
            (200.0, "2026-11-05", ""))
        self.conn.commit()
        data = self.client.get("/api/low-stock/alerts").json()
        self.assertEqual(data["count"], 0)  # 10 + 200 >= 105 → 已下單
        rows = {r["part_number"]: r for r in self.client.get("/api/low-stock/watchlist").json()["rows"]}
        self.assertEqual(rows["A-1"]["status"], "on_order")
        self.assertEqual(rows["A-1"]["on_order_qty"], 200.0)
        self.assertEqual(rows["A-1"]["next_expected_date"], "2026-11-05")
        self.assertEqual(rows["B-1"]["on_order_qty"], 0)
        self.assertEqual(rows["B-1"]["next_expected_date"], "")
        self.assertIn("on_order_note", rows["B-1"])
        self.assertEqual(self.client.get("/api/low-stock/watchlist").json()["low_count"], 0)
        # 在途不足仍是 low，alerts 帶欄位
        self.conn.execute("UPDATE low_stock_watch SET on_order_qty=20 WHERE part_number='A-1'")
        self.conn.commit()
        alert = self.client.get("/api/low-stock/alerts").json()["alerts"][0]
        self.assertEqual((alert["on_order_qty"], alert["next_expected_date"]), (20.0, "2026-11-05"))
        self.assertIn("on_order_note", alert)

    def test_alerts_never_calls_erp(self):
        with patch.object(ls, "_erp_session", side_effect=AssertionError("no ERP")):
            self.assertEqual(self.client.get("/api/low-stock/alerts").status_code, 200)

    def test_alerts_sorted_by_cover(self):
        ls.update_part("C-1", enabled=True)
        self.set_stock("C-1", 1)
        parts = [a["part_number"] for a in self.client.get("/api/low-stock/alerts").json()["alerts"]]
        self.assertEqual(parts, ["C-1", "A-1"])

    def test_watchlist(self):
        db.set_setting("low_stock_lookback_days", "")
        data = self.client.get("/api/low-stock/watchlist").json()
        self.assertEqual(data["low_count"], 1)
        self.assertEqual(data["settings"]["lookback_days"], 365)
        self.assertEqual(data["settings"]["suggest_lookback_days"], 90)
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

    def test_item_functions(self):
        """舊關注清單的新增／修改／移除函式保留（HTTP /items 已改成新清單的手動新增料號，見 test_low_stock_plan）。"""
        self.assertEqual(ls.add_part("p/q #9", threshold=3)["part_number"], "P/Q #9")
        with self.assertRaises(ls.LowStockExists):
            ls.add_part("P/Q #9")
        ls.update_part("P/Q #9", threshold_qty=None)
        self.assertEqual(self.row("P/Q #9")["threshold_source"], "auto")
        ls.update_part("P/Q #9", note="n")
        self.assertEqual(self.row("P/Q #9")["note"], "n")
        with self.assertRaises(ls.LowStockNotFound):
            ls.update_part("ZZ", note="n")
        self.assertTrue(ls.remove_part("P/Q #9"))
        self.assertFalse(ls.remove_part("P/Q #9"))

    def test_edit_auth_requirements(self):
        self.assertFalse(request_requires_edit_auth(make_request("GET", "/api/low-stock/alerts")))
        self.assertFalse(request_requires_edit_auth(make_request("GET", "/api/low-stock/watchlist")))
        for path in ("/api/low-stock/suggest", "/api/low-stock/items", "/api/low-stock/items/update",
                     "/api/low-stock/items/remove", "/api/low-stock/settings"):
            self.assertTrue(request_requires_edit_auth(make_request("POST", path)), path)


class PrioritySupplierTests(DbCase):
    def setUp(self):
        super().setUp()
        app = FastAPI()
        app.include_router(low_stock_router.router, prefix="/api")
        self.client = TestClient(app)
        self.usage = {"A-1": (900.0, 5), "B-1": (90.0, 4), "C-1": (900.0, 5), "D-1": (90.0, 4)}
        ls.run_suggestion()
        self.set_stock("A-1", 10)    # low
        self.set_stock("B-1", 1000)  # ok
        self.set_stock("C-1", 5)     # low
        self.set_stock("D-1", 1000)  # ok
        self.set_supplier("A-1", "其他電子")
        self.set_supplier("B-1", "萬晉興業有限")
        self.set_supplier("C-1", "")
        self.set_supplier("D-1", "崧嵐企業有限")

    def set_supplier(self, part, name):
        self.conn.execute("UPDATE low_stock_watch SET last_purchase_supplier=? WHERE part_number=?", (name, part))
        self.conn.commit()

    def rows(self):
        return self.client.get("/api/low-stock/watchlist").json()["rows"]

    def test_default_list(self):
        self.assertEqual(ls.get_priority_suppliers(), ["萬晉", "崧嵐"])
        self.assertEqual(self.client.get("/api/low-stock/priority-suppliers").json(), {"suppliers": ["萬晉", "崧嵐"]})
        self.assertEqual(self.client.get("/api/low-stock/watchlist").json()["priority_suppliers"], ["萬晉", "崧嵐"])
        db.set_setting(ls.PRIORITY_SUPPLIERS_KEY, "not json")
        self.assertEqual(ls.get_priority_suppliers(), ["萬晉", "崧嵐"])

    def test_match_tolerates_case_width_and_spaces(self):
        self.assertEqual(ls.match_priority_supplier("萬晉興業有限", ["萬晉", "崧嵐"]), "萬晉")
        self.assertEqual(ls.match_priority_supplier("崧 嵐企業", ["萬晉", "崧嵐"]), "崧嵐")
        self.assertEqual(ls.match_priority_supplier("ＡＢＣ　Corp", ["abc"]), "abc")
        self.assertEqual(ls.match_priority_supplier("abc corp", ["ＡＢＣ"]), "ＡＢＣ")
        self.assertEqual(ls.match_priority_supplier("其他電子", ["萬晉", "崧嵐"]), "")
        self.assertEqual(ls.match_priority_supplier("", ["萬晉"]), "")
        self.assertEqual(ls.match_priority_supplier("萬晉", []), "")

    def test_parse_list(self):
        self.assertEqual(ls.parse_priority_suppliers("萬晉、崧嵐, ABC，abc ;  "), ["萬晉", "崧嵐", "ABC"])
        self.assertEqual(ls.parse_priority_suppliers(["萬晉、崧嵐", " ", "萬晉"]), ["萬晉", "崧嵐"])
        self.assertEqual(ls.parse_priority_suppliers(""), [])
        with self.assertRaises(ls.LowStockError):
            ls.parse_priority_suppliers([f"S{i}" for i in range(ls.PRIORITY_SUPPLIER_MAX + 1)])

    def test_rows_tagged_and_sorted_first(self):
        rows = self.rows()
        by_part = {r["part_number"]: r for r in rows}
        self.assertEqual(by_part["B-1"]["priority_supplier"], "萬晉")
        self.assertTrue(by_part["B-1"]["is_priority_supplier"])
        self.assertEqual(by_part["D-1"]["priority_supplier"], "崧嵐")
        self.assertEqual(by_part["A-1"]["priority_supplier"], "")
        self.assertFalse(by_part["A-1"]["is_priority_supplier"])
        self.assertFalse(by_part["C-1"]["is_priority_supplier"])
        order = [r["part_number"] for r in rows]
        # 重點廠商（皆 ok）排最上面，其內與其外都維持原排序（狀態 → 可撐天數）
        self.assertEqual(set(order[:2]), {"B-1", "D-1"})
        self.assertEqual(order[2:], ["C-1", "A-1"])
        self.assertEqual([r["status"] for r in rows[2:]], ["low", "low"])

    def test_alerts_unchanged(self):
        data = self.client.get("/api/low-stock/alerts").json()
        self.assertEqual([a["part_number"] for a in data["alerts"]], ["C-1", "A-1"])
        self.assertNotIn("priority_supplier", data["alerts"][0])
        self.assertNotIn("is_priority_supplier", data["alerts"][0])
        self.set_stock("B-1", 0)  # 重點廠商變 low，alerts 仍只依可撐天數排序
        parts = [a["part_number"] for a in self.client.get("/api/low-stock/alerts").json()["alerts"]]
        self.assertEqual(parts, ["B-1", "C-1", "A-1"])
        self.set_stock("B-1", 4)
        parts = [a["part_number"] for a in self.client.get("/api/low-stock/alerts").json()["alerts"]]
        self.assertEqual(parts, ["C-1", "A-1", "B-1"])

    def test_api_save_and_read(self):
        res = self.client.post("/api/low-stock/priority-suppliers", json={"suppliers": "其他、 萬晉 "})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), {"suppliers": ["其他", "萬晉"]})
        self.assertEqual(self.client.get("/api/low-stock/priority-suppliers").json()["suppliers"], ["其他", "萬晉"])
        by_part = {r["part_number"]: r for r in self.rows()}
        self.assertEqual(by_part["A-1"]["priority_supplier"], "其他")
        self.assertEqual(by_part["B-1"]["priority_supplier"], "萬晉")
        self.assertFalse(by_part["D-1"]["is_priority_supplier"])
        res = self.client.post("/api/low-stock/priority-suppliers", json={"suppliers": ["崧嵐"]})
        self.assertEqual(res.json()["suppliers"], ["崧嵐"])
        too_many = [f"S{i}" for i in range(ls.PRIORITY_SUPPLIER_MAX + 1)]
        self.assertEqual(self.client.post("/api/low-stock/priority-suppliers", json={"suppliers": too_many}).status_code, 400)
        self.assertEqual(ls.get_priority_suppliers(), ["崧嵐"])

    def test_empty_list_tags_nothing(self):
        res = self.client.post("/api/low-stock/priority-suppliers", json={"suppliers": []})
        self.assertEqual(res.json()["suppliers"], [])
        self.assertEqual(ls.get_priority_suppliers(), [])
        rows = self.rows()
        self.assertFalse(any(r["is_priority_supplier"] for r in rows))
        self.assertTrue(all(r["priority_supplier"] == "" for r in rows))
        self.assertEqual([r["status"] for r in rows], ["low", "low", "ok", "ok"])

    def test_post_requires_edit_auth(self):
        path = "/api/low-stock/priority-suppliers"
        self.assertFalse(request_requires_edit_auth(make_request("GET", path)))
        self.assertTrue(request_requires_edit_auth(make_request("POST", path)))


class MainFileFilterTests(DbCase):
    def setUp(self):
        super().setUp()
        ls._MAIN_PARTS_CACHE.update(key=None, parts=None)
        app = FastAPI()
        app.include_router(low_stock_router.router, prefix="/api")
        self.client = TestClient(app)
        self.usage = {"A-1": (900.0, 5), "B-1": (90.0, 4), "C-1": (900.0, 5)}
        ls.run_suggestion()
        self.set_stock("A-1", 10)    # 門檻 105 → low
        self.set_stock("B-1", 1)     # 門檻 10.5 → low
        self.set_stock("C-1", 5000)  # ok
        ls.add_part("manual-9", threshold=100)  # 手動新增、不在主檔（現有量 0 → low）

    def use_main(self, parts):
        real = db.get_setting
        def fake_setting(key, default=""):
            return "/x/main.xlsx" if key == "main_file_path" else real(key, default)
        stack = [
            patch.object(ls.db, "get_setting", fake_setting),
            patch("os.stat", return_value=type("S", (), {"st_mtime_ns": 1})()),
            patch.object(ls.main_reader, "read_stock", lambda path: {p: 0 for p in parts}),
        ]
        for p in stack:
            p.start()
            self.addCleanup(p.stop)

    def test_filter_applied_hides_non_main_parts_but_keeps_rows(self):
        self.use_main(["A-1", "C-1", "B-1"])
        data = self.client.get("/api/low-stock/watchlist").json()
        self.assertTrue(data["main_file_filter_applied"])
        self.assertEqual(data["main_file_note"], "")
        self.assertNotIn("MANUAL-9", [r["part_number"] for r in data["rows"]])
        self.assertEqual(data["low_count"], 2)
        alerts = self.client.get("/api/low-stock/alerts").json()
        self.assertTrue(alerts["main_file_filter_applied"])
        self.assertEqual(sorted(a["part_number"] for a in alerts["alerts"]), ["A-1", "B-1"])
        self.assertIsNotNone(self.row("MANUAL-9"))

    def test_excluded_parts_hidden_from_alerts(self):
        self.use_main(["B-1", "C-1"])
        alerts = self.client.get("/api/low-stock/alerts").json()
        self.assertEqual([a["part_number"] for a in alerts["alerts"]], ["B-1"])
        self.assertEqual(self.client.get("/api/low-stock/watchlist").json()["low_count"], 1)
        self.assertIsNotNone(self.row("A-1"))

    def test_suggest_only_picks_main_parts_and_keeps_rows(self):
        self.use_main(["B-1"])
        self.usage = {"A-1": (900.0, 5), "B-1": (90.0, 4), "D-1": (900.0, 5)}
        res = self.client.post("/api/low-stock/suggest").json()
        self.assertTrue(res["main_file_filter_applied"])
        self.assertIsNone(self.row("D-1"))
        self.assertIsNotNone(self.row("A-1"))
        self.assertEqual(res["candidates"], 1)

    def test_fallback_when_main_file_unavailable(self):
        data = self.client.get("/api/low-stock/watchlist").json()
        self.assertFalse(data["main_file_filter_applied"])
        self.assertTrue(data["main_file_note"])
        self.assertIn("MANUAL-9", [r["part_number"] for r in data["rows"]])
        alerts = self.client.get("/api/low-stock/alerts").json()
        self.assertFalse(alerts["main_file_filter_applied"])
        self.assertIn("MANUAL-9", [a["part_number"] for a in alerts["alerts"]])
        res = self.client.post("/api/low-stock/suggest").json()
        self.assertFalse(res["main_file_filter_applied"])
        self.assertTrue(res["main_file_note"])

    def test_read_error_returns_none(self):
        self.use_main([])
        with patch.object(ls.main_reader, "read_stock", side_effect=OSError("boom")):
            self.assertIsNone(ls.main_file_parts())


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))

    def fetchall(self):
        return list(self.rows)


class FakeConn:
    def __init__(self, rows):
        self.cur = FakeCursor(rows)

    def cursor(self):
        return self.cur


class PriorityQueryTests(unittest.TestCase):
    def test_gl_usage_takes_positive_and_filters_flag_331_ware(self):
        conn = FakeConn([("oc-10823b ", -8600.0, 14), ("X-1", 50.0, 1)])
        out = ls._query_gl_usage(conn, "015", date(2025, 11, 11), date(2026, 11, 11), ["OC-10823B", "X-1"])
        self.assertEqual(out, {"OC-10823B": (8600.0, 14), "X-1": (0.0, 1)})
        sql, params = conn.cur.calls[0]
        self.assertIn("Flag = 331", sql)
        self.assertIn("WareID = %s", sql)
        self.assertEqual(params[:3], ("015", 20251111, 20261111))

    def test_suppliers_match_keywords_flag_2(self):
        conn = FakeConn([("P0175", "萬晉興業有限", "萬晉興業有限公司"), ("P0195", "崧嵐企業有限", ""),
                         ("P0001", "其他電子", "其他電子")])
        out = ls._query_priority_suppliers(conn, ["萬晉", "崧嵐"])
        self.assertEqual(sorted(out), ["P0175", "P0195"])
        self.assertIn("Flag = 2", conn.cur.calls[0][0])

    def test_priority_parts_any_ware_last_purchase(self):
        conn = FakeConn([("OC-10823B", 20261029, "P0175")])
        out = ls._query_priority_parts(conn, ["P0175"])
        self.assertEqual(out, {"OC-10823B": {"supplier_id": "P0175", "last_purchase_date": "2026-10-29"}})
        sql = conn.cur.calls[0][0]
        self.assertIn("Flag = 100", sql)
        self.assertNotIn("WareID", sql)  # 任何倉

    def test_ware_qty(self):
        conn = FakeConn([("OC-10823B", 530.0)])
        self.assertEqual(ls._query_ware_qty(conn, "015", ["OC-10823B"]), {"OC-10823B": 530.0})
        self.assertEqual(conn.cur.calls[0][1], ("015", "OC-10823B"))

    def test_gl_ware_id_follows_shortage_env(self):
        with patch.object(erp_stock, "get_settings", lambda: {"ware_id": "001"}):
            with patch.dict("os.environ", {"SHORTAGE_GL_WARE_ID": ""}):
                self.assertEqual(ls._gl_ware_id(), "015")
            with patch.dict("os.environ", {"SHORTAGE_GL_WARE_ID": "016"}):
                self.assertEqual(ls._gl_ware_id(), "016")


class PriorityGlTests(DbCase):
    # 重點廠商料也只列主檔內料號：OC-10823B / OK-PART 在主檔
    MAIN = {"A-1", "B-1", "W-1", "OC-10823B", "OK-PART"}

    def setUp(self):
        super().setUp()
        app = FastAPI()
        app.include_router(low_stock_router.router, prefix="/api")
        self.client = TestClient(app)
        self.today = ls.local_now().date()
        self.eta = (self.today + timedelta(days=54)).isoformat()
        # 非重點廠商的關注料（ST 領用）
        self.usage = {"A-1": (900.0, 5), "B-1": (90.0, 4), "W-1": (90.0, 4)}
        ls.run_suggestion()
        db.set_setting("low_stock_lookback_days", "365")
        ls.recompute_all()
        self.set_stock("A-1", 10)    # low
        self.set_stock("B-1", 1000)  # ok
        self.set_stock("W-1", 1000)
        self.conn.execute("UPDATE low_stock_watch SET last_purchase_supplier='其他電子' WHERE part_number IN ('A-1','B-1')")
        self.conn.commit()
        ls.add_part("OTHER-X", threshold=100)  # 非重點、不在主檔（現有量 0 → low）
        # 正航：萬晉 P0175、崧嵐 P0195、其他 P0001
        self.erp_suppliers = {"P0175": ("萬晉興業有限", "萬晉興業有限公司"), "P0195": ("崧嵐企業有限", ""),
                              "P0001": ("其他電子", "其他電子")}
        self.erp_last_purchase = {
            "OC-10823B": {"supplier_id": "P0175", "last_purchase_date": "2026-10-29"},
            "OK-PART": {"supplier_id": "P0195", "last_purchase_date": "2026-10-01"},
            "W-1": {"supplier_id": "P0195", "last_purchase_date": "2026-09-01"},
            "OTHER-X": {"supplier_id": "P0001", "last_purchase_date": "2026-09-01"},
            "A-1": {"supplier_id": "P0001", "last_purchase_date": "2026-09-01"},
        }
        self.gl_usage = {"OC-10823B": (8600.0, 14), "OK-PART": (8600.0, 14), "W-1": (3650.0, 20)}
        self.gl_qty = {"OC-10823B": 530.0, "OK-PART": 6000.0, "W-1": 200.0}
        self.on_order = {"OC-10823B": (6000.0, self.eta), "OK-PART": (6000.0, self.eta)}

    def rows(self, main=None):
        return {r["part_number"]: r for r in ls.list_rows(set(self.MAIN) if main is None else main)}

    def test_oc_10823b_listed_because_cannot_last_until_eta(self):
        res = ls.refresh_priority_parts()
        self.assertTrue(res["ok"], res)
        stored = dict(self.conn.execute("SELECT * FROM low_stock_priority WHERE part_number='OC-10823B'").fetchone())
        self.assertEqual((stored["supplier_id"], stored["gl_ware_id"], stored["gl_qty"]), ("P0175", "015", 530.0))
        self.assertEqual((stored["st_usage_qty"], stored["gl_usage_qty"], stored["usage_qty"]), (0.0, 8600.0, 8600.0))
        r = self.rows()["OC-10823B"]
        self.assertTrue(r["is_priority_supplier"])
        self.assertEqual(r["priority_supplier"], "萬晉")
        self.assertEqual(r["last_purchase_supplier"], "萬晉興業有限")
        self.assertTrue(r["priority_extra"])
        self.assertFalse(r["in_watch"])
        self.assertEqual((r["qty"], r["st_qty"], r["gl_qty"]), (530.0, 0.0, 530.0))
        self.assertAlmostEqual(r["avg_daily_usage"], 8600 / 365)
        self.assertEqual(r["threshold"], 248)  # 日均 23.56 × 預設 7 天 × 1.5 → 不是低於門檻
        self.assertTrue(r["eta_short"])        # 原因：530 / 23.56 ≈ 22.5 天 < 54 天
        self.assertEqual(r["eta_gap_days"], 32)
        self.assertEqual(r["status"], "low")
        self.assertEqual((r["on_order_qty"], r["next_expected_date"]), (6000.0, self.eta))
        # watchlist / 只看重點廠商 / 側欄 alerts 都看得到
        with patch.object(ls, "main_file_parts", lambda: set(self.MAIN)):
            wl = self.client.get("/api/low-stock/watchlist").json()
            alerts = self.client.get("/api/low-stock/alerts").json()
        self.assertTrue(wl["priority_refreshed_at"])
        self.assertEqual(wl["rows"][0]["part_number"], "OC-10823B")
        by_alert = {a["part_number"]: a for a in alerts["alerts"]}
        self.assertTrue(by_alert["OC-10823B"]["eta_short"])
        self.assertEqual(by_alert["OC-10823B"]["gl_qty"], 530.0)
        self.assertNotIn("eta_short", by_alert["A-1"])

    def test_priority_part_with_enough_stock_not_listed(self):
        ls.refresh_priority_parts()
        self.assertIsNotNone(self.conn.execute("SELECT 1 FROM low_stock_priority WHERE part_number='OK-PART'").fetchone())
        self.assertNotIn("OK-PART", self.rows())  # 6000 / 23.56 ≈ 255 天 > 54 天，且高於門檻

    def test_priority_without_on_order_only_threshold(self):
        self.on_order = {}
        ls.refresh_priority_parts()
        self.assertNotIn("OC-10823B", self.rows())  # 沒在途：530 >= 門檻 248 → 不列
        self.gl_qty["OC-10823B"] = 100.0
        ls.refresh_priority_parts()
        r = self.rows()["OC-10823B"]
        self.assertEqual(r["status"], "low")
        self.assertFalse(r["eta_short"])

    def test_non_priority_not_in_main_still_filtered(self):
        ls.refresh_priority_parts()
        self.assertNotIn("OTHER-X", self.rows())
        self.assertIsNone(self.conn.execute("SELECT 1 FROM low_stock_priority WHERE part_number='OTHER-X'").fetchone())
        with patch.object(ls, "main_file_parts", lambda: set(self.MAIN)):
            parts = [a["part_number"] for a in self.client.get("/api/low-stock/alerts").json()["alerts"]]
        self.assertNotIn("OTHER-X", parts)

    def test_non_priority_rows_unchanged(self):
        self.conn.execute("DELETE FROM low_stock_priority")
        self.conn.commit()
        before_rows = [r for r in ls.list_rows(set(self.MAIN)) if r["part_number"] in ("A-1", "B-1")]
        before_alerts = [a for a in ls.list_alerts(set(self.MAIN)) if a["part_number"] in ("A-1", "B-1")]
        with patch.object(ls, "main_file_parts", lambda: set(self.MAIN)):
            before_api = [a for a in self.client.get("/api/low-stock/alerts").json()["alerts"] if a["part_number"] == "A-1"]
        ls.refresh_priority_parts()
        after_rows = [r for r in ls.list_rows(set(self.MAIN)) if r["part_number"] in ("A-1", "B-1")]
        after_alerts = [a for a in ls.list_alerts(set(self.MAIN)) if a["part_number"] in ("A-1", "B-1")]
        with patch.object(ls, "main_file_parts", lambda: set(self.MAIN)):
            after_api = [a for a in self.client.get("/api/low-stock/alerts").json()["alerts"] if a["part_number"] == "A-1"]
        self.assertEqual(after_rows, before_rows)
        self.assertEqual(after_alerts, before_alerts)
        self.assertEqual(after_api, before_api)
        self.assertEqual(before_rows[0]["qty"], 10.0)
        for key in ls.PRIORITY_VIEW_KEYS:
            self.assertNotIn(key, after_rows[0])

    def test_watched_priority_row_uses_max_usage_and_st_plus_gl(self):
        ls.update_part("W-1", note="重要")
        ls.refresh_priority_parts()
        r = self.rows()["W-1"]
        self.assertFalse(r["priority_extra"])
        self.assertTrue(r["in_watch"])
        self.assertEqual((r["qty"], r["st_qty"], r["gl_qty"]), (1200.0, 1000.0, 200.0))
        self.assertAlmostEqual(r["avg_daily_usage"], 10.0)  # max(ST 90, 015 3650) / 365
        self.assertEqual(r["threshold"], 105)                # 10 × 7 × 1.5
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["note"], "重要")
        ls.update_part("W-1", threshold_qty=5000)            # 手動門檻照用
        self.assertEqual(self.rows()["W-1"]["threshold"], 5000)
        self.assertEqual(self.rows()["W-1"]["status"], "low")

    def test_dismissed_priority_part_not_listed_and_watch_add(self):
        ls.refresh_priority_parts()
        # v2026.11.12.1：/api/low-stock/items 改成新清單的手動新增料號；舊關注清單函式仍在，直接呼叫
        ls.add_part("OC-10823B")
        r = self.rows()["OC-10823B"]
        self.assertTrue(r["in_watch"])        # 已在關注且在主檔 → 一般關注列，可編輯
        self.assertFalse(r["priority_extra"])
        self.assertTrue(r["is_priority_supplier"])
        ls.update_part("OC-10823B", note="n")
        self.assertTrue(ls.remove_part("OC-10823B"))
        self.assertNotIn("OC-10823B", self.rows())

    def test_erp_failure_keeps_old_data(self):
        self.assertTrue(ls.refresh_priority_parts()["ok"])
        before = [dict(r) for r in self.conn.execute("SELECT * FROM low_stock_priority ORDER BY part_number")]
        with patch.object(ls, "_query_gl_usage", side_effect=erp_stock.ErpUnavailable("ERP 查詢失敗：連線逾時")):
            res = ls.refresh_priority_parts()
        self.assertFalse(res["ok"])
        self.assertIn("連線逾時", res["error"])
        with patch.object(ls, "_erp_session", side_effect=RuntimeError("down")):
            self.assertFalse(ls.refresh_priority_parts()["ok"])
        after = [dict(r) for r in self.conn.execute("SELECT * FROM low_stock_priority ORDER BY part_number")]
        self.assertEqual(after, before)
        self.assertIn("OC-10823B", self.rows())
        # 建議流程：重點廠商重算失敗不影響建議本身
        with patch.object(ls, "_query_gl_usage", side_effect=RuntimeError("boom")):
            summary = ls.run_suggestion()
        self.assertFalse(summary["priority"]["ok"])
        self.assertGreater(summary["total_rows"], 0)

    def test_empty_list_produces_no_extra_rows(self):
        ls.refresh_priority_parts()
        ls.save_priority_suppliers([])
        rows = self.rows()  # 尚未重算也不產生額外列
        self.assertNotIn("OC-10823B", rows)
        self.assertFalse(any(r["is_priority_supplier"] for r in rows.values()))
        self.assertTrue(all("gl_qty" not in r for r in rows.values()))
        self.assertTrue(ls.refresh_priority_parts()["ok"])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM low_stock_priority").fetchone()[0], 0)

    def test_suggest_and_st_sync_call_refresh(self):
        summary = ls.run_suggestion()
        self.assertTrue(summary["priority"]["ok"])
        self.assertIn("OC-10823B", self.rows())
        root = Path(__file__).resolve().parents[1]
        sync = (root / "app" / "services" / "st_erp_sync.py").read_text(encoding="utf-8")
        self.assertIn("low_stock.refresh_on_order()", sync)
        self.assertIn("low_stock.refresh_priority_parts(actor)", sync)

    def test_list_rows_never_calls_erp(self):
        ls.refresh_priority_parts()
        with patch.object(ls, "_erp_session", side_effect=AssertionError("no ERP")):
            self.assertIn("OC-10823B", self.rows())

    def _windowed(self, full, recent):
        """依查詢起日回不同窗口：近 suggest_lookback_days（90）天回 recent，其餘（lookback 365 天）回 full。"""
        recent_start = self.today - timedelta(days=90)

        def query(conn, ware, start, end, parts=None):
            src = recent if start == recent_start else full
            return {p: v for p, v in src.items() if parts is None or p in parts}
        return query

    def test_one_off_priority_parts_not_listed(self):
        # 一單一買：015 現有量 0、近 90 天只有 1–2 張單據 → 永遠低於門檻，但不是常用料 → 不列
        for i, src in enumerate(("P0175", "P0195", "P0175", "P0195")):
            self.erp_last_purchase[f"ONE-{i}"] = {"supplier_id": src, "last_purchase_date": "2019-05-01"}
        gl_full = dict(self.gl_usage, **{"ONE-0": (500.0, 1), "ONE-1": (800.0, 2), "ONE-3": (300.0, 5)})
        gl_recent = dict(self.gl_usage, **{"ONE-0": (500.0, 1), "ONE-1": (800.0, 2), "ONE-3": (300.0, 2)})
        st_full = dict(self.usage, **{"ONE-2": (120.0, 2), "ONE-3": (50.0, 4)})
        st_recent = dict(self.usage, **{"ONE-2": (120.0, 2), "ONE-3": (-10.0, 4)})  # ST 4 張但淨領用 ≤ 0
        with patch.object(ls, "_query_gl_usage", self._windowed(gl_full, gl_recent)), \
                patch.object(ls, "_query_usage", self._windowed(st_full, st_recent)):
            self.assertTrue(ls.refresh_priority_parts()["ok"])
        stored = {r["part_number"]: dict(r) for r in self.conn.execute("SELECT * FROM low_stock_priority")}
        self.assertEqual((stored["ONE-1"]["gl_bill_count"], stored["ONE-1"]["gl_recent_bill_count"]), (2, 2))
        self.assertEqual((stored["ONE-3"]["gl_bill_count"], stored["ONE-3"]["gl_recent_bill_count"]), (5, 2))
        self.assertEqual((stored["ONE-3"]["st_recent_bill_count"], stored["ONE-3"]["st_recent_usage_qty"]), (4, -10.0))
        rows = self.rows(set(self.MAIN) | {f"ONE-{i}" for i in range(4)})  # 都在主檔，擋下的只有頻率
        for i in range(4):
            self.assertIn(f"ONE-{i}", stored)
            self.assertNotIn(f"ONE-{i}", rows)
        # 單看門檻其實是低於門檻（現有量 0），擋下的只有頻率條件
        prec, word = ls._priority_records(ls.get_priority_suppliers())["ONE-1"]
        view = ls._priority_view(prec, word, None, 0.0, ls.get_config(), ls.get_priority_suppliers(), self.today)
        self.assertEqual(view["status"], "low")
        self.assertFalse(ls._priority_frequent(prec, ls.get_config()))
        self.assertIn("OC-10823B", rows)  # 常用料照列

    def test_oc_10823b_recent_three_bills_listed(self):
        # 線上情境：015 近 90 天 Flag 331 3 張（2026-09-03 ×2、2026-10-13），共 1,600；撐不到預交日 → 仍列入
        self.on_order = {"OC-10823B": (6000.0, "2027-01-04")}
        gl_recent = dict(self.gl_usage, **{"OC-10823B": (1600.0, 3)})
        with patch.object(ls, "_query_gl_usage", self._windowed(self.gl_usage, gl_recent)):
            self.assertTrue(ls.refresh_priority_parts()["ok"])
        stored = dict(self.conn.execute("SELECT * FROM low_stock_priority WHERE part_number='OC-10823B'").fetchone())
        self.assertEqual((stored["gl_recent_bill_count"], stored["gl_recent_usage_qty"]), (3, 1600.0))
        self.assertEqual((stored["gl_bill_count"], stored["gl_usage_qty"]), (14, 8600.0))  # 日均仍用 365 天窗口
        r = self.rows()["OC-10823B"]
        self.assertTrue(r["priority_extra"])
        self.assertTrue(r["eta_short"])
        self.assertEqual(r["status"], "low")
        # 最少單據數調高到 4 → 不再是常用料
        ls.save_config({"min_bills": 4})
        self.assertNotIn("OC-10823B", self.rows())

    def test_watched_main_priority_row_ignores_frequency(self):
        gl_recent = dict(self.gl_usage, **{"W-1": (30.0, 1)})
        with patch.object(ls, "_query_gl_usage", self._windowed(self.gl_usage, gl_recent)):
            ls.refresh_priority_parts()
        r = self.rows()["W-1"]
        self.assertTrue(r["in_watch"])
        self.assertFalse(r["priority_extra"])
        self.assertTrue(r["is_priority_supplier"])

    def test_priority_not_in_main_follows_main_filter(self):
        # 不在主檔的重點廠商料：額外列（NM-EXTRA）與已在關注（NM-WATCH）都一樣，主檔過濾開啟時不列、讀不到主檔（不過濾）時才列
        for part in ("NM-EXTRA", "NM-WATCH"):
            self.erp_last_purchase[part] = {"supplier_id": "P0175", "last_purchase_date": "2026-10-29"}
            self.gl_usage[part] = (8600.0, 14)
            self.gl_qty[part] = 100.0
        ls.add_part("NM-WATCH")
        self.assertTrue(ls.refresh_priority_parts()["ok"])
        self.assertIsNotNone(self.conn.execute("SELECT 1 FROM low_stock_priority WHERE part_number='NM-EXTRA'").fetchone())
        on = self.rows()
        self.assertNotIn("NM-EXTRA", on)
        self.assertNotIn("NM-WATCH", on)
        self.assertTrue(on["OC-10823B"]["priority_extra"])  # 在主檔的照列
        with patch.object(ls, "main_file_parts", lambda: set(self.MAIN)):
            wl = [r["part_number"] for r in self.client.get("/api/low-stock/watchlist").json()["rows"]]
            alerts = [a["part_number"] for a in self.client.get("/api/low-stock/alerts").json()["alerts"]]
        for parts in (wl, alerts):
            self.assertNotIn("NM-EXTRA", parts)
            self.assertNotIn("NM-WATCH", parts)
            self.assertIn("OC-10823B", parts)
        # 主檔加入後不必重算即列出
        on = self.rows(set(self.MAIN) | {"NM-EXTRA", "NM-WATCH"})
        self.assertTrue(on["NM-EXTRA"]["priority_extra"])
        self.assertFalse(on["NM-WATCH"]["priority_extra"])
        self.assertEqual(on["NM-WATCH"]["status"], "low")
        # 不過濾（讀不到主檔）→ 跟一般料一樣全列
        off = {r["part_number"]: r for r in ls.list_rows(None)}
        self.assertTrue(off["NM-EXTRA"]["priority_extra"])
        self.assertTrue(off["NM-WATCH"]["in_watch"])
        self.assertTrue(off["NM-WATCH"]["is_priority_supplier"])
        self.assertIn("OTHER-X", off)
        with patch.object(ls, "main_file_parts", lambda: None):
            wl = [r["part_number"] for r in self.client.get("/api/low-stock/watchlist").json()["rows"]]
            alerts = [a["part_number"] for a in self.client.get("/api/low-stock/alerts").json()["alerts"]]
        for parts in (wl, alerts):
            self.assertIn("NM-EXTRA", parts)
            self.assertIn("NM-WATCH", parts)

    def test_priority_table_migration_adds_recent_columns(self):
        self.conn.execute("DROP TABLE low_stock_priority")
        self.conn.execute("CREATE TABLE low_stock_priority (part_number TEXT PRIMARY KEY, gl_qty REAL NOT NULL DEFAULT 0)")
        self.conn.execute("INSERT INTO low_stock_priority(part_number, gl_qty) VALUES('OLD-1', 0)")
        db.ensure_low_stock_table()
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(low_stock_priority)").fetchall()}
        self.assertTrue({"st_recent_usage_qty", "st_recent_bill_count", "gl_recent_usage_qty", "gl_recent_bill_count"} <= cols)
        old = dict(self.conn.execute("SELECT * FROM low_stock_priority WHERE part_number='OLD-1'").fetchone())
        self.assertEqual((old["gl_recent_bill_count"], old["st_recent_bill_count"]), (0, 0))


class FrontendWiringTests(unittest.TestCase):
    def test_tab_module_and_notice_wired(self):
        """v2026.11.11.7：低庫存頁改成交期判斷清單（主清單 / 待確認區 / 廠商交期），舊門檻關注清單畫面拿掉。"""
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
        self.assertIn("/api/low-stock/plan", module)
        self.assertIn("/api/low-stock/lead-settings/supplier", module)
        self.assertIn("/api/low-stock/lead-settings/part", module)
        self.assertIn('id="low-stock-main-hint"', index)
        # 隱藏已下單：預設不勾，記在 localStorage
        self.assertIn('<input type="checkbox" id="low-stock-hide-ordered">', index)
        self.assertIn("隱藏已下單", index)
        self.assertIn("localStorage", module)
        self.assertNotIn("fetch(", module.replace("apiFetch", ""))
        # 待確認區預設收合；廠商交期區可收合
        self.assertIn('<details id="low-stock-check"', index)
        self.assertNotIn('<details id="low-stock-check" open', index)
        self.assertIn('id="low-stock-supplier-panel" class="low-stock-supplier-panel" hidden', index)
        self.assertIn("自由 ST", index)
        # 自由 ST 已確認算進可用量：畫面不再有待確認標記（字串拆開寫，測試檔本身也不出現這六個字）
        marker = "待總指揮" + "確認"
        self.assertNotIn(marker, index)
        self.assertNotIn(marker, module)
        for text in ("推定廠商", "手動", "萬晉/崧嵐", "可隨時下單", "僅參考，非交期", "改廠商"):
            self.assertIn(text, module)
        # 舊門檻 / 預設交期 / 頻率設定不再出現在畫面
        for old in ("low-stock-set-default-lead", "low-stock-set-min-bills", "重新建議"):
            self.assertNotIn(old, index)
        # v2026.11.12.1：放回手動新增料號（沒有門檻欄）與「開採購單」按鈕
        form = index[index.index('<form id="low-stock-add-form"'):index.index("</form>", index.index('<form id="low-stock-add-form"'))]
        for field in ('id="low-stock-add-part"', 'id="low-stock-add-lead"', 'id="low-stock-add-note"', "新增料號"):
            self.assertIn(field, form)
        self.assertNotIn("門檻", form)
        self.assertNotIn("threshold", form)
        for text in ('data-act="open-po"', 'data-act="remove"', "/api/low-stock/items", "/api/low-stock/items/remove",
                     "/api/low-stock/po-suggest", 'data-tab="erp-po-tab"', "prefillPurchaseOrder", "同廠商",
                     "已開過採購單", "low-stock-add-form"):
            self.assertIn(text, module)
        self.assertNotIn("threshold", module)
        po_module = (root / "static" / "modules" / "erp_purchase.js").read_text(encoding="utf-8")
        for name in ("export function prefillPurchaseOrder", "export function purchaseDraftSupplier",
                     "export function clearPurchaseDraft"):
            self.assertIn(name, po_module)
        css = (root / "static" / "style.css").read_text(encoding="utf-8")
        self.assertIn(".low-stock-row.priority-supplier", css)
        self.assertIn(".low-stock-supplier-panel", css)


if __name__ == "__main__":
    unittest.main()
