from __future__ import annotations

import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date
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
        self.add_patchers = [
            patch.object(ls, "_query_on_order", lambda conn, parts, *a, **k: {
                p: v for p, v in self.on_order.items() if p in parts}),
            patch.object(ls, "_query_usage", lambda conn, ware, start, end, parts=None: {
                k: v for k, v in self.usage.items() if parts is None or k in parts}),
            patch.object(ls, "_query_part_info", lambda conn, parts, ware, today, cfg: {
                p: dict(self.info.get(p, {})) for p in parts}),
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
        self.assertEqual((rows["A-1"]["suggested_po_qty"], rows["A-1"]["moq"], rows["A-1"]["moq_source"]), (500, 500, "主檔"))
        self.assertEqual(rows["B-1"]["suggested_po_qty"], 95)
        self.assertEqual((rows["C-1"]["suggested_po_qty"], rows["C-1"]["moq"], rows["C-1"]["moq_source"]), (95, 0, ""))
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
        self.assertIn("main_file_filter_applied", module)
        self.assertIn('id="low-stock-main-hint"', index)
        self.assertIn('id="low-stock-hide-on-order"', index)
        self.assertIn("隱藏已下單", index)
        self.assertIn("已下單", module)
        self.assertIn("localStorage", module)
        self.assertNotIn("fetch(", module.replace("apiFetch", ""))


if __name__ == "__main__":
    unittest.main()
