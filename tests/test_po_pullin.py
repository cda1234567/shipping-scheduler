from __future__ import annotations

import os
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.requests import Request

import app.database as db
from app.routers import dispatch as dispatch_router
from app.services import po_pullin as pp
from app.services import shortage_purchase as sp
from app.services.edit_auth import request_requires_edit_auth

TODAY = datetime(2026, 10, 29, 10, 0)
NEW_SHIP = "2027-01-10"  # 打件 → 2026-11-26；後段 → 2026-12-20


def line(qty, po_date, bill="PO1", row=1, supplier="S1", part="EC-1"):
    return {"part_number": part, "bill_no": bill, "row_no": row, "qty": qty,
            "po_date": date.fromisoformat(po_date) if po_date else None, "supplier_id": supplier}


class PureTests(unittest.TestCase):
    def test_required_date_uses_shortage_purchase_rules(self):
        today = date(2026, 10, 29)
        self.assertEqual(pp.required_date("smt", date(2027, 1, 10), today)[:2], (date(2026, 11, 26), False))
        self.assertEqual(pp.required_date("backend", date(2027, 1, 10), today)[:2], (date(2026, 12, 20), False))
        # 早於今天 → 今天，保留原推算日
        d, adjusted, raw = pp.required_date("smt", date(2026, 12, 10), today)
        self.assertEqual((d, adjusted, raw), (today, True, date(2026, 10, 26)))
        d, adjusted, raw = pp.required_date("backend", date(2026, 11, 5), today)
        self.assertEqual((d, adjusted, raw), (today, True, date(2026, 10, 15)))

    def test_plan_part_pull_in_days(self):
        plan = pp.plan_part(100, date(2026, 11, 26), [line(100, "2027-01-05")])
        self.assertEqual(len(plan["pull_in"]), 1)
        self.assertEqual(plan["pull_in"][0]["days_earlier"], 40)
        self.assertEqual(plan["pull_in"][0]["pull_qty"], 100)
        self.assertEqual(plan["unordered_qty"], 0)

    def test_plan_part_on_time_covers_first(self):
        lines = [line(60, "2026-11-20", row=1), line(60, "2027-02-01", row=2), line(60, "2027-03-01", row=3)]
        plan = pp.plan_part(100, date(2026, 11, 26), lines)
        self.assertEqual([l["row_no"] for l in plan["on_time"]], [1])
        self.assertEqual([(p["line"]["row_no"], p["pull_qty"]) for p in plan["pull_in"]], [(2, 40)])
        self.assertEqual(plan["unordered_qty"], 0)  # 第 3 列不需要

    def test_plan_part_on_time_line_equal_to_required_not_pulled(self):
        plan = pp.plan_part(10, date(2026, 11, 26), [line(10, "2026-11-26")])
        self.assertEqual(plan["pull_in"], [])
        self.assertEqual(len(plan["on_time"]), 1)

    def test_plan_part_unordered(self):
        self.assertEqual(pp.plan_part(80, date(2026, 11, 26), [])["unordered_qty"], 80)
        plan = pp.plan_part(100, date(2026, 11, 26), [line(30, "2026-11-01")])
        self.assertEqual(plan["unordered_qty"], 70)

    def test_plan_part_unknown_po_date_is_last_and_no_days(self):
        plan = pp.plan_part(50, date(2026, 11, 26), [line(50, None, row=2), line(50, "2026-11-01", row=1)])
        self.assertEqual([l["row_no"] for l in plan["on_time"]], [1])
        self.assertEqual(plan["pull_in"], [])

    def test_line_key(self):
        self.assertEqual(pp.make_line_key("S1", "PO1", 3, "ec-1"), "S1|PO1|3|EC-1")

    def test_draft_text(self):
        lines = [
            {"bill_no": "PO1", "row_no": 2, "part_number": "EC-1", "qty": 1000, "po_date": "2027-01-05",
             "requested_date": "2026-11-26", "days_earlier": 40, "status": "未詢問"},
            {"bill_no": "PO2", "row_no": 1, "part_number": "EC-2", "qty": 2.5, "po_date": "2027-02-01",
             "requested_date": "2026-11-26", "days_earlier": 67, "status": "已詢問"},
            {"bill_no": "PO3", "row_no": 1, "part_number": "EC-3", "qty": 5, "po_date": "2027-02-01",
             "requested_date": "2026-11-26", "days_earlier": 67, "status": "不能提前"},
        ]
        text = pp.build_draft("供一", lines)
        self.assertTrue(text.startswith("供一 您好"))
        self.assertIn("提前交貨", text)
        self.assertIn("採購單 PO1 項次 2", text)
        self.assertIn("料號 EC-1", text)
        self.assertIn("數量 1000", text)
        self.assertIn("原交期 2027-01-05", text)
        self.assertIn("希望提前至 2026-11-26（提前 40 天）", text)
        self.assertIn("數量 2.5", text)
        self.assertNotIn("PO3", text)  # 已有結論的不再詢問
        self.assertIn("謝謝", text)
        self.assertEqual(pp.build_draft("供一", [lines[2]]), "")


class Base(unittest.TestCase):
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

        self.st = {"EC-1": 0, "EC-2": 0, "EC-3": 0, "OC-1": 0, "PK-1": 0, "ZZ-9": 1000}
        self.groups = [{
            "order_id": 1, "batch_code": "1-1", "model": "M", "po_number": "P1", "ship_date": "2026-12-28",
            "items": [
                {"part": "EC-1", "desc": "0402", "need_qty": 100},
                {"part": "EC-2", "desc": "0603", "need_qty": 100},
                {"part": "EC-3", "desc": "0805", "need_qty": 100},
                {"part": "OC-1", "desc": "外殼", "need_qty": 50},
                {"part": "PK-1", "desc": "紙箱", "need_qty": 30},
                {"part": "ZZ-9", "desc": "夠用", "need_qty": 10},
            ]}]
        self.open_lines = [
            line(100, "2027-01-05", "PO1", 1, "S1", "EC-1"),        # 晚 → 要提前 40 天
            line(60, "2026-11-20", "PO2", 1, "S2", "EC-2"),         # 準時
            line(60, "2027-02-01", "PO2", 2, "S2", "EC-2"),         # 要提前
            line(30, "2026-11-01", "PO3", 1, "S1", "EC-3"),         # 準時但量不足 → 未下單 70
            line(50, "2026-12-15", "PO4", 1, "S3", "OC-1"),         # 後段準時
        ]
        self.names = {"S1": "供一", "S2": "供二", "S3": "供三"}
        self.tab_map = {}
        self.open_calls = []

        def fake_open(parts, today):
            self.open_calls.append(list(parts))
            if isinstance(self.open_lines, Exception):
                raise self.open_lines
            return [l for l in self.open_lines if l["part_number"] in parts]

        def fake_names(ids):
            if isinstance(self.names, Exception):
                raise self.names
            return {k: v for k, v in self.names.items() if k in ids}

        patches = [
            patch.object(db, "get_conn", temp_conn),
            patch.object(dispatch_router, "build_dispatch_groups", lambda ids, dec: self.groups),
            patch.object(db, "get_st_inventory_stock", lambda: self.st),
            patch.object(pp, "_fetch_open_lines", fake_open),
            patch.object(pp, "_fetch_supplier_names", fake_names),
            patch.object(pp, "_fetch_tab_stock", lambda parts, wares: self.tab_stock(parts, wares)),
            patch.object(pp, "local_now", lambda: TODAY),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def tab_stock(self, parts, wares):
        if isinstance(self.tab_map, Exception):
            raise self.tab_map
        return {k: v for k, v in self.tab_map.items() if k in parts}

    def add_order(self, status="merged", ship="2026-12-28"):
        cur = self.conn.execute(
            "INSERT INTO orders(po_number, model, status, ship_date, delivery_date, code) VALUES(?,?,?,?,?,?)",
            ("P1", "M", status, ship, ship, "1-1"))
        self.conn.commit()
        return cur.lastrowid


class BuildListTests(Base):
    def test_pull_in_lines_and_days(self):
        r = pp.build_pullin_list([1], NEW_SHIP)
        self.assertTrue(r["erp_ok"])
        parts = {p["part_number"]: p for p in r["parts"]}
        self.assertNotIn("ZZ-9", parts)  # 庫存夠
        self.assertEqual(parts["EC-1"]["required_date"], "2026-11-26")
        self.assertEqual(parts["OC-1"]["required_date"], "2026-12-20")
        self.assertEqual(parts["EC-1"]["status"], "pull_in")
        self.assertEqual(parts["OC-1"]["status"], "on_time")
        by_supplier = {s["supplier_id"]: s for s in r["suppliers"]}
        self.assertEqual(set(by_supplier), {"S1", "S2"})  # S3 準時不用提前
        l1 = by_supplier["S1"]["lines"][0]
        self.assertEqual((l1["bill_no"], l1["row_no"], l1["part_number"]), ("PO1", 1, "EC-1"))
        self.assertEqual((l1["po_date"], l1["requested_date"], l1["days_earlier"]), ("2027-01-05", "2026-11-26", 40))
        self.assertEqual(by_supplier["S1"]["supplier_name"], "供一")
        s2 = by_supplier["S2"]["lines"]
        self.assertEqual([(l["row_no"], l["pull_qty"], l["days_earlier"]) for l in s2], [(2, 40.0, 67)])
        self.assertEqual(r["summary"]["pull_in_lines"], 2)
        self.assertEqual(r["summary"]["unresolved"], 2)

    def test_unordered_parts(self):
        r = pp.build_pullin_list([1], NEW_SHIP)
        un = {p["part_number"]: p for p in r["unordered"]}
        self.assertEqual(set(un), {"EC-3", "PK-1"})
        self.assertEqual(un["PK-1"]["unordered_qty"], 30)
        self.assertEqual(un["EC-3"]["unordered_qty"], 70)
        self.assertEqual(r["summary"]["unordered_parts"], 2)

    def test_date_earlier_than_today_uses_today(self):
        r = pp.build_pullin_list([1], "2026-12-10")  # 打件推算 10/26 < 今天
        ec1 = next(p for p in r["parts"] if p["part_number"] == "EC-1")
        self.assertEqual(ec1["required_date"], "2026-10-29")
        self.assertEqual(ec1["original_required_date"], "2026-10-26")
        self.assertTrue(ec1["date_adjusted"])
        l = next(l for s in r["suppliers"] for l in s["lines"] if l["part_number"] == "EC-1")
        self.assertEqual(l["days_earlier"], (date(2027, 1, 5) - date(2026, 10, 29)).days)

    def test_category_override_and_stock_taken_into_account(self):
        db.set_part_purchase_category("EC-1", "backend", "t")
        self.st["EC-1"] = 60
        r = pp.build_pullin_list([1], NEW_SHIP)
        ec1 = next(p for p in r["parts"] if p["part_number"] == "EC-1")
        self.assertEqual(ec1["category"], "backend")
        self.assertEqual(ec1["required_date"], "2026-12-20")
        self.assertEqual(ec1["shortage"], 40)
        line1 = next(l for s in r["suppliers"] for l in s["lines"] if l["part_number"] == "EC-1")
        self.assertEqual(line1["days_earlier"], 16)
        self.assertEqual(line1["pull_qty"], 40)

    def test_converted_part_demand_and_stock(self):
        db.apply_part_conversion("EC-9", "EC-1", [], {}, [], "t")
        self.groups[0]["items"].append({"part": "EC-9", "desc": "舊", "need_qty": 20})
        self.st["EC-9"] = -10
        r = pp.build_pullin_list([1], NEW_SHIP)
        parts = {p["part_number"]: p for p in r["parts"]}
        self.assertNotIn("EC-9", parts)
        self.assertEqual(parts["EC-1"]["need"], 120)
        self.assertEqual(parts["EC-1"]["shortage"], 120)  # 有效庫存 0 + (-10) → 負值不抵
        self.assertEqual(parts["EC-1"]["merged_from"], [{"part": "EC-9", "stock": -10.0}])

    def test_allocations_shown_for_site_created_po_lines(self):
        db.save_po_line_allocations(1, "PO1", "T", [
            {"row_no": 1, "part_number": "EC-1", "order_id": 7, "order_code": "6-1", "model": "M", "qty": 100}])
        r = pp.build_pullin_list([1], NEW_SHIP)
        l = next(l for s in r["suppliers"] for l in s["lines"] if l["bill_no"] == "PO1")
        self.assertEqual(l["allocations"][0]["order_code"], "6-1")

    def test_erp_failure_degrades(self):
        self.open_lines = RuntimeError("連線逾時 secret-pw")
        r = pp.build_pullin_list([1], NEW_SHIP)
        self.assertFalse(r["erp_ok"])
        self.assertIn("正航採購單查詢失敗", r["erp_error"])
        self.assertTrue(r["warnings"][0].startswith("正航採購單查詢失敗"))
        parts = {p["part_number"]: p for p in r["parts"]}
        self.assertEqual(parts["EC-1"]["required_date"], "2026-11-26")  # 本機資料仍在
        self.assertEqual(parts["EC-1"]["status"], "unverified")
        self.assertEqual(r["unordered"], [])  # 查不到就不能說未下單
        self.assertEqual(r["suppliers"], [])
        self.assertEqual(r["summary"]["unverified_parts"], 5)

    def test_supplier_name_failure_keeps_lines(self):
        self.names = RuntimeError("x")
        r = pp.build_pullin_list([1], NEW_SHIP)
        self.assertTrue(r["erp_ok"])
        s1 = next(s for s in r["suppliers"] if s["supplier_id"] == "S1")
        self.assertEqual(s1["supplier_name"], "S1")
        self.assertTrue(any("供應商名稱" in w for w in r["warnings"]))

    def test_erp_not_queried_when_no_shortage(self):
        self.st = {k: 10 ** 6 for k in self.st}
        r = pp.build_pullin_list([1], NEW_SHIP)
        self.assertEqual(self.open_calls, [])
        self.assertEqual(r["parts"], [])

    def test_validation(self):
        with self.assertRaises(pp.PullinError):
            pp.build_pullin_list([], NEW_SHIP)
        with self.assertRaises(pp.PullinError):
            pp.build_pullin_list([1], "not-a-date")

    def test_preview_is_read_only(self):
        pp.build_pullin_list([1], NEW_SHIP)
        self.assertEqual(db.list_pullin_items(), [])
        self.assertEqual(db.count_unresolved_pullin_items(), 0)

    def test_draft_per_supplier(self):
        r = pp.build_pullin_list([1], NEW_SHIP)
        s2 = next(s for s in r["suppliers"] if s["supplier_id"] == "S2")
        self.assertIn("供二 您好", s2["draft_text"])
        self.assertIn("採購單 PO2 項次 2", s2["draft_text"])
        self.assertNotIn("PO1", s2["draft_text"])


class StatusAndConfirmTests(Base):
    def test_confirm_persists_lines_and_badge(self):
        r = pp.confirm([1], NEW_SHIP)
        self.assertEqual(r["saved_lines"], 2)
        self.assertEqual(pp.unresolved_count(), 2)
        rows = db.list_pullin_items()
        self.assertEqual({x["status"] for x in rows}, {"未詢問"})
        self.assertEqual(rows[0]["order_ids"], [1])

    def test_status_persists_across_reruns(self):
        pp.confirm([1], NEW_SHIP)
        key = "S1|PO1|1|EC-1"
        pp.set_status([{"line_key": key}], "已詢問")
        r = pp.build_pullin_list([1], NEW_SHIP)
        l = next(l for s in r["suppliers"] for l in s["lines"] if l["line_key"] == key)
        self.assertEqual(l["status"], "已詢問")
        pp.confirm([1], NEW_SHIP)  # 再次確認不洗掉狀態
        self.assertEqual({x["line_key"]: x["status"] for x in db.list_pullin_items()}[key], "已詢問")
        self.assertEqual(pp.unresolved_count(), 2)
        pp.set_status([{"line_key": key}], "可提前", note="廠商回覆可提前")
        self.assertEqual(pp.unresolved_count(), 1)
        pp.set_status([{"line_key": "S2|PO2|2|EC-2"}], "不能提前")
        self.assertEqual(pp.unresolved_count(), 0)
        r = pp.build_pullin_list([1], NEW_SHIP)
        s1 = next(s for s in r["suppliers"] if s["supplier_id"] == "S1")
        self.assertEqual(s1["unresolved"], 0)
        self.assertEqual(s1["draft_text"], "")  # 都有結論，不再產生詢問草稿
        self.assertEqual(r["summary"]["unresolved"], 0)

    def test_set_status_without_prior_confirm_creates_row(self):
        pp.set_status([{"supplier_id": "S9", "bill_no": "PO9", "row_no": 3, "part_number": "ec-9", "qty": 5,
                        "po_date": "2027-01-01", "requested_date": "2026-11-26", "days_earlier": 36}], "已詢問")
        row = db.list_pullin_items()[0]
        self.assertEqual((row["line_key"], row["status"], row["part_number"]), ("S9|PO9|3|EC-9", "已詢問", "EC-9"))

    def test_invalid_status_rejected(self):
        with self.assertRaises(pp.PullinError):
            pp.set_status([{"line_key": "k"}], "亂填")

    def test_ship_date_not_updated_by_default(self):
        oid = self.add_order()
        r = pp.confirm([oid], NEW_SHIP)
        self.assertEqual(r["updated_orders"], [])
        o = db.get_order(oid)
        self.assertEqual((o["ship_date"], o["delivery_date"]), ("2026-12-28", "2026-12-28"))

    def test_ship_date_updated_only_with_flag(self):
        oid = self.add_order()
        r = pp.confirm([oid], NEW_SHIP, update_order_ship_date=True)
        self.assertEqual(r["updated_orders"][0]["old"], "2026-12-28")
        o = db.get_order(oid)
        self.assertEqual((o["ship_date"], o["delivery_date"]), (NEW_SHIP, NEW_SHIP))
        self.assertTrue(any(l["action"] == "delivery_changed" for l in db.get_activity_logs()))

    def test_dispatched_order_creates_alert_when_updated(self):
        oid = self.add_order(status="dispatched")
        pp.confirm([oid], NEW_SHIP, update_order_ship_date=True)
        n = self.conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        self.assertEqual(n, 1)

    def test_dry_run_with_flag_changes_nothing(self):
        oid = self.add_order()
        r = pp.confirm([oid], NEW_SHIP, update_order_ship_date=True, dry_run=True)
        self.assertTrue(r["dry_run"])
        self.assertEqual(r["would_update_orders"], [oid])
        self.assertEqual(db.get_order(oid)["ship_date"], "2026-12-28")
        self.assertEqual(db.list_pullin_items(), [])

    def test_confirm_with_erp_failure_still_updates_dates(self):
        oid = self.add_order()
        self.open_lines = RuntimeError("down")
        r = pp.confirm([oid], NEW_SHIP, update_order_ship_date=True)
        self.assertFalse(r["erp_ok"])
        self.assertEqual(r["saved_lines"], 0)
        self.assertEqual(db.get_order(oid)["ship_date"], NEW_SHIP)


class FakeCursor:
    def __init__(self, rows):
        self.rows, self.sql, self.params = rows, [], []

    def execute(self, sql, params=()):
        self.sql.append(sql)
        self.params.append(params)

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows):
        self.cur = FakeCursor(rows)

    def cursor(self):
        return self.cur


class ErpQueryTests(unittest.TestCase):
    def test_query_is_read_only_and_parses_rows(self):
        conn = FakeConn([
            ("ec-1", "PO1", 2, 80.0, 20270105, "S1"),
            ("EC-2", "PO2", 1, 0.0, 20270105, "S1"),    # 已收完 → 略過
            ("", "PO3", 1, 5.0, 20270105, "S1"),
        ])
        rows = pp._query_open_po_lines(conn, ["ec-1", "EC-2"], date(2026, 10, 29), 90)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], {"part_number": "EC-1", "bill_no": "PO1", "row_no": 2, "qty": 80.0,
                                   "po_date": date(2027, 1, 5), "supplier_id": "S1"})
        sql = conn.cur.sql[0].lstrip().upper()
        self.assertTrue(sql.startswith("SELECT"))
        for word in ("INSERT", "UPDATE ", "DELETE", "EXEC"):
            self.assertNotIn(word, sql)
        self.assertEqual(conn.cur.params[0][0], 20260731)  # 今天 - 90 天


class RouterTests(Base):
    def setUp(self):
        super().setUp()
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)

    def test_edit_auth_covers_posts_not_gets(self):
        def req(method, path):
            return Request({"type": "http", "method": method, "path": path, "headers": [], "query_string": b""})
        for path in ("confirm", "status"):
            self.assertTrue(request_requires_edit_auth(req("POST", f"/api/po-pullin/{path}")))
        for path in ("preview", "badge", "items"):
            self.assertFalse(request_requires_edit_auth(req("GET", f"/api/po-pullin/{path}")))

    def test_api_flow(self):
        oid = self.add_order()
        r = self.client.get("/api/po-pullin/preview", params={"order_ids": str(oid), "new_ship_date": NEW_SHIP})
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["summary"]["pull_in_lines"], 2)
        self.assertEqual(self.client.get("/api/po-pullin/badge").json(), {"unresolved": 0})

        r = self.client.post("/api/po-pullin/confirm", json={"order_ids": [oid], "new_ship_date": NEW_SHIP})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["saved_lines"], 2)
        self.assertEqual(db.get_order(oid)["ship_date"], "2026-12-28")  # 預設不改
        self.assertEqual(self.client.get("/api/po-pullin/badge").json(), {"unresolved": 2})

        line_ = body["suppliers"][0]["lines"][0]
        r = self.client.post("/api/po-pullin/status", json={"lines": [line_], "status": "可提前"})
        self.assertEqual(r.json()["unresolved"], 1)
        items = self.client.get("/api/po-pullin/items", params={"unresolved_only": True}).json()["items"]
        self.assertEqual(len(items), 1)

        r = self.client.post("/api/po-pullin/confirm", json={
            "order_ids": [oid], "new_ship_date": NEW_SHIP, "update_order_ship_date": True})
        self.assertEqual(db.get_order(oid)["ship_date"], NEW_SHIP)

    def test_api_errors(self):
        self.assertEqual(self.client.get("/api/po-pullin/preview", params={"order_ids": "x"}).status_code, 400)
        self.assertEqual(self.client.get("/api/po-pullin/preview", params={"order_ids": "1", "new_ship_date": "?"}).status_code, 400)
        self.assertEqual(self.client.post("/api/po-pullin/status", json={"lines": [], "status": "亂填"}).status_code, 400)

    def test_api_erp_failure_still_200(self):
        self.open_lines = RuntimeError("down")
        r = self.client.get("/api/po-pullin/preview", params={"order_ids": "1", "new_ship_date": NEW_SHIP})
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.json()["erp_ok"])


if __name__ == "__main__":
    unittest.main()


class OrderIdsKeptTests(Base):
    def test_status_update_without_order_ids_keeps_them(self):
        pp.confirm([1, 2], NEW_SHIP)
        key = "S1|PO1|1|EC-1"
        pp.set_status([{"line_key": key, "supplier_id": "S1", "bill_no": "PO1", "row_no": 1,
                        "part_number": "EC-1", "qty": 100}], "已詢問")
        row = next(r for r in db.list_pullin_items() if r["line_key"] == key)
        self.assertEqual(row["order_ids"], [1, 2])
        self.assertEqual(row["status"], "已詢問")


class CustomerSuppliedTests(Base):
    def setUp(self):
        super().setUp()
        self.groups[0]["items"] += [
            {"part": "CS-1-TAB", "desc": "客供", "need_qty": 50},       # -TAB 結尾（類別不影響）
            {"part": "BOARD-TAB", "desc": "客供板", "need_qty": 8},  # -TAB 結尾
        ]
        self.st.update({"CS-1-TAB": 0, "BOARD-TAB": 0})
        self.tab_map = {"CS-1-TAB": 30.0, "BOARD-TAB": 5.0}

    def test_not_unordered_not_in_drafts_or_badge(self):
        r = pp.confirm([1], NEW_SHIP)
        self.assertNotIn("CS-1-TAB", {p["part_number"] for p in r["unordered"]})
        self.assertNotIn("BOARD-TAB", {p["part_number"] for p in r["unordered"]})
        self.assertEqual({p["part_number"] for p in r["unordered"]}, {"EC-3", "PK-1"})
        self.assertNotIn("CS-1-TAB", {p["part_number"] for p in r["parts"]})
        for s in r["suppliers"]:
            self.assertNotIn("CS-1-TAB", s["draft_text"])
            self.assertNotIn("BOARD-TAB", s["draft_text"])
        self.assertTrue(all(i["part_number"] not in ("CS-1-TAB", "BOARD-TAB") for i in db.list_pullin_items()))
        self.assertEqual(pp.unresolved_count(), 2)

    def test_separate_list_with_stock_and_shortage(self):
        r = pp.build_pullin_list([1], NEW_SHIP)
        cs = {c["part_number"]: c for c in r["customer_supplied"]}
        self.assertEqual(set(cs), {"CS-1-TAB", "BOARD-TAB"})
        self.assertEqual((cs["CS-1-TAB"]["need"], cs["CS-1-TAB"]["tab_stock"], cs["CS-1-TAB"]["shortage"]), (50, 30, 20))
        self.assertEqual(cs["BOARD-TAB"]["shortage"], 3)
        self.assertEqual(cs["CS-1-TAB"]["ware_ids"], ["002"])
        self.assertTrue(cs["CS-1-TAB"]["required_date"])
        self.assertEqual(r["summary"]["customer_supplied_parts"], 2)
        self.assertEqual(r["summary"]["pull_in_lines"], 2)  # 其餘不受影響

    def test_stock_failure_shows_unknown(self):
        self.tab_map = RuntimeError("down")
        r = pp.build_pullin_list([1], NEW_SHIP)
        cs = {c["part_number"]: c for c in r["customer_supplied"]}
        self.assertIsNone(cs["CS-1-TAB"]["tab_stock"])
        self.assertFalse(cs["CS-1-TAB"]["stock_known"])
        self.assertIsNone(cs["CS-1-TAB"]["shortage"])
        self.assertTrue(any("客供料倉庫" in w for w in r["warnings"]))

    def test_class_003_non_tab_is_treated_as_normal_part(self):
        self.groups[0]["items"].append({"part": "OC-10222B", "desc": "類別003一般料", "need_qty": 40})
        self.st["OC-10222B"] = 0
        r = pp.build_pullin_list([1], NEW_SHIP)
        self.assertEqual({c["part_number"] for c in r["customer_supplied"]}, {"CS-1-TAB", "BOARD-TAB"})
        self.assertIn("OC-10222B", {p["part_number"] for p in r["unordered"]})
        self.assertFalse(hasattr(pp, "_fetch_class_ids"))

    def test_open_po_not_queried_for_customer_supplied(self):
        pp.build_pullin_list([1], NEW_SHIP)
        for call in self.open_calls:
            self.assertNotIn("CS-1-TAB", call)
            self.assertNotIn("BOARD-TAB", call)
