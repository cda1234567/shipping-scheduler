from __future__ import annotations

import math
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
from app.routers import shortage_purchase as sp_router
from app.services import erp_purchase as ep
from app.services import low_stock
from app.services import shortage_purchase as sp
from app.services.edit_auth import request_requires_edit_auth

TODAY = datetime(2026, 10, 29, 10, 0)


class DateRuleTests(unittest.TestCase):
    def test_smt_examples(self):
        cases = {
            date(2026, 12, 10): date(2026, 10, 26),
            date(2026, 12, 28): date(2026, 11, 26),
            date(2026, 12, 26): date(2026, 11, 26),
            date(2027, 1, 10): date(2026, 11, 26),
            date(2027, 2, 5): date(2026, 12, 26),
            date(2026, 3, 31): date(2026, 2, 26),
        }
        for ship, expected in cases.items():
            self.assertEqual(sp.smt_pre_in_date(ship), expected, str(ship))

    def test_smt_month_end_clamp_and_leap_year(self):
        self.assertEqual(sp.smt_pre_in_date(date(2024, 3, 30)), date(2024, 2, 26))  # → 2/29 → 26 日
        self.assertEqual(sp._minus_one_month(date(2024, 3, 30)), date(2024, 2, 29))
        self.assertEqual(sp._minus_one_month(date(2026, 3, 31)), date(2026, 2, 28))
        self.assertEqual(sp._minus_one_month(date(2026, 5, 31)), date(2026, 4, 30))

    def test_smt_january_rollover(self):
        self.assertEqual(sp.smt_pre_in_date(date(2027, 1, 31)), date(2026, 12, 26))
        self.assertEqual(sp.smt_pre_in_date(date(2027, 1, 1)), date(2026, 11, 26))
        self.assertEqual(sp.smt_pre_in_date(date(2027, 2, 1)), date(2026, 12, 26))

    def test_backend(self):
        self.assertEqual(sp.backend_pre_in_date(date(2027, 1, 10)), date(2026, 12, 20))
        self.assertEqual(sp.backend_pre_in_date(date(2026, 3, 10)), date(2026, 2, 17))
        self.assertEqual(sp.backend_pre_in_date(date(2024, 3, 10)), date(2024, 2, 18))  # 閏年

    def test_clamp_pre_in_date(self):
        today = date(2026, 10, 29)
        self.assertEqual(sp.clamp_pre_in_date(date(2026, 10, 26), today), (today, True))
        self.assertEqual(sp.clamp_pre_in_date(today, today), (today, False))
        self.assertEqual(sp.clamp_pre_in_date(date(2026, 11, 1), today), (date(2026, 11, 1), False))

    def test_parse_ship_date(self):
        self.assertEqual(sp.parse_ship_date("2026-12-10"), date(2026, 12, 10))
        self.assertEqual(sp.parse_ship_date("2026/12/10"), date(2026, 12, 10))
        self.assertIsNone(sp.parse_ship_date(""))
        self.assertIsNone(sp.parse_ship_date(None))
        self.assertIsNone(sp.parse_ship_date("abc"))


class ClassifyTests(unittest.TestCase):
    def test_prefixes(self):
        for part in ("EC-1001", "ic-22", "UC-9", "PB-3"):
            self.assertEqual(sp.classify_part(part, ""), "smt", part)
        for part in ("OC-1", "PK-2", "SP-3", "AC-40838B", "RC-1", "CM-1", "CF-1", "SS-1", "GH-1"):
            self.assertEqual(sp.classify_part(part, ""), "backend", part)

    def test_description_keywords(self):
        for desc in ("0402 100nF", "晶片 0603", "1206 電阻", "smd led", "QFN-32 MCU", "BGA256", "SOT-23", "TSSOP14",
                     "SOP8", "PCB 板", "貼片電容", "電感 10uH", "SMT 零件", "0201 R", "0805", "QFP100"):
            self.assertEqual(sp.classify_part("XX-1", desc), "smt", desc)
        for desc in ("外殼 ABS", "螺絲 M3", "包裝紙箱", "標籤", "10201 長度", ""):
            self.assertEqual(sp.classify_part("XX-1", desc), "backend", desc)


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

        p = patch.object(db, "get_conn", temp_conn)
        p.start()
        self.addCleanup(p.stop)


class CategoryOverrideTests(Base):
    def test_override_wins_and_persists(self):
        self.assertEqual(db.get_part_purchase_categories(["OC-1", "EC-1"]), {})
        db.set_part_purchase_category("oc-1", "smt", "web")
        db.set_part_purchase_category("EC-1", "backend", "web")
        got = db.get_part_purchase_categories(["OC-1", "ec-1", "ZZ"])
        self.assertEqual(got, {"OC-1": "smt", "EC-1": "backend"})
        db.set_part_purchase_category("OC-1", "backend", "web")  # upsert
        self.assertEqual(db.get_part_purchase_categories(["OC-1"]), {"OC-1": "backend"})

    def test_invalid_values_rejected(self):
        with self.assertRaises(ValueError):
            db.set_part_purchase_category("OC-1", "other")
        with self.assertRaises(ValueError):
            db.set_part_purchase_category(" ", "smt")
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("INSERT INTO part_purchase_categories(part_number, category) VALUES('X','bad')")


class FakeErpCursor:
    def __init__(self, data):
        self.d = data
        self.rows = []

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.d["sql"].append(s)
        self.d["params"].append(params)
        if "FROM comProduct" in s:
            self.rows = [r for r in self.d["products"] if r[0] in params]
        elif "ROW_NUMBER" in s:
            self.rows = [r for r in self.d["last_po"] if r[0] in params]
        elif "FROM comCustomer" in s:
            self.rows = [r for r in self.d["suppliers"] if r[0] in params]
        else:
            raise AssertionError(s)

    def fetchall(self):
        return list(self.rows)


class FakeErpConn:
    def __init__(self, data):
        self.data = data

    def cursor(self):
        return FakeErpCursor(self.data)


def group(order_id, code, ship, items, model="M1"):
    return {"order_id": order_id, "batch_code": code, "po_number": f"PO{order_id}", "model": model,
            "ship_date": ship, "date": ship.replace("-", "/"), "items": items}


def item(part, qty, desc="", shortage=False):
    return {"part": part, "desc": desc, "qty": "缺" if shortage else qty, "need_qty": qty,
            "fill_color": None, "is_shortage": shortage}


class ShortageListTests(Base):
    def setUp(self):
        super().setUp()
        self.erp = {
            "sql": [], "params": [],
            "products": [("EC-1", "電容 0402", "S1"), ("OC-1", "外殼", "GONE"), ("PK-1", "紙箱", ""), ("AC-1", "線", "")],
            "last_po": [("OC-1", "S2", 3.5, "PO-20260901001", 20260901), ("PK-1", "S3", 1.25, "PO-20260801002", 20260801),
                        ("AC-1", "S2", 9.0, "PO-20260701001", 20260701)],
            "suppliers": [("S1", "供一", "供一公司"), ("S2", "供二", ""), ("S3", "供三", "")],
        }
        self.groups = [
            group(1, "6-2", "2026-12-28", [item("ec-1", 100, "0402 電容"), item("OC-1", 50), item("PK-1", 10)]),
            group(2, "6-1", "2026-12-10", [item("EC-1", 40), item("OC-1", 20, shortage=True), item("AC-1", 5)], model="M2"),
            group(3, "6-3", "", [item("PK-1", 6), item("ZERO", 0)]),
        ]
        self.st = {"EC-1": 60, "OC-1": -30, "PK-1": 100, "AC-1": 0}
        self.on_order = {"AC-1": (5.0, "2026-11-01")}

        @contextmanager
        def source_session():
            yield FakeErpConn(self.erp)

        @contextmanager
        def erp_session():
            yield object()

        patches = [
            patch.object(dispatch_router, "build_dispatch_groups", lambda ids, dec: self.groups),
            patch.object(db, "get_st_inventory_stock", lambda: self.st),
            patch.object(ep, "_source_session", source_session),
            patch.object(low_stock, "_erp_session", erp_session),
            patch.object(low_stock, "_query_on_order", lambda conn, parts, today, md: self.on_order),
            patch.object(low_stock, "get_config", lambda: {"on_order_max_overdue_days": 90}),
            patch.object(ep, "get_config", lambda: {"label": "公司2（測試）", "available": True, "is_test": True}),
            patch.object(sp, "local_now", lambda: TODAY),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        for i, code in ((1, "6-2"), (2, "6-1"), (3, "6-3")):
            self.conn.execute("INSERT INTO orders(id, code, model, po_number, status) VALUES(?,?,?,?, 'dispatched')",
                              (i, code, "M", f"PO{i}"))

    def parts(self):
        res = sp.build_shortage_list([1, 2, 3])
        return res, {p["part_number"]: p for p in res["parts"]}

    def test_need_sums_and_earliest_order(self):
        res, p = self.parts()
        self.assertEqual(p["EC-1"]["need"], 140)
        self.assertEqual(p["EC-1"]["earliest_order"]["code"], "6-1")  # 12-10 比 12-28 早
        self.assertEqual(len(p["EC-1"]["orders"]), 2)
        self.assertNotIn("ZERO", p)
        # 空出貨日排最後
        self.assertEqual(p["PK-1"]["earliest_order"]["code"], "6-2")
        self.assertEqual(p["PK-1"]["need"], 16)

    def test_earliest_tie_takes_first_in_batch_order(self):
        self.groups = [group(1, "A", "2026-12-10", [item("X-1", 1)]), group(2, "B", "2026-12-10", [item("X-1", 1)])]
        _, p = self.parts()
        self.assertEqual(p["X-1"]["earliest_order"]["code"], "A")

    def test_shortage_math(self):
        _, p = self.parts()
        self.assertEqual((p["EC-1"]["st"], p["EC-1"]["shortage"], p["EC-1"]["suggested_qty"]), (60, 80, 80))
        # ST 為負當 0；Shortage 項目的 need 取缺料量，不是「缺」
        self.assertEqual((p["OC-1"]["need"], p["OC-1"]["shortage"]), (70, 70))
        self.assertTrue(p["OC-1"]["needs_purchase"])
        self.assertEqual((p["PK-1"]["shortage"], p["PK-1"]["needs_purchase"], p["PK-1"]["suggested_qty"]), (0, False, 0))
        # 在途只供參考，不扣除：AC-1 需求 5、ST 0、在途 5 → 仍缺 5
        self.assertEqual((p["AC-1"]["on_order"], p["AC-1"]["shortage"], p["AC-1"]["needs_purchase"]), (5, 5, True))
        self.on_order = {"AC-1": (2.0, "")}
        self.groups[1]["items"][2]["need_qty"] = 5.4
        _, p = self.parts()
        self.assertAlmostEqual(p["AC-1"]["shortage"], 5.4)
        self.assertEqual((p["AC-1"]["on_order"], p["AC-1"]["suggested_qty"]), (2, 6))

    def test_on_order_not_deducted(self):
        self.groups = [group(1, "A", "2026-12-10", [item("AC-1", 100)])]
        self.st = {"AC-1": 30}
        self.on_order = {"AC-1": (50.0, "2026-11-01")}
        _, p = self.parts()
        self.assertEqual((p["AC-1"]["shortage"], p["AC-1"]["on_order"], p["AC-1"]["on_order_date"]), (70, 50, "2026-11-01"))

    def test_shortage_matches_summarize_requested_supply(self):
        from app.services.shortage_rules import summarize_requested_supply
        for need, st, expect_buy in ((100, 30, True), (100, -20, True), (100, 100, False), (100, 250, False)):
            self.groups = [group(1, "A", "2026-12-10", [item("AC-1", need)])]
            self.st = {"AC-1": st}
            self.on_order = {"AC-1": (999.0, "")}
            _, p = self.parts()
            want = summarize_requested_supply(need, st)["purchase_needed_qty"]
            self.assertEqual(p["AC-1"]["shortage"], want)
            self.assertEqual(p["AC-1"]["needs_purchase"], expect_buy)
        self.assertEqual((p["AC-1"]["shortage"], p["AC-1"]["suggested_qty"]), (0, 0))

    def test_supplier_priority(self):
        _, p = self.parts()
        self.assertEqual((p["EC-1"]["supplier_id"], p["EC-1"]["supplier_source"], p["EC-1"]["supplier_name"]), ("S1", "master", "供一"))
        # 主檔供應商 GONE 不在 comCustomer → 退回最近採購
        self.assertEqual((p["OC-1"]["supplier_id"], p["OC-1"]["supplier_source"]), ("S2", "last_po"))
        self.assertEqual((p["PK-1"]["supplier_id"], p["PK-1"]["supplier_source"]), ("S3", "last_po"))
        self.erp["last_po"] = []
        _, p = self.parts()
        self.assertEqual((p["PK-1"]["supplier_id"], p["PK-1"]["supplier_source"]), ("", "none"))
        self.assertEqual(p["PK-1"]["price"], 0)

    def test_resolve_supplier_pure(self):
        self.assertEqual(sp.resolve_supplier("A", "B", {"A": "x"}), ("A", "master"))
        self.assertEqual(sp.resolve_supplier("A", "B", {}), ("B", "last_po"))
        self.assertEqual(sp.resolve_supplier(" ", " ", {}), ("", "none"))

    def test_price_and_last_po(self):
        _, p = self.parts()
        self.assertEqual((p["OC-1"]["price"], p["OC-1"]["last_bill_no"], p["OC-1"]["last_bill_date"]),
                         (3.5, "PO-20260901001", "2026-09-01"))

    def test_dates_category_and_clamp(self):
        res, p = self.parts()
        # EC-1：料號前綴 → 打件；最早出貨 2026-12-10 → 推算 2026-10-26 早於今天 10-29 → 改今天並保留原推算日
        self.assertEqual((p["EC-1"]["category"], p["EC-1"]["category_source"]), ("smt", "auto"))
        self.assertEqual((p["EC-1"]["pre_in_date"], p["EC-1"]["original_pre_in_date"], p["EC-1"]["date_adjusted"]),
                         ("2026-10-29", "2026-10-26", True))
        self.assertNotIn("is_late", p["EC-1"])
        # OC-1：後段，最早 2026-12-10 − 21 天 = 2026-11-19
        self.assertEqual((p["OC-1"]["category"], p["OC-1"]["pre_in_date"], p["OC-1"]["date_adjusted"]), ("backend", "2026-11-19", False))
        self.assertEqual(p["OC-1"]["pre_in_dates"], {"smt": "2026-10-29", "backend": "2026-11-19"})
        self.assertEqual(p["OC-1"]["original_pre_in_dates"], {"smt": "2026-10-26", "backend": "2026-11-19"})
        self.assertEqual(p["OC-1"]["dates_adjusted"], {"smt": True, "backend": False})
        # 等於今天不調整：後段出貨日 = 今天 + 21 天
        self.groups = [group(3, "6-3", "2026-11-19", [item("OC-1", 6)])]
        _, p = self.parts()
        self.assertEqual((p["OC-1"]["pre_in_date"], p["OC-1"]["original_pre_in_date"], p["OC-1"]["date_adjusted"]),
                         ("2026-10-29", "2026-10-29", False))
        # 沒有出貨日 → 日期空白、不標遲了
        self.groups = [group(3, "6-3", "", [item("PK-1", 6)])]
        _, p = self.parts()
        self.assertEqual((p["PK-1"]["pre_in_date"], p["PK-1"]["original_pre_in_date"], p["PK-1"]["date_adjusted"]), ("", "", False))

    def test_manual_override_beats_heuristic(self):
        db.set_part_purchase_category("OC-1", "smt", "web")
        _, p = self.parts()
        self.assertEqual((p["OC-1"]["category"], p["OC-1"]["category_source"]), ("smt", "manual"))
        self.assertEqual((p["OC-1"]["pre_in_date"], p["OC-1"]["original_pre_in_date"]), ("2026-10-29", "2026-10-26"))

    def test_sorted_by_supplier_then_part_and_metadata(self):
        res, _ = self.parts()
        keys = [(x["supplier_id"], x["part_number"]) for x in res["parts"]]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(res["today"], "2026-10-29")
        self.assertEqual(res["config"]["label"], "公司2（測試）")
        self.assertEqual([o["code"] for o in res["orders"]], ["6-2", "6-1", "6-3"])
        self.assertEqual(res["warnings"], [])

    def test_lookups_use_nolock_and_chunking(self):
        self.groups = [group(1, "A", "2026-12-10", [item(f"P-{i:04d}", 1) for i in range(450)])]
        self.parts()
        product_calls = [(s, p) for s, p in zip(self.erp["sql"], self.erp["params"]) if "FROM comProduct" in s]
        self.assertEqual([len(p) for _, p in product_calls], [200, 200, 50])
        self.assertTrue(all("WITH (NOLOCK)" in s for s in self.erp["sql"]))

    def test_erp_unavailable_degrades_with_warnings(self):
        @contextmanager
        def broken():
            raise ep.ErpUnavailable("連不上")
            yield

        with patch.object(ep, "_source_session", broken), patch.object(low_stock, "_erp_session", broken):
            res, p = self.parts()
        self.assertEqual(len(res["warnings"]), 2)
        self.assertEqual((p["EC-1"]["supplier_id"], p["EC-1"]["on_order"], p["EC-1"]["shortage"]), ("", 0, 80))

    def test_no_groups_warns(self):
        self.groups = []
        res = sp.build_shortage_list([1])
        self.assertEqual(res["parts"], [])
        self.assertTrue(res["warnings"])


def aorder(oid, code, ship, qty):
    return {"order_id": oid, "code": code, "model": f"M{oid}", "po_number": f"PO{oid}", "ship_date": ship, "qty": qty}


class AllocateTests(unittest.TestCase):
    def pairs(self, out):
        return [(a["order_id"], a["qty"], a["kind"]) for a in out]

    def test_st_consumed_earliest_first_then_purchase(self):
        orders = [aorder(1, "B", "2026-12-28", 100), aorder(2, "A", "2026-12-10", 40)]
        out = sp.allocate_purchase_to_orders(orders, 60, 80)  # ST 先抵 A 40、B 20；剩 B 80
        self.assertEqual(self.pairs(out), [(1, 80.0, "order")])
        self.assertEqual((out[0]["order_code"], out[0]["customer_po"], out[0]["ship_date"]), ("B", "PO1", "2026-12-28"))

    def test_partial_fills_earliest_orders_first(self):
        orders = [aorder(1, "B", "2026-12-28", 100), aorder(2, "A", "2026-12-10", 40), aorder(3, "C", "2027-01-05", 10)]
        self.assertEqual(self.pairs(sp.allocate_purchase_to_orders(orders, 0, 50)), [(2, 40.0, "order"), (1, 10.0, "order")])

    def test_extra_when_overbuying(self):
        orders = [aorder(1, "A", "2026-12-10", 40)]
        out = sp.allocate_purchase_to_orders(orders, 10, 50)
        self.assertEqual(self.pairs(out), [(1, 30.0, "order"), (0, 20.0, "extra")])

    def test_empty_dates_last_and_stable(self):
        orders = [aorder(1, "N1", "", 10), aorder(2, "D", "2026-12-10", 10), aorder(3, "N2", "", 10)]
        self.assertEqual([a["order_id"] for a in sp.allocate_purchase_to_orders(orders, 0, 30)], [2, 1, 3])
        self.assertEqual(self.pairs(sp.allocate_purchase_to_orders(orders, 0, 15)), [(2, 10.0, "order"), (1, 5.0, "order")])

    def test_negative_st_counts_as_zero(self):
        orders = [aorder(1, "A", "2026-12-10", 70)]
        self.assertEqual(self.pairs(sp.allocate_purchase_to_orders(orders, -30, 70)), [(1, 70.0, "order")])

    def test_st_covers_everything_and_zero_purchase(self):
        orders = [aorder(1, "A", "2026-12-10", 40)]
        self.assertEqual(sp.allocate_purchase_to_orders(orders, 100, 0), [])
        self.assertEqual(self.pairs(sp.allocate_purchase_to_orders(orders, 100, 5)), [(0, 5.0, "extra")])

    def test_same_order_rows_merge(self):
        orders = [aorder(1, "A", "2026-12-10", 10), aorder(1, "A", "2026-12-10", 15)]
        self.assertEqual(self.pairs(sp.allocate_purchase_to_orders(orders, 0, 25)), [(1, 25.0, "order")])


class AllocationFlowTests(ShortageListTests):
    def test_list_has_default_allocations_and_nothing_is_subtracted(self):
        self.groups = [group(1, "A", "2026-12-10", [item("AC-1", 100)]), group(2, "B", "2026-12-28", [item("AC-1", 50)])]
        self.st = {"AC-1": 30}
        self.on_order = {"AC-1": (500.0, "2026-11-01")}
        # 把這張 PO 記為替訂單 1 買的，也不會改變缺料
        db.save_po_line_allocations(1, "B1", "CHIComp01", [
            {"row_no": 1, "part_number": "AC-1", "order_id": 1, "qty": 500, "kind": "order", "source": "shortage_po"}])
        _, p = self.parts()
        self.assertEqual((p["AC-1"]["shortage"], p["AC-1"]["suggested_qty"]), (120, 120))
        self.assertEqual([(a["order_id"], a["qty"], a["kind"]) for a in p["AC-1"]["allocations"]],
                         [(1, 70.0, "order"), (2, 50.0, "order")])

    def test_prepare_groups_recomputes_from_final_qty(self):
        self.groups = [group(1, "A", "2026-12-10", [item("AC-1", 100)]), group(2, "B", "2026-12-28", [item("AC-1", 50)])]
        self.st = {"AC-1": 30}
        raw = [{"supplier_id": "S2", "remark": "", "lines": [
            {"part_number": "ac-1", "qty": 130, "price": 1, "pre_in_date": "2026-11-01",
             "allocations": [{"order_id": 99, "qty": 1}]},
            {"part_number": "ZZ-9", "qty": 7, "price": 1, "pre_in_date": "2026-11-01"}]}]
        out = sp.prepare_groups(raw, [1, 2])
        self.assertEqual(out[0]["alloc_source"], "shortage_po")
        la, lz = out[0]["lines"]
        self.assertEqual([(a["order_id"], a["qty"], a["kind"]) for a in la["allocations"]],
                         [(1, 70.0, "order"), (2, 50.0, "order"), (0, 10.0, "extra")])  # 客戶端送的 99 被忽略
        self.assertEqual([(a["kind"], a["qty"]) for a in lz["allocations"]], [("manual", 7.0)])
        # 沒有 order_ids → 全部 manual
        out = sp.prepare_groups(raw, None)
        self.assertEqual([a["kind"] for a in out[0]["lines"][0]["allocations"]], ["manual"])

    def test_prepare_groups_context_failure_degrades_to_manual(self):
        raw = [{"supplier_id": "S2", "lines": [{"part_number": "AC-1", "qty": 3, "price": 1, "pre_in_date": "2026-11-01"}]}]
        with patch.object(dispatch_router, "build_dispatch_groups", side_effect=RuntimeError("x")):
            out = sp.prepare_groups(raw, [1])
        self.assertEqual(out[0]["lines"][0]["allocations"][0]["kind"], "manual")

    def test_prepare_groups_clamps_past_dates_to_today(self):
        def line(d):
            return {"part_number": "AC-1", "qty": 3, "price": 1, "pre_in_date": d}
        raw = [{"supplier_id": "S2", "lines": [line("2026-10-26"), line("2026-10-29"), line("2026-11-19"), line("bad")]}]
        dates = [l["pre_in_date"] for l in sp.prepare_groups(raw, None)[0]["lines"]]
        self.assertEqual(dates, ["2026-10-29", "2026-10-29", "2026-11-19", "bad"])

    def test_preview_and_create_payload_use_clamped_date(self):
        from main import app
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        client = TestClient(app)
        grp = {"supplier_id": "S2", "remark": "", "lines": [{"part_number": "EC-1", "qty": 80, "price": 1, "pre_in_date": "2026-10-26"}]}
        seen = {}
        with patch.object(ep, "preview_many", lambda reqs: seen.setdefault("p", reqs) and [{"ok": True}]):
            client.post("/api/shortage-po/preview", json={"groups": [grp], "order_ids": [1, 2, 3]})
        self.assertEqual(seen["p"][0]["lines"][0]["pre_in_date"], "2026-10-29")
        with patch.object(ep, "create_po", lambda req, actor="web", client_ip="": seen.setdefault("c", req) and
                          {"ok": True, "bill_no": "X", "supplier": {}, "totals": {}, "lines": []}):
            client.post("/api/shortage-po/create", json={"groups": [grp], "confirm": True})
        self.assertEqual(seen["c"]["lines"][0]["pre_in_date"], "2026-10-29")

    def test_router_passes_order_ids_to_preview_and_create(self):
        from main import app
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        client = TestClient(app)
        grp = {"supplier_id": "S2", "remark": "", "lines": [{"part_number": "EC-1", "qty": 80, "price": 1, "pre_in_date": "2026-11-01"}]}
        seen = {}
        with patch.object(ep, "preview_many", lambda reqs: seen.setdefault("p", reqs) and [{"ok": True}]):
            client.post("/api/shortage-po/preview", json={"groups": [grp], "order_ids": [1, 2, 3]})
        line = seen["p"][0]["lines"][0]
        self.assertEqual([(a["order_id"], a["qty"]) for a in line["allocations"]], [(1, 80.0)])  # ST 60 先抵訂單 2 的 40 與訂單 1 的 20

        with patch.object(ep, "create_po", lambda req, actor="web", client_ip="": seen.setdefault("c", req) and
                          {"ok": True, "bill_no": "X", "supplier": {}, "totals": {}, "lines": []}):
            client.post("/api/shortage-po/create", json={"groups": [grp], "order_ids": [1, 2, 3], "confirm": True})
        self.assertEqual(seen["c"]["alloc_source"], "shortage_po")


class SequentialAllocationTests(AllocationFlowTests):
    def test_same_part_split_across_lines_continues_and_consumes_st_once(self):
        self.groups = [group(1, "A", "2026-12-10", [item("AC-1", 100)]), group(2, "B", "2026-12-28", [item("AC-1", 50)])]
        self.st = {"AC-1": 30}  # 淨需求 120：A 70、B 50
        raw = [
            {"supplier_id": "S2", "lines": [{"part_number": "AC-1", "qty": 40, "price": 1, "pre_in_date": "2026-11-01"}]},
            {"supplier_id": "S3", "lines": [{"part_number": "ac-1", "qty": 90, "price": 1, "pre_in_date": "2026-11-01"}]},
        ]
        out = sp.prepare_groups(raw, [1, 2])
        pairs = lambda g: [(a["order_id"], a["qty"], a["kind"]) for a in out[g]["lines"][0]["allocations"]]
        self.assertEqual(pairs(0), [(1, 40.0, "order")])
        self.assertEqual(pairs(1), [(1, 30.0, "order"), (2, 50.0, "order"), (0, 10.0, "extra")])
        # 兩行合計 = 淨需求 120 + 超買 10，ST 沒有被重複扣
        total = sum(a["qty"] for g in out for a in g["lines"][0]["allocations"])
        self.assertEqual(total, 130.0)


class SupplierResolutionTests(ShortageListTests):
    def test_resolve_supplier_rules(self):
        sups = {"S1": "供一", "S2": "供二"}
        r = sp.resolve_supplier
        self.assertEqual(r("S1", "S2", sups), ("S1", "master"))  # 主檔有效 > 最近採購
        self.assertEqual(r("GONE", "S2", sups), ("S2", "last_po"))  # 主檔不是現有供應商 → 最近採購
        self.assertEqual(r("", "S2", sups), ("S2", "last_po"))
        self.assertEqual(r("", "", sups), ("", "none"))
        self.assertEqual(r(None, None, sups), ("", "none"))
        self.assertEqual(r("  S1 ", "S2", sups), ("S1", "master"))  # 去空白
        self.assertEqual(r("", "  S2 ", sups), ("S2", "last_po"))
        self.assertEqual(r("   ", "   ", sups), ("", "none"))

    def test_build_shortage_list_supplier_per_part(self):
        self.groups = [group(1, "A", "2026-12-10", [item("EC-1", 1), item("OC-1", 1), item("PK-1", 1), item("NEW-1", 1)])]
        self.erp["products"].append(("NEW-1", "新料", ""))
        _, p = self.parts()
        got = {k: (v["supplier_id"], v["supplier_source"]) for k, v in p.items()}
        self.assertEqual(got["EC-1"], ("S1", "master"))      # 主檔 S1 有效
        self.assertEqual(got["OC-1"], ("S2", "last_po"))     # 主檔 GONE 不存在 → 最近採購
        self.assertEqual(got["PK-1"], ("S3", "last_po"))     # 主檔空 → 最近採購
        self.assertEqual(got["NEW-1"], ("", "none"))         # 都沒有
        self.assertEqual(p["EC-1"]["supplier_name"], "供一")

    def test_master_wins_even_if_last_po_differs(self):
        self.groups = [group(1, "A", "2026-12-10", [item("EC-1", 1)])]
        self.erp["last_po"].append(("EC-1", "S3", 2.0, "PO-20260901009", 20260901))
        _, p = self.parts()
        self.assertEqual((p["EC-1"]["supplier_id"], p["EC-1"]["supplier_source"]), ("S1", "master"))

    def test_lookup_last_po_orders_by_date_then_billno_desc(self):
        data = {"sql": [], "params": [], "last_po": [("OC-1", "S2", 3.5, "PO-1", 20260901)]}
        res = sp._lookup_last_po(FakeErpConn(data), ["OC-1"])
        sql = data["sql"][0]
        self.assertIn("ORDER BY m.BillDate DESC, m.BillNO DESC", sql)
        self.assertIn("PARTITION BY s.ProdID", sql)
        self.assertIn("rn = 1", sql)
        self.assertIn("s.Flag = 4", sql)
        self.assertEqual(res["OC-1"]["supplier_id"], "S2")
        self.assertEqual(res["OC-1"]["bill_date"], "2026-09-01")


class QtySourceTests(ShortageListTests):
    def test_suggested_qty_is_need_minus_st_and_ignores_on_order(self):
        self.groups = [
            group(1, "A", "2026-12-10", [item("AC-1", 33.5)]),
            group(2, "B", "2026-12-28", [item("AC-1", 50)]),
            group(3, "C", "2027-01-05", [item("AC-1", 20)]),
        ]
        self.st = {"AC-1": 10}
        self.on_order = {}
        _, p = self.parts()
        expected = math.ceil(33.5 + 50 + 20 - 10)  # 94
        self.assertEqual(p["AC-1"]["suggested_qty"], expected)
        self.assertEqual(p["AC-1"]["on_order"], 0)
        self.on_order = {"AC-1": (1000.0, "2026-11-01")}
        _, p = self.parts()
        self.assertEqual(p["AC-1"]["suggested_qty"], expected)  # 在途不影響
        self.assertEqual(p["AC-1"]["on_order"], 1000.0)

    def test_st_covers_need_means_no_purchase(self):
        self.groups = [group(1, "A", "2026-12-10", [item("AC-1", 20)])]
        self.st = {"AC-1": 20}
        _, p = self.parts()
        self.assertEqual((p["AC-1"]["suggested_qty"], p["AC-1"]["needs_purchase"]), (0, False))


class GroupLinesTests(unittest.TestCase):
    def test_one_request_per_supplier(self):
        lines = [
            {"part_number": "A", "qty": 1, "price": 2, "pre_in_date": "2026-11-01", "supplier_id": "S2"},
            {"part_number": "B", "qty": 3, "price": 4, "pre_in_date": "2026-11-02", "supplier_id": "S1"},
            {"part_number": "C", "qty": 5, "price": 6, "pre_in_date": "2026-11-03", "supplier_id": " S2 "},
        ]
        out = sp.group_lines(lines, remarks={"S2": "缺料採購：6-1"}, salesman="017")
        self.assertEqual([g["supplier_id"] for g in out], ["S2", "S1"])
        self.assertEqual([l["part_number"] for l in out[0]["lines"]], ["A", "C"])
        self.assertEqual((out[0]["remark"], out[1]["remark"], out[0]["salesman"]), ("缺料採購：6-1", "", "017"))
        self.assertNotIn("supplier_id", out[0]["lines"][0])

    def test_missing_supplier_refused(self):
        with self.assertRaises(ValueError):
            sp.group_lines([{"part_number": "A", "qty": 1, "price": 1, "pre_in_date": "2026-11-01", "supplier_id": ""}])


class CreateManyTests(Base):
    def test_continues_after_failure(self):
        calls = []

        def fake_create(req, actor="web", client_ip=""):
            calls.append((req["supplier_id"], req["confirm"], actor, client_ip))
            if req["supplier_id"] == "BAD":
                raise ep.PoValidationError("正航找不到供應商 BAD")
            if req["supplier_id"] == "BOOM":
                raise RuntimeError("secret-pw exploded")
            return {"ok": True, "bill_no": f"PO-{req['supplier_id']}", "supplier": {"id": req["supplier_id"]},
                    "totals": {"total": 105.0}, "lines": [{}]}

        groups = [{"supplier_id": s, "remark": "", "lines": []} for s in ("S1", "BAD", "BOOM", "S2")]
        with patch.object(ep, "create_po", fake_create):
            res = sp.create_many(groups, actor="web", client_ip="1.2.3.4")
        self.assertEqual([c[0] for c in calls], ["S1", "BAD", "BOOM", "S2"])
        self.assertTrue(all(c[1] is True and c[3] == "1.2.3.4" for c in calls))
        self.assertEqual([r["ok"] for r in res], [True, False, False, True])
        self.assertEqual(res[0]["bill_no"], "PO-S1")
        self.assertIn("找不到供應商", res[1]["error"])
        self.assertIn("RuntimeError", res[2]["error"])
        logs = db.get_activity_logs()
        self.assertTrue(any(l["action"] == "shortage_po_created" and "成功 2" in l["detail"] and "失敗 2" in l["detail"]
                            for l in logs))

    def test_max_groups(self):
        with self.assertRaises(ep.PoValidationError):
            sp.create_many([{"supplier_id": f"S{i}", "lines": []} for i in range(21)])


class RouterTests(Base):
    def setUp(self):
        super().setUp()
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)

    GROUP = {"supplier_id": "S1", "remark": "r",
             "lines": [{"part_number": "A", "qty": 1, "price": 1, "pre_in_date": "2026-11-01"}]}

    def test_edit_auth_covers_posts_not_gets(self):
        def req(method, path):
            return Request({"type": "http", "method": method, "path": path, "headers": [], "query_string": b""})
        for path in ("preview", "create", "category"):
            self.assertTrue(request_requires_edit_auth(req("POST", f"/api/shortage-po/{path}")))
        self.assertFalse(request_requires_edit_auth(req("GET", "/api/shortage-po/list")))

    def test_post_without_edit_login_is_refused(self):
        saved = os.environ.pop("PYTEST_CURRENT_TEST")
        try:
            r = self.client.post("/api/shortage-po/create", json={"groups": [self.GROUP], "confirm": True})
        finally:
            os.environ["PYTEST_CURRENT_TEST"] = saved
        self.assertIn(r.status_code, (401, 403))
        self.assertEqual(r.json().get("code"), "edit_auth_required")

    def test_create_requires_confirm(self):
        with patch.object(sp, "create_many") as m:
            r = self.client.post("/api/shortage-po/create", json={"groups": [self.GROUP]})
            self.assertEqual(r.status_code, 400)
            r = self.client.post("/api/shortage-po/create", json={"groups": [self.GROUP], "confirm": False})
            self.assertEqual(r.status_code, 400)
            m.assert_not_called()

    def test_create_and_preview_delegate(self):
        with patch.object(sp, "create_many", lambda g, a, ip: [{"supplier_id": g[0]["supplier_id"], "ok": True, "bill_no": "X"}]):
            r = self.client.post("/api/shortage-po/create", json={"groups": [self.GROUP], "confirm": True})
        self.assertEqual((r.status_code, r.json()["results"][0]["bill_no"]), (200, "X"))
        with patch.object(ep, "preview_many", lambda reqs: [{"ok": True, "n": len(reqs)}]):
            r = self.client.post("/api/shortage-po/preview", json={"groups": [self.GROUP]})
        self.assertEqual(r.json()["results"], [{"ok": True, "n": 1}])

    def test_category_endpoint(self):
        r = self.client.post("/api/shortage-po/category", json={"part_number": "oc-1", "category": "smt"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(db.get_part_purchase_categories(["OC-1"]), {"OC-1": "smt"})
        r = self.client.post("/api/shortage-po/category", json={"part_number": "oc-1", "category": "zzz"})
        self.assertEqual(r.status_code, 400)

    def test_list_validation(self):
        self.assertEqual(self.client.get("/api/shortage-po/list").status_code, 400)
        self.assertEqual(self.client.get("/api/shortage-po/list", params={"order_ids": "a,b"}).status_code, 400)
        with patch.object(sp, "build_shortage_list", lambda ids: {"ids": ids}):
            r = self.client.get("/api/shortage-po/list", params={"order_ids": "1, 2,3"})
        self.assertEqual(r.json(), {"ids": [1, 2, 3]})

    def test_orders_and_latest_batch(self):
        rows = [
            (1, "1-1", "dispatched", "2026-10-29T10:00:00", "2026-12-01"),
            (2, "1-2", "dispatched", "2026-10-29T10:05:00", "2026-12-02"),
            (3, "1-3", "completed", "2026-10-28T09:00:00", "2026-12-03"),
            (4, "1-4", "cancelled", "2026-10-29T10:01:00", "2026-12-04"),
            (5, "1-5", "dispatched", "2026-10-29T10:02:00", "2026-12-05"),  # 已退回 → 無有效發料時間
        ]
        for oid, code, status, ts, ship in rows:
            self.conn.execute("INSERT INTO orders(id, code, model, po_number, status, delivery_date) VALUES(?,?,?,?,?,?)",
                              (oid, code, "M", f"P{oid}", status, ship))
            self.conn.execute("INSERT INTO dispatch_sessions(order_id, dispatched_at, rolled_back_at) VALUES(?,?,?)",
                              (oid, ts, "2026-10-29T11:00:00" if oid == 5 else ""))
        data = self.client.get("/api/shortage-po/orders").json()
        self.assertEqual([o["id"] for o in data["orders"]], [2, 1, 3, 5])  # 無 cancelled；最新在前
        self.assertEqual(sorted(data["latest_batch_ids"]), [1, 2])
        self.assertEqual(data["orders"][0]["ship_date"], "2026-12-02")
        self.assertEqual(data["orders"][-1]["dispatched_at"], "")

    def test_latest_batch_window(self):
        orders = [{"id": 1, "dispatched_at": "2026-10-29T10:10:00"}, {"id": 2, "dispatched_at": "2026-10-29T10:00:00"},
                  {"id": 3, "dispatched_at": "2026-10-29T09:59:59"}, {"id": 4, "dispatched_at": ""}]
        self.assertEqual(sp_router._latest_batch_ids(orders), [1, 2])
        self.assertEqual(sp_router._latest_batch_ids([]), [])


if __name__ == "__main__":
    unittest.main()


class TabFakeCursor(FakeErpCursor):
    """在 FakeErpCursor 之上支援 comWareAmount 查詢（依參數的倉庫 / 料號過濾並加總）。"""

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        if "FROM comWareAmount" in s:
            self.d["sql"].append(s)
            self.d["params"].append(params)
            marks = s.split("WareID IN (")[1].split(")")[0].count("%s")
            wares, parts = set(params[:marks]), set(params[marks:])
            totals = {}
            for ware, prod, qty in self.d["wares"]:
                if ware in wares and prod in parts:
                    totals[prod] = totals.get(prod, 0) + qty
            self.rows = list(totals.items())
            return
        super().execute(sql, params)


class CustomerSuppliedListTests(Base):
    def setUp(self):
        super().setUp()
        self.erp = {
            "sql": [], "params": [],
            "products": [("EC-1", "電容", "S1", "001"), ("CS-1-TAB", "客供電阻", "S9", "003"), ("OC-10222B", "類別003一般料", "S1", "003"),
                         ("CS-2-TAB", "客供板", "S9", "001"), ("NOSUP", "無供應商", "", "001")],
            "last_po": [("EC-1", "S1", 2.0, "PO-1", 20260901), ("CS-1-TAB", "S9", 1.0, "PO-2", 20260901),
                        ("CS-2-TAB", "S9", 1.0, "PO-3", 20260901)],
            "suppliers": [("S1", "供一", ""), ("S9", "供九", "")],
            "wares": [("002", "CS-1-TAB", 30.0), ("002", "CS-2-TAB", 5.0), ("015", "CS-1-TAB", 1000.0),
                      ("001", "CS-1-TAB", 777.0), ("003", "CS-1-TAB", 4.0)],
        }
        self.groups = [group(1, "1-1", "2026-12-10", [
            item("EC-1", 100), item("cs-1-tab", 50), item("CS-2-TAB", 8), item("NOSUP", 3)])]
        self.st = {"EC-1": 0, "CS-1-TAB": 9999, "CS-2-TAB": 9999, "NOSUP": 0}
        self.tab_down = False
        conn = type("C", (), {"cursor": lambda s_: TabFakeCursor(self.erp)})()

        @contextmanager
        def source_session():
            yield conn

        @contextmanager
        def erp_session():
            if self.tab_down:
                raise RuntimeError("down")
            yield conn

        self.on_order_parts = []

        def on_order(c, parts, today, md):
            self.on_order_parts.extend(parts)
            return {}

        patches = [
            patch.object(dispatch_router, "build_dispatch_groups", lambda ids, dec: self.groups),
            patch.object(db, "get_st_inventory_stock", lambda: self.st),
            patch.object(ep, "_source_session", source_session),
            patch.object(low_stock, "_erp_session", erp_session),
            patch.object(low_stock, "_query_on_order", on_order),
            patch.object(low_stock, "get_config", lambda: {"on_order_max_overdue_days": 90}),
            patch.object(ep, "get_config", lambda: {"label": "x", "available": True, "is_test": True}),
            patch.object(sp, "local_now", lambda: TODAY),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def build(self):
        res = sp.build_shortage_list([1])
        return res, {p["part_number"]: p for p in res["parts"]}, {c["part_number"]: c for c in res["customer_supplied"]}

    def test_classification(self):
        self.assertEqual(ep.customer_supplied_reason("X-TAB"), "suffix")
        self.assertEqual(ep.customer_supplied_reason("x-tab"), "suffix")
        self.assertEqual(ep.customer_supplied_reason("X", "003"), "")  # 類別 003 不再是客供料
        self.assertEqual(ep.customer_supplied_reason("OC-10222B", "003"), "")
        self.assertEqual(ep.customer_supplied_reason("X", "001"), "")

    def test_excluded_from_purchasable_and_listed_separately(self):
        res, parts, cs = self.build()
        self.assertEqual(set(parts), {"EC-1", "NOSUP"})
        self.assertEqual(set(cs), {"CS-1-TAB", "CS-2-TAB"})
        self.assertEqual(cs["CS-1-TAB"]["reason"], "suffix")
        self.assertEqual(cs["CS-2-TAB"]["reason"], "suffix")
        self.assertEqual(cs["CS-1-TAB"]["description"], "客供電阻")
        self.assertEqual(cs["CS-1-TAB"]["earliest_order"]["code"], "1-1")
        self.assertEqual(cs["CS-1-TAB"]["orders"][0]["qty"], 50)

    def test_no_supplier_or_price_lookup_for_customer_supplied(self):
        self.build()
        looked_up = " ".join(str(p) for p in self.erp["params"])
        last_po_params = [p for s, p in zip(self.erp["sql"], self.erp["params"]) if "ROW_NUMBER" in s]
        self.assertTrue(last_po_params)
        for params in last_po_params:
            self.assertNotIn("CS-1-TAB", params)
            self.assertNotIn("CS-2-TAB", params)
        supplier_params = [p for s, p in zip(self.erp["sql"], self.erp["params"]) if "FROM comCustomer" in s]
        for params in supplier_params:
            self.assertNotIn("S9", params)
        self.assertNotIn("CS-1-TAB", self.on_order_parts)
        self.assertNotIn("CS-2-TAB", self.on_order_parts)
        self.assertIn("EC-1", self.on_order_parts)
        self.assertTrue(looked_up)

    def test_stock_is_002_only_not_015_not_st(self):
        _, _, cs = self.build()
        self.assertEqual(cs["CS-1-TAB"]["tab_stock"], 30)  # 015=1000、001=777、003=4、ST=9999 都不算
        self.assertEqual(cs["CS-1-TAB"]["need"], 50)
        self.assertEqual(cs["CS-1-TAB"]["shortage"], 20)
        self.assertEqual(cs["CS-1-TAB"]["ware_ids"], ["002"])
        self.assertEqual(cs["CS-2-TAB"]["tab_stock"], 5)
        self.assertEqual(cs["CS-2-TAB"]["shortage"], 3)
        wares_sql = [(s, p) for s, p in zip(self.erp["sql"], self.erp["params"]) if "comWareAmount" in s]
        self.assertEqual(len(wares_sql), 1)
        self.assertTrue(wares_sql[0][0].upper().lstrip().startswith("SELECT"))
        self.assertEqual(wares_sql[0][1][0], "002")

    def test_shortage_never_negative(self):
        self.groups = [group(1, "1-1", "2026-12-10", [item("CS-1-TAB", 10)])]
        _, _, cs = self.build()
        self.assertEqual(cs["CS-1-TAB"]["shortage"], 0)

    def test_env_override_multiple_wares(self):
        with patch.dict(os.environ, {"ERP_TAB_WARE_IDS": "002, 003 ,"}):
            _, _, cs = self.build()
        self.assertEqual(cs["CS-1-TAB"]["ware_ids"], ["002", "003"])
        self.assertEqual(cs["CS-1-TAB"]["tab_stock"], 34)  # 30 + 4，015 仍不算
        self.assertEqual(cs["CS-1-TAB"]["shortage"], 16)

    def test_env_blank_falls_back_to_default(self):
        with patch.dict(os.environ, {"ERP_TAB_WARE_IDS": " , "}):
            self.assertEqual(sp.tab_ware_ids(), ["002"])

    def test_tab_stock_failure_shows_unknown(self):
        self.tab_down = True
        res, parts, cs = self.build()
        self.assertIsNone(cs["CS-1-TAB"]["tab_stock"])
        self.assertIsNone(cs["CS-1-TAB"]["shortage"])
        self.assertFalse(cs["CS-1-TAB"]["stock_known"])
        self.assertIn("庫存查詢失敗", cs["CS-1-TAB"]["stock_error"])
        self.assertTrue(any("客供料倉庫" in w for w in res["warnings"]))
        self.assertIn("EC-1", parts)  # 其餘照常

    def test_erp_down_falls_back_to_suffix_rule_only(self):
        @contextmanager
        def broken():
            raise RuntimeError("down")
            yield

        with patch.object(ep, "_source_session", broken), patch.object(low_stock, "_erp_session", broken):
            res, parts, cs = self.build()
        self.assertEqual(set(cs), {"CS-1-TAB", "CS-2-TAB"})  # 只看 -TAB 結尾，與正航連線無關
        self.assertNotIn("CS-1-TAB", parts)
        self.assertIsNone(cs["CS-2-TAB"]["tab_stock"])
        self.assertTrue(res["warnings"])

    def test_part_without_supplier_unchanged(self):
        _, parts, _ = self.build()
        self.assertEqual(parts["NOSUP"]["supplier_id"], "")
        self.assertEqual(parts["NOSUP"]["supplier_source"], "none")
        self.assertEqual(parts["EC-1"]["supplier_id"], "S1")

    def test_classified_after_conversion(self):
        # A 轉成 B：以轉換後的料號判斷（B 是 -TAB → 客供料；A 的需求併入）
        db.apply_part_conversion("OLD-1", "CS-2-TAB", [], {}, [], "t")
        self.groups = [group(1, "1-1", "2026-12-10", [item("OLD-1", 4), item("CS-2-TAB", 8)])]
        _, parts, cs = self.build()
        self.assertNotIn("OLD-1", parts)
        self.assertEqual(cs["CS-2-TAB"]["need"], 12)
        self.assertEqual(cs["CS-2-TAB"]["shortage"], 7)

    def test_only_customer_supplied_does_not_warn_empty(self):
        self.groups = [group(1, "1-1", "2026-12-10", [item("CS-1-TAB", 10)])]
        res, parts, cs = self.build()
        self.assertEqual(parts, {})
        self.assertFalse(any("沒有補料" in w for w in res["warnings"]))


    def test_class_003_non_tab_is_purchasable(self):
        self.groups = [group(1, "1-1", "2026-12-10", [item("OC-10222B", 40)])]
        self.st["OC-10222B"] = 0
        res, parts, cs = self.build()
        self.assertEqual(cs, {})
        self.assertIn("OC-10222B", parts)
        self.assertEqual(parts["OC-10222B"]["supplier_id"], "S1")
        self.assertIn("OC-10222B", self.on_order_parts)
