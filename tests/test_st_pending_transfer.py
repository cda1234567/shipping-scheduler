"""v2026.11.04.9：缺料採購「待調撥」（B1）。"""
from __future__ import annotations

import os
import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.requests import Request

import app.database as db
from app.routers import dispatch as dispatch_router
from app.services import erp_purchase as ep
from app.services import low_stock
from app.services import moq as moq_service
from app.services import part_conversion
from app.services import shortage_purchase as sp
from app.services import st_pending_transfer as spt
from app.services.edit_auth import request_requires_edit_auth

from tests.test_shortage_purchase import TODAY, Base, FakeErpConn, group, item


class PureTests(unittest.TestCase):
    def test_parse_code(self):
        self.assertEqual(spt.parse_code("10-3"), (10, 3))
        self.assertEqual(spt.parse_code(" 1-12 "), (1, 12))
        for bad in ("", "A", "13-1", "0-2", "10-", "10-3a"):
            self.assertIsNone(spt.parse_code(bad), bad)

    def test_batch_year_rollover(self):
        self.assertEqual(spt.batch_year(10, date(2026, 9, 18)), 2026)
        self.assertEqual(spt.batch_year(12, date(2026, 3, 11)), 2025)   # 3 月建立的舊年度 12-x
        self.assertEqual(spt.batch_year(11, date(2026, 3, 11)), 2025)
        self.assertEqual(spt.batch_year(1, date(2026, 3, 11)), 2026)
        self.assertEqual(spt.batch_year(1, date(2026, 11, 20)), 2027)   # 11 月發料的 1-x → 隔年
        self.assertEqual(spt.batch_year(12, date(2026, 10, 29)), 2026)

    def test_parse_setting(self):
        self.assertEqual(spt.parse_setting("10-3"), (None, (10, 3)))
        self.assertEqual(spt.parse_setting("2026/10-3"), (2026, (10, 3)))
        self.assertEqual(spt.parse_setting("x/10-3"), (None, None))

    def test_list_batches_order_and_labels(self):
        orders = [
            {"id": 1, "code": "11-5", "status": "dispatched", "created_at": "2026-03-11T00:00:00", "first_dispatched_at": ""},
            {"id": 2, "code": "12-1", "status": "dispatched", "created_at": "2026-03-11T00:00:00", "first_dispatched_at": ""},
            {"id": 3, "code": "1-2", "status": "dispatched", "created_at": "2026-03-11T00:00:00", "first_dispatched_at": ""},
            {"id": 10, "code": "10-10", "status": "dispatched", "created_at": "", "first_dispatched_at": "2026-10-14T09:00:00"},
            {"id": 11, "code": "10-3", "status": "dispatched", "created_at": "", "first_dispatched_at": "2026-09-18T09:00:00"},
            {"id": 12, "code": "11-5", "status": "dispatched", "created_at": "", "first_dispatched_at": "2026-11-04T16:00:00"},
            {"id": 13, "code": "10-3", "status": "dispatched", "created_at": "", "first_dispatched_at": "2026-09-19T09:00:00"},
            {"id": 14, "code": "", "status": "dispatched", "created_at": "", "first_dispatched_at": "2026-09-19T09:00:00"},
            {"id": 15, "code": "1-1", "status": "dispatched", "created_at": "", "first_dispatched_at": "2026-11-20T09:00:00"},
        ]
        b = spt.list_batches(orders)
        self.assertEqual([x["key"] for x in b],
                         ["2025/11-5", "2025/12-1", "2026/1-2", "2026/10-3", "2026/10-10", "2026/11-5", "2027/1-1"])
        self.assertEqual(b[3]["order_ids"], [11, 13])
        self.assertEqual(b[3]["first_dispatched_at"], "2026-09-18T09:00:00")
        self.assertEqual(b[3]["label"], "10-3　發料 09/18")
        self.assertEqual(b[0]["label"], "11-5（2025）")  # 同代碼跨年 → 標年份
        # 沒寫年份 → 同代碼取最新那批
        self.assertEqual(spt.resolve_cutoff("11-5", b)["key"], "2026/11-5")
        self.assertEqual(spt.resolve_cutoff("2025/11-5", b)["key"], "2025/11-5")
        self.assertIsNone(spt.resolve_cutoff("9-9", b))

    def test_net_pending(self):
        self.assertEqual(spt.net_pending(10000, 0), {"deducted": 10000, "transferred": 0, "pending": 10000, "over": 0})
        self.assertEqual(spt.net_pending(10000, 4000)["pending"], 6000)
        r = spt.net_pending(1000, 3000)
        self.assertEqual((r["pending"], r["over"]), (0, 2000))  # 超轉：不隱藏，待調撥當 0


class FakeTransferCursor:
    """comProdRec Flag 400：依 SQL 型態回傳結算日偵測 / 淨調撥。"""

    def __init__(self, data):
        self.d = data
        self.rows = []

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.d["sql"].append((s, params))
        assert "comProdRec" in s and "Flag = 400" in s, s
        assert "INSERT" not in s.upper() and "UPDATE" not in s.upper()
        if s.startswith("SELECT BillDate, ProdID"):
            st, gl, start, *parts = params
            self.rows = sorted({(d, p) for d, p, wo, wi, q in self.d["lines"]
                                if wo == st and wi == gl and d >= start and p in parts})
        else:
            after, st, gl, gl2, st2 = params
            agg = {}
            for d, p, wo, wi, q in self.d["lines"]:
                if d > after and ((wo == st and wi == gl) or (wo == gl2 and wi == st2)):
                    agg[(p, wo)] = agg.get((p, wo), 0) + q
            self.rows = [(p, wo, q) for (p, wo), q in agg.items()]

    def fetchall(self):
        return list(self.rows)


class FakeTransferConn:
    def __init__(self, data):
        self.data = data

    def cursor(self):
        return FakeTransferCursor(self.data)


class ComputeBase(Base):
    """模擬：10-2（08/18）、10-3（09/18）已在 10/06 調撥；10-9（10/14）、11-5（11/04）還沒調撥。"""

    def setUp(self):
        super().setUp()
        self.erp_t = {"sql": [], "lines": [
            # (BillDate, ProdID, WareOut, WareID, Quantity)
            (20261006, "EC-10025A", "001", "015", 40000),   # 9-x / 10-2 的料（截止以前）
            (20261006, "EC-B", "001", "015", 300),
            (20261006, "EC-C", "001", "015", 100),
            (20260922, "EC-B", "001", "015", 50),            # 截止批次發料後、結算日前：不算
            (20261013, "EC-20117A", "001", "015", 4000),     # 10-9 的料已補調撥
            (20261020, "EC-20117A", "015", "001", 1000),     # 退回 ST：扣回
            (20261015, "EC-20117A", "001", "018", 9999),     # 其他倉：不算
            (20261015, "EC-20117A", "002", "015", 9999),     # 非 ST 倉：不算
            (20261104, "OC-OLD", "001", "015", 1000),        # 截止以前的料零星補調撥 → 超轉
            (20261016, "A-OLD", "001", "015", 70),           # 轉換料：A-OLD → A-NEW
        ]}
        self.loaded_at = "2026-11-04T16:56:23"
        self.conversions = {"A-OLD": "A-NEW"}

        @contextmanager
        def erp_session():
            yield FakeTransferConn(self.erp_t)

        patches = [
            patch.object(low_stock, "_erp_session", erp_session),
            patch.object(spt, "ware_ids", lambda: ("001", "015")),
            patch.object(part_conversion, "active_map", lambda: self.conversions),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        db.set_setting("st_inventory_loaded_at", self.loaded_at)
        self.orders = {}
        for oid, code, at in ((209, "10-2", "2026-08-18T10:00:00"), (210, "10-3", "2026-09-18T10:00:00"),
                              (221, "10-9", "2026-10-14T10:00:00"), (264, "11-5", "2026-11-04T16:10:00"),
                              (300, "11-6", "2026-11-04T17:30:00")):
            self.add_order(oid, code, at)
        self.consume(209, "EC-10025A", 10000)
        self.consume(210, "EC-B", 300)
        self.consume(210, "EC-C", 100)
        self.consume(221, "EC-10025A", 10000)
        self.consume(221, "EC-20117A", 4000)
        self.consume(221, "A-OLD", 100)
        self.consume(264, "EC-10025A", 10000)
        self.consume(300, "EC-10025A", 555, at="2026-11-04T17:30:00")  # ST 同步之後：本機 ST 已扣過
        self.consume(221, "EC-ROLLED", 777, rolled_back=True)

    def add_order(self, oid, code, at, status="dispatched"):
        self.conn.execute("INSERT INTO orders(id, code, model, po_number, status) VALUES(?,?,?,?,?)",
                          (oid, code, "M", f"PO{oid}", status))
        cur = self.conn.execute("INSERT INTO dispatch_sessions(order_id, dispatched_at) VALUES(?,?)", (oid, at))
        self.orders[oid] = cur.lastrowid

    def consume(self, oid, part, qty, at=None, rolled_back=False):
        self.conn.execute(
            "INSERT INTO st_dispatch_consumptions(dispatch_session_id, order_id, part_number, used_qty, consumed_at, rolled_back_at)"
            " VALUES(?,?,?,?,?,?)",
            (self.orders[oid], oid, part, qty, at or "2026-10-14T10:00:00", "x" if rolled_back else ""))


class ComputeTests(ComputeBase):
    def test_default_cutoff_and_settle_date(self):
        self.assertEqual(spt.get_cutoff_setting(), "10-3")
        r = spt.compute([264], self.conversions)
        info = r["info"]
        self.assertEqual((info["cutoff_code"], info["settle_date"], info["settle_source"]), ("10-3", "2026-10-06", "auto"))
        self.assertEqual(info["batches"], ["10-9", "11-5", "11-6"])
        self.assertEqual(r["warnings"], [])
        p = r["parts"]
        # EC-10025A：10/06 的 40000 屬截止以前，不抵；10-9 的 10000 未調撥；11-5 是本次勾選；11-6 在同步後
        self.assertEqual(p["EC-10025A"], {"deducted": 10000, "transferred": 0, "pending": 10000, "over": 0})
        # EC-20117A：扣 4000，調撥 4000 − 退回 1000 = 3000；018 / 002 不算
        self.assertEqual((p["EC-20117A"]["transferred"], p["EC-20117A"]["pending"]), (3000, 1000))
        self.assertEqual(p["OC-OLD"], {"deducted": 0, "transferred": 1000, "pending": 0, "over": 1000})
        self.assertEqual(p["A-NEW"]["pending"], 30)  # 轉換料併到最終料號
        self.assertNotIn("A-OLD", p)
        self.assertNotIn("EC-ROLLED", p)
        self.assertNotIn("EC-B", p)  # 09/22 在結算日之前

    def test_selected_orders_excluded_and_cutoff_change(self):
        p = spt.compute([], self.conversions)["parts"]
        self.assertEqual(p["EC-10025A"]["deducted"], 20000)  # 10-9 + 11-5（不含同步後的 11-6）
        spt.set_cutoff_setting("10-9")
        self.assertEqual(db.get_setting(spt.SETTING_KEY), "2026/10-9")
        r = spt.compute([], self.conversions)
        self.assertEqual(r["info"]["batches"], ["11-5", "11-6"])
        # 10-9 的料在 10/13、10/16 各調一項：同票數取較晚那天
        self.assertEqual(r["info"]["settle_date"], "2026-10-16")
        self.assertEqual(r["parts"]["EC-10025A"]["pending"], 10000)  # 只剩 11-5
        # 勾選 11-5 後已無待調撥扣量 → 不查正航
        n = len(self.erp_t["sql"])
        self.assertEqual(spt.compute([264], self.conversions)["parts"], {})
        self.assertEqual(len(self.erp_t["sql"]), n)

    def test_unknown_cutoff_warns_and_no_pending(self):
        db.set_setting(spt.SETTING_KEY, "9-9")
        r = spt.compute([264], self.conversions)
        self.assertEqual(r["parts"], {})
        self.assertIn("找不到截止批次", r["warnings"][0])
        with self.assertRaises(ValueError):
            spt.set_cutoff_setting("9-9")

    def test_erp_failure_falls_back_to_zero_with_warning(self):
        @contextmanager
        def broken():
            raise low_stock.ErpUnavailable("down")
            yield  # pragma: no cover

        with patch.object(low_stock, "_erp_session", broken):
            r = spt.compute([264], self.conversions)
        self.assertEqual(r["parts"], {})
        self.assertIn("正航調撥查詢失敗", r["warnings"][0])

    def test_no_settle_transfer_uses_cutoff_dispatch_date(self):
        self.erp_t["lines"] = [l for l in self.erp_t["lines"] if l[1] not in ("EC-B", "EC-C")]
        r = spt.compute([264], self.conversions)
        self.assertEqual((r["info"]["settle_source"], r["info"]["settle_date"]), ("fallback", "2026-09-18"))
        self.assertIn("找不到 10-3 的調撥紀錄", r["warnings"][0])
        # 從 09/18 之後起算 → 10/06 的 40000 會被算進來 → EC-10025A 超轉
        self.assertEqual(r["parts"]["EC-10025A"]["over"], 30000)

    def test_queries_are_read_only_and_scoped_to_st_and_gl(self):
        spt.compute([264], self.conversions)
        for s, params in self.erp_t["sql"]:
            self.assertTrue(s.startswith("SELECT"), s)
            self.assertIn("WITH (NOLOCK)", s)
            self.assertIn("001", params)
            self.assertIn("015", params)


class ShortageListPendingTests(ComputeBase):
    def setUp(self):
        super().setUp()
        self.groups = [group(264, "11-5", "2026-12-10", [item("EC-10025A", 10000), item("EC-20117A", 1000),
                                                          item("OC-OLD", 10), item("AB-TAB", 5)])]
        self.st = {"EC-10025A": 10000, "EC-20117A": 3000, "OC-OLD": 100}

        @contextmanager
        def source_session():
            yield FakeErpConn({"sql": [], "params": [], "products": [], "last_po": [], "suppliers": []})

        patches = [
            patch.object(dispatch_router, "build_dispatch_groups", lambda ids, dec: self.groups),
            patch.object(db, "get_st_inventory_stock", lambda: self.st),
            patch.object(ep, "_source_session", source_session),
            patch.object(low_stock, "_query_on_order", lambda conn, parts, today, md: {"EC-10025A": (5000.0, "2026-11-20")}),
            patch.object(low_stock, "get_config", lambda: {"on_order_max_overdue_days": 90}),
            patch.object(low_stock, "_query_tab_stock", lambda *a: {}, create=True),
            patch.object(sp, "_query_tab_stock", lambda conn, parts, wares: {}),
            patch.object(ep, "get_config", lambda: {"label": "測試", "available": True, "is_test": True}),
            patch.object(sp, "local_now", lambda: TODAY),
            patch.object(moq_service, "site_moq_map", lambda: {}),
        ]
        moq_service.clear_receipt_cache()
        self.addCleanup(moq_service.clear_receipt_cache)
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_shortage_includes_pending(self):
        res = sp.build_shortage_list([264])
        p = {x["part_number"]: x for x in res["parts"]}
        a = p["EC-10025A"]
        # 需求 10000 + 待調撥 10000 − ST 10000 = 10000；在途 5000 只供參考
        self.assertEqual((a["need"], a["pending_transfer"], a["st"], a["on_order"], a["shortage"]),
                         (10000, 10000, 10000, 5000, 10000))
        self.assertTrue(a["needs_purchase"])
        self.assertEqual(sum(x["qty"] for x in a["allocations"] if x["kind"] == "order"), 10000)
        b = p["EC-20117A"]
        self.assertEqual((b["pending_transfer"], b["shortage"]), (1000, 0))  # 1000 + 1000 − 3000 < 0
        c = p["OC-OLD"]
        self.assertEqual((c["pending_transfer"], c["over_transfer"], c["shortage"]), (0, 1000, 0))
        self.assertNotIn("AB-TAB", p)  # 客供料照舊不採購
        self.assertEqual(res["pending_transfer"]["cutoff_code"], "10-3")

    def test_allocation_context_uses_st_minus_pending(self):
        # v2026.11.11.2：ST 與待調撥分開存，分配時才用 ST − 待調撥；缺 / 建議量與缺料清單同一套
        ctx = sp._allocation_context([264])
        a, b = ctx["EC-10025A"], ctx["EC-20117A"]
        self.assertEqual((a["st"] - a["pending"], a["shortage"], a["suggested_qty"]), (0, 10000, 10000))
        self.assertEqual((b["st"] - b["pending"], b["shortage"]), (2000, 0))


class RouterTests(ComputeBase):
    def setUp(self):
        super().setUp()
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)

    def test_get_and_set_cutoff(self):
        r = self.client.get("/api/shortage-po/transfer-cutoff")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual((data["cutoff"], data["cutoff_code"], data["cutoff_found"]), ("2026/10-3", "10-3", True))
        self.assertEqual([b["code"] for b in data["batches"]], ["10-2", "10-3", "10-9", "11-5", "11-6"])
        r = self.client.post("/api/shortage-po/transfer-cutoff", json={"cutoff": "2026/10-9"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["cutoff_code"], "10-9")
        self.assertEqual(self.client.post("/api/shortage-po/transfer-cutoff", json={"cutoff": "zz"}).status_code, 400)

    def test_post_requires_edit_auth(self):
        req = Request({"type": "http", "method": "POST", "path": "/api/shortage-po/transfer-cutoff",
                       "headers": [], "query_string": b""})
        self.assertTrue(request_requires_edit_auth(req))


if __name__ == "__main__":
    unittest.main()
