from __future__ import annotations

import os
import re
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.requests import Request

import app.database as db
from app.services import erp_purchase as ep, erp_stock
from app.services.edit_auth import request_requires_edit_auth
from app.services.erp_stock import ErpNotConfigured

MAIN_OVERRIDE_COLS = [
    "Flag", "BillNO", "BillDate", "Remark", "SumQty", "SumBTaxAmt", "SumTax", "SumAmtATax", "LocalTotal",
    "LocalTax", "BillStatus", "EndacaseDate", "TaxType", "SalesMan", "Maker", "MakerID", "Permitter",
    "PermitterID", "IsTrans", "MergeOutState", "CustBillNo", "AccMonth", "PrepayDay", "EncashDay",
]
MAIN_COLS = MAIN_OVERRIDE_COLS + ["CustomerID", "DueTo", "CustAddress", "CurrID"]
SUB_OVERRIDE_COLS = [
    "Flag", "BillNO", "BillDate", "RowNO", "SerNO", "FromRow", "FromNO", "ProdID", "ProdName", "Quantity",
    "Price", "Amount", "TaxRate", "TaxAmt", "PreInDate", "QtyRemain", "MlAmount", "MLTaxAmt", "sPrice",
    "sQuantity", "EQuantity", "OutQty", "BClearTranQty", "UnValidQty", "Discount", "IsGift", "ItemRemark",
    "NeedDate",
]
SUB_COLS = SUB_OVERRIDE_COLS + ["Unit", "Warehouse"]
CUSTOMER = {"Flag": 2, "ID": "P0175", "ShortName": "供應商甲", "FullName": "供應商甲股份有限公司"}


def main_tpl(**kw):
    row = {c: 0 for c in MAIN_COLS}
    row.update(Flag=4, BillNO="PO-20260901001", BillDate=20260901, CustomerID="P0175", DueTo="月結30天",
               CustAddress="台北", CurrID="NTD", Remark="old", SalesMan="099", Maker="X", MakerID="099",
               Permitter="Y", PermitterID="Y", CustBillNo="CB", AccMonth=202610, PrepayDay=20261031,
               EncashDay=20260901, BillStatus=1, TaxType=1)
    row.update(kw)
    return row


def sub_tpl(**kw):
    row = {c: 0 for c in SUB_COLS}
    row.update(Flag=4, BillNO="PO-20260801001", ProdID="AC-40838B", Unit="PCS", Warehouse="001", ItemRemark="x",
               FromNO="Z", ProdName="old")
    row.update(kw)
    return row


class FakeCursor:
    def __init__(self, conn):
        self.c = conn
        self.rows = []
        self.description = None

    def _set(self, rows, names=None):
        self.rows = rows
        self.description = [(n,) for n in names] if names else None

    def _dicts(self, d):
        self._set([tuple(d.values())] if d else [], list(d.keys()) if d else ["x"])

    def execute(self, sql, params=None):
        c = self.c
        s = " ".join(sql.split())
        c.log.append((s, params))
        if s.startswith("INSERT INTO"):
            table = re.match(r"INSERT INTO dbo\.(\w+)", s).group(1)
            if c.fail_insert == table:
                raise RuntimeError("insert boom secret-pw")
            names = re.search(r"\((.*?)\) VALUES", s).group(1).replace("[", "").replace("]", "").split(", ")
            c.inserted.append((table, dict(zip(names, params)), names))
            return
        if s == "SELECT DB_NAME()":
            return self._set([(c.db_name,)])
        if "sys.columns" in s:
            table = params[0].split(".")[1]
            return self._set([(n, 0, 0) for n in c.columns.get(table, [])])
        if "MAX(BillNO)" in s:
            c.max_sql = s
            return self._set([(c.max_bill,)])
        if s.startswith("SELECT 1 FROM ordBillSub"):
            return self._set([(1,)] if params[0] in c.sub_bills else [])
        m = re.match(r"SELECT 1 FROM (comCustomer|comProductClass|comProduct|comWareHouse|comDepartment|comPerson)", s)
        if m:
            c.master_checks.append((m.group(1), params[0]))
            return self._set([(1,)] if params[0] in c.master_ids.get(m.group(1), set()) else [])
        m = re.match(r"SELECT \* FROM (comProductClass|comDepartment|comPerson)", s)
        if m:
            key_col = {"comProductClass": "ClassID", "comDepartment": "DepartID", "comPerson": "PersonID"}[m.group(1)]
            return self._dicts({key_col: params[0], "Name": "n"} if params[0] in c.master_rows.get(m.group(1), set()) else None)
        if s.startswith("SELECT * FROM comCustomer"):
            return self._dicts(CUSTOMER if params[0] == "P0175" else None)
        if s.startswith("SELECT * FROM comProduct"):
            p = c.products.get(params[0])
            return self._dicts(p)
        if s.startswith("SELECT * FROM comWareHouse"):
            return self._dicts({"WareHouseID": params[0], "WareHouseName": "ST"})
        if s.startswith("SELECT TOP 1 * FROM ordBillMain"):
            return self._dicts(c.main_template)
        if s.startswith("SELECT TOP 1 * FROM ordBillSub WHERE Flag = 4 AND ProdID"):
            return self._dicts(c.sub_templates.get(params[0]))
        if s.startswith("SELECT TOP 1 * FROM ordBillSub WHERE Flag = 4 AND BillNO"):
            return self._dicts(c.fallback_sub)
        if s.startswith("SELECT BillNO, SumQty"):
            main = [i for i in c.inserted if i[0] == "ordBillMain"][0][1]
            keys = ["BillNO", "SumQty", "SumBTaxAmt", "SumTax", "SumAmtATax"]
            return self._set([tuple(main[k] for k in keys)], keys)
        if s.startswith("SELECT RowNO, ProdID"):
            keys = ["RowNO", "ProdID", "Quantity", "Price", "Amount", "TaxAmt", "PreInDate"]
            subs = [i[1] for i in c.inserted if i[0] == "ordBillSub"]
            if c.corrupt_verify and subs:
                subs[0] = {**subs[0], "Amount": Decimal("999")}
            return self._set([tuple(r[k] for k in keys) for r in subs], keys)
        raise AssertionError(f"unexpected SQL: {s}")

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class FakeConn:
    def __init__(self, db_name="CHIComp01"):
        self.db_name = db_name
        self.log, self.inserted = [], []
        self.commits = self.rollbacks = 0
        self.closed = False
        self.fail_insert = None
        self.corrupt_verify = False
        self.max_bill = None
        self.max_sql = ""
        self.sub_bills = set()
        self.columns = {}
        self.master_ids = {"comCustomer": {"P0175"}, "comProduct": {"AC-40838B"}, "comWareHouse": {"001"},
                           "comProductClass": set(), "comDepartment": set(), "comPerson": {"017"}}
        self.master_rows = {"comProductClass": {"C1"}, "comDepartment": {"D1"}, "comPerson": {"017"}}  # 來源有的
        self.master_checks = []
        self.products = {"AC-40838B": {"ProdID": "AC-40838B", "ProdName": "電容"}}
        self.main_template = main_tpl()
        self.sub_templates = {"AC-40838B": sub_tpl()}
        self.fallback_sub = sub_tpl(ProdID="OTHER")

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def make_target(db_name="CHIComp01"):
    t = FakeConn(db_name)
    # 目標欄位順序刻意與來源列順序相反
    t.columns = {"ordBillMain": list(reversed(MAIN_COLS)), "ordBillSub": list(reversed(SUB_COLS)),
                 "comCustomer": list(CUSTOMER), "comProduct": ["ProdID", "ProdName"],
                 "comWareHouse": ["WareHouseID", "WareHouseName"],
                 "comProductClass": ["ClassID", "Name"], "comDepartment": ["DepartID", "Name"],
                 "comPerson": ["PersonID", "Name"]}
    return t


REQ = {
    "supplier_id": "P0175", "remark": "測試", "salesman": None,
    "lines": [{"part_number": "ac-40838b", "qty": 10, "price": 10, "pre_in_date": "2026-11-15"}],
}
ENV = {"ERP_SQL_TEST_USER": "tester", "ERP_SQL_TEST_PASSWORD": "secret-pw", "ERP_WRITE_TARGET": "CHIComp01"}


class EnvCase(unittest.TestCase):
    extra_env: dict = {}

    def setUp(self):
        clean = {k: v for k, v in os.environ.items() if not k.startswith(("ERP_SQL_WRITE", "ERP_SQL_TEST", "ERP_WRITE", "ERP_PO"))}
        clean.update(ENV)
        clean.update(self.extra_env)
        p = patch.dict(os.environ, clean, clear=True)
        p.start()
        self.addCleanup(p.stop)


class PureTests(unittest.TestCase):
    def test_money_rounding(self):
        lines, t = ep.compute_money([
            {"qty": Decimal("10"), "price": Decimal("10")},
            {"qty": Decimal("3"), "price": Decimal("0.125")},  # 0.375 → 0.38
            {"qty": Decimal("1"), "price": Decimal("10.5")},   # 稅 0.525 → 1
        ])
        self.assertEqual([l["amount"] for l in lines], [Decimal("100.00"), Decimal("0.38"), Decimal("10.50")])
        self.assertEqual([l["tax_amt"] for l in lines], [Decimal("5"), Decimal("0"), Decimal("1")])
        self.assertEqual(t["sum_amount"], Decimal("110.88"))
        self.assertEqual(t["sum_tax"], Decimal("6"))  # 5.544 → 6
        self.assertEqual(t["total"], Decimal("116.88"))
        self.assertEqual(t["sum_qty"], Decimal("14"))

    def test_money_example_row(self):
        lines, t = ep.compute_money([{"qty": Decimal("10"), "price": Decimal("10")}])
        self.assertEqual((lines[0]["amount"], lines[0]["tax_amt"], t["total"]), (100, 5, 105))

    def test_payment_dates_same_day_keeps_template(self):
        tpl = {"BillDate": 20261029, "AccMonth": 202611, "PrepayDay": 20261130, "EncashDay": 20261129}
        self.assertEqual(ep.payment_dates(tpl, 20261029), {"AccMonth": 202611, "PrepayDay": 20261130, "EncashDay": 20261129})

    def test_payment_dates_shift_and_clamp(self):
        tpl = {"BillDate": 20260831, "AccMonth": 202609, "PrepayDay": 20260831, "EncashDay": 20261031}
        got = ep.payment_dates(tpl, 20261029)  # +2 個月
        self.assertEqual(got, {"AccMonth": 202611, "PrepayDay": 20261031, "EncashDay": 20261231})
        tpl = {"BillDate": 20260101, "AccMonth": 0, "PrepayDay": 0, "EncashDay": 20260131}
        self.assertEqual(ep.payment_dates(tpl, 20260301)["EncashDay"], 20260331)
        self.assertEqual(ep.shift_ymd(20260131, 1), 20260228)
        self.assertEqual(ep.shift_ymd(20261130, 3), 20270228)

    def test_payment_dates_encash_equals_bill_date_uses_today(self):
        tpl = {"BillDate": 20260901, "AccMonth": 202610, "PrepayDay": 0, "EncashDay": 20260901}
        got = ep.payment_dates(tpl, 20261029)
        self.assertEqual(got["EncashDay"], 20261029)
        self.assertEqual(got["AccMonth"], 202611)
        self.assertEqual(got["PrepayDay"], 0)

    def test_bill_no_generation(self):
        t = FakeConn()
        cur = t.cursor()
        self.assertEqual(ep.next_bill_no(cur, 20261029, lock=True), "PO-20261029001")
        self.assertIn("UPDLOCK, HOLDLOCK", t.max_sql)
        t.max_bill = "PO-20261029007"
        self.assertEqual(ep.next_bill_no(cur, 20261029, lock=False), "PO-20261029008")
        self.assertNotIn("UPDLOCK", t.max_sql)
        t.sub_bills = {"PO-20261029008"}
        self.assertEqual(ep.next_bill_no(cur, 20261029, lock=False), "PO-20261029009")
        t.max_bill = "PO-20261029999"
        with self.assertRaises(ep.PoValidationError):
            ep.next_bill_no(cur, 20261029, lock=True)

    def test_parse_server(self):
        self.assertEqual(ep.parse_server("10.0.0.5,1500", "h", 1433), ("10.0.0.5", 1500))
        self.assertEqual(ep.parse_server("10.0.0.5", "h", 1433), ("10.0.0.5", 1433))
        self.assertEqual(ep.parse_server("", "h", 1433), ("h", 1433))
        self.assertEqual(ep.parse_server("x,abc", "h", 1433), ("x", 1433))

    def test_labels(self):
        self.assertEqual(ep.target_label("CHIComp01"), "公司1（測試）")
        self.assertEqual(ep.target_label("CHIComp02"), "公司2（測試）")
        self.assertEqual(ep.target_label("CHIComp03"), "公司3（正式）")
        self.assertEqual(ep.target_label("CHIComp05"), "公司5")

    def test_build_insert_detects_missing_and_extra(self):
        cols = [{"name": "A", "identity": False, "computed": False}, {"name": "B", "identity": False, "computed": False}]
        sql, params = ep.build_insert("T", cols, {"b": 2, "a": 1})
        self.assertEqual(sql, "INSERT INTO dbo.T ([A], [B]) VALUES (%s, %s)")
        self.assertEqual(params, (1, 2))
        with self.assertRaises(ep.PoSchemaError):
            ep.build_insert("T", cols, {"a": 1})
        with self.assertRaises(ep.PoSchemaError):
            ep.build_insert("T", cols, {"a": 1, "b": 2, "c": 3})

    def test_normalize_validation(self):
        bad = [
            {**REQ, "supplier_id": " "},
            {**REQ, "lines": []},
            {**REQ, "lines": [REQ["lines"][0]] * 51},
            {**REQ, "lines": [{**REQ["lines"][0], "qty": 0}]},
            {**REQ, "lines": [{**REQ["lines"][0], "price": -1}]},
            {**REQ, "lines": [{**REQ["lines"][0], "pre_in_date": "2026-13-01"}]},
            {**REQ, "lines": [{**REQ["lines"][0], "qty": float("nan")}]},
        ]
        for r in bad:
            with self.assertRaises(ep.PoValidationError, msg=str(r)[:80]):
                ep.normalize_request(r)
        ok = ep.normalize_request(REQ)
        self.assertEqual(ok["lines"][0]["part_number"], "AC-40838B")
        self.assertEqual(ok["lines"][0]["pre_in_date"], 20261115)


class CredentialTests(EnvCase):
    def test_test_login_used_for_both_test_targets(self):
        for target in ("CHIComp01", "CHIComp02"):
            creds = ep.write_credentials(target)
            self.assertEqual((creds["user"], creds["database"]), ("tester", target))
        # ERP_SQL_TEST_DB / _ALT 完全不影響：資料庫永遠是 target
        with patch.dict(os.environ, {"ERP_SQL_TEST_DB": "CHIComp01", "ERP_SQL_TEST_DB_ALT": "CHIComp09"}):
            self.assertEqual(ep.write_credentials("CHIComp02")["database"], "CHIComp02")
            self.assertIsNone(ep.write_credentials("CHIComp09"))
        with patch.dict(os.environ, {"ERP_SQL_TEST_DB": "CHIComp03"}):
            self.assertIsNone(ep.write_credentials("CHIComp03"))
            self.assertIsNotNone(ep.write_credentials("CHIComp01"))

    def test_test_login_never_used_for_production(self):
        self.assertIsNone(ep.write_credentials("CHIComp03"))
        self.assertIsNone(ep.write_credentials("CHIComp05"))

    def test_test_server_fallback(self):
        with patch.dict(os.environ, {"ERP_SQL_TEST_SERVER": "10.1.1.1,1444"}):
            c = ep.write_credentials("CHIComp02")
        self.assertEqual((c["server"], c["port"]), ("10.1.1.1", 1444))

    def test_per_target_creds_win_over_test_login(self):
        with patch.dict(os.environ, {"ERP_SQL_WRITE_USER_CHICOMP02": "w2", "ERP_SQL_WRITE_PASSWORD_CHICOMP02": "pw2"}):
            self.assertEqual(ep.write_credentials("CHIComp02")["user"], "w2")
            self.assertEqual(ep.write_credentials("CHIComp01")["user"], "tester")

    def test_default_target_is_comp02(self):
        with patch.dict(os.environ):
            os.environ.pop("ERP_WRITE_TARGET", None)
            self.assertEqual(ep.get_target(), "CHIComp02")
            cfg = ep.get_config()
            self.assertEqual((cfg["target"], cfg["label"], cfg["is_test"]), ("CHIComp02", "公司2（測試）", True))

    def test_specific_write_creds_and_server(self):
        with patch.dict(os.environ, {"ERP_SQL_WRITE_USER_CHICOMP03": "w", "ERP_SQL_WRITE_PASSWORD_CHICOMP03": "pw",
                                     "ERP_SQL_WRITE_SERVER_CHICOMP03": "10.9.9.9,1555"}):
            c = ep.write_credentials("CHIComp03")
        self.assertEqual((c["user"], c["server"], c["port"]), ("w", "10.9.9.9", 1555))

    def test_config_never_leaks_secrets_and_unavailable(self):
        cfg = ep.get_config()
        self.assertTrue(cfg["available"] and cfg["is_test"])
        self.assertNotIn("secret-pw", str(cfg))
        self.assertNotIn("tester", str(cfg))
        self.assertEqual([t["db"] for t in cfg["targets"]], ["CHIComp01", "CHIComp02", "CHIComp03"])
        self.assertEqual([t["available"] for t in cfg["targets"]], [True, True, False])
        with patch.dict(os.environ, {"ERP_WRITE_TARGET": "CHIComp03"}):
            cfg = ep.get_config()
            self.assertFalse(cfg["available"])
            self.assertFalse(cfg["is_test"])
            self.assertEqual(cfg["reason"], "無法使用（未設定寫入帳號）")
            with self.assertRaises(ErpNotConfigured):
                ep.preview_po(REQ)
            with self.assertRaises(ErpNotConfigured):
                ep.create_po({**REQ, "confirm": True})

    def test_invalid_target_rejected(self):
        with patch.dict(os.environ, {"ERP_WRITE_TARGET": "master; DROP"}):
            self.assertFalse(ep.get_config()["available"])
            with self.assertRaises(ErpNotConfigured):
                ep.preview_po(REQ)


class WriteFlowCase(EnvCase):
    def setUp(self):
        super().setUp()
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

        self.src = FakeConn("CHIComp03")
        self.tgt = make_target("CHIComp01")
        src = self.src

        @contextmanager
        def source_session():
            yield src

        self.connect_calls = []

        def connect_write(creds):
            self.connect_calls.append(creds["database"])
            return self.tgt

        for p in (
            patch.object(db, "get_conn", temp_conn),
            patch.object(ep, "_source_session", source_session),
            patch.object(ep, "_connect_write", connect_write),
            patch.object(erp_stock, "get_settings", lambda: {
                "server": "s", "port": 1433, "database": "CHIComp03", "ware_id": "001",
                "timeout": 5.0, "encryption": "off", "tds_version": "7.3"}),
            patch.object(ep, "local_now", lambda: datetime(2026, 10, 29, 10, 0)),
        ):
            p.start()
            self.addCleanup(p.stop)

    def inserts(self, table):
        return [i for i in self.tgt.inserted if i[0] == table]

    def test_create_example_row(self):
        res = ep.create_po({**REQ, "confirm": True}, actor="web", client_ip="1.2.3.4")
        self.assertEqual(res["bill_no"], "PO-20261029001")
        self.assertEqual(res["totals"]["total"], 105.0)
        self.assertEqual((self.tgt.commits, self.tgt.rollbacks), (1, 0))
        (_, main, names), = self.inserts("ordBillMain")
        # 欄位順序依目標資料表（與範本來源順序相反），不依賴位置
        self.assertEqual(names, list(reversed(MAIN_COLS)))
        self.assertEqual(main["BillDate"], 20261029)
        self.assertEqual(main["SumBTaxAmt"], Decimal("100.00"))
        self.assertEqual(main["SumTax"], Decimal("5"))
        self.assertEqual(main["SumAmtATax"], Decimal("105.00"))
        self.assertEqual((main["LocalTotal"], main["LocalTax"]), (Decimal("100.00"), Decimal("5")))
        self.assertEqual((main["BillStatus"], main["TaxType"], main["EndacaseDate"]), (0, 0, 0))
        self.assertEqual((main["SalesMan"], main["Maker"], main["MakerID"]), ("017", "Andy", "017"))
        self.assertEqual((main["Permitter"], main["PermitterID"], main["CustBillNo"]), ("", "", ""))
        self.assertEqual((main["CustomerID"], main["DueTo"], main["CustAddress"]), ("P0175", "月結30天", "台北"))
        self.assertEqual(main["Remark"], "測試")
        # 範本 BillDate 2026-09-01 → 今天相差 +1 個月；EncashDay = 範本單日 → 今天
        self.assertEqual((main["AccMonth"], main["PrepayDay"], main["EncashDay"]), (202611, 20261130, 20261029))
        (_, sub, sub_names), = self.inserts("ordBillSub")
        self.assertEqual(sub_names, list(reversed(SUB_COLS)))
        self.assertEqual((sub["ProdID"], sub["ProdName"], sub["RowNO"], sub["SerNO"], sub["FromRow"]), ("AC-40838B", "電容", 1, 1, 1))
        self.assertEqual((sub["Quantity"], sub["Price"], sub["Amount"], sub["TaxAmt"]), (Decimal("10"), Decimal("10"), Decimal("100.00"), Decimal("5")))
        self.assertEqual((sub["TaxRate"], sub["PreInDate"], sub["QtyRemain"]), (Decimal("0.05"), 20261115, Decimal("10")))
        self.assertEqual((sub["Discount"], sub["IsGift"], sub["NeedDate"], sub["FromNO"], sub["ItemRemark"]), (1, 0, 0, "", ""))
        self.assertEqual(sub["Unit"], "PCS")  # 範本欄位原樣帶入
        # 本機紀錄
        hist = db.list_erp_purchase_orders()
        self.assertEqual((len(hist), hist[0]["status"], hist[0]["bill_no"], hist[0]["client_ip"]), (1, "ok", "PO-20261029001", "1.2.3.4"))
        self.assertTrue(any(l["action"] == "erp_po_created" for l in db.get_activity_logs()))

    def test_requires_confirm(self):
        with self.assertRaises(ep.PoValidationError):
            ep.create_po(REQ)
        self.assertEqual(self.tgt.inserted, [])

    def test_sub_template_fallback_to_main_template_row(self):
        self.src.sub_templates = {}
        ep.create_po({**REQ, "confirm": True})
        self.assertEqual(self.inserts("ordBillSub")[0][1]["Warehouse"], "001")

    def test_no_supplier_template_refused(self):
        self.src.main_template = None
        with self.assertRaises(ep.PoValidationError) as cm:
            ep.create_po({**REQ, "confirm": True})
        self.assertIn("請先在正航開第一張", str(cm.exception))
        self.assertEqual(self.tgt.inserted, [])
        self.assertEqual(db.list_erp_purchase_orders()[0]["status"], "error")

    def test_unknown_product_and_supplier(self):
        self.src.products = {}
        with self.assertRaises(ep.PoValidationError):
            ep.preview_po(REQ)
        with self.assertRaises(ep.PoValidationError):
            ep.preview_po({**REQ, "supplier_id": "NOPE"})

    def test_preview_does_not_write_and_uses_no_lock(self):
        out = ep.preview_po(REQ)
        self.assertEqual(out["bill_no"], "PO-20261029001")
        self.assertEqual(out["target_label"], "公司1（測試）")
        self.assertEqual(out["lines"][0]["product_name"], "電容")
        self.assertEqual(out["totals"]["total"], 105.0)
        self.assertEqual(self.tgt.inserted, [])
        self.assertNotIn("UPDLOCK", self.tgt.max_sql)
        self.assertEqual(self.tgt.commits, 0)
        self.assertTrue(self.tgt.closed)

    def test_missing_extra_column_aborts_without_commit(self):
        self.tgt.columns["ordBillMain"] = MAIN_COLS + ["NewCol"]
        with self.assertRaises(ep.PoSchemaError):
            ep.create_po({**REQ, "confirm": True})
        self.assertEqual((self.tgt.inserted, self.tgt.commits), ([], 0))
        self.tgt.columns["ordBillMain"] = MAIN_COLS[:-1]
        with self.assertRaises(ep.PoSchemaError):
            ep.preview_po(REQ)

    def test_master_specs_order_and_skips(self):
        specs = ep._master_specs("P1", ["A", "B"], "001", ["C1", "", "C1"], "D1", ["017", "017", ""])
        self.assertEqual([(s["table"], s["key"]) for s in specs], [
            ("comCustomer", "P1"), ("comProductClass", "C1"), ("comProduct", "A"), ("comProduct", "B"),
            ("comWareHouse", "001"), ("comDepartment", "D1"), ("comPerson", "017")])
        self.assertEqual([s["required"] for s in specs], [True, False, True, True, True, False, False])
        bare = ep._master_specs("P1", ["A"], "001")
        self.assertEqual([s["table"] for s in bare], ["comCustomer", "comProduct", "comWareHouse"])

    def test_masters_copied_into_test_company_only(self):
        self.tgt.master_ids = {k: set() for k in ("comCustomer", "comProduct", "comWareHouse", "comProductClass", "comDepartment", "comPerson")}
        self.src.products = {"AC-40838B": {"ProdID": "AC-40838B", "ProdName": "電容", "ClassID": "C1"}}
        self.src.main_template = main_tpl(DepartID="D1")
        self.src.master_ids.update(comProductClass={"C1"}, comDepartment={"D1"}, comPerson={"017"})
        self.tgt.columns["ordBillMain"] = list(reversed(MAIN_COLS + ["DepartID"]))
        self.tgt.columns["comProduct"] = ["ProdID", "ProdName", "ClassID"]
        out = ep.preview_po(REQ)
        self.assertEqual([(m["table"], m["key"]) for m in out["masters_to_copy"]], [
            ("comCustomer", "P0175"), ("comProductClass", "C1"), ("comProduct", "AC-40838B"),
            ("comWareHouse", "001"), ("comDepartment", "D1"), ("comPerson", "017")])
        self.assertEqual(self.tgt.inserted, [])
        ep.create_po({**REQ, "confirm": True})
        tables = [i[0] for i in self.tgt.inserted]
        self.assertEqual(tables, ["comCustomer", "comProductClass", "comProduct", "comWareHouse",
                                  "comDepartment", "comPerson", "ordBillMain", "ordBillSub"])
        self.assertEqual(self.tgt.commits, 1)

    def test_optional_masters_missing_in_source_are_skipped(self):
        self.tgt.master_ids = {k: set() for k in ("comCustomer", "comProduct", "comWareHouse", "comProductClass", "comDepartment", "comPerson")}
        self.src.master_rows = {"comProductClass": set(), "comDepartment": set(), "comPerson": set()}
        self.src.master_ids.update(comProductClass=set(), comDepartment=set(), comPerson=set())
        self.src.products = {"AC-40838B": {"ProdID": "AC-40838B", "ProdName": "電容", "ClassID": "C1"}}
        self.src.main_template = main_tpl(DepartID="D1")
        self.tgt.columns["ordBillMain"] = list(reversed(MAIN_COLS + ["DepartID"]))
        self.tgt.columns["comProduct"] = ["ProdID", "ProdName", "ClassID"]
        out = ep.preview_po(REQ)
        self.assertEqual([m["table"] for m in out["masters_to_copy"]], ["comCustomer", "comProduct", "comWareHouse"])
        ep.create_po({**REQ, "confirm": True})
        self.assertEqual([i[0] for i in self.tgt.inserted],
                         ["comCustomer", "comProduct", "comWareHouse", "ordBillMain", "ordBillSub"])

    def test_required_master_missing_in_source_still_fails(self):
        self.tgt.master_ids["comWareHouse"] = set()  # 來源 FakeCursor 的倉庫一律存在 → 改掉來源行為
        orig = FakeCursor.execute

        def execute(cur, sql, params=None):
            if " ".join(sql.split()).startswith("SELECT * FROM comWareHouse"):
                return cur._dicts(None)
            return orig(cur, sql, params)

        with patch.object(FakeCursor, "execute", execute):
            with self.assertRaises(ep.PoValidationError) as cm:
                ep.create_po({**REQ, "confirm": True})
        self.assertIn("來源公司也找不到", str(cm.exception))
        self.assertEqual((self.tgt.inserted, self.tgt.commits), ([], 0))

    def test_copy_masters_refused_for_production_and_source(self):
        with patch.dict(os.environ, {"ERP_SQL_WRITE_USER_CHICOMP03": "w", "ERP_SQL_WRITE_PASSWORD_CHICOMP03": "pw"}):
            self.assertFalse(ep._can_copy_masters("CHIComp03", "CHIComp03"))
            self.assertFalse(ep._can_copy_masters("CHIComp03", "CHIComp01"))
            self.assertTrue(ep._can_copy_masters("CHIComp02", "CHIComp03"))
            self.assertFalse(ep._can_copy_masters("CHIComp02", "CHIComp02"))
            with self.assertRaises(ep.PoValidationError):
                ep._copy_masters(self.src.cursor(), self.tgt.cursor(),
                                 [{"table": "comCustomer", "label": "供應商", "key": "P0175",
                                   "where": "Flag = 2 AND ID = %s", "params": ("P0175",), "required": True}],
                                 "CHIComp03")
        self.assertEqual(self.tgt.inserted, [])

    def test_comp02_target_writes_with_test_login(self):
        self.tgt = make_target("CHIComp02")
        with patch.dict(os.environ, {"ERP_WRITE_TARGET": "CHIComp02"}):
            res = ep.create_po({**REQ, "confirm": True})
        self.assertEqual((res["target"], res["target_label"], res["is_test"]), ("CHIComp02", "公司2（測試）", True))
        self.assertEqual(self.connect_calls, ["CHIComp02"])

    def test_preview_many_sequential_bill_numbers_and_per_item_errors(self):
        self.tgt.max_bill = "PO-20261029004"
        reqs = [REQ, {**REQ, "supplier_id": "NOPE"}, {**REQ, "remark": "第二張"}, {**REQ, "lines": []}]
        out = ep.preview_many(reqs)
        self.assertEqual([r["ok"] for r in out], [True, False, True, False])
        self.assertEqual([r.get("bill_no") for r in out if r["ok"]], ["PO-20261029005", "PO-20261029006"])
        self.assertTrue(all(r["bill_no_estimated"] for r in out if r["ok"]))
        self.assertIn("NOPE", out[1]["error"])
        self.assertEqual(out[0]["lines"][0]["part_number"], "AC-40838B")
        self.assertEqual(out[0]["totals"]["total"], 105.0)
        self.assertEqual(out[0]["missing_masters"], [])
        # 只開一個寫入連線、唯讀（rollback、不 commit、不寫入）
        self.assertEqual(self.connect_calls, ["CHIComp01"])
        self.assertEqual((self.tgt.inserted, self.tgt.commits, self.tgt.rollbacks), ([], 0, 1))
        self.assertTrue(self.tgt.closed)
        self.assertNotIn("UPDLOCK", self.tgt.max_sql)

    def test_preview_many_reports_missing_master_labels(self):
        self.tgt.master_ids["comProduct"] = set()
        out = ep.preview_many([REQ])
        self.assertEqual(out[0]["missing_masters"], ["料號 AC-40838B"])

    def test_preview_many_bill_overflow_is_per_item(self):
        self.tgt.max_bill = "PO-20261029998"
        out = ep.preview_many([REQ, REQ])
        self.assertEqual([r["ok"] for r in out], [True, False])
        self.assertEqual(out[0]["bill_no"], "PO-20261029999")

    def test_preview_many_unavailable(self):
        with patch.dict(os.environ, {"ERP_WRITE_TARGET": "CHIComp03"}):
            with self.assertRaises(ErpNotConfigured):
                ep.preview_many([REQ])

    def test_never_copy_into_comp03(self):
        self.tgt = make_target("CHIComp03")
        self.tgt.master_ids = {"comCustomer": set(), "comProduct": {"AC-40838B"}, "comWareHouse": {"001"}}
        with patch.dict(os.environ, {"ERP_WRITE_TARGET": "CHIComp03", "ERP_SQL_WRITE_USER_CHICOMP03": "w",
                                     "ERP_SQL_WRITE_PASSWORD_CHICOMP03": "pw"}):
            with self.assertRaises(ep.PoValidationError) as cm:
                ep.create_po({**REQ, "confirm": True})
            self.assertIn("不會自動複製", str(cm.exception))
            self.assertEqual((self.tgt.inserted, self.tgt.commits), ([], 0))
            self.assertEqual(self.tgt.rollbacks, 1)
            # 主檔齊全時可正常寫入公司3
            self.tgt.master_ids["comCustomer"] = {"P0175"}
            self.assertEqual(ep.create_po({**REQ, "confirm": True})["target"], "CHIComp03")

    def test_db_name_mismatch_aborts(self):
        self.tgt.db_name = "CHIComp03"
        with self.assertRaises(ep.PoValidationError) as cm:
            ep.create_po({**REQ, "confirm": True})
        self.assertIn("不符", str(cm.exception))
        self.assertEqual((self.tgt.inserted, self.tgt.commits), ([], 0))

    def test_rollback_on_insert_error_hides_secrets(self):
        self.tgt.fail_insert = "ordBillSub"
        with self.assertRaises(ep.ErpError) as cm:
            ep.create_po({**REQ, "confirm": True})
        self.assertNotIn("secret-pw", str(cm.exception))
        self.assertIn("rollback", str(cm.exception))
        self.assertEqual((self.tgt.commits, self.tgt.rollbacks), (0, 1))
        hist = db.list_erp_purchase_orders()
        self.assertEqual((hist[0]["status"], hist[0]["bill_no"]), ("error", ""))
        self.assertNotIn("secret-pw", hist[0]["error"])

    def test_verify_mismatch_rolls_back(self):
        self.tgt.corrupt_verify = True
        with self.assertRaises(ep.PoSchemaError):
            ep.create_po({**REQ, "confirm": True})
        self.assertEqual((self.tgt.commits, self.tgt.rollbacks), (0, 1))

    def test_bill_number_overflow(self):
        self.tgt.max_bill = "PO-20261029999"
        with self.assertRaises(ep.PoValidationError):
            ep.create_po({**REQ, "confirm": True})
        self.assertEqual(self.tgt.commits, 0)


class ApiCase(WriteFlowCase):
    def setUp(self):
        super().setUp()
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)

    def test_edit_auth_protects_post_not_get(self):
        def req(method, path):
            return Request({"type": "http", "method": method, "path": path, "headers": [], "query_string": b""})
        self.assertTrue(request_requires_edit_auth(req("POST", "/api/erp/po/preview")))
        self.assertTrue(request_requires_edit_auth(req("POST", "/api/erp/po/create")))
        self.assertFalse(request_requires_edit_auth(req("GET", "/api/erp/po/config")))

    def test_config_preview_create_history(self):
        cfg = self.client.get("/api/erp/po/config").json()
        self.assertTrue(cfg["available"])
        self.assertNotIn("secret-pw", str(cfg))
        pv = self.client.post("/api/erp/po/preview", json=REQ)
        self.assertEqual(pv.status_code, 200, pv.text)
        self.assertEqual(pv.json()["bill_no"], "PO-20261029001")
        no_confirm = self.client.post("/api/erp/po/create", json=REQ)
        self.assertEqual(no_confirm.status_code, 400)
        ok = self.client.post("/api/erp/po/create", json={**REQ, "confirm": True})
        self.assertEqual(ok.status_code, 200, ok.text)
        self.assertEqual(ok.json()["bill_no"], "PO-20261029001")
        hist = self.client.get("/api/erp/po/history").json()["orders"]
        self.assertEqual(hist[0]["bill_no"], "PO-20261029001")

    def test_errors_map_to_http_codes(self):
        bad = self.client.post("/api/erp/po/preview", json={**REQ, "lines": []})
        self.assertEqual(bad.status_code, 400)
        self.tgt.fail_insert = "ordBillMain"
        fail = self.client.post("/api/erp/po/create", json={**REQ, "confirm": True})
        self.assertEqual(fail.status_code, 503)
        self.assertNotIn("secret-pw", fail.text)
        with patch.dict(os.environ, {"ERP_WRITE_TARGET": "CHIComp03"}):
            na = self.client.post("/api/erp/po/preview", json=REQ)
            self.assertEqual(na.status_code, 503)

    def test_suppliers_and_part_defaults(self):
        calls = []

        class Cur:
            description = None

            def execute(self, sql, params=None):
                calls.append((" ".join(sql.split()), params))
                self.sql = sql

            def fetchall(self):
                return [("P0175", "甲", "甲公司")]

            def fetchone(self):
                if "comProduct" in self.sql:
                    return ("AC-40838B", "電容")
                if "ordBillSub" in self.sql:
                    return ("P0175", Decimal("9.5"), "PO-20260901001", 20260901)
                return ("甲", "甲公司")

        class Conn:
            def cursor(self):
                return Cur()

        @contextmanager
        def session():
            yield Conn()

        with patch.object(ep, "_source_session", session), patch.object(ep, "_suggested_qty", lambda p: 7):
            r = self.client.get("/api/erp/po/suppliers", params={"q": "a_%"})
            self.assertEqual(r.json()["suppliers"][0]["id"], "P0175")
            self.assertEqual(calls[0][1][0], "%a[_][%]%")
            d = self.client.get("/api/erp/po/part-defaults", params={"part": "ac-40838b"}).json()
        self.assertEqual((d["product_name"], d["supplier_id"], d["supplier_name"], d["last_price"], d["suggested_qty"]),
                         ("電容", "P0175", "甲", 9.5, 7))
        self.assertEqual(d["last_bill_date"], "2026-09-01")


class SuggestedQtyTests(unittest.TestCase):
    def test_gap_formula(self):
        from app.services import low_stock
        rows = [{"part_number": "A-1", "threshold": 100, "qty": 30.5, "on_order_qty": 20},
                {"part_number": "B-1", "threshold": 10, "qty": 50, "on_order_qty": 0},
                {"part_number": "C-1", "threshold": None, "qty": 0, "on_order_qty": 0}]
        with patch.object(low_stock, "list_rows", lambda parts=None: rows):
            self.assertEqual(ep._suggested_qty("a-1"), 50)  # ceil(49.5)
            self.assertEqual(ep._suggested_qty("B-1"), 0)
            self.assertIsNone(ep._suggested_qty("C-1"))
            self.assertIsNone(ep._suggested_qty("Z-9"))


class AllocationRecordTests(WriteFlowCase):
    """開採購單：每行為哪些訂單而買的紀錄（僅記錄）。"""

    def setUp(self):
        super().setUp()
        self.conn.execute("INSERT INTO orders(id, code, model, po_number, status) VALUES(7, '10-1', 'T35U', '4500123', 'dispatched')")
        self.conn.execute("INSERT INTO dispatch_sessions(id, order_id, rolled_back_at) VALUES(11, 7, '')")
        self.conn.execute("INSERT INTO dispatch_sessions(id, order_id, rolled_back_at) VALUES(12, 7, '2026-10-01')")

    def test_manual_default_without_allocations(self):
        res = ep.create_po({**REQ, "confirm": True})
        self.assertEqual([(a["kind"], a["qty"], a["order_id"]) for a in res["lines"][0]["allocations"]], [("manual", 10.0, 0)])
        hist = db.list_erp_purchase_orders()
        rows = db.get_po_line_allocations([hist[0]["id"]])
        self.assertEqual([(r["kind"], r["source"], r["qty"], r["part_number"], r["row_no"], r["bill_no"], r["target_db"])
                          for r in rows], [("manual", "manual_po", 10.0, "AC-40838B", 1, "PO-20261029001", "CHIComp01")])
        self.assertIn("allocations", hist[0]["lines_json"])

    def test_explicit_allocations_remainder_is_manual(self):
        line = {**REQ["lines"][0], "allocations": [{"order_id": 7, "qty": 6, "kind": "order"}]}
        res = ep.create_po({**REQ, "lines": [line], "confirm": True})
        allocs = res["lines"][0]["allocations"]
        self.assertEqual([(a["kind"], a["qty"]) for a in allocs], [("order", 6.0), ("manual", 4.0)])
        # 訂單資料與有效發料批次（排除已退回的 12）由伺服器補上
        self.assertEqual((allocs[0]["order_code"], allocs[0]["model"], allocs[0]["customer_po"], allocs[0]["dispatch_session_id"]),
                         ("10-1", "T35U", "4500123", 11))

    def test_allocation_validation(self):
        for allocs in ([{"order_id": 7, "qty": 0}], [{"order_id": 7, "qty": -1}],
                       [{"order_id": 7, "qty": 6}, {"order_id": 8, "qty": 5}], [{"order_id": 7, "qty": 1, "kind": "zzz"}]):
            line = {**REQ["lines"][0], "allocations": allocs}
            with self.assertRaises(ep.PoValidationError):
                ep.normalize_request({**REQ, "lines": [line]})
        self.assertEqual(db.list_erp_purchase_orders(), [])

    def test_failed_write_saves_no_allocations(self):
        self.tgt.fail_insert = "ordBillSub"
        line = {**REQ["lines"][0], "allocations": [{"order_id": 7, "qty": 6}]}
        with self.assertRaises(ep.ErpError):
            ep.create_po({**REQ, "lines": [line], "confirm": True})
        self.assertEqual(db.list_erp_purchase_orders()[0]["status"], "error")
        self.assertEqual(db.get_po_line_allocations(), [])

    def test_preview_includes_allocations_and_saves_nothing(self):
        line = {**REQ["lines"][0], "allocations": [{"order_id": 7, "qty": 10}]}
        out = ep.preview_many([{**REQ, "lines": [line]}])
        self.assertEqual(out[0]["lines"][0]["allocations"][0]["order_code"], "10-1")
        self.assertEqual(db.get_po_line_allocations(), [])

    def test_history_includes_allocations_and_old_rows_empty(self):
        old_id = db.add_erp_purchase_order({"bill_no": "OLD", "status": "ok", "lines_json": "[]"})
        line = {**REQ["lines"][0], "allocations": [{"order_id": 7, "qty": 6}]}
        ep.create_po({**REQ, "lines": [line], "confirm": True})
        hist = {h["bill_no"]: h for h in ep.list_history()}
        self.assertEqual(hist["OLD"]["id"], old_id)
        self.assertEqual(hist["OLD"]["allocations"], [])
        self.assertEqual([(a["row_no"], a["order_code"], a["qty"]) for a in hist["PO-20261029001"]["allocations"] if a["kind"] == "order"],
                         [(1, "10-1", 6.0)])

    def test_history_api(self):
        from main import app
        ep.create_po({**REQ, "confirm": True})
        orders = TestClient(app).get("/api/erp/po/history").json()["orders"]
        self.assertEqual(orders[0]["allocations"][0]["kind"], "manual")


class DbAllocationTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db._CREATE_SQL)

        @contextmanager
        def temp_conn():
            yield self.conn
            self.conn.commit()

        p = patch.object(db, "get_conn", temp_conn)
        p.start()
        self.addCleanup(p.stop)

    def test_save_and_get(self):
        n = db.save_po_line_allocations(5, "B1", "CHIComp01", [
            {"row_no": 1, "part_number": " ab-1 ", "order_id": 3, "order_code": "10-1", "qty": 4, "kind": "order",
             "source": "shortage_po", "dispatch_session_id": 9},
            {"row_no": 1, "part_number": "AB-1", "qty": 1, "kind": "extra", "source": "shortage_po"},
        ])
        self.assertEqual(n, 2)
        db.save_po_line_allocations(6, "B2", "CHIComp01", [{"row_no": 1, "part_number": "X", "qty": 2}])
        rows = db.get_po_line_allocations([5])
        self.assertEqual([(r["part_number"], r["order_id"], r["qty"], r["kind"], r["dispatch_session_id"]) for r in rows],
                         [("AB-1", 3, 4.0, "order", 9), ("AB-1", 0, 1.0, "extra", 0)])
        self.assertEqual(db.get_po_line_allocations(bill_no="B2")[0]["kind"], "manual")
        self.assertEqual(db.get_po_line_allocations([]), [])
        self.assertEqual(len(db.get_po_line_allocations()), 3)
        self.assertEqual(db.save_po_line_allocations(1, "B", "T", []), 0)

    def test_latest_active_session(self):
        for sid, oid, rb in ((1, 7, ""), (2, 7, ""), (3, 7, "2026-01-01"), (4, 8, "2026-01-01")):
            self.conn.execute("INSERT INTO dispatch_sessions(id, order_id, rolled_back_at) VALUES(?,?,?)", (sid, oid, rb))
        self.assertEqual(db.get_latest_active_dispatch_session_ids([7, 8, 9]), {7: 2})


if __name__ == "__main__":
    unittest.main()
