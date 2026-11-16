"""v2026.11.13.6：GET /api/erp/po/print-info（採購單列印補資料，給 t357）。用假的正航，不連 DB。

資料照 2026-11-16 唯讀查 CHIComp03 的深福 W0083 / 017 / EC-20136A、UC-A0013B。
"""
from __future__ import annotations

import os
import re
import sqlite3
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.requests import Request

import app.database as db
from app.services import erp_po_print_info as pi, erp_purchase as ep, erp_stock
from app.services.edit_auth import request_requires_edit_auth

SHENFU = {"Flag": 2, "ID": "W0083", "ShortName": "深福", "FullName": "深福公司", "LinkMan": "",
          "Telephone1": "13728317003", "Telephone2": "", "Telephone3": "002-86-7698-388-5068",
          "MobileTel": "13900000000", "FaxNo": "002-86-7698-388-5038", "GatherOther": "", "Password": "secret"}
SHENFU_LINK = "002-86-7698-388-5068"


def bill_main(no, date, **kw):
    row = {"Flag": 4, "BillNO": no, "BillDate": date, "CustomerID": "W0083", "SalesMan": "017", "LinkMan": "",
           "LinkTelephone": SHENFU_LINK, "FaxNo": "002-86-7698-388-5038", "GatherStyle": 1, "GatherDelay": 30,
           "GatherOther": ""}
    row.update(kw)
    return row


class FakeErp:
    """只回答 print-info 會用到的 SELECT；任何非 SELECT 都記下來讓測試抓。"""

    def __init__(self):
        self.customers = {"W0083": dict(SHENFU)}
        self.mains = [bill_main("PO-20260723002", 20260723), bill_main("PO-20260601001", 20260601, LinkTelephone="舊電話")]
        self.subs = [{"Flag": 4, "BillNO": "PO-20260723002", "RowNO": 1, "ProdID": "EC-20136A", "Unit": "PCS"}]
        self.products = {"EC-20136A": "PCS", "UC-A0013B": "PCS"}
        self.persons = {"017": "李韋頡"}
        self.sql = []
        self.commits = 0

    def cursor(self):
        return FakeCur(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        pass


class FakeCur:
    def __init__(self, erp):
        self.e = erp
        self.rows, self.description = [], None

    def _dicts(self, dicts, sql):
        # 照 SELECT 的欄位清單回欄，查了不存在的欄就炸（像真的正航）
        cols = re.match(r"\s*SELECT\s+(?:TOP\s+\d+\s+)?(.+?)\s+FROM\s", sql, re.I | re.S).group(1)
        names = [c.strip() for c in cols.split(",")]
        for d in dicts:
            missing = [n for n in names if n not in d]
            if missing:
                raise AssertionError(f"查了不存在的欄 {missing}：{sql}")
        self.description = [(n,) for n in names]
        self.rows = [tuple(d.get(n) for n in names) for d in dicts]

    def _tuples(self, rows, names):
        self.description = [(n,) for n in names]
        self.rows = [tuple(r) for r in rows]

    def execute(self, sql, params=()):
        e = self.e
        e.sql.append(sql)
        if not sql.lstrip().upper().startswith("SELECT"):
            raise AssertionError(f"print-info 只能 SELECT：{sql}")
        if "FROM comCustomer" in sql:
            c = e.customers.get(params[0])
            self._dicts([c] if c else [], sql)
        elif "FROM ordBillMain" in sql and "BillNO = %s" in sql:
            self._dicts([m for m in e.mains if m["BillNO"] == params[0] and m["CustomerID"] == params[1]], sql)
        elif "FROM ordBillMain" in sql:
            rows = sorted((m for m in e.mains if m["CustomerID"] == params[0]),
                          key=lambda m: (m["BillDate"], m["BillNO"]), reverse=True)
            self._dicts(rows[:1], sql)
        elif "FROM ordBillSub" in sql:
            self._tuples([(s["ProdID"], s["Unit"]) for s in e.subs if s["BillNO"] == params[0]], ["ProdID", "Unit"])
        elif "FROM comProduct" in sql:
            u = e.products.get(params[0])
            self._tuples([(u,)] if u is not None else [], ["Unit"])
        elif "FROM comPerson" in sql:
            n = e.persons.get(params[0])
            self._tuples([(n,)] if n is not None else [], ["PersonName"])
        else:
            raise AssertionError(f"沒預期的查詢：{sql}")

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class PrintInfoCase(unittest.TestCase):
    def setUp(self):
        clean = {k: v for k, v in os.environ.items()
                 if not k.startswith(("ERP_SQL_WRITE", "ERP_SQL_TEST", "ERP_WRITE", "ERP_PO", "ERP_ALLOW_PROD"))}
        p = patch.dict(os.environ, clean, clear=True)
        p.start()
        self.addCleanup(p.stop)

        self.sqlite = sqlite3.connect(":memory:", check_same_thread=False)
        self.sqlite.row_factory = sqlite3.Row
        self.sqlite.executescript(db._CREATE_SQL)

        @contextmanager
        def temp_conn():
            yield self.sqlite
            self.sqlite.commit()

        self.erp = FakeErp()

        @contextmanager
        def source_session():
            yield self.erp

        def no_write(creds):
            raise AssertionError("print-info 不可開寫入連線")

        for p in (
            patch.object(db, "get_conn", temp_conn),
            patch.object(ep, "_source_session", source_session),
            patch.object(ep, "_connect_write", no_write),
            patch.object(erp_stock, "get_settings", lambda: {
                "server": "s", "port": 1433, "database": "CHIComp03", "ware_id": "001",
                "timeout": 5.0, "encryption": "off", "tds_version": "7.3"}),
        ):
            p.start()
            self.addCleanup(p.stop)
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)

    def get(self, **params):
        r = self.client.get("/api/erp/po/print-info", params=params)
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    def tearDown(self):
        for sql in self.erp.sql:
            self.assertTrue(sql.lstrip().upper().startswith("SELECT"), sql)
        self.assertEqual(self.erp.commits, 0)


class PrintInfoTests(PrintInfoCase):
    def test_with_bill_uses_bill_phone(self):
        self.erp.mains.append(bill_main("PO-20261116001", 20261116, LinkTelephone="單據電話", LinkMan="王先生",
                                        FaxNo="單據傳真"))
        d = self.get(supplier="W0083", salesman="017", bill="PO-20260601001", parts="EC-20136A")
        self.assertEqual(d["supplier"]["phone"], "舊電話")  # 用指定那張，不是最近一張
        self.assertEqual(d["supplier"]["fax"], "002-86-7698-388-5038")

    def test_shenfu_without_bill_uses_latest_link_telephone_not_telephone1(self):
        d = self.get(supplier="W0083", salesman="017", parts="EC-20136A,UC-A0013B")
        self.assertEqual(d, {
            "supplier": {"id": "W0083", "contact": "", "phone": SHENFU_LINK, "fax": "002-86-7698-388-5038",
                         "pay_terms": "次月"},
            "buyer_name": "李韋頡",
            "units": {"EC-20136A": "PCS", "UC-A0013B": "PCS"},
        })
        self.assertNotIn("13728317003", str(d))
        self.assertTrue(any("ORDER BY BillDate DESC, BillNO DESC" in s for s in self.erp.sql))

    def test_no_link_telephone_leaves_phone_blank(self):
        for m in self.erp.mains:
            m["LinkTelephone"] = ""
        d = self.get(supplier="W0083")
        self.assertEqual(d["supplier"]["phone"], "")  # 不用 Telephone1 / Telephone3 / MobileTel

    def test_no_po_at_all_phone_blank_fax_from_master(self):
        self.erp.mains = []
        d = self.get(supplier="W0083")
        self.assertEqual(d["supplier"]["phone"], "")
        self.assertEqual(d["supplier"]["fax"], "002-86-7698-388-5038")

    def test_pay_terms_name_only(self):
        d = self.get(supplier="W0083")
        self.assertEqual(d["supplier"]["pay_terms"], "次月")
        self.assertNotIn("30", d["supplier"]["pay_terms"])
        self.assertEqual(pi.pay_terms_name(2, "x"), "月結")
        self.assertEqual(pi.pay_terms_name(0, "貨到付款"), "貨到付款")
        self.assertEqual(pi.pay_terms_name(5, ""), "")

    def test_contact_and_fax_fall_back_to_master_when_blank(self):
        self.erp.customers["W0083"]["LinkMan"] = "陳小姐"
        self.erp.mains[0]["FaxNo"] = ""
        d = self.get(supplier="W0083")
        self.assertEqual((d["supplier"]["contact"], d["supplier"]["fax"]), ("陳小姐", "002-86-7698-388-5038"))
        self.erp.mains[0]["LinkMan"] = "單據聯絡人"
        self.assertEqual(self.get(supplier="W0083")["supplier"]["contact"], "單據聯絡人")

    def test_unknown_person_buyer_name_empty(self):
        self.assertEqual(self.get(supplier="W0083", salesman="999")["buyer_name"], "")

    def test_units_from_comproduct_and_bill(self):
        d = self.get(supplier="W0083", parts="EC-20136A,UC-A0013B")
        self.assertEqual(d["units"], {"EC-20136A": "PCS", "UC-A0013B": "PCS"})
        self.assertFalse(any("ordBillSub" in s for s in self.erp.sql))  # 沒帶單號不看明細
        self.erp.subs[0]["Unit"] = "KPCS"
        self.erp.sql.clear()
        d = self.get(supplier="W0083", bill="PO-20260723002", parts="EC-20136A,UC-A0013B,NOPE-1")
        self.assertEqual(d["units"], {"EC-20136A": "KPCS", "UC-A0013B": "PCS"})

    def test_unknown_supplier_same_shape(self):
        d = self.get(supplier="ZZZ", salesman="017", parts="EC-20136A")
        self.assertEqual(d, {"supplier": {"id": "ZZZ", "contact": "", "phone": "", "fax": "", "pay_terms": ""},
                             "buyer_name": "", "units": {}})

    def test_supplier_required_and_company_checked(self):
        self.assertEqual(self.client.get("/api/erp/po/print-info").status_code, 400)
        r = self.client.get("/api/erp/po/print-info", params={"supplier": "W0083", "company": "CHIComp02"})
        self.assertEqual(r.status_code, 400)

    def test_select_only_no_commit(self):
        self.get(supplier="W0083", salesman="017", bill="PO-20260723002", parts="EC-20136A")
        self.assertTrue(self.erp.sql)
        for sql in self.erp.sql:
            self.assertRegex(sql.lstrip(), re.compile(r"^SELECT\b", re.I))
            self.assertNotRegex(sql, re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|EXEC)\b", re.I))
        self.assertEqual(self.erp.commits, 0)

    def test_selects_only_needed_columns(self):
        self.get(supplier="W0083", salesman="017", bill="PO-20260723002", parts="EC-20136A,UC-A0013B")
        for sql in self.erp.sql:
            self.assertNotRegex(sql, r"SELECT\s+(TOP\s+\d+\s+)?\*", sql)
        cust = [s for s in self.erp.sql if "FROM comCustomer" in s]
        self.assertEqual(cust, ["SELECT ID, LinkMan, FaxNo, GatherOther FROM comCustomer WHERE Flag = 2 AND ID = %s"])
        mains = [s for s in self.erp.sql if "FROM ordBillMain" in s]
        self.assertTrue(mains)
        for sql in mains:
            self.assertIn("SELECT BillNO, CustomerID, SalesMan, LinkMan, LinkTelephone, FaxNo, GatherStyle, "
                          "GatherDelay, GatherOther FROM ordBillMain", sql.replace("TOP 1 ", ""))

    def test_no_login_required(self):
        req = Request({"type": "http", "method": "GET", "path": "/api/erp/po/print-info", "headers": [],
                       "query_string": b""})
        self.assertFalse(request_requires_edit_auth(req))


if __name__ == "__main__":
    unittest.main()
