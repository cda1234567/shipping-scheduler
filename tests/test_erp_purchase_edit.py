"""v2026.11.13.2：修改正航舊採購單（搜尋 / 載入 / 預覽 / 寫入）。全部用假的正航（不連 DB）。"""
from __future__ import annotations

import copy
import os
import re
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.requests import Request

import app.database as db
from app.services import erp_purchase as ep, erp_purchase_edit as ee, erp_stock
from app.services.edit_auth import request_requires_edit_auth

MAIN_COLS = [
    "Flag", "BillNO", "BillDate", "CustomerID", "DueTo", "CustAddress", "CurrID", "ExchRate", "Remark", "SalesMan",
    "TaxType", "SumQty", "SumBTaxAmt", "SumTax", "SumAmtATax", "LocalTotal", "LocalTax", "BillStatus", "EndacaseDate",
    "Maker", "MakerID", "Permitter", "PermitterID", "IsTrans", "MergeOutState", "CustBillNo", "AccMonth", "PrepayDay",
    "EncashDay",
]
SUB_COLS = [
    "Flag", "BillNO", "BillDate", "RowNO", "SerNO", "FromRow", "FromNO", "ProdID", "ProdName", "Quantity", "Price",
    "Amount", "TaxRate", "TaxAmt", "PreInDate", "QtyRemain", "MlAmount", "MLTaxAmt", "sPrice", "sQuantity", "EQuantity",
    "OutQty", "BClearTranQty", "UnValidQty", "Discount", "IsGift", "ItemRemark", "NeedDate", "Unit", "Warehouse",
]
ENV = {"ERP_SQL_TEST_USER": "tester", "ERP_SQL_TEST_PASSWORD": "secret-pw", "ERP_WRITE_TARGET": "CHIComp01"}


def main_row(bill_no="PO-20261101001", **kw):
    row = {c: 0 for c in MAIN_COLS}
    row.update(Flag=4, BillNO=bill_no, BillDate=20261101, CustomerID="P0175", DueTo="月結30天", CustAddress="台北",
               CurrID="NTD", ExchRate=Decimal("1"), Remark="原備註", SalesMan="017", TaxType=0,
               Maker="Andy", MakerID="017", Permitter="主管", PermitterID="M01", IsTrans=1, CustBillNo="CB",
               AccMonth=202612, PrepayDay=20261231, EncashDay=20261101)
    row.update(kw)
    return row


def sub_row(bill_no, row_no, part, qty, price, name="品名", remain=None, **kw):
    qty, price = Decimal(str(qty)), Decimal(str(price))
    amount = (qty * price).quantize(Decimal("0.01"))
    tax = (amount * Decimal("0.05")).quantize(Decimal("1"))
    row = {c: 0 for c in SUB_COLS}
    row.update(Flag=4, BillNO=bill_no, BillDate=20261101, RowNO=row_no, SerNO=row_no, FromRow=row_no, FromNO="",
               ProdID=part, ProdName=name, Quantity=qty, Price=price, Amount=amount, TaxRate=Decimal("0.05"),
               TaxAmt=tax, PreInDate=20261120, QtyRemain=qty if remain is None else Decimal(str(remain)),
               MlAmount=amount, MLTaxAmt=tax, sPrice=price, sQuantity=qty, EQuantity=qty, Discount=1,
               ItemRemark="", Unit="PCS", Warehouse="001")
    row.update(kw)
    return row


def _like(pattern, value):
    text = pattern.strip("%").replace("[[]", "[").replace("[%]", "%").replace("[_]", "_")
    return text.lower() in str(value or "").lower()


class FakeErp:
    """有狀態的假正航：ordBillMain / ordBillSub 可 SELECT / UPDATE / DELETE / INSERT，commit / rollback 生效。"""

    def __init__(self, name):
        self.db_name = name
        self.state = {"ordBillMain": [], "ordBillSub": []}
        self.saved = copy.deepcopy(self.state)
        self.customers = {"P0175": {"Flag": 2, "ID": "P0175", "ShortName": "甲", "FullName": "甲公司"},
                          "P0200": {"Flag": 2, "ID": "P0200", "ShortName": "", "FullName": "乙公司"}}
        self.tax_kind = {"P0175": 0, "P0200": 4}
        self.products = {p: {"ProdID": p, "ProdName": f"{p} 名"} for p in ("AC-1", "AC-2", "AC-3", "BB-9", "CS-1-TAB")}
        self.rates = {"USD": (Decimal("31.5"), 3)}
        self.columns = {"ordBillMain": list(reversed(MAIN_COLS)), "ordBillSub": list(reversed(SUB_COLS))}
        self.log = []
        self.commits = self.rollbacks = 0
        self.corrupt_readback = False

    # ── 交易 ──
    def cursor(self):
        return FakeCur(self)

    def commit(self):
        self.commits += 1
        self.saved = copy.deepcopy(self.state)

    def rollback(self):
        self.rollbacks += 1
        self.state = copy.deepcopy(self.saved)

    def close(self):
        pass

    def seed(self, main, subs):
        self.state["ordBillMain"].append(main)
        self.state["ordBillSub"].extend(subs)
        self.saved = copy.deepcopy(self.state)

    def main(self, bill_no):
        return next((m for m in self.saved["ordBillMain"] if m["BillNO"] == bill_no), None)

    def subs(self, bill_no):
        return sorted((s for s in self.saved["ordBillSub"] if s["BillNO"] == bill_no), key=lambda s: s["RowNO"])

    def writes(self):
        return [s for s, _ in self.log if s.startswith(("UPDATE", "INSERT", "DELETE"))]


class FakeCur:
    def __init__(self, conn):
        self.c = conn
        self.rows, self.description, self.rowcount = [], None, -1

    def _set(self, rows, names):
        self.rows = [tuple(r) for r in rows]
        self.description = [(n,) for n in names]

    def _dicts(self, dicts, names=None):
        if not dicts:
            return self._set([], names or ["x"])
        names = names or list(dicts[0].keys())
        self._set([[d.get(n) for n in names] for d in dicts], names)

    def _where(self, table, cond, params):
        cols = [c.strip("[] ") for c in re.findall(r"\[?(\w+)\]? = %s", cond)]
        return [r for r in self.c.state[table] if all(str(r.get(c)) == str(v) for c, v in zip(cols, params))]

    def execute(self, sql, params=()):
        c = self.c
        s = " ".join(sql.split())
        c.log.append((s, params))
        params = tuple(params or ())
        if s == "SELECT DB_NAME()":
            return self._set([(c.db_name,)], ["n"])
        if "sys.columns" in s:
            return self._set([(n, 0, 0) for n in c.columns[params[0].split(".")[1]]], ["name", "i", "c"])
        m = re.match(r"UPDATE dbo\.(\w+) SET (.*) WHERE (.*)$", s)
        if m:
            table, sets, cond = m.groups()
            set_cols = [x.split("]")[0].strip("[") for x in sets.split(", ")]
            n = len(set_cols)
            hits = self._where(table, cond, params[n:])
            for r in hits:
                for col, v in zip(set_cols, params[:n]):
                    assert col in r, col
                    r[col] = v
            self.rowcount = len(hits)
            return
        m = re.match(r"DELETE FROM dbo\.(\w+) WHERE (.*)$", s)
        if m:
            hits = self._where(m.group(1), m.group(2), params)
            c.state[m.group(1)] = [r for r in c.state[m.group(1)] if r not in hits]
            self.rowcount = len(hits)
            return
        m = re.match(r"INSERT INTO dbo\.(\w+) \((.*?)\) VALUES", s)
        if m:
            names = m.group(2).replace("[", "").replace("]", "").split(", ")
            c.state[m.group(1)].append(dict(zip(names, params)))
            self.rowcount = 1
            return
        if s.startswith("SELECT TOP 30 m.BillNO"):
            out = []
            for mm in sorted(c.state["ordBillMain"], key=lambda r: (r["BillDate"], r["BillNO"]), reverse=True):
                cust = c.customers.get(mm["CustomerID"], {})
                if mm["Flag"] != 4:
                    continue
                if params:  # 有 q：表頭欄位或 EXISTS 明細料號 / 品名（排除 -TAB 列）
                    assert "EXISTS (SELECT 1 FROM ordBillSub s" in s and params[6] == "%-TAB", s
                    head = any(_like(params[0], v) for v in (mm["BillNO"], mm["CustomerID"], cust.get("ShortName"), cust.get("FullName")))
                    line = any(sb["Flag"] == 4 and sb["BillNO"] == mm["BillNO"]
                               and (_like(params[4], sb["ProdID"]) or _like(params[5], sb["ProdName"]))
                               and not str(sb["ProdID"]).rstrip().upper().endswith("-TAB")
                               for sb in c.state["ordBillSub"])
                    if not (head or line):
                        continue
                else:
                    assert "LIKE" not in s, s
                out.append((mm["BillNO"], mm["BillDate"], mm["CustomerID"], cust.get("ShortName"), cust.get("FullName"),
                            mm["SumBTaxAmt"], mm["SumAmtATax"], mm["CurrID"], mm["BillStatus"], mm["EndacaseDate"]))
            return self._set(out[:30], ["a"] * 10)
        m = re.match(r"SELECT \* FROM (ordBillMain|ordBillSub)(?: WITH \(UPDLOCK, HOLDLOCK\))? WHERE Flag = 4 AND BillNO = %s", s)
        if m:
            rows = sorted(self._where(m.group(1), "BillNO = %s", params), key=lambda r: r.get("RowNO", 0))
            return self._dicts([dict(r) for r in rows], c.columns[m.group(1)][::-1])
        if s.startswith("SELECT BillNO, CustomerID, SumQty"):
            names = ["BillNO", "CustomerID", "SumQty", "SumBTaxAmt", "SumTax", "SumAmtATax", "LocalTotal", "LocalTax",
                     "BillStatus", "EndacaseDate"]
            return self._dicts(self._where("ordBillMain", "BillNO = %s", params), names)
        if s.startswith("SELECT RowNO, ProdID, Quantity"):
            names = ["RowNO", "ProdID", "Quantity", "Price", "Amount", "TaxAmt", "PreInDate", "QtyRemain"]
            rows = [dict(r) for r in sorted(self._where("ordBillSub", "BillNO = %s", params), key=lambda r: r["RowNO"])]
            if c.corrupt_readback and rows:
                rows[0]["Amount"] = Decimal("999999")
            return self._dicts(rows, names)
        if s.startswith("SELECT ShortName, FullName FROM comCustomer"):
            cust = c.customers.get(params[0])
            return self._set([(cust["ShortName"], cust["FullName"])] if cust else [], ["s", "f"])
        if s.startswith("SELECT * FROM comCustomer"):
            cust = c.customers.get(params[0])
            return self._dicts([cust] if cust else [])
        if s.startswith("SELECT * FROM comProduct"):
            p = c.products.get(params[0])
            return self._dicts([p] if p else [])
        if s.startswith("SELECT TOP 1 * FROM ordBillSub WHERE Flag = 4 AND ProdID"):
            rows = sorted(self._where("ordBillSub", "ProdID = %s", params),
                          key=lambda r: (r["BillDate"], r["BillNO"], r["RowNO"]), reverse=True)
            return self._dicts([dict(rows[0])] if rows else [], c.columns["ordBillSub"][::-1])
        if s.startswith("SELECT CurrencyID, BuyInRate"):
            r = c.rates.get(params[0])
            return self._dicts([{"CurrencyID": params[0], "BuyInRate": r[0], "FixedRate": 0, "AmountDeciDigits": r[1]}] if r else [])
        if s.startswith("SELECT TaxKind FROM comCustTrade"):
            return self._dicts([{"TaxKind": c.tax_kind.get(params[0], 0)}])
        raise AssertionError(f"unexpected SQL: {s}")

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


BILL = "PO-20261101001"


def seed_open_bill(erp, bill_no=BILL, extra_rows=()):
    subs = [sub_row(bill_no, 1, "AC-1", 100, "1.5"), sub_row(bill_no, 2, "AC-2", 10, "3"), *extra_rows]
    # 同開新單的稅額分攤（表頭 round(Σ未稅 × 5%)，尾差由第 1 行吸收）
    money, _ = ep.compute_money([{"qty": s["Quantity"], "price": s["Price"]} for s in subs])
    for s, m in zip(subs, money):
        s.update(Amount=m["amount"], TaxAmt=m["tax_amt"], MlAmount=m["amount"], MLTaxAmt=m["tax_amt"])
    amount = sum(s["Amount"] for s in subs)
    tax = sum(s["TaxAmt"] for s in subs)
    erp.seed(main_row(bill_no, SumQty=sum(s["Quantity"] for s in subs), SumBTaxAmt=amount, SumTax=tax,
                      SumAmtATax=amount + tax, LocalTotal=amount, LocalTax=tax), subs)


def loaded_of(data):
    return {"bill_status": data["bill_status"], "endacase_date": data["endacase_date"],
            "rows": [{"row_no": l["row_no"], "quantity": l["quantity"], "qty_remain": l["qty_remain"]} for l in data["lines"]]}


def edit_body(data, lines=None, **kw):
    lines = lines if lines is not None else [
        {"row_no": l["row_no"], "part_number": l["part_number"], "qty": l["qty"], "price": l["price"],
         "pre_in_date": l["pre_in_date"]} for l in data["lines"]]
    return {"bill_no": data["bill_no"], "company": data["company"], "supplier_id": data["supplier"]["id"],
            "remark": data["remark"], "salesman": data["salesman"], "currency": data["currency"],
            "tax_mode": data["tax_mode"], "lines": lines, "loaded": loaded_of(data), **kw}


class EditCase(unittest.TestCase):
    extra_env: dict = {}

    def setUp(self):
        clean = {k: v for k, v in os.environ.items()
                 if not k.startswith(("ERP_SQL_WRITE", "ERP_SQL_TEST", "ERP_WRITE", "ERP_PO", "ERP_ALLOW_PROD"))}
        clean.update(ENV)
        clean.update(self.extra_env)
        p = patch.dict(os.environ, clean, clear=True)
        p.start()
        self.addCleanup(p.stop)

        self.sqlite = sqlite3.connect(":memory:", check_same_thread=False)
        self.sqlite.row_factory = sqlite3.Row
        self.sqlite.executescript(db._CREATE_SQL)

        @contextmanager
        def temp_conn():
            try:
                yield self.sqlite
                self.sqlite.commit()
            except Exception:
                self.sqlite.rollback()
                raise

        self.erps = {"CHIComp03": FakeErp("CHIComp03"), "CHIComp13": FakeErp("CHIComp13")}
        self.write_connects = []

        @contextmanager
        def source_session():
            yield self.erps["CHIComp03"]

        def connect_write(creds):
            self.write_connects.append(creds["database"])
            return self.erps[creds["database"]]

        for p in (
            patch.object(db, "get_conn", temp_conn),
            patch.object(ep, "_source_session", source_session),
            patch.object(ep, "_connect_write", connect_write),
            patch.object(erp_stock, "get_settings", lambda: {
                "server": "s", "port": 1433, "database": "CHIComp03", "ware_id": "001",
                "timeout": 5.0, "encryption": "off", "tds_version": "7.3"}),
            patch.object(ep, "local_now", lambda: datetime(2026, 11, 13, 10, 0)),
        ):
            p.start()
            self.addCleanup(p.stop)
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)

    @property
    def c13(self):
        return self.erps["CHIComp13"]

    @property
    def c03(self):
        return self.erps["CHIComp03"]


class ConfigTests(EditCase):
    def test_default_edit_target_is_comp13_and_separate_from_create(self):
        cfg = self.client.get("/api/erp/po/config").json()
        self.assertEqual((cfg["target"], cfg["label"]), ("CHIComp01", "公司1（測試）"))  # 開新單不受影響
        self.assertEqual((cfg["edit_target"], cfg["edit_label"], cfg["edit_is_test"], cfg["edit_available"]),
                         ("CHIComp13", "公司13（測試）", True, True))
        self.assertFalse(cfg["edit_prod_allowed"])
        self.assertEqual([x["db"] for x in cfg["edit_companies"]], ["CHIComp03", "CHIComp13"])
        self.assertIn("正式公司改單尚未開放", cfg["edit_reason"])
        self.assertNotIn("secret-pw", str(cfg))
        self.assertNotIn("tester", str(cfg))

    def test_prod_edit_needs_all_four_switches(self):
        keys = {"ERP_PO_EDIT_TARGET": "CHIComp03", "ERP_ALLOW_PROD_PO_EDIT_CHICOMP03": "1",
                "ERP_ALLOW_PROD_WRITE_CHICOMP03": "1", "ERP_ALLOW_PROD_PO_CHICOMP03": "1"}
        for missing in keys:
            env = {k: v for k, v in keys.items() if k != missing}
            with patch.dict(os.environ, env):
                if missing == "ERP_PO_EDIT_TARGET":
                    self.assertEqual(ee.get_edit_target(), "CHIComp13", missing)  # 沒指定 → 仍是公司13
                    continue
                with self.assertRaises(erp_stock.ErpNotConfigured, msg=missing):
                    ee.get_edit_target()
                cfg = self.client.get("/api/erp/po/config").json()
                self.assertFalse(cfg["edit_available"], missing)
                self.assertIn("尚未開放修改採購單", cfg["edit_reason"])
                self.assertIn(ee.write_block_reason("CHIComp03"), cfg["edit_reason"])
        with patch.dict(os.environ, {**keys, "ERP_SQL_WRITE_USER_CHICOMP03": "u", "ERP_SQL_WRITE_PASSWORD_CHICOMP03": "p"}):
            self.assertEqual(ee.get_edit_target(), "CHIComp03")
            self.assertEqual(ee.write_block_reason("CHIComp03"), "")

    def test_prod_flags_off_cannot_write_comp03(self):
        seed_open_bill(self.c03)
        with patch.dict(os.environ, {"ERP_PO_EDIT_TARGET": "CHIComp03", "ERP_ALLOW_PROD_WRITE_CHICOMP03": "1",
                                     "ERP_ALLOW_PROD_PO_CHICOMP03": "1",
                                     "ERP_SQL_WRITE_USER_CHICOMP03": "u", "ERP_SQL_WRITE_PASSWORD_CHICOMP03": "p"}):
            data = self.client.get("/api/erp/po/bill", params={"no": BILL}).json()
            self.assertTrue(data["editable"])
            self.assertIn("尚未開放修改採購單", data["write_block_reason"])
            r = self.client.post("/api/erp/po/update", json={**edit_body(data), "confirm": True})
        self.assertEqual(r.status_code, 400, r.text)
        self.assertIn("ERP_ALLOW_PROD_PO_EDIT_CHICOMP03", r.json()["detail"])
        self.assertEqual(self.write_connects, [])
        self.assertEqual(self.c03.writes(), [])


class SearchLoadTests(EditCase):
    def test_search_like_escape_top30_newest_first_and_status(self):
        seed_open_bill(self.c03, "PO-20261001001")
        seed_open_bill(self.c03, "PO-20261102001")
        self.c03.seed(main_row("PO-20260901001", CustomerID="P0200", BillStatus=1, EndacaseDate=20261005), [])
        r = self.client.get("/api/erp/po/bills", params={"q": "PO-2026"})
        self.assertEqual(r.status_code, 200, r.text)
        bills = r.json()["bills"]
        self.assertEqual([b["bill_no"] for b in bills], ["PO-20261102001", "PO-20261001001", "PO-20260901001"])
        self.assertEqual((bills[0]["status"], bills[2]["status"], bills[2]["supplier_name"]), ("未結", "已結", "乙公司"))
        self.assertEqual(r.json()["company"], "CHIComp03")
        sql, params = next(x for x in self.c03.log if x[0].startswith("SELECT TOP 30 m.BillNO"))
        self.assertIn("WHERE m.Flag = 4 AND (m.BillNO LIKE %s OR m.CustomerID LIKE %s OR c.ShortName LIKE %s OR c.FullName LIKE %s"
                      " OR EXISTS (SELECT 1 FROM ordBillSub s WHERE s.Flag = 4 AND s.BillNO = m.BillNO"
                      " AND (s.ProdID LIKE %s OR s.ProdName LIKE %s) AND UPPER(RTRIM(s.ProdID)) NOT LIKE %s))", sql)
        self.assertIn("ORDER BY m.BillDate DESC, m.BillNO DESC", sql)
        self.client.get("/api/erp/po/bills", params={"q": "a_%["})
        self.assertEqual(self.c03.log[-1][1], ("%a[_][%][[]%",) * 6 + ("%-TAB",))
        self.assertEqual(self.client.get("/api/erp/po/bills", params={"q": "乙"}).json()["bills"][0]["bill_no"], "PO-20260901001")
        self.assertEqual([b["bill_no"] for b in self.client.get("/api/erp/po/bills", params={"q": "P0175"}).json()["bills"]],
                         ["PO-20261102001", "PO-20261001001"])

    def test_search_by_part_or_name_dedup(self):
        self.c03.seed(main_row("PO-A"), [sub_row("PO-A", 1, "AC-1", 10, 1, name="電容"), sub_row("PO-A", 2, "AC-1", 5, 1, name="電容"),
                                         sub_row("PO-A", 3, "AC-12", 5, 1, name="電容 X")])
        self.c03.seed(main_row("PO-B", BillDate=20261102), [sub_row("PO-B", 1, "BB-9", 10, 1, name="電阻")])
        self.c03.seed(main_row("PO-C", BillDate=20261103, CustomerID="P0200", BillStatus=1), [sub_row("PO-C", 1, "XAC-1Y", 1, 1)])
        bills = lambda q: [b["bill_no"] for b in self.client.get("/api/erp/po/bills", params={"q": q}).json()["bills"]]
        self.assertEqual(bills("AC-1"), ["PO-C", "PO-A"])  # 部分符合、已結也列、同單多列只一次
        self.assertEqual(bills("電容"), ["PO-A"])
        self.assertEqual(bills("bb-9"), ["PO-B"])
        d = self.client.get("/api/erp/po/bills", params={"q": "電阻"}).json()["bills"][0]
        self.assertEqual(set(d), {"bill_no", "bill_date", "supplier_id", "supplier_name", "sum_amount", "total", "currency",
                                  "closed", "status"})

    def test_search_tab_line_alone_does_not_match_but_bill_no_does(self):
        self.c03.seed(main_row("PO-T"), [sub_row("PO-T", 1, "AC-1", 10, 1, name="一般"),
                                         sub_row("PO-T", 2, "CS-1-tab ", 5, 0, name="客供件")])
        bills = lambda q: [b["bill_no"] for b in self.client.get("/api/erp/po/bills", params={"q": q}).json()["bills"]]
        self.assertEqual(bills("CS-1"), [])
        self.assertEqual(bills("客供件"), [])
        self.assertEqual(bills("AC-1"), ["PO-T"])  # 同單非 -TAB 列符合照列
        self.assertEqual(bills("PO-T"), ["PO-T"])  # 單號找得到，即使有 -TAB 列
        self.assertEqual(bills("甲"), ["PO-T"])

    def test_search_blank_lists_recent_30(self):
        for i in range(35):
            self.c03.seed(main_row(f"PO-{i:03d}", BillDate=20261001 + i), [])
        r = self.client.get("/api/erp/po/bills", params={"q": "  "})
        self.assertEqual(r.status_code, 200, r.text)
        bills = [b["bill_no"] for b in r.json()["bills"]]
        self.assertEqual(len(bills), 30)
        self.assertEqual(bills[0], "PO-034")
        self.assertEqual(bills[-1], "PO-005")
        sql, params = self.c03.log[-1]
        self.assertIn("SELECT TOP 30 m.BillNO", sql)
        self.assertIn("WHERE m.Flag = 4 ORDER BY m.BillDate DESC, m.BillNO DESC", sql)
        self.assertEqual(params, ())
        self.assertEqual([b["bill_no"] for b in self.client.get("/api/erp/po/bills").json()["bills"]], bills)
        self.assertTrue(all(s.startswith("SELECT") for s, _ in self.c03.log))  # 唯讀

    def test_search_other_company_refused(self):
        r = self.client.get("/api/erp/po/bills", params={"q": "PO", "company": "CHIComp02"})
        self.assertEqual(r.status_code, 400)

    def test_load_header_and_all_lines_including_tab(self):
        seed_open_bill(self.c03, extra_rows=[sub_row(BILL, 3, "CS-1-TAB", 5, 0, name="客供")])
        r = self.client.get("/api/erp/po/bill", params={"no": BILL})
        self.assertEqual(r.status_code, 200, r.text)
        d = r.json()
        self.assertEqual((d["bill_no"], d["bill_date"], d["supplier"], d["salesman"], d["remark"], d["currency"], d["tax_mode"]),
                         (BILL, "2026-11-01", {"id": "P0175", "name": "甲"}, "017", "原備註", "NTD", "taxable"))
        self.assertEqual((d["bill_status"], d["endacase_date"], d["editable"], d["lock_reason"]), (0, 0, True, ""))
        self.assertEqual([l["part_number"] for l in d["lines"]], ["AC-1", "AC-2", "CS-1-TAB"])
        self.assertEqual([l["customer_supplied"] for l in d["lines"]], [False, False, True])
        self.assertEqual(d["lines"][0], {
            "row_no": 1, "part_number": "AC-1", "product_name": "品名", "qty": 100.0, "price": 1.5, "amount": 150.0,
            "tax_amt": 7.0, "pre_in_date": "2026-11-20", "quantity": 100.0, "qty_remain": 100.0, "customer_supplied": False,
        })
        # 載入來源公司3；改單目標公司13 → 寫入原因要講清楚
        self.assertIn("目前改單只能寫入 公司13（測試）", d["write_block_reason"])
        self.assertIn("不可拿 公司3（正式） 的單去改 公司13（測試）", d["write_block_reason"])
        self.assertTrue(all(s.startswith("SELECT") for s, _ in self.c03.log))  # 唯讀

    def test_load_not_found_closed_received(self):
        self.assertEqual(self.client.get("/api/erp/po/bill", params={"no": "PO-NOPE"}).status_code, 400)
        self.assertIn("找不到採購單", self.client.get("/api/erp/po/bill", params={"no": "PO-NOPE"}).json()["detail"])
        self.c03.seed(main_row("PO-C", BillStatus=1, EndacaseDate=20261110), [sub_row("PO-C", 1, "AC-1", 10, 1, remain=0)])
        d = self.client.get("/api/erp/po/bill", params={"no": "PO-C"}).json()
        self.assertFalse(d["editable"])
        self.assertIn("已結案", d["lock_reason"])
        self.c03.seed(main_row("PO-R"), [sub_row("PO-R", 1, "AC-1", 10, 1), sub_row("PO-R", 2, "AC-2", 10, 1, remain=4)])
        d = self.client.get("/api/erp/po/bill", params={"no": "PO-R"}).json()
        self.assertFalse(d["editable"])
        self.assertIn("已有明細進過貨", d["lock_reason"])
        self.assertIn("第 2 列 AC-2", d["lock_reason"])

    def test_get_endpoints_not_edit_protected_posts_are(self):
        def req(method, path):
            return Request({"type": "http", "method": method, "path": path, "headers": [], "query_string": b""})
        for path in ("/api/erp/po/bills", "/api/erp/po/bill"):
            self.assertFalse(request_requires_edit_auth(req("GET", path)))
        for path in ("/api/erp/po/update/preview", "/api/erp/po/update"):
            self.assertTrue(request_requires_edit_auth(req("POST", path)))


class PreviewTests(EditCase):
    def _load13(self, bill=BILL):
        r = self.client.get("/api/erp/po/bill", params={"no": bill, "company": "CHIComp13"})
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    def test_open_unreceived_bill_preview_money_and_no_write(self):
        seed_open_bill(self.c13)
        d = self._load13()
        self.assertEqual(d["write_block_reason"], "")
        lines = [
            {"row_no": 1, "part_number": "AC-1", "qty": 200, "price": 1.5, "pre_in_date": "2026-11-25"},
            {"row_no": 0, "part_number": "ac-3", "qty": 3, "price": "0.333", "pre_in_date": "2026-11-30"},
        ]
        r = self.client.post("/api/erp/po/update/preview", json=edit_body(d, lines))
        self.assertEqual(r.status_code, 200, r.text)
        p = r.json()
        self.assertTrue(p["can_write"])
        self.assertEqual(p["counts"], {"update": 1, "insert": 1, "delete": 1, "keep": 0})
        self.assertEqual([(l["row_no"], l["action"]) for l in p["lines"]], [(1, "update"), (3, "insert")])
        self.assertEqual([l["part_number"] for l in p["deleted_lines"]], ["AC-2"])
        # 金額沿用 compute_money：300 + round(0.999, 2)=1.00；表頭稅 round(301 × 5%) = 15
        expected, totals = ep.compute_money(
            [{"qty": 200, "price": Decimal("1.5")}, {"qty": 3, "price": Decimal("0.333")}], currency="NTD")
        self.assertEqual(p["totals"]["sum_amount"], float(totals["sum_amount"]))
        self.assertEqual(p["totals"]["sum_tax"], float(totals["sum_tax"]))
        self.assertEqual(p["totals"]["total"], 316.0)
        self.assertEqual([l["tax_amt"] for l in p["lines"]], [float(x["tax_amt"]) for x in expected])
        self.assertEqual(p["old_totals"]["total"], 189.0)  # 180 + 9
        self.assertEqual(p["bill_date"], "2026-11-01")
        self.assertEqual(self.c13.writes(), [])
        self.assertGreaterEqual(self.c13.rollbacks, 1)

    def test_comp03_bill_preview_ok_but_cannot_write(self):
        seed_open_bill(self.c03)
        d = self.client.get("/api/erp/po/bill", params={"no": BILL}).json()
        p = self.client.post("/api/erp/po/update/preview", json=edit_body(d)).json()
        self.assertFalse(p["can_write"])
        self.assertIn("不可拿 公司3（正式） 的單去改 公司13（測試）", p["write_block_reason"])
        self.assertEqual(p["counts"]["keep"], 2)  # 什麼都沒改
        r = self.client.post("/api/erp/po/update", json={**edit_body(d), "confirm": True})
        self.assertEqual(r.status_code, 400)
        self.assertIn("目前改單只能寫入 公司13（測試）", r.json()["detail"])
        self.assertEqual(self.write_connects, [])
        self.assertEqual(self.c03.writes() + self.c13.writes(), [])

    def test_closed_and_received_refused_in_preview_and_update(self):
        self.c13.seed(main_row("PO-C", BillStatus=0, EndacaseDate=20261110), [sub_row("PO-C", 1, "AC-1", 10, 1)])
        self.c13.seed(main_row("PO-R"), [sub_row("PO-R", 1, "AC-1", 10, 1, remain=0)])
        for bill, word in (("PO-C", "已結案"), ("PO-R", "已有明細進過貨")):
            d = self._load13(bill)
            for path, extra in (("/api/erp/po/update/preview", {}), ("/api/erp/po/update", {"confirm": True})):
                r = self.client.post(path, json={**edit_body(d), **extra})
                self.assertEqual(r.status_code, 400, (bill, path))
                self.assertIn(word, r.json()["detail"])
        self.assertEqual(self.c13.writes(), [])

    def test_unknown_bill_and_tab_new_line_and_part_change_refused(self):
        seed_open_bill(self.c13)
        d = self._load13()
        r = self.client.post("/api/erp/po/update/preview", json={**edit_body(d), "bill_no": "PO-NOPE"})
        self.assertIn("找不到採購單", r.json()["detail"])
        tab = edit_body(d, [*edit_body(d)["lines"], {"row_no": 0, "part_number": "CS-1-tab", "qty": 1, "price": 0,
                                                     "pre_in_date": "2026-11-30"}])
        r = self.client.post("/api/erp/po/update/preview", json=tab)
        self.assertEqual(r.status_code, 400)
        self.assertIn("客供料", r.json()["detail"])
        changed = edit_body(d)
        changed["lines"][0]["part_number"] = "BB-9"
        r = self.client.post("/api/erp/po/update/preview", json=changed)
        self.assertIn("不可改成 BB-9", r.json()["detail"])


class UpdateTests(EditCase):
    def _load13(self, bill=BILL):
        return self.client.get("/api/erp/po/bill", params={"no": bill, "company": "CHIComp13"}).json()

    def test_update_insert_delete_and_readback(self):
        seed_open_bill(self.c13, extra_rows=[sub_row(BILL, 3, "AC-3", 1, 1)])
        db.add_erp_purchase_order({"bill_no": BILL, "target_db": "CHIComp13", "supplier_id": "P0175", "total": 189,
                                   "lines_json": "[]", "status": "ok"})
        d = self._load13()
        before = dict(self.c13.main(BILL))
        lines = [
            {"row_no": 1, "part_number": "AC-1", "qty": 120, "price": 1.5, "pre_in_date": "2026-11-28"},
            {"row_no": 3, "part_number": "AC-3", "qty": 1, "price": 1, "pre_in_date": "2026-11-20"},  # 沒改
            {"row_no": 0, "part_number": "BB-9", "qty": 7, "price": 2, "pre_in_date": "2026-12-01"},
        ]
        body = edit_body(d, lines, remark="改過", salesman="018")
        self.assertEqual(self.client.post("/api/erp/po/update", json=body).status_code, 400)  # 沒 confirm
        r = self.client.post("/api/erp/po/update", json={**body, "confirm": True})
        self.assertEqual(r.status_code, 200, r.text)
        res = r.json()
        self.assertTrue(res["ok"])
        self.assertEqual(res["counts"], {"update": 1, "insert": 1, "delete": 1, "keep": 1})
        self.assertEqual((self.c13.commits, self.write_connects[-1]), (1, "CHIComp13"))

        subs = self.c13.subs(BILL)
        self.assertEqual([(s["RowNO"], s["ProdID"]) for s in subs], [(1, "AC-1"), (3, "AC-3"), (4, "BB-9")])
        r1, r3, r4 = subs
        self.assertEqual((r1["Quantity"], r1["QtyRemain"], r1["Amount"], r1["PreInDate"]),
                         (Decimal("120"), Decimal("120"), Decimal("180.00"), 20261128))
        self.assertEqual((r1["sQuantity"], r1["EQuantity"], r1["Unit"], r1["ProdName"]), (Decimal("120"), Decimal("120"), "PCS", "品名"))
        self.assertEqual((r4["QtyRemain"], r4["BillDate"], r4["SerNO"], r4["ProdName"]), (Decimal("7"), 20261101, 4, "BB-9 名"))
        main = self.c13.main(BILL)
        self.assertEqual((main["Remark"], main["SalesMan"], main["SumBTaxAmt"], main["SumAmtATax"]),
                         ("改過", "018", Decimal("195.00"), Decimal("195.00") + main["SumTax"]))
        self.assertEqual(sum(s["TaxAmt"] for s in subs), main["SumTax"])
        # 付款條件 / 地址 / 開單日 / 核准人 / 拋轉 / 狀態都保持原值
        for col in ("BillDate", "DueTo", "CustAddress", "AccMonth", "PrepayDay", "EncashDay", "Permitter", "PermitterID",
                    "IsTrans", "Maker", "MakerID", "BillStatus", "EndacaseDate", "CustBillNo"):
            self.assertEqual(main[col], before[col], col)
        # UPDATE 欄位名明列；沒改的第 3 列不 UPDATE；表頭 WHERE 帶 BillStatus=0 / EndacaseDate=0 防呆
        writes = self.c13.writes()
        self.assertTrue(any(w.startswith("UPDATE dbo.ordBillMain SET [CustomerID] = %s") and
                            w.endswith("WHERE [Flag] = %s AND [BillNO] = %s AND [BillStatus] = %s AND [EndacaseDate] = %s")
                            for w in writes))
        self.assertEqual(sum(w.startswith("UPDATE dbo.ordBillSub") for w in writes), 1)
        self.assertEqual(sum(w.startswith("DELETE FROM dbo.ordBillSub") for w in writes), 1)
        self.assertFalse(any("DELETE FROM dbo.ordBillMain" in w for w in writes))
        # 本機紀錄註記已修改、不刪
        hist = db.list_erp_purchase_orders()
        self.assertEqual(len(hist), 1)
        self.assertTrue(hist[0]["edited_at"])
        self.assertIn("1 列更新、1 列新增、1 列刪除", hist[0]["edit_note"])
        self.assertTrue(any(l["action"] == "erp_po_updated" for l in db.get_activity_logs()))
        # 第二次用舊的 loaded 送出 → 已被改過，要重新載入
        again = self.client.post("/api/erp/po/update", json={**body, "confirm": True})
        self.assertEqual(again.status_code, 400)
        self.assertIn("請重新載入", again.json()["detail"])

    def test_tab_rows_kept_and_cannot_be_dropped(self):
        seed_open_bill(self.c13, extra_rows=[sub_row(BILL, 3, "CS-1-TAB", 5, 0, name="客供")])
        d = self._load13()
        self.assertEqual(d["lines"][2]["part_number"], "CS-1-TAB")
        without_tab = edit_body(d, [l for l in edit_body(d)["lines"] if l["row_no"] != 3])
        for path, extra in (("/api/erp/po/update/preview", {}), ("/api/erp/po/update", {"confirm": True})):
            r = self.client.post(path, json={**without_tab, **extra})
            self.assertEqual(r.status_code, 400, path)
            self.assertIn("-TAB 客供料", r.json()["detail"])
        self.assertEqual(self.c13.writes(), [])
        body = edit_body(d)
        body["lines"][0]["qty"] = 150
        r = self.client.post("/api/erp/po/update", json={**body, "confirm": True})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual([s["ProdID"] for s in self.c13.subs(BILL)], ["AC-1", "AC-2", "CS-1-TAB"])
        self.assertEqual(self.c13.subs(BILL)[2]["Quantity"], Decimal("5"))

    def test_readback_mismatch_rolls_back(self):
        seed_open_bill(self.c13)
        d = self._load13()
        body = edit_body(d)
        body["lines"][0]["qty"] = 300
        self.c13.corrupt_readback = True
        r = self.client.post("/api/erp/po/update", json={**body, "confirm": True})
        self.assertEqual(r.status_code, 500, r.text)
        self.assertIn("rollback", r.json()["detail"])
        self.assertEqual(self.c13.commits, 0)
        self.assertEqual(self.c13.subs(BILL)[0]["Quantity"], Decimal("100"))  # 沒寫進去

    def test_received_after_load_is_refused(self):
        seed_open_bill(self.c13)
        d = self._load13()
        self.c13.subs(BILL)  # 載入後有人進貨
        self.c13.saved["ordBillSub"][0]["QtyRemain"] = Decimal("40")
        self.c13.state = copy.deepcopy(self.c13.saved)
        r = self.client.post("/api/erp/po/update", json={**edit_body(d), "confirm": True})
        self.assertEqual(r.status_code, 400)
        self.assertIn("已有明細進過貨", r.json()["detail"])
        self.assertEqual(self.c13.writes(), [])

    def test_update_requires_loaded_snapshot(self):
        seed_open_bill(self.c13)
        d = self._load13()
        r = self.client.post("/api/erp/po/update", json={**edit_body(d), "loaded": None, "confirm": True})
        self.assertEqual(r.status_code, 400)
        self.assertIn("loaded", r.json()["detail"])

    def test_usd_keeps_original_rate_and_exempt_keeps_tax_type(self):
        subs = [sub_row("PO-U", 1, "AC-1", 1000, "0.125", TaxAmt=0, MLTaxAmt=0)]
        self.c13.seed(main_row("PO-U", CurrID="USD", ExchRate=Decimal("30.1"), TaxType=4, SumBTaxAmt=Decimal("125"),
                               SumAmtATax=Decimal("125"), SumQty=Decimal("1000"), LocalTotal=Decimal("3763")), subs)
        d = self._load13("PO-U")
        self.assertEqual((d["currency"], d["tax_mode"], d["tax_type"], d["exch_rate"]), ("USD", "exempt", 4, 30.1))
        body = edit_body(d)
        body["lines"][0]["qty"] = 2000
        p = self.client.post("/api/erp/po/update/preview", json=body).json()
        self.assertEqual((p["exch_rate"], p["tax_type"], p["totals"]["sum_tax"]), (30.1, 4, 0.0))
        self.assertEqual((p["totals"]["sum_amount"], p["totals"]["local_total"]), (250.0, 7525.0))  # round(250 × 30.1)
        r = self.client.post("/api/erp/po/update", json={**body, "confirm": True})
        self.assertEqual(r.status_code, 200, r.text)
        main = self.c13.main("PO-U")
        self.assertEqual((main["CurrID"], main["ExchRate"], main["TaxType"], main["LocalTotal"]),
                         ("USD", Decimal("30.1"), 4, Decimal("7525")))

    def test_switch_to_taxable_uses_compute_money(self):
        subs = [sub_row("PO-E", 1, "AC-1", 3, "33.33", TaxAmt=0, MLTaxAmt=0)]
        self.c13.seed(main_row("PO-E", TaxType=4, SumBTaxAmt=Decimal("99.99"), SumAmtATax=Decimal("99.99")), subs)
        d = self._load13("PO-E")
        p = self.client.post("/api/erp/po/update/preview", json=edit_body(d, tax_mode="taxable")).json()
        self.assertEqual((p["tax_type"], p["totals"]["sum_tax"], p["totals"]["total"]), (0, 5.0, 104.99))
        self.assertEqual(p["counts"]["update"], 1)

    def test_conversion_rows_refuse_qty_change(self):
        self.c13.seed(main_row("PO-K"), [sub_row("PO-K", 1, "AC-1", 10, 1, EQuantity=Decimal("10000"))])
        d = self._load13("PO-K")
        body = edit_body(d)
        body["lines"][0]["qty"] = 20
        r = self.client.post("/api/erp/po/update/preview", json=body)
        self.assertEqual(r.status_code, 400)
        self.assertIn("單位換算或折扣", r.json()["detail"])

    def test_prod_edit_fully_enabled_writes_comp03(self):
        seed_open_bill(self.c03)
        env = {"ERP_PO_EDIT_TARGET": "CHIComp03", "ERP_ALLOW_PROD_PO_EDIT_CHICOMP03": "1",
               "ERP_ALLOW_PROD_WRITE_CHICOMP03": "1", "ERP_ALLOW_PROD_PO_CHICOMP03": "1",
               "ERP_SQL_WRITE_USER_CHICOMP03": "u", "ERP_SQL_WRITE_PASSWORD_CHICOMP03": "p"}
        with patch.dict(os.environ, env):
            d = self.client.get("/api/erp/po/bill", params={"no": BILL}).json()
            self.assertEqual(d["write_block_reason"], "")
            body = edit_body(d)
            body["lines"][1]["price"] = 4
            r = self.client.post("/api/erp/po/update", json={**body, "confirm": True})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(self.write_connects, ["CHIComp03"])
        self.assertEqual(self.c03.subs(BILL)[1]["Price"], Decimal("4"))


if __name__ == "__main__":
    unittest.main()
