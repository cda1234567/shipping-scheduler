from __future__ import annotations

import copy
import os
import re
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import app.database as db
from app.services import erp_purchase as ep
from app.services import erp_receive as er
from app.services import erp_receive_schema as schema
from app.services import erp_stock

NOW = datetime(2026, 10, 30, 10, 0)
ENV = {"ERP_SQL_TEST_USER": "tester", "ERP_SQL_TEST_PASSWORD": "secret-pw", "ERP_WRITE_TARGET": "CHIComp13"}
FORBIDDEN = ("comInvoice", "stkBatchInvo", "accVoucher")


# ── 純函式 ───────────────────────────────────────────────────────────────────

class TermsTests(unittest.TestCase):
    def check(self, d, doc, g, gd, c, cd, acc, prepay, encash=None):
        t = er.calc_terms(d, doc, g, gd, c, cd)
        self.assertEqual(t["AccMonth"], acc)
        self.assertEqual(t["PrepayDay"], prepay)
        if encash is not None:
            self.assertEqual(t["EncashDay"], encash)

    def test_vectors(self):
        self.check(20261027, 25, 1, 60, 0, 0, 202611, 20270129, 20261027)
        self.check(20261001, 25, 1, 30, 0, 0, 202610, 20261130)
        self.check(20261029, 31, 1, 30, 1, 30, 202610, 20261130, 20261130)
        self.check(20261021, 25, 1, 60, 1, 60, 202610, 20261230, 20261230)
        self.check(20261102, 25, 1, 60, 3, 0, 202611, 20270129, 20261102)
        self.check(20261016, 25, 2, 30, 2, 30, 202610, 20261124, 20261224)
        self.check(20260921, 25, 2, 30, 2, 30, 202609, 20261025, 20261124)
        self.check(20250725, 25, 2, 90, 0, 0, 202507, 20251023)
        self.check(20261030, 31, 2, 6, 2, 3, 202610, 20261106, 20261109)  # Andy 的鏈

    def test_doc_zero_is_month_end_and_december_rollover(self):
        self.check(20261215, 0, 2, 0, 0, 0, 202612, 20261231)
        self.check(20261226, 25, 1, 0, 0, 0, 202701, 20270131)

    def test_unknown_styles_fall_back_to_date(self):
        t = er.calc_terms(20261030, 31, 9, 5, 9, 5)
        self.assertEqual((t["PrepayDay"], t["EncashDay"]), (20261030, 20261030))


class AmountTests(unittest.TestCase):
    def test_rounding_half_up_to_integer(self):
        self.assertEqual(er.calc_line_amounts(100, 24, 1, "0.05"), (Decimal(2400), Decimal(120)))
        self.assertEqual(er.calc_line_amounts(3, "0.5", 1, "0.05"), (Decimal(2), Decimal(0)))  # 1.5 → 2, 0.1 → 0
        self.assertEqual(er.calc_line_amounts(9000, "0.46", 1, "0.05"), (Decimal(4140), Decimal(207)))
        self.assertEqual(er.calc_line_amounts(10, "1.05", 1, "0.05"), (Decimal(11), Decimal(1)))  # 10.5 → 11, 0.55 → 1

    def test_zero_discount_treated_as_one(self):
        self.assertEqual(er.calc_line_amounts(10, 5, 0, 0)[0], Decimal(50))

    def test_rm_date_validation(self):
        self.assertEqual(er.parse_rm_date(None, 20261030), 20261030)
        self.assertEqual(er.parse_rm_date("2026-11-02", 20261030), 20261102)
        for bad in (20261340, 20260230, "abc", 2026102):
            with self.assertRaises(er.ReceiveValidationError):
                er.parse_rm_date(bad, 20261030)


# ── 假 ERP ───────────────────────────────────────────────────────────────────

def _lc(row):
    return {k.lower(): v for k, v in row.items()}


class FakeErp:
    def __init__(self, db_name="CHIComp13"):
        self.db_name = db_name
        self.tables = {}
        self.executed = []  # (sql, params)
        self.committed = False
        self.rolled_back = False
        self.fail_on = None  # 子字串：包含它的 SQL 丟例外
        self._snapshot = None

    def table(self, name):
        return self.tables.setdefault(name, [])

    def connect(self):
        self._snapshot = copy.deepcopy(self.tables)
        return FakeConn(self)

    def inserted(self, table):
        return [i for i in self.executed if i[0].startswith(f"INSERT INTO dbo.{table} ")]

    def sql_text(self):
        return "\n".join(s for s, _ in self.executed)


class FakeConn:
    def __init__(self, erp):
        self.erp = erp

    def cursor(self):
        return FakeCursor(self.erp)

    def commit(self):
        self.erp.committed = True

    def rollback(self):
        self.erp.rolled_back = True
        self.erp.tables = copy.deepcopy(self.erp._snapshot)

    def close(self):
        pass


COND = re.compile(r"(?:LEN\((\w+)\)|\[?(\w+)\]?) (=|LIKE) (%s|\d+)")


class FakeCursor:
    def __init__(self, erp):
        self.erp = erp
        self.rows = []
        self.description = None
        self.rowcount = 0

    def _match(self, rows, where, params):
        params = list(params)
        for part in where.split(" AND "):
            m = COND.fullmatch(part.strip())
            assert m, f"fake 看不懂條件：{part}"
            len_col, col, op, rhs = m.groups()
            val = params.pop(0) if rhs == "%s" else int(rhs)
            if len_col:
                rows = [r for r in rows if len(str(_lc(r).get(len_col.lower(), ""))) == val]
            elif op == "LIKE":
                rows = [r for r in rows if str(_lc(r).get(col.lower(), "")).startswith(val.rstrip("%"))]
            else:
                rows = [r for r in rows if str(_lc(r).get(col.lower())) == str(val)]
        return rows

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        params = tuple(params or ())
        e = self.erp
        e.executed.append((sql, params))
        if e.fail_on and e.fail_on in sql:
            raise RuntimeError("boom secret-pw")
        if sql == "SELECT DB_NAME()":
            self.rows, self.description = [(e.db_name,)], [("",)]
        elif "FROM sys.columns" in sql:
            table = params[0].split(".", 1)[1]
            self.rows = [(c, 0, 0) for c in schema.DEFAULTS[table]]
            self.description = [("name",), ("is_identity",), ("is_computed",)]
        elif sql.startswith("INSERT INTO"):
            m = re.match(r"INSERT INTO dbo\.(\w+) \((.*?)\) VALUES", sql)
            cols = [c.strip("[]") for c in m.group(2).split(", ")]
            e.table(m.group(1)).append(dict(zip(cols, params)))
            self.rowcount = 1
        elif sql.startswith("UPDATE"):
            m = re.match(r"UPDATE dbo\.(\w+) SET (.*?) WHERE (.*)", sql)
            set_cols = re.findall(r"\[(\w+)\] = %s", m.group(2))
            hit = self._match(e.table(m.group(1)), m.group(3), params[len(set_cols):])
            for r in hit:
                low = {k.lower(): k for k in r}
                for c, v in zip(set_cols, params):
                    r[low.get(c.lower(), c)] = v
            self.rowcount = len(hit)
        elif "FROM ordBillSub s JOIN ordBillMain m" in sql:
            self.description = [(n,) for n in "BillNO RowNO BillDate CustomerID ShortName FullName ProdID ProdName "
                                "Quantity QtyRemain Price TaxRate Unit CurrID Discount".split()]
            self.rows = []
            for s in e.table("ordBillSub"):
                m = next(x for x in e.table("ordBillMain") if x["BillNO"] == s["BillNO"])
                c = next((x for x in e.table("comCustomer") if x["ID"] == m["CustomerID"]), {})
                if m["BillStatus"] == 0 and s["QtyRemain"] > 0:
                    self.rows.append((s["BillNO"], s["RowNO"], m["BillDate"], m["CustomerID"], c.get("ShortName", ""),
                                      c.get("FullName", ""), s["ProdID"], s["ProdName"], s["Quantity"], s["QtyRemain"],
                                      s["Price"], s["TaxRate"], s["Unit"], m["CurrID"], s["Discount"]))
        else:
            m = re.match(r"SELECT (.+?) FROM (?:dbo\.)?(\w+)(?: WITH \(UPDLOCK, HOLDLOCK\))?"
                         r"(?: WHERE (.+?))?(?: ORDER BY .+)?$", sql)
            assert m, f"fake 看不懂：{sql}"
            cols, table, where = m.groups()
            rows = self._match(e.table(table), where, params) if where else list(e.table(table))
            if cols.startswith("MAX("):
                col = cols[4:-1]
                vals = [str(_lc(r)[col.lower()]) for r in rows]
                self.rows, self.description = [(max(vals) if vals else None,)], [("",)]
            elif cols == "*":
                self.description = [(k,) for k in (rows[0] if rows else schema.DEFAULTS.get(table, {}))]
                self.rows = [tuple(r.values()) for r in rows]
            elif cols == "1":
                self.rows, self.description = [(1,)] * len(rows), [("",)]
            else:
                names = [c.split(".")[-1].strip() for c in cols.split(",")]
                self.description = [(n,) for n in names]
                self.rows = [tuple(_lc(r).get(n.lower()) for n in names) for r in rows]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


def po_main(no, vendor="CS007", status=0, **kw):
    row = {"Flag": 4, "BillNO": no, "BillDate": 20261029, "CustomerID": vendor, "AddressID": "001", "ZipCode": "105",
           "CustAddress": "台北市", "SalesMan": "R022", "DepartID": "D010", "CurrID": "NTD", "ExchRate": 1,
           "BillStatus": status, "PriceOfTax": True, "GatherStyle": 2, "GatherDelay": 6, "CheckStyle": 2,
           "CheckDelay": 3, "GatherOther": "", "CheckOther": "", "LinkMan": "", "LinkTelephone": "",
           "LinkManProf": "", "EndacaseDate": 0}
    row.update(kw)
    return row


def po_sub(no, row_no, prod, qty, remain=None, price=24, **kw):
    row = {"Flag": 4, "BillNO": no, "RowNO": row_no, "ProdID": prod, "ProdName": f"{prod} 名", "Quantity": Decimal(qty),
           "QtyRemain": Decimal(qty if remain is None else remain), "Price": Decimal(price), "TaxRate": Decimal("0.05"),
           "Discount": 1, "Unit": ""}
    row.update(kw)
    return row


def params(by_ware, digits, dot, describe="A"):
    # ComDefDescribe 照 Comp13（'A'）；Comp13 沒有 CreateAccByFund
    return [{"UserID": "", "ParamName": n, "paramValue": str(v)}
            for n, v in (("CostByWare", by_ware), ("ComCostDigs", digits), ("comCostDot", dot), ("ComDefDescribe", describe))]


def make_erp(db_name="CHIComp13"):
    erp = FakeErp(db_name)
    erp.tables = {
        "ordBillMain": [po_main("PO-1"), po_main("PO-2"), po_main("PO-X", vendor="P0087"), po_main("PO-CLOSED", status=1)],
        "ordBillSub": [
            po_sub("PO-1", 1, "AC-1", 100), po_sub("PO-1", 2, "AC-2", 50),
            po_sub("PO-2", 1, "AC-1", 10, price=24), po_sub("PO-X", 1, "AC-3", 5),
            po_sub("PO-CLOSED", 1, "AC-1", 5),
        ],
        "comCustomer": [{"Flag": 2, "ID": "CS007", "ShortName": "甲", "FullName": "甲公司"}],
        "comCustTrade": [{"Flag": 2, "ID": "CS007", "DayOfClose": 31, "InvoiceStyle": 1, "LatelyTradeDate": 0},
                         {"Flag": 2, "ID": "P0087", "DayOfClose": 25, "InvoiceStyle": 1, "LatelyTradeDate": 0}],
        "comWareHouse": [{"WareHouseID": "001"}],
        "comProduct": [{"ProdID": p, "CTotalCost": Decimal(5447), "LatestPurchDate": 0} for p in ("AC-1", "AC-2", "AC-3")],
        "comWareAmount": [{"ProdID": "AC-2", "WareID": "001", "Quantity": Decimal(10), "TotalCost": Decimal(100),
                           "Cost": Decimal(10), "BCQty": Decimal(10), "LatestPurchDate": 0}],
        "StkYearMonthQty": [{"YearMonth": 202610, "ProdID": "AC-2", "WareID": "001", "Quantity": Decimal(10)}],
        "stkYearMonthAvg": [], "comCustAccounts": [{"Flag": 2, "SysID": 2, "ID": "CS007", "CurrID": "NTD",
                                                    "CurReceivable": Decimal(2520), "LocCurReceivable": Decimal(2520)}],
        # Andy 當天已有無前綴的 20261030001
        "comParams": params(1, 5, 0),
        "ordExamineMain": [{"Flag": 5, "ExamineNO": "20261030001"}, {"Flag": 6, "ExamineNO": "20261030001"}],
        "stkBillMain": [{"Flag": 100, "BillNO": "20261030001"}],
        "comBillAccounts": [{"Flag": 100, "SysID": 2, "FundBillNo": "20261030001"}],
    }
    return erp


class ErpCase(unittest.TestCase):
    def setUp(self):
        clean = {k: v for k, v in os.environ.items() if not k.startswith(("ERP_SQL_WRITE", "ERP_SQL_TEST", "ERP_WRITE", "ERP_PO", "ERP_RECEIVE"))}
        clean.update(ENV)
        p = patch.dict(os.environ, clean, clear=True)
        p.start()
        self.addCleanup(p.stop)
        self.erp = make_erp()
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

        for p in (
            patch.object(db, "get_conn", temp_conn),
            patch.object(ep, "_connect_write", lambda creds: self.erp.connect()),
            patch.object(er, "local_now", lambda: NOW),
            patch.object(erp_stock, "get_settings", lambda: {
                "server": "s", "port": 1433, "database": "CHIComp03", "ware_id": "001",
                "timeout": 5.0, "encryption": "off", "tds_version": "7.3"}),
        ):
            p.start()
            self.addCleanup(p.stop)

    def req(self, lines=None, **kw):
        return {"lines": lines or [{"po_no": "PO-1", "row_no": 1, "qty": 100}], **kw}

    def rows(self, table):
        return self.erp.tables[table]


class PlanTests(ErpCase):
    def test_preview_numbers_dates_and_rows_without_writes(self):
        r = er.preview_receive(self.req())
        self.assertEqual((r["im_no"], r["qm_no"], r["rm_no"]), ("IM-20261030002", "QM-20261030002", "RM-20261030002"))
        self.assertEqual(r["terms"], {"AccMonth": 202610, "PrepayDay": 20261106, "EncashDay": 20261109})
        self.assertEqual(r["totals"], {"sum_qty": 100.0, "sum_amount": 2400.0, "sum_tax": 120.0, "total": 2520.0})
        self.assertTrue(r["numbers_estimated"])
        self.assertEqual(set(r["rows"]), {"ordExamineMain", "ordExamineSub", "stkBillMain", "stkBillSub",
                                          "comBillAccounts", "comProdRec", "StkYearMonthQty", "stkYearMonthAvg",
                                          "comWareAmount", "comChangeLog"})
        self.assertEqual(r["rows"]["stkBillSub"][0]["StkMlAmount"], 2400.0)
        self.assertFalse(self.erp.inserted("ordExamineMain") or self.erp.committed)
        self.assertNotIn("UPDLOCK", self.erp.sql_text())

    def test_serial_shared_and_prefixes(self):
        cur = FakeCursor(self.erp)
        pre = ("IM-", "QM-", "RM-")
        # 無前綴的既有單號（Andy 手動鏈）也算進去：001 → 002
        self.assertEqual(er.next_receive_serial(cur, pre, [20261030], False), 2)
        # Comp13 當天有無前綴 001、002 → 下一號 003
        self.rows("stkBillMain").append({"Flag": 100, "BillNO": "20261030002"})
        self.assertEqual(er.next_receive_serial(cur, pre, [20261030], False), 3)
        # 有前綴與無前綴混合：取最大
        self.rows("ordExamineMain").append({"Flag": 6, "ExamineNO": "QM-20261030005"})
        self.assertEqual(er.next_receive_serial(cur, pre, [20261030], False), 6)
        self.rows("comBillAccounts").append({"Flag": 100, "SysID": 2, "FundBillNo": "20261030009"})
        self.assertEqual(er.next_receive_serial(cur, pre, [20261030], False), 10)
        self.rows("stkBillMain").append({"Flag": 100, "BillNO": "RM-20261030011"})
        self.assertEqual(er.next_receive_serial(cur, pre, [20261030], False), 12)
        self.assertEqual(er.make_numbers(pre, 20261030, 20261102, 4),
                         {"im_no": "IM-20261030004", "qm_no": "QM-20261030004", "rm_no": "RM-20261102004"})
        self.assertEqual(er.number_prefixes("CHIComp13"), pre)
        self.assertEqual(er.number_prefixes("CHIComp99"), pre)

    def test_serial_two_dates_takes_max_of_both(self):
        cur = FakeCursor(self.erp)
        self.rows("stkBillMain").append({"Flag": 100, "BillNO": "RM-20261102007"})
        pre = ("IM-", "QM-", "RM-")
        self.assertEqual(er.next_receive_serial(cur, pre, [20261030, 20261102], False), 8)
        self.assertEqual(er.next_receive_serial(cur, pre, [20261030], False), 2)

    def test_serial_exhausted(self):
        self.rows("stkBillMain").append({"Flag": 100, "BillNO": "RM-20261030999"})
        with self.assertRaises(er.ReceiveValidationError):
            er.next_receive_serial(FakeCursor(self.erp), ("IM-", "QM-", "RM-"), [20261030], False)

    def test_serial_malformed_raises(self):
        self.rows("stkBillMain").append({"Flag": 100, "BillNO": "RM-2026103x001"})
        self.rows("stkBillMain")[-1]["BillNO"] = "RM-20261030abc"
        with self.assertRaises(ep.PoSchemaError):
            er.next_receive_serial(FakeCursor(self.erp), ("IM-", "QM-", "RM-"), [20261030], False)

    def test_lock_hint_only_when_locking(self):
        er.create_receive(self.req(), "t", "1.2.3.4")
        text = self.erp.sql_text()
        for t in ("ordBillMain", "ordBillSub", "comWareAmount", "stkBillMain", "comCustTrade"):
            self.assertRegex(text, rf"FROM {t} WITH \(UPDLOCK, HOLDLOCK\)")

    def test_rm_date_differs_uses_both_dates(self):
        self.rows("stkBillMain").append({"Flag": 100, "BillNO": "20261102004"})
        r = er.preview_receive(self.req(rm_date=20261102))
        self.assertEqual((r["im_no"], r["rm_no"]), ("IM-20261030005", "RM-20261102005"))
        self.assertTrue(any(w["code"] == "rm_date_not_today" for w in r["warnings"]))
        self.assertEqual(r["terms"]["AccMonth"], 202611)


class CreateTests(ErpCase):
    def test_single_po_chain_matches_andy(self):
        r = er.create_receive(self.req(), "tester", "10.0.0.1")
        self.assertTrue(r["ok"] and self.erp.committed)
        n = "RM-20261030002"
        im_no, qm_no = "IM-20261030002", "QM-20261030002"
        self.assertEqual((r["im_no"], r["qm_no"], r["rm_no"]), (im_no, qm_no, n))
        im = [x for x in self.rows("ordExamineMain") if x["ExamineNO"] in (im_no, qm_no)]
        self.assertEqual(sorted(x["Flag"] for x in im), [5, 6])
        for x in im:
            self.assertEqual((x["CustomerID"], x["Examiner"], x["DepartID"], x["HasCheck"], x["ExamineDate"]),
                             ("CS007", "R022", "D010", True, 20261030))
            self.assertEqual((x["Maker"], x["MakerID"]), ("Andy", "017"))
        subs = {x["Flag"]: x for x in self.rows("ordExamineSub") if x["ExamineNO"] in (im_no, qm_no)}
        self.assertEqual((subs[5]["TranType"], subs[5]["srcFrom"], subs[5]["FromNO"], subs[5]["FromRow"]), (304, 304, "PO-1", 1))
        self.assertEqual((subs[6]["TranType"], subs[6]["srcFrom"], subs[6]["FromNO"], subs[6]["FromRow"]), (306, 304, im_no, 1))
        for s in subs.values():
            self.assertEqual((s["ExmineType"], s["QtyRemain"], s["Quantity"], s["IsOdd"]), ("A1", 0, Decimal(100), True))
        stk = next(x for x in self.rows("stkBillMain") if x["BillNO"] == n)
        self.assertEqual((stk["SumAmtATax"], stk["SumNum"], stk["GatherStyle"], stk["GatherDelay"], stk["CheckStyle"],
                          stk["CheckDelay"], stk["Detail"], stk["BillStatus"], stk["Maker"]),
                         (Decimal(2520), Decimal(100), 2, 6, 2, 3, "A", 1, ""))
        sub = next(x for x in self.rows("stkBillSub") if x["BillNO"] == n)
        self.assertEqual((sub["StkMlAmount"], sub["LocalPayment"], sub["Amount"], sub["Price"]), (Decimal(2400), 0, 0, 0))
        acc = next(x for x in self.rows("comBillAccounts") if x["FundBillNo"] == n)
        self.assertEqual((acc["Total"], acc["Tax"], acc["LocalTotal"], acc["AccMonth"], acc["PrepayDay"], acc["EncashDay"],
                          acc["DeptID"], acc["InvoStyle"], acc["Salesman"], acc["CashPayStyle"], acc["VisaPayStyle"],
                          acc["CostFlag"], acc["InvoiceNo"], acc["AddrID"], acc["ZipCode"], acc["BillDate"]),
                         (Decimal(2400), Decimal(120), Decimal(2400), 202610, 20261106, 20261109, "D010", 1, "R022",
                          "001", "002", 100, "", "001", "105", 20261030))
        rec = next(x for x in self.rows("comProdRec") if x["BillNO"] == n)
        self.assertEqual((rec["TranType"], rec["FromNO"], rec["FromRow"], rec["WareID"], rec["Amount"], rec["MLAmount"],
                          rec["CostForAcc"], rec["TaxAmt"], rec["MLTaxAmt"], rec["QtyRemain"], rec["UnitRelation"],
                          rec["DisCount"], rec["CostAvg"]),
                         (205, qm_no, 1, "001", Decimal(2400), Decimal(2400), Decimal(2400), Decimal(120), Decimal(120),
                          Decimal(100), 1, 1, Decimal(24)))

    def test_rollups(self):
        er.create_receive(self.req([{"po_no": "PO-1", "row_no": 1, "qty": 100}, {"po_no": "PO-1", "row_no": 2, "qty": 20}]), "t", "")
        po1 = {x["RowNO"]: x for x in self.rows("ordBillSub") if x["BillNO"] == "PO-1"}
        self.assertEqual((po1[1]["QtyRemain"], po1[2]["QtyRemain"]), (0, Decimal(30)))
        main = next(x for x in self.rows("ordBillMain") if x["BillNO"] == "PO-1")
        self.assertEqual(main["BillStatus"], 0)  # 第 2 列未收完，不結案
        ware = {(x["ProdID"], x["WareID"]): x for x in self.rows("comWareAmount")}
        self.assertEqual((ware[("AC-1", "001")]["Quantity"], ware[("AC-1", "001")]["TotalCost"], ware[("AC-1", "001")]["Cost"],
                          ware[("AC-1", "001")]["BCQty"], ware[("AC-1", "001")]["LatestPurchDate"]),
                         (Decimal(100), Decimal(2400), Decimal(24), Decimal(100), 20261030))
        w2 = ware[("AC-2", "001")]  # 既有 10 / 100 + 20 × 24
        self.assertEqual((w2["Quantity"], w2["TotalCost"], w2["BCQty"]), (Decimal(30), Decimal(580), Decimal(30)))
        self.assertEqual(w2["Cost"], 580 / 30)  # CostByWare=1、comCostDot=0：完整浮點數
        prod = {x["ProdID"]: x for x in self.rows("comProduct")}
        self.assertEqual((prod["AC-1"]["CTotalCost"], prod["AC-1"]["LatestPurchDate"]), (Decimal(7847), 20261030))
        ym = {x["ProdID"]: x["Quantity"] for x in self.rows("StkYearMonthQty")}
        self.assertEqual((ym["AC-1"], ym["AC-2"]), (Decimal(100), Decimal(30)))
        avg = self.rows("stkYearMonthAvg")
        self.assertEqual({(x["ProdID"], x["WareID"], x["BatchID"], x["AtLastQuan"]) for x in avg},
                         {("AC-1", "001", "", Decimal(100)), ("AC-2", "001", "", Decimal(20))})
        trade = next(x for x in self.rows("comCustTrade") if x["ID"] == "CS007")
        self.assertEqual(trade["LatelyTradeDate"], 20261030)
        acct = self.rows("comCustAccounts")[0]
        self.assertEqual((acct["CurReceivable"], acct["LocCurReceivable"]), (Decimal(2520 + 2520 + 504), Decimal(5544)))

    def test_missing_accounts_row_inserted(self):
        self.erp.tables["comCustAccounts"] = []
        er.create_receive(self.req(), "t", "")
        acct = self.rows("comCustAccounts")[0]
        self.assertEqual((acct["ID"], acct["CurrID"], acct["CurReceivable"], acct["StartExchRate"]), ("CS007", "NTD", Decimal(2520), 1))

    def test_po_closes_when_all_lines_zero_and_changelog(self):
        er.create_receive(self.req([{"po_no": "PO-1", "row_no": 1, "qty": 100}, {"po_no": "PO-1", "row_no": 2, "qty": 50}]), "t", "9.9.9.9")
        main = next(x for x in self.rows("ordBillMain") if x["BillNO"] == "PO-1")
        self.assertEqual((main["BillStatus"], main["EndacaseDate"]), (1, 20261030))
        logs = self.rows("comChangeLog")
        self.assertEqual([(x["ProgID"], x["PKValue"], x["Action"]) for x in logs],
                         [("ChiOrder.OrdGet", "IM-20261030002", 1), ("ChiOrder.OrdExamIn", "QM-20261030002", 1),
                          ("ChiStock.Buy", "RM-20261030002", 1), ("CHIOrder.Purchase", "PO-1", 9)])
        self.assertEqual(logs[3]["ActionName"], "進貨憑單RM-20261030002過賬單況為已結案")
        self.assertEqual((logs[0]["UserName"], logs[0]["IPAddr"], logs[0]["WinLang"]), ("017", "9.9.9.9", 1))

    def test_batch_multi_po_rows_and_links(self):
        r = er.create_receive(self.req([
            {"po_no": "PO-2", "row_no": 1, "qty": 4}, {"po_no": "PO-1", "row_no": 2, "qty": 5}, {"po_no": "PO-1", "row_no": 1, "qty": 6}]), "t", "")
        n = r["rm_no"]
        for flag in (5, 6):
            no = r["im_no"] if flag == 5 else r["qm_no"]
            subs = sorted((x for x in self.rows("ordExamineSub") if x["Flag"] == flag and x["ExamineNO"] == no), key=lambda x: x["RowNO"])
            self.assertEqual([x["RowNO"] for x in subs], [1, 2, 3])
            expect = [("PO-2", 1), ("PO-1", 2), ("PO-1", 1)] if flag == 5 else [(r["im_no"], 1), (r["im_no"], 2), (r["im_no"], 3)]
            self.assertEqual([(x["FromNO"], x["FromRow"]) for x in subs], expect)
        self.assertEqual(len([x for x in self.rows("ordExamineMain") if x["ExamineNO"] in (r["im_no"], r["qm_no"])]), 2)
        self.assertTrue(all(x["FromNO"] == r["qm_no"] for x in self.rows("comProdRec") if x["BillNO"] == n))
        self.assertEqual(len(self.rows("stkBillSub")), 3)
        recs = sorted((x for x in self.rows("comProdRec") if x["BillNO"] == n), key=lambda x: x["RowNO"])
        self.assertEqual([(x["ProdID"], x["FromRow"]) for x in recs], [("AC-1", 1), ("AC-2", 2), ("AC-1", 3)])
        ware = next(x for x in self.rows("comWareAmount") if x["ProdID"] == "AC-1")
        self.assertEqual(ware["Quantity"], Decimal(10))  # 兩列同料號合併成一次更新

    def test_rm_date_in_rows(self):
        r = er.create_receive(self.req(rm_date=20261102), "t", "")
        self.assertEqual((r["im_no"], r["rm_no"]), ("IM-20261030002", "RM-20261102002"))
        acc = next(x for x in self.rows("comBillAccounts") if x["FundBillNo"] == "RM-20261102002")
        self.assertEqual((acc["BillDate"], acc["AccMonth"], acc["PrepayDay"]), (20261102, 202611, 20261206))
        sub = next(x for x in self.rows("ordExamineSub") if x["ExamineNO"] == "IM-20261030002")
        self.assertEqual(sub["ExamineDate"], 20261030)
        self.assertEqual(next(x for x in self.rows("StkYearMonthQty") if x["ProdID"] == "AC-1")["YearMonth"], 202611)
        main = next(x for x in self.rows("ordBillMain") if x["BillNO"] == "PO-1")
        self.assertEqual(main["EndacaseDate"], 0)  # PO-1 第 2 列還沒收

    def test_price_override(self):
        r = er.create_receive(self.req([{"po_no": "PO-1", "row_no": 1, "qty": 10, "price": 25.5}]), "t", "")
        self.assertEqual(r["totals"]["sum_amount"], 255.0)
        self.assertTrue(any(w["code"] == "price_diff" for w in r["warnings"]))

    def test_no_forbidden_tables_touched(self):
        er.create_receive(self.req(), "t", "")
        text = self.erp.sql_text()
        for name in FORBIDDEN:
            self.assertNotIn(name, text)
        touched = {m.group(1) for m in re.finditer(r"(?:INSERT INTO|UPDATE) dbo\.(\w+)", text)}
        self.assertTrue(touched <= {"ordExamineMain", "ordExamineSub", "stkBillMain", "stkBillSub", "comBillAccounts",
                                    "comProdRec", "ordBillSub", "ordBillMain", "comWareAmount", "comProduct",
                                    "StkYearMonthQty", "stkYearMonthAvg", "comCustTrade", "comCustAccounts", "comChangeLog"}, touched)
        updates = " ".join(sql for sql, _ in self.erp.executed if sql.startswith("UPDATE")).lower()
        self.assertNotIn("latestindate", updates)

    def test_history_logged(self):
        r = er.create_receive(self.req(client_token="tok1"), "tester", "1.1.1.1")
        h = er.list_history()
        self.assertEqual((h[0]["rm_no"], h[0]["status"], h[0]["vendor_id"], h[0]["total"]), (r["rm_no"], "ok", "CS007", 2520.0))


class RuleTests(ErpCase):
    def test_over_receive_needs_confirm(self):
        with self.assertRaises(er.OverReceiveWarning) as cm:
            er.create_receive(self.req([{"po_no": "PO-2", "row_no": 1, "qty": 15}]), "t", "")
        self.assertEqual(cm.exception.items[0]["over"], 5.0)
        self.assertFalse(self.erp.inserted("ordExamineMain") or self.erp.committed)
        self.assertEqual(er.list_history(), [])  # 警告不是錯誤，不記失敗
        pv = er.preview_receive(self.req([{"po_no": "PO-2", "row_no": 1, "qty": 15}]))
        self.assertTrue(pv["needs_confirm"])
        r = er.create_receive(self.req([{"po_no": "PO-2", "row_no": 1, "qty": 15}], confirm=True), "t", "")
        self.assertTrue(r["ok"])
        sub = next(x for x in self.rows("ordBillSub") if x["BillNO"] == "PO-2")
        self.assertEqual(sub["QtyRemain"], 0)  # 不會變負
        self.assertEqual(next(x for x in self.rows("ordBillMain") if x["BillNO"] == "PO-2")["BillStatus"], 1)

    def test_vendor_lock(self):
        with self.assertRaisesRegex(er.ReceiveValidationError, "同一家廠商"):
            er.preview_receive(self.req([{"po_no": "PO-1", "row_no": 1, "qty": 1}, {"po_no": "PO-X", "row_no": 1, "qty": 1}]))
        self.assertFalse(self.erp.inserted("ordExamineMain"))

    def test_currency_mismatch(self):
        self.rows("ordBillMain")[1]["CurrID"] = "USD"
        with self.assertRaisesRegex(er.ReceiveValidationError, "幣別"):
            er.preview_receive(self.req([{"po_no": "PO-1", "row_no": 1, "qty": 1}, {"po_no": "PO-2", "row_no": 1, "qty": 1}]))

    def test_bad_inputs(self):
        for lines, msg in (
            ([{"po_no": "PO-NONE", "row_no": 1, "qty": 1}], "找不到採購單"),
            ([{"po_no": "PO-1", "row_no": 9, "qty": 1}], "沒有第 9 列"),
            ([{"po_no": "PO-CLOSED", "row_no": 1, "qty": 1}], "已結案"),
            ([{"po_no": "PO-1", "row_no": 1, "qty": 0}], "大於 0"),
            ([{"po_no": "PO-1", "row_no": 1, "qty": 1}, {"po_no": "PO-1", "row_no": 1, "qty": 2}], "重複"),
            ([], "1～"),
        ):
            with self.assertRaisesRegex(er.ReceiveValidationError, msg):
                er.preview_receive({"lines": lines})

    def test_missing_trade_or_warehouse(self):
        self.erp.tables["comCustTrade"] = []
        with self.assertRaisesRegex(er.ReceiveValidationError, "comCustTrade"):
            er.preview_receive(self.req())
        self.erp.tables["comCustTrade"] = [{"Flag": 2, "ID": "CS007", "DayOfClose": 31, "InvoiceStyle": 1}]
        self.erp.tables["comWareHouse"] = []
        with self.assertRaisesRegex(er.ReceiveValidationError, "倉庫"):
            er.preview_receive(self.req())

    def test_warehouse_env(self):
        self.erp.tables["comWareHouse"].append({"WareHouseID": "05"})
        with patch.dict(os.environ, {"ERP_RECEIVE_WAREHOUSE": "05"}):
            er.create_receive(self.req(), "t", "")
        self.assertEqual(self.rows("comProdRec")[0]["WareID"], "05")

    def test_guard_refuses_production_and_non_test(self):
        for target in ("CHIComp03", "CHIComp07", "CHIComp99"):
            with self.assertRaises(er.ReceiveValidationError):
                er.preview_receive(self.req(target=target))
            with self.assertRaises(er.ReceiveValidationError):
                er.create_receive(self.req(target=target), "t", "")
        with patch.dict(os.environ, {"ERP_WRITE_TARGET": "CHIComp03"}):
            with self.assertRaises(er.ReceiveValidationError):
                er.create_receive(self.req(), "t", "")
            self.assertFalse(er.get_config()["available"])
        with patch.dict(os.environ, {"ERP_TEST_TARGETS": "CHIComp03,CHIComp13"}):
            with self.assertRaises(er.ReceiveValidationError):
                er.create_receive(self.req(target="CHIComp03"), "t", "")
        self.assertEqual(self.erp.executed, [])

    def test_db_name_mismatch_aborts(self):
        self.erp.db_name = "CHIComp03"
        with self.assertRaisesRegex(ep.PoValidationError, "不符"):
            er.create_receive(self.req(), "t", "")
        self.assertFalse(self.erp.inserted("ordExamineMain"))

    def test_rollback_on_midway_failure_hides_secrets(self):
        self.erp.fail_on = "UPDATE dbo.comProduct"
        before = copy.deepcopy(self.erp.tables)
        with self.assertRaises(ep.ErpUnavailable) as cm:
            er.create_receive(self.req(), "t", "")
        self.assertNotIn("secret-pw", str(cm.exception))
        self.assertTrue(self.erp.rolled_back and not self.erp.committed)
        self.assertEqual(self.erp.tables, before)
        h = er.list_history()
        self.assertEqual((h[0]["status"], h[0]["rm_no"]), ("error", ""))
        self.assertNotIn("secret-pw", h[0]["error"])

    def test_schema_mismatch_aborts_without_commit(self):
        orig = ep.table_columns

        def cols(cur, table):
            c = orig(cur, table)
            return c + [{"name": "NewCol", "identity": False, "computed": False}] if table == "stkBillSub" else c

        with patch.object(ep, "table_columns", cols):
            with self.assertRaises(ep.ErpError):
                er.create_receive(self.req(), "t", "")
        self.assertFalse(self.erp.committed)
        self.assertTrue(self.erp.rolled_back)

    def test_verify_mismatch_rolls_back(self):
        with patch.object(er, "_verify", side_effect=ep.PoSchemaError("讀回不符")):
            with self.assertRaises(ep.PoSchemaError):
                er.create_receive(self.req(), "t", "")
        self.assertTrue(self.erp.rolled_back and not self.erp.committed)

    def test_duplicate_submit_protection(self):
        er.create_receive(self.req(client_token="abc"), "t", "")
        with self.assertRaises(ep.DuplicateSubmitError):
            er.create_receive(self.req(client_token="abc"), "t", "")
        with self.assertRaises(ep.DuplicateSubmitError):  # 無 token 但內容相同、60 秒內
            er.create_receive(self.req(), "t", "")

    def test_schema_overrides_use_real_columns(self):
        with self.assertRaises(ep.PoSchemaError):
            er._row("stkBillSub", NoSuchColumn=1)

    def test_open_lines_filters(self):
        r = er.list_open_lines()
        self.assertEqual([(l["po_no"], l["row_no"]) for l in r["lines"]], [("PO-1", 1), ("PO-1", 2), ("PO-2", 1), ("PO-X", 1)])
        self.assertEqual(r["lines"][0]["vendor_name"], "甲")
        self.assertIn("m.BillStatus = 0", self.erp.executed[-1][0])
        er.list_open_lines(vendor="CS007", q="a_b")
        sql, params = self.erp.executed[-1]
        self.assertIn("m.CustomerID = %s", sql)
        self.assertIn("%a[_]b%", params)


class ApiTests(ErpCase):
    def setUp(self):
        super().setUp()
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)

    def test_endpoints(self):
        cfg = self.client.get("/api/erp-receive/config").json()
        self.assertTrue(cfg["available"] and cfg["is_test"] and cfg["warehouse"] == "001")
        self.assertNotIn("secret-pw", str(cfg))
        self.assertEqual(len(self.client.get("/api/erp-receive/open-lines").json()["lines"]), 4)
        body = {"lines": [{"po_no": "PO-1", "row_no": 1, "qty": 100}]}
        pv = self.client.post("/api/erp-receive/preview", json=body)
        self.assertEqual(pv.status_code, 200, pv.text)
        ok = self.client.post("/api/erp-receive/create", json={**body, "client_token": "t1"})
        self.assertEqual(ok.status_code, 200, ok.text)
        self.assertEqual(ok.json()["rm_no"], "RM-20261030002")
        self.assertEqual(self.client.get("/api/erp-receive/history").json()["receipts"][0]["rm_no"], "RM-20261030002")
        dup = self.client.post("/api/erp-receive/create", json={**body, "client_token": "t1"})
        self.assertEqual(dup.status_code, 409)

    def test_over_receive_and_errors(self):
        body = {"lines": [{"po_no": "PO-2", "row_no": 1, "qty": 99}]}
        r = self.client.post("/api/erp-receive/create", json=body)
        self.assertEqual(r.status_code, 200)
        self.assertEqual((r.json()["ok"], r.json()["over_receive_warning"][0]["over"]), (False, 89.0))
        self.assertEqual(self.client.post("/api/erp-receive/create", json={**body, "confirm": True}).status_code, 200)
        self.assertEqual(self.client.post("/api/erp-receive/preview", json={"lines": []}).status_code, 400)
        self.assertEqual(self.client.post("/api/erp-receive/preview", json={"target": "CHIComp03", "lines": body["lines"]}).status_code, 400)
        self.erp.fail_on = "UPDATE dbo.comProduct"
        fail = self.client.post("/api/erp-receive/create", json={"lines": [{"po_no": "PO-1", "row_no": 1, "qty": 1}]})
        self.assertEqual(fail.status_code, 503)
        self.assertNotIn("secret-pw", fail.text)

    def test_edit_auth_protects_post_not_get(self):
        from starlette.requests import Request
        from app.services.edit_auth import request_requires_edit_auth

        def req(method, path):
            return Request({"type": "http", "method": method, "path": path, "headers": [], "query_string": b""})
        self.assertTrue(request_requires_edit_auth(req("POST", "/api/erp-receive/preview")))
        self.assertTrue(request_requires_edit_auth(req("POST", "/api/erp-receive/create")))
        self.assertFalse(request_requires_edit_auth(req("GET", "/api/erp-receive/open-lines")))


class StaticTests(unittest.TestCase):
    root = Path(__file__).resolve().parent.parent / "static"

    def test_picker_and_tab_present(self):
        picker = (self.root / "modules" / "open_line_picker.js").read_text(encoding="utf-8")
        self.assertIn("export function createOpenLinePicker", picker)
        self.assertNotIn("erp-receive", picker)
        recv = (self.root / "modules" / "erp_receive.js").read_text(encoding="utf-8")
        self.assertIn("/api/erp-receive/create", recv)
        self.assertIn("open_line_picker.js", recv)
        html = (self.root / "index.html").read_text(encoding="utf-8")
        self.assertIn('data-tab="erp-receive-tab"', html)
        self.assertIn('id="tab-erp-receive-tab"', html)
        self.assertIn("erp_receive.js", html)
        self.assertIn("一次收料", html)


# ── 成本 / 日期（照正航 comParams）────────────────────────────────────────────

class CostTests(ErpCase):
    def setup_params(self, by_ware, digits, dot):
        self.erp.tables["comParams"] = params(by_ware, digits, dot)

    def product(self, prod):
        return next(x for x in self.rows("comProduct") if x["ProdID"] == prod)

    def ware(self, prod, ware="001"):
        return next((x for x in self.rows("comWareAmount") if x["ProdID"] == prod and x["WareID"] == ware), None)

    def rec(self, prod):
        return next(x for x in self.rows("comProdRec") if x["ProdID"] == prod)

    def stock_company(self, prod, ctotal, avg, wares):
        """設定一個料號的全公司成本資料：wares = {WareID: Quantity}"""
        self.erp.tables["comProduct"] = [{"ProdID": prod, "CTotalCost": Decimal(ctotal), "CAvgCost": avg,
                                          "LatestPurchDate": 0, "LatestIndate": 11111111}]
        self.erp.tables["comWareAmount"] = [{"ProdID": prod, "WareID": w, "Quantity": Decimal(q), "TotalCost": Decimal(0),
                                             "Cost": Decimal(7), "BCQty": Decimal(q), "LatestPurchDate": 0, "LatestInDate": 22222222}
                                            for w, q in wares.items()]
        self.erp.tables["comWareHouse"] = [{"WareHouseID": w} for w in set(wares) | {"001"}]
        self.erp.tables["ordBillSub"] = [po_sub("PO-1", 1, prod, 100000, price=1)]
        self.erp.tables["StkYearMonthQty"] = []

    def receive(self, prod, qty, price):
        self.erp.tables["ordBillSub"] = [po_sub("PO-1", 1, prod, 10 ** 7, price=price)]
        return er.create_receive({"lines": [{"po_no": "PO-1", "row_no": 1, "qty": qty}], "confirm": True}, "t", "")

    def comp03_case(self, prod, old_q, old_ct, qty, price, expect_avg, other_wares=None):
        self.setup_params(0, 3, 1)
        wares = {"001": old_q}
        wares.update(other_wares or {})
        self.stock_company(prod, old_ct, 0.0, wares)
        self.receive(prod, qty, price)
        self.assertEqual(self.product(prod)["CAvgCost"], expect_avg)
        self.assertEqual(self.rec(prod)["CostAvg"], expect_avg)

    # Comp03 實際資料四個例子（CostByWare=0, 3 位）
    def test_comp03_examples(self):
        self.comp03_case("EC-30026A", 365, 33521, 1500, 26, 38.885)
        self.comp03_case("OC-10962B", 375, 2626, 1200, 5, 5.477)
        self.comp03_case("UC-M0005A", 487, 303387, 648, Decimal("405833") / 648, 624.863)
        self.comp03_case("OC-10803B", 0, 0, 22000, Decimal("10300") / 22000, 0.468)

    def test_company_wide_uses_all_warehouses_and_leaves_ware_cost(self):
        self.setup_params(0, 3, 1)
        self.stock_company("AC-1", 33521, 91.838, {"001": 100, "002": 265})
        self.receive("AC-1", 1500, 26)
        self.assertEqual(self.product("AC-1")["CAvgCost"], 38.885)  # 72521 / 1865
        self.assertEqual(self.product("AC-1")["CTotalCost"], Decimal(72521))
        w = self.ware("AC-1")
        self.assertEqual((w["Quantity"], w["TotalCost"], w["Cost"]), (Decimal(1600), Decimal(39000), Decimal(7)))  # Cost 不維護
        self.assertEqual(self.ware("AC-1", "002")["Quantity"], Decimal(265))
        self.assertEqual({(x["WareID"], x["AtLastQuan"]) for x in self.rows("stkYearMonthAvg")}, {("", Decimal(1500))})
        self.assertEqual(self.rows("StkYearMonthQty")[0]["WareID"], "001")  # 月庫存仍依倉庫

    def test_company_wide_new_ware_row_cost_zero_and_avg_row_accumulates(self):
        self.setup_params(0, 3, 1)
        self.stock_company("AC-1", 0, 0.0, {})
        self.erp.tables["stkYearMonthAvg"] = [{"YearMonth": 202610, "ProdID": "AC-1", "WareID": "", "BatchID": "",
                                               "AtLastQuan": Decimal(5)}]
        self.receive("AC-1", 10, 3)
        self.assertEqual(self.ware("AC-1")["Cost"], 0)
        self.assertEqual(self.rows("stkYearMonthAvg")[0]["AtLastQuan"], Decimal(15))
        self.assertEqual(self.product("AC-1")["CAvgCost"], 3.0)

    def test_company_wide_nonpositive_qty_keeps_avg_and_warns(self):
        self.setup_params(0, 3, 1)
        self.stock_company("AC-1", 500, 12.5, {"001": -50})
        r = self.receive("AC-1", 30, 2)  # -50 + 30 = -20
        self.assertEqual(self.product("AC-1")["CAvgCost"], 12.5)
        self.assertEqual(self.product("AC-1")["CTotalCost"], Decimal(560))
        self.assertEqual(self.rec("AC-1")["CostAvg"], 12.5)
        self.assertTrue(any(w["code"] == "cost_nonpositive_qty" for w in r["warnings"]))
        self.stock_company("AC-1", 500, 12.5, {"001": -31})
        r = self.receive("AC-1", 31, 2)  # 剛好 0
        self.assertEqual(self.product("AC-1")["CAvgCost"], 12.5)
        self.assertTrue(any(w["code"] == "cost_nonpositive_qty" for w in r["warnings"]))

    def test_company_wide_multiple_lines_same_product_aggregated(self):
        self.setup_params(0, 3, 1)
        self.stock_company("AC-1", 1000, 10.0, {"001": 100})
        self.erp.tables["ordBillSub"] = [po_sub("PO-1", 1, "AC-1", 1000, price=20), po_sub("PO-1", 2, "AC-1", 1000, price=30)]
        er.create_receive({"lines": [{"po_no": "PO-1", "row_no": 1, "qty": 10}, {"po_no": "PO-1", "row_no": 2, "qty": 20}]}, "t", "")
        # (1000 + 200 + 600) / (100 + 30) = 13.846...
        self.assertEqual(self.product("AC-1")["CAvgCost"], 13.846)
        self.assertEqual([x["CostAvg"] for x in self.rows("comProdRec")], [13.846, 13.846])
        self.assertEqual(len([u for u in self.erp.executed if u[0].startswith("UPDATE dbo.comProduct")]), 1)

    def test_per_ware_cost_and_company_avg_untouched(self):
        self.setup_params(1, 5, 0)
        self.erp.tables["comProduct"] = [{"ProdID": "AC-2", "CTotalCost": Decimal(5447), "CAvgCost": 23.996,
                                          "LatestPurchDate": 0, "LatestIndate": 1}]
        r = er.create_receive(self.req([{"po_no": "PO-1", "row_no": 2, "qty": 20}]), "t", "")  # AC-2: 10/100 + 20×24
        self.assertEqual(self.product("AC-2")["CAvgCost"], 23.996)
        self.assertEqual(self.product("AC-2")["CTotalCost"], Decimal(5927))
        self.assertEqual(self.ware("AC-2")["Cost"], 580 / 30)
        self.assertEqual(self.rec("AC-2")["CostAvg"], 580 / 30)
        self.assertEqual({(x["WareID"], x["AtLastQuan"]) for x in self.rows("stkYearMonthAvg")}, {("001", Decimal(20))})
        self.assertEqual(r["cost_params"], {"cost_by_ware": True, "digits": 5, "dot": False})

    def test_per_ware_new_row_and_rounding(self):
        self.setup_params(1, 3, 1)
        self.receive_new = er.create_receive(self.req([{"po_no": "PO-1", "row_no": 1, "qty": 3, "price": "10.0005"}]), "t", "")
        # 金額 round(3 × 10.0005) = 30 → 30/3 = 10
        self.assertEqual(self.ware("AC-1")["Cost"], 10.0)
        self.setup_params(1, 3, 1)
        self.erp.tables["comWareAmount"] = [{"ProdID": "AC-2", "WareID": "001", "Quantity": Decimal(10), "TotalCost": Decimal(100),
                                             "Cost": Decimal(10), "BCQty": Decimal(10)}]
        er.create_receive(self.req([{"po_no": "PO-1", "row_no": 2, "qty": 3, "price": 1}]), "t", "")  # 103 / 13 = 7.923076…
        self.assertEqual(self.ware("AC-2")["Cost"], 7.923)

    def test_rounding_half_up_vs_unrounded(self):
        params3 = {"cost_by_ware": False, "digits": 3, "dot": True}
        self.assertEqual(er._round_cost(Decimal("0.0005"), params3), 0.001)
        self.assertEqual(er._round_cost(Decimal("2.6665"), params3), 2.667)
        self.assertEqual(er._round_cost(Decimal("2.6664"), params3), 2.666)
        raw = {"cost_by_ware": True, "digits": 5, "dot": False}
        self.assertEqual(er._round_cost(Decimal(26113) / Decimal(407), raw), 26113 / 407)
        self.assertEqual(er._round_cost(Decimal(128048) / Decimal(492), raw), 128048 / 492)

    def test_per_ware_nonpositive_qty_keeps_cost_and_warns(self):
        self.setup_params(1, 5, 0)
        self.erp.tables["comWareAmount"] = [{"ProdID": "AC-2", "WareID": "001", "Quantity": Decimal(-50), "TotalCost": Decimal(-500),
                                             "Cost": Decimal(10), "BCQty": Decimal(0)}]
        r = er.create_receive(self.req([{"po_no": "PO-1", "row_no": 2, "qty": 20}]), "t", "")
        self.assertEqual(self.ware("AC-2")["Cost"], Decimal(10))
        self.assertEqual(self.rec("AC-2")["CostAvg"], 10.0)
        self.assertTrue(any(w["code"] == "cost_nonpositive_qty" for w in r["warnings"]))

    def test_latest_indate_never_updated(self):
        for by_ware in (0, 1):
            self.erp = make_erp()
            self.setup_params(by_ware, 3, 1)
            self.stock_company("AC-1", 100, 1.0, {"001": 10})
            with patch.object(ep, "_connect_write", lambda creds: self.erp.connect()):
                self.receive("AC-1", 5 + by_ware, 2)
            self.assertNotIn("latestindate", " ".join(s for s, _ in self.erp.executed if s.startswith("UPDATE")).lower())
            self.assertEqual(self.product("AC-1")["LatestIndate"], 11111111)
            self.assertEqual(self.ware("AC-1")["LatestInDate"], 22222222)
            self.assertEqual((self.product("AC-1")["LatestPurchDate"], self.ware("AC-1")["LatestPurchDate"]), (20261030, 20261030))

    def test_missing_or_bad_params_refused(self):
        for name in ("CostByWare", "ComCostDigs", "comCostDot"):
            self.erp.tables["comParams"] = [x for x in params(1, 5, 0) if x["ParamName"] != name]
            with self.assertRaisesRegex(er.ReceiveValidationError, f"找不到正航成本參數.*{name}"):
                er.preview_receive(self.req())
            with self.assertRaises(er.ReceiveValidationError):
                er.create_receive(self.req(), "t", "")
            self.assertFalse(self.erp.inserted("ordExamineMain"))
        self.erp.tables["comParams"] = [x for x in params(1, 5, 0)] + []
        self.erp.tables["comParams"][0]["paramValue"] = "2"
        with self.assertRaises(er.ReceiveValidationError):
            er.preview_receive(self.req())

    def test_user_specific_params_ignored(self):
        self.erp.tables["comParams"] = params(1, 5, 0) + [{"UserID": "andy", "ParamName": "CostByWare", "paramValue": "0"}]
        self.assertTrue(er.preview_receive(self.req())["cost_params"]["cost_by_ware"])


# ── 進貨單表頭照公司設定（v2026.10.30.10）──────────────────────────────────────

class BillParamTests(ErpCase):
    def stk(self, n):
        return next(x for x in self.rows("stkBillMain") if x["BillNO"] == n)

    def test_load_bill_params_by_company(self):
        cur = FakeCursor(self.erp)
        self.assertEqual(er.load_bill_params(cur, "CHIComp13"),
                         {"detail": "A", "bill_status": 1, "bill_status_source": "company", "create_acc_by_fund": ""})
        self.erp.tables["comParams"] = params(0, 3, 1, describe="") + [
            {"UserID": "", "ParamName": "CreateAccByFund", "paramValue": "1"}]
        self.assertEqual(er.load_bill_params(cur, "CHIComp03"),
                         {"detail": "", "bill_status": 0, "bill_status_source": "company", "create_acc_by_fund": "1"})
        # 沒列在 RM_BILL_STATUS_AT_CREATION 的公司：依 CreateAccByFund
        self.assertEqual(er.load_bill_params(cur, "CHIComp07")["bill_status"], 0)
        self.erp.tables["comParams"] = params(0, 3, 1)
        p = er.load_bill_params(cur, "CHIComp07")
        self.assertEqual((p["bill_status"], p["bill_status_source"], p["detail"]), (1, "CreateAccByFund", "A"))
        # 使用者層級的參數不算
        self.erp.tables["comParams"] = [x for x in params(0, 3, 1) if x["ParamName"] != "ComDefDescribe"] + [
            {"UserID": "andy", "ParamName": "ComDefDescribe", "paramValue": "Z"}]
        self.assertEqual(er.load_bill_params(cur, "CHIComp13")["detail"], "")

    def test_comp13_header_detail_a_status_1(self):
        r = er.create_receive(self.req(), "t", "")
        self.assertEqual((self.stk(r["rm_no"])["Detail"], self.stk(r["rm_no"])["BillStatus"]), ("A", 1))
        self.assertEqual(r["bill_params"]["bill_status"], 1)

    def test_comp03_style_header_and_po_close_unaffected(self):
        self.erp.tables["comParams"] = params(1, 5, 0, describe="") + [
            {"UserID": "", "ParamName": "CreateAccByFund", "paramValue": "1"}]
        with patch.dict(er.RM_BILL_STATUS_AT_CREATION, {"CHIComp13": 0}):
            r = er.create_receive(self.req([{"po_no": "PO-1", "row_no": 1, "qty": 100},
                                            {"po_no": "PO-1", "row_no": 2, "qty": 50}]), "t", "")
        stk = self.stk(r["rm_no"])
        self.assertEqual((stk["Detail"], stk["BillStatus"], stk["NotPayed"], stk["InCost"], stk["ShareType"]), ("", 0, 0, 0, 0))
        main = next(x for x in self.rows("ordBillMain") if x["BillNO"] == "PO-1")
        self.assertEqual((main["BillStatus"], main["EndacaseDate"]), (1, 20261030))  # 採購單結案照舊

    def test_fax_from_po(self):
        self.rows("ordBillMain")[0]["FaxNo"] = "06-2535027"
        r = er.create_receive(self.req(), "t", "")
        acc = next(x for x in self.rows("comBillAccounts") if x["FundBillNo"] == r["rm_no"])
        self.assertEqual(acc["FaxNo"], "06-2535027")


# ── v2026.10.30.11：正式公司 CHIComp03 開關、ERP_RECEIVE_TARGET、更新平均成本開關 ───────────

PROD_ON = {"ERP_RECEIVE_TARGET": "CHIComp03", "ERP_ALLOW_PROD_WRITE_CHICOMP03": "1",
           "ERP_SQL_WRITE_USER_CHICOMP03": "w03", "ERP_SQL_WRITE_PASSWORD_CHICOMP03": "prod-secret",
           "ERP_SQL_WRITE_SERVER_CHICOMP03": "10.0.0.3,1433"}


class ProductionTargetTests(ErpCase):
    def prod_env(self, **overrides):
        env = {**PROD_ON, **overrides}
        for k in [k for k, v in env.items() if v is None]:
            env.pop(k)
            os.environ.pop(k, None)
        return patch.dict(os.environ, env)

    def test_prod_write_allowed_with_flag_target_and_creds(self):
        self.erp.db_name = "CHIComp03"
        self.erp.tables["comParams"] = params(0, 3, 1, describe="")
        with self.prod_env():
            cfg = er.get_config()
            self.assertEqual((cfg["target"], cfg["label"], cfg["is_production"], cfg["available"]),
                             ("CHIComp03", "公司3（正式）", True, True))
            self.assertNotIn("prod-secret", str(cfg))
            creds = ep.write_credentials("CHIComp03")
            self.assertEqual((creds["user"], creds["server"], creds["port"], creds["database"]),
                             ("w03", "10.0.0.3", 1433, "CHIComp03"))
            pv = er.preview_receive(self.req())
            self.assertEqual((pv["target"], pv["target_label"], pv["is_production"]), ("CHIComp03", "公司3（正式）", True))
            r = er.create_receive(self.req(), "t", "")
            self.assertEqual(r["target"], "CHIComp03")
            stk = next(x for x in self.rows("stkBillMain") if x["BillNO"] == r["rm_no"])
            self.assertEqual((stk["BillStatus"], stk["Detail"]), (0, ""))  # Comp03 既有規則不變
            self.assertTrue(r["rm_no"].startswith("RM-"))
            # 開採購單仍寫 ERP_WRITE_TARGET（CHIComp13）
            self.assertEqual(ep.get_target(), "CHIComp13")
            self.assertEqual(ep.get_config()["target"], "CHIComp13")

    def test_prod_refused_without_each_requirement(self):
        self.erp.db_name = "CHIComp03"
        cases = [
            {"ERP_ALLOW_PROD_WRITE_CHICOMP03": None},
            {"ERP_ALLOW_PROD_WRITE_CHICOMP03": "0"},
            {"ERP_ALLOW_PROD_WRITE_CHICOMP03": "true"},  # 只接受 '1'
            {"ERP_RECEIVE_TARGET": None},  # 只靠 ERP_WRITE_TARGET / 前端 target 不能開正式公司
        ]
        for case in cases:
            with self.subTest(case=case), self.prod_env(**case):
                with self.assertRaises(er.ReceiveValidationError):
                    er.create_receive(self.req(target="CHIComp03"), "t", "")
                with self.assertRaises(er.ReceiveValidationError):
                    er.preview_receive(self.req(target="CHIComp03"))
        with self.prod_env(ERP_RECEIVE_TARGET=None, ERP_WRITE_TARGET="CHIComp03"):
            self.assertFalse(er.get_config()["available"])
            with self.assertRaises(er.ReceiveValidationError):
                er.create_receive(self.req(), "t", "")
        # 沒有專用帳密：不可退回測試帳（ERP_SQL_TEST_*）
        with self.prod_env(ERP_SQL_WRITE_USER_CHICOMP03=None, ERP_SQL_WRITE_PASSWORD_CHICOMP03=None):
            self.assertFalse(er.get_config()["available"])
            with self.assertRaises(ep.ErpNotConfigured):
                er.create_receive(self.req(), "t", "")
        self.assertEqual(self.erp.executed, [])

    def test_receive_target_on_test_company_refuses_requested_prod(self):
        with patch.dict(os.environ, {**PROD_ON, "ERP_RECEIVE_TARGET": "CHIComp13"}):
            self.assertEqual(er.get_config()["target"], "CHIComp13")
            with self.assertRaises(er.ReceiveValidationError):
                er.create_receive(self.req(target="CHIComp03"), "t", "")
            r = er.create_receive(self.req(), "t", "")
            self.assertEqual(r["target"], "CHIComp13")
        with patch.dict(os.environ, {"ERP_RECEIVE_TARGET": "bad"}):
            self.assertIn("ERP_RECEIVE_TARGET", er.get_config()["reason"])

    def test_over_receive_2582_of_2500_allowed_on_prod(self):
        self.erp.db_name = "CHIComp03"
        self.erp.tables["comParams"] = params(0, 3, 1, describe="")
        self.erp.tables["ordBillSub"] = [po_sub("PO-2", 1, "AC-1", 2500)]
        line = [{"po_no": "PO-2", "row_no": 1, "qty": 2582}]
        with self.prod_env():
            pv = er.preview_receive(self.req(line))
            self.assertTrue(pv["needs_confirm"])
            self.assertTrue(any(w["code"] == "over_receive" for w in pv["warnings"]))
            with self.assertRaises(er.OverReceiveWarning) as cm:
                er.create_receive(self.req(line), "t", "")
            self.assertEqual(cm.exception.items[0]["over"], 82.0)
            r = er.create_receive(self.req(line, confirm=True), "t", "")
        self.assertTrue(r["ok"])
        self.assertEqual(r["totals"]["sum_qty"], 2582.0)
        sub = next(x for x in self.rows("ordBillSub") if x["BillNO"] == "PO-2")
        self.assertEqual(sub["QtyRemain"], 0)


class UpdateCostFlagTests(ErpCase):
    def product(self, prod):
        return next(x for x in self.rows("comProduct") if x["ProdID"] == prod)

    def ware(self, prod):
        return next(x for x in self.rows("comWareAmount") if x["ProdID"] == prod)

    def test_default_on_and_env_off(self):
        self.assertTrue(er.get_config()["update_cost_default"])
        self.assertTrue(er.normalize_request(self.req())["update_cost"])
        for v in ("0", "false", "off", "no"):
            with patch.dict(os.environ, {"ERP_RECEIVE_UPDATE_COST": v}):
                self.assertFalse(er.get_config()["update_cost_default"])
                self.assertFalse(er.normalize_request(self.req())["update_cost"])
                self.assertTrue(er.normalize_request(self.req(update_cost=True))["update_cost"])
        with patch.dict(os.environ, {"ERP_RECEIVE_UPDATE_COST": "1"}):
            self.assertFalse(er.normalize_request(self.req(update_cost=False))["update_cost"])

    def test_skip_cost_company_wide(self):
        # CostByWare = 0（Comp03 方式）：CAvgCost / CTotalCost 不動，LatestPurchDate 與庫存量照常
        self.erp.tables["comParams"] = params(0, 3, 1)
        self.erp.tables["comProduct"] = [{"ProdID": "AC-2", "CTotalCost": Decimal(100), "CAvgCost": 10.0, "LatestPurchDate": 0}]
        r = er.create_receive(self.req([{"po_no": "PO-1", "row_no": 2, "qty": 50}], update_cost=False), "t", "")
        self.assertFalse(r["update_cost"])
        self.assertTrue(any(w["code"] == "cost_not_updated" for w in r["warnings"]))
        p = self.product("AC-2")
        self.assertEqual((p["CAvgCost"], p["CTotalCost"], p["LatestPurchDate"]), (10.0, Decimal(100), 20261030))
        w = self.ware("AC-2")
        self.assertEqual((w["Quantity"], w["TotalCost"], w["Cost"], w["LatestPurchDate"]),
                         (Decimal(60), Decimal(100), Decimal(10), 20261030))
        rec = next(x for x in self.rows("comProdRec") if x["BillNO"] == r["rm_no"])
        self.assertEqual(rec["CostAvg"], 10.0)  # 帶現有平均成本
        updates = " ".join(sql for sql, _ in self.erp.executed if sql.startswith("UPDATE dbo.comProduct")).lower()
        self.assertNotIn("cavgcost", updates)
        self.assertNotIn("ctotalcost", updates)
        self.assertIn("latestpurchdate", updates)

    def test_skip_cost_by_ware_and_new_ware_row(self):
        # CostByWare = 1（Comp13 方式）：倉庫 Cost / TotalCost 不動；新倉庫列 TotalCost = 0、Cost = 0
        r = er.create_receive(self.req([{"po_no": "PO-1", "row_no": 1, "qty": 100},
                                        {"po_no": "PO-1", "row_no": 2, "qty": 50}], update_cost=False), "t", "")
        w2 = self.ware("AC-2")
        self.assertEqual((w2["Quantity"], w2["TotalCost"], w2["Cost"]), (Decimal(60), Decimal(100), Decimal(10)))
        w1 = self.ware("AC-1")
        self.assertEqual((w1["Quantity"], w1["TotalCost"], w1["Cost"], w1["LatestPurchDate"]), (100, 0, 0, 20261030))
        p1 = self.product("AC-1")
        self.assertEqual((p1["CTotalCost"], p1["LatestPurchDate"]), (Decimal(5447), 20261030))
        recs = {x["ProdID"]: x["CostAvg"] for x in self.rows("comProdRec") if x["BillNO"] == r["rm_no"]}
        self.assertEqual(recs, {"AC-1": 0.0, "AC-2": 10.0})

    def test_cost_on_still_updates(self):
        r = er.create_receive(self.req([{"po_no": "PO-1", "row_no": 2, "qty": 50}], update_cost=True), "t", "")
        self.assertTrue(r["update_cost"])
        self.assertEqual(self.ware("AC-2")["TotalCost"], Decimal(1300))
        self.assertEqual(self.product("AC-2")["CTotalCost"], Decimal(6647))

    def test_api_param(self):
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        client = TestClient(app)
        with patch.dict(os.environ, {"ERP_RECEIVE_UPDATE_COST": "0"}):
            self.assertFalse(client.get("/api/erp-receive/config").json()["update_cost_default"])
            body = {"lines": [{"po_no": "PO-1", "row_no": 2, "qty": 50}]}
            pv = client.post("/api/erp-receive/preview", json=body).json()
            self.assertFalse(pv["update_cost"])
            pv = client.post("/api/erp-receive/preview", json={**body, "update_cost": True}).json()
            self.assertTrue(pv["update_cost"])
            r = client.post("/api/erp-receive/create", json={**body, "update_cost": False, "client_token": "c1"})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertFalse(r.json()["update_cost"])
        self.assertEqual(self.ware("AC-2")["TotalCost"], Decimal(100))
