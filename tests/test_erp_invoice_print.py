"""開 Invoice 列印（仿正航 KPM5-INVOICE，以 ST-2026304 列印結果為準）：對應、格式、-TAB 排除、分頁、唯讀。

本檔不連任何 ERP；假 cursor 依 SQL 回假資料，並記下所有查詢以確認只有 SELECT。
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services import erp_invoice as inv
from app.services import erp_invoice_print as prt
from app.services import erp_invoice_schema as schema
from app.services import erp_purchase as ep
from app.services import erp_stock
from app.services import invoice_print_pdf as ipp

ROOT = Path(__file__).resolve().parents[1]

DESC1 = ("IC, BUCK CONVERTER, 4.5V-16V INPUT, 6A, ADJUSTABLE OUTPUT, SYNCHRONOUS, WITH INTERNAL COMPENSATION, "
         "REFERENCE, MPS NB675GL-Z, QFN21 (3mm x 4mm)")
DESC2 = "IC, FBG484. Xilinx XC7K70T-1FBG484I. ---代購料"
# ST-2026304 正航實單的品名（Description 本身就截在 "(3mm x"）；正航列印折成 3 行 / 2 行
REAL_DESC1 = ("IC-NB675GL-Z IC, 10A, 5V-24V INPUT SYNCHRONOUS BUCK CONVERTER WITH 1.5A LDO AND LOW-NOISE "
              "REFERENCE, MPS NB675GL-Z, QFN21 (3mm x")
REAL_DESC2 = "IC-XC7K70T-1FBG484I IC, FBG484. Xilinx XC7K70T-1FBG484I. ---代購料"


def _main(**kw):
    row = {k: "" for k in prt.RAW_HEADER_FIELDS}
    row.update(BillNo="ST-2026304", Ref="INV-REF", IsFormal=True, Term=0, ValidDate=0, ExchRate=0, Total=0, SumQty=5900,
               BillDate=0, CustID="", CurrID="", CustName="", TradeWay="FOB")
    row.update(kw)
    return row


def _sub(row, item, qty, price, amount, *, desc="", sc="MO-2610001", sub_desc="", remark=""):
    return {"RowNo": row, "SerNo": row, "ItemNo": item, "Description": desc, "Quantity": qty, "Unit": "PCS",
            "Price": price, "Amount": amount, "SubDesc": sub_desc, "ItemRemark": remark, "OriginBillNo": sc,
            "FromNO": "", "POBillNO": ""}


class _Cur:
    """依 SQL 關鍵字回假資料；記下所有 SQL。"""

    def __init__(self, data, fail_company=False):
        self.data, self.fail_company, self.sqls, self._rows, self.description = data, fail_company, [], [], []

    def execute(self, sql, params=()):
        self.sqls.append(sql)
        d = self.data
        if sql.startswith("SELECT DB_NAME()"):
            rows, cols = [("CHIComp03",)], ["db"]
            self._set(cols, rows)
            return
        if "INFORMATION_SCHEMA" in sql:
            if self.fail_company:
                raise RuntimeError("no permission")
            self._dicts(d.get("info_schema", []))
        elif "FROM [" in sql:
            self._dicts(d.get("company_row", []))
        elif "FROM TraBillMain" in sql and f"Flag = {inv.SC_FLAG}" in sql:
            self._dicts([r for r in d.get("scs", []) if r["BillNo"] in params])
        elif "FROM TraBillMain" in sql:
            self._dicts([d["main"]] if d.get("main") and params[0] == d["main"]["BillNo"] else [])
        elif "FROM TraBillSub" in sql:
            self._dicts(d.get("subs", []))
        elif "FROM comProdRec" in sql:
            self._dicts(d.get("recs", []))
        elif "FROM comBillAccounts" in sql:
            self._dicts(d.get("accs", []))
        elif "FROM comCustomer" in sql:
            self._dicts(d.get("customers", []))
        elif "FROM comCustDesc" in sql:
            if d.get("cust_desc_error"):
                raise RuntimeError("Invalid object name 'comCustDesc'")
            self._dicts([r for r in d.get("cust_desc", []) if r["ID"] == params[0]])
        else:
            raise AssertionError(f"unexpected SQL {sql}")

    def _set(self, cols, rows):
        self.description = [(c,) for c in cols]
        self._rows = rows

    def _dicts(self, rows):
        cols = list(rows[0].keys()) if rows else ["x"]
        self._set(cols, [tuple(r.get(c) for c in cols) for r in rows])

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Conn:
    def __init__(self, cur):
        self.cur, self.rolled_back, self.closed, self.committed = cur, False, False, False

    def cursor(self):
        return self.cur

    def rollback(self):
        self.rolled_back = True

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def sample_data(**kw):
    data = {
        "main": _main(),
        "subs": [_sub(1, "IC-NB675GL-Z", 1, 0, 0, desc=DESC1, sc="MO-2610001"),
                 _sub(2, "IC-XC7K70T-1FBG484I", 1, 0, 0, desc=DESC2, sc="MO-2610001"),
                 _sub(3, "PCB-XYZ-TAB", 1, 0, 0, desc="tab", sc="MO-2610001")],
        "recs": [{"RowNO": 1, "Quantity": 5000, "Price": 0.88, "Amount": 4400},
                 {"RowNO": 2, "Quantity": 900, "Price": 90, "Amount": 81000},
                 {"RowNO": 3, "Quantity": 10, "Price": 1.5, "Amount": 15}],
        "accs": [{"BillDate": 20261116, "CustID": "C0152", "CurrID": "USD", "ExchRate": 31.505, "Total": 85415}],
        "customers": [{"ID": "C0152", "ShortName": "Open Text", "FullName": "Open Text Inc"}],
        "scs": [{"BillNo": "MO-2610001", "Ref": "4500059700", "CustBillNo": "CB-1"}],
    }
    data.update(kw)
    return data


def run_print(data, bill="ST-2026304", fail_company=False):
    cur = _Cur(data, fail_company)
    conn = _Conn(cur)
    prt._COMPANY_CACHE.clear()
    with patch.object(inv, "get_invoice_target", return_value="CHIComp03"), \
            patch.object(prt, "get_invoice_target", return_value="CHIComp03"), \
            patch.object(prt, "_connect_read", return_value=(conn, None)):
        result = prt.get_print_data(bill)
    return result, cur, conn


class FormatTests(unittest.TestCase):
    def test_numbers(self):
        self.assertEqual(prt.fmt_num(5000, 4), "5,000.0000")
        self.assertEqual(prt.fmt_num(0.88, 3), "0.880")
        self.assertEqual(prt.fmt_num(81000, 3), "81,000.000")
        self.assertEqual(prt.fmt_num(1234567.0005, 3), "1,234,567.001")

    def test_dates(self):
        self.assertEqual(prt.format_date(20261116), "Nov.16,2026")
        self.assertEqual(prt.format_date(20260105), "Jan.5,2026")
        expect = ["Jan.", "Feb.", "Mar.", "Apr.", "May.", "Jun.", "Jul.", "Aug.", "Sep.", "Oct.", "Nov.", "Dec."]
        for m, abbr in enumerate(expect, 1):
            self.assertEqual(prt.format_date(20260001 + m * 100), f"{abbr}1,2026")
        self.assertEqual(prt.format_date(0), "")

    def test_bill_no_validation(self):
        self.assertEqual(prt.normalize_bill_no(" st-2026304 "), "ST-2026304")
        for bad in ("", "304", "ST-26304", "ST-20263045", "MO-2026304", "ST2026304"):
            with self.assertRaises(inv.InvoiceValidationError, msg=bad):
                prt.normalize_bill_no(bad)

    def test_print_description(self):
        self.assertEqual(prt.print_description("A", "desc", ""), "A desc")
        self.assertEqual(prt.print_description("A", "desc ---代購料", "---代購料"), "A desc ---代購料")
        self.assertEqual(prt.print_description("A", "desc", "---代購料"), "A desc ---代購料")
        self.assertEqual(prt.print_description("A", "  x   y  ", ""), "A x   y")  # 內文空白不動

    def test_raw_header_fields_exist_in_schema(self):
        cols = set(schema.DEFAULTS["TraBillMain"])
        self.assertEqual([f for f in prt.RAW_HEADER_FIELDS if f not in cols], [])
        self.assertEqual([f for f in prt.SUB_FIELDS if f not in set(schema.DEFAULTS["TraBillSub"])], [])
        self.assertEqual([f for f in prt.SC_FIELDS if f not in cols], [])


class PrintDataTests(unittest.TestCase):
    def test_st_2026304_mapping(self):
        r, cur, conn = run_print(sample_data())
        self.assertEqual((r["bill_no"], r["title"], r["date_display"], r["date_raw"]),
                         ("ST-2026304", "銷貨單", "Nov.16,2026", 20261116))
        self.assertEqual(r["customer"], {"id": "C0152", "name": "Open Text Inc", "name_source": "comCustomer.FullName"})
        self.assertEqual((r["trade_term"], r["trade_term_source"], r["currency"]), ("FOB", "TraBillMain.TradeWay", "USD"))
        self.assertEqual(r["header_sources"]["date"], "comBillAccounts")
        l1, l2 = r["lines"]
        self.assertEqual((l1["qty_display"], l1["price_display"], l1["amount_display"]), ("5,000.0000", "0.880", "4,400.000"))
        self.assertEqual((l2["qty_display"], l2["price_display"], l2["amount_display"]), ("900.0000", "90.000", "81,000.000"))
        self.assertEqual(l2["print_description"], "IC-XC7K70T-1FBG484I " + DESC2)
        self.assertEqual((l1["po"], l1["po_source"], l1["qty_source"]), ("4500059700", "sc.Ref", "comProdRec"))
        t = r["totals"]
        self.assertEqual((t["qty_display"], t["amount_display"]), ("5,900.0000", "85,400.000"))
        self.assertEqual((t["full_qty"], t["full_amount"], t["main_total"]), (5910, 85415, 85415))
        self.assertTrue(t["matches_main_total"])
        self.assertEqual(r["signatures"], ["簽核", "審核", "製單"])
        self.assertEqual(r["company"]["company_source"], "fallback")
        self.assertEqual(r["company"]["name"], "Songtech Enterprise Co., Ltd.")
        self.assertEqual(set(r["raw_header"]), set(prt.RAW_HEADER_FIELDS))
        self.assertEqual(len(r["raw_sc"]), 1)
        self.assertEqual(set(r["raw_sc"][0]), set(prt.RAW_SC_FIELDS))
        self.assertEqual((r["raw_sc"][0]["BillNo"], r["raw_sc"][0]["Ref"]), ("MO-2610001", "4500059700"))
        self.assertEqual(r["transport_way"], "")
        self.assertNotIn("trade_term_probe", r)
        self.assertNotIn("trade_term_sc_no", r)
        self.assertNotIn("trade_term_unmapped_term", r)
        self.assertTrue(conn.rolled_back and conn.closed and not conn.committed)

    def test_tab_rows_excluded_and_reported(self):
        r, _, _ = run_print(sample_data())
        self.assertEqual([l["item_no"] for l in r["lines"]], ["IC-NB675GL-Z", "IC-XC7K70T-1FBG484I"])
        self.assertEqual(r["excluded_tab_rows"], [{"row_no": 3, "item_no": "PCB-XYZ-TAB", "qty": 10, "amount": 15}])
        self.assertEqual((r["excluded_tab_qty"], r["excluded_tab_amount"]), (10, 15))
        self.assertEqual((r["full_total_qty"], r["full_total_amount"]), (5910, 85415))
        self.assertTrue(prt.is_tab_item("abc-tab"))
        self.assertFalse(prt.is_tab_item("TAB-1"))

    def test_po_grouping_and_fallbacks(self):
        subs = [_sub(1, "A", 1, 1, 1, sc="MO-1"), _sub(2, "B", 1, 1, 1, sc="MO-1"), _sub(3, "C", 1, 1, 1, sc="MO-2"),
                _sub(4, "D", 1, 1, 1, sc="MO-9"), {**_sub(5, "E", 1, 1, 1, sc=""), "FromNO": "MO-2"}]
        scs = [{"BillNo": "MO-1", "Ref": "PO-A", "CustBillNo": "X"}, {"BillNo": "MO-2", "Ref": "", "CustBillNo": "CB-2"}]
        r, _, _ = run_print(sample_data(subs=subs, recs=[], scs=scs))
        self.assertEqual([(l["po"], l["po_source"]) for l in r["lines"]],
                         [("PO-A", "sc.Ref"), ("PO-A", "sc.Ref"), ("CB-2", "sc.CustBillNo"),
                          ("INV-REF", "invoice.Ref"), ("CB-2", "sc.CustBillNo")])
        rows = r["pages"][0]["rows"]
        # 同 PDF / 正航：PO 非空的每一列都印 PO#（同一 PO 連續多列也每列都印）
        self.assertEqual([x.get("po") for x in rows if x["type"] == "po"], ["PO-A", "PO-A", "CB-2", "INV-REF", "CB-2"])
        self.assertEqual(r["lines"][0]["qty_source"], "TraBillSub")

    def test_amount_computed_and_ordering(self):
        subs = [_sub(1, "A", 3, 0.3335, 0)]
        r, cur, _ = run_print(sample_data(subs=subs, recs=[], scs=[]))
        self.assertEqual((r["lines"][0]["amount"], r["lines"][0]["amount_computed"]), (1.001, True))
        self.assertIn("ORDER BY RowNo, SerNo", next(s for s in cur.sqls if "FROM TraBillSub" in s))
        self.assertFalse(r["totals"]["matches_main_total"])  # 85415 vs 1.001

    def test_customer_name_and_header_from_main(self):
        main = _main(CustName="Open Text Inc.", CustID="C0152", BillDate=20260301, CurrID="NTD", Total=85415,
                     TradeWay="", Way="", Shipment="", TermContent="CIF HONG KONG")
        r, _, _ = run_print(sample_data(main=main))
        self.assertEqual((r["customer"]["name"], r["customer"]["name_source"]), ("Open Text Inc.", "TraBillMain.CustName"))
        self.assertEqual((r["date_display"], r["currency"]), ("Mar.1,2026", "NTD"))
        self.assertEqual((r["trade_term"], r["trade_term_source"]), ("CIF HONG KONG", "TraBillMain.TermContent"))
        r2, _, _ = run_print(sample_data(customers=[{"ID": "C0152", "ShortName": "Open Text", "FullName": ""}]))
        self.assertEqual((r2["customer"]["name"], r2["customer"]["name_source"]), ("Open Text", "comCustomer.ShortName"))

    def test_customer_english_name_from_cust_desc(self):
        # 正航 Invoice 印客戶英文全名 comCustDesc.EngFullName：C0034 FullName 'NEXTEK  LLC'、英文名 'NEXTEK  INC'
        accs = [{"BillDate": 20260130, "CustID": "C0034", "CurrID": "USD", "ExchRate": 31.5, "Total": 85415}]
        customers = [{"ID": "C0034", "ShortName": "NEXTEK  LLC", "FullName": "NEXTEK  LLC"}]
        cust_desc = [{"ID": "C0034", "EngFullName": "NEXTEK  INC  "}]
        r, cur, _ = run_print(sample_data(accs=accs, customers=customers, cust_desc=cust_desc))
        self.assertEqual((r["customer"]["name"], r["customer_name_source"]), ("NEXTEK  INC", "comCustDesc.EngFullName"))
        self.assertEqual(r["customer"]["name_source"], "comCustDesc.EngFullName")
        sql = next(q for q in cur.sqls if "comCustDesc" in q)
        self.assertIn(f"SELECT TOP 1 ID, EngFullName FROM comCustDesc WHERE Flag = {inv.CUSTOMER_FLAG} AND ID = %s", sql)
        # TraBillMain.CustName 仍優先
        r, _, _ = run_print(sample_data(main=_main(CustName="Main Name"), accs=accs, customers=customers,
                                        cust_desc=cust_desc))
        self.assertEqual((r["customer"]["name"], r["customer_name_source"]), ("Main Name", "TraBillMain.CustName"))

    def test_customer_english_name_fallbacks(self):
        accs = [{"BillDate": 20260130, "CustID": "C0034", "CurrID": "USD", "ExchRate": 31.5, "Total": 85415}]
        customers = [{"ID": "C0034", "ShortName": "NEXTEK", "FullName": "NEXTEK  LLC"}]
        for extra in ({"cust_desc": [{"ID": "C0034", "EngFullName": "  "}]}, {"cust_desc": []},
                      {"cust_desc_error": True}):  # 空白、沒資料、表不存在（測試公司）→ 略過
            r, _, conn = run_print(sample_data(accs=accs, customers=customers, **extra))
            self.assertEqual((r["customer"]["name"], r["customer_name_source"]), ("NEXTEK  LLC", "comCustomer.FullName"),
                             extra)
            self.assertTrue(conn.rolled_back and conn.closed and not conn.committed)

    def test_trade_term_from_term_enum(self):
        sc = [{"BillNo": "MO-2610001", "Ref": "4500059700", "CustBillNo": "", "TradeWay": "CIF", "Shipment": "CFR X",
               "TermContent": "DAP"}]
        # ST-2026304 實單：TradeWay / Shipment / TermContent 都空、Way=Air、Term=0 → 正航印 FOB
        r, _, _ = run_print(sample_data(main=_main(TradeWay="", Shipment="", TermContent="", Way="Air", Term=0), scs=sc))
        self.assertEqual((r["trade_term"], r["trade_term_source"]), ("FOB", "TraBillMain.Term=0 (inferred FOB)"))
        self.assertNotIn("trade_term_unmapped_term", r)
        self.assertEqual(r["transport_way"], "Air")
        self.assertEqual(r["raw_sc"][0]["TradeWay"], "CIF")  # S/C 值只回 raw_sc，不拿來當貿易條件

    def test_trade_term_priority(self):
        r, _, _ = run_print(sample_data(main=_main(TradeWay="CIF", TermContent="DAP", Term=0)))
        self.assertEqual((r["trade_term"], r["trade_term_source"]), ("CIF", "TraBillMain.TradeWay"))
        r, _, _ = run_print(sample_data(main=_main(TradeWay="  ", TermContent="EXW TAIPEI", Term=0)))
        self.assertEqual((r["trade_term"], r["trade_term_source"]), ("EXW TAIPEI", "TraBillMain.TermContent"))

    def test_trade_term_unmapped_and_way_shipment_ignored(self):
        main = _main(TradeWay="", TermContent="", Shipment="CIF HONG KONG", Way="Air", Term=2)
        r, _, _ = run_print(sample_data(main=main))
        self.assertEqual((r["trade_term"], r["trade_term_source"], r["trade_term_unmapped_term"]), ("", "none", 2))
        for term in (None, ""):
            r, _, _ = run_print(sample_data(main=_main(TradeWay="", Shipment="FOB", Way="FOB", Term=term)))
            self.assertEqual((r["trade_term"], r["trade_term_source"]), ("", "none"))
            self.assertNotIn("trade_term_unmapped_term", r)

    def test_only_select_statements(self):
        _, cur, _ = run_print(sample_data())
        self.assertTrue(cur.sqls)
        for sql in cur.sqls:
            self.assertTrue(sql.lstrip().upper().startswith("SELECT"), sql)
            for word in ("INSERT", "UPDATE ", "DELETE", "MERGE", "EXEC", "DROP", "ALTER", "TRUNCATE", "WITH (UPDLOCK"):
                self.assertNotIn(word, sql.upper(), sql)

    def test_not_found(self):
        with self.assertRaises(prt.InvoiceNotFound):
            run_print(sample_data(main=None))


def _fixture(bill_no):
    return json.loads((ROOT / "tests" / "fixtures" / f"{bill_no}.json").read_text(encoding="utf-8"))


class PaginationTests(unittest.TestCase):
    """pages / print_pages 同伺服器 PDF（invoice_print_pdf.layout）：頁數 = PDF page_count。"""

    def setUp(self):
        patcher = patch.dict(os.environ, {"INVOICE_PRINT_SIGN_MODE": "", "RM_PRINT_HALF_PAGE_MM": "148.5"})
        patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def _lines(n, po="PO1", desc="x"):
        return [{"row_no": i + 1, "po": po, "print_description": desc, "qty": 1, "price": 1, "amount": 1}
                for i in range(n)]

    def test_page_count_equals_pdf(self):
        for bill_no, form, count in (("ST-2026019", "a4", 2), ("ST-2026304", "half", 1), ("ST-2026307", "a4", 1)):
            data = prt.add_print_layout(_fixture(bill_no))
            lay = ipp.layout(data)
            self.assertEqual((data["print_form"], len(data["pages"]), len(data["print_pages"])), (form, count, count), bill_no)
            self.assertEqual(lay["page_count"], count, bill_no)
            self.assertEqual([p["of"] for p in data["pages"]], [count] * count)
            self.assertEqual([p["last"] for p in data["pages"]], [False] * (count - 1) + [True])

    def test_st_2026019_print_pages(self):
        data = prt.add_print_layout(_fixture("ST-2026019"))
        pages = data["print_pages"]
        self.assertEqual([len(p["lines"]) for p in pages], [14, 6])
        self.assertEqual([sum(r["type"] == "po" for r in p["rows"]) for p in data["pages"]], [14, 6])
        last1 = pages[0]["lines"][-1]
        self.assertEqual((last1["desc"], last1["qty"]), (["AHXAAAX24-000 P/N:381-0001, Outer Contact (BeCu consign)"], "86.0000"))
        pdf_po = [[round(e["base"], 3) for e in p["elements"] if e["type"] == "po"] for p in ipp.layout(data)["pages"]]
        self.assertEqual([[l["po_base"] for l in p["lines"]] for p in pages], pdf_po)  # 同 PDF 的 PO# 基線
        self.assertEqual(pages[1]["lines"][0]["desc"], ["70MB0V27X-006 P/N: 330-0253-02 CONNECTOR, Press-In",
                                                         "SMA(F), Solder dipped and 24AWG wire installed, with Lead", 'L=0.4"'])
        # 簽核每一頁都有位置：非最後一頁在頁尾 band，最後一頁緊接合計；合計只在最後一頁
        self.assertEqual((pages[0]["sign_base"], pages[0]["frame_bottom"], pages[0]["last"]), (273.85, 282.8, False))
        self.assertNotIn("totals_base", pages[0])
        self.assertTrue(pages[1]["last"])
        self.assertAlmostEqual(pages[1]["sign_base"], pages[1]["totals_base"] + ipp.SIGN_GAP, places=2)
        self.assertEqual([l["index"] for p in pages for l in p["lines"]], list(range(20)))
        g = data["print_geometry"]
        self.assertEqual((g["amount_x"], g["price_right"], g["qty_right"], g["desc_x"]), (176.5, 170.74, 32.65, 44.07))
        self.assertAlmostEqual(g["size"], 9.96 * 25.4 / 72)
        self.assertAlmostEqual(g["term_x"], ipp.PRICE_RIGHT - ipp._plain_width("FOB "))

    def test_st_2026304_half_wraps_like_pdf(self):
        data = prt.add_print_layout(_fixture("ST-2026304"))
        self.assertEqual(data["print_form"], "half")
        self.assertEqual([len(l["desc"]) for l in data["print_pages"][0]["lines"]], [3, 2])
        self.assertEqual([(r["type"], r["height"]) for r in data["pages"][0]["rows"]],
                         [("po", 1), ("line", 3), ("po", 1), ("line", 2)])
        self.assertLessEqual(data["print_pages"][0]["frame_bottom"], 148.5)

    def test_paginate_matches_layout(self):
        for n in (2, 20, 40, 60):
            lines = self._lines(n)
            pages = prt.paginate(lines)
            self.assertEqual(len(pages), ipp.layout({"lines": lines})["page_count"], n)
            self.assertEqual(sum(1 for p in pages for r in p["rows"] if r["type"] == "line"), n)
            self.assertEqual([r["row_no"] for p in pages for r in p["rows"] if r["type"] == "line"], list(range(1, n + 1)))
        self.assertEqual(len(prt.paginate(self._lines(2))), 1)
        self.assertGreater(len(prt.paginate(self._lines(40))), 1)

    def test_po_header_not_orphaned(self):
        for p in prt.paginate(self._lines(11, "P1") + self._lines(30, "P2")):
            self.assertEqual(p["rows"][-1]["type"], "line")
            self.assertEqual(p["rows"][0]["type"], "po")


class PrintDataResponseTests(unittest.TestCase):
    def test_no_page_probe(self):
        r, _, conn = run_print(sample_data())
        self.assertTrue(r["ok"])
        self.assertNotIn("page_probe", r)
        self.assertTrue(conn.rolled_back)


class CompanyLookupTests(unittest.TestCase):
    def setUp(self):
        prt._COMPANY_CACHE.clear()

    def test_introspection_failure_falls_back(self):
        r, _, _ = run_print(sample_data(), fail_company=True)
        self.assertEqual(r["company"]["company_source"], "fallback")
        self.assertEqual(r["company"]["tel"], "886 2 82261276")

    def test_nothing_found_falls_back(self):
        r, _, _ = run_print(sample_data(info_schema=[{"TABLE_NAME": "comCompany", "COLUMN_NAME": "CompanyName"}]))
        self.assertEqual(r["company"]["company_source"], "fallback")

    def test_single_table_used_per_piece(self):
        info = [{"TABLE_NAME": "comCompanyInfo", "COLUMN_NAME": c} for c in ("CompanyName", "CompEngName", "EngAddress", "Telphone", "Fax")]
        row = [{"CompEngName": "Songtech Ent. DB", "EngAddress": "DB Addr", "Telphone": "", "Fax": "999"}]
        r, cur, _ = run_print(sample_data(info_schema=info, company_row=row))
        c = r["company"]
        self.assertEqual((c["name"], c["address"], c["tel"], c["fax"]), ("Songtech Ent. DB", "DB Addr", "886 2 82261276", "999"))
        self.assertEqual(c["company_source"], "db:comCompanyInfo")
        self.assertEqual(c["pieces"]["tel"], "fallback")
        self.assertIn("SELECT TOP 1 [CompEngName], [EngAddress], [Telphone], [Fax] FROM [comCompanyInfo]", cur.sqls)

    def test_cached(self):
        cur = _Cur({}, fail_company=False)
        prt.lookup_company(cur, "CHIComp03")
        n = len(cur.sqls)
        prt.lookup_company(cur, "CHIComp03")
        self.assertEqual(len(cur.sqls), n)


class ReadConnectionTests(unittest.TestCase):
    def test_uses_read_account_when_source_is_target(self):
        settings = {"database": "CHIComp03", "user": "ro", "password": "x"}
        with patch.object(erp_stock, "get_settings", return_value=settings), \
                patch.object(erp_stock, "is_configured", return_value=True), \
                patch.object(erp_stock, "_connect", return_value="RO") as ro, \
                patch.object(ep, "_connect_write", side_effect=AssertionError("不可用寫入帳號")):
            self.assertEqual(prt._connect_read("CHIComp03"), ("RO", None))
        ro.assert_called_once()

    def test_not_configured(self):
        with patch.object(erp_stock, "get_settings", return_value={"database": "CHIComp03"}), \
                patch.object(ep, "write_credentials", return_value=None):
            with self.assertRaises(erp_stock.ErpNotConfigured):
                prt._connect_read("CHIComp13")

    def test_does_not_depend_on_write_switch(self):
        with patch.object(inv, "ensure_writable_target", side_effect=AssertionError("不可檢查寫入開關")):
            run_print(sample_data())


class PrintRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from fastapi.testclient import TestClient
        from main import app
        cls.app = app
        cls.client = TestClient(app)

    def test_registered(self):
        self.assertIn("/api/erp-invoice/print-data", {r.path for r in self.app.routes})

    def test_bad_format_400(self):
        res = self.client.get("/api/erp-invoice/print-data?bill_no=304")
        self.assertEqual(res.status_code, 400)

    def test_not_found_404(self):
        with patch.object(prt, "get_print_data", side_effect=prt.InvoiceNotFound("正航找不到 Invoice ST-2026999")):
            res = self.client.get("/api/erp-invoice/print-data?bill_no=ST-2026999")
        self.assertEqual(res.status_code, 404)
        self.assertIn("找不到", res.json()["detail"])

    def test_unavailable_503(self):
        with patch.object(prt, "get_print_data", side_effect=erp_stock.ErpNotConfigured("x")):
            res = self.client.get("/api/erp-invoice/print-data?bill_no=ST-2026304")
        self.assertEqual(res.status_code, 503)

    def test_ok(self):
        with patch.object(prt, "get_print_data", return_value={"ok": True, "bill_no": "ST-2026304"}) as fn:
            res = self.client.get("/api/erp-invoice/print-data?bill_no=st-2026304")
        self.assertEqual(res.status_code, 200)
        fn.assert_called_once_with("st-2026304")

    def test_static_page_served(self):
        res = self.client.get("/static/erp_invoice_print.html")
        self.assertEqual(res.status_code, 200)
        self.assertIn("erp_invoice_print.js", res.text)


class PrintFrontendTests(unittest.TestCase):
    def test_files_contain_key_strings(self):
        html = (ROOT / "static" / "erp_invoice_print.html").read_text(encoding="utf-8")
        for s in ("@page { size: A4 portrait; margin: 0; }", 'id="btn-print"', ">列印<", ">關閉<", "page-break-after",
                  "height: 297mm", "white-space: pre", '/static/modules/erp_invoice_print.js'):
            self.assertIn(s, html)
        js = (ROOT / "static" / "modules" / "erp_invoice_print.js").read_text(encoding="utf-8")
        for s in ("export function renderInvoicePrint(data, root)", "window.__INVOICE_PRINT_DATA__",
                  "/api/erp-invoice/print-data", 'params.get("auto") === "1"', "window.print()", "客戶名稱",
                  "data.print_pages", "data.print_geometry", 'viewBox="0 0 210 297"', "getComputedTextLength",
                  "document.fonts"):
            self.assertIn(s, js)
        # 不再自己估高度分頁（分頁、折行都照後端 print_pages = 伺服器 PDF）
        for s in ("paginateUnits", "estimateHeights", "measureLayout", "PAGE_HEIGHT_MM", "data.pages"):
            self.assertNotIn(s, js)
        page = (ROOT / "static" / "modules" / "erp_invoice.js").read_text(encoding="utf-8")
        for s in ("data-print-bill", "列印 ${esc(r.bill_no)}", "/api/erp-invoice/print-info"):
            self.assertIn(s, page)
        index = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        self.assertIn('placeholder="輸入 ST-2026xxx"', index)

    def test_render_with_node(self):
        with patch.dict(os.environ, {"INVOICE_PRINT_SIGN_MODE": "", "RM_PRINT_HALF_PAGE_MM": "148.5"}):
            fixtures = {b: prt.add_print_layout(_fixture(b)) for b in ("ST-2026019", "ST-2026304")}
        script = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src = fs.readFileSync('./static/modules/erp_invoice_print.js', 'utf8');
const mod = await import('data:text/javascript,' + encodeURIComponent(src));
const fx = JSON.parse(fs.readFileSync(process.env.INV_FIXTURES, 'utf8'));
assert.equal(mod.fmtNum(5000, 4), '5,000.0000');
assert.equal(mod.fmtNum(85400, 3), '85,400.000');
const root = { innerHTML: '' };
const sheets = html => html.split('<section class="inv-sheet"').slice(1);
const count = (s, re) => (s.match(re) || []).length;
// ST-2026019：照 print_pages 畫 2 頁（14 / 6 個 PO#），簽核每一頁都有，合計只在最後一頁
const d19 = fx['ST-2026019'];
assert.equal(mod.renderInvoicePrint(d19, root), 2);
const [p1, p2] = sheets(root.innerHTML);
assert.equal(count(p1, /class="inv-po"/g), 14);
assert.equal(count(p2, /class="inv-po"/g), 6);
for (const p of [p1, p2]) {
  assert.equal(count(p, /class="inv-sign"/g), 3);
  for (const s of ['簽核 ', '審核 ', '製單 ', '_'.repeat(18), '客戶名稱: NEXTEK  LLC', 'UNIT PRICE', 'TTL AMOUNT', '>FOB<']) assert.ok(p.includes(s), s);
}
assert.ok(p1.includes('y="273.85" font-size') && p1.includes('Page 1 of 2'));
assert.ok(p2.includes('Page 2 of 2') && p2.includes('USD') && p2.includes('22,141.0000') && p2.includes('30,176.670'));
assert.ok(!p1.includes('22,141.0000'));
assert.ok(p1.includes('>AHXAAAX24-000 P/N:381-0001, Outer Contact (BeCu consign)<') && p1.includes('>86.0000<'));
assert.ok(p2.includes('>L=0.4&quot;<'));  // PDF 折好的第 3 行
// TTL AMOUNT 靠左 x=176.5（不是靠右）；單價 / 數量靠右
assert.ok(p1.includes('<text x="176.5" y="86.78" font-size="3.5137" class="inv-amt">6,388.480</text>'.replace('3.5137', String(d19.print_geometry.size))));
assert.ok(!/class="inv-amt"[^>]*text-anchor/.test(root.innerHTML) && !/text-anchor="end"[^>]*class="inv-amt"/.test(root.innerHTML));
assert.ok(/x="170.74"[^>]*text-anchor="end"[^>]*class="inv-price">4.960</.test(p1));
// 外粗框 + 內細框到框底；非最後一頁框到 282.8，最後一頁框在簽核下方收尾
assert.ok(p1.includes(`height="${282.8 - 53.6}"`));
assert.equal(count(p1, /<rect /g), 2);
// HTML 跳脫
const bad = { ...d19, customer: { name: 'A <b>&' } };
mod.renderInvoicePrint(bad, root);
assert.ok(root.innerHTML.includes('客戶名稱: A &lt;b&gt;&amp;'));
// ST-2026304：半張 1 頁，品名照 PDF 折成 3 行 / 2 行
assert.equal(mod.renderInvoicePrint(fx['ST-2026304'], root), 1);
assert.equal(count(root.innerHTML, /class="inv-desc"/g), 5);
assert.ok(root.innerHTML.includes('>---代購料<'));
// 沒有 print_pages（舊伺服器）就報錯，不自己估
assert.throws(() => mod.renderInvoicePrint({ ...d19, print_pages: undefined }, root), /print_pages/);
const page = fs.readFileSync('./static/modules/erp_invoice.js', 'utf8').replace(/^import .*;\r?\n/gm, '');
const inv = await import('data:text/javascript,' + encodeURIComponent(page));
assert.equal(inv.printUrl(' st-2026304 '), '/static/erp_invoice_print.html?bill=ST-2026304');
assert.equal(inv.printUrl('ST-202630'), null);
assert.equal(inv.printUrl(''), null);
"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fixtures.json"
            path.write_text(json.dumps(fixtures, ensure_ascii=False), encoding="utf-8")
            res = subprocess.run(["node", "--input-type=module", "-e", script], cwd=ROOT, capture_output=True, text=True,
                                 encoding="utf-8", env={**os.environ, "INV_FIXTURES": str(path)})
        self.assertEqual(res.returncode, 0, res.stderr)


if __name__ == "__main__":
    unittest.main()
