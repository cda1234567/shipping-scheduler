"""開 Invoice 列印（仿正航 KPM5-INVOICE，以 ST-2026304 列印結果為準）：對應、格式、-TAB 排除、分頁、唯讀。

本檔不連任何 ERP；假 cursor 依 SQL 回假資料，並記下所有查詢以確認只有 SELECT。
"""
from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services import erp_invoice as inv
from app.services import erp_invoice_print as prt
from app.services import erp_invoice_schema as schema
from app.services import erp_purchase as ep
from app.services import erp_stock

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
        self.assertEqual([x.get("po") for x in rows if x["type"] == "po"], ["PO-A", "CB-2", "INV-REF", "CB-2"])
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


class PaginationTests(unittest.TestCase):
    @staticmethod
    def _lines(n, po="PO1", desc="x"):
        return [{"row_no": i + 1, "po": po, "print_description": desc} for i in range(n)]

    def test_25_lines_three_pages(self):
        pages = prt.paginate(self._lines(25))
        self.assertEqual(len(pages), 3)
        self.assertEqual([p["last"] for p in pages], [False, False, True])
        self.assertEqual([p["of"] for p in pages], [3, 3, 3])
        self.assertEqual(sum(1 for p in pages for r in p["rows"] if r["type"] == "line"), 25)

    def test_two_lines_one_page_and_clamp(self):
        self.assertEqual(len(prt.paginate(self._lines(2))), 1)
        self.assertEqual(prt.desc_line_count("x" * 1000), 3)
        self.assertEqual(prt.desc_line_count("IC-XC7K70T-1FBG484I " + DESC2), 2)

    def test_st_2026304_real_heights(self):
        lines = [{"row_no": 1, "po": "4500059700", "print_description": REAL_DESC1},
                 {"row_no": 2, "po": "4500059703", "print_description": REAL_DESC2}]
        pages = prt.paginate(lines)
        self.assertEqual(len(pages), 1)
        self.assertEqual([(r["type"], r["height"]) for r in pages[0]["rows"]],
                         [("po", 1), ("line", 3), ("po", 1), ("line", 2)])

    def test_po_header_not_orphaned(self):
        lines = self._lines(11, "P1") + self._lines(1, "P2")
        pages = prt.paginate(lines)  # 1 + 11 = 12 滿一頁，P2 標題與它的明細一起到下一頁
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[1]["rows"][0], {"type": "po", "po": "P2", "height": 1})


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
        for s in ("@page { size: A4 portrait; margin: 10mm; }", 'id="btn-print"', ">列印<", ">關閉<", "page-break-after",
                  "max-height: calc(1.35em * 3)", "3px double",
                  "grid-template-columns: 146fr 5fr 342fr 110fr 94fr", "letter-spacing: -0.07em; word-spacing: 0.15em;", '/static/modules/erp_invoice_print.js'):
            self.assertIn(s, html)
        self.assertNotIn("line-clamp", html)  # 不用 line-clamp（會加省略號），超過 3 行直接裁掉
        js = (ROOT / "static" / "modules" / "erp_invoice_print.js").read_text(encoding="utf-8")
        for s in ("export function renderInvoicePrint(data, root)", "window.__INVOICE_PRINT_DATA__",
                  "/api/erp-invoice/print-data", 'params.get("auto") === "1"', "window.print()", "客戶名稱"):
            self.assertIn(s, js)
        page = (ROOT / "static" / "modules" / "erp_invoice.js").read_text(encoding="utf-8")
        for s in ('window.open(url, "_blank", "noopener")', "data-print-bill", "列印 ${esc(r.bill_no)}"):
            self.assertIn(s, page)
        index = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        self.assertIn('placeholder="輸入 ST-2026xxx"', index)

    def test_render_with_node(self):
        script = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src = fs.readFileSync('./static/modules/erp_invoice_print.js', 'utf8');
const mod = await import('data:text/javascript,' + encodeURIComponent(src));
assert.equal(mod.fmtNum(5000, 4), '5,000.0000');
assert.equal(mod.fmtNum(85400, 3), '85,400.000');
const data = { bill_no: 'ST-2026304', title: '銷貨單', date_display: 'Nov.16,2026', currency: 'USD', trade_term: 'FOB',
  company: { name: 'Songtech Enterprise Co., Ltd.', address: 'addr', tel: '1', fax: '2' }, customer: { name: 'Open Text Inc' },
  lines: [{ qty: 5000, price: 0.88, amount: 4400, print_description: 'A <b>', po: '4500059700' }],
  totals: { qty: 5000, amount: 4400 }, signatures: ['簽核', '審核', '製單'] };
const root = { innerHTML: '' };
assert.equal(mod.renderInvoicePrint(data, root), 1);
for (const s of ['Page 1 of 1', 'INVOICE:&nbsp;&nbsp;ST-2026304', 'DATE:&nbsp;&nbsp;Nov.16,2026', '客戶名稱:&nbsp;Open Text Inc',
  'PO#4500059700', '5,000.0000', '0.880', '4,400.000', 'USD 4,400.000', 'A &lt;b&gt;', '簽核', 'FOB']) assert.ok(root.innerHTML.includes(s), s);
const two = { ...data, pages: [{ no: 1, rows: [{ type: 'line', line: 0 }], last: false }, { no: 2, rows: [], last: true }] };
assert.equal(mod.renderInvoicePrint(two, root), 2);
assert.equal((root.innerHTML.match(/class="inv-sheet"/g) || []).length, 2);
assert.equal((root.innerHTML.match(/inv-sign-item/g) || []).length, 3);
assert.ok(root.innerHTML.includes('Page 2 of 2'));
const page = fs.readFileSync('./static/modules/erp_invoice.js', 'utf8').replace(/^import .*;\r?\n/gm, '');
const inv = await import('data:text/javascript,' + encodeURIComponent(page));
assert.equal(inv.printUrl(' st-2026304 '), '/static/erp_invoice_print.html?bill=ST-2026304');
assert.equal(inv.printUrl('ST-202630'), null);
assert.equal(inv.printUrl(''), null);
"""
        res = subprocess.run(["node", "--input-type=module", "-e", script], cwd=ROOT, capture_output=True, text=True,
                             encoding="utf-8")
        self.assertEqual(res.returncode, 0, res.stderr)


if __name__ == "__main__":
    unittest.main()
