import json
import os
import re
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

from app.services import rm_print, rm_print_data, rm_print_dispatch

FIXTURE = Path(__file__).parent / "fixtures" / "rm_20261103001.json"

ENV_KEYS = ("RM_PRINT_HALF_PAGE_MM", "RM_PRINT_PRINTER_HALF", "RM_PRINT_PRINTER_FULL",
            "RM_PRINT_HELPER_ENABLED", "RM_PRINT_HELPER_TOKEN")


def sample():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def short_line(i):
    return {"row_no": i, "prod_id": f"P{i:04d}", "prod_name": "電阻", "ware": "ST", "qty": 1, "unit": "PCS",
            "price": 1, "amount": 1, "tax": 0, "remark": ""}


def bill(n):
    d = sample()
    d["lines"] = [short_line(i) for i in range(1, n + 1)]
    d["sum_amount"], d["sum_tax"] = n, 0
    return d


def clean_env(**values):
    """回傳一個 patch.dict：先清掉列印相關環境變數，再設定指定值（離開時還原）。"""
    env = {k: v for k, v in os.environ.items() if k not in ENV_KEYS}
    env.update(values)
    return mock.patch.dict(os.environ, env, clear=True)


class RmPrintPureTest(unittest.TestCase):
    """不需要 reportlab / 字型的純函式測試。"""

    def test_fmt_num(self):
        self.assertEqual(rm_print.fmt_num(6000, 4), "6,000.0000")
        self.assertEqual(rm_print.fmt_num(2.2, 3), "2.200")
        self.assertEqual(rm_print.fmt_num(13860, 0), "13,860")
        self.assertEqual(rm_print.fmt_num(0.0005, 3), "0.001")  # 四捨五入（不是銀行家捨入）
        self.assertEqual(rm_print.fmt_num(-1234.5, 0), "-1,235")
        self.assertEqual(rm_print.fmt_num(None, 0), "0")

    def test_roc_date(self):
        self.assertEqual(rm_print.roc_date(20261104), "115/11/04")
        self.assertEqual(rm_print.roc_date("2026-12-30"), "115/12/30")
        self.assertEqual(rm_print.roc_date(None), "")
        self.assertEqual(rm_print.roc_date(0), "")

    def test_pay_terms(self):
        self.assertEqual(rm_print.pay_terms_text(1, 30), "次月 30 天")
        self.assertEqual(rm_print.pay_terms_text(2, 30), "")  # 其他種類不猜文字
        self.assertEqual(rm_print.pay_terms_text(2, 30, "貨到付現"), "貨到付現")  # 有正航文字就照印
        self.assertEqual(rm_print.pay_terms_text(0, 0), "")

    def test_tax_type_text(self):
        self.assertEqual(rm_print.tax_type_text(0), "應稅")
        self.assertEqual(rm_print.tax_type_text(None), "應稅")
        for t in (1, 2, 3, 4):
            self.assertEqual(rm_print.tax_type_text(t), "免稅")

    def test_price_label(self):
        # 正航 comBillAccounts.PriceOfTax = 1 → 單價未稅（RM-20261103001 樣張印「未稅」）
        self.assertEqual(rm_print.price_label(True), "未稅")
        self.assertEqual(rm_print.price_label(1), "未稅")
        self.assertEqual(rm_print.price_label(False), "含稅")
        self.assertEqual(rm_print.price_label(None), "")

    def test_totals_and_unpaid(self):
        t = rm_print.totals(sample())
        self.assertEqual((t["sum_amount"], t["sum_tax"], t["total"]), (Decimal(13200), Decimal(660), Decimal(13860)))
        self.assertEqual(t["unpaid"], Decimal(13860))
        d = sample()
        d.update(discount=100, cash_pay=760)
        self.assertEqual(rm_print.totals(d)["unpaid"], Decimal(13000))
        d = sample()
        d.pop("sum_amount"), d.pop("sum_tax")
        self.assertEqual(rm_print.totals(d)["total"], Decimal(13860))  # 沒有表頭合計時用明細加總

    def test_printer_choice_and_env(self):
        with clean_env():
            self.assertEqual(rm_print.choose_printer(True), "NPI975BEF (HP LaserJet M111w)")
            self.assertEqual(rm_print.choose_printer(False), "HP Color LaserJet M652 [7D2006]")
        with clean_env(RM_PRINT_PRINTER_HALF="HALF-X", RM_PRINT_PRINTER_FULL="FULL-Y"):
            self.assertEqual(rm_print.choose_printer(True), "HALF-X")
            self.assertEqual(rm_print.choose_printer(False), "FULL-Y")

    def test_build_from_erp_maps_fields(self):
        # 欄位名稱照正航 CHIComp03 實際資料（RM-20261028002）
        acct = {"FundBillNo": "RM-1", "BillDate": 20261028, "CustID": "P0175", "CurrID": "NTD", "Total": 100, "Tax": 5,
                "TaxType": 0, "PrepayDay": 20261230, "EncashDay": 20261028, "DeptID": "001", "Salesman": "005",
                "PriceOfTax": True, "AddrID": "001", "CustAddress": "", "ContactPhone": "06-2535069",
                "FaxNo": "06-2535027", "Maker": "Andy", "InvoiceNo": "EC43707713", "InvoBillNO": "EC43707713-2705",
                "Discount": 0, "CashPay": 0, "Remark": ""}
        main = {"GatherStyle": 1, "GatherDelay": 60, "CheckStyle": 3, "CheckOther": "90天,月底票",
                "InvAddress": "", "InvoAddr": "", "TaxNo": "", "PriceofTax": False}
        recs = [{"RowNO": 1, "ProdID": "A", "ProdName": "品名", "Quantity": 2, "Price": 50, "Amount": 100,
                 "TaxAmt": 5, "WareID": "001"}]
        vendor = {"FullName": "萬晉興業有限公司", "TaxNo": "86934557", "Telephone1": "06-253-8069", "FaxNo": "06-2535027"}
        local = {"rm_no": "RM-1", "rm_date": 20261028, "print_date": 20261104, "vendor_id": "P0175", "vendor_name": "x",
                 "lines": [{"prod_id": "A", "unit": "PCS"}]}
        invoice = {"InvoiceNO": "EC43707713", "InvoiceDate": 20261028, "InvoBillNo": "EC43707713-2705"}
        addresses = {"001": {"AddrID": "001", "Address": "台南市永康區正南一街57巷31號"}}
        d = rm_print_data.build_from_erp(acct, main, recs, vendor, {"PersonName": "楊子瑩"}, {"DepartName": "業務部"},
                                         {"A": ""}, {"001": "ST"}, local, invoice=invoice, addresses=addresses)
        self.assertEqual(d["vendor_name"], "萬晉興業有限公司")
        self.assertEqual(d["tax_no"], "86934557")
        self.assertEqual(d["vendor_phone"], "06-253-8069")
        self.assertEqual(d["contact_phone"], "06-2535069")
        self.assertEqual((d["buyer"], d["dept"]), ("楊子瑩", "業務部"))
        self.assertEqual(d["pay_terms"], "次月 60 天")  # 付款條件看 Gather*（Check* 是票期）
        self.assertIs(d["price_of_tax"], True)
        self.assertEqual(d["invoice_no"], "EC43707713")
        self.assertEqual(d["invoice_date"], 20261028)  # 來自 comInvoice.InvoiceDate
        self.assertEqual(d["vendor_address"], "台南市永康區正南一街57巷31號")  # 單據地址空白 → 地址代號
        self.assertEqual(d["invoice_address"], d["vendor_address"])
        self.assertEqual(d["lines"][0]["unit"], "PCS")
        self.assertEqual(d["lines"][0]["ware"], "ST")  # 倉庫代號 → 倉庫名稱

    def test_build_from_erp_no_invoice(self):
        acct = {"FundBillNo": "RM-2", "CustID": "V", "PriceOfTax": True, "InvoiceNo": "", "CustAddress": "地址"}
        local = {"rm_no": "RM-2", "rm_date": 20261103, "print_date": 20261104, "vendor_id": "V", "vendor_name": "x",
                 "lines": []}
        d = rm_print_data.build_from_erp(acct, {"InvoAddr": "009"}, [], None, None, None, {}, {}, local,
                                         invoice={"InvoiceDate": 20261103},
                                         addresses={"009": {"Address": "發票地址"}})
        self.assertIsNone(d["invoice_date"])  # 沒有發票號碼就不印發票日期
        self.assertEqual(d["invoice_address"], "發票地址")  # InvoAddr 是地址代號，不直接印

    def test_from_local(self):
        rec = {"rm_no": "RM-9", "rm_date": 20261104, "vendor_id": "V", "vendor_name": "N", "currency": "NTD",
               "sum_amount": 10, "sum_tax": 0.5,
               "lines_json": json.dumps([{"row_no": 1, "prod_id": "A", "prod_name": "B", "unit": "PCS", "qty": 1,
                                          "price": 10, "amount": 10, "tax": 0.5}])}
        d = rm_print_data.from_local(rec)
        self.assertEqual(d["rm_no"], "RM-9")
        self.assertEqual(d["lines"][0]["prod_id"], "A")
        self.assertNotIn("price_of_tax", d)  # 本機紀錄不知道含稅/未稅 → 留白

    def test_helper_disabled_by_default(self):
        with clean_env():
            self.assertIs(rm_print_dispatch.helper_config()["enabled"], False)
            with self.assertRaises(rm_print_dispatch.PrintHelperError):
                rm_print_dispatch.send_pdf(b"%PDF", "x", "RM")
        with clean_env(RM_PRINT_HELPER_ENABLED="1"):
            self.assertIs(rm_print_dispatch.helper_config()["enabled"], False)  # 沒 token 不算啟用


class RmPrintLayoutTest(unittest.TestCase):
    """需要 reportlab 與中文字型；環境沒有就跳過。"""

    def setUp(self):
        try:
            import reportlab  # noqa: F401
        except ImportError:
            self.skipTest("reportlab 未安裝")
        try:
            rm_print.resolve_fonts()
        except RuntimeError as exc:
            self.skipTest(str(exc))
        patcher = clean_env()
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_sample_fits_half_page(self):
        lay = rm_print.layout(sample())
        self.assertEqual(lay["page_count"], 1)
        self.assertIs(lay["fits_half_page"], True)
        self.assertLess(abs(lay["bottom_mm"] - 133.42), 0.05)  # 與正航樣張的簽核列位置一致
        self.assertEqual(lay["pages"][0]["rows"][0]["lines"], 4)  # 品名規格折成 4 行（同樣張）

    def test_half_page_threshold(self):
        n = rm_print.max_single_line_rows_in_half_page()
        self.assertEqual(n, 5)
        self.assertIs(rm_print.layout(bill(n))["fits_half_page"], True)
        self.assertIs(rm_print.layout(bill(n + 1))["fits_half_page"], False)

    def test_half_page_env(self):
        with mock.patch.dict(os.environ, {"RM_PRINT_HALF_PAGE_MM": "100"}):
            self.assertIs(rm_print.layout(sample())["fits_half_page"], False)

    def test_multi_page(self):
        lay = rm_print.layout(bill(60))
        self.assertGreaterEqual(lay["page_count"], 2)
        self.assertIs(lay["fits_half_page"], False)
        self.assertEqual(sum(len(p["rows"]) for p in lay["pages"]), 60)
        self.assertTrue(all(p["footer_top"] is None for p in lay["pages"][:-1]))
        self.assertIsNotNone(lay["pages"][-1]["footer_top"])
        for p in lay["pages"]:
            for r in p["rows"]:
                self.assertLessEqual(r["top"] + r["height"], rm_print.PAGE_BOTTOM_MM + 1e-6)
        self.assertLessEqual(lay["bottom_mm"], rm_print.PAGE_BOTTOM_MM)

    def test_render_pdf_pages_and_page_numbers(self):
        pdf, info = rm_print.render_pdf(bill(60))
        self.assertTrue(pdf.startswith(b"%PDF"))
        self.assertGreaterEqual(info["page_count"], 2)
        self.assertEqual(info["printer"], rm_print.choose_printer(False))
        pdf1, info1 = rm_print.render_pdf(sample())
        self.assertEqual(info1["page_count"], 1)
        self.assertIs(info1["fits_half_page"], True)
        self.assertEqual(len(re.findall(rb"/Type /Page\b", pdf)), info["page_count"])
        self.assertEqual(len(re.findall(rb"/Type /Page\b", pdf1)), 1)
        self.assertEqual(rm_print.page_label(1, 1), "1")
        self.assertEqual(rm_print.page_label(2, 3), "2/3")

    def test_wrap_long_name(self):
        lines = [l.rstrip() for l in rm_print.wrap_text("CRYSTAL,25.000MHZ (+/-50ppm OR BETTER) FUNDAMENTAL,HC-49/S", 36.40)]
        self.assertEqual(lines, ["CRYSTAL,25.000MHZ", "(+/-50ppm OR BETTER)", "FUNDAMENTAL,HC-49/", "S"])

    def test_print_endpoints(self):
        try:
            from fastapi import FastAPI
            from fastapi.testclient import TestClient
        except Exception as exc:  # httpx 等相依套件沒裝
            self.skipTest(f"TestClient 無法使用：{exc}")
        from app import database as db
        from app.routers.erp_receive import router
        from app.services.erp_stock import ErpUnavailable

        app = FastAPI()
        app.include_router(router, prefix="/api")
        rec = {"id": 7, "status": "ok", "rm_no": "RM-20261103001", "rm_date": 20261103, "vendor_id": "P0281",
               "vendor_name": "雅士博", "currency": "NTD", "sum_amount": 13200, "sum_tax": 660, "target_db": "CHIComp03",
               "lines_json": json.dumps(sample()["lines"], ensure_ascii=False)}

        def no_erp(r):
            raise ErpUnavailable("offline")

        with mock.patch.object(db, "get_erp_receipt", lambda i: rec if int(i) == 7 else None), \
                mock.patch.object(rm_print_data, "load_from_erp", no_erp):
            c = TestClient(app)
            r = c.get("/api/erp-receive/receipts/7/print-info")
            self.assertEqual(r.status_code, 200)
            j = r.json()
            self.assertIs(j["fits_half_page"], True)
            self.assertEqual(j["printer"], rm_print.choose_printer(True))
            self.assertIs(j["helper_enabled"], False)
            self.assertEqual(j["source"], "local")
            self.assertTrue(j["warning"])
            r = c.get("/api/erp-receive/receipts/7/print.pdf")
            self.assertEqual(r.status_code, 200)
            self.assertTrue(r.content.startswith(b"%PDF"))
            self.assertEqual(r.headers["content-type"], "application/pdf")
            self.assertEqual(c.get("/api/erp-receive/receipts/8/print-info").status_code, 404)


if __name__ == "__main__":
    unittest.main()
