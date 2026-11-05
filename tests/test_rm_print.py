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
        self.assertEqual(rm_print.pay_terms_text(2, 30), "月結 30 天")
        self.assertEqual(rm_print.pay_terms_text(3, 30, " 貨到付現 "), "貨到付現")
        self.assertEqual(rm_print.pay_terms_text(99, 30, " 其他 "), "其他")
        self.assertEqual(rm_print.pay_terms_text(0, 0), "")

    def test_fr2_breaks(self):
        for text, limit, expected in (
                ("AA BB CC", 5, ["AA BB", "CC"]),
                ("A AB-CD", 5, ["A", "AB-CD"]),
                ("A AB.CD", 5, ["A", "AB.CD"]),
                ("A AB,CD", 5, ["A", "AB,CD"]),
                ("ABCDEFG", 4, ["ABCD", "EFG"]),
                ("AA ABCDEFG", 4, ["AA", "ABCD", "EFG"]),
                ("中文字測試", 3, ["中文字", "測試"]),
                ("A\r\nB\n", 4, ["A", "B", ""]),
                (" A  ", 10, [" A  "]),
                ("", 4, [""])):
            with self.subTest(text=text):
                self.assertEqual(rm_print._wrap_text(text, limit, len), expected)

    def test_fr2_geometry(self):
        self.assertEqual(rm_print.WRAP_PX, {"row_no": 35, "prod_id": 71, "prod_name": 132, "ware": 64, "remark": 53})
        self.assertEqual(rm_print.row_height(1), 5.85)
        self.assertAlmostEqual(rm_print.row_height(6), 26.24)
        self.assertEqual(rm_print.memo_extra(1), 0)
        self.assertAlmostEqual(rm_print.memo_extra(3), 0.916 + 3 * 4.064 - 4.91)
        self.assertEqual(rm_print.FOOT_MID_RIGHTS, (161.48, 161.48, 161.76))
        self.assertEqual(rm_print.FOOT_RIGHT_RIGHT, 200.24)
        with mock.patch.object(rm_print, "wrap_text", return_value=[""]) as wrap:
            rm_print._header_lines({}, {})
            self.assertEqual([c.args[1] for c in wrap.call_args_list], [384, 452, 704])
        with mock.patch.object(rm_print, "_char_em", return_value=0.51):
            self.assertEqual(rm_print.screen_px("AB", {}), 14)
        with mock.patch.object(rm_print, "_char_em", return_value=1):
            self.assertAlmostEqual(rm_print.text_width("中", fonts={"mode": "subst"}), 83 / 600 * 25.4)
            self.assertAlmostEqual(rm_print.text_width("中", 18, {"mode": "subst"}), 18 * rm_print.PT_MM)

    def test_header_memo_push_down(self):
        painter = mock.Mock(fonts={"mode": "subst"})
        painter.text_center.return_value = (10, 20)
        wrapped = {2: ["廠商一", "廠商二"], 5: ["地址一", "地址二", "地址三"], 6: ["發票地址"]}
        extra = rm_print.memo_extra(2) + rm_print.memo_extra(3)
        with mock.patch.object(rm_print, "_header_lines", return_value=wrapped), \
                mock.patch.object(rm_print, "clip_text", side_effect=lambda text, *args: text), \
                mock.patch.object(rm_print, "text_width", return_value=10):
            rm_print._header(painter, {}, 1, 1, extra)
        calls = painter.text.call_args_list
        def baseline(text):
            return next(c.args[1] for c in calls if c.args[2] == text)
        self.assertAlmostEqual(baseline("廠商二") - baseline("廠商一"), 4.064)
        self.assertAlmostEqual(baseline("地址一"), rm_print.HEAD_BASE[5] + rm_print.memo_extra(2))
        self.assertAlmostEqual(baseline("發票地址"), rm_print.HEAD_BASE[6] + extra)

    def test_exact_glyph_advances(self):
        canvas = mock.Mock()
        fonts = {"mode": "exact", "cjk": "測試字型"}
        with mock.patch.object(rm_print, "_char_em", return_value=1):
            rm_print._Painter(canvas, fonts).text(10, 20, "中 文")
        self.assertEqual(canvas.drawText.call_count, 2)
        origins = canvas.beginText.return_value.setTextOrigin.call_args_list
        self.assertAlmostEqual(origins[0].args[0], 10 / rm_print.PT_MM)
        self.assertAlmostEqual(origins[1].args[0], (10 + 2 * 83 / 600 * 25.4) / rm_print.PT_MM)
        self.assertEqual([c.args[0] for c in canvas.beginText.return_value.textOut.call_args_list], ["中", "文"])

    def test_footer_right_positions(self):
        painter = mock.Mock(fonts={"mode": "subst"})
        with mock.patch.object(rm_print, "clip_text", side_effect=lambda text, *args: text):
            rm_print._footer(painter, {"terms_text": ""}, 100)
        self.assertEqual([c.args[0] for c in painter.text_right.call_args_list],
                         [161.48, 161.48, 161.76, 200.24, 200.24, 200.24])

    def test_invoice_address_priority(self):
        main = {"InvAddress": "單據文字", "InvoAddr": "M"}
        desc = {"AddrOfInvo": "廠商發票文字", "AddrID": "D"}
        invoice = {"Address": "發票主檔"}
        addresses = {"M": {"Address": "單據代號"}, "D": {"Address": "廠商代號"}}
        local = dict(rm_no="RM", rm_date=20261104, print_date=20261105, vendor_id="V", vendor_name="廠商", lines=[])
        for expected, source, key in (("單據文字", main, "InvAddress"), ("單據代號", main, "InvoAddr"),
                                      ("廠商發票文字", desc, "AddrOfInvo"), ("廠商代號", desc, "AddrID"),
                                      ("發票主檔", invoice, "Address"), ("廠商地址", {}, "")):
            data = rm_print_data.build_from_erp({"CustAddress": "廠商地址"}, main, [], None, None, None,
                                               {}, {}, local, invoice=invoice, addresses=addresses, cust_desc=desc)
            self.assertEqual(data["invoice_address"], expected)
            source.pop(key, None)

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

    def test_all_payments(self):
        d = dict(sum_amount=1000, sum_tax=50, sum_amtatax=1100, cash_pay=100, visa_pay=200,
                 other_pay=50, offset=40, discount=30, dist_offset=10)
        t = rm_print.totals(d)
        self.assertEqual((t["total"], t["paid"], t["offset"], t["unpaid"]), (1050, 350, 40, 690))
        d["sum_amtatax"] = None
        self.assertEqual(rm_print.totals(d)["unpaid"], 640)
        self.assertEqual(rm_print.totals({})["unpaid"], 0)

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
                "Discount": 0, "CashPay": 1, "VisaPay": 2, "OtherPay": 3, "OffSet": 4, "Remark": ""}
        main = {"GatherStyle": 1, "GatherDelay": 60, "CheckStyle": 3, "CheckOther": "90天,月底票",
                "InvAddress": "", "InvoAddr": "", "TaxNo": "", "PriceofTax": False, "SumAmtATax": 105}
        recs = [{"RowNO": 1, "ProdID": "A", "ProdName": "品名", "Quantity": 2, "Price": 50, "Amount": 100,
                 "TaxAmt": 5, "WareID": "001"}]
        vendor = {"FullName": "萬晉興業有限公司", "TaxNo": "86934557", "Telephone1": "06-253-8069", "FaxNo": "06-2535027"}
        local = {"rm_no": "RM-1", "rm_date": 20261028, "print_date": 20261104, "vendor_id": "P0175", "vendor_name": "x",
                 "lines": [{"prod_id": "A", "unit": "PCS"}]}
        invoice = {"InvoiceNO": "EC43707713", "InvoiceDate": 20261028, "InvoBillNo": "EC43707713-2705"}
        addresses = {"001": {"AddrID": "001", "Address": "台南市永康區正南一街57巷31號"}}
        d = rm_print_data.build_from_erp(acct, main, recs, vendor, {"PersonName": "楊子瑩"}, {"DepartName": "業務部"},
                                         {"A": ""}, {"001": "ST"}, local, invoice=invoice, addresses=addresses, terms_text="\r\n條款")
        self.assertEqual([d[k] for k in ("cash_pay", "visa_pay", "other_pay", "offset", "dist_offset", "sum_amtatax", "terms_text")],
                         [1, 2, 3, 4, 0, 105, "\r\n條款"])
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

    def test_load_terms_preserves_raw_text(self):
        rec = {"rm_no": "RM-1", "lines_json": "[]"}
        conn = mock.Mock()
        for raw in ("\r\n\r\n條款  ", "", None):
            for row in ({"TermsContext": raw}, None):
                def optional(cur, sql, params):
                    return row if "FROM comTerms " in sql else None
                with mock.patch.object(rm_print_data.erp_stock, "get_settings", return_value={}), \
                        mock.patch.object(rm_print_data.erp_stock, "_connect", return_value=conn), \
                        mock.patch.object(rm_print_data, "_one", return_value={"FundBillNo": "RM-1", "CustID": "V"}), \
                        mock.patch.object(rm_print_data, "_all", return_value=[]), \
                        mock.patch.object(rm_print_data, "_optional_all", return_value=[]), \
                        mock.patch.object(rm_print_data, "_optional", side_effect=optional) as query, \
                        mock.patch.dict(os.environ, {"RM_PRINT_TERMS_ID": "CUSTOM"}):
                    d = rm_print_data.load_from_erp(rec)
                    self.assertEqual(d["terms_text"], raw if row else None)
                    query.assert_any_call(conn.cursor(), "SELECT * FROM comCustDesc WHERE Flag = 2 AND ID = %s", ("V",))
                    query.assert_any_call(conn.cursor(), "SELECT TermsContext FROM comTerms WHERE TermsID = %s", ("CUSTOM",))
        self.assertTrue(conn.close.called)

    def test_from_local(self):
        rec = {"rm_no": "RM-9", "rm_date": 20261104, "vendor_id": "V", "vendor_name": "N", "currency": "NTD",
               "sum_amount": 10, "sum_tax": 0.5,
               "lines_json": json.dumps([{"row_no": 1, "prod_id": "A", "prod_name": "B", "unit": "PCS", "qty": 1,
                                          "price": 10, "amount": 10, "tax": 0.5}])}
        d = rm_print_data.from_local(rec)
        self.assertIsNone(d["terms_text"])
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
        self.assertLess(abs(lay["bottom_mm"] - 133.50), 0.05)  # 與正航樣張的簽核列位置一致
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
        self.assertEqual(rm_print.page_label(2, 3), "2")

    def test_wrap_long_name(self):
        lines = [l.rstrip() for l in rm_print.wrap_text("CRYSTAL,25.000MHZ (+/-50ppm OR BETTER) FUNDAMENTAL,HC-49/S", 132)]
        self.assertEqual(lines, ["CRYSTAL,25.000MHZ", "(+/-50ppm OR BETTER)", "FUNDAMENTAL,HC-49/", "S"])

    def test_detail_wrap(self):
        for key, value in (("prod_id", "ABCD-EFGH-IJKL-MNOP-QRST"),
                           ("ware", "倉庫名稱超過五個字的測試"), ("row_no", "12345678901234567890")):
            d = bill(1)
            d["lines"][0][key] = value
            row = rm_print.layout(d)["pages"][0]["rows"][0]
            n = len(row["cells"][key])
            self.assertGreaterEqual(n, 2)
            self.assertEqual(row["height"], rm_print.row_height(n))

    def test_header_wrap(self):
        d = bill(60)
        d.update(vendor_name="廠商" * 70, vendor_address="地址" * 70, invoice_address="發票" * 70)
        fonts = rm_print.resolve_fonts()
        k = sum(rm_print.memo_extra(len(v)) for v in rm_print._header_lines(d, fonts).values())
        self.assertGreater(k, 0)
        lay = rm_print.layout(d, fonts)
        self.assertAlmostEqual(lay["header_extra"], k)
        for page in lay["pages"]:
            if page["rows"]:
                self.assertAlmostEqual(page["rows"][0]["top"], rm_print.DETAIL_TOP + k)

    def test_terms_height_and_printers(self):
        self.assertAlmostEqual(rm_print.footer_height({}), rm_print.SIGN_OFFSET + rm_print.DESCENT_MM)
        self.assertLess(rm_print.footer_height({"terms_text": ""}), rm_print.footer_height({}))
        avail = 148.5 - 75.40 - (39.29 + 0.70)
        n = int(avail // rm_print.row_height(1))
        self.assertEqual(rm_print.max_single_line_rows_in_half_page(), n)
        for count, printer in ((n, "NPI975BEF (HP LaserJet M111w)"), (n + 1, "HP Color LaserJet M652 [7D2006]")):
            lay = rm_print.layout(bill(count))
            self.assertEqual(rm_print.choose_printer(lay["fits_half_page"]), printer)

    def test_exact_terms_positions(self):
        path = os.environ.get("RM_TEST_MINGLIU")
        if not path or not Path(path).is_file():
            self.skipTest("未設定 RM_TEST_MINGLIU 字型")
        old_cache = dict(rm_print._FONT_CACHE)
        self.addCleanup(lambda: (rm_print._FONT_CACHE.clear(), rm_print._FONT_CACHE.update(old_cache)))
        with mock.patch.dict(os.environ, {"RM_PRINT_FONT_PATH": path}):
            rm_print._FONT_CACHE.clear()
            fonts = rm_print.resolve_fonts()
            self.assertEqual(fonts["mode"], "exact")
            self.assertEqual(rm_print.wrap_text("IC-LM393DGKRG4", 71, fonts), ["IC-LM393DG", "KRG4"])
            self.assertEqual(rm_print.wrap_text(
                "IC, DUAL DIFFERENTIAL COMPARATOR, VSSOP- 8 (3mm x 5mm), TI LM393DGKRG4 OR EQUIV.", 132, fonts),
                ["IC, DUAL", "DIFFERENTIAL", "COMPARATOR,", "VSSOP- 8 (3mm x 5mm),", "TI LM393DGKRG4 OR", "EQUIV."])
            lines = rm_print.terms_positions(rm_print.DEFAULT_TERMS_TEXT, fonts)
            self.assertEqual(len(lines), 6)
            for label, expected in (("審核", 90.01), ("製單", 172.37)):
                x = next(x for x, run in lines[-1] if run == label)
                self.assertAlmostEqual(x, expected, delta=0.15)

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
