"""開 Invoice 伺服器 PDF 列印（同進貨單列印流程）：折行、選印表機、分頁、簽核模式、PDF、路由。

不連 ERP、不連列印小幫手：get_print_data 與 send_pdf_with_auto_restart 一律用假的。
"""
from __future__ import annotations

import os
import re
import unittest
from pathlib import Path
from unittest import mock

from app.services import erp_invoice_print, erp_stock, invoice_print_pdf as ipp, rm_print, rm_print_dispatch

ROOT = Path(__file__).resolve().parents[1]

DESC_304 = [
    "IC-NB675GL-Z IC, 10A, 5V-24V INPUT SYNCHRONOUS BUCK CONVERTER WITH 1.5A LDO AND LOW-NOISE "
    "REFERENCE, MPS NB675GL-Z, QFN21 (3mm x",
    "IC-XC7K70T-1FBG484I IC, FBG484. Xilinx XC7K70T-1FBG484I. ---代購料",
]
WRAPS_304 = [
    ["IC-NB675GL-Z IC, 10A, 5V-24V INPUT SYNCHRONOUS", "BUCK CONVERTER WITH 1.5A LDO AND LOW-NOISE",
     "REFERENCE, MPS NB675GL-Z, QFN21 (3mm x"],
    ["IC-XC7K70T-1FBG484I IC, FBG484. Xilinx XC7K70T-1FBG484I.", "---代購料"],
]
UNATTACHED = ("PCB-A1122 T356789iu Rev 3.4-A + REV 1.1-B Forensic Universal Bridge, Bay Mountable Unit W/ Tableau "
              "Artwork.“Unattached\" Tableau Keypad \"Loosely\" Attached")
LINES_302 = [
    ("4500059433", "PCB-A1080 T3iu A+B -REV-1.2+1.0 Forensic SATA Imaging Bay", 120, 92.8, 11136),
    ("4500059418", UNATTACHED, 192, 110.48, 21212.16),
    ("4500059497-B", "PCB-A1132 TA7-9 Adapter. PCIe Gen 4-FireWire Adapter.", 200, 20.98, 4196),
    ("4500059563", "PCB-A1068 T35u-REV-2.0 USB 3.0 Forensic SATA/IDE Bridge. (Black Plastic)", 580, 36.32, 21065.6),
    ("4500059563", "PCB-A1071 T35u-REV-2.0 USB 3.0 Forensic SATA/IDE Bridge Pre-Configured For Read-Write "
                   "Operation.(Yellow Plastic)", 20, 36.32, 726.4),
    ("4500059563-A", "PCB-A1121 T356789iu Rev 3.4-A + REV 1.1-B Forensic Universal Bridge, Bay Mountable Unit W/ "
                     "Tableau Artwork.", 72, 110.48, 7954.56),
    ("4500059563-A", UNATTACHED, 120, 110.48, 13257.6),
]


def _line(po, desc, qty, price, amount, **kw):
    return {"po": po, "print_description": desc, "qty": qty, "price": price, "amount": amount,
            "qty_display": erp_invoice_print.fmt_num(qty, 4), "price_display": erp_invoice_print.fmt_num(price, 3),
            "amount_display": erp_invoice_print.fmt_num(amount, 3), "excluded_tab": False, **kw}


def invoice(bill_no, lines, qty, amount):
    """與 erp_invoice_print.get_print_data() 相同的資料形狀（只放列印用到的欄位）。"""
    return {
        "ok": True, "bill_no": bill_no, "title": "銷貨單", "date_display": "Nov.16,2026",
        "company": dict(erp_invoice_print.FALLBACK_COMPANY), "customer": {"id": "C0152", "name": "Open Text Inc"},
        "trade_term": "FOB", "currency": "USD", "lines": lines, "signatures": ["簽核", "審核", "製單"],
        "totals": {"qty": qty, "amount": amount, "qty_display": erp_invoice_print.fmt_num(qty, 4),
                   "amount_display": erp_invoice_print.fmt_num(amount, 3)},
    }


def st_2026304():
    return invoice("ST-2026304", [_line("4500059700", DESC_304[0], 5000, 0.88, 4400),
                                  _line("4500059703", DESC_304[1], 900, 90, 81000)], 5900, 85400)


def st_2026302():
    return invoice("ST-2026302", [_line(*row) for row in LINES_302], 1304, 79548.32)


def st_2026307():
    """正航 ST-2026307 的 print-data 明細（4 列、3 個 PO）：簽核貼框底會壓到合計，緊接合計則放得進半張。"""
    contact = "APXA0VX2X-003 P/N:1-2090-1 M99, (QPL) Contact, BNC, KN, F"
    return invoice("ST-2026307", [
        _line("W058361", "A0C01V001-000 P/N:2-1940, SHV Plug SUB-ASSY", 2000, 4.3, 8600),
        _line("W058376", contact, 10, 0.75, 7.5),
        _line("W058376", contact, 2690, 0.75, 2017.5),
        _line("W058993", "ABXAAA001-012 P/N:1-7807-10 M06, Body, KGP, BNC, M", 860, 0.53, 455.8),
    ], 5560, 11080.8)


def synthetic(n=60):
    descs = ["SHORT PART", "PCB-A1080 T3iu A+B -REV-1.2+1.0 Forensic SATA Imaging Bay", UNATTACHED]
    lines = [_line(f"PO{i // 8}", f"P{i:02d} {descs[i % 3]}", 10, 1.5, 15) for i in range(n)]
    lines.insert(5, _line("PO0", "X-TAB", 1, 1, 1, excluded_tab=True))
    return invoice("ST-9999999", lines, 10 * n, 15 * n)


def _fonts_or_skip(test):
    try:
        return ipp.resolve_fonts()
    except RuntimeError as exc:
        test.skipTest(f"沒有列印字型：{exc}")


def pdf_page_count(pdf: bytes) -> int:
    return len(re.findall(rb"/Type\s*/Page(?![A-Za-z])", pdf))


class WrapTest(unittest.TestCase):
    def test_st_2026304_wraps_with_arial_screen_widths(self):
        self.assertEqual([ipp.wrap_description(d) for d in DESC_304], WRAPS_304)
        # 放得下的最寬行 364px、要折掉的最窄行 367px（ST-2026304 實單）
        self.assertEqual(ipp.screen_px(WRAPS_304[1][0]), 364)
        self.assertEqual(ipp.screen_px(WRAPS_304[0][0] + " BUCK"), 367)
        self.assertTrue(364 <= ipp.DESC_WRAP_PX < 367)

    def test_wraps_with_real_font_metrics(self):
        try:
            from fontTools.ttLib import TTFont
        except ImportError:
            self.skipTest("沒有 fontTools，無法讀字型 hdmx")
        path = ipp.latin_font_path()
        if not path:
            self.skipTest("沒有安裝 Liberation Sans / Arimo / Arial（改用內建 Helvetica）")
        font = TTFont(path)
        hdmx = font["hdmx"].hdmx.get(13) if "hdmx" in font else None
        if not hdmx:
            self.skipTest(f"{os.path.basename(path)} 沒有 13ppem 螢幕字寬表")
        cmap = font.getBestCmap()
        widths = {chr(c): hdmx[cmap[c]] for c in range(32, 127) if c in cmap}
        if any(widths.get(ch) != w for ch, w in ipp.ARIAL_13PX.items() if ch in widths):
            self.skipTest(f"{os.path.basename(path)} 的螢幕字寬與 Arial 不同，以內建 ARIAL_13PX 為準")

        def measure(text):
            return sum(widths.get(ch, 13) for ch in text)
        self.assertEqual([ipp.wrap_description(d, measure) for d in DESC_304], WRAPS_304)

    def test_wrap_logic_with_fake_metrics(self):
        self.assertEqual(ipp.wrap_description("aa bb cc", len, 5), ["aa bb", "cc"])
        self.assertEqual(ipp.wrap_description("a b c d e", len, 1), ["a", "b", "c"])  # 最多 3 行，不加省略號
        self.assertEqual(ipp.wrap_description("", len, 5), [""])
        self.assertEqual(ipp.wrap_description("abcdefgh", len, 3), ["abc", "def", "gh"])  # 超寬單字硬切


class PrinterChoiceTest(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.dict(os.environ, {"INVOICE_PRINT_SIGN_MODE": "", "RM_PRINT_HALF_PAGE_MM": "",
                                               "RM_PRINT_PRINTER_HALF": "", "RM_PRINT_PRINTER_FULL": ""})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_st_2026304_half_page(self):
        lay = ipp.layout(st_2026304())
        self.assertTrue(lay["fits_half_page"])
        self.assertEqual(lay["page_count"], 1)
        self.assertLessEqual(lay["bottom_mm"], 146.5)
        info = ipp.info_from_layout(lay)
        self.assertEqual(info["printer"], rm_print.choose_printer(True))
        self.assertIn("M111w", info["printer"])
        lines = [e["desc"] for e in lay["pages"][0]["elements"] if e["type"] == "line"]
        self.assertEqual(lines, WRAPS_304)
        pos = [(e["type"], round(e["base"], 1)) for e in lay["pages"][0]["elements"]]
        self.assertEqual(pos, [("po", 80.5), ("line", 86.6), ("po", 99.6), ("line", 105.7)])  # 實單 291/313/360px

    def test_st_2026302_a4_one_page(self):
        lay = ipp.layout(st_2026302())
        self.assertFalse(lay["fits_half_page"])
        self.assertEqual(lay["page_count"], 1)
        info = ipp.info_from_layout(lay)
        self.assertEqual(info["printer"], rm_print.choose_printer(False))
        self.assertIn("M652", info["printer"])
        self.assertEqual(info["paper"], "A4")
        pos = [e["po"] for e in lay["pages"][0]["elements"] if e["type"] == "po"]
        self.assertEqual(pos, ["4500059433", "4500059418", "4500059497-B", "4500059563", "4500059563-A"])

    def test_st_2026304_half_signature_pinned(self):
        lay = ipp.layout(st_2026304(), "auto")
        page = lay["pages"][-1]
        self.assertEqual((lay["form"], lay["bottom_mm"]), ("half", 146.5))
        self.assertAlmostEqual(page["frame_bottom"], ipp.HALF_FRAME_BOTTOM)
        self.assertAlmostEqual(page["sign_base"], ipp.HALF_SIGN_BASE)

    def test_st_2026307_half_signature_follows_totals(self):
        lay = ipp.layout(st_2026307(), "auto")
        page = lay["pages"][-1]
        self.assertGreater(page["totals_base"] + ipp.SIGN_GAP, ipp.HALF_SIGN_BASE)  # 貼框底會壓到合計
        self.assertTrue(lay["fits_half_page"])
        self.assertEqual((lay["form"], lay["page_count"]), ("half", 1))
        self.assertAlmostEqual(page["sign_base"], page["totals_base"] + ipp.SIGN_GAP)
        self.assertAlmostEqual(page["frame_bottom"], page["sign_base"] + ipp.SIGN_TO_FRAME_FOLLOW)
        self.assertEqual(lay["bottom_mm"], round(page["frame_bottom"], 2))
        self.assertLessEqual(lay["bottom_mm"], 148.5)
        self.assertEqual(lay["bottom_mm"], 145.29)
        self.assertIn("M111w", ipp.info_from_layout(lay)["printer"])
        self.assertTrue(ipp.layout(st_2026307(), "follow")["fits_half_page"])
        self.assertFalse(ipp.layout(st_2026307(), "bottom")["fits_half_page"])  # bottom 要簽核貼框底才用半張

    def test_st_2026302_a4(self):
        lay = ipp.layout(st_2026302(), "auto")
        self.assertEqual((lay["form"], lay["fits_half_page"], lay["bottom_mm"]), ("a4", False, 196.47))

    def test_half_page_limit_env(self):
        with mock.patch.dict(os.environ, {"RM_PRINT_HALF_PAGE_MM": "140"}):
            lay = ipp.layout(st_2026304())  # 框底 146.5 放不下 → 簽核緊接合計（133.94）仍是半張
            self.assertEqual((lay["form"], lay["bottom_mm"]), ("half", 133.94))
        with mock.patch.dict(os.environ, {"RM_PRINT_HALF_PAGE_MM": "130"}):
            self.assertFalse(ipp.layout(st_2026304())["fits_half_page"])

    def test_tab_rows_not_printed(self):
        data = st_2026304()
        data["lines"].append(_line("4500059703", "Z-TAB", 1, 1, 1, excluded_tab=True))
        lines = [e for e in ipp.layout(data)["pages"][0]["elements"] if e["type"] == "line"]
        self.assertEqual(len(lines), 2)


class PaginationTest(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.dict(os.environ, {"INVOICE_PRINT_SIGN_MODE": ""})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.lay = ipp.layout(synthetic())
        self.pages = self.lay["pages"]

    def test_pages_and_order(self):
        self.assertFalse(self.lay["fits_half_page"])
        self.assertGreaterEqual(self.lay["page_count"], 2)
        idx = [e["index"] for p in self.pages for e in p["elements"] if e["type"] == "line"]
        self.assertEqual(idx, list(range(60)))
        for p in self.pages:
            self.assertLessEqual(p["frame_bottom"], ipp.PAGE_BOTTOM_MM + 1e-9)

    def test_po_header_kept_with_first_line_and_reprinted(self):
        reprints = 0
        prev_po = None
        for p in self.pages:
            els = p["elements"]
            self.assertEqual(els[-1]["type"], "line")  # PO 標題不會單獨留在頁底
            for a, b in zip(els, els[1:]):
                if a["type"] == "po":
                    self.assertEqual(b["type"], "line")
                    self.assertEqual(b["po"], a["po"])
            first_line = next(e for e in els if e["type"] == "line")
            if prev_po is not None and first_line["po"] == prev_po:
                self.assertEqual(els[0]["type"], "po")
                self.assertTrue(els[0]["reprint"])
                reprints += 1
            prev_po = [e for e in els if e["type"] == "line"][-1]["po"]
        # 合成資料每 8 列換 PO（每頁約 21 列），跨頁時至少一次接續同一個 PO
        self.assertGreaterEqual(reprints, 1)

    def test_totals_and_signature_only_on_last_page(self):
        for p in self.pages[:-1]:
            self.assertNotIn("totals_base", p)
            self.assertNotIn("sign_base", p)
            self.assertAlmostEqual(p["close_rule"], p["last_base"] + ipp.TOTALS_RULE_GAP)
        last = self.pages[-1]
        self.assertTrue(last["last"])
        self.assertTrue(any(e["type"] == "line" for e in last["elements"]))  # 最後一頁不會只有合計
        self.assertLessEqual(last["sign_base"], ipp.A4_SIGN_MAX + 1e-9)

    def test_last_unit_moved_when_totals_do_not_fit(self):
        keys = ("index", "po", "header", "desc", "qty", "price", "amount")
        for n in range(30, 90):
            pages = ipp.layout(synthetic(n))["pages"]
            lines = [[{k: e[k] for k in keys} for e in p["elements"] if e["type"] == "line"] for p in pages]
            if len(pages) < 2 or len(lines[-1]) != 1:
                continue
            prev = lines[-2] + lines[-1]
            last = ipp._last_base(prev)
            if last + ipp.TOTALS_RULE_GAP + ipp.FRAME_PAD <= ipp.PAGE_BOTTOM_MM:
                # 明細放得下、合計 + 簽核放不下 → 最後一列移到新的一頁
                self.assertGreater(ipp._follow_sign_base(last), ipp.A4_SIGN_MAX)
                return
        self.fail("找不到最後一列被移到下一頁的情況")

    def test_single_po_reprinted_on_every_page(self):
        data = synthetic()
        for line in data["lines"]:
            line["po"] = "4500000001"
        pages = ipp.layout(data)["pages"]
        self.assertGreaterEqual(len(pages), 2)
        self.assertEqual([p["elements"][0]["reprint"] for p in pages], [False] + [True] * (len(pages) - 1))
        self.assertTrue(all(p["elements"][0]["type"] == "po" for p in pages))
        self.assertEqual(sum(e["type"] == "po" for p in pages for e in p["elements"]), len(pages))

    def test_empty_invoice_single_page(self):
        lay = ipp.layout(invoice("ST-2026001", [], 0, 0))
        self.assertEqual(lay["page_count"], 1)
        self.assertTrue(lay["fits_half_page"])


class SignModeTest(unittest.TestCase):
    def _last(self, data, mode):
        return ipp.layout(data, mode)["pages"][-1]

    def test_half_page_modes(self):
        auto, follow, bottom = (self._last(st_2026304(), m) for m in ("auto", "follow", "bottom"))
        self.assertAlmostEqual(auto["frame_bottom"], ipp.HALF_FRAME_BOTTOM)
        self.assertAlmostEqual(auto["sign_base"], ipp.HALF_SIGN_BASE)
        self.assertEqual((bottom["frame_bottom"], bottom["sign_base"]), (auto["frame_bottom"], auto["sign_base"]))
        self.assertAlmostEqual(follow["sign_base"], follow["totals_base"] + ipp.SIGN_GAP)
        self.assertAlmostEqual(follow["frame_bottom"], follow["sign_base"] + ipp.SIGN_TO_FRAME_FOLLOW)
        self.assertLess(follow["frame_bottom"], auto["frame_bottom"])
        # 合計一律緊接最後一行
        for p in (auto, follow, bottom):
            self.assertAlmostEqual(p["totals_rule"], p["last_base"] + ipp.TOTALS_RULE_GAP)

    def test_a4_modes(self):
        auto, follow, bottom = (self._last(st_2026302(), m) for m in ("auto", "follow", "bottom"))
        self.assertAlmostEqual(auto["sign_base"], auto["totals_base"] + ipp.SIGN_GAP)
        self.assertAlmostEqual(auto["frame_bottom"], auto["sign_base"] + ipp.SIGN_TO_FRAME_FOLLOW)
        self.assertEqual(follow["sign_base"], auto["sign_base"])
        self.assertAlmostEqual(bottom["frame_bottom"], ipp.PAGE_BOTTOM_MM)
        self.assertAlmostEqual(bottom["sign_base"], ipp.PAGE_BOTTOM_MM - ipp.SIGN_TO_FRAME_PINNED)

    def test_mode_does_not_change_printer(self):
        for m in ipp.SIGN_MODES:
            self.assertTrue(ipp.layout(st_2026304(), m)["fits_half_page"])
            self.assertFalse(ipp.layout(st_2026302(), m)["fits_half_page"])

    def test_env(self):
        for value, expected in (("", "auto"), ("FOLLOW", "follow"), ("bottom", "bottom"), ("x", "auto")):
            with mock.patch.dict(os.environ, {"INVOICE_PRINT_SIGN_MODE": value}):
                self.assertEqual(ipp.sign_mode(), expected)
                self.assertEqual(ipp.layout(st_2026304())["sign_mode"], expected)


class RenderTest(unittest.TestCase):
    def test_pdf_valid_and_page_count(self):
        _fonts_or_skip(self)
        for data in (st_2026304(), st_2026307(), st_2026302(), synthetic()):
            pdf, info = ipp.render_pdf(data)
            self.assertTrue(pdf.startswith(b"%PDF"))
            self.assertEqual(pdf_page_count(pdf), info["page_count"])
            self.assertEqual(info["page_count"], ipp.layout(data)["page_count"])
            self.assertEqual(info["bill_no"], data["bill_no"])
            for key in ("bottom_mm", "fits_half_page", "printer", "paper", "printers", "font"):
                self.assertIn(key, info)


class RouteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from app.routers.erp_invoice import router

        app = FastAPI()
        app.include_router(router, prefix="/api")
        cls.client = TestClient(app)

    def setUp(self):
        _fonts_or_skip(self)
        patcher = mock.patch.object(erp_invoice_print, "get_print_data", side_effect=self._data)
        patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def _data(bill_no):
        bill = erp_invoice_print.normalize_bill_no(bill_no)
        if bill == "ST-2026999":
            raise erp_invoice_print.InvoiceNotFound("正航找不到 Invoice ST-2026999")
        if bill == "ST-2026998":
            raise erp_stock.ErpNotConfigured("x")
        return st_2026302() if bill == "ST-2026302" else st_2026304()

    def _helper(self, enabled):
        return mock.patch.object(rm_print_dispatch, "helper_config", lambda: {"enabled": enabled})

    def test_pdf_inline(self):
        r = self.client.get("/api/erp-invoice/print.pdf?bill_no=ST-2026304")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers["content-type"], "application/pdf")
        self.assertTrue(r.headers["content-disposition"].startswith("inline"))
        self.assertTrue(r.content.startswith(b"%PDF"))

    def test_print_info(self):
        with self._helper(False):
            j = self.client.get("/api/erp-invoice/print-info?bill_no=st-2026302").json()
        self.assertEqual(j["bill_no"], "ST-2026302")
        self.assertIs(j["fits_half_page"], False)
        self.assertEqual(j["printer"], rm_print.choose_printer(False))
        self.assertIs(j["helper_enabled"], False)
        self.assertEqual(j["pdf_url"], "/api/erp-invoice/print.pdf?bill_no=ST-2026302")

    def test_bill_no_validation_and_errors(self):
        for url in ("/api/erp-invoice/print.pdf?bill_no=304", "/api/erp-invoice/print-info?bill_no=ST-202630",
                    "/api/erp-invoice/print-info"):
            self.assertEqual(self.client.get(url).status_code, 400, url)
        self.assertEqual(self.client.post("/api/erp-invoice/print", json={"bill_no": "ST-1"}).status_code, 400)
        r = self.client.get("/api/erp-invoice/print-info?bill_no=ST-2026999")
        self.assertEqual(r.status_code, 404)
        self.assertIn("找不到", r.json()["detail"])
        self.assertEqual(self.client.get("/api/erp-invoice/print.pdf?bill_no=ST-2026998").status_code, 503)

    def test_print_helper_disabled(self):
        with self._helper(False), mock.patch.object(rm_print_dispatch, "send_pdf_with_auto_restart") as send:
            j = self.client.post("/api/erp-invoice/print", json={"bill_no": "ST-2026304"}).json()
        send.assert_not_called()
        self.assertIs(j["sent"], False)
        self.assertIs(j["ok"], False)
        self.assertIn(rm_print.choose_printer(True), j["message"])

    def test_print_helper_error_502(self):
        err = rm_print_dispatch.PrintHelperError("連不到列印小幫手")
        with self._helper(True), mock.patch.object(rm_print_dispatch, "send_pdf_with_auto_restart", side_effect=err):
            r = self.client.post("/api/erp-invoice/print", json={"bill_no": "ST-2026304"})
        self.assertEqual(r.status_code, 502)
        self.assertIn("列印小幫手", r.json()["detail"])

    def test_print_success(self):
        fake = mock.Mock(return_value={"ok": True, "printer": "x", "job": 42, "elapsed_ms": 9})
        with self._helper(True), mock.patch.object(rm_print_dispatch, "send_pdf_with_auto_restart", fake):
            j302 = self.client.post("/api/erp-invoice/print", json={"bill_no": "ST-2026302"}).json()
            j304 = self.client.post("/api/erp-invoice/print", json={"bill_no": "st-2026304"}).json()
        (pdf, printer, job), _ = fake.call_args_list[0]
        self.assertTrue(pdf.startswith(b"%PDF"))
        self.assertEqual((printer, job), (rm_print.choose_printer(False), "ST-2026302"))
        (_pdf, printer, job), _ = fake.call_args_list[1]
        self.assertEqual((printer, job), (rm_print.choose_printer(True), "ST-2026304"))
        self.assertIs(j302["sent"], True)
        self.assertEqual(j302["job"], 42)
        self.assertIs(j304["fits_half_page"], True)


class FrontendTest(unittest.TestCase):
    def test_buttons_use_server_print(self):
        js = (ROOT / "static" / "modules" / "erp_invoice.js").read_text(encoding="utf-8")
        for s in ("/api/erp-invoice/print-info?bill_no=", 'apiPost("/api/erp-invoice/print", { bill_no: no })',
                  "info.pdf_url", "info.helper_enabled", "r.job",
                  'el("btn-erp-invoice-print-created")?.addEventListener("click", event => printInvoice(',
                  "if (btn) printInvoice(btn.dataset.printBill, btn);",
                  'printInvoice(input?.value, el("btn-erp-invoice-print-no"))', ">預覽</a>"):
            self.assertIn(s, js)
        self.assertNotIn("window.print", js)
        self.assertNotIn("openPrint", js)


if __name__ == "__main__":
    unittest.main()
