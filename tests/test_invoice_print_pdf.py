"""開 Invoice 伺服器 PDF 列印（同進貨單列印流程）：折行、選印表機、分頁、簽核模式、PDF、路由。

不連 ERP、不連列印小幫手：get_print_data 與 send_pdf_with_auto_restart 一律用假的。
tests/fixtures/ST-2026019.json 等是正式站 /api/erp-invoice/print-data 的唯讀快照；ST-2026019 的分頁、折行、
基線對照正航列印到 Microsoft Print to PDF 的向量 PDF（2 頁：14 列 / 6 列 + 合計）。
"""
from __future__ import annotations

import json
import os
import re
import unittest
from pathlib import Path
from unittest import mock

from app.services import erp_invoice_print, erp_stock, invoice_print_pdf as ipp, rm_print, rm_print_dispatch

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"

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


def fixture(bill_no):
    return json.loads((FIXTURES / f"{bill_no}.json").read_text(encoding="utf-8"))


# 正航 ST-2026019 實印（向量 PDF）每頁的品名行（含折行）
ZH_019_DESC = [
    [["OC-10268B Housing"], ["AHIAAVXA3-000 Housing, HPR 1-20 Mount"], ["OC-10277B Housing, N(F), PTC E"],
     ["ANXAAAX7P-001 P/N:501-0041-02, Hex Nut, Type N (5/8-24),", "St St/Passivated"],
     ["APXA0V024-000 P/N:380-0232-00, Pin 4.3-10(F)"], ["ABXAAV024-000 P/N:382-0001-00, Center Conductor"],
     ["AIXAAAXXX-016 P/N:410-0137-00, Insulator"],
     ["ABXAAA204-000 P/N:415-0211-00, Connector Shell, 4.3-10(F)", "Bulkhead Mount, Thread-In"],
     ["ABXAAAX04-002 P/N:415-0212-00, Connector Shell, 4.3-10(F),", "Thread-In"],
     ["APXA1V004-000 P/N:380-0234-00, Pin, 4.3-10(M)"],
     ["ABXAAAX04-003 P/N:415-0213-00, Connector Shell, 4.3-10(M),", "Press-In"],
     ["AHXAAA004-001 P/N:416-0001-00, Housing, 4.3-10(F)"],
     ["ANXAAAX04-001 P/N:510-0001-00, Coupling Nut, 4.3-10(M),", "22mm HEX"],
     ["AHXAAAX24-000 P/N:381-0001, Outer Contact (BeCu consign)"]],
    [["70MB0V27X-006 P/N: 330-0253-02 CONNECTOR, Press-In",
      "SMA(F), Solder dipped and 24AWG wire installed, with Lead", 'L=0.4"'],
     ["ABXAAV00X-008 P/N:750-0897-00, Electrode"], ["OC-10279B Cover, Press-In N 6 GHZ"],
     ["ABXAAA0AX-002 P/N:415-0205-00, Cover, Type N(M), Alum"], ["APXAAVX44-000 P/N:380-0227-00, Pin, Type N F-M"],
     ["AVXAAAX0X-000 P/N:750-0840-00, Back Cover, ADP-C003", "(Unfinished by Sandy 2024/04/26)"]],
]
# 正航 ST-2026019 每頁 PO# 基線（mm）
ZH_019_PO_BASES = [
    [80.90, 92.37, 103.84, 115.32, 130.60, 142.07, 153.54, 165.02, 180.34, 195.62, 207.09, 222.42, 233.89, 249.17],
    [80.90, 100.29, 111.76, 123.23, 134.70, 146.18],
]


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

    def test_wrap_366_fits_367_wraps(self):
        # ST-2026019：正航把 366px 的這行放在同一行；ST-2026304 的 367px 要折
        line = "ABXAAAX04-003 P/N:415-0213-00, Connector Shell, 4.3-10(M),"
        self.assertEqual(ipp.screen_px(line), 366)
        self.assertEqual(ipp.wrap_description(line), [line])
        self.assertEqual(ipp.wrap_description(line + " Press-In"), [line, "Press-In"])
        self.assertEqual(ipp.wrap_description("x" * 360 + " yyyyy", len), ["x" * 360 + " yyyyy"])  # 366
        self.assertEqual(ipp.wrap_description("x" * 360 + " yyyyyy", len), ["x" * 360, "yyyyyy"])  # 367

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
        # 正航 band 模型（80.9 / 86.8 / 100.3 / 106.1），半張整頁上移 HALF_SHIFT_MM
        self.assertEqual(pos, [("po", 76.9), ("line", 82.8), ("po", 96.3), ("line", 102.1)])

    def test_st_2026302_a4_one_page(self):
        lay = ipp.layout(st_2026302())
        self.assertFalse(lay["fits_half_page"])
        self.assertEqual(lay["page_count"], 1)
        info = ipp.info_from_layout(lay)
        self.assertEqual(info["printer"], rm_print.choose_printer(False))
        self.assertIn("M652", info["printer"])
        self.assertEqual(info["paper"], "A4")
        pos = [e["po"] for e in lay["pages"][0]["elements"] if e["type"] == "po"]
        # 同一 PO 連續多列也每列都印 PO#（同正航）
        self.assertEqual(pos, ["4500059433", "4500059418", "4500059497-B", "4500059563", "4500059563",
                               "4500059563-A", "4500059563-A"])

    def test_st_2026304_half_signature_follows_totals(self):
        for mode in ("auto", "follow", None):
            lay = ipp.layout(st_2026304(), mode)
            page = lay["pages"][-1]
            self.assertEqual((lay["form"], lay["page_count"]), ("half", 1))
            self.assertAlmostEqual(page["sign_base"], page["totals_base"] + ipp.SIGN_GAP)
            self.assertAlmostEqual(page["frame_bottom"], page["sign_base"] + ipp.SIGN_TO_FRAME_FOLLOW)
            self.assertLess(page["frame_bottom"], ipp.HALF_FRAME_BOTTOM)  # 框底跟著上移，不再固定 146.5
            self.assertEqual(lay["bottom_mm"], round(page["frame_bottom"], 2))
            self.assertEqual(lay["bottom_mm"], 128.44)  # 132.44 上移 4.0
            self.assertIn("M111w", ipp.info_from_layout(lay)["printer"])

    def test_st_2026307_signature_follows_totals(self):
        # 4 列都印 PO#（W058376 兩列各一個）+ 兩列品名 373px 折 2 行：框底 151.32，整頁上移 4.0 → 147.32 <= 148.5 → 半張
        lay = ipp.layout(st_2026307(), "auto")
        page = lay["pages"][-1]
        self.assertEqual(sum(e["type"] == "po" for e in page["elements"]), 4)
        self.assertGreater(page["totals_base"] + ipp.SIGN_GAP, ipp.HALF_SIGN_BASE)  # 貼框底會壓到合計
        self.assertAlmostEqual(page["sign_base"], page["totals_base"] + ipp.SIGN_GAP)
        self.assertAlmostEqual(page["frame_bottom"], page["sign_base"] + ipp.SIGN_TO_FRAME_FOLLOW)
        self.assertEqual(lay["bottom_mm"], round(page["frame_bottom"], 2))
        self.assertEqual(lay["bottom_mm"], 147.32)
        self.assertEqual((lay["form"], lay["page_count"], lay["fits_half_page"]), ("half", 1, True))
        self.assertEqual(lay["shift_mm"], ipp.HALF_SHIFT_MM)
        self.assertIn("M111w", ipp.info_from_layout(lay)["printer"])
        self.assertFalse(ipp.layout(st_2026307(), "bottom")["fits_half_page"])
        self.assertEqual(ipp.layout(fixture("ST-2026307"), "auto")["bottom_mm"], 147.32)  # 正式站資料同結果

    def test_half_shift_moves_every_y(self):
        # 半張上移 = 不上移的半張版面每個 y 都少 HALF_SHIFT_MM（RM_PRINT_HALF_PAGE_MM 放寬讓兩邊都選半張）
        with mock.patch.dict(os.environ, {"RM_PRINT_HALF_PAGE_MM": "160"}):
            with mock.patch.object(ipp, "HALF_SHIFT_MM", 0.0):
                flat = ipp.layout(fixture("ST-2026307"))
            lay = ipp.layout(fixture("ST-2026307"))
        self.assertEqual((flat["form"], lay["form"], flat["shift_mm"], lay["shift_mm"]), ("half", "half", 0.0, 4.0))
        self.assertAlmostEqual(lay["bottom_mm"], flat["bottom_mm"] - 4.0)
        a, b = flat["pages"][0], lay["pages"][0]
        for key in ("last_base", "band_bottom", "totals_rule", "totals_base", "sign_base", "frame_bottom"):
            self.assertAlmostEqual(b[key], a[key] - 4.0, msg=key)
        for ea, eb in zip(a["elements"], b["elements"]):
            for key in ("base", "band_top", "band_bottom"):
                if key in ea:
                    self.assertAlmostEqual(eb[key], ea[key] - 4.0, msg=key)

    def test_st_2026302_a4(self):
        lay = ipp.layout(st_2026302(), "auto")
        self.assertEqual((lay["form"], lay["fits_half_page"], lay["bottom_mm"]), ("a4", False, 209.17))

    def test_half_page_limit_env(self):
        with mock.patch.dict(os.environ, {"RM_PRINT_HALF_PAGE_MM": "140"}):
            lay = ipp.layout(st_2026304())  # 簽核緊接合計、上移後的框底 128.44 <= 140 → 仍是半張
            self.assertEqual((lay["form"], lay["bottom_mm"]), ("half", 128.44))
        with mock.patch.dict(os.environ, {"RM_PRINT_HALF_PAGE_MM": "128"}):
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
            self.assertLessEqual(p["frame_bottom"], ipp.A4_FRAME_BOTTOM + 1e-9)

    def test_po_header_on_every_line(self):
        for p in self.pages:
            els = p["elements"]
            self.assertEqual(els[-1]["type"], "line")  # PO 標題不會單獨留在頁底
            for i, e in enumerate(els):
                if e["type"] == "line":
                    self.assertTrue(e["header"])
                    self.assertEqual((els[i - 1]["type"], els[i - 1]["po"]), ("po", e["po"]))  # 每列都有自己的 PO#
                    self.assertLessEqual(e["band_bottom"], ipp.FOOTER_TOP + 1e-9)
        n_lines = sum(e["type"] == "line" for p in self.pages for e in p["elements"])
        self.assertEqual(sum(e["type"] == "po" for p in self.pages for e in p["elements"]), n_lines)

    def test_empty_po_has_no_header(self):
        data = invoice("ST-2026001", [_line("P1", "A", 1, 1, 1), _line("", "B", 1, 1, 1), _line("P1", "C", 1, 1, 1)],
                       3, 3)
        els = ipp.layout(data)["pages"][0]["elements"]
        self.assertEqual([(e["type"], e.get("po")) for e in els],
                         [("po", "P1"), ("line", "P1"), ("line", ""), ("po", "P1"), ("line", "P1")])

    def test_totals_only_on_last_page_signature_on_every_page(self):
        for p in self.pages[:-1]:
            self.assertNotIn("totals_base", p)
            self.assertNotIn("close_rule", p)  # 非最後一頁最後一列下面不畫收尾線
            self.assertEqual((p["sign_base"], p["frame_bottom"]), (ipp.FOOTER_SIGN_BASE, ipp.A4_FRAME_BOTTOM))
            self.assertEqual((p["sign_base"], p["frame_bottom"]), (273.85, 282.8))  # 正航頁尾 band
        last = self.pages[-1]
        self.assertTrue(last["last"])
        self.assertTrue(any(e["type"] == "line" for e in last["elements"]))  # 最後一頁不會只有合計
        self.assertLessEqual(last["sign_base"], ipp.A4_SIGN_MAX + 1e-9)
        self.assertAlmostEqual(last["sign_base"], last["totals_base"] + ipp.SIGN_GAP)  # 最後一頁緊接合計

    def test_last_unit_moved_when_totals_do_not_fit(self):
        keys = ("index", "po", "header", "desc", "qty", "price", "amount")
        for n in range(30, 90):
            pages = ipp.layout(synthetic(n))["pages"]
            lines = [[{k: e[k] for k in keys} for e in p["elements"] if e["type"] == "line"] for p in pages]
            if len(pages) < 2 or len(lines[-1]) != 1:
                continue
            prev = lines[-2] + lines[-1]
            bottom = ipp._band_bottom(prev)
            if bottom <= ipp.FOOTER_TOP:
                # 明細放得下、合計 + 簽核放不下 → 最後一列移到新的一頁
                self.assertGreater(ipp._follow_sign_base(bottom), ipp.A4_SIGN_MAX)
                return
        self.fail("找不到最後一列被移到下一頁的情況")

    def test_single_po_printed_on_every_line(self):
        data = synthetic()
        for line in data["lines"]:
            line["po"] = "4500000001"
        pages = ipp.layout(data)["pages"]
        self.assertGreaterEqual(len(pages), 2)
        self.assertTrue(all(p["elements"][0]["type"] == "po" for p in pages))
        self.assertEqual(sum(e["type"] == "po" for p in pages for e in p["elements"]), 60)

    def test_empty_invoice_single_page(self):
        lay = ipp.layout(invoice("ST-2026001", [], 0, 0))
        self.assertEqual(lay["page_count"], 1)
        self.assertTrue(lay["fits_half_page"])


class SignModeTest(unittest.TestCase):
    def _last(self, data, mode):
        return ipp.layout(data, mode)["pages"][-1]

    def test_half_page_modes(self):
        auto, follow, bottom = (self._last(st_2026304(), m) for m in ("auto", "follow", "bottom"))
        self.assertEqual((auto["frame_bottom"], auto["sign_base"]), (follow["frame_bottom"], follow["sign_base"]))
        self.assertAlmostEqual(follow["sign_base"], follow["totals_base"] + ipp.SIGN_GAP)
        self.assertAlmostEqual(follow["frame_bottom"], follow["sign_base"] + ipp.SIGN_TO_FRAME_FOLLOW)
        # bottom（舊版，需明確指定）才貼框底 146.5
        self.assertAlmostEqual(bottom["frame_bottom"], ipp.HALF_FRAME_BOTTOM)
        self.assertAlmostEqual(bottom["sign_base"], ipp.HALF_SIGN_BASE)
        self.assertLess(follow["frame_bottom"], bottom["frame_bottom"])
        # 合計一律緊接最後一行
        for p in (auto, follow, bottom):
            self.assertAlmostEqual(p["totals_rule"], p["band_bottom"] + ipp.TOTALS_RULE_GAP)

    def test_a4_modes(self):
        auto, follow, bottom = (self._last(st_2026302(), m) for m in ("auto", "follow", "bottom"))
        self.assertAlmostEqual(auto["sign_base"], auto["totals_base"] + ipp.SIGN_GAP)
        self.assertAlmostEqual(auto["frame_bottom"], auto["sign_base"] + ipp.SIGN_TO_FRAME_FOLLOW)
        self.assertEqual(follow["sign_base"], auto["sign_base"])
        self.assertAlmostEqual(bottom["frame_bottom"], ipp.A4_FRAME_BOTTOM)
        self.assertAlmostEqual(bottom["sign_base"], ipp.FOOTER_SIGN_BASE)

    def test_mode_does_not_change_printer(self):
        for m in ipp.SIGN_MODES:
            self.assertTrue(ipp.layout(st_2026304(), m)["fits_half_page"])
            self.assertFalse(ipp.layout(st_2026302(), m)["fits_half_page"])

    def test_signature_gap_fixed_for_every_shape(self):
        # 合計 → 簽核的距離固定（沒有可變的空白），框底 = 簽核 + 固定邊距
        for data in (st_2026304(), st_2026307(), st_2026302(), synthetic(), synthetic(5),
                     invoice("ST-2026001", [], 0, 0)):
            for mode in ("auto", "follow", None):
                lay = ipp.layout(data, mode)
                last = lay["pages"][-1]
                self.assertAlmostEqual(last["sign_base"] - last["totals_base"], ipp.SIGN_GAP)
                self.assertAlmostEqual(last["frame_bottom"] - last["sign_base"], ipp.SIGN_TO_FRAME_FOLLOW)
                self.assertEqual(lay["bottom_mm"], round(last["frame_bottom"], 2))
                if lay["page_count"] == 1:  # 半張判斷 = 整份 follow 版面的框底放得進半張
                    self.assertEqual(lay["fits_half_page"], last["frame_bottom"] <= lay["half_page_mm"])

    def test_env(self):
        for value, expected in (("", "auto"), ("FOLLOW", "follow"), ("bottom", "bottom"), ("x", "auto")):
            with mock.patch.dict(os.environ, {"INVOICE_PRINT_SIGN_MODE": value}):
                self.assertEqual(ipp.sign_mode(), expected)
                self.assertEqual(ipp.layout(st_2026304())["sign_mode"], expected)


class ZhenghangSt2026019Test(unittest.TestCase):
    """正式站 ST-2026019 print-data 快照 → 與正航實印（向量 PDF）同分頁、同折行、同基線。"""

    def setUp(self):
        patcher = mock.patch.dict(os.environ, {"INVOICE_PRINT_SIGN_MODE": "", "RM_PRINT_HALF_PAGE_MM": ""})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.lay = ipp.layout(fixture("ST-2026019"))
        self.pages = self.lay["pages"]

    def _lines(self, page):
        return [e for e in page["elements"] if e["type"] == "line"]

    def test_split_14_and_6(self):
        self.assertEqual((self.lay["form"], self.lay["page_count"]), ("a4", 2))
        p1, p2 = (self._lines(p) for p in self.pages)
        self.assertEqual((len(p1), len(p2)), (14, 6))
        self.assertTrue(p1[-1]["desc"][0].startswith("AHXAAAX24-000"))
        self.assertEqual(p1[-1]["qty"], "86.0000")
        self.assertTrue(p2[0]["desc"][0].startswith("70MB0V27X-006"))
        self.assertTrue(p2[-1]["desc"][0].startswith("AVXAAAX0X-000"))
        self.assertEqual([sum(e["type"] == "po" for e in p["elements"]) for p in self.pages], [14, 6])
        self.assertEqual(sum(e.get("po") == "70003767" for e in self.pages[0]["elements"] if e["type"] == "po"), 10)

    def test_description_breaks_match_zhenghang(self):
        self.assertEqual([[e["desc"] for e in self._lines(p)] for p in self.pages], ZH_019_DESC)

    def test_baselines_match_zhenghang(self):
        for page, expect in zip(self.pages, ZH_019_PO_BASES):
            got = [e["base"] for e in page["elements"] if e["type"] == "po"]
            self.assertEqual(len(got), len(expect))
            for g, z in zip(got, expect):
                self.assertAlmostEqual(g, z, delta=0.1)
        first = self._lines(self.pages[0])[0]
        self.assertAlmostEqual(first["base"], 86.74, delta=0.1)
        last = self.pages[-1]
        self.assertAlmostEqual(last["totals_rule"], 157.6, delta=0.1)
        self.assertAlmostEqual(last["totals_base"], 161.88, delta=0.1)
        self.assertLessEqual(self.pages[0]["band_bottom"], ipp.FOOTER_TOP)

    def test_signature_every_page(self):
        p1, p2 = self.pages
        self.assertEqual((p1["sign_base"], p1["frame_bottom"]), (273.85, 282.8))  # 非最後一頁：頁尾 band
        self.assertNotIn("totals_base", p1)
        self.assertAlmostEqual(p2["sign_base"], p2["totals_base"] + ipp.SIGN_GAP)  # 最後一頁：緊接合計
        self.assertAlmostEqual(p2["frame_bottom"], p2["sign_base"] + ipp.SIGN_TO_FRAME_FOLLOW)
        self.assertEqual(self.lay["bottom_mm"], round(p2["frame_bottom"], 2))

    def test_half_page_fixture_st_2026304(self):
        lay = ipp.layout(fixture("ST-2026304"))
        self.assertEqual((lay["form"], lay["page_count"], lay["bottom_mm"], lay["shift_mm"]), ("half", 1, 128.44, 4.0))


class DrawTest(unittest.TestCase):
    """畫出來的字：TTL AMOUNT 數值靠左（AMOUNT_X）、每一頁都有三個簽核（標籤 + 空白 + 18 個底線）。"""

    def _calls(self, data):
        _fonts_or_skip(self)
        calls = []
        orig = ipp._Painter.text

        def spy(painter, x, base, s, *args, **kwargs):
            calls.append((painter.c.getPageNumber(), round(x, 2), round(base, 2), s))
            return orig(painter, x, base, s, *args, **kwargs)

        with mock.patch.object(ipp._Painter, "text", spy), mock.patch.dict(os.environ, {"INVOICE_PRINT_SIGN_MODE": ""}):
            pdf, info = ipp.render_pdf(data)
        return calls, info

    def test_amount_left_aligned(self):
        data = fixture("ST-2026019")
        calls, _ = self._calls(data)
        amounts = [l["amount_display"] for l in data["lines"]] + [data["totals"]["amount_display"]]
        xs = {x for _, x, _, s in calls if s in amounts}
        self.assertEqual(xs, {ipp.AMOUNT_X})
        self.assertEqual(ipp.AMOUNT_X, 176.5)

    def test_signature_drawn_on_every_page(self):
        calls, info = self._calls(fixture("ST-2026019"))
        self.assertEqual(info["page_count"], 2)
        for page_no, base in ((1, 273.85), (2, None)):
            signs = [(x, b, s) for n, x, b, s in calls if n == page_no and s.endswith("_" * 18)]
            self.assertEqual([s for _, _, s in signs], [f"{l} " + "_" * 18 for l in ("簽核", "審核", "製單")])
            self.assertEqual([x for x, _, _ in signs], list(ipp.SIGN_X))
            if base:
                self.assertEqual({b for _, b, _ in signs}, {base})
            else:
                self.assertLess(signs[0][1], 200)  # 最後一頁緊接合計，不在頁底

    def _drawn(self, data, shift):
        """PDF 實際畫的座標（reportlab 層：線、框、文字起點，mm、原點左上）。"""
        _fonts_or_skip(self)
        from reportlab.pdfgen.canvas import Canvas
        from reportlab.pdfgen.textobject import PDFTextObject
        out = []
        orig_line, orig_rect, orig_origin = Canvas.line, Canvas.rect, PDFTextObject.setTextOrigin
        mm = lambda v: round(v * rm_print.PT_MM, 3)
        top = lambda v: round(rm_print.PAGE_H - v * rm_print.PT_MM, 3)

        def line(c, x1, y1, x2, y2):
            out.append(("line", mm(x1), top(y1), mm(x2), top(y2)))
            return orig_line(c, x1, y1, x2, y2)

        def rect(c, x, y, w, h, **kw):
            out.append(("rect", mm(x), top(y + h), mm(w), mm(h)))
            return orig_rect(c, x, y, w, h, **kw)

        def origin(t, x, y):
            if (x, y) != (0, 0):  # beginText() 先設 (0, 0)，不是畫的位置
                out.append(("text", mm(x), top(y)))
            return orig_origin(t, x, y)

        env = {"INVOICE_PRINT_SIGN_MODE": "", "RM_PRINT_HALF_PAGE_MM": "160"}
        with mock.patch.object(Canvas, "line", line), mock.patch.object(Canvas, "rect", rect), \
                mock.patch.object(PDFTextObject, "setTextOrigin", origin), mock.patch.dict(os.environ, env), \
                mock.patch.object(ipp, "HALF_SHIFT_MM", shift):
            _, info = ipp.render_pdf(data)
        return out, info

    def test_half_sheet_drawn_4mm_higher(self):
        # ST-2026307 半張：表頭、標題、頁碼、框、明細、合計、簽核全部比不上移的版面高 4.0mm，x 不變
        flat, _ = self._drawn(fixture("ST-2026307"), 0.0)
        moved, info = self._drawn(fixture("ST-2026307"), 4.0)
        self.assertEqual(info["bottom_mm"], 147.32)
        self.assertEqual(len(flat), len(moved))
        self.assertGreater(len(flat), 40)
        for a, b in zip(flat, moved):
            self.assertEqual(a[0], b[0])
            if a[0] == "line":
                self.assertEqual((a[1], a[3]), (b[1], b[3]))
                self.assertAlmostEqual(b[2], a[2] - 4.0, places=2)
                self.assertAlmostEqual(b[4], a[4] - 4.0, places=2)
            elif a[0] == "rect":
                self.assertEqual((a[1], a[3], a[4]), (b[1], b[3], b[4]))
                self.assertAlmostEqual(b[2], a[2] - 4.0, places=2)
            else:
                self.assertEqual(a[1], b[1])
                self.assertAlmostEqual(b[2], a[2] - 4.0, places=2)
        # 外框底 = bottom_mm（實際畫的位置）
        frame = [r for r in moved if r[0] == "rect"][0]
        self.assertAlmostEqual(frame[2] + frame[4], 147.32, places=2)

    def test_customer_name_printed(self):
        data = fixture("ST-2026019")
        data["customer"]["name"] = "NEXTEK  INC"
        calls, _ = self._calls(data)
        self.assertIn("客戶名稱: NEXTEK  INC", [s for *_, s in calls])


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
