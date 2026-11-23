"""開 Invoice 列印（正航「銷貨單」KPM5-INVOICE）：伺服器產生 A4 PDF，沿用進貨單列印的選印表機 / 送印流程。

版面座標量自正航列印 ST-2026019 到「Microsoft Print to PDF」的向量 PDF（A4，2 頁），
單位 mm、原點左上、y 為文字基線（baseline）。英數用 Arial 相容字型（Liberation Sans / Arimo / Arial，
都沒有就用 PDF 內建 Helvetica，字寬相同），中文沿用 rm_print.resolve_fonts()（新細明體或文鼎明體）。

品名折行：正航 FR2 以螢幕 DC（96dpi、10pt = 13px）的 Arial 整數字寬折行（字型 hdmx 表，下方 ARIAL_13PX），
不是線性字寬——用線性字寬 ST-2026304 的兩個品名不可能同時折成正航的樣子。限寬 DESC_WRAP_PX = 366：
ST-2026019 "ABXAAAX04-003 P/N:415-0213-00, Connector Shell, 4.3-10(M)," 366px 不折，
ST-2026304 "IC-NB675GL-Z IC, 10A, 5V-24V INPUT SYNCHRONOUS BUCK" 367px 要折。

明細（band 模型，同正航）：每一列 = PO band（PO 非空的每一列都印粗體 PO#，同一 PO 連續多列也每列都印）
+ 明細 band，一個接一個往下排；每頁第一個 PO 基線 80.90。band 底超過頁尾 band 頂 FOOTER_TOP（262.3）
的那一列移到下一頁（ST-2026019：第 1 頁 14 列、第 2 頁 6 列 + 合計）。TTL AMOUNT 數值靠左。

選印表機與進貨單相同：整張（含合計、簽核）放得進 A4 上半張（rm_print.half_page_limit()，預設 148.5mm）
→ rm_print.choose_printer(True)（NPI975BEF，A4 紙只印上半張）；否則 A4（M652），可多頁。

簽核列每一頁都印：非最後一頁在頁尾 band（基線 FOOTER_SIGN_BASE），框到頁底 A4_FRAME_BOTTOM，
最後一列下面不畫收尾線。最後一頁 / 半張的簽核位置（環境變數 INVOICE_PRINT_SIGN_MODE）：
  follow / auto（預設，auto = follow）：簽核一律緊接合計下方（固定距離 SIGN_GAP），框在簽核下方收尾
    （SIGN_TO_FRAME_FOLLOW），半張也一樣；明細 + 合計 + 簽核 + 框底邊距的框底 <= 半張上限就用半張
    （例 ST-2026304 框底 132.44mm），否則 A4，可多頁（合計只在最後一頁）。
  bottom（舊版，需明確指定）：一律貼框底（半張框底 HALF_FRAME_BOTTOM、A4 框底 A4_FRAME_BOTTOM），
    所以半張要簽核貼框底放得下才用。
print-info 的 bottom_mm 就是 PDF 實際畫的框底（同一份 layout）。

純函式：layout(data) / render_pdf(data) 不連 ERP；資料來自 erp_invoice_print.get_print_data()。
"""
from __future__ import annotations

import io
import os
import unicodedata
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import rm_print
from .rm_print import PAGE_H, PT_MM, fmt_num

SIZE = 9.96             # 內文（10pt @96dpi），表頭地址 / 頁碼 / 貿易條件 / 簽核同大小（正航向量 PDF 實量）
COMPANY_SIZE = 18.0
TITLE_SIZE = 18.0
LINE_W = 0.72           # 細線 pt
FRAME_LINE_W = 2.28     # 外框 pt

# 表頭（文字中心各自量自正航 ST-2026019，公司名稱與地址不同中心）
COMPANY_CX, COMPANY_BASE = 101.40, 24.60
ADDRESS_CX, ADDRESS_BASE = 103.06, 30.48
TEL_CX, TEL_BASE = 103.59, 35.69
TITLE_CX, TITLE_BASE = 103.17, 45.89
TITLE_UL = (92.8, 113.5, 48.1)  # x0, x1, y
PAGE_NO_RIGHT, PAGE_NO_BASE = 200.24, 51.77

# 框與格線：外粗框 + 內細框（內框左右細線一路到底），表頭三列
FRAME_X = (6.0, 200.9)
FRAME_TOP = 53.6
INNER_X = (7.1, 199.8)
INNER_TOP = 54.4
INNER_BOTTOM_PAD = 1.4       # 內框底線 → 外框底（正航 281.4 / 282.8）
ROW_A_BOTTOM, ROW_A_DIV = 60.1, 131.0
ROW_B_BOTTOM = 65.3
HEAD_BOTTOM = 70.8
HEAD_VLINES = (43.4, 145.2, 171.4)

TEXT_X = 7.79
INVOICE_VALUE_X = 24.69      # 單號、日期是獨立欄位（不是「標籤 + 空白」）
DATE_X, DATE_VALUE_X = 132.80, 144.83
ROW_A_BASE, ROW_B_BASE, HEAD_BASE = 58.08, 63.54, 69.26
HEAD_DESC_X = 44.08
HEAD_PRICE_RIGHT = 170.74
HEAD_AMOUNT_CX = 185.6       # TTL AMOUNT 在欄內置中（174.3–196.9）
TERM_BASE, TERM_UL_Y = 75.27, 76.2  # 貿易條件（"FOB " 靠單價欄右緣），底線到單價欄右界 171.4

# 明細（band 模型：每個單位 = PO band + 明細 band，一個接一個往下排）
QTY_RIGHT = 32.65
PO_X = 44.37                 # 粗體 PO#
DESC_X = 44.07
PRICE_RIGHT = 170.74
AMOUNT_X = 176.50            # TTL AMOUNT 數值靠左（正航不是靠右）
DESC_MAX_W = HEAD_VLINES[1] - DESC_X - 0.3  # 畫的時候超過才水平壓縮（折行看 DESC_WRAP_PX）
BODY_TOP = 76.8              # 每頁第一個 band 的頂（貿易條件列底）
BAND_TO_BASE = 4.10          # band 頂 → 第一行基線（每頁第一個 PO 基線 80.90）
PO_TO_LINE = 5.88            # PO 基線 → 明細第一行基線
DESC_PITCH = 4.06            # 品名折行行距
LINE_TO_NEXT_MIN = 5.59      # 單行明細：明細基線 → 下一個 PO 基線（單位間距 11.47）
LAST_DESC_TO_NEXT = 5.36     # 折行明細：最後一行基線 → 下一個 PO 基線（2 行 9.42、3 行 13.48）
DESC_MAX_LINES = 3
DESC_WRAP_PX = 366           # ST-2026019：366px 不折；ST-2026304：要折掉的最窄行 367px

# 頁尾（非最後一頁：框到頁底，簽核在頁尾 band）
FOOTER_TOP = 262.3           # 頁尾 band 頂：明細 band 底超過就換頁
FOOTER_SIGN_BASE = 273.85
A4_FRAME_BOTTOM = 282.8

# 合計與簽核（最後一頁 / 半張：簽核緊接合計，框在簽核下方收尾）
TOTALS_RULE_GAP = 0.2        # 最後一個 band 底 → 合計細線
TOTALS_BASE_GAP = 4.28       # 合計細線 → 合計數量基線
AMOUNT_BASE_DY = 0.30        # 合計金額基線比數量低一點（正航 161.88 / 162.18）
DOUBLE_UL = (1.3, 1.8)       # 合計數量雙底線（數量基線下）
AMOUNT_DOUBLE_UL = (1.2, 1.7)  # 合計金額雙底線（金額基線下）
QTY_UL = (12.3, 33.3)
AMOUNT_UL = (166.8, 194.9)
CURRENCY_FIELD = (166.8, 175.8)  # 幣別在這格內置中，金額靠左 AMOUNT_X
SIGN_GAP = 12.0              # 合計基線 → 簽核基線（follow）
SIGN_TO_FRAME_FOLLOW = 4.5   # 簽核基線 → 框底（follow）
SIGN_TO_FRAME_PINNED = A4_FRAME_BOTTOM - FOOTER_SIGN_BASE  # 簽核貼框底時（頁尾 band / bottom 模式）
SIGN_X = (8.85, 73.4, 138.0)
SIGN_SIZE = SIZE
SIGN_UNDERSCORES = 18        # 每個簽核標籤後面「空白 + 18 個底線」（同正航）
DEFAULT_SIGNATURES = ("簽核", "審核", "製單")

# 半張 bottom 模式：框底固定（實單 150.5mm，整體上移到 146.5mm，印表機只印上 148.5mm）
HALF_FRAME_BOTTOM = 146.5
HALF_SIGN_BASE = HALF_FRAME_BOTTOM - SIGN_TO_FRAME_PINNED
A4_SIGN_MAX = FOOTER_SIGN_BASE  # A4 最後一頁：緊接合計的簽核不能低於頁尾簽核位置

SIGN_MODES = ("auto", "follow", "bottom")

# Arial 10pt 螢幕字寬（96dpi = 13ppem，px），取自 Windows arial.ttf 的 hdmx 表（GDI 實際推進量）
ARIAL_13PX = {
    **dict(zip(" !\"#$%&'()*+,-./", (4, 3, 5, 7, 7, 12, 9, 2, 4, 4, 5, 8, 4, 4, 4, 4))),
    **{d: 7 for d in "0123456789"},
    **dict(zip(":;<=>?@", (4, 4, 8, 8, 8, 7, 13))),
    **dict(zip("ABCDEFGHIJKLMNOPQRSTUVWXYZ", (9, 9, 9, 9, 9, 8, 10, 9, 3, 6, 9, 7, 11, 9, 10, 9, 10, 9, 9, 7, 9, 9,
                                               13, 7, 9, 7))),
    **dict(zip("[\\]^_`", (4, 4, 4, 5, 7, 4))),
    **dict(zip("abcdefghijklmnopqrstuvwxyz", (7, 7, 7, 7, 7, 3, 7, 7, 3, 3, 7, 3, 11, 7, 7, 7, 7, 4, 7, 4, 7, 5, 9,
                                               7, 7, 7))),
    **dict(zip("{|}~", (4, 3, 4, 8))),
    "‘": 3, "’": 3, "“": 4, "”": 4, "°": 5, "±": 7, "µ": 7, "×": 8, "–": 7, "—": 13, "Ω": 10,
}


def sign_mode() -> str:
    mode = os.environ.get("INVOICE_PRINT_SIGN_MODE", "").strip().lower()
    return mode if mode in SIGN_MODES else "auto"


# ── 字型 ──────────────────────────────────────────────────────────────────

_FONT_CACHE: Dict[str, Any] = {}

LATIN_CANDIDATES = (
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/croscore/Arimo-Regular.ttf",
    "C:/Windows/Fonts/arial.ttf",
)
LATIN_BOLD_CANDIDATES = (
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/croscore/Arimo-Bold.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
)


def latin_font_path() -> Optional[str]:
    return rm_print._first_existing([os.environ.get("INVOICE_PRINT_LATIN_FONT_PATH", "").strip()] + list(LATIN_CANDIDATES))


def resolve_fonts() -> Dict[str, Any]:
    """{"cjk", "latin", "latin_bold", "builtin", "description"}；中文找不到時 rm_print 會丟 RuntimeError。"""
    if "fonts" in _FONT_CACHE:
        return _FONT_CACHE["fonts"]
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    cjk = rm_print.resolve_fonts()
    regular = latin_font_path()
    bold = rm_print._first_existing([os.environ.get("INVOICE_PRINT_LATIN_BOLD_FONT_PATH", "").strip()] + list(LATIN_BOLD_CANDIDATES))
    if regular:
        pdfmetrics.registerFont(TTFont("InvSans", regular))
        latin, latin_desc = "InvSans", os.path.splitext(os.path.basename(regular))[0]
        if bold:
            pdfmetrics.registerFont(TTFont("InvSansBold", bold))
        latin_bold = "InvSansBold" if bold else "InvSans"
    else:
        latin, latin_bold, latin_desc = "Helvetica", "Helvetica-Bold", "Helvetica（PDF 內建）"
    fonts = {"cjk": cjk["cjk"], "latin": latin, "latin_bold": latin_bold, "builtin": not regular,
             "description": f"{latin_desc}（英數）+ {cjk['description']}（中文）"}
    _FONT_CACHE["fonts"] = fonts
    return fonts


def _wide(ch: str) -> bool:
    return unicodedata.east_asian_width(ch) in ("W", "F")


def _is_latin(ch: str, fonts: Dict[str, Any]) -> bool:
    if _wide(ch):
        return False
    if fonts.get("builtin"):
        try:
            ch.encode("cp1252")
        except UnicodeEncodeError:
            return False
    return True


def screen_px(text: str) -> int:
    """正航折行用的螢幕字寬（px）：Arial 13ppem 表；表外全形字 13px，其他依 Helvetica（= Arial）線性字寬四捨五入。"""
    from reportlab.pdfbase.pdfmetrics import stringWidth
    total = 0
    for ch in text:
        if ch in ARIAL_13PX:
            total += ARIAL_13PX[ch]
        elif _wide(ch):
            total += 13
        else:
            total += round(stringWidth(ch, "Helvetica", 1000) / 1000 * 13)
    return total


def wrap_description(text: Any, measure: Optional[Callable[[str], float]] = None,
                     limit_px: float = DESC_WRAP_PX) -> List[str]:
    """品名折行（只在空格斷行，同 rm_print），最多 DESC_MAX_LINES 行、超過直接裁掉（不加省略號）。"""
    lines = rm_print._wrap_text(str(text or ""), limit_px, measure or screen_px)
    return lines[:DESC_MAX_LINES]


# ── 版面計算（純資料，不畫圖、不需要字型）─────────────────────────────────

def print_lines(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [l for l in data.get("lines") or [] if not l.get("excluded_tab")]


def _units(data: Dict[str, Any], measure=None) -> List[Dict[str, Any]]:
    """一列明細 = 一個單位；PO 非空的每一列都帶 PO 標題（同一 PO 連續多列也每列都印，同正航；
    瀏覽器預覽頁照 preview_pages 畫）。"""
    units = []
    for i, line in enumerate(print_lines(data)):
        po = str(line.get("po") or "").strip()
        units.append({
            "index": i, "po": po, "header": bool(po),
            "desc": wrap_description(line.get("print_description") or line.get("description") or "", measure),
            "qty": line.get("qty_display") or fmt_num(line.get("qty"), 4),
            "price": line.get("price_display") or fmt_num(line.get("price"), 3),
            "amount": line.get("amount_display") or fmt_num(line.get("amount"), 3),
        })
    return units


def _next_base(line_base: float, n_lines: int) -> float:
    """明細第一行基線 → 下一個單位第一行基線（正航：1 行 5.59、2 行 9.42、3 行 13.48）。"""
    return line_base + max(LINE_TO_NEXT_MIN, (n_lines - 1) * DESC_PITCH + LAST_DESC_TO_NEXT)


def _place_page(units: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], float, float]:
    """依序排一頁的單位 → (元素, 最後一個 band 底, 最後一行基線)。空頁：band 底 = BODY_TOP、最後一行 = 貿易條件列。"""
    elements: List[Dict[str, Any]] = []
    top, last = BODY_TOP, TERM_BASE
    for u in units:
        y = top + BAND_TO_BASE
        if u["header"]:
            elements.append({"type": "po", "po": u["po"], "base": y})
            y += PO_TO_LINE
        last = y + (len(u["desc"]) - 1) * DESC_PITCH
        bottom = _next_base(y, len(u["desc"])) - BAND_TO_BASE
        elements.append({"type": "line", **u, "base": y, "band_top": top, "band_bottom": bottom})
        top = bottom
    return elements, top, last


def _band_bottom(units: List[Dict[str, Any]]) -> float:
    return _place_page(units)[1]


def _totals_base(band_bottom: float) -> float:
    return band_bottom + TOTALS_RULE_GAP + TOTALS_BASE_GAP


def _follow_sign_base(band_bottom: float) -> float:
    return _totals_base(band_bottom) + SIGN_GAP


def _paginate_a4(units: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """band 底超過頁尾 band 頂（FOOTER_TOP）的單位移到下一頁。"""
    pages: List[List[Dict[str, Any]]] = [[]]
    for u in units:
        if pages[-1] and _band_bottom(pages[-1] + [u]) > FOOTER_TOP:
            pages.append([])
        pages[-1].append(u)
    # 最後一頁放不下合計 + 簽核：把最後一個單位移到新的一頁（最後一頁不會只有合計）
    if len(pages[-1]) > 1 and _follow_sign_base(_band_bottom(pages[-1])) > A4_SIGN_MAX:
        pages.append([pages[-1].pop()])
    return pages


def layout(data: Dict[str, Any], mode: Optional[str] = None, measure=None) -> Dict[str, Any]:
    mode = mode if mode in SIGN_MODES else sign_mode()
    units = _units(data, measure)
    half_limit = rm_print.half_page_limit()
    bottom = _band_bottom(units)
    # follow / auto：簽核緊接合計，整份（含框底邊距）放得進半張就用半張；bottom：簽核貼框底放得下才用半張
    follow = mode != "bottom"
    if follow:
        half = _follow_sign_base(bottom) + SIGN_TO_FRAME_FOLLOW <= half_limit
    else:
        half = _follow_sign_base(bottom) <= HALF_SIGN_BASE and HALF_FRAME_BOTTOM <= half_limit
    groups = [units] if half else _paginate_a4(units)
    pages = []
    for no, group in enumerate(groups, 1):
        elements, bottom, last = _place_page(group)
        page = {"no": no, "elements": elements, "last": no == len(groups), "last_base": last, "band_bottom": bottom}
        if page["last"]:
            page["totals_rule"] = bottom + TOTALS_RULE_GAP
            page["totals_base"] = _totals_base(bottom)
            if follow:
                page["sign_base"] = _follow_sign_base(bottom)
                page["frame_bottom"] = page["sign_base"] + SIGN_TO_FRAME_FOLLOW
            else:
                page["frame_bottom"] = HALF_FRAME_BOTTOM if half else A4_FRAME_BOTTOM
                page["sign_base"] = page["frame_bottom"] - SIGN_TO_FRAME_PINNED
        else:
            # 非最後一頁：框到頁底、簽核在頁尾 band（同正航），最後一列下面不畫收尾線
            page["sign_base"] = FOOTER_SIGN_BASE
            page["frame_bottom"] = A4_FRAME_BOTTOM
        pages.append(page)
    frame_bottom = pages[-1]["frame_bottom"]
    return {"pages": pages, "page_count": len(pages), "bottom_mm": round(frame_bottom, 2), "fits_half_page": half,
            "form": "half" if half else "a4", "sign_mode": mode, "half_page_mm": half_limit,
            "bill_no": data.get("bill_no") or ""}


def _mm(value: float) -> float:
    return round(value, 3)


def preview_pages(lay: Dict[str, Any]) -> List[Dict[str, Any]]:
    """瀏覽器預覽頁（erp_invoice_print.js）照畫的分頁：每頁的明細（PO# 基線、明細基線、PDF 折好的品名行）、
    簽核 / 合計 / 框底位置（mm），和 PDF 同一份 layout。"""
    pages = []
    for page in lay["pages"]:
        lines, po_base = [], None
        for el in page["elements"]:
            if el["type"] == "po":
                po_base = el["base"]
                continue
            lines.append({"index": el["index"], "po": el["po"], "po_base": _mm(po_base) if el["header"] else None,
                          "base": _mm(el["base"]), "desc": list(el["desc"]),
                          "qty": el["qty"], "price": el["price"], "amount": el["amount"]})
            po_base = None
        out = {"no": page["no"], "of": lay["page_count"], "last": page["last"], "lines": lines,
               "sign_base": _mm(page["sign_base"]), "frame_bottom": _mm(page["frame_bottom"])}
        if page["last"]:
            out["totals_rule"] = _mm(page["totals_rule"])
            out["totals_base"] = _mm(page["totals_base"])
        pages.append(out)
    return pages


def _plain_width(text: str, size: float = SIZE) -> float:
    """不需註冊字型的字寬（mm）：英數用 Helvetica（= Arial / Liberation Sans 字寬），全形字算 1 em。"""
    from reportlab.pdfbase.pdfmetrics import stringWidth
    return sum(size if _wide(ch) else stringWidth(ch, "Helvetica", size) for ch in text) * PT_MM


def preview_geometry(data: Dict[str, Any]) -> Dict[str, Any]:
    """預覽頁用的版面常數（mm，字級也換成 mm），和 PDF 的 _header / _body / _footer 同一組數字。"""
    term = data.get("trade_term") or ""
    term_x = PRICE_RIGHT - _plain_width(term + " ") if term else None
    return {
        "size": SIZE * PT_MM, "company_size": COMPANY_SIZE * PT_MM, "title_size": TITLE_SIZE * PT_MM,
        "line_w": LINE_W * PT_MM, "frame_line_w": FRAME_LINE_W * PT_MM,
        "company": [COMPANY_CX, COMPANY_BASE], "address": [ADDRESS_CX, ADDRESS_BASE], "tel": [TEL_CX, TEL_BASE],
        "title": [TITLE_CX, TITLE_BASE], "title_ul": list(TITLE_UL), "page_no": [PAGE_NO_RIGHT, PAGE_NO_BASE],
        "frame_x": list(FRAME_X), "frame_top": FRAME_TOP, "inner_x": list(INNER_X), "inner_top": INNER_TOP,
        "inner_bottom_pad": INNER_BOTTOM_PAD, "row_a_bottom": ROW_A_BOTTOM, "row_a_div": ROW_A_DIV,
        "row_b_bottom": ROW_B_BOTTOM, "head_bottom": HEAD_BOTTOM, "head_vlines": list(HEAD_VLINES),
        "text_x": TEXT_X, "invoice_value_x": INVOICE_VALUE_X, "date_x": DATE_X, "date_value_x": DATE_VALUE_X,
        "row_a_base": ROW_A_BASE, "row_b_base": ROW_B_BASE, "head_base": HEAD_BASE, "head_desc_x": HEAD_DESC_X,
        "head_price_right": HEAD_PRICE_RIGHT, "head_amount_cx": HEAD_AMOUNT_CX,
        "customer_max_w": INNER_X[1] - TEXT_X - 1.0,
        "term_x": term_x, "term_base": TERM_BASE, "term_ul_y": TERM_UL_Y,
        "qty_right": QTY_RIGHT, "po_x": PO_X, "desc_x": DESC_X, "desc_max_w": DESC_MAX_W, "desc_pitch": DESC_PITCH,
        "price_right": PRICE_RIGHT, "amount_x": AMOUNT_X,
        "amount_base_dy": AMOUNT_BASE_DY, "double_ul": list(DOUBLE_UL), "amount_double_ul": list(AMOUNT_DOUBLE_UL),
        "qty_ul": list(QTY_UL), "amount_ul": list(AMOUNT_UL), "currency_cx": sum(CURRENCY_FIELD) / 2,
        "sign_x": list(SIGN_X), "sign_underscores": SIGN_UNDERSCORES,
    }


def info_from_layout(lay: Dict[str, Any], fonts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = {
        "bill_no": lay["bill_no"], "page_count": lay["page_count"], "bottom_mm": lay["bottom_mm"],
        "fits_half_page": lay["fits_half_page"], "half_page_mm": lay["half_page_mm"],
        "printer": rm_print.choose_printer(lay["fits_half_page"]),
        "paper": "A4 上半張（半張紙）" if lay["fits_half_page"] else "A4",
        "printers": rm_print.printers(), "form": lay["form"], "sign_mode": lay["sign_mode"],
    }
    if fonts:
        out["font"] = fonts["description"]
    return out


# ── 繪圖 ──────────────────────────────────────────────────────────────────

class _Painter:
    def __init__(self, c, fonts):
        self.c = c
        self.fonts = fonts

    def _runs(self, s: str) -> List[Tuple[bool, str]]:
        out: List[Tuple[bool, str]] = []
        for ch in s:
            lat = _is_latin(ch, self.fonts)
            if out and out[-1][0] == lat:
                out[-1] = (lat, out[-1][1] + ch)
            else:
                out.append((lat, ch))
        return out

    def _font(self, latin: bool, bold: bool) -> str:
        if not latin:
            return self.fonts["cjk"]
        return self.fonts["latin_bold" if bold else "latin"]

    def width(self, s: str, size: float = SIZE, bold: bool = False) -> float:
        from reportlab.pdfbase.pdfmetrics import stringWidth
        return sum(stringWidth(run, self._font(lat, bold), size) for lat, run in self._runs(s)) * PT_MM

    def text(self, x: float, base: float, s: str, size: float = SIZE, bold: bool = False,
             max_width: Optional[float] = None) -> Tuple[float, float]:
        """畫字 → (x0, x1)；max_width 有給且超過時整段水平壓縮。"""
        from reportlab.pdfbase.pdfmetrics import stringWidth
        if not s:
            return x, x
        natural = self.width(s, size, bold)
        scale = max_width / natural if max_width and natural > max_width else 1.0
        cx, y = x, (PAGE_H - base) / PT_MM
        for lat, run in self._runs(s):
            font = self._font(lat, bold)
            # 水平縮放（Tz）會延續到下一段文字，所以每段都明確設定
            t = self.c.beginText()
            t.setFont(font, size)
            t.setHorizScale(100.0 * scale)
            t.setTextOrigin(cx / PT_MM, y)
            t.textOut(run)
            self.c.drawText(t)
            cx += stringWidth(run, font, size) * PT_MM * scale
        return x, cx

    def text_right(self, right: float, base: float, s: str, size: float = SIZE, bold: bool = False) -> None:
        self.text(right - self.width(s, size, bold), base, s, size, bold)

    def text_center(self, cx: float, base: float, s: str, size: float = SIZE) -> Tuple[float, float]:
        return self.text(cx - self.width(s, size) / 2, base, s, size)

    def hline(self, x0: float, x1: float, y: float) -> None:
        self.c.line(x0 / PT_MM, (PAGE_H - y) / PT_MM, x1 / PT_MM, (PAGE_H - y) / PT_MM)

    def vline(self, x: float, y0: float, y1: float) -> None:
        self.c.line(x / PT_MM, (PAGE_H - y0) / PT_MM, x / PT_MM, (PAGE_H - y1) / PT_MM)

    def box(self, x0: float, x1: float, y0: float, y1: float) -> None:
        self.c.rect(x0 / PT_MM, (PAGE_H - y1) / PT_MM, (x1 - x0) / PT_MM, (y1 - y0) / PT_MM, stroke=1, fill=0)


def _header(p: _Painter, data: Dict[str, Any], page_no: int, page_count: int, frame_bottom: float) -> None:
    company = data.get("company") or {}
    p.text_center(COMPANY_CX, COMPANY_BASE, company.get("name") or "", COMPANY_SIZE)
    p.text_center(ADDRESS_CX, ADDRESS_BASE, company.get("address") or "")
    p.text_center(TEL_CX, TEL_BASE, f"TEL: {company.get('tel') or ''} FAX: {company.get('fax') or ''}")
    p.text_center(TITLE_CX, TITLE_BASE, data.get("title") or "銷貨單", TITLE_SIZE)
    p.hline(*TITLE_UL)
    p.text_right(PAGE_NO_RIGHT, PAGE_NO_BASE, f"Page {page_no} of {page_count}")

    p.c.setLineWidth(FRAME_LINE_W)
    p.box(FRAME_X[0], FRAME_X[1], FRAME_TOP, frame_bottom)
    p.c.setLineWidth(LINE_W)
    p.box(INNER_X[0], INNER_X[1], INNER_TOP, frame_bottom - INNER_BOTTOM_PAD)
    for y in (ROW_A_BOTTOM, ROW_B_BOTTOM, HEAD_BOTTOM):
        p.hline(*INNER_X, y)
    p.vline(ROW_A_DIV, INNER_TOP, ROW_A_BOTTOM)
    for x in HEAD_VLINES:
        p.vline(x, ROW_B_BOTTOM, HEAD_BOTTOM)

    p.text(TEXT_X, ROW_A_BASE, "INVOICE:")
    p.text(INVOICE_VALUE_X, ROW_A_BASE, data.get("bill_no") or "")
    p.text(DATE_X, ROW_A_BASE, "DATE:")
    p.text(DATE_VALUE_X, ROW_A_BASE, data.get("date_display") or "")
    customer = (data.get("customer") or {}).get("name") or ""
    p.text(TEXT_X, ROW_B_BASE, f"客戶名稱: {customer}", max_width=INNER_X[1] - TEXT_X - 1.0)
    p.text(TEXT_X, HEAD_BASE, "QTY")
    p.text(HEAD_DESC_X, HEAD_BASE, "DESCRIPTION")
    p.text_right(HEAD_PRICE_RIGHT, HEAD_BASE, "UNIT PRICE")
    p.text_center(HEAD_AMOUNT_CX, HEAD_BASE, "TTL AMOUNT")
    term = data.get("trade_term") or ""
    if term:
        x0 = PRICE_RIGHT - p.width(term + " ")
        p.text(x0, TERM_BASE, term)
        p.hline(x0 - 0.9, HEAD_VLINES[2], TERM_UL_Y)


def _body(p: _Painter, page: Dict[str, Any]) -> None:
    for el in page["elements"]:
        if el["type"] == "po":
            p.text(PO_X, el["base"], f"PO#{el['po']}", bold=True)
            continue
        p.text_right(QTY_RIGHT, el["base"], el["qty"])
        p.text_right(PRICE_RIGHT, el["base"], el["price"])
        p.text(AMOUNT_X, el["base"], el["amount"])
        for i, s in enumerate(el["desc"]):
            p.text(DESC_X, el["base"] + i * DESC_PITCH, s, max_width=DESC_MAX_W)


def _double_ul(p: _Painter, x0: float, x1: float, base: float, dys: Tuple[float, float]) -> None:
    for dy in dys:
        p.hline(x0, x1, base + dy)


def _footer(p: _Painter, data: Dict[str, Any], page: Dict[str, Any]) -> None:
    if page["last"]:
        t = data.get("totals") or {}
        p.hline(*INNER_X, page["totals_rule"])
        base = page["totals_base"]
        p.text_right(QTY_RIGHT, base, t.get("qty_display") or fmt_num(t.get("qty"), 4))
        _double_ul(p, *QTY_UL, base, DOUBLE_UL)
        amount_base = base + AMOUNT_BASE_DY
        currency = data.get("currency") or ""
        if currency:
            p.text_center(sum(CURRENCY_FIELD) / 2, amount_base, currency)
        p.text(AMOUNT_X, amount_base, t.get("amount_display") or fmt_num(t.get("amount"), 3))
        _double_ul(p, *AMOUNT_UL, amount_base, AMOUNT_DOUBLE_UL)
    # 簽核每一頁都印（非最後一頁在頁尾 band，最後一頁緊接合計）：標籤 + 空白 + 底線字元，同正航
    labels = list(data.get("signatures") or DEFAULT_SIGNATURES)[:3]
    for x, label in zip(SIGN_X, labels):
        p.text(x, page["sign_base"], f"{label} " + "_" * SIGN_UNDERSCORES, SIGN_SIZE)


def render_pdf(data: Dict[str, Any], mode: Optional[str] = None) -> Tuple[bytes, Dict[str, Any]]:
    """回傳 (PDF bytes, 資訊 {page_count, bottom_mm, fits_half_page, printer, paper, printers, font, bill_no, ...})。"""
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    fonts = resolve_fonts()
    lay = layout(data, mode)
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4, pageCompression=1)
    c.setTitle(str(data.get("bill_no") or "Invoice"))
    c.setAuthor((data.get("company") or {}).get("name") or "")
    p = _Painter(c, fonts)
    for page in lay["pages"]:
        c.setLineWidth(LINE_W)
        c.setStrokeColorRGB(0, 0, 0)
        c.setFillColorRGB(0, 0, 0)
        _header(p, data, page["no"], lay["page_count"], page["frame_bottom"])
        _body(p, page)
        _footer(p, data, page)
        c.showPage()
    c.save()
    return buf.getvalue(), info_from_layout(lay, fonts)
