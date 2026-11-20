"""開 Invoice 列印（正航「銷貨單」KPM5-INVOICE）：伺服器產生 A4 PDF，沿用進貨單列印的選印表機 / 送印流程。

版面座標量自正航實際列印的 ST-2026304（759px 寬螢幕截圖 = A4 寬 210mm，1px = 210/759 mm），
單位 mm、原點左上、y 為文字基線（baseline）。英數用 Arial 相容字型（Liberation Sans / Arimo / Arial，
都沒有就用 PDF 內建 Helvetica，字寬相同），中文沿用 rm_print.resolve_fonts()（新細明體或文鼎明體）。

品名折行：正航 FR2 以螢幕 DC（96dpi、10pt = 13px）的 Arial 整數字寬折行（字型 hdmx 表，下方 ARIAL_13PX），
不是線性字寬——用線性字寬 ST-2026304 的兩個品名不可能同時折成正航的樣子。限寬 DESC_WRAP_PX。

選印表機與進貨單相同：整張（含合計、簽核）放得進 A4 上半張（rm_print.half_page_limit()，預設 148.5mm）
→ rm_print.choose_printer(True)（NPI975BEF，A4 紙只印上半張）；否則 A4（M652），可多頁。

簽核列位置（環境變數 INVOICE_PRINT_SIGN_MODE）：
  auto（預設）：半張表單框固定高度、簽核在框底（同正航 ST-2026304 實單）；A4 長單簽核緊接合計下方、框隨內容收尾。
  follow：一律緊接合計下方，框在簽核下方收尾（半張也一樣）。
  bottom：一律貼框底（半張框底 HALF_FRAME_BOTTOM、A4 框底 PAGE_BOTTOM_MM）。
半張與否只看內容放不放得下（三種模式相同），模式只影響簽核與框底位置。

純函式：layout(data) / render_pdf(data) 不連 ERP；資料來自 erp_invoice_print.get_print_data()。
"""
from __future__ import annotations

import io
import os
import unicodedata
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import rm_print
from .rm_print import PAGE_BOTTOM_MM, PAGE_H, PT_MM, fmt_num

PX = 210.0 / 759.0  # 實單截圖 1px → mm

SIZE = 9.96             # 內文（10pt @96dpi）
COMPANY_SIZE = 18.0
ADDRESS_SIZE = 9.5
TITLE_SIZE = 15.96
PAGE_NO_SIZE = 9.0
TERM_SIZE = 9.0
SIGN_SIZE = 11.0
LINE_W = 0.72           # 細線 pt
FRAME_LINE_W = 2.2      # 外框 pt

# 表頭
CENTER_X = 375 * PX
COMPANY_BASE, ADDRESS_BASE, TEL_BASE = 85 * PX, 107 * PX, 127 * PX
TITLE_BASE = 165 * PX
TITLE_UL = (337 * PX, 414 * PX, 171 * PX)  # x0, x1, y
PAGE_NO_RIGHT, PAGE_NO_BASE = 731 * PX, 186 * PX

# 框與格線
FRAME_X = (19 * PX, 734 * PX)
FRAME_TOP = 191 * PX
INNER_X = (24 * PX, 729 * PX)
ROW_A = (195 * PX, 215 * PX)
ROW_A_DIV = 478 * PX
ROW_B = (216 * PX, 234 * PX)
HEAD_ROW = (235 * PX, 255 * PX)
HEAD_VLINES = (157 * PX, 529 * PX, 624 * PX)
FRAME_PAD = ROW_A[0] - FRAME_TOP  # 內框線 → 外框

TEXT_X = 26 * PX
DATE_X = 483 * PX
ROW_A_BASE, ROW_B_BASE, HEAD_BASE = 209 * PX, 229 * PX, 249 * PX
HEAD_DESC_X = 158 * PX
HEAD_PRICE_RIGHT, HEAD_AMOUNT_RIGHT = 622 * PX, 718 * PX
TERM_CX, TERM_BASE = 606 * PX, 272 * PX

# 明細
QTY_RIGHT = 117 * PX
DESC_X = 159 * PX
PRICE_RIGHT = 622 * PX
AMOUNT_RIGHT = 708 * PX
DESC_MAX_W = HEAD_VLINES[1] - DESC_X - 0.3  # 畫的時候超過才水平壓縮（折行看 DESC_WRAP_PX）
BODY_FIRST_BASE = 291 * PX   # 每頁第一個元素（通常是 PO 標題）的基線
PO_TO_LINE = 22 * PX         # PO 標題 → 該 PO 第一列
DESC_PITCH = 15 * PX         # 品名折行行距
ITEM_GAP = 17 * PX           # 一列最後一行 → 下一個 PO 標題 / 下一列
DESC_MAX_LINES = 3
DESC_WRAP_PX = 365           # ST-2026304：要放得下的最寬行 364px、要折掉的最窄行 367px

# 合計與簽核
TOTALS_RULE_GAP = 3.0        # 最後一行 → 合計細線（非最後一頁的收尾細線同位置）
TOTALS_BASE_GAP = 4.6        # 合計細線 → 合計文字基線
DOUBLE_UL = (1.0, 1.6)       # 合計雙底線（基線下）
QTY_UL = (52 * PX, 119 * PX)
AMOUNT_UL = (612 * PX, 711 * PX)
SIGN_GAP = 12.0              # 合計基線 → 簽核基線（follow）
SIGN_TO_FRAME_FOLLOW = 4.5   # 簽核基線 → 框底（follow）
SIGN_TO_FRAME_PINNED = (544 - 513) * PX  # 簽核貼框底時（同實單）
SIGN_X = (30 * PX, 279 * PX, 528 * PX)
SIGN_UL = ((65 * PX, 190 * PX), (315 * PX, 440 * PX), (565 * PX, 690 * PX))
SIGN_UL_DY = 0.6
DEFAULT_SIGNATURES = ("簽核", "審核", "製單")

# 半張：框底固定（實單 150.5mm，整體上移到 146.5mm，印表機只印上 148.5mm）
HALF_FRAME_BOTTOM = 146.5
HALF_SIGN_BASE = HALF_FRAME_BOTTOM - SIGN_TO_FRAME_PINNED
A4_SIGN_MAX = PAGE_BOTTOM_MM - SIGN_TO_FRAME_PINNED  # A4 最後一頁合計 + 簽核要放得下

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
    """一列明細 = 一個單位；PO 換了（非空）才帶 PO 標題（同 erp_invoice_print.js buildUnits）。"""
    units, prev = [], None
    for i, line in enumerate(print_lines(data)):
        po = str(line.get("po") or "").strip()
        units.append({
            "index": i, "po": po, "header": bool(po) and po != prev,
            "desc": wrap_description(line.get("print_description") or line.get("description") or "", measure),
            "qty": line.get("qty_display") or fmt_num(line.get("qty"), 4),
            "price": line.get("price_display") or fmt_num(line.get("price"), 3),
            "amount": line.get("amount_display") or fmt_num(line.get("amount"), 3),
        })
        prev = po
    return units


def _place_page(units: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], float]:
    """依序排一頁的單位 → (元素, 最後一行基線)；頁首接續中的 PO 重印標題。空頁的「最後一行」= 貿易條件列。"""
    elements: List[Dict[str, Any]] = []
    y, last = BODY_FIRST_BASE, TERM_BASE
    for k, u in enumerate(units):
        reprint = k == 0 and bool(u["po"]) and not u["header"]
        if u["header"] or reprint:
            elements.append({"type": "po", "po": u["po"], "base": y, "reprint": reprint})
            y += PO_TO_LINE
        elements.append({"type": "line", **u, "base": y})
        last = y + (len(u["desc"]) - 1) * DESC_PITCH
        y = last + ITEM_GAP
    return elements, last


def _last_base(units: List[Dict[str, Any]]) -> float:
    return _place_page(units)[1]


def _totals_base(last: float) -> float:
    return last + TOTALS_RULE_GAP + TOTALS_BASE_GAP


def _follow_sign_base(last: float) -> float:
    return _totals_base(last) + SIGN_GAP


def _paginate_a4(units: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    pages: List[List[Dict[str, Any]]] = [[]]
    for u in units:
        if pages[-1] and _last_base(pages[-1] + [u]) + TOTALS_RULE_GAP + FRAME_PAD > PAGE_BOTTOM_MM:
            pages.append([])
        pages[-1].append(u)
    # 最後一頁放不下合計 + 簽核：把最後一個單位移到新的一頁（最後一頁不會只有合計）
    if len(pages[-1]) > 1 and _follow_sign_base(_last_base(pages[-1])) > A4_SIGN_MAX:
        pages.append([pages[-1].pop()])
    return pages


def layout(data: Dict[str, Any], mode: Optional[str] = None, measure=None) -> Dict[str, Any]:
    mode = mode if mode in SIGN_MODES else sign_mode()
    units = _units(data, measure)
    half_limit = rm_print.half_page_limit()
    last = _last_base(units)
    half_frame = _follow_sign_base(last) + SIGN_TO_FRAME_FOLLOW if mode == "follow" else HALF_FRAME_BOTTOM
    half = _follow_sign_base(last) <= HALF_SIGN_BASE and half_frame <= half_limit
    groups = [units] if half else _paginate_a4(units)
    pages = []
    for no, group in enumerate(groups, 1):
        elements, last = _place_page(group)
        page = {"no": no, "elements": elements, "last": no == len(groups), "last_base": last}
        if page["last"]:
            page["totals_rule"] = last + TOTALS_RULE_GAP
            page["totals_base"] = _totals_base(last)
            if mode == "follow" or (not half and mode == "auto"):
                page["sign_base"] = _follow_sign_base(last)
                page["frame_bottom"] = page["sign_base"] + SIGN_TO_FRAME_FOLLOW
            else:
                page["frame_bottom"] = HALF_FRAME_BOTTOM if half else PAGE_BOTTOM_MM
                page["sign_base"] = page["frame_bottom"] - SIGN_TO_FRAME_PINNED
        else:
            page["close_rule"] = last + TOTALS_RULE_GAP
            page["frame_bottom"] = page["close_rule"] + FRAME_PAD
        pages.append(page)
    bottom = pages[-1]["frame_bottom"]
    return {"pages": pages, "page_count": len(pages), "bottom_mm": round(bottom, 2), "fits_half_page": half,
            "form": "half" if half else "a4", "sign_mode": mode, "half_page_mm": half_limit,
            "bill_no": data.get("bill_no") or ""}


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
    p.text_center(CENTER_X, COMPANY_BASE, company.get("name") or "", COMPANY_SIZE)
    p.text_center(CENTER_X, ADDRESS_BASE, company.get("address") or "", ADDRESS_SIZE)
    p.text_center(CENTER_X, TEL_BASE, f"TEL: {company.get('tel') or ''} FAX: {company.get('fax') or ''}", ADDRESS_SIZE)
    p.text_center(CENTER_X, TITLE_BASE, data.get("title") or "銷貨單", TITLE_SIZE)
    p.hline(*TITLE_UL)
    p.text_right(PAGE_NO_RIGHT, PAGE_NO_BASE, f"Page {page_no} of {page_count}", PAGE_NO_SIZE)

    p.c.setLineWidth(FRAME_LINE_W)
    p.box(FRAME_X[0], FRAME_X[1], FRAME_TOP, frame_bottom)
    p.c.setLineWidth(LINE_W)
    p.box(INNER_X[0], INNER_X[1], *ROW_A)
    p.vline(ROW_A_DIV, *ROW_A)
    p.box(INNER_X[0], INNER_X[1], *ROW_B)
    p.box(INNER_X[0], INNER_X[1], *HEAD_ROW)
    for x in HEAD_VLINES:
        p.vline(x, *HEAD_ROW)

    p.text(TEXT_X, ROW_A_BASE, f"INVOICE:  {data.get('bill_no') or ''}")
    p.text(DATE_X, ROW_A_BASE, f"DATE:  {data.get('date_display') or ''}")
    customer = (data.get("customer") or {}).get("name") or ""
    p.text(TEXT_X, ROW_B_BASE, f"客戶名稱: {customer}", max_width=INNER_X[1] - TEXT_X - 1.0)
    p.text(TEXT_X, HEAD_BASE, "QTY")
    p.text(HEAD_DESC_X, HEAD_BASE, "DESCRIPTION")
    p.text_right(HEAD_PRICE_RIGHT, HEAD_BASE, "UNIT PRICE")
    p.text_right(HEAD_AMOUNT_RIGHT, HEAD_BASE, "TTL AMOUNT")
    term = data.get("trade_term") or ""
    if term:
        x0, x1 = p.text_center(TERM_CX, TERM_BASE, term, TERM_SIZE)
        p.hline(x0, x1, TERM_BASE + 0.7)


def _body(p: _Painter, page: Dict[str, Any]) -> None:
    for el in page["elements"]:
        if el["type"] == "po":
            p.text(DESC_X, el["base"], f"PO#{el['po']}", bold=True)
            continue
        p.text_right(QTY_RIGHT, el["base"], el["qty"])
        p.text_right(PRICE_RIGHT, el["base"], el["price"])
        p.text_right(AMOUNT_RIGHT, el["base"], el["amount"])
        for i, s in enumerate(el["desc"]):
            p.text(DESC_X, el["base"] + i * DESC_PITCH, s, max_width=DESC_MAX_W)


def _double_ul(p: _Painter, x0: float, x1: float, base: float) -> None:
    for dy in DOUBLE_UL:
        p.hline(x0, x1, base + dy)


def _footer(p: _Painter, data: Dict[str, Any], page: Dict[str, Any]) -> None:
    if not page["last"]:
        p.hline(*INNER_X, page["close_rule"])
        return
    t = data.get("totals") or {}
    p.hline(*INNER_X, page["totals_rule"])
    base = page["totals_base"]
    p.text_right(QTY_RIGHT, base, t.get("qty_display") or fmt_num(t.get("qty"), 4))
    _double_ul(p, *QTY_UL, base)
    amount = t.get("amount_display") or fmt_num(t.get("amount"), 3)
    p.text_right(AMOUNT_RIGHT, base, f"{data.get('currency') or ''} {amount}".strip())
    _double_ul(p, *AMOUNT_UL, base)
    labels = list(data.get("signatures") or DEFAULT_SIGNATURES)[:3]
    for x, (u0, u1), label in zip(SIGN_X, SIGN_UL, labels):
        p.text(x, page["sign_base"], label, SIGN_SIZE)
        p.hline(u0, u1, page["sign_base"] + SIGN_UL_DY)


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
