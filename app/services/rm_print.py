"""進貨單（正航「進貨憑單」RM-xxxx）列印：產生與正航列印版面一致的 A4 PDF。

版面座標全部量自 Andy 的正航實際列印（RM-20261103001，Microsoft Print to PDF，A4），
單位 mm、原點左上、y 為文字基線（baseline）。字型：正航用新細明體（PMingLiU）。

字型策略（見 resolve_fonts）：
  1. RM_PRINT_FONT_PATH（可指到 Windows 的 mingliu.ttc，subfont 1 = PMingLiU）→ 完全相同字形與字寬。
  2. 否則：中文用 AR PL UMing TW（文鼎明體，免費授權），英數用 Liberation Serif（Times 等寬相容），
     英數每段依 PMingLiU 實際字寬（下方 PMINGLIU_LATIN 表，量自正航 PDF 內嵌字型）水平縮放，
     所以每個字的位置、斷行點都和正航一樣，只有字形略有差異。

純函式：render_pdf(data) / layout(data) 不連 ERP，可單元測試。
"""
from __future__ import annotations

import io
import os
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple

PAGE_W = 210.0
PAGE_H = 297.0
HALF_PAGE_MM = 148.5
PAGE_BOTTOM_MM = 287.0  # 明細 / 頁尾可用到的最低位置（A4 下緣留 10mm）

SIZE = 9.96          # 內文字級 pt（正航 10pt @96dpi → PDF 9.96）
TITLE_SIZE = 18.0    # 公司名稱
SUBTITLE_SIZE = 15.96  # 進貨憑單
LINE_W = 0.72        # 線寬 pt
DESCENT_MM = 0.70    # 內文字下緣（基線下）

# 表頭
TITLE_CX, TITLE_BASE = 102.66, 15.20
SUB_CX, SUB_BASE = 103.335, 23.03
SUB_UL = (24.60, 25.15)
SUB_UL_PAD = 0.885
HEAD_X = (6.69, 74.92, 132.51)
PAGE_NO_X = 189.68
HEAD_BASE = (30.69, 35.60, 41.06, 46.52, 51.99, 57.45, 62.65, 67.86)
VENDOR_VALUE_X = 23.88
RULE_X = (6.01, 200.91)
RULE_TOP, RULE_COLS = 31.96, 69.93
COL_HEAD_BASE = 73.70
COL_UL = (74.85, 75.40)
DETAIL_TOP = 75.40
ROW_BASE_OFFSET = 3.64   # 列頂 → 第一行基線
ROW_LINE_PITCH = 4.06    # 折行行距
ROW_PAD = 1.79           # 列高 = 行數 × 行距 + ROW_PAD
ROW_MIN_H = 5.85

# 明細欄位：(key, 標題, 底線 x0, 底線 x1, 對齊, 文字 x)
COLUMNS = (
    ("row_no", "欄號", 6.01, 15.58, "L", 6.69),
    ("prod_id", "產品編號", 16.09, 36.32, "L", 16.76),
    ("prod_name", "品名規格", 36.83, 73.96, "L", 37.51),
    ("ware", "倉庫", 74.80, 93.34, "L", 75.48),
    ("qty", "數量", 94.19, 113.83, "R", 113.17),
    ("unit", "單位", 114.38, 123.91, "L", 115.06),
    ("price", "單價", 124.46, 142.49, "R", 141.81),
    ("amount", "金額", 143.04, 162.43, "R", 161.77),
    ("amount_tax", "含稅金額", 162.98, 183.73, "R", 183.06),
    ("remark", "分錄備註", 184.23, 200.07, "L", 184.91),
)
WRAP_WIDTH = {"row_no": 9.56, "prod_id": 19.39, "ware": 17.47, "prod_name": 36.40, "remark": 15.10}
CLIP_WIDTH = {"unit": 8.8}

# 頁尾（相對於頁尾頂線 ftop）
FOOT_BASES = (3.64, 8.55, 13.50)
FOOT_VLINES_X = (121.45, 137.03, 163.24, 175.81)
FOOT_VLINE_SPAN = (0.55, 15.32)
FOOT_BOTTOM = 15.58
FOOT_BOTTOM_X = (6.01, 200.07)
FOOT_LEFT_X = (6.69, 45.42, 73.58)
FOOT_MID_X, FOOT_MID_RIGHT = 122.13, 161.72
FOOT_RIGHT_X, FOOT_RIGHT_RIGHT = 163.91, 200.24
SIGN_OFFSET = 39.29
DEFAULT_TERMS_ID = "ST"
DEFAULT_TERMS_TEXT = "\r\n"*5 + "簽核 " + "_"*18 + " "*49 + "審核 " + "_"*18 + " "*48 + "製單 " + "_"*18
TERMS_WRAP_PX = round(194.92 / 0.2730) - 4  # Memo6 寬 714px − 2×InternalGapX
TERMS_FIRST_BASE = SIGN_OFFSET - 5 * ROW_LINE_PITCH

# PMingLiU 英數字寬（1/1024 em），量自正航 PDF 內嵌字型；表上沒有的字 ≈ Times × 0.94
PMINGLIU_LATIN = {
    " ": 266, '"': 392, "(": 321, ")": 321, "+": 543, ",": 241, "-": 321, ".": 241, "/": 267, ":": 267,
    **{d: 481 for d in "0123456789"},
    "A": 694, "B": 642, "C": 642, "D": 694, "E": 588, "F": 535, "H": 694, "L": 588, "M": 855, "N": 694,
    "O": 694, "P": 535, "R": 642, "S": 535, "T": 588, "U": 694, "Y": 694, "Z": 588, "_": 481,
    "d": 481, "m": 749, "n": 481, "p": 481, "r": 321, "y": 481,
}
LATIN_SCALE = 0.94

DEFAULT_COMPANY = "辰尚企業有限公司"
def price_label(price_of_tax: Any) -> str:
    """正航 comBillAccounts.PriceOfTax：1 = 單價未稅、0 = 單價含稅（正航採購單版面同樣以 PriceOfTax 為真時取未稅金額）。
    沒讀到（None，例如正航連不上用本機紀錄）就留白。"""
    if price_of_tax is None:
        return ""
    return "未稅" if bool(price_of_tax) else "含稅"


def tax_type_text(tax_type: Any) -> str:
    """課稅類別：0 = 應稅，其他一律免稅（Andy 確認）。"""
    try:
        return "應稅" if int(tax_type or 0) == 0 else "免稅"
    except (TypeError, ValueError):
        return "應稅"


# ── 數字 / 日期格式 ─────────────────────────────────────────────────────────

def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def fmt_num(value: Any, digits: int) -> str:
    """千分位 + 固定小數位（四捨五入）。數量 4 位、單價 3 位、金額依幣別（台幣 0 位）。"""
    q = Decimal(1).scaleb(-digits) if digits > 0 else Decimal(1)
    d = _dec(value).quantize(q, rounding=ROUND_HALF_UP)
    if d == 0:
        d = abs(d)
    return f"{d:,.{max(digits, 0)}f}"


def roc_date(value: Any) -> str:
    """20261104 / date → 115/11/04（民國年）。空值回空字串。"""
    if value in (None, "", 0, "0"):
        return ""
    if isinstance(value, date):
        y, m, d = value.year, value.month, value.day
    else:
        s = "".join(ch for ch in str(value) if ch.isdigit())
        if len(s) != 8:
            return ""
        y, m, d = int(s[:4]), int(s[4:6]), int(s[6:])
    return f"{y - 1911}/{m:02d}/{d:02d}"


def pay_terms_text(gather_style: Any, gather_delay: Any, other: str = "") -> str:
    """付款條件文字。GatherStyle 1 = 次月 N 天（對過正航實單）；其他種類不自行編文字，
    只印正航單上的文字（stkBillMain.GatherOther），沒有就留白。"""
    try:
        style, delay = int(gather_style or 0), int(gather_delay or 0)
    except (TypeError, ValueError):
        style, delay = 0, 0
    if style == 1:
        return f"次月 {delay} 天"
    return (other or "").strip()


# ── 字型 ──────────────────────────────────────────────────────────────────

_FONT_CACHE: Dict[str, Any] = {}

MINGLIU_CANDIDATES = (
    "/app/data/fonts/mingliu.ttc",
    "/usr/share/fonts/truetype/mingliu/mingliu.ttc",
    "/usr/local/share/fonts/mingliu.ttc",
    "C:/Windows/Fonts/mingliu.ttc",
)
UMING_CANDIDATES = ("/usr/share/fonts/truetype/arphic/uming.ttc", "/usr/share/fonts/arphic/uming.ttc")
LATIN_CANDIDATES = (
    "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf",
    "/usr/share/fonts/truetype/liberation2/LiberationSerif-Regular.ttf",
    "/usr/share/fonts/truetype/croscore/Tinos-Regular.ttf",
)


def _first_existing(paths) -> Optional[str]:
    for p in paths:
        if p and os.path.isfile(p):
            return p
    return None


def resolve_fonts() -> Dict[str, Any]:
    """回傳 {"mode": "exact"|"subst", "cjk": 名稱, "latin": 名稱, "description": 說明}。"""
    if "fonts" in _FONT_CACHE:
        return _FONT_CACHE["fonts"]
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    exact = _first_existing([os.environ.get("RM_PRINT_FONT_PATH", "").strip()] + list(MINGLIU_CANDIDATES))
    if exact:
        index = int(os.environ.get("RM_PRINT_FONT_INDEX", "1" if exact.lower().endswith(".ttc") else "0"))
        pdfmetrics.registerFont(TTFont("RMMing", exact, subfontIndex=index))
        fonts = {"mode": "exact", "cjk": "RMMing", "latin": "RMMing",
                 "description": f"新細明體（{os.path.basename(exact)} #{index}）"}
    else:
        uming = _first_existing(UMING_CANDIDATES)
        latin = _first_existing(LATIN_CANDIDATES)
        if not (uming and latin):
            raise RuntimeError("找不到列印字型：請安裝 fonts-arphic-uming 與 fonts-liberation，或設定 RM_PRINT_FONT_PATH")
        pdfmetrics.registerFont(TTFont("RMUMing", uming, subfontIndex=2))  # AR PL UMing TW
        pdfmetrics.registerFont(TTFont("RMLatin", latin))
        fonts = {"mode": "subst", "cjk": "RMUMing", "latin": "RMLatin",
                 "description": "AR PL UMing TW（中文）+ Liberation Serif（英數，依新細明體字寬縮放）"}
    _FONT_CACHE["fonts"] = fonts
    return fonts


def _is_latin(ch: str) -> bool:
    return ord(ch) < 0x2000


def _char_em(ch: str, fonts: Dict[str, Any]) -> float:
    from reportlab.pdfbase.pdfmetrics import stringWidth
    if fonts["mode"] == "exact":
        return stringWidth(ch, fonts["cjk"], 1000) / 1000
    if not _is_latin(ch):
        return 1.0
    if ch in PMINGLIU_LATIN:
        return PMINGLIU_LATIN[ch] / 1024
    return stringWidth(ch, fonts["latin"], 1000) / 1000 * LATIN_SCALE


PT_MM = 25.4 / 72


def text_width(text: str, size: float = SIZE, fonts: Optional[Dict[str, Any]] = None) -> float:
    """文字寬度（mm），依新細明體字寬。"""
    fonts = fonts or resolve_fonts()
    return sum(_char_em(ch, fonts) for ch in text) * size * PT_MM


def _runs(text: str) -> List[Tuple[bool, str]]:
    out: List[Tuple[bool, str]] = []
    for ch in text:
        lat = _is_latin(ch)
        if out and out[-1][0] == lat:
            out[-1] = (lat, out[-1][1] + ch)
        else:
            out.append((lat, ch))
    return out


def wrap_text(text: str, width: float, size: float = SIZE, fonts: Optional[Dict[str, Any]] = None) -> List[str]:
    """正航（FastReport）式折行：英文以字為單位、行尾空白保留不計寬；單字超寬就逐字切；中文每字可斷。"""
    fonts = fonts or resolve_fonts()
    return _wrap_text(text, width, lambda t: text_width(t, size, fonts))


def _wrap_text(text: str, width: float, measure: Callable[[str], float]) -> List[str]:
    """FR2：空白、點、逗號、連字號後可斷，斷字保留行尾。"""
    lines: List[str] = []
    for para in str(text or "").replace("\r\n", "\n").split("\n"):
        tokens: List[str] = []
        for ch in para:
            if not _is_latin(ch):
                tokens.append(ch)
            elif ch == " " and tokens:
                tokens[-1] += ch
            elif tokens and _is_latin(tokens[-1][-1]) and tokens[-1][-1] not in " .,-":
                tokens[-1] += ch
            else:
                tokens.append(ch)
        line = ""
        for tok in tokens:
            if measure((line + tok).rstrip(" ")) <= width:
                line += tok
                continue
            if line:
                lines.append(line)
                line = ""
            while measure(tok.rstrip(" ")) > width and len(tok.rstrip(" ")) > 1:
                cut = 1
                while cut < len(tok) and measure(tok[:cut + 1].rstrip(" ")) <= width:
                    cut += 1
                lines.append(tok[:cut])
                tok = tok[cut:]
            line = tok
        lines.append(line)
    return lines or [""]


def terms_positions(text: str, fonts: Optional[Dict[str, Any]] = None) -> List[List[Tuple[float, str]]]:
    """條款以印表機 DC 整數字寬推進；空白只移位，不畫字。"""
    fonts = fonts or resolve_fonts()
    def advance(ch: str) -> float:
        return round(_char_em(ch, fonts) * 83) / 600 * 25.4
    def screen_px(t: str) -> int:
        return sum(round(_char_em(ch, fonts) * 13) for ch in t)
    # FR2 折行用螢幕字寬（96dpi、10pt = 13px）判斷、列印時用印表機字寬推進：
    # ST 簽核列螢幕寬 702px < 710px 不折行，但印出來會超出 memo 寬（同正航實印）。
    lines = _wrap_text(text, TERMS_WRAP_PX, screen_px) if text else []
    out = []
    for line in lines:
        x, runs = HEAD_X[0], []
        for word in line.split(" "):
            if word:
                runs.append((x, word))
            x += sum(advance(ch) for ch in word) + advance(" ")
        out.append(runs)
    return out


def _terms_text(data: Dict[str, Any]) -> str:
    text = data.get("terms_text")
    return DEFAULT_TERMS_TEXT if text is None else text


def footer_height(data: Dict[str, Any], fonts: Optional[Dict[str, Any]] = None) -> float:
    text = _terms_text(data)
    n = len(terms_positions(text, fonts)) if text else 0
    return (TERMS_FIRST_BASE + (n - 1) * ROW_LINE_PITCH if n else FOOT_BOTTOM) + DESCENT_MM


def _header_lines(data: Dict[str, Any], fonts: Dict[str, Any]) -> Dict[int, List[str]]:
    vendor = f"{data.get('vendor_id') or ''}   {data.get('vendor_name') or ''}".strip()
    return {2: wrap_text(vendor, 104.83, fonts=fonts),
            5: wrap_text(f"廠商地址: {data.get('vendor_address') or ''}", 123.40, fonts=fonts),
            6: wrap_text(f"發票地址: {data.get('invoice_address') or ''}", 192.19, fonts=fonts)}


def clip_text(text: str, width: float, size: float = SIZE, fonts: Optional[Dict[str, Any]] = None) -> str:
    fonts = fonts or resolve_fonts()
    s = str(text or "")
    while s and text_width(s.rstrip(" "), size, fonts) > width:
        s = s[:-1]
    return s


# ── 版面計算（純資料，不畫圖）─────────────────────────────────────────────

def _line_cells(line: Dict[str, Any], digits: int, fonts) -> Dict[str, List[str]]:
    amount = _dec(line.get("amount"))
    tax = _dec(line.get("tax"))
    raw = {
        "row_no": str(line.get("row_no") or ""),
        "prod_id": str(line.get("prod_id") or ""),
        "prod_name": str(line.get("prod_name") or ""),
        "ware": str(line.get("ware") or ""),
        "qty": fmt_num(line.get("qty"), 4),
        "unit": str(line.get("unit") or ""),
        "price": fmt_num(line.get("price"), 3),
        "amount": fmt_num(amount, digits),
        "amount_tax": fmt_num(amount + tax, digits),
        "remark": str(line.get("remark") or ""),
    }
    cells: Dict[str, List[str]] = {}
    for key, text in raw.items():
        if key in WRAP_WIDTH:
            cells[key] = wrap_text(text, WRAP_WIDTH[key], SIZE, fonts)
        elif key in CLIP_WIDTH:
            cells[key] = [clip_text(text, CLIP_WIDTH[key], SIZE, fonts)]
        else:
            cells[key] = [text]
    return cells


def row_height(n_lines: int) -> float:
    return max(ROW_MIN_H, ROW_PAD + ROW_LINE_PITCH * max(1, n_lines))


def totals(data: Dict[str, Any]) -> Dict[str, Decimal]:
    lines = data.get("lines") or []
    sum_amount = _dec(data["sum_amount"]) if data.get("sum_amount") is not None else sum(
        (_dec(l.get("amount")) for l in lines), Decimal(0))
    sum_tax = _dec(data["sum_tax"]) if data.get("sum_tax") is not None else sum(
        (_dec(l.get("tax")) for l in lines), Decimal(0))
    total = sum_amount + sum_tax
    discount = _dec(data.get("discount"))
    cash = _dec(data.get("cash_pay"))
    paid = cash + _dec(data.get("visa_pay")) + _dec(data.get("other_pay"))
    offset = _dec(data.get("offset"))
    amtatax = _dec(data["sum_amtatax"]) if data.get("sum_amtatax") is not None else total
    return {"sum_amount": sum_amount, "sum_tax": sum_tax, "total": total, "discount": discount,
            "cash_pay": cash, "paid": paid, "offset": offset,
            "unpaid": amtatax - paid - offset - (discount - _dec(data.get("dist_offset")))}


def layout(data: Dict[str, Any], fonts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """分頁：每頁重複表頭；明細放不下換頁；頁尾（合計 + 簽核）只在最後一頁。"""
    fonts = fonts or resolve_fonts()
    digits = int(data.get("amount_digits") or 0)
    header_extra = sum(len(v) - 1 for v in _header_lines(data, fonts).values()) * ROW_LINE_PITCH
    fh = footer_height(data, fonts)
    rows = []
    for line in data.get("lines") or []:
        cells = _line_cells(line, digits, fonts)
        n = max(len(v) for v in cells.values())
        rows.append({"cells": cells, "height": row_height(n), "lines": n})
    pages: List[Dict[str, Any]] = [{"rows": [], "footer_top": None}]
    y = DETAIL_TOP + header_extra
    for r in rows:
        if pages[-1]["rows"] and y + r["height"] > PAGE_BOTTOM_MM:
            pages.append({"rows": [], "footer_top": None})
            y = DETAIL_TOP + header_extra
        pages[-1]["rows"].append({**r, "top": y})
        y += r["height"]
    if y + fh > PAGE_BOTTOM_MM:
        pages.append({"rows": [], "footer_top": None})
        y = DETAIL_TOP + header_extra
    pages[-1]["footer_top"] = y
    bottom = y + fh
    return {"pages": pages, "page_count": len(pages), "bottom_mm": round(bottom, 2),
            "fits_half_page": len(pages) == 1 and bottom <= half_page_limit(), "fonts": fonts, "header_extra": header_extra}


def half_page_limit() -> float:
    try:
        return float(os.environ.get("RM_PRINT_HALF_PAGE_MM", HALF_PAGE_MM))
    except ValueError:
        return HALF_PAGE_MM


def max_single_line_rows_in_half_page() -> int:
    avail = half_page_limit() - DETAIL_TOP - footer_height({})
    n = 0
    while row_height(1) * (n + 1) <= avail + 1e-9:
        n += 1
    return n


# ── 印表機選擇 ──────────────────────────────────────────────────────────

DEFAULT_PRINTER_HALF = "NPI975BEF (HP LaserJet M111w)"
DEFAULT_PRINTER_FULL = "HP Color LaserJet M652 [7D2006]"


def printers() -> Dict[str, str]:
    return {
        "half": os.environ.get("RM_PRINT_PRINTER_HALF", "").strip() or DEFAULT_PRINTER_HALF,
        "full": os.environ.get("RM_PRINT_PRINTER_FULL", "").strip() or DEFAULT_PRINTER_FULL,
    }


def choose_printer(fits_half_page: bool) -> str:
    p = printers()
    return p["half"] if fits_half_page else p["full"]


# ── 繪圖 ──────────────────────────────────────────────────────────────────

class _Painter:
    def __init__(self, c, fonts):
        self.c = c
        self.fonts = fonts

    def text(self, x: float, base: float, s: str, size: float = SIZE) -> None:
        from reportlab.pdfbase.pdfmetrics import stringWidth
        if not s:
            return
        c, f = self.c, self.fonts
        y = (PAGE_H - base) / PT_MM
        cx = x
        for lat, run in _runs(s):
            w_mm = text_width(run, size, f)
            # 水平縮放（Tz）屬於圖形狀態，會延續到下一段文字，所以每段都明確設定
            t = c.beginText()
            if f["mode"] == "subst" and lat:
                natural = stringWidth(run, f["latin"], size) * PT_MM
                t.setFont(f["latin"], size)
                t.setHorizScale(100.0 * w_mm / natural if natural else 100.0)
            else:
                t.setFont(f["cjk"], size)
                t.setHorizScale(100.0)
            t.setTextOrigin(cx / PT_MM, y)
            t.textOut(run)
            c.drawText(t)
            cx += w_mm

    def text_right(self, right: float, base: float, s: str, size: float = SIZE) -> None:
        self.text(right - text_width(s, size, self.fonts), base, s, size)

    def text_center(self, cx: float, base: float, s: str, size: float) -> Tuple[float, float]:
        w = text_width(s, size, self.fonts)
        self.text(cx - w / 2, base, s, size)
        return cx - w / 2, cx + w / 2

    def hline(self, x0: float, x1: float, y: float) -> None:
        self.c.line(x0 / PT_MM, (PAGE_H - y) / PT_MM, x1 / PT_MM, (PAGE_H - y) / PT_MM)

    def vline(self, x: float, y0: float, y1: float) -> None:
        self.c.line(x / PT_MM, (PAGE_H - y0) / PT_MM, x / PT_MM, (PAGE_H - y1) / PT_MM)


def page_label(page_no: int, page_count: int) -> str:
    """正航 [MyPage]：每張單據從 1 起算。"""
    return str(page_no)


def _header(p: _Painter, data: Dict[str, Any], page_no: int, page_count: int, header_extra: float = 0) -> None:
    f = p.fonts
    p.text_center(TITLE_CX, TITLE_BASE, data.get("company_name") or DEFAULT_COMPANY, TITLE_SIZE)
    x0, x1 = p.text_center(SUB_CX, SUB_BASE, "進貨憑單", SUBTITLE_SIZE)
    for y in SUB_UL:
        p.hline(x0 - SUB_UL_PAD, x1 + SUB_UL_PAD, y)
    page_text = page_label(page_no, page_count)
    wrapped = _header_lines(data, f)
    b, extra = [], 0.0
    for row, base in enumerate(HEAD_BASE):
        b.append(base + extra)
        extra += (len(wrapped.get(row, [""])) - 1) * ROW_LINE_PITCH

    def memo(row, x):
        for i, text in enumerate(wrapped[row]):
            p.text(x, b[row] + i * ROW_LINE_PITCH, text)
    p.text(HEAD_X[0], b[0], f"列印日期: {roc_date(data.get('print_date') or date.today())}")
    p.text(PAGE_NO_X, b[0], f"頁碼: {page_text}")
    p.hline(*RULE_X, RULE_TOP)
    w2 = HEAD_X[1] - HEAD_X[0] - 1.0   # 左欄可用寬
    w3 = HEAD_X[2] - HEAD_X[1] - 1.0
    wr = RULE_X[1] - HEAD_X[2]

    def cell(col: int, row: int, text: str, width: float) -> None:
        p.text(HEAD_X[col], b[row], clip_text(text, width, SIZE, f))

    cell(0, 1, f"進貨日期: {roc_date(data.get('rm_date'))}", w2)
    cell(1, 1, f"進貨單號: {data.get('rm_no') or ''}", w3)
    cell(2, 1, f"公司電話: {data.get('vendor_phone') or ''}", wr)
    p.text(HEAD_X[0], b[2], "廠商名稱: ")
    memo(2, VENDOR_VALUE_X)
    cell(2, 2, f"統一編號: {data.get('tax_no') or ''}", wr)
    cell(0, 3, f"聯絡電話: {data.get('contact_phone') or ''}", w2)
    cell(1, 3, f"傳真號碼: {data.get('fax') or ''}", w3)
    cell(2, 3, f"產品單價: {price_label(data.get('price_of_tax'))}", wr)
    cell(0, 4, f"幣別編號: {data.get('currency') or 'NTD'}", w2)
    cell(1, 4, f"採購人員: {data.get('buyer') or ''}", w3)
    cell(2, 4, f"所屬部門: {data.get('dept') or ''}", wr)
    memo(5, HEAD_X[0])
    cell(2, 5, f"製單人員: {data.get('maker') or ''}", wr)
    memo(6, HEAD_X[0])
    cell(0, 7, f"付款條件: {data.get('pay_terms') or ''}", w2)
    cell(1, 7, f"預付日期: {roc_date(data.get('prepay_date'))}", w3)
    cell(2, 7, f"預兌日期: {roc_date(data.get('encash_date'))}", wr)
    p.hline(*RULE_X, RULE_COLS + header_extra)
    for key, title, u0, u1, align, tx in COLUMNS:
        if align == "R":
            p.text_right(tx, COL_HEAD_BASE + header_extra, title)
        else:
            p.text(tx, COL_HEAD_BASE + header_extra, title)
        for y in COL_UL:
            p.hline(u0, u1, y + header_extra)


def _rows(p: _Painter, rows: List[Dict[str, Any]]) -> None:
    for r in rows:
        for key, title, u0, u1, align, tx in COLUMNS:
            for i, s in enumerate(r["cells"][key]):
                base = r["top"] + ROW_BASE_OFFSET + i * ROW_LINE_PITCH
                if align == "R":
                    p.text_right(tx, base, s)
                else:
                    p.text(tx, base, s)


def _footer(p: _Painter, data: Dict[str, Any], ftop: float) -> None:
    f = p.fonts
    digits = int(data.get("amount_digits") or 0)
    t = totals(data)
    p.hline(*RULE_X, ftop)
    b = [ftop + d for d in FOOT_BASES]
    p.text(FOOT_LEFT_X[0], b[0], clip_text(f"發票日期: {roc_date(data.get('invoice_date'))}", FOOT_LEFT_X[1] - FOOT_LEFT_X[0] - 0.5, SIZE, f))
    p.text(FOOT_LEFT_X[1], b[0], f"課稅類別: {tax_type_text(data.get('tax_type'))}")
    p.text(FOOT_LEFT_X[2], b[0], clip_text(f"發票號碼: {data.get('invoice_no') or ''}", FOOT_VLINES_X[0] - FOOT_LEFT_X[2] - 0.5, SIZE, f))
    p.text(FOOT_LEFT_X[0], b[1], f"發票聯式: {data.get('invoice_style') or ''}")
    p.text(FOOT_LEFT_X[0], b[2], clip_text(f"備註: {data.get('remark') or ''}", FOOT_VLINES_X[0] - FOOT_LEFT_X[0] - 0.5, SIZE, f))
    for i, (label, key) in enumerate((("折讓: ", "discount"), ("現付: ", "paid"), ("未付款: ", "unpaid"))):
        p.text(FOOT_MID_X, b[i], label)
        p.text_right(FOOT_MID_RIGHT, b[i], fmt_num(t[key], digits))
    for i, (label, key) in enumerate((("合計: ", "sum_amount"), ("稅金: ", "sum_tax"), ("總計: ", "total"))):
        p.text(FOOT_RIGHT_X, b[i], label)
        p.text_right(FOOT_RIGHT_RIGHT, b[i], fmt_num(t[key], digits))
    for x in FOOT_VLINES_X:
        p.vline(x, ftop + FOOT_VLINE_SPAN[0], ftop + FOOT_VLINE_SPAN[1])
    p.hline(*FOOT_BOTTOM_X, ftop + FOOT_BOTTOM)
    for i, runs in enumerate(terms_positions(_terms_text(data), f)):
        for x, run in runs:
            p.text(x, ftop + TERMS_FIRST_BASE + i * ROW_LINE_PITCH, run)


def render_pdf(data: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
    """回傳 (PDF bytes, 版面資訊 {page_count, bottom_mm, fits_half_page, printer, font})。"""
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    fonts = resolve_fonts()
    lay = layout(data, fonts)
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4, pageCompression=1)
    c.setTitle(str(data.get("rm_no") or "進貨憑單"))
    c.setAuthor(data.get("company_name") or DEFAULT_COMPANY)
    p = _Painter(c, fonts)
    for i, page in enumerate(lay["pages"], 1):
        c.setLineWidth(LINE_W)
        c.setStrokeColorRGB(0, 0, 0)
        c.setFillColorRGB(0, 0, 0)
        _header(p, data, i, lay["page_count"], lay["header_extra"])
        _rows(p, page["rows"])
        if page["footer_top"] is not None:
            _footer(p, data, page["footer_top"])
        c.showPage()
    c.save()
    return buf.getvalue(), info_from_layout(lay)


def info_from_layout(lay: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "page_count": lay["page_count"], "bottom_mm": lay["bottom_mm"], "fits_half_page": lay["fits_half_page"],
        "half_page_mm": half_page_limit(), "printer": choose_printer(lay["fits_half_page"]),
        "paper": "A4 上半張（半張紙）" if lay["fits_half_page"] else "A4",
        "printers": printers(), "font": lay["fonts"]["description"], "font_mode": lay["fonts"]["mode"],
    }


def print_info(data: Dict[str, Any]) -> Dict[str, Any]:
    return info_from_layout(layout(data))
