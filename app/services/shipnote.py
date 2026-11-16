"""出貨單（v2026.11.16.1）：從正航 S/C 或 Invoice 帶品項，套 OPENTEXT 出貨單模板匯出 xlsx。

正航只讀（erp_stock._connect 唯讀連線，資料庫必須是 CHIComp03），只 SELECT：
- S/C：TraBillMain（Flag=2）JOIN TraBillSub；客戶 PO 在 TraBillMain.Ref（CustBillNo 是空的）。
  同一 PO 會拆後綴（4500059497、4500059497-A、-B），搜尋 PO 列出精確 Ref 與「PO-」後綴，各自保留完整 Ref。
- Invoice（Flag=501，ST-…）：表頭 Ref/CustID/Total 常是空的，數量單價在 comProdRec；
  客戶 PO 走 comProdRec.FromNO → TraBillMain(Flag=2).Ref。
- 製令 prdMKOrdMain.CustBill 的數量是製令量，不拿來帶品項。
- -TAB 客供料（料號結尾 -TAB，不分大小寫）一律排除。

品名：淨重主檔有（以料號對到，沿用 sea_freight._find_spec）用主檔名稱，沒有用正航 Description。
HS code 沿用 sea_freight._infer_harmonized_code，回傳空字串就留空。

匯出直接改模板的 sheet XML（不經 openpyxl 重存），欄寬、列高、合併、框線、字型、
列印設定與嘜頭上的 OPENTEXT 菱形繪圖原樣保留。
"""
from __future__ import annotations

import io
import math
import re
import zipfile
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from xml.sax.saxutils import escape

from ..config import BASE_DIR
from . import erp_stock
from .erp_stock import ErpError, ErpUnavailable
from .local_time import local_now
from .sea_freight import PACKING_SPEC_TEMPLATE, _extract_item_no, _find_spec, _infer_harmonized_code, _load_packing_specs

ERP_DATABASE = "CHIComp03"
CUSTOMER_SUPPLIED_SUFFIX = "-TAB"
MAX_KEYWORDS = 30

TEMPLATE_DIR = BASE_DIR / "templates"
# 類型 → (模板檔、聯數、標題、檔名字尾、PO 前綴)
SHIPNOTE_TYPES: Dict[str, Dict[str, Any]] = {
    "general": {"label": "一般", "template": "OPENTEXT 261113出貨單.xlsx", "sections": 3, "suffix": "出貨單", "po_prefix": "PO:"},
    "ups": {"label": "UPS", "template": "OPENTEXT 261113出貨單.xlsx", "sections": 3, "suffix": "出貨單-UPS", "po_prefix": "PO:"},
    "sea": {"label": "海運", "template": "OPENTEXT 261028海運出貨單.xlsx", "sections": 2, "suffix": "海運出貨單", "po_prefix": "PO: "},
    "billing": {"label": "請款", "template": "OPENTEXT 261109請款單.xlsx", "sections": 2, "suffix": "請款單", "po_prefix": "PO:"},
}
MARKS = ("HILLIARD", "WAUKESHA")
DEFAULT_MAKER = "Andy"

# 模板版面：每聯 33 列；每聯 6 格品項，每格 4 列
SECTION_ROWS = 33
SLOTS_PER_SECTION = 6
SLOT_FIRST_ROW = 6
SLOT_ROWS = 4
DATE_FORMAT_CODE = "yyyy/mm/dd"


class ShipnoteError(ErpError):
    """出貨單輸入錯誤（訊息為繁體中文）。"""


def _text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def _num(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def is_customer_supplied(item_no: Any) -> bool:
    return _text(item_no).upper().endswith(CUSTOMER_SUPPLIED_SUFFIX)


# ── 搜尋 ────────────────────────────────────────────────────────────────

def parse_keywords(value: Any) -> List[str]:
    """多張單號 / PO：空白、逗號、頓號、換行分隔；去重保留順序，單號一律大寫。"""
    if isinstance(value, (list, tuple)):
        parts = [p for v in value for p in re.split(r"[\s,，、;；]+", _text(v))]
    else:
        parts = re.split(r"[\s,，、;；]+", _text(value))
    out = list(dict.fromkeys(p.strip().upper() for p in parts if p.strip()))
    return out[:MAX_KEYWORDS]


def _is_invoice_keyword(keyword: str) -> bool:
    return keyword.upper().startswith("ST-")


def _like_escape(text: str) -> str:
    return text.replace("[", "[[]").replace("%", "[%]").replace("_", "[_]")


def build_sc_query(keywords: List[str]) -> Tuple[str, tuple]:
    """S/C 單號精確、客戶 PO（Ref）精確或「PO-」後綴。"""
    conds: List[str] = []
    params: List[str] = []
    for kw in keywords:
        conds.append("m.BillNo = %s OR m.Ref = %s OR m.Ref LIKE %s")
        params.extend([kw, kw, _like_escape(kw) + "-%"])
    sql = (
        "SELECT m.BillNo, m.Ref, m.BillDate, m.CustID, m.CurrID, "
        "s.RowNo, s.ItemNo, s.Description, s.Quantity, s.Price, s.Amount, s.QtyRemain "
        "FROM TraBillMain AS m "
        "JOIN TraBillSub AS s ON s.Flag = m.Flag AND s.BillNo = m.BillNo "
        "WHERE m.Flag = 2 AND (" + " OR ".join(f"({c})" for c in conds) + ") "
        "ORDER BY m.Ref, m.BillNo, s.RowNo"
    )
    return sql, tuple(params)


def build_invoice_query(bill_nos: List[str]) -> Tuple[str, tuple]:
    ph = ",".join(["%s"] * len(bill_nos))
    sql = (
        "SELECT r.BillNO, r.RowNO, r.ProdID, r.ProdName, r.Quantity, r.Price, r.Amount, "
        "r.FromNO, r.FromRow, sc.Ref, sc.CustID, sc.CurrID "
        "FROM comProdRec AS r "
        "LEFT JOIN TraBillMain AS sc ON sc.Flag = 2 AND sc.BillNo = r.FromNO "
        f"WHERE r.Flag = 501 AND r.BillNO IN ({ph}) "
        "ORDER BY r.BillNO, r.RowNO"
    )
    return sql, tuple(bill_nos)


def _rows(cur) -> List[Dict[str, Any]]:
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def fetch_erp_rows(keywords: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """唯讀查正航；回 (S/C 明細列, Invoice 明細列)。"""
    settings = erp_stock.get_settings()
    if _text(settings.get("database")) != ERP_DATABASE:
        raise ShipnoteError(f"出貨單只查正航 {ERP_DATABASE}，目前設定的資料庫不是 {ERP_DATABASE}")
    invoice_kws = [k for k in keywords if _is_invoice_keyword(k)]
    other_kws = [k for k in keywords if not _is_invoice_keyword(k)]
    conn = erp_stock._connect(settings)
    try:
        cur = conn.cursor()
        cur.execute("SELECT DB_NAME()")
        found = cur.fetchone()
        if not found or _text(found[0]) != ERP_DATABASE:
            raise ShipnoteError(f"出貨單只查正航 {ERP_DATABASE}，連線到的資料庫不符")
        sc_rows: List[Dict[str, Any]] = []
        inv_rows: List[Dict[str, Any]] = []
        if other_kws:
            cur.execute(*build_sc_query(other_kws))
            sc_rows = _rows(cur)
        if invoice_kws:
            cur.execute(*build_invoice_query(invoice_kws))
            inv_rows = _rows(cur)
        return sc_rows, inv_rows
    except ErpError:
        raise
    except Exception as exc:
        raise ErpUnavailable(f"ERP 查詢失敗：{type(exc).__name__}") from None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _sc_row_matches(row: Dict[str, Any], keyword: str) -> bool:
    bill_no = _text(row.get("BillNo")).upper()
    ref = _text(row.get("Ref")).upper()
    return bill_no == keyword or ref == keyword or ref.startswith(keyword + "-")


def map_sc_row(row: Dict[str, Any]) -> Dict[str, Any]:
    qty = _num(row.get("Quantity"))
    price = _num(row.get("Price"))
    amount = row.get("Amount")
    return {
        "source": "S/C",
        "cust_po": _text(row.get("Ref")),
        "bill_no": _text(row.get("BillNo")),
        "row_no": int(_num(row.get("RowNo"))),
        "sc_no": _text(row.get("BillNo")),
        "item_no": _text(row.get("ItemNo")),
        "erp_description": _text(row.get("Description")),
        "qty": qty,
        "price": price,
        "amount": _num(amount) if amount is not None else qty * price,
        "qty_remain": _num(row.get("QtyRemain")),
        "bill_date": _text(row.get("BillDate")),
        "cust_id": _text(row.get("CustID")),
        "curr_id": _text(row.get("CurrID")),
    }


def map_invoice_row(row: Dict[str, Any]) -> Dict[str, Any]:
    qty = _num(row.get("Quantity"))
    price = _num(row.get("Price"))
    amount = row.get("Amount")
    return {
        "source": "Invoice",
        "cust_po": _text(row.get("Ref")),  # FromNO → TraBillMain(Flag=2).Ref；對不到就空白
        "bill_no": _text(row.get("BillNO")),
        "row_no": int(_num(row.get("RowNO"))),
        "sc_no": _text(row.get("FromNO")),
        "item_no": _text(row.get("ProdID")),
        "erp_description": _text(row.get("ProdName")),
        "qty": qty,
        "price": price,
        "amount": _num(amount) if amount is not None else qty * price,
        "qty_remain": None,
        "bill_date": "",
        "cust_id": _text(row.get("CustID")),
        "curr_id": _text(row.get("CurrID")),
    }


def template_packing_specs(path=PACKING_SPEC_TEMPLATE) -> List[dict]:
    """templates 的淨重主檔原始名稱（含 PCB-A… 轉接板；_extract_item_no 認不出的料號留空，靠名稱比對）。"""
    specs = []
    for spec in _load_packing_specs(path):
        specs.append({**spec, "item_no": _extract_item_no(spec["name"]), "packing_name": spec["name"]})
    return specs


def get_packing_specs() -> List[dict]:
    """淨重主檔：系統內的包裝主檔（海運出貨畫面可改）優先，其次 templates 的淨重主檔。"""
    from .. import database as db

    try:
        specs = list(db.get_sea_packing_specs())
    except Exception:
        specs = []
    return specs + template_packing_specs()


def _name_has_item(name: str, item_no: str) -> bool:
    pattern = r"(?<![A-Z0-9-])" + re.escape(item_no.upper()) + r"(?![A-Z0-9])"
    return bool(re.search(pattern, name.upper()))


def match_packing_spec(item_no: str, specs: List[dict]) -> Optional[dict]:
    """沿用 sea_freight._find_spec（先比料號、再比名稱）；名稱比對要整段料號，PCB-A112 不會對到 PCB-A1121。"""
    spec = _find_spec(item_no, specs)
    if not spec:
        return None
    if _text(spec.get("item_no")).upper() == _text(item_no).upper():
        return spec
    if _name_has_item(_text(spec.get("packing_name") or spec.get("name")), _text(item_no)):
        return spec
    candidates = [
        sp for sp in specs
        if _name_has_item(_text(sp.get("packing_name") or sp.get("name")), _text(item_no))
    ]
    return candidates[0] if candidates else None


def apply_packing_spec(item: Dict[str, Any], specs: List[dict]) -> Dict[str, Any]:
    spec = match_packing_spec(item.get("item_no") or "", specs)
    name = _text((spec or {}).get("packing_name") or (spec or {}).get("name"))
    out = dict(item)
    out["name"] = name or item.get("erp_description") or ""
    out["spec_matched"] = bool(spec)
    for key in ("per_box_qty", "net_weight", "gross_weight", "volume"):
        value = _num((spec or {}).get(key))
        out[key] = value if spec and value else None  # 主檔沒有就空白讓人填
    out["harmonized_code"] = _infer_harmonized_code(out.get("item_no") or "", out.get("erp_description") or "")
    return out


def build_items(
    keywords: List[str],
    sc_rows: Iterable[Dict[str, Any]],
    inv_rows: Iterable[Dict[str, Any]],
    specs: List[dict],
) -> List[Dict[str, Any]]:
    sc_kws = [k for k in keywords if not _is_invoice_keyword(k)]
    inv_kws = set(k for k in keywords if _is_invoice_keyword(k))
    items: List[Dict[str, Any]] = []
    seen = set()
    for row in sc_rows:
        if not any(_sc_row_matches(row, kw) for kw in sc_kws):
            continue
        items.append(map_sc_row(row))
    for row in inv_rows:
        if _text(row.get("BillNO")).upper() not in inv_kws:
            continue
        items.append(map_invoice_row(row))
    out = []
    for item in items:
        if is_customer_supplied(item["item_no"]):
            continue
        key = (item["source"], item["bill_no"].upper(), item["row_no"])
        if key in seen:
            continue
        seen.add(key)
        item["key"] = f"{item['source']}|{item['bill_no']}|{item['row_no']}"
        out.append(apply_packing_spec(item, specs))
    return out


def search_items(
    keywords: Any,
    fetcher: Optional[Callable[[List[str]], Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]]] = None,
    specs: Optional[List[dict]] = None,
) -> Dict[str, Any]:
    kws = parse_keywords(keywords)
    if not kws:
        return {"keywords": [], "items": [], "not_found": []}
    sc_rows, inv_rows = (fetcher or fetch_erp_rows)(kws)
    items = build_items(kws, sc_rows, inv_rows, get_packing_specs() if specs is None else specs)
    not_found = []
    for kw in kws:
        if _is_invoice_keyword(kw):
            hit = any(i["source"] == "Invoice" and i["bill_no"].upper() == kw for i in items)
        else:
            hit = any(
                i["source"] == "S/C"
                and (i["bill_no"].upper() == kw or i["cust_po"].upper() == kw or i["cust_po"].upper().startswith(kw + "-"))
                for i in items
            )
        if not hit:
            not_found.append(kw)
    return {"keywords": kws, "items": items, "not_found": not_found}


# ── 匯出：XML 層級改模板 ─────────────────────────────────────────────────

_ROW_RE = re.compile(r"<row\b([^>]*?)(?:/>|>(.*?)</row>)", re.S)
_CELL_RE = re.compile(r"<c\b([^>]*?)(?:/>|>(.*?)</c>)", re.S)
_REF_RE = re.compile(r'\br="([A-Z]+)(\d+)"')
_S_RE = re.compile(r'\bs="(\d+)"')


def _col_index(col: str) -> int:
    n = 0
    for ch in col:
        n = n * 26 + (ord(ch) - 64)
    return n


def _col_letter(index: int) -> str:
    out = ""
    while index:
        index, rem = divmod(index - 1, 26)
        out = chr(65 + rem) + out
    return out


def _fmt_num(value: float) -> str:
    value = float(value)
    if math.isfinite(value) and value == int(value) and abs(value) < 1e15:
        return str(int(value))
    return repr(round(value, 10))


class _Sheet:
    """以 <c r="..."> 為單位改 sheetData；其他 XML（合併、列印設定、繪圖）原封不動。"""

    def __init__(self, xml: str):
        start = xml.index("<sheetData")
        open_end = xml.index(">", start) + 1
        if xml[open_end - 2] == "/":  # <sheetData/>
            self.prefix, self.suffix, body = xml[:start] + "<sheetData>", "</sheetData>" + xml[open_end:], ""
        else:
            end = xml.index("</sheetData>")
            self.prefix, self.suffix, body = xml[:open_end], xml[end:], xml[open_end:end]
        self.rows: Dict[int, Dict[str, Any]] = {}
        for m in _ROW_RE.finditer(body):
            attrs = m.group(1)
            num = int(re.search(r'\br="(\d+)"', attrs).group(1))
            cells: Dict[int, Tuple[str, str]] = {}
            for cm in _CELL_RE.finditer(m.group(2) or ""):
                ref = _REF_RE.search(cm.group(1))
                cells[_col_index(ref.group(1))] = (cm.group(1), cm.group(2) or "")
            self.rows[num] = {"attrs": attrs, "cells": cells}

    def style(self, ref: str) -> Optional[str]:
        col, row = re.match(r"([A-Z]+)(\d+)", ref).groups()
        cell = self.rows.get(int(row), {}).get("cells", {}).get(_col_index(col))
        if not cell:
            return None
        m = _S_RE.search(cell[0])
        return m.group(1) if m else None

    def raw(self, ref: str) -> str:
        col, row = re.match(r"([A-Z]+)(\d+)", ref).groups()
        cell = self.rows.get(int(row), {}).get("cells", {}).get(_col_index(col))
        return cell[1] if cell else ""

    def _put(self, ref: str, inner: str, t: Optional[str], style: Optional[str] = None):
        col, row = re.match(r"([A-Z]+)(\d+)", ref).groups()
        r = int(row)
        if r not in self.rows:
            self.rows[r] = {"attrs": f' r="{r}"', "cells": {}}
        cells = self.rows[r]["cells"]
        ci = _col_index(col)
        s = style if style is not None else self.style(ref)
        attrs = f' r="{ref}"' + (f' s="{s}"' if s is not None else "") + (f' t="{t}"' if t else "")
        cells[ci] = (attrs, inner)

    def clear(self, ref: str):
        col, row = re.match(r"([A-Z]+)(\d+)", ref).groups()
        if _col_index(col) in self.rows.get(int(row), {}).get("cells", {}):
            self._put(ref, "", None)

    def set_text(self, ref: str, text: Any):
        text = _text(text)
        if not text:
            self.clear(ref)
            return
        self._put(ref, f'<is><t xml:space="preserve">{escape(text)}</t></is>', "inlineStr")

    def set_number(self, ref: str, value: Any, style: Optional[str] = None):
        if value is None or value == "":
            self.clear(ref)
            return
        self._put(ref, f"<v>{_fmt_num(value)}</v>", None, style)

    def set_formula(self, ref: str, formula: str, cached: float):
        self._put(ref, f"<f>{escape(formula.lstrip('='))}</f><v>{_fmt_num(cached)}</v>", None)

    def serialize(self) -> str:
        parts = []
        for r in sorted(self.rows):
            row = self.rows[r]
            cells = "".join(
                f"<c{attrs}/>" if not inner else f"<c{attrs}>{inner}</c>"
                for _, (attrs, inner) in sorted(row["cells"].items())
            )
            parts.append(f"<row{row['attrs']}>{cells}</row>" if cells else f"<row{row['attrs']}/>")
        return self.prefix + "".join(parts) + self.suffix


def _add_date_style(styles_xml: str, base_styles: Iterable[str]) -> Tuple[str, Dict[str, str]]:
    """複製原 DATE 格的樣式，只把數值格式換成日期；回 (styles.xml, {原 s: 新 s})。"""
    ids = [int(x) for x in re.findall(r'<numFmt\b[^>]*numFmtId="(\d+)"', styles_xml)]
    fmt_id = max([175] + ids) + 1
    numfmt = f'<numFmt numFmtId="{fmt_id}" formatCode="{DATE_FORMAT_CODE}"/>'
    m = re.search(r'<numFmts count="(\d+)">', styles_xml)
    if m:
        styles_xml = styles_xml.replace(m.group(0), f'<numFmts count="{int(m.group(1)) + 1}">', 1)
        styles_xml = styles_xml.replace("</numFmts>", numfmt + "</numFmts>", 1)
    else:
        styles_xml = re.sub(r"(<styleSheet\b[^>]*>)", r"\1" + f'<numFmts count="1">{numfmt}</numFmts>', styles_xml, count=1)

    m = re.search(r'<cellXfs count="\d+">(.*?)</cellXfs>', styles_xml, re.S)
    xfs = re.findall(r"<xf\b[^>]*?(?:/>|>.*?</xf>)", m.group(1), re.S)
    mapping: Dict[str, str] = {}
    added = []
    for s in dict.fromkeys(base_styles):
        base = xfs[int(s)] if s is not None and int(s) < len(xfs) else '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        new = re.sub(r'numFmtId="\d+"', f'numFmtId="{fmt_id}"', base, count=1)
        if "applyNumberFormat" in new:
            new = re.sub(r'applyNumberFormat="\d"', 'applyNumberFormat="1"', new)
        else:
            new = new.replace("<xf ", '<xf applyNumberFormat="1" ', 1)
        mapping[s] = str(len(xfs) + len(added))
        added.append(new)
    new_block = f'<cellXfs count="{len(xfs) + len(added)}">' + m.group(1) + "".join(added) + "</cellXfs>"
    styles_xml = styles_xml[: m.start()] + new_block + styles_xml[m.end():]
    return styles_xml, mapping


def _excel_serial(d: date) -> int:
    return (d - date(1899, 12, 30)).days


def _parse_date(value: Any) -> date:
    text = _text(value).replace("/", "-")[:10]
    if not text:
        return local_now().date()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        raise ShipnoteError("日期格式需為 YYYY-MM-DD") from None


def _clean_item(raw: Dict[str, Any], billing: bool) -> Dict[str, Any]:
    qty = _num(raw.get("qty"))
    price = _num(raw.get("price"))
    item = {
        "cust_po": _text(raw.get("cust_po")),
        "item_no": _text(raw.get("item_no")),
        "name": _text(raw.get("name")) or _text(raw.get("erp_description")),
        "harmonized_code": _text(raw.get("harmonized_code")),
        "qty": qty,
        "price": price,
        "amount": round(qty * price, 6),
    }
    if billing:
        item.update(carton_no="", box_count=0, per_box_qty=0, net_weight=0, gross_weight=0, volume=0,
                    tail_carton_no="", tail_qty=0, tail_net_weight=0, tail_gross_weight=0, tail_volume=0)
        return item
    item.update(
        carton_no=_text(raw.get("carton_no")),
        box_count=_num(raw.get("box_count")),
        per_box_qty=_num(raw.get("per_box_qty")),
        net_weight=_num(raw.get("net_weight")),
        gross_weight=_num(raw.get("gross_weight")),
        volume=_num(raw.get("volume")),
        tail_carton_no=_text(raw.get("tail_carton_no")),
        tail_qty=_num(raw.get("tail_qty")),
        tail_net_weight=_num(raw.get("tail_net_weight")),
        tail_gross_weight=_num(raw.get("tail_gross_weight")),
        tail_volume=_num(raw.get("tail_volume")),
    )
    return item


def _has_tail(item: Dict[str, Any]) -> bool:
    return bool(item["tail_qty"] or item["tail_carton_no"] or item["tail_net_weight"]
                or item["tail_gross_weight"] or item["tail_volume"])


def _sum_formula(refs: List[str]) -> str:
    chunks = [refs[i:i + 200] for i in range(0, len(refs), 200)]  # Excel 函數參數上限 255
    return "+".join(f"SUM({','.join(c)})" for c in chunks)


def _qualify(sheet_name: str, ref: str, multi: bool) -> str:
    if not multi:
        return ref
    return "'" + sheet_name.replace("'", "''") + "'!" + ref


def normalize_request(data: Dict[str, Any]) -> Dict[str, Any]:
    kind = _text(data.get("type")).lower() or "general"
    if kind not in SHIPNOTE_TYPES:
        raise ShipnoteError("出貨單類型只能是：一般、UPS、海運、請款")
    mark = _text(data.get("mark")).upper() or MARKS[0]
    if mark not in MARKS:
        raise ShipnoteError("嘜頭只能選 HILLIARD 或 WAUKESHA")
    billing = kind == "billing"
    items = [_clean_item(i, billing) for i in (data.get("items") or [])]
    items = [i for i in items if not is_customer_supplied(i["item_no"])]
    if not items:
        raise ShipnoteError("沒有可匯出的品項")
    return {
        "type": kind,
        "date": _parse_date(data.get("date")),
        "invoice_no": _text(data.get("invoice_no")),
        "maker": _text(data.get("maker")) or DEFAULT_MAKER,
        "mark": mark,
        "remark": _text(data.get("remark")),
        "customer": _text(data.get("customer")),
        "items": items,
    }


def export_filename(req: Dict[str, Any]) -> str:
    conf = SHIPNOTE_TYPES[req["type"]]
    return f"OPENTEXT {req['date'].strftime('%y%m%d')}{conf['suffix']}.xlsx"


def export_shipnote(data: Dict[str, Any]) -> Tuple[bytes, str]:
    req = normalize_request(data)
    conf = SHIPNOTE_TYPES[req["type"]]
    billing = req["type"] == "billing"
    template = TEMPLATE_DIR / conf["template"]
    if not template.exists():
        raise ShipnoteError(f"找不到出貨單模板：{conf['template']}")

    with zipfile.ZipFile(template) as zin:
        files = {name: zin.read(name) for name in zin.namelist()}
        order = zin.namelist()

    sections = conf["sections"]
    per_sheet = sections * SLOTS_PER_SECTION
    items = req["items"]
    pages = [items[i:i + per_sheet] for i in range(0, len(items), per_sheet)]
    multi = len(pages) > 1
    sheet_names = [conf["suffix"] + (str(i + 1) if multi else "") for i in range(len(pages))]

    base_xml = files["xl/worksheets/sheet1.xml"].decode("utf-8")
    probe = _Sheet(base_xml)
    date_styles = [probe.style(f"Q{s * SECTION_ROWS + 3}") for s in range(sections)]
    styles_xml, date_style_map = _add_date_style(files["xl/styles.xml"].decode("utf-8"), date_styles)
    files["xl/styles.xml"] = styles_xml.encode("utf-8")
    serial = _excel_serial(req["date"])

    # 全部頁的合計來源格（給每一聯一致的 SUM）
    box_refs: List[str] = []
    vol_refs: List[str] = []
    weight_refs: List[str] = []
    total_refs: List[str] = []
    total_boxes = total_volume = total_weight = 0.0
    grand_total = sum(i["amount"] for i in items)
    layout: List[List[Tuple[int, int, Dict[str, Any]]]] = []
    for p, page_items in enumerate(pages):
        name = sheet_names[p]
        placed = []
        for idx, item in enumerate(page_items):
            sec, slot = divmod(idx, SLOTS_PER_SECTION)
            r = sec * SECTION_ROWS + SLOT_FIRST_ROW + slot * SLOT_ROWS
            placed.append((p * per_sheet + idx + 1, r, item))
            if billing:
                continue
            if item["box_count"]:
                box_refs.append(_qualify(name, f"J{r + 1}", multi))
                total_boxes += item["box_count"]
            if item["box_count"] and item["volume"]:
                vol_refs.append(_qualify(name, f"N{r + 2}", multi))
                total_volume += item["box_count"] * item["volume"]
            if item["box_count"] and item["gross_weight"]:
                weight_refs.append(_qualify(name, f"M{r + 2}", multi))
                total_weight += item["box_count"] * item["gross_weight"]
            if _has_tail(item):
                box_refs.append(_qualify(name, f"J{r + 3}", multi))
                total_boxes += 1
                if item["tail_volume"]:
                    vol_refs.append(_qualify(name, f"N{r + 3}", multi))
                    total_volume += item["tail_volume"]
                if item["tail_gross_weight"]:
                    weight_refs.append(_qualify(name, f"M{r + 3}", multi))
                    total_weight += item["tail_gross_weight"]
        for sec in range(sections):
            total_refs.append(_qualify(name, f"Q{sec * SECTION_ROWS + 32}", multi))
        layout.append(placed)

    sheet_xmls = []
    for p, placed in enumerate(layout):
        ws = _Sheet(base_xml)
        _clear_template(ws, sections)
        for sec in range(sections):
            b = sec * SECTION_ROWS
            ws.set_text(f"Q{b + 2}", req["invoice_no"])
            ws.set_number(f"Q{b + 3}", serial, date_style_map.get(ws.style(f"Q{b + 3}")))
            if req["customer"]:
                ws.set_text(f"D{b + 4}", req["customer"])
            ws.set_text(f"B{b + 30}", req["maker"])
            ws.set_text(f"E{b + 30}", req["mark"])
            ws.set_text(f"J{b + 31}", req["remark"])
        for number, r, item in placed:
            _write_item(ws, number, r, item, conf["po_prefix"], billing)
        for sec in range(sections):
            b = sec * SECTION_ROWS
            amount_rows = [r + 2 for _, r, _i in placed if b < r < b + SECTION_ROWS]
            subtotal = sum(i["amount"] for _, r, i in placed if b < r < b + SECTION_ROWS)
            if amount_rows:
                ws.set_formula(f"Q{b + 30}", _sum_formula([f"Q{x}" for x in amount_rows]), subtotal)
            else:
                ws.set_number(f"Q{b + 30}", 0)
            ws.set_number(f"Q{b + 31}", 0)  # 銷項稅額維持 0
            ws.set_formula(f"Q{b + 32}", f"SUM(Q{b + 30},Q{b + 31})", subtotal)
            ws.set_formula(f"Q{b + 33}", _sum_formula(total_refs), grand_total)
            for ref, refs, value in (
                (f"O{b + 30}", box_refs, total_boxes),
                (f"O{b + 31}", vol_refs, total_volume),
                (f"O{b + 32}", weight_refs, total_weight),
            ):
                if refs:
                    ws.set_formula(ref, _sum_formula(refs), value)
                else:
                    ws.set_number(ref, 0)
        xml = ws.serialize()
        if p:
            xml = xml.replace(' tabSelected="1"', "")
        sheet_xmls.append(xml)

    files["xl/worksheets/sheet1.xml"] = sheet_xmls[0].encode("utf-8")
    _drop_calc_chain(files, order)
    files["xl/workbook.xml"] = _workbook_xml(files["xl/workbook.xml"].decode("utf-8"), sheet_names).encode("utf-8")
    for p in range(1, len(sheet_xmls)):
        _add_sheet_copy(files, order, p + 1, sheet_xmls[p], sheet_names[p])

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in order:
            zout.writestr(name, files[name])
    return out.getvalue(), export_filename(req)


def _clear_template(ws: _Sheet, sections: int):
    """清掉舊樣本：品項區 A~Q 全清；表頭 INVOICE NO / DATE；表尾合計、備註區。只清值，樣式保留。"""
    for sec in range(sections):
        b = sec * SECTION_ROWS
        for row in range(b + SLOT_FIRST_ROW, b + SECTION_ROWS - 3):
            for col in range(1, 18):
                ws.clear(f"{_col_letter(col)}{row}")
        for ref in (f"Q{b + 2}", f"Q{b + 3}"):
            ws.clear(ref)
        for row in range(b + 30, b + 34):
            for col in "JKLMOQ":
                ws.clear(f"{col}{row}")


def _write_item(ws: _Sheet, number: int, r: int, item: Dict[str, Any], po_prefix: str, billing: bool):
    ws.set_number(f"A{r}", number)
    ws.set_text(f"B{r}", f"HARMONIZED CODE: {item['harmonized_code']}")
    ws.set_text(f"B{r + 1}", f"{po_prefix}{item['cust_po']}")
    ws.set_text(f"B{r + 2}", item["name"])
    ws.set_number(f"O{r + 2}", item["qty"])
    ws.set_number(f"P{r + 2}", item["price"])
    ws.set_formula(f"Q{r + 2}", f"P{r + 2}*O{r + 2}", item["amount"])
    if billing:
        return
    boxes = item["box_count"]
    ws.set_text(f"I{r + 1}", item["carton_no"])  # 箱號一律文字，避免套到模板的日期格式
    ws.set_number(f"J{r + 1}", boxes or None)
    ws.set_number(f"K{r + 1}", item["per_box_qty"] or None)
    ws.set_number(f"L{r + 1}", item["net_weight"] or None)
    ws.set_number(f"M{r + 1}", item["gross_weight"] or None)
    ws.set_number(f"N{r + 1}", item["volume"] or None)
    if boxes:
        for col, key in (("K", "per_box_qty"), ("L", "net_weight"), ("M", "gross_weight"), ("N", "volume")):
            if item[key]:
                ws.set_formula(f"{col}{r + 2}", f"{col}{r + 1}*J{r + 1}", item[key] * boxes)
    if _has_tail(item):
        ws.set_text(f"I{r + 3}", item["tail_carton_no"])
        ws.set_number(f"J{r + 3}", 1)
        ws.set_number(f"K{r + 3}", item["tail_qty"] or None)
        ws.set_number(f"L{r + 3}", item["tail_net_weight"] or None)
        ws.set_number(f"M{r + 3}", item["tail_gross_weight"] or None)
        ws.set_number(f"N{r + 3}", item["tail_volume"] or None)


def _drop_calc_chain(files: Dict[str, bytes], order: List[str]):
    """公式改過，舊 calcChain 會讓 Excel 要求修復；拿掉並設定開檔全部重算。"""
    if "xl/calcChain.xml" in files:
        del files["xl/calcChain.xml"]
        order.remove("xl/calcChain.xml")
    rels = files["xl/_rels/workbook.xml.rels"].decode("utf-8")
    rels = re.sub(r'<Relationship\b[^>]*calcChain[^>]*/>', "", rels)
    files["xl/_rels/workbook.xml.rels"] = rels.encode("utf-8")
    ct = files["[Content_Types].xml"].decode("utf-8")
    ct = re.sub(r'<Override\b[^>]*calcChain[^>]*/>', "", ct)
    files["[Content_Types].xml"] = ct.encode("utf-8")
    wb = files["xl/workbook.xml"].decode("utf-8")
    if "fullCalcOnLoad" not in wb:
        wb = re.sub(r"<calcPr\b", '<calcPr fullCalcOnLoad="1"', wb, count=1)
    files["xl/workbook.xml"] = wb.encode("utf-8")


def _workbook_xml(wb: str, sheet_names: List[str]) -> str:
    return re.sub(r'(<sheet\b[^>]*\bname=")[^"]*(")', lambda m: m.group(1) + escape(sheet_names[0], {'"': "&quot;"}) + m.group(2), wb, count=1)


def _add_sheet_copy(files: Dict[str, bytes], order: List[str], n: int, xml: str, name: str):
    """品項超過一張可容納時再加一張同版面的工作表（繪圖與印表設定一起複製）。"""
    sheet_part = f"xl/worksheets/sheet{n}.xml"
    files[sheet_part] = xml.encode("utf-8")
    order.append(sheet_part)
    rels = files.get("xl/worksheets/_rels/sheet1.xml.rels", b"").decode("utf-8")
    if rels:
        if "xl/drawings/drawing1.xml" in files:
            files[f"xl/drawings/drawing{n}.xml"] = files["xl/drawings/drawing1.xml"]
            order.append(f"xl/drawings/drawing{n}.xml")
            rels = rels.replace("drawing1.xml", f"drawing{n}.xml")
        if "xl/printerSettings/printerSettings1.bin" in files:
            files[f"xl/printerSettings/printerSettings{n}.bin"] = files["xl/printerSettings/printerSettings1.bin"]
            order.append(f"xl/printerSettings/printerSettings{n}.bin")
            rels = rels.replace("printerSettings1.bin", f"printerSettings{n}.bin")
        files[f"xl/worksheets/_rels/sheet{n}.xml.rels"] = rels.encode("utf-8")
        order.append(f"xl/worksheets/_rels/sheet{n}.xml.rels")

    ct = files["[Content_Types].xml"].decode("utf-8")
    add = f'<Override PartName="/{sheet_part}" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
    if f"xl/drawings/drawing{n}.xml" in files:
        add += f'<Override PartName="/xl/drawings/drawing{n}.xml" ContentType="application/vnd.openxmlformats-officedocument.drawing+xml"/>'
    files["[Content_Types].xml"] = ct.replace("</Types>", add + "</Types>").encode("utf-8")

    wb_rels = files["xl/_rels/workbook.xml.rels"].decode("utf-8")
    rid = f"rIdShipnote{n}"
    wb_rels = wb_rels.replace(
        "</Relationships>",
        f'<Relationship Id="{rid}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{n}.xml"/></Relationships>',
    )
    files["xl/_rels/workbook.xml.rels"] = wb_rels.encode("utf-8")
    wb = files["xl/workbook.xml"].decode("utf-8")
    ids = [int(x) for x in re.findall(r'<sheet\b[^>]*\bsheetId="(\d+)"', wb)]
    wb = wb.replace(
        "</sheets>",
        f'<sheet name="{escape(name, {chr(34): "&quot;"})}" sheetId="{max(ids) + 1}" r:id="{rid}"/></sheets>',
    )
    files["xl/workbook.xml"] = wb.encode("utf-8")


def config() -> Dict[str, Any]:
    return {
        "types": [{"value": k, "label": v["label"], "has_boxes": k != "billing"} for k, v in SHIPNOTE_TYPES.items()],
        "marks": list(MARKS),
        "maker": DEFAULT_MAKER,
        "date": local_now().date().isoformat(),
        "capacity": {k: v["sections"] * SLOTS_PER_SECTION for k, v in SHIPNOTE_TYPES.items()},
    }
