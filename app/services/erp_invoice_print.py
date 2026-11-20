"""開 Invoice 列印：仿正航外銷 Invoice 報表格式「KPM5-INVOICE」（A4，以 ST-2026304 實際列印結果為準）。

全程唯讀（只下 SELECT / INFORMATION_SCHEMA 查詢），結束一律 rollback + close；不看開 Invoice 的寫入開關。
連線：目標公司 = ERP_INVOICE_TARGET；若唯讀來源（ERP_SQL_DATABASE）就是目標公司，用唯讀帳號，否則用該公司的寫入帳密
（與 list_open_lines 相同的 write_credentials，但只讀）。

欄位對應（正航實單的 TraBill Flag 501 有些值留空 / 佔位，真正的值在 comBillAccounts / comProdRec，見 erp_invoice 模組說明）：
- 表頭日期 / 客戶 / 幣別 / 金額：TraBillMain 有值就用，空（0 / ''）才用 comBillAccounts（Flag 501、FundBillNo=單號），回傳 header_sources。
- 數量 / 單價 / 金額：同列號（RowNO）的 comProdRec（Flag 501）優先，沒有才用 TraBillSub 本身，回傳 qty_source。
- 客戶 PO：S/C（TraBillMain Flag 2，單號 = OriginBillNo，空則 FromNO / POBillNO）的 Ref → CustBillNo → 本張 Invoice 的 Ref。
- 料號結尾 -TAB 的列不印、不算進列印合計，但列在 excluded_tab_rows / excluded_tab_qty / excluded_tab_amount。
"""
from __future__ import annotations

import math
import re
import threading
import time
import unicodedata
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, List, Optional, Tuple

from . import erp_purchase as ep
from . import erp_stock
from .erp_invoice import CUSTOMER_FLAG, INVOICE_FLAG, SC_FLAG, InvoiceValidationError, get_invoice_target
from .erp_stock import ErpError, ErpNotConfigured, ErpUnavailable

BILL_RE = re.compile(r"^ST-\d{7}$")
TITLE = "銷貨單"
SIGNATURES = ["簽核", "審核", "製單"]
MONTHS = ["Jan.", "Feb.", "Mar.", "Apr.", "May.", "Jun.", "Jul.", "Aug.", "Sep.", "Oct.", "Nov.", "Dec."]
LINES_PER_PAGE = 12      # 每頁明細行數（PO 標題 1 行、每個品名依折行數計，最多 3 行）
DESC_CHARS_PER_LINE = 55  # DESCRIPTION 欄（約 370px、9.5pt）每行約可放的半形字數；全形字算 2
DESC_MAX_LINES = 3
COMPANY_CACHE_SECONDS = 600

FALLBACK_COMPANY = {
    "name": "Songtech Enterprise Co., Ltd.",
    "address": "16F-7. No.738, Chung Cheng Road, Chung Ho Dist, New Taipei City, Taiwan",
    "tel": "886 2 82261276",
    "fax": "886 2 82261279",
}

# raw_header 只回這些 TraBillMain 欄位（供對照正航列印結果，其他欄位一律不回）
RAW_HEADER_FIELDS = (
    "BillNo", "Ref", "IsFormal", "Term", "TermContent", "TradeWay", "Way", "Shipment", "PerSS", "LCNo", "ValidDate",
    "Shipper", "LoadPort", "DestPort", "Payment", "Memo", "EndTale", "TopTale", "CustID", "CustName", "CustBillNo",
    "BillDate", "CurrID", "ExchRate", "Total", "SumQty", "Maker", "TEL", "FAX", "Addr", "Address",
)
SUB_FIELDS = ("RowNo", "SerNo", "ItemNo", "Description", "Quantity", "Unit", "Price", "Amount", "SubDesc",
              "ItemRemark", "OriginBillNo", "FromNO", "POBillNO")


class InvoiceNotFound(ErpError):
    """正航找不到這張 Invoice（路由回 404）。"""


# ── 純函式：格式 ──────────────────────────────────────────────────────────────

def _d(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal(0)
    return value if isinstance(value, Decimal) else Decimal(str(value))


def fmt_num(value: Any, places: int) -> str:
    """千分位 + 固定小數位（四捨五入）：fmt_num(5000, 4) = '5,000.0000'。"""
    q = _d(value).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)
    return f"{q:,.{places}f}"


def format_date(value: Any) -> str:
    """yyyymmdd → 'Nov.16,2026'（英文月份縮寫 + 句點、日不補零）；不合法回 ''。"""
    try:
        n = int(value or 0)
    except (TypeError, ValueError):
        return ""
    y, m, d = n // 10000, n // 100 % 100, n % 100
    if y < 1900 or not 1 <= m <= 12 or not 1 <= d <= 31:
        return ""
    return f"{MONTHS[m - 1]}{d},{y}"


def normalize_bill_no(value: Any) -> str:
    bill_no = ep._text(value).upper()
    if not BILL_RE.match(bill_no):
        raise InvoiceValidationError("Invoice 單號格式不正確，請輸入完整單號，例如 ST-2026304")
    return bill_no


def is_tab_item(item_no: Any) -> bool:
    return ep._text(item_no).upper().endswith("-TAB")


def _s(value: Any) -> str:
    """去頭尾空白（char 欄位會補空白），內文不動。"""
    return "" if value is None else str(value).strip()


def first_text(pairs: List[Tuple[str, Any]]) -> Tuple[str, str]:
    """[(來源, 值), ...] 第一個非空白 → (值, 來源)；都空回 ('', '')。"""
    for source, value in pairs:
        text = _s(value)
        if text:
            return text, source
    return "", ""


def print_description(item_no: Any, description: Any, sub_desc: Any) -> str:
    """料號 + 空白 + 品名；SubDesc 非空白且品名沒包含它才接在後面（不印 ItemRemark）。"""
    item, desc, sub = _s(item_no), _s(description), _s(sub_desc)
    text = " ".join(p for p in (item, desc) if p)
    if sub and sub not in text:
        text = f"{text} {sub}" if text else sub
    return text


def desc_line_count(text: str) -> int:
    """估計品名在 DESCRIPTION 欄折成幾行（全形字算 2 個半形寬），最多 3 行（列印時 line-clamp 3）。"""
    width = sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in text or "")
    return max(1, min(DESC_MAX_LINES, math.ceil(width / DESC_CHARS_PER_LINE)))


def paginate(lines: List[Dict[str, Any]], per_page: int = LINES_PER_PAGE) -> List[Dict[str, Any]]:
    """依客戶 PO 分組排版：PO 換了才印一行 PO#；PO 標題不單獨留在頁尾。合計 / 簽核只在最後一頁（last=True）。"""
    blocks: List[List[Dict[str, Any]]] = []
    prev_po: Optional[str] = None
    for i, line in enumerate(lines):
        rows: List[Dict[str, Any]] = []
        if i == 0 or line["po"] != prev_po:
            if line["po"]:
                rows.append({"type": "po", "po": line["po"], "height": 1})
        prev_po = line["po"]
        rows.append({"type": "line", "line": i, "row_no": line["row_no"], "height": desc_line_count(line["print_description"])})
        blocks.append(rows)
    pages: List[List[Dict[str, Any]]] = [[]]
    used = 0
    for rows in blocks:
        need = sum(r["height"] for r in rows)
        if used and used + need > per_page:
            pages.append([])
            used = 0
        pages[-1].extend(rows)
        used += need
    total = len(pages)
    return [{"no": n, "of": total, "rows": rows, "last": n == total} for n, rows in enumerate(pages, 1)]


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return None
    if isinstance(value, str):
        return value.strip()
    return value


# ── 讀取 ──────────────────────────────────────────────────────────────────────

def _query(cur, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    cur.execute(sql, params)
    return ep._rows(cur)


def _connect_read(target: str):
    """唯讀來源就是目標公司 → 唯讀帳號；否則用目標公司的寫入帳密（只讀）。回傳 (conn, creds 或 None)。"""
    settings = erp_stock.get_settings()
    if settings["database"] == target and erp_stock.is_configured(settings):
        return erp_stock._connect(settings), None
    creds = ep.write_credentials(target)
    if creds is None:
        raise ErpNotConfigured(ep.UNAVAILABLE_REASON)
    return ep._connect_write(creds), creds


_COMPANY_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_COMPANY_LOCK = threading.Lock()

_NAME_COLS = ("corpname", "companyname", "compname", "name")
_ADDR_COLS = ("companyaddress", "compaddress", "corpaddress", "address", "addr")
_TEL_COLS = ("telphone", "telephone", "tel", "phone")
_FAX_COLS = ("fax", "faxno")


def _pick(cols: List[str], names: Tuple[str, ...], prefer_eng: bool) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    if prefer_eng:
        for c in cols:
            cl = c.lower()
            if "eng" in cl and any(n in cl for n in names):
                return c
    for n in names:
        if n in lower:
            return lower[n]
    return None


def _fallback_company(reason: str = "") -> Dict[str, Any]:
    out = dict(FALLBACK_COMPANY, company_source="fallback",
               pieces={k: "fallback" for k in FALLBACK_COMPANY})
    if reason:
        out["reason"] = reason
    return out


def lookup_company(cur, target: str) -> Dict[str, Any]:
    """公司抬頭（英文名 / 地址 / 電話 / 傳真）：查 INFORMATION_SCHEMA 找名稱含 Company / Corp 的表，
    剛好一張表有英文名稱欄才讀它的第一列；任何失敗或缺漏的部分用常數。結果每個 process 快取 10 分鐘。絕不丟例外。"""
    now = time.monotonic()
    with _COMPANY_LOCK:
        hit = _COMPANY_CACHE.get(target)
        if hit and now - hit[0] < COMPANY_CACHE_SECONDS:
            return dict(hit[1])
    try:
        result = _lookup_company(cur)
    except Exception as exc:  # 抬頭查不到不影響列印
        result = _fallback_company(type(exc).__name__)
    with _COMPANY_LOCK:
        _COMPANY_CACHE[target] = (now, result)
    return dict(result)


def _lookup_company(cur) -> Dict[str, Any]:
    rows = _query(cur, "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                       "WHERE TABLE_NAME LIKE '%Company%' OR TABLE_NAME LIKE '%Corp%' "
                       "ORDER BY TABLE_NAME, ORDINAL_POSITION")
    tables: Dict[str, List[str]] = {}
    for r in rows:
        table, col = _s(ep._get(r, "TABLE_NAME")), _s(ep._get(r, "COLUMN_NAME"))
        if table and col:
            tables.setdefault(table, []).append(col)
    candidates = []
    for table, cols in tables.items():
        name_col = next((c for c in cols if "eng" in c.lower() and "name" in c.lower()), None)
        if name_col and re.match(r"^\w+$", table):
            candidates.append((table, cols, name_col))
    if len(candidates) != 1:
        return _fallback_company("no_table" if not candidates else "ambiguous")
    table, cols, name_col = candidates[0]
    picked = {"name": name_col, "address": _pick(cols, _ADDR_COLS, True),
              "tel": _pick(cols, _TEL_COLS, False), "fax": _pick(cols, _FAX_COLS, False)}
    select = [c for c in dict.fromkeys(v for v in picked.values() if v) if re.match(r"^\w+$", c)]
    found = _query(cur, f"SELECT TOP 1 {', '.join(f'[{c}]' for c in select)} FROM [{table}]")
    row = found[0] if found else {}
    out: Dict[str, Any] = {"pieces": {}}
    for key, col in picked.items():
        value = _s(row.get(col)) if col else ""
        out[key] = value or FALLBACK_COMPANY[key]
        out["pieces"][key] = f"db:{table}.{col}" if value else "fallback"
    used_db = any(v != "fallback" for v in out["pieces"].values())
    out["company_source"] = f"db:{table}" if used_db else "fallback"
    return out


def _load(cur, bill_no: str) -> Dict[str, Any]:
    main_rows = _query(cur, f"SELECT TOP 1 {', '.join(RAW_HEADER_FIELDS)} FROM TraBillMain "
                            f"WHERE Flag = {INVOICE_FLAG} AND BillNo = %s", (bill_no,))
    if not main_rows:
        raise InvoiceNotFound(f"正航找不到 Invoice {bill_no}（TraBillMain Flag {INVOICE_FLAG}），請確認單號")
    subs = _query(cur, f"SELECT {', '.join(SUB_FIELDS)} FROM TraBillSub WHERE Flag = {INVOICE_FLAG} AND BillNo = %s "
                       "ORDER BY RowNo, SerNo", (bill_no,))
    recs = _query(cur, f"SELECT RowNO, Quantity, Price, Amount FROM comProdRec WHERE Flag = {INVOICE_FLAG} "
                       "AND BillNO = %s ORDER BY RowNO", (bill_no,))
    accs = _query(cur, f"SELECT TOP 1 BillDate, CustID, CurrID, ExchRate, Total FROM comBillAccounts "
                       f"WHERE Flag = {INVOICE_FLAG} AND FundBillNo = %s", (bill_no,))
    main = main_rows[0]
    acc = accs[0] if accs else {}
    cust_id, _ = first_text([("TraBillMain", ep._get(main, "CustID")), ("comBillAccounts", ep._get(acc, "CustID"))])
    customers = _query(cur, f"SELECT TOP 1 ID, ShortName, FullName FROM comCustomer WHERE Flag = {CUSTOMER_FLAG} "
                            "AND ID = %s", (cust_id,)) if cust_id else []
    sc_nos = sorted({_sc_no(s) for s in subs} - {""})
    scs: Dict[str, Dict[str, Any]] = {}
    if sc_nos:
        marks = ", ".join(["%s"] * len(sc_nos))
        for r in _query(cur, f"SELECT BillNo, Ref, CustBillNo FROM TraBillMain WHERE Flag = {SC_FLAG} "
                             f"AND BillNo IN ({marks})", tuple(sc_nos)):
            scs[_s(ep._get(r, "BillNo"))] = r
    return {"main": main, "subs": subs, "recs": recs, "acc": acc, "cust_id": cust_id,
            "customer": customers[0] if customers else {}, "scs": scs}


def _sc_no(sub: Dict[str, Any]) -> str:
    text, _ = first_text([("OriginBillNo", ep._get(sub, "OriginBillNo")), ("FromNO", ep._get(sub, "FromNO")),
                          ("POBillNO", ep._get(sub, "POBillNO"))])
    return text


# ── 組裝（純函式，測試直接餵假資料）───────────────────────────────────────────

def build_print_data(raw: Dict[str, Any], *, bill_no: str, target: str, company: Dict[str, Any]) -> Dict[str, Any]:
    main, acc, cust = raw["main"], raw["acc"], raw["customer"]
    g = ep._get
    sources: Dict[str, str] = {}

    bill_date = int(g(main, "BillDate") or 0)
    sources["date"] = "TraBillMain"
    if not bill_date and int(g(acc, "BillDate") or 0):
        bill_date, sources["date"] = int(g(acc, "BillDate")), "comBillAccounts"
    currency, sources["currency"] = first_text([("TraBillMain", g(main, "CurrID")), ("comBillAccounts", g(acc, "CurrID"))])
    main_total = _d(g(main, "Total"))
    sources["total"] = "TraBillMain"
    if not main_total and acc:
        main_total, sources["total"] = _d(g(acc, "Total")), "comBillAccounts"
    sources["customer_id"] = "TraBillMain" if _s(g(main, "CustID")) else ("comBillAccounts" if raw["cust_id"] else "")

    cust_name, name_source = first_text([("TraBillMain.CustName", g(main, "CustName")),
                                         ("comCustomer.FullName", g(cust, "FullName")),
                                         ("comCustomer.ShortName", g(cust, "ShortName"))])
    trade_term, term_source = first_text([("TraBillMain.TradeWay", g(main, "TradeWay")),
                                          ("TraBillMain.Way", g(main, "Way")),
                                          ("TraBillMain.Shipment", g(main, "Shipment"))])
    inv_ref = _s(g(main, "Ref"))
    recs = {int(g(r, "RowNO") or 0): r for r in raw["recs"]}

    lines: List[Dict[str, Any]] = []
    excluded: List[Dict[str, Any]] = []
    full_qty = full_amount = qty_total = amount_total = Decimal(0)
    for sub in raw["subs"]:
        row_no = int(g(sub, "RowNo") or 0)
        rec = recs.get(row_no)
        src = rec if rec is not None else sub
        qty, price, amount = _d(g(src, "Quantity")), _d(g(src, "Price")), _d(g(src, "Amount"))
        computed = False
        if amount == 0 and price * qty > 0:
            amount = (price * qty).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
            computed = True
        full_qty += qty
        full_amount += amount
        item_no = _s(g(sub, "ItemNo"))
        if is_tab_item(item_no):
            excluded.append({"row_no": row_no, "item_no": item_no, "qty": float(qty), "amount": float(amount)})
            continue
        qty_total += qty
        amount_total += amount
        sc_no = _sc_no(sub)
        sc = raw["scs"].get(sc_no, {})
        po, po_source = first_text([("sc.Ref", g(sc, "Ref")), ("sc.CustBillNo", g(sc, "CustBillNo")),
                                    ("invoice.Ref", inv_ref)])
        lines.append({
            "row_no": row_no, "item_no": item_no, "description": _s(g(sub, "Description")),
            "print_description": print_description(item_no, g(sub, "Description"), g(sub, "SubDesc")),
            "qty": float(qty), "unit": _s(g(sub, "Unit")), "price": float(price), "amount": float(amount),
            "qty_display": fmt_num(qty, 4), "price_display": fmt_num(price, 3), "amount_display": fmt_num(amount, 3),
            "amount_computed": computed, "qty_source": "comProdRec" if rec is not None else "TraBillSub",
            "sc_no": sc_no, "po": po, "po_source": po_source or "none",
            "sub_desc": _s(g(sub, "SubDesc")), "item_remark": _s(g(sub, "ItemRemark")), "excluded_tab": False,
        })
    tab_qty = sum((_d(e["qty"]) for e in excluded), Decimal(0))
    tab_amount = sum((_d(e["amount"]) for e in excluded), Decimal(0))
    return {
        "ok": True, "bill_no": bill_no, "target": ep.target_label(target), "target_db": target,
        "company": company, "title": TITLE,
        "date_display": format_date(bill_date), "date_raw": bill_date,
        "customer": {"id": raw["cust_id"], "name": cust_name, "name_source": name_source or "none"},
        "customer_name_source": name_source or "none",
        "trade_term": trade_term, "trade_term_source": term_source or "none",
        "currency": currency, "header_sources": sources,
        "lines": lines,
        "excluded_tab_rows": excluded, "excluded_tab_qty": float(tab_qty), "excluded_tab_amount": float(tab_amount),
        "totals": {
            "qty": float(qty_total), "amount": float(amount_total),
            "qty_display": fmt_num(qty_total, 4), "amount_display": fmt_num(amount_total, 3),
            "full_qty": float(full_qty), "full_amount": float(full_amount),
            "main_total": float(main_total), "matches_main_total": abs(full_amount - main_total) <= Decimal("0.0005"),
        },
        "full_total_qty": float(full_qty), "full_total_amount": float(full_amount),
        "pages": paginate(lines),
        "signatures": list(SIGNATURES),
        "raw_header": {k: _json_value(g(main, k)) for k in RAW_HEADER_FIELDS},
    }


def get_print_data(bill_no: Any) -> Dict[str, Any]:
    """讀一張 Invoice 的列印資料（唯讀，結束一律 rollback + close）。"""
    bill_no = normalize_bill_no(bill_no)
    target = get_invoice_target()
    conn, creds = _connect_read(target)
    try:
        cur = conn.cursor()
        ep._check_db_name(cur, target)
        raw = _load(cur, bill_no)
        company = lookup_company(cur, target)
    except ErpError:
        raise
    except Exception as exc:
        raise ErpUnavailable(f"ERP 查詢失敗：{ep._safe_message(exc, creds)}") from None
    finally:
        ep._close_quietly(conn, rollback=True)
    return build_print_data(raw, bill_no=bill_no, target=target, company=company)
