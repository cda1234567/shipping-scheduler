"""修改正航舊採購單（ordBillMain / ordBillSub，Flag=4）。v2026.11.13.2 起。

開新單（erp_purchase.py）只 INSERT；這裡是另一條寫入路徑，規則更保守：
- 讀取 / 搜尋：預設從來源公司（ERP_SQL_DATABASE，現況 CHIComp03）唯讀；也可讀改單寫入目標公司（用寫入帳號，最後 rollback）。
- 寫入目標另開開關 ERP_PO_EDIT_TARGET（預設 CHIComp13 測試）。正式公司 CHIComp03 需同時
  ERP_PO_EDIT_TARGET=CHIComp03、ERP_ALLOW_PROD_PO_EDIT_CHICOMP03=1、ERP_ALLOW_PROD_WRITE_CHICOMP03=1、ERP_ALLOW_PROD_PO_CHICOMP03=1。
- 只能改「載入的那家公司」的單：載入公司3、寫入目標公司13 → 拒絕（不可拿公司3的單去改公司13）。
- 已結案（BillStatus≠0 或 EndacaseDate≠0）、任一明細已進貨（QtyRemain≠Quantity）→ 拒絕。
- 表頭只改：供應商、備註、採購員、課稅別、幣別 / 匯率、合計；付款條件 / 地址 / 開單日期 / Permitter / IsTrans 等保持原值。
- 明細以 RowNO 對應 UPDATE、新列 INSERT（範本同開新單）、移除的列 DELETE；未進貨列 QtyRemain 跟著新數量。
- 原有 -TAB 客供料列照實保留，不可從本站移除；新增列不可是 -TAB。
- UPDATE / INSERT 一律明列目標庫 sys.columns 的欄位名；同一 transaction，寫完讀回比對，不符即 rollback。
- 絕不整張刪除採購單。金額 / 稅額沿用 erp_purchase.compute_money。
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple

from . import erp_purchase as ep
from . import erp_stock
from .erp_stock import ErpError, ErpNotConfigured, ErpUnavailable

EDIT_TARGET_ENV = "ERP_PO_EDIT_TARGET"
DEFAULT_EDIT_TARGET = "CHIComp13"
PROD_PO_EDIT_FLAG_ENV = "ERP_ALLOW_PROD_PO_EDIT_CHICOMP03"
MAX_SEARCH = 30

PoValidationError = ep.PoValidationError
PoSchemaError = ep.PoSchemaError

_text = ep._text
_get = ep._get
_one = ep._one
_rows = ep._rows


# ── 開關 / 寫入目標 ──────────────────────────────────────────────────────────

def prod_po_edit_allowed() -> bool:
    """正式公司改單：開新單的兩把鑰匙（WRITE + PO）之外，還要 ERP_ALLOW_PROD_PO_EDIT_CHICOMP03=1。"""
    return ep.prod_po_allowed() and ep._flag_on(PROD_PO_EDIT_FLAG_ENV)


def _prod_edit_requirement() -> str:
    return (f"需同時設定 {EDIT_TARGET_ENV}={ep.PRODUCTION_TARGET}、{PROD_PO_EDIT_FLAG_ENV}=1、"
            f"{ep.PROD_WRITE_FLAG_ENV}=1、{ep.PROD_PO_FLAG_ENV}=1")


def get_edit_target() -> str:
    """改舊單寫入目標公司（與開新單的 ERP_WRITE_TARGET 分開）。正式公司未開齊開關一律拒絕。"""
    target = ep._env(EDIT_TARGET_ENV, DEFAULT_EDIT_TARGET)
    if not ep.TARGET_RE.match(target):
        raise ErpNotConfigured(f"{EDIT_TARGET_ENV} 格式不正確（需為 CHIComp 加兩位數字，例如 CHIComp13）")
    if target == ep.PRODUCTION_TARGET and not prod_po_edit_allowed():
        raise ErpNotConfigured(f"{ep.target_label(target)} 尚未開放修改採購單（{_prod_edit_requirement()}）")
    return target


def _source_db() -> str:
    return erp_stock.get_settings()["database"]


def edit_config() -> Dict[str, Any]:
    """/api/erp/po/config 的改單欄位（與開新單的 target / available 分開）；不含帳密。"""
    source = _source_db()
    out: Dict[str, Any] = {
        "edit_target": "", "edit_label": "", "edit_is_test": False, "edit_available": False, "edit_reason": "",
        "edit_source": source, "edit_source_label": ep.target_label(source),
        "edit_prod_allowed": prod_po_edit_allowed(),
        "edit_companies": [{"db": source, "label": ep.target_label(source), "is_test": ep.is_test(source)}],
    }
    try:
        target = get_edit_target()
    except ErpError as exc:
        out["edit_reason"] = str(exc)
        return out
    out.update(edit_target=target, edit_label=ep.target_label(target), edit_is_test=ep.is_test(target))
    if ep.write_credentials(target) is None:
        out["edit_reason"] = f"{ep.target_label(target)} {ep.UNAVAILABLE_REASON}"
        return out
    out["edit_available"] = True
    if target != source:
        out["edit_companies"].append({"db": target, "label": ep.target_label(target), "is_test": ep.is_test(target)})
        if source == ep.PRODUCTION_TARGET:
            out["edit_reason"] = (f"目前改單只能寫入 {ep.target_label(target)}；{ep.target_label(source)} 的舊單可以載入、預覽，"
                                  f"但正式公司改單尚未開放")
    return out


def write_block_reason(company: str) -> str:
    """這家公司的單現在能不能寫入；空字串 = 可以。"""
    try:
        target = get_edit_target()
    except ErpError as exc:
        return str(exc)
    if ep.write_credentials(target) is None:
        return f"改單寫入目標 {ep.target_label(target)} {ep.UNAVAILABLE_REASON}"
    if company != target:
        extra = f"；正式公司改單尚未開放（{_prod_edit_requirement()}）" if company == ep.PRODUCTION_TARGET else ""
        return (f"這張單是 {ep.target_label(company)} 的單，目前改單只能寫入 {ep.target_label(target)}，"
                f"不可拿 {ep.target_label(company)} 的單去改 {ep.target_label(target)}{extra}")
    return ""


# ── 連線（讀：來源公司或改單目標；寫：改單目標）───────────────────────────────

def _resolve_company(company: str) -> str:
    company = _text(company) or _source_db()
    if company == _source_db():
        return company
    try:
        target = get_edit_target()
    except ErpError:
        target = ""
    if company != target:
        raise PoValidationError(f"只能讀取來源公司（{ep.target_label(_source_db())}）或改單寫入目標公司的採購單，收到：{company}")
    return company


@contextmanager
def _read_session(company: str) -> Iterator[Any]:
    """唯讀查詢用連線：來源公司走 _source_session；改單目標公司用寫入帳號，結束一律 rollback。"""
    company = _resolve_company(company)
    if company == _source_db():
        with ep._source_session() as conn:
            yield conn
        return
    creds = ep.write_credentials(company)
    if creds is None:
        raise ErpNotConfigured(f"{ep.target_label(company)} {ep.UNAVAILABLE_REASON}")
    conn = ep._connect_write(creds)
    try:
        ep._check_db_name(conn.cursor(), company)
        yield conn
    except ErpError:
        raise
    except Exception as exc:
        raise ErpUnavailable(f"ERP 查詢失敗：{ep._safe_message(exc, creds)}") from None
    finally:
        ep._close_quietly(conn, rollback=True)


# ── 狀態判斷 ─────────────────────────────────────────────────────────────────

def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value if value is not None else 0))
    except Exception:
        return Decimal(0)


def is_closed(main: Dict[str, Any]) -> bool:
    return _int(_get(main, "BillStatus")) != 0 or _int(_get(main, "EndacaseDate")) != 0


def received_rows(subs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [s for s in subs if _dec(_get(s, "QtyRemain")) != _dec(_get(s, "Quantity"))]


def lock_reason(main: Dict[str, Any], subs: List[Dict[str, Any]]) -> str:
    """不可修改的原因；空字串 = 可以改（未結案、全部未進貨）。"""
    bill_no = _text(_get(main, "BillNO"))
    if is_closed(main):
        return (f"採購單 {bill_no} 已結案（BillStatus={_int(_get(main, 'BillStatus'))}，"
                f"EndacaseDate={_int(_get(main, 'EndacaseDate'))}），不可從本站修改")
    got = received_rows(subs)
    if got:
        rows = "、".join(f"第 {_int(_get(s, 'RowNO'))} 列 {_text(_get(s, 'ProdID'))}"
                        f"（數量 {float(_dec(_get(s, 'Quantity'))):g}，未進 {float(_dec(_get(s, 'QtyRemain'))):g}）" for s in got[:5])
        return f"採購單 {bill_no} 已有明細進過貨（{rows}），改了會跟進貨對不上，不可從本站修改"
    return ""


def _currency_of(main: Dict[str, Any]) -> Tuple[str, str]:
    """(正規化幣別, 原始 CurrID)；CurrID 空白視為台幣。不支援的幣別回 ('', 原值)。"""
    raw = _text(_get(main, "CurrID"))
    if not raw:
        return ep.LOCAL_CURRENCY, raw
    code = ep._CURRENCY_ALIASES.get(raw) or ep._CURRENCY_ALIASES.get(raw.upper())
    return (code or ""), raw


def _supplier_name(row: Optional[Dict[str, Any]]) -> str:
    return _text(_get(row or {}, "ShortName")) or _text(_get(row or {}, "FullName"))


# ── 搜尋 / 載入（唯讀）────────────────────────────────────────────────────────

def search_bills(q: str, company: str = "") -> Dict[str, Any]:
    """舊採購單搜尋：單號 / 供應商代號 / 供應商簡稱或全名部分符合（LIKE 跳脫同供應商搜尋），TOP 30，新的在前。"""
    text = (q or "").strip()
    company = _resolve_company(company)
    result: Dict[str, Any] = {"company": company, "company_label": ep.target_label(company), "bills": []}
    if not text:
        return result
    pattern = f"%{ep._like_escape(text)}%"
    with _read_session(company) as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT TOP {MAX_SEARCH} m.BillNO, m.BillDate, m.CustomerID, c.ShortName, c.FullName, "
            "m.SumBTaxAmt, m.SumAmtATax, m.CurrID, m.BillStatus, m.EndacaseDate "
            "FROM ordBillMain m LEFT JOIN comCustomer c ON c.Flag = 2 AND c.ID = m.CustomerID "
            "WHERE m.Flag = 4 AND (m.BillNO LIKE %s OR m.CustomerID LIKE %s OR c.ShortName LIKE %s OR c.FullName LIKE %s) "
            "ORDER BY m.BillDate DESC, m.BillNO DESC",
            (pattern, pattern, pattern, pattern),
        )
        for r in cur.fetchall():
            closed = _int(r[8]) != 0 or _int(r[9]) != 0
            result["bills"].append({
                "bill_no": _text(r[0]), "bill_date": ep._fmt_bill_date(r[1]),
                "supplier_id": _text(r[2]), "supplier_name": _text(r[3]) or _text(r[4]),
                "sum_amount": float(r[5] or 0), "total": float(r[6] or 0), "currency": _text(r[7]) or ep.LOCAL_CURRENCY,
                "closed": closed, "status": "已結" if closed else "未結",
            })
    return result


def _read_bill(cur, bill_no: str, lock: bool = False) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    hint = " WITH (UPDLOCK, HOLDLOCK)" if lock else ""
    cur.execute(f"SELECT * FROM ordBillMain{hint} WHERE Flag = 4 AND BillNO = %s", (bill_no,))
    main = _one(cur)
    if main is None:
        raise PoValidationError(f"找不到採購單 {bill_no}")
    cur.execute(f"SELECT * FROM ordBillSub{hint} WHERE Flag = 4 AND BillNO = %s ORDER BY RowNO", (bill_no,))
    return main, _rows(cur)


def _line_json(s: Dict[str, Any]) -> Dict[str, Any]:
    part = _text(_get(s, "ProdID"))
    return {
        "row_no": _int(_get(s, "RowNO")),
        "part_number": part,
        "product_name": _text(_get(s, "ProdName")),
        "qty": float(_dec(_get(s, "Quantity"))),
        "price": float(_dec(_get(s, "Price"))),
        "amount": float(_dec(_get(s, "Amount"))),
        "tax_amt": float(_dec(_get(s, "TaxAmt"))),
        "pre_in_date": ep._ymd_text(_get(s, "PreInDate")),
        "quantity": float(_dec(_get(s, "Quantity"))),
        "qty_remain": float(_dec(_get(s, "QtyRemain"))),
        "customer_supplied": bool(ep.customer_supplied_reason(part)),
    }


def load_bill(bill_no: str, company: str = "") -> Dict[str, Any]:
    """載入舊採購單表頭 + 全部明細（含原有 -TAB 列），以及能不能改、能不能寫入的原因。唯讀。"""
    no = _text(bill_no)
    if not no:
        raise PoValidationError("請輸入採購單號")
    company = _resolve_company(company)
    with _read_session(company) as conn:
        cur = conn.cursor()
        main, subs = _read_bill(cur, no)
        supplier_id = _text(_get(main, "CustomerID"))
        cur.execute("SELECT ShortName, FullName FROM comCustomer WHERE Flag = 2 AND ID = %s", (supplier_id,))
        found = cur.fetchone()
    currency, raw_curr = _currency_of(main)
    tax_type = _int(_get(main, "TaxType"))
    tax_mode = ep.TAX_TAXABLE if tax_type == 0 else ep.TAX_EXEMPT
    reason = lock_reason(main, subs)
    if not reason and not currency:
        reason = f"採購單幣別 {raw_curr} 本站不支援（只支援台幣 NTD / 美金 USD），請到正航修改"
    return {
        "company": company,
        "company_label": ep.target_label(company),
        "company_is_test": ep.is_test(company),
        "bill_no": _text(_get(main, "BillNO")),
        "bill_date": ep._ymd_text(_get(main, "BillDate")),
        "supplier": {"id": supplier_id, "name": (_text(found[0]) or _text(found[1])) if found else supplier_id},
        "salesman": _text(_get(main, "SalesMan")),
        "remark": _text(_get(main, "Remark")),
        "currency": currency or raw_curr,
        "exch_rate": float(_dec(_get(main, "ExchRate", 1)) or 1),
        "tax_type": tax_type,
        "tax_mode": tax_mode,
        "tax_label": ep.tax_label(tax_mode, tax_type),
        "bill_status": _int(_get(main, "BillStatus")),
        "endacase_date": _int(_get(main, "EndacaseDate")),
        "totals": {
            "sum_qty": float(_dec(_get(main, "SumQty"))),
            "sum_amount": float(_dec(_get(main, "SumBTaxAmt"))),
            "sum_tax": float(_dec(_get(main, "SumTax"))),
            "total": float(_dec(_get(main, "SumAmtATax"))),
            "local_total": float(_dec(_get(main, "LocalTotal"))),
            "local_tax": float(_dec(_get(main, "LocalTax"))),
        },
        "lines": [_line_json(s) for s in subs],
        "editable": not reason,
        "lock_reason": reason,
        "write_block_reason": write_block_reason(company),
    }


# ── 輸入驗證 ─────────────────────────────────────────────────────────────────

def normalize_edit_request(req: Dict[str, Any]) -> Dict[str, Any]:
    bill_no = _text(req.get("bill_no"))
    if not bill_no:
        raise PoValidationError("請先載入要修改的採購單")
    supplier_id = _text(req.get("supplier_id"))
    if not supplier_id:
        raise PoValidationError("請選擇供應商")
    raw_lines = req.get("lines") or []
    if not raw_lines:
        raise PoValidationError("明細至少要 1 行（本站不會整張刪除採購單）")
    lines = []
    seen_rows = set()
    for i, raw in enumerate(raw_lines, 1):
        try:
            row_no = int(raw.get("row_no") or 0)
        except (TypeError, ValueError):
            raise PoValidationError(f"第 {i} 行：列號不正確") from None
        if row_no < 0:
            raise PoValidationError(f"第 {i} 行：列號不正確")
        if row_no and row_no in seen_rows:
            raise PoValidationError(f"第 {i} 行：原有明細第 {row_no} 列重複")
        seen_rows.add(row_no)
        part = _text(raw.get("part_number")).upper()
        if not part:
            raise PoValidationError(f"第 {i} 行：請輸入料號")
        if not row_no and ep.customer_supplied_reason(part):
            raise ep._customer_supplied_error(part)
        qty = ep._to_decimal(raw.get("qty"), f"第 {i} 行數量")
        if qty <= 0:
            raise PoValidationError(f"第 {i} 行：數量必須大於 0")
        price = ep._to_decimal(raw.get("price"), f"第 {i} 行單價")
        if price < 0:
            raise PoValidationError(f"第 {i} 行：單價不可為負")
        try:
            pre_in = datetime.strptime(_text(raw.get("pre_in_date")), "%Y-%m-%d").date()
        except ValueError:
            raise PoValidationError(f"第 {i} 行：預計進貨日格式需為 YYYY-MM-DD") from None
        lines.append({"row_no": row_no, "part_number": part, "qty": qty, "price": price,
                      "pre_in_date": ep.date_to_int(pre_in)})
    loaded = req.get("loaded")
    snap = None
    if isinstance(loaded, dict):
        snap = {
            "bill_status": _int(loaded.get("bill_status")),
            "endacase_date": _int(loaded.get("endacase_date")),
            "rows": {_int(r.get("row_no")): (_dec(r.get("quantity")), _dec(r.get("qty_remain")))
                     for r in loaded.get("rows") or [] if _int(r.get("row_no"))},
        }
    return {
        "bill_no": bill_no,
        "company": _text(req.get("company")),
        "supplier_id": supplier_id,
        "remark": _text(req.get("remark")),
        "salesman": _text(req.get("salesman")) or ep._salesman_default(),
        "currency": ep.normalize_currency(req.get("currency"), "幣別"),
        "tax_mode": ep.normalize_tax_mode(req.get("tax_mode")),
        "lines": lines,
        "loaded": snap,
    }


def _check_snapshot(main: Dict[str, Any], subs: List[Dict[str, Any]], snap: Optional[Dict[str, Any]]) -> None:
    """載入當下看到的 BillStatus / EndacaseDate / 每列 RowNO、Quantity、QtyRemain 要跟現在一樣，否則請他重新載入。"""
    if snap is None:
        return
    bill_no = _text(_get(main, "BillNO"))
    stale = PoValidationError(f"採購單 {bill_no} 在載入後已被改過（例如進貨或其他人修改），請重新載入後再改")
    if (_int(_get(main, "BillStatus")), _int(_get(main, "EndacaseDate"))) != (snap["bill_status"], snap["endacase_date"]):
        raise stale
    now = {_int(_get(s, "RowNO")): (_dec(_get(s, "Quantity")), _dec(_get(s, "QtyRemain"))) for s in subs}
    if now != snap["rows"]:
        raise stale


# ── 規劃（讀 + 組 SQL，不寫入）─────────────────────────────────────────────────

def _col_map(columns: List[Dict[str, Any]]) -> Dict[str, str]:
    return {c["name"].lower(): c["name"] for c in columns if not c["identity"] and not c["computed"]}


def build_update(
    table: str, columns: List[Dict[str, Any]], values: Dict[str, Any], where: List[Tuple[str, Any]],
) -> Tuple[str, Tuple[Any, ...]]:
    """UPDATE 明列目標庫欄位名（大小寫依 sys.columns）；任何欄位目標沒有就中止。"""
    cols = _col_map(columns)
    names = list(values) + [w[0] for w in where]
    missing = [n for n in names if n.lower() not in cols]
    if missing:
        raise PoSchemaError(f"{table} 目標沒有的欄位：{', '.join(missing[:10])}，已中止")
    sets = ", ".join(f"[{cols[k.lower()]}] = %s" for k in values)
    cond = " AND ".join(f"[{cols[w[0].lower()]}] = %s" for w in where)
    return f"UPDATE dbo.{table} SET {sets} WHERE {cond}", tuple(values.values()) + tuple(w[1] for w in where)


def build_delete(table: str, columns: List[Dict[str, Any]], where: List[Tuple[str, Any]]) -> Tuple[str, Tuple[Any, ...]]:
    cols = _col_map(columns)
    missing = [w[0] for w in where if w[0].lower() not in cols]
    if missing:
        raise PoSchemaError(f"{table} 目標沒有的欄位：{', '.join(missing)}，已中止")
    cond = " AND ".join(f"[{cols[w[0].lower()]}] = %s" for w in where)
    return f"DELETE FROM dbo.{table} WHERE {cond}", tuple(w[1] for w in where)


def _same(a: Any, b: Any) -> bool:
    return ep._same(a, b)


# 單位換算 / 折扣欄位：原列這些值與 Quantity / Price 不一致時，本站無法安全改數量或單價
_CONVERSION_COLS = (("sQuantity", "Quantity"), ("EQuantity", "Quantity"), ("sPrice", "Price"))


def _conversion_issue(row: Dict[str, Any]) -> str:
    low = ep._lower(row)
    for a, b in _CONVERSION_COLS:
        if a.lower() in low and not _same(low[a.lower()], low.get(b.lower())):
            return f"{a}≠{b}"
    if "discount" in low and not _same(low["discount"], 1):
        return f"折扣 Discount={low['discount']}"
    return ""


def _plan_edit(cur, company: str, req: Dict[str, Any], lock: bool) -> Dict[str, Any]:
    main, subs = _read_bill(cur, req["bill_no"], lock=lock)
    reason = lock_reason(main, subs)
    if reason:
        raise PoValidationError(reason)
    _check_snapshot(main, subs, req["loaded"])
    bill_no = _text(_get(main, "BillNO"))
    bill_date = _int(_get(main, "BillDate"))
    by_row = {_int(_get(s, "RowNO")): s for s in subs}
    max_lines = max(ep.MAX_LINES, len(subs))
    if len(req["lines"]) > max_lines:
        raise PoValidationError(f"明細最多 {max_lines} 行")

    # 供應商（這家公司的主檔）
    cur.execute("SELECT * FROM comCustomer WHERE Flag = 2 AND ID = %s", (req["supplier_id"],))
    supplier = _one(cur)
    if supplier is None:
        raise PoValidationError(f"{ep.target_label(company)} 找不到供應商 {req['supplier_id']}")
    supplier_id = _text(_get(supplier, "ID"))
    old_supplier_id = _text(_get(main, "CustomerID"))

    # 原有列：RowNO 要存在、料號不可改；原有 -TAB 列不可被移除
    kept_rows = {l["row_no"] for l in req["lines"] if l["row_no"]}
    for l in req["lines"]:
        if not l["row_no"]:
            continue
        orig = by_row.get(l["row_no"])
        if orig is None:
            raise PoValidationError(f"採購單 {bill_no} 沒有第 {l['row_no']} 列（請重新載入）")
        if _text(_get(orig, "ProdID")).upper() != l["part_number"]:
            raise PoValidationError(
                f"第 {l['row_no']} 列原料號 {_text(_get(orig, 'ProdID'))} 不可改成 {l['part_number']}，請移除該列後新增一列")
    for row_no, s in by_row.items():
        if row_no not in kept_rows and ep.customer_supplied_reason(_get(s, "ProdID")):
            raise PoValidationError(
                f"第 {row_no} 列 {_text(_get(s, 'ProdID'))} 是原有的 -TAB 客供料明細，本站不會刪除，請保留該列")

    # 新列：料號主檔與明細範本（同開新單：該料號最近一張採購明細，沒有就用本單第 1 列）
    products: Dict[str, Dict[str, Any]] = {}
    sub_tpls: Dict[str, Dict[str, Any]] = {}
    for l in req["lines"]:
        if l["row_no"] or l["part_number"] in products:
            continue
        cur.execute("SELECT * FROM comProduct WHERE ProdID = %s", (l["part_number"],))
        prod = _one(cur)
        if prod is None:
            raise PoValidationError(f"{ep.target_label(company)} 找不到料號 {l['part_number']}，請先在正航建立")
        products[l["part_number"]] = prod
        cur.execute(
            "SELECT TOP 1 * FROM ordBillSub WHERE Flag = 4 AND ProdID = %s ORDER BY BillDate DESC, BillNO DESC, RowNO DESC",
            (_text(_get(prod, "ProdID")),),
        )
        tpl = _one(cur) or (subs[0] if subs else None)
        if tpl is None:
            raise PoValidationError("找不到可用的明細範本，無法新增明細列")
        sub_tpls[l["part_number"]] = tpl

    # 幣別 / 匯率：沒換幣別 → 保留原單匯率；換成外幣 → 同開新單取 comCurrencySys.BuyInRate
    old_currency, raw_curr = _currency_of(main)
    if not old_currency:
        raise PoValidationError(f"採購單幣別 {raw_curr} 本站不支援（只支援台幣 NTD / 美金 USD），請到正航修改")
    currency = req["currency"] or old_currency
    exch_rate = Decimal(1)
    amount_digits = 2
    if currency != ep.LOCAL_CURRENCY:
        cur.execute(
            "SELECT CurrencyID, BuyInRate, FixedRate, AmountDeciDigits FROM comCurrencySys WHERE CurrencyID = %s",
            (currency,),
        )
        row = _one(cur)
        digits = _get(row or {}, "AmountDeciDigits", None)
        amount_digits = int(digits) if digits is not None else 3
        if currency == old_currency and _dec(_get(main, "ExchRate")) > 0:
            exch_rate = _dec(_get(main, "ExchRate"))
        else:
            rate = _dec(_get(row or {}, "BuyInRate", 0)) or _dec(_get(row or {}, "FixedRate", 0))
            if row is None or rate <= 0:
                raise PoValidationError(f"{ep.target_label(company)} 幣別檔（comCurrencySys）找不到 {currency} 的匯率")
            exch_rate = rate

    # 課稅別：沒改課稅別、沒換供應商 → 保留原 TaxType；否則同開新單依供應商 TaxKind
    old_tax_type = _int(_get(main, "TaxType"))
    old_mode = ep.TAX_TAXABLE if old_tax_type == 0 else ep.TAX_EXEMPT
    tax_mode = req["tax_mode"] or old_mode
    tax_kind = ep.vendor_tax_kind(cur, supplier_id)
    if tax_mode == old_mode and supplier_id == old_supplier_id:
        tax_type = old_tax_type
    else:
        tax_type = ep.tax_type_for(tax_mode, tax_kind)

    lines_in = []
    for l in req["lines"]:
        if l["row_no"]:
            orig = by_row[l["row_no"]]
            prod_id, prod_name = _text(_get(orig, "ProdID")), _text(_get(orig, "ProdName"))
        else:
            prod = products[l["part_number"]]
            prod_id, prod_name = _text(_get(prod, "ProdID")), _text(_get(prod, "ProdName"))
        lines_in.append({**l, "prod_id": prod_id, "prod_name": prod_name})
    lines, totals = ep.compute_money(lines_in, currency=currency, exch_rate=exch_rate,
                                     taxable=tax_type == 0, amount_digits=amount_digits)

    main_cols = ep.table_columns(cur, "ordBillMain")
    sub_cols = ep.table_columns(cur, "ordBillSub")
    main_names = _col_map(main_cols)

    # 明細：UPDATE（有變才寫）/ INSERT / DELETE
    next_row = max(by_row) if by_row else 0
    sub_updates, sub_inserts, sub_deletes = [], [], []
    for line in lines:
        line["currency"] = currency
        if line["row_no"]:
            orig = by_row[line["row_no"]]
            values = {
                "Quantity": line["qty"], "Price": line["price"], "Amount": line["amount"], "TaxAmt": line["tax_amt"],
                "PreInDate": line["pre_in_date"], "QtyRemain": line["qty"],
                "MlAmount": line["ml_amount"], "MLTaxAmt": line["ml_tax"],
                "sPrice": line["price"], "sQuantity": line["qty"], "EQuantity": line["qty"],
            }
            changed = any(not _same(_get(orig, k), v) for k, v in values.items())
            line["action"] = "update" if changed else "keep"
            line["old"] = _line_json(orig)
            if not changed:
                continue
            qty_or_price = not (_same(_get(orig, "Quantity"), line["qty"]) and _same(_get(orig, "Price"), line["price"]))
            issue = _conversion_issue(orig)
            if qty_or_price and issue:
                raise PoValidationError(
                    f"第 {line['row_no']} 列 {line['prod_id']} 在正航有單位換算或折扣（{issue}），本站無法安全修改數量 / 單價，請到正航修改")
            sub_updates.append(build_update("ordBillSub", sub_cols, values, [
                ("Flag", 4), ("BillNO", bill_no), ("RowNO", line["row_no"]),
                ("QtyRemain", _get(orig, "QtyRemain")),  # 防呆：未進貨才更新
            ]))
        else:
            next_row += 1
            line["row_no"] = next_row
            line["action"] = "insert"
            values = ep.build_sub_row(sub_tpls[line["part_number"]], bill_no=bill_no, today=bill_date,
                                      index=next_row, line=line)
            sub_inserts.append(ep.build_insert("ordBillSub", sub_cols, values))
    deleted = [by_row[r] for r in sorted(by_row) if r not in kept_rows]
    for s in deleted:
        sub_deletes.append(build_delete("ordBillSub", sub_cols, [
            ("Flag", 4), ("BillNO", bill_no), ("RowNO", _int(_get(s, "RowNO"))), ("QtyRemain", _get(s, "QtyRemain")),
        ]))

    # 表頭：只改這些欄位；付款條件 / 地址 / BillDate / Permitter / IsTrans 等不動
    main_values: Dict[str, Any] = {
        "CustomerID": supplier_id, "Remark": req["remark"], "SalesMan": req["salesman"], "TaxType": int(tax_type),
        "SumQty": totals["sum_qty"], "SumBTaxAmt": totals["sum_amount"], "SumTax": totals["sum_tax"],
        "SumAmtATax": totals["total"], "LocalTotal": totals["local_total"], "LocalTax": totals["local_tax"],
    }
    if "currid" in main_names:
        main_values["CurrID"] = currency
    if "exchrate" in main_names:
        main_values["ExchRate"] = exch_rate
    main_update = build_update("ordBillMain", main_cols, main_values, [
        ("Flag", 4), ("BillNO", bill_no), ("BillStatus", 0), ("EndacaseDate", 0),
    ])

    lines.sort(key=lambda l: l["row_no"])
    return {
        "company": company,
        "bill_no": bill_no,
        "bill_date": bill_date,
        "supplier": supplier,
        "supplier_id": supplier_id,
        "old_supplier_id": old_supplier_id,
        "lines": lines,
        "deleted": deleted,
        "totals": totals,
        "old_totals": {
            "sum_qty": _dec(_get(main, "SumQty")), "sum_amount": _dec(_get(main, "SumBTaxAmt")),
            "sum_tax": _dec(_get(main, "SumTax")), "total": _dec(_get(main, "SumAmtATax")),
            "local_total": _dec(_get(main, "LocalTotal")), "local_tax": _dec(_get(main, "LocalTax")),
        },
        "terms": {"currency": currency, "exch_rate": exch_rate, "amount_digits": amount_digits, "tax_mode": tax_mode,
                  "tax_mode_source": "manual" if req["tax_mode"] else "default", "tax_type": tax_type, "tax_kind": tax_kind},
        "main_update": main_update,
        "sub_updates": sub_updates,
        "sub_inserts": sub_inserts,
        "sub_deletes": sub_deletes,
    }


def _summary(plan: Dict[str, Any], req: Dict[str, Any]) -> Dict[str, Any]:
    company = plan["company"]
    lines = []
    for line in plan["lines"]:
        item = ep._jsonable_lines([line])[0]
        item.pop("allocations", None)
        item.update(row_no=line["row_no"], action=line["action"], qty_remain=float(line["qty"]),
                    customer_supplied=bool(ep.customer_supplied_reason(line["prod_id"])))
        if line.get("old"):
            item["old"] = line["old"]
        lines.append(item)
    return {
        "company": company,
        "company_label": ep.target_label(company),
        "is_test": ep.is_test(company),
        "target": company,
        "target_label": ep.target_label(company),
        "bill_no": plan["bill_no"],
        "bill_date": ep._ymd_text(plan["bill_date"]),
        "supplier": {"id": plan["supplier_id"], "name": _supplier_name(plan["supplier"]),
                     "full_name": _text(_get(plan["supplier"], "FullName"))},
        "supplier_changed": plan["supplier_id"] != plan["old_supplier_id"],
        "old_supplier_id": plan["old_supplier_id"],
        "remark": req["remark"],
        "salesman": req["salesman"],
        "lines": lines,
        "deleted_lines": [_line_json(s) for s in plan["deleted"]],
        "totals": ep._jsonable_totals(plan["totals"]),
        "old_totals": ep._jsonable_totals(plan["old_totals"]),
        "counts": {"update": len(plan["sub_updates"]), "insert": len(plan["sub_inserts"]),
                   "delete": len(plan["sub_deletes"]),
                   "keep": sum(1 for l in plan["lines"] if l["action"] == "keep")},
        **ep._terms_summary(plan["terms"]),
    }


# ── 對外：預覽 / 寫入 ────────────────────────────────────────────────────────

def preview_update(raw_req: Dict[str, Any]) -> Dict[str, Any]:
    """改單預覽：在單據所屬公司唯讀計算（不寫入）；附上現在能不能寫入與原因。"""
    req = normalize_edit_request(raw_req)
    company = _resolve_company(req["company"])
    with _read_session(company) as conn:
        try:
            plan = _plan_edit(conn.cursor(), company, req, lock=False)
        except ErpError:
            raise
        except Exception as exc:
            raise ErpUnavailable(f"ERP 查詢失敗：{ep._safe_message(exc)}") from None
    block = write_block_reason(company)
    return {**_summary(plan, req), "can_write": not block, "write_block_reason": block}


def _verify(wcur, plan: Dict[str, Any]) -> None:
    bill_no = plan["bill_no"]
    wcur.execute(
        "SELECT BillNO, CustomerID, SumQty, SumBTaxAmt, SumTax, SumAmtATax, LocalTotal, LocalTax, BillStatus, EndacaseDate "
        "FROM ordBillMain WHERE Flag = 4 AND BillNO = %s",
        (bill_no,),
    )
    head = _one(wcur)
    if head is None:
        raise PoSchemaError("寫入後讀回不到採購單表頭，已 rollback")
    t = plan["totals"]
    for col, key in (("SumQty", "sum_qty"), ("SumBTaxAmt", "sum_amount"), ("SumTax", "sum_tax"),
                     ("SumAmtATax", "total"), ("LocalTotal", "local_total"), ("LocalTax", "local_tax")):
        if not _same(_get(head, col), t[key]):
            raise PoSchemaError(f"寫入後讀回的 {col} 與預期不符，已 rollback")
    if _text(_get(head, "CustomerID")) != plan["supplier_id"] or is_closed(head):
        raise PoSchemaError("寫入後讀回的表頭供應商 / 狀態與預期不符，已 rollback")
    wcur.execute(
        "SELECT RowNO, ProdID, Quantity, Price, Amount, TaxAmt, PreInDate, QtyRemain FROM ordBillSub "
        "WHERE Flag = 4 AND BillNO = %s ORDER BY RowNO",
        (bill_no,),
    )
    subs = _rows(wcur)
    if len(subs) != len(plan["lines"]):
        raise PoSchemaError("寫入後讀回的明細行數與預期不符，已 rollback")
    for got, line in zip(subs, plan["lines"]):
        same = (
            _int(_get(got, "RowNO")) == line["row_no"]
            and _text(_get(got, "ProdID")).upper() == line["prod_id"].upper()
            and _same(_get(got, "Quantity"), line["qty"])
            and _same(_get(got, "Price"), line["price"])
            and _same(_get(got, "Amount"), line["amount"])
            and _same(_get(got, "TaxAmt"), line["tax_amt"])
            and _int(_get(got, "PreInDate")) == line["pre_in_date"]
            and _same(_get(got, "QtyRemain"), line["qty"])
        )
        if not same:
            raise PoSchemaError(f"寫入後讀回的第 {line['row_no']} 列明細與預期不符，已 rollback")


def _exec_one(wcur, stmt: Tuple[str, Tuple[Any, ...]], what: str) -> None:
    wcur.execute(*stmt)
    count = getattr(wcur, "rowcount", None)
    if isinstance(count, int) and count >= 0 and count != 1:
        raise PoSchemaError(f"{what}影響 {count} 列（預期 1 列），已 rollback")


def _write(req: Dict[str, Any], target: str, creds: Dict[str, Any]) -> Dict[str, Any]:
    with ep._write_lock:
        wconn = ep._connect_write(creds)
        committed = False
        try:
            wcur = wconn.cursor()
            ep._check_db_name(wcur, target)
            plan = _plan_edit(wcur, target, req, lock=True)
            _exec_one(wcur, plan["main_update"], "更新表頭")
            for stmt in plan["sub_deletes"]:
                _exec_one(wcur, stmt, "刪除明細")
            for stmt in plan["sub_updates"]:
                _exec_one(wcur, stmt, "更新明細")
            for stmt in plan["sub_inserts"]:
                wcur.execute(*stmt)
            _verify(wcur, plan)
            ep._check_db_name(wcur, target)
            try:
                wconn.commit()
                committed = True
            except Exception as exc:
                raise ErpUnavailable(
                    f"commit 失敗，結果不確定，請到正航確認採購單 {plan['bill_no']} 是否已修改：{ep._safe_message(exc, creds)}"
                ) from None
        except ErpError:
            raise
        except Exception as exc:
            raise ErpUnavailable(f"寫入正航失敗，已 rollback：{ep._safe_message(exc, creds)}") from None
        finally:
            ep._close_quietly(wconn, rollback=not committed)
    return _summary(plan, req)


def update_po(raw_req: Dict[str, Any], actor: str = "web", client_ip: str = "") -> Dict[str, Any]:
    """修改舊採購單。需 confirm=True、載入當下的狀態快照（loaded），且單據公司 = 改單寫入目標。"""
    from .. import database as db

    if raw_req.get("confirm") is not True:
        raise PoValidationError("需確認後才會寫入正航（confirm=true）")
    req = normalize_edit_request(raw_req)
    if req["loaded"] is None:
        raise PoValidationError("缺少載入當下的單據狀態（loaded），請重新載入採購單後再改")
    company = _text(req["company"]) or _source_db()
    block = write_block_reason(company)
    if block:
        raise PoValidationError(block)
    target = get_edit_target()
    creds = ep.write_credentials(target)
    if creds is None:
        raise ErpNotConfigured(ep.UNAVAILABLE_REASON)
    try:
        result = _write(req, target, creds)
    except Exception as exc:
        message = ep._safe_message(exc, creds)
        db.log_activity("erp_po_update_failed", f"{ep.target_label(target)} 採購單 {req['bill_no']}：{message}")
        raise
    c = result["counts"]
    note = (f"本站修改：{c['update']} 列更新、{c['insert']} 列新增、{c['delete']} 列刪除，"
            f"含稅合計 {result['totals']['total']:g}")
    try:
        db.mark_erp_purchase_order_edited(result["bill_no"], target, note)
    except Exception:
        pass  # 本機註記失敗不可蓋掉已完成的 ERP 結果
    db.log_activity(
        "erp_po_updated",
        f"{ep.target_label(target)} 採購單 {result['bill_no']} 已修改（{note}，actor={actor}，ip={client_ip}）",
    )
    return {"ok": True, **result}

