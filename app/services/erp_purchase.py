"""從網站在正航 ERP（SQL Server）開採購單（ordBillMain / ordBillSub，Flag=4）。

與 erp_stock.py（唯讀）不同，這裡會「寫入」ERP，所以規則很保守：
- 只 INSERT 新單據 / 缺少的主檔，絕不 UPDATE / DELETE 既有資料。
- 寫入目標公司由 ERP_WRITE_TARGET 指定（預設 CHIComp02 測試公司），帳密只放環境變數。
- INSERT 一律明列欄位，欄位清單取自「目標」資料庫的 sys.columns（兩家公司欄位順序不同）。
- 整張單（含補主檔）在同一個 transaction 內，寫完讀回比對後才 commit，任何錯誤即 rollback。
- 範本單（付款條件 / 地址等）取自來源公司（ERP_SQL_DATABASE，預設 CHIComp03）的最近一張採購單。
錯誤訊息一律不含帳號密碼。
"""
from __future__ import annotations

import calendar
import json
import math
import os
import re
import threading
from contextlib import contextmanager
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Dict, Iterator, List, Optional, Tuple

from . import erp_stock
from .erp_stock import ErpError, ErpNotConfigured, ErpUnavailable
from .local_time import local_now

TARGET_RE = re.compile(r"^CHIComp\d\d$")
KNOWN_TARGETS = ["CHIComp01", "CHIComp02", "CHIComp03"]
DEFAULT_TEST_TARGETS = "CHIComp01,CHIComp02,CHIComp13"
PRODUCTION_TARGET = "CHIComp03"  # 正式公司：絕不自動複製主檔進去、絕不使用測試帳密
DEFAULT_TARGET = "CHIComp02"
TAX_RATE = Decimal("0.05")
MAX_LINES = 50
MAX_PREVIEW_GROUPS = 20
DEFAULT_SALESMAN = "017"
DEFAULT_MAKER = "Andy"
DEFAULT_MAKER_ID = "017"
UNAVAILABLE_REASON = "無法使用（未設定寫入帳號）"
BILL_PREFIX = "PO-"

_write_lock = threading.Lock()  # 同一程序內一次只寫一張，避免單號競爭


def test_targets() -> set:
    """測試公司（可共用 ERP_SQL_TEST_* 帳密、可從來源複製主檔）；每次呼叫都讀 ERP_TEST_TARGETS。

    格式不符 CHIComp + 兩位數字者略過；正式公司與來源資料庫即使列入也一律排除。
    """
    source_db = _env("ERP_SQL_DATABASE", "CHIComp03")
    items = {t.strip() for t in _env("ERP_TEST_TARGETS", DEFAULT_TEST_TARGETS).split(",")}
    return {t for t in items if TARGET_RE.match(t) and t != PRODUCTION_TARGET and t != source_db}


def is_test(target: str) -> bool:
    return target in test_targets()


class PoValidationError(ErpError):
    """輸入資料或 ERP 資料不符規則（HTTP 400）。"""


# 客供料：只看料號 -TAB 結尾（不分大小寫），與正航料號類別無關。客供料向客戶要料，不可開採購單。
CUSTOMER_SUPPLIED_SUFFIX = "-TAB"


def customer_supplied_reason(part_number: Any, class_id: Any = "") -> str:
    """客供料判斷：回傳 'suffix' / ''（不是客供料）。class_id 保留參數相容，已不參與判斷。"""
    if str(part_number or "").strip().upper().endswith(CUSTOMER_SUPPLIED_SUFFIX):
        return "suffix"
    return ""


def _customer_supplied_error(part: str, reason: str = "suffix") -> PoValidationError:
    return PoValidationError(f"料號 {part} 為客供料（料號以 -TAB 結尾），不可開採購單，請向客戶要料")


class DuplicateWarning(PoValidationError):
    """明細在本站已開過採購單，未帶 allow_duplicate 時不寫入並回報（items 為已開過的明細）。"""

    def __init__(self, items: List[Dict[str, Any]]):
        super().__init__("部分明細在本站已開過採購單，請確認後再送出")
        self.items = items


class DuplicateSubmitError(PoValidationError):
    """相同內容的採購單剛建立過 / 正在建立中（防連點，不能用 allow_duplicate 繞過）。"""


class PoSchemaError(ErpError):
    """目標資料庫欄位與要寫入的資料對不上（HTTP 500）。"""


# ── 設定 / 帳密 ──────────────────────────────────────────────────────────────

def _env(name: str, default: str = "") -> str:
    return erp_stock._env(name, default)


def target_label(target: str) -> str:
    if target == PRODUCTION_TARGET:
        return "公司3（正式）"
    m = re.match(r"^CHIComp(\d\d)$", target or "")
    if not m:
        return str(target)
    return f"公司{int(m.group(1))}" + ("（測試）" if is_test(target) else "")


def get_target() -> str:
    target = _env("ERP_WRITE_TARGET", DEFAULT_TARGET)
    if not TARGET_RE.match(target):
        raise ErpNotConfigured("ERP_WRITE_TARGET 格式不正確（需為 CHIComp 加兩位數字，例如 CHIComp02）")
    return target


def parse_server(value: str, default_server: str, default_port: int) -> Tuple[str, int]:
    """'host' 或 'host,port' → (host, port)。"""
    text = (value or "").strip()
    if not text:
        return default_server, default_port
    host, _, port_text = text.partition(",")
    host = host.strip() or default_server
    try:
        port = int(port_text.strip()) if port_text.strip() else default_port
    except ValueError:
        port = default_port
    return host, port


def write_credentials(target: str) -> Optional[Dict[str, Any]]:
    """目標公司的寫入連線設定（含密碼，只供內部連線，不可回傳前端）；沒設定回 None。"""
    suffix = target.upper()
    user = _env(f"ERP_SQL_WRITE_USER_{suffix}")
    password = os.environ.get(f"ERP_SQL_WRITE_PASSWORD_{suffix}") or ""
    server_value = _env(f"ERP_SQL_WRITE_SERVER_{suffix}")
    if not (user and password):
        # 測試帳只給測試公司用；資料庫一律是 target，不看 ERP_SQL_TEST_DB
        if is_test(target):
            user = _env("ERP_SQL_TEST_USER")
            password = os.environ.get("ERP_SQL_TEST_PASSWORD") or ""
            server_value = _env("ERP_SQL_TEST_SERVER")
    if not (user and password):
        return None
    base = erp_stock.get_settings()
    host, port = parse_server(server_value, base["server"], base["port"])
    return {
        "server": host,
        "port": port,
        "user": user,
        "password": password,
        "database": target,
        "timeout": max(15.0, base["timeout"]),
        "encryption": base["encryption"],
        "tds_version": base["tds_version"],
    }


def _salesman_default() -> str:
    return _env("ERP_PO_SALESMAN", DEFAULT_SALESMAN)


def get_config() -> Dict[str, Any]:
    """前端顯示用設定；絕不含帳號 / 密碼。"""
    base = erp_stock.get_settings()
    targets = [
        {"db": t, "label": target_label(t), "available": write_credentials(t) is not None}
        for t in dict.fromkeys(KNOWN_TARGETS + sorted(test_targets()))
    ]
    result: Dict[str, Any] = {
        "target": "",
        "label": "",
        "is_test": False,
        "available": False,
        "reason": "",
        "source": base["database"],
        "default_salesman": _salesman_default(),
        "default_maker": _env("ERP_PO_MAKER", DEFAULT_MAKER),
        "tax_rate": float(TAX_RATE),
        "targets": targets,
    }
    try:
        target = get_target()
    except ErpError as exc:
        result["reason"] = str(exc)
        return result
    result["target"] = target
    result["label"] = target_label(target)
    result["is_test"] = is_test(target)
    if write_credentials(target) is None:
        result["reason"] = UNAVAILABLE_REASON
    else:
        result["available"] = True
    return result


def _require_available() -> Tuple[str, Dict[str, Any]]:
    target = get_target()
    creds = write_credentials(target)
    if creds is None:
        raise ErpNotConfigured(UNAVAILABLE_REASON)
    return target, creds


# ── 連線 ─────────────────────────────────────────────────────────────────────

def _scrub(text: str, creds: Optional[Dict[str, Any]]) -> str:
    for secret in ((creds or {}).get("password"), (creds or {}).get("user")):
        if secret:
            text = text.replace(str(secret), "***")
    return text


def _safe_message(exc: Exception, creds: Optional[Dict[str, Any]] = None) -> str:
    if isinstance(exc, ErpError):
        return _scrub(str(exc), creds)
    detail = _scrub(str(exc), creds).strip().replace("\n", " ")[:200]
    return f"{type(exc).__name__}：{detail}" if detail else type(exc).__name__


def _connect_write(creds: Dict[str, Any]):
    try:
        import pymssql  # 延遲載入，未安裝時 app 仍可啟動
    except ImportError as exc:
        raise ErpUnavailable("缺少 pymssql 套件，無法連線 ERP") from exc
    timeout = int(creds["timeout"])
    try:
        return pymssql.connect(
            server=creds["server"],
            port=creds["port"],
            user=creds["user"],
            password=creds["password"],
            database=creds["database"],
            login_timeout=timeout,
            timeout=timeout,
            as_dict=False,
            autocommit=False,
            encryption=creds["encryption"],
            tds_version=creds["tds_version"],
        )
    except Exception as exc:
        raise ErpUnavailable(
            f"無法連線 ERP 寫入帳號（{creds['server']}:{creds['port']} / {creds['database']}）：{type(exc).__name__}"
        ) from None


@contextmanager
def _source_session() -> Iterator[Any]:
    """唯讀來源連線（ERP_SQL_DATABASE）。"""
    conn = erp_stock._connect(erp_stock.get_settings())
    try:
        yield conn
    except ErpError:
        raise
    except Exception as exc:
        raise ErpUnavailable(f"ERP 查詢失敗：{type(exc).__name__}") from None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _rows(cur) -> List[Dict[str, Any]]:
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def _one(cur) -> Optional[Dict[str, Any]]:
    rows = _rows(cur)
    return rows[0] if rows else None


def _lower(row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).lower(): v for k, v in row.items()}


def _get(row: Dict[str, Any], name: str, default: Any = None) -> Any:
    return _lower(row).get(name.lower(), default)


def _text(value: Any) -> str:
    return str(value if value is not None else "").strip()


# ── 純函式：金額 / 日期 / 單號 ───────────────────────────────────────────────

def _round(value: Decimal, places: int) -> Decimal:
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def compute_money(lines: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Decimal]]:
    """每行 amount=round(qty*price,2)、tax=round(amount*5%,0)；表頭稅額由未稅合計重算（正航整數稅）。"""
    out = []
    sum_qty = Decimal(0)
    sum_amount = Decimal(0)
    for line in lines:
        qty = Decimal(line["qty"])
        price = Decimal(line["price"])
        amount = _round(qty * price, 2)
        tax = _round(amount * TAX_RATE, 0)
        out.append({**line, "qty": qty, "price": price, "amount": amount, "tax_amt": tax})
        sum_qty += qty
        sum_amount += amount
    sum_tax = _round(sum_amount * TAX_RATE, 0)
    totals = {
        "sum_qty": sum_qty,
        "sum_amount": sum_amount,
        "sum_tax": sum_tax,
        "total": sum_amount + sum_tax,
    }
    return out, totals


def date_to_int(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def _split_ymd(value: int) -> Optional[Tuple[int, int, int]]:
    y, m, d = value // 10000, value // 100 % 100, value % 100
    if y < 1 or not 1 <= m <= 12 or d < 1:
        return None
    return y, m, d


def _month_index(value: int) -> int:
    return (value // 10000) * 12 + (value // 100 % 100) - 1


def shift_ymd(value: Any, months: int) -> int:
    """YYYYMMDD 平移 N 個月，日期超過月底則取月底；0 / 非法值原樣返回。"""
    try:
        v = int(value)
    except (TypeError, ValueError):
        return 0
    parts = _split_ymd(v)
    if v == 0 or parts is None or months == 0:
        return v
    y, m, d = parts
    ny, nm = divmod(y * 12 + (m - 1) + months, 12)
    nm += 1
    if ny < 1:
        return v
    return ny * 10000 + nm * 100 + min(d, calendar.monthrange(ny, nm)[1])


def shift_ym(value: Any, months: int) -> int:
    """YYYYMM 平移 N 個月；0 / 非法值原樣返回。"""
    try:
        v = int(value)
    except (TypeError, ValueError):
        return 0
    y, m = v // 100, v % 100
    if v == 0 or y < 1 or not 1 <= m <= 12 or months == 0:
        return v
    ny, nm = divmod(y * 12 + (m - 1) + months, 12)
    return ny * 100 + nm + 1 if ny >= 1 else v


def payment_dates(template: Dict[str, Any], today: int) -> Dict[str, int]:
    """範本單的 AccMonth / PrepayDay / EncashDay 換算到今天。

    範本日期就是今天 → 原值；否則每個非 0 值平移「範本單日與今天相差的月數」；
    範本的 EncashDay 等於範本單日者，EncashDay 直接用今天。
    """
    tpl = _lower(template)
    tpl_date = int(tpl.get("billdate") or 0)
    acc = int(tpl.get("accmonth") or 0)
    prepay = int(tpl.get("prepayday") or 0)
    encash = int(tpl.get("encashday") or 0)
    if tpl_date != today and _split_ymd(tpl_date) is not None:
        diff = _month_index(today) - _month_index(tpl_date)
        encash_same_day = encash == tpl_date
        acc = shift_ym(acc, diff)
        prepay = shift_ymd(prepay, diff)
        encash = today if encash_same_day else shift_ymd(encash, diff)
    return {"AccMonth": acc, "PrepayDay": prepay, "EncashDay": encash}


def next_bill_no(cur, today: int, lock: bool) -> str:
    """PO-YYYYMMDD + 3 碼流水號；lock=True 時用 UPDLOCK/HOLDLOCK 鎖住範圍（寫入交易內）。"""
    prefix = f"{BILL_PREFIX}{today}"
    hint = " WITH (UPDLOCK, HOLDLOCK)" if lock else ""
    cur.execute(
        f"SELECT MAX(BillNO) FROM ordBillMain{hint} WHERE Flag = 4 AND BillNO LIKE %s AND LEN(BillNO) = 14",
        (prefix + "%",),
    )
    found = cur.fetchone()
    last = _text(found[0]) if found else ""
    seq = 0
    if last:
        try:
            seq = int(last[len(prefix):])
        except ValueError:
            raise PoSchemaError(f"既有採購單號格式異常：{last}") from None
    while True:
        seq += 1
        if seq > 999:
            raise PoValidationError(f"{prefix} 當日採購單號已用完（最多 999 張）")
        bill_no = f"{prefix}{seq:03d}"
        cur.execute("SELECT 1 FROM ordBillSub WHERE Flag = 4 AND BillNO = %s", (bill_no,))
        if cur.fetchone() is None:
            return bill_no


# ── 欄位 / INSERT ────────────────────────────────────────────────────────────

def table_columns(cur, table: str) -> List[Dict[str, Any]]:
    cur.execute(
        "SELECT c.name, c.is_identity, c.is_computed FROM sys.columns c "
        "WHERE c.object_id = OBJECT_ID(%s) ORDER BY c.column_id",
        (f"dbo.{table}",),
    )
    cols = [
        {"name": str(r[0]), "identity": bool(r[1]), "computed": bool(r[2])}
        for r in cur.fetchall()
    ]
    if not cols:
        raise PoSchemaError(f"目標資料庫找不到資料表 {table}")
    return cols


def build_insert(table: str, columns: List[Dict[str, Any]], values: Dict[str, Any]) -> Tuple[str, Tuple[Any, ...]]:
    """依目標資料表欄位順序組 INSERT；缺值或多餘的值一律中止，不依賴欄位位置。"""
    vals = _lower(values)
    all_names = {c["name"].lower() for c in columns}
    insertable = [c for c in columns if not c["identity"] and not c["computed"]]
    extra = sorted(k for k in vals if k not in all_names)
    missing = [c["name"] for c in insertable if c["name"].lower() not in vals]
    if extra or missing:
        parts = []
        if missing:
            parts.append(f"目標缺值欄位：{', '.join(missing[:10])}")
        if extra:
            parts.append(f"目標沒有的欄位：{', '.join(extra[:10])}")
        raise PoSchemaError(f"{table} 欄位與要寫入的資料不一致（{'；'.join(parts)}），已中止")
    names = [c["name"] for c in insertable]
    sql = (
        f"INSERT INTO dbo.{table} ({', '.join('[' + n + ']' for n in names)}) "
        f"VALUES ({', '.join(['%s'] * len(names))})"
    )
    return sql, tuple(vals[n.lower()] for n in names)


def _with(row: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    out = _lower(row)
    out.update({k.lower(): v for k, v in overrides.items()})
    return out


def build_main_row(
    template: Dict[str, Any], *, bill_no: str, today: int, remark: str, salesman: str,
    totals: Dict[str, Decimal],
) -> Dict[str, Any]:
    return _with(template, {
        "Flag": 4,
        "BillNO": bill_no,
        "BillDate": today,
        "Remark": remark,
        "SumQty": totals["sum_qty"],
        "SumBTaxAmt": totals["sum_amount"],
        "SumTax": totals["sum_tax"],
        "SumAmtATax": totals["total"],
        "LocalTotal": totals["sum_amount"],
        "LocalTax": totals["sum_tax"],
        "BillStatus": 0,
        "EndacaseDate": 0,
        "TaxType": 0,
        "SalesMan": salesman,
        "Maker": _env("ERP_PO_MAKER", DEFAULT_MAKER),
        "MakerID": _env("ERP_PO_MAKER_ID", DEFAULT_MAKER_ID),
        "Permitter": "",
        "PermitterID": "",
        "IsTrans": 0,
        "MergeOutState": 0,
        "CustBillNo": "",
        **payment_dates(template, today),
    })


def build_sub_row(
    template: Dict[str, Any], *, bill_no: str, today: int, index: int, line: Dict[str, Any],
) -> Dict[str, Any]:
    qty, price, amount, tax = line["qty"], line["price"], line["amount"], line["tax_amt"]
    return _with(template, {
        "Flag": 4,
        "BillNO": bill_no,
        "BillDate": today,
        "RowNO": index,
        "SerNO": index,
        "FromRow": index,
        "FromNO": "",
        "ProdID": line["prod_id"],
        "ProdName": line["prod_name"],
        "Quantity": qty,
        "Price": price,
        "Amount": amount,
        "TaxRate": TAX_RATE,
        "TaxAmt": tax,
        "PreInDate": line["pre_in_date"],
        "QtyRemain": qty,
        "MlAmount": amount,
        "MLTaxAmt": tax,
        "sPrice": price,
        "sQuantity": qty,
        "EQuantity": qty,
        "OutQty": 0,
        "BClearTranQty": 0,
        "UnValidQty": 0,
        "Discount": 1,
        "IsGift": 0,
        "ItemRemark": "",
        "NeedDate": 0,
    })


# ── 輸入驗證 ─────────────────────────────────────────────────────────────────

def _to_decimal(value: Any, label: str) -> Decimal:
    try:
        if isinstance(value, float) and not math.isfinite(value):
            raise InvalidOperation
        d = Decimal(str(value))
        if not d.is_finite():
            raise InvalidOperation
        return d
    except (InvalidOperation, ValueError):
        raise PoValidationError(f"{label}不是有效數字") from None


ALLOC_KINDS = ("order", "extra", "manual")
ALLOC_SOURCES = ("shortage_po", "manual_po")
_ALLOC_EPS = Decimal("0.000001")


def _normalize_allocations(raw_allocs: Any, qty: Decimal, index: int) -> List[Dict[str, Any]]:
    """驗證明細的訂單分配（每筆 qty>0、合計不得超過本行數量）；不足的部分補一筆 manual。"""
    allocs: List[Dict[str, Any]] = []
    total = Decimal(0)
    for raw in raw_allocs or []:
        a_qty = _to_decimal(raw.get("qty"), f"第 {index} 行分配數量")
        if a_qty <= 0:
            raise PoValidationError(f"第 {index} 行：分配數量必須大於 0")
        kind = _text(raw.get("kind")) or ("order" if raw.get("order_id") else "manual")
        if kind not in ALLOC_KINDS:
            raise PoValidationError(f"第 {index} 行：分配類型不正確")
        try:
            order_id = int(raw.get("order_id") or 0)
        except (TypeError, ValueError):
            raise PoValidationError(f"第 {index} 行：分配訂單編號不正確") from None
        total += a_qty
        allocs.append({
            "order_id": order_id,
            "order_code": _text(raw.get("order_code")),
            "model": _text(raw.get("model")),
            "customer_po": _text(raw.get("customer_po")),
            "ship_date": _text(raw.get("ship_date")),
            "dispatch_session_id": 0,
            "qty": float(a_qty),
            "kind": kind,
        })
    if total > qty + _ALLOC_EPS:
        raise PoValidationError(f"第 {index} 行：分配數量合計超過採購數量")
    if qty - total > _ALLOC_EPS:
        allocs.append({
            "order_id": 0, "order_code": "", "model": "", "customer_po": "", "ship_date": "",
            "dispatch_session_id": 0, "qty": float(qty - total), "kind": "manual",
        })
    return allocs


def _resolve_allocation_orders(lines: List[Dict[str, Any]]) -> None:
    """綁訂單的分配：補上訂單資料缺漏的欄位，並以本機資料查出目前有效的發料批次 id。"""
    ids = [a["order_id"] for l in lines for a in l["allocations"] if a["order_id"] > 0]
    if not ids:
        return
    from .. import database as db

    sessions = db.get_latest_active_dispatch_session_ids(ids)
    for line in lines:
        for a in line["allocations"]:
            if a["order_id"] <= 0:
                continue
            a["dispatch_session_id"] = sessions.get(a["order_id"], 0)
            if not (a["order_code"] and a["model"]):
                order = db.get_order(a["order_id"]) or {}
                a["order_code"] = a["order_code"] or _text(order.get("code"))
                a["model"] = a["model"] or _text(order.get("model"))
                a["customer_po"] = a["customer_po"] or _text(order.get("po_number"))
                a["ship_date"] = a["ship_date"] or _text(order.get("delivery_date") or order.get("ship_date"))


def normalize_request(req: Dict[str, Any]) -> Dict[str, Any]:
    supplier_id = _text(req.get("supplier_id"))
    if not supplier_id:
        raise PoValidationError("請選擇供應商")
    raw_lines = req.get("lines") or []
    if not 1 <= len(raw_lines) <= MAX_LINES:
        raise PoValidationError(f"明細需為 1～{MAX_LINES} 行")
    lines = []
    for i, raw in enumerate(raw_lines, 1):
        part = _text(raw.get("part_number")).upper()
        if not part:
            raise PoValidationError(f"第 {i} 行：請輸入料號")
        if customer_supplied_reason(part) == "suffix":
            raise _customer_supplied_error(part, "suffix")
        qty = _to_decimal(raw.get("qty"), f"第 {i} 行數量")
        if qty <= 0:
            raise PoValidationError(f"第 {i} 行：數量必須大於 0")
        price = _to_decimal(raw.get("price"), f"第 {i} 行單價")
        if price < 0:
            raise PoValidationError(f"第 {i} 行：單價不可為負")
        try:
            pre_in = datetime.strptime(_text(raw.get("pre_in_date")), "%Y-%m-%d").date()
        except ValueError:
            raise PoValidationError(f"第 {i} 行：預計進貨日格式需為 YYYY-MM-DD") from None
        lines.append({
            "part_number": part, "qty": qty, "price": price, "pre_in_date": date_to_int(pre_in),
            "allocations": _normalize_allocations(raw.get("allocations"), qty, i),
        })
    _resolve_allocation_orders(lines)
    return {
        "alloc_source": _text(req.get("alloc_source")) if _text(req.get("alloc_source")) in ALLOC_SOURCES else "manual_po",
        "supplier_id": supplier_id,
        "remark": _text(req.get("remark")),
        "salesman": _text(req.get("salesman")) or _salesman_default(),
        "lines": lines,
    }


# ── 讀來源 / 規劃 ────────────────────────────────────────────────────────────

def _load_source(src, req: Dict[str, Any]) -> Dict[str, Any]:
    cur = src.cursor()
    cur.execute("SELECT * FROM comCustomer WHERE Flag = 2 AND ID = %s", (req["supplier_id"],))
    supplier = _one(cur)
    if supplier is None:
        raise PoValidationError(f"正航找不到供應商 {req['supplier_id']}")
    supplier_id = _text(_get(supplier, "ID"))

    products: Dict[str, Dict[str, Any]] = {}
    for part in dict.fromkeys(line["part_number"] for line in req["lines"]):
        cur.execute("SELECT * FROM comProduct WHERE ProdID = %s", (part,))
        row = _one(cur)
        if row is None:
            raise PoValidationError(f"正航找不到料號 {part}")
        products[part] = row

    cur.execute(
        "SELECT TOP 1 * FROM ordBillMain WHERE Flag = 4 AND CustomerID = %s ORDER BY BillDate DESC, BillNO DESC",
        (supplier_id,),
    )
    main_tpl = _one(cur)
    if main_tpl is None:
        raise PoValidationError(
            f"供應商 {supplier_id} 在正航沒有任何採購單，付款條件 / 地址無從帶入，請先在正航開第一張"
        )

    sub_tpls: Dict[str, Dict[str, Any]] = {}
    fallback: Optional[Dict[str, Any]] = None
    for part in products:
        prod_id = _text(_get(products[part], "ProdID"))
        cur.execute(
            "SELECT TOP 1 * FROM ordBillSub WHERE Flag = 4 AND ProdID = %s "
            "ORDER BY BillDate DESC, BillNO DESC, RowNO DESC",
            (prod_id,),
        )
        row = _one(cur)
        if row is None:
            if fallback is None:
                cur.execute(
                    "SELECT TOP 1 * FROM ordBillSub WHERE Flag = 4 AND BillNO = %s ORDER BY RowNO",
                    (_text(_get(main_tpl, "BillNO")),),
                )
                fallback = _one(cur)
                if fallback is None:
                    raise PoValidationError("範本採購單沒有明細列，無法帶入明細欄位")
            row = fallback
        sub_tpls[part] = row
    return {"supplier": supplier, "products": products, "main_tpl": main_tpl, "sub_tpls": sub_tpls}


def _master_specs(
    supplier_id: str, prod_ids: List[str], ware_id: str,
    class_ids: Optional[List[str]] = None, depart_id: str = "", person_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """要確認存在於目標公司的主檔，順序即複製順序：類別在料號前、部門在人員前。

    key 為空的項目直接略過；required=False 的（類別 / 部門 / 人員）來源也沒有時略過，不中止。
    """
    specs: List[Dict[str, Any]] = []

    def add(table: str, label: str, key: str, where: str, required: bool, copy_required: bool = False) -> None:
        key = _text(key)
        if key:
            specs.append({"table": table, "label": label, "key": key, "where": where,
                          "params": (key,), "required": required,
                          # 複製進測試公司時來源一定要有（採購員 / 製單人缺了單據會不完整）
                          "copy_required": copy_required})

    add("comCustomer", "供應商", supplier_id, "Flag = 2 AND ID = %s", True)
    for cid in dict.fromkeys(_text(c) for c in (class_ids or [])):
        add("comProductClass", "料號類別", cid, "ClassID = %s", False)
    for pid in prod_ids:
        add("comProduct", "料號", pid, "ProdID = %s", True)
    add("comWareHouse", "倉庫", ware_id, "WareHouseID = %s", True)
    add("comDepartment", "部門", depart_id, "DepartID = %s", False)
    for pid in dict.fromkeys(_text(p) for p in (person_ids or [])):
        add("comPerson", "人員", pid, "PersonID = %s", False, copy_required=True)
    return specs


def _can_copy_masters(target: str, source_db: str) -> bool:
    """只有測試公司（且不是來源本身）才可從來源複製主檔；正式公司一律不行。"""
    return is_test(target) and target != PRODUCTION_TARGET and target != source_db


def _offset_bill_no(bill_no: str, offset: int) -> str:
    """單號尾端 3 碼流水號加 offset，保留補零；超過 999 視為當日用完。"""
    if not offset:
        return bill_no
    head, tail = bill_no[:-3], bill_no[-3:]
    try:
        seq = int(tail) + offset
    except ValueError:
        raise PoSchemaError(f"採購單號格式異常：{bill_no}") from None
    if seq > 999:
        raise PoValidationError(f"{head} 當日採購單號已用完（最多 999 張）")
    return f"{head}{seq:03d}"


def _plan(src, wcur, target: str, req: Dict[str, Any], today: int, lock: bool, bill_offset: int = 0) -> Dict[str, Any]:
    """讀來源範本 + 目標欄位 / 缺少的主檔 / 單號，組出整張單的 INSERT；不寫入。"""
    _check_db_name(wcur, target)
    ctx = _load_source(src, req)
    source_db = erp_stock.get_settings()["database"]

    main_cols = table_columns(wcur, "ordBillMain")
    sub_cols = table_columns(wcur, "ordBillSub")

    supplier_id = _text(_get(ctx["supplier"], "ID"))
    prod_ids = list(dict.fromkeys(_text(_get(r, "ProdID")) for r in ctx["products"].values()))
    ware_id = erp_stock.get_settings()["ware_id"]
    class_ids = [_text(_get(p, "ClassID")) for p in ctx["products"].values()]
    depart_id = _text(_get(ctx["main_tpl"], "DepartID"))
    # 表頭實際寫入的採購員 / 製單人（build_main_row 會覆蓋範本值）
    person_ids = [req["salesman"], _env("ERP_PO_MAKER_ID", DEFAULT_MAKER_ID)]
    missing = []
    for spec in _master_specs(supplier_id, prod_ids, ware_id, class_ids, depart_id, person_ids):
        wcur.execute(f"SELECT 1 FROM {spec['table']} WHERE {spec['where']}", spec["params"])
        if wcur.fetchone() is None:
            missing.append(spec)
    can_copy = _can_copy_masters(target, source_db)
    if missing and can_copy:
        # 類別 / 部門 / 人員來源也沒有就略過（不是必要主檔）；必要主檔來源沒有則在 _copy_masters 報錯
        scur = src.cursor()
        kept = []
        for spec in missing:
            if spec["table"] == "comPerson":
                # 人員自己的部門（DepartID）也要在目標存在，否則補上（排在人員前面）
                scur.execute(f"SELECT * FROM {spec['table']} WHERE {spec['where']}", spec["params"])
                prow = _one(scur)
                if prow is None:
                    if spec.get("copy_required"):
                        raise PoValidationError(f"來源公司也找不到{spec['label']} {spec['key']}，無法複製主檔")
                    continue
                dept = _text(_get(prow, "DepartID"))
                if dept and not any(k["table"] == "comDepartment" and k["key"] == dept for k in kept):
                    wcur.execute("SELECT 1 FROM comDepartment WHERE DepartID = %s", (dept,))
                    if wcur.fetchone() is None:
                        kept.append({"table": "comDepartment", "label": "部門", "key": dept,
                                     "where": "DepartID = %s", "params": (dept,),
                                     "required": False, "copy_required": False})
            elif not spec["required"]:
                scur.execute(f"SELECT 1 FROM {spec['table']} WHERE {spec['where']}", spec["params"])
                if scur.fetchone() is None:
                    continue
            kept.append(spec)
        missing = kept
    elif missing:
        # 正式公司（或來源本身）絕不複製主檔：必要主檔缺就中止，非必要的直接忽略
        required_missing = [m for m in missing if m["required"]]
        if required_missing:
            names = "、".join(f"{m['label']} {m['key']}" for m in required_missing[:5])
            raise PoValidationError(
                f"{target_label(target)} 缺少主檔（{names}），不會自動複製到此公司，請先在正航建立"
            )
        missing = []

    lines_in = []
    for line in req["lines"]:
        prod = ctx["products"][line["part_number"]]
        lines_in.append({
            **line,
            "prod_id": _text(_get(prod, "ProdID")),
            "prod_name": _text(_get(prod, "ProdName")),
        })
    lines, totals = compute_money(lines_in)

    bill_no = _offset_bill_no(next_bill_no(wcur, today, lock), bill_offset)
    main_values = build_main_row(
        ctx["main_tpl"], bill_no=bill_no, today=today, remark=req["remark"],
        salesman=req["salesman"], totals=totals,
    )
    main_insert = build_insert("ordBillMain", main_cols, main_values)
    sub_inserts = []
    for i, line in enumerate(lines, 1):
        tpl = ctx["sub_tpls"][line["part_number"]]
        sub_values = build_sub_row(tpl, bill_no=bill_no, today=today, index=i, line=line)
        sub_inserts.append(build_insert("ordBillSub", sub_cols, sub_values))

    supplier_name = _text(_get(ctx["supplier"], "ShortName")) or _text(_get(ctx["supplier"], "FullName"))
    return {
        "bill_no": bill_no,
        "supplier_id": supplier_id,
        "supplier_name": supplier_name,
        "supplier_full_name": _text(_get(ctx["supplier"], "FullName")),
        "template_bill_no": _text(_get(ctx["main_tpl"], "BillNO")),
        "lines": lines,
        "totals": totals,
        "missing_masters": missing,
        "main_insert": main_insert,
        "sub_inserts": sub_inserts,
    }


def _check_db_name(cur, target: str) -> None:
    cur.execute("SELECT DB_NAME()")
    found = cur.fetchone()
    actual = _text(found[0]) if found else ""
    if actual != target:
        raise PoValidationError(f"連線到的資料庫（{actual or '未知'}）與寫入目標（{target}）不符，已中止")


def _jsonable_lines(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for i, line in enumerate(lines, 1):
        p = line["pre_in_date"]
        out.append({
            "row_no": i,
            "part_number": line["prod_id"],
            "product_name": line["prod_name"],
            "qty": float(line["qty"]),
            "price": float(line["price"]),
            "amount": float(line["amount"]),
            "tax_amt": float(line["tax_amt"]),
            "pre_in_date": f"{p // 10000:04d}-{p // 100 % 100:02d}-{p % 100:02d}",
            "allocations": [dict(a) for a in line.get("allocations") or []],
        })
    return out


def _jsonable_totals(totals: Dict[str, Decimal]) -> Dict[str, float]:
    return {k: float(v) for k, v in totals.items()}


def _summary(plan: Dict[str, Any], target: str, req: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "target": target,
        "target_label": target_label(target),
        "is_test": is_test(target),
        "bill_no": plan["bill_no"],
        "supplier": {
            "id": plan["supplier_id"],
            "name": plan["supplier_name"],
            "full_name": plan["supplier_full_name"],
        },
        "remark": req["remark"],
        "salesman": req["salesman"],
        "template_bill_no": plan["template_bill_no"],
        "lines": _jsonable_lines(plan["lines"]),
        "totals": _jsonable_totals(plan["totals"]),
        "masters_to_copy": [{"table": m["table"], "label": m["label"], "key": m["key"]} for m in plan["missing_masters"]],
    }


# ── 對外：預覽 / 建立 ────────────────────────────────────────────────────────

def preview_po(raw_req: Dict[str, Any]) -> Dict[str, Any]:
    req = normalize_request(raw_req)
    target, creds = _require_available()
    today = date_to_int(local_now().date())
    with _source_session() as src:
        wconn = _connect_write(creds)
        try:
            plan = _plan(src, wconn.cursor(), target, req, today, lock=False)
        except ErpError:
            raise
        except Exception as exc:
            raise ErpUnavailable(f"ERP 查詢失敗：{_safe_message(exc, creds)}") from None
        finally:
            _close_quietly(wconn, rollback=True)
    return _summary(plan, target, req)


def preview_many(raw_reqs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """多張採購單一次預覽：共用一個來源連線與一個（唯讀、最後 rollback）寫入連線。

    預估單號依序遞增（第一張取下一個可用號，之後 +1）；每張的錯誤只回在該筆，不影響其他張。
    """
    if len(raw_reqs) > MAX_PREVIEW_GROUPS:
        raise PoValidationError(f"一次最多預覽 {MAX_PREVIEW_GROUPS} 張採購單")
    target, creds = _require_available()
    today = date_to_int(local_now().date())
    results: List[Dict[str, Any]] = []
    with _source_session() as src:
        wconn = _connect_write(creds)
        try:
            wcur = wconn.cursor()
            offset = 0
            for raw in raw_reqs:
                supplier_id = _text((raw or {}).get("supplier_id"))
                try:
                    req = normalize_request(raw)
                    plan = _plan(src, wcur, target, req, today, lock=False, bill_offset=offset)
                except ErpError as exc:
                    results.append({"ok": False, "supplier_id": supplier_id, "error": _safe_message(exc, creds)})
                    continue
                except Exception as exc:
                    results.append({
                        "ok": False, "supplier_id": supplier_id,
                        "error": f"ERP 查詢失敗：{_safe_message(exc, creds)}",
                    })
                    continue
                offset += 1
                summary = _summary(plan, target, req)
                results.append({
                    "ok": True,
                    "supplier_id": plan["supplier_id"],
                    "bill_no": plan["bill_no"],
                    "bill_no_estimated": True,
                    "supplier": summary["supplier"],
                    "remark": summary["remark"],
                    "lines": summary["lines"],
                    "totals": summary["totals"],
                    "missing_masters": [f"{m['label']} {m['key']}" for m in plan["missing_masters"]],
                    "masters_to_copy": summary["masters_to_copy"],
                    "target": target,
                    "target_label": summary["target_label"],
                    "is_test": summary["is_test"],
                })
        finally:
            _close_quietly(wconn, rollback=True)
    return results


def _close_quietly(conn, rollback: bool = False) -> None:
    if rollback:
        try:
            conn.rollback()
        except Exception:
            pass
    try:
        conn.close()
    except Exception:
        pass


def _copy_masters(src_cur, wcur, missing: List[Dict[str, Any]], target: str = "") -> None:
    if missing and not _can_copy_masters(target, erp_stock.get_settings()["database"]):
        raise PoValidationError(f"{target_label(target)} 不會自動複製主檔，已中止")
    for spec in missing:
        src_cur.execute(f"SELECT * FROM {spec['table']} WHERE {spec['where']}", spec["params"])
        row = _one(src_cur)
        if row is None:
            if not spec.get("required", True) and not spec.get("copy_required"):
                continue
            raise PoValidationError(f"來源公司也找不到{spec['label']} {spec['key']}，無法複製主檔")
        cols = table_columns(wcur, spec["table"])
        sql, params = build_insert(spec["table"], cols, row)
        wcur.execute(sql, params)


def _same(a: Any, b: Any) -> bool:
    try:
        return abs(Decimal(str(a)) - Decimal(str(b))) < Decimal("0.005")
    except (InvalidOperation, ValueError):
        return _text(a) == _text(b)


def _verify(wcur, plan: Dict[str, Any]) -> None:
    bill_no = plan["bill_no"]
    wcur.execute(
        "SELECT BillNO, SumQty, SumBTaxAmt, SumTax, SumAmtATax FROM ordBillMain WHERE Flag = 4 AND BillNO = %s",
        (bill_no,),
    )
    head = _one(wcur)
    t = plan["totals"]
    if head is None:
        raise PoSchemaError("寫入後讀回不到採購單表頭，已 rollback")
    for col, key in (("SumQty", "sum_qty"), ("SumBTaxAmt", "sum_amount"), ("SumTax", "sum_tax"), ("SumAmtATax", "total")):
        if not _same(_get(head, col), t[key]):
            raise PoSchemaError(f"寫入後讀回的 {col} 與預期不符，已 rollback")
    wcur.execute(
        "SELECT RowNO, ProdID, Quantity, Price, Amount, TaxAmt, PreInDate FROM ordBillSub "
        "WHERE Flag = 4 AND BillNO = %s ORDER BY RowNO",
        (bill_no,),
    )
    subs = _rows(wcur)
    if len(subs) != len(plan["lines"]):
        raise PoSchemaError("寫入後讀回的明細行數與預期不符，已 rollback")
    for i, (got, line) in enumerate(zip(subs, plan["lines"]), 1):
        same = (
            int(_get(got, "RowNO", 0)) == i
            and _text(_get(got, "ProdID")).upper() == line["prod_id"].upper()
            and _same(_get(got, "Quantity"), line["qty"])
            and _same(_get(got, "Price"), line["price"])
            and _same(_get(got, "Amount"), line["amount"])
            and _same(_get(got, "TaxAmt"), line["tax_amt"])
            and int(_get(got, "PreInDate", 0)) == line["pre_in_date"]
        )
        if not same:
            raise PoSchemaError(f"寫入後讀回的第 {i} 行明細與預期不符，已 rollback")


def _write(req: Dict[str, Any], target: str, creds: Dict[str, Any]) -> Dict[str, Any]:
    today = date_to_int(local_now().date())
    with _write_lock, _source_session() as src:
        wconn = _connect_write(creds)
        committed = False
        try:
            wcur = wconn.cursor()
            plan = _plan(src, wcur, target, req, today, lock=True)
            _copy_masters(src.cursor(), wcur, plan["missing_masters"], target)
            wcur.execute(*plan["main_insert"])
            for insert in plan["sub_inserts"]:
                wcur.execute(*insert)
            _verify(wcur, plan)
            _check_db_name(wcur, target)
            try:
                wconn.commit()
                committed = True
            except Exception as exc:
                raise ErpUnavailable(
                    f"commit 失敗，結果不確定，請到正航確認是否已有單號 {plan['bill_no']}：{_safe_message(exc, creds)}"
                ) from None
        except ErpError:
            raise
        except Exception as exc:
            raise ErpUnavailable(f"寫入正航失敗，已 rollback：{_safe_message(exc, creds)}") from None
        finally:
            _close_quietly(wconn, rollback=not committed)
    return _summary(plan, target, req)


def create_po(raw_req: Dict[str, Any], actor: str = "web", client_ip: str = "") -> Dict[str, Any]:
    """建立採購單並記錄本機紀錄（成功 / 失敗嘗試都記）。需 confirm=True。"""
    from .. import database as db

    if raw_req.get("confirm") is not True:
        raise PoValidationError("需確認後才會寫入正航（confirm=true）")
    req = normalize_request(raw_req)
    target, creds = _require_available()
    from . import po_duplicates

    fp = po_duplicates.fingerprint(target, req)
    recent = po_duplicates.recent_identical(target, req)
    if recent:
        raise DuplicateSubmitError(
            f"相同內容的採購單 {recent['bill_no']} 在 {recent['seconds_ago']} 秒前已建立，已略過重複送出"
        )
    if raw_req.get("allow_duplicate") is not True:
        dups = po_duplicates.find_duplicates(req, target)
        if dups:
            raise DuplicateWarning(dups)
    if not po_duplicates.claim(fp):
        raise DuplicateSubmitError("相同內容的採購單正在建立中，請勿重複送出")
    try:
        return _create_checked(req, target, creds, actor, client_ip)
    finally:
        po_duplicates.release(fp)


def _create_checked(req: Dict[str, Any], target: str, creds: Dict[str, Any], actor: str, client_ip: str) -> Dict[str, Any]:
    from .. import database as db

    base_log = {
        "target_db": target,
        "supplier_id": req["supplier_id"],
        "remark": req["remark"],
        "actor": actor,
        "client_ip": client_ip,
        "lines_json": json.dumps(
            [{"part_number": l["part_number"], "qty": str(l["qty"]), "price": str(l["price"]),
              "pre_in_date": l["pre_in_date"]} for l in req["lines"]],
            ensure_ascii=False,
        ),
    }
    try:
        result = _write(req, target, creds)
    except Exception as exc:
        message = _safe_message(exc, creds)
        _safe_log(db, {**base_log, "status": "error", "error": message})
        db.log_activity("erp_po_failed", f"{target_label(target)} 供應商 {req['supplier_id']}：{message}")
        raise
    totals = result["totals"]
    po_id = _safe_log(db, {
        **base_log,
        "bill_no": result["bill_no"],
        "supplier_name": result["supplier"]["name"],
        "lines_json": json.dumps(result["lines"], ensure_ascii=False),
        "sum_qty": totals["sum_qty"],
        "sum_amount": totals["sum_amount"],
        "sum_tax": totals["sum_tax"],
        "total": totals["total"],
        "status": "ok",
    })
    _safe_save_allocations(db, po_id, result, req["alloc_source"])
    db.log_activity(
        "erp_po_created",
        f"{target_label(target)} 採購單 {result['bill_no']}，供應商 {result['supplier']['id']}，"
        f"{len(result['lines'])} 行，含稅合計 {totals['total']:.0f}",
    )
    return {"ok": True, **result}


def _safe_log(db, rec: Dict[str, Any]) -> Optional[int]:
    try:
        return db.add_erp_purchase_order(rec)
    except Exception:
        return None  # 本機紀錄失敗不可蓋掉已完成的 ERP 結果


def _safe_save_allocations(db, po_id: Optional[int], result: Dict[str, Any], source: str) -> None:
    """成功寫入 ERP 後，把每行「為哪些訂單而買」存入本機（僅記錄）；失敗不影響 ERP 結果。"""
    if not po_id:
        return
    rows = [
        {**a, "row_no": line["row_no"], "part_number": line["part_number"], "source": source}
        for line in result["lines"] for a in line.get("allocations") or []
    ]
    try:
        db.save_po_line_allocations(po_id, result["bill_no"], result["target"], rows)
    except Exception:
        pass


def list_history(limit: int = 50) -> List[Dict[str, Any]]:
    """採購單本機紀錄，成功的單附上 allocations（扁平清單，含 row_no / part_number）；舊單為空清單。"""
    from .. import database as db

    orders = db.list_erp_purchase_orders(limit)
    by_po: Dict[int, List[Dict[str, Any]]] = {}
    for a in db.get_po_line_allocations([o["id"] for o in orders]):
        by_po.setdefault(int(a["erp_po_id"]), []).append(a)
    return [{**o, "allocations": by_po.get(int(o["id"]), [])} for o in orders]


# ── 查詢（來源公司，唯讀）────────────────────────────────────────────────────

def _like_escape(text: str) -> str:
    return text.replace("[", "[[]").replace("%", "[%]").replace("_", "[_]")


def search_suppliers(q: str) -> List[Dict[str, str]]:
    pattern = f"%{_like_escape(q.strip())}%"
    with _source_session() as src:
        cur = src.cursor()
        cur.execute(
            "SELECT TOP 30 ID, ShortName, FullName FROM comCustomer "
            "WHERE Flag = 2 AND (ID LIKE %s OR ShortName LIKE %s OR FullName LIKE %s) ORDER BY ID",
            (pattern, pattern, pattern),
        )
        return [
            {"id": _text(r[0]), "short_name": _text(r[1]), "full_name": _text(r[2])}
            for r in cur.fetchall()
        ]


def _suggested_qty(part: str) -> Optional[int]:
    """低庫存關注清單內的料號：ceil(max(門檻 - 現有量 - 在途量, 0))（未計 MOQ）；不在清單或無門檻回 None。"""
    from . import low_stock

    try:
        key = low_stock.norm_part(part)
        for row in low_stock.list_rows(None):
            if low_stock.norm_part(row["part_number"]) == key:
                if row.get("threshold") is None:
                    return None
                gap = float(row["threshold"]) - float(row["qty"]) - float(row.get("on_order_qty") or 0)
                return int(math.ceil(max(gap, 0)))
    except Exception:
        return None
    return None


def part_defaults(part: str) -> Dict[str, Any]:
    key = part.strip().upper()
    result: Dict[str, Any] = {
        "part_number": key, "found": False, "product_name": "",
        "supplier_id": "", "supplier_name": "", "last_price": None,
        "last_bill_no": "", "last_bill_date": "", "suggested_qty": None,
        "need_qty": None, "moq": 0, "moq_source": "", "moq_adjusted": False,
    }
    if not key:
        return result
    with _source_session() as src:
        cur = src.cursor()
        cur.execute("SELECT ProdID, ProdName FROM comProduct WHERE ProdID = %s", (key,))
        found = cur.fetchone()
        if found:
            result.update(found=True, part_number=_text(found[0]), product_name=_text(found[1]))
        cur.execute(
            "SELECT TOP 1 m.CustomerID, s.Price, m.BillNO, m.BillDate FROM ordBillSub s "
            "JOIN ordBillMain m ON m.Flag = s.Flag AND m.BillNO = s.BillNO "
            "WHERE s.Flag = 4 AND s.ProdID = %s ORDER BY m.BillDate DESC, m.BillNO DESC",
            (key,),
        )
        last = cur.fetchone()
        if last:
            supplier_id = _text(last[0])
            bill_date = int(last[3] or 0)
            result.update(
                supplier_id=supplier_id,
                last_price=float(last[1] or 0),
                last_bill_no=_text(last[2]),
                last_bill_date=(
                    f"{bill_date // 10000:04d}-{bill_date // 100 % 100:02d}-{bill_date % 100:02d}" if bill_date else ""
                ),
            )
            cur.execute("SELECT ShortName, FullName FROM comCustomer WHERE Flag = 2 AND ID = %s", (supplier_id,))
            name = cur.fetchone()
            if name:
                result["supplier_name"] = _text(name[0]) or _text(name[1])
    # 建議量 = max(缺口, MOQ)；MOQ 本站優先、正航 MinPurch 補缺；使用者仍可手動改數量
    from . import moq as moq_service

    need = _suggested_qty(key)
    if need is not None:
        need = max(1, need)  # 與低庫存頁相同：缺口至少 1，再與 MOQ 取大
    info = moq_service.lookup_moq([key]).get(key)
    result.update(moq_service.moq_fields(need, info))
    if need is None:
        result["need_qty"] = result["suggested_qty"] = None
        result["moq_adjusted"] = False
    return result
