"""常用料號低庫存提醒。

從正航 ERP 讀 ST 倉的領用紀錄與採購交期，建議「常用料號」與低庫存門檻，
並維護 low_stock_watch 關注清單。警示本身只讀本機 st_inventory_snapshot，不連 ERP。
重點廠商的料另由 refresh_priority_parts 算入 015（庚霖倉）庫存與領用，存 low_stock_priority。

ERP 全程唯讀 SELECT（%s 參數化）；任何 ERP 失敗都在寫入前丟出 ErpError，不改任何本機資料。
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import unicodedata
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from .. import database as db
from . import erp_stock, main_reader
from .erp_stock import ErpError, ErpUnavailable
from .local_time import local_now

logger = logging.getLogger("low_stock")

SETTING_PREFIX = "low_stock_"
LAST_SUGGESTED_KEY = "low_stock_last_suggested_at"
PRIORITY_SUPPLIERS_KEY = "low_stock_priority_suppliers"
DEFAULT_PRIORITY_SUPPLIERS: List[str] = ["萬晉", "崧嵐"]
PRIORITY_SUPPLIER_MAX = 30
PRIORITY_REFRESHED_KEY = "low_stock_priority_refreshed_at"
# 只有重點廠商列才有的欄位（非重點列與 /alerts 的非重點項目不帶這些鍵）
PRIORITY_VIEW_KEYS = (
    "st_qty", "gl_qty", "gl_ware_id", "st_usage_qty", "gl_usage_qty",
    "in_watch", "priority_extra", "eta_short", "eta_gap_days",
)
_PRIORITY_SPLIT_RE = re.compile(r"[,，、;；\n\r]+")
IN_CHUNK = 500

DEFAULT_CONFIG: Dict[str, float] = {
    "lookback_days": 365,
    "suggest_lookback_days": 90,
    "min_bills": 3,
    "safety_factor": 1.5,
    "min_cover_days": 7,
    "default_lead_days": 7,
    "lead_lookback_days": 1095,
    "lead_sample_size": 5,
    "actual_lead_ratio": 0.6,
    "actual_lead_min_gap_days": 7,
    "on_order_max_overdue_days": 90,
}
# (最小, 最大, 是否整數)
CONFIG_RULES: Dict[str, Tuple[float, float, bool]] = {
    "lookback_days": (7, 730, True),
    "suggest_lookback_days": (7, 730, True),
    "min_bills": (1, 50, True),
    "safety_factor": (0.1, 10, False),
    "min_cover_days": (0, 365, False),
    "default_lead_days": (1, 365, False),
    "lead_lookback_days": (30, 3650, True),
    "lead_sample_size": (1, 50, True),
    "actual_lead_ratio": (0, 1, False),
    "actual_lead_min_gap_days": (0, 365, False),
    "on_order_max_overdue_days": (0, 3650, True),
}
CONFIG_LABELS = {
    "lookback_days": "用量統計天數",
    "suggest_lookback_days": "建議清單統計天數",
    "min_bills": "最少單據數",
    "safety_factor": "安全係數",
    "min_cover_days": "最低可撐天數",
    "default_lead_days": "預設交期",
    "lead_lookback_days": "交期統計回溯天數",
    "lead_sample_size": "交期取樣筆數",
    "actual_lead_ratio": "實際交期比例上限",
    "actual_lead_min_gap_days": "實際交期最少縮短天數",
    "on_order_max_overdue_days": "在途逾期排除天數",
}

LEAD_LABELS = {
    "planned": "預定交期",
    "planned_delay": "預定+延遲",
    "actual": "實際交期",
    "actual_long_planned": "實際交期（預定過長）",
    "default": "預設",
    "manual": "手動",
}

ON_ORDER_NOTE_FAILED = "在途量查詢失敗，暫以 0 計"

_UNSET = object()


class LowStockError(ValueError):
    """操作錯誤；訊息為繁體中文。"""


class LowStockNotFound(LowStockError):
    pass


class LowStockExists(LowStockError):
    pass


# ── 純公式（不碰 DB / ERP） ────────────────────────────────────────────────

def median_int(values: Iterable[float]) -> Optional[int]:
    """中位數，四捨五入為整數（.5 進位）；空 → None。"""
    items = sorted(float(v) for v in values)
    if not items:
        return None
    mid = len(items) // 2
    med = items[mid] if len(items) % 2 else (items[mid - 1] + items[mid]) / 2.0
    return int(math.floor(med + 0.5))


def calc_avg_daily_usage(net_usage: float, lookback_days: float) -> float:
    if lookback_days <= 0:
        return 0.0
    return max(float(net_usage or 0), 0.0) / float(lookback_days)


def calc_lead_time(
    planned_days: Optional[float],
    delay_days: Optional[float],
    actual_days: Optional[float],
    default_days: float,
    actual_ratio: float = DEFAULT_CONFIG["actual_lead_ratio"],
    actual_min_gap_days: float = DEFAULT_CONFIG["actual_lead_min_gap_days"],
) -> Tuple[float, str]:
    """回傳 (交期天數, 來源 planned/actual/default)。延遲只加不減。

    預定與實際都有，且實際 <= 預定 × 比例上限、並比預定短至少 N 天（預定明顯過長）時，改採實際交期。
    """
    if (
        planned_days is not None and actual_days is not None
        and float(actual_days) <= float(planned_days) * float(actual_ratio) + 1e-9
        and float(planned_days) - float(actual_days) >= float(actual_min_gap_days)
    ):
        return float(actual_days), "actual"
    if planned_days is not None:
        return float(planned_days) + max(0.0, float(delay_days or 0)), "planned"
    if actual_days is not None:
        return float(actual_days), "actual"
    return float(default_days), "default"


def lead_label(source: str, delay_days: Optional[float] = None, planned_days: Optional[float] = None) -> str:
    if source == "actual" and planned_days is not None:
        return LEAD_LABELS["actual_long_planned"]
    if source == "planned":
        return LEAD_LABELS["planned_delay"] if (delay_days or 0) > 0 else LEAD_LABELS["planned"]
    return LEAD_LABELS.get(source, LEAD_LABELS["default"])


def calc_threshold(
    avg_daily_usage: Optional[float],
    lead_time_days: float,
    safety_factor: float,
    min_cover_days: float,
) -> Optional[int]:
    """建議門檻 = ceil(max(日均×交期×安全係數, 日均×最低可撐天數))；沒有用量資料 → None。"""
    if avg_daily_usage is None:
        return None
    raw = max(
        avg_daily_usage * lead_time_days * safety_factor,
        avg_daily_usage * min_cover_days,
    )
    return int(math.ceil(round(raw, 6)))


def calc_days_of_cover(qty: float, avg_daily_usage: Optional[float]) -> Optional[float]:
    if not avg_daily_usage or avg_daily_usage <= 0:
        return None
    return round(float(qty) / float(avg_daily_usage), 1)


def calc_status(enabled: bool, qty: float, threshold: Optional[float], on_order_qty: float = 0.0) -> str:
    """low = 低於門檻；on_order = 低於門檻但現有量 + 在途量已達門檻（已下單，不算低庫存）。"""
    if not enabled:
        return "disabled"
    if threshold is not None and qty < threshold:
        if qty + max(float(on_order_qty or 0), 0.0) >= threshold:
            return "on_order"
        return "low"
    return "ok"


def norm_part(part: Any) -> str:
    return str(part or "").strip().upper()


# ── 主檔料號範圍 ──────────────────────────────────────────────────────────

_MAIN_PARTS_CACHE: Dict[str, Any] = {"key": None, "parts": None}
MAIN_FILE_NOTE_FALLBACK = "讀不到主檔，暫時顯示全部料號"


def main_file_parts() -> Optional[set]:
    """目前主檔內的料號集合（strip/upper）；以 (路徑, mtime) 快取。無路徑/檔案不存在/讀取失敗 → None。"""
    try:
        path = db.get_setting("main_file_path", "")
        if not path:
            return None
        key = (path, os.stat(path).st_mtime_ns)
        if _MAIN_PARTS_CACHE["key"] == key:
            return _MAIN_PARTS_CACHE["parts"]
        parts = {norm_part(p) for p in main_reader.read_stock(path).keys()}
        parts.discard("")
        _MAIN_PARTS_CACHE.update(key=key, parts=parts)
        return parts
    except Exception as exc:
        logger.info("讀不到主檔料號：%s", type(exc).__name__)
        return None


def main_file_status(parts: Optional[set]) -> dict:
    """回應用欄位：是否已套用主檔過濾，以及未套用時的中文說明。"""
    applied = parts is not None
    return {"main_file_filter_applied": applied, "main_file_note": "" if applied else MAIN_FILE_NOTE_FALLBACK}


# ── 設定 ──────────────────────────────────────────────────────────────────

def _coerce(key: str, value: Any) -> float:
    lo, hi, is_int = CONFIG_RULES[key]
    label = CONFIG_LABELS[key]
    if isinstance(value, bool):
        raise LowStockError(f"{label}格式不正確")
    try:
        num = float(value)
    except (TypeError, ValueError):
        raise LowStockError(f"{label}格式不正確") from None
    if math.isnan(num) or math.isinf(num) or not (lo <= num <= hi):
        raise LowStockError(f"{label}需介於 {lo:g} 到 {hi:g}")
    if is_int:
        if num != int(num):
            raise LowStockError(f"{label}必須是整數")
        return int(num)
    return num


def _default_for(key: str) -> float:
    """預設值；用量統計天數可用環境變數 LOW_STOCK_LOOKBACK_DAYS 覆蓋（設定表的值優先，格式不符則忽略）。"""
    if key == "lookback_days":
        env = os.environ.get("LOW_STOCK_LOOKBACK_DAYS", "").strip()
        if env:
            try:
                return _coerce(key, env)
            except LowStockError:
                pass
    return DEFAULT_CONFIG[key]


def get_config() -> Dict[str, float]:
    cfg: Dict[str, float] = {}
    for key in DEFAULT_CONFIG:
        default = _default_for(key)
        raw = db.get_setting(SETTING_PREFIX + key, "")
        try:
            cfg[key] = _coerce(key, raw) if str(raw).strip() != "" else default
        except LowStockError:
            cfg[key] = default
    return cfg


def validate_config(values: Dict[str, Any]) -> Dict[str, float]:
    unknown = [k for k in values if k not in CONFIG_RULES]
    if unknown:
        raise LowStockError(f"未知的設定項目：{', '.join(unknown)}")
    return {k: _coerce(k, v) for k, v in values.items()}


def save_config(values: Dict[str, Any], actor: str = "web") -> Dict[str, float]:
    """驗證並儲存設定，接著用已存的統計重算自動門檻（不連 ERP）。"""
    clean = validate_config(values)
    for key, val in clean.items():
        db.set_setting(SETTING_PREFIX + key, str(val))
    recompute_all(actor=actor)
    return get_config()


# ── 重點廠商（低庫存提醒特別標註；比對 last_purchase_supplier） ──────────────

def _norm_supplier_text(value: Any) -> str:
    """全半形統一（NFKC）、不分大小寫、去掉所有空白。"""
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return "".join(text.split())


def parse_priority_suppliers(value: Any) -> List[str]:
    """接受 list 或逗號 / 頓號分隔字串；去空白、去重（依正規化後比對），保留原順序。"""
    items = value if isinstance(value, (list, tuple)) else [value]
    out: List[str] = []
    seen = set()
    for item in items:
        for part in _PRIORITY_SPLIT_RE.split(str(item or "")):
            word = part.strip()
            key = _norm_supplier_text(word)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(word)
    if len(out) > PRIORITY_SUPPLIER_MAX:
        raise LowStockError(f"重點廠商最多 {PRIORITY_SUPPLIER_MAX} 個")
    return out


def get_priority_suppliers() -> List[str]:
    """未設定（或內容壞掉）時用預設名單；存過空清單則為空（不標註）。"""
    raw = db.get_setting(PRIORITY_SUPPLIERS_KEY, "")
    if str(raw).strip() == "":
        return list(DEFAULT_PRIORITY_SUPPLIERS)
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            raise ValueError("not a list")
        return parse_priority_suppliers(data)
    except ValueError:  # LowStockError 也是 ValueError
        return list(DEFAULT_PRIORITY_SUPPLIERS)


def save_priority_suppliers(value: Any, actor: str = "web") -> List[str]:
    clean = parse_priority_suppliers(value)
    db.set_setting(PRIORITY_SUPPLIERS_KEY, json.dumps(clean, ensure_ascii=False))
    db.log_activity("低庫存提醒重點廠商", f"{actor}：{'、'.join(clean) or '（清空）'}")
    return clean


def match_priority_supplier(supplier: Any, keywords: Iterable[str]) -> str:
    """回傳第一個被廠商名稱包含的關鍵字；都沒命中回傳空字串。"""
    name = _norm_supplier_text(supplier)
    if not name:
        return ""
    for word in keywords:
        key = _norm_supplier_text(word)
        if key and key in name:
            return word
    return ""


# ── DB 存取 ───────────────────────────────────────────────────────────────

def _now_text() -> str:
    return local_now().strftime("%Y-%m-%d %H:%M:%S")


def _get_row(conn, part: str) -> Optional[dict]:
    row = conn.execute("SELECT * FROM low_stock_watch WHERE part_number=?", (part,)).fetchone()
    return dict(row) if row else None


_ROW_COLUMNS = (
    "name", "source", "enabled", "dismissed", "note", "threshold_qty", "threshold_source",
    "lead_time_days", "lead_time_source", "planned_lead_days", "actual_lead_days", "delay_days",
    "delay_samples", "avg_daily_usage", "usage_qty", "bill_count", "last_purchase_date",
    "last_purchase_supplier", "suggested_at", "updated_at", "updated_by",
)


def _save_row(conn, part: str, row: dict) -> None:
    values = [row.get(col) for col in _ROW_COLUMNS]
    conn.execute(
        "INSERT INTO low_stock_watch(part_number, " + ", ".join(_ROW_COLUMNS) + ") VALUES("
        + ",".join("?" * (len(_ROW_COLUMNS) + 1)) + ") "
        "ON CONFLICT(part_number) DO UPDATE SET " + ", ".join(f"{c}=excluded.{c}" for c in _ROW_COLUMNS),
        [part, *values],
    )


def _new_row(source: str, actor: str) -> dict:
    return {
        "name": "", "source": source, "enabled": 1, "dismissed": 0, "note": "",
        "threshold_qty": None, "threshold_source": "auto",
        "lead_time_days": None, "lead_time_source": "default",
        "planned_lead_days": None, "actual_lead_days": None, "delay_days": None, "delay_samples": 0,
        "avg_daily_usage": None, "usage_qty": None, "bill_count": None,
        "last_purchase_date": "", "last_purchase_supplier": "",
        "suggested_at": "", "updated_at": _now_text(), "updated_by": actor,
    }


def derive_auto_fields(row: dict, cfg: Dict[str, float]) -> dict:
    """依已存統計，重算非手動的交期與門檻（就地修改並回傳）。"""
    if row.get("lead_time_source") != "manual":
        days, source = calc_lead_time(
            row.get("planned_lead_days"), row.get("delay_days"),
            row.get("actual_lead_days"), cfg["default_lead_days"],
            cfg["actual_lead_ratio"], cfg["actual_lead_min_gap_days"],
        )
        row["lead_time_days"] = days
        row["lead_time_source"] = source
    if row.get("threshold_source") != "manual":
        lead = row.get("lead_time_days")
        if lead is None:
            lead = cfg["default_lead_days"]
        row["threshold_qty"] = calc_threshold(
            row.get("avg_daily_usage"), float(lead), cfg["safety_factor"], cfg["min_cover_days"]
        )
    return row


def recompute_all(actor: str = "system") -> int:
    cfg = get_config()
    db.ensure_low_stock_table()
    count = 0
    with db.get_conn() as conn:
        for rec in conn.execute("SELECT * FROM low_stock_watch").fetchall():
            row = derive_auto_fields(dict(rec), cfg)
            conn.execute(
                "UPDATE low_stock_watch SET lead_time_days=?, lead_time_source=?, threshold_qty=? "
                "WHERE part_number=?",
                (row["lead_time_days"], row["lead_time_source"], row["threshold_qty"], rec["part_number"]),
            )
            count += 1
    return count


# ── ERP 查詢（唯讀） ──────────────────────────────────────────────────────

def _yyyymmdd(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def _to_date(value: Any) -> Optional[date]:
    try:
        n = int(value)
        return date(n // 10000, n // 100 % 100, n % 100)
    except (TypeError, ValueError):
        return None


def _day_diff(later: Any, earlier: Any) -> Optional[int]:
    a, b = _to_date(later), _to_date(earlier)
    if not a or not b:
        return None
    return (a - b).days


def _chunks(items: List[str], size: int = IN_CHUNK) -> Iterator[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _in_clause(n: int) -> str:
    return "(" + ",".join(["%s"] * n) + ")"


@contextmanager
def _erp_session() -> Iterator[Any]:
    """開啟唯讀 ERP 連線；所有非 ErpError 例外一律轉成不含密碼的 ErpUnavailable。"""
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


def _query_usage(conn, ware_id: str, start: date, end: date, parts: Optional[List[str]] = None) -> Dict[str, Tuple[float, int]]:
    """ST 倉淨領用量與單據數：轉出至委外倉(400) + 銷貨出庫(500/501)。負數為沖銷，須帶正負號加總。"""
    base = (
        "SELECT ProdID, SUM(Quantity), COUNT(DISTINCT BillNO) FROM comProdRec "
        "WHERE BillDate >= %s AND BillDate <= %s "
        "AND ((Flag = 400 AND WareOut = %s) OR (Flag IN (500, 501) AND WareID = %s))"
    )
    head = (_yyyymmdd(start), _yyyymmdd(end), ware_id, ware_id)
    result: Dict[str, Tuple[float, int]] = {}
    cur = conn.cursor()
    batches: List[Optional[List[str]]] = list(_chunks(parts)) if parts is not None else [None]
    for batch in batches:
        if batch is None:
            cur.execute(base + " GROUP BY ProdID", head)
        else:
            cur.execute(base + " AND ProdID IN " + _in_clause(len(batch)) + " GROUP BY ProdID", head + tuple(batch))
        for prod_id, qty, bills in cur.fetchall():
            part = norm_part(prod_id)
            if part:
                old_qty, old_bills = result.get(part, (0.0, 0))
                result[part] = (old_qty + float(qty or 0), old_bills + int(bills or 0))
    return result


def _recent(entries: List[tuple], n: int) -> List[tuple]:
    """entries 第一欄為 PO 日期、第二欄為 PO 單號/列號排序鍵；取最近 n 筆。"""
    return sorted(entries, key=lambda e: (e[0], e[1]), reverse=True)[:n]


def _query_part_info(
    conn, parts: List[str], ware_id: str, today: date, cfg: Dict[str, float]
) -> Dict[str, dict]:
    """逐料號的品名、預定/實際交期、延遲、最近進貨與廠商。只查傳入的料號清單。"""
    lead_start = _yyyymmdd(today - timedelta(days=int(cfg["lead_lookback_days"])))
    sample_n = int(cfg["lead_sample_size"])
    info: Dict[str, dict] = {p: {} for p in parts}
    cur = conn.cursor()

    for batch in _chunks(parts):
        marks = _in_clause(len(batch))

        # 品名
        cur.execute(f"SELECT ProdID, ProdName FROM comProduct WHERE ProdID IN {marks}", tuple(batch))
        for prod_id, name in cur.fetchall():
            part = norm_part(prod_id)
            if part in info and name:
                info[part]["name"] = str(name).strip()

        # 預定交期：PO 明細 預交日 - 訂購日
        cur.execute(
            "SELECT ProdID, BillDate, BillNO, RowNO, PreInDate FROM ordBillSub "
            f"WHERE Flag = 4 AND BillDate >= %s AND ProdID IN {marks}",
            (lead_start, *batch),
        )
        planned: Dict[str, List[tuple]] = {}
        for prod_id, bill_date, bill_no, row_no, pre_in in cur.fetchall():
            part = norm_part(prod_id)
            if part not in info or not pre_in:
                continue
            days = _day_diff(pre_in, bill_date)
            if days is None or days < 0:
                continue
            planned.setdefault(part, []).append((int(bill_date), f"{bill_no}-{row_no}", days))
        for part, entries in planned.items():
            info[part]["planned_lead_days"] = median_int(e[2] for e in _recent(entries, sample_n))

        # 實際交期與延遲：收貨日 - 訂購日 / 收貨日 - 預交日（經 QM 驗收、IM 到貨回溯 PO 明細）
        cur.execute(
            "SELECT s.ProdID, s.BillDate, s.BillNO, s.RowNO, s.PreInDate, r.BillDate "
            "FROM comProdRec r "
            "JOIN ordExamineSub x ON x.ExamineNO = r.FromNO AND x.RowNO = r.FromRow "
            "JOIN ordExamineSub m ON m.ExamineNO = x.FromNO AND m.RowNO = x.FromRow "
            "JOIN ordBillSub s ON s.Flag = 4 AND s.BillNO = m.FromNO AND s.RowNO = m.FromRow "
            f"WHERE r.Flag = 100 AND r.WareID = %s AND s.BillDate >= %s AND s.ProdID IN {marks}",
            (ware_id, lead_start, *batch),
        )
        received: Dict[str, Dict[str, tuple]] = {}
        for prod_id, po_date, bill_no, row_no, pre_in, rec_date in cur.fetchall():
            part = norm_part(prod_id)
            if part not in info:
                continue
            actual = _day_diff(rec_date, po_date)
            if actual is None or actual < 0:
                continue
            delay = _day_diff(rec_date, pre_in) if pre_in else None
            key = f"{bill_no}-{row_no}"
            entry = (int(po_date), key, actual, delay, int(rec_date))
            old = received.setdefault(part, {}).get(key)
            if old is None or entry[4] < old[4]:  # 分批收貨以首次收貨日計
                received[part][key] = entry
        for part, by_line in received.items():
            recent = _recent(list(by_line.values()), sample_n)
            info[part]["actual_lead_days"] = median_int(e[2] for e in recent)
            delays = [e[3] for e in recent if e[3] is not None]
            info[part]["delay_days"] = median_int(delays)
            info[part]["delay_samples"] = len(delays)

        # 最近一次進貨
        cur.execute(
            "SELECT ProdID, BillDate, BillNO FROM ("
            "SELECT ProdID, BillDate, BillNO, ROW_NUMBER() OVER ("
            "PARTITION BY ProdID ORDER BY BillDate DESC, BillNO DESC) AS rn "
            f"FROM comProdRec WHERE Flag = 100 AND WareID = %s AND ProdID IN {marks}) t WHERE rn = 1",
            (ware_id, *batch),
        )
        last_bill: Dict[str, str] = {}
        for prod_id, bill_date, bill_no in cur.fetchall():
            part = norm_part(prod_id)
            d = _to_date(bill_date)
            if part in info and d:
                info[part]["last_purchase_date"] = d.isoformat()
                last_bill[part] = str(bill_no)

        # 廠商：進貨單 → 應付帳款 → 廠商簡稱
        bills = sorted(set(last_bill.values()))
        supplier_by_bill: Dict[str, str] = {}
        for bill_batch in _chunks(bills):
            cur.execute(
                "SELECT a.FundBillNo, a.CustID, c.ShortName FROM comBillAccounts a "
                "LEFT JOIN comCustomer c ON c.ID = a.CustID "
                f"WHERE a.Flag = 100 AND a.FundBillNo IN {_in_clause(len(bill_batch))}",
                tuple(bill_batch),
            )
            for fund_no, cust_id, short in cur.fetchall():
                supplier_by_bill.setdefault(str(fund_no), str(short or cust_id or "").strip())
        for part, bill in last_bill.items():
            if supplier_by_bill.get(bill):
                info[part]["last_purchase_supplier"] = supplier_by_bill[bill]
    return info


def _query_on_order(
    conn, parts: List[str], today: Optional[date] = None, max_overdue_days: Optional[float] = None
) -> Dict[str, Tuple[float, str]]:
    """在途量（已下採購單未進貨）：{料號: (未結量合計, 最早預交日 YYYY-MM-DD 或 "")}。

    只取表頭未結（BillStatus=0 且 EndacaseDate=0）、明細 QtyRemain>0 的列，
    PreInDate 早於「今天 - 逾期排除天數」（含 0）的視為舊髒資料不計。
    每列未結量 = 訂購量 - 已進貨量（comProdRec → QM → IM → 採購明細，不限倉）；> 0 才計入。
    """
    if today is None:
        today = local_now().date()
    if max_overdue_days is None:
        max_overdue_days = get_config()["on_order_max_overdue_days"]
    cutoff = _yyyymmdd(today - timedelta(days=int(max_overdue_days)))
    result: Dict[str, Tuple[float, str]] = {}
    earliest: Dict[str, int] = {}
    cur = conn.cursor()
    for batch in _chunks(sorted({norm_part(p) for p in parts if norm_part(p)})):
        marks = _in_clause(len(batch))
        cur.execute(
            "SELECT c.ProdID, c.BillNO, c.RowNO, c.Quantity - COALESCE(rcv.recv_qty, 0), c.PreInDate FROM ("
            "SELECT s.ProdID, s.BillNO, s.RowNO, s.Quantity, s.PreInDate FROM ordBillSub s "
            "JOIN ordBillMain h ON h.Flag = 4 AND h.BillNO = s.BillNO "
            "WHERE s.Flag = 4 AND h.BillStatus = 0 AND h.EndacaseDate = 0 "
            f"AND s.QtyRemain > 0 AND s.PreInDate >= %s AND s.ProdID IN {marks}) c "
            "LEFT JOIN ("
            "SELECT m.FromNO AS BillNO, m.FromRow AS RowNO, SUM(r.Quantity) AS recv_qty "
            "FROM comProdRec r "
            "JOIN ordExamineSub x ON x.ExamineNO = r.FromNO AND x.RowNO = r.FromRow "
            "JOIN ordExamineSub m ON m.ExamineNO = x.FromNO AND m.RowNO = x.FromRow "
            f"WHERE r.Flag = 100 AND r.ProdID IN {marks} "
            "GROUP BY m.FromNO, m.FromRow) rcv ON rcv.BillNO = c.BillNO AND rcv.RowNO = c.RowNO",
            (cutoff, *batch, *batch),
        )
        for prod_id, _bill_no, _row_no, open_qty, pre_in in cur.fetchall():
            part = norm_part(prod_id)
            open_qty = float(open_qty or 0)
            if not part or open_qty <= 0:
                continue
            qty, date_text = result.get(part, (0.0, ""))
            result[part] = (qty + open_qty, date_text)
            d = _to_date(pre_in)
            if d and (part not in earliest or _yyyymmdd(d) < earliest[part]):
                earliest[part] = _yyyymmdd(d)
                result[part] = (result[part][0], d.isoformat())
    return result


def _fetch_on_order(erp, parts: List[str], today: date, cfg: Dict[str, float]) -> Optional[Dict[str, Tuple[float, str]]]:
    """查在途量；任何失敗只記 warning 並回 None（呼叫端視為 0 並寫入備註）。"""
    if not parts:
        return {}
    try:
        return _query_on_order(erp, parts, today, cfg["on_order_max_overdue_days"])
    except Exception as exc:
        logger.warning("在途量查詢失敗：%s", type(exc).__name__)
        return None


def _write_on_order(conn, part: str, on_order: Optional[Dict[str, Tuple[float, str]]], now: str) -> None:
    """on_order 為 None 代表查詢失敗：在途量歸 0 並留備註。"""
    if on_order is None:
        qty, date_text, note = 0.0, "", ON_ORDER_NOTE_FAILED
    else:
        qty, date_text = on_order.get(part, (0.0, ""))
        note = ""
    conn.execute(
        "UPDATE low_stock_watch SET on_order_qty=?, next_expected_date=?, on_order_synced_at=?, on_order_note=? "
        "WHERE part_number=?",
        (qty, date_text, now, note, part),
    )


def refresh_on_order() -> int:
    """只刷新關注清單的在途量（ST 庫存 ERP 同步後呼叫）。失敗一律吞掉並把在途量記為 0；回傳更新列數。"""
    try:
        db.ensure_low_stock_table()
        cfg = get_config()
        today = local_now().date()
        with db.get_conn() as conn:
            parts = [r["part_number"] for r in conn.execute(
                "SELECT part_number FROM low_stock_watch WHERE dismissed=0").fetchall()]
        if not parts:
            return 0
        try:
            with _erp_session() as erp:
                on_order = _fetch_on_order(erp, parts, today, cfg)
        except Exception as exc:
            logger.warning("在途量查詢失敗：%s", type(exc).__name__)
            on_order = None
        now = _now_text()
        with db.get_conn() as conn:
            for part in parts:
                _write_on_order(conn, part, on_order, now)
        return len(parts)
    except Exception:
        logger.warning("刷新在途量失敗", exc_info=True)
        return 0


# ── 重點廠商料號（含 015 庫存與領用；ERP 唯讀，結果存 low_stock_priority） ─────

def _gl_ware_id() -> str:
    """庚霖倉代號，沿用 st_pending_transfer.ware_ids()（環境變數 SHORTAGE_GL_WARE_ID，預設 015）。"""
    from . import st_pending_transfer  # 延遲匯入：st_pending_transfer 也匯入本模組
    return st_pending_transfer.ware_ids()[1]


def _query_priority_suppliers(conn, keywords: List[str]) -> Dict[str, Tuple[str, str]]:
    """正航廠商（comCustomer Flag=2）簡稱或全名含名單關鍵字者：{廠商 ID: (簡稱, 全名)}。"""
    cur = conn.cursor()
    cur.execute("SELECT ID, ShortName, FullName FROM comCustomer WHERE Flag = 2")
    out: Dict[str, Tuple[str, str]] = {}
    for cust_id, short, full in cur.fetchall():
        key = str(cust_id or "").strip()
        short, full = str(short or "").strip(), str(full or "").strip()
        if key and (match_priority_supplier(short, keywords) or match_priority_supplier(full, keywords)):
            out[key] = (short, full)
    return out


def _query_priority_parts(conn, supplier_ids: List[str]) -> Dict[str, dict]:
    """最近一張進貨（comProdRec Flag=100，不限倉）的廠商在 supplier_ids 內的料號：{料號: {supplier_id, last_purchase_date}}。"""
    out: Dict[str, dict] = {}
    cur = conn.cursor()
    for batch in _chunks(sorted(set(supplier_ids))):
        cur.execute(
            "SELECT t.ProdID, t.BillDate, a.CustID FROM ("
            "SELECT ProdID, BillDate, BillNO, ROW_NUMBER() OVER ("
            "PARTITION BY ProdID ORDER BY BillDate DESC, BillNO DESC) AS rn "
            "FROM comProdRec WHERE Flag = 100) t "
            "JOIN comBillAccounts a ON a.Flag = 100 AND a.FundBillNo = t.BillNO "
            f"WHERE t.rn = 1 AND a.CustID IN {_in_clause(len(batch))}",
            tuple(batch),
        )
        for prod_id, bill_date, cust_id in cur.fetchall():
            part = norm_part(prod_id)
            if part and part not in out:
                d = _to_date(bill_date)
                out[part] = {"supplier_id": str(cust_id or "").strip(), "last_purchase_date": d.isoformat() if d else ""}
    return out


def _query_gl_usage(conn, gl_ware: str, start: date, end: date, parts: List[str]) -> Dict[str, Tuple[float, int]]:
    """015 倉出庫（Flag 331，Quantity 為負數）領用量取正數與單據數；正數為沖銷，一併加總。"""
    result: Dict[str, Tuple[float, int]] = {}
    cur = conn.cursor()
    for batch in _chunks(parts):
        cur.execute(
            "SELECT ProdID, SUM(Quantity), COUNT(DISTINCT BillNO) FROM comProdRec "
            "WHERE Flag = 331 AND WareID = %s AND BillDate >= %s AND BillDate <= %s "
            f"AND ProdID IN {_in_clause(len(batch))} GROUP BY ProdID",
            (gl_ware, _yyyymmdd(start), _yyyymmdd(end), *batch),
        )
        for prod_id, qty, bills in cur.fetchall():
            part = norm_part(prod_id)
            if part:
                result[part] = (max(-float(qty or 0), 0.0), int(bills or 0))
    return result


def _query_ware_qty(conn, ware_id: str, parts: List[str]) -> Dict[str, float]:
    """指定倉庫現有量（comWareAmount）。"""
    result: Dict[str, float] = {}
    cur = conn.cursor()
    for batch in _chunks(parts):
        cur.execute(
            "SELECT ProdID, SUM(Quantity) FROM comWareAmount "
            f"WHERE WareID = %s AND ProdID IN {_in_clause(len(batch))} GROUP BY ProdID",
            (ware_id, *batch),
        )
        for prod_id, qty in cur.fetchall():
            part = norm_part(prod_id)
            if part:
                result[part] = result.get(part, 0.0) + float(qty or 0)
    return result


_PRIORITY_COLUMNS = (
    "name", "supplier_id", "supplier_name", "supplier_full_name", "last_purchase_date",
    "st_usage_qty", "st_bill_count", "gl_ware_id", "gl_usage_qty", "gl_bill_count", "gl_qty",
    "usage_qty", "avg_daily_usage", "lookback_days", "planned_lead_days", "actual_lead_days",
    "delay_days", "delay_samples", "on_order_qty", "next_expected_date", "on_order_note", "refreshed_at",
    "st_recent_usage_qty", "st_recent_bill_count", "gl_recent_usage_qty", "gl_recent_bill_count",
)


def refresh_priority_parts(actor: str = "system") -> dict:
    """從正航（唯讀）重算重點廠商料號並整批覆寫本機 low_stock_priority。永遠不丟例外。

    廠商：名單關鍵字比對 comCustomer(Flag=2) → 最近一張進貨（Flag 100，任何倉）是這些廠商的料號。
    用量 = max(ST 領用, 015 倉 Flag 331 領用)，期間沿用 lookback_days；另存 015 現有量、交期統計、在途量，
    以及近 suggest_lookback_days 天 ST／015 的淨領用與單據數（list_rows 判斷額外列是否為常用料）。
    ERP 任何失敗：不動本機舊資料，回傳 ok=False。名單為空：清空此表（不需 ERP）。
    """
    try:
        db.ensure_low_stock_table()
        keywords = get_priority_suppliers()
        now = _now_text()
        if not keywords:
            with db.get_conn() as conn:
                conn.execute("DELETE FROM low_stock_priority")
            db.set_setting(PRIORITY_REFRESHED_KEY, now)
            return {"ok": True, "suppliers": 0, "parts": 0, "rows": 0}
        cfg = get_config()
        today = local_now().date()
        lookback = int(cfg["lookback_days"])
        start = today - timedelta(days=lookback)
        suggest_start = today - timedelta(days=int(cfg["suggest_lookback_days"]))  # 常用料頻率判斷用
        ware_id = erp_stock.get_settings()["ware_id"]
        gl_ware = _gl_ware_id()
        with db.get_conn() as conn:
            watch_parts = {norm_part(r["part_number"]) for r in conn.execute(
                "SELECT part_number FROM low_stock_watch WHERE dismissed=0").fetchall()}

        # ── 階段一：只讀 ERP（失敗就跳到 except，本機資料不動） ──
        with _erp_session() as erp:
            suppliers = _query_priority_suppliers(erp, keywords)
            part_supplier = _query_priority_parts(erp, list(suppliers)) if suppliers else {}
            parts = sorted(part_supplier)
            st_usage = _query_usage(erp, ware_id, start, today, parts) if parts else {}
            gl_usage = _query_gl_usage(erp, gl_ware, start, today, parts) if parts else {}
            # 沒有任何領用的料號不會被列入，只保留有用量或已在關注清單的
            targets = [
                p for p in parts
                if st_usage.get(p, (0.0, 0))[0] > 0 or gl_usage.get(p, (0.0, 0))[0] > 0 or p in watch_parts
            ]
            if suggest_start == start:
                st_recent, gl_recent = st_usage, gl_usage
            else:
                st_recent = _query_usage(erp, ware_id, suggest_start, today, targets) if targets else {}
                gl_recent = _query_gl_usage(erp, gl_ware, suggest_start, today, targets) if targets else {}
            gl_qty = _query_ware_qty(erp, gl_ware, targets) if targets else {}
            info = _query_part_info(erp, targets, ware_id, today, cfg) if targets else {}
            on_order = _fetch_on_order(erp, targets, today, cfg)

        # ── 階段二：寫入 ──
        snapshot = db.get_st_inventory_snapshot()
        rows = []
        for part in targets:
            st_net, st_bills = st_usage.get(part, (0.0, 0))
            gl_net, gl_bills = gl_usage.get(part, (0.0, 0))
            st_recent_net, st_recent_bills = st_recent.get(part, (0.0, 0))
            gl_recent_net, gl_recent_bills = gl_recent.get(part, (0.0, 0))
            usage = max(max(float(st_net), 0.0), float(gl_net))
            meta = part_supplier[part]
            short, full = suppliers.get(meta["supplier_id"], ("", ""))
            pinfo = info.get(part, {})
            if on_order is None:
                oo_qty, oo_date, oo_note = 0.0, "", ON_ORDER_NOTE_FAILED
            else:
                (oo_qty, oo_date), oo_note = on_order.get(part, (0.0, "")), ""
            snap_name = str((snapshot.get(part) or {}).get("description") or "").strip()
            rows.append({
                "name": snap_name or pinfo.get("name") or "",
                "supplier_id": meta["supplier_id"], "supplier_name": short or meta["supplier_id"],
                "supplier_full_name": full, "last_purchase_date": meta["last_purchase_date"],
                "st_usage_qty": float(st_net), "st_bill_count": int(st_bills),
                "gl_ware_id": gl_ware, "gl_usage_qty": float(gl_net), "gl_bill_count": int(gl_bills),
                "gl_qty": float(gl_qty.get(part, 0.0)),
                "usage_qty": usage, "avg_daily_usage": calc_avg_daily_usage(usage, lookback), "lookback_days": lookback,
                "planned_lead_days": pinfo.get("planned_lead_days"), "actual_lead_days": pinfo.get("actual_lead_days"),
                "delay_days": pinfo.get("delay_days"), "delay_samples": int(pinfo.get("delay_samples") or 0),
                "on_order_qty": float(oo_qty), "next_expected_date": oo_date, "on_order_note": oo_note,
                "refreshed_at": now, "part_number": part,
                "st_recent_usage_qty": float(st_recent_net), "st_recent_bill_count": int(st_recent_bills),
                "gl_recent_usage_qty": float(gl_recent_net), "gl_recent_bill_count": int(gl_recent_bills),
            })
        with db.get_conn() as conn:
            conn.execute("DELETE FROM low_stock_priority")
            for row in rows:
                conn.execute(
                    "INSERT INTO low_stock_priority(part_number, " + ", ".join(_PRIORITY_COLUMNS) + ") VALUES("
                    + ",".join("?" * (len(_PRIORITY_COLUMNS) + 1)) + ")",
                    [row["part_number"], *(row[c] for c in _PRIORITY_COLUMNS)],
                )
        db.set_setting(PRIORITY_REFRESHED_KEY, now)
        db.log_activity(
            "低庫存提醒重點廠商",
            f"{actor}：重算 015 庫存與領用，廠商 {len(suppliers)}、料號 {len(parts)}、有用量 {len(targets)} 筆",
        )
        return {"ok": True, "suppliers": len(suppliers), "parts": len(parts), "rows": len(targets),
                "on_order_ok": on_order is not None}
    except Exception as exc:
        logger.warning("重點廠商低庫存重算失敗（保留舊資料）：%s", type(exc).__name__, exc_info=True)
        message = str(exc) if isinstance(exc, ErpError) else type(exc).__name__
        return {"ok": False, "error": f"重點廠商 015 庫存重算失敗：{message}（保留上次資料）"}


# ── 建議 ──────────────────────────────────────────────────────────────────

def _apply_stats(row: dict, usage: Tuple[float, int], info: dict, cfg: Dict[str, float], snapshot_name: str) -> None:
    net, bills = usage
    row["usage_qty"] = net
    row["bill_count"] = bills
    row["avg_daily_usage"] = calc_avg_daily_usage(net, cfg["lookback_days"])
    row["planned_lead_days"] = info.get("planned_lead_days")
    row["actual_lead_days"] = info.get("actual_lead_days")
    row["delay_days"] = info.get("delay_days")
    row["delay_samples"] = int(info.get("delay_samples") or 0)
    name = snapshot_name or info.get("name") or row.get("name") or ""
    row["name"] = name
    if info.get("last_purchase_date"):
        row["last_purchase_date"] = info["last_purchase_date"]
    if info.get("last_purchase_supplier"):
        row["last_purchase_supplier"] = info["last_purchase_supplier"]


def run_suggestion(actor: str = "system") -> dict:
    """從 ERP 建議常用料號並更新關注清單。ERP 失敗丟 ErpError 且不寫入任何資料。"""
    db.ensure_low_stock_table()
    cfg = get_config()
    today = local_now().date()
    ware_id = erp_stock.get_settings()["ware_id"]
    start = today - timedelta(days=int(cfg["lookback_days"]))  # 平均日用量（門檻）用
    suggest_start = today - timedelta(days=int(cfg["suggest_lookback_days"]))  # 只用來判斷是否符合「最少單據數」候選

    with db.get_conn() as conn:
        existing = {r["part_number"]: dict(r) for r in conn.execute("SELECT * FROM low_stock_watch").fetchall()}
    dismissed = {p for p, r in existing.items() if r["dismissed"]}
    main_parts = main_file_parts()  # None = 讀不到主檔，不過濾
    in_main = (lambda p: True) if main_parts is None else (lambda p: norm_part(p) in main_parts)
    active = {p: r for p, r in existing.items() if not r["dismissed"] and in_main(p)}

    # ── 階段一：只讀 ERP（失敗就直接丟出，尚未寫入任何資料） ──
    with _erp_session() as erp:
        usage = _query_usage(erp, ware_id, start, today)
        suggest_usage = usage if suggest_start == start else _query_usage(erp, ware_id, suggest_start, today)
        qualifying = {
            p for p, (net, bills) in suggest_usage.items()
            if bills >= cfg["min_bills"] and net > 0 and in_main(p)
        }
        candidates = qualifying - dismissed
        targets = sorted(candidates | set(active))
        info = _query_part_info(erp, targets, ware_id, today, cfg) if targets else {}
        on_order = _fetch_on_order(erp, targets, today, cfg)  # 失敗 → None，不影響其他結果

    # ── 階段二：寫入 ──
    snapshot = db.get_st_inventory_snapshot()
    now = _now_text()
    summary = {
        "candidates": len(qualifying), "delay_parts": 0, "added": 0, "updated": 0,
        "kept_manual_threshold": 0, "kept_manual_lead": 0,
        "planned_lead_parts": 0, "actual_lead_parts": 0, "default_lead_parts": 0, "total_rows": 0,
        "on_order_ok": on_order is not None,
        **main_file_status(main_parts),
    }
    with db.get_conn() as conn:
        for part in targets:
            row = active.get(part)
            is_new = row is None
            if is_new:
                row = _new_row("auto", actor)
            snap_name = str((snapshot.get(part) or {}).get("description") or "").strip()
            _apply_stats(row, usage.get(part, (0.0, 0)), info.get(part, {}), cfg, snap_name)
            derive_auto_fields(row, cfg)
            row["suggested_at"] = now
            row["updated_at"] = now
            row["updated_by"] = actor
            _save_row(conn, part, row)
            _write_on_order(conn, part, on_order, now)

            if is_new:
                summary["added"] += 1
            else:
                summary["updated"] += 1
                if row["threshold_source"] == "manual":
                    summary["kept_manual_threshold"] += 1
                if row["lead_time_source"] == "manual":
                    summary["kept_manual_lead"] += 1
            src = row["lead_time_source"]
            if src == "planned":
                summary["planned_lead_parts"] += 1
            elif src == "actual":
                summary["actual_lead_parts"] += 1
            elif src == "default":
                summary["default_lead_parts"] += 1
            if row["delay_days"] is not None:
                summary["delay_parts"] += 1
        summary["total_rows"] = conn.execute(
            "SELECT COUNT(*) FROM low_stock_watch WHERE dismissed=0"
        ).fetchone()[0]

    db.set_setting(LAST_SUGGESTED_KEY, now)
    db.log_activity(
        "低庫存提醒建議",
        f"{actor}：候選 {summary['candidates']}、新增 {summary['added']}、更新 {summary['updated']}、"
        f"保留手動門檻 {summary['kept_manual_threshold']}、保留手動交期 {summary['kept_manual_lead']}、"
        f"清單共 {summary['total_rows']} 筆",
    )
    summary["priority"] = refresh_priority_parts(actor)  # 獨立處理，失敗不影響建議結果
    return summary


# ── 編輯 ──────────────────────────────────────────────────────────────────

def _check_threshold(value: Any) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        raise LowStockError("門檻必須是數字") from None
    if math.isnan(num) or math.isinf(num) or num < 0:
        raise LowStockError("門檻不可為負數")
    return num


def _check_lead(value: Any) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        raise LowStockError("交期必須是數字") from None
    if math.isnan(num) or math.isinf(num) or not (0 < num <= 365):
        raise LowStockError("交期需介於 1 到 365 天")
    return num


def _stats_for_new_part(part: str, cfg: Dict[str, float]) -> Tuple[Tuple[float, int], dict]:
    """單一料號向 ERP 取統計；任何失敗都回空（料號仍可加入，之後由建議或手動補齊）。"""
    try:
        today = local_now().date()
        ware_id = erp_stock.get_settings()["ware_id"]
        with _erp_session() as erp:
            usage = _query_usage(erp, ware_id, today - timedelta(days=int(cfg["lookback_days"])), today, [part])
            info = _query_part_info(erp, [part], ware_id, today, cfg).get(part, {})
        return usage.get(part, (0.0, 0)), info
    except Exception as exc:
        logger.info("新增料號 %s 取不到 ERP 統計：%s", part, type(exc).__name__)
        return (0.0, 0), {}


def add_part(part: str, threshold: Any = None, lead_time_days: Any = None, note: str = "", actor: str = "web") -> dict:
    key = norm_part(part)
    if not key:
        raise LowStockError("請輸入料號")
    new_threshold = _check_threshold(threshold) if threshold not in (None, "") else None
    new_lead = _check_lead(lead_time_days) if lead_time_days not in (None, "") else None
    db.ensure_low_stock_table()
    cfg = get_config()
    with db.get_conn() as conn:
        row = _get_row(conn, key)
    if row and not row["dismissed"]:
        raise LowStockExists("此料號已在清單中")

    if row is None:
        row = _new_row("manual", actor)
        usage, info = _stats_for_new_part(key, cfg)
        snap_name = str((db.get_st_inventory_snapshot().get(key) or {}).get("description") or "").strip()
        if usage != (0.0, 0) or info:
            _apply_stats(row, usage, info, cfg, snap_name)
        else:
            row["name"] = snap_name
    else:
        row["dismissed"] = 0
        row["enabled"] = 1
    row["note"] = str(note or "").strip()
    if new_threshold is not None:
        row["threshold_qty"] = new_threshold
        row["threshold_source"] = "manual"
    else:
        row["threshold_source"] = "auto"
    if new_lead is not None:
        row["lead_time_days"] = new_lead
        row["lead_time_source"] = "manual"
    else:
        row["lead_time_source"] = "default"
    derive_auto_fields(row, cfg)
    row["updated_at"] = _now_text()
    row["updated_by"] = actor
    with db.get_conn() as conn:
        _save_row(conn, key, row)
    db.log_activity("低庫存提醒新增料號", f"{actor}：{key}")
    return {"part_number": key, **row}


def update_part(part: str, actor: str = "web", **fields: Any) -> dict:
    """可更新 threshold_qty / lead_time_days / enabled / note；值為 None 的 threshold / lead 表示恢復自動。"""
    allowed = {"threshold_qty", "lead_time_days", "enabled", "note"}
    unknown = set(fields) - allowed
    if unknown:
        raise LowStockError(f"不支援的欄位：{', '.join(sorted(unknown))}")
    key = norm_part(part)
    db.ensure_low_stock_table()
    cfg = get_config()
    with db.get_conn() as conn:
        row = _get_row(conn, key)
    if not row or row["dismissed"]:
        raise LowStockNotFound("找不到此料號")

    if "threshold_qty" in fields:
        if fields["threshold_qty"] is None:
            row["threshold_source"] = "auto"
        else:
            row["threshold_qty"] = _check_threshold(fields["threshold_qty"])
            row["threshold_source"] = "manual"
    if "lead_time_days" in fields:
        if fields["lead_time_days"] is None:
            row["lead_time_source"] = "default"  # 先標為自動，下方 derive 會依統計決定實際來源
        else:
            row["lead_time_days"] = _check_lead(fields["lead_time_days"])
            row["lead_time_source"] = "manual"
    if "enabled" in fields:
        row["enabled"] = 1 if fields["enabled"] else 0
    if "note" in fields:
        row["note"] = str(fields["note"] or "").strip()
    derive_auto_fields(row, cfg)
    row["updated_at"] = _now_text()
    row["updated_by"] = actor
    with db.get_conn() as conn:
        _save_row(conn, key, row)
    db.log_activity("低庫存提醒修改", f"{actor}：{key} {sorted(fields)}")
    return {"part_number": key, **row}


def remove_part(part: str, actor: str = "web") -> bool:
    """移除 = dismissed=1（保留資料列，建議不會再自動加回）。"""
    key = norm_part(part)
    db.ensure_low_stock_table()
    with db.get_conn() as conn:
        row = _get_row(conn, key)
        if not row or row["dismissed"]:
            return False
        conn.execute(
            "UPDATE low_stock_watch SET dismissed=1, updated_at=?, updated_by=? WHERE part_number=?",
            (_now_text(), actor, key),
        )
    db.log_activity("低庫存提醒移除料號", f"{actor}：{key}")
    return True


# ── 讀取（只讀本機，不連 ERP） ─────────────────────────────────────────────

def _view_row(rec: dict, qty: float, priority_suppliers: Iterable[str] = ()) -> dict:
    avg_daily = rec.get("avg_daily_usage")
    enabled = bool(rec.get("enabled"))
    threshold = rec.get("threshold_qty")
    lead_source = rec.get("lead_time_source") or "default"
    on_order_qty = float(rec.get("on_order_qty") or 0)
    supplier = rec.get("last_purchase_supplier") or ""
    priority = match_priority_supplier(supplier, priority_suppliers)
    return {
        "part_number": rec["part_number"],
        "name": rec.get("name") or "",
        "qty": qty,
        "enabled": enabled,
        "status": calc_status(enabled, qty, threshold, on_order_qty),
        "on_order_qty": on_order_qty,
        "next_expected_date": rec.get("next_expected_date") or "",
        "on_order_note": rec.get("on_order_note") or "",
        "threshold": threshold,
        "threshold_source": rec.get("threshold_source") or "auto",
        "avg_daily_usage": avg_daily,
        "avg_monthly_usage": round(avg_daily * 30, 1) if avg_daily is not None else None,
        "lead_time_days": rec.get("lead_time_days"),
        "lead_time_source": lead_source,
        "lead_time_label": lead_label(lead_source, rec.get("delay_days"), rec.get("planned_lead_days")),
        "planned_lead_days": rec.get("planned_lead_days"),
        "actual_lead_days": rec.get("actual_lead_days"),
        "delay_days": rec.get("delay_days"),
        "delay_samples": rec.get("delay_samples") or 0,
        "days_of_cover": calc_days_of_cover(qty, avg_daily),
        "last_purchase_date": rec.get("last_purchase_date") or "",
        "last_purchase_supplier": supplier,
        "priority_supplier": priority,
        "is_priority_supplier": bool(priority),
        "note": rec.get("note") or "",
        "source": rec.get("source") or "auto",
        "usage_qty": rec.get("usage_qty"),
        "bill_count": rec.get("bill_count"),
        "suggested_at": rec.get("suggested_at") or "",
    }


def _sort_key(view: dict) -> tuple:
    cover = view["days_of_cover"]
    threshold = view["threshold"]
    shortfall = (threshold - view["qty"]) if threshold is not None else 0
    return (cover is None, cover if cover is not None else 0, -shortfall, view["part_number"])


def suggested_po_gap(row: dict) -> int:
    """開採購單建議量（未計 MOQ）：門檻 - 現有量 - 在途量，至少 1；沒有門檻則為 1。"""
    threshold = row.get("threshold")
    if threshold is None:
        return 1
    gap = float(threshold) - float(row.get("qty") or 0) - float(row.get("on_order_qty") or 0)
    return max(1, int(math.ceil(max(gap, 0) - 1e-9)))


def with_moq(views: List[dict]) -> List[dict]:
    """替每列加上 moq / moq_source / last_receipt_* / receipt_gcd / receipt_count / receipt_qtys / moq_floor_applied / suggested_po_qty（= max(缺口, 估算 MOQ)）。

    MOQ 以近 10 次進貨量的最大公因數估算（不低於主檔 / 正航最小採購量）（見 moq.lookup_moq，批次查詢 + 快取）；正航連不上時退回本站 MOQ。"""
    from . import moq as moq_service

    info = moq_service.lookup_moq([v["part_number"] for v in views])
    for v in views:
        i = info.get(norm_part(v["part_number"])) or {}
        value = float(i.get("moq") or 0)
        v["moq"] = value
        v["moq_source"] = i.get("moq_source") or moq_service.SOURCE_NONE
        v["last_receipt_qty"] = i.get("last_receipt_qty")
        v["last_receipt_date"] = i.get("last_receipt_date") or ""
        v["last_receipt_bill"] = i.get("last_receipt_bill") or ""
        v["receipt_gcd"] = i.get("receipt_gcd")
        v["receipt_count"] = i.get("receipt_count") or 0
        v["receipt_qtys"] = i.get("receipt_qtys") or []
        v["moq_floor_applied"] = bool(i.get("moq_floor_applied"))
        v["suggested_po_qty"] = moq_service.adjust_qty(suggested_po_gap(v), value)
    return views


def _priority_records(keywords: List[str]) -> Dict[str, Tuple[dict, str]]:
    """low_stock_priority 中廠商仍符合目前名單的列：{料號: (資料列, 命中關鍵字)}；名單為空 → {}。"""
    if not keywords:
        return {}
    with db.get_conn() as conn:
        recs = [dict(r) for r in conn.execute("SELECT * FROM low_stock_priority").fetchall()]
    out: Dict[str, Tuple[dict, str]] = {}
    for r in recs:
        word = (match_priority_supplier(r.get("supplier_name"), keywords)
                or match_priority_supplier(r.get("supplier_full_name"), keywords))
        if word:
            out[norm_part(r["part_number"])] = (r, word)
    return out


def _priority_view(
    prec: dict, keyword: str, watch: Optional[dict], st_qty: float,
    cfg: Dict[str, float], keywords: List[str], today: date,
) -> dict:
    """重點廠商列：用量 = max(ST, 015)、現有量 = ST + 015；關注清單內的手動門檻 / 交期 / 停用 / 備註照用。

    依日均用量撐不到在途最近預交日（今天 + 可撐天數 < 預交日）→ eta_short，狀態視為 low。"""
    if watch:
        rec = dict(watch)
    else:
        rec = _new_row("priority", "system")
        rec.update(
            part_number=prec["part_number"], name=prec.get("name") or "",
            planned_lead_days=prec.get("planned_lead_days"), actual_lead_days=prec.get("actual_lead_days"),
            delay_days=prec.get("delay_days"), delay_samples=prec.get("delay_samples") or 0,
            suggested_at=prec.get("refreshed_at") or "",
        )
    # 在途量一律用重點廠商表（與關注清單同時刷新；剛加入關注的料號在關注清單內還沒有在途資料）
    rec.update(
        on_order_qty=prec.get("on_order_qty"), next_expected_date=prec.get("next_expected_date") or "",
        on_order_note=prec.get("on_order_note") or "",
    )
    rec["avg_daily_usage"] = prec.get("avg_daily_usage")
    rec["usage_qty"] = prec.get("usage_qty")
    rec["bill_count"] = int(prec.get("st_bill_count") or 0) + int(prec.get("gl_bill_count") or 0)
    rec["last_purchase_supplier"] = prec.get("supplier_name") or rec.get("last_purchase_supplier") or ""
    if prec.get("last_purchase_date"):
        rec["last_purchase_date"] = prec["last_purchase_date"]
    if not rec.get("name"):
        rec["name"] = prec.get("name") or ""
    derive_auto_fields(rec, cfg)
    gl_qty = float(prec.get("gl_qty") or 0)
    view = _view_row(rec, st_qty + gl_qty, keywords)
    view.update(
        priority_supplier=keyword, is_priority_supplier=True,
        st_qty=st_qty, gl_qty=gl_qty, gl_ware_id=prec.get("gl_ware_id") or "",
        st_usage_qty=prec.get("st_usage_qty"), gl_usage_qty=prec.get("gl_usage_qty"),
        in_watch=bool(watch), priority_extra=False, eta_short=False, eta_gap_days=None,
    )
    avg = view["avg_daily_usage"]
    try:
        eta = date.fromisoformat(view["next_expected_date"]) if view["next_expected_date"] else None
    except ValueError:
        eta = None
    if view["enabled"] and eta and avg and avg > 0:
        gap = (eta - today).days - view["qty"] / float(avg)
        if gap > 1e-9:
            view["eta_short"] = True
            view["eta_gap_days"] = int(math.ceil(gap - 1e-9))
            view["status"] = "low"
    return view


def _priority_frequent(prec: dict, cfg: Dict[str, float]) -> bool:
    """常用料：近 suggest_lookback_days 天 ST 或 015 任一來源單據數 ≥ min_bills 且該來源淨領用 > 0（同建議候選規則）。"""
    min_bills = cfg["min_bills"]
    return any(
        int(prec.get(f"{src}_recent_bill_count") or 0) >= min_bills and float(prec.get(f"{src}_recent_usage_qty") or 0) > 0
        for src in ("st", "gl")
    )


def _priority_listed(view: dict, prec: dict, cfg: Dict[str, float]) -> bool:
    """額外列（不在關注清單、但在主檔）須先是常用料，再「現有量低於門檻」或「撐不到預交日」才列入。

    一單一買、進來就用掉的料現有量常為 0，永遠低於門檻，不先篩頻率會灌爆清單。"""
    if not view["enabled"] or not _priority_frequent(prec, cfg):
        return False
    threshold = view["threshold"]
    return view["eta_short"] or (threshold is not None and view["qty"] < threshold)


def list_rows(main_parts: Any = _UNSET) -> List[dict]:
    """所有未移除的關注料號（含現有量與狀態），重點廠商在前，再來低庫存，其次依可撐天數。只顯示主檔內料號（主檔讀不到則不過濾）。

    重點廠商料號（low_stock_priority，廠商符合目前名單）改用 ST + 015 的用量與現有量，同樣只列主檔內料號；
    不在關注清單的重點廠商料，須為常用料（近 suggest_lookback_days 天 ST 或 015 單據數 ≥ min_bills），
    且低於門檻或撐不到預交日才額外列入（使用者移除過的不列）。"""
    db.ensure_low_stock_table()
    if main_parts is _UNSET:
        main_parts = main_file_parts()
    with db.get_conn() as conn:
        all_recs = [dict(r) for r in conn.execute("SELECT * FROM low_stock_watch WHERE dismissed=0").fetchall()]
    recs = all_recs
    if main_parts is not None:
        recs = [r for r in recs if norm_part(r["part_number"]) in main_parts]
    stock = db.get_st_inventory_stock()
    priority = get_priority_suppliers()
    prio = _priority_records(priority)
    if not prio:
        views = [_view_row(r, float(stock.get(r["part_number"], 0.0)), priority) for r in recs]
    else:
        cfg = get_config()
        today = local_now().date()
        views = []
        shown = set()
        for r in recs:
            part = norm_part(r["part_number"])
            shown.add(part)
            if part in prio:
                prec, word = prio[part]
                views.append(_priority_view(prec, word, r, float(stock.get(part, 0.0)), cfg, priority, today))
            else:
                views.append(_view_row(r, float(stock.get(r["part_number"], 0.0)), priority))
        watch_by_part = {norm_part(r["part_number"]): r for r in all_recs}
        with db.get_conn() as conn:
            dismissed = {norm_part(r["part_number"]) for r in conn.execute(
                "SELECT part_number FROM low_stock_watch WHERE dismissed=1").fetchall()}
        for part in sorted(prio):
            if part in shown or part in dismissed:
                continue
            if main_parts is not None and part not in main_parts:
                continue  # 重點廠商料也只列主檔有的料號（與一般料同一套過濾）
            prec, word = prio[part]
            view = _priority_view(prec, word, watch_by_part.get(part), float(stock.get(part, 0.0)), cfg, priority, today)
            if _priority_listed(view, prec, cfg):
                view["priority_extra"] = True
                views.append(view)
    views = with_moq(views)
    order = {"low": 0, "on_order": 1, "ok": 2, "disabled": 3}
    views.sort(key=lambda v: (not v["is_priority_supplier"], order[v["status"]], *_sort_key(v)))
    return views


def list_alerts(main_parts: Any = _UNSET) -> List[dict]:
    low = [v for v in list_rows(main_parts) if v["status"] == "low"]
    low.sort(key=_sort_key)
    return low


def last_suggested_at() -> str:
    return db.get_setting(LAST_SUGGESTED_KEY, "")


def priority_refreshed_at() -> str:
    return db.get_setting(PRIORITY_REFRESHED_KEY, "")
