"""採購最低量（MOQ）：開採購單 / 缺料採購 / 低庫存的預設數量 = max(需求, 估算 MOQ)。

估算 MOQ 來源（依序）：
1. last_receipt：該料號「最近一次進貨」的數量 —— 正航來源公司 comProdRec（Flag=100 進貨、TranType=205、
   WareID=預設倉 001），最新 = BillDate 最大、再取 BillNO 最大；同一張單有多列同料號就加總。
   BillDate 在未來的單據也算。唯讀 SELECT，批次查詢（分段 IN + ROW_NUMBER），結果在程序內快取 30 分鐘。
2. site_master：本站有效 MOQ（dispatch_pipeline.get_effective_moq：主檔 MOQ 欄，被庫存快照 / 手動編輯覆蓋）。
3. erp_minpurch：正航 comProduct.MinPurch > 0。
4. none：沒有任何資料，預設數量就是需求。
正航連不上時進貨查詢靜默略過（改走 2、3，並在 receipt_lookup_failed 標記；失敗 60 秒內不再重試）。
目前沒有可靠的訂購倍數，所以不進位到倍數；adjust_qty 預留 multiple 參數。
"""
from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .. import database as db

logger = logging.getLogger(__name__)

SOURCE_RECEIPT = "last_receipt"
SOURCE_SITE = "site_master"
SOURCE_ERP = "erp_minpurch"
SOURCE_NONE = "none"
IN_CHUNK = 200
RECEIPT_TTL_SECONDS = 30 * 60
RECEIPT_FAIL_RETRY_SECONDS = 60
RECEIPT_FLAG = 100
RECEIPT_TRAN_TYPE = 205

_cache_lock = threading.Lock()
_receipt_cache: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}  # 料號 → (取得時間, 紀錄；None = 沒有進貨紀錄)
_receipt_fail_until = 0.0


def _clock() -> float:
    return time.monotonic()


def clear_receipt_cache() -> None:
    global _receipt_fail_until
    with _cache_lock:
        _receipt_cache.clear()
        _receipt_fail_until = 0.0


def _norm(part: Any) -> str:
    return str(part or "").strip().upper()


def adjust_qty(need: Any, moq: Any = 0, multiple: Any = 0) -> int:
    """預設採購量：need 先無條件進位成整數，再取 max(need, moq)；multiple > 0 時再進位到倍數。
    need <= 0 回 0（不需要買就不湊 MOQ）；moq 為空 / 0 / 負數 / 無法轉數字時視為沒有 MOQ。
    """
    try:
        n = float(need or 0)
    except (TypeError, ValueError):
        n = 0.0
    if n <= 0:
        return 0
    qty = int(math.ceil(n - 1e-9))
    try:
        m = float(moq or 0)
    except (TypeError, ValueError):
        m = 0.0
    if m > 0:
        qty = max(qty, int(math.ceil(m - 1e-9)))
    try:
        mult = float(multiple or 0)
    except (TypeError, ValueError):
        mult = 0.0
    if mult > 0:
        qty = int(math.ceil(qty / mult - 1e-9) * mult)
    return qty


def site_moq_map() -> Dict[str, float]:
    """本站有效 MOQ（大寫料號 → MOQ）；找不到主檔時只用庫存快照，任何錯誤都回空 dict。"""
    try:
        from . import dispatch_pipeline

        main_path = str(db.get_setting("main_file_path") or "").strip()
        raw = dispatch_pipeline.get_effective_moq(main_path)
        return {_norm(p): float(v or 0) for p, v in raw.items() if _norm(p)}
    except Exception as exc:
        logger.info("讀不到本站 MOQ：%s", type(exc).__name__)
        return {}


def _erp_min_purch(parts: Iterable[str]) -> Dict[str, float]:
    """正航 comProduct.MinPurch > 0 的料號（唯讀；失敗回空 dict）。"""
    items = sorted({p for p in parts if p})
    result: Dict[str, float] = {}
    if not items:
        return result
    try:
        from . import erp_purchase

        with erp_purchase._source_session() as conn:
            cur = conn.cursor()
            for i in range(0, len(items), IN_CHUNK):
                batch = items[i:i + IN_CHUNK]
                cur.execute(
                    "SELECT ProdID, MinPurch FROM comProduct WITH (NOLOCK) "
                    f"WHERE ProdID IN ({','.join(['%s'] * len(batch))})",
                    tuple(batch),
                )
                for prod_id, min_purch in cur.fetchall():
                    key = _norm(prod_id)
                    value = float(min_purch or 0)
                    if key and value > 0:
                        result[key] = value
    except Exception as exc:
        logger.info("正航最低採購量查詢失敗，略過：%s", type(exc).__name__)
        return {}
    return result


def _fmt_bill_date(value: Any) -> str:
    text = str(value or "").strip().replace("-", "").replace("/", "")[:8]
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}" if len(text) == 8 and text.isdigit() else ""


def _query_last_receipts(parts: List[str]) -> Dict[str, Dict[str, Any]]:
    """每個料號最近一張進貨單的 {qty, date, bill}（唯讀；沒有進貨紀錄的料號不在結果內；連線失敗會丟例外）。"""
    from . import erp_purchase, erp_stock

    ware_id = erp_stock.get_settings()["ware_id"]
    result: Dict[str, Dict[str, Any]] = {}
    with erp_purchase._source_session() as conn:
        cur = conn.cursor()
        for i in range(0, len(parts), IN_CHUNK):
            batch = parts[i:i + IN_CHUNK]
            cur.execute(
                "SELECT ProdID, BillNO, BillDate, Qty FROM ("
                "SELECT ProdID, BillNO, BillDate, SUM(Quantity) AS Qty, "
                "ROW_NUMBER() OVER (PARTITION BY ProdID ORDER BY BillDate DESC, BillNO DESC) AS rn "
                "FROM comProdRec WITH (NOLOCK) "
                "WHERE Flag = %s AND TranType = %s AND WareID = %s "
                f"AND ProdID IN ({','.join(['%s'] * len(batch))}) "
                "GROUP BY ProdID, BillNO, BillDate) t WHERE rn = 1",
                (RECEIPT_FLAG, RECEIPT_TRAN_TYPE, ware_id, *batch),
            )
            for prod_id, bill_no, bill_date, qty in cur.fetchall():
                key = _norm(prod_id)
                if key:
                    result[key] = {"qty": float(qty or 0), "date": _fmt_bill_date(bill_date),
                                   "bill": str(bill_no or "").strip()}
    return result


def last_receipts(parts: Iterable[str]) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    """(料號 → 最近進貨紀錄, 查詢是否失敗)；有快取就不查，失敗時 60 秒內不重試。"""
    global _receipt_fail_until
    keys = list(dict.fromkeys(_norm(p) for p in parts if _norm(p)))
    now = _clock()
    out: Dict[str, Dict[str, Any]] = {}
    todo: List[str] = []
    with _cache_lock:
        for key in keys:
            hit = _receipt_cache.get(key)
            if hit and now - hit[0] < RECEIPT_TTL_SECONDS:
                if hit[1]:
                    out[key] = hit[1]
            else:
                todo.append(key)
        blocked = now < _receipt_fail_until
    if not todo:
        return out, False
    if blocked:
        return out, True
    try:
        found = _query_last_receipts(todo)
    except Exception as exc:
        logger.info("正航最近進貨查詢失敗，改用其他 MOQ 來源：%s", type(exc).__name__)
        with _cache_lock:
            _receipt_fail_until = _clock() + RECEIPT_FAIL_RETRY_SECONDS
        return out, True
    stamp = _clock()
    with _cache_lock:
        for key in todo:
            _receipt_cache[key] = (stamp, found.get(key))
    out.update(found)
    return out, False


def lookup_moq(parts: Iterable[str], use_erp: bool = True) -> Dict[str, Dict[str, Any]]:
    """每個料號的 {moq, moq_source, last_receipt_qty, last_receipt_date, last_receipt_bill, receipt_lookup_failed}。

    順序：最近進貨量 > 本站 MOQ > 正航 MinPurch > 無（moq=0、moq_source='none'）。
    最近進貨紀錄有就一律帶出（即使 MOQ 採用的是其他來源，例如進貨量為 0）。
    """
    keys = list(dict.fromkeys(_norm(p) for p in parts if _norm(p)))
    site = site_moq_map()
    receipts, failed = last_receipts(keys) if use_erp and keys else ({}, False)
    out: Dict[str, Dict[str, Any]] = {}
    missing = []
    for key in keys:
        rec = receipts.get(key)
        info: Dict[str, Any] = {
            "moq": 0, "moq_source": SOURCE_NONE,
            "last_receipt_qty": rec["qty"] if rec else None,
            "last_receipt_date": rec["date"] if rec else "",
            "last_receipt_bill": rec["bill"] if rec else "",
            "receipt_lookup_failed": failed,
        }
        out[key] = info
        if rec and rec["qty"] > 0:
            info.update(moq=rec["qty"], moq_source=SOURCE_RECEIPT)
        elif site.get(key, 0.0) > 0:
            info.update(moq=site[key], moq_source=SOURCE_SITE)
        else:
            missing.append(key)
    erp = _erp_min_purch(missing) if use_erp and missing else {}
    for key in missing:
        if erp.get(key, 0.0) > 0:
            out[key].update(moq=erp[key], moq_source=SOURCE_ERP)
    return out


def moq_fields(need: Any, info: Optional[Dict[str, Any]], multiple: Any = 0) -> Dict[str, Any]:
    """回應用欄位：need_qty / moq / moq_source / last_receipt_* / moq_adjusted / suggested_qty。"""
    info = info or {}
    moq = float(info.get("moq") or 0)
    need_qty = adjust_qty(need)
    qty = adjust_qty(need, moq, multiple)
    return {
        "need_qty": need_qty,
        "moq": moq,
        "moq_source": (info.get("moq_source") or SOURCE_NONE) if moq > 0 else SOURCE_NONE,
        "last_receipt_qty": info.get("last_receipt_qty"),
        "last_receipt_date": info.get("last_receipt_date") or "",
        "last_receipt_bill": info.get("last_receipt_bill") or "",
        "moq_adjusted": qty > need_qty,
        "suggested_qty": qty,
    }
