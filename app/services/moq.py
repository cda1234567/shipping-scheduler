"""採購最低量（MOQ）：開採購單 / 缺料採購的預設數量 = max(需求, MOQ)。

MOQ 來源（依序）：
1. 本站有效 MOQ（dispatch_pipeline.get_effective_moq：主檔 MOQ 欄，被庫存快照 / 手動編輯覆蓋）→「主檔」。
2. 正航 comProduct.MinPurch（最低採購量，唯讀 SELECT）→「ERP 最低採購量」；只在本站沒有或為 0 時使用，
   正航連不上則靜默略過。
目前沒有可靠的訂購倍數（TakeMatMultiple 是領料倍數、MinPackQty 全為 0），所以不進位到倍數；
adjust_qty 預留 multiple 參數，之後有可靠來源再帶入。
"""
from __future__ import annotations

import logging
import math
from typing import Any, Dict, Iterable, Optional

from .. import database as db

logger = logging.getLogger(__name__)

SOURCE_SITE = "主檔"
SOURCE_ERP = "ERP 最低採購量"
IN_CHUNK = 200


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


def lookup_moq(parts: Iterable[str], use_erp: bool = True) -> Dict[str, Dict[str, Any]]:
    """每個料號的 {moq, moq_source}；沒有 MOQ 時 moq=0、moq_source=""。本站優先，ERP 只補缺。"""
    keys = list(dict.fromkeys(_norm(p) for p in parts if _norm(p)))
    site = site_moq_map()
    out: Dict[str, Dict[str, Any]] = {}
    missing = []
    for key in keys:
        value = site.get(key, 0.0)
        if value > 0:
            out[key] = {"moq": value, "moq_source": SOURCE_SITE}
        else:
            missing.append(key)
    erp = _erp_min_purch(missing) if use_erp and missing else {}
    for key in missing:
        value = erp.get(key, 0.0)
        out[key] = {"moq": value, "moq_source": SOURCE_ERP} if value > 0 else {"moq": 0, "moq_source": ""}
    return out


def moq_fields(need: Any, info: Optional[Dict[str, Any]], multiple: Any = 0) -> Dict[str, Any]:
    """回應用欄位：need_qty / moq / moq_source / moq_adjusted / suggested_qty。"""
    info = info or {}
    moq = float(info.get("moq") or 0)
    need_qty = adjust_qty(need)
    qty = adjust_qty(need, moq, multiple)
    return {
        "need_qty": need_qty,
        "moq": moq,
        "moq_source": info.get("moq_source") or "" if moq > 0 else "",
        "moq_adjusted": qty > need_qty,
        "suggested_qty": qty,
    }
