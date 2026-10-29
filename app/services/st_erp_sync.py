"""把正航 ERP ST 倉庫存同步成本機 ST 庫存快照。

等同一次自動「上傳新 Excel」：走 apply_st_inventory_snapshot，
之後缺料 / 補料 / 發料 / 排程仍全部讀 db.get_st_inventory_stock()。
失敗（連不上、0 筆、筆數暴跌）時完全不動快照與既有 settings，只記錄錯誤。
"""
from __future__ import annotations

import logging
import os
import threading

from .. import database as db
from . import erp_stock
from .inventory_count_lock import get_inventory_count_lock
from .local_time import local_now
from .st_inventory import ST_SOURCE_ERP, apply_st_inventory_snapshot

logger = logging.getLogger("st_erp_sync")

# 新筆數低於現有快照的這個比例，視為 ERP 異常，拒絕覆蓋
MIN_PART_RATIO = 0.5
ERP_ACTOR = "erp_sync"
AUTO_SYNC_DEFAULT_MINUTES = 60
AUTO_SYNC_MIN_MINUTES = 5
AUTO_SYNC_STARTUP_DELAY_SECONDS = 60.0

_sync_lock = threading.Lock()


class StErpSyncError(Exception):
    """同步失敗；訊息為繁體中文，且不含密碼。"""


def parse_auto_sync_minutes(raw: str | None = None) -> int:
    """解析 ERP_ST_AUTO_SYNC_MINUTES：0 / 負數 / 非數字（非空）= 關閉；未設定 = 預設；過小取下限。"""
    text = (os.environ.get("ERP_ST_AUTO_SYNC_MINUTES") if raw is None else raw)
    text = str(text if text is not None else "").strip()
    if not text:
        return AUTO_SYNC_DEFAULT_MINUTES
    try:
        minutes = int(float(text))
    except ValueError:
        return 0
    if minutes <= 0:
        return 0
    return max(AUTO_SYNC_MIN_MINUTES, minutes)


def _record_failure(message: str) -> None:
    try:
        db.set_setting("st_inventory_last_sync_error", message)
        db.set_setting("st_inventory_last_sync_error_at", local_now().isoformat(timespec="seconds"))
    except Exception:  # 記錄失敗不可掩蓋原本錯誤
        logger.exception("記錄 ERP 同步錯誤失敗")


def _fail(message: str, *, record: bool = True) -> StErpSyncError:
    logger.warning("ST 庫存 ERP 同步失敗：%s", message)
    if record:
        _record_failure(message)
    return StErpSyncError(message)


def sync_st_inventory_from_erp(actor: str = ERP_ACTOR) -> dict:
    """從 ERP 讀 ST 庫存並取代本機快照。成功回傳摘要；失敗丟 StErpSyncError 且不改任何資料。"""
    if not _sync_lock.acquire(blocking=False):
        raise _fail("ERP 同步進行中，請稍後再試。", record=False)
    try:
        try:
            data = erp_stock.get_st_stock(force_refresh=True)
        except erp_stock.ErpError as exc:
            raise _fail(f"從正航更新失敗：{exc}（本機 ST 庫存維持原狀）") from None
        except Exception as exc:
            raise _fail(f"從正航更新失敗：{type(exc).__name__}（本機 ST 庫存維持原狀）") from None

        stock: dict[str, float] = {}
        descriptions: dict[str, str] = {}
        for row in data.get("rows") or []:
            part = str(row.get("part_number") or "").strip().upper()
            if not part:
                continue
            stock[part] = float(stock.get(part, 0.0) + float(row.get("quantity") or 0))
            name = str(row.get("name") or "").strip()
            if name and not descriptions.get(part):
                descriptions[part] = name

        if not stock:
            raise _fail("正航回傳 0 筆 ST 庫存，已拒絕更新（本機 ST 庫存維持原狀）。")
        existing_count = len(db.get_st_inventory_snapshot())
        if existing_count and len(stock) < existing_count * MIN_PART_RATIO:
            raise _fail(
                f"正航僅回傳 {len(stock)} 筆，少於現有 {existing_count} 筆的 {int(MIN_PART_RATIO * 100)}%，"
                "疑似異常，已拒絕更新（本機 ST 庫存維持原狀）。"
            )

        ware_id = str(data.get("ware_id") or "")
        ware_name = str(data.get("ware_name") or "").strip()
        label = f"正航 ERP（ST 倉 {ware_id}{' ' + ware_name if ware_name else ''}）"
        result = apply_st_inventory_snapshot(
            stock,
            descriptions,
            source=ST_SOURCE_ERP,
            filename=label,
            file_path="",
            actor=actor,
            activity_detail=f"從正航 ERP 同步 ST 庫存（{ware_id}），{len(stock)} 筆",
        )
        try:  # 在途量刷新獨立處理，失敗不影響庫存同步結果
            from . import low_stock
            low_stock.refresh_on_order()
        except Exception:
            logger.warning("同步後刷新低庫存在途量失敗", exc_info=True)
        return {
            "ok": True,
            "source": ST_SOURCE_ERP,
            "filename": label,
            "part_count": result["part_count"],
            "loaded_at": result["loaded_at"],
        }
    finally:
        _sync_lock.release()


def run_auto_sync_once() -> bool:
    """背景排程的單次執行；永遠不丟例外。回傳是否成功同步。"""
    if not erp_stock.is_configured():
        return False
    try:
        if get_inventory_count_lock():
            logger.info("ST 盤點進行中，略過本次 ERP 自動同步")
            return False
        sync_st_inventory_from_erp()
        return True
    except StErpSyncError:
        return False  # 已記錄
    except Exception:
        logger.exception("ERP 自動同步發生未預期錯誤")
        return False


class StErpAutoSyncScheduler:
    def __init__(self):
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, minutes: int | None = None) -> bool:
        interval = parse_auto_sync_minutes() if minutes is None else minutes
        if interval <= 0:
            logger.info("ERP ST 自動同步已關閉")
            return False
        if self._thread and self._thread.is_alive():
            return True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            args=(interval * 60.0,),
            name="st-erp-auto-sync",
            daemon=True,
        )
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None

    def _run_loop(self, interval_seconds: float) -> None:
        if self._stop_event.wait(AUTO_SYNC_STARTUP_DELAY_SECONDS):
            return
        run_auto_sync_once()
        while not self._stop_event.wait(interval_seconds):
            run_auto_sync_once()


st_erp_auto_sync_scheduler = StErpAutoSyncScheduler()
