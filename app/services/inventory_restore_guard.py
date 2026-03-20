from __future__ import annotations

from fastapi import HTTPException

from .. import database as db

RESTORE_BLOCKED_MESSAGE = (
    "後面已有其他庫存異動，不能直接回復。"
    "請先下載目前主檔，手動修正後重新上傳主檔。"
    "重新上傳後一定要重設快照。"
)

_ROLLBACK_BLOCKING_LOG_ACTIONS = (
    "main_file_upload",
    "主檔編輯",
    "supplement_part",
    "order_rollback",
    "刪除不良品批次",
    "刪除加工多打批次",
    "追加不良品",
)

_BATCH_DELETE_BLOCKING_LOG_ACTIONS = _ROLLBACK_BLOCKING_LOG_ACTIONS


def ensure_dispatch_rollback_allowed(session: dict | None) -> None:
    if not session:
        return

    cutoff = str(session.get("dispatched_at") or "").strip()
    if not cutoff:
        return

    if db.get_defective_batch_summaries_after(cutoff):
        raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)

    if db.get_activity_logs_after(cutoff, actions=_ROLLBACK_BLOCKING_LOG_ACTIONS, limit=1):
        raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)


def ensure_defective_batch_delete_allowed(batch: dict | None) -> None:
    if not batch:
        return

    cutoff = str(batch.get("imported_at") or "").strip()
    if not cutoff:
        return

    batch_id = int(batch.get("id") or 0)
    if batch_id > 0 and db.get_defective_batch_summaries_after_id(batch_id):
        raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)

    if db.get_active_dispatch_sessions_after(cutoff):
        raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)

    if db.get_activity_logs_after(cutoff, actions=_BATCH_DELETE_BLOCKING_LOG_ACTIONS, limit=1):
        raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)
