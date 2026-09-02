from __future__ import annotations

from fastapi import HTTPException

from .. import database as db

RESTORE_BLOCKED_MESSAGE = (
    "後面已有其他庫存異動，不能直接回復。"
    "請先下載目前主檔，手動修正後重新上傳主檔。"
    "重新上傳後一定要重設快照。"
)
ABSORBED_HISTORY_MESSAGE = "這筆紀錄已被盤點數量吸收，只能保留查帳，不能再刪除或回復庫存。"
OLD_PERIOD_HISTORY_MESSAGE = "這筆紀錄屬於舊年度主檔，只能保留查帳，不能回復到目前主檔。"

_ROLLBACK_BLOCKING_LOG_ACTIONS = (
    "main_file_upload",
    "主檔編輯",
    "supplement_part",
    "刪除不良品批次",
    "刪除加工多打批次",
    "追加不良品",
)

_BATCH_DELETE_BLOCKING_LOG_ACTIONS = _ROLLBACK_BLOCKING_LOG_ACTIONS + ("order_rollback",)


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

    if any(int(row.get("absorbed_by_alignment_id") or 0) > 0 for row in (batch.get("items") or [])):
        raise HTTPException(400, ABSORBED_HISTORY_MESSAGE)
    if int(batch.get("main_period_id") or 0) != db.get_current_main_period_id():
        raise HTTPException(400, OLD_PERIOD_HISTORY_MESSAGE)

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


def ensure_defective_replay_allowed(cutoff: str) -> None:
    normalized_cutoff = str(cutoff or "").strip()
    if not normalized_cutoff:
        raise HTTPException(400, "缺少重放截止時間")

    replay_rows = db.get_activity_logs_after(
        normalized_cutoff,
        actions=("補回退回不良品扣帳",),
        limit=1,
    )
    if replay_rows:
        raise HTTPException(400, "這批不良品扣帳已補回過，請勿重複補回。")

    rows = db.get_activity_logs_after(
        normalized_cutoff,
        actions=("刪除不良品批次", "刪除加工多打批次"),
        limit=1,
    )
    if rows:
        raise HTTPException(400, "退回後曾刪除不良品或加工多打批次，無法安全一鍵補回，請手動核對主檔。")
