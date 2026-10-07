from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path

import openpyxl
from fastapi import HTTPException

from .. import database as db

RESTORE_BLOCKED_MESSAGE = (
    "後面已有其他庫存異動，不能直接回復。"
    "請先下載目前主檔，手動修正後重新上傳主檔。"
    "重新上傳後一定要重設快照。"
)
ABSORBED_HISTORY_MESSAGE = "這筆紀錄已被盤點數量吸收，只能保留查帳，不能再刪除或回復庫存。"
COUNT_HISTORY_MESSAGE = "此料號已完成盤點，舊扣帳無法逐筆對應主檔，不能直接刪除或補回，請先核對。"
COUNT_COMMENT_PREFIX = "inventory-count-boundary:"


@lru_cache(maxsize=8)
def _read_count_boundaries(path: str, modified_ns: int, size: int) -> tuple:
    workbook = openpyxl.load_workbook(path, read_only=False)
    try:
        boundaries = []
        for cell in workbook.worksheets[0][1]:
            comment = cell.comment
            if comment and comment.text.startswith(COUNT_COMMENT_PREFIX):
                boundaries.append(json.loads(comment.text[len(COUNT_COMMENT_PREFIX):]))
        return tuple(boundaries)
    finally:
        workbook.close()


def get_count_absorbed_record_ids() -> set[int]:
    path = db.get_setting('main_file_path')
    if not path or not Path(path).is_file():
        return set()
    stat = Path(path).stat()
    return {int(record_id)
            for boundary in _read_count_boundaries(path, stat.st_mtime_ns, stat.st_size)
            for record_id in boundary.get('absorbed_record_ids', [])}


def is_count_protected_record(record: dict) -> bool:
    path = db.get_setting('main_file_path')
    if not path or not Path(path).is_file():
        return False
    stat = Path(path).stat()
    return any(
        str(record.get('part_number') or '').strip().upper() in boundary['parts']
        and str(record.get('created_at') or '') > boundary['cutoff_at']
        and 0 < int(record.get('id') or 0) <= boundary['max_record_id']
        for boundary in _read_count_boundaries(path, stat.st_mtime_ns, stat.st_size)
    )


OLD_PERIOD_HISTORY_MESSAGE = "這筆紀錄屬於舊年度主檔，只能保留查帳，不能回復到目前主檔。"

_ROLLBACK_BLOCKING_LOG_ACTIONS = (
    "main_file_upload",
    "inventory_count_undone",
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

    if any(is_count_protected_record(row) for row in (batch.get("items") or [])):
        raise HTTPException(400, COUNT_HISTORY_MESSAGE)

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
        actions=("刪除不良品批次", "刪除加工多打批次", "inventory_count_undone"),
        limit=1,
    )
    if rows:
        raise HTTPException(400, "退回後曾刪除不良品或加工多打批次，無法安全一鍵補回，請手動核對主檔。")

    active_records = db.get_defective_records_after(normalized_cutoff)
    if not active_records and db.get_defective_records_after(normalized_cutoff, include_count_absorbed=True):
        raise HTTPException(400, ABSORBED_HISTORY_MESSAGE)
    if any(is_count_protected_record(row) for row in active_records):
        raise HTTPException(400, COUNT_HISTORY_MESSAGE)
