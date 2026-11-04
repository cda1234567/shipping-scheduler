from __future__ import annotations

from functools import lru_cache
import json
import re
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
_REPLAY_LOG_ACTION = "補回退回不良品扣帳"
_ORDER_ROLLBACK_LOG_ACTION = "order_rollback"
_MAIN_EDIT_LOG_ACTION = "主檔編輯"
_REPLAY_CUTOFF_RE = re.compile(r"^\s*(\S+)\s*之後")
_MAIN_EDIT_CELL_RE = re.compile(r"^\s*(?:\[(?P<sheet>[^\]]*)\]\s*)?R(?P<row>\d+)C(?P<col>\d+)\s*:\s*(?P<rest>.*)$", re.S)
BATCH_NOT_APPLIED_MESSAGE = (
    "目前主檔找不到這批扣帳（可能已被退回 / 覆蓋或欄位被改過），"
    "不能自動回復庫存，請先核對主檔。"
)


def parse_main_edit_detail(detail: str) -> dict | None:
    """解析「主檔編輯」紀錄：[Sheet] R{row}C{col}: old → new（料號 X）。"""
    match = _MAIN_EDIT_CELL_RE.match(str(detail or ""))
    if not match:
        return None
    rest = match.group("rest")
    part = ""
    part_match = re.search(r"（料號\s*([^）]+)）\s*$", rest)
    if part_match:
        part = part_match.group(1).strip().upper()
        rest = rest[:part_match.start()].rstrip()
    old_value, sep, new_value = rest.partition("→")
    return {
        "sheet": match.group("sheet") or "",
        "row": int(match.group("row")),
        "col": int(match.group("col")),
        "old_value": old_value.strip() if sep else rest.strip(),
        "new_value": new_value.strip() if sep else "",
        "part_number": part,
    }


def _replay_anchor_for_batch(imported_at: str, logs: list[dict]) -> tuple[str, str]:
    """找出「強制退回後再補回、而且補回範圍涵蓋這批」的最後一次補回時間。

    回傳 (補回時間, 該補回之前最後一次 order_rollback 時間)；沒有就回傳 ("", "")。
    """
    anchor = ""
    rollback_before_anchor = ""
    last_rollback = ""
    for log in logs:
        action = str(log.get("action") or "")
        created_at = str(log.get("created_at") or "")
        if action == _ORDER_ROLLBACK_LOG_ACTION:
            last_rollback = created_at
        elif action == _REPLAY_LOG_ACTION and last_rollback:
            match = _REPLAY_CUTOFF_RE.match(str(log.get("detail") or ""))
            if match and match.group(1) < imported_at:
                anchor = created_at
                rollback_before_anchor = last_rollback
    return anchor, rollback_before_anchor


def check_defective_batch_delete(batch: dict | None, *, main_path: str | None = None) -> dict:
    """刪除批次前檢查（唯讀）。擋住時 raise HTTPException。

    回傳 {"relaxed": bool, "applied": dict | None}：relaxed=True 表示後面雖有強制退回 / 主檔編輯，
    但已確認不衝突，而且這批扣帳目前確實在主檔（applied 為定位結果），刪除時必須回寫庫存。
    """
    outcome = {"relaxed": False, "applied": None}
    if not batch:
        return outcome

    if any(is_count_protected_record(row) for row in (batch.get("items") or [])):
        raise HTTPException(400, COUNT_HISTORY_MESSAGE)

    if any(int(row.get("absorbed_by_alignment_id") or 0) > 0 for row in (batch.get("items") or [])):
        raise HTTPException(400, ABSORBED_HISTORY_MESSAGE)
    if int(batch.get("main_period_id") or 0) != db.get_current_main_period_id():
        raise HTTPException(400, OLD_PERIOD_HISTORY_MESSAGE)

    cutoff = str(batch.get("imported_at") or "").strip()
    if not cutoff:
        return outcome

    batch_id = int(batch.get("id") or 0)
    if batch_id > 0 and db.get_defective_batch_summaries_after_id(batch_id):
        raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)

    if db.get_active_dispatch_sessions_after(cutoff):
        raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)

    logs = db.get_activity_logs_after(
        cutoff,
        actions=_BATCH_DELETE_BLOCKING_LOG_ACTIONS + (_REPLAY_LOG_ACTION,),
        limit=100000,
    )
    blocking = [log for log in logs if str(log.get("action") or "") != _REPLAY_LOG_ACTION]
    if not blocking:
        return outcome

    anchor, rollback_before_anchor = _replay_anchor_for_batch(cutoff, logs)
    located: dict | None = None

    def ensure_applied() -> dict:
        nonlocal located
        if located is None:
            path = main_path if main_path is not None else str(db.get_setting("main_file_path") or "").strip()
            from .defective_deduction import locate_applied_batch_deduction
            located = locate_applied_batch_deduction(path, batch)
        if located.get("status") != "applied":
            raise HTTPException(400, BATCH_NOT_APPLIED_MESSAGE)
        return located

    for log in blocking:
        action = str(log.get("action") or "")
        created_at = str(log.get("created_at") or "")
        if action == _ORDER_ROLLBACK_LOG_ACTION and anchor and created_at < anchor:
            # 強制退回沖掉這批後，已經用「補回」重新扣回，不再算衝突。
            continue
        if action == _MAIN_EDIT_LOG_ACTION:
            if anchor and created_at < rollback_before_anchor:
                # 這筆編輯已被強制退回的備份覆蓋，不在目前主檔裡。
                continue
            edit = parse_main_edit_detail(log.get("detail") or "")
            if not edit:
                raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)
            start_col = ensure_applied()["columns"][0]
            if start_col <= edit["col"] <= start_col + 2:
                # 直接改到這批扣帳欄位，不能自動回復。
                raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)
            continue
        raise HTTPException(400, RESTORE_BLOCKED_MESSAGE)

    outcome["relaxed"] = True
    outcome["applied"] = ensure_applied()
    return outcome


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


def ensure_defective_batch_delete_allowed(batch: dict | None) -> dict:
    return check_defective_batch_delete(batch)


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
