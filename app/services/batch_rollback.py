"""已發料清單「退回選取的單」（v2026.11.04.7）。

規則（Andy）：
- 依發料時間由新到舊，一筆一筆往回做；每筆用 v2026.11.04.6 的精準反悔（只撤銷那筆的主檔欄位）。
- 執行前先試算：列出哪些可精準退回、哪些只能整檔還原。只要有任何一筆需整檔還原，就先停下來給使用者看，
  使用者明確確認後才繼續——批次只做精準退回；需整檔還原的單不在批次裡動（整檔還原會連帶還原之後所有發料，
  請到該單逐筆處理）。
- 中途任何一筆失敗（主檔鎖定 / 被其他人同時寫入 / 主檔讀不到 / 欄位比對不符…）就停止剩下的，
  回報哪些已退回、哪些沒有。
"""
from __future__ import annotations

import logging
import os
from typing import Callable, Optional

from .. import database as db
from .dispatch_precise_rollback import evaluate_precise_rollback
from .main_event_rollback import PreciseRollbackUnavailable

log = logging.getLogger(__name__)

MAX_BATCH = 100


class BatchRollbackError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def _order_label(order: dict) -> dict:
    return {
        "order_id": int(order["id"]),
        "po_number": str(order.get("po_number") or ""),
        "model": str(order.get("model") or ""),
        "code": str(order.get("code") or ""),
    }


def _normalize_ids(order_ids) -> list[int]:
    out: list[int] = []
    for value in order_ids or []:
        try:
            oid = int(value)
        except (TypeError, ValueError):
            continue
        if oid > 0 and oid not in out:
            out.append(oid)
    return out


def collect_targets(order_ids) -> list[dict]:
    """把選取的單整理成 [{order, session}]，依發料時間由新到舊排序。不能退回的單直接報錯（整批不動）。"""
    ids = _normalize_ids(order_ids)
    if not ids:
        raise BatchRollbackError(400, "請先勾選要退回的單")
    if len(ids) > MAX_BATCH:
        raise BatchRollbackError(400, f"一次最多退回 {MAX_BATCH} 筆")
    targets: list[dict] = []
    problems: list[str] = []
    for oid in ids:
        order = db.get_order(oid)
        if not order:
            problems.append(f"#{oid} 找不到訂單")
            continue
        if order.get("status") not in ("dispatched", "completed"):
            problems.append(f"{order.get('po_number') or oid} 不是已發料狀態")
            continue
        session = db.get_active_dispatch_session(oid)
        if not session:
            problems.append(f"{order.get('po_number') or oid} 找不到發料歷史")
            continue
        targets.append({"order": order, "session": session})
    if problems:
        raise BatchRollbackError(400, "以下訂單不能退回，整批未執行：" + "、".join(problems))
    targets.sort(key=lambda t: (str(t["session"].get("dispatched_at") or ""), int(t["session"]["id"])), reverse=True)
    return targets


def plan(order_ids, *, evaluator: Optional[Callable[[dict], dict]] = None) -> dict:
    """唯讀試算（不動主檔 / 資料庫）。回傳依執行順序（新→舊）的清單與是否有需整檔還原的單。"""
    evaluator = evaluator or evaluate_precise_rollback
    items = []
    for t in collect_targets(order_ids):
        evaluation = evaluator(t["session"]) or {}
        ok = bool(evaluation.get("ok"))
        items.append({
            **_order_label(t["order"]),
            "session_id": int(t["session"]["id"]),
            "dispatched_at": str(t["session"].get("dispatched_at") or ""),
            "mode": "precise" if ok else "full",
            "reason": "" if ok else str(evaluation.get("reason") or "無法精準退回"),
        })
    precise = [i for i in items if i["mode"] == "precise"]
    full = [i for i in items if i["mode"] == "full"]
    return {
        "ok": True,
        "orders": items,
        "precise_count": len(precise),
        "full_count": len(full),
        "needs_full_restore": bool(full),
    }


def _mtime(path: str) -> Optional[int]:
    try:
        return os.stat(path).st_mtime_ns
    except OSError:
        return None


def execute(
    order_ids,
    *,
    confirm: bool,
    acknowledge_full: bool = False,
    rollback_one: Callable[[dict], dict],
    after_each: Optional[Callable[[dict, dict], None]] = None,
    evaluator: Optional[Callable[[dict], dict]] = None,
) -> dict:
    """依新→舊逐筆精準退回。需 confirm=True；有需整檔還原的單時還需 acknowledge_full=True（那些單會略過不動）。"""
    if confirm is not True:
        raise BatchRollbackError(400, "需確認後才會退回（confirm=true）")
    checked = plan(order_ids, evaluator=evaluator)
    if checked["needs_full_restore"] and not acknowledge_full:
        return {**checked, "ok": False, "stopped": True, "executed": False,
                "message": "有訂單只能整檔還原，已停止；請確認後再執行（這些單不會在批次中處理）",
                "rolled_back": [], "not_rolled_back": checked["orders"], "skipped_full": []}

    by_id = {int(t["order"]["id"]): t for t in collect_targets(order_ids)}
    rolled_back: list[dict] = []
    skipped_full: list[dict] = []
    failure: Optional[dict] = None
    last_main_path = ""
    last_mtime: Optional[int] = None
    pending = list(checked["orders"])
    while pending:
        item = pending.pop(0)
        if item["mode"] == "full":
            skipped_full.append(item)
            continue
        target = by_id[item["order_id"]]
        session = db.get_active_dispatch_session(item["order_id"])
        if not session or int(session["id"]) != item["session_id"]:
            failure = {**item, "error": "發料紀錄已變動（可能有人同時操作），請重新整理"}
            break
        main_path = str(session.get("main_file_path") or last_main_path or "")
        if last_mtime is not None and main_path == last_main_path and _mtime(main_path) != last_mtime:
            failure = {**item, "error": "主檔在批次退回期間被其他人寫入，已停止"}
            break
        try:
            result = rollback_one(session)
        except PreciseRollbackUnavailable as exc:
            failure = {**item, "error": f"無法精準退回：{exc.reason}"}
            break
        except PermissionError as exc:
            failure = {**item, "error": f"主檔被鎖定或無法寫入（可能有人用 Excel 開著）：{exc}"}
            break
        except Exception as exc:  # 讀不到主檔、資料庫錯誤…
            log.warning("batch rollback failed on order %s: %s", item["order_id"], exc)
            failure = {**item, "error": f"退回失敗：{exc}"}
            break
        rolled_back.append({**item, "main_backup_path": str((result or {}).get("main_backup_path") or "")})
        last_main_path = str((result or {}).get("main_file_path") or main_path)
        last_mtime = _mtime(last_main_path)
        if after_each:
            try:
                after_each(target["order"], result or {})
            except Exception as exc:
                log.warning("batch rollback post-step failed: %s", exc)

    not_done = ([failure] if failure else []) + [i for i in pending if i["mode"] == "precise"]
    return {
        "ok": failure is None,
        "executed": True,
        "stopped": failure is not None,
        "orders": checked["orders"],
        "rolled_back": rolled_back,
        "not_rolled_back": not_done + [i for i in pending if i["mode"] == "full"],
        "skipped_full": skipped_full,
        "failure": failure,
        "message": (
            f"已退回 {len(rolled_back)} 筆" + (f"；第 {len(rolled_back) + 1} 筆失敗，剩下的已停止" if failure else "")
            + (f"；{len(skipped_full)} 筆需整檔還原未處理" if skipped_full else "")
        ),
    }
