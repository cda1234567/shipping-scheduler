from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import shutil
from typing import Callable

from fastapi import HTTPException

from .. import database as db
from .main_reader import read_moq
from .merge_to_main import merge_row_to_main, preview_order_batches
from .shortage_rules import (
    filter_main_write_blocking_shortages,
    get_shortage_resulting_stock,
    is_ec_part,
)
from .merge_drafts import rebuild_merge_drafts, restore_recent_committed_merge_drafts
from ..snapshot_sync import refresh_snapshot_from_main


@dataclass
class DispatchContext:
    order: dict
    groups: list[dict]
    all_components: list[dict]
    decisions: dict[str, str] = field(default_factory=dict)
    supplements: dict[str, float] = field(default_factory=dict)
    draft: dict | None = None

    @property
    def order_id(self) -> int:
        return int(self.order["id"])

    def to_preview_batch(self) -> dict:
        return {
            "order_id": self.order_id,
            "model": self.order.get("model", ""),
            "groups": self.groups,
            "supplements": dict(self.supplements),
            "decisions": dict(self.decisions),
        }

    @classmethod
    def from_value(cls, value) -> "DispatchContext":
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            return cls(
                order=dict(value.get("order") or {}),
                groups=list(value.get("groups") or []),
                all_components=list(value.get("all_components") or []),
                decisions=normalize_decisions(value.get("decisions")),
                supplements=normalize_supplements(value.get("supplements")),
                draft=dict(value.get("draft") or {}) or None,
            )
        raise TypeError("Unsupported dispatch context value")


@dataclass
class DispatchPlan:
    main_path: str
    contexts: list[DispatchContext]
    preview: dict
    use_drafts: bool = False
    supplement_allocations: dict[int, dict[str, float]] | None = None

    @property
    def order_ids(self) -> list[int]:
        return [context.order_id for context in self.contexts]

    @property
    def merged_parts(self) -> int:
        return int(self.preview.get("merged_parts") or 0)

    @property
    def shortages(self) -> list[dict]:
        return [dict(item) for item in (self.preview.get("shortages") or [])]

    @property
    def remaining_shortages(self) -> list[dict]:
        return [
            dict(item)
            for item in (self.preview.get("shortages") or [])
            if float(item.get("shortage_amount") or 0) > 0
        ]

    def to_preview_response(self) -> dict:
        return {
            "ok": True,
            "count": len(self.contexts),
            "merged_parts": self.merged_parts,
            "shortages": self.shortages,
        }


@dataclass
class DispatchCommitResult:
    plan: DispatchPlan
    results: list[dict]
    committed_draft_ids: list[int] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.results)

    @property
    def merged_parts(self) -> int:
        return sum(int(item.get("merged_parts") or 0) for item in self.results)

    @property
    def order_ids(self) -> list[int]:
        return [int(item["order_id"]) for item in self.results]

    @property
    def shortages(self) -> list[dict]:
        return self.plan.remaining_shortages


def normalize_order_ids(order_ids: list[int] | None = None) -> list[int]:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    return list(dict.fromkeys(normalized_ids))


def normalize_decisions(decisions: dict[str, str] | None = None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for part, decision in (decisions or {}).items():
        key = str(part or "").strip().upper()
        if not key or not decision:
            continue
        normalized[key] = str(decision)
    return normalized


def normalize_supplements(supplements: dict[str, float] | None = None) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for part, qty in (supplements or {}).items():
        key = str(part or "").strip().upper()
        try:
            amount = float(qty or 0)
        except (TypeError, ValueError):
            amount = 0.0
        if not key or amount <= 0:
            continue
        normalized[key] = amount
    return normalized


def normalize_supplements_preserve_keys(supplements: dict[str, float] | None = None) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for part, qty in (supplements or {}).items():
        key = str(part or "").strip()
        try:
            amount = float(qty or 0)
        except (TypeError, ValueError):
            amount = 0.0
        if not key or amount <= 0:
            continue
        normalized[key] = amount
    return normalized


def normalize_order_decisions(order_decisions: dict | None = None) -> dict[int, dict[str, str]]:
    normalized: dict[int, dict[str, str]] = {}
    for raw_order_id, decisions in (order_decisions or {}).items():
        try:
            order_id = int(raw_order_id)
        except (TypeError, ValueError):
            continue
        normalized[order_id] = normalize_decisions(decisions or {})
    return normalized


def normalize_order_supplements(order_supplements: dict | None = None) -> dict[int, dict[str, float]]:
    normalized: dict[int, dict[str, float]] = {}
    for raw_order_id, supplements in (order_supplements or {}).items():
        try:
            order_id = int(raw_order_id)
        except (TypeError, ValueError):
            continue
        normalized[order_id] = normalize_supplements(supplements or {})
    return normalized


def normalize_supplement_updates(supplements: dict[str, float] | None = None) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for part, qty in (supplements or {}).items():
        key = str(part or "").strip().upper()
        if not key:
            continue
        try:
            amount = float(qty or 0)
        except (TypeError, ValueError):
            amount = 0.0
        normalized[key] = amount
    return normalized


def normalize_order_supplement_updates(order_supplements: dict | None = None) -> dict[int, dict[str, float]]:
    normalized: dict[int, dict[str, float]] = {}
    for raw_order_id, supplements in (order_supplements or {}).items():
        try:
            order_id = int(raw_order_id)
        except (TypeError, ValueError):
            continue
        normalized[order_id] = normalize_supplement_updates(supplements or {})
    return normalized


def format_blocking_shortage_line(shortage: dict) -> str:
    part = str(shortage.get("part_number") or "").strip() or "未命名料號"
    try:
        shortage_amount = float(shortage.get("shortage_amount") or 0)
    except (TypeError, ValueError):
        shortage_amount = 0.0

    if is_ec_part(part):
        resulting_stock = get_shortage_resulting_stock(shortage)
        if resulting_stock is not None:
            return f"{part}: 寫入後結存 {resulting_stock:g}，EC 料不可為負數"
        return f"{part}: EC 料結存無法判定，暫時不能寫入主檔"

    if shortage_amount > 0:
        return f"{part}: 仍缺 {shortage_amount:g}"
    return f"{part}: 仍有缺料"


def build_main_write_block_message(shortages: list[dict]) -> str:
    lines: list[str] = []
    seen: set[str] = set()
    for item in shortages:
        part = str(item.get("part_number") or "").strip()
        if part and part in seen:
            continue
        if part:
            seen.add(part)
        lines.append(f"- {format_blocking_shortage_line(item)}")
        if len(lines) >= 6:
            break

    hidden_count = max(0, len(filter_main_write_blocking_shortages(shortages)) - len(lines))
    message = "以下料號仍不能寫入主檔，請先補料或調整決策："
    if lines:
        message += "\n" + "\n".join(lines)
    if hidden_count:
        message += f"\n- 另有 {hidden_count} 項未展開"
    return message


def ensure_main_write_allowed(shortages: list[dict]):
    blocking_shortages = filter_main_write_blocking_shortages(shortages)
    if blocking_shortages:
        raise HTTPException(400, build_main_write_block_message(blocking_shortages))


def require_existing_main_path(main_path: str | None = None) -> str:
    resolved = str(main_path or db.get_setting("main_file_path") or "").strip()
    if not resolved or not Path(resolved).exists():
        raise HTTPException(400, "請先上傳主檔")
    return resolved


def prepare_dispatch_context(order_id: int, main_path: str) -> tuple[dict, list[dict], list[dict]]:
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(404, "找不到此訂單")
    if order["status"] not in ("pending", "merged"):
        raise HTTPException(400, f"訂單狀態為 {order['status']}，無法發料")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先上傳主檔")

    model_key = (order.get("model") or "").upper()
    bom_files = db.get_bom_files_by_models([model_key])
    if not bom_files:
        raise HTTPException(400, f"機種 {order.get('model')} 沒有對應的 BOM")

    label = order.get("code") or order.get("model") or str(order_id)
    po_number = str(order.get("po_number", ""))

    groups = []
    all_components = []
    for bf in bom_files:
        comps = db.get_bom_components(bf["id"])
        groups.append({
            "batch_code": label,
            "po_number": po_number,
            "bom_model": bf["model"],
            "components": comps,
        })
        all_components.extend(comps)

    if not all_components:
        raise HTTPException(400, f"機種 {order.get('model')} 沒有 BOM 零件資料")

    return order, groups, all_components


def get_effective_moq(main_path: str) -> dict[str, float]:
    live_moq = read_moq(main_path) if main_path and Path(main_path).exists() else {}
    snapshot = db.get_snapshot()
    if snapshot:
        snapshot_moq = {part: float((row or {}).get("moq") or 0) for part, row in snapshot.items()}
        live_moq.update(snapshot_moq)
    return live_moq


def current_main_signature(main_path: str) -> str:
    return str(Path(main_path).stat().st_mtime_ns)


def load_active_merge_draft_context(draft_id: int, main_path: str) -> DispatchContext:
    draft = db.get_merge_draft(draft_id)
    if not draft or draft.get("status") != "active":
        raise HTTPException(404, "找不到可提交的副檔草稿")

    resolved_main_path = db.resolve_managed_path(str(main_path or "").strip(), "main_file_path")
    draft_main_path = db.resolve_managed_path(str(draft.get("main_file_path") or "").strip(), "main_file_path")
    if draft_main_path and Path(draft_main_path) != Path(resolved_main_path):
        raise HTTPException(400, "主檔路徑已變更，請重新整理副檔後再提交")

    if str(draft.get("main_file_mtime_ns") or "") != current_main_signature(resolved_main_path):
        raise HTTPException(400, "主檔內容已變更，請先重新整理副檔")

    order, groups, all_components = prepare_dispatch_context(int(draft["order_id"]), resolved_main_path)
    return DispatchContext(
        draft=draft,
        order=order,
        groups=groups,
        all_components=all_components,
        decisions=normalize_decisions(draft.get("decisions")),
        supplements=normalize_supplements(draft.get("supplements")),
    )


def build_context_supplement_allocations(contexts: list[DispatchContext]) -> dict[int, dict[str, float]]:
    allocations: dict[int, dict[str, float]] = {}
    for context in contexts:
        item = DispatchContext.from_value(context)
        allocations[item.order_id] = dict(item.supplements)
    return allocations


def build_dispatch_plan(
    main_path: str,
    contexts: list[DispatchContext],
    *,
    preview_builder: Callable = preview_order_batches,
    moq_map: dict[str, float] | None = None,
    st_inventory_stock: dict[str, float] | None = None,
    use_drafts: bool = False,
    supplement_allocations: dict[int, dict[str, float]] | None = None,
) -> DispatchPlan:
    normalized_contexts = [DispatchContext.from_value(item) for item in contexts]
    preview = preview_builder(
        main_path,
        [context.to_preview_batch() for context in normalized_contexts],
        {},
        moq_map=moq_map or {},
        st_inventory_stock=st_inventory_stock or {},
    )
    return DispatchPlan(
        main_path=main_path,
        contexts=normalized_contexts,
        preview=preview,
        use_drafts=use_drafts,
        supplement_allocations=supplement_allocations,
    )


def execute_dispatch_context(
    context: DispatchContext,
    main_path: str,
    *,
    merge_executor: Callable = merge_row_to_main,
    backup_dir: str,
) -> dict:
    item = DispatchContext.from_value(context)
    result = merge_executor(
        main_path=main_path,
        groups=item.groups,
        decisions=item.decisions,
        supplements=item.supplements,
        backup_dir=backup_dir,
    )

    session = None
    try:
        session = db.save_dispatch_session(
            order_id=item.order_id,
            previous_status=item.order["status"],
            backup_path=result.get("backup_path") or "",
            main_file_path=main_path,
        )

        dispatch_records = []
        for comp in item.all_components:
            if comp.get("is_dash") or comp.get("needed_qty", 0) <= 0:
                continue
            part_number = str(comp.get("part_number") or "")
            dispatch_records.append({
                "part_number": part_number,
                "needed_qty": comp["needed_qty"],
                "prev_qty_cs": comp.get("prev_qty_cs", 0),
                "decision": item.decisions.get(part_number.strip().upper(), "None"),
            })
        db.save_dispatch_records(item.order_id, dispatch_records)
        db.update_order(item.order_id, status="dispatched")
    except Exception:
        backup_path = Path(str(result.get("backup_path") or "")).expanduser()
        if backup_path.exists():
            shutil.copy2(backup_path, main_path)
        if session:
            db.delete_dispatch_records_for_orders([item.order_id])
            db.mark_dispatch_sessions_rolled_back([int(session["id"])])
            db.update_order(item.order_id, status=item.order["status"], folder=item.order.get("folder", ""))
        raise

    db.log_activity(
        "order_dispatched",
        f"訂單 {item.order['po_number']} ({item.order['model']}) 已發料，{result['merged_parts']} 筆 merge",
    )
    return {
        "ok": True,
        "order_id": item.order_id,
        "merged_parts": result["merged_parts"],
        "backup_path": result["backup_path"],
        "session": session,
    }


def rollback_dispatch_sessions(sessions: list[dict]) -> dict:
    if not sessions:
        raise HTTPException(400, "找不到可反悔的發料紀錄")

    normalized_sessions = [dict(session) for session in sessions if session]
    if not normalized_sessions:
        raise HTTPException(400, "找不到可反悔的發料紀錄")

    first_session = normalized_sessions[0]
    backup_path = Path(str(first_session.get("backup_path") or "")).expanduser()
    if not backup_path.exists():
        raise HTTPException(400, "找不到這次發料的主檔備份，無法反悔")

    current_main_path = db.resolve_managed_path(str(db.get_setting("main_file_path") or "").strip(), "main_file_path")
    session_paths = {
        db.resolve_managed_path(str(session.get("main_file_path") or "").strip(), "main_file_path")
        for session in normalized_sessions
        if str(session.get("main_file_path") or "").strip()
    }
    if len(session_paths) > 1:
        raise HTTPException(400, "這批發料使用了不同主檔，無法自動反悔")

    session_main_path = next(iter(session_paths), "")
    restore_target = session_main_path or current_main_path
    if not restore_target:
        raise HTTPException(400, "找不到目前主檔路徑，無法反悔")
    if current_main_path and session_main_path and Path(current_main_path) != Path(session_main_path):
        raise HTTPException(400, "目前主檔已更換，請確認後再反悔")

    restore_target_path = Path(restore_target)
    restore_target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(backup_path, restore_target_path)
    refresh_snapshot_from_main(str(restore_target_path))

    order_ids = [int(session["order_id"]) for session in normalized_sessions]
    session_ids = [int(session["id"]) for session in normalized_sessions]
    db.delete_dispatch_records_for_orders(order_ids)
    db.mark_dispatch_sessions_rolled_back(session_ids)

    restored_orders = []
    for session in normalized_sessions:
        order_id = int(session["order_id"])
        order = db.get_order(order_id)
        restore_status = session.get("previous_status") or "merged"
        db.update_order(order_id, status=restore_status, folder="")
        restored_orders.append({
            "id": order_id,
            "po_number": order.get("po_number", "") if order else "",
            "model": order.get("model", "") if order else "",
            "status": order.get("status", "") if order else "",
            "restore_status": restore_status,
        })

    restored_draft_orders = restore_recent_committed_merge_drafts(order_ids)

    return {
        "count": len(restored_orders),
        "restored_from": str(backup_path),
        "main_file_path": str(restore_target_path),
        "orders": restored_orders,
        "restored_draft_order_ids": restored_draft_orders,
        "restored_draft_count": len(restored_draft_orders),
    }


def commit_dispatch_plan(
    plan: DispatchPlan,
    *,
    merge_executor: Callable = merge_row_to_main,
    backup_dir: str,
    rollback_executor: Callable = rollback_dispatch_sessions,
    execute_dispatcher: Callable | None = None,
    snapshot_refresher: Callable = refresh_snapshot_from_main,
) -> DispatchCommitResult:
    results: list[dict] = []
    processed_sessions: list[dict] = []
    committed_draft_ids: list[int] = []

    try:
        for context in plan.contexts:
            if execute_dispatcher is not None:
                result = execute_dispatcher(
                    context.order,
                    context.groups,
                    context.all_components,
                    plan.main_path,
                    context.decisions,
                    context.supplements,
                )
            else:
                result = execute_dispatch_context(
                    context,
                    plan.main_path,
                    merge_executor=merge_executor,
                    backup_dir=backup_dir,
                )
            results.append(result)
            if result.get("session"):
                processed_sessions.append(result["session"])
            if context.draft:
                committed_draft_ids.append(int(context.draft["id"]))
    except Exception:
        if processed_sessions:
            rollback_executor(processed_sessions)
        raise

    if plan.supplement_allocations is not None:
        db.replace_order_supplements(plan.order_ids, plan.supplement_allocations)

    if plan.use_drafts and committed_draft_ids:
        for draft_id in committed_draft_ids:
            db.mark_merge_draft_committed(draft_id)
        remaining_active_orders = normalize_order_ids([
            item.get("order_id")
            for item in db.get_active_merge_drafts()
            if item.get("order_id") is not None
        ])
        if remaining_active_orders:
            rebuild_merge_drafts(remaining_active_orders)

    snapshot_refresher(plan.main_path)
    return DispatchCommitResult(plan=plan, results=results, committed_draft_ids=committed_draft_ids)
