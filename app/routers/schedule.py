"""
排程與訂單管理路由 — 四階段狀態 + 已發料隔離 + 批次 merge。

訂單狀態: pending → merged → dispatched → completed
                                    ↘ cancelled
"""
from __future__ import annotations
import logging
import shutil
import time
from datetime import datetime, date

log = logging.getLogger(__name__)
from dateutil.relativedelta import relativedelta
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, HTTPException
from ..config import SCHEDULE_DIR, BACKUP_DIR, cfg
from ..services.main_reader import find_legacy_snapshot_stock_fixes, read_moq, read_stock
from ..services.schedule_parser import parse_schedule
from ..services.calculator import run as calc_run
from ..services.merge_to_main import merge_row_to_main, preview_order_batches, supplement_part_in_main
from ..services.order_decisions import build_order_decision_allocations
from ..services.order_supplements import build_order_supplement_allocations
from ..services.shortage_rules import is_order_scoped_shortage_part
from ..services.dispatch_pipeline import (
    DispatchContext,
    build_context_supplement_allocations,
    build_dispatch_plan,
    commit_dispatch_plan,
    current_main_signature as _current_main_signature,
    ensure_main_write_allowed as _ensure_main_write_allowed,
    execute_dispatch_context,
    get_effective_moq as _get_effective_moq,
    normalize_decisions as _normalize_decisions,
    normalize_order_decisions as _normalize_order_decisions,
    normalize_order_ids as _normalize_order_ids,
    normalize_order_supplements as _normalize_order_supplements,
    normalize_order_supplement_updates as _normalize_order_supplement_updates,
    normalize_supplements as _normalize_supplements,
    prepare_dispatch_context as _prepare_dispatch_context,
    require_existing_main_path as _require_existing_main_path,
)
from ..services.inventory_restore_guard import ensure_dispatch_rollback_allowed
from ..services.merge_drafts import (
    rebuild_merge_drafts,
    delete_merge_draft_and_refresh,
    get_schedule_draft_map,
    get_committed_schedule_draft_map,
    get_draft_detail,
    download_merge_draft,
    download_selected_merge_drafts,
    restore_recent_committed_merge_drafts,
)
from ..snapshot_sync import refresh_snapshot_from_main
from ..models import (
    ReorderRequest, UpdateDeliveryRequest, BatchMergeRequest,
    BatchDispatchRequest, DecisionRequest, RowCodeRequest, UpdateModelRequest, AlertType,
    SupplementPartRequest,
)
from .. import database as db

router = APIRouter()


def _repair_legacy_snapshot_if_needed(main_path: str) -> dict[str, dict]:
    snapshot = db.get_snapshot()
    fixes = find_legacy_snapshot_stock_fixes(main_path, snapshot)
    repaired = db.update_snapshot_stock(fixes)
    if repaired:
        db.log_activity("snapshot_repaired", f"自動修正舊版快照庫存 {repaired} 筆")
        for part, qty in fixes.items():
            if part in snapshot:
                snapshot[part]["stock_qty"] = qty
    return snapshot


def _get_active_dispatched_consumption() -> dict[str, float]:
    snapshot_at = db.get_snapshot_taken_at()
    return db.get_all_dispatched_consumption(snapshot_at)


def _get_st_inventory_stock() -> dict[str, float]:
    return db.get_st_inventory_stock()


def _build_rollback_preview(order_id: int, *, force: bool = False) -> tuple[dict, dict, list[dict]]:
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(404, "找不到此訂單")
    if order["status"] not in ("dispatched", "completed"):
        raise HTTPException(400, "只能反悔已發料訂單")

    session = db.get_active_dispatch_session(order_id)
    if not session:
        raise HTTPException(400, "找不到這筆訂單的發料歷史，無法反悔")
    if not force:
        ensure_dispatch_rollback_allowed(session)

    tail_sessions = db.get_dispatch_session_tail(int(session["id"]))
    if not tail_sessions:
        raise HTTPException(400, "找不到可反悔的發料紀錄")

    affected_orders = []
    for row in tail_sessions:
        target = db.get_order(int(row["order_id"]))
        if not target:
            continue
        affected_orders.append({
            "id": int(target["id"]),
            "po_number": target.get("po_number", ""),
            "model": target.get("model", ""),
            "status": target.get("status", ""),
            "restore_status": row.get("previous_status") or "merged",
        })

    if not affected_orders:
        raise HTTPException(400, "找不到可反悔的訂單資料")

    return order, session, affected_orders


def _merge_order_decision_allocations(
    order_ids: list[int],
    decisions: dict[str, str] | None = None,
    order_decisions: dict[int, dict[str, str]] | None = None,
    *,
    include_none: bool = False,
) -> dict[int, dict[str, str]]:
    merged = build_order_decision_allocations(
        order_ids,
        decisions or {},
        include_none=include_none,
    )
    for order_id in order_ids:
        scoped = _normalize_decisions((order_decisions or {}).get(order_id) or {})
        if not scoped:
            merged.setdefault(order_id, {})
            continue
        merged.setdefault(order_id, {}).update(scoped)
    return merged


def _merge_order_supplement_allocations(
    order_ids: list[int],
    supplements: dict[str, float] | None = None,
    order_supplements: dict[int, dict[str, float]] | None = None,
) -> dict[int, dict[str, float]]:
    merged = build_order_supplement_allocations(order_ids, supplements or {})
    for order_id in order_ids:
        scoped = _normalize_supplements((order_supplements or {}).get(order_id) or {})
        if not scoped:
            merged.setdefault(order_id, {})
            continue
        current = dict(merged.get(order_id, {}))
        for part, qty in scoped.items():
            if is_order_scoped_shortage_part(part):
                current[part] = qty
            else:
                # 非 order-scoped 料號由全域分配決定，per-order 只在全域沒分配時才補上
                if part not in current:
                    current[part] = qty
        merged[order_id] = current
    return merged


def _merge_decision_updates(order_id: int, updates: dict[str, str] | None = None) -> dict[str, str]:
    merged = _normalize_decisions(db.get_order_decisions([order_id]).get(order_id, {}))
    for part, decision in (updates or {}).items():
        key = str(part or "").strip().upper()
        value = str(decision or "").strip() or "None"
        if not key:
            continue
        if value == "None":
            merged.pop(key, None)
            continue
        merged[key] = value
    return merged


def _merge_supplement_updates(order_id: int, updates: dict[str, float] | None = None) -> dict[str, float]:
    merged = _normalize_supplements((db.get_order_supplements([order_id]).get(order_id) or {}))
    for part, qty in (updates or {}).items():
        key = str(part or "").strip().upper()
        if not key:
            continue
        try:
            amount = float(qty or 0)
        except (TypeError, ValueError):
            amount = 0.0
        if amount <= 0:
            merged.pop(key, None)
            continue
        merged[key] = amount
    return merged


def _normalize_order_supplement_notes(value: dict | None = None) -> dict[int, dict[str, str]]:
    normalized: dict[int, dict[str, str]] = {}
    for raw_order_id, part_notes in (value or {}).items():
        try:
            order_id = int(raw_order_id)
        except (TypeError, ValueError):
            continue
        notes: dict[str, str] = {}
        for raw_part, raw_note in (part_notes or {}).items():
            part = str(raw_part or "").strip().upper()
            if not part:
                continue
            notes[part] = str(raw_note or "").strip()
        normalized[order_id] = notes
    return normalized


def _apply_request_overrides_to_contexts(
    contexts: list[DispatchContext],
    order_ids: list[int],
    req: BatchDispatchRequest,
) -> tuple[list[DispatchContext], dict[int, dict[str, float]]]:
    normalized_contexts = [DispatchContext.from_value(context) for context in contexts]
    if not normalized_contexts:
        return [], {}

    scoped_decisions = _normalize_order_decisions(req.order_decisions)
    scoped_supplements = _normalize_order_supplements(req.order_supplements)
    if not scoped_decisions and not scoped_supplements:
        return normalized_contexts, build_context_supplement_allocations(normalized_contexts)

    updated_contexts: list[DispatchContext] = []
    for context in normalized_contexts:
        decisions = dict(context.decisions or {})
        supplements = dict(context.supplements or {})
        order_id = context.order_id
        order_decision_updates = scoped_decisions.get(order_id, {})
        order_supplement_updates = scoped_supplements.get(order_id, {})

        for part, decision in order_decision_updates.items():
            if decision == "None":
                decisions.pop(part, None)
            else:
                decisions[part] = decision

        visible_parts = set(order_decision_updates) | set(order_supplement_updates)
        if visible_parts:
            for part in visible_parts:
                qty = float(order_supplement_updates.get(part, 0) or 0)
                decision = order_decision_updates.get(part, decisions.get(part, "None"))
                if qty > 0 and decision != "Shortage":
                    supplements[part] = qty
                else:
                    supplements.pop(part, None)

        updated_contexts.append(DispatchContext(
            draft=context.draft,
            order=context.order,
            groups=context.groups,
            all_components=context.all_components,
            decisions=decisions,
            supplements=supplements,
        ))

    return updated_contexts, build_context_supplement_allocations(updated_contexts)


def _execute_dispatch(
    order: dict,
    groups: list[dict],
    all_components: list[dict],
    main_path: str,
    decisions: dict[str, str],
    supplements: dict[str, float] | None = None,
) -> dict:
    return execute_dispatch_context(
        DispatchContext(
            order=order,
            groups=groups,
            all_components=all_components,
            decisions=decisions,
            supplements=supplements or {},
        ),
        main_path,
        merge_executor=merge_row_to_main,
        backup_dir=str(BACKUP_DIR),
    )


def _load_active_merge_draft_context(draft_id: int, main_path: str):
    draft = db.get_merge_draft(draft_id)
    if not draft or draft.get("status") != "active":
        raise HTTPException(404, "找不到可提交的副檔草稿")

    resolved_main_path = db.resolve_managed_path(str(main_path or "").strip(), "main_file_path")
    draft_main_path = db.resolve_managed_path(str(draft.get("main_file_path") or "").strip(), "main_file_path")
    if draft_main_path and Path(draft_main_path) != Path(resolved_main_path):
        raise HTTPException(400, "主檔路徑已變更，請重新整理副檔後再提交")

    if str(draft.get("main_file_mtime_ns") or "") != _current_main_signature(resolved_main_path):
        raise HTTPException(400, "主檔內容已變更，請先重新整理副檔")

    order_id = int(draft["order_id"])
    order, groups, all_components = _prepare_dispatch_context(order_id, resolved_main_path)
    return DispatchContext(
        draft=draft,
        order=order,
        groups=groups,
        all_components=all_components,
        decisions=_normalize_decisions(db.get_order_decisions([order_id]).get(order_id, {})),
        supplements=_normalize_supplements((db.get_order_supplements([order_id]).get(order_id) or {})),
    )


def _rollback_dispatch_sessions(sessions: list[dict]) -> dict:
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


def _resolve_single_order_dispatch_plan(order_id: int, req: DecisionRequest, main_path: str):
    order, groups, all_components = _prepare_dispatch_context(order_id, main_path)
    decisions = _normalize_decisions(req.decisions)
    supplements = _normalize_supplements(req.supplements)
    context = DispatchContext(
        order=order,
        groups=groups,
        all_components=all_components,
        decisions=decisions,
        supplements=supplements,
    )
    supplement_allocations = (
        build_order_supplement_allocations([order_id], supplements)
        if supplements
        else None
    )
    return build_dispatch_plan(
        main_path,
        [context],
        preview_builder=preview_order_batches,
        moq_map=_get_effective_moq(main_path),
        st_inventory_stock=_get_st_inventory_stock(),
        supplement_allocations=supplement_allocations,
    )


def _resolve_batch_dispatch_plan(req: BatchDispatchRequest, normalized_order_ids: list[int], main_path: str):
    draft_id_map = db.get_active_merge_draft_ids_by_order_ids(normalized_order_ids)
    missing_orders = [order_id for order_id in normalized_order_ids if order_id not in draft_id_map]
    if missing_orders and draft_id_map:
        raise HTTPException(400, "有訂單還沒有副檔草稿，請先 merge 生成副檔")

    if draft_id_map:
        rebuild_merge_drafts(normalized_order_ids)
        contexts = [
            DispatchContext.from_value(_load_active_merge_draft_context(draft_id_map[order_id], main_path))
            for order_id in normalized_order_ids
        ]
        contexts, supplement_allocations = _apply_request_overrides_to_contexts(contexts, normalized_order_ids, req)
        use_drafts = True
    else:
        decisions = _normalize_decisions(req.decisions)
        supplements = _normalize_supplements(req.supplements)
        order_decisions = _normalize_order_decisions(req.order_decisions)
        order_supplements = _normalize_order_supplements(req.order_supplements)
        decision_allocations = _merge_order_decision_allocations(
            normalized_order_ids,
            decisions,
            order_decisions,
            include_none=True,
        )
        supplement_allocations = _merge_order_supplement_allocations(
            normalized_order_ids,
            supplements,
            order_supplements,
        )
        contexts = []
        for order_id in normalized_order_ids:
            order, groups, all_components = _prepare_dispatch_context(order_id, main_path)
            contexts.append(DispatchContext(
                order=order,
                groups=groups,
                all_components=all_components,
                decisions=decision_allocations.get(int(order["id"]), {}),
                supplements=supplement_allocations.get(int(order["id"]), {}),
            ))
        use_drafts = False

    return build_dispatch_plan(
        main_path,
        contexts,
        preview_builder=preview_order_batches,
        moq_map=_get_effective_moq(main_path),
        st_inventory_stock=_get_st_inventory_stock(),
        use_drafts=use_drafts,
        supplement_allocations=supplement_allocations,
    )


def _resolve_draft_commit_plan(draft_id: int, main_path: str):
    context = DispatchContext.from_value(_load_active_merge_draft_context(draft_id, main_path))
    return build_dispatch_plan(
        main_path,
        [context],
        preview_builder=preview_order_batches,
        moq_map=_get_effective_moq(main_path),
        st_inventory_stock=_get_st_inventory_stock(),
        use_drafts=True,
        supplement_allocations=build_context_supplement_allocations([context]),
    )


# ── Upload schedule ───────────────────────────────────────────────────────────

@router.post("/schedule/upload")
async def upload_schedule(file: UploadFile = File(...)):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in {".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(400, "僅支援 xlsx / xls / xlsm")

    dest = SCHEDULE_DIR / f"schedule{ext}"
    dest.write_bytes(await file.read())

    rows = parse_schedule(str(dest))
    row_dicts = [r.dict() for r in rows]
    diff = db.upsert_orders_from_schedule(row_dicts)

    db.set_setting("schedule_file_path", str(dest))
    db.set_setting("schedule_filename", file.filename or dest.name)
    db.set_setting("schedule_loaded_at", datetime.now().isoformat())

    summary_parts = [f"共 {len(rows)} 筆"]
    if diff["added"]:
        summary_parts.append(f"新增 {diff['added']}")
    if diff["updated"]:
        summary_parts.append(f"更新 {diff['updated']}")
    if diff["skipped"]:
        summary_parts.append(f"已發料跳過 {diff['skipped']}")
    if diff["removed"]:
        summary_parts.append(f"移除 {diff['removed']}")
    db.log_activity("schedule_upload", f"{file.filename}：{'、'.join(summary_parts)}")

    return {
        "ok": True,
        "row_count": len(rows),
        "filename": file.filename,
        "loaded_at": db.get_setting("schedule_loaded_at"),
        "diff": diff,
    }


# ── 清理重複 PO（pending 與已發料重複）────────────────────────────────────

@router.post("/schedule/dedup")
async def dedup_schedule():
    """比對 pending/merged 與已發料訂單，移除 PO 重複的待處理項。"""
    result = db.remove_duplicate_pending_orders()
    if result["removed"]:
        db.log_activity("排程清理", f"移除 {result['removed']} 筆重複 PO")
    return result


# ── Get orders by status ─────────────────────────────────────────────────────

@router.get("/schedule/rows")
async def get_schedule_rows():
    """回傳未完成的訂單（pending + merged）。"""
    orders = db.get_orders(["pending", "merged"])
    dispatched_count = len(db.get_orders(["dispatched", "completed"]))
    order_ids = [int(order["id"]) for order in orders if order.get("id") is not None]
    return {
        "rows": orders,
        "loaded_at": db.get_setting("schedule_loaded_at"),
        "filename": db.get_setting("schedule_filename"),
        "completed_count": dispatched_count,
        "dispatched_consumption": _get_active_dispatched_consumption(),
        "decisions": db.get_all_decisions(),
        "merge_drafts": get_schedule_draft_map(),
        "order_supplements": db.get_order_supplements(order_ids) if order_ids else {},
        "order_supplement_details": db.get_order_supplement_details(order_ids) if order_ids else {},
    }


@router.get("/schedule/completed")
async def get_completed_rows():
    """回傳已發料/已完成的訂單 + 資料夾清單。"""
    orders = db.get_orders(["dispatched", "completed"])
    folders = db.get_dispatch_folders()
    order_ids = [int(order["id"]) for order in orders if order.get("id") is not None]
    return {
        "rows": orders,
        "folders": folders,
        "committed_merge_drafts": get_committed_schedule_draft_map(order_ids),
    }


@router.post("/schedule/orders/move-folder")
async def move_orders_to_folder(req: dict):
    order_ids = req.get("order_ids", [])
    folder = req.get("folder", "")
    if not order_ids:
        raise HTTPException(400, "請選擇訂單")
    db.move_orders_to_folder(order_ids, folder)
    db.log_activity("move_folder", f"{len(order_ids)} 筆訂單移至「{folder or '未歸檔'}」")
    return {"ok": True}


@router.delete("/schedule/folders/{folder_name}")
async def delete_folder(folder_name: str):
    """刪除資料夾（訂單移回未歸檔）。"""
    db.move_orders_to_folder_by_name(folder_name)
    db.log_activity("delete_folder", f"刪除資料夾「{folder_name}」")
    return {"ok": True}


@router.get("/schedule/cancelled")
async def get_cancelled_rows():
    orders = db.get_orders(["cancelled"])
    return {"rows": orders}


# ── Calculate shortage ────────────────────────────────────────────────────────

@router.get("/schedule/calculate")
async def calculate_shortage():
    """用快照庫存 + 已發料隔離 + running balance 計算缺料。"""
    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先上傳主檔")

    snapshot = _repair_legacy_snapshot_if_needed(main_path)
    if snapshot:
        snapshot_stock = {part: values["stock_qty"] for part, values in snapshot.items()}
        snapshot_moq = {part: values["moq"] for part, values in snapshot.items()}

        live_moq = read_moq(main_path)
        live_moq.update(snapshot_moq)
        moq = live_moq
    else:
        snapshot_stock = read_stock(main_path)
        moq = read_moq(main_path)

    # 快照是目前主檔基準，只扣快照之後新增的發料紀錄。
    dispatched_consumption = _get_active_dispatched_consumption()
    orders = db.get_orders(["pending", "merged"])
    bom_map = db.get_all_bom_components_by_model()

    results = calc_run(orders, bom_map, snapshot_stock, moq, dispatched_consumption, _get_st_inventory_stock())
    return {"results": results}


# ── Order status changes ──────────────────────────────────────────────────────

@router.post("/schedule/batch-merge")
def batch_merge(req: BatchMergeRequest):
    """批次將 pending 訂單改為 merged。"""
    if not req.order_ids:
        raise HTTPException(400, "請選擇要 merge 的訂單")
    t0 = time.monotonic()
    db.batch_merge_orders(req.order_ids)
    log.info("[batch_merge] status updated, rebuilding drafts for %d orders...", len(req.order_ids))
    drafts = rebuild_merge_drafts(req.order_ids)
    log.info("[batch_merge] done in %.1fs, %d drafts created", time.monotonic() - t0, len(drafts))
    db.log_activity("batch_merge", f"批次 merge {len(req.order_ids)} 筆訂單")
    db.create_alert(AlertType.BATCH_MERGE_DONE, f"批次 merge 完成，共 {len(req.order_ids)} 筆")
    return {"ok": True, "count": len(req.order_ids), "draft_count": len(drafts)}


@router.post("/schedule/auto-merge")
async def auto_merge():
    """自動 merge：交期 ≤ 下下個月底的 pending 訂單。"""
    today = date.today()
    cutoff = (today + relativedelta(months=2)).replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
    cutoff_str = cutoff.isoformat()

    orders = db.get_orders(["pending"])
    to_merge = [o["id"] for o in orders if (o.get("delivery_date") or o.get("ship_date") or "9999-99-99") <= cutoff_str]

    if to_merge:
        db.batch_merge_orders(to_merge)
        db.log_activity("auto_merge", f"自動 merge {len(to_merge)} 筆（截止 {cutoff_str}）")
        db.create_alert(AlertType.BATCH_MERGE_DONE, f"自動 merge {len(to_merge)} 筆（截止 {cutoff_str}）")

    return {"ok": True, "merged_count": len(to_merge), "cutoff": cutoff_str}


@router.patch("/schedule/orders/{order_id}/delivery")
async def update_delivery(order_id: int, req: UpdateDeliveryRequest):
    """改交期。已發料的訂單會跳警報。"""
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(404, "找不到此訂單")

    old_date = order.get("delivery_date") or order.get("ship_date") or "未知"

    alert_created = False
    if order["status"] in ("dispatched", "completed"):
        db.create_alert(
            AlertType.DELIVERY_CHANGE,
            f"訂單 {order['po_number']} ({order['model']}) 已發料，客人改交期 {old_date} → {req.delivery_date}",
            order_id=order_id,
        )
        alert_created = True

    db.update_order(order_id, delivery_date=req.delivery_date, ship_date=req.delivery_date)
    db.log_activity("delivery_changed", f"訂單 {order['po_number']} 交期 {old_date} → {req.delivery_date}")
    return {"ok": True, "alert": alert_created}


@router.post("/schedule/orders/{order_id}/cancel")
async def cancel_order(order_id: int):
    """取消訂單。已發料的會跳警報。"""
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(404, "找不到此訂單")

    if order["status"] in ("dispatched", "completed"):
        db.create_alert(
            AlertType.CANCELLATION,
            f"訂單 {order['po_number']} ({order['model']}) 已發料，客人要求取消",
            order_id=order_id,
        )
        db.log_activity("cancel_alert", f"已發料訂單 {order['po_number']} 被要求取消")
        return {"ok": True, "alert": True, "message": "已發料訂單，已建立警報，請人工處理"}

    db.update_order(order_id, status="cancelled")
    db.log_activity("order_cancelled", f"訂單 {order['po_number']} 已取消")
    return {"ok": True, "alert": False}


@router.post("/schedule/orders/{order_id}/restore")
async def restore_order(order_id: int):
    """恢復已取消的訂單。"""
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(404, "找不到此訂單")
    if order["status"] != "cancelled":
        raise HTTPException(400, "只能恢復已取消的訂單")
    db.update_order(order_id, status="pending")
    db.log_activity("order_restored", f"訂單 {order['po_number']} 已恢復")
    return {"ok": True}


# ── Dispatch (complete + merge) ───────────────────────────────────────────────

@router.post("/schedule/orders/{order_id}/dispatch")
async def dispatch_order(order_id: int, req: DecisionRequest):
    """
    標記訂單為已發料，每份 BOM 分別 merge 到主檔。
    """
    main_path = _require_existing_main_path()
    plan = _resolve_single_order_dispatch_plan(order_id, req, main_path)
    result = commit_dispatch_plan(
        plan,
        merge_executor=merge_row_to_main,
        backup_dir=str(BACKUP_DIR),
        rollback_executor=_rollback_dispatch_sessions,
        execute_dispatcher=_execute_dispatch,
        snapshot_refresher=refresh_snapshot_from_main,
    )
    return result.results[0]


@router.post("/schedule/main-write-preview")
async def preview_main_write(req: BatchDispatchRequest):
    normalized_order_ids = _normalize_order_ids(req.order_ids)
    if not normalized_order_ids:
        raise HTTPException(400, "請先選擇要預覽寫入主檔的訂單")

    main_path = _require_existing_main_path()
    plan = _resolve_batch_dispatch_plan(req, normalized_order_ids, main_path)
    return plan.to_preview_response()


@router.post("/schedule/batch-dispatch")
def batch_dispatch(req: BatchDispatchRequest):
    if not req.order_ids:
        raise HTTPException(400, "請選擇要發料的訂單")

    normalized_order_ids = _normalize_order_ids(req.order_ids)
    if not normalized_order_ids:
        raise HTTPException(400, "請選擇要發料的訂單")

    main_path = _require_existing_main_path()
    plan = _resolve_batch_dispatch_plan(req, normalized_order_ids, main_path)

    result = commit_dispatch_plan(
        plan,
        merge_executor=merge_row_to_main,
        backup_dir=str(BACKUP_DIR),
        rollback_executor=_rollback_dispatch_sessions,
        execute_dispatcher=_execute_dispatch,
        snapshot_refresher=refresh_snapshot_from_main,
    )
    db.log_activity("batch_dispatch", f"批次發料 {result.count} 筆訂單，合計 {result.merged_parts} 筆 merge")
    return {
        "ok": True,
        "count": result.count,
        "merged_parts": result.merged_parts,
        "order_ids": result.order_ids,
        "shortages": result.shortages,
    }


@router.put("/schedule/shortage-settings")
async def update_schedule_shortage_settings(req: BatchDispatchRequest):
    normalized_order_ids = _normalize_order_ids(req.order_ids)
    if not normalized_order_ids:
        raise HTTPException(400, "請先選擇要更新的訂單")

    decision_updates = build_order_decision_allocations(
        normalized_order_ids,
        req.decisions,
        include_none=True,
    )
    scoped_decisions = _normalize_order_decisions(req.order_decisions)
    decision_allocations = {
        order_id: _merge_decision_updates(
            order_id,
            {
                **decision_updates.get(order_id, {}),
                **scoped_decisions.get(order_id, {}),
            },
        )
        for order_id in normalized_order_ids
    }
    supplement_updates = _normalize_order_supplement_updates(req.order_supplements)
    supplement_allocations = {
        order_id: _merge_supplement_updates(
            order_id,
            supplement_updates.get(order_id, {}),
        )
        for order_id in normalized_order_ids
    }
    supplement_note_updates = _normalize_order_supplement_notes(req.order_supplement_notes)

    db.replace_order_decisions(normalized_order_ids, decision_allocations)
    db.replace_order_supplements(normalized_order_ids, supplement_allocations, supplement_note_updates)

    active_draft_orders = list(dict.fromkeys(
        int(item["order_id"])
        for item in db.get_active_merge_drafts()
        if item.get("order_id") is not None
    ))
    if active_draft_orders:
        rebuild_merge_drafts(active_draft_orders)

    db.log_activity("shortage_settings_update", f"更新右側補料設定 {len(normalized_order_ids)} 筆")
    return {
        "ok": True,
        "count": len(normalized_order_ids),
        "order_supplement_details": db.get_order_supplement_details(normalized_order_ids),
    }


@router.get("/schedule/drafts")
async def get_schedule_drafts():
    return {"drafts": get_schedule_draft_map()}


@router.put("/schedule/drafts")
async def update_selected_schedule_drafts(req: BatchDispatchRequest):
    normalized_order_ids = _normalize_order_ids(req.order_ids)
    if not normalized_order_ids:
        raise HTTPException(400, "請先選擇要更新副檔的訂單")

    draft_id_map = db.get_active_merge_draft_ids_by_order_ids(normalized_order_ids)
    missing_orders = [order_id for order_id in normalized_order_ids if order_id not in draft_id_map]
    if missing_orders:
        raise HTTPException(400, "部分訂單尚未建立副檔，請先重新 merge")

    decision_updates = build_order_decision_allocations(
        normalized_order_ids,
        req.decisions,
        include_none=True,
    )
    scoped_decisions = _normalize_order_decisions(req.order_decisions)
    decision_allocations = {
        order_id: _merge_decision_updates(
            order_id,
            {
                **decision_updates.get(order_id, {}),
                **scoped_decisions.get(order_id, {}),
            },
        )
        for order_id in normalized_order_ids
    }
    supplement_allocations = _merge_order_supplement_allocations(
        normalized_order_ids,
        req.supplements,
        _normalize_order_supplements(req.order_supplements),
    )
    db.replace_order_decisions(normalized_order_ids, decision_allocations)
    db.replace_order_supplements(normalized_order_ids, supplement_allocations)
    refreshed = rebuild_merge_drafts(normalized_order_ids)
    drafts = get_schedule_draft_map()
    db.log_activity("merge_draft_batch_update", f"批次更新副檔 {len(normalized_order_ids)} 筆")
    return {
        "ok": True,
        "count": len(normalized_order_ids),
        "draft_count": len(refreshed),
        "drafts": {str(order_id): drafts.get(order_id) for order_id in normalized_order_ids},
    }


@router.get("/schedule/drafts/{draft_id}")
async def get_schedule_draft_detail(draft_id: int):
    detail = get_draft_detail(draft_id)
    return {"ok": True, **detail}


@router.post("/schedule/drafts/download")
async def download_selected_schedule_drafts(req: BatchMergeRequest):
    return download_selected_merge_drafts(req.order_ids)


@router.get("/schedule/drafts/{draft_id}/download")
async def download_schedule_draft(draft_id: int, file_id: int | None = None):
    return download_merge_draft(draft_id, file_id=file_id)


@router.put("/schedule/drafts/{draft_id}")
async def update_schedule_draft(draft_id: int, req: DecisionRequest):
    draft = db.get_merge_draft(draft_id)
    if not draft or draft.get("status") != "active":
        raise HTTPException(404, "找不到副檔草稿")
    order_id = int(draft["order_id"])
    decision_allocations = {
        order_id: _merge_decision_updates(order_id, req.decisions or {})
    }
    supplement_allocations = {
        order_id: _normalize_supplements(req.supplements or {})
    }
    db.replace_order_decisions([order_id], decision_allocations)
    db.replace_order_supplements([order_id], supplement_allocations)
    refreshed = rebuild_merge_drafts([order_id])

    current = db.get_active_merge_draft_for_order(order_id)
    if not current:
        raise HTTPException(404, "副檔草稿更新後不存在")
    db.log_activity("merge_draft_update", f"更新副檔草稿 {current['id']} / order {current['order_id']}")
    return {"ok": True, "draft": get_draft_detail(int(current["id"]))["draft"], "refreshed_count": len(refreshed)}


@router.delete("/schedule/drafts/{draft_id}")
async def delete_schedule_draft(draft_id: int):
    detail = get_draft_detail(draft_id)
    delete_merge_draft_and_refresh(draft_id)
    order = detail.get("order") or {}
    db.log_activity("merge_draft_delete", f"刪除副檔草稿 {draft_id} / {order.get('po_number', '')} {order.get('model', '')}")
    return {"ok": True, "draft_id": draft_id}


@router.post("/schedule/drafts/{draft_id}/commit")
def commit_schedule_draft(draft_id: int):
    main_path = _require_existing_main_path()
    plan = _resolve_draft_commit_plan(draft_id, main_path)
    result = commit_dispatch_plan(
        plan,
        merge_executor=merge_row_to_main,
        backup_dir=str(BACKUP_DIR),
        rollback_executor=_rollback_dispatch_sessions,
        execute_dispatcher=_execute_dispatch,
        snapshot_refresher=refresh_snapshot_from_main,
    )
    db.log_activity(
        "merge_draft_commit",
        f"提交副檔草稿 {draft_id} / {plan.contexts[0].order.get('po_number', '')} {plan.contexts[0].order.get('model', '')}",
    )
    return {
        "ok": True,
        "draft_id": draft_id,
        "order_id": plan.contexts[0].order_id,
        "merged_parts": result.merged_parts,
        "shortages": result.shortages,
    }


@router.get("/schedule/orders/{order_id}/rollback-preview")
async def rollback_order_preview(order_id: int, force: bool = False):
    _, session, affected_orders = _build_rollback_preview(order_id, force=force)
    return {
        "ok": True,
        "count": len(affected_orders),
        "backup_path": session.get("backup_path", ""),
        "orders": affected_orders,
        "forced": force,
    }


@router.post("/schedule/orders/{order_id}/rollback")
async def rollback_order(order_id: int, force: bool = False):
    order, session, affected_orders = _build_rollback_preview(order_id, force=force)
    tail_sessions = db.get_dispatch_session_tail(int(session["id"]))
    result = _rollback_dispatch_sessions(tail_sessions)
    db.log_activity(
        "order_rollback",
        (
            f"從訂單 {order['po_number']} ({order['model']}) 開始"
            f"{'強制' if force else ''}反悔，共 {len(affected_orders)} 筆，主檔已還原"
        ),
    )
    return {"ok": True, "forced": force, **result}


# ── Reorder / Sort ────────────────────────────────────────────────────────────

@router.post("/schedule/reorder")
async def save_order_sort(req: ReorderRequest):
    db.update_orders_sort(req.order_ids)
    return {"ok": True}


@router.post("/schedule/auto-sort")
async def auto_sort():
    orders = db.get_orders(["pending", "merged"])
    sorted_orders = sorted(orders, key=lambda o: (o.get("delivery_date") or o.get("ship_date") or "9999-99-99", o["id"]))
    db.update_orders_sort([o["id"] for o in sorted_orders])
    return {"ok": True}


# ── Code ──────────────────────────────────────────────────────────────────────

@router.patch("/schedule/orders/{order_id}/code")
async def update_order_code(order_id: int, req: RowCodeRequest):
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(404, "找不到此訂單")
    db.update_order(order_id, code=req.code)
    return {"ok": True}


@router.patch("/schedule/orders/{order_id}/model")
async def update_order_model(order_id: int, req: UpdateModelRequest):
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(404, "找不到此訂單")
    new_model = req.model.strip()
    if not new_model:
        raise HTTPException(400, "機種名稱不可為空")
    db.update_order(order_id, model=new_model)
    db.log_activity("model_changed", f"訂單 {order['po_number']} 機種 {order['model']} → {new_model}")
    return {"ok": True}


# ── Decisions ─────────────────────────────────────────────────────────────────

@router.post("/schedule/orders/{order_id}/decisions")
async def save_decisions(order_id: int, req: DecisionRequest):
    for part, decision in req.decisions.items():
        db.save_decision(order_id, part, decision)
    return {"ok": True}


@router.get("/schedule/decisions")
async def get_all_decisions_api():
    return {"decisions": db.get_all_decisions()}


# ── Supplement (post-dispatch) ────────────────────────────────────────────────

@router.post("/schedule/supplement-part")
def supplement_part(req: SupplementPartRequest):
    """寫入後補料：直接對主檔指定料號新增一欄補料庫存。"""
    main_path = str(db.get_setting("main_file_path") or "").strip()
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先載入主檔")
    result = supplement_part_in_main(
        main_path,
        req.part_number,
        req.supplement_qty,
        backup_dir=str(BACKUP_DIR),
    )
    if not result.get("ok"):
        raise HTTPException(400, result.get("message", "補料失敗"))
    refresh_snapshot_from_main(main_path)
    note = str(req.note or "").strip()
    detail = f"補料 {result['part_number']}: {result['supplement_qty']:g} → 庫存 {result['stock_before']:g} → {result['stock_after']:g}"
    if note:
        detail += f"（{note}）"
    db.log_activity("supplement_part", detail)
    return result


@router.get("/schedule/supplement-logs")
def get_supplement_logs(limit: int = 50):
    """查詢手動補料紀錄。"""
    logs = db.get_activity_logs_by_action("supplement_part", limit=limit)
    return {"logs": logs}
