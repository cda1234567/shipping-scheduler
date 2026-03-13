"""
排程與訂單管理路由 — 四階段狀態 + 已發料隔離 + 批次 merge。

訂單狀態: pending → merged → dispatched → completed
                                    ↘ cancelled
"""
from __future__ import annotations
import shutil
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel

from ..config import SCHEDULE_DIR, BACKUP_DIR, cfg
from ..services.main_reader import find_legacy_snapshot_stock_fixes, read_moq, read_stock
from ..services.schedule_parser import parse_schedule
from ..services.calculator import run as calc_run
from ..services.merge_to_main import merge_row_to_main, preview_order_batches
from ..services.order_supplements import build_order_supplement_allocations
from ..services.merge_drafts import (
    rebuild_merge_drafts,
    delete_merge_draft_and_refresh,
    get_schedule_draft_map,
    get_draft_detail,
    download_merge_draft,
)
from ..models import (
    ReorderRequest, UpdateDeliveryRequest, BatchMergeRequest,
    BatchDispatchRequest, DecisionRequest, RowCodeRequest, UpdateModelRequest, AlertType,
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


def _build_rollback_preview(order_id: int) -> tuple[dict, dict, list[dict]]:
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(404, "找不到此訂單")
    if order["status"] not in ("dispatched", "completed"):
        raise HTTPException(400, "只能反悔已發料訂單")

    session = db.get_active_dispatch_session(order_id)
    if not session:
        raise HTTPException(400, "找不到這筆訂單的發料歷史，無法反悔")

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


def _normalize_decisions(decisions: dict[str, str] | None = None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for part, decision in (decisions or {}).items():
        key = str(part or "").strip().upper()
        if not key or not decision:
            continue
        normalized[key] = str(decision)
    return normalized


def _normalize_supplements(supplements: dict[str, float] | None = None) -> dict[str, float]:
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


def _prepare_dispatch_context(order_id: int, main_path: str) -> tuple[dict, list[dict], list[dict]]:
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


def _get_effective_moq(main_path: str) -> dict[str, float]:
    live_moq = read_moq(main_path) if main_path and Path(main_path).exists() else {}
    snapshot = db.get_snapshot()
    if snapshot:
        snapshot_moq = {part: float((row or {}).get("moq") or 0) for part, row in snapshot.items()}
        live_moq.update(snapshot_moq)
    return live_moq


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

    current_main_path = str(db.get_setting("main_file_path") or "").strip()
    session_paths = {
        str(session.get("main_file_path") or "").strip()
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

    return {
        "count": len(restored_orders),
        "restored_from": str(backup_path),
        "main_file_path": str(restore_target_path),
        "orders": restored_orders,
    }


def _execute_dispatch(
    order: dict,
    groups: list[dict],
    all_components: list[dict],
    main_path: str,
    decisions: dict[str, str],
    supplements: dict[str, float] | None = None,
) -> dict:
    result = merge_row_to_main(
        main_path=main_path,
        groups=groups,
        decisions=decisions,
        supplements=supplements or {},
        backup_dir=str(BACKUP_DIR),
    )

    session = None
    try:
        session = db.save_dispatch_session(
            order_id=int(order["id"]),
            previous_status=order["status"],
            backup_path=result.get("backup_path") or "",
            main_file_path=main_path,
        )

        dispatch_records = []
        for comp in all_components:
            if comp.get("is_dash") or comp.get("needed_qty", 0) <= 0:
                continue
            part_number = str(comp.get("part_number") or "")
            dispatch_records.append({
                "part_number": part_number,
                "needed_qty": comp["needed_qty"],
                "prev_qty_cs": comp.get("prev_qty_cs", 0),
                "decision": decisions.get(part_number.strip().upper(), "None"),
            })
        db.save_dispatch_records(int(order["id"]), dispatch_records)
        db.update_order(int(order["id"]), status="dispatched")
    except Exception:
        backup_path = Path(str(result.get("backup_path") or "")).expanduser()
        if backup_path.exists():
            shutil.copy2(backup_path, main_path)
        if session:
            db.delete_dispatch_records_for_orders([int(order["id"])])
            db.mark_dispatch_sessions_rolled_back([int(session["id"])])
            db.update_order(int(order["id"]), status=order["status"], folder=order.get("folder", ""))
        raise

    db.log_activity(
        "order_dispatched",
        f"訂單 {order['po_number']} ({order['model']}) 已發料，{result['merged_parts']} 筆 merge",
    )
    return {
        "ok": True,
        "order_id": int(order["id"]),
        "merged_parts": result["merged_parts"],
        "backup_path": result["backup_path"],
        "session": session,
    }


def _current_main_signature(main_path: str) -> str:
    return str(Path(main_path).stat().st_mtime_ns)


def _load_active_merge_draft_context(draft_id: int, main_path: str) -> dict:
    draft = db.get_merge_draft(draft_id)
    if not draft or draft.get("status") != "active":
        raise HTTPException(404, "找不到可提交的副檔草稿")

    draft_main_path = str(draft.get("main_file_path") or "").strip()
    if draft_main_path and Path(draft_main_path) != Path(main_path):
        raise HTTPException(400, "主檔路徑已變更，請重新整理副檔後再提交")

    current_signature = _current_main_signature(main_path)
    if str(draft.get("main_file_mtime_ns") or "") != current_signature:
        raise HTTPException(400, "主檔內容已變更，請先重新整理副檔")

    order, groups, all_components = _prepare_dispatch_context(int(draft["order_id"]), main_path)
    return {
        "draft": draft,
        "order": order,
        "groups": groups,
        "all_components": all_components,
        "decisions": _normalize_decisions(draft.get("decisions")),
        "supplements": _normalize_supplements(draft.get("supplements")),
    }


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
    db.upsert_orders_from_schedule(row_dicts)

    db.set_setting("schedule_file_path", str(dest))
    db.set_setting("schedule_filename", file.filename or dest.name)
    db.set_setting("schedule_loaded_at", datetime.now().isoformat())
    db.log_activity("schedule_upload", f"{file.filename}, {len(rows)} 筆")

    return {
        "ok": True,
        "row_count": len(rows),
        "filename": file.filename,
        "loaded_at": db.get_setting("schedule_loaded_at"),
    }


# ── Get orders by status ─────────────────────────────────────────────────────

@router.get("/schedule/rows")
async def get_schedule_rows():
    """回傳未完成的訂單（pending + merged）。"""
    orders = db.get_orders(["pending", "merged"])
    dispatched_count = len(db.get_orders(["dispatched", "completed"]))
    return {
        "rows": orders,
        "loaded_at": db.get_setting("schedule_loaded_at"),
        "filename": db.get_setting("schedule_filename"),
        "completed_count": dispatched_count,
        "dispatched_consumption": _get_active_dispatched_consumption(),
        "decisions": db.get_all_decisions(),
        "merge_drafts": get_schedule_draft_map(),
    }


@router.get("/schedule/completed")
async def get_completed_rows():
    """回傳已發料/已完成的訂單 + 資料夾清單。"""
    orders = db.get_orders(["dispatched", "completed"])
    folders = db.get_dispatch_folders()
    return {"rows": orders, "folders": folders}


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

    results = calc_run(orders, bom_map, snapshot_stock, moq, dispatched_consumption)
    return {"results": results}


# ── Order status changes ──────────────────────────────────────────────────────

@router.post("/schedule/batch-merge")
async def batch_merge(req: BatchMergeRequest):
    """批次將 pending 訂單改為 merged。"""
    if not req.order_ids:
        raise HTTPException(400, "請選擇要 merge 的訂單")
    db.batch_merge_orders(req.order_ids)
    drafts = rebuild_merge_drafts(req.order_ids)
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
    main_path = db.get_setting("main_file_path")
    order, groups, all_components = _prepare_dispatch_context(order_id, main_path)
    decisions = _normalize_decisions(req.decisions)
    supplements = _normalize_supplements(req.supplements)
    result = _execute_dispatch(order, groups, all_components, main_path, decisions, supplements)
    if supplements:
        allocations = build_order_supplement_allocations([order_id], supplements)
        db.replace_order_supplements([order_id], allocations)
    return result


@router.post("/schedule/main-write-preview")
async def preview_main_write(req: BatchDispatchRequest):
    normalized_order_ids = []
    for order_id in req.order_ids:
        try:
            normalized_order_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_order_ids = list(dict.fromkeys(normalized_order_ids))
    if not normalized_order_ids:
        raise HTTPException(400, "隢?靘?閮")

    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "隢?銝銝餅?")

    decisions = _normalize_decisions(req.decisions)
    supplements = _normalize_supplements(req.supplements)
    supplement_allocations = build_order_supplement_allocations(normalized_order_ids, supplements)

    batches = []
    for order_id in normalized_order_ids:
        order, groups, _ = _prepare_dispatch_context(order_id, main_path)
        batches.append({
            "order_id": int(order["id"]),
            "model": order.get("model", ""),
            "groups": groups,
            "supplements": supplement_allocations.get(int(order["id"]), {}),
        })

    preview = preview_order_batches(
        main_path,
        batches,
        decisions,
        moq_map=_get_effective_moq(main_path),
    )
    return {
        "ok": True,
        "count": len(batches),
        "merged_parts": preview["merged_parts"],
        "shortages": preview["shortages"],
    }


@router.post("/schedule/batch-dispatch")
async def batch_dispatch(req: BatchDispatchRequest):
    if not req.order_ids:
        raise HTTPException(400, "請選擇要發料的訂單")

    normalized_order_ids = []
    for order_id in req.order_ids:
        try:
            normalized_order_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_order_ids = list(dict.fromkeys(normalized_order_ids))
    if not normalized_order_ids:
        raise HTTPException(400, "請選擇要發料的訂單")

    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先上傳主檔")

    draft_id_map = db.get_active_merge_draft_ids_by_order_ids(normalized_order_ids)
    missing_orders = [order_id for order_id in normalized_order_ids if order_id not in draft_id_map]
    if missing_orders and draft_id_map:
        raise HTTPException(400, "有訂單還沒有副檔草稿，請先 merge 生成副檔")
    if draft_id_map:
        contexts = [_load_active_merge_draft_context(draft_id_map[order_id], main_path) for order_id in normalized_order_ids]
        use_drafts = True
    else:
        decisions = _normalize_decisions(req.decisions)
        supplements = _normalize_supplements(req.supplements)
        supplement_allocations = build_order_supplement_allocations(normalized_order_ids, supplements)
        raw_contexts = [_prepare_dispatch_context(order_id, main_path) for order_id in normalized_order_ids]
        contexts = [
            {
                "draft": None,
                "order": order,
                "groups": groups,
                "all_components": all_components,
                "decisions": decisions,
                "supplements": supplement_allocations.get(int(order["id"]), {}),
            }
            for order, groups, all_components in raw_contexts
        ]
        use_drafts = False

    results: list[dict] = []
    processed_sessions: list[dict] = []
    committed_draft_ids: list[int] = []
    try:
        for context in contexts:
            result = _execute_dispatch(
                context["order"],
                context["groups"],
                context["all_components"],
                main_path,
                context["decisions"],
                context["supplements"],
            )
            results.append(result)
            if result.get("session"):
                processed_sessions.append(result["session"])
            if context.get("draft"):
                committed_draft_ids.append(int(context["draft"]["id"]))
    except Exception:
        if processed_sessions:
            _rollback_dispatch_sessions(processed_sessions)
            db.log_activity("batch_dispatch_rollback", f"批次發料失敗，已回復 {len(processed_sessions)} 筆")
        raise

    if use_drafts:
        for context in contexts:
            db.replace_order_supplements(
                [int(context["order"]["id"])],
                {int(context["order"]["id"]): context["supplements"]},
            )
    else:
        db.replace_order_supplements(
            normalized_order_ids,
            {int(context["order"]["id"]): context["supplements"] for context in contexts},
        )
    if use_drafts:
        for draft_id in committed_draft_ids:
            db.mark_merge_draft_committed(draft_id)
        remaining_active_orders = [item["order_id"] for item in db.get_active_merge_drafts()]
        if remaining_active_orders:
            rebuild_merge_drafts(remaining_active_orders)

    total_merged_parts = sum(int(item.get("merged_parts") or 0) for item in results)
    db.log_activity("batch_dispatch", f"批次發料 {len(results)} 筆訂單，合計 {total_merged_parts} 筆 merge")
    return {
        "ok": True,
        "count": len(results),
        "merged_parts": total_merged_parts,
        "order_ids": [int(item["order_id"]) for item in results],
    }


@router.get("/schedule/drafts")
async def get_schedule_drafts():
    return {"drafts": get_schedule_draft_map()}


@router.get("/schedule/drafts/{draft_id}")
async def get_schedule_draft_detail(draft_id: int):
    detail = get_draft_detail(draft_id)
    return {"ok": True, **detail}


@router.get("/schedule/drafts/{draft_id}/download")
async def download_schedule_draft(draft_id: int):
    return download_merge_draft(draft_id)


@router.put("/schedule/drafts/{draft_id}")
async def update_schedule_draft(draft_id: int, req: DecisionRequest):
    draft = db.get_merge_draft(draft_id)
    if not draft or draft.get("status") != "active":
        raise HTTPException(404, "找不到副檔草稿")
    refreshed = rebuild_merge_drafts(
        [int(draft["order_id"])],
        {
            int(draft["order_id"]): {
                "decisions": req.decisions,
                "supplements": req.supplements,
            }
        },
    )
    current = db.get_active_merge_draft_for_order(int(draft["order_id"]))
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
async def commit_schedule_draft(draft_id: int):
    main_path = str(db.get_setting("main_file_path") or "").strip()
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先載入主檔")

    context = _load_active_merge_draft_context(draft_id, main_path)
    result = _execute_dispatch(
        context["order"],
        context["groups"],
        context["all_components"],
        main_path,
        context["decisions"],
        context["supplements"],
    )
    db.replace_order_supplements(
        [int(context["order"]["id"])],
        {int(context["order"]["id"]): context["supplements"]},
    )
    db.mark_merge_draft_committed(draft_id)
    remaining_active_orders = [item["order_id"] for item in db.get_active_merge_drafts()]
    if remaining_active_orders:
        rebuild_merge_drafts(remaining_active_orders)
    db.log_activity(
        "merge_draft_commit",
        f"提交副檔草稿 {draft_id} / {context['order'].get('po_number', '')} {context['order'].get('model', '')}",
    )
    return {
        "ok": True,
        "draft_id": draft_id,
        "order_id": int(context["order"]["id"]),
        "merged_parts": int(result.get("merged_parts") or 0),
    }


@router.get("/schedule/orders/{order_id}/rollback-preview")
async def rollback_order_preview(order_id: int):
    _, session, affected_orders = _build_rollback_preview(order_id)
    return {
        "ok": True,
        "count": len(affected_orders),
        "backup_path": session.get("backup_path", ""),
        "orders": affected_orders,
    }


@router.post("/schedule/orders/{order_id}/rollback")
async def rollback_order(order_id: int):
    order, session, affected_orders = _build_rollback_preview(order_id)
    tail_sessions = db.get_dispatch_session_tail(int(session["id"]))
    result = _rollback_dispatch_sessions(tail_sessions)
    db.log_activity(
        "order_rollback",
        f"從訂單 {order['po_number']} ({order['model']}) 開始反悔，共 {len(affected_orders)} 筆，主檔已還原",
    )
    return {"ok": True, **result}


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
