"""
排程與訂單管理路由 — 四階段狀態 + 已發料隔離 + 批次 merge。

訂單狀態: pending → merged → dispatched → completed
                                    ↘ cancelled
"""
from __future__ import annotations
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel

from ..config import SCHEDULE_DIR, BACKUP_DIR, cfg
from ..services.schedule_parser import parse_schedule
from ..services.calculator import run as calc_run
from ..services.merge_to_main import merge_row_to_main
from ..models import (
    ReorderRequest, UpdateDeliveryRequest, BatchMergeRequest,
    DecisionRequest, RowCodeRequest, UpdateModelRequest, AlertType,
)
from .. import database as db

router = APIRouter()


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
        "dispatched_consumption": db.get_all_dispatched_consumption(),
        "decisions": db.get_all_decisions(),
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

    snapshot_stock = db.get_snapshot_stock()
    moq = db.get_snapshot_moq()
    if not snapshot_stock:
        from ..services.main_reader import read_stock, read_moq
        snapshot_stock = read_stock(main_path)
        moq = read_moq(main_path)
    else:
        from ..services.main_reader import read_moq
        live_moq = read_moq(main_path)
        live_moq.update(moq)
        moq = live_moq

    dispatched_consumption = db.get_all_dispatched_consumption()
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
    db.log_activity("batch_merge", f"批次 merge {len(req.order_ids)} 筆訂單")
    db.create_alert(AlertType.BATCH_MERGE_DONE, f"批次 merge 完成，共 {len(req.order_ids)} 筆")
    return {"ok": True, "count": len(req.order_ids)}


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
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(404, "找不到此訂單")
    if order["status"] not in ("pending", "merged"):
        raise HTTPException(400, f"訂單狀態為 {order['status']}，無法發料")

    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先上傳主檔")

    # 取得個別 BOM 檔案（不合併）
    model_key = (order.get("model") or "").upper()
    bom_files = db.get_bom_files_by_models([model_key])
    if not bom_files:
        raise HTTPException(400, f"機種 {order.get('model')} 沒有對應的 BOM")

    label = order.get("code") or order.get("model") or str(order_id)
    po_number = str(order.get("po_number", ""))

    # 每份 BOM 各自一組
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

    result = merge_row_to_main(
        main_path=main_path,
        groups=groups,
        decisions=req.decisions,
        backup_dir=str(BACKUP_DIR),
    )

    # 儲存發料紀錄
    dispatch_records = []
    for comp in all_components:
        if comp.get("is_dash") or comp.get("needed_qty", 0) <= 0:
            continue
        dispatch_records.append({
            "part_number": comp["part_number"],
            "needed_qty": comp["needed_qty"],
            "prev_qty_cs": comp.get("prev_qty_cs", 0),
            "decision": req.decisions.get(comp["part_number"], "None"),
        })
    db.save_dispatch_records(order_id, dispatch_records)

    db.update_order(order_id, status="dispatched")
    db.log_activity("order_dispatched",
                    f"訂單 {order['po_number']} ({order['model']}) 已發料，{result['merged_parts']} 筆 merge")

    return {
        "ok": True,
        "order_id": order_id,
        "merged_parts": result["merged_parts"],
        "backup_path": result["backup_path"],
    }


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
