"""不良品處理 API"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from .. import database as db
from ..models import CreateDefectiveRequest, UpdateDefectiveRequest

router = APIRouter(prefix="/defectives", tags=["defectives"])


@router.get("")
async def list_defectives(status: str = "all"):
    records = db.get_defective_records(status)
    order_cache: dict[int, dict] = {}
    for record in records:
        order_id = record.get("order_id")
        if order_id and order_id not in order_cache:
            order_cache[order_id] = db.get_order(order_id) or {}
        order = order_cache.get(order_id, {})
        record["po_number"] = order.get("po_number", "")
        record["model"] = order.get("model", "")
    return {"records": records}


@router.post("")
async def create_defective(req: CreateDefectiveRequest):
    data = req.dict(exclude_none=True)
    record_id = db.create_defective_record(data)
    db.log_activity("新增不良品", f"料號 {req.part_number} 數量 {req.defective_qty}")
    return {"ok": True, "id": record_id}


@router.patch("/{record_id}")
async def update_defective(record_id: int, req: UpdateDefectiveRequest):
    existing = db.get_defective_record(record_id)
    if not existing:
        raise HTTPException(404, "找不到不良品紀錄")
    data = req.dict(exclude_none=True)
    if not data:
        raise HTTPException(400, "沒有要更新的欄位")
    db.update_defective_record(record_id, data)
    db.log_activity("更新不良品", f"ID={record_id}")
    return {"ok": True}


@router.post("/{record_id}/confirm")
async def confirm_defective(record_id: int):
    if not db.confirm_defective_record(record_id):
        raise HTTPException(400, "無法確認（可能已確認或不存在）")
    db.log_activity("確認不良品", f"ID={record_id}")
    return {"ok": True}


@router.post("/{record_id}/close")
async def close_defective(record_id: int):
    if not db.close_defective_record(record_id):
        raise HTTPException(400, "無法結案（可能已結案或不存在）")
    db.log_activity("結案不良品", f"ID={record_id}")
    return {"ok": True}
