"""海運出貨 API。"""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .. import database as db
from ..services.sea_freight import export_sea_shipment, parse_sea_order_file, save_uploaded_sea_file

router = APIRouter(prefix="/sea-freight", tags=["sea-freight"])


class SeaShipmentUpdateRequest(BaseModel):
    customer: str = ""
    cust_po: str = ""
    shipment_date: str = ""
    delivery_date: str = ""
    maker: str = "Andy"
    mark_text: str = "HILLIARD"
    invoice_no: str = ""
    items: list[dict] = Field(default_factory=list)


@router.post("/upload")
async def upload_sea_freight(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(400, "請選擇海運 Excel")
    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".xlsx", ".xlsm"):
        raise HTTPException(400, "目前海運匯入支援 .xlsx / .xlsm")

    path = save_uploaded_sea_file(file.filename, await file.read())
    try:
        meta, items = parse_sea_order_file(path, db.get_sea_harmonized_codes())
    except Exception as exc:
        path.unlink(missing_ok=True)
        raise HTTPException(400, f"解析海運檔失敗：{exc}")

    meta["filename"] = file.filename
    meta["source_path"] = str(path)
    shipment_id = db.create_sea_shipment(meta, items)
    db.log_activity("sea_freight_upload", f"{file.filename}：{len(items)} 筆")
    return {"shipment_id": shipment_id, "item_count": len(items), "shipment": db.get_sea_shipment(shipment_id)}


@router.get("/shipments")
async def list_shipments(limit: int = 30):
    return {"shipments": db.list_sea_shipments(limit)}


@router.get("/shipments/{shipment_id}")
async def get_shipment(shipment_id: int):
    shipment = db.get_sea_shipment(shipment_id)
    if not shipment:
        raise HTTPException(404, "找不到海運批次")
    return {"shipment": shipment}


@router.put("/shipments/{shipment_id}")
async def update_shipment(shipment_id: int, req: SeaShipmentUpdateRequest):
    items = req.items or []
    ok = db.update_sea_shipment(shipment_id, req.dict(exclude={"items"}), items)
    if not ok:
        raise HTTPException(404, "找不到海運批次")
    for item in items:
        item_no = str(item.get("item_no") or "").strip()
        code = str(item.get("harmonized_code") or "").strip()
        if item_no and code:
            db.upsert_sea_harmonized_code(item_no, code, "海運出貨畫面更新")
    db.log_activity("sea_freight_update", f"海運批次 #{shipment_id} 已更新")
    return {"ok": True, "shipment": db.get_sea_shipment(shipment_id)}


@router.delete("/shipments/{shipment_id}")
async def delete_shipment(shipment_id: int):
    ok = db.delete_sea_shipment(shipment_id)
    if not ok:
        raise HTTPException(404, "找不到海運批次")
    db.log_activity("sea_freight_delete", f"刪除海運批次 #{shipment_id}")
    return {"ok": True}


@router.post("/shipments/{shipment_id}/export")
async def export_shipment(shipment_id: int):
    shipment = db.get_sea_shipment(shipment_id)
    if not shipment:
        raise HTTPException(404, "找不到海運批次")
    try:
        output = export_sea_shipment(shipment)
    except Exception as exc:
        raise HTTPException(400, f"匯出海運出貨單失敗：{exc}")
    db.log_activity("sea_freight_export", f"海運批次 #{shipment_id} 匯出 {output.name}")
    return FileResponse(
        str(output),
        filename=output.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
