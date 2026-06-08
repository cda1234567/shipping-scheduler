"""海運出貨 API。"""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .. import database as db
from ..services.sea_freight import (
    export_sea_shipment,
    _infer_harmonized_code,
    load_packing_specs_from_template,
    parse_sea_order_file,
    save_uploaded_sea_file,
)

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


class SeaPackingSpecRequest(BaseModel):
    item_no: str
    packing_name: str = ""
    per_box_qty: float = 0
    net_weight: float = 0
    gross_weight: float = 0
    volume: float = 0
    vendor: str = ""


class SeaHscRequest(BaseModel):
    item_no: str
    harmonized_code: str = ""
    note: str = ""


def _ensure_packing_specs_seeded() -> list[dict]:
    specs = db.get_sea_packing_specs()
    if specs:
        return specs
    template_specs = load_packing_specs_from_template()
    if template_specs:
        db.upsert_sea_packing_specs(template_specs)
        specs = db.get_sea_packing_specs()
        db.log_activity("sea_packing_seed", f"初始化海運包裝主檔 {len(specs)} 筆")
    return specs


def _ensure_hsc_seeded() -> list[dict]:
    codes = db.list_sea_harmonized_codes()
    if codes:
        return codes
    specs = _ensure_packing_specs_seeded()
    seeded = 0
    for spec in specs:
        item_no = str(spec.get("item_no") or "").strip()
        packing_name = str(spec.get("packing_name") or "").strip()
        code = _infer_harmonized_code(item_no, packing_name)
        if item_no and code:
            db.upsert_sea_harmonized_code(item_no, code, "系統依品名初始帶入")
            seeded += 1
    if seeded:
        db.log_activity("sea_hsc_seed", f"初始化海運 HSC 主檔 {seeded} 筆")
    return db.list_sea_harmonized_codes()


@router.post("/upload")
async def upload_sea_freight(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(400, "請選擇海運 Excel")
    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".xlsx", ".xlsm"):
        raise HTTPException(400, "目前海運匯入支援 .xlsx / .xlsm")

    path = save_uploaded_sea_file(file.filename, await file.read())
    try:
        meta, items = parse_sea_order_file(
            path,
            {row["item_no"]: row["harmonized_code"] for row in _ensure_hsc_seeded()},
            _ensure_packing_specs_seeded(),
        )
    except Exception as exc:
        path.unlink(missing_ok=True)
        raise HTTPException(400, f"解析海運檔失敗：{exc}")

    meta["filename"] = file.filename
    meta["source_path"] = str(path)
    shipment_id = db.create_sea_shipment(meta, items)
    db.log_activity("sea_freight_upload", f"{file.filename}：{len(items)} 筆")
    return {"shipment_id": shipment_id, "item_count": len(items), "shipment": db.get_sea_shipment(shipment_id)}


@router.get("/packing-specs")
async def list_packing_specs():
    return {"specs": _ensure_packing_specs_seeded()}


@router.put("/packing-specs/{item_no}")
async def update_packing_spec(item_no: str, req: SeaPackingSpecRequest):
    data = req.dict()
    data["item_no"] = item_no.strip() or req.item_no.strip()
    if not data["item_no"]:
        raise HTTPException(400, "請輸入 ITEM NO")
    db.upsert_sea_packing_spec(data)
    db.log_activity("sea_packing_update", f"{data['item_no']} 包裝主檔已更新")
    return {"ok": True, "spec": data}


@router.delete("/packing-specs/{item_no}")
async def delete_packing_spec(item_no: str):
    ok = db.delete_sea_packing_spec(item_no)
    if not ok:
        raise HTTPException(404, "找不到包裝主檔")
    db.log_activity("sea_packing_delete", f"{item_no} 包裝主檔已刪除")
    return {"ok": True}


@router.get("/hsc-codes")
async def list_hsc_codes():
    return {"codes": _ensure_hsc_seeded()}


@router.put("/hsc-codes/{item_no}")
async def update_hsc_code(item_no: str, req: SeaHscRequest):
    saved_item_no = item_no.strip() or req.item_no.strip()
    if not saved_item_no:
        raise HTTPException(400, "請輸入 ITEM NO")
    code = req.harmonized_code.strip()
    db.upsert_sea_harmonized_code(saved_item_no, code, req.note.strip())
    db.log_activity("sea_hsc_update", f"{saved_item_no} HSC -> {code}")
    return {"ok": True, "code": {"item_no": saved_item_no, "harmonized_code": code, "note": req.note.strip()}}


@router.delete("/hsc-codes/{item_no}")
async def delete_hsc_code(item_no: str):
    ok = db.delete_sea_harmonized_code(item_no)
    if not ok:
        raise HTTPException(404, "找不到 HSC 主檔")
    db.log_activity("sea_hsc_delete", f"{item_no} HSC 已刪除")
    return {"ok": True}


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
