from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from .. import database as db
from ..models import (
    LowStockAddRequest,
    LowStockRemoveRequest,
    LowStockSettingsRequest,
    LowStockUpdateRequest,
)
from ..services import low_stock
from ..services.erp_stock import ErpError
from ..services.local_time import local_now

router = APIRouter(prefix="/low-stock")

# 這個路由的 POST 一律沿用 edit_auth middleware（/api 下非 GET），不加入豁免名單。
ACTOR = "web"


def _http_error(exc: low_stock.LowStockError) -> HTTPException:
    if isinstance(exc, low_stock.LowStockNotFound):
        return HTTPException(404, str(exc))
    if isinstance(exc, low_stock.LowStockExists):
        return HTTPException(409, str(exc))
    return HTTPException(400, str(exc))


@router.get("/alerts")
async def get_low_stock_alerts():
    """低於門檻的常用料號。讀本機資料庫與 ST 庫存快照；MOQ 以正航最近進貨量估算（唯讀、有快取，連不上就退回本站 MOQ）。"""
    main_parts = low_stock.main_file_parts()
    alerts = low_stock.list_alerts(main_parts)
    return {
        **low_stock.main_file_status(main_parts),
        "generated_at": local_now().strftime("%Y-%m-%d %H:%M:%S"),
        "st_inventory_loaded_at": db.get_setting("st_inventory_loaded_at", ""),
        "st_inventory_source": db.get_setting("st_inventory_source", ""),
        "count": len(alerts),
        "alerts": [
            {
                "part_number": a["part_number"],
                "name": a["name"],
                "qty": a["qty"],
                "threshold": a["threshold"],
                "threshold_source": a["threshold_source"],
                "avg_monthly_usage": a["avg_monthly_usage"],
                "avg_daily_usage": a["avg_daily_usage"],
                "lead_time_days": a["lead_time_days"],
                "lead_time_source": a["lead_time_source"],
                "lead_time_label": a["lead_time_label"],
                "planned_lead_days": a["planned_lead_days"],
                "actual_lead_days": a["actual_lead_days"],
                "delay_days": a["delay_days"],
                "days_of_cover": a["days_of_cover"],
                "last_purchase_date": a["last_purchase_date"],
                "last_purchase_supplier": a["last_purchase_supplier"],
                "note": a["note"],
                "on_order_qty": a["on_order_qty"],
                "next_expected_date": a["next_expected_date"],
                "on_order_note": a["on_order_note"],
                "moq": a["moq"],
                "moq_source": a["moq_source"],
                "last_receipt_qty": a.get("last_receipt_qty"),
                "last_receipt_date": a.get("last_receipt_date", ""),
                "last_receipt_bill": a.get("last_receipt_bill", ""),
                "suggested_po_qty": a["suggested_po_qty"],
            }
            for a in alerts
        ],
    }


@router.get("/watchlist")
async def get_low_stock_watchlist():
    main_parts = low_stock.main_file_parts()
    rows = low_stock.list_rows(main_parts)
    return {
        **low_stock.main_file_status(main_parts),
        "settings": low_stock.get_config(),
        "rows": rows,
        "low_count": sum(1 for r in rows if r["status"] == "low"),
        "last_suggested_at": low_stock.last_suggested_at(),
        "st_inventory_loaded_at": db.get_setting("st_inventory_loaded_at", ""),
    }


@router.get("/settings")
async def get_low_stock_settings():
    return low_stock.get_config()


@router.post("/settings")
async def save_low_stock_settings(req: LowStockSettingsRequest):
    values = req.dict(exclude_unset=True)
    values = {k: v for k, v in values.items() if v is not None}
    try:
        return low_stock.save_config(values, actor=ACTOR)
    except low_stock.LowStockError as exc:
        raise _http_error(exc) from None


@router.post("/suggest")
async def suggest_low_stock():
    try:
        summary = await run_in_threadpool(low_stock.run_suggestion, ACTOR)
    except ErpError as exc:
        raise HTTPException(503, f"從正航建議常用料號失敗：{exc}（關注清單維持原狀）") from None
    return {"ok": True, **summary}


@router.post("/items")
async def add_low_stock_item(req: LowStockAddRequest):
    try:
        row = await run_in_threadpool(
            low_stock.add_part, req.part_number, req.threshold_qty, req.lead_time_days, req.note, ACTOR
        )
    except low_stock.LowStockError as exc:
        raise _http_error(exc) from None
    return {"ok": True, "part_number": row["part_number"]}


@router.post("/items/update")
async def update_low_stock_item(req: LowStockUpdateRequest):
    fields = req.dict(exclude_unset=True)
    part = fields.pop("part_number")
    if "enabled" in fields and fields["enabled"] is None:
        fields.pop("enabled")
    if "note" in fields and fields["note"] is None:
        fields["note"] = ""
    try:
        low_stock.update_part(part, actor=ACTOR, **fields)
    except low_stock.LowStockError as exc:
        raise _http_error(exc) from None
    return {"ok": True}


@router.post("/items/remove")
async def remove_low_stock_item(req: LowStockRemoveRequest):
    if not low_stock.remove_part(req.part_number, actor=ACTOR):
        raise HTTPException(404, "找不到此料號")
    return {"ok": True}
