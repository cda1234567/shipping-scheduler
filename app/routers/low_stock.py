from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from .. import database as db
from ..models import (
    LowStockAddRequest,
    LowStockPartSettingRequest,
    LowStockPrioritySuppliersRequest,
    LowStockRemoveRequest,
    LowStockSettingsRequest,
    LowStockSupplierLeadRequest,
    LowStockUpdateRequest,
)
from ..services import low_stock, low_stock_plan
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
                "receipt_gcd": a.get("receipt_gcd"),
                "receipt_count": a.get("receipt_count", 0),
                "receipt_qtys": a.get("receipt_qtys", []),
                "moq_floor_applied": a.get("moq_floor_applied", False),
                "suggested_po_qty": a["suggested_po_qty"],
                # 重點廠商列才有（ST／015 分開、撐不到預交日）；非重點項目內容不變
                **{k: a[k] for k in low_stock.PRIORITY_VIEW_KEYS if k in a},
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
        "priority_suppliers": low_stock.get_priority_suppliers(),
        "rows": rows,
        "low_count": sum(1 for r in rows if r["status"] == "low"),
        "last_suggested_at": low_stock.last_suggested_at(),
        "priority_refreshed_at": low_stock.priority_refreshed_at(),
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


@router.get("/priority-suppliers")
async def get_low_stock_priority_suppliers():
    return {"suppliers": low_stock.get_priority_suppliers()}


@router.post("/priority-suppliers")
async def save_low_stock_priority_suppliers(req: LowStockPrioritySuppliersRequest):
    try:
        return {"suppliers": low_stock.save_priority_suppliers(req.suppliers, actor=ACTOR)}
    except low_stock.LowStockError as exc:
        raise _http_error(exc) from None


@router.post("/suggest")
async def suggest_low_stock():
    try:
        summary = await run_in_threadpool(low_stock.run_suggestion, ACTOR)
    except ErpError as exc:
        raise HTTPException(503, f"從正航建議常用料號失敗：{exc}（關注清單維持原狀）") from None
    return {"ok": True, **summary}


# ── v2026.11.12.1：手動新增料號（R-B9）、開採購單建議量（R-E2/E3/E10） ──────────

def _plan_error(exc: low_stock_plan.LeadSettingError) -> HTTPException:
    return HTTPException(404 if isinstance(exc, low_stock_plan.LowStockPlanNotFound) else 400, str(exc))


@router.post("/items")
async def add_low_stock_item(req: LowStockAddRequest):
    """手動新增料號：進低庫存清單判斷（不在母體也列）；-TAB 不收。新增後唯讀抓這顆料的正航數字。"""
    try:
        return {"ok": True, **await run_in_threadpool(
            low_stock_plan.add_manual_part, req.part_number, req.lead_days, req.note, ACTOR)}
    except low_stock_plan.LeadSettingError as exc:
        status = 409 if "已手動新增" in str(exc) else None
        raise (HTTPException(status, str(exc)) if status else _plan_error(exc)) from None


@router.post("/items/update")
async def update_low_stock_item(req: LowStockUpdateRequest):
    """手動料號改交期／備註；enabled = 啟用勾選（只改本機旗標，任何清單上的料號都可改）。"""
    fields = req.dict(exclude_unset=True)
    part = fields.pop("part_number")
    enabled = fields.pop("enabled", None)
    result = {}
    try:
        if fields or enabled is None:
            result.update(await run_in_threadpool(
                lambda: low_stock_plan.update_manual_part(part, actor=ACTOR, **fields)))
        if enabled is not None:
            result.update(await run_in_threadpool(low_stock_plan.set_part_enabled, part, enabled, ACTOR))
    except low_stock_plan.LeadSettingError as exc:
        raise _plan_error(exc) from None
    return {"ok": True, **result}


@router.post("/items/remove")
async def remove_low_stock_item(req: LowStockRemoveRequest):
    if not await run_in_threadpool(low_stock_plan.remove_manual_part, req.part_number, ACTOR):
        raise HTTPException(404, "找不到此手動料號")
    return {"ok": True}


@router.get("/po-suggest")
async def get_low_stock_po_suggest(part: str):
    """開採購單建議量：缺料量照 MOQ 往上取整（不多買）＋同廠商其他缺料＋近期已開過的採購單。不寫正航。"""
    try:
        return await run_in_threadpool(low_stock_plan.po_suggest, part)
    except low_stock_plan.LeadSettingError as exc:
        raise _plan_error(exc) from None


# ── v2026.11.11.7：交期設定判斷的低庫存清單（GET 只讀本機快照，不連正航） ──────────

@router.get("/plan")
async def get_low_stock_plan():
    return await run_in_threadpool(low_stock_plan.build_plan)


@router.get("/plan/count")
async def get_low_stock_plan_count():
    return {"count": await run_in_threadpool(low_stock_plan.alert_count)}


@router.post("/plan/refresh")
async def refresh_low_stock_plan():
    """重新從正航（唯讀）抓資料算快照；失敗保留上次資料。"""
    result = await run_in_threadpool(low_stock_plan.refresh_snapshot, ACTOR)
    if not result.get("ok"):
        raise HTTPException(503, result.get("error") or "低庫存快照重算失敗")
    return result


@router.get("/lead-settings")
async def get_low_stock_lead_settings():
    return await run_in_threadpool(low_stock_plan.get_settings)


@router.post("/lead-settings/supplier")
async def save_low_stock_supplier_lead(req: LowStockSupplierLeadRequest):
    try:
        return await run_in_threadpool(
            low_stock_plan.save_supplier_lead, req.supplier_id, req.lead_days, req.anytime, ACTOR)
    except low_stock_plan.LeadSettingError as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/lead-settings/part")
async def save_low_stock_part_setting(req: LowStockPartSettingRequest):
    fields = req.dict(exclude_unset=True)
    part = fields.pop("part")
    if fields.get("anytime", False) is None:
        fields.pop("anytime")
    try:
        return await run_in_threadpool(lambda: low_stock_plan.save_part_setting(part, actor=ACTOR, **fields))
    except low_stock_plan.LeadSettingError as exc:
        raise HTTPException(400, str(exc)) from None
