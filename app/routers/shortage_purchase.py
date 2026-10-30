from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from .. import database as db
from ..models import ShortagePoCategoryRequest, ShortagePoCreateRequest, ShortagePoRequest
from ..services import erp_purchase, shortage_purchase
from ..services.erp_stock import ErpError
from .erp_purchase import ACTOR, _http_error

router = APIRouter(prefix="/shortage-po")

# POST 皆為 /api 下的非 GET，沿用 edit_auth middleware，不加入豁免名單。
LATEST_BATCH_WINDOW_SECONDS = 600


def _parse_ts(text: str):
    try:
        return datetime.fromisoformat(str(text))
    except ValueError:
        return None


def _latest_batch_ids(orders: list[dict]) -> list[int]:
    """最近一次「寫入主檔」那批：發料時間與最新一筆相差 10 分鐘內的訂單。"""
    stamped = [(o["id"], _parse_ts(o.get("dispatched_at"))) for o in orders]
    stamped = [(i, t) for i, t in stamped if t is not None]
    if not stamped:
        return []
    newest = max(t for _, t in stamped)
    return [
        i for i, t in stamped
        if abs((newest - t).total_seconds()) <= LATEST_BATCH_WINDOW_SECONDS
    ]


@router.get("/list")
def shortage_list(order_ids: str = ""):
    try:
        ids = [int(x) for x in order_ids.split(",") if x.strip()]
    except ValueError:
        raise HTTPException(400, "order_ids 格式不正確") from None
    if not ids:
        raise HTTPException(400, "請先選擇訂單")
    return shortage_purchase.build_shortage_list(ids)


@router.get("/orders")
async def shortage_orders():
    orders = await run_in_threadpool(db.list_dispatched_orders_for_purchase)
    return {"orders": orders, "latest_batch_ids": _latest_batch_ids(orders)}


@router.post("/category")
async def set_category(req: ShortagePoCategoryRequest):
    try:
        await run_in_threadpool(db.set_part_purchase_category, req.part_number, req.category, ACTOR)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None
    return {"ok": True, "part_number": req.part_number.strip().upper(), "category": req.category}


@router.post("/preview")
async def shortage_preview(req: ShortagePoRequest):
    try:
        groups = await run_in_threadpool(
            shortage_purchase.prepare_groups, [g.dict() for g in req.groups], req.order_ids
        )
        results = await run_in_threadpool(erp_purchase.preview_many, groups)
    except ErpError as exc:
        raise _http_error(exc) from None
    return {"results": results}


@router.post("/create")
async def shortage_create(req: ShortagePoCreateRequest, request: Request):
    if req.confirm is not True:
        raise HTTPException(400, "需確認後才會寫入正航（confirm=true）")
    ip = request.client.host if request.client else ""
    try:
        groups = await run_in_threadpool(
            shortage_purchase.prepare_groups, [g.dict() for g in req.groups], req.order_ids
        )
        if req.allow_duplicate is not True:
            dups = await run_in_threadpool(shortage_purchase.find_group_duplicates, groups)
            if dups:  # 沒有寫入任何東西；前端確認後帶 allow_duplicate=true 重送
                return {"results": [], "duplicate_warning": dups}
        results = await run_in_threadpool(shortage_purchase.create_many, groups, ACTOR, ip)
    except ErpError as exc:
        raise _http_error(exc) from None
    return {"results": results}
