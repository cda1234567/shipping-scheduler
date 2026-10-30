from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from .. import database as db
from ..models import PullinConfirmRequest, PullinStatusRequest
from ..services import po_pullin

router = APIRouter(prefix="/po-pullin")

# POST 皆為 /api 下的非 GET，沿用 edit_auth middleware，不加入豁免名單。
ACTOR = "web"


def _ids(text: str) -> list[int]:
    try:
        return [int(x) for x in text.split(",") if x.strip()]
    except ValueError:
        raise HTTPException(400, "order_ids 格式不正確") from None


@router.get("/preview")
async def preview(order_ids: str = "", new_ship_date: str = ""):
    """唯讀：重算需要到貨日並對照正航未結採購單；不寫任何資料（ERP 失敗時仍回本機資料）。"""
    try:
        return await run_in_threadpool(po_pullin.build_pullin_list, _ids(order_ids), new_ship_date)
    except po_pullin.PullinError as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/confirm")
async def confirm(req: PullinConfirmRequest):
    """保存提前清單（供提醒徽章 / 狀態追蹤）；勾選 update_order_ship_date 才會更新訂單出貨日。"""
    try:
        return await run_in_threadpool(
            po_pullin.confirm, req.order_ids, req.new_ship_date, req.update_order_ship_date, req.dry_run, ACTOR
        )
    except po_pullin.PullinError as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/status")
async def set_status(req: PullinStatusRequest):
    try:
        n = await run_in_threadpool(
            po_pullin.set_status, [l.dict() for l in req.lines], req.status, req.note, ACTOR
        )
    except po_pullin.PullinError as exc:
        raise HTTPException(400, str(exc)) from None
    return {"ok": True, "updated": n, "unresolved": await run_in_threadpool(po_pullin.unresolved_count)}


@router.get("/badge")
async def badge():
    return {"unresolved": await run_in_threadpool(po_pullin.unresolved_count)}


@router.get("/items")
async def items(unresolved_only: bool = False):
    return {"items": await run_in_threadpool(db.list_pullin_items, unresolved_only)}
