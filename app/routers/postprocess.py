from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from .. import database as db
from ..models import PostprocessLinkCreate, PostprocessLinkUpdate, PostprocessSuggestRequest
from ..services import postprocess

router = APIRouter(prefix="/postprocess")

# POST / PATCH / DELETE 皆為 /api 下的非 GET，沿用 edit_auth middleware，不加入豁免名單。


def _log(action: str, detail: str) -> None:
    try:
        db.log_activity(action, detail)
    except Exception:
        pass


def _need(link_id: int) -> dict:
    link = db.get_postprocess_link(link_id)
    if link is None:
        raise HTTPException(404, "找不到這筆對應")
    return link


@router.get("/links")
def list_links(include_rejected: bool = False):
    return {"links": db.list_postprocess_links(include_rejected=include_rejected)}


@router.post("/links")
def create_link(req: PostprocessLinkCreate):
    try:
        link_id = db.add_postprocess_link(req.base_part, req.process_part, req.ratio, note=req.note, source="manual")
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None
    _log("postprocess_link", f"新增後段加工對應 {req.base_part.strip().upper()} → {req.process_part.strip().upper()}")
    return {"ok": True, "link": db.get_postprocess_link(link_id)}


@router.patch("/links/{link_id}")
def update_link(link_id: int, req: PostprocessLinkUpdate):
    _need(link_id)
    fields = {k: v for k, v in req.dict().items() if v is not None}
    try:
        link = db.update_postprocess_link(link_id, **fields)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None
    _log("postprocess_link", f"修改後段加工對應 #{link_id}")
    return {"ok": True, "link": link}


@router.delete("/links/{link_id}")
def delete_link(link_id: int):
    link = _need(link_id)
    db.delete_postprocess_link(link_id)
    _log("postprocess_link", f"刪除後段加工對應 {link['base_part']} → {link['process_part']}")
    return {"ok": True}


@router.post("/links/{link_id}/confirm")
def confirm_link(link_id: int):
    _need(link_id)
    link = db.update_postprocess_link(link_id, status="confirmed", enabled=True)
    _log("postprocess_link", f"確認後段加工對應 {link['base_part']} → {link['process_part']}")
    return {"ok": True, "link": link}


@router.post("/links/{link_id}/reject")
def reject_link(link_id: int):
    _need(link_id)
    link = db.update_postprocess_link(link_id, status="rejected")
    _log("postprocess_link", f"駁回後段加工對應 {link['base_part']} → {link['process_part']}")
    return {"ok": True, "link": link}


@router.post("/suggest")
async def suggest(req: PostprocessSuggestRequest):
    """產生待確認候選（BOM 虛線行 + 正航採購單歷史）；正航連不上時只做 BOM，不會失敗。"""
    return await run_in_threadpool(postprocess.suggest, req.use_erp)


@router.get("/check")
def check_parts(parts: str = ""):
    """輸入料號（逗號分隔）→ 其中有生效對應的：{base: [{process_part, ratio}]}（手動開單建議加工行用）。"""
    keys = [p.strip().upper() for p in parts.split(",") if p.strip()]
    return {"links": db.active_postprocess_links(keys) if keys else {}}
