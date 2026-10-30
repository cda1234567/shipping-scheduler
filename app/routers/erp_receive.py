from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from ..models import ErpReceiveRequest
from ..services import erp_purchase, erp_receive
from ..services.erp_stock import ErpError, ErpNotConfigured, ErpUnavailable

router = APIRouter(prefix="/erp-receive")

# preview / create 都是 /api 下的 POST，沿用 edit_auth middleware（需登入編輯），不加入豁免名單。
ACTOR = "web"


def _http_error(exc: Exception) -> HTTPException:
    message = erp_purchase._safe_message(exc)
    if isinstance(exc, erp_purchase.DuplicateSubmitError):
        return HTTPException(409, message)
    if isinstance(exc, erp_purchase.PoValidationError):
        return HTTPException(400, message)
    if isinstance(exc, (ErpNotConfigured, ErpUnavailable)):
        return HTTPException(503, message)
    return HTTPException(500, message)


@router.get("/config")
async def receive_config():
    return erp_receive.get_config()


@router.get("/open-lines")
async def receive_open_lines(vendor: str = "", q: str = "", limit: int = 300):
    try:
        return await run_in_threadpool(erp_receive.list_open_lines, vendor, q, limit)
    except ErpError as exc:
        raise _http_error(exc) from None


@router.post("/preview")
async def receive_preview(req: ErpReceiveRequest):
    try:
        return await run_in_threadpool(erp_receive.preview_receive, req.dict())
    except ErpError as exc:
        raise _http_error(exc) from None


@router.post("/create")
async def receive_create(req: ErpReceiveRequest, request: Request):
    ip = request.client.host if request.client else ""
    try:
        return await run_in_threadpool(erp_receive.create_receive, req.dict(), ACTOR, ip)
    except erp_receive.OverReceiveWarning as exc:
        # 200 + over_receive_warning：沒有寫入任何東西，前端確認後帶 confirm=true 重送
        return {"ok": False, "over_receive_warning": exc.items}
    except Exception as exc:  # 已記錄 error 紀錄；統一回應，不洩漏細節
        raise _http_error(exc) from None


@router.get("/history")
async def receive_history(limit: int = 50):
    return {"receipts": await run_in_threadpool(erp_receive.list_history, max(1, min(int(limit), 200)))}
