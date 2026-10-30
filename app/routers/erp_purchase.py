from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from ..models import ErpPoCreateRequest, ErpPoRequest
from ..services import erp_purchase
from ..services.erp_stock import ErpError, ErpNotConfigured, ErpUnavailable

router = APIRouter(prefix="/erp/po")

# preview / create 都是 /api 下的 POST，沿用 edit_auth middleware，不加入豁免名單。
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
async def po_config():
    return erp_purchase.get_config()


@router.get("/suppliers")
async def po_suppliers(q: str = ""):
    try:
        rows = await run_in_threadpool(erp_purchase.search_suppliers, q)
    except ErpError as exc:
        raise _http_error(exc) from None
    return {"suppliers": rows}


@router.get("/part-defaults")
async def po_part_defaults(part: str = ""):
    try:
        return await run_in_threadpool(erp_purchase.part_defaults, part)
    except ErpError as exc:
        raise _http_error(exc) from None


@router.post("/preview")
async def po_preview(req: ErpPoRequest):
    try:
        return await run_in_threadpool(erp_purchase.preview_po, req.dict())
    except ErpError as exc:
        raise _http_error(exc) from None


@router.post("/create")
async def po_create(req: ErpPoCreateRequest, request: Request):
    ip = request.client.host if request.client else ""
    try:
        return await run_in_threadpool(erp_purchase.create_po, req.dict(), ACTOR, ip)
    except erp_purchase.DuplicateWarning as exc:
        # 200 + duplicate_warning：沒有寫入任何東西，前端確認後帶 allow_duplicate=true 重送
        return {"ok": False, "duplicate_warning": exc.items}
    except ErpError as exc:
        raise _http_error(exc) from None
    except Exception as exc:  # 其他例外（已記錄 error 紀錄）統一回 500，不洩漏細節
        raise _http_error(exc) from None


@router.get("/history")
async def po_history(limit: int = 50):
    return {"orders": await run_in_threadpool(erp_purchase.list_history, max(1, min(int(limit), 200)))}
