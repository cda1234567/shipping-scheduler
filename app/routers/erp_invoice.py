from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from ..models import ErpInvoiceRequest
from ..services import erp_invoice, erp_purchase
from ..services.erp_stock import ErpError, ErpNotConfigured, ErpUnavailable

router = APIRouter(prefix="/erp-invoice")

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
async def invoice_config():
    return erp_invoice.get_config()


@router.get("/open-lines")
async def invoice_open_lines(customer: str = "", q: str = "", limit: int = 300):
    try:
        return await run_in_threadpool(erp_invoice.list_open_lines, customer, q, limit)
    except ErpError as exc:
        raise _http_error(exc) from None


@router.post("/preview")
async def invoice_preview(req: ErpInvoiceRequest):
    try:
        return await run_in_threadpool(erp_invoice.preview_invoice, req.dict())
    except ErpError as exc:
        raise _http_error(exc) from None


@router.post("/create")
async def invoice_create(req: ErpInvoiceRequest, request: Request):
    ip = request.client.host if request.client else ""
    try:
        return await run_in_threadpool(erp_invoice.create_invoice, req.dict(), ACTOR, ip)
    except erp_invoice.InvoiceConfirmWarning as exc:
        # 200 + confirm_warning：沒有寫入任何東西，前端確認後帶 confirm=true 重送
        return {"ok": False, "confirm_warning": exc.items}
    except Exception as exc:  # 已記錄 error 紀錄；統一回應，不洩漏細節
        raise _http_error(exc) from None


@router.get("/history")
async def invoice_history(limit: int = 50):
    return {"invoices": await run_in_threadpool(erp_invoice.list_history, max(1, min(int(limit), 200)))}
