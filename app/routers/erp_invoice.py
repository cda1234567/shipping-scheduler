from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from ..models import ErpInvoicePrintRequest, ErpInvoiceRequest
from ..services import erp_invoice, erp_invoice_print, erp_purchase
from ..services.erp_stock import ErpError, ErpNotConfigured, ErpUnavailable

router = APIRouter(prefix="/erp-invoice")

# preview / create 都是 /api 下的 POST，沿用 edit_auth middleware（需登入編輯），不加入豁免名單。
ACTOR = "web"


def _http_error(exc: Exception) -> HTTPException:
    message = erp_purchase._safe_message(exc)
    if isinstance(exc, erp_invoice_print.InvoiceNotFound):
        return HTTPException(404, message)
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


@router.get("/print-data")
async def invoice_print_data(bill_no: str = ""):
    """列印用資料（唯讀，不看寫入開關）：單號需為完整 ST-yyyynnn。"""
    try:
        return await run_in_threadpool(erp_invoice_print.get_print_data, bill_no)
    except ErpError as exc:
        raise _http_error(exc) from None


# ── Invoice 列印（伺服器 PDF，同進貨單列印）───────────────────────────────────
# 全程唯讀（get_print_data 只下 SELECT）；POST /print 送到主機列印小幫手（RM_PRINT_HELPER_ENABLED，預設關閉）。

def _invoice_print_payload(bill_no: str):
    from ..services import invoice_print_pdf

    data = erp_invoice_print.get_print_data(bill_no)
    try:
        pdf, info = invoice_print_pdf.render_pdf(data)
    except RuntimeError as exc:  # 缺字型
        raise HTTPException(500, str(exc)) from None
    return data, pdf, info


async def _invoice_print(bill_no: str):
    try:
        return await run_in_threadpool(_invoice_print_payload, bill_no)
    except ErpError as exc:
        raise _http_error(exc) from None


def _invoice_print_info(data, info):
    from urllib.parse import quote
    from ..services import rm_print_dispatch

    return {**info, "bill_no": data.get("bill_no"), "helper_enabled": rm_print_dispatch.helper_config()["enabled"],
            "pdf_url": f"/api/erp-invoice/print.pdf?bill_no={quote(str(data.get('bill_no') or ''))}"}


@router.get("/print.pdf")
async def invoice_print_pdf(bill_no: str = ""):
    from fastapi.responses import Response

    data, pdf, info = await _invoice_print(bill_no)
    return Response(pdf, media_type="application/pdf", headers={
        "Content-Disposition": f"inline; filename={data.get('bill_no') or 'Invoice'}.pdf",
        "X-Invoice-Fits-Half-Page": "1" if info["fits_half_page"] else "0",
        "Cache-Control": "no-store",
    })


@router.get("/print-info")
async def invoice_print_info(bill_no: str = ""):
    data, _pdf, info = await _invoice_print(bill_no)
    return _invoice_print_info(data, info)


@router.post("/print")
async def invoice_print(req: ErpInvoicePrintRequest):
    from ..services import rm_print_dispatch

    data, pdf, info = await _invoice_print(req.bill_no)
    out = _invoice_print_info(data, info)
    if not out["helper_enabled"]:
        return {**out, "ok": False, "sent": False, "message": f"未啟用自動送印：請開 PDF 自行列印到「{info['printer']}」"}
    try:
        res = await run_in_threadpool(rm_print_dispatch.send_pdf_with_auto_restart, pdf, info["printer"], data.get("bill_no") or "Invoice")
    except rm_print_dispatch.PrintHelperError as exc:
        raise HTTPException(502, str(exc)) from None
    return {**out, **res, "sent": True}
