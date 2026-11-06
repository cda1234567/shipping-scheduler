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


# ── 進貨單列印（正航進貨憑單版面）──────────────────────────────────────────
# GET 只讀（唯讀帳號讀回正航 RM 單，不寫入）；POST /print 送到主機列印小幫手（RM_PRINT_HELPER_ENABLED，預設關閉）。

def _print_payload(receipt_id: int):
    from ..services import rm_print, rm_print_data

    try:
        data = rm_print_data.load_print_data(receipt_id)
    except rm_print_data.ReceiptNotPrintable as exc:
        raise HTTPException(404, str(exc)) from None
    try:
        pdf, info = rm_print.render_pdf(data)
    except RuntimeError as exc:  # 缺字型
        raise HTTPException(500, str(exc)) from None
    return data, pdf, info


def _print_info(receipt_id: int, data, info):
    from ..services import rm_print_dispatch

    helper = rm_print_dispatch.helper_config()
    return {
        "receipt_id": receipt_id, "rm_no": data.get("rm_no"), "source": data.get("source"),
        "warning": data.get("warning", ""), **info, "helper_enabled": helper["enabled"],
        "pdf_url": f"/api/erp-receive/receipts/{receipt_id}/print.pdf",
    }


@router.get("/receipts/{receipt_id}/print.pdf")
async def receipt_print_pdf(receipt_id: int):
    from fastapi.responses import Response
    from urllib.parse import quote

    data, pdf, info = await run_in_threadpool(_print_payload, receipt_id)
    name = f"{data.get('rm_no') or 'RM'}.pdf"
    return Response(pdf, media_type="application/pdf", headers={
        "Content-Disposition": f"inline; filename*=UTF-8''{quote(name)}",
        "X-RM-Fits-Half-Page": "1" if info["fits_half_page"] else "0",
        "Cache-Control": "no-store",
    })


@router.get("/receipts/{receipt_id}/print-info")
async def receipt_print_info(receipt_id: int):
    data, _pdf, info = await run_in_threadpool(_print_payload, receipt_id)
    return _print_info(receipt_id, data, info)


@router.post("/receipts/{receipt_id}/print")
async def receipt_print(receipt_id: int):
    from ..services import rm_print_dispatch

    data, pdf, info = await run_in_threadpool(_print_payload, receipt_id)
    out = _print_info(receipt_id, data, info)
    if not out["helper_enabled"]:
        return {**out, "ok": False, "sent": False, "message": f"未啟用自動送印：請開 PDF 自行列印到「{info['printer']}」"}
    try:
        res = await run_in_threadpool(rm_print_dispatch.send_pdf_with_auto_restart, pdf, info["printer"], data.get("rm_no") or "RM")
    except rm_print_dispatch.PrintHelperError as exc:
        raise HTTPException(502, str(exc)) from None
    return {**out, **res, "sent": True}
