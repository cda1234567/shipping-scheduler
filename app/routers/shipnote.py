"""出貨單 API：正航 S/C / Invoice / 客戶 PO 查品項（唯讀），套 OPENTEXT 模板匯出 xlsx。"""
from __future__ import annotations

import io
from typing import List, Union
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .. import database as db
from ..services import shipnote
from ..services.erp_stock import ErpError, ErpNotConfigured, ErpUnavailable
from ..services.server_downloads import maybe_server_save_bytes_response

router = APIRouter(prefix="/shipnote", tags=["shipnote"])

XLSX_MEDIA = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class ShipnoteSearchRequest(BaseModel):
    keywords: Union[str, List[str]] = ""


class ShipnoteExportRequest(BaseModel):
    type: str = "general"
    date: str = ""
    invoice_no: str = ""
    maker: str = shipnote.DEFAULT_MAKER
    mark: str = shipnote.MARKS[0]
    remark: str = ""
    customer: str = ""
    items: List[dict] = Field(default_factory=list)


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, shipnote.ShipnoteError):
        return HTTPException(400, str(exc))
    if isinstance(exc, (ErpNotConfigured, ErpUnavailable)):
        return HTTPException(503, str(exc))
    if isinstance(exc, ErpError):
        return HTTPException(500, str(exc))
    return HTTPException(500, f"出貨單處理失敗：{type(exc).__name__}")


@router.get("/config")
async def shipnote_config():
    return shipnote.config()


@router.post("/search")
async def shipnote_search(req: ShipnoteSearchRequest):
    try:
        return await run_in_threadpool(shipnote.search_items, req.keywords)
    except Exception as exc:
        raise _http_error(exc) from None


@router.post("/export")
async def shipnote_export(req: ShipnoteExportRequest, request: Request):
    try:
        content, filename = await run_in_threadpool(shipnote.export_shipnote, req.dict())
    except Exception as exc:
        raise _http_error(exc) from None
    db.log_activity("shipnote_export", f"{filename}：{len(req.items)} 筆")
    server_response = maybe_server_save_bytes_response(request, content, filename)
    if server_response:
        return server_response
    return StreamingResponse(
        io.BytesIO(content),
        media_type=XLSX_MEDIA,
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"},
    )
