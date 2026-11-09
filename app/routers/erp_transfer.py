"""一鍵調撥 API（倉庫 → 一鍵調撥）：預覽只讀；確認寫入每個批次各開一張調撥單。"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool

from .. import database as db
from ..models import ErpTransferCreateRequest
from ..services import erp_transfer
from .erp_receive import _http_error

router = APIRouter(prefix='/shortage-po/transfer')


@router.get('/config')
async def config():
    """寫入公司與是否可用；公司由伺服器環境決定，瀏覽器不能指定。"""
    return erp_transfer.get_config()


@router.post('/preview')
async def preview():
    try:
        return await run_in_threadpool(erp_transfer.preview)
    except Exception as exc:
        raise _http_error(exc) from None


@router.post('/create')
async def create(req: ErpTransferCreateRequest, request: Request):
    client_ip = request.client.host if request.client else ''
    try:
        return await run_in_threadpool(erp_transfer.create, req.dict(), 'web', client_ip)
    except Exception as exc:
        raise _http_error(exc) from None


@router.get('/history')
async def history():
    return {'records': await run_in_threadpool(db.list_erp_transfers, '', 200)}


@router.post('/recheck/{record_id}')
async def recheck(record_id: int):
    try:
        return await run_in_threadpool(erp_transfer.recheck, record_id)
    except Exception as exc:
        raise _http_error(exc) from None
