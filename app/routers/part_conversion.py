from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from ..models import PartConversionRequest, PartConversionRestoreRequest
from ..services import part_conversion

router = APIRouter(prefix="/part-conversion")

# POST 皆為 /api 下的非 GET，沿用 edit_auth middleware，不加入豁免名單。
ACTOR = "web"


def _http(exc: part_conversion.ConversionError) -> HTTPException:
    return HTTPException(exc.status_code, str(exc))


@router.get("/preview")
async def preview(part_a: str = "", part_b: str = ""):
    """唯讀 dry-run：列出本網站所有使用 A 的 BOM（含與 B 合併的標記）。"""
    try:
        return await run_in_threadpool(part_conversion.preview, part_a, part_b)
    except part_conversion.ConversionError as exc:
        raise _http(exc) from None


@router.post("/convert")
async def convert(req: PartConversionRequest):
    try:
        return await run_in_threadpool(part_conversion.convert, req.part_a, req.part_b, ACTOR, req.dry_run)
    except part_conversion.ConversionError as exc:
        raise _http(exc) from None


@router.get("/list")
async def list_conversions():
    return await run_in_threadpool(part_conversion.list_conversions)


@router.post("/{conversion_id}/restore")
async def restore(conversion_id: int, req: PartConversionRestoreRequest):
    try:
        return await run_in_threadpool(part_conversion.restore, conversion_id, ACTOR, req.force)
    except part_conversion.ConversionError as exc:
        raise _http(exc) from None
