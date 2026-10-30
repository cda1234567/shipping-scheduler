from __future__ import annotations

from typing import Optional

from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool

from ..services import automail_status

router = APIRouter(prefix="/automail")

# 全部唯讀：只向 automail 做 GET，不寄信、不寫入任何資料。


def _pos(po: Optional[str]):
    if po is None:
        return None
    return [p for p in po.split(",") if p.strip()]


@router.get("/po-status")
async def po_status(po: Optional[str] = None):
    return await run_in_threadpool(automail_status.get_status, False, _pos(po))


@router.post("/refresh")
async def refresh(po: Optional[str] = None):
    return await run_in_threadpool(automail_status.get_status, True, _pos(po))
