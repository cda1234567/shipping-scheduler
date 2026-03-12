"""活動日誌路由"""
from __future__ import annotations
from fastapi import APIRouter
from .. import database as db

router = APIRouter()


@router.get("/logs")
async def get_logs(limit: int = 100):
    logs = db.get_activity_logs(limit=limit)
    return {"logs": logs}
