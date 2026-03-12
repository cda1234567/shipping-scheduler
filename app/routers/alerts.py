"""提醒機制路由"""
from __future__ import annotations
from fastapi import APIRouter
from .. import database as db

router = APIRouter()


@router.get("/alerts")
async def get_alerts(unread_only: bool = False):
    alerts = db.get_alerts(unread_only=unread_only)
    unread_count = len([a for a in db.get_alerts(unread_only=True)])
    return {"alerts": alerts, "unread_count": unread_count}


@router.post("/alerts/{alert_id}/read")
async def mark_read(alert_id: int):
    db.mark_alert_read(alert_id)
    return {"ok": True}


@router.post("/alerts/read-all")
async def mark_all_read():
    db.mark_all_alerts_read()
    return {"ok": True}
