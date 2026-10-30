from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..services import tw_holidays
from ..services.local_time import local_now

router = APIRouter(prefix="/holidays")

DEFAULT_LEAD_DAYS = 14


@router.get("/status")
def holiday_status():
    """已載入的日曆年份與來源（official / override / fallback）。"""
    return tw_holidays.calendar_status()


@router.get("/shift")
def holiday_shift(date: str = ""):
    """預交日套用假日規則（遇假日往前、不早於今天）；回傳原日期、調整後日期與說明。"""
    from datetime import datetime

    try:
        datetime.strptime(date.strip().replace("/", "-")[:10], "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "date 格式需為 YYYY-MM-DD") from None
    return tw_holidays.shift_iso(date)


@router.get("/default-due")
def default_due(days: int = DEFAULT_LEAD_DAYS):
    """手動開採購單的預設預交日：今天 + days 天，再套用假日規則。"""
    from datetime import timedelta

    days = max(0, min(int(days), 365))
    raw = local_now().date() + timedelta(days=days)
    return tw_holidays.shift_iso(raw.isoformat())
