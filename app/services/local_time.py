from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..config import cfg

DEFAULT_TIMEZONE = "Asia/Taipei"


@lru_cache(maxsize=1)
def get_app_timezone() -> tzinfo:
    timezone_name = str(
        cfg("app.timezone", cfg("system.timezone", DEFAULT_TIMEZONE)) or DEFAULT_TIMEZONE
    ).strip() or DEFAULT_TIMEZONE
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=8), name=DEFAULT_TIMEZONE)


def local_now() -> datetime:
    """回傳台北時區的現在時間，並以 naive datetime 供既有程式沿用。"""
    return datetime.now(get_app_timezone()).replace(tzinfo=None)


def local_fromtimestamp(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, get_app_timezone()).replace(tzinfo=None)
