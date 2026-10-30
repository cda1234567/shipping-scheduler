"""沒有年份的日期推斷（例如排程表出貨日格式 m/d）。

使用者在 Excel 輸入「1/29」時，Excel 會存成輸入當年的日期（2026-01-29）而顯示 1/29，
實際想表達的是下一個 1/29。規則：以今天（台北時區）為準，今年的該月日若比今天早超過
window_days 天，就用明年；明確帶年份的日期一律不動。
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Optional

from .local_time import local_now

WINDOW_DAYS = 60

_QUOTED_RE = re.compile(r'"[^"]*"|\\.|\[[^\]]*\]|_.|\*.')
_YEARLESS_TEXT_RE = re.compile(r"^\s*(\d{1,2})\s*[/\-.月]\s*(\d{1,2})\s*日?\s*$")


def format_is_yearless(number_format: Any) -> bool:
    """儲存格格式是日期格式、但沒有年份（沒有 y / e 記號）。例：m/d、mm/dd、m"月"d"日"。
    General / 空白 / 非日期格式一律回 False（無從判斷，不動它）。"""
    fmt = _QUOTED_RE.sub("", str(number_format or ""))
    fmt = fmt.split(";")[0].lower()
    if not fmt or fmt == "general":
        return False
    if "y" in fmt or "e" in fmt:
        return False
    return "d" in fmt and "m" in fmt


def _safe_date(year: int, month: int, day: int) -> Optional[date]:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def infer_year(month: int, day: int, today: Optional[date] = None, window_days: int = WINDOW_DAYS) -> date:
    """今年的月/日比今天早超過 window_days 天就用明年，否則用今年。2/29 遇到非閏年會往後找最近的閏年。"""
    today = today or local_now().date()
    this_year = _safe_date(today.year, month, day)
    if this_year is not None and this_year >= today - timedelta(days=window_days):
        return this_year
    year = today.year + 1
    for _ in range(8):
        candidate = _safe_date(year, month, day)
        if candidate is not None:
            return candidate
        year += 1
    raise ValueError(f"無效的日期：{month}/{day}")


def apply_yearless(value: Any, number_format: Any, today: Optional[date] = None) -> Any:
    """datetime / date 儲存格值：格式沒有年份時改用推斷的年份，其餘原樣回傳。"""
    if isinstance(value, (datetime, date)) and format_is_yearless(number_format):
        return datetime(*infer_year(value.month, value.day, today).timetuple()[:3])
    return value


def parse_yearless_text(text: Any, today: Optional[date] = None) -> Optional[date]:
    """「1/29」「1-29」「1月29日」→ 推斷年份後的日期；不是無年份格式或日期無效回 None。"""
    match = _YEARLESS_TEXT_RE.match(str(text or ""))
    if not match:
        return None
    month, day = int(match[1]), int(match[2])
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    try:
        return infer_year(month, day, today)
    except ValueError:
        return None


def normalize_date_text(value: Any, today: Optional[date] = None) -> Any:
    """字串若是無年份日期就轉成 YYYY-MM-DD；其他（含已有年份的）原樣回傳。"""
    if isinstance(value, str):
        inferred = parse_yearless_text(value, today)
        if inferred is not None:
            return inferred.isoformat()
    return value
