"""台灣辦公日曆：預交日（PreInDate）遇假日往前移到最近的上班日。

資料來源（優先順序）：
1. 資料目錄 holidays/tw_YYYY.csv（override，可不重新 build 直接更新；update_tw_holidays 工具會寫到這裡）
2. 隨程式打包的 app/resources/holidays/tw_YYYY.csv（行政院人事行政總處，https://data.gov.tw/dataset/14718）
3. 都沒有時為備援：週末 + 固定國定假日（1/1、2/28、4/4、4/5、5/1、9/28、10/10、10/25、12/25）

CSV 欄位：西元日期,星期,是否放假,備註（0 = 上班、2 = 放假）。檔內有列出的日期以檔案為準
（所以週末若標 0 即為補班、平日標 2 即為放假）；檔內沒有的日期（不完整的檔）退回「週末放假」。
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .. import config
from .local_time import local_now

logger = logging.getLogger(__name__)

PACKAGED_DIR = Path(__file__).resolve().parents[1] / "resources" / "holidays"
FIXED_HOLIDAYS = ((1, 1), (2, 28), (4, 4), (4, 5), (5, 1), (9, 28), (10, 10), (10, 25), (12, 25))
MAX_BACK_DAYS = 31
SOURCE_LABELS = {"override": "自訂更新檔", "official": "行政院人事行政總處", "fallback": "備援（週末＋固定國定假日）"}

_cache: Dict[int, Tuple[Any, Dict[str, Any]]] = {}


def override_dir() -> Path:
    return Path(config.DATA_DIR) / "holidays"


def _file_name(year: int) -> str:
    return f"tw_{int(year)}.csv"


def _parse_csv(text: str) -> Dict[date, bool]:
    """回傳 {日期: 是否上班}；格式不符的列略過。"""
    out: Dict[date, bool] = {}
    for row in csv.reader(io.StringIO(text.lstrip("﻿"))):
        if len(row) < 3:
            continue
        raw = row[0].strip().replace("/", "").replace("-", "")
        if len(raw) != 8 or not raw.isdigit():
            continue  # 標題列
        try:
            d = datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            continue
        flag = row[2].strip()
        if flag not in ("0", "1", "2"):
            try:
                flag = str(int(float(flag)))
            except ValueError:
                continue
        out[d] = flag == "0"
    return out


def _mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def _load_year(year: int) -> Dict[str, Any]:
    candidates = (("override", override_dir() / _file_name(year)), ("official", PACKAGED_DIR / _file_name(year)))
    sig = tuple((label, str(p), _mtime(p)) for label, p in candidates)
    cached = _cache.get(year)
    if cached and cached[0] == sig:
        return cached[1]
    info: Dict[str, Any] = {"year": year, "source": "fallback", "days": {}, "path": ""}
    for label, path in candidates:
        if _mtime(path) is None:
            continue
        try:
            days = _parse_csv(path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeDecodeError) as exc:
            logger.warning("讀取假日檔失敗 %s：%s", path.name, type(exc).__name__)
            continue
        if sum(1 for d in days if d.year == year) >= 300:  # 不完整的檔視為無效，退回下一個來源
            info = {"year": year, "source": label, "days": days, "path": str(path)}
            break
        logger.warning("假日檔 %s 資料不完整（%d 天），已略過", path.name, len(days))
    _cache[year] = (sig, info)
    return info


def clear_cache() -> None:
    _cache.clear()


def year_source(year: int) -> str:
    return _load_year(year)["source"]


def is_working_day(d: date) -> bool:
    info = _load_year(d.year)
    if info["source"] != "fallback" and d in info["days"]:
        return info["days"][d]
    if d.weekday() >= 5:
        return False
    if info["source"] == "fallback" and (d.month, d.day) in FIXED_HOLIDAYS:
        return False
    return True


def next_working_day(d: date) -> date:
    cur = d
    for _ in range(MAX_BACK_DAYS):
        if is_working_day(cur):
            return cur
        cur += timedelta(days=1)
    return d


def previous_working_day(d: date) -> date:
    cur = d
    for _ in range(MAX_BACK_DAYS):
        if is_working_day(cur):
            return cur
        cur -= timedelta(days=1)
    return d


def _md(d: date) -> str:
    return f"{d.month}/{d.day}"


def shift_due_date(d: date, today: Optional[date] = None) -> Dict[str, Any]:
    """預交日套用假日規則（純計算）。

    - d 不是上班日 → 往前（更早）移到最近的上班日。
    - 結果早於今天 → 今天若是上班日用今天，否則用今天之後第一個上班日。
    回傳 date / original / shifted / reason（顯示用文字）/ reason_code（'' | 'holiday' | 'clamp'）/ fallback_year。
    """
    today = today or local_now().date()
    d_working = is_working_day(d)
    moved = d if d_working else previous_working_day(d)
    clamped = moved < today
    if clamped:
        moved = next_working_day(today)
    shifted = moved != d
    if not shifted:
        code, reason = "", ""
    elif clamped:
        code = "clamp"
        reason = f"遇假日，改為 {_md(moved)}" if not d_working else f"早於今天，改為 {_md(moved)}"
    else:
        code, reason = "holiday", f"遇假日提前至 {_md(moved)}"
    fallback = year_source(d.year) == "fallback" or year_source(moved.year) == "fallback"
    return {
        "date": moved, "original": d, "shifted": shifted, "reason": reason, "reason_code": code,
        "fallback_year": fallback,
    }


def shift_iso(value: Any, today: Optional[date] = None) -> Dict[str, Any]:
    """字串版（YYYY-MM-DD）：回 API 用的 dict（日期為字串）；日期無法解析時原樣回傳、不調整。"""
    text = str(value or "").strip().replace("/", "-")[:10]
    try:
        d = datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return {"date": str(value or ""), "original": str(value or ""), "shifted": False, "reason": "",
                "reason_code": "", "fallback_year": False}
    r = shift_due_date(d, today)
    return {**r, "date": r["date"].isoformat(), "original": r["original"].isoformat()}


def calendar_status(today: Optional[date] = None) -> Dict[str, Any]:
    """已載入的年份與來源（含今年 / 明年，以及打包與 override 目錄內有的年份）。"""
    today = today or local_now().date()
    years = {today.year, today.year + 1}
    for folder in (PACKAGED_DIR, override_dir()):
        try:
            for p in folder.glob("tw_*.csv"):
                digits = p.stem[3:]
                if digits.isdigit():
                    years.add(int(digits))
        except OSError:
            pass
    items = []
    for y in sorted(years):
        info = _load_year(y)
        items.append({
            "year": y, "source": info["source"], "source_label": SOURCE_LABELS[info["source"]],
            "fallback": info["source"] == "fallback",
            "holidays": sum(1 for d, w in info["days"].items() if not w and d.year == y),
        })
    return {"today": today.isoformat(), "override_dir": str(override_dir()), "years": items,
            "any_fallback": any(i["fallback"] for i in items if i["year"] in (today.year, today.year + 1))}
