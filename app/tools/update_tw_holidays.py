"""下載政府行政機關辦公日曆表（data.gov.tw 14718）到資料目錄 holidays/，不用重新 build 即可更新假日。

用法（容器內）：
    python -m app.tools.update_tw_holidays [--year 2028] [--dir DIR]
沒給 --year 時下載今年與明年。只寫入 override 目錄（預設 <data>/holidays/tw_YYYY.csv），不動隨程式打包的檔案。
"""
from __future__ import annotations

import argparse
import sys
import urllib.request
from datetime import date
from pathlib import Path
from typing import List, Optional

from ..services import tw_holidays

API_URL = "https://data.gov.tw/api/v2/rest/dataset/14718"
MIN_ROWS = 300


def pick_download_url(dataset: dict, year: int) -> Optional[str]:
    """找「{民國年}年中華民國政府行政機關辦公日曆表」的 CSV；優先完全相符，其次同名加註記（不含 Google 專用版）。"""
    roc = year - 1911
    title = f"{roc}年中華民國政府行政機關辦公日曆表"
    dists = (dataset.get("result") or dataset).get("distribution") or []
    exact, loose = [], []
    for d in dists:
        desc = str(d.get("resourceDescription") or "").strip()
        url = d.get("resourceDownloadUrl")
        if not url or "google" in desc.lower() or str(d.get("resourceFormat") or "CSV").upper() != "CSV":
            continue
        if desc == title:
            exact.append(url)
        elif desc.startswith(title):
            loose.append(url)
    return (exact or loose or [None])[-1]


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "shipping-scheduler/holiday-updater"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def update_year(year: int, dest_dir: Path, fetch=_get) -> str:
    import json

    dataset = json.loads(fetch(API_URL).decode("utf-8"))
    url = pick_download_url(dataset, year)
    if not url:
        raise RuntimeError(f"data.gov.tw 找不到 {year - 1911} 年（{year}）的辦公日曆表")
    text = fetch(url).decode("utf-8-sig")
    days = tw_holidays._parse_csv(text)
    if sum(1 for d in days if d.year == year) < MIN_ROWS:
        raise RuntimeError(f"下載的 {year} 年資料不完整（{len(days)} 天），未寫入")
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / f"tw_{year}.csv"
    tmp = target.with_suffix(".tmp")
    tmp.write_text("﻿" + text, encoding="utf-8")
    tmp.replace(target)
    holidays = sum(1 for d, w in days.items() if not w and d.year == year)
    return f"{year}：已寫入 {target}（{len(days)} 天，放假 {holidays} 天）"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="更新台灣辦公日曆（假日）資料")
    parser.add_argument("--year", type=int, action="append", help="西元年；可重複給，預設今年與明年")
    parser.add_argument("--dir", default="", help="輸出目錄，預設 <data>/holidays")
    args = parser.parse_args(argv)
    today = date.today()
    years = args.year or [today.year, today.year + 1]
    dest = Path(args.dir) if args.dir else tw_holidays.override_dir()
    failed = 0
    for y in years:
        try:
            print(update_year(y, dest))
        except Exception as exc:
            failed += 1
            print(f"{y}：失敗 {type(exc).__name__}: {exc}", file=sys.stderr)
    tw_holidays.clear_cache()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
