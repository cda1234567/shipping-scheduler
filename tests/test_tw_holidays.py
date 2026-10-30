from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.services import po_pullin, shortage_purchase as sp, tw_holidays
from app.tools import update_tw_holidays as upd

TODAY = date(2026, 10, 30)  # 週五


def make_csv(year: int, overrides: dict | None = None, rows_missing: bool = False) -> str:
    """產生一整年的辦公日曆 CSV：週末 2、平日 0，overrides = {date: 0|2}。"""
    from datetime import timedelta

    lines = ["西元日期,星期,是否放假,備註"]
    d = date(year, 1, 1)
    while d.year == year:
        flag = 2 if d.weekday() >= 5 else 0
        if overrides and d in overrides:
            flag = overrides[d]
        lines.append(f"{d:%Y%m%d},x,{flag},")
        d += timedelta(days=1)
    if rows_missing:
        lines = lines[:50]
    return "﻿" + "\n".join(lines) + "\n"


class HolidayBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.override = Path(self.tmp.name) / "holidays"
        p = patch.object(tw_holidays, "override_dir", lambda: self.override)
        p.start()
        self.addCleanup(p.stop)
        tw_holidays.clear_cache()
        self.addCleanup(tw_holidays.clear_cache)

    def put_override(self, year: int, text: str):
        self.override.mkdir(parents=True, exist_ok=True)
        (self.override / f"tw_{year}.csv").write_text(text, encoding="utf-8")
        tw_holidays.clear_cache()


class LoaderTests(HolidayBase):
    def test_bundled_official_years_are_loaded(self):
        self.assertEqual(tw_holidays.year_source(2026), "official")
        self.assertEqual(tw_holidays.year_source(2027), "official")
        self.assertFalse(tw_holidays.is_working_day(date(2026, 2, 17)))  # 春節
        self.assertFalse(tw_holidays.is_working_day(date(2026, 10, 26)))  # 補假（週一）
        self.assertTrue(tw_holidays.is_working_day(date(2026, 10, 27)))
        self.assertFalse(tw_holidays.is_working_day(date(2026, 11, 28)))  # 週六

    def test_packaged_files_are_original_content(self):
        packaged = tw_holidays.PACKAGED_DIR
        for year in (2026, 2027):
            text = (packaged / f"tw_{year}.csv").read_text(encoding="utf-8-sig")
            self.assertTrue(text.startswith("西元日期,星期,是否放假,備註"))
        self.assertTrue((packaged / "README.md").read_text(encoding="utf-8").count("data.gov.tw/dataset/14718"))

    def test_make_up_workday_on_weekend_is_working(self):
        csv = make_csv(2026, {date(2026, 11, 7): 0})  # 週六補班
        self.put_override(2026, csv)
        self.assertEqual(tw_holidays.year_source(2026), "override")
        self.assertTrue(tw_holidays.is_working_day(date(2026, 11, 7)))
        self.assertFalse(tw_holidays.is_working_day(date(2026, 11, 8)))

    def test_weekday_holiday_in_override(self):
        self.put_override(2026, make_csv(2026, {date(2026, 11, 12): 2}))
        self.assertFalse(tw_holidays.is_working_day(date(2026, 11, 12)))

    def test_override_takes_precedence_over_packaged(self):
        self.put_override(2026, make_csv(2026, {date(2026, 2, 17): 0}))  # 官方檔春節放假，override 改成上班
        self.assertTrue(tw_holidays.is_working_day(date(2026, 2, 17)))

    def test_incomplete_override_is_ignored(self):
        self.put_override(2026, make_csv(2026, rows_missing=True))
        self.assertEqual(tw_holidays.year_source(2026), "official")

    def test_fallback_year_weekends_and_fixed_holidays(self):
        self.assertEqual(tw_holidays.year_source(2030), "fallback")
        self.assertFalse(tw_holidays.is_working_day(date(2030, 1, 1)))  # 元旦（週二）
        self.assertFalse(tw_holidays.is_working_day(date(2030, 10, 10)))  # 國慶（週四）
        self.assertFalse(tw_holidays.is_working_day(date(2030, 12, 25)))
        self.assertFalse(tw_holidays.is_working_day(date(2030, 3, 2)))  # 週六
        self.assertTrue(tw_holidays.is_working_day(date(2030, 3, 4)))  # 週一
        r = tw_holidays.shift_due_date(date(2030, 10, 10), date(2030, 9, 1))
        self.assertEqual(r["date"], date(2030, 10, 9))
        self.assertTrue(r["fallback_year"])

    def test_calendar_status(self):
        st = tw_holidays.calendar_status(TODAY)
        by_year = {i["year"]: i for i in st["years"]}
        self.assertEqual(by_year[2026]["source"], "official")
        self.assertEqual(by_year[2027]["source"], "official")
        self.assertFalse(st["any_fallback"])
        st = tw_holidays.calendar_status(date(2031, 6, 1))
        self.assertTrue(st["any_fallback"])
        self.assertTrue(all(i["fallback"] for i in st["years"] if i["year"] >= 2031))


class ShiftTests(HolidayBase):
    def test_working_day_unchanged(self):
        r = tw_holidays.shift_due_date(date(2026, 11, 27), TODAY)
        self.assertEqual((r["date"], r["shifted"], r["reason"], r["reason_code"]), (date(2026, 11, 27), False, "", ""))

    def test_weekend_moves_earlier_to_friday(self):
        r = tw_holidays.shift_due_date(date(2026, 11, 28), TODAY)  # 週六
        self.assertEqual((r["date"], r["original"], r["shifted"]), (date(2026, 11, 27), date(2026, 11, 28), True))
        self.assertEqual((r["reason"], r["reason_code"]), ("遇假日提前至 11/27", "holiday"))
        self.assertEqual(tw_holidays.shift_due_date(date(2026, 11, 29), TODAY)["date"], date(2026, 11, 27))

    def test_consecutive_holidays_go_back_past_all_of_them(self):
        r = tw_holidays.shift_due_date(date(2026, 2, 19), date(2026, 1, 5))  # 春節連假 2/14~2/22
        self.assertEqual(r["date"], date(2026, 2, 13))

    def test_make_up_workday_not_moved(self):
        self.put_override(2026, make_csv(2026, {date(2026, 11, 7): 0}))
        self.assertFalse(tw_holidays.shift_due_date(date(2026, 11, 7), TODAY)["shifted"])

    def test_shift_never_before_today(self):
        # 10/25（週日）→ 往前是 10/23，早於今天 10/30 → 今天（週五，上班日）
        r = tw_holidays.shift_due_date(date(2026, 10, 25), TODAY)
        self.assertEqual((r["date"], r["reason_code"]), (TODAY, "clamp"))
        self.assertEqual(r["reason"], "遇假日，改為 10/30")

    def test_today_is_holiday_uses_next_working_day(self):
        sat = date(2026, 10, 31)  # 今天是週六
        r = tw_holidays.shift_due_date(date(2026, 10, 20), sat)
        self.assertEqual(r["date"], date(2026, 11, 2))  # 週一
        r = tw_holidays.shift_due_date(sat, sat)
        self.assertEqual(r["date"], date(2026, 11, 2))
        self.assertEqual(r["reason_code"], "clamp")

    def test_past_working_day_clamps_to_today(self):
        r = tw_holidays.shift_due_date(date(2026, 10, 27), TODAY)
        self.assertEqual((r["date"], r["reason"]), (TODAY, "早於今天，改為 10/30"))

    def test_today_itself_working_not_shifted(self):
        self.assertFalse(tw_holidays.shift_due_date(TODAY, TODAY)["shifted"])

    def test_shift_iso_and_bad_input(self):
        r = tw_holidays.shift_iso("2026-11-28", TODAY)
        self.assertEqual((r["date"], r["original"], r["shifted"]), ("2026-11-27", "2026-11-28", True))
        bad = tw_holidays.shift_iso("abc", TODAY)
        self.assertFalse(bad["shifted"])
        self.assertEqual(bad["date"], "abc")


class UpdateToolTests(HolidayBase):
    DATASET = {"result": {"distribution": [
        {"resourceDescription": "115年中華民國政府行政機關辦公日曆表_Google行事曆專用", "resourceFormat": "CSV", "resourceDownloadUrl": "http://x/google"},
        {"resourceDescription": "115年中華民國政府行政機關辦公日曆表", "resourceFormat": "CSV", "resourceDownloadUrl": "http://x/115"},
        {"resourceDescription": "117年中華民國政府行政機關辦公日曆表(1171020更新)", "resourceFormat": "CSV", "resourceDownloadUrl": "http://x/117u"},
        {"resourceDescription": "117年中華民國政府行政機關辦公日曆表_Google行事曆專用", "resourceFormat": "CSV", "resourceDownloadUrl": "http://x/117g"},
    ]}}

    def test_pick_url_prefers_exact_and_never_google(self):
        self.assertEqual(upd.pick_download_url(self.DATASET, 2026), "http://x/115")
        self.assertEqual(upd.pick_download_url(self.DATASET, 2028), "http://x/117u")
        self.assertIsNone(upd.pick_download_url(self.DATASET, 2030))

    def test_update_year_writes_override_and_is_used(self):
        csv = make_csv(2028, {date(2028, 6, 6): 2})

        def fetch(url):
            return json.dumps({"result": {"distribution": [
                {"resourceDescription": "117年中華民國政府行政機關辦公日曆表", "resourceFormat": "CSV", "resourceDownloadUrl": "http://x/117"}]}}
            ).encode() if "api/v2" in url else csv.encode("utf-8")

        msg = upd.update_year(2028, self.override, fetch=fetch)
        self.assertIn("tw_2028.csv", msg)
        tw_holidays.clear_cache()
        self.assertEqual(tw_holidays.year_source(2028), "override")
        self.assertFalse(tw_holidays.is_working_day(date(2028, 6, 6)))

    def test_update_year_rejects_incomplete_download(self):
        def fetch(url):
            if "api/v2" in url:
                return json.dumps(self.DATASET).encode()
            return make_csv(2026, rows_missing=True).encode("utf-8")

        with self.assertRaises(RuntimeError):
            upd.update_year(2026, self.override, fetch=fetch)
        self.assertFalse((self.override / "tw_2026.csv").exists())


class DueDatePathTests(HolidayBase):
    """每一條預交日路徑都套用假日規則。"""

    def test_pullin_required_date_shifts(self):
        # 後段：出貨 2026-12-20 + ... 用 11/28(週六)−0 不行；用 ship = 12/19 → −21 = 11/28（週六）→ 11/27
        d, adjusted, raw = po_pullin.required_date("backend", date(2026, 12, 19), TODAY)
        self.assertEqual((d, adjusted, raw), (date(2026, 11, 27), False, date(2026, 11, 28)))
        self.assertEqual(po_pullin.required_date_reason("backend", date(2026, 12, 19), TODAY), "遇假日提前至 11/27")

    def test_pullin_required_date_never_before_today(self):
        d, adjusted, raw = po_pullin.required_date("backend", date(2026, 11, 5), TODAY)
        self.assertEqual((d, adjusted), (TODAY, True))

    def test_shortage_prepare_groups_clamp_uses_next_working_day(self):
        sat_today = date(2026, 10, 31)
        self.assertEqual(sp._clamp_date_text("2026-10-20", sat_today), "2026-11-02")
        self.assertEqual(sp._clamp_date_text("2026-11-28", sat_today), "2026-11-28")  # 使用者自己填的未來日期不動
        self.assertEqual(sp._clamp_date_text("abc", sat_today), "abc")

    def test_default_due_endpoint_and_shift_endpoint(self):
        from main import app

        client = TestClient(app)
        with patch("app.routers.holidays.local_now", lambda: datetime(2026, 10, 30, 9, 0)):
            r = client.get("/api/holidays/default-due?days=14").json()  # 11/13（週五）
            self.assertEqual((r["date"], r["shifted"]), ("2026-11-13", False))
            r = client.get("/api/holidays/default-due?days=29").json()  # 11/28（週六）→ 11/27
            self.assertEqual((r["date"], r["original"], r["reason"]), ("2026-11-27", "2026-11-28", "遇假日提前至 11/27"))
        shifted = client.get("/api/holidays/shift?date=2026-12-26").json()
        self.assertTrue(shifted["shifted"])
        self.assertEqual(client.get("/api/holidays/shift?date=nope").status_code, 400)
        status = client.get("/api/holidays/status").json()
        self.assertIn("years", status)


if __name__ == "__main__":
    unittest.main()
