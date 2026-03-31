from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.services.download_names import append_minute_timestamp, build_generated_filename
from app.services.local_time import local_fromtimestamp
from app.services.merge_to_main import backup_main_file


class LocalTimeTests(unittest.TestCase):
    def test_generated_filenames_follow_local_time(self):
        local_dt = datetime(2026, 3, 31, 10, 30, 0)

        self.assertEqual(
            build_generated_filename("發料單", ".xlsx", now=local_dt),
            "發料單_20260331_1030.xlsx",
        )
        self.assertEqual(
            append_minute_timestamp("BOM.xlsx", now=local_dt),
            "BOM_20260331_1030.xlsx",
        )

    def test_local_fromtimestamp_uses_taipei_time(self):
        utc_timestamp = datetime(2026, 3, 31, 2, 30, 0, tzinfo=timezone.utc).timestamp()
        local_dt = local_fromtimestamp(utc_timestamp)
        self.assertEqual(local_dt.strftime("%Y-%m-%d %H:%M"), "2026-03-31 10:30")

    def test_backup_main_file_uses_local_timestamp(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            backup_dir = Path(temp_dir) / "backups"
            main_path.write_bytes(b"test")

            with patch("app.services.merge_to_main.local_now", return_value=datetime(2026, 3, 31, 10, 30, 15)):
                backup_path = backup_main_file(str(main_path), str(backup_dir))

        self.assertTrue(str(backup_path).endswith("main_backup_20260331_103015.xlsx"))


if __name__ == "__main__":
    unittest.main()
