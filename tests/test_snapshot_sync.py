from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.snapshot_sync import refresh_snapshot_from_main


class SnapshotSyncTests(unittest.TestCase):
    def test_refresh_updates_main_part_count_from_actual_stock(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"placeholder")

            with patch("app.snapshot_sync.read_stock", return_value={"A": 1, "B": 2}), \
                 patch("app.snapshot_sync.read_moq", return_value={"A": 10, "B": 20}), \
                 patch("app.snapshot_sync.db.get_manual_snapshot_moq", return_value={}), \
                 patch("app.snapshot_sync.db.save_snapshot") as mock_save, \
                 patch("app.snapshot_sync.db.set_setting") as mock_set, \
                 patch("app.routers.main_file.invalidate_main_data_cache"):
                count = refresh_snapshot_from_main(str(main_path))

        self.assertEqual(count, 2)
        mock_save.assert_called_once()
        mock_set.assert_called_once_with("main_part_count", "2")


if __name__ == "__main__":
    unittest.main()
