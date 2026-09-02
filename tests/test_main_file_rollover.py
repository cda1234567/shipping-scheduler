from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from openpyxl import Workbook

from app import database as db
from app.services import main_file_rollover as rollover_service
from app.services.dispatch_pipeline import get_dispatch_rollback_unavailable_reason
from app.services.inventory_count_lock import is_inventory_mutation_request
from app.services.main_reader import read_stock


def _build_main(path: Path, part: str, stock: float) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["料號", "廠商", "MOQ", "期初", "盤點", "", "M/O", "結存"])
    sheet.append([part, "", 1, None, None, None, 0, stock])
    workbook.save(path)
    workbook.close()


class MainFileRolloverTests(unittest.TestCase):
    def test_period_change_blocks_old_dispatch_rollback(self):
        with patch.object(db, "get_current_main_period_id", return_value=2):
            reason = get_dispatch_rollback_unavailable_reason({"main_period_id": 1})
        self.assertIn("舊年度主檔", reason)

    def test_inventory_count_lock_includes_year_rollover(self):
        self.assertTrue(is_inventory_mutation_request("POST", "/api/main-file/rollover"))

    def test_rollover_archives_old_files_and_sets_new_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            main_dir = root / "main_file"
            backup_dir = root / "backups"
            main_dir.mkdir()
            old_path = main_dir / "main.xlsx"
            incoming_path = root / "2027.xlsx"
            _build_main(old_path, "OLD-PART", 12)
            _build_main(incoming_path, "NEW-PART", 34)

            with (
                patch.object(db, "DB_PATH", root / "system.db"),
                patch.object(rollover_service, "MAIN_FILE_DIR", main_dir),
                patch.object(rollover_service, "BACKUP_DIR", backup_dir),
            ):
                db.init_db()
                db.set_setting("main_file_path", str(old_path))
                db.set_setting("main_filename", "2026.xlsx")
                db.set_setting("main_loaded_at", "2026-12-31T08:00:00")

                result = rollover_service.rollover_main_file(
                    content=incoming_path.read_bytes(),
                    filename="2027.xlsx",
                    period_label="2027",
                )

                self.assertEqual(read_stock(str(old_path)), {"NEW-PART": 34.0})
                self.assertEqual(read_stock(result["archived_main_path"]), {"OLD-PART": 12.0})
                self.assertTrue(Path(result["database_backup_path"]).exists())
                self.assertEqual(db.get_setting("main_file_period_label"), "2027")
                self.assertEqual(db.get_snapshot()["NEW-PART"]["stock_qty"], 34.0)
                self.assertNotIn("OLD-PART", db.get_snapshot())

    def test_invalid_new_file_does_not_replace_current_main(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            main_dir = root / "main_file"
            main_dir.mkdir()
            old_path = main_dir / "main.xlsx"
            _build_main(old_path, "OLD-PART", 12)

            with (
                patch.object(db, "DB_PATH", root / "system.db"),
                patch.object(rollover_service, "MAIN_FILE_DIR", main_dir),
                patch.object(rollover_service, "BACKUP_DIR", root / "backups"),
            ):
                db.init_db()
                db.set_setting("main_file_path", str(old_path))
                with self.assertRaises(ValueError):
                    rollover_service.rollover_main_file(
                        content=b"not an excel file",
                        filename="2027.xlsx",
                        period_label="2027",
                    )
                self.assertEqual(read_stock(str(old_path)), {"OLD-PART": 12.0})
                self.assertEqual(db.list_main_file_periods(), [])


if __name__ == "__main__":
    unittest.main()
