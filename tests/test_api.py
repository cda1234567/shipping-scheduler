from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from main import app


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_schedule_rows_include_decisions_and_dispatched_consumption(self):
        pending_rows = [{"id": 1, "model": "MODEL-A"}]
        completed_rows = [{"id": 2, "model": "MODEL-B"}]

        def fake_get_orders(statuses=None):
            if statuses == ["pending", "merged"]:
                return pending_rows
            if statuses == ["dispatched", "completed"]:
                return completed_rows
            return []

        with patch("app.routers.schedule.db.get_orders", side_effect=fake_get_orders), \
             patch("app.routers.schedule.db.get_setting", side_effect=lambda key, default="": {
                 "schedule_loaded_at": "2026-03-12T08:00:00",
                 "schedule_filename": "schedule.xlsx",
             }.get(key, default)), \
             patch("app.routers.schedule.db.get_all_dispatched_consumption", return_value={"PART-1": 12}), \
             patch("app.routers.schedule.db.get_all_decisions", return_value={"PART-1": "CreateRequirement"}):
            response = self.client.get("/api/schedule/rows")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["rows"], pending_rows)
        self.assertEqual(data["completed_count"], 1)
        self.assertEqual(data["dispatched_consumption"], {"PART-1": 12})
        self.assertEqual(data["decisions"], {"PART-1": "CreateRequirement"})

    def test_main_file_data_backfills_missing_moq_from_live_main_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"placeholder")

            def fake_setting(key, default=""):
                mapping = {
                    "main_file_path": str(main_path),
                    "main_part_count": "1",
                    "main_loaded_at": "2026-03-12T08:00:00",
                    "main_filename": "main.xlsx",
                }
                return mapping.get(key, default)

            snapshot = {"AAA": {"stock_qty": 5, "moq": 8}}

            with patch("app.routers.main_file.db.get_setting", side_effect=fake_setting), \
                 patch("app.routers.main_file.db.get_snapshot", return_value=snapshot), \
                 patch("app.routers.main_file.read_moq", return_value={"AAA": 99, "BBB": 12}):
                response = self.client.get("/api/main-file/data")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["stock"], {"AAA": 5})
        self.assertEqual(data["moq"]["AAA"], 8)
        self.assertEqual(data["moq"]["BBB"], 12)
