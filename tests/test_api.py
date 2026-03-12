from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import openpyxl
from fastapi.testclient import TestClient

from app.models import BomComponent, BomFile
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

    def test_schedule_rows_use_snapshot_cutoff_for_dispatched_consumption(self):
        with patch("app.routers.schedule.db.get_orders", side_effect=[[], []]), \
             patch("app.routers.schedule.db.get_setting", side_effect=lambda key, default="": default), \
             patch("app.routers.schedule.db.get_snapshot_taken_at", return_value="2026-03-12T11:05:45.000000"), \
             patch("app.routers.schedule.db.get_all_dispatched_consumption", return_value={"PART-2": 15}) as mock_consumption, \
             patch("app.routers.schedule.db.get_all_decisions", return_value={}):
            response = self.client.get("/api/schedule/rows")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["dispatched_consumption"], {"PART-2": 15})
        mock_consumption.assert_called_once_with("2026-03-12T11:05:45.000000")

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
                 patch("app.routers.main_file.find_legacy_snapshot_stock_fixes", return_value={}), \
                 patch("app.routers.main_file.db.update_snapshot_stock", return_value=0), \
                 patch("app.routers.main_file.read_moq", return_value={"AAA": 99, "BBB": 12}):
                response = self.client.get("/api/main-file/data")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["stock"], {"AAA": 5})
        self.assertEqual(data["moq"]["AAA"], 8)
        self.assertEqual(data["moq"]["BBB"], 12)

    def test_main_file_data_repairs_legacy_snapshot_stock_bug(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"placeholder")

            def fake_setting(key, default=""):
                mapping = {
                    "main_file_path": str(main_path),
                    "main_part_count": "2",
                    "main_loaded_at": "2026-03-12T08:00:00",
                    "main_filename": "main.xlsx",
                }
                return mapping.get(key, default)

            snapshot = {
                "AAA": {"stock_qty": 8, "moq": 8},
                "BBB": {"stock_qty": 5, "moq": 12},
            }

            with patch("app.routers.main_file.db.get_setting", side_effect=fake_setting), \
                 patch("app.routers.main_file.db.get_snapshot", return_value=snapshot), \
                 patch("app.routers.main_file.find_legacy_snapshot_stock_fixes", return_value={"AAA": 0.0}), \
                 patch("app.routers.main_file.db.update_snapshot_stock", return_value=1), \
                 patch("app.routers.main_file.db.log_activity"), \
                 patch("app.routers.main_file.read_moq", return_value={"AAA": 99, "BBB": 12}):
                response = self.client.get("/api/main-file/data")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["stock"]["AAA"], 0.0)
        self.assertEqual(data["stock"]["BBB"], 5)

    def test_update_snapshot_moq_endpoint_normalizes_part_number(self):
        with patch("app.routers.main_file.db.upsert_snapshot_moq", return_value="IC-LD39100PUR-TAB") as mock_update, \
             patch("app.routers.main_file.db.log_activity") as mock_log:
            response = self.client.patch("/api/main-file/moq", json={
                "part_number": " ic-ld39100pur-tab ",
                "moq": 2500,
            })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "ok": True,
            "part_number": "IC-LD39100PUR-TAB",
            "moq": 2500,
        })
        mock_update.assert_called_once_with("IC-LD39100PUR-TAB", 2500)
        mock_log.assert_called_once()

    def test_set_snapshot_keeps_manual_moq_overrides_over_excel(self):
        with patch("app.routers.main_file.db.get_setting", return_value="C:/main.xlsx"), \
             patch("app.routers.main_file.Path.exists", return_value=True), \
             patch("app.routers.main_file.db.get_manual_snapshot_moq", return_value={"PART-1": 3000}), \
             patch("app.routers.main_file.read_stock", return_value={"PART-1": 10, "PART-2": 20}), \
             patch("app.routers.main_file.read_moq", return_value={"PART-1": 500, "PART-2": 1200}), \
             patch("app.routers.main_file.db.save_snapshot") as mock_save_snapshot, \
             patch("app.routers.main_file.db.log_activity"):
            response = self.client.post("/api/main-file/snapshot")

        self.assertEqual(response.status_code, 200)
        mock_save_snapshot.assert_called_once_with(
            {"PART-1": 10, "PART-2": 20},
            {"PART-1": 3000, "PART-2": 1200},
            manual_moq_parts={"PART-1"},
        )

    def test_bom_editor_returns_source_metadata(self):
        bom_record = {
            "id": "bom-1",
            "filename": "formal.xlsx",
            "filepath": "C:/formal.xlsx",
            "source_filename": "legacy.xls",
            "source_format": ".xls",
            "is_converted": 1,
            "group_model": "MODEL-A",
            "uploaded_at": "2026-03-12T08:00:00",
        }
        parsed = BomFile(
            id="bom-1",
            filename="formal.xlsx",
            path="C:/formal.xlsx",
            po_number=123,
            model="MODEL-A",
            pcb="PCB-A",
            group_model="MODEL-A",
            order_qty=10,
            uploaded_at="2026-03-12T08:00:00",
            source_filename="legacy.xls",
            source_format=".xls",
            is_converted=True,
            components=[
                BomComponent(part_number="PART-1", source_row=5),
            ],
        )

        with patch("app.routers.bom._get_required_bom", return_value=bom_record), \
             patch("app.routers.bom._ensure_editable_bom_record", return_value=bom_record), \
             patch("app.routers.bom.parse_bom_for_storage", return_value=parsed):
            response = self.client.get("/api/bom/bom-1/editor")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["filename"], "formal.xlsx")
        self.assertEqual(data["source_filename"], "legacy.xls")
        self.assertEqual(data["source_format"], ".xls")
        self.assertTrue(data["is_converted"])
        self.assertEqual(data["component_count"], 1)

    def test_dispatch_download_writes_prev_batch_and_supplements_to_separate_columns(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_path = Path(temp_dir) / "dispatch.xlsx"

            wb = openpyxl.Workbook()
            ws = wb.active
            ws.cell(row=1, column=8).value = "製單號碼M/O:4500059234"
            ws.cell(row=1, column=10).value = "訂單數量:"
            ws.cell(row=2, column=3).value = "MODEL-A"
            ws.cell(row=2, column=4).value = "PCB-A"
            ws.cell(row=3, column=7).value = "上批餘料"
            ws.cell(row=3, column=8).value = "增添料數"
            ws.cell(row=5, column=3).value = "PART-1"
            ws.cell(row=5, column=6).value = 10
            ws.cell(row=5, column=8).value = 3
            ws.cell(row=6, column=3).value = "PART-2"
            ws.cell(row=6, column=6).value = 20
            ws.cell(row=6, column=8).value = 8
            wb.save(bom_path)
            wb.close()

            bom_record = {
                "id": "bom-1",
                "filename": "dispatch.xlsx",
                "filepath": str(bom_path),
                "source_filename": "dispatch.xlsx",
                "source_format": ".xlsx",
                "is_converted": 0,
                "group_model": "MODEL-A",
                "uploaded_at": "2026-03-12T08:00:00",
            }

            with patch("app.routers.bom.db.get_bom_files", return_value=[bom_record]):
                response = self.client.post("/api/bom/dispatch-download", json={
                    "bom_ids": ["bom-1"],
                    "supplements": {"PART-1": 7},
                })

        self.assertEqual(response.status_code, 200)
        downloaded = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = downloaded.active
        self.assertEqual(ws.cell(row=5, column=7).value, 3)
        self.assertEqual(ws.cell(row=5, column=8).value, 7)
        self.assertEqual(ws.cell(row=6, column=7).value, 8)
        self.assertIsNone(ws.cell(row=6, column=8).value)
        downloaded.close()
