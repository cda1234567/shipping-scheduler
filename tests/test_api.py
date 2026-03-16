from __future__ import annotations

import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import call, patch

import openpyxl
from fastapi import HTTPException
from fastapi.responses import Response
from fastapi.testclient import TestClient
from openpyxl.styles import Font, PatternFill

import app.routers.schedule as schedule_router
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
             patch("app.routers.schedule.db.get_all_decisions", return_value={"PART-1": "CreateRequirement"}), \
             patch("app.routers.schedule.get_schedule_draft_map", return_value={1: {"id": 9, "files": [], "shortages": []}}):
            response = self.client.get("/api/schedule/rows")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["rows"], pending_rows)
        self.assertEqual(data["completed_count"], 1)
        self.assertEqual(data["dispatched_consumption"], {"PART-1": 12})
        self.assertEqual(data["decisions"], {"PART-1": "CreateRequirement"})
        self.assertEqual(data["merge_drafts"], {"1": {"id": 9, "files": [], "shortages": []}})

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

    def test_batch_merge_creates_merge_drafts(self):
        with patch("app.routers.schedule.db.batch_merge_orders") as mock_batch_merge, \
             patch("app.routers.schedule.rebuild_merge_drafts", return_value=[{"id": 7}, {"id": 8}]) as mock_rebuild, \
             patch("app.routers.schedule.db.log_activity"), \
             patch("app.routers.schedule.db.create_alert"):
            response = self.client.post("/api/schedule/batch-merge", json={"order_ids": [1, 2]})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True, "count": 2, "draft_count": 2})
        mock_batch_merge.assert_called_once_with([1, 2])
        mock_rebuild.assert_called_once_with([1, 2])

    def test_update_selected_schedule_drafts_allocates_supplements_per_order(self):
        with patch("app.routers.schedule.db.get_active_merge_draft_ids_by_order_ids", return_value={1: 11, 2: 12}), \
             patch("app.routers.schedule.build_order_supplement_allocations", return_value={
                 1: {"PART-1": 3000},
                 2: {},
             }) as mock_allocations, \
             patch("app.routers.schedule.rebuild_merge_drafts", return_value=[{"id": 11}, {"id": 12}]) as mock_rebuild, \
             patch("app.routers.schedule.get_schedule_draft_map", return_value={
                 1: {"id": 11, "order_id": 1},
                 2: {"id": 12, "order_id": 2},
             }), \
             patch("app.routers.schedule.db.log_activity"):
            response = self.client.put("/api/schedule/drafts", json={
                "order_ids": [1, 2],
                "decisions": {"part-1": "Shortage"},
                "supplements": {"part-1": 3000},
            })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 2)
        self.assertEqual(response.json()["draft_count"], 2)
        mock_allocations.assert_called_once_with([1, 2], {"part-1": 3000})
        mock_rebuild.assert_called_once_with(
            [1, 2],
            {
                1: {
                    "decisions": {"part-1": "Shortage"},
                    "supplements": {"PART-1": 3000},
                },
                2: {
                    "decisions": {"part-1": "Shortage"},
                    "supplements": {},
                },
            },
        )

    def test_download_selected_schedule_drafts_proxies_to_bundle_service(self):
        with patch(
            "app.routers.schedule.download_selected_merge_drafts",
            return_value=Response(content=b"ok", media_type="application/zip"),
        ) as mock_download:
            response = self.client.post("/api/schedule/drafts/download", json={"order_ids": [1, 2]})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"ok")
        mock_download.assert_called_once_with([1, 2])

    def test_dispatch_order_saves_dispatch_session(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")

            order = {
                "id": 1,
                "po_number": "4500059234",
                "model": "MODEL-A",
                "code": "1-3",
                "status": "merged",
            }
            bom_file = {"id": "bom-1", "model": "MODEL-A"}
            components = [{"part_number": "PART-1", "needed_qty": 5, "prev_qty_cs": 0, "is_dash": 0}]

            with patch("app.routers.schedule.db.get_order", return_value=order), \
                 patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule._get_effective_moq", return_value={}), \
                 patch("app.routers.schedule.db.get_bom_files_by_models", return_value=[bom_file]), \
                 patch("app.routers.schedule.db.get_bom_components", return_value=components), \
                 patch("app.routers.schedule.preview_order_batches", return_value={
                     "merged_parts": 1,
                     "shortages": [],
                 }), \
                 patch("app.routers.schedule.merge_row_to_main", return_value={"merged_parts": 1, "backup_path": "C:/backup.xlsx"}), \
                 patch("app.routers.schedule.db.save_dispatch_records") as mock_records, \
                 patch("app.routers.schedule.db.save_dispatch_session") as mock_session, \
                 patch("app.routers.schedule.db.update_order"), \
                 patch("app.routers.schedule.db.log_activity"):
                response = self.client.post("/api/schedule/orders/1/dispatch", json={"decisions": {}})

        self.assertEqual(response.status_code, 200)
        mock_records.assert_called_once()
        mock_session.assert_called_once_with(
            order_id=1,
            previous_status="merged",
            backup_path="C:/backup.xlsx",
            main_file_path=str(main_path),
        )

    def test_batch_dispatch_processes_checked_orders_in_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")

            context_a = ({"id": 1, "po_number": "4500059234", "model": "MODEL-A", "status": "merged"}, [{"components": []}], [])
            context_b = ({"id": 2, "po_number": "4500059235", "model": "MODEL-B", "status": "pending"}, [{"components": []}], [])

            with patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule._get_effective_moq", return_value={}), \
                 patch("app.routers.schedule._prepare_dispatch_context", side_effect=[context_a, context_b]) as mock_prepare, \
                 patch("app.routers.schedule.preview_order_batches", return_value={
                     "merged_parts": 7,
                     "shortages": [],
                 }), \
                 patch("app.routers.schedule._execute_dispatch", side_effect=[
                     {"order_id": 1, "merged_parts": 3, "backup_path": "C:/b1.xlsx", "session": {"id": 11}},
                     {"order_id": 2, "merged_parts": 4, "backup_path": "C:/b2.xlsx", "session": {"id": 12}},
                 ]) as mock_execute, \
                 patch("app.routers.schedule.db.log_activity"):
                response = self.client.post("/api/schedule/batch-dispatch", json={
                    "order_ids": [1, 2],
                    "decisions": {"part-1": "CreateRequirement"},
                })

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["count"], 2)
        self.assertEqual(data["merged_parts"], 7)
        self.assertEqual(data["order_ids"], [1, 2])
        mock_prepare.assert_has_calls([call(1, str(main_path)), call(2, str(main_path))])
        self.assertEqual(mock_execute.call_args_list[0].args[4], {"PART-1": "CreateRequirement"})
        self.assertEqual(mock_execute.call_args_list[1].args[4], {"PART-1": "CreateRequirement"})

    def test_load_active_merge_draft_context_accepts_repaired_main_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")
            signature = str(main_path.stat().st_mtime_ns)

            draft = {
                "id": 9,
                "order_id": 1,
                "status": "active",
                "main_file_path": "Z:/legacy/main.xlsx",
                "main_file_mtime_ns": signature,
                "decisions": {"PART-1": "Shortage"},
                "supplements": {"PART-1": 3000},
            }
            order = {"id": 1, "po_number": "4500059234", "model": "MODEL-A"}

            def fake_resolve(path_value: str, setting_key: str = ""):
                if str(path_value or "").strip():
                    return str(main_path)
                return ""

            with patch("app.routers.schedule.db.get_merge_draft", return_value=draft), \
                 patch("app.routers.schedule.db.resolve_managed_path", side_effect=fake_resolve), \
                 patch("app.routers.schedule._prepare_dispatch_context", return_value=(order, [{"components": []}], [])):
                context = schedule_router._load_active_merge_draft_context(9, str(main_path))

        self.assertEqual(context["draft"]["id"], 9)
        self.assertEqual(context["order"]["id"], 1)
        self.assertEqual(context["decisions"], {"PART-1": "Shortage"})
        self.assertEqual(context["supplements"], {"PART-1": 3000.0})

    def test_main_write_preview_uses_allocated_supplements(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")

            context_a = ({"id": 1, "po_number": "4500059234", "model": "MODEL-A", "status": "merged"}, [{"components": []}], [])
            context_b = ({"id": 2, "po_number": "4500059235", "model": "MODEL-B", "status": "pending"}, [{"components": []}], [])

            with patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule._get_effective_moq", return_value={}), \
                 patch("app.routers.schedule._prepare_dispatch_context", side_effect=[context_a, context_b]), \
                 patch("app.routers.schedule.build_order_supplement_allocations", return_value={
                     1: {"PART-1": 3000},
                     2: {},
                 }) as mock_allocations, \
                 patch("app.routers.schedule._get_effective_moq", return_value={"PART-1": 500}), \
                 patch("app.routers.schedule.preview_order_batches", return_value={
                     "merged_parts": 4,
                     "shortages": [{"part_number": "PART-1", "moq": 500, "shortage_amount": 12}],
                 }) as mock_preview:
                response = self.client.post("/api/schedule/main-write-preview", json={
                    "order_ids": [1, 2],
                    "decisions": {"part-1": "CreateRequirement"},
                    "supplements": {"part-1": 3000},
                })

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["count"], 2)
        self.assertEqual(data["merged_parts"], 4)
        self.assertEqual(data["shortages"], [{"part_number": "PART-1", "moq": 500, "shortage_amount": 12}])
        mock_allocations.assert_called_once_with([1, 2], {"PART-1": 3000.0})
        self.assertEqual(mock_preview.call_args.kwargs["moq_map"], {"PART-1": 500})
        self.assertEqual(
            mock_preview.call_args.args[1],
            [
                {"order_id": 1, "model": "MODEL-A", "groups": [{"components": []}], "supplements": {"PART-1": 3000}},
                {"order_id": 2, "model": "MODEL-B", "groups": [{"components": []}], "supplements": {}},
            ],
        )

    def test_main_write_preview_uses_active_draft_supplements_when_available(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")
            context = {
                "draft": {
                    "id": 11,
                    "order_id": 1,
                    "shortages": [{
                        "part_number": "EC-10264A",
                        "shortage_amount": 18,
                        "supplement_qty": 3000,
                        "decision": "CreateRequirement",
                    }],
                },
                "order": {"id": 1, "model": "T356789IU+LCD", "code": "1-3"},
                "groups": [{"components": []}],
                "all_components": [{"part_number": "EC-10264A"}],
                "decisions": {"EC-10264A": "CreateRequirement"},
                "supplements": {"EC-10264A": 3000},
            }

            with patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule.db.get_active_merge_draft_ids_by_order_ids", return_value={1: 11}), \
                 patch("app.routers.schedule.rebuild_merge_drafts", return_value=[{"id": 11}]) as mock_rebuild, \
                 patch("app.routers.schedule._load_active_merge_draft_context", return_value=context):
                response = self.client.post("/api/schedule/main-write-preview", json={
                    "order_ids": [1],
                    "decisions": {},
                    "supplements": {},
                })

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["shortages"][0]["part_number"], "EC-10264A")
        self.assertEqual(data["shortages"][0]["supplement_qty"], 3000)
        self.assertEqual(data["shortages"][0]["model"], "T356789IU+LCD")
        self.assertEqual(data["shortages"][0]["batch_code"], "1-3")
        mock_rebuild.assert_called_once_with([1])

    def test_batch_dispatch_passes_allocated_supplements_to_each_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")

            context_a = ({"id": 1, "po_number": "4500059234", "model": "MODEL-A", "status": "merged"}, [{"components": []}], [])
            context_b = ({"id": 2, "po_number": "4500059235", "model": "MODEL-B", "status": "pending"}, [{"components": []}], [])

            with patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule._get_effective_moq", return_value={}), \
                 patch("app.routers.schedule._prepare_dispatch_context", side_effect=[context_a, context_b]), \
                 patch("app.routers.schedule.build_order_supplement_allocations", return_value={
                     1: {"PART-1": 3000},
                     2: {},
                 }), \
                 patch("app.routers.schedule.preview_order_batches", return_value={
                     "merged_parts": 5,
                     "shortages": [],
                 }), \
                 patch("app.routers.schedule._execute_dispatch", side_effect=[
                     {"order_id": 1, "merged_parts": 3, "backup_path": "C:/b1.xlsx", "session": {"id": 11}},
                     {"order_id": 2, "merged_parts": 2, "backup_path": "C:/b2.xlsx", "session": {"id": 12}},
                 ]) as mock_execute, \
                 patch("app.routers.schedule.db.replace_order_supplements") as mock_replace, \
                 patch("app.routers.schedule.db.log_activity"):
                response = self.client.post("/api/schedule/batch-dispatch", json={
                    "order_ids": [1, 2],
                    "decisions": {"part-1": "CreateRequirement"},
                    "supplements": {"part-1": 3000},
                })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_execute.call_args_list[0].args[5], {"PART-1": 3000})
        self.assertEqual(mock_execute.call_args_list[1].args[5], {})
        mock_replace.assert_called_once_with([1, 2], {1: {"PART-1": 3000}, 2: {}})

    def test_batch_dispatch_blocks_main_write_when_preview_has_non_ec_shortage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")

            context_a = ({"id": 1, "po_number": "4500059234", "model": "MODEL-A", "status": "merged"}, [{"components": []}], [])

            with patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule.db.get_active_merge_draft_ids_by_order_ids", return_value={}), \
                 patch("app.routers.schedule._get_effective_moq", return_value={}), \
                 patch("app.routers.schedule._prepare_dispatch_context", return_value=context_a), \
                 patch("app.routers.schedule.build_order_supplement_allocations", return_value={1: {}}), \
                 patch("app.routers.schedule.preview_order_batches", return_value={
                     "merged_parts": 0,
                     "shortages": [{"part_number": "PART-1", "shortage_amount": 5, "resulting_stock": -5}],
                 }), \
                 patch("app.routers.schedule._execute_dispatch") as mock_execute:
                response = self.client.post("/api/schedule/batch-dispatch", json={
                    "order_ids": [1],
                    "decisions": {},
                    "supplements": {},
                })

        self.assertEqual(response.status_code, 400)
        self.assertIn("PART-1", response.json()["detail"])
        mock_execute.assert_not_called()

    def test_batch_dispatch_rolls_back_processed_orders_when_later_order_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")

            context_a = ({"id": 1, "po_number": "4500059234", "model": "MODEL-A", "status": "merged"}, [{"components": []}], [])
            context_b = ({"id": 2, "po_number": "4500059235", "model": "MODEL-B", "status": "pending"}, [{"components": []}], [])

            with patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule._get_effective_moq", return_value={}), \
                 patch("app.routers.schedule._prepare_dispatch_context", side_effect=[context_a, context_b]), \
                 patch("app.routers.schedule.preview_order_batches", return_value={
                     "merged_parts": 3,
                     "shortages": [],
                 }), \
                 patch("app.routers.schedule._execute_dispatch", side_effect=[
                     {"order_id": 1, "merged_parts": 3, "backup_path": "C:/b1.xlsx", "session": {"id": 11, "order_id": 1}},
                     HTTPException(status_code=400, detail="第二筆發料失敗"),
                 ]), \
                 patch("app.routers.schedule._rollback_dispatch_sessions", return_value={"count": 1}) as mock_rollback, \
                 patch("app.routers.schedule.db.log_activity"):
                response = self.client.post("/api/schedule/batch-dispatch", json={
                    "order_ids": [1, 2],
                    "decisions": {},
                })

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "第二筆發料失敗")
        mock_rollback.assert_called_once_with([{"id": 11, "order_id": 1}])

    def test_update_schedule_draft_rebuilds_from_saved_payload(self):
        with patch("app.routers.schedule.db.get_merge_draft", return_value={"id": 5, "order_id": 1, "status": "active"}), \
             patch("app.routers.schedule.rebuild_merge_drafts", return_value=[{"id": 8}]) as mock_rebuild, \
             patch("app.routers.schedule.db.get_active_merge_draft_for_order", return_value={"id": 8, "order_id": 1, "status": "active"}), \
             patch("app.routers.schedule.get_draft_detail", return_value={"draft": {"id": 8, "supplements": {"PART-1": 3000}}}), \
             patch("app.routers.schedule.db.log_activity"):
            response = self.client.put("/api/schedule/drafts/5", json={
                "decisions": {"part-1": "Shortage"},
                "supplements": {"part-1": 3000},
            })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["draft"]["id"], 8)
        self.assertEqual(response.json()["refreshed_count"], 1)
        mock_rebuild.assert_called_once_with(
            [1],
            {
                1: {
                    "decisions": {"part-1": "Shortage"},
                    "supplements": {"part-1": 3000},
                }
            },
        )

    def test_commit_schedule_draft_uses_latest_saved_draft_context(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")
            context = {
                "draft": {"id": 5, "order_id": 1},
                "order": {"id": 1, "po_number": "4500059234", "model": "MODEL-A"},
                "groups": [{"components": []}],
                "all_components": [{"part_number": "PART-1", "needed_qty": 5}],
                "decisions": {"PART-1": "Shortage"},
                "supplements": {"PART-1": 3000},
            }

            with patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule._get_effective_moq", return_value={}), \
                 patch("app.routers.schedule._load_active_merge_draft_context", return_value=context) as mock_context, \
                 patch("app.routers.schedule.preview_order_batches", return_value={
                     "merged_parts": 4,
                     "shortages": [],
                 }), \
                 patch("app.routers.schedule._execute_dispatch", return_value={"order_id": 1, "merged_parts": 4}) as mock_execute, \
                 patch("app.routers.schedule.db.replace_order_supplements") as mock_replace, \
                 patch("app.routers.schedule.db.mark_merge_draft_committed") as mock_mark, \
                 patch("app.routers.schedule.db.get_active_merge_drafts", return_value=[]), \
                 patch("app.routers.schedule.db.log_activity"):
                response = self.client.post("/api/schedule/drafts/5/commit")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["draft_id"], 5)
        self.assertEqual(response.json()["order_id"], 1)
        self.assertEqual(response.json()["merged_parts"], 4)
        mock_context.assert_called_once_with(5, str(main_path))
        self.assertEqual(mock_execute.call_args.args[4], {"PART-1": "Shortage"})
        self.assertEqual(mock_execute.call_args.args[5], {"PART-1": 3000})
        mock_replace.assert_called_once_with([1], {1: {"PART-1": 3000}})
        mock_mark.assert_called_once_with(5)

    def test_commit_schedule_draft_blocks_non_ec_shortage_but_allows_ec_safety_stock(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")

            blocking_context = {
                "draft": {"id": 5, "order_id": 1, "shortages": [{"part_number": "PART-1", "shortage_amount": 8}]},
                "order": {"id": 1, "po_number": "4500059234", "model": "MODEL-A"},
                "groups": [{"components": []}],
                "all_components": [{"part_number": "PART-1", "needed_qty": 5}],
                "decisions": {"PART-1": "CreateRequirement"},
                "supplements": {},
            }
            allowed_context = {
                "draft": {"id": 6, "order_id": 2, "shortages": [{"part_number": "EC-001", "shortage_amount": 20}]},
                "order": {"id": 2, "po_number": "4500059235", "model": "MODEL-EC"},
                "groups": [{"components": []}],
                "all_components": [{"part_number": "EC-001", "needed_qty": 5}],
                "decisions": {"EC-001": "CreateRequirement"},
                "supplements": {},
            }

            with patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule._get_effective_moq", return_value={}), \
                 patch("app.routers.schedule._load_active_merge_draft_context", side_effect=[blocking_context, allowed_context]), \
                 patch("app.routers.schedule.preview_order_batches", side_effect=[
                     {"merged_parts": 0, "shortages": [{"part_number": "PART-1", "shortage_amount": 8, "resulting_stock": -8}]},
                     {"merged_parts": 0, "shortages": [{"part_number": "EC-001", "shortage_amount": 20, "resulting_stock": 80}]},
                 ]), \
                 patch("app.routers.schedule._execute_dispatch", return_value={"order_id": 2, "merged_parts": 1}) as mock_execute, \
                 patch("app.routers.schedule.db.replace_order_supplements"), \
                 patch("app.routers.schedule.db.mark_merge_draft_committed"), \
                 patch("app.routers.schedule.db.get_active_merge_drafts", return_value=[]), \
                 patch("app.routers.schedule.db.log_activity"):
                blocked = self.client.post("/api/schedule/drafts/5/commit")
                allowed = self.client.post("/api/schedule/drafts/6/commit")

        self.assertEqual(blocked.status_code, 400)
        self.assertIn("PART-1", blocked.json()["detail"])
        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(allowed.json()["order_id"], 2)
        mock_execute.assert_called_once()

    def test_commit_schedule_draft_blocks_ec_negative_stock(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"main")

            context = {
                "draft": {"id": 7, "order_id": 3, "shortages": [{"part_number": "EC-001", "shortage_amount": 101}]},
                "order": {"id": 3, "po_number": "4500059236", "model": "MODEL-EC"},
                "groups": [{"components": []}],
                "all_components": [{"part_number": "EC-001", "needed_qty": 5}],
                "decisions": {"EC-001": "CreateRequirement"},
                "supplements": {},
            }

            with patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule._get_effective_moq", return_value={}), \
                 patch("app.routers.schedule._load_active_merge_draft_context", return_value=context), \
                 patch("app.routers.schedule.preview_order_batches", return_value={
                     "merged_parts": 0,
                     "shortages": [{"part_number": "EC-001", "shortage_amount": 101, "resulting_stock": -1}],
                 }), \
                 patch("app.routers.schedule._execute_dispatch") as mock_execute:
                response = self.client.post("/api/schedule/drafts/7/commit")

        self.assertEqual(response.status_code, 400)
        self.assertIn("EC-001", response.json()["detail"])
        mock_execute.assert_not_called()

    def test_rollback_preview_returns_tail_orders(self):
        orders = {
            5: {"id": 5, "po_number": "4500059234", "model": "MODEL-E", "status": "dispatched"},
            6: {"id": 6, "po_number": "4500059235", "model": "MODEL-F", "status": "completed"},
        }
        session = {"id": 9, "order_id": 5, "backup_path": "C:/backup.xlsx", "main_file_path": "C:/main.xlsx"}
        tail = [
            {"id": 9, "order_id": 5, "previous_status": "merged"},
            {"id": 10, "order_id": 6, "previous_status": "pending"},
        ]

        with patch("app.routers.schedule.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.schedule.db.get_active_dispatch_session", return_value=session), \
             patch("app.routers.schedule.db.get_dispatch_session_tail", return_value=tail):
            response = self.client.get("/api/schedule/orders/5/rollback-preview")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["count"], 2)
        self.assertEqual(
            data["orders"],
            [
                {"id": 5, "po_number": "4500059234", "model": "MODEL-E", "status": "dispatched", "restore_status": "merged"},
                {"id": 6, "po_number": "4500059235", "model": "MODEL-F", "status": "completed", "restore_status": "pending"},
            ],
        )

    def test_rollback_restores_backup_and_reverts_tail_orders(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            backup_path = Path(temp_dir) / "backup.xlsx"
            main_path.write_text("after-dispatch", encoding="utf-8")
            backup_path.write_text("before-dispatch", encoding="utf-8")

            orders = {
                5: {"id": 5, "po_number": "4500059234", "model": "MODEL-E", "status": "dispatched"},
                6: {"id": 6, "po_number": "4500059235", "model": "MODEL-F", "status": "completed"},
            }
            session = {"id": 9, "order_id": 5, "backup_path": str(backup_path), "main_file_path": str(main_path)}
            tail = [
                {"id": 9, "order_id": 5, "previous_status": "merged", "backup_path": str(backup_path), "main_file_path": str(main_path)},
                {"id": 10, "order_id": 6, "previous_status": "pending", "backup_path": str(backup_path), "main_file_path": str(main_path)},
            ]

            with patch("app.routers.schedule.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
                 patch("app.routers.schedule.db.get_active_dispatch_session", return_value=session), \
                 patch("app.routers.schedule.db.get_dispatch_session_tail", return_value=tail), \
                 patch("app.routers.schedule.db.get_setting", return_value=str(main_path)), \
                 patch("app.routers.schedule.db.delete_dispatch_records_for_orders") as mock_delete_records, \
                 patch("app.routers.schedule.db.mark_dispatch_sessions_rolled_back") as mock_mark_rolled_back, \
                 patch("app.routers.schedule.restore_recent_committed_merge_drafts", return_value=[5, 6]) as mock_restore_drafts, \
                 patch("app.routers.schedule.db.update_order") as mock_update_order, \
                 patch("app.routers.schedule.db.log_activity"):
                response = self.client.post("/api/schedule/orders/5/rollback")
                restored_text = main_path.read_text(encoding="utf-8")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["count"], 2)
        self.assertEqual(restored_text, "before-dispatch")
        mock_delete_records.assert_called_once_with([5, 6])
        mock_mark_rolled_back.assert_called_once_with([9, 10])
        mock_restore_drafts.assert_called_once_with([5, 6])
        self.assertEqual(data["restored_draft_count"], 2)
        self.assertEqual(data["restored_draft_order_ids"], [5, 6])
        mock_update_order.assert_has_calls([
            call(5, status="merged", folder=""),
            call(6, status="pending", folder=""),
        ])

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

    def test_main_file_preview_returns_live_sheet_structure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"

            workbook = openpyxl.Workbook()
            sheet = workbook.active
            sheet.title = "庫存主檔"
            sheet.column_dimensions["A"].width = 18
            sheet.column_dimensions["B"].width = 12
            sheet.merge_cells("A1:C1")
            sheet["A1"] = "主檔即時預覽"
            sheet["A1"].font = Font(name="Calibri", bold=True, size=14)
            sheet["A1"].fill = PatternFill(fill_type="solid", fgColor="D9EAF7")
            sheet["A2"] = "料號"
            sheet["B2"] = "MOQ"
            sheet["C2"] = "庫存"
            sheet["A3"] = "EC-20023A"
            sheet["B3"] = 10000
            sheet["C3"] = 5625
            workbook.create_sheet("第二頁")
            workbook.save(main_path)
            workbook.close()

            def fake_setting(key, default=""):
                mapping = {
                    "main_file_path": str(main_path),
                    "main_filename": "主檔260306test.xlsx",
                    "main_loaded_at": "2026-03-12T22:45:00",
                }
                return mapping.get(key, default)

            with patch("app.routers.main_file.db.get_setting", side_effect=fake_setting):
                response = self.client.get("/api/main-file/preview")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["filename"], "主檔260306test.xlsx")
        self.assertEqual(data["selected_sheet"], "庫存主檔")
        self.assertEqual(data["sheet_names"], ["庫存主檔", "第二頁"])
        self.assertTrue(data["style_preserved"])
        self.assertEqual(data["sheet"]["columns"][0]["letter"], "A")
        self.assertGreater(data["sheet"]["columns"][0]["width_px"], 100)
        self.assertEqual(data["sheet"]["rows"][0]["cells"][0]["value"], "主檔即時預覽")
        self.assertEqual(data["sheet"]["rows"][0]["cells"][0]["colspan"], 3)
        self.assertEqual(data["sheet"]["styles"][data["sheet"]["rows"][0]["cells"][0]["style_id"]]["background"], "#D9EAF7")

    def test_main_file_preview_can_switch_sheet(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"

            workbook = openpyxl.Workbook()
            workbook.active.title = "第一頁"
            second = workbook.create_sheet("第二頁")
            second["B2"] = "切換成功"
            workbook.save(main_path)
            workbook.close()

            def fake_setting(key, default=""):
                mapping = {
                    "main_file_path": str(main_path),
                    "main_filename": "main.xlsx",
                    "main_loaded_at": "2026-03-12T22:45:00",
                }
                return mapping.get(key, default)

            with patch("app.routers.main_file.db.get_setting", side_effect=fake_setting):
                response = self.client.get("/api/main-file/preview?sheet=第二頁")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["selected_sheet"], "第二頁")
        self.assertEqual(data["sheet"]["name"], "第二頁")
        target_cell = next(cell for cell in data["sheet"]["rows"][1]["cells"] if cell["col"] == 2)
        self.assertEqual(target_cell["value"], "切換成功")

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

    def test_root_and_static_disable_cache(self):
        root_response = self.client.get("/")
        static_response = self.client.get("/static/style.css")

        self.assertEqual(root_response.status_code, 200)
        self.assertEqual(static_response.status_code, 200)
        self.assertEqual(root_response.headers.get("cache-control"), "no-store, no-cache, must-revalidate, max-age=0")
        self.assertEqual(static_response.headers.get("cache-control"), "no-store, no-cache, must-revalidate, max-age=0")

    def test_get_bom_file_normalizes_legacy_xls_before_download(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_path = Path(temp_dir) / "legacy.xlsx"
            wb = openpyxl.Workbook()
            ws = wb.active
            ws["A1"] = "legacy"
            wb.save(bom_path)
            wb.close()

            raw_bom_record = {
                "id": "bom-legacy",
                "filename": "legacy.xls",
                "filepath": str(Path(temp_dir) / "legacy.xls"),
            }
            converted_bom_record = {
                **raw_bom_record,
                "filename": "legacy.xlsx",
                "filepath": str(bom_path),
            }

            with patch("app.routers.bom._get_required_bom", return_value=raw_bom_record), \
                 patch("app.routers.bom._ensure_editable_bom_record", return_value=converted_bom_record) as mock_ensure, \
                 patch("app.routers.bom.append_minute_timestamp", return_value="legacy_20260312_1740.xlsx"):
                response = self.client.get("/api/bom/bom-legacy/file")

        self.assertEqual(response.status_code, 200)
        self.assertIn("legacy_20260312_1740.xlsx", response.headers["content-disposition"])
        mock_ensure.assert_called_once_with(raw_bom_record)

    def test_bom_revision_list_returns_history(self):
        bom_record = {
            "id": "bom-1",
            "filename": "formal.xlsx",
            "filepath": "C:/formal.xlsx",
            "source_filename": "legacy.xls",
        }
        revisions = [
            {
                "id": 11,
                "bom_file_id": "bom-1",
                "revision_number": 2,
                "filename": "formal.xlsx",
                "filepath": "C:/history/v2.xlsx",
                "source_action": "edit",
                "note": "編輯後儲存",
                "created_at": "2026-03-12T17:40:00",
            },
            {
                "id": 7,
                "bom_file_id": "bom-1",
                "revision_number": 1,
                "filename": "formal.xlsx",
                "filepath": "C:/history/v1.xlsx",
                "source_action": "upload",
                "note": "上傳 BOM",
                "created_at": "2026-03-12T16:00:00",
            },
        ]

        with patch("app.routers.bom._get_required_bom", return_value=bom_record), \
             patch("app.routers.bom._ensure_editable_bom_record", return_value=bom_record), \
             patch("app.routers.bom.ensure_bom_revision_history", return_value=revisions) as mock_history:
            response = self.client.get("/api/bom/bom-1/revisions")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["bom"]["id"], "bom-1")
        self.assertEqual(data["bom"]["source_filename"], "legacy.xls")
        self.assertEqual(len(data["revisions"]), 2)
        self.assertEqual(data["revisions"][0]["revision_number"], 2)
        mock_history.assert_called_once_with(bom_record)

    def test_download_bom_revision_uses_versioned_filename(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            revision_path = Path(temp_dir) / "revision.xlsx"
            wb = openpyxl.Workbook()
            ws = wb.active
            ws["A1"] = "revision"
            wb.save(revision_path)
            wb.close()

            revision = {
                "id": 9,
                "bom_file_id": "bom-1",
                "revision_number": 3,
                "filename": "formal.xlsx",
                "filepath": str(revision_path),
            }

            with patch("app.routers.bom.db.get_bom_revision", return_value=revision), \
                 patch("app.routers.bom._build_revision_download_name", return_value="formal_v003_20260312_1740.xlsx"):
                response = self.client.get("/api/bom/bom-1/revisions/9/file")

        self.assertEqual(response.status_code, 200)
        self.assertIn("formal_v003_20260312_1740.xlsx", response.headers["content-disposition"])

    def test_save_bom_editor_creates_revision_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_path = Path(temp_dir) / "formal.xlsx"
            wb = openpyxl.Workbook()
            ws = wb.active
            ws["A1"] = "formal"
            wb.save(bom_path)
            wb.close()

            bom_record = {
                "id": "bom-1",
                "filename": "formal.xlsx",
                "filepath": str(bom_path),
                "uploaded_at": "2026-03-12T08:00:00",
                "source_filename": "formal.xlsx",
                "source_format": ".xlsx",
                "is_converted": 0,
            }
            parsed = BomFile(
                id="bom-1",
                filename="formal.xlsx",
                path=str(bom_path),
                po_number=123,
                model="MODEL-A",
                pcb="PCB-A",
                group_model="MODEL-A",
                order_qty=10,
                uploaded_at="2026-03-12T08:00:00",
                components=[BomComponent(part_number="PART-1", source_row=5)],
            )

            with patch("app.routers.bom._get_required_bom", return_value=bom_record), \
                 patch("app.routers.bom._ensure_editable_bom_record", return_value=bom_record), \
                 patch("app.routers.bom.ensure_bom_revision_history") as mock_ensure_history, \
                 patch("app.routers.bom.backup_bom_file", return_value=str(Path(temp_dir) / "backup.xlsx")), \
                 patch("app.routers.bom.apply_bom_editor_changes"), \
                 patch("app.routers.bom.parse_bom_for_storage", return_value=parsed), \
                 patch("app.routers.bom.db.save_bom_file") as mock_save_bom, \
                 patch("app.routers.bom.snapshot_bom_revision") as mock_snapshot_revision, \
                 patch("app.routers.bom.db.log_activity"):
                response = self.client.put("/api/bom/bom-1/editor", json={
                    "po_number": 123,
                    "order_qty": 10,
                    "model": "MODEL-A",
                    "pcb": "PCB-A",
                    "group_model": "MODEL-A",
                    "components": [
                        {
                            "source_row": 5,
                            "part_number": "PART-1",
                            "description": "",
                            "qty_per_board": 1,
                            "needed_qty": 2,
                            "prev_qty_cs": 3,
                            "is_dash": False,
                        }
                    ],
                })

        self.assertEqual(response.status_code, 200)
        mock_ensure_history.assert_called_once_with(bom_record)
        mock_save_bom.assert_called_once()
        mock_snapshot_revision.assert_called_once()

    def test_bom_download_zip_entries_include_timestamp(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            first_path = Path(temp_dir) / "first.xlsx"
            second_path = Path(temp_dir) / "second.xlsx"

            for path, value in ((first_path, "A"), (second_path, "B")):
                wb = openpyxl.Workbook()
                ws = wb.active
                ws["A1"] = value
                wb.save(path)
                wb.close()

            bom_records = [
                {"id": "bom-1", "filename": "first.xlsx", "filepath": str(first_path)},
                {"id": "bom-2", "filename": "second.xlsx", "filepath": str(second_path)},
            ]

            with patch("app.routers.bom.db.get_bom_files_by_models", return_value=bom_records), \
                 patch("app.routers.bom.append_minute_timestamp", side_effect=lambda filename, now=None: f"{Path(filename).stem}_20260312_1740{Path(filename).suffix}"), \
                 patch("app.routers.bom.build_generated_filename", return_value="BOM_20260312_1740.zip"):
                response = self.client.post("/api/bom/download", json={"models": ["MODEL-A"]})

        self.assertEqual(response.status_code, 200)
        archive = zipfile.ZipFile(io.BytesIO(response.content))
        self.assertEqual(sorted(archive.namelist()), ["first_20260312_1740.xlsx", "second_20260312_1740.xlsx"])
        archive.close()

    def test_dispatch_download_writes_prev_batch_and_supplements_to_separate_columns(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_path = Path(temp_dir) / "dispatch.xlsx"

            wb = openpyxl.Workbook()
            ws = wb.active
            ws.merge_cells("G1:H1")
            ws.cell(row=1, column=7).value = "製單號碼M/O:"
            ws.cell(row=1, column=10).value = "訂單數量:"
            ws.cell(row=2, column=3).value = "MODEL-A"
            ws.cell(row=2, column=4).value = "PCB-A"
            ws.cell(row=3, column=7).value = "上批餘料"
            ws.cell(row=3, column=8).value = "增添料數"
            ws.cell(row=5, column=3).value = "PART-1"
            ws.cell(row=5, column=6).value = 10
            ws.cell(row=5, column=7).value = 3
            ws.cell(row=5, column=8).value = 99
            ws.cell(row=6, column=3).value = "PART-2"
            ws.cell(row=6, column=6).value = 20
            ws.cell(row=6, column=8).value = 8
            ws.cell(row=7, column=3).value = "PART-3"
            ws.cell(row=7, column=6).value = 30
            ws.cell(row=7, column=7).value = 0
            ws.cell(row=7, column=8).value = 0
            ws.cell(row=8, column=3).value = "PART-4"
            ws.cell(row=8, column=6).value = 40
            ws.cell(row=8, column=7).value = 12
            ws.cell(row=8, column=8).value = 5
            wb.save(bom_path)
            wb.close()

            bom_record = {
                "id": "bom-1",
                "filename": "dispatch.xlsx",
                "filepath": str(bom_path),
                "source_filename": "dispatch.xlsx",
                "source_format": ".xlsx",
                "is_converted": 0,
                "po_number": "0",
                "group_model": "MODEL-A",
                "uploaded_at": "2026-03-12T08:00:00",
            }

            with patch("app.routers.bom.db.get_bom_files", return_value=[bom_record]):
                response = self.client.post("/api/bom/dispatch-download", json={
                    "bom_ids": ["bom-1"],
                    "supplements": {"PART-1": 7},
                    "header_overrides": {"bom-1": {"po_number": "4500059234"}},
                    "carry_overs": {"bom-1": {"PART-1": 135, "PART-2": 246}},
                })

        self.assertEqual(response.status_code, 200)
        downloaded = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = downloaded.active
        self.assertEqual(ws.cell(row=1, column=7).value, "製單號碼M/O:4500059234")
        self.assertEqual(ws.cell(row=5, column=7).value, 135)
        self.assertEqual(ws.cell(row=5, column=8).value, 7)
        self.assertEqual(ws.cell(row=6, column=7).value, 246)
        self.assertEqual(ws.cell(row=6, column=8).value, 0)
        self.assertEqual(ws.cell(row=7, column=7).value, 0)
        self.assertEqual(ws.cell(row=7, column=8).value, 0)
        self.assertEqual(ws.cell(row=8, column=7).value, 12)
        self.assertEqual(ws.cell(row=8, column=8).value, 0)
        downloaded.close()

    def test_dispatch_download_persists_order_supplements_for_selected_orders(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_path = Path(temp_dir) / "dispatch.xlsx"
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.merge_cells("G1:H1")
            ws.cell(row=1, column=7).value = "製單號碼M/O:"
            ws.cell(row=5, column=3).value = "PART-1"
            wb.save(bom_path)
            wb.close()

            bom_record = {
                "id": "bom-1",
                "filename": "dispatch.xlsx",
                "filepath": str(bom_path),
                "source_filename": "dispatch.xlsx",
                "source_format": ".xlsx",
                "is_converted": 0,
                "po_number": "0",
                "group_model": "MODEL-A",
                "uploaded_at": "2026-03-12T08:00:00",
            }

            with patch("app.routers.bom.db.get_bom_files", return_value=[bom_record]), \
                 patch("app.routers.bom._build_order_based_export_values", return_value=({}, {}, {})), \
                 patch("app.routers.bom.build_order_supplement_allocations", return_value={5: {"PART-1": 3000}}) as mock_allocations, \
                 patch("app.routers.bom.db.replace_order_supplements") as mock_replace:
                response = self.client.post("/api/bom/dispatch-download", json={
                    "bom_ids": ["bom-1"],
                    "order_ids": [5],
                    "supplements": {"PART-1": 3000},
                    "header_overrides": {"bom-1": {"po_number": "4500059234"}},
                    "carry_overs": {"bom-1": {"PART-1": 135}},
                })

        self.assertEqual(response.status_code, 200)
        mock_allocations.assert_called_once_with([5], {"PART-1": 3000})
        mock_replace.assert_called_once_with([5], {5: {"PART-1": 3000}})

    def test_dispatch_download_computes_carry_over_per_bom_in_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_a_path = Path(temp_dir) / "board-a.xlsx"
            bom_c_path = Path(temp_dir) / "board-c.xlsx"

            for path, label in ((bom_a_path, "A"), (bom_c_path, "C")):
                wb = openpyxl.Workbook()
                ws = wb.active
                ws.merge_cells("G1:H1")
                ws.cell(row=1, column=7).value = "M/O:"
                ws.cell(row=5, column=3).value = "EC-20023A"
                ws.cell(row=5, column=7).value = None
                ws.cell(row=5, column=8).value = None
                ws.cell(row=6, column=3).value = f"PART-{label}"
                ws.cell(row=6, column=7).value = None
                ws.cell(row=6, column=8).value = None
                wb.save(path)
                wb.close()

            bom_a = {
                "id": "bom-a",
                "filename": "board-a.xlsx",
                "filepath": str(bom_a_path),
                "source_filename": "board-a.xlsx",
                "source_format": ".xlsx",
                "is_converted": 1,
                "po_number": "0",
                "model": "T356789IU_MAIN_BOARD_A",
                "group_model": "MODEL-A",
                "sort_order": 0,
                "uploaded_at": "2026-03-12T08:00:00",
            }
            bom_c = {
                "id": "bom-c",
                "filename": "board-c.xlsx",
                "filepath": str(bom_c_path),
                "source_filename": "board-c.xlsx",
                "source_format": ".xlsx",
                "is_converted": 1,
                "po_number": "0",
                "model": "T356789IU_DISPLAY_C",
                "group_model": "MODEL-A",
                "sort_order": 1,
                "uploaded_at": "2026-03-12T08:01:00",
            }

            components_by_bom = {
                "bom-a": [
                    {"part_number": "EC-20023A", "needed_qty": 5505, "prev_qty_cs": 0, "is_dash": 0},
                    {"part_number": "PART-A", "needed_qty": 10, "prev_qty_cs": 0, "is_dash": 0},
                ],
                "bom-c": [
                    {"part_number": "EC-20023A", "needed_qty": 100, "prev_qty_cs": 0, "is_dash": 0},
                    {"part_number": "PART-C", "needed_qty": 20, "prev_qty_cs": 0, "is_dash": 0},
                ],
            }

            with patch("app.routers.bom.db.get_bom_files", return_value=[bom_a, bom_c]), \
                 patch("app.routers.bom.db.get_order", return_value={"id": 1, "model": "MODEL-A"}), \
                 patch("app.routers.bom.db.get_bom_components", side_effect=lambda bom_id: components_by_bom[bom_id]), \
                 patch("app.routers.bom.db.get_snapshot", return_value={
                     "EC-20023A": {"stock_qty": 5625, "moq": 0},
                     "PART-A": {"stock_qty": 30, "moq": 0},
                     "PART-C": {"stock_qty": 40, "moq": 0},
                 }), \
                 patch("app.routers.bom.db.get_setting", return_value=""), \
                 patch("app.routers.bom.db.get_snapshot_taken_at", return_value="2026-03-12T08:00:00"), \
                 patch("app.routers.bom.db.get_all_dispatched_consumption", return_value={}), \
                 patch("app.routers.bom.db.replace_order_supplements"):
                response = self.client.post("/api/bom/dispatch-download", json={
                    "bom_ids": ["bom-a", "bom-c"],
                    "order_ids": [1],
                    "supplements": {},
                    "header_overrides": {
                        "bom-a": {"po_number": "4500059234"},
                        "bom-c": {"po_number": "4500059234"},
                    },
                })

        self.assertEqual(response.status_code, 200)
        archive = zipfile.ZipFile(io.BytesIO(response.content))
        archive_names = sorted(archive.namelist())
        self.assertEqual(len(archive_names), 2)
        self.assertTrue(archive_names[0].startswith("board-a_"))
        self.assertTrue(archive_names[1].startswith("board-c_"))

        wb_a = openpyxl.load_workbook(io.BytesIO(archive.read(archive_names[0])), data_only=False)
        ws_a = wb_a.active
        self.assertEqual(ws_a.cell(row=5, column=7).value, 5625)
        self.assertEqual(ws_a.cell(row=5, column=8).value, 0)
        wb_a.close()

        wb_c = openpyxl.load_workbook(io.BytesIO(archive.read(archive_names[1])), data_only=False)
        ws_c = wb_c.active
        self.assertEqual(ws_c.cell(row=5, column=7).value, 120)
        self.assertEqual(ws_c.cell(row=5, column=8).value, 0)
        wb_c.close()
        archive.close()

    def test_dispatch_download_applies_supplement_once_and_carries_it_forward(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_a_path = Path(temp_dir) / "board-a.xlsx"
            bom_c_path = Path(temp_dir) / "board-c.xlsx"

            for path in (bom_a_path, bom_c_path):
                wb = openpyxl.Workbook()
                ws = wb.active
                ws.merge_cells("G1:H1")
                ws.cell(row=1, column=7).value = "M/O:"
                ws.cell(row=5, column=3).value = "EC-20023A"
                ws.cell(row=5, column=7).value = None
                ws.cell(row=5, column=8).value = None
                wb.save(path)
                wb.close()

            bom_a = {
                "id": "bom-a",
                "filename": "board-a.xlsx",
                "filepath": str(bom_a_path),
                "source_filename": "board-a.xlsx",
                "source_format": ".xlsx",
                "is_converted": 1,
                "po_number": "0",
                "model": "T356789IU_MAIN_BOARD_A",
                "group_model": "MODEL-A",
                "sort_order": 0,
                "uploaded_at": "2026-03-12T08:00:00",
            }
            bom_c = {
                "id": "bom-c",
                "filename": "board-c.xlsx",
                "filepath": str(bom_c_path),
                "source_filename": "board-c.xlsx",
                "source_format": ".xlsx",
                "is_converted": 1,
                "po_number": "0",
                "model": "T356789IU_DISPLAY_C",
                "group_model": "MODEL-A",
                "sort_order": 1,
                "uploaded_at": "2026-03-12T08:01:00",
            }

            components_by_bom = {
                "bom-a": [{"part_number": "EC-20023A", "needed_qty": 5505, "prev_qty_cs": 0, "is_dash": 0}],
                "bom-c": [{"part_number": "EC-20023A", "needed_qty": 2000, "prev_qty_cs": 0, "is_dash": 0}],
            }

            with patch("app.routers.bom.db.get_bom_files", return_value=[bom_a, bom_c]), \
                 patch("app.routers.bom.db.get_order", return_value={"id": 1, "model": "MODEL-A"}), \
                 patch("app.routers.bom.db.get_bom_components", side_effect=lambda bom_id: components_by_bom[bom_id]), \
                 patch("app.routers.bom.db.get_snapshot", return_value={
                     "EC-20023A": {"stock_qty": 5625, "moq": 0},
                 }), \
                 patch("app.routers.bom.db.get_setting", return_value=""), \
                 patch("app.routers.bom.db.get_snapshot_taken_at", return_value="2026-03-12T08:00:00"), \
                 patch("app.routers.bom.db.get_all_dispatched_consumption", return_value={}), \
                 patch("app.routers.bom.build_order_supplement_allocations", return_value={1: {"EC-20023A": 3000}}), \
                 patch("app.routers.bom.db.replace_order_supplements"):
                response = self.client.post("/api/bom/dispatch-download", json={
                    "bom_ids": ["bom-a", "bom-c"],
                    "order_ids": [1],
                    "supplements": {"EC-20023A": 3000},
                    "header_overrides": {
                        "bom-a": {"po_number": "4500059234"},
                        "bom-c": {"po_number": "4500059234"},
                    },
                })

        self.assertEqual(response.status_code, 200)
        archive = zipfile.ZipFile(io.BytesIO(response.content))
        archive_names = sorted(archive.namelist())
        self.assertEqual(len(archive_names), 2)

        wb_a = openpyxl.load_workbook(io.BytesIO(archive.read(archive_names[0])), data_only=False)
        ws_a = wb_a.active
        self.assertEqual(ws_a.cell(row=5, column=7).value, 5625)
        self.assertEqual(ws_a.cell(row=5, column=8).value, 0)
        wb_a.close()

        wb_c = openpyxl.load_workbook(io.BytesIO(archive.read(archive_names[1])), data_only=False)
        ws_c = wb_c.active
        self.assertEqual(ws_c.cell(row=5, column=7).value, 120)
        self.assertEqual(ws_c.cell(row=5, column=8).value, 3000)
        wb_c.close()
        archive.close()

    def test_dispatch_download_normalizes_legacy_xls_before_export(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bom_path = Path(temp_dir) / "dispatch.xlsx"

            wb = openpyxl.Workbook()
            ws = wb.active
            ws.merge_cells("G1:H1")
            ws.cell(row=1, column=7).value = "鋆賢?ⅣM/O:"
            ws.cell(row=3, column=7).value = "銝擗?"
            ws.cell(row=3, column=8).value = "憓溶?"
            ws.cell(row=5, column=3).value = "PART-1"
            ws.cell(row=5, column=7).value = 0
            ws.cell(row=5, column=8).value = 0
            wb.save(bom_path)
            wb.close()

            raw_bom_record = {
                "id": "bom-legacy",
                "filename": "dispatch.xls",
                "filepath": str(Path(temp_dir) / "dispatch.xls"),
                "source_filename": "dispatch.xls",
                "source_format": ".xls",
                "is_converted": 0,
                "po_number": "0",
                "group_model": "MODEL-A",
                "uploaded_at": "2026-03-12T08:00:00",
            }
            converted_bom_record = {
                **raw_bom_record,
                "filename": "dispatch.xlsx",
                "filepath": str(bom_path),
                "is_converted": 1,
            }

            with patch("app.routers.bom.db.get_bom_files", return_value=[raw_bom_record]), \
                 patch("app.routers.bom._ensure_editable_bom_record", return_value=converted_bom_record) as mock_ensure:
                response = self.client.post("/api/bom/dispatch-download", json={
                    "bom_ids": ["bom-legacy"],
                    "supplements": {"PART-1": 7},
                    "header_overrides": {"bom-legacy": {"po_number": "4500059234"}},
                    "carry_overs": {"bom-legacy": {"PART-1": 135}},
                })

        self.assertEqual(response.status_code, 200)
        mock_ensure.assert_called_once_with(raw_bom_record)
        downloaded = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = downloaded.active
        self.assertEqual(ws.cell(row=1, column=7).value, "鋆賢?ⅣM/O:4500059234")
        self.assertEqual(ws.cell(row=5, column=7).value, 135)
        self.assertEqual(ws.cell(row=5, column=8).value, 7)
        downloaded.close()

    def test_get_database_backups_returns_overview(self):
        overview = {
            "enabled": True,
            "hour": 2,
            "minute": 0,
            "keep_count": 14,
            "backups": [{"name": "system_backup_20260313_020000.db"}],
        }

        with patch("app.routers.system.db_backup.get_database_backup_overview", return_value=overview):
            response = self.client.get("/api/system/db-backups")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), overview)

    def test_get_system_app_meta_returns_version_payload(self):
        meta = {
            "app_name": "OpenText 出貨排程系統",
            "version": "v2026.03.16.1",
            "released_at": "2026-03-16",
            "headline": "這版聚焦在發料流程與版本可視性。",
            "sections": [{"title": "發料", "items": ["加入版本號與更新說明"]}],
        }

        with patch("app.routers.system.get_app_meta", return_value=meta):
            response = self.client.get("/api/system/app-meta")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), meta)

    def test_get_st_inventory_data_returns_snapshot_payload(self):
        snapshot = {
            "PART-1": {"stock_qty": 8.0, "description": "Capacitor"},
            "PART-2": {"stock_qty": 3.0, "description": "Resistor"},
        }

        with patch("app.routers.system.db.get_st_inventory_snapshot", return_value=snapshot), \
             patch("app.routers.system.db.get_setting", side_effect=lambda key, default="": {
                 "st_inventory_loaded_at": "2026-03-16T09:30:00",
                 "st_inventory_filename": "st_inventory.xlsx",
             }.get(key, default)):
            response = self.client.get("/api/system/st-inventory/data")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "stock": {"PART-1": 8.0, "PART-2": 3.0},
            "descriptions": {"PART-1": "Capacitor", "PART-2": "Resistor"},
            "part_count": 2,
            "loaded_at": "2026-03-16T09:30:00",
            "filename": "st_inventory.xlsx",
        })

    def test_update_database_backup_settings_uses_request_payload(self):
        with patch(
            "app.routers.system.db_backup.update_database_backup_settings",
            return_value={"enabled": False, "hour": 4, "minute": 30, "keep_count": 7},
        ) as mock_update:
            response = self.client.put("/api/system/db-backups/settings", json={
                "enabled": False,
                "hour": 4,
                "minute": 30,
                "keep_count": 7,
            })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "ok": True,
            "enabled": False,
            "hour": 4,
            "minute": 30,
            "keep_count": 7,
        })
        mock_update.assert_called_once_with(enabled=False, hour=4, minute=30, keep_count=7)

    def test_restore_database_backup_endpoint_returns_reload_flag(self):
        with patch(
            "app.routers.system.db_backup.restore_database_backup",
            return_value={
                "restored_backup": {"name": "system_backup_20260313_020000.db"},
                "safety_backup": {"name": "system_backup_20260313_113000.db"},
                "restored_at": "2026-03-13T11:30:00",
                "removed_count": 0,
            },
        ) as mock_restore:
            response = self.client.post("/api/system/db-backups/restore", json={
                "backup_name": "system_backup_20260313_020000.db",
            })

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["requires_reload"])
        mock_restore.assert_called_once_with("system_backup_20260313_020000.db")
