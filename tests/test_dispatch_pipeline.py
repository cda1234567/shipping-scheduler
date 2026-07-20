from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services import dispatch_pipeline


class DispatchPipelineTests(unittest.TestCase):
    def test_prepare_dispatch_context_syncs_bom_before_scaling_components(self):
        order = {
            "id": 44,
            "po_number": "4500059234",
            "model": "MODEL-M24C",
            "order_qty": 300,
            "status": "merged",
            "code": "4-4",
        }
        raw_bom = {
            "id": "bom-m24c",
            "model": "MODEL-M24C",
            "group_model": "MODEL-M24C",
            "order_qty": 300,
        }
        synced_bom = {
            **raw_bom,
            "order_qty": 600,
        }
        components = [{
            "part_number": "IC-M24C",
            "description": "EEPROM",
            "qty_per_board": 1,
            "needed_qty": 600,
            "prev_qty_cs": 0,
            "is_dash": 0,
        }]

        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"placeholder")
            raw_bom["filepath"] = str(main_path)

            with patch("app.services.dispatch_pipeline.db.get_order", return_value=order), \
                 patch("app.services.dispatch_pipeline.db.get_bom_files_by_models", return_value=[raw_bom]), \
                 patch("app.services.dispatch_pipeline._ensure_editable_bom_for_draft", return_value=synced_bom) as mock_sync, \
                 patch("app.services.dispatch_pipeline.db.get_bom_components", return_value=components):
                _, groups, all_components = dispatch_pipeline.prepare_dispatch_context(44, str(main_path))

        mock_sync.assert_called_once_with(raw_bom)
        self.assertEqual(groups[0]["components"][0]["needed_qty"], 300)
        self.assertEqual(all_components[0]["needed_qty"], 300)

    def test_prepare_dispatch_context_uses_stored_components_without_touching_bom_file(self):
        order = {
            "id": 44,
            "model": "MODEL-M24C",
            "order_qty": 300,
            "status": "merged",
        }
        raw_bom = {
            "id": "bom-m24c",
            "model": "MODEL-M24C",
            "group_model": "MODEL-M24C",
            "order_qty": 300,
            "filepath": "legacy.xls",
        }
        components = [{
            "part_number": "IC-M24C",
            "qty_per_board": 1,
            "needed_qty": 300,
            "prev_qty_cs": 0,
            "is_dash": 0,
        }]

        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"placeholder")

            with patch("app.services.dispatch_pipeline.db.get_order", return_value=order), \
                 patch("app.services.dispatch_pipeline.db.get_bom_files_by_models", return_value=[raw_bom]), \
                 patch("app.services.dispatch_pipeline._ensure_editable_bom_for_draft") as mock_sync, \
                 patch("app.services.dispatch_pipeline.db.get_bom_components", return_value=components):
                _, groups, all_components = dispatch_pipeline.prepare_dispatch_context(
                    44,
                    str(main_path),
                    sync_bom_files=False,
                )

        mock_sync.assert_not_called()
        self.assertEqual(groups[0]["components"][0]["needed_qty"], 300)
        self.assertEqual(all_components[0]["needed_qty"], 300)

    def test_rollback_availability_marks_missing_session_and_backup(self):
        orders = [{"id": 1}, {"id": 2}, {"id": 3}]
        with tempfile.TemporaryDirectory() as temp_dir:
            existing_backup = Path(temp_dir) / "existing.xlsx"
            existing_backup.write_bytes(b"backup")
            sessions = [
                {"id": 11, "order_id": 1, "backup_path": str(existing_backup)},
                {"id": 12, "order_id": 2, "backup_path": str(Path(temp_dir) / "missing.xlsx")},
            ]
            with patch("app.services.dispatch_pipeline.db.get_active_dispatch_sessions", return_value=sessions):
                result = dispatch_pipeline.build_dispatch_rollback_availability(orders)

        self.assertTrue(result[1]["available"])
        self.assertFalse(result[2]["available"])
        self.assertIn("備份", result[2]["reason"])
        self.assertFalse(result[3]["available"])
        self.assertIn("發料歷史", result[3]["reason"])


if __name__ == "__main__":
    unittest.main()


class ResetModeSupplementPrefillTests(unittest.TestCase):
    def test_reset_mode_keeps_general_part_input_after_it_fully_resolves_shortage(self):
        from app.services.dispatch_pipeline import DispatchContext, DispatchPlan

        context = DispatchContext(
            order={"id": 1, "code": "9-1", "model": "MODEL-A"},
            groups=[],
            all_components=[],
        )
        preview = {
            "shortages": [],
            "batches": [{
                "order_id": 1,
                "model": "MODEL-A",
                "groups": [{"batch_code": "9-1", "rows": [{
                    "part_number": "EC-TEST",
                    "current_stock": 0,
                    "needed_qty": 300,
                    "shortage_amount": 0,
                    "supplement_qty": 300,
                    "j_value": 0,
                    "decision": "CreateRequirement",
                }]}],
            }],
        }

        plan = DispatchPlan(
            main_path="",
            contexts=[context],
            preview=preview,
            reset_stored=True,
        )
        item = plan.to_preview_response()["scopes"][0]["shortages"][0]

        self.assertEqual(item["supplement_qty"], 300)
        self.assertEqual(item["default_supplement"], 300)

    def test_reset_mode_keeps_general_part_input_when_shortage_remains(self):
        from app.services.dispatch_pipeline import DispatchContext, DispatchPlan

        contexts = [
            DispatchContext(order={"id": 1, "code": "9-1", "model": "MODEL-A"}, groups=[], all_components=[]),
            DispatchContext(order={"id": 2, "code": "9-2", "model": "MODEL-B"}, groups=[], all_components=[]),
        ]
        preview = {
            "shortages": [
                {"order_id": 2, "part_number": "EC-TEST", "shortage_amount": 500, "suggested_qty": 500},
            ],
            "batches": [
                {"order_id": 1, "model": "MODEL-A", "groups": [{"batch_code": "9-1", "rows": [{
                    "part_number": "EC-TEST", "current_stock": 0, "needed_qty": 1000,
                    "shortage_amount": 0, "supplement_qty": 1000, "j_value": 0,
                    "decision": "CreateRequirement",
                }]}]},
                {"order_id": 2, "model": "MODEL-B", "groups": [{"batch_code": "9-2", "rows": [{
                    "part_number": "EC-TEST", "current_stock": 0, "needed_qty": 500,
                    "shortage_amount": 500, "supplement_qty": 0, "j_value": -500,
                }]}]},
            ],
        }

        plan = DispatchPlan(main_path="", contexts=contexts, preview=preview, reset_stored=True)
        scopes = plan.to_preview_response()["scopes"]

        self.assertEqual(scopes[0]["shortages"][0]["supplement_qty"], 1000)
        self.assertEqual(scopes[0]["shortages"][0]["lookahead_suggested_qty"], 500)
        self.assertEqual(scopes[1]["shortages"], [])

    def test_order_scoped_ic_parts_stay_on_each_order_with_own_suggestion(self):
        from app.services.dispatch_pipeline import DispatchContext, DispatchPlan

        contexts = [
            DispatchContext(order={"id": 1, "code": "8-1", "model": "MODEL-A"}, groups=[], all_components=[]),
            DispatchContext(order={"id": 2, "code": "8-2", "model": "MODEL-B"}, groups=[], all_components=[]),
        ]
        parts = ("IC-STM32F", "IC-XC2C32A", "IC-M24C02")
        shortages = []
        for index, part in enumerate(parts, start=1):
            shortages.extend([
                {
                    "order_id": 1,
                    "part_number": part,
                    "shortage_amount": index * 100,
                    "suggested_qty": index * 100,
                },
                {
                    "order_id": 2,
                    "part_number": part,
                    "shortage_amount": index * 100 + 50,
                    "suggested_qty": index * 100 + 50,
                },
            ])

        plan = DispatchPlan(
            main_path="",
            contexts=contexts,
            preview={"shortages": shortages, "batches": []},
            reset_stored=True,
        )
        scopes = plan.to_preview_response()["scopes"]

        self.assertEqual(len(scopes[0]["shortages"]), 3)
        self.assertEqual(len(scopes[1]["shortages"]), 3)
        first_order = {item["part_number"]: item for item in scopes[0]["shortages"]}
        second_order = {item["part_number"]: item for item in scopes[1]["shortages"]}
        for index, part in enumerate(parts, start=1):
            self.assertEqual(first_order[part]["supplement_qty"], index * 100)
            self.assertEqual(second_order[part]["supplement_qty"], index * 100 + 50)

    def test_fully_supplemented_order_scoped_part_keeps_each_order_editor(self):
        from app.services.dispatch_pipeline import DispatchContext, DispatchPlan

        contexts = [
            DispatchContext(order={"id": 1, "code": "8-1", "model": "MODEL-A"}, groups=[], all_components=[]),
            DispatchContext(order={"id": 2, "code": "8-2", "model": "MODEL-B"}, groups=[], all_components=[]),
        ]
        preview = {
            "shortages": [],
            "batches": [
                {"order_id": 1, "model": "MODEL-A", "groups": [{"batch_code": "8-1", "rows": [{
                    "part_number": "IC-M24C02", "current_stock": 0, "needed_qty": 200,
                    "shortage_amount": 0, "supplement_qty": 200, "j_value": 0,
                }]}]},
                {"order_id": 2, "model": "MODEL-B", "groups": [{"batch_code": "8-2", "rows": [{
                    "part_number": "IC-M24C02", "current_stock": 0, "needed_qty": 300,
                    "shortage_amount": 0, "supplement_qty": 300, "j_value": 0,
                }]}]},
            ],
        }

        plan = DispatchPlan(main_path="", contexts=contexts, preview=preview)
        scopes = plan.to_preview_response()["scopes"]

        self.assertEqual(scopes[0]["shortages"][0]["supplement_qty"], 200)
        self.assertEqual(scopes[0]["shortages"][0]["model"], "MODEL-A")
        self.assertEqual(scopes[1]["shortages"][0]["supplement_qty"], 300)
        self.assertEqual(scopes[1]["shortages"][0]["model"], "MODEL-B")

    def test_reset_mode_prefills_total_suggestion_only_on_first_shortage_order(self):
        from app.services.dispatch_pipeline import DispatchContext, DispatchPlan
        contexts = [
            DispatchContext(order={"id": 1, "code": "8-1", "model": "MODEL-A"}, groups=[], all_components=[]),
            DispatchContext(order={"id": 2, "code": "8-2", "model": "MODEL-B"}, groups=[], all_components=[]),
        ]
        preview = {
            "shortages": [
                {"order_id": 2, "part_number": "PART-1", "shortage_amount": 10000, "suggested_qty": 10000},
            ],
            "batches": [
                {"order_id": 1, "model": "MODEL-A", "groups": [{"batch_code": "8-1", "rows": [{
                    "part_number": "PART-1", "current_stock": 2000, "needed_qty": 1000,
                    "shortage_amount": 0, "supplement_qty": 0, "j_value": 1000,
                }]}]},
                {"order_id": 2, "model": "MODEL-B", "groups": [{"batch_code": "8-2", "rows": [{
                    "part_number": "PART-1", "current_stock": 1000, "needed_qty": 11000,
                    "shortage_amount": 10000, "supplement_qty": 0, "j_value": -10000,
                }]}]},
            ],
        }
        plan = DispatchPlan(main_path="", contexts=contexts, preview=preview, reset_stored=True)
        out = plan.to_preview_response()
        self.assertEqual(out["scopes"][0]["shortages"], [])
        self.assertEqual(len(out["scopes"][1]["shortages"]), 1)
        first = out["scopes"][1]["shortages"][0]
        self.assertEqual(first["supplement_qty"], 10000)
        self.assertEqual(first["default_supplement"], 10000)
        self.assertEqual(first["lookahead_shortage_amount"], 10000)

    def test_partial_stored_supplement_keeps_editor_on_original_order(self):
        from app.services.dispatch_pipeline import DispatchContext, DispatchPlan
        contexts = [
            DispatchContext(order={"id": 1, "code": "8-1", "model": "MODEL-A"}, groups=[], all_components=[]),
            DispatchContext(order={"id": 2, "code": "8-2", "model": "MODEL-B"}, groups=[], all_components=[]),
        ]
        preview = {
            "shortages": [
                {"order_id": 2, "part_number": "PART-1", "shortage_amount": 500, "suggested_qty": 500},
            ],
            "batches": [
                {"order_id": 1, "model": "MODEL-A", "groups": [{"batch_code": "8-1", "rows": [{
                    "part_number": "PART-1", "current_stock": 0, "needed_qty": 1000,
                    "shortage_amount": 0, "supplement_qty": 1000, "j_value": 0,
                }]}]},
                {"order_id": 2, "model": "MODEL-B", "groups": [{"batch_code": "8-2", "rows": [{
                    "part_number": "PART-1", "current_stock": 0, "needed_qty": 500,
                    "shortage_amount": 500, "supplement_qty": 0, "j_value": -500,
                }]}]},
            ],
        }
        plan = DispatchPlan(main_path="", contexts=contexts, preview=preview)
        out = plan.to_preview_response()
        self.assertEqual(len(out["scopes"][0]["shortages"]), 1)
        self.assertEqual(out["scopes"][1]["shortages"], [])
        first = out["scopes"][0]["shortages"][0]
        self.assertEqual(first["supplement_qty"], 1000)
        self.assertEqual(first["lookahead_shortage_amount"], 500)

    def test_non_reset_mode_keeps_stored_supplement(self):
        from app.services.dispatch_pipeline import DispatchPlan
        plan = DispatchPlan(main_path="", contexts=[], preview={})
        out = plan._normalize_preview_shortage({"supplement_qty": 0, "suggested_qty": 10000})
        self.assertEqual(out.get("supplement_qty", 0), 0)

    def test_reset_mode_respects_existing_positive_supplement(self):
        from app.services.dispatch_pipeline import DispatchPlan
        plan = DispatchPlan(main_path="", contexts=[], preview={})
        plan.reset_stored = True
        out = plan._normalize_preview_shortage({"supplement_qty": 375, "suggested_qty": 10000})
        self.assertEqual(out["supplement_qty"], 375)
