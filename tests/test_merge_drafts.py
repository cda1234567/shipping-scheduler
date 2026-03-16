from __future__ import annotations

import unittest
from openpyxl import Workbook
from unittest.mock import patch

from app.services import merge_drafts


class MergeDraftDetailTests(unittest.TestCase):
    def test_plan_order_draft_keeps_manual_supplement_even_without_shortage(self):
        order = {"id": 12, "code": "1-1", "model": "MODEL-A"}
        draft = {"decisions": {}, "supplements": {"PART-1": 25}}
        bom_files = [{"id": "bom-1", "model": "MODEL-A", "group_model": "MODEL-A"}]
        running_stock = {"PART-1": 100.0}
        moq_map = {"PART-1": 0.0}
        components = [{
            "part_number": "PART-1",
            "description": "Resistor",
            "needed_qty": 10,
            "prev_qty_cs": 0,
            "is_dash": 0,
        }]

        with patch("app.services.merge_drafts.db.get_bom_components", return_value=components):
            plan = merge_drafts._plan_order_draft(order, draft, bom_files, running_stock, moq_map)

        self.assertEqual(plan["file_plans"][0]["supplements"], {"PART-1": 25.0})
        self.assertEqual(plan["running_stock"]["PART-1"], 115.0)

    def test_get_draft_detail_includes_preview_rows_without_shortages(self):
        draft = {
            "id": 5,
            "order_id": 12,
            "status": "active",
            "main_loaded_at": "2026-03-13T15:00:00",
            "updated_at": "2026-03-13T15:05:00",
            "supplements": {},
            "decisions": {},
            "shortages": [],
        }
        file_item = {
            "id": 9,
            "bom_file_id": "bom-1",
            "filename": "draft-1.xlsx",
            "filepath": "C:/draft-1.xlsx",
            "source_filename": "source-1.xlsx",
            "source_format": ".xlsx",
            "model": "MODEL-A",
            "group_model": "MODEL-A",
            "carry_overs": {"PART-1": 120, "PART-2": 45},
            "supplements": {"PART-2": 300},
        }
        components = [
            {
                "part_number": "PART-1",
                "description": "Resistor",
                "needed_qty": 10,
                "prev_qty_cs": 0,
                "is_dash": 0,
                "is_customer_supplied": 0,
            },
            {
                "part_number": "PART-2",
                "description": "Capacitor",
                "needed_qty": 6,
                "prev_qty_cs": 2,
                "is_dash": 0,
                "is_customer_supplied": 0,
            },
        ]

        with patch("app.services.merge_drafts.db.get_merge_draft", return_value=draft), \
             patch("app.services.merge_drafts.db.get_order", return_value={"id": 12, "model": "MODEL-A"}), \
             patch("app.services.merge_drafts.db.get_merge_draft_files", return_value=[file_item]), \
             patch("app.services.merge_drafts.db.get_bom_components", return_value=components):
            detail = merge_drafts.get_draft_detail(5)

        preview_rows = detail["draft"]["files"][0]["preview_rows"]
        self.assertEqual(len(preview_rows), 2)
        self.assertEqual(preview_rows[0]["part_number"], "PART-1")
        self.assertEqual(preview_rows[0]["carry_over"], 120.0)
        self.assertEqual(preview_rows[0]["supplement_qty"], 0.0)
        self.assertEqual(preview_rows[1]["part_number"], "PART-2")
        self.assertEqual(preview_rows[1]["needed"], 6.0)
        self.assertEqual(preview_rows[1]["prev_qty_cs"], 2.0)
        self.assertEqual(preview_rows[1]["supplement_qty"], 300.0)

    def test_plan_order_draft_treats_ec_below_100_as_shortage(self):
        order = {"id": 18, "code": "2-1", "model": "MODEL-EC"}
        draft = {"decisions": {}, "supplements": {"EC-001": 15}}
        bom_files = [{"id": "bom-ec", "model": "MODEL-EC", "group_model": "MODEL-EC"}]
        running_stock = {"EC-001": 120.0}
        moq_map = {"EC-001": 20.0}
        components = [{
            "part_number": "EC-001",
            "description": "EC Part",
            "needed_qty": 30,
            "prev_qty_cs": 0,
            "is_dash": 0,
        }]

        with patch("app.services.merge_drafts.db.get_bom_components", return_value=components):
            plan = merge_drafts._plan_order_draft(order, draft, bom_files, running_stock, moq_map)

        self.assertEqual(plan["file_plans"][0]["supplements"], {"EC-001": 15.0})
        self.assertEqual(plan["running_stock"]["EC-001"], 105.0)
        self.assertEqual(plan["shortages"], [])

    def test_plan_order_draft_records_resulting_stock_for_remaining_shortage(self):
        order = {"id": 20, "code": "3-1", "model": "MODEL-B"}
        draft = {"decisions": {}, "supplements": {}}
        bom_files = [{"id": "bom-2", "model": "MODEL-B", "group_model": "MODEL-B"}]
        running_stock = {"PART-9": 4.0}
        moq_map = {"PART-9": 10.0}
        components = [{
            "part_number": "PART-9",
            "description": "IC",
            "needed_qty": 7,
            "prev_qty_cs": 2,
            "is_dash": 0,
        }]

        with patch("app.services.merge_drafts.db.get_bom_components", return_value=components):
            plan = merge_drafts._plan_order_draft(order, draft, bom_files, running_stock, moq_map)

        self.assertEqual(len(plan["shortages"]), 1)
        self.assertEqual(plan["shortages"][0]["prev_qty_cs"], 2.0)
        self.assertEqual(plan["shortages"][0]["resulting_stock"], -1.0)

    def test_plan_order_draft_keeps_total_supply_suggestion_when_st_covers_part_of_shortage(self):
        order = {"id": 22, "code": "3-2", "model": "MODEL-ST"}
        draft = {"decisions": {}, "supplements": {}}
        bom_files = [{"id": "bom-st", "model": "MODEL-ST", "group_model": "MODEL-ST"}]
        running_stock = {"PART-ST": 2.0}
        moq_map = {"PART-ST": 5.0}
        components = [{
            "part_number": "PART-ST",
            "description": "ST assisted part",
            "needed_qty": 10,
            "prev_qty_cs": 0,
            "is_dash": 0,
        }]

        with patch("app.services.merge_drafts.db.get_bom_components", return_value=components):
            plan = merge_drafts._plan_order_draft(
                order,
                draft,
                bom_files,
                running_stock,
                moq_map,
                {"PART-ST": 6.0},
            )

        shortage = plan["shortages"][0]
        self.assertEqual(shortage["shortage_amount"], 8.0)
        self.assertEqual(shortage["st_available_qty"], 6.0)
        self.assertEqual(shortage["purchase_needed_qty"], 2.0)
        self.assertEqual(shortage["purchase_suggested_qty"], 5.0)
        self.assertEqual(shortage["suggested_qty"], 11.0)
        self.assertEqual(plan["file_plans"][0]["purchase_parts"], ["PART-ST"])

    def test_write_dispatch_values_to_ws_marks_purchase_parts_in_orange(self):
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.cell(row=5, column=3, value="PART-ST")

        merge_drafts._write_dispatch_values_to_ws(
            worksheet,
            supplements={"PART-ST": 11},
            carry_overs={},
            purchase_parts={"PART-ST"},
        )

        h_cell = worksheet.cell(row=5, column=8)
        self.assertEqual(h_cell.value, 11)
        self.assertEqual(h_cell.fill.fill_type, "solid")
        self.assertTrue(str(h_cell.fill.start_color.rgb or "").endswith("FFC000"))

    def test_restore_recent_committed_merge_drafts_reactivates_and_rebuilds(self):
        active_drafts = [{"id": 7, "order_id": 21}]

        with patch("app.services.merge_drafts.db.get_active_merge_draft_for_order", return_value=None), \
             patch("app.services.merge_drafts.db.get_latest_committed_merge_draft_for_order", return_value={"id": 7, "order_id": 21}), \
             patch("app.services.merge_drafts.db.reactivate_merge_draft", return_value=1) as mock_reactivate, \
             patch("app.services.merge_drafts.db.get_active_merge_drafts", return_value=active_drafts), \
             patch("app.services.merge_drafts.rebuild_merge_drafts", return_value=active_drafts) as mock_rebuild:
            restored = merge_drafts.restore_recent_committed_merge_drafts([21])

        self.assertEqual(restored, [21])
        mock_reactivate.assert_called_once_with(7)
        mock_rebuild.assert_called_once_with([21])
