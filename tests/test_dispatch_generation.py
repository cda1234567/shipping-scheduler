from __future__ import annotations

import io
import unittest
from unittest.mock import patch

import openpyxl
from fastapi.testclient import TestClient

from main import app
from app.routers.dispatch import _build_dispatch_result_by_order, _get_selected_orders


class DispatchGenerationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_selected_orders_include_dispatched_and_completed_orders(self):
        orders = {
            1: {"id": 1, "status": "merged"},
            2: {"id": 2, "status": "dispatched"},
            3: {"id": 3, "status": "completed"},
            4: {"id": 4, "status": "cancelled"},
        }

        with patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)):
            selected = _get_selected_orders([1, 2, 3, 4])

        self.assertEqual([order["id"] for order in selected], [1, 2, 3])

    def test_dispatch_generate_uses_selected_orders_saved_supplements_and_sample_layout(self):
        orders = {
            1: {
                "id": 1,
                "status": "merged",
                "po_number": "4500059234",
                "model": "MODEL-A",
                "code": "1-3",
                "delivery_date": "2026-03-27",
            },
            2: {
                "id": 2,
                "status": "pending",
                "po_number": "4500059162",
                "model": "MODEL-B",
                "code": "1-4",
                "delivery_date": "2026-05-08",
            },
        }
        bom_map = {
            "MODEL-A": [
                {"part_number": "PART-SAVED", "description": "Saved desc", "needed_qty": 1, "is_dash": 0},
                {"part_number": "PART-MISSING", "description": "Missing desc", "needed_qty": 1, "is_dash": 0},
            ],
            "MODEL-B": [
                {"part_number": "PART-PENDING", "description": "Pending desc", "needed_qty": 1, "is_dash": 0},
            ],
        }
        calc_results = [
            {
                "order_id": 1,
                "shortages": [
                    {"part_number": "PART-SAVED", "description": "Saved desc", "suggested_qty": 999, "shortage_amount": 999, "purchase_needed_qty": 999},
                    {"part_number": "PART-MISSING", "description": "Missing desc", "suggested_qty": 888, "shortage_amount": 888, "purchase_needed_qty": 0},
                ],
                "customer_material_shortages": [],
            },
            {
                "order_id": 2,
                "shortages": [
                    {"part_number": "PART-PENDING", "description": "Pending desc", "suggested_qty": 2400, "shortage_amount": 2400, "purchase_needed_qty": 2400},
                ],
                "customer_material_shortages": [],
            },
        ]

        with patch("app.routers.dispatch.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.dispatch._load_shortage_inputs", return_value=({}, {}, {})), \
             patch("app.routers.dispatch.calc_run", return_value=calc_results), \
             patch("app.routers.dispatch.db.get_order_supplements", return_value={1: {"PART-SAVED": 3000}}), \
             patch("app.routers.dispatch.db.get_decisions_for_order", side_effect=lambda order_id: {
                 1: {"PART-SAVED": "CreateRequirement", "PART-MISSING": "Shortage"},
                 2: {},
             }.get(order_id, {})), \
             patch("app.routers.dispatch.build_generated_filename", return_value="發料單_20260312_1740.xlsx"), \
             patch("app.routers.dispatch.db.log_activity"):
            response = self.client.post("/api/dispatch/generate", json={
                "order_ids": [1, 2],
                "decisions": {"PART-PENDING": "CreateRequirement"},
            })

        self.assertEqual(response.status_code, 200)
        self.assertIn("20260312_1740", response.headers["content-disposition"])

        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = wb.active
        self.assertEqual(ws.cell(row=1, column=1).value, "1-3")
        self.assertEqual(ws.cell(row=2, column=1).value, 4500059234)
        self.assertEqual(ws.cell(row=1, column=5).value, "日期")
        self.assertEqual(ws.cell(row=3, column=3).value, "PART-MISSING")
        self.assertEqual(ws.cell(row=3, column=5).value, "缺")
        self.assertEqual(ws.cell(row=4, column=3).value, "PART-SAVED")
        self.assertEqual(ws.cell(row=4, column=5).value, 3000)
        self.assertEqual(ws.cell(row=5, column=1).value, "1-4")
        self.assertEqual(ws.cell(row=7, column=3).value, "PART-PENDING")
        self.assertEqual(ws.cell(row=7, column=5).value, 2400)
        self.assertIn("A1:C1", {str(rng) for rng in ws.merged_cells.ranges})
        self.assertIn("D1:D2", {str(rng) for rng in ws.merged_cells.ranges})
        self.assertEqual(ws.title, "庚霖-TD2U  ")
        self.assertEqual(ws["A1"].font.name, "Calibri")
        self.assertEqual(ws["C3"].font.name, "Calibri")
        self.assertEqual(ws["D3"].font.name, "Calibri")
        self.assertEqual(ws["E3"].font.name, "Calibri")
        self.assertEqual(ws["E4"].font.name, "Calibri")
        self.assertEqual(ws["E3"].fill.fgColor.rgb, "FFFFFFFF")
        self.assertEqual(ws["E4"].fill.fgColor.rgb, "FFFFC000")
        self.assertEqual(ws.column_dimensions["D"].width, 72)
        self.assertTrue(str(ws["D1"].value).startswith("辰尚-庚霖"))
        self.assertFalse(str(ws["D1"].value).startswith(" "))
        self.assertEqual(ws["E2"].font.name, "Calibri")
        self.assertEqual(ws["E2"].font.sz, 9)
        wb.close()

    def test_dispatch_generate_recalculates_dispatched_order_with_its_own_consumption_added_back(self):
        orders = {
            1: {
                "id": 1,
                "status": "dispatched",
                "po_number": "4500059234",
                "model": "MODEL-A",
                "code": "5-1",
                "delivery_date": "2026-05-01",
            },
        }
        bom_map = {
            "MODEL-A": [
                {"part_number": "EC-80004A", "description": "Cap", "needed_qty": 1, "is_dash": 0},
                {"part_number": "EC-LATER", "description": "Later negative", "needed_qty": 1, "is_dash": 0},
                {"part_number": "EC-NEEDS", "description": "Needs supplement", "needed_qty": 1, "is_dash": 0},
            ],
        }
        dispatch_records = [
            {"part_number": "EC-80004A", "needed_qty": 320, "decision": "None"},
            {"part_number": "EC-LATER", "needed_qty": 20, "decision": "None"},
        ]
        calc_results = [
            {
                "order_id": 1,
                "shortages": [
                    {
                        "part_number": "EC-NEEDS",
                        "description": "Needs supplement",
                        "suggested_qty": 100,
                        "shortage_amount": 100,
                    },
                ],
                "customer_material_shortages": [],
            },
        ]

        with patch("app.routers.dispatch.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.dispatch._load_shortage_inputs", return_value=({"EC-80004A": 149, "EC-LATER": -10}, {}, {})), \
             patch("app.routers.dispatch.db.get_dispatch_records", return_value=dispatch_records), \
             patch("app.routers.dispatch.calc_run", return_value=calc_results) as mock_calc, \
             patch("app.routers.dispatch.db.get_order_supplements", return_value={}), \
             patch("app.routers.dispatch.db.get_decisions_for_order", return_value={}), \
             patch("app.routers.dispatch.db.get_st_inventory_stock", return_value={}), \
             patch("app.routers.dispatch.build_generated_filename", return_value="發料單測試.xlsx"), \
             patch("app.routers.dispatch.db.log_activity"):
            response = self.client.post("/api/dispatch/generate", json={
                "order_ids": [1],
                "decisions": {},
            })

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("x-dispatch-warning", {key.lower(): value for key, value in response.headers.items()})
        mock_calc.assert_called_once()
        adjusted_consumption = mock_calc.call_args.args[4]
        self.assertEqual(adjusted_consumption["EC-80004A"], -320)
        self.assertEqual(adjusted_consumption["EC-LATER"], -20)

        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = wb.active
        parts = [
            str(ws.cell(row=row_idx, column=3).value or "").strip()
            for row_idx in range(1, ws.max_row + 1)
        ]
        self.assertNotIn("EC-80004A", parts)
        self.assertNotIn("EC-LATER", parts)
        self.assertIn("EC-NEEDS", parts)
        self.assertEqual(ws.cell(row=3, column=5).value, 100)
        wb.close()

    def test_dispatch_generate_5_1_ec_80004a_current_main_stock_plus_own_dispatch_record_is_enough(self):
        orders = [
            {
                "id": 51,
                "status": "dispatched",
                "po_number": "4500059234",
                "model": "MODEL-A",
                "code": "5-1",
                "delivery_date": "2026-05-01",
                "order_qty": 320,
            },
        ]
        bom_map = {
            "MODEL-A": [
                {"part_number": "EC-80004A", "description": "Cap", "needed_qty": 1, "is_dash": 0},
            ],
        }

        with patch("app.routers.dispatch._load_shortage_inputs", return_value=({"EC-80004A": 149}, {"EC-80004A": 1}, {})), \
             patch("app.routers.dispatch.db.get_dispatch_records", return_value=[
                 {"part_number": "EC-80004A", "needed_qty": 320, "decision": "None"},
             ]), \
             patch("app.routers.dispatch.db.get_st_inventory_stock", return_value={}):
            result_by_order = _build_dispatch_result_by_order(orders, bom_map)

        self.assertEqual(result_by_order[51]["shortages"], [])
        self.assertEqual(result_by_order[51]["status"], "ok")

    def test_dispatch_generate_keeps_st_covered_rows_white_when_no_purchase_is_needed(self):
        orders = {
            1: {
                "id": 1,
                "status": "merged",
                "po_number": "4500059234",
                "model": "MODEL-A",
                "code": "1-3",
                "delivery_date": "2026-03-27",
            },
        }
        bom_map = {
            "MODEL-A": [
                {"part_number": "PART-ST", "description": "ST desc", "needed_qty": 1, "is_dash": 0},
            ],
        }
        calc_results = [
            {
                "order_id": 1,
                "shortages": [
                    {
                        "part_number": "PART-ST",
                        "description": "ST desc",
                        "suggested_qty": 8,
                        "shortage_amount": 8,
                        "st_available_qty": 8,
                        "purchase_needed_qty": 0,
                    },
                ],
                "customer_material_shortages": [],
            },
        ]

        with patch("app.routers.dispatch.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.dispatch._load_shortage_inputs", return_value=({}, {}, {})), \
             patch("app.routers.dispatch.calc_run", return_value=calc_results), \
             patch("app.routers.dispatch.db.get_order_supplements", return_value={}), \
             patch("app.routers.dispatch.db.get_decisions_for_order", return_value={}), \
             patch("app.routers.dispatch.build_generated_filename", return_value="發料單測試.xlsx"), \
             patch("app.routers.dispatch.db.log_activity"):
            response = self.client.post("/api/dispatch/generate", json={
                "order_ids": [1],
                "decisions": {"PART-ST": "CreateRequirement"},
            })

        self.assertEqual(response.status_code, 200)

        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = wb.active
        self.assertEqual(ws.cell(row=3, column=3).value, "PART-ST")
        self.assertEqual(ws.cell(row=3, column=5).value, 8)
        self.assertEqual(ws["E3"].fill.fgColor.rgb, "FFFFFFFF")
        wb.close()

    def test_dispatch_generate_marks_manual_supplement_orange_when_qty_exceeds_st_stock(self):
        orders = {
            1: {
                "id": 1,
                "status": "merged",
                "po_number": "4500059234",
                "model": "MODEL-A",
                "code": "1-3",
                "delivery_date": "2026-03-27",
            },
        }
        bom_map = {
            "MODEL-A": [
                {"part_number": "PART-MANUAL", "description": "Manual desc", "needed_qty": 1, "is_dash": 0},
            ],
        }
        calc_results = [
            {
                "order_id": 1,
                "shortages": [],
                "customer_material_shortages": [],
            },
        ]

        with patch("app.routers.dispatch.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.dispatch._load_shortage_inputs", return_value=({}, {}, {})), \
             patch("app.routers.dispatch.calc_run", return_value=calc_results), \
             patch("app.routers.dispatch.db.get_order_supplements", return_value={1: {"PART-MANUAL": 20000}}), \
             patch("app.routers.dispatch.db.get_decisions_for_order", return_value={"PART-MANUAL": "CreateRequirement"}), \
             patch("app.routers.dispatch.db.get_st_inventory_stock", return_value={"PART-MANUAL": 10000}), \
             patch("app.routers.dispatch.build_generated_filename", return_value="發料單測試.xlsx"), \
             patch("app.routers.dispatch.db.log_activity"):
            response = self.client.post("/api/dispatch/generate", json={
                "order_ids": [1],
                "decisions": {},
            })

        self.assertEqual(response.status_code, 200)

        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = wb.active
        self.assertEqual(ws.cell(row=3, column=3).value, "PART-MANUAL")
        self.assertEqual(ws.cell(row=3, column=5).value, 20000)
        self.assertEqual(ws["E3"].fill.fgColor.rgb, "FFFFC000")
        wb.close()

    def test_dispatch_generate_ignores_non_bom_decision_parts_for_selected_order(self):
        orders = {
            1: {
                "id": 1,
                "status": "merged",
                "po_number": "4500059234",
                "model": "TX2",
                "code": "2-1",
                "delivery_date": "2026-03-31",
            },
        }
        bom_map = {
            "TX2": [
                {"part_number": "IC-CSD18531Q5AT-TAB", "description": "Valid TX2 part", "needed_qty": 1, "is_dash": 0},
            ],
        }
        calc_results = [
            {
                "order_id": 1,
                "shortages": [
                    {
                        "part_number": "IC-CSD18531Q5AT-TAB",
                        "description": "Valid TX2 part",
                        "suggested_qty": 1500,
                        "shortage_amount": 1500,
                    },
                ],
                "customer_material_shortages": [],
            },
        ]

        with patch("app.routers.dispatch.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.dispatch._load_shortage_inputs", return_value=({}, {}, {})), \
             patch("app.routers.dispatch.calc_run", return_value=calc_results), \
             patch("app.routers.dispatch.db.get_order_supplements", return_value={}), \
             patch("app.routers.dispatch.db.get_decisions_for_order", return_value={
                 "IC-CSD18531Q5AT-TAB": "CreateRequirement",
                 "IC-NB675-TAB": "CreateRequirement",
             }), \
             patch("app.routers.dispatch.build_generated_filename", return_value="發料單測試.xlsx"), \
             patch("app.routers.dispatch.db.log_activity"):
            response = self.client.post("/api/dispatch/generate", json={
                "order_ids": [1],
                "decisions": {},
            })

        self.assertEqual(response.status_code, 200)

        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = wb.active
        parts = [
            str(ws.cell(row=row_idx, column=3).value or "").strip()
            for row_idx in range(1, ws.max_row + 1)
        ]
        self.assertIn("IC-CSD18531Q5AT-TAB", parts)
        self.assertNotIn("IC-NB675-TAB", parts)
        wb.close()

    def test_dispatch_generate_keeps_order_scoped_ic_parts_separate_per_model(self):
        orders = {
            1: {
                "id": 1,
                "status": "merged",
                "po_number": "4500059234",
                "model": "MODEL-A",
                "code": "1-3",
                "delivery_date": "2026-03-27",
            },
            2: {
                "id": 2,
                "status": "merged",
                "po_number": "4500059235",
                "model": "MODEL-B",
                "code": "1-4",
                "delivery_date": "2026-03-28",
            },
        }
        bom_map = {
            "MODEL-A": [
                {"part_number": "IC-STM32F", "description": "STM part", "needed_qty": 1, "is_dash": 0},
            ],
            "MODEL-B": [
                {"part_number": "IC-STM32F", "description": "STM part", "needed_qty": 1, "is_dash": 0},
            ],
        }
        calc_results = [
            {
                "order_id": 1,
                "shortages": [
                    {"part_number": "IC-STM32F", "description": "STM part", "suggested_qty": 100, "shortage_amount": 100},
                ],
                "customer_material_shortages": [],
            },
            {
                "order_id": 2,
                "shortages": [
                    {"part_number": "IC-STM32F", "description": "STM part", "suggested_qty": 50, "shortage_amount": 50},
                ],
                "customer_material_shortages": [],
            },
        ]

        with patch("app.routers.dispatch.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.dispatch._load_shortage_inputs", return_value=({}, {}, {})), \
             patch("app.routers.dispatch.calc_run", return_value=calc_results), \
             patch("app.routers.dispatch.db.get_order_supplements", return_value={}), \
             patch("app.routers.dispatch.db.get_decisions_for_order", return_value={}), \
             patch("app.routers.dispatch.db.get_st_inventory_stock", return_value={}), \
             patch("app.routers.dispatch.build_generated_filename", return_value="發料單測試.xlsx"), \
             patch("app.routers.dispatch.db.log_activity"):
            response = self.client.post("/api/dispatch/generate", json={
                "order_ids": [1, 2],
                "decisions": {"IC-STM32F": "CreateRequirement"},
            })

        self.assertEqual(response.status_code, 200)

        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = wb.active
        self.assertEqual(ws.cell(row=1, column=1).value, "1-3")
        self.assertEqual(ws.cell(row=3, column=3).value, "IC-STM32F")
        self.assertEqual(ws.cell(row=3, column=5).value, 100)
        self.assertEqual(ws.cell(row=4, column=1).value, "1-4")
        self.assertEqual(ws.cell(row=6, column=3).value, "IC-STM32F")
        self.assertEqual(ws.cell(row=6, column=5).value, 50)
        wb.close()

    def test_dispatch_generate_aggregates_saved_supplements_from_later_orders_for_normal_parts(self):
        orders = {
            1: {
                "id": 1,
                "status": "merged",
                "po_number": "4500059234",
                "model": "MODEL-A",
                "code": "2-2",
                "delivery_date": "2026-03-27",
            },
            2: {
                "id": 2,
                "status": "merged",
                "po_number": "4500059235",
                "model": "MODEL-B",
                "code": "2-3",
                "delivery_date": "2026-03-28",
            },
        }
        bom_map = {
            "MODEL-A": [
                {"part_number": "EC-20080A", "description": "Cap desc", "needed_qty": 1, "is_dash": 0},
            ],
            "MODEL-B": [
                {"part_number": "EC-20080A", "description": "Cap desc", "needed_qty": 1, "is_dash": 0},
            ],
        }
        calc_results = [
            {
                "order_id": 1,
                "shortages": [
                    {"part_number": "EC-20080A", "description": "Cap desc", "suggested_qty": 1000, "shortage_amount": 1000},
                ],
                "customer_material_shortages": [],
            },
            {
                "order_id": 2,
                "shortages": [
                    {"part_number": "EC-20080A", "description": "Cap desc", "suggested_qty": 2000, "shortage_amount": 2000},
                ],
                "customer_material_shortages": [],
            },
        ]

        with patch("app.routers.dispatch.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.dispatch._load_shortage_inputs", return_value=({}, {}, {})), \
             patch("app.routers.dispatch.calc_run", return_value=calc_results), \
             patch("app.routers.dispatch.db.get_order_supplements", return_value={1: {}, 2: {"EC-20080A": 3000}}), \
             patch("app.routers.dispatch.db.get_decisions_for_order", return_value={"EC-20080A": "CreateRequirement"}), \
             patch("app.routers.dispatch.db.get_st_inventory_stock", return_value={}), \
             patch("app.routers.dispatch.build_generated_filename", return_value="發料單測試.xlsx"), \
             patch("app.routers.dispatch.db.log_activity"):
            response = self.client.post("/api/dispatch/generate", json={
                "order_ids": [1, 2],
                "decisions": {},
            })

        self.assertEqual(response.status_code, 200)

        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = wb.active
        self.assertEqual(ws.cell(row=1, column=1).value, "2-2")
        self.assertEqual(ws.cell(row=3, column=3).value, "EC-20080A")
        self.assertEqual(ws.cell(row=3, column=5).value, 3000)
        self.assertIsNone(ws.cell(row=4, column=1).value)
        wb.close()

    def test_dispatch_generate_respects_reviewed_draft_parts_cleared_back_to_none(self):
        orders = {
            1: {
                "id": 1,
                "status": "merged",
                "po_number": "4500059319",
                "model": "OT35C",
                "code": "2-8",
                "delivery_date": "2026-05-08",
            },
        }
        bom_map = {
            "OT35C": [
                {"part_number": "EC-50004A", "description": "Diode", "needed_qty": 1, "is_dash": 0},
                {"part_number": "EC-50005A", "description": "Cap", "needed_qty": 1, "is_dash": 0},
            ],
        }
        calc_results = [
            {
                "order_id": 1,
                "shortages": [
                    {"part_number": "EC-50004A", "description": "Diode", "suggested_qty": 1800, "shortage_amount": 6},
                    {"part_number": "EC-50005A", "description": "Cap", "suggested_qty": 2000, "shortage_amount": 2000},
                ],
                "customer_material_shortages": [],
            },
        ]
        reviewed_draft = {
            "order_id": 1,
            "decisions": {"EC-50005A": "CreateRequirement"},
            "supplements": {},
            "shortages": [
                {"part_number": "EC-50004A", "description": "Diode", "suggested_qty": 1800, "shortage_amount": 6, "decision": "None"},
                {"part_number": "EC-50005A", "description": "Cap", "suggested_qty": 2000, "shortage_amount": 2000, "decision": "CreateRequirement"},
            ],
        }

        with patch("app.routers.dispatch.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.dispatch._load_shortage_inputs", return_value=({}, {}, {})), \
             patch("app.routers.dispatch.calc_run", return_value=calc_results), \
             patch("app.routers.dispatch.db.get_order_supplements", return_value={1: {}}), \
             patch("app.routers.dispatch.db.get_decisions_for_order", return_value={"EC-50005A": "CreateRequirement"}), \
             patch("app.routers.dispatch.db.get_active_merge_drafts", return_value=[reviewed_draft]), \
             patch("app.routers.dispatch.build_generated_filename", return_value="發料單測試.xlsx"), \
             patch("app.routers.dispatch.db.log_activity"):
            response = self.client.post("/api/dispatch/generate", json={
                "order_ids": [1],
                "decisions": {},
            })

        self.assertEqual(response.status_code, 200)

        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = wb.active
        parts = [
            str(ws.cell(row=row_idx, column=3).value or "").strip()
            for row_idx in range(1, ws.max_row + 1)
        ]
        self.assertNotIn("EC-50004A", parts)
        self.assertIn("EC-50005A", parts)
        wb.close()

    def test_dispatch_generate_skips_reviewed_draft_create_requirement_without_qty_source(self):
        orders = {
            1: {
                "id": 1,
                "status": "merged",
                "po_number": "4500059291",
                "model": "TA7-2",
                "code": "2-5",
                "delivery_date": "2026-06-05",
            },
        }
        bom_map = {
            "TA7-2": [
                {"part_number": "EC-10032A", "description": "Cap A", "needed_qty": 1, "is_dash": 0},
                {"part_number": "EC-20117A", "description": "Cap B", "needed_qty": 1, "is_dash": 0},
            ],
        }
        calc_results = [
            {
                "order_id": 1,
                "shortages": [
                    {"part_number": "EC-20117A", "description": "Cap B", "suggested_qty": 40000, "shortage_amount": 38829},
                ],
                "customer_material_shortages": [],
            },
        ]
        reviewed_draft = {
            "order_id": 1,
            "decisions": {
                "EC-10032A": "CreateRequirement",
                "EC-20117A": "CreateRequirement",
            },
            "supplements": {},
            "shortages": [
                {"part_number": "EC-20117A", "description": "Cap B", "suggested_qty": 40000, "shortage_amount": 38829},
            ],
        }

        with patch("app.routers.dispatch.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.routers.dispatch.db.get_order", side_effect=lambda order_id: orders.get(order_id)), \
             patch("app.routers.dispatch._load_shortage_inputs", return_value=({}, {}, {})), \
             patch("app.routers.dispatch.calc_run", return_value=calc_results), \
             patch("app.routers.dispatch.db.get_order_supplements", return_value={1: {}}), \
             patch("app.routers.dispatch.db.get_decisions_for_order", return_value=reviewed_draft["decisions"]), \
             patch("app.routers.dispatch.db.get_active_merge_drafts", return_value=[reviewed_draft]), \
             patch("app.routers.dispatch.build_generated_filename", return_value="發料單測試.xlsx"), \
             patch("app.routers.dispatch.db.log_activity"):
            response = self.client.post("/api/dispatch/generate", json={
                "order_ids": [1],
                "decisions": {},
            })

        self.assertEqual(response.status_code, 200)

        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)
        ws = wb.active
        parts = [
            str(ws.cell(row=row_idx, column=3).value or "").strip()
            for row_idx in range(1, ws.max_row + 1)
        ]
        self.assertNotIn("EC-10032A", parts)
        self.assertIn("EC-20117A", parts)
        wb.close()
