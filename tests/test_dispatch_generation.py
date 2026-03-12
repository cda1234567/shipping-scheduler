from __future__ import annotations

import io
import unittest
from unittest.mock import patch

import openpyxl
from fastapi.testclient import TestClient

from main import app


class DispatchGenerationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

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
                    {"part_number": "PART-SAVED", "description": "Saved desc", "suggested_qty": 999, "shortage_amount": 999},
                    {"part_number": "PART-MISSING", "description": "Missing desc", "suggested_qty": 888, "shortage_amount": 888},
                ],
                "customer_material_shortages": [],
            },
            {
                "order_id": 2,
                "shortages": [
                    {"part_number": "PART-PENDING", "description": "Pending desc", "suggested_qty": 2400, "shortage_amount": 2400},
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
        self.assertEqual(ws.cell(row=2, column=1).value, "4500059234")
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
        wb.close()
