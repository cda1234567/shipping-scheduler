from __future__ import annotations

import unittest

from app.services.material_risk import build_frequent_zero_stock_analysis


class MaterialRiskTests(unittest.TestCase):
    def test_lists_only_frequent_parts_with_no_main_or_st_available_stock(self):
        result = build_frequent_zero_stock_analysis(
            history_usage=[
                {
                    "part_number": "IC-ZERO",
                    "order_count": 5,
                    "total_qty": 500,
                    "last_used_at": "2026-07-20T10:00:00",
                },
                {
                    "part_number": "OC-STOCK",
                    "order_count": 10,
                    "total_qty": 1000,
                    "last_used_at": "2026-07-22T10:00:00",
                },
                {
                    "part_number": "UC-LOW-FREQUENCY",
                    "order_count": 2,
                    "total_qty": 20,
                    "last_used_at": "2026-07-23T10:00:00",
                },
                {
                    "part_number": "EC-10001A",
                    "order_count": 20,
                    "total_qty": 3000,
                    "last_used_at": "2026-07-24T10:00:00",
                },
                {
                    "part_number": "PART-WATCH",
                    "order_count": 3,
                    "total_qty": 90,
                    "last_used_at": "2026-07-10T10:00:00",
                },
            ],
            active_orders=[
                {
                    "id": 1,
                    "code": "7-1",
                    "po_number": "PO-1",
                    "model": "MODEL-A",
                    "order_qty": 100,
                    "ship_date": "2026-08-05",
                },
                {
                    "id": 2,
                    "model": "NO-BOM",
                    "order_qty": 50,
                },
            ],
            bom_map={
                "MODEL-A": [
                    {
                        "part_number": "IC-ZERO",
                        "description": "常用 IC",
                        "qty_per_board": 1,
                        "scrap_factor": 0,
                        "needed_qty": 0,
                        "is_dash": False,
                    },
                    {
                        "part_number": "IC-ZERO",
                        "description": "常用 IC",
                        "qty_per_board": 0.5,
                        "scrap_factor": 0,
                        "needed_qty": 0,
                        "is_dash": False,
                    },
                ],
            },
            main_snapshot={
                "IC-ZERO": {"stock_qty": -20, "description": ""},
                "OC-STOCK": {"stock_qty": 0, "description": ""},
                "PART-WATCH": {"stock_qty": 0, "description": "觀察料"},
            },
            st_snapshot={
                "IC-ZERO": {"stock_qty": 0, "description": ""},
                "OC-STOCK": {"stock_qty": 5, "description": ""},
                "PART-WATCH": {"stock_qty": 0, "description": ""},
            },
            vendors={"IC-ZERO": "Vendor A"},
            history_months=6,
            min_order_count=3,
        )

        self.assertEqual(
            [item["part_number"] for item in result["items"]],
            ["IC-ZERO", "PART-WATCH"],
        )
        urgent = result["items"][0]
        self.assertEqual(urgent["priority"], "urgent")
        self.assertEqual(urgent["main_stock_qty"], -20)
        self.assertEqual(urgent["st_stock_qty"], 0)
        self.assertEqual(urgent["total_available_qty"], 0)
        self.assertEqual(urgent["active_order_count"], 1)
        self.assertEqual(urgent["active_demand_qty"], 150)
        self.assertEqual(urgent["description"], "常用 IC")
        self.assertEqual(urgent["vendor"], "Vendor A")
        self.assertEqual(result["summary"]["common_part_count"], 3)
        self.assertEqual(result["summary"]["zero_stock_count"], 2)
        self.assertEqual(result["summary"]["urgent_count"], 1)
        self.assertEqual(result["summary"]["watch_count"], 1)
        self.assertEqual(result["summary"]["missing_bom_orders"], 1)


if __name__ == "__main__":
    unittest.main()
