from __future__ import annotations

import unittest

from app.services.calculator import run


class CalculatorTests(unittest.TestCase):
    def test_reports_shortage_when_opening_balance_is_already_negative(self):
        results = run(
            orders=[{"id": 1, "po_number": 123, "pcb": "A", "model": "MODEL-A"}],
            bom_map={
                "MODEL-A": [
                    {
                        "part_number": "PART-1",
                        "description": "Capacitor",
                        "needed_qty": 3,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
            },
            snapshot_stock={"PART-1": 10},
            moq={"PART-1": 5},
            dispatched_consumption={"PART-1": 12},
        )

        self.assertEqual(results[0]["status"], "shortage")
        self.assertEqual(len(results[0]["shortages"]), 1)
        self.assertEqual(results[0]["shortages"][0]["part_number"], "PART-1")
        self.assertEqual(results[0]["shortages"][0]["current_stock"], -2)
        self.assertEqual(results[0]["shortages"][0]["needed"], 3)
        self.assertEqual(results[0]["shortages"][0]["shortage_amount"], 5)
        self.assertEqual(results[0]["shortages"][0]["suggested_qty"], 5)

    def test_aggregates_duplicate_parts_within_same_order(self):
        results = run(
            orders=[{"id": 1, "po_number": 456, "pcb": "B", "model": "MODEL-B"}],
            bom_map={
                "MODEL-B": [
                    {
                        "part_number": "PART-2",
                        "description": "Resistor",
                        "needed_qty": 8,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                    {
                        "part_number": "PART-2",
                        "description": "Resistor",
                        "needed_qty": 5,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
            },
            snapshot_stock={"PART-2": 10},
            moq={"PART-2": 4},
        )

        self.assertEqual(results[0]["status"], "shortage")
        self.assertEqual(len(results[0]["shortages"]), 1)
        self.assertEqual(results[0]["shortages"][0]["part_number"], "PART-2")
        self.assertEqual(results[0]["shortages"][0]["current_stock"], 10)
        self.assertEqual(results[0]["shortages"][0]["needed"], 13)
        self.assertEqual(results[0]["shortages"][0]["shortage_amount"], 3)
        self.assertEqual(results[0]["shortages"][0]["suggested_qty"], 4)

    def test_customer_supplied_flag_is_ignored_and_uses_normal_shortage_list(self):
        results = run(
            orders=[{"id": 1, "po_number": 789, "pcb": "C", "model": "MODEL-C"}],
            bom_map={
                "MODEL-C": [
                    {
                        "part_number": "PART-3",
                        "description": "Connector",
                        "needed_qty": 6,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": True,
                    },
                ],
            },
            snapshot_stock={"PART-3": 2},
            moq={"PART-3": 0},
        )

        self.assertEqual(results[0]["status"], "shortage")
        self.assertEqual(len(results[0]["shortages"]), 1)
        self.assertEqual(results[0]["shortages"][0]["part_number"], "PART-3")
        self.assertEqual(results[0]["customer_material_shortages"], [])

    def test_ec_part_below_100_is_treated_as_shortage(self):
        results = run(
            orders=[{"id": 1, "po_number": 999, "pcb": "D", "model": "MODEL-D"}],
            bom_map={
                "MODEL-D": [
                    {
                        "part_number": "EC-001",
                        "description": "EC Part",
                        "needed_qty": 30,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
            },
            snapshot_stock={"EC-001": 120},
            moq={"EC-001": 5},
        )

        self.assertEqual(results[0]["status"], "shortage")
        self.assertEqual(len(results[0]["shortages"]), 1)
        self.assertEqual(results[0]["shortages"][0]["part_number"], "EC-001")
        self.assertEqual(results[0]["shortages"][0]["current_stock"], 120)
        self.assertEqual(results[0]["shortages"][0]["needed"], 30)
        self.assertEqual(results[0]["shortages"][0]["shortage_amount"], 10)
        self.assertEqual(results[0]["shortages"][0]["suggested_qty"], 10)

    def test_ec_6_part_does_not_trigger_ec_low_stock_warning(self):
        results = run(
            orders=[{"id": 1, "po_number": 998, "pcb": "D", "model": "MODEL-D6"}],
            bom_map={
                "MODEL-D6": [
                    {
                        "part_number": "EC-60001A",
                        "description": "EC-6 part",
                        "needed_qty": 30,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
            },
            snapshot_stock={"EC-60001A": 120},
            moq={"EC-60001A": 5},
        )

        self.assertEqual(results[0]["status"], "ok")
        self.assertEqual(results[0]["shortages"], [])

    def test_st_stock_reduces_purchase_needed_without_losing_total_supply_suggestion(self):
        results = run(
            orders=[{"id": 1, "po_number": 1001, "pcb": "E", "model": "MODEL-E"}],
            bom_map={
                "MODEL-E": [
                    {
                        "part_number": "PART-4",
                        "description": "ST assisted part",
                        "needed_qty": 10,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
            },
            snapshot_stock={"PART-4": 2},
            moq={"PART-4": 5},
            st_inventory_stock={"PART-4": 6},
        )

        shortage = results[0]["shortages"][0]
        self.assertEqual(shortage["shortage_amount"], 8)
        self.assertEqual(shortage["st_stock_qty"], 6)
        self.assertEqual(shortage["st_available_qty"], 6)
        self.assertEqual(shortage["purchase_needed_qty"], 2)
        self.assertEqual(shortage["purchase_suggested_qty"], 5)
        self.assertTrue(shortage["needs_purchase"])
        self.assertEqual(shortage["suggested_qty"], 11)

    def test_order_scoped_ic_parts_report_only_current_order_shortage(self):
        results = run(
            orders=[
                {"id": 1, "po_number": 2001, "pcb": "F", "model": "MODEL-F1"},
                {"id": 2, "po_number": 2002, "pcb": "G", "model": "MODEL-F2"},
            ],
            bom_map={
                "MODEL-F1": [
                    {
                        "part_number": "IC-STM32F",
                        "description": "STM MCU",
                        "needed_qty": 100,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
                "MODEL-F2": [
                    {
                        "part_number": "IC-STM32F",
                        "description": "STM MCU",
                        "needed_qty": 50,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
            },
            snapshot_stock={"IC-STM32F": 0},
            moq={"IC-STM32F": 100},
            st_inventory_stock={"IC-STM32F": 80},
        )

        first_shortage = results[0]["shortages"][0]
        second_shortage = results[1]["shortages"][0]
        self.assertEqual(first_shortage["shortage_amount"], 100)
        self.assertEqual(first_shortage["suggested_qty"], 100)
        self.assertEqual(first_shortage["purchase_suggested_qty"], 20)
        self.assertEqual(second_shortage["shortage_amount"], 50)
        self.assertEqual(second_shortage["suggested_qty"], 50)
        self.assertEqual(second_shortage["purchase_suggested_qty"], 0)

    def test_m24_parts_report_only_current_order_shortage(self):
        results = run(
            orders=[
                {"id": 1, "po_number": 2101, "pcb": "M", "model": "MODEL-M1"},
                {"id": 2, "po_number": 2102, "pcb": "N", "model": "MODEL-M2"},
            ],
            bom_map={
                "MODEL-M1": [
                    {
                        "part_number": "IC-M24C02-WMN6TP-TAB",
                        "description": "EEPROM",
                        "needed_qty": 100,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
                "MODEL-M2": [
                    {
                        "part_number": "IC-M24C02-WMN6TP-TAB",
                        "description": "EEPROM",
                        "needed_qty": 50,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
            },
            snapshot_stock={"IC-M24C02-WMN6TP-TAB": 0},
            moq={"IC-M24C02-WMN6TP-TAB": 100},
        )

        first_shortage = results[0]["shortages"][0]
        second_shortage = results[1]["shortages"][0]
        self.assertEqual(first_shortage["shortage_amount"], 100)
        self.assertEqual(second_shortage["current_stock"], -100)
        self.assertEqual(second_shortage["shortage_amount"], 50)

    def test_scales_component_need_by_schedule_order_qty(self):
        results = run(
            orders=[{"id": 1, "po_number": 3001, "pcb": "H", "model": "MODEL-H", "order_qty": 5}],
            bom_map={
                "MODEL-H": [
                    {
                        "part_number": "PART-SCALED",
                        "description": "Scaled part",
                        "qty_per_board": 2,
                        "bom_order_qty": 10,
                        "needed_qty": 20,
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
            },
            snapshot_stock={"PART-SCALED": 8},
            moq={"PART-SCALED": 0},
        )

        self.assertEqual(results[0]["status"], "shortage")
        self.assertEqual(results[0]["shortages"][0]["needed"], 10)
        self.assertEqual(results[0]["shortages"][0]["shortage_amount"], 2)

    def test_scales_component_need_with_bom_scrap_rate_preserved(self):
        # qty_per_board × schedule × (1 + scrap_factor)
        # 即使 needed_qty 在 BOM 裡存錯值，也用即時公式算，不再讀 F 欄
        results = run(
            orders=[{"id": 1, "po_number": 3002, "pcb": "H", "model": "MODEL-H-SCRAP", "order_qty": 5}],
            bom_map={
                "MODEL-H-SCRAP": [
                    {
                        "part_number": "PART-SCRAP",
                        "description": "Scaled with scrap",
                        "qty_per_board": 2,
                        "scrap_factor": 0.06,
                        "bom_order_qty": 10,
                        "needed_qty": 99999,  # 故意寫錯，驗證系統會忽略 F 欄
                        "prev_qty_cs": 0,
                        "is_dash": False,
                        "is_customer_supplied": False,
                    },
                ],
            },
            snapshot_stock={"PART-SCRAP": 8},
            moq={"PART-SCRAP": 0},
        )

        self.assertEqual(results[0]["status"], "shortage")
        self.assertAlmostEqual(results[0]["shortages"][0]["needed"], 10.6)
        self.assertAlmostEqual(results[0]["shortages"][0]["shortage_amount"], 2.6)


if __name__ == "__main__":
    unittest.main()
