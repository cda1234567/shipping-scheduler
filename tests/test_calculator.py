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


if __name__ == "__main__":
    unittest.main()
