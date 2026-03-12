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


if __name__ == "__main__":
    unittest.main()
