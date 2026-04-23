from __future__ import annotations

import unittest
from unittest.mock import patch

from app.services.order_supplements import build_order_supplement_allocations, merge_order_supplement_allocations


class OrderSupplementAllocationTests(unittest.TestCase):
    def test_build_order_supplement_allocations_splits_order_scoped_ic_parts_by_current_shortage(self):
        orders = {
            1: {"id": 1, "model": "MODEL-A"},
            2: {"id": 2, "model": "MODEL-B"},
        }
        bom_map = {
            "MODEL-A": [
                {"part_number": "IC-STM32F", "needed_qty": 100, "prev_qty_cs": 0, "is_dash": 0},
            ],
            "MODEL-B": [
                {"part_number": "IC-STM32F", "needed_qty": 50, "prev_qty_cs": 0, "is_dash": 0},
            ],
        }

        with patch("app.services.order_supplements.db.get_all_bom_components_by_model", return_value=bom_map), \
             patch("app.services.order_supplements.build_dispatch_running_stock", return_value={"IC-STM32F": 0.0}), \
             patch("app.services.order_supplements.db.get_order", side_effect=lambda order_id: orders.get(order_id)):
            allocations = build_order_supplement_allocations([1, 2], {"IC-STM32F": 150})

        self.assertEqual(allocations, {
            1: {"IC-STM32F": 100.0},
            2: {"IC-STM32F": 50.0},
        })

    def test_merge_order_supplement_allocations_uses_order_values_as_source_of_truth(self):
        def allocator(order_ids, supplements):
            self.assertEqual(order_ids, [1, 2])
            self.assertEqual(supplements, {"EC-30059A": 1500})
            return {
                1: {"EC-30059A": 750.0},
                2: {"EC-30059A": 750.0},
            }

        allocations = merge_order_supplement_allocations(
            [1, 2],
            {"EC-30059A": 1500},
            {
                1: {"EC-30059A": 1500},
                2: {},
            },
            allocator=allocator,
        )

        self.assertEqual(allocations, {
            1: {"EC-30059A": 1500.0},
            2: {},
        })

    def test_merge_order_supplement_allocations_keeps_global_fallback_for_legacy_callers(self):
        def allocator(order_ids, supplements):
            return {
                1: {"EC-30059A": 1500.0},
                2: {},
            }

        allocations = merge_order_supplement_allocations(
            [1, 2],
            {"EC-30059A": 1500},
            None,
            allocator=allocator,
        )

        self.assertEqual(allocations, {
            1: {"EC-30059A": 1500.0},
            2: {},
        })
