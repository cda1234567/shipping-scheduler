"""R-D1：-TAB 客供料不列入排程缺料計算結果；一鍵調撥（002 倉 -TAB）維持不變。不連線 ERP。"""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.routers import schedule as schedule_router
from app.services import st_pending_transfer as st


def _short(part, qty=10):
    return {"part_number": part, "shortage_amount": qty}


class ScheduleCalculateTabTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        tmp.close()
        self.main_path = tmp.name
        self.addCleanup(os.unlink, self.main_path)
        self.results = [
            {"order_id": 1, "status": "shortage", "shortages": [_short("EC-1"), _short("P-TAB"), _short(" p2-tab ")],
             "customer_material_shortages": []},
            {"order_id": 2, "status": "shortage", "shortages": [_short("CS-TAB")], "customer_material_shortages": []},
            {"order_id": 3, "status": "no_bom", "shortages": [], "customer_material_shortages": []},
            {"order_id": 4, "status": "ok", "shortages": [], "customer_material_shortages": []},
        ]

    def call(self):
        from main import app

        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        patches = [
            patch.object(schedule_router.db, "get_setting", lambda key: self.main_path),
            patch.object(schedule_router, "_repair_legacy_snapshot_if_needed", lambda p: {}),
            patch.object(schedule_router, "read_stock", lambda p: {}),
            patch.object(schedule_router, "read_moq", lambda p: {}),
            patch.object(schedule_router, "_get_active_dispatched_consumption", lambda: {}),
            patch.object(schedule_router, "_get_st_inventory_stock", lambda: {}),
            patch.object(schedule_router.db, "get_orders", lambda statuses: []),
            patch.object(schedule_router.db, "get_all_bom_components_by_model", lambda: {}),
            patch.object(schedule_router.db, "list_bom_substitution_rules", lambda active_only=True: []),
            patch.object(schedule_router.db, "get_order_substitution_allocations", lambda ids: {}),
            patch.object(schedule_router, "calc_run", lambda *a, **k: self.results),
        ]
        for p in patches:
            p.start()
        try:
            res = TestClient(app).get("/api/schedule/calculate")
        finally:
            for p in reversed(patches):
                p.stop()
        self.assertEqual(res.status_code, 200, res.text)
        return {r["order_id"]: r for r in res.json()["results"]}

    def test_tab_absent_and_status_recomputed(self):
        out = self.call()
        parts = [s["part_number"] for r in out.values() for s in r["shortages"]]
        self.assertFalse([p for p in parts if p.strip().upper().endswith("-TAB")])
        self.assertEqual([s["part_number"] for s in out[1]["shortages"]], ["EC-1"])
        self.assertEqual(out[1]["status"], "shortage")
        self.assertEqual((out[2]["status"], out[2]["shortages"]), ("ok", []))  # 只缺 -TAB → 不算缺料
        self.assertEqual(out[3]["status"], "no_bom")
        self.assertEqual(out[4]["status"], "ok")


class TransferPreviewKeepsTabTests(unittest.TestCase):
    def test_transfer_allocation_still_includes_tab_from_002(self):
        batches = [dict(key="A", deductions=[("P-TAB", 500), ("P", 20)])]
        out = st.allocate_batches(batches, {}, [], {})
        self.assertIn({"part": "P-TAB", "pending": 500, "ware_out": "002"}, out[0]["rows"])
        self.assertIn({"part": "P", "pending": 20, "ware_out": "001"}, out[0]["rows"])


if __name__ == "__main__":
    unittest.main()
