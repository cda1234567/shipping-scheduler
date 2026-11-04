"""ITEM 3（v2026.11.04.7）：批次扣帳前 BOM 預檢——任何一筆沒有網站 BOM 就整批不扣帳，並列出缺 BOM 的訂單 / 機種。"""
from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient

import app.routers.schedule as schedule_router
from main import app

ORDERS = {
    1: {"id": 1, "po_number": "PO-A", "model": "MODEL-A", "code": "A1", "status": "merged"},
    2: {"id": 2, "po_number": "PO-B", "model": "NOBOM", "code": "B1", "status": "merged"},
    3: {"id": 3, "po_number": "", "model": "EMPTY", "code": "C1", "status": "pending"},
}
BOM_FILES = {"MODEL-A": [{"id": 10, "model": "MODEL-A"}], "EMPTY": [{"id": 30, "model": "EMPTY"}]}
COMPONENTS = {10: [{"part_number": "P1"}], 30: []}


def patches():
    return [
        patch.object(schedule_router.db, "get_order", lambda oid: ORDERS.get(int(oid))),
        patch.object(schedule_router.db, "get_bom_files_by_models", lambda models: BOM_FILES.get(models[0], [])),
        patch.object(schedule_router.db, "get_bom_components", lambda fid: COMPONENTS.get(fid, [])),
    ]


class BomPrecheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        cls.client = TestClient(app)

    def setUp(self):
        for p in patches():
            p.start()
            self.addCleanup(p.stop)

    def test_lists_orders_without_bom_or_components(self):
        missing = schedule_router._orders_missing_bom([1, 2, 3, 99])
        self.assertEqual([m["order_id"] for m in missing], [2, 3])
        self.assertEqual(missing[0]["model"], "NOBOM")

    def test_all_have_bom_passes(self):
        schedule_router._require_bom_for_all_orders([1])

    def test_message_lists_every_missing_order(self):
        with self.assertRaises(HTTPException) as cm:
            schedule_router._require_bom_for_all_orders([1, 2, 3])
        detail = cm.exception.detail
        self.assertEqual(cm.exception.status_code, 400)
        for needle in ("2 筆", "PO-B（機種 NOBOM）", "C1（機種 EMPTY）", "全部沒有扣帳"):
            self.assertIn(needle, detail)

    def test_batch_dispatch_deducts_nothing_when_any_missing(self):
        with patch.object(schedule_router, "_resolve_batch_dispatch_plan") as plan, \
                patch.object(schedule_router, "commit_dispatch_plan") as commit:
            r = self.client.post("/api/schedule/batch-dispatch", json={"order_ids": [1, 2]})
        self.assertEqual(r.status_code, 400)
        self.assertIn("PO-B", r.json()["detail"])
        plan.assert_not_called()
        commit.assert_not_called()

    def test_update_and_commit_drafts_refuses_before_starting_job(self):
        with patch.object(schedule_router, "_start_update_and_commit_drafts_job") as start:
            r = self.client.post("/api/schedule/update-and-commit-drafts", json={"order_ids": [3, 1]})
        self.assertEqual(r.status_code, 400)
        self.assertIn("C1（機種 EMPTY）", r.json()["detail"])
        start.assert_not_called()

    def test_commit_job_also_checks(self):
        with patch.object(schedule_router, "_apply_batch_draft_updates") as apply:
            with self.assertRaises(HTTPException):
                schedule_router._run_update_and_commit_drafts_job("job", {"order_ids": [1, 2]})
        apply.assert_not_called()


if __name__ == "__main__":
    unittest.main()
