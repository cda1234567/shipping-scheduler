import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.services import batch_rollback
from app.services.main_event_rollback import PreciseRollbackUnavailable

ORDERS = {
    1: {"id": 1, "po_number": "PO-1", "model": "M1", "status": "dispatched"},
    2: {"id": 2, "po_number": "PO-2", "model": "M2", "status": "dispatched"},
    3: {"id": 3, "po_number": "PO-3", "model": "M3", "status": "completed"},
    4: {"id": 4, "po_number": "PO-4", "model": "M4", "status": "pending"},
}
SESSIONS = {
    1: {"id": 11, "order_id": 1, "dispatched_at": "2026-11-01T08:00:00", "main_file_path": ""},
    2: {"id": 12, "order_id": 2, "dispatched_at": "2026-11-02T08:00:00", "main_file_path": ""},
    3: {"id": 13, "order_id": 3, "dispatched_at": "2026-11-03T08:00:00", "main_file_path": ""},
}


class FakeDb:
    def __init__(self):
        self.active = dict(SESSIONS)

    def get_order(self, oid):
        return ORDERS.get(oid)

    def get_active_dispatch_session(self, oid):
        return self.active.get(oid)


class BatchRollbackServiceTests(unittest.TestCase):
    def setUp(self):
        self.db = FakeDb()
        p = patch.object(batch_rollback, "db", self.db)
        p.start()
        self.addCleanup(p.stop)

    def _rollback(self, calls, fail_on=None, exc=None):
        def run(session):
            if session["order_id"] == fail_on:
                raise exc
            calls.append(session["order_id"])
            self.db.active.pop(session["order_id"], None)
            return {"ok": True, "main_backup_path": f"bk{session['order_id']}"}
        return run

    def test_plan_orders_newest_first_and_marks_full(self):
        evaluator = lambda s: {"ok": s["order_id"] != 2, "reason": "後面有盤點覆蓋"}
        plan = batch_rollback.plan([1, 2, 3], evaluator=evaluator)
        self.assertEqual([o["order_id"] for o in plan["orders"]], [3, 2, 1])
        self.assertEqual(plan["precise_count"], 2)
        self.assertTrue(plan["needs_full_restore"])
        self.assertEqual(plan["orders"][1]["mode"], "full")
        self.assertIn("盤點", plan["orders"][1]["reason"])

    def test_plan_rejects_non_dispatched_whole_batch(self):
        with self.assertRaises(batch_rollback.BatchRollbackError) as ctx:
            batch_rollback.plan([1, 4], evaluator=lambda s: {"ok": True})
        self.assertIn("整批未執行", ctx.exception.message)

    def test_execute_requires_confirm(self):
        calls = []
        with self.assertRaises(batch_rollback.BatchRollbackError):
            batch_rollback.execute([1], confirm=False, rollback_one=self._rollback(calls),
                                   evaluator=lambda s: {"ok": True})
        self.assertEqual(calls, [])

    def test_execute_all_precise_newest_first(self):
        calls = []
        after = []
        result = batch_rollback.execute([1, 3, 2], confirm=True, rollback_one=self._rollback(calls),
                                        after_each=lambda o, r: after.append(o["id"]),
                                        evaluator=lambda s: {"ok": True})
        self.assertTrue(result["ok"])
        self.assertEqual(calls, [3, 2, 1])
        self.assertEqual(after, [3, 2, 1])
        self.assertEqual(result["not_rolled_back"], [])

    def test_full_restore_needed_stops_before_anything(self):
        calls = []
        result = batch_rollback.execute([1, 2, 3], confirm=True, rollback_one=self._rollback(calls),
                                        evaluator=lambda s: {"ok": s["order_id"] != 1, "reason": "x"})
        self.assertFalse(result["executed"])
        self.assertTrue(result["stopped"])
        self.assertEqual(calls, [])

    def test_full_restore_acknowledged_skips_those_orders(self):
        calls = []
        result = batch_rollback.execute([1, 2, 3], confirm=True, acknowledge_full=True,
                                        rollback_one=self._rollback(calls),
                                        evaluator=lambda s: {"ok": s["order_id"] != 2, "reason": "x"})
        self.assertEqual(calls, [3, 1])
        self.assertEqual([i["order_id"] for i in result["skipped_full"]], [2])
        self.assertEqual([i["order_id"] for i in result["not_rolled_back"]], [])
        self.assertTrue(result["ok"])

    def test_midway_failure_stops_rest(self):
        for exc in (PreciseRollbackUnavailable("主檔欄位比對不符"), PermissionError("locked"), OSError("無法讀取主檔")):
            self.db.active = dict(SESSIONS)
            calls = []
            result = batch_rollback.execute([1, 2, 3], confirm=True, rollback_one=self._rollback(calls, 2, exc),
                                            evaluator=lambda s: {"ok": True})
            self.assertFalse(result["ok"])
            self.assertEqual(calls, [3])
            self.assertEqual([i["order_id"] for i in result["rolled_back"]], [3])
            self.assertEqual([i["order_id"] for i in result["not_rolled_back"]], [2, 1])
            self.assertEqual(result["failure"]["order_id"], 2)

    def test_session_changed_concurrently_stops(self):
        calls = []

        def run(session):
            calls.append(session["order_id"])
            self.db.active.pop(session["order_id"], None)
            self.db.active[2] = {**SESSIONS[2], "id": 99}  # 有人同時重新發料
            return {"ok": True}
        result = batch_rollback.execute([1, 2, 3], confirm=True, rollback_one=run, evaluator=lambda s: {"ok": True})
        self.assertEqual(calls, [3])
        self.assertIn("變動", result["failure"]["error"])

    def test_main_file_written_by_other_stops(self):
        import os, tempfile, time
        fd, path = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(os.remove, path)
        for oid in (1, 2, 3):
            self.db.active[oid] = {**SESSIONS[oid], "main_file_path": path}
        calls = []

        def run(session):
            calls.append(session["order_id"])
            self.db.active.pop(session["order_id"], None)
            return {"ok": True, "main_file_path": path}

        orig_exec = run
        def run_then_touch(session):
            r = orig_exec(session)
            def touch():
                st = os.stat(path)
                os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))
            self._touch = touch
            return r
        after = lambda o, r: self._touch()
        # after_each 在記錄 mtime 之後執行 → 模擬其他人寫入
        result = batch_rollback.execute([1, 2, 3], confirm=True, rollback_one=run_then_touch,
                                        after_each=after, evaluator=lambda s: {"ok": True})
        self.assertEqual(calls, [3])
        self.assertIn("其他人寫入", result["failure"]["error"])


class BatchRollbackApiTests(unittest.TestCase):
    def setUp(self):
        import main
        self.client = TestClient(main.app)
        p = patch("app.routers.schedule.batch_rollback.db", FakeDb())
        p.start()
        self.addCleanup(p.stop)
        self.edit = patch("app.services.edit_auth.request_requires_edit_auth", return_value=False)

    def test_plan_endpoint_read_only(self):
        with patch("app.services.batch_rollback.evaluate_precise_rollback", return_value={"ok": True}), \
             patch("app.routers.schedule._precise_rollback_dispatch_session") as do_rb:
            resp = self.client.post("/api/schedule/rollback-batch/plan", json={"order_ids": [1, 3]})
        if resp.status_code in (401, 403):
            self.skipTest("edit auth required in this env")
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual([o["order_id"] for o in resp.json()["orders"]], [3, 1])
        do_rb.assert_not_called()

    def test_execute_endpoint_uses_precise_rollback(self):
        fake = FakeDb()
        with patch("app.routers.schedule.batch_rollback.db", fake), \
             patch("app.services.batch_rollback.evaluate_precise_rollback", return_value={"ok": True}), \
             patch("app.routers.schedule._precise_rollback_dispatch_session", return_value={"ok": True, "precise": {}}) as do_rb, \
             patch("app.routers.schedule.db.log_activity"), \
             patch("app.routers.schedule.db.get_active_merge_drafts", return_value=[]):
            resp = self.client.post("/api/schedule/rollback-batch/execute",
                                    json={"order_ids": [1, 2], "confirm": True})
        if resp.status_code in (401, 403):
            self.skipTest("edit auth required in this env")
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual([c.args[0]["order_id"] for c in do_rb.call_args_list], [2, 1])
        self.assertTrue(resp.json()["ok"])

    def test_execute_without_confirm_400(self):
        with patch("app.routers.schedule._precise_rollback_dispatch_session") as do_rb:
            resp = self.client.post("/api/schedule/rollback-batch/execute", json={"order_ids": [1]})
        if resp.status_code in (401, 403):
            self.skipTest("edit auth required in this env")
        self.assertEqual(resp.status_code, 400)
        do_rb.assert_not_called()


class BatchRollbackFrontendTests(unittest.TestCase):
    def test_button_and_handler_present(self):
        from pathlib import Path
        root = Path(__file__).resolve().parents[1]
        html = (root / "static/index.html").read_text(encoding="utf-8")
        js = (root / "static/modules/schedule.js").read_text(encoding="utf-8")
        self.assertIn('id="btn-completed-rollback-selected"', html)
        self.assertIn("退回選取的單", html)
        self.assertIn("/api/schedule/rollback-batch/plan", js)
        self.assertIn("/api/schedule/rollback-batch/execute", js)
        self.assertIn("需整檔還原", js)
        self.assertIn("acknowledge_full", js)


if __name__ == "__main__":
    unittest.main()
