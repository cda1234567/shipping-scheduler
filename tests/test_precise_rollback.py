from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from openpyxl import Workbook, load_workbook

from app.services import dispatch_precise_rollback as precise
from app.services.defective_deduction import (
    deduct_defectives_from_main,
    remove_batch_deduction_from_main,
)
from app.services.dispatch_precise_rollback import (
    build_dispatch_column_record,
    ensure_precise_preconditions,
    plan_and_apply,
)
from app.services.main_event_rollback import PreciseRollbackUnavailable
from app.services.main_reader import read_stock

ORDER = {"id": 11, "code": "1-1", "po_number": "PO-A", "model": "M-A", "insert_before_code": ""}
SESSION = {"id": 21, "order_id": 11, "dispatched_at": "2026-11-01T09:00:00", "backup_path": ""}


def _base_rows():
    # 欄：1 料號 2 廠商 3 MOQ 4 備註 8 盤點 | 9-11 1-1 | 12-14 1-2
    return [
        ["料號", "廠商", "MOQ", "備註", None, None, None, "盤點", "1-1", "PO-A", "M-A", "1-2", "PO-B", "M-B"],
        ["P1", "", 1, "", None, None, None, 100, 0, 30, 70, 0, 10, 60],
        ["P2", "", 1, "", None, None, None, 50, 10, 20, 40, None, None, None],
        ["P3", None, 1, None, None, None, None, 9, None, None, None, 5, 0, 14],
    ]


def _record(groups):
    return {"version": 1, "groups": groups, "insert_before_code": "", "code_shifts": False}


GROUP_11 = {
    "col_h": 9, "col_f": 10, "col_j": 11, "batch_code": "1-1", "po_number": "PO-A", "bom_model": "M-A",
    "rows": [
        {"row": 2, "part": "P1", "h": 0, "f": 30, "j": 70},
        {"row": 3, "part": "P2", "h": 10, "f": 20, "j": 40},
    ],
}


class PreciseRollbackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "main.xlsx"
        self.logs: list[dict] = []
        self.record = _record([GROUP_11])
        self.records: list[dict] = []
        for target, attr, value in (
            (precise.db, "get_dispatch_column_record", lambda session_id: self.record),
            (precise.db, "get_dispatch_records", lambda order_id: self.records),
            (precise.db, "get_activity_logs_after", lambda *a, **k: list(self.logs)),
        ):
            patcher = patch.object(target, attr, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def save(self, rows, extra=None):
        wb = Workbook()
        ws = wb.active
        for row in rows:
            ws.append(row)
        if extra:
            extra(ws)
        wb.save(self.path)
        wb.close()

    def apply(self):
        wb = load_workbook(self.path)
        try:
            summary = plan_and_apply(wb.active, SESSION, ORDER)
            wb.save(self.path)
        finally:
            wb.close()
        return summary

    def cells(self, row):
        wb = load_workbook(self.path)
        try:
            return [cell.value for cell in wb.active[row]]
        finally:
            wb.close()

    def test_mid_sheet_order_is_zeroed_and_later_balances_shift(self):
        self.save(_base_rows())
        summary = self.apply()
        self.assertEqual(summary["mode"], "zeroed")
        self.assertEqual(summary["part_deltas"], {"P1": 30.0, "P2": 10.0})
        header = self.cells(1)
        self.assertEqual(header[8], "已退回 1-1")
        self.assertEqual(self.cells(2)[8:14], [0, 0, None, 0, 10, 90])
        self.assertEqual(self.cells(3)[8:11], [0, 0, None])
        self.assertEqual(self.cells(4), ["P3", None, 1, None, None, None, None, 9, None, None, None, 5, 0, 14])
        stock = read_stock(str(self.path))
        self.assertEqual((stock["P1"], stock["P2"], stock["P3"]), (90, 50, 14))
        wb = load_workbook(self.path)
        try:
            self.assertIn("dispatch-precise-rollback:", wb.active.cell(1, 9).comment.text)
        finally:
            wb.close()

    def test_trailing_order_columns_are_deleted(self):
        rows = [row[:11] for row in _base_rows()]
        self.save(rows)
        summary = self.apply()
        self.assertEqual(summary["mode"], "removed")
        wb = load_workbook(self.path)
        try:
            self.assertEqual(wb.active.max_column, 8)
        finally:
            wb.close()
        stock = read_stock(str(self.path))
        self.assertEqual((stock["P1"], stock["P2"]), (100, 50))

    def test_later_manual_edit_defective_block_and_receipts_survive(self):
        rows = _base_rows()
        rows[0] += ["不良品扣帳", "使用數量", "11/04 10:00"]
        rows[1] += [None, 5, 55]
        rows[2] += [None, None, None]
        rows[3] += [None, None, None]
        rows[1][3] = "手動備註"
        self.save(rows)
        self.logs = [{"action": "主檔編輯", "detail": "[Sheet] R2C4:  → 手動備註（料號 P1）"}]
        self.apply()
        self.assertEqual(self.cells(2)[3], "手動備註")
        self.assertEqual(self.cells(2)[11:17], [0, 10, 90, None, 5, 85])
        self.assertEqual(self.cells(1)[14], "不良品扣帳")
        self.assertEqual(read_stock(str(self.path))["P1"], 85)

    def test_reconcile_after_dispatch_keeps_counted_balance(self):
        rows = _base_rows()
        rows[0] += ["盤點調整 11/02", "使用數量", "盤點結存", "1-3", "PO-C", "M-C"]
        rows[1] += [None, 12, 48, 0, 8, 40]
        rows[2] += [None, None, None, None, None, None]
        rows[3] += [None, None, None, None, None, None]
        self.save(rows)
        summary = self.apply()
        self.assertEqual(summary["anchored_parts"], ["P1"])
        p1 = self.cells(2)
        self.assertEqual(p1[13], 90)          # 1-2 結餘 +30
        self.assertEqual(p1[14:17], [None, 42, 48])  # 盤點數不變，增減量調整
        self.assertEqual(p1[17:20], [0, 8, 40])
        self.assertEqual(read_stock(str(self.path))["P1"], 40)

    def test_manually_overridden_later_balance_falls_back(self):
        rows = _base_rows()
        rows[1][13] = 75  # 70 - 10 應為 60
        self.save(rows)
        with self.assertRaises(PreciseRollbackUnavailable):
            self.apply()

    def test_rounding_drift_of_one_is_tolerated(self):
        rows = _base_rows()
        rows[1][13] = 61
        self.save(rows)
        self.apply()
        self.assertEqual(self.cells(2)[13], 91)

    def test_edited_order_columns_fall_back(self):
        rows = _base_rows()
        rows[1][9] = 31
        rows[1][10] = 69
        self.save(rows)
        with self.assertRaises(PreciseRollbackUnavailable):
            self.apply()

    def test_main_edit_log_on_order_columns_falls_back(self):
        self.save(_base_rows())
        self.logs = [{"action": "主檔編輯", "detail": "[Sheet] R2C10: 30 → 30（料號 P1）"}]
        with self.assertRaises(PreciseRollbackUnavailable):
            self.apply()

    def test_insert_order_falls_back(self):
        self.save(_base_rows())
        self.record = {**_record([GROUP_11]), "code_shifts": True}
        with patch.object(precise.db, "get_current_main_period_id", return_value=0), \
             patch.object(precise.db, "get_setting", return_value=str(self.path)), \
             patch.object(precise.db, "resolve_managed_path", side_effect=lambda p, k: p):
            with self.assertRaisesRegex(PreciseRollbackUnavailable, "插單"):
                ensure_precise_preconditions(SESSION, ORDER)
            self.record = _record([GROUP_11])
            with self.assertRaisesRegex(PreciseRollbackUnavailable, "插單"):
                ensure_precise_preconditions(SESSION, {**ORDER, "insert_before_code": "1-2"})
            self.assertEqual(ensure_precise_preconditions(SESSION, ORDER), str(self.path))

    def test_legacy_dispatch_is_matched_by_header_and_records(self):
        self.save(_base_rows())
        self.record = None
        self.records = [
            {"part_number": "P1", "needed_qty": 29.6},
            {"part_number": "P2", "needed_qty": 20.2},
        ]
        summary = self.apply()
        self.assertTrue(summary["legacy"])
        self.assertEqual(read_stock(str(self.path))["P1"], 90)

    def test_legacy_mismatch_or_ambiguous_falls_back(self):
        self.record = None
        self.records = [{"part_number": "P1", "needed_qty": 30}]
        self.save(_base_rows())
        with self.assertRaises(PreciseRollbackUnavailable):
            self.apply()  # P2 在主檔但不在發料紀錄
        rows = _base_rows()
        rows[0][11:14] = ["1-1", "PO-A", "M-A"]
        rows[3][11:14] = [None, None, None]
        rows[1][11:14] = [0, 30, 40]
        rows[2][11:14] = [10, 20, 30]
        self.records = [{"part_number": "P1", "needed_qty": 30}, {"part_number": "P2", "needed_qty": 20}]
        self.save(rows)
        with self.assertRaises(PreciseRollbackUnavailable):
            self.apply()

    def test_formula_in_later_balance_falls_back(self):
        rows = _base_rows()
        rows[1][13] = "=K2-M2"
        self.save(rows)
        with self.assertRaises(PreciseRollbackUnavailable):
            self.apply()

    def test_build_record_groups_plan_rows(self):
        record = build_dispatch_column_record([
            {"col_h": 9, "col_f": 10, "col_j": 11, "group_batch_code": "1-1", "group_po_number": "PO-A",
             "group_bom_model": "M-A", "row_idx": 2, "part_number": "p1", "effective_h": 0, "f_value": 30, "j_value": 70},
        ], insert_before_code="", code_shifts=[])
        self.assertEqual(record["groups"][0]["rows"][0]["part"], "P1")
        self.assertFalse(record["code_shifts"])


class DefectiveBatchRemovalTests(unittest.TestCase):
    ITEMS = [{"part_number": "P1", "defective_qty": 5}]

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "main.xlsx"
        wb = Workbook()
        ws = wb.active
        ws.append(["料號", "廠商", "MOQ", None, None, None, None, "盤點", "1-1", "PO", "M"])
        ws.append(["P1", "", 1, None, None, None, None, 100, 0, 0, 100])
        ws.append(["P2", "", 1, None, None, None, None, 50, 0, 0, 50])
        wb.save(self.path)
        wb.close()
        patcher = patch("app.services.defective_deduction.refresh_snapshot_from_main", lambda *a, **k: None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_trailing_batch_block_is_deleted(self):
        deduct_defectives_from_main(str(self.path), self.ITEMS, entry_header="加工多打扣帳")
        self.assertEqual(read_stock(str(self.path))["P1"], 95)
        result = remove_batch_deduction_from_main(str(self.path), {"id": 7, "items": self.ITEMS})
        self.assertEqual(result["mode"], "removed")
        wb = load_workbook(self.path)
        try:
            self.assertEqual(wb.active.max_column, 11)
        finally:
            wb.close()
        self.assertEqual(read_stock(str(self.path))["P1"], 100)

    def test_mid_sheet_batch_block_is_zeroed_and_marked(self):
        deduct_defectives_from_main(str(self.path), self.ITEMS, entry_header="不良品扣帳")
        wb = load_workbook(self.path)
        ws = wb.active
        start = ws.max_column + 1
        for col, value in enumerate(["1-2", "PO2", "M2"], start=start):
            ws.cell(1, col).value = value
        ws.cell(2, start).value, ws.cell(2, start + 1).value, ws.cell(2, start + 2).value = 0, 10, 85
        wb.save(self.path)
        wb.close()
        result = remove_batch_deduction_from_main(str(self.path), {"id": 7, "items": self.ITEMS})
        self.assertEqual(result["mode"], "zeroed")
        wb = load_workbook(self.path)
        try:
            ws = wb.active
            self.assertEqual(ws.cell(1, 12).value, "已刪除批次 #7")
            self.assertEqual(ws.cell(2, start + 2).value, 90)
        finally:
            wb.close()
        self.assertEqual(read_stock(str(self.path))["P1"], 90)
        self.assertEqual(read_stock(str(self.path))["P2"], 50)


class PreciseRollbackRouterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient
        from main import app
        cls.client = TestClient(app)

    def _patches(self):
        from app.routers import schedule
        order = {"id": 5, "po_number": "PO-5", "model": "M5", "status": "dispatched"}
        session = {"id": 9, "order_id": 5, "previous_status": "merged", "dispatched_at": "2026-11-01T09:00:00"}
        return schedule, order, session

    def test_precise_mode_success_does_not_offer_replay(self):
        schedule, order, session = self._patches()
        result = {"mode": "precise", "count": 1, "main_backup_path": "/x", "precise": {"mode": "zeroed", "part_deltas": {"P": 1}}}
        with patch.object(schedule.db, "get_order", return_value=order), \
             patch.object(schedule.db, "get_active_dispatch_session", return_value=session), \
             patch.object(schedule, "_precise_rollback_dispatch_session", return_value=result), \
             patch.object(schedule.db, "get_active_merge_drafts", return_value=[]), \
             patch.object(schedule.db, "log_activity") as log_activity:
            response = self.client.post("/api/schedule/orders/5/rollback?mode=precise")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["mode"], "precise")
        self.assertEqual(data["replay_cutoff"], "")
        self.assertEqual(log_activity.call_args[0][0], "order_rollback_precise")

    def test_precise_mode_unavailable_returns_409_without_full_restore(self):
        schedule, order, session = self._patches()
        with patch.object(schedule.db, "get_order", return_value=order), \
             patch.object(schedule.db, "get_active_dispatch_session", return_value=session), \
             patch.object(schedule, "_precise_rollback_dispatch_session",
                          side_effect=PreciseRollbackUnavailable("插單")), \
             patch.object(schedule, "_rollback_dispatch_sessions") as full_restore:
            response = self.client.post("/api/schedule/orders/5/rollback?mode=precise")
        self.assertEqual(response.status_code, 409)
        full_restore.assert_not_called()

    def test_full_restore_refused_when_precise_rollback_crosses_cutoff(self):
        schedule, order, session = self._patches()
        with patch.object(schedule, "_build_rollback_preview", return_value=(order, session, [order], [], "2026-11-01T09:00:00")), \
             patch.object(schedule.db, "get_precise_rollbacks_crossing", return_value=[{"order_id": 6, "po_number": "PO-6"}]), \
             patch.object(schedule, "_rollback_dispatch_sessions") as full_restore:
            response = self.client.post("/api/schedule/orders/5/rollback?force=1")
        self.assertEqual(response.status_code, 409)
        full_restore.assert_not_called()


if __name__ == "__main__":
    unittest.main()
