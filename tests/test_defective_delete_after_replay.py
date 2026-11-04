from __future__ import annotations

import asyncio
import os
import shutil
import sqlite3
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi import HTTPException
from openpyxl import Workbook, load_workbook

from app import database as db
from app.models import DefectiveReplayRequest
from app.routers import defectives
from app.services import defective_deduction
from app.services.defective_deduction import (
    deduct_defectives_from_main,
    locate_applied_batch_deduction,
)
from app.services.inventory_restore_guard import (
    BATCH_NOT_APPLIED_MESSAGE,
    RESTORE_BLOCKED_MESSAGE,
    ensure_defective_batch_delete_allowed,
    parse_main_edit_detail,
)
from app.services.main_reader import read_stock

OVERRUN = "加工多打扣帳"
ITEMS = [
    {"part_number": "IC-AAA", "defective_qty": 100},
    {"part_number": "IC-BBB", "defective_qty": 6},
]


class DefectiveDeleteAfterReplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db._CREATE_SQL)
        self.addCleanup(self.conn.close)

        @contextmanager
        def connection():
            with self.conn:
                yield self.conn

        for target, attr, value in (
            (db, "get_conn", connection),
            (defectives, "backup_main_file", lambda *a, **k: ""),
            (defectives, "_refresh_active_merge_drafts_after_main_change", lambda: None),
            (defectives, "refresh_snapshot_from_main", lambda *a, **k: None),
            (defective_deduction, "refresh_snapshot_from_main", lambda *a, **k: None),
        ):
            patcher = patch.object(target, attr, value)
            patcher.start()
            self.addCleanup(patcher.stop)

        self.main = Path(self.tmp.name) / "main.xlsx"
        wb = Workbook()
        ws = wb.active
        ws.append(["料號", "廠商", "MOQ", "10-29", None, "結存", "11-1", None, "結存"])
        ws.append(["IC-AAA", "", 1, 0, 0, 1000, 0, 0, 1000])
        ws.append(["IC-BBB", "", 1, 0, 0, 50, 0, 0, 50])
        ws.append(["IC-OTHER", "", 1, 0, 0, 70, 500, 0, 570])
        wb.save(self.main)
        wb.close()
        db.set_setting("main_file_path", str(self.main))
        self.rollback_backup = Path(self.tmp.name) / "main_backup_rollback.xlsx"
        shutil.copy2(self.main, self.rollback_backup)
        self.original_stock = read_stock(str(self.main))

    # ── helpers ──
    def import_batch(self) -> int:
        time.sleep(0.01)
        deduct_defectives_from_main(str(self.main), ITEMS, entry_header=OVERRUN)
        batch_id = db.create_defective_batch("加工多打明細｜test.xlsx", main_file_mtime=os.path.getmtime(self.main))
        for item in ITEMS:
            db.create_defective_record({
                "batch_id": batch_id, "part_number": item["part_number"],
                "defective_qty": item["defective_qty"], "action_taken": OVERRUN, "status": "confirmed",
            })
        time.sleep(0.01)
        return batch_id

    def batch(self, batch_id: int) -> dict:
        return next(b for b in db.get_defective_batches() if b["id"] == batch_id)

    def force_rollback_and_replay(self, replay: bool = True):
        time.sleep(0.01)
        shutil.copy2(self.rollback_backup, self.main)
        os.utime(self.main, None)
        db.log_activity("order_rollback", "從訂單 X (M) 開始強制反悔，共 1 筆，主檔已還原")
        time.sleep(0.01)
        if replay:
            asyncio.run(defectives.replay_after_rollback(DefectiveReplayRequest(cutoff="2020-01-01T00:00:00")))
            time.sleep(0.01)

    def manual_edit(self, row: int, col: int, value):
        wb = load_workbook(self.main)
        ws = wb.active
        old = ws.cell(row, col).value
        ws.cell(row, col).value = value
        wb.save(self.main)
        wb.close()
        db.log_activity("主檔編輯", f"[Sheet] R{row}C{col}: {old} → {value}")
        time.sleep(0.01)

    # ── tests ──
    def test_forced_rollback_then_replay_batch_is_deletable_and_reverses_stock(self):
        batch_id = self.import_batch()
        self.manual_edit(4, 7, 600)  # 退回前改的 → 會被強制退回蓋掉
        self.force_rollback_and_replay()
        self.assertEqual(read_stock(str(self.main))["IC-AAA"], 900)

        guard = ensure_defective_batch_delete_allowed(self.batch(batch_id))
        self.assertTrue(guard["relaxed"])
        self.assertEqual(guard["applied"]["status"], "applied")
        self.assertEqual(guard["applied"]["qty"], 106)

        result = defectives._delete_batch_with_history(batch_id)
        self.assertFalse(result["main_file_changed"])
        self.assertEqual(result["reversed_count"], 2)
        stock = read_stock(str(self.main))
        self.assertEqual(stock["IC-AAA"], 1000)
        self.assertEqual(stock["IC-BBB"], 50)
        self.assertEqual(locate_applied_batch_deduction(str(self.main), {"items": ITEMS})["status"], "missing")

    def test_replay_updates_batch_mtime_and_marks_header(self):
        batch_id = self.import_batch()
        self.force_rollback_and_replay()
        self.assertAlmostEqual(self.batch(batch_id)["main_file_mtime"], os.path.getmtime(self.main), places=3)
        wb = load_workbook(self.main)
        try:
            col = locate_applied_batch_deduction(str(self.main), self.batch(batch_id))["columns"][0]
            self.assertEqual(wb.active.cell(1, col).comment.text, f"defective-batch:{batch_id}")
        finally:
            wb.close()

    def test_unrelated_manual_edit_after_replay_does_not_block_and_reverses(self):
        batch_id = self.import_batch()
        self.force_rollback_and_replay()
        self.manual_edit(2, 7, 30)  # 同料號、但不是這批扣帳欄位
        guard = ensure_defective_batch_delete_allowed(self.batch(batch_id))
        self.assertTrue(guard["relaxed"])
        before = read_stock(str(self.main))["IC-AAA"]
        result = defectives._delete_batch_with_history(batch_id)
        self.assertFalse(result["main_file_changed"])
        self.assertEqual(read_stock(str(self.main))["IC-AAA"], before + 100)

    def test_manual_edit_without_rollback_on_unrelated_cell_still_reverses(self):
        batch_id = self.import_batch()
        self.manual_edit(4, 7, 600)
        result = defectives._delete_batch_with_history(batch_id)
        self.assertFalse(result["main_file_changed"])
        self.assertEqual(read_stock(str(self.main))["IC-AAA"], 1000)

    def test_edit_inside_batch_columns_blocks(self):
        batch_id = self.import_batch()
        self.force_rollback_and_replay()
        col = locate_applied_batch_deduction(str(self.main), self.batch(batch_id))["columns"][0]
        self.manual_edit(4, col + 2, 1)  # 改到這批的結存欄（其他料號），內容比對仍成立但欄位衝突
        with self.assertRaises(HTTPException) as caught:
            ensure_defective_batch_delete_allowed(self.batch(batch_id))
        self.assertEqual(caught.exception.detail, RESTORE_BLOCKED_MESSAGE)

    def test_changed_deduction_value_blocks_as_not_applied(self):
        batch_id = self.import_batch()
        self.force_rollback_and_replay()
        col = locate_applied_batch_deduction(str(self.main), self.batch(batch_id))["columns"][0]
        wb = load_workbook(self.main)
        wb.active.cell(2, col + 1).value = 99
        wb.save(self.main)
        wb.close()
        with self.assertRaises(HTTPException) as caught:
            ensure_defective_batch_delete_allowed(self.batch(batch_id))
        self.assertEqual(caught.exception.detail, BATCH_NOT_APPLIED_MESSAGE)

    def test_rollback_without_replay_still_blocks(self):
        batch_id = self.import_batch()
        self.force_rollback_and_replay(replay=False)
        with self.assertRaises(HTTPException) as caught:
            ensure_defective_batch_delete_allowed(self.batch(batch_id))
        self.assertEqual(caught.exception.detail, RESTORE_BLOCKED_MESSAGE)

    def test_second_rollback_after_replay_blocks(self):
        batch_id = self.import_batch()
        self.force_rollback_and_replay()
        self.force_rollback_and_replay(replay=False)
        with self.assertRaises(HTTPException):
            ensure_defective_batch_delete_allowed(self.batch(batch_id))

    def test_other_blocking_action_still_blocks(self):
        batch_id = self.import_batch()
        self.force_rollback_and_replay()
        db.log_activity("main_file_upload", "x")
        with self.assertRaises(HTTPException):
            ensure_defective_batch_delete_allowed(self.batch(batch_id))

    def test_parse_main_edit_detail(self):
        parsed = parse_main_edit_detail("[2026] R430C480: 500 → 600（料號 IC-AP7381-33Y）")
        self.assertEqual((parsed["row"], parsed["col"], parsed["old_value"], parsed["new_value"], parsed["part_number"]),
                         (430, 480, "500", "600", "IC-AP7381-33Y"))
        parsed = parse_main_edit_detail("[2026] R552C486: None → 300")
        self.assertEqual((parsed["row"], parsed["col"], parsed["old_value"], parsed["part_number"]), (552, 486, "None", ""))
        self.assertIsNone(parse_main_edit_detail("亂寫"))

    def test_rollback_preview_lists_overwritten_main_edits(self):
        from app.routers import schedule
        cutoff = db._now()
        time.sleep(0.01)
        self.manual_edit(4, 7, 600)
        edits = schedule._build_overwritten_main_edits(cutoff)
        self.assertEqual(len(edits), 1)
        self.assertEqual(edits[0]["part_number"], "IC-OTHER")
        self.assertEqual(edits[0]["column_header"], "11-1")
        self.assertEqual((edits[0]["old_value"], edits[0]["new_value"]), ("500", "600"))
        self.assertEqual(schedule._build_overwritten_main_edits(db._now()), [])


if __name__ == "__main__":
    unittest.main()
