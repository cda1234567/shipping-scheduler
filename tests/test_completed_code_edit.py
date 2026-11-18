"""已發料列雙擊改批次代號（v2026.11.17.1）：orders.code 與主檔批次欄標頭一起改。"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import openpyxl
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app import database as db
from app.services import completed_code_edit as cce
from app.services import dispatch_precise_rollback as precise
from app.services.dispatch_pipeline import rollback_dispatch_sessions
from app.services.merge_drafts import _find_main_batch_col_for_file
from main import app

ROOT = Path(__file__).resolve().parents[1]
PO = "4500059697"
_REAL_AFTER_MAIN_CHANGED = cce._after_main_changed


def _header(*groups):
    # 1 料號 2 廠商 3 MOQ 4 盤點 | 每組 3 欄：批次 / PO / 機種
    row = ["料號", "廠商", "MOQ", "盤點"]
    for code, po, model in groups:
        row += [code, po, model]
    return row


class CompletedCodeEditTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.dir = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.stack.enter_context(patch.object(db, "DB_PATH", self.dir / "test.db"))
        self.stack.enter_context(patch.object(db, "DATA_DIR", self.dir))
        self.stack.enter_context(patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "1"}))
        self.backup_dir = self.dir / "backups"
        self.stack.enter_context(patch("app.routers.schedule.BACKUP_DIR", self.backup_dir))
        # 主檔寫入後的收尾（快照 / 預覽快取 / 副檔重建）在這裡只確認有被呼叫
        self.after_calls = []
        self.stack.enter_context(patch.object(cce, "_after_main_changed", lambda path: self.after_calls.append(path)))
        db.init_db()
        self.main_path = self.dir / "main.xlsx"
        db.set_setting("main_file_path", str(self.main_path))
        self.client = TestClient(app)

    # ── helpers ──────────────────────────────────────────────────────────
    def save_main(self, header, rows=()):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(header)
        for row in rows:
            ws.append(row)
        wb.save(self.main_path)
        wb.close()

    def header(self):
        wb = openpyxl.load_workbook(self.main_path)
        try:
            return [cell.value for cell in wb.active[1]]
        finally:
            wb.close()

    def add_order(self, code="12-13", po=PO, model="TX2-S1", status="dispatched", archived="", **extra):
        fields = {"code": code, "po_number": po, "model": model, "pcb": "PCB-A1134", "order_qty": 100,
                  "status": status, "archived_at": archived, "delivery_date": "2027-01-29",
                  "created_at": "2026-11-10T09:00:00", **extra}
        with db.get_conn() as conn:
            cols = ",".join(fields)
            return conn.execute(f"INSERT INTO orders({cols}) VALUES({','.join('?' * len(fields))})",
                                list(fields.values())).lastrowid

    def add_session(self, order_id, dispatched_at="2026-11-10T10:00:00", backup_path="", groups=None):
        with db.get_conn() as conn:
            session_id = conn.execute(
                "INSERT INTO dispatch_sessions(order_id, previous_status, backup_path, main_file_path, dispatched_at, "
                "main_period_id) VALUES(?,?,?,?,?,?)",
                (order_id, "merged", backup_path, str(self.main_path), dispatched_at, db.get_current_main_period_id()),
            ).lastrowid
        if groups is not None:
            db.save_dispatch_column_record(session_id, order_id, {"version": 1, "groups": groups,
                                                                   "insert_before_code": "", "code_shifts": False})
        return session_id

    def patch_code(self, order_id, code, force=False):
        return self.client.patch(f"/api/schedule/completed/{order_id}/code", json={"code": code, "force": force})

    def order_code(self, order_id):
        return db.get_order(order_id)["code"]

    # ── 格式 / 狀態 ──────────────────────────────────────────────────────
    def test_invalid_format_is_rejected_without_changes(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        order_id = self.add_order()
        for bad in ("12_7", "12-", "-7", "a-1", "12-7-1", "", "  ", "１２-７"):
            response = self.patch_code(order_id, bad)
            self.assertEqual(response.status_code, 400, bad)
            self.assertIn("數字-數字", response.json()["detail"])
        self.assertEqual(self.order_code(order_id), "12-13")
        self.assertEqual(self.header()[4], "12-13")
        self.assertFalse(self.backup_dir.exists())

    def test_same_code_after_trim_is_ok_and_untouched(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        order_id = self.add_order()
        before = self.main_path.stat().st_mtime_ns
        response = self.patch_code(order_id, " 12-13 ")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["unchanged"])
        self.assertEqual(self.main_path.stat().st_mtime_ns, before)
        self.assertEqual(self.after_calls, [])

    def test_only_dispatched_orders_can_change(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        for status in ("pending", "merged", "completed", "cancelled"):
            order_id = self.add_order(status=status, po=f"45000596{len(status):02d}")
            response = self.patch_code(order_id, "12-7")
            self.assertEqual(response.status_code, 409, status)
            self.assertIn("已發料", response.json()["detail"])
            self.assertEqual(self.order_code(order_id), "12-13")
        self.assertEqual(self.patch_code(99999, "12-7").status_code, 404)

    def test_insert_order_cannot_change(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        order_id = self.add_order(insert_before_code="12-13")
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.order_code(order_id), "12-13")

    # ── 成功：DB + 主檔一致 ──────────────────────────────────────────────
    def test_success_updates_db_and_main_header_together(self):
        self.save_main(_header(("12-12", "4500059600", "T7U"), ("12-13", PO, "TX2-S1")),
                       [["P1", None, 1, 100, 0, 10, 90, 5, 20, 75]])
        order_id = self.add_order()
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual((body["old_code"], body["code"], body["columns"]), ("12-13", "12-7", [8]))
        self.assertEqual(self.order_code(order_id), "12-7")
        header = self.header()
        self.assertEqual(header[4:10], ["12-12", "4500059600", "T7U", "12-7", PO, "TX2-S1"])
        # 只改標頭，其他格（含數字）不動
        wb = openpyxl.load_workbook(self.main_path)
        try:
            self.assertEqual([c.value for c in wb.active[2]], ["P1", None, 1, 100, 0, 10, 90, 5, 20, 75])
        finally:
            wb.close()
        # 改之前有備份主檔，備份裡是舊標頭
        backup = Path(body["main_backup_path"])
        self.assertTrue(backup.exists())
        self.assertEqual(backup.parent, self.backup_dir)
        wb = openpyxl.load_workbook(backup)
        try:
            self.assertEqual(wb.active.cell(1, 8).value, "12-13")
        finally:
            wb.close()
        # activity log + 改代號紀錄
        logs = db.get_activity_logs_by_action(cce.CODE_CHANGE_LOG_ACTION, limit=5)
        self.assertEqual(len(logs), 1)
        self.assertIn("12-13 → 12-7", logs[0]["detail"])
        self.assertIn(PO, logs[0]["detail"])
        self.assertIn(f"#{order_id}", logs[0]["detail"])
        self.assertIn("H", logs[0]["detail"])
        changes = db.get_order_code_changes_after("")
        self.assertEqual([(c["order_id"], c["old_code"], c["new_code"]) for c in changes], [(order_id, "12-13", "12-7")])
        self.assertEqual(json.loads(changes[0]["columns_json"])[0]["col"], 8)
        self.assertEqual(self.after_calls, [str(self.main_path)])

    def test_formulas_are_kept(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")), [["P1", "", 1, 100, 0, 10, "=D2+E2-F2"]])
        order_id = self.add_order()
        self.assertEqual(self.patch_code(order_id, "12-7").status_code, 200)
        wb = openpyxl.load_workbook(self.main_path)
        try:
            self.assertEqual(wb.active.cell(2, 7).value, "=D2+E2-F2")
        finally:
            wb.close()

    def test_ab_boards_are_all_changed(self):
        self.save_main(_header(("12-13", PO, "T356789IU-REV-3.4 (A板)"), ("12-13", PO, "T356789IU-REV-1.1 (B板)"),
                               ("12-14", "4500059698", "T8U")))
        order_id = self.add_order(model="T356789IU")
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["columns"], [5, 8])
        header = self.header()
        self.assertEqual([header[4], header[7], header[10]], ["12-7", "12-7", "12-14"])

    def test_same_old_code_of_other_po_is_not_touched(self):
        # 舊循環殘留的同名批次 12-13（別的 PO）不能被一起改
        self.save_main(_header(("12-13", "4500059105", "T356789IU"), ("12-13", PO, "TX2-S1")))
        old_cycle = self.add_order(code="12-13", po="4500059105", model="T356789IU", archived="2026-11-01 09:00:00")
        order_id = self.add_order()
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 200, response.text)
        header = self.header()
        self.assertEqual((header[4], header[7]), ("12-13", "12-7"))
        self.assertEqual(self.order_code(old_cycle), "12-13")

    def test_po_suffix_header_matches(self):
        # 標頭 ≥5 碼且為 PO 結尾也算（同 dispatch._cols_for_order）
        self.save_main(_header(("12-13", "59697", "TX2-S1")))
        order_id = self.add_order()
        self.assertEqual(self.patch_code(order_id, "12-7").status_code, 200)
        self.assertEqual(self.header()[4], "12-7")

    # ── 找不到 / 無法確定 → 整個不改 ─────────────────────────────────────
    def assert_nothing_changed(self, order_id, header_before):
        self.assertEqual(self.order_code(order_id), "12-13")
        self.assertEqual(self.header(), header_before)
        self.assertEqual(db.get_order_code_changes_after(""), [])
        self.assertEqual(db.get_activity_logs_by_action(cce.CODE_CHANGE_LOG_ACTION, limit=5), [])
        self.assertEqual(self.after_calls, [])

    def test_main_columns_not_found_changes_nothing(self):
        self.save_main(_header(("12-13", "4500059105", "T356789IU"), ("12-14", PO, "TX2-S1")))
        before = self.header()
        order_id = self.add_order()
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 409)
        self.assertIn("主檔找不到批次 12-13", response.json()["detail"])
        self.assert_nothing_changed(order_id, before)

    def test_missing_main_file_changes_nothing(self):
        order_id = self.add_order()
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.order_code(order_id), "12-13")

    def test_columns_shared_with_other_dispatched_order_of_same_po_changes_nothing(self):
        self.save_main(_header(("12-13", PO, "TX2-S1"), ("12-13", PO, "TX2-S2")))
        before = self.header()
        order_id = self.add_order()
        self.add_order(model="TX2-S2")
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 409)
        self.assertIn("也可能屬於其他已發料單", response.json()["detail"])
        self.assert_nothing_changed(order_id, before)

    def test_columns_not_matching_dispatch_record_changes_nothing(self):
        self.save_main(_header(("12-13", PO, "TX2-S1"), ("12-13", PO, "OTHER-BOARD")))
        before = self.header()
        order_id = self.add_order()
        self.add_session(order_id, groups=[{"col_h": 5, "col_f": 6, "col_j": 7, "batch_code": "12-13",
                                            "po_number": PO, "bom_model": "TX2-S1", "rows": []}])
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 409)
        self.assertIn("發料紀錄", response.json()["detail"])
        self.assert_nothing_changed(order_id, before)

    def test_db_failure_restores_main_from_backup(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        before = self.header()
        order_id = self.add_order()
        def boom(*_args, **_kwargs):
            raise RuntimeError("disk full")

        with patch("app.services.merge_to_main._save_workbook_atomically", boom):
            with self.assertRaises(RuntimeError):
                cce.change_completed_order_code(order_id, "12-7", backup_dir=str(self.backup_dir))
        self.assert_nothing_changed(order_id, before)

    def test_commit_failure_after_main_saved_restores_main(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        before = self.header()
        order_id = self.add_order()
        from app.services import merge_to_main

        real_save = merge_to_main._save_workbook_atomically

        def save_then_fail(workbook, path, **kwargs):
            real_save(workbook, path, **kwargs)
            self.assertEqual(self.header()[4], "12-7")
            raise RuntimeError("after save")

        with patch("app.services.merge_to_main._save_workbook_atomically", save_then_fail):
            with self.assertRaises(RuntimeError):
                cce.change_completed_order_code(order_id, "12-7", backup_dir=str(self.backup_dir))
        self.assert_nothing_changed(order_id, before)

    # ── 重複代號 ─────────────────────────────────────────────────────────
    def test_duplicate_code_needs_force(self):
        self.save_main(_header(("12-7", "4500059105", "T356789IU"), ("12-13", PO, "TX2-S1")))
        other = self.add_order(code="12-7", po="4500059105", model="T356789IU")
        order_id = self.add_order()
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 409)
        detail = response.json()["detail"]
        self.assertEqual(detail["code"], "duplicate_code")
        self.assertIn("4500059105", detail["message"])
        self.assertEqual([item["id"] for item in detail["duplicates"]], [other])
        self.assertEqual(self.order_code(order_id), "12-13")
        self.assertEqual(self.header()[7], "12-13")

        response = self.patch_code(order_id, "12-7", force=True)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["forced"])
        self.assertEqual(self.order_code(order_id), "12-7")
        header = self.header()
        # 別張 12-7（不同 PO）的欄位不受影響
        self.assertEqual((header[4], header[5], header[7]), ("12-7", "4500059105", "12-7"))
        self.assertEqual(self.order_code(other), "12-7")

    def test_archived_or_same_po_order_is_not_duplicate(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        self.add_order(code="12-7", po="4500059105", archived="2026-11-01 09:00:00")
        self.add_order(code="12-7", po=PO, model="TX2-S2", status="pending")
        order_id = self.add_order()
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["forced"])

    def test_old_cycle_dispatched_without_main_columns_is_not_duplicate(self):
        # 2026-08 舊循環 #17 12-7 沒封存，但主檔已換期、沒有 12-7｜4500059105 的欄位
        # （主檔裡別的 PO 的 12-7 不算它的欄位）
        self.save_main(_header(("12-7", "4500059999", "T8U"), ("12-13", PO, "TX2-S1")))
        self.add_order(code="12-7", po="4500059105", model="T356789IU")
        self.add_order(code="12-7", po="4500059106", model="T7U", status="completed")
        order_id = self.add_order()
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["forced"])
        self.assertEqual(self.order_code(order_id), "12-7")
        self.assertEqual(self.header()[4:8], ["12-7", "4500059999", "T8U", "12-7"])

    def test_dispatched_with_main_columns_is_duplicate(self):
        for status in ("dispatched", "completed"):
            with self.subTest(status=status):
                self.save_main(_header(("12-7", "4500059105", "T356789IU"), ("12-13", PO, "TX2-S1")))
                other = self.add_order(code="12-7", po="4500059105", model="T356789IU", status=status)
                order_id = self.add_order(model=f"TX2-{status}")
                response = self.patch_code(order_id, "12-7")
                self.assertEqual(response.status_code, 409)
                detail = response.json()["detail"]
                self.assertEqual(detail["code"], "duplicate_code")
                self.assertEqual([item["id"] for item in detail["duplicates"]], [other])
                self.assertEqual(self.order_code(order_id), "12-13")
                with db.get_conn() as conn:
                    conn.execute("DELETE FROM orders")

    def test_pending_or_merged_same_code_other_po_is_duplicate(self):
        # 待排程的單主檔還沒有欄位，照舊只看代號
        for status in ("pending", "merged"):
            with self.subTest(status=status):
                self.save_main(_header(("12-13", PO, "TX2-S1")))
                other = self.add_order(code="12-7", po="4500059105", model="T356789IU", status=status)
                order_id = self.add_order()
                response = self.patch_code(order_id, "12-7")
                self.assertEqual(response.status_code, 409)
                detail = response.json()["detail"]
                self.assertEqual(detail["code"], "duplicate_code")
                self.assertEqual([item["id"] for item in detail["duplicates"]], [other])
                self.assertEqual(self.order_code(order_id), "12-13")
                self.assertEqual(self.header()[4], "12-13")

                response = self.patch_code(order_id, "12-7", force=True)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertTrue(response.json()["forced"])
                self.assertEqual(self.order_code(order_id), "12-7")
                with db.get_conn() as conn:
                    conn.execute("DELETE FROM orders")

    def test_cancelled_same_code_is_not_duplicate(self):
        self.save_main(_header(("12-7", "4500059105", "T356789IU"), ("12-13", PO, "TX2-S1")))
        self.add_order(code="12-7", po="4500059105", model="T356789IU", status="cancelled")
        order_id = self.add_order()
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["forced"])
        self.assertEqual(self.order_code(order_id), "12-7")

    # ── 調撥 / 插單 ──────────────────────────────────────────────────────
    def test_transferred_batch_is_rejected(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        order_id = self.add_order()
        self.add_session(order_id, dispatched_at="2026-11-10T10:00:00")
        with db.get_conn() as conn:
            conn.execute("INSERT INTO erp_transfers(batch_key, batch_code, target_db, move_no, status) "
                         "VALUES('2026/12-13','12-13','CHIComp03','MV-001','ok')")
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 409)
        self.assertIn("一鍵調撥", response.json()["detail"])
        self.assertEqual(self.order_code(order_id), "12-13")

    def test_crossing_transfer_cutoff_is_rejected(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        cutoff_order = self.add_order(code="12-10", po="4500059600", model="T7U")
        self.add_session(cutoff_order, dispatched_at="2026-11-09T10:00:00")
        order_id = self.add_order()
        self.add_session(order_id, dispatched_at="2026-11-10T10:00:00")
        db.set_setting("shortage_transfer_cutoff_batch", "2026/12-10")
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 409)
        self.assertIn("截止批次", response.json()["detail"])
        self.assertEqual(self.order_code(order_id), "12-13")
        # 同一側（都在截止批次之後）可以改
        self.assertEqual(self.patch_code(order_id, "12-11").status_code, 200)

    def test_pending_insert_anchored_on_code_is_rejected(self):
        self.save_main(_header(("12-13", PO, "TX2-S1")))
        order_id = self.add_order()
        self.add_order(code="12-13", po="4500059800", status="merged", insert_before_code="12-13", is_manual=1)
        response = self.patch_code(order_id, "12-7")
        self.assertEqual(response.status_code, 409)
        self.assertIn("插單", response.json()["detail"])

    # ── 改完後副檔重抓 / 精準退回 / 整檔還原 ─────────────────────────────
    def test_committed_draft_refetch_finds_renamed_columns(self):
        self.save_main(_header(("12-13", "4500059105", "TX2-S1"), ("12-13", PO, "TX2-S1")))
        order_id = self.add_order()
        self.assertEqual(self.patch_code(order_id, "12-7").status_code, 200)
        wb = openpyxl.load_workbook(self.main_path)
        try:
            col = _find_main_batch_col_for_file(wb.active, db.get_order(order_id), {"model": "TX2-S1"}, None)
        finally:
            wb.close()
        self.assertEqual(col, 8)

    def _precise_main(self):
        self.save_main(
            _header(("12-13", PO, "TX2-S1")),
            [["P1", "", 1, 100, 0, 30, 70], ["P2", "", 1, 50, 10, 20, 40]],
        )

    def _locate(self, order_id, session_id):
        wb = openpyxl.load_workbook(self.main_path)
        try:
            session = {"id": session_id, "order_id": order_id, "dispatched_at": "2026-11-10T10:00:00"}
            return precise.plan_and_apply(wb.active, session, db.get_order(order_id))
        finally:
            wb.close()

    def test_precise_rollback_locate_with_record_after_rename(self):
        self._precise_main()
        order_id = self.add_order()
        session_id = self.add_session(order_id, groups=[{
            "col_h": 5, "col_f": 6, "col_j": 7, "batch_code": "12-13", "po_number": PO, "bom_model": "TX2-S1",
            "rows": [{"row": 2, "part": "P1", "h": 0, "f": 30, "j": 70},
                     {"row": 3, "part": "P2", "h": 10, "f": 20, "j": 40}],
        }])
        self.assertEqual(self.patch_code(order_id, "12-7").status_code, 200)
        # 紀錄裡仍是舊代號 12-13；主檔與 orders.code 是 12-7 → 用目前 order.code 仍找得到
        summary = self._locate(order_id, session_id)
        self.assertEqual(summary["columns"], [5])
        self.assertFalse(summary["legacy"])
        # 改代號不是「主檔編輯」紀錄，不會擋精準退回
        self.assertEqual(db.get_activity_logs_by_action("主檔編輯", limit=5), [])

    def test_precise_rollback_accepts_recorded_code_too(self):
        # 主檔標頭還是舊代號（例如之後被整檔還原）時，靠紀錄的 group.batch_code 一樣找得到
        self._precise_main()
        order_id = self.add_order(code="12-7")
        session_id = self.add_session(order_id, groups=[{
            "col_h": 5, "col_f": 6, "col_j": 7, "batch_code": "12-13", "po_number": PO, "bom_model": "TX2-S1",
            "rows": [{"row": 2, "part": "P1", "h": 0, "f": 30, "j": 70},
                     {"row": 3, "part": "P2", "h": 10, "f": 20, "j": 40}],
        }])
        self.assertEqual(self._locate(order_id, session_id)["columns"], [5])

    def test_precise_rollback_legacy_locate_after_rename(self):
        self._precise_main()
        order_id = self.add_order()
        session_id = self.add_session(order_id)
        with db.get_conn() as conn:
            for part, qty in (("P1", 30), ("P2", 20)):
                conn.execute("INSERT INTO dispatch_records(order_id, part_number, needed_qty, dispatched_at) "
                             "VALUES(?,?,?,?)", (order_id, part, qty, "2026-11-10T10:00:00"))
        self.assertEqual(self.patch_code(order_id, "12-7").status_code, 200)
        summary = self._locate(order_id, session_id)
        self.assertTrue(summary["legacy"])
        self.assertEqual(summary["columns"], [5])

    def test_full_rollback_of_later_dispatch_is_blocked_after_rename(self):
        self.save_main(_header(("12-13", PO, "TX2-S1"), ("12-14", "4500059698", "T8U")))
        backup = self.dir / "before_12_14.xlsx"
        wb = openpyxl.Workbook()
        wb.active.append(_header(("12-13", PO, "TX2-S1")))
        wb.save(backup)
        wb.close()
        order_id = self.add_order()
        self.add_session(order_id, dispatched_at="2026-11-10T10:00:00")
        later = self.add_order(code="12-14", po="4500059698", model="T8U")
        later_session = self.add_session(later, dispatched_at="2026-11-11T10:00:00", backup_path=str(backup))
        self.assertEqual(self.patch_code(order_id, "12-7").status_code, 200)

        sessions = db.get_dispatch_session_tail(later_session)
        with self.assertRaises(HTTPException) as raised:
            rollback_dispatch_sessions(sessions)
        self.assertEqual(raised.exception.status_code, 409)
        self.assertIn("改過批次代號", raised.exception.detail)
        # 主檔沒被還原
        self.assertEqual(self.header()[4], "12-7")
        self.assertEqual(db.get_order(later)["status"], "dispatched")

        # 改回原代號後就不再擋（只檢查判斷函式，不實際還原）
        self.assertEqual(self.patch_code(order_id, "12-13").status_code, 200)
        self.assertEqual(cce.code_changes_restored_by_full_rollback(sessions), [])

    def test_full_rollback_including_renamed_order_is_not_blocked(self):
        order_id = self.add_order(code="12-7")
        session_id = self.add_session(order_id, dispatched_at="2026-11-10T10:00:00")
        with db.get_conn() as conn:
            conn.execute("INSERT INTO order_code_changes(order_id, old_code, new_code, changed_at) "
                         "VALUES(?,?,?,?)", (order_id, "12-13", "12-7", "2026-11-12T10:00:00"))
        self.assertEqual(cce.code_changes_restored_by_full_rollback(db.get_dispatch_session_tail(session_id)), [])
        # 改代號比較早（之後的發料備份已含新代號）也不擋
        other = self.add_order(code="12-14", po="4500059698")
        other_session = self.add_session(other, dispatched_at="2026-11-13T10:00:00")
        self.assertEqual(cce.code_changes_restored_by_full_rollback(db.get_dispatch_session_tail(other_session)), [])

    def test_after_main_changed_keeps_snapshot_caches_and_drafts_in_sync(self):
        from app.services import main_preview
        from app.snapshot_sync import refresh_snapshot_from_main

        self.save_main(_header(("12-13", PO, "TX2-S1")), [["P1", "", 1, 100, 0, 10, 90]])
        refresh_snapshot_from_main(str(self.main_path))
        db.set_setting("main_loaded_at", "2026-11-17T08:00:00")
        snapshot_before = db.get_snapshot()
        order_id = self.add_order()
        main_preview._response_cache[("stale",)] = b"{}"
        rebuilt = []
        with patch.object(cce, "_after_main_changed", _REAL_AFTER_MAIN_CHANGED), \
                patch.object(db, "get_active_merge_drafts", lambda: [{"order_id": 42}]), \
                patch("app.services.merge_drafts.rebuild_merge_drafts", lambda ids: rebuilt.append(list(ids))):
            self.assertEqual(self.patch_code(order_id, "12-7").status_code, 200)
        self.assertEqual(db.get_snapshot(), snapshot_before)
        self.assertEqual(db.get_setting("main_loaded_at"), "2026-11-17T08:00:00")
        self.assertNotIn(("stale",), main_preview._response_cache)
        # 待發料副檔記的是舊主檔 mtime，改完重建避免「主檔內容已變更」誤判
        self.assertEqual(rebuilt, [[42]])

    def test_inventory_count_lock_covers_endpoint(self):
        from app.services.inventory_count_lock import is_inventory_mutation_request

        self.assertTrue(is_inventory_mutation_request("PATCH", "/api/schedule/completed/12/code"))
        self.assertFalse(is_inventory_mutation_request("GET", "/api/schedule/completed"))


class CompletedCodeEditFrontendTests(unittest.TestCase):
    def run_node(self, script: str):
        result = subprocess.run(["node", "--input-type=module", "-e", script], cwd=ROOT,
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def test_pure_helpers(self):
        self.run_node(r"""
import assert from 'node:assert/strict';
import { validateBatchCode, batchCodeKeyAction, batchCodeInputWidth, normalizeBatchCode,
         submitBatchCodeChange, isDuplicateCodeError } from './static/modules/completed_code_edit.js';
assert.equal(validateBatchCode('12-7'), '');
assert.equal(validateBatchCode(' 12-7 '), '');
for (const bad of ['', ' ', '12_7', '12-', '-7', 'a-1', '12-7-1', '１２-７']) assert.notEqual(validateBatchCode(bad), '', bad);
assert.equal(batchCodeKeyAction('Enter'), 'save');
assert.equal(batchCodeKeyAction('Escape'), 'cancel');
assert.equal(batchCodeKeyAction('a'), '');
assert.equal(batchCodeInputWidth('12-13'), '6ch');
assert.equal(batchCodeInputWidth('1-1'), '6ch');
assert.equal(batchCodeInputWidth('123-456'), '8ch');
assert.equal(normalizeBatchCode('  12-7 '), '12-7');

// 相同代號不送出
let calls = [];
const req = async (id, code, force) => { calls.push([id, code, force]); return { ok: true, code, columns: [5] }; };
assert.deepEqual(await submitBatchCodeChange(1, '12-13', ' 12-13 ', { request: req }), { status: 'unchanged', result: null });
assert.equal(calls.length, 0);
// 格式錯前端就擋
await assert.rejects(() => submitBatchCodeChange(1, '12-13', '12_7', { request: req }), /數字-數字/);
assert.equal(calls.length, 0);
// 成功
assert.equal((await submitBatchCodeChange(1, '12-13', '12-7', { request: req })).status, 'saved');
assert.deepEqual(calls, [[1, '12-7', false]]);

// 409 重複 → confirm 後 force 重送；取消就不送
const dup = () => Object.assign(new Error('批次代號 12-7 已被其他訂單使用'), { status: 409, payload: { detail: { code: 'duplicate_code' } } });
assert.ok(isDuplicateCodeError(dup()));
assert.ok(!isDuplicateCodeError(Object.assign(new Error('x'), { status: 409, payload: { detail: 'x' } })));
calls = [];
const dupReq = async (id, code, force) => { calls.push(force); if (!force) throw dup(); return { ok: true, code, columns: [5] }; };
let asked = '';
const saved = await submitBatchCodeChange(1, '12-13', '12-7', { request: dupReq, confirmFn: msg => { asked = msg; return true; } });
assert.equal(saved.status, 'saved');
assert.deepEqual(calls, [false, true]);
assert.match(asked, /已被其他訂單使用/);
calls = [];
assert.equal((await submitBatchCodeChange(1, '12-13', '12-7', { request: dupReq, confirmFn: () => false })).status, 'cancelled');
assert.deepEqual(calls, [false]);
// 其他錯誤直接丟出
await assert.rejects(() => submitBatchCodeChange(1, '12-13', '12-7', {
  request: async () => { throw Object.assign(new Error('主檔找不到批次 12-13'), { status: 409, payload: { detail: '主檔找不到批次 12-13' } }); },
  confirmFn: () => { throw new Error('should not confirm'); },
}), /主檔找不到/);
""")

    def test_editor_dom_flow(self):
        self.run_node(r"""
import assert from 'node:assert/strict';
import { attachCompletedCodeEditor } from './static/modules/completed_code_edit.js';

function el(tag) {
  const node = { tag, _h: {}, children: [], dataset: {}, style: {}, value: '', disabled: false, title: '', _text: '',
    classList: { _s: new Set(), add(c) { this._s.add(c); }, contains(c) { return this._s.has(c); } },
    attrs: {}, setAttribute(k, v) { this.attrs[k] = v; },
    addEventListener(type, fn) { (this._h[type] ||= []).push(fn); },
    fire(type, extra = {}) { for (const fn of this._h[type] || []) fn({ preventDefault() {}, stopPropagation() {}, ...extra }); },
    appendChild(child) { this.children.push(child); },
    focus() {}, select() {},
    get textContent() { return this._text; },
    set textContent(v) { this._text = v; this.children = []; } };
  return node;
}
const doc = { createElement: el };
const tick = () => new Promise(r => setTimeout(r, 0));

async function setup({ request, confirmFn = () => true, requireEdit = () => true } = {}) {
  const tag = el('span');
  tag.textContent = '12-13';
  const toasts = [], saved = [];
  attachCompletedCodeEditor(tag, { orderId: 7, code: '12-13', doc, request, confirmFn, requireEdit,
    toast: (msg, opts) => toasts.push({ msg, opts }), onSaved: async result => saved.push(result) });
  return { tag, toasts, saved };
}

// title 提示
{
  const { tag } = await setup({ request: async () => ({}) });
  assert.equal(tag.title, '雙擊修改批次代號');
  assert.ok(tag.classList.contains('completed-code-tag'));
}
// Esc 取消：不送出、恢復原值
{
  const calls = [];
  const { tag, toasts } = await setup({ request: async (...a) => { calls.push(a); return {}; } });
  tag.fire('dblclick'); await tick();
  const input = tag.children[0];
  assert.equal(input.value, '12-13');
  assert.equal(input.className, 'completed-code-input');
  assert.equal(input.style.width, '6ch');
  input.value = '12-7';
  input.fire('keydown', { key: 'Escape' });
  input.fire('blur');
  await tick();
  assert.equal(calls.length, 0);
  assert.equal(tag.textContent, '12-13');
  assert.equal(toasts.length, 0);
}
// Enter 存檔（blur 不會再送一次）→ toast + 重新載入
{
  const calls = [];
  const { tag, toasts, saved } = await setup({ request: async (id, code, force) => { calls.push([id, code, force]); return { ok: true, code, columns: [5, 8] }; } });
  tag.fire('dblclick'); await tick();
  const input = tag.children[0];
  input.value = ' 12-7 ';
  input.fire('keydown', { key: 'Enter' });
  input.fire('blur');
  await tick(); await tick();
  assert.deepEqual(calls, [[7, '12-7', false]]);
  assert.equal(tag.textContent, '12-7');
  assert.equal(toasts[0].opts.tone, 'success');
  assert.match(toasts[0].msg, /12-7/);
  assert.equal(saved.length, 1);
}
// 失焦也存檔
{
  const calls = [];
  const { tag } = await setup({ request: async (id, code, force) => { calls.push(code); return { ok: true, code, columns: [5] }; } });
  tag.fire('dblclick'); await tick();
  tag.children[0].value = '12-8';
  tag.children[0].fire('blur');
  await tick(); await tick();
  assert.deepEqual(calls, ['12-8']);
}
// 格式錯：前端擋、toast、恢復原值
{
  const calls = [];
  const { tag, toasts } = await setup({ request: async (...a) => { calls.push(a); return {}; } });
  tag.fire('dblclick'); await tick();
  tag.children[0].value = '12/7';
  tag.children[0].fire('keydown', { key: 'Enter' });
  await tick();
  assert.equal(calls.length, 0);
  assert.equal(tag.textContent, '12-13');
  assert.match(toasts[0].msg, /數字-數字/);
}
// 後端失敗：toast 原因、恢復原值、不重新載入
{
  const { tag, toasts, saved } = await setup({ request: async () => { throw Object.assign(new Error('主檔找不到批次 12-13 的欄位'), { status: 409 }); } });
  tag.fire('dblclick'); await tick();
  tag.children[0].value = '12-7';
  tag.children[0].fire('keydown', { key: 'Enter' });
  await tick(); await tick();
  assert.equal(tag.textContent, '12-13');
  assert.equal(toasts[0].opts.tone, 'error');
  assert.match(toasts[0].msg, /主檔找不到/);
  assert.equal(saved.length, 0);
}
// 沒有編輯權限：不開輸入框
{
  const { tag } = await setup({ request: async () => ({}), requireEdit: () => false });
  tag.fire('dblclick'); await tick();
  assert.equal(tag.children.length, 0);
  assert.equal(tag.textContent, '12-13');
}
""")

    def test_schedule_wires_editor_only_for_dispatched(self):
        source = (ROOT / "static" / "modules" / "schedule.js").read_text(encoding="utf-8")
        self.assertIn('import { attachCompletedCodeEditor } from "./completed_code_edit.js";', source)
        self.assertIn('const codeEditable = r.status === "dispatched";', source)
        self.assertIn("onSaved: () => refreshCompleted()", source)
        css = (ROOT / "static" / "style.css").read_text(encoding="utf-8")
        self.assertIn(".completed-code-input", css)

    def test_api_error_keeps_status_and_object_detail_message(self):
        self.run_node(r"""
import assert from 'node:assert/strict';
const { apiFetch } = await import('./static/modules/api.js');
globalThis.fetch = async () => new Response(JSON.stringify({ detail: { code: 'duplicate_code', message: '代號重複' } }), { status: 409 });
await assert.rejects(() => apiFetch('/x'), err => err.status === 409 && err.message === '代號重複' && err.payload.detail.code === 'duplicate_code');
globalThis.fetch = async () => new Response(JSON.stringify({ detail: '普通錯誤' }), { status: 400 });
await assert.rejects(() => apiFetch('/x'), err => err.status === 400 && err.message === '普通錯誤');
""")


if __name__ == "__main__":
    unittest.main()
