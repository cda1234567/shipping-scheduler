"""一鍵調撥需求改用最新主檔該批批次碼欄（左欄，實際扣帳量）；不連線 ERP。"""
from __future__ import annotations

import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from openpyxl import Workbook

from app.services import st_pending_transfer as st
from app.services.main_reader import read_batch_usage

HEADERS = ["料號", "廠商", "MOQ", "說明", None, None, None, "期初庫存",
           "10-5", "PO-1", "EC-MODEL",
           "10-6", "PO-2", "TX2-S1 Main",
           "10-6", "PO-3", "TX2-S1 Conn"]


def _save(path: Path, rows: list[list]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.append(HEADERS)
    for row in rows:
        ws.append(row)
    wb.save(path)
    wb.close()


ROWS = [
    # 批次碼欄（左）是實際扣帳量；中間 PO 欄是機種用量、型號欄是結存，都不算。
    ["EC-20117A", "V", 10000, "", None, None, None, 20000, 1811, 10000, 18189, None, None, None, None, None, None],
    ["UC-20033B-TAB", "V", 0, "", None, None, None, 500, None, None, None, 100, 140, 400, "=40+60", 77, 300],
    ["MAIN-ONLY", "V", 0, "", None, None, None, 500, 50, 999, 450, None, None, None, None, None, None],
    ["NO-USAGE", "V", 0, "", None, None, None, 500, None, 30, 500, None, None, None, None, None, None],
]


def _batches(codes):
    return st.list_batches([{"id": i, "code": code, "status": "dispatched", "created_at": at,
                             "first_dispatched_at": at} for i, (code, at) in enumerate(codes, start=1)])


class MainBatchUsageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "main.xlsx"
        _save(self.path, ROWS)

    def tearDown(self):
        self.tmp.cleanup()

    def test_reads_batch_code_column_and_sums_same_batch_groups(self):
        usage = read_batch_usage(str(self.path))
        self.assertEqual(usage["10-5"], {"EC-20117A": 1811, "MAIN-ONLY": 50})
        self.assertEqual(usage["10-6"], {"UC-20033B-TAB": 200})

    def test_unreadable_usage_formula_raises(self):
        rows = [list(ROWS[0])]
        rows[0][8] = "=SUM(Z1:Z9)"
        _save(self.path, rows)
        with self.assertRaises(ValueError):
            read_batch_usage(str(self.path))

    def test_deductions_scope_follows_st_consumptions(self):
        usage = read_batch_usage(str(self.path))["10-5"]
        consumed = [("EC-20117A", 10000), ("NO-USAGE", 30)]
        # 主檔用量覆蓋整包扣量；主檔沒用量的不撥；只在主檔有用量的不加進來。
        self.assertEqual(st.main_usage_deductions(consumed, usage, {}), [("EC-20117A", 1811)])
        self.assertEqual(st.main_usage_deductions([("OLD", 5)], {"P": 7}, {"OLD": "P"}), [("P", 7)])
        self.assertEqual(st.main_usage_deductions([("P", 0)], {"P": 7}, {}), [])

    def test_batch_breakdown_uses_main_usage_not_used_qty(self):
        batches = _batches([("10-3", "2026-10-20 08:00:00"), ("10-5", "2026-10-28 08:00:00"),
                            ("10-6", "2026-10-30 08:00:00")])
        consumptions = {
            (1,): [("X", 1)],
            (2,): [("EC-20117A", 10000), ("NO-USAGE", 30)],
            (3,): [("UC-20033B-TAB", 140)],
        }

        @contextmanager
        def session():
            yield object()

        def settings(key, default=""):
            return {"main_file_path": str(self.path), "st_inventory_loaded_at": ""}.get(key, default)

        with patch.object(st, "list_batches", return_value=batches), \
                patch.object(st, "get_cutoff_setting", return_value="10-3"), \
                patch.object(st, "_consumptions", side_effect=lambda ids, loaded: consumptions[tuple(ids)]), \
                patch.object(st.part_conversion, "active_map", return_value={}), \
                patch.object(st.low_stock, "_erp_session", session), \
                patch.object(st, "detect_settle_date", return_value=None), \
                patch.object(st, "query_transfers", return_value={}), \
                patch.object(st.db, "get_setting", side_effect=settings), \
                patch.object(st.db, "successful_erp_transfers", return_value=[]):
            out = st.batch_breakdown("CHIComp03")
        rows = {b["code"]: b["rows"] for b in out["batches"]}
        self.assertEqual(rows["10-5"], [{"part": "EC-20117A", "pending": 1811, "ware_out": "001"}])
        self.assertEqual(rows["10-6"], [{"part": "UC-20033B-TAB", "pending": 200, "ware_out": "002"}])

    def test_batch_breakdown_requires_main_file(self):
        with patch.object(st, "list_batches", return_value=_batches([("10-3", "2026-10-20 08:00:00")])), \
                patch.object(st, "get_cutoff_setting", return_value="10-3"), \
                patch.object(st, "_consumptions", return_value=[]), \
                patch.object(st.part_conversion, "active_map", return_value={}), \
                patch.object(st.db, "get_setting", return_value=""):
            with self.assertRaises(ValueError):
                st.batch_breakdown("CHIComp03")


if __name__ == "__main__":
    unittest.main()
