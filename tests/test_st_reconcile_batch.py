from __future__ import annotations

import asyncio
import hashlib
import io
import sqlite3
import unittest
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi import HTTPException, UploadFile
from openpyxl import Workbook, load_workbook

from app import database as db
from app.routers import reconcile as reconcile_router
from app.services import st_reconcile
from app.services.main_reader import read_batch_stock, read_stock
from app.services.st_reconcile import (
    StaleReconcilePreviewError,
    build_st_reconcile_preview,
    commit_st_reconcile_stop_loss,
    parse_st_reconcile_file,
)


class BatchReconcileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db._CREATE_SQL)
        self.addCleanup(self.conn.close)

        @contextmanager
        def connection():
            with self.conn:
                yield self.conn

        self.patcher = patch.object(db, 'get_conn', connection)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.backup_patcher = patch.object(st_reconcile, 'BACKUP_DIR', Path(self.tmp.name) / 'backups')
        self.backup_patcher.start()
        self.addCleanup(self.backup_patcher.stop)

        self.count = Path(self.tmp.name) / 'count_2026-9-23.xlsx'
        wb = Workbook()
        ws = wb.active
        ws['A1'] = '盤點日期 2026/9/23'
        for col, value in {4: 'consign invoice NO', 6: '辰尚庫存', 7: '庚霖庫存 當下實際', 21: '實際庫存總和', 22: '生產結餘'}.items():
            ws.cell(3, col, value)
        source_rows = (
            ('EC-20128A-TAB', 780, 771, 33179, 33200),
            ('EC-30037A-TAB', -2848, 351, 99999, 88888),
            ('PART-KEEP', 50, 50, 777, 666),
        )
        for row_idx, values in enumerate(source_rows, start=5):
            for col, value in zip((4, 6, 7, 21, 22), values):
                ws.cell(row_idx, col, value)
        wb.save(self.count)
        wb.close()

        self.main = Path(self.tmp.name) / 'main.xlsx'
        wb = Workbook()
        ws = wb.active
        ws.append(['料號', '廠商', 'MOQ', None, None, None, None, '盤點', '9-11', None, '結存', '9-12', None, '結存', '9-12', None, '結存', '9-13', None, '結存'])
        ws.append(['EC-20128A-TAB', '', 1000, None, None, None, None, 900, 0, 10, 890, 0, 90, 800, 0, 20, 780, 0, 145, 635])
        ws.append(['EC-30037A-TAB', '', 1000, None, None, None, None, -2800, 0, 20, -2820, 0, 28, -2848, 0, 0, -2848, 3617, 0, 769])
        ws.append(['PART-KEEP', '', 1000, None, None, None, None, 100, 0, 10, 90, 0, 20, 70, 0, 20, 50, 0, 0, 50])
        wb.save(self.main)
        wb.close()
        db.set_setting('main_file_path', str(self.main))
        self.conn.execute(
            "INSERT INTO st_inventory_snapshot(part_number,stock_qty) VALUES"
            "('EC-20128A-TAB',22000),('EC-30037A-TAB',9236),('PART-KEEP',444)"
        )
        self.conn.executemany(
            "INSERT INTO inventory_snapshot(part_number,stock_qty,moq,moq_manual,description,snapshot_at) "
            "VALUES(?,?,?,?,?,?)",
            [
                ('EC-20128A-TAB', 635, 111, 1, '原始說明 A', '2026-09-19T09:00:01'),
                ('EC-30037A-TAB', 769, 222, 0, '原始說明 B', '2026-09-19T09:00:02'),
                ('PART-KEEP', 50, 333, 1, '原始說明 C', '2026-09-19T09:00:03'),
            ],
        )
        db.set_setting('main_part_count', 'before-count')
        self.conn.executemany("INSERT INTO defective_records(part_number,defective_qty,status,created_at) VALUES(?,?,?,?)", [
            ('EC-20128A-TAB', 4, 'open', '2026-09-20T10:00:00'),
            ('EC-20128A-TAB', 5, 'confirmed', '2026-09-23T12:00:00'),
            ('EC-20128A-TAB', 100, 'open', '2026-09-24T00:00:00'),
        ])
        db.start_inventory_count_session(cutoff_at='2026-09-19T10:00:00', cutoff_code='9-12')

    def preview(self):
        return build_st_reconcile_preview(
            str(self.count),
            '2026-09-19T10:00:00',
            cutoff_batch_code='9-12',
            source_filename=self.count.name,
        )

    def commit(self, *, parts=None, token=None):
        if token is None:
            token = self.preview()['preview_token']
        return commit_st_reconcile_stop_loss(
            str(self.count),
            '2026-09-19T10:00:00',
            cutoff_label='9-12',
            source_filename=self.count.name,
            part_numbers=parts or ['EC-20128A-TAB'],
            preview_token=token,
        )

    @staticmethod
    def file_hash(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def test_parser_prefers_fg_and_preserves_exact_tab(self):
        parsed = parse_st_reconcile_file(str(self.count))
        self.assertEqual(parsed['source_columns'], {'book': 'F', 'physical': 'G'})
        self.assertEqual(parsed['rows'][0]['part_number'], 'EC-20128A-TAB')
        self.assertEqual(parsed['rows'][0]['physical_qty'], 771)
        self.assertEqual(parsed['count_date'], '2026-09-23')

    def test_preview_uses_main_ledger_and_st_is_warning_only(self):
        rows = {row['part_number']: row for row in self.preview()['parts']}
        ec = rows['EC-20128A-TAB']
        self.assertEqual(
            [ec[key] for key in ('cutoff_main', 'defect_delta', 'expected_count', 'current_main', 'preserved_delta', 'target_main', 'main_adjustment')],
            [780, -9, 771, 635, -136, 635, 0],
        )
        self.assertEqual(ec['current_st'], 22000)
        other = rows['EC-30037A-TAB']
        self.assertEqual(
            [other[key] for key in ('cutoff_main', 'expected_count', 'physical_qty', 'current_main', 'target_main', 'main_adjustment')],
            [-2848, -2848, 351, 769, 3968, 3199],
        )
        self.assertEqual(other['current_st'], 9236)

    def test_commit_writes_only_selected_main_and_never_st_or_alignment(self):
        st_before = db.get_st_inventory_stock()
        wb = load_workbook(self.main, data_only=False)
        untouched_before = [cell.value for cell in wb.active[4]]
        wb.close()

        result = self.commit(parts=['EC-30037A-TAB'])

        self.assertTrue(Path(result['backup_path']).is_file())
        self.assertEqual(result['parts'][0]['main_before'], 769)
        self.assertEqual(result['parts'][0]['main_after'], 3968)
        self.assertEqual(result['parts'][0]['main_adjustment'], 3199)
        self.assertEqual(db.get_st_inventory_stock(), st_before)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM st_reconcile_alignments').fetchone()[0], 0)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM st_inventory_audit_log').fetchone()[0], 0)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM defective_records WHERE absorbed_by_alignment_id<>0').fetchone()[0], 0)
        self.assertIsNone(db.get_active_inventory_count_session('st'))

        wb = load_workbook(self.main, data_only=False)
        ws = wb.active
        self.assertIn('盤點調整', ws.cell(1, 18).value)
        self.assertEqual([ws.cell(3, col).value for col in (18, 19, 20)], [3199, 0, 351])
        untouched_after = [cell.value for cell in ws[4]]
        self.assertEqual(untouched_after[:17] + untouched_after[20:], untouched_before)
        self.assertEqual(untouched_after[17:20], [None, None, None])
        wb.close()
        self.assertEqual(read_stock(str(self.main))['EC-30037A-TAB'], 3968)
        self.assertEqual(db.get_snapshot_stock()['EC-30037A-TAB'], 3968)

    def test_zero_adjustment_inserts_event_and_recalculates_later_balance(self):
        result = self.commit(parts=['EC-20128A-TAB'])
        self.assertEqual(result['parts'][0]['main_adjustment'], 0)
        self.assertEqual(result['parts'][0]['main_after'], 635)
        wb = load_workbook(self.main, data_only=False)
        ws = wb.active
        self.assertEqual([ws.cell(2, col).value for col in (18, 19, 20, 21, 22, 23)], [0, 0, 780, 0, 145, 635])
        wb.close()

    def test_nonzero_adjustment_recalculates_all_later_balances(self):
        wb = load_workbook(self.count)
        wb.active['G5'] = 770
        wb.save(self.count)
        wb.close()
        result = self.commit(parts=['EC-20128A-TAB'])
        self.assertEqual(result['parts'][0]['main_adjustment'], -1)
        self.assertEqual(result['parts'][0]['main_after'], 634)
        wb = load_workbook(self.main, data_only=False)
        self.assertEqual([wb.active.cell(2, col).value for col in (18, 19, 20, 23)], [0, 1, 779, 634])
        wb.close()

    def test_stale_token_writes_nothing_and_session_stays_active(self):
        token = self.preview()['preview_token']
        wb = load_workbook(self.main)
        wb.active['T2'] = 630
        wb.save(self.main)
        wb.close()
        before = self.file_hash(self.main)
        with self.assertRaisesRegex(StaleReconcilePreviewError, '重新試算'):
            self.commit(parts=['EC-20128A-TAB'], token=token)
        self.assertEqual(self.file_hash(self.main), before)
        self.assertIsNotNone(db.get_active_inventory_count_session('st'))

    def test_formula_in_later_input_is_rejected_before_backup_or_write(self):
        wb = load_workbook(self.main)
        wb.active['R2'] = '=1+1'
        wb.save(self.main)
        wb.close()
        before = self.file_hash(self.main)
        token = self.preview()['preview_token']
        with self.assertRaisesRegex(ValueError, '含公式'):
            self.commit(parts=['EC-20128A-TAB'], token=token)
        self.assertEqual(self.file_hash(self.main), before)
        self.assertFalse((Path(self.tmp.name) / 'backups').exists())

    def test_formula_in_later_balance_is_preserved_and_writes_nothing(self):
        wb = load_workbook(self.main)
        wb.active['T2'] = '=Q2+R2-S2'
        wb.save(self.main)
        wb.close()
        before = self.file_hash(self.main)
        token = self.preview()['preview_token']
        with self.assertRaisesRegex(ValueError, '含公式'):
            self.commit(parts=['EC-20128A-TAB'], token=token)
        self.assertEqual(self.file_hash(self.main), before)
        saved = load_workbook(self.main, data_only=False)
        self.assertEqual(saved.active['T2'].value, '=Q2+R2-S2')
        saved.close()
        self.assertFalse((Path(self.tmp.name) / 'backups').exists())

    def test_snapshot_failure_restores_main_and_keeps_session_active(self):
        before = self.file_hash(self.main)
        token = self.preview()['preview_token']
        snapshot_before = db.capture_inventory_snapshot_state()
        with patch.object(st_reconcile, 'refresh_snapshot_from_main', side_effect=RuntimeError('snapshot failed')):
            with self.assertRaisesRegex(RuntimeError, 'snapshot failed'):
                self.commit(parts=['EC-30037A-TAB'], token=token)
        self.assertEqual(self.file_hash(self.main), before)
        self.assertIsNotNone(db.get_active_inventory_count_session('st'))
        self.assertEqual(db.capture_inventory_snapshot_state(), snapshot_before)

    def test_finish_session_failure_restores_main_and_snapshot(self):
        before = self.file_hash(self.main)
        token = self.preview()['preview_token']
        snapshot_before = db.capture_inventory_snapshot_state()
        with patch.object(db, 'finish_inventory_count_session', return_value=False):
            with self.assertRaisesRegex(RuntimeError, '工作階段完成失敗'):
                self.commit(parts=['EC-30037A-TAB'], token=token)
        self.assertEqual(self.file_hash(self.main), before)
        self.assertIsNotNone(db.get_active_inventory_count_session('st'))
        self.assertEqual(db.capture_inventory_snapshot_state(), snapshot_before)

    def test_preview_rebuild_failure_is_stale_and_writes_nothing(self):
        token = self.preview()['preview_token']
        wb = load_workbook(self.main)
        ws = wb.active
        ws['L1'] = 'changed'
        ws['O1'] = 'changed'
        wb.save(self.main)
        wb.close()
        before = self.file_hash(self.main)
        with self.assertRaisesRegex(StaleReconcilePreviewError, '重新試算'):
            self.commit(parts=['EC-20128A-TAB'], token=token)
        self.assertEqual(self.file_hash(self.main), before)
        self.assertIsNotNone(db.get_active_inventory_count_session('st'))

    def test_missing_part_anchor_or_selection_writes_nothing(self):
        token = self.preview()['preview_token']
        before = self.file_hash(self.main)
        with self.assertRaisesRegex(ValueError, '至少勾選'):
            commit_st_reconcile_stop_loss(
                str(self.count), '2026-09-19T10:00:00', cutoff_label='9-12',
                part_numbers=[], preview_token=token,
            )
        with self.assertRaisesRegex(ValueError, '不可對帳'):
            self.commit(parts=['MISSING'], token=token)
        self.assertEqual(self.file_hash(self.main), before)

    def test_api_batch_commit_rejects_empty_part_numbers_before_read(self):
        upload = UploadFile(filename=self.count.name, file=io.BytesIO(self.count.read_bytes()))
        with patch.object(reconcile_router, '_resolve_cutoff', return_value=('2026-09-19T10:00:00', '9-12')):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(reconcile_router.commit_st_reconcile(
                    cutoff_batch_code='9-12',
                    file=upload,
                    part_numbers=None,
                    preview_token='token',
                ))
        self.assertEqual(raised.exception.status_code, 400)
        self.assertIn('至少勾選', raised.exception.detail)

    def test_api_activity_log_failure_does_not_turn_success_into_failure(self):
        upload = UploadFile(filename=self.count.name, file=io.BytesIO(self.count.read_bytes()))
        expected = {
            'ok': True,
            'session_completed': True,
            'summary': {'session_id': 1, 'part_count': 1},
        }
        with (
            patch.object(reconcile_router, '_resolve_cutoff', return_value=('2026-09-19T10:00:00', '9-12')),
            patch.object(reconcile_router, 'commit_st_reconcile_stop_loss', return_value=expected),
            patch.object(db, 'log_activity', side_effect=RuntimeError('log failed')),
            patch.object(reconcile_router.log, 'exception'),
        ):
            result = asyncio.run(reconcile_router.commit_st_reconcile(
                cutoff_batch_code='9-12',
                file=upload,
                part_numbers=['EC-20128A-TAB'],
                preview_token='token',
            ))
        self.assertEqual(result, expected)

    def test_api_stale_preview_returns_409(self):
        upload = UploadFile(filename=self.count.name, file=io.BytesIO(self.count.read_bytes()))
        with (
            patch.object(reconcile_router, '_resolve_cutoff', return_value=('2026-09-19T10:00:00', '9-12')),
            patch.object(
                reconcile_router,
                'commit_st_reconcile_stop_loss',
                side_effect=StaleReconcilePreviewError('主檔已變更，請重新試算'),
            ),
        ):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(reconcile_router.commit_st_reconcile(
                    cutoff_batch_code='9-12',
                    file=upload,
                    part_numbers=['EC-20128A-TAB'],
                    preview_token='token',
                ))
        self.assertEqual(raised.exception.status_code, 409)
        self.assertIn('重新試算', raised.exception.detail)

    def test_api_legacy_date_commit_does_not_require_preview_token(self):
        upload = UploadFile(filename=self.count.name, file=io.BytesIO(self.count.read_bytes()))
        expected = {
            'ok': True,
            'session_completed': True,
            'summary': {'session_id': 1, 'part_count': 1},
        }
        with (
            patch.object(reconcile_router, '_resolve_cutoff', return_value=('2026-09-19T10:00:00', '')),
            patch.object(reconcile_router, 'commit_st_reconcile_stop_loss', return_value=expected) as commit_mock,
            patch.object(db, 'log_activity'),
        ):
            result = asyncio.run(reconcile_router.commit_st_reconcile(
                cutoff_date='2026-09-19',
                file=upload,
                part_numbers=['EC-20128A-TAB'],
                preview_token='',
            ))
        self.assertEqual(result, expected)
        self.assertEqual(commit_mock.call_args.kwargs['preview_token'], '')

    def test_aggregate_only_falls_back_and_filename_date_is_detected(self):
        wb = load_workbook(self.count)
        ws = wb.active
        ws['A1'] = None
        for row in range(5, 8):
            ws.cell(row, 6).value = None
            ws.cell(row, 7).value = None
        wb.save(self.count)
        wb.close()
        parsed = parse_st_reconcile_file(str(self.count))
        self.assertEqual(parsed['source_columns'], {'book': 'V', 'physical': 'U'})
        self.assertEqual(parsed['rows'][0]['physical_qty'], 33179)
        self.assertEqual(parsed['count_date'], '2026-09-23')

    def test_same_batch_rightmost_valid_and_blank_falls_back(self):
        wb = load_workbook(self.main)
        ws = wb.active
        ws['Q1'] = 'MODEL-X'
        ws['Q2'] = None
        wb.save(self.main)
        stock = read_batch_stock(str(self.main), '9-12')['EC-20128A-TAB']
        self.assertEqual(stock, {'cutoff_main': 800, 'current_main': 635})
        ws['N2'] = None
        ws['P2'] = 99999
        wb.save(self.main)
        wb.close()
        stock = read_batch_stock(str(self.main), '9-12')['EC-20128A-TAB']
        self.assertEqual(stock['cutoff_main'], 890)


if __name__ == '__main__':
    unittest.main()
