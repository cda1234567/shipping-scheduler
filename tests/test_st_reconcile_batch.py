from __future__ import annotations

import sqlite3
import unittest
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from openpyxl import Workbook, load_workbook

from app import database as db
from app.services.st_reconcile import build_st_reconcile_preview, commit_st_reconcile_stop_loss, parse_st_reconcile_file
from app.services.main_reader import read_batch_stock


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
        self.count = Path(self.tmp.name) / 'count_2026-9-23.xlsx'
        wb = Workbook()
        ws = wb.active
        ws['A1'] = '盤點日期 2026/9/23'
        for col, value in {4: 'consign invoice NO', 6: '辰尚庫存', 7: '庚霖庫存 當下實際', 21: '實際庫存總和', 22: '生產結餘'}.items():
            ws.cell(3, col, value)
        for col, value in {4: 'EC-20128A-TAB', 6: 780, 7: 771, 21: 33179, 22: 33200}.items():
            ws.cell(5, col, value)
        wb.save(self.count)
        wb.close()
        self.main = Path(self.tmp.name) / 'main.xlsx'
        wb = Workbook()
        ws = wb.active
        ws.append(['料號', '廠商', 'MOQ', None, None, None, None, '盤點', '9-11', None, '結存', '9-12', None, '結存', '9-12', None, '結存', '9-13', None, '結存'])
        ws.append(['EC-20128A-TAB', '', 1000, None, None, None, None, 900, 0, 10, 890, 0, 90, 800, 0, 20, 780, 0, 145, 635])
        ws.append(['EC-20128A', '', 999, None, None, None, None, 200, 0, 1, 199, 0, 1, 198, 0, 1, 197, 0, 1, 196])
        wb.save(self.main)
        wb.close()
        db.set_setting('main_file_path', str(self.main))
        self.conn.execute("INSERT INTO st_inventory_snapshot(part_number,stock_qty) VALUES('EC-20128A-TAB',635),('EC-20128A',196)")
        self.conn.executemany("INSERT INTO defective_records(part_number,defective_qty,status,created_at) VALUES(?,?,?,?)", [
            ('EC-20128A-TAB', 4, 'open', '2026-09-20T10:00:00'),
            ('EC-20128A-TAB', 5, 'confirmed', '2026-09-23T12:00:00'),
            ('EC-20128A-TAB', 100, 'open', '2026-09-24T00:00:00'),
            ('EC-20128A', 30, 'open', '2026-09-20T10:00:00'),
        ])
        db.start_inventory_count_session(cutoff_at='2026-09-19T10:00:00', cutoff_code='9-12')

    def preview(self):
        return build_st_reconcile_preview(str(self.count), '2026-09-19T10:00:00', cutoff_batch_code='9-12')

    def commit(self, **kwargs):
        return commit_st_reconcile_stop_loss(str(self.count), '2026-09-19T10:00:00', cutoff_label='9-12', **kwargs)

    def test_parser_prefers_fg_and_preserves_exact_tab(self):
        parsed = parse_st_reconcile_file(str(self.count))
        self.assertEqual(parsed['source_columns'], {'book': 'F', 'physical': 'G'})
        self.assertEqual(parsed['rows'][0]['part_number'], 'EC-20128A-TAB')
        self.assertEqual(parsed['rows'][0]['physical_qty'], 771)
        self.assertEqual(parsed['count_date'], '2026-09-23')

    def test_batch_formula_exact_match_and_second_commit_idempotence(self):
        report = self.preview()
        row = report['parts'][0]
        self.assertEqual([row[k] for k in ['cutoff_main', 'defect_delta', 'expected_count', 'current_main', 'preserved_delta', 'target_current']], [780, -9, 771, 635, -136, 635])
        result = self.commit(part_numbers=['EC-20128A-TAB'])
        self.assertEqual(result['parts'][0]['aligned_qty'], 635)
        self.assertEqual(result['summary']['absorbed_defective_records'], 2)
        self.assertEqual(db.get_st_inventory_stock()['EC-20128A'], 196)
        records = self.conn.execute('SELECT absorbed_by_alignment_id FROM defective_records ORDER BY id').fetchall()
        self.assertEqual([bool(r[0]) for r in records], [True, True, False, False])
        again = self.commit(part_numbers=['EC-20128A-TAB'])
        self.assertEqual(again['adjustments'][0]['adjust_qty'], 0)
        self.assertEqual(db.get_st_inventory_stock()['EC-20128A-TAB'], 635)

    def test_missing_part_or_batch_writes_nothing(self):
        for kwargs in [{'part_numbers': ['MISSING']}, {'cutoff_label': '9-99'}]:
            with self.assertRaises(ValueError):
                commit_st_reconcile_stop_loss(str(self.count), '2026-09-19T10:00:00', **({'cutoff_label': '9-12'} | kwargs))
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM st_reconcile_alignments').fetchone()[0], 0)

    def test_missing_date_writes_nothing(self):
        wb = load_workbook(self.count)
        wb.active['A1'] = None
        undated = Path(self.tmp.name) / 'undated.xlsx'
        wb.save(undated)
        wb.close()
        with self.assertRaisesRegex(ValueError, '盤點日期'):
            commit_st_reconcile_stop_loss(str(undated), '2026-09-19T10:00:00', cutoff_label='9-12')
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM st_reconcile_alignments').fetchone()[0], 0)

    def test_aggregate_only_falls_back_and_filename_date_is_detected(self):
        wb = load_workbook(self.count)
        ws = wb.active
        ws['A1'] = None
        ws['F5'] = None
        ws['G5'] = None
        wb.save(self.count)
        wb.close()
        parsed = parse_st_reconcile_file(str(self.count))
        self.assertEqual(parsed['source_columns'], {'book': 'V', 'physical': 'U'})
        self.assertEqual(parsed['rows'][0]['physical_qty'], 33179)
        self.assertEqual(parsed['count_date'], '2026-09-23')

    def test_alias_only_when_exact_absent_from_main_and_system(self):
        wb = load_workbook(self.main)
        wb.active.delete_rows(2)
        wb.save(self.main)
        wb.close()
        # 系統仍有 exact 時不能借用另一支料的主檔庫存。
        report = self.preview()
        self.assertEqual(report['parts'], [])
        self.assertEqual(report['uncovered_parts'][0]['part_number'], 'EC-20128A-TAB')
        with self.assertRaises(ValueError):
            self.commit(part_numbers=['EC-20128A-TAB'])
        with self.assertRaises(ValueError):
            self.commit()
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM st_reconcile_alignments').fetchone()[0], 0)
        self.conn.execute("DELETE FROM st_inventory_snapshot WHERE part_number='EC-20128A-TAB'")
        row = self.preview()['parts'][0]
        self.assertEqual(row['part_number'], 'EC-20128A')
        self.assertEqual(row['cutoff_main'], 197)
        self.assertEqual(row['defect_delta'], -30)

    def test_unknown_before_known_part_is_reported_without_blocking_preview(self):
        wb = load_workbook(self.count)
        ws = wb.active
        ws.insert_rows(5)
        ws['D5'], ws['F5'], ws['G5'] = 'AAA-UNKNOWN-TAB', 10, 7
        wb.save(self.count)
        wb.close()
        report = self.preview()
        row = report['parts'][0]
        self.assertEqual([row[k] for k in ['cutoff_main', 'defect_delta', 'expected_count', 'current_main', 'preserved_delta', 'target_current']], [780, -9, 771, 635, -136, 635])
        unknown = report['uncovered_parts'][0]
        self.assertEqual(unknown['part_number'], 'AAA-UNKNOWN-TAB')
        self.assertEqual(unknown['source_part_numbers'], ['AAA-UNKNOWN-TAB'])
        self.assertIn('結存', unknown['reason'])
        for parts in [['AAA-UNKNOWN-TAB'], ['EC-20128A-TAB', 'AAA-UNKNOWN-TAB']]:
            with self.assertRaisesRegex(ValueError, '不可對帳'):
                self.commit(part_numbers=parts)
            self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM st_reconcile_alignments').fetchone()[0], 0)
            self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM st_inventory_audit_log').fetchone()[0], 0)
            self.assertEqual(db.get_st_inventory_stock()['EC-20128A-TAB'], 635)
        result = self.commit(part_numbers=['EC-20128A-TAB'])
        self.assertEqual(result['parts'][0]['aligned_qty'], 635)

    def test_exact_and_tab_both_present_remain_separate(self):
        wb = load_workbook(self.count)
        ws = wb.active
        ws['D6'], ws['F6'], ws['G6'] = 'EC-20128A', 197, 167
        wb.save(self.count)
        wb.close()
        rows = {row['part_number']: row for row in self.preview()['parts']}
        self.assertEqual(set(rows), {'EC-20128A', 'EC-20128A-TAB'})
        self.assertEqual(rows['EC-20128A-TAB']['physical_qty'], 771)
        self.assertEqual(rows['EC-20128A']['physical_qty'], 167)
        self.commit(part_numbers=['EC-20128A-TAB'])
        self.assertEqual(db.get_st_inventory_stock()['EC-20128A'], 196)

    def test_same_batch_rightmost_valid_and_all_blank_previous_ending(self):
        wb = load_workbook(self.main)
        ws = wb.active
        # 實際主檔 ending 表頭是機種，只有三欄組起始處有批次碼。
        ws['Q1'] = 'MODEL-X'
        ws['Q2'] = None
        wb.save(self.main)
        stock = read_batch_stock(str(self.main), '9-12')['EC-20128A-TAB']
        self.assertEqual(stock, {'cutoff_main': 800, 'current_main': 635})
        ws['N2'] = None
        ws['P2'] = 99999  # 不可把用量当結存
        wb.save(self.main)
        wb.close()
        stock = read_batch_stock(str(self.main), '9-12')['EC-20128A-TAB']
        self.assertEqual(stock['cutoff_main'], 890)

    def test_lock_time_anchor_and_zero_target_are_preserved(self):
        wb = load_workbook(self.count)
        wb.active['G5'] = 136
        wb.save(self.count)
        wb.close()
        result = self.commit(part_numbers=['EC-20128A-TAB'])
        self.assertEqual(result['parts'][0]['aligned_qty'], 0)
        session = db.get_active_inventory_count_session('st')
        anchor = db.get_latest_st_reconcile_anchor('9999-12-31')
        self.assertEqual(anchor['aligned_at'], session['started_at'])
        self.assertEqual(anchor['baseline_qty']['EC-20128A-TAB'], 0)

    def test_commit_recalculates_changed_main_and_defects(self):
        self.preview()
        wb = load_workbook(self.main)
        wb.active['T2'] = 630
        wb.save(self.main)
        wb.close()
        self.conn.execute("INSERT INTO defective_records(part_number,defective_qty,status,created_at) VALUES('EC-20128A-TAB',2,'open','2026-09-22T10:00:00')")
        result = self.commit(part_numbers=['EC-20128A-TAB'])
        self.assertEqual(result['parts'][0]['expected_count'], 769)
        self.assertEqual(result['parts'][0]['aligned_qty'], 632)
        self.assertEqual(result['summary']['absorbed_defective_records'], 3)


if __name__ == '__main__':
    unittest.main()
