"""v2026.11.24.6：一鍵調撥不撥客供料 -TAB（R-G2）；Invoice 待開明細排除已結案 S/C（R-I）。全部假連線，不碰正航。"""
from __future__ import annotations

import time
import unittest
from unittest.mock import Mock, patch

from app.services import erp_invoice as inv
from app.services import erp_purchase as ep
from app.services import erp_transfer as et


def _plan(rows_by_key):
    return {'target': 'CHIComp13', 'date': 20261124, 'warnings': [], 'stock_source': 'CHIComp03',
            'batches': [{'key': k, 'code': k, 'already': None, 'rows': rows,
                         'totals': {c: sum(r[c] for r in rows) for c in ('pending', 'transfer', 'short')}}
                        for k, rows in rows_by_key.items()]}


def _row(part, transfer, ware='001'):
    return dict(part=part, ware_out=ware, pending=transfer, transfer=transfer, short=0, cost=1)


class TransferCreateRejectsTabTests(unittest.TestCase):
    def _create(self, plan, keys):
        token = et.digest(plan)
        with patch.dict(et._PREVIEWS, {token: (time.monotonic(), plan)}, clear=True), \
                patch.object(et, 'resolve_target', return_value=('CHIComp13', {})), \
                patch.object(et.db, 'successful_erp_transfers', return_value=[]), \
                patch.object(et, '_safe_log', return_value=1) as log, \
                patch.object(et, '_write', return_value=dict(move_no='RM-1')) as write:
            try:
                return et.create(dict(batches=keys, client_token='tok', preview_token=token)), write, log
            except ep.PoValidationError as exc:
                return exc, write, log

    def test_preview_token_with_tab_line_is_rejected_before_write(self):
        plan = _plan({'12-10': [_row('P', 10), _row('EC-30059A-TAB', 5, '002')]})
        exc, write, log = self._create(plan, ['12-10'])
        self.assertIsInstance(exc, ep.PoValidationError)
        self.assertIn('料號 EC-30059A-TAB 為客供料（-TAB），不走一鍵調撥，已拒絕寫入', str(exc))
        write.assert_not_called()
        log.assert_not_called()

    def test_tab_line_rejected_case_and_space_insensitive_even_from_001(self):
        plan = _plan({'A': [_row(' uc-m0003a-tab ', 5)]})
        exc, write, _ = self._create(plan, ['A'])
        self.assertIsInstance(exc, ep.PoValidationError)
        self.assertIn('客供料', str(exc))
        write.assert_not_called()

    def test_002_source_rejected(self):
        plan = _plan({'A': [_row('P', 5, '002')]})
        exc, write, _ = self._create(plan, ['A'])
        self.assertIn('來源倉 002 不可調撥，只能從 001 倉撥出', str(exc))
        write.assert_not_called()

    def test_unselected_batch_with_tab_does_not_block(self):
        plan = _plan({'A': [_row('P', 5)], 'B': [_row('IC-M24C02-WMN6TP-TAB', 3, '002')]})
        result, write, _ = self._create(plan, ['A'])
        self.assertEqual([r['batch_key'] for r in result['results']], ['A'])
        self.assertEqual(write.call_count, 1)

    def test_empty_batch_cannot_be_confirmed(self):
        plan = _plan({'12-12': []})
        exc, write, log = self._create(plan, ['12-12'])
        self.assertEqual(str(exc), '批次 12-12 沒有可調撥的料，不能送出')
        write.assert_not_called()
        log.assert_not_called()

    def test_write_path_relocks_and_rejects_tab_from_fresh_plan(self):
        # 即使重算後的 plan 出現 -TAB 列，execute_bill 也在任何 SQL 之前拒絕。
        cur = Mock()
        with patch.object(et, 'ensure_masters') as masters, patch.object(et, 'insert') as insert:
            with self.assertRaisesRegex(ep.PoValidationError, '客供料'):
                et.execute_bill(cur, 'CHIComp13', dict(rows=[_row('EC-30059A-TAB', 1, '001')]), 20261124)
        masters.assert_not_called()
        insert.assert_not_called()
        cur.execute.assert_not_called()


class _Cur:
    def __init__(self, rows=()):
        self.rows, self.sql, self.params = list(rows), '', ()

    def execute(self, sql, params=()):
        self.sql, self.params = sql, params

    def fetchall(self):
        return self.rows


class InvoiceClosedScTests(unittest.TestCase):
    def test_open_lines_require_bill_status_open(self):
        cur = _Cur()
        conn = Mock()
        conn.cursor.return_value = cur
        with patch.object(inv, 'resolve_target', return_value=('CHIComp13', {})), \
                patch.object(ep, '_connect_write', return_value=conn), patch.object(ep, '_check_db_name'), \
                patch.object(ep, '_close_quietly'):
            inv.list_open_lines()
        self.assertIn('JOIN TraBillMain m ON m.Flag = s.Flag AND m.BillNo = s.BillNo', cur.sql)
        self.assertIn('m.BillStatus = 0', cur.sql)
        self.assertIn('s.QtyRemain > 0', cur.sql)
        self.assertEqual(cur.sql.count('%s'), len(cur.params))

    def test_preview_rejects_closed_sc(self):
        req = {'lines': [{'sc_no': 'MO-2503001', 'row_no': 4, 'qty': 1, 'price': None}]}
        main = {'Flag': 2, 'BillNo': 'MO-2503001', 'CustID': 'C1', 'BillStatus': 1}
        with patch.object(inv.er, 'load_cost_params', return_value={}), \
                patch.object(inv, '_one_row', return_value=main):
            with self.assertRaisesRegex(inv.InvoiceValidationError, 'S/C MO-2503001 已結案，不能再開 Invoice'):
                inv._load_context(_Cur(), 'CHIComp13', req, lock=False)

    def test_open_sc_passes_status_check(self):
        req = {'lines': [{'sc_no': 'MO-1', 'row_no': 9, 'qty': 1, 'price': None}]}
        main = {'Flag': 2, 'BillNo': 'MO-1', 'CustID': 'C1', 'BillStatus': 0}
        with patch.object(inv.er, 'load_cost_params', return_value={}), \
                patch.object(inv, '_one_row', return_value=main), patch.object(ep, '_rows', return_value=[]):
            # 狀態檢查通過後才走到「沒有第 9 列」。
            with self.assertRaisesRegex(inv.InvoiceValidationError, 'MO-1 沒有第 9 列'):
                inv._load_context(_Cur(), 'CHIComp13', req, lock=False)


if __name__ == '__main__':
    unittest.main()
