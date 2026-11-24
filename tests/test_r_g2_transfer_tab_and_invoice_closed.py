"""v2026.11.24.6：Invoice 待開明細排除已結案 S/C（R-I）。一鍵調撥排除 -TAB（R-G2）已於 v2026.11.24.7 撤回。全部假連線，不碰正航。"""
from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from app.services import erp_invoice as inv
from app.services import erp_purchase as ep


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
