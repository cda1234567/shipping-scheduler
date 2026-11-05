"""不連線 ERP；驗證調撥規劃、樣本、交易與重送保護。"""
import copy
import json
import os
import unittest
from unittest.mock import Mock, patch
from decimal import Decimal

from app.services import erp_transfer as et, erp_purchase as ep, st_pending_transfer as st


def sample_plan():
    return {'target': 'CHIComp13', 'date': 20261105, 'warnings': [], 'batches': [
        {'key': '2026/11-1', 'code': '11-1', 'already': None,
         'rows': [{'part': 'P', 'pending': 100, 'transfer': 100, 'ware_out': '001', 'stock_src': 150, 'stock_target': 150}]}]}


class TransferTests(unittest.TestCase):
    def test_number_max_prefixed_unprefixed_locked(self):
        cur = Mock()
        cur.fetchall.return_value = [('RM-20261105002',), ('20261105008',), ('RM-20261105999x',), ('20261104099',)]
        self.assertEqual(et.next_number(cur, 20261105), 'RM-20261105009')
        sql, args = cur.execute.call_args.args
        self.assertIn('UPDLOCK, HOLDLOCK', sql)
        self.assertIn('stkMoveMain', sql)
        self.assertNotIn('stkBillMain', sql)
        self.assertEqual(args, ('RM-20261105%', '20261105%'))
        cur.fetchall.return_value = []
        self.assertEqual(et.next_number(cur, 20261105), 'RM-20261105001')

    def test_cost_modes(self):
        product = {'CAvgCost': 21.123456}
        out = {'Cost': 12.123456, 'TotalCost': 500}
        incoming = {'Quantity': 10, 'TotalCost': 200}
        whole = et.cost_values(product, out, incoming, 1000, dict(cost_by_ware=False, digits=3, dot=True))
        self.assertEqual(whole, dict(cost=21.123, amount=21123, avg=21.123456, avg_out=21.123456))
        ware = et.cost_values(product, out, incoming, 1000, dict(cost_by_ware=True, digits=5, dot=False))
        self.assertEqual(ware['cost'], 12.123456)
        self.assertEqual(ware['amount'], 12123)
        self.assertEqual(ware['avg'], 12323/1010)
        self.assertEqual(incoming, {'Quantity': 10, 'TotalCost': 200})
        self.assertEqual(et.cost_values({'CAvgCost': 1.5}, {}, {}, 1, dict(cost_by_ware=False, digits=3, dot=True))['amount'], 2)

    def test_comp03_sample_nondefaults(self):
        # RM-20261104001 第一行實際 SELECT *；主檔 SumQty=2100（含其他行），DataVer=2。
        row = dict(part='OC-10793B', name='T356789iu Plastic Faceplate 面板', transfer=1000,
                   cost=21, amount=21000, avg=21, avg_out=21)
        with patch.dict(os.environ, {'ERP_PO_MAKER': 'Andy', 'ERP_PO_MAKER_ID': '017'}):
            built = et.build_rows('RM-20261104001', 20261104, [row])
        expected = {
            'stkMoveMain': dict(Flag=400, MoveNO='RM-20261104001', MoveDate=20261104, Maker='Andy', MakerID='017', SumQty=1000, DataVer=1, HasCheck=True),
            'stkMoveSub': dict(Flag=400, MoveNO='RM-20261104001', RowNo=1, SerNo=1, MoveDate=20261104, Cost=21),
            'comProdRec': dict(Flag=400, BillNO='RM-20261104001', RowNO=1, SerNO=1, BillDate=20261104,
                ProdID=row['part'], ProdName=row['name'], CostAvg=21, CostAvgOut=21, Quantity=1000,
                MLPrice=21, MLAmount=21000, WareID='015', WareOut='001', QtyRemain=1, NeedUpdate=True,
                HasCheck=True, SQuantity=1, UnitRelation=1, EQuantity=1000, IsOdd=True)}
        for table in expected:
            actual = {k: v for k, v in built[table][0].items() if v not in ('', 0, False, None)}
            self.assertEqual(actual, expected[table])
            columns = [dict(name=k, identity=False, computed=False) for k in built[table][0]]
            ep.build_insert(table, columns, built[table][0])
            with self.assertRaises(ep.PoSchemaError):
                ep.build_insert(table, columns + [dict(name='Unexpected', identity=False, computed=False)], built[table][0])
        self.assertEqual(built['comProdRec'][0]['EUnitRelation'], 0)

    def test_production_gate_independent(self):
        for env in ({'ERP_ALLOW_PROD_WRITE_CHICOMP03': '1', 'ERP_RECEIVE_TARGET': 'CHIComp03'},
                    {'ERP_TRANSFER_TARGET': 'CHIComp03', 'ERP_ALLOW_PROD_WRITE_CHICOMP03': '1'},
                    {'ERP_TRANSFER_TARGET': 'CHIComp03', 'ERP_ALLOW_PROD_TRANSFER_CHICOMP03': 'true'}):
            with patch.dict(os.environ, env, clear=True), patch.object(ep, 'write_credentials', return_value={'database': 'CHIComp13'}):
                if 'ERP_TRANSFER_TARGET' in env:
                    with self.assertRaises(ep.PoValidationError): et.resolve_target()
                else:
                    self.assertEqual(et.resolve_target()[0], 'CHIComp13')
        env = {'ERP_TRANSFER_TARGET': 'CHIComp03', 'ERP_ALLOW_PROD_TRANSFER_CHICOMP03': '1',
               'ERP_SQL_WRITE_USER_CHICOMP03': 'dedicated', 'ERP_SQL_WRITE_PASSWORD_CHICOMP03': 'secret'}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(et.resolve_target()[0], 'CHIComp03')
        with patch.dict(os.environ, {'ERP_TRANSFER_TARGET': 'CHIComp03', 'ERP_ALLOW_PROD_TRANSFER_CHICOMP03': '1',
                                    'ERP_SQL_TEST_USER': 'test', 'ERP_SQL_TEST_PASSWORD': 'test'}, clear=True):
            with self.assertRaises(ep.PoValidationError): et.resolve_target()
        with patch.dict(os.environ, {'ERP_TRANSFER_TARGET': 'CHIComp99'}, clear=True):
            with self.assertRaises(ep.PoValidationError): et.resolve_target()

    def test_fifo_conversions_customer_parts_and_no_double_count(self):
        batches = [dict(key='A', deductions=[('OLD', 100), ('P-TAB', 500)]),
                   dict(key='B', deductions=[('P', 100)]), dict(key='C', deductions=[('P', 20)])]
        out = st.allocate_batches(batches, {'OLD': 120}, [], {'OLD': 'P'})
        self.assertEqual(out[0]['rows'], [{'part': 'P-TAB', 'pending': 500, 'ware_out': '002'}])
        self.assertEqual(out[1]['rows'], [{'part': 'P', 'pending': 80, 'ware_out': '001'}])
        self.assertEqual(out[2]['rows'], [{'part': 'P', 'pending': 20, 'ware_out': '001'}])
        website = [dict(batch_key='A', target_db='CHIComp03', lines_json=json.dumps([dict(part='P', qty=100)]))]
        self.assertEqual(st.allocate_batches(batches, {'P': 120}, website, {'OLD': 'P'}), out)
        website[0]['target_db'] = 'CHIComp13'
        out = st.allocate_batches(batches, {}, website, {'OLD': 'P'})
        self.assertEqual(out[0]['rows'], [{'part': 'P-TAB', 'pending': 500, 'ware_out': '002'}])
        self.assertEqual(out[1]['rows'][0]['pending'], 100)

    def test_plan_caps_final_part_stock_and_already(self):
        info = {'warnings': [], 'batches': [dict(key='A', rows=[dict(part='P', pending=100)]),
                                           dict(key='B', rows=[dict(part='P', pending=100)]),
                                           dict(key='C', rows=[dict(part='P', pending=10)])]}
        def read(cur, sql, args):
            if 'comProduct' in sql: return dict(ProdName='品名', CAvgCost=21)
            return dict(Quantity=900 if args[-1] == '001' else 0, TotalCost=0)
        with patch.object(et, 'source_stock', return_value={('P', '001'): 150}), patch.object(st, 'batch_breakdown', return_value=copy.deepcopy(info)), patch.object(et.er, 'load_cost_params', return_value=dict(cost_by_ware=False, digits=3, dot=True)), patch.object(et, 'one', side_effect=read), patch.object(et.db, 'successful_erp_transfers', return_value=[]):
            result = et.plan(Mock(), 'CHIComp13', lock=True)
        self.assertEqual([b['rows'][0]['transfer'] for b in result['batches']], [100, 50, 0])
        self.assertEqual(result['batches'][1]['rows'][0]['short'], 50)
        self.assertEqual(result['batches'][0]['rows'][0]['stock_target'], 900)
        self.assertEqual(result['stock_source'], 'CHIComp03')
        self.assertEqual(result['batches'][2]['totals']['transfer'], 0)
        prior = dict(batch_key='A', move_no='RM-1')
        with patch.object(et, 'source_stock', return_value={('P', '001'): 150}), patch.object(st, 'batch_breakdown', return_value=copy.deepcopy(info)), patch.object(et.er, 'load_cost_params', return_value=dict(cost_by_ware=False, digits=3, dot=True)), patch.object(et, 'one', side_effect=read), patch.object(et.db, 'successful_erp_transfers', return_value=[prior]):
            result = et.plan(Mock(), 'CHIComp13')
        self.assertEqual(result['batches'][0]['rows'][0]['transfer'], 0)
        self.assertEqual(result['batches'][0]['already']['move_no'], 'RM-1')

    def test_tab_allocation_uses_only_002_transfers(self):
        batches = [dict(key='A', deductions=[('P-TAB', 100), ('P', 50)])]
        out = st.allocate_batches(batches, {'P-TAB': 1000, 'P': 10}, [], {},
                                  tab_transfers={'P-TAB': 30, 'P': 1000})
        self.assertEqual(out[0]['rows'], [dict(part='P', pending=40, ware_out='001'),
                                          dict(part='P-TAB', pending=70, ware_out='002')])

    def test_tab_plan_consumes_company3_pool(self):
        info = dict(warnings=[], batches=[dict(key=k, rows=[dict(part='P-TAB', ware_out='002', pending=40)]) for k in ('A', 'B')])
        def read(cur, sql, args):
            return dict(ProdName='客供料', CAvgCost=2) if 'comProduct' in sql else dict(Quantity=999)
        with patch.object(st, 'batch_breakdown', return_value=info), patch.object(et, 'source_stock', return_value={('P-TAB', '002'): 60}) as source, patch.object(et, 'one', side_effect=read), patch.object(et.db, 'successful_erp_transfers', return_value=[]), patch.object(et.er, 'load_cost_params', return_value=dict(cost_by_ware=False, digits=3, dot=True)):
            result = et.plan(Mock(), 'CHIComp13')
        source.assert_called_once_with([('P-TAB', '002'), ('P-TAB', '002')])
        rows = [b['rows'][0] for b in result['batches']]
        self.assertEqual([r['transfer'] for r in rows], [40, 20])
        self.assertEqual([r['stock_src'] for r in rows], [60, 20])
        self.assertEqual([r['stock_target'] for r in rows], [999, 999])

    def test_build_rows_mixes_source_wares_on_same_bill(self):
        rows = [dict(part=p, name=p, ware_out=w, transfer=10, cost=0, amount=0, avg=0, avg_out=0)
                for p, w in [('P', '001'), ('P-TAB', '002')]]
        built = et.build_rows('RM-1', 20261105, rows)
        self.assertEqual(len(built['stkMoveMain']), 1)
        self.assertEqual([r['MoveNO'] for r in built['stkMoveSub']], ['RM-1', 'RM-1'])
        self.assertEqual([r['BillNO'] for r in built['comProdRec']], ['RM-1', 'RM-1'])
        self.assertEqual([r['WareOut'] for r in built['comProdRec']], ['001', '002'])
        self.assertEqual([r['RowNO'] for r in built['comProdRec']], [1, 2])

    def test_source_stock_rejects_other_company(self):
        from contextlib import contextmanager
        conn = Mock()
        conn.cursor.return_value.fetchone.return_value = ('CHIComp13',)
        @contextmanager
        def session(): yield conn
        with patch.object(et.low_stock, '_erp_session', session):
            with self.assertRaisesRegex(ep.PoValidationError, '庫存來源不是公司3，已中止'):
                et.source_stock([('P', '001')])
        self.assertEqual(conn.cursor.return_value.execute.call_count, 1)

    def test_source_stock_normalizes_chunks_and_missing(self):
        from contextlib import contextmanager
        conn = Mock()
        cur = conn.cursor.return_value
        cur.fetchone.return_value = ('CHIComp03',)
        cur.fetchall.side_effect = [[(' p0 ', '001', 12), ('P1', '001', -5)], [], [('P-TAB', '002', 8)]]
        @contextmanager
        def session(): yield conn
        with patch.object(et.low_stock, '_erp_session', session):
            stock = et.source_stock([(f'P{i}', '001') for i in range(501)] + [('P-TAB', '002')])
        self.assertEqual(stock[('P0', '001')], 12)
        self.assertEqual(stock[('P1', '001')], 0)
        self.assertEqual(stock[('P2', '001')], 0)
        self.assertEqual(stock[('P-TAB', '002')], 8)
        self.assertEqual(len(cur.execute.call_args_list), 4)
        self.assertEqual(len(cur.execute.call_args_list[1].args[1]), 501)

    def test_changed_plan_rolls_back_without_insert(self):
        expected = sample_plan()
        changed = copy.deepcopy(expected)
        changed['batches'][0]['rows'][0]['stock_src'] = 149
        conn = Mock()
        with patch.object(ep, '_connect_write', return_value=conn), patch.object(ep, '_check_db_name'), patch.object(et, 'plan', return_value=changed), patch.object(et, 'execute_bill') as write:
            with self.assertRaisesRegex(ep.PoValidationError, '資料已變動，請重新預覽'):
                et._write('CHIComp13', {}, '2026/11-1', expected, True)
            write.assert_not_called()
            conn.commit.assert_not_called()
            conn.rollback.assert_called()

    def test_transaction_collision_retry_commit_and_db_check(self):
        expected = sample_plan()
        conn = Mock()
        with patch.object(ep, '_connect_write', return_value=conn), patch.object(ep, '_check_db_name') as check, patch.object(et, 'plan', return_value=expected), patch.object(et, 'execute_bill', side_effect=[Exception('duplicate'), dict(move_no='RM-2')]), patch.object(et.er, 'is_duplicate_key_error', return_value=True):
            self.assertEqual(et._write('CHIComp13', {}, '2026/11-1', expected), dict(move_no='RM-2'))
            conn.rollback.assert_called_once()
            conn.commit.assert_called_once()
            self.assertEqual(check.call_count, 2)

    def test_duplicate_returns_move_number(self):
        rec = dict(batch_key='A', client_token='token', status='ok', move_no='RM-1')
        with patch.object(et, 'resolve_target', return_value=('CHIComp13', {})), patch.object(et.db, 'successful_erp_transfers', return_value=[rec]), patch.object(et, '_write') as write:
            with self.assertRaisesRegex(ep.DuplicateSubmitError, 'RM-1'):
                et.create(dict(batches=['A'], client_token='token', preview_token='bad'))
            write.assert_not_called()

    def test_recheck_deleted_select_only(self):
        rec = dict(target_db='CHIComp13', status='ok', move_no='RM-1')
        conn = Mock()
        conn.cursor.return_value.fetchone.return_value = None
        with patch.object(et, 'resolve_target', return_value=('CHIComp13', {})), patch.object(et.db, 'get_erp_transfer', return_value=rec), patch.object(et.db, 'mark_erp_transfer_deleted') as mark, patch.object(ep, '_connect_write', return_value=conn), patch.object(ep, '_check_db_name'):
            self.assertEqual(et.recheck(1)['status'], 'deleted')
            mark.assert_called_once_with(1)
            self.assertTrue(all(call.args[0].startswith('SELECT') for call in conn.cursor.return_value.execute.call_args_list))
            conn.commit.assert_not_called()




class TransferWriteEffectsTests(unittest.TestCase):
    def test_bill_writes_only_expected_tables_and_cost_modes(self):
        for by_ware, source_ware in ((False, '001'), (True, '001'), (False, '002'), (True, '002')):
            with self.subTest(by_ware=by_ware, source_ware=source_ware):
                stocks = {source_ware: {'Quantity': 2000, 'Cost': 21, 'TotalCost': 42000,
                                  'LatestInDate': 20261001, 'LatestOutDate': 20261002}}
                rolls = {source_ware: {'Quantity': 2000}}
                inserts = []
                updates = []
                cur = Mock()
                def read(cursor, sql, args):
                    if 'comProduct' in sql: return dict(CAvgCost=21, ProdName='面板')
                    if 'StkYearMonthQty' in sql:
                        self.assertEqual(args[0], 202611)
                        return copy.deepcopy(rolls.get(args[-1]))
                    return copy.deepcopy(stocks.get(args[-1]))
                def ins(cursor, table, values):
                    inserts.append((table, values))
                    if table == 'comWareAmount': stocks[values['WareID']] = dict(values)
                    if table == 'StkYearMonthQty': rolls[values['WareID']] = dict(values)
                def update(table, values, where):
                    updates.append((table, values, where))
                    if table == 'comWareAmount': stocks[where['WareID']].update(values)
                    if table == 'StkYearMonthQty': rolls[where['WareID']].update(values)
                    return 'UPDATE '+table, tuple(values.values())+tuple(where.values())
                def execute(sql, args=()):
                    if sql.startswith('SELECT COUNT'):
                        cur.fetchone.return_value = (1,)
                    if sql.startswith('SELECT SumQty'):
                        cur.fetchone.return_value = (1000,)
                    if sql.startswith('SELECT Quantity'):
                        cur.fetchone.return_value = (stocks[args[-1]]['Quantity'],)
                cur.execute.side_effect = execute
                batch = dict(rows=[dict(part='P-TAB' if source_ware == '002' else 'P', ware_out=source_ware, name='面板', transfer=1000, cost=21),
                                       dict(part='ZERO', name='零', transfer=0, cost=21)])
                with patch.object(et, 'one', side_effect=read), patch.object(et, 'insert', side_effect=ins), patch.object(et, 'ensure_masters'), patch.object(et, 'next_number', return_value='RM-20261105001'), patch.object(et.er, 'load_cost_params', return_value=dict(cost_by_ware=by_ware, digits=3, dot=True)), patch.object(et.er, 'build_update', side_effect=update):
                    result = et.execute_bill(cur, 'CHIComp13', batch, 20261105)
                self.assertEqual(result['sum_qty'], 1000)
                self.assertEqual(json.loads(result['lines_json'])[0]['ware_out'], source_ware)
                stock_updates = [c.args[1] for c in cur.execute.call_args_list if c.args[0] == 'UPDATE comWareAmount']
                self.assertEqual(stock_updates[0][-1], source_ware)
                self.assertEqual(stocks[source_ware]['Quantity'], 1000)
                self.assertEqual(stocks['015']['Quantity'], 1000)
                self.assertEqual(rolls[source_ware]['Quantity'], 1000)
                self.assertEqual(rolls['015']['Quantity'], 1000)
                self.assertEqual(stocks[source_ware]['LatestInDate'], 20261001)
                self.assertEqual(stocks[source_ware]['LatestOutDate'], 20261002)
                self.assertEqual(stocks[source_ware]['Cost'], 21)
                self.assertEqual(stocks[source_ware]['TotalCost'], 21000 if by_ware else 42000)
                self.assertEqual(stocks['015']['Cost'], 21 if by_ware else 0)
                self.assertEqual(stocks['015']['TotalCost'], 21000 if by_ware else 0)
                self.assertEqual({table for table, _ in inserts}, {'stkMoveMain', 'stkMoveSub', 'comProdRec', 'comWareAmount', 'StkYearMonthQty'})
                self.assertTrue(all('LatestInDate' not in vals and 'LatestOutDate' not in vals for _, vals, _ in updates))
                product_updates = [c for c in cur.execute.call_args_list if c.args[0].startswith('UPDATE comProduct')]
                self.assertEqual(len(product_updates), 1)
                self.assertEqual(product_updates[0].args[1], (20261105, 20261105, batch['rows'][0]['part']))

    def test_target_insufficient_aborts_before_bill_writes(self):
        batch = dict(rows=[dict(part='P-TAB', ware_out='002', transfer=10, cost=0)])
        def read(cur, sql, args):
            return dict(CAvgCost=0) if 'comProduct' in sql else dict(Quantity=5)
        cur = Mock()
        with patch.object(et, 'ensure_masters'), patch.object(et, 'one', side_effect=read), patch.object(et.er, 'load_cost_params', return_value=dict(cost_by_ware=False, digits=3, dot=True)), patch.object(et, 'insert') as insert:
            with self.assertRaisesRegex(ep.PoValidationError, '目標公司 002 倉庫存不足：P-TAB（需 10），已中止'):
                et.execute_bill(cur, 'CHIComp13', batch, 20261105)
        insert.assert_not_called()
        cur.execute.assert_not_called()

    def test_copy_masters_only_test_companies(self):
        cur = Mock()
        cur.fetchone.return_value = None
        with patch.object(ep, '_can_copy_masters', return_value=False), patch.object(ep, '_copy_masters') as copy_masters:
            with self.assertRaises(ep.PoValidationError): et.ensure_masters(cur, 'CHIComp03', ['P'])
            copy_masters.assert_not_called()
        from contextlib import contextmanager
        @contextmanager
        def source(): yield Mock()
        with patch.object(ep, '_can_copy_masters', return_value=True), patch.object(et.low_stock, '_erp_session', source), patch.object(ep, '_copy_masters') as copy_masters:
            et.ensure_masters(cur, 'CHIComp13', ['P'])
            specs = copy_masters.call_args.args[2]
            self.assertEqual({s['table'] for s in specs}, {'comProduct', 'comWareHouse'})
            self.assertEqual(specs[-1]['params'], ('015',))


class TransferSubmitTests(unittest.TestCase):
    def test_returns_increase_earliest_pending(self):
        batches = [dict(key='A', deductions=[('P', 10)]), dict(key='B', deductions=[('P', 20)])]
        result = st.allocate_batches(batches, {'P': -5}, [], {})
        self.assertEqual([b['rows'][0]['pending'] for b in result], [15, 20])

    def test_older_website_bills_do_not_remove_fifo_credits(self):
        prior = dict(batch_key='OLDER', target_db='CHIComp03', lines_json='[{"part":"P","qty":10}]')
        result = st.allocate_batches([dict(key='A', deductions=[('P', 10)])], {'P': 10}, [prior], {})
        self.assertEqual(result[0]['rows'], [])

    def test_create_each_bill_and_stop_after_error(self):
        import time
        plan = sample_plan()
        plan['batches'] = [dict(plan['batches'][0], key=k, code=k) for k in ('A', 'B', 'C')]
        token = et.digest(plan)
        with patch.dict(et._PREVIEWS, {token: (time.monotonic(), plan)}, clear=True), patch.object(et, 'resolve_target', return_value=('CHIComp13', {})), patch.object(et.db, 'successful_erp_transfers', return_value=[]), patch.object(et, '_safe_log', return_value=1) as log, patch.object(et, '_write', side_effect=[dict(move_no='RM-1'), ep.PoValidationError('資料已變動，請重新預覽')]) as write:
            result = et.create(dict(batches=['C', 'A', 'B'], client_token='token', preview_token=token))
            self.assertEqual([r['batch_key'] for r in result['results']], ['A', 'B'])
            self.assertEqual(result['stopped'], ['C'])
            self.assertEqual(write.call_count, 2)
            self.assertEqual([c.args[0]['status'] for c in log.call_args_list], ['ok', 'error'])
            self.assertTrue(write.call_args_list[0].kwargs['check_full'])
            self.assertFalse(write.call_args_list[1].kwargs['check_full'])

    def test_claim_blocks_inflight_batch(self):
        import time
        expected = sample_plan()
        token = et.digest(expected)
        with patch.dict(et._PREVIEWS, {token: (time.monotonic(), expected)}, clear=True), patch.object(et, 'resolve_target', return_value=('CHIComp13', {})), patch.object(et.db, 'successful_erp_transfers', return_value=[]), patch.object(et.po_duplicates, 'claim', return_value=False) as claim, patch.object(et, '_write') as write:
            with self.assertRaises(ep.DuplicateSubmitError):
                et.create(dict(batches=['2026/11-1'], client_token='token', preview_token=token))
            claim.assert_called_once_with('transfer:CHIComp13:2026/11-1')
            write.assert_not_called()

    def test_preview_only_selects_target_and_rolls_back(self):
        conn = Mock()
        expected = sample_plan()
        with patch.object(et, 'resolve_target', return_value=('CHIComp13', {'database': 'CHIComp13'})), patch.object(ep, '_connect_write', return_value=conn) as connect, patch.object(ep, '_check_db_name') as check, patch.object(et, 'plan', return_value=expected):
            result = et.preview()
            connect.assert_called_once_with({'database': 'CHIComp13'})
            check.assert_called_once_with(conn.cursor.return_value, 'CHIComp13')
            conn.rollback.assert_called_once()
            conn.commit.assert_not_called()
            self.assertEqual(result['preview_token'], et.digest(expected))


if __name__ == '__main__':
    unittest.main()
