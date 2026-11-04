import os
import tempfile
import unittest
from types import SimpleNamespace
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from app import database as db
from app.services import st_pending_transfer
from main import app


class OrderArchiveTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        directory = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.stack.enter_context(patch.object(db, 'DB_PATH', directory / 'test.db'))
        self.stack.enter_context(patch.object(db, 'DATA_DIR', directory))
        db.init_db()
        self.stack.enter_context(patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "1"}))
        self.client = TestClient(app)

    def add(self, code='11-5', created='2026-03-11', keep=0):
        with db.get_conn() as conn:
            return conn.execute(
                "INSERT INTO orders(code, po_number, model, status, created_at, archive_keep) VALUES(?, 'PO', 'MODEL', 'dispatched', ?, ?)",
                (code, created, keep)).lastrowid

    def test_legacy_migration_and_startup_are_idempotent(self):
        # 模擬尚無封存欄位的既有資料庫。
        with db.get_conn() as conn:
            for column in ('archived_at', 'archive_reason', 'archive_keep'):
                conn.execute(f'ALTER TABLE orders DROP COLUMN {column}')
            conn.execute("INSERT INTO orders(code,created_at) VALUES('11-5','2026-03-11')")
            conn.execute("INSERT INTO orders(code,created_at) VALUES('11-5','2026-11-04')")
        db.init_db()
        first = db.list_archived_orders()
        db.init_db()
        self.assertEqual(first, db.list_archived_orders())
        self.assertEqual(len(first), 1)
        self.assertEqual(len(db.get_orders()), 1)
        with db.get_conn() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 2)

    def test_rule_dates_ties_empty_codes_and_keep(self):
        old = self.add()
        new = self.add(created='2026-11-04')
        tie = self.add(created='2026-11-04')
        protected = self.add(created='2026-01-01', keep=1)
        empty1, empty2 = self.add(code=''), self.add(code='')
        e1, e2 = self.add(code='12-1', created=''), self.add(code='12-1', created='')
        # 日期優先於 id。
        late_id = self.add(created='2026-02-01')
        archived = db.auto_archive_orders()
        self.assertEqual({o['id'] for o in archived}, {old, new, e1, late_id})
        self.assertIn(f'#{tie}', db.get_order(old)['archive_reason'])
        for oid in (tie, protected, empty1, empty2, e2):
            self.assertEqual(db.get_order(oid)['archived_at'], '')
        self.assertEqual(db.auto_archive_orders(), [])
        self.assertEqual(db.get_order_by_code('11-5')['id'], tie)

    def test_unarchive_api_keeps_order_and_missing_id(self):
        old = self.add()
        self.add(created='2026-11-04')
        db.auto_archive_orders()
        response = self.client.get('/api/schedule/orders/archived')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['rows'][0]['id'], old)
        response = self.client.post(f'/api/schedule/orders/{old}/unarchive')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(db.get_order(old)['archive_keep'], 1)
        self.assertEqual(db.auto_archive_orders(), [])
        self.assertEqual(db.get_order(old)['archive_reason'], '')
        self.assertEqual(self.client.post('/api/schedule/orders/99999/unarchive').status_code, 404)

    def test_lists_and_shortage_latest_batch_exclude_archived(self):
        old = self.add()
        new = self.add(created='2026-11-04')
        db.auto_archive_orders()
        self.assertEqual([o['id'] for o in db.list_dispatched_orders_for_purchase()], [new])
        response = self.client.get('/api/shortage-po/orders').json()
        self.assertEqual([o['id'] for o in response['orders']], [new])
        self.assertNotIn(old, response['latest_batch_ids'])
        self.assertEqual([o['id'] for o in db.get_orders()], [new])
        self.assertEqual(st_pending_transfer.list_batches()[0]['order_ids'], [new])
        self.assertEqual(st_pending_transfer.list_batches([db.get_order(old)]), [])
        self.assertEqual(db.get_order(old)['status'], 'dispatched')

    def test_code_edit_triggers_rule(self):
        old = self.add()
        new = self.add(code='other', created='2026-11-04')
        db.update_order(new, code='11-5')
        self.assertTrue(db.get_order(old)['archived_at'])

    def test_import_does_not_readd_or_delete_archived_orders(self):
        old = self.add()
        new = self.add(created='2026-11-04')
        db.update_order(new, po_number='NEWPO')
        db.auto_archive_orders()
        with db.get_conn() as conn:
            conn.execute("UPDATE orders SET status='pending', ship_date='2026-03-06' WHERE id=?", (old,))
        with patch.object(db, '_get_bom_model_groups', return_value={}):
            # 排程表仍含舊單：不重新新增；排程表沒有舊單：封存的 pending 單也不刪除
            db.upsert_orders_from_schedule([dict(po_number='PO', model='MODEL', ship_date='2026-03-06')])
            db.upsert_orders_from_schedule([dict(po_number='OTHER', model='M', ship_date='2026-12-01')])
        self.assertTrue(db.get_order(old)['archived_at'])
        with db.get_conn() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM orders WHERE po_number='PO'").fetchone()[0], 1)

    def test_preview_filters_old_columns_without_mutating_cache(self):
        from app.services.main_preview import _exclude_archived_columns
        old = self.add()
        new = self.add(created='2026-11-04')
        db.update_order(new, po_number='NEW')
        db.auto_archive_orders()
        preview = {'sheet': {'columns': [{'index': i} for i in range(1, 7)], 'col_count': 6,
                            'rows': [{'index': 1, 'cells': [{'col': 1, 'value': '11-5'}, {'col': 2, 'value': 'PO'}, {'col': 3, 'value': 'MODEL'}, {'col': 4, 'value': '11-5'}, {'col': 5, 'value': 'NEW'}, {'col': 6, 'value': 'MODEL'}]}]}}
        result = _exclude_archived_columns(preview)
        self.assertEqual([c['index'] for c in result['sheet']['columns']], [4, 5, 6])
        self.assertEqual(preview['sheet']['col_count'], 6)

    def test_unarchive_requires_existing_edit_auth(self):
        with patch.dict(os.environ):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            with patch("main.get_edit_auth_status", return_value=SimpleNamespace(authenticated=False)):
                response = self.client.post('/api/schedule/orders/1/unarchive')
        self.assertEqual(response.status_code, 403)
