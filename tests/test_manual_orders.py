import sqlite3
import unittest
import tempfile
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch

from pydantic import ValidationError

from app import database as db
from app.models import CreateOrderRequest


class ManualOrderTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db._CREATE_SQL)

        @contextmanager
        def connection():
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

        self.patch = patch.object(db, "get_conn", connection)
        self.patch.start()
        self.groups = patch.object(db, "_get_bom_model_groups", return_value={})
        self.groups.start()
        self.data = dict(code="10-0", po_number="12345", model="TEST", pcb="PCB", order_qty=100,
                         balance_qty=80, ship_date="2026-10-01", delivery_date="2026-09-25", remark="手填")

    def tearDown(self):
        self.groups.stop()
        self.patch.stop()
        self.conn.close()

    def create(self, **overrides):
        return db.create_manual_order(CreateOrderRequest(**{**self.data, **overrides}).dict())

    def test_create_preserves_fields_and_sets_pending(self):
        row = self.create()
        for key, value in self.data.items():
            self.assertEqual(row[key], value)
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["is_manual"], 1)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM dispatch_records").fetchone()[0], 0)

    def test_import_does_not_remove_or_overwrite_manual_order(self):
        row = self.create()
        db.upsert_orders_from_schedule([])
        db.upsert_orders_from_schedule([{**self.data, "order_qty": 999, "remark": "匯入"}])
        saved = db.get_order(row["id"])
        self.assertEqual(saved["order_qty"], 100)
        self.assertEqual(saved["remark"], "手填")
        self.assertEqual(len(db.get_orders()), 1)
        db.update_order(row["id"], status="dispatched")
        db.upsert_orders_from_schedule([self.data])
        self.assertEqual(len(db.get_orders()), 1)

    def test_duplicate_code_requires_explicit_insert(self):
        self.create()
        with self.assertRaises(ValueError):
            self.create()
        inserted = self.create(insert_before_code="10-0")
        self.assertEqual(inserted["insert_before_code"], "10-0")
        with self.assertRaises(ValueError):
            self.create(code="11-0", insert_before_code="10-0")

    def test_dedup_preserves_manually_created_order(self):
        first = self.create()
        db.update_order(first["id"], status="dispatched")
        inserted = self.create(insert_before_code="10-0")
        self.assertEqual(db.remove_duplicate_pending_orders()["removed"], 0)
        self.assertIsNotNone(db.get_order(inserted["id"]))

    def test_http_create_and_validation(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from app.routers.schedule import router

        app = FastAPI()
        app.include_router(router, prefix="/api")
        with TestClient(app) as client:
            response = client.post("/api/schedule/orders", json=self.data)
            self.assertEqual(response.status_code, 201)
            self.assertEqual(response.json()["status"], "pending")
            self.assertEqual(client.post("/api/schedule/orders", json=self.data).status_code, 409)
            self.assertEqual(client.post("/api/schedule/orders", json={**self.data, "order_qty": -1}).status_code, 422)

    def test_shift_uses_snapshot_and_is_reversible(self):
        first = self.create()
        second = self.create(code="10-1")
        inserted = self.create(insert_before_code="10-0")
        changes = db.apply_dispatch_code_shifts({"10-0": "10-1", "10-1": "10-2"}, [inserted["id"]])
        self.assertEqual(db.get_order(first["id"])["code"], "10-1")
        self.assertEqual(db.get_order(second["id"])["code"], "10-2")
        self.assertEqual(db.get_order(inserted["id"])["code"], "10-0")
        db.restore_dispatch_code_shifts(changes)
        self.assertEqual(db.get_order(first["id"])["code"], "10-0")
        self.assertEqual(db.get_order(second["id"])["code"], "10-1")

    def test_rejects_blank_invalid_dates_and_nonfinite_quantities(self):
        for override in ({"model": "  "}, {"ship_date": "2026-02-30"}, {"order_qty": 0},
                         {"order_qty": float("inf")}, {"balance_qty": -1}, {"balance_qty": float("nan")}):
            with self.subTest(override=override), self.assertRaises(ValidationError):
                CreateOrderRequest(**{**self.data, **override})

    def test_import_preserves_shifted_codes(self):
        db.upsert_orders_from_schedule([self.data])
        original = db.get_orders()[0]
        db.apply_dispatch_code_shifts({"10-0": "10-1"}, [])
        db.upsert_orders_from_schedule([self.data])
        self.assertEqual(db.get_order(original["id"])["code"], "10-1")
        db.update_order(original["id"], status="dispatched")
        db.upsert_orders_from_schedule([self.data])
        self.assertEqual(db.get_order(original["id"])["code"], "10-1")

    def test_restore_conflict_is_atomic_and_batch_precheck_is_read_only(self):
        first = self.create()
        second = self.create(code="10-1")
        changes = db.apply_dispatch_code_shifts({"10-0": "10-1", "10-1": "10-2"}, [])
        later = db.apply_dispatch_code_shifts({"10-1": "10-2", "10-2": "10-3"}, [])
        db.validate_dispatch_code_shift_restores([later, changes])
        self.assertEqual(db.get_order(first["id"])["code"], "10-2")
        db.restore_dispatch_code_shifts(later)
        db.update_order(second["id"], code="99-0")
        with self.assertRaises(ValueError):
            db.restore_dispatch_code_shifts(changes)
        self.assertEqual(db.get_order(first["id"])["code"], "10-1")

    def test_api_validates_insertion_anchor_without_dispatching(self):
        import openpyxl
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from app.routers.schedule import router

        app = FastAPI()
        app.include_router(router, prefix="/api")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "main.xlsx"
            workbook = openpyxl.Workbook()
            workbook.active.cell(1, 9, "10-0")
            workbook.save(path)
            workbook.close()
            with patch("app.routers.schedule._require_existing_main_path", return_value=str(path)), TestClient(app) as client:
                payload = {**self.data, "insert_before_code": "10-0"}
                self.assertEqual(client.post("/api/schedule/orders", json={**payload, "insert_before_code": "abc"}).status_code, 422)
                self.assertEqual(client.post("/api/schedule/orders", json={**payload, "code": "11-0", "insert_before_code": "11-0"}).status_code, 400)
                response = client.post("/api/schedule/orders", json=payload)
                self.assertEqual(response.status_code, 201)
                self.assertEqual(response.json()["status"], "pending")
                self.assertEqual(client.patch(f"/api/schedule/orders/{response.json()['id']}/code", json={"code": "11-0"}).status_code, 409)
                self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM dispatch_records").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
