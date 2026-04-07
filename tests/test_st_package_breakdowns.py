from __future__ import annotations

import sqlite3
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import app.database as db
from app.services import st_package_breakdowns as svc
from app.services.dispatch_pipeline import DispatchContext, DispatchPlan, commit_dispatch_plan


class InMemoryDbTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(db._CREATE_SQL)
        self.conn.execute("ALTER TABLE orders ADD COLUMN folder TEXT NOT NULL DEFAULT ''")

        @contextmanager
        def temp_conn():
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

        self.get_conn_patcher = patch.object(db, "get_conn", temp_conn)
        self.group_patcher = patch.object(db, "_get_bom_model_groups", lambda: {})
        self.get_conn_patcher.start()
        self.group_patcher.start()

    def tearDown(self):
        self.group_patcher.stop()
        self.get_conn_patcher.stop()
        self.conn.close()


class StPackageBreakdownTests(InMemoryDbTestCase):
    def test_deduct_package_values_prefers_exact_match(self):
        result = svc.deduct_package_values([200, 300, 500], 300)
        self.assertEqual(result, [200, 500])

    def test_deduct_package_values_falls_back_to_left_to_right_split(self):
        result = svc.deduct_package_values([200, 300, 500], 450)
        self.assertEqual(result, [50, 500])

    def test_build_missing_moq_package_rows_uses_main_parts_but_st_stock_qty(self):
        db.save_snapshot({"PART-1": 10, "PART-2": 8}, {"PART-1": 0, "PART-2": 1200})
        db.save_st_inventory_snapshot({"PART-1": 1000, "PART-2": 500}, {"PART-1": "Cap", "PART-2": "IC"})
        db.save_st_package_breakdown("part-1", "200,300,500", "2026-04-07T10:00:00")

        rows = svc.build_missing_moq_package_rows()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["part_number"], "PART-1")
        self.assertEqual(rows[0]["stock_qty"], 1000)
        self.assertEqual(rows[0]["package_sum"], 1000)
        self.assertTrue(rows[0]["matches_stock"])

    def test_consume_st_package_breakdowns_updates_stock_and_packages(self):
        db.save_snapshot({"PART-1": 10}, {"PART-1": 0})
        db.save_st_inventory_snapshot({"PART-1": 1000}, {"PART-1": "Cap"})
        db.save_st_package_breakdown("PART-1", "200,300,500", "2026-04-07T10:00:00")

        result = svc.consume_st_package_breakdowns({1: {"part-1": 300}})

        self.assertEqual(result["usage_by_part"], {"PART-1": 300.0})
        self.assertEqual(db.get_st_inventory_stock()["PART-1"], 700.0)
        self.assertEqual(db.get_st_package_breakdowns(["PART-1"])["PART-1"]["package_text"], "200,500")


class DispatchPipelineStPackageHookTests(unittest.TestCase):
    def test_commit_dispatch_plan_consumes_st_packages_after_success(self):
        context = DispatchContext(order={"id": 1}, groups=[], all_components=[], supplements={"PART-1": 300})
        plan = DispatchPlan(
            main_path="C:/main.xlsx",
            contexts=[context],
            preview={},
            supplement_allocations={1: {"PART-1": 300}},
        )

        with patch("app.services.dispatch_pipeline.db.replace_order_supplements") as mock_replace, \
             patch("app.services.dispatch_pipeline.consume_st_package_breakdowns") as mock_consume, \
             patch("app.services.dispatch_pipeline.refresh_snapshot_from_main") as mock_refresh:
            result = commit_dispatch_plan(
                plan,
                backup_dir="C:/backups",
                execute_dispatcher=lambda *_args, **_kwargs: {"order_id": 1, "merged_parts": 1},
                snapshot_refresher=mock_refresh,
            )

        self.assertEqual(result.count, 1)
        mock_replace.assert_called_once_with([1], {1: {"PART-1": 300}})
        mock_consume.assert_called_once_with({1: {"PART-1": 300}})
        mock_refresh.assert_called_once_with("C:/main.xlsx")
