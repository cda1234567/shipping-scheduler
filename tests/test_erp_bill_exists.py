"""已開採購單 / 歷史紀錄以正航為準：正航已刪的單不算已開、歷史標 erp_deleted（假 ERP cursor，全程唯讀）。"""
from __future__ import annotations

import json
import re
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

import app.database as db
from app.services import erp_bill_exists as ebe, erp_purchase as ep, erp_receive as er, po_duplicates as pd
from app.services import shortage_purchase as sp

TODAY = date(2026, 11, 24)


class FakeCursor:
    def __init__(self, erp):
        self.erp = erp
        self.rows = []

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        self.erp["sql"].append(s)
        if self.erp.get("fail"):
            raise RuntimeError("boom")
        if s == "SELECT DB_NAME()":
            self.rows = [(self.erp.get("db_name", "CHIComp01"),)]
            return
        assert s.startswith("SELECT "), s  # 只能是 SELECT
        bills = set(params or ())
        if s.startswith("SELECT s.BillNO, m.CustomerID, s.ProdID FROM ordBillSub s"):
            self.rows = [r for r in self.erp["po_lines"] if r[0].strip() in bills]
            return
        if "stkBillMain" in s and "CustomerID" in s:  # 正航 stkBillMain 沒有 CustomerID 欄（實機 error 207）
            raise RuntimeError("Invalid column name 'CustomerID'. (207)")
        m = re.match(r"SELECT BillNO, CustomerID FROM (ordBillMain) WHERE Flag = (\d+)", s)
        if m:
            self.rows = [r[1:] for r in self.erp["mains"] if r[0] == (m.group(1), int(m.group(2))) and r[1] in bills]
            return
        m = re.match(r"SELECT BillNO FROM (stkBillMain) WHERE Flag = (\d+)", s)
        if m:
            self.rows = [r[1:2] for r in self.erp["mains"] if r[0] == (m.group(1), int(m.group(2))) and r[1] in bills]
            return
        raise AssertionError(f"unexpected SQL: {s}")

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class FakeConn:
    def __init__(self, erp):
        self.erp = erp

    def cursor(self):
        return FakeCursor(self.erp)

    def rollback(self):
        self.erp["rollbacks"] += 1

    def commit(self):
        raise AssertionError("不可 commit")

    def close(self):
        self.erp["closed"] += 1


class Base(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db._CREATE_SQL)

        @contextmanager
        def temp_conn():
            yield self.conn
            self.conn.commit()

        self.erp = {"sql": [], "po_lines": [], "mains": [], "rollbacks": 0, "closed": 0}

        @contextmanager
        def read_session(target):
            self.erp.setdefault("targets", []).append(target)
            if self.erp.get("connect_fail"):
                raise ebe.erp_stock.ErpUnavailable("無法連線")
            yield FakeConn(self.erp)

        for p in (patch.object(db, "get_conn", temp_conn), patch.object(ebe, "_read_session", read_session)):
            p.start()
            self.addCleanup(p.stop)

    def record_po(self, bill_no, lines, supplier="W0276", target="CHIComp01", order_ids=(), status="ok"):
        po_id = db.add_erp_purchase_order({
            "bill_no": bill_no, "target_db": target, "supplier_id": supplier, "status": status,
            "lines_json": json.dumps([{"row_no": i, "part_number": p, "qty": q, "price": 1, "pre_in_date": "2026-11-30"}
                                      for i, (p, q) in enumerate(lines, 1)]),
        })
        rows = [{"row_no": 1, "part_number": lines[0][0], "order_id": o, "qty": lines[0][1], "kind": "order"} for o in order_ids] \
            or [{"row_no": i, "part_number": p, "qty": q, "kind": "manual"} for i, (p, q) in enumerate(lines, 1)]
        db.save_po_line_allocations(po_id, bill_no, target, rows)
        return po_id


class LoadPriorTests(Base):
    def test_deleted_po_not_matched_and_reported_separately(self):
        self.record_po("PO-20261104004", [("PK-10011B", 50)])
        self.record_po("PO-20261120001", [("PK-10011B", 100)])
        self.erp["po_lines"] = [("PO-20261120001", "W0276", "PK-10011B")]
        deleted = {}
        prior = pd.load_prior("CHIComp01", ["pk-10011b"], deleted)
        self.assertEqual([e["bill_no"] for e in prior["PK-10011B"]], ["PO-20261120001"])
        self.assertEqual([e["bill_no"] for e in deleted["PK-10011B"]], ["PO-20261104004"])
        # 一次批次查完（DB_NAME 由 _read_session 負責，這裡被替換掉）
        self.assertEqual(len(self.erp["sql"]), 1)
        self.assertEqual(self.erp["targets"], ["CHIComp01"])

    def test_same_bill_no_other_supplier_treated_as_deleted(self):
        # 本站 PO-20261119003 是 W0276 的 PK 料；正航刪掉後同號改開 P0311 的 AC-40804B
        self.record_po("PO-20261119003", [("PK-10011B", 100), ("PK-10008B", 300)])
        self.erp["po_lines"] = [("PO-20261119003", "P0311", "AC-40804B")]
        deleted = {}
        prior = pd.load_prior("CHIComp01", ["PK-10011B", "PK-10008B"], deleted)
        self.assertEqual(prior, {})
        self.assertEqual(sorted(deleted), ["PK-10008B", "PK-10011B"])

    def test_same_bill_and_supplier_but_part_removed_is_deleted(self):
        self.record_po("PO-1", [("PK-1", 1), ("PK-2", 2)])
        self.erp["po_lines"] = [(" PO-1 ", "w0276", "pk-1")]
        prior = pd.load_prior("CHIComp01", ["PK-1", "PK-2"])
        self.assertEqual(list(prior), ["PK-1"])

    def test_erp_failure_falls_back_to_local_records(self):
        self.record_po("PO-20261119003", [("PK-10011B", 100)])
        for key in ("connect_fail", "fail"):
            self.erp[key] = True
            with self.assertLogs("app.services.erp_bill_exists", "WARNING"):
                prior = pd.load_prior("CHIComp01", ["PK-10011B"])
            self.assertEqual([e["bill_no"] for e in prior["PK-10011B"]], ["PO-20261119003"])
            self.erp[key] = False

    def test_find_duplicates_skips_deleted_po(self):
        self.record_po("PO-20261119003", [("PK-10011B", 100)])
        req = {"supplier_id": "W0276", "lines": [{"part_number": "PK-10011B", "qty": 100, "allocations": []}]}
        today = date.fromisoformat(db.list_erp_purchase_orders()[0]["created_at"][:10])
        self.assertEqual(pd.find_duplicates(req, "CHIComp01", today), [])
        self.erp["po_lines"] = [("PO-20261119003", "W0276", "PK-10011B")]
        self.assertEqual(len(pd.find_duplicates(req, "CHIComp01", today)), 1)

    def test_already_ordered_and_deleted_hint(self):
        self.record_po("PO-20261119003", [("PK-10011B", 100)], order_ids=(285,))
        self.record_po("PO-20261121001", [("PK-10007B", 100)], order_ids=(285,))
        self.erp["po_lines"] = [("PO-20261121001", "W0276", "PK-10007B")]
        parts = [
            {"part_number": p, "supplier_id": "W0276", "needs_purchase": True, "allocations": [{"order_id": 285}]}
            for p in ("PK-10011B", "PK-10007B")
        ]
        with patch.object(ep, "get_target", lambda kind="": "CHIComp01"):
            sp._already_ordered(parts)
        p = {x["part_number"]: x for x in parts}
        self.assertEqual(p["PK-10011B"]["already_ordered"], [])
        self.assertEqual([m["bill_no"] for m in p["PK-10011B"]["already_ordered_deleted"]], ["PO-20261119003"])
        self.assertEqual(set(p["PK-10011B"]["already_ordered_deleted"][0]), {"bill_no", "date", "qty", "supplier_id"})
        self.assertEqual([m["bill_no"] for m in p["PK-10007B"]["already_ordered"]], ["PO-20261121001"])
        self.assertEqual(p["PK-10007B"]["already_ordered_deleted"], [])


class HistoryFlagTests(Base):
    def test_po_history_flags_deleted_and_supplier_mismatch_only(self):
        self.record_po("PO-20261104004", [("PK-1", 1)])
        self.record_po("PO-20261119003", [("PK-10011B", 100)], supplier="W0276")
        self.record_po("PO-20261120001", [("PK-2", 1)], supplier="P0175")
        self.record_po("", [("PK-3", 1)], status="error")
        self.erp["mains"] = [(("ordBillMain", 4), "PO-20261119003", "P0311"), (("ordBillMain", 4), "PO-20261120001", "P0175")]
        rows = {r["bill_no"]: r for r in ep.list_history()}
        self.assertTrue(rows["PO-20261104004"]["erp_deleted"])
        self.assertTrue(rows["PO-20261119003"]["erp_deleted"])
        self.assertNotIn("erp_deleted", rows["PO-20261120001"])
        self.assertNotIn("erp_deleted", rows[""])
        self.assertEqual(len(rows["PO-20261120001"]["allocations"]), 1)
        # 本機紀錄不動
        self.assertEqual({r["status"] for r in db.list_erp_purchase_orders()}, {"ok", "error"})

    def test_receipt_history_flag_by_rm_no(self):
        for rm in ("RM-20261116002", "RM-20261120001"):
            db.add_erp_receipt({"status": "ok", "target_db": "CHIComp01", "rm_no": rm, "vendor_id": "P0175", "lines_json": "[]"})
        self.erp["mains"] = [(("stkBillMain", 100), "RM-20261120001", "P0175")]
        rows = {r["rm_no"]: r for r in er.list_history()}
        self.assertTrue(rows["RM-20261116002"]["erp_deleted"])
        self.assertNotIn("erp_deleted", rows["RM-20261120001"])

    def test_receipt_query_never_uses_customer_id(self):
        # 實機：RM-20261116002（CHIComp03、W0083）11/18 已在正航刪除，查 CustomerID 會 207 失敗而不標
        for rm in ("RM-20261116002", "RM-20261120001"):
            db.add_erp_receipt({"status": "ok", "target_db": "CHIComp03", "rm_no": rm, "vendor_id": "W0083", "lines_json": "[]"})
        self.erp["mains"] = [(("stkBillMain", 100), "RM-20261120001")]
        rows = {r["rm_no"]: r for r in er.list_history()}
        self.assertTrue(rows["RM-20261116002"]["erp_deleted"])
        self.assertNotIn("erp_deleted", rows["RM-20261120001"])
        receipt_sql = [q for q in self.erp["sql"] if "stkBillMain" in q]
        self.assertTrue(receipt_sql)
        self.assertFalse(any("CustomerID" in q for q in receipt_sql))
        self.assertEqual(ebe.bill_customers("CHIComp03", "receipt", ["RM-20261120001"]), {"RM-20261120001": set()})

    def test_history_erp_failure_omits_flag(self):
        self.record_po("PO-20261104004", [("PK-1", 1)])
        self.erp["connect_fail"] = True
        with self.assertLogs("app.services.erp_bill_exists", "WARNING"):
            rows = ep.list_history()
        self.assertNotIn("erp_deleted", rows[0])


class ReadSessionTests(unittest.TestCase):
    def test_read_session_checks_db_name_and_always_rolls_back(self):
        erp = {"sql": [], "po_lines": [("PO-1", "S1", "X")], "mains": [], "rollbacks": 0, "closed": 0, "db_name": "CHIComp01"}
        with patch.object(ep, "write_credentials", lambda t: {"database": t}), \
                patch.object(ep, "_connect_write", lambda creds: FakeConn(erp)):
            self.assertEqual(ebe.po_line_keys("CHIComp01", ["PO-1"]), {("PO-1", "S1", "X")})
            self.assertEqual((erp["rollbacks"], erp["closed"]), (1, 1))
            erp["db_name"] = "CHIComp03"  # 連錯資料庫 → 視為查詢失敗，不可把所有單當成已刪
            with self.assertLogs("app.services.erp_bill_exists", "WARNING"):
                self.assertIsNone(ebe.po_line_keys("CHIComp01", ["PO-1"]))
            self.assertEqual((erp["rollbacks"], erp["closed"]), (2, 2))

    def test_chunked_in_list(self):
        erp = {"sql": [], "po_lines": [], "mains": [], "rollbacks": 0, "closed": 0}

        @contextmanager
        def read_session(target):
            yield FakeConn(erp)

        with patch.object(ebe, "_read_session", read_session):
            self.assertEqual(ebe.bill_customers("CHIComp01", "po", [f"PO-{i}" for i in range(ebe.CHUNK + 5)]), {})
        self.assertEqual(len(erp["sql"]), 2)


if __name__ == "__main__":
    unittest.main()
