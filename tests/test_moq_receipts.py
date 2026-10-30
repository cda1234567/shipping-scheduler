from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from app.services import erp_purchase as ep, erp_stock, low_stock, moq

ROOT = Path(__file__).resolve().parents[1]


class SqliteCursor:
    """把 SQL Server 的 %s / WITH (NOLOCK) 轉成 sqlite，用真的 SQL 驗證「最新一張、同單加總」。"""

    def __init__(self, conn, log):
        self.cur, self.log = conn.cursor(), log

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        self.log.append((s, tuple(params)))
        self.cur.execute(s.replace("WITH (NOLOCK)", "").replace("%s", "?"), tuple(params))

    def fetchall(self):
        return self.cur.fetchall()


class ReceiptBase(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE comProdRec(Flag INT, TranType INT, WareID TEXT, ProdID TEXT, BillNO TEXT, BillDate INT, Quantity REAL)")
        self.log = []
        self.down = False
        self.sessions = 0

        @contextmanager
        def session():
            self.sessions += 1
            if self.down:
                raise ep.ErpUnavailable("down")
            conn = type("C", (), {"cursor": lambda c: SqliteCursor(self.db, self.log)})()
            yield conn

        self.clock = [1000.0]
        for p in (
            patch.object(ep, "_source_session", session),
            patch.object(erp_stock, "get_settings", lambda: {"ware_id": "001", "database": "CHIComp03"}),
            patch.object(moq, "_clock", lambda: self.clock[0]),
            patch.object(moq, "site_moq_map", lambda: self.site),
            patch.object(moq, "_erp_min_purch", lambda parts: {p: v for p, v in self.minpurch.items() if p in parts}),
        ):
            p.start()
            self.addCleanup(p.stop)
        self.site, self.minpurch = {}, {}
        moq.clear_receipt_cache()

    def rec(self, part, bill, date, qty, flag=100, tran=205, ware="001"):
        self.db.execute("INSERT INTO comProdRec VALUES(?,?,?,?,?,?,?)", (flag, tran, ware, part, bill, date, qty))


class QueryTests(ReceiptBase):
    def test_latest_bill_wins_and_qty_from_that_bill(self):
        self.rec("EC-30033A", "RM-20260801001", 20260801, 3000)
        self.rec("EC-30033A", "RM-20260915002", 20260915, 6000)
        self.rec("EC-80012A", "RM-20260721002", 20260721, 2000)
        out, failed = moq.last_receipts(["ec-30033a", "EC-80012A", "NONE-1"])
        self.assertFalse(failed)
        self.assertEqual(out["EC-30033A"], {"qty": 6000.0, "date": "2026-09-15", "bill": "RM-20260915002"})
        self.assertEqual(out["EC-80012A"]["date"], "2026-07-21")
        self.assertNotIn("NONE-1", out)

    def test_multiple_rows_same_bill_are_summed(self):
        self.rec("A-1", "RM-20260915001", 20260915, 1000)
        self.rec("A-1", "RM-20260915001", 20260915, 500)
        self.rec("A-1", "RM-20260915001", 20260915, 250)
        self.assertEqual(moq.last_receipts(["A-1"])[0]["A-1"]["qty"], 1750.0)

    def test_same_date_takes_highest_bill_no_and_future_date_counts(self):
        self.rec("A-1", "RM-20260915001", 20260915, 100)
        self.rec("A-1", "RM-20260915003", 20260915, 300)
        self.rec("A-1", "RM-20260915002", 20260915, 200)
        self.assertEqual(moq.last_receipts(["A-1"])[0]["A-1"]["bill"], "RM-20260915003")
        moq.clear_receipt_cache()
        self.rec("A-1", "RM-20261102001", 20261102, 4000)  # 未來日期仍算
        self.assertEqual(moq.last_receipts(["A-1"])[0]["A-1"]["qty"], 4000.0)

    def test_filters_flag_trantype_and_ware(self):
        self.rec("A-1", "RM-1", 20260101, 10)
        self.rec("A-1", "RM-9", 20261201, 999, flag=400)
        self.rec("A-1", "RM-9", 20261201, 999, tran=206)
        self.rec("A-1", "RM-9", 20261201, 999, ware="002")
        self.assertEqual(moq.last_receipts(["A-1"])[0]["A-1"]["qty"], 10.0)
        sql, params = self.log[-1]
        self.assertTrue(sql.startswith("SELECT"))
        self.assertIn("WITH (NOLOCK)", sql)
        self.assertEqual(params[:3], (100, 205, "001"))

    def test_batched_in_chunks(self):
        parts = [f"P-{i:04d}" for i in range(450)]
        for p in parts[::100]:
            self.rec(p, "RM-1", 20260101, 5)
        out, _ = moq.last_receipts(parts)
        self.assertEqual(len(self.log), 3)  # 200 + 200 + 50
        self.assertEqual(len(out), 5)

    def test_cache_and_ttl(self):
        self.rec("A-1", "RM-1", 20260101, 10)
        moq.last_receipts(["A-1", "B-1"])
        moq.last_receipts(["A-1", "B-1"])  # 含「沒有紀錄」也快取
        self.assertEqual(len(self.log), 1)
        moq.last_receipts(["A-1", "C-1"])  # 只查沒快取的
        self.assertEqual(self.log[-1][1][-1:], ("C-1",))
        self.assertEqual(len(self.log), 2)
        self.clock[0] += moq.RECEIPT_TTL_SECONDS + 1
        moq.last_receipts(["A-1"])
        self.assertEqual(len(self.log), 3)

    def test_erp_unavailable_degrades_and_backs_off(self):
        self.down = True
        out, failed = moq.last_receipts(["A-1"])
        self.assertEqual((out, failed), ({}, True))
        moq.last_receipts(["A-1"])
        self.assertEqual(self.sessions, 1)  # 60 秒內不重試
        self.clock[0] += moq.RECEIPT_FAIL_RETRY_SECONDS + 1
        self.down = False
        self.rec("A-1", "RM-1", 20260101, 10)
        self.assertEqual(moq.last_receipts(["A-1"]), ({"A-1": {"qty": 10.0, "date": "2026-01-01", "bill": "RM-1"}}, False))


class FallbackChainTests(ReceiptBase):
    def test_receipt_beats_site_beats_minpurch_beats_none(self):
        self.rec("R-1", "RM-1", 20260915, 6000)
        self.rec("Z-1", "RM-2", 20260915, 0)  # 進貨量 0 不算
        self.site = {"R-1": 1000.0, "S-1": 800.0, "Z-1": 700.0}
        self.minpurch = {"R-1": 50.0, "S-1": 50.0, "M-1": 300.0}
        info = moq.lookup_moq(["R-1", "S-1", "M-1", "N-1", "Z-1"])
        self.assertEqual((info["R-1"]["moq"], info["R-1"]["moq_source"], info["R-1"]["last_receipt_date"]), (6000.0, "last_receipt", "2026-09-15"))
        self.assertEqual((info["S-1"]["moq"], info["S-1"]["moq_source"]), (800.0, "site_master"))
        self.assertEqual((info["M-1"]["moq"], info["M-1"]["moq_source"]), (300.0, "erp_minpurch"))
        self.assertEqual((info["N-1"]["moq"], info["N-1"]["moq_source"], info["N-1"]["last_receipt_qty"]), (0, "none", None))
        self.assertEqual((info["Z-1"]["moq"], info["Z-1"]["moq_source"], info["Z-1"]["last_receipt_qty"]), (700.0, "site_master", 0.0))

    def test_erp_down_falls_back_and_marks(self):
        self.down = True
        self.site = {"S-1": 800.0}
        info = moq.lookup_moq(["S-1", "N-1"])
        self.assertEqual((info["S-1"]["moq_source"], info["S-1"]["receipt_lookup_failed"]), ("site_master", True))
        self.assertEqual(info["N-1"]["moq_source"], "none")

    def test_use_erp_false_never_queries(self):
        self.rec("R-1", "RM-1", 20260915, 6000)
        self.site = {"R-1": 100.0}
        info = moq.lookup_moq(["R-1"], use_erp=False)
        self.assertEqual((info["R-1"]["moq_source"], self.log), ("site_master", []))

    def test_default_qty_and_fields(self):
        self.rec("R-1", "RM-1", 20260915, 6000)
        f = moq.moq_fields(80, moq.lookup_moq(["R-1"])["R-1"])
        self.assertEqual((f["suggested_qty"], f["moq_source"], f["last_receipt_qty"], f["last_receipt_date"], f["last_receipt_bill"], f["moq_adjusted"]),
                         (6000, "last_receipt", 6000.0, "2026-09-15", "RM-1", True))
        f = moq.moq_fields(9000, moq.lookup_moq(["R-1"])["R-1"])
        self.assertEqual((f["suggested_qty"], f["moq_adjusted"]), (9000, False))  # 需求 > 上次進貨量
        f = moq.moq_fields(80, moq.lookup_moq(["N-1"])["N-1"])
        self.assertEqual((f["suggested_qty"], f["moq_source"], f["last_receipt_date"]), (80, "none", ""))

    def test_low_stock_with_moq_uses_receipts(self):
        self.rec("A-1", "RM-1", 20260915, 2000)
        views = low_stock.with_moq([{"part_number": "a-1", "threshold": 105, "qty": 10, "on_order_qty": 0}])
        v = views[0]
        self.assertEqual((v["moq"], v["moq_source"], v["last_receipt_date"], v["suggested_po_qty"]), (2000.0, "last_receipt", "2026-09-15", 2000))

    def test_part_defaults_returns_receipt_fields(self):
        self.rec("A-1", "RM-1", 20260915, 2000)

        outer = self

        class Cur:
            description = None

            def execute(self, sql, params=None):
                self.sql = sql
                self.inner = None
                if "comProdRec" in sql:  # 最近進貨查詢走 sqlite 假資料
                    self.inner = SqliteCursor(outer.db, outer.log)
                    self.inner.execute(sql, params or ())

            def fetchone(self):
                return ("A-1", "電容") if "FROM comProduct" in self.sql else None

            def fetchall(self):
                return self.inner.fetchall() if self.inner else []

        @contextmanager
        def session():
            yield type("C", (), {"cursor": lambda c: Cur()})()

        with patch.object(ep, "_source_session", session), patch.object(ep, "_suggested_qty", lambda part: 30):
            d = ep.part_defaults("a-1")
        self.assertEqual((d["moq_source"], d["last_receipt_qty"], d["last_receipt_date"], d["last_receipt_bill"], d["suggested_qty"]),
                         ("last_receipt", 2000.0, "2026-09-15", "RM-1", 2000))
        with patch.object(ep, "_source_session", session), patch.object(ep, "_suggested_qty", lambda part: None):
            d = ep.part_defaults("a-1")
        self.assertIsNone(d["suggested_qty"])  # 手動行沒有需求：不預填數量，只回進貨資訊給提示用
        self.assertEqual(d["last_receipt_date"], "2026-09-15")


@unittest.skipUnless(shutil.which("node"), "需要 node")
class HintTextTests(unittest.TestCase):
    def run_js(self, calls):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        for f in (ROOT / "static" / "modules").glob("*.js"):
            (tmp / f.name).write_text(f.read_text(encoding="utf-8").replace('"/static/modules/', '"./'), encoding="utf-8")
        (tmp / "package.json").write_text('{"type":"module"}')
        (tmp / "t.mjs").write_text(
            'globalThis.document={getElementById:()=>null,addEventListener(){},querySelector:()=>null,querySelectorAll:()=>[]};'
            'globalThis.window={addEventListener(){},location:{}};globalThis.localStorage={getItem(){return null},setItem(){}};'
            'const m=await import("./erp_purchase.js");'
            f'console.log(JSON.stringify({calls}.map(a=>m.moqHintText(...a))));')
        out = subprocess.run(["node", str(tmp / "t.mjs")], capture_output=True, text=True, timeout=30)
        self.assertEqual(out.returncode, 0, out.stderr)
        return json.loads(out.stdout)

    def test_hint_texts(self):
        got = self.run_js('[[1234,6000,"last_receipt",{date:"2026-09-15"}],[null,2000,"last_receipt",{date:"2026-07-21"}],'
                          '[1,2000,"last_receipt",{date:"2026-07-21"}],[300,1000,"site_master"],[300,500,"erp_minpurch"],'
                          '[300,0,"none"],[null,0,"none"],[300,0,""],[null,0,""]]')
        self.assertEqual(got, [
            "需 1,234，上次進貨 6,000（2026/09/15）",
            "上次進貨 2,000（2026/07/21）",
            "上次進貨 2,000（2026/07/21）",
            "需 300，MOQ 1,000（主檔）",
            "需 300，MOQ 500（正航最小採購量）",
            "需 300，無 MOQ 資料",
            "無 MOQ 資料",
            "需 300",
            "",
        ])


if __name__ == "__main__":
    unittest.main()
