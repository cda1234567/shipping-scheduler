"""PO-20261105001（2026-11-05 08:12，P0327 汎翊，「缺料採購：11-5、11-4」）回歸測試。

當時線上版本 v2026.11.04.10（b34d0b6）。用 tests/fixtures/shortage_po_20261105.json（從
system_backup_20261105_020018.db 抽出）+ 假正航（11/05 早上還沒有 001→015 調撥 → 待調撥只看網站扣量）重跑：

舊行為（b34d0b6，已用 detached worktree 實際重跑確認）：
- moq.lookup_moq 把「最近一張進貨單的數量」直接當 MOQ。這幾顆料最近一張進貨正是上一次照 10-8（#220 T7U）
  整包補料買進來的量（EC-10032A 30,000、EC-20136A 40,000、EC-20097A 75,000、EC-20137A 16,000、EC-20087A 20,000…）。
- 建議量 = max(缺, MOQ) → EC-10032A 缺 10,000（需求 10,000 + 待調撥 60,000 − ST 60,000）卻建議 30,000。
- allocate_purchase_to_orders 把超過訂單需求的 20,000 記成 kind='extra'（order_id=0），畫面叫「備品」。
v2026.11.05.6（9c4084f）已把 MOQ 改成近 10 張進貨 GCD（EC-10032A = 10,000），但 kind='extra' 仍在；
v2026.11.11.2 拿掉 extra：多於缺的部分只會是「MOQ 進位」或「使用者改量」，並把計算依據存進 lines_json。
"""
from __future__ import annotations

import json
import random
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import app.database as db
from app.routers import dispatch as dispatch_router
from app.services import erp_purchase as ep
from app.services import low_stock
from app.services import moq as moq_service
from app.services import shortage_purchase as sp

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "shortage_po_20261105.json"
NOW = datetime(2026, 11, 5, 8, 12)


class _NoTransferCursor:
    """正航唯讀查詢：11/05 早上沒有 RM-20261106* / RM-20261109* 調撥 → 調撥量 0；其他查詢也都回空。"""

    def execute(self, sql, params=None):
        self.rows = []

    def fetchall(self):
        return []

    def fetchone(self):
        return None


class _NoTransferConn:
    def cursor(self):
        return _NoTransferCursor()


class Po20261105Base(unittest.TestCase):
    #: 近幾張進貨單（新到舊）。預設：最近一張是 10-8 的整包，GCD = 10,000（與備份中 EC-10032A 的近 10 張進貨 GCD 一致）
    RECEIPTS = {
        "EC-10032A": [30000.0, 20000.0, 10000.0],
        "EC-20136A": [40000.0, 20000.0, 10000.0],
        "EC-20097A": [75000.0, 30000.0, 15000.0],
        "EC-20137A": [16000.0, 4000.0],
        "EC-20087A": [20000.0, 12000.0, 4000.0],
        "EC-20117A": [60000.0, 70000.0, 10000.0],
    }

    def setUp(self):
        fx = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.fx = fx
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db._CREATE_SQL)
        if "archived_at" not in {r[1] for r in self.conn.execute("PRAGMA table_info(orders)")}:
            self.conn.execute("ALTER TABLE orders ADD COLUMN archived_at TEXT NOT NULL DEFAULT ''")
        c = self.conn
        for o in fx["orders"]:
            c.execute("INSERT INTO orders(id, code, status, model, po_number, order_qty, ship_date, delivery_date, created_at, archived_at)"
                      " VALUES(?,?,?,?,?,?,?,?,?,?)",
                      (o["id"], o["code"], o["status"], o["model"], o["po_number"], o["order_qty"], o["ship_date"],
                       o["delivery_date"], o["created_at"], o["archived_at"]))
        for s in fx["st"]:
            c.execute("INSERT INTO st_inventory_snapshot(part_number, stock_qty, loaded_at) VALUES(?,?,?)",
                      (s["part_number"], s["stock_qty"], s["loaded_at"]))
        # 當時沒有 shortage_transfer_cutoff_batch → 預設截止批次 10-3
        c.execute("INSERT INTO settings(key, value) VALUES('st_inventory_loaded_at', ?)", (fx["settings"]["st_inventory_loaded_at"],))
        sessions = {}
        for x in fx["consumptions"]:
            key = (x["order_id"], x["consumed_at"], x["rolled_back_at"])
            if key not in sessions:
                sessions[key] = c.execute(
                    "INSERT INTO dispatch_sessions(order_id, dispatched_at, rolled_back_at) VALUES(?,?,?)", key).lastrowid
            c.execute("INSERT INTO st_dispatch_consumptions(dispatch_session_id, order_id, part_number, used_qty, stock_before,"
                      " stock_after, consumed_at, rolled_back_at) VALUES(?,?,?,?,?,?,?,?)",
                      (sessions[key], x["order_id"], x["part_number"], x["used_qty"], x["stock_before"], x["stock_after"],
                       x["consumed_at"], x["rolled_back_at"]))
        for o in fx["orders"]:
            if not c.execute("SELECT 1 FROM dispatch_sessions WHERE order_id=?", (o["id"],)).fetchone():
                c.execute("INSERT INTO dispatch_sessions(order_id, dispatched_at) VALUES(?,?)", (o["id"], o["created_at"]))
        c.commit()

        orders = {o["id"]: o for o in fx["orders"]}
        # 勾選 11-5(#264)、11-4(#262)：需求 = 本次補料；11-4 不用這幾顆料
        self.groups = [
            {"order_id": 264, "batch_code": "11-5", "po_number": orders[264]["po_number"], "model": orders[264]["model"],
             "ship_date": orders[264]["ship_date"],
             "items": [{"part": s["part_number"], "desc": "0402", "need_qty": s["supplement_qty"]}
                       for s in fx["supplements"] if s["order_id"] == 264]},
            {"order_id": 262, "batch_code": "11-4", "po_number": orders[262]["po_number"], "model": orders[262]["model"],
             "ship_date": orders[262]["ship_date"], "items": []},
        ]
        self.receipts = dict(self.RECEIPTS)
        self.site_moq = {}

        @contextmanager
        def temp_conn():
            yield self.conn
            self.conn.commit()

        @contextmanager
        def erp_session(*a, **k):
            yield _NoTransferConn()

        @contextmanager
        def source_down(*a, **k):
            raise RuntimeError("ERP offline (test)")
            yield

        def fake_receipts(parts):
            return {p: {"qty": self.receipts[p][0], "date": "2026-10-20", "bill": "RM-TEST", "qtys": list(self.receipts[p])}
                    for p in parts if p in self.receipts}

        moq_service.clear_receipt_cache()
        self.addCleanup(moq_service.clear_receipt_cache)
        patches = [
            patch.object(db, "get_conn", temp_conn),
            patch.object(dispatch_router, "build_dispatch_groups",
                         lambda ids, dec: [g for g in self.groups if g["order_id"] in ids]),
            patch.object(low_stock, "_erp_session", erp_session),
            patch.object(ep, "_source_session", source_down),  # 供應商 / 單價查不到不影響數量
            patch.object(ep, "get_config", lambda: {}),
            patch.object(moq_service, "_query_last_receipts", fake_receipts),
            patch.object(moq_service, "site_moq_map", lambda: self.site_moq),
            patch.object(moq_service, "_erp_min_purch", lambda parts: {}),
            patch.object(sp, "local_now", lambda: NOW),
            patch.object(sp, "_already_ordered", lambda parts: [p.setdefault("already_ordered", []) for p in parts]),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def parts(self):
        res = sp.build_shortage_list([264, 262])
        return {p["part_number"]: p for p in res["parts"]}, res

    @staticmethod
    def kinds(allocs):
        return [(a["order_code"] or "-", a["qty"], a["kind"]) for a in allocs]

    def prepare(self, part, qty, **flags):
        raw = [{"supplier_id": "P0327", "lines": [
            {"part_number": part, "qty": qty, "price": 1, "pre_in_date": "2026-11-26", **flags}]}]
        return sp.prepare_groups(raw, [264, 262])[0]["lines"][0]


class Po20261105RegressionTests(Po20261105Base):
    def test_ec_10032a_suggests_shortage_not_10_8_package(self):
        p, res = self.parts()
        ec = p["EC-10032A"]
        self.assertEqual(res["pending_transfer"]["cutoff_code"], "10-3")
        # 缺 = 需求 10,000 + 待調撥（10-4 10,000 + 10-8 30,000 + 11-1 20,000）− ST 60,000 = 10,000；#264 自己不算待調撥
        self.assertEqual((ec["need"], ec["pending_transfer"], ec["st"], ec["shortage"]), (10000, 60000, 60000, 10000))
        self.assertEqual((ec["moq"], ec["moq_source"], ec["suggested_qty"]), (10000, "last_receipt", 10000))
        self.assertNotEqual(ec["suggested_qty"], 30000)  # 舊版：MOQ = 最近一張進貨（10-8 整包）30,000
        self.assertEqual((ec["overage_qty"], ec["overage_reason"]), (0, ""))
        self.assertEqual(self.kinds(ec["allocations"]), [("11-5", 10000.0, "order")])

    def test_other_parts_from_same_po_also_follow_shortage(self):
        p, _ = self.parts()
        for part, short in (("EC-20136A", 10000), ("EC-20097A", 15000), ("EC-20137A", 4000), ("EC-20087A", 4000)):
            row = p[part]
            self.assertEqual((row["shortage"], row["suggested_qty"]), (short, short), part)
            self.assertFalse([a for a in row["allocations"] if a["kind"] == "extra"], part)
        # EC-20117A：需求 30,000 + 待調撥 140,000 − ST 180,000 < 0 → 不用買（舊單卻開了 60,000）
        self.assertEqual((p["EC-20117A"]["shortage"], p["EC-20117A"]["suggested_qty"], p["EC-20117A"]["allocations"]), (0, 0, []))

    def test_default_po_qty_and_snapshot(self):
        p, _ = self.parts()
        line = self.prepare("EC-10032A", p["EC-10032A"]["suggested_qty"])
        self.assertEqual(line["qty"], 10000)
        self.assertEqual(self.kinds(line["allocations"]), [("11-5", 10000.0, "order")])
        snap = line["shortage_snapshot"]
        self.assertEqual(
            {k: snap[k] for k in ("need", "pending_transfer", "st", "shortage", "moq", "suggested_qty", "qty", "qty_edited",
                                  "overage_qty", "overage_reason")},
            {"need": 10000, "pending_transfer": 60000, "st": 60000, "shortage": 10000, "moq": 10000, "suggested_qty": 10000,
             "qty": 10000, "qty_edited": False, "overage_qty": 0, "overage_reason": ""})

    def test_old_qty_30000_without_edit_is_refused(self):
        """舊單的 30,000（沒有使用者改量）現在送進來會被擋，不會再默默開成「訂單 10,000 + 多買 20,000」。"""
        with self.assertRaises(ep.PoValidationError) as cm:
            self.prepare("EC-10032A", 30000)
        self.assertIn("超過建議量 10000", str(cm.exception))

    def test_snapshot_survives_normalize_and_lines_json(self):
        line = self.prepare("EC-10032A", 10000)
        req = ep.normalize_request({"supplier_id": "P0327", "lines": [line], "alloc_source": "shortage_po"})
        nl = req["lines"][0]
        self.assertEqual(nl["shortage_snapshot"]["suggested_qty"], 10000)
        planned = {**nl, "prod_id": "EC-10032A", "prod_name": "RES 1K 0402", "amount": 10000, "tax_amt": 500}
        out = ep._jsonable_lines([planned])[0]
        self.assertEqual(out["shortage_snapshot"]["shortage"], 10000)
        self.assertEqual([a["kind"] for a in out["allocations"]], ["order"])
        json.dumps(out, ensure_ascii=False)  # lines_json 可序列化


class MoqRoundTests(Po20261105Base):
    def test_moq_above_shortage_is_marked_moq_round(self):
        # 只有 1 張進貨（剛好是 10-8 整包）：GCD = 30,000 > 缺 10,000 → 建議量 30,000，多的照實標 MOQ 進位
        self.receipts["EC-10032A"] = [30000.0]
        p, _ = self.parts()
        ec = p["EC-10032A"]
        self.assertEqual((ec["shortage"], ec["moq"], ec["suggested_qty"]), (10000, 30000, 30000))
        self.assertEqual((ec["overage_qty"], ec["overage_reason"], ec["moq_round_qty"], ec["user_over_qty"]),
                         (20000, "moq", 20000, 0))
        self.assertEqual(self.kinds(ec["allocations"]), [("11-5", 10000.0, "order"), ("-", 20000.0, "moq_round")])
        line = self.prepare("EC-10032A", 30000)  # 沒改量、等於建議量 → 允許
        self.assertEqual((line["overage_reason"], line["moq_round_qty"]), ("moq", 20000))
        self.assertEqual(line["shortage_snapshot"]["overage_reason"], "moq")
        self.assertNotIn("extra", [a["kind"] for a in line["allocations"]])

    def test_site_moq_floor(self):
        self.site_moq = {"EC-20137A": 10000.0}  # 缺 4,000，GCD 4,000 < 主檔 MOQ 10,000
        p, _ = self.parts()
        row = p["EC-20137A"]
        self.assertEqual((row["suggested_qty"], row["moq_source"], row["overage_reason"], row["moq_round_qty"]),
                         (10000, "site_master", "moq", 6000))


class UserEditTests(Po20261105Base):
    def test_user_edit_above_suggested_is_user_over(self):
        line = self.prepare("EC-10032A", 25000, qty_edited=True)
        self.assertEqual(self.kinds(line["allocations"]), [("11-5", 10000.0, "order"), ("-", 15000.0, "user_over")])
        self.assertEqual((line["overage_qty"], line["overage_reason"], line["user_over_qty"], line["moq_round_qty"]),
                         (15000, "user", 15000, 0))
        snap = line["shortage_snapshot"]
        self.assertEqual((snap["qty"], snap["qty_edited"], snap["suggested_qty"], snap["overage_reason"]), (25000, True, 10000, "user"))

    def test_user_edit_on_top_of_moq_round_splits_reasons(self):
        self.receipts["EC-10032A"] = [30000.0]
        line = self.prepare("EC-10032A", 35000, qty_edited=True)
        self.assertEqual(self.kinds(line["allocations"]),
                         [("11-5", 10000.0, "order"), ("-", 20000.0, "moq_round"), ("-", 5000.0, "user_over")])
        self.assertEqual((line["moq_round_qty"], line["user_over_qty"], line["overage_reason"]), (20000, 5000, "user"))

    def test_user_edit_below_shortage_has_no_overage(self):
        line = self.prepare("EC-10032A", 6000, qty_edited=True)
        self.assertEqual(self.kinds(line["allocations"]), [("11-5", 6000.0, "order")])
        self.assertEqual((line["overage_qty"], line["overage_reason"]), (0, ""))


class SuggestedCapTests(unittest.TestCase):
    """沒有使用者改量時，建議量不得大於 max(缺, MOQ)；分配不會出現 extra，合計 = 採購量。"""

    def test_property_random(self):
        rnd = random.Random(20261105)
        for _ in range(500):
            shortage = rnd.choice([0, 0.5, 1, 999.2, 4000, 10000, 12345.6])
            moq = rnd.choice([0, 1, 1000, 10000, 30000])
            fields = moq_service.moq_fields(shortage, {"moq": moq, "moq_source": "last_receipt"})
            cap = max(-(-shortage // 1), moq) if shortage > 0 else 0
            self.assertLessEqual(fields["suggested_qty"], cap, (shortage, moq))
            over = sp.overage_breakdown(fields["suggested_qty"], fields["need_qty"], fields["suggested_qty"])
            self.assertEqual(over["user_over_qty"], 0)
            self.assertIn(over["overage_reason"], ("", "moq"))
            orders = [{"order_id": i, "code": f"1-{i}", "ship_date": f"2026-12-{10 + i}", "qty": rnd.choice([0, 100, 5000])}
                      for i in range(1, 4)]
            st, pending = rnd.choice([-500, 0, 3000, 60000]), rnd.choice([0, 2000, 60000])
            q = fields["suggested_qty"] + rnd.choice([0, 0, 777])
            allocs = sp.allocate_purchase_to_orders(orders, st, q, pending, fields["suggested_qty"] - fields["need_qty"])
            kinds = {a["kind"] for a in allocs}
            self.assertNotIn("extra", kinds)
            self.assertLessEqual(kinds, {"order", "pending_transfer", "moq_round", "user_over"})
            self.assertAlmostEqual(sum(a["qty"] for a in allocs), q, places=4)

    def test_erp_purchase_refuses_extra_kind(self):
        with self.assertRaises(ep.PoValidationError):
            ep.normalize_request({"supplier_id": "S1", "lines": [{"part_number": "EC-1", "qty": 10, "price": 1,
                                  "pre_in_date": "2026-11-26", "allocations": [{"order_id": 0, "qty": 5, "kind": "extra"}]}]})


class NoSparesWordingTests(unittest.TestCase):
    ROOT = Path(__file__).resolve().parents[1]

    def test_no_spares_word_in_shortage_paths(self):
        word = "備品"  # 「備品」
        for rel in ("app/services/shortage_purchase.py", "app/services/erp_purchase.py", "app/services/po_duplicates.py",
                    "app/models.py", "static/modules/shortage_po.js", "static/modules/erp_purchase.js"):
            self.assertNotIn(word, (self.ROOT / rel).read_text(encoding="utf-8"), rel)

    def test_completed_tab_keeps_sidebar(self):
        html = (self.ROOT / "static" / "index.html").read_text(encoding="utf-8")
        line = next(l for l in html.splitlines() if 'toggle("sidebar-hidden"' in l)
        self.assertIn('"main-preview-v2"', line)
        self.assertNotIn('"completed"', line.split("//")[0])

    def test_frontend_overage_note_with_node(self):
        import subprocess

        script = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source = fs.readFileSync('./static/modules/shortage_po.js', 'utf8')
  .replace(/^import [\s\S]*?;\r?\n/gm, '').replace(/^export /gm, '');
const ctx = vm.createContext({ console, document: { getElementById: () => null, querySelectorAll: () => [] }, window: {} });
vm.runInContext(source, ctx);
const run = code => vm.runInContext(code, ctx);
run(`globalThis.R = (o) => ({ needQty: 10000, suggested: 10000, moq: 10000, moqSource: 'last_receipt', lastReceipt: { count: 3 }, qtyEdited: false, linkedFrom: [], ...o });`);
assert.equal(run(`overageNote(R({ qty: '10000' }))`), '');
const moq = run(`overageNote(R({ qty: '30000', suggested: 30000, moq: 30000, lastReceipt: { count: 1 } }))`);
assert.ok(moq.startsWith('MOQ 進位：缺 10,000，MOQ 30,000，多買 20,000'), moq);
assert.ok(moq.includes('只依 1 張進貨'), moq);
assert.equal(run(`overageNote(R({ qty: '25000', qtyEdited: true }))`), '多買 15,000，原因：使用者改量');
const snap = run(`snapshotText({ need: 10000, pending_transfer: 60000, st: 60000, shortage: 10000, moq: 10000, suggested_qty: 10000, qty: 25000, moq_round_qty: 0, user_over_qty: 15000, overage_reason: 'user' })`);
assert.ok(snap.includes('多買 15,000，原因：使用者改量'), snap);
assert.ok(!/備品/.test(source));
"""
        root = self.ROOT
        done = subprocess.run(["node", "--input-type=module", "-e", script], cwd=root, capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)


if __name__ == "__main__":
    unittest.main()
