from __future__ import annotations

import csv
import io
import json
import tempfile
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import test_erp_purchase as tep
from app.services import erp_purchase as ep
from app.tools import erp_master_backfill as bf


class BackfillTests(tep.WriteFlowCase):
    def setUp(self):
        super().setUp()
        self.tgt = tep.make_target("CHIComp13")
        self.src.vendor_row = tep.RelatedMasterCopyTests.VENDOR
        acc = tep.RelatedMasterCopyTests.ACCOUNT
        self.src.related = {
            "comCustClass": [{"Flag": 2, "ClassID": "001", "Name": "c"}],
            "comArea": [{"AreaID": "A01", "Name": "a"}],
            "comCustDesc": [{"Flag": 2, "ID": "P0175", "DueTo": "月結"}],
            "comCustTrade": [{"Flag": 2, "ID": "P0175", "DayOfClose": 30, "LatelyTradeDate": 20260901, "LatelyReturnDate": 1}],
            "comCustAddress": [{"AddrID": "001", "Flag": 2, "ID": "P0175", "Addr": "台北"}],
            "comCustAccounts": [acc],
            "comProdLLC": [{"ProdID": "AC-40838B", "LLC": 1}],
            "ComCommoditySet": [{"ProdID": "AC-40838B", "CommodityID": "X", "Flag": 1}],
            "ComProdctCheckProject": [],
        }
        self.tgt.columns.update({t: list(r[0]) for t, r in self.src.related.items() if r})
        self.tgt.columns["comCustomer"] = list(tep.RelatedMasterCopyTests.VENDOR)
        self.tgt.columns["ComProdctCheckProject"] = ["ProdID", "RowNo"]
        # 目標：供應商 / 料號都已存在（舊版只複製了主表），關聯列全缺
        self.tgt.master_ids["comCustomer"] = {"P0175"}
        self.tgt.master_ids["comProduct"] = {"AC-40838B"}
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.backup = Path(self.tmp.name) / "bk"

    def run_tool(self, **kw):
        args = dict(target="CHIComp13", vendors=["P0175"], products=["AC-40838B"], auto=False, backup_dir=str(self.backup), apply=False)
        args.update(kw)
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = bf.run(**args)
        return rc, buf.getvalue()

    def test_dry_run_prints_plan_and_writes_nothing(self):
        rc, out = self.run_tool()
        self.assertEqual(rc, 0)
        for table in ("comCustDesc", "comCustTrade", "comCustAddress", "comCustAccounts", "comCustClass", "comArea", "comProdLLC", "ComCommoditySet"):
            self.assertIn(table, out)
        self.assertIn("dry-run", out)
        self.assertEqual((self.tgt.inserted, self.tgt.commits), ([], 0))
        self.assertFalse(self.backup.exists())

    def test_apply_exports_then_inserts_only_missing_in_one_transaction(self):
        self.tgt.related = {"comCustAddress": [{"AddrID": "001", "Flag": 2, "ID": "P0175", "Addr": "既有"}]}
        rc, out = self.run_tool(apply=True)
        self.assertEqual(rc, 0)
        tables = [i[0] for i in self.tgt.inserted]
        self.assertNotIn("comCustAddress", tables)  # 已有的不動
        self.assertNotIn("comCustomer", tables)
        self.assertNotIn("comProduct", tables)
        for t in ("comCustClass", "comArea", "comCustDesc", "comCustTrade", "comCustAccounts", "comProdLLC", "ComCommoditySet"):
            self.assertIn(t, tables)
        self.assertLess(tables.index("comCustClass"), tables.index("comCustDesc"))
        self.assertEqual(self.tgt.commits, 1)
        acc = next(i[1] for i in self.tgt.inserted if i[0] == "comCustAccounts")
        self.assertEqual((acc["CurPayable"], acc["StartExchRate"]), (0, 1.5))
        self.assertFalse([i for i in self.tgt.inserted if i[0].lower() == "comwareamount"])
        for sql, _ in self.tgt.log:
            self.assertFalse(sql.upper().startswith(("UPDATE", "DELETE")), sql)
        for sql, _ in self.src.log:
            self.assertTrue(sql.upper().startswith("SELECT"), sql)
        self.assertIn("已 commit", out)
        # 備份：每表 JSON + CSV（UTF-8）與 plan.json，備份的是寫入前目標的列
        self.assertTrue((self.backup / "plan.json").exists())
        addr = json.loads((self.backup / "comCustAddress.json").read_text(encoding="utf-8"))
        self.assertEqual(addr[0]["Addr"], "既有")
        with open(self.backup / "comCustomer.csv", encoding="utf-8-sig", newline="") as fh:
            self.assertEqual(next(csv.reader(fh))[:2], ["Flag", "ID"])
        plan = json.loads((self.backup / "plan.json").read_text(encoding="utf-8"))
        self.assertIn("comCustDesc", plan["to_insert"])
        self.assertNotIn("secret-pw", out + json.dumps(plan))

    def test_auto_uses_discovery(self):
        with patch.object(bf, "auto_vendors", lambda s, w: ["P0175"]), patch.object(bf, "auto_products", lambda s, w: ["AC-40838B"]):
            rc, out = self.run_tool(vendors=[], products=[], auto=True)
        self.assertEqual(rc, 0)
        self.assertIn("comProdLLC", out)

    def test_nothing_to_do(self):
        self.tgt.related = {t: list(r) for t, r in self.src.related.items()}
        rc, out = self.run_tool(apply=True)
        self.assertIn("沒有需要補", out)
        self.assertEqual((self.tgt.inserted, self.tgt.commits), ([], 0))

    def test_missing_vendor_in_target_is_skipped_not_created(self):
        self.tgt.master_ids["comCustomer"] = set()
        rc, out = self.run_tool(products=[], apply=True)
        self.assertIn("目標公司沒有此供應商", out)
        self.assertEqual(self.tgt.inserted, [])

    def test_refuses_production_non_test_and_source(self):
        for target in ("CHIComp03", "CHIComp09", "CHIComp"):
            with self.assertRaises(bf.BackfillError, msg=target):
                self.run_tool(target=target, apply=True)
        with patch.dict("os.environ", {"ERP_SQL_DATABASE": "CHIComp13"}):
            with self.assertRaises(bf.BackfillError):
                bf.check_target("CHIComp13")
        self.assertEqual((self.tgt.inserted, self.tgt.commits), ([], 0))

    def test_apply_requires_backup_dir_and_selection(self):
        with self.assertRaises(bf.BackfillError):
            self.run_tool(apply=True, backup_dir=None)
        with self.assertRaises(bf.BackfillError):
            self.run_tool(vendors=[], products=[], auto=False)

    def test_wrong_database_aborts_before_any_write(self):
        self.tgt.db_name = "CHIComp01"
        with self.assertRaises(Exception):
            self.run_tool(apply=True)
        self.assertEqual((self.tgt.inserted, self.tgt.commits), ([], 0))

    def test_insert_failure_rolls_back_without_commit(self):
        self.tgt.fail_insert = "comCustDesc"
        with self.assertRaises(bf.BackfillError) as cm:
            self.run_tool(apply=True)
        self.assertNotIn("secret-pw", str(cm.exception))
        self.assertEqual(self.tgt.commits, 0)
        self.assertGreaterEqual(self.tgt.rollbacks, 1)


for _name in dir(tep.WriteFlowCase):
    if _name.startswith("test_") and _name not in BackfillTests.__dict__:
        setattr(BackfillTests, _name, None)
