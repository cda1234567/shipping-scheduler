"""execute_bill 重構等價性：用假正航記錄每一次 SELECT／INSERT／UPDATE，與重構前錄下的 golden trace 逐筆比對。

重新錄製（只在確定寫入語意要改時）：
    UPDATE_TRANSFER_GOLDEN=1 python -m unittest tests.test_erp_transfer_equivalence
"""
from __future__ import annotations

import copy
import json
import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from app.services import erp_transfer as et

GOLDEN = Path(__file__).resolve().parent / "fixtures" / "erp_transfer_bill_trace.json"


class FakeErp:
    """只模擬 execute_bill 會碰到的表；所有呼叫依序記到 trace。"""

    def __init__(self, seed: dict):
        self.products = copy.deepcopy(seed["products"])
        self.stocks = {tuple(k.split("|")): v for k, v in copy.deepcopy(seed["stocks"]).items()}
        self.rolls = {tuple(k.split("|")): v for k, v in copy.deepcopy(seed.get("rolls", {})).items()}
        self.avgs = {tuple(k.split("|")): v for k, v in copy.deepcopy(seed.get("avgs", {})).items()}
        self.trace: list = []
        self.inserted: list = []
        self._next = None

    # cursor 介面
    def execute(self, sql, args=()):
        self.trace.append(["execute", sql, list(args)])
        if sql.startswith("SELECT COUNT"):
            table = sql.split()[3]
            number = args[0]
            self._next = (sum(1 for t, _ in self.inserted if t == table and number in (_.get("MoveNO"), _.get("BillNO"))),)
        elif sql.startswith("SELECT SUM(Quantity)"):
            self._next = (sum(v["Quantity"] for (p, _), v in self.stocks.items() if p == args[0]),)
        elif sql.startswith("SELECT SumQty"):
            self._next = (next(v["SumQty"] for t, v in self.inserted if t == "stkMoveMain"),)
        elif sql.startswith("SELECT Quantity FROM comWareAmount"):
            self._next = (self.stocks[(args[0], args[1])]["Quantity"],)
        elif sql.startswith("UPDATE comProduct"):
            self.products[args[2]].update(LatestIndate=args[0], LatestOutDate=args[1])
        elif sql.startswith("UPDATE "):
            pass  # build_update 已在 fake_update 套用

    def fetchone(self):
        return self._next

    # 被 patch 的模組函式
    def one(self, cur, sql, args):
        self.trace.append(["one", sql, list(args)])
        if "FROM comProduct" in sql:
            return copy.deepcopy(self.products.get(args[0]))
        if "FROM comWareAmount" in sql:
            return copy.deepcopy(self.stocks.get((args[0], args[1])))
        if "FROM StkYearMonthQty" in sql:
            return copy.deepcopy(self.rolls.get((str(args[0]), args[1], args[2])))
        if "FROM stkYearMonthAvg" in sql:
            return copy.deepcopy(self.avgs.get((str(args[0]), args[1], args[2])))
        raise AssertionError(sql)

    def insert(self, cur, table, values):
        self.trace.append(["insert", table, values])
        self.inserted.append((table, dict(values)))
        if table == "comWareAmount":
            self.stocks[(values["ProdID"], values["WareID"])] = dict(values)
        elif table == "StkYearMonthQty":
            self.rolls[(str(values["YearMonth"]), values["ProdID"], values["WareID"])] = dict(values)
        elif table == "stkYearMonthAvg":
            self.avgs[(str(values["YearMonth"]), values["ProdID"], values["WareID"])] = dict(values)

    def update(self, table, values, where):
        self.trace.append(["update", table, dict(values), dict(where)])
        if table == "comWareAmount":
            self.stocks[(where["ProdID"], where["WareID"])].update(values)
        elif table == "StkYearMonthQty":
            self.rolls[(str(where["YearMonth"]), where["ProdID"], where["WareID"])].update(values)
        elif table == "stkYearMonthAvg":
            self.avgs[(str(where["YearMonth"]), where["ProdID"], where["WareID"])].update(values)
        else:
            raise AssertionError(table)
        return "UPDATE " + table, tuple(values.values()) + tuple(where.values())


def _seed_stock(qty, cost=21, total=None):
    return {"Quantity": qty, "BCQty": qty, "Cost": cost, "TotalCost": qty * cost if total is None else total}


SCENARIOS = {
    # 公司3：CostByWare=0，一般料 + 客供料同單，015 已有庫存／沒有庫存，月檔有／沒有
    "company_cost": dict(
        cost_by_ware=False,
        seed=dict(
            products={"P": {"ProdID": "P", "ProdName": "面板", "CAvgCost": 21, "CTotalCost": 63000},
                      "Q-TAB": {"ProdID": "Q-TAB", "ProdName": "客供", "CAvgCost": 0, "CTotalCost": 0},
                      "R": {"ProdID": "R", "ProdName": "電阻", "CAvgCost": 0.5, "CTotalCost": 250}},
            stocks={"P|001": _seed_stock(2000), "P|015": _seed_stock(1000, 0, 0),
                    "Q-TAB|002": _seed_stock(80, 0), "R|001": _seed_stock(500, 0.5)},
            rolls={"202611|P|001": {"Quantity": 2000}, "202611|Q-TAB|002": {"Quantity": 80}},
            avgs={"202611|P|": {"AtLastQuan": 1, "MonthCost": 1, "AtLastCost": 1},
                  "202610|P|": {"AtLastQuan": 999, "MonthCost": 9, "AtLastCost": 9}}),
        rows=[dict(part="P", name="面板", ware_out="001", transfer=1500, cost=21),
              dict(part="Q-TAB", name="客供", ware_out="002", transfer=80, cost=0),
              dict(part="ZERO", name="不撥", ware_out="001", transfer=0, cost=1),
              dict(part="R", name="電阻", ware_out="001", transfer=123, cost=0.5)]),
    # 公司13：CostByWare=1，倉庫成本、AtLastQuan 只加減
    "ware_cost": dict(
        cost_by_ware=True,
        seed=dict(
            products={"P": {"ProdID": "P", "ProdName": "面板", "CAvgCost": 21, "CTotalCost": 63000},
                      "Q-TAB": {"ProdID": "Q-TAB", "ProdName": "客供", "CAvgCost": 3, "CTotalCost": 300}},
            stocks={"P|001": _seed_stock(2000, 20.5), "P|015": _seed_stock(100, 30),
                    "Q-TAB|002": _seed_stock(100, 3)},
            rolls={"202611|P|015": {"Quantity": 100}},
            avgs={"202611|P|001": {"AtLastQuan": 2000}, "202611|P|015": {"AtLastQuan": 100}}),
        rows=[dict(part="P", name="面板", ware_out="001", transfer=700, cost=20.5),
              dict(part="Q-TAB", name="客供", ware_out="002", transfer=40, cost=3)]),
}


def run_scenario(name: str) -> list:
    spec = SCENARIOS[name]
    fake = FakeErp(spec["seed"])
    params = dict(cost_by_ware=spec["cost_by_ware"], digits=3, dot=True)
    with patch.object(et, "one", side_effect=fake.one), \
         patch.object(et, "insert", side_effect=fake.insert), \
         patch.object(et, "ensure_masters"), \
         patch.object(et, "next_number", return_value="RM-20261109001"), \
         patch.object(et.er, "load_cost_params", return_value=params), \
         patch.object(et.er, "build_update", side_effect=fake.update), \
         patch.object(et, "local_now", return_value=datetime(2026, 11, 9, 10, 30, 0)), \
         patch.object(et.socket, "gethostname", return_value="HOST"), \
         patch.dict(os.environ, {"ERP_PO_MAKER": "Andy", "ERP_PO_MAKER_ID": "017"}):
        result = et.execute_bill(fake, "CHIComp03", dict(rows=copy.deepcopy(spec["rows"])), 20261109,
                                 actor="web", client_ip="10.0.0.5")
    state = dict(stocks={"|".join(k): v for k, v in sorted(fake.stocks.items())},
                 rolls={"|".join(k): v for k, v in sorted(fake.rolls.items())},
                 avgs={"|".join(k): v for k, v in sorted(fake.avgs.items())},
                 products=fake.products)
    return json.loads(json.dumps(dict(result=result, trace=fake.trace, state=state), ensure_ascii=False))


class ExecuteBillEquivalenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("UPDATE_TRANSFER_GOLDEN") == "1":
            GOLDEN.write_text(json.dumps({n: run_scenario(n) for n in SCENARIOS}, ensure_ascii=False, indent=1),
                              encoding="utf-8")
        cls.golden = json.loads(GOLDEN.read_text(encoding="utf-8"))

    def test_trace_identical_to_golden(self):
        for name in SCENARIOS:
            with self.subTest(name):
                actual = run_scenario(name)
                expected = self.golden[name]
                self.assertEqual(actual["result"], expected["result"])
                self.assertEqual(len(actual["trace"]), len(expected["trace"]))
                for i, (a, e) in enumerate(zip(actual["trace"], expected["trace"])):
                    self.assertEqual(a, e, f"第 {i} 筆呼叫不同")
                self.assertEqual(actual["state"], expected["state"])

    def test_golden_covers_required_semantics(self):
        """golden 本身要符合 RM-20261109001 對過的寫入規則，避免錄到錯的行為。"""
        company = self.golden["company_cost"]
        inserts = [(t, v) for kind, t, *rest in company["trace"] if kind == "insert" for v in rest]
        tables = [t for t, _ in inserts]
        self.assertEqual(tables.count("stkMoveMain"), 1)  # 一個批次一張單
        recs = [v for t, v in inserts if t == "comProdRec"]
        self.assertEqual([(r["ProdID"], r["WareOut"], r["WareID"]) for r in recs],
                         [("P", "001", "015"), ("Q-TAB", "002", "015"), ("R", "001", "015")])
        self.assertTrue(all(r["BillDate"] == 20261109 for r in recs))
        logs = [v for t, v in inserts if t == "comChangeLog"]
        self.assertEqual(len(logs), 1)
        self.assertEqual((logs[0]["ProgID"], logs[0]["Action"], logs[0]["PKValue"], logs[0]["UserName"]),
                         ("ChiStock.Move", 1, "RM-20261109001", "017"))
        for state in (company["state"], self.golden["ware_cost"]["state"]):
            for v in state["stocks"].values():
                self.assertEqual(v["BCQty"], v["Quantity"])
        # CostByWare=0：只有當月 WareID=''，上月不動不新建，不改 CAvgCost/CTotalCost 與各倉成本
        avgs = company["state"]["avgs"]
        self.assertEqual(set(avgs), {"202611|P|", "202610|P|", "202611|Q-TAB|", "202611|R|"})
        self.assertEqual(avgs["202610|P|"], {"AtLastQuan": 999, "MonthCost": 9, "AtLastCost": 9})
        self.assertEqual(avgs["202611|P|"]["AtLastQuan"], 3000)
        self.assertEqual(company["state"]["products"]["P"]["CAvgCost"], 21)
        self.assertEqual(company["state"]["products"]["P"]["CTotalCost"], 63000)
        self.assertEqual(company["state"]["stocks"]["P|001"]["TotalCost"], 42000)
        self.assertEqual(company["state"]["stocks"]["P|015"]["Cost"], 0)
        for kind, *rest in company["trace"]:
            if kind == "update":
                self.assertNotIn("CAvgCost", rest[1])
                self.assertNotIn("CTotalCost", rest[1])
        # CostByWare=1：AtLastQuan 只加減當月
        ware_avgs = self.golden["ware_cost"]["state"]["avgs"]
        self.assertEqual(ware_avgs["202611|P|001"]["AtLastQuan"], 1300)
        self.assertEqual(ware_avgs["202611|P|015"]["AtLastQuan"], 800)
        self.assertEqual(ware_avgs["202611|Q-TAB|002"]["AtLastQuan"], -40)
        self.assertFalse(any(k.startswith("202610") for k in ware_avgs))


if __name__ == "__main__":
    unittest.main()
