"""開 Invoice（外銷 ST- 單，TraBill Flag 501）：金額、表格欄位、S/C 回寫、請求模型、正式公司開關。

金額案例全部取自公司3實單（2026-11-10 唯讀研究）：ST-2026288（合併 3 張 S/C）、ST-2026285、ST-2026292。
本檔不連任何 ERP；只測純函式 build_plan / sc_status_updates / 開關與路由。
"""
from __future__ import annotations

import os
import subprocess
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from app.services import erp_invoice as inv
from app.services import erp_purchase as ep

ROOT = Path(__file__).resolve().parents[1]
D = Decimal

COMP03_PARAMS = {"cost_by_ware": False, "digits": 3, "dot": True}
COMP13_PARAMS = {"cost_by_ware": True, "digits": 5, "dot": False}


def _sc_main(no, *, cust="C0184", status=0, sales="002", dept="001"):
    return {"Flag": 2, "BillNo": no, "CustID": cust, "CurrID": "USD", "ExchRate": 30.385, "BillStatus": status,
            "SalesID": sales, "DeptID": dept, "Addr": "001", "InvoAddr": "", "InvAddress": "Somewhere Rd.",
            "IsDirectExport": True, "InvoiceType": 31, "NWUnit": "KG", "GWUnit": "KG", "VolumeUnit": "CU'FT",
            "Payment": "T/T", "DueTo": "", "Attn": "Mr. X"}


def _sc_sub(no, row, item, qty, remain, price):
    return {"Flag": 2, "BillNo": no, "RowNo": row, "ItemNo": item, "Description": f"{item} desc", "Quantity": qty,
            "QtyRemain": remain, "NotTranInvQty": remain, "NotTranPackingQty": qty, "TransedQty": 0, "Price": price}


def make_ctx(scs, picks, *, products, stocks=None, params=COMP03_PARAMS, rate=None, ware="001"):
    """scs = {sc_no: (main, [subs])}；picks = [(sc_no, row, qty, price 或 None)]；products = {ProdID: CAvgCost}。"""
    sc_map = {no: {"main": m, "subs": {s["RowNo"]: s for s in subs}} for no, (m, subs) in scs.items()}
    picked = [{"sc_no": no, "row_no": row, "qty": D(str(q)), "price": None if p is None else D(str(p)),
               "main": sc_map[no]["main"], "sub": sc_map[no]["subs"][row]} for no, row, q, p in picks]
    prods = list(dict.fromkeys(p["sub"]["ItemNo"] for p in picked))
    stocks = stocks or {prod: {ware: D(10 ** 6)} for prod in prods}
    return {
        "scs": sc_map, "picked": picked, "cust_id": next(iter(sc_map.values()))["main"]["CustID"], "curr": "USD",
        "currency_row": {"CurrencyID": "USD", "BuyInRate": 30.385, "FixedRate": 0, "AmountDeciDigits": 2},
        "customer": {"ShortName": "客戶"}, "missing_masters": [],
        "products": {p: {"ProdID": p, "CAvgCost": products[p], "CTotalCost": products[p] * 10 ** 6} for p in prods},
        "default_ware": ware, "wares": {p: ware for p in prods}, "ware_switched": {},
        "ware_amounts": {p: {"ProdID": p, "WareID": ware, "Quantity": stocks[p].get(ware, 0), "BCQty": stocks[p].get(ware, 0),
                             "TotalCost": 0, "Cost": products[p]} for p in prods},
        "stocks": stocks, "all_qty": {p: sum(stocks[p].values(), D(0)) for p in prods}, "cost_params": params,
        "req_exch_rate": None if rate is None else D(str(rate)), "hint": "", "ym_qty": {}, "ym_avg": {},
    }


def plan_of(ctx, inv_date=20261028, **kw):
    from datetime import datetime
    return inv.build_plan(ctx, target="CHIComp13", today=20261110, inv_date=inv_date, bill_no="ST-2026001",
                          maker="Andy", maker_id="017", client_ip="10.0.0.1", actor="web",
                          now=datetime(2026, 11, 10, 9, 0), **kw)


def rows_of(plan, table):
    return [v for t, v in plan["inserts"] if t == table]


def st_2026288_ctx():
    a, b, c = "MO-2607031", "MO-2607015", "MO-2608009"
    scs = {
        a: (_sc_main(a, sales="002"), [_sc_sub(a, 1, "10AB10001-007", 10000, 10000, 1.56),
                                       _sc_sub(a, 2, "10AB10001-004", 12000, 12000, 1.62)]),
        b: (_sc_main(b, sales="005"), [_sc_sub(b, 1, "10AB10001-008", 2000, 2000, 1.7)]),
        c: (_sc_main(c, sales="005"), [_sc_sub(c, 1, "P0C61V001-002", 10000, 10000, 1.95),
                                       _sc_sub(c, 2, "P0C61V001-001", 3000, 3000, 2.25)]),
    }
    picks = [(a, 1, 10000, None), (a, 2, 12000, None), (b, 1, 2000, None), (c, 1, 10000, None), (c, 2, 3000, None)]
    products = {"10AB10001-007": 31.06, "10AB10001-004": 32.49, "10AB10001-008": 34.08,
                "P0C61V001-002": 39.0, "P0C61V001-001": 43.51}
    return make_ctx(scs, picks, products=products, rate=32.215)


class InvoiceMoneyTests(unittest.TestCase):
    def test_line_money_matches_real_invoices(self):
        # (qty, price, Amount, MLPrice, MLAmount)：ST-2026288 / ST-2026285 / ST-2026292 實單
        cases = [
            (10000, "1.56", "15600", "50.255", "502554"), (12000, "1.62", "19440", "52.188", "626260"),
            (2000, "1.7", "3400", "54.766", "109531"), (10000, "1.95", "19500", "62.819", "628193"),
            (3000, "2.25", "6750", "72.484", "217451"), (1545, "5.03", "7771.35", "162.041", "250354"),
            (258, "2.67", "688.86", "86.014", "22192"), (360, "0.75", "270", "24.161", "8698"),
            (46, "0.4", "18.4", "12.886", "593"), (412, "1.02", "420.24", "32.859", "13538"),
            (7, "0.27", "1.89", "8.698", "61"), (2575, "0.27", "695.25", "8.698", "22397"),
        ]
        for qty, price, amount, ml_price, ml_amount in cases:
            m = inv.calc_line_money(qty, price, "32.215", 2)
            self.assertEqual(m["amount"], D(amount), (qty, price))
            self.assertEqual(m["ml_price"], D(ml_price), (qty, price))
            self.assertEqual(m["ml_amount"], D(ml_amount), (qty, price))

    def test_ml_amount_is_one_shot_not_amount_times_rate(self):
        # 258 × 2.67 × 32.215 = 22191.61 → 22192；若先 Amount=688.86 再 × 匯率會是 22191（實單 22192）
        self.assertEqual(inv.calc_line_money(258, "2.67", "32.215", 2)["ml_amount"], D(22192))

    def test_local_total_is_sum_of_ml_amount(self):
        # ST-2026288：Σ MLAmount = 2083989（實單）；round(64690 × 32.215) = 2083988 會差 1
        self.assertEqual(inv.calc_local_total([502554, 626260, 109531, 628193, 217451]), D(2083989))
        self.assertEqual(inv.calc_local_total([250354, 444631, 8698, 593, 13538, 22192]), D(740006))  # ST-2026285
        self.assertEqual(inv.calc_local_total([61, 22397]), D(22458))  # ST-2026292

    def test_cost_for_acc(self):
        self.assertEqual(inv.calc_cost_for_acc(3.39000774593339, 7), D(24))
        self.assertEqual(inv.calc_cost_for_acc(3.39000774593339, 2575), D(8729))
        self.assertEqual(inv.calc_cost_for_acc(12.1305555555556, 360), D(4367))
        self.assertEqual(inv.calc_cost_for_acc(31.06, 10000), D(310600))

    def test_bill_no_format(self):
        self.assertEqual(inv.make_bill_no(2026, 292), "ST-2026292")
        self.assertEqual(inv.make_bill_no(2026, 1), "ST-2026001")


class InvoicePlanTests(unittest.TestCase):
    def test_st_2026288_totals_and_header(self):
        plan = plan_of(st_2026288_ctx())
        t = plan["totals"]
        self.assertEqual(t["sum_qty"], D(37000))
        self.assertEqual(t["total"], D(64690))
        self.assertEqual(t["local_total"], D(2083989))
        self.assertEqual([l["ml_amount"] for l in plan["lines"]],
                         [D(502554), D(626260), D(109531), D(628193), D(217451)])
        self.assertEqual([l["cost_for_acc"] for l in plan["lines"]],
                         [D(310600), D(389880), D(68160), D(390000), D(130530)])

        head = rows_of(plan, "TraBillMain")[0]
        self.assertEqual((head["Flag"], head["BillNo"], head["OriginType"], head["OriginNo"]),
                         (501, "ST-2026001", "S/C", "MO-2607031"))
        self.assertIs(head["MulOriSCFlag"], False)  # 合併多張 S/C 實單仍是 False
        self.assertEqual(head["SumQty"], D(37000))
        for col in ("BillDate", "ExchRate", "Total", "CloseDate", "OnBoardDate", "InvoStyle", "BillStatus"):
            self.assertEqual(head[col], 0, col)
        for col in ("CustID", "CurrID", "SalesID", "Maker", "DeptID", "VoucherNo", "CVoucherNo"):
            self.assertEqual(head[col], "", col)
        self.assertEqual((head["InvoiceType"], head["NWUnit"], head["VolumeUnit"]), (31, "KG", "CU'FT"))

        acct = rows_of(plan, "comBillAccounts")[0]
        self.assertEqual((acct["Flag"], acct["SysID"], acct["CustFlag"], acct["FundBillNo"]), (501, 4, 1, "ST-2026001"))
        self.assertEqual((acct["CustID"], acct["DueTo"], acct["CurrID"]), ("C0184", "C0184", "USD"))
        self.assertEqual(acct["ExchRate"], D("32.215"))
        self.assertEqual((acct["Total"], acct["LocalTotal"]), (D(64690), D(2083989)))
        self.assertEqual((acct["AccMonth"], acct["PrepayDay"], acct["EncashDay"]), (202610, 20261028, 20261028))
        self.assertEqual((acct["OffSet"], acct["LocalOffSet"], acct["Tax"], acct["TaxType"]), (0, 0, 0, 0))
        self.assertEqual((acct["DeptID"], acct["Salesman"]), ("001", "002"))  # 第一張 S/C 的業務，不是另外兩張的 005
        self.assertEqual((acct["Status"], acct["CostFlag"], acct["InvoStyle"]), (1, 501, 1))
        self.assertIs(acct["PriceOfTax"], True)
        self.assertEqual((acct["VoucherNO"], acct["CVoucherNO"], acct["MakerID"]), ("", "", "017"))

    def test_trabillsub_quantity_is_always_one(self):
        plan = plan_of(st_2026288_ctx())
        subs = rows_of(plan, "TraBillSub")
        recs = rows_of(plan, "comProdRec")
        self.assertEqual([s["RowNo"] for s in subs], [1, 2, 3, 4, 5])  # 新建用連續 1..n
        for s, r, qty in zip(subs, recs, (10000, 12000, 2000, 10000, 3000)):
            self.assertEqual((s["Quantity"], s["Price"], s["Amount"], s["QtyRemain"], s["QtyCtn"]), (1, 0, 0, 1, 1))
            self.assertEqual((s["DeclareQty"], s["LegalQty"], s["LegalQtyB"]), (D(qty),) * 3)
            self.assertEqual((s["TranType"], s["FromRow"], s["NotTranInvQty"], s["NotTranPackingQty"]), (0, 0, 0, 0))
            self.assertIs(s["IsOdd"], False)
            self.assertEqual(r["Quantity"], D(qty))
            self.assertEqual(r["QtyRemain"], D(qty))
            self.assertEqual((r["TranType"], r["TaxRate"], r["TaxAmt"], r["UnitRelation"]), (404, 0, 0, 1))
            self.assertIs(r["IsOdd"], True)
            self.assertEqual(r["WareID"], "001")
        self.assertEqual([(s["OriginBillNo"], s["OriginRowNo"]) for s in subs],
                         [("MO-2607031", 1), ("MO-2607031", 2), ("MO-2607015", 1), ("MO-2608009", 1), ("MO-2608009", 2)])
        self.assertEqual([(r["FromNO"], r["FromRow"]) for r in recs],
                         [(s["OriginBillNo"], s["OriginRowNo"]) for s in subs])

    def test_st_2026292_sum_qty_and_cost(self):
        no = "MO-2608013"
        scs = {no: (_sc_main(no, cust="C0034"), [_sc_sub(no, 1, "OC-10232B", 773, 7, 0.27),
                                                 _sc_sub(no, 2, "OC-10232B", 2500, 2500, 0.27)])}
        ctx = make_ctx(scs, [(no, 1, 7, None), (no, 2, 2575, None)], products={"OC-10232B": 3.39000774593339}, rate=32.215)
        plan = plan_of(ctx, inv_date=20261030)
        self.assertEqual(plan["totals"]["sum_qty"], D(2582))
        self.assertEqual(plan["totals"]["total"], D("697.14"))
        self.assertEqual(plan["totals"]["local_total"], D(22458))
        recs = rows_of(plan, "comProdRec")
        self.assertEqual([r["CostForAcc"] for r in recs], [D(24), D(8729)])
        self.assertEqual([r["MLAmount"] for r in recs], [D(61), D(22397)])
        self.assertEqual(recs[0]["MLPrice"], D("8.698"))
        # 2500 開 2575：超開警告，需 confirm
        self.assertTrue(plan["needs_confirm"])
        self.assertEqual([c["kind"] for c in plan["confirm_items"]], ["over_remain"])

    def test_invoice_rate_overrides_sc_rate_and_defaults_to_sc_rate(self):
        ctx = st_2026288_ctx()
        ctx["req_exch_rate"] = None
        plan = plan_of(ctx)
        self.assertEqual((plan["exch_rate"], plan["rate_source"]), (D("30.385"), "sc"))
        self.assertEqual(plan["rate_hint"], {"sc": 30.385, "currency": 30.385})
        ctx["req_exch_rate"] = D("32.77")
        plan = plan_of(ctx)
        self.assertEqual((plan["exch_rate"], plan["rate_source"]), (D("32.77"), "manual"))
        self.assertEqual(rows_of(plan, "comBillAccounts")[0]["ExchRate"], D("32.77"))

    @staticmethod
    def _rate_ctx(sc_rates, picks=None, rate=None):
        """ST-2026304 情境：S/C 匯率 31.505、正航幣別檔 30.385；sc_rates = {sc_no: ExchRate（None = 沒這欄）}。"""
        scs = {}
        for i, (no, r) in enumerate(sc_rates.items(), 1):
            main = _sc_main(no)
            if r is None:
                del main["ExchRate"]
            else:
                main["ExchRate"] = r
            scs[no] = (main, [_sc_sub(no, 1, f"P{i}", 1000, 1000, 1.5)])
        picks = picks or [(no, 1, 1000, None) for no in sc_rates]
        products = {f"P{i}": 10.0 for i in range(1, len(sc_rates) + 1)}
        return make_ctx(scs, picks, products=products, rate=rate)

    def test_default_rate_is_first_sc_rate_like_st_2026304(self):
        plan = plan_of(self._rate_ctx({"MO-1": 31.505}))
        self.assertEqual((plan["exch_rate"], plan["rate_source"], plan["rate_required"]), (D("31.505"), "sc", False))
        self.assertEqual(plan["rate_hint"], {"sc": 31.505, "currency": 30.385})
        line = plan["lines"][0]
        self.assertEqual((line["amount"], line["ml_amount"]), (D(1500), D(47258)))  # 1500 × 31.505 = 47257.5
        self.assertEqual(rows_of(plan, "comBillAccounts")[0]["ExchRate"], D("31.505"))
        self.assertEqual(plan["totals"]["local_total"], D(47258))
        self.assertEqual([w["code"] for w in plan["warnings"] if w["code"] == "rate_required"], [])

    def test_multi_sc_uses_first_picked_sc_rate(self):
        rates = {"MO-1": 31.505, "MO-2": 32.0, "MO-3": 30.0}
        plan = plan_of(self._rate_ctx(rates))
        self.assertEqual((plan["exch_rate"], plan["rate_source"]), (D("31.505"), "sc"))
        # 勾選順序第一張是 MO-2 → 用 MO-2 的匯率，所有行都用同一個匯率
        picks = [("MO-2", 1, 1000, None), ("MO-1", 1, 1000, None), ("MO-3", 1, 1000, None)]
        plan = plan_of(self._rate_ctx(rates, picks))
        self.assertEqual((plan["exch_rate"], plan["rate_source"]), (D("32"), "sc"))
        self.assertEqual(plan["rate_hint"]["sc"], 32.0)
        self.assertEqual([l["ml_amount"] for l in plan["lines"]], [D(48000)] * 3)

    def test_sc_rate_zero_or_missing_falls_back_to_currency(self):
        for sc_rate in (0, None):
            plan = plan_of(self._rate_ctx({"MO-1": sc_rate}))
            self.assertEqual((plan["exch_rate"], plan["rate_source"]), (D("30.385"), "currency"), sc_rate)
            self.assertEqual(plan["rate_hint"], {"sc": None, "currency": 30.385})
        ctx = self._rate_ctx({"MO-1": 0})
        ctx["currency_row"].update(BuyInRate=0, FixedRate=31.0)
        self.assertEqual((plan_of(ctx)["exch_rate"], plan_of(ctx)["rate_source"]), (D("31"), "currency"))

    def test_both_rates_zero_is_missing(self):
        ctx = self._rate_ctx({"MO-1": 0})
        ctx["currency_row"].update(BuyInRate=0, FixedRate=0)
        plan = plan_of(ctx)
        self.assertEqual((plan["exch_rate"], plan["rate_source"], plan["rate_required"]), (D(0), "missing", True))
        self.assertEqual(plan["rate_hint"], {"sc": None, "currency": None})
        self.assertIn("rate_required", [w["code"] for w in plan["warnings"]])

    def test_manual_rate_beats_sc_rate(self):
        plan = plan_of(self._rate_ctx({"MO-1": 31.505}, rate="32.77"))
        self.assertEqual((plan["exch_rate"], plan["rate_source"]), (D("32.77"), "manual"))
        self.assertEqual(plan["rate_hint"], {"sc": 31.505, "currency": 30.385})

    def test_local_currency_rate_is_one(self):
        ctx = self._rate_ctx({"MO-1": 31.505})
        ctx["curr"] = "NTD"
        ctx["currency_row"] = {"CurrencyID": "NTD", "BuyInRate": 1, "FixedRate": 1, "AmountDeciDigits": 0}
        plan = plan_of(ctx)
        self.assertEqual((plan["exch_rate"], plan["rate_source"]), (D(1), "local"))

    def test_user_price_used(self):
        no = "MO-1"
        scs = {no: (_sc_main(no), [_sc_sub(no, 1, "P1", 100, 100, 1.0)])}
        plan = plan_of(make_ctx(scs, [(no, 1, 100, 1.25)], products={"P1": 0.5}, rate=32))
        r = rows_of(plan, "comProdRec")[0]
        self.assertEqual((r["Price"], r["SPrice"], r["Amount"]), (D("1.25"), D("1.25"), D("125")))
        self.assertIn("price_diff", [w["code"] for w in plan["warnings"]])

    def test_no_forbidden_tables(self):
        plan = plan_of(st_2026288_ctx())
        tables = {t for t, _ in plan["inserts"]} | {u["table"] for u in plan["updates"]} | {t for t, _ in plan["change_logs"]}
        for bad in ("accTranBill", "comFundSub", "comInvoice", "TraPackingMain", "TraPackingSub"):
            self.assertNotIn(bad, tables)
        log = plan["change_logs"][0][1]
        self.assertEqual((log["ProgID"], log["Action"], log["PKValue"], log["UserName"]),
                         ("ChiTrade.Invoice", 1, "ST-2026001", "017"))

    def test_rows_build_against_schema_columns(self):
        plan = plan_of(st_2026288_ctx())
        for table, values in plan["inserts"] + plan["change_logs"]:
            cols = [dict(name=k, identity=False, computed=False) for k in values]
            ep.build_insert(table, cols, values)

    def test_stock_short_needs_confirm_and_inventory_decreases(self):
        no = "MO-1"
        scs = {no: (_sc_main(no), [_sc_sub(no, 1, "P1", 100, 100, 1.0)])}
        ctx = make_ctx(scs, [(no, 1, 100, None)], products={"P1": 2.0}, stocks={"P1": {"001": D(40)}}, rate=32)
        plan = plan_of(ctx)
        self.assertTrue(plan["needs_confirm"])
        self.assertEqual([c["kind"] for c in plan["confirm_items"]], ["stock_short"])
        ware = [u for u in plan["updates"] if u["table"] == "comWareAmount"][0]
        self.assertEqual((ware["set"]["Quantity"], ware["set"]["BCQty"]), (D(-60), D(-60)))
        self.assertEqual(ware["set"]["LatestSalesDate"], 20261028)
        ym = [u for u in plan["updates"] if u["table"] == "StkYearMonthQty"][0]
        self.assertEqual(ym["values"]["Quantity"], D(-100))
        avg = [u for u in plan["updates"] if u["table"] == "stkYearMonthAvg"][0]
        self.assertEqual((avg["values"]["WareID"], avg["values"]["AtLastQuan"]), ("", D(-100)))  # CostByWare=0

    def _cost_plan(self, issue, stocks, *, params=COMP03_PARAMS, ware_total=D(888960), **kw):
        no = "MO-1"
        scs = {no: (_sc_main(no), [_sc_sub(no, 1, "P1", issue, issue, 1.0)])}
        ctx = make_ctx(scs, [(no, 1, issue, None)], products={"P1": 2.0}, stocks={"P1": stocks}, params=params, rate=32)
        ctx["ware_amounts"]["P1"]["TotalCost"] = ware_total
        plan = plan_of(ctx, **kw)
        ware = [u for u in plan["updates"] if u["table"] == "comWareAmount"][0]["set"]
        prod = [u for u in plan["updates"] if u["table"] == "comProduct"][0]["set"]
        cost = rows_of(plan, "comProdRec")[0]["CostForAcc"]
        return ware, prod, cost

    def test_update_cost_ware_qty_zero_keeps_total_minus_cost(self):
        # ST-2026304：倉庫數量歸 0，倉庫 TotalCost = 原值 - CostForAcc（不歸 0）；全倉總庫存 0 → CTotalCost = 0
        ware, prod, cost = self._cost_plan(40, {"001": D(40)})
        self.assertEqual(ware["Quantity"], D(0))
        self.assertGreater(cost, 0)
        self.assertEqual(ware["TotalCost"], D(888960) - cost)
        self.assertEqual(prod["CTotalCost"], D(0))
        ware13, _, _ = self._cost_plan(40, {"001": D(40)}, params=COMP13_PARAMS)
        self.assertNotIn("Cost", ware13)  # 數量 0：Cost 不動

    def test_update_cost_qty_remains_both_minus_cost(self):
        ware, prod, cost = self._cost_plan(40, {"001": D(100), "002": D(50)})
        self.assertEqual(ware["Quantity"], D(60))
        self.assertEqual(ware["TotalCost"], D(888960) - cost)
        self.assertEqual(prod["CTotalCost"], D(2.0) * 10 ** 6 - cost)
        ware13, _, _ = self._cost_plan(40, {"001": D(100)}, params=COMP13_PARAMS)
        self.assertIn("Cost", ware13)  # CostByWare=1 且數量 > 0：Cost = 新 TotalCost / 新 Quantity

    def test_update_cost_false_leaves_costs(self):
        for stocks in ({"001": D(40)}, {"001": D(100)}):
            ware, prod, _ = self._cost_plan(40, stocks, params=COMP13_PARAMS, update_cost=False)
            self.assertNotIn("TotalCost", ware)
            self.assertNotIn("Cost", ware)
            self.assertNotIn("CTotalCost", prod)

    def test_choose_warehouse_switches_when_default_empty(self):
        self.assertEqual(inv.choose_warehouse("001", D(700), {"02": D(6735)}), ("02", True))
        self.assertEqual(inv.choose_warehouse("001", D(100), {"001": D(500), "02": D(9000)}), ("001", False))
        self.assertEqual(inv.choose_warehouse("001", D(100), {"01": D(50), "02": D(50)}), ("01", True))
        self.assertEqual(inv.choose_warehouse("001", D(100), {}), ("001", False))


class ScWritebackTests(unittest.TestCase):
    def test_remain_and_not_tran_inv_decrease_packing_untouched(self):
        no = "MO-2609006"
        main = _sc_main(no, status=0)
        subs = [_sc_sub(no, 1, "A", 250, 250, 10.15), _sc_sub(no, 2, "B", 250, 250, 2.67), _sc_sub(no, 3, "B", 250, 250, 2.67)]
        sc = {no: {"main": main, "subs": {s["RowNo"]: s for s in subs}}}
        updates, closed, reopened = inv.sc_status_updates(sc, {(no, 2): D(258)}, 20261110)
        sub_up = [u for u in updates if u["table"] == "TraBillSub"]
        self.assertEqual(len(sub_up), 1)
        self.assertEqual(sub_up[0]["set"], {"QtyRemain": D(-8), "NotTranInvQty": D(-8)})  # 允許變負
        self.assertEqual(sub_up[0]["where"], {"Flag": 2, "BillNo": no, "RowNo": 2})
        self.assertNotIn("NotTranPackingQty", sub_up[0]["set"])
        self.assertNotIn("TransedQty", sub_up[0]["set"])
        self.assertEqual((closed, reopened), ([], []))  # 其他列還有剩

    def test_all_rows_done_closes_sc(self):
        no = "MO-2608013"
        subs = [_sc_sub(no, 1, "OC", 773, 7, 0.27), _sc_sub(no, 2, "OC", 2500, 2500, 0.27)]
        sc = {no: {"main": _sc_main(no, status=0), "subs": {s["RowNo"]: s for s in subs}}}
        updates, closed, _ = inv.sc_status_updates(sc, {(no, 1): D(7), (no, 2): D(2575)}, 20261110)
        self.assertEqual(closed, [no])
        head = [u for u in updates if u["table"] == "TraBillMain"][0]
        self.assertEqual(head["set"], {"BillStatus": 1, "Endacasedate": 20261110})

    def test_reopen_and_leave_other_status(self):
        no = "S1"
        subs = [_sc_sub(no, 1, "A", 100, 100, 1)]
        sc = {no: {"main": _sc_main(no, status=1), "subs": {1: subs[0]}}}
        updates, closed, reopened = inv.sc_status_updates(sc, {(no, 1): D(10)}, 20261110)
        self.assertEqual(reopened, [no])
        self.assertEqual([u for u in updates if u["table"] == "TraBillMain"][0]["set"]["BillStatus"], 0)
        sc[no]["main"] = _sc_main(no, status=2)  # 公司13示範資料 BillStatus=2：還有剩就不動
        updates, closed, reopened = inv.sc_status_updates(sc, {(no, 1): D(10)}, 20261110)
        self.assertEqual((closed, reopened), ([], []))
        self.assertFalse([u for u in updates if u["table"] == "TraBillMain"])

    def test_plan_includes_writeback(self):
        plan = plan_of(st_2026288_ctx())
        self.assertEqual(sorted(plan["closed_scs"]), ["MO-2607015", "MO-2607031", "MO-2608009"])
        subs = [u for u in plan["updates"] if u["table"] == "TraBillSub"]
        self.assertEqual(len(subs), 5)
        self.assertTrue(all(u["set"]["QtyRemain"] == 0 and u["set"]["NotTranInvQty"] == 0 for u in subs))


class InvoiceRequestTests(unittest.TestCase):
    def test_normalize_keeps_exch_rate(self):
        req = inv.normalize_request({"lines": [{"sc_no": "MO-1", "row_no": 1, "qty": 5}], "exch_rate": 32.215})
        self.assertEqual(req["exch_rate"], D("32.215"))
        self.assertIsNone(inv.normalize_request({"lines": [{"sc_no": "MO-1", "row_no": 1, "qty": 5}]})["exch_rate"])
        with self.assertRaises(inv.InvoiceValidationError):
            inv.normalize_request({"lines": [{"sc_no": "MO-1", "row_no": 1, "qty": 5}], "exch_rate": 0})
        with self.assertRaises(inv.InvoiceValidationError):
            inv.normalize_request({"lines": [{"sc_no": "MO-1", "row_no": 1, "qty": 5},
                                             {"sc_no": "MO-1", "row_no": 1, "qty": 3}]})

    def test_inv_date(self):
        self.assertEqual(inv.parse_inv_date(None, 20261110), 20261110)
        self.assertEqual(inv.parse_inv_date("2026-10-28", 20261110), 20261028)
        self.assertEqual(inv.parse_inv_date(20261030, 20261110), 20261030)
        with self.assertRaises(inv.InvoiceValidationError):
            inv.parse_inv_date(20261332, 20261110)


class InvoiceTargetTests(unittest.TestCase):
    CREDS = {"ERP_SQL_WRITE_USER_CHICOMP03": "dedicated", "ERP_SQL_WRITE_PASSWORD_CHICOMP03": "secret",
             "ERP_SQL_TEST_USER": "t", "ERP_SQL_TEST_PASSWORD": "t"}

    def test_default_target_is_comp13(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(inv.get_invoice_target(), "CHIComp13")
        with patch.dict(os.environ, {"ERP_SQL_TEST_USER": "t", "ERP_SQL_TEST_PASSWORD": "t"}, clear=True):
            self.assertEqual(inv.resolve_target(None)[0], "CHIComp13")

    def test_comp03_refused_by_default(self):
        with patch.dict(os.environ, self.CREDS, clear=True):
            with self.assertRaises(inv.InvoiceValidationError):
                inv.resolve_target("CHIComp03")
            cfg = inv.get_config()
            self.assertEqual(cfg["target"], "CHIComp13")

    def test_comp03_needs_all_three_keys(self):
        keys = {"ERP_INVOICE_TARGET": "CHIComp03", "ERP_ALLOW_PROD_INVOICE_CHICOMP03": "1",
                "ERP_ALLOW_PROD_WRITE_CHICOMP03": "1"}
        for missing in keys:
            env = {**self.CREDS, **{k: v for k, v in keys.items() if k != missing}}
            with patch.dict(os.environ, env, clear=True):
                with self.assertRaises(inv.InvoiceValidationError, msg=missing):
                    inv.resolve_target("CHIComp03")
                cfg = inv.get_config()
                if missing == "ERP_INVOICE_TARGET":
                    self.assertEqual(cfg["target"], "CHIComp13")  # 沒指定就回到測試公司
                else:
                    self.assertFalse(cfg["available"], missing)
        # 其他寫入開關（收料 / 採購 / 調撥）不能代替 Invoice 開關
        env = {**self.CREDS, "ERP_RECEIVE_TARGET": "CHIComp03", "ERP_ALLOW_PROD_WRITE_CHICOMP03": "1",
               "ERP_ALLOW_PROD_PO_CHICOMP03": "1", "ERP_ALLOW_PROD_TRANSFER_CHICOMP03": "1"}
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(inv.InvoiceValidationError):
                inv.resolve_target("CHIComp03")
        with patch.dict(os.environ, {**self.CREDS, **keys}, clear=True):
            target, _ = inv.resolve_target(None)
            self.assertEqual(target, "CHIComp03")  # 三把鑰匙都在才通過（本測試不連線、不寫入）

    def test_unknown_target_refused(self):
        with patch.dict(os.environ, self.CREDS, clear=True):
            with self.assertRaises(inv.InvoiceValidationError):
                inv.resolve_target("CHIComp99")

    def test_create_refuses_comp03_before_any_connection(self):
        with patch.dict(os.environ, self.CREDS, clear=True), \
                patch.object(ep, "_connect_write", side_effect=AssertionError("不可連線")):
            with self.assertRaises(inv.InvoiceValidationError):
                inv.create_invoice({"target": "CHIComp03", "lines": [{"sc_no": "MO-1", "row_no": 1, "qty": 1}]})


class _FakeConn:
    """假連線：不碰任何 ERP；cursor 只給 _plan / _write 傳遞用（相關查詢全部 patch 掉）。"""
    def __init__(self):
        self.committed = False

    def cursor(self):
        return object()

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        pass


class InvoicePreviewCreateRateTests(unittest.TestCase):
    """預覽與寫入都走 _plan → build_plan：沒帶匯率時兩邊的預設匯率、金額必須相同。"""
    RAW = {"target": None, "inv_date": 20261028, "lines": [{"sc_no": "MO-1", "row_no": 1, "qty": 1000}]}

    def _ctx(self, req_rate):
        ctx = InvoicePlanTests._rate_ctx({"MO-1": 31.505, "MO-2": 32.0})
        ctx["req_exch_rate"] = req_rate
        return ctx

    def _run(self, raw):
        executed = []
        patches = [
            patch.object(inv, "resolve_target", return_value=("CHIComp13", {})),
            patch.object(ep, "_connect_write", side_effect=lambda creds: _FakeConn()),
            patch.object(ep, "_check_db_name"),
            patch.object(inv, "_load_context", side_effect=lambda wcur, target, req, lock: self._ctx(req.get("exch_rate"))),
            patch.object(inv, "_load_rollup_state"),
            patch.object(inv, "next_invoice_serial", return_value=1),
            patch.object(inv, "existing_numbers", return_value=[]),
            patch.object(inv.er, "_execute_plan", side_effect=lambda wcur, plan: executed.append(plan)),
            patch.object(inv, "_verify", return_value={}),
        ]
        for pt in patches:
            pt.start()
        try:
            preview = inv.preview_invoice(dict(raw))
            req = inv.normalize_request(dict(raw, confirm=True))
            created = inv._write(req, "CHIComp13", {}, "web", "10.0.0.1")
        finally:
            for pt in reversed(patches):
                pt.stop()
        return preview, created, executed

    def test_preview_and_create_use_same_default_rate(self):
        preview, created, executed = self._run(self.RAW)
        self.assertEqual((preview["exch_rate"], preview["rate_source"]), (31.505, "sc"))
        for key in ("exch_rate", "rate_source", "rate_hint", "totals"):
            self.assertEqual(created[key], preview[key], key)
        self.assertEqual([l["ml_amount"] for l in created["lines"]], [l["ml_amount"] for l in preview["lines"]])
        self.assertEqual(len(executed), 1)
        self.assertEqual(rows_of(executed[0], "comBillAccounts")[0]["ExchRate"], D("31.505"))

    def test_preview_and_create_same_manual_rate(self):
        preview, created, _ = self._run({**self.RAW, "exch_rate": 32.77})
        self.assertEqual((preview["exch_rate"], preview["rate_source"]), (32.77, "manual"))
        self.assertEqual((created["exch_rate"], created["rate_source"]), (32.77, "manual"))

    def test_fingerprint_is_stable_for_same_request(self):
        fp = lambda raw: inv._fingerprint("CHIComp13", inv.normalize_request(raw))
        self.assertEqual(fp(self.RAW), fp(dict(self.RAW)))
        self.assertEqual(fp({**self.RAW, "exch_rate": 31.505}), fp({**self.RAW, "exch_rate": "31.505"}))
        self.assertNotEqual(fp({**self.RAW, "exch_rate": 31.505}), fp({**self.RAW, "exch_rate": 32.77}))

    def test_duplicate_submit_still_blocked_without_connecting(self):
        from app import database as db
        with patch.object(inv, "resolve_target", return_value=("CHIComp13", {})),                 patch.object(db, "find_recent_erp_invoice", return_value={"bill_no": "ST-2026001"}) as recent,                 patch.object(ep, "_connect_write", side_effect=AssertionError("不可連線")):
            for raw in (self.RAW, {**self.RAW, "exch_rate": 31.505}):
                with self.assertRaises(ep.DuplicateSubmitError):
                    inv.create_invoice(dict(raw))
        self.assertEqual(recent.call_count, 2)


class _TrackConn(_FakeConn):
    def __init__(self):
        super().__init__()
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


class InvoiceBillSerialTests(unittest.TestCase):
    """使用者自填 3 碼流水號：空白 = 自動；自填不呼叫 next_invoice_serial；已存在預覽擋、寫入交易內再擋。"""
    RAW = {"target": None, "inv_date": 20261028, "lines": [{"sc_no": "MO-1", "row_no": 1, "qty": 1000}]}

    def _patches(self, existing, executed, conn, serial=304):
        ctx = lambda wcur, target, req, lock: InvoicePlanTests._rate_ctx({"MO-1": 31.505})
        return [
            patch.object(inv, "resolve_target", return_value=("CHIComp13", {})),
            patch.object(ep, "_connect_write", side_effect=lambda creds: conn),
            patch.object(ep, "_check_db_name"),
            patch.object(inv, "_load_context", side_effect=ctx),
            patch.object(inv, "_load_rollup_state"),
            patch.object(inv, "next_invoice_serial", return_value=serial),
            patch.object(inv, "existing_numbers", side_effect=existing),
            patch.object(inv.er, "_execute_plan", side_effect=lambda wcur, plan: executed.append(plan)),
            patch.object(inv, "_verify", return_value={}),
        ]

    def _run(self, fn, existing=lambda cur, no, lock=True: [], conn=None):
        executed, conn = [], conn or _TrackConn()
        patches = self._patches(existing, executed, conn)
        for pt in patches:
            pt.start()
        try:
            return fn(), executed, conn, inv.next_invoice_serial
        finally:
            for pt in reversed(patches):
                pt.stop()

    def _req(self, serial):
        return inv.normalize_request({**self.RAW, "bill_serial": serial, "confirm": True})

    def test_parse_serial(self):
        year = lambda: 2026
        for value, want in ((None, None), ("", None), ("  ", None), (305, "305"), ("305", "305"), (" 305 ", "305"),
                            ("05", "005"), (5, "005"), ("5", "005"), ("ST-2026305", "305"), ("st-2026007", "007")):
            self.assertEqual(inv.parse_bill_serial(value, year), want, value)
        for bad in (0, "0", "000", 1000, "1000", "abc", "3.5", "-5", "３０５", True, "ST-2025305"):
            with self.assertRaises(inv.InvoiceValidationError, msg=bad):
                inv.parse_bill_serial(bad, year)

    def test_normalize_uses_invoice_year_for_full_number(self):
        self.assertEqual(self._req("ST-2026305")["bill_serial"], "305")
        self.assertIsNone(self._req("")["bill_serial"])
        with self.assertRaises(inv.InvoiceValidationError):
            inv.normalize_request({**self.RAW, "inv_date": 20251231, "bill_serial": "ST-2026305"})

    def test_blank_is_auto_next_number(self):
        r, _, _, nxt = self._run(lambda: inv.preview_invoice({**self.RAW, "bill_serial": ""}))
        self.assertEqual((r["bill_no"], r["bill_serial"], r["bill_serial_manual"], r["bill_prefix"]),
                         ("ST-2026304", "304", False, "ST-2026"))
        self.assertFalse(r["number_taken"])
        nxt.assert_called_once()

    def test_typed_serial_skips_next_serial(self):
        for typed, want in ((305, "ST-2026305"), ("5", "ST-2026005"), ("271", "ST-2026271")):  # 271 = 跳號，可補
            r, _, _, nxt = self._run(lambda: inv.preview_invoice({**self.RAW, "bill_serial": typed}))
            self.assertEqual((r["bill_no"], r["bill_serial_manual"]), (want, True), typed)
            self.assertFalse(r["number_taken"])
            nxt.assert_not_called()
            created, executed, conn, nxt = self._run(lambda: inv._write(self._req(typed), "CHIComp13", {}, "web", ""))
            self.assertEqual(created["bill_no"], want)
            self.assertEqual(rows_of(executed[0], "TraBillMain")[0]["BillNo"], want)
            self.assertTrue(conn.committed)
            nxt.assert_not_called()

    def test_taken_typed_number_preview_blocks(self):
        r, _, _, _ = self._run(lambda: inv.preview_invoice({**self.RAW, "bill_serial": "304"}),
                               existing=lambda cur, no, lock=True: ["TraBillMain", "comProdRec"])
        self.assertTrue(r["number_taken"])
        self.assertEqual(r["number_taken_in"], ["TraBillMain", "comProdRec"])
        w = [w for w in r["warnings"] if w["code"] == "number_taken"]
        self.assertEqual(len(w), 1)
        self.assertIs(w[0]["blocking"], True)
        self.assertIs(w[0]["needs_confirm"], False)
        self.assertIn("ST-2026304", w[0]["message"])
        self.assertIn("TraBillMain", w[0]["message"])
        self.assertTrue(r["lines"])  # 明細照樣算出來

    def test_create_with_taken_typed_number_writes_nothing(self):
        def boom():
            return inv._write(self._req("304"), "CHIComp13", {}, "web", "")
        with self.assertRaises(inv.InvoiceValidationError) as cm:
            self._run(boom, existing=lambda cur, no, lock=True: ["TraBillMain"])
        self.assertIn("ST-2026304", str(cm.exception))

    def test_race_free_at_preview_taken_at_write(self):
        existing = lambda cur, no, lock=True: ["comBillAccounts"] if lock else []
        r, _, _, _ = self._run(lambda: inv.preview_invoice({**self.RAW, "bill_serial": "306"}), existing=existing)
        self.assertFalse(r["number_taken"])
        conn, executed = _TrackConn(), []
        patches = self._patches(existing, executed, conn)
        for pt in patches:
            pt.start()
        try:
            with self.assertRaises(inv.InvoiceValidationError):
                inv._write(self._req("306"), "CHIComp13", {}, "web", "")
            inv.next_invoice_serial.assert_not_called()
        finally:
            for pt in reversed(patches):
                pt.stop()
        self.assertEqual(executed, [])
        self.assertFalse(conn.committed)
        self.assertGreaterEqual(conn.rollbacks, 1)

    def test_duplicate_key_with_typed_serial_does_not_retry(self):
        conn, calls = _TrackConn(), []

        def dup(wcur, plan):
            calls.append(plan["bill_no"])
            raise RuntimeError("Cannot insert duplicate key row")

        patches = self._patches(lambda cur, no, lock=True: [], [], conn)
        patches[7] = patch.object(inv.er, "_execute_plan", side_effect=dup)
        for pt in patches:
            pt.start()
        try:
            with patch.object(inv.er, "is_duplicate_key_error", return_value=True):
                with self.assertRaises(inv.InvoiceValidationError) as cm:
                    inv._write(self._req("307"), "CHIComp13", {}, "web", "")
        finally:
            for pt in reversed(patches):
                pt.stop()
        self.assertEqual(calls, ["ST-2026307"])  # 不換號、不重試
        self.assertIn("ST-2026307", str(cm.exception))
        self.assertFalse(conn.committed)
        self.assertGreaterEqual(conn.rollbacks, 1)

    def test_create_invoice_rejects_taken_number_and_logs_error(self):
        from app import database as db
        with patch.object(db, "find_erp_invoice_by_token", return_value=None), \
                patch.object(db, "find_recent_erp_invoice", return_value=None), \
                patch.object(db, "add_erp_invoice", return_value=1) as add, \
                patch.object(db, "log_activity"):
            with self.assertRaises(inv.InvoiceValidationError):
                self._run(lambda: inv.create_invoice({**self.RAW, "bill_serial": "304", "confirm": True}),
                          existing=lambda cur, no, lock=True: ["TraBillMain"])
        self.assertEqual(add.call_args[0][0]["status"], "error")

    def test_fingerprint_includes_serial(self):
        fp = lambda raw: inv._fingerprint("CHIComp13", inv.normalize_request(raw))
        self.assertNotEqual(fp(self.RAW), fp({**self.RAW, "bill_serial": "305"}))
        self.assertEqual(fp(self.RAW), fp({**self.RAW, "bill_serial": " "}))
        self.assertEqual(fp({**self.RAW, "bill_serial": 5}), fp({**self.RAW, "bill_serial": "005"}))
        self.assertNotEqual(fp({**self.RAW, "bill_serial": "305"}), fp({**self.RAW, "bill_serial": "306"}))


# 與 static/modules/erp_invoice.js buildBody() 送出的欄位一致
BODY = {
    "target": None,
    "inv_date": 20261028,
    "lines": [{"sc_no": "MO-2607031", "row_no": 1, "qty": 10000, "price": 1.56}],
    "confirm": False,
    "client_token": "tok",
    "update_cost": True,
    "exch_rate": 32.215,
    "bill_serial": None,
}


class InvoiceRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")  # 繞過 edit_auth
        from fastapi.testclient import TestClient
        from main import app
        cls.client = TestClient(app)

    def test_model_keeps_every_field_frontend_sends(self):
        from app.models import ErpInvoiceRequest
        req = ErpInvoiceRequest(**BODY).dict()
        self.assertEqual(set(req), set(BODY))
        self.assertEqual(set(req["lines"][0]), set(BODY["lines"][0]))
        self.assertEqual(req["exch_rate"], 32.215)
        self.assertIsNone(ErpInvoiceRequest(lines=[]).dict()["exch_rate"])
        self.assertEqual(ErpInvoiceRequest(lines=[], inv_date="2026-10-28").dict()["inv_date"], "2026-10-28")
        for typed in (305, "305", " 305 ", "05", "ST-2026305"):
            got = ErpInvoiceRequest(lines=[], bill_serial=typed).dict()["bill_serial"]
            self.assertEqual(inv.parse_bill_serial(got, lambda: 2026), "005" if typed == "05" else "305", typed)

    def test_preview_passes_exch_rate(self):
        for rate in (32.77, 32.215):
            seen = {}
            with patch("app.services.erp_invoice.preview_invoice", side_effect=lambda r: seen.update(r) or {"ok": True}):
                res = self.client.post("/api/erp-invoice/preview", json={**BODY, "exch_rate": rate})
            self.assertEqual(res.status_code, 200, res.text)
            self.assertEqual(seen["exch_rate"], rate)
            # 再經過 service 的 normalize 也不可變成 0 / None
            self.assertEqual(inv.normalize_request(seen)["exch_rate"], D(str(rate)))

    def test_create_passes_exch_rate_and_confirm_warning(self):
        seen = {}

        def fake_create(req, actor, ip):
            seen.update(req)
            raise inv.InvoiceConfirmWarning([{"kind": "stock_short", "part_number": "P1"}])

        with patch("app.services.erp_invoice.create_invoice", side_effect=fake_create):
            res = self.client.post("/api/erp-invoice/create", json={**BODY, "confirm": True})
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(res.json(), {"ok": False, "confirm_warning": [{"kind": "stock_short", "part_number": "P1"}]})
        self.assertEqual(seen["exch_rate"], 32.215)
        self.assertIs(seen["confirm"], True)

    def test_validation_error_is_400(self):
        with patch("app.services.erp_invoice.preview_invoice", side_effect=inv.InvoiceValidationError("x")):
            res = self.client.post("/api/erp-invoice/preview", json=BODY)
        self.assertEqual(res.status_code, 400)

    def test_open_lines_groups_by_customer_and_defaults_qty_remain(self):
        line = inv.open_line(("MO-1", 2, 20261001, "K005", "星際物流", "", "BPJ01", "desc", 1000, 700, 1.5, "USD", 30.385, "PCS"))
        self.assertEqual((line["customer_id"], line["customer_name"], line["qty_remain"]), ("K005", "星際物流", 700))
        self.assertEqual(line["date_text"], "2026-10-01")
        js = (ROOT / "static" / "modules" / "erp_invoice.js").read_text(encoding="utf-8")
        self.assertIn("groupKey: l => l.customer_id", js)
        self.assertIn("qtyOf: l => l.qty_remain", js)
        self.assertIn('from "/static/modules/open_line_picker.js"', js)
        self.assertEqual(line["customer_po"], "")  # 舊 14 欄 tuple 不會壞

    def _open_lines_with_fake_cursor(self, rows, **kwargs):
        seen = {}

        class _Cur:
            def execute(self, sql, params):
                seen["sql"], seen["params"] = sql, params

            def fetchall(self):
                return rows

        class _Conn(_FakeConn):
            def cursor(self):
                return _Cur()

        with patch.object(inv, "resolve_target", return_value=("CHIComp13", {})),                 patch.object(ep, "_connect_write", side_effect=lambda creds: _Conn()),                 patch.object(ep, "_check_db_name"):
            result = inv.list_open_lines(**kwargs)
        return result, seen

    def test_open_lines_q_searches_customer_po_ref_and_cust_bill_no(self):
        row = ("MO-2608006", 1, 20260815, "C0152", "客戶", "", "BPJ01", "desc", 100, 100, 1.5, "USD", 31.5, "PCS",
               " 4500059564 ")
        result, seen = self._open_lines_with_fake_cursor([row], q="4500059564")
        sql = seen["sql"]
        self.assertIn("m.Ref LIKE %s", sql)
        self.assertIn("m.CustBillNo LIKE %s", sql)
        self.assertIn("s.Unit, m.Ref FROM", sql)
        self.assertEqual(sql.count("%s"), len(seen["params"]))
        self.assertEqual(seen["params"], ("%4500059564%",) * 8)
        line = result["lines"][0]
        self.assertEqual((line["sc_no"], line["customer_po"], line["unit"]), ("MO-2608006", "4500059564", "PCS"))

    def test_open_lines_customer_and_q_param_count(self):
        _, seen = self._open_lines_with_fake_cursor([], customer="C0152", q="a_b")
        self.assertEqual(seen["sql"].count("%s"), len(seen["params"]))
        self.assertEqual(seen["params"][0], "C0152")
        self.assertEqual(len(seen["params"]), 9)
        js = (ROOT / "static" / "modules" / "erp_invoice.js").read_text(encoding="utf-8")
        self.assertIn("PO ${esc(l.customer_po)}", js)
        self.assertIn("搜尋客戶 / 客戶 PO / 料號 / S/C 單號 / 品名", js)


class InvoiceDatabaseTests(unittest.TestCase):
    def test_history_and_token_lookup(self):
        import tempfile
        from app import database as db
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        p1 = patch.object(db, "DB_PATH", Path(tmp.name) / "system.db")
        p2 = patch.object(db, "DATA_DIR", Path(tmp.name))
        p1.start(); p2.start()
        self.addCleanup(p1.stop); self.addCleanup(p2.stop)
        db.init_db()
        token = "tok-1"
        rid = db.add_erp_invoice({"target_db": "CHIComp13", "bill_no": "ST-2026001", "inv_date": 20261028,
                                  "customer_id": "K005", "currency": "USD", "exch_rate": 32.215, "total": 100,
                                  "client_token": token, "fingerprint": "fp-" + token, "status": "ok"})
        self.assertIsInstance(rid, int)
        self.assertEqual(db.find_erp_invoice_by_token(token)["bill_no"], "ST-2026001")
        self.assertIsNone(db.find_erp_invoice_by_token(""))
        self.assertEqual(db.find_recent_erp_invoice("fp-" + token, "2000-01-01")["exch_rate"], 32.215)
        self.assertTrue(any(r["id"] == rid for r in db.list_erp_invoices(10)))


class InvoiceFrontendTests(unittest.TestCase):
    def test_pure_helpers_with_node(self):
        script = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src = fs.readFileSync('./static/modules/erp_invoice.js', 'utf8').replace(/^import .*;\r?\n/gm, '');
const mod = await import('data:text/javascript,' + encodeURIComponent(src));
const lines = mod.invoiceLines([{ line: { sc_no: 'MO-1', row_no: 2 }, qty: 700, price: 1.5 }]);
assert.deepEqual(lines, [{ sc_no: 'MO-1', row_no: 2, qty: 700, price: 1.5 }]);
assert.throws(() => mod.invoiceLines([]));
assert.throws(() => mod.invoiceLines([{ line: { sc_no: 'MO-1', row_no: 2 }, qty: 0, price: 1 }]));
assert.equal(mod.parseRate('32.215'), 32.215);
assert.equal(mod.parseRate(''), null);
assert.equal(mod.parseRate('0'), null);
assert.equal(mod.parseSerial(''), '');
assert.equal(mod.parseSerial(' 305 '), '305');
assert.equal(mod.parseSerial('5'), '005');
assert.equal(mod.parseSerial('0'), null);
assert.equal(mod.parseSerial('abc'), null);
"""
        res = subprocess.run(["node", "--input-type=module", "-e", script], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(res.returncode, 0, res.stderr)

    def test_tab_wired_in_index(self):
        html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        self.assertIn('data-group="warehouse" data-tab="erp-invoice-tab">開 Invoice</button>', html)
        self.assertIn('id="tab-erp-invoice-tab"', html)
        self.assertIn('import { initErpInvoice, onErpInvoiceTabActivated } from "/static/modules/erp_invoice.js";', html)
        self.assertIn("onErpInvoiceTabActivated()", html)


if __name__ == "__main__":
    unittest.main()
