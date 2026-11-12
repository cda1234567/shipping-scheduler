"""v2026.11.11.7：低庫存改用廠商交期設定判斷、新可用量（含 015 未領需求）、推定廠商、待確認區、已下單標註。"""
from __future__ import annotations

import os
import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

from fastapi.testclient import TestClient

import app.database as db
from app.services import available_qty as aq
from app.services import low_stock
from app.services import low_stock_plan as lp
from app.services import part_conversion
from app.services import st_pending_transfer as spt

from tests.test_shortage_purchase import Base

TODAY = date(2026, 11, 11)


# ── 新可用量（純函式） ─────────────────────────────────────────────────────

def demand(oid, code, po, parts, bom=None, source=aq.SOURCE_DISPATCH):
    return {"order_id": oid, "code": code, "po": aq.po_key(po), "po_number": po, "model": "M", "order_qty": 0,
            "source": source, "parts": parts, "bom_parts": parts if bom is None else bom}


class AvailableFormulaTests(unittest.TestCase):
    def test_oc10888b_is_538(self):
        """手算案例：ST 800、待調撥 800（11-6 扣 800）、015 238；11-6 250 + 12-2 250 未領；舊 PO 都已領完。"""
        p = "OC-10888B"
        demands = [demand(i, c, po, {p: 250.0 if po != "4500059362" else 200.0}) for i, c, po in (
            (101, "12-10", "4500059105"), (102, "1-4", "4500059162"), (103, "4-6", "4500059323"),
            (104, "6-10", "4500059362"), (105, "8-9", "4500059418"),
            (270, "11-6", "4500059674"), (272, "12-2", "4500059664"))]
        issued = {("4500059105", p): 250.0, ("4500059162", p): 250.0, ("4500059323", p): 250.0,
                  ("4500059362", p): 200.0, ("4500059418", p): 250.0}
        un = aq.unissued_by_part(demands, issued)[p]
        self.assertEqual(un["unissued"], 500)
        by_po = {d["po"]: d["unissued"] for d in un["detail"]}
        for po in ("4500059105", "4500059162", "4500059323", "4500059362", "4500059418"):
            self.assertEqual(by_po[po], 0, po)  # 已領完 = 0
        self.assertEqual((by_po["4500059674"], by_po["4500059664"]), (250, 250))
        a = aq.calc_available(800, 800, 238, un["unissued"])
        self.assertEqual((a["free_st"], a["pending"], a["available"], a["available_without_free_st"]), (0, 800, 538, 538))

    def test_same_po_multiple_batches_summed_before_issued(self):
        demands = [demand(1, "10-4", "4500059613", {"EC-1": 100.0}), demand(2, "10-5", "4500059613", {"EC-1": 150.0})]
        un = aq.unissued_by_part(demands, {("4500059613", "EC-1"): 200.0})["EC-1"]
        self.assertEqual(un["unissued"], 50)
        self.assertEqual(un["detail"][0]["codes"], ["10-4", "10-5"])
        self.assertEqual((un["detail"][0]["need"], un["detail"][0]["issued"]), (250, 200))

    def test_each_batch_counted_once(self):
        """需求只看派工紀錄（每批一次），不管 ST 扣了幾包、由哪次補料供應。"""
        orders = [{"id": 270, "code": "11-6", "po_number": "4500059674", "model": "TDA3-3", "order_qty": 250},
                  {"id": 272, "code": "12-2", "po_number": "4500059664", "model": "TDA3-3", "order_qty": 250}]
        records = {270: [{"part_number": "OC-10888B", "needed_qty": 250}],
                   272: [{"part_number": "oc-10888b ", "needed_qty": 250}]}
        bom = {"TDA3-3": [{"part_number": "OC-10888B", "qty_per_board": 1, "scrap_factor": 0, "needed_qty": 0}]}
        d = aq.order_demands(orders, records, bom, {})
        un = aq.unissued_by_part(d, {})["OC-10888B"]
        self.assertEqual(un["unissued"], 500)
        self.assertEqual([x["po"] for x in un["detail"]], ["4500059664", "4500059674"])

    def test_po_suffix_maps_to_po(self):
        self.assertEqual(aq.po_key("4500059418-B"), "4500059418")
        self.assertEqual(aq.po_key(" 4500059418 "), "4500059418")
        issued = aq.convert_issued({("4500059418-A", "EC-1"): 100.0, ("4500059418-B", "EC-1"): 50.0}, {})
        self.assertEqual(issued, {("4500059418", "EC-1"): 150.0})
        un = aq.unissued_by_part([demand(1, "8-1", "4500059418", {"EC-1": 200.0})], issued)
        self.assertEqual(un["EC-1"]["unissued"], 50)

    def test_conversion_both_sides(self):
        conv = {"A-OLD": "A-NEW"}
        orders = [{"id": 1, "code": "11-1", "po_number": "P1", "model": "X", "order_qty": 10}]
        records = {1: [{"part_number": "A-OLD", "needed_qty": 300}]}
        d = aq.order_demands(orders, records, {}, conv)
        self.assertEqual(d[0]["parts"], {"A-NEW": 300})
        issued = aq.convert_issued({("P1", "A-OLD"): 100.0, ("P1", "A-NEW"): 50.0}, conv)
        un = aq.unissued_by_part(d, issued)
        self.assertNotIn("A-OLD", un)
        self.assertEqual(un["A-NEW"]["unissued"], 150)
        self.assertIn("A-OLD", aq.issued_query_parts(["A-NEW"], d, conv))  # 查已領時帶舊料號

    def test_free_st_both_values(self):
        a = aq.calc_available(1000, 200, 50, 100)
        self.assertEqual((a["free_st"], a["pending"]), (800, 200))
        self.assertEqual(a["available"], 800 + 200 + 50 - 100)
        self.assertEqual(a["available_without_free_st"], 200 + 50 - 100)
        b = aq.calc_available(800, 800, 238, 500)
        self.assertEqual(b["available"], b["available_without_free_st"])  # 自由 ST = 0 時相同

    def test_pending_capped_by_st_and_flagged(self):
        a = aq.calc_available(0, 10000, 5, 0)
        self.assertEqual((a["pending"], a["pending_raw"], a["available"]), (0, 10000, 5))
        self.assertTrue(a["pending_over_st"])
        self.assertEqual(aq.calc_available(0, 0, 0, 10)["available"], -10)  # 可以是負數

    def test_dispatch_records_preferred_bom_fallback_and_no_bom(self):
        orders = [{"id": 1, "code": "1-1", "po_number": "P1", "model": "M1", "order_qty": 100},
                  {"id": 2, "code": "1-2", "po_number": "P2", "model": "M1", "order_qty": 100},
                  {"id": 3, "code": "1-3", "po_number": "P3", "model": "NOBOM", "order_qty": 100}]
        bom = {"M1": [{"part_number": "EC-1", "qty_per_board": 2, "scrap_factor": "5%", "needed_qty": 0},
                      {"part_number": "EC-DASH", "qty_per_board": 1, "is_dash": True}]}
        records = {1: [{"part_number": "EC-1", "needed_qty": 199}]}
        d = {x["order_id"]: x for x in aq.order_demands(orders, records, bom, {})}
        self.assertEqual((d[1]["source"], d[1]["parts"]["EC-1"]), (aq.SOURCE_DISPATCH, 199))
        self.assertAlmostEqual(d[1]["bom_parts"]["EC-1"], 210)  # BOM 版另列（含損耗）
        self.assertEqual(d[2]["source"], aq.SOURCE_BOM)
        self.assertAlmostEqual(d[2]["parts"]["EC-1"], 210)
        self.assertNotIn("EC-DASH", d[2]["parts"])
        self.assertEqual((d[3]["source"], d[3]["parts"], d[3]["bom_parts"]), (aq.SOURCE_NO_BOM, {}, None))
        un = aq.unissued_by_part(list(d.values()), {})["EC-1"]
        self.assertEqual({x["po"]: x["bom_need"] for x in un["detail"]}, {"P1": 210, "P2": 210})

    def test_tab_parts_excluded(self):
        for p in ("OC-10935B-TAB-C", "ab-tab", "X-Tab"):
            self.assertTrue(aq.is_tab(p), p)
        self.assertFalse(aq.is_tab("OC-10935B"))
        un = aq.unissued_by_part([demand(1, "1-1", "P", {"OC-10935B-TAB-C": 5.0, "EC-1": 1.0})], {})
        self.assertEqual(set(un), {"EC-1"})


class FakeIssuedCursor:
    """prdProdRec ⨝ prdMKOrdMain ⨝ comProdRec：依 WareID 參數篩選。"""

    def __init__(self, lines, sqls):
        self.lines = lines  # (CustBill, ProdID, WareID, Quantity)
        self.sqls = sqls
        self.rows = []

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        self.sqls.append((s, params))
        assert s.startswith("SELECT") and "INSERT" not in s.upper() and "UPDATE" not in s.upper()
        ware, *parts = params
        agg = {}
        for bill, prod, w, qty in self.lines:
            if w == ware and prod in parts:
                agg[(bill, prod)] = agg.get((bill, prod), 0) + qty
        self.rows = [(b, p, q) for (b, p), q in agg.items()]

    def fetchall(self):
        return list(self.rows)


class IssuedQueryTests(unittest.TestCase):
    def test_only_015_and_sign(self):
        sqls = []
        lines = [("4500059418-A", "EC-1", "015", -100), ("4500059418-B", "EC-1", "015", -50),
                 ("4500059418-A", "EC-1", "001", -999),  # 非 015 倉：不算
                 ("4500059418-A", "EC-1", "015", 10)]   # 沖銷（正數）
        conn = type("C", (), {"cursor": lambda self: FakeIssuedCursor(lines, sqls)})()
        got = aq.query_issued_by_po(conn, "015", ["ec-1"])
        self.assertEqual(got, {("4500059418", "EC-1"): 140})
        s, params = sqls[0]
        self.assertIn("p.Flag = 331", s)
        self.assertIn("c.Flag = 331", s)
        self.assertIn("c.WareID = %s", s)
        self.assertIn("prdMKOrdMain", s)
        self.assertEqual(params[0], "015")


# ── 廠商歸屬與有效交期 ─────────────────────────────────────────────────────

class SupplierLeadTests(unittest.TestCase):
    def test_supplier_attribution(self):
        self.assertEqual(lp.resolve_supplier({"major_supplier_id": "P1", "last_supplier_id": "P2"}, None), ("P1", "major"))
        self.assertEqual(lp.resolve_supplier({"major_supplier_id": "", "last_supplier_id": "P2"}, None), ("P2", "inferred"))
        self.assertEqual(lp.resolve_supplier({"major_supplier_id": "P1"}, {"supplier_override": "P9"}), ("P9", "manual"))
        self.assertEqual(lp.resolve_supplier({}, {"supplier_override": ""}), ("", ""))

    def test_four_long_lead_suppliers_default_60(self):
        for name in ("洽智工業股份", "萬晉興業有限", "積禾科技有限", "崧嵐企業有限"):
            r = lp.resolve_lead("PX", (name, ""), None, None)
            self.assertEqual((r["lead_days"], r["anytime"], r["source"]), (60, False, "supplier_default"), name)
            self.assertEqual(r["text"], "廠商預設 60 天")

    def test_other_suppliers_default_anytime(self):
        r = lp.resolve_lead("P0327", ("汎翊國際有限", ""), None, None)
        self.assertEqual((r["lead_days"], r["anytime"], r["source"]), (None, True, "supplier_default_anytime"))

    def test_no_supplier_has_no_lead(self):
        r = lp.resolve_lead("", ("", ""), None, None)
        self.assertEqual((r["lead_days"], r["anytime"], r["source"]), (None, False, "none"))

    def test_supplier_setting_overrides_default(self):
        r = lp.resolve_lead("P0175", ("萬晉興業有限", ""), {"lead_days": 90, "anytime": 0}, None)
        self.assertEqual((r["lead_days"], r["source"], r["text"]), (90, "supplier", "廠商設定"))
        r = lp.resolve_lead("P0175", ("萬晉興業有限", ""), {"lead_days": None, "anytime": 1}, None)
        self.assertTrue(r["anytime"])
        r = lp.resolve_lead("P0327", ("汎翊國際有限", ""), {"lead_days": 30, "anytime": 0}, None)
        self.assertEqual((r["lead_days"], r["anytime"]), (30, False))

    def test_part_override_wins(self):
        r = lp.resolve_lead("P0175", ("萬晉興業有限", ""), {"lead_days": 90, "anytime": 0}, {"lead_days": 20, "anytime": 0})
        self.assertEqual((r["lead_days"], r["source"], r["text"]), (20, "part", "料號設定"))
        # 料號覆蓋交期也能蓋掉「廠商可隨時下單」
        r = lp.resolve_lead("P0327", ("汎翊國際有限", ""), None, {"lead_days": 45, "anytime": 0})
        self.assertEqual((r["lead_days"], r["anytime"]), (45, False))
        r = lp.resolve_lead("P0175", ("萬晉興業有限", ""), None, {"lead_days": None, "anytime": 1})
        self.assertEqual((r["anytime"], r["source"]), (True, "part_anytime"))

    def test_validate_lead(self):
        self.assertIsNone(lp.validate_lead(None))
        self.assertIsNone(lp.validate_lead(" "))
        self.assertEqual((lp.validate_lead(0), lp.validate_lead("730"), lp.validate_lead(60.0)), (0, 730, 60))
        for bad in (731, -1, 1.5, "abc", True, "7天"):
            with self.assertRaises(lp.LeadSettingError, msg=repr(bad)):
                lp.validate_lead(bad)


# ── 分類（快照 + 設定 → 主清單 / 待確認） ───────────────────────────────────

def snap(**kw):
    base = {"st": 0, "pending_raw": 0, "pending": 0, "free_st": 0, "gl015": 0, "unissued": 0,
            "available": 0, "available_without_free_st": 0, "pending_over_st": False,
            "name": "", "perdays": 0, "major_supplier_id": "", "last_supplier_id": "", "last_purchase_date": "",
            "usage_st_1y": 365, "usage_015_1y": 0, "daily": 1.0, "need": 0, "on_order": 0, "eta": "",
            "unissued_detail": []}
    base.update(kw)
    return base


class PlanBase(Base):
    SUPPLIERS = {"P0175": ("萬晉興業有限", "萬晉興業有限公司"), "P0327": ("汎翊國際有限", ""),
                 "P0304": ("積禾科技有限", ""), "P0195": ("崧嵐企業有限", "")}

    def setUp(self):
        super().setUp()
        self.main = set()
        p = patch.object(low_stock, "main_file_parts", lambda: self.main)
        p.start()
        self.addCleanup(p.stop)

    def load(self, rows):
        self.main = set(rows) if not self.main else self.main
        db.replace_low_stock_plan_snapshot(rows, self.SUPPLIERS, "2026-11-11 23:50:00")

    def plan(self):
        return lp.build_plan(TODAY)


class ClassifyTests(PlanBase):
    def test_conditions_a_b_c_and_check_area(self):
        self.load({
            "A-ZERO": snap(available=0, major_supplier_id="P0327"),                       # a（可隨時下單也照列）
            "B-NEED": snap(available=100, need=300, major_supplier_id="P0327"),          # b
            "C-LEAD": snap(available=30, daily=1.0, major_supplier_id="P0175"),          # c：可撐 30 < 60
            "OK-LEAD": snap(available=100, daily=1.0, major_supplier_id="P0304"),        # 有交期撐得到 → 不列
            "ANY": snap(available=5, daily=1.0, major_supplier_id="P0327"),              # 可隨時下單 → 不列
            "NOSUP": snap(available=1000, daily=10.0),                                   # 沒廠商 → 待確認
        })
        res = self.plan()
        main = {r["part"]: r for r in res["main"]}
        self.assertEqual(set(main), {"A-ZERO", "B-NEED", "C-LEAD"})
        self.assertEqual(main["A-ZERO"]["reasons"], ["可用 0 ≤ 0"])
        self.assertEqual(main["B-NEED"]["reasons"], ["可用 100 < 已排程需求 300（缺 200）"])
        self.assertEqual(main["C-LEAD"]["reasons"], ["可撐 30.0 天 < 交期 60 天（來源：廠商預設 60 天）"])
        self.assertIn(lp.PRIORITY_TAG, main["C-LEAD"]["tags"])  # 萬晉
        self.assertEqual([r["part"] for r in res["check"]], ["NOSUP"])
        self.assertEqual(res["check"][0]["reasons"], ["無廠商交期，無法判斷能否撐到交期；可撐 100.0 天"])
        c = res["counts"]
        self.assertEqual((c["population"], c["main"], c["check"], c["ok_with_lead"]), (6, 3, 1, 1))
        self.assertEqual(c["anytime"], 3)  # A-ZERO、B-NEED、ANY（汎翊預設可隨時下單）
        # 廠商交期區：料數照歸屬算，四家長交期在前
        sup = {s["supplier_id"]: s for s in res["suppliers"]}
        self.assertEqual((sup["P0327"]["part_count"], sup["P0327"]["anytime"]), (3, True))
        self.assertEqual((sup["P0175"]["lead_days"], sup["P0175"]["is_default"]), (60, True))
        self.assertEqual(res["no_supplier_count"], 1)

    def test_cover_uses_available_minus_scheduled_need(self):
        self.load({"C-1": snap(available=100, need=50, daily=1.0, major_supplier_id="P0175")})
        r = self.plan()["main"][0]
        self.assertEqual(r["cover_days"], 50.0)
        self.assertIn("可撐 50.0 天 < 交期 60 天", r["reasons"][0])

    def test_tab_not_main_and_unused_excluded(self):
        self.main = {"OC-10935B-TAB-C", "AB-TAB", "UNUSED-1", "IN-1"}
        self.load({
            "OC-10935B-TAB-C": snap(available=0), "AB-TAB": snap(available=0),
            "NOT-MAIN": snap(available=0),
            "UNUSED-1": snap(available=0, usage_st_1y=0, usage_015_1y=0, daily=0),
            "IN-1": snap(available=0),
        })
        res = self.plan()
        self.assertEqual([r["part"] for r in res["main"]], ["IN-1"])
        c = res["counts"]
        self.assertEqual((c["excluded_tab"], c["excluded_not_main"], c["excluded_unused"]), (2, 1, 1))

    def test_015_usage_counts_as_used(self):
        self.load({"GL-ONLY": snap(available=0, usage_st_1y=0, usage_015_1y=10, daily=10 / 365)})
        self.assertEqual([r["part"] for r in self.plan()["main"]], ["GL-ONLY"])

    def test_inferred_supplier_each_long_lead_supplier(self):
        """2d：推定廠商落在洽智、萬晉、積禾、崧嵐都套 60 天並標推定；其他推定廠商可隨時下單；料號覆寫優先。"""
        self.assertEqual(lp.LONG_LEAD_SUPPLIERS, ("洽智", "萬晉", "積禾", "崧嵐"))
        sups = {"P0001": ("洽智工業", ""), "P0175": ("萬晉興業有限", ""), "P0304": ("積禾科技有限", ""),
                "P0195": ("崧嵐企業有限", ""), "P0327": ("汎翊國際有限", "")}
        db.replace_low_stock_plan_snapshot({f"I-{sid}": snap(available=10, daily=1.0, last_supplier_id=sid) for sid in sups},
                                           sups, "2026-11-11 23:50:00")
        self.main = {f"I-{sid}" for sid in sups}
        rows = {r["part"]: r for r in lp.build_rows(TODAY)["rows"]}
        for sid in ("P0001", "P0175", "P0304", "P0195"):
            r = rows[f"I-{sid}"]
            self.assertEqual((r["supplier_kind"], r["lead_days"], r["anytime"], r["area"]), ("inferred", 60, False, "main"), sid)
        other = rows["I-P0327"]
        self.assertEqual((other["supplier_kind"], other["lead_days"], other["anytime"], other["area"]),
                         ("inferred", None, True, "anytime"))
        lp.save_part_setting("I-P0304", lead_days=5, actor="t")
        r = {r["part"]: r for r in lp.build_rows(TODAY)["rows"]}["I-P0304"]
        self.assertEqual((r["lead_days"], r["lead_source"], r["area"]), (5, "part", "ok"))

    def test_inferred_supplier_gets_60_and_label(self):
        self.load({"INF-1": snap(available=10, daily=1.0, last_supplier_id="P0195")})
        r = self.plan()["main"][0]
        self.assertEqual((r["supplier_id"], r["supplier_kind"], r["lead_days"]), ("P0195", "inferred", 60))
        self.assertIn(lp.PRIORITY_TAG, r["tags"])  # 崧嵐

    def test_manual_supplier_and_part_settings(self):
        self.load({"M-1": snap(available=10, daily=1.0, major_supplier_id="P0327")})
        self.assertEqual(self.plan()["main"], [])  # 汎翊：可隨時下單
        lp.save_part_setting("m-1", supplier_override="p0175", actor="t")
        r = self.plan()["main"][0]
        self.assertEqual((r["supplier_id"], r["supplier_kind"], r["lead_days"]), ("P0175", "manual", 60))
        lp.save_part_setting("M-1", lead_days="5", actor="t")  # 料號覆蓋優先於廠商
        res = self.plan()
        self.assertEqual(res["main"], [])
        self.assertEqual(res["counts"]["ok_with_lead"], 1)
        lp.save_part_setting("M-1", lead_days="", anytime=True, actor="t")
        res = self.plan()
        self.assertEqual((res["counts"]["anytime"], res["counts"]["check"]), (1, 0))
        lp.save_part_setting("M-1", anytime=False, supplier_override="", actor="t")
        self.assertEqual(db.get_low_stock_part_settings(), {})  # 全空 → 刪列，回到自動
        with self.assertRaises(lp.LeadSettingError):
            lp.save_part_setting("M-1", supplier_override="P9999", actor="t")
        with self.assertRaises(lp.LeadSettingError):
            lp.save_part_setting("OC-10935B-TAB-C", lead_days=10, actor="t")

    def test_anytime_part_skips_check_but_still_main_when_short(self):
        self.load({"N-1": snap(available=1000, daily=1.0), "N-2": snap(available=-5, daily=1.0)})
        lp.save_part_setting("N-1", anytime=True, actor="t")
        lp.save_part_setting("N-2", anytime=True, actor="t")
        res = self.plan()
        self.assertEqual(res["check"], [])
        self.assertEqual([r["part"] for r in res["main"]], ["N-2"])

    def test_supplier_setting_save_and_clear(self):
        self.load({"S-1": snap(available=40, daily=1.0, major_supplier_id="P0327")})
        lp.save_supplier_lead("P0327", 50, False, actor="t")
        r = self.plan()["main"][0]
        self.assertEqual(r["reasons"], ["可撐 40.0 天 < 交期 50 天（來源：廠商設定）"])
        self.assertEqual(db.get_low_stock_supplier_leads()["P0327"]["supplier_name"], "汎翊國際有限")
        lp.save_supplier_lead("P0327", None, False, actor="t")  # 清除 → 回到預設可隨時下單
        self.assertEqual(self.plan()["main"], [])
        lp.save_supplier_lead("P0175", None, True, actor="t")  # 長交期廠商也可改成可隨時下單
        self.assertTrue(db.get_low_stock_supplier_leads()["P0175"]["anytime"])
        with self.assertRaises(lp.LeadSettingError):
            lp.save_supplier_lead("P0327", 999, False, actor="t")

    def test_ordered_sorted_last_with_tags(self):
        self.load({
            "O-1": snap(available=-100, on_order=5000, eta="2026-11-26"),
            "O-2": snap(available=0),
            "O-3": snap(available=-10),
            "O-4": snap(available=-1, on_order=10, eta="2026-09-28", gl015=3, pending_over_st=True),
        })
        res = self.plan()
        self.assertEqual([r["part"] for r in res["main"]], ["O-3", "O-2", "O-1", "O-4"])
        o1 = next(r for r in res["main"] if r["part"] == "O-1")
        self.assertIn("已下單 5000（預交 2026/11/26）", o1["tags"])
        self.assertNotIn(lp.OVERDUE_TAG, o1["tags"])
        o4 = next(r for r in res["main"] if r["part"] == "O-4")
        for tag in (lp.OVERDUE_TAG, lp.GL_TAG, lp.PENDING_OVER_TAG):
            self.assertIn(tag, o4["tags"])
        self.assertEqual(res["counts"]["main_ordered"], 2)

    def test_check_sorted_by_cover(self):
        self.load({"K-1": snap(available=300, daily=1.0), "K-2": snap(available=100, daily=1.0),
                   "K-3": snap(available=50, daily=1.0, on_order=1)})
        self.assertEqual([r["part"] for r in self.plan()["check"]], ["K-2", "K-1", "K-3"])

    def test_no_frequency_or_threshold_filter(self):
        """一年只領 1 次、量很少的料照樣判斷（不准用頻率／張數篩）。"""
        db.set_setting("low_stock_min_bills", "50")
        self.load({"RARE-1": snap(available=1, usage_st_1y=15, daily=15 / 365, major_supplier_id="P0175")})
        res = self.plan()
        self.assertEqual([r["part"] for r in res["main"]], ["RARE-1"])  # 可撐 24.3 天 < 60 天

    def test_perdays_not_used_as_lead(self):
        self.load({"PD-1": snap(available=10, daily=1.0, perdays=60)})
        r = self.plan()["check"][0]
        self.assertIsNone(r["lead_days"])
        self.assertEqual(r["perdays"], 60)


# ── refresh：正航（假）+ 本機 → 快照；OC-10888B 端對端 538 ─────────────────

class FakePlanCursor:
    def __init__(self, data):
        self.d = data
        self.rows = []

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        self.d["sql"].append(s)
        assert s.startswith("SELECT") and not any(w in s.upper() for w in ("INSERT", "UPDATE ", "DELETE"))
        if "FROM comProduct" in s:
            self.rows = [r for r in self.d["products"] if r[0] in params]
        elif "FROM comCustomer" in s:
            self.rows = list(self.d["suppliers"])
        elif "comBillAccounts" in s:
            self.rows = [r for r in self.d["last"] if r[0] in params]
        elif "prdMKOrdMain" in s:
            ware, *parts = params
            self.rows = [r for r in self.d["issued"] if r[1] in parts]
        elif "Flag = 331" in s:  # 015 領用
            self.rows = [(p, -q, 3) for p, q in self.d["gl_usage"].items() if p in params]
        elif "Flag = 400" in s:  # ST 領用
            self.rows = [(p, q, 3) for p, q in self.d["st_usage"].items() if p in params]
        elif "comWareAmount" in s:
            self.rows = [(p, q) for p, q in self.d["gl_qty"].items() if p in params]
        else:
            raise AssertionError(s)

    def fetchall(self):
        return list(self.rows)


class RefreshTests(PlanBase):
    def setUp(self):
        super().setUp()
        self.erp = {
            "sql": [],
            "products": [("OC-10888B", "螺絲", 0, "P0175"), ("EC-NEW", "電阻", 14, "")],
            "suppliers": [("P0175", "萬晉興業有限", "萬晉興業有限公司"), ("P0327", "汎翊國際有限", "")],
            "last": [("EC-NEW", 20261001, "P0327")],
            "issued": [("4500059105-A", "OC-10888B", -250), ("4500059418-A", "OC-10888B", -250)],
            "st_usage": {"OC-10888B": 0}, "gl_usage": {"OC-10888B": 2450, "EC-NEW": 100},
            "gl_qty": {"OC-10888B": 238},
        }

        @contextmanager
        def erp_session():
            yield type("C", (), {"cursor": lambda s: FakePlanCursor(self.erp)})()

        self.main = {"OC-10888B", "EC-NEW", "OC-10935B-TAB-C"}
        patches = [
            patch.object(low_stock, "_erp_session", erp_session),
            patch.object(low_stock, "_fetch_on_order", lambda erp, parts, today, cfg: {"EC-NEW": (1000.0, "2026-12-01")}),
            patch.object(low_stock, "_gl_ware_id", lambda: "015"),
            patch.object(lp.erp_stock, "get_settings", lambda: {"ware_id": "001"}),
            patch.object(db, "get_st_inventory_stock", lambda: {"OC-10888B": 800.0}),
            patch.object(spt, "compute", lambda *a, **k: {
                "parts": {"OC-10888B": {"pending": 800.0}}, "warnings": [],
                "info": {"cutoff_code": "11-2", "cutoff_source": "auto_erp_transfer", "cutoff_move_no": "RM-20261109001",
                         "cutoff_source_text": "自動：最後一張成功調撥單批次 11-2（RM-20261109001）"}}),
            patch.object(lp, "scheduled_need", lambda conv, warnings: {"EC-NEW": 50.0}),
            patch.object(part_conversion, "active_map", lambda: {}),
            patch.object(low_stock, "local_now", lambda: __import__("datetime").datetime(2026, 11, 11, 23, 50)),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        for oid, code, po in ((101, "12-10", "4500059105"), (105, "8-9", "4500059418"),
                              (270, "11-6", "4500059674"), (272, "12-2", "4500059664")):
            self.conn.execute("INSERT INTO orders(id, code, model, po_number, status, order_qty) VALUES(?,?,?,?,?,?)",
                              (oid, code, "TDA3-3", po, "dispatched", 250))
            self.conn.execute("INSERT INTO dispatch_records(order_id, part_number, needed_qty) VALUES(?,?,?)",
                              (oid, "OC-10888B", 250))
        self.conn.execute("INSERT INTO orders(id, code, model, po_number, status, order_qty) VALUES(300,'12-9','T35U','P9','pending',350)")

    def test_refresh_then_plan_oc10888b_538(self):
        res = lp.refresh_snapshot("t")
        self.assertTrue(res["ok"], res)
        self.assertEqual(res["parts"], 2)  # -TAB 不進快照
        snapshot = db.get_low_stock_plan_snapshot()
        self.assertNotIn("OC-10935B-TAB-C", snapshot)
        oc = snapshot["OC-10888B"]
        self.assertEqual((oc["st"], oc["pending"], oc["free_st"], oc["gl015"], oc["unissued"]), (800, 800, 0, 238, 500))
        self.assertEqual((oc["available"], oc["available_without_free_st"]), (538, 538))
        self.assertEqual({d["po"]: d["unissued"] for d in oc["unissued_detail"]},
                         {"4500059105": 0, "4500059418": 0, "4500059664": 250, "4500059674": 250})
        self.assertAlmostEqual(oc["daily"], round(2450 / 365, 4))
        plan = self.plan()
        self.assertEqual(plan["cutoff"]["code"], "11-2")
        self.assertIn("RM-20261109001", plan["cutoff"]["text"])
        self.assertEqual(plan["refreshed_at"], "2026-11-11 23:50:00")
        rows = {r["part"]: r for r in plan["main"] + plan["check"]}
        # 538 / 6.71 ≈ 80 天 > 萬晉 60 天 → 撐得到，不列
        self.assertNotIn("OC-10888B", rows)
        self.assertEqual(plan["counts"]["ok_with_lead"], 1)
        # EC-NEW：沒主要廠商 → 推定汎翊 → 可隨時下單；可用 0 ≤ 0 → 主清單，已下單
        ec = rows["EC-NEW"]
        self.assertEqual((ec["supplier_kind"], ec["supplier_id"], ec["anytime"]), ("inferred", "P0327", True))
        self.assertEqual(ec["need"], 50)
        self.assertIn("已下單 1000（預交 2026/12/01）", ec["tags"])
        self.assertEqual(ec["perdays"], 14)
        # 萬晉交期改 120 天 → 可撐 80.2 天 < 120 → 進主清單
        lp.save_supplier_lead("P0175", 120, False, actor="t")
        oc_row = next(r for r in self.plan()["main"] if r["part"] == "OC-10888B")
        self.assertEqual(oc_row["reasons"], ["可撐 80.2 天 < 交期 120 天（來源：廠商設定）"])

    def test_reconcile_report_tool(self):
        import importlib
        import json
        import tempfile
        from pathlib import Path

        tool = importlib.import_module("tools.available_reconcile_report")
        with tempfile.TemporaryDirectory() as tmp:
            main_path = Path(tmp) / "main.xlsx"
            main_path.write_bytes(b"x")
            db.set_setting("main_file_path", str(main_path))
            stock = {"OC-10888B": 538.0, "EC-NEW": 10.0, "OC-10935B-TAB-C": 5.0}
            with patch.object(tool.main_reader, "read_stock", lambda path: stock):
                report = tool.build(TODAY)
                json_path, csv_path = tool.write(report, Path(tmp) / "out")
            self.assertEqual([r["part"] for r in report["rows"]], ["EC-NEW", "OC-10888B"])  # 依 |差異| 大到小，-TAB 排除
            oc = report["rows"][1]
            self.assertEqual((oc["available_without_free_st"], oc["main_balance"], oc["diff"], oc["match"]),
                             (538, 538, 0, True))
            ec = report["rows"][0]
            self.assertEqual((ec["diff"], ec["match"]), (-10, False))
            self.assertTrue(ec["causes"])
            self.assertEqual(report["summary"]["mismatch"], 1)
            self.assertEqual(json.loads(json_path.read_text(encoding="utf-8"))["summary"]["total"], 2)
            self.assertIn("OC-10888B", csv_path.read_text(encoding="utf-8-sig"))
        self.assertEqual(db.get_low_stock_plan_snapshot(), {})  # 唯讀：不寫快照

    def test_refresh_priority_parts_also_refreshes_snapshot(self):
        with patch.object(low_stock, "_refresh_priority_rows", lambda actor: {"ok": True}):
            res = low_stock.refresh_priority_parts("t")
        self.assertTrue(res["plan"]["ok"])
        self.assertIn("OC-10888B", db.get_low_stock_plan_snapshot())

    def test_erp_failure_keeps_old_snapshot(self):
        lp.refresh_snapshot("t")

        @contextmanager
        def broken():
            raise low_stock.ErpUnavailable("down")
            yield  # pragma: no cover

        with patch.object(low_stock, "_erp_session", broken):
            res = lp.refresh_snapshot("t")
        self.assertFalse(res["ok"])
        self.assertIn("保留上次資料", res["error"])
        self.assertIn("OC-10888B", db.get_low_stock_plan_snapshot())
        self.assertIn("保留上次資料", self.plan()["error"])

    def test_no_main_file_fails_softly(self):
        self.main = None
        with patch.object(low_stock, "main_file_parts", lambda: None):
            res = lp.refresh_snapshot("t")
        self.assertFalse(res["ok"])
        self.assertIn("讀不到主檔", res["error"])


class RouterTests(PlanBase):
    def setUp(self):
        super().setUp()
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)

    def test_plan_and_settings_api(self):
        self.load({"A-1": snap(available=0, major_supplier_id="P0175")})
        data = self.client.get("/api/low-stock/plan").json()
        self.assertEqual([r["part"] for r in data["main"]], ["A-1"])
        self.assertEqual(self.client.get("/api/low-stock/plan/count").json(), {"count": 1})
        r = self.client.post("/api/low-stock/lead-settings/supplier", json={"supplier_id": "P0175", "lead_days": 45})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["lead_days"], 45)
        for bad in (731, -1, 1.5, "x"):
            r = self.client.post("/api/low-stock/lead-settings/supplier", json={"supplier_id": "P0175", "lead_days": bad})
            self.assertEqual(r.status_code, 400, bad)
        r = self.client.post("/api/low-stock/lead-settings/part", json={"part": "a-1", "lead_days": "10"})
        self.assertEqual((r.status_code, r.json()["lead_days"]), (200, 10))
        r = self.client.post("/api/low-stock/lead-settings/part", json={"part": "A-1", "anytime": True})
        self.assertEqual((r.json()["lead_days"], r.json()["anytime"]), (10, True))  # 沒帶的欄位不變
        r = self.client.post("/api/low-stock/lead-settings/part", json={"part": "A-1", "lead_days": None})
        self.assertIsNone(r.json()["lead_days"])
        s = self.client.get("/api/low-stock/lead-settings").json()
        self.assertEqual(s["long_lead_suppliers"], ["洽智", "萬晉", "積禾", "崧嵐"])
        self.assertEqual(s["long_lead_default_days"], 60)
        self.assertEqual([x["supplier_id"] for x in s["suppliers"]], ["P0175"])


# ── v2026.11.12.1：手動新增料號（R-B9） ────────────────────────────────────

class ManualPartTests(PlanBase):
    def setUp(self):
        super().setUp()
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)
        self.main = {"IN-MAIN"}
        self.fetched = []

        def fake_collect(parts, today=None):
            self.fetched.append(list(parts))
            rows = {p: snap(available=0, usage_st_1y=365 if p in self.main else 0, daily=0, major_supplier_id="P0327")
                    for p in parts}
            return {"rows": rows, "suppliers": self.SUPPLIERS, "warnings": [], "pending_info": {},
                    "no_bom_orders": [], "no_record_orders": [], "dispatched_order_count": 0}

        p = patch.object(lp, "collect", fake_collect)
        p.start()
        self.addCleanup(p.stop)

    def rows(self):
        plan = self.plan()
        return {r["part"]: r for r in plan["main"] + plan["check"]}

    def test_add_lists_part_outside_population_and_remove(self):
        self.load({"IN-MAIN": snap(available=500, major_supplier_id="P0327")})
        r = self.client.post("/api/low-stock/items", json={"part_number": " ec-manual ", "note": "Andy 自己加"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual((r.json()["part"], r.json()["fetched"]), ("EC-MANUAL", True))
        self.assertEqual(self.fetched, [["EC-MANUAL"]])  # 只抓這顆，不重算整批
        row = self.rows()["EC-MANUAL"]  # 不在主檔、一年沒領用也列（照可用量判斷：可用 0 ≤ 0）
        self.assertEqual((row["area"], row["manual"], row["manual_note"]), ("main", True, "Andy 自己加"))
        self.assertEqual(row["tags"][0], lp.MANUAL_TAG)
        self.assertIn("IN-MAIN", db.get_low_stock_plan_snapshot())  # 其他料號快照不動
        self.assertEqual(self.client.post("/api/low-stock/items", json={"part_number": "EC-MANUAL"}).status_code, 409)
        r = self.client.post("/api/low-stock/items/remove", json={"part_number": "ec-manual"})
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("EC-MANUAL", self.rows())  # 不在主檔 → 移除後不再列
        self.assertEqual(self.client.post("/api/low-stock/items/remove", json={"part_number": "EC-MANUAL"}).status_code, 404)

    def test_tab_rejected_and_no_threshold_field(self):
        for part in ("OC-10935B-TAB-C", "ab-tab"):
            r = self.client.post("/api/low-stock/items", json={"part_number": part})
            self.assertEqual(r.status_code, 400, part)
            self.assertIn("-TAB", r.json()["detail"])
        self.assertEqual(db.get_low_stock_manual_parts(), {})
        # 舊門檻欄位不再被接受成設定（被忽略）
        r = self.client.post("/api/low-stock/items", json={"part_number": "EC-1", "threshold_qty": 99})
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("threshold", str(db.get_low_stock_manual_parts()["EC-1"]))
        self.assertEqual(self.client.post("/api/low-stock/items", json={"part_number": " "}).status_code, 400)

    def test_lead_on_add_update_and_clear(self):
        r = self.client.post("/api/low-stock/items", json={"part_number": "EC-L", "lead_days": "45"})
        self.assertEqual((r.status_code, r.json()["lead_days"]), (200, 45))
        row = self.rows()["EC-L"]
        self.assertEqual((row["lead_days"], row["lead_source"], row["part_lead_days"]), (45, "part", 45))
        r = self.client.post("/api/low-stock/items/update", json={"part_number": "EC-L", "lead_days": 90, "note": "改"})
        self.assertEqual(r.status_code, 200, r.text)
        row = self.rows()["EC-L"]
        self.assertEqual((row["lead_days"], row["manual_note"]), (90, "改"))
        # 清空 → 恢復自動（汎翊：廠商預設可隨時下單）
        self.client.post("/api/low-stock/items/update", json={"part_number": "EC-L", "lead_days": None})
        row = self.rows()["EC-L"]
        self.assertEqual((row["lead_days"], row["anytime"], row["lead_source"]), (None, True, "supplier_default_anytime"))
        # 料號列交期也走 v7 既有料號設定 API
        r = self.client.post("/api/low-stock/lead-settings/part", json={"part": "EC-L", "lead_days": "30"})
        self.assertEqual(self.rows()["EC-L"]["lead_days"], 30)
        for bad in (731, -1, "abc"):
            r = self.client.post("/api/low-stock/items", json={"part_number": "EC-BAD", "lead_days": bad})
            self.assertEqual(r.status_code, 400, bad)
        self.assertEqual(self.client.post("/api/low-stock/items/update",
                                          json={"part_number": "NOPE", "note": "x"}).status_code, 404)

    def test_add_survives_erp_failure_shows_no_data(self):
        with patch.object(lp, "collect", side_effect=low_stock.ErpUnavailable("down")):
            r = self.client.post("/api/low-stock/items", json={"part_number": "EC-OFF"})
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.json()["fetched"])
        self.assertIn("重新抓正航", r.json()["warning"])
        row = self.rows()["EC-OFF"]
        self.assertEqual((row["area"], row["reasons"], row["available"], row["no_data"]),
                         ("check", [lp.NO_DATA_REASON], None, True))  # 沒數字不當 0 判斷
        self.assertEqual(row["shortage"], 0)

    def test_manual_part_in_population_stays_after_remove(self):
        self.load({"IN-MAIN": snap(available=0, major_supplier_id="P0327")})
        lp.add_manual_part("IN-MAIN", actor="t")
        lp.remove_manual_part("IN-MAIN", actor="t")
        self.assertIn("IN-MAIN", self.rows())  # 原本就在母體 → 照規則列

    def test_refresh_snapshot_includes_manual_parts(self):
        db.save_low_stock_manual_part("EC-OUT", "", "t")
        db.save_low_stock_manual_part("XX-TAB", "", "t")  # 萬一舊資料有 -TAB 也不抓
        with patch.object(low_stock, "main_file_parts", lambda: {"IN-MAIN"}):
            res = lp.refresh_snapshot("t")
        self.assertTrue(res["ok"], res)
        self.assertEqual(self.fetched[-1], ["EC-OUT", "IN-MAIN"])


# ── v2026.11.12.1：開採購單建議量（R-E2/E3/E4/E10） ───────────────────────

def moq_info(moq, qtys=()):
    return {"moq": moq, "moq_source": "last_receipt" if qtys else ("site_master" if moq else "none"),
            "last_receipt_qty": qtys[0] if qtys else None, "last_receipt_date": "2026-10-01" if qtys else "",
            "last_receipt_bill": "", "receipt_gcd": moq if qtys else None, "receipt_count": len(qtys),
            "receipt_qtys": list(qtys), "moq_floor_applied": False}


class PoSuggestTests(PlanBase):
    def setUp(self):
        super().setUp()
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)
        self.moq = {"P-1": moq_info(500, (1000, 1500, 500)), "P-2": moq_info(0), "P-5": moq_info(200, (200,))}
        patches = [
            patch.object(lp.moq_service, "lookup_moq", lambda parts: {p: self.moq.get(p, moq_info(0)) for p in parts}),
            patch.object(lp, "_po_target", lambda: "CHIComp02"),
            patch.object(lp, "local_now", lambda: __import__("datetime").datetime(2026, 11, 11, 10, 0)),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.load({
            "P-1": snap(available=100, need=1150, major_supplier_id="P0175"),   # 缺 1050
            "P-2": snap(available=-10, need=20, major_supplier_id="P0175"),     # 缺 30（同廠商）
            "P-3": snap(available=0, need=0, major_supplier_id="P0175"),        # 主清單（可用 0）但不缺
            "P-4": snap(available=-5, need=0, major_supplier_id="P0175"),       # 已排需求 0 → 不無故多買
            "P-5": snap(available=0, need=10, major_supplier_id="P0327"),       # 別家廠商
        })

    def add_po(self, bill, part, qty, created, target="CHIComp02", supplier="P0175", status="ok"):
        import json
        self.conn.execute(
            "INSERT INTO erp_purchase_orders(bill_no, target_db, supplier_id, lines_json, created_at, status) VALUES(?,?,?,?,?,?)",
            (bill, target, supplier, json.dumps([{"part_number": part, "qty": qty}]), created, status))

    def test_qty_rounds_up_to_moq_no_extra(self):
        res = lp.po_suggest("p-1", TODAY)
        it = res["item"]
        self.assertEqual((it["part"], it["shortage"], it["moq"], it["moq_source"]), ("P-1", 1050, 500, "last_receipt"))
        self.assertEqual(it["suggested_qty"], 1500)  # ceil(1050 / 500) × 500，不是 max(1050, 500)
        self.assertLess(it["suggested_qty"] - it["shortage"], it["moq"])  # 多出來的只有 MOQ 進位，不到一個 MOQ
        self.assertEqual((it["supplier_id"], it["supplier_name"]), ("P0175", "萬晉興業有限"))
        self.assertEqual(it["receipt_qtys"], [1000, 1500, 500])
        # 純函式：剛好整倍數不進位、沒 MOQ 就是缺料量、不缺就是 0
        self.assertEqual((lp.suggested_po_qty(1000, 500), lp.suggested_po_qty(30, 0), lp.suggested_po_qty(0, 500)),
                         (1000, 30, 0))
        self.assertEqual(lp.suggested_po_qty(0.2, 0), 1)

    def test_shortage_definition(self):
        rows = {r["part"]: r for r in lp.build_rows(TODAY)["rows"]}
        self.assertEqual({p: r["shortage"] for p, r in rows.items()},
                         {"P-1": 1050, "P-2": 30, "P-3": 0, "P-4": 0, "P-5": 10})
        for part in ("P-3", "P-4"):  # 可用 ≤ 0 但已排需求 0：建議 0，不湊 MOQ
            self.assertEqual(lp.po_suggest(part, TODAY)["item"]["suggested_qty"], 0, part)

    def test_same_supplier_merged_in_main_order(self):
        res = lp.po_suggest("P-2", TODAY)
        self.assertEqual([x["part"] for x in res["same_supplier"]], ["P-1"])  # 同廠商、缺料 > 0；P-3/P-4 不缺、P-5 別家
        self.assertEqual(res["same_supplier"][0]["suggested_qty"], 1500)
        self.assertEqual(res["item"]["suggested_qty"], 30)
        self.assertEqual(lp.po_suggest("P-5", TODAY)["same_supplier"], [])

    def test_recent_po_reminder(self):
        self.add_po("PO-20261110001", "P-1", 1500, "2026-11-10T09:00:00")
        self.add_po("PO-20260901001", "P-1", 500, "2026-09-01T09:00:00")      # 超過 30 天不提醒
        self.add_po("PO-20261110002", "P-1", 9, "2026-11-10T09:00:00", target="CHIComp03")  # 別家公司
        self.add_po("PO-20261110003", "P-1", 9, "2026-11-10T09:00:00", status="error")
        res = lp.po_suggest("P-2", TODAY)
        p1 = res["same_supplier"][0]
        self.assertEqual(p1["recent_pos"], [{"bill_no": "PO-20261110001", "date": "2026-11-10", "qty": 1500.0,
                                             "supplier_id": "P0175"}])
        self.assertEqual(res["item"]["recent_pos"], [])
        self.assertEqual(res["recent_days"], 30)

    def test_api_and_errors(self):
        r = self.client.get("/api/low-stock/po-suggest", params={"part": "P-1"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["item"]["suggested_qty"], 1500)
        self.assertEqual(self.client.get("/api/low-stock/po-suggest", params={"part": "OC-10935B-TAB-C"}).status_code, 400)
        self.assertEqual(self.client.get("/api/low-stock/po-suggest", params={"part": "ZZ-9"}).status_code, 404)

    def test_po_page_need_uses_new_shortage(self):
        """開採購單頁手動輸入料號時的「需」也用新缺料量，不再用舊門檻。"""
        from app.services import erp_purchase as ep
        self.assertEqual(ep._suggested_qty("P-1"), 1050)
        self.assertIsNone(ep._suggested_qty("P-4"))

    def test_no_erp_write_and_edit_auth(self):
        from starlette.requests import Request
        from app.services.edit_auth import request_requires_edit_auth

        def req(method, path):
            return Request({"type": "http", "method": method, "path": path, "headers": [], "query_string": b""})

        self.assertFalse(request_requires_edit_auth(req("GET", "/api/low-stock/po-suggest")))
        for path in ("/api/low-stock/items", "/api/low-stock/items/update", "/api/low-stock/items/remove"):
            self.assertTrue(request_requires_edit_auth(req("POST", path)), path)
        from app.services import erp_purchase as ep
        with patch.object(ep, "create_po", side_effect=AssertionError("不可寫正航")):
            lp.po_suggest("P-1", TODAY)


# ── 待調撥截止批次自動判斷（總指揮 D5） ─────────────────────────────────────

class AutoCutoffTests(Base):
    BATCHES = [{"key": k, "code": k.split("/")[1], "year": int(k[:4]),
                "sort": [int(k[:4]), *map(int, k.split("/")[1].split("-"))]}
               for k in ("2025/12-1", "2025/12-9", "2026/1-2", "2026/10-3", "2026/10-9", "2026/11-2")]

    def add(self, key, target="CHIComp03", status="ok", move_no=""):
        self.conn.execute("INSERT INTO erp_transfers(batch_key, batch_code, target_db, move_no, status) VALUES(?,?,?,?,?)",
                          (key, key.split("/")[-1], target, move_no or f"RM-{key}", status))

    def test_last_ok_prod_batch_ignores_error_and_test_company(self):
        self.add("2026/10-3", move_no="RM-20261006001")
        self.add("2026/10-9", status="error")
        self.add("2026/11-2", target="CHIComp13")
        cut = spt.effective_cutoff(self.BATCHES)
        self.assertEqual((cut["batch"]["key"], cut["source"], cut["move_no"], cut["warnings"]),
                         ("2026/10-3", spt.SOURCE_AUTO, "RM-20261006001", []))
        self.assertEqual(spt.source_text(cut), "自動：最後一張成功調撥單批次 10-3（RM-20261006001）")

    def test_cross_year_order(self):
        self.add("2026/1-2")
        self.add("2025/12-9")  # 後寫入但比較早
        self.assertEqual(spt.last_transfer_record()["batch_key"], "2026/1-2")
        self.assertEqual(spt.effective_cutoff(self.BATCHES)["batch"]["key"], "2026/1-2")
        self.assertGreater(spt.parse_key("2026/1-2"), spt.parse_key("2025/12-9"))
        self.assertGreater(spt.parse_key("2026/10-10"), spt.parse_key("2026/10-9"))
        self.assertIsNone(spt.parse_key("10-3"))

    def test_old_setting_does_not_override_auto(self):
        self.add("2026/11-2")
        db.set_setting(spt.SETTING_KEY, "2026/10-3")  # 舊設定（例如 10-4）留著但不覆蓋
        self.assertEqual(spt.effective_cutoff(self.BATCHES)["batch"]["key"], "2026/11-2")

    def test_fallback_when_not_found(self):
        self.add("2026/12-5")  # 對應不到已發料批次
        db.set_setting(spt.SETTING_KEY, "2026/10-9")
        cut = spt.effective_cutoff(self.BATCHES)
        self.assertEqual((cut["batch"]["key"], cut["source"], cut["move_no"]), ("2026/10-9", spt.SOURCE_FALLBACK, ""))
        self.assertIn("對應不到已發料批次", cut["warnings"][0])
        self.assertIn("改用設定值 2026/10-9", cut["warnings"][0])
        self.conn.execute("DELETE FROM erp_transfers")
        self.conn.execute("DELETE FROM settings")
        cut = spt.effective_cutoff(self.BATCHES)
        self.assertIsNone(cut["batch"])
        self.assertIn("找不到成功的正式調撥單", cut["warnings"][0])
        self.assertIn("也沒有設定值", cut["warnings"][0])  # 不再預設 10-3


if __name__ == "__main__":
    unittest.main()
