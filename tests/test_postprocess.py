from __future__ import annotations

import unittest
from datetime import date

import app.database as db
from app.routers import shortage_purchase as sp_router  # noqa: F401
from app.services import postprocess as pp
from app.services import shortage_purchase as sp
import test_shortage_purchase as tsp

TODAY = date(2026, 10, 29)


class SeedAndCrudTests(unittest.TestCase):
    def setUp(self):
        import sqlite3
        from contextlib import contextmanager
        from unittest.mock import patch

        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db._CREATE_SQL)

        @contextmanager
        def temp_conn():
            yield self.conn
            self.conn.commit()

        p = patch.object(db, "get_conn", temp_conn)
        p.start()
        self.addCleanup(p.stop)

    def test_seed_is_idempotent_and_confirmed(self):
        with db.get_conn() as c:
            self.assertEqual(db.seed_postprocess_links(c), 8)
            self.assertEqual(db.seed_postprocess_links(c), 0)
        links = {(l["base_part"], l["process_part"]): l for l in db.list_postprocess_links()}
        self.assertEqual(len(links), 8)
        self.assertEqual(links[("OC-10978B", "SP-10022B")]["status"], "confirmed")
        self.assertTrue(links[("OC-10988B", "SP-10024B")]["active"])
        # 使用者停用後，重新 seed 不會復活
        db.update_postprocess_link(links[("OC-10988B", "SP-10024B")]["id"], enabled=False)
        with db.get_conn() as c:
            db.seed_postprocess_links(c)
        self.assertEqual(db.active_postprocess_links(["OC-10988B"]), {})

    def test_crud_confirm_reject_and_pending_not_active(self):
        i = db.add_postprocess_link("oc-1", "sp-1", 2)
        self.assertEqual(db.active_postprocess_links(["OC-1"]), {"OC-1": [{"id": i, "process_part": "SP-1", "ratio": 2.0}]})
        with self.assertRaises(ValueError):
            db.add_postprocess_link("OC-1", "SP-1")
        with self.assertRaises(ValueError):
            db.add_postprocess_link("OC-1", "OC-1")
        with self.assertRaises(ValueError):
            db.add_postprocess_link("OC-2", "SP-2", 0)
        j = db.add_postprocess_link("OC-2", "SP-2", status="pending", source="auto_bom", evidence="x")
        self.assertEqual(db.active_postprocess_links(["OC-2"]), {})  # 待確認不生效
        db.update_postprocess_link(j, status="confirmed")
        self.assertIn("OC-2", db.active_postprocess_links())
        db.update_postprocess_link(j, status="rejected")
        self.assertNotIn("OC-2", db.active_postprocess_links())
        self.assertTrue(db.postprocess_pair_exists("OC-2", "SP-2"))  # 駁回的仍算存在
        self.assertTrue(db.delete_postprocess_link(i))
        self.assertFalse(db.delete_postprocess_link(i))

    def test_api_crud_and_confirm(self):
        from fastapi.testclient import TestClient
        from main import app

        c = TestClient(app)
        r = c.post("/api/postprocess/links", json={"base_part": "OC-9", "process_part": "SP-9", "ratio": 1.5})
        self.assertEqual(r.status_code, 200, r.text)
        lid = r.json()["link"]["id"]
        self.assertEqual(c.post("/api/postprocess/links", json={"base_part": "OC-9", "process_part": "SP-9"}).status_code, 400)
        self.assertEqual(c.patch(f"/api/postprocess/links/{lid}", json={"ratio": 2}).json()["link"]["ratio"], 2)
        pid = db.add_postprocess_link("OC-8", "SP-8", status="pending", source="auto_po")
        self.assertEqual(c.post(f"/api/postprocess/links/{pid}/confirm").json()["link"]["status"], "confirmed")
        self.assertEqual(c.get("/api/postprocess/check?parts=OC-9,OC-8").json()["links"].keys(), {"OC-9", "OC-8"})
        self.assertEqual(c.post(f"/api/postprocess/links/{lid}/reject").json()["link"]["status"], "rejected")
        self.assertEqual(c.delete(f"/api/postprocess/links/{lid}").status_code, 200)
        self.assertEqual(c.delete("/api/postprocess/links/9999").status_code, 404)

    def test_bom_suggestion_pending_and_erp_down_safe(self):
        from unittest.mock import patch

        self.conn.execute("INSERT INTO bom_files(id, filename, group_model) VALUES('f1','a.xlsx','M1')")
        for i, (part, dash) in enumerate([("OC-5", 0), ("SP-5", 1), ("EC-1", 0), ("SP-6", 1)]):
            self.conn.execute("INSERT INTO bom_components(bom_file_id, part_number, is_dash) VALUES('f1',?,?)", (part, dash))
        db.add_postprocess_link("OC-9", "SP-9")
        # ERP 不可用：只用 OC- 前綴推測；EC-1 → SP-6 不是 OC- 不產生
        with patch.object(pp, "_resolve_base_vendors", lambda parts: ({}, "boom")), \
                patch.object(pp, "_query_po_history", lambda since: (_ for _ in ()).throw(RuntimeError("down"))):
            r = pp.suggest()
        self.assertEqual([(c["base_part"], c["process_part"]) for c in r["created"]], [("OC-5", "SP-5")])
        self.assertTrue(r["warnings"])
        link = next(l for l in db.list_postprocess_links() if l["base_part"] == "OC-5")
        self.assertEqual((link["status"], link["source"]), ("pending", "auto_bom"))
        self.assertIn("OC- 前綴", link["evidence"])
        self.assertNotIn("OC-5", db.active_postprocess_links())
        with patch.object(pp, "_resolve_base_vendors", lambda parts: ({}, "boom")), \
                patch.object(pp, "_query_po_history", lambda since: []):
            self.assertEqual(pp.suggest()["created_count"], 0)  # 已存在不重複

    def test_bom_suggestion_uses_erp_vendor(self):
        from unittest.mock import patch

        self.conn.execute("INSERT INTO bom_files(id, filename, group_model) VALUES('f1','a.xlsx','M1')")
        for part, dash in [("OC-5", 0), ("SP-5", 1), ("OC-7", 0), ("SP-7", 1)]:
            self.conn.execute("INSERT INTO bom_components(bom_file_id, part_number, is_dash) VALUES('f1',?,?)", (part, dash))
        with patch.object(pp, "_resolve_base_vendors", lambda parts: ({"OC-5": "P0372", "OC-7": "P0001"}, "")), \
                patch.object(pp, "_query_po_history", lambda since: []):
            r = pp.suggest()
        self.assertEqual([c["base_part"] for c in r["created"]], ["OC-5"])


class PurePoTests(unittest.TestCase):
    def test_pair_po_lines(self):
        rows = [
            ("PO-20260901001", 20260901, "P0372", "OC-10988B", 1000),
            ("PO-20260901002", 20260901, "P0332", "SP-10024B", 1000),
            ("PO-20260901003", 20260901, "P0332", "SP-10025B", 1000),   # 不連號
            ("PO-20260902001", 20260902, "P0372", "OC-10989B", 500),
            ("PO-20260902002", 20260902, "P0332", "SP-10025B", 400),    # 數量不同
            ("PO-20260903001", 20260903, "P0372", "OC-10990B", 300),
            ("PO-20260903002", 20260903, "P9999", "SP-10026B", 300),    # 非加工廠
            ("PO-20260904001", 20260904, "P0372", "OC-10991B", 200),
            ("PO-20260904002", 20260904, "P0020", "XX-1", 200),         # 非 SP- 前綴
        ]
        found = pp.pair_po_lines(rows, "P0372", pp.processor_vendors(), ("SP-",))
        self.assertEqual(list(found), [("OC-10988B", "SP-10024B")])
        self.assertEqual(found[("OC-10988B", "SP-10024B")]["bills"], ["PO-20260901001/PO-20260901002"])

    def test_linked_qty_and_dates(self):
        self.assertEqual(pp.linked_qty(1000, 1), 1000)
        self.assertEqual(pp.linked_qty(1001, 0.5), 501)
        self.assertEqual(pp.linked_qty(0, 1), 0)
        d = pp.linked_dates(date(2026, 12, 10), TODAY)  # 加工 11/19，鈞洋 11/15（週日→11/13）
        self.assertEqual(d["process"]["date"], date(2026, 11, 19))
        self.assertEqual(d["base_raw"], date(2026, 11, 15))
        self.assertEqual(d["base"]["date"], date(2026, 11, 13))
        d = pp.linked_dates(date(2026, 11, 1), TODAY)  # 加工 clamp 今天 → 鈞洋 = 今天 − 4 → clamp 今天
        self.assertEqual((d["process"]["date"], d["base"]["date"]), (TODAY, TODAY))
        self.assertIsNone(pp.linked_dates(None, TODAY))


class ShortageLinkTests(tsp.ShortageListTests):
    def setUp(self):
        super().setUp()
        self.erp["products"].append(("SP-1", "印刷外殼", "S4"))
        self.erp["suppliers"].append(("S4", "聖佑", ""))
        self.erp["last_po"].append(("SP-1", "S4", 0.8, "PO-20260801009", 20260801))
        self.link_id = db.add_postprocess_link("OC-1", "SP-1", 1)

    # 繼承來的日期測試假設 OC-1 沒有對應（鈞洋件日期會改成加工件 − 4 天），在這個類別以下面的測試取代
    test_dates_category_and_clamp = None
    test_manual_override_beats_heuristic = None

    def test_mapped_part_carries_processing_info_and_dates(self):
        _, p = self.parts()
        entry = p["OC-1"]["postprocess"][0]
        self.assertEqual((entry["process_part"], entry["supplier_id"], entry["price"], entry["supplier_source"]),
                         ("SP-1", "S4", 0.8, "master"))
        self.assertEqual(entry["pre_in_date"], "2026-11-19")        # 12/10 − 21
        self.assertEqual(entry["original_pre_in_date"], "2026-11-19")
        self.assertFalse(entry["in_list"])
        # 鈞洋件 = 加工件 − 4 = 11/15（週日）→ 遇假日提前 11/13
        self.assertEqual(p["OC-1"]["pre_in_dates"]["backend"], "2026-11-13")
        self.assertEqual(p["OC-1"]["due_shift_reason"], "遇假日提前至 11/13")
        self.assertEqual(p["PK-1"]["postprocess"], [])

    def test_pending_and_disabled_not_attached(self):
        db.update_postprocess_link(self.link_id, enabled=False)
        _, p = self.parts()
        self.assertEqual(p["OC-1"]["postprocess"], [])
        self.assertEqual(p["OC-1"]["pre_in_dates"]["backend"], "2026-11-19")

    def test_clamps_to_today(self):
        self.groups = [tsp.group(3, "6-3", "2026-11-05", [tsp.item("OC-1", 6)])]
        _, p = self.parts()
        self.assertEqual(p["OC-1"]["postprocess"][0]["pre_in_date"], "2026-10-29")
        self.assertEqual(p["OC-1"]["pre_in_dates"]["backend"], "2026-10-29")

    def test_existing_process_row_flagged_in_list(self):
        self.groups = list(self.groups) + [tsp.group(4, "6-4", "2026-12-10", [tsp.item("SP-1", 7)])]
        self.conn.execute("INSERT INTO orders(id, code, model, po_number, status) VALUES(4,'6-4','M','PO4','dispatched')")
        _, p = self.parts()
        self.assertTrue(p["OC-1"]["postprocess"][0]["in_list"])

    def test_missing_postprocess_warning(self):
        lines = [{"part_number": "oc-1", "qty": 1001}]
        w = pp.missing_postprocess(lines)
        self.assertEqual([(i["base_part"], i["process_part"], i["suggested_qty"]) for i in w], [("OC-1", "SP-1", 1001)])
        self.assertEqual(pp.missing_postprocess(lines + [{"part_number": "SP-1", "qty": 1}]), [])
        self.assertEqual(pp.missing_postprocess([{"part_number": "PK-1", "qty": 1}]), [])
        self.assertEqual(pp.missing_postprocess_for_groups([{"lines": lines}, {"lines": [{"part_number": "SP-1", "qty": 5}]}]), [])


class EnforcementApiTests(unittest.TestCase):
    """shortage create / manual create 在加工件缺席時不寫入，要 confirm_missing_postprocess 才繼續。"""

    def setUp(self):
        import sqlite3
        from contextlib import contextmanager
        from unittest.mock import patch

        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db._CREATE_SQL)

        @contextmanager
        def temp_conn():
            yield self.conn
            self.conn.commit()

        self.calls = []
        for p in (
            patch.object(db, "get_conn", temp_conn),
            patch.object(sp, "find_group_duplicates", lambda groups: []),
            patch.object(sp, "prepare_groups", lambda groups, ids=None: groups),
            patch.object(sp, "create_many", lambda groups, actor="", ip="": self.calls.append(groups) or [{"ok": True, "supplier_id": "P0372"}]),
            patch("app.services.erp_purchase.create_po", lambda raw, actor="", client_ip="": self.calls.append(raw) or {
                "ok": True, "bill_no": "PO-1", "target_label": "t", "supplier": {"id": "P0372"}, "totals": {"total": 1}}),
        ):
            p.start()
            self.addCleanup(p.stop)
        db.add_postprocess_link("OC-1", "SP-1", 1)
        from fastapi.testclient import TestClient
        from main import app

        self.client = TestClient(app)
        self.group = {"supplier_id": "P0372", "remark": "", "lines": [{"part_number": "OC-1", "qty": 100, "price": 1, "pre_in_date": "2026-11-13"}]}

    def test_shortage_create_blocks_then_proceeds_with_flag(self):
        body = {"groups": [self.group], "order_ids": [], "confirm": True}
        r = self.client.post("/api/shortage-po/create", json=body).json()
        self.assertEqual(r["results"], [])
        self.assertEqual(r["postprocess_warning"][0]["process_part"], "SP-1")
        self.assertEqual(self.calls, [])
        ok = self.client.post("/api/shortage-po/create", json={**body, "confirm_missing_postprocess": True}).json()
        self.assertTrue(ok["results"][0]["ok"])
        self.assertEqual(len(self.calls), 1)

    def test_shortage_create_ok_when_process_part_in_other_group(self):
        other = {"supplier_id": "P0332", "remark": "", "lines": [{"part_number": "SP-1", "qty": 100, "price": 1, "pre_in_date": "2026-11-19"}]}
        r = self.client.post("/api/shortage-po/create", json={"groups": [self.group, other], "order_ids": [], "confirm": True}).json()
        self.assertNotIn("postprocess_warning", r)
        self.assertEqual(len(self.calls), 1)

    def test_manual_create_blocks_then_proceeds(self):
        body = {**self.group, "confirm": True}
        r = self.client.post("/api/erp/po/create", json=body).json()
        self.assertEqual(r["ok"], False)
        self.assertEqual(r["postprocess_warning"][0]["base_part"], "OC-1")
        self.assertEqual(self.calls, [])
        ok = self.client.post("/api/erp/po/create", json={**body, "confirm_missing_postprocess": True}).json()
        self.assertEqual(ok["bill_no"], "PO-1")


if __name__ == "__main__":
    unittest.main()
