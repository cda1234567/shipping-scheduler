"""出貨單（v2026.11.16.1 起；v2026.11.16.2 HS 歷史對照、品名／HS 可改）：正航 S/C / Invoice / 客戶 PO 帶品項，套 OPENTEXT 模板匯出。不連正航。"""
from __future__ import annotations

import io
import re
import unittest
import warnings
import zipfile
from datetime import datetime
from unittest.mock import patch

from fastapi.testclient import TestClient
from openpyxl import load_workbook

from app.services import shipnote as sn

SPECS = [
    # 淨重主檔：轉接板（_extract_item_no 認不出 PCB-，靠名稱比對）與線材（有 item_no）
    {"name": "T3IU ADAPTER PCB-A1080", "packing_name": "T3IU ADAPTER PCB-A1080", "item_no": "",
     "per_box_qty": 16, "net_weight": 11.2, "gross_weight": 14, "volume": 2.6},
    {"name": "TA7-9 ADAPTER PCB-A1132", "packing_name": "TA7-9 ADAPTER PCB-A1132", "item_no": "",
     "per_box_qty": 80, "net_weight": 4.4, "gross_weight": 5.4, "volume": 1.85},
    {"name": "TC2-8-R2 Cable AC-40803B", "packing_name": "TC2-8-R2 Cable AC-40803B", "item_no": "AC-40803B",
     "per_box_qty": 400, "net_weight": 7, "gross_weight": 8, "volume": 1.2},
]


def _sc(bill, ref, row, item, desc, qty, price, amount=None, remain=None):
    return {"BillNo": bill, "Ref": ref, "BillDate": 20261113, "CustID": "C0152", "CurrID": "USD",
            "RowNo": row, "ItemNo": item, "Description": desc, "Quantity": qty, "Price": price,
            "Amount": qty * price if amount is None else amount, "QtyRemain": qty if remain is None else remain}


def _inv(bill, row, item, name, qty, price, from_no, from_row, ref):
    return {"BillNO": bill, "RowNO": row, "ProdID": item, "ProdName": name, "Quantity": qty, "Price": price,
            "Amount": qty * price, "FromNO": from_no, "FromRow": from_row, "Ref": ref, "CustID": "C0152", "CurrID": "USD"}


SC_ROWS = [
    _sc("41807000", "4500059497", 5, "PCB-A1132", "TA7-9 Adapter. PCIe Gen 4-FireWire Adapter.", 200, 20.98),
    _sc("41807000", "4500059497", 6, "PCB-A1132-TAB", "客供料", 200, 0),
    _sc("41808000", "4500059497-A", 1, "PCB-A1127", "TA7-1 Adapter. PCIe Gen 4 Card SSD Adapter.", 300, 15.35),
    _sc("41809000", "4500059497-B", 1, "PCB-A1080", "T3iu A+B -REV-1.2+1.0 Forensic SATA Imaging Bay", 120, 92.8),
    _sc("41810000", "45000594970", 1, "PCB-A1080", "前綴相同但不是後綴拆單", 1, 1),
    _sc("4131D000", "4500059433", 1, "pcb-a1080-tab", "小寫客供料", 1, 1),
]
INV_ROWS = [
    _inv("ST-2026276", 3, "PCB-A1080", "T3iu A+B -REV-1.2+1.0 Forensic SATA Imaging Bay", 120, 92.8, "4131D000", 1, "4500059433"),
    _inv("ST-2026276", 4, "PCB-A1109", "TDA3-3-WIP  REV-1.0", 250, 37.45, "4131D000", 3, "4500059433"),
    _inv("ST-2026276", 9, "AC-40852B-TAB", "客供", 1, 0, "4131D000", 9, "4500059433"),
    _inv("ST-2026220", 4, "PB-20117A", "T356789iu B (IDE) REV 1.1 bare PCB. ---代購料", 1000, 1.82, "", 0, None),
]


def _fake_fetch(keywords):
    return SC_ROWS, INV_ROWS


# ── 搜尋 ────────────────────────────────────────────────────────────────

class SearchTests(unittest.TestCase):
    def test_parse_keywords_splits_multiple_and_uppercases(self):
        assert sn.parse_keywords("4500059497, st-2026276\n4131d000  4500059497") == ["4500059497", "ST-2026276", "4131D000"]
        assert sn.parse_keywords(["a b", "C"]) == ["A", "B", "C"]
        assert sn.parse_keywords("  ") == []

    def test_search_excludes_customer_supplied_tab_rows(self):
        res = sn.search_items("4500059497 4500059433 ST-2026276", fetcher=_fake_fetch, specs=SPECS)
        item_nos = [i["item_no"] for i in res["items"]]
        assert item_nos
        assert not any(n.upper().endswith("-TAB") for n in item_nos)

    def test_po_search_lists_exact_and_suffix_refs_without_merging(self):
        res = sn.search_items("4500059497", fetcher=_fake_fetch, specs=SPECS)
        refs = [i["cust_po"] for i in res["items"]]
        assert refs == ["4500059497", "4500059497-A", "4500059497-B"]  # 完整 Ref 各自保留
        assert "45000594970" not in refs  # 只是前綴相同，不是「PO-」後綴
        assert len({i["key"] for i in res["items"]}) == 3
        assert res["not_found"] == []

        # 指定後綴只列那一張
        only_a = sn.search_items("4500059497-A", fetcher=_fake_fetch, specs=SPECS)
        assert [i["cust_po"] for i in only_a["items"]] == ["4500059497-A"]

    def test_sc_query_matches_bill_ref_and_dash_suffix_only(self):
        sql, params = sn.build_sc_query(["4500059497", "A_1%"])
        assert "m.Flag = 2" in sql and "JOIN TraBillSub" in sql and "m.Ref" in sql
        assert "CustBillNo" not in sql
        assert params[:3] == ("4500059497", "4500059497", "4500059497-%")
        assert params[5] == "A[_]1[%]-%"  # LIKE 跳脫

    def test_invoice_query_uses_comprodrec_and_fromno_to_sc_ref(self):
        sql, params = sn.build_invoice_query(["ST-2026276"])
        assert "FROM comProdRec" in sql and "r.Flag = 501" in sql
        assert "sc.Flag = 2 AND sc.BillNo = r.FromNO" in sql
        assert "sc.Ref" in sql
        assert params == ("ST-2026276",)

    def test_sc_field_mapping(self):
        res = sn.search_items("4500059497-B", fetcher=_fake_fetch, specs=SPECS)
        (item,) = res["items"]
        assert item["source"] == "S/C"
        assert item["cust_po"] == "4500059497-B"
        assert item["bill_no"] == "41809000"
        assert item["row_no"] == 1
        assert item["item_no"] == "PCB-A1080"
        assert item["erp_description"] == "T3iu A+B -REV-1.2+1.0 Forensic SATA Imaging Bay"
        assert item["qty"] == 120 and item["price"] == 92.8
        self.assertAlmostEqual(item["amount"], 11136)

    def test_invoice_field_mapping_takes_po_from_fromno_ref(self):
        res = sn.search_items("ST-2026276 ST-2026220", fetcher=_fake_fetch, specs=SPECS)
        by_row = {(i["bill_no"], i["row_no"]): i for i in res["items"]}
        item = by_row[("ST-2026276", 3)]
        assert item["source"] == "Invoice"
        assert item["cust_po"] == "4500059433"  # 不是 Invoice 表頭 Ref（空的），而是 FromNO 的 S/C Ref
        assert item["sc_no"] == "4131D000"
        assert item["item_no"] == "PCB-A1080"
        assert item["qty"] == 120 and item["price"] == 92.8
        self.assertAlmostEqual(item["amount"], 11136)
        # FromNO 對不到 S/C：PO 留空，不編值
        assert by_row[("ST-2026220", 4)]["cust_po"] == ""
        assert ("ST-2026276", 9) not in by_row  # -TAB

    def test_not_found_keywords_reported(self):
        res = sn.search_items("4500099999 ST-0000001", fetcher=_fake_fetch, specs=SPECS)
        assert res["items"] == []
        assert res["not_found"] == ["4500099999", "ST-0000001"]

    def test_packing_master_name_and_weights_when_present_blank_when_missing(self):
        res = sn.search_items("4500059497 ST-2026276", fetcher=_fake_fetch, specs=SPECS)
        by_item = {i["item_no"]: i for i in res["items"]}
        hit = by_item["PCB-A1132"]
        assert hit["name"] == "TA7-9 ADAPTER PCB-A1132"  # 淨重主檔名稱，不是正航 Description
        assert (hit["per_box_qty"], hit["net_weight"], hit["gross_weight"], hit["volume"]) == (80, 4.4, 5.4, 1.85)
        assert hit["spec_matched"] is True

        miss = by_item["PCB-A1127"]  # 主檔沒有
        assert miss["name"] == "TA7-1 Adapter. PCIe Gen 4 Card SSD Adapter."  # 用正航 Description
        assert miss["per_box_qty"] is None and miss["net_weight"] is None
        assert miss["gross_weight"] is None and miss["volume"] is None
        assert miss["spec_matched"] is False

    def test_packing_match_requires_whole_item_no(self):
        specs = [{"name": "T356789IU ADAPTER PCB-A1121", "packing_name": "T356789IU ADAPTER PCB-A1121", "item_no": "",
                  "per_box_qty": 24, "net_weight": 8.3, "gross_weight": 11, "volume": 2.6}]
        assert sn.match_packing_spec("PCB-A112", specs) is None
        assert sn.match_packing_spec("PCB-A1121", specs) is specs[0]

    def test_template_packing_specs_include_pcb_adapters(self):
        specs = sn.template_packing_specs()
        spec = sn.match_packing_spec("PCB-A1080", specs)
        assert spec is not None
        assert spec["packing_name"] == "T3IU ADAPTER PCB-A1080"
        assert spec["per_box_qty"] == 16

    def test_harmonized_code_from_history_lookup(self):
        res = sn.search_items("4500059497", fetcher=_fake_fetch, specs=SPECS)
        by_item = {i["item_no"]: i for i in res["items"]}
        # PCB-A1080 淨重主檔品名「T3IU ADAPTER PCB-A1080」→ 歷史對照 8471.90.0000
        self.assertEqual(by_item["PCB-A1080"]["name"], "T3IU ADAPTER PCB-A1080")
        self.assertEqual(by_item["PCB-A1080"]["harmonized_code"], "8471.90.0000")
        self.assertEqual(by_item["PCB-A1132"]["harmonized_code"], "8471.90.0000")  # 品名對照
        # PCB-A1127 淨重主檔沒有，品名用正航 Description（對照表沒有）→ 改用料號對照
        self.assertEqual(by_item["PCB-A1127"]["name"], "TA7-1 Adapter. PCIe Gen 4 Card SSD Adapter.")
        self.assertEqual(by_item["PCB-A1127"]["harmonized_code"], "8471.90.0000")


class HsLookupTests(unittest.TestCase):
    """HS 只用歷史出貨單對照：先品名、再料號；衝突或沒對到留空，不猜。"""

    def test_lookup_file_is_packaged_and_clean(self):
        import json

        path = sn.HS_LOOKUP_PATH
        self.assertTrue(path.exists())
        self.assertEqual(path.parent.name, "resources")  # 跟著 app/ 進 Docker image 與桌面版
        data = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(len(data["unique_item_to_hs"]), 70)
        self.assertEqual(len(data["unique_norm_name_to_hs"]), 112)
        self.assertNotIn("PCB-S/A", data["unique_item_to_hs"])
        self.assertFalse(set(data["unique_item_to_hs"]) & set(data["item_conflicts"]))
        self.assertFalse(set(data["unique_norm_name_to_hs"]) & set(data["name_conflicts"]))

    def test_pcb_a1080_defaults_to_8471(self):
        self.assertEqual(sn.lookup_harmonized_code("T3IU ADAPTER PCB-A1080", "PCB-A1080"), "8471.90.0000")
        self.assertEqual(sn.lookup_harmonized_code("", "PCB-A1080"), "8471.90.0000")

    def test_name_normalizes_whitespace(self):
        self.assertEqual(sn.lookup_harmonized_code("  T3IU   ADAPTER\tPCB-A1080 ", ""), "8471.90.0000")

    def test_item_no_case_insensitive(self):
        self.assertEqual(sn.lookup_harmonized_code("不在對照表的品名", "pcb-a1080"), "8471.90.0000")

    def test_name_takes_priority_over_item(self):
        # OC-10960B 料號有衝突（轉接板 / 袋子），品名可以分得開
        self.assertEqual(sn.lookup_harmonized_code("TB6 Bag OC-10960B", "OC-10960B"), "3923.29.0000")
        self.assertEqual(sn.lookup_harmonized_code("TB6 ADAPTER OC-10960B", "OC-10960B"), "8471.90.0000")

    def test_conflicts_stay_blank(self):
        self.assertEqual(sn.lookup_harmonized_code("TC2-8-R2 Cable AC-40803B", "AC-40803B"), "")  # 品名衝突
        self.assertEqual(sn.lookup_harmonized_code("TC2-8-R2 Cable AC-40803B", "PCB-A1080"), "")  # 品名衝突不再退回料號
        self.assertEqual(sn.lookup_harmonized_code("不在對照表的品名", "AC-40852B"), "")  # 料號衝突
        self.assertEqual(sn.lookup_harmonized_code("不在對照表的品名", "ac-40852b"), "")

    def test_unknown_stays_blank_and_no_guessing(self):
        self.assertEqual(sn.lookup_harmonized_code("", ""), "")
        self.assertEqual(sn.lookup_harmonized_code("USB Cable 1M", "ZZ-99999"), "")  # 不靠關鍵字猜
        self.assertEqual(sn.lookup_harmonized_code("", "PCB-S/A"), "")  # 已從對照表刪除
        self.assertFalse(hasattr(sn, "_infer_harmonized_code"))

    def test_search_conflict_and_unmatched_rows_blank(self):
        rows = [
            _sc("41811000", "4500059600", 1, "AC-40803B", "Cable", 10, 1),  # 淨重主檔品名有衝突
            _sc("41811000", "4500059600", 2, "ZZ-00001", "Some Power Cable", 10, 1),  # 對照表沒有
        ]
        res = sn.search_items("4500059600", fetcher=lambda kws: (rows, []), specs=SPECS)
        by_item = {i["item_no"]: i for i in res["items"]}
        self.assertEqual(by_item["AC-40803B"]["name"], "TC2-8-R2 Cable AC-40803B")
        self.assertEqual(by_item["AC-40803B"]["harmonized_code"], "")
        self.assertEqual(by_item["ZZ-00001"]["harmonized_code"], "")


class FetchGuardTests(unittest.TestCase):
    def test_fetch_refuses_non_chicomp03_database(self):
        settings = {"database": "CHIComp13", "user": "u", "password": "p"}
        with patch.object(sn.erp_stock, "get_settings", return_value=settings), \
                patch.object(sn.erp_stock, "_connect", side_effect=AssertionError("不該連線")):
            with self.assertRaises(sn.ShipnoteError):
                sn.fetch_erp_rows(["4500059497"])


class _FakeCursor:
    def __init__(self, db_name):
        self.db_name = db_name
        self.executed = []
        self._result = []
        self.description = []

    def execute(self, sql, params=()):
        self.executed.append((sql, params))
        if sql.startswith("SELECT DB_NAME()"):
            self.description = [("name",)]
            self._result = [(self.db_name,)]
        elif "FROM comProdRec" in sql:
            cols = list(INV_ROWS[0].keys())
            self.description = [(c,) for c in cols]
            self._result = [tuple(r[c] for c in cols) for r in INV_ROWS]
        else:
            cols = list(SC_ROWS[0].keys())
            self.description = [(c,) for c in cols]
            self._result = [tuple(r[c] for c in cols) for r in SC_ROWS]

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result)


class _FakeConn:
    def __init__(self, db_name="CHIComp03"):
        self.cur = _FakeCursor(db_name)
        self.closed = False

    def cursor(self):
        return self.cur

    def close(self):
        self.closed = True

    def commit(self):  # pragma: no cover - 唯讀不該呼叫
        raise AssertionError("出貨單不可寫正航")


class FetchTests(unittest.TestCase):
    def test_fetch_uses_readonly_connection_select_only(self):
        conn = _FakeConn()
        settings = {"database": "CHIComp03", "user": "u", "password": "p"}
        with patch.object(sn.erp_stock, "get_settings", return_value=settings), \
                patch.object(sn.erp_stock, "_connect", return_value=conn) as connect:
            sc_rows, inv_rows = sn.fetch_erp_rows(["4500059497", "ST-2026276"])
        connect.assert_called_once_with(settings)
        assert conn.closed
        assert len(sc_rows) == len(SC_ROWS) and len(inv_rows) == len(INV_ROWS)
        for sql, _ in conn.cur.executed:
            assert sql.lstrip().upper().startswith("SELECT")
            assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|EXEC)\b", sql.upper())

    def test_fetch_checks_connected_database_name(self):
        conn = _FakeConn(db_name="CHIComp13")
        with patch.object(sn.erp_stock, "get_settings", return_value={"database": "CHIComp03"}), \
                patch.object(sn.erp_stock, "_connect", return_value=conn):
            with self.assertRaises(sn.ShipnoteError):
                sn.fetch_erp_rows(["4500059497"])


# ── 匯出 ────────────────────────────────────────────────────────────────

def _item(po, name, qty, price, **kw):
    base = {"cust_po": po, "item_no": kw.pop("item_no", "PCB-A1080"), "name": name, "harmonized_code": "",
            "qty": qty, "price": price}
    base.update(kw)
    return base


SHIP_ITEMS = [
    _item("4500059433", "T3IU ADAPTER PCB-A1080", 120, 92.8, carton_no="1~7", box_count=7, per_box_qty=16,
          net_weight=11.2, gross_weight=14, volume=2.6, tail_carton_no="8", tail_qty=8,
          tail_net_weight=5.6, tail_gross_weight=8.4, tail_volume=2.6),
    _item("4500059497-A", "TA7-9 ADAPTER PCB-A1132", 160, 20.98, item_no="PCB-A1132", carton_no="24~25",
          box_count=2, per_box_qty=80, net_weight=4.4, gross_weight=5.4, volume=1.85),
    _item("4500059563", "T35U ADAPTER PCB-A1068", 580, 36.32, item_no="PCB-A1068", carton_no="9~22",
          box_count=14, per_box_qty=40, net_weight=7, gross_weight=8, volume=1.85, tail_carton_no="23",
          tail_qty=20, tail_net_weight=7, tail_gross_weight=8, tail_volume=1.85),
]


def _load(content, data_only=True):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return load_workbook(io.BytesIO(content), data_only=data_only)


def _export(**kw):
    payload = {"type": "general", "date": "2026-11-16", "items": SHIP_ITEMS, "mark": "WAUKESHA"}
    payload.update(kw)
    return sn.export_shipnote(payload)


def _all_values(wb):
    return [c.value for ws in wb for row in ws.iter_rows() for c in row if c.value is not None]


class ExportTests(unittest.TestCase):
    def test_export_general_items_po_name_qty_price_total(self):
        content, filename = _export()
        assert filename == "OPENTEXT 261116出貨單.xlsx"
        ws = _load(content).active
        assert ws["A6"].value == 1
        assert ws["B7"].value == "PO:4500059433"
        assert ws["B8"].value == "T3IU ADAPTER PCB-A1080"
        assert ws["O8"].value == 120 and ws["P8"].value == 92.8
        self.assertAlmostEqual(ws["Q8"].value, 11136)
        assert ws["B11"].value == "PO:4500059497-A"
        assert ws["B12"].value == "TA7-9 ADAPTER PCB-A1132"
        self.assertAlmostEqual(ws["Q12"].value, 160 * 20.98)
        assert ws["A14"].value == 3 and ws["B15"].value == "PO:4500059563"
        assert ws["I7"].value == "1~7" and ws["J7"].value == 7 and ws["K7"].value == 16
        assert ws["I9"].value == "8" and ws["J9"].value == 1 and ws["K9"].value == 8
        # 公式本身是 P*O，不是舊模板的手列
        formulas = _load(content, data_only=False).active
        assert formulas["Q8"].value == "=P8*O8"

    def test_export_uses_edited_name_and_hs_code(self):
        items = [dict(SHIP_ITEMS[0], name="T3IU ADAPTER PCB-A1080 (REV-2)", harmonized_code="8471.90.0000"),
                 dict(SHIP_ITEMS[1], name="改過的品名", harmonized_code="1234.56.7890")]
        ws = _load(_export(items=items)[0]).active
        self.assertEqual(ws["B6"].value, "HARMONIZED CODE: 8471.90.0000")
        self.assertEqual(ws["B8"].value, "T3IU ADAPTER PCB-A1080 (REV-2)")
        self.assertEqual(ws["B10"].value, "HARMONIZED CODE: 1234.56.7890")
        self.assertEqual(ws["B12"].value, "改過的品名")
        # 匯出不會用對照表蓋掉畫面上的值；HS 清空就照空白匯出
        items = [dict(SHIP_ITEMS[0], harmonized_code="")]
        ws = _load(_export(items=items)[0]).active
        self.assertEqual(ws["B6"].value.strip(), "HARMONIZED CODE:")

    def test_export_clears_old_sample_data_and_bad_formulas(self):
        content, _ = _export()
        values = [str(v) for v in _all_values(_load(content, data_only=False))]
        for old in ("4500059418", "PCB-A1122", "PCB-A1121", "PCB-A1071", "HILLIARD", "27~34", "8471.90.0000"):
            assert not any(old in v for v in values), old
        joined = "\n".join(values)
        for bad in ("#REF!", "TOCOL", "VSTACK", "_xlfn"):
            assert bad not in joined
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            sheet = z.read("xl/worksheets/sheet1.xml").decode("utf-8")
            assert 't="array"' not in sheet and 't="shared"' not in sheet
            assert "xl/calcChain.xml" not in z.namelist()

    def test_export_totals_correct_and_consistent_across_sections(self):
        content, _ = _export()
        ws = _load(content).active
        total_amount = 120 * 92.8 + 160 * 20.98 + 580 * 36.32
        total_boxes = (7 + 1) + 2 + (14 + 1)
        total_volume = 7 * 2.6 + 2.6 + 2 * 1.85 + 14 * 1.85 + 1.85
        total_weight = 7 * 14 + 8.4 + 2 * 5.4 + 14 * 8 + 8
        for b in (0, 33, 66):
            self.assertAlmostEqual(ws[f"O{b + 30}"].value, total_boxes)
            self.assertAlmostEqual(ws[f"O{b + 31}"].value, total_volume)
            self.assertAlmostEqual(ws[f"O{b + 32}"].value, total_weight)
            assert ws[f"Q{b + 31}"].value == 0  # 銷項稅額
            self.assertAlmostEqual(ws[f"Q{b + 33}"].value, total_amount)  # 總計
        self.assertAlmostEqual(ws["Q30"].value, total_amount)  # 第一聯小計
        self.assertAlmostEqual(ws["Q32"].value, total_amount)
        assert ws["Q63"].value == 0 and ws["Q65"].value == 0  # 沒品項的聯小計 0

        formulas = _load(content, data_only=False).active
        assert formulas["O30"].value == "=SUM(J7,J9,J11,J15,J17)"
        assert formulas["O31"].value == "=SUM(N8,N9,N12,N16,N17)"
        assert formulas["O32"].value == "=SUM(M8,M9,M12,M16,M17)"
        assert formulas["O63"].value == formulas["O30"].value
        assert formulas["Q30"].value == "=SUM(Q8,Q12,Q16)"
        assert formulas["Q33"].value == "=SUM(Q32,Q65,Q98)"

    def test_export_date_and_mark_synced_in_every_section_with_date_format(self):
        content, _ = _export(date="2026-11-20", mark="WAUKESHA", maker="Andy")
        ws = _load(content).active
        for b in (0, 33, 66):
            cell = ws[f"Q{b + 3}"]
            assert isinstance(cell.value, datetime) and cell.value.date().isoformat() == "2026-11-20"
            assert cell.is_date and cell.number_format == sn.DATE_FORMAT_CODE
            assert ws[f"E{b + 30}"].value == "WAUKESHA"
            assert ws[f"B{b + 30}"].value == "Andy"

    def test_export_invoice_no_blank_by_default_and_filled_when_given(self):
        ws = _load(_export()[0]).active
        for b in (0, 33, 66):
            assert ws[f"Q{b + 2}"].value is None
            assert ws[f"P{b + 2}"].value == "INVOICE NO:"
        ws = _load(_export(invoice_no="ST-2026276")[0]).active
        assert [ws[f"Q{b + 2}"].value for b in (0, 33, 66)] == ["ST-2026276"] * 3

    def test_export_default_date_is_today_and_maker_andy(self):
        with patch.object(sn, "local_now", return_value=datetime(2026, 11, 16, 10, 0)):
            content, filename = sn.export_shipnote({"type": "ups", "items": SHIP_ITEMS[:1]})
        assert filename == "OPENTEXT 261116出貨單-UPS.xlsx"
        ws = _load(content).active
        assert ws["Q3"].value.date().isoformat() == "2026-11-16"
        assert ws["B30"].value == "Andy" and ws["E30"].value == "HILLIARD"

    def test_export_billing_has_no_box_weights_zero_totals_and_remark(self):
        items = [dict(SHIP_ITEMS[0], qty=1000, price=27.5)]
        content, filename = _export(type="billing", items=items, remark="請款 貨未進，用於支付 30%預付款")
        assert filename == "OPENTEXT 261116請款單.xlsx"
        ws = _load(content).active
        assert ws["B8"].value == "T3IU ADAPTER PCB-A1080"
        assert ws["O8"].value == 1000 and ws["P8"].value == 27.5
        self.assertAlmostEqual(ws["Q8"].value, 27500)
        for r in range(6, 30):
            for col in "IJKLMN":
                assert ws[f"{col}{r}"].value is None, f"{col}{r}"
        for b in (0, 33):
            assert ws[f"O{b + 30}"].value == 0
            assert ws[f"O{b + 31}"].value == 0
            assert ws[f"O{b + 32}"].value == 0
            assert ws[f"J{b + 31}"].value == "請款 貨未進，用於支付 30%預付款"
            self.assertAlmostEqual(ws[f"Q{b + 33}"].value, 27500)
        assert ws["K31"].value is None  # 舊樣本備註已清
        assert ws.max_row == 66  # 請款單兩聯

    def test_export_sea_template_two_sections_and_overflow_to_next_section(self):
        items = [dict(SHIP_ITEMS[1], cust_po=f"PO{i}") for i in range(8)]
        content, filename = _export(type="sea", items=items)
        assert filename == "OPENTEXT 261116海運出貨單.xlsx"
        wb = _load(content)
        assert len(wb.sheetnames) == 1
        ws = wb.active
        assert ws["B7"].value == "PO: PO0"
        assert ws["A26"].value == 6 and ws["B27"].value == "PO: PO5"
        assert ws["A39"].value == 7 and ws["B40"].value == "PO: PO6"  # 第 7 筆接到第二聯
        assert ws["A43"].value == 8 and ws["A47"].value is None
        total = 8 * 160 * 20.98
        self.assertAlmostEqual(ws["Q33"].value, total)
        self.assertAlmostEqual(ws["Q66"].value, total)
        self.assertAlmostEqual(ws["Q30"].value, 6 * 160 * 20.98)
        self.assertAlmostEqual(ws["Q63"].value, 2 * 160 * 20.98)
        # 不重複印同一批
        pos = [ws[f"B{r}"].value for r in range(1, 67) if str(ws[f"B{r}"].value or "").startswith("PO:")]
        assert len(pos) == len(set(pos)) == 8

    def test_export_more_items_than_one_sheet_adds_sheet_with_consistent_totals(self):
        items = [dict(SHIP_ITEMS[1], cust_po=f"PO{i}") for i in range(20)]
        content, _ = _export(items=items)
        wb = _load(content)
        assert wb.sheetnames == ["出貨單1", "出貨單2"]
        first, second = wb.worksheets
        assert first["A72"].value == 13 and first["A92"].value == 18
        assert second["A6"].value == 19 and second["A10"].value == 20 and second["A14"].value is None
        total = 20 * 160 * 20.98
        for ws in (first, second):
            for b in (0, 33, 66):
                self.assertAlmostEqual(ws[f"Q{b + 33}"].value, total)
                assert ws[f"O{b + 30}"].value == 40
                assert ws[f"Q{b + 3}"].is_date
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            names = z.namelist()
            assert "xl/drawings/drawing2.xml" in names
            assert "drawing2.xml" in z.read("xl/worksheets/_rels/sheet2.xml.rels").decode("utf-8")

    def test_export_keeps_layout_drawing_and_print_settings(self):
        content, _ = _export()
        template = sn.TEMPLATE_DIR / sn.SHIPNOTE_TYPES["general"]["template"]
        with zipfile.ZipFile(template) as zt, zipfile.ZipFile(io.BytesIO(content)) as zo:
            assert zo.read("xl/drawings/drawing1.xml") == zt.read("xl/drawings/drawing1.xml")  # OPENTEXT 菱形
            assert "OPENTEXT" in zo.read("xl/drawings/drawing1.xml").decode("utf-8")
            t_sheet = zt.read("xl/worksheets/sheet1.xml").decode("utf-8")
            o_sheet = zo.read("xl/worksheets/sheet1.xml").decode("utf-8")
            for tag in ("cols", "mergeCells", "pageMargins", "headerFooter"):
                m = re.search(rf"<{tag}\b.*?</{tag}>|<{tag}\b[^>]*/>", t_sheet, re.S)
                assert m and m.group(0) in o_sheet, tag
            assert re.search(r"<pageSetup\b[^>]*/>", t_sheet).group(0) in o_sheet
            assert '<drawing r:id="rId2"/>' in o_sheet
            # 列高保留
            assert re.findall(r'<row r="(\d+)"[^>]*ht="([\d.]+)"', t_sheet) == re.findall(r'<row r="(\d+)"[^>]*ht="([\d.]+)"', o_sheet)

    def test_export_excludes_tab_items_and_validates_input(self):
        items = SHIP_ITEMS[:1] + [_item("4500059433", "客供", 1, 0, item_no="AC-40852B-TAB")]
        ws = _load(_export(items=items)[0]).active
        assert ws["A10"].value is None
        with self.assertRaises(sn.ShipnoteError):
            _export(items=[_item("X", "客供", 1, 0, item_no="X-TAB")])
        with self.assertRaises(sn.ShipnoteError):
            _export(mark="OTHER")
        with self.assertRaises(sn.ShipnoteError):
            _export(type="air")


class ApiTests(unittest.TestCase):
    def test_export_api_streams_xlsx(self):
        from main import app

        client = TestClient(app)
        resp = client.post("/api/shipnote/export", json={
            "type": "billing", "date": "2026-11-09", "remark": "請款", "items": [SHIP_ITEMS[0]],
        })
        assert resp.status_code == 200
        assert "attachment" in resp.headers["content-disposition"]
        ws = _load(resp.content).active
        assert ws["J31"].value == "請款"
        assert client.post("/api/shipnote/export", json={"items": []}).status_code == 400

    def test_export_api_uses_edited_name_and_hs_code(self):
        from main import app

        client = TestClient(app)
        item = dict(SHIP_ITEMS[0], name="人工改的品名", harmonized_code="8471.90.0000")
        resp = client.post("/api/shipnote/export", json={"type": "general", "date": "2026-11-16", "items": [item]})
        self.assertEqual(resp.status_code, 200)
        ws = _load(resp.content).active
        self.assertEqual(ws["B6"].value, "HARMONIZED CODE: 8471.90.0000")
        self.assertEqual(ws["B8"].value, "人工改的品名")

    def test_frontend_name_and_hs_are_editable_and_exported(self):
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        js = (root / "static" / "modules" / "shipnote.js").read_text(encoding="utf-8")
        html = (root / "static" / "index.html").read_text(encoding="utf-8")
        self.assertIn('_input(index, "name", item.name', js)
        self.assertIn('_input(index, "harmonized_code", item.harmonized_code', js)
        self.assertIn('harmonized_code: String(item.harmonized_code ?? "").trim()', js)
        self.assertIn('name: String(item.name ?? "").trim()', js)
        self.assertIn("<th>HS code</th>", html)

    def test_search_api_uses_service(self):
        from main import app

        client = TestClient(app)
        with patch.object(sn, "fetch_erp_rows", _fake_fetch), patch.object(sn, "get_packing_specs", lambda: SPECS):
            data = client.post("/api/shipnote/search", json={"keywords": "4500059497"}).json()
        assert [i["cust_po"] for i in data["items"]] == ["4500059497", "4500059497-A", "4500059497-B"]
        assert client.get("/api/shipnote/config").json()["marks"] == ["HILLIARD", "WAUKESHA"]

    def test_shipnote_search_and_export_do_not_need_edit_login(self):
        from types import SimpleNamespace

        from app.services.edit_auth import request_requires_edit_auth

        for path in ("/api/shipnote/search", "/api/shipnote/export"):
            req = SimpleNamespace(method="POST", url=SimpleNamespace(path=path))
            assert request_requires_edit_auth(req) is False


if __name__ == "__main__":
    unittest.main()
