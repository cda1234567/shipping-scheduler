"""採購單列印補資料（v2026.11.13.6）：給 t357 產 PDF 前查廠商聯絡人／電話／傳真／付款方式、採購員姓名、明細單位。

唯讀：只 SELECT，連線沿用 erp_purchase_edit._read_session（公司解析同 /api/erp/po/bill），不 commit。
規則（2026-11-16 以 CHIComp03 深福 W0083 / 017 / EC-20136A、UC-A0013B 確認）：
- 表頭：有 bill 且找得到（Flag=4、同廠商）用該張；否則用該廠商最近一張採購單（BillDate DESC, BillNO DESC）。
- 電話只用 ordBillMain.LinkTelephone；沒有就空白。comCustomer.Telephone1/2/3、MobileTel 一律不用。
- 聯絡人 LinkMan、傳真 FaxNo：表頭空白才用 comCustomer 同名欄位。
- 付款方式只印 GatherStyle 名稱（1=次月、2=月結，不加 GatherDelay 天數）；其他值有 GatherOther 印它，否則空白。
  不用 rm_print.pay_terms_text（會變成「次月 30 天」），也不用 comCustTrade.GatherStyle。
- 採購員：comPerson.PersonName；查不到回空字串（t357 自己印代號）。
- 單位：有單號用 ordBillSub.Unit，沒有或缺的用 comProduct.Unit；都查不到就不列。
查不到就留空，不編值。
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from . import erp_purchase as ep
from . import erp_purchase_edit as ee

GATHER_STYLE_NAMES = {1: "次月", 2: "月結"}
MAX_PARTS = 200

_text = ep._text
_get = ep._get
_one = ep._one

# 只查需要的欄；comCustomer 有 Password 等不該讀的欄，也沒有 GatherStyle
_CUSTOMER_COLS = "ID, LinkMan, FaxNo, GatherOther"
_MAIN_COLS = "BillNO, CustomerID, SalesMan, LinkMan, LinkTelephone, FaxNo, GatherStyle, GatherDelay, GatherOther"


def empty_result(supplier_id: str = "") -> Dict[str, Any]:
    return {
        "supplier": {"id": supplier_id, "contact": "", "phone": "", "fax": "", "pay_terms": ""},
        "buyer_name": "",
        "units": {},
    }


def pay_terms_name(gather_style: Any, gather_other: Any = "") -> str:
    """付款方式只印名稱：1=次月、2=月結；其他值有 GatherOther 就印它，否則空白。"""
    try:
        style = int(gather_style or 0)
    except (TypeError, ValueError):
        style = 0
    return GATHER_STYLE_NAMES.get(style) or _text(gather_other)


def parse_parts(parts: Any) -> List[str]:
    if isinstance(parts, str):
        parts = parts.split(",")
    out = [_text(p) for p in (parts or [])]
    return list(dict.fromkeys(p for p in out if p))[:MAX_PARTS]


def _first(*values: Any) -> str:
    for v in values:
        t = _text(v)
        if t:
            return t
    return ""


def _header(cur, supplier_id: str, bill_no: str) -> Optional[Dict[str, Any]]:
    if bill_no:
        cur.execute(
            f"SELECT {_MAIN_COLS} FROM ordBillMain WHERE Flag = 4 AND BillNO = %s AND CustomerID = %s",
            (bill_no, supplier_id),
        )
        main = _one(cur)
        if main is not None:
            return main
    cur.execute(
        f"SELECT TOP 1 {_MAIN_COLS} FROM ordBillMain WHERE Flag = 4 AND CustomerID = %s"
        " ORDER BY BillDate DESC, BillNO DESC",
        (supplier_id,),
    )
    return _one(cur)


def _units(cur, bill_no: str, parts: Iterable[str]) -> Dict[str, str]:
    units: Dict[str, str] = {}
    if bill_no:
        cur.execute("SELECT ProdID, Unit FROM ordBillSub WHERE Flag = 4 AND BillNO = %s ORDER BY RowNO", (bill_no,))
        for r in cur.fetchall():
            part, unit = _text(r[0]), _text(r[1])
            if part and unit and part not in units:
                units[part] = unit
    for part in parts:
        if units.get(part):
            continue
        cur.execute("SELECT Unit FROM comProduct WHERE ProdID = %s", (part,))
        row = cur.fetchone()
        unit = _text(row[0]) if row else ""
        if unit:
            units[part] = unit
    wanted = set(parts)
    return {p: u for p, u in units.items() if p in wanted}


def print_info(supplier: str, salesman: str = "", bill: str = "", parts: Any = "", company: str = "") -> Dict[str, Any]:
    supplier_id = _text(supplier)
    if not supplier_id:
        raise ep.PoValidationError("請提供供應商代號（supplier）")
    bill_no, part_list = _text(bill), parse_parts(parts)
    company = ee._resolve_company(company)
    result = empty_result(supplier_id)
    with ee._read_session(company) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT {_CUSTOMER_COLS} FROM comCustomer WHERE Flag = 2 AND ID = %s", (supplier_id,))
        vendor = _one(cur)
        if vendor is None:
            return result
        main = _header(cur, supplier_id, bill_no)
        head = main or {}
        # 付款方式：有表頭用表頭；沒有任何採購單才看廠商主檔（comCustTrade 不用）
        pay_src = head if main is not None else vendor
        result["supplier"].update(
            contact=_first(_get(head, "LinkMan"), _get(vendor, "LinkMan")),
            phone=_text(_get(head, "LinkTelephone")),
            fax=_first(_get(head, "FaxNo"), _get(vendor, "FaxNo")),
            pay_terms=pay_terms_name(_get(pay_src, "GatherStyle"), _get(pay_src, "GatherOther")),
        )
        person_id = _text(salesman) or _text(_get(head, "SalesMan"))
        if person_id:
            cur.execute("SELECT PersonName FROM comPerson WHERE PersonID = %s", (person_id,))
            row = cur.fetchone()
            result["buyer_name"] = _text(row[0]) if row else ""
        # 單位只拿「指定且找得到的那張單」的明細；改用最近一張單時不算有單號
        own_bill = _text(_get(head, "BillNO")) if bill_no and _text(_get(head, "BillNO")) == bill_no else ""
        result["units"] = _units(cur, own_bill, part_list)
    return result
