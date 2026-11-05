"""進貨單列印資料：依本機 erp_receipts 紀錄，從正航「唯讀」讀回 RM 單（comBillAccounts / stkBillMain / comProdRec
與廠商、人員、部門主檔），組成 rm_print.render_pdf 用的 dict。

只用唯讀帳號（ERP_SQL_USER，資料庫改成該紀錄的 target_db），只有 SELECT；正航連不上時退回本機紀錄（表頭部分欄位空白）。
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from . import erp_stock
from .erp_stock import ErpError, ErpUnavailable
from .local_time import local_now

logger = logging.getLogger(__name__)


class ReceiptNotPrintable(Exception):
    """找不到紀錄 / 紀錄不是成功寫入的進貨單（HTTP 404 / 400）。"""


def _pick(row: Optional[Dict[str, Any]], *names: str, default: Any = None) -> Any:
    """不分大小寫取第一個有值的欄位（正航各版本欄位名略有差異）。"""
    if not row:
        return default
    low = {str(k).lower(): v for k, v in row.items()}
    for n in names:
        v = low.get(n.lower())
        if v is not None and str(v).strip() != "":
            return v.strip() if isinstance(v, str) else v
    return default


def _text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def from_local(rec: Dict[str, Any]) -> Dict[str, Any]:
    """只用本機紀錄能組出的資料（正航連不上時用）。"""
    try:
        lines = json.loads(rec.get("lines_json") or "[]")
    except ValueError:
        lines = []
    return {
        "rm_no": rec.get("rm_no") or "", "rm_date": rec.get("rm_date") or 0,
        "print_date": int(local_now().strftime("%Y%m%d")),
        "vendor_id": rec.get("vendor_id") or "", "vendor_name": rec.get("vendor_name") or "",
        "terms_text": None,
        "currency": rec.get("currency") or "NTD",
        "sum_amount": rec.get("sum_amount"), "sum_tax": rec.get("sum_tax"), "amount_digits": 0,
        "lines": [{
            "row_no": l.get("row_no") or i, "prod_id": l.get("prod_id") or "", "prod_name": l.get("prod_name") or "",
            "ware": l.get("ware") or "", "qty": l.get("qty"), "unit": l.get("unit") or "", "price": l.get("price"),
            "amount": l.get("amount"), "tax": l.get("tax"), "remark": "",
        } for i, l in enumerate(lines, 1) if isinstance(l, dict)],
    }


def _one(cur, sql: str, params) -> Optional[Dict[str, Any]]:
    cur.execute(sql, params)
    names = [d[0] for d in cur.description]
    row = cur.fetchone()
    return dict(zip(names, row)) if row else None


def _all(cur, sql: str, params) -> List[Dict[str, Any]]:
    cur.execute(sql, params)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def _optional(cur, sql: str, params) -> Optional[Dict[str, Any]]:
    try:
        return _one(cur, sql, params)
    except Exception as exc:  # 主檔欄位 / 表在某些公司不存在：該欄留白
        logger.info("進貨單列印：選用查詢失敗（%s）", type(exc).__name__)
        return None


def _optional_all(cur, sql: str, params) -> List[Dict[str, Any]]:
    try:
        return _all(cur, sql, params)
    except Exception as exc:
        logger.info("進貨單列印：選用查詢失敗（%s）", type(exc).__name__)
        return []


def build_from_erp(acct: Dict[str, Any], main: Optional[Dict[str, Any]], recs: List[Dict[str, Any]],
                   vendor: Optional[Dict[str, Any]], buyer: Optional[Dict[str, Any]], dept: Optional[Dict[str, Any]],
                   units: Dict[str, str], wares: Dict[str, str], local: Dict[str, Any],
                   amount_digits: int = 0, invoice: Optional[Dict[str, Any]] = None,
                   addresses: Optional[Dict[str, Dict[str, Any]]] = None, terms_text: Optional[str] = None) -> Dict[str, Any]:
    """把正航讀回的列組成列印資料（純函式，方便測試）。"""
    from . import rm_print

    addresses = addresses or {}

    def addr_of(addr_id: Any) -> str:
        return _text(_pick(addresses.get(_text(addr_id)), "Address"))

    # 廠商地址：單據上的地址 → 單據地址代號（comCustAddress）
    cust_addr = _text(_pick(acct, "CustAddress")) or addr_of(_pick(acct, "AddrID"))
    # 發票地址：stkBillMain.InvAddress（文字）→ InvoAddr 是地址「代號」要查 comCustAddress → 發票主檔地址 → 廠商地址
    inv_addr = (_text(_pick(main, "InvAddress")) or addr_of(_pick(main, "InvoAddr"))
                or _text(_pick(invoice, "Address")) or cust_addr)
    tax_type = int(_pick(acct, "TaxType", default=0) or 0)
    lines = []
    for r in recs:
        prod = _text(_pick(r, "ProdID"))
        ware = _text(_pick(r, "WareID"))
        lines.append({
            "row_no": int(_pick(r, "RowNO", default=0) or 0), "prod_id": prod, "prod_name": _text(_pick(r, "ProdName")),
            "ware": wares.get(ware, ware), "qty": _pick(r, "Quantity", default=0), "unit": units.get(prod, ""),
            "price": _pick(r, "Price", default=0), "amount": _pick(r, "Amount", default=0),
            "tax": _pick(r, "TaxAmt", default=0), "remark": _text(_pick(r, "ItemRemark")),
        })
    local_units = {l["prod_id"]: l["unit"] for l in local.get("lines", []) if l.get("unit")}
    for l in lines:
        l["unit"] = l["unit"] or local_units.get(l["prod_id"], "")
    curr = _text(_pick(acct, "CurrID")) or "NTD"
    return {
        "company_name": rm_print.DEFAULT_COMPANY,
        "print_date": local["print_date"],
        "rm_no": _text(_pick(acct, "FundBillNo")) or local["rm_no"],
        "rm_date": _pick(acct, "BillDate", default=local["rm_date"]),
        "vendor_id": _text(_pick(acct, "CustID")) or local["vendor_id"],
        "vendor_name": _text(_pick(vendor, "FullName", "ShortName")) or local["vendor_name"],
        "vendor_phone": _text(_pick(vendor, "Telephone1")) or _text(_pick(acct, "ContactPhone")),
        "tax_no": _text(_pick(main, "TaxNo")) or _text(_pick(vendor, "TaxNo")),
        "contact_phone": _text(_pick(acct, "ContactPhone")),
        "fax": _text(_pick(acct, "FaxNo")) or _text(_pick(vendor, "FaxNo")),
        # comBillAccounts.PriceOfTax：1 = 單價未稅（stkBillMain.PriceofTax 正航自己的單也是 0，不用它）
        "price_of_tax": bool(_pick(acct, "PriceOfTax", default=False)),
        "currency": curr,
        "buyer": _text(_pick(buyer, "PersonName")) or _text(_pick(acct, "Salesman")),
        "dept": _text(_pick(dept, "DepartName")) or _text(_pick(acct, "DeptID")),
        "vendor_address": cust_addr,
        "maker": _text(_pick(acct, "Maker")),
        "invoice_address": inv_addr,
        "pay_terms": rm_print.pay_terms_text(_pick(main, "GatherStyle", default=0), _pick(main, "GatherDelay", default=0),
                                             _text(_pick(main, "GatherOther"))),
        "prepay_date": _pick(acct, "PrepayDay"), "encash_date": _pick(acct, "EncashDay"),
        "tax_type": tax_type,
        "invoice_no": _text(_pick(acct, "InvoiceNo")),
        # 發票日期在 comInvoice.InvoiceDate（以 comBillAccounts.InvoBillNO 對 comInvoice.InvoBillNo）
        "invoice_date": _pick(invoice, "InvoiceDate") if _text(_pick(acct, "InvoiceNo")) else None,
        "invoice_style": "",
        "discount": _pick(acct, "Discount", default=0) or 0,
        "cash_pay": _pick(acct, "CashPay", default=0) or 0,
        "visa_pay": _pick(acct, "VisaPay", default=0), "other_pay": _pick(acct, "OtherPay", default=0),
        "offset": _pick(acct, "OffSet", default=0),
        "dist_offset": 0,  # comBillAccounts 未找到 DistOffset 欄位
        "sum_amtatax": _pick(main, "SumAmtATax"), "terms_text": terms_text,
        "remark": _text(_pick(acct, "Remark")),
        "sum_amount": _pick(acct, "Total", default=local.get("sum_amount")),
        "sum_tax": _pick(acct, "Tax", default=local.get("sum_tax")),
        "amount_digits": amount_digits,
        "lines": lines or local["lines"],
    }


def load_from_erp(rec: Dict[str, Any]) -> Dict[str, Any]:
    local = from_local(rec)
    settings = dict(erp_stock.get_settings())
    if rec.get("target_db"):
        settings["database"] = rec["target_db"]  # 唯讀帳號，只換資料庫
    conn = erp_stock._connect(settings)
    try:
        cur = conn.cursor()
        rm_no = rec["rm_no"]
        acct = _one(cur, "SELECT * FROM comBillAccounts WHERE Flag = 100 AND SysID = 2 AND FundBillNo = %s", (rm_no,))
        if acct is None:
            raise ErpUnavailable(f"正航 {rec.get('target_db')} 找不到進貨單 {rm_no}")
        main = _optional(cur, "SELECT * FROM stkBillMain WHERE Flag = 100 AND BillNO = %s", (rm_no,))
        recs = _all(cur, "SELECT * FROM comProdRec WHERE Flag = 100 AND BillNO = %s ORDER BY RowNO", (rm_no,))
        vendor = _optional(cur, "SELECT * FROM comCustomer WHERE Flag = 2 AND ID = %s", (_text(_pick(acct, "CustID")),))
        buyer = _optional(cur, "SELECT * FROM comPerson WHERE PersonID = %s", (_text(_pick(acct, "Salesman")),))
        dept = _optional(cur, "SELECT * FROM comDepartment WHERE DepartID = %s", (_text(_pick(acct, "DeptID")),))
        units: Dict[str, str] = {}
        for prod in dict.fromkeys(_text(_pick(r, "ProdID")) for r in recs):
            row = _optional(cur, "SELECT * FROM comProduct WHERE ProdID = %s", (prod,))
            units[prod] = _text(_pick(row, "Unit"))
        wares: Dict[str, str] = {}
        for ware in dict.fromkeys(_text(_pick(r, "WareID")) for r in recs):
            if ware:
                row = _optional(cur, "SELECT WareHouseID, WareHouseName FROM comWareHouse WHERE WareHouseID = %s", (ware,))
                wares[ware] = _text(_pick(row, "WareHouseName")) or ware
        addresses: Dict[str, Dict[str, Any]] = {}
        for row in _optional_all(cur, "SELECT * FROM comCustAddress WHERE Flag = 2 AND ID = %s", (_text(_pick(acct, "CustID")),)):
            addresses[_text(_pick(row, "AddrID"))] = row
        invoice = None
        if _text(_pick(acct, "InvoiceNo")):
            inv_bill = _text(_pick(acct, "InvoBillNO"))
            if inv_bill:
                invoice = _optional(cur, "SELECT * FROM comInvoice WHERE InvoBillNo = %s", (inv_bill,))
            if invoice is None:
                invoice = _optional(cur, "SELECT * FROM comInvoice WHERE InvoiceNO = %s AND SrcBillNO = %s",
                                    (_text(_pick(acct, "InvoiceNo")), rm_no))
        from .rm_print import DEFAULT_TERMS_ID
        terms_id = os.environ.get("RM_PRINT_TERMS_ID") or DEFAULT_TERMS_ID
        terms = _optional(cur, "SELECT TermsContext FROM comTerms WHERE TermsID = %s", (terms_id,))
        # 保留條款開頭 CRLF 與空白，不能經過 _pick。
        terms_text = next((v for k, v in (terms or {}).items() if k.lower() == "termscontext"), None)
        digits = 0
        curr = _text(_pick(acct, "CurrID"))
        if curr and curr.upper() != "NTD":
            row = _optional(cur, "SELECT * FROM comCurrencySys WHERE CurrencyID = %s", (curr,))
            raw = _pick(row, "AmountDeciDigits")
            digits = int(raw) if raw is not None else 2
        return build_from_erp(acct, main, recs, vendor, buyer, dept, units, wares, local, digits,
                              invoice=invoice, addresses=addresses, terms_text=terms_text)
    except ErpError:
        raise
    except Exception as exc:
        raise ErpUnavailable(f"讀取正航進貨單失敗：{type(exc).__name__}") from None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def load_print_data(receipt_id: int) -> Dict[str, Any]:
    from .. import database as db

    rec = db.get_erp_receipt(receipt_id)
    if rec is None:
        raise ReceiptNotPrintable("找不到這筆收料紀錄")
    if rec.get("status") != "ok" or not rec.get("rm_no"):
        raise ReceiptNotPrintable("這筆收料紀錄沒有成功寫入的進貨單，無法列印")
    try:
        data = load_from_erp(rec)
        data["source"] = "erp"
    except ErpError as exc:
        data = from_local(rec)
        data["source"] = "local"
        data["warning"] = f"無法從正航讀回完整表頭，改用本機紀錄列印（部分欄位空白）：{exc}"
    return data
