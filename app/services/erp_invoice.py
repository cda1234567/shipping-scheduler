"""開 Invoice：在正航外銷貿易模組由 S/C（TraBill Flag 2）開立 Invoice（ST- 單，TraBill Flag 501）。

逐欄規則取自 2026-11-10 唯讀 CHIComp03 三張實單（ST-2026288 合併 3 張 S/C、ST-2026285 合併 5 張、ST-2026292 單張），
欄位預設值取自 erp_invoice_schema（CHIComp03 sys.columns）。

寫入公司 / 開關
- 寫入公司 = ERP_INVOICE_TARGET（預設 CHIComp13）。測試公司（ERP_TEST_TARGETS）可寫；
  正式公司 CHIComp03 需同時 ERP_INVOICE_TARGET=CHIComp03、ERP_ALLOW_PROD_INVOICE_CHICOMP03=1、ERP_ALLOW_PROD_WRITE_CHICOMP03=1，
  並有專用寫入帳密（erp_purchase.write_credentials；正式公司絕不使用測試帳密）。其他公司一律拒絕。
- 同一張 Invoice 的所有表（含 S/C 回寫、庫存、彙總、comChangeLog）在同一個 transaction，任何錯誤 rollback；寫完讀回比對才 commit。
- client_token / 內容指紋防重複送出（比照一次收料）。
- 不寫 Packing List（TraPackingMain/Sub）、不寫傳票／收款（accTranBill、comFundSub、comInvoice、VoucherNO、CVoucherNO）
  —— 公司3實單這些都是事後才產生。

單號：ST- + 發票日西元年 + 3 碼年流水（ST-2026292）= TraBillMain Flag 501（與 comBillAccounts Flag 501）
  ST-{year}% 的最大流水 + 1；寫入前確認不存在，撞號 rollback 重選（最多 3 次）。公司13舊單是 yyyymmddNNN，不影響。

TraBillMain（Flag 501）：表頭的客戶 / 日期 / 幣別 / 金額留空（BillDate=0、CustID=''、CurrID=''、ExchRate=0、Total=0、
  SalesID=''、Maker=''、CloseDate=0、OnBoardDate=0、InvoStyle=0、BillStatus=0），真正的值在 comBillAccounts。
  抄第一張被選 S/C：OriginType='S/C'、OriginNo=第一張單號（合併多張也只寫第一張，MulOriSCFlag 仍 False）、IsDirectExport、
  InvoiceType、InvoAddr（空則 Addr）、InvAddress、NWUnit / GWUnit / VolumeUnit、Payment；IsFormal=True、HasCheck=True；
  SumQty = Σ comProdRec.Quantity。
TraBillSub（Flag 501）：Quantity=1、Price=0、Amount=0、QtyRemain=1、QtyCtn=1（三張實單都這樣）；
  真正數量在 DeclareQty = LegalQty = LegalQtyB；ItemNo / Description 抄 S/C；OriginBillNo / OriginRowNo = S/C 單號 / 列號；
  TranType=0、FromRow=0、BillDate=發票日、RowNo=SerNo=1..n、HasCheck=True、IsOdd=False、NotTranPackingQty=NotTranInvQty=0。
comProdRec（Flag 501）：真正的出庫與金額。Quantity=開立數量、Price=SPrice=單價（預設 S/C.Price）、
  Amount=round(qty × price, comCurrencySys.AmountDeciDigits)、MLPrice=round(price × 匯率, 3)（1.56×32.215=50.255）、
  MLAmount=round(qty × price × 匯率, 0)（一次算再捨入：258×2.67×32.215=22191.61→22192，不是 round(Amount×匯率)）、
  ExchRate 用本次發票匯率（不是 S/C 匯率）、TranType=404、FromNO / FromRow=S/C、QtyRemain=Quantity、
  CostAvg=出貨時平均成本（CostByWare=0：comProduct.CAvgCost；=1：該倉 comWareAmount.Cost，沒有就退回 CAvgCost）、
  CostForAcc=round(CostAvg × Quantity, 0)（3.39000774593339×7=23.73→24）、HasCheck=NeedUpdate=IsOdd=True、稅 0。
comBillAccounts（Flag 501、SysID 4、CustFlag 1）：BillDate / PrepayDay / EncashDay=發票日、AccMonth=發票日 yyyymm、
  CustID、DueTo（S/C.DueTo，空則 CustID）、CurrID=S/C、ExchRate=發票匯率、Total=Σ Amount、
  LocalTotal=Σ comProdRec.MLAmount（ST-2026288：Σ=2083989，而 round(64690×32.215)=2083988 與實單不符；
  ST-2026285 740006、ST-2026292 22458 兩種算法相同）、稅 0、OffSet 0（建立時未收款）、
  DeptID / Salesman 抄第一張 S/C（DeptID / SalesID）、AddrID=S/C.Addr 或 InvoAddr、CustAddress=S/C.InvAddress、
  ContactPerson=S/C.Attn、Status=1、HasCheck=True、PriceOfTax=True、CostFlag=501、InvoStyle=1、
  Maker / MakerID=ERP_PO_MAKER / ERP_PO_MAKER_ID、VoucherNO=CVoucherNO=''。
S/C 回寫（Flag 2）：TraBillSub.QtyRemain 與 NotTranInvQty 各減開立數量（允許變負：公司3 MO-2608013 #2 2500 開 2575 → -75）；
  NotTranPackingQty、TransedQty、InvNO / InvFromNo 不動。TraBillMain：全部列 QtyRemain<=0 → BillStatus=1（Endacasedate=今天，
  公司3實單 = 開單當天）；還有列 >0 且原本 BillStatus=1 → 改回 0（Endacasedate=0）；其他狀態不動。
庫存（出庫）：comWareAmount.Quantity / BCQty 減、LatestSalesDate=發票日；StkYearMonthQty 當月減；
  stkYearMonthAvg.AtLastQuan 當月減（CostByWare=0 WareID=''、=1 以倉庫為鍵）；comProduct.LatestSalesDate=發票日。
  LatestOutDate：公司3三張實單的 comWareAmount / comProduct.LatestOutDate 都是 0（只有 LatestSalesDate 被改），
  所以預設不動（SET_LATEST_OUT_DATE 改 True 才會寫）。
  update_cost（預設 ERP_INVOICE_UPDATE_COST=1）：comWareAmount.TotalCost 與 comProduct.CTotalCost 減 CostForAcc
  （數量歸 0 時總成本歸 0，與公司3實單相符）；CostByWare=1 時 Cost = 新 TotalCost / 新 Quantity（數量 <= 0 則不動）。
  出庫不重算 CAvgCost。
倉庫：ERP_INVOICE_WAREHOUSE（預設 001，公司3實單全是 001）。預設倉不夠、別倉較多時改用庫存最多的倉並在預覽顯示。
  庫存不足、超開 S/C 剩餘量：預覽警告，沒帶 confirm 不寫入。料號在 comProduct 不存在（例如公司13的 ItemNo='*'）直接報錯。
匯率：預設 comCurrencySys.BuyInRate（公司3 USD 30.385），預覽可改（請求模型一定要收 exch_rate），顯示 S/C 原匯率當參考。
主檔：測試公司缺客戶（comCustomer Flag=1）/ 料號時從來源公司（ERP_SQL_DATABASE）照抄；正式公司禁止自動抄主檔。
錯誤訊息一律不含帳號密碼。
"""
from __future__ import annotations

import hashlib
import json
import logging
import socket
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from . import erp_invoice_schema as schema
from . import erp_purchase as ep
from . import erp_receive as er
from . import erp_stock
from . import po_duplicates
from .erp_stock import ErpError, ErpNotConfigured, ErpUnavailable
from .local_time import local_now

logger = logging.getLogger(__name__)

INVOICE_FLAG = 501
SC_FLAG = 2
CUSTOMER_FLAG = 1  # comCustomer：1 = 客戶、2 = 廠商
ACCOUNT_SYS_ID = 4  # comBillAccounts.SysID：4 = 出口
TRAN_TYPE = 404  # comProdRec.TranType：S/C → Invoice
PROG_ID = "ChiTrade.Invoice"
BILL_PREFIX = "ST-"
MAX_LINES = 100
DEFAULT_WAREHOUSE = "001"
DEFAULT_TARGET = "CHIComp13"
ML_PRICE_DIGITS = 3  # comProdRec.MLPrice：1.56×32.215=50.255、0.27×32.215=8.698、1.7×32.215=54.766
DEFAULT_AMOUNT_DIGITS = 3  # comCurrencySys 沒有 AmountDeciDigits 時（同一次收料）
IDEMPOTENCY_SECONDS = 60
MAX_NUMBER_ATTEMPTS = er.MAX_NUMBER_ATTEMPTS
SET_LATEST_OUT_DATE = False  # 公司3實單 LatestOutDate 維持 0，見模組說明
_HINT = er._HINT

TARGET_ENV = "ERP_INVOICE_TARGET"
WAREHOUSE_ENV = "ERP_INVOICE_WAREHOUSE"
UPDATE_COST_ENV = "ERP_INVOICE_UPDATE_COST"
PROD_INVOICE_FLAG_ENV = "ERP_ALLOW_PROD_INVOICE_CHICOMP03"
PROD_WRITE_FLAG_ENV = ep.PROD_WRITE_FLAG_ENV  # ERP_ALLOW_PROD_WRITE_CHICOMP03


class InvoiceValidationError(ep.PoValidationError):
    """輸入或 ERP 資料不符規則（HTTP 400）。"""


class InvoiceConfirmWarning(InvoiceValidationError):
    """超開 S/C 剩餘量或庫存不足，未帶 confirm=true 時不寫入並回報。"""

    def __init__(self, items: List[Dict[str, Any]]):
        super().__init__("部分明細超開 S/C 剩餘量或庫存不足，請確認後再送出")
        self.items = items


class NumberCollision(Exception):
    """選好的單號在寫入前已存在、或寫入時遇到重複索引：rollback 後重選單號重試。"""


# ── 設定 / 目標 ───────────────────────────────────────────────────────────────

def warehouse() -> str:
    return ep._env(WAREHOUSE_ENV, DEFAULT_WAREHOUSE) or DEFAULT_WAREHOUSE


def update_cost_default() -> bool:
    """ERP_INVOICE_UPDATE_COST：預設 1（出庫扣總成本）；0 / false / no / off = 不動成本。"""
    return er._env_flag(UPDATE_COST_ENV, True)


def get_invoice_target() -> str:
    raw = ep._env(TARGET_ENV, DEFAULT_TARGET) or DEFAULT_TARGET
    if not ep.TARGET_RE.match(raw):
        raise ErpNotConfigured(f"{TARGET_ENV} 格式不正確（需為 CHIComp 加兩位數字，例如 CHIComp13）")
    return raw


def production_write_allowed(target: str) -> bool:
    """正式公司三把鑰匙：ERP_INVOICE_TARGET=CHIComp03、ERP_ALLOW_PROD_INVOICE_CHICOMP03=1、ERP_ALLOW_PROD_WRITE_CHICOMP03=1。"""
    return (
        target == ep.PRODUCTION_TARGET
        and ep._env(TARGET_ENV, "") == ep.PRODUCTION_TARGET
        and ep._env(PROD_INVOICE_FLAG_ENV, "") == "1"
        and ep._env(PROD_WRITE_FLAG_ENV, "") == "1"
    )


def ensure_writable_target(target: str) -> None:
    if target == ep.PRODUCTION_TARGET:
        if not production_write_allowed(target):
            raise InvoiceValidationError(
                f"{ep.target_label(target)} 未開放開 Invoice（需設定 {TARGET_ENV}=CHIComp03、"
                f"{PROD_INVOICE_FLAG_ENV}=1 與 {PROD_WRITE_FLAG_ENV}=1）")
        return
    if not ep.is_test(target):
        raise InvoiceValidationError(f"{target} 不在測試公司清單（ERP_TEST_TARGETS），拒絕寫入")


def resolve_target(requested: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    target = ep._text(requested) or get_invoice_target()
    if not ep.TARGET_RE.match(target):
        raise InvoiceValidationError("目標公司格式不正確")
    ensure_writable_target(target)
    creds = ep.write_credentials(target)
    if creds is None:
        raise ErpNotConfigured(ep.UNAVAILABLE_REASON)
    return target, creds


def get_config() -> Dict[str, Any]:
    """前端設定；絕不含帳號密碼。"""
    result: Dict[str, Any] = {
        "target": "", "label": "", "is_test": False, "available": False, "reason": "",
        "warehouse": warehouse(), "today": er.date_to_int(local_now().date()),
        "maker": ep._env("ERP_PO_MAKER", ep.DEFAULT_MAKER), "is_production": False,
        "update_cost_default": update_cost_default(),
    }
    try:
        target = get_invoice_target()
    except ErpError as exc:
        result["reason"] = str(exc)
        return result
    result.update(target=target, label=ep.target_label(target), is_test=ep.is_test(target),
                  is_production=target == ep.PRODUCTION_TARGET)
    try:
        ensure_writable_target(target)
    except ErpError as exc:
        result["reason"] = str(exc)
        return result
    if ep.write_credentials(target) is None:
        result["reason"] = ep.UNAVAILABLE_REASON
    else:
        result["available"] = True
    return result


# ── 純函式：金額 / 日期 / 單號 ───────────────────────────────────────────────

_dec = er._dec


def calc_line_money(qty: Any, price: Any, rate: Any, amount_digits: int) -> Dict[str, Decimal]:
    """comProdRec 一列的金額：Amount / MLPrice / MLAmount（MLAmount = round(qty × price × 匯率, 0) 一次算）。"""
    q, p, r = _dec(qty), _dec(price), _dec(rate)
    return {
        "amount": ep._round(q * p, amount_digits),
        "ml_price": ep._round(p * r, ML_PRICE_DIGITS),
        "ml_amount": ep._round(q * p * r, 0),
    }


def calc_cost_for_acc(cost_avg: Any, qty: Any) -> Decimal:
    """CostForAcc = round(CostAvg × Quantity, 0)。"""
    return ep._round(_dec(cost_avg) * _dec(qty), 0)


def calc_local_total(ml_amounts: List[Any]) -> Decimal:
    """comBillAccounts.LocalTotal = Σ comProdRec.MLAmount（不是 round(Total × 匯率)：ST-2026288 實單 2083989 ≠ 2083988）。"""
    return sum((_dec(v) for v in ml_amounts), Decimal(0))


def parse_inv_date(value: Any, today: int) -> int:
    """發票日：空值 = 今天；否則需為真實日期（yyyymmdd 整數或 YYYY-MM-DD 字串）。"""
    try:
        return er.parse_rm_date(value, today)
    except er.ReceiveValidationError as exc:
        raise InvoiceValidationError(str(exc).replace("進貨日期", "發票日期")) from None


def make_bill_no(year: int, serial: int) -> str:
    return f"{BILL_PREFIX}{int(year)}{int(serial):03d}"


def next_invoice_serial(cur, year: int, lock: bool) -> int:
    """ST-{year} 的最大流水 + 1（TraBillMain Flag 501 與 comBillAccounts Flag 501 都看）。"""
    hint = _HINT if lock else ""
    head = f"{BILL_PREFIX}{int(year)}"
    top = 0
    for table, col, cond in (("TraBillMain", "BillNo", f"Flag = {INVOICE_FLAG}"),
                             ("comBillAccounts", "FundBillNo", f"Flag = {INVOICE_FLAG}")):
        cur.execute(f"SELECT MAX({col}) FROM {table}{hint} WHERE {cond} AND {col} LIKE %s AND LEN({col}) = %s",
                    (head + "%", len(head) + 3))
        found = cur.fetchone()
        last = ep._text(found[0]) if found else ""
        if last:
            try:
                top = max(top, int(last[len(head):]))
            except ValueError:
                raise ep.PoSchemaError(f"既有 Invoice 單號格式異常：{last}") from None
    if top + 1 > 999:
        raise InvoiceValidationError(f"{year} 年的 Invoice 單號已用完（最多 999 張）")
    return top + 1


def existing_numbers(cur, bill_no: str, lock: bool = True) -> List[str]:
    """寫入前確認單號在正航都還不存在；回傳已存在的表（空 = 可以寫）。"""
    hint = _HINT if lock else ""
    found: List[str] = []
    for table, col in (("TraBillMain", "BillNo"), ("TraBillSub", "BillNo"), ("comProdRec", "BillNO"),
                       ("comBillAccounts", "FundBillNo")):
        cur.execute(f"SELECT 1 FROM {table}{hint} WHERE Flag = {INVOICE_FLAG} AND {col} = %s", (bill_no,))
        if cur.fetchone():
            found.append(table)
    return found


# ── 輸入驗證 ──────────────────────────────────────────────────────────────────

def normalize_request(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw_lines = raw.get("lines") or []
    if not 1 <= len(raw_lines) <= MAX_LINES:
        raise InvoiceValidationError(f"明細需為 1～{MAX_LINES} 行")
    lines: List[Dict[str, Any]] = []
    seen = set()
    for i, r in enumerate(raw_lines, 1):
        sc_no = ep._text(r.get("sc_no"))
        if not sc_no:
            raise InvoiceValidationError(f"第 {i} 行：缺少 S/C 單號")
        try:
            row_no = int(r.get("row_no"))
        except (TypeError, ValueError):
            raise InvoiceValidationError(f"第 {i} 行：S/C 列號不正確") from None
        if (sc_no, row_no) in seen:
            raise InvoiceValidationError(f"第 {i} 行：{sc_no} 第 {row_no} 列重複")
        seen.add((sc_no, row_no))
        qty = ep._to_decimal(r.get("qty"), f"第 {i} 行數量")
        if qty <= 0:
            raise InvoiceValidationError(f"第 {i} 行：數量必須大於 0")
        price = None
        if r.get("price") is not None:
            price = ep._to_decimal(r.get("price"), f"第 {i} 行單價")
            if price < 0:
                raise InvoiceValidationError(f"第 {i} 行：單價不可為負")
        lines.append({"sc_no": sc_no, "row_no": row_no, "qty": qty, "price": price})
    exch_rate = None
    if raw.get("exch_rate") not in (None, ""):
        exch_rate = ep._to_decimal(raw.get("exch_rate"), "匯率")
        if exch_rate <= 0:
            raise InvoiceValidationError("匯率必須大於 0")
    return {
        "target": ep._text(raw.get("target")),
        "inv_date_raw": raw.get("inv_date"),
        "lines": lines,
        "exch_rate": exch_rate,
        "confirm": raw.get("confirm") is True,
        "client_token": ep._text(raw.get("client_token"))[:80],
        "update_cost": update_cost_default() if raw.get("update_cost") is None else raw.get("update_cost") is True,
    }


# ── 讀取：可開立的 S/C 明細 ──────────────────────────────────────────────────

def list_open_lines(
    customer: Optional[str] = None, q: Optional[str] = None, limit: int = 300, target: Optional[str] = None,
) -> Dict[str, Any]:
    """寫入公司的 S/C（TraBillSub Flag 2）未完成量 QtyRemain > 0 的明細（唯讀）。"""
    target, creds = resolve_target(target)
    limit = max(1, min(int(limit), 1000))
    where = [f"s.Flag = {SC_FLAG}", "s.QtyRemain > 0"]
    params: List[Any] = []
    if ep._text(customer):
        where.append("m.CustID = %s")
        params.append(ep._text(customer))
    if ep._text(q):
        pat = f"%{ep._like_escape(ep._text(q))}%"
        where.append("(m.CustID LIKE %s OR c.ShortName LIKE %s OR c.FullName LIKE %s OR s.ItemNo LIKE %s "
                     "OR s.Description LIKE %s OR m.BillNo LIKE %s)")
        params += [pat] * 6
    sql = (
        f"SELECT TOP {limit + 1} m.BillNo, s.RowNo, m.BillDate, m.CustID, c.ShortName, c.FullName, s.ItemNo, "
        "s.Description, s.Quantity, s.QtyRemain, s.Price, m.CurrID, m.ExchRate, s.Unit "
        "FROM TraBillSub s JOIN TraBillMain m ON m.Flag = s.Flag AND m.BillNo = s.BillNo "
        f"LEFT JOIN comCustomer c ON c.Flag = {CUSTOMER_FLAG} AND c.ID = m.CustID "
        f"WHERE {' AND '.join(where)} ORDER BY m.CustID, m.BillDate, m.BillNo, s.RowNo"
    )
    wconn = ep._connect_write(creds)
    try:
        cur = wconn.cursor()
        ep._check_db_name(cur, target)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    except ErpError:
        raise
    except Exception as exc:
        raise ErpUnavailable(f"ERP 查詢失敗：{ep._safe_message(exc, creds)}") from None
    finally:
        ep._close_quietly(wconn, rollback=True)
    return {"target": target, "target_label": ep.target_label(target),
            "lines": [open_line(r) for r in rows[:limit]], "truncated": len(rows) > limit}


def open_line(r: Tuple[Any, ...]) -> Dict[str, Any]:
    bd = int(r[2] or 0)
    return {
        "sc_no": ep._text(r[0]), "row_no": int(r[1]), "date": bd,
        "date_text": f"{bd // 10000:04d}-{bd // 100 % 100:02d}-{bd % 100:02d}" if bd > 10000000 else "",
        "customer_id": ep._text(r[3]), "customer_name": ep._text(r[4]) or ep._text(r[5]),
        "part_number": ep._text(r[6]), "part_name": ep._text(r[7]),
        "qty": float(r[8] or 0), "qty_remain": float(r[9] or 0), "price": float(r[10] or 0),
        "currency": ep._text(r[11]), "sc_exch_rate": float(r[12] or 0), "unit": ep._text(r[13]),
    }


# ── 讀取：規劃用的資料 ────────────────────────────────────────────────────────

_one_row = er._one_row


def _invalid_part(prod: str) -> bool:
    return not prod or prod.strip("*") == ""


def _load_masters(wcur, target: str, cust_id: str, prods: List[str], hint: str) -> Dict[str, Any]:
    """客戶 / 料號主檔：目標有就用；測試公司缺的從來源公司讀（寫入時照抄）；正式公司缺就報錯。"""
    customer = _one_row(wcur, f"SELECT * FROM comCustomer WHERE Flag = {CUSTOMER_FLAG} AND ID = %s", (cust_id,))
    products: Dict[str, Dict[str, Any]] = {}
    missing_prods: List[str] = []
    for prod in prods:
        if _invalid_part(prod):
            raise InvoiceValidationError(f"S/C 料號「{prod or '（空白）'}」不是有效的料號主檔，不能開 Invoice")
        row = _one_row(wcur, f"SELECT * FROM comProduct{hint} WHERE ProdID = %s", (prod,))
        if row is None:
            missing_prods.append(prod)
        else:
            products[prod] = row
    missing: List[Dict[str, Any]] = []
    if customer is not None and not missing_prods:
        return {"customer": customer, "products": products, "missing": missing}
    source_db = erp_stock.get_settings()["database"]
    label = ep.target_label(target)
    if not ep._can_copy_masters(target, source_db):
        what = ([f"客戶 {cust_id}"] if customer is None else []) + [f"料號 {p}" for p in missing_prods]
        raise InvoiceValidationError(f"{label} 找不到{'、'.join(what)}（正式公司不會自動複製主檔），不能開 Invoice")
    with ep._source_session() as src:
        scur = src.cursor()
        if customer is None:
            customer = _one_row(scur, f"SELECT * FROM comCustomer WHERE Flag = {CUSTOMER_FLAG} AND ID = %s", (cust_id,))
            if customer is None:
                raise InvoiceValidationError(f"{label} 與來源公司都找不到客戶 {cust_id}")
            missing.append({"table": "comCustomer", "label": "客戶", "key": cust_id,
                            "where": f"Flag = {CUSTOMER_FLAG} AND ID = %s", "params": (cust_id,), "required": True})
        for prod in missing_prods:
            row = _one_row(scur, "SELECT * FROM comProduct WHERE ProdID = %s", (prod,))
            if row is None:
                raise InvoiceValidationError(f"{label} 與來源公司都找不到料號 {prod}，不能開 Invoice")
            products[prod] = row
            missing.append({"table": "comProduct", "label": "料號", "key": prod, "where": "ProdID = %s",
                            "params": (prod,), "required": True})
    return {"customer": customer, "products": products, "missing": missing}


def choose_warehouse(default: str, need: Decimal, stocks: Dict[str, Decimal]) -> Tuple[str, bool]:
    """出貨倉：預設倉夠就用預設倉；否則若別倉庫存比預設倉多，改用庫存最多的倉（同量取倉號小的）。回傳 (倉, 是否改倉)。"""
    have = stocks.get(default, Decimal(0))
    if have >= need:
        return default, False
    others = sorted(((q, w) for w, q in stocks.items() if w != default and q > have), key=lambda x: (-x[0], x[1]))
    if others:
        return others[0][1], True
    return default, False


def _load_context(wcur, target: str, req: Dict[str, Any], lock: bool) -> Dict[str, Any]:
    hint = _HINT if lock else ""
    cost_params = er.load_cost_params(wcur, target)
    scs: Dict[str, Dict[str, Any]] = {}
    for sc_no in dict.fromkeys(l["sc_no"] for l in req["lines"]):
        main = _one_row(wcur, f"SELECT * FROM TraBillMain{hint} WHERE Flag = {SC_FLAG} AND BillNo = %s", (sc_no,))
        if main is None:
            raise InvoiceValidationError(f"找不到 S/C {sc_no}")
        wcur.execute(f"SELECT * FROM TraBillSub{hint} WHERE Flag = {SC_FLAG} AND BillNo = %s ORDER BY RowNo", (sc_no,))
        subs = {int(ep._get(r, "RowNo")): r for r in ep._rows(wcur)}
        scs[sc_no] = {"main": main, "subs": subs}

    picked: List[Dict[str, Any]] = []
    for i, l in enumerate(req["lines"], 1):
        sub = scs[l["sc_no"]]["subs"].get(l["row_no"])
        if sub is None:
            raise InvoiceValidationError(f"第 {i} 行：{l['sc_no']} 沒有第 {l['row_no']} 列")
        picked.append({**l, "main": scs[l["sc_no"]]["main"], "sub": sub})

    custs = {ep._text(ep._get(s["main"], "CustID")) for s in scs.values()}
    if len(custs) != 1:
        raise InvoiceValidationError("一張 Invoice 只能選同一個客戶的 S/C 明細：" + "、".join(sorted(custs)))
    currs = {ep._text(ep._get(s["main"], "CurrID")) for s in scs.values()}
    if len(currs) != 1:
        raise InvoiceValidationError("一張 Invoice 的 S/C 幣別必須相同（" + "、".join(sorted(c or "NTD" for c in currs)) + "）")
    cust_id = custs.pop()
    curr = currs.pop()
    if not cust_id:
        raise InvoiceValidationError("S/C 沒有客戶代號，不能開 Invoice")

    prods = list(dict.fromkeys(ep._text(ep._get(p["sub"], "ItemNo")) for p in picked))
    masters = _load_masters(wcur, target, cust_id, prods, hint)
    currency = er.load_currency(wcur, curr) if curr else None

    default_ware = warehouse()
    need: Dict[str, Decimal] = {}
    for p in picked:
        prod = ep._text(ep._get(p["sub"], "ItemNo"))
        need[prod] = need.get(prod, Decimal(0)) + p["qty"]
    wares: Dict[str, str] = {}
    ware_switched: Dict[str, str] = {}
    ware_amounts: Dict[str, Optional[Dict[str, Any]]] = {}
    stocks: Dict[str, Dict[str, Decimal]] = {}
    all_qty: Dict[str, Decimal] = {}
    for prod in prods:
        wcur.execute(f"SELECT * FROM comWareAmount{hint} WHERE ProdID = %s", (prod,))
        rows = {ep._text(ep._get(r, "WareID")): r for r in ep._rows(wcur)}
        stocks[prod] = {w: _dec(ep._get(r, "Quantity")) for w, r in rows.items()}
        all_qty[prod] = sum(stocks[prod].values(), Decimal(0))
        ware, switched = choose_warehouse(default_ware, need[prod], stocks[prod])
        if switched:
            ware_switched[prod] = ware
        wares[prod] = ware
        ware_amounts[prod] = rows.get(ware)
    for ware in dict.fromkeys(wares.values()):
        if _one_row(wcur, "SELECT 1 FROM comWareHouse WHERE WareHouseID = %s", (ware,)) is None:
            raise InvoiceValidationError(f"{ep.target_label(target)} 找不到倉庫 {ware}（{WAREHOUSE_ENV}）")
    return {
        "scs": scs, "picked": picked, "cust_id": cust_id, "curr": curr, "currency_row": currency,
        "customer": masters["customer"], "products": masters["products"], "missing_masters": masters["missing"],
        "default_ware": default_ware, "wares": wares, "ware_switched": ware_switched, "ware_amounts": ware_amounts,
        "stocks": stocks, "all_qty": all_qty, "cost_params": cost_params, "req_exch_rate": req.get("exch_rate"),
        "hint": hint,
    }


def _load_rollup_state(wcur, ctx: Dict[str, Any], ym: int) -> None:
    hint = ctx["hint"]
    by_ware = ctx["cost_params"]["cost_by_ware"]
    ctx["ym_qty"], ctx["ym_avg"] = {}, {}
    for prod, ware in ctx["wares"].items():
        ctx["ym_qty"][prod] = _one_row(
            wcur, f"SELECT * FROM StkYearMonthQty{hint} WHERE YearMonth = %s AND ProdID = %s AND WareID = %s",
            (ym, prod, ware))
        ctx["ym_avg"][prod] = _one_row(
            wcur, f"SELECT * FROM stkYearMonthAvg{hint} WHERE YearMonth = %s AND ProdID = %s AND WareID = %s AND BatchID = %s",
            (ym, prod, ware if by_ware else "", ""))


# ── 規劃（純函式）────────────────────────────────────────────────────────────

def _row(table: str, **values: Any) -> Dict[str, Any]:
    """該表全部欄位的預設值 + 覆寫；覆寫了表上不存在的欄位就中止（防打錯字）。"""
    row = schema.defaults(table)
    names = {k.lower(): k for k in row}
    for key, value in values.items():
        exact = names.get(key.lower())
        if exact is None:
            raise ep.PoSchemaError(f"{table} 沒有欄位 {key}")
        row[exact] = value
    return row


def cost_avg_for(product: Dict[str, Any], ware_amount: Optional[Dict[str, Any]], cost_by_ware: bool) -> float:
    """出貨平均成本：CostByWare=0 用 comProduct.CAvgCost；=1 用該倉 comWareAmount.Cost，沒有（或 <= 0）就退回 CAvgCost。"""
    if cost_by_ware and ware_amount is not None:
        cost = _dec(ep._get(ware_amount, "Cost"))
        if cost > 0:
            return float(cost)
    return float(_dec(ep._get(product, "CAvgCost")))


def sc_status_updates(scs: Dict[str, Dict[str, Any]], issued: Dict[Tuple[str, int], Decimal], today: int,
                      ) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    """S/C 回寫：QtyRemain / NotTranInvQty 各減開立數量（可負）；BillStatus 依剩餘量切換。回傳 (updates, 結案, 重開)。"""
    updates: List[Dict[str, Any]] = []
    closed: List[str] = []
    reopened: List[str] = []
    for sc_no, sc in scs.items():
        any_open = False
        for row_no, sub in sc["subs"].items():
            remain = _dec(ep._get(sub, "QtyRemain"))
            if (sc_no, row_no) in issued:
                q = issued[(sc_no, row_no)]
                remain -= q
                updates.append({"op": "update", "table": "TraBillSub",
                                "set": {"QtyRemain": remain, "NotTranInvQty": _dec(ep._get(sub, "NotTranInvQty")) - q},
                                "where": {"Flag": SC_FLAG, "BillNo": sc_no, "RowNo": row_no}})
            if remain > 0:
                any_open = True
        status = int(ep._get(sc["main"], "BillStatus", 0) or 0)
        where = {"Flag": SC_FLAG, "BillNo": sc_no}
        if not any_open and status != 1:
            closed.append(sc_no)
            updates.append({"op": "update", "table": "TraBillMain", "set": {"BillStatus": 1, "Endacasedate": today},
                            "where": where})
        elif any_open and status == 1:
            reopened.append(sc_no)
            updates.append({"op": "update", "table": "TraBillMain", "set": {"BillStatus": 0, "Endacasedate": 0},
                            "where": where})
    return updates, closed, reopened


def build_plan(
    ctx: Dict[str, Any], *, target: str, today: int, inv_date: int, bill_no: str, maker: str, maker_id: str,
    client_ip: str, actor: str, now: datetime, update_cost: bool = True,
) -> Dict[str, Any]:
    picked = ctx["picked"]
    first = picked[0]["main"]  # 使用者勾選順序的第一張 S/C
    first_no = picked[0]["sc_no"]
    cust_id, curr = ctx["cust_id"], ctx["curr"]
    params = ctx["cost_params"]
    cur_row = ctx.get("currency_row") or {}
    local = er.is_local_currency(curr)
    sc_rate = _dec(ep._get(first, "ExchRate", 0))
    sys_rate = _dec(ep._get(cur_row, "BuyInRate", 0)) or _dec(ep._get(cur_row, "FixedRate", 0))
    if ctx.get("req_exch_rate"):
        rate, rate_source = _dec(ctx["req_exch_rate"]), "manual"
    elif local:
        rate, rate_source = Decimal(1), "local"
    elif sys_rate > 0:
        rate, rate_source = sys_rate, "currency"
    else:
        rate, rate_source = Decimal(0), "missing"
    digits_raw = ep._get(cur_row, "AmountDeciDigits", None) if ctx.get("currency_row") is not None else None
    amount_digits = int(digits_raw) if digits_raw is not None else (0 if local else DEFAULT_AMOUNT_DIGITS)

    warnings: List[Dict[str, Any]] = []
    confirm_items: List[Dict[str, Any]] = []
    lines: List[Dict[str, Any]] = []
    for i, p in enumerate(picked, 1):
        sub = p["sub"]
        prod = ep._text(ep._get(sub, "ItemNo"))
        product = ctx["products"][prod]
        ware = ctx["wares"][prod]
        qty = p["qty"]
        sc_price = _dec(ep._get(sub, "Price"))
        price = p["price"] if p["price"] is not None else sc_price
        money = calc_line_money(qty, price, rate, amount_digits)
        cost = cost_avg_for(product, ctx["ware_amounts"].get(prod), params["cost_by_ware"])
        remain = _dec(ep._get(sub, "QtyRemain"))
        line = {
            "row_no": i, "sc_no": p["sc_no"], "sc_row": p["row_no"], "prod_id": prod,
            "prod_name": ep._text(ep._get(sub, "Description")) or ep._text(ep._get(product, "ProdName")),
            "qty": qty, "qty_remain": remain, "price": price, "sc_price": sc_price, **money,
            "cost_avg": cost, "cost_for_acc": calc_cost_for_acc(cost, qty),
            "warehouse": ware, "stock": ctx["stocks"][prod].get(ware, Decimal(0)),
        }
        lines.append(line)
        if qty > remain:
            confirm_items.append({"kind": "over_remain", "line_no": i, "sc_no": p["sc_no"], "row_no": p["row_no"],
                                  "part_number": prod, "qty": float(qty), "qty_remain": float(remain),
                                  "over": float(qty - remain)})
    over = [c for c in confirm_items if c["kind"] == "over_remain"]
    if over:
        warnings.append({"code": "over_remain", "needs_confirm": True, "items": over,
                         "message": "開立數量超過 S/C 未完成量（剩餘量會變負，公司3實單也有超開）：" + "、".join(
                             f"{o['sc_no']}#{o['row_no']} {o['part_number']} 超開 {o['over']:g}" for o in over)})

    by_prod: Dict[str, Dict[str, Decimal]] = {}
    for l in lines:
        agg = by_prod.setdefault(l["prod_id"], {"qty": Decimal(0), "cost": Decimal(0)})
        agg["qty"] += l["qty"]
        agg["cost"] += l["cost_for_acc"]
    short = []
    for prod, agg in by_prod.items():
        ware = ctx["wares"][prod]
        have = ctx["stocks"][prod].get(ware, Decimal(0))
        if agg["qty"] > have:
            short.append({"kind": "stock_short", "part_number": prod, "warehouse": ware, "qty": float(agg["qty"]),
                          "stock": float(have), "short": float(agg["qty"] - have)})
    if short:
        confirm_items += short
        warnings.append({"code": "stock_short", "needs_confirm": True, "items": short,
                         "message": "庫存不足（寫入後庫存會變負）：" + "、".join(
                             f"{s['part_number']} 倉 {s['warehouse']} 現有 {s['stock']:g}、需 {s['qty']:g}" for s in short)})
    for prod, ware in ctx["ware_switched"].items():
        warnings.append({"code": "warehouse_switched", "needs_confirm": False,
                         "message": f"{prod} 預設倉 {ctx['default_ware']} 庫存不夠，改由倉 {ware} 出貨"
                                    f"（現有 {ctx['stocks'][prod].get(ware, Decimal(0)):g}）"})
    diff = [l for l in lines if l["price"] != l["sc_price"]]
    if diff:
        warnings.append({"code": "price_diff", "needs_confirm": False, "message": "單價與 S/C 不同：" + "、".join(
            f"{l['sc_no']}#{l['sc_row']} {l['sc_price']:g}→{l['price']:g}" for l in diff)})
    if rate_source == "missing":
        warnings.append({"code": "rate_required", "needs_confirm": False,
                         "message": f"正航幣別檔沒有 {curr} 匯率：請輸入本次發票匯率後才能寫入"})
    for m in ctx["missing_masters"]:
        warnings.append({"code": "copy_master", "needs_confirm": False,
                         "message": f"寫入公司缺{m['label']} {m['key']}，寫入時會從來源公司照抄主檔"})
    if not update_cost:
        warnings.append({"code": "cost_not_updated", "needs_confirm": False,
                         "message": "本次不扣總成本（CTotalCost / 倉庫 TotalCost / Cost 不動，庫存量照常扣）"})

    sum_qty = sum((l["qty"] for l in lines), Decimal(0))
    total = sum((l["amount"] for l in lines), Decimal(0))
    totals = {
        "sum_qty": sum_qty, "total": total, "local_total": calc_local_total([l["ml_amount"] for l in lines]),
        "sum_ml_amount": sum((l["ml_amount"] for l in lines), Decimal(0)),
        "sum_cost": sum((l["cost_for_acc"] for l in lines), Decimal(0)),
    }

    inserts: List[Tuple[str, Dict[str, Any]]] = []
    inserts.append(("TraBillMain", _row(
        "TraBillMain", Flag=INVOICE_FLAG, BillNo=bill_no, IsFormal=True, OriginType="S/C", OriginNo=first_no,
        MulOriSCFlag=False, IsDirectExport=bool(ep._get(first, "IsDirectExport", False)), HasCheck=True,
        InvoiceType=int(ep._get(first, "InvoiceType", 0) or 0),
        InvoAddr=ep._text(ep._get(first, "InvoAddr")) or ep._text(ep._get(first, "Addr")),
        InvAddress=ep._text(ep._get(first, "InvAddress")), NWUnit=ep._text(ep._get(first, "NWUnit")),
        GWUnit=ep._text(ep._get(first, "GWUnit")), VolumeUnit=ep._text(ep._get(first, "VolumeUnit")),
        Payment=ep._text(ep._get(first, "Payment")), SumQty=sum_qty, BillStatus=0, InvoStyle=0)))
    for l in lines:
        inserts.append(("TraBillSub", _row(
            "TraBillSub", Flag=INVOICE_FLAG, BillNo=bill_no, RowNo=l["row_no"], SerNo=l["row_no"], BillDate=inv_date,
            ItemNo=l["prod_id"], Description=l["prod_name"], Quantity=1, Price=0, Amount=0, QtyRemain=1, QtyCtn=1,
            OriginBillNo=l["sc_no"], OriginRowNo=l["sc_row"], TranType=0, FromRow=0, HasCheck=True, IsOdd=False,
            DeclareQty=l["qty"], LegalQty=l["qty"], LegalQtyB=l["qty"], NotTranPackingQty=0, NotTranInvQty=0)))
    for l in lines:
        inserts.append(("comProdRec", _row(
            "comProdRec", Flag=INVOICE_FLAG, BillNO=bill_no, RowNO=l["row_no"], SerNO=l["row_no"], BillDate=inv_date,
            ProdID=l["prod_id"], ProdName=l["prod_name"], CostAvg=l["cost_avg"], Quantity=l["qty"],
            MLPrice=l["ml_price"], MLAmount=l["ml_amount"], WareID=l["warehouse"], QtyRemain=l["qty"],
            TranType=TRAN_TYPE, FromNO=l["sc_no"], FromRow=l["sc_row"], TaxRate=0, TaxAmt=0, NeedUpdate=True,
            CostForAcc=l["cost_for_acc"], HasCheck=True, Price=l["price"], Amount=l["amount"], SPrice=l["price"],
            SQuantity=l["qty"], UnitRelation=1, EQuantity=l["qty"], EUnitRelation=1, MLTaxAmt=0, IsOdd=True)))
    inserts.append(("comBillAccounts", _row(
        "comBillAccounts", Flag=INVOICE_FLAG, SysID=ACCOUNT_SYS_ID, FundBillNo=bill_no, BillDate=inv_date,
        CustFlag=CUSTOMER_FLAG, CustID=cust_id, DueTo=ep._text(ep._get(first, "DueTo")) or cust_id,
        AccMonth=inv_date // 100, CurrID=curr, ExchRate=rate, TaxType=0, Total=total, Tax=0,
        LocalTotal=totals["local_total"], LocalTax=0, OffSet=0, LocalOffSet=0,
        PrepayDay=inv_date, EncashDay=inv_date, DeptID=ep._text(ep._get(first, "DeptID")), Status=1, InvoStyle=1,
        Salesman=ep._text(ep._get(first, "SalesID")), HasCheck=True, CostFlag=INVOICE_FLAG, PriceOfTax=True,
        AddrID=ep._text(ep._get(first, "Addr")) or ep._text(ep._get(first, "InvoAddr")),
        CustAddress=ep._text(ep._get(first, "InvAddress")), ContactPerson=ep._text(ep._get(first, "Attn")),
        MakerID=maker_id, Maker=maker, VoucherNO="", CVoucherNO="")))

    issued: Dict[Tuple[str, int], Decimal] = {}
    for l in lines:
        issued[(l["sc_no"], l["sc_row"])] = issued.get((l["sc_no"], l["sc_row"]), Decimal(0)) + l["qty"]
    updates, closed_scs, reopened_scs = sc_status_updates(ctx["scs"], issued, today)

    ym = inv_date // 100
    by_ware = params["cost_by_ware"]
    dates: Dict[str, Any] = {"LatestSalesDate": inv_date}
    if SET_LATEST_OUT_DATE:
        dates["LatestOutDate"] = inv_date
    for prod, agg in by_prod.items():
        q, cost = agg["qty"], agg["cost"]
        ware = ctx["wares"][prod]
        wa = ctx["ware_amounts"].get(prod)
        new_q = (_dec(ep._get(wa, "Quantity")) if wa is not None else Decimal(0)) - q
        if wa is None:
            updates.append({"op": "insert", "table": "comWareAmount", "values": _row(
                "comWareAmount", ProdID=prod, WareID=ware, Quantity=new_q, BCQty=new_q, **dates)})
        else:
            ware_set: Dict[str, Any] = {"Quantity": new_q, "BCQty": new_q, **dates}
            if update_cost:
                new_total = Decimal(0) if new_q == 0 else _dec(ep._get(wa, "TotalCost")) - cost
                ware_set["TotalCost"] = new_total
                if by_ware and new_q > 0:
                    ware_set["Cost"] = er._round_cost(new_total / new_q, params)
            updates.append({"op": "update", "table": "comWareAmount", "where": {"ProdID": prod, "WareID": ware},
                            "set": ware_set})
        prod_set: Dict[str, Any] = dict(dates)
        if update_cost:
            remaining_all = ctx["all_qty"].get(prod, Decimal(0)) - q
            prod_set["CTotalCost"] = Decimal(0) if remaining_all == 0 else _dec(ep._get(ctx["products"][prod], "CTotalCost")) - cost
        updates.append({"op": "update", "table": "comProduct", "where": {"ProdID": prod}, "set": prod_set})
        yq = ctx["ym_qty"].get(prod)
        if yq is None:
            updates.append({"op": "insert", "table": "StkYearMonthQty", "values": _row(
                "StkYearMonthQty", YearMonth=ym, ProdID=prod, WareID=ware, Quantity=-q)})
        else:
            updates.append({"op": "update", "table": "StkYearMonthQty",
                            "where": {"YearMonth": ym, "ProdID": prod, "WareID": ware},
                            "set": {"Quantity": _dec(ep._get(yq, "Quantity")) - q}})
        avg_ware = ware if by_ware else ""
        ya = ctx["ym_avg"].get(prod)
        if ya is None:
            updates.append({"op": "insert", "table": "stkYearMonthAvg", "values": _row(
                "stkYearMonthAvg", YearMonth=ym, ProdID=prod, WareID=avg_ware, BatchID="", AtLastQuan=-q)})
        else:
            updates.append({"op": "update", "table": "stkYearMonthAvg",
                            "where": {"YearMonth": ym, "ProdID": prod, "WareID": avg_ware, "BatchID": ""},
                            "set": {"AtLastQuan": _dec(ep._get(ya, "AtLastQuan")) - q}})

    logs = [("comChangeLog", _row(
        "comChangeLog", ProgID=PROG_ID, ChangeTime=er._ole_date(now), IPAddr=(client_ip or "")[:15], UserName=maker_id,
        PKValue=bill_no, Action=1, ComputerName=socket.gethostname()[:20], WinUser=(actor or "web")[:20],
        Flag=0, OldValue="", WinLang=1, IsChiDataSet=False, ActionName=""))]

    customer = ctx.get("customer") or {}
    return {
        "target": target, "bill_no": bill_no, "today": today, "inv_date": inv_date,
        "customer": {"id": cust_id, "name": ep._text(ep._get(customer, "ShortName")) or ep._text(ep._get(customer, "FullName"))},
        "currency": curr, "exch_rate": rate, "rate_source": rate_source, "rate_required": rate_source == "missing",
        "rate_hint": {"sc": float(sc_rate) if sc_rate > 0 else None, "currency": float(sys_rate) if sys_rate > 0 else None},
        "amount_digits": amount_digits, "origin_no": first_no, "sc_nos": list(ctx["scs"]),
        "default_warehouse": ctx["default_ware"], "lines": lines, "totals": totals, "warnings": warnings,
        "confirm_items": confirm_items, "needs_confirm": bool(confirm_items), "closed_scs": closed_scs,
        "reopened_scs": reopened_scs, "cost_params": params, "update_cost": update_cost,
        "missing_masters": ctx["missing_masters"], "inserts": inserts, "updates": updates, "change_logs": logs,
    }


# ── 寫入 / 讀回 ──────────────────────────────────────────────────────────────

def _verify(wcur, plan: Dict[str, Any]) -> Dict[str, Any]:
    no = plan["bill_no"]
    lines = plan["lines"]
    t = plan["totals"]

    def rows(sql: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
        wcur.execute(sql, params)
        return ep._rows(wcur)

    def fail(what: str):
        raise ep.PoSchemaError(f"寫入後讀回的{what}與預期不符，已 rollback")

    head = rows(f"SELECT SumQty, OriginNo FROM TraBillMain WHERE Flag = {INVOICE_FLAG} AND BillNo = %s", (no,))
    if not head or not ep._same(ep._get(head[0], "SumQty"), t["sum_qty"]) \
            or ep._text(ep._get(head[0], "OriginNo")) != plan["origin_no"]:
        fail("Invoice 表頭")
    subs = rows(f"SELECT RowNo, ItemNo, Quantity, DeclareQty, OriginBillNo, OriginRowNo FROM TraBillSub "
                f"WHERE Flag = {INVOICE_FLAG} AND BillNo = %s ORDER BY RowNo", (no,))
    if len(subs) != len(lines):
        fail("Invoice 明細行數")
    for got, l in zip(subs, lines):
        if not (int(ep._get(got, "RowNo")) == l["row_no"] and ep._text(ep._get(got, "ItemNo")) == l["prod_id"]
                and ep._same(ep._get(got, "Quantity"), 1) and ep._same(ep._get(got, "DeclareQty"), l["qty"])
                and ep._text(ep._get(got, "OriginBillNo")) == l["sc_no"] and int(ep._get(got, "OriginRowNo")) == l["sc_row"]):
            fail(f"第 {l['row_no']} 行 Invoice 明細")
    recs = rows(f"SELECT RowNO, ProdID, Quantity, Amount, MLAmount, WareID, FromNO, FromRow FROM comProdRec "
                f"WHERE Flag = {INVOICE_FLAG} AND BillNO = %s ORDER BY RowNO", (no,))
    if len(recs) != len(lines):
        fail("出庫明細行數")
    for got, l in zip(recs, lines):
        if not (ep._text(ep._get(got, "ProdID")) == l["prod_id"] and ep._same(ep._get(got, "Quantity"), l["qty"])
                and ep._same(ep._get(got, "Amount"), l["amount"]) and ep._same(ep._get(got, "MLAmount"), l["ml_amount"])
                and ep._text(ep._get(got, "WareID")) == l["warehouse"]
                and ep._text(ep._get(got, "FromNO")) == l["sc_no"] and int(ep._get(got, "FromRow")) == l["sc_row"]):
            fail(f"第 {l['row_no']} 行出庫明細")
    acct = rows(f"SELECT Total, LocalTotal, ExchRate, CustID, CurrID FROM comBillAccounts "
                f"WHERE Flag = {INVOICE_FLAG} AND SysID = {ACCOUNT_SYS_ID} AND FundBillNo = %s", (no,))
    if not acct or not ep._same(ep._get(acct[0], "Total"), t["total"]) \
            or not ep._same(ep._get(acct[0], "LocalTotal"), t["local_total"]) \
            or not ep._same(ep._get(acct[0], "ExchRate"), plan["exch_rate"]) \
            or ep._text(ep._get(acct[0], "CustID")) != plan["customer"]["id"] \
            or ep._text(ep._get(acct[0], "CurrID")) != ep._text(plan["currency"]):
        fail("應收帳款")
    for u in plan["updates"]:
        if u["op"] == "update" and u["table"] in ("TraBillSub", "TraBillMain"):
            cond = " AND ".join(f"{k} = %s" for k in u["where"])
            got = rows(f"SELECT {', '.join(u['set'])} FROM {u['table']} WHERE {cond}", tuple(u["where"].values()))
            if not got or any(not ep._same(ep._get(got[0], k), v) for k, v in u["set"].items()):
                fail("S/C 回寫")
    return {"TraBillMain": 1, "TraBillSub": len(subs), "comProdRec": len(recs), "comBillAccounts": 1}


# ── 序列化 ────────────────────────────────────────────────────────────────────

_jsonable = er._jsonable

LINE_KEYS = ("row_no", "sc_no", "sc_row", "prod_id", "prod_name", "qty", "qty_remain", "price", "sc_price", "amount",
             "ml_price", "ml_amount", "cost_avg", "cost_for_acc", "warehouse", "stock")


def _changed_columns(table: str, values: Dict[str, Any]) -> Dict[str, Any]:
    base = schema.DEFAULTS[table]
    return {k: _jsonable(v) for k, v in values.items() if base.get(k) != v}


def _summary(plan: Dict[str, Any], *, include_rows: bool) -> Dict[str, Any]:
    target = plan["target"]
    out = {
        "target": target, "target_label": ep.target_label(target), "is_test": ep.is_test(target),
        "is_production": target == ep.PRODUCTION_TARGET, "update_cost": plan["update_cost"],
        "bill_no": plan["bill_no"], "today": plan["today"], "inv_date": plan["inv_date"],
        "customer": plan["customer"], "currency": plan["currency"], "exch_rate": _jsonable(plan["exch_rate"]),
        "rate_source": plan["rate_source"], "rate_required": plan["rate_required"], "rate_hint": plan["rate_hint"],
        "origin_no": plan["origin_no"], "sc_nos": plan["sc_nos"], "default_warehouse": plan["default_warehouse"],
        "lines": _jsonable([{k: l[k] for k in LINE_KEYS} for l in plan["lines"]]),
        "totals": _jsonable(plan["totals"]), "warnings": plan["warnings"], "needs_confirm": plan["needs_confirm"],
        "confirm_items": plan["confirm_items"], "closed_scs": plan["closed_scs"], "reopened_scs": plan["reopened_scs"],
        "cost_params": plan["cost_params"],
        "missing_masters": [{"table": m["table"], "key": m["key"]} for m in plan["missing_masters"]],
    }
    if include_rows:
        rows: Dict[str, List[Dict[str, Any]]] = {}
        for table, values in plan["inserts"] + [(u["table"], u["values"]) for u in plan["updates"] if u["op"] == "insert"] \
                + plan["change_logs"]:
            rows.setdefault(table, []).append(_changed_columns(table, values))
        out["rows"] = rows
        out["updates"] = [{"table": u["table"], "set": _jsonable(u["set"]), "where": u["where"]}
                          for u in plan["updates"] if u["op"] == "update"]
    return out


# ── 對外：預覽 / 建立 ────────────────────────────────────────────────────────

def _plan(wcur, target: str, req: Dict[str, Any], lock: bool, client_ip: str = "", actor: str = "web") -> Dict[str, Any]:
    ep._check_db_name(wcur, target)
    now = local_now()
    today = er.date_to_int(now.date())
    inv_date = parse_inv_date(req["inv_date_raw"], today)
    ctx = _load_context(wcur, target, req, lock)
    _load_rollup_state(wcur, ctx, inv_date // 100)
    year = inv_date // 10000
    bill_no = make_bill_no(year, next_invoice_serial(wcur, year, lock))
    return build_plan(
        ctx, target=target, today=today, inv_date=inv_date, bill_no=bill_no,
        maker=ep._env("ERP_PO_MAKER", ep.DEFAULT_MAKER), maker_id=ep._env("ERP_PO_MAKER_ID", ep.DEFAULT_MAKER_ID),
        client_ip=client_ip, actor=actor, now=now, update_cost=req.get("update_cost", True))


def preview_invoice(raw_req: Dict[str, Any]) -> Dict[str, Any]:
    """試算（不寫入）：單號為預估值，實際以寫入當下為準。"""
    req = normalize_request(raw_req)
    target, creds = resolve_target(req["target"])
    wconn = ep._connect_write(creds)
    try:
        plan = _plan(wconn.cursor(), target, req, lock=False)
    except ErpError:
        raise
    except Exception as exc:
        raise ErpUnavailable(f"ERP 查詢失敗：{ep._safe_message(exc, creds)}") from None
    finally:
        ep._close_quietly(wconn, rollback=True)
    return {**_summary(plan, include_rows=True), "numbers_estimated": True}


def _fingerprint(target: str, req: Dict[str, Any]) -> str:
    lines = sorted([l["sc_no"], l["row_no"], str(l["qty"]), str(l["price"])] for l in req["lines"])
    parts: List[Any] = [target, str(req.get("inv_date_raw") or ""), lines, bool(req.get("update_cost", True)),
                        str(req.get("exch_rate"))]
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False).encode("utf-8")).hexdigest()


def _write(req: Dict[str, Any], target: str, creds: Dict[str, Any], actor: str, client_ip: str) -> Dict[str, Any]:
    """寫入正航（單一交易）。單號撞號時 rollback、重選單號再試，最多 MAX_NUMBER_ATTEMPTS 次。"""
    with ep._write_lock:
        wconn = ep._connect_write(creds)
        committed = False
        plan: Dict[str, Any] = {}
        collisions: List[str] = []
        try:
            wcur = wconn.cursor()
            for attempt in range(1, MAX_NUMBER_ATTEMPTS + 1):
                try:
                    plan = _plan(wcur, target, req, lock=True, client_ip=client_ip, actor=actor)
                    if plan["rate_required"]:
                        raise InvoiceValidationError(f"{plan['currency']}：請輸入發票匯率（大於 0）後再寫入")
                    if plan["needs_confirm"] and not req["confirm"]:
                        raise InvoiceConfirmWarning(plan["confirm_items"])  # 尚未寫入任何東西
                    taken = existing_numbers(wcur, plan["bill_no"], lock=True)
                    if taken:
                        raise NumberCollision(f"單號 {plan['bill_no']} 已存在（{'、'.join(taken)}）")
                    try:
                        if plan["missing_masters"]:
                            with ep._source_session() as src:
                                ep._copy_masters(src.cursor(), wcur, plan["missing_masters"], target)
                        er._execute_plan(wcur, plan)
                    except Exception as exc:
                        if er.is_duplicate_key_error(exc):
                            raise NumberCollision(f"寫入時單號重複：{ep._safe_message(exc, creds)}") from None
                        raise
                    readback = _verify(wcur, plan)
                    ep._check_db_name(wcur, target)
                    break
                except NumberCollision as exc:
                    collisions.append(str(exc))
                    logger.warning("開 Invoice 單號撞號（第 %s 次），rollback 後重選：%s", attempt, exc)
                    wconn.rollback()
                    if attempt >= MAX_NUMBER_ATTEMPTS:
                        raise ErpUnavailable(
                            f"單號連續 {MAX_NUMBER_ATTEMPTS} 次撞號，已 rollback、未寫入：{collisions[-1]}") from None
            plan["number_attempts"] = attempt
            try:
                wconn.commit()
                committed = True
            except Exception as exc:
                raise ErpUnavailable(
                    f"commit 失敗，結果不確定，請到正航確認是否已有 Invoice {plan['bill_no']}："
                    f"{ep._safe_message(exc, creds)}") from None
        except ErpError:
            raise
        except Exception as exc:
            raise ErpUnavailable(f"寫入正航失敗，已 rollback：{ep._safe_message(exc, creds)}") from None
        finally:
            ep._close_quietly(wconn, rollback=not committed)
    return {**_summary(plan, include_rows=False), "readback": readback, "number_attempts": plan["number_attempts"]}


def create_invoice(raw_req: Dict[str, Any], actor: str = "web", client_ip: str = "") -> Dict[str, Any]:
    """建立 Invoice 並記錄本機紀錄（成功與失敗都記）。超開 / 庫存不足只是警告：帶 confirm=true 即寫入。"""
    from .. import database as db

    req = normalize_request(raw_req)
    target, creds = resolve_target(req["target"])
    fp = _fingerprint(target, req)
    if req["client_token"]:
        prior = db.find_erp_invoice_by_token(req["client_token"])
        if prior:
            raise ep.DuplicateSubmitError(f"此次 Invoice 已建立（{prior['bill_no']}），已略過重複送出")
    since = (local_now() - timedelta(seconds=IDEMPOTENCY_SECONDS)).isoformat(timespec="seconds")
    recent = db.find_recent_erp_invoice(fp, since)
    if recent:
        raise ep.DuplicateSubmitError(f"相同內容的 Invoice 剛建立過（{recent['bill_no']}），已略過重複送出")
    if not po_duplicates.claim("inv:" + fp):
        raise ep.DuplicateSubmitError("相同內容的 Invoice 正在建立中，請勿重複送出")
    try:
        base = {
            "target_db": target, "actor": actor, "client_ip": client_ip, "client_token": req["client_token"],
            "fingerprint": fp,
            "lines_json": json.dumps([{"sc_no": l["sc_no"], "row_no": l["row_no"], "qty": str(l["qty"]),
                                       "price": None if l["price"] is None else str(l["price"])}
                                      for l in req["lines"]], ensure_ascii=False),
        }
        try:
            result = _write(req, target, creds, actor, client_ip)
        except InvoiceConfirmWarning:
            raise
        except Exception as exc:
            message = ep._safe_message(exc, creds)
            _safe_log(db, {**base, "status": "error", "error": message})
            db.log_activity("erp_invoice_failed", f"{ep.target_label(target)}：{message}")
            raise
        t = result["totals"]
        invoice_id = _safe_log(db, {
            **base, "bill_no": result["bill_no"], "inv_date": result["inv_date"],
            "customer_id": result["customer"]["id"], "customer_name": result["customer"]["name"],
            "currency": result["currency"], "exch_rate": result["exch_rate"],
            "lines_json": json.dumps(result["lines"], ensure_ascii=False), "sum_qty": t["sum_qty"],
            "total": t["total"], "local_total": t["local_total"], "status": "ok"})
        db.log_activity(
            "erp_invoice_created",
            f"{ep.target_label(target)} Invoice {result['bill_no']}，客戶 {result['customer']['id']}，"
            f"{len(result['lines'])} 行，{result['currency']} {t['total']:g}（匯率 {result['exch_rate']:g}）")
        return {"ok": True, **result, "invoice_id": invoice_id}
    finally:
        po_duplicates.release("inv:" + fp)


def _safe_log(db, rec: Dict[str, Any]) -> Optional[int]:
    try:
        return db.add_erp_invoice(rec)
    except Exception:
        return None  # 本機紀錄失敗不可蓋掉已完成的 ERP 結果


def list_history(limit: int = 50) -> List[Dict[str, Any]]:
    from .. import database as db

    return db.list_erp_invoices(limit)
