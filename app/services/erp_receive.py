"""一次收料：在正航測試公司把採購單明細一次做成「收料(IM) → 驗收(QM) → 進貨(RM)」。

欄位逐欄對照 Andy 在 CHIComp13 的手動鏈（採購 20261030001 → 收料 → 驗收 → 進貨），
資料欄位預設值取自 erp_receive_schema（該手動鏈的 SELECT *）。規則：
- 寫入公司 = ERP_RECEIVE_TARGET（沒設定時沿用 ERP_WRITE_TARGET），與開採購單（ERP_WRITE_TARGET）分開設定。
- 測試公司（ERP_TEST_TARGETS）可直接寫；正式公司 CHIComp03（v2026.10.30.11 起）只有同時符合才准寫：
  ERP_RECEIVE_TARGET=CHIComp03、ERP_ALLOW_PROD_WRITE_CHICOMP03=1、且有專用寫入帳密
  ERP_SQL_WRITE_USER_CHICOMP03 / ERP_SQL_WRITE_PASSWORD_CHICOMP03（可選 ERP_SQL_WRITE_SERVER_CHICOMP03；正式公司絕不使用測試帳密）。
  前端傳來的 target 若是正式公司，也必須等於 ERP_RECEIVE_TARGET；其他非測試公司一律拒絕。
- 平均成本：ERP_RECEIVE_UPDATE_COST（預設 1）為預設值，每次請求可帶 update_cost 覆寫；
  false 時不動 comProduct.CAvgCost / CTotalCost 與 comWareAmount.Cost / TotalCost（庫存量、LatestPurchDate 照常更新）。
- 與 erp_purchase（只 INSERT）不同，這裡必須 UPDATE 採購單剩餘量、庫存與彙總表（正航本身也是這樣記帳）。
- 整件事（收料、驗收、進貨、所有彙總與採購單回寫）在同一個 transaction，任何錯誤 rollback；寫完讀回比對才 commit。
- 收料 / 驗收日 = 今天；進貨日 = 發票日（預設今天，可改）。
- IM / QM / RM 共用同一個流水號（單號 = IM- / QM- / RM- + yyyymmdd + 3 碼）；流水號 = 當日有前綴與無前綴單號的最大號 + 1。
- 成本 / 日期更新照正航參數（comParams：CostByWare、ComCostDigs、comCostDot），見 build_plan 的 _cost_plan。
- 進貨單表頭 BillStatus / Detail 照公司設定（v2026.10.30.10），見 load_bill_params。
- 不寫 comInvoice、stkBatchInvo*、accVoucher*，也不碰任何計數表（正航單號 = 當日 MAX+1）。
錯誤訊息一律不含帳號密碼。
"""
from __future__ import annotations

import calendar
import hashlib
import json
import logging
import socket
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import erp_purchase as ep
from . import erp_receive_schema as schema
from . import po_duplicates
from .erp_stock import ErpError, ErpNotConfigured, ErpUnavailable
from .local_time import local_now

logger = logging.getLogger(__name__)

MAX_LINES = 100
DEFAULT_WAREHOUSE = "001"
EXAMINE_TYPE = "A1"  # 驗收一律良好
IDEMPOTENCY_SECONDS = 60
# 各公司單號前綴（收料, 驗收, 進貨）。所有公司（含 Comp13 與其他測試公司）一律 IM- / QM- / RM-。
# 流水號會同時考慮既有「有前綴」與「無前綴」（Andy 手動鏈原生單號）的單號，所以不會撞號。
DEFAULT_PREFIXES: Tuple[str, str, str] = ("IM-", "QM-", "RM-")
NUMBER_PREFIXES: Dict[str, Tuple[str, str, str]] = {
    "CHIComp13": DEFAULT_PREFIXES,
    "CHIComp03": DEFAULT_PREFIXES,
}
# 正航成本參數（comParams，UserID = ''）：CostByWare 0 = 全公司成本 / 1 = 依倉庫；ComCostDigs 成本小數位；comCostDot 1 = 四捨五入到該位數 / 0 = 不捨入
COST_PARAMS = ("CostByWare", "ComCostDigs", "comCostDot")
_HINT = " WITH (UPDLOCK, HOLDLOCK)"
# stkBillSub.LocalPayment：Andy 手動鏈（無發票）為 0；Comp03 RM-20261102001 為列未稅金額（疑為事後「進項發票」回寫）。
# per Comp03 RM-20261102001 只借用「一張 RM 每列一筆 stkBillSub、StkMlAmount = 列未稅金額」；LocalPayment 依 Andy 的鏈取 0。
# 若要改成與 Comp03 相同，把這個旗標改 True。
STK_SUB_LOCAL_PAYMENT_EQUALS_AMOUNT = False
# 進貨單 stkBillMain.BillStatus（建立當下）。Comp03 的 BillStatus = 「已轉傳票」：2021 起 192 張未轉傳票的 RM 有 183 張是 0，
# 3811 張已轉傳票的全是 1（會計轉傳票時正航才改 1，NotPayed / InCost / ShareType / LocalPayment 也是那時回寫）；
# Comp13 正航自己建單就是 1（Andy 手動 20261030001）。兩家 comParams 唯一說得通的開關是 CreateAccByFund
# （Comp03 = '1'「由帳款產生傳票」，Comp13 沒有這個參數）：'1' → 建立時 0（等轉傳票），其他 → 1。
# 只驗證過兩家公司，所以已驗證的公司在這裡明確釘住；其他公司才用 CreateAccByFund 推。
RM_BILL_STATUS_AT_CREATION: Dict[str, int] = {"CHIComp03": 0, "CHIComp13": 1}


class ReceiveValidationError(ep.PoValidationError):
    """輸入或 ERP 資料不符規則（HTTP 400）。"""


class OverReceiveWarning(ReceiveValidationError):
    """收料量超過採購單剩餘量，未帶 confirm=true 時不寫入並回報。"""

    def __init__(self, items: List[Dict[str, Any]]):
        super().__init__("部分明細收料量超過採購單剩餘量，請確認後再送出")
        self.items = items


# ── 設定 / 目標 ───────────────────────────────────────────────────────────────

def warehouse() -> str:
    return ep._env("ERP_RECEIVE_WAREHOUSE", DEFAULT_WAREHOUSE) or DEFAULT_WAREHOUSE


def number_prefixes(target: str) -> Tuple[str, str, str]:
    return NUMBER_PREFIXES.get(target, DEFAULT_PREFIXES)


RECEIVE_TARGET_ENV = "ERP_RECEIVE_TARGET"
PROD_WRITE_FLAG_ENV = "ERP_ALLOW_PROD_WRITE_CHICOMP03"
UPDATE_COST_ENV = "ERP_RECEIVE_UPDATE_COST"
_FALSY = {"0", "false", "no", "off", "n", "f"}


def _env_flag(name: str, default: bool) -> bool:
    raw = ep._env(name, "")
    if not raw:
        return default
    return raw.strip().lower() not in _FALSY


def update_cost_default() -> bool:
    """ERP_RECEIVE_UPDATE_COST：預設 1（更新平均成本）；0 / false / no / off = 不更新。"""
    return _env_flag(UPDATE_COST_ENV, True)


def get_receive_target() -> str:
    """一次收料的寫入公司：ERP_RECEIVE_TARGET，沒設定時沿用開採購單的 ERP_WRITE_TARGET。"""
    raw = ep._env(RECEIVE_TARGET_ENV, "")
    if not raw:
        return ep.get_target()
    if not ep.TARGET_RE.match(raw):
        raise ErpNotConfigured("ERP_RECEIVE_TARGET 格式不正確（需為 CHIComp 加兩位數字，例如 CHIComp13）")
    return raw


def production_write_allowed(target: str) -> bool:
    """正式公司只有在 ERP_RECEIVE_TARGET 明確指向它、且 ERP_ALLOW_PROD_WRITE_CHICOMP03=1 時才開放。"""
    return (
        target == ep.PRODUCTION_TARGET
        and ep._env(RECEIVE_TARGET_ENV, "") == ep.PRODUCTION_TARGET
        and ep._env(PROD_WRITE_FLAG_ENV, "") == "1"
    )


def ensure_writable_target(target: str) -> None:
    """測試公司照常；正式公司需明確開關（見模組說明）；其他公司一律拒絕。"""
    if target == ep.PRODUCTION_TARGET:
        if not production_write_allowed(target):
            raise ReceiveValidationError(
                f"{ep.target_label(target)} 未開放一次收料（需設定 ERP_RECEIVE_TARGET=CHIComp03 與 "
                f"{PROD_WRITE_FLAG_ENV}=1）")
        return
    if not ep.is_test(target):
        raise ReceiveValidationError(f"{target} 不在測試公司清單（ERP_TEST_TARGETS），拒絕寫入")


def resolve_target(requested: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    target = ep._text(requested) or get_receive_target()
    if not ep.TARGET_RE.match(target):
        raise ReceiveValidationError("目標公司格式不正確")
    ensure_writable_target(target)
    creds = ep.write_credentials(target)
    if creds is None:
        raise ErpNotConfigured(ep.UNAVAILABLE_REASON)
    return target, creds


def get_config() -> Dict[str, Any]:
    """前端設定；絕不含帳號密碼。"""
    result: Dict[str, Any] = {
        "target": "", "label": "", "is_test": False, "available": False, "reason": "",
        "warehouse": warehouse(), "today": date_to_int(local_now().date()),
        "maker": ep._env("ERP_PO_MAKER", ep.DEFAULT_MAKER),
        "is_production": False, "update_cost_default": update_cost_default(),
    }
    try:
        target = get_receive_target()
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


# ── 純函式：日期 / 金額 / 付款條件 / 單號 ─────────────────────────────────────

date_to_int = ep.date_to_int


def int_to_date(value: int) -> date:
    return date(value // 10000, value // 100 % 100, value % 100)


def parse_rm_date(value: Any, today: int) -> int:
    """進貨日：空值 = 今天；否則需為真實日期（yyyymmdd 整數或 YYYY-MM-DD 字串）。"""
    if value in (None, "", 0):
        return today
    text = str(value).strip().replace("-", "").replace("/", "")
    if not (text.isdigit() and len(text) == 8):
        raise ReceiveValidationError("進貨日期格式需為 yyyymmdd")
    v = int(text)
    try:
        d = int_to_date(v)
    except ValueError:
        raise ReceiveValidationError("進貨日期不是有效日期") from None
    if not 2000 <= d.year <= 2100:
        raise ReceiveValidationError("進貨日期不是有效日期")
    return v


def _month_end(y: int, m: int) -> date:
    return date(y, m, calendar.monthrange(y, m)[1])


def calc_terms(
    d: int, day_of_close: int, gather_style: int, gather_delay: int, check_style: int, check_delay: int,
) -> Dict[str, int]:
    """AccMonth / PrepayDay / EncashDay（公式由約 50 張 Comp03 進貨單反推）。

    DOC=comCustTrade.DayOfClose（31 或 0 = 月底）。
    AccMonth = D 的年月；DOC 在 1~30 且 D 的日 > DOC 則順延一個月。結帳日 C = AccMonth 的第 DOC 日（超過月底取月底）。
    GatherStyle 0 → D；1 → AccMonth 月底 + GatherDelay 天；2 → C + GatherDelay 天；其他 → D（並記警告）。
    CheckStyle 0 / 3 → D；1 → AccMonth 月底 + CheckDelay 天；2 → PrepayDay + CheckDelay 天。
    """
    dd = int_to_date(d)
    y, m = dd.year, dd.month
    doc = int(day_of_close or 0)
    if 1 <= doc <= 30 and dd.day > doc:
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    month_end = _month_end(y, m)
    closing = date(y, m, min(doc, month_end.day)) if 1 <= doc <= 30 else month_end

    if gather_style == 0:
        prepay = dd
    elif gather_style == 1:
        prepay = month_end + timedelta(days=int(gather_delay))
    elif gather_style == 2:
        prepay = closing + timedelta(days=int(gather_delay))
    else:
        logger.warning("未知的 GatherStyle=%s，PrepayDay 以進貨日代替", gather_style)
        prepay = dd

    if check_style in (0, 3):
        encash = dd
    elif check_style == 1:
        encash = month_end + timedelta(days=int(check_delay))
    elif check_style == 2:
        encash = prepay + timedelta(days=int(check_delay))
    else:
        logger.warning("未知的 CheckStyle=%s，EncashDay 以進貨日代替", check_style)
        encash = dd
    return {"AccMonth": y * 100 + m, "PrepayDay": date_to_int(prepay), "EncashDay": date_to_int(encash)}


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value if value is not None else 0))
    except InvalidOperation:
        return Decimal(0)


def calc_line_amounts(qty: Any, price: Any, discount: Any, tax_rate: Any) -> Tuple[Decimal, Decimal]:
    """列未稅金額 = round(數量 × 單價 × 折扣, 0)；稅額 = round(金額 × 稅率, 0)（四捨五入到整數，研究資料中金額皆為整數）。"""
    disc = _dec(discount)
    if disc <= 0:
        disc = Decimal(1)
    amount = ep._round(_dec(qty) * _dec(price) * disc, 0)
    tax = ep._round(amount * _dec(tax_rate), 0)
    return amount, tax


def _serial_series(prefixes: Tuple[str, str, str]) -> List[Tuple[str, str, str, str]]:
    im, qm, rm = prefixes
    return [
        ("ordExamineMain", "ExamineNO", "Flag = 5", im),
        ("ordExamineMain", "ExamineNO", "Flag = 6", qm),
        ("stkBillMain", "BillNO", "Flag = 100", rm),
        ("comBillAccounts", "FundBillNo", "Flag = 100 AND SysID = 2", rm),  # 安全檢查
    ]


def next_receive_serial(cur, prefixes: Tuple[str, str, str], dates: Iterable[int], lock: bool) -> int:
    """IM / QM / RM 共用的流水號 = 各系列（收料 Flag5、驗收 Flag6、進貨 Flag100、應付帳款）當日最大號 + 1。

    IM / QM 用今天、RM 用進貨日；兩日不同時每個系列都查兩個日期取最大，三張單才會用同一號且不撞號。
    lock=True 時用 UPDLOCK/HOLDLOCK 鎖住範圍（必須在寫入交易內）。
    """
    hint = _HINT if lock else ""
    top = 0
    for d in sorted(set(int(x) for x in dates)):
        for table, col, cond, prefix in _serial_series(prefixes):
            # 有前綴與無前綴都要查（LEN 過濾讓兩者互不混淆）
            for pre in dict.fromkeys((prefix, "")):
                head = f"{pre}{d}"
                cur.execute(
                    f"SELECT MAX({col}) FROM {table}{hint} WHERE {cond} AND {col} LIKE %s AND LEN({col}) = %s",
                    (head + "%", len(head) + 3),
                )
                found = cur.fetchone()
                last = ep._text(found[0]) if found else ""
                if last:
                    try:
                        top = max(top, int(last[len(head):]))
                    except ValueError:
                        raise ep.PoSchemaError(f"既有單號格式異常：{last}") from None
    serial = top + 1
    if serial > 999:
        raise ReceiveValidationError("當日收料 / 驗收 / 進貨單號已用完（最多 999 張）")
    return serial


def make_numbers(prefixes: Tuple[str, str, str], today: int, rm_date: int, serial: int) -> Dict[str, str]:
    im, qm, rm = prefixes
    return {
        "im_no": f"{im}{today}{serial:03d}",
        "qm_no": f"{qm}{today}{serial:03d}",
        "rm_no": f"{rm}{rm_date}{serial:03d}",
    }


# ── 輸入驗證 ──────────────────────────────────────────────────────────────────

def normalize_request(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw_lines = raw.get("lines") or []
    if not 1 <= len(raw_lines) <= MAX_LINES:
        raise ReceiveValidationError(f"明細需為 1～{MAX_LINES} 行")
    lines: List[Dict[str, Any]] = []
    seen = set()
    for i, r in enumerate(raw_lines, 1):
        po_no = ep._text(r.get("po_no"))
        if not po_no:
            raise ReceiveValidationError(f"第 {i} 行：缺少採購單號")
        try:
            row_no = int(r.get("row_no"))
        except (TypeError, ValueError):
            raise ReceiveValidationError(f"第 {i} 行：採購單列號不正確") from None
        if (po_no, row_no) in seen:
            raise ReceiveValidationError(f"第 {i} 行：{po_no} 第 {row_no} 列重複")
        seen.add((po_no, row_no))
        qty = ep._to_decimal(r.get("qty"), f"第 {i} 行數量")
        if qty <= 0:
            raise ReceiveValidationError(f"第 {i} 行：數量必須大於 0")
        price = None
        if r.get("price") is not None:
            price = ep._to_decimal(r.get("price"), f"第 {i} 行單價")
            if price < 0:
                raise ReceiveValidationError(f"第 {i} 行：單價不可為負")
        lines.append({"po_no": po_no, "row_no": row_no, "qty": qty, "price": price})
    return {
        "target": ep._text(raw.get("target")),
        "rm_date_raw": raw.get("rm_date"),
        "lines": lines,
        "confirm": raw.get("confirm") is True,
        "client_token": ep._text(raw.get("client_token"))[:80],
        "update_cost": update_cost_default() if raw.get("update_cost") is None else raw.get("update_cost") is True,
    }


# ── 讀取：可收料明細 ──────────────────────────────────────────────────────────

def list_open_lines(
    vendor: Optional[str] = None, q: Optional[str] = None, limit: int = 300, target: Optional[str] = None,
) -> Dict[str, Any]:
    """寫入目標公司尚未結案、剩餘量 > 0 的採購單明細（唯讀）。"""
    target, creds = resolve_target(target)
    limit = max(1, min(int(limit), 1000))
    where = ["s.Flag = 4", "m.BillStatus = 0", "s.QtyRemain > 0"]
    params: List[Any] = []
    if ep._text(vendor):
        where.append("m.CustomerID = %s")
        params.append(ep._text(vendor))
    if ep._text(q):
        pat = f"%{ep._like_escape(ep._text(q))}%"
        where.append("(m.CustomerID LIKE %s OR c.ShortName LIKE %s OR c.FullName LIKE %s OR s.ProdID LIKE %s "
                     "OR s.ProdName LIKE %s OR m.BillNO LIKE %s)")
        params += [pat] * 6
    sql = (
        f"SELECT TOP {limit + 1} m.BillNO, s.RowNO, m.BillDate, m.CustomerID, c.ShortName, c.FullName, s.ProdID, "
        "s.ProdName, s.Quantity, s.QtyRemain, s.Price, s.TaxRate, s.Unit, m.CurrID, s.Discount "
        "FROM ordBillSub s JOIN ordBillMain m ON m.Flag = s.Flag AND m.BillNO = s.BillNO "
        "LEFT JOIN comCustomer c ON c.Flag = 2 AND c.ID = m.CustomerID "
        f"WHERE {' AND '.join(where)} ORDER BY m.BillDate, m.BillNO, s.RowNO"
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
    lines = []
    for r in rows[:limit]:
        bd = int(r[2] or 0)
        lines.append({
            "po_no": ep._text(r[0]), "row_no": int(r[1]), "date": bd,
            "date_text": f"{bd // 10000:04d}-{bd // 100 % 100:02d}-{bd % 100:02d}" if bd else "",
            "vendor_id": ep._text(r[3]), "vendor_name": ep._text(r[4]) or ep._text(r[5]),
            "part_number": ep._text(r[6]), "part_name": ep._text(r[7]),
            "qty": float(r[8] or 0), "qty_remain": float(r[9] or 0), "price": float(r[10] or 0),
            "tax_rate": float(r[11] or 0), "unit": ep._text(r[12]), "currency": ep._text(r[13]),
            "discount": float(r[14] or 1),
        })
    return {"target": target, "target_label": ep.target_label(target), "lines": lines, "truncated": len(rows) > limit}


# ── 讀取：規劃用的資料 ────────────────────────────────────────────────────────

def _one_row(cur, sql: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    cur.execute(sql, params)
    return ep._one(cur)


def load_cost_params(wcur, target: str) -> Dict[str, Any]:
    """讀 comParams（UserID = ''）的成本參數；任何一個缺或格式不對就拒絕，不猜。"""
    found: Dict[str, Decimal] = {}
    missing: List[str] = []
    for name in COST_PARAMS:
        row = _one_row(wcur, "SELECT paramValue FROM comParams WHERE UserID = %s AND ParamName = %s", ("", name))
        text = ep._text(ep._get(row, "paramValue")) if row else ""
        try:
            found[name] = Decimal(text)
        except InvalidOperation:
            missing.append(name)
    if missing:
        raise ReceiveValidationError(f"找不到正航成本參數 {'、'.join(missing)}（{ep.target_label(target)} comParams），拒絕寫入")
    by_ware, digits, dot = found["CostByWare"], found["ComCostDigs"], found["comCostDot"]
    if by_ware not in (0, 1) or dot not in (0, 1) or digits != digits.to_integral_value() or not 0 <= digits <= 10:
        raise ReceiveValidationError("正航成本參數值不正確（CostByWare / comCostDot 需為 0 或 1，ComCostDigs 需為 0～10）")
    return {"cost_by_ware": int(by_ware) == 1, "digits": int(digits), "dot": int(dot) == 1}


def _param_text(wcur, name: str) -> Optional[str]:
    row = _one_row(wcur, "SELECT paramValue FROM comParams WHERE UserID = %s AND ParamName = %s", ("", name))
    return None if row is None else ep._text(ep._get(row, "paramValue"))


def load_bill_params(wcur, target: str) -> Dict[str, Any]:
    """進貨單表頭照公司設定：
    - Detail（摘要）= comParams.ComDefDescribe（Comp03 ''、Comp13 'A'；參數不存在時 ''）。
    - BillStatus = RM_BILL_STATUS_AT_CREATION[target]；沒列的公司依 CreateAccByFund（'1' → 0 未轉傳票，否則 1）。
    """
    detail = _param_text(wcur, "ComDefDescribe") or ""
    by_fund = (_param_text(wcur, "CreateAccByFund") or "").strip()
    if target in RM_BILL_STATUS_AT_CREATION:
        status, source = RM_BILL_STATUS_AT_CREATION[target], "company"
    else:
        status, source = (0 if by_fund == "1" else 1), "CreateAccByFund"
    return {"detail": detail, "bill_status": status, "bill_status_source": source, "create_acc_by_fund": by_fund}


def _load_context(wcur, target: str, req: Dict[str, Any], lock: bool) -> Dict[str, Any]:
    hint = _HINT if lock else ""
    cost_params = load_cost_params(wcur, target)
    bill_params = load_bill_params(wcur, target)
    pos: Dict[str, Dict[str, Any]] = {}
    for po_no in dict.fromkeys(l["po_no"] for l in req["lines"]):
        main = _one_row(wcur, f"SELECT * FROM ordBillMain{hint} WHERE Flag = 4 AND BillNO = %s", (po_no,))
        if main is None:
            raise ReceiveValidationError(f"找不到採購單 {po_no}")
        if int(ep._get(main, "BillStatus", 0) or 0) != 0:
            raise ReceiveValidationError(f"採購單 {po_no} 已結案，不能再收料")
        wcur.execute(f"SELECT * FROM ordBillSub{hint} WHERE Flag = 4 AND BillNO = %s ORDER BY RowNO", (po_no,))
        subs = {int(ep._get(r, "RowNO")): r for r in ep._rows(wcur)}
        pos[po_no] = {"main": main, "subs": subs}

    picked: List[Dict[str, Any]] = []
    for i, l in enumerate(req["lines"], 1):
        sub = pos[l["po_no"]]["subs"].get(l["row_no"])
        if sub is None:
            raise ReceiveValidationError(f"第 {i} 行：{l['po_no']} 沒有第 {l['row_no']} 列")
        picked.append({**l, "main": pos[l["po_no"]]["main"], "sub": sub})

    vendors = {ep._text(ep._get(p["main"], "CustomerID")) for p in pos.values()}
    if len(vendors) != 1:
        raise ReceiveValidationError("一次收料只能選同一家廠商的採購單明細")
    currs = {ep._text(ep._get(p["main"], "CurrID")) for p in pos.values()}
    if len(currs) != 1:
        raise ReceiveValidationError("一次收料的採購單幣別必須相同")
    vendor_id = vendors.pop()

    cust = _one_row(wcur, "SELECT ShortName, FullName FROM comCustomer WHERE Flag = 2 AND ID = %s", (vendor_id,)) or {}
    trade = _one_row(wcur, f"SELECT * FROM comCustTrade{hint} WHERE Flag = 2 AND ID = %s", (vendor_id,))
    if trade is None:
        raise ReceiveValidationError(f"{ep.target_label(target)} 缺少廠商 {vendor_id} 的交易條件（comCustTrade），請先補齊主檔")
    ware = warehouse()
    if _one_row(wcur, "SELECT 1 FROM comWareHouse WHERE WareHouseID = %s", (ware,)) is None:
        raise ReceiveValidationError(f"{ep.target_label(target)} 找不到倉庫 {ware}")

    products: Dict[str, Dict[str, Any]] = {}
    ware_amounts: Dict[str, Optional[Dict[str, Any]]] = {}
    all_qty: Dict[str, Decimal] = {}
    for prod in dict.fromkeys(ep._text(ep._get(p["sub"], "ProdID")) for p in picked):
        row = _one_row(wcur, f"SELECT * FROM comProduct{hint} WHERE ProdID = %s", (prod,))
        if row is None:
            raise ReceiveValidationError(f"{ep.target_label(target)} 找不到料號 {prod}")
        products[prod] = row
        ware_amounts[prod] = _one_row(
            wcur, f"SELECT * FROM comWareAmount{hint} WHERE ProdID = %s AND WareID = %s", (prod, ware))
        if not cost_params["cost_by_ware"]:
            # 全公司成本：要用所有倉庫的庫存合計
            wcur.execute(f"SELECT Quantity FROM comWareAmount{hint} WHERE ProdID = %s", (prod,))
            all_qty[prod] = sum((_dec(r[0]) for r in wcur.fetchall()), Decimal(0))
    return {
        "pos": pos, "picked": picked, "vendor_id": vendor_id,
        "vendor_name": ep._text(ep._get(cust, "ShortName")) or ep._text(ep._get(cust, "FullName")),
        "trade": trade, "ware": ware, "products": products, "ware_amounts": ware_amounts,
        "curr": currs.pop(), "hint": hint,
        "cost_params": cost_params, "all_qty": all_qty, "bill_params": bill_params,
    }


def _load_rollup_state(wcur, ctx: Dict[str, Any], ym: int) -> None:
    """彙總表現況（要加總的 key 才讀）：月庫存、月平均、應付帳款。"""
    hint = ctx["hint"]
    ware = ctx["ware"]
    avg_ware = ware if ctx["cost_params"]["cost_by_ware"] else ""  # 全公司成本時月平均表 WareID 為空字串
    ctx["ym_qty"], ctx["ym_avg"] = {}, {}
    for prod in ctx["products"]:
        ctx["ym_qty"][prod] = _one_row(
            wcur, f"SELECT * FROM StkYearMonthQty{hint} WHERE YearMonth = %s AND ProdID = %s AND WareID = %s",
            (ym, prod, ware))
        ctx["ym_avg"][prod] = _one_row(
            wcur, f"SELECT * FROM stkYearMonthAvg{hint} WHERE YearMonth = %s AND ProdID = %s AND WareID = %s AND BatchID = %s",
            (ym, prod, avg_ware, ""))
    ctx["accounts"] = _one_row(
        wcur, f"SELECT * FROM comCustAccounts{hint} WHERE Flag = 2 AND SysID = 2 AND ID = %s AND CurrID = %s",
        (ctx["vendor_id"], ctx["curr"]))


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


def _ole_date(now: datetime) -> float:
    return (now - datetime(1899, 12, 30)).total_seconds() / 86400


def _round_cost(value: Decimal, params: Dict[str, Any]) -> float:
    """comCostDot = 1 → 四捨五入（half-up）到 ComCostDigs 位；否則不捨入，直接存完整浮點數。"""
    if params["dot"]:
        value = value.quantize(Decimal(1).scaleb(-params["digits"]), rounding=ROUND_HALF_UP)
    return float(value)


def _cost_plan(ctx: Dict[str, Any], by_prod: Dict[str, Dict[str, Decimal]], warnings: List[Dict[str, Any]],
               update_cost: bool = True) -> Dict[str, Dict[str, Any]]:
    """每個料號進貨後的成本（依正航 CostByWare / ComCostDigs / comCostDot）。

    CostByWare = 0（全公司，如 Comp03）：CAvgCost = r((舊 CTotalCost + 金額) / (所有倉庫舊庫存合計 + 數量))；
      新庫存 <= 0 時 CAvgCost 不動並警告（行為未確認）。comWareAmount.Cost 不維護。
    CostByWare = 1（依倉庫，如 Comp13）：comWareAmount.Cost = r(新倉庫 TotalCost / 新倉庫數量)；comProduct.CAvgCost 不動。
    update_cost=False：不重算，CostAvg 帶現有成本（CostByWare=1 取倉庫 Cost，否則 CAvgCost），avg / ware_cost 皆 None。
    回傳 {prod: {"cost": comProdRec.CostAvg, "avg": 新 CAvgCost 或 None, "ware_cost": 倉庫 Cost 或 None}}
    """
    params = ctx["cost_params"]
    out: Dict[str, Dict[str, Any]] = {}
    for prod, agg in by_prod.items():
        q, amt = agg["qty"], agg["amount"]
        product = ctx["products"][prod]
        if not update_cost:
            # 不更新平均成本：comProdRec.CostAvg 帶現有成本，CAvgCost / 倉庫 Cost 都不動
            if params["cost_by_ware"]:
                wa = ctx["ware_amounts"].get(prod)
                cost = float(_dec(ep._get(wa, "Cost"))) if wa is not None else 0.0
            else:
                cost = float(_dec(ep._get(product, "CAvgCost")))
            out[prod] = {"cost": cost, "avg": None, "ware_cost": None}
            continue
        if params["cost_by_ware"]:
            wa = ctx["ware_amounts"].get(prod)
            old_cost = float(_dec(ep._get(wa, "Cost"))) if wa is not None else 0.0
            new_q = (_dec(ep._get(wa, "Quantity")) if wa is not None else Decimal(0)) + q
            new_total = (_dec(ep._get(wa, "TotalCost")) if wa is not None else Decimal(0)) + amt
            if new_q > 0:
                cost = _round_cost(new_total / new_q, params)
                ware_cost: Optional[float] = cost
            else:
                cost, ware_cost = old_cost, None
                warnings.append({"code": "cost_nonpositive_qty", "needs_confirm": False,
                                 "message": f"{prod} 進貨後倉庫庫存 {new_q:g} ≤ 0，倉庫成本維持不變（正航行為未確認）"})
            out[prod] = {"cost": cost, "avg": None, "ware_cost": ware_cost}
        else:
            new_q = ctx["all_qty"].get(prod, Decimal(0)) + q
            new_total = _dec(ep._get(product, "CTotalCost")) + amt
            if new_q > 0:
                avg: Optional[float] = _round_cost(new_total / new_q, params)
                cost = avg
            else:
                avg, cost = None, float(_dec(ep._get(product, "CAvgCost")))
                warnings.append({"code": "cost_nonpositive_qty", "needs_confirm": False,
                                 "message": f"{prod} 進貨後全公司庫存 {new_q:g} ≤ 0，平均成本維持不變（正航行為未確認）"})
            out[prod] = {"cost": cost, "avg": avg, "ware_cost": None}
    return out


def build_plan(
    ctx: Dict[str, Any], *, target: str, today: int, rm_date: int, numbers: Dict[str, str],
    maker: str, maker_id: str, client_ip: str, actor: str, now: datetime, update_cost: bool = True,
) -> Dict[str, Any]:
    picked = ctx["picked"]
    first = picked[0]["main"]
    vendor_id, ware, curr = ctx["vendor_id"], ctx["ware"], ctx["curr"]
    im_no, qm_no, rm_no = numbers["im_no"], numbers["qm_no"], numbers["rm_no"]
    exch = _dec(ep._get(first, "ExchRate", 1)) or Decimal(1)
    salesman = ep._text(ep._get(first, "SalesMan"))
    depart = ep._text(ep._get(first, "DepartID"))
    g_style, g_delay = int(ep._get(first, "GatherStyle", 0) or 0), int(ep._get(first, "GatherDelay", 0) or 0)
    c_style, c_delay = int(ep._get(first, "CheckStyle", 0) or 0), int(ep._get(first, "CheckDelay", 0) or 0)
    terms = calc_terms(rm_date, int(ep._get(ctx["trade"], "DayOfClose", 0) or 0), g_style, g_delay, c_style, c_delay)

    warnings: List[Dict[str, Any]] = []
    over: List[Dict[str, Any]] = []
    lines: List[Dict[str, Any]] = []
    for i, p in enumerate(picked, 1):
        sub = p["sub"]
        qty = p["qty"]
        remain = _dec(ep._get(sub, "QtyRemain"))
        po_price = _dec(ep._get(sub, "Price"))
        price = p["price"] if p["price"] is not None else po_price
        discount = _dec(ep._get(sub, "Discount", 1))
        tax_rate = _dec(ep._get(sub, "TaxRate", 0))
        amount, tax = calc_line_amounts(qty, price, discount, tax_rate)
        line = {
            "row_no": i, "po_no": p["po_no"], "po_row": p["row_no"],
            "prod_id": ep._text(ep._get(sub, "ProdID")), "prod_name": ep._text(ep._get(sub, "ProdName")),
            "unit": ep._text(ep._get(sub, "Unit")), "qty": qty, "qty_remain": remain, "price": price,
            "po_price": po_price, "discount": discount if discount > 0 else Decimal(1), "tax_rate": tax_rate,
            "amount": amount, "tax": tax,
        }
        lines.append(line)
        if qty > remain:
            over.append({"line_no": i, "po_no": p["po_no"], "row_no": p["row_no"], "part_number": line["prod_id"],
                         "qty": float(qty), "qty_remain": float(remain), "over": float(qty - remain)})
    if over:
        warnings.append({"code": "over_receive", "needs_confirm": True, "items": over,
                         "message": "收料量超過採購單剩餘量：" + "、".join(
                             f"{o['po_no']}#{o['row_no']} {o['part_number']} 超收 {o['over']:g}" for o in over)})
    diff = [l for l in lines if l["price"] != l["po_price"]]
    if diff:
        warnings.append({"code": "price_diff", "needs_confirm": False,
                         "message": "單價與採購單不同：" + "、".join(
                             f"{l['po_no']}#{l['po_row']} {l['po_price']:g}→{l['price']:g}" for l in diff)})
    if rm_date != today:
        warnings.append({"code": "rm_date_not_today", "needs_confirm": False,
                         "message": f"進貨日期（{rm_date}）不是今天，收料 / 驗收仍用今天（{today}）"})
    term_keys = {(int(ep._get(p["main"], "GatherStyle", 0) or 0), int(ep._get(p["main"], "GatherDelay", 0) or 0),
                  int(ep._get(p["main"], "CheckStyle", 0) or 0), int(ep._get(p["main"], "CheckDelay", 0) or 0))
                 for p in picked}
    if len(term_keys) > 1:
        warnings.append({"code": "terms_mismatch", "needs_confirm": False,
                         "message": "各採購單付款條件不同，進貨單以第一張採購單的條件為準"})

    sum_qty = sum((l["qty"] for l in lines), Decimal(0))
    sum_amount = sum((l["amount"] for l in lines), Decimal(0))
    sum_tax = sum((l["tax"] for l in lines), Decimal(0))
    total = sum_amount + sum_tax
    totals = {"sum_qty": sum_qty, "sum_amount": sum_amount, "sum_tax": sum_tax, "total": total}

    by_prod: Dict[str, Dict[str, Decimal]] = {}
    for l in lines:
        agg = by_prod.setdefault(l["prod_id"], {"qty": Decimal(0), "amount": Decimal(0)})
        agg["qty"] += l["qty"]
        agg["amount"] += l["amount"]
    costs = _cost_plan(ctx, by_prod, warnings, update_cost)
    if not update_cost:
        warnings.append({"code": "cost_not_updated", "needs_confirm": False,
                         "message": "本次不更新平均成本（CAvgCost / CTotalCost / 倉庫成本不動，庫存量與最近進貨日照常更新）"})
    cost_params = ctx["cost_params"]

    inserts: List[Tuple[str, Dict[str, Any]]] = []
    # 收料 / 驗收（Flag 5 / 6）
    for flag, no in ((5, im_no), (6, qm_no)):
        inserts.append(("ordExamineMain", _row(
            "ordExamineMain", Flag=flag, ExamineNO=no, ExamineDate=today, CustomerID=vendor_id, Examiner=salesman,
            Maker=maker, MakerID=maker_id, HasCheck=True, DepartID=depart)))
        for l in lines:
            if flag == 5:
                tran, from_no, from_row = 304, l["po_no"], l["po_row"]
            else:
                tran, from_no, from_row = 306, im_no, l["row_no"]
            inserts.append(("ordExamineSub", _row(
                "ordExamineSub", Flag=flag, ExamineNO=no, RowNO=l["row_no"], SerNO=l["row_no"], ExamineDate=today,
                ProdID=l["prod_id"], ProdName=l["prod_name"], TranType=tran, FromNO=from_no, FromRow=from_row,
                Quantity=l["qty"], QtyRemain=0, Unit=l["unit"], ExmineType=EXAMINE_TYPE, HasCheck=True,
                sQuantity=l["qty"], srcFrom=304, EQuantity=l["qty"], IsOdd=True)))
    # 進貨（Flag 100）
    inserts.append(("stkBillMain", _row(
        "stkBillMain", Flag=100, BillNO=rm_no, SumAmtATax=total, SumNum=sum_qty, GatherStyle=g_style,
        GatherDelay=g_delay, CheckStyle=c_style, CheckDelay=c_delay,
        Detail=ctx["bill_params"]["detail"], BillStatus=ctx["bill_params"]["bill_status"],
        GatherOther=ep._text(ep._get(first, "GatherOther")), CheckOther=ep._text(ep._get(first, "CheckOther")))))
    for l in lines:
        inserts.append(("stkBillSub", _row(
            "stkBillSub", Flag=100, BillNO=rm_no, RowNO=l["row_no"], SerNO=l["row_no"], StkMlAmount=l["amount"],
            LocalPayment=l["amount"] if STK_SUB_LOCAL_PAYMENT_EQUALS_AMOUNT else 0)))
    inserts.append(("comBillAccounts", _row(
        "comBillAccounts", Flag=100, SysID=2, FundBillNo=rm_no, BillDate=rm_date, CustFlag=2, CustID=vendor_id,
        DueTo=vendor_id, AccMonth=terms["AccMonth"], CurrID=curr, ExchRate=exch, Total=sum_amount, Tax=sum_tax,
        LocalTotal=ep._round(sum_amount * exch, 0), LocalTax=ep._round(sum_tax * exch, 0),
        PrepayDay=terms["PrepayDay"], EncashDay=terms["EncashDay"], DeptID=depart, Status=1,
        InvoStyle=int(ep._get(ctx["trade"], "InvoiceStyle", 1) or 0), Salesman=salesman,
        CashPayStyle="001", VisaPayStyle="002", HasCheck=True, CostFlag=100,
        PriceOfTax=bool(ep._get(first, "PriceOfTax", False)),
        AddrID=ep._text(ep._get(first, "AddressID")), ZipCode=ep._text(ep._get(first, "ZipCode")),
        CustAddress=ep._text(ep._get(first, "CustAddress")),
        ContactPerson=ep._text(ep._get(first, "LinkMan")), LinkManProf=ep._text(ep._get(first, "LinkManProf")),
        ContactPhone=ep._text(ep._get(first, "LinkTelephone")), FaxNo=ep._text(ep._get(first, "FaxNo")),
        MakerID=maker_id, Maker=maker, InvoiceNo="")))
    for l in lines:
        inserts.append(("comProdRec", _row(
            "comProdRec", Flag=100, BillNO=rm_no, RowNO=l["row_no"], SerNO=l["row_no"], BillDate=rm_date,
            ProdID=l["prod_id"], ProdName=l["prod_name"], CostAvg=costs[l["prod_id"]]["cost"], Quantity=l["qty"],
            MLPrice=l["price"], MLAmount=l["amount"], WareID=ware, QtyRemain=l["qty"], TranType=205,
            FromNO=qm_no, FromRow=l["row_no"], TaxRate=l["tax_rate"], TaxAmt=l["tax"], NeedUpdate=True,
            CostForAcc=l["amount"], HasCheck=True, Price=l["price"], DisCount=l["discount"], Amount=l["amount"],
            SPrice=l["price"], SQuantity=l["qty"], UnitRelation=1, EQuantity=l["qty"], MLTaxAmt=l["tax"],
            IsOdd=True)))

    updates: List[Dict[str, Any]] = []
    # 採購單回寫：剩餘量不低於 0；全部列都為 0 才結案（EndacaseDate = 做收料的今天，Comp03 鏈進貨日在未來時也是如此）
    received: Dict[Tuple[str, int], Decimal] = {}
    for l in lines:
        received[(l["po_no"], l["po_row"])] = received.get((l["po_no"], l["po_row"]), Decimal(0)) + l["qty"]
    closed_pos: List[str] = []
    for po_no, po in ctx["pos"].items():
        all_zero = True
        for row_no, sub in po["subs"].items():
            remain = _dec(ep._get(sub, "QtyRemain"))
            if (po_no, row_no) in received:
                new = max(Decimal(0), remain - received[(po_no, row_no)])
                updates.append({"op": "update", "table": "ordBillSub", "set": {"QtyRemain": new},
                                "where": {"Flag": 4, "BillNO": po_no, "RowNO": row_no}})
                remain = new
            if remain > 0:
                all_zero = False
        if all_zero:
            closed_pos.append(po_no)
            updates.append({"op": "update", "table": "ordBillMain", "set": {"BillStatus": 1, "EndacaseDate": today},
                            "where": {"Flag": 4, "BillNO": po_no}})

    ym = rm_date // 100
    avg_ware = ware if cost_params["cost_by_ware"] else ""  # 全公司成本：月平均表 WareID 為空字串
    # 進貨「不」更新 comProduct.LatestIndate / comWareAmount.LatestInDate（那是調撥 Flag400 的入庫日；Andy 的手動鏈也沒動它）
    for prod, agg in by_prod.items():
        q, amt = agg["qty"], agg["amount"]
        wa = ctx["ware_amounts"].get(prod)
        c = costs[prod]
        if wa is None:
            ware_cost = c["ware_cost"] if cost_params["cost_by_ware"] else 0
            updates.append({"op": "insert", "table": "comWareAmount", "values": _row(
                "comWareAmount", ProdID=prod, WareID=ware, Cost=ware_cost or 0, Quantity=q,
                TotalCost=amt if update_cost else 0, LatestPurchDate=rm_date, BCQty=q)})
        else:
            ware_set = {"Quantity": _dec(ep._get(wa, "Quantity")) + q,
                        "BCQty": _dec(ep._get(wa, "BCQty")) + q, "LatestPurchDate": rm_date}
            if update_cost:
                ware_set["TotalCost"] = _dec(ep._get(wa, "TotalCost")) + amt
            if cost_params["cost_by_ware"] and c["ware_cost"] is not None:
                ware_set["Cost"] = c["ware_cost"]
            updates.append({"op": "update", "table": "comWareAmount", "where": {"ProdID": prod, "WareID": ware},
                            "set": ware_set})
        prod_set: Dict[str, Any] = {"LatestPurchDate": rm_date}
        if update_cost:
            prod_set["CTotalCost"] = _dec(ep._get(ctx["products"][prod], "CTotalCost")) + amt
        if c["avg"] is not None:
            prod_set["CAvgCost"] = c["avg"]
        updates.append({"op": "update", "table": "comProduct", "where": {"ProdID": prod}, "set": prod_set})
        # 只更新進貨日當月；進貨日早於當月時之後月份的期末量不連動（正航月結時重算）
        yq = ctx["ym_qty"].get(prod)
        if yq is None:
            updates.append({"op": "insert", "table": "StkYearMonthQty", "values": _row(
                "StkYearMonthQty", YearMonth=ym, ProdID=prod, WareID=ware, Quantity=q)})
        else:
            updates.append({"op": "update", "table": "StkYearMonthQty",
                            "where": {"YearMonth": ym, "ProdID": prod, "WareID": ware},
                            "set": {"Quantity": _dec(ep._get(yq, "Quantity")) + q}})
        ya = ctx["ym_avg"].get(prod)
        if ya is None:
            updates.append({"op": "insert", "table": "stkYearMonthAvg", "values": _row(
                "stkYearMonthAvg", YearMonth=ym, ProdID=prod, WareID=avg_ware, BatchID="", AtLastQuan=q)})
        else:
            updates.append({"op": "update", "table": "stkYearMonthAvg",
                            "where": {"YearMonth": ym, "ProdID": prod, "WareID": avg_ware, "BatchID": ""},
                            "set": {"AtLastQuan": _dec(ep._get(ya, "AtLastQuan")) + q}})
    updates.append({"op": "update", "table": "comCustTrade", "where": {"Flag": 2, "ID": vendor_id},
                    "set": {"LatelyTradeDate": rm_date}})
    # comCustAccounts：inferred from Andy's chain (CS007 2520) —— 應付 = 含稅總額
    local_total = ep._round(total * exch, 0)
    acc = ctx["accounts"]
    if acc is None:
        updates.append({"op": "insert", "table": "comCustAccounts", "values": _row(
            "comCustAccounts", Flag=2, SysID=2, ID=vendor_id, CurrID=curr, StartExchRate=exch,
            StartAdvExchRate=exch, StartCommExchRate=exch, CurReceivable=total, LocCurReceivable=local_total)})
    else:
        updates.append({"op": "update", "table": "comCustAccounts",
                        "where": {"Flag": 2, "SysID": 2, "ID": vendor_id, "CurrID": curr},
                        "set": {"CurReceivable": _dec(ep._get(acc, "CurReceivable")) + total,
                                "LocCurReceivable": _dec(ep._get(acc, "LocCurReceivable")) + local_total}})

    # 異動紀錄（照 Andy 鏈：OrdGet / OrdExamIn / Buy 各一筆 Action 1，每張被結案的採購單再一筆 Action 9）
    def log(prog: str, pk: str, action: int, name: str = "") -> Tuple[str, Dict[str, Any]]:
        return ("comChangeLog", _row(
            "comChangeLog", ProgID=prog, ChangeTime=_ole_date(now), IPAddr=(client_ip or "")[:15], UserName=maker_id,
            PKValue=pk, Action=action, ComputerName=socket.gethostname()[:20], WinUser=(actor or "web")[:20],
            Flag=0, OldValue="", WinLang=1, IsChiDataSet=False, ActionName=name))

    logs = [log("ChiOrder.OrdGet", im_no, 1), log("ChiOrder.OrdExamIn", qm_no, 1), log("ChiStock.Buy", rm_no, 1)]
    logs += [log("CHIOrder.Purchase", po, 9, f"進貨憑單{rm_no}過賬單況為已結案") for po in closed_pos]

    return {
        "target": target, "numbers": numbers, "today": today, "rm_date": rm_date, "terms": terms,
        "vendor": {"id": vendor_id, "name": ctx["vendor_name"]}, "currency": curr, "warehouse": ware,
        "lines": lines, "totals": totals, "warnings": warnings, "over_receive": over,
        "cost_params": cost_params, "bill_params": ctx["bill_params"], "update_cost": update_cost,
        "needs_confirm": bool(over), "closed_pos": closed_pos, "inserts": inserts, "updates": updates,
        "change_logs": logs,
    }


# ── 組 SQL / 寫入 / 讀回 ─────────────────────────────────────────────────────

def build_update(table: str, set_values: Dict[str, Any], where: Dict[str, Any]) -> Tuple[str, Tuple[Any, ...]]:
    sql = (f"UPDATE dbo.{table} SET {', '.join(f'[{k}] = %s' for k in set_values)} "
           f"WHERE {' AND '.join(f'[{k}] = %s' for k in where)}")
    return sql, (*set_values.values(), *where.values())


def _execute_plan(wcur, plan: Dict[str, Any]) -> None:
    columns: Dict[str, List[Dict[str, Any]]] = {}

    def insert(table: str, values: Dict[str, Any]) -> None:
        if table not in columns:
            columns[table] = ep.table_columns(wcur, table)
        wcur.execute(*ep.build_insert(table, columns[table], values))

    for table, values in plan["inserts"]:
        insert(table, values)
    for u in plan["updates"]:
        if u["op"] == "insert":
            insert(u["table"], u["values"])
        else:
            sql, params = build_update(u["table"], u["set"], u["where"])
            wcur.execute(sql, params)
            if getattr(wcur, "rowcount", 1) == 0:
                raise ep.PoSchemaError(f"更新 {u['table']} 沒有命中任何列，已 rollback")
    for table, values in plan["change_logs"]:
        insert(table, values)


def _verify(wcur, plan: Dict[str, Any]) -> Dict[str, Any]:
    n = plan["numbers"]
    lines = plan["lines"]
    t = plan["totals"]

    def rows(sql: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
        wcur.execute(sql, params)
        return ep._rows(wcur)

    def fail(what: str):
        raise ep.PoSchemaError(f"寫入後讀回的{what}與預期不符，已 rollback")

    for flag, no in ((5, n["im_no"]), (6, n["qm_no"])):
        if not rows("SELECT ExamineNO FROM ordExamineMain WHERE Flag = %s AND ExamineNO = %s", (flag, no)):
            fail(f"{'收料' if flag == 5 else '驗收'}單表頭")
        subs = rows("SELECT RowNO, ProdID, Quantity, QtyRemain, FromNO, FromRow FROM ordExamineSub "
                    "WHERE Flag = %s AND ExamineNO = %s ORDER BY RowNO", (flag, no))
        if len(subs) != len(lines):
            fail("收料 / 驗收明細行數")
        for got, l in zip(subs, lines):
            src_no, src_row = (l["po_no"], l["po_row"]) if flag == 5 else (n["im_no"], l["row_no"])
            if not (int(ep._get(got, "RowNO")) == l["row_no"] and ep._text(ep._get(got, "ProdID")) == l["prod_id"]
                    and ep._same(ep._get(got, "Quantity"), l["qty"]) and ep._same(ep._get(got, "QtyRemain"), 0)
                    and ep._text(ep._get(got, "FromNO")) == src_no and int(ep._get(got, "FromRow")) == src_row):
                fail(f"第 {l['row_no']} 行{'收料' if flag == 5 else '驗收'}明細")
    head = rows("SELECT SumAmtATax, SumNum FROM stkBillMain WHERE Flag = 100 AND BillNO = %s", (n["rm_no"],))
    if not head or not ep._same(ep._get(head[0], "SumAmtATax"), t["total"]) or not ep._same(ep._get(head[0], "SumNum"), t["sum_qty"]):
        fail("進貨單合計")
    if len(rows("SELECT RowNO FROM stkBillSub WHERE Flag = 100 AND BillNO = %s", (n["rm_no"],))) != len(lines):
        fail("進貨單明細行數")
    acct = rows("SELECT Total, Tax, AccMonth, PrepayDay, EncashDay FROM comBillAccounts "
                "WHERE Flag = 100 AND SysID = 2 AND FundBillNo = %s", (n["rm_no"],))
    terms = plan["terms"]
    if not acct or not ep._same(ep._get(acct[0], "Total"), t["sum_amount"]) or not ep._same(ep._get(acct[0], "Tax"), t["sum_tax"]) \
            or int(ep._get(acct[0], "AccMonth")) != terms["AccMonth"] or int(ep._get(acct[0], "PrepayDay")) != terms["PrepayDay"] \
            or int(ep._get(acct[0], "EncashDay")) != terms["EncashDay"]:
        fail("應付帳款")
    recs = rows("SELECT RowNO, ProdID, Quantity, Amount, TaxAmt, WareID, FromNO, FromRow FROM comProdRec "
                "WHERE Flag = 100 AND BillNO = %s ORDER BY RowNO", (n["rm_no"],))
    if len(recs) != len(lines):
        fail("進貨明細行數")
    for got, l in zip(recs, lines):
        if not (ep._text(ep._get(got, "ProdID")) == l["prod_id"] and ep._same(ep._get(got, "Quantity"), l["qty"])
                and ep._same(ep._get(got, "Amount"), l["amount"]) and ep._same(ep._get(got, "TaxAmt"), l["tax"])
                and ep._text(ep._get(got, "WareID")) == plan["warehouse"]
                and ep._text(ep._get(got, "FromNO")) == n["qm_no"] and int(ep._get(got, "FromRow")) == l["row_no"]):
            fail(f"第 {l['row_no']} 行進貨明細")
    for u in plan["updates"]:
        if u["op"] == "update" and u["table"] in ("ordBillSub", "ordBillMain"):
            cond = " AND ".join(f"{k} = %s" for k in u["where"])
            got = rows(f"SELECT {', '.join(u['set'])} FROM {u['table']} WHERE {cond}", tuple(u["where"].values()))
            if not got or any(not ep._same(ep._get(got[0], k), v) for k, v in u["set"].items()):
                fail("採購單回寫")
    return {
        "ordExamineMain": 2, "ordExamineSub": 2 * len(subs), "stkBillMain": 1, "stkBillSub": len(lines),
        "comBillAccounts": 1, "comProdRec": len(recs), "po_updated": len(plan["updates"]),
    }


# ── 序列化 ────────────────────────────────────────────────────────────────────

def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _changed_columns(table: str, values: Dict[str, Any]) -> Dict[str, Any]:
    base = schema.DEFAULTS[table]
    return {k: _jsonable(v) for k, v in values.items() if base.get(k) != v}


def _summary(plan: Dict[str, Any], *, include_rows: bool) -> Dict[str, Any]:
    target = plan["target"]
    out = {
        "target": target, "target_label": ep.target_label(target), "is_test": ep.is_test(target),
        "is_production": target == ep.PRODUCTION_TARGET, "update_cost": plan["update_cost"],
        **plan["numbers"], "today": plan["today"], "rm_date": plan["rm_date"], "terms": plan["terms"],
        "vendor": plan["vendor"], "currency": plan["currency"], "warehouse": plan["warehouse"],
        "lines": _jsonable([
            {k: l[k] for k in ("row_no", "po_no", "po_row", "prod_id", "prod_name", "unit", "qty", "qty_remain",
                               "price", "po_price", "tax_rate", "amount", "tax")} for l in plan["lines"]]),
        "totals": _jsonable(plan["totals"]),
        "cost_params": plan["cost_params"], "bill_params": plan["bill_params"], "warnings": plan["warnings"], "needs_confirm": plan["needs_confirm"], "closed_pos": plan["closed_pos"],
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
    today = date_to_int(now.date())
    rm_date = parse_rm_date(req["rm_date_raw"], today)
    ctx = _load_context(wcur, target, req, lock)
    _load_rollup_state(wcur, ctx, rm_date // 100)
    prefixes = number_prefixes(target)
    serial = next_receive_serial(wcur, prefixes, (today, rm_date), lock)
    return build_plan(
        ctx, target=target, today=today, rm_date=rm_date, numbers=make_numbers(prefixes, today, rm_date, serial),
        maker=ep._env("ERP_PO_MAKER", ep.DEFAULT_MAKER), maker_id=ep._env("ERP_PO_MAKER_ID", ep.DEFAULT_MAKER_ID),
        client_ip=client_ip, actor=actor, now=now, update_cost=req.get("update_cost", True))


def preview_receive(raw_req: Dict[str, Any]) -> Dict[str, Any]:
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


def _fingerprint(target: str, req: Dict[str, Any], rm_date_raw: Any) -> str:
    lines = sorted([l["po_no"], l["row_no"], str(l["qty"]), str(l["price"])] for l in req["lines"])
    raw = json.dumps([target, str(rm_date_raw or ""), lines, bool(req.get("update_cost", True))], ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _write(req: Dict[str, Any], target: str, creds: Dict[str, Any], actor: str, client_ip: str) -> Dict[str, Any]:
    with ep._write_lock:
        wconn = ep._connect_write(creds)
        committed = False
        plan: Dict[str, Any] = {}
        try:
            wcur = wconn.cursor()
            plan = _plan(wcur, target, req, lock=True, client_ip=client_ip, actor=actor)
            if plan["needs_confirm"] and not req["confirm"]:
                raise OverReceiveWarning(plan["over_receive"])  # 尚未寫入任何東西
            _execute_plan(wcur, plan)
            readback = _verify(wcur, plan)
            ep._check_db_name(wcur, target)
            try:
                wconn.commit()
                committed = True
            except Exception as exc:
                raise ErpUnavailable(
                    f"commit 失敗，結果不確定，請到正航確認是否已有單號 {plan['numbers']['rm_no']}："
                    f"{ep._safe_message(exc, creds)}") from None
        except ErpError:
            raise
        except Exception as exc:
            raise ErpUnavailable(f"寫入正航失敗，已 rollback：{ep._safe_message(exc, creds)}") from None
        finally:
            ep._close_quietly(wconn, rollback=not committed)
    return {**_summary(plan, include_rows=False), "readback": readback}


def create_receive(raw_req: Dict[str, Any], actor: str = "web", client_ip: str = "") -> Dict[str, Any]:
    """建立收料 / 驗收 / 進貨並記錄本機紀錄（成功與失敗都記）。超收只是警告：帶 confirm=true 即寫入（剩餘量扣到 0）。"""
    from .. import database as db

    req = normalize_request(raw_req)
    target, creds = resolve_target(req["target"])
    fp = _fingerprint(target, req, req["rm_date_raw"])
    if req["client_token"]:
        prior = db.find_erp_receipt_by_token(req["client_token"])
        if prior:
            raise ep.DuplicateSubmitError(f"此次收料已建立（進貨單 {prior['rm_no']}），已略過重複送出")
    recent = db.find_recent_erp_receipt(fp, (local_now() - timedelta(seconds=IDEMPOTENCY_SECONDS)).isoformat(timespec="seconds"))
    if recent:
        raise ep.DuplicateSubmitError(f"相同內容的收料剛建立過（進貨單 {recent['rm_no']}），已略過重複送出")
    if not po_duplicates.claim("rcv:" + fp):
        raise ep.DuplicateSubmitError("相同內容的收料正在建立中，請勿重複送出")
    try:
        base = {
            "target_db": target, "actor": actor, "client_ip": client_ip, "client_token": req["client_token"],
            "fingerprint": fp,
            "lines_json": json.dumps([{"po_no": l["po_no"], "row_no": l["row_no"], "qty": str(l["qty"]),
                                       "price": None if l["price"] is None else str(l["price"])}
                                      for l in req["lines"]], ensure_ascii=False),
        }
        try:
            result = _write(req, target, creds, actor, client_ip)
        except OverReceiveWarning:
            raise
        except Exception as exc:
            message = ep._safe_message(exc, creds)
            _safe_log(db, {**base, "status": "error", "error": message})
            db.log_activity("erp_receive_failed", f"{ep.target_label(target)}：{message}")
            raise
        t = result["totals"]
        _safe_log(db, {
            **base, "im_no": result["im_no"], "qm_no": result["qm_no"], "rm_no": result["rm_no"],
            "rm_date": result["rm_date"], "vendor_id": result["vendor"]["id"], "vendor_name": result["vendor"]["name"],
            "lines_json": json.dumps(result["lines"], ensure_ascii=False), "sum_qty": t["sum_qty"],
            "sum_amount": t["sum_amount"], "sum_tax": t["sum_tax"], "total": t["total"], "status": "ok"})
        db.log_activity(
            "erp_receive_created",
            f"{ep.target_label(target)} 收料 {result['im_no']} / 驗收 {result['qm_no']} / 進貨 {result['rm_no']}，"
            f"廠商 {result['vendor']['id']}，{len(result['lines'])} 行，含稅合計 {t['total']:.0f}"
            + ("" if result["update_cost"] else "（未更新平均成本）"))
        return {"ok": True, **result}
    finally:
        po_duplicates.release("rcv:" + fp)


def _safe_log(db, rec: Dict[str, Any]) -> None:
    try:
        db.add_erp_receipt(rec)
    except Exception:
        pass  # 本機紀錄失敗不可蓋掉已完成的 ERP 結果


def list_history(limit: int = 50) -> List[Dict[str, Any]]:
    from .. import database as db

    return db.list_erp_receipts(limit)
