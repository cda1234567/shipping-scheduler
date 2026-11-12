"""低庫存提醒 v2026.11.11.7：用廠商交期設定判斷、新可用量（含 015 未領需求）、推定廠商、待確認區。

依據（/workspace/shipping-rules/andy-rules.md）：R-B1~B9、R-D1、R-A2/A3、R-I8、總指揮 19:05／19:24／19:30／19:47 決定、
Andy 22:03 廠商交期定案。

母體：主檔料號、料號不含 -TAB、近 365 天 ST(001) 或 015 有領用（> 0）。不用任何頻率／張數／門檻篩料。
主清單（任一成立）：
  a. 可用 ≤ 0
  b. 可用 < 已排程需求（status='pending' 訂單的 BOM 需求）
  c. 有有效交期，且 可撐天數 < 交期天數；可撐天數 = (可用 − 已排程需求) / 日均，日均 = max(ST, 015 一年領用) / 365
其餘：可隨時下單的不列；沒有有效交期的進「資料待確認」；有交期且撐得到的不列（只計數）。
有效交期 = 料號可隨時下單 > 料號覆蓋交期 > 廠商設定 > 廠商預設（洽智、萬晉、積禾、崧嵐 60 天，其他廠商可隨時下單）> 無。
不准預設 7 天；PerDays、PO 預交日、下單到收貨天數、舊 lead_time_days 等一律不當交期（PerDays 只顯示參考）。
廠商歸屬：料號手動改的廠商 > 正航 comProduct.MajorSupplier > 最近一張進貨的廠商（推定）。

手動新增料號（R-B9）：Andy 自己加的料一律進清單判斷（即使不在主檔母體或一年沒領用），套用同一套可用量與交期；
料號含 -TAB 一律不收。開採購單建議量（R-E2/E3）＝缺料量照 MOQ（近 10 次進貨最大公因數）往上取整，不多買。

資料：refresh_snapshot() 從正航唯讀查詢 + 本機資料算好每顆料的數字存 low_stock_plan_snapshot（ST 庫存同步後
由 low_stock.refresh_priority_parts 一併呼叫）；build_plan() 只讀本機快照與設定做分類，不連正航。
"""
from __future__ import annotations

import json
import logging
import math
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .. import database as db
from . import available_qty as aq
from . import erp_stock, low_stock, part_conversion
from . import moq as moq_service
from .erp_stock import ErpError
from .local_time import local_now

logger = logging.getLogger("low_stock_plan")

META_KEY = "low_stock_plan_meta"
LOOKBACK_DAYS = 365  # R-B7
LEAD_MIN, LEAD_MAX = 0, 730
# Andy 22:03：這四家「至少都1-2個月以上」→ 預設 60 天；其他廠商預設可隨時下單
LONG_LEAD_SUPPLIERS: Tuple[str, ...] = ("洽智", "萬晉", "積禾", "崧嵐")
LONG_LEAD_DEFAULT_DAYS = 60

AREA_MAIN = "main"
AREA_CHECK = "check"
AREA_OK = "ok"
AREA_ANYTIME = "anytime"

PRIORITY_TAG = "萬晉/崧嵐 趕交期注意"
GL_TAG = "015 可能含已發料未領用（Q12 資料待確認）"
OVERDUE_TAG = "在途逾期（資料待確認）"
PENDING_OVER_TAG = "待調撥 > 001 現有（資料待確認）"
MANUAL_TAG = "手動新增"
NO_DATA_REASON = "手動新增，尚未抓到正航資料，請按「重新抓正航」"


class LeadSettingError(ValueError):
    """設定輸入錯誤；訊息為繁體中文。"""


# ── 小工具 ─────────────────────────────────────────────────────────────────

def _num(value: Any) -> float:
    try:
        n = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(n) or math.isinf(n) else n


def fmt_qty(value: Any) -> str:
    n = round(_num(value), 2)
    return str(int(n)) if n == int(n) else f"{n:g}"


def fmt_days(value: Optional[float]) -> str:
    return "—" if value is None else f"{value:.1f}"


def fmt_date_full(text: Any) -> str:
    """'2026-11-26' → '2026/11/26'；格式不對照原樣。"""
    s = str(text or "").strip()
    return s[:10].replace("-", "/") if len(s) >= 10 else s


def _parse_date(text: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(text or "").strip()[:10])
    except ValueError:
        return None


def is_long_lead_supplier(*names: Any) -> bool:
    return any(low_stock.match_priority_supplier(n, LONG_LEAD_SUPPLIERS) for n in names if n)


def validate_lead(value: Any) -> Optional[int]:
    """整數 0~730 或空（None / ""）= 清除。"""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    if isinstance(value, bool):
        raise LeadSettingError("交期需為 0 到 730 的整數天數")
    try:
        num = float(str(value).strip())
    except (TypeError, ValueError):
        raise LeadSettingError("交期需為 0 到 730 的整數天數") from None
    if math.isnan(num) or num != int(num) or not (LEAD_MIN <= num <= LEAD_MAX):
        raise LeadSettingError("交期需為 0 到 730 的整數天數")
    return int(num)


# ── 廠商歸屬與有效交期（純函式） ───────────────────────────────────────────

def resolve_supplier(payload: Dict[str, Any], part_setting: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    """(廠商代碼, 來源)：manual = 料號手動改；major = 正航主要廠商；inferred = 最近進貨推定；'' = 都沒有。"""
    override = str((part_setting or {}).get("supplier_override") or "").strip()
    if override:
        return override, "manual"
    major = str(payload.get("major_supplier_id") or "").strip()
    if major:
        return major, "major"
    last = str(payload.get("last_supplier_id") or "").strip()
    if last:
        return last, "inferred"
    return "", ""


def resolve_lead(supplier_id: str, supplier_names: Tuple[str, str], supplier_row: Optional[Dict[str, Any]],
                 part_setting: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """{lead_days, anytime, source, text}。lead_days 為 None 且 anytime=False → 沒有有效交期。"""
    ps = part_setting or {}
    if int(ps.get("anytime") or 0):
        return {"lead_days": None, "anytime": True, "source": "part_anytime", "text": "料號設定：可隨時下單"}
    if ps.get("lead_days") is not None:
        return {"lead_days": int(ps["lead_days"]), "anytime": False, "source": "part", "text": "料號設定"}
    if supplier_row:
        if int(supplier_row.get("anytime") or 0):
            return {"lead_days": None, "anytime": True, "source": "supplier_anytime", "text": "廠商設定：可隨時下單"}
        if supplier_row.get("lead_days") is not None:
            return {"lead_days": int(supplier_row["lead_days"]), "anytime": False, "source": "supplier", "text": "廠商設定"}
    if not supplier_id:
        return {"lead_days": None, "anytime": False, "source": "none", "text": "無廠商"}
    if is_long_lead_supplier(*supplier_names):
        return {"lead_days": LONG_LEAD_DEFAULT_DAYS, "anytime": False, "source": "supplier_default",
                "text": f"廠商預設 {LONG_LEAD_DEFAULT_DAYS} 天"}
    return {"lead_days": None, "anytime": True, "source": "supplier_default_anytime", "text": "廠商預設：可隨時下單"}


# ── 分類（純函式） ─────────────────────────────────────────────────────────

def classify(row: Dict[str, Any], today: date) -> Dict[str, Any]:
    """依可用、已排程需求、可撐天數與有效交期決定區域與依據文字（就地補欄位並回傳）。"""
    avail = _num(row["available"])
    need = _num(row["need"])
    daily = _num(row["daily"])
    cover = None if daily <= 0 else round((avail - need) / daily, 1)
    row["cover_days"] = cover
    lead = row.get("lead_days")
    reasons: List[str] = []
    if avail <= 0:
        reasons.append(f"可用 {fmt_qty(avail)} ≤ 0")
    if avail < need:
        reasons.append(f"可用 {fmt_qty(avail)} < 已排程需求 {fmt_qty(need)}（缺 {fmt_qty(need - avail)}）")
    if lead is not None and not row.get("anytime") and cover is not None and cover < lead:
        reasons.append(f"可撐 {fmt_days(cover)} 天 < 交期 {lead} 天（來源：{row.get('lead_text') or ''}）")
    if reasons:
        area = AREA_MAIN
    elif row.get("anytime"):
        area = AREA_ANYTIME
    elif lead is None:
        area = AREA_CHECK
        reasons.append(f"無廠商交期，無法判斷能否撐到交期；可撐 {fmt_days(cover)} 天")
    else:
        area = AREA_OK
    row["area"] = area
    row["reasons"] = reasons

    tags: List[str] = []
    if row.get("priority_supplier"):
        tags.append(PRIORITY_TAG)
    if _num(row.get("gl015")) > 0:
        tags.append(GL_TAG)
    on_order = _num(row.get("on_order"))
    eta = _parse_date(row.get("eta"))
    row["ordered"] = on_order > 0
    if on_order > 0:
        tags.append(f"已下單 {fmt_qty(on_order)}" + (f"（預交 {fmt_date_full(row.get('eta'))}）" if row.get("eta") else ""))
        if eta and eta < today:
            tags.append(OVERDUE_TAG)
    if row.get("pending_over_st"):
        tags.append(PENDING_OVER_TAG)
    row["tags"] = tags
    return row


def sort_key_main(row: Dict[str, Any]) -> tuple:
    """未下單在前；同組依 (可用 − 需求) 由少到多。"""
    return (bool(row.get("ordered")), _num(row["available"]) - _num(row["need"]), row["part"])


def sort_key_check(row: Dict[str, Any]) -> tuple:
    """已下單排最後；同組照可撐天數由少到多（沒有天數的排後面）。"""
    cover = row.get("cover_days")
    return (bool(row.get("ordered")), cover is None, cover if cover is not None else 0, row["part"])


# ── 正航唯讀查詢（refresh 用） ─────────────────────────────────────────────

def _query_products(conn, parts: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    cur = conn.cursor()
    for batch in low_stock._chunks(parts):
        cur.execute(
            "SELECT ProdID, ProdName, PerDays, MajorSupplier FROM comProduct WHERE ProdID IN "
            + low_stock._in_clause(len(batch)), tuple(batch),
        )
        for prod_id, name, per_days, major in cur.fetchall():
            part = aq.norm_part(prod_id)
            if part:
                out[part] = {"name": str(name or "").strip(), "perdays": _num(per_days),
                             "major_supplier_id": str(major or "").strip()}
    return out


def _query_suppliers(conn) -> Dict[str, Tuple[str, str]]:
    cur = conn.cursor()
    cur.execute("SELECT ID, ShortName, FullName FROM comCustomer WHERE Flag = 2")
    return {str(i or "").strip(): (str(s or "").strip(), str(f or "").strip())
            for i, s, f in cur.fetchall() if str(i or "").strip()}


def _query_last_suppliers(conn, parts: List[str]) -> Dict[str, Dict[str, str]]:
    """最近一張進貨（comProdRec Flag=100，不限倉）的廠商（與 low_stock._query_priority_parts 同口徑）。"""
    out: Dict[str, Dict[str, str]] = {}
    cur = conn.cursor()
    for batch in low_stock._chunks(parts):
        cur.execute(
            "SELECT t.ProdID, t.BillDate, a.CustID FROM ("
            "SELECT ProdID, BillDate, BillNO, ROW_NUMBER() OVER ("
            "PARTITION BY ProdID ORDER BY BillDate DESC, BillNO DESC) AS rn "
            f"FROM comProdRec WHERE Flag = 100 AND ProdID IN {low_stock._in_clause(len(batch))}) t "
            "JOIN comBillAccounts a ON a.Flag = 100 AND a.FundBillNo = t.BillNO WHERE t.rn = 1",
            tuple(batch),
        )
        for prod_id, bill_date, cust_id in cur.fetchall():
            part = aq.norm_part(prod_id)
            if part and part not in out:
                d = low_stock._to_date(bill_date)
                out[part] = {"supplier_id": str(cust_id or "").strip(), "date": d.isoformat() if d else ""}
    return out


# ── 本機資料 ───────────────────────────────────────────────────────────────

def scheduled_need(conversions: Dict[str, str], warnings: List[str]) -> Dict[str, float]:
    """已排程需求 = status='pending' 訂單的 BOM 需求（缺料採購同一套：_aggregate(build_dispatch_groups)，含轉換料）。"""
    from ..routers import dispatch as dispatch_router  # 延遲載入避免循環相依
    from . import shortage_purchase

    ids = [int(o["id"]) for o in db.get_orders_by_status("pending")]
    if not ids:
        return {}
    try:
        agg = shortage_purchase._aggregate(dispatch_router.build_dispatch_groups(ids, {}), conversions)
    except Exception as exc:
        detail = getattr(exc, "detail", "") or type(exc).__name__
        warnings.append(f"已排程需求計算失敗（{detail}），本次以 0 計")
        return {}
    return {k: float(v.get("need") or 0) for k, v in agg.items()}


def dispatched_demands(conversions: Dict[str, str]) -> List[Dict[str, Any]]:
    orders = db.get_orders_by_status("dispatched")
    records = db.get_dispatch_records_by_orders([int(o["id"]) for o in orders])
    try:
        bom_map = db.get_all_bom_components_by_model()
    except Exception:
        bom_map = {}
    return aq.order_demands(orders, records, bom_map, conversions)


def collect(parts: List[str], today: Optional[date] = None) -> Dict[str, Any]:
    """算每顆料的數字（正航唯讀 + 本機）。正航失敗丟 ErpError。parts 須已排除 -TAB。"""
    today = today or local_now().date()
    from . import st_pending_transfer

    warnings: List[str] = []
    conversions = part_conversion.active_map()
    st_stock = {aq.norm_part(k): _num(v) for k, v in db.get_st_inventory_stock().items()}
    pending_result = st_pending_transfer.compute((), conversions)
    pending = {aq.norm_part(k): _num(v.get("pending")) for k, v in (pending_result.get("parts") or {}).items()}
    warnings.extend(pending_result.get("warnings") or [])
    need = scheduled_need(conversions, warnings)
    demands = dispatched_demands(conversions)

    ware_id = erp_stock.get_settings()["ware_id"]
    gl_ware = low_stock._gl_ware_id()
    start = today - timedelta(days=LOOKBACK_DAYS)
    cfg = low_stock.get_config()
    with low_stock._erp_session() as erp:
        products = _query_products(erp, parts)
        suppliers = _query_suppliers(erp)
        last = _query_last_suppliers(erp, parts)
        st_usage = low_stock._query_usage(erp, ware_id, start, today, parts)
        gl_usage = low_stock._query_gl_usage(erp, gl_ware, start, today, parts)
        gl_qty = low_stock._query_ware_qty(erp, gl_ware, parts)
        on_order = low_stock._fetch_on_order(erp, parts, today, cfg)
        issued_raw = aq.query_issued_by_po(erp, gl_ware, aq.issued_query_parts(parts, demands, conversions))
    if on_order is None:
        warnings.append(low_stock.ON_ORDER_NOTE_FAILED)
        on_order = {}
    issued = aq.convert_issued(issued_raw, conversions)
    unissued = aq.unissued_by_part(demands, issued)

    rows: Dict[str, Dict[str, Any]] = {}
    for part in parts:
        prod = products.get(part, {})
        avail = aq.calc_available(st_stock.get(part, 0.0), pending.get(part, 0.0), gl_qty.get(part, 0.0),
                                  (unissued.get(part) or {}).get("unissued", 0.0))
        su = max(0.0, _num(st_usage.get(part, (0, 0))[0]))
        gu = max(0.0, _num(gl_usage.get(part, (0, 0))[0]))
        oo_qty, oo_eta = on_order.get(part, (0.0, ""))
        rows[part] = {
            **avail,
            "name": prod.get("name", ""), "perdays": prod.get("perdays", 0.0),
            "major_supplier_id": prod.get("major_supplier_id", ""),
            "last_supplier_id": (last.get(part) or {}).get("supplier_id", ""),
            "last_purchase_date": (last.get(part) or {}).get("date", ""),
            "usage_st_1y": su, "usage_015_1y": gu, "daily": round(max(su, gu) / LOOKBACK_DAYS, 4),
            "need": round(need.get(part, 0.0), 4), "on_order": _num(oo_qty), "eta": oo_eta or "",
            "unissued_detail": (unissued.get(part) or {}).get("detail", []),
        }
    info = pending_result.get("info") or {}
    no_bom = [{"order_id": d["order_id"], "code": d["code"], "po": d["po_number"], "model": d["model"]}
              for d in demands if d["source"] == aq.SOURCE_NO_BOM]
    no_records = [{"order_id": d["order_id"], "code": d["code"], "po": d["po_number"], "model": d["model"]}
                  for d in demands if d["source"] == aq.SOURCE_BOM]
    if no_bom:
        warnings.append(f"已發料訂單缺 BOM 也沒有派工紀錄 {len(no_bom)} 張，未領需求算不出來（資料待確認）")
    return {
        "rows": rows, "suppliers": suppliers, "warnings": warnings, "no_bom_orders": no_bom,
        "no_record_orders": no_records,
        "pending_info": {k: info.get(k) for k in (
            "cutoff", "cutoff_code", "cutoff_source", "cutoff_move_no", "cutoff_source_text", "settle_date",
            "batches", "st_loaded_at")},
        "dispatched_order_count": len(demands),
        "issued": issued,  # {(PO, 最終料號): 015 已領}；對帳報表用，不存快照
    }


def population_parts(main_parts: Optional[set]) -> List[str]:
    return sorted(p for p in (main_parts or set()) if p and not aq.is_tab(p))


def manual_parts() -> Dict[str, Dict[str, Any]]:
    """手動新增的料號（排除 -TAB）。"""
    return {p: v for p, v in db.get_low_stock_manual_parts().items() if p and not aq.is_tab(p)}


def refresh_snapshot(actor: str = "system") -> Dict[str, Any]:
    """重算快照並整批覆寫本機表。永遠不丟例外；正航或主檔失敗時保留舊快照。"""
    try:
        db.ensure_low_stock_table()
        main_parts = low_stock.main_file_parts()
        if main_parts is None:
            return _fail("讀不到主檔，低庫存快照未更新（保留上次資料）")
        parts = sorted(set(population_parts(main_parts)) | set(manual_parts()))
        data = collect(parts)
        now = low_stock._now_text()
        db.replace_low_stock_plan_snapshot(data["rows"], data["suppliers"], now)
        meta = {
            "ok": True, "refreshed_at": now, "error": "", "warnings": data["warnings"],
            "pending_info": data["pending_info"], "no_bom_orders": data["no_bom_orders"],
            "no_record_orders": data["no_record_orders"], "dispatched_order_count": data["dispatched_order_count"],
            "st_loaded_at": db.get_setting("st_inventory_loaded_at", ""), "part_count": len(parts),
        }
        db.set_setting(META_KEY, json.dumps(meta, ensure_ascii=False))
        db.log_activity("低庫存提醒快照", f"{actor}：主檔料號 {len(parts)} 顆（已排除 -TAB）")
        return {"ok": True, "parts": len(parts), "refreshed_at": now, "warnings": data["warnings"]}
    except Exception as exc:
        logger.warning("低庫存快照重算失敗（保留舊資料）：%s：%s", type(exc).__name__, exc)
        message = str(exc) if isinstance(exc, ErpError) else type(exc).__name__
        return _fail(f"低庫存快照重算失敗：{message}（保留上次資料）")


def _fail(message: str) -> Dict[str, Any]:
    meta = get_meta()
    meta.update(ok=False, error=message, failed_at=low_stock._now_text())
    db.set_setting(META_KEY, json.dumps(meta, ensure_ascii=False))
    return {"ok": False, "error": message}


def get_meta() -> Dict[str, Any]:
    try:
        data = json.loads(db.get_setting(META_KEY, "") or "{}")
        return data if isinstance(data, dict) else {}
    except ValueError:
        return {}


# ── 讀取（只讀本機） ───────────────────────────────────────────────────────

def _supplier_names(suppliers: Dict[str, Tuple[str, str]], sid: str) -> Tuple[str, str]:
    return suppliers.get(sid, ("", ""))


def build_rows(today: Optional[date] = None) -> Dict[str, Any]:
    """快照 + 設定 → 每顆料一列（含區域與依據）。"""
    today = today or local_now().date()
    db.ensure_low_stock_table()
    snapshot = db.get_low_stock_plan_snapshot()
    suppliers = db.get_low_stock_plan_suppliers()
    sup_rows = db.get_low_stock_supplier_leads()
    part_rows = db.get_low_stock_part_settings()
    main_parts = low_stock.main_file_parts()
    keywords = low_stock.get_priority_suppliers()
    manual = manual_parts()
    rows: List[Dict[str, Any]] = []
    excluded = {"tab": 0, "not_main": 0, "unused": 0}
    for part in sorted(set(snapshot) | set(manual)):
        if aq.is_tab(part):
            excluded["tab"] += 1
            continue
        payload = snapshot.get(part)
        is_manual = part in manual
        if not is_manual:
            if main_parts is not None and part not in main_parts:
                excluded["not_main"] += 1
                continue
            if _num(payload.get("usage_st_1y")) <= 0 and _num(payload.get("usage_015_1y")) <= 0:
                excluded["unused"] += 1
                continue
        no_data = payload is None
        payload = payload or _empty_payload()
        ps = part_rows.get(part)
        sid, kind = resolve_supplier(payload, ps)
        short, full = _supplier_names(suppliers, sid)
        lead = resolve_lead(sid, (short, full), sup_rows.get(sid), ps)
        row = {
            "part": part, **payload,
            "supplier_id": sid, "supplier_name": short or full or sid, "supplier_kind": kind,
            "priority_supplier": low_stock.match_priority_supplier(short, keywords)
            or low_stock.match_priority_supplier(full, keywords),
            "lead_days": lead["lead_days"], "anytime": lead["anytime"], "lead_source": lead["source"],
            "lead_text": lead["text"],
            "part_lead_days": (ps or {}).get("lead_days"), "part_anytime": bool(int((ps or {}).get("anytime") or 0)),
            "supplier_override": str((ps or {}).get("supplier_override") or ""),
            "manual": is_manual, "manual_note": str((manual.get(part) or {}).get("note") or ""),
            "no_data": no_data,
        }
        if no_data:
            row.update(area=AREA_CHECK, reasons=[NO_DATA_REASON], tags=[MANUAL_TAG], ordered=False, cover_days=None)
        else:
            classify(row, today)
            if is_manual:
                row["tags"].insert(0, MANUAL_TAG)
        row["shortage"] = shortage_qty(row)
        rows.append(row)
    return {"rows": rows, "excluded": excluded, "suppliers": suppliers, "supplier_rows": sup_rows}


def _empty_payload() -> Dict[str, Any]:
    """手動新增、尚未抓過正航的料：數字一律空（畫面顯示 —），不當成 0 判斷。"""
    return {
        "st": None, "pending_raw": None, "pending": None, "free_st": None, "gl015": None, "unissued": None,
        "available": None, "available_without_free_st": None, "pending_over_st": False,
        "name": "", "perdays": 0, "major_supplier_id": "", "last_supplier_id": "", "last_purchase_date": "",
        "usage_st_1y": 0, "usage_015_1y": 0, "daily": 0, "need": 0, "on_order": 0, "eta": "", "unissued_detail": [],
    }


def supplier_table(rows: List[Dict[str, Any]], suppliers: Dict[str, Tuple[str, str]],
                   sup_rows: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """母體涉及的廠商（照廠商歸屬算料數）＋目前生效的交期設定。"""
    counts: Dict[str, int] = {}
    for r in rows:
        if r["supplier_id"]:
            counts[r["supplier_id"]] = counts.get(r["supplier_id"], 0) + 1
    out = []
    for sid, n in counts.items():
        short, full = _supplier_names(suppliers, sid)
        eff = resolve_lead(sid, (short, full), sup_rows.get(sid), None)
        row = sup_rows.get(sid) or {}
        out.append({
            "supplier_id": sid, "supplier_name": short or full or sid, "part_count": n,
            "lead_days": eff["lead_days"], "anytime": eff["anytime"], "source": eff["source"], "text": eff["text"],
            "is_default": not bool(row), "long_lead_default": is_long_lead_supplier(short, full),
            "updated_at": row.get("updated_at", ""),
        })
    out.sort(key=lambda s: (not s["long_lead_default"], -s["part_count"], s["supplier_id"]))
    return out


def build_plan(today: Optional[date] = None) -> Dict[str, Any]:
    data = build_rows(today)
    rows = data["rows"]
    main = sorted((r for r in rows if r["area"] == AREA_MAIN), key=sort_key_main)
    check = sorted((r for r in rows if r["area"] == AREA_CHECK), key=sort_key_check)
    meta = get_meta()
    pinfo = meta.get("pending_info") or {}
    return {
        "refreshed_at": meta.get("refreshed_at", ""),
        "st_loaded_at": meta.get("st_loaded_at", ""),
        "error": meta.get("error", "") if meta.get("ok") is False else "",
        "warnings": meta.get("warnings", []),
        "no_bom_orders": meta.get("no_bom_orders", []),
        "no_record_orders": meta.get("no_record_orders", []),
        "cutoff": {"code": pinfo.get("cutoff_code") or "", "source": pinfo.get("cutoff_source") or "",
                   "move_no": pinfo.get("cutoff_move_no") or "", "text": pinfo.get("cutoff_source_text") or ""},
        "counts": {
            "population": len(rows), "main": len(main), "check": len(check),
            "anytime": sum(1 for r in rows if r["anytime"]),
            "ok_with_lead": sum(1 for r in rows if r["area"] == AREA_OK),
            "main_ordered": sum(1 for r in main if r["ordered"]),
            **{f"excluded_{k}": v for k, v in data["excluded"].items()},
        },
        "main": main,
        "check": check,
        "suppliers": supplier_table(rows, data["suppliers"], data["supplier_rows"]),
        "no_supplier_count": sum(1 for r in rows if not r["supplier_id"]),
        "long_lead_suppliers": list(LONG_LEAD_SUPPLIERS),
        "long_lead_default_days": LONG_LEAD_DEFAULT_DAYS,
    }


def alert_count() -> int:
    """側欄／徽章用：主清單列數（不連正航）。"""
    try:
        return sum(1 for r in build_rows()["rows"] if r["area"] == AREA_MAIN)
    except Exception:
        logger.warning("低庫存主清單計數失敗", exc_info=True)
        return 0


# ── 設定 ───────────────────────────────────────────────────────────────────

def get_settings() -> Dict[str, Any]:
    db.ensure_low_stock_table()
    return {
        "suppliers": list(db.get_low_stock_supplier_leads().values()),
        "parts": list(db.get_low_stock_part_settings().values()),
        "long_lead_suppliers": list(LONG_LEAD_SUPPLIERS),
        "long_lead_default_days": LONG_LEAD_DEFAULT_DAYS,
    }


def save_supplier_lead(supplier_id: Any, lead_days: Any = None, anytime: Any = False, actor: str = "web") -> Dict[str, Any]:
    """存廠商交期；交期空且不勾可隨時下單 = 清除（回到預設：四家 60 天、其他可隨時下單）。"""
    sid = str(supplier_id or "").strip()
    if not sid:
        raise LeadSettingError("請指定廠商代碼")
    lead = validate_lead(lead_days)
    any_flag = bool(anytime)
    db.ensure_low_stock_table()
    short, full = db.get_low_stock_plan_suppliers().get(sid, ("", ""))
    if lead is None and not any_flag:
        db.delete_low_stock_supplier_lead(sid)
        text = "清除（回到預設）"
    else:
        db.save_low_stock_supplier_lead(sid, short or full, lead, any_flag, actor)
        text = "可隨時下單" if any_flag else f"{lead} 天"
    db.log_activity("低庫存廠商交期", f"{actor}：{sid} {short} → {text}")
    return {"supplier_id": sid, "lead_days": lead, "anytime": any_flag}


_UNSET = object()


def save_part_setting(part: Any, lead_days: Any = _UNSET, anytime: Any = _UNSET,
                      supplier_override: Any = _UNSET, actor: str = "web") -> Dict[str, Any]:
    """存料號設定；沒帶的欄位維持原值。lead_days 空 = 清除覆蓋；supplier_override 空 = 用自動判斷。"""
    key = aq.norm_part(part)
    if not key:
        raise LeadSettingError("請指定料號")
    if aq.is_tab(key):
        raise LeadSettingError("-TAB 客供料不列入低庫存")
    db.ensure_low_stock_table()
    cur = db.get_low_stock_part_settings().get(key) or {}
    lead = cur.get("lead_days") if lead_days is _UNSET else validate_lead(lead_days)
    any_flag = bool(int(cur.get("anytime") or 0)) if anytime is _UNSET else bool(anytime)
    if supplier_override is _UNSET:
        sup = str(cur.get("supplier_override") or "")
    else:
        sup = str(supplier_override or "").strip().upper()
        known = db.get_low_stock_plan_suppliers()
        known_upper = {k.upper(): k for k in known}
        if sup:
            if known and sup not in known_upper:
                raise LeadSettingError(f"找不到廠商代碼 {sup}")
            sup = known_upper.get(sup, sup)
    db.save_low_stock_part_setting(key, lead, any_flag, sup, actor)
    db.log_activity("低庫存料號設定", f"{actor}：{key} 交期 {lead if lead is not None else '—'}、"
                                      f"可隨時下單 {'是' if any_flag else '否'}、廠商 {sup or '自動'}")
    return {"part": key, "lead_days": lead, "anytime": any_flag, "supplier_override": sup}


# ── 手動新增料號（R-B9） ───────────────────────────────────────────────────

def _manual_key(part: Any) -> str:
    key = aq.norm_part(part)
    if not key:
        raise LeadSettingError("請輸入料號")
    if aq.is_tab(key):
        raise LeadSettingError("-TAB 客供料不列入低庫存，不能新增")
    return key


def refresh_parts(parts: List[str], actor: str = "system") -> Dict[str, Any]:
    """只重抓指定料號（正航唯讀）並更新快照列；失敗不丟例外、其他料號不動。"""
    try:
        db.ensure_low_stock_table()
        data = collect(parts)
        db.upsert_low_stock_plan_rows(data["rows"], data["suppliers"], low_stock._now_text())
        return {"ok": True}
    except Exception as exc:
        logger.warning("低庫存料號重抓失敗：%s：%s", type(exc).__name__, exc)
        message = str(exc) if isinstance(exc, ErpError) else type(exc).__name__
        return {"ok": False, "error": f"正航資料沒抓到（{message}），請稍後按「重新抓正航」"}


def add_manual_part(part: Any, lead_days: Any = None, note: Any = "", actor: str = "web") -> Dict[str, Any]:
    """手動新增料號：記在 low_stock_manual_part；有填交期就存成料號覆蓋交期；接著唯讀抓這顆料的正航數字。"""
    key = _manual_key(part)
    lead = validate_lead(lead_days)
    db.ensure_low_stock_table()
    if key in db.get_low_stock_manual_parts():
        raise LeadSettingError("此料號已手動新增過")
    db.save_low_stock_manual_part(key, str(note or "").strip(), actor)
    if lead is not None:
        save_part_setting(key, lead_days=lead, actor=actor)
    db.log_activity("低庫存手動新增料號", f"{actor}：{key}" + (f" 交期 {lead} 天" if lead is not None else ""))
    fetched = refresh_parts([key], actor)
    return {"part": key, "lead_days": lead, "fetched": fetched["ok"], "warning": fetched.get("error", "")}


def update_manual_part(part: Any, actor: str = "web", **fields: Any) -> Dict[str, Any]:
    """改手動料號的備註（note）；lead_days 走料號設定（空 = 清除，恢復廠商交期）。"""
    key = _manual_key(part)
    db.ensure_low_stock_table()
    cur = db.get_low_stock_manual_parts().get(key)
    if cur is None:
        raise LowStockPlanNotFound("找不到此手動料號")
    if "note" in fields:
        db.save_low_stock_manual_part(key, str(fields["note"] or "").strip(), actor)
    if "lead_days" in fields:
        save_part_setting(key, lead_days=fields["lead_days"], actor=actor)
    return {"part": key}


def remove_manual_part(part: Any, actor: str = "web") -> bool:
    """移除手動新增的料號；料號設定（交期覆蓋等）保留。原本就在母體的料照樣會列（照規則判斷）。"""
    key = aq.norm_part(part)
    db.ensure_low_stock_table()
    if not key or not db.delete_low_stock_manual_part(key):
        return False
    db.log_activity("低庫存移除手動料號", f"{actor}：{key}")
    return True


class LowStockPlanNotFound(LeadSettingError):
    """找不到料號。"""


# ── 開採購單建議量（R-E2/E3/E4/E10） ───────────────────────────────────────

def shortage_qty(row: Dict[str, Any]) -> float:
    """缺料量 = 已排需求 − 可用（可用 < 已排需求時）；已排需求為 0 時不買（不因可用 ≤ 0 無故多買）。"""
    if row.get("no_data") or row.get("available") is None:
        return 0.0
    avail, need = _num(row["available"]), _num(row.get("need"))
    if need <= 0 or avail >= need:
        return 0.0
    return round(need - avail, 4)


def suggested_po_qty(shortage: Any, moq: Any) -> int:
    """缺料量照 MOQ 往上取整（MOQ 的整數倍），不多買；缺料量 0 就是 0。"""
    return moq_service.adjust_qty(shortage, moq, moq)


def _po_target() -> str:
    """重複開單提醒要比對的寫入公司（同開採購單）；只讀設定，不碰寫入開關。"""
    from . import erp_purchase

    try:
        return erp_purchase.get_target()
    except Exception:
        return erp_purchase._env("ERP_WRITE_TARGET", erp_purchase.DEFAULT_TARGET)


def po_suggest(part: Any, today: Optional[date] = None) -> Dict[str, Any]:
    """低庫存列「開採購單」：這顆料＋同廠商其他缺料（主清單順序）的建議量、MOQ 依據、近期已開過的採購單。"""
    from . import po_duplicates

    today = today or local_now().date()
    key = aq.norm_part(part)
    if not key or aq.is_tab(key):
        raise LeadSettingError("-TAB 客供料不開採購單" if key else "請指定料號")
    rows = build_rows(today)["rows"]
    by_part = {r["part"]: r for r in rows}
    row = by_part.get(key)
    if row is None:
        raise LowStockPlanNotFound("低庫存清單沒有這顆料")
    siblings = []
    if row["supplier_id"]:
        main = sorted((r for r in rows if r["area"] == AREA_MAIN), key=sort_key_main)
        siblings = [r for r in main if r["part"] != key and r["supplier_id"] == row["supplier_id"] and r["shortage"] > 0]
    targets = [row, *siblings]
    moq_info = moq_service.lookup_moq([r["part"] for r in targets])
    target = _po_target()
    prior = po_duplicates.load_prior(target, [r["part"] for r in targets])

    def item(r: Dict[str, Any]) -> Dict[str, Any]:
        info = moq_info.get(r["part"]) or {}
        fields = moq_service.moq_fields(r["shortage"], info, info.get("moq") or 0)
        recent = [{k: e[k] for k in ("bill_no", "date", "qty", "supplier_id")}
                  for e in prior.get(r["part"], [])
                  if (d := _parse_date(e.get("date"))) and 0 <= (today - d).days <= po_duplicates.PART_ALONE_DAYS]
        return {
            "part": r["part"], "name": r.get("name", ""), "supplier_id": r["supplier_id"],
            "supplier_name": r["supplier_name"], "supplier_kind": r["supplier_kind"],
            "available": r.get("available"), "need": r.get("need"), "shortage": r["shortage"],
            "on_order": r.get("on_order") or 0, "eta": r.get("eta") or "",
            **fields, "suggested_qty": suggested_po_qty(r["shortage"], info.get("moq") or 0),
            "recent_pos": recent,
        }

    return {"item": item(row), "same_supplier": [item(r) for r in siblings],
            "recent_days": po_duplicates.PART_ALONE_DAYS}


def shortage_for_part(part: Any) -> Optional[int]:
    """開採購單頁手動輸入料號時的「需」：低庫存清單有這顆料且缺料量 > 0 才回整數，否則 None。"""
    key = aq.norm_part(part)
    if not key:
        return None
    for r in build_rows()["rows"]:
        if r["part"] == key:
            return int(math.ceil(r["shortage"] - 1e-9)) if r["shortage"] > 0 else None
    return None
