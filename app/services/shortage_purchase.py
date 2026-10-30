"""缺料採購：寫入主檔 / 生成發料單之後，列出哪些料要買，勾選後依供應商各開一張正航採購單。

流程：選訂單 → build_shortage_list（彙整需求、扣 ST 現有（在途僅供參考、不扣除）、解析供應商 / 單價 / 預計進貨日）
→ 使用者勾選 → erp_purchase.preview_many 預覽 → create_many 逐張寫入（每張獨立 transaction）。

預計進貨日（以「最早出貨日的訂單」為準）：
- 打件料：出貨日往前 1 個月（月底則取該月底，如 3/31→2/28），再取「不晚於該日的最近一個 26 日」。
- 後段料：出貨日 - 21 天。
- 推算日早於今天（台北時區）時改用今天，並保留原推算日（original_pre_in_date / date_adjusted），不再標記過期。

打件 / 後段的自動判斷（classify_part，人工指定優先，存於 part_purchase_categories）：
- 打件（smt）：料號前兩碼為 EC / IC / UC / PB，或品名（不分大小寫）含
  0201 0402 0603 0805 1206 SMD SMT SOT QFN QFP BGA SOP TSSOP PCB 電容 電阻 電感。
- 其餘為後段（backend）：OC 外殼 / 螺絲、PK 包材、SP 印刷外殼、AC 線組、RC、CM / CF 標籤、
  SS / GH 壓著端子等。
"""
from __future__ import annotations

import calendar
import logging
import math
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .. import database as db
from . import erp_purchase, erp_stock, low_stock, part_conversion
from .shortage_rules import summarize_requested_supply
from .local_time import local_now

logger = logging.getLogger(__name__)

MAX_GROUPS = 20
IN_CHUNK = 200
SMT_PREFIXES = ("EC", "IC", "UC", "PB")
_SIZE_RE = re.compile(r"(?<!\d)(?:0201|0402|0603|0805|1206)(?!\d)")
_KEYWORD_RE = re.compile(r"SMD|SMT|SOT|QFN|QFP|BGA|SOP|TSSOP|PCB|電容|電阻|電感", re.IGNORECASE)
CATEGORY_LABELS = {"smt": "打件", "backend": "後段"}
SUPPLIER_SOURCE_LABELS = {"master": "主檔", "last_po": "最近採購", "none": "未設定"}


# ── 日期規則（純函式）────────────────────────────────────────────────────────

def _minus_one_month(d: date) -> date:
    year, month = (d.year - 1, 12) if d.month == 1 else (d.year, d.month - 1)
    return date(year, month, min(d.day, calendar.monthrange(year, month)[1]))


def smt_pre_in_date(ship: date) -> date:
    """打件料預計進貨日：出貨日 -1 個月後，取不晚於該日的最近一個 26 日。"""
    d = _minus_one_month(ship)
    if d.day >= 26:
        return date(d.year, d.month, 26)
    prev = _minus_one_month(date(d.year, d.month, 1))
    return date(prev.year, prev.month, 26)


def backend_pre_in_date(ship: date) -> date:
    return ship - timedelta(days=21)


def pre_in_date_for(category: str, ship: date) -> date:
    return smt_pre_in_date(ship) if category == "smt" else backend_pre_in_date(ship)


def clamp_pre_in_date(pre_in: date, today: date) -> Tuple[date, bool]:
    """推算日早於今天就改用今天；回傳 (採用日期, 是否被調整)。等於今天或更晚則不變。"""
    return (today, True) if pre_in < today else (pre_in, False)


def parse_ship_date(value: Any) -> Optional[date]:
    text = str(value or "").strip().replace("/", "-")[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


# ── 類別 ─────────────────────────────────────────────────────────────────────

def classify_part(part_number: str, description: str = "") -> str:
    part = str(part_number or "").strip().upper()
    if part[:2] in SMT_PREFIXES:
        return "smt"
    desc = str(description or "")
    if _SIZE_RE.search(desc) or _KEYWORD_RE.search(desc):
        return "smt"
    return "backend"


# ── 採購量 → 訂單分配（僅記錄 / 顯示，不參與缺料計算）──────────────────────

def _qty_clean(value: float) -> float:
    return round(float(value), 6)


def allocate_purchase_to_orders(
    orders: List[Dict[str, Any]], st: float, purchase_qty: float
) -> List[Dict[str, Any]]:
    return _allocate(orders, st, purchase_qty)[0]


def _allocate(
    orders: List[Dict[str, Any]], st: float, purchase_qty: float
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """把一次採購量分配給各訂單（純函式）。

    依出貨日由早到晚（空日期排最後、同日維持原順序）：先用 max(ST, 0) 依序抵各訂單需求，
    再把採購量依序填入各訂單剩餘需求；買不夠時較晚的訂單分到較少或沒有（零量不列出），
    買超過的部分記成一筆 kind='extra'（order_id=0）。同一張訂單的多筆需求會合併。
    回傳 (分配結果, 扣除 ST 與本次採購後各訂單的剩餘需求)，供同料號多行接續分配。
    """
    merged: Dict[Any, Dict[str, Any]] = {}
    for o in orders:
        key = o.get("order_id")
        need = float(o.get("qty") or 0)
        if need <= 0:
            continue
        if key in merged:
            merged[key]["need"] += need
        else:
            merged[key] = {"order": o, "need": need}
    ordered = sorted(
        merged.values(),
        key=lambda m: parse_ship_date(m["order"].get("ship_date")) or date.max,
    )
    st_left = max(float(st or 0), 0.0)
    for m in ordered:
        used = min(st_left, m["need"])
        st_left -= used
        m["need"] -= used
    left = max(float(purchase_qty or 0), 0.0)
    out: List[Dict[str, Any]] = []
    takes = []
    for m in ordered:
        take = min(left, m["need"])
        takes.append((m, take if take > 1e-9 else 0.0))
        if take <= 1e-9:
            continue
        left -= take
        o = m["order"]
        out.append({
            "order_id": o.get("order_id") or 0,
            "order_code": o.get("code") or "",
            "model": o.get("model") or "",
            "customer_po": str(o.get("po_number") or ""),
            "ship_date": str(o.get("ship_date") or ""),
            "dispatch_session_id": 0,
            "qty": _qty_clean(take),
            "kind": "order",
        })
    if left > 1e-9:
        out.append({
            "order_id": 0, "order_code": "", "model": "", "customer_po": "", "ship_date": "",
            "dispatch_session_id": 0, "qty": _qty_clean(left), "kind": "extra",
        })
    remaining = [{**m["order"], "qty": m["need"] - min(left_take, m["need"])} for m, left_take in takes]
    return out, [o for o in remaining if o["qty"] > 1e-9]


def _allocation_context(order_ids: List[int]) -> Dict[str, Dict[str, Any]]:
    """依勾選的訂單重建每個料號的 ST 與各訂單需求（與缺料清單同一來源，不查 ERP）。"""
    from ..routers import dispatch as dispatch_router  # 延遲載入避免循環相依

    groups = dispatch_router.build_dispatch_groups(order_ids, {})
    st_stock = {
        str(p or "").strip().upper(): float(q or 0) for p, q in db.get_st_inventory_stock().items()
    }
    mapping = part_conversion.active_map()
    sources = part_conversion.sources_by_target(mapping)
    return {
        key: {"st": part_conversion.effective_stock(key, st_stock, sources)[0], "orders": entry["orders"]}
        for key, entry in _aggregate(groups, mapping).items()
    }


def _manual_allocation(qty: float) -> List[Dict[str, Any]]:
    return [{
        "order_id": 0, "order_code": "", "model": "", "customer_po": "", "ship_date": "",
        "dispatch_session_id": 0, "qty": _qty_clean(qty), "kind": "manual",
    }]


def _clamp_date_text(value: Any, today: date) -> Any:
    """伺服器端保險：預計進貨日早於今天一律改為今天（用戶端可能送來過期的舊推算日）；無法解析者原樣交給後續驗證。"""
    parsed = parse_ship_date(value)
    return clamp_pre_in_date(parsed, today)[0].isoformat() if parsed else value


def prepare_groups(
    groups: List[Dict[str, Any]], order_ids: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    """替每個勾選行依最終數量重算 allocations，並標記來源為 shortage_po。

    沒有 order_ids、或料號不在這批訂單的需求內時，該行記成一筆 manual；
    用戶端送來的 allocations 一律忽略（以伺服器重算為準）；早於今天的預計進貨日一律改為今天。
    """
    today = local_now().date()
    ids = list(dict.fromkeys(int(i) for i in (order_ids or []) if i))
    context: Dict[str, Dict[str, Any]] = {}
    if ids:
        try:
            context = _allocation_context(ids)
        except Exception as exc:
            logger.warning("缺料採購：重建訂單分配失敗，改記為手動：%s", type(exc).__name__)
    # 同料號出現在多行（如拆給不同供應商）時依序接續分配：ST 只扣一次，後一行從前一行已覆蓋的需求之後開始
    state: Dict[str, Dict[str, Any]] = {k: {"st": v["st"], "orders": v["orders"]} for k, v in context.items()}
    out = []
    for group in groups:
        lines = []
        for line in group.get("lines") or []:
            part = str(line.get("part_number") or "").strip().upper()
            qty = float(line.get("qty") or 0)
            ctx = state.get(part)
            if ctx:
                allocations, remaining = _allocate(ctx["orders"], ctx["st"], qty)
                ctx["st"], ctx["orders"] = 0.0, remaining
            else:
                allocations = _manual_allocation(qty)
            lines.append({
                **line,
                "pre_in_date": _clamp_date_text(line.get("pre_in_date"), today),
                "allocations": allocations,
            })
        out.append({**group, "lines": lines, "alloc_source": "shortage_po"})
    return out


# ── 勾選行 → 採購單請求 ──────────────────────────────────────────────────────

def group_lines(
    lines: List[Dict[str, Any]],
    remarks: Optional[Dict[str, str]] = None,
    salesman: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """把勾選的行（每行帶 supplier_id）依供應商分組：同供應商所有料進同一張採購單。

    供應商順序依第一次出現；沒有供應商的行會被丟出 ValueError，不會悄悄略過。
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for line in lines:
        supplier_id = str(line.get("supplier_id") or "").strip()
        if not supplier_id:
            raise ValueError(f"料號 {line.get('part_number', '')} 尚未指定供應商")
        grouped.setdefault(supplier_id, []).append({
            "part_number": line["part_number"],
            "qty": line["qty"],
            "price": line["price"],
            "pre_in_date": line["pre_in_date"],
        })
    return [
        {
            "supplier_id": supplier_id,
            "remark": (remarks or {}).get(supplier_id, ""),
            "salesman": salesman,
            "lines": group,
        }
        for supplier_id, group in grouped.items()
    ]


# ── ERP 唯讀查詢（來源公司，批次 IN）─────────────────────────────────────────

def _chunks(items: List[str], size: int = IN_CHUNK) -> Iterator[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _marks(n: int) -> str:
    return "(" + ",".join(["%s"] * n) + ")"


def _fmt_yyyymmdd(value: Any) -> str:
    try:
        v = int(value or 0)
    except (TypeError, ValueError):
        return ""
    if v <= 0:
        return ""
    return f"{v // 10000:04d}-{v // 100 % 100:02d}-{v % 100:02d}"


def _lookup_products(conn, parts: List[str]) -> Dict[str, Dict[str, str]]:
    result: Dict[str, Dict[str, str]] = {}
    cur = conn.cursor()
    for batch in _chunks(parts):
        cur.execute(
            "SELECT ProdID, ProdName, MajorSupplier, ClassID FROM comProduct WITH (NOLOCK) "
            f"WHERE ProdID IN {_marks(len(batch))}",
            tuple(batch),
        )
        for row in cur.fetchall():
            prod_id, name, major = row[0], row[1], row[2]
            class_id = row[3] if len(row) > 3 else ""
            key = erp_purchase._text(prod_id).upper()
            if key:
                result[key] = {
                    "name": erp_purchase._text(name),
                    "major_supplier": erp_purchase._text(major),
                    "class_id": erp_purchase._text(class_id),
                }
    return result


# ── 客供料（-TAB 結尾或正航類別 003）：向客戶要料，不採購 ────────────────────────

DEFAULT_TAB_WARE_IDS = "002"  # TABLEAU；不含 015


def tab_ware_ids() -> List[str]:
    """客供料可用庫存的正航倉庫（env ERP_TAB_WARE_IDS，逗號分隔，預設 002）。"""
    raw = erp_stock._env("ERP_TAB_WARE_IDS", DEFAULT_TAB_WARE_IDS)
    ids = [w.strip() for w in raw.split(",") if w.strip()]
    return list(dict.fromkeys(ids)) or [DEFAULT_TAB_WARE_IDS]


def _query_tab_stock(conn, parts: List[str], ware_ids: List[str]) -> Dict[str, float]:
    """客供料在指定倉庫的庫存合計（只有 SELECT；comWareAmount，與 ST 庫存同一張表，不是 ST 001）。"""
    result: Dict[str, float] = {}
    cur = conn.cursor()
    for batch in _chunks(parts):
        cur.execute(
            "SELECT w.ProdID, SUM(w.Quantity) FROM comWareAmount AS w WITH (NOLOCK) "
            f"WHERE w.WareID IN {_marks(len(ware_ids))} AND w.ProdID IN {_marks(len(batch))} "
            "GROUP BY w.ProdID",
            (*ware_ids, *batch),
        )
        for prod_id, qty in cur.fetchall():
            key = erp_purchase._text(prod_id).upper()
            if key:
                result[key] = result.get(key, 0.0) + float(qty or 0)
    return result


def _lookup_last_po(conn, parts: List[str]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    cur = conn.cursor()
    for batch in _chunks(parts):
        cur.execute(
            "SELECT ProdID, CustomerID, Price, BillNO, BillDate FROM ("
            "SELECT s.ProdID, m.CustomerID, s.Price, m.BillNO, m.BillDate, "
            "ROW_NUMBER() OVER (PARTITION BY s.ProdID ORDER BY m.BillDate DESC, m.BillNO DESC) AS rn "
            "FROM ordBillSub s WITH (NOLOCK) "
            "JOIN ordBillMain m WITH (NOLOCK) ON m.Flag = s.Flag AND m.BillNO = s.BillNO "
            f"WHERE s.Flag = 4 AND s.ProdID IN {_marks(len(batch))}) t WHERE rn = 1",
            tuple(batch),
        )
        for prod_id, supplier, price, bill_no, bill_date in cur.fetchall():
            key = erp_purchase._text(prod_id).upper()
            if key:
                result[key] = {
                    "supplier_id": erp_purchase._text(supplier),
                    "price": float(price or 0),
                    "bill_no": erp_purchase._text(bill_no),
                    "bill_date": _fmt_yyyymmdd(bill_date),
                }
    return result


def _lookup_suppliers(conn, ids: List[str]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    cur = conn.cursor()
    for batch in _chunks(ids):
        cur.execute(
            "SELECT ID, ShortName, FullName FROM comCustomer WITH (NOLOCK) "
            f"WHERE Flag = 2 AND ID IN {_marks(len(batch))}",
            tuple(batch),
        )
        for sid, short, full in cur.fetchall():
            key = erp_purchase._text(sid)
            if key:
                result[key] = erp_purchase._text(short) or erp_purchase._text(full)
    return result


def resolve_supplier(
    major_supplier: str, last_po_supplier: str, suppliers: Dict[str, str]
) -> Tuple[str, str]:
    """(供應商代號, 來源)：主檔慣用供應商（須存在於正航供應商）> 最近一次採購 > 未設定。"""
    major = (major_supplier or "").strip()
    if major and major in suppliers:
        return major, "master"
    last = (last_po_supplier or "").strip()
    if last:
        return last, "last_po"
    return "", "none"


def _erp_lookups(parts: List[str], today: date) -> Dict[str, Any]:
    """所有 ERP 唯讀查詢；ERP 連不上時回空結果並附 warnings，缺料清單照常產生。

    客供料（-TAB 結尾或 ClassID 003）不查供應商 / 最近採購 / 單價 / 在途，改查客供倉庫庫存。
    ERP 不可用時只能靠 -TAB 結尾判斷客供料（products 為空）。
    """
    out: Dict[str, Any] = {
        "products": {}, "last_po": {}, "suppliers": {}, "on_order": {}, "warnings": [],
        "tab_stock": {}, "tab_stock_error": "", "tab_ware_ids": tab_ware_ids(),
    }
    if not parts:
        return out
    purchasable = [p for p in parts if not erp_purchase.customer_supplied_reason(p)]
    try:
        with erp_purchase._source_session() as conn:
            out["products"] = _lookup_products(conn, parts)
            purchasable = [
                p for p in parts
                if not erp_purchase.customer_supplied_reason(p, out["products"].get(p, {}).get("class_id", ""))
            ]
            out["last_po"] = _lookup_last_po(conn, purchasable)
            ids = {p["major_supplier"] for k, p in out["products"].items() if p["major_supplier"] and k in purchasable}
            ids |= {p["supplier_id"] for p in out["last_po"].values() if p["supplier_id"]}
            out["suppliers"] = _lookup_suppliers(conn, sorted(ids))
    except Exception as exc:
        logger.warning("缺料採購：ERP 供應商 / 單價查詢失敗：%s", type(exc).__name__)
        out["warnings"].append(
            f"正航查詢失敗，供應商與單價需手動填寫；客供料僅依料號 -TAB 結尾判斷（{erp_purchase._safe_message(exc)}）"
        )
    try:
        max_overdue = low_stock.get_config()["on_order_max_overdue_days"]
        with low_stock._erp_session() as conn:
            out["on_order"] = low_stock._query_on_order(conn, purchasable, today, max_overdue)
    except Exception as exc:
        logger.warning("缺料採購：在途量查詢失敗：%s", type(exc).__name__)
        out["warnings"].append(f"在途量查詢失敗，已當作 0（{erp_purchase._safe_message(exc)}）")
    cs_parts = [p for p in parts if p not in purchasable]
    if cs_parts:
        try:
            with low_stock._erp_session() as conn:
                out["tab_stock"] = _query_tab_stock(conn, cs_parts, out["tab_ware_ids"])
        except Exception as exc:
            logger.warning("缺料採購：客供料庫存查詢失敗：%s", type(exc).__name__)
            out["tab_stock_error"] = f"客供料倉庫（{'、'.join(out['tab_ware_ids'])}）庫存查詢失敗（{erp_purchase._safe_message(exc)}）"
            out["warnings"].append(out["tab_stock_error"])
    return out


# ── 缺料清單 ─────────────────────────────────────────────────────────────────

def _order_summary(order: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": order.get("id"),
        "code": order.get("code") or "",
        "model": order.get("model") or "",
        "po_number": str(order.get("po_number") or ""),
        "ship_date": str(order.get("delivery_date") or order.get("ship_date") or ""),
        "status": order.get("status") or "",
    }


def _aggregate(
    groups: List[Dict[str, Any]], conversions: Optional[Dict[str, str]] = None
) -> Dict[str, Dict[str, Any]]:
    """依正規化料號彙整需求；earliest 取最早出貨日（空日期排最後，同日取批次順序在前者）。

    conversions 為生效中的轉換料 {A: B}：A 的需求併入 B（A 不再單獨出現）。
    """
    parts: Dict[str, Dict[str, Any]] = {}
    for group in groups:
        for item in group.get("items") or []:
            key = part_conversion.resolve(str(item.get("part") or ""), conversions or {})
            qty = float(item.get("need_qty") or 0)
            if not key or qty <= 0:
                continue
            entry = parts.setdefault(key, {"desc": "", "need": 0.0, "orders": []})
            entry["need"] += qty
            entry["desc"] = entry["desc"] or str(item.get("desc") or "")
            entry["orders"].append({
                "order_id": group.get("order_id"),
                "code": group.get("batch_code") or "",
                "model": group.get("model") or "",
                "po_number": group.get("po_number") or "",
                "ship_date": group.get("ship_date") or "",
                "qty": qty,
            })
    for entry in parts.values():
        entry["earliest"] = min(
            entry["orders"],
            key=lambda o: (parse_ship_date(o["ship_date"]) or date.max),
        )
    return parts


def _customer_supplied_item(
    key: str, entry: Dict[str, Any], product: Dict[str, Any], reason: str, lookups: Dict[str, Any]
) -> Dict[str, Any]:
    """客供料明細：庫存 = 客供倉庫（預設 002）合計，不是 ST；查不到庫存時 stock / shortage 為 None。"""
    known = not lookups["tab_stock_error"]
    stock = float(lookups["tab_stock"].get(key, 0.0)) if known else None
    return {
        "part_number": key,
        "description": product.get("name") or entry["desc"],
        "reason": reason,
        "need": entry["need"],
        "tab_stock": stock,
        "stock_known": known,
        "stock_error": lookups["tab_stock_error"],
        "ware_ids": lookups["tab_ware_ids"],
        "shortage": max(0.0, entry["need"] - stock) if known else None,
        "earliest_order": {k: entry["earliest"][k] for k in ("order_id", "code", "model", "po_number", "ship_date")},
        "orders": entry["orders"],
    }


def build_shortage_list(order_ids: List[int]) -> Dict[str, Any]:
    from ..routers import dispatch as dispatch_router  # 延遲載入避免循環相依

    today = local_now().date()
    groups = dispatch_router.build_dispatch_groups(order_ids, {})
    conversions = part_conversion.active_map()
    sources = part_conversion.sources_by_target(conversions)
    aggregated = _aggregate(groups, conversions)
    keys = sorted(aggregated)

    st_stock = {
        str(p or "").strip().upper(): float(q or 0) for p, q in db.get_st_inventory_stock().items()
    }
    lookups = _erp_lookups(keys, today)
    overrides = db.get_part_purchase_categories(keys)

    parts_out: List[Dict[str, Any]] = []
    customer_supplied: List[Dict[str, Any]] = []
    for key in keys:
        entry = aggregated[key]
        product = lookups["products"].get(key, {})
        reason = erp_purchase.customer_supplied_reason(key, product.get("class_id", ""))
        if reason:
            customer_supplied.append(_customer_supplied_item(key, entry, product, reason, lookups))
            continue
        last = lookups["last_po"].get(key, {})
        supplier_id, source = resolve_supplier(
            product.get("major_supplier", ""), last.get("supplier_id", ""), lookups["suppliers"]
        )
        description = product.get("name") or entry["desc"]
        category_source = "manual" if key in overrides else "auto"
        category = overrides.get(key) or classify_part(key, entry["desc"] or description)
        # 轉換料：B 的有效庫存 = B + 併入的 A（A 可為負），與 A→B 之後 A 的需求一併計算
        st, st_own, merged_from = part_conversion.effective_stock(key, st_stock, sources)
        open_qty, open_date = lookups["on_order"].get(key, (0.0, ""))
        # 與發料單同一套規則：需求 − ST；在途僅供參考，不扣除
        shortage = float(summarize_requested_supply(entry["need"], st)["purchase_needed_qty"])

        ship = parse_ship_date(entry["earliest"]["ship_date"])
        raw_dates = {c: pre_in_date_for(c, ship) if ship else None for c in CATEGORY_LABELS}
        clamped = {c: clamp_pre_in_date(d, today) if d else None for c, d in raw_dates.items()}
        dates = {c: v[0].isoformat() if v else "" for c, v in clamped.items()}
        original_dates = {c: d.isoformat() if d else "" for c, d in raw_dates.items()}
        adjusted = {c: bool(v and v[1]) for c, v in clamped.items()}
        suggested = int(math.ceil(shortage - 1e-9)) if shortage > 0 else 0
        parts_out.append({
            "part_number": key,
            "description": description,
            "category": category,
            "category_source": category_source,
            "supplier_id": supplier_id,
            "supplier_name": lookups["suppliers"].get(supplier_id, ""),
            "supplier_source": source,
            "need": entry["need"],
            "st": st,
            "st_own": st_own,
            "merged_from": merged_from,
            "on_order": float(open_qty or 0),
            "on_order_date": open_date,
            "shortage": shortage,
            "needs_purchase": shortage > 0,
            "suggested_qty": suggested,
            "allocations": allocate_purchase_to_orders(entry["orders"], st, suggested),
            "price": float(last.get("price") or 0),
            "last_bill_no": last.get("bill_no", ""),
            "last_bill_date": last.get("bill_date", ""),
            "pre_in_dates": dates,
            "pre_in_date": dates[category],
            "original_pre_in_date": original_dates[category],
            "date_adjusted": adjusted[category],
            "original_pre_in_dates": original_dates,
            "dates_adjusted": adjusted,
            "earliest_order": {k: entry["earliest"][k] for k in ("order_id", "code", "model", "po_number", "ship_date")},
            "orders": entry["orders"],
        })
    parts_out.sort(key=lambda p: (p["supplier_id"], p["part_number"]))

    orders = [o for o in (db.get_order(int(i)) for i in dict.fromkeys(order_ids)) if o]
    warnings = list(lookups["warnings"])
    if not parts_out and not customer_supplied:
        warnings.append("選取的訂單沒有補料 / 缺料內容")
    return {
        "config": erp_purchase.get_config(),
        "today": today.isoformat(),
        "orders": [_order_summary(o) for o in orders],
        "parts": parts_out,
        "customer_supplied": customer_supplied,
        "customer_supplied_ware_ids": lookups["tab_ware_ids"],
        "warnings": warnings,
    }


# ── 建立多張採購單 ───────────────────────────────────────────────────────────

def create_many(groups: List[Dict[str, Any]], actor: str = "web", client_ip: str = "") -> List[Dict[str, Any]]:
    """每個供應商一張，逐張呼叫 create_po（各自獨立 transaction）；單張失敗不中斷其他張。"""
    if len(groups) > MAX_GROUPS:
        raise erp_purchase.PoValidationError(f"一次最多建立 {MAX_GROUPS} 張採購單")
    results: List[Dict[str, Any]] = []
    for group in groups:
        supplier_id = str(group.get("supplier_id") or "").strip()
        try:
            r = erp_purchase.create_po({**group, "confirm": True}, actor=actor, client_ip=client_ip)
            results.append({
                "supplier_id": supplier_id, "ok": True, "bill_no": r["bill_no"],
                "supplier": r["supplier"], "totals": r["totals"], "lines": r["lines"],
            })
        except Exception as exc:
            results.append({
                "supplier_id": supplier_id, "ok": False, "error": erp_purchase._safe_message(exc),
            })
    ok_count = sum(1 for r in results if r["ok"])
    try:
        db.log_activity(
            "shortage_po_created",
            f"缺料採購：{len(results)} 張採購單，成功 {ok_count}、失敗 {len(results) - ok_count}",
        )
    except Exception:
        pass
    return results
