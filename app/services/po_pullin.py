"""客人改交期 → 廠商提前清單。

選訂單 + 輸入新出貨日 → 依缺料採購同一套規則（pre_in_date_for + clamp_pre_in_date）重算每個缺料料號的
「需要到貨日」→ 對照正航未結採購單（唯讀，沿用 low_stock._query_on_order 的未結量邏輯）→ 列出哪些採購單項次
要請廠商提前、提前幾天；缺料但沒有採購單（或採購單量不夠）者列為「未下單」。

- 正航全程只做 SELECT；查詢失敗時 erp_error 帶訊息，本機資料（需求 / 缺料 / 需要到貨日 / 本網站開單的分配紀錄）照常回傳，
  缺料料號標為「未驗證」而不是「未下單」。
- 依供應商分組產生詢問信草稿（純文字，只供複製；本系統不寄信）。
- 每一行的詢問狀態存在本機 po_pullin_items（line_key 為鍵），重跑清單不會洗掉狀態。
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from .. import database as db
from . import erp_purchase, low_stock, part_conversion, shortage_purchase as sp
from .local_time import local_now

logger = logging.getLogger(__name__)

STATUSES = db.PULLIN_STATUSES
UNRESOLVED = db.PULLIN_UNRESOLVED


class PullinError(ValueError):
    pass


# ── 純函式 ───────────────────────────────────────────────────────────────────

def make_line_key(supplier_id: str, bill_no: str, row_no: int, part: str) -> str:
    return "|".join([str(supplier_id or "").strip(), str(bill_no or "").strip(), str(int(row_no or 0)),
                     str(part or "").strip().upper()])


def required_date(category: str, new_ship: date, today: date) -> Tuple[date, bool, date]:
    """(需要到貨日, 是否因早於今天而改用今天, 原推算日)；規則與缺料採購相同。"""
    raw = sp.pre_in_date_for(category, new_ship)
    adjusted, was_adjusted = sp.clamp_pre_in_date(raw, today)
    return adjusted, was_adjusted, raw


def plan_part(shortage: float, required: date, po_lines: List[Dict[str, Any]]) -> Dict[str, Any]:
    """單一料號的提前判斷（純函式）。

    po_lines 每項 {qty, po_date(date|None), ...}，依交期由早到晚消耗缺料量：
    交期不晚於需要到貨日的行先抵缺料；其餘（交期較晚、且缺料尚未被前面的行抵完）才需要請廠商提前。
    回傳 {pull_in: [{line, pull_qty, days_earlier}], on_time: [line], unordered_qty}。
    交期未知（None）的行視為最晚。
    """
    remaining = max(float(shortage or 0), 0.0)
    ordered = sorted(po_lines, key=lambda l: (l["po_date"] is None, l["po_date"] or date.max))
    pull_in: List[Dict[str, Any]] = []
    on_time: List[Dict[str, Any]] = []
    for line in ordered:
        if remaining <= 1e-9:
            break
        qty = max(float(line.get("qty") or 0), 0.0)
        po_date = line["po_date"]
        if po_date is not None and po_date <= required:
            on_time.append(line)
        else:
            days = (po_date - required).days if po_date else 0
            pull_in.append({"line": line, "pull_qty": min(qty, remaining), "days_earlier": days})
        remaining -= qty
    return {"pull_in": pull_in, "on_time": on_time, "unordered_qty": max(remaining, 0.0)}


def build_draft(supplier_name: str, lines: List[Dict[str, Any]]) -> str:
    """詢問提前交貨的草稿（繁體中文、純文字）。只列尚未有結論（未詢問 / 已詢問）的行。"""
    todo = [l for l in lines if l.get("status", "未詢問") in UNRESOLVED]
    if not todo:
        return ""
    out = [f"{supplier_name or '廠商'} 您好，", "",
           "因客戶變更出貨日期，想請教下列採購單項目是否有機會提前交貨：", ""]
    for i, l in enumerate(todo, 1):
        out.append(
            f"{i}. 採購單 {l['bill_no']} 項次 {l['row_no']}　料號 {l['part_number']}　數量 {_fmt_qty(l['qty'])}　"
            f"原交期 {l['po_date'] or '未定'}　希望提前至 {l['requested_date']}（提前 {l['days_earlier']} 天）"
        )
    out += ["", "麻煩協助確認可提前的數量與日期；若無法提前也請告知，謝謝您的協助！"]
    return "\n".join(out)


def _fmt_qty(value: Any) -> str:
    n = float(value or 0)
    return str(int(n)) if n == int(n) else f"{n:g}"


# ── 正航唯讀查詢 ─────────────────────────────────────────────────────────────

def _query_open_po_lines(conn, parts: List[str], today: date, max_overdue_days: int) -> List[Dict[str, Any]]:
    """未結採購單明細（只有 SELECT）：條件與 low_stock._query_on_order 相同，但保留每一列與供應商。"""
    from datetime import timedelta

    cutoff = low_stock._yyyymmdd(today - timedelta(days=int(max_overdue_days)))
    rows: List[Dict[str, Any]] = []
    cur = conn.cursor()
    for batch in low_stock._chunks(sorted({low_stock.norm_part(p) for p in parts if low_stock.norm_part(p)})):
        marks = low_stock._in_clause(len(batch))
        cur.execute(
            "SELECT c.ProdID, c.BillNO, c.RowNO, c.Quantity - COALESCE(rcv.recv_qty, 0), c.PreInDate, c.CustomerID FROM ("
            "SELECT s.ProdID, s.BillNO, s.RowNO, s.Quantity, s.PreInDate, h.CustomerID FROM ordBillSub s "
            "JOIN ordBillMain h ON h.Flag = 4 AND h.BillNO = s.BillNO "
            "WHERE s.Flag = 4 AND h.BillStatus = 0 AND h.EndacaseDate = 0 "
            f"AND s.QtyRemain > 0 AND s.PreInDate >= %s AND s.ProdID IN {marks}) c "
            "LEFT JOIN ("
            "SELECT m.FromNO AS BillNO, m.FromRow AS RowNO, SUM(r.Quantity) AS recv_qty "
            "FROM comProdRec r "
            "JOIN ordExamineSub x ON x.ExamineNO = r.FromNO AND x.RowNO = r.FromRow "
            "JOIN ordExamineSub m ON m.ExamineNO = x.FromNO AND m.RowNO = x.FromRow "
            f"WHERE r.Flag = 100 AND r.ProdID IN {marks} "
            "GROUP BY m.FromNO, m.FromRow) rcv ON rcv.BillNO = c.BillNO AND rcv.RowNO = c.RowNO",
            (cutoff, *batch, *batch),
        )
        for prod_id, bill_no, row_no, open_qty, pre_in, supplier in cur.fetchall():
            qty = float(open_qty or 0)
            part = low_stock.norm_part(prod_id)
            if not part or qty <= 0:
                continue
            rows.append({
                "part_number": part,
                "bill_no": erp_purchase._text(bill_no),
                "row_no": int(row_no or 0),
                "qty": qty,
                "po_date": low_stock._to_date(pre_in),
                "supplier_id": erp_purchase._text(supplier),
            })
    return rows


def _fetch_open_lines(parts: List[str], today: date) -> List[Dict[str, Any]]:
    max_overdue = low_stock.get_config()["on_order_max_overdue_days"]
    with low_stock._erp_session() as conn:
        return _query_open_po_lines(conn, parts, today, max_overdue)


def _fetch_tab_stock(parts: List[str], ware_ids: List[str]) -> Dict[str, float]:
    """客供料倉庫（預設 002）庫存，沿用缺料採購的查詢。"""
    with low_stock._erp_session() as conn:
        return sp._query_tab_stock(conn, parts, ware_ids)


def _fetch_supplier_names(ids: List[str]) -> Dict[str, str]:
    if not ids:
        return {}
    with erp_purchase._source_session() as conn:
        return sp._lookup_suppliers(conn, sorted(set(ids)))


# ── 主流程 ───────────────────────────────────────────────────────────────────

def parse_new_ship_date(value: Any) -> date:
    parsed = sp.parse_ship_date(value)
    if not parsed:
        raise PullinError("新出貨日格式不正確（需為 YYYY-MM-DD）")
    return parsed


def _allocation_index(bill_nos: List[str]) -> Dict[Tuple[str, int], List[Dict[str, Any]]]:
    index: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    for bill_no in sorted({b for b in bill_nos if b}):
        for a in db.get_po_line_allocations(bill_no=bill_no):
            index.setdefault((a["bill_no"], int(a["row_no"])), []).append({
                "order_code": a.get("order_code") or "", "model": a.get("model") or "",
                "customer_po": a.get("customer_po") or "", "ship_date": a.get("ship_date") or "",
                "qty": float(a.get("qty") or 0), "kind": a.get("kind") or "",
            })
    return index


def _parse_order_ids(order_ids: List[int]) -> List[int]:
    ids = list(dict.fromkeys(int(i) for i in (order_ids or []) if i))
    if not ids:
        raise PullinError("請先選擇訂單")
    return ids


def build_pullin_list(order_ids: List[int], new_ship_date: Any) -> Dict[str, Any]:
    from ..routers import dispatch as dispatch_router  # 延遲載入避免循環相依

    ids = _parse_order_ids(order_ids)
    new_ship = parse_new_ship_date(new_ship_date)
    today = local_now().date()

    groups = dispatch_router.build_dispatch_groups(ids, {})
    conversions = part_conversion.active_map()
    sources = part_conversion.sources_by_target(conversions)
    aggregated = sp._aggregate(groups, conversions)
    keys = sorted(aggregated)
    st_stock = {str(p or "").strip().upper(): float(q or 0) for p, q in db.get_st_inventory_stock().items()}
    overrides = db.get_part_purchase_categories(keys)

    warnings: List[str] = []
    # 客供料（僅料號 -TAB 結尾，與缺料採購同一判斷）：不採購，不列未下單 / 草稿 / 徽章
    cs_keys = [k for k in keys if erp_purchase.customer_supplied_reason(k)]
    tab_wares = sp.tab_ware_ids()
    tab_stock: Dict[str, float] = {}
    tab_error = ""
    if cs_keys:
        try:
            tab_stock = _fetch_tab_stock(cs_keys, tab_wares)
        except Exception as exc:
            logger.warning("提前清單：客供料庫存查詢失敗：%s", type(exc).__name__)
            tab_error = f"客供料倉庫（{'、'.join(tab_wares)}）庫存查詢失敗（{erp_purchase._safe_message(exc)}）"
            warnings.append(tab_error)
    customer_supplied: List[Dict[str, Any]] = []
    for key in cs_keys:
        entry = aggregated[key]
        category = overrides.get(key) or sp.classify_part(key, entry["desc"])
        req, adjusted, _raw = required_date(category, new_ship, today)
        stock = None if tab_error else float(tab_stock.get(key, 0.0))
        customer_supplied.append({
            "part_number": key, "description": entry["desc"], "need": entry["need"],
            "tab_stock": stock, "stock_known": not tab_error, "stock_error": tab_error,
            "ware_ids": tab_wares,
            "shortage": None if stock is None else max(0.0, entry["need"] - stock),
            "required_date": req.isoformat(), "date_adjusted": adjusted,
        })
    keys = [k for k in keys if k not in cs_keys]

    parts_info: Dict[str, Dict[str, Any]] = {}
    for key in keys:
        entry = aggregated[key]
        category = overrides.get(key) or sp.classify_part(key, entry["desc"])
        st, _own, merged_from = part_conversion.effective_stock(key, st_stock, sources)
        shortage = float(sp.summarize_requested_supply(entry["need"], st)["purchase_needed_qty"])
        req, adjusted, raw = required_date(category, new_ship, today)
        parts_info[key] = {
            "part_number": key, "description": entry["desc"], "category": category,
            "category_label": sp.CATEGORY_LABELS[category], "need": entry["need"], "st": st,
            "merged_from": merged_from, "shortage": shortage,
            "required_date": req.isoformat(), "original_required_date": raw.isoformat(),
            "date_adjusted": adjusted, "po_lines": [], "unordered_qty": 0.0, "status": "no_shortage",
        }
    short_parts = [k for k, v in parts_info.items() if v["shortage"] > 0]

    erp_error = ""
    open_lines: List[Dict[str, Any]] = []
    names: Dict[str, str] = {}
    if short_parts:
        try:
            open_lines = _fetch_open_lines(short_parts, today)
        except Exception as exc:
            logger.warning("提前清單：正航未結採購單查詢失敗：%s", type(exc).__name__)
            erp_error = f"正航採購單查詢失敗（{erp_purchase._safe_message(exc)}）"
        if not erp_error:
            try:
                names = _fetch_supplier_names([l["supplier_id"] for l in open_lines if l["supplier_id"]])
            except Exception as exc:
                logger.warning("提前清單：供應商名稱查詢失敗：%s", type(exc).__name__)
                warnings.append(f"供應商名稱查詢失敗，僅顯示代號（{erp_purchase._safe_message(exc)}）")
    if erp_error:
        warnings.insert(0, erp_error + "；以下僅顯示本機資料，無法判斷是否已下單 / 要提前哪張採購單")

    alloc = _allocation_index([l["bill_no"] for l in open_lines])
    by_part: Dict[str, List[Dict[str, Any]]] = {}
    for l in open_lines:
        by_part.setdefault(l["part_number"], []).append(l)

    pull_lines: List[Dict[str, Any]] = []
    for key in short_parts:
        info = parts_info[key]
        if erp_error:
            info["status"] = "unverified"
            continue
        req = date.fromisoformat(info["required_date"])
        plan = plan_part(info["shortage"], req, by_part.get(key, []))
        info["unordered_qty"] = plan["unordered_qty"]
        for line in plan["on_time"]:
            info["po_lines"].append(_line_view(line, key, names, alloc, info, pull=False))
        for item in plan["pull_in"]:
            view = _line_view(item["line"], key, names, alloc, info, pull=True)
            view.update(pull_qty=item["pull_qty"], days_earlier=item["days_earlier"])
            info["po_lines"].append(view)
            if item["days_earlier"] > 0:
                pull_lines.append(view)
        if info["unordered_qty"] > 1e-9:
            info["status"] = "unordered"
        elif any(l["needs_pull_in"] for l in info["po_lines"]):
            info["status"] = "pull_in"
        else:
            info["status"] = "on_time"

    saved = db.get_pullin_statuses([l["line_key"] for l in pull_lines])
    for l in pull_lines:
        rec = saved.get(l["line_key"])
        l["status"] = rec["status"] if rec else "未詢問"
        l["note"] = rec["note"] if rec else ""

    suppliers = _group_suppliers(pull_lines)
    orders = [o for o in (db.get_order(i) for i in ids) if o]
    unordered = [p for p in parts_info.values() if p["status"] == "unordered"]
    return {
        "today": today.isoformat(),
        "new_ship_date": new_ship.isoformat(),
        "orders": [sp._order_summary(o) for o in orders],
        "erp_ok": not erp_error,
        "erp_error": erp_error,
        "warnings": warnings,
        "parts": [parts_info[k] for k in keys if parts_info[k]["shortage"] > 0],
        "suppliers": suppliers,
        "unordered": unordered,
        "customer_supplied": customer_supplied,
        "summary": {
            "customer_supplied_parts": len(customer_supplied),
            "shortage_parts": len(short_parts),
            "pull_in_lines": len(pull_lines),
            "suppliers": len(suppliers),
            "unordered_parts": len(unordered),
            "unverified_parts": sum(1 for p in parts_info.values() if p["status"] == "unverified"),
            "unresolved": sum(1 for l in pull_lines if l["status"] in UNRESOLVED),
        },
    }


def _line_view(line: Dict[str, Any], part: str, names: Dict[str, str], alloc, info: Dict[str, Any], pull: bool) -> Dict[str, Any]:
    supplier_id = line.get("supplier_id") or ""
    po_date = line["po_date"].isoformat() if line.get("po_date") else ""
    return {
        "line_key": make_line_key(supplier_id, line["bill_no"], line["row_no"], part),
        "supplier_id": supplier_id,
        "supplier_name": names.get(supplier_id, ""),
        "bill_no": line["bill_no"],
        "row_no": line["row_no"],
        "part_number": part,
        "description": info["description"],
        "qty": line["qty"],
        "po_date": po_date,
        "requested_date": info["required_date"],
        "needs_pull_in": bool(pull and line.get("po_date") is not None and line["po_date"] > date.fromisoformat(info["required_date"])),
        "pull_qty": 0.0,
        "days_earlier": 0,
        "allocations": alloc.get((line["bill_no"], line["row_no"]), []),
        "status": "未詢問",
        "note": "",
    }


def _group_suppliers(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for l in lines:
        grouped.setdefault(l["supplier_id"], []).append(l)
    out = []
    for supplier_id in sorted(grouped):
        items = sorted(grouped[supplier_id], key=lambda l: (l["bill_no"], l["row_no"]))
        name = next((l["supplier_name"] for l in items if l["supplier_name"]), "") or supplier_id
        out.append({
            "supplier_id": supplier_id,
            "supplier_name": name,
            "lines": items,
            "unresolved": sum(1 for l in items if l["status"] in UNRESOLVED),
            "draft_text": build_draft(name, items),
        })
    return out


# ── 確認：保存清單、選擇性更新訂單出貨日 ─────────────────────────────────────

def update_order_ship_dates(order_ids: List[int], new_ship: date) -> List[Dict[str, Any]]:
    """沿用「改交期」的做法：更新 delivery_date / ship_date、寫操作日誌；已發料的訂單另外建提醒。"""
    from ..models import AlertType

    text = new_ship.isoformat()
    updated = []
    for oid in order_ids:
        order = db.get_order(int(oid))
        if not order:
            continue
        old = order.get("delivery_date") or order.get("ship_date") or "未知"
        if order["status"] in ("dispatched", "completed"):
            db.create_alert(
                AlertType.DELIVERY_CHANGE,
                f"訂單 {order['po_number']} ({order['model']}) 已發料，客人改交期 {old} → {text}",
                order_id=int(oid),
            )
        db.update_order(int(oid), delivery_date=text, ship_date=text)
        db.log_activity("delivery_changed", f"訂單 {order['po_number']} 交期 {old} → {text}（廠商提前清單）")
        updated.append({"id": int(oid), "code": order.get("code") or "", "old": old, "new": text})
    return updated


def confirm(order_ids: List[int], new_ship_date: Any, update_order_ship_date: bool = False,
            dry_run: bool = False, actor: str = "web") -> Dict[str, Any]:
    ids = _parse_order_ids(order_ids)
    new_ship = parse_new_ship_date(new_ship_date)
    result = build_pullin_list(ids, new_ship.isoformat())
    lines = [l for s in result["suppliers"] for l in s["lines"]]
    existing = [o["id"] for o in result["orders"]]
    if dry_run:
        return {**result, "dry_run": True, "saved_lines": 0,
                "would_update_orders": existing if update_order_ship_date else []}
    saved = db.upsert_pullin_items([{**l, "order_ids": ids} for l in lines], actor=actor)
    updated = update_order_ship_dates(ids, new_ship) if update_order_ship_date else []
    return {**result, "dry_run": False, "saved_lines": saved, "updated_orders": updated}


def set_status(lines: List[Dict[str, Any]], status: str, note: Optional[str] = None, actor: str = "web") -> int:
    if status not in STATUSES:
        raise PullinError(f"狀態需為 {' / '.join(STATUSES)}")
    clean = []
    for l in lines:
        key = l.get("line_key") or make_line_key(
            l.get("supplier_id", ""), l.get("bill_no", ""), l.get("row_no", 0), l.get("part_number", ""))
        clean.append({**l, "line_key": key})
    return db.upsert_pullin_items(clean, status=status, actor=actor, note=note)


def unresolved_count() -> int:
    return db.count_unresolved_pullin_items()
