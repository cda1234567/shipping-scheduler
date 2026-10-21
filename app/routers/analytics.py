"""發料趨勢分析、歷史統計、排程差異比對 API"""
from __future__ import annotations

import tempfile
from difflib import SequenceMatcher
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File

from .. import database as db
from ..services.main_reader import read_vendors
from ..services.material_risk import build_frequent_zero_stock_analysis
from ..services.schedule_parser import parse_schedule

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _is_hidden_analysis_part(part_number: object) -> bool:
    part = str(part_number or "").strip().upper()
    return part.startswith(("EC-1", "EC-2"))


# ── 料號模糊建議 ──────────────────────────────────────────────────────────────

_PART_STRIP_CHARS = ("-", " ", "_", ".")


def _normalize_part_text(value: object) -> str:
    return str(value or "").strip().upper()


def _compact_part_text(value: str) -> str:
    compact = value
    for ch in _PART_STRIP_CHARS:
        compact = compact.replace(ch, "")
    return compact


def _levenshtein_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr[j] = min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost)
        prev = curr
    return prev[-1]


def _best_window_similarity(query: str, candidate: str) -> float:
    """以與 query 等長的候選子字串逐一比對，取最高相似度（處理部分輸入中的錯字）。"""
    if not query or not candidate:
        return 0.0
    qlen = len(query)
    if len(candidate) <= qlen:
        return SequenceMatcher(None, query, candidate).ratio()
    best = 0.0
    for i in range(len(candidate) - qlen + 1):
        ratio = SequenceMatcher(None, query, candidate[i:i + qlen]).ratio()
        if ratio > best:
            best = ratio
    return best


def rank_part_suggestions(query: str, candidates: list[str], limit: int = 20) -> list[dict]:
    """依 query 比對候選料號並評分排序，供部分輸入／錯字容錯的自動建議使用。"""
    normalized_query = _normalize_part_text(query)
    normalized_limit = max(1, min(int(limit or 20), 50))
    if not normalized_query:
        return []
    compact_query = _compact_part_text(normalized_query)

    seen: set[str] = set()
    scored: list[tuple[int, float, str, str, float]] = []

    for cand in candidates:
        normalized_cand = _normalize_part_text(cand)
        if not normalized_cand or normalized_cand in seen:
            continue
        seen.add(normalized_cand)
        compact_cand = _compact_part_text(normalized_cand)

        if normalized_cand == normalized_query or compact_cand == compact_query:
            scored.append((0, 0.0, normalized_cand, "exact", 1.0))
            continue

        if normalized_cand.startswith(normalized_query) or compact_cand.startswith(compact_query):
            len_diff = max(len(normalized_cand) - len(normalized_query), 0)
            score = max(0.8, round(0.95 - len_diff * 0.01, 4))
            scored.append((1, float(len_diff), normalized_cand, "prefix", score))
            continue

        pos = normalized_cand.find(normalized_query)
        if pos < 0:
            pos = compact_cand.find(compact_query)
        if pos >= 0:
            score = max(0.5, round(0.75 - pos * 0.02 - len(normalized_cand) * 0.001, 4))
            scored.append((2, pos * 1000.0 + len(normalized_cand), normalized_cand, "substring", score))
            continue

        max_dist = max(1, len(normalized_query) // 4)
        dist = _levenshtein_distance(normalized_query, normalized_cand)
        window_sim = _best_window_similarity(normalized_query, normalized_cand)
        # 短輸入（<5 字）只認整串編輯距離，避免 3/4 字元相似就誤配一大堆料號
        window_ok = len(normalized_query) >= 5 and window_sim >= 0.8
        if dist <= max_dist or window_ok:
            dist_score = 1 - dist / max(len(normalized_query), len(normalized_cand), 1)
            # 模糊分數壓在 0.49 以下（永遠排在包含之後），但保留相對高低以便排序
            score = round(0.49 * (0.5 * dist_score + 0.5 * window_sim), 4)
            scored.append((3, -score, normalized_cand, "fuzzy", score))

    scored.sort(key=lambda item: (item[0], item[1], item[2]))
    return [
        {"part_number": cand, "match": match, "score": score}
        for _, _, cand, match, score in scored[:normalized_limit]
    ]


# ── 發料趨勢 ──────────────────────────────────────────────────────────────────

@router.get("/dispatch-trend")
async def dispatch_trend(period: str = "month"):
    if period not in ("week", "month"):
        period = "month"
    raw = db.get_dispatch_trend(period)

    periods_set: dict[str, dict[str, float]] = {}
    for row in raw:
        p = row["period"]
        part = row["part_number"]
        if _is_hidden_analysis_part(part):
            continue
        qty = float(row["total_qty"] or 0)
        if p not in periods_set:
            periods_set[p] = {}
        periods_set[p][part] = periods_set[p].get(part, 0) + qty

    periods_sorted = sorted(periods_set.keys())
    all_parts: dict[str, float] = {}
    for parts in periods_set.values():
        for part, qty in parts.items():
            all_parts[part] = all_parts.get(part, 0) + qty
    top_parts = sorted(all_parts, key=lambda p: all_parts[p], reverse=True)[:15]

    chart_data = {
        "labels": periods_sorted,
        "datasets": [
            {
                "label": part,
                "data": [periods_set.get(p, {}).get(part, 0) for p in periods_sorted],
            }
            for part in top_parts
        ],
    }
    return {"chart_data": chart_data, "period": period}


@router.get("/top-parts")
async def top_parts(limit: int = 20, months: int = 6):
    parts = db.get_top_dispatched_parts(limit, months)
    visible_parts = [
        item for item in parts
        if not _is_hidden_analysis_part(item.get("part_number"))
    ]
    return {"parts": visible_parts[:limit]}


@router.get("/frequent-zero-stock")
async def frequent_zero_stock(months: int = 6, min_orders: int = 3):
    """列出近期常用、但主檔與 ST 可用庫存皆為零的料號。"""
    normalized_months = max(1, min(int(months or 6), 24))
    normalized_min_orders = max(1, min(int(min_orders or 3), 100))
    main_snapshot = db.get_snapshot()
    st_snapshot = db.get_st_inventory_snapshot()
    main_path = str(db.get_setting("main_file_path") or "").strip()
    vendors: dict[str, str] = {}
    if main_path and Path(main_path).exists():
        try:
            vendors = read_vendors(main_path)
        except Exception:
            vendors = {}

    result = build_frequent_zero_stock_analysis(
        history_usage=db.get_recent_dispatched_part_usage(normalized_months),
        active_orders=db.get_orders(["pending", "merged"]),
        bom_map=db.get_all_bom_components_by_model(),
        main_snapshot=main_snapshot,
        st_snapshot=st_snapshot,
        vendors=vendors,
        history_months=normalized_months,
        min_order_count=normalized_min_orders,
    )
    result["sources"] = {
        "main_loaded": bool(main_snapshot),
        "main_loaded_at": db.get_snapshot_taken_at(),
        "st_loaded": bool(st_snapshot),
        "st_loaded_at": db.get_setting("st_inventory_loaded_at") or db.get_st_inventory_taken_at(),
        "stock_data_complete": bool(main_snapshot) and bool(st_snapshot),
    }
    return result


# ── 發料歷史統計 ──────────────────────────────────────────────────────────────

@router.get("/part-suggestions")
def part_suggestions(q: str = "", limit: int = 20):
    """輸入部分料號或含錯字時，提供自動建議清單。"""
    normalized_limit = max(1, min(int(limit or 20), 50))
    query = str(q or "").strip()
    if len(query) < 2:
        return {"query": query, "suggestions": []}

    candidates = [
        part for part in db.get_dispatch_part_numbers()
        if not _is_hidden_analysis_part(part)
    ]
    suggestions = rank_part_suggestions(query, candidates, normalized_limit)
    return {"query": query, "suggestions": suggestions}


@router.get("/part-dispatch-history")
def part_dispatch_history(part_number: str = "", days: int = 30):
    """查詢單一料號在最近一段期間內的發料 BOM 用量與訂單明細。"""
    normalized_part = str(part_number or "").strip().upper()
    if not normalized_part:
        raise HTTPException(400, "請輸入料號")

    normalized_days = 0 if days == 0 else max(1, min(int(days or 30), 3650))
    rows = db.get_part_dispatch_history(normalized_part, normalized_days)
    total_qty = sum(float(row.get("issued_qty") or 0) for row in rows)
    dispatch_count = sum(int(row.get("record_count") or 0) for row in rows)
    dispatched_times = [str(row.get("dispatched_at") or "") for row in rows if row.get("dispatched_at")]

    return {
        "part_number": normalized_part,
        "days": normalized_days,
        "period_label": "全部紀錄" if normalized_days == 0 else f"最近 {normalized_days} 天",
        "total_qty": total_qty,
        "order_count": len(rows),
        "dispatch_count": dispatch_count,
        "first_dispatched_at": min(dispatched_times) if dispatched_times else "",
        "last_dispatched_at": max(dispatched_times) if dispatched_times else "",
        "rows": rows,
    }


@router.get("/dispatch-history")
async def dispatch_history(group_by: str = "model"):
    if group_by not in ("model", "month"):
        group_by = "model"
    rows = db.get_dispatch_history(group_by)

    if group_by == "month":
        labels = [r["period"] for r in rows]
    else:
        labels = [r["label"] for r in rows]

    chart_data = {
        "labels": labels,
        "datasets": [
            {"label": "訂單數", "data": [r["order_count"] for r in rows]},
            {"label": "總數量", "data": [float(r["total_qty"] or 0) for r in rows]},
        ],
    }
    return {"chart_data": chart_data, "rows": rows, "group_by": group_by}


# ── 排程差異比對 ──────────────────────────────────────────────────────────────

@router.post("/schedule-diff")
async def schedule_diff(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(400, "請選擇排程檔案")

    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".xlsx", ".xls", ".xlsm"):
        raise HTTPException(400, "僅支援 .xlsx / .xls / .xlsm")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        new_rows = parse_schedule(tmp_path)
    except Exception as e:
        Path(tmp_path).unlink(missing_ok=True)
        raise HTTPException(400, f"解析排程失敗：{e}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    existing_orders = db.get_orders(["pending", "merged"])
    old_by_po: dict[int, dict] = {}
    for order in existing_orders:
        po = int(order.get("po_number") or 0)
        if po:
            old_by_po[po] = order

    new_by_po: dict[int, dict] = {}
    for row in new_rows:
        new_by_po[row.po_number] = row.dict()

    diffs = []

    for po, new_data in new_by_po.items():
        if po not in old_by_po:
            diffs.append({
                "type": "added",
                "po_number": po,
                "model": new_data.get("model", ""),
                "pcb": new_data.get("pcb", ""),
                "new_qty": new_data.get("order_qty", 0),
                "new_date": new_data.get("ship_date", ""),
            })
            continue

        old = old_by_po[po]
        changes = []
        old_qty = float(old.get("order_qty") or 0)
        new_qty = float(new_data.get("order_qty") or 0)
        if old_qty != new_qty:
            changes.append({
                "field": "order_qty",
                "label": "數量",
                "old": old_qty,
                "new": new_qty,
            })

        old_date = str(old.get("ship_date") or "")
        new_date = str(new_data.get("ship_date") or "")
        if old_date != new_date:
            changes.append({
                "field": "ship_date",
                "label": "交期",
                "old": old_date,
                "new": new_date,
            })

        old_model = str(old.get("model") or "")
        new_model = str(new_data.get("model") or "")
        if old_model != new_model:
            changes.append({
                "field": "model",
                "label": "機種",
                "old": old_model,
                "new": new_model,
            })

        if changes:
            diffs.append({
                "type": "changed",
                "po_number": po,
                "model": new_model or old_model,
                "pcb": new_data.get("pcb") or old.get("pcb", ""),
                "changes": changes,
            })

    for po, old in old_by_po.items():
        if po not in new_by_po:
            diffs.append({
                "type": "removed",
                "po_number": po,
                "model": old.get("model", ""),
                "pcb": old.get("pcb", ""),
                "old_qty": float(old.get("order_qty") or 0),
                "old_date": str(old.get("ship_date") or ""),
            })

    diffs.sort(key=lambda d: ({"removed": 0, "changed": 1, "added": 2}.get(d["type"], 3), d["po_number"]))

    return {
        "diffs": diffs,
        "summary": {
            "added": sum(1 for d in diffs if d["type"] == "added"),
            "removed": sum(1 for d in diffs if d["type"] == "removed"),
            "changed": sum(1 for d in diffs if d["type"] == "changed"),
            "unchanged": len(old_by_po) - sum(1 for d in diffs if d["type"] in ("changed", "removed")),
        },
        "new_filename": file.filename,
    }
