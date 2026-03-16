"""發料趨勢分析、歷史統計、排程差異比對 API"""
from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File

from .. import database as db
from ..services.schedule_parser import parse_schedule

router = APIRouter(prefix="/analytics", tags=["analytics"])


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
    return {"parts": db.get_top_dispatched_parts(limit, months)}


# ── 發料歷史統計 ──────────────────────────────────────────────────────────────

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
