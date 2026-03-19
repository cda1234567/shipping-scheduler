"""
改造後的計算引擎 — 已發料隔離 + Running Balance 混合方案。

1. 從快照取起始庫存
2. 先扣掉所有「已發料」的消耗（鎖死不動）
3. 再對未發料的行跑 running balance（保留現有邏輯的可讀性）
"""
from __future__ import annotations
from ..models import calc_suggested_qty
from .shortage_rules import (
    calculate_current_order_shortage_amount,
    calculate_shortage_amount,
    is_order_scoped_shortage_part,
    summarize_requested_supply,
    summarize_st_supply,
)


def _build_shortage_item(summary: dict, moq: dict[str, float], st_inventory_stock: dict[str, float] | None = None) -> dict:
    shortage_amt = calculate_current_order_shortage_amount(
        summary["part_number"],
        float(summary.get("current_stock") or 0) + float(summary.get("prev_qty_cs") or 0),
        float(summary.get("needed") or 0),
    )
    item_moq = moq.get(summary["part_key"], 0.0)
    st_stock_qty = (st_inventory_stock or {}).get(summary["part_key"], 0.0)
    if is_order_scoped_shortage_part(summary["part_number"]):
        st_context = summarize_requested_supply(shortage_amt, st_stock_qty)
        st_available_qty = float(st_context["st_available_qty"] or 0.0)
        purchase_needed_qty = float(st_context["purchase_needed_qty"] or 0.0)
        purchase_suggested_qty = purchase_needed_qty
        suggested_qty = shortage_amt
    else:
        st_context = summarize_st_supply(shortage_amt, st_stock_qty, item_moq)
        st_available_qty = float(st_context["st_available_qty"] or 0.0)
        purchase_needed_qty = float(st_context["purchase_needed_qty"] or 0.0)
        purchase_suggested_qty = calc_suggested_qty(purchase_needed_qty, item_moq) if purchase_needed_qty > 0 else 0.0
        suggested_qty = st_available_qty + purchase_suggested_qty
    return {
        "part_number": summary["part_number"],
        "description": summary["description"],
        "shortage_amount": shortage_amt,
        "current_stock": summary["current_stock"],
        "needed": summary["needed"],
        "moq": item_moq,
        "suggested_qty": suggested_qty if shortage_amt > 0 else 0.0,
        "purchase_suggested_qty": purchase_suggested_qty,
        "decision": "None",
        **st_context,
    }


def run(
    orders: list[dict],
    bom_map: dict[str, list[dict]],
    snapshot_stock: dict[str, float],
    moq: dict[str, float],
    dispatched_consumption: dict[str, float] | None = None,
    st_inventory_stock: dict[str, float] | None = None,
) -> list[dict]:
    """
    依 orders 順序做 running balance，回傳每個 order 的料況。

    Parameters
    ----------
    orders : 未發料的訂單列表（已排序）
    bom_map : { MODEL_UPPER: [component_dict, ...] }
    snapshot_stock : 快照庫存 { PART_UPPER: qty }
    moq : { PART_UPPER: moq_value }
    dispatched_consumption : 已發料總消耗 { PART_UPPER: total_needed }
                             如果提供，會先從快照扣掉

    Returns
    -------
    [{ order_id, po_number, pcb, model, status, shortages, customer_material_shortages }]
    """
    # Step 1: 從快照複製一份 running balance
    running = dict(snapshot_stock)

    # Step 2: 扣掉已發料的消耗（鎖死部分）
    if dispatched_consumption:
        for part, consumed in dispatched_consumption.items():
            running[part.upper()] = running.get(part.upper(), 0) - consumed

    # Step 3: 對未發料的行逐列 running balance
    results: list[dict] = []

    for order in orders:
        model_key = (order.get("model") or "").upper()
        components = bom_map.get(model_key)

        if components is None:
            results.append({
                "order_id":   order.get("id"),
                "po_number":  order.get("po_number"),
                "pcb":        order.get("pcb"),
                "model":      order.get("model"),
                "status":     "no_bom",
                "shortages":  [],
                "customer_material_shortages": [],
            })
            continue

        shortages: list[dict] = []
        part_summaries: dict[str, dict] = {}

        for comp in components:
            is_dash = comp.get("is_dash", False)
            needed_qty = comp.get("needed_qty", 0)
            if is_dash or needed_qty <= 0:
                continue

            part = comp.get("part_number", "").upper()
            summary = part_summaries.get(part)
            if summary is None:
                summary = {
                    "part_key": part,
                    "part_number": comp.get("part_number", ""),
                    "description": comp.get("description", ""),
                    "current_stock": running.get(part, 0.0),
                    "needed": 0.0,
                    "prev_qty_cs": 0.0,
                    "ending_stock": running.get(part, 0.0),
                }
                part_summaries[part] = summary
            elif not summary["description"] and comp.get("description", ""):
                summary["description"] = comp.get("description", "")

            g = running.get(part, 0.0)
            f = needed_qty
            h = comp.get("prev_qty_cs", 0)
            j = g + h - f
            running[part] = j
            summary["needed"] += f
            summary["prev_qty_cs"] += h
            summary["ending_stock"] = j

        for summary in part_summaries.values():
            if calculate_shortage_amount(summary["part_number"], summary["ending_stock"]) <= 0:
                continue

            shortages.append(_build_shortage_item(summary, moq, st_inventory_stock))

        has_shortage = bool(shortages)
        results.append({
            "order_id":   order.get("id"),
            "po_number":  order.get("po_number"),
            "pcb":        order.get("pcb"),
            "model":      order.get("model"),
            "status":     "shortage" if has_shortage else "ok",
            "shortages":  shortages,
            "customer_material_shortages": [],
        })

    return results
