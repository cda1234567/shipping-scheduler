"""
改造後的計算引擎 — 已發料隔離 + Running Balance 混合方案。

1. 從快照取起始庫存
2. 先扣掉所有「已發料」的消耗（鎖死不動）
3. 再對未發料的行跑 running balance（保留現有邏輯的可讀性）
4. 客供料單獨標記，不計入採購清單
"""
from __future__ import annotations
from ..models import calc_suggested_qty


def run(
    orders: list[dict],
    bom_map: dict[str, list[dict]],
    snapshot_stock: dict[str, float],
    moq: dict[str, float],
    dispatched_consumption: dict[str, float] | None = None,
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
        cs_shortages: list[dict] = []

        for comp in components:
            is_dash = comp.get("is_dash", False)
            needed_qty = comp.get("needed_qty", 0)
            if is_dash or needed_qty <= 0:
                continue

            part = comp.get("part_number", "").upper()
            is_cs = comp.get("is_customer_supplied", False)
            g = running.get(part, 0.0)
            f = needed_qty
            h = comp.get("prev_qty_cs", 0)
            j = g + h - f
            running[part] = j

            if g >= 0 and j < 0:
                shortage_amt = abs(j)
                item_moq = moq.get(part, 0.0)
                suggested = calc_suggested_qty(shortage_amt, item_moq)
                shortage_item = {
                    "part_number":     comp.get("part_number", ""),
                    "description":     comp.get("description", ""),
                    "shortage_amount": shortage_amt,
                    "current_stock":   g,
                    "needed":          f,
                    "moq":             item_moq,
                    "suggested_qty":   suggested,
                    "decision":        "None",
                    "is_customer_supplied": is_cs,
                }
                if is_cs:
                    cs_shortages.append(shortage_item)
                else:
                    shortages.append(shortage_item)

        has_shortage = bool(shortages) or bool(cs_shortages)
        results.append({
            "order_id":   order.get("id"),
            "po_number":  order.get("po_number"),
            "pcb":        order.get("pcb"),
            "model":      order.get("model"),
            "status":     "shortage" if has_shortage else "ok",
            "shortages":  shortages,
            "customer_material_shortages": cs_shortages,
        })

    return results
