from __future__ import annotations

from pathlib import Path

from .. import database as db
from .main_reader import read_stock
from .shortage_rules import calculate_current_order_shortage_amount, calculate_shortage_amount, is_order_scoped_shortage_part


def normalize_part_key(value) -> str:
    return str(value or "").strip().upper()


def build_dispatch_running_stock() -> dict[str, float]:
    main_path = str(db.get_setting("main_file_path") or "").strip()
    if main_path and Path(main_path).exists():
        running = {
            normalize_part_key(part): float(qty or 0)
            for part, qty in read_stock(main_path).items()
            if normalize_part_key(part)
        }
    else:
        snapshot = db.get_snapshot()
        running = {
            normalize_part_key(part): float((values or {}).get("stock_qty") or 0)
            for part, values in snapshot.items()
            if normalize_part_key(part)
        }

    return running


def build_order_supplement_allocations(order_ids: list[int], supplements: dict[str, float]) -> dict[int, dict[str, float]]:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return {}

    remaining_supplements = {
        normalize_part_key(part): float(qty or 0)
        for part, qty in (supplements or {}).items()
        if normalize_part_key(part) and float(qty or 0) > 0
    }
    if not remaining_supplements:
        return {order_id: {} for order_id in normalized_ids}

    bom_map = db.get_all_bom_components_by_model()
    running = build_dispatch_running_stock()
    allocations: dict[int, dict[str, float]] = {}

    for order_id in normalized_ids:
        order = db.get_order(order_id)
        if not order:
            allocations[order_id] = {}
            continue

        model_key = normalize_part_key(order.get("model"))
        components = bom_map.get(model_key, [])
        part_totals: dict[str, dict[str, float]] = {}
        for component in components:
            needed_qty = float(component.get("needed_qty") or 0)
            if component.get("is_dash") or needed_qty <= 0:
                continue

            part = normalize_part_key(component.get("part_number"))
            if not part:
                continue

            summary = part_totals.setdefault(part, {"needed_qty": 0.0, "prev_qty_cs": 0.0})
            summary["needed_qty"] += needed_qty
            summary["prev_qty_cs"] += float(component.get("prev_qty_cs") or 0)

        order_allocations: dict[str, float] = {}
        for part, totals in part_totals.items():
            current_stock = float(running.get(part, 0))
            available_before = (
                current_stock
                + float(totals.get("prev_qty_cs") or 0)
            )
            ending_without_supplement = (
                available_before
                - float(totals.get("needed_qty") or 0)
            )
            shortage_without_supplement = calculate_shortage_amount(part, ending_without_supplement)
            current_order_shortage = calculate_current_order_shortage_amount(
                part,
                available_before,
                float(totals.get("needed_qty") or 0),
            )

            supplement_qty = 0.0
            if shortage_without_supplement > 0 and remaining_supplements.get(part, 0) > 0:
                supplement_qty = float(remaining_supplements.get(part, 0))
                if is_order_scoped_shortage_part(part):
                    supplement_qty = min(supplement_qty, current_order_shortage)
                order_allocations[part] = supplement_qty
                remaining_supplements[part] = max(0.0, remaining_supplements.get(part, 0) - supplement_qty)

            running[part] = ending_without_supplement + supplement_qty

        allocations[order_id] = order_allocations

    return allocations
