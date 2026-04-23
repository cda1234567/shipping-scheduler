from __future__ import annotations

from pathlib import Path
from typing import Callable

from .. import database as db
from .bom_quantity import coerce_qty, get_component_effective_needed_qty
from .main_reader import read_stock
from .shortage_rules import calculate_current_order_shortage_amount, calculate_shortage_amount, is_order_scoped_shortage_part


def normalize_part_key(value) -> str:
    return str(value or "").strip().upper()


def normalize_order_id_list(order_ids: list[int] | None) -> list[int]:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    return list(dict.fromkeys(normalized_ids))


def _normalize_explicit_order_supplements(
    order_supplements: dict[int, dict[str, float]] | None = None,
) -> dict[int, dict[str, float]]:
    normalized: dict[int, dict[str, float]] = {}
    for raw_order_id, part_map in (order_supplements or {}).items():
        try:
            order_id = int(raw_order_id)
        except (TypeError, ValueError):
            continue

        parts: dict[str, float] = {}
        for part, qty in (part_map or {}).items():
            key = normalize_part_key(part)
            try:
                amount = float(qty or 0)
            except (TypeError, ValueError):
                amount = 0.0
            if key and amount > 0:
                parts[key] = amount
        normalized[order_id] = parts
    return normalized


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
    normalized_ids = normalize_order_id_list(order_ids)
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
        schedule_order_qty = coerce_qty(order.get("order_qty"))
        part_totals: dict[str, dict[str, float]] = {}
        for component in components:
            needed_qty = get_component_effective_needed_qty(component, schedule_order_qty=schedule_order_qty)
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


def merge_order_supplement_allocations(
    order_ids: list[int],
    supplements: dict[str, float] | None = None,
    order_supplements: dict[int, dict[str, float]] | None = None,
    *,
    allocator: Callable[[list[int], dict[str, float]], dict[int, dict[str, float]]] | None = None,
) -> dict[int, dict[str, float]]:
    normalized_ids = normalize_order_id_list(order_ids)
    if not normalized_ids:
        return {}

    build_allocations = allocator or build_order_supplement_allocations
    merged = (
        build_allocations(normalized_ids, supplements or {})
        if supplements
        else {order_id: {} for order_id in normalized_ids}
    )

    explicit = _normalize_explicit_order_supplements(order_supplements)
    for order_id in normalized_ids:
        if order_id in explicit:
            # 有訂單別資料時以該列輸入為準；全域補料只留給舊呼叫相容。
            merged[order_id] = dict(explicit.get(order_id) or {})
        else:
            merged.setdefault(order_id, {})

    return {
        order_id: {
            normalize_part_key(part): float(qty or 0)
            for part, qty in (merged.get(order_id) or {}).items()
            if normalize_part_key(part) and float(qty or 0) > 0
        }
        for order_id in normalized_ids
    }
