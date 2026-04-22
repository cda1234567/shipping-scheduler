from __future__ import annotations

from .. import database as db


def normalize_part_key(value) -> str:
    return str(value or "").strip().upper()


def build_order_decision_allocations(
    order_ids: list[int],
    decisions: dict[str, str],
    *,
    include_none: bool = False,
) -> dict[int, dict[str, str]]:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return {}

    normalized_decisions: dict[str, tuple[str, str]] = {}
    for part, decision in (decisions or {}).items():
        original_key = str(part or "").strip()
        key = normalize_part_key(original_key)
        value = str(decision or "").strip()
        if not key or not value:
            continue
        if value == "None" and not include_none:
            continue
        normalized_decisions[key] = (original_key, value)
    if not normalized_decisions:
        return {order_id: {} for order_id in normalized_ids}

    bom_map = db.get_all_bom_components_by_model()
    allocations: dict[int, dict[str, str]] = {}
    fallback_order_id = normalized_ids[0]

    for order_id in normalized_ids:
        order = db.get_order(order_id)
        if not order:
            allocations[order_id] = (
                {
                    original_key: value
                    for original_key, value in normalized_decisions.values()
                }
                if order_id == fallback_order_id
                else {}
            )
            continue

        model_key = normalize_part_key(order.get("model"))
        components = bom_map.get(model_key, [])
        if not components:
            allocations[order_id] = {}
            continue

        order_parts = {
            normalize_part_key(component.get("part_number"))
            for component in components
            if normalize_part_key(component.get("part_number"))
        }
        allocations[order_id] = {
            original_key: value
            for part, (original_key, value) in normalized_decisions.items()
            if part in order_parts
        }

    return allocations
