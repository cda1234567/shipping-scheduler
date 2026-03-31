from __future__ import annotations


def coerce_qty(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def resolve_effective_order_qty(schedule_order_qty, bom_order_qty=0.0) -> float:
    schedule_qty = coerce_qty(schedule_order_qty)
    if schedule_qty > 0:
        return schedule_qty
    return coerce_qty(bom_order_qty)


def calculate_effective_needed_qty(
    *,
    needed_qty,
    qty_per_board=0.0,
    schedule_order_qty=0.0,
    bom_order_qty=0.0,
) -> float:
    original_needed_qty = coerce_qty(needed_qty)
    schedule_qty = coerce_qty(schedule_order_qty)
    if schedule_qty <= 0:
        return original_needed_qty

    per_board_qty = coerce_qty(qty_per_board)
    if per_board_qty > 0:
        return per_board_qty * schedule_qty

    original_order_qty = coerce_qty(bom_order_qty)
    if original_order_qty > 0 and original_needed_qty > 0:
        return original_needed_qty * schedule_qty / original_order_qty

    return original_needed_qty


def get_component_effective_needed_qty(component: dict, schedule_order_qty=0.0, bom_order_qty=0.0) -> float:
    source_bom_order_qty = component.get("bom_order_qty", bom_order_qty)
    return calculate_effective_needed_qty(
        needed_qty=component.get("needed_qty"),
        qty_per_board=component.get("qty_per_board"),
        schedule_order_qty=schedule_order_qty,
        bom_order_qty=source_bom_order_qty,
    )


def build_effective_component(component: dict, schedule_order_qty=0.0, bom_order_qty=0.0) -> dict:
    effective = dict(component)
    effective_bom_order_qty = coerce_qty(component.get("bom_order_qty", bom_order_qty))
    effective["bom_order_qty"] = effective_bom_order_qty
    effective["effective_order_qty"] = resolve_effective_order_qty(schedule_order_qty, effective_bom_order_qty)
    effective["needed_qty"] = get_component_effective_needed_qty(
        component,
        schedule_order_qty=schedule_order_qty,
        bom_order_qty=effective_bom_order_qty,
    )
    return effective


def build_effective_components(
    components: list[dict],
    schedule_order_qty=0.0,
    bom_order_qty=0.0,
) -> list[dict]:
    return [
        build_effective_component(
            component,
            schedule_order_qty=schedule_order_qty,
            bom_order_qty=bom_order_qty,
        )
        for component in (components or [])
    ]


def format_excel_qty(value):
    number = coerce_qty(value)
    return int(number) if float(number).is_integer() else number
