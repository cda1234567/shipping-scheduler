"""分析頁：近期常用但主檔與 ST 可用庫存皆歸零的料件。"""
from __future__ import annotations

from .bom_quantity import coerce_qty, get_component_effective_needed_qty

_ZERO_EPSILON = 1e-9


def _part_key(value: object) -> str:
    return str(value or "").strip().upper()


def _stock_qty(source: dict[str, object], part_number: str) -> float:
    raw = source.get(part_number, 0)
    if isinstance(raw, dict):
        raw = raw.get("stock_qty", 0)
    return coerce_qty(raw)


def _description(source: dict[str, object], part_number: str) -> str:
    raw = source.get(part_number, {})
    if not isinstance(raw, dict):
        return ""
    return str(raw.get("description") or "").strip()


def _build_active_usage(
    orders: list[dict],
    bom_map: dict[str, list[dict]],
) -> tuple[dict[str, list[dict]], dict[str, str], int]:
    usage: dict[str, list[dict]] = {}
    descriptions: dict[str, str] = {}
    missing_bom_orders = 0

    for order in orders:
        model = str(order.get("model") or "").strip()
        components = bom_map.get(model.upper())
        if components is None:
            missing_bom_orders += 1
            continue

        order_qty = coerce_qty(order.get("order_qty"))
        order_usage: dict[str, float] = {}
        for component in components:
            if bool(component.get("is_dash")):
                continue
            part = _part_key(component.get("part_number"))
            if not part:
                continue
            needed_qty = get_component_effective_needed_qty(component, order_qty)
            if needed_qty <= 0:
                continue
            order_usage[part] = order_usage.get(part, 0.0) + needed_qty
            if part not in descriptions and component.get("description"):
                descriptions[part] = str(component.get("description") or "").strip()

        for part, used_qty in order_usage.items():
            usage.setdefault(part, []).append({
                "order_id": order.get("id"),
                "code": str(order.get("code") or "").strip(),
                "po_number": str(order.get("po_number") or "").strip(),
                "model": model,
                "ship_date": str(order.get("ship_date") or order.get("delivery_date") or "").strip(),
                "used_qty": used_qty,
            })

    return usage, descriptions, missing_bom_orders


def build_frequent_zero_stock_analysis(
    *,
    history_usage: list[dict],
    active_orders: list[dict],
    bom_map: dict[str, list[dict]],
    main_snapshot: dict[str, object],
    st_snapshot: dict[str, object],
    vendors: dict[str, str] | None = None,
    history_months: int = 6,
    min_order_count: int = 3,
    hidden_prefixes: tuple[str, ...] = ("EC-1", "EC-2"),
) -> dict:
    """建立常用零庫存清單；常用以近期已發料的不同訂單數判定。"""
    normalized_months = max(1, min(int(history_months or 6), 24))
    normalized_min_orders = max(1, min(int(min_order_count or 3), 100))
    normalized_main = {_part_key(part): value for part, value in (main_snapshot or {}).items() if _part_key(part)}
    normalized_st = {_part_key(part): value for part, value in (st_snapshot or {}).items() if _part_key(part)}
    normalized_vendors = {
        _part_key(part): str(vendor or "").strip()
        for part, vendor in (vendors or {}).items()
        if _part_key(part)
    }
    active_usage, bom_descriptions, missing_bom_orders = _build_active_usage(active_orders, bom_map)

    common_part_count = 0
    items: list[dict] = []
    for history in history_usage or []:
        part = _part_key(history.get("part_number"))
        if not part or part.startswith(hidden_prefixes):
            continue

        order_count = int(history.get("order_count") or 0)
        if order_count < normalized_min_orders:
            continue
        common_part_count += 1

        main_stock_qty = _stock_qty(normalized_main, part)
        st_stock_qty = _stock_qty(normalized_st, part)
        total_available_qty = max(0.0, main_stock_qty) + max(0.0, st_stock_qty)
        if total_available_qty > _ZERO_EPSILON:
            continue

        used_by = active_usage.get(part, [])
        active_demand_qty = sum(coerce_qty(row.get("used_qty")) for row in used_by)
        description = (
            _description(normalized_st, part)
            or _description(normalized_main, part)
            or bom_descriptions.get(part, "")
        )
        items.append({
            "part_number": part,
            "description": description,
            "vendor": normalized_vendors.get(part, "") or "未分類廠商",
            "history_order_count": order_count,
            "history_total_qty": coerce_qty(history.get("total_qty")),
            "last_used_at": str(history.get("last_used_at") or ""),
            "main_stock_qty": main_stock_qty,
            "st_stock_qty": st_stock_qty,
            "total_available_qty": total_available_qty,
            "active_order_count": len(used_by),
            "active_demand_qty": active_demand_qty,
            "active_demand": bool(used_by),
            "used_by": used_by,
            "priority": "urgent" if used_by else "watch",
        })

    items.sort(key=lambda item: str(item["last_used_at"]), reverse=True)
    items.sort(key=lambda item: (
        0 if item["active_demand"] else 1,
        -int(item["active_order_count"]),
        -int(item["history_order_count"]),
        -float(item["history_total_qty"]),
        str(item["part_number"]),
    ))

    urgent_count = sum(1 for item in items if item["active_demand"])
    return {
        "items": items,
        "summary": {
            "common_part_count": common_part_count,
            "zero_stock_count": len(items),
            "urgent_count": urgent_count,
            "watch_count": len(items) - urgent_count,
            "active_order_count": len(active_orders),
            "missing_bom_orders": missing_bom_orders,
        },
        "parameters": {
            "history_months": normalized_months,
            "min_order_count": normalized_min_orders,
        },
    }
