from __future__ import annotations

import re


def coerce_qty(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def coerce_scrap_factor(value) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace("％", "%").replace(",", "")
    if not text:
        return 0.0
    percent_value = "%" in text
    if text.startswith("="):
        formula_body = text[1:].strip()
        if re.fullmatch(r"[+-]?\d+(?:\.\d+)?\s*%?", formula_body):
            text = formula_body
            percent_value = percent_value or text.endswith("%")
        else:
            return 0.0
    elif re.fullmatch(r"\$?[A-Za-z]{1,3}\$?\d+", text):
        return 0.0
    try:
        if text.endswith("%"):
            amount = float(text[:-1].strip() or 0) / 100
        else:
            amount = float(value)
    except (TypeError, ValueError):
        match = re.search(r"([+-]?\d+(?:\.\d+)?)\s*%", text) if percent_value else None
        if not match:
            match = re.search(r"([+-]?\d+(?:\.\d+)?)\s*%?", text)
        if not match:
            return 0.0
        try:
            amount = float(match.group(1))
        except (TypeError, ValueError):
            return 0.0
        if percent_value or match.group(0).strip().endswith("%"):
            amount /= 100
    if amount < 0:
        return 0.0
    if amount > 1:
        return amount / 100
    return amount


def resolve_effective_order_qty(schedule_order_qty, bom_order_qty=0.0) -> float:
    schedule_qty = coerce_qty(schedule_order_qty)
    if schedule_qty > 0:
        return schedule_qty
    return coerce_qty(bom_order_qty)


def calculate_effective_needed_qty(
    *,
    needed_qty,
    qty_per_board=0.0,
    scrap_factor=0.0,
    schedule_order_qty=0.0,
    bom_order_qty=0.0,
) -> float:
    # 需求量永遠由 qty_per_board × 排程數量 × (1 + 拋料率) 即時算，
    # 不再讀 BOM F 欄的 needed_qty（容易因上傳時公式/值不對而錯），
    # 只有在這筆 BOM 沒有 qty_per_board 時才退回原始 F 欄值。
    original_needed_qty = coerce_qty(needed_qty)
    schedule_qty = coerce_qty(schedule_order_qty)
    if schedule_qty <= 0:
        return original_needed_qty

    per_board_qty = coerce_qty(qty_per_board)
    if per_board_qty > 0:
        return per_board_qty * schedule_qty * (1 + coerce_scrap_factor(scrap_factor))

    return original_needed_qty


def get_component_effective_needed_qty(component: dict, schedule_order_qty=0.0, bom_order_qty=0.0) -> float:
    source_bom_order_qty = component.get("bom_order_qty", bom_order_qty)
    return calculate_effective_needed_qty(
        needed_qty=component.get("needed_qty"),
        qty_per_board=component.get("qty_per_board"),
        scrap_factor=component.get("scrap_factor"),
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
