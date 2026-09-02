from __future__ import annotations

import re


def normalize_part(value) -> str:
    return str(value or "").strip().upper()


def normalize_model(value) -> str:
    return str(value or "").strip().upper()


def _code_key(value: str) -> tuple:
    text = str(value or "").strip().upper()
    if not text:
        return ()
    return tuple(
        (0, int(token)) if token.isdigit() else (1, token)
        for token in re.findall(r"\d+|[^\d]+", text)
    )


def rule_applies(rule: dict, *, model: str, batch_code: str = "") -> bool:
    if str(rule.get("status") or "active") != "active":
        return False
    rule_model = normalize_model(rule.get("model"))
    if rule_model not in {"*", normalize_model(model)}:
        return False
    start_code = str(rule.get("effective_from_code") or "").strip()
    if not start_code:
        return True
    current_code = str(batch_code or "").strip()
    return bool(current_code) and _code_key(current_code) >= _code_key(start_code)


def find_rule(
    rules: list[dict],
    *,
    model: str,
    part_number: str,
    batch_code: str = "",
) -> dict | None:
    part = normalize_part(part_number)
    model_key = normalize_model(model)
    candidates = sorted(
        (rules or []),
        key=lambda rule: 0 if normalize_model(rule.get("model")) == model_key else 1,
    )
    for rule in candidates:
        if normalize_part(rule.get("old_part_number")) != part:
            continue
        if rule_applies(rule, model=model, batch_code=batch_code):
            return dict(rule)
    return None


def allocate_substitution(
    component: dict,
    rule: dict | None,
    *,
    old_available: float,
    new_available: float,
    manual_allocation: dict | None = None,
) -> list[dict]:
    """把一筆舊料需求拆成舊料與新料；需求只計算一次。"""
    demand = max(0.0, float(component.get("needed_qty") or 0))
    if not rule or demand <= 0:
        return [dict(component)]

    ratio = float(rule.get("new_per_old_ratio") or 1)
    if ratio <= 0:
        ratio = 1.0
    manual = manual_allocation or {}
    if manual:
        old_qty = max(0.0, min(demand, float(manual.get("old_qty") or 0)))
        new_qty = max(0.0, float(manual.get("new_qty") or 0))
    elif str(rule.get("strategy") or "old_first") == "new_first":
        new_base_capacity = max(0.0, float(new_available or 0)) / ratio
        new_base_qty = min(demand, new_base_capacity)
        new_qty = new_base_qty * ratio
        old_qty = demand - new_base_qty
    else:
        old_qty = min(demand, max(0.0, float(old_available or 0)))
        new_qty = (demand - old_qty) * ratio

    old_part = normalize_part(rule.get("old_part_number"))
    new_part = normalize_part(rule.get("new_part_number"))
    base_meta = {
        "substitution_rule_id": int(rule.get("id") or 0),
        "substitution_old_part": old_part,
        "substitution_new_part": new_part,
        "substitution_ratio": ratio,
        "substitution_total_demand": demand,
        "substitution_manual": bool(manual),
    }
    result: list[dict] = []
    if old_qty > 0:
        old_component = dict(component)
        old_component.update(base_meta)
        old_component["part_number"] = old_part
        old_component["needed_qty"] = old_qty
        old_component["qty_per_board"] = 0
        old_component["scrap_factor"] = 0
        result.append(old_component)
    if new_qty > 0:
        new_component = dict(component)
        new_component.update(base_meta)
        new_component["part_number"] = new_part
        new_component["needed_qty"] = new_qty
        new_component["qty_per_board"] = 0
        new_component["scrap_factor"] = 0
        new_component["prev_qty_cs"] = 0
        description = str(component.get("description") or "").strip()
        new_component["description"] = f"{description}（替代 {old_part}）" if description else f"替代 {old_part}"
        result.append(new_component)
    return result


def validate_manual_allocation(demand: float, ratio: float, old_qty: float, new_qty: float, tolerance: float = 1e-6) -> None:
    normalized_ratio = float(ratio or 1)
    covered = float(old_qty or 0) + float(new_qty or 0) / normalized_ratio
    if abs(covered - float(demand or 0)) > tolerance:
        raise ValueError(
            f"分配量不符：需求 {float(demand or 0):g}，舊料 + 新料換算後為 {covered:g}"
        )
