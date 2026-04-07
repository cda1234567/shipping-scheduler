from __future__ import annotations

from math import isclose

from .. import database as db
from .local_time import local_now

_TOLERANCE = 1e-6


def normalize_part_number(value: str) -> str:
    return str(value or "").strip().upper()


def _format_number(value: float) -> str:
    rounded = round(float(value or 0), 6)
    if isclose(rounded, round(rounded), abs_tol=_TOLERANCE):
        return str(int(round(rounded)))
    return f"{rounded:.6f}".rstrip("0").rstrip(".")


def parse_package_text(package_text: str | None) -> list[float]:
    raw = str(package_text or "").replace("，", ",").strip()
    if not raw:
        return []

    values: list[float] = []
    for token in raw.split(","):
        item = str(token or "").strip()
        if not item:
            continue
        try:
            amount = float(item)
        except (TypeError, ValueError) as error:
            raise ValueError(f"包裝數量格式不正確：{item}") from error
        if amount <= 0:
            raise ValueError(f"包裝數量需大於 0：{item}")
        values.append(round(amount, 6))
    return values


def serialize_package_values(values: list[float]) -> str:
    return ",".join(_format_number(value) for value in values if float(value or 0) > _TOLERANCE)


def summarize_package_text(package_text: str | None, stock_qty: float) -> dict:
    values = parse_package_text(package_text)
    package_sum = round(sum(values), 6)
    target_qty = float(stock_qty or 0)
    diff_qty = round(package_sum - target_qty, 6)
    return {
        "package_values": values,
        "package_sum": package_sum,
        "diff_qty": diff_qty,
        "matches_stock": isclose(package_sum, target_qty, abs_tol=_TOLERANCE),
    }


def deduct_package_values(values: list[float], used_qty: float) -> list[float]:
    remaining = round(float(used_qty or 0), 6)
    current = [round(float(value or 0), 6) for value in values if float(value or 0) > _TOLERANCE]
    if remaining <= _TOLERANCE or not current:
        return current

    for index, value in enumerate(current):
        if isclose(value, remaining, abs_tol=_TOLERANCE):
            return current[:index] + current[index + 1 :]

    index = 0
    while remaining > _TOLERANCE and index < len(current):
        value = current[index]
        if value <= remaining + _TOLERANCE:
            remaining = round(max(0.0, remaining - value), 6)
            current.pop(index)
            continue
        current[index] = round(value - remaining, 6)
        remaining = 0.0
        break

    return [value for value in current if value > _TOLERANCE]


def deduct_package_text(package_text: str | None, used_qty: float) -> str:
    values = parse_package_text(package_text)
    next_values = deduct_package_values(values, used_qty)
    return serialize_package_values(next_values)


def build_missing_moq_package_rows() -> list[dict]:
    snapshot = db.get_snapshot()
    st_snapshot = db.get_st_inventory_snapshot()
    saved = db.get_st_package_breakdowns()

    rows: list[dict] = []
    for part_number in sorted(snapshot):
        snapshot_row = snapshot.get(part_number) or {}
        moq = float(snapshot_row.get("moq") or 0)
        if moq > 0:
            continue

        package_state = saved.get(part_number) or {}
        package_text = str(package_state.get("package_text") or "")
        st_row = st_snapshot.get(part_number) or {}
        stock_qty = float(st_row.get("stock_qty") or 0)
        summary = summarize_package_text(package_text, stock_qty)
        rows.append({
            "part_number": part_number,
            "description": str(snapshot_row.get("description") or st_row.get("description") or ""),
            "stock_qty": stock_qty,
            "package_text": package_text,
            "package_values": summary["package_values"],
            "package_sum": summary["package_sum"],
            "diff_qty": summary["diff_qty"],
            "matches_stock": summary["matches_stock"],
            "updated_at": str(package_state.get("updated_at") or ""),
        })
    return rows


def get_missing_moq_package_row(part_number: str) -> dict | None:
    part = normalize_part_number(part_number)
    if not part:
        return None
    for row in build_missing_moq_package_rows():
        if normalize_part_number(row.get("part_number")) == part:
            return row
    return None


def save_missing_moq_package_text(part_number: str, package_text: str | None) -> dict:
    part = normalize_part_number(part_number)
    if not part:
        raise ValueError("料號不可空白")
    if get_missing_moq_package_row(part) is None:
        raise ValueError(f"{part} 目前不是無 MOQ 管理料")
    normalized_text = serialize_package_values(parse_package_text(package_text))
    db.save_st_package_breakdown(part, normalized_text, local_now().isoformat(timespec="seconds"))
    row = get_missing_moq_package_row(part)
    if row is None:
        raise ValueError(f"{part} 儲存後無法重新讀取")
    return row


def build_usage_by_part(allocations: dict[int, dict[str, float]] | None = None) -> dict[str, float]:
    usage: dict[str, float] = {}
    for supplements in (allocations or {}).values():
        for part_number, qty in (supplements or {}).items():
            part = normalize_part_number(part_number)
            amount = round(float(qty or 0), 6)
            if not part or amount <= _TOLERANCE:
                continue
            usage[part] = round(usage.get(part, 0.0) + amount, 6)
    return usage


def consume_st_package_breakdowns(allocations: dict[int, dict[str, float]] | None = None) -> dict:
    usage_by_part = build_usage_by_part(allocations)
    if not usage_by_part:
        return {"usage_by_part": {}, "stock_updates": {}, "package_updates": {}}

    st_stock = db.get_st_inventory_stock()
    package_rows = db.get_st_package_breakdowns(list(usage_by_part))
    stock_updates: dict[str, float] = {}
    package_updates: dict[str, str] = {}
    updated_at = local_now().isoformat(timespec="seconds")

    for part_number, used_qty in usage_by_part.items():
        stock_updates[part_number] = round(float(st_stock.get(part_number) or 0) - used_qty, 6)
        existing = package_rows.get(part_number) or {}
        package_text = str(existing.get("package_text") or "")
        if not package_text.strip():
            continue
        next_package_text = deduct_package_text(package_text, used_qty)
        if next_package_text == package_text:
            continue
        db.save_st_package_breakdown(part_number, next_package_text, updated_at)
        package_updates[part_number] = next_package_text

    if stock_updates:
        db.update_st_inventory_stock(stock_updates)

    return {
        "usage_by_part": usage_by_part,
        "stock_updates": stock_updates,
        "package_updates": package_updates,
    }
