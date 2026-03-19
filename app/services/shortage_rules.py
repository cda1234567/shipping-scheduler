from __future__ import annotations

EC_PART_PREFIX = "EC-"
EC_MIN_ENDING_STOCK = 100.0
ORDER_SCOPED_PART_PREFIXES = (
    "IC-STM",
    "IC-M24",
    "IC-XC2C32",
)


def normalize_part_key(value) -> str:
    return str(value or "").strip().upper()


def _coerce_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_ec_part(part_number: str) -> bool:
    return normalize_part_key(part_number).startswith(EC_PART_PREFIX)


def is_order_scoped_shortage_part(part_number: str) -> bool:
    part_key = normalize_part_key(part_number)
    return any(part_key.startswith(prefix) for prefix in ORDER_SCOPED_PART_PREFIXES)


def get_min_ending_stock(part_number: str) -> float:
    part_key = normalize_part_key(part_number)
    if part_key.startswith(EC_PART_PREFIX):
        return EC_MIN_ENDING_STOCK
    return 0.0


def calculate_shortage_amount(part_number: str, ending_stock: float) -> float:
    required_min = get_min_ending_stock(part_number)
    return max(0.0, float(required_min) - float(ending_stock or 0))


def calculate_current_order_shortage_amount(part_number: str, available_before: float, needed_qty: float) -> float:
    if is_order_scoped_shortage_part(part_number):
        return max(0.0, float(needed_qty or 0) - max(0.0, float(available_before or 0)))
    ending_stock = float(available_before or 0) - float(needed_qty or 0)
    return calculate_shortage_amount(part_number, ending_stock)


def _ceil_to_moq(qty: float, moq: float) -> float:
    if moq <= 0 or qty <= 0:
        return qty
    import math
    return math.ceil(qty / moq) * moq


def summarize_st_supply(shortage_amount: float, st_stock_qty: float, moq: float = 0) -> dict[str, float | bool]:
    shortage = max(0.0, float(shortage_amount or 0))
    st_stock = max(0.0, float(st_stock_qty or 0))
    moq_val = max(0.0, float(moq or 0))
    # ST 調撥量也要按 MOQ 向上取整，再跟 ST 庫存取較小值
    shortage_rounded = _ceil_to_moq(shortage, moq_val)
    st_available = min(shortage_rounded, st_stock)
    purchase_needed = max(0.0, shortage - st_available)
    return {
        "st_stock_qty": st_stock,
        "st_available_qty": st_available,
        "purchase_needed_qty": purchase_needed,
        "needs_purchase": purchase_needed > 0,
    }


def summarize_requested_supply(request_qty: float, st_stock_qty: float) -> dict[str, float | bool]:
    requested = max(0.0, float(request_qty or 0))
    st_stock = max(0.0, float(st_stock_qty or 0))
    st_available = min(requested, st_stock)
    purchase_needed = max(0.0, requested - st_available)
    return {
        "requested_qty": requested,
        "st_stock_qty": st_stock,
        "st_available_qty": st_available,
        "purchase_needed_qty": purchase_needed,
        "needs_purchase": purchase_needed > 0,
    }


def get_shortage_resulting_stock(shortage: dict | None) -> float | None:
    if not isinstance(shortage, dict):
        return None

    resulting_stock = _coerce_float(shortage.get("resulting_stock"))
    if resulting_stock is not None:
        return resulting_stock

    ending_stock = _coerce_float(shortage.get("ending_stock"))
    if ending_stock is not None:
        return ending_stock

    return None


def is_main_write_blocking_shortage(shortage: dict | None) -> bool:
    if not isinstance(shortage, dict):
        return False

    shortage_amount = _coerce_float(shortage.get("shortage_amount")) or 0.0
    if shortage_amount <= 0:
        return False

    if str(shortage.get("decision") or "") == "Shortage":
        return True

    if not is_ec_part(str(shortage.get("part_number") or "")):
        return True

    resulting_stock = get_shortage_resulting_stock(shortage)
    return resulting_stock is None or resulting_stock < 0


def filter_main_write_blocking_shortages(shortages: list[dict] | None) -> list[dict]:
    return [dict(item) for item in (shortages or []) if is_main_write_blocking_shortage(item)]
