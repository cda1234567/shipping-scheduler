from __future__ import annotations

EC_PART_PREFIX = "EC-"
EC_MIN_ENDING_STOCK = 100.0


def normalize_part_key(value) -> str:
    return str(value or "").strip().upper()


def _coerce_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_ec_part(part_number: str) -> bool:
    return normalize_part_key(part_number).startswith(EC_PART_PREFIX)


def get_min_ending_stock(part_number: str) -> float:
    part_key = normalize_part_key(part_number)
    if part_key.startswith(EC_PART_PREFIX):
        return EC_MIN_ENDING_STOCK
    return 0.0


def calculate_shortage_amount(part_number: str, ending_stock: float) -> float:
    required_min = get_min_ending_stock(part_number)
    return max(0.0, float(required_min) - float(ending_stock or 0))


def summarize_st_supply(shortage_amount: float, st_stock_qty: float) -> dict[str, float | bool]:
    shortage = max(0.0, float(shortage_amount or 0))
    st_stock = max(0.0, float(st_stock_qty or 0))
    st_available = min(shortage, st_stock)
    purchase_needed = max(0.0, shortage - st_available)
    return {
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
