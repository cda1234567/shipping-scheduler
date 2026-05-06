from __future__ import annotations

import re
from typing import Any

_BATCH_CODE_RE = re.compile(r"^\d+-\d+$")
_PART_COL = 1
_STOCK_FALLBACK_COL = 8


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _to_number(value: Any) -> float | None:
    if _is_blank(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _display_number(value: float) -> int | float:
    return int(value) if value == int(value) else value


def _part_number(ws, row: int) -> str:
    return str(ws.cell(row=row, column=_PART_COL).value or "").strip().upper()


def find_first_batch_col(ws) -> int | None:
    """找 row 1 第一個 X-Y 批次 code 欄位。"""
    for col in range(1, ws.max_column + 1):
        value = str(ws.cell(row=1, column=col).value or "").strip()
        if _BATCH_CODE_RE.match(value):
            return col
    return None


def _previous_balance(ws, row: int, first_batch_col: int, start_batch_col: int) -> float | None:
    for batch_col in range(start_batch_col - 3, first_batch_col - 1, -3):
        value = _to_number(ws.cell(row=row, column=batch_col + 2).value)
        if value is not None:
            return value
    return None


def _initial_balance(
    ws,
    row: int,
    part_number: str,
    first_batch_col: int,
    snapshot_stock: dict[str, float] | None,
) -> float:
    previous = _previous_balance(ws, row, first_batch_col, first_batch_col)
    if previous is not None:
        return previous

    # 現場主檔常用 H 欄作為盤點/起始庫存；若批次區從 H 之後才開始，優先採用它。
    if _STOCK_FALLBACK_COL < first_batch_col:
        fallback = _to_number(ws.cell(row=row, column=_STOCK_FALLBACK_COL).value)
        if fallback is not None:
            return fallback

    snapshot_value = (snapshot_stock or {}).get(part_number)
    if snapshot_value is not None:
        return float(snapshot_value or 0)

    fallback = _to_number(ws.cell(row=row, column=_STOCK_FALLBACK_COL).value)
    return fallback if fallback is not None else 0.0


def _last_balance(ws, row: int, first_batch_col: int) -> float | None:
    last: float | None = None
    for batch_col in range(first_batch_col, ws.max_column + 1, 3):
        value = _to_number(ws.cell(row=row, column=batch_col + 2).value)
        if value is not None:
            last = value
    return last


def recalc_batch_balances_for_cell(
    ws,
    *,
    row: int,
    col: int,
    snapshot_stock: dict[str, float] | None = None,
) -> dict:
    """
    若編輯的是批次區補料/用量 cell，重算該列後續批次結餘。

    回傳:
        affected_cells: [{row, col, value}]
        part_number: 該列料號
        current_stock: 最後一個非空結餘
        recalculated: 是否觸發重算
    """
    if row <= 1:
        return {"affected_cells": [], "part_number": "", "current_stock": None, "recalculated": False}

    first_batch_col = find_first_batch_col(ws)
    if first_batch_col is None or col < first_batch_col:
        return {"affected_cells": [], "part_number": _part_number(ws, row), "current_stock": None, "recalculated": False}

    offset = (col - first_batch_col) % 3
    if offset not in {0, 1}:
        return {"affected_cells": [], "part_number": _part_number(ws, row), "current_stock": None, "recalculated": False}

    part_number = _part_number(ws, row)
    if not part_number:
        return {"affected_cells": [], "part_number": "", "current_stock": None, "recalculated": False}

    start_batch_col = col - offset
    previous = _previous_balance(ws, row, first_batch_col, start_batch_col)
    if previous is None:
        previous = _initial_balance(ws, row, part_number, first_batch_col, snapshot_stock)

    affected_cells: list[dict[str, int | float]] = []
    for batch_col in range(start_batch_col, ws.max_column + 1, 3):
        supplement_cell = ws.cell(row=row, column=batch_col)
        usage_cell = ws.cell(row=row, column=batch_col + 1)
        balance_cell = ws.cell(row=row, column=batch_col + 2)

        if _is_blank(supplement_cell.value) and _is_blank(usage_cell.value) and _is_blank(balance_cell.value):
            continue

        supplement = _to_number(supplement_cell.value) or 0.0
        usage = _to_number(usage_cell.value) or 0.0
        previous = previous - usage + supplement
        value = _display_number(previous)
        balance_cell.value = value
        affected_cells.append({"row": row, "col": batch_col + 2, "value": value})

    return {
        "affected_cells": affected_cells,
        "part_number": part_number,
        "current_stock": _last_balance(ws, row, first_batch_col),
        "recalculated": True,
    }
