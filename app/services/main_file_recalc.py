from __future__ import annotations

import re
from typing import Any

_BATCH_CODE_RE = re.compile(r"^\d+-\d+$")
_DEDUCT_HEADER_KEYWORDS = ("扣帳",)
_REVERSE_HEADER_KEYWORDS = ("回復", "恢復")
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
    batch_cols = find_batch_cols(ws)
    return batch_cols[0] if batch_cols else None


def find_batch_cols(ws) -> list[int]:
    """找 row 1 所有 X-Y 批次 code 欄位，不要求批次區彼此連續。"""
    cols: list[int] = []
    for col in range(1, ws.max_column + 1):
        value = str(ws.cell(row=1, column=col).value or "").strip()
        if _BATCH_CODE_RE.match(value):
            cols.append(col)
    return cols


def find_batch_col_for_cell(ws, col: int) -> int | None:
    """回傳 cell 所屬批次起始欄；只接受補料欄或用量欄。"""
    for batch_col in reversed(find_batch_cols(ws)):
        if col in {batch_col, batch_col + 1}:
            return batch_col
        if col > batch_col + 1:
            return None
    return None


def _header_text(ws, col: int) -> str:
    return str(ws.cell(row=1, column=col).value or "").strip()


def _adjustment_kind(header: str) -> str | None:
    if any(keyword in header for keyword in _REVERSE_HEADER_KEYWORDS):
        return "reverse"
    if any(keyword in header for keyword in _DEDUCT_HEADER_KEYWORDS):
        return "deduct"
    return None


def _stock_events(ws) -> list[dict[str, int | str]]:
    """找出會影響庫存 running balance 的欄位事件。"""
    events: list[dict[str, int | str]] = []
    for col in range(1, ws.max_column + 1):
        header = _header_text(ws, col)
        if _BATCH_CODE_RE.match(header):
            events.append({
                "kind": "batch",
                "start_col": col,
                "balance_col": col + 2,
            })
            continue

        adjustment_kind = _adjustment_kind(header)
        if adjustment_kind:
            events.append({
                "kind": adjustment_kind,
                "start_col": col,
                "balance_col": col + 1,
            })

    return sorted(events, key=lambda item: int(item["start_col"]))


def _event_for_cell(events: list[dict[str, int | str]], col: int) -> dict[str, int | str] | None:
    for event in reversed(events):
        kind = str(event["kind"])
        start_col = int(event["start_col"])
        if kind == "batch" and col in {start_col, start_col + 1}:
            return event
        if kind in {"deduct", "reverse"} and col == start_col:
            return event
        if col > int(event["balance_col"]):
            return None
    return None


def _previous_balance(ws, row: int, events: list[dict[str, int | str]], start_col: int) -> float | None:
    for event in reversed([item for item in events if int(item["start_col"]) < start_col]):
        value = _to_number(ws.cell(row=row, column=int(event["balance_col"])).value)
        if value is not None:
            return value
    return None


def _initial_balance(
    ws,
    row: int,
    part_number: str,
    first_event_col: int,
    events: list[dict[str, int | str]],
    snapshot_stock: dict[str, float] | None,
) -> float:
    previous = _previous_balance(ws, row, events, first_event_col)
    if previous is not None:
        return previous

    # 現場主檔常用 H 欄作為盤點/起始庫存；若批次區從 H 之後才開始，優先採用它。
    if _STOCK_FALLBACK_COL < first_event_col:
        fallback = _to_number(ws.cell(row=row, column=_STOCK_FALLBACK_COL).value)
        if fallback is not None:
            return fallback

    snapshot_value = (snapshot_stock or {}).get(part_number)
    if snapshot_value is not None:
        return float(snapshot_value or 0)

    fallback = _to_number(ws.cell(row=row, column=_STOCK_FALLBACK_COL).value)
    return fallback if fallback is not None else 0.0


def _last_balance(ws, row: int, first_event_col: int) -> float | None:
    last: float | None = None
    for event in [item for item in _stock_events(ws) if int(item["start_col"]) >= first_event_col]:
        value = _to_number(ws.cell(row=row, column=int(event["balance_col"])).value)
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
    若編輯的是批次補料/用量，或不良品/多打扣帳數量，重算該列後續結餘。

    回傳:
        affected_cells: [{row, col, value}]
        part_number: 該列料號
        current_stock: 最後一個非空結餘
        recalculated: 是否觸發重算
    """
    if row <= 1:
        return {"affected_cells": [], "part_number": "", "current_stock": None, "recalculated": False}

    events = _stock_events(ws)
    first_event_col = int(events[0]["start_col"]) if events else None
    if first_event_col is None or col < first_event_col:
        return {"affected_cells": [], "part_number": _part_number(ws, row), "current_stock": None, "recalculated": False}

    start_event = _event_for_cell(events, col)
    if start_event is None:
        return {"affected_cells": [], "part_number": _part_number(ws, row), "current_stock": None, "recalculated": False}
    start_col = int(start_event["start_col"])

    part_number = _part_number(ws, row)
    if not part_number:
        return {"affected_cells": [], "part_number": "", "current_stock": None, "recalculated": False}

    previous = _previous_balance(ws, row, events, start_col)
    if previous is None:
        previous = _initial_balance(ws, row, part_number, first_event_col, events, snapshot_stock)

    affected_cells: list[dict[str, int | float]] = []
    for event in [item for item in events if int(item["start_col"]) >= start_col]:
        kind = str(event["kind"])
        event_col = int(event["start_col"])
        balance_col = int(event["balance_col"])
        balance_cell = ws.cell(row=row, column=balance_col)

        if kind == "batch":
            supplement_cell = ws.cell(row=row, column=event_col)
            usage_cell = ws.cell(row=row, column=event_col + 1)
            if _is_blank(supplement_cell.value) and _is_blank(usage_cell.value) and _is_blank(balance_cell.value):
                continue

            supplement = _to_number(supplement_cell.value) or 0.0
            usage = _to_number(usage_cell.value) or 0.0
            previous = previous - usage + supplement
        else:
            qty_cell = ws.cell(row=row, column=event_col)
            if _is_blank(qty_cell.value) and _is_blank(balance_cell.value):
                continue

            qty = _to_number(qty_cell.value) or 0.0
            if kind == "reverse":
                previous = previous + qty
            else:
                previous = previous - qty

        value = _display_number(previous)
        balance_cell.value = value
        affected_cells.append({"row": row, "col": balance_col, "value": value})

    return {
        "affected_cells": affected_cells,
        "part_number": part_number,
        "current_stock": _last_balance(ws, row, first_event_col),
        "recalculated": True,
    }
