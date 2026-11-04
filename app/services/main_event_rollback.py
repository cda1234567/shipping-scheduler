"""主檔單一欄組（發料三欄 / 不良品或加工多打扣帳三欄）的精準撤銷。

兩種做法，都只動這一組欄位與受影響料號的後續結餘：
1. 欄組在主檔最右邊（後面沒有任何資料）→ 直接刪掉這幾欄，不留殘欄。
2. 欄組在中間 → 數量歸零、清掉該組結餘、表頭改名標記（讓它不再被當成庫存事件），
   再把後續結餘逐格加回差額（逐事件核對結餘鏈）；遇到盤點調整則調整該次盤點的增減量，
   讓盤點後的結餘維持盤點數（盤點錨定）。

任何前置檢查不過（公式、合併儲存格、後續結餘曾被手改導致鏈不一致…）都 raise
PreciseRollbackUnavailable，由呼叫端退回舊做法；本模組只改記憶體內的 workbook。
"""
from __future__ import annotations

from typing import Any

from openpyxl.comments import Comment

from .main_file_recalc import _stock_events
from .main_reader import main_balance_columns

_TOL = 1e-6
RED_RGB_SUFFIX = "FFC7CE"


class PreciseRollbackUnavailable(Exception):
    """無法安全精準撤銷；reason 給使用者看。"""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def number(value: Any) -> float | None:
    if is_blank(value):
        return 0.0
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def display_number(value: float) -> int | float:
    rounded = round(float(value), 6)
    return int(rounded) if rounded == int(rounded) else rounded


def is_formula(value: Any) -> bool:
    return isinstance(value, str) and value.strip().startswith("=")


def sheet_values(ws) -> list[tuple]:
    return [tuple(row) for row in ws.iter_rows(min_row=1, max_row=ws.max_row,
                                               max_col=ws.max_column, values_only=True)]


def last_used_col(ws) -> int:
    last = 0
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
        for cell in reversed(row):
            if not is_blank(cell.value):
                last = max(last, cell.column)
                break
    return last


def part_rows(ws, part_col: int = 1) -> dict[str, list[int]]:
    rows: dict[str, list[int]] = {}
    for row in range(2, ws.max_row + 1):
        part = str(ws.cell(row, part_col).value or "").strip().upper()
        if part:
            rows.setdefault(part, []).append(row)
    return rows


def ensure_no_formulas(ws, rows: list[int], start_col: int, cols: set[int]) -> None:
    """受影響列在欄組之後若有公式，或任何公式參照這組欄位，就不做精準撤銷。"""
    from openpyxl.utils import get_column_letter
    import re

    letters = {get_column_letter(col) for col in cols}
    pattern = re.compile(r"(?<![A-Z$])\$?(" + "|".join(sorted(letters, key=len, reverse=True)) + r")\$?\d")
    row_set = set(rows)
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            if not is_formula(cell.value):
                continue
            if cell.row in row_set and cell.column >= start_col:
                raise PreciseRollbackUnavailable(f"主檔 {cell.coordinate} 是公式，無法精準撤銷")
            if pattern.search(str(cell.value).upper()):
                raise PreciseRollbackUnavailable(f"主檔 {cell.coordinate} 的公式參照到要撤銷的欄位")


def ensure_no_merged(ws, cols: set[int]) -> None:
    for item in ws.merged_cells.ranges:
        if any(item.min_col <= col <= item.max_col for col in cols):
            raise PreciseRollbackUnavailable("要撤銷的欄位有合併儲存格，無法精準撤銷")


_DRIFT_TOL = 1.0 + 1e-6  # 發料時結餘用原始用量四捨五入，和 前結餘 − 整數用量 可能差 1


def _event_net(ws, row: int, event: dict) -> tuple[bool, float]:
    """和 main_file_recalc 相同的事件語意；回傳 (是否有資料, 淨增減)。"""
    kind = str(event["kind"])
    start = int(event["start_col"])
    balance_col = int(event["balance_col"])
    supply_value = ws.cell(row, start).value
    usage_value = ws.cell(row, start + 1).value
    balance_value = ws.cell(row, balance_col).value
    if kind == "batch" or balance_col == start + 2:
        if is_blank(supply_value) and is_blank(usage_value) and is_blank(balance_value):
            return False, 0.0
        supply, usage = number(supply_value), number(usage_value)
        if supply is None or usage is None:
            raise PreciseRollbackUnavailable(f"主檔 {ws.cell(row, start).coordinate} 附近有無法辨識的數量")
        return True, supply - usage
    if is_blank(supply_value) and is_blank(balance_value):
        return False, 0.0
    qty = number(supply_value)
    if qty is None:
        raise PreciseRollbackUnavailable(f"主檔 {ws.cell(row, start).coordinate} 有無法辨識的數量")
    return True, qty if kind == "reverse" else -qty


def _shift_plan_for_row(ws, row: int, end_col: int, start_balance: float | None, delta: float) -> dict:
    """算出這列在欄組之後每個結餘要加多少（盤點後錨定），不寫入。

    逐事件檢查：結餘 ≈ 前結餘 + 淨增減（容許四捨五入差 1）；差更多表示結餘曾被手動改過，
    這時位移結果無法確定，raise 讓呼叫端改走整檔還原。
    """
    shifts: list[tuple[int, float]] = []
    anchor = None
    previous = start_balance
    for event in _stock_events(ws):
        if int(event["start_col"]) <= end_col:
            continue
        has_values, net = _event_net(ws, row, event)
        if not has_values:
            continue
        balance_col = int(event["balance_col"])
        balance_value = ws.cell(row, balance_col).value
        balance = number(balance_value)
        if is_blank(balance_value) or balance is None:
            raise PreciseRollbackUnavailable(
                f"料號 {ws.cell(row, 1).value} 在 {ws.cell(1, int(event['start_col'])).coordinate} 欄組沒有結餘，無法精準撤銷"
            )
        if str(event["kind"]) == "reconcile":
            anchor = event
            break
        if previous is not None and abs(previous + net - balance) > _DRIFT_TOL:
            raise PreciseRollbackUnavailable(
                f"料號 {ws.cell(row, 1).value} 在 {ws.cell(1, int(event['start_col'])).value or ws.cell(1, int(event['start_col'])).coordinate}"
                " 的結餘和前後數量對不起來（可能曾手動改過結餘），無法精準撤銷"
            )
        shifts.append((balance_col, delta))
        previous = balance
    return {"shifts": shifts, "anchor": anchor}


def _is_red(cell) -> bool:
    try:
        return str(cell.fill.fgColor.rgb or "").upper().endswith(RED_RGB_SUFFIX) and cell.fill.fill_type == "solid"
    except Exception:
        return False


def _apply_sign_fill(ws, row: int, col: int, value: float) -> None:
    from .merge_to_main import RED_FILL, clear_cell_fill

    cell = ws.cell(row, col)
    if value < 0 and not _is_red(cell):
        cell.fill = RED_FILL
    elif value >= 0 and _is_red(cell):
        clear_cell_fill(cell)


def neutralize_or_remove_block(
    ws,
    *,
    start_col: int,
    width: int,
    row_deltas: dict[int, float],
    zero_cols: list[int],
    clear_cols: list[int],
    marked_header: str,
    marker_comment: str,
    allow_remove: bool = True,
) -> dict:
    """撤銷一個欄組。row_deltas：列 → 撤銷後該列庫存的增加量。

    回傳 {"mode": "removed" | "zeroed", "changed": {(r, c)}, "rows": {...}}。
    """
    end_col = start_col + width - 1
    cols = set(range(start_col, end_col + 1))
    rows = sorted(row_deltas)
    ensure_no_merged(ws, cols)
    ensure_no_formulas(ws, rows, start_col, cols)

    before = sheet_values(ws)
    before_max_col = ws.max_column

    def old(r: int, c: int):
        return before[r - 1][c - 1] if r - 1 < len(before) and c - 1 < len(before[r - 1]) else None

    # 其他列在這組欄位不能有資料，否則不是單純這一筆的欄位。
    for r in range(2, ws.max_row + 1):
        if r in row_deltas:
            continue
        for c in cols:
            value = old(r, c)
            if not is_blank(value) and number(value) != 0:
                raise PreciseRollbackUnavailable(
                    f"主檔 {ws.cell(r, c).coordinate} 有不屬於這筆的資料，無法精準撤銷"
                )

    if allow_remove and last_used_col(ws) <= end_col:
        ws.delete_cols(start_col, width)
        from openpyxl.utils import get_column_letter
        for col in range(start_col, before_max_col + 1):
            ws.column_dimensions.pop(get_column_letter(col), None)
        after = sheet_values(ws)
        for r, row_values in enumerate(after, start=1):
            for c, value in enumerate(row_values, start=1):
                if c >= start_col and not is_blank(value):
                    raise PreciseRollbackUnavailable("刪除尾端欄位後仍有殘留資料，已取消")
                if c < start_col and value != old(r, c):
                    raise PreciseRollbackUnavailable("刪除尾端欄位時其他儲存格被改動，已取消")
        return {"mode": "removed", "changed": set(), "rows": {row: {"delta": row_deltas[row]} for row in rows}}

    plans: dict[int, dict] = {}
    for row in rows:
        start_balance = number(old(row, end_col)) if not is_blank(old(row, end_col)) else None
        plans[row] = _shift_plan_for_row(ws, row, end_col, start_balance, float(row_deltas[row]))

    # 歸零、清結餘、改表頭（改名後不再被辨認為庫存事件或結存欄）
    from .merge_to_main import clear_cell_fill
    for row in rows:
        for col in zero_cols:
            ws.cell(row, col).value = 0
        for col in clear_cols:
            ws.cell(row, col).value = None
        for col in cols:
            clear_cell_fill(ws.cell(row, col))
    header = ws.cell(1, start_col)
    original_comment = header.comment.text if header.comment else ""
    header.value = marked_header
    header.comment = Comment(
        marker_comment + (("\n" + original_comment) if original_comment else ""), "shipping-scheduler",
    )
    balance_cols, _ = main_balance_columns([ws.cell(1, col).value for col in range(1, ws.max_column + 1)])
    if any(col - 1 in balance_cols for col in cols) or any(
        int(event["start_col"]) in cols for event in _stock_events(ws)
    ):
        raise PreciseRollbackUnavailable("標記後的欄位仍會被當成庫存欄，已取消")

    changed: set[tuple[int, int]] = {(1, start_col)}
    for row in rows:
        for col in zero_cols + clear_cols:
            changed.add((row, col))

    batch_balance_cols = {int(e["balance_col"]) for e in _stock_events(ws) if str(e["kind"]) == "batch"}
    row_info: dict[int, dict] = {}
    for row in rows:
        delta = float(row_deltas[row])
        plan = plans[row]
        if abs(delta) > _TOL:
            for col, shift in plan["shifts"]:
                new_value = display_number(number(ws.cell(row, col).value) + shift)
                ws.cell(row, col).value = new_value
                changed.add((row, col))
                if col in batch_balance_cols:
                    _apply_sign_fill(ws, row, col, float(new_value))
            anchor = plan["anchor"]
            if anchor is not None:
                supply_col, usage_col = int(anchor["start_col"]), int(anchor["start_col"]) + 1
                net = (number(ws.cell(row, supply_col).value) or 0.0) - (number(ws.cell(row, usage_col).value) or 0.0) - delta
                for col, value in ((supply_col, max(net, 0.0)), (usage_col, max(-net, 0.0))):
                    keep_blank = abs(value) <= _TOL and is_blank(ws.cell(row, col).value)
                    ws.cell(row, col).value = None if keep_blank else display_number(value)
                changed.update({(row, supply_col), (row, usage_col)})
        row_info[row] = {
            "delta": delta,
            "anchored": plan["anchor"] is not None,
            "anchor_col": int(plan["anchor"]["start_col"]) if plan["anchor"] is not None else None,
            "shifted_cells": len(plan["shifts"]),
        }

    after = sheet_values(ws)
    for r in range(1, max(len(after), len(before)) + 1):
        row_after = after[r - 1] if r - 1 < len(after) else ()
        for c in range(1, max(len(row_after), before_max_col) + 1):
            value = row_after[c - 1] if c - 1 < len(row_after) else None
            prev_value = old(r, c)
            if (r, c) in changed:
                continue
            if value != prev_value and not (is_blank(value) and is_blank(prev_value)):
                raise PreciseRollbackUnavailable(f"主檔 {ws.cell(r, c).coordinate} 意外被改動，已取消")
    return {"mode": "zeroed", "changed": changed, "rows": row_info}
