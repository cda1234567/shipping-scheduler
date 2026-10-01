from __future__ import annotations

from pathlib import Path
import math
import re
from typing import Any, Callable

import openpyxl
from openpyxl.utils.cell import coordinate_to_tuple, get_column_letter

from ..config import cfg
from .main_file_lock import serialized_main_file_write
from .bom_parser import _evaluate_numeric_formula
from .xls_reader import open_workbook_any

_PART_COL = None
_VENDOR_COL = None
_MOQ_COL = None
_STOCK_SEARCH_START_COL = None


def _part_col():
    global _PART_COL
    if _PART_COL is None:
        _PART_COL = cfg("excel.main_part_col", 0)
    return _PART_COL


def _vendor_col():
    global _VENDOR_COL
    if _VENDOR_COL is None:
        _VENDOR_COL = cfg("excel.main_vendor_col", 1)
    return _VENDOR_COL


def _moq_col():
    global _MOQ_COL
    if _MOQ_COL is None:
        _MOQ_COL = cfg("excel.main_moq_col", 2)
    return _MOQ_COL


def _stock_search_start_col():
    global _STOCK_SEARCH_START_COL
    if _STOCK_SEARCH_START_COL is None:
        # 庫存只能從 MOQ 右側開始找，避免把 C 欄 MOQ 誤讀成目前庫存。
        _STOCK_SEARCH_START_COL = _moq_col() + 1
    return _STOCK_SEARCH_START_COL


def _try_float(v) -> float | None:
    if v is None:
        return None
    try:
        number = float(v.strip().replace(",", "") if isinstance(v, str) else v)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def find_current_stock_cell_from_row_values(row_vals) -> float | None:
    """找出 MOQ 右側最後一個可用庫存值；若沒有任何數值則回傳 None。"""
    start_col = _stock_search_start_col()
    if not row_vals or len(row_vals) <= start_col:
        return None

    for value in reversed(row_vals[start_col:]):
        stock = _try_float(value)
        if stock is not None:
            return stock
    return None


def find_current_stock_from_row_values(row_vals) -> float:
    """找出 MOQ 右側最後一個可用庫存值；若沒有任何數值則視為 0。"""
    stock = find_current_stock_cell_from_row_values(row_vals)
    return stock if stock is not None else 0.0


def read_stock(path: str) -> dict[str, float]:
    return {part: cell["stock_qty"] for part, cell in read_stock_cells(path).items()}


def create_main_cell_resolver(
    read_formula: Callable[[int, int], Any],
) -> Callable[[int, int], float | None]:
    """共用主檔算術公式讀取器；無法現算就回傳 None，不使用可能過期的 Excel 快取。"""
    resolved = {}
    resolving = set()

    def resolve_cell(row: int, col: int) -> float | None:
        key = (row, col)
        if key in resolved:
            return resolved[key]
        if key in resolving:
            return None
        raw = read_formula(row, col)
        if isinstance(raw, str) and raw.strip().startswith("="):
            resolving.add(key)

            def reference_value(cell_ref: str) -> float | None:
                ref_row, ref_col = coordinate_to_tuple(cell_ref.replace("$", ""))
                return resolve_cell(ref_row - 1, ref_col - 1)

            try:
                number = _evaluate_numeric_formula(raw, (), (), cell_value_resolver=reference_value)
            finally:
                resolving.remove(key)
        else:
            number = 0.0 if raw is None or str(raw).strip() == "" else _try_float(raw)
        resolved[key] = number
        return number

    return resolve_cell


def main_balance_columns(headers) -> tuple[set[int], dict[str, list[int]]]:
    """辨認結存欄（0-based），包含空白批次碼的歷史三欄組。"""
    balances: set[int] = set()
    batches: dict[str, list[int]] = {}
    occupied: set[int] = set()
    idx = _stock_search_start_col()
    while idx < len(headers):
        header = str(headers[idx] or "").strip()
        next_header = str(headers[idx + 1] or "").strip() if idx + 1 < len(headers) else ""
        if re.fullmatch(r"\d+-\d+", header):
            end = idx + 2
            batches.setdefault(header, []).append(end)
        elif any(word in header for word in ("扣帳", "回復", "恢復", "盤點調整")):
            end = idx + (2 if "盤點調整" in header or next_header in {
                "使用數量", "扣帳數量", "扣除數量", "用量",
            } else 1)
        elif any(word in header for word in ("結存", "結餘", "盤點", "庫存", "期初", "不良")) or header.lower() in {"stock", "balance"} or idx == 7:
            end = idx
        else:
            idx += 1
            continue
        if end < len(headers):
            balances.add(end)
        occupied.update(range(idx, end + 1))
        idx = end + 1
    # 先辨認有名稱的事件，避免歷史欄群跨過下一個批次，把 PO/用量誤當結存。
    for idx in range(8, len(headers) - 2):
        if any(col in occupied for col in (idx, idx + 1, idx + 2)):
            continue
        if str(headers[idx] or "").strip():
            continue
        po = str(headers[idx + 1] or "").strip()
        model = str(headers[idx + 2] or "").strip()
        if po and model and (idx - 1 in balances or _try_float(po) is not None or po.upper().startswith("PO")):
            balances.add(idx + 2)
            occupied.update((idx, idx + 1, idx + 2))
    return balances, batches


def _latest_balance(row: int, columns, read_raw, resolver, part: str) -> tuple[int | None, float | None]:
    for col in sorted(columns, reverse=True):
        raw = read_raw(row, col)
        if raw is None or str(raw).strip() == "":
            continue
        number = resolver(row, col)
        if number is None:
            raise ValueError(f"主檔料號 {part} 的 {get_column_letter(col + 1)}{row + 1} 結存無法讀取，請確認公式或數值")
        return col, number
    return None, None


def read_latest_stock_from_sheet(ws, row: int, max_col: int) -> float:
    """供發料讀取記憶體內主檔，與盤點/庫存畫面使用相同結存與公式規則。"""
    balances, _ = main_balance_columns([ws.cell(1, col).value for col in range(1, max_col + 1)])
    read_raw = lambda r, c: ws.cell(r + 1, c + 1).value
    resolver = create_main_cell_resolver(read_raw)
    part = str(ws.cell(row, _part_col() + 1).value or "").strip().upper()
    _, number = _latest_balance(row - 1, balances, read_raw, resolver, part)
    return number if number is not None else 0.0


def read_stock_cells(path: str) -> dict[str, dict]:
    """唯讀最右結存；無快取的算術公式直接計算，不把用量誤當庫存。"""
    wb = open_workbook_any(path, read_only=True, data_only=False)
    try:
        formula_rows = list(wb.worksheets[0].iter_rows(values_only=True))
        read_raw = lambda row, col: formula_rows[row][col] if 0 <= row < len(formula_rows) and 0 <= col < len(formula_rows[row]) else None
        resolve_cell = create_main_cell_resolver(read_raw)
        columns, _ = main_balance_columns(formula_rows[0] if formula_rows else ())

        result: dict[str, dict] = {}
        pc = _part_col()
        for row_idx, row in enumerate(formula_rows[1:], start=1):
            part = str(row[pc] or "").strip().upper() if len(row) > pc else ""
            if not part:
                continue
            col, number = _latest_balance(row_idx, columns, read_raw, resolve_cell, part)
            result[part] = {"row": row_idx + 1, "col": col + 1 if col is not None else None,
                            "stock_qty": number if number is not None else 0.0}
        return result
    finally:
        wb.close()


def read_batch_stock(path: str, batch_code: str) -> dict[str, dict[str, float]]:
    """唯讀取得各料指定批次與現在結存，不把補料、用量或 MOQ 當庫存。"""
    wb = open_workbook_any(path, read_only=True, data_only=False)
    try:
        ws = wb.worksheets[0]
        rows = list(ws.iter_rows(values_only=True))
        balance_cols, batches = main_balance_columns(rows[0] if rows else ())
        batch_ends = batches.get(batch_code, [])
        if not batch_ends:
            raise ValueError(f'主檔找不到截止批次 {batch_code}')
        # 空白批次碼的歷史三欄組仍有 row 1 結存表頭；不猜測任意數值欄。
        cutoff_boundary = max(batch_ends)
        read_raw = lambda row, col: rows[row][col] if 0 <= row < len(rows) and 0 <= col < len(rows[row]) else None
        resolver = create_main_cell_resolver(read_raw)
        result = {}
        for row_idx, row in enumerate(rows[1:], start=1):
            part = str(row[_part_col()] or '').strip().upper()
            if not part:
                continue
            def latest(columns):
                return _latest_balance(row_idx, columns, read_raw, resolver, part)[1]
            cutoff = latest(batch_ends)
            if cutoff is None:
                cutoff = latest(col for col in balance_cols if col <= cutoff_boundary)
            current = latest(balance_cols)
            if cutoff is not None and current is not None:
                result[part] = {'cutoff_main': cutoff, 'current_main': current}
        return result
    finally:
        wb.close()


def read_vendors(path: str) -> dict[str, str]:
    """讀取主檔 B 欄廠商，以料號為 key。"""
    wb = open_workbook_any(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]
    result: dict[str, str] = {}
    pc = _part_col()
    vc = _vendor_col()

    for row_vals in ws.iter_rows(min_row=2, values_only=True):
        if not row_vals or len(row_vals) <= pc:
            continue
        part = str(row_vals[pc] or "").strip()
        if not part:
            continue
        vendor = str(row_vals[vc] if len(row_vals) > vc and row_vals[vc] is not None else "").strip()
        result[part.upper()] = vendor

    wb.close()
    return result


@serialized_main_file_write
def update_vendor(path: str, part_number: str, vendor: str) -> dict:
    """更新主檔 B 欄廠商。"""
    part_key = str(part_number or "").strip().upper()
    if not part_key:
        raise ValueError("料號不可空白")

    suffix = Path(path).suffix.lower()
    if suffix == ".xls":
        raise ValueError("xls 主檔不支援直接修改廠商，請先轉成 xlsx 或 xlsm")

    wb = openpyxl.load_workbook(path, keep_vba=(suffix == ".xlsm"))
    try:
        ws = wb.worksheets[0]
        pc = _part_col() + 1
        vc = _vendor_col() + 1
        for row_idx in range(2, ws.max_row + 1):
            cell_part = str(ws.cell(row=row_idx, column=pc).value or "").strip().upper()
            if cell_part != part_key:
                continue
            cell = ws.cell(row=row_idx, column=vc)
            old_vendor = str(cell.value or "").strip()
            new_vendor = str(vendor or "").strip()
            cell.value = new_vendor
            wb.save(path)
            return {
                "part_number": part_key,
                "old_vendor": old_vendor,
                "vendor": new_vendor,
                "row": row_idx,
            }
    finally:
        wb.close()

    raise KeyError(f"主檔找不到料號 {part_key}")


def read_moq(path: str) -> dict[str, float]:
    """讀取主檔 MOQ。"""
    wb = open_workbook_any(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]
    result: dict[str, float] = {}
    pc = _part_col()
    mc = _moq_col()

    for row_vals in ws.iter_rows(min_row=2, values_only=True):
        if not row_vals or len(row_vals) <= mc:
            continue
        part = str(row_vals[pc] or "").strip()
        if not part:
            continue
        moq = _try_float(row_vals[mc]) or 0.0
        result[part.upper()] = moq

    wb.close()
    return result


def find_legacy_snapshot_stock_fixes(path: str, snapshot: dict[str, dict]) -> dict[str, float]:
    """
    找出舊版快照把 MOQ 誤存成庫存的料號。

    只修正可以明確判斷的情況：
    1. snapshot.stock_qty == snapshot.moq 且 moq != 0
    2. 主檔在 MOQ 右側完全沒有任何庫存數字

    這種資料在舊邏輯下會被誤讀成「庫存 = MOQ」，正確值應為 0。
    """
    if not snapshot:
        return {}

    suspicious_parts = {
        str(part).strip().upper()
        for part, values in snapshot.items()
        if (
            str(part).strip()
            and float((values or {}).get("moq") or 0) != 0
            and float((values or {}).get("stock_qty") or 0) == float((values or {}).get("moq") or 0)
        )
    }
    if not suspicious_parts:
        return {}

    wb = open_workbook_any(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]
    fixes: dict[str, float] = {}
    pc = _part_col()

    for row_vals in ws.iter_rows(min_row=2, values_only=True):
        if not row_vals:
            continue
        part = str(row_vals[pc] or "").strip().upper()
        if not part or part not in suspicious_parts:
            continue
        if find_current_stock_cell_from_row_values(row_vals) is None:
            fixes[part] = 0.0

    wb.close()
    return fixes
