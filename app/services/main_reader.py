from __future__ import annotations
from ..config import cfg
from .xls_reader import open_workbook_any

_PART_COL = None
_MOQ_COL = None


def _part_col():
    global _PART_COL
    if _PART_COL is None:
        _PART_COL = cfg("excel.main_part_col", 0)
    return _PART_COL


def _moq_col():
    global _MOQ_COL
    if _MOQ_COL is None:
        _MOQ_COL = cfg("excel.main_moq_col", 2)
    return _MOQ_COL


def _try_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def read_stock(path: str) -> dict[str, float]:
    """讀主檔：每料號取最後一個數字欄作為現有庫存。"""
    wb = open_workbook_any(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]
    result: dict[str, float] = {}
    pc = _part_col()

    for row_vals in ws.iter_rows(min_row=2, values_only=True):
        if not row_vals:
            continue
        part = str(row_vals[pc] or "").strip()
        if not part:
            continue
        for v in reversed(row_vals):
            f = _try_float(v)
            if f is not None:
                result[part.upper()] = f
                break

    wb.close()
    return result


def read_moq(path: str) -> dict[str, float]:
    """讀主檔：MOQ。"""
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
