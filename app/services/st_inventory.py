from __future__ import annotations

from pathlib import Path

from .xls_reader import open_workbook_any

PART_HEADER = "產品編號"
DESC_HEADER = "品名規格"
STOCK_HEADER = "實際在庫存量"


def _normalize_part(value) -> str:
    return str(value or "").strip().upper()


def _normalize_desc(value) -> str:
    return str(value or "").strip()


def _try_float(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _find_header_row(ws) -> tuple[int, dict[str, int]]:
    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=min(12, ws.max_row), values_only=True), start=1):
        values = [_normalize_desc(cell) for cell in row]
        if PART_HEADER not in values or STOCK_HEADER not in values:
            continue
        columns = {
            "part": values.index(PART_HEADER),
            "stock": values.index(STOCK_HEADER),
            "desc": values.index(DESC_HEADER) if DESC_HEADER in values else -1,
        }
        return row_idx, columns
    raise ValueError("找不到 ST 庫存表頭，需包含「產品編號」與「實際在庫存量」")


def parse_st_inventory_file(path: str) -> dict:
    source_path = Path(path)
    workbook = open_workbook_any(str(source_path), read_only=True, data_only=True)
    try:
        worksheet = workbook.worksheets[0]
        header_row, columns = _find_header_row(worksheet)
        stock: dict[str, float] = {}
        descriptions: dict[str, str] = {}

        for row in worksheet.iter_rows(min_row=header_row + 1, values_only=True):
            if not row:
                continue
            part = _normalize_part(row[columns["part"]] if len(row) > columns["part"] else "")
            if not part:
                continue

            stock_qty = _try_float(row[columns["stock"]] if len(row) > columns["stock"] else 0)
            stock[part] = float(stock.get(part, 0.0) + stock_qty)

            if columns["desc"] >= 0 and len(row) > columns["desc"]:
                description = _normalize_desc(row[columns["desc"]])
                if description and not descriptions.get(part):
                    descriptions[part] = description

        return {
            "sheet_name": worksheet.title,
            "stock": stock,
            "descriptions": descriptions,
            "part_count": len(stock),
        }
    finally:
        workbook.close()
