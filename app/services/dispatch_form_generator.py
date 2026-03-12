from __future__ import annotations

from datetime import datetime

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill

ORANGE_FILL = PatternFill(start_color="FFC000", end_color="FFC000", fill_type="solid")
WHITE_FILL = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")

HEADER_CODE_FONT = Font(name="Calibri", size=14)
HEADER_TITLE_FONT = Font(name="Calibri", size=16)
HEADER_META_FONT = Font(name="Calibri", size=10)
INDEX_FONT = Font(name="Calibri", size=10)
DATA_FONT = Font(name="Calibri", size=9)

HEADER_ALIGNMENT = Alignment(horizontal="center", vertical="center")
TEXT_ALIGNMENT = Alignment(vertical="center")
VALUE_ALIGNMENT = Alignment(horizontal="center", vertical="center")


def _roc_year(western_year: int) -> int:
    return western_year - 1911


def _set_column_layout(ws):
    ws.column_dimensions["A"].width = 3.625
    ws.column_dimensions["B"].width = 0.125
    ws.column_dimensions["C"].width = 18.125
    ws.column_dimensions["D"].width = 68.875
    ws.column_dimensions["E"].width = 8.625


def _build_title(model: str, date_str: str, now: datetime) -> str:
    try:
        parts = date_str.replace("-", "/").split("/")
        year, month = int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        year, month = now.year, now.month
    return f"           辰尚-庚霖   {_roc_year(year)}年 {month}月份  {model}  之發料單　　　"


def _write_section_header(ws, row_idx: int, group: dict, now: datetime):
    batch_code = group.get("batch_code") or ""
    po_number = str(group.get("po_number", ""))
    date_str = group.get("date") or now.strftime("%Y/%m/%d")
    title = _build_title(group.get("model", ""), date_str, now)

    ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=3)
    ws.merge_cells(start_row=row_idx + 1, start_column=1, end_row=row_idx + 1, end_column=3)
    ws.merge_cells(start_row=row_idx, start_column=4, end_row=row_idx + 1, end_column=4)

    ws.row_dimensions[row_idx].height = 18.75
    ws.row_dimensions[row_idx + 1].height = 18.75

    code_cell = ws.cell(row=row_idx, column=1)
    code_cell.value = batch_code
    code_cell.font = HEADER_CODE_FONT
    code_cell.alignment = HEADER_ALIGNMENT

    po_cell = ws.cell(row=row_idx + 1, column=1)
    po_cell.value = po_number
    po_cell.font = HEADER_CODE_FONT
    po_cell.alignment = HEADER_ALIGNMENT

    title_cell = ws.cell(row=row_idx, column=4)
    title_cell.value = title
    title_cell.font = HEADER_TITLE_FONT
    title_cell.alignment = HEADER_ALIGNMENT

    label_cell = ws.cell(row=row_idx, column=5)
    label_cell.value = "日期"
    label_cell.font = HEADER_META_FONT
    label_cell.alignment = HEADER_ALIGNMENT

    date_cell = ws.cell(row=row_idx + 1, column=5)
    date_cell.value = date_str
    date_cell.font = HEADER_META_FONT
    date_cell.alignment = HEADER_ALIGNMENT


def _write_item_row(ws, row_idx: int, index: int, item: dict):
    qty_value = item.get("qty", "")
    is_shortage = bool(item.get("is_shortage"))
    description = item.get("desc", "") or ""

    ws.row_dimensions[row_idx].height = 24.0 if len(description) > 90 or is_shortage else 18.75

    index_cell = ws.cell(row=row_idx, column=1)
    index_cell.value = index
    index_cell.font = INDEX_FONT
    index_cell.alignment = VALUE_ALIGNMENT

    part_cell = ws.cell(row=row_idx, column=3)
    part_cell.value = item.get("part", "")
    part_cell.font = DATA_FONT
    part_cell.alignment = TEXT_ALIGNMENT

    desc_cell = ws.cell(row=row_idx, column=4)
    desc_cell.value = description
    desc_cell.font = DATA_FONT
    desc_cell.alignment = Alignment(vertical="center", wrap_text=True)

    qty_cell = ws.cell(row=row_idx, column=5)
    qty_cell.value = "缺" if is_shortage else qty_value
    qty_cell.font = DATA_FONT
    qty_cell.alignment = VALUE_ALIGNMENT
    qty_cell.fill = ORANGE_FILL if item.get("fill_color") else WHITE_FILL


def generate_dispatch_form(groups: list[dict], output_path: str) -> str:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "發料單"
    _set_column_layout(ws)

    now = datetime.now()
    current_row = 1

    for group in groups:
        items = group.get("items", [])
        if not items:
            continue

        _write_section_header(ws, current_row, group, now)
        current_row += 2

        for index, item in enumerate(items, start=1):
            _write_item_row(ws, current_row, index, item)
            current_row += 1

    wb.save(output_path)
    wb.close()
    return output_path
