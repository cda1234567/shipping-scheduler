from __future__ import annotations
from pathlib import Path
import openpyxl
import xlrd
import xlrd.xldate


def is_xls(path: str) -> bool:
    return Path(path).suffix.lower() == ".xls"


def open_workbook_any(path: str, read_only: bool = False, data_only: bool = True):
    """開啟 .xls 或 .xlsx，統一回傳 openpyxl workbook。"""
    if is_xls(path):
        return _xls_to_openpyxl(path)
    return openpyxl.load_workbook(path, read_only=read_only, data_only=data_only)


def _xls_to_openpyxl(path: str):
    try:
        xls_wb = xlrd.open_workbook(path, formatting_info=True)
    except Exception:  # 格式資訊讀不到就退回只讀值（僅影響「無年份日期」判斷）
        xls_wb = xlrd.open_workbook(path)
    new_wb = openpyxl.Workbook()
    new_wb.remove(new_wb.active)

    for si in range(xls_wb.nsheets):
        xls_ws = xls_wb.sheets()[si]
        new_ws = new_wb.create_sheet(xls_ws.name)
        for row in range(xls_ws.nrows):
            for col in range(xls_ws.ncols):
                cell = xls_ws.cell(row, col)
                val = cell.value
                if cell.ctype == xlrd.XL_CELL_DATE:
                    val = xlrd.xldate.xldate_as_datetime(val, xls_wb.datemode)
                new_cell = new_ws.cell(row + 1, col + 1, val)
                if cell.ctype == xlrd.XL_CELL_DATE:  # 保留日期格式，供「無年份」判斷
                    try:
                        xf = xls_wb.xf_list[cell.xf_index]
                        new_cell.number_format = xls_wb.format_map[xf.format_key].format_str
                    except (KeyError, IndexError, AttributeError):
                        pass

    return new_wb
