"""
發料單生成器 — 從排程計算結果生成發料單 xlsx

格式：
  - 每個 section = 一個排程行（一張工單）
  - Section Header (2 rows):
    Row 1: A:C merged=批次代碼, D=標題, E="日期", K="生產量"
    Row 2: A:C merged=PO#, E=日期值, K=生產數量(order_qty)
  - Data Rows: A=序號, B=空, C=零件號, D=說明, E=數量/"缺"
  - 橘色 E 欄 = 需採購（CreateRequirement）
  - "缺" = 缺料（Shortage）
"""
from __future__ import annotations
import os
from datetime import datetime

import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment

ORANGE_FILL = PatternFill(start_color="FFC000", end_color="FFC000", fill_type="solid")

COL_K = 11   # K 欄（生產數量）


def _roc_year(western_year: int) -> int:
    return western_year - 1911


def generate_dispatch_form(groups: list[dict], output_path: str) -> str:
    """
    生成發料單 xlsx。

    groups 格式：
    [
      {
        "batch_code": "2-5",       # row.code
        "po_number": "4500059234",
        "model": "T7U",
        "date": "2026/03/09",
        "order_qty": 144,          # 生產數量 (K1)
        "items": [
          {"part": "ABCD1234", "desc": "...", "qty": 100,
           "fill_color": "FFC000" or None, "is_shortage": False}, ...
        ]
      }, ...
    ]
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "發料單"

    ws.column_dimensions['A'].width = 6
    ws.column_dimensions['B'].width = 0.5
    ws.column_dimensions['C'].width = 18
    ws.column_dimensions['D'].width = 60
    ws.column_dimensions['E'].width = 10
    ws.column_dimensions['K'].width = 10

    header_font = Font(name="新細明體", size=10, bold=True)
    data_font   = Font(name="新細明體", size=9)

    current_row = 1
    now = datetime.now()

    for g_idx, group in enumerate(groups):
        items = group.get("items", [])
        if not items:
            continue

        batch_code = group.get("batch_code") or f"{g_idx + 1}"
        po_number  = str(group.get("po_number", ""))
        model      = group.get("model", "")
        date_str   = group.get("date", now.strftime("%Y/%m/%d"))
        order_qty  = group.get("order_qty", 0)

        try:
            parts = date_str.replace("-", "/").split("/")
            year, month = int(parts[0]), int(parts[1])
        except (ValueError, IndexError):
            year, month = now.year, now.month

        roc   = _roc_year(year)
        title = f"{roc}年 {month}月份  {model}  之發料單"

        # ─ Header Row 1：批次代碼 | 標題 | "日期" | K="生產量"
        ws.merge_cells(start_row=current_row, start_column=1,
                       end_row=current_row,   end_column=3)
        c = ws.cell(row=current_row, column=1)
        c.value, c.font = batch_code, header_font

        ws.merge_cells(start_row=current_row,   start_column=4,
                       end_row=current_row + 1, end_column=4)
        c = ws.cell(row=current_row, column=4)
        c.value     = f"        {title}"
        c.font      = header_font
        c.alignment = Alignment(vertical="center", wrap_text=True)

        ws.cell(row=current_row, column=5).value = "日期"
        ws.cell(row=current_row, column=5).font  = header_font

        c = ws.cell(row=current_row, column=COL_K)
        c.value, c.font = "生產量", header_font

        # ─ Header Row 2：PO# | 日期值 | K=生產數量
        current_row += 1
        ws.merge_cells(start_row=current_row, start_column=1,
                       end_row=current_row,   end_column=3)
        c = ws.cell(row=current_row, column=1)
        c.value, c.font = po_number, header_font

        ws.cell(row=current_row, column=5).value = date_str
        ws.cell(row=current_row, column=5).font  = header_font

        c = ws.cell(row=current_row, column=COL_K)
        c.value, c.font = order_qty, header_font

        # ─ Data Rows
        current_row += 1
        for idx, item in enumerate(items, start=1):
            ws.cell(row=current_row, column=1).value = idx
            ws.cell(row=current_row, column=1).font  = data_font
            ws.cell(row=current_row, column=3).value = item.get("part", "")
            ws.cell(row=current_row, column=3).font  = data_font
            ws.cell(row=current_row, column=4).value = item.get("desc", "")
            ws.cell(row=current_row, column=4).font  = data_font

            qty_cell = ws.cell(row=current_row, column=5)
            qty_cell.font = data_font
            if item.get("is_shortage"):
                qty_cell.value = "缺"
            else:
                qty_cell.value = item.get("qty", 0)

            fill_color = item.get("fill_color")
            if fill_color:
                qty_cell.fill = PatternFill(start_color=fill_color,
                                            end_color=fill_color,
                                            fill_type="solid")
            current_row += 1

        current_row += 1  # 各 section 之間空一行

    wb.save(output_path)
    wb.close()
    return output_path
