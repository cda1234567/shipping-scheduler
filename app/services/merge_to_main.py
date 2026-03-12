"""
Merge 已完成排程列的 BOM 扣帳到主檔 Excel。

每份 BOM 檔案各自新增 3 欄（不合併同機種的不同 BOM）：
  col_1 = 增添料 (H, prev_qty_cs)
  col_2 = 生產用量 (F, needed_qty)
  col_3 = 結存 (J = 庫存 + H - F)

表頭格式：batch_code | PO# | BOM model

主檔結構：
  Row 1 = 表頭
  Row 2+ = 資料（A 欄 = 料號）
  最後的數字欄 = 當前庫存
"""
from __future__ import annotations
import shutil
from datetime import datetime
from math import copysign, floor
from pathlib import Path

import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

PART_COL = 1  # A 欄 (1-based)
RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
ORANGE_FILL = PatternFill(start_color="FFC000", end_color="FFC000", fill_type="solid")


def _try_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _round_away(x: float) -> float:
    """四捨五入遠離零"""
    if x == 0:
        return 0.0
    return copysign(floor(abs(x) + 0.5), x)


def backup_main_file(main_path: str, backup_dir: str) -> str:
    """備份主檔，回傳備份路徑。"""
    p = Path(main_path)
    bd = Path(backup_dir)
    bd.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{p.stem}_backup_{ts}{p.suffix}"
    dst = bd / backup_name
    shutil.copy2(main_path, dst)
    return str(dst)


def merge_row_to_main(
    main_path: str,
    groups: list[dict],
    decisions: dict[str, str],
    backup_dir: str | None = None,
) -> dict:
    """
    將已完成排程列的 BOM 扣帳寫入主檔。

    每份 BOM 獨立 3 欄，表頭為 batch_code | PO# | BOM model。

    Parameters
    ----------
    main_path : 主檔 Excel 路徑
    groups : 每份 BOM 的資訊
        [{"batch_code": "1-3", "po_number": "4500059234",
          "bom_model": "T356789IU (A板)", "components": [...]}]
    decisions : { part_number: decision_str }
    backup_dir : 備份資料夾（None = 不備份）

    Returns
    -------
    { "backup_path", "merged_parts", "new_col_count" }
    """
    backup_path = None
    if backup_dir:
        backup_path = backup_main_file(main_path, backup_dir)

    is_macro = Path(main_path).suffix.lower() == ".xlsm"
    wb = openpyxl.load_workbook(main_path, keep_vba=is_macro)
    ws = wb.active

    # 建立 料號 → 列號 對應表
    part_row_map: dict[str, int] = {}
    for row_idx in range(2, ws.max_row + 1):
        raw = ws.cell(row=row_idx, column=PART_COL).value
        part = str(raw or "").strip().upper()
        if part:
            part_row_map[part] = row_idx

    max_col = ws.max_column
    total_merged = 0
    header_font = Font(bold=True, size=9)

    for group in groups:
        components = group.get("components", [])
        if not components:
            continue

        col_h = max_col + 1  # 增添料
        col_f = max_col + 2  # 生產用量
        col_j = max_col + 3  # 結存

        # 表頭：batch_code | PO# | BOM model
        ws.cell(row=1, column=col_h).value = group.get("batch_code", "")
        ws.cell(row=1, column=col_h).font = header_font
        ws.cell(row=1, column=col_f).value = group.get("po_number", "")
        ws.cell(row=1, column=col_f).font = header_font
        ws.cell(row=1, column=col_j).value = group.get("bom_model", "")
        ws.cell(row=1, column=col_j).font = header_font

        for comp in components:
            part_number = comp.get("part_number", "")
            is_dash = comp.get("is_dash", False)
            needed_qty = comp.get("needed_qty", 0)
            prev_qty_cs = comp.get("prev_qty_cs", 0)

            if is_dash or needed_qty <= 0:
                continue

            part_upper = part_number.strip().upper()
            row_idx = part_row_map.get(part_upper)
            if row_idx is None:
                continue

            # 讀目前庫存（從 max_col 往回找最後一個數字）
            current_stock = 0.0
            for c in range(max_col, 0, -1):
                v = _try_float(ws.cell(row=row_idx, column=c).value)
                if v is not None:
                    current_stock = v
                    break

            h_val = _round_away(prev_qty_cs) if prev_qty_cs else 0
            f_val = _round_away(needed_qty)
            j_val = _round_away(current_stock + h_val - f_val)

            decision = decisions.get(part_number, "None")

            # 寫 H（增添料）
            ws.cell(row=row_idx, column=col_h).value = h_val

            # 寫 F（生產用量）
            if decision == "Shortage":
                ws.cell(row=row_idx, column=col_f).value = "缺"
            else:
                ws.cell(row=row_idx, column=col_f).value = f_val
                if decision == "CreateRequirement":
                    ws.cell(row=row_idx, column=col_f).fill = ORANGE_FILL

            # 寫 J（結存）
            if decision == "Shortage":
                j_val_actual = _round_away(current_stock + h_val)
                ws.cell(row=row_idx, column=col_j).value = j_val_actual
                if j_val_actual < 0:
                    ws.cell(row=row_idx, column=col_j).fill = RED_FILL
            else:
                ws.cell(row=row_idx, column=col_j).value = j_val
                if j_val < 0:
                    ws.cell(row=row_idx, column=col_j).fill = RED_FILL

            total_merged += 1

        # 下一組從這裡開始（確保 running balance 正確銜接）
        max_col = col_j

    wb.save(main_path)
    wb.close()

    return {
        "backup_path": backup_path,
        "merged_parts": total_merged,
        "new_col_count": max_col,
    }
