"""
主檔負數紅字：用條件式格式（cellIs lessThan 0 → 紅字）標示主檔庫存帳的負數。

只動條件式格式，不改任何儲存格自身樣式；重複套用只會留一條規則，範圍跟著欄列數更新。
"""
from __future__ import annotations

import io
from pathlib import Path

from openpyxl.formatting.formatting import ConditionalFormattingList
from openpyxl.formatting.rule import CellIsRule, Rule
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

NEGATIVE_RED_COLOR = "FF0000"
NEGATIVE_RED_FIRST_COL = 4  # D 欄；A–C 料號／廠商／MOQ 不套
NEGATIVE_RED_FIRST_ROW = 2


def _is_negative_red_rule(rule: Rule) -> bool:
    if getattr(rule, "type", None) != "cellIs" or getattr(rule, "operator", None) != "lessThan":
        return False
    if [str(f).strip() for f in (rule.formula or [])] != ["0"]:
        return False
    font = getattr(getattr(rule, "dxf", None), "font", None)
    color = getattr(font, "color", None)
    rgb = str(getattr(color, "rgb", "") or "").upper()
    return rgb.endswith(NEGATIVE_RED_COLOR)


def negative_red_range(worksheet) -> str | None:
    max_row = worksheet.max_row or 0
    max_col = worksheet.max_column or 0
    if max_row < NEGATIVE_RED_FIRST_ROW or max_col < NEGATIVE_RED_FIRST_COL:
        return None
    return (
        f"{get_column_letter(NEGATIVE_RED_FIRST_COL)}{NEGATIVE_RED_FIRST_ROW}:"
        f"{get_column_letter(max_col)}{max_row}"
    )


def find_negative_red_ranges(worksheet) -> list[str]:
    """回傳工作表上本系統負數紅字規則所在的範圍（測試與冪等檢查用）。"""
    ranges: list[str] = []
    for cf in worksheet.conditional_formatting:
        for rule in cf.rules:
            if _is_negative_red_rule(rule):
                ranges.append(str(cf.sqref))
    return ranges


def has_current_negative_red_rule(workbook) -> bool:
    if not workbook.worksheets:
        return True
    worksheet = workbook.worksheets[0]
    target = negative_red_range(worksheet)
    ranges = find_negative_red_ranges(worksheet)
    if target is None:
        return not ranges
    return ranges == [target]


def apply_main_negative_red(workbook) -> bool:
    """在主檔第一張工作表套用負數紅字條件式格式；已是最新則不動。回傳是否有變更。"""
    if not workbook.worksheets or has_current_negative_red_rule(workbook):
        return False
    worksheet = workbook.worksheets[0]

    # 拿掉舊的本系統規則（可能範圍過期或重複），其他條件式格式原樣保留
    rebuilt = ConditionalFormattingList()
    for cf in worksheet.conditional_formatting:
        kept = [rule for rule in cf.rules if not _is_negative_red_rule(rule)]
        for rule in kept:
            rebuilt.add(str(cf.sqref), rule)
    for rules in rebuilt._cf_rules.values():
        for rule in rules:
            rebuilt.max_priority = max(rebuilt.max_priority, int(rule.priority or 0))

    target = negative_red_range(worksheet)
    if target is not None:
        rebuilt.add(
            target,
            CellIsRule(operator="lessThan", formula=["0"], font=Font(color=NEGATIVE_RED_COLOR)),
        )
    worksheet.conditional_formatting = rebuilt
    return True


def save_main_workbook(workbook, path) -> None:
    """所有寫主檔的路徑共用：存檔前補上負數紅字規則。"""
    apply_main_negative_red(workbook)
    workbook.save(path)


def main_file_download_bytes(path: str) -> bytes | None:
    """下載主檔用：檔案還沒有最新規則時，在記憶體套用後輸出；已有規則或 .xls 回 None（直接送原檔）。"""
    import openpyxl

    suffix = Path(path).suffix.lower()
    if suffix not in {".xlsx", ".xlsm"}:
        return None
    workbook = openpyxl.load_workbook(path, keep_vba=(suffix == ".xlsm"))
    try:
        if not apply_main_negative_red(workbook):
            return None
        buffer = io.BytesIO()
        workbook.save(buffer)
        return buffer.getvalue()
    finally:
        workbook.close()
        if getattr(workbook, "vba_archive", None) is not None:
            workbook.vba_archive.close()
