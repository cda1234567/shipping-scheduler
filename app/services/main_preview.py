from __future__ import annotations

import json
import os
import threading
from collections import OrderedDict
from datetime import date, datetime, time
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

from openpyxl.styles.numbers import is_date_format
from openpyxl.utils import get_column_letter, range_boundaries

from .. import database as db
from ..config import DATA_DIR
from .xls_reader import is_xls, open_workbook_any

_DEFAULT_COLUMN_WIDTH_PX = 92
_DEFAULT_ROW_HEIGHT_PX = 24
MAIN_PREVIEW_CACHE_DIR = DATA_DIR / "cache" / "main_preview"
# 預覽 payload 格式變動時遞增，舊磁碟快取自動失效
_PREVIEW_CACHE_VERSION = 2


_RESPONSE_CACHE_MAX = 8
_response_cache: "OrderedDict[tuple, bytes]" = OrderedDict()
_response_cache_lock = threading.Lock()


def get_main_preview_json_bytes(
    path: str,
    sheet_name: str | None = None,
    *,
    extra: dict | None = None,
    compact: bool = False,
) -> bytes:
    """回傳組好的預覽 JSON bytes；主檔 mtime/size、封存欄位、extra 沒變就直接重用。"""
    path_obj = Path(path)
    stat = path_obj.stat()
    archived = db.list_archived_orders()
    active_keys = (
        tuple(sorted((str(o["code"]), str(o["po_number"])) for o in db.get_orders()))
        if archived else ()
    )
    key = (
        str(path_obj),
        stat.st_mtime_ns,
        stat.st_size,
        str(sheet_name or "").strip(),
        repr(archived),
        active_keys,
        json.dumps(extra or {}, sort_keys=True, ensure_ascii=False, default=str),
        bool(compact),
    )
    with _response_cache_lock:
        cached = _response_cache.get(key)
        if cached is not None:
            _response_cache.move_to_end(key)
            return cached

    preview = dict(read_live_main_preview(path, sheet_name=sheet_name))
    if compact:
        preview = _compact_preview(preview)
    preview.update(extra or {})
    body = json.dumps(preview, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")

    with _response_cache_lock:
        _response_cache[key] = body
        _response_cache.move_to_end(key)
        while len(_response_cache) > _RESPONSE_CACHE_MAX:
            _response_cache.popitem(last=False)
    return body


def clear_main_preview_response_cache() -> None:
    with _response_cache_lock:
        _response_cache.clear()


def _compact_preview(preview: dict) -> dict:
    """前端用：省略沒有值、也不是合併儲存格的空白格（約佔 9 成），其餘欄位原樣保留。"""
    sheet = dict(preview["sheet"])
    sheet["rows"] = [
        {
            **row,
            "cells": [
                c for c in row["cells"]
                if c.get("value") not in ("", None) or "rowspan" in c or "colspan" in c
            ],
        }
        for row in sheet["rows"]
    ]
    return {**preview, "sheet": sheet, "compact": True}


def read_live_main_preview(path: str, sheet_name: str | None = None) -> dict:
    path_obj = Path(path)
    stat = path_obj.stat()
    preview = _read_live_main_preview_cached(
        str(path_obj),
        stat.st_mtime_ns,
        stat.st_size,
        str(sheet_name or "").strip(),
    )

    return _exclude_archived_columns(preview)


def _exclude_archived_columns(preview: dict) -> dict:
    # 回傳淺層新結構、共用快取內的 cell dict（呼叫端不得改動 cell）；不再整包 deepcopy
    archived = db.list_archived_orders()
    if not archived:
        return {**preview, "sheet": {**preview["sheet"]}}
    active = db.get_orders()
    archived_keys = {(str(o["code"]), str(o["po_number"])) for o in archived}
    active_keys = {(str(o["code"]), str(o["po_number"])) for o in active}
    sheet = preview["sheet"]
    headers = {c["col"]: str(c.get("value") or "").strip() for r in sheet["rows"] if r["index"] == 1 for c in r["cells"]}
    hidden = set()
    for col, code in headers.items():
        key = (code, headers.get(col + 1, ""))
        if key in archived_keys and key not in active_keys:
            hidden.update((col, col + 1, col + 2))
    result = {**preview, "sheet": {**sheet}}
    result["sheet"]["columns"] = [c for c in sheet["columns"] if c["index"] not in hidden]
    result["sheet"]["rows"] = [
        {**row, "cells": [c for c in row["cells"] if c["col"] not in hidden]}
        for row in sheet["rows"]
    ]
    # 保留原始座標與總欄數，讓未封存欄位仍可正確編輯。
    return result


@lru_cache(maxsize=16)
def _read_live_main_preview_cached(path: str, modified_ns: int, size: int, requested_sheet: str) -> dict:
    cached_preview = _read_disk_cache(modified_ns, size, requested_sheet)
    if cached_preview is not None:
        return cached_preview

    workbook = open_workbook_any(path, read_only=False, data_only=True)
    try:
        sheet_names = list(workbook.sheetnames)
        if not sheet_names:
            raise ValueError("Workbook has no sheets")

        selected_sheet = requested_sheet if requested_sheet in sheet_names else workbook.active.title
        worksheet = workbook[selected_sheet]
        preview = {
            "sheet_names": sheet_names,
            "selected_sheet": selected_sheet,
            "style_preserved": not is_xls(path),
            "sheet": _serialize_sheet(worksheet),
            "cache_version": _PREVIEW_CACHE_VERSION,
        }
        _write_disk_cache(modified_ns, size, requested_sheet, preview)
        return preview
    finally:
        try:
            workbook.close()
        except Exception:
            pass


def _disk_cache_path(modified_ns: int, size: int, sheet_name: str) -> Path:
    sheet_token = quote(str(sheet_name or ""), safe="") or "_"
    return MAIN_PREVIEW_CACHE_DIR / f"{modified_ns}_{size}_{sheet_token}.json"


def _read_disk_cache(modified_ns: int, size: int, requested_sheet: str) -> dict | None:
    if not MAIN_PREVIEW_CACHE_DIR.exists():
        return None

    cache_path = _disk_cache_path(modified_ns, size, requested_sheet)
    if not cache_path.exists():
        return None

    try:
        with cache_path.open("r", encoding="utf-8") as f:
            cached = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(cached, dict) or cached.get("cache_version") != _PREVIEW_CACHE_VERSION:
        return None
    return cached


def _write_disk_cache(modified_ns: int, size: int, sheet_name: str, preview: dict) -> None:
    try:
        MAIN_PREVIEW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        clean_main_preview_disk_cache(modified_ns)
        cache_path = _disk_cache_path(modified_ns, size, sheet_name)
        tmp_path = cache_path.with_suffix(cache_path.suffix + f".{os.getpid()}.tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(preview, f, ensure_ascii=False, separators=(",", ":"))
        tmp_path.replace(cache_path)
    except OSError:
        return


def clean_main_preview_disk_cache(current_main_mtime_ns: int) -> None:
    if not MAIN_PREVIEW_CACHE_DIR.exists():
        return

    for cache_path in MAIN_PREVIEW_CACHE_DIR.glob("*.json"):
        try:
            cache_mtime_ns = int(cache_path.name.split("_", 1)[0])
        except (ValueError, IndexError):
            continue
        if cache_mtime_ns < current_main_mtime_ns:
            try:
                cache_path.unlink()
            except OSError:
                pass


def _serialize_sheet(worksheet) -> dict:
    max_row, max_col = _detect_used_bounds(worksheet)
    merged_anchors, merged_skips = _build_merged_maps(worksheet)
    styles: list[dict] = []
    style_map: dict[str, int] = {}
    rows: list[dict] = []

    for row_idx in range(1, max_row + 1):
        row_cells: list[dict] = []
        for col_idx in range(1, max_col + 1):
            if (row_idx, col_idx) in merged_skips:
                continue

            cell = worksheet.cell(row=row_idx, column=col_idx)
            style_id = _register_style(_serialize_style(cell), styles, style_map)
            cell_payload = {
                "col": col_idx,
                "value": _format_cell_value(cell.value, cell.number_format),
                "style_id": style_id,
            }
            if _is_negative_number(cell.value):
                # 顯示層標示負數紅字，不動原儲存格樣式
                cell_payload["negative"] = True
            merge = merged_anchors.get((row_idx, col_idx))
            if merge:
                cell_payload["rowspan"] = merge["rowspan"]
                cell_payload["colspan"] = merge["colspan"]
            row_cells.append(cell_payload)

        rows.append({
            "index": row_idx,
            "height_px": _row_height_to_px(worksheet.row_dimensions[row_idx].height),
            "cells": row_cells,
        })

    columns = [
        {
            "index": col_idx,
            "letter": get_column_letter(col_idx),
            "width_px": _column_width_to_px(worksheet.column_dimensions[get_column_letter(col_idx)].width),
        }
        for col_idx in range(1, max_col + 1)
    ]

    return {
        "name": worksheet.title,
        "row_count": max_row,
        "col_count": max_col,
        "columns": columns,
        "rows": rows,
        "styles": styles,
    }


def _is_negative_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value < 0


def _detect_used_bounds(worksheet) -> tuple[int, int]:
    try:
        _, _, max_col, max_row = range_boundaries(worksheet.calculate_dimension())
    except ValueError:
        max_row, max_col = worksheet.max_row or 1, worksheet.max_column or 1

    for merged_range in worksheet.merged_cells.ranges:
        max_row = max(max_row, merged_range.max_row)
        max_col = max(max_col, merged_range.max_col)

    return max(max_row, 1), max(max_col, 1)


def _build_merged_maps(worksheet) -> tuple[dict[tuple[int, int], dict], set[tuple[int, int]]]:
    anchors: dict[tuple[int, int], dict] = {}
    skips: set[tuple[int, int]] = set()

    for merged_range in worksheet.merged_cells.ranges:
        anchor = (merged_range.min_row, merged_range.min_col)
        anchors[anchor] = {
            "rowspan": merged_range.max_row - merged_range.min_row + 1,
            "colspan": merged_range.max_col - merged_range.min_col + 1,
        }
        for row_idx in range(merged_range.min_row, merged_range.max_row + 1):
            for col_idx in range(merged_range.min_col, merged_range.max_col + 1):
                if (row_idx, col_idx) == anchor:
                    continue
                skips.add((row_idx, col_idx))

    return anchors, skips


def _register_style(style: dict, styles: list[dict], style_map: dict[str, int]) -> int:
    style_key = json.dumps(style, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    cached = style_map.get(style_key)
    if cached is not None:
        return cached

    style_id = len(styles)
    styles.append(style)
    style_map[style_key] = style_id
    return style_id


def _serialize_style(cell) -> dict:
    style: dict[str, object] = {}

    background = _extract_fill_color(cell.fill)
    if background:
        style["background"] = background

    font_color = _color_to_hex(cell.font.color)
    if font_color:
        style["color"] = font_color
    if cell.font.name:
        style["font_name"] = cell.font.name
    if cell.font.sz:
        style["font_size"] = round(float(cell.font.sz), 1)
    if cell.font.bold:
        style["bold"] = True
    if cell.font.italic:
        style["italic"] = True
    if cell.font.underline and cell.font.underline != "none":
        style["underline"] = True

    horizontal = cell.alignment.horizontal or ("right" if isinstance(cell.value, (int, float)) else "left")
    vertical = cell.alignment.vertical or "center"
    if horizontal:
        style["align"] = horizontal
    if vertical:
        style["valign"] = vertical
    if cell.alignment.wrap_text:
        style["wrap"] = True

    for side_name in ("top", "right", "bottom", "left"):
        border_css = _border_side_to_css(getattr(cell.border, side_name))
        if border_css:
            style[f"border_{side_name}"] = border_css

    return style


def _extract_fill_color(fill) -> str | None:
    if getattr(fill, "fill_type", None) != "solid":
        return None
    return _color_to_hex(getattr(fill, "fgColor", None))


def _color_to_hex(color) -> str | None:
    if not color:
        return None

    color_type = getattr(color, "type", None)
    if color_type != "rgb":
        return None

    rgb = str(getattr(color, "rgb", "") or "").strip()
    if len(rgb) == 8:
        rgb = rgb[2:]
    if len(rgb) == 6 and rgb.upper() != "000000":
        return f"#{rgb.upper()}"
    return None


def _border_side_to_css(side) -> str | None:
    border_style = getattr(side, "style", None)
    if not border_style:
        return None

    width = "1px"
    css_style = "solid"
    if border_style in {"medium", "mediumDashDot", "mediumDashDotDot", "mediumDashed"}:
        width = "2px"
    elif border_style == "thick":
        width = "3px"
    elif border_style == "double":
        width = "3px"
        css_style = "double"
    elif border_style in {"dashed", "mediumDashed"}:
        css_style = "dashed"
    elif border_style == "dotted":
        css_style = "dotted"

    color = _color_to_hex(getattr(side, "color", None)) or "#d0d7e2"
    return f"{width} {css_style} {color}"


def _format_cell_value(value, number_format: str | None = None) -> str:
    if value is None:
        return ""

    if isinstance(value, datetime):
        return value.strftime("%Y/%m/%d %H:%M")
    if isinstance(value, date):
        return value.strftime("%Y/%m/%d")
    if isinstance(value, time):
        return value.strftime("%H:%M")
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        if number_format and is_date_format(number_format):
            return str(value)
        return _format_numeric(value, number_format or "")

    return str(value)


def _format_numeric(value: int | float, number_format: str) -> str:
    format_code = (number_format or "").split(";")[0]
    decimals = _count_decimal_places(format_code)
    use_grouping = "," in format_code and "%" not in format_code

    if "%" in format_code:
        digits = 0 if decimals is None else decimals
        return f"{value * 100:.{digits}f}%"

    if decimals is not None:
        template = f"{{:{',' if use_grouping else ''}.{decimals}f}}"
        rendered = template.format(value)
        if decimals > 0:
            rendered = rendered.rstrip("0").rstrip(".")
        return rendered

    if float(value).is_integer():
        integer_value = int(round(float(value)))
        return f"{integer_value:,}" if use_grouping else str(integer_value)

    rendered = f"{value:,.4f}" if use_grouping else f"{value:.4f}"
    return rendered.rstrip("0").rstrip(".")


def _count_decimal_places(format_code: str) -> int | None:
    if "." not in format_code:
        return 0 if "0" in format_code else None

    decimals = 0
    started = False
    for char in format_code.split(".", 1)[1]:
        if char in {"0", "#"}:
            decimals += 1
            started = True
            continue
        if started:
            break
    return decimals


def _column_width_to_px(width: float | None) -> int:
    if width is None:
        return _DEFAULT_COLUMN_WIDTH_PX
    return max(48, int(round(width * 7 + 14)))


def _row_height_to_px(height: float | None) -> int:
    if height is None:
        return _DEFAULT_ROW_HEIGHT_PX
    return max(22, int(round(height * 1.34)))
