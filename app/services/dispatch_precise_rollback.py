"""單筆發料精準反悔：只撤銷這張單的發料欄位（尾端直接刪欄，否則歸零 + 重算後續結餘）。

和整檔還原（用發料前備份覆蓋主檔）不同，之後的手動主檔編輯、收料、不良品 / 加工多打扣帳
與其他訂單的發料都會保留。只要有任何不確定（插單、欄位被改過、找不到唯一欄位、結餘鏈不一致、
有公式…）就 raise PreciseRollbackUnavailable，由呼叫端改走原本的整檔還原。
"""
from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path

import openpyxl

from .. import database as db
from .main_event_rollback import (
    PreciseRollbackUnavailable,
    is_blank,
    neutralize_or_remove_block,
    number,
    part_rows,
)
from .merge_to_main import _round_away, read_dispatch_code_changes

log = logging.getLogger(__name__)

PRECISE_ROLLBACK_LOG_ACTION = "order_rollback_precise"
ROLLED_BACK_HEADER_PREFIX = "已退回"
MARKER_PREFIX = "dispatch-precise-rollback:"
_MAIN_EDIT_LOG_ACTION = "主檔編輯"
_TOL = 1e-6


def norm_header(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value).strip()


def build_dispatch_column_record(plan_rows: list[dict], *, insert_before_code: str = "",
                                 code_shifts=None) -> dict:
    groups: dict[tuple, dict] = {}
    for row in plan_rows or []:
        key = (row.get("col_h"), row.get("col_f"), row.get("col_j"))
        if None in key:
            continue
        group = groups.setdefault(key, {
            "col_h": int(key[0]), "col_f": int(key[1]), "col_j": int(key[2]),
            "batch_code": norm_header(row.get("group_batch_code")),
            "po_number": norm_header(row.get("group_po_number")),
            "bom_model": norm_header(row.get("group_bom_model")),
            "rows": [],
        })
        h_value = number(row.get("effective_h")) or 0.0
        group["rows"].append({
            "row": int(row.get("row_idx") or 0),
            "part": str(row.get("part_number") or "").strip().upper(),
            "h": h_value,
            "f": float(row.get("f_value") or 0),
            "j": float(row.get("j_value") or 0),
        })
    return {
        "version": 1,
        "groups": sorted(groups.values(), key=lambda item: item["col_h"]),
        "insert_before_code": str(insert_before_code or ""),
        "code_shifts": bool(code_shifts),
    }


def _row_for_part(rows_by_part: dict[str, list[int]], part: str, hint: int | None = None) -> int:
    rows = rows_by_part.get(part) or []
    if len(rows) == 1:
        return rows[0]
    if hint and hint in rows:
        return hint
    raise PreciseRollbackUnavailable(f"主檔找不到唯一的料號 {part} 列")


def _cell_number(ws, row: int, col: int) -> float | None:
    return number(ws.cell(row, col).value)


def _header_triples(ws) -> list[tuple[int, str, str, str]]:
    headers = [norm_header(ws.cell(1, col).value) for col in range(1, ws.max_column + 1)]
    return [(col + 1, headers[col], headers[col + 1] if col + 1 < len(headers) else "",
             headers[col + 2] if col + 2 < len(headers) else "") for col in range(len(headers))]


def _locate_recorded(ws, order: dict, record: dict) -> list[dict]:
    rows_by_part = part_rows(ws)
    triples = _header_triples(ws)
    current_code = norm_header(order.get("code"))
    located: list[dict] = []
    used: set[int] = set()
    for group in record.get("groups") or []:
        codes = {norm_header(group.get("batch_code")), current_code} - {""}
        po = norm_header(group.get("po_number"))
        model = norm_header(group.get("bom_model"))
        recorded_rows = []
        for item in group.get("rows") or []:
            recorded_rows.append((_row_for_part(rows_by_part, item["part"], item.get("row")), item))
        matches = []
        for start, code, po_header, model_header in triples:
            if code not in codes or po_header != po or model_header != model or start in used:
                continue
            ok = True
            for row, item in recorded_rows:
                if abs((_cell_number(ws, row, start) or 0.0) - float(item.get("h") or 0)) > _TOL \
                        or _cell_number(ws, row, start + 1) is None \
                        or abs(_cell_number(ws, row, start + 1) - float(item.get("f") or 0)) > _TOL:
                    ok = False
                    break
            if ok:
                matches.append(start)
        if len(matches) != 1:
            raise PreciseRollbackUnavailable(
                "主檔找不到這筆發料原本的欄位（可能被手動改過、補料或被插單調整）" if not matches
                else "主檔有多組相同的發料欄位，無法判斷要撤銷哪一組"
            )
        start = matches[0]
        used.add(start)
        located.append({
            "start_col": start,
            "code": next(iter(codes & {norm_header(ws.cell(1, start).value)}), current_code),
            "rows": {row: {"part": item["part"], "h": float(item.get("h") or 0), "f": float(item.get("f") or 0)}
                     for row, item in recorded_rows},
        })
    if not located:
        raise PreciseRollbackUnavailable("這筆發料沒有寫入主檔欄位")
    return located


def _locate_legacy(ws, order: dict, records: list[dict]) -> list[dict]:
    code = norm_header(order.get("code"))
    po = norm_header(order.get("po_number"))
    if not code or not po:
        raise PreciseRollbackUnavailable("訂單缺少批次碼或 PO，無法比對主檔欄位")
    expected = Counter()
    for record in records:
        qty = _round_away(float(record.get("needed_qty") or 0))
        if qty:
            expected[(str(record.get("part_number") or "").strip().upper(), qty)] += 1
    if not expected:
        raise PreciseRollbackUnavailable("找不到這筆的發料明細，無法比對主檔欄位")

    starts = [start for start, header_code, header_po, _ in _header_triples(ws)
              if header_code == code and header_po == po]
    runs: list[list[int]] = []
    for start in starts:
        if runs and start == runs[-1][-1] + 3:
            runs[-1].append(start)
        else:
            runs.append([start])
    rows_by_part = part_rows(ws)
    part_by_row = {row: part for part, rows in rows_by_part.items() for row in rows}
    candidates = []
    for run in runs:
        actual = Counter()
        groups = []
        for start in run:
            group_rows = {}
            for row in range(2, ws.max_row + 1):
                f_value = _cell_number(ws, row, start + 1)
                h_value = _cell_number(ws, row, start)
                if f_value is None or h_value is None:
                    group_rows = None
                    break
                if is_blank(ws.cell(row, start + 1).value) and is_blank(ws.cell(row, start).value) \
                        and is_blank(ws.cell(row, start + 2).value):
                    continue
                part = part_by_row.get(row, "")
                if not part or len(rows_by_part.get(part) or []) != 1:
                    group_rows = None
                    break
                group_rows[row] = {"part": part, "h": h_value, "f": f_value}
                if f_value:
                    actual[(part, f_value)] += 1
            if group_rows is None:
                break
            groups.append({"start_col": start, "code": code, "rows": group_rows})
        else:
            if actual == expected:
                candidates.append(groups)
    if len(candidates) != 1:
        raise PreciseRollbackUnavailable(
            "主檔欄位和發料紀錄對不起來（可能被手動改過或補料）" if not candidates
            else "主檔有多組相同單號的欄位，無法判斷"
        )
    return candidates[0]


def _ensure_no_later_edits(session: dict, located: list[dict], *, legacy: bool) -> None:
    dispatched_at = str(session.get("dispatched_at") or "")
    cols = {group["start_col"] + offset for group in located for offset in range(3)}
    from .inventory_restore_guard import parse_main_edit_detail

    for item in db.get_activity_logs_after(dispatched_at, actions=(_MAIN_EDIT_LOG_ACTION,), limit=100000):
        parsed = parse_main_edit_detail(str(item.get("detail") or ""))
        if parsed is None or parsed["col"] in cols:
            raise PreciseRollbackUnavailable("這筆發料欄位之後被手動編輯過，請改用整檔還原")
    if legacy and db.get_activity_logs_after(dispatched_at, actions=("supplement_part",), limit=1):
        raise PreciseRollbackUnavailable("這筆發料之後有補料紀錄，舊發料無法確認欄位，請改用整檔還原")


def ensure_precise_preconditions(session: dict, order: dict | None) -> str:
    if not session or not order:
        raise PreciseRollbackUnavailable("找不到發料紀錄")
    if int(session.get("main_period_id") or 0) != db.get_current_main_period_id():
        raise PreciseRollbackUnavailable("這筆發料屬於舊年度主檔")
    main_path = db.resolve_managed_path(str(db.get_setting("main_file_path") or "").strip(), "main_file_path")
    session_path = str(session.get("main_file_path") or "").strip()
    if session_path:
        session_path = db.resolve_managed_path(session_path, "main_file_path")
        if main_path and Path(session_path) != Path(main_path):
            raise PreciseRollbackUnavailable("目前主檔已更換")
    if not main_path or not Path(main_path).exists():
        raise PreciseRollbackUnavailable("找不到目前主檔")
    if str(order.get("insert_before_code") or "").strip():
        raise PreciseRollbackUnavailable("這筆是插單（有調整其他單的批次碼），請改用整檔還原")
    record = db.get_dispatch_column_record(int(session["id"]))
    if record and (record.get("code_shifts") or str(record.get("insert_before_code") or "").strip()):
        raise PreciseRollbackUnavailable("這筆是插單（有調整其他單的批次碼），請改用整檔還原")
    try:
        changes = read_dispatch_code_changes(str(session.get("backup_path") or ""), [int(order["id"])])
    except Exception:
        changes = []
    if any(changes):
        raise PreciseRollbackUnavailable("這筆發料有調整其他單的批次碼，請改用整檔還原")
    return main_path


def plan_and_apply(ws, session: dict, order: dict) -> dict:
    """在記憶體 workbook 上執行精準反悔；回傳摘要。失敗 raise PreciseRollbackUnavailable。"""
    record = db.get_dispatch_column_record(int(session["id"]))
    legacy = record is None
    if legacy:
        located = _locate_legacy(ws, order, db.get_dispatch_records(int(order["id"])))
    else:
        located = _locate_recorded(ws, order, record)
    _ensure_no_later_edits(session, located, legacy=legacy)

    modes = []
    parts: dict[str, float] = {}
    anchored_parts: set[str] = set()
    removed_cols: list[int] = []
    # 由右往左處理：最右邊的組先刪 / 歸零，前一組才可能變成尾端。
    for group in sorted(located, key=lambda item: item["start_col"], reverse=True):
        start = int(group["start_col"])
        row_deltas = {row: float(info["f"]) - float(info["h"]) for row, info in group["rows"].items()}
        marker = MARKER_PREFIX + json.dumps({
            "session_id": int(session["id"]),
            "order_id": int(order["id"]),
            "code": group.get("code") or norm_header(order.get("code")),
            "po_number": norm_header(ws.cell(1, start + 1).value),
            "rows": [{"part": info["part"], "h": info["h"], "f": info["f"]} for info in group["rows"].values()],
        }, ensure_ascii=False)
        result = neutralize_or_remove_block(
            ws,
            start_col=start,
            width=3,
            row_deltas=row_deltas,
            zero_cols=[start, start + 1],
            clear_cols=[start + 2],
            marked_header=f"{ROLLED_BACK_HEADER_PREFIX} {group.get('code') or norm_header(order.get('code'))}",
            marker_comment=marker,
        )
        modes.append(result["mode"])
        if result["mode"] == "removed":
            removed_cols.append(start)
        for row, info in group["rows"].items():
            parts[info["part"]] = parts.get(info["part"], 0.0) + row_deltas[row]
            if (result["rows"].get(row) or {}).get("anchored"):
                anchored_parts.add(info["part"])
    return {
        "legacy": legacy,
        "mode": "removed" if all(mode == "removed" for mode in modes) else ("zeroed" if all(
            mode == "zeroed" for mode in modes) else "mixed"),
        "group_count": len(located),
        "columns": sorted(int(group["start_col"]) for group in located),
        "removed_columns": sorted(removed_cols),
        "part_deltas": parts,
        "anchored_parts": sorted(anchored_parts),
    }


def _load(main_path: str):
    return openpyxl.load_workbook(main_path, keep_vba=(Path(main_path).suffix.lower() == ".xlsm"))


def evaluate_precise_rollback(session: dict | None) -> dict:
    """唯讀試算：回傳 {"ok": True, ...摘要} 或 {"ok": False, "reason": ...}。"""
    try:
        order = db.get_order(int(session["order_id"])) if session else None
        main_path = ensure_precise_preconditions(session, order)
        workbook = _load(main_path)
        try:
            summary = plan_and_apply(workbook.active, session, order)
        finally:
            workbook.close()
        return {"ok": True, **summary}
    except PreciseRollbackUnavailable as exc:
        return {"ok": False, "reason": exc.reason}
    except Exception as exc:  # 試算失敗就走原本流程
        log.warning("precise rollback evaluation failed: %s", exc)
        return {"ok": False, "reason": f"無法試算精準反悔：{exc}"}
