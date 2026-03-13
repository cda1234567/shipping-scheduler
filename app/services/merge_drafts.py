from __future__ import annotations

import io
import re
import shutil
import zipfile
from pathlib import Path
from urllib.parse import quote

import openpyxl
from fastapi import HTTPException
from fastapi.responses import FileResponse, StreamingResponse

from .. import database as db
from ..config import MERGE_DRAFT_DIR, cfg
from .download_names import append_minute_timestamp, build_generated_filename
from .main_reader import read_moq, read_stock
from ..models import calc_suggested_qty


def normalize_part_key(value) -> str:
    return str(value or "").strip().upper()


def _sanitize_filename_piece(value: str, fallback: str = "draft") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    text = re.sub(r'[\\/:*?"<>|]+', "-", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    return text or fallback


def _normalize_decisions(decisions: dict[str, str] | None = None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for part_number, decision in (decisions or {}).items():
        key = normalize_part_key(part_number)
        if key and decision:
            normalized[key] = str(decision)
    return normalized


def _normalize_supplements(supplements: dict[str, float] | None = None) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for part_number, qty in (supplements or {}).items():
        key = normalize_part_key(part_number)
        try:
            amount = float(qty or 0)
        except (TypeError, ValueError):
            amount = 0.0
        if key and amount > 0:
            normalized[key] = amount
    return normalized


def _get_main_signature(main_path: str) -> tuple[str, str]:
    path = Path(main_path)
    if not path.exists():
        raise HTTPException(400, "找不到主檔，無法建立副檔")
    return str(path), str(path.stat().st_mtime_ns)


def _load_effective_moq(main_path: str) -> dict[str, float]:
    live = {
        normalize_part_key(part): float(qty or 0)
        for part, qty in read_moq(main_path).items()
        if normalize_part_key(part)
    }
    snapshot = db.get_snapshot()
    for part, row in snapshot.items():
        key = normalize_part_key(part)
        if not key:
            continue
        live[key] = float((row or {}).get("moq") or 0)
    return live


def _build_running_stock(main_path: str) -> dict[str, float]:
    return {
        normalize_part_key(part): float(qty or 0)
        for part, qty in read_stock(main_path).items()
        if normalize_part_key(part)
    }


def _resolve_cell_for_write(ws, row_idx: int, col_idx: int):
    cell = ws.cell(row=row_idx, column=col_idx)
    for merged_range in ws.merged_cells.ranges:
        if cell.coordinate in merged_range:
            return ws.cell(row=merged_range.min_row, column=merged_range.min_col)
    return cell


def _set_cell_value(ws, row_idx: int, col_idx: int, value):
    _resolve_cell_for_write(ws, row_idx, col_idx).value = value


def _format_bom_po_value(existing_value, po_number):
    po_text = str(po_number or "").strip()
    if not po_text:
        return existing_value

    existing_text = str(existing_value or "").strip()
    if any(token in existing_text for token in ("製單號碼", "M/O", "PO")):
        if ":" in existing_text:
            prefix = existing_text.split(":", 1)[0]
            return f"{prefix}:{po_text}"
        return f"{existing_text}{po_text}"
    return po_text


def _write_bom_header_values(ws, po_number):
    po_col = cfg("excel.bom_po_col", 7) + 1
    po_cell = _resolve_cell_for_write(ws, 1, po_col)
    po_cell.value = _format_bom_po_value(po_cell.value, po_number)


def _write_dispatch_values_to_ws(ws, supplements: dict[str, float], carry_overs: dict[str, float]):
    part_col = cfg("excel.bom_part_col", 2) + 1
    g_col = cfg("excel.bom_g_col", 6) + 1
    h_col = cfg("excel.bom_h_col", 7) + 1
    data_start = cfg("excel.bom_data_start_row", 5)
    dash_markers = {"-", "x", "X", "n", "N", "n/a", "N/A", "na", "NA", "?"}
    supplemented_parts: set[str] = set()

    for row_idx in range(data_start, ws.max_row + 1):
        part = normalize_part_key(ws.cell(row=row_idx, column=part_col).value)
        if not part:
            continue

        g_text = str(ws.cell(row=row_idx, column=g_col).value or "").strip()
        h_text = str(ws.cell(row=row_idx, column=h_col).value or "").strip()
        if g_text in dash_markers or h_text in dash_markers:
            continue

        if part in carry_overs:
            _set_cell_value(ws, row_idx, g_col, carry_overs[part])

        supplement_qty = 0
        if part not in supplemented_parts and part in supplements:
            supplement_qty = supplements[part]
            supplemented_parts.add(part)
        _set_cell_value(ws, row_idx, h_col, supplement_qty)


def _cleanup_draft_files(draft_id: int):
    for item in db.get_merge_draft_files(draft_id):
        Path(str(item.get("filepath") or "")).unlink(missing_ok=True)
    draft_dir = MERGE_DRAFT_DIR / f"draft_{draft_id}"
    if draft_dir.exists():
        shutil.rmtree(draft_dir, ignore_errors=True)


def _build_download_response(file_entries: list[dict], archive_label: str = "副檔草稿"):
    if not file_entries:
        raise HTTPException(404, "副檔檔案不存在")

    if len(file_entries) == 1:
        entry = file_entries[0]
        return FileResponse(
            path=str(entry["path"]),
            filename=entry["download_name"],
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    zip_buffer = io.BytesIO()
    used_names: set[str] = set()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for entry in file_entries:
            target_name = entry["download_name"]
            stem = Path(target_name).stem
            suffix = Path(target_name).suffix or entry["path"].suffix
            counter = 1
            while target_name in used_names:
                target_name = f"{stem}_{counter}{suffix}"
                counter += 1
            used_names.add(target_name)
            zf.write(str(entry["path"]), target_name)
    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(build_generated_filename(archive_label, '.zip'))}"},
    )


def _plan_order_draft(order: dict, draft: dict, bom_files: list[dict], running_stock: dict[str, float], moq_map: dict[str, float]) -> dict:
    decisions = _normalize_decisions(draft.get("decisions"))
    remaining_supplements = _normalize_supplements(draft.get("supplements"))
    file_plans: list[dict] = []
    shortages: list[dict] = []

    for bom in bom_files:
        components = db.get_bom_components(str(bom["id"]))
        if not components:
            continue

        carry_overs: dict[str, float] = {}
        supplement_allocations: dict[str, float] = {}
        part_totals: dict[str, dict[str, float]] = {}

        for component in components:
            needed_qty = float(component.get("needed_qty") or 0)
            if component.get("is_dash") or needed_qty <= 0:
                continue

            part = normalize_part_key(component.get("part_number"))
            if not part:
                continue

            if part not in carry_overs:
                carry_overs[part] = float(running_stock.get(part, 0))

            summary = part_totals.setdefault(part, {
                "part_number": str(component.get("part_number") or ""),
                "description": str(component.get("description") or ""),
                "needed_qty": 0.0,
                "prev_qty_cs": 0.0,
            })
            summary["needed_qty"] += needed_qty
            summary["prev_qty_cs"] += float(component.get("prev_qty_cs") or 0)

        for part, summary in part_totals.items():
            current_stock = float(running_stock.get(part, 0))
            prev_qty_cs = float(summary.get("prev_qty_cs") or 0)
            needed_qty = float(summary.get("needed_qty") or 0)
            decision = decisions.get(part, "None")

            available_before = current_stock + prev_qty_cs
            shortage_before = max(0.0, needed_qty - available_before)
            supplement_qty = 0.0
            if decision != "Shortage" and shortage_before > 0 and remaining_supplements.get(part, 0) > 0:
                supplement_qty = float(remaining_supplements.get(part, 0))
                remaining_supplements[part] = 0.0
                supplement_allocations[part] = supplement_qty

            available_after_supply = available_before + supplement_qty
            if decision == "Shortage":
                ending_stock = available_after_supply
                shortage_after = shortage_before
            else:
                ending_stock = available_after_supply - needed_qty
                shortage_after = max(0.0, needed_qty - available_after_supply)

            running_stock[part] = ending_stock

            if shortage_after > 0:
                moq = float(moq_map.get(part, 0) or 0)
                shortages.append({
                    "part_number": summary.get("part_number") or part,
                    "description": summary.get("description") or "",
                    "current_stock": float(carry_overs.get(part, current_stock)),
                    "needed": needed_qty,
                    "shortage_amount": shortage_after,
                    "moq": moq,
                    "suggested_qty": calc_suggested_qty(shortage_after, moq),
                    "decision": decision,
                    "supplement_qty": supplement_qty,
                    "_row_code": order.get("code") or order.get("model") or "",
                    "_row_model": order.get("model") or "",
                })

        file_plans.append({
            "bom_file_id": str(bom["id"]),
            "source_filename": str(bom.get("filename") or ""),
            "source_format": str(bom.get("source_format") or Path(str(bom.get("filename") or "")).suffix.lower()),
            "model": str(bom.get("model") or ""),
            "group_model": str(bom.get("group_model") or ""),
            "po_number": str(order.get("po_number") or bom.get("po_number") or ""),
            "carry_overs": carry_overs,
            "supplements": supplement_allocations,
        })

    return {
        "running_stock": running_stock,
        "file_plans": file_plans,
        "shortages": shortages,
    }


def _write_draft_files(draft_id: int, file_plans: list[dict]) -> list[dict]:
    draft_dir = MERGE_DRAFT_DIR / f"draft_{draft_id}"
    draft_dir.mkdir(parents=True, exist_ok=True)
    written: list[dict] = []

    for index, plan in enumerate(file_plans, start=1):
        bom = db.get_bom_file(plan["bom_file_id"])
        if not bom:
            continue

        source_path = Path(str(bom.get("filepath") or ""))
        if not source_path.exists():
            continue

        display_name = append_minute_timestamp(bom.get("filename") or source_path.name)
        internal_name = f"{index:02d}_{display_name}"
        output_path = draft_dir / internal_name

        ext = source_path.suffix.lower()
        workbook = openpyxl.load_workbook(str(source_path), keep_vba=(ext == ".xlsm"))
        try:
            sheet = workbook.active
            _write_bom_header_values(sheet, plan.get("po_number", ""))
            _write_dispatch_values_to_ws(sheet, plan.get("supplements") or {}, plan.get("carry_overs") or {})
            workbook.save(output_path)
        finally:
            workbook.close()

        written.append({
            "bom_file_id": plan["bom_file_id"],
            "filename": display_name,
            "filepath": str(output_path),
            "source_filename": plan.get("source_filename", ""),
            "source_format": plan.get("source_format", ""),
            "model": plan.get("model", ""),
            "group_model": plan.get("group_model", ""),
            "carry_overs": plan.get("carry_overs") or {},
            "supplements": plan.get("supplements") or {},
        })

    return written


def rebuild_merge_drafts(order_ids: list[int], overrides: dict[int, dict] | None = None) -> list[dict]:
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return []

    main_path = str(db.get_setting("main_file_path") or "").strip()
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先載入主檔，才能建立副檔")

    main_loaded_at = str(db.get_setting("main_loaded_at") or "")
    main_path, main_mtime_ns = _get_main_signature(main_path)
    normalized_overrides: dict[int, dict] = {}
    for order_id, payload in (overrides or {}).items():
        if not (isinstance(order_id, int) or str(order_id).strip().isdigit()):
            continue
        normalized_overrides[int(order_id)] = {
            "decisions": _normalize_decisions((payload or {}).get("decisions")),
            "supplements": _normalize_supplements((payload or {}).get("supplements")),
        }

    for raw_order_id in normalized_ids:
        order = db.get_order(raw_order_id)
        if not order:
            continue
        existing = db.get_active_merge_draft_for_order(raw_order_id) or {}
        payload = normalized_overrides.get(raw_order_id, {})
        decisions = payload.get("decisions", existing.get("decisions", {}))
        supplements = payload.get("supplements", existing.get("supplements", {}))
        db.replace_merge_draft(
            order_id=raw_order_id,
            main_file_path=main_path,
            main_file_mtime_ns=main_mtime_ns,
            main_loaded_at=main_loaded_at,
            decisions=decisions,
            supplements=supplements,
            shortages=existing.get("shortages", []),
        )

    active_drafts = db.get_active_merge_drafts()
    running_stock = _build_running_stock(main_path)
    moq_map = _load_effective_moq(main_path)
    refreshed_ids: set[int] = set()

    for draft in active_drafts:
        draft_id = int(draft["id"])
        order = db.get_order(int(draft["order_id"]))
        if not order or order.get("status") not in ("pending", "merged"):
            _cleanup_draft_files(draft_id)
            db.delete_merge_draft(draft_id)
            continue

        bom_files = db.get_bom_files_by_models([str(order.get("model") or "")])
        plan = _plan_order_draft(order, draft, bom_files, running_stock, moq_map)
        db.replace_merge_draft(
            order_id=int(order["id"]),
            main_file_path=main_path,
            main_file_mtime_ns=main_mtime_ns,
            main_loaded_at=main_loaded_at,
            decisions=draft.get("decisions", {}),
            supplements=draft.get("supplements", {}),
            shortages=plan["shortages"],
        )
        refreshed = db.get_active_merge_draft_for_order(int(order["id"]))
        if not refreshed:
            continue
        _cleanup_draft_files(int(refreshed["id"]))
        written = _write_draft_files(int(refreshed["id"]), plan["file_plans"])
        db.replace_merge_draft_files(int(refreshed["id"]), written)
        refreshed_ids.add(int(refreshed["id"]))

    return [draft for draft in db.get_active_merge_drafts() if int(draft["id"]) in refreshed_ids]


def delete_merge_draft_and_refresh(draft_id: int):
    draft = db.get_merge_draft(draft_id)
    if not draft or draft.get("status") != "active":
        raise HTTPException(404, "找不到副檔草稿")
    _cleanup_draft_files(draft_id)
    db.delete_merge_draft(draft_id)
    remaining = [item["order_id"] for item in db.get_active_merge_drafts()]
    if remaining:
        rebuild_merge_drafts(remaining)


def get_schedule_draft_map() -> dict[int, dict]:
    drafts = db.get_active_merge_drafts()
    result: dict[int, dict] = {}
    for draft in drafts:
        result[int(draft["order_id"])] = {
            "id": int(draft["id"]),
            "order_id": int(draft["order_id"]),
            "status": draft.get("status", "active"),
            "model": draft.get("model", ""),
            "po_number": draft.get("po_number", ""),
            "main_loaded_at": draft.get("main_loaded_at", ""),
            "updated_at": draft.get("updated_at", ""),
            "supplements": draft.get("supplements", {}),
            "decisions": draft.get("decisions", {}),
            "shortages": draft.get("shortages", []),
            "files": [
                {
                    "id": int(file_item["id"]),
                    "bom_file_id": file_item.get("bom_file_id", ""),
                    "filename": file_item.get("filename", ""),
                    "filepath": file_item.get("filepath", ""),
                    "source_filename": file_item.get("source_filename", ""),
                }
                for file_item in draft.get("files", [])
            ],
        }
    return result


def get_draft_detail(draft_id: int) -> dict:
    draft = db.get_merge_draft(draft_id)
    if not draft or draft.get("status") != "active":
        raise HTTPException(404, "找不到副檔草稿")
    order = db.get_order(int(draft["order_id"]))
    files = db.get_merge_draft_files(draft_id)
    return {
        "draft": {
            "id": int(draft["id"]),
            "order_id": int(draft["order_id"]),
            "status": draft.get("status", "active"),
            "main_loaded_at": draft.get("main_loaded_at", ""),
            "updated_at": draft.get("updated_at", ""),
            "supplements": draft.get("supplements", {}),
            "decisions": draft.get("decisions", {}),
            "shortages": draft.get("shortages", []),
            "files": files,
        },
        "order": order or {},
    }


def download_merge_draft(draft_id: int):
    draft = db.get_merge_draft(draft_id)
    if not draft or draft.get("status") != "active":
        raise HTTPException(404, "找不到副檔草稿")
    files = db.get_merge_draft_files(draft_id)
    valid_files = [
        {
            "path": file_path,
            "download_name": item.get("filename") or file_path.name,
        }
        for item in files
        for file_path in [Path(str(item.get("filepath") or ""))]
        if file_path.exists()
    ]
    return _build_download_response(valid_files)


def download_selected_merge_drafts(order_ids: list[int]):
    normalized_ids: list[int] = []
    for order_id in order_ids or []:
        try:
            normalized_ids.append(int(order_id))
        except (TypeError, ValueError):
            continue
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        raise HTTPException(400, "請先選擇要下載副檔的訂單")

    draft_id_map = db.get_active_merge_draft_ids_by_order_ids(normalized_ids)
    missing_orders = [order_id for order_id in normalized_ids if order_id not in draft_id_map]
    if missing_orders:
        raise HTTPException(404, "部分訂單尚未建立副檔")

    file_entries: list[dict] = []
    for order_id in normalized_ids:
        order = db.get_order(order_id) or {}
        po_prefix = _sanitize_filename_piece(order.get("po_number", ""), "PO")
        model_prefix = _sanitize_filename_piece(order.get("model", ""), "MODEL")
        draft_files = db.get_merge_draft_files(int(draft_id_map[order_id]))
        for item in draft_files:
            file_path = Path(str(item.get("filepath") or ""))
            if not file_path.exists():
                continue
            original_name = item.get("filename") or file_path.name
            download_name = f"{po_prefix}_{model_prefix}_{original_name}"
            file_entries.append({
                "path": file_path,
                "download_name": download_name,
            })

    return _build_download_response(file_entries)
