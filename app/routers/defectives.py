"""不良品 / 加工多打扣帳 API。"""
from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File

from .. import database as db
from ..config import BACKUP_DIR
from ..models import (
    DefectiveImportConfirmRequest,
    OverrunDeductionRequest,
    OverrunImportConfirmRequest,
)
from ..services.defective_deduction import (
    parse_defective_excel,
    deduct_defectives_from_main,
    reverse_defectives_from_main,
)
from ..services.inventory_restore_guard import ensure_defective_batch_delete_allowed
from ..services.merge_drafts import rebuild_merge_drafts
from ..services.overrun_deduction import (
    apply_overrun_import_confirmations,
    build_overrun_import_preview,
    build_model_overrun_plan,
    parse_overrun_detail_excel,
    preview_deductions_against_main,
)
from ..snapshot_sync import refresh_snapshot_from_main

router = APIRouter(prefix="/defectives", tags=["defectives"])
log = logging.getLogger(__name__)
OVERRUN_DEDUCT_HEADER = "加工多打扣帳"
OVERRUN_REVERSE_HEADER = "加工多打回復"


def _get_main_file_mtime() -> float:
    """取得目前主檔的修改時間（mtime），用來判斷主檔是否被更換。"""
    main_path = str(db.get_setting("main_file_path") or "").strip()
    if main_path and Path(main_path).exists():
        return os.path.getmtime(main_path)
    return 0


def _require_main_path() -> str:
    main_path = str(db.get_setting("main_file_path") or "").strip()
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "主檔尚未上傳，無法扣帳")
    return main_path


def _refresh_active_merge_drafts_after_main_change():
    active_drafts = db.get_active_merge_drafts()
    if not active_drafts:
        return
    order_ids = [int(item.get("order_id") or 0) for item in active_drafts if int(item.get("order_id") or 0) > 0]
    if order_ids:
        try:
            rebuild_merge_drafts(order_ids)
        except Exception as exc:
            log.warning("refresh active merge drafts skipped after defective mutation: %s", exc)


def _detect_batch_type(batch: dict) -> str:
    items = batch.get("items") or []
    if any(str(item.get("action_taken") or "").strip() == "加工多打扣帳" for item in items):
        return "overrun"
    if str(batch.get("filename") or "").startswith("加工多打｜"):
        return "overrun"
    return "defective"


def _decorate_batch(batch: dict) -> dict:
    data = dict(batch)
    batch_type = _detect_batch_type(data)
    data["batch_type"] = batch_type
    data["can_add_file"] = batch_type == "defective"
    return data


def _format_overrun_batch_name(model: str, extra_pcs: float) -> str:
    return f"加工多打｜{model}｜+{extra_pcs:g} pcs"


def _format_overrun_batch_note(req: OverrunDeductionRequest, plan: dict) -> str:
    lines = [
        "類型：加工多打扣帳",
        f"機種：{plan.get('requested_model') or plan.get('model') or req.model}",
        f"多打：{float(plan.get('extra_pcs') or req.extra_pcs):g} pcs",
    ]
    matched_models = [str(item).strip() for item in (plan.get("matched_models") or []) if str(item).strip()]
    if matched_models:
        lines.append(f"BOM 對應：{'、'.join(matched_models)}")
    if req.reason.strip():
        lines.append(f"原因：{req.reason.strip()}")
    if req.note.strip():
        lines.append(f"備註：{req.note.strip()}")
    if req.reported_by.strip():
        lines.append(f"登錄人：{req.reported_by.strip()}")
    return "\n".join(lines)


def _format_overrun_file_batch_name(filename: str) -> str:
    return f"加工多打明細｜{filename}"


def _format_overrun_file_batch_note(filename: str, parsed: dict) -> str:
    lines = [
        "類型：加工多打扣帳",
        f"來源檔案：{filename}",
    ]
    if str(parsed.get("title") or "").strip():
        lines.append(f"明細標題：{str(parsed.get('title') or '').strip()}")
    if str(parsed.get("mo_info") or "").strip():
        lines.append(f"M/O：{str(parsed.get('mo_info') or '').strip()}")
    return "\n".join(lines)


def _append_import_resolution_summary(note: str, applied: dict) -> str:
    lines = [str(note or "").strip()] if str(note or "").strip() else []
    replaced_items = applied.get("replaced_items") or []
    skipped_items = applied.get("skipped_items") or []
    if replaced_items:
        lines.append("改正料號：")
        for item in replaced_items[:12]:
            lines.append(
                f"第 {int(item.get('source_row') or 0)} 列："
                f"{item.get('source_part_number', '')} -> {item.get('target_part_number', '')}"
            )
        hidden = len(replaced_items) - min(len(replaced_items), 12)
        if hidden > 0:
            lines.append(f"...另有 {hidden} 筆改正")
    if skipped_items:
        lines.append("不扣項目：")
        for item in skipped_items[:12]:
            lines.append(
                f"第 {int(item.get('source_row') or 0)} 列："
                f"{item.get('part_number', '')} / {float(item.get('defective_qty') or 0):g}"
            )
        hidden = len(skipped_items) - min(len(skipped_items), 12)
        if hidden > 0:
            lines.append(f"...另有 {hidden} 筆不扣")
    return "\n".join(lines)


def _get_defective_batch(batch_id: int) -> dict | None:
    return next((batch for batch in db.get_defective_batches() if int(batch.get("id") or 0) == int(batch_id)), None)


def _parse_upload_to_items(file: UploadFile, parser, empty_message: str) -> tuple[str, list[dict]]:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in (".xlsx", ".xls", ".xlsm"):
        raise HTTPException(400, "僅支援 .xlsx / .xls / .xlsm")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(file.file.read())
        tmp_path = tmp.name

    try:
        items = parser(tmp_path)
    except Exception as exc:
        Path(tmp_path).unlink(missing_ok=True)
        raise HTTPException(400, f"解析失敗：{exc}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    if not items:
        raise HTTPException(400, empty_message)

    return file.filename or "unknown", items


def _preview_defective_items(main_path: str, filename: str, items: list[dict], batch_id: int | None = None) -> dict:
    preview = build_overrun_import_preview(main_path, {
        "source_filename": filename,
        "items": items,
    })
    if batch_id:
        preview["batch_id"] = int(batch_id)
    return preview


def _finalize_defective_import(
    *,
    main_path: str,
    source_filename: str,
    final_items: list[dict],
    applied: dict,
    batch_id: int | None = None,
) -> dict:
    target_batch_id = int(batch_id or 0)
    batch_note = _append_import_resolution_summary("", applied)
    if target_batch_id:
        target_batch = _get_defective_batch(target_batch_id)
        if not target_batch:
            raise HTTPException(404, "找不到要追加的不良品批次")
        if _detect_batch_type(target_batch) != "defective":
            raise HTTPException(400, "加工多打批次不可追加副檔")

    result = deduct_defectives_from_main(
        main_path,
        final_items,
        backup_dir=str(BACKUP_DIR),
    )
    refresh_snapshot_from_main(main_path)

    if not target_batch_id:
        target_batch_id = db.create_defective_batch(
            source_filename or "unknown",
            note=batch_note,
            main_file_mtime=_get_main_file_mtime(),
        )

    result_map = {r["part_number"]: r for r in (result.get("results") or [])}
    created_ids: list[int] = []
    for item in final_items:
        part = str(item.get("part_number") or "").strip().upper()
        if part in (result.get("skipped_parts") or []):
            continue
        matched = result_map.get(part, {})
        record_id = db.create_defective_record({
            "batch_id": target_batch_id,
            "part_number": part,
            "description": item.get("description", ""),
            "defective_qty": item.get("defective_qty", 0),
            "stock_before": matched.get("stock_before", 0),
            "stock_after": matched.get("stock_after", 0),
            "status": "confirmed",
        })
        created_ids.append(record_id)

    return {
        "batch_id": target_batch_id,
        "deducted_count": result["deducted_count"],
        "skipped_parts": result["skipped_parts"],
        "results": result["results"],
        "created_ids": created_ids,
        "replaced_count": len(applied.get("replaced_items") or []),
        "skipped_count": len(applied.get("skipped_items") or []),
    }


@router.get("/batches")
async def list_batches():
    """取得所有匯入批次（含明細）。"""
    batches = [_decorate_batch(batch) for batch in db.get_defective_batches()]
    return {"batches": batches}


@router.post("/import-preview")
async def preview_defectives(file: UploadFile = File(...)):
    """匯入副檔格式的不良品 Excel 前先預覽，確認主檔抓不到的料號。"""
    if not file.filename:
        raise HTTPException(400, "請選擇檔案")

    filename, items = _parse_upload_to_items(
        file,
        parse_defective_excel,
        "檔案中沒有有效的不良品資料（需要料號 + 數量 > 0）",
    )
    main_path = _require_main_path()
    preview = _preview_defective_items(main_path, filename, items)
    return {"ok": True, **preview}


@router.post("/batches/{batch_id}/add-preview")
async def preview_add_defectives(batch_id: int, file: UploadFile = File(...)):
    """追加不良品前先預覽，沿用同一個確認 modal。"""
    target_batch = _get_defective_batch(batch_id)
    if not target_batch:
        raise HTTPException(404, "找不到批次")
    if _detect_batch_type(target_batch) != "defective":
        raise HTTPException(400, "加工多打批次不可追加副檔")

    filename, items = _parse_upload_to_items(file, parse_defective_excel, "沒有有效的不良品資料")
    main_path = _require_main_path()
    preview = _preview_defective_items(main_path, filename, items, batch_id=batch_id)
    return {"ok": True, **preview}


@router.post("/import-confirm")
async def confirm_defectives(req: DefectiveImportConfirmRequest):
    """確認不良品 preview 後，正式扣帳並建立/追加批次。"""
    if not req.items:
        raise HTTPException(400, "沒有可扣帳的不良品資料")

    main_path = _require_main_path()
    applied = apply_overrun_import_confirmations(
        main_path,
        [item.dict() for item in req.items],
    )
    unresolved_items = applied.get("unresolved_items") or []
    if unresolved_items:
        raise HTTPException(400, "仍有抓不到的料號尚未處理，請先選擇不扣或改正料號")

    final_items = applied.get("final_items") or []
    if not final_items:
        raise HTTPException(400, "這次全部都選擇不扣，沒有可扣帳的料號")

    result = _finalize_defective_import(
        main_path=main_path,
        source_filename=req.source_filename or "unknown",
        final_items=final_items,
        applied=applied,
        batch_id=req.batch_id,
    )

    if req.batch_id:
        action_label = "追加不良品"
        action_detail = f"批次#{result['batch_id']}：扣帳 {result['deducted_count']} 筆"
    else:
        action_label = "匯入不良品"
        action_detail = f"{req.source_filename or 'unknown'}：扣帳 {result['deducted_count']} 筆"
    if result["replaced_count"]:
        action_detail += f"，改正 {result['replaced_count']} 筆"
    if result["skipped_count"]:
        action_detail += f"，不扣 {result['skipped_count']} 筆"

    db.log_activity(action_label, action_detail)

    return {
        "ok": True,
        **result,
    }


@router.post("/import")
async def import_defectives(file: UploadFile = File(...)):
    """匯入副檔格式的不良品 Excel → 自動扣主檔庫存 + 建立批次紀錄。"""
    if not file.filename:
        raise HTTPException(400, "請選擇檔案")

    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".xlsx", ".xls", ".xlsm"):
        raise HTTPException(400, "僅支援 .xlsx / .xls / .xlsm")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        items = parse_defective_excel(tmp_path)
    except Exception as e:
        Path(tmp_path).unlink(missing_ok=True)
        raise HTTPException(400, f"解析失敗：{e}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    if not items:
        raise HTTPException(400, "檔案中沒有有效的不良品資料（需要料號 + 數量 > 0）")

    main_path = _require_main_path()

    result = deduct_defectives_from_main(
        main_path, items, backup_dir=str(BACKUP_DIR),
    )
    refresh_snapshot_from_main(main_path)
    _refresh_active_merge_drafts_after_main_change()

    # 記錄扣帳當下的主檔 mtime
    mtime = _get_main_file_mtime()
    batch_id = db.create_defective_batch(
        file.filename or "unknown", main_file_mtime=mtime,
    )

    # 建立紀錄（含扣帳前後庫存）
    result_map = {r["part_number"]: r for r in (result.get("results") or [])}
    created_ids: list[int] = []
    for item in items:
        part = item["part_number"]
        if part in (result.get("skipped_parts") or []):
            continue
        matched = result_map.get(part, {})
        record_id = db.create_defective_record({
            "batch_id": batch_id,
            "part_number": part,
            "description": item.get("description", ""),
            "defective_qty": item["defective_qty"],
            "stock_before": matched.get("stock_before", 0),
            "stock_after": matched.get("stock_after", 0),
            "status": "confirmed",
        })
        created_ids.append(record_id)

    db.log_activity(
        "匯入不良品",
        f"{file.filename}：扣帳 {result['deducted_count']} 筆"
        + (f"，略過 {len(result['skipped_parts'])} 筆" if result["skipped_parts"] else ""),
    )

    return {
        "ok": True,
        "batch_id": batch_id,
        "deducted_count": result["deducted_count"],
        "skipped_parts": result["skipped_parts"],
        "results": result["results"],
        "created_ids": created_ids,
    }


@router.post("/batches/{batch_id}/add")
async def add_item_to_batch(batch_id: int, file: UploadFile = File(...)):
    """對已存在的批次追加不良品項目（解析 Excel + 扣主檔）。"""
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in (".xlsx", ".xls", ".xlsm"):
        raise HTTPException(400, "僅支援 .xlsx / .xls / .xlsm")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        items = parse_defective_excel(tmp_path)
    except Exception as e:
        Path(tmp_path).unlink(missing_ok=True)
        raise HTTPException(400, f"解析失敗：{e}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    if not items:
        raise HTTPException(400, "沒有有效的不良品資料")

    main_path = _require_main_path()

    result = deduct_defectives_from_main(
        main_path, items, backup_dir=str(BACKUP_DIR),
    )
    refresh_snapshot_from_main(main_path)
    _refresh_active_merge_drafts_after_main_change()

    result_map = {r["part_number"]: r for r in (result.get("results") or [])}
    for item in items:
        part = item["part_number"]
        if part in (result.get("skipped_parts") or []):
            continue
        matched = result_map.get(part, {})
        db.create_defective_record({
            "batch_id": batch_id,
            "part_number": part,
            "description": item.get("description", ""),
            "defective_qty": item["defective_qty"],
            "stock_before": matched.get("stock_before", 0),
            "stock_after": matched.get("stock_after", 0),
            "status": "confirmed",
        })

    db.log_activity("追加不良品", f"批次#{batch_id}：{result['deducted_count']} 筆")

    return {
        "ok": True,
        "deducted_count": result["deducted_count"],
        "skipped_parts": result["skipped_parts"],
        "results": result["results"],
    }


@router.delete("/records/{record_id}")
async def delete_record(record_id: int):
    if not db.delete_defective_record(record_id):
        raise HTTPException(404, "找不到紀錄")
    db.log_activity("刪除不良品", f"ID={record_id}")
    return {"ok": True}


@router.post("/overrun/preview")
async def preview_model_overrun(req: OverrunDeductionRequest):
    """預覽加工多打扣帳，不實際寫主檔。"""
    main_path = _require_main_path()
    try:
        plan = build_model_overrun_plan(req.model, req.extra_pcs)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except LookupError as exc:
        raise HTTPException(404, str(exc))

    preview = preview_deductions_against_main(main_path, plan["items"])
    return {
        "ok": True,
        "model": plan["model"],
        "requested_model": plan["requested_model"],
        "extra_pcs": plan["extra_pcs"],
        "matched_models": plan["matched_models"],
        "matched_boms": plan["matched_boms"],
        "item_count": len(plan["items"]),
        **preview,
    }


@router.post("/overrun")
async def create_model_overrun(req: OverrunDeductionRequest):
    """依機種與多打 pcs 建立一筆可回復的加工多打扣帳批次。"""
    main_path = _require_main_path()
    try:
        plan = build_model_overrun_plan(req.model, req.extra_pcs)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except LookupError as exc:
        raise HTTPException(404, str(exc))

    result = deduct_defectives_from_main(
        main_path,
        plan["items"],
        backup_dir=str(BACKUP_DIR),
        entry_header=OVERRUN_DEDUCT_HEADER,
    )
    refresh_snapshot_from_main(main_path)
    _refresh_active_merge_drafts_after_main_change()

    if int(result.get("deducted_count") or 0) <= 0:
        skipped_parts = result.get("skipped_parts") or []
        if skipped_parts:
            raise HTTPException(400, f"主檔找不到對應料號，無法扣帳：{'、'.join(skipped_parts[:8])}")
        raise HTTPException(400, "沒有可扣帳的料號")

    batch_id = db.create_defective_batch(
        _format_overrun_batch_name(plan.get("requested_model") or plan["model"], plan["extra_pcs"]),
        note=_format_overrun_batch_note(req, plan),
        main_file_mtime=_get_main_file_mtime(),
    )

    result_map = {r["part_number"]: r for r in (result.get("results") or [])}
    created_ids: list[int] = []
    for item in plan["items"]:
        part = str(item.get("part_number") or "").strip().upper()
        if part in (result.get("skipped_parts") or []):
            continue
        matched = result_map.get(part, {})
        record_id = db.create_defective_record({
            "batch_id": batch_id,
            "part_number": part,
            "description": item.get("description", ""),
            "defective_qty": item.get("defective_qty", 0),
            "stock_before": matched.get("stock_before", 0),
            "stock_after": matched.get("stock_after", 0),
            "action_taken": "加工多打扣帳",
            "action_note": _format_overrun_batch_note(req, plan),
            "status": "confirmed",
            "reported_by": req.reported_by.strip(),
        })
        created_ids.append(record_id)

    db.log_activity(
        "加工多打扣帳",
        f"{plan.get('requested_model') or plan['model']}：多打 {plan['extra_pcs']:g} pcs，扣帳 {result['deducted_count']} 筆"
        + (f"，略過 {len(result['skipped_parts'])} 筆" if result["skipped_parts"] else ""),
    )

    return {
        "ok": True,
        "batch_id": batch_id,
        "batch_type": "overrun",
        "deducted_count": result["deducted_count"],
        "skipped_parts": result["skipped_parts"],
        "results": result["results"],
        "created_ids": created_ids,
        "model": plan["model"],
        "requested_model": plan["requested_model"],
        "extra_pcs": plan["extra_pcs"],
    }


@router.post("/overrun/import-preview")
async def preview_overrun_detail_import(file: UploadFile = File(...)):
    """匯入加工多打明細前先預覽，抓出主檔找不到的料號。"""
    if not file.filename:
        raise HTTPException(400, "請選擇檔案")

    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".xlsx", ".xls", ".xlsm"):
        raise HTTPException(400, "僅支援 .xlsx / .xls / .xlsm")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        parsed = parse_overrun_detail_excel(tmp_path)
    except Exception as exc:
        Path(tmp_path).unlink(missing_ok=True)
        raise HTTPException(400, f"解析失敗：{exc}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    main_path = _require_main_path()
    parsed["source_filename"] = file.filename
    preview = build_overrun_import_preview(main_path, parsed)
    return {"ok": True, **preview}


@router.post("/overrun/import-confirm")
async def confirm_overrun_detail_import(req: OverrunImportConfirmRequest):
    """確認加工多打明細缺漏料號處理後，正式扣帳。"""
    if not req.items:
        raise HTTPException(400, "沒有可扣帳的料號資料")

    main_path = _require_main_path()
    applied = apply_overrun_import_confirmations(
        main_path,
        [item.dict() for item in req.items],
    )
    unresolved_items = applied.get("unresolved_items") or []
    if unresolved_items:
        raise HTTPException(400, "仍有抓不到的料號尚未處理，請先選擇不扣或改正料號")

    final_items = applied.get("final_items") or []
    if not final_items:
        raise HTTPException(400, "這次全部都選擇不扣，沒有可扣帳的料號")

    result = deduct_defectives_from_main(
        main_path,
        final_items,
        backup_dir=str(BACKUP_DIR),
        entry_header=OVERRUN_DEDUCT_HEADER,
    )
    refresh_snapshot_from_main(main_path)
    _refresh_active_merge_drafts_after_main_change()

    batch_note = _append_import_resolution_summary(
        _format_overrun_file_batch_note(req.source_filename, {
            "title": req.title,
            "mo_info": req.mo_info,
        }),
        applied,
    )
    batch_id = db.create_defective_batch(
        _format_overrun_file_batch_name(req.source_filename or "明細匯入"),
        note=batch_note,
        main_file_mtime=_get_main_file_mtime(),
    )

    result_map = {r["part_number"]: r for r in (result.get("results") or [])}
    created_ids: list[int] = []
    replaced_by_target = {
        str(item.get("target_part_number") or "").strip().upper(): item
        for item in (applied.get("replaced_items") or [])
        if str(item.get("target_part_number") or "").strip()
    }
    for item in final_items:
        part = str(item.get("part_number") or "").strip().upper()
        matched = result_map.get(part, {})
        replaced = replaced_by_target.get(part)
        action_note = batch_note
        if replaced:
            action_note += (
                f"\n本列改正：第 {int(replaced.get('source_row') or 0)} 列 "
                f"{replaced.get('source_part_number', '')} -> {part}"
            )
        record_id = db.create_defective_record({
            "batch_id": batch_id,
            "part_number": part,
            "description": item.get("description", ""),
            "defective_qty": item.get("defective_qty", 0),
            "stock_before": matched.get("stock_before", 0),
            "stock_after": matched.get("stock_after", 0),
            "action_taken": "加工多打扣帳",
            "action_note": action_note,
            "status": "confirmed",
        })
        created_ids.append(record_id)

    db.log_activity(
        "確認加工多打明細",
        f"{req.source_filename or '明細匯入'}：扣帳 {result['deducted_count']} 筆"
        + (
            f"，改正 {len(applied.get('replaced_items') or [])} 筆"
            if applied.get("replaced_items")
            else ""
        )
        + (
            f"，不扣 {len(applied.get('skipped_items') or [])} 筆"
            if applied.get("skipped_items")
            else ""
        ),
    )

    return {
        "ok": True,
        "batch_id": batch_id,
        "batch_type": "overrun",
        "deducted_count": result["deducted_count"],
        "skipped_parts": result["skipped_parts"],
        "results": result["results"],
        "created_ids": created_ids,
        "replaced_count": len(applied.get("replaced_items") or []),
        "skipped_count": len(applied.get("skipped_items") or []),
    }


@router.post("/overrun/import")
async def import_overrun_detail(file: UploadFile = File(...)):
    """匯入加工廠提供的多打扣帳明細，直接依料號數量扣主檔。"""
    if not file.filename:
        raise HTTPException(400, "請選擇檔案")

    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".xlsx", ".xls", ".xlsm"):
        raise HTTPException(400, "僅支援 .xlsx / .xls / .xlsm")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        parsed = parse_overrun_detail_excel(tmp_path)
    except Exception as exc:
        Path(tmp_path).unlink(missing_ok=True)
        raise HTTPException(400, f"解析失敗：{exc}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    main_path = _require_main_path()
    result = deduct_defectives_from_main(
        main_path,
        parsed["items"],
        backup_dir=str(BACKUP_DIR),
        entry_header=OVERRUN_DEDUCT_HEADER,
    )
    refresh_snapshot_from_main(main_path)
    _refresh_active_merge_drafts_after_main_change()

    if int(result.get("deducted_count") or 0) <= 0:
        skipped_parts = result.get("skipped_parts") or []
        if skipped_parts:
            raise HTTPException(400, f"主檔找不到對應料號，無法扣帳：{'、'.join(skipped_parts[:8])}")
        raise HTTPException(400, "沒有可扣帳的料號")

    batch_note = _format_overrun_file_batch_note(file.filename, parsed)
    batch_id = db.create_defective_batch(
        _format_overrun_file_batch_name(file.filename),
        note=batch_note,
        main_file_mtime=_get_main_file_mtime(),
    )

    result_map = {r["part_number"]: r for r in (result.get("results") or [])}
    created_ids: list[int] = []
    for item in parsed["items"]:
        part = str(item.get("part_number") or "").strip().upper()
        if part in (result.get("skipped_parts") or []):
            continue
        matched = result_map.get(part, {})
        record_id = db.create_defective_record({
            "batch_id": batch_id,
            "part_number": part,
            "description": item.get("description", ""),
            "defective_qty": item.get("defective_qty", 0),
            "stock_before": matched.get("stock_before", 0),
            "stock_after": matched.get("stock_after", 0),
            "action_taken": "加工多打扣帳",
            "action_note": batch_note,
            "status": "confirmed",
        })
        created_ids.append(record_id)

    db.log_activity(
        "匯入加工多打明細",
        f"{file.filename}：扣帳 {result['deducted_count']} 筆"
        + (f"，略過 {len(result['skipped_parts'])} 筆" if result["skipped_parts"] else ""),
    )

    return {
        "ok": True,
        "batch_id": batch_id,
        "batch_type": "overrun",
        "deducted_count": result["deducted_count"],
        "skipped_parts": result["skipped_parts"],
        "results": result["results"],
        "created_ids": created_ids,
        "title": parsed.get("title", ""),
        "mo_info": parsed.get("mo_info", ""),
    }


@router.delete("/batches/{batch_id}")
async def delete_batch(batch_id: int):
    # 先取出該批次的所有紀錄，用來回寫主檔
    batches = db.get_defective_batches()
    target_batch = next((b for b in batches if b["id"] == batch_id), None)
    if not target_batch:
        raise HTTPException(404, "找不到批次")
    ensure_defective_batch_delete_allowed(target_batch)

    records = target_batch.get("items") or []
    reverse_items = [
        {"part_number": r["part_number"], "defective_qty": r["defective_qty"]}
        for r in records
        if r.get("defective_qty") and r["defective_qty"] > 0
    ]

    # 比對主檔 mtime — 如果主檔已被更換就不回寫
    reversed_count = 0
    main_file_changed = False
    batch_mtime = float(target_batch.get("main_file_mtime") or 0)
    current_mtime = _get_main_file_mtime()

    if reverse_items:
        main_path = str(db.get_setting("main_file_path") or "").strip()
        if not main_path or not Path(main_path).exists():
            main_file_changed = True
        elif batch_mtime > 0 and abs(current_mtime - batch_mtime) > 1:
            # mtime 差距超過 1 秒 → 主檔已被更換
            main_file_changed = True
        else:
            reverse_header = OVERRUN_REVERSE_HEADER if _detect_batch_type(target_batch) == "overrun" else "不良品回復"
            result = reverse_defectives_from_main(
                main_path,
                reverse_items,
                backup_dir=str(BACKUP_DIR),
                entry_header=reverse_header,
            )
            reversed_count = result["reversed_count"]
            refresh_snapshot_from_main(main_path)
            _refresh_active_merge_drafts_after_main_change()

    # 刪除 DB 紀錄
    if not db.delete_defective_batch(batch_id):
        raise HTTPException(404, "刪除失敗")

    decorated_batch = _decorate_batch(target_batch)
    batch_name = decorated_batch.get("filename", f"#{batch_id}")
    batch_label = "加工多打批次" if decorated_batch.get("batch_type") == "overrun" else "不良品批次"
    if main_file_changed:
        detail = f"{batch_name}：主檔已更換，僅刪除紀錄（未回寫庫存）"
    else:
        detail = f"{batch_name}：已回復 {reversed_count} 筆庫存"
    db.log_activity(f"刪除{batch_label}", detail)

    return {
        "ok": True,
        "reversed_count": reversed_count,
        "main_file_changed": main_file_changed,
    }
