from __future__ import annotations

import io
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote
from uuid import uuid4

import openpyxl
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from .. import database as db
from ..config import BOM_DIR, cfg
from ..models import BomEditorSaveRequest
from ..services.bom_editor import (
    apply_bom_editor_changes,
    backup_bom_file,
    build_bom_storage_payload,
    normalize_bom_record_to_editable,
    parse_bom_for_storage,
    prepare_uploaded_bom_file,
)
from ..services.xls_reader import open_workbook_any

router = APIRouter()


def _get_required_bom(bom_id: str) -> dict:
    bom = db.get_bom_file(bom_id)
    if not bom:
        raise HTTPException(404, "找不到 BOM")
    return bom


def _ensure_editable_bom_record(bom: dict) -> dict:
    normalized = normalize_bom_record_to_editable(bom)
    if normalized == bom:
        return normalized

    parsed = parse_bom_for_storage(
        path=normalized["filepath"],
        bom_id=normalized["id"],
        filename=normalized["filename"],
        uploaded_at=normalized["uploaded_at"],
        group_model=normalized.get("group_model", ""),
        source_filename=normalized.get("source_filename", ""),
        source_format=normalized.get("source_format", ""),
        is_converted=bool(normalized.get("is_converted")),
    )
    db.save_bom_file(build_bom_storage_payload(parsed))
    db.log_activity("bom_convert", f"{bom.get('filename') or bom['id']} 已轉為可編輯 xlsx")
    return db.get_bom_file(bom["id"]) or normalized


@router.post("/bom/upload")
async def upload_bom_files(files: List[UploadFile] = File(...), group_model: str = Form("")):
    group_model = group_model.strip()
    saved = []
    errors = []

    for uf in files:
        ext = Path(uf.filename or "").suffix.lower()
        if ext not in {".xlsx", ".xls", ".xlsm"}:
            errors.append(f"{uf.filename}: 僅支援 xlsx / xls / xlsm")
            continue

        bom_id = uuid4().hex
        stored = None
        try:
            stored = prepare_uploaded_bom_file(
                bom_id=bom_id,
                upload_name=uf.filename or f"{bom_id}{ext}",
                content=await uf.read(),
            )
            parsed = parse_bom_for_storage(
                path=str(stored["filepath"]),
                bom_id=bom_id,
                filename=str(stored["filename"]),
                uploaded_at=datetime.now().isoformat(),
                group_model=group_model,
                source_filename=str(stored["source_filename"]),
                source_format=str(stored["source_format"]),
                is_converted=bool(stored["is_converted"]),
            )
            db.save_bom_file(build_bom_storage_payload(parsed))
            saved.append({
                "id": parsed.id,
                "filename": parsed.filename,
                "source_filename": parsed.source_filename,
                "source_format": parsed.source_format,
                "is_converted": parsed.is_converted,
                "po_number": parsed.po_number,
                "model": parsed.model,
                "pcb": parsed.pcb,
                "order_qty": parsed.order_qty,
                "components": len(parsed.components),
                "customer_supplied_count": sum(1 for c in parsed.components if c.is_customer_supplied),
            })
        except Exception as exc:
            if stored and stored.get("filepath"):
                Path(str(stored["filepath"])).unlink(missing_ok=True)
            for suffix in (".xlsx", ".xls", ".xlsm"):
                (BOM_DIR / f"{bom_id}{suffix}").unlink(missing_ok=True)
            errors.append(f"{uf.filename}: {exc}")
            continue

    if saved:
        converted_count = sum(1 for item in saved if item["is_converted"])
        detail = f"上傳 {len(saved)} 份 BOM"
        if group_model:
            detail += f"（group_model: {group_model}）"
        if converted_count:
            detail += f"，其中 {converted_count} 份 xls 已轉為 xlsx"
        db.log_activity("bom_upload", detail)

    return {"saved": saved, "errors": errors}


@router.get("/bom/list")
async def list_bom_files():
    bom_files = db.get_bom_files()

    groups: dict[str, list] = {}
    for bom in bom_files:
        model_key = bom["group_model"] or bom["model"] or "未指定機種"
        groups.setdefault(model_key, [])
        components = db.get_bom_components(bom["id"])
        cs_count = sum(1 for comp in components if comp.get("is_customer_supplied"))
        groups[model_key].append({
            "id": bom["id"],
            "filename": bom["filename"],
            "source_filename": bom.get("source_filename") or bom["filename"],
            "source_format": bom.get("source_format") or Path(bom["filename"]).suffix.lower(),
            "is_converted": bool(bom.get("is_converted")),
            "po_number": bom["po_number"],
            "model": bom["model"],
            "pcb": bom["pcb"],
            "order_qty": bom["order_qty"],
            "components": len(components),
            "uploaded_at": bom["uploaded_at"],
            "customer_supplied_count": cs_count,
        })

    return {"groups": [{"model": model, "items": items} for model, items in groups.items()]}


@router.delete("/bom/{bom_id}")
async def delete_bom(bom_id: str):
    bom = _get_required_bom(bom_id)
    Path(bom["filepath"]).unlink(missing_ok=True)
    db.delete_bom_file(bom_id)
    db.log_activity("bom_delete", f"刪除 BOM {bom['filename']}")
    return {"ok": True}


@router.get("/bom/data")
async def get_bom_data():
    bom_map = db.get_all_bom_components_by_model()
    return {model: {"model": model, "components": components} for model, components in bom_map.items()}


@router.get("/bom/{bom_id}/file")
async def get_bom_file(bom_id: str):
    bom = _get_required_bom(bom_id)
    file_path = Path(bom["filepath"])
    if not file_path.exists():
        raise HTTPException(404, "BOM 檔案不存在")
    return FileResponse(
        path=str(file_path),
        filename=bom["filename"] or file_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


class BomLookupRequest(BaseModel):
    models: List[str]


@router.post("/bom/lookup")
async def lookup_bom_files(req: BomLookupRequest):
    bom_files = db.get_bom_files_by_models(req.models)
    return {
        "files": [
            {
                "id": bom["id"],
                "filename": bom["filename"],
                "model": bom["model"],
                "group_model": bom["group_model"],
            }
            for bom in bom_files
            if Path(bom["filepath"]).exists()
        ]
    }


class BomDownloadRequest(BaseModel):
    models: List[str]


@router.post("/bom/download")
async def download_bom_files(req: BomDownloadRequest):
    if not req.models:
        raise HTTPException(400, "請提供要下載的機種")

    bom_files = db.get_bom_files_by_models(req.models)
    if not bom_files:
        raise HTTPException(404, "找不到對應的 BOM")

    valid = [(bom, Path(bom["filepath"])) for bom in bom_files if Path(bom["filepath"]).exists()]
    if not valid:
        raise HTTPException(404, "BOM 檔案不存在")

    if len(valid) == 1:
        bom, file_path = valid[0]
        return FileResponse(
            path=str(file_path),
            filename=bom["filename"] or file_path.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        seen_names: dict[str, int] = {}
        for bom, file_path in valid:
            name = bom["filename"] or file_path.name
            seen_names[name] = seen_names.get(name, -1) + 1
            if seen_names[name] > 0:
                stem, suffix = Path(name).stem, Path(name).suffix
                name = f"{stem}_{seen_names[name]}{suffix}"
            zf.write(str(file_path), name)
    zip_buffer.seek(0)

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=BOM.zip"},
    )


@router.get("/bom/{bom_id}/editor")
async def get_bom_editor(bom_id: str):
    bom = _ensure_editable_bom_record(_get_required_bom(bom_id))
    parsed = parse_bom_for_storage(
        path=bom["filepath"],
        bom_id=bom["id"],
        filename=bom["filename"],
        uploaded_at=bom["uploaded_at"],
        group_model=bom.get("group_model", ""),
        source_filename=bom.get("source_filename", ""),
        source_format=bom.get("source_format", ""),
        is_converted=bool(bom.get("is_converted")),
    )
    payload = build_bom_storage_payload(parsed)
    payload["component_count"] = len(parsed.components)
    return payload


@router.put("/bom/{bom_id}/editor")
async def save_bom_editor(bom_id: str, req: BomEditorSaveRequest):
    bom = _ensure_editable_bom_record(_get_required_bom(bom_id))
    file_path = Path(bom["filepath"])
    if not file_path.exists():
        raise HTTPException(404, "BOM 檔案不存在")

    backup_path = backup_bom_file(str(file_path))
    try:
        apply_bom_editor_changes(str(file_path), req)
        parsed = parse_bom_for_storage(
            path=str(file_path),
            bom_id=bom["id"],
            filename=bom["filename"],
            uploaded_at=bom["uploaded_at"],
            group_model=req.group_model.strip(),
            source_filename=bom.get("source_filename", ""),
            source_format=bom.get("source_format", ""),
            is_converted=bool(bom.get("is_converted")),
        )
        db.save_bom_file(build_bom_storage_payload(parsed))
    except Exception as exc:
        shutil.copy2(backup_path, file_path)
        raise HTTPException(400, f"儲存 BOM 失敗：{exc}")

    db.log_activity(
        "bom_edit",
        f"更新 BOM {parsed.filename}，{len(parsed.components)} 列元件已同步回正式檔",
    )
    return {
        "ok": True,
        "filename": parsed.filename,
        "components": len(parsed.components),
        "backup_path": backup_path,
    }


class BomDispatchDownloadRequest(BaseModel):
    bom_ids: List[str]
    supplements: Dict[str, float]


def _write_supplements_to_ws(ws, supplements: dict[str, float]):
    part_col = cfg("excel.bom_part_col", 2) + 1
    h_col = cfg("excel.bom_h_col", 7) + 1
    data_start = cfg("excel.bom_data_start_row", 5)

    written = 0
    for row_idx in range(data_start, ws.max_row + 1):
        part = str(ws.cell(row=row_idx, column=part_col).value or "").strip().upper()
        if part and part in supplements:
            ws.cell(row=row_idx, column=h_col).value = supplements[part]
            written += 1
    return written


@router.post("/bom/dispatch-download")
async def dispatch_download_bom(req: BomDispatchDownloadRequest):
    if not req.bom_ids:
        raise HTTPException(400, "請提供 BOM ID")

    target_ids = set(req.bom_ids)
    target_boms = [bom for bom in db.get_bom_files() if bom["id"] in target_ids]
    if not target_boms:
        raise HTTPException(404, "找不到指定的 BOM")

    supplements = {part.strip().upper(): qty for part, qty in req.supplements.items()}
    output_files: list[tuple[str, io.BytesIO]] = []

    for bom in target_boms:
        src = Path(bom["filepath"])
        if not src.exists():
            continue

        ext = src.suffix.lower()
        output_name = bom["filename"] or src.name
        if ext == ".xls":
            wb = open_workbook_any(str(src))
            output_name = str(Path(output_name).with_suffix(".xlsx"))
        else:
            wb = openpyxl.load_workbook(str(src), keep_vba=(ext == ".xlsm"))

        _write_supplements_to_ws(wb.active, supplements)
        buffer = io.BytesIO()
        wb.save(buffer)
        wb.close()
        buffer.seek(0)
        output_files.append((output_name, buffer))

    if not output_files:
        raise HTTPException(404, "找不到可下載的 BOM 檔案")

    if len(output_files) == 1:
        name, buffer = output_files[0]
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}"},
        )

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, buffer in output_files:
            zf.writestr(name, buffer.read())
    zip_buffer.seek(0)

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=BOM.zip"},
    )
