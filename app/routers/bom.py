from __future__ import annotations
import io
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

import openpyxl
from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from ..config import BOM_DIR, cfg
from ..services.bom_parser import parse_bom
from ..services.xls_reader import open_workbook_any
from .. import database as db

router = APIRouter()


@router.post("/bom/upload")
async def upload_bom_files(files: List[UploadFile] = File(...), group_model: str = Form("")):
    group_model = group_model.strip()
    saved = []
    errors = []

    for uf in files:
        ext = Path(uf.filename or "").suffix.lower()
        if ext not in {".xlsx", ".xls", ".xlsm"}:
            errors.append(f"{uf.filename}: 不支援的格式")
            continue

        content = await uf.read()
        bom_id = str(uuid.uuid4())
        dest = BOM_DIR / f"{bom_id}{ext}"
        dest.write_bytes(content)

        try:
            bom = parse_bom(
                path=str(dest),
                bom_id=bom_id,
                filename=uf.filename or dest.name,
                uploaded_at=datetime.now().isoformat(),
            )
            if group_model:
                bom.group_model = group_model
        except Exception as e:
            dest.unlink(missing_ok=True)
            errors.append(f"{uf.filename}: {e}")
            continue

        bom_dict = {
            "id": bom_id,
            "filename": bom.filename,
            "filepath": str(dest),
            "po_number": bom.po_number,
            "model": bom.model,
            "pcb": bom.pcb,
            "group_model": bom.group_model,
            "order_qty": bom.order_qty,
            "uploaded_at": bom.uploaded_at,
            "components": [c.dict() for c in bom.components],
        }
        db.save_bom_file(bom_dict)

        saved.append({
            "id": bom_id,
            "filename": bom.filename,
            "po_number": bom.po_number,
            "model": bom.model,
            "pcb": bom.pcb,
            "order_qty": bom.order_qty,
            "components": len(bom.components),
            "customer_supplied_count": sum(1 for c in bom.components if c.is_customer_supplied),
        })

    if saved:
        db.log_activity("bom_upload", f"上傳 {len(saved)} 個 BOM" + (f"（{group_model}）" if group_model else ""))

    return {"saved": saved, "errors": errors}


@router.get("/bom/list")
async def list_bom_files():
    """回傳以機種分組的 BOM 清單。"""
    bom_files = db.get_bom_files()

    groups: dict[str, list] = {}
    for b in bom_files:
        m = b["group_model"] or b["model"] or "（未指定機種）"
        if m not in groups:
            groups[m] = []
        comps = db.get_bom_components(b["id"])
        cs_count = sum(1 for c in comps if c.get("is_customer_supplied"))
        groups[m].append({
            "id":          b["id"],
            "filename":    b["filename"],
            "po_number":   b["po_number"],
            "model":       b["model"],
            "pcb":         b["pcb"],
            "order_qty":   b["order_qty"],
            "components":  len(comps),
            "uploaded_at": b["uploaded_at"],
            "customer_supplied_count": cs_count,
        })

    result = [{"model": m, "items": items} for m, items in groups.items()]
    return {"groups": result}


@router.delete("/bom/{bom_id}")
async def delete_bom(bom_id: str):
    bom_files = db.get_bom_files()
    found = next((b for b in bom_files if b["id"] == bom_id), None)
    if not found:
        raise HTTPException(404, "找不到此 BOM")

    Path(found["filepath"]).unlink(missing_ok=True)
    db.delete_bom_file(bom_id)
    db.log_activity("bom_delete", f"刪除 BOM {found['filename']}")
    return {"ok": True}


@router.get("/bom/data")
async def get_bom_data():
    """回傳以 model 為 key 的合併 BOM。"""
    bom_map = db.get_all_bom_components_by_model()
    result = {}
    for model_key, comps in bom_map.items():
        result[model_key] = {"model": model_key, "components": comps}
    return result


@router.get("/bom/{bom_id}/file")
async def get_bom_file(bom_id: str):
    """直接下載/開啟單一 BOM 檔案。"""
    bom_files = db.get_bom_files()
    found = next((b for b in bom_files if b["id"] == bom_id), None)
    if not found:
        raise HTTPException(404, "找不到此 BOM")
    fpath = Path(found["filepath"])
    if not fpath.exists():
        raise HTTPException(404, "BOM 檔案不存在於磁碟")
    return FileResponse(
        path=str(fpath),
        filename=found["filename"] or fpath.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


class BomLookupRequest(BaseModel):
    models: List[str]


@router.post("/bom/lookup")
async def lookup_bom_files(req: BomLookupRequest):
    """依機種名稱查詢 BOM 檔案 metadata（不下載）。"""
    bom_files = db.get_bom_files_by_models(req.models)
    return {
        "files": [
            {
                "id": b["id"],
                "filename": b["filename"],
                "model": b["model"],
                "group_model": b["group_model"],
            }
            for b in bom_files
            if Path(b["filepath"]).exists()
        ]
    }


class BomDownloadRequest(BaseModel):
    models: List[str]


@router.post("/bom/download")
async def download_bom_files(req: BomDownloadRequest):
    """依機種名稱下載對應的 BOM 檔案（多檔打包 zip）。"""
    if not req.models:
        raise HTTPException(400, "請提供機種名稱")

    bom_files = db.get_bom_files_by_models(req.models)
    if not bom_files:
        raise HTTPException(404, "找不到對應的 BOM 檔案")

    # 過濾掉磁碟上不存在的檔案
    valid = [(b, Path(b["filepath"])) for b in bom_files if Path(b["filepath"]).exists()]
    if not valid:
        raise HTTPException(404, "BOM 檔案不存在於磁碟")

    # 單檔直接回傳
    if len(valid) == 1:
        bom, fpath = valid[0]
        return FileResponse(
            path=str(fpath),
            filename=bom["filename"] or fpath.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # 多檔打包 zip
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        seen_names: dict[str, int] = {}
        for bom, fpath in valid:
            name = bom["filename"] or fpath.name
            if name in seen_names:
                seen_names[name] += 1
                stem, ext = Path(name).stem, Path(name).suffix
                name = f"{stem}_{seen_names[name]}{ext}"
            else:
                seen_names[name] = 0
            zf.write(str(fpath), name)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=BOM.zip"},
    )


# ── Dispatch download: write supplements into BOM copies ──────────────────────

class BomDispatchDownloadRequest(BaseModel):
    bom_ids: List[str]
    supplements: Dict[str, float]  # {part_number: qty}


def _write_supplements_to_ws(ws, supplements: dict[str, float]):
    """將補料數量寫入 BOM 工作表的 H 欄。"""
    part_col = cfg("excel.bom_part_col", 2) + 1   # 0-based → 1-based
    h_col = cfg("excel.bom_h_col", 7) + 1         # 0-based → 1-based
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
    """將補料數量寫入 BOM 副本的 H 欄後回傳下載。"""
    if not req.bom_ids:
        raise HTTPException(400, "請提供 BOM ID")

    all_boms = db.get_bom_files()
    id_set = set(req.bom_ids)
    target_boms = [b for b in all_boms if b["id"] in id_set]
    if not target_boms:
        raise HTTPException(404, "找不到指定的 BOM 檔案")

    norm_suppl = {k.strip().upper(): v for k, v in req.supplements.items()}

    output_files: list[tuple[str, io.BytesIO]] = []

    for bf in target_boms:
        src = Path(bf["filepath"])
        if not src.exists():
            continue

        ext = src.suffix.lower()
        out_name = bf["filename"] or src.name

        if ext == ".xls":
            wb = open_workbook_any(str(src))
            out_name = str(Path(out_name).with_suffix(".xlsx"))
        else:
            is_macro = ext == ".xlsm"
            wb = openpyxl.load_workbook(str(src), keep_vba=is_macro)

        _write_supplements_to_ws(wb.active, norm_suppl)

        buf = io.BytesIO()
        wb.save(buf)
        wb.close()
        buf.seek(0)
        output_files.append((out_name, buf))

    if not output_files:
        raise HTTPException(404, "沒有可用的 BOM 檔案")

    # 單檔直接回傳
    if len(output_files) == 1:
        name, buf = output_files[0]
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}"},
        )

    # 多檔打包 zip
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, buf in output_files:
            zf.writestr(name, buf.read())
    zip_buf.seek(0)

    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=BOM.zip"},
    )
