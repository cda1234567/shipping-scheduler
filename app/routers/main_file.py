from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

from .. import database as db
from ..config import MAIN_FILE_DIR
from ..models import UpdateMoqRequest
from ..services.main_reader import (
    find_legacy_snapshot_stock_fixes,
    read_moq,
    read_stock,
)

router = APIRouter()


def _repair_legacy_snapshot_if_needed(main_path: str, snapshot: dict[str, dict]) -> dict[str, dict]:
    if not snapshot:
        return snapshot

    fixes = find_legacy_snapshot_stock_fixes(main_path, snapshot)
    repaired = db.update_snapshot_stock(fixes)
    if repaired:
        db.log_activity("snapshot_repaired", f"自動修正舊版快照庫存 {repaired} 筆")
        for part, qty in fixes.items():
            if part in snapshot:
                snapshot[part]["stock_qty"] = qty
    return snapshot


@router.post("/main-file/upload")
async def upload_main_file(file: UploadFile = File(...)):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in {".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(400, "僅支援 xlsx / xls / xlsm")

    dest = MAIN_FILE_DIR / f"main{ext}"
    dest.write_bytes(await file.read())

    stock = read_stock(str(dest))
    moq = read_moq(str(dest))

    db.set_setting("main_file_path", str(dest))
    db.set_setting("main_filename", file.filename or dest.name)
    db.set_setting("main_loaded_at", datetime.now().isoformat())
    db.set_setting("main_part_count", str(len(stock)))

    existing = db.get_snapshot()
    if not existing:
        db.save_snapshot(stock, moq)
        db.log_activity("snapshot_created", f"起始庫存快照已建立，{len(stock)} 筆料號")

    db.log_activity("main_file_upload", f"{file.filename}, {len(stock)} 筆料號")
    return {"ok": True, "part_count": len(stock), "filename": file.filename}


@router.post("/main-file/snapshot")
async def set_snapshot():
    """以目前主檔內容重建起始庫存快照。"""
    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先上傳主檔")

    stock = read_stock(main_path)
    moq = read_moq(main_path)
    db.save_snapshot(stock, moq)
    db.log_activity("snapshot_set", f"已重建起始庫存快照，{len(stock)} 筆料號")
    return {"ok": True, "part_count": len(stock)}


@router.get("/main-file/data")
async def get_main_data():
    """取得起始庫存快照與 MOQ。"""
    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(404, "找不到主檔")

    snapshot = db.get_snapshot()
    if snapshot:
        snapshot = _repair_legacy_snapshot_if_needed(main_path, snapshot)
        stock = {k: v["stock_qty"] for k, v in snapshot.items()}
        snapshot_moq = {k: v["moq"] for k, v in snapshot.items()}

        # 舊快照可能沒有完整 MOQ，這裡用最新主檔補齊缺漏鍵。
        moq = read_moq(main_path)
        moq.update(snapshot_moq)
    else:
        stock = read_stock(main_path)
        moq = read_moq(main_path)

    return {
        "stock": stock,
        "moq": moq,
        "part_count": int(db.get_setting("main_part_count", "0")),
        "loaded_at": db.get_setting("main_loaded_at"),
        "filename": db.get_setting("main_filename") or Path(main_path).name,
        "has_snapshot": bool(snapshot),
    }


@router.patch("/main-file/moq")
async def update_snapshot_moq(req: UpdateMoqRequest):
    part_number = str(req.part_number or "").strip().upper()
    if not part_number:
        raise HTTPException(400, "料號不可空白")

    saved_part = db.upsert_snapshot_moq(part_number, req.moq)
    db.log_activity("snapshot_moq_updated", f"{saved_part} MOQ -> {req.moq}")
    return {"ok": True, "part_number": saved_part, "moq": req.moq}


@router.get("/main-file/download")
async def download_main_file():
    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(404, "找不到主檔")
    filename = db.get_setting("main_filename") or Path(main_path).name
    return FileResponse(main_path, filename=filename)


@router.get("/main-file/info")
async def get_main_info():
    main_path = db.get_setting("main_file_path")
    snapshot = db.get_snapshot()
    return {
        "loaded": bool(main_path),
        "filename": db.get_setting("main_filename") or (Path(main_path).name if main_path else ""),
        "part_count": int(db.get_setting("main_part_count", "0")),
        "loaded_at": db.get_setting("main_loaded_at"),
        "has_snapshot": bool(snapshot),
    }
