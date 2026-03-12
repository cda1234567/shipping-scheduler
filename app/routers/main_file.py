from __future__ import annotations
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import FileResponse

from ..config import MAIN_FILE_DIR
from ..services.main_reader import read_stock, read_moq
from .. import database as db

router = APIRouter()


@router.post("/main-file/upload")
async def upload_main_file(file: UploadFile = File(...)):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in {".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(400, "僅支援 xlsx / xls / xlsm")

    dest = MAIN_FILE_DIR / f"main{ext}"
    content = await file.read()
    dest.write_bytes(content)

    stock = read_stock(str(dest))
    moq = read_moq(str(dest))

    db.set_setting("main_file_path", str(dest))
    db.set_setting("main_filename", file.filename or dest.name)
    db.set_setting("main_loaded_at", datetime.now().isoformat())
    db.set_setting("main_part_count", str(len(stock)))

    # 第一次上傳自動建立快照
    existing = db.get_snapshot()
    if not existing:
        db.save_snapshot(stock, moq)
        db.log_activity("snapshot_created", f"起始庫存快照已建立，{len(stock)} 筆料號")

    db.log_activity("main_file_upload", f"{file.filename}, {len(stock)} 筆料號")

    return {"ok": True, "part_count": len(stock), "filename": file.filename}


@router.post("/main-file/snapshot")
async def set_snapshot():
    """手動將目前主檔庫存設為起始快照（截止點）。"""
    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "請先上傳主檔")

    stock = read_stock(main_path)
    moq = read_moq(main_path)
    db.save_snapshot(stock, moq)
    db.log_activity("snapshot_set", f"手動設定起始庫存快照，{len(stock)} 筆料號")
    return {"ok": True, "part_count": len(stock)}


@router.get("/main-file/data")
async def get_main_data():
    """回傳快照庫存 + MOQ。"""
    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(404, "尚未上傳主檔")

    snapshot = db.get_snapshot()
    if snapshot:
        stock = {k: v["stock_qty"] for k, v in snapshot.items()}
        moq = {k: v["moq"] for k, v in snapshot.items()}
    else:
        stock = read_stock(main_path)
        moq = read_moq(main_path)

    return {
        "stock":       stock,
        "moq":         moq,
        "part_count":  int(db.get_setting("main_part_count", "0")),
        "loaded_at":   db.get_setting("main_loaded_at"),
        "filename":    db.get_setting("main_filename") or Path(main_path).name,
        "has_snapshot": bool(snapshot),
    }


@router.get("/main-file/download")
async def download_main_file():
    main_path = db.get_setting("main_file_path")
    if not main_path or not Path(main_path).exists():
        raise HTTPException(404, "尚未上傳主檔")
    filename = db.get_setting("main_filename") or Path(main_path).name
    return FileResponse(main_path, filename=filename)


@router.get("/main-file/info")
async def get_main_info():
    main_path = db.get_setting("main_file_path")
    snapshot = db.get_snapshot()
    return {
        "loaded":       bool(main_path),
        "filename":     db.get_setting("main_filename") or (Path(main_path).name if main_path else ""),
        "part_count":   int(db.get_setting("main_part_count", "0")),
        "loaded_at":    db.get_setting("main_loaded_at"),
        "has_snapshot":  bool(snapshot),
    }
