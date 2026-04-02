from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Request, Response, UploadFile

from .. import database as db
from ..config import ST_INVENTORY_DIR
from ..models import DatabaseBackupRestoreRequest, DatabaseBackupSettingsRequest, EditAuthLoginRequest
from ..services.edit_auth import (
    EDIT_AUTH_REQUIRED_MESSAGE,
    apply_edit_auth_cookie,
    clear_edit_auth_cookie,
    get_edit_auth_status,
    verify_edit_password,
)
from ..services import db_backup
from ..services.local_time import local_now
from ..services.st_inventory import parse_st_inventory_file
from ..version_info import get_app_meta

router = APIRouter()


@router.get("/system/app-meta")
async def get_system_app_meta():
    return get_app_meta()


@router.get("/system/edit-auth/status")
async def get_system_edit_auth_status(request: Request):
    session = get_edit_auth_status(request)
    return {
        "authenticated": session.authenticated,
        "readonly": not session.authenticated,
        "expires_at": session.expires_at,
    }


@router.post("/system/edit-auth/login")
async def login_system_edit_auth(req: EditAuthLoginRequest, request: Request, response: Response):
    if not verify_edit_password(req.password):
        raise HTTPException(401, "登入失敗，密碼不正確。")
    expires_at = apply_edit_auth_cookie(response, request)
    return {
        "ok": True,
        "authenticated": True,
        "readonly": False,
        "expires_at": expires_at,
    }


@router.post("/system/edit-auth/logout")
async def logout_system_edit_auth(response: Response):
    clear_edit_auth_cookie(response)
    return {
        "ok": True,
        "authenticated": False,
        "readonly": True,
        "detail": EDIT_AUTH_REQUIRED_MESSAGE,
    }


@router.post("/system/st-inventory/upload")
async def upload_st_inventory(file: UploadFile = File(...)):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in {".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(400, "ST 庫存只支援 xlsx / xls / xlsm")

    dest = ST_INVENTORY_DIR / f"st_inventory{ext}"
    dest.write_bytes(await file.read())

    try:
        parsed = parse_st_inventory_file(str(dest))
    except Exception as error:
        dest.unlink(missing_ok=True)
        raise HTTPException(400, f"ST 庫存解析失敗：{error}") from error

    db.save_st_inventory_snapshot(parsed["stock"], parsed["descriptions"])
    loaded_at = local_now().isoformat(timespec="seconds")
    db.set_setting("st_inventory_file_path", str(dest))
    db.set_setting("st_inventory_filename", file.filename or dest.name)
    db.set_setting("st_inventory_loaded_at", loaded_at)
    db.set_setting("st_inventory_part_count", str(parsed["part_count"]))
    db.log_activity("st_inventory_upload", f"{file.filename}, {parsed['part_count']} 筆")

    return {
        "ok": True,
        "filename": file.filename or dest.name,
        "part_count": parsed["part_count"],
        "sheet_name": parsed["sheet_name"],
        "loaded_at": loaded_at,
    }


@router.get("/system/st-inventory/info")
async def get_st_inventory_info():
    file_path = str(db.get_setting("st_inventory_file_path") or "").strip()
    filename = str(db.get_setting("st_inventory_filename") or "").strip()
    return {
        "loaded": bool(filename and db.get_st_inventory_snapshot()),
        "filename": filename or (Path(file_path).name if file_path else ""),
        "part_count": int(db.get_setting("st_inventory_part_count", "0")),
        "loaded_at": db.get_setting("st_inventory_loaded_at"),
    }


@router.get("/system/st-inventory/data")
async def get_st_inventory_data():
    snapshot = db.get_st_inventory_snapshot()
    return {
        "stock": {part: float(item.get("stock_qty") or 0) for part, item in snapshot.items()},
        "descriptions": {part: str(item.get("description") or "") for part, item in snapshot.items()},
        "part_count": len(snapshot),
        "loaded_at": db.get_setting("st_inventory_loaded_at"),
        "filename": db.get_setting("st_inventory_filename"),
    }


@router.get("/system/db-backups")
async def get_database_backups():
    return db_backup.get_database_backup_overview()


@router.put("/system/db-backups/settings")
async def update_database_backup_settings(req: DatabaseBackupSettingsRequest):
    settings = db_backup.update_database_backup_settings(
        enabled=req.enabled,
        hour=req.hour,
        minute=req.minute,
        keep_count=req.keep_count,
    )
    return {"ok": True, **settings}


@router.post("/system/db-backups/run")
async def run_database_backup():
    try:
        backup = db_backup.create_database_backup(reason="manual")
    except Exception as error:
        raise HTTPException(500, f"建立資料庫備份失敗：{error}") from error
    return {"ok": True, "backup": backup}


@router.post("/system/db-backups/restore")
async def restore_database_backup(req: DatabaseBackupRestoreRequest):
    try:
        result = db_backup.restore_database_backup(req.backup_name)
    except FileNotFoundError as error:
        raise HTTPException(404, str(error)) from error
    except Exception as error:
        raise HTTPException(500, f"還原資料庫備份失敗：{error}") from error
    return {"ok": True, "requires_reload": True, **result}
