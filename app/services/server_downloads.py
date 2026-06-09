from __future__ import annotations

import shutil
import re
from pathlib import Path

from fastapi import HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

from .. import database as db

SERVER_DOWNLOAD_DIR = Path("/app/user_downloads")


def _safe_download_filename(filename: str) -> str:
    raw = str(filename or "").strip()
    name = re.split(r"[\\/]+", raw)[-1]
    if not name or name in {".", ".."}:
        name = "download"
    name = re.sub(r'[:*?"<>|\x00-\x1f]+', "_", name).strip(" .")
    return name or "download"


def is_server_download_available() -> bool:
    return SERVER_DOWNLOAD_DIR.is_dir()


def _available_user_download_path(filename: str) -> Path | None:
    if not SERVER_DOWNLOAD_DIR.is_dir():
        return None
    safe_name = _safe_download_filename(filename)
    dest = SERVER_DOWNLOAD_DIR / safe_name
    if dest.exists():
        stem = dest.stem
        suffix = dest.suffix
        counter = 1
        while dest.exists():
            dest = SERVER_DOWNLOAD_DIR / f"{stem} ({counter}){suffix}"
            counter += 1
    return dest


def save_to_user_downloads(source_path: str, filename: str) -> str | None:
    dest = _available_user_download_path(filename)
    if dest is None:
        return None
    shutil.copy2(source_path, str(dest))
    return dest.name


def save_bytes_to_user_downloads(content: bytes, filename: str) -> str | None:
    dest = _available_user_download_path(filename)
    if dest is None:
        return None
    dest.write_bytes(content)
    return dest.name


def _server_save_json(saved_name: str) -> JSONResponse:
    display_path = db.get_setting("server_download_display_path") or "D:\\Download\\excel"
    return JSONResponse({"ok": True, "filename": saved_name, "directory": display_path})


def _server_save_requested(request: Request) -> bool:
    return request.query_params.get("server_save") == "1"


def _ensure_server_save_available():
    if db.get_setting("server_download_enabled") != "1":
        raise HTTPException(400, "伺服器下載資料夾未啟用，請先到下載設定開啟。")
    if not is_server_download_available():
        display_path = db.get_setting("server_download_display_path") or "D:\\Download\\excel"
        raise HTTPException(500, f"伺服器下載資料夾不可用，請確認 {display_path} 已掛載且可寫入。")


def maybe_server_save_response(
    request: Request,
    file_path: str,
    filename: str,
    media_type: str,
) -> FileResponse | JSONResponse:
    if _server_save_requested(request):
        _ensure_server_save_available()
        saved_name = save_to_user_downloads(file_path, filename)
        if saved_name:
            return _server_save_json(saved_name)
        raise HTTPException(500, "檔案已產生，但寫入伺服器下載資料夾失敗。")
    return FileResponse(file_path, filename=filename, media_type=media_type)


def maybe_server_save_bytes_response(
    request: Request,
    content: bytes,
    filename: str,
) -> JSONResponse | None:
    if _server_save_requested(request):
        _ensure_server_save_available()
        saved_name = save_bytes_to_user_downloads(content, filename)
        if saved_name:
            return _server_save_json(saved_name)
        raise HTTPException(500, "檔案已產生，但寫入伺服器下載資料夾失敗。")
    return None
