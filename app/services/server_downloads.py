from __future__ import annotations

import shutil
from pathlib import Path

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse

from .. import database as db

SERVER_DOWNLOAD_DIR = Path("/app/user_downloads")


def is_server_download_available() -> bool:
    return SERVER_DOWNLOAD_DIR.is_dir()


def _available_user_download_path(filename: str) -> Path | None:
    if not SERVER_DOWNLOAD_DIR.is_dir():
        return None
    dest = SERVER_DOWNLOAD_DIR / filename
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
    display_path = db.get_setting("server_download_display_path") or ""
    return JSONResponse({"ok": True, "filename": saved_name, "directory": display_path})


def maybe_server_save_response(
    request: Request,
    file_path: str,
    filename: str,
    media_type: str,
) -> FileResponse | JSONResponse:
    server_save = request.query_params.get("server_save") == "1"
    if server_save and is_server_download_available() and db.get_setting("server_download_enabled") == "1":
        saved_name = save_to_user_downloads(file_path, filename)
        if saved_name:
            return _server_save_json(saved_name)
    return FileResponse(file_path, filename=filename, media_type=media_type)


def maybe_server_save_bytes_response(
    request: Request,
    content: bytes,
    filename: str,
) -> JSONResponse | None:
    server_save = request.query_params.get("server_save") == "1"
    if server_save and is_server_download_available() and db.get_setting("server_download_enabled") == "1":
        saved_name = save_bytes_to_user_downloads(content, filename)
        if saved_name:
            return _server_save_json(saved_name)
    return None
