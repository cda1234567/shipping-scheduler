from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import STATIC_DIR
from app.database import init_db
from app.routers import alerts, analytics, bom, defectives, dispatch, logs, main_file, schedule, system
from app.services.edit_auth import EDIT_AUTH_REQUIRED_MESSAGE, get_edit_auth_status, request_requires_edit_auth
from app.services.db_backup import database_backup_scheduler
from app.services.backup_cleanup import cleanup_old_backups
from app.services.main_preview import clean_main_preview_disk_cache
from app.snapshot_sync import refresh_snapshot_from_main
from app.version_info import APP_VERSION


NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        for header, value in NO_CACHE_HEADERS.items():
            response.headers[header] = value
        return response


# 啟動前先初始化資料庫
init_db()


def _sync_snapshot_on_startup():
    """Server 啟動時，把快照同步到當前主檔庫存。"""
    from app.database import get_setting
    main_path = str(get_setting("main_file_path") or "").strip()
    if main_path:
        count = refresh_snapshot_from_main(main_path)
        if count:
            print(f"[startup] 快照已同步，共 {count} 筆料號")
        try:
            clean_main_preview_disk_cache(os.stat(main_path).st_mtime_ns)
        except OSError:
            pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not os.environ.get("PYTEST_CURRENT_TEST"):
        _sync_snapshot_on_startup()
        cleanup_old_backups()
        database_backup_scheduler.start()
    try:
        yield
    finally:
        database_backup_scheduler.stop()


app = FastAPI(title="出貨排程系統", version=APP_VERSION, lifespan=lifespan)

# Gzip 大 JSON 回應（主檔預覽等 4MB+ 回應會壓到 ~10% 大小）
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)


@app.middleware("http")
async def edit_auth_guard(request: Request, call_next):
    if not os.environ.get("PYTEST_CURRENT_TEST") and request_requires_edit_auth(request):
        session = get_edit_auth_status(request)
        if not session.authenticated:
            return JSONResponse(
                status_code=403,
                content={
                    "detail": EDIT_AUTH_REQUIRED_MESSAGE,
                    "code": "edit_auth_required",
                },
            )
    return await call_next(request)

app.include_router(main_file.router, prefix="/api")
app.include_router(schedule.router, prefix="/api")
app.include_router(bom.router, prefix="/api")
app.include_router(dispatch.router, prefix="/api")
app.include_router(alerts.router, prefix="/api")
app.include_router(logs.router, prefix="/api")
app.include_router(system.router, prefix="/api")
app.include_router(analytics.router, prefix="/api")
app.include_router(defectives.router, prefix="/api")

app.mount("/static", NoCacheStaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/api/health")
async def health():
    return {"ok": True, "version": APP_VERSION}


@app.get("/")
async def root():
    return FileResponse(str(STATIC_DIR / "index.html"), headers=NO_CACHE_HEADERS)
