from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import STATIC_DIR
from app.database import init_db
from app.routers import alerts, analytics, bom, defectives, dispatch, logs, main_file, schedule, system
from app.services.db_backup import database_backup_scheduler
from app.services.merge_drafts import cleanup_expired_committed_merge_drafts
from app.services.backup_cleanup import cleanup_old_backups
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


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not os.environ.get("PYTEST_CURRENT_TEST"):
        _sync_snapshot_on_startup()
        cleanup_expired_committed_merge_drafts()
        cleanup_old_backups()
        database_backup_scheduler.start()
    try:
        yield
    finally:
        database_backup_scheduler.stop()


app = FastAPI(title="OpenText 出貨排程系統", version=APP_VERSION, lifespan=lifespan)

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
