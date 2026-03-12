from __future__ import annotations
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.config import STATIC_DIR
from app.database import init_db
from app.routers import main_file, schedule, bom, dispatch, alerts, logs


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

# 啟動時初始化 DB
init_db()

app = FastAPI(title="OpenText 出貨排程系統")

app.include_router(main_file.router, prefix="/api")
app.include_router(schedule.router,  prefix="/api")
app.include_router(bom.router,       prefix="/api")
app.include_router(dispatch.router,  prefix="/api")
app.include_router(alerts.router,    prefix="/api")
app.include_router(logs.router,      prefix="/api")

app.mount("/static", NoCacheStaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def root():
    return FileResponse(str(STATIC_DIR / "index.html"), headers=NO_CACHE_HEADERS)
