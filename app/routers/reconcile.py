from __future__ import annotations

import re
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from ..config import DATA_DIR
from ..services.st_reconcile import build_st_reconcile_preview

router = APIRouter()

RECONCILE_UPLOAD_DIR = DATA_DIR / "st_reconcile"
RECONCILE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MAX_RECONCILE_UPLOAD_BYTES = 10 * 1024 * 1024
CUTOFF_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@router.post("/reconcile/st/preview")
async def preview_st_reconcile(
    cutoff_date: str = Form(...),
    file: UploadFile = File(...),
):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in {".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(400, "盤點對帳只支援 xlsx / xls / xlsm")
    cutoff_text = str(cutoff_date or "").strip()
    if not cutoff_text:
        raise HTTPException(400, "請輸入盤點截止日")
    if not CUTOFF_DATE_RE.fullmatch(cutoff_text):
        raise HTTPException(400, "盤點截止日格式需為 YYYY-MM-DD")

    content = await file.read(MAX_RECONCILE_UPLOAD_BYTES + 1)
    if len(content) > MAX_RECONCILE_UPLOAD_BYTES:
        raise HTTPException(400, "盤點檔案超過 10MB，請縮小後再上傳")

    temp_path = RECONCILE_UPLOAD_DIR / f"preview_{uuid4().hex}{ext}"
    try:
        temp_path.write_bytes(content)
        return build_st_reconcile_preview(str(temp_path), cutoff_text)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    except Exception as error:
        raise HTTPException(400, f"盤點對帳試算失敗：{error}") from error
    finally:
        temp_path.unlink(missing_ok=True)
