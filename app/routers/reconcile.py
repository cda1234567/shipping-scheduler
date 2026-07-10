from __future__ import annotations

import re
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from .. import database as db
from ..config import DATA_DIR
from ..services.st_reconcile import (
    build_st_reconcile_preview,
    commit_st_reconcile_stop_loss,
    resolve_cutoff_batch,
)

router = APIRouter()

RECONCILE_UPLOAD_DIR = DATA_DIR / "st_reconcile"
RECONCILE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MAX_RECONCILE_UPLOAD_BYTES = 10 * 1024 * 1024
CUTOFF_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _format_batch_label(option: dict) -> str:
    stamp = str(option.get("dispatched_at") or "").replace("T", " ")[:16]
    return f"{option.get('code')}（{stamp} 發料）" if stamp else str(option.get("code") or "")


def _resolve_cutoff(cutoff_date: str | None, cutoff_batch_code: str | None) -> tuple[str, str]:
    """回傳 (cutoff_text, batch_label)。批次優先；否則沿用日期。"""
    batch = str(cutoff_batch_code or "").strip()
    if batch:
        try:
            option = resolve_cutoff_batch(batch)
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        return str(option.get("cutoff_at") or option["dispatched_at"]), str(option["code"])
    text = str(cutoff_date or "").strip()
    if not text:
        raise HTTPException(400, "請選擇停損批次或輸入盤點截止日")
    if not CUTOFF_DATE_RE.fullmatch(text):
        raise HTTPException(400, "盤點截止日格式需為 YYYY-MM-DD")
    return text, ""


@router.get("/reconcile/st/cutoff-options")
async def get_st_reconcile_cutoff_options():
    options = db.get_st_reconcile_cutoff_batch_options()
    return {
        "options": [
            {
                "code": option["code"],
                "dispatched_at": option["dispatched_at"],
                "label": _format_batch_label(option),
            }
            for option in options
        ]
    }


def _normalize_part_numbers(values: list[str] | None) -> list[str] | None:
    if values is None:
        return None
    normalized = [
        part
        for value in values
        for part in [str(value or "").strip().upper()]
        if part and part != "[]"
    ]
    return list(dict.fromkeys(normalized))


@router.post("/reconcile/st/preview")
async def preview_st_reconcile(
    cutoff_date: str | None = Form(None),
    cutoff_batch_code: str | None = Form(None),
    file: UploadFile = File(...),
):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in {".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(400, "盤點對帳只支援 xlsx / xls / xlsm")
    cutoff_text, _ = _resolve_cutoff(cutoff_date, cutoff_batch_code)

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


@router.post("/reconcile/st/commit")
async def commit_st_reconcile(
    cutoff_date: str | None = Form(None),
    cutoff_batch_code: str | None = Form(None),
    file: UploadFile = File(...),
    part_numbers: list[str] | None = Form(None),
):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in {".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(400, "盤點對帳只支援 xlsx / xls / xlsm")
    cutoff_text, batch_label = _resolve_cutoff(cutoff_date, cutoff_batch_code)

    content = await file.read(MAX_RECONCILE_UPLOAD_BYTES + 1)
    if len(content) > MAX_RECONCILE_UPLOAD_BYTES:
        raise HTTPException(400, "盤點檔案超過 10MB，請縮小後再上傳")
    selected_parts = _normalize_part_numbers(part_numbers)
    if part_numbers is not None and not selected_parts:
        raise HTTPException(400, "請至少勾選 1 支料號再建立停損點")

    temp_path = RECONCILE_UPLOAD_DIR / f"commit_{uuid4().hex}{ext}"
    try:
        temp_path.write_bytes(content)
        return commit_st_reconcile_stop_loss(
            str(temp_path),
            cutoff_text,
            source_filename=file.filename or "",
            part_numbers=selected_parts,
            cutoff_label=batch_label,
        )
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    except Exception as error:
        raise HTTPException(400, f"盤點停損點提交失敗：{error}") from error
    finally:
        temp_path.unlink(missing_ok=True)
