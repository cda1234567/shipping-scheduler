"""不良品處理 API — Excel 匯入（副檔格式）+ 主檔扣帳，批次管理"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File

from .. import database as db
from ..config import BACKUP_DIR
from ..services.defective_deduction import (
    parse_defective_excel,
    deduct_defectives_from_main,
    reverse_defectives_from_main,
)

router = APIRouter(prefix="/defectives", tags=["defectives"])


def _get_main_file_mtime() -> float:
    """取得目前主檔的修改時間（mtime），用來判斷主檔是否被更換。"""
    main_path = str(db.get_setting("main_file_path") or "").strip()
    if main_path and Path(main_path).exists():
        return os.path.getmtime(main_path)
    return 0


@router.get("/batches")
async def list_batches():
    """取得所有匯入批次（含明細）。"""
    batches = db.get_defective_batches()
    return {"batches": batches}


@router.post("/import")
async def import_defectives(file: UploadFile = File(...)):
    """匯入副檔格式的不良品 Excel → 自動扣主檔庫存 + 建立批次紀錄。"""
    if not file.filename:
        raise HTTPException(400, "請選擇檔案")

    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".xlsx", ".xls", ".xlsm"):
        raise HTTPException(400, "僅支援 .xlsx / .xls / .xlsm")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        items = parse_defective_excel(tmp_path)
    except Exception as e:
        Path(tmp_path).unlink(missing_ok=True)
        raise HTTPException(400, f"解析失敗：{e}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    if not items:
        raise HTTPException(400, "檔案中沒有有效的不良品資料（需要料號 + 數量 > 0）")

    main_path = str(db.get_setting("main_file_path") or "").strip()
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "主檔尚未上傳，無法扣帳")

    result = deduct_defectives_from_main(
        main_path, items, backup_dir=str(BACKUP_DIR),
    )

    # 記錄扣帳當下的主檔 mtime
    mtime = _get_main_file_mtime()
    batch_id = db.create_defective_batch(
        file.filename or "unknown", main_file_mtime=mtime,
    )

    # 建立紀錄（含扣帳前後庫存）
    result_map = {r["part_number"]: r for r in (result.get("results") or [])}
    created_ids: list[int] = []
    for item in items:
        part = item["part_number"]
        if part in (result.get("skipped_parts") or []):
            continue
        matched = result_map.get(part, {})
        record_id = db.create_defective_record({
            "batch_id": batch_id,
            "part_number": part,
            "description": item.get("description", ""),
            "defective_qty": item["defective_qty"],
            "stock_before": matched.get("stock_before", 0),
            "stock_after": matched.get("stock_after", 0),
            "status": "confirmed",
        })
        created_ids.append(record_id)

    db.log_activity(
        "匯入不良品",
        f"{file.filename}：扣帳 {result['deducted_count']} 筆"
        + (f"，略過 {len(result['skipped_parts'])} 筆" if result["skipped_parts"] else ""),
    )

    return {
        "ok": True,
        "batch_id": batch_id,
        "deducted_count": result["deducted_count"],
        "skipped_parts": result["skipped_parts"],
        "results": result["results"],
        "created_ids": created_ids,
    }


@router.post("/batches/{batch_id}/add")
async def add_item_to_batch(batch_id: int, file: UploadFile = File(...)):
    """對已存在的批次追加不良品項目（解析 Excel + 扣主檔）。"""
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in (".xlsx", ".xls", ".xlsm"):
        raise HTTPException(400, "僅支援 .xlsx / .xls / .xlsm")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        items = parse_defective_excel(tmp_path)
    except Exception as e:
        Path(tmp_path).unlink(missing_ok=True)
        raise HTTPException(400, f"解析失敗：{e}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    if not items:
        raise HTTPException(400, "沒有有效的不良品資料")

    main_path = str(db.get_setting("main_file_path") or "").strip()
    if not main_path or not Path(main_path).exists():
        raise HTTPException(400, "主檔尚未上傳")

    result = deduct_defectives_from_main(
        main_path, items, backup_dir=str(BACKUP_DIR),
    )

    result_map = {r["part_number"]: r for r in (result.get("results") or [])}
    for item in items:
        part = item["part_number"]
        if part in (result.get("skipped_parts") or []):
            continue
        matched = result_map.get(part, {})
        db.create_defective_record({
            "batch_id": batch_id,
            "part_number": part,
            "description": item.get("description", ""),
            "defective_qty": item["defective_qty"],
            "stock_before": matched.get("stock_before", 0),
            "stock_after": matched.get("stock_after", 0),
            "status": "confirmed",
        })

    db.log_activity("追加不良品", f"批次#{batch_id}：{result['deducted_count']} 筆")

    return {
        "ok": True,
        "deducted_count": result["deducted_count"],
        "skipped_parts": result["skipped_parts"],
        "results": result["results"],
    }


@router.delete("/records/{record_id}")
async def delete_record(record_id: int):
    if not db.delete_defective_record(record_id):
        raise HTTPException(404, "找不到紀錄")
    db.log_activity("刪除不良品", f"ID={record_id}")
    return {"ok": True}


@router.delete("/batches/{batch_id}")
async def delete_batch(batch_id: int):
    # 先取出該批次的所有紀錄，用來回寫主檔
    batches = db.get_defective_batches()
    target_batch = next((b for b in batches if b["id"] == batch_id), None)
    if not target_batch:
        raise HTTPException(404, "找不到批次")

    records = target_batch.get("items") or []
    reverse_items = [
        {"part_number": r["part_number"], "defective_qty": r["defective_qty"]}
        for r in records
        if r.get("defective_qty") and r["defective_qty"] > 0
    ]

    # 比對主檔 mtime — 如果主檔已被更換就不回寫
    reversed_count = 0
    main_file_changed = False
    batch_mtime = float(target_batch.get("main_file_mtime") or 0)
    current_mtime = _get_main_file_mtime()

    if reverse_items:
        main_path = str(db.get_setting("main_file_path") or "").strip()
        if not main_path or not Path(main_path).exists():
            main_file_changed = True
        elif batch_mtime > 0 and abs(current_mtime - batch_mtime) > 1:
            # mtime 差距超過 1 秒 → 主檔已被更換
            main_file_changed = True
        else:
            result = reverse_defectives_from_main(
                main_path, reverse_items, backup_dir=str(BACKUP_DIR),
            )
            reversed_count = result["reversed_count"]

    # 刪除 DB 紀錄
    if not db.delete_defective_batch(batch_id):
        raise HTTPException(404, "刪除失敗")

    batch_name = target_batch.get("filename", f"#{batch_id}")
    if main_file_changed:
        detail = f"{batch_name}：主檔已更換，僅刪除紀錄（未回寫庫存）"
    else:
        detail = f"{batch_name}：已回復 {reversed_count} 筆庫存"
    db.log_activity("刪除不良品批次", detail)

    return {
        "ok": True,
        "reversed_count": reversed_count,
        "main_file_changed": main_file_changed,
    }
