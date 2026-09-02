from __future__ import annotations

import os
import re
import shutil
import tempfile
from pathlib import Path

from .. import database as db
from ..config import BACKUP_DIR, MAIN_FILE_DIR
from ..snapshot_sync import refresh_snapshot_from_main
from .db_backup import create_database_backup
from .local_time import local_now
from .main_file_lock import serialized_main_file_write
from .main_reader import read_stock


def _safe_label(value: str) -> str:
    label = str(value or "").strip()
    if not re.fullmatch(r"[0-9A-Za-z_-]{2,20}", label):
        raise ValueError("年度請輸入 2–20 碼英數字，例如 2027")
    return label


@serialized_main_file_write
def rollover_main_file(*, content: bytes, filename: str, period_label: str) -> dict:
    """封存目前主檔與資料庫，再以新檔建立全新的年度庫存基準。"""
    label = _safe_label(period_label)
    source_name = Path(str(filename or "").strip()).name
    extension = Path(source_name).suffix.lower()
    if extension not in {".xlsx", ".xlsm"}:
        raise ValueError("年度新主檔僅支援 .xlsx / .xlsm")
    if not content:
        raise ValueError("新主檔內容是空的")
    if db.get_active_inventory_count_session("st"):
        raise ValueError("盤點鎖定中，請先完成或取消盤點再換檔")

    current_path_text = str(db.get_setting("main_file_path") or "").strip()
    current_path = Path(current_path_text) if current_path_text else None
    if not current_path or not current_path.exists():
        raise ValueError("找不到目前主檔，請先用一般上傳建立主檔")

    previous_label = str(db.get_setting("main_file_period_label") or "").strip()
    if previous_label == label:
        raise ValueError(f"目前已是 {label} 主檔")
    if not previous_label:
        loaded_at = str(db.get_setting("main_loaded_at") or "")
        previous_label = loaded_at[:4] if re.match(r"^\d{4}", loaded_at) else "既有"

    MAIN_FILE_DIR.mkdir(parents=True, exist_ok=True)
    fd, staged_name = tempfile.mkstemp(prefix=".rollover-", suffix=extension, dir=str(MAIN_FILE_DIR))
    os.close(fd)
    staged_path = Path(staged_name)
    destination = MAIN_FILE_DIR / f"main{extension}"
    old_filename = str(db.get_setting("main_filename") or current_path.name)
    old_loaded_at = str(db.get_setting("main_loaded_at") or "")
    old_part_count = str(db.get_setting("main_part_count") or "0")
    replaced = False

    try:
        staged_path.write_bytes(content)
        try:
            stock = read_stock(str(staged_path))
        except Exception as error:
            raise ValueError(f"無法讀取新主檔：{error}") from error
        if not stock:
            raise ValueError("新主檔沒有讀到任何有效料號，未執行換檔")

        now = local_now()
        stamp = now.strftime("%Y%m%d_%H%M%S")
        archive_root = BACKUP_DIR / "year_rollover"
        archive_dir = archive_root / f"{previous_label}-to-{label}_{stamp}"
        suffix = 1
        while archive_dir.exists():
            archive_dir = archive_root / f"{previous_label}-to-{label}_{stamp}_{suffix}"
            suffix += 1
        archive_dir.mkdir(parents=True, exist_ok=False)
        archived_main_path = archive_dir / current_path.name
        shutil.copy2(current_path, archived_main_path)
        database_backup = create_database_backup(
            reason=f"main_file_rollover_{label}",
            backup_dir=archive_dir,
            now=now,
            prune=False,
        )

        os.replace(staged_path, destination)
        replaced = True
        db.set_setting("main_file_path", str(destination))
        db.set_setting("main_filename", source_name or destination.name)
        db.set_setting("main_loaded_at", now.isoformat(timespec="seconds"))
        part_count = refresh_snapshot_from_main(str(destination))
        if part_count <= 0:
            raise ValueError("新主檔無法建立庫存基準，已取消換檔")

        period = db.create_main_file_period(
            label=label,
            source_filename=source_name,
            main_file_path=str(destination),
            previous_label=previous_label,
            archived_main_path=str(archived_main_path),
            database_backup_path=str(database_backup["path"]),
            started_at=now.isoformat(timespec="seconds"),
        )
    except Exception:
        if replaced:
            shutil.copy2(archived_main_path, current_path)
            db.set_setting("main_file_path", str(current_path))
            db.set_setting("main_filename", old_filename)
            db.set_setting("main_loaded_at", old_loaded_at)
            db.set_setting("main_part_count", old_part_count)
            refresh_snapshot_from_main(str(current_path))
        raise
    finally:
        staged_path.unlink(missing_ok=True)

    try:
        from .merge_drafts import rebuild_merge_drafts

        merged_ids = [int(order["id"]) for order in db.get_orders(["merged"])]
        if merged_ids:
            rebuild_merge_drafts(merged_ids)
    except Exception:
        pass

    db.log_activity(
        "main_file_rollover",
        f"{previous_label} → {label}，新主檔 {source_name}，{part_count} 筆料號",
    )
    return {
        "ok": True,
        "period": period,
        "filename": source_name,
        "part_count": part_count,
        "archived_main_path": str(archived_main_path),
        "database_backup_path": str(database_backup["path"]),
    }
