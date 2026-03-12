from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from .. import database as db
from ..config import BOM_HISTORY_DIR


def _build_revision_storage_path(bom_id: str, filename: str) -> Path:
    safe_name = Path(filename or "bom.xlsx").name
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target_dir = BOM_HISTORY_DIR / str(bom_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{stamp}_{safe_name}"
    counter = 1
    while target_path.exists():
        target_path = target_dir / f"{stamp}_{counter}_{safe_name}"
        counter += 1
    return target_path


def snapshot_bom_revision(bom: dict, source_action: str, note: str = "") -> dict | None:
    src_path = Path(str(bom.get("filepath") or ""))
    if not src_path.exists():
        return None

    target_path = _build_revision_storage_path(str(bom.get("id") or ""), str(bom.get("filename") or src_path.name))
    shutil.copy2(src_path, target_path)
    return db.save_bom_revision({
        "bom_file_id": str(bom.get("id") or ""),
        "filename": str(bom.get("filename") or src_path.name),
        "filepath": str(target_path),
        "source_action": str(source_action or "").strip() or "snapshot",
        "note": str(note or "").strip(),
    })


def ensure_bom_revision_history(bom: dict) -> list[dict]:
    bom_id = str(bom.get("id") or "").strip()
    if not bom_id:
        return []

    revisions = db.get_bom_revisions(bom_id)
    if revisions:
        return revisions

    snapshot_bom_revision(bom, "baseline", "系統自動建立的初始版本")
    return db.get_bom_revisions(bom_id)


def delete_bom_revision_files(bom_id: str):
    revisions = db.get_bom_revisions(str(bom_id or ""))
    for revision in revisions:
        Path(str(revision.get("filepath") or "")).unlink(missing_ok=True)

    target_dir = BOM_HISTORY_DIR / str(bom_id or "")
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)
