"""備份檔自動清理 — 刪除超過指定天數的舊備份。"""
from __future__ import annotations

import logging
import time
from pathlib import Path

from ..config import BACKUP_DIR

BACKUP_RETENTION_DAYS = 90
logger = logging.getLogger(__name__)


def cleanup_old_backups(
    backup_dir: str | Path | None = None,
    retention_days: int = BACKUP_RETENTION_DAYS,
) -> int:
    """刪除 backup_dir 中超過 retention_days 天的檔案，回傳刪除數量。"""
    target_dir = Path(backup_dir) if backup_dir else BACKUP_DIR
    if not target_dir.exists():
        return 0

    cutoff = time.time() - (retention_days * 86400)
    removed = 0
    for filepath in target_dir.iterdir():
        if not filepath.is_file():
            continue
        try:
            if filepath.stat().st_mtime < cutoff:
                filepath.unlink()
                removed += 1
        except Exception as e:
            logger.warning("清理備份失敗 %s: %s", filepath.name, e)

    if removed:
        logger.info("已清理 %d 個超過 %d 天的備份檔", removed, retention_days)
    return removed
