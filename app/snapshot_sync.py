"""
主檔快照同步 — 任何修改主檔的操作完成後呼叫 refresh_snapshot_from_main，
讓快照永遠跟著當前主檔的庫存。
"""

import logging
from pathlib import Path

from . import database as db
from .services.main_reader import read_moq, read_stock

log = logging.getLogger(__name__)


def refresh_snapshot_from_main(main_path: str) -> int:
    """讀取當前主檔庫存與 MOQ，重設快照基準。

    Returns:
        更新的料號數量
    """
    resolved = str(main_path or "").strip()
    if not resolved or not Path(resolved).exists():
        return 0

    try:
        stock = read_stock(resolved)
        moq = read_moq(resolved)
    except Exception as exc:
        log.warning("refresh_snapshot_from_main skipped for unreadable main file %s: %s", resolved, exc)
        return 0

    # 保留使用者手動設定的 MOQ（不被主檔覆蓋）
    manual_moq = db.get_manual_snapshot_moq()
    moq.update(manual_moq)

    db.save_snapshot(stock, moq, manual_moq_parts=set(manual_moq))

    # 主檔或快照變了，清掉 main-file/data 的回應快取
    try:
        from .routers.main_file import invalidate_main_data_cache
        invalidate_main_data_cache()
    except ImportError:
        pass

    return len(stock)
