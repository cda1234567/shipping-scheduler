from __future__ import annotations

import re

from .. import database as db


INVENTORY_COUNT_LOCKED_MESSAGE = (
    "盤點進行中，庫存異動已暫停。請先完成或取消盤點後再操作。"
)

_SCHEDULE_DYNAMIC_RE = re.compile(
    r"^/api/schedule/(?:orders/\d+/(?:dispatch|rollback)|drafts/\d+/commit)$"
)


def is_inventory_mutation_request(method: str, path: str) -> bool:
    verb = str(method or "").upper()
    normalized_path = str(path or "").rstrip("/")
    if verb not in {"POST", "PUT", "PATCH", "DELETE"}:
        return False

    if normalized_path in {
        "/api/main-file/upload",
        "/api/main-file/rollover",
        "/api/main-file/snapshot",
        "/api/main-file/cell",
        "/api/system/st-inventory/upload",
        "/api/schedule/batch-dispatch",
        "/api/schedule/update-and-commit-drafts",
        "/api/schedule/supplement-part",
    }:
        return True
    if _SCHEDULE_DYNAMIC_RE.fullmatch(normalized_path):
        return True

    if normalized_path.startswith("/api/defectives"):
        if verb == "DELETE":
            return True
        safe_preview_suffixes = ("/preview", "-preview")
        return verb == "POST" and not normalized_path.endswith(safe_preview_suffixes)
    return False


def get_inventory_count_lock() -> dict | None:
    return db.get_active_inventory_count_session("st")
