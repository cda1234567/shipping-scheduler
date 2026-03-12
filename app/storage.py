from __future__ import annotations
import json
from .config import METADATA_FILE
from .models import Metadata

_cache: Metadata | None = None


def load() -> Metadata:
    global _cache
    if _cache is None:
        if METADATA_FILE.exists():
            _cache = Metadata.parse_raw(METADATA_FILE.read_text(encoding="utf-8"))
        else:
            _cache = Metadata()
    return _cache


def save(meta: Metadata) -> None:
    global _cache
    _cache = meta
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    METADATA_FILE.write_text(json.dumps(meta.dict(), indent=2, ensure_ascii=False), encoding="utf-8")


def invalidate() -> None:
    global _cache
    _cache = None
