from __future__ import annotations

from pathlib import Path

from .local_time import local_now


def minute_timestamp(now=None) -> str:
    return (now or local_now()).strftime("%Y%m%d_%H%M")


def append_minute_timestamp(filename: str, now=None) -> str:
    path = Path(filename or "download.bin")
    stem = path.stem or "download"
    suffix = path.suffix
    return f"{stem}_{minute_timestamp(now)}{suffix}"


def build_generated_filename(prefix: str, suffix: str, now=None) -> str:
    normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
    safe_prefix = str(prefix or "download").strip() or "download"
    return f"{safe_prefix}_{minute_timestamp(now)}{normalized_suffix}"
