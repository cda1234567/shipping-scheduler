from __future__ import annotations

import sys
from pathlib import Path


def get_app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def get_resource_base_dir() -> Path:
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass).resolve()
    return get_app_base_dir()


def get_config_file_path(filename: str = "config.yaml") -> Path:
    app_config = get_app_base_dir() / filename
    if app_config.exists():
        return app_config
    return get_resource_base_dir() / filename

