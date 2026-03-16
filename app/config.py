"""
設定管理：路徑 + YAML config 讀取。
"""
from __future__ import annotations
from pathlib import Path

import yaml

BASE_DIR      = Path(__file__).parent.parent
DATA_DIR      = BASE_DIR / "data"
MAIN_FILE_DIR = DATA_DIR / "main_file"
SCHEDULE_DIR  = DATA_DIR / "schedule"
BOM_DIR       = DATA_DIR / "bom"
BOM_HISTORY_DIR = DATA_DIR / "bom_history"
MERGE_DRAFT_DIR = DATA_DIR / "merge_drafts"
METADATA_FILE = DATA_DIR / "metadata.json"
BACKUP_DIR    = DATA_DIR / "backups"
ST_INVENTORY_DIR = DATA_DIR / "st_inventory"
STATIC_DIR    = BASE_DIR / "static"
CONFIG_FILE   = BASE_DIR / "config.yaml"

for _d in [MAIN_FILE_DIR, SCHEDULE_DIR, BOM_DIR, BOM_HISTORY_DIR, MERGE_DRAFT_DIR, BACKUP_DIR, ST_INVENTORY_DIR]:
    _d.mkdir(parents=True, exist_ok=True)


def _load_yaml() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


_cfg: dict | None = None


def get_config() -> dict:
    global _cfg
    if _cfg is None:
        _cfg = _load_yaml()
    return _cfg


def cfg(dotpath: str, default=None):
    """用 dot notation 取設定值，如 cfg("excel.main_part_col", 0)"""
    keys = dotpath.split(".")
    node = get_config()
    for k in keys:
        if isinstance(node, dict):
            node = node.get(k)
        else:
            return default
        if node is None:
            return default
    return node
