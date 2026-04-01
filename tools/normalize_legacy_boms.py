from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import database as db
from app.services.bom_editor import (
    build_bom_storage_payload,
    normalize_bom_record_to_editable,
    parse_bom_for_storage,
)


def _needs_normalize(bom: dict) -> bool:
    filepath = Path(str(bom.get("filepath") or ""))
    return filepath.suffix.lower() == ".xls" or not bool(bom.get("is_converted"))


def normalize_legacy_boms(dry_run: bool = False) -> tuple[int, int]:
    converted = 0
    skipped = 0
    for bom in db.get_bom_files():
        if not _needs_normalize(bom):
            skipped += 1
            continue

        if dry_run:
            print(f"[DRY-RUN] {bom['id']} {bom.get('filename') or ''}")
            converted += 1
            continue

        normalized = normalize_bom_record_to_editable(bom)
        parsed = parse_bom_for_storage(
            path=normalized["filepath"],
            bom_id=normalized["id"],
            filename=normalized["filename"],
            uploaded_at=normalized["uploaded_at"],
            group_model=normalized.get("group_model", ""),
            source_filename=normalized.get("source_filename", ""),
            source_format=normalized.get("source_format", ""),
            is_converted=bool(normalized.get("is_converted")),
        )
        db.save_bom_file(build_bom_storage_payload(parsed))
        db.log_activity("bom_convert", f"{normalized['filename']} 已批次轉為可編輯 xlsx")
        print(f"[OK] {normalized['id']} -> {normalized['filepath']}")
        converted += 1

    return converted, skipped


def main():
    parser = argparse.ArgumentParser(description="將舊版 .xls BOM 批次轉成可編輯 .xlsx")
    parser.add_argument("--dry-run", action="store_true", help="只列出會轉換的 BOM，不實際修改")
    args = parser.parse_args()

    converted, skipped = normalize_legacy_boms(dry_run=args.dry_run)
    if args.dry_run:
        print(f"DRY-RUN: {converted} 份需要轉換，{skipped} 份已是可編輯格式")
    else:
        print(f"完成：{converted} 份已轉換，{skipped} 份原本就已是可編輯格式")


if __name__ == "__main__":
    main()
