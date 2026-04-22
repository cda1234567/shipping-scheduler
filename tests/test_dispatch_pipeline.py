from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services import dispatch_pipeline


class DispatchPipelineTests(unittest.TestCase):
    def test_prepare_dispatch_context_syncs_bom_before_scaling_components(self):
        order = {
            "id": 44,
            "po_number": "4500059234",
            "model": "MODEL-M24C",
            "order_qty": 300,
            "status": "merged",
            "code": "4-4",
        }
        raw_bom = {
            "id": "bom-m24c",
            "model": "MODEL-M24C",
            "group_model": "MODEL-M24C",
            "order_qty": 300,
        }
        synced_bom = {
            **raw_bom,
            "order_qty": 600,
        }
        components = [{
            "part_number": "IC-M24C",
            "description": "EEPROM",
            "qty_per_board": 1,
            "needed_qty": 600,
            "prev_qty_cs": 0,
            "is_dash": 0,
        }]

        with tempfile.TemporaryDirectory() as temp_dir:
            main_path = Path(temp_dir) / "main.xlsx"
            main_path.write_bytes(b"placeholder")
            raw_bom["filepath"] = str(main_path)

            with patch("app.services.dispatch_pipeline.db.get_order", return_value=order), \
                 patch("app.services.dispatch_pipeline.db.get_bom_files_by_models", return_value=[raw_bom]), \
                 patch("app.services.dispatch_pipeline._ensure_editable_bom_for_draft", return_value=synced_bom) as mock_sync, \
                 patch("app.services.dispatch_pipeline.db.get_bom_components", return_value=components):
                _, groups, all_components = dispatch_pipeline.prepare_dispatch_context(44, str(main_path))

        mock_sync.assert_called_once_with(raw_bom)
        self.assertEqual(groups[0]["components"][0]["needed_qty"], 300)
        self.assertEqual(all_components[0]["needed_qty"], 300)


if __name__ == "__main__":
    unittest.main()
