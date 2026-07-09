from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi import HTTPException

from app.services.inventory_restore_guard import ensure_defective_replay_allowed


class DefectiveReplayGuardTests(unittest.TestCase):
    def test_blocks_when_replay_log_exists_after_cutoff(self):
        with patch(
            "app.services.inventory_restore_guard.db.get_activity_logs_after",
            return_value=[{"id": 11, "action": "補回退回不良品扣帳"}],
        ) as mock_logs:
            with self.assertRaises(HTTPException) as ctx:
                ensure_defective_replay_allowed("2026-07-08T09:00:00")

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, "這批不良品扣帳已補回過，請勿重複補回。")
        mock_logs.assert_called_once_with(
            "2026-07-08T09:00:00",
            actions=("補回退回不良品扣帳",),
            limit=1,
        )


if __name__ == "__main__":
    unittest.main()
