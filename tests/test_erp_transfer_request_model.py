"""ErpTransferCreateRequest 必須能在 from __future__ import annotations 下建構。"""
import unittest

from app.models import ErpTransferCreateRequest


class ErpTransferCreateRequestTests(unittest.TestCase):
    def test_create_request_accepts_batches_list(self):
        req = ErpTransferCreateRequest(
            batches=["2026/11-1"],
            client_token="t",
            preview_token="p",
        )
        self.assertEqual(req.batches, ["2026/11-1"])
        self.assertEqual(req.client_token, "t")
        self.assertEqual(req.preview_token, "p")


if __name__ == "__main__":
    unittest.main()
