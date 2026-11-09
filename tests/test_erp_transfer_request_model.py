"""ErpTransferCreateRequest 與 /create 路由：from __future__ import annotations 下不可再因 ForwardRef 爆 500。"""
import os
import typing
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from pydantic import BaseModel

from app import models
from app.models import ErpTransferCreateRequest
from app.services import erp_purchase as ep


class ErpTransferCreateRequestTests(unittest.TestCase):
    def test_create_request_accepts_batches_list(self):
        req = ErpTransferCreateRequest(batches=["2026/11-1"], client_token="t", preview_token="p")
        self.assertEqual(req.dict(), {"batches": ["2026/11-1"], "client_token": "t", "preview_token": "p"})

    def test_missing_fields_default_so_service_returns_chinese_400(self):
        self.assertEqual(ErpTransferCreateRequest().dict(), {"batches": [], "client_token": "", "preview_token": ""})

    def test_no_model_in_app_models_has_unresolved_forward_ref(self):
        for name, obj in vars(models).items():
            if isinstance(obj, type) and issubclass(obj, BaseModel) and obj is not BaseModel:
                for field in obj.__fields__.values():
                    self.assertNotIsInstance(field.outer_type_, typing.ForwardRef, f"{name}.{field.name}")
                    self.assertNotIsInstance(field.type_, typing.ForwardRef, f"{name}.{field.name}")


class ErpTransferRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        cls.client = TestClient(app)

    def test_create_passes_dict_to_service(self):
        body = {"batches": ["2026/11-1", "2026/11-2"], "client_token": "tok", "preview_token": "pv"}
        reply = {"results": [{"batch_key": "2026/11-1", "ok": True, "move_no": "RM-1"}], "stopped": []}
        with patch("app.services.erp_transfer.create", return_value=reply) as create:
            res = self.client.post("/api/shortage-po/transfer/create", json=body)
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(res.json(), reply)
        self.assertEqual(create.call_args.args[0], body)
        self.assertEqual(create.call_args.args[1], "web")

    def test_validation_error_is_400_not_500(self):
        with patch("app.services.erp_transfer.create", side_effect=ep.PoValidationError("預覽已失效，請重新預覽")):
            res = self.client.post("/api/shortage-po/transfer/create", json={})
        self.assertEqual(res.status_code, 400, res.text)
        self.assertIn("預覽已失效", res.text)

    def test_empty_batches_rejected_by_service_with_400(self):
        with patch("app.services.erp_transfer.resolve_target", return_value=("CHIComp13", {})):
            res = self.client.post("/api/shortage-po/transfer/create", json={"batches": [], "client_token": "t", "preview_token": "p"})
        self.assertEqual(res.status_code, 400, res.text)
        self.assertIn("請選擇批次", res.text)


if __name__ == "__main__":
    unittest.main()
