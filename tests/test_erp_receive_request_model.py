"""ErpReceiveRequest 與 /preview、/create 路由：前端送的 exch_rate 不可被 pydantic 丟掉。"""
import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.models import ErpReceiveRequest


# 與 static/modules/erp_receive.js buildBody() 送出的欄位一致
BODY = {
    "target": None,
    "rm_date": 20261109,
    "lines": [{"po_no": "PO-1", "row_no": 1, "qty": 3000, "price": 6.6}],
    "confirm": False,
    "client_token": "tok",
    "update_cost": True,
    "exch_rate": 32.77,
    "entry_ccy": "NTD",
}


class ErpReceiveRequestModelTests(unittest.TestCase):
    def test_model_keeps_every_field_frontend_sends(self):
        req = ErpReceiveRequest(**BODY).dict()
        self.assertEqual(set(req), set(BODY))
        self.assertEqual(set(req["lines"][0]), set(BODY["lines"][0]))
        self.assertEqual(req["exch_rate"], 32.77)
        self.assertEqual(req["entry_ccy"], "NTD")

    def test_exch_rate_defaults_to_none(self):
        self.assertIsNone(ErpReceiveRequest(lines=[]).dict()["exch_rate"])
        self.assertIsNone(ErpReceiveRequest(lines=[]).dict()["entry_ccy"])


class ErpReceiveRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")  # 繞過 edit_auth
        from main import app
        cls.client = TestClient(app)

    def test_preview_passes_exch_rate_to_service(self):
        seen = {}

        def fake_preview(req):
            seen.update(req)
            return {"ok": True}

        with patch("app.services.erp_receive.preview_receive", side_effect=fake_preview):
            res = self.client.post("/api/erp-receive/preview", json=BODY)
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(seen["exch_rate"], 32.77)
        self.assertEqual(seen["entry_ccy"], "NTD")

    def test_create_passes_exch_rate_to_service(self):
        seen = {}

        def fake_create(req, actor, ip):
            seen.update(req)
            return {"ok": True}

        with patch("app.services.erp_receive.create_receive", side_effect=fake_create):
            res = self.client.post("/api/erp-receive/create", json={**BODY, "confirm": True})
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(seen["exch_rate"], 32.77)
        self.assertIs(seen["confirm"], True)
        self.assertEqual(seen["entry_ccy"], "NTD")

    def test_old_client_without_entry_ccy_still_accepted(self):
        seen = {}

        def fake_preview(req):
            seen.update(req)
            return {"ok": True}

        body = {k: v for k, v in BODY.items() if k != "entry_ccy"}
        with patch("app.services.erp_receive.preview_receive", side_effect=fake_preview):
            res = self.client.post("/api/erp-receive/preview", json=body)
        self.assertEqual(res.status_code, 200, res.text)
        self.assertIsNone(seen["entry_ccy"])


if __name__ == "__main__":
    unittest.main()
