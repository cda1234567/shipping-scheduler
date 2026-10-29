import os
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers import erp as erp_router
from app.services import erp_stock

ENV = {
    "ERP_SQL_SERVER": "erp.test",
    "ERP_SQL_USER": "reader",
    "ERP_SQL_PASSWORD": "s3cret-pw",
}


class FakeCursor:
    def __init__(self, log):
        self.log = log
        self.last = ""

    def execute(self, sql, params=None):
        self.last = sql
        self.log.append((sql, params))

    def fetchall(self):
        return [("AC-40838B", "電容 A", 1000), ("  ", "空料號", 5), ("R-1", None, None)]

    def fetchone(self):
        if "comWareHouse" in self.last:
            return ("ST倉",)
        return (1,)


class FakeConn:
    def __init__(self, log):
        self.log = log
        self.closed = False

    def cursor(self):
        return FakeCursor(self.log)

    def close(self):
        self.closed = True


class ErpStockTests(unittest.TestCase):
    def setUp(self):
        erp_stock.clear_cache()
        self.env = mock.patch.dict(os.environ, ENV, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.log = []
        self.connect = mock.patch.object(
            erp_stock, "_connect", side_effect=lambda s: FakeConn(self.log)
        )
        self.connect.start()
        self.addCleanup(self.connect.stop)

    def test_not_configured(self):
        with mock.patch.dict(os.environ, {"ERP_SQL_USER": "", "ERP_SQL_PASSWORD": ""}):
            with self.assertRaises(erp_stock.ErpNotConfigured):
                erp_stock.get_st_stock()

    def test_rows_and_parameterized_query(self):
        data = erp_stock.get_st_stock()
        self.assertEqual(data["source"], "erp")
        self.assertEqual(
            data["rows"],
            [
                {"part_number": "AC-40838B", "name": "電容 A", "quantity": 1000.0},
                {"part_number": "R-1", "name": "", "quantity": 0.0},
            ],
        )
        self.assertEqual(data["ware_name"], "ST倉")
        sql, params = self.log[0]
        self.assertTrue(sql.lstrip().upper().startswith("SELECT"))
        self.assertEqual(params, ("001",))

    def test_cache_and_force_refresh(self):
        erp_stock.get_st_stock()
        erp_stock.get_st_stock()
        self.assertEqual(len([1 for q, _ in self.log if "comWareAmount" in q]), 1)
        erp_stock.get_st_stock(force_refresh=True)
        self.assertEqual(len([1 for q, _ in self.log if "comWareAmount" in q]), 2)

    def test_cache_expires(self):
        erp_stock.get_st_stock()
        with mock.patch.object(erp_stock.time, "monotonic", return_value=1e12):
            erp_stock.get_st_stock()
        self.assertEqual(len([1 for q, _ in self.log if "comWareAmount" in q]), 2)

    def test_failure_not_cached(self):
        with mock.patch.object(erp_stock, "_connect", side_effect=erp_stock.ErpUnavailable("x")):
            with self.assertRaises(erp_stock.ErpUnavailable):
                erp_stock.get_st_stock()
        self.assertEqual(erp_stock.get_st_stock()["rows"][0]["part_number"], "AC-40838B")

    def test_real_connect_error_hides_password(self):
        self.connect.stop()
        try:
            fake = mock.MagicMock()
            fake.connect.side_effect = RuntimeError("login failed pw=s3cret-pw")
            with mock.patch.dict("sys.modules", {"pymssql": fake}):
                with self.assertRaises(erp_stock.ErpUnavailable) as ctx:
                    erp_stock.get_st_stock()
            self.assertNotIn("s3cret-pw", str(ctx.exception))
            kwargs = fake.connect.call_args.kwargs
            self.assertEqual(kwargs["login_timeout"], 5)
            self.assertEqual(kwargs["encryption"], "off")
            self.assertEqual(kwargs["tds_version"], "7.3")
            with mock.patch.dict("sys.modules", {"pymssql": fake}), mock.patch.dict(
                os.environ, {"ERP_SQL_ENCRYPTION": "Request", "ERP_SQL_TDS_VERSION": "7.4"}
            ):
                with self.assertRaises(erp_stock.ErpUnavailable):
                    erp_stock.get_st_stock(force_refresh=True)
            kwargs = fake.connect.call_args.kwargs
            self.assertEqual(kwargs["encryption"], "request")
            self.assertEqual(kwargs["tds_version"], "7.4")
        finally:
            self.connect.start()


class ErpApiTests(unittest.TestCase):
    def setUp(self):
        erp_stock.clear_cache()
        app = FastAPI()
        app.include_router(erp_router.router, prefix="/api")
        self.client = TestClient(app)

    def test_health_not_configured(self):
        with mock.patch.dict(os.environ, {"ERP_SQL_USER": "", "ERP_SQL_PASSWORD": ""}):
            body = self.client.get("/api/erp/health").json()
        self.assertFalse(body["configured"])
        self.assertFalse(body["connected"])
        self.assertIn("error", body)

    def test_health_connected_no_secrets(self):
        with mock.patch.dict(os.environ, ENV), mock.patch.object(
            erp_stock, "_connect", side_effect=lambda s: FakeConn([])
        ):
            res = self.client.get("/api/erp/health")
        body = res.json()
        self.assertTrue(body["configured"])
        self.assertTrue(body["connected"])
        self.assertEqual(body["database"], "CHIComp03")
        self.assertEqual(body["ware_id"], "001")
        self.assertNotIn("s3cret-pw", res.text)
        self.assertNotIn("reader", res.text)

    def test_health_unreachable(self):
        with mock.patch.dict(os.environ, ENV), mock.patch.object(
            erp_stock, "_connect", side_effect=erp_stock.ErpUnavailable("無法連線 ERP")
        ):
            body = self.client.get("/api/erp/health").json()
        self.assertTrue(body["configured"])
        self.assertFalse(body["connected"])
        self.assertEqual(body["error"], "無法連線 ERP")

    def test_st_stock_503_when_unavailable(self):
        with mock.patch.dict(os.environ, {"ERP_SQL_USER": "", "ERP_SQL_PASSWORD": ""}):
            res = self.client.get("/api/erp/st-stock")
        self.assertEqual(res.status_code, 503)
        self.assertIn("ERP", res.json()["detail"])

    def test_st_stock_enriched_with_main_snapshot(self):
        snap = {"AC-40838B": {"moq": 500, "description": "主檔描述"}}
        with mock.patch.dict(os.environ, ENV), mock.patch.object(
            erp_stock, "_connect", side_effect=lambda s: FakeConn([])
        ), mock.patch.object(erp_router.db, "get_snapshot", return_value=snap):
            rows = {r["part_number"]: r for r in self.client.get("/api/erp/st-stock").json()["rows"]}
        self.assertTrue(rows["AC-40838B"]["in_main"])
        self.assertEqual(rows["AC-40838B"]["moq"], 500)
        self.assertEqual(rows["AC-40838B"]["description"], "電容 A")
        self.assertFalse(rows["R-1"]["in_main"])
        self.assertEqual(rows["R-1"]["moq"], 0)

    def test_st_stock_ok_and_refresh_param(self):
        log = []
        with mock.patch.dict(os.environ, ENV), mock.patch.object(
            erp_stock, "_connect", side_effect=lambda s: FakeConn(log)
        ):
            self.assertEqual(self.client.get("/api/erp/st-stock").status_code, 200)
            self.client.get("/api/erp/st-stock")
            self.client.get("/api/erp/st-stock?refresh=true")
        self.assertEqual(len([1 for q, _ in log if "comWareAmount" in q]), 2)


if __name__ == "__main__":
    unittest.main()
