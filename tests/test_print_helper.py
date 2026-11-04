import importlib.util
import json
import os
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

_PATH = Path(__file__).resolve().parents[1] / "tools" / "print_helper" / "print_helper.py"
_spec = importlib.util.spec_from_file_location("print_helper", _PATH)
ph = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ph)


class PrintHelperTests(unittest.TestCase):
    def test_allow_list(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PRINT_HELPER_ALLOW", None)
            self.assertTrue(ph._allowed("127.0.0.1"))
            self.assertTrue(ph._allowed("::1"))
            self.assertTrue(ph._allowed("192.168.65.1"))
            self.assertFalse(ph._allowed("10.0.0.55"))
            self.assertFalse(ph._allowed("192.168.1.20"))
            self.assertFalse(ph._allowed("garbage"))

    def test_default_a4(self):
        os.environ.pop("SUMATRA_SETTINGS", None)
        self.assertIn("paper=A4", ph._cfg()["settings"])
        self.assertIn("noscale", ph._cfg()["settings"])

    def test_dry_run_print_and_token(self):
        env = {"PRINT_HELPER_TOKEN": "t0k", "PRINT_HELPER_DRY_RUN": "1"}
        with patch.dict(os.environ, env):
            srv = ThreadingHTTPServer(("127.0.0.1", 0), ph.Handler)
            th = threading.Thread(target=srv.serve_forever, daemon=True)
            th.start()
            try:
                base = f"http://127.0.0.1:{srv.server_address[1]}"
                printer = urllib.request.quote("HP Color LaserJet M652 [7D2006]")
                req = urllib.request.Request(f"{base}/print?printer={printer}&name=RM-1", data=b"%PDF-1.4 x",
                                             headers={"X-Print-Token": "t0k", "Content-Type": "application/pdf"})
                body = json.loads(urllib.request.urlopen(req).read())
                self.assertTrue(body["ok"])
                bad = urllib.request.Request(f"{base}/print?printer={printer}", data=b"%PDF-1.4 x",
                                             headers={"X-Print-Token": "nope"})
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(bad)
                self.assertEqual(ctx.exception.code, 401)
            finally:
                srv.shutdown()
                srv.server_close()


if __name__ == "__main__":
    unittest.main()
