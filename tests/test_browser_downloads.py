"""所有下載改由瀏覽器下載（v2026.11.18.1）：伺服器另存後提供 /api/download-file 讓瀏覽器抓回。"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote

from fastapi.testclient import TestClient

from app.services import server_downloads
from main import app

ROOT = Path(__file__).resolve().parents[1]
XLSX_MEDIA = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _settings(key, default=""):
    return {
        "server_download_enabled": "1",
        "server_download_display_path": "D:\\Download\\excel",
    }.get(key, default)


class DownloadFileEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        cls.client = TestClient(app)

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.download_dir = self.root / "downloads"
        self.download_dir.mkdir()
        (self.root / "secret.xlsx").write_bytes(b"outside")
        self._patch = patch.object(server_downloads, "SERVER_DOWNLOAD_DIR", self.download_dir)
        self._patch.start()

    def tearDown(self):
        self._patch.stop()
        self._tmp.cleanup()

    def test_downloads_saved_file_as_attachment_with_utf8_filename(self):
        name = "發料單_20261118_0930.xlsx"
        (self.download_dir / name).write_bytes(b"xlsx-bytes")

        resp = self.client.get(f"/api/download-file?name={quote(name)}")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content, b"xlsx-bytes")
        disposition = resp.headers["content-disposition"]
        self.assertTrue(disposition.startswith("attachment"))
        self.assertIn(f"filename*=utf-8''{quote(name)}", disposition)
        self.assertIn(XLSX_MEDIA, resp.headers["content-type"])

    def test_rejects_path_traversal_and_bad_names(self):
        for bad in ["../secret.xlsx", "..\\secret.xlsx", "sub/a.xlsx", "..", ".", "", "  ", "a:b.xlsx"]:
            with self.subTest(name=bad):
                resp = self.client.get("/api/download-file", params={"name": bad})
                self.assertEqual(resp.status_code, 400, resp.text)
                self.assertNotEqual(resp.content, b"outside")

    def test_rejects_subdirectory_and_missing_file(self):
        (self.download_dir / "folder").mkdir()
        self.assertEqual(self.client.get("/api/download-file", params={"name": "folder"}).status_code, 404)
        self.assertEqual(self.client.get("/api/download-file", params={"name": "none.xlsx"}).status_code, 404)

    def test_returns_404_when_download_dir_unavailable(self):
        with patch.object(server_downloads, "SERVER_DOWNLOAD_DIR", self.root / "missing"):
            resp = self.client.get("/api/download-file", params={"name": "a.xlsx"})
        self.assertEqual(resp.status_code, 404)

    def test_server_save_then_download_round_trip(self):
        source = self.root / "主檔.xlsx"
        source.write_bytes(b"main-file")
        request = SimpleNamespace(query_params={"server_save": "1"})

        with patch.object(server_downloads.db, "get_setting", side_effect=_settings):
            first = server_downloads.maybe_server_save_response(request, str(source), "主檔.xlsx", XLSX_MEDIA)
            second = server_downloads.maybe_server_save_response(request, str(source), "主檔.xlsx", XLSX_MEDIA)

        payloads = [json.loads(r.body.decode("utf-8")) for r in (first, second)]
        # 既有欄位不變，另外多 download_url
        self.assertEqual(payloads[0]["ok"], True)
        self.assertEqual(payloads[0]["filename"], "主檔.xlsx")
        self.assertEqual(payloads[0]["directory"], "D:\\Download\\excel")
        self.assertEqual(payloads[1]["filename"], "主檔 (1).xlsx")
        # 伺服器端照舊存檔
        self.assertEqual((self.download_dir / "主檔.xlsx").read_bytes(), b"main-file")
        self.assertEqual((self.download_dir / "主檔 (1).xlsx").read_bytes(), b"main-file")

        for payload in payloads:
            self.assertEqual(payload["download_url"], f"/api/download-file?name={quote(payload['filename'])}")
            resp = self.client.get(payload["download_url"])
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.content, b"main-file")
            self.assertTrue(resp.headers["content-disposition"].startswith("attachment"))

    def test_bytes_server_save_includes_download_url(self):
        request = SimpleNamespace(query_params={"server_save": "1"})
        with patch.object(server_downloads.db, "get_setting", side_effect=_settings):
            response = server_downloads.maybe_server_save_bytes_response(request, b"zip", "副檔.zip")
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(set(payload), {"ok", "filename", "directory", "download_url"})
        self.assertEqual(self.client.get(payload["download_url"]).content, b"zip")

    def test_without_server_save_returns_attachment_file(self):
        source = self.root / "src.xlsx"
        source.write_bytes(b"direct")
        request = SimpleNamespace(query_params={})
        response = server_downloads.maybe_server_save_response(request, str(source), "買料提醒.xlsx", XLSX_MEDIA)
        disposition = response.headers["content-disposition"]
        self.assertTrue(disposition.startswith("attachment"))
        self.assertIn(quote("買料提醒.xlsx"), disposition)
        self.assertEqual(list(self.download_dir.iterdir()), [])


class FrontendBrowserDownloadTests(unittest.TestCase):
    def test_server_save_result_is_fetched_back_and_downloaded_by_browser(self):
        js = (ROOT / "static" / "modules" / "desktop_bridge.js").read_text(encoding="utf-8")
        start = js.index("async function fallbackBrowserDownload(")
        body = js[start:js.index("export function buildDownloadToastMessage(")]
        self.assertIn("server_save=1", body)
        self.assertIn("result.download_url", body)
        self.assertIn("/api/download-file?name=", body)
        # JSON 結果與非 JSON 檔案回應都要交給瀏覽器下載，不能只回伺服器路徑
        self.assertEqual(body.count("deliverResponseToBrowser("), 3)
        self.assertNotIn("return result;", body)
        self.assertIn("anchor.download = outputName", js)
        self.assertIn("server_directory", js[js.index("export function buildDownloadToastMessage("):])

    def test_toast_shows_browser_download_and_server_copy(self):
        script = r"""
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const src = readFileSync('static/modules/desktop_bridge.js', 'utf8');
const body = src.slice(src.indexOf('export function buildDownloadToastMessage('), src.indexOf('export function showDownloadToast('));
const build = new Function('normalizeDownloadText', body.replace('export ', '') + '; return buildDownloadToastMessage;')(v => String(v || '').trim());
assert.equal(
  build({ filename: '主檔.xlsx', path: '主檔.xlsx', directory: '', server_directory: 'D:\\Download\\excel', server_filename: '主檔 (1).xlsx' }, '主檔'),
  '主檔已下載：主檔.xlsx\n伺服器另存：D:\\Download\\excel\\主檔 (1).xlsx',
);
assert.equal(build({ filename: 'a.xlsx', path: 'a.xlsx', directory: '' }, '主檔'), '主檔已下載：a.xlsx');
"""
        result = subprocess.run(["node", "--input-type=module", "-e", script], cwd=ROOT,
                                capture_output=True, text=True, encoding="utf-8", timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)


if __name__ == "__main__":
    unittest.main()
