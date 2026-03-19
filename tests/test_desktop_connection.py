from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.desktop_connection import (
    DESKTOP_CLIENT_CONFIG_FILENAME,
    load_desktop_client_config,
    normalize_server_url,
    resolve_remote_server_url,
)


class DesktopConnectionTests(unittest.TestCase):
    def test_normalize_server_url_adds_scheme_and_trailing_slash(self):
        self.assertEqual(
            normalize_server_url("192.168.1.10:8765"),
            "http://192.168.1.10:8765/",
        )
        self.assertEqual(
            normalize_server_url("https://demo.example.com/app"),
            "https://demo.example.com/app/",
        )

    def test_load_desktop_client_config_reads_json_object(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / DESKTOP_CLIENT_CONFIG_FILENAME
            config_path.write_text('{"server_url":"http://server:8765/"}', encoding="utf-8")

            config = load_desktop_client_config(temp_dir)

        self.assertEqual(config["server_url"], "http://server:8765/")

    def test_resolve_remote_server_url_uses_cli_then_env_then_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / DESKTOP_CLIENT_CONFIG_FILENAME
            config_path.write_text('{"server_url":"http://from-file:8765"}', encoding="utf-8")

            self.assertEqual(
                resolve_remote_server_url(temp_dir, cli_url="http://from-cli:8765"),
                "http://from-cli:8765/",
            )
            self.assertEqual(
                resolve_remote_server_url(temp_dir, env_url="http://from-env:8765"),
                "http://from-env:8765/",
            )
            self.assertEqual(
                resolve_remote_server_url(temp_dir),
                "http://from-file:8765/",
            )

    def test_resolve_remote_server_url_returns_empty_when_not_configured(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertEqual(resolve_remote_server_url(temp_dir), "")


if __name__ == "__main__":
    unittest.main()
