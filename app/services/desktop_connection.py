from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse


DESKTOP_CLIENT_CONFIG_FILENAME = "desktop_client.json"


def normalize_server_url(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "://" not in text:
        text = f"http://{text}"
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("桌面版遠端服務網址格式不正確")
    return text.rstrip("/") + "/"


def load_desktop_client_config(base_dir: str | Path) -> dict:
    config_path = Path(base_dir) / DESKTOP_CLIENT_CONFIG_FILENAME
    if not config_path.exists():
        return {}

    try:
        payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    except Exception as error:
        raise ValueError(f"桌面版設定檔解析失敗：{error}") from error

    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("桌面版設定檔格式不正確，必須是 JSON 物件")
    return payload


def resolve_remote_server_url(
    base_dir: str | Path,
    *,
    cli_url: str | None = None,
    env_url: str | None = None,
) -> str:
    cli_value = str(cli_url or "").strip()
    if cli_value:
        return normalize_server_url(cli_value)

    env_value = str(env_url or "").strip()
    if env_value:
        return normalize_server_url(env_value)

    config = load_desktop_client_config(base_dir)
    return normalize_server_url(config.get("server_url"))
