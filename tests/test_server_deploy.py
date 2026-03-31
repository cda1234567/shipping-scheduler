from __future__ import annotations

import unittest
from pathlib import Path


class ServerDeployTests(unittest.TestCase):
    def test_server_compose_uses_caddy_for_https(self):
        root = Path(__file__).resolve().parents[1]
        compose = (root / "docker-compose.server.yml").read_text(encoding="utf-8")

        self.assertIn("caddy:", compose)
        self.assertIn("image: caddy:2", compose)
        self.assertIn('- "80:80"', compose)
        self.assertIn('- "443:443"', compose)
        self.assertIn("./deploy/Caddyfile:/etc/caddy/Caddyfile:ro", compose)
        self.assertIn("caddy_data:", compose)
        self.assertIn("caddy_config:", compose)
        self.assertIn("APP_DOMAIN: ${APP_DOMAIN}", compose)
        self.assertIn("APP_UPSTREAM: ${APP_UPSTREAM:-dispatch-scheduler:8765}", compose)
        self.assertIn("APP_BASIC_AUTH_USER: ${APP_BASIC_AUTH_USER:-admin}", compose)
        self.assertIn("APP_BASIC_AUTH_PASSWORD_HASH: ${APP_BASIC_AUTH_PASSWORD_HASH}", compose)
        self.assertIn("expose:", compose)
        self.assertIn('- "8765"', compose)

    def test_caddyfile_routes_domain_and_www(self):
        root = Path(__file__).resolve().parents[1]
        caddyfile = (root / "deploy" / "Caddyfile").read_text(encoding="utf-8")

        self.assertIn("{$APP_DOMAIN}", caddyfile)
        self.assertIn("basic_auth * {", caddyfile)
        self.assertIn("{$APP_BASIC_AUTH_USER} {$APP_BASIC_AUTH_PASSWORD_HASH}", caddyfile)
        self.assertIn("reverse_proxy {$APP_UPSTREAM}", caddyfile)
        self.assertIn("www.{$APP_DOMAIN}", caddyfile)
        self.assertIn("redir https://{$APP_DOMAIN}{uri} permanent", caddyfile)
        self.assertIn("Strict-Transport-Security", caddyfile)

    def test_server_env_example_includes_domain_settings(self):
        root = Path(__file__).resolve().parents[1]
        env_example = (root / ".env.server.example").read_text(encoding="utf-8")

        self.assertIn("APP_DOMAIN=cda1234567.com", env_example)
        self.assertIn("ACME_EMAIL=you@example.com", env_example)
        self.assertIn("APP_UPSTREAM=dispatch-scheduler:8765", env_example)
        self.assertIn("APP_BASIC_AUTH_USER=admin", env_example)
        self.assertIn("APP_BASIC_AUTH_PASSWORD_HASH='$2b$12$Soo61t0XpKANoBu0jiWNmuiP6gaIf.i3TlfZO/L4sn.YkQ4LsM2IO'", env_example)
        self.assertIn("CADDY_CONTAINER_NAME=dispatch-caddy", env_example)
