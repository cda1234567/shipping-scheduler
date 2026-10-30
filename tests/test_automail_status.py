"""automail 寄信狀態：彙整、快取、獨立失敗、API、唯讀（絕不寄信）。"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient

from app.services import automail_status as am

ROOT = Path(__file__).resolve().parents[1]

SENDLOG = [
    {"vendor": "甲", "orderNumber": "PO-20261030001", "filename": "a.pdf", "to": ["a@x.com"], "cc": [], "sentAt": "2026-10-30T06:20:00.000Z"},
    {"vendor": "甲", "orderNumber": " po-20261030001 ", "filename": "a.pdf", "to": ["b@x.com"], "cc": [], "sentAt": "2026-10-30T01:28:09.880Z"},
    {"vendor": "乙", "orderNumber": "PO-20261030002", "filename": "b.pdf", "to": ["c@x.com"], "cc": [], "sentAt": "2026-10-29T16:30:00.000Z"},
]
REPLIES = {"enabled": True, "fetchedAt": "x", "error": "", "replies": [
    {"orderNumber": "po-20261030001", "vendor": "甲", "from": "v@x.com", "fromName": "王", "subject": "Re: PO", "receivedAt": "2026-10-30T07:00:00Z", "matchedBy": "subject"},
]}


class FakeNet:
    def __init__(self, sendlog=SENDLOG, replies=REPLIES):
        self.sendlog, self.replies, self.calls = sendlog, replies, []

    def __call__(self, path):
        self.calls.append(path)
        data = self.sendlog if path == am.SENDLOG_PATH else self.replies
        if isinstance(data, Exception):
            raise data
        return data


class Base(unittest.TestCase):
    def setUp(self):
        am.reset_cache()
        self.env = mock.patch.dict(os.environ, {"AUTOMAIL_BASE_URL": "https://automail.test", "AUTOMAIL_REFRESH_SECONDS": "180"})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.addCleanup(am.reset_cache)

    def patch_net(self, net):
        p = mock.patch.object(am, "_get_json", net)
        p.start()
        self.addCleanup(p.stop)
        return net


class AggregateTests(Base):
    def test_aggregate_and_normalize(self):
        self.patch_net(FakeNet())
        r = am.get_status()
        self.assertTrue(r["enabled"] and r["ok"] and r["replies_enabled"])
        p = r["pos"]["PO-20261030001"]
        self.assertEqual(p["sent_count"], 2)
        self.assertEqual(p["first_sent_at"], "2026-10-30T01:28:09.880Z")
        self.assertEqual(p["last_sent_at"], "2026-10-30T06:20:00.000Z")
        self.assertEqual(p["last_sent_local"], "10/30 14:20")
        self.assertEqual(p["to"], ["a@x.com"])
        self.assertEqual(p["vendor"], "甲")
        self.assertEqual((p["replied_count"], p["last_reply_local"], p["last_reply_from"], p["last_reply_subject"]),
                         (1, "10/30 15:00", "王", "Re: PO"))
        self.assertEqual(r["pos"]["PO-20261030002"]["last_sent_local"], "10/30 00:30")

    def test_taipei_formatting(self):
        self.assertEqual(am.local_text("2026-10-30T01:28:09.880Z"), "10/30 09:28")
        self.assertEqual(am.local_text("garbage"), "")
        self.assertEqual(am.normalize_po(" po-1 2\t"), "PO-12")

    def test_replies_independent_of_sendlog(self):
        for replies in (urllib.error.HTTPError("u", 404, "nf", {}, None), {"enabled": False}, OSError("boom")):
            am.reset_cache()
            self.patch_net(FakeNet(replies=replies))
            r = am.get_status()
            self.assertTrue(r["ok"], replies)
            self.assertEqual(r["pos"]["PO-20261030001"]["sent_count"], 2)
            self.assertEqual(r["pos"]["PO-20261030001"]["replied_count"], 0)
            self.assertFalse(r["replies_enabled"])

    def test_replies_error_message_kept_without_url(self):
        self.patch_net(FakeNet(replies=urllib.error.HTTPError("https://automail.test/api/replies", 404, "nf", {}, None)))
        r = am.get_status()
        self.assertIn("404", r["replies_error"])
        self.assertNotIn("http", r["replies_error"])

    def test_sendlog_failure_keeps_stale(self):
        net = self.patch_net(FakeNet())
        am.get_status()
        net.sendlog = OSError("down https://secret")
        r = am.get_status(force=True)
        self.assertFalse(r["ok"])
        self.assertTrue(r["error"] and "http" not in r["error"])
        self.assertTrue(r["last_error_at"])
        self.assertEqual(r["pos"]["PO-20261030001"]["sent_count"], 2)  # 沿用舊資料

    def test_first_failure_never_raises(self):
        self.patch_net(FakeNet(sendlog=OSError("x"), replies=OSError("y")))
        r = am.get_status()
        self.assertFalse(r["ok"])
        self.assertEqual(r["pos"], {})

    def test_ttl_and_force(self):
        net = self.patch_net(FakeNet())
        am.get_status()
        n = len(net.calls)
        am.get_status()
        self.assertEqual(len(net.calls), n)
        am.get_status(force=True)
        self.assertEqual(len(net.calls), n * 2)
        with mock.patch.object(am.time, "time", return_value=time.time() + 181):
            am.get_status()
        self.assertEqual(len(net.calls), n * 3)

    def test_min_refresh_seconds(self):
        with mock.patch.dict(os.environ, {"AUTOMAIL_REFRESH_SECONDS": "5"}):
            self.assertEqual(am.refresh_seconds(), 30)

    def test_disabled_makes_no_http_call(self):
        for value in ("", "off"):
            with mock.patch.dict(os.environ, {"AUTOMAIL_BASE_URL": value}), \
                    mock.patch("urllib.request.urlopen") as urlopen:
                r = am.get_status(force=True)
                urlopen.assert_not_called()
                self.assertFalse(r["enabled"])

    def test_po_filter_unknown_is_unsent(self):
        self.patch_net(FakeNet())
        r = am.get_status(pos=["po-20261030002", "PO-NOPE"])
        self.assertEqual(set(r["pos"]), {"PO-20261030002", "PO-NOPE"})
        self.assertFalse(r["pos"]["PO-NOPE"]["sent"])
        self.assertTrue(r["pos"]["PO-20261030002"]["sent"])


class ApiTests(Base):
    def setUp(self):
        super().setUp()
        os.environ.setdefault("PYTEST_CURRENT_TEST", "1")
        from main import app
        self.client = TestClient(app)
        self.patch_net(FakeNet())

    def test_endpoints(self):
        r = self.client.get("/api/automail/po-status")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()["pos"]), 2)
        r = self.client.get("/api/automail/po-status", params={"po": "po-20261030001, PO-NOPE"}).json()
        self.assertEqual(set(r["pos"]), {"PO-20261030001", "PO-NOPE"})
        self.assertFalse(r["pos"]["PO-NOPE"]["sent"])
        r = self.client.post("/api/automail/refresh")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["ok"])

    def test_refresh_path_exempt_from_edit_auth(self):
        from app.services.edit_auth import _EXEMPT_PATHS
        self.assertIn("/api/automail/refresh", _EXEMPT_PATHS)


class ReadOnlyTests(unittest.TestCase):
    def test_module_never_sends_mail_or_posts(self):
        src = (ROOT / "app" / "services" / "automail_status.py").read_text(encoding="utf-8")
        router = (ROOT / "app" / "routers" / "automail.py").read_text(encoding="utf-8")
        for text in (src, router):
            self.assertNotRegex(text, r"(?m)^\s*(import|from)\s+(smtplib|email\.mime|aiosmtplib)|\.sendmail\(|\.send_message\(")
        self.assertNotRegex(src, r'method\s*=\s*"(?!GET")')
        self.assertNotIn("data=", src)
        self.assertEqual(set(re.findall(r'"(/api/[a-z]+)"', src)), {"/api/sendlog", "/api/replies"})

    def test_requests_are_get_only(self):
        seen = []

        class Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return b"[]"

        def fake_urlopen(req, timeout=None):
            seen.append((req.get_method(), req.full_url, req.data))
            return Resp()

        with mock.patch.dict(os.environ, {"AUTOMAIL_BASE_URL": "https://automail.test"}), \
                mock.patch("urllib.request.urlopen", fake_urlopen):
            am.reset_cache()
            am.get_status(force=True)
            am.reset_cache()
        self.assertEqual(seen, [("GET", "https://automail.test/api/sendlog", None), ("GET", "https://automail.test/api/replies", None)])


class FrontendTests(unittest.TestCase):
    def test_module_imported_where_pos_listed(self):
        mods = ROOT / "static" / "modules"
        self.assertTrue((mods / "po_mail_status.js").exists())
        for name in ("erp_purchase.js", "shortage_po.js", "erp_receive.js"):
            self.assertIn('/static/modules/po_mail_status.js', (mods / name).read_text(encoding="utf-8"), name)

    def test_node_check_and_badge_html(self):
        mods = ROOT / "static" / "modules"
        for name in ("po_mail_status.js", "erp_purchase.js", "shortage_po.js", "erp_receive.js", "open_line_picker.js"):
            out = subprocess.run(["node", "--check", str(mods / name)], capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, f"{name}: {out.stderr}")
        script = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src = fs.readFileSync('static/modules/po_mail_status.js', 'utf8').replace(/^import .*$/m,
  'const esc = s => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;"); const apiJson = async () => ({}); const apiPost = apiJson;');
const m = await import('data:text/javascript;base64,' + Buffer.from(src).toString('base64'));
const st = { enabled: true, ok: true, pos: {
  'PO-1': { sent: true, sent_count: 2, last_sent_local: '10/30 14:20', to: ['a@x.com'], replied_count: 1, last_reply_local: '10/30 15:00', last_reply_from: '王', last_reply_subject: '<b>' },
  'PO-2': { sent: false } } };
let h = m.poMailBadgeHtml(' po-1 ', st);
assert.ok(h.includes('po-mail-sent') && h.includes('已寄 10/30 14:20') && h.includes('寄出 2 次') && h.includes('廠商已回 10/30') && h.includes('&lt;b&gt;'));
assert.ok(m.poMailBadgeHtml('PO-2', st).includes('未寄'));
assert.ok(m.poMailBadgeHtml('PO-9', st).includes('未寄'));
assert.equal(m.poMailBadgeHtml('PO-1', { enabled: false }), '');
assert.equal(m.poMailBadgeHtml('PO-1', { enabled: true, ok: false, pos: {} }), '');
"""
        out = subprocess.run(["node", "--input-type=module", "-e", script], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)


class PickerTests(unittest.TestCase):
    def test_picker_hides_other_groups(self):
        src = (ROOT / "static" / "modules" / "open_line_picker.js").read_text(encoding="utf-8")
        self.assertIn("visibleLines(lines, lockKey, opts.groupKey)", src)
        self.assertNotIn("olp-blocked", src)
        script = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
const src = fs.readFileSync('static/modules/open_line_picker.js', 'utf8').replace(/^import .*$/m,
  'const esc = s => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");');
const m = await import('data:text/javascript;base64,' + Buffer.from(src).toString('base64'));

const mk = () => { const e = { innerHTML: '', textContent: '', hidden: false, value: '', handlers: {}, classList: { add() {} },
  addEventListener(t, f) { this.handlers[t] = f; }, querySelector() { return null; } }; return e; };
const root = mk(), els = { '.olp-search': mk(), '.olp-lock': mk(), '.olp-count': mk(), thead: mk(), tbody: mk(), '.olp-unlock': mk(), '.olp-lock-text': mk() };
els['.olp-lock'].querySelector = () => els['.olp-lock-text'];
root.querySelector = sel => els[sel];
const lines = [
  { id: 'a1', v: 'A', n: '甲' }, { id: 'a2', v: 'A', n: '甲' }, { id: 'b1', v: 'B', n: '乙' }, { id: 'b2', v: 'B', n: '乙' }, { id: 'b3', v: 'B', n: '乙' },
];
const p = m.createOpenLinePicker(root, {
  fetchLines: async () => lines, lineKey: l => l.id, groupKey: l => l.v, groupLabel: l => l.n,
  columns: [{ label: 'x', render: l => l.id }], qtyOf: () => 1, otherGroupText: '已隱藏其他廠商 {n} 筆',
});
const rows = () => [...els.tbody.innerHTML.matchAll(/data-key="([^"]+)"/g)].map(x => x[1]);
const toggle = (key, checked) => els.tbody.handlers.change({ target: { checked, classList: { contains: c => c === 'olp-check' }, closest: () => ({ dataset: { key } }) } });
await p.reload();
assert.deepEqual(rows(), ['a1', 'a2', 'b1', 'b2', 'b3']);
assert.equal(els['.olp-count'].textContent, '');
toggle('a1', true);
assert.deepEqual(rows(), ['a1', 'a2']);
assert.ok(!els.tbody.innerHTML.includes('olp-blocked'));
assert.ok(els['.olp-count'].textContent.includes('已隱藏其他廠商 3 筆'), els['.olp-count'].textContent);
assert.equal(p.lockedGroup(), 'A');
toggle('a1', false);
assert.deepEqual(rows(), ['a1', 'a2', 'b1', 'b2', 'b3']);
assert.equal(p.lockedGroup(), null);
assert.equal(els['.olp-count'].textContent, '');
toggle('b2', true);
assert.deepEqual(rows(), ['b1', 'b2', 'b3']);
p.clear();
assert.deepEqual(rows(), ['a1', 'a2', 'b1', 'b2', 'b3']);
assert.deepEqual(m.visibleLines(lines, 'A', l => l.v).map(l => l.id), ['a1', 'a2']);
assert.equal(m.visibleLines(lines, null, l => l.v).length, 5);
"""
        out = subprocess.run(["node", "--input-type=module", "-e", script], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)

    def test_server_still_rejects_mixed_vendors(self):
        src = (ROOT / "tests" / "test_erp_receive.py").read_text(encoding="utf-8")
        self.assertIn("def test_vendor_lock", src)  # 該測試實際驗證後端拒絕混廠商
        self.assertIn("同一家廠商", (ROOT / "app" / "services" / "erp_receive.py").read_text(encoding="utf-8"))


def test_requests_send_explicit_user_agent(monkeypatch):
    """Cloudflare 擋 Python-urllib 預設 UA（403），必須帶自訂 User-Agent。"""
    import io, json as _json, urllib.request
    seen = []

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=None):
        seen.append(req)
        return _Resp(_json.dumps([]).encode())

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    am._get_json("/api/sendlog")
    assert seen and seen[0].get_method() == "GET"
    ua = seen[0].get_header("User-agent")
    assert ua and "urllib" not in ua.lower()
