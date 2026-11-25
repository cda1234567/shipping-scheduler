"""開 Invoice 匯率：預覽只唯讀顯示自動帶入的匯率，沒有匯率輸入框、也不要求使用者輸入（v2026.11.25.1）。"""
from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# 共用：把 erp_invoice.js 載入 vm、最小 DOM（依 id 取元素）、假 picker
HARNESS = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { esc } from './static/modules/api.js';

const source = fs.readFileSync('./static/modules/erp_invoice.js', 'utf8')
  .replace(/^import .*;\r?\n/gm, '')
  .replace(/^export /gm, '');

function mkEl() {
  return { hidden: false, disabled: false, title: '', value: '', innerHTML: '', style: {}, _h: {},
    addEventListener(type, fn) { (this._h[type] ||= []).push(fn); },
    fire(type, event = {}) { for (const fn of this._h[type] || []) fn(event); } };
}
const els = {};
const document = { getElementById: id => (els[id] ||= mkEl()) };
const ctx = vm.createContext({ esc, document, showToast() {}, isEditAuthenticated: () => true,
  openEditAuthModal() {}, createOpenLinePicker() {}, apiJson() {}, apiPost() {}, window: {}, console });
vm.runInContext(source + '\n_picker = { lockedGroup: () => "C01", getSelection: () => [{ line: { sc_no: "MO-2611009", row_no: 1 }, qty: 1, price: 1 }] };'
  + '\n_config = { target: null };\nthis.renderPreview = renderPreview;\nthis.buildBody = buildBody;', ctx);

const preview = over => ({ bill_no: 'ST-2026001', bill_prefix: 'ST-2026', customer: { id: 'C01', name: 'ACME' },
  sc_nos: ['MO-2611009'], inv_date: 20261124, currency: 'USD', lines: [], totals: { sum_qty: 0, total: 0, local_total: 0 },
  warnings: [], is_production: false, target_label: '公司13（測試）', ...over });
const html = () => els['erp-invoice-preview'].innerHTML;
const noInput = () => {
  assert.doesNotMatch(html(), /id="erp-invoice-rate"/);
  assert.doesNotMatch(html(), /<input[^>]*type="number"/);
  assert.ok(!html().includes('請輸入'), html());
  assert.ok(!html().includes('照發票填'));
  assert.ok(!html().includes('按 Enter 重新計算'));
};
"""


def run_node(body: str) -> subprocess.CompletedProcess:
    return subprocess.run(["node", "--input-type=module", "-e", HARNESS + body], cwd=ROOT,
                          capture_output=True, text=True, encoding="utf-8")


class InvoiceRateFrontendTests(unittest.TestCase):
    def _run(self, body: str):
        completed = run_node(body)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_first_sc_rate_read_only(self):
        self._run(r"""
ctx.renderPreview(preview({ exch_rate: 32.625, rate_source: 'sc', rate_required: false,
  rate_hint: { sc: 32.625, currency: 30.385, sc_no: 'MO-2611009' } }));
noInput();
assert.ok(html().includes('匯率 <span class="mono" id="erp-invoice-rate-text">32.625</span>'));
assert.ok(html().includes('（自動帶第一張 S/C MO-2611009）'));
assert.equal(els['btn-erp-invoice-create'].disabled, false);
""")

    def test_other_and_latest_sc_and_currency_text(self):
        self._run(r"""
ctx.renderPreview(preview({ exch_rate: 31.505, rate_source: 'sc_other', rate_required: false,
  rate_hint: { sc: null, currency: 30.385, sc_no: 'MO-2611010' } }));
noInput();
assert.ok(html().includes('（第一張 S/C 沒有匯率，自動帶 S/C MO-2611010 的匯率 31.505）'));
ctx.renderPreview(preview({ exch_rate: 31.505, rate_source: 'sc_latest', rate_required: false,
  rate_hint: { sc: null, currency: 30.385, sc_no: 'MO-2611001' } }));
noInput();
assert.ok(html().includes('（S/C 沒有匯率，自動帶最近一張美金 S/C MO-2611001 的匯率 31.505）'));
ctx.renderPreview(preview({ exch_rate: 30.385, rate_source: 'currency', rate_required: false,
  rate_hint: { sc: null, currency: 30.385, sc_no: null } }));
noInput();
assert.ok(html().includes('（自動帶正航幣別檔 30.385）'));
""")

    def test_missing_rate_does_not_ask_user_to_type(self):
        self._run(r"""
ctx.renderPreview(preview({ exch_rate: 0, rate_source: 'missing', rate_required: true,
  rate_hint: { sc: null, currency: null, sc_no: null } }));
noInput();
assert.ok(html().includes('找不到匯率，請聯絡系統管理員'));
""")

    def test_preview_request_never_sends_rate(self):
        self._run(r"""
document.getElementById('erp-invoice-date').value = '2026-11-25';
assert.equal(ctx.buildBody().exch_rate, null);
""")


if __name__ == "__main__":
    unittest.main()
