"""開 Invoice 匯率：預覽自動帶入 S/C / 幣別檔匯率、「按 Enter 重新計算」只在改過匯率後才出現（v2026.11.24.9）。"""
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
vm.runInContext(source + '\n_picker = { lockedGroup: () => "C01" };\nthis.renderPreview = renderPreview;', ctx);

const preview = over => ({ bill_no: 'ST-2026001', bill_prefix: 'ST-2026', customer: { id: 'C01', name: 'ACME' },
  sc_nos: ['MO-2611009'], inv_date: 20261124, currency: 'USD', lines: [], totals: { sum_qty: 0, total: 0, local_total: 0 },
  warnings: [], is_production: false, target_label: '公司13（測試）', ...over });
const html = () => els['erp-invoice-preview'].innerHTML;
const rateValue = () => html().match(/id="erp-invoice-rate"[^>]*value="([^"]*)"/)[1];
const tipTag = () => html().match(/<span id="erp-invoice-rate-tip"[^>]*>/)[0];
"""


def run_node(body: str) -> subprocess.CompletedProcess:
    return subprocess.run(["node", "--input-type=module", "-e", HARNESS + body], cwd=ROOT,
                          capture_output=True, text=True, encoding="utf-8")


class InvoiceRateFrontendTests(unittest.TestCase):
    def _run(self, body: str):
        completed = run_node(body)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_sc_rate_prefilled_and_tip_hidden_until_input(self):
        self._run(r"""
ctx.renderPreview(preview({ exch_rate: 32.625, rate_source: 'sc', rate_required: false, rate_hint: { sc: 32.625, currency: 30.385 } }));
assert.equal(rateValue(), '32.625');
assert.match(tipTag(), /\shidden[\s>]/);
assert.doesNotMatch(tipTag(), /style=/);                // 不能有 inline display 蓋掉 hidden
assert.doesNotMatch(html(), /id="erp-invoice-rate"[^>]*placeholder=/);
assert.ok(html().includes('已自動帶入第一張 S/C 匯率 32.625，不用輸入'));
assert.ok(html().includes('正航幣別檔 30.385（參考）'));
assert.ok(!html().includes('請輸入發票上的匯率'));
const tip = document.getElementById('erp-invoice-rate-tip');
tip.hidden = true;                                       // 對應 HTML 上的 hidden 屬性
els['erp-invoice-rate'].fire('input');
assert.equal(tip.hidden, false);
assert.notEqual(tip.style.display, 'none');
assert.equal(els['btn-erp-invoice-create'].disabled, true);
""")

    def test_currency_fallback_prefills_currency_rate(self):
        self._run(r"""
ctx.renderPreview(preview({ exch_rate: 30.385, rate_source: 'currency', rate_required: false, rate_hint: { sc: null, currency: 30.385 } }));
assert.equal(rateValue(), '30.385');
assert.match(tipTag(), /\shidden[\s>]/);
assert.ok(html().includes('S/C 沒有匯率，已自動帶入正航幣別檔 30.385，不用輸入'));
""")

    def test_missing_rate_asks_for_input(self):
        self._run(r"""
ctx.renderPreview(preview({ exch_rate: 0, rate_source: 'missing', rate_required: true, rate_hint: { sc: null, currency: null } }));
assert.equal(rateValue(), '');
assert.match(html(), /id="erp-invoice-rate"[^>]*placeholder="照發票填"/);
assert.ok(html().includes('請輸入發票上的匯率'));
assert.ok(!html().includes('不用輸入'));
""")


if __name__ == "__main__":
    unittest.main()
