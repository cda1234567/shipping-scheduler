"""一次收料前端：外幣採購單輸入發票匯率後一定重新預覽、顯示換算後單價 / 金額；預覽失敗不整塊消失。"""
from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "static"


class ErpReceiveRateRenderTests(unittest.TestCase):
    def test_rate_input_repreviews_with_node(self):
        script = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { esc } from './static/modules/api.js';

const fx = JSON.parse(fs.readFileSync('./tests/fixtures/erp_receive_preview_usd_convert.json', 'utf8'));
const source = fs.readFileSync('./static/modules/erp_receive.js', 'utf8')
  .replace(/^import .*;\r?\n/gm, '')
  .replace(/^export /gm, '');

// 最小 DOM：getElementById + innerHTML 內有 id 的 input / button / div / span 會建立元素
function makeDom() {
  const els = {};
  function mk(id, tag = 'div') {
    return { id, tag, hidden: false, value: '', disabled: false, title: '', textContent: '', _h: {}, _html: '',
      addEventListener(type, fn) { (this._h[type] ||= []).push(fn); },
      fire(type, extra = {}) { let prevented = false; for (const fn of this._h[type] || []) fn({ target: this, preventDefault() { prevented = true; }, ...extra }); return prevented; },
      get innerHTML() { return this._html; },
      set innerHTML(v) {
        this._html = v;
        for (const m of v.matchAll(/<(input|button|div|span)\b[^>]*\bid="([^"]+)"[^>]*>/g)) {
          const c = mk(m[2], m[1]);
          c.value = (m[0].match(/\bvalue="([^"]*)"/) || [])[1] ?? '';
          c.disabled = /\sdisabled[\s>]/.test(m[0]);
          c.hidden = /\shidden[\s>]/.test(m[0]);
          els[m[2]] = c;
        }
      } };
  }
  for (const id of ['erp-receive-preview', 'erp-receive-result', 'erp-receive-date']) els[id] = mk(id);
  els['erp-receive-date'].value = '2026-11-09';
  return { els, mk };
}

async function setup(respond) {
  const { els } = makeDom();
  const toasts = [], posts = [];
  const ctx = vm.createContext({
    console: { ...console, error() {} }, esc, crypto: globalThis.crypto,
    document: { getElementById: id => els[id] || null },
    showToast: (msg, opts) => toasts.push({ msg, opts }), isEditAuthenticated: () => true, openEditAuthModal() {},
    apiPost: async (path, body) => { posts.push(body); return respond(body); }, apiJson: async () => ({}), window: {},
  });
  vm.runInContext(source, ctx);
  vm.runInContext(`_picker = { getSelection: () => [{ line: { po_no: 'PO-20260612001', row_no: 1 }, qty: 3000, price: 6.6 }], lockedGroup: () => 'P0373' };`, ctx);
  return { els, toasts, posts, ctx, run: code => vm.runInContext(code, ctx) };
}
const flush = () => new Promise(r => setTimeout(r, 0));

// 線上那份 with JSON 直接 render 不丟例外，換算後單價 / 金額都在畫面上
{
  const t = await setup(() => fx.with);
  t.run('_rate = "32.77"; _rateVendor = "P0373";');
  t.run(`renderPreview(${JSON.stringify(fx.with)})`);
  const html = t.els['erp-receive-preview'].innerHTML;
  assert.equal(t.els['erp-receive-preview'].hidden, false);
  for (const s of ['216.282', '648,846', '681,288', '原單價（USD）', '6.6']) assert.ok(html.includes(s), s);
  assert.equal(t.els['btn-erp-receive-create'].disabled, false);
  assert.equal(t.els['erp-receive-rate'].value, '32.77');
}

// 沒匯率 → 輸入 32.77 按 Enter（之後 blur 又觸發 change）：只重新預覽一次，畫面顯示換算結果，確認鈕可按
for (const how of ['enter', 'change']) {
  const t = await setup(body => (body.exch_rate ? fx.with : fx.without));
  await t.run('handlePreview()');
  assert.equal(t.els['btn-erp-receive-create'].disabled, true, '沒匯率不能收料');
  assert.equal(t.els['erp-receive-rate'].value, '');
  const rate = t.els['erp-receive-rate'];
  rate.value = '32.77';
  rate.fire('input');
  assert.equal(t.els['btn-erp-receive-create'].disabled, true, '改了匯率還沒重新預覽前鎖住');
  assert.equal(t.els['erp-receive-rate-tip'].hidden, false, '提示按 Enter');
  if (how === 'enter') {
    assert.equal(rate.fire('keydown', { key: 'Enter' }), true, 'Enter 要 preventDefault');
    rate.fire('change');  // 瀏覽器 Enter / blur 也會送 change，不能重複預覽
  } else {
    rate.fire('change');
  }
  assert.equal(t.els['btn-erp-receive-create'].textContent, '重新預覽中…');
  await flush(); await flush();
  assert.deepEqual(t.posts.map(b => b.exch_rate), [null, 32.77], how);
  const html = t.els['erp-receive-preview'].innerHTML;
  assert.equal(t.els['erp-receive-preview'].hidden, false, how);
  assert.ok(html.includes('216.282') && html.includes('648,846'), how);
  assert.equal(t.els['btn-erp-receive-create'].disabled, false, how);
  assert.equal(t.els['erp-receive-rate'].value, '32.77');
  assert.equal(t.toasts.length, 0, JSON.stringify(t.toasts));
}

// 預覽中又改匯率：排隊重跑，不會被 _busy 吃掉
{
  let release;
  const t = await setup(body => (body.exch_rate === 30 ? new Promise(r => { release = () => r(fx.with); }) : body.exch_rate ? fx.with : fx.without));
  await t.run('handlePreview()');
  const rate = t.els['erp-receive-rate'];
  rate.value = '30'; rate.fire('change');
  rate.value = '32.77'; rate.fire('change');
  release(); await flush(); await flush(); await flush();
  assert.deepEqual(t.posts.map(b => b.exch_rate), [null, 30, 32.77]);
}

// 預覽失敗：畫面不消失，預覽區看得到錯誤，匯率保留，確認鈕鎖住
{
  const t = await setup(body => { if (body.exch_rate) throw new Error('正航<連線>逾時'); return fx.without; });
  await t.run('handlePreview()');
  const rate = t.els['erp-receive-rate'];
  rate.value = '32.77'; rate.fire('change');
  await flush(); await flush();
  const p = t.els['erp-receive-preview'];
  assert.equal(p.hidden, false, '失敗不能整塊清掉');
  assert.ok(p.innerHTML.includes('預覽失敗：正航&lt;連線&gt;逾時'));
  assert.equal(t.els['btn-erp-receive-create'].disabled, true);
  assert.equal(t.els['erp-receive-rate'].value, '32.77', '剛輸入的匯率不能被清掉');
  assert.ok(t.toasts.some(x => x.msg.includes('預覽失敗') && x.opts.sticky));
  assert.equal(t.run('_preview'), null, '失敗後不能拿舊預覽收料');
  // 第一次就失敗（沒有上一份）也要顯示錯誤
  const t2 = await setup(() => { throw new Error('HTTP 500'); });
  await t2.run('handlePreview()');
  assert.equal(t2.els['erp-receive-preview'].hidden, false);
  assert.ok(t2.els['erp-receive-preview'].innerHTML.includes('預覽失敗：HTTP 500'));
}

// 匯率 0 / 空白：不送預覽，提示錯誤
{
  const t = await setup(body => (body.exch_rate ? fx.with : fx.without));
  await t.run('handlePreview()');
  const rate = t.els['erp-receive-rate'];
  rate.value = '0'; rate.fire('keydown', { key: 'Enter' });
  await flush();
  assert.equal(t.posts.length, 1);
  assert.ok(t.toasts.some(x => x.msg.includes('發票匯率必須大於 0')));
}
"""
        completed = subprocess.run(["node", "--input-type=module", "-e", script], cwd=ROOT,
                                   capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_module_syntax(self):
        for name in ("erp_receive.js", "open_line_picker.js"):
            completed = subprocess.run(["node", "--check", str(STATIC / "modules" / name)], capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
