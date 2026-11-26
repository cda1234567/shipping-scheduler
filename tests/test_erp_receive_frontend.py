"""一次收料前端：外幣採購單輸入發票匯率後一定重新預覽、顯示換算後單價 / 金額；預覽失敗不整塊消失。"""
from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "static"


# 共用：載入 erp_receive.js 到 vm、最小 DOM、假 apiPost
HARNESS = r"""
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
        for (const m of v.matchAll(/<(input|button|div|span|select)\b[^>]*\bid="([^"]+)"[^>]*>/g)) {
          const c = mk(m[2], m[1]);
          c.value = (m[0].match(/\bvalue="([^"]*)"/) || [])[1] ?? '';
          if (m[1] === 'select') {
            const body = v.slice(m.index).split('</select>')[0];
            c.options = [...body.matchAll(/<option value="([^"]*)"([^>]*)>([^<]*)<\/option>/g)]
              .map(o => ({ value: o[1], text: o[3], selected: /\sselected/.test(o[2]), disabled: /\sdisabled/.test(o[2]),
                title: (o[2].match(/title="([^"]*)"/) || [])[1] ?? '' }));
            c.value = (c.options.find(o => o.selected) || c.options[0] || {}).value ?? '';
          }
          c.disabled = /\sdisabled[\s>]/.test(m[0]);
          c.hidden = /\shidden[\s>]/.test(m[0]);
          c.checked = /\schecked[\s>]/.test(m[0]);
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
"""


def run_node(body: str) -> subprocess.CompletedProcess:
    return subprocess.run(["node", "--input-type=module", "-e", HARNESS + body], cwd=ROOT,
                          capture_output=True, text=True, encoding="utf-8")


class ErpReceiveRateRenderTests(unittest.TestCase):
    def test_rate_input_repreviews_with_node(self):
        script = r"""

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
        completed = run_node(script)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_usd_entry_ccy_choice_with_node(self):
        # v2026.11.16.5：USD 採購單可選打美金 / 打台幣；切換重新預覽並帶 entry_ccy
        script = r"""
// 打美金（foreign、免稅）的預覽：從線上 convert 那份改出來
const usd = {
  ...fx.with, mode: 'foreign', currency: 'USD', entry_ccy: 'USD', entry_ccy_default: 'NTD', entry_ccy_choosable: true,
  tax_type: 1, po_tax_type: 0, tax_label: '免稅（課稅別 1，稅額 0）',
  lines: fx.with.lines.map(l => ({ ...l, price: 6.6, amount: 19800, tax: 0, ml_price: 216.282, ml_amount: 648846 })),
  totals: { ...fx.with.totals, sum_amount: 19800, sum_tax: 0, total: 19800, local_amount: 648846, local_tax: 0, local_total: 648846 },
  warnings: [{ code: 'tax_free', message: '打美金：免稅，進貨單課稅別 1（採購單課稅別 0），稅額 0' }],
};
const ntd = { ...fx.with, entry_ccy: 'NTD', entry_ccy_default: 'NTD', entry_ccy_choosable: true };
const ntdNoRate = { ...fx.without, entry_ccy: 'NTD', entry_ccy_default: 'NTD', entry_ccy_choosable: true };
const pick = body => (body.entry_ccy === 'USD' ? usd : body.exch_rate ? ntd : ntdNoRate);

// v2026.11.26.1：USD 單有「單據幣別」下拉（NTD 台幣 / USD 美金），預設 NTD；首次預覽不帶 entry_ccy（用後端預設 NTD）
{
  const t = await setup(pick);
  await t.run('handlePreview()');
  const html = t.els['erp-receive-preview'].innerHTML;
  assert.ok(html.includes('<strong>單據幣別</strong>'), '下拉標籤');
  assert.ok(!html.includes('type="radio"'), '不再用 radio');
  const sel = t.els['erp-receive-ccy'];
  assert.equal(sel.tag, 'select');
  assert.deepEqual(sel.options.map(o => [o.value, o.text, o.disabled]), [['NTD', 'NTD 台幣', false], ['USD', 'USD 美金', false]]);
  assert.equal(sel.value, 'NTD');
  assert.equal(t.posts[0].entry_ccy, null);
  // 選台幣沒匯率：確認鈕鎖住，按了也不送 create
  assert.equal(t.els['btn-erp-receive-create'].disabled, true, '台幣沒匯率不能收料');
  await t.run('handleCreate()');
  assert.equal(t.posts.length, 1, '沒匯率不能送出');
  assert.ok(t.toasts.some(x => x.msg.includes('發票匯率')));
}

// 切到美金：重新預覽並帶 entry_ccy=USD，稅額顯示免稅 / 0；再切回台幣也重新預覽
{
  const t = await setup(pick);
  t.run('_rate = "32.77"; _rateVendor = "P0373";');
  await t.run('handlePreview()');
  t.els['erp-receive-ccy'].value = 'USD'; t.els['erp-receive-ccy'].fire('change');
  assert.equal(t.els['btn-erp-receive-create'].disabled, true, '切換後重新預覽前鎖住');
  await flush(); await flush();
  assert.deepEqual(t.posts.map(b => b.entry_ccy), [null, 'USD']);
  assert.deepEqual(t.posts.map(b => b.exch_rate), [32.77, 32.77], '匯率照帶');
  let html = t.els['erp-receive-preview'].innerHTML;
  assert.equal(t.els['erp-receive-ccy'].value, 'USD');
  assert.ok(html.includes('<strong>發票匯率</strong>（預帶正航幣別檔，可改）'), '美金匯率標籤');
  assert.ok(html.includes('免稅'), '要標明免稅');
  assert.ok(html.includes('課稅別 1'), '要標明課稅別 1');
  assert.ok(/稅 0（免稅）/.test(html), '合計稅額 0（免稅）');
  assert.ok(html.includes('USD 進貨單'));
  assert.equal(t.els['btn-erp-receive-create'].disabled, false);
  t.els['erp-receive-ccy'].value = 'NTD'; t.els['erp-receive-ccy'].fire('change');
  await flush(); await flush();
  assert.deepEqual(t.posts.map(b => b.entry_ccy), [null, 'USD', 'NTD']);
  html = t.els['erp-receive-preview'].innerHTML;
  assert.ok(html.includes('216.282') && html.includes('32,442'), '台幣：換算單價、稅 5%');
  assert.equal(t.els['erp-receive-ccy'].value, 'NTD');
  assert.ok(html.includes('<strong>換算匯率</strong>（預設正航幣別檔，可改成當日匯率）'), '台幣匯率標籤');
  assert.equal(t.run('buildBody().entry_ccy'), 'NTD');
}

// 預覽失敗重畫：選擇與匯率都不能被清掉
{
  const t = await setup(body => { if (body.entry_ccy === 'USD') throw new Error('逾時'); return pick(body); });
  t.run('_rate = "32.77"; _rateVendor = "P0373";');
  await t.run('handlePreview()');
  t.els['erp-receive-ccy'].value = 'USD'; t.els['erp-receive-ccy'].fire('change');
  await flush(); await flush();
  assert.ok(t.els['erp-receive-preview'].innerHTML.includes('預覽失敗：逾時'));
  assert.equal(t.els['erp-receive-ccy'].value, 'USD', '剛選的美金不能被清掉');
  assert.equal(t.els['erp-receive-rate'].value, '32.77');
  assert.equal(t.els['btn-erp-receive-create'].disabled, true);
}

// 換廠商：不沿用上一家的選擇（回到新單預設）
{
  const t = await setup(pick);
  t.run('_entryCcy = "USD"; _entryVendor = "P0373"; _entryChoosable = true;');
  assert.equal(t.run('buildBody().entry_ccy'), 'USD');
  t.run(`_picker.lockedGroup = () => 'P0304';`);
  assert.equal(t.run('buildBody().entry_ccy'), null);
}

// 台幣採購單（local）：同一個下拉固定 NTD，USD 選項停用（後端不支援台幣採購單開美金進貨單）；選了也不重新預覽
{
  const t = await setup(pick);
  t.run(`renderPreview(${JSON.stringify({ ...fx.with, mode: 'local', currency: 'NTD', po_currency: 'NTD' })})`);
  const html = t.els['erp-receive-preview'].innerHTML;
  const sel = t.els['erp-receive-ccy'];
  assert.equal(sel.value, 'NTD');
  assert.deepEqual(sel.options.map(o => [o.value, o.disabled, o.title]),
    [['NTD', false, ''], ['USD', true, '台幣採購單不能開美金進貨單']]);
  assert.ok(!html.includes('erp-receive-price-ntd') && !html.includes('erp-receive-rate'), '台幣單沒有單價是台幣 / 匯率');
  sel.value = 'USD'; sel.fire('change');
  await flush();
  assert.equal(t.posts.length, 0, '台幣單改下拉不送預覽');
}
"""
        completed = run_node(script)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_price_ntd_checkbox_with_node(self):
        # v2026.11.20.14：USD 採購單打台幣時可勾「單價是台幣」：不用匯率、確認鈕不鎖、送 price_ntd
        script = r"""
const LABEL = '\u55ae\u50f9\u662f\u53f0\u5e63\uff08\u73fe\u8ca8\u5831\u50f9\u53f0\u5e63\uff0c\u4e0d\u7528\u532f\u7387\uff09';
const ntdNoRate = { ...fx.without, entry_ccy: 'NTD', entry_ccy_default: 'NTD', entry_ccy_choosable: true, price_ntd: false };
const ntdPrice = {
  ...fx.with, entry_ccy: 'NTD', entry_ccy_default: 'NTD', entry_ccy_choosable: true, price_ntd: true,
  exch_rate: 1, rate_source: 'ntd_price', rate_required: false,
  lines: fx.with.lines.map(l => ({ ...l, price: 8.5, src_price: 6.6, amount: 25500, tax: 1275, ml_price: 8.5, ml_amount: 25500 })),
  totals: { ...fx.with.totals, sum_amount: 25500, sum_tax: 1275, total: 26775, local_amount: 25500, local_tax: 1275, local_total: 26775 },
  warnings: [{ code: 'price_ntd', message: '單價直接輸入台幣' }],
};
const usd = { ...fx.with, mode: 'foreign', currency: 'USD', entry_ccy: 'USD', entry_ccy_default: 'NTD', entry_ccy_choosable: true, tax_type: 1 };
const pick = body => (body.entry_ccy === 'USD' ? usd : body.price_ntd ? ntdPrice : ntdNoRate);

// USD + 打台幣：有勾選框；勾了重新預覽、帶 price_ntd、不要匯率，確認鈕可按，送出不被擋
{
  const t = await setup(pick);
  t.run(`_picker.clear = () => {}; _picker.reload = async () => {}; window.confirm = m => { globalThis._msg = m; return true; };`);
  await t.run('handlePreview()');
  let html = t.els['erp-receive-preview'].innerHTML;
  assert.ok(html.includes('id="erp-receive-price-ntd"'), '台幣要有勾選框');
  assert.ok(html.includes(LABEL), '勾選框文字');
  assert.equal(t.els['erp-receive-price-ntd'].checked, false);
  assert.equal(t.els['btn-erp-receive-create'].disabled, true, '沒勾、沒匯率仍鎖住');
  const box = t.els['erp-receive-price-ntd'];
  box.checked = true; box.fire('change');
  assert.equal(t.els['btn-erp-receive-create'].disabled, true, '重新預覽前鎖住');
  await flush(); await flush();
  assert.deepEqual(t.posts.map(b => b.price_ntd), [false, true]);
  assert.equal(t.posts[1].exch_rate, null);
  html = t.els['erp-receive-preview'].innerHTML;
  assert.ok(!html.includes('id="erp-receive-rate"'), '勾了就不顯示匯率欄');
  assert.ok(html.includes('單價（NTD，直接打台幣）') && html.includes('採購單單價（USD）'), '欄位標題');
  assert.ok(html.includes('8.5') && html.includes('26,775'));
  assert.equal(t.els['erp-receive-price-ntd'].checked, true);
  assert.equal(t.els['btn-erp-receive-create'].disabled, false, '不用匯率也能收料');
  const body = t.run('buildBody()');
  assert.equal(body.price_ntd, true);
  await t.run('handleCreate()');
  assert.equal(t.posts.length, 3, '送出 create 不被匯率擋');
  assert.equal(t.posts[2].price_ntd, true);
  assert.ok(!t.toasts.some(x => x.msg.includes('發票匯率')), JSON.stringify(t.toasts));
  assert.ok(t.run('globalThis._msg').includes('單價是台幣，不用匯率'));
}

// 切到打美金：勾選框不出現、price_ntd 不送；換廠商也不沿用
{
  const t = await setup(pick);
  await t.run('handlePreview()');
  t.els['erp-receive-price-ntd'].checked = true; t.els['erp-receive-price-ntd'].fire('change');
  await flush(); await flush();
  t.els['erp-receive-ccy'].value = 'USD'; t.els['erp-receive-ccy'].fire('change');
  await flush(); await flush();
  assert.equal(t.posts.at(-1).entry_ccy, 'USD');
  assert.equal(t.posts.at(-1).price_ntd, false);
  assert.ok(!t.els['erp-receive-preview'].innerHTML.includes('erp-receive-price-ntd'), '美金不出現勾選框');
  t.els['erp-receive-ccy'].value = 'NTD'; t.els['erp-receive-ccy'].fire('change');
  await flush(); await flush();
  assert.equal(t.posts.at(-1).price_ntd, true, '切回台幣保留勾選');
  t.run(`_picker.lockedGroup = () => 'P0304';`);
  assert.equal(t.run('buildBody().price_ntd'), false);
}

// 台幣採購單：沒有勾選框
{
  const t = await setup(pick);
  t.run(`renderPreview(${JSON.stringify({ ...fx.with, mode: 'local', currency: 'NTD', po_currency: 'NTD' })})`);
  assert.ok(!t.els['erp-receive-preview'].innerHTML.includes('erp-receive-price-ntd'));
}
"""
        completed = run_node(script)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_entry_choices_reset_per_receipt_with_node(self):
        # v2026.11.24.8：「單價是台幣」、打美金 / 台幣、匯率不跨收料沿用（換明細、收料後、載入頁面都回預設）
        script = r"""
const ntdNoRate = { ...fx.without, entry_ccy: 'NTD', entry_ccy_default: 'NTD', entry_ccy_choosable: true, price_ntd: false };
const ntdRate = { ...fx.with, entry_ccy: 'NTD', entry_ccy_default: 'NTD', entry_ccy_choosable: true, price_ntd: false };
const ntdPrice = { ...fx.with, entry_ccy: 'NTD', entry_ccy_default: 'NTD', entry_ccy_choosable: true, price_ntd: true,
  exch_rate: 1, rate_source: 'ntd_price', rate_required: false };
// 台幣採購單（PO-20261029002）：後端不讓選、price_ntd 一律 false
const local = { ...fx.with, mode: 'local', currency: 'NTD', po_currency: 'NTD', entry_ccy: null, entry_ccy_default: null,
  entry_ccy_choosable: false, price_ntd: false, exch_rate: 1, rate_required: false };
const pick = body => (body.lines[0].po_no === 'PO-20261029002' ? local
  : body.price_ntd ? ntdPrice : body.exch_rate ? ntdRate : ntdNoRate);
const L = (po, row, qty = 100, price = 6.6) => ({ line: { po_no: po, row_no: row }, qty, price });
const USD_A = L('PO-20260904003', 1), USD_B = L('PO-20260904003', 2), NTD = L('PO-20261029002', 2);

// 用假 picker 跑真的 initErpReceive，onChange 走實際接線
async function boot() {
  const t = await setup(pick);
  t.els['btn-erp-receive-preview'] = { addEventListener() {} };
  t.run(`
    globalThis._sel = []; globalThis._lock = null; globalThis._confirms = [];
    globalThis.decoratePoMailBadges = () => {}; globalThis.poMailSlot = () => '';
    globalThis.createOpenLinePicker = (root, opts) => { globalThis._opts = opts; return {
      getSelection: () => globalThis._sel, lockedGroup: () => globalThis._lock,
      clear() { globalThis._sel = []; globalThis._lock = null; opts.onChange([]); },
      reload: async () => {} }; };
    window.confirm = m => { globalThis._confirms.push(m); return true; };
  `);
  await t.run('initErpReceive()');
  t.els['erp-receive-date'].value = '2026-11-09';  // init 會填今天；對齊 fixture 的發票日期
  t.select = (sel, lock = 'P0304') => { t.run(`globalThis._sel = ${JSON.stringify(sel)}; globalThis._lock = ${JSON.stringify(lock)}; globalThis._opts.onChange(globalThis._sel);`); };
  t.state = () => JSON.parse(t.run('JSON.stringify({ r: _rate, rv: _rateVendor, c: _entryCcy, cv: _entryVendor, p: _priceNtd, pv: _priceNtdVendor })'));
  return t;
}
const DEFAULTS = { r: '', rv: null, c: '', cv: null, p: false, pv: null };

// 勾「單價是台幣」收料成功 → 同廠商挑下一張單：下一次預覽 price_ntd false、entry_ccy null、exch_rate null
{
  const t = await boot();
  t.select([USD_A]);
  await t.run('handlePreview()');
  const box = t.els['erp-receive-price-ntd'];
  box.checked = true; box.fire('change');
  await flush(); await flush();
  assert.equal(t.posts.at(-1).price_ntd, true);
  await t.run('handleCreate()');
  assert.equal(t.run('globalThis._confirms.length'), 1, '確認收料要開確認視窗');
  assert.equal(t.posts.at(-1).price_ntd, true, 'create 帶 price_ntd');
  assert.deepEqual(t.state(), DEFAULTS, '收料成功後清回預設');
  t.select([NTD]);
  await t.run('handlePreview()');
  const b = t.posts.at(-1);
  assert.deepEqual([b.price_ntd, b.entry_ccy, b.exch_rate], [false, null, null]);
  // 台幣單：確認收料直接開確認視窗，不出現「已改單價是台幣」重新預覽
  const before = t.posts.length;
  await t.run('handleCreate()');
  assert.equal(t.run('globalThis._confirms.length'), 2, '台幣單要開確認視窗');
  assert.equal(t.posts.length, before + 1, '直接送 create，不重新預覽');
  assert.ok(!t.toasts.some(x => x.msg.includes('單價是台幣')), JSON.stringify(t.toasts));
}

// 同廠商換別的明細：選擇清回預設
{
  const t = await boot();
  t.select([USD_A]);
  t.run('_rate = "32.77"; _rateVendor = "P0304"; _entryCcy = "NTD"; _entryVendor = "P0304"; _priceNtd = true; _priceNtdVendor = "P0304"; _entryChoosable = true;');
  t.select([USD_A, USD_B]);
  assert.deepEqual(t.state(), DEFAULTS, '加勾一筆');
  t.run('_rate = "32.77"; _rateVendor = "P0304"; _priceNtd = true; _priceNtdVendor = "P0304"; _entryChoosable = true;');
  t.select([USD_B]);
  assert.deepEqual(t.state(), DEFAULTS, '換成另一筆');
  const b = t.run('buildBody()');
  assert.deepEqual([b.price_ntd, b.entry_ccy, b.exch_rate], [false, null, null]);
}

// 換廠商再換回來：不會恢復舊選擇
{
  const t = await boot();
  t.select([USD_A]);
  t.run('_rate = "32.77"; _rateVendor = "P0304"; _entryCcy = "NTD"; _entryVendor = "P0304"; _priceNtd = true; _priceNtdVendor = "P0304"; _entryChoosable = true;');
  t.select([]);
  t.select([L('PO-20260612001', 1)], 'P0373');
  t.select([USD_A]);
  assert.deepEqual(t.state(), DEFAULTS);
  const b = t.run('buildBody()');
  assert.deepEqual([b.price_ntd, b.entry_ccy, b.exch_rate], [false, null, null]);
}

// 同一批明細只改數量 / 單價：剛打的匯率保留
{
  const t = await boot();
  t.select([USD_A]);
  await t.run('handlePreview()');
  const rate = t.els['erp-receive-rate'];
  rate.value = '32.77'; rate.fire('change');
  await flush(); await flush();
  assert.equal(t.posts.at(-1).exch_rate, 32.77);
  t.select([L('PO-20260904003', 1, 50, 7.1)]);
  assert.equal(t.run('_rate'), '32.77', '改數量不清匯率');
  assert.equal(t.run('buildBody().exch_rate'), 32.77);
  assert.equal(t.run('_entryChoosable'), true, '同一批明細仍可選');
}

// 防呆：就算殘留舊勾選，台幣單（entry_ccy_choosable false）也不送 price_ntd、確認收料不會重新預覽迴圈
{
  const t = await boot();
  t.select([NTD]);
  t.run('_priceNtd = true; _priceNtdVendor = "P0304"; _entryCcy = "NTD"; _entryVendor = "P0304"; _entryChoosable = true;');
  await t.run('handlePreview()');
  assert.equal(t.run('buildBody().price_ntd'), false, '台幣單預覽後不送 price_ntd');
  assert.equal(t.run('buildBody().entry_ccy'), null, '台幣單預覽後不送 entry_ccy');
  await t.run('handleCreate()');
  assert.equal(t.run('globalThis._confirms.length'), 1, '直接開確認視窗');
  assert.ok(!t.toasts.some(x => x.msg.includes('單價是台幣')), JSON.stringify(t.toasts));
}

// 載入頁面 / 切回分頁：回到預設
{
  const t = await boot();
  assert.deepEqual(t.state(), DEFAULTS, 'init 後是預設');
  t.select([USD_A]);
  t.run('_rate = "32.77"; _rateVendor = "P0304"; _entryCcy = "USD"; _entryVendor = "P0304"; _priceNtd = true; _priceNtdVendor = "P0304"; _entryChoosable = true;');
  await t.run('onErpReceiveTabActivated()');
  assert.deepEqual(t.state(), DEFAULTS, '切回分頁後是預設');
  assert.equal(t.run('_entryChoosable'), false);
  const b = t.run('buildBody()');
  assert.deepEqual([b.price_ntd, b.entry_ccy, b.exch_rate], [false, null, null]);
}
"""
        completed = run_node(script)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_currency_default_rate_with_node(self):
        # v2026.11.26.1：匯率預帶正航幣別檔 BuyInRate（rate_source currency_default），不再是 0 / 鎖住；打了就改送 manual
        script = r"""
const dflt = { ...fx.with, exch_rate: 30.385, rate_source: 'currency_default', rate_required: false,
  entry_ccy: 'NTD', entry_ccy_default: 'NTD', entry_ccy_choosable: true, price_ntd: false };
const usdDflt = { ...dflt, mode: 'foreign', currency: 'USD', entry_ccy: 'USD', tax_type: 4, po_tax_type: 4 };
const pick = body => {
  const base = body.entry_ccy === 'USD' ? usdDflt : dflt;
  return body.exch_rate ? { ...base, exch_rate: body.exch_rate, rate_source: 'manual' } : base;
};

// 預設 NTD、匯率欄預帶 30.385、確認鈕可按；沒改匯率直接收料，送 exch_rate null（後端同樣預帶）
{
  const t = await setup(pick);
  t.run(`_picker.clear = () => {}; _picker.reload = async () => {}; window.confirm = m => { globalThis._msg = m; return true; };`);
  await t.run('handlePreview()');
  assert.equal(t.els['erp-receive-ccy'].value, 'NTD');
  assert.equal(t.els['erp-receive-rate'].value, '30.385');
  const html = t.els['erp-receive-preview'].innerHTML;
  assert.ok(!html.includes('請輸入發票上的匯率'));
  assert.equal(t.els['btn-erp-receive-create'].disabled, false, '預帶匯率不鎖確認鈕');
  await t.run('handleCreate()');
  assert.equal(t.posts.length, 2, '直接送 create');
  assert.deepEqual([t.posts[1].exch_rate, t.posts[1].entry_ccy], [null, null]);
  assert.ok(!t.toasts.some(x => x.msg.includes('發票匯率')), JSON.stringify(t.toasts));
  assert.ok(t.run('globalThis._msg').includes('單據幣別 NTD 台幣'));
}

// 選 USD：同一批明細保留選擇，匯率照預帶；使用者打的匯率優先
{
  const t = await setup(pick);
  await t.run('handlePreview()');
  const sel = t.els['erp-receive-ccy'];
  sel.value = 'USD'; sel.fire('change');
  await flush(); await flush();
  assert.equal(t.els['erp-receive-ccy'].value, 'USD');
  assert.equal(t.els['erp-receive-rate'].value, '30.385');
  const rate = t.els['erp-receive-rate'];
  rate.value = '32.5'; rate.fire('change');
  await flush(); await flush();
  assert.deepEqual(t.posts.map(b => [b.entry_ccy, b.exch_rate]), [[null, null], ['USD', null], ['USD', 32.5]]);
  assert.equal(t.els['erp-receive-ccy'].value, 'USD', '同一批明細選擇不跳回 NTD');
  assert.equal(t.els['erp-receive-rate'].value, '32.5');
  // 換一批明細：回到預設 NTD
  t.run('resetEntryChoices()');
  assert.equal(t.run('buildBody().entry_ccy'), null);
}
"""
        completed = run_node(script)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_module_syntax(self):
        for name in ("erp_receive.js", "open_line_picker.js"):
            completed = subprocess.run(["node", "--check", str(STATIC / "modules" / name)], capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
