"""未結明細挑選器：鎖定 / 解除時重抓一次、勾選與改過的數量單價保留、舊的 reload 結果丟掉（v2026.11.24.5）。"""
from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# 共用：把 open_line_picker.js 載入 vm、最小 DOM、可控制回應時機的假 fetchLines
HARNESS = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { esc } from './static/modules/api.js';

const source = fs.readFileSync('./static/modules/open_line_picker.js', 'utf8')
  .replace(/^import .*;\r?\n/gm, '')
  .replace(/^export /gm, '');
const ctx = vm.createContext({ esc, setTimeout, clearTimeout, console });
vm.runInContext(source + '\nthis.createOpenLinePicker = createOpenLinePicker;', ctx);

function mkEl() {
  return { hidden: false, value: '', textContent: '', innerHTML: '', _h: {},
    classList: { add() {}, contains: () => false },
    addEventListener(type, fn) { (this._h[type] ||= []).push(fn); },
    fire(type, event) { for (const fn of this._h[type] || []) fn(event); },
    querySelector() { return mkEl(); } };
}
function mkRoot() {
  const parts = {};
  for (const sel of ['.olp-search', '.olp-lock', '.olp-count', 'thead', 'tbody', '.olp-msg', '.olp-unlock', '.olp-all', '.olp-none']) parts[sel] = mkEl();
  const root = mkEl();
  root.querySelector = sel => parts[sel];
  return { root, parts };
}

const L = (key, group, qty = 10, price = 1) => ({ key, group, qty, price });
// mode: 'auto' = 依 group 立即回應；'manual' = 回應由測試 resolve
function setup(respond, mode = 'auto') {
  const { root, parts } = mkRoot();
  const calls = [];
  const pending = [];
  const picker = ctx.createOpenLinePicker(root, {
    fetchLines: query => {
      calls.push({ ...query });
      if (mode === 'manual') return new Promise((resolve, reject) => pending.push({ query, resolve, reject }));
      return Promise.resolve(respond(query));
    },
    lineKey: l => l.key, groupKey: l => l.group, groupLabel: l => l.group,
    columns: [{ label: 'k', render: l => esc(l.key) }],
    qtyOf: l => l.qty, priceOf: l => l.price,
  });
  const tick = (key, on) => parts.tbody.fire('change', { target: {
    checked: on, classList: { contains: c => c === 'olp-check' }, closest: () => ({ dataset: { key } }) } });
  const edit = (key, cls, value) => parts.tbody.fire('change', { target: {
    value, classList: { contains: c => c === cls }, closest: () => ({ dataset: { key } }) } });
  const keysShown = () => [...parts.tbody.innerHTML.matchAll(/data-key="([^"]+)"/g)].map(m => m[1]);
  return { picker, parts, calls, pending, tick, edit, keysShown };
}
const flush = () => new Promise(r => setTimeout(r, 0));
const plain = v => JSON.parse(JSON.stringify(v)); // vm 內建立的陣列跨 realm，deepStrictEqual 前轉成一般物件
const unlocked = [L('a1', 'A'), L('b1', 'B'), L('a2', 'A')];
const lockedA = [L('a9', 'A'), L('a1', 'A', 7, 2), L('a2', 'A'), L('a3', 'A')];
const byGroup = q => (q.group === 'A' ? lockedA : unlocked);
"""


def run_node(body: str) -> subprocess.CompletedProcess:
    return subprocess.run(["node", "--input-type=module", "-e", HARNESS + body], cwd=ROOT,
                          capture_output=True, text=True, encoding="utf-8")


class OpenLinePickerLockReloadTests(unittest.TestCase):
    def _run(self, body: str):
        completed = run_node(body)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_refetch_once_on_lock_and_keep_server_order(self):
        self._run(r"""
const t = setup(byGroup);
await t.picker.reload();
assert.equal(t.calls.length, 1);
t.tick('a1', true);
await flush();
assert.equal(t.calls.length, 2);
assert.deepEqual(t.calls[1], { q: '', group: 'A' });
assert.deepEqual(t.keysShown(), ['a9', 'a1', 'a2', 'a3']); // 照伺服器回傳順序
t.tick('a2', true);   // 已鎖定再勾 → 不重抓
t.tick('a2', false);  // 還有一筆勾選 → 不重抓
await flush();
assert.equal(t.calls.length, 2);
// 勾選的列資料更新成新結果（qty 7），但已存在的 edits 不被覆蓋
assert.equal(plain(t.picker.getSelection())[0].line.qty, 7);
""")

    def test_refetch_once_on_unlock_last_untick_and_clear(self):
        self._run(r"""
const t = setup(byGroup);
await t.picker.reload();
t.tick('a1', true);
await flush();
t.tick('a1', false); // 最後一筆取消 → 解除 → 重抓一次（不帶 group）
await flush();
assert.equal(t.calls.length, 3);
assert.deepEqual(t.calls[2], { q: '', group: null });
assert.deepEqual(t.keysShown(), ['a1', 'b1', 'a2']);
t.picker.clear(); // 本來就沒鎖 → 不重抓
await flush();
assert.equal(t.calls.length, 3);
t.tick('a1', true);
await flush();
t.parts['.olp-unlock'].fire('click', {}); // 按「解除」= clear()
await flush();
assert.equal(t.calls.length, 5);
assert.equal(t.calls[4].group, null);
assert.equal(t.picker.lockedGroup(), null);
""")

    def test_select_all_lock_refetches_once(self):
        self._run(r"""
const t = setup(q => (q.group === 'A' ? lockedA : [L('a1', 'A'), L('a2', 'A')]));
await t.picker.reload();
assert.equal(t.picker.selectAll().ok, true);
await flush();
assert.equal(t.calls.length, 2);
assert.equal(t.calls[1].group, 'A');
assert.deepEqual(plain(t.picker.getSelection()).map(s => s.line.key), ['a1', 'a2']);
""")

    def test_selections_and_edits_survive_refetch(self):
        self._run(r"""
// 第一次結果有 a1、a2；鎖定後的結果沒有 a2 → a2 仍保留勾選（pinned），改過的量價都在
const t = setup(q => (q.group === 'A' ? [L('a9', 'A'), L('a1', 'A', 99, 9)] : unlocked));
await t.picker.reload();
t.tick('a1', true);
t.tick('a2', true);
await flush(); // 第一次勾選觸發的重抓先回來
t.edit('a1', 'olp-qty', '5');
t.edit('a2', 'olp-price', '3.3');
t.tick('a1', false); t.tick('a1', true); // 還有 a2 勾著 → 不重抓；edits 保留
await flush();
assert.equal(t.calls.length, 2);
const sel = plain(t.picker.getSelection());
assert.deepEqual(sel.map(s => s.line.key), ['a2', 'a1']);
assert.equal(sel[0].price, 3.3);
assert.equal(sel[1].qty, 5);
assert.equal(sel[1].line.qty, 99); // 有在新結果裡 → 資料更新
assert.ok(t.parts.tbody.innerHTML.includes('olp-pinned')); // a2 不在新結果 → 固定顯示在上方
""")

    def test_stale_response_ignored(self):
        self._run(r"""
const t = setup(null, 'manual');
const first = t.picker.reload();            // 例：搜尋的 debounce reload（未鎖定）
t.pending[0].resolve(unlocked);
await first;
t.tick('a1', true);                          // 鎖定 → 第二次 reload（group A）
assert.equal(t.calls.length, 2);
const third = t.picker.reload();             // 使用者又改了搜尋 → 第三次
t.pending[2].resolve([L('a1', 'A'), L('a3', 'A')]);
await third;
t.pending[1].resolve(lockedA);               // 第二次晚回來 → 丟掉
await flush();
assert.deepEqual(t.keysShown(), ['a1', 'a3']);
t.pending.length = 0;
const fourth = t.picker.reload();
const fifth = t.picker.reload();
t.pending[1].resolve([L('a1', 'A')]);
await fifth;
t.pending[0].reject(new Error('timeout'));   // 舊的失敗也不拋錯
await fourth;
assert.deepEqual(t.keysShown(), ['a1']);
""")

    def test_invoice_customer_lock_refetches_with_customer_param(self):
        # erp_invoice.js 用 group 帶 &customer=；鎖定後要用新的 group 重抓
        self._run(r"""
const src = fs.readFileSync('./static/modules/erp_invoice.js', 'utf8');
assert.match(src, /fetchLines: async \(\{ q, group \}\) => apiJson\(`\/api\/erp-invoice\/open-lines\?q=\$\{encodeURIComponent\(q\)\}\$\{group \? `&customer=/);
const urls = [];
const { root } = mkRoot();
const parts = {};
const fetchLines = ({ q, group }) => { urls.push(`/api/erp-invoice/open-lines?q=${encodeURIComponent(q)}${group ? `&customer=${encodeURIComponent(group)}` : ''}`);
  return Promise.resolve(group ? [{ sc: 'S2', c: 'C01' }, { sc: 'S1', c: 'C01' }] : [{ sc: 'S1', c: 'C01' }, { sc: 'X1', c: 'C02' }]); };
const tbody = root.querySelector('tbody');
const picker = ctx.createOpenLinePicker(root, { fetchLines, lineKey: l => l.sc, groupKey: l => l.c, groupLabel: l => l.c,
  columns: [{ label: 'sc', render: l => esc(l.sc) }], qtyOf: () => 1, priceOf: () => 2 });
await picker.reload();
tbody.fire('change', { target: { checked: true, classList: { contains: c => c === 'olp-check' }, closest: () => ({ dataset: { key: 'S1' } }) } });
tbody.fire('change', { target: { value: '4', classList: { contains: c => c === 'olp-qty' }, closest: () => ({ dataset: { key: 'S1' } }) } });
await flush();
assert.deepEqual(urls, ['/api/erp-invoice/open-lines?q=', '/api/erp-invoice/open-lines?q=&customer=C01']);
assert.equal(picker.lockedGroup(), 'C01');
assert.deepEqual(plain(picker.getSelection()).map(s => [s.line.sc, s.qty, s.price]), [['S1', 4, 2]]);
""")


if __name__ == "__main__":
    unittest.main()
