"""一鍵調撥前端：獨立分頁／模組、表頭人話、「X-X · 一張調撥單」、缺料採購不再藏調撥對話框。"""
from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "static"


class TransferViewRenderTests(unittest.TestCase):
    def test_render_with_node(self):
        script = r"""
import assert from 'node:assert/strict';
import { COLUMNS, INTRO, renderBatch, renderBatches, batchState, submitLabel, confirmText, resultHtml, renderHistory, wareLabel } from './static/modules/transfer_view.js';

assert.deepEqual(COLUMNS, ['料號', '品名', '從哪倉', '要撥', '公司倉現有', '本次撥', '短少', '成本']);
assert.ok(INTRO.split('。').filter(Boolean).length <= 2, '說明最多兩句');
for (const bad of ['來源倉', '待調撥', '庫存（公司3）', '寫入公司庫存', '庚霖現有']) {
  assert.ok(!COLUMNS.join().includes(bad), bad);
  assert.ok(!INTRO.includes(bad), bad);
}

const ready = { key: '2026/11-1', code: '11-1', already: null, totals: { pending: 130, transfer: 110, short: 20 }, rows: [
  { part: 'P', name: '面板', ware_out: '001', pending: 100, stock_src: 100, stock_target: 100, transfer: 100, short: 0, cost: 21 },
  { part: 'Q-TAB', name: '客<供>', ware_out: '002', pending: 30, stock_src: 10, stock_target: 7, transfer: 10, short: 20, cost: 0 }] };
const html = renderBatch(ready);
assert.ok(html.includes('11-1 · 一張調撥單'));
assert.ok(html.includes('data-transfer-batch="2026/11-1" checked'));
assert.ok(html.includes('001 一般倉') && html.includes('002 客供倉'));
assert.ok(html.includes('客&lt;供&gt;'), '品名要跳脫');
assert.ok(html.includes('transfer-short'));
assert.ok(html.includes('短少 20'));
assert.ok(html.includes('寫入公司的 002 倉現有 7'), 'stock_target 標成寫入公司的來源倉，不是庚霖');
assert.ok(!html.includes('庚霖現有'));
assert.equal(batchState(ready), 'ready');

const done = { ...ready, key: 'B', code: '11-2', already: { move_no: 'RM-20261109001' } };
assert.equal(batchState(done), 'done');
assert.ok(renderBatch(done).includes('disabled'));
assert.ok(renderBatch(done).includes('已開 RM-20261109001'));
assert.ok(!renderBatch(done).includes('<table'));

const empty = { key: 'C', code: '11-3', already: null, totals: { pending: 5, transfer: 0, short: 5 }, rows: [
  { part: 'Z', name: '', ware_out: '001', pending: 5, stock_src: 0, stock_target: 0, transfer: 0, short: 5, cost: 1 }] };
assert.equal(batchState(empty), 'empty');
assert.ok(renderBatch(empty).includes('disabled'));
assert.ok(renderBatches({ batches: [] }).includes('沒有要調撥'));

assert.equal(submitLabel(2), '開 2 張調撥單');
assert.ok(confirmText('公司3（正式）', ['11-1', '11-2']).includes('開 2 張調撥單'));
const res = resultHtml({ results: [{ batch_key: '2026/11-1', ok: true, move_no: 'RM-1' }, { batch_key: 'B', ok: false, error: '庫存不足' }], stopped: ['C'] },
  k => ({ '2026/11-1': '11-1', B: '11-2', C: '11-3' }[k]));
assert.ok(res.includes('11-1 → RM-1') && res.includes('11-2 沒開成：庫存不足') && res.includes('11-3'));
assert.ok(renderHistory([{ id: 3, batch_code: '11-1', move_no: 'RM-1', target_db: 'CHIComp03', status: 'ok', error: '', created_at: 'x' }]).includes('data-recheck="3"'));
assert.equal(wareLabel('015'), '015 庚霖倉');
"""
        completed = subprocess.run(["node", "--input-type=module", "-e", script], cwd=ROOT,
                                   capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_modules_parse(self):
        for name in ("erp_transfer.js", "transfer_view.js", "shortage_po.js"):
            completed = subprocess.run(["node", "--check", str(STATIC / "modules" / name)], capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, name + completed.stderr)


class TransferWiringTests(unittest.TestCase):
    def setUp(self):
        self.html = (STATIC / "index.html").read_text(encoding="utf-8")
        self.module = (STATIC / "modules" / "erp_transfer.js").read_text(encoding="utf-8")
        self.shortage = (STATIC / "modules" / "shortage_po.js").read_text(encoding="utf-8")

    def test_warehouse_tab_is_the_transfer_page(self):
        panel = re.search(r'<div id="tab-transfer-tab" class="tab-content">(.*?)\n    </div>\n', self.html, re.S).group(1)
        for needed in ('id="transfer-batches"', 'id="btn-transfer-submit"', 'id="transfer-history"', 'id="btn-transfer-refresh"'):
            self.assertIn(needed, panel)
        self.assertIn('data-group="warehouse" data-tab="transfer-tab"', self.html)
        self.assertIn('from "/static/modules/erp_transfer.js"', self.html)
        self.assertIn('btn.dataset.tab === "transfer-tab") void onTransferTabActivated()', self.html)

    def test_shortage_po_keeps_shortcut_but_not_dialog(self):
        self.assertIn('id="btn-shortage-transfer"', self.html)
        self.assertNotIn("openTransfer", self.shortage)
        self.assertNotIn("/transfer/create", self.shortage)
        self.assertNotIn("transfer-dialog", self.shortage)
        self.assertIn('data-tab="transfer-tab"', self.module)

    def test_module_uses_api_helpers_and_server_side_target(self):
        self.assertNotRegex(self.module, r"(?<![A-Za-z])fetch\(")
        self.assertIn('from "/static/modules/api.js"', self.module)
        self.assertNotIn("target:", self.module)
        self.assertIn("confirm(", self.module)
        self.assertIn("preview_token", self.module)


if __name__ == "__main__":
    unittest.main()
