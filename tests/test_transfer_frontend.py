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
import { COLUMNS, INTRO, renderPicker, renderDetails, batchState, submitLabel, confirmText, resultHtml, renderHistory, wareLabel } from './static/modules/transfer_view.js';

assert.deepEqual(COLUMNS, ['料號', '品名', '本次撥']);
assert.ok(INTRO.split('。').filter(Boolean).length <= 2, '說明最多兩句');
for (const bad of ['來源倉', '待調撥', '庫存（公司3）', '寫入公司庫存', '庚霖現有']) {
  assert.ok(!COLUMNS.join().includes(bad), bad);
  assert.ok(!INTRO.includes(bad), bad);
}

const ready = { key: '2026/11-1', code: '11-1', already: null, totals: { pending: 130, transfer: 110, short: 20 }, rows: [
  { part: 'P', name: '面板', ware_out: '001', pending: 100, stock_src: 100, stock_target: 100, transfer: 100, short: 0, cost: 21 },
  { part: 'Q-TAB', name: '客<供>', ware_out: '002', pending: 30, stock_src: 10, stock_target: 7, transfer: 10, short: 20, cost: 0 },
  { part: 'R-NONE', name: '沒貨', ware_out: '001', pending: 9, stock_src: 0, stock_target: 0, transfer: 0, short: 9, cost: 5 }] };
const ready2 = { ...ready, key: 'D', code: '11-4', totals: { pending: 1, transfer: 1, short: 0 }, rows: [
  { part: 'S', name: '螺絲', ware_out: '001', pending: 1, stock_src: 1, stock_target: 1, transfer: 1, short: 0, cost: 1 }] };
const done = { ...ready, key: 'B', code: '11-2', already: { move_no: 'RM-20261109001' } };
const empty = { key: 'C', code: '11-3', already: null, totals: { pending: 5, transfer: 0, short: 5 }, rows: [
  { part: 'Z', name: '', ware_out: '001', pending: 5, stock_src: 0, stock_target: 0, transfer: 0, short: 5, cost: 1 }] };
const data = { batches: [ready, done, empty, ready2] };
assert.equal(batchState(ready), 'ready');
assert.equal(batchState(done), 'done');
assert.equal(batchState(empty), 'empty');

// 右邊清單：所有批次都在；預設沒有勾選
const picker = renderPicker(data);
for (const code of ['11-1', '11-2', '11-3', '11-4']) assert.ok(picker.includes(code), code);
assert.ok(!picker.includes('checked'), '預設不勾');
assert.ok(picker.includes('data-transfer-batch="2026/11-1"') && picker.includes('data-transfer-batch="D"'));
assert.ok(picker.includes('2 項，撥 110'), '右邊寫幾項、撥多少');
// 已開過的另外一區，只有代號與單號，沒有可送出的 checkbox
const [openPart, donePart] = picker.split('transfer-side-done');
assert.ok(donePart && donePart.includes('11-2') && donePart.includes('RM-20261109001'));
assert.ok(!donePart.includes('<input'), '已開過的不能勾');
assert.ok(!openPart.includes('11-2'), '已開過的不跟還沒開的混在一起');
assert.ok(!picker.includes('data-transfer-batch="B"'));
// 沒貨的仍在「還沒開」，但不能勾、註明沒貨
assert.ok(openPart.includes('11-3') && openPart.includes('公司倉都沒貨，短少 5'));
assert.ok(!picker.includes('data-transfer-batch="C"'));
assert.match(openPart, /transfer-pick-empty">\s*<input type="checkbox" disabled/);
assert.ok(renderPicker(data, new Set(['D'])).includes('data-transfer-batch="D" checked'));
assert.ok(renderPicker({ batches: [] }).includes('沒有要調撥'));

// 左邊明細：沒勾時是提示；只列被勾、可開的批次
assert.ok(renderDetails(data).includes('右邊勾選要開的批次，明細會出現在這裡'));
assert.ok(!renderDetails(data).includes('<table'));
const detail = renderDetails(data, new Set(['2026/11-1', 'B', 'C']));
assert.ok(detail.includes('11-1 · 一張調撥單'));
assert.ok(!detail.includes('11-2') && !detail.includes('11-3') && !detail.includes('11-4'), '只出現勾到且可開的批次');
assert.equal((detail.match(/<table/g) || []).length, 1);
assert.ok(detail.includes('客&lt;供&gt;'), '品名要跳脫');
assert.ok(!detail.includes('R-NONE'), '本次撥 0 的料不列');
const heads = [...detail.matchAll(/<th[^>]*>([^<]*)<\/th>/g)].map(m => m[1]);
assert.deepEqual(heads, ['料號', '品名', '本次撥']);
for (const bad of ['從哪倉', '要撥', '公司倉現有', '短少', '成本', '001 一般倉', '002 客供倉', 'transfer-short']) assert.ok(!detail.includes(bad), bad);
const both = renderDetails(data, new Set(['2026/11-1', 'D']));
assert.equal((both.match(/<table/g) || []).length, 2);
assert.ok(both.includes('螺絲'));

assert.equal(submitLabel(2), '開 2 張調撥單');
assert.equal(submitLabel(0), '請勾選要開的批次');
assert.ok(confirmText('公司3（正式）', ['11-1', '11-2']).includes('開 2 張調撥單'));
const res = resultHtml({ results: [{ batch_key: '2026/11-1', ok: true, move_no: 'RM-1' }, { batch_key: 'B', ok: false, error: '庫存<不足>' }], stopped: ['C'] },
  k => ({ '2026/11-1': '11-1', B: '11-2', C: '11-3' }[k]));
assert.ok(res.includes('11-1 → RM-1') && res.includes('11-2 沒開成：庫存&lt;不足&gt;') && res.includes('11-3'));
assert.ok(renderHistory([{ id: 3, batch_code: '11-1', move_no: 'RM-1', target_db: 'CHIComp03', status: 'ok', error: '', created_at: 'x' }]).includes('data-recheck="3"'));
assert.equal(wareLabel('015'), '015 庚霖倉');

// 機種名稱：右邊卡片（可開／沒貨／已開過）與左邊明細標題都顯示，多個以 " / " 串接、要跳脫
const withModels = { batches: [
  { ...ready, models: ['TX1(TD4)-Power-REV-1.0', 'TD4-REV-1.1'] },
  { ...done, models: ['DONE<M>'] },
  { ...empty, models: ['EMPTY-M'] },
  { ...ready2, models: [] }] };
const mp = renderPicker(withModels);
assert.ok(mp.includes('<span class="transfer-models">TX1(TD4)-Power-REV-1.0 / TD4-REV-1.1</span>'));
assert.ok(mp.includes('EMPTY-M'));
assert.ok(mp.split('transfer-side-done')[1].includes('DONE&lt;M&gt;'));
assert.equal((mp.match(/transfer-models/g) || []).length, 3, '沒有機種不畫空行');
assert.ok(!renderPicker(data).includes('transfer-models'), '舊資料沒有 models 也能畫');
const md = renderDetails(withModels, new Set(['2026/11-1']));
assert.ok(md.includes('TX1(TD4)-Power-REV-1.0 / TD4-REV-1.1'));
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
        # 左明細、右清單；送出鈕在右欄頂端（清單之前），不會被長表格推到最下面
        layout = re.search(r'<div class="transfer-layout">(.*?)</aside>', panel, re.S).group(1)
        self.assertLess(layout.index('id="transfer-detail"'), layout.index('<aside class="transfer-side">'))
        self.assertLess(layout.index('id="btn-transfer-submit"'), layout.index('id="transfer-batches"'))
        self.assertIn('data-group="warehouse" data-tab="transfer-tab"', self.html)
        self.assertIn('from "/static/modules/erp_transfer.js"', self.html)
        self.assertIn('btn.dataset.tab === "transfer-tab") void onTransferTabActivated()', self.html)

    def test_shortage_po_keeps_shortcut_but_not_dialog(self):
        self.assertIn('id="btn-shortage-transfer"', self.html)
        self.assertNotIn("openTransfer", self.shortage)
        self.assertNotIn("/transfer/create", self.shortage)
        self.assertNotIn("transfer-dialog", self.shortage)
        self.assertIn('data-tab="transfer-tab"', self.module)

    def test_layout_css(self):
        css = (STATIC / "style.css").read_text(encoding="utf-8")
        self.assertNotIn(".transfer-shell { max-width", css)
        self.assertRegex(css, r"\.transfer-layout \{[^}]*grid-template-columns: minmax\(0, 1fr\) clamp\(280px, [^,]+, 360px\)")
        self.assertRegex(css, r"\.transfer-actions \{[^}]*position: sticky")
        detail_rule = re.search(r"\.transfer-detail-table th, \.transfer-detail-table td \{([^}]*)\}", css).group(1)
        self.assertNotIn("nowrap", detail_rule)
        self.assertIn("body.desktop-dark .transfer-detail-table", css)
        models_rule = re.search(r"\.transfer-models \{([^}]*)\}", css).group(1)
        self.assertIn("flex-basis: 100%", models_rule)
        self.assertIn("overflow-wrap: anywhere", models_rule)
        self.assertNotIn("nowrap", models_rule)
        self.assertIn("body.desktop-dark .transfer-models", css)

    def test_module_sends_only_checked_ready_batches(self):
        self.assertIn("_checked = new Set()", self.module)
        self.assertIn('batchState(b) === "ready"', self.module)
        self.assertIn("renderDetails(_data, _checked)", self.module)

    def test_module_uses_api_helpers_and_server_side_target(self):
        self.assertNotRegex(self.module, r"(?<![A-Za-z])fetch\(")
        self.assertIn('from "/static/modules/api.js"', self.module)
        self.assertNotIn("target:", self.module)
        self.assertIn("confirm(", self.module)
        self.assertIn("preview_token", self.module)


if __name__ == "__main__":
    unittest.main()
