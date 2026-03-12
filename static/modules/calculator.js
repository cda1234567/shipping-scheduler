/**
 * 前端 running balance 計算器 — 已發料隔離版
 *
 * 1. 從快照庫存開始
 * 2. 扣掉已發料消耗（dispatched_consumption）
 * 3. 對未發料訂單跑 running balance
 */
export function calculate(orders, bomMap, stock, moq, dispatchedConsumption = {}) {
  const running = { ...stock };

  for (const [part, consumed] of Object.entries(dispatchedConsumption)) {
    const key = part.toUpperCase();
    running[key] = (running[key] ?? 0) - consumed;
  }

  const bom = {};
  for (const [k, v] of Object.entries(bomMap)) {
    bom[k.toUpperCase()] = v;
  }

  const results = [];

  for (const order of orders) {
    const key = (order.model || "").toUpperCase();
    const bomEntry = bom[key] ?? null;

    if (!bomEntry) {
      results.push({
        order_id: order.id, po_number: order.po_number,
        pcb: order.pcb, model: order.model,
        status: "no_bom", shortages: [], customer_material_shortages: [],
      });
      continue;
    }

    const shortages = [];
    const components = bomEntry.components || [];
    const partSummaries = {};

    for (const comp of components) {
      if (comp.is_dash || comp.needed_qty <= 0) continue;

      const part = (comp.part_number || "").toUpperCase();
      if (!partSummaries[part]) {
        partSummaries[part] = {
          part_key: part,
          part_number: comp.part_number,
          description: comp.description || "",
          current_stock: running[part] ?? 0,
          needed: 0,
          ending_stock: running[part] ?? 0,
        };
      } else if (!partSummaries[part].description && comp.description) {
        partSummaries[part].description = comp.description;
      }

      const summary = partSummaries[part];
      const g = running[part] ?? 0;
      const f = comp.needed_qty;
      const h = comp.prev_qty_cs || 0;
      const j = g + h - f;
      running[part] = j;
      summary.needed += f;
      summary.ending_stock = j;
    }

    for (const summary of Object.values(partSummaries)) {
      if (summary.ending_stock >= 0) continue;

      const shortage_amount = Math.abs(summary.ending_stock);
      const item_moq = moq[summary.part_key] ?? 0;
      const item = {
        part_number: summary.part_number,
        description: summary.description,
        shortage_amount,
        current_stock: summary.current_stock,
        needed: summary.needed,
        moq: item_moq,
        suggested_qty: calcSuggested(shortage_amount, item_moq),
        decision: "None",
      };
      shortages.push(item);
    }

    results.push({
      order_id: order.id, po_number: order.po_number,
      pcb: order.pcb, model: order.model,
      status: shortages.length ? "shortage" : "ok",
      shortages,
      customer_material_shortages: [],
    });
  }

  return results;
}

function calcSuggested(shortage, moq) {
  if (moq > 0) return Math.ceil(shortage / moq) * moq;
  return shortage;
}
