/**
 * 前端 running balance 計算器 — 已發料隔離版
 *
 * 1. 從快照庫存開始
 * 2. 扣掉已發料消耗（dispatched_consumption）
 * 3. 套用各訂單已保存的補料值
 * 4. 對未發料訂單跑 running balance
 */
const ORDER_SCOPED_PART_PREFIXES = ["IC-STM", "IC-XC2C32"];

export function calculate(orders, bomMap, stock, moq, dispatchedConsumption = {}, stStock = {}, orderSupplementsByOrder = {}) {
  const running = { ...stock };
  const normalizedOrderSupplements = normalizeOrderSupplements(orderSupplementsByOrder);

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
    const orderId = Number(order?.id);
    const key = (order.model || "").toUpperCase();
    const bomEntry = bom[key] ?? null;
    const orderQty = toNumber(order?.order_qty);

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
      const effectiveNeededQty = calculateEffectiveNeededQty(comp, orderQty);
      if (comp.is_dash || effectiveNeededQty <= 0) continue;

      const part = (comp.part_number || "").toUpperCase();
      if (!partSummaries[part]) {
        partSummaries[part] = {
          part_key: part,
          part_number: comp.part_number,
          description: comp.description || "",
          current_stock: running[part] ?? 0,
          needed: 0,
          prev_qty_cs: 0,
          ending_stock: running[part] ?? 0,
        };
      } else if (!partSummaries[part].description && comp.description) {
        partSummaries[part].description = comp.description;
      }

      const summary = partSummaries[part];
      const g = running[part] ?? 0;
      const f = effectiveNeededQty;
      const h = comp.prev_qty_cs || 0;
      const j = g + h - f;
      running[part] = j;
      summary.needed += f;
      summary.prev_qty_cs += h;
      summary.ending_stock = j;
    }

    const supplements = normalizedOrderSupplements[orderId] || {};
    for (const [part, supplementQty] of Object.entries(supplements)) {
      if (!Number.isFinite(supplementQty) || supplementQty <= 0) continue;

      running[part] = (running[part] ?? 0) + supplementQty;

      const summary = partSummaries[part];
      if (!summary) continue;

      summary.supplement_qty = (summary.supplement_qty || 0) + supplementQty;
      summary.ending_stock = (summary.ending_stock || 0) + supplementQty;
    }

    for (const summary of Object.values(partSummaries)) {
      const shortage_amount = calculateCurrentOrderShortageAmount(
        summary.part_number,
        Number(summary.current_stock || 0) + Number(summary.prev_qty_cs || 0),
        summary.needed,
      );
      if (shortage_amount <= 0) continue;

      const isOrderScoped = isOrderScopedShortagePart(summary.part_number);
      const st_stock_qty = Math.max(0, Number(stStock[summary.part_key] ?? 0) || 0);
      const item_moq = moq[summary.part_key] ?? 0;
      const st_available_qty = isOrderScoped
        ? Math.min(shortage_amount, st_stock_qty)
        : Math.min(calcSuggested(shortage_amount, item_moq), st_stock_qty);
      const purchase_needed_qty = Math.max(0, shortage_amount - st_available_qty);
      const purchase_suggested_qty = isOrderScoped
        ? purchase_needed_qty
        : purchase_needed_qty > 0
          ? calcSuggested(purchase_needed_qty, item_moq)
          : 0;
      const suggested_qty = isOrderScoped
        ? shortage_amount
        : calcSuggested(shortage_amount, item_moq);
      const item = {
        part_number: summary.part_number,
        description: summary.description,
        shortage_amount,
        current_stock: summary.current_stock,
        needed: summary.needed,
        supplement_qty: summary.supplement_qty || 0,
        moq: item_moq,
        suggested_qty,
        purchase_suggested_qty,
        decision: "None",
        st_stock_qty,
        st_available_qty,
        purchase_needed_qty,
        needs_purchase: purchase_needed_qty > 0,
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

function toNumber(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? number : 0;
}

function resolveEffectiveOrderQty(scheduleOrderQty, bomOrderQty = 0) {
  const scheduleQty = toNumber(scheduleOrderQty);
  if (scheduleQty > 0) return scheduleQty;
  return toNumber(bomOrderQty);
}

function calculateEffectiveNeededQty(component = {}, scheduleOrderQty = 0) {
  const originalNeededQty = toNumber(component.needed_qty);
  const scheduleQty = toNumber(scheduleOrderQty);
  if (scheduleQty <= 0) return originalNeededQty;

  const qtyPerBoard = toNumber(component.qty_per_board);
  if (qtyPerBoard > 0) return qtyPerBoard * scheduleQty;

  const bomOrderQty = toNumber(component.bom_order_qty);
  if (bomOrderQty > 0 && originalNeededQty > 0) {
    return originalNeededQty * scheduleQty / bomOrderQty;
  }

  return originalNeededQty;
}

function isOrderScopedShortagePart(partNumber) {
  const key = String(partNumber || "").trim().toUpperCase();
  return ORDER_SCOPED_PART_PREFIXES.some(prefix => key.startsWith(prefix));
}

function normalizeOrderSupplements(orderSupplementsByOrder = {}) {
  const normalized = {};
  for (const [rawOrderId, supplements] of Object.entries(orderSupplementsByOrder || {})) {
    const orderId = Number.parseInt(rawOrderId, 10);
    if (!Number.isInteger(orderId)) continue;

    const orderSupplements = {};
    for (const [rawPart, rawQty] of Object.entries(supplements || {})) {
      const part = String(rawPart || "").trim().toUpperCase();
      const qty = Number(rawQty || 0);
      if (!part || !Number.isFinite(qty) || qty <= 0) continue;
      orderSupplements[part] = qty;
    }

    normalized[orderId] = orderSupplements;
  }
  return normalized;
}

function getRequiredMinStock(partNumber) {
  const normalized = String(partNumber || "").trim().toUpperCase();
  if (normalized.startsWith("EC-6")) return 0;
  return normalized.startsWith("EC-") ? 100 : 0;
}

function calculateShortageAmount(partNumber, endingStock) {
  const requiredMin = getRequiredMinStock(partNumber);
  return Math.max(0, requiredMin - Number(endingStock || 0));
}

function calculateCurrentOrderShortageAmount(partNumber, availableBefore, neededQty) {
  if (isOrderScopedShortagePart(partNumber)) {
    return Math.max(0, Number(neededQty || 0) - Math.max(0, Number(availableBefore || 0)));
  }
  const endingStock = Number(availableBefore || 0) - Number(neededQty || 0);
  return calculateShortageAmount(partNumber, endingStock);
}
