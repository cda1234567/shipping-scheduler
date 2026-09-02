/**
 * 前端 running balance 計算器 — 已發料隔離版
 *
 * 1. 從快照庫存開始
 * 2. 扣掉已發料消耗（dispatched_consumption）
 * 3. 套用各訂單已保存的補料值
 * 4. 對未發料訂單跑 running balance
 */
import {
  calculateCurrentOrderShortageAmount,
  isOrderScopedPart as isOrderScopedShortagePart,
} from "./shortage_rules.js";

export function calculate(
  orders,
  bomMap,
  stock,
  moq,
  dispatchedConsumption = {},
  stStock = {},
  orderSupplementsByOrder = {},
  orderSubstitutionAllocations = {},
) {
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
    const substitutionRules = bomEntry.substitution_rules || [];
    const manualRemaining = normalizeManualAllocations(orderSubstitutionAllocations[orderId] || {});
    const partSummaries = {};

    for (const comp of components) {
      const effectiveNeededQty = calculateEffectiveNeededQty(comp, orderQty);
      if (comp.is_dash || effectiveNeededQty <= 0) continue;

      const effectiveComponent = { ...comp, needed_qty: effectiveNeededQty };
      const rule = findSubstitutionRule(
        substitutionRules,
        order.model,
        comp.part_number,
        order.code,
      );
      const actualComponents = allocateSubstitution(
        effectiveComponent,
        rule,
        running,
        manualRemaining,
      );

      for (const actual of actualComponents) {
        const part = String(actual.part_number || "").trim().toUpperCase();
        if (!partSummaries[part]) {
          partSummaries[part] = {
            part_key: part,
            part_number: actual.part_number,
            description: actual.description || "",
            current_stock: running[part] ?? 0,
            needed: 0,
            prev_qty_cs: 0,
            ending_stock: running[part] ?? 0,
          };
        } else if (!partSummaries[part].description && actual.description) {
          partSummaries[part].description = actual.description;
        }

        const summary = partSummaries[part];
        const g = running[part] ?? 0;
        const f = toNumber(actual.needed_qty);
        const h = toNumber(actual.prev_qty_cs);
        const j = g + h - f;
        running[part] = j;
        summary.needed += f;
        summary.prev_qty_cs += h;
        summary.ending_stock = j;
      }
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

function normalizeManualAllocations(allocations = {}) {
  const normalized = {};
  for (const [rawRuleId, allocation] of Object.entries(allocations || {})) {
    const ruleId = Number.parseInt(rawRuleId, 10);
    if (!Number.isInteger(ruleId)) continue;
    normalized[ruleId] = {
      old_qty: Math.max(0, toNumber(allocation?.old_qty)),
      new_qty: Math.max(0, toNumber(allocation?.new_qty)),
    };
  }
  return normalized;
}

function findSubstitutionRule(rules, model, partNumber, batchCode) {
  const targetModel = String(model || "").trim().toUpperCase();
  const targetPart = String(partNumber || "").trim().toUpperCase();
  const code = String(batchCode || "").trim();
  const candidates = [...(rules || [])].sort((a, b) => {
    const aExact = String(a.model || "").trim().toUpperCase() === targetModel ? 0 : 1;
    const bExact = String(b.model || "").trim().toUpperCase() === targetModel ? 0 : 1;
    return aExact - bExact;
  });
  return candidates.find(rule => {
    const ruleModel = String(rule.model || "").trim().toUpperCase();
    const oldPart = String(rule.old_part_number || "").trim().toUpperCase();
    const startCode = String(rule.effective_from_code || "").trim();
    if (String(rule.status || "active") !== "active") return false;
    if (ruleModel !== "*" && ruleModel !== targetModel) return false;
    if (oldPart !== targetPart) return false;
    if (!startCode) return true;
    return Boolean(code) && code.localeCompare(startCode, undefined, { numeric: true, sensitivity: "base" }) >= 0;
  }) || null;
}

function allocateSubstitution(component, rule, running, manualRemaining) {
  if (!rule) return [{ ...component }];

  const demand = Math.max(0, toNumber(component.needed_qty));
  const ratio = Math.max(Number.EPSILON, toNumber(rule.new_per_old_ratio) || 1);
  const oldPart = String(rule.old_part_number || component.part_number || "").trim().toUpperCase();
  const newPart = String(rule.new_part_number || "").trim().toUpperCase();
  const ruleId = Number(rule.id || 0);
  const manual = manualRemaining[ruleId];
  let oldQty = 0;
  let newQty = 0;

  if (manual) {
    oldQty = Math.min(demand, manual.old_qty);
    newQty = Math.min((demand - oldQty) * ratio, manual.new_qty);
    manual.old_qty -= oldQty;
    manual.new_qty -= newQty;
  } else if (String(rule.strategy || "old_first") === "new_first") {
    const newBaseQty = Math.min(demand, Math.max(0, toNumber(running[newPart])) / ratio);
    newQty = newBaseQty * ratio;
    oldQty = demand - newBaseQty;
  } else {
    oldQty = Math.min(
      demand,
      Math.max(0, toNumber(running[oldPart]) + toNumber(component.prev_qty_cs)),
    );
    newQty = (demand - oldQty) * ratio;
  }

  const result = [];
  if (oldQty > 0) {
    result.push({ ...component, part_number: oldPart, needed_qty: oldQty });
  }
  if (newQty > 0) {
    const description = String(component.description || "").trim();
    result.push({
      ...component,
      part_number: newPart,
      needed_qty: newQty,
      prev_qty_cs: 0,
      description: description ? `${description}（替代 ${oldPart}）` : `替代 ${oldPart}`,
    });
  }
  return result;
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
  // 需求量永遠由 qty_per_board × 排程數量 × (1 + 拋料率) 即時算，
  // 不再讀 BOM F 欄的 needed_qty；只有在該 BOM 沒有 qty_per_board 時才退回原值。
  const originalNeededQty = toNumber(component.needed_qty);
  const scheduleQty = toNumber(scheduleOrderQty);
  if (scheduleQty <= 0) return originalNeededQty;

  const qtyPerBoard = toNumber(component.qty_per_board);
  if (qtyPerBoard > 0) {
    const scrap = Math.max(0, toNumber(component.scrap_factor));
    return qtyPerBoard * scheduleQty * (1 + scrap);
  }

  return originalNeededQty;
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
