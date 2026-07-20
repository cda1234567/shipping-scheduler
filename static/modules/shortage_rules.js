export const ORDER_SCOPED_PART_PREFIXES = ["IC-STM", "IC-XC2C32", "IC-M24"];

export function isOrderScopedPart(partNumber) {
  const key = String(partNumber || "").trim().toUpperCase();
  return ORDER_SCOPED_PART_PREFIXES.some(prefix => key.startsWith(prefix));
}

export function calculateCurrentOrderShortageAmount(
  partNumber,
  availableBefore,
  neededQty,
  ignoreEcMin = false,
) {
  if (isOrderScopedPart(partNumber)) {
    return Math.max(0, Number(neededQty || 0) - Math.max(0, Number(availableBefore || 0)));
  }

  const endingStock = Number(availableBefore || 0) - Number(neededQty || 0);
  const normalized = String(partNumber || "").trim().toUpperCase();
  let requiredMin = 0;
  if (normalized.startsWith("EC-6")) requiredMin = 0;
  else if (ignoreEcMin && normalized.startsWith("EC-")) requiredMin = 0;
  else if (normalized.startsWith("EC-")) requiredMin = 100;
  else if (normalized.startsWith("PK-")) requiredMin = normalized.startsWith("PK-50070") ? 0 : 1;
  return Math.max(0, requiredMin - Number(endingStock || 0));
}
