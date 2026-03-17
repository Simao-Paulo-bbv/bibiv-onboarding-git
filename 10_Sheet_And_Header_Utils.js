/** =========================
 *  SHEET + HEADER UTILS
 *  ========================= */
function getSheet_(ss, name, createIfMissing) {
  const sh = ss.getSheetByName(name);
  if (sh) return sh;
  if (!createIfMissing) return null;
  return ss.insertSheet(name);
}

function getHeaderRow_(sheet) {
  const lastCol = Math.max(1, sheet.getLastColumn());
  return sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(h => String(h || "").trim());
}

function indexByNormalized_(headers) {
  const map = {};
  for (let i = 0; i < headers.length; i++) {
    const k = normalizeKey_(headers[i]);
    if (!k) continue;
    if (map[k] == null) map[k] = i;
  }
  return map;
}

function indexByExact_(headers) {
  const map = {};
  for (let i = 0; i < headers.length; i++) {
    const k = String(headers[i] || "").trim();
    if (!k) continue;
    if (map[k] == null) map[k] = i;
  }
  return map;
}

function normalizeKey_(s) {
  const k = String(s || "").trim().toLowerCase();
  if (!k) return "";
  // Optional header aliasing (e.g. Squarespace changed header name).
  // HEADER_ALIASES is defined in 00_Config.gs.
  try {
    const aliases = (typeof HEADER_ALIASES !== "undefined" && HEADER_ALIASES) ? HEADER_ALIASES : null;
    if (aliases && aliases[k] != null && aliases[k] !== "") {
      return String(aliases[k]).trim().toLowerCase();
    }
  } catch (e) {
    // ignore
  }
  return k;
}
