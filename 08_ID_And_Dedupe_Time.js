/** =========================
 *  ID allocation
 *  ========================= */
const TEXT_COL_FORMATTED_CACHE = {};
const ID_ALLOC_CACHE = {};

function allocateNextId_(dest, idIdx) {
  const key = getIdAllocCacheKey_(dest, idIdx);
  if (!ID_ALLOC_CACHE[key]) {
    const lastRow = dest.getLastRow();
    let max = 0;
    if (lastRow >= 2) {
      const vals = dest.getRange(2, idIdx + 1, lastRow - 1, 1).getValues();
      for (let i = 0; i < vals.length; i++) {
        const s = String(vals[i][0] || "").trim();
        const m = s.match(/^ID(\d+)$/);
        if (!m) continue;
        const n = parseInt(m[1], 10);
        if (n > max) max = n;
      }
    }
    ID_ALLOC_CACHE[key] = { max: max };
  }

  ID_ALLOC_CACHE[key].max++;
  return "ID" + String(ID_ALLOC_CACHE[key].max).padStart(8, "0");
}

function getIdAllocCacheKey_(sheet, idIdx) {
  try {
    return String(sheet.getSheetId()) + ":" + String(idIdx);
  } catch (e) {
    return String(sheet.getName() || "sheet") + ":" + String(idIdx);
  }
}

/** =========================
 *  DEDUPE / TIME
 *  ========================= */
function makeDedupeKey_(nip, submittedOn) {
  const n = String(nip || "").trim();
  if (CONFIG.DEDUPE && CONFIG.DEDUPE.MODE === "NIP_ONLY") {
    return n;
  }
  const d = normalizeSubmittedOnKey_(submittedOn);
  return n + "||" + d;
}

function normalizeSubmittedOnKey_(submittedOn) {
  if (submittedOn === null || submittedOn === undefined) return "";

  const parsed = parseSubmittedOnDate_(submittedOn);
  if (parsed) {
    return Utilities.formatDate(parsed, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  }

  // Fallback: keep deterministic normalized string if parsing failed.
  return String(submittedOn || "").trim().replace(/\s+/g, " ");
}

function parseSubmittedOnDate_(value) {
  try {
    if (value instanceof Date) {
      return isNaN(value.getTime()) ? null : value;
    }

    if (typeof value === "number" && isFinite(value)) {
      // Google Sheets serial date fallback (days since 1899-12-30).
      if (value > 20000 && value < 80000) {
        const serialMs = Math.round((value - 25569) * 86400 * 1000);
        const serialDate = new Date(serialMs);
        if (!isNaN(serialDate.getTime())) return serialDate;
      }
      const epochDate = new Date(value);
      if (!isNaN(epochDate.getTime())) return epochDate;
    }

    const raw = String(value || "").trim();
    if (!raw) return null;
    const s = raw.replace("T", " ").replace(/Z$/, "");

    // yyyy-mm-dd [HH:mm[:ss]]
    let m = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ ,]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
    if (m) {
      const y = Number(m[1]);
      const mo = Number(m[2]);
      const d = Number(m[3]);
      const hh = Number(m[4] || 0);
      const mi = Number(m[5] || 0);
      const ss = Number(m[6] || 0);
      const dt = new Date(y, mo - 1, d, hh, mi, ss);
      if (dt.getFullYear() === y && dt.getMonth() === mo - 1 && dt.getDate() === d) return dt;
    }

    // slash format (MM/DD/YYYY or DD/MM/YYYY) with heuristic
    m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[ ,]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
    if (m) {
      const a = Number(m[1]);
      const b = Number(m[2]);
      const y = Number(m[3]);
      const hh = Number(m[4] || 0);
      const mi = Number(m[5] || 0);
      const ss = Number(m[6] || 0);

      let month = a;
      let day = b;
      if (a > 12 && b <= 12) {
        // DD/MM/YYYY
        day = a;
        month = b;
      } else if (b > 12 && a <= 12) {
        // MM/DD/YYYY
        month = a;
        day = b;
      }
      const dt = new Date(y, month - 1, day, hh, mi, ss);
      if (dt.getFullYear() === y && dt.getMonth() === month - 1 && dt.getDate() === day) return dt;
    }

    // dot format (DD.MM.YYYY)
    m = s.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})(?:[ ,]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
    if (m) {
      const d = Number(m[1]);
      const mo = Number(m[2]);
      const y = Number(m[3]);
      const hh = Number(m[4] || 0);
      const mi = Number(m[5] || 0);
      const ss = Number(m[6] || 0);
      const dt = new Date(y, mo - 1, d, hh, mi, ss);
      if (dt.getFullYear() === y && dt.getMonth() === mo - 1 && dt.getDate() === d) return dt;
    }

    const guessed = new Date(s);
    return isNaN(guessed.getTime()) ? null : guessed;
  } catch (e) {
    return null;
  }
}

function makeRunId_() {
  const a = Math.random().toString(16).slice(2, 10);
  const t = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "HHmmss");
  return a + "-" + t;
}

function formatNow_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
}

function arraysEqual_(a, b) {
  if (!a || !b) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (String(a[i]) !== String(b[i])) return false;
  }
  return true;
}

function writeIfColExists_(sheet, mapping, rowNum, colName, value) {
  const idx = mapping && mapping.dstIndex ? mapping.dstIndex[colName] : null;
  if (idx == null) return;

  const cell = sheet.getRange(rowNum, idx + 1);

  // Kolumny, które MUSZĄ być traktowane jako tekst (żeby Sheets nie robił notacji naukowej / zaokrągleń)
  // - krs/regon: mogą mieć wiodące zera
  // - accountNumbers: bardzo długie numery rachunków (26 cyfr) / lista po przecinku
  if (colName === "krs" || colName === "regon" || colName === "accountNumbers" || colName === "numer rachunku bankowego") {
    ensureTextFormatForColumnOnce_(sheet, idx + 1);
    // Dodatkowo wymuś typ string po stronie Apps Script
    value = (value === null || value === undefined) ? "" : String(value);
  }

  cell.setValue(value);
}

function ensureTextFormatForColumnOnce_(sheet, col1Based) {
  try {
    const sid = String(sheet.getSheetId());
    const key = sid + ":" + String(col1Based);
    if (TEXT_COL_FORMATTED_CACHE[key]) return;
    sheet.getRange(1, col1Based, sheet.getMaxRows(), 1).setNumberFormat("@");
    TEXT_COL_FORMATTED_CACHE[key] = true;
  } catch (e) {
    // no-op
  }
}

function isVerbose_() {
  return CONFIG.LOGGING.ENABLE && CONFIG.LOGGING.LEVEL === "VERBOSE";
}

function safeHeaders_(headers) {
  // Response headers are generally safe, but we still drop anything that could carry tokens/cookies.
  const out = {};
  try {
    Object.keys(headers || {}).forEach((k) => {
      const key = String(k);
      const lk = key.toLowerCase();
      if (lk.includes("authorization") || lk.includes("cookie") || lk.includes("set-cookie") || lk.includes("token")) return;
      out[key] = headers[k];
    });
  } catch (e) {
    // ignore
  }
  return out;
}
