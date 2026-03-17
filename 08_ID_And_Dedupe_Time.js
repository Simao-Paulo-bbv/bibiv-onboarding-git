/** =========================
 *  ID allocation
 *  ========================= */
function allocateNextId_(dest, idIdx) {
  const lastRow = dest.getLastRow();
  if (lastRow < 2) return "ID00000001";

  const vals = dest.getRange(2, idIdx + 1, lastRow - 1, 1).getValues();
  let max = 0;

  for (let i = 0; i < vals.length; i++) {
    const s = String(vals[i][0] || "").trim();
    const m = s.match(/^ID(\d+)$/);
    if (m) {
      const n = parseInt(m[1], 10);
      if (n > max) max = n;
    }
  }

  return "ID" + String(max + 1).padStart(8, "0");
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
  if (!submittedOn) return "";
  if (submittedOn instanceof Date) {
    return Utilities.formatDate(submittedOn, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  }
  return String(submittedOn).trim();
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
    cell.setNumberFormat("@");
    // Dodatkowo wymuś typ string po stronie Apps Script
    value = (value === null || value === undefined) ? "" : String(value);
  }

  cell.setValue(value);
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


/** =========================
 * OPTIONAL: Trigger installer
 * ========================= */
function installTimeTriggerEveryMinute() {
  ScriptApp.newTrigger("runSyncAndProcess")
    .timeBased()
    .everyMinutes(1)
    .create();
}
