/** =========================
 *  Build payload from DEST
 *  ========================= */
function buildAppSheetPayloadFromDest_(dest, rowNum, action) {
  const row = dest.getRange(rowNum, 1, 1, DEST_SCHEMA.length).getValues()[0];
  const payload = {};

  for (let i = 0; i < DEST_SCHEMA.length; i++) {
    const col = DEST_SCHEMA[i];
    payload[col] = normalizeForAppSheet_(col, row[i]);
  }

  payload["ID"] = String(payload["ID"] || "").trim();
  payload["Status"] = String(payload["Status"] || "").trim();

  payload["NIP_Control"] = normalizeNipMaybeNumber_(payload["NIP_Control"]);
  payload["nip"] = normalizeNipMaybeNumber_(payload["nip"]);

  if (payload["submitted on"] instanceof Date) {
    payload["submitted on"] = Utilities.formatDate(payload["submitted on"], Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  }

  if (payload["numer telefonu osoby kontaktowej"] !== "" && payload["numer telefonu osoby kontaktowej"] != null) {
    const phone = String(payload["numer telefonu osoby kontaktowej"]).replace(/[^\d]/g, "");
    payload["numer telefonu osoby kontaktowej"] = phone ? Number(phone) : "";
  }


  // Url columns (AppSheet "Url" type):
  // - For EDIT: if empty/placeholder -> omit so we don't overwrite existing value.
  // - For ADD: AppSheet may treat Url as required; when missing, send a safe placeholder URL.
  const NO_WEBSITE_URL = "https://no-website.invalid/";
  const rawWebsite = payload["website"];
  const websiteStr = rawWebsite === null || rawWebsite === undefined ? "" : String(rawWebsite).trim();
  const isWebsiteEmpty = websiteStr === "" || websiteStr === "-" || websiteStr === "//";
  const actionStr = String(action || "").toLowerCase();

  if (isWebsiteEmpty) {
    if (actionStr === "add") {
      payload["website"] = NO_WEBSITE_URL;
    } else {
      delete payload["website"];
    }
  }

  delete payload["_RowNumber"];
  return payload;
}

/** =========================
 *  VALUE NORMALIZATION
 *  ========================= */
function safeJsonParse_(text) {
  try {
    return JSON.parse(text);
  } catch (e) {
    return null;
  }
}

function normalizeForAppSheet_(col, v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "boolean") return v;

  const colName = String(col || "");
  const yesNoCols = {
    "Lead Created": true,
    "Bank aproval": true,
    "Documents sent to client": true,
    "Documents sent to bank": true
  };

  if (yesNoCols[colName]) {
    if (typeof v === "number") return v === 1;
    const s = String(v).trim().toLowerCase();
    if (s === "") return "";
    if (s === "true" || s === "yes" || s === "tak" || s === "1") return true;
    if (s === "false" || s === "no" || s === "nie" || s === "0") return false;
    return v;
  }

  if (v instanceof Date) {
    if (colName === "submitted on") {
      return Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
    }
    if (colName === "registrationLegalDate" || colName === "data urodzenia 6" || colName === "data urodzenia") {
      return Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd");
    }
    return Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  }

  if (colName === "nip" || colName === "NIP_Control") {
    return normalizeNipMaybeNumber_(v);
  }

  if (colName === "krs") {
    return normalizeKrs_(v);
  }

  if (colName === "regon") {
    return normalizeRegon_(v);
  }


  if (colName === "numer telefonu osoby kontaktowej") {
    if (typeof v === "number") return v;
    const s = String(v).replace(/[^\d]/g, "");
    if (!s) return "";
    const n = Number(s);
    return isNaN(n) ? "" : n;
  }

  if (typeof v === "string") return v.trim();
  return v;
}

function normalizeNipMaybeNumber_(v) {
  const s = String(v || "").trim();
  if (!s) return "";
  if (!CONFIG.NIP_AS_NUMBER) return s;

  const digits = s.replace(/[^\d]/g, "");
  if (!digits) return s;

  const n = Number(digits);
  return isNaN(n) ? s : n;
}


function normalizeKrs_(v) {
  const s = String(v === null || v === undefined ? "" : v).trim();
  if (!s) return "";
  const digits = s.replace(/[^\d]/g, "");
  if (!digits) return s;
  // Polish KRS is typically 10 digits; preserve leading zeros
  return digits.padStart(10, "0").slice(-10);
}

function normalizeRegon_(v) {
  const s = String(v === null || v === undefined ? "" : v).trim();
  if (!s) return "";
  const digits = s.replace(/[^\d]/g, "");
  if (!digits) return s;
  // REGON can be 9 or 14 digits; preserve leading zeros
  if (digits.length <= 9) return digits.padStart(9, "0");
  if (digits.length <= 14) return digits.padStart(14, "0");
  return digits;
}
