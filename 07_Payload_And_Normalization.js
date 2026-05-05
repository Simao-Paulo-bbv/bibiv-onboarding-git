/** =========================
 *  Build payload from DEST
 *  ========================= */
function buildAppSheetPayloadFromDest_(dest, rowNum, action) {
  const headers = getHeaderRow_(dest);
  const row = dest.getRange(rowNum, 1, 1, headers.length).getValues()[0];
  const payload = {};

  for (let i = 0; i < headers.length; i++) {
    const col = String(headers[i] || "").trim();
    if (!col) continue;
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
    const phone = normalizePhoneDigits_(payload["numer telefonu osoby kontaktowej"]);
    payload["numer telefonu osoby kontaktowej"] = phone || "";
  }

  // AppSheet MAIN requires accountNumbers (EnumList) for standard VAT flow.
  // For NOT VAT records keep accountNumbers as-is (expected empty).
  const isNotVatStatus = String(payload["statusVat"] || "").trim().toLowerCase() === "not vat";
  payload["accountNumbers"] = isNotVatStatus
    ? normalizeAccountNumbersCsv_(payload["accountNumbers"])
    : normalizeAccountNumbersForPayload_(
        payload["accountNumbers"],
        payload["numer rachunku bankowego"]
      );


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

  // AppSheet MAIN has required Email for contact person in current schema.
  // For Add requests, provide deterministic email fallback only.
  // Phone must stay true-to-source (no synthetic values).
  if (actionStr === "add") {
    payload["email osoby kontaktowej"] = ensureRequiredContactEmail_(payload["email osoby kontaktowej"], payload);
    payload["numer telefonu osoby kontaktowej"] = ensureRequiredContactPhone_(payload["numer telefonu osoby kontaktowej"]);
  }

  // Some form fields may temporarily disappear in Squarespace.
  // Keep them in flow when present, but avoid sending empty placeholders to AppSheet.
  dropOptionalEmptyPayloadFields_(payload);

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
  const isBirthDateCol = colName === "data urodzenia 6" || colName === "data urodzenia";
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

  // For optional birth-date fields we must preserve true blank values.
  // Sending empty string may trigger AppSheet initial-value/default date behavior.
  if (isBirthDateCol) {
    const s = String(v === null || v === undefined ? "" : v).trim();
    if (!s || s === "-" || s === "//") return null;
    return s;
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
    const raw = String(v === null || v === undefined ? "" : v).trim().toLowerCase();
    if (raw === "(null) null-null") return "";
    if (typeof v === "number") return String(v);
    const s = String(v).replace(/[^\d]/g, "");
    if (!s) return "";
    return s;
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

function normalizeAccountNumbersForPayload_(primaryValue, fallbackValue) {
  const normalizedPrimary = normalizeAccountNumbersCsv_(primaryValue);
  if (normalizedPrimary) return normalizedPrimary;
  return normalizeAccountNumbersCsv_(fallbackValue);
}

function normalizeAccountNumbersCsv_(value) {
  const raw = String(value === null || value === undefined ? "" : value).trim();
  if (!raw) return "";

  const parts = raw
    .split(",")
    .map((s) => String(s || "").replace(/\s+/g, "").trim())
    .filter(Boolean);

  const seen = {};
  const out = [];
  for (let i = 0; i < parts.length; i++) {
    const v = parts[i];
    if (seen[v]) continue;
    seen[v] = true;
    out.push(v);
  }
  return out.join(",");
}

function ensureRequiredContactEmail_(emailValue, payload) {
  const raw = String(emailValue === null || emailValue === undefined ? "" : emailValue).trim();
  if (looksLikeEmail_(raw)) return raw;

  // Try any known fallback already present in payload.
  const fallbackSource = String(payload && payload["email przedstawiciela handlowego"] ? payload["email przedstawiciela handlowego"] : "").trim();
  if (looksLikeEmail_(fallbackSource)) return fallbackSource;

  const nipRaw = String(payload && (payload["NIP_Control"] || payload["nip"]) ? (payload["NIP_Control"] || payload["nip"]) : "").trim();
  const nipDigits = nipRaw.replace(/[^\d]/g, "") || "unknown";
  return "noemail+" + nipDigits + "@bibiv.invalid";
}

function ensureRequiredContactPhone_(phoneValue) {
  const direct = normalizePhoneDigits_(phoneValue);
  if (direct) return direct;
  return "";
}

function normalizePhoneDigits_(value) {
  const raw = String(value === null || value === undefined ? "" : value).trim().toLowerCase();
  if (!raw || raw === "(null) null-null") return "";
  return raw.replace(/[^\d]/g, "");
}

function looksLikeEmail_(value) {
  const s = String(value || "").trim();
  if (!s) return false;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s);
}

function dropOptionalEmptyPayloadFields_(payload) {
  const optionalCols = {
    "pesel przedstawiciela handlowego": true,
    "numer dowodu beneficjenta": true,
    "pesel osoby kontaktowej": true
  };

  for (const col in optionalCols) {
    if (!Object.prototype.hasOwnProperty.call(payload, col)) continue;
    if (isBlankOptionalPayloadValue_(payload[col])) delete payload[col];
  }
}

function isBlankOptionalPayloadValue_(value) {
  if (value === null || value === undefined) return true;
  if (typeof value !== "string") return false;
  const s = value.trim();
  return s === "" || s === "-" || s === "//";
}
