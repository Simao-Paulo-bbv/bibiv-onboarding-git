/** =========================
 *  GOV APIs (REGON + VAT + IBAN + KNF)
 *  ========================= */
function callMfAndWrite_(runId, dest, mapping, rowNum, nip, dateStr) {
  const nipClean = normalizeNipForApi_(nip);
  const govBase = String((CONFIG && CONFIG.GOV_API_BASE_URL) || "").trim();
  const govKey = String((CONFIG && CONFIG.GOV_API_KEY) || "").trim();
  const hasGovConfig = govBase !== "" && govKey !== "";
  if (!hasGovConfig) {
    log_(runId, "ERROR", "GOV_CONFIG_REQUIRED_FOR_VAT", { rowNum, nip: nipClean });
    return { ok: false, httpCode: 0, rateLimited: false, reason: "GOV_CONFIG_REQUIRED" };
  }

  const companyNameIdx = (mapping && mapping.dstIndex) ? mapping.dstIndex["nazwa firmy"] : null;
  const companyNameFromRow = companyNameIdx != null
    ? String(dest.getRange(rowNum, companyNameIdx + 1).getValue() || "").trim()
    : "";

  // --- 1) REGON (by NIP) ---
  let nameFromRegon = "";
  let regonFromRegon = "";
  let residenceFromRegon = "";
  let regonHttpCode = 0;
  if (hasGovConfig) {
    try {
      log_(runId, "INFO", "GOV_REGON_CALL", { rowNum, nip: nipClean });
      const regonRes = fetchGovApiGet_(CONFIG.GOV_REGON_PATH, { nip: nipClean });
      regonHttpCode = regonRes.httpCode;
      if (regonRes.httpCode === 200) {
        nameFromRegon = pickNameFromRegon_(regonRes.parsed, "", nipClean);
        regonFromRegon = pickRegonFromRegon_(regonRes.parsed, "", nipClean);
        residenceFromRegon = pickResidenceAddressFromRegon_(regonRes.parsed, "", nipClean);
        log_(runId, "INFO", "GOV_REGON_OK", {
          rowNum,
          nip: nipClean,
          hasName: !!nameFromRegon,
          hasRegon: !!regonFromRegon,
          hasResidenceAddress: !!residenceFromRegon
        });
      } else {
        log_(runId, "WARN", "GOV_REGON_HTTP", { rowNum, httpCode: regonRes.httpCode, nip: nipClean });
      }
    } catch (e) {
      log_(runId, "WARN", "GOV_REGON_FETCH_ERROR", { rowNum, nip: nipClean, err: String(e).slice(0, 400) });
      return { ok: false, httpCode: 0, rateLimited: false, reason: "REGON_FETCH_ERROR" };
    }
  }

  // REGON is mandatory step #1 for every record.
  // If REGON fails or returns no data for NIP, stop processing this row.
  if (hasGovConfig) {
    if (regonHttpCode !== 200) {
      return { ok: false, httpCode: regonHttpCode, rateLimited: false, reason: "REGON_HTTP_" + String(regonHttpCode || 0) };
    }
    const hasAnyRegonData = !!(nameFromRegon || regonFromRegon || residenceFromRegon);
    if (!hasAnyRegonData) {
      log_(runId, "WARN", "GOV_REGON_NO_DATA", { rowNum, nip: nipClean });
      return { ok: false, httpCode: 200, rateLimited: false, reason: "REGON_NO_DATA" };
    }
  }

  // --- 2) VAT ---
  log_(runId, "INFO", "MF_CALL", { rowNum });
  let vatHttpCode = 0;
  let vatBody = "";
  let vatParsed = null;

  try {
    const vatRes = fetchGovApiGet_(CONFIG.GOV_VAT_PATH, {
      nip: nipClean,
      date: String(dateStr || "").trim()
    });
    vatHttpCode = vatRes.httpCode;
    vatBody = vatRes.body;
    vatParsed = vatRes.parsed;
  } catch (e) {
    log_(runId, "WARN", "MF_FETCH_ERROR", { rowNum, err: String(e).slice(0, 400) });
    return { ok: false, httpCode: 0, rateLimited: false, reason: "VAT_FETCH_ERROR" };
  }

  let subj = null;
  let vatHasRecognizedSubjectField = false;
  if (vatHttpCode === 200) subj = pickSubjectFromMf_(vatParsed);
  if (vatHttpCode === 200) vatHasRecognizedSubjectField = hasRecognizedVatSubjectField_(vatParsed);

  // Retry strategy for GOV VAT when subject is null:
  // 1) retry with today's date
  // 2) retry without date param
  if (vatHttpCode === 200 && !subj && hasGovConfig) {
    const submittedDate = String(dateStr || "").trim();
    const todayDate = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");

    if (submittedDate && submittedDate !== todayDate) {
      try {
        const retryToday = fetchGovApiGet_(CONFIG.GOV_VAT_PATH, { nip: nipClean, date: todayDate });
        log_(runId, "INFO", "MF_RETRY_TODAY", { rowNum, httpCode: retryToday.httpCode, date: todayDate });
        if (retryToday.httpCode === 200) {
          vatHttpCode = retryToday.httpCode;
          vatBody = retryToday.body;
          vatParsed = retryToday.parsed;
          subj = pickSubjectFromMf_(vatParsed);
          vatHasRecognizedSubjectField = hasRecognizedVatSubjectField_(vatParsed);
        }
      } catch (e) {
        log_(runId, "WARN", "MF_RETRY_TODAY_FAIL", { rowNum, err: String(e).slice(0, 300) });
      }
    }

    if (!subj) {
      try {
        const retryNoDate = fetchGovApiGet_(CONFIG.GOV_VAT_PATH, { nip: nipClean });
        log_(runId, "INFO", "MF_RETRY_NODATE", { rowNum, httpCode: retryNoDate.httpCode });
        if (retryNoDate.httpCode === 200) {
          vatHttpCode = retryNoDate.httpCode;
          vatBody = retryNoDate.body;
          vatParsed = retryNoDate.parsed;
          subj = pickSubjectFromMf_(vatParsed);
          vatHasRecognizedSubjectField = hasRecognizedVatSubjectField_(vatParsed);
        }
      } catch (e) {
        log_(runId, "WARN", "MF_RETRY_NODATE_FAIL", { rowNum, err: String(e).slice(0, 300) });
      }
    }
  }

  if (isVerbose_()) {
    log_(runId, "INFO", "MF_RESULT", { rowNum, httpCode: vatHttpCode, bodySnippet: String(vatBody || "").slice(0, 700) });
  } else {
    log_(runId, "INFO", "MF_RESULT", { rowNum, httpCode: vatHttpCode });
  }

  if (vatHttpCode !== 200) {
    const rateLimited = vatHttpCode === 429;
    return { ok: false, httpCode: vatHttpCode, rateLimited, reason: rateLimited ? "RATE_LIMIT" : "VAT_HTTP_" + String(vatHttpCode) };
  }

  if (!subj && !vatHasRecognizedSubjectField) {
    log_(runId, "WARN", "MF_VAT_UNEXPECTED_RESPONSE", {
      rowNum,
      httpCode: vatHttpCode,
      bodySnippet: String(vatBody || "").slice(0, 500)
    });
    return { ok: false, httpCode: vatHttpCode, rateLimited: false, reason: "VAT_UNEXPECTED_RESPONSE" };
  }

  let acc = [];
  let isNotVat = false;
  if (subj) {
    writeIfColExists_(dest, mapping, rowNum, "statusVat", subj.statusVat || "");
    writeIfColExists_(dest, mapping, rowNum, "regon", String(subj.regon || regonFromRegon || ""));
    writeIfColExists_(dest, mapping, rowNum, "krs", String(subj.krs || ""));
    writeIfColExists_(dest, mapping, rowNum, "registrationLegalDate", subj.registrationLegalDate || "");
    writeIfColExists_(dest, mapping, rowNum, "workingAddress", subj.workingAddress || "");
    writeIfColExists_(dest, mapping, rowNum, "residenceAddress", subj.residenceAddress || "");

    acc = Array.isArray(subj.accountNumbers)
      ? subj.accountNumbers.filter(Boolean).map(String)
      : (subj.accountNumbers ? [String(subj.accountNumbers)] : []);
    writeIfColExists_(dest, mapping, rowNum, "accountNumbers", acc.join(","));
  } else {
    log_(runId, "WARN", "MF_NO_SUBJECT", { rowNum });
    isNotVat = true;
    writeIfColExists_(dest, mapping, rowNum, "statusVat", "Not VAT");
    if (regonFromRegon) writeIfColExists_(dest, mapping, rowNum, "regon", String(regonFromRegon || ""));
    writeIfColExists_(dest, mapping, rowNum, "registrationLegalDate", "");
    writeIfColExists_(dest, mapping, rowNum, "workingAddress", "");
    writeIfColExists_(dest, mapping, rowNum, "accountNumbers", "");
    writeIfColExists_(dest, mapping, rowNum, "residenceAddress", residenceFromRegon || "");
    log_(runId, "INFO", "MF_NOT_VAT", { rowNum, nip: nipClean });
  }

  if (nameFromRegon) {
    writeIfColExists_(dest, mapping, rowNum, "name_api", nameFromRegon);
  } else if (companyNameFromRow) {
    // Do not use VAT/MF subject name for name_api; it can be incomplete.
    // REGON is the canonical source. Source company name is only a last resort.
    writeIfColExists_(dest, mapping, rowNum, "name_api", companyNameFromRow);
  }
  if (regonFromRegon && (!subj || !String(subj.regon || "").trim())) {
    writeIfColExists_(dest, mapping, rowNum, "regon", String(regonFromRegon || ""));
  }

  // --- 3) IBAN metadata ---
  // Use source/main bank account column directly ("numer rachunku bankowego"), country fixed to PL.
  if (hasGovConfig) {
    const mainAccount = getMainBankAccountForIban_(dest, mapping, rowNum);
    if (mainAccount) {
      if (hasCompleteIbanMetadata_(dest, mapping, rowNum)) {
        log_(runId, "INFO", "GOV_IBAN_SKIP_ALREADY_PRESENT", {
          rowNum,
          accountLast4: mainAccount.slice(-4)
        });
      } else {
        const cachedBankMeta = getCachedIbanMeta_(mainAccount);
        if (cachedBankMeta && hasAnyIbanMeta_(cachedBankMeta)) {
          writeBankMetaToRow_(dest, mapping, rowNum, cachedBankMeta);
          log_(runId, "INFO", "GOV_IBAN_CACHE_HIT", {
            rowNum,
            accountLast4: mainAccount.slice(-4),
            hasBic: !!cachedBankMeta.bic,
            hasBankName: !!cachedBankMeta.bankName,
            hasAddress: !!cachedBankMeta.address,
            hasCity: !!cachedBankMeta.city
          });
        } else {
          try {
            log_(runId, "INFO", "GOV_IBAN_CALL", {
              rowNum,
              countryCode: "PL",
              accountLast4: mainAccount.slice(-4)
            });
            const ibanRes = fetchGovApiGet_(CONFIG.GOV_IBAN_PATH, {
              country_code: "PL",
              account_number: mainAccount
            });
            if (ibanRes.httpCode === 200) {
              const bankMeta = pickBankMetaFromIban_(ibanRes.parsed);
              writeBankMetaToRow_(dest, mapping, rowNum, bankMeta);
              putCachedIbanMeta_(mainAccount, bankMeta);
              log_(runId, "INFO", "GOV_IBAN_OK", {
                rowNum,
                hasBic: !!bankMeta.bic,
                hasBankName: !!bankMeta.bankName,
                hasAddress: !!bankMeta.address,
                hasCity: !!bankMeta.city
              });
            } else {
              log_(runId, "WARN", "GOV_IBAN_HTTP", { rowNum, httpCode: ibanRes.httpCode });
            }
          } catch (e) {
            log_(runId, "WARN", "GOV_IBAN_FETCH_ERROR", { rowNum, err: String(e).slice(0, 400) });
          }
        }
      }
    } else {
      log_(runId, "INFO", "GOV_IBAN_SKIP_NO_MAIN_ACCOUNT", { rowNum });
    }
  }

  // --- 4) KNF/RPK verification ---
  // Non-blocking: KNF API availability must not prevent AppSheet Add.
  writeKnfVerifiedForRow_(runId, dest, mapping, rowNum, nipClean, {
    skipIfPresent: true
  });

  const nipControlIdx = mapping.destKey.nipControlIdx;
  if (nipControlIdx != null) dest.getRange(rowNum, nipControlIdx + 1).setValue(nipClean);

  return subj || isNotVat
    ? { ok: true, httpCode: vatHttpCode, rateLimited: false, reason: (isNotVat ? "NOT_VAT" : "OK") }
    : { ok: false, httpCode: vatHttpCode, rateLimited: false, reason: "NO_SUBJECT" };
}

function pickSubjectFromMf_(parsed) {
  try {
    // GOV VAT response
    if (parsed && parsed.data && parsed.data.result && parsed.data.result.subject) return parsed.data.result.subject;
    // Legacy MF response
    if (parsed && parsed.result && parsed.result.subject) return parsed.result.subject;
    const entries = parsed && parsed.result && parsed.result.entries;
    if (entries && entries.length && entries[0].subjects && entries[0].subjects.length) {
      return entries[0].subjects[0];
    }
    return null;
  } catch (e) {
    return null;
  }
}

function hasRecognizedVatSubjectField_(parsed) {
  try {
    if (parsed && parsed.data && parsed.data.result && Object.prototype.hasOwnProperty.call(parsed.data.result, "subject")) {
      return true;
    }
    if (parsed && parsed.result && Object.prototype.hasOwnProperty.call(parsed.result, "subject")) {
      return true;
    }
    const entries = parsed && parsed.result && parsed.result.entries;
    if (Array.isArray(entries)) return true;
    return false;
  } catch (e) {
    return false;
  }
}

function fetchGovApiGet_(path, query) {
  const base = String((CONFIG && CONFIG.GOV_API_BASE_URL) || "").trim().replace(/\/+$/, "");
  const key = String((CONFIG && CONFIG.GOV_API_KEY) || "").trim();
  const p = String(path || "").trim();

  const queryParts = [];
  const src = query || {};
  for (const k in src) {
    const v = src[k];
    if (v === null || v === undefined) continue;
    const s = String(v).trim();
    if (!s) continue;
    queryParts.push(encodeURIComponent(k) + "=" + encodeURIComponent(s));
  }
  const qs = queryParts.length ? ("?" + queryParts.join("&")) : "";
  const url = base + p + qs;

  const res = UrlFetchApp.fetch(url, {
    method: "get",
    muteHttpExceptions: true,
    followRedirects: true,
    contentType: "application/json",
    validateHttpsCertificates: true,
    timeout: (CONFIG && CONFIG.GOV_TIMEOUT_MS) || 20000,
    headers: { "X-API-Key": key, Accept: "application/json" }
  });

  const body = res.getContentText() || "";
  return { httpCode: res.getResponseCode(), body: body, parsed: safeJsonParse_(body) };
}

function writeKnfVerifiedForRow_(runId, dest, mapping, rowNum, nip, options) {
  options = options || {};
  const colName = "KNF_verified";
  if (!mapping || !mapping.dstIndex || mapping.dstIndex[colName] == null) {
    log_(runId, "INFO", "GOV_KNF_SKIP_NO_COLUMN", { rowNum: rowNum });
    return { ok: false, skipped: true, reason: "NO_COLUMN" };
  }

  const nipClean = normalizeNipForApi_(nip);
  if (!nipClean) {
    log_(runId, "INFO", "GOV_KNF_SKIP_NO_NIP", { rowNum: rowNum });
    return { ok: false, skipped: true, reason: "NO_NIP" };
  }

  const colIdx = mapping.dstIndex[colName];
  const existing = String(dest.getRange(rowNum, colIdx + 1).getValue() || "").trim();
  if (options.skipIfPresent && existing) {
    log_(runId, "INFO", "GOV_KNF_SKIP_ALREADY_PRESENT", {
      rowNum: rowNum,
      nip: nipClean,
      value: existing
    });
    return { ok: true, skipped: true, value: existing, reason: "ALREADY_PRESENT" };
  }

  try {
    log_(runId, "INFO", "GOV_KNF_CALL", { rowNum: rowNum, nip: nipClean });
    const res = fetchGovApiGet_(CONFIG.GOV_KNF_RPK_PATH, { nip: nipClean });
    if (res.httpCode !== 200) {
      log_(runId, "WARN", "GOV_KNF_HTTP", {
        rowNum: rowNum,
        nip: nipClean,
        httpCode: res.httpCode,
        bodySnippet: String(res.body || "").slice(0, 500)
      });
      return { ok: false, httpCode: res.httpCode, reason: "HTTP_" + String(res.httpCode) };
    }

    const knfNumber = pickKnfRpkNumber_(res.parsed);
    if (!knfNumber) {
      log_(runId, "WARN", "GOV_KNF_NO_RPK", {
        rowNum: rowNum,
        nip: nipClean,
        bodySnippet: String(res.body || "").slice(0, 500)
      });
      return { ok: false, httpCode: res.httpCode, reason: "NO_RPK" };
    }

    dest.getRange(rowNum, colIdx + 1).setValue(knfNumber);
    log_(runId, "INFO", "GOV_KNF_OK", {
      rowNum: rowNum,
      nip: nipClean,
      knfNumber: knfNumber
    });
    return { ok: true, httpCode: res.httpCode, value: knfNumber };
  } catch (e) {
    log_(runId, "WARN", "GOV_KNF_FETCH_ERROR", {
      rowNum: rowNum,
      nip: nipClean,
      err: String(e).slice(0, 500)
    });
    return { ok: false, httpCode: 0, reason: "FETCH_ERROR" };
  }
}

function pickKnfRpkNumber_(parsed) {
  if (!parsed || typeof parsed !== "object") return "";

  const data = parsed.data && typeof parsed.data === "object" ? parsed.data : parsed;
  const direct = normalizeKnfRpkNumber_(
    data.registry_number ||
      data.registryNumber ||
      data.REGISTRY_NUMBER ||
      (data.record && data.record.REGISTRY_NUMBER)
  );
  if (direct) return direct;

  const found = findKnfRpkNumberDeep_(data, 0);
  return found ? normalizeKnfRpkNumber_(found) : "";
}

function findKnfRpkNumberDeep_(value, depth) {
  if (depth > 8 || value === null || value === undefined) return "";

  if (typeof value === "string" || typeof value === "number") {
    const normalized = normalizeKnfRpkNumber_(value);
    if (normalized) return normalized;
    return "";
  }

  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      const found = findKnfRpkNumberDeep_(value[i], depth + 1);
      if (found) return found;
    }
    return "";
  }

  if (typeof value === "object") {
    const preferredKeys = [
      "rpk",
      "RPK",
      "rpkNumber",
      "rpk_number",
      "knfNumber",
      "knf_number",
      "registrationNumber",
      "registration_number",
      "REGISTRY_NUMBER",
      "registerNumber",
      "register_number",
      "numerWpisu",
      "NumerWpisu",
      "numer_wpisu",
      "nrWpisu",
      "NrWpisu",
      "number",
      "Number"
    ];

    for (let i = 0; i < preferredKeys.length; i++) {
      const key = preferredKeys[i];
      if (!Object.prototype.hasOwnProperty.call(value, key)) continue;
      const found = findKnfRpkNumberDeep_(value[key], depth + 1);
      if (found) return found;
    }

    for (const key in value) {
      const found = findKnfRpkNumberDeep_(value[key], depth + 1);
      if (found) return found;
    }
  }

  return "";
}

function normalizeKnfRpkNumber_(value) {
  const raw = String(value === null || value === undefined ? "" : value).trim();
  if (!raw) return "";

  const direct = raw.match(/\bRPK[\s-]*\d{3,}\b/i);
  if (direct) return direct[0].replace(/[\s-]+/g, "").toUpperCase();

  return "";
}

function pickNameFromRegon_(parsed, regonClean, nipClean) {
  try {
    const rows =
      (parsed && parsed.results) ||
      (parsed && parsed.data && parsed.data.results) ||
      (parsed && parsed.data && Array.isArray(parsed.data) ? parsed.data : null) ||
      [];
    if (!rows || !rows.length) return "";

    const targetRegon = normalizeRegonQueryValue_(regonClean);
    const targetNip = normalizeNipForApi_(nipClean);
    let best = null;

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] || {};
      const name = String(r.Nazwa || r.name || "").trim();
      if (!name) continue;

      const rRegon = normalizeRegonQueryValue_(r.Regon || r.regon);
      const rNip = normalizeNipForApi_(r.Nip || r.nip);
      const typ = String(r.Typ || r.typ || "").trim().toUpperCase();
      const score =
        (rRegon && targetRegon && rRegon === targetRegon ? 200 : 0) +
        (rNip && targetNip && rNip === targetNip ? 120 : 0) +
        (rRegon ? 2 : 0) +
        (typ === "P" ? 10 : 0) +
        (typ === "LP" ? 5 : 0);

      if (!best || score > best.score) best = { score: score, name: name };
    }

    return best ? best.name : "";
  } catch (e) {
    return "";
  }
}

function pickRegonFromRegon_(parsed, regonClean, nipClean) {
  try {
    const rows =
      (parsed && parsed.results) ||
      (parsed && parsed.data && parsed.data.results) ||
      (parsed && parsed.data && Array.isArray(parsed.data) ? parsed.data : null) ||
      [];
    if (!rows || !rows.length) return "";

    const targetRegon = normalizeRegonQueryValue_(regonClean);
    const targetNip = normalizeNipForApi_(nipClean);
    let best = null;

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] || {};
      const reg = normalizeRegonQueryValue_(r.Regon || r.regon);
      if (!reg) continue;

      const rRegon = normalizeRegonQueryValue_(r.Regon || r.regon);
      const rNip = normalizeNipForApi_(r.Nip || r.nip);
      const typ = String(r.Typ || r.typ || "").trim().toUpperCase();
      const score =
        (rRegon && targetRegon && rRegon === targetRegon ? 200 : 0) +
        (rNip && targetNip && rNip === targetNip ? 120 : 0) +
        (rRegon ? 2 : 0) +
        (typ === "P" ? 10 : 0) +
        (typ === "LP" ? 5 : 0);

      if (!best || score > best.score) best = { score: score, regon: reg };
    }

    return best ? best.regon : "";
  } catch (e) {
    return "";
  }
}

function pickResidenceAddressFromRegon_(parsed, regonClean, nipClean) {
  try {
    const rows =
      (parsed && parsed.results) ||
      (parsed && parsed.data && parsed.data.results) ||
      (parsed && parsed.data && Array.isArray(parsed.data) ? parsed.data : null) ||
      [];
    if (!rows || !rows.length) return "";

    const targetRegon = normalizeRegonQueryValue_(regonClean);
    const targetNip = normalizeNipForApi_(nipClean);
    let best = null;

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] || {};
      const rRegon = normalizeRegonQueryValue_(r.Regon || r.regon);
      const rNip = normalizeNipForApi_(r.Nip || r.nip);
      const typ = String(r.Typ || r.typ || "").trim().toUpperCase();
      const score =
        (rRegon && targetRegon && rRegon === targetRegon ? 200 : 0) +
        (rNip && targetNip && rNip === targetNip ? 120 : 0) +
        (rRegon ? 2 : 0) +
        (typ === "P" ? 10 : 0) +
        (typ === "LP" ? 5 : 0);
      if (!best || score > best.score) best = { score: score, row: r };
    }

    if (!best || !best.row) return "";
    const row = best.row || {};
    const ulica = String(row.Ulica || row.ulica || "").trim();
    const nrNier = String(row.NrNieruchomosci || row.nrNieruchomosci || "").trim();
    const nrLok = String(row.NrLokalu || row.nrLokalu || "").trim();
    const kod = String(row.KodPocztowy || row.kodPocztowy || "").trim();
    const miejsc = String(row.Miejscowosc || row.miejscowosc || "").trim();

    // Requested format:
    // "Ulica" + " " + "NrNieruchomosci" + " " + "NrNieruchomosci" + "/" + "NrLokalu" + "," + "KodPocztowy" + " " + "Miejscowosc"
    const nr2 = nrNier && nrLok ? (nrNier + "/" + nrLok) : (nrNier || (nrLok ? ("/" + nrLok) : ""));
    const streetPart = [ulica, nrNier, nr2].filter(Boolean).join(" ").trim();
    const cityPart = [kod, miejsc].filter(Boolean).join(" ").trim();
    return [streetPart, cityPart].filter(Boolean).join(", ").trim();
  } catch (e) {
    return "";
  }
}

function pickFirstAccountNumber_(accounts) {
  if (!accounts || !accounts.length) return "";
  for (let i = 0; i < accounts.length; i++) {
    const normalized = String(accounts[i] || "").replace(/[\s\-–—]+/g, "").replace(/^[A-Za-z]{2}/, "").trim();
    if (normalized) return normalized;
  }
  return "";
}

function normalizeRegonQueryValue_(value) {
  const raw = String(value === null || value === undefined ? "" : value).trim();
  if (!raw) return "";
  return raw.replace(/[^\d]/g, "");
}

function normalizeNipForApi_(value) {
  const raw = String(value === null || value === undefined ? "" : value).trim();
  if (!raw) return "";
  const digits = raw.replace(/[^\d]/g, "");
  return digits || raw;
}

function getMainBankAccountForIban_(dest, mapping, rowNum) {
  try {
    const idx = mapping && mapping.dstIndex ? mapping.dstIndex["numer rachunku bankowego"] : null;
    if (idx == null) return "";
    const raw = dest.getRange(rowNum, idx + 1).getValue();
    const normalized = normalizeBankAccountForIban_(raw);
    return normalized;
  } catch (e) {
    return "";
  }
}

function normalizeBankAccountForIban_(value) {
  const raw = String(value === null || value === undefined ? "" : value).trim();
  if (!raw) return "";
  // remove spaces, dashes and optional country prefix (e.g. PL)
  const compact = raw.replace(/[\s\-–—]+/g, "");
  return compact.replace(/^[A-Za-z]{2}/, "");
}

function pickBankMetaFromIban_(parsed) {
  const out = { bic: "", bankName: "", address: "", city: "" };
  try {
    const root = (parsed && parsed.data) ? parsed.data : parsed;
    const data = (root && root.data) ? root.data : {};
    const bank = (data && data.bank) ? data.bank : {};
    out.bic = String(bank.bic || data.bic || "").trim();
    out.bankName = String(bank.bank_name || data.bank_name || "").trim();
    out.address = String(bank.address || data.address || "").trim();
    out.city = String(bank.city || data.city || "").trim();
  } catch (e) {}
  return out;
}

function hasCompleteIbanMetadata_(dest, mapping, rowNum) {
  const meta = getExistingIbanMetadata_(dest, mapping, rowNum);
  return !!(meta.bic && meta.bankName && meta.address && meta.city);
}

function getExistingIbanMetadata_(dest, mapping, rowNum) {
  function getCol(colName) {
    try {
      const idx = mapping && mapping.dstIndex ? mapping.dstIndex[colName] : null;
      if (idx == null) return "";
      return String(dest.getRange(rowNum, idx + 1).getValue() || "").trim();
    } catch (e) {
      return "";
    }
  }
  const swiftBic = getCol("swift/bic");
  return {
    bic: swiftBic,
    bankName: getCol("Bank name"),
    address: getCol("Bank address"),
    city: getCol("Bank city")
  };
}

function hasAnyIbanMeta_(meta) {
  if (!meta) return false;
  return !!(
    String(meta.bic || "").trim() ||
    String(meta.bankName || "").trim() ||
    String(meta.address || "").trim() ||
    String(meta.city || "").trim()
  );
}

function writeBankMetaToRow_(dest, mapping, rowNum, bankMeta) {
  const meta = bankMeta || {};
  const bic = String(meta.bic || "").trim();
  const bankName = String(meta.bankName || "").trim();
  const address = String(meta.address || "").trim();
  const city = String(meta.city || "").trim();

  if (bic) {
    writeIfColExists_(dest, mapping, rowNum, "swift/bic", bic);
  }
  if (bankName) writeIfColExists_(dest, mapping, rowNum, "Bank name", bankName);
  if (address) writeIfColExists_(dest, mapping, rowNum, "Bank address", address);
  if (city) writeIfColExists_(dest, mapping, rowNum, "Bank city", city);
}

function getCachedIbanMeta_(accountNumber) {
  try {
    const key = buildIbanCacheKey_(accountNumber);
    if (!key) return null;
    const cached = CacheService.getScriptCache().get(key);
    if (!cached) return null;
    return safeJsonParse_(cached);
  } catch (e) {
    return null;
  }
}

function putCachedIbanMeta_(accountNumber, bankMeta) {
  try {
    if (!hasAnyIbanMeta_(bankMeta)) return;
    const key = buildIbanCacheKey_(accountNumber);
    if (!key) return;
    CacheService.getScriptCache().put(key, JSON.stringify(bankMeta || {}), 21600);
  } catch (e) {
    // cache is only an optimization
  }
}

function buildIbanCacheKey_(accountNumber) {
  const normalized = normalizeBankAccountForIban_(accountNumber);
  if (!normalized) return "";
  return "iban_meta:" + normalized;
}

function safeDateForMf_(submittedOn) {
  if (submittedOn instanceof Date) {
    return Utilities.formatDate(submittedOn, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }
  const s = String(submittedOn || "").trim();
  if (s) {
    const d = new Date(s);
    if (!isNaN(d.getTime())) {
      return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
    }
  }
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
}
