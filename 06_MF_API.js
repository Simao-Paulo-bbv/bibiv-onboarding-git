/** =========================
 *  GOV APIs (VAT + REGON + IBAN)
 *  ========================= */
function callMfAndWrite_(runId, dest, mapping, rowNum, nip, dateStr) {
  const nipClean = String(nip || "").trim();
  const govBase = String((CONFIG && CONFIG.GOV_API_BASE_URL) || "").trim();
  const govKey = String((CONFIG && CONFIG.GOV_API_KEY) || "").trim();
  const hasGovConfig = govBase !== "" && govKey !== "";

  // --- 1) VAT ---
  log_(runId, "INFO", "MF_CALL", { rowNum });
  let vatHttpCode = 0;
  let vatBody = "";
  let vatParsed = null;

  if (hasGovConfig) {
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
      return { ok: false, httpCode: 0, rateLimited: false, reason: "FETCH_ERROR" };
    }
  } else {
    // Rollback path: legacy MF VAT API (direct/relay) when GOV config is not available.
    const relayUrl = String((CONFIG && CONFIG.MF_RELAY_URL) || "").trim();
    const useRelay = !!(CONFIG && CONFIG.MF_USE_RELAY) && relayUrl !== "";
    const url = useRelay
      ? relayUrl
      : CONFIG.MF_API_URL
          .replace("{nip}", encodeURIComponent(nipClean))
          .replace("{date}", encodeURIComponent(dateStr));
    try {
      const res = useRelay
        ? fetchViaMfRelay_(runId, rowNum, url, nipClean, dateStr)
        : UrlFetchApp.fetch(url, {
            method: "get",
            muteHttpExceptions: true,
            followRedirects: true,
            contentType: "application/json",
            validateHttpsCertificates: true,
            timeout: CONFIG.MF_TIMEOUT_MS
          });
      vatHttpCode = res.getResponseCode();
      vatBody = res.getContentText() || "";
      vatParsed = safeJsonParse_(vatBody);
    } catch (e) {
      log_(runId, "WARN", "MF_FETCH_ERROR", { rowNum, err: String(e).slice(0, 400) });
      return { ok: false, httpCode: 0, rateLimited: false, reason: "FETCH_ERROR" };
    }
  }

  let subj = null;
  if (vatHttpCode === 200) subj = pickSubjectFromMf_(vatParsed);

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
    const bodyStr = String(vatBody || "");
    const rateLimited = vatHttpCode === 429 || bodyStr.indexOf("WL-191") >= 0;
    return { ok: false, httpCode: vatHttpCode, rateLimited, reason: rateLimited ? "RATE_LIMIT" : "HTTP_" + String(vatHttpCode) };
  }

  let acc = [];
  if (subj) {
    writeIfColExists_(dest, mapping, rowNum, "statusVat", subj.statusVat || "");
    writeIfColExists_(dest, mapping, rowNum, "regon", String(subj.regon || ""));
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
  }

  // --- 2) REGON (name_api <- Nazwa) ---
  let nameFromRegon = "";
  let regonFromRegon = "";
  if (hasGovConfig) {
    try {
      const regonRes = fetchGovApiGet_(CONFIG.GOV_REGON_PATH, { nip: nipClean });
      if (regonRes.httpCode === 200) {
        nameFromRegon = pickNameFromRegon_(regonRes.parsed, nipClean);
        regonFromRegon = pickRegonFromRegon_(regonRes.parsed, nipClean);
      } else {
        log_(runId, "WARN", "GOV_REGON_HTTP", { rowNum, httpCode: regonRes.httpCode });
      }
    } catch (e) {
      log_(runId, "WARN", "GOV_REGON_FETCH_ERROR", { rowNum, err: String(e).slice(0, 400) });
    }
  }

  if (nameFromRegon) writeIfColExists_(dest, mapping, rowNum, "name_api", nameFromRegon);
  else if (subj) writeIfColExists_(dest, mapping, rowNum, "name_api", subj.name || "");
  if (regonFromRegon && (!subj || !String(subj.regon || "").trim())) {
    writeIfColExists_(dest, mapping, rowNum, "regon", String(regonFromRegon || ""));
  }

  // --- 3) IBAN metadata ---
  // Hotfix-safe mapping: write only to existing column "kod swift banku".
  // Additional columns (swift/bic, Bank name/address/city) require AppSheet schema migration first.
  if (hasGovConfig) {
    const firstAccount = pickFirstAccountNumber_(acc);
    if (firstAccount) {
      try {
        const ibanRes = fetchGovApiGet_(CONFIG.GOV_IBAN_PATH, {
          country_code: "PL",
          account_number: firstAccount
        });
        if (ibanRes.httpCode === 200) {
          const bankMeta = pickBankMetaFromIban_(ibanRes.parsed);
          writeIfColExists_(dest, mapping, rowNum, "kod swift banku", bankMeta.bic || "");
        } else {
          log_(runId, "WARN", "GOV_IBAN_HTTP", { rowNum, httpCode: ibanRes.httpCode });
        }
      } catch (e) {
        log_(runId, "WARN", "GOV_IBAN_FETCH_ERROR", { rowNum, err: String(e).slice(0, 400) });
      }
    }
  }

  const nipControlIdx = mapping.destKey.nipControlIdx;
  if (nipControlIdx != null) dest.getRange(rowNum, nipControlIdx + 1).setValue(nipClean);

  return subj
    ? { ok: true, httpCode: vatHttpCode, rateLimited: false, reason: "OK" }
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

function pickNameFromRegon_(parsed, nipClean) {
  try {
    const rows =
      (parsed && parsed.results) ||
      (parsed && parsed.data && parsed.data.results) ||
      (parsed && parsed.data && Array.isArray(parsed.data) ? parsed.data : null) ||
      [];
    if (!rows || !rows.length) return "";

    const targetNip = String(nipClean || "").trim();
    let best = null;

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] || {};
      const name = String(r.Nazwa || r.name || "").trim();
      if (!name) continue;

      const rNip = String(r.Nip || r.nip || "").trim();
      const typ = String(r.Typ || r.typ || "").trim().toUpperCase();
      const score =
        (rNip && targetNip && rNip === targetNip ? 100 : 0) +
        (typ === "P" ? 10 : 0) +
        (typ === "LP" ? 5 : 0);

      if (!best || score > best.score) best = { score: score, name: name };
    }

    return best ? best.name : "";
  } catch (e) {
    return "";
  }
}

function pickRegonFromRegon_(parsed, nipClean) {
  try {
    const rows =
      (parsed && parsed.results) ||
      (parsed && parsed.data && parsed.data.results) ||
      (parsed && parsed.data && Array.isArray(parsed.data) ? parsed.data : null) ||
      [];
    if (!rows || !rows.length) return "";

    const targetNip = String(nipClean || "").trim();
    let best = null;

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] || {};
      const reg = String(r.Regon || r.regon || "").trim();
      if (!reg) continue;

      const rNip = String(r.Nip || r.nip || "").trim();
      const typ = String(r.Typ || r.typ || "").trim().toUpperCase();
      const score =
        (rNip && targetNip && rNip === targetNip ? 100 : 0) +
        (typ === "P" ? 10 : 0) +
        (typ === "LP" ? 5 : 0);

      if (!best || score > best.score) best = { score: score, regon: reg };
    }

    return best ? best.regon : "";
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

function fetchViaMfRelay_(runId, rowNum, relayUrl, nip, dateStr) {
  const headers = { "Content-Type": "application/json" };
  const sharedToken = String((CONFIG && CONFIG.MF_RELAY_AUTH_TOKEN) || "").trim();
  const sharedTokenHeader = String((CONFIG && CONFIG.MF_RELAY_AUTH_HEADER) || "X-Relay-Auth").trim();
  const useGoogleIdToken = !CONFIG || CONFIG.MF_RELAY_USE_GOOGLE_ID_TOKEN !== false;

  if (useGoogleIdToken) {
    const idToken = getRelayIdTokenForAudience_(runId, rowNum, relayUrl);
    if (idToken) {
      headers["Authorization"] = "Bearer " + idToken;
    } else {
      log_(runId, "WARN", "MF_RELAY_IDTOKEN_MISSING", {
        rowNum,
        audience: normalizeRelayAudience_(relayUrl),
        serviceAccount: String((CONFIG && CONFIG.MF_RELAY_IDTOKEN_SERVICE_ACCOUNT) || "")
      });
    }
    if (sharedToken) headers[sharedTokenHeader] = sharedToken;
  } else if (sharedToken) {
    // Legacy mode for public relay: bearer is the relay shared secret.
    headers["Authorization"] = "Bearer " + sharedToken;
  }

  return UrlFetchApp.fetch(relayUrl, {
    method: "post",
    muteHttpExceptions: true,
    followRedirects: true,
    contentType: "application/json",
    validateHttpsCertificates: true,
    timeout: (CONFIG && CONFIG.MF_RELAY_TIMEOUT_MS) || 20000,
    headers: headers,
    payload: JSON.stringify({ nip: String(nip || ""), date: String(dateStr || "") })
  });
}

function getRelayIdTokenForAudience_(runId, rowNum, relayUrl) {
  const audience = normalizeRelayAudience_(relayUrl);
  const cacheKey = "mf_relay_idt:" + audience;
  try {
    const cache = CacheService.getScriptCache();
    const cached = String(cache.get(cacheKey) || "").trim();
    if (cached) return cached;
  } catch (e) {}

  const serviceAccount = String((CONFIG && CONFIG.MF_RELAY_IDTOKEN_SERVICE_ACCOUNT) || "").trim();
  if (!serviceAccount) return "";

  const url =
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/" +
    encodeURIComponent(serviceAccount) +
    ":generateIdToken";

  const res = UrlFetchApp.fetch(url, {
    method: "post",
    muteHttpExceptions: true,
    contentType: "application/json",
    headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
    payload: JSON.stringify({
      audience: audience,
      includeEmail: true
    }),
    timeout: 15000
  });

  if (res.getResponseCode() !== 200) {
    log_(runId, "WARN", "MF_RELAY_IDTOKEN_FETCH_FAIL", {
      rowNum,
      httpCode: res.getResponseCode(),
      bodySnippet: String(res.getContentText() || "").slice(0, 800),
      audience: audience,
      serviceAccount: serviceAccount
    });
    return "";
  }
  const parsed = safeJsonParse_(res.getContentText() || "");
  const token = String(parsed && parsed.token ? parsed.token : "").trim();
  if (!token) return "";

  try {
    const cache = CacheService.getScriptCache();
    cache.put(cacheKey, token, 300);
  } catch (e) {}
  return token;
}

function normalizeRelayAudience_(relayUrl) {
  const raw = String(relayUrl || "").trim();
  if (!raw) return "";
  const q = raw.indexOf("?");
  const noQuery = q >= 0 ? raw.slice(0, q) : raw;
  const slash = noQuery.indexOf("/", noQuery.indexOf("://") + 3);
  if (slash < 0) return noQuery.replace(/\/+$/, "");
  return noQuery.slice(0, slash).replace(/\/+$/, "");
}
