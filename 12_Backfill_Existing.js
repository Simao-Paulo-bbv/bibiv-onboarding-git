/** =========================
 *  EXISTING ROWS BACKFILL (missing fields only)
 *  - fills only empty target fields
 *  - DOES NOT change Status
 *  - DOES NOT call AppSheet API
 *  ========================= */

function runBackfillExistingMissingFields() {
  const runId = makeRunId_();
  const startedAt = Date.now();

  const lock = LockService.getScriptLock();
  const gotLock = lock.tryLock(CONFIG.LOCK_TIMEOUT_MS);
  if (!gotLock) {
    log_(runId, "WARN", "LOCK_NOT_ACQUIRED");
    return;
  }

  try {
    const sourceSS = openSpreadsheetWithLog_(runId, CONFIG.SOURCE_SPREADSHEET_ID, "SOURCE_SPREADSHEET_ID");
    const destSS = openSpreadsheetWithLog_(runId, CONFIG.DEST_SPREADSHEET_ID, "DEST_SPREADSHEET_ID");
    const source = getSheet_(sourceSS, CONFIG.SOURCE_SHEET_NAME, false);
    const dest = getSheet_(destSS, CONFIG.DEST_SHEET_NAME, false);
    if (!source || !dest) {
      log_(runId, "ERROR", "SHEET_MISSING", { sourceOk: !!source, destOk: !!dest });
      return;
    }

    if (CONFIG.FEATURES.ENFORCE_DEST_HEADERS) enforceDestHeaders_(runId, dest);
    const mapping = buildMapping_(runId, source, dest);
    ensureSourceMarkColumn_(runId, source, mapping);

    const res = backfillExistingMissingFields_(runId, mapping, source, dest, startedAt);
    log_(runId, "INFO", "BACKFILL_MANUAL_END", {
      rows: res.rows || 0,
      cells: res.cells || 0,
      candidates: res.candidates || 0,
      apiCalls: res.apiCalls || 0,
      elapsedMs: Date.now() - startedAt
    });
  } catch (e) {
    log_(runId, "ERROR", "BACKFILL_MANUAL_FATAL", { message: String(e), stack: e && e.stack ? String(e.stack) : "" });
    throw e;
  } finally {
    lock.releaseLock();
  }
}

function backfillExistingMissingFields_(runId, mapping, source, dest, startedAt) {
  const out = { rows: 0, cells: 0, candidates: 0, apiCalls: 0, examined: 0 };

  if (!CONFIG.BACKFILL_EXISTING_ENABLED) {
    log_(runId, "INFO", "BACKFILL_SKIP_DISABLED");
    return out;
  }
  if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 3000) {
    log_(runId, "INFO", "BACKFILL_SKIP_RUNTIME_NEAR_LIMIT");
    return out;
  }

  const lastRow = dest.getLastRow();
  if (lastRow < 2) {
    log_(runId, "INFO", "BACKFILL_NONE", { reason: "DEST_EMPTY" });
    return out;
  }

  const idx = {
    nip: mapping.destKey.nipControlIdx != null ? mapping.destKey.nipControlIdx : mapping.destKey.nipIdx,
    submitted: mapping.destKey.submittedIdx,
    bankAccount: mapping.dstIndex["numer rachunku bankowego"],
    bankBic: mapping.dstIndex["swift/bic"],
    bankName: mapping.dstIndex["Bank name"],
    bankAddress: mapping.dstIndex["Bank address"],
    bankCity: mapping.dstIndex["Bank city"],
    repEmail: mapping.dstIndex["email przedstawiciela handlowego"],
    repName: mapping.dstIndex["imię i nazwisko przedstawiciela handlowego"],
    repPesel: mapping.dstIndex["pesel przedstawiciela handlowego"],
    repPhone: mapping.dstIndex["numer telefonu przedstawiciela handlowego"]
  };

  const missingDestCols = [];
  ["bankBic", "bankName", "bankAddress", "bankCity", "repEmail", "repName", "repPesel", "repPhone"].forEach((k) => {
    if (idx[k] == null) missingDestCols.push(k);
  });
  if (missingDestCols.length) {
    log_(runId, "WARN", "BACKFILL_SKIP_DEST_COLS_MISSING", { missing: missingDestCols.join(",") });
    return out;
  }

  const maxRows = Number(CONFIG.BACKFILL_EXISTING_MAX_ROWS_PER_RUN || 20) || 20;
  const scanLastN = Number(CONFIG.BACKFILL_EXISTING_SCAN_LAST_N || 0) || 0;
  const scanRowStart = (scanLastN && lastRow > scanLastN) ? Math.max(2, lastRow - scanLastN + 1) : 2;

  const colsToFetch = uniqueSortedIndices_([
    idx.nip, idx.submitted, idx.bankAccount,
    idx.bankBic, idx.bankName, idx.bankAddress, idx.bankCity,
    idx.repEmail, idx.repName, idx.repPesel, idx.repPhone
  ]);
  if (!colsToFetch.length) return out;

  const minIdx = colsToFetch[0] + 1;
  const maxIdx = colsToFetch[colsToFetch.length - 1] + 1;
  const width = maxIdx - minIdx + 1;
  const raw = dest.getRange(scanRowStart, minIdx, lastRow - scanRowStart + 1, width).getValues();

  const candidates = [];
  for (let i = raw.length - 1; i >= 0; i--) {
    if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 2500) break;

    const rowNum = scanRowStart + i;
    const row = raw[i];
    const v = (colIdx) => row[(colIdx + 1) - minIdx];

    const nip = String(v(idx.nip) || "").trim();
    const submitted = v(idx.submitted);
    const repEmail = v(idx.repEmail);
    const repName = v(idx.repName);
    const repPesel = v(idx.repPesel);
    const repPhone = v(idx.repPhone);
    const bankName = v(idx.bankName);
    const bankAddress = v(idx.bankAddress);
    const bankCity = v(idx.bankCity);
    const bankBic = v(idx.bankBic);
    const accountRaw = v(idx.bankAccount);
    const accountNorm = normalizeBackfillBankAccount_(accountRaw);

    const needsRep = isBlankForBackfill_(repEmail) || isBlankForBackfill_(repName) || isBlankForBackfill_(repPesel) || isBlankForBackfill_(repPhone);
    const needsBank = !!accountNorm && (isBlankForBackfill_(bankBic) || isBlankForBackfill_(bankName) || isBlankForBackfill_(bankAddress) || isBlankForBackfill_(bankCity));
    if (!needsRep && !needsBank) continue;

    const key = nip ? makeDedupeKey_(nip, submitted) : "";
    candidates.push({
      rowNum: rowNum,
      key: key,
      nip: nip,
      submitted: submitted,
      account: accountNorm,
      needsRep: needsRep,
      needsBank: needsBank,
      current: {
        repEmail: repEmail,
        repName: repName,
        repPesel: repPesel,
        repPhone: repPhone,
        bankBic: bankBic,
        bankName: bankName,
        bankAddress: bankAddress,
        bankCity: bankCity
      }
    });
  }

  out.candidates = candidates.length;
  if (!candidates.length) {
    log_(runId, "INFO", "BACKFILL_NONE", { reason: "NO_MISSING_TARGET_FIELDS", scanRowStart: scanRowStart, lastRow: lastRow });
    return out;
  }

  const repMaps = buildSourceRepMapForBackfill_(runId, mapping, source, candidates, startedAt);
  const useIbanApi = !!CONFIG.BACKFILL_EXISTING_USE_IBAN_API && hasGovApiConfigForBackfill_();
  const ibanCache = {};

  for (let i = 0; i < candidates.length; i++) {
    if (out.rows >= maxRows) break;
    if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 1800) break;
    const c = candidates[i];
    out.examined++;
    const updates = [];

    if (c.needsRep) {
      const rep =
        (c.key ? (repMaps.byKey[c.key] || null) : null) ||
        (c.nip ? (repMaps.byNip[c.nip] || null) : null) ||
        {};
      const repEmail = sanitizeBackfillText_("repEmail", rep.repEmail);
      const repName = sanitizeBackfillText_("repName", rep.repName);
      const repPesel = sanitizeBackfillText_("repPesel", rep.repPesel);
      const repPhone = sanitizeBackfillText_("repPhone", rep.repPhone);

      if (isBlankForBackfill_(c.current.repEmail) && repEmail) updates.push({ col: idx.repEmail + 1, value: repEmail });
      if (isBlankForBackfill_(c.current.repName) && repName) updates.push({ col: idx.repName + 1, value: repName });
      if (isBlankForBackfill_(c.current.repPesel) && repPesel) updates.push({ col: idx.repPesel + 1, value: repPesel });
      if (isBlankForBackfill_(c.current.repPhone) && repPhone) updates.push({ col: idx.repPhone + 1, value: repPhone });
    }

    if (c.needsBank && c.account && useIbanApi) {
      let meta = ibanCache[c.account];
      if (!meta) {
        meta = fetchIbanMetaForBackfill_(runId, c.rowNum, c.account);
        ibanCache[c.account] = meta || { bankName: "", address: "", city: "" };
        out.apiCalls++;
      }
      if (meta) {
        if (isBlankForBackfill_(c.current.bankBic) && meta.bic) {
          updates.push({ col: idx.bankBic + 1, value: meta.bic });
        }
        if (isBlankForBackfill_(c.current.bankName) && meta.bankName) updates.push({ col: idx.bankName + 1, value: meta.bankName });
        if (isBlankForBackfill_(c.current.bankAddress) && meta.address) updates.push({ col: idx.bankAddress + 1, value: meta.address });
        if (isBlankForBackfill_(c.current.bankCity) && meta.city) updates.push({ col: idx.bankCity + 1, value: meta.city });
      }
    }

    if (!updates.length) continue;
    for (let j = 0; j < updates.length; j++) {
      dest.getRange(c.rowNum, updates[j].col).setValue(updates[j].value);
    }
    out.rows++;
    out.cells += updates.length;
  }

  log_(runId, "INFO", "BACKFILL_DONE", {
    rows: out.rows,
    cells: out.cells,
    candidates: out.candidates,
    apiCalls: out.apiCalls,
    examined: out.examined
  });

  return out;
}

function buildSourceRepMapForBackfill_(runId, mapping, source, candidates, startedAt) {
  const out = { byKey: {}, byNip: {} };
  const keysToFind = {};
  const nipsToFind = {};

  for (let i = 0; i < candidates.length; i++) {
    const c = candidates[i];
    if (!c.needsRep) continue;
    if (c.key) keysToFind[c.key] = true;
    if (c.nip) nipsToFind[String(c.nip).trim()] = true;
  }
  const keysCount = Object.keys(keysToFind).length;
  const nipsCount = Object.keys(nipsToFind).length;
  if (!keysCount && !nipsCount) return out;

  const srcNipIdx = mapping.sourceKey.nipIdx;
  const srcDateIdx = mapping.sourceKey.dateIdx;
  const srcRepEmailIdx = mapping.srcIndex[normalizeKey_("email przedstawiciela handlowego")];
  const srcRepNameIdx = mapping.srcIndex[normalizeKey_("imię i nazwisko przedstawiciela handlowego")];
  const srcRepPeselIdx = mapping.srcIndex[normalizeKey_("pesel przedstawiciela handlowego")];
  const srcRepPhoneIdx = mapping.srcIndex[normalizeKey_("numer telefonu przedstawiciela handlowego")];

  if (srcNipIdx == null || srcDateIdx == null || srcRepEmailIdx == null || srcRepNameIdx == null || srcRepPeselIdx == null || srcRepPhoneIdx == null) {
    log_(runId, "WARN", "BACKFILL_SOURCE_COLS_MISSING", {
      srcNipIdx: srcNipIdx,
      srcDateIdx: srcDateIdx,
      srcRepEmailIdx: srcRepEmailIdx,
      srcRepNameIdx: srcRepNameIdx,
      srcRepPeselIdx: srcRepPeselIdx,
      srcRepPhoneIdx: srcRepPhoneIdx
    });
    return out;
  }

  const sourceLastRow = source.getLastRow();
  if (sourceLastRow < 2) return out;

  const srcCols = uniqueSortedIndices_([srcNipIdx, srcDateIdx, srcRepEmailIdx, srcRepNameIdx, srcRepPeselIdx, srcRepPhoneIdx]);
  const minIdx = srcCols[0] + 1;
  const maxIdx = srcCols[srcCols.length - 1] + 1;
  const width = maxIdx - minIdx + 1;
  const values = source.getRange(2, minIdx, sourceLastRow - 1, width).getValues();

  for (let i = 0; i < values.length; i++) {
    if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 1600) break;
    const row = values[i];
    const v = (colIdx) => row[(colIdx + 1) - minIdx];

    const nip = String(v(srcNipIdx) || "").trim();
    if (!nip) continue;
    const sub = v(srcDateIdx);
    const key = makeDedupeKey_(nip, sub);
    const repEmail = sanitizeBackfillText_("repEmail", v(srcRepEmailIdx));
    const repName = sanitizeBackfillText_("repName", v(srcRepNameIdx));
    const repPesel = sanitizeBackfillText_("repPesel", v(srcRepPeselIdx));
    const repPhone = sanitizeBackfillText_("repPhone", v(srcRepPhoneIdx));

    if (keysToFind[key]) {
      if (!out.byKey[key]) out.byKey[key] = { repEmail: "", repName: "", repPesel: "", repPhone: "" };
      const recByKey = out.byKey[key];
      if (!recByKey.repEmail && repEmail) recByKey.repEmail = repEmail;
      if (!recByKey.repName && repName) recByKey.repName = repName;
      if (!recByKey.repPesel && repPesel) recByKey.repPesel = repPesel;
      if (!recByKey.repPhone && repPhone) recByKey.repPhone = repPhone;
    }

    const nipKey = String(nip || "").trim();
    if (nipsToFind[nipKey]) {
      if (!out.byNip[nipKey]) out.byNip[nipKey] = { repEmail: "", repName: "", repPesel: "", repPhone: "" };
      const recByNip = out.byNip[nipKey];
      if (!recByNip.repEmail && repEmail) recByNip.repEmail = repEmail;
      if (!recByNip.repName && repName) recByNip.repName = repName;
      if (!recByNip.repPesel && repPesel) recByNip.repPesel = repPesel;
      if (!recByNip.repPhone && repPhone) recByNip.repPhone = repPhone;
    }
  }

  return out;
}

function fetchIbanMetaForBackfill_(runId, rowNum, normalizedAccount) {
  try {
    const res = fetchGovApiGet_(CONFIG.GOV_IBAN_PATH, {
      country_code: "PL",
      account_number: String(normalizedAccount || "")
    });
    if (res.httpCode !== 200) {
      log_(runId, "WARN", "BACKFILL_IBAN_HTTP", { rowNum: rowNum, httpCode: res.httpCode });
      return null;
    }
    const meta = pickBankMetaFromIban_(res.parsed);
    meta.validIban = isValidIbanResponse_(res.parsed);
    return {
      bic: String(meta.bic || "").trim(),
      bankName: String(meta.bankName || "").trim(),
      address: String(meta.address || "").trim(),
      city: String(meta.city || "").trim(),
      validIban: !!meta.validIban
    };
  } catch (e) {
    log_(runId, "WARN", "BACKFILL_IBAN_FAIL", { rowNum: rowNum, err: String(e).slice(0, 300) });
    return null;
  }
}

function hasGovApiConfigForBackfill_() {
  const base = String((CONFIG && CONFIG.GOV_API_BASE_URL) || "").trim();
  const key = String((CONFIG && CONFIG.GOV_API_KEY) || "").trim();
  return base !== "" && key !== "";
}

function normalizeBackfillBankAccount_(value) {
  if (typeof normalizeBankAccountForIban_ === "function") {
    return normalizeBankAccountForIban_(value);
  }
  const raw = String(value === null || value === undefined ? "" : value).trim();
  if (!raw) return "";
  const compact = raw.replace(/[\s\-–—]+/g, "");
  return compact.replace(/^[A-Za-z]{2}/, "");
}

function sanitizeBackfillText_(fieldName, value) {
  if (value === null || value === undefined) return "";
  let s = String(value).trim();
  if (!s) return "";
  if (fieldName === "repPhone" && s.toLowerCase() === "(null) null-null") return "";
  return s;
}

function isBlankForBackfill_(value) {
  if (value === null || value === undefined) return true;
  return String(value).trim() === "";
}

function uniqueSortedIndices_(arr) {
  const seen = {};
  const out = [];
  for (let i = 0; i < (arr || []).length; i++) {
    const v = arr[i];
    if (v == null) continue;
    const n = Number(v);
    if (!isFinite(n)) continue;
    const k = String(n);
    if (seen[k]) continue;
    seen[k] = true;
    out.push(n);
  }
  out.sort(function (a, b) { return a - b; });
  return out;
}
