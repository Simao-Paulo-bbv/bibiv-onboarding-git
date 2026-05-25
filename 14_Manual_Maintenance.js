/** =========================
 *  MANUAL MAINTENANCE HELPERS
 *  =========================
 *
 * These functions are intended to be run manually from the Apps Script editor.
 */

const MANUAL_IBAN_REFRESH_CURSOR_KEY = "MANUAL_IBAN_REFRESH_CURSOR_ROW";
const MANUAL_PEOPLE_REFS_CURSOR_KEY = "MANUAL_PEOPLE_REFS_CURSOR_ROW";

const MANUAL_MAIN_STATUS_REPAIR = {
  ONBOARDING_ID: "",
  STATUS: "Init"
};

const MANUAL_KNF_VERIFICATION = {
  NIPS_TEXT: "",
  NIPS: [],
  FORCE_ALL_ROWS: false,
  OVERWRITE_EXISTING: false,
  MAX_ROWS: 250
};

function runManualRefreshIbanBankMetadata() {
  return refreshIbanBankMetadataRows_({
    forceAllRows: false,
    dryRun: false,
    maxRows: 250
  });
}

function runManualRefreshAllIbanBankMetadata() {
  return refreshIbanBankMetadataRows_({
    forceAllRows: true,
    dryRun: false,
    maxRows: 250
  });
}

function resetManualIbanRefreshCursor() {
  PropertiesService.getScriptProperties().deleteProperty(MANUAL_IBAN_REFRESH_CURSOR_KEY);
  return { ok: true, cursorReset: true };
}

function runManualAuditIbanBankMetadata() {
  return refreshIbanBankMetadataRows_({
    forceAllRows: false,
    dryRun: true
  });
}

function runManualRepairPeopleRefsFromPeopleList() {
  return repairPeopleRefsFromPeopleList_({
    dryRun: false,
    maxRows: 250
  });
}

function runManualAuditPeopleRefsFromPeopleList() {
  return repairPeopleRefsFromPeopleList_({
    dryRun: true,
    maxRows: 250
  });
}

function resetManualPeopleRefsCursor() {
  PropertiesService.getScriptProperties().deleteProperty(MANUAL_PEOPLE_REFS_CURSOR_KEY);
  return { ok: true, cursorReset: true };
}

function runManualRepairMainStatus(onboardingIdArg, statusArg) {
  const runId = makeRunId_("manual-main-status");
  const onboardingId = String(onboardingIdArg || MANUAL_MAIN_STATUS_REPAIR.ONBOARDING_ID || "").trim();
  const status = String(statusArg || MANUAL_MAIN_STATUS_REPAIR.STATUS || CONFIG.STATUS_TO_SEND || "").trim();

  if (!onboardingId) throw new Error("MANUAL_MAIN_STATUS_REPAIR.ONBOARDING_ID is blank.");
  if (!status) throw new Error("MANUAL_MAIN_STATUS_REPAIR.STATUS is blank.");

  const result = callAppSheet_(runId, CONFIG.APPSHEET_TABLE_MAIN, {
    ID: onboardingId,
    Status: status
  }, CONFIG.APPSHEET_ACTION_EDIT, onboardingId);

  log_(runId, "INFO", "MANUAL_MAIN_STATUS_REPAIR_DONE", {
    onboardingId: onboardingId,
    status: status
  });

  return {
    ok: true,
    onboardingId: onboardingId,
    status: status,
    response: result && result.parsed || null
  };
}

function runManualRefreshKnfVerified() {
  return refreshKnfVerifiedRows_({
    forceAllRows: true,
    overwriteExisting: false,
    maxRows: MANUAL_KNF_VERIFICATION.MAX_ROWS || 250
  });
}

function runManualRefreshKnfVerifiedForNips() {
  return refreshKnfVerifiedRows_({
    forceAllRows: true,
    overwriteExisting: !!MANUAL_KNF_VERIFICATION.OVERWRITE_EXISTING,
    maxRows: MANUAL_KNF_VERIFICATION.MAX_ROWS || 250,
    targetNips: getManualKnfVerificationNips_()
  });
}

function runManualRefreshKnfVerifiedForNipsText(nipsText, overwriteExistingArg) {
  return refreshKnfVerifiedRows_({
    forceAllRows: true,
    overwriteExisting: overwriteExistingArg === true,
    maxRows: MANUAL_KNF_VERIFICATION.MAX_ROWS || 250,
    targetNips: parseManualKnfVerificationNips_(nipsText)
  });
}

function runManualRefreshKnfVerifiedForceAll() {
  return refreshKnfVerifiedRows_({
    forceAllRows: true,
    overwriteExisting: true,
    maxRows: MANUAL_KNF_VERIFICATION.MAX_ROWS || 250
  });
}

function getManualKnfVerificationNips_() {
  return parseManualKnfVerificationNips_(
    String(MANUAL_KNF_VERIFICATION.NIPS_TEXT || "") + " " +
      (Array.isArray(MANUAL_KNF_VERIFICATION.NIPS) ? MANUAL_KNF_VERIFICATION.NIPS.join(" ") : "")
  );
}

function parseManualKnfVerificationNips_(nipsText) {
  const out = {};
  String(nipsText || "")
    .split(/[\s,;]+/)
    .forEach(value => {
      const nip = normalizeNipForApi_(value);
      if (nip) out[nip] = true;
    });

  const nips = Object.keys(out);
  if (!nips.length) throw new Error("Set MANUAL_KNF_VERIFICATION.NIPS_TEXT or NIPS.");
  return out;
}

function refreshKnfVerifiedRows_(options) {
  options = options || {};
  const runId = makeRunId_("manual-knf");
  const startedAt = Date.now();
  const maxRuntimeMs = Number(options.maxRuntimeMs || 300000);
  const maxRows = Math.max(1, Number(options.maxRows || 250));
  const overwriteExisting = !!options.overwriteExisting;
  const targetNips = options.targetNips || null;

  const lock = LockService.getScriptLock();
  const gotLock = lock.tryLock(CONFIG.LOCK_TIMEOUT_MS || 20000);
  if (!gotLock) {
    log_(runId, "WARN", "MANUAL_KNF_LOCK_NOT_ACQUIRED", {});
    return { ok: false, reason: "LOCK_NOT_ACQUIRED" };
  }

  try {
    const sourceSS = openSpreadsheetWithLog_(runId, CONFIG.SOURCE_SPREADSHEET_ID, "SOURCE_SPREADSHEET_ID");
    const destSS = openSpreadsheetWithLog_(runId, CONFIG.DEST_SPREADSHEET_ID, "DEST_SPREADSHEET_ID");
    const source = getSheet_(sourceSS, CONFIG.SOURCE_SHEET_NAME, false);
    const dest = getSheet_(destSS, CONFIG.DEST_SHEET_NAME, false);
    if (!source || !dest) throw new Error("Source or destination sheet missing.");

    if (CONFIG.FEATURES.ENFORCE_DEST_HEADERS) enforceDestHeaders_(runId, dest);
    const mapping = buildMapping_(runId, source, dest);
    validateManualKnfColumns_(mapping);

    const result = refreshKnfVerifiedRowsInSheet_(runId, dest, mapping, {
      startedAt: startedAt,
      maxRuntimeMs: maxRuntimeMs,
      maxRows: maxRows,
      overwriteExisting: overwriteExisting,
      targetNips: targetNips
    });

    log_(runId, "INFO", "MANUAL_KNF_REFRESH_END", Object.assign({}, result, {
      overwriteExisting: overwriteExisting,
      targeted: !!targetNips,
      elapsedMs: Date.now() - startedAt
    }));
    return result;
  } catch (e) {
    log_(runId, "ERROR", "MANUAL_KNF_REFRESH_FATAL", {
      message: String(e),
      stack: e && e.stack ? String(e.stack).slice(0, 3000) : ""
    });
    throw e;
  } finally {
    lock.releaseLock();
  }
}

function validateManualKnfColumns_(mapping) {
  const required = ["ID", "NIP_Control", "nip", "KNF_verified"];
  const missing = [];
  for (let i = 0; i < required.length; i++) {
    if (mapping.dstIndex[required[i]] == null) missing.push(required[i]);
  }
  if (missing.length) throw new Error("Missing destination columns for manual KNF refresh: " + missing.join(", "));
}

function refreshKnfVerifiedRowsInSheet_(runId, dest, mapping, options) {
  const out = {
    ok: true,
    scanned: 0,
    candidates: 0,
    updated: 0,
    skippedNoNip: 0,
    skippedTarget: 0,
    skippedAlreadyPresent: 0,
    failed: 0,
    stoppedEarly: false
  };

  const lastRow = dest.getLastRow();
  if (lastRow < 2) return out;

  const idx = {
    nipControl: mapping.dstIndex["NIP_Control"],
    nip: mapping.dstIndex["nip"],
    knf: mapping.dstIndex["KNF_verified"]
  };

  const data = dest.getRange(2, 1, lastRow - 1, dest.getLastColumn()).getValues();
  for (let i = 0; i < data.length; i++) {
    if (out.updated >= options.maxRows) break;
    if (Date.now() - Number(options.startedAt || Date.now()) > Number(options.maxRuntimeMs || 300000) - 15000) {
      out.stoppedEarly = true;
      break;
    }

    const rowNum = i + 2;
    const row = data[i];
    out.scanned++;

    const nip = normalizeNipForApi_(row[idx.nipControl] || row[idx.nip]);
    if (!nip) {
      out.skippedNoNip++;
      continue;
    }

    if (options.targetNips && !options.targetNips[nip]) {
      out.skippedTarget++;
      continue;
    }

    const existing = String(row[idx.knf] || "").trim();
    if (existing && !options.overwriteExisting) {
      out.skippedAlreadyPresent++;
      continue;
    }

    out.candidates++;
    const result = writeKnfVerifiedForRow_(runId, dest, mapping, rowNum, nip, {
      skipIfPresent: !options.overwriteExisting
    });
    if (result && result.ok && !result.skipped) {
      out.updated++;
    } else if (!result || !result.ok) {
      out.failed++;
    }
  }

  return out;
}

function refreshIbanBankMetadataRows_(options) {
  options = options || {};
  const runId = makeRunId_("manual-iban");
  const startedAt = Date.now();
  const maxRuntimeMs = Number(options.maxRuntimeMs || 300000);
  const maxRows = Number(options.maxRows || 100);
  const forceAllRows = !!options.forceAllRows;
  const dryRun = !!options.dryRun;

  const lock = LockService.getScriptLock();
  const gotLock = lock.tryLock(CONFIG.LOCK_TIMEOUT_MS || 20000);
  if (!gotLock) {
    log_(runId, "WARN", "MANUAL_IBAN_LOCK_NOT_ACQUIRED", {});
    return { ok: false, reason: "LOCK_NOT_ACQUIRED" };
  }

  try {
    const sourceSS = openSpreadsheetWithLog_(runId, CONFIG.SOURCE_SPREADSHEET_ID, "SOURCE_SPREADSHEET_ID");
    const destSS = openSpreadsheetWithLog_(runId, CONFIG.DEST_SPREADSHEET_ID, "DEST_SPREADSHEET_ID");
    const source = getSheet_(sourceSS, CONFIG.SOURCE_SHEET_NAME, false);
    const dest = getSheet_(destSS, CONFIG.DEST_SHEET_NAME, false);
    if (!source || !dest) throw new Error("Source or destination sheet missing.");

    if (CONFIG.FEATURES.ENFORCE_DEST_HEADERS) enforceDestHeaders_(runId, dest);
    const mapping = buildMapping_(runId, source, dest);
    validateManualIbanColumns_(mapping);

    const result = refreshIbanBankMetadataRowsInSheet_(runId, dest, mapping, {
      startedAt: startedAt,
      maxRuntimeMs: maxRuntimeMs,
      maxRows: maxRows,
      forceAllRows: forceAllRows,
      dryRun: dryRun
    });

    log_(runId, "INFO", "MANUAL_IBAN_REFRESH_END", Object.assign({}, result, {
      forceAllRows: forceAllRows,
      dryRun: dryRun,
      elapsedMs: Date.now() - startedAt
    }));
    return result;
  } catch (e) {
    log_(runId, "ERROR", "MANUAL_IBAN_REFRESH_FATAL", {
      message: String(e),
      stack: e && e.stack ? String(e.stack).slice(0, 3000) : ""
    });
    throw e;
  } finally {
    lock.releaseLock();
  }
}

function validateManualIbanColumns_(mapping) {
  const required = ["ID", "numer rachunku bankowego", "swift/bic", "Bank name", "Bank address", "Bank city"];
  const missing = [];
  for (let i = 0; i < required.length; i++) {
    if (mapping.dstIndex[required[i]] == null) missing.push(required[i]);
  }
  if (missing.length) throw new Error("Missing destination columns for manual IBAN refresh: " + missing.join(", "));
}

function refreshIbanBankMetadataRowsInSheet_(runId, dest, mapping, options) {
  const out = {
    ok: true,
    scanned: 0,
    candidates: 0,
    apiCalls: 0,
    sheetRowsUpdated: 0,
    skippedNoId: 0,
    skippedNoAccount: 0,
    skippedAlreadyComplete: 0,
    skippedIncompleteApiMeta: 0,
    skippedEmptyApiMeta: 0,
    failed: 0,
    stoppedEarly: false
  };

  const lastRow = dest.getLastRow();
  if (lastRow < 2) return out;
  const props = PropertiesService.getScriptProperties();
  const cursorRow = options.forceAllRows
    ? Math.max(2, Number(props.getProperty(MANUAL_IBAN_REFRESH_CURSOR_KEY) || 2))
    : 2;
  const startIndex = Math.max(0, Math.min(lastRow - 2, cursorRow - 2));

  const idx = {
    id: mapping.dstIndex["ID"],
    account: mapping.dstIndex["numer rachunku bankowego"],
    declaredSwift: mapping.dstIndex["kod swift banku"],
    bic: mapping.dstIndex["swift/bic"],
    bankName: mapping.dstIndex["Bank name"],
    bankAddress: mapping.dstIndex["Bank address"],
    bankCity: mapping.dstIndex["Bank city"]
  };

  const maxRows = Math.max(1, Number(options.maxRows || 100));
  const maxRuntimeMs = Math.max(10000, Number(options.maxRuntimeMs || 300000));
  const startedAt = Number(options.startedAt || Date.now());
  const data = dest.getRange(2, 1, lastRow - 1, dest.getLastColumn()).getValues();
  const cache = {};
  let lastVisitedRow = startIndex + 1;

  for (let i = startIndex; i < data.length; i++) {
    if (out.sheetRowsUpdated >= maxRows) break;
    if (Date.now() - startedAt > maxRuntimeMs - 15000) {
      out.stoppedEarly = true;
      break;
    }

    const rowNum = i + 2;
    lastVisitedRow = rowNum;
    const row = data[i];
    out.scanned++;

    const onboardingId = String(row[idx.id] || "").trim();
    if (!onboardingId) {
      out.skippedNoId++;
      continue;
    }

    const account = normalizeBankAccountForIban_(row[idx.account]);
    if (!account) {
      out.skippedNoAccount++;
      continue;
    }

    const existing = {
      bic: String(row[idx.bic] || "").trim(),
      bankName: String(row[idx.bankName] || "").trim(),
      address: String(row[idx.bankAddress] || "").trim(),
      city: String(row[idx.bankCity] || "").trim()
    };

    if (!options.forceAllRows && isCompleteManualIbanMeta_(existing)) {
      out.skippedAlreadyComplete++;
      continue;
    }

    out.candidates++;

    let meta = cache[account];
    if (!meta) {
      meta = fetchManualIbanBankMeta_(runId, rowNum, account);
      cache[account] = meta || { bic: "", bankName: "", address: "", city: "" };
      out.apiCalls++;
    }
    if (meta && !String(meta.bic || "").trim() && meta.validIban && idx.declaredSwift != null) {
      const declaredSwift = String(row[idx.declaredSwift] || "").replace(/\s+/g, "").trim().toUpperCase();
      if (declaredSwift) {
        meta.bic = declaredSwift;
        meta.bicFallback = true;
      }
    }

    if (!hasAnyManualIbanMeta_(meta)) {
      out.skippedEmptyApiMeta++;
      log_(runId, "WARN", "MANUAL_IBAN_INCOMPLETE_API_META", {
        rowNum: rowNum,
        onboardingId: onboardingId,
        accountLast4: account.slice(-4),
        hasBic: !!(meta && meta.bic),
        hasBankName: !!(meta && meta.bankName),
        hasAddress: !!(meta && meta.address),
        hasCity: !!(meta && meta.city)
      });
      continue;
    }
    if (!isCompleteManualIbanMeta_(meta)) {
      out.skippedIncompleteApiMeta++;
      log_(runId, "WARN", "MANUAL_IBAN_PARTIAL_API_META_WRITING_AVAILABLE_FIELDS", {
        rowNum: rowNum,
        onboardingId: onboardingId,
        accountLast4: account.slice(-4),
        hasBic: !!(meta && meta.bic),
        hasBankName: !!(meta && meta.bankName),
        hasAddress: !!(meta && meta.address),
        hasCity: !!(meta && meta.city)
      });
    }

    let verified = {};
    try {
      if (!options.dryRun) {
        writeManualIbanMetaToSheet_(dest, idx, rowNum, meta);
        verified = readManualIbanMetaFromSheet_(dest, idx, rowNum);
        out.sheetRowsUpdated++;
      }
      if (options.dryRun) out.sheetRowsUpdated++;
      log_(runId, "INFO", "MANUAL_IBAN_ROW_UPDATED", {
        rowNum: rowNum,
        onboardingId: onboardingId,
        accountLast4: account.slice(-4),
        cols: manualIbanMetaColumnSummary_(idx),
        sheetValues: options.dryRun ? {} : verified,
        dryRun: !!options.dryRun
      });
    } catch (e) {
      out.failed++;
      log_(runId, "WARN", "MANUAL_IBAN_ROW_UPDATE_FAILED", {
        rowNum: rowNum,
        onboardingId: onboardingId,
        err: String(e).slice(0, 900)
      });
    }
  }

  if (options.forceAllRows) {
    if (lastVisitedRow < lastRow && (out.stoppedEarly || out.sheetRowsUpdated >= maxRows)) {
      props.setProperty(MANUAL_IBAN_REFRESH_CURSOR_KEY, String(lastVisitedRow + 1));
      out.nextCursorRow = lastVisitedRow + 1;
    } else {
      props.deleteProperty(MANUAL_IBAN_REFRESH_CURSOR_KEY);
      out.nextCursorRow = "";
    }
  }

  return out;
}

function fetchManualIbanBankMeta_(runId, rowNum, account) {
  try {
    log_(runId, "INFO", "MANUAL_IBAN_API_CALL", {
      rowNum: rowNum,
      countryCode: "PL",
      accountLast4: String(account || "").slice(-4)
    });
    const res = fetchGovApiGet_(CONFIG.GOV_IBAN_PATH, {
      country_code: "PL",
      account_number: String(account || "")
    });
    if (res.httpCode !== 200) {
      log_(runId, "WARN", "MANUAL_IBAN_API_HTTP", { rowNum: rowNum, httpCode: res.httpCode });
      return null;
    }
    const meta = pickBankMetaFromIban_(res.parsed);
    meta.validIban = isValidIbanResponse_(res.parsed);
    return meta;
  } catch (e) {
    log_(runId, "WARN", "MANUAL_IBAN_API_FAIL", { rowNum: rowNum, err: String(e).slice(0, 900) });
    return null;
  }
}

function writeManualIbanMetaToSheet_(dest, idx, rowNum, meta) {
  const bic = String(meta && meta.bic || "").trim();
  const bankName = String(meta && meta.bankName || "").trim();
  const address = String(meta && meta.address || "").trim();
  const city = String(meta && meta.city || "").trim();

  if (bic) dest.getRange(rowNum, idx.bic + 1).setValue(bic);
  if (bankName) dest.getRange(rowNum, idx.bankName + 1).setValue(bankName);
  if (address) dest.getRange(rowNum, idx.bankAddress + 1).setValue(address);
  if (city) dest.getRange(rowNum, idx.bankCity + 1).setValue(city);
  SpreadsheetApp.flush();
}

function readManualIbanMetaFromSheet_(dest, idx, rowNum) {
  const out = {};
  out.bic = String(dest.getRange(rowNum, idx.bic + 1).getDisplayValue() || "").trim();
  out.bankName = String(dest.getRange(rowNum, idx.bankName + 1).getDisplayValue() || "").trim();
  out.address = String(dest.getRange(rowNum, idx.bankAddress + 1).getDisplayValue() || "").trim();
  out.city = String(dest.getRange(rowNum, idx.bankCity + 1).getDisplayValue() || "").trim();
  return out;
}

function manualIbanMetaColumnSummary_(idx) {
  return {
    bic: columnNumberToLetter_(idx.bic + 1),
    bankName: columnNumberToLetter_(idx.bankName + 1),
    bankAddress: columnNumberToLetter_(idx.bankAddress + 1),
    bankCity: columnNumberToLetter_(idx.bankCity + 1)
  };
}

function columnNumberToLetter_(columnNumber) {
  let n = Number(columnNumber || 0);
  let out = "";
  while (n > 0) {
    const rem = (n - 1) % 26;
    out = String.fromCharCode(65 + rem) + out;
    n = Math.floor((n - 1) / 26);
  }
  return out;
}

function isCompleteManualIbanMeta_(meta) {
  return !!(
    meta &&
    String(meta.bic || "").trim() &&
    String(meta.bankName || "").trim() &&
    String(meta.address || "").trim() &&
    String(meta.city || "").trim()
  );
}

function hasAnyManualIbanMeta_(meta) {
  return !!(
    meta &&
    (
      String(meta.bic || "").trim() ||
      String(meta.bankName || "").trim() ||
      String(meta.address || "").trim() ||
      String(meta.city || "").trim()
    )
  );
}

function repairPeopleRefsFromPeopleList_(options) {
  options = options || {};
  const runId = makeRunId_("manual-people-refs");
  const startedAt = Date.now();
  const maxRuntimeMs = Number(options.maxRuntimeMs || 300000);
  const maxRows = Math.max(1, Number(options.maxRows || 250));
  const dryRun = !!options.dryRun;

  const lock = LockService.getScriptLock();
  const gotLock = lock.tryLock(CONFIG.LOCK_TIMEOUT_MS || 20000);
  if (!gotLock) {
    log_(runId, "WARN", "MANUAL_PEOPLE_REFS_LOCK_NOT_ACQUIRED", {});
    return { ok: false, reason: "LOCK_NOT_ACQUIRED" };
  }

  try {
    const sourceSS = openSpreadsheetWithLog_(runId, CONFIG.SOURCE_SPREADSHEET_ID, "SOURCE_SPREADSHEET_ID");
    const destSS = openSpreadsheetWithLog_(runId, CONFIG.DEST_SPREADSHEET_ID, "DEST_SPREADSHEET_ID");
    const source = getSheet_(sourceSS, CONFIG.SOURCE_SHEET_NAME, false);
    const dest = getSheet_(destSS, CONFIG.DEST_SHEET_NAME, false);
    const people = getSheet_(destSS, CONFIG.APPSHEET_TABLE_PEOPLE, false);
    if (!source || !dest) throw new Error("Source or destination sheet missing.");
    if (!people) throw new Error("People_List sheet missing in DEST spreadsheet.");

    if (CONFIG.FEATURES.ENFORCE_DEST_HEADERS) enforceDestHeaders_(runId, dest);
    const mapping = buildMapping_(runId, source, dest);
    validateManualPeopleRefsColumns_(mapping);

    const peopleIndex = buildPeopleListRefIndex_(runId, people);
    const result = repairPeopleRefsRowsInSheet_(runId, dest, mapping, peopleIndex, {
      startedAt: startedAt,
      maxRuntimeMs: maxRuntimeMs,
      maxRows: maxRows,
      dryRun: dryRun
    });

    log_(runId, "INFO", "MANUAL_PEOPLE_REFS_END", Object.assign({}, result, {
      dryRun: dryRun,
      elapsedMs: Date.now() - startedAt
    }));
    return result;
  } catch (e) {
    log_(runId, "ERROR", "MANUAL_PEOPLE_REFS_FATAL", {
      message: String(e),
      stack: e && e.stack ? String(e.stack).slice(0, 3000) : ""
    });
    throw e;
  } finally {
    lock.releaseLock();
  }
}

function validateManualPeopleRefsColumns_(mapping) {
  const required = [
    "ID",
    CONFIG.MAIN_REF_COLS.CONTACT,
    CONFIG.MAIN_REF_COLS.MANAGER,
    CONFIG.MAIN_REF_COLS.BENEFICIAL,
    "imię i nazwisko osoby kontaktowej",
    "imię i nazwisko kierownika",
    "imię i nazwisko beneficjenta"
  ];
  const missing = [];
  for (let i = 0; i < required.length; i++) {
    if (mapping.dstIndex[required[i]] == null) missing.push(required[i]);
  }
  if (missing.length) throw new Error("Missing destination columns for manual People refs repair: " + missing.join(", "));
}

function buildPeopleListRefIndex_(runId, peopleSheet) {
  const headers = getHeaderRow_(peopleSheet);
  const idx = indexByExact_(headers);
  const required = [
    CONFIG.PEOPLE.COL_PERSON_ID,
    CONFIG.PEOPLE.COL_FULL_NAME,
    CONFIG.PEOPLE.COL_ROLE,
    CONFIG.PEOPLE.COL_ONBOARDING_ID
  ];
  const missing = [];
  for (let i = 0; i < required.length; i++) {
    if (idx[required[i]] == null) missing.push(required[i]);
  }
  if (missing.length) throw new Error("Missing People_List columns: " + missing.join(", "));

  const out = {
    byOnboardingAndRole: {},
    duplicates: {},
    rows: 0
  };
  const lastRow = peopleSheet.getLastRow();
  if (lastRow < 2) return out;

  const values = peopleSheet.getRange(2, 1, lastRow - 1, peopleSheet.getLastColumn()).getValues();
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    const personId = String(row[idx[CONFIG.PEOPLE.COL_PERSON_ID]] || "").trim();
    const onboardingId = String(row[idx[CONFIG.PEOPLE.COL_ONBOARDING_ID]] || "").trim();
    const role = normalizeManualPeopleRole_(row[idx[CONFIG.PEOPLE.COL_ROLE]]);
    const fullName = String(row[idx[CONFIG.PEOPLE.COL_FULL_NAME]] || "").trim();
    if (!personId || !onboardingId || !role) continue;

    const key = buildManualPeopleRefKey_(onboardingId, role);
    const rec = {
      personId: personId,
      onboardingId: onboardingId,
      role: role,
      fullName: fullName,
      fullNameKey: normalizeManualPersonName_(fullName),
      rowNum: i + 2
    };
    if (!out.byOnboardingAndRole[key]) {
      out.byOnboardingAndRole[key] = rec;
    } else {
      if (!out.duplicates[key]) out.duplicates[key] = [out.byOnboardingAndRole[key]];
      out.duplicates[key].push(rec);
    }
    out.rows++;
  }

  log_(runId, "INFO", "MANUAL_PEOPLE_REFS_INDEX_READY", {
    peopleRows: out.rows,
    duplicateKeys: Object.keys(out.duplicates).length
  });
  return out;
}

function repairPeopleRefsRowsInSheet_(runId, dest, mapping, peopleIndex, options) {
  const out = {
    ok: true,
    scanned: 0,
    candidates: 0,
    rowsUpdated: 0,
    cellsUpdated: 0,
    alreadyCorrect: 0,
    skippedNoOnboardingId: 0,
    skippedNoPersonName: 0,
    skippedMissingPeopleRow: 0,
    skippedDuplicatePeopleRows: 0,
    skippedNameMismatch: 0,
    failed: 0,
    stoppedEarly: false
  };

  const lastRow = dest.getLastRow();
  if (lastRow < 2) return out;
  const props = PropertiesService.getScriptProperties();
  const cursorRow = Math.max(2, Number(props.getProperty(MANUAL_PEOPLE_REFS_CURSOR_KEY) || 2));
  const startIndex = Math.max(0, Math.min(lastRow - 2, cursorRow - 2));

  const idx = {
    id: mapping.dstIndex["ID"],
    contactName: mapping.dstIndex["imię i nazwisko osoby kontaktowej"],
    managerName: mapping.dstIndex["imię i nazwisko kierownika"],
    beneficialName: mapping.dstIndex["imię i nazwisko beneficjenta"],
    contactRef: mapping.dstIndex[CONFIG.MAIN_REF_COLS.CONTACT],
    managerRef: mapping.dstIndex[CONFIG.MAIN_REF_COLS.MANAGER],
    beneficialRef: mapping.dstIndex[CONFIG.MAIN_REF_COLS.BENEFICIAL]
  };
  const roles = [
    { role: "Contact", nameIdx: idx.contactName, refIdx: idx.contactRef, refCol: CONFIG.MAIN_REF_COLS.CONTACT },
    { role: "Manager", nameIdx: idx.managerName, refIdx: idx.managerRef, refCol: CONFIG.MAIN_REF_COLS.MANAGER },
    { role: "BeneficialOwner", nameIdx: idx.beneficialName, refIdx: idx.beneficialRef, refCol: CONFIG.MAIN_REF_COLS.BENEFICIAL }
  ];

  const values = dest.getRange(2, 1, lastRow - 1, dest.getLastColumn()).getValues();
  const startedAt = Number(options.startedAt || Date.now());
  const maxRuntimeMs = Math.max(10000, Number(options.maxRuntimeMs || 300000));
  const maxRows = Math.max(1, Number(options.maxRows || 250));
  let lastVisitedRow = startIndex + 1;

  for (let i = startIndex; i < values.length; i++) {
    if (out.rowsUpdated >= maxRows) break;
    if (Date.now() - startedAt > maxRuntimeMs - 10000) {
      out.stoppedEarly = true;
      break;
    }

    const rowNum = i + 2;
    lastVisitedRow = rowNum;
    const row = values[i];
    out.scanned++;

    const onboardingId = String(row[idx.id] || "").trim();
    if (!onboardingId) {
      out.skippedNoOnboardingId++;
      continue;
    }

    const updates = [];
    for (let r = 0; r < roles.length; r++) {
      const roleCfg = roles[r];
      const fullName = String(row[roleCfg.nameIdx] || "").trim();
      const currentRef = String(row[roleCfg.refIdx] || "").trim();
      if (!fullName) {
        out.skippedNoPersonName++;
        continue;
      }

      const key = buildManualPeopleRefKey_(onboardingId, roleCfg.role);
      const duplicates = peopleIndex.duplicates[key] || null;
      if (duplicates) {
        const matched = findManualPeopleRefByName_(duplicates, fullName);
        if (!matched) {
          out.skippedDuplicatePeopleRows++;
          log_(runId, "WARN", "MANUAL_PEOPLE_REFS_DUPLICATE_SKIP", {
            rowNum: rowNum,
            onboardingId: onboardingId,
            role: roleCfg.role,
            mainName: fullName,
            matches: duplicates.length
          });
          continue;
        }
        if (currentRef === matched.personId) {
          out.alreadyCorrect++;
          continue;
        }
        updates.push({ refIdx: roleCfg.refIdx, refCol: roleCfg.refCol, value: matched.personId, role: roleCfg.role, current: currentRef });
        continue;
      }

      const rec = peopleIndex.byOnboardingAndRole[key] || null;
      if (!rec) {
        out.skippedMissingPeopleRow++;
        continue;
      }
      if (rec.fullNameKey && normalizeManualPersonName_(fullName) !== rec.fullNameKey) {
        out.skippedNameMismatch++;
        log_(runId, "WARN", "MANUAL_PEOPLE_REFS_NAME_MISMATCH_SKIP", {
          rowNum: rowNum,
          onboardingId: onboardingId,
          role: roleCfg.role,
          mainName: fullName,
          peopleName: rec.fullName,
          peopleRow: rec.rowNum
        });
        continue;
      }
      if (currentRef === rec.personId) {
        out.alreadyCorrect++;
        continue;
      }
      updates.push({ refIdx: roleCfg.refIdx, refCol: roleCfg.refCol, value: rec.personId, role: roleCfg.role, current: currentRef });
    }

    if (!updates.length) continue;
    out.candidates++;

    try {
      if (!options.dryRun) {
        for (let u = 0; u < updates.length; u++) {
          dest.getRange(rowNum, updates[u].refIdx + 1).setValue(updates[u].value);
        }
        SpreadsheetApp.flush();
      }
      out.rowsUpdated++;
      out.cellsUpdated += updates.length;
      log_(runId, "INFO", "MANUAL_PEOPLE_REFS_ROW_UPDATED", {
        rowNum: rowNum,
        onboardingId: onboardingId,
        dryRun: !!options.dryRun,
        updates: updates.map(function (u) {
          return {
            role: u.role,
            col: u.refCol,
            from: u.current,
            to: u.value
          };
        })
      });
    } catch (e) {
      out.failed++;
      log_(runId, "WARN", "MANUAL_PEOPLE_REFS_ROW_UPDATE_FAILED", {
        rowNum: rowNum,
        onboardingId: onboardingId,
        err: String(e).slice(0, 900)
      });
    }
  }

  if (lastVisitedRow < lastRow && (out.stoppedEarly || out.rowsUpdated >= maxRows)) {
    props.setProperty(MANUAL_PEOPLE_REFS_CURSOR_KEY, String(lastVisitedRow + 1));
    out.nextCursorRow = lastVisitedRow + 1;
  } else {
    props.deleteProperty(MANUAL_PEOPLE_REFS_CURSOR_KEY);
    out.nextCursorRow = "";
  }

  return out;
}

function findManualPeopleRefByName_(records, fullName) {
  const nameKey = normalizeManualPersonName_(fullName);
  for (let i = 0; i < (records || []).length; i++) {
    if (records[i].fullNameKey && records[i].fullNameKey === nameKey) return records[i];
  }
  return null;
}

function buildManualPeopleRefKey_(onboardingId, role) {
  return String(onboardingId || "").trim() + "|" + normalizeManualPeopleRole_(role);
}

function normalizeManualPeopleRole_(role) {
  const raw = String(role || "").trim();
  const key = raw.toLowerCase().replace(/[\s_\-]+/g, "");
  if (key === "contact") return "Contact";
  if (key === "manager") return "Manager";
  if (key === "beneficialowner" || key === "beneficial" || key === "beneficiary") return "BeneficialOwner";
  return raw;
}

function normalizeManualPersonName_(value) {
  return String(value || "").trim().replace(/\s+/g, " ").toLowerCase();
}
