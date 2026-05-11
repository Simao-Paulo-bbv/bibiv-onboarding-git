/** =========================
 *  MANUAL MAINTENANCE HELPERS
 *  =========================
 *
 * These functions are intended to be run manually from the Apps Script editor.
 */

const MANUAL_IBAN_REFRESH_CURSOR_KEY = "MANUAL_IBAN_REFRESH_CURSOR_ROW";

function runManualRefreshIbanBankMetadata() {
  return refreshIbanBankMetadataRows_({
    forceAllRows: false,
    updateAppSheet: false,
    dryRun: false,
    maxRows: 250
  });
}

function runManualRefreshAllIbanBankMetadata() {
  return refreshIbanBankMetadataRows_({
    forceAllRows: true,
    updateAppSheet: false,
    dryRun: false,
    maxRows: 250
  });
}

function resetManualIbanRefreshCursor() {
  PropertiesService.getScriptProperties().deleteProperty(MANUAL_IBAN_REFRESH_CURSOR_KEY);
  return { ok: true, cursorReset: true };
}

function runManualRefreshIbanBankMetadataAndAppSheet() {
  return refreshIbanBankMetadataRows_({
    forceAllRows: false,
    updateAppSheet: true,
    dryRun: false,
    maxRows: 100
  });
}

function runManualAuditIbanBankMetadata() {
  return refreshIbanBankMetadataRows_({
    forceAllRows: false,
    updateAppSheet: false,
    dryRun: true
  });
}

function refreshIbanBankMetadataRows_(options) {
  options = options || {};
  const runId = makeRunId_("manual-iban");
  const startedAt = Date.now();
  const maxRuntimeMs = Number(options.maxRuntimeMs || 300000);
  const maxRows = Number(options.maxRows || 100);
  const forceAllRows = !!options.forceAllRows;
  const updateAppSheet = options.updateAppSheet !== false;
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
      updateAppSheet: updateAppSheet,
      dryRun: dryRun
    });

    log_(runId, "INFO", "MANUAL_IBAN_REFRESH_END", Object.assign({}, result, {
      forceAllRows: forceAllRows,
      updateAppSheet: updateAppSheet,
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
    appSheetRowsUpdated: 0,
    appSheetRowsFailed: 0,
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
    legacyBic: mapping.dstIndex["kod swift banku"],
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
      bic: String(row[idx.bic] || row[idx.legacyBic] || "").trim(),
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

    try {
      if (!options.dryRun) {
        writeManualIbanMetaToSheet_(dest, idx, rowNum, meta);
        out.sheetRowsUpdated++;
        if (options.updateAppSheet) {
          if (updateManualIbanMetaInAppSheet_(runId, rowNum, onboardingId, meta)) {
            out.appSheetRowsUpdated++;
          } else {
            out.appSheetRowsFailed++;
          }
        }
      }
      if (options.dryRun) out.sheetRowsUpdated++;
      log_(runId, "INFO", "MANUAL_IBAN_ROW_UPDATED", {
        rowNum: rowNum,
        onboardingId: onboardingId,
        accountLast4: account.slice(-4),
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
    return pickBankMetaFromIban_(res.parsed);
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

  if (bic && idx.legacyBic != null) dest.getRange(rowNum, idx.legacyBic + 1).setValue(bic);
  if (bic) dest.getRange(rowNum, idx.bic + 1).setValue(bic);
  if (bankName) dest.getRange(rowNum, idx.bankName + 1).setValue(bankName);
  if (address) dest.getRange(rowNum, idx.bankAddress + 1).setValue(address);
  if (city) dest.getRange(rowNum, idx.bankCity + 1).setValue(city);
  SpreadsheetApp.flush();
}

function updateManualIbanMetaInAppSheet_(runId, rowNum, onboardingId, meta) {
  const payloadWithLegacy = {
    ID: String(onboardingId || "").trim(),
    "kod swift banku": String(meta.bic || "").trim(),
    "swift/bic": String(meta.bic || "").trim(),
    "Bank name": String(meta.bankName || "").trim(),
    "Bank address": String(meta.address || "").trim(),
    "Bank city": String(meta.city || "").trim()
  };
  try {
    callAppSheet_(runId, CONFIG.APPSHEET_TABLE_MAIN, payloadWithLegacy, CONFIG.APPSHEET_ACTION_EDIT, rowNum);
    return true;
  } catch (firstErr) {
    log_(runId, "WARN", "MANUAL_IBAN_APPSHEET_RETRY_NO_LEGACY_BIC", {
      rowNum: rowNum,
      onboardingId: onboardingId,
      err: String(firstErr).slice(0, 900)
    });
  }

  const payload = {
    ID: String(onboardingId || "").trim(),
    "swift/bic": String(meta.bic || "").trim(),
    "Bank name": String(meta.bankName || "").trim(),
    "Bank address": String(meta.address || "").trim(),
    "Bank city": String(meta.city || "").trim()
  };
  try {
    callAppSheet_(runId, CONFIG.APPSHEET_TABLE_MAIN, payload, CONFIG.APPSHEET_ACTION_EDIT, rowNum);
    return true;
  } catch (secondErr) {
    log_(runId, "WARN", "MANUAL_IBAN_APPSHEET_UPDATE_FAILED", {
      rowNum: rowNum,
      onboardingId: onboardingId,
      err: String(secondErr).slice(0, 900)
    });
    return false;
  }
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
