/** =========================
 *  HEADERS / MAPPING
 *  ========================= */
function enforceDestHeaders_(runId, dest) {
  const schemaLen = DEST_SCHEMA.length;

  const currentLastCol = dest.getLastColumn();
  if (currentLastCol < 1 && !CONFIG.FEATURES.DRY_RUN) {
    dest.getRange(1, 1).setValue("");
  }

  if (looksLikeQueueColumnsInsertedButHeadersOld_(dest)) {
    repairQueueColumnHeadersOnly_(runId, dest);
    enforceTextFormats_(dest);
    log_(runId, "WARN", "DEST_HEADERS_REPAIRED_QUEUE_INSERT", { totalCols: dest.getLastColumn(), schemaCols: schemaLen });
    return;
  }

  ensureDestSchemaColumnsNonDestructive_(runId, dest);

  enforceTextFormats_(dest);
  const headers = getHeaderRow_(dest);
  const missing = getMissingDestSchemaHeaders_(headers);
  if (missing.length) {
    log_(runId, "WARN", "DEST_HEADERS_MISSING_AFTER_ENFORCE", { missing: missing.join(",") });
  } else {
    log_(runId, "INFO", "DEST_HEADERS_OK", { totalCols: dest.getLastColumn(), schemaCols: schemaLen });
  }
}

function repairDestHeadersOnlyAfterQueueInsert() {
  const runId = makeRunId_("repair-headers");
  const destSS = openSpreadsheetWithLog_(runId, CONFIG.DEST_SPREADSHEET_ID, "DEST_SPREADSHEET_ID");
  const dest = getSheet_(destSS, CONFIG.DEST_SHEET_NAME, false);
  if (!dest) throw new Error("Destination sheet not found: " + CONFIG.DEST_SHEET_NAME);
  enforceDestHeaders_(runId, dest);
  return {
    ok: true,
    sheetName: CONFIG.DEST_SHEET_NAME,
    cols: dest.getLastColumn(),
    headers: getHeaderRow_(dest).slice(0, DEST_SCHEMA.length)
  };
}

function ensureDestSchemaColumnsNonDestructive_(runId, dest) {
  if (!dest || CONFIG.FEATURES.DRY_RUN) return;

  for (let i = 0; i < DEST_SCHEMA.length; i++) {
    const wanted = DEST_SCHEMA[i];
    let headers = getHeaderRow_(dest);
    if (headers.indexOf(wanted) >= 0) continue;

    const prevHeader = findPreviousExistingSchemaHeader_(headers, i);
    if (prevHeader) {
      const prevIdx = headers.indexOf(prevHeader);
      dest.insertColumnAfter(prevIdx + 1);
      dest.getRange(1, prevIdx + 2).setValue(wanted);
      log_(runId, "WARN", "DEST_HEADER_INSERTED", { header: wanted, after: prevHeader, col: prevIdx + 2 });
    } else {
      const lastCol = Math.max(1, dest.getLastColumn());
      dest.insertColumnAfter(lastCol);
      dest.getRange(1, lastCol + 1).setValue(wanted);
      log_(runId, "WARN", "DEST_HEADER_APPENDED", { header: wanted, col: lastCol + 1 });
    }
  }
}

function findPreviousExistingSchemaHeader_(headers, schemaIdx) {
  for (let i = schemaIdx - 1; i >= 0; i--) {
    const h = DEST_SCHEMA[i];
    if (headers.indexOf(h) >= 0) return h;
  }
  return "";
}

function getMissingDestSchemaHeaders_(headers) {
  const present = {};
  (headers || []).forEach((h) => {
    const key = String(h || "").trim();
    if (key) present[key] = true;
  });
  return DEST_SCHEMA.filter((h) => !present[h]);
}

function looksLikeQueueColumnsInsertedButHeadersOld_(dest) {
  try {
    const headers = getHeaderRow_(dest);
    if (headers.indexOf("Generation_Requested_By") >= 0) return false;

    const isIdx = headers.indexOf("Is_Generating_Now");
    const triggeredIdx = headers.indexOf("Generation_Triggered_By");
    const syncIdx = headers.indexOf("sync_status");
    if (isIdx < 0 || triggeredIdx !== isIdx + 1 || syncIdx !== isIdx + 2) return false;

    const lastRow = dest.getLastRow();
    if (lastRow < 2) return false;

    const sampleRows = Math.min(lastRow - 1, 25);
    const width = Math.min(dest.getLastColumn(), isIdx + 9);
    if (width < isIdx + 7) return false;

    const values = dest.getRange(2, 1, sampleRows, width).getValues();
    for (let r = 0; r < values.length; r++) {
      const row = values[r];
      const oldSyncSlot = String(row[syncIdx] || "");
      const shiftedSyncSlot = String(row[isIdx + 6] || "");
      if (!looksLikeSyncMarker_(oldSyncSlot) && looksLikeSyncMarker_(shiftedSyncSlot)) {
        return true;
      }
    }
  } catch (e) {}
  return false;
}

function repairQueueColumnHeadersOnly_(runId, dest) {
  if (!dest || CONFIG.FEATURES.DRY_RUN) return;
  const schemaLen = DEST_SCHEMA.length;
  const lastCol = dest.getLastColumn();
  backupSheetBeforeHeaderRepair_(runId, dest);
  if (lastCol < schemaLen) {
    dest.insertColumnsAfter(lastCol, schemaLen - lastCol);
  }
  dest.getRange(1, 1, 1, schemaLen).setValues([DEST_SCHEMA]);
  log_(runId, "WARN", "DEST_QUEUE_HEADER_REPAIR_APPLIED", {
    previousCols: lastCol,
    currentCols: dest.getLastColumn(),
    note: "data columns were already shifted by manual insert; headers only were realigned"
  });
}

function backupSheetBeforeHeaderRepair_(runId, sheet) {
  try {
    const ss = sheet.getParent();
    const tz = Session.getScriptTimeZone();
    const stamp = Utilities.formatDate(new Date(), tz, "yyyyMMdd_HHmmss");
    const baseName = String(sheet.getName() || "Sheet");
    const backupName = (baseName + "_backup_before_header_repair_" + stamp).slice(0, 99);
    const copy = sheet.copyTo(ss).setName(backupName);
    try { copy.hideSheet(); } catch (eHide) {}
    log_(runId, "WARN", "DEST_BACKUP_CREATED_BEFORE_HEADER_REPAIR", { sheetName: backupName });
  } catch (e) {
    log_(runId, "WARN", "DEST_BACKUP_BEFORE_HEADER_REPAIR_FAILED", { err: String(e).slice(0, 700) });
  }
}

function looksLikeSyncMarker_(value) {
  const s = String(value || "");
  if (!s) return false;
  return (
    s.indexOf("APPSHEET_") >= 0 ||
    s.indexOf("PEOPLE_REFS_") >= 0 ||
    s.indexOf("MF_") >= 0 ||
    s.indexOf("IMPORTED ") >= 0 ||
    s.indexOf("IN_DEST ") >= 0
  );
}
function enforceTextFormats_(dest) {
  try {
    const colsText = ["krs", "regon"];
    for (let i = 0; i < colsText.length; i++) {
      const name = colsText[i];
      const idx = DEST_SCHEMA.indexOf(name);
      if (idx >= 0) {
        dest.getRange(1, idx + 1, dest.getMaxRows(), 1).setNumberFormat("@");
      }
    }
  } catch (e) {}
}

function buildMapping_(runId, source, dest) {
  const sourceHeaders = getHeaderRow_(source);
  const destHeaders = getHeaderRow_(dest);

  const srcIndex = indexByNormalized_(sourceHeaders);
  const dstIndex = indexByExact_(destHeaders);

  const nipIdx = srcIndex["nip"];
  const dateIdx = srcIndex["submitted on"];

  if (nipIdx == null || dateIdx == null) {
    log_(runId, "ERROR", "IMPORT_SOURCE_KEY_COLS_MISSING", {
      nipIdx, dateIdx,
      srcHeadersPreview: sourceHeaders.slice(0, 80),
    });
    throw new Error("SOURCE missing required headers: nip / submitted on");
  }

  const destKey = {
    idIdx: dstIndex["ID"],
    statusIdx: dstIndex["Status"],
    nipControlIdx: dstIndex["NIP_Control"],
    submittedIdx: dstIndex["submitted on"],
    nipIdx: dstIndex["nip"],
    syncStatusIdx: dstIndex[CONFIG.DEST_SYNC_STATUS_COL],

    // NEW ref cols
    contactPersonIdx: dstIndex[CONFIG.MAIN_REF_COLS.CONTACT],
    managerPersonIdx: dstIndex[CONFIG.MAIN_REF_COLS.MANAGER],
    beneficialPersonIdx: dstIndex[CONFIG.MAIN_REF_COLS.BENEFICIAL],
  };

  log_(runId, "INFO", "MAPPING_READY", {
    sourceCols: sourceHeaders.length,
    destCols: destHeaders.length,
    sourceHasMarkCol: srcIndex[normalizeKey_(CONFIG.SOURCE_MARK_COLUMN_NAME)] != null,
    destHasSyncStatus: destKey.syncStatusIdx != null,
    hasRefCols: {
      ContactPersonID: destKey.contactPersonIdx != null,
      ManagerPersonID: destKey.managerPersonIdx != null,
      BeneficialOwnerPersonID: destKey.beneficialPersonIdx != null,
    }
  });

  return {
    sourceHeaders,
    destHeaders,
    srcIndex,
    dstIndex,
    sourceKey: { nipIdx, dateIdx },
    destKey
  };
}

/**
 * Upewnia się, że w arkuszu źródłowym istnieje kolumna CONFIG.SOURCE_MARK_COLUMN_NAME.
 * Jeśli jej nie ma (częsty przypadek w Form Responses), dodaje ją na końcu i aktualizuje mapping.
 */
function ensureSourceMarkColumn_(runId, source, mapping) {
  const key = normalizeKey_(CONFIG.SOURCE_MARK_COLUMN_NAME);
  if (mapping.srcIndex[key] != null) return; // already present

  const lastCol = source.getLastColumn();
  const headerRow = source.getRange(1, 1, 1, lastCol).getValues()[0];

  // Jeśli ostatnia komórka nagłówka jest pusta, a arkusz ma "dziurę" na końcu, spróbuj użyć tej kolumny.
  // W przeciwnym razie dołóż nową kolumnę.
  let targetCol = null;
  if (String(headerRow[lastCol - 1] || "").trim() === "") {
    targetCol = lastCol;
  } else {
    source.insertColumnAfter(lastCol);
    targetCol = lastCol + 1;
  }

  source.getRange(1, targetCol).setValue(CONFIG.SOURCE_MARK_COLUMN_NAME);

  // Update mapping in-place
  mapping.sourceHeaders[targetCol - 1] = CONFIG.SOURCE_MARK_COLUMN_NAME;
  mapping.srcIndex[key] = targetCol - 1;

  log_(runId, "INFO", "SOURCE_MARK_COL_CREATED", { col: targetCol, header: CONFIG.SOURCE_MARK_COLUMN_NAME });
}
