/** =========================
 *  HEADERS / MAPPING
 *  ========================= */
function enforceDestHeaders_(runId, dest) {
  const schemaLen = DEST_SCHEMA.length;

  const currentLastCol = dest.getLastColumn();
  if (currentLastCol < 1 && !CONFIG.FEATURES.DRY_RUN) {
    dest.getRange(1, 1).setValue("");
  }

  const afterInitLastCol = Math.max(1, dest.getLastColumn());
  if (afterInitLastCol < schemaLen && !CONFIG.FEATURES.DRY_RUN) {
    dest.insertColumnsAfter(afterInitLastCol, schemaLen - afterInitLastCol);
  }

  const current = dest.getRange(1, 1, 1, schemaLen).getValues()[0].map(v => String(v || "").trim());
  const same = arraysEqual_(current, DEST_SCHEMA);

  if (same) {
    enforceTextFormats_(dest);
    log_(runId, "INFO", "DEST_HEADERS_OK", { totalCols: dest.getLastColumn(), schemaCols: schemaLen });
    return;
  }

  if (!CONFIG.FEATURES.DRY_RUN) {
    dest.getRange(1, 1, 1, schemaLen).setValues([DEST_SCHEMA]);
  }

  enforceTextFormats_(dest);
  log_(runId, "INFO", "DEST_HEADERS_ENFORCED", { schemaCols: schemaLen, lastCol: dest.getLastColumn() });
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
