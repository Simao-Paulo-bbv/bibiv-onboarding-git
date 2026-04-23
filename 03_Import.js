/** =========================
 *  IMPORT (SOURCE -> DEST)
 *  ========================= */
function importFromSource_(runId, mapping, source, dest, startedAt) {
  const sourceLastRow = source.getLastRow();
  if (sourceLastRow < 2) {
    log_(runId, "INFO", "IMPORT_NONE_DIAG", { reason: "SOURCE_EMPTY", sourceLastRow, checkedRows: 0 });
    return { imported: 0, destStartRow: 0, destEndRow: 0 };
  }

  const dedupe = buildDestDedupeIndex_(runId, mapping, dest);
  const srcCols = mapping.sourceHeaders.length;
  const srcValues = source.getRange(2, 1, sourceLastRow - 1, srcCols).getValues();
  const historyStore = (CONFIG.FORCE_IMPORT === true)
    ? null
    : getImportHistoryStore_(runId, dest.getParent());

  const markIdx = mapping.srcIndex[normalizeKey_(CONFIG.SOURCE_MARK_COLUMN_NAME)];

  log_(runId, "INFO", "SOURCE_MARK_COL", {
    SOURCE_MARK_COLUMN_NAME: CONFIG.SOURCE_MARK_COLUMN_NAME,
    markIdx,
    sourceHeadersPreview: mapping.sourceHeaders.slice(0, 40),
    sourceLastRow,
    srcCols
  });

  const candidates = [];
  const reasons = {
    noNip: 0,
    alreadyMarkedImported: 0,
    markedButMissingInDest: 0,
    deduped: 0,
    historySeen: 0,
    accepted: 0,
    runtimeCut: 0,
    limitCut: 0
  };
  const forceMode = CONFIG.FORCE_IMPORT === true;

  // Dodatkowe zabezpieczenie: jeżeli w SOURCE są duplikaty (np. ten sam nip+submitted on),
  // nie importuj ich podwójnie w jednym przebiegu.
  const seenKeysInRun = {};

  for (let i = 0; i < srcValues.length; i++) {
    if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 2000) { reasons.runtimeCut++; break; }
    if (!forceMode && candidates.length >= CONFIG.MAX_ROWS_PER_RUN) { reasons.limitCut++; break; }
    if (forceMode && candidates.length >= CONFIG.FORCE_IMPORT_LIMIT) { reasons.limitCut++; break; }

    const rowNum = i + 2;
    const row = srcValues[i];

    const nip = String(row[mapping.sourceKey.nipIdx] || "").trim();
    const sub = row[mapping.sourceKey.dateIdx];

    if (!nip) { reasons.noNip++; continue; }

    // Klucz deduplikacji (DEST / wiersze usunięte / odtworzenie braków)
    const key = makeDedupeKey_(nip, sub);

    if (!forceMode) {
      if (dedupe[key]) { reasons.deduped++; continue; }
      // History is advisory only. Real blocker is presence in DEST (dedupe above)
      // or explicit SOURCE marker state below.
      if (historyStore && historyStore.index[key]) { reasons.historySeen++; }

      // Safety rule:
      // marked SOURCE rows (IN_DEST/DONE) are archived in durable key-history and skipped.
      if (markIdx != null && markIdx >= 0) {
        const markVal = String(row[markIdx] || "").trim();
        const markState = getSourceMarkState_(markVal);
        if (markState) {
          const allowReimportMissing = !!(CONFIG && CONFIG.SOURCE_REIMPORT_IF_MISSING_IN_DEST);
          if (!allowReimportMissing) {
            reasons.alreadyMarkedImported++;
            if (historyStore) {
              queueImportHistoryEntry_(historyStore, key, nip, sub, rowNum, "SOURCE_MARK_" + markState);
            }
            continue;
          }
          reasons.markedButMissingInDest++;
        }
      }

      if (seenKeysInRun[key]) { reasons.duplicateInRun = (reasons.duplicateInRun || 0) + 1; continue; }
      seenKeysInRun[key] = true;
    }

    candidates.push({ sourceRow: rowNum, nip, submittedOn: sub, values: row, key });
    reasons.accepted++;
  }


  if (candidates.length === 0) {
    if (historyStore) flushImportHistoryStore_(runId, historyStore);
    log_(runId, "INFO", "IMPORT_NONE_DIAG", {
      reason: "NO_CANDIDATES",
      sourceLastRow,
      checkedRows: srcValues.length,
      markIdx,
      nipIdx: mapping.sourceKey.nipIdx,
      submittedIdx: mapping.sourceKey.dateIdx,
      reasons,
      forceMode,
    });
    log_(runId, "INFO", "IMPORT_NONE");
    return { imported: 0, destStartRow: 0, destEndRow: 0 };
  }

  const destStartRow = Math.max(2, dest.getLastRow() + 1);
  const destEndRow = destStartRow + candidates.length - 1;

  const out = [];
  const preferredRpkIdx = findSourceHeaderRawIndex_(mapping.sourceHeaders, "numer rpk");
  const legacyRpkIdx = findSourceHeaderRawIndex_(mapping.sourceHeaders, "numer rpk w knf");

  for (let i = 0; i < candidates.length; i++) {
    const srcRow = candidates[i].values;
    const destRow = new Array(DEST_SCHEMA.length).fill("");

    for (const k in SYSTEM_DEFAULTS) {
      const idx = mapping.dstIndex[k];
      if (idx != null && idx < DEST_SCHEMA.length) destRow[idx] = SYSTEM_DEFAULTS[k];
    }

    for (let s = 0; s < mapping.sourceHeaders.length; s++) {
      const srcHeaderNorm = normalizeKey_(mapping.sourceHeaders[s]);
      if (!srcHeaderNorm) continue;

      const destHeader = findDestHeaderByNormalized_(srcHeaderNorm);
      if (!destHeader) continue;

      const dIdx = mapping.dstIndex[destHeader];
      if (dIdx == null || dIdx >= DEST_SCHEMA.length) continue;

      // Priority rule for renamed RPK field:
      // when both columns exist, prefer "numer rpk" over legacy "numer rpk w knf".
      if (
        destHeader === "numer wpisu do knf" &&
        preferredRpkIdx != null &&
        legacyRpkIdx != null &&
        s === legacyRpkIdx &&
        !isBlankImportedValue_(srcRow[preferredRpkIdx])
      ) {
        continue;
      }

      // === FIX: opcjonalna data z Squarespace może przyjść jako "//" ===
      // Kolumna "data urodzenia 6" jest nieobowiązkowa; AppSheet wywala błąd na "//".
      let v = srcRow[s];
      if (destHeader === "data urodzenia 6" && typeof v === "string" && v.trim() === "//") {
        v = "";
      }
      // Source can send placeholder phone "(null) null-null" - keep it as blank.
      if (destHeader === "numer telefonu osoby kontaktowej" && typeof v === "string") {
        const phoneRaw = String(v || "").trim().toLowerCase();
        if (phoneRaw === "(null) null-null") {
          v = "";
        }
      }
      // ===============================================================

      if (shouldOverwriteImportedValue_(destRow[dIdx], v)) {
        destRow[dIdx] = v;
      }
    }

    const nipSchemaIdx = mapping.dstIndex["nip"];
    const nipControlIdx = mapping.dstIndex["NIP_Control"];
    if (nipControlIdx != null && nipSchemaIdx != null) {
      const nipVal = String(destRow[nipSchemaIdx] || "").trim();
      if (nipVal) destRow[nipControlIdx] = nipVal;
    }

    const dsIdx = mapping.destKey.syncStatusIdx;
    if (dsIdx != null) destRow[dsIdx] = `IMPORTED ${formatNow_()}`;

    out.push(destRow);
  }

  if (!CONFIG.FEATURES.DRY_RUN) {
    dest.getRange(destStartRow, 1, out.length, DEST_SCHEMA.length).setValues(out);
  }

  if (!CONFIG.FEATURES.DRY_RUN && !forceMode && markIdx != null && markIdx >= 0) {
    for (let i = 0; i < candidates.length; i++) {
      const sourceRowNum = candidates[i].sourceRow;
      const mark = `${CONFIG.SOURCE_MARK_PREFIX_IMPORT} ${formatNow_()} -> DEST_ROW ${destStartRow + i}`;
      source.getRange(sourceRowNum, markIdx + 1).setValue(mark);
    }
  }

  if (!forceMode && historyStore) {
    for (let i = 0; i < candidates.length; i++) {
      const c = candidates[i];
      queueImportHistoryEntry_(historyStore, c.key, c.nip, c.submittedOn, c.sourceRow, "IMPORTED");
    }
    flushImportHistoryStore_(runId, historyStore);
  }

  const rowMap = candidates.map((c, i) => ({ srcRow: c.sourceRow, destRow: destStartRow + i }));

  log_(runId, "INFO", "IMPORT_OK", { imported: candidates.length, destStartRow, destEndRow, forceMode, reasons });
  return { imported: candidates.length, destStartRow, destEndRow, rowMap };
}

function buildDestDedupeIndex_(runId, mapping, dest) {
  const lastRow = dest.getLastRow();
  const idx = {};

  if (lastRow < 2) {
    log_(runId, "WARN", "DEST_DEDUPE_INDEX_WEAK", { lastRow, note: "DEST_EMPTY_OK" });
    return idx;
  }

  const nipIdx = mapping.destKey.nipControlIdx != null ? mapping.destKey.nipControlIdx : mapping.destKey.nipIdx;
  const subIdx = mapping.destKey.submittedIdx;

  if (nipIdx == null || subIdx == null) {
    log_(runId, "WARN", "DEST_DEDUPE_INDEX_WEAK", { lastRow, note: "MISSING_KEY_COLS_IN_DEST" });
    return idx;
  }

  const data = dest.getRange(2, 1, lastRow - 1, DEST_SCHEMA.length).getValues();
  for (let i = 0; i < data.length; i++) {
    const nip = String(data[i][nipIdx] || "").trim();
    const sub = data[i][subIdx];
    if (!nip) continue;
    idx[makeDedupeKey_(nip, sub)] = true;
  }

  log_(runId, "INFO", "DEST_DEDUPE_INDEX_READY", { keys: Object.keys(idx).length });
  return idx;
}

function findDestHeaderByNormalized_(normKey) {
  const nk = String(normKey || "").trim().toLowerCase();
  if (!nk) return null;
  for (let i = 0; i < DEST_SCHEMA.length; i++) {
    if (normalizeKey_(DEST_SCHEMA[i]) === nk) return DEST_SCHEMA[i];
  }
  return null;
}

function shouldOverwriteImportedValue_(currentValue, nextValue) {
  if (isBlankImportedValue_(currentValue)) return true;
  return !isBlankImportedValue_(nextValue);
}

function isBlankImportedValue_(value) {
  if (value === null || value === undefined) return true;
  if (typeof value === "string") return value.trim() === "";
  return false;
}

function findSourceHeaderRawIndex_(headers, rawName) {
  const target = String(rawName || "").trim().toLowerCase();
  if (!target) return null;
  for (let i = 0; i < (headers || []).length; i++) {
    const raw = String(headers[i] || "").trim().toLowerCase();
    if (raw === target) return i;
  }
  return null;
}

function getImportHistoryStore_(runId, ss) {
  try {
    const sheetName = String((CONFIG && CONFIG.IMPORT_HISTORY_SHEET_NAME) || "_Import_History");
    const headers = ["DedupeKey", "NIP", "SubmittedOnKey", "SourceRow", "Reason", "MarkedAt"];

    let sh = ss.getSheetByName(sheetName);
    if (!sh && !CONFIG.FEATURES.DRY_RUN) {
      sh = ss.insertSheet(sheetName);
      try { sh.hideSheet(); } catch (eHide) {}
    }
    if (!sh) {
      return { sheet: null, index: {}, pending: [] };
    }

    // Ensure sheet has enough physical columns before write.
    let maxCols = 0;
    try {
      maxCols = Number(sh.getMaxColumns() || 0);
    } catch (eMax) {
      maxCols = Number(sh.getLastColumn() || 0);
    }
    if (!Number.isFinite(maxCols)) maxCols = 0;

    if (maxCols < headers.length && !CONFIG.FEATURES.DRY_RUN) {
      const toAdd = headers.length - maxCols;
      if (maxCols <= 0) {
        // Defensive fallback for corrupted/empty sheet grid.
        sh.insertColumns(1, headers.length);
      } else {
        sh.insertColumnsAfter(maxCols, toAdd);
      }
      maxCols = headers.length;
    }

    const lastCol = Number(sh.getLastColumn() || 0);
    const currentHeaders =
      (lastCol >= headers.length)
        ? sh.getRange(1, 1, 1, headers.length).getValues()[0].map(v => String(v || "").trim())
        : [];
    const needHeaderWrite = currentHeaders.length !== headers.length || !arraysEqual_(currentHeaders, headers);
    if (needHeaderWrite && !CONFIG.FEATURES.DRY_RUN) {
      sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    }

    const idx = {};
    const lastRow = sh.getLastRow();
    if (lastRow >= 2) {
      const keys = sh.getRange(2, 1, lastRow - 1, 1).getValues();
      for (let i = 0; i < keys.length; i++) {
        const key = String(keys[i][0] || "").trim();
        if (!key) continue;
        idx[key] = true;
      }
    }

    log_(runId, "INFO", "IMPORT_HISTORY_READY", { sheetName: sheetName, keys: Object.keys(idx).length });
    return { sheet: sh, index: idx, pending: [] };
  } catch (e) {
    // Import history is a safety layer; it must never crash whole sync run.
    log_(runId, "WARN", "IMPORT_HISTORY_DISABLED", { err: String(e).slice(0, 700) });
    return { sheet: null, index: {}, pending: [] };
  }
}

function queueImportHistoryEntry_(store, key, nip, submittedOn, sourceRow, reason) {
  if (!store || !store.sheet) return false;
  const k = String(key || "").trim();
  if (!k || store.index[k]) return false;

  const row = [
    k,
    String(nip || "").trim(),
    normalizeSubmittedOnKey_(submittedOn),
    Number(sourceRow || 0) || "",
    String(reason || "").trim(),
    formatNow_()
  ];

  store.index[k] = true;
  store.pending.push(row);
  return true;
}

function flushImportHistoryStore_(runId, store) {
  if (!store || !store.sheet || !store.pending || store.pending.length === 0) return;
  if (CONFIG.FEATURES.DRY_RUN) {
    store.pending = [];
    return;
  }

  try {
    const startRow = Math.max(2, store.sheet.getLastRow() + 1);
    const rows = store.pending.slice();
    store.sheet.getRange(startRow, 1, rows.length, rows[0].length).setValues(rows);
    store.pending = [];

    log_(runId, "INFO", "IMPORT_HISTORY_APPEND", {
      added: rows.length,
      totalKeys: Object.keys(store.index).length
    });
  } catch (e) {
    log_(runId, "WARN", "IMPORT_HISTORY_APPEND_FAIL", { err: String(e).slice(0, 700) });
    store.pending = [];
  }
}

function getSourceMarkState_(markVal) {
  const s = String(markVal || "").trim();
  if (!s) return "";
  if (s.indexOf(CONFIG.SOURCE_MARK_PREFIX_DONE) === 0) return "DONE";
  if (s.indexOf(CONFIG.SOURCE_MARK_PREFIX_IMPORT) === 0) return "IN_DEST";
  return "";
}

function parseMarkedDestRow_(markVal) {
  const s = String(markVal || "").trim();
  if (!s) return null;
  const m = s.match(/DEST_ROW\s+(\d+)/i);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) && n >= 2 ? n : null;
}

function destRowMatchesSource_(dest, mapping, rowNum, sourceRowValues) {
  if (!dest || !mapping || !sourceRowValues) return false;
  if (!rowNum || rowNum < 2 || rowNum > dest.getLastRow()) return false;

  const nipIdx = mapping.destKey.nipControlIdx != null ? mapping.destKey.nipControlIdx : mapping.destKey.nipIdx;
  const subIdx = mapping.destKey.submittedIdx;
  const srcNipIdx = mapping.sourceKey.nipIdx;
  const srcSubIdx = mapping.sourceKey.dateIdx;
  const srcNameIdx = mapping.srcIndex[normalizeKey_("Nazwa firmy")];
  const dstNameIdx = mapping.dstIndex["nazwa firmy"];

  if (nipIdx == null || subIdx == null || srcNipIdx == null || srcSubIdx == null) return false;

  const destRow = dest.getRange(rowNum, 1, 1, DEST_SCHEMA.length).getValues()[0];
  const destNip = String(destRow[nipIdx] || "").trim();
  const destSub = destRow[subIdx];
  if (!destNip) return false;

  const srcNip = String(sourceRowValues[srcNipIdx] || "").trim();
  const srcSub = sourceRowValues[srcSubIdx];
  const sameKey = makeDedupeKey_(destNip, destSub) === makeDedupeKey_(srcNip, srcSub);
  if (!sameKey) return false;

  // Strengthen check with company name to avoid false match when key collides.
  if (srcNameIdx != null && dstNameIdx != null) {
    const srcName = String(sourceRowValues[srcNameIdx] || "").trim().toLowerCase();
    const dstName = String(destRow[dstNameIdx] || "").trim().toLowerCase();
    if (srcName !== dstName) return false;
  }

  return true;
}
