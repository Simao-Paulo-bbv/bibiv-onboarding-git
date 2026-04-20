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

  // Użytkownik często czyści arkusz DEST "do zera" (łącznie z nagłówkami).
  // enforceDestHeaders_ odtwarza nagłówki w wierszu 1, więc "pusty" DEST oznacza: brak danych poniżej nagłówków.
  const destIsEmpty = dest.getLastRow() < 2;

  const markIdx = mapping.srcIndex[normalizeKey_(CONFIG.SOURCE_MARK_COLUMN_NAME)];

  log_(runId, "INFO", "SOURCE_MARK_COL", {
    SOURCE_MARK_COLUMN_NAME: CONFIG.SOURCE_MARK_COLUMN_NAME,
    markIdx,
    sourceHeadersPreview: mapping.sourceHeaders.slice(0, 40),
    sourceLastRow,
    srcCols
  });

  const candidates = [];
  const reasons = { noNip: 0, alreadyMarkedImported: 0, deduped: 0, accepted: 0, runtimeCut: 0, limitCut: 0 };
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
      let allowRestoreImport = false;

      // 1) Jeśli SOURCE ma marker IN_DEST/DONE z DEST_ROW, ale ten wiersz w DEST już nie istnieje
      //    albo nie odpowiada temu samemu kluczowi, pozwól na odtworzenie importu.
      if (markIdx != null && markIdx >= 0 && !destIsEmpty) {
        const markVal = String(row[markIdx] || "").trim();
        if (markVal) {
          const markedDestRow = parseMarkedDestRow_(markVal);
          if (markedDestRow != null && !destRowMatchesKey_(dest, mapping, markedDestRow, key)) {
            allowRestoreImport = true;
            reasons.markedButMissingInDest = (reasons.markedButMissingInDest || 0) + 1;
          }
        }
      }

      // 2) Standardowa deduplikacja tylko gdy NIE odtwarzamy brakującego rekordu.
      if (!allowRestoreImport) {
        if (dedupe[key]) { reasons.deduped++; continue; }
        if (seenKeysInRun[key]) { reasons.duplicateInRun = (reasons.duplicateInRun || 0) + 1; continue; }
        seenKeysInRun[key] = true;
      }
    }

    candidates.push({ sourceRow: rowNum, nip, submittedOn: sub, values: row });
    reasons.accepted++;
  }


  if (candidates.length === 0) {
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

function parseMarkedDestRow_(markVal) {
  const s = String(markVal || "").trim();
  if (!s) return null;
  const m = s.match(/DEST_ROW\s+(\d+)/i);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) && n >= 2 ? n : null;
}

function destRowMatchesKey_(dest, mapping, rowNum, expectedKey) {
  if (!dest || !mapping || !expectedKey) return false;
  if (!rowNum || rowNum < 2 || rowNum > dest.getLastRow()) return false;

  const nipIdx = mapping.destKey.nipControlIdx != null ? mapping.destKey.nipControlIdx : mapping.destKey.nipIdx;
  const subIdx = mapping.destKey.submittedIdx;
  if (nipIdx == null || subIdx == null) return false;

  const row = dest.getRange(rowNum, 1, 1, DEST_SCHEMA.length).getValues()[0];
  const nip = String(row[nipIdx] || "").trim();
  const sub = row[subIdx];
  if (!nip) return false;

  return makeDedupeKey_(nip, sub) === expectedKey;
}
