/** =========================
 *  ENTRYPOINT
 *  ========================= */
function runSyncAndProcess() {
  const runId = makeRunId_();
  const startedAt = Date.now();

  const lock = LockService.getScriptLock();
  const gotLock = lock.tryLock(CONFIG.LOCK_TIMEOUT_MS);
  if (!gotLock) {
    log_(runId, "WARN", "LOCK_NOT_ACQUIRED");
    return;
  }

  try {
    log_(runId, "INFO", "START");

    const sourceSS = SpreadsheetApp.openById(CONFIG.SOURCE_SPREADSHEET_ID);
    const destSS = SpreadsheetApp.openById(CONFIG.DEST_SPREADSHEET_ID);

    const source = getSheet_(sourceSS, CONFIG.SOURCE_SHEET_NAME, false);
    const dest = getSheet_(destSS, CONFIG.DEST_SHEET_NAME, false);

    if (!source || !dest) {
      log_(runId, "ERROR", "SHEET_MISSING", { sourceOk: !!source, destOk: !!dest });
      return;
    }

    if (CONFIG.LOGGING.TO_SHEET && CONFIG.FEATURES.AUTO_CREATE_LOG_SHEET) {
      const logSS = SpreadsheetApp.openById(CONFIG.LOG_SPREADSHEET_ID);
      ensureLogSheet_(logSS, CONFIG.LOG_SHEET_NAME);
    }

    if (CONFIG.FEATURES.ENFORCE_DEST_HEADERS) {
      enforceDestHeaders_(runId, dest);
    }

    const mapping = buildMapping_(runId, source, dest);

    // Kluczowe: jeśli w arkuszu źródłowym nie ma kolumny "sync_status",
    // to import będzie się wykonywał w kółko (bo nie ma jak oznaczyć wierszy jako zaimportowane).
    // Ten arkusz (Form Responses) zwykle można bezpiecznie rozszerzyć o dodatkową kolumnę.
    ensureSourceMarkColumn_(runId, source, mapping);

    // markIdx jest ustawiany przez ensureSourceMarkColumn_ (kolumna w SOURCE) i przechowywany w mapping.srcIndex
    // (0-based index w tablicy wartości). Jest niezbędny do oznaczania wierszy w arkuszu źródłowym.
    const markIdx = (mapping && mapping.srcIndex) ? mapping.srcIndex[normalizeKey_(CONFIG.SOURCE_MARK_COLUMN_NAME)] : -1;
    // utrzymaj też w sourceKey (dla kompatybilności z innymi helperami)
    if (mapping && mapping.sourceKey) mapping.sourceKey.markIdx = markIdx;

    const importRes = importFromSource_(runId, mapping, source, dest, startedAt);

    let processed = 0;

    // Always try to process DEST rows that are still "pending" (e.g. runtime cut on previous run).
    // If we imported rows in this run, process only that range; otherwise process the whole DEST (data) range.
    const destLastRow = dest.getLastRow();
    const hasDestData = destLastRow >= 2;

    if (importRes.imported > 0) {
      processed = processDestRows_(
        runId,
        mapping,
        source,
        dest,
        importRes.destStartRow,
        importRes.destEndRow,
        startedAt,
        importRes.rowMap || [],
        markIdx
      );
    } else if (hasDestData) {
      log_(runId, "INFO", "NO_NEW_IMPORTS_PROCESS_PENDING", { destLastRow });
      processed = processDestRows_(
        runId,
        mapping,
        source,
        dest,
        2,
        destLastRow,
        startedAt,
        [], // no new rowMap
        markIdx
      );
    } else {
      log_(runId, "INFO", "NO_NEW_ROWS_TO_PROCESS");
    }

log_(runId, "INFO", "END", {
      imported: importRes.imported,
      processedLogic: processed,
      elapsedMs: Date.now() - startedAt,
      forceImport: CONFIG.FORCE_IMPORT ? true : false
    });

  } catch (e) {
    log_(runId, "ERROR", "FATAL", { message: String(e), stack: e && e.stack ? String(e.stack) : "" });
    throw e;
  } finally {
    lock.releaseLock();
  }
}

function processPendingDestRows_(runId, mapping, source, dest, startedAt) {
  const lastRow = dest.getLastRow();
  if (lastRow < 2) return 0;

  const syncIdx = mapping.destKey.syncStatusIdx;
  const idIdx = mapping.destKey.idIdx;
  const nipIdx = mapping.destKey.nipControlIdx != null ? mapping.destKey.nipControlIdx : mapping.destKey.nipIdx;

  // ref cols (mogą istnieć)
  const cIdx = mapping.destKey.contactPersonIdx;
  const mIdx = mapping.destKey.managerPersonIdx;
  const bIdx = mapping.destKey.beneficialPersonIdx;

  if (syncIdx == null || idIdx == null || nipIdx == null) {
    log_(runId, "WARN", "PENDING_SKIP_MISSING_INDEXES", { syncIdx, idIdx, nipIdx });
    return 0;
  }

  // Czytamy tylko kluczowe kolumny, żeby było szybko:
  // ID, NIP_Control, sync_status, + 3 refy (jeśli są)
  const colsToFetch = [];
  const colMap = {}; // logical -> position in fetched row
  function addCol(idx, key) {
    if (idx == null) return;
    colsToFetch.push(idx);
    colMap[key] = colsToFetch.length - 1;
  }

  addCol(idIdx, "ID");
  addCol(nipIdx, "NIP");
  addCol(syncIdx, "SYNC");
  addCol(cIdx, "CREF");
  addCol(mIdx, "MREF");
  addCol(bIdx, "BREF");

  // zbuduj zakres minimalny: od minIdx do maxIdx i potem wyciągaj wartości po offsetach
  const minIdx = Math.min.apply(null, colsToFetch) + 1;
  const maxIdx = Math.max.apply(null, colsToFetch) + 1;
  // For very large DEST sheets, scanning every row each run becomes expensive.
// We default to scanning only the last N rows (configurable).
const scanLastN = CONFIG.PENDING_SCAN_LAST_N || 0;
const scanRowStart = (scanLastN && lastRow > scanLastN) ? Math.max(2, lastRow - scanLastN + 1) : 2;

const width = maxIdx - minIdx + 1;

const raw = dest.getRange(scanRowStart, minIdx, lastRow - scanRowStart + 1, width).getValues();

if (scanRowStart !== 2) {
  log_(runId, "INFO", "PENDING_SCAN_WINDOW", { scanRowStart, lastRow, scanLastN });
}

  const pendingRows = [];
  for (let i = 0; i < raw.length; i++) {
    if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 2500) break;

    const rowNum = i + scanRowStart;
    const rowSlice = raw[i];

    const getVal = (logical) => {
      const pos = colMap[logical];
      if (pos == null) return "";
      const actualCol = colsToFetch[pos] + 1; // 1-based
      const offset = actualCol - minIdx;
      return rowSlice[offset];
    };

    const id = String(getVal("ID") || "").trim();
    const nip = String(getVal("NIP") || "").trim();
    const sync = String(getVal("SYNC") || "");

    if (!nip) continue;

    const hasMainOk = sync.indexOf(CONFIG.MARKERS.MAIN_OK) >= 0;
    const hasPeopleRefsOk = sync.indexOf("PEOPLE_REFS_OK") >= 0;
    const hasMfOk = sync.indexOf("MF_OK") >= 0;

    // braki refów
    const cref = String(getVal("CREF") || "").trim();
    const mref = String(getVal("MREF") || "").trim();
    const bref = String(getVal("BREF") || "").trim();

    const needsRefs =
      (cIdx != null && !cref) ||
      (mIdx != null && !mref) ||
      (bIdx != null && !bref);

    // pending, jeśli:
    // - brak ID (nigdy nie dokończył startu)
    // - albo brak MF_OK
    // - albo brak PEOPLE_REFS_OK
    // - albo brak APPSHEET_OK
    // - albo brak refów, mimo że main już poszedł (wtedy robimy Edit)
    const isPending =
      !id ||
      !hasMfOk ||
      !hasPeopleRefsOk ||
      !hasMainOk ||
      (hasMainOk && needsRefs);

    if (isPending) pendingRows.push(rowNum);

    if (pendingRows.length >= CONFIG.MAX_ROWS_PER_RUN) break;
  }

  if (pendingRows.length === 0) {
    log_(runId, "INFO", "PENDING_NONE");
    return 0;
  }

  // Procesuj pendingi w małych porcjach (pojedyncze range’y)
  let processed = 0;
  for (let i = 0; i < pendingRows.length; i++) {
    if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 2500) break;

    const r = pendingRows[i];
    processed += processDestRows_(runId, mapping, source, dest, r, r, startedAt, [], (mapping && mapping.sourceKey && typeof mapping.sourceKey.markIdx === 'number') ? mapping.sourceKey.markIdx : -1);
  }

  log_(runId, "INFO", "PENDING_DONE", { pendingRows: pendingRows.length, processed });
  return processed;
}

/** =========================
 * OPTIONAL: Trigger installer
 * ========================= */
function installTimeTriggerEveryMinute() {
  ScriptApp.newTrigger("runSyncAndProcess")
    .timeBased()
    .everyMinutes(1)
    .create();
}