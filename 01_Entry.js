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
    const effectiveUser = safeGetEffectiveUserEmail_();
    log_(runId, "INFO", "START", {
      effectiveUser: effectiveUser || "",
      mfUseRelay: !!(CONFIG && CONFIG.MF_USE_RELAY),
      mfRelayUrl: String((CONFIG && CONFIG.MF_RELAY_URL) || ""),
      mfRelayUseGoogleIdToken: !CONFIG || CONFIG.MF_RELAY_USE_GOOGLE_ID_TOKEN !== false,
      mfRelayIdTokenSa: String((CONFIG && CONFIG.MF_RELAY_IDTOKEN_SERVICE_ACCOUNT) || "")
    });

    const sourceSS = openSpreadsheetWithLog_(runId, CONFIG.SOURCE_SPREADSHEET_ID, "SOURCE_SPREADSHEET_ID");
    const destSS = openSpreadsheetWithLog_(runId, CONFIG.DEST_SPREADSHEET_ID, "DEST_SPREADSHEET_ID");

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
    let backfill = { rows: 0, cells: 0, candidates: 0, apiCalls: 0 };

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
      processed = processPendingDestRows_(
        runId,
        mapping,
        source,
        dest,
        startedAt
      );
    } else {
      log_(runId, "INFO", "NO_NEW_ROWS_TO_PROCESS");
    }

    // Optional one-off maintenance: fill missing fields in already imported rows.
    // This step updates only blank values and never touches Status.
    if (CONFIG.BACKFILL_EXISTING_ENABLED && hasDestData) {
      backfill = backfillExistingMissingFields_(runId, mapping, source, dest, startedAt);
    }

    log_(runId, "INFO", "END", {
      imported: importRes.imported,
      processedLogic: processed,
      backfillRows: backfill.rows || 0,
      backfillCells: backfill.cells || 0,
      backfillCandidates: backfill.candidates || 0,
      backfillApiCalls: backfill.apiCalls || 0,
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

function safeGetEffectiveUserEmail_() {
  try {
    const u = Session.getEffectiveUser();
    return u && u.getEmail ? String(u.getEmail() || "") : "";
  } catch (e) {
    return "";
  }
}

function openSpreadsheetWithLog_(runId, spreadsheetId, configKey) {
  try {
    return SpreadsheetApp.openById(spreadsheetId);
  } catch (e) {
    log_(runId, "ERROR", "SPREADSHEET_ACCESS_DENIED", {
      configKey: configKey,
      spreadsheetId: String(spreadsheetId || ""),
      err: String(e)
    });
    throw e;
  }
}

function processPendingDestRows_(runId, mapping, source, dest, startedAt) {
  const lastRow = dest.getLastRow();
  if (lastRow < 2) return 0;

  const syncIdx = mapping.destKey.syncStatusIdx;
  const idIdx = mapping.destKey.idIdx;
  const statusIdx = mapping.destKey.statusIdx;
  const nipIdx = mapping.destKey.nipControlIdx != null ? mapping.destKey.nipControlIdx : mapping.destKey.nipIdx;

  // ref cols (mogą istnieć)
  const cIdx = mapping.destKey.contactPersonIdx;
  const mIdx = mapping.destKey.managerPersonIdx;
  const bIdx = mapping.destKey.beneficialPersonIdx;
  const cNameIdx = mapping.dstIndex["imię i nazwisko osoby kontaktowej"];
  const mNameIdx = mapping.dstIndex["imię i nazwisko kierownika"];
  const bNameIdx = mapping.dstIndex["imię i nazwisko beneficjenta"];

  if (syncIdx == null || idIdx == null || nipIdx == null) {
    log_(runId, "WARN", "PENDING_SKIP_MISSING_INDEXES", { syncIdx, idIdx, statusIdx, nipIdx });
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
  addCol(statusIdx, "STATUS");
  addCol(cIdx, "CREF");
  addCol(mIdx, "MREF");
  addCol(bIdx, "BREF");
  addCol(cNameIdx, "CNAME");
  addCol(mNameIdx, "MNAME");
  addCol(bNameIdx, "BNAME");

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
    const status = String(getVal("STATUS") || "").trim();

    if (!nip) continue;

    const isStatusInit = (status !== "" && status === String(CONFIG.STATUS_TO_SEND));
    const hasExternalManagedStatus = (status !== "" && !isStatusInit);
    if (hasExternalManagedStatus) continue;

    const hasMainOk = sync.indexOf(CONFIG.MARKERS.MAIN_OK) >= 0;
    const hasRegonBlock = sync.indexOf("MF_REGON_BLOCK") >= 0;
    const hasVatBlock = sync.indexOf("MF_VAT_BLOCK") >= 0;
    const hasMfRateLimit = sync.indexOf("MF_RATE_LIMIT") >= 0;
    if (hasRegonBlock || hasVatBlock || hasMfRateLimit) continue;
    // braki refów
    const cref = String(getVal("CREF") || "").trim();
    const mref = String(getVal("MREF") || "").trim();
    const bref = String(getVal("BREF") || "").trim();
    const cName = String(getVal("CNAME") || "").trim();
    const mName = String(getVal("MNAME") || "").trim();
    const bName = String(getVal("BNAME") || "").trim();

    const needsRefs =
      (cIdx != null && cName !== "" && !cref) ||
      (mIdx != null && mName !== "" && !mref) ||
      (bIdx != null && bName !== "" && !bref);

    // Pending only when MAIN is not yet confirmed in AppSheet
    // or when MAIN exists but refs are still missing (Edit backfill).
    // Do not depend on MF_OK/PEOPLE markers here — they are auxiliary and
    // may be absent on historical rows, which caused starvation of true pending rows.
    const isPending =
      !id ||
      !hasMainOk ||
      (hasMainOk && needsRefs);

    if (isPending) pendingRows.push(rowNum);

    if (pendingRows.length >= CONFIG.MAX_ROWS_PER_RUN) break;
  }

  if (pendingRows.length === 0) {
    log_(runId, "INFO", "PENDING_NONE");
    return 0;
  }

  // Procesuj pendingi w porcjach po ciągłych zakresach (mniej overhead niż 1-row call).
  const groups = [];
  let gStart = pendingRows[0];
  let gEnd = pendingRows[0];
  for (let i = 1; i < pendingRows.length; i++) {
    const r = pendingRows[i];
    if (r === gEnd + 1) {
      gEnd = r;
    } else {
      groups.push({ start: gStart, end: gEnd });
      gStart = r;
      gEnd = r;
    }
  }
  groups.push({ start: gStart, end: gEnd });

  const sourceMarkIdx =
    (mapping && mapping.sourceKey && typeof mapping.sourceKey.markIdx === "number")
      ? mapping.sourceKey.markIdx
      : -1;

  let processed = 0;
  for (let i = 0; i < groups.length; i++) {
    if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 2500) break;
    const g = groups[i];
    processed += processDestRows_(runId, mapping, source, dest, g.start, g.end, startedAt, [], sourceMarkIdx);
  }

  log_(runId, "INFO", "PENDING_DONE", { pendingRows: pendingRows.length, groups: groups.length, processed });
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
