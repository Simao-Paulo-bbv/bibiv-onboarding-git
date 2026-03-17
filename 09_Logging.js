/** =========================
 *  LOG: compact summary payload
 *  - SUMMARY level will include only minimal, non-noisy fields
 *  - VERBOSE keeps full payload
 *  ========================= */
function compactLogData_(event, data) {
  if (!data) return null;
  try {
    const e = String(event || "");
    // Default: keep only rowNum / counts / codes if present
    const pick = {};
    const has = (k) => Object.prototype.hasOwnProperty.call(data, k);

    // Common small fields
    ["rowNum","srcRow","destRow","imported","processedLogic","elapsedMs","forceImport","httpCode","tableName","action","reason","sourceLastRow","destLastRow","checkedRows"].forEach((k)=>{
      if (has(k)) pick[k] = data[k];
    });

    // Special cases
    if (e === "END") {
      return {
        imported: data.imported,
        processedLogic: data.processedLogic,
        elapsedMs: data.elapsedMs,
        forceImport: data.forceImport
      };
    }
    if (e === "IMPORT_OK") {
      return { imported: data.imported, destStartRow: data.destStartRow, destEndRow: data.destEndRow, forceMode: data.forceMode };
    }
    if (e === "IMPORT_NONE_DIAG") {
      return { reason: data.reason, sourceLastRow: data.sourceLastRow, checkedRows: data.checkedRows, reasons: data.reasons, forceMode: data.forceMode };
    }
    if (e === "MAPPING_READY") {
      return { sourceCols: data.sourceCols, destCols: data.destCols, sourceHasMarkCol: data.sourceHasMarkCol, destHasSyncStatus: data.destHasSyncStatus, hasRefCols: data.hasRefCols };
    }
    if (e === "DEST_HEADERS_OK" || e === "DEST_HEADERS_ENFORCED") {
      return { totalCols: data.totalCols, schemaCols: data.schemaCols, lastCol: data.lastCol };
    }
    if (e === "MF_RESULT") {
      return { rowNum: data.rowNum, httpCode: data.httpCode };
    }
    if (e === "APPSHEET_RESPONSE") {
      return { rowNum: data.rowNum, tableName: data.tableName, httpCode: data.httpCode };
    }
    if (e === "APPSHEET_FAIL" || e === "MF_FETCH_ERROR" || e === "FATAL") {
      // keep short error
      return { rowNum: data.rowNum, message: data.message || data.err || data.error || "" };
    }

    // If we have anything meaningful, return it; otherwise null (event-only summary)
    const keys = Object.keys(pick);
    return keys.length ? pick : null;
  } catch (e) {
    return null;
  }
}

/** =========================
 *  LOGGING
 *  ========================= */
function log_(runId, level, event, data) {
  const cfg = CONFIG.LOGGING;
  if (!cfg.ENABLE || cfg.LEVEL === "OFF") return;

  const ts = formatNow_();
  const shortOnly = cfg.LEVEL === "SUMMARY";

  let payload = "";
  if (!shortOnly) {
    payload = data ? JSON.stringify(data) : "";
  } else {
    const compact = compactLogData_(event, data);
    payload = compact ? JSON.stringify(compact) : "";
  }

  const line = shortOnly
    ? (payload ? `[${level}] [${runId}] ${event} :: ${payload}` : `[${level}] [${runId}] ${event}`)
    : `[${level}] [${runId}] ${event} :: ${payload || "{}"}`;

  if (cfg.TO_CONSOLE) {
    if (level === "ERROR") console.error(line);
    else if (level === "WARN") console.warn(line);
    else console.log(line);
  }

  if (cfg.TO_EXECUTION_LOGGER) {
    Logger.log(line);
  }

  if (cfg.TO_SHEET) {
    try {
      const ss = SpreadsheetApp.openById(CONFIG.LOG_SPREADSHEET_ID);
      const sh = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);
      if (!sh) return;
      sh.appendRow([ts, level, runId, event, shortOnly ? payload : (payload || "{}")]);
    } catch (e) {}
  }
}

function ensureLogSheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (sh) return;
  sh = ss.insertSheet(name);
  sh.getRange(1, 1, 1, 5).setValues([["ts", "level", "runId", "event", "data"]]);
}
