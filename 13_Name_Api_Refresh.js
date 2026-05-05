/** =========================
 *  NAME_API ONLY REFRESH
 *  - maintenance job for historical rows
 *  - updates only DEST[name_api]
 *  - does not import, enrich other fields, call AppSheet, People, or Bank_Accounts
 *  ========================= */

function runRefreshNameApiOnly() {
  const runId = makeRunId_("name-api");
  const startedAt = Date.now();

  if (!CONFIG.NAME_API_REFRESH_ENABLED) {
    log_(runId, "INFO", "NAME_API_REFRESH_SKIP_DISABLED");
    return { ok: true, enabled: false, updated: 0, examined: 0 };
  }

  const lock = LockService.getScriptLock();
  const gotLock = lock.tryLock(CONFIG.LOCK_TIMEOUT_MS);
  if (!gotLock) {
    log_(runId, "WARN", "NAME_API_REFRESH_LOCK_NOT_ACQUIRED");
    return { ok: false, reason: "LOCK_NOT_ACQUIRED", updated: 0, examined: 0 };
  }

  try {
    const sourceSS = openSpreadsheetWithLog_(runId, CONFIG.SOURCE_SPREADSHEET_ID, "SOURCE_SPREADSHEET_ID");
    const destSS = openSpreadsheetWithLog_(runId, CONFIG.DEST_SPREADSHEET_ID, "DEST_SPREADSHEET_ID");
    const source = getSheet_(sourceSS, CONFIG.SOURCE_SHEET_NAME, false);
    const dest = getSheet_(destSS, CONFIG.DEST_SHEET_NAME, false);
    if (!source || !dest) {
      log_(runId, "ERROR", "NAME_API_REFRESH_SHEET_MISSING", { sourceOk: !!source, destOk: !!dest });
      return { ok: false, reason: "SHEET_MISSING", updated: 0, examined: 0 };
    }

    if (CONFIG.FEATURES.ENFORCE_DEST_HEADERS) enforceDestHeaders_(runId, dest);
    const mapping = buildMapping_(runId, source, dest);
    const result = refreshNameApiOnly_(runId, mapping, dest, startedAt);
    log_(runId, "INFO", "NAME_API_REFRESH_END", result);
    return result;
  } catch (e) {
    log_(runId, "ERROR", "NAME_API_REFRESH_FATAL", { message: String(e), stack: e && e.stack ? String(e.stack) : "" });
    throw e;
  } finally {
    lock.releaseLock();
  }
}

function refreshNameApiOnly_(runId, mapping, dest, startedAt) {
  const out = { ok: true, enabled: true, updated: 0, examined: 0, skipped: 0, failed: 0 };
  const lastRow = dest.getLastRow();
  if (lastRow < 2) {
    log_(runId, "INFO", "NAME_API_REFRESH_NONE", { reason: "DEST_EMPTY" });
    return out;
  }

  const nameIdx = mapping && mapping.dstIndex ? mapping.dstIndex["name_api"] : null;
  const nipIdx = mapping && mapping.destKey
    ? (mapping.destKey.nipControlIdx != null ? mapping.destKey.nipControlIdx : mapping.destKey.nipIdx)
    : null;
  if (nameIdx == null || nipIdx == null) {
    log_(runId, "WARN", "NAME_API_REFRESH_MISSING_COLS", { nameIdx: nameIdx, nipIdx: nipIdx });
    out.ok = false;
    out.reason = "MISSING_COLS";
    return out;
  }

  const maxRows = Number(CONFIG.NAME_API_REFRESH_MAX_ROWS_PER_RUN || 100) || 100;
  const scanLastN = Number(CONFIG.NAME_API_REFRESH_SCAN_LAST_N || 0) || 0;
  const scanRowStart = (scanLastN && lastRow > scanLastN) ? Math.max(2, lastRow - scanLastN + 1) : 2;
  const overwriteExisting = CONFIG.NAME_API_REFRESH_OVERWRITE_EXISTING !== false;
  const targetNips = buildNameApiRefreshTargetNips_();
  const hasTargetNips = Object.keys(targetNips).length > 0;

  const minIdx = Math.min(nameIdx, nipIdx) + 1;
  const maxIdx = Math.max(nameIdx, nipIdx) + 1;
  const width = maxIdx - minIdx + 1;
  const values = dest.getRange(scanRowStart, minIdx, lastRow - scanRowStart + 1, width).getValues();
  const cache = {};

  for (let i = 0; i < values.length; i++) {
    if (out.examined >= maxRows) break;
    if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 2500) break;

    const rowNum = scanRowStart + i;
    const row = values[i];
    const currentName = String(row[(nameIdx + 1) - minIdx] || "").trim();
    const nip = normalizeNipForApi_(row[(nipIdx + 1) - minIdx]);
    if (!nip) {
      out.skipped++;
      continue;
    }
    if (hasTargetNips && !targetNips[nip]) {
      out.skipped++;
      continue;
    }
    if (!overwriteExisting && currentName) {
      out.skipped++;
      continue;
    }

    out.examined++;
    let regonName = cache[nip];
    if (regonName == null) {
      regonName = fetchNameApiFromRegonOnly_(runId, rowNum, nip);
      cache[nip] = regonName || "";
    }

    if (!regonName) {
      out.failed++;
      continue;
    }
    if (currentName === regonName) {
      out.skipped++;
      continue;
    }

    dest.getRange(rowNum, nameIdx + 1).setValue(regonName);
    out.updated++;
    log_(runId, "INFO", "NAME_API_REFRESH_UPDATED", {
      rowNum: rowNum,
      nip: nip,
      from: currentName,
      to: regonName
    });
  }

  return out;
}

function buildNameApiRefreshTargetNips_() {
  const out = {};
  const raw = CONFIG && CONFIG.NAME_API_REFRESH_TARGET_NIPS;
  const list = Array.isArray(raw) ? raw : [];
  for (let i = 0; i < list.length; i++) {
    const nip = normalizeNipForApi_(list[i]);
    if (nip) out[nip] = true;
  }
  return out;
}

function fetchNameApiFromRegonOnly_(runId, rowNum, nip) {
  const nipClean = normalizeNipForApi_(nip);
  const govBase = String((CONFIG && CONFIG.GOV_API_BASE_URL) || "").trim();
  const govKey = String((CONFIG && CONFIG.GOV_API_KEY) || "").trim();
  if (!govBase || !govKey || !nipClean) return "";

  try {
    log_(runId, "INFO", "NAME_API_REFRESH_REGON_CALL", { rowNum: rowNum, nip: nipClean });
    const res = fetchGovApiGet_(CONFIG.GOV_REGON_PATH, { nip: nipClean });
    if (res.httpCode !== 200) {
      log_(runId, "WARN", "NAME_API_REFRESH_REGON_HTTP", { rowNum: rowNum, nip: nipClean, httpCode: res.httpCode });
      return "";
    }
    const name = pickNameFromRegon_(res.parsed, "", nipClean);
    if (!name) {
      log_(runId, "WARN", "NAME_API_REFRESH_REGON_NO_NAME", { rowNum: rowNum, nip: nipClean });
      return "";
    }
    return name;
  } catch (e) {
    log_(runId, "WARN", "NAME_API_REFRESH_REGON_FAIL", { rowNum: rowNum, nip: nipClean, err: String(e).slice(0, 400) });
    return "";
  }
}
