/** =========================
 *  MF VAT
 *  ========================= */
function callMfAndWrite_(runId, dest, mapping, rowNum, nip, dateStr) {
  const nipClean = String(nip || "").trim();
  const url = CONFIG.MF_API_URL
    .replace("{nip}", encodeURIComponent(nipClean))
    .replace("{date}", encodeURIComponent(dateStr));

  log_(runId, "INFO", "MF_CALL", { rowNum });

  let httpCode = 0;
  let body = "";

  try {
    const res = UrlFetchApp.fetch(url, {
      method: "get",
      muteHttpExceptions: true,
      followRedirects: true,
      contentType: "application/json",
      validateHttpsCertificates: true,
      timeout: CONFIG.MF_TIMEOUT_MS
    });
    httpCode = res.getResponseCode();
    body = res.getContentText() || "";
  } catch (e) {
    log_(runId, "WARN", "MF_FETCH_ERROR", { rowNum, err: String(e).slice(0, 400) });
    return;
  }

  if (isVerbose_()) {
    log_(runId, "INFO", "MF_RESULT", { rowNum, httpCode, bodySnippet: body.slice(0, 700) });
  } else {
    log_(runId, "INFO", "MF_RESULT", { rowNum, httpCode });
  }

  if (httpCode !== 200) return;

  const parsed = safeJsonParse_(body);
  const subj = pickSubjectFromMf_(parsed);
  if (!subj) {
    log_(runId, "WARN", "MF_NO_SUBJECT", { rowNum });
    return;
  }

  writeIfColExists_(dest, mapping, rowNum, "name_api", subj.name || "");
  writeIfColExists_(dest, mapping, rowNum, "statusVat", subj.statusVat || "");
  writeIfColExists_(dest, mapping, rowNum, "regon", String(subj.regon || ""));
  writeIfColExists_(dest, mapping, rowNum, "krs", String(subj.krs || ""));
  writeIfColExists_(dest, mapping, rowNum, "registrationLegalDate", subj.registrationLegalDate || "");

  // MF fields that may be absent in response
  writeIfColExists_(dest, mapping, rowNum, "workingAddress", subj.workingAddress || "");
  writeIfColExists_(dest, mapping, rowNum, "residenceAddress", subj.residenceAddress || "");

  // accountNumbers is a list in MF response. Store/send in AppSheet-friendly format.
  // For Sheets/AppSheet a comma-separated string is the most compatible representation.
  const acc = Array.isArray(subj.accountNumbers)
    ? subj.accountNumbers.filter(Boolean).map(String)
    : (subj.accountNumbers ? [String(subj.accountNumbers)] : []);
  writeIfColExists_(dest, mapping, rowNum, "accountNumbers", acc.join(","));

  const nipControlIdx = mapping.destKey.nipControlIdx;
  if (nipControlIdx != null) dest.getRange(rowNum, nipControlIdx + 1).setValue(nipClean);
}

function pickSubjectFromMf_(parsed) {
  try {
    if (parsed && parsed.result && parsed.result.subject) return parsed.result.subject;
    const entries = parsed && parsed.result && parsed.result.entries;
    if (entries && entries.length && entries[0].subjects && entries[0].subjects.length) {
      return entries[0].subjects[0];
    }
    return null;
  } catch (e) {
    return null;
  }
}

function safeDateForMf_(submittedOn) {
  if (submittedOn instanceof Date) {
    return Utilities.formatDate(submittedOn, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }
  const s = String(submittedOn || "").trim();
  if (s) {
    const d = new Date(s);
    if (!isNaN(d.getTime())) {
      return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
    }
  }
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
}
