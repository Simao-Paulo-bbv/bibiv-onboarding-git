/*******************************************************
 * Manual maintenance helpers for Docs Creator
 *******************************************************/

const MANUAL_TEMPLATE_PDF_GENERATION = {
  ONBOARDING_IDS_TEXT: "",
  ONBOARDING_IDS: [],
  OUTPUT_FOLDER_ID: "",
  TEMPLATE_DOC_ID: ""
};

const MANUAL_DOCGEN_REQUEUE = {
  ONBOARDING_ID: "",
  JOB_ID: "",
  AGREEMENT_FILE_ID: ""
};

function runAuthorizeDocGeneratorSheetAccess() {
  const runId = makeRunId_();
  const spreadsheetId = String(CONFIG.DOC_GENERATOR.DATA_SPREADSHEET_ID || "").trim();
  if (!spreadsheetId) throw new Error("CONFIG.DOC_GENERATOR.DATA_SPREADSHEET_ID is blank.");

  const spreadsheet = SpreadsheetApp.openById(spreadsheetId);
  const sheetName = getSheetNameForDocgenTable_(DOCGEN_TABLES.MAIN);
  const sheet = spreadsheet.getSheetByName(sheetName);
  if (!sheet) throw new Error("Sheet not found: " + sheetName);

  const sample = sheet.getLastRow() && sheet.getLastColumn()
    ? sheet.getRange(1, 1, 1, Math.min(sheet.getLastColumn(), 5)).getValues()[0]
    : [];
  log_(runId, "INFO", "DOCGEN_SHEET_ACCESS_AUTHORIZED", {
    spreadsheetId: spreadsheetId,
    spreadsheetName: spreadsheet.getName(),
    sheetName: sheetName,
    sampleCols: sample.length
  });
  return {
    ok: true,
    spreadsheetId: spreadsheetId,
    spreadsheetName: spreadsheet.getName(),
    sheetName: sheetName
  };
}

function runAuthorizeDocGeneratorDocsApiAccess() {
  const runId = makeRunId_();
  const doc = DocumentApp.create("DOCGEN Docs API authorization check");
  const docId = doc.getId();
  doc.getBody().appendParagraph("DOCGEN_DOCS_API_AUTH_CHECK");
  doc.saveAndClose();

  try {
    const url = "https://docs.googleapis.com/v1/documents/" + encodeURIComponent(docId) +
      "?fields=documentId,title,body";
    const response = UrlFetchApp.fetch(url, {
      method: "get",
      headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
      muteHttpExceptions: true
    });
    const httpCode = response.getResponseCode();
    const body = response.getContentText();

    if (httpCode < 200 || httpCode >= 300) {
      log_(runId, "ERROR", "DOCGEN_DOCS_API_AUTH_FAILED", {
        httpCode: httpCode,
        docId: docId,
        message: body.slice(0, 1000)
      });
      throw new Error(
        "Google Docs API authorization check failed. httpCode=" + httpCode +
        ". If the message says docs.googleapis.com is disabled, enable Google Docs API in the Google Cloud project first."
      );
    }

    const parsed = safeJsonParse_(body) || {};
    log_(runId, "INFO", "DOCGEN_DOCS_API_AUTHORIZED", {
      httpCode: httpCode,
      docId: docId,
      title: parsed.title || ""
    });
    return {
      ok: true,
      httpCode: httpCode,
      docId: docId,
      title: parsed.title || ""
    };
  } finally {
    try {
      DriveApp.getFileById(docId).setTrashed(true);
    } catch (e) {
      log_(runId, "WARN", "DOCGEN_DOCS_API_AUTH_TEMP_DOC_CLEANUP_FAILED", {
        docId: docId,
        error: e && e.message || String(e)
      });
    }
  }
}

function runAuthorizeDocGeneratorAllAccess() {
  const runId = makeRunId_();
  const checks = [];

  checks.push(runAuthorizationCheck_(runId, "script", () => {
    return {
      tokenLength: String(ScriptApp.getOAuthToken() || "").length
    };
  }));

  checks.push(runAuthorizationCheck_(runId, "external_request", () => {
    const response = UrlFetchApp.fetch("https://www.googleapis.com/discovery/v1/apis/docs/v1/rest", {
      muteHttpExceptions: true
    });
    return {
      httpCode: response.getResponseCode()
    };
  }));

  checks.push(runAuthorizationCheck_(runId, "drive", () => {
    const root = DriveApp.getFolderById(CONFIG.DOC_GENERATOR.OUTPUT_ROOT_FOLDER_ID);
    return {
      outputRootFolderId: root.getId(),
      outputRootFolderName: root.getName()
    };
  }));

  checks.push(runAuthorizationCheck_(runId, "documents", () => {
    const doc = DocumentApp.create("DOCGEN DocumentApp authorization check");
    const docId = doc.getId();
    doc.getBody().appendParagraph("DOCGEN_DOCUMENT_APP_AUTH_CHECK");
    doc.saveAndClose();
    DriveApp.getFileById(docId).setTrashed(true);
    return {
      docId: docId
    };
  }));

  checks.push(runAuthorizationCheck_(runId, "spreadsheets", () => runAuthorizeDocGeneratorSheetAccess()));
  checks.push(runAuthorizationCheck_(runId, "docs_api", () => runAuthorizeDocGeneratorDocsApiAccess()));

  const failed = checks.filter(check => !check.ok);
  log_(runId, failed.length ? "ERROR" : "INFO", "DOCGEN_ALL_ACCESS_AUTHORIZATION_END", {
    ok: failed.length === 0,
    total: checks.length,
    failed: failed.length,
    checks: checks
  });

  if (failed.length) {
    throw new Error("Doc generator authorization checks failed: " + failed.map(check => check.name).join(", "));
  }

  return {
    ok: true,
    checks: checks
  };
}

function runManualRequeueDocGenerationJob() {
  const runId = makeRunId_();
  const args = getManualDocgenRequeueSettings_();
  const result = enqueueDocGenerationJob_(runId, args, { front: true, continuation: true });
  ensureDocGenerationQueueTrigger({ refresh: true });
  log_(runId, "INFO", "MANUAL_DOCGEN_JOB_REQUEUED", {
    onboardingId: args.onboardingId,
    jobId: args.jobId,
    agreementFileId: args.agreementFileId,
    added: Boolean(result && result.added),
    queueSize: result && result.size || ""
  });
  return {
    ok: true,
    queued: true,
    added: Boolean(result && result.added),
    queueSize: result && result.size || 0,
    args: args
  };
}

function getManualDocgenRequeueSettings_() {
  const onboardingId = String(MANUAL_DOCGEN_REQUEUE.ONBOARDING_ID || "").trim();
  const jobId = String(MANUAL_DOCGEN_REQUEUE.JOB_ID || "").trim();
  const agreementFileId = String(MANUAL_DOCGEN_REQUEUE.AGREEMENT_FILE_ID || "").trim();

  if (!onboardingId) throw new Error("MANUAL_DOCGEN_REQUEUE.ONBOARDING_ID is blank.");
  if (!jobId && !agreementFileId) {
    throw new Error("Set MANUAL_DOCGEN_REQUEUE.JOB_ID or AGREEMENT_FILE_ID.");
  }

  return {
    onboardingId: onboardingId,
    jobId: jobId,
    agreementFileId: agreementFileId
  };
}

function runAuthorizationCheck_(runId, name, fn) {
  try {
    const result = fn();
    const check = {
      name: name,
      ok: true,
      result: result || {}
    };
    log_(runId, "INFO", "DOCGEN_AUTHORIZATION_CHECK_OK", check);
    return check;
  } catch (e) {
    const check = {
      name: name,
      ok: false,
      error: e && e.message || String(e)
    };
    log_(runId, "ERROR", "DOCGEN_AUTHORIZATION_CHECK_FAILED", check);
    return check;
  }
}

function runManualGenerateTemplatePdfs() {
  const runId = makeRunId_();
  const settings = getManualTemplatePdfGenerationSettings_();
  const templateRow = findManualDocTemplateRow_(runId, settings.templateDocId);
  const outputRootFolder = DriveApp.getFolderById(settings.outputFolderId);
  const outputFolder = ensureManualJobOutputFolder_(runId, outputRootFolder);
  const results = [];

  log_(runId, "INFO", "MANUAL_TEMPLATE_PDF_GENERATION_START", {
    onboardingIds: settings.onboardingIds,
    outputFolderId: settings.outputFolderId,
    templateDocId: settings.templateDocId,
    jobFolderId: outputFolder.getId(),
    jobFolderName: outputFolder.getName()
  });

  settings.onboardingIds.forEach(onboardingId => {
    try {
      const mainRow = findManualMainOnboardingRow_(runId, onboardingId);
      const fileRow = buildManualTemplatePdfFileRow_(runId, mainRow, onboardingId, outputFolder, settings.templateDocId);
      const generated = generatePdfForAgreementFileRow_(runId, fileRow, mainRow, templateRow, {
        outputRootFolder: outputFolder
      });
      results.push({
        onboardingId: onboardingId,
        ok: true,
        file: generated.relativePath,
        pdfId: generated.pdfId,
        docId: generated.docId
      });
      log_(runId, "INFO", "MANUAL_TEMPLATE_PDF_GENERATED", {
        onboardingId: onboardingId,
        relativePath: generated.relativePath,
        pdfId: generated.pdfId
      });
    } catch (err) {
      const message = String(err && err.message || err);
      results.push({
        onboardingId: onboardingId,
        ok: false,
        error: message
      });
      log_(runId, "ERROR", "MANUAL_TEMPLATE_PDF_FAILED", {
        onboardingId: onboardingId,
        error: message
      });
    }
  });

  const failed = results.filter(item => !item.ok);
  log_(runId, failed.length ? "ERROR" : "INFO", "MANUAL_TEMPLATE_PDF_GENERATION_END", {
    total: results.length,
    failed: failed.length,
    outputFolderId: settings.outputFolderId,
    templateDocId: settings.templateDocId,
    jobFolderId: outputFolder.getId(),
    jobFolderName: outputFolder.getName()
  });

  return {
    ok: failed.length === 0,
    total: results.length,
    failed: failed.length,
    results: results
  };
}

function getManualTemplatePdfGenerationSettings_() {
  const rawIds = MANUAL_TEMPLATE_PDF_GENERATION.ONBOARDING_IDS || [];
  const rawIdsText = String(MANUAL_TEMPLATE_PDF_GENERATION.ONBOARDING_IDS_TEXT || "");
  const onboardingIds = [];
  const seen = {};

  rawIdsText
    .split(/[\n,\t; ]+/)
    .forEach(value => {
      const clean = String(value || "").trim();
      if (!clean || seen[clean]) return;
      seen[clean] = true;
      onboardingIds.push(clean);
    });

  rawIds.forEach(value => {
    const clean = String(value || "").trim();
    if (!clean || seen[clean]) return;
    seen[clean] = true;
    onboardingIds.push(clean);
  });

  const outputFolderId = String(MANUAL_TEMPLATE_PDF_GENERATION.OUTPUT_FOLDER_ID || "").trim();
  const templateDocId = String(MANUAL_TEMPLATE_PDF_GENERATION.TEMPLATE_DOC_ID || "").trim();

  if (!onboardingIds.length) {
    throw new Error("MANUAL_TEMPLATE_PDF_GENERATION.ONBOARDING_IDS_TEXT / ONBOARDING_IDS is empty.");
  }
  if (!outputFolderId) {
    throw new Error("MANUAL_TEMPLATE_PDF_GENERATION.OUTPUT_FOLDER_ID is blank.");
  }
  if (!templateDocId) {
    throw new Error("MANUAL_TEMPLATE_PDF_GENERATION.TEMPLATE_DOC_ID is blank.");
  }

  return {
    onboardingIds: onboardingIds,
    outputFolderId: outputFolderId,
    templateDocId: templateDocId
  };
}

function buildManualTemplatePdfFileRow_(runId, mainRow, onboardingId, outputFolder, templateDocId) {
  const existingAgreementFileRow = findManualAgreementFileRow_(runId, onboardingId, templateDocId);
  const pdfName = buildManualTemplatePdfFileName_(runId, mainRow, existingAgreementFileRow, templateDocId);
  const relativePath = pdfName;

  log_(runId, "INFO", "MANUAL_TEMPLATE_PDF_FILE_ROW", {
    onboardingId: onboardingId,
    outputFolderId: outputFolder.getId(),
    fileName: pdfName,
    namingSource: existingAgreementFileRow ? "Agreements_Files" : "AppSheetFormulaFallback"
  });

  return {
    ID: "manual-" + onboardingId + "-" + templateDocId.slice(0, 8),
    Onboarding_ID: onboardingId,
    Template_ID_Reference: String(templateDocId || "").trim(),
    File_Name: String(pdfName || "").replace(/\.pdf$/i, ""),
    Prefix: existingAgreementFileRow ? (getField_(existingAgreementFileRow, "Prefix") || "") : "",
    File: relativePath
  };
}

function findManualDocTemplateRow_(runId, templateDocId) {
  const cleanTemplateId = String(templateDocId || "").trim();
  if (!cleanTemplateId) throw new Error("Missing template doc ID.");

  const sheetName = getSheetNameForDocgenTable_(DOCGEN_TABLES.DOC_TEMPLATES);
  const values = fetchManualSpreadsheetSheetValues_(runId, sheetName);
  const row = findRowInValuesByColumn_(values, "Template_ID", cleanTemplateId);
  if (row) return row;
  return buildFallbackTemplateRow_(runId, cleanTemplateId);
}

function findManualAgreementFileRow_(runId, onboardingId, templateDocId) {
  const cleanOnboardingId = String(onboardingId || "").trim();
  const cleanTemplateId = String(templateDocId || "").trim();
  if (!cleanOnboardingId || !cleanTemplateId) return null;

  const sheetName = getSheetNameForDocgenTable_(DOCGEN_TABLES.AGREEMENTS_FILES);
  const values = fetchManualSpreadsheetSheetValues_(runId, sheetName);
  const rows = buildRowsFromValues_(values);
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (String(getField_(row, "Onboarding_ID") || "").trim() !== cleanOnboardingId) continue;
    if (String(getField_(row, "Template_ID_Reference") || "").trim() !== cleanTemplateId) continue;
    return row;
  }
  return null;
}

function findManualMainOnboardingRow_(runId, onboardingId) {
  const cleanId = String(onboardingId || "").trim();
  if (!cleanId) throw new Error("Missing onboarding ID.");

  const sheetName = getSheetNameForDocgenTable_(DOCGEN_TABLES.MAIN);
  const values = fetchManualSpreadsheetSheetValues_(runId, sheetName);
  if (!values || values.length < 2) {
    throw new Error("Main onboarding sheet is empty: " + sheetName);
  }

  const row = findRowInValuesByColumn_(values, "ID", cleanId);
  if (row) return row;
  throw new Error("Onboarding row not found in sheet for ID: " + cleanId);
}

function fetchManualSpreadsheetSheetValues_(runId, sheetName) {
  const cleanSheetName = String(sheetName || "").trim();
  if (!cleanSheetName) return [];
  if (DOCGEN_RUNTIME_CACHE.sheetValuesByName[cleanSheetName]) {
    return DOCGEN_RUNTIME_CACHE.sheetValuesByName[cleanSheetName];
  }

  const spreadsheetId = String(CONFIG.DOC_GENERATOR.DATA_SPREADSHEET_ID || "").trim();
  if (!spreadsheetId) {
    throw new Error("CONFIG.DOC_GENERATOR.DATA_SPREADSHEET_ID is blank.");
  }

  const spreadsheet = SpreadsheetApp.openById(spreadsheetId);
  const sheet = spreadsheet.getSheetByName(cleanSheetName);
  if (!sheet) {
    throw new Error("Sheet not found: " + cleanSheetName);
  }

  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();
  if (lastRow < 1 || lastColumn < 1) {
    DOCGEN_RUNTIME_CACHE.sheetValuesByName[cleanSheetName] = [];
    return [];
  }

  const values = sheet.getRange(1, 1, lastRow, lastColumn).getValues();
  DOCGEN_RUNTIME_CACHE.sheetValuesByName[cleanSheetName] = values;
  log_(runId, "INFO", "MANUAL_SPREADSHEET_READ", {
    spreadsheetId: spreadsheetId,
    sheetName: cleanSheetName,
    rows: values.length,
    cols: lastColumn
  });
  return values;
}

function findRowInValuesByColumn_(values, columnName, wantedValue) {
  const headers = (values[0] || []).map(header => String(header || "").trim());
  const columnIndex = headers.indexOf(String(columnName || "").trim());
  if (columnIndex < 0) {
    throw new Error("Column " + columnName + " not found in sheet.");
  }

  for (let r = 1; r < values.length; r++) {
    if (String(values[r][columnIndex] || "").trim() !== String(wantedValue || "").trim()) continue;
    const row = {};
    headers.forEach((header, c) => {
      if (header) row[header] = values[r][c];
    });
    return row;
  }
  return null;
}

function buildRowsFromValues_(values) {
  const headers = (values[0] || []).map(header => String(header || "").trim());
  const rows = [];
  for (let r = 1; r < values.length; r++) {
    const row = {};
    headers.forEach((header, c) => {
      if (header) row[header] = values[r][c];
    });
    rows.push(row);
  }
  return rows;
}

function ensureManualJobOutputFolder_(runId, outputRootFolder) {
  const datePart = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
  const prefix = datePart + "__v";
  let nextVersion = 1;
  const folders = outputRootFolder.getFolders();

  while (folders.hasNext()) {
    const folder = folders.next();
    const name = String(folder.getName() || "").trim();
    const match = name.match(new RegExp("^" + escapeRegExp_(prefix) + "(\\d+)$"));
    if (!match) continue;
    nextVersion = Math.max(nextVersion, Number(match[1]) + 1);
  }

  const folderName = prefix + String(nextVersion);
  const jobFolder = outputRootFolder.createFolder(folderName);
  log_(runId, "INFO", "MANUAL_TEMPLATE_PDF_JOB_FOLDER_CREATED", {
    outputRootFolderId: outputRootFolder.getId(),
    jobFolderId: jobFolder.getId(),
    jobFolderName: folderName
  });
  return jobFolder;
}

function buildManualTemplatePdfFileName_(runId, mainRow, existingAgreementFileRow, templateDocId) {
  if (existingAgreementFileRow) {
    const existingFile = String(getField_(existingAgreementFileRow, "File") || "").trim();
    const existingName = existingFile ? existingFile.split("/").pop() : "";
    if (existingName) return existingName;
  }

  const templateRow = findManualDocTemplateRow_(runId, templateDocId);
  const prefix = sanitizeDriveFileNamePart_(
    getField_(templateRow, "File_Name_Prefix") ||
    getField_(templateRow, "Prefix") ||
    getField_(templateRow, "Template_Name") ||
    getField_(templateRow, "Name") ||
    "Document"
  );
  const nipControl = sanitizeDriveFileNamePart_(getManualNipControl_(mainRow));
  const datePart = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd-MM-yyyy");
  const extension = normalizeManualFileExtension_(
    getField_(templateRow, "File Extension") ||
    getField_(templateRow, "File_Extension") ||
    ".pdf"
  );
  return [prefix, nipControl, datePart].filter(Boolean).join("__") + extension;
}

function sanitizeDriveFileNamePart_(value) {
  return String(value || "")
    .trim()
    .replace(/[\\/:*?"<>|#%]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\.+$/g, "")
    .trim();
}

function getManualNipControl_(mainRow) {
  return String(
    getField_(mainRow, "NIP_Control") ||
    getField_(mainRow, "NIP Control") ||
    getField_(mainRow, "NIP") ||
    ""
  ).trim();
}

function normalizeManualFileExtension_(value) {
  const clean = String(value || "").trim();
  if (!clean) return ".pdf";
  return clean.charAt(0) === "." ? clean : "." + clean;
}
