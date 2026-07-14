/*******************************************************
 * Independent document generation by NIP or Onboarding ID
 *
 * Fill the settings below and run:
 *   runGenerateSingleDocuments
 *
 * OUTPUT_FOLDER may be blank (default), a folder name,
 * a Google Drive folder ID, or a Google Drive folder URL.
 * Text lists accept values separated by commas, semicolons,
 * tabs, or new lines.
 *******************************************************/

const SINGLE_DOCUMENT_GENERATION = {
  NIP: "",
  NIPS_TEXT: "",
  NIPS: [],
  ONBOARDING_ID: "",
  ONBOARDING_IDS_TEXT: "",
  ONBOARDING_IDS: [],
  TEMPLATE_ID: "",
  OUTPUT_FOLDER: ""
};

const SINGLE_DOCUMENT_DEFAULT_OUTPUT_FOLDER = "Files_Single_Generations_";

function runGenerateSingleDocumentForNip() {
  return runGenerateSingleDocuments();
}

function runGenerateSingleDocuments() {
  return generateSingleDocumentsForEntities_(SINGLE_DOCUMENT_GENERATION);
}

function generateSingleDocumentForNip(nip, templateId, outputFolder) {
  const batch = generateSingleDocumentsForEntities_({
    NIP: nip,
    TEMPLATE_ID: templateId,
    OUTPUT_FOLDER: outputFolder
  });
  if (!batch.ok) {
    throw new Error(batch.results[0] && batch.results[0].error || "Single document generation failed.");
  }
  return batch.results[0];
}

function generateSingleDocumentsForEntities_(options) {
  const runId = makeRunId_();
  const settings = getSingleDocumentBatchSettings_(options || {});

  log_(runId, "INFO", "SINGLE_DOCGEN_START", {
    nips: settings.nips,
    onboardingIds: settings.onboardingIds,
    templateId: settings.templateId,
    outputFolder: settings.outputFolder || SINGLE_DOCUMENT_DEFAULT_OUTPUT_FOLDER
  });

  const resolved = resolveSingleDocumentMainRows_(runId, settings);
  if (!resolved.targets.length) {
    const message = resolved.failures.map(item => item.error).join(" | ") ||
      "No onboarding rows were resolved for generation.";
    log_(runId, "ERROR", "SINGLE_DOCGEN_NO_TARGETS", {
      failures: resolved.failures
    });
    throw new Error(message);
  }

  const templateRow = findManualDocTemplateRow_(runId, settings.templateId);
  const outputRoot = resolveSingleDocumentOutputRoot_(runId, settings.outputFolder);
  const datedFolder = ensureSingleDocumentDateFolder_(runId, outputRoot);
  const results = resolved.failures.slice();

  resolved.targets.forEach(target => {
    const mainRow = target.mainRow;
    const onboardingId = String(getField_(mainRow, "ID") || "").trim();
    const nip = normalizeSingleDocumentNip_(getManualNipControl_(mainRow));

    try {
      const fileRow = buildSingleDocumentFileRow_(
        runId,
        mainRow,
        onboardingId,
        settings.templateId,
        datedFolder
      );
      const generated = generatePdfForAgreementFileRow_(runId, fileRow, mainRow, templateRow, {
        outputRootFolder: datedFolder
      });
      const item = {
        ok: true,
        nip: nip,
        onboardingId: onboardingId,
        matchedBy: target.matchedBy,
        fileName: generated.relativePath,
        pdfId: generated.pdfId,
        pdfUrl: generated.pdfUrl
      };
      results.push(item);
      log_(runId, "INFO", "SINGLE_DOCGEN_ITEM_DONE", item);
    } catch (e) {
      const item = {
        ok: false,
        nip: nip,
        onboardingId: onboardingId,
        matchedBy: target.matchedBy,
        error: e && e.message || String(e)
      };
      results.push(item);
      log_(runId, "ERROR", "SINGLE_DOCGEN_ITEM_FAILED", item);
    }
  });

  const failed = results.filter(item => !item.ok);
  const result = {
    ok: failed.length === 0,
    total: results.length,
    generated: results.length - failed.length,
    failed: failed.length,
    templateId: settings.templateId,
    outputRootFolderId: outputRoot.getId(),
    outputRootFolderName: outputRoot.getName(),
    datedFolderId: datedFolder.getId(),
    datedFolderName: datedFolder.getName(),
    results: results
  };

  log_(runId, "INFO", "SINGLE_DOCGEN_DONE", result);
  return result;
}

function getSingleDocumentBatchSettings_(options) {
  const rawNips = collectSingleDocumentValues_(
    options.NIP,
    options.NIPS_TEXT,
    options.NIPS,
    false
  );
  const nipEntries = rawNips.map(raw => ({
    raw: raw,
    normalized: normalizeSingleDocumentNip_(raw)
  }));
  const onboardingIds = collectSingleDocumentValues_(
    options.ONBOARDING_ID,
    options.ONBOARDING_IDS_TEXT,
    options.ONBOARDING_IDS,
    true
  );
  const cleanTemplateId = extractSingleDocumentDriveId_(options.TEMPLATE_ID);
  const cleanOutputFolder = String(options.OUTPUT_FOLDER || "").trim();

  const nips = nipEntries
    .filter(entry => /^\d{10}$/.test(entry.normalized))
    .map(entry => entry.normalized);
  const invalidNips = nipEntries
    .filter(entry => !/^\d{10}$/.test(entry.normalized))
    .map(entry => entry.raw);
  if (!rawNips.length && !onboardingIds.length) {
    throw new Error("Provide at least one NIP or Onboarding ID in SINGLE_DOCUMENT_GENERATION.");
  }
  if (!cleanTemplateId) {
    throw new Error("SINGLE_DOCUMENT_GENERATION.TEMPLATE_ID is blank or invalid.");
  }

  return {
    nips: uniqueSingleDocumentValues_(nips),
    invalidNips: uniqueSingleDocumentValues_(invalidNips),
    onboardingIds: uniqueSingleDocumentValues_(onboardingIds),
    templateId: cleanTemplateId,
    outputFolder: cleanOutputFolder
  };
}

function collectSingleDocumentValues_(singleValue, textValue, arrayValue, splitOnSpaces) {
  const values = [];
  const separator = splitOnSpaces ? /[\n,;\t ]+/ : /[\n,;\t]+/;

  [singleValue].concat(String(textValue || "").split(separator), arrayValue || [])
    .forEach(value => {
      const clean = String(value || "").trim();
      if (clean) values.push(clean);
    });
  return uniqueSingleDocumentValues_(values);
}

function uniqueSingleDocumentValues_(values) {
  const seen = {};
  return (values || []).filter(value => {
    const key = String(value || "").trim();
    if (!key || seen[key]) return false;
    seen[key] = true;
    return true;
  });
}

function resolveSingleDocumentMainRows_(runId, settings) {
  const targets = [];
  const failures = (settings.invalidNips || []).map(nip => ({
    ok: false,
    referenceType: "nip",
    referenceValue: nip,
    error: "Invalid NIP. Every NIP must contain exactly 10 digits."
  }));
  const seenRows = {};

  settings.nips.forEach(nip => {
    addSingleDocumentResolvedTarget_(runId, targets, failures, seenRows, "nip", nip, () => {
      return findSingleDocumentMainRowByNip_(runId, nip);
    });
  });

  settings.onboardingIds.forEach(onboardingId => {
    addSingleDocumentResolvedTarget_(runId, targets, failures, seenRows, "onboardingId", onboardingId, () => {
      return findSingleDocumentMainRowByOnboardingId_(runId, onboardingId);
    });
  });

  return { targets: targets, failures: failures };
}

function addSingleDocumentResolvedTarget_(runId, targets, failures, seenRows, type, value, resolver) {
  try {
    const mainRow = resolver();
    const onboardingId = String(getField_(mainRow, "ID") || "").trim();
    const nip = normalizeSingleDocumentNip_(getManualNipControl_(mainRow));
    const dedupeKey = onboardingId ? "id:" + onboardingId : "nip:" + nip;

    if (seenRows[dedupeKey]) {
      seenRows[dedupeKey].matchedBy.push(type + ":" + value);
      return;
    }

    const target = {
      mainRow: mainRow,
      matchedBy: [type + ":" + value]
    };
    seenRows[dedupeKey] = target;
    targets.push(target);
  } catch (e) {
    const failure = {
      ok: false,
      referenceType: type,
      referenceValue: value,
      error: e && e.message || String(e)
    };
    failures.push(failure);
    log_(runId, "ERROR", "SINGLE_DOCGEN_TARGET_NOT_RESOLVED", failure);
  }
}

function findSingleDocumentMainRowByNip_(runId, nip) {
  const sheetName = getSheetNameForDocgenTable_(DOCGEN_TABLES.MAIN);
  const values = fetchManualSpreadsheetSheetValues_(runId, sheetName);
  if (!values || values.length < 2) {
    throw new Error("Main onboarding sheet is empty: " + sheetName);
  }

  const headers = (values[0] || []).map(header => String(header || "").trim());
  const nipColumns = ["NIP_Control", "NIP Control", "nip", "NIP"]
    .map(name => findSingleDocumentHeaderIndex_(headers, name))
    .filter((index, position, indexes) => index >= 0 && indexes.indexOf(index) === position);

  if (!nipColumns.length) {
    throw new Error("No NIP column found in the main onboarding sheet.");
  }

  const matches = [];
  for (let r = 1; r < values.length; r++) {
    const matchesNip = nipColumns.some(columnIndex => {
      return normalizeSingleDocumentNip_(values[r][columnIndex]) === nip;
    });
    if (!matchesNip) continue;

    const row = {};
    headers.forEach((header, c) => {
      if (header) row[header] = values[r][c];
    });
    matches.push(row);
  }

  if (!matches.length) {
    throw new Error("Onboarding row not found for NIP: " + nip);
  }
  if (matches.length > 1) {
    throw new Error("More than one onboarding row found for NIP: " + nip + ". Refusing ambiguous generation.");
  }

  log_(runId, "INFO", "SINGLE_DOCGEN_MAIN_ROW_FOUND", {
    nip: nip,
    onboardingId: String(getField_(matches[0], "ID") || "").trim(),
    sheetName: sheetName
  });
  return matches[0];
}

function findSingleDocumentMainRowByOnboardingId_(runId, onboardingId) {
  const cleanId = String(onboardingId || "").trim();
  const sheetName = getSheetNameForDocgenTable_(DOCGEN_TABLES.MAIN);
  const values = fetchManualSpreadsheetSheetValues_(runId, sheetName);
  if (!values || values.length < 2) {
    throw new Error("Main onboarding sheet is empty: " + sheetName);
  }

  const matches = buildRowsFromValues_(values).filter(row => {
    return String(getField_(row, "ID") || "").trim() === cleanId;
  });
  if (!matches.length) {
    throw new Error("Onboarding row not found for ID: " + cleanId);
  }
  if (matches.length > 1) {
    throw new Error("More than one onboarding row found for ID: " + cleanId + ". Refusing ambiguous generation.");
  }

  log_(runId, "INFO", "SINGLE_DOCGEN_MAIN_ROW_FOUND", {
    nip: normalizeSingleDocumentNip_(getManualNipControl_(matches[0])),
    onboardingId: cleanId,
    matchedBy: "onboardingId",
    sheetName: sheetName
  });
  return matches[0];
}

function findSingleDocumentHeaderIndex_(headers, wantedName) {
  const wanted = normalizeKey_(wantedName);
  for (let i = 0; i < headers.length; i++) {
    if (normalizeKey_(headers[i]) === wanted) return i;
  }
  return -1;
}

function resolveSingleDocumentOutputRoot_(runId, outputFolder) {
  const configured = String(outputFolder || "").trim();
  const projectRoot = getDocGeneratorOutputRootFolder_();
  if (!projectRoot) {
    throw new Error("CONFIG.DOC_GENERATOR.OUTPUT_ROOT_FOLDER_ID is blank.");
  }

  if (!configured) {
    return logSingleDocumentOutputRoot_(
      runId,
      ensureChildFolder_(projectRoot, SINGLE_DOCUMENT_DEFAULT_OUTPUT_FOLDER),
      "default"
    );
  }

  const folderId = extractSingleDocumentDriveId_(configured);
  if (configured.indexOf("drive.google.com") >= 0) {
    try {
      return logSingleDocumentOutputRoot_(runId, DriveApp.getFolderById(folderId), "id_or_url");
    } catch (e) {
      throw new Error("Cannot open configured output folder: " + configured + ". " + (e && e.message || e));
    }
  }

  const cleanName = sanitizeDriveFileNamePart_(configured);
  if (!cleanName) throw new Error("SINGLE_DOCUMENT_GENERATION.OUTPUT_FOLDER is invalid.");

  const namedFolders = projectRoot.getFoldersByName(cleanName);
  if (namedFolders.hasNext()) {
    return logSingleDocumentOutputRoot_(runId, namedFolders.next(), "name");
  }

  if (folderId && /^[A-Za-z0-9_-]{20,}$/.test(configured)) {
    try {
      return logSingleDocumentOutputRoot_(runId, DriveApp.getFolderById(folderId), "id_or_url");
    } catch (e) {
      log_(runId, "WARN", "SINGLE_DOCGEN_OUTPUT_FOLDER_ID_FALLBACK_TO_NAME", {
        configuredValue: configured,
        error: e && e.message || String(e)
      });
    }
  }

  return logSingleDocumentOutputRoot_(runId, ensureChildFolder_(projectRoot, cleanName), "name");
}

function logSingleDocumentOutputRoot_(runId, folder, source) {
  log_(runId, "INFO", "SINGLE_DOCGEN_OUTPUT_ROOT_RESOLVED", {
    source: source,
    folderId: folder.getId(),
    folderName: folder.getName()
  });
  return folder;
}

function ensureSingleDocumentDateFolder_(runId, outputRoot) {
  const dateFolderName = Utilities.formatDate(
    new Date(),
    Session.getScriptTimeZone(),
    "yyyy-MM-dd"
  );
  const folder = ensureChildFolder_(outputRoot, dateFolderName);
  log_(runId, "INFO", "SINGLE_DOCGEN_DATE_FOLDER_RESOLVED", {
    outputRootFolderId: outputRoot.getId(),
    datedFolderId: folder.getId(),
    datedFolderName: dateFolderName
  });
  return folder;
}

function buildSingleDocumentFileRow_(runId, mainRow, onboardingId, templateId, outputFolder) {
  const pdfName = buildManualTemplatePdfFileName_(
    runId,
    mainRow,
    null,
    templateId
  );

  const entityKey = normalizeSingleDocumentNip_(getManualNipControl_(mainRow)) || onboardingId || "unknown";
  return {
    ID: "single-" + entityKey + "-" + templateId.slice(0, 8),
    Onboarding_ID: onboardingId,
    Template_ID_Reference: templateId,
    File_Name: String(pdfName || "").replace(/\.pdf$/i, ""),
    Prefix: "",
    File: pdfName,
    Single_Generation_Output_Folder_ID: outputFolder.getId()
  };
}

function normalizeSingleDocumentNip_(value) {
  return String(value == null ? "" : value).replace(/\D/g, "");
}

function extractSingleDocumentDriveId_(value) {
  const clean = String(value || "").trim();
  if (!clean) return "";
  const match = clean.match(/[-\w]{20,}/);
  return match ? match[0] : "";
}
