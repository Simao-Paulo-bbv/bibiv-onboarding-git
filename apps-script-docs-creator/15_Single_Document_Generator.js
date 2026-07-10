/*******************************************************
 * Independent single-document generation by NIP
 *
 * Fill the three settings below and run:
 *   runGenerateSingleDocumentForNip
 *
 * OUTPUT_FOLDER may be blank (default), a folder name,
 * a Google Drive folder ID, or a Google Drive folder URL.
 *******************************************************/

const SINGLE_DOCUMENT_GENERATION = {
  NIP: "",
  TEMPLATE_ID: "",
  OUTPUT_FOLDER: ""
};

const SINGLE_DOCUMENT_DEFAULT_OUTPUT_FOLDER = "Files_Single_Generations_";

function runGenerateSingleDocumentForNip() {
  return generateSingleDocumentForNip(
    SINGLE_DOCUMENT_GENERATION.NIP,
    SINGLE_DOCUMENT_GENERATION.TEMPLATE_ID,
    SINGLE_DOCUMENT_GENERATION.OUTPUT_FOLDER
  );
}

function generateSingleDocumentForNip(nip, templateId, outputFolder) {
  const runId = makeRunId_();
  const settings = getSingleDocumentGenerationSettings_(nip, templateId, outputFolder);

  log_(runId, "INFO", "SINGLE_DOCGEN_START", {
    nip: settings.nip,
    templateId: settings.templateId,
    outputFolder: settings.outputFolder || SINGLE_DOCUMENT_DEFAULT_OUTPUT_FOLDER
  });

  const mainRow = findSingleDocumentMainRowByNip_(runId, settings.nip);
  const onboardingId = String(getField_(mainRow, "ID") || "").trim();
  const templateRow = findManualDocTemplateRow_(runId, settings.templateId);
  const outputRoot = resolveSingleDocumentOutputRoot_(runId, settings.outputFolder);
  const datedFolder = ensureSingleDocumentDateFolder_(runId, outputRoot);
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

  const result = {
    ok: true,
    nip: settings.nip,
    onboardingId: onboardingId,
    templateId: settings.templateId,
    outputRootFolderId: outputRoot.getId(),
    outputRootFolderName: outputRoot.getName(),
    datedFolderId: datedFolder.getId(),
    datedFolderName: datedFolder.getName(),
    fileName: generated.relativePath,
    pdfId: generated.pdfId,
    pdfUrl: generated.pdfUrl
  };

  log_(runId, "INFO", "SINGLE_DOCGEN_DONE", result);
  return result;
}

function getSingleDocumentGenerationSettings_(nip, templateId, outputFolder) {
  const cleanNip = normalizeSingleDocumentNip_(nip);
  const cleanTemplateId = extractSingleDocumentDriveId_(templateId);
  const cleanOutputFolder = String(outputFolder || "").trim();

  if (!/^\d{10}$/.test(cleanNip)) {
    throw new Error("SINGLE_DOCUMENT_GENERATION.NIP must contain exactly 10 digits.");
  }
  if (!cleanTemplateId) {
    throw new Error("SINGLE_DOCUMENT_GENERATION.TEMPLATE_ID is blank or invalid.");
  }

  return {
    nip: cleanNip,
    templateId: cleanTemplateId,
    outputFolder: cleanOutputFolder
  };
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

  return {
    ID: "single-" + normalizeSingleDocumentNip_(getManualNipControl_(mainRow)) + "-" + templateId.slice(0, 8),
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
