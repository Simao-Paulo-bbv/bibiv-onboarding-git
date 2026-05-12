/*******************************************************
 * Manual maintenance helpers for Docs Creator
 *******************************************************/

const MANUAL_TEMPLATE_PDF_GENERATION = {
  ONBOARDING_IDS_TEXT: "",
  ONBOARDING_IDS: [],
  OUTPUT_FOLDER_ID: "",
  TEMPLATE_DOC_ID: ""
};

function runManualGenerateTemplatePdfs() {
  const runId = makeRunId_();
  const settings = getManualTemplatePdfGenerationSettings_();
  const templateRow = buildFallbackTemplateRow_(runId, settings.templateDocId);
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
      const mainRow = findMainOnboardingRow_(runId, onboardingId);
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
  const templateFile = DriveApp.getFileById(templateDocId);
  const templateName = String(templateFile.getName() || "").trim() || "Template";
  const pdfName = buildManualTemplatePdfFileName_(mainRow, onboardingId, templateName);
  const relativePath = pdfName;

  log_(runId, "INFO", "MANUAL_TEMPLATE_PDF_FILE_ROW", {
    onboardingId: onboardingId,
    outputFolderId: outputFolder.getId(),
    fileName: pdfName
  });

  return {
    ID: "manual-" + onboardingId + "-" + templateDocId.slice(0, 8),
    Onboarding_ID: onboardingId,
    Template_ID_Reference: String(templateDocId || "").trim(),
    File: relativePath
  };
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

function buildManualTemplatePdfFileName_(mainRow, onboardingId, templateName) {
  const companyName = sanitizeDriveFileNamePart_(
    getField_(mainRow, "Company_name") ||
    getField_(mainRow, "Company Name") ||
    getField_(mainRow, "Nazwa firmy") ||
    onboardingId
  );
  const templatePart = sanitizeDriveFileNamePart_(templateName.replace(/\.gdoc$/i, "").replace(/\.docx$/i, ""));
  return [templatePart, companyName, onboardingId].filter(Boolean).join("__") + ".pdf";
}

function sanitizeDriveFileNamePart_(value) {
  return String(value || "")
    .trim()
    .replace(/[\\/:*?"<>|#%]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\.+$/g, "")
    .trim();
}
