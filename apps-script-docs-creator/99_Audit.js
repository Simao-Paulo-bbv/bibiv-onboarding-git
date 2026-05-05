function auditAgreementTemplatePlaceholders() {
  const templateIds = [
    "1GnZM7kU2l2CzWBnSI2Z1VCL03VZarh6cO0_vxQVVLTU",
    "1KEUT-51DRypLNgXwSX7-eRkdZQvyRaEx2CGj05hi21s",
    "1AR2NkizWPcazbQFAAj9W89m4ZBnQS_wv2VIoYoLIkBg",
    "1aruVKM0vlyvcMfOFkZsKLL-RqsBxyMJqvwYKchkgH0Q",
    "1r8hZukGAlg7V0O8mk2v3SdTfgu1CL1PpMrv2MpQ8dBc"
  ];

  const result = templateIds.map(id => {
    const doc = DocumentApp.openById(id);
    const text = getPrimaryDocumentSections_(doc).map(section => section.getText()).join("\n");
    const placeholders = uniqueMatches_(text, /<<[\s\S]*?>>/g);
    return {
      id: id,
      name: DriveApp.getFileById(id).getName(),
      placeholders: placeholders
    };
  });

  console.log(JSON.stringify(result, null, 2));
  return result;
}

function uniqueMatches_(text, pattern) {
  const seen = {};
  const out = [];
  const matches = String(text || "").match(pattern) || [];
  matches.forEach(match => {
    const clean = match.replace(/\s+/g, " ").trim();
    if (seen[clean]) return;
    seen[clean] = true;
    out.push(clean);
  });
  return out.sort();
}

function authorizeDocGenerator() {
  const runId = makeRunId_();
  const result = {
    ok: true,
    projectRootFolderId: CONFIG.DOC_GENERATOR.OUTPUT_ROOT_FOLDER_ID,
    dataSpreadsheetId: CONFIG.DOC_GENERATOR.DATA_SPREADSHEET_ID,
    queueSize: readDocGenerationQueue_(PropertiesService.getScriptProperties()).length,
    triggers: ScriptApp.getProjectTriggers().map(trigger => ({
      handler: trigger.getHandlerFunction(),
      eventType: String(trigger.getEventType())
    })),
    sheets: {}
  };

  const root = DriveApp.getFolderById(CONFIG.DOC_GENERATOR.OUTPUT_ROOT_FOLDER_ID);
  result.projectRootFolderName = root.getName();

  const spreadsheet = SpreadsheetApp.openById(CONFIG.DOC_GENERATOR.DATA_SPREADSHEET_ID);
  result.dataSpreadsheetName = spreadsheet.getName();

  Object.keys(CONFIG.DOC_GENERATOR.TABLE_SHEET_NAMES || {}).forEach(tableName => {
    const sheetName = CONFIG.DOC_GENERATOR.TABLE_SHEET_NAMES[tableName];
    const sheet = spreadsheet.getSheetByName(sheetName);
    result.sheets[tableName] = sheet ? {
      sheetName: sheetName,
      lastRow: sheet.getLastRow(),
      lastColumn: sheet.getLastColumn()
    } : {
      sheetName: sheetName,
      missing: true
    };
  });

  log_(runId, "INFO", "DOCGEN_AUTH_CHECK", result);
  return result;
}

function debugDocGenerationState(onboardingId, jobId, agreementFileId) {
  const runId = makeRunId_();
  const args = {
    onboardingId: String(onboardingId || "").trim(),
    jobId: String(jobId || "").trim(),
    agreementFileId: String(agreementFileId || "").trim()
  };
  const queue = readDocGenerationQueue_(PropertiesService.getScriptProperties());
  const pendingFiles = (args.onboardingId || args.jobId || args.agreementFileId)
    ? findPendingAgreementFileRows_(runId, args)
    : [];

  const result = {
    ok: true,
    args: args,
    queueSize: queue.length,
    queue: queue,
    triggers: ScriptApp.getProjectTriggers().map(trigger => ({
      handler: trigger.getHandlerFunction(),
      eventType: String(trigger.getEventType())
    })),
    pendingFiles: pendingFiles.map(row => ({
      id: getField_(row, "ID"),
      jobId: getField_(row, "Job_ID"),
      onboardingId: getField_(row, "Onboarding_ID"),
      status: getField_(row, "File_status"),
      category: getField_(row, "Category"),
      templateId: getField_(row, "Template_ID_Reference"),
      file: getField_(row, "File")
    }))
  };

  log_(runId, "INFO", "DOCGEN_DEBUG_STATE", result);
  return result;
}
