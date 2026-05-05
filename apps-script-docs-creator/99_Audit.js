function auditAgreementTemplatePlaceholders() {
  const runId = makeRunId_();
  const templateRows = findAgreementTemplateRowsForAudit_(runId);

  const result = templateRows.map(row => {
    const id = String(getField_(row, "Template_ID") || "").trim();
    const doc = DocumentApp.openById(id);
    const text = getPrimaryDocumentSections_(doc).map(section => section.getText()).join("\n");
    const placeholders = uniqueMatches_(text, /<<[\s\S]*?>>/g);
    return {
      id: id,
      category: getField_(row, "Category"),
      prefix: getField_(row, "File_Name_Prefix"),
      name: DriveApp.getFileById(id).getName(),
      placeholders: placeholders
    };
  });

  console.log(JSON.stringify(result, null, 2));
  return result;
}

function findAgreementTemplateRowsForAudit_(runId) {
  const rows = findRowsFromSheet_(runId, DOCGEN_TABLES.DOC_TEMPLATES, row => {
    const id = String(getField_(row, "Template_ID") || "").trim();
    const category = String(getField_(row, "Category") || "").trim();
    const active = String(getField_(row, "Is_Active") || "").trim().toLowerCase();
    const isActive = active === "" || active === "true" || active === "yes" || active === "y" || active === "1";
    return id && category === CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY && isActive;
  });

  if (rows) return rows;

  const selector = 'FILTER("' + DOCGEN_TABLES.DOC_TEMPLATES + '", AND(' +
    "[Category] = " + appSheetQuote_(CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY) +
    "))";
  return callAppSheetFind_(runId, DOCGEN_TABLES.DOC_TEMPLATES, selector).filter(row => {
    const active = String(getField_(row, "Is_Active") || "").trim().toLowerCase();
    return active === "" || active === "true" || active === "yes" || active === "y" || active === "1";
  });
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

  if (!CONFIG.DOC_GENERATOR.USE_SHEET_READS) {
    result.sheetReadsEnabled = false;
    log_(runId, "INFO", "DOCGEN_AUTH_CHECK", result);
    return result;
  }

  try {
    const metadata = fetchDocGeneratorSpreadsheetMetadata_(runId);
    result.dataSpreadsheetName = metadata && metadata.properties && metadata.properties.title || "";
    const availableSheets = {};
    (metadata && metadata.sheets || []).forEach(sheet => {
      const title = sheet && sheet.properties && sheet.properties.title;
      if (title) availableSheets[title] = true;
    });

    Object.keys(CONFIG.DOC_GENERATOR.TABLE_SHEET_NAMES || {}).forEach(tableName => {
      const sheetName = CONFIG.DOC_GENERATOR.TABLE_SHEET_NAMES[tableName];
      if (!availableSheets[sheetName]) {
        result.sheets[tableName] = {
          sheetName: sheetName,
          missing: true
        };
        return;
      }

      const values = fetchDocGeneratorSheetValues_(runId, sheetName);
      result.sheets[tableName] = {
        sheetName: sheetName,
        rowsRead: values.length,
        columnsRead: values.length ? values[0].length : 0
      };
    });
  } catch (e) {
    result.sheetReadsEnabled = true;
    result.sheetReadError = String(e && e.message || e).slice(0, 900);
  }

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
