/*******************************************************
 * BIBIV_Onboarding_DocsCreator
 *
 * Standalone Apps Script entrypoint for AppSheet:
 *   generateAgreementFilesFromAppSheet(onboardingId, jobId, agreementFileId)
 *******************************************************/

const DOCGEN_TABLES = {
  MAIN: "BIBIV_onboarding_APP",
  AGREEMENTS_FILES: "Agreements_Files",
  DOC_TEMPLATES: "Doc_Templates",
  GENERATION_JOB_ITEMS: "Generation_Job_Items",
  SIGNED_DOCUMENTS: "Signed_Documents"
};

const DOCGEN_RUNTIME_CACHE = {
  projectRootFolder: null,
  folderByPath: {},
  sheetValuesByName: {},
  mainRowsById: {},
  templateRowsById: {},
  docsApiPlaceholderReplacementAvailable: true
};

function generateAgreementFilesFromAppSheet(onboardingId, jobId, agreementFileId) {
  const runId = makeRunId_();
  const args = {
    onboardingId: String(onboardingId || "").trim(),
    jobId: String(jobId || "").trim(),
    agreementFileId: String(agreementFileId || "").trim()
  };

  const enqueueResult = enqueueDocGenerationJob_(runId, args);
  ensureDocGenerationQueueTrigger({ refresh: Boolean(enqueueResult && enqueueResult.added) });
  log_(runId, "INFO", "DOCGEN_ENQUEUED", Object.assign({}, args, {
    added: Boolean(enqueueResult && enqueueResult.added)
  }));
  return { ok: true, queued: true, added: Boolean(enqueueResult && enqueueResult.added), jobId: args.jobId };
}

function processNextQueuedDocGenerationJob() {
  return processQueuedDocGenerationJobs_({});
}

function processQueuedDocGenerationJobs_(options) {
  options = options || {};
  const runId = makeRunId_();
  const workerStartedAt = Date.now();
  const globalLock = tryAcquireGlobalGenerationLock_(runId);
  if (!globalLock) {
    log_(runId, "INFO", "DOCGEN_WORKER_BUSY", {});
    return { ok: true, busy: true };
  }

  let claimKey = "";
  const workerResults = [];
  try {
    while (true) {
      const args = dequeueDocGenerationJob_(runId);
      if (!args) {
        log_(runId, "INFO", "DOCGEN_QUEUE_EMPTY", {
          processedJobs: workerResults.length,
          elapsedMs: Date.now() - workerStartedAt
        });
        deleteTriggersForHandler_("processNextQueuedDocGenerationJob");
        return { ok: true, empty: true, results: workerResults };
      }

      claimKey = claimDocGenerationJob_(runId, args);
      if (!claimKey) {
        enqueueDocGenerationJob_(runId, args, { front: true, continuation: true });
        ensureDocGenerationQueueTrigger({ refresh: true });
        log_(runId, "INFO", "DOCGEN_ALREADY_RUNNING_REQUEUED", {
          jobId: args.jobId || "",
          onboardingId: args.onboardingId || "",
          agreementFileId: args.agreementFileId || ""
        });
        return {
          ok: true,
          alreadyRunning: true,
          queued: true,
          processedJobs: workerResults.length,
          results: workerResults
        };
      }
      let result;
      try {
        result = processDocGenerationJob_(runId, args, workerStartedAt, options);
        workerResults.push(result);
      } catch (err) {
        requeueDocGenerationJobAfterFailure_(runId, args, err);
        ensureDocGenerationQueueTrigger({ refresh: true });
        throw err;
      } finally {
        releaseDocGenerationJob_(claimKey);
        claimKey = "";
      }

      if (!queueHasItems_()) {
        deleteTriggersForHandler_("processNextQueuedDocGenerationJob");
        return {
          ok: workerResults.every(item => item && item.ok),
          processedJobs: workerResults.length,
          results: workerResults
        };
      }

      if (shouldYieldDocGenerationWorker_(workerStartedAt, options)) {
        ensureDocGenerationQueueTrigger({ refresh: true });
        log_(runId, "INFO", "DOCGEN_WORKER_YIELDING", {
          processedJobs: workerResults.length,
          elapsedMs: Date.now() - workerStartedAt,
          nextTriggerRefreshed: true
        });
        return {
          ok: workerResults.every(item => item && item.ok),
          queued: true,
          processedJobs: workerResults.length,
          results: workerResults
        };
      }
    }
  } finally {
    releaseDocGenerationJob_(claimKey);
    releaseGlobalGenerationLock_(globalLock);
  }
}

function processDocGenerationJob_(runId, args, workerStartedAt, options) {
  options = options || {};
  const jobStartedAt = Date.now();
  log_(runId, "INFO", "DOCGEN_START", args);

  const files = findPendingAgreementFileRows_(runId, args);
  if (!files.length) {
    log_(runId, "INFO", "DOCGEN_NO_PENDING_FILES", args);
    return { ok: true, processed: 0, message: "No pending Agreement files." };
  }

  const configuredMaxFiles = options.maxFilesPerRun || CONFIG.DOC_GENERATOR.MAX_FILES_PER_RUN || 3;
  const maxFiles = Math.max(1, Number(configuredMaxFiles));
  const filesToProcess = selectFilesForThisPass_(runId, files, maxFiles, workerStartedAt, options);
  const shouldContinue = files.length > filesToProcess.length;
  const mainRowsById = {};
  const templateRowsById = preloadTemplateRowsForFiles_(runId, filesToProcess);
  const results = [];
  const readyFileUpdates = [];
  const readyJobItemUpdates = [];

  filesToProcess.forEach(fileRow => {
    const fileId = getField_(fileRow, "ID");
    try {
      const fileOnboardingId = getField_(fileRow, "Onboarding_ID") || args.onboardingId;
      const templateId = getField_(fileRow, "Template_ID_Reference");

      if (!fileOnboardingId) throw new Error("Missing Onboarding_ID.");
      if (!templateId) throw new Error("Missing Template_ID_Reference.");

      if (!mainRowsById[fileOnboardingId]) {
        mainRowsById[fileOnboardingId] = findMainOnboardingRow_(runId, fileOnboardingId);
      }

      const templateRow = templateRowsById[templateId] || buildFallbackTemplateRow_(runId, templateId);

      let generated = getGeneratedArtifactFromExistingRow_(fileRow);
      if (!generated) {
        markAgreementFileGenerating_(runId, fileRow);
        generated = generatePdfForAgreementFileRow_(
          runId,
          fileRow,
          mainRowsById[fileOnboardingId],
          templateRow
        );
        markAgreementFileGenerated_(runId, buildAgreementFileGeneratedRow_(fileRow, generated));
      } else {
        log_(runId, "INFO", "DOCGEN_FILE_ALREADY_GENERATED_FINALIZING", {
          agreementFileId: fileId,
          file: generated.relativePath
        });
      }

      readyFileUpdates.push(buildAgreementFileReadyRow_(fileRow, generated));
      readyJobItemUpdates.push(buildGenerationJobItemCreatedRow_(fileRow));

      results.push({
        id: fileId,
        ok: true,
        file: generated.relativePath,
        pdfId: generated.pdfId,
        docId: generated.docId
      });
    } catch (err) {
      markAgreementFileFailed_(runId, fileRow, err);
      results.push({ id: fileId || "", ok: false, error: String(err && err.message || err) });
    }
  });

  createSignedDocumentUploadRowsForReadyAgreements_(runId, filesToProcess, readyFileUpdates);
  batchMarkAgreementFilesReady_(runId, readyFileUpdates);
  batchMarkGenerationJobItemsCreated_(runId, readyJobItemUpdates);
  if (!shouldContinue) {
    finishMainRowsWhenAllAgreementsReady_(runId, filesToProcess, args, readyFileUpdates);
  } else {
    log_(runId, "INFO", "DOCGEN_FINALIZATION_DEFERRED", {
      jobId: args.jobId || "",
      onboardingId: args.onboardingId || "",
      remainingAtStart: files.length - filesToProcess.length
    });
  }

  if (shouldContinue) {
    enqueueDocGenerationJob_(runId, args, { front: true, continuation: true });
    ensureDocGenerationQueueTrigger();
    log_(runId, "INFO", "DOCGEN_CONTINUATION_ENQUEUED", {
      jobId: args.jobId || "",
      onboardingId: args.onboardingId || "",
      processedThisRun: filesToProcess.length,
      pendingAtStart: files.length
    });
  }

  const failed = results.filter(r => !r.ok);
  log_(runId, failed.length ? "ERROR" : "INFO", "DOCGEN_DONE", {
    processed: results.length,
    failed: failed.length,
    continuation: shouldContinue,
    durationMs: Date.now() - jobStartedAt
  });

  return {
    ok: failed.length === 0,
    processed: results.length,
    failed: failed.length,
    continuation: shouldContinue,
    results: results
  };
}

function selectFilesForThisPass_(runId, files, maxFiles, workerStartedAt, options) {
  const selected = [];
  const limit = Math.max(1, Number(maxFiles || 1));
  for (let i = 0; i < files.length && selected.length < limit; i++) {
    if (selected.length > 0 && shouldYieldDocGenerationWorker_(workerStartedAt, options)) {
      log_(runId, "INFO", "DOCGEN_CHUNK_TIME_BUDGET_REACHED", {
        selected: selected.length,
        pendingAtStart: files.length
      });
      break;
    }
    selected.push(files[i]);
  }
  return selected;
}

function shouldYieldDocGenerationWorker_(startedAt, options) {
  options = options || {};
  const maxRuntime = Number(options.maxWorkerRuntimeMs || CONFIG.DOC_GENERATOR.MAX_WORKER_RUNTIME_MS || 300000);
  const minRemaining = Number(options.minRemainingMs || CONFIG.DOC_GENERATOR.MIN_REMAINING_MS_FOR_NEXT_CHUNK || 90000);
  return Date.now() - Number(startedAt || Date.now()) >= Math.max(1, maxRuntime - minRemaining);
}

function tryAcquireGlobalGenerationLock_(runId) {
  if (!CONFIG.DOC_GENERATOR.SERIALIZE_JOBS) return null;
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(CONFIG.GLOBAL_GENERATION_LOCK_WAIT_MS || 1000);
    log_(runId, "INFO", "DOCGEN_GLOBAL_LOCK_ACQUIRED", {});
    return lock;
  } catch (e) {
    return null;
  }
}

function releaseGlobalGenerationLock_(lock) {
  if (!lock) return;
  try {
    lock.releaseLock();
  } catch (e) {
    // best effort
  }
}

function claimDocGenerationJob_(runId, args) {
  const key = "DOCGEN_CLAIM_" + (args.jobId || args.agreementFileId || args.onboardingId || "unknown");
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const cache = CacheService.getScriptCache();
    if (cache.get(key)) {
      log_(runId, "INFO", "DOCGEN_ALREADY_RUNNING", { key: key });
      return "";
    }
    cache.put(key, "1", CONFIG.DOC_GENERATOR.JOB_CLAIM_TTL_SECONDS || 900);
    return key;
  } finally {
    lock.releaseLock();
  }
}

function releaseDocGenerationJob_(key) {
  if (!key) return;
  try {
    CacheService.getScriptCache().remove(key);
  } catch (e) {
    // best effort
  }
}

function enqueueDocGenerationJob_(runId, args, options) {
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readDocGenerationQueue_(props);
    const key = args.jobId || args.agreementFileId || args.onboardingId;
    if (!key) throw new Error("Cannot enqueue doc generation without jobId/onboardingId/agreementFileId.");

    const exists = queue.some(item => (item.jobId || item.agreementFileId || item.onboardingId) === key);
    let added = false;
    if (!exists) {
      const item = {
        onboardingId: args.onboardingId,
        jobId: args.jobId,
        agreementFileId: args.agreementFileId,
        queuedAt: new Date().toISOString(),
        continuation: Boolean(options && options.continuation)
      };
      if (options && options.front) {
        queue.unshift(item);
      } else {
        queue.push(item);
      }
      writeDocGenerationQueue_(props, queue);
      added = true;
    }
    log_(runId, "INFO", "DOCGEN_QUEUE_STATE", { size: queue.length, added: added });
    return { added: added, size: queue.length };
  } finally {
    lock.releaseLock();
  }
}

function dequeueDocGenerationJob_(runId) {
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readDocGenerationQueue_(props);
    const next = queue.shift();
    writeDocGenerationQueue_(props, queue);
    log_(runId, "INFO", "DOCGEN_DEQUEUE", { remaining: queue.length, next: next || null });
    return next || null;
  } finally {
    lock.releaseLock();
  }
}

function queueHasItems_() {
  const queue = readDocGenerationQueue_(PropertiesService.getScriptProperties());
  return queue.length > 0;
}

function readDocGenerationQueue_(props) {
  return safeJsonParse_(props.getProperty(CONFIG.DOC_GENERATOR.QUEUE_KEY)) || [];
}

function writeDocGenerationQueue_(props, queue) {
  props.setProperty(CONFIG.DOC_GENERATOR.QUEUE_KEY, JSON.stringify(queue || []));
}

function requeueDocGenerationJobAfterFailure_(runId, args, err) {
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readDocGenerationQueue_(props);
    const key = args.jobId || args.agreementFileId || args.onboardingId;
    const retryCount = Number(args.retryCount || 0) + 1;
    const maxRetries = Number(CONFIG.DOC_GENERATOR.JOB_REQUEUE_MAX_RETRIES || 3);
    if (retryCount > maxRetries) {
      log_(runId, "ERROR", "DOCGEN_JOB_REQUEUE_LIMIT_REACHED", {
        jobId: args.jobId || "",
        onboardingId: args.onboardingId || "",
        retryCount: retryCount,
        error: String(err && err.message || err).slice(0, 900)
      });
      return;
    }
    const exists = queue.some(item => (item.jobId || item.agreementFileId || item.onboardingId) === key);
    if (!exists) {
      queue.unshift({
        onboardingId: args.onboardingId,
        jobId: args.jobId,
        agreementFileId: args.agreementFileId,
        queuedAt: new Date().toISOString(),
        retryCount: retryCount,
        retryAfterError: String(err && err.message || err).slice(0, 300)
      });
      writeDocGenerationQueue_(props, queue);
    }
    log_(runId, "ERROR", "DOCGEN_JOB_REQUEUED_AFTER_ERROR", {
      jobId: args.jobId || "",
      onboardingId: args.onboardingId || "",
      retryCount: retryCount,
      error: String(err && err.message || err).slice(0, 900)
    });
  } catch (requeueErr) {
    log_(runId, "ERROR", "DOCGEN_JOB_REQUEUE_FAILED", {
      jobId: args && args.jobId || "",
      error: String(requeueErr && requeueErr.message || requeueErr).slice(0, 900)
    });
  }
}

function ensureDocGenerationQueueTrigger(options) {
  options = options || {};
  const handler = "processNextQueuedDocGenerationJob";
  const afterMs = Number(CONFIG.DOC_GENERATOR.QUEUE_TRIGGER_AFTER_MS || 0);
  if (options.refresh && afterMs > 0) {
    deleteTriggersForHandler_(handler);
  } else {
    const exists = ScriptApp.getProjectTriggers().some(trigger => trigger.getHandlerFunction() === handler);
    if (exists) return;
  }

  const builder = ScriptApp.newTrigger(handler).timeBased();
  if (afterMs > 0) {
    builder.after(afterMs).create();
    return;
  }
  builder.everyMinutes(CONFIG.DOC_GENERATOR.QUEUE_TRIGGER_MINUTES || 1).create();
}

function deleteTriggersForHandler_(handler) {
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (trigger.getHandlerFunction() === handler) ScriptApp.deleteTrigger(trigger);
  });
}

function findPendingAgreementFileRows_(runId, args) {
  const sheetRows = findPendingAgreementFileRowsFromSheet_(runId, args);
  if (sheetRows && sheetRows.length) return sheetRows;

  const parts = [
    "IN([File_status], LIST(" +
      appSheetQuote_(CONFIG.DOC_GENERATOR.FILE_STATUS_SET_UP) + ", " +
      appSheetQuote_(CONFIG.DOC_GENERATOR.FILE_STATUS_GENERATING) + ", " +
      appSheetQuote_(CONFIG.DOC_GENERATOR.FILE_STATUS_GENERATED) +
    "))",
    "[Category] = " + appSheetQuote_(CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY)
  ];

  if (args.jobId) {
    parts.push("[Job_ID] = " + appSheetQuote_(args.jobId));
  } else if (args.onboardingId) {
    parts.push("[Onboarding_ID] = " + appSheetQuote_(args.onboardingId));
  } else if (args.agreementFileId) {
    parts.push("[ID] = " + appSheetQuote_(args.agreementFileId));
  } else {
    throw new Error("Pass at least Job_ID, Onboarding_ID, or Agreements_Files ID.");
  }

  const selector = 'FILTER("' + DOCGEN_TABLES.AGREEMENTS_FILES + '", AND(' + parts.join(", ") + "))";
  return callAppSheetFind_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, selector);
}

function preloadTemplateRowsForFiles_(runId, files) {
  const ids = [];
  const seen = {};
  files.forEach(fileRow => {
    const id = String(getField_(fileRow, "Template_ID_Reference") || "").trim();
    if (!id || seen[id] || DOCGEN_RUNTIME_CACHE.templateRowsById[id]) return;
    seen[id] = true;
    ids.push(id);
  });

  if (!ids.length) return Object.assign({}, DOCGEN_RUNTIME_CACHE.templateRowsById);

  const sheetRows = findRowsByValuesFromSheet_(runId, DOCGEN_TABLES.DOC_TEMPLATES, "Template_ID", ids);
  if (sheetRows) {
    const fromSheet = {};
    sheetRows.forEach(row => {
      const id = getField_(row, "Template_ID");
      if (id) {
        fromSheet[id] = row;
        DOCGEN_RUNTIME_CACHE.templateRowsById[id] = row;
      }
    });
    return Object.assign({}, DOCGEN_RUNTIME_CACHE.templateRowsById);
  }

  const selector = 'FILTER("' + DOCGEN_TABLES.DOC_TEMPLATES + '", OR(' +
    ids.map(id => "[Template_ID] = " + appSheetQuote_(id)).join(", ") +
    "))";
  const rows = callAppSheetFind_(runId, DOCGEN_TABLES.DOC_TEMPLATES, selector);
  const out = {};
  rows.forEach(row => {
    const id = getField_(row, "Template_ID");
    if (id) {
      out[id] = row;
      DOCGEN_RUNTIME_CACHE.templateRowsById[id] = row;
    }
  });
  return Object.assign({}, DOCGEN_RUNTIME_CACHE.templateRowsById);
}

function findMainOnboardingRow_(runId, onboardingId) {
  const cleanId = String(onboardingId || "").trim();
  if (DOCGEN_RUNTIME_CACHE.mainRowsById[cleanId]) {
    return DOCGEN_RUNTIME_CACHE.mainRowsById[cleanId];
  }

  const sheetRows = findRowsByValuesFromSheet_(runId, DOCGEN_TABLES.MAIN, "ID", [cleanId]);
  if (sheetRows && sheetRows.length) {
    DOCGEN_RUNTIME_CACHE.mainRowsById[cleanId] = sheetRows[0];
    return sheetRows[0];
  }

  const row = findRequiredSingleRow_(
    runId,
    DOCGEN_TABLES.MAIN,
    "[ID] = " + appSheetQuote_(cleanId),
    "main onboarding row " + cleanId
  );
  DOCGEN_RUNTIME_CACHE.mainRowsById[cleanId] = row;
  return row;
}

function findPendingAgreementFileRowsFromSheet_(runId, args) {
  const allowedStatuses = {};
  [
    CONFIG.DOC_GENERATOR.FILE_STATUS_SET_UP,
    CONFIG.DOC_GENERATOR.FILE_STATUS_GENERATING,
    CONFIG.DOC_GENERATOR.FILE_STATUS_GENERATED
  ].forEach(status => {
    allowedStatuses[String(status || "").trim()] = true;
  });

  return findRowsFromSheet_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, row => {
    const status = String(getField_(row, "File_status") || "").trim();
    const category = String(getField_(row, "Category") || "").trim();
    if (!allowedStatuses[status] || category !== CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY) return false;

    if (args.jobId) return String(getField_(row, "Job_ID") || "").trim() === args.jobId;
    if (args.onboardingId) return String(getField_(row, "Onboarding_ID") || "").trim() === args.onboardingId;
    if (args.agreementFileId) return String(getField_(row, "ID") || "").trim() === args.agreementFileId;
    throw new Error("Pass at least Job_ID, Onboarding_ID, or Agreements_Files ID.");
  });
}

function findRowsByValuesFromSheet_(runId, tableName, keyColumn, values) {
  const wanted = {};
  (values || []).forEach(value => {
    const clean = String(value || "").trim();
    if (clean) wanted[clean] = true;
  });
  if (!Object.keys(wanted).length) return [];

  return findRowsFromSheet_(runId, tableName, row => wanted[String(getField_(row, keyColumn) || "").trim()]);
}

function findRowsFromSheet_(runId, tableName, predicate) {
  if (!CONFIG.DOC_GENERATOR.USE_SHEET_READS) return null;

  const sheetName = getSheetNameForDocgenTable_(tableName);
  if (!sheetName) return null;

  try {
    const values = fetchDocGeneratorSheetValues_(runId, sheetName);
    if (values.length < 2) {
      log_(runId, "INFO", "DOCGEN_SHEET_FIND", { tableName: tableName, sheetName: sheetName, rows: 0 });
      return [];
    }

    const headers = values[0].map(header => String(header || "").trim());
    const rows = [];
    for (let r = 1; r < values.length; r++) {
      const row = {};
      headers.forEach((header, c) => {
        if (header) row[header] = values[r][c];
      });
      if (!predicate || predicate(row)) rows.push(row);
    }

    log_(runId, "INFO", "DOCGEN_SHEET_FIND", {
      tableName: tableName,
      sheetName: sheetName,
      rows: rows.length
    });
    return rows;
  } catch (e) {
    log_(runId, "WARN", "DOCGEN_SHEET_FIND_FALLBACK", {
      tableName: tableName,
      sheetName: sheetName,
      error: String(e && e.message || e).slice(0, 900)
    });
    return null;
  }
}

function getSheetNameForDocgenTable_(tableName) {
  const map = CONFIG.DOC_GENERATOR.TABLE_SHEET_NAMES || {};
  return String(map[tableName] || tableName || "").trim();
}

function fetchDocGeneratorSheetValues_(runId, sheetName) {
  const cleanSheetName = String(sheetName || "").trim();
  if (!cleanSheetName) return [];
  if (DOCGEN_RUNTIME_CACHE.sheetValuesByName[cleanSheetName]) {
    return DOCGEN_RUNTIME_CACHE.sheetValuesByName[cleanSheetName];
  }

  const id = String(CONFIG.DOC_GENERATOR.DATA_SPREADSHEET_ID || "").trim();
  if (!id) return [];

  const spreadsheet = SpreadsheetApp.openById(id);
  const sheet = spreadsheet.getSheetByName(cleanSheetName);
  if (!sheet) throw new Error("Sheet not found: " + cleanSheetName);

  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();
  const values = lastRow && lastColumn
    ? sheet.getRange(1, 1, lastRow, lastColumn).getValues()
    : [];
  DOCGEN_RUNTIME_CACHE.sheetValuesByName[cleanSheetName] = values;
  log_(runId, "INFO", "DOCGEN_SPREADSHEET_READ", {
    sheetName: cleanSheetName,
    rows: values.length,
    cols: lastColumn
  });
  return values;
}

function fetchDocGeneratorSpreadsheetMetadata_(runId) {
  const id = String(CONFIG.DOC_GENERATOR.DATA_SPREADSHEET_ID || "").trim();
  if (!id) return null;
  const spreadsheet = SpreadsheetApp.openById(id);
  const sheets = spreadsheet.getSheets().map(sheet => ({
    properties: { title: sheet.getName() }
  }));
  const parsed = {
    properties: { title: spreadsheet.getName() },
    sheets: sheets
  };
  log_(runId, "INFO", "DOCGEN_SPREADSHEET_METADATA_READ", {
    title: parsed.properties.title,
    sheets: sheets.length
  });
  return parsed;
}

function quoteSheetNameForA1_(sheetName) {
  return "'" + String(sheetName || "").replace(/'/g, "''") + "'";
}

function buildFallbackTemplateRow_(runId, templateId) {
  const cleanTemplateId = String(templateId || "").trim();
  if (!cleanTemplateId) throw new Error("Missing Template_ID_Reference.");

  log_(runId, "WARN", "DOCGEN_TEMPLATE_METADATA_FALLBACK", {
    templateId: cleanTemplateId,
    message: "Doc_Templates row was not returned by AppSheet; using Template_ID_Reference as Google Docs file id."
  });

  return { Template_ID: cleanTemplateId };
}

function generatePdfForAgreementFileRow_(runId, fileRow, mainRow, templateRow, options) {
  options = options || {};
  const startedAt = Date.now();
  const templateId = getField_(templateRow, "Template_ID");
  if (!templateId) throw new Error("Doc_Templates.Template_ID is blank.");

  const relativePath = normalizeRelativeDrivePath_(getField_(fileRow, "File") || buildRelativeFilePathFromRow_(fileRow));
  if (!relativePath) throw new Error("Agreements_Files.File is blank and cannot be rebuilt.");

  const output = timeDocgenStep_(runId, "DOCGEN_TIMING_RESOLVE_OUTPUT", {
    agreementFileId: getField_(fileRow, "ID"),
    templateId: templateId
  }, () => {
    const outputRoot = options.outputRootFolder || getDocGeneratorOutputRootFolder_();
    return ensureFolderPathAndName_(outputRoot, relativePath);
  });

  const workingFolder = timeDocgenStep_(runId, "DOCGEN_TIMING_RESOLVE_WORKING_FOLDER", {
    agreementFileId: getField_(fileRow, "ID")
  }, () => ensureChildFolder_(output.rootFolder, CONFIG.DOC_GENERATOR.WORKING_DOCS_FOLDER_NAME));

  if (CONFIG.DOC_GENERATOR.OVERWRITE_EXISTING_PDF) {
    timeDocgenStep_(runId, "DOCGEN_TIMING_TRASH_EXISTING", {
      agreementFileId: getField_(fileRow, "ID")
    }, () => trashExistingFilesByName_(output.folder, output.fileName));
  }

  const sourceFile = timeDocgenStep_(runId, "DOCGEN_TIMING_GET_TEMPLATE", {
    agreementFileId: getField_(fileRow, "ID"),
    templateId: templateId
  }, () => DriveApp.getFileById(templateId));
  const docName = output.fileName.replace(/\.pdf$/i, "");
  const docCopy = timeDocgenStep_(runId, "DOCGEN_TIMING_COPY_TEMPLATE", {
    agreementFileId: getField_(fileRow, "ID"),
    templateId: templateId
  }, () => sourceFile.makeCopy(docName, workingFolder));

  const placeholdersReplacedByApi = timeDocgenStep_(runId, "DOCGEN_TIMING_REPLACE_PLACEHOLDERS", {
    agreementFileId: getField_(fileRow, "ID")
  }, () => replaceTemplatePlaceholdersForCopiedDoc_(runId, docCopy.getId(), mainRow, fileRow, templateRow));
  const doc = timeDocgenStep_(runId, "DOCGEN_TIMING_OPEN_DOC", {
    agreementFileId: getField_(fileRow, "ID")
  }, () => DocumentApp.openById(docCopy.getId()));
  const exportTabId = timeDocgenStep_(runId, "DOCGEN_TIMING_GET_TAB", {
    agreementFileId: getField_(fileRow, "ID")
  }, () => getFirstDocumentTabId_(doc));
  if (placeholdersReplacedByApi) {
    timeDocgenStep_(runId, "DOCGEN_TIMING_CLOSE_DOC", {
      agreementFileId: getField_(fileRow, "ID")
    }, () => doc.saveAndClose());
  } else {
    timeDocgenStep_(runId, "DOCGEN_TIMING_SAVE_DOC", {
      agreementFileId: getField_(fileRow, "ID")
    }, () => doc.saveAndClose());
  }

  const pdfBlob = timeDocgenStep_(runId, "DOCGEN_TIMING_EXPORT_PDF", {
    agreementFileId: getField_(fileRow, "ID")
  }, () => exportGoogleDocTabToPdf_(docCopy.getId(), exportTabId, output.fileName));
  const pdfFile = timeDocgenStep_(runId, "DOCGEN_TIMING_CREATE_PDF_FILE", {
    agreementFileId: getField_(fileRow, "ID")
  }, () => output.folder.createFile(pdfBlob));

  if (!CONFIG.DOC_GENERATOR.KEEP_WORKING_DOC_COPY) {
    timeDocgenStep_(runId, "DOCGEN_TIMING_TRASH_WORKING_DOC", {
      agreementFileId: getField_(fileRow, "ID")
    }, () => docCopy.setTrashed(true));
  }

  log_(runId, "INFO", "DOCGEN_FILE_CREATED", {
    agreementFileId: getField_(fileRow, "ID"),
    templateId: templateId,
    relativePath: relativePath,
    pdfId: pdfFile.getId(),
    docId: docCopy.getId(),
    durationMs: Date.now() - startedAt
  });

  return {
    relativePath: relativePath,
    pdfId: pdfFile.getId(),
    pdfUrl: pdfFile.getUrl(),
    docId: docCopy.getId(),
    docUrl: docCopy.getUrl()
  };
}

function timeDocgenStep_(runId, eventName, data, fn) {
  const startedAt = Date.now();
  try {
    return fn();
  } finally {
    const payload = data || {};
    payload.durationMs = Date.now() - startedAt;
    log_(runId, "INFO", eventName, payload);
  }
}

function replaceTemplatePlaceholdersForCopiedDoc_(runId, docId, mainRow, fileRow, templateRow) {
  if (CONFIG.DOC_GENERATOR.USE_DOCS_API_PLACEHOLDER_REPLACEMENT !== false &&
      DOCGEN_RUNTIME_CACHE.docsApiPlaceholderReplacementAvailable !== false) {
    try {
      const replaced = replaceTemplatePlaceholdersWithDocsApi_(runId, docId, mainRow, fileRow, templateRow);
      if (replaced) return true;
    } catch (e) {
      if (isDocsApiDisabledError_(e)) {
        DOCGEN_RUNTIME_CACHE.docsApiPlaceholderReplacementAvailable = false;
      }
      log_(runId, "WARN", "DOCGEN_PLACEHOLDER_DOCS_API_FALLBACK", {
        docId: docId,
        disabledForRun: DOCGEN_RUNTIME_CACHE.docsApiPlaceholderReplacementAvailable === false,
        error: e && e.message || String(e)
      });
    }
  }

  const doc = DocumentApp.openById(docId);
  replaceTemplatePlaceholders_(doc, mainRow, fileRow, templateRow, runId);
  doc.saveAndClose();
  return false;
}

function isDocsApiDisabledError_(error) {
  const message = error && error.message || String(error || "");
  return message.indexOf("docs.googleapis.com") >= 0 &&
    (message.indexOf("httpCode=403") >= 0 || message.indexOf("PERMISSION_DENIED") >= 0);
}

function replaceTemplatePlaceholdersWithDocsApi_(runId, docId, mainRow, fileRow, templateRow) {
  const values = {};
  addReplacementValues_(values, mainRow, "");
  addReplacementValues_(values, fileRow, "File.");
  addReplacementValues_(values, templateRow, "Template.");

  const context = {
    Onboarding_ID: mainRow,
    Template_ID_Reference: templateRow,
    Template_ID: templateRow,
    File: fileRow
  };

  const docText = fetchGoogleDocText_(docId);
  if (!textContainsTemplateMarker_(docText)) {
    throw new Error("Docs API text read did not find template markers; using DocumentApp fallback.");
  }

  const replacements = buildTemplatePlaceholderReplacements_(docText, context, values);
  const markers = Object.keys(replacements);
  if (!markers.length) {
    throw new Error("Docs API did not build replacement requests; using DocumentApp fallback.");
  }

  const requests = markers.map(marker => ({
    replaceAllText: {
      containsText: {
        text: marker,
        matchCase: true
      },
      replaceText: replacements[marker]
    }
  }));

  batchUpdateGoogleDoc_(docId, requests);
  const remainingText = fetchGoogleDocText_(docId);
  if (textContainsTemplateMarker_(remainingText)) {
    throw new Error("Docs API replacement left template markers; using DocumentApp fallback.");
  }

  log_(runId, "INFO", "DOCGEN_PLACEHOLDER_DOCS_API_PASS", {
    markers: markers.length,
    requests: requests.length,
    fallback: false
  });
  return true;
}

function fetchGoogleDocText_(docId) {
  const url = "https://docs.googleapis.com/v1/documents/" + encodeURIComponent(docId) +
    "?includeTabsContent=true";
  const response = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
    muteHttpExceptions: true
  });
  const code = response.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error("Docs API get failed for " + docId + " httpCode=" + code + " body=" + response.getContentText().slice(0, 500));
  }

  const doc = safeJsonParse_(response.getContentText()) || {};
  const chunks = [];
  collectGoogleDocTextRuns_(doc, chunks);
  return chunks.join("");
}

function collectGoogleDocTextRuns_(value, out) {
  if (value == null) return;
  if (Array.isArray(value)) {
    value.forEach(item => collectGoogleDocTextRuns_(item, out));
    return;
  }
  if (typeof value !== "object") return;
  if (value.textRun && typeof value.textRun.content === "string") {
    out.push(value.textRun.content);
  }
  Object.keys(value).forEach(key => collectGoogleDocTextRuns_(value[key], out));
}

function buildTemplatePlaceholderReplacements_(docText, context, values) {
  const replacements = {};
  const text = String(docText || "");

  const angleMatches = text.match(/<<[\s\S]*?>>/g) || [];
  angleMatches.forEach(match => {
    const replacement = evaluateTemplateAnglePlaceholder_(match, context, values);
    if (replacement == null || replacement === match) return;
    replacements[match] = replacement;
  });

  const curlyMatches = text.match(/\{\{([^{}]+)\}\}/g) || [];
  curlyMatches.forEach(match => {
    const replacement = evaluateTemplateCurlyPlaceholder_(match, context, values);
    if (replacement == null || replacement === match) return;
    replacements[match] = replacement;
  });

  return replacements;
}

function evaluateTemplateAnglePlaceholder_(match, context, values) {
  if (/^<<\s*IF\(/i.test(match)) {
    const replacement = evaluateAppSheetIfExpression_(match, context);
    return replacement == null ? null : stringifyDocValue_(replacement);
  }

  const known = evaluateKnownAppSheetExpression_(match, context);
  if (known != null) return stringifyDocValue_(known);

  if (/^<<\s*\[[^\]]+\]\.(?:\[[^\]]+\]|[^<>]+?)\s*>>$/.test(match)) {
    return stringifyDocValue_(resolveAppSheetValue_(match, context));
  }

  const simple = match.match(/^<<\s*\[([^\]]+)\]\s*>>$/);
  if (simple) {
    const field = simple[1];
    const direct = values[field];
    return stringifyDocValue_(direct != null ? direct : resolveAppSheetValue_("[" + field + "]", context));
  }

  return null;
}

function evaluateTemplateCurlyPlaceholder_(match, context, values) {
  const raw = String(match || "").replace(/^\{\{/, "").replace(/\}\}$/, "");
  const key = raw.trim();
  if (!key) return null;
  if (Object.prototype.hasOwnProperty.call(values, key)) return stringifyDocValue_(values[key]);

  const deref = key.match(/^([^.[\]]+)\.([\s\S]+)$/);
  if (deref) {
    const row = getContextRow_(context, deref[1]);
    if (row) return stringifyDocValue_(getField_(row, deref[2]));
  }

  return null;
}

function batchUpdateGoogleDoc_(docId, requests) {
  const url = "https://docs.googleapis.com/v1/documents/" + encodeURIComponent(docId) + ":batchUpdate";
  const response = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
    payload: JSON.stringify({ requests: requests || [] }),
    muteHttpExceptions: true
  });
  const code = response.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error("Docs API batchUpdate failed for " + docId + " httpCode=" + code + " body=" + response.getContentText().slice(0, 500));
  }
}

function replaceTemplatePlaceholders_(doc, mainRow, fileRow, templateRow, runId) {
  const values = {};
  addReplacementValues_(values, mainRow, "");
  addReplacementValues_(values, fileRow, "File.");
  addReplacementValues_(values, templateRow, "Template.");

  const sections = getPrimaryDocumentSections_(doc);

  const context = {
    Onboarding_ID: mainRow,
    Template_ID_Reference: templateRow,
    Template_ID: templateRow,
    File: fileRow
  };

  const stats = replaceTemplatePlaceholdersInTextNodes_(sections, context, values);
  const needsFallback = sectionsContainTemplateMarkers_(sections);
  if (runId) {
    log_(runId, "INFO", "DOCGEN_PLACEHOLDER_FAST_PASS", {
      textNodes: stats.textNodes,
      nodesWithMarkers: stats.nodesWithMarkers,
      nodesUpdated: stats.nodesUpdated,
      fallback: needsFallback
    });
  }
  if (!needsFallback) return;

  replaceTemplatePlaceholdersFallback_(sections, context, values, mainRow, templateRow);
}

function replaceTemplatePlaceholdersFallback_(sections, context, values, mainRow, templateRow) {
  replaceAppSheetIfExpressionsInTextNodes_(sections, context);
  replaceAppSheetIfExpressions_(sections, context);
  replaceKnownAppSheetExpressions_(sections, context);
  replaceAppSheetTodayExpressions_(sections);
  replaceAppSheetReferencesInTextNodes_(sections, context);
  replaceAppSheetReferences_(sections, context, values);

  replaceLiteralValuePlaceholders_(sections, values);

  replaceDereferencedValues_(sections, "Onboarding_ID", mainRow);
  replaceDereferencedValues_(sections, "Template_ID_Reference", templateRow);
  replaceDereferencedValues_(sections, "Template_ID", templateRow);
}

function replaceTemplatePlaceholdersInTextNodes_(sections, context, values) {
  const stats = {
    textNodes: 0,
    nodesWithMarkers: 0,
    nodesUpdated: 0
  };

  sections.forEach(section => {
    walkDocElement_(section, element => {
      if (!element || element.getType() !== DocumentApp.ElementType.TEXT) return;
      stats.textNodes++;
      const text = element.asText();
      const original = text.getText();
      if (!textContainsTemplateMarker_(original)) return;
      stats.nodesWithMarkers++;

      const updated = replaceTemplatePlaceholdersInString_(original, context, values);
      if (updated !== original) {
        text.setText(updated);
        stats.nodesUpdated++;
      }
    });
  });

  return stats;
}

function replaceTemplatePlaceholdersInString_(text, context, values) {
  let out = String(text || "");

  out = out.replace(/<<\s*IF\([\s\S]*?\)\s*>>/g, match => {
    const replacement = evaluateAppSheetIfExpression_(match, context);
    return replacement == null ? match : replacement;
  });

  out = out.replace(/<<\s*TODAY\(\)\s*>>/gi, () =>
    Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd-MM-yyyy")
  );

  out = out.replace(/<<[\s\S]*?>>/g, match => {
    const known = evaluateKnownAppSheetExpression_(match, context);
    if (known != null) return stringifyDocValue_(known);

    const deref = match.match(/^<<\s*\[[^\]]+\]\.(?:\[[^\]]+\]|[^<>]+?)\s*>>$/);
    if (deref) return stringifyDocValue_(resolveAppSheetValue_(match, context));

    const simple = match.match(/^<<\s*\[([^\]]+)\]\s*>>$/);
    if (simple) {
      const field = simple[1];
      const direct = values[field];
      return stringifyDocValue_(direct != null ? direct : resolveAppSheetValue_("[" + field + "]", context));
    }

    return match;
  });

  out = out.replace(/\{\{([^{}]+)\}\}/g, (match, rawKey) => {
    const key = String(rawKey || "").trim();
    if (!key) return match;
    if (Object.prototype.hasOwnProperty.call(values, key)) return stringifyDocValue_(values[key]);

    const deref = key.match(/^([^.[\]]+)\.([\s\S]+)$/);
    if (deref) {
      const row = getContextRow_(context, deref[1]);
      if (row) return stringifyDocValue_(getField_(row, deref[2]));
    }

    return match;
  });

  return out;
}

function sectionsContainTemplateMarkers_(sections) {
  return sections.some(section => textContainsTemplateMarker_(getSectionText_(section)));
}

function textContainsTemplateMarker_(text) {
  const s = String(text || "");
  return s.indexOf("<<") >= 0 || s.indexOf("{{") >= 0;
}

function getPrimaryDocumentSections_(doc) {
  const sections = [];
  const documentTab = getFirstDocumentTab_(doc);
  if (documentTab) {
    if (documentTab.getBody()) sections.push(documentTab.getBody());
    if (documentTab.getHeader()) sections.push(documentTab.getHeader());
    if (documentTab.getFooter()) sections.push(documentTab.getFooter());
    return sections;
  }

  if (doc.getBody()) sections.push(doc.getBody());
  if (doc.getHeader()) sections.push(doc.getHeader());
  if (doc.getFooter()) sections.push(doc.getFooter());
  return sections;
}

function getFirstDocumentTab_(doc) {
  try {
    if (!doc.getTabs) return null;
    const tabs = flattenTabs_(doc.getTabs());
    if (!tabs.length) return null;
    return tabs[0].asDocumentTab();
  } catch (e) {
    return null;
  }
}

function getFirstDocumentTabId_(doc) {
  try {
    if (!doc.getTabs) return "";
    const tabs = flattenTabs_(doc.getTabs());
    return tabs.length ? tabs[0].getId() : "";
  } catch (e) {
    return "";
  }
}

function flattenTabs_(tabs) {
  const out = [];
  (tabs || []).forEach(tab => {
    out.push(tab);
    const children = tab.getChildTabs ? tab.getChildTabs() : [];
    flattenTabs_(children).forEach(child => out.push(child));
  });
  return out;
}

function exportGoogleDocTabToPdf_(docId, tabId, pdfName) {
  if (tabId) {
    const url = "https://docs.google.com/document/d/" + encodeURIComponent(docId) +
      "/export?format=pdf&tab=" + encodeURIComponent(tabId);
    const response = UrlFetchApp.fetch(url, {
      headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
      muteHttpExceptions: true
    });

    if (response.getResponseCode() === 200) {
      return response.getBlob().setName(pdfName);
    }

    console.warn("DOCGEN_TAB_EXPORT_FALLBACK " + response.getResponseCode() + " " + response.getContentText().slice(0, 500));
  }

  return DriveApp.getFileById(docId).getAs(MimeType.PDF).setName(pdfName);
}

function addReplacementValues_(out, row, prefix) {
  if (!row) return;
  Object.keys(row).forEach(key => {
    const clean = String(key || "").trim();
    if (clean) out[prefix + clean] = row[key];
  });
}

function replaceLiteralValuePlaceholders_(sections, values) {
  if (!values) return;
  const keys = Object.keys(values);
  if (!keys.length) return;

  sections.forEach(section => {
    const sectionText = getSectionText_(section);
    if (sectionText.indexOf("{{") < 0 && sectionText.indexOf("<<[") < 0) return;

    keys.forEach(key => {
      const curly = "{{" + key + "}}";
      const appSheet = "<<[" + key + "]>>";
      if (sectionText.indexOf(curly) < 0 && sectionText.indexOf(appSheet) < 0) return;

      const value = stringifyDocValue_(values[key]);
      if (sectionText.indexOf(curly) >= 0) section.replaceText(escapeRegExp_(curly), value);
      if (sectionText.indexOf(appSheet) >= 0) section.replaceText(escapeRegExp_(appSheet), value);
    });
  });
}

function replaceDereferencedValues_(sections, refName, row) {
  if (!row) return;
  const keys = Object.keys(row);
  sections.forEach(section => {
    const sectionText = getSectionText_(section);
    if (sectionText.indexOf(refName) < 0) return;

    keys.forEach(key => {
      const bracketed = "<<[" + refName + "].[" + key + "]>>";
      const loose = "<<[" + refName + "]." + key + ">>";
      const curly = "{{" + refName + "." + key + "}}";
      if (sectionText.indexOf(bracketed) < 0 && sectionText.indexOf(loose) < 0 && sectionText.indexOf(curly) < 0) return;

      const value = stringifyDocValue_(row[key]);
      if (sectionText.indexOf(bracketed) >= 0) section.replaceText(escapeRegExp_(bracketed), value);
      if (sectionText.indexOf(loose) >= 0) section.replaceText(escapeRegExp_(loose), value);
      if (sectionText.indexOf(curly) >= 0) section.replaceText(escapeRegExp_(curly), value);
    });
  });
}

function replaceAppSheetIfExpressions_(sections, context) {
  sections.forEach(section => {
    let text = getSectionText_(section);
    const matches = text.match(/<<\s*IF\([\s\S]*?\)\s*>>/g) || [];
    matches.forEach(match => {
      const replacement = evaluateAppSheetIfExpression_(match, context);
      if (replacement == null) return;
      section.replaceText(escapeRegExp_(match), replacement);
      text = text.replace(match, replacement);
    });
  });
}

function replaceAppSheetIfExpressionsInTextNodes_(sections, context) {
  sections.forEach(section => {
    walkDocElement_(section, element => {
      if (!element || element.getType() !== DocumentApp.ElementType.TEXT) return;
      const text = element.asText();
      const original = text.getText();
      if (original.indexOf("<<") < 0 || original.toUpperCase().indexOf("IF(") < 0) return;

      const updated = original.replace(/<<\s*IF\([\s\S]*?\)\s*>>/g, match => {
        const replacement = evaluateAppSheetIfExpression_(match, context);
        return replacement == null ? match : replacement;
      });

      if (updated !== original) {
        text.setText(updated);
      }
    });
  });
}

function walkDocElement_(element, visitor) {
  visitor(element);
  if (!element || !element.getNumChildren) return;
  const count = element.getNumChildren();
  for (let i = 0; i < count; i++) {
    walkDocElement_(element.getChild(i), visitor);
  }
}

function evaluateAppSheetIfExpression_(expression, context) {
  const inner = String(expression || "").replace(/^<<\s*IF\(/i, "").replace(/\)\s*>>$/i, "");
  const args = splitExpressionArgs_(inner);
  if (args.length !== 3) return null;

  const conditionResult = evaluateAppSheetCondition_(args[0], context);
  if (conditionResult == null) return null;
  const result = conditionResult ? stripExpressionQuotes_(args[1]) : stripExpressionQuotes_(args[2]);
  return replaceInlineReferencesInText_(result, context);
}

function replaceKnownAppSheetExpressions_(sections, context) {
  sections.forEach(section => {
    let text = getSectionText_(section);
    const matches = text.match(/<<[\s\S]*?>>/g) || [];
    matches.forEach(match => {
      const value = evaluateKnownAppSheetExpression_(match, context);
      if (value == null) return;
      section.replaceText(escapeRegExp_(match), stringifyDocValue_(value));
      text = text.replace(match, stringifyDocValue_(value));
    });
  });
}

function evaluateKnownAppSheetExpression_(expression, context) {
  const inner = String(expression || "").replace(/^<<\s*/, "").replace(/\s*>>$/, "").trim();
  if (!inner) return null;
  if (/^\[[^\]]+\](\.\[[^\]]+\])?$/.test(inner)) return null;

  const ifMatch = inner.match(/^IF\(([\s\S]*)\)$/i);
  if (ifMatch) return evaluateAppSheetIfExpression_("<<" + inner + ">>", context);

  const textMatch = inner.match(/^TEXT\(([\s\S]*)\)$/i);
  if (textMatch) {
    const args = splitExpressionArgs_(textMatch[1]);
    if (args.length < 2) return null;
    const rawValue = evaluateAppSheetScalar_(args[0], context);
    const format = stripExpressionQuotes_(args[1]);
    return formatDateLikeAppSheet_(rawValue, format);
  }

  return null;
}

function evaluateAppSheetCondition_(condition, context) {
  const s = String(condition || "").trim();

  const notMatch = s.match(/^NOT\(([\s\S]+)\)$/i);
  if (notMatch) {
    const value = evaluateAppSheetCondition_(notMatch[1], context);
    return value == null ? null : !value;
  }

  const andMatch = s.match(/^AND\(([\s\S]+)\)$/i);
  if (andMatch) {
    const args = splitExpressionArgs_(andMatch[1]);
    if (!args.length) return null;
    return args.every(arg => evaluateAppSheetCondition_(arg, context) === true);
  }

  const orMatch = s.match(/^OR\(([\s\S]+)\)$/i);
  if (orMatch) {
    const args = splitExpressionArgs_(orMatch[1]);
    if (!args.length) return null;
    return args.some(arg => evaluateAppSheetCondition_(arg, context) === true);
  }

  const isBlankMatch = s.match(/^ISBLANK\(([\s\S]+)\)$/i);
  if (isBlankMatch) return isBlank_(evaluateAppSheetScalar_(isBlankMatch[1], context));

  const containsMatch = s.match(/^CONTAINS\(([\s\S]+)\)$/i);
  if (containsMatch) {
    const args = splitExpressionArgs_(containsMatch[1]);
    if (args.length !== 2) return null;
    const haystack = stringifyDocValue_(evaluateAppSheetScalar_(args[0], context)).toLowerCase();
    const needle = stringifyDocValue_(evaluateAppSheetScalar_(args[1], context)).toLowerCase();
    return haystack.indexOf(needle) >= 0;
  }

  const compare = s.match(/^([\s\S]+?)(<>|=)([\s\S]+)$/);
  if (compare) {
    const left = stringifyDocValue_(evaluateAppSheetScalar_(compare[1], context));
    const right = stringifyDocValue_(evaluateAppSheetScalar_(compare[3], context));
    return compare[2] === "=" ? left === right : left !== right;
  }

  return null;
}

function evaluateAppSheetScalar_(expr, context) {
  const s = String(expr || "").trim();
  if (!s) return "";
  if ((s.charAt(0) === '"' && s.charAt(s.length - 1) === '"') ||
      (s.charAt(0) === "'" && s.charAt(s.length - 1) === "'")) {
    return stripExpressionQuotes_(s);
  }

  const textMatch = s.match(/^TEXT\(([\s\S]*)\)$/i);
  if (textMatch) {
    const args = splitExpressionArgs_(textMatch[1]);
    if (args.length >= 2) return formatDateLikeAppSheet_(evaluateAppSheetScalar_(args[0], context), stripExpressionQuotes_(args[1]));
  }

  const todayMatch = s.match(/^TODAY\(\)$/i);
  if (todayMatch) return new Date();

  return resolveAppSheetValue_(s, context);
}

function replaceAppSheetTodayExpressions_(sections) {
  const todayDash = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd-MM-yyyy");
  const todaySlash = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd/MM/yyyy");

  sections.forEach(section => {
    section.replaceText(/<<\s*TEXT\(\s*TODAY\(\)\s*,\s*"DD-MM-YYYY"\s*\)\s*>>/gi.source, todayDash);
    section.replaceText(/<<\s*TEXT\(\s*TODAY\(\)\s*,\s*"DD\/MM\/YYYY"\s*\)\s*>>/gi.source, todaySlash);
    section.replaceText(/<<\s*TODAY\(\)\s*>>/gi.source, todayDash);
  });
}

function formatDateLikeAppSheet_(value, format) {
  const pattern = String(format || "").trim().toUpperCase();
  const date = coerceDate_(value);
  if (!date) return stringifyDocValue_(value);

  if (pattern === "DD/MM/YYYY") {
    return Utilities.formatDate(date, Session.getScriptTimeZone(), "dd/MM/yyyy");
  }
  if (pattern === "DD-MM-YYYY") {
    return Utilities.formatDate(date, Session.getScriptTimeZone(), "dd-MM-yyyy");
  }
  if (pattern === "YYYY-MM-DD") {
    return Utilities.formatDate(date, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }
  return Utilities.formatDate(date, Session.getScriptTimeZone(), "dd-MM-yyyy");
}

function coerceDate_(value) {
  if (value instanceof Date && !isNaN(value.getTime())) return value;
  const s = String(value || "").trim();
  if (!s) return null;
  if (/^TODAY\(\)$/i.test(s)) return new Date();

  const iso = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (iso) return new Date(Number(iso[1]), Number(iso[2]) - 1, Number(iso[3]));

  const slash = s.match(/^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{4})/);
  if (slash) return new Date(Number(slash[3]), Number(slash[2]) - 1, Number(slash[1]));

  const parsed = new Date(s);
  return isNaN(parsed.getTime()) ? null : parsed;
}

function replaceAppSheetReferences_(sections, context, values) {
  sections.forEach(section => {
    let text = getSectionText_(section);
    const derefMatches = text.match(/<<\s*\[[^\]]+\]\.(?:\[[^\]]+\]|[^<>]+?)\s*>>/g) || [];
    derefMatches.forEach(match => {
      const value = stringifyDocValue_(resolveAppSheetValue_(match, context));
      section.replaceText(escapeRegExp_(match), value);
      text = text.replace(match, value);
    });

    const simpleMatches = text.match(/<<\s*\[[^\]]+\]\s*>>/g) || [];
    simpleMatches.forEach(match => {
      const field = match.replace(/^<<\s*\[/, "").replace(/\]\s*>>$/, "");
      const direct = values[field];
      const value = direct != null ? direct : resolveAppSheetValue_("[" + field + "]", context);
      section.replaceText(escapeRegExp_(match), stringifyDocValue_(value));
    });
  });
}

function replaceAppSheetReferencesInTextNodes_(sections, context) {
  sections.forEach(section => {
    walkDocElement_(section, element => {
      if (!element || element.getType() !== DocumentApp.ElementType.TEXT) return;
      const text = element.asText();
      const original = text.getText();
      if (original.indexOf("<<") < 0 || original.indexOf("[") < 0) return;

      const updated = original
        .replace(/<<\s*\[([^\]]+)\]\.\[([^\]]+)\]\s*>>/g, function(match) {
          return stringifyDocValue_(resolveAppSheetValue_(match, context));
        })
        .replace(/<<\s*\[([^\]]+)\]\.([^<>]+?)\s*>>/g, function(match) {
          return stringifyDocValue_(resolveAppSheetValue_(match, context));
        })
        .replace(/<<\s*\[([^\]]+)\]\s*>>/g, function(match) {
          return stringifyDocValue_(resolveAppSheetValue_(match, context));
        });

      if (updated !== original) text.setText(updated);
    });
  });
}

function replaceInlineReferencesInText_(text, context) {
  return String(text || "")
    .replace(/<<\s*\[([^\]]+)\]\.\[([^\]]+)\]\s*>>/g, function(match) {
      return stringifyDocValue_(resolveAppSheetValue_(match, context));
    })
    .replace(/<<\s*\[([^\]]+)\]\.([^<>]+?)\s*>>/g, function(match) {
      return stringifyDocValue_(resolveAppSheetValue_(match, context));
    })
    .replace(/<<\s*\[([^\]]+)\]\s*>>/g, function(match) {
      return stringifyDocValue_(resolveAppSheetValue_(match, context));
    })
    .replace(/\[([^\]]+)\]\.\[([^\]]+)\]/g, function(match) {
      return stringifyDocValue_(resolveAppSheetValue_(match, context));
    })
    .replace(/\[([^\]]+)\]\.([^\],)"]+)/g, function(match) {
      return stringifyDocValue_(resolveAppSheetValue_(match, context));
    });
}

function resolveAppSheetValue_(expr, context) {
  const s = String(expr || "").trim();
  const wrapped = s.match(/^<<\s*(.+?)\s*>>$/);
  if (wrapped) return resolveAppSheetValue_(wrapped[1], context);

  const deref = s.match(/^\[([^\]]+)\]\.\[([^\]]+)\]$/);
  if (deref) {
    const row = getContextRow_(context, deref[1]);
    return getField_(row, deref[2]);
  }

  const looseDeref = s.match(/^\[([^\]]+)\]\.([\s\S]+)$/);
  if (looseDeref) {
    const row = getContextRow_(context, looseDeref[1]);
    return getField_(row, looseDeref[2].replace(/\]+$/, "").trim());
  }

  const simple = s.match(/^\[([^\]]+)\]$/);
  if (simple) {
    return getField_(context.Onboarding_ID, simple[1]) ||
      getField_(context.File, simple[1]) ||
      getField_(context.Template_ID_Reference, simple[1]);
  }

  return "";
}

function getContextRow_(context, refName) {
  const target = normalizeKey_(refName);
  const keys = Object.keys(context || {});
  for (let i = 0; i < keys.length; i++) {
    if (normalizeKey_(keys[i]) === target) return context[keys[i]];
  }
  return null;
}

function splitExpressionArgs_(text) {
  const args = [];
  let current = "";
  let quote = "";
  let depth = 0;
  const s = String(text || "");

  for (let i = 0; i < s.length; i++) {
    const ch = s.charAt(i);
    if (quote) {
      current += ch;
      if (ch === quote && s.charAt(i - 1) !== "\\") quote = "";
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      current += ch;
      continue;
    }
    if (ch === "(") depth++;
    if (ch === ")") depth--;
    if (ch === "," && depth === 0) {
      args.push(current.trim());
      current = "";
      continue;
    }
    current += ch;
  }
  if (current.trim() || s.endsWith(",")) args.push(current.trim());
  return args;
}

function stripExpressionQuotes_(value) {
  const s = String(value || "").trim();
  if ((s.charAt(0) === '"' && s.charAt(s.length - 1) === '"') ||
      (s.charAt(0) === "'" && s.charAt(s.length - 1) === "'")) {
    return s.slice(1, -1);
  }
  return s;
}

function isBlank_(value) {
  return value == null || String(value).trim() === "";
}

function getSectionText_(section) {
  try {
    return section.getText ? section.getText() : "";
  } catch (e) {
    return "";
  }
}

function buildAgreementFileReadyRow_(fileRow, generated) {
  return {
    ID: getField_(fileRow, "ID"),
    File_status: CONFIG.DOC_GENERATOR.FILE_STATUS_READY,
    File: generated.relativePath
  };
}

function buildAgreementFileGeneratedRow_(fileRow, generated) {
  return {
    ID: getField_(fileRow, "ID"),
    File_status: CONFIG.DOC_GENERATOR.FILE_STATUS_GENERATED,
    File: generated.relativePath
  };
}

function buildAgreementFileGeneratingRow_(fileRow) {
  return {
    ID: getField_(fileRow, "ID"),
    File_status: CONFIG.DOC_GENERATOR.FILE_STATUS_GENERATING
  };
}

function markAgreementFileGenerating_(runId, fileRow) {
  if (!CONFIG.DOC_GENERATOR.FILE_PROGRESS_STATUSES_ENABLED) return;
  if (!shouldWriteAgreementFileProgressStatus_("generating")) return;
  markAgreementFileProgressStatus_(runId, buildAgreementFileGeneratingRow_(fileRow), "DOCGEN_FILE_GENERATING");
}

function markAgreementFileGenerated_(runId, row) {
  if (!CONFIG.DOC_GENERATOR.FILE_PROGRESS_STATUSES_ENABLED) return;
  if (!shouldWriteAgreementFileProgressStatus_("generated")) {
    log_(runId, "INFO", "DOCGEN_FILE_GENERATED_DEFERRED_TO_READY_BATCH", {
      id: row && row.ID || "",
      status: row && row.File_status || "",
      file: row && row.File || ""
    });
    return;
  }
  markAgreementFileProgressStatus_(runId, row, "DOCGEN_FILE_GENERATED");
}

function shouldWriteAgreementFileProgressStatus_(stage) {
  const mode = String(CONFIG.DOC_GENERATOR.FILE_PROGRESS_UPDATE_MODE || "all").trim().toLowerCase();
  if (mode === "off" || mode === "none") return false;
  if (mode === "generating_only") return stage === "generating";
  if (mode === "generated_only") return stage === "generated";
  return true;
}

function markAgreementFileProgressStatus_(runId, row, eventName) {
  if (!row || !row.ID) return;
  try {
    callAppSheet_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, row, CONFIG.APPSHEET_ACTION_EDIT, row.ID);
    log_(runId, "INFO", eventName, {
      id: row.ID,
      status: row.File_status || "",
      file: row.File || ""
    });
  } catch (e) {
    log_(runId, "WARN", eventName + "_UPDATE_FAILED_CONTINUING", {
      id: row.ID,
      status: row.File_status || "",
      error: String(e && e.message || e).slice(0, 900)
    });
  }
}

function getGeneratedArtifactFromExistingRow_(fileRow) {
  const status = String(getField_(fileRow, "File_status") || "").trim();
  const file = String(getField_(fileRow, "File") || "").trim();
  if (status !== CONFIG.DOC_GENERATOR.FILE_STATUS_GENERATED || !file) return null;
  return { relativePath: file, pdfId: "", docId: "" };
}

function batchMarkAgreementFilesReady_(runId, rows) {
  const cleanRows = (rows || []).filter(row => row && row.ID);
  if (!cleanRows.length) return;
  callAppSheetRows_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, cleanRows, CONFIG.APPSHEET_ACTION_EDIT, "batch-ready-files");
}

function markAgreementFileFailed_(runId, fileRow, err) {
  const id = getField_(fileRow, "ID");
  if (!id) return;
  if (!CONFIG.DOC_GENERATOR.FILE_STATUS_FAILED) {
    log_(runId, "ERROR", "DOCGEN_FILE_FAILED_RETRY_LEFT_SET_UP", {
      id: id,
      error: String(err && err.message || err)
    });
    return;
  }
  try {
    callAppSheet_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, {
      ID: id,
      File_status: CONFIG.DOC_GENERATOR.FILE_STATUS_FAILED
    }, CONFIG.APPSHEET_ACTION_EDIT, id);
  } catch (updateErr) {
    log_(runId, "ERROR", "DOCGEN_MARK_FILE_FAILED_ERROR", {
      id: id,
      originalError: String(err && err.message || err),
      updateError: String(updateErr && updateErr.message || updateErr)
    });
  }
}

function buildGenerationJobItemCreatedRow_(fileRow) {
  const jobItemId = getField_(fileRow, "Job_Item_ID");
  if (!jobItemId) return null;
  return {
    Job_Item_ID: jobItemId,
    Item_Status: CONFIG.DOC_GENERATOR.ITEM_STATUS_FILE_CREATED
  };
}

function batchMarkGenerationJobItemsCreated_(runId, rows) {
  const cleanRows = (rows || []).filter(row => row && row.Job_Item_ID);
  if (!cleanRows.length) return;
  callAppSheetRows_(runId, DOCGEN_TABLES.GENERATION_JOB_ITEMS, cleanRows, CONFIG.APPSHEET_ACTION_EDIT, "batch-job-items-created");
}

function createSignedDocumentUploadRowsForReadyAgreements_(runId, processedFiles, readyFileUpdates) {
  const readyIds = {};
  (readyFileUpdates || []).forEach(row => {
    if (row && row.ID) readyIds[String(row.ID)] = true;
  });
  if (!Object.keys(readyIds).length) return;

  const candidateRows = (processedFiles || []).filter(fileRow => {
    const id = String(getField_(fileRow, "ID") || "");
    const category = String(getField_(fileRow, "Category") || "");
    const templateId = String(getField_(fileRow, "Template_ID_Reference") || "").trim();
    return readyIds[id] && category === CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY && templateId !== "";
  });
  if (!candidateRows.length) return;

  const existingKeys = findExistingSignedDocumentKeys_(runId, candidateRows);
  const rowsToAdd = [];
  candidateRows.forEach(fileRow => {
    const onboardingId = String(getField_(fileRow, "Onboarding_ID") || "").trim();
    const category = String(getField_(fileRow, "Category") || CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY).trim();
    const templateId = String(getField_(fileRow, "Template_ID_Reference") || "").trim();
    const key = buildSignedDocumentDedupeKey_(onboardingId, templateId, category);
    if (!onboardingId || !templateId || existingKeys[key]) return;

    existingKeys[key] = true;
    rowsToAdd.push(buildSignedDocumentUploadRow_(fileRow, onboardingId, category, templateId));
  });

  if (!rowsToAdd.length) {
    log_(runId, "INFO", "SIGNED_DOCS_UPLOAD_ROWS_ALREADY_EXIST", { candidates: candidateRows.length });
    return;
  }

  callAppSheetRows_(runId, DOCGEN_TABLES.SIGNED_DOCUMENTS, rowsToAdd, CONFIG.APPSHEET_ACTION_ADD, "batch-signed-doc-upload-rows");
  log_(runId, "INFO", "SIGNED_DOCS_UPLOAD_ROWS_CREATED", { rows: rowsToAdd.length });
}

function findExistingSignedDocumentKeys_(runId, fileRows) {
  const onboardingIds = {};
  (fileRows || []).forEach(row => {
    const id = String(getField_(row, "Onboarding_ID") || "").trim();
    if (id) onboardingIds[id] = true;
  });

  const out = {};
  Object.keys(onboardingIds).forEach(onboardingId => {
    const selector = 'FILTER("' + DOCGEN_TABLES.SIGNED_DOCUMENTS + '", AND(' +
      "[Onboarding_ID] = " + appSheetQuote_(onboardingId) + ", " +
      "[Category] = " + appSheetQuote_(CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY) +
      "))";
    const rows = callAppSheetFind_(runId, DOCGEN_TABLES.SIGNED_DOCUMENTS, selector);
    rows.forEach(row => {
      const existingOnboardingId = String(getField_(row, "Onboarding_ID") || onboardingId).trim();
      const templateId = String(getField_(row, "Template_ID_Reference") || "").trim();
      const category = String(getField_(row, "Category") || CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY).trim();
      if (existingOnboardingId && templateId && category) {
        out[buildSignedDocumentDedupeKey_(existingOnboardingId, templateId, category)] = true;
      }
    });
  });
  return out;
}

function buildSignedDocumentUploadRow_(fileRow, onboardingId, category, templateId) {
  return {
    ID: makeShortId_(),
    Onboarding_ID: onboardingId,
    "File Extension": getField_(fileRow, "File Extension") || getField_(fileRow, "File_Extension") || ".pdf",
    File: "",
    Prefix: getField_(fileRow, "Prefix") || getField_(fileRow, "File_Name_Prefix") || "",
    Category: category,
    Date_Created: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd"),
    File_status: CONFIG.DOC_GENERATOR.SIGNED_DOCUMENT_STATUS_WAITING,
    Template_ID_Reference: templateId
  };
}

function buildSignedDocumentDedupeKey_(onboardingId, templateId, category) {
  return [onboardingId, templateId, category].map(v => String(v || "").trim()).join("|");
}

function makeShortId_() {
  return Utilities.getUuid().replace(/-/g, "").slice(0, 8);
}

function finishMainRowsWhenAllAgreementsReady_(runId, processedFiles, args, readyFileUpdates) {
  if (args.jobId) {
    finishMainRowWhenJobAgreementFilesReady_(runId, args, readyFileUpdates);
    return;
  }

  const onboardingIds = {};
  processedFiles.forEach(row => {
    const id = getField_(row, "Onboarding_ID") || args.onboardingId;
    if (id) onboardingIds[id] = true;
  });

  Object.keys(onboardingIds).forEach(onboardingId => {
    const pendingSelector = 'FILTER("' + DOCGEN_TABLES.AGREEMENTS_FILES + '", AND(' +
      "[Onboarding_ID] = " + appSheetQuote_(onboardingId) + ", " +
      "[Category] = " + appSheetQuote_(CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY) + ", " +
      "[File_status] <> " + appSheetQuote_(CONFIG.DOC_GENERATOR.FILE_STATUS_READY) +
      "))";
    const pending = callAppSheetFind_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, pendingSelector);
    if (pending.length) return;

    callAppSheet_(runId, DOCGEN_TABLES.MAIN, {
      ID: onboardingId,
      Status: CONFIG.DOC_GENERATOR.MAIN_STATUS_AGREEMENTS_GENERATED
    }, CONFIG.APPSHEET_ACTION_EDIT, onboardingId);
  });
}

function finishMainRowWhenJobAgreementFilesReady_(runId, args, readyFileUpdates) {
  const jobId = args && args.jobId || "";
  const fallbackOnboardingId = args && args.onboardingId || "";
  const itemSelector = 'FILTER("' + DOCGEN_TABLES.GENERATION_JOB_ITEMS + '", [Job_ID] = ' + appSheetQuote_(jobId) + ")";
  const items = callAppSheetFind_(runId, DOCGEN_TABLES.GENERATION_JOB_ITEMS, itemSelector);
  if (!items.length) {
    log_(runId, "INFO", "DOCGEN_JOB_NOT_COMPLETE_NO_ITEMS", { jobId: jobId });
    return;
  }

  const readyUpdates = readyFileUpdates || [];
  let readyCount = readyUpdates.length;
  if (readyCount < items.length) {
    const readySelector = 'FILTER("' + DOCGEN_TABLES.AGREEMENTS_FILES + '", AND(' +
      "[Job_ID] = " + appSheetQuote_(jobId) + ", " +
      "[Category] = " + appSheetQuote_(CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY) + ", " +
      "[File_status] = " + appSheetQuote_(CONFIG.DOC_GENERATOR.FILE_STATUS_READY) +
      "))";
    const readyFiles = callAppSheetFind_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, readySelector);
    const readyIds = {};
    readyFiles.forEach(row => {
      const id = getField_(row, "ID");
      if (id) readyIds[id] = true;
    });
    readyUpdates.forEach(row => {
      if (row && row.ID) readyIds[row.ID] = true;
    });
    readyCount = Object.keys(readyIds).length;
  }

  if (readyCount < items.length) {
    log_(runId, "INFO", "DOCGEN_JOB_NOT_COMPLETE", {
      jobId: jobId,
      expectedItems: items.length,
      readyFiles: readyCount
    });
    enqueueDocGenerationJob_(runId, {
      onboardingId: fallbackOnboardingId,
      jobId: jobId,
      agreementFileId: args && args.agreementFileId || ""
    }, { front: true, continuation: true });
    ensureDocGenerationQueueTrigger({ refresh: true });
    log_(runId, "INFO", "DOCGEN_INCOMPLETE_JOB_REQUEUED", {
      jobId: jobId,
      onboardingId: fallbackOnboardingId,
      expectedItems: items.length,
      readyFiles: readyCount
    });
    return;
  }

  const onboardingId = getField_(items[0], "Onboarding_ID") || fallbackOnboardingId;
  if (!onboardingId) {
    log_(runId, "ERROR", "DOCGEN_JOB_COMPLETE_MISSING_ONBOARDING_ID", { jobId: jobId });
    return;
  }

  callAppSheet_(runId, DOCGEN_TABLES.MAIN, {
    ID: onboardingId,
    Status: CONFIG.DOC_GENERATOR.MAIN_STATUS_AGREEMENTS_GENERATED
  }, CONFIG.APPSHEET_ACTION_EDIT, onboardingId);
}

function buildRelativeFilePathFromRow_(fileRow) {
  const folder = String(getField_(fileRow, "Folder_Path") || "").trim();
  const name = String(getField_(fileRow, "File_Name") || "").trim();
  const ext = String(getField_(fileRow, "File Extension") || getField_(fileRow, "File_Extension") || ".pdf").trim() || ".pdf";
  if (!folder || !name) return "";
  return folder + "/" + name + (/\.pdf$/i.test(name) ? "" : ext);
}

function callAppSheet_(runId, tableName, rowPayload, action, rowNum) {
  return callAppSheetRows_(runId, tableName, [rowPayload], action, rowNum);
}

function callAppSheetRows_(runId, tableName, rowsPayload, action, rowNum) {
  const url = CONFIG.APPSHEET_API_URL
    .replace("{appId}", encodeURIComponent(CONFIG.APPSHEET_APP_ID))
    .replace("{table}", encodeURIComponent(tableName));
  const body = {
    Action: action || CONFIG.APPSHEET_ACTION_EDIT,
    Properties: { Locale: "pl-PL", Timezone: Session.getScriptTimeZone() },
    Rows: rowsPayload
  };

  log_(runId, "INFO", "APPSHEET_REQUEST", { tableName: tableName, action: body.Action, rowNum: rowNum, rows: rowsPayload.length });

  const res = UrlFetchApp.fetch(url, {
    method: "post",
    muteHttpExceptions: true,
    contentType: "application/json",
    payload: JSON.stringify(body),
    headers: { ApplicationAccessKey: CONFIG.APPSHEET_ACCESS_KEY },
    timeout: CONFIG.APPSHEET_TIMEOUT_MS
  });

  const httpCode = res.getResponseCode();
  const text = res.getContentText() || "";
  if (httpCode !== 200) {
    throw new Error("AppSheet " + tableName + " httpCode=" + httpCode + " body=" + text.slice(0, 900));
  }
  const parsed = safeJsonParse_(text);
  if (parsed && parsed.Success === false) {
    throw new Error("AppSheet Success=false: " + (parsed.ErrorDescription || parsed.Error || "unknown"));
  }
  return { httpCode: httpCode, parsed: parsed };
}

function callAppSheetFind_(runId, tableName, selector) {
  const url = CONFIG.APPSHEET_API_URL
    .replace("{appId}", encodeURIComponent(CONFIG.APPSHEET_APP_ID))
    .replace("{table}", encodeURIComponent(tableName));
  const body = {
    Action: "Find",
    Properties: {
      Locale: "pl-PL",
      Timezone: Session.getScriptTimeZone(),
      Selector: selector
    },
    Rows: []
  };

  log_(runId, "INFO", "APPSHEET_FIND_REQUEST", { tableName: tableName, selector: selector });

  const res = UrlFetchApp.fetch(url, {
    method: "post",
    muteHttpExceptions: true,
    contentType: "application/json",
    payload: JSON.stringify(body),
    headers: { ApplicationAccessKey: CONFIG.APPSHEET_ACCESS_KEY },
    timeout: CONFIG.APPSHEET_TIMEOUT_MS
  });

  const httpCode = res.getResponseCode();
  const text = res.getContentText() || "";
  if (httpCode !== 200) {
    throw new Error("AppSheet Find " + tableName + " httpCode=" + httpCode + " body=" + text.slice(0, 900));
  }

  const parsed = safeJsonParse_(text);
  if (!parsed) return [];
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.Rows)) return parsed.Rows;
  if (parsed.Success === false) {
    throw new Error("AppSheet Find " + tableName + " Success=false: " + (parsed.ErrorDescription || parsed.Error || "unknown"));
  }
  return [];
}

function findRequiredSingleRow_(runId, tableName, condition, label) {
  const rows = callAppSheetFind_(runId, tableName, 'FILTER("' + tableName + '", ' + condition + ")");
  if (!rows.length) throw new Error("Cannot find " + label + ".");
  return rows[0];
}

function getDocGeneratorOutputRootFolder_() {
  const id = String(CONFIG.DOC_GENERATOR.OUTPUT_ROOT_FOLDER_ID || "").trim();
  if (id) {
    if (!DOCGEN_RUNTIME_CACHE.projectRootFolder) {
      DOCGEN_RUNTIME_CACHE.projectRootFolder = DriveApp.getFolderById(id);
    }
    return DOCGEN_RUNTIME_CACHE.projectRootFolder;
  }
  return null;
}

function ensureFolderPathAndName_(rootFolder, relativePath) {
  const clean = normalizeRelativeDrivePath_(relativePath);
  const parts = clean.split("/").filter(Boolean);
  if (!parts.length) throw new Error("Invalid relative file path: " + relativePath);
  const fileName = parts.pop();

  const pathParts = parts.slice();
  const folderPathKey = pathParts.join("/");
  if (DOCGEN_RUNTIME_CACHE.folderByPath[folderPathKey]) {
    return {
      rootFolder: DOCGEN_RUNTIME_CACHE.folderByPath[pathParts[0]] || rootFolder,
      folder: DOCGEN_RUNTIME_CACHE.folderByPath[folderPathKey],
      fileName: fileName
    };
  }

  let effectiveRoot = rootFolder;
  let folder = rootFolder;
  const firstFolderName = parts.shift();
  if (!firstFolderName) {
    if (!rootFolder) throw new Error("File path has no folder segment: " + relativePath);
    return {
      rootFolder: rootFolder,
      folder: rootFolder,
      fileName: fileName
    };
  }

  if (folder) {
    folder = getRequiredFileRootFolder_(folder, firstFolderName);
  } else {
    folder = findExistingFolderByName_(firstFolderName) || DriveApp.getRootFolder().createFolder(firstFolderName);
  }
  effectiveRoot = folder;

  parts.forEach(part => {
    folder = ensureChildFolder_(folder, part);
  });
  DOCGEN_RUNTIME_CACHE.folderByPath[folderPathKey] = folder;
  DOCGEN_RUNTIME_CACHE.folderByPath[firstFolderName] = effectiveRoot;
  return { rootFolder: effectiveRoot, folder: folder, fileName: fileName };
}

function getRequiredFileRootFolder_(projectRootFolder, folderName) {
  const configuredNames = CONFIG.DOC_GENERATOR.FILE_ROOT_FOLDER_NAMES || [];
  if (configuredNames.indexOf(folderName) >= 0) {
    const existing = projectRootFolder.getFoldersByName(folderName);
    if (!existing.hasNext()) {
      throw new Error("Missing required file root folder under project root: " + folderName);
    }
    return existing.next();
  }
  return ensureChildFolder_(projectRootFolder, folderName);
}

function ensureChildFolder_(parent, name) {
  const clean = String(name || "").trim();
  if (!clean) throw new Error("Blank folder name.");
  const existing = parent.getFoldersByName(clean);
  if (existing.hasNext()) return existing.next();
  return parent.createFolder(clean);
}

function findExistingFolderByName_(name) {
  const folders = DriveApp.getFoldersByName(String(name || "").trim());
  if (folders.hasNext()) return folders.next();
  return null;
}

function trashExistingFilesByName_(folder, fileName) {
  const files = folder.getFilesByName(fileName);
  while (files.hasNext()) {
    files.next().setTrashed(true);
  }
}

function normalizeRelativeDrivePath_(path) {
  return String(path || "").trim().replace(/^\.\/+/, "").replace(/^\/+/, "").replace(/\/{2,}/g, "/");
}

function getField_(row, field) {
  if (!row) return "";
  if (row[field] != null) return row[field];
  const target = normalizeKey_(field);
  const keys = Object.keys(row);
  for (let i = 0; i < keys.length; i++) {
    if (normalizeKey_(keys[i]) === target) return row[keys[i]];
  }
  return "";
}

function appSheetQuote_(value) {
  return '"' + String(value || "").replace(/"/g, '""') + '"';
}

function stringifyDocValue_(value) {
  if (value == null) return "";
  if (typeof value === "string" && value.charAt(0) === "{") {
    const parsed = safeJsonParse_(value);
    if (parsed && typeof parsed === "object") return stringifyDocValue_(parsed);
  }
  if (typeof value === "object" && !(value instanceof Date)) {
    if (value.Url != null) return String(value.Url);
    if (value.URL != null) return String(value.URL);
    if (value.url != null) return String(value.url);
    if (value.LinkText != null && Object.keys(value).length === 1) return String(value.LinkText);
  }
  if (value instanceof Date) {
    return Utilities.formatDate(value, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }
  return String(value);
}

function escapeRegExp_(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
