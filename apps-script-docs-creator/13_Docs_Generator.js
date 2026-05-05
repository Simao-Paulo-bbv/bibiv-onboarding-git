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
  folderByPath: {}
};

function generateAgreementFilesFromAppSheet(onboardingId, jobId, agreementFileId) {
  const runId = makeRunId_();
  const args = {
    onboardingId: String(onboardingId || "").trim(),
    jobId: String(jobId || "").trim(),
    agreementFileId: String(agreementFileId || "").trim()
  };

  enqueueDocGenerationJob_(runId, args);
  ensureDocGenerationQueueTrigger();
  log_(runId, "INFO", "DOCGEN_ENQUEUED", args);
  return { ok: true, queued: true, jobId: args.jobId };
}

function processNextQueuedDocGenerationJob() {
  const runId = makeRunId_();
  const globalLock = tryAcquireGlobalGenerationLock_(runId);
  if (!globalLock) {
    log_(runId, "INFO", "DOCGEN_WORKER_BUSY", {});
    return { ok: true, busy: true };
  }

  let claimKey = "";
  try {
    const args = dequeueDocGenerationJob_(runId);
    if (!args) {
      log_(runId, "INFO", "DOCGEN_QUEUE_EMPTY", {});
      deleteTriggersForHandler_("processNextQueuedDocGenerationJob");
      return { ok: true, empty: true };
    }

    claimKey = claimDocGenerationJob_(runId, args);
    let result;
    try {
      result = processDocGenerationJob_(runId, args);
    } catch (err) {
      requeueDocGenerationJobAfterFailure_(runId, args, err);
      throw err;
    }
    if (queueHasItems_()) ensureDocGenerationQueueTrigger();
    return result;
  } finally {
    releaseDocGenerationJob_(claimKey);
    releaseGlobalGenerationLock_(globalLock);
  }
}

function processDocGenerationJob_(runId, args) {
  log_(runId, "INFO", "DOCGEN_START", args);

  const files = findPendingAgreementFileRows_(runId, args);
  if (!files.length) {
    log_(runId, "INFO", "DOCGEN_NO_PENDING_FILES", args);
    return { ok: true, processed: 0, message: "No pending Agreement files." };
  }

  const maxFiles = Math.max(1, Number(CONFIG.DOC_GENERATOR.MAX_FILES_PER_RUN || 3));
  const filesToProcess = files.slice(0, maxFiles);
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
        mainRowsById[fileOnboardingId] = findRequiredSingleRow_(
          runId,
          DOCGEN_TABLES.MAIN,
          "[ID] = " + appSheetQuote_(fileOnboardingId),
          "main onboarding row " + fileOnboardingId
        );
      }

      if (!templateRowsById[templateId]) throw new Error("Cannot find doc template " + templateId + ".");

      let generated = getGeneratedArtifactFromExistingRow_(fileRow);
      if (!generated) {
        markAgreementFileGenerating_(runId, fileRow);
        generated = generatePdfForAgreementFileRow_(
          runId,
          fileRow,
          mainRowsById[fileOnboardingId],
          templateRowsById[templateId]
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
  finishMainRowsWhenAllAgreementsReady_(runId, filesToProcess, args, readyFileUpdates);

  if (shouldContinue) {
    enqueueDocGenerationJob_(runId, args);
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
    continuation: shouldContinue
  });

  return {
    ok: failed.length === 0,
    processed: results.length,
    failed: failed.length,
    continuation: shouldContinue,
    results: results
  };
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
      throw new Error("Document generation already running for this job.");
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

function enqueueDocGenerationJob_(runId, args) {
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readDocGenerationQueue_(props);
    const key = args.jobId || args.agreementFileId || args.onboardingId;
    if (!key) throw new Error("Cannot enqueue doc generation without jobId/onboardingId/agreementFileId.");

    const exists = queue.some(item => (item.jobId || item.agreementFileId || item.onboardingId) === key);
    if (!exists) {
      queue.push({
        onboardingId: args.onboardingId,
        jobId: args.jobId,
        agreementFileId: args.agreementFileId,
        queuedAt: new Date().toISOString()
      });
      writeDocGenerationQueue_(props, queue);
    }
    log_(runId, "INFO", "DOCGEN_QUEUE_STATE", { size: queue.length });
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

function ensureDocGenerationQueueTrigger() {
  const handler = "processNextQueuedDocGenerationJob";
  const exists = ScriptApp.getProjectTriggers().some(trigger => trigger.getHandlerFunction() === handler);
  if (exists) return;
  ScriptApp.newTrigger(handler)
    .timeBased()
    .everyMinutes(CONFIG.DOC_GENERATOR.QUEUE_TRIGGER_MINUTES || 1)
    .create();
}

function deleteTriggersForHandler_(handler) {
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (trigger.getHandlerFunction() === handler) ScriptApp.deleteTrigger(trigger);
  });
}

function findPendingAgreementFileRows_(runId, args) {
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
    if (!id || seen[id]) return;
    seen[id] = true;
    ids.push(id);
  });

  if (!ids.length) return {};

  const selector = 'FILTER("' + DOCGEN_TABLES.DOC_TEMPLATES + '", OR(' +
    ids.map(id => "[Template_ID] = " + appSheetQuote_(id)).join(", ") +
    "))";
  const rows = callAppSheetFind_(runId, DOCGEN_TABLES.DOC_TEMPLATES, selector);
  const out = {};
  rows.forEach(row => {
    const id = getField_(row, "Template_ID");
    if (id) out[id] = row;
  });
  return out;
}

function generatePdfForAgreementFileRow_(runId, fileRow, mainRow, templateRow) {
  const templateId = getField_(templateRow, "Template_ID");
  if (!templateId) throw new Error("Doc_Templates.Template_ID is blank.");

  const relativePath = normalizeRelativeDrivePath_(getField_(fileRow, "File") || buildRelativeFilePathFromRow_(fileRow));
  if (!relativePath) throw new Error("Agreements_Files.File is blank and cannot be rebuilt.");

  const outputRoot = getDocGeneratorOutputRootFolder_();
  const output = ensureFolderPathAndName_(outputRoot, relativePath);
  const workingFolder = ensureChildFolder_(output.rootFolder, CONFIG.DOC_GENERATOR.WORKING_DOCS_FOLDER_NAME);

  if (CONFIG.DOC_GENERATOR.OVERWRITE_EXISTING_PDF) {
    trashExistingFilesByName_(output.folder, output.fileName);
  }

  const sourceFile = DriveApp.getFileById(templateId);
  const docName = output.fileName.replace(/\.pdf$/i, "");
  const docCopy = sourceFile.makeCopy(docName, workingFolder);
  const doc = DocumentApp.openById(docCopy.getId());

  replaceTemplatePlaceholders_(doc, mainRow, fileRow, templateRow);
  const exportTabId = getFirstDocumentTabId_(doc);
  doc.saveAndClose();

  const pdfBlob = exportGoogleDocTabToPdf_(docCopy.getId(), exportTabId, output.fileName);
  const pdfFile = output.folder.createFile(pdfBlob);

  if (!CONFIG.DOC_GENERATOR.KEEP_WORKING_DOC_COPY) {
    docCopy.setTrashed(true);
  }

  log_(runId, "INFO", "DOCGEN_FILE_CREATED", {
    agreementFileId: getField_(fileRow, "ID"),
    templateId: templateId,
    relativePath: relativePath,
    pdfId: pdfFile.getId(),
    docId: docCopy.getId()
  });

  return {
    relativePath: relativePath,
    pdfId: pdfFile.getId(),
    pdfUrl: pdfFile.getUrl(),
    docId: docCopy.getId(),
    docUrl: docCopy.getUrl()
  };
}

function replaceTemplatePlaceholders_(doc, mainRow, fileRow, templateRow) {
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

  replaceAppSheetIfExpressionsInTextNodes_(sections, context);
  replaceAppSheetIfExpressions_(sections, context);
  replaceKnownAppSheetExpressions_(sections, context);
  replaceAppSheetTodayExpressions_(sections);
  replaceAppSheetReferencesInTextNodes_(sections, context);
  replaceAppSheetReferences_(sections, context, values);

  Object.keys(values).forEach(key => {
    const value = stringifyDocValue_(values[key]);
    sections.forEach(section => {
      section.replaceText(escapeRegExp_("{{" + key + "}}"), value);
      section.replaceText(escapeRegExp_("<<[" + key + "]>>"), value);
    });
  });

  replaceDereferencedValues_(sections, "Onboarding_ID", mainRow);
  replaceDereferencedValues_(sections, "Template_ID_Reference", templateRow);
  replaceDereferencedValues_(sections, "Template_ID", templateRow);
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

function replaceDereferencedValues_(sections, refName, row) {
  if (!row) return;
  Object.keys(row).forEach(key => {
    const value = stringifyDocValue_(row[key]);
    sections.forEach(section => {
      section.replaceText(escapeRegExp_("<<[" + refName + "].[" + key + "]>>"), value);
      section.replaceText(escapeRegExp_("<<[" + refName + "]." + key + ">>"), value);
      section.replaceText(escapeRegExp_("{{" + refName + "." + key + "}}"), value);
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
  markAgreementFileProgressStatus_(runId, buildAgreementFileGeneratingRow_(fileRow), "DOCGEN_FILE_GENERATING");
}

function markAgreementFileGenerated_(runId, row) {
  if (!CONFIG.DOC_GENERATOR.FILE_PROGRESS_STATUSES_ENABLED) return;
  markAgreementFileProgressStatus_(runId, row, "DOCGEN_FILE_GENERATED");
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
    finishMainRowWhenJobAgreementFilesReady_(runId, args.jobId, args.onboardingId, readyFileUpdates);
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

function finishMainRowWhenJobAgreementFilesReady_(runId, jobId, fallbackOnboardingId, readyFileUpdates) {
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
  if (!firstFolderName) throw new Error("File path has no folder segment: " + relativePath);

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
