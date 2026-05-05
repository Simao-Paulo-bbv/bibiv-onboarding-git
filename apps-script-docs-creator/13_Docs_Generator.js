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

function resetDocGenerationQueuesManual() {
  const runId = makeRunId_();
  const props = PropertiesService.getScriptProperties();
  const keys = [
    CONFIG.DOC_GENERATOR.ACTIVE_JOB_KEY,
    CONFIG.DOC_GENERATOR.QUEUE_KEY,
    CONFIG.DOC_GENERATOR.FILE_TASK_QUEUE_KEY,
    CONFIG.DOC_GENERATOR.FINALIZER_QUEUE_KEY
  ];

  keys.forEach(key => {
    if (key) props.deleteProperty(key);
  });

  const handlers = {
    processNextQueuedDocGenerationJob: true,
    processNextAgreementFileTask: true,
    processNextAgreementFinalizer: true
  };
  let deletedTriggers = 0;
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (!handlers[trigger.getHandlerFunction()]) return;
    ScriptApp.deleteTrigger(trigger);
    deletedTriggers++;
  });

  log_(runId, "WARN", "DOCGEN_MANUAL_RESET_DONE", {
    clearedKeys: keys.filter(Boolean),
    deletedTriggers: deletedTriggers
  });

  return { ok: true, clearedKeys: keys.filter(Boolean), deletedTriggers: deletedTriggers };
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
    const active = getActiveDocGenerationJob_(runId);
    if (active) {
      recoverActiveDocGenerationJob_(runId, active);
      log_(runId, "INFO", "DOCGEN_ACTIVE_JOB_BLOCKS_DISPATCH", active);
      ensureDocGenerationQueueTrigger();
      return { ok: true, busy: true, activeJob: active };
    }

    const args = dequeueDocGenerationJob_(runId);
    if (!args) {
      log_(runId, "INFO", "DOCGEN_QUEUE_EMPTY", {});
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
    setActiveDocGenerationJob_(runId, args);
    enqueueAgreementFinalizer_(runId, args);
    ensureAgreementFinalizerTrigger_();
    log_(runId, "INFO", "DOCGEN_NO_PENDING_FILES_FINALIZER_ENQUEUED", args);
    return { ok: true, dispatched: 0, message: "No pending Agreement files." };
  }

  setActiveDocGenerationJob_(runId, args);
  const dispatched = enqueueAgreementFileTasks_(runId, args, files);
  enqueueAgreementFinalizer_(runId, args);
  ensureAgreementFileWorkerTriggers_(dispatched);
  if (dispatched > 0) {
    processAgreementFileTaskBatch_(runId, "dispatcher-inline");
  }
  ensureAgreementFinalizerTrigger_();

  log_(runId, "INFO", "DOCGEN_DISPATCHED_FILE_TASKS", {
    jobId: args.jobId || "",
    onboardingId: args.onboardingId || "",
    dispatched: dispatched
  });

  return { ok: true, dispatched: dispatched };
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

function docGenerationJobKey_(args) {
  return String(args && (args.jobId || args.onboardingId || args.agreementFileId) || "").trim();
}

function getActiveDocGenerationJob_(runId) {
  const props = PropertiesService.getScriptProperties();
  const active = safeJsonParse_(props.getProperty(CONFIG.DOC_GENERATOR.ACTIVE_JOB_KEY));
  if (!active || !active.key) return null;

  const startedAt = new Date(active.startedAt || 0).getTime();
  const ageSeconds = startedAt ? (Date.now() - startedAt) / 1000 : 0;
  const ttl = Number(CONFIG.DOC_GENERATOR.ACTIVE_JOB_TTL_SECONDS || 1800);
  if (ageSeconds > ttl) {
    props.deleteProperty(CONFIG.DOC_GENERATOR.ACTIVE_JOB_KEY);
    log_(runId, "WARN", "DOCGEN_ACTIVE_JOB_EXPIRED_CLEARED", {
      key: active.key,
      ageSeconds: Math.round(ageSeconds)
    });
    return null;
  }

  return active;
}

function setActiveDocGenerationJob_(runId, args) {
  const key = docGenerationJobKey_(args);
  if (!key) return;
  const active = {
    key: key,
    jobId: args.jobId || "",
    onboardingId: args.onboardingId || "",
    startedAt: new Date().toISOString()
  };
  PropertiesService.getScriptProperties().setProperty(CONFIG.DOC_GENERATOR.ACTIVE_JOB_KEY, JSON.stringify(active));
  log_(runId, "INFO", "DOCGEN_ACTIVE_JOB_SET", active);
}

function clearActiveDocGenerationJob_(runId, args) {
  const key = docGenerationJobKey_(args);
  const props = PropertiesService.getScriptProperties();
  const active = safeJsonParse_(props.getProperty(CONFIG.DOC_GENERATOR.ACTIVE_JOB_KEY));
  if (!active || !active.key || active.key === key) {
    props.deleteProperty(CONFIG.DOC_GENERATOR.ACTIVE_JOB_KEY);
    log_(runId, "INFO", "DOCGEN_ACTIVE_JOB_CLEARED", { key: key || active && active.key || "" });
  }
}

function recoverActiveDocGenerationJob_(runId, active) {
  if (!active || !active.key) return;
  if (agreementFileTaskQueueHasItems_()) {
    ensureAgreementFileWorkerTriggers_(1);
    return;
  }

  const args = {
    jobId: active.jobId || "",
    onboardingId: active.onboardingId || "",
    agreementFileId: ""
  };
  try {
    const files = findPendingAgreementFileRows_(runId, args);
    const missingFiles = files.filter(fileRow => !getGeneratedArtifactFromExistingRow_(fileRow));
    if (!missingFiles.length) {
      enqueueAgreementFinalizer_(runId, args);
      ensureAgreementFinalizerTrigger_();
      return;
    }

    const requeued = enqueueAgreementFileTasks_(runId, args, missingFiles);
    if (requeued > 0) {
      ensureAgreementFileWorkerTriggers_(requeued);
      processAgreementFileTaskBatch_(runId, "active-job-recovery");
    }
    log_(runId, "INFO", "DOCGEN_ACTIVE_JOB_RECOVERY", {
      jobId: args.jobId,
      onboardingId: args.onboardingId,
      missingFiles: missingFiles.length,
      requeued: requeued
    });
  } catch (e) {
    log_(runId, "ERROR", "DOCGEN_ACTIVE_JOB_RECOVERY_FAILED", {
      jobId: args.jobId,
      onboardingId: args.onboardingId,
      error: String(e && e.message || e).slice(0, 900)
    });
  }
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

function processNextAgreementFileTask() {
  const runId = makeRunId_();
  return processAgreementFileTaskBatch_(runId, "trigger");
}

function processAgreementFileTaskBatch_(runId, source) {
  const startedAt = Date.now();
  const maxItems = Math.max(1, Number(CONFIG.DOC_GENERATOR.FILE_WORKER_BATCH_MAX_ITEMS || 7));
  const maxRuntimeMs = Math.max(30000, Number(CONFIG.DOC_GENERATOR.FILE_WORKER_MAX_RUNTIME_MS || 300000));
  const results = [];

  while (results.length < maxItems && Date.now() - startedAt < maxRuntimeMs) {
    const task = dequeueAgreementFileTask_(runId);
    if (!task) {
      log_(runId, "INFO", "DOCGEN_FILE_TASK_QUEUE_EMPTY", {
        source: source || "",
        processed: results.length
      });
      break;
    }

    const result = processSingleAgreementFileTaskWithGuards_(runId, task);
    results.push(result);
  }

  const hasMoreTasks = agreementFileTaskQueueHasItems_();
  if (hasMoreTasks) ensureAgreementFileWorkerTriggers_(1);
  if (!hasMoreTasks) ensureAgreementFinalizerTrigger_();
  log_(runId, "INFO", "DOCGEN_FILE_TASK_BATCH_DONE", {
    source: source || "",
    processed: results.length,
    remaining: readAgreementFileTaskQueue_(PropertiesService.getScriptProperties()).length
  });

  return { ok: results.every(result => result.ok !== false), processed: results.length, results: results };
}

function processSingleAgreementFileTaskWithGuards_(runId, task) {
  let claimKey = "";
  try {
    const active = getActiveDocGenerationJob_(runId);
    if (active && docGenerationJobKey_(task) !== active.key) {
      requeueAgreementFileTask_(runId, task, "active-job-mismatch");
      log_(runId, "INFO", "DOCGEN_FILE_TASK_WAITING_FOR_ACTIVE_JOB", {
        taskKey: docGenerationJobKey_(task),
        activeKey: active.key
      });
      return { ok: true, waiting: true, agreementFileId: task.agreementFileId || "" };
    }

    claimKey = claimAgreementFileTask_(runId, task);
    const result = processAgreementFileTask_(runId, task);
    enqueueAgreementFinalizer_(runId, task);
    return result;
  } catch (err) {
    requeueAgreementFileTaskAfterFailure_(runId, task, err);
    enqueueAgreementFinalizer_(runId, task);
    log_(runId, "ERROR", "DOCGEN_FILE_TASK_FAILED", {
      agreementFileId: task.agreementFileId || "",
      error: String(err && err.message || err).slice(0, 900)
    });
    return { ok: false, agreementFileId: task.agreementFileId || "", error: String(err && err.message || err) };
  } finally {
    releaseDocGenerationJob_(claimKey);
  }
}

function processAgreementFileTask_(runId, task) {
  const fileId = String(task.agreementFileId || "").trim();
  if (!fileId) throw new Error("Missing agreementFileId in file task.");

  const fileRow = findOptionalSingleRow_(
    runId,
    DOCGEN_TABLES.AGREEMENTS_FILES,
    "[ID] = " + appSheetQuote_(fileId),
    "agreement file row " + fileId
  );
  if (!fileRow) {
    log_(runId, "WARN", "DOCGEN_STALE_FILE_TASK_SKIPPED", {
      agreementFileId: fileId,
      jobId: task.jobId || "",
      onboardingId: task.onboardingId || ""
    });
    return { ok: true, stale: true, agreementFileId: fileId };
  }

  const existing = getGeneratedArtifactFromExistingRow_(fileRow);
  if (existing) {
    log_(runId, "INFO", "DOCGEN_FILE_ALREADY_GENERATED", {
      agreementFileId: fileId,
      file: existing.relativePath
    });
    return { ok: true, generated: false, file: existing.relativePath };
  }

  const onboardingId = getField_(fileRow, "Onboarding_ID") || task.onboardingId;
  const templateId = getField_(fileRow, "Template_ID_Reference");
  if (!onboardingId) throw new Error("Missing Onboarding_ID.");
  if (!templateId) throw new Error("Missing Template_ID_Reference.");

  const mainRow = findRequiredSingleRow_(
    runId,
    DOCGEN_TABLES.MAIN,
    "[ID] = " + appSheetQuote_(onboardingId),
    "main onboarding row " + onboardingId
  );
  const templateRow = findRequiredSingleRow_(
    runId,
    DOCGEN_TABLES.DOC_TEMPLATES,
    "[Template_ID] = " + appSheetQuote_(templateId),
    "doc template " + templateId
  );

  markAgreementFileGenerating_(runId, fileRow);
  const generated = generatePdfForAgreementFileRow_(runId, fileRow, mainRow, templateRow);
  markAgreementFileGenerated_(runId, buildAgreementFileGeneratedRow_(fileRow, generated));

  return {
    ok: true,
    generated: true,
    agreementFileId: fileId,
    file: generated.relativePath,
    pdfId: generated.pdfId,
    docId: generated.docId
  };
}

function processNextAgreementFinalizer() {
  const runId = makeRunId_();
  const task = dequeueAgreementFinalizer_(runId);
  if (!task) {
    log_(runId, "INFO", "DOCGEN_FINALIZER_QUEUE_EMPTY", {});
    return { ok: true, empty: true };
  }

  const result = finalizeAgreementJobIfComplete_(runId, task);
  if (!result.complete && !result.giveUp) {
    requeueAgreementFinalizer_(runId, task);
  }
  if (agreementFinalizerQueueHasItems_()) ensureAgreementFinalizerTrigger_();
  return result;
}

function finalizeAgreementJobIfComplete_(runId, task) {
  const jobId = String(task.jobId || "").trim();
  const onboardingId = String(task.onboardingId || "").trim();
  if (!jobId && !onboardingId) throw new Error("Finalizer requires jobId or onboardingId.");

  const files = findAllAgreementFileRowsForJob_(runId, task);
  const items = jobId ? findGenerationJobItemsForJob_(runId, jobId) : [];
  const expected = items.length || files.length;
  const readyFileUpdates = [];
  const readyJobItemUpdates = [];

  files.forEach(fileRow => {
    const artifact = getGeneratedArtifactFromExistingRow_(fileRow);
    if (!artifact) return;
    readyFileUpdates.push(buildAgreementFileReadyRow_(fileRow, artifact));
    readyJobItemUpdates.push(buildGenerationJobItemCreatedRow_(fileRow));
  });

  if (!expected || readyFileUpdates.length < expected) {
    const missingFiles = files.filter(fileRow => !getGeneratedArtifactFromExistingRow_(fileRow));
    if (missingFiles.length) {
      const requeued = enqueueAgreementFileTasks_(runId, task, missingFiles);
      if (requeued > 0) {
        ensureAgreementFileWorkerTriggers_(requeued);
        processAgreementFileTaskBatch_(runId, "finalizer-recovery");
      }
      log_(runId, "INFO", "DOCGEN_FINALIZER_REQUEUED_MISSING_FILE_TASKS", {
        jobId: jobId,
        onboardingId: onboardingId,
        missingFiles: missingFiles.length,
        requeued: requeued
      });
    }

    const retryCount = Number(task.finalizerRetryCount || 0) + 1;
    const maxRetries = Number(CONFIG.DOC_GENERATOR.FINALIZER_REQUEUE_MAX_RETRIES || 12);
    const giveUp = retryCount > maxRetries;
    log_(runId, giveUp ? "ERROR" : "INFO", giveUp ? "DOCGEN_FINALIZER_GIVE_UP" : "DOCGEN_FINALIZER_WAITING", {
      jobId: jobId,
      onboardingId: onboardingId,
      expectedItems: expected,
      generatedFiles: readyFileUpdates.length,
      retryCount: retryCount
    });
    task.finalizerRetryCount = retryCount;
    if (giveUp) clearActiveDocGenerationJob_(runId, task);
    return { ok: !giveUp, complete: false, giveUp: giveUp };
  }

  createSignedDocumentUploadRowsForReadyAgreements_(runId, files, readyFileUpdates);
  batchMarkAgreementFilesReady_(runId, readyFileUpdates);
  batchMarkGenerationJobItemsCreated_(runId, readyJobItemUpdates);
  finishMainRowsWhenAllAgreementsReady_(runId, files, task, readyFileUpdates);

  log_(runId, "INFO", "DOCGEN_FINALIZED_JOB", {
    jobId: jobId,
    onboardingId: onboardingId,
    readyFiles: readyFileUpdates.length
  });

  clearActiveDocGenerationJob_(runId, task);
  if (queueHasItems_()) ensureDocGenerationQueueTrigger();

  return { ok: true, complete: true, readyFiles: readyFileUpdates.length };
}

function enqueueAgreementFileTasks_(runId, args, files) {
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readAgreementFileTaskQueue_(props);
    const queuedIds = {};
    queue.forEach(item => {
      if (item.agreementFileId) queuedIds[String(item.agreementFileId)] = true;
    });

    let added = 0;
    (files || []).forEach(fileRow => {
      const fileId = String(getField_(fileRow, "ID") || "").trim();
      if (!fileId || queuedIds[fileId]) return;
      const existing = getGeneratedArtifactFromExistingRow_(fileRow);
      if (existing) return;
      queuedIds[fileId] = true;
      queue.push({
        onboardingId: getField_(fileRow, "Onboarding_ID") || args.onboardingId || "",
        jobId: getField_(fileRow, "Job_ID") || args.jobId || "",
        agreementFileId: fileId,
        queuedAt: new Date().toISOString()
      });
      added++;
    });

    writeAgreementFileTaskQueue_(props, queue);
    log_(runId, "INFO", "DOCGEN_FILE_TASK_QUEUE_STATE", { size: queue.length, added: added });
    return added;
  } finally {
    lock.releaseLock();
  }
}

function dequeueAgreementFileTask_(runId) {
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readAgreementFileTaskQueue_(props);
    const next = queue.shift();
    writeAgreementFileTaskQueue_(props, queue);
    log_(runId, "INFO", "DOCGEN_FILE_TASK_DEQUEUE", { remaining: queue.length, next: next || null });
    return next || null;
  } finally {
    lock.releaseLock();
  }
}

function readAgreementFileTaskQueue_(props) {
  return safeJsonParse_(props.getProperty(CONFIG.DOC_GENERATOR.FILE_TASK_QUEUE_KEY)) || [];
}

function writeAgreementFileTaskQueue_(props, queue) {
  props.setProperty(CONFIG.DOC_GENERATOR.FILE_TASK_QUEUE_KEY, JSON.stringify(queue || []));
}

function agreementFileTaskQueueHasItems_() {
  return readAgreementFileTaskQueue_(PropertiesService.getScriptProperties()).length > 0;
}

function claimAgreementFileTask_(runId, task) {
  const key = "DOCGEN_FILE_CLAIM_" + String(task.agreementFileId || "unknown");
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const cache = CacheService.getScriptCache();
    if (cache.get(key)) {
      log_(runId, "INFO", "DOCGEN_FILE_ALREADY_RUNNING", { key: key });
      throw new Error("Document file generation already running for this file.");
    }
    cache.put(key, "1", CONFIG.DOC_GENERATOR.JOB_CLAIM_TTL_SECONDS || 900);
    return key;
  } finally {
    lock.releaseLock();
  }
}

function requeueAgreementFileTaskAfterFailure_(runId, task, err) {
  const retryCount = Number(task.retryCount || 0) + 1;
  const maxRetries = Number(CONFIG.DOC_GENERATOR.FILE_TASK_REQUEUE_MAX_RETRIES || 2);
  if (retryCount > maxRetries) {
    log_(runId, "ERROR", "DOCGEN_FILE_TASK_REQUEUE_LIMIT_REACHED", {
      agreementFileId: task.agreementFileId || "",
      retryCount: retryCount,
      error: String(err && err.message || err).slice(0, 900)
    });
    return;
  }

  task.retryCount = retryCount;
  task.retryAfterError = String(err && err.message || err).slice(0, 300);
  requeueAgreementFileTask_(runId, task, "after-error");
  log_(runId, "ERROR", "DOCGEN_FILE_TASK_REQUEUED_AFTER_ERROR", {
    agreementFileId: task.agreementFileId || "",
    retryCount: retryCount,
    error: String(err && err.message || err).slice(0, 900)
  });
}

function requeueAgreementFileTask_(runId, task, reason) {
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readAgreementFileTaskQueue_(props);
    const exists = queue.some(item => String(item.agreementFileId || "") === String(task.agreementFileId || ""));
    if (!exists) {
      queue.push(task);
      writeAgreementFileTaskQueue_(props, queue);
    }
    log_(runId, "INFO", "DOCGEN_FILE_TASK_REQUEUED", {
      agreementFileId: task.agreementFileId || "",
      reason: reason || ""
    });
  } finally {
    lock.releaseLock();
  }
}

function enqueueAgreementFinalizer_(runId, args) {
  const key = args.jobId || args.onboardingId;
  if (!key) return;

  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readAgreementFinalizerQueue_(props);
    const exists = queue.some(item => (item.jobId || item.onboardingId) === key);
    if (!exists) {
      queue.push({
        onboardingId: args.onboardingId || "",
        jobId: args.jobId || "",
        queuedAt: new Date().toISOString(),
        finalizerRetryCount: args.finalizerRetryCount || 0
      });
      writeAgreementFinalizerQueue_(props, queue);
    }
    log_(runId, "INFO", "DOCGEN_FINALIZER_QUEUE_STATE", { size: queue.length });
  } finally {
    lock.releaseLock();
  }
}

function dequeueAgreementFinalizer_(runId) {
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readAgreementFinalizerQueue_(props);
    const next = queue.shift();
    writeAgreementFinalizerQueue_(props, queue);
    log_(runId, "INFO", "DOCGEN_FINALIZER_DEQUEUE", { remaining: queue.length, next: next || null });
    return next || null;
  } finally {
    lock.releaseLock();
  }
}

function requeueAgreementFinalizer_(runId, task) {
  const lock = LockService.getScriptLock();
  lock.waitLock(CONFIG.LOCK_TIMEOUT_MS || 2000);
  try {
    const props = PropertiesService.getScriptProperties();
    const queue = readAgreementFinalizerQueue_(props);
    const key = task.jobId || task.onboardingId;
    const exists = queue.some(item => (item.jobId || item.onboardingId) === key);
    if (!exists) {
      queue.push(task);
      writeAgreementFinalizerQueue_(props, queue);
    }
    log_(runId, "INFO", "DOCGEN_FINALIZER_REQUEUED", {
      jobId: task.jobId || "",
      onboardingId: task.onboardingId || "",
      retryCount: task.finalizerRetryCount || 0
    });
  } finally {
    lock.releaseLock();
  }
}

function readAgreementFinalizerQueue_(props) {
  return safeJsonParse_(props.getProperty(CONFIG.DOC_GENERATOR.FINALIZER_QUEUE_KEY)) || [];
}

function writeAgreementFinalizerQueue_(props, queue) {
  props.setProperty(CONFIG.DOC_GENERATOR.FINALIZER_QUEUE_KEY, JSON.stringify(queue || []));
}

function agreementFinalizerQueueHasItems_() {
  return readAgreementFinalizerQueue_(PropertiesService.getScriptProperties()).length > 0;
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

function ensureAgreementFileWorkerTriggers_(taskCount) {
  const handler = "processNextAgreementFileTask";
  const parallelism = Math.max(1, Number(CONFIG.DOC_GENERATOR.FILE_WORKER_PARALLELISM || 2));
  const count = Math.min(Math.max(1, Number(taskCount || 1)), parallelism);
  for (let i = 0; i < count; i++) {
    ScriptApp.newTrigger(handler).timeBased().after(1).create();
  }
}

function ensureAgreementFinalizerTrigger_() {
  ScriptApp.newTrigger("processNextAgreementFinalizer")
    .timeBased()
    .after(CONFIG.DOC_GENERATOR.FINALIZER_REQUEUE_DELAY_MS || 60000)
    .create();
}

function countTriggersForHandler_(handler) {
  return ScriptApp.getProjectTriggers().filter(trigger => trigger.getHandlerFunction() === handler).length;
}

function findPendingAgreementFileRows_(runId, args) {
  const parts = [
    "IN([File_status], LIST(" +
      appSheetQuote_(CONFIG.DOC_GENERATOR.FILE_STATUS_SET_UP) + ", " +
      appSheetQuote_(CONFIG.DOC_GENERATOR.FILE_STATUS_GENERATING) + ", " +
      appSheetQuote_(CONFIG.DOC_GENERATOR.FILE_STATUS_GENERATED) + ", " +
      appSheetQuote_(CONFIG.DOC_GENERATOR.FILE_STATUS_READY) +
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

function findAllAgreementFileRowsForJob_(runId, args) {
  const parts = [
    "[Category] = " + appSheetQuote_(CONFIG.DOC_GENERATOR.AGREEMENT_CATEGORY)
  ];

  if (args.jobId) {
    parts.push("[Job_ID] = " + appSheetQuote_(args.jobId));
  } else if (args.onboardingId) {
    parts.push("[Onboarding_ID] = " + appSheetQuote_(args.onboardingId));
  } else {
    throw new Error("Pass Job_ID or Onboarding_ID.");
  }

  const selector = 'FILTER("' + DOCGEN_TABLES.AGREEMENTS_FILES + '", AND(' + parts.join(", ") + "))";
  return callAppSheetFind_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, selector);
}

function findGenerationJobItemsForJob_(runId, jobId) {
  const selector = 'FILTER("' + DOCGEN_TABLES.GENERATION_JOB_ITEMS + '", [Job_ID] = ' + appSheetQuote_(jobId) + ")";
  return callAppSheetFind_(runId, DOCGEN_TABLES.GENERATION_JOB_ITEMS, selector);
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
  if (!row || !row.ID) return;
  if (!CONFIG.DOC_GENERATOR.FILE_PROGRESS_STATUSES_ENABLED) {
    callAppSheet_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, {
      ID: row.ID,
      File: row.File || ""
    }, CONFIG.APPSHEET_ACTION_EDIT, row.ID);
    return;
  }
  try {
    callAppSheet_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, row, CONFIG.APPSHEET_ACTION_EDIT, row.ID);
    log_(runId, "INFO", "DOCGEN_FILE_GENERATED", {
      id: row.ID,
      status: row.File_status || "",
      file: row.File || ""
    });
  } catch (e) {
    log_(runId, "WARN", "DOCGEN_FILE_GENERATED_STATUS_UPDATE_FAILED_FALLING_BACK_TO_FILE_ONLY", {
      id: row.ID,
      status: row.File_status || "",
      error: String(e && e.message || e).slice(0, 900)
    });
    callAppSheet_(runId, DOCGEN_TABLES.AGREEMENTS_FILES, {
      ID: row.ID,
      File: row.File || ""
    }, CONFIG.APPSHEET_ACTION_EDIT, row.ID);
  }
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
  if (!file) return null;
  if (!generatedDriveFileExists_(file)) return null;
  return { relativePath: file, pdfId: "", docId: "" };
}

function generatedDriveFileExists_(relativePath) {
  try {
    const outputRoot = getDocGeneratorOutputRootFolder_();
    const resolved = findExistingFolderPathAndName_(outputRoot, relativePath);
    if (!resolved || !resolved.folder || !resolved.fileName) return false;
    return resolved.folder.getFilesByName(resolved.fileName).hasNext();
  } catch (e) {
    return false;
  }
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
  const row = findOptionalSingleRow_(runId, tableName, condition, label);
  if (!row) throw new Error("Cannot find " + label + ".");
  return row;
}

function findOptionalSingleRow_(runId, tableName, condition, label) {
  const rows = callAppSheetFind_(runId, tableName, 'FILTER("' + tableName + '", ' + condition + ")");
  if (!rows.length) {
    log_(runId, "INFO", "APPSHEET_OPTIONAL_ROW_NOT_FOUND", {
      tableName: tableName,
      label: label || "",
      condition: condition || ""
    });
    return null;
  }
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

function findExistingFolderPathAndName_(rootFolder, relativePath) {
  const clean = normalizeRelativeDrivePath_(relativePath);
  const parts = clean.split("/").filter(Boolean);
  if (!parts.length) return null;
  const fileName = parts.pop();
  const firstFolderName = parts.shift();
  if (!firstFolderName || !fileName) return null;

  let folder = null;
  if (rootFolder) {
    const roots = rootFolder.getFoldersByName(firstFolderName);
    if (!roots.hasNext()) return null;
    folder = roots.next();
  } else {
    folder = findExistingFolderByName_(firstFolderName);
    if (!folder) return null;
  }

  for (let i = 0; i < parts.length; i++) {
    const children = folder.getFoldersByName(parts[i]);
    if (!children.hasNext()) return null;
    folder = children.next();
  }

  return { folder: folder, fileName: fileName };
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
