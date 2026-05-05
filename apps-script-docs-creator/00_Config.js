const CONFIG = {
  LOCK_TIMEOUT_MS: 2000,
  GLOBAL_GENERATION_LOCK_WAIT_MS: 1000,
  APPSHEET_APP_ID: "ebb1aa13-9408-4a7d-8d41-8cb03b9e766f",
  APPSHEET_ACCESS_KEY: "V2-dLnyp-uEkhm-vaJQY-lQAmj-czhAH-xLbR3-dA8Oc-oFxgE",
  APPSHEET_API_URL: "https://www.appsheet.com/api/v2/apps/{appId}/tables/{table}/Action",
  APPSHEET_TIMEOUT_MS: 20000,
  APPSHEET_ACTION_ADD: "Add",
  APPSHEET_ACTION_EDIT: "Edit",
  DOC_GENERATOR: {
    OUTPUT_ROOT_FOLDER_ID: "1iHYmHQCpA4IHEfnjrV7okk3sqjw6MkOd",
    FILE_ROOT_FOLDER_NAMES: ["Files_Agreements_", "Files_Application_"],
    AGREEMENT_CATEGORY: "Agreement",
    FILE_STATUS_SET_UP: "Set Up",
    FILE_STATUS_GENERATING: "Generating",
    FILE_STATUS_GENERATED: "Generated",
    FILE_STATUS_READY: "Ready",
    FILE_STATUS_FAILED: "",
    FILE_PROGRESS_STATUSES_ENABLED: true,
    SIGNED_DOCUMENT_STATUS_WAITING: "Waiting for upload",
    ITEM_STATUS_FILE_CREATED: "Agreement file created",
    MAIN_STATUS_AGREEMENTS_GENERATED: "Agreements Generated",
    OVERWRITE_EXISTING_PDF: true,
    KEEP_WORKING_DOC_COPY: false,
    WORKING_DOCS_FOLDER_NAME: "_Generated_Docs_Source",
    SERIALIZE_JOBS: true,
    JOB_CLAIM_TTL_SECONDS: 900,
    JOB_REQUEUE_MAX_RETRIES: 3,
    QUEUE_KEY: "DOCGEN_QUEUE",
    FILE_TASK_QUEUE_KEY: "DOCGEN_FILE_TASK_QUEUE",
    FINALIZER_QUEUE_KEY: "DOCGEN_FINALIZER_QUEUE",
    QUEUE_TRIGGER_MINUTES: 1,
    FILE_WORKER_PARALLELISM: 2,
    FILE_TASK_REQUEUE_MAX_RETRIES: 2,
    FINALIZER_REQUEUE_MAX_RETRIES: 12,
    FINALIZER_REQUEUE_DELAY_MS: 15000
  }
};

function makeRunId_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyyMMdd-HHmmss") + "-" + Math.random().toString(16).slice(2, 8);
}

function log_(runId, level, event, data) {
  console.log(JSON.stringify({ runId: runId, level: level, event: event, data: data || {} }));
}

function safeJsonParse_(text) {
  try {
    return JSON.parse(String(text || ""));
  } catch (e) {
    return null;
  }
}

function normalizeKey_(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFKC")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ");
}
