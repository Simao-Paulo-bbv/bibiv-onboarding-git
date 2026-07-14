# Apps Script Reference

The Apps Script source is clasp-managed. Push with `clasp push`.

## File map (numeric prefixes are the file ordering convention; rename-safe)

### `00_Config.js` — central config
- `CONFIG` — all runtime constants (spreadsheet IDs, timeouts, feature toggles).
- `HEADER_ALIASES` — Squarespace column rename map. **Aliases are parallel** — either form is read; renames in Squarespace don't break the script.
- `APPSHEET_SCHEMA` — 47 columns. New columns are always appended to the end.
- `DEST_SCHEMA` — DEST sheet schema; same additivity rule.
- `APPSHEET_MAIN_ALLOWED_COLS` — payload allowlist enforced by `filterPayloadForAppSheet_`.
- `SYSTEM_DEFAULTS` — fallback values for required fields.
- AppSheet creds:
  - `APPSHEET_APP_ID = "ebb1aa13-9408-4a7d-8d41-8cb03b9e766f"`
  - `APPSHEET_TABLE_MAIN = "BIBIV_onboarding_APP"`
  - `APPSHEET_TABLE_PEOPLE = "People_List"`
  - `MAIN_REF_COLS = { CONTACT, MANAGER, BENEFICIAL }`
  - `MARKERS = { MAIN_OK: "APPSHEET_OK", PEOPLE_OK: "PEOPLE_OK" }`
- Toggles to know:
  - `WRITE_STATUS_JUST_IN_TIME = true` — never overwrite AppSheet-managed Status
  - `SOURCE_REIMPORT_IF_MISSING_IN_DEST = true` — auto-heal if a DEST row was deleted
  - `NIP_AS_NUMBER` — controls whether NIP/NIP_Control are sent as Number or String
  - `PENDING_SCAN_LAST_N = 5000` — fallback scan window for pending DEST rows
  - `BACKFILL_*` toggles in `12_Backfill_Existing.js` — turn off after backfill runs
  - Docs Creator toggles in `apps-script-docs-creator/00_Config.js`:
    - `USE_SHEET_READS=false` — production generator reads through AppSheet, not directly from the Google Sheet.
    - `USE_DOCS_API_PLACEHOLDER_REPLACEMENT=true` — placeholder replacement uses Google Docs API first, with `DocumentApp` fallback.

### `01_Entry.js`
- `runSyncAndProcess()` — entrypoint. Acquires lock, opens sheets, runs `importFromSource_`, then processes freshly imported rows. Falls back to `processPendingDestRows_` over the last N rows.
- `installTimeTriggerEveryMinute()` — installs the trigger.

### `02_Headers_Mapping.js`
- Resolves headers in SOURCE through `HEADER_ALIASES`. Either alias wins — protects against Squarespace form-field renames.

### `03_Import.js`
- `importFromSource_` with two-layer dedupe:
  1. `buildDestDedupeIndex_` — in-memory `(NIP, SubmittedOn)` set from current DEST.
  2. `_Import_History` — durable hidden sheet, survives DEST row deletions.
- Source markers: `IN_DEST`, `DONE`. Auto-heal on missing DEST when `SOURCE_REIMPORT_IF_MISSING_IN_DEST=true`.
- All history-sheet I/O is non-blocking on errors.

### `04_Process.js` — pipeline core
Order per row:
1. Assign `Onboarding_ID` (skipped if already present).
2. **MF enrichment in REGON → VAT → IBAN order** (`06_MF_API.js`).
3. `Bank_Accounts` child sync (`11_Bank_Accounts.js`).
4. People refs (`ensurePeopleRefsForRow_`) — Contact / Manager / Beneficial.
5. AppSheet Add or Edit.

Critical guards:
- `hasExternalManagedStatus(row)` — skip if AppSheet already owns Status.
- `liveExternalManaged(row)` — re-check immediately before Add (race window).
- `evaluateMfReadinessForAdd_` — gate: must have `statusVat` + `name_api` + `regon` + `working/residence`. Not-VAT branch fast-paths through with sparse data.
- `appendSyncMarker_` — append-only marker concatenation, preserves `APPSHEET_OK` token.

### `05_AppSheet_API.js`
- `filterPayloadForAppSheet_(payload)` — allowlist by `APPSHEET_MAIN_ALLOWED_COLS`.
- `callAppSheet_(table, action, row)`:
  - URL: `https://www.appsheet.com/api/v2/apps/${APPSHEET_APP_ID}/tables/${table}/Action`
  - Header: `ApplicationAccessKey`
  - Body: `{ Action: "Add"|"Edit", Properties: { Locale: "pl-PL", Timezone }, Rows: [row] }`
- `isAppSheetSchemaMismatchBody_` — detects "mismatch in number of columns" / "regenerate the table column structure" responses → marker `APPSHEET_SCHEMA_MISMATCH`.

### `06_MF_API.js`
- Order: **REGON (by NIP, mandatory) → VAT → IBAN → KNF/RPK**.
- Endpoints under `gov.api.hypnotype.com`.
- VAT retry strategy: today's date, then no date.
- Not-VAT path: `subject:null` from REGON → write `statusVat="Not VAT"`, build `residenceAddress` from REGON: `"Ulica NrNier NrNier/NrLok, KodPocztowy Miejscowosc"`. Skip VAT/IBAN.
- IBAN cache: `CacheService.getScriptCache()` keyed by account number. Skipped entirely if full bank meta already on row.
- IBAN skipped on hard REGON/VAT error.
- VAT calls use GOV API only; no direct MF/relay fallback remains in the Apps Script.
- Manual bank metadata repair lives in `14_Manual_Maintenance.js`: run `runManualRefreshIbanBankMetadata()` to refill Google Sheet rows with missing IBAN-derived fields, or `runManualRefreshAllIbanBankMetadata()` to force a fresh IBAN API check for all Google Sheet rows with an onboarding ID and bank account. It updates only `swift/bic`, `Bank name`, `Bank address`, and `Bank city`. `kod swift banku` is a source/form field and is not written by IBAN API helpers. Use `resetManualIbanRefreshCursor()` if the forced all-row refresh should start again from the top.
- KNF/RPK verification uses GOV API `/v1/knf/rpk?nip=...`, writes append-only `KNF_verified`, and is non-blocking. Manual helpers: `runManualRefreshKnfVerified()`, `runManualRefreshKnfVerifiedForceAll()`, `runManualRefreshKnfVerifiedForNips()` with `MANUAL_KNF_VERIFICATION.NIPS_TEXT` / `NIPS`, and `runManualRefreshKnfVerifiedForNipsText(nipsText, overwriteExisting)` for ad-hoc targeted reruns.
- Manual People ref repair also lives in `14_Manual_Maintenance.js`: run `runManualAuditPeopleRefsFromPeopleList()` first, then `runManualRepairPeopleRefsFromPeopleList()` to copy existing `People_List[PersonID]` values back into `ContactPersonID`, `ManagerPersonID`, and `BeneficialOwnerPersonID` in the main Google Sheet. It reads `People_List`; it does not create or parse people. Matching is by `OnboardingID + Role`, with a name check against the main row.

### `07_Payload_And_Normalization.js`
- `buildAppSheetPayloadFromDest_(dest, rowNum, action)`:
  - Date format: dates → `yyyy-MM-dd`; `submitted on` → `yyyy-MM-dd HH:mm:ss`.
  - NIP/KRS/REGON normalization with leading-zero preservation.
  - Phone digits-only; `(null) null-null` → empty.
  - `accountNumbers` normalized + deduped CSV.
  - Website fallback `https://no-website.invalid/` only on Add.
  - Email fallback `noemail+<nipDigits>@bibiv.invalid` only on Add (when contact email blank).
  - Birth-date fields preserve true blanks (`null`) — empty string would trigger AppSheet default-value behavior.
  - `dropOptionalEmptyPayloadFields_` — drops `pesel*` and `numer dowodu beneficjenta` when blank/`-`/`//`.
  - Status stripped on Edit (caller responsibility verified at the call site).

### `08_ID_And_Dedupe_Time.js`
- Onboarding_ID assignment.
- Submitted-on canonicalization (handles Date object + string variants from Squarespace).

### `09_Logging.js`
- Sync-status marker concat. Latest marker first, `APPSHEET_OK` token preserved across appends.

### `10_Sheet_And_Header_Utils.js`
- `repairDestHeadersOnlyAfterQueueInsert` — **non-destructive**. Never truncates. New columns are always appended; existing column order is sacred.
- Hidden backup logic for header repairs.

### `11_Bank_Accounts.js`
- Child table `Bank_Accounts` (`AccountID, Onboarding_ID, AccountNumber, CreatedAt`).
- Source priority: `accountNumbers` (from MF) → `numer rachunku bankowego` (form fallback).
- Skip-unchanged logic + runtime cache. Never touches `sync_status`.

### `12_Backfill_Existing.js`
- One-shot enrichment for legacy rows. Only fills blanks. Does **not** touch `Status`.
- Toggle off after run.

### `apps-script-docs-creator/13_Docs_Generator.js`
- Standalone agreement PDF generator called by AppSheet through `generateAgreementFilesFromAppSheet(onboardingId, jobId, agreementFileId)`.
- AppSheet call only enqueues; `processNextQueuedDocGenerationJob()` does the real work from a time-driven worker.
- Queue dedupes by `Job_ID`. Duplicate calls for the same job log `added:false`.
- Per-job claim guard logs `DOCGEN_ALREADY_RUNNING` and exits cleanly if a duplicate worker wakes for the same job.
- Production data reads are through AppSheet API. The optional Google Sheet read path remains behind `USE_SHEET_READS=false`.
- Placeholder replacement path:
  1. Google Docs API `documents.get` + `documents.batchUpdate` `replaceAllText`; logs `DOCGEN_PLACEHOLDER_DOCS_API_PASS`.
  2. Fast `DocumentApp` text-node fallback; logs `DOCGEN_PLACEHOLDER_FAST_PASS`.
  3. Older section-level fallback only if markers remain after the fast pass.
- If Docs API is disabled or errors, generation continues with `DocumentApp`; no PDF should be emitted with unresolved placeholders.

### `apps-script-docs-creator/14_Manual_Maintenance.js`
- `runAuthorizeDocGeneratorAllAccess()` — one-stop authorization/preflight check after switching Apps Script to a standard Google Cloud project.
- `runAuthorizeDocGeneratorDocsApiAccess()` — checks Google Docs API access with a temporary document.
- `runAuthorizeDocGeneratorSheetAccess()` — checks SpreadsheetApp access to the main onboarding sheet.
- `runManualGenerateTemplatePdfs()` — manual one-off PDF generator for selected `Onboarding_ID` values, selected output folder, and selected template doc. Creates dated subfolders `YYYY-MM-DD__vN` and uses the same naming pattern as the main flow.

### `apps-script-docs-creator/15_Single_Document_Generator.js`
- `runGenerateSingleDocuments()` — independent PDF generation for one or many NIPs and/or Onboarding IDs, using one selected template and optional output folder.
- `runGenerateSingleDocumentForNip()` — backward-compatible alias for the same configuration.
- Text and array inputs are supported; entities repeated across NIP and ID inputs are deduplicated.
- Blank output folder uses `Files_Single_Generations_`; every PDF is placed in its `YYYY-MM-DD` subfolder.
- Custom folder accepts a name under the onboarding Drive root, a Drive folder ID, or a Drive folder URL.
- Reads main rows by header name and NIP/ID; it does not invoke the agreement queue, AppSheet, or status updates.

## Critical invariants

1. **Status protection** — `WRITE_STATUS_JUST_IN_TIME=true`. Never persist `Init` locally; once AppSheet has the row, it owns Status.
2. **Schema additivity** — Always append new columns; never reorder or truncate.
3. **Dedupe** — `(NIP, SubmittedOn)` + `_Import_History`. Survives manual DEST deletes.
4. **Deterministic PersonID** — `"P_" + base64(SHA256(onboardingId|role|fullName)).slice(0,22)`. AppSheet duplicate-key error on People_List Add is treated as OK.
5. **No `LOOKUP(USEREMAIL())` patterns** in AppSheet — superseded entirely by Generation_Jobs.

## Local commands

```bash
clasp push                # push to Apps Script (run from project folder)
clasp open                # open in editor
git push bibiv-onboarding-git
```
