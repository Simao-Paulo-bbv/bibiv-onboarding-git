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
- Order: **REGON (by NIP, mandatory) → VAT → IBAN**.
- Endpoints under `gov.api.hypnotype.com`.
- VAT retry strategy: today's date, then no date.
- Not-VAT path: `subject:null` from REGON → write `statusVat="Not VAT"`, build `residenceAddress` from REGON: `"Ulica NrNier NrNier/NrLok, KodPocztowy Miejscowosc"`. Skip VAT/IBAN.
- IBAN cache: `CacheService.getScriptCache()` keyed by account number. Skipped entirely if full bank meta already on row.
- IBAN skipped on hard REGON/VAT error.
- MF VAT relay (Cloud Run) used for paths that hit MF rate limits (WL-191/429).

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
