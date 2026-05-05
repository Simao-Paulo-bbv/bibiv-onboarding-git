# BIBIV_onboarding_APP — LM/Codex project reference

> Target reader: language model working inside VS Code/Codex/Claude with the project folder attached. This file is an implementation-oriented context map. It describes the actual current AppSheet + Google Sheets + Apps Script state inferred from supplied files and screenshots. Do not treat this as user-facing documentation.

## 0. System purpose

`BIBIV_onboarding_APP` is an onboarding workflow for business leads/clients. One main row in `BIBIV_onboarding_APP` represents one onboarding case. The system imports leads from a source Google Sheet, normalizes/enriches data, creates/updates AppSheet rows, generates required application/agreement documents, tracks document readiness, supports upload of signed documents, and moves the onboarding record through status-driven workflow stages.

Stack:

```text
Source Google Sheet
  -> Google Apps Script sync/import/normalization/MF lookup/AppSheet API
  -> AppSheet app tables/views/actions/bots
  -> Generation_Jobs / Generation_Job_Items queue
  -> Agreements_Files document file-request rows
  -> AppSheet automation document generation + email sending
  -> Signed_Documents upload flow
```

Important design assumption: **AppSheet status values and action/bot names are part of the system contract.** Do not rename them casually; many formulas compare literal strings.

---

## 1. Repository / Apps Script file structure

The Apps Script project is split into numbered modules:

```text
00_Config.gs
01_Entry.gs
02_Headers_Mapping.gs
03_Import.gs
04_Process.gs
05_AppSheet_API.gs
06_MF_API.gs
07_Payload_And_Normalization.gs
08_ID_And_Dedupe_Time.gs
09_Logging.gs
10_Sheet_And_Header_Utils.gs
```

### 1.1 `00_Config.gs`

Global configuration and schemas.

Contains:

- spreadsheet IDs and sheet names for source, destination and logs,
- AppSheet app ID/table names/API URL/access key configuration,
- MF API URL and timeout,
- feature flags,
- logging config,
- source/destination sync status columns,
- dedupe mode,
- `HEADER_ALIASES`, `APPSHEET_SCHEMA`, `DEST_SCHEMA`, `SYSTEM_DEFAULTS`, `APPSHEET_MAIN_ALLOWED_COLS`.

Operational notes:

- Treat API keys and app IDs in config as secrets. Do not commit raw secrets to public repos.
- `CONFIG.FEATURES.APPSHEET_ENABLED` controls whether Apps Script pushes into AppSheet API.
- `CONFIG.FEATURES.MF_ENABLED` controls Polish Ministry of Finance VAT lookup.
- `CONFIG.FEATURES.PEOPLE_LIST_ENABLED` controls creating person rows.
- `CONFIG.DEDUPE.MODE = "NIP_ONLY"` means import dedupe primarily by NIP.

### 1.2 `01_Entry.gs`

Main entry/orchestration.

Known functions:

```javascript
runSyncAndProcess()
processPendingDestRows_(runId, mapping, source, dest, startedAt)
installTimeTriggerEveryMinute()
```

Role:

- starts import/sync/process pipeline,
- scans pending destination rows,
- respects runtime/row limits,
- can install time trigger.

### 1.3 `02_Headers_Mapping.gs`

Header validation and mapping.

Known functions:

```javascript
enforceDestHeaders_(runId, dest)
enforceTextFormats_(dest)
buildMapping_(runId, source, dest)
ensureSourceMarkColumn_(runId, source, mapping)
```

Role:

- ensures destination headers match expected schema,
- maps normalized source headers to destination/AppSheet columns,
- ensures source has `sync_status` marker column.

### 1.4 `03_Import.gs`

Source-to-destination import.

Known functions:

```javascript
importFromSource_(runId, mapping, source, dest, startedAt)
buildDestDedupeIndex_(runId, mapping, dest)
findDestHeaderByNormalized_(normKey)
```

Role:

- reads source rows,
- computes dedupe key,
- copies new rows to destination,
- marks source rows as imported (`IN_DEST...`) while preserving ability to re-import if needed depending on config.

### 1.5 `04_Process.gs`

Main row processing.

Known functions:

```javascript
processDestRows_(runId, mapping, source, dest, startRow, endRow, startedAt, rowMap, markIdx)
ensurePeopleRefsForRow_(runId, dest, mapping, rowNum, payloadMain)
pushPersonToPeopleList_(runId, personId, fullName, role, sourceFullNameCol, onboardingId, rowNum)
appendSyncMarker_(sheet, rowNum, syncIdx, marker)
```

Role:

- processes destination rows pending AppSheet/MF sync,
- enriches with MF API data,
- pushes main row to AppSheet,
- creates/updates `People_List` rows and writes person refs back to main table,
- appends sync markers.

### 1.6 `05_AppSheet_API.gs`

AppSheet API wrapper.

Known functions:

```javascript
filterPayloadForAppSheet_(payload, allowedCols)
callAppSheet_(runId, tableName, rowPayload, action, rowNum)
```

Role:

- filters payload to allowed columns,
- calls AppSheet REST API `Action` endpoint with `Add` / `Edit`,
- logs status/body, truncating long response bodies.

### 1.7 `06_MF_API.gs`

MF/VAT lookup wrapper.

Known functions:

```javascript
callMfAndWrite_(runId, dest, mapping, rowNum, nip, dateStr)
pickSubjectFromMf_(parsed)
safeDateForMf_(submittedOn)
```

Role:

- queries `wl-api.mf.gov.pl` by NIP and date,
- writes returned fields like company name, VAT status, REGON, KRS, registration date,
- safely derives query date from submitted date.

### 1.8 `07_Payload_And_Normalization.gs`

Payload creation and normalization.

Known functions:

```javascript
buildAppSheetPayloadFromDest_(dest, rowNum)
safeJsonParse_(text)
normalizeForAppSheet_(col, v)
normalizeNipMaybeNumber_(v)
normalizeKrs_(v)
normalizeRegon_(v)
```

Role:

- reads destination row and builds AppSheet-compatible payload,
- normalizes IDs, dates, NIP/REGON/KRS, booleans/text fields.

### 1.9 `08_ID_And_Dedupe_Time.gs`

IDs, dedupe and time utils.

Known functions:

```javascript
allocateNextId_(dest, idIdx)
makeDedupeKey_(nip, submittedOn)
normalizeSubmittedOnKey_(submittedOn)
makeRunId_()
formatNow_()
arraysEqual_(a, b)
writeIfColExists_(sheet, mapping, rowNum, colName, value)
isVerbose_()
safeHeaders_(headers)
installTimeTriggerEveryMinute()
```

Role:

- allocates IDs,
- builds dedupe keys,
- normalizes submitted timestamp,
- utility writing and logging helpers.

Note: `installTimeTriggerEveryMinute()` appears also in `01_Entry.gs`; verify duplicate definitions before modifying.

### 1.10 `09_Logging.gs`

Logging.

Known functions:

```javascript
compactLogData_(event, data)
log_(runId, level, event, data)
ensureLogSheet_(ss, name)
```

Role:

- logs to console/logger/sheet depending on config,
- compacts large log payloads.

### 1.11 `10_Sheet_And_Header_Utils.gs`

Sheet/header utilities.

Known functions:

```javascript
getSheet_(ss, name, createIfMissing)
getHeaderRow_(sheet)
indexByNormalized_(headers)
indexByExact_(headers)
normalizeKey_(s)
```

Role:

- opens/creates sheets,
- reads headers,
- creates exact and normalized header indexes.

---

## 2. Data model / tables

### 2.1 Main table: `BIBIV_onboarding_APP`

One row = one onboarding case.

Key/identity/status columns:

```text
ID
Status
NIP_Control
sync_status
submitted on
```

Generation control columns seen in sheets/screens:

```text
Is_Generating_Now
Generation_Triggered_By
Generation_Requested_By
Generation_Queued_At
Generation_Started_At
Generation_Finished_At
```

MF/API enrichment columns:

```text
name_api
statusVat
regon
krs
registrationLegalDate
```

Business input columns include, among others:

```text
nazwa firmy
nip
address
mam firmową stronę internetową
website
zakres działalności
posiadam wpis do knf
numer wpisu do knf
numer rachunku bankowego
kod swift banku
imię i nazwisko osoby kontaktowej
pesel osoby kontaktowej
email osoby kontaktowej
numer telefonu osoby kontaktowej
imię i nazwisko kierownika
data urodzenia 6
imię i nazwisko beneficjenta
data urodzenia
powiązania z sektorem publicznym
konflikt interesów
powiązania z grupą rbi
postępowanie upadłościowe
zgody i oświadczenia
```

Person reference columns:

```text
ContactPersonID
ManagerPersonID
BeneficialOwnerPersonID
```

Related virtual/list columns used by actions:

```text
[Related Agreements_Files]
[Related Bank_Accounts]
[Related Signed_Documents]
```

Current observed statuses / literals:

```text
New
In progress
Preparing applications
Applications Generated
Preparing documents
Agreements Generated
Sending documents
Waiting for client signature
Sending signed documents
Signed documents... / Signed documents
Queued for application generation
Active
```

Do not assume all status strings are normalized; screenshots show some fields truncated. Exact strings in formulas should be preserved from AppSheet/CSV/code.

### 2.2 `Doc_Templates`

Template catalog. One row = one document template.

Headers from CSV:

```text
Template_ID
Category
File_Name_Prefix
Folder_Path
Type
File Extension
Is_Active
Trigger_Condition
```

Also visible in AppSheet text dump:

```text
Temlate_Expression__Path   // note spelling if present; do not auto-correct blindly
```

Observed categories:

```text
Application
Agreement
```

Observed sample rows:

```text
Agreement / Appx_10   / Files_Agreements_ / Doc / .pdf / TRUE
Agreement / Appx_10.3 / Files_Agreements_ / Doc / .pdf / TRUE
Application / GDRP    / Files_Application_ / Doc / .pdf / TRUE
```

Role:

- Source of truth for which documents should be generated.
- `Job_Type` in `Generation_Jobs` is matched against `Doc_Templates[Category]`.
- File names/paths are derived from `File_Name_Prefix`, `Folder_Path`, `File Extension`, NIP and date.

### 2.3 `Agreements_Files`

File request/output table. One row = one expected/generated/ready document file.

Headers from CSV:

```text
ID
Onboarding_ID
Type
File Extension
File
File_Name
Folder_Path
Prefix
Category
Date_Created
File_status
Template_ID_Reference
```

Additional columns seen in actions/screens:

```text
Company _Name
Job_ID
Job_Item_ID
```

Observed `File_status` values:

```text
Set Up
Ready
Waiting for upload
Uploaded
```

Observed `Category` values:

```text
Application
Agreement
```

Role:

- AppSheet document generation tasks read these rows and create actual documents/PDFs.
- Completion triggers compare `Agreements_Files` rows with `File_status = "Ready"` against active templates.
- It is also used as the attachment source when sending generated documents by email.

### 2.4 `Generation_Jobs`

Queue table. One row = one generation job for an onboarding and a category.

Known columns:

```text
Job_ID
Onboarding_ID
Job_Type
Job_Status
Queued_At
Started_At
Finished_At
Requested_By
NIP_Control
```

Observed `Job_Type`:

```text
Application
Agreement
```

Observed `Job_Status`:

```text
Queued
Creating items
File requests created
Generating files
Finished
```

Role:

- Serializes generation work.
- Bots/actions select the oldest queued job if no job is currently in `Creating items` or `Generating files`.
- A job first creates `Generation_Job_Items`, then file request rows in `Agreements_Files`, then generation proceeds.

### 2.5 `Generation_Job_Items`

Queue item table. One row = one document/file request to create under a job.

Known columns:

```text
Job_Item_ID
Job_ID
Onboarding_ID
Template_ID
Category
Type
NIP_Control
File_Name_Prefix
Folder_Path
File_Extension
File_Name
File
Item_Status
Created_At
```

Observed `Item_Status` values:

```text
Queued
File request created
```

Role:

- Bot `JOB - create job items` creates one item per active template for the job category.
- Bot `JOBITEM - create Agreements_Files rows` or AppSheet action `JOBITEM - create file request and mark done` converts each item into an `Agreements_Files` row.

### 2.6 `Signed_Documents`

Signed document upload table.

Observed columns/actions imply:

```text
ID
Onboarding_ID
File
File_status
File Extension
Prefix
Category
Template_ID_Reference
```

Observed statuses:

```text
Waiting for upload
Uploaded
```

Role:

- Contains signed documents expected from the client.
- User opens form action `Upload File` and attaches file.
- Completion condition checks related signed docs and whether every related row has nonblank `[File]`.

### 2.7 `People_List`

People linked to onboarding.

Headers from CSV:

```text
PersonID
FullName
FirstNameFinal
LastNameFinal
Role
SourceFullNameColum
OnboardingID
NameOverride
NameNeedsReview
```

Additional columns seen in AppSheet text dump:

```text
_ComputedName
FullNameClean
NameTokens
LastNameParsed
FirstNameParsed
FullNameKey
PersonKey
Related BIBIV_onboarding_APPs By ContactPersonID
Related BIBIV_onboarding_APPs By BeneficialOwnerPersonID
```

Roles inferred from code/comments:

```text
Contact
Manager
BeneficialOwner
```

Role:

- Stores normalized contact/manager/beneficial owner names.
- Apps Script creates/pushes people and writes refs back to main row.

### 2.8 `People_Prefixes`

Small dictionary table for name parsing. Header:

```text
Particle
```

Contains surname particles such as `van`, `van de`, `van den`, etc. Used by name parsing logic/AppSheet formulas.

### 2.9 `Bank_Accounts`

Bank account table linked to onboarding.

Observed action columns:

```text
AccountID
Onboarding_ID
AccountNumber
```

Role:

- Stores one or more bank account numbers for onboarding.
- AppSheet actions can add one account and delete related accounts.

### 2.10 `Static_Attachments`

Seen in Data Explorer and email attachment formula. Likely table of fixed files attached to outgoing agreement/application emails.

Known fields from screenshot/formula:

```text
File
Is_Active
Category
```

Used by email task attachments:

```appsheet
SELECT(
  Static_Attachments[File],
  AND(
    [Is_Active] = TRUE,
    [Category] = "Agreement"
  )
)
```

---

## 3. AppSheet actions — current observed state

### 3.1 Main workflow actions on `BIBIV_onboarding_APP`

#### `Send for approval`

Effect: grouped sequence.

Actions:

```text
1. Change status (Preparing applications)
2. Create application generation job
```

Condition:

```appsheet
AND(
  IN(CONTEXT("View"), LIST("New leads", "New or Sending for approval_Detail")),
  [Status] = "New"
)
```

Role: starts application generation for a new lead.

#### `Send Documents to client`

Effect: grouped sequence.

Actions:

```text
1. Change status (Preparing documents)
2. Create agreement generation job
```

Condition:

```appsheet
IN(CONTEXT("View"), LIST("In progress"))
```

Role: starts agreement/document generation for an onboarding in progress.

#### `Send signed documents`

Effect: grouped sequence.

Actions:

```text
1. Change status (Sending signed documents)
```

Condition:

```appsheet
AND(
  [Status] = "Waiting for client signature",
  COUNT([Related Signed_Documents]) > 0,
  COUNT(
    SELECT(
      [Related Signed_Documents][File],
      ISBLANK([File])
    )
  ) = 0
)
```

Role: allows moving forward only if all related signed docs have files uploaded.

#### `Upload signed documents - primary`

Effect: app navigation.

Target:

```appsheet
LINKTOVIEW("Signed_Documents_Inline")
```

Condition observed truncated, likely view/status-based. Treat as separate primary button for upload detail flow.

#### `Upload signed documents - inline`

Effect: app navigation.

Target:

```appsheet
LINKTOVIEW("Signed_Documents_Inline")
```

Condition:

```appsheet
AND(
  COUNT([Related Signed_Documents]) > 0,
  COUNT(
    SELECT(
      [Related Signed_Documents][File],
      ISBLANK([File])
    )
  ) <> 0
)
```

Role: show inline upload action when at least one expected signed document is still missing a file.

#### `Add new store (store accepted)`

Effect: set current row values.

Set:

```appsheet
Status = "Active"
```

Condition screenshot partial:

```appsheet
AND(IN(CONTEXT("View"), LIST("Waiting")), [Status] ...)
```

Role: marks store/onboarding as accepted/active.

### 3.2 Main status actions on `BIBIV_onboarding_APP`

These are simple set-column actions, often hidden, used inside grouped actions/bots.

```text
Change status (In progress)              -> Status = "In progress"
Change status (Preparing applications)   -> Status = "Preparing applications"
Change status (Preparing documents)      -> Status = "Preparing documents"
Change status (Sending documents)        -> Status = "Sending documents"
Change status (Waiting)                  -> Status = "Waiting for client signature"
Change status (Sent signed documents)    -> Status = "Signed documents..." / likely "Signed documents"
Change status (Sending signed documents) -> Status = "Sending signed documents"
```

All show `Only if this condition is true = true` unless noted.

### 3.3 Generation job creation actions on `BIBIV_onboarding_APP`

#### `Create application generation job`

Effect: add a row to `Generation_Jobs`.

Set:

```appsheet
Job_ID       = UNIQUEID()
Onboarding_ID = [ID]
Job_Type     = "Application"
Job_Status   = "Queued"
Queued_At    = NOW()
Requested_By = USEREMAIL()
NIP_Control  = [NIP_Control]
```

Condition: `true`.

#### `Create agreement generation job`

Effect: add a row to `Generation_Jobs`.

Set:

```appsheet
Job_ID       = UNIQUEID()
Onboarding_ID = [ID]
Job_Type     = "Agreement"
Job_Status   = "Queued"
Queued_At    = NOW()
Requested_By = USEREMAIL()
NIP_Control  = [NIP_Control]
```

Condition: `true`.

### 3.4 Legacy/older file generation actions on `BIBIV_onboarding_APP`

These coexist with the newer queue/job mechanism. Be careful before deleting: some bots/actions may still reference them.

#### `Prepare applications`

Effect: execute action on set of rows.

Referenced table:

```text
Doc_Templates
```

Referenced rows:

```appsheet
FILTER(
  "Doc_Templates",
  AND(
    [Is_Active],
    [Category] = "Application"
  )
)
```

Referenced action:

```text
Add templates files (multiple)
```

Condition:

```appsheet
AND(
  [Is_Generating_Now] = TRUE,
  [Status] = "Preparing applications"
)
```

Role: old direct creation of `Agreements_Files` from active application templates. Newer queue flow creates `Generation_Job_Items` first.

#### `Prepare agreements`

Effect: execute action on set of rows.

Referenced table:

```text
Doc_Templates
```

Referenced rows:

```appsheet
FILTER("Doc_Templates", AND([Is_Active] = TRUE, [Category] = "Agreement"))
```

Referenced action:

```text
Add templates files (multiple)
```

Condition:

```appsheet
FALSE
```

Role: disabled legacy direct agreement file creation.

#### `Add agreements files`

Effect: add row to `Agreements_Files` from `BIBIV_onboarding_APP`.

Set values seen across screenshots/iterations:

```appsheet
Onboarding_ID = [ID]
Category      = "Agreement"
ID            = ""              // screenshot showed blank string in older version; unsafe for key if still current
File_Name     = CONCATENATE("APPX_10___", TRIM([NIP]), "___", TEXT(TODAY(), "DD-MM-YYYY"))
File          = CONCATENATE("Agreements_Files_Files_/", [NIP], "/APPX_10___", [NIP], "___", TEXT(TODAY(), "DD-MM-YYYY"), ".pdf")
```

Later expression screenshots show a revised formula using `NIP_Control` and double underscores:

```appsheet
File_Name = CONCATENATE("APPX_10___", TRIM([NIP]), "___", TEXT(TODAY(), "DD-MM-YYYY"))
File      = CONCATENATE("Agreements_Files_Files_/", [NIP], "/APPX_10___", [NIP], "___", TEXT(TODAY(), "DD-MM-YYYY"), ".pdf")
```

Condition:

```appsheet
ISBLANK(
  SELECT(
    Agreements_Files[_RowNumber],
    AND(
      [Onboarding_ID] = [_THISROW].[ID],
      [Category] = "Agreement"
    )
  )
)
```

Role: one-off/legacy APPX_10 agreement request creation. Prefer the job/template system for new work unless maintaining backward compatibility.

### 3.5 `Doc_Templates` action

#### `Add templates files (multiple)`

Source table: `Doc_Templates`.

Effect: add row to `Agreements_Files`.

Observed set columns:

```appsheet
Onboarding_ID = ANY(SELECT(BIBIV_onboarding_APP[ID], [Is_Generating_Now] = TRUE))
Template_ID_Reference = [Template_ID]
ID = UNIQUEID()
Category = [Category]
File_Name = CONCATENATE(
  ANY(SELECT(BIBIV_onboarding_APP[NIP_Control], [Is_Generating_Now] = TRUE)),
  "/",
  [File_Name_Prefix],
  "__",
  ANY(SELECT(BIBIV_onboarding_APP[NIP_Control], [Is_Generating_Now] = TRUE)),
  "__",
  TEXT(TODAY(), "DD-MM-YYYY")
)
File = CONCATENATE(
  [Folder_Path],
  ANY(SELECT(BIBIV_onboarding_APP[NIP_Control], [Is_Generating_Now] = TRUE)),
  "/",
  [File_Name_Prefix],
  "__",
  ANY(SELECT(BIBIV_onboarding_APP[NIP_Control], [Is_Generating_Now] = TRUE)),
  "__",
  TEXT(TODAY(), "DD-MM-YYYY"),
  [File Extension]
)
Prefix = [File_Name_Prefix]
Folder_Path = [Folder_Path]
File_status = "Set Up"
File Extension = [File Extension]
```

Earlier screenshots showed em dash/long dash separators (`—`) in file formulas; latest JSON and later expressions use `__`. Treat `__` as the current desired separator.

### 3.6 `Agreements_Files` actions

#### `Change File status (Ready)`

Effect: set current row values.

```appsheet
File_status = "Ready"
```

#### `Trigger: Applications Done`

Effect: execute action on set of rows.

Referenced table:

```text
BIBIV_onboarding_APP
```

Referenced rows:

```appsheet
LIST([Onboarding_ID])
```

Referenced action:

```text
Status: Applications Generated
```

Condition:

```appsheet
COUNT(
  SELECT(
    Agreements_Files[ID],
    AND(
      [Onboarding_ID] = [_THISROW].[Onboarding_ID],
      [File_status] = "Ready",
      [Category] = "Application"
    )
  )
)
=
COUNT(
  SELECT(
    Doc_Templates[Template_ID],
    AND([Is_Active], [Category] = "Application")
  )
)
```

Role: when all active application templates have ready files for the onboarding, mark main row as applications generated.

#### `Trigger: Agreements Done`

Same pattern as applications, but category `Agreement` and referenced action `Status: Agreements Generated`.

Condition:

```appsheet
COUNT(
  SELECT(
    Agreements_Files[ID],
    AND(
      [Onboarding_ID] = [_THISROW].[Onboarding_ID],
      [File_status] = "Ready",
      [Category] = "Agreement"
    )
  )
)
=
COUNT(
  SELECT(
    Doc_Templates[Template_ID],
    AND([Is_Active], [Category] = "Agreement")
  )
)
```

### 3.7 `Generation_Jobs` actions

#### `JOB - start next queued`

Effect: execute action on set of rows.

Referenced table:

```text
Generation_Jobs
```

Referenced rows:

```appsheet
TOP(
  ORDERBY(
    SELECT(
      Generation_Jobs[Job_ID],
      [Job_Status] = "Queued"
    ),
    [Queued_At],
    FALSE
  ),
  1
)
```

Referenced action:

```text
Job - start if next
```

Position: prominent.

#### `Job - start if next`

Effect: set current row values.

Set:

```appsheet
Job_Status = "Creating items"
Started_At = NOW()
```

Condition:

```appsheet
AND(
  [Job_Status] = "Queued",
  [Job_ID]
  =
  INDEX(
    ORDERBY(
      SELECT(
        Generation_Jobs[Job_ID],
        [Job_Status] = "Queued"
      ),
      [Queued_At],
      FALSE
    ),
    1
  ),
  COUNT(
    SELECT(
      Generation_Jobs[Job_ID],
      IN([Job_Status], LIST("Creating items", "Generating files"))
    )
  ) = 0
)
```

Role: safely starts only the first queued job and only when no job is active.

#### `JOB - mark file requests created`

Effect: set current row values.

Set:

```appsheet
Job_Status = "File requests created"
Finished_At = NOW()
```

Role: marks job after all job items created their file request rows.

### 3.8 `Generation_Job_Items` actions

#### `JOBITEM - create file request`

Effect: add row to `Agreements_Files` from `Generation_Job_Items`.

Set:

```appsheet
ID                    = UNIQUEID()
Onboarding_ID         = [Onboarding_ID]
Type                  = [Type]
File Extension        = [File_Extension]
File                  = CONCATENATE(
                          [Folder_Path],
                          "/",
                          [NIP_Control],
                          "/",
                          [File_Name_Prefix],
                          "__",
                          [NIP_Control],
                          "__",
                          TEXT(TODAY(), "DD-MM-YYYY"),
                          [File_Extension]
                        )
File_Name             = CONCATENATE(
                          [NIP_Control],
                          "/",
                          [File_Name_Prefix],
                          "__",
                          [NIP_Control],
                          "__",
                          TEXT(TODAY(), "DD-MM-YYYY")
                        )
Folder_Path           = [Folder_Path]
Prefix                = [File_Name_Prefix]
Category              = [Category]
Date_Created          = TODAY()
File_status           = "Set Up"
Template_ID_Reference = [Template_ID]
Job_ID                = [Job_ID]
Job_Item_ID           = [Job_Item_ID]
```

Note: Earlier screenshots showed `CONCATENATE([Folder_Path], [NIP_Control], "/", ...)` without an explicit slash after folder path. The current working setup uses `[Folder_Path], "/", [NIP_Control], "/"` in both `Generation_Job_Items[File]` and the `JOBITEM - create file request` mapping to `Agreements_Files`.

#### `JOBITEM - mark file request created`

Effect: set current row values.

```appsheet
Item_Status = "File request created"
```

#### `JOBITEM - mark file request created 2`

Effect: set current row values.

```appsheet
Item_Status = "File request created"
```

Likely duplicate/variant. Check references before removing.

#### `JOBITEM - create file request and mark done`

Effect: grouped sequence.

Actions:

```text
1. JOBITEM - create file request
2. JOBITEM - mark file request created
```

### 3.9 `Bank_Accounts` actions

#### `BA_AddOneAccount`

Source table: `BIBIV_onboarding_APP`.

Target table: `Bank_Accounts`.

Set:

```appsheet
AccountID     = UNIQUEID()
Onboarding_ID = [ID]
AccountNumber = "<<ACCOUNT>>"
```

Condition: `true`.

#### `BA_DeleteAllAccounts`

Source table: `BIBIV_onboarding_APP`.

Effect: execute action on set of rows.

Referenced table:

```text
Bank_Accounts
```

Referenced rows:

```appsheet
[Related Bank_Accounts]
```

Referenced action:

```text
BA_DeleteThisRow
```

#### `BA_DeleteAllForThisOnboarding`

Same as above: references `[Related Bank_Accounts]` and action `BA_DeleteThisRow`.

### 3.10 `Signed_Documents` actions

#### `Upload File`

Effect: app edit this row.

Desktop behavior:

```text
Open a form
```

Condition: `true`.

#### `Status: Waiting for upload`

Effect: set current row values.

```appsheet
File_status = "Waiting for upload"
```

#### `Status: Uploaded`

Effect: set current row values.

```appsheet
File_status = "Uploaded"
```

#### `Trigger: Signed Docs Ready`

Effect: execute action on set of rows.

Referenced table:

```text
BIBIV_onboarding_APP
```

Referenced rows:

```appsheet
LIST([Onboarding_ID])
```

Referenced action:

```text
Status: Signed Docs Ready
```

Condition visible on screenshot starts like the generated docs triggers:

```appsheet
COUNT(
  SELECT(
    Agreements_Files[ID],
    AND(
      [Onboarding_ID] = [_THISROW].[Onboarding_ID],
      ...
    )
  )
)
```

Full condition was not captured. Treat as incomplete.

### 3.11 UI/contact/navigation actions on `BIBIV_onboarding_APP`

Standard/helper actions observed:

```text
Edit
Delete
Open Url (Website)
Send SMS (numer telefonu osoby kontaktowej)
Compose Email (Email osoby kontaktowej)
Call Phone (numer telefonu osoby kontaktowej)
View Map (Address_Source)
View Map (Address)
View Map (Bank address)
View Map (residenceAddress)
View Map (workingAddress)
View Ref (BeneficialOwnerPersonID)
View Ref (ContactPersonID)
View Ref (ManagerPersonID)
```

---

## 4. Bots / automations — current observed state

### 4.1 Bot inventory from screenshots

`Agreements_Files`:

```text
Generate Agreements
Generate Applications
(disabled) QUEUE - continue after application files
```

`BIBIV_onboarding_APP`:

```text
Initiation
(disabled) Send for approval
Send agreements
Send applications
Send signed documents
(disabled) QUEUE - dispatch next generation
(disabled) QUEUE - finish completed generation
(disabled) QUEUE - create application files when active
```

`Generation_Job_Items`:

```text
JOBITEM - create Agreements_Files rows
JOB - finish and continue queue
```

`Generation_Jobs`:

```text
JOB - start queued job
JOB - create job items
```

`People_List`:

```text
Parse Names – People_List
BOT_SetOverrideOnManualEdit
```

`Signed_Documents`:

```text
Status: Uploaded
Status: Waiting for upload
Signed documents ready
```

### 4.2 `JOB - start queued job`

Event:

```text
Data change on Generation_Jobs
```

Process:

```text
Run action on rows
Referenced Table: Generation_Jobs
Referenced rows: LIST([Job_ID])
Referenced Action: Job - start if next
```

Role: whenever a `Generation_Jobs` row changes, try to start the job if it is the next eligible queued job.

### 4.3 `JOB - create job items`

Event:

```text
Data change 3 on Generation_Jobs
Data change type: Updates
Condition:
AND(
  [Job_Status] = "Creating items",
  COUNT(
    SELECT(
      Generation_Job_Items[Job_Item_ID],
      [Job_ID] = [_THISROW].[Job_ID]
    )
  ) = 0
)
```

Process:

```text
Call a webhook
```

Current working webhook body:

```json
{
  "Action": "Add",
  "Properties": {
    "Locale": "pl-PL",
    "Timezone": "Europe/Warsaw"
  },
  "Rows": [
<<Start: ORDERBY(SELECT(Doc_Templates[Template_ID], AND([Is_Active] = TRUE, [Category] = [_THISROW-1].[Job_Type])), [File_Name_Prefix], FALSE)>>
    {
      "Template_ID": "<<[Template_ID]>>",
      "Item_Status": "Queued"
    }<<If: [Template_ID] <> INDEX(ORDERBY(SELECT(Doc_Templates[Template_ID], AND([Is_Active] = TRUE, [Category] = [_THISROW-1].[Job_Type])), [File_Name_Prefix], FALSE), COUNT(SELECT(Doc_Templates[Template_ID], AND([Is_Active] = TRUE, [Category] = [_THISROW-1].[Job_Type]))))>>,<<EndIf>>
<<End>>
  ]
}
```

Interpretation:

- When job enters `Creating items`, if no items for that `Job_ID` exist, bot creates one `Generation_Job_Items` row per active template where `Doc_Templates.Category = Generation_Jobs.Job_Type`.
- Rows are ordered by `[File_Name_Prefix]`, descending/`FALSE` according to AppSheet `ORDERBY` syntax.
- The webhook is intentionally minimal. `Job_Item_ID`, `Job_ID`, `Created_At`, `File`, `File_Name`, and template/job-derived fields are set by `Generation_Job_Items` initial values / app formulas.
- File paths/names use `NIP_Control`, folder path, prefix, current date, extension, and must include explicit `"/"` between `[Folder_Path]` and `[NIP_Control]`.
- Do not put inline `CONCATENATE(... TEXT(TODAY(), "DD-MM-YYYY") ...)` expressions inside the webhook JSON; this caused AppSheet quote-parsing errors.
- Initial item status is `Queued`.

### 4.4 `JOBITEM - create Agreements_Files rows`

Event:

```text
Data change on Generation_Job_Items
Data change type: Adds
Condition: [Item_Status] = "Queued"
```

Process:

```text
Run a data action
Referenced Table: Generation_Job_Items
Referenced rows: LIST([Job_Item_ID])
Referenced Action: JOBITEM - create file request and mark done
```

Role: converts new queued job items into `Agreements_Files` rows and marks each item as file request created.

### 4.5 `JOB - finish and continue queue`

Event:

```text
Data change on Generation_Job_Items
Data change type: Updates
Condition:
AND(
  [Item_Status] = "File request created",
  COUNT(
    SELECT(
      Generation_Job_Items[Job_Item_ID],
      AND(
        [Job_ID] = [_THISROW].[Job_ID],
        [Item_Status] <> "File request created"
      )
    )
  ) = 0
)
```

Process:

```text
1. Run data action on Generation_Jobs:
   Referenced rows: LIST([Job_ID])
   Referenced Action: JOB - mark file requests created

2. Start next queued
   likely references/executes JOB - start next queued
```

Role: once all items for a job have produced file request rows, mark job done with file requests and continue queue by starting the next job.

### 4.6 `Generate Agreements`

Event source/table:

```text
Agreements_Files
```

Event condition shown:

```appsheet
AND([File_status] = "Set Up", [Category] = "Agreement")
```

Process graph from screenshot:

```text
Check status: Set up And Agreements
  -> If: Appx_10
      YES -> Create a document (appx10)
      NO  -> If: Appx_10.3
              YES -> Create a document (appx10.3)
              NO  -> If: Appx_10.2
                      YES -> Create a document (appx10.2)
                      NO  -> If: Appx_10.1
                              YES -> Create a document (appx10.1)
                              NO  -> GDPR
                                      YES -> Create a document (GDPR)
  -> Change File Status (Ready)
```

Observed document task settings:

- table: `Agreements_Files`,
- content type: PDF,
- template provider: Google Docs templates from `Doc_Templates.Template_ID`/specific template IDs,
- folder path expression:

```appsheet
CONCATENATE("/", [Folder_Path])
```

- file name prefix:

```appsheet
[File_Name]
```

- disable timestamp: enabled in screenshot,
- page orientation: Portrait,
- page size: A4.

Role: generates agreement PDF files based on `Agreements_Files` rows and marks file ready.

### 4.7 `Generate Applications`

Likely same as Generate Agreements but for application category. Screens show its presence under `Agreements_Files` but not all details. Completion trigger is `Trigger: Applications Done`.

Expected event condition by analogy:

```appsheet
AND([File_status] = "Set Up", [Category] = "Application")
```

Expected tasks: create documents for application templates such as `GDRP` and `Appx_...` depending on template prefix/category, then `Change File Status (Ready)`.

### 4.8 `Send agreements`

Event:

```text
BIBIV_onboarding_APP
Check if Generated
```

Process screenshot:

```text
Change status (Sending documents)
  -> Send mail with all documents
  -> Change status to Waiting
```

Email attachment formula shown for generated and static agreement files:

```appsheet
SELECT(
  Agreements_Files[File],
  AND(
    [Onboarding_ID] = [_THISROW].[ID],
    [File_status] = "Ready",
    [Category] = "Agreement"
  )
)
+
SELECT(
  Static_Attachments[File],
  AND(
    [Is_Active] = TRUE,
    [Category] = "Agreement"
  )
)
```

Attachment name visible:

```text
ChangeReport
```

Then status changes to waiting for client signature.

### 4.9 `Send applications`

Present under `BIBIV_onboarding_APP`. Details not fully captured, likely analogous to `Send agreements` for `Application` category.

### 4.10 `Send signed documents`

Present under `BIBIV_onboarding_APP`; likely sends uploaded signed docs onward. Action `Send signed documents` has condition requiring all related signed documents have files.

### 4.11 Disabled queue bots on `BIBIV_onboarding_APP`

Observed disabled bots:

```text
(disabled) QUEUE - dispatch next generation
(disabled) QUEUE - finish completed generation
(disabled) QUEUE - create application files when active
```

Interpretation: legacy queue mechanism on main onboarding rows has been superseded by `Generation_Jobs` / `Generation_Job_Items` bots. Preserve only if backward compatibility is needed.

### 4.12 `Signed_Documents` bots

Observed:

```text
Status: Uploaded
Status: Waiting for upload
Signed documents ready
```

Likely update `Signed_Documents.File_status` based on file upload and trigger main onboarding status once all required signed documents are uploaded.

Full event conditions were not captured.

---

## 5. Main process flows

### 5.1 New lead import and enrichment

```text
Source Sheet row
  -> Apps Script importFromSource_
  -> destination Sheet1 / AppSheet main table
  -> MF API lookup by NIP/date
  -> AppSheet Add/Edit BIBIV_onboarding_APP row
  -> People_List rows created for Contact/Manager/BeneficialOwner
  -> refs written to ContactPersonID / ManagerPersonID / BeneficialOwnerPersonID
```

### 5.2 Application generation flow — current queue version

```text
BIBIV_onboarding_APP row with Status = New
  -> user/action Send for approval
     -> Change status (Preparing applications)
     -> Create application generation job
        Generation_Jobs.Job_Type = "Application"
        Generation_Jobs.Job_Status = "Queued"

Generation_Jobs data change
  -> bot JOB - start queued job
     -> action Job - start if next
        if this job is oldest Queued and no active job exists:
          Job_Status = "Creating items"
          Started_At = NOW()

Generation_Jobs update to Creating items
  -> bot JOB - create job items
     -> webhook Add rows to Generation_Job_Items
        one item per active Doc_Templates row where Category = "Application"
        Item_Status = "Queued"

Generation_Job_Items add
  -> bot JOBITEM - create Agreements_Files rows
     -> action JOBITEM - create file request and mark done
        -> creates Agreements_Files row with Category = Application, File_status = Set Up
        -> marks item Item_Status = File request created

Generation_Job_Items updates
  -> bot JOB - finish and continue queue
     when all items for job are File request created:
        -> Generation_Jobs.Job_Status = "File requests created"
        -> maybe starts next queued job

Agreements_Files rows with Category Application and File_status Set Up
  -> bot Generate Applications
     -> create PDFs/documents
     -> Change File status (Ready)

Agreements_Files update to Ready
  -> Trigger: Applications Done
     if ready Application file count equals active Application template count:
        -> BIBIV_onboarding_APP.Status = "Applications Generated"
        -> generation flags cleared by Status: Applications Generated action
```

### 5.3 Agreement generation flow — current queue version

```text
BIBIV_onboarding_APP row in In progress view/status
  -> user/action Send Documents to client
     -> Change status (Preparing documents)
     -> Create agreement generation job
        Generation_Jobs.Job_Type = "Agreement"
        Generation_Jobs.Job_Status = "Queued"

Generation_Jobs queue path same as application flow
  -> JOB - start queued job
  -> JOB - create job items for Doc_Templates Category = Agreement
  -> JOBITEM creates Agreements_Files rows Category Agreement, File_status Set Up
  -> Generate Agreements creates PDFs
  -> Change File status (Ready)
  -> Trigger: Agreements Done marks main Status = Agreements Generated

BIBIV_onboarding_APP when agreements generated
  -> bot Send agreements
     -> Change status (Sending documents)
     -> send email with ready Agreement files + Static_Attachments Category Agreement
     -> Change status (Waiting / Waiting for client signature)
```

### 5.4 Signed documents flow

```text
BIBIV_onboarding_APP Status = Waiting for client signature
  -> related Signed_Documents rows exist with File_status = Waiting for upload
  -> user uses Upload File action/form
  -> Signed_Documents.File populated
  -> Status: Uploaded action/bot sets File_status = Uploaded
  -> Send signed documents action becomes visible only when:
       COUNT([Related Signed_Documents]) > 0
       and all [Related Signed_Documents][File] are nonblank
  -> Change status (Sending signed documents)
```

---

## 6. Naming/path conventions

### 6.1 Current preferred generated file name

For queue/job generated docs:

```appsheet
CONCATENATE(
  [NIP_Control],
  "/",
  [File_Name_Prefix],
  "__",
  [NIP_Control],
  "__",
  TEXT(TODAY(), "DD-MM-YYYY")
)
```

Example logical file name:

```text
7011183752/Appx_10__7011183752__04-05-2026
```

### 6.2 Current preferred generated file path

Generated by `Generation_Job_Items[File]` app formula, not by the webhook JSON:

```appsheet
CONCATENATE(
  [Folder_Path],
  "/",
  [NIP_Control],
  "/",
  [File_Name_Prefix],
  "__",
  [NIP_Control],
  "__",
  TEXT(TODAY(), "DD-MM-YYYY"),
  [File_Extension]
)
```

Example:

```text
Files_Agreements_/7011183752/Appx_10__7011183752__04-05-2026.pdf
```

### 6.3 Known legacy variants

Older screenshots/context showed:

```text
APPX_10___<NIP>___<DD-MM-YYYY>.pdf
```

and/or long dash separators:

```text
<File_Name_Prefix>—<NIP_Control>—<DD-MM-YYYY>
```

Do not mix separators without checking actual current AppSheet formulas. New queue JSON uses `__`.

---

## 7. Completion logic / invariants

### 7.1 Document completion per category

For a given onboarding and category, generated docs are complete when:

```text
count(Agreements_Files where Onboarding_ID = this onboarding, Category = category, File_status = Ready)
==
count(Doc_Templates where Is_Active = TRUE, Category = category)
```

This means inactive templates are ignored. If a template is activated/deactivated, completion logic changes immediately.

### 7.2 Queue concurrency invariant

Only one generation job should be active at a time. Active means:

```text
Job_Status IN ("Creating items", "Generating files")
```

Starting a job requires:

```text
this job is oldest queued job by Queued_At
AND no active jobs exist
```

### 7.3 Job-item idempotence invariant

`JOB - create job items` only creates items when no items exist for that job:

```appsheet
COUNT(SELECT(Generation_Job_Items[Job_Item_ID], [Job_ID] = [_THISROW].[Job_ID])) = 0
```

This prevents duplicate item creation for the same job.

### 7.4 File-request idempotence invariant

`JOB - finish and continue queue` waits until all related job items are `File request created`.

Potential gap: Ensure `JOBITEM - create file request` is not run twice for the same item. AppSheet action groups mark item done, but if bot retry occurs after file row creation and before item status update, duplicates are possible unless dedupe constraints/keys exist in `Agreements_Files`. Consider guarding by `Job_Item_ID` uniqueness if modifying.

### 7.5 Signed document completion invariant

All related signed documents must have nonblank `File`:

```appsheet
AND(
  COUNT([Related Signed_Documents]) > 0,
  COUNT(SELECT([Related Signed_Documents][File], ISBLANK([File]))) = 0
)
```

---

## 8. Critical literals and names to preserve

### 8.1 Tables

```text
BIBIV_onboarding_APP
Doc_Templates
Agreements_Files
Generation_Jobs
Generation_Job_Items
Signed_Documents
People_List
People_Prefixes
Bank_Accounts
Static_Attachments
User_Notifications
```

### 8.2 Key action names

```text
Send for approval
Send Documents to client
Send signed documents
Create application generation job
Create agreement generation job
Job - start if next
JOB - start next queued
JOB - mark file requests created
JOBITEM - create file request
JOBITEM - mark file request created
JOBITEM - create file request and mark done
Change File status (Ready)
Trigger: Applications Done
Trigger: Agreements Done
Status: Applications Generated
Status: Agreements Generated
Status: Signed Docs Ready
Upload File
Status: Waiting for upload
Status: Uploaded
```

### 8.3 Bot names

```text
JOB - start queued job
JOB - create job items
JOBITEM - create Agreements_Files rows
JOB - finish and continue queue
Generate Agreements
Generate Applications
Send agreements
Send applications
Send signed documents
Parse Names – People_List
BOT_SetOverrideOnManualEdit
```

### 8.4 Status literals

```text
New
In progress
Preparing applications
Applications Generated
Preparing documents
Agreements Generated
Sending documents
Waiting for client signature
Sending signed documents
Queued
Creating items
File requests created
Generating files
Finished
Set Up
Ready
Waiting for upload
Uploaded
File request created
```

---

## 9. Known ambiguities / verify before editing

The following are not fully captured in screenshots and should be verified in AppSheet before changing related logic:

1. Full condition and target action for `Trigger: Signed Docs Ready`.
2. Exact current formula for `Upload signed documents - primary` condition.
3. Exact current values in `Status: Signed Docs Ready` and `Change status (Sent signed documents)` because screenshots truncate the right side.
4. Exact current `Generate Applications` bot process and its create-document branch conditions.
5. Whether old direct actions (`Prepare applications`, `Prepare agreements`, `Add templates files (multiple)`, `Add agreements files`) are still actively referenced by enabled bots or are legacy-only.
6. Whether file path expressions consistently include a slash between `[Folder_Path]` and `[NIP_Control]`.
7. Whether duplicate action variants (`JOBITEM - mark file request created` and `JOBITEM - mark file request created 2`) are both referenced.
8. Whether `installTimeTriggerEveryMinute()` duplicate definitions exist and which one is effective in Apps Script runtime.

---

## 10. Development guidance for LM agents

When modifying this project:

- Prefer additive, backward-compatible changes unless asked to refactor.
- Preserve AppSheet literal strings unless updating all dependent formulas/actions/bots at once.
- Any change to status strings must update every AppSheet expression comparing that string.
- Any change to `Doc_Templates` schema affects job item creation, file paths, and document generation bots.
- Any change to file naming/path conventions affects AppSheet generated files, attachments, upload references, and possibly Drive folders.
- Queue logic must preserve single-active-job semantics.
- Apps Script changes should keep header aliasing and normalized mapping tolerant of Polish form headers.
- Do not hardcode secrets in new files. Read AppSheet/MF config from `CONFIG` or script properties.
- Avoid deleting legacy actions without checking bot/action references in AppSheet.

---

## 11. Compact mental model

```text
Main row = BIBIV_onboarding_APP.ID

Lead import:
source sheet -> Apps Script -> BIBIV_onboarding_APP + People_List

Application docs:
Send for approval -> Generation_Jobs(Application/Queued)
-> start queued job -> Creating items
-> bot creates Generation_Job_Items from active Application Doc_Templates
-> each item creates Agreements_Files row Set Up
-> Generate Applications bot creates files and sets Ready
-> Trigger: Applications Done -> Status Applications Generated

Agreement docs:
Send Documents to client -> Generation_Jobs(Agreement/Queued)
-> same queue path
-> Agreements_Files Agreement Set Up -> Generate Agreements -> Ready
-> Trigger: Agreements Done -> Status Agreements Generated
-> Send agreements bot emails ready Agreement files + static attachments
-> Status Waiting for client signature

Signed docs:
Signed_Documents rows Waiting for upload -> user uploads File
-> File_status Uploaded
-> all related files nonblank -> Send signed documents available
```
