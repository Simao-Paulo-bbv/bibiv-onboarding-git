# AppSheet Architecture (Final)

> Most important reference. The previous direct-from-Doc_Templates flow was abandoned after it caused cross-user record mixing, empty NIPs, and broken folder paths. The system below is the **stable, working architecture**.

## App identity

| Item | Value |
|---|---|
| App ID | `ebb1aa13-9408-4a7d-8d41-8cb03b9e766f` |
| Locale | `pl-PL` |
| API endpoint | `https://www.appsheet.com/api/v2/apps/{appId}/tables/{table}/Action` |
| Auth header | `ApplicationAccessKey` |
| Body shape | `{ Action, Properties: { Locale: "pl-PL", Timezone }, Rows: [row] }` |

## Tables

### `BIBIV_onboarding_APP` (main)
The main onboarding record. Apps Script Adds new rows after MF enrichment. Columns mirror `APPSHEET_SCHEMA` in `00_Config.js` (47 cols incl. trailing bank-meta + sales-rep fields). Allowed payload columns are gated by `APPSHEET_MAIN_ALLOWED_COLS`. Refs to People_List: `MAIN_REF_COLS = { CONTACT, MANAGER, BENEFICIAL }`.

### `People_List`
Contact / Manager / Beneficial Owner records. Apps Script Adds via `pushPersonToPeopleList_`. **Treat duplicate-key error as OK** (already exists, deterministic ID guarantees identity). Marker: `MARKERS.PEOPLE_OK = "PEOPLE_OK"`.

### `Bank_Accounts`
Child table. Columns: `AccountID, Onboarding_ID, AccountNumber, CreatedAt`. Source priority in Apps Script: `accountNumbers` (from MF) → fallback `numer rachunku bankowego` (form). Skip-unchanged + runtime cache. **Never touches `sync_status`.**

### `Doc_Templates`
Catalog of templates. Key columns:
- `Template_ID` (Text, Key)
- `Is_Active` (Yes/No)
- `Category` ("Application" / "Agreement" / "Signed")
- `Type`
- `File_Name_Prefix` (Text)
- `Folder_Path` (Text — constant root, e.g. `Files_Application_` or `Files_Agreements_`)
- `File Extension` (Text — `.pdf`, `.xlsx`, etc., **with leading dot**)

### `Generation_Jobs` (queue head)
One row per "Send" click.
- `Job_ID` (Text, Key)
- `Onboarding_ID` (Ref → BIBIV_onboarding_APP)
- `NIP_Control` (Text — `[Onboarding_ID].[NIP_Control]`)
- `Job_Type` ("Application" / "Agreement" / "Signed")
- `Job_Status` ("Queued" / "Creating items" / "Generating files" / "Done" / "Failed")
- `Requested_By`, `Queued_At`, `Started_At`, `Finished_At`

### `Generation_Job_Items` (per-template line items)
One row per template that needs to be generated for a job.

| Column | Type | Initial value / App formula |
|---|---|---|
| `Job_Item_ID` | Text | Initial value: `UNIQUEID()` |
| `Job_ID` | Ref → Generation_Jobs | Initial value: `ANY(SELECT(Generation_Jobs[Job_ID], [Job_Status]="Creating items"))` |
| `Template_ID` | Ref → Doc_Templates | (set by webhook body) |
| `Onboarding_ID` | Ref → BIBIV_onboarding_APP | App formula: `[Job_ID].[Onboarding_ID]` |
| `NIP_Control` | Text | App formula: `[Job_ID].[NIP_Control]` |
| `Category` | Text | App formula: `[Template_ID].[Category]` |
| `Type` | Text | App formula: `[Template_ID].[Type]` |
| `File_Name_Prefix` | Text | App formula: `[Template_ID].[File_Name_Prefix]` |
| `Folder_Path` | Text | App formula: `[Template_ID].[Folder_Path]` |
| `File_Extension` | Text | App formula: `[Template_ID].[File Extension]` |
| `File_Name` | Text | App formula: `CONCATENATE([NIP_Control], "/", [File_Name_Prefix], "__", [NIP_Control], "__", TEXT(TODAY(), "DD-MM-YYYY"))` |
| `File` | Text | App formula: `CONCATENATE([Folder_Path], "/", [NIP_Control], "/", [File_Name_Prefix], "__", [NIP_Control], "__", TEXT(TODAY(), "DD-MM-YYYY"), [File_Extension])` |
| `Item_Status` | Text | Webhook value or initial value: `"Queued"` → `"File request created"` → `"Agreement file created"` |
| `Created_At` | DateTime | Initial value: `NOW()` |

> The `"/"` separators are **explicit and required**. Earlier versions concatenated `Folder_Path` directly with NIP and produced paths like `Files_Application_5531549891/...` (missing `/`) → broken folder routing. Never glue `Folder_Path` and NIP without `"/"`.

### `Agreements_Files`
The actual file-factory input table. Existing `Generate Applications` / `Generate Agreements` bots watch this table and produce physical files.

Key columns: `File_status` (`Set Up` → optional `Generating` → optional `Generated` → `Ready`), `File`, `File_Name`, `Job_ID`, `Job_Item_ID`, `Template_ID_Reference`, plus the file artifacts.

## Bot chain (the queue)

> All bots that update data needed by chained bots **must** have **"Trigger other bots = ON"**.

### 1. `JOB - start queued job`
- **Event**: `Generation_Jobs` — Adds only
- **Condition**: oldest Queued, with no other active job
  ```
  AND(
    [Job_Status] = "Queued",
    [Job_ID] = ANY(ORDERBY(SELECT(Generation_Jobs[Job_ID],
       [Job_Status]="Queued"), [Queued_At])),
    ISBLANK(ANY(SELECT(Generation_Jobs[Job_ID],
       OR([Job_Status]="Creating items", [Job_Status]="Generating files"))))
  )
  ```
- **Process**: action `Job - start if next` → `Job_Status = "Creating items"`, `Started_At = NOW()`

### 2. `JOB - create job items`
- **Event**: `Generation_Jobs` — Updates only
- **Condition**: `AND([Job_Status] = "Creating items", ISBLANK(ANY(SELECT(Generation_Job_Items[Job_Item_ID], [Job_ID]=[_THISROW].[Job_ID]))))`
- **Process step**: `Call a webhook`
  - Verb: `POST`
  - URL: `https://api.appsheet.com/api/v2/apps/{APP_ID}/tables/Generation_Job_Items/Action`
  - Header: `ApplicationAccessKey: <key>`
  - **Body** (minimal — other columns auto-fill via initial values / app formulas above):
    ```
    {
      "Action": "Add",
      "Properties": {
        "Locale": "pl-PL",
        "Timezone": "Europe/Warsaw"
      },
      "Rows": [
    <<Start: ORDERBY(SELECT(Doc_Templates[Template_ID],
        AND([Is_Active]=TRUE, [Category]=[_THISROW-1].[Job_Type])),
        [File_Name_Prefix], FALSE)>>
        {
        "Template_ID": "<<[Template_ID]>>",
        "Item_Status": "Queued"
        }<<If: [Template_ID] <> INDEX(ORDERBY(SELECT(Doc_Templates[Template_ID], AND([Is_Active]=TRUE, [Category]=[_THISROW-1].[Job_Type])), [File_Name_Prefix], FALSE), COUNT(SELECT(Doc_Templates[Template_ID], AND([Is_Active]=TRUE, [Category]=[_THISROW-1].[Job_Type]))))>>,<<EndIf>>
    <<End>>
      ]
    }
    ```
  - Keep `File`, `File_Name`, `Folder_Path`, `NIP_Control`, and date expressions out of this JSON body. Earlier inline `CONCATENATE(... TEXT(TODAY(), "DD-MM-YYYY") ...)` bodies caused quote-parsing errors.
  - The `[_THISROW-1]` syntax is **the only way** to access the parent (Generation_Jobs) row from inside `<<Start>>` over Doc_Templates.

### 3. `JOBITEM - create Agreements_Files rows`
- **Event**: `Generation_Job_Items` — Adds only
- **Condition**: `[Item_Status] = "Queued"`
- **Process**: grouped action
  1. Add row to `Agreements_Files` with:
     - `File_status` = `"Set Up"`
     - `File` = `[_THISROW].[File]`
     - `File_Name` = `[_THISROW].[File_Name]`
     - `Job_ID` = `[_THISROW].[Job_ID]`
     - `Job_Item_ID` = `[_THISROW].[Job_Item_ID]`
     - `Template_ID_Reference` = `[_THISROW].[Template_ID]`
  2. Set `Item_Status = "File request created"`

### 4. `Generate Applications` / `Generate Agreements`
- **Applications**: existing AppSheet file-factory bot can stay as-is.
- **Agreements**: legacy native AppSheet PDF generator should stay disabled; `Generate Agreements - Apps Script` calls `generateAgreementFilesFromAppSheet()`.
- Apps Script dispatches per-file worker tasks, writes intermediate `Generating`/`Generated` statuses when accepted by AppSheet, creates missing `Signed_Documents` rows, and finalizes with `Ready`.
- AppSheet completion/email bots must continue to key only on `Ready`.

### 5. `JOB - finish and continue queue`
- **Event**: `Generation_Job_Items` — Updates only
- **Condition**: `[Item_Status] = "Agreement file created"` AND all sibling items also done:
  ```
  AND(
    [Item_Status] = "Agreement file created",
    ISBLANK(ANY(SELECT(Generation_Job_Items[Job_Item_ID],
      AND([Job_ID]=[_THISROW].[Job_ID],
          NOT(IN([Item_Status], LIST("Agreement file created","Failed")))))))
  )
  ```
- **Process**: grouped action
  1. `Job - finish` → on parent `[Job_ID]`: `Job_Status="Done"`, `Finished_At=NOW()`
  2. `Job - start next queued` → re-evaluates Bot 1's condition

## User-facing actions on `BIBIV_onboarding_APP`

These now create only a `Generation_Jobs` row — no direct `Agreements_Files` rows.

| Action | Purpose | Job_Type |
|---|---|---|
| `Send applications` (was `Send for approval`) | Generate Application docs for review | `"Application"` |
| `Send agreements` (was `Send Documents to client`) | Generate Agreement docs for client | `"Agreement"` |
| `Send signed documents` | Same flow, post-signature handling | `"Signed"` |

Each does:
```
Add row to Generation_Jobs:
  Job_ID         = UNIQUEID()
  Onboarding_ID  = [_THISROW].[ID]
  Job_Type       = "Application" | "Agreement" | "Signed"
  Job_Status     = "Queued"
  Requested_By   = USEREMAIL()
  Queued_At      = NOW()
```

## Actions / bots to DELETE or DISABLE

These are legacy / superseded — leaving them on can re-introduce the old bugs (empty NIP, mixed records, broken paths):

- ❌ `Add templates files (multiple)` — the old direct-from-Doc_Templates copy. **The single biggest source of old bugs.**
- ❌ `Prepare applications`
- ❌ `Flag creating documents`
- ❌ `QUEUE - mark active`, `QUEUE - start one generation`, `QUEUE - dispatch next generation` — pre-Generation_Jobs queue
- ❌ `Template - set active onboarding context`
- ❌ Any action containing `LOOKUP(USEREMAIL(), "BIBIV_onboarding_APP", "Generation_Triggered_By", "ID")` — root cause of cross-user record mixing.

## Legacy fields on `BIBIV_onboarding_APP` (can be deprecated)

These exist in `APPSHEET_SCHEMA` only for backward compatibility and are no longer needed once the queue flow is the only path:

- `Is_Generating_Now`
- `Generation_Triggered_By`
- `Generation_Requested_By`
- `Generation_Queued_At`
- `Generation_Started_At`
- `Generation_Finished_At`

All of these are superseded by `Generation_Jobs[Requested_By/Queued_At/Started_At/Finished_At]`. Keep them in the Apps Script schema until you verify nothing else reads them.

## File-path rules (HARD)

```
✅  Files_Application_/5531549891/Onboarding_form__5531549891__30-04-2026.xlsx
✅  Files_Agreements_/5140154728/Appx_10__5140154728__30-04-2026.pdf
❌  Files_Application_5531549891/...      ← missing slash, breaks folder
❌  Files_Application_/Onboarding_form_...  ← missing NIP subfolder
```

Constants:
- `Files_Application_` and `Files_Agreements_` are **fixed root folder names**, never glued with NIP.
- NIP is a **subfolder**.
- Formula must use explicit `"/"` between every segment.

## Status overwrite protection

Apps Script must respect AppSheet-managed Status. Implemented in `04_Process.js`:

- `WRITE_STATUS_JUST_IN_TIME = true` — write Status only on truly fresh row (live status blank AND `idAssignedNow`).
- `hasExternalManagedStatus` — pre-check before any AppSheet write.
- `liveExternalManaged` — re-check immediately before Add (race-safe).
- On Edit, `Status` is **stripped from the payload entirely**.

## Marker semantics in `sync_status`

Markers are append-only (latest concatenated, `APPSHEET_OK` token preserved):

| Marker | Meaning |
|---|---|
| `IMPORTED` | Row copied SOURCE → DEST |
| `MF_OK` | All MF lookups succeeded |
| `MF_NOT_VAT` | REGON returned `subject:null` — fast path with synthesized residence address |
| `MF_NO_SUBJECT` | MF returned no subject (transient) |
| `MF_RATE_LIMIT` | GOV VAT HTTP 429 |
| `MF_REGON_BLOCK` / `MF_VAT_BLOCK` | Hard error from upstream |
| `APPSHEET_OK` | Add succeeded |
| `APPSHEET_EDIT_OK` | Edit succeeded |
| `APPSHEET_FAIL` | Add/Edit failed |
| `APPSHEET_WAITING_MF_DATA` | Required MF fields missing — gate by `evaluateMfReadinessForAdd_` |
| `APPSHEET_SCHEMA_MISMATCH` | "mismatch in number of columns" — sheet structure changed in AppSheet |
| `PEOPLE_REFS_OK` | Contact/Manager/Beneficial pushed (or already-exists treated as OK) |
