# BIBIV™ Onboarding — Project Overview

> Reference doc for LM agents joining the project. Reflects **final, working state** of the system. No history, no journey — only the current solution.

## What this system does

End-to-end onboarding pipeline for B2B clients of BIBIV™. Receives form submissions from a public Squarespace landing page, enriches them with Polish company-registry data (REGON/VAT/IBAN), pushes them into AppSheet for the back-office team to review, and then runs a document-generation queue (Application docs → Agreement docs → Signed-document handling) inside AppSheet.

## High-level data flow

```
Squarespace form
      │
      ▼
Google Sheet "SOURCE" (raw submissions, headers may rename in Squarespace)
      │  [03_Import.js] dedupe by NIP+SubmittedOn + _Import_History
      ▼
Google Sheet "DEST" (canonical schema, stable column order)
      │  [04_Process.js]
      │   ├─ assign Onboarding_ID
      │   ├─ GOV enrichment (REGON → VAT → IBAN) via gov.api.hypnotype.com
      │   ├─ Bank_Accounts child sync (11_Bank_Accounts.js)
      │   ├─ optional historical backfill for blank bank/sales-rep fields
      │   ├─ People_List refs (Contact / Manager / Beneficial Owner) with
      │   │   deterministic PersonID = "P_" + base64(SHA256(OnboardingID|Role|fullName))
      │   └─ AppSheet Add / Edit (07_Payload_And_Normalization.js + 05_AppSheet_API.js)
      ▼
AppSheet app  (App ID: ebb1aa13-9408-4a7d-8d41-8cb03b9e766f)
   tables: BIBIV_onboarding_APP, People_List, Bank_Accounts,
           Doc_Templates, Generation_Jobs, Generation_Job_Items, Agreements_Files
      │
      ├─ User clicks "Send applications" / "Send agreements"
      │       → creates row in Generation_Jobs (Job_Type, Job_Status="Queued")
      │
      ▼
Generation_Jobs queue
      │  Bot: JOB - start queued job   (Adds, oldest queued only, no other active job)
      │       → Job_Status = "Creating items"
      │  Bot: JOB - create job items   (Updates, condition above)
      │       → webhook → AppSheet API → adds rows to Generation_Job_Items
      ▼
Generation_Job_Items
      │  Bot: JOBITEM - create Agreements_Files rows  (Adds, Item_Status="Queued")
      │       → grouped action: Add row to Agreements_Files (File_status="Set Up")
      │                          + set Item_Status = "File request created"
      ▼
Agreements_Files (the file-factory input table)
      │  Existing bot: "Generate Applications"
      │       → produces Application PDF/XLSX in Drive
      │       → sets File_status = "Ready"
      │  Agreement docs:
      │       Bot "Generate Agreements - Apps Script" fires once all job items
      │       are "File request created" and calls generateAgreementFilesFromAppSheet()
      │       in standalone script BIBIV_Onboarding_DocsCreator.
      │       That call only enqueues the Job_ID. A 1-minute dispatcher
      │       processNextQueuedDocGenerationJob creates per-file tasks:
      │       bounded workers copy Google Docs templates from Doc_Templates[Template_ID],
      │       fill placeholders, export the first Docs tab to PDF, and write to Drive.
      │       The finalizer creates Signed_Documents upload rows, sets
      │       Agreements_Files[File_status]="Ready", and marks
      │       Generation_Job_Items[Item_Status]="Agreement file created".
      ▼
Bot: JOB - finish and continue queue (Updates on Generation_Job_Items,
     condition Item_Status="Agreement file created")
     → Job_Status = "Done", Finished_At = NOW
     → starts next queued Generation_Jobs row
```

## Repository layout

The Apps Script source lives in the project subfolder (clasp-managed; push with `clasp push`). Connected to git remote `bibiv-onboarding-git`.

| File | Purpose |
|---|---|
| `00_Config.js` | Spreadsheet IDs, runtime limits, feature toggles, AppSheet creds, GOV config, schema arrays |
| `01_Entry.js` | `runSyncAndProcess()` entrypoint + 1-minute trigger installer |
| `02_Headers_Mapping.js` | Squarespace header rename map (parallel aliases, either column wins) |
| `03_Import.js` | SOURCE → DEST import with dedupe + `_Import_History` hidden sheet |
| `04_Process.js` | Core pipeline: ID → MF → Bank_Accounts → People_List refs → AppSheet Add/Edit |
| `05_AppSheet_API.js` | REST v2 caller, payload allowlist, schema-mismatch detection |
| `06_MF_API.js` | REGON → VAT → IBAN order, Not-VAT fast path, IBAN cache |
| `07_Payload_And_Normalization.js` | Build & normalize AppSheet payload (NIP/KRS/REGON, dates, phone, accountNumbers) |
| `08_ID_And_Dedupe_Time.js` | Onboarding_ID assignment, time helpers |
| `09_Logging.js` | Logger + sync_status marker append |
| `10_Sheet_And_Header_Utils.js` | Header repair (non-destructive — never truncates) |
| `11_Bank_Accounts.js` | Child table sync to AppSheet `Bank_Accounts` |
| `12_Backfill_Existing.js` | Toggle-controlled backfill for legacy rows; currently OFF after representative-field catch-up |
| `13_Name_Api_Refresh.js` | Maintenance-only refresh for historical `name_api`; updates only that column |
| `apps-script-docs-creator/` | Standalone Apps Script `BIBIV_Onboarding_DocsCreator` for Agreement PDFs |

## External services

- **GOV API at `gov.api.hypnotype.com`** — REGON, VAT, IBAN
- **AppSheet REST API v2** — `https://www.appsheet.com/api/v2/apps/{appId}/tables/{table}/Action`, header `ApplicationAccessKey`
- **Standalone Apps Script `BIBIV_Onboarding_DocsCreator`** — Script ID `1KOKGrJuBw6U2xiNg8UP_7ZlFWbihc2ug7UbBhHgD2p427HN6-drxp3qU`; exports Agreement PDFs through Google Docs/Drive renderer.

## Critical invariants (do not violate)

1. **Status protection** — Once AppSheet has a row, AppSheet owns `Status`. Apps Script writes Status only Just-In-Time on a truly fresh row (blank live status + `idAssignedNow`). Never overwrite an external-managed status.
2. **Schema additivity** — New DEST columns are always appended to the end. Never reorder. `repairDestHeadersOnlyAfterQueueInsert` is non-destructive.
3. **Dedupe key** — `(NIP, SubmittedOn)`, persisted in hidden `_Import_History` sheet. Survives DEST row deletions.
4. **Deterministic PersonID** — `"P_" + base64(SHA256(onboardingId|role|fullName)).slice(0,22)`. Same person always gets the same key across re-imports.
5. **Folder paths in AppSheet** — `Folder_Path` is a constant root (`Files_Application_` or `Files_Agreements_`); NIP is a separate subfolder; formulas always use explicit `"/"` separators.
6. **No `LOOKUP(USEREMAIL(), …)` for record context** — abandoned; caused cross-user record mixing. Always carry context via `Generation_Jobs[Onboarding_ID]` → `Generation_Job_Items` → `Agreements_Files`.
7. **Agreement generator is queue-backed** — AppSheet must enqueue via `generateAgreementFilesFromAppSheet`; the dispatcher creates per-file tasks, bounded workers generate PDFs, and the finalizer is the only step that writes `Ready`, so mail cannot send before the complete document set exists.
8. **REGON hard failures require humans** — persistent `MF_REGON_BLOCK` rows are moved to `Status = "need verification"` and are not retried forever.
9. **`name_api` refresh is isolated** — `runRefreshNameApiOnly()` updates only `name_api` from REGON/GOV. It must not run import/AppSheet/People/bank logic.
10. **GOV enrichment status gate** — regular GOV/MF enrichment runs only while `Status = "Init"`; later AppSheet workflow/status changes must not re-run GOV or overwrite `name_api`.
11. **`name_api` source is REGON** — never derive `name_api` from VAT subject names. Regular VAT enrichment must use GOV API only.

## Key reference docs in this folder

- `APPSHEET_ARCHITECTURE.md` — tables, bots, actions, webhook bodies, file paths
- `APPS_SCRIPT_REFERENCE.md` — file-by-file map, constants, invariants
- `API_INTEGRATIONS.md` — REGON/VAT/IBAN order and AppSheet REST
- `CONVENTIONS_AND_PITFALLS.md` — what not to do, recovery patterns
- `APPS_SCRIPT_AGREEMENT_GENERATOR.md` — new Apps Script PDF generator and AppSheet bot setup for agreements
