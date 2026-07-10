# Apps Script Agreement PDF Generator

Current direction: replace native AppSheet `Create a new file` tasks for agreement PDFs with a standalone Apps Script, so the final PDF is exported by the Google Docs/Drive renderer and keeps real Google Docs layout, headers, footers, margins, and page numbers.

## Standalone script project

Do not keep this generator in the Google-Sheet-bound sync script. AppSheet may not list functions from sheet-bound scripts created from a spreadsheet.

Standalone script:

```text
Name: BIBIV_Onboarding_DocsCreator
Script ID: 1KOKGrJuBw6U2xiNg8UP_7ZlFWbihc2ug7UbBhHgD2p427HN6-drxp3qU
Local folder: apps-script-docs-creator/
```

Push this standalone script from its own folder:

```bash
cd apps-script-docs-creator
../node_modules/.bin/clasp push --force
```

The main sheet-bound Apps Script ignores `apps-script-docs-creator/**` via root `.claspignore`, so the two scripts stay separate.

## Code entrypoints

AppSheet calls this function:

```text
generateAgreementFilesFromAppSheet(onboardingId, jobId, agreementFileId)
```

It only enqueues the job and returns quickly. It does not generate PDFs inline.

The time-driven worker does the real generation:

```text
processNextQueuedDocGenerationJob()
```

The worker is created automatically by `ensureDocGenerationQueueTrigger()`. It uses a short one-shot trigger and then drains the queue within a safe runtime budget. A `Job_ID` is still processed in chunks controlled by `CONFIG.DOC_GENERATOR.MAX_FILES_PER_RUN`, but continuation chunks for the same job are put back at the front of the queue and are picked up immediately by the same worker while time remains. If the worker yields while queue items remain, it refreshes the one-shot trigger before returning so the next queued `Job_ID` is not left in `Set Up`.

The worker also has a per-job claim guard. If a duplicate trigger wakes while the same `Job_ID` is already being processed, it logs `DOCGEN_ALREADY_RUNNING` and exits cleanly instead of throwing a failed execution.

Recommended AppSheet parameters:

```text
onboardingId      = [Onboarding_ID]
jobId             = [Job_ID]
agreementFileId   = [ID]
```

The script uses `jobId` first. Queue deduplication is per `Job_ID`: the first file event for a job logs `added:true`, later file events for the same job log `added:false` and do not create duplicate work or refresh the worker trigger. One queued job generates all pending agreement files for that job. If more files remain after the safe per-run limit, the script re-enqueues the same `Job_ID` for continuation.

## Required config before production

In `apps-script-docs-creator/00_Config.js`, set:

```javascript
CONFIG.DOC_GENERATOR.OUTPUT_ROOT_FOLDER_ID = "1iHYmHQCpA4IHEfnjrV7okk3sqjw6MkOd"
```

Current production toggles as of 2026-05-13:

```javascript
CONFIG.DOC_GENERATOR.USE_SHEET_READS = false;
CONFIG.DOC_GENERATOR.USE_DOCS_API_PLACEHOLDER_REPLACEMENT = true;
```

`USE_SHEET_READS=false` is intentional. The generator reads production rows through AppSheet so AppSheet remains the source of workflow truth. The optional sheet-read path exists for diagnostics/manual helpers only.

Use the Drive folder that contains the AppSheet file roots such as:

```text
Files_Agreements_
Files_Application_
```

Current known folder IDs:

```text
Project root:        1iHYmHQCpA4IHEfnjrV7okk3sqjw6MkOd
Files_Agreements_:  1KanLb-sGCMctfa_BfoFaTWwegxhvUINq
Files_Application_: 1ZOsWaGKdaVtcvNymcSAOvWuLAZ36Vl6K
```

The script stores only the project root ID in config and then requires the first path segment (`Files_Agreements_` or `Files_Application_`) to exist under that root. This prevents accidentally creating a second root folder if a path has a typo.

This preserves the old AppSheet file mechanics:

```text
File Folder Path = CONCATENATE("./", [Folder_Path])
File Name Prefix = [File_Name]
Disable Timestamp = true
```

The Apps Script generator writes to the relative path already stored in `Agreements_Files[File]`, for example:

```text
Files_Agreements_/5140154728/Appx_10__5140154728__04-05-2026.pdf
```

## AppSheet bot

The stable production setup is one enqueue call per newly created agreement file row. AppSheet may call the script 7 times for one agreement job, but Apps Script deduplicates those calls by `Job_ID`; only the first call adds the job to the queue.

Disable old/unsafe agreement generators:

```text
Generate Agreements
Generate Agreements - Apps Script   (old Generation_Job_Items update variant, if present)
generateAgreementFilesFromAppSheetInlineStart callers
```

Create / keep this bot:

```text
Bot:   Kick Apps Script generator
Event: Added Set Up agreement
```

Event:

```text
Table: Agreements_Files
Data change: Adds only
Condition:
AND(
  [Category] = "Agreement",
  [File_status] = "Set Up",
  ISNOTBLANK([Job_ID]),
  ISNOTBLANK([Onboarding_ID])
)
```

Process:

```text
Step: Call a script
Script function: generateAgreementFilesFromAppSheet
```

Arguments:

```text
onboardingId      [Onboarding_ID]
jobId             [Job_ID]
agreementFileId   [ID]
```

Settings:

```text
Trigger other bots: ON
```

Return value can be ignored for now; the script updates AppSheet rows directly through the AppSheet API.

`Run asynchronously?` can be ON or OFF because the AppSheet call only enqueues. The worker runs independently.

Do not call an inline-start function from AppSheet. Inline generation from the AppSheet automation context was tested and caused canceled tasks / partial file-row creation. Keep AppSheet's call short: enqueue only, then let `processNextQueuedDocGenerationJob` run from the time-driven trigger.

## What the script does

For a queued job:

1. Worker finds all `Agreements_Files` rows where:

   ```text
   File_status = "Set Up"
   Category = "Agreement"
   Job_ID = current Job_ID
   ```

2. For each pending row:
   - marks the file row as `Generating` when progress statuses are enabled,
   - reads `Template_ID_Reference`,
   - finds the matching `Doc_Templates[Template_ID]`,
   - copies the Google Docs template file,
   - replaces simple placeholders,
   - exports the copy as PDF,
   - writes the PDF to `Agreements_Files[File]` path,
   - logs the file as generated. In the current production config, the per-file `Generated` AppSheet write is deferred to the final `Ready` batch.

3. Batch marks the processed chunk as `Ready` and updates matching `Generation_Job_Items`.

4. Creates missing `Signed_Documents` upload rows for successfully generated agreement docs.

5. Batch marks:

   ```text
   Agreements_Files[File_status] = "Ready"
   Generation_Job_Items[Item_Status] = "Agreement file created"
   ```

6. If all generated files for the job are ready, updates:

   ```text
   BIBIV_onboarding_APP[Status] = "Agreements Generated"
   ```

The existing `JOB - finish and continue queue` bot can still finish the `Generation_Jobs` row because the script sets `Generation_Job_Items[Item_Status] = "Agreement file created"`.

## Progress statuses

The generator can show per-file progress in `Agreements_Files[File_status]`:

```text
Set Up      -> waiting for generator
Generating  -> current PDF is being copied/replaced/exported
Generated   -> PDF exists and upload-placeholder/finalization steps can run
Ready       -> final state used by existing AppSheet completion/email bots
```

`Ready` remains the only generated-document status that should drive AppSheet completion and sending bots. Do not make bots react to `Generating` or `Generated`.

Current production mode:

```javascript
CONFIG.DOC_GENERATOR.FILE_PROGRESS_STATUSES_ENABLED = true;
CONFIG.DOC_GENERATOR.FILE_PROGRESS_UPDATE_MODE = "generating_only";
```

That means each file may be marked `Generating`, but successful files are batch-marked directly to `Ready` at the end. `Generated` can stay in the enum for compatibility, but the script does not need to write it per file in this mode. Progress status updates are non-blocking in the script, so generation continues even if AppSheet rejects an intermediate enum value.

## Signed upload placeholders

For every successfully generated agreement file, the worker creates a matching row in `Signed_Documents` so the client-returned signed file can be uploaded later.

Static attachments are intentionally excluded because they are stored in `Static_Attachments`, not in `Agreements_Files`.

Deduplication key:

```text
Onboarding_ID + Template_ID_Reference + Category
```

Created row values:

```text
ID                    = short generated ID
Onboarding_ID         = Agreements_Files[Onboarding_ID]
File Extension        = Agreements_Files[File Extension]
File                  = blank
Prefix                = Agreements_Files[Prefix] or File_Name_Prefix
Category              = Agreement
Date_Created          = today
File_status           = Waiting for upload
Template_ID_Reference = Agreements_Files[Template_ID_Reference]
```

## Template placeholders

Supported placeholders / expressions:

```text
<<[nazwa firmy]>>
{{nazwa firmy}}
<<[Onboarding_ID].[nazwa firmy]>>
{{Onboarding_ID.nazwa firmy}}
<<[Template_ID_Reference].[File_Name_Prefix]>>
{{Template.File_Name_Prefix}}
<<TEXT(TODAY(), "DD-MM-YYYY")>>
<<TEXT(TODAY(), "DD/MM/YYYY")>>
<<IF(ISBLANK([Onboarding_ID].[KRS]), "", "...")>>
<<IF(CONTAINS([Onboarding_ID].[website], "invalid"), "n/a", [Onboarding_ID].[website])>>
```

The script replaces values in:

```text
body
header
footer
```

Placeholder replacement is optimized in two layers:

1. Preferred path: Google Docs API `documents.get` + `documents.batchUpdate` with `replaceAllText` requests. Success logs `DOCGEN_PLACEHOLDER_DOCS_API_PASS`.
2. Fallback path: a fast `DocumentApp` text-node pass. It updates only text nodes containing `<<...>>` or `{{...}}`; if unresolved markers remain, it can still fall back to the older section-level replacement logic. This logs `DOCGEN_PLACEHOLDER_FAST_PASS`.

The Docs API path is guarded. If Docs API is disabled or returns a non-success response, the run falls back to `DocumentApp` and still generates PDFs. After the first disabled-API response in one worker execution, Docs API is skipped for the remaining files in that run.

Unsupported for now:

```text
<<Start: ...>>
SELECT(), LOOKUP(), and complex list/table generation
```

If an agreement template uses unsupported expressions, convert them to simple placeholders or extend `13_Docs_Generator.js` for that specific pattern.

## Failure behavior

Default behavior leaves failed rows in:

```text
File_status = "Set Up"
```

This makes the row retryable after fixing the cause. To write an explicit failure status, first add `Failed` to the AppSheet enum and then set:

```javascript
CONFIG.DOC_GENERATOR.FILE_STATUS_FAILED = "Failed";
```

## Performance notes

The generator is queue-backed because Google Docs copy/edit/export is slower than native AppSheet file tasks and Apps Script has a hard execution-time limit.

Current optimizations:

- Read-heavy lookups for `Agreements_Files`, `BIBIV_onboarding_APP`, and `Doc_Templates` normally use AppSheet API. A disabled optional fast path (`CONFIG.DOC_GENERATOR.USE_SHEET_READS=false`) can read the underlying Google Sheet through the Google Sheets API, but it requires Sheets API to be enabled in the script's Cloud project. Writes always go through AppSheet API so data-change bots and AppSheet state remain authoritative.
- Placeholder replacement normally uses Google Docs API. This requires the script to be attached to a standard Google Cloud project with Google Docs API enabled and OAuth consent configured. The combined manual authorization helper is `runAuthorizeDocGeneratorAllAccess()`.
- AppSheet file-row events enqueue by `Job_ID`, not by individual PDF. Duplicate events for the same `Job_ID` log `added:false` and do not reset the one-shot trigger.
- `Doc_Templates` rows and the main onboarding row are cached during one worker execution so continuation chunks do not refetch stable metadata.
- Each chunk processes at most `CONFIG.DOC_GENERATOR.MAX_FILES_PER_RUN` files, currently `10`. The worker immediately continues with the same `Job_ID` while its runtime budget allows, and checks the time budget before starting another file so it can yield before timeout.
- When the worker yields because the runtime budget is nearly spent, it creates a fresh one-shot trigger with `refresh:true`. This matters because the currently executing trigger is still visible to Apps Script until the function exits.
- Rows stuck in `Generating` after a timeout are included in the next retry pass.
- Successful files are batch-marked `Ready` after PDFs are created; the per-file `Generated` write is skipped in `generating_only` progress mode.
- `Agreements_Files` and `Generation_Job_Items` final status updates are batched after PDFs are created.
- Drive folder lookups are cached during one execution.
- Placeholder replacement builds replacement requests only for markers found in the copied document. It avoids per-field full-document scans.
- Timing logs named `DOCGEN_TIMING_*` identify whether slow runs are spending time in Drive copy, Docs open/save, placeholder replacement, PDF export, or file creation.
- Working Google Docs copies are trashed after PDF export by default (`KEEP_WORKING_DOC_COPY=false`).
- PDF export targets the first Google Docs tab by URL (`export?format=pdf&tab=...`) to avoid the Docs Tabs cover page.
- Jobs are serialized through a script-property queue. Continuation for the active onboarding is prioritized ahead of later queued jobs so one NIP/Onboarding_ID can finish and send mail before the next starts.

Observed production behavior:

- 2026-05-06: one 7-file agreement job usually took about 2.5-4 minutes once the worker started. Multiple NIPs queued correctly and were processed sequentially.
- 2026-05-13 before placeholder optimization: slow templates could spend 60-140 seconds in `DOCGEN_TIMING_REPLACE_PLACEHOLDERS`.
- 2026-05-13 after the fast `DocumentApp` pass: placeholder replacement dropped to roughly 1-5 seconds per document when no fallback was needed.
- 2026-05-13 after enabling Docs API in a standard Cloud project and fixing the tabbed-docs request: all files logged `DOCGEN_PLACEHOLDER_DOCS_API_PASS`; replacement stayed around 2-4.5 seconds per document, with no fallback.

The main latency outside the code is Apps Script's time-driven trigger startup, which can take roughly 1-2 minutes after the first enqueue.

Remaining bottlenecks: each PDF still requires a Google Docs template copy, Docs API replacement, PDF export, Drive file creation, and AppSheet status writes. Further optimization should focus on Drive/AppSheet I/O rather than placeholder replacement.

## Manual maintenance helpers

Manual helper functions live in `apps-script-docs-creator/14_Manual_Maintenance.js`.

Authorization / setup:

```text
runAuthorizeDocGeneratorAllAccess()
runAuthorizeDocGeneratorDocsApiAccess()
runAuthorizeDocGeneratorSheetAccess()
```

Run `runAuthorizeDocGeneratorAllAccess()` after switching the Apps Script project to a standard Google Cloud project. It checks ScriptApp token access, UrlFetchApp, DriveApp, DocumentApp, SpreadsheetApp, and the Google Docs API REST endpoint. A successful run logs `DOCGEN_ALL_ACCESS_AUTHORIZATION_END` with `ok:true`.

Manual one-off PDF generation:

```text
runManualGenerateTemplatePdfs()
```

Settings are at the top of `14_Manual_Maintenance.js` in `MANUAL_TEMPLATE_PDF_GENERATION`:

```javascript
ONBOARDING_IDS_TEXT: "ID00000001, ID00000003",
ONBOARDING_IDS: ["ID00000005"],
OUTPUT_FOLDER_ID: "Drive folder id",
TEMPLATE_DOC_ID: "Google Docs template id"
```

The manual generator is intentionally separate from the main queue. It reads the same onboarding data, creates a dated output folder under the selected root using `YYYY-MM-DD__vN`, and uses the same PDF naming pattern as the main agreement flow.

Single PDF generation by NIP:

```text
runGenerateSingleDocumentForNip()
```

Settings are at the top of `apps-script-docs-creator/15_Single_Document_Generator.js`:

```javascript
const SINGLE_DOCUMENT_GENERATION = {
  NIP: "1234567890",
  TEMPLATE_ID: "Google Docs template id or URL",
  OUTPUT_FOLDER: ""
};
```

`OUTPUT_FOLDER` is optional. When blank, the PDF is saved under `Files_Single_Generations_/YYYY-MM-DD`. A custom value may be a folder name under the configured onboarding Drive root, a Drive folder ID, or a Drive folder URL. The function finds exactly one main onboarding row by NIP, generates one PDF with the production placeholder engine, and returns the PDF URL and IDs in the execution result. It does not enqueue a job and does not write to AppSheet, statuses, or source sheets.

For programmatic calls, use:

```javascript
generateSingleDocumentForNip(nip, templateId, outputFolder)
```
