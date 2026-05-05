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

The first time-driven worker dispatches file tasks:

```text
processNextQueuedDocGenerationJob()
```

It is created automatically by `ensureDocGenerationQueueTrigger()` and runs every minute. It takes one queued `Job_ID`, finds its pending `Agreements_Files` rows, and enqueues one file task per document.

The actual PDF work is done by bounded per-file workers:

```text
processNextAgreementFileTask()
```

Worker concurrency is intentionally single-lane and scoped to one active generation job:

```text
CONFIG.DOC_GENERATOR.FILE_WORKER_PARALLELISM = 1
CONFIG.DOC_GENERATOR.FILE_WORKER_BATCH_MAX_ITEMS = 7
```

The active unit is `Job_ID` first, with `Onboarding_ID` as fallback. This keeps parallelism inside one onboarding/NIP package instead of spreading worker time across many different NIPs.

The finalizer is:

```text
processNextAgreementFinalizer()
```

It checks whether all expected files for a `Job_ID` have a stored file path, creates missing `Signed_Documents` upload rows, batch marks `Agreements_Files` as `Ready`, batch marks `Generation_Job_Items` as `Agreement file created`, and only then updates the main row to `Agreements Generated`.

Manual maintenance function:

```text
resetDocGenerationQueuesManual()
```

Use it only when no valid agreement generation should be running. It clears `DOCGEN_ACTIVE_JOB`, the dispatcher/file/finalizer queues, and generator time triggers.

Recommended AppSheet parameters:

```text
onboardingId      = [Onboarding_ID]
jobId             = [Job_ID]
agreementFileId   = [ID]
```

The script uses `jobId` first. One queued job dispatches all pending agreement files for that job, while per-file workers do the heavier Google Docs copy/export work.

## Required config before production

In `apps-script-docs-creator/00_Config.js`, set:

```javascript
CONFIG.DOC_GENERATOR.OUTPUT_ROOT_FOLDER_ID = "1iHYmHQCpA4IHEfnjrV7okk3sqjw6MkOd"
```

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

## New AppSheet bot

Disable the old bot:

```text
Generate Agreements
```

Create / keep this bot:

```text
Generate Agreements - Apps Script
```

Event:

```text
Table: Generation_Job_Items
Data change: Updates only
Condition:
AND(
  [Item_Status] = "File request created",
  ISBLANK(
    ANY(
      SELECT(
        Generation_Job_Items[Job_Item_ID],
        AND(
          [Job_ID] = [_THISROW].[Job_ID],
          [Item_Status] <> "File request created"
        )
      )
    )
  )
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

## What the script does

For a queued job:

1. Dispatcher finds all `Agreements_Files` rows where:

   ```text
   File_status = "Set Up"
   Category = "Agreement"
   Job_ID = current Job_ID
   ```

2. Dispatcher enqueues one per-file task for each pending row.

3. Each per-file worker:
   - marks the file row as `Generating` when progress statuses are enabled,
   - reads `Template_ID_Reference`,
   - finds the matching `Doc_Templates[Template_ID]`,
   - copies the Google Docs template file,
   - replaces simple placeholders,
   - exports the copy as PDF,
   - writes the PDF to `Agreements_Files[File]` path,
   - marks the file row as `Generated`.

4. Finalizer waits until all files for the job physically exist in Drive, then creates missing `Signed_Documents` upload rows for generated agreement docs.

5. Finalizer batch marks:

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

Before relying on visible progress in AppSheet, add `Generating` and `Generated` to the `Agreements_Files[File_status]` enum. `Agreements_Files[File]` is a planned path and is not treated as proof that the PDF exists; the finalizer verifies the file in Drive before writing `Ready`.

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

- Jobs are dispatched quickly and the slow Google Docs copy/export work starts inline in one active worker batch.
- `FILE_WORKER_PARALLELISM = 1` is intentional: it avoids duplicate worker triggers, Apps Script trigger quota issues, and `already running` claim collisions.
- A worker processes a batch of files from the active job, not just one file, so the remaining files do not wait for repeated 1-minute trigger delays.
- Dispatcher starts the first worker batch inline right after enqueueing tasks, so generation begins immediately when the job is picked up.
- Finalizer runs inline as soon as the worker queue is empty, so `Generated` files are promoted to `Ready` without waiting for a separate time trigger.
- `DOCGEN_ACTIVE_JOB` blocks dispatching the next queued job until the current job finalizer has written `Ready` for the complete file set.
- If the active job is waiting but worker tasks are missing, dispatcher/finalizer rebuild missing file tasks from `Agreements_Files` and starts a recovery batch.
- Stale worker tasks whose `Agreements_Files[ID]` no longer exists are consumed without retry, so old queue entries cannot block the current job.
- Dispatcher deletes its own time trigger when there is no active job and no queued job, so the script does not keep polling forever.
- Finalizer verifies the physical PDF exists in Drive; a prefilled `Agreements_Files[File]` path alone is never enough to mark a row `Ready`.
- `Agreements_Files` and `Generation_Job_Items` status updates are batched after PDFs are created.
- Drive folder lookups are cached during one execution.
- Working Google Docs copies are trashed after PDF export by default (`KEEP_WORKING_DOC_COPY=false`).
- PDF export targets the first Google Docs tab by URL (`export?format=pdf&tab=...`) to avoid the Docs Tabs cover page.
- `Ready` is written only by the finalizer, so existing completion/email bots do not send before all generated files and upload placeholders exist.

Remaining bottleneck: each PDF still requires a Google Docs template copy, text replacement, save, and PDF export. That is the slowest part and cannot be made instant without moving to pre-rendered/static PDFs or a lower-fidelity HTML/PDF renderer.
