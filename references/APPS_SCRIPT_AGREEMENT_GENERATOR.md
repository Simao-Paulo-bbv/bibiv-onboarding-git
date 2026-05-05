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

The worker is created automatically by `ensureDocGenerationQueueTrigger()`. It uses a short one-shot trigger and then drains the queue within a safe runtime budget. A `Job_ID` is still processed in chunks controlled by `CONFIG.DOC_GENERATOR.MAX_FILES_PER_RUN`, but continuation chunks for the same job are put back at the front of the queue and are picked up immediately by the same worker while time remains.

Recommended AppSheet parameters:

```text
onboardingId      = [Onboarding_ID]
jobId             = [Job_ID]
agreementFileId   = [ID]
```

The script uses `jobId` first. One queued job generates pending agreement files for that job. If more files remain after the safe per-run limit, the script re-enqueues the same `Job_ID` for continuation.

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
   - marks the file row as `Generated`.

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

Before enabling visible progress in AppSheet, add `Generating` and `Generated` to the `Agreements_Files[File_status]` enum. Progress status updates are non-blocking in the script, so generation continues even if AppSheet rejects those intermediate enum values.

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

- Read-heavy lookups for `Agreements_Files`, `BIBIV_onboarding_APP`, and `Doc_Templates` can use the underlying Google Sheet directly through the Google Sheets API (`CONFIG.DOC_GENERATOR.USE_SHEET_READS=true`) with fallback to AppSheet API. The script manifest uses only the read-only spreadsheet scope; writes still go through AppSheet API so data-change bots and AppSheet state remain authoritative.
- `Doc_Templates` rows are preloaded with one AppSheet `Find` per job.
- Each chunk processes at most `CONFIG.DOC_GENERATOR.MAX_FILES_PER_RUN` files. The worker immediately continues with the same `Job_ID` while its runtime budget allows, then schedules a short one-shot continuation trigger if it must yield before timeout.
- Rows stuck in `Generating` after a timeout are included in the next retry pass.
- `Agreements_Files` and `Generation_Job_Items` status updates are batched after PDFs are created.
- Drive folder lookups are cached during one execution.
- Placeholder replacement avoids scanning the document for fields that are not present in the template.
- Timing logs named `DOCGEN_TIMING_*` identify whether slow runs are spending time in Drive copy, Docs open/save, placeholder replacement, PDF export, or file creation.
- Working Google Docs copies are trashed after PDF export by default (`KEEP_WORKING_DOC_COPY=false`).
- PDF export targets the first Google Docs tab by URL (`export?format=pdf&tab=...`) to avoid the Docs Tabs cover page.
- Jobs are serialized through a script-property queue. Continuation for the active onboarding is prioritized ahead of later queued jobs so one NIP/Onboarding_ID can finish and send mail before the next starts.

Remaining bottleneck: each PDF still requires a Google Docs template copy, text replacement, save, and PDF export. That is the slowest part and cannot be made instant without moving to pre-rendered/static PDFs or a lower-fidelity HTML/PDF renderer.
