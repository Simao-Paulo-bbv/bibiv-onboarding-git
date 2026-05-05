# Conventions & Pitfalls

> All of these were learned the hard way. Following them keeps the system stable.

## DO NOT

### ❌ Do not reorder or truncate DEST / AppSheet schema columns
- **Always append new columns to the end.** Reordering shifts all values one column over and silently corrupts rows.
- The non-destructive header repair (`repairDestHeadersOnlyAfterQueueInsert`) is intentional — never replace it with a "fix everything" version.
- Applies equally to `swift/bic`, `Bank name`, `Bank address`, `Bank city`, sales-rep fields, and any future fields.

### ❌ Do not overwrite AppSheet-managed Status from Apps Script
- Once a row exists in AppSheet, AppSheet owns `Status`. The script wrote `Init`, AppSheet flipped to `New`, the script flipped back to `Init`, ad infinitum.
- **Rule**: write Status only Just-In-Time on a truly fresh row (live status blank AND `idAssignedNow`). On Edit, strip Status from payload entirely.

### ❌ Do not use `LOOKUP(USEREMAIL(), "BIBIV_onboarding_APP", "Generation_Triggered_By", "ID")`
- This is the bug that mixed records across users — second user inherited first user's record context.
- Replaced entirely by `Generation_Jobs[Onboarding_ID]` carried through `Generation_Job_Items` → `Agreements_Files`.
- If you see this LOOKUP anywhere, delete the action.

### ❌ Do not glue `Folder_Path` with NIP without `"/"`
- `Files_Application_5531549891/...` (missing slash) → broken folder routing.
- `Folder_Path` is a **constant root** (`Files_Application_` / `Files_Agreements_`). NIP is a **subfolder**.
- Formulas always use explicit `"/"`:
  ```
  CONCATENATE([Folder_Path], "/", [NIP_Control], "/", [File_Name_Prefix],
              "__", [NIP_Control], "__", TEXT(TODAY(), "DD-MM-YYYY"),
              [File_Extension])
  ```

### ❌ Do not block on missing phone
- AppSheet schema does not require phone. Don't gate Add on it. (Email contact is required and gets a deterministic fallback.)

### ❌ Do not send `(null) null-null` or `//` or `-` as real values
- These come from Squarespace when fields are emptied or templated. Normalize to `""` (or `null` for birth dates).
- Birth-date columns specifically: empty string would trigger AppSheet's default-value behavior and write today's date. Use `null`.

### ❌ Do not call IBAN on every row
- Cache by account number with `CacheService.getScriptCache()`.
- Skip if row already has complete bank metadata.
- Skip if REGON or VAT had a hard error.
- We burned 1500 calls before adding these guards.

### ❌ Do not delete DEST rows expecting clean re-import
- DEST deletion alone does NOT trigger re-import — `_Import_History` still has the `(NIP, SubmittedOn)` key.
- If a manual re-import is needed: clear the relevant rows in `_Import_History` AND set `SOURCE_REIMPORT_IF_MISSING_IN_DEST=true`.

### ❌ Do not skip the MF readiness gate before AppSheet Add
- `evaluateMfReadinessForAdd_` must pass: `statusVat` + `name_api` + `regon` + working/residence address.
- Not-VAT records get the fast path with synthesized residence address.
- Skipping the gate causes `APPSHEET_FAIL` 400 loops.

### ❌ Do not run `Add templates files (multiple)` action
- The original direct-from-Doc_Templates copy. Caused empty NIPs and mixed records.
- Delete it. Use the `Generation_Jobs` queue exclusively.

## DO

### ✅ Treat AppSheet "duplicate key" on People_List Add as success
- Deterministic PersonID guarantees same person → same key. Duplicate error means the row already exists, which is correct.

### ✅ Make all bots that update chained data set "Trigger other bots = ON"
- The queue depends on this. Without it, `JOB - create job items` won't fire after `JOB - start queued job` updates the parent row.

### ✅ Place the webhook step inside the **same** bot as the start action
- Earlier attempt: a separate "Updates only" bot for the webhook didn't fire reliably after another bot's update. Inline the webhook in the bot whose event triggers it.

### ✅ Keep webhook templates minimal — use initial-value formulas for everything else
- `<<Start: …>>` body only carries `Template_ID` and `Item_Status`. Other columns auto-fill via:
  - `[Job_ID].[Onboarding_ID]`
  - `[Job_ID].[NIP_Control]`
  - `[Template_ID].[Folder_Path]`
  - etc.
- Quote-escape errors in big template bodies were a recurring source of webhook failures.

### ✅ Use `[_THISROW-1]` to access parent row inside `<<Start: SELECT(...)>>`
- `[_THISROW]` inside the SELECT iterates the SELECT rows, not the bot's triggering row.
- `[_THISROW-1]` steps up one level to the parent (e.g. `Generation_Jobs`).

### ✅ Verify by NIP, not by row number
- DEST rows can be deleted/re-inserted. Row numbers shift. NIP + SubmittedOn is stable.

### ✅ Use GOV API for all enrichment
- REGON, VAT, and IBAN enrichment all go through `gov.api.hypnotype.com`.
- Do not reintroduce direct MF or relay paths in the Apps Script.

## Recovery patterns

### "Old records re-imported after I deleted DEST rows"
- Check `_Import_History` — entries by `(NIP, SubmittedOn)` are the durable dedupe.
- To force a re-import: delete the matching `_Import_History` row(s).

### "AppSheet Add fails with 'mismatch in number of columns'"
- Marker `APPSHEET_SCHEMA_MISMATCH`.
- Open AppSheet → Data → Tables → "Regenerate column structure" on the affected table.
- Verify `APPSHEET_SCHEMA` in `00_Config.js` matches.

### "MF stuck on `MF_NO_SUBJECT`"
- Transient. Wait one cycle. If persistent, check upstream `gov.api.hypnotype.com` health.

### "Generation job stuck — items created but no files"
- Check that `Generate Applications` / `Generate Agreements` bots are enabled and watching `Agreements_Files` Adds.
- Verify `File_status` transitions: `"Set Up"` → `"Ready"`.
- Verify `Folder_Path` and `File` formula output — the most common silent failure is a missing `"/"`.

### "Apps Script broke after I added a column in AppSheet"
- Don't reorder DEST. Add the new column at the END of `DEST_SCHEMA` and `APPSHEET_SCHEMA`.
- If `APPSHEET_MAIN_ALLOWED_COLS` gates payload, add the column there too.
