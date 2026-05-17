# API Integrations

## 1. GOV API at `gov.api.hypnotype.com`

Four endpoints, called in this **fixed order** per row:

### REGON (mandatory, first)
- Input: NIP.
- Output: company subject + address structure.
- **REGON accepts NIP** — start here, not from VAT.
- If REGON returns an HTTP/fetch/no-data error, stop the row, mark `MF_REGON_BLOCK`, and set `Status = "need verification"`.
- `name_api` is sourced from REGON. If REGON returns a valid row but no name, source-sheet `nazwa firmy` is the last-resort fallback.

### VAT
- Input: NIP (+ optional date).
- Retry strategy:
  1. With `today's date`.
  2. With no date.
- Returns `accountNumbers` if VAT-registered.
- If VAT returns no subject, the row is treated as `Not VAT`; REGON data still supplies the company identity/address.
- On hard error (block / rate-limit) → marker `MF_VAT_BLOCK` or `MF_RATE_LIMIT`; skip IBAN.

### IBAN
- Input: each account number from VAT result.
- **Cache**: `CacheService.getScriptCache()` keyed by account number.
- **Skip entirely if the row already has full bank metadata** (avoids burning quota — earlier we burned 1500 calls before this guard).
- **Skip if REGON or VAT had a hard error** (no useful work to do).

### KNF / RPK
- Input: NIP.
- Endpoint: `/v1/knf/rpk?nip=...`.
- Writes verified RPK number to append-only column `KNF_verified`.
- Non-blocking: HTTP/fetch/no-result issues are logged and must not prevent AppSheet Add.
- Manual repair in `14_Manual_Maintenance.js`:
  - `runManualRefreshKnfVerified()` fills missing values for existing rows.
  - `runManualRefreshKnfVerifiedForceAll()` overwrites existing values.
  - `runManualRefreshKnfVerifiedForNips()` targets `MANUAL_KNF_VERIFICATION.NIPS_TEXT` / `NIPS`.

## 2. AppSheet REST API v2

| Item | Value |
|---|---|
| Base | `https://www.appsheet.com/api/v2/apps/{appId}/tables/{table}/Action` |
| Auth | header `ApplicationAccessKey: <key>` |
| Body | `{ Action, Properties: { Locale: "pl-PL", Timezone }, Rows: [row] }` |
| Actions used | `Add`, `Edit` |

**Detection of schema drift** (sheet structure changed in AppSheet):
- Response body contains "mismatch in number of columns" or "regenerate the table column structure" → marker `APPSHEET_SCHEMA_MISMATCH`. Apps Script does not retry; user must regenerate column structure in AppSheet.

**Webhook from inside AppSheet** (used by `JOB - create job items` bot):
- POST to the same REST API on `Generation_Job_Items`.
- Body uses `<<Start: ORDERBY(SELECT(Doc_Templates[Template_ID], …), [File_Name_Prefix], FALSE)>>` to expand one row per active template, with `[_THISROW-1]` to access the parent `Generation_Jobs` row.
- Body is intentionally **minimal** — only `Template_ID` and `Item_Status="Queued"`. All other columns auto-fill via initial-value formulas on `Generation_Job_Items` (e.g. `[Job_ID].[Onboarding_ID]`, `[Template_ID].[Folder_Path]`).
- Quote-escaping issues in templates were the main reason to keep the body minimal.

## Rate-limit / failure markers

| Marker | Source | Recovery |
|---|---|---|
| `MF_RATE_LIMIT` | GOV VAT 429/rate limit | Auto-retry next pass |
| `MF_REGON_BLOCK` | REGON hard error | Manual investigation |
| `MF_VAT_BLOCK` | VAT hard error | Manual investigation |
| `MF_NOT_VAT` | REGON subject:null | OK — fast path |
| `MF_NO_SUBJECT` | Transient empty | Retry next pass |
| `APPSHEET_SCHEMA_MISMATCH` | AppSheet response | Regenerate columns in AppSheet UI |
| `APPSHEET_WAITING_MF_DATA` | `evaluateMfReadinessForAdd_` failed | Wait for MF success |
| `APPSHEET_FAIL` | HTTP non-2xx | Inspect log + payload |
