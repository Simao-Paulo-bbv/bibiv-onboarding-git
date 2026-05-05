/*******************************************************
 * 11_Bank_Accounts.gs  (FINAL)
 * -----------------------------------------------------
 * Rozbijanie kont bankowych do tabeli/arkusza Bank_Accounts
 * (1 wiersz = 1 konto), powiązane przez Onboarding_ID = Main.ID.
 *
 * ŹRÓDŁO DANYCH (DEST / tabela główna):
 * - Preferowana kolumna: "accountNumbers" (z MF VAT API)
 * - Fallback: "numer rachunku bankowego"
 *
 * ✅ Nie zmienia pola w tabeli głównej.
 * ✅ Nie rusza sync_status (żeby nie psuć logiki mainAlreadyOk / JIT Status=Init).
 * ✅ Nie mieli w kółko: jeśli lista kont się nie zmieniła, NIE usuwa i NIE dodaje.
 * ✅ Wymusza format tekstowy (żeby nigdy nie było notacji E+).
 *
 * WYMAGANIA (Google Sheets):
 * - Arkusz: "Bank_Accounts"
 * - Nagłówki (row 1) minimum:
 *     AccountID | Onboarding_ID | AccountNumber | CreatedAt
 *******************************************************/

const BANK_ACCOUNTS_SHEET_NAME_DEFAULT = "Bank_Accounts";
const BANK_ACCOUNTS_HEADERS = ["AccountID", "Onboarding_ID", "AccountNumber", "CreatedAt"];
const BANK_ACCOUNTS_RUNTIME_CACHE = {};

/**
 * Hook: wołaj z 04_Process.gs dla każdego rekordu, np. tuż przed wysłaniem do AppSheet.
 *
 * @param {string} runId
 * @param {GoogleAppsScript.Spreadsheet.Sheet} destMainSheet
 * @param {Object} mapping
 * @param {number} rowNum
 * @param {string} onboardingId
 */
function syncBankAccountsFromMainRow_(runId, destMainSheet, mapping, rowNum, onboardingId) {
  try {
    if (!onboardingId) return;
    const rawPrimary = getDestCellByColName_(destMainSheet, mapping, rowNum, "accountNumbers");
    const rawFallback = getDestCellByColName_(destMainSheet, mapping, rowNum, "numer rachunku bankowego");

    // Priorytet:
    // 1) accountNumbers (z MF), jeśli niepuste
    // 2) numer rachunku bankowego (fallback formularzowy)
    const csvRawPrimary = String(rawPrimary || "").trim();
    const csvRawFallback = String(rawFallback || "").trim();
    const csvRaw = csvRawPrimary || csvRawFallback;

    if (!hasMappingCol_(mapping, "accountNumbers") && !hasMappingCol_(mapping, "numer rachunku bankowego")) {
      log_(runId, "WARN", "BANK_ACCOUNTS_COL_MISSING", { rowNum, tried: ["accountNumbers", "numer rachunku bankowego"] });
      return;
    }

    const ss = destMainSheet.getParent();
    const bankSheetName =
      (typeof CONFIG !== "undefined" && CONFIG && CONFIG.BANK_ACCOUNTS_SHEET_NAME)
        ? CONFIG.BANK_ACCOUNTS_SHEET_NAME
        : BANK_ACCOUNTS_SHEET_NAME_DEFAULT;

    const bankSheet = getOrCreateBankAccountsSheet_(runId, ss, bankSheetName);
    const accounts = parseAccountCsv_(csvRaw);

    replaceBankAccountsForOnboarding_(runId, bankSheet, String(onboardingId), accounts, rowNum);

    log_(runId, "INFO", "BANK_ACCOUNTS_SYNC_DONE", { rowNum, onboardingId, cnt: accounts.length });
  } catch (e) {
    log_(runId, "WARN", "BANK_ACCOUNTS_SYNC_FAIL", { rowNum, err: String(e).slice(0, 900) });
  }
}

function hasMappingCol_(mapping, colName) {
  return !!(mapping && mapping.dstIndex && mapping.dstIndex[colName] != null);
}

function getDestCellByColName_(destMainSheet, mapping, rowNum, colName) {
  if (!hasMappingCol_(mapping, colName)) return "";
  const idx = mapping.dstIndex[colName];
  return destMainSheet.getRange(rowNum, idx + 1).getValue();
}

function parseAccountCsv_(csvRaw) {
  if (!csvRaw) return [];

  const parts = String(csvRaw)
    .split(",")
    .map((s) => String(s || "").trim())
    .filter(Boolean);

  const cleaned = parts.map((s) => s.replace(/\s+/g, "")).filter(Boolean);

  const seen = {};
  const out = [];
  for (let i = 0; i < cleaned.length; i++) {
    const v = cleaned[i];
    if (!v) continue;
    if (seen[v]) continue;
    seen[v] = true;
    out.push(v);
  }
  return out;
}

/**
 * Nie mieli w kółko:
 * - porównuje istniejące konta vs nowe
 * - jeśli identyczne (w tej samej kolejności) -> SKIP
 * - jeśli różne -> usuwa stare i dodaje nowe
 */
function replaceBankAccountsForOnboarding_(runId, bankSheet, onboardingId, accounts, rowNum) {
  const existing = readExistingAccountsForOnboarding_(bankSheet, onboardingId);

  if (arraysEqual_(existing, accounts)) {
    log_(runId, "INFO", "BANK_ACCOUNTS_SKIP_UNCHANGED", { rowNum, onboardingId, cnt: accounts.length });
    return;
  }

  if (existing.length > 0) {
    deleteAccountsForOnboarding_(runId, bankSheet, onboardingId, rowNum);
  }

  if (!accounts || !accounts.length) {
    log_(runId, "INFO", "BANK_ACCOUNTS_CLEARED_NOW_EMPTY", { rowNum, onboardingId });
    return;
  }

  const now = new Date();
  const rows = accounts.map((acc) => [
    buildDeterministicAccountId_(onboardingId, acc),
    String(onboardingId),
    String(acc),
    now,
  ]);

  bankSheet.getRange(bankSheet.getLastRow() + 1, 1, rows.length, BANK_ACCOUNTS_HEADERS.length).setValues(rows);
  invalidateBankAccountsCache_(bankSheet);

  log_(runId, "INFO", "BANK_ACCOUNTS_UPSERT_OK", { rowNum, onboardingId, inserted: rows.length, prev: existing.length });
}

function readExistingAccountsForOnboarding_(bankSheet, onboardingId) {
  const index = getBankAccountsCache_(bankSheet);
  const node = index.byOnboarding[String(onboardingId || "").trim()];
  if (!node || !node.accounts) return [];
  return node.accounts.slice();
}

function deleteAccountsForOnboarding_(runId, bankSheet, onboardingId, rowNum) {
  const index = getBankAccountsCache_(bankSheet);
  const node = index.byOnboarding[String(onboardingId || "").trim()];
  const rowsToDelete = node && node.rows ? node.rows.slice() : [];

  for (let i = rowsToDelete.length - 1; i >= 0; i--) {
    bankSheet.deleteRow(rowsToDelete[i]);
  }
  if (rowsToDelete.length) invalidateBankAccountsCache_(bankSheet);

  if (rowsToDelete.length) {
    log_(runId, "INFO", "BANK_ACCOUNTS_CLEARED", { rowNum, onboardingId, deleted: rowsToDelete.length });
  }
}

function arraysEqual_(a, b) {
  if (a === b) return true;
  if (!a || !b) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (String(a[i]) !== String(b[i])) return false;
  }
  return true;
}

function getOrCreateBankAccountsSheet_(runId, ss, sheetName) {
  let sh = ss.getSheetByName(sheetName);
  if (!sh) {
    sh = ss.insertSheet(sheetName);
    sh.getRange(1, 1, 1, BANK_ACCOUNTS_HEADERS.length).setValues([BANK_ACCOUNTS_HEADERS]);
    enforceBankAccountsHeadersAndFormats_(sh, true);
    log_(runId, "INFO", "BANK_ACCOUNTS_SHEET_CREATED", { sheetName });
  } else {
    enforceBankAccountsHeadersAndFormats_(sh, false);
  }
  return sh;
}

/**
 * FIX na błąd:
 * "Please make a selection within a single column to perform column level actions."
 * Formatujemy kolumny pojedynczo.
 */
function enforceBankAccountsHeadersAndFormats_(sheet, forceFormats) {
  const header = sheet
    .getRange(1, 1, 1, BANK_ACCOUNTS_HEADERS.length)
    .getValues()[0]
    .map((v) => String(v || "").trim());

  const same = header.join("|") === BANK_ACCOUNTS_HEADERS.join("|");
  if (!same) {
    sheet.getRange(1, 1, 1, BANK_ACCOUNTS_HEADERS.length).setValues([BANK_ACCOUNTS_HEADERS]);
    forceFormats = true;
  }
  if (!forceFormats) return;

  const maxRows = sheet.getMaxRows();
  sheet.getRange(1, 1, maxRows, 1).setNumberFormat("@"); // AccountID
  sheet.getRange(1, 2, maxRows, 1).setNumberFormat("@"); // Onboarding_ID
  sheet.getRange(1, 3, maxRows, 1).setNumberFormat("@"); // AccountNumber
}

function getBankAccountsCacheKey_(sheet) {
  try {
    return String(sheet.getSheetId());
  } catch (e) {
    return String(sheet.getName() || "Bank_Accounts");
  }
}

function invalidateBankAccountsCache_(sheet) {
  const key = getBankAccountsCacheKey_(sheet);
  delete BANK_ACCOUNTS_RUNTIME_CACHE[key];
}

function getBankAccountsCache_(sheet) {
  const key = getBankAccountsCacheKey_(sheet);
  const cached = BANK_ACCOUNTS_RUNTIME_CACHE[key];
  if (cached) return cached;

  const index = { byOnboarding: {} };
  const lastRow = sheet.getLastRow();
  if (lastRow >= 2) {
    const data = sheet.getRange(2, 1, lastRow - 1, BANK_ACCOUNTS_HEADERS.length).getValues();
    for (let i = 0; i < data.length; i++) {
      const onboardingId = String(data[i][1] || "").trim();
      if (!onboardingId) continue;
      const acc = String(data[i][2] || "").trim();
      if (!index.byOnboarding[onboardingId]) {
        index.byOnboarding[onboardingId] = { accounts: [], rows: [] };
      }
      index.byOnboarding[onboardingId].rows.push(i + 2);
      if (acc) index.byOnboarding[onboardingId].accounts.push(acc);
    }
  }

  BANK_ACCOUNTS_RUNTIME_CACHE[key] = index;
  return index;
}

function buildDeterministicAccountId_(onboardingId, accountNumber) {
  const raw = String(onboardingId || "").trim() + "|" + String(accountNumber || "").trim();
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, raw, Utilities.Charset.UTF_8);
  const b64 = Utilities.base64EncodeWebSafe(digest);
  return "BA_" + b64.slice(0, 22);
}
