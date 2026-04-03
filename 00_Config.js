/*******************************************************
 * BIBIV™ Onboarding Sync + MF VAT + AppSheet (FIXED)
 * + People roles: Contact / Manager / BeneficialOwner
 * + Writes PersonID refs back to Main table:
 *   ContactPersonID / ManagerPersonID / BeneficialOwnerPersonID
 *******************************************************/

const CONFIG = {
  DEST_SPREADSHEET_ID: "1EaefdCq8vT6QZO6fxazxGZXBiReR1Y7zZ4FA7tdwebA",
  DEST_SHEET_NAME: "Sheet1",

  SOURCE_SPREADSHEET_ID: "14DW1bMACzUnerIJlsAu2qJFp5lGtaaIOuGCn6cuQYi4",
  SOURCE_SHEET_NAME: "Sheet1",

  LOG_SPREADSHEET_ID: "1EaefdCq8vT6QZO6fxazxGZXBiReR1Y7zZ4FA7tdwebA",
  LOG_SHEET_NAME: "LOGS",

  MAX_ROWS_PER_RUN: 20,
  MAX_RUNTIME_MS: 45000,
  LOCK_TIMEOUT_MS: 20000,

  MAX_APPSHEET_LOG_BODY_CHARS: 8000,
  PENDING_SCAN_LAST_N: 5000, // for huge sheets: scan only last N rows for pending processing


  FEATURES: {
    ENFORCE_DEST_HEADERS: true,
    AUTO_CREATE_LOG_SHEET: true,
    MF_ENABLED: true,
    MF_SKIP_IF_MF_OK: true,
    MF_SKIP_IF_DATA_PRESENT: true,
    APPSHEET_ENABLED: true,
    PEOPLE_LIST_ENABLED: true,
    DRY_RUN: false,
  },

  LOGGING: {
    ENABLE: true,
    LEVEL: "VERBOSE", // OFF | SUMMARY | VERBOSE
    TO_CONSOLE: true,
    TO_EXECUTION_LOGGER: false,
    TO_SHEET: false,
  },

  SOURCE_MARK_COLUMN_NAME: "sync_status",
  // Markowanie w SOURCE:
  // - IN_DEST  ...  => wiersz skopiowany do DEST (nie blokuje ponownego importu, gdy wyczyścisz DEST)
  // - DONE ...      => pełny proces zakończony (MF + AppSheet), wiersz nie jest ponownie przetwarzany
  SOURCE_MARK_PREFIX_IMPORT: "IN_DEST",
  SOURCE_MARK_PREFIX_DONE: "DONE",

  // Dedupe strategy.
  // Problem: "submitted on" może mieć różny format (string / Date / ISO), co rozjeżdża klucz deduplikacji.
  // W praktyce NIP jest wystarczająco stabilnym kluczem dla importu z formularza.
  DEDUPE: {
    MODE: "NIP_ONLY", // "NIP_ONLY" | "NIP_AND_SUBMITTED"
  },

  DEST_SYNC_STATUS_COL: "sync_status",
  SKIP_IF_DEST_HAS_APPSHEET_OK: true,

  FORCE_IMPORT: false,
  FORCE_IMPORT_LIMIT: 1,

  MF_API_URL: "https://wl-api.mf.gov.pl/api/search/nip/{nip}?date={date}",
  MF_TIMEOUT_MS: 15000,

  APPSHEET_APP_ID: "ebb1aa13-9408-4a7d-8d41-8cb03b9e766f",
  APPSHEET_TABLE_MAIN: "BIBIV_onboarding_APP",
  APPSHEET_TABLE_PEOPLE: "People_List",
  APPSHEET_ACCESS_KEY: "V2-dLnyp-uEkhm-vaJQY-lQAmj-czhAH-xLbR3-dA8Oc-oFxgE",
  APPSHEET_API_URL: "https://www.appsheet.com/api/v2/apps/{appId}/tables/{table}/Action",
  APPSHEET_TIMEOUT_MS: 20000,

  APPSHEET_ACTION_ADD: "Add",
  APPSHEET_ACTION_EDIT: "Edit",

  NIP_AS_NUMBER: false,

  STATUS_TO_SEND: "Init",
  WRITE_STATUS_JUST_IN_TIME: true,

  MAX_PAYLOAD_PREVIEW_CHARS: 2000,
  MAX_RESPONSE_SNIPPET_CHARS: 1400,

  // People_List mapping (AppSheet column names)
  PEOPLE: {
    COL_PERSON_ID: "PersonID",                       // key [Text]
    COL_FULL_NAME: "FullName",                       // [Text]
    COL_FULL_NAME_KEY: "FullNameKey",                // [Text]
    COL_ROLE: "Role",                                // [Text]
    COL_SOURCE_FULLNAME_COL: "SourceFullNameColum",  // [Name]
    COL_ONBOARDING_ID: "OnboardingID",               // [Ref] -> Main.ID
  },

  // Main table Ref columns (must be PHYSICAL columns)
  MAIN_REF_COLS: {
    CONTACT: "ContactPersonID",
    MANAGER: "ManagerPersonID",
    BENEFICIAL: "BeneficialOwnerPersonID",
  },

  MARKERS: {
    MAIN_OK: "APPSHEET_OK",
    PEOPLE_OK: "PEOPLE_OK",
  }
};

/**
 * SOURCE header aliases -> DEST canonical headers.
 * Dzięki temu zmiana nazwy kolumny w arkuszu Squarespace nie psuje importu.
 * Klucze i wartości są normalizowane (trim + lower) w normalizeKey_.
 */
const HEADER_ALIASES = {
  "adres": "address",
  "adres strony internetowej": "website",
  "czy posiadasz wpis do rejestru rejestru pośredników kredytowych rpk w knf": "posiadam wpis do knf",
  "numer rpk w knf": "numer wpisu do knf",
  "numer rachunku firmowego iban": "numer rachunku bankowego",
  "email służbowy do kontaktu": "email osoby kontaktowej",
};

/** =========================
 *  DEST schema (Google Sheet)
 *  NOTE: dodałem 3 kolumny Ref do people
 *  ========================= */
const APPSHEET_SCHEMA = [
  "_RowNumber",
  "ID",
  "Status",
  "NIP_Control",
  "Lead Created",
  "Bank aproval",
  "Documents sent to client",
  "Documents sent to bank",
  "name_api",
  "statusVat",
  "regon",
  "krs",
  "registrationLegalDate",
  // MF API extras
  "workingAddress",
  "residenceAddress",
  "accountNumbers",
  "Is_Generating_Now",
  "Generation_Triggered_By",
  "sync_status",
  "submitted on",
  "nazwa firmy",
  "nip",
  "address",
  "mam firmową stronę internetową",
  "website",
  "zakres działalności",
  "posiadam wpis do knf",
  "numer wpisu do knf",
  "numer rachunku bankowego",
  "kod swift banku",
  "imię i nazwisko osoby kontaktowej",
  "email osoby kontaktowej",
  "numer telefonu osoby kontaktowej",
  "pesel osoby kontaktowej",
  "imię i nazwisko kierownika",
  "data urodzenia 6",
  "imię i nazwisko beneficjenta",
  "numer dowodu beneficjenta",
  "pesel beneficjenta",
  "data urodzenia",
  "powiązania z sektorem publicznym",
  "konflikt interesów",
  "powiązania z grupą rbi",
  "postępowanie upadłościowe",
  "zgody i oświadczenia",

  // NEW: refs to People_List
  "ContactPersonID",
  "ManagerPersonID",
  "BeneficialOwnerPersonID",
];

const DEST_SCHEMA = APPSHEET_SCHEMA.filter(h => h !== "_RowNumber");

const SYSTEM_DEFAULTS = {
  "Lead Created": "",
  "Bank aproval": "",
  "Documents sent to client": "",
  "Documents sent to bank": "",
  "Is_Generating_Now": "",
  "Generation_Triggered_By": "",
  "name_api": "",
  "statusVat": "",
  "regon": "",
  "krs": "",
  "registrationLegalDate": "",
  "workingAddress": "",
  "residenceAddress": "",
  "accountNumbers": "",
  "sync_status": "",
};

/**
 * AppSheet MAIN allowlist.
 * Payload jest filtrowany – dzięki temu różnice w kolumnach nie wysypią API.
 */
const APPSHEET_MAIN_ALLOWED_COLS = [
  "ID",
  "Status",
  "NIP_Control",
  // Ref do People_List (wypełniane przez skrypt)
  "ContactPersonID",
  "ManagerPersonID",
  "BeneficialOwnerPersonID",
  "Lead Created",
  "Bank aproval",
  "Documents sent to client",
  "Documents sent to bank",
  "name_api",
  "statusVat",
  "regon",
  "krs",
  "registrationLegalDate",
  "workingAddress",
  "residenceAddress",
  "accountNumbers",
  "Is_Generating_Now",
  "Generation_Triggered_By",
  "sync_status",
  "submitted on",
  "nazwa firmy",
  "nip",
  "address",
  "mam firmową stronę internetową",
  "website",
  "zakres działalności",
  "posiadam wpis do knf",
  "numer wpisu do knf",
  "numer rachunku bankowego",
  "kod swift banku",
  "imię i nazwisko osoby kontaktowej",
  "email osoby kontaktowej",
  "numer telefonu osoby kontaktowej",
  "pesel osoby kontaktowej",
  "imię i nazwisko kierownika",
  "data urodzenia 6",
  "imię i nazwisko beneficjenta",
  "numer dowodu beneficjenta",
  "pesel beneficjenta",
  "data urodzenia",
  "powiązania z sektorem publicznym",
  "konflikt interesów",
  "powiązania z grupą rbi",
  "postępowanie upadłościowe",
  "zgody i oświadczenia",

  // NEW refs:
  "ContactPersonID",
  "ManagerPersonID",
  "BeneficialOwnerPersonID",
];
