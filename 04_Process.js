/** =========================
 *  PROCESSING (ID + MF + People + AppSheet Main)
 *  ========================= */
function processDestRows_(runId, mapping, source, dest, startRow, endRow, startedAt, rowMap, markIdx) {
  const rowMapByDest = {};
  (rowMap || []).forEach((m) => {
    if (m && m.destRow != null) rowMapByDest[String(m.destRow)] = m.srcRow;
  });

  // Index kolumny markującej w SOURCE (0-based). Jeśli brak / -1, nie dotykamy SOURCE.
  let __markIdxNum = markIdx;
  // Accept numeric strings from mapping (e.g. "25")
  if (typeof __markIdxNum === 'string') {
    const t = __markIdxNum.trim();
    if (t !== '' && !isNaN(Number(t))) __markIdxNum = Number(t);
  }
  const sourceMarkIdx = (typeof __markIdxNum === 'number' && __markIdxNum >= 0) ? __markIdxNum : -1;

  if (startRow <= 0 || endRow <= 0 || endRow < startRow) return 0;

  const idIdx = mapping.destKey.idIdx;
  const statusIdx = mapping.destKey.statusIdx;
  const nipControlIdx = mapping.destKey.nipControlIdx;
  const nipIdx = mapping.destKey.nipIdx;
  const syncIdx = mapping.destKey.syncStatusIdx;

  const contactPersonIdx = mapping.destKey.contactPersonIdx;
  const managerPersonIdx = mapping.destKey.managerPersonIdx;
  const beneficialPersonIdx = mapping.destKey.beneficialPersonIdx;

  const rowsToRead = endRow - startRow + 1;
  const data = dest.getRange(startRow, 1, rowsToRead, DEST_SCHEMA.length).getValues();

  let processed = 0;

  for (let i = 0; i < data.length; i++) {
    if (Date.now() - startedAt > CONFIG.MAX_RUNTIME_MS - 2500) break;

    const rowNum = startRow + i;
    const row = data[i];

    const nipRaw = String(row[nipControlIdx] || row[nipIdx] || "").trim();
    if (!nipRaw) {
      log_(runId, "WARN", "ROW_SKIP_NO_NIP", { rowNum });
      continue;
    }

    const currentSync = syncIdx != null ? String(row[syncIdx] || "") : "";
    const mainAlreadyOk = currentSync.indexOf(CONFIG.MARKERS.MAIN_OK) >= 0;

    // Czy rekord w AppSheet MAIN jest już poprawnie zapisany.
    // Uwaga: mainOk MUSI być zawsze zainicjalizowane, bo używamy go później do markowania SOURCE.
    let mainOk = mainAlreadyOk;

    log_(runId, "INFO", "ROW_START", { rowNum, mainAlreadyOk });

    if (CONFIG.FEATURES.DRY_RUN) {
      processed++;
      continue;
    }

    // Ensure ID exists
    let id = String(row[idIdx] || "").trim();
    if (!id) {
      id = allocateNextId_(dest, idIdx);
      dest.getRange(rowNum, idIdx + 1).setValue(id);
      SpreadsheetApp.flush();
      log_(runId, "INFO", "ID_ASSIGNED", { rowNum, id });
    }

    // MF enrichment (skip if already done unless missing data)
    if (CONFIG.FEATURES.MF_ENABLED) {
      const hasMfOk = currentSync.indexOf("MF_OK") >= 0;

      const nameApiIdx = mapping.dstIndex["name_api"];
      const statusVatIdx = mapping.dstIndex["statusVat"];
      const regonIdx = mapping.dstIndex["regon"];
      const krsIdx = mapping.dstIndex["krs"];

      const hasMfData =
        (nameApiIdx != null && String(row[nameApiIdx] || "").trim()) ||
        (statusVatIdx != null && String(row[statusVatIdx] || "").trim()) ||
        (regonIdx != null && String(row[regonIdx] || "").trim()) ||
        (krsIdx != null && String(row[krsIdx] || "").trim());

      const shouldSkipMf =
        (CONFIG.FEATURES.MF_SKIP_IF_MF_OK && hasMfOk) ||
        (CONFIG.FEATURES.MF_SKIP_IF_DATA_PRESENT && hasMfData);

      if (shouldSkipMf) {
        log_(runId, "INFO", "MF_SKIP", { rowNum, hasMfOk, hasMfData });
      } else {
        const subDate = safeDateForMf_(row[mapping.destKey.submittedIdx]);
        callMfAndWrite_(runId, dest, mapping, rowNum, nipRaw, subDate);
        if (syncIdx != null) appendSyncMarker_(dest, rowNum, syncIdx, `MF_OK ${formatNow_()}`);
      }
    }

    // BANK ACCOUNTS (child table / sheet)
    // Must run AFTER MF enrichment (so "accountNumbers" is already written to DEST if available)
    // and BEFORE AppSheet push (so child rows exist when bots/actions depend on them).
    // Safe-guard: only run if the helper exists in the project.
    try {
      if (typeof syncBankAccountsFromMainRow_ === 'function') {
        syncBankAccountsFromMainRow_(runId, dest, mapping, rowNum, id);
      }
    } catch (e) {
      log_(runId, "WARN", "BANK_ACCOUNTS_HOOK_FAIL", { rowNum, err: String(e).slice(0, 900) });
    }

    // Build main payload (after MF)

    // Decide action early (needed for payload normalization e.g. optional Url on Add vs Edit)
    const needRefs =
      (contactPersonIdx != null && !String(dest.getRange(rowNum, contactPersonIdx + 1).getValue() || "").trim()) ||
      (managerPersonIdx != null && !String(dest.getRange(rowNum, managerPersonIdx + 1).getValue() || "").trim()) ||
      (beneficialPersonIdx != null && !String(dest.getRange(rowNum, beneficialPersonIdx + 1).getValue() || "").trim());

    // Sending rules:
// - ADD: only when row is truly new: Status is blank/empty OR already equals STATUS_TO_SEND (Init),
//        and row has NOT been successfully sent to AppSheet yet.
// - EDIT: only when already sent (APPSHEET_OK) AND we still need to backfill refs.
    const statusValRaw = (statusIdx != null) ? String(dest.getRange(rowNum, statusIdx + 1).getValue() || "") : "";
    const statusVal = statusValRaw.trim();
    const isStatusEmpty = (statusVal === "");
    const isStatusInit = (statusVal !== "" && statusVal === String(CONFIG.STATUS_TO_SEND));

    const shouldAdd = (!mainAlreadyOk) && (isStatusEmpty || isStatusInit);
    const shouldEdit = (CONFIG.SKIP_IF_DEST_HAS_APPSHEET_OK && mainAlreadyOk && needRefs);

    // Hint for payload builder (some columns require different handling for Add vs Edit)
    const actionHint = shouldAdd ? "Add" : "Edit";
    SpreadsheetApp.flush();
    let payloadMain = buildAppSheetPayloadFromDest_(dest, rowNum, actionHint);
    payloadMain = filterPayloadForAppSheet_(payloadMain, APPSHEET_MAIN_ALLOWED_COLS);

    // 1) PEOPLE: ensure 3 roles and write PersonIDs back to MAIN sheet
    if (CONFIG.FEATURES.APPSHEET_ENABLED && CONFIG.FEATURES.PEOPLE_LIST_ENABLED) {
      const refs = ensurePeopleRefsForRow_(runId, dest, mapping, rowNum, payloadMain);
      // inject refs into payload
      if (refs.contactId) payloadMain[CONFIG.MAIN_REF_COLS.CONTACT] = refs.contactId;
      if (refs.managerId) payloadMain[CONFIG.MAIN_REF_COLS.MANAGER] = refs.managerId;
      if (refs.beneficialId) payloadMain[CONFIG.MAIN_REF_COLS.BENEFICIAL] = refs.beneficialId;
    }

    // 2) MAIN push (Add for new, Edit for already OK but missing refs)
    if (CONFIG.FEATURES.APPSHEET_ENABLED) {

      // IMPORTANT:
      // - We only ever set Status=Init just-in-time for the FIRST push (Add).
      // - We NEVER overwrite Status on subsequent runs (especially when doing Edit to backfill refs),
      //   because AppSheet automations/bots may have already advanced the workflow status.
      let actionToSend = null;
      if (shouldAdd) actionToSend = CONFIG.APPSHEET_ACTION_ADD;
      else if (shouldEdit) actionToSend = CONFIG.APPSHEET_ACTION_EDIT;

      if (actionToSend === CONFIG.APPSHEET_ACTION_ADD && CONFIG.WRITE_STATUS_JUST_IN_TIME) {
        // Do not keep re-writing Status in the sheet forever; set only if empty (optional)
        if (statusIdx != null) {
          // For a brand-new record we set Init right before the FIRST push.
          // (Subsequent runs will not touch Status at all.)
          if (String(dest.getRange(rowNum, statusIdx + 1).getValue() || "").trim() === "") {
            dest.getRange(rowNum, statusIdx + 1).setValue(CONFIG.STATUS_TO_SEND);
          }
        }
        // Ensure payload carries Init
        payloadMain["Status"] = CONFIG.STATUS_TO_SEND;
      } else if (actionToSend === CONFIG.APPSHEET_ACTION_EDIT) {
        // Never send Status during EDIT (protect against overwriting AppSheet-side status)
        if (payloadMain && Object.prototype.hasOwnProperty.call(payloadMain, "Status")) delete payloadMain["Status"];
      }

      SpreadsheetApp.flush();

      // rebuild payload after potential Status adjustments (JIT)
      const actionHint2 = (actionToSend === CONFIG.APPSHEET_ACTION_ADD) ? "Add" : "Edit";
      payloadMain = buildAppSheetPayloadFromDest_(dest, rowNum, actionHint2);
      payloadMain = filterPayloadForAppSheet_(payloadMain, APPSHEET_MAIN_ALLOWED_COLS);

      // If we're doing EDIT, strip Status again after filter (allowed list contains it)
      if (actionToSend === CONFIG.APPSHEET_ACTION_EDIT && payloadMain && Object.prototype.hasOwnProperty.call(payloadMain, "Status")) {
        delete payloadMain["Status"];
      }

      // Safety guard:
      // if payload unexpectedly lost key fields, skip API call instead of sending
      // an invalid Edit/Add request (e.g. key='' -> AppSheet 404).
      const payloadId = String(payloadMain && payloadMain.ID ? payloadMain.ID : "").trim();
      const payloadNip = String(
        (payloadMain && (payloadMain.NIP_Control || payloadMain.nip)) ? (payloadMain.NIP_Control || payloadMain.nip) : ""
      ).trim();
      const hasCorePayloadData = hasCoreMainPayloadData_(payloadMain);

      if (actionToSend && (!payloadId || !payloadNip || !hasCorePayloadData)) {
        if (syncIdx != null) {
          appendSyncMarker_(dest, rowNum, syncIdx, `APPSHEET_SKIP_INVALID_PAYLOAD ${formatNow_()}`);
        }
        log_(runId, "WARN", "APPSHEET_SKIP_INVALID_PAYLOAD", {
          rowNum,
          actionToSend,
          payloadId,
          payloadNip,
          rowId: id,
          rowNip: nipRaw
        });
        log_(runId, "INFO", "ROW_END", { rowNum });
        processed++;
        continue;
      }

      try {
        if (shouldAdd) {
          callAppSheet_(runId, CONFIG.APPSHEET_TABLE_MAIN, payloadMain, CONFIG.APPSHEET_ACTION_ADD, rowNum);
          if (syncIdx != null) appendSyncMarker_(dest, rowNum, syncIdx, `APPSHEET_OK ${formatNow_()}`);
          log_(runId, "INFO", "APPSHEET_OK_ADD", { rowNum });
          mainOk = true;
        } else if (shouldEdit) {
          callAppSheet_(runId, CONFIG.APPSHEET_TABLE_MAIN, payloadMain, CONFIG.APPSHEET_ACTION_EDIT, rowNum);
          if (syncIdx != null) appendSyncMarker_(dest, rowNum, syncIdx, `APPSHEET_EDIT_OK ${formatNow_()}`);
          log_(runId, "INFO", "APPSHEET_OK_EDIT", { rowNum });
          mainOk = true;
        } else {
          log_(runId, "INFO", "ROW_SKIP_MAIN_ALREADY_OK", { rowNum });
        }
      } catch (e) {
        if (syncIdx != null) appendSyncMarker_(dest, rowNum, syncIdx, `APPSHEET_FAIL ${formatNow_()} ${String(e).slice(0, 180)}`);
        log_(runId, "WARN", "APPSHEET_FAIL", { rowNum, err: String(e).slice(0, 900) });
      }
    }

    // Finalny znacznik w SOURCE (po pełnym sukcesie w AppSheet),
    // dzięki temu możesz wyczyścić DEST (nawet z nagłówkami) bez ryzyka duplikacji w AppSheet.
    if (mainOk && source && sourceMarkIdx >= 0) {
      const srcRow = rowMapByDest[String(rowNum)];
      if (srcRow != null) {
        const idStr = String(payloadMain && payloadMain.ID ? payloadMain.ID : "");
        const doneMark = `${CONFIG.SOURCE_MARK_PREFIX_DONE} ${formatNow_()}${idStr ? " | ID=" + idStr : ""} | APPSHEET_OK`;
        source.getRange(Number(srcRow), Number(sourceMarkIdx) + 1).setValue(doneMark);
        log_(runId, "INFO", "SOURCE_MARK_DONE", { rowNum, srcRow, id: idStr || "" });
      }
    }

    log_(runId, "INFO", "ROW_END", { rowNum });
    processed++;
  }

  return processed;
}

/**
 * Creates People_List rows for 3 FullName variants (if present)
 * and writes PersonIDs into main sheet columns:
 * ContactPersonID / ManagerPersonID / BeneficialOwnerPersonID
 */
function ensurePeopleRefsForRow_(runId, dest, mapping, rowNum, payloadMain) {
  const onboardingId = String(payloadMain["ID"] || "").trim();
  if (!onboardingId) return { contactId: "", managerId: "", beneficialId: "" };

  const idxContact = mapping.destKey.contactPersonIdx;
  const idxManager = mapping.destKey.managerPersonIdx;
  const idxBeneficial = mapping.destKey.beneficialPersonIdx;

  const fullContact = String(payloadMain["imię i nazwisko osoby kontaktowej"] || "").trim();
  const fullManager = String(payloadMain["imię i nazwisko kierownika"] || "").trim();
  const fullBeneficial = String(payloadMain["imię i nazwisko beneficjenta"] || "").trim();

  let contactId = idxContact != null ? String(dest.getRange(rowNum, idxContact + 1).getValue() || "").trim() : "";
  let managerId = idxManager != null ? String(dest.getRange(rowNum, idxManager + 1).getValue() || "").trim() : "";
  let beneficialId = idxBeneficial != null ? String(dest.getRange(rowNum, idxBeneficial + 1).getValue() || "").trim() : "";

  // CONTACT
  if (fullContact && idxContact != null && !contactId) {
    contactId = Utilities.getUuid();
    dest.getRange(rowNum, idxContact + 1).setValue(contactId);
    SpreadsheetApp.flush();
    pushPersonToPeopleList_(runId, contactId, fullContact, "Contact", "imię i nazwisko osoby kontaktowej", onboardingId, rowNum);
  }

  // MANAGER
  if (fullManager && idxManager != null && !managerId) {
    managerId = Utilities.getUuid();
    dest.getRange(rowNum, idxManager + 1).setValue(managerId);
    SpreadsheetApp.flush();
    pushPersonToPeopleList_(runId, managerId, fullManager, "Manager", "imię i nazwisko kierownika", onboardingId, rowNum);
  }

  // BENEFICIAL OWNER (natural person)
  if (fullBeneficial && idxBeneficial != null && !beneficialId) {
    beneficialId = Utilities.getUuid();
    dest.getRange(rowNum, idxBeneficial + 1).setValue(beneficialId);
    SpreadsheetApp.flush();
    pushPersonToPeopleList_(runId, beneficialId, fullBeneficial, "BeneficialOwner", "imię i nazwisko beneficjenta", onboardingId, rowNum);
  }

  // marker
  if (mapping.destKey.syncStatusIdx != null) {
    appendSyncMarker_(dest, rowNum, mapping.destKey.syncStatusIdx, `PEOPLE_REFS_OK ${formatNow_()}`);
  }

  return { contactId, managerId, beneficialId };
}

function pushPersonToPeopleList_(runId, personId, fullName, role, sourceFullNameCol, onboardingId, rowNum) {
  const peopleRow = {};
  peopleRow[CONFIG.PEOPLE.COL_PERSON_ID] = personId;
  peopleRow[CONFIG.PEOPLE.COL_FULL_NAME] = fullName;
  peopleRow[CONFIG.PEOPLE.COL_FULL_NAME_KEY] = fullName.toLowerCase().trim();
  peopleRow[CONFIG.PEOPLE.COL_ROLE] = role;
  peopleRow[CONFIG.PEOPLE.COL_SOURCE_FULLNAME_COL] = sourceFullNameCol;
  peopleRow[CONFIG.PEOPLE.COL_ONBOARDING_ID] = onboardingId;

  callAppSheet_(runId, CONFIG.APPSHEET_TABLE_PEOPLE, peopleRow, CONFIG.APPSHEET_ACTION_ADD, rowNum);
  log_(runId, "INFO", "PEOPLE_ADD_OK", { rowNum, role, personId });
}

/** =========================
 *  sync_status marker helpers
 *  ========================= */
function appendSyncMarker_(sheet, rowNum, syncIdx, marker) {
  try {
    const cell = sheet.getRange(rowNum, syncIdx + 1);
    const prev = String(cell.getValue() || "");
    const next = String(marker || "");

    // We need to remember that MAIN was already pushed to AppSheet (APPSHEET_OK),
    // otherwise next runs will keep sending Add again.
    const hasMainOk = prev.indexOf(CONFIG.MARKERS.MAIN_OK) >= 0 || next.indexOf(CONFIG.MARKERS.MAIN_OK) >= 0;

    // Keep sync_status compact:
    // - Always keep the MAIN_OK token if it ever occurred.
    // - Also keep the latest marker (with timestamp/details).
    // Example: "APPSHEET_OK | PEOPLE_REFS_OK 2026-02-24 16:12:09"
    let value = next;
    if (hasMainOk && next.indexOf(CONFIG.MARKERS.MAIN_OK) < 0) {
      value = `${CONFIG.MARKERS.MAIN_OK} | ${next}`;
    }

    // Safety length cap (Google Sheets limit is large, keep plenty of margin)
    cell.setValue(String(value).slice(0, 49000));
  } catch (e) {
    // ignore
  }
}

function hasCoreMainPayloadData_(payload) {
  if (!payload) return false;
  const keys = ["ID", "NIP_Control", "nip", "submitted on", "nazwa firmy"];
  for (let i = 0; i < keys.length; i++) {
    const v = payload[keys[i]];
    if (v === null || v === undefined) continue;
    if (typeof v === "string") {
      if (v.trim() !== "") return true;
      continue;
    }
    return true;
  }
  return false;
}
