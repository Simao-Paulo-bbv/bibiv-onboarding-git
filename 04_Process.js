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
  const data = dest.getRange(startRow, 1, rowsToRead, mapping.destHeaders.length).getValues();

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
    const statusValRaw = (statusIdx != null) ? String(row[statusIdx] || "") : "";
    const statusVal = statusValRaw.trim();
    const isStatusInit = (statusVal !== "" && statusVal === String(CONFIG.STATUS_TO_SEND));
    const hasExternalManagedStatus = (statusVal !== "" && !isStatusInit);
    const mainAlreadyOk = currentSync.indexOf(CONFIG.MARKERS.MAIN_OK) >= 0 || hasExternalManagedStatus;

    // Czy rekord w AppSheet MAIN jest już poprawnie zapisany.
    // Uwaga: mainOk MUSI być zawsze zainicjalizowane, bo używamy go później do markowania SOURCE.
    let mainOk = mainAlreadyOk;

    log_(runId, "INFO", "ROW_START", { rowNum, mainAlreadyOk, hasExternalManagedStatus });

    // Hard safety: if Status is already managed by AppSheet (not empty and not Init),
    // this script must not touch that record anymore.
    if (hasExternalManagedStatus) {
      log_(runId, "INFO", "ROW_SKIP_EXTERNAL_STATUS_MANAGED", { rowNum, status: statusVal });
      log_(runId, "INFO", "ROW_END", { rowNum });
      processed++;
      continue;
    }

    if (CONFIG.FEATURES.DRY_RUN) {
      processed++;
      continue;
    }

    // Ensure ID exists
    let id = String(row[idIdx] || "").trim();
    let idAssignedNow = false;
    if (!id) {
      id = allocateNextId_(dest, idIdx);
      dest.getRange(rowNum, idIdx + 1).setValue(id);
      idAssignedNow = true;
      log_(runId, "INFO", "ID_ASSIGNED", { rowNum, id });
    }

    const canRunGovEnrichment = canRunGovEnrichmentForStatus_(statusVal, idAssignedNow);

    // MF enrichment (skip if already done unless missing data)
    let mfCallResult = { ok: false, httpCode: 0, rateLimited: false, reason: "NOT_CALLED" };
    if (CONFIG.FEATURES.MF_ENABLED) {
      if (!canRunGovEnrichment) {
        log_(runId, "INFO", "MF_SKIP_STATUS_NOT_INIT", { rowNum, status: statusVal });
      } else if (mainAlreadyOk) {
        log_(runId, "INFO", "MF_SKIP_MAIN_ALREADY_OK", { rowNum });
      } else {
      const hasMfOk = currentSync.indexOf("MF_OK") >= 0;
      const hasMfRateLimit = currentSync.indexOf("MF_RATE_LIMIT") >= 0;
      const hasMfNoSubject = currentSync.indexOf("MF_NO_SUBJECT") >= 0;
      const hasMfNotVatMarker = currentSync.indexOf("MF_NOT_VAT") >= 0;
      const hasMfRegonBlock = currentSync.indexOf("MF_REGON_BLOCK") >= 0;
      const hasMfVatBlock = currentSync.indexOf("MF_VAT_BLOCK") >= 0;

      const nameApiIdx = mapping.dstIndex["name_api"];
      const statusVatIdx = mapping.dstIndex["statusVat"];
      const regonIdx = mapping.dstIndex["regon"];
      const krsIdx = mapping.dstIndex["krs"];
      const workingAddressIdx = mapping.dstIndex["workingAddress"];
      const residenceAddressIdx = mapping.dstIndex["residenceAddress"];

      const nameApiVal = nameApiIdx != null ? String(row[nameApiIdx] || "").trim() : "";
      const statusVatVal = statusVatIdx != null ? String(row[statusVatIdx] || "").trim() : "";
      const regonVal = regonIdx != null ? String(row[regonIdx] || "").trim() : "";
      const krsVal = krsIdx != null ? String(row[krsIdx] || "").trim() : "";
      const workingAddressVal = workingAddressIdx != null ? String(row[workingAddressIdx] || "").trim() : "";
      const residenceAddressVal = residenceAddressIdx != null ? String(row[residenceAddressIdx] || "").trim() : "";
      const isNotVatStatus = statusVatVal.toLowerCase() === "not vat";

      const hasMfDataAny = !!(nameApiVal || statusVatVal || regonVal || krsVal || workingAddressVal || residenceAddressVal);
      const hasMfDataComplete =
        !!nameApiVal &&
        !!statusVatVal &&
        !!regonVal &&
        !!(workingAddressVal || residenceAddressVal) &&
        (!(CONFIG && CONFIG.REQUIRE_KRS_FOR_ADD === true) || !!krsVal);
      const hasConfirmedAndCompleteMf = hasMfOk && hasMfDataComplete;
      const hasMfNoSubjectButComplete = hasMfNoSubject && hasMfDataComplete;
      // For NOT VAT records we intentionally allow sparse enrichment:
      // statusVat=Not VAT is enough to stop repeated MF retries.
      const hasConfirmedNotVat = hasMfNotVatMarker || (hasMfOk && isNotVatStatus);
      const shouldSkipMf =
        (CONFIG.FEATURES.MF_SKIP_IF_MF_OK && hasConfirmedAndCompleteMf) ||
        (CONFIG.FEATURES.MF_SKIP_IF_DATA_PRESENT && hasMfDataComplete && hasMfOk) ||
        hasConfirmedNotVat ||
        hasMfRegonBlock ||
        hasMfVatBlock ||
        hasMfRateLimit;

      if (shouldSkipMf) {
        log_(runId, "INFO", "MF_SKIP", {
          rowNum,
          hasMfOk,
          hasMfData: hasMfDataAny,
          hasMfDataComplete,
          hasConfirmedAndCompleteMf,
          hasConfirmedNotVat,
          hasMfRegonBlock,
          hasMfVatBlock,
          hasMfRateLimit,
          hasMfNoSubject,
          hasMfNoSubjectButComplete
        });
      } else {
        const subDate = safeDateForMf_(row[mapping.destKey.submittedIdx]);
        mfCallResult = callMfAndWrite_(runId, dest, mapping, rowNum, nipRaw, subDate) || mfCallResult;
        if (syncIdx != null) {
          if (mfCallResult.ok) {
            const okMarker = (mfCallResult.reason === "NOT_VAT")
              ? `MF_OK MF_NOT_VAT ${formatNow_()}`
              : `MF_OK ${formatNow_()}`;
            appendSyncMarker_(dest, rowNum, syncIdx, okMarker);
          } else if (mfCallResult.rateLimited) {
            appendSyncMarker_(dest, rowNum, syncIdx, `MF_RATE_LIMIT ${formatNow_()}`);
          } else if (mfCallResult.reason === "NO_SUBJECT") {
            appendSyncMarker_(dest, rowNum, syncIdx, `MF_NO_SUBJECT ${formatNow_()}`);
          } else if (String(mfCallResult.reason || "").indexOf("REGON_") === 0) {
            appendSyncMarker_(dest, rowNum, syncIdx, `MF_REGON_BLOCK ${formatNow_()} ${String(mfCallResult.reason || "").slice(0, 80)}`);
          } else if (String(mfCallResult.reason || "").indexOf("VAT_") === 0) {
            appendSyncMarker_(dest, rowNum, syncIdx, `MF_VAT_BLOCK ${formatNow_()} ${String(mfCallResult.reason || "").slice(0, 80)}`);
          }
        }
      }
      }
    }

    // Hard stop for REGON-blocked rows:
    // do not call further APIs and do not send to AppSheet until row is corrected.
    const hasMfRegonBlockNow = currentSync.indexOf("MF_REGON_BLOCK") >= 0 || String(mfCallResult.reason || "").indexOf("REGON_") === 0;
    if (hasMfRegonBlockNow) {
      markRowAsNeedVerification_(runId, dest, rowNum, statusIdx, row, "REGON_BLOCK");
      log_(runId, "WARN", "ROW_BLOCKED_REGON", {
        rowNum,
        nip: nipRaw,
        reason: String(mfCallResult.reason || "MF_REGON_BLOCK")
      });
      log_(runId, "INFO", "ROW_END", { rowNum });
      processed++;
      continue;
    }

    const hasMfRateLimitNow = currentSync.indexOf("MF_RATE_LIMIT") >= 0 || !!mfCallResult.rateLimited;
    if (hasMfRateLimitNow) {
      log_(runId, "WARN", "ROW_BLOCKED_RATE_LIMIT", {
        rowNum,
        nip: nipRaw,
        reason: String(mfCallResult.reason || "MF_RATE_LIMIT")
      });
      log_(runId, "INFO", "ROW_END", { rowNum });
      processed++;
      continue;
    }

    // Hard stop for VAT technical/unrecognized failures:
    // subject:null is handled as NOT VAT, but request/HTTP/shape errors must not reach IBAN/AppSheet.
    const hasMfVatBlockNow = currentSync.indexOf("MF_VAT_BLOCK") >= 0 || String(mfCallResult.reason || "").indexOf("VAT_") === 0;
    if (hasMfVatBlockNow) {
      log_(runId, "WARN", "ROW_BLOCKED_VAT", {
        rowNum,
        nip: nipRaw,
        reason: String(mfCallResult.reason || "MF_VAT_BLOCK")
      });
      log_(runId, "INFO", "ROW_END", { rowNum });
      processed++;
      continue;
    }

    // BANK ACCOUNTS (child table / sheet)
    // Heavy operation; skip for rows already finalized in main flow.
    // Must run AFTER MF enrichment (so "accountNumbers" is already written to DEST if available)
    // and BEFORE AppSheet push (so child rows exist when bots/actions depend on them).
    if (!mainAlreadyOk) {
      try {
        if (typeof syncBankAccountsFromMainRow_ === 'function') {
          syncBankAccountsFromMainRow_(runId, dest, mapping, rowNum, id);
        }
      } catch (e) {
        log_(runId, "WARN", "BANK_ACCOUNTS_HOOK_FAIL", { rowNum, err: String(e).slice(0, 900) });
      }
    } else {
      log_(runId, "INFO", "BANK_ACCOUNTS_SKIP_MAIN_ALREADY_OK", { rowNum, onboardingId: id });
    }

    // Build main payload (after MF)

    // Decide action early (needed for payload normalization e.g. optional Url on Add vs Edit)
    const contactFullNameIdx = mapping.dstIndex["imię i nazwisko osoby kontaktowej"];
    const managerFullNameIdx = mapping.dstIndex["imię i nazwisko kierownika"];
    const beneficialFullNameIdx = mapping.dstIndex["imię i nazwisko beneficjenta"];

    const hasContactFullName = contactFullNameIdx != null && String(row[contactFullNameIdx] || "").trim();
    const hasManagerFullName = managerFullNameIdx != null && String(row[managerFullNameIdx] || "").trim();
    const hasBeneficialFullName = beneficialFullNameIdx != null && String(row[beneficialFullNameIdx] || "").trim();

    const needRefs =
      (contactPersonIdx != null && hasContactFullName && !String(row[contactPersonIdx] || "").trim()) ||
      (managerPersonIdx != null && hasManagerFullName && !String(row[managerPersonIdx] || "").trim()) ||
      (beneficialPersonIdx != null && hasBeneficialFullName && !String(row[beneficialPersonIdx] || "").trim());

    if (mainAlreadyOk && !needRefs) {
      log_(runId, "INFO", "ROW_SKIP_ALREADY_COMPLETE", { rowNum });
      log_(runId, "INFO", "ROW_END", { rowNum });
      processed++;
      continue;
    }

    // Sending rules:
// - ADD: only when row is truly new: Status is blank/empty OR already equals STATUS_TO_SEND (Init),
//        and row has NOT been successfully sent to AppSheet yet.
// - EDIT: only when already sent (APPSHEET_OK) AND we still need to backfill refs.
    const isStatusEmpty = (statusVal === "");

    const shouldAdd = (!mainAlreadyOk) && (isStatusEmpty || isStatusInit);
    const shouldEdit = (CONFIG.SKIP_IF_DEST_HAS_APPSHEET_OK && mainAlreadyOk && needRefs);

    // Hint for payload builder (some columns require different handling for Add vs Edit)
    const actionHint = shouldAdd ? "Add" : "Edit";
    let payloadMain = buildAppSheetPayloadFromDest_(dest, rowNum, actionHint);
    payloadMain = filterPayloadForAppSheet_(payloadMain, APPSHEET_MAIN_ALLOWED_COLS);

    // Hard gate before ANY AppSheet call:
    // without confirmed MF success and complete MF fields we do not send to People_List nor MAIN.
    if (CONFIG.FEATURES.APPSHEET_ENABLED && CONFIG.FEATURES.MF_ENABLED && shouldAdd) {
      const hasMfOkMarker = currentSync.indexOf("MF_OK") >= 0 || !!mfCallResult.ok;
      const mfReadinessPre = evaluateMfReadinessForAdd_(payloadMain);
      if (!hasMfOkMarker || !mfReadinessPre.ready) {
        if (syncIdx != null) {
          appendSyncMarker_(dest, rowNum, syncIdx, `APPSHEET_WAITING_MF_DATA ${formatNow_()}`);
        }
        const missingPre = mfReadinessPre.missing.slice();
        if (!hasMfOkMarker) missingPre.push("mf_ok_marker");
        log_(runId, "WARN", "APPSHEET_WAITING_MF_DATA", {
          rowNum,
          actionToSend: CONFIG.APPSHEET_ACTION_ADD,
          payloadId: String(payloadMain && payloadMain.ID ? payloadMain.ID : "").trim(),
          payloadNip: String((payloadMain && (payloadMain.NIP_Control || payloadMain.nip)) ? (payloadMain.NIP_Control || payloadMain.nip) : "").trim(),
          missing: missingPre.join(","),
          mfCallReason: String(mfCallResult && mfCallResult.reason ? mfCallResult.reason : "NOT_CALLED")
        });
        log_(runId, "INFO", "ROW_END", { rowNum });
        processed++;
        continue;
      }
    }

    // 1) PEOPLE: ensure 3 roles and write PersonIDs back to MAIN sheet
    if (CONFIG.FEATURES.APPSHEET_ENABLED && CONFIG.FEATURES.PEOPLE_LIST_ENABLED) {
      try {
        const refs = ensurePeopleRefsForRow_(runId, dest, mapping, rowNum, payloadMain);
        // inject refs into payload
        if (refs.contactId) payloadMain[CONFIG.MAIN_REF_COLS.CONTACT] = refs.contactId;
        if (refs.managerId) payloadMain[CONFIG.MAIN_REF_COLS.MANAGER] = refs.managerId;
        if (refs.beneficialId) payloadMain[CONFIG.MAIN_REF_COLS.BENEFICIAL] = refs.beneficialId;
      } catch (e) {
        if (isAppSheetSchemaMismatchError_(e)) {
          if (syncIdx != null) appendSyncMarker_(dest, rowNum, syncIdx, `APPSHEET_SCHEMA_MISMATCH ${formatNow_()}`);
          log_(runId, "WARN", "APPSHEET_SCHEMA_MISMATCH", { rowNum, err: String(e).slice(0, 900) });
          log_(runId, "INFO", "ROW_END", { rowNum });
          processed++;
          continue;
        }
        throw e;
      }
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

      if (actionToSend === CONFIG.APPSHEET_ACTION_ADD) {
        // Live Status guard (important):
        // row snapshot can be stale; never overwrite any externally managed status.
        const liveStatus = statusIdx != null ? String(dest.getRange(rowNum, statusIdx + 1).getValue() || "").trim() : "";
        const liveIsInit = (liveStatus !== "" && liveStatus === String(CONFIG.STATUS_TO_SEND));
        const liveIsEmpty = (liveStatus === "");
        const liveExternalManaged = (liveStatus !== "" && !liveIsInit);
        if (liveExternalManaged) {
          log_(runId, "INFO", "ROW_SKIP_EXTERNAL_STATUS_MANAGED_LIVE", { rowNum, status: liveStatus });
          log_(runId, "INFO", "ROW_END", { rowNum });
          processed++;
          continue;
        }

        if (CONFIG.WRITE_STATUS_JUST_IN_TIME) {
          // Never persist Init in local sheet before a successful Add.
          // Init in local sheet caused repeated Add attempts and status overwrite loops.
          // We send Init only in the first Add payload while the live local status is still
          // blank/Init; AppSheet owns all subsequent status transitions.
          if (liveIsEmpty || liveIsInit) {
            const verificationStatus = resolveInitialMainStatus_(payloadMain);
            payloadMain["Status"] = verificationStatus.status;
            if (verificationStatus.needsVerification) {
              log_(runId, "WARN", "APPSHEET_ADD_NEEDS_VERIFICATION", {
                rowNum: rowNum,
                status: verificationStatus.status,
                reasons: verificationStatus.reasons
              });
            }
          } else if (payloadMain && Object.prototype.hasOwnProperty.call(payloadMain, "Status")) {
            delete payloadMain["Status"];
          }
        } else if (payloadMain && Object.prototype.hasOwnProperty.call(payloadMain, "Status")) {
          delete payloadMain["Status"];
        }
        log_(runId, "INFO", "APPSHEET_ADD_STATUS_PREPARED", {
          rowNum,
          idAssignedNow,
          liveStatus: liveStatus,
          statusInPayload: String(payloadMain && payloadMain["Status"] ? payloadMain["Status"] : "")
        });
      } else if (actionToSend === CONFIG.APPSHEET_ACTION_EDIT) {
        // Never send Status during EDIT (protect against overwriting AppSheet-side status)
        if (payloadMain && Object.prototype.hasOwnProperty.call(payloadMain, "Status")) delete payloadMain["Status"];
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

      const mfReadiness = evaluateMfReadinessForAdd_(payloadMain);
      const hasMfOkMarkerAfter = currentSync.indexOf("MF_OK") >= 0 || !!mfCallResult.ok;
      if (actionToSend === CONFIG.APPSHEET_ACTION_ADD && (!mfReadiness.ready || !hasMfOkMarkerAfter)) {
        if (syncIdx != null) {
          appendSyncMarker_(dest, rowNum, syncIdx, `APPSHEET_WAITING_MF_DATA ${formatNow_()}`);
        }
        const missing = mfReadiness.missing.slice();
        if (!hasMfOkMarkerAfter) missing.push("mf_ok_marker");
        log_(runId, "WARN", "APPSHEET_WAITING_MF_DATA", {
          rowNum,
          actionToSend,
          payloadId,
          payloadNip,
          missing: missing.join(","),
          rowId: id,
          rowNip: nipRaw,
          mfCallReason: String(mfCallResult && mfCallResult.reason ? mfCallResult.reason : "NOT_CALLED")
        });
        log_(runId, "INFO", "ROW_END", { rowNum });
        processed++;
        continue;
      }

      const payloadAccountNumbers = String(payloadMain && payloadMain.accountNumbers ? payloadMain.accountNumbers : "").trim();
      const isNotVatStatus = String(payloadMain && payloadMain.statusVat ? payloadMain.statusVat : "").trim().toLowerCase() === "not vat";
      if (actionToSend === CONFIG.APPSHEET_ACTION_ADD && !payloadAccountNumbers && !isNotVatStatus) {
        if (syncIdx != null) {
          appendSyncMarker_(dest, rowNum, syncIdx, `APPSHEET_SKIP_MISSING_ACCOUNTNUMBERS ${formatNow_()}`);
        }
        log_(runId, "WARN", "APPSHEET_SKIP_MISSING_ACCOUNTNUMBERS", {
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
      if (actionToSend === CONFIG.APPSHEET_ACTION_ADD && !payloadAccountNumbers && isNotVatStatus) {
        log_(runId, "INFO", "APPSHEET_ALLOW_MISSING_ACCOUNTNUMBERS_NOT_VAT", {
          rowNum,
          payloadId,
          payloadNip,
          rowId: id,
          rowNip: nipRaw
        });
      }

      try {
        if (shouldAdd) {
          const addRes = callAppSheet_(runId, CONFIG.APPSHEET_TABLE_MAIN, payloadMain, CONFIG.APPSHEET_ACTION_ADD, rowNum);
          if (statusIdx != null) {
            const appSheetStatus = extractAppSheetStatusFromResponse_(addRes && addRes.parsed);
            if (appSheetStatus) {
              dest.getRange(rowNum, statusIdx + 1).setValue(appSheetStatus);
              log_(runId, "INFO", "STATUS_SYNCED_FROM_APPSHEET", { rowNum, status: appSheetStatus });
            } else {
              log_(runId, "WARN", "APPSHEET_ADD_STATUS_NOT_RETURNED", {
                rowNum,
                id: payloadId,
                statusSentInAddPayload: String(payloadMain && payloadMain["Status"] ? payloadMain["Status"] : "")
              });
            }
          }
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
  let refsChanged = false;

  // CONTACT
  if (fullContact && idxContact != null && !contactId) {
    contactId = buildDeterministicPersonId_(onboardingId, "Contact", fullContact);
    dest.getRange(rowNum, idxContact + 1).setValue(contactId);
    refsChanged = true;
    pushPersonToPeopleList_(runId, contactId, fullContact, "Contact", "imię i nazwisko osoby kontaktowej", onboardingId, rowNum);
  }

  // MANAGER
  if (fullManager && idxManager != null && !managerId) {
    managerId = buildDeterministicPersonId_(onboardingId, "Manager", fullManager);
    dest.getRange(rowNum, idxManager + 1).setValue(managerId);
    refsChanged = true;
    pushPersonToPeopleList_(runId, managerId, fullManager, "Manager", "imię i nazwisko kierownika", onboardingId, rowNum);
  }

  // BENEFICIAL OWNER (natural person)
  if (fullBeneficial && idxBeneficial != null && !beneficialId) {
    beneficialId = buildDeterministicPersonId_(onboardingId, "BeneficialOwner", fullBeneficial);
    dest.getRange(rowNum, idxBeneficial + 1).setValue(beneficialId);
    refsChanged = true;
    pushPersonToPeopleList_(runId, beneficialId, fullBeneficial, "BeneficialOwner", "imię i nazwisko beneficjenta", onboardingId, rowNum);
  }

  // marker
  if (refsChanged && mapping.destKey.syncStatusIdx != null) {
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

  try {
    callAppSheet_(runId, CONFIG.APPSHEET_TABLE_PEOPLE, peopleRow, CONFIG.APPSHEET_ACTION_ADD, rowNum);
    log_(runId, "INFO", "PEOPLE_ADD_OK", { rowNum, role, personId });
  } catch (e) {
    if (isDuplicatePeopleRowError_(e)) {
      log_(runId, "INFO", "PEOPLE_ADD_SKIP_EXISTS", { rowNum, role, personId });
      return;
    }
    throw e;
  }
}

function buildDeterministicPersonId_(onboardingId, role, fullName) {
  const normalizedName = String(fullName || "").trim().toLowerCase();
  const raw = [String(onboardingId || "").trim(), String(role || "").trim(), normalizedName].join("|");
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, raw, Utilities.Charset.UTF_8);
  const b64 = Utilities.base64EncodeWebSafe(digest);
  return "P_" + b64.slice(0, 22);
}

function isDuplicatePeopleRowError_(err) {
  const s = String(err || "").toLowerCase();
  if (!s) return false;
  return (
    s.indexOf("duplicate") >= 0 ||
    s.indexOf("already exists") >= 0 ||
    s.indexOf("already in use") >= 0 ||
    s.indexOf("already has a row with key") >= 0 ||
    s.indexOf("cannot add duplicate") >= 0
  );
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

function evaluateMfReadinessForAdd_(payload) {
  const missing = [];
  if (!payload) return { ready: false, missing: ["payload"] };

  const statusVat = String(payload["statusVat"] || "").trim();
  const isNotVat = statusVat.toLowerCase() === "not vat";

  // NOT VAT flow: allow sending with minimal MF footprint.
  // REGON can be missing for some NIPs and should not block the pipeline.
  const requiredCols = isNotVat ? ["statusVat"] : ["name_api", "statusVat", "regon"];
  if (!isNotVat && CONFIG && CONFIG.REQUIRE_KRS_FOR_ADD === true) requiredCols.push("krs");
  for (let i = 0; i < requiredCols.length; i++) {
    const key = requiredCols[i];
    const val = String(payload[key] || "").trim();
    if (!val) missing.push(key);
  }

  if (!isNotVat) {
    const hasWorkingAddress = String(payload["workingAddress"] || "").trim() !== "";
    const hasResidenceAddress = String(payload["residenceAddress"] || "").trim() !== "";
    if (!hasWorkingAddress && !hasResidenceAddress) {
      missing.push("workingAddress_or_residenceAddress");
    }
  }

  return { ready: missing.length === 0, missing: missing };
}

function canRunGovEnrichmentForStatus_(statusVal, idAssignedNow) {
  if (!CONFIG || CONFIG.GOV_ENRICH_ONLY_STATUS_INIT !== true) return true;
  const status = String(statusVal || "").trim();
  const initStatus = String(CONFIG.STATUS_TO_SEND || "").trim();
  if (initStatus && status === initStatus) return true;
  // Newly imported rows keep Status blank until the just-in-time AppSheet Add decision.
  // This allows enrichment before AppSheet/downstream automations take over the status.
  return status === "" && idAssignedNow === true;
}

function resolveInitialMainStatus_(payload) {
  const initStatus = String((CONFIG && CONFIG.STATUS_TO_SEND) || "Init").trim() || "Init";
  const verificationStatus = String((CONFIG && CONFIG.STATUS_NEED_VERIFICATION) || "need verification").trim() || "need verification";
  const reasons = evaluateMainVerificationIssues_(payload);
  return {
    status: reasons.length ? verificationStatus : initStatus,
    needsVerification: reasons.length > 0,
    reasons: reasons
  };
}

function evaluateMainVerificationIssues_(payload) {
  payload = payload || {};
  const issues = [];

  const declaredKnf = normalizeRpkForVerification_(payload["numer wpisu do knf"]);
  const verifiedKnf = normalizeRpkForVerification_(payload["KNF_verified"]);
  if (!declaredKnf || !verifiedKnf || declaredKnf !== verifiedKnf) {
    issues.push("knf_mismatch_or_missing");
  }

  const declaredSwift = normalizeComparableCode_(payload["kod swift banku"]);
  const verifiedSwift = normalizeComparableCode_(payload["swift/bic"]);
  if (declaredSwift !== verifiedSwift) {
    issues.push("swift_mismatch");
  }

  const vatStatus = String(payload["statusVat"] || "").trim().toLowerCase();
  const skipAccountNumbersCheck = vatStatus === "not vat";
  if (!skipAccountNumbersCheck) {
    const primaryAccount = normalizeAccountNumberForVerification_(payload["numer rachunku bankowego"]);
    const accountNumbers = parseAccountNumbersForVerification_(payload["accountNumbers"]);
    if (!primaryAccount || accountNumbers.indexOf(primaryAccount) < 0) {
      issues.push("account_not_in_accountNumbers");
    }
  }

  if (!String(payload["imię i nazwisko przedstawiciela handlowego"] || "").trim()) {
    issues.push("missing_sales_rep_name");
  }
  if (!String(payload["email przedstawiciela handlowego"] || "").trim()) {
    issues.push("missing_sales_rep_email");
  }

  return issues;
}

function normalizeRpkForVerification_(value) {
  const raw = String(value === null || value === undefined ? "" : value).trim();
  if (!raw) return "";
  const direct = raw.match(/\bRPK[\s-]*\d{3,}\b/i);
  if (direct) return direct[0].replace(/[\s-]+/g, "").toUpperCase();
  const digits = raw.replace(/\D/g, "");
  return digits ? ("RPK" + digits) : "";
}

function normalizeComparableCode_(value) {
  return String(value === null || value === undefined ? "" : value)
    .replace(/\s+/g, "")
    .trim()
    .toUpperCase();
}

function normalizeAccountNumberForVerification_(value) {
  return String(value === null || value === undefined ? "" : value).replace(/\D/g, "");
}

function parseAccountNumbersForVerification_(value) {
  const raw = String(value === null || value === undefined ? "" : value).trim();
  if (!raw) return [];
  const parts = raw.split(/[,\s;]+/);
  const out = [];
  const seen = {};
  for (let i = 0; i < parts.length; i++) {
    const account = normalizeAccountNumberForVerification_(parts[i]);
    if (!account || seen[account]) continue;
    seen[account] = true;
    out.push(account);
  }
  return out;
}

function markRowAsNeedVerification_(runId, dest, rowNum, statusIdx, row, reason) {
  if (statusIdx == null || !CONFIG || !CONFIG.STATUS_NEED_VERIFICATION) return;
  const currentStatus = String(row && row[statusIdx] != null ? row[statusIdx] : "").trim();
  const initStatus = String(CONFIG.STATUS_TO_SEND || "").trim();
  const canSet =
    currentStatus === "" ||
    (initStatus !== "" && currentStatus === initStatus) ||
    currentStatus.toLowerCase() === "init";
  if (!canSet || currentStatus === CONFIG.STATUS_NEED_VERIFICATION) return;

  dest.getRange(rowNum, statusIdx + 1).setValue(CONFIG.STATUS_NEED_VERIFICATION);
  if (row) row[statusIdx] = CONFIG.STATUS_NEED_VERIFICATION;
  log_(runId, "WARN", "ROW_MARKED_NEED_VERIFICATION", {
    rowNum: rowNum,
    status: CONFIG.STATUS_NEED_VERIFICATION,
    reason: String(reason || "")
  });
}

function isAppSheetSchemaMismatchError_(err) {
  const s = String(err || "").toLowerCase();
  if (!s) return false;
  return (
    s.indexOf("mismatch in the number of columns") >= 0 ||
    s.indexOf("regenerate the table column structure") >= 0 ||
    (s.indexOf("data table") >= 0 && s.indexOf("is not available") >= 0) ||
    s.indexOf("appsheet_schema_mismatch") >= 0
  );
}

function extractAppSheetStatusFromResponse_(parsed) {
  try {
    const rows = parsed && parsed.Rows;
    if (!rows || !rows.length) return "";
    const status = String(rows[0] && rows[0].Status ? rows[0].Status : "").trim();
    return status;
  } catch (e) {
    return "";
  }
}
