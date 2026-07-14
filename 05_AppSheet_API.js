/** =========================
 *  AppSheet: payload filter + call
 *  ========================= */
function filterPayloadForAppSheet_(payload, allowedCols) {
  const out = {};
  const allow = {};
  for (let i = 0; i < allowedCols.length; i++) allow[allowedCols[i]] = true;

  for (const k in payload) {
    if (allow[k]) out[k] = payload[k];
  }

  out["ID"] = String(out["ID"] || "").trim();
  out["Status"] = String(out["Status"] || "").trim();
  return out;
}

function callAppSheet_(runId, tableName, rowPayload, action, rowNum) {
  const url = CONFIG.APPSHEET_API_URL
    .replace("{appId}", encodeURIComponent(CONFIG.APPSHEET_APP_ID))
    .replace("{table}", encodeURIComponent(tableName));

  const body = {
    Action: action || CONFIG.APPSHEET_ACTION_ADD,
    Properties: { Locale: "pl-PL", Timezone: Session.getScriptTimeZone() },
    Rows: [rowPayload]
  };

  if (isVerbose_()) {
    log_(runId, "INFO", "APPSHEET_REQUEST", {
      rowNum,
      tableName,
      action: body.Action,
      payloadPreview: JSON.stringify(redactSensitiveAppSheetLogData_(rowPayload)).slice(0, CONFIG.MAX_PAYLOAD_PREVIEW_CHARS),
    });
  } else {
    log_(runId, "INFO", "APPSHEET_REQUEST", { rowNum, tableName, action: body.Action });
  }

  const res = UrlFetchApp.fetch(url, {
    method: "post",
    muteHttpExceptions: true,
    contentType: "application/json",
    payload: JSON.stringify(body),
    headers: { "ApplicationAccessKey": CONFIG.APPSHEET_ACCESS_KEY },
    timeout: CONFIG.APPSHEET_TIMEOUT_MS
  });

  const httpCode = res.getResponseCode();
  const text = res.getContentText() || "";
  const parsed = safeJsonParse_(text);

  const headers = safeHeaders_(res.getAllHeaders ? res.getAllHeaders() : {});
  const safeLogText = redactSensitiveAppSheetLogText_(text);
  const bodyLen = text.length;
  const maxFull = CONFIG.MAX_APPSHEET_LOG_BODY_CHARS || CONFIG.MAX_RESPONSE_SNIPPET_CHARS || 4000;
  const fullPreview = (safeLogText.length <= maxFull)
    ? safeLogText
    : (safeLogText.slice(0, maxFull) + `…(truncated, len=${safeLogText.length})`);

  // Always log httpCode. Log full body only in VERBOSE or when non-200.
  if (isVerbose_() || httpCode !== 200) {
    log_(runId, "INFO", "APPSHEET_RESPONSE", {
      rowNum,
      tableName,
      httpCode,
      headers,
      bodyLen,
      body: fullPreview,
    });
  } else {
    log_(runId, "INFO", "APPSHEET_RESPONSE", { rowNum, tableName, httpCode });
  }

  if (httpCode !== 200) {
    const err = new Error(`AppSheet httpCode=${httpCode} body=${safeLogText.slice(0, 900)}`);
    if (isAppSheetSchemaMismatchBody_(text)) {
      err.code = "APPSHEET_SCHEMA_MISMATCH";
    }
    throw err;
  }
  if (parsed && parsed.Success === false) {
    throw new Error(`AppSheet Success=false: ${parsed.ErrorDescription || parsed.Error || "unknown"}`);
  }
  return { httpCode, parsed };
}

function redactSensitiveAppSheetLogText_(text) {
  const raw = String(text || "");
  const parsed = safeJsonParse_(raw);
  if (parsed == null) {
    return raw.replace(/("Credentials"\s*:\s*")[^"]*(")/gi, "$1[REDACTED]$2");
  }
  try {
    return JSON.stringify(redactSensitiveAppSheetLogData_(parsed));
  } catch (e) {
    return "[RESPONSE_REDACTED]";
  }
}

function redactSensitiveAppSheetLogData_(value) {
  if (Array.isArray(value)) {
    return value.map(item => redactSensitiveAppSheetLogData_(item));
  }
  if (!value || typeof value !== "object") return value;

  const out = {};
  Object.keys(value).forEach(key => {
    if (String(key || "").trim().toLowerCase() === "credentials") {
      out[key] = "[REDACTED]";
    } else {
      out[key] = redactSensitiveAppSheetLogData_(value[key]);
    }
  });
  return out;
}

function isAppSheetSchemaMismatchBody_(bodyText) {
  const s = String(bodyText || "").toLowerCase();
  if (!s) return false;
  return (
    s.indexOf("mismatch in the number of columns") >= 0 ||
    s.indexOf("please regenerate the table column structure") >= 0 ||
    s.indexOf("data table") >= 0 && s.indexOf("is not available") >= 0
  );
}
