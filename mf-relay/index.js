import express from "express";
import axios from "axios";
import { HttpsProxyAgent } from "https-proxy-agent";

const app = express();
app.use(express.json({ limit: "256kb" }));

const PORT = Number(process.env.PORT || 8080);
const RELAY_AUTH_TOKEN = String(process.env.RELAY_AUTH_TOKEN || "").trim();
const PROXY_HOST = String(process.env.PROXY_HOST || process.env.BRD_PROXY_HOST || "brd.superproxy.io").trim();
const PROXY_PORT = Number(process.env.PROXY_PORT || process.env.BRD_PROXY_PORT || 33335);
const PROXY_USER = String(process.env.PROXY_USER || process.env.BRD_PROXY_USERNAME || "").trim();
const PROXY_PASS = String(process.env.PROXY_PASS || process.env.BRD_PROXY_PASSWORD || "").trim();
const MF_BASE_URL = String(process.env.MF_BASE_URL || "https://wl-api.mf.gov.pl").trim().replace(/\/+$/, "");
const TIMEOUT_MS = Number(process.env.MF_TIMEOUT_MS || process.env.MF_RELAY_TIMEOUT_MS || 20000);

function unauthorized(res) {
  return res.status(401).json({ error: "unauthorized" });
}

function parseBearerToken(req) {
  const auth = String(req.headers.authorization || "");
  if (!auth.toLowerCase().startsWith("bearer ")) return "";
  return auth.slice(7).trim();
}

function parseRelayAuthToken(req) {
  const fromHeader = String(req.headers["x-relay-auth"] || "").trim();
  if (fromHeader) return fromHeader;
  return parseBearerToken(req);
}

function validateInput(nip, date) {
  const nipDigits = String(nip || "").replace(/\D/g, "");
  if (!/^\d{10}$/.test(nipDigits)) {
    return { ok: false, error: "invalid_nip" };
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(date || ""))) {
    return { ok: false, error: "invalid_date" };
  }
  return { ok: true, nip: nipDigits, date: String(date) };
}

function buildProxyAgent() {
  if (!PROXY_USER || !PROXY_PASS) {
    throw new Error("missing_proxy_credentials");
  }
  const proxyUrl = `http://${encodeURIComponent(PROXY_USER)}:${encodeURIComponent(PROXY_PASS)}@${PROXY_HOST}:${PROXY_PORT}`;
  return new HttpsProxyAgent(proxyUrl);
}

app.get("/health", (_req, res) => {
  res.status(200).json({ ok: true });
});

app.post("/mf/search", async (req, res) => {
  try {
    if (RELAY_AUTH_TOKEN) {
      const token = parseRelayAuthToken(req);
      if (!token || token !== RELAY_AUTH_TOKEN) {
        return unauthorized(res);
      }
    }

    const input = validateInput(req.body?.nip, req.body?.date);
    if (!input.ok) {
      return res.status(400).json({ error: input.error });
    }

    const url = `${MF_BASE_URL}/api/search/nip/${encodeURIComponent(input.nip)}?date=${encodeURIComponent(input.date)}`;
    const agent = buildProxyAgent();

    const response = await axios.get(url, {
      httpsAgent: agent,
      proxy: false,
      timeout: TIMEOUT_MS,
      validateStatus: () => true
    });

    return res.status(response.status).json(response.data);
  } catch (err) {
    const message = err?.message ? String(err.message) : "relay_error";
    return res.status(502).json({ error: "relay_failed", message });
  }
});

app.get("/debug/proxy-check", async (req, res) => {
  try {
    if (RELAY_AUTH_TOKEN) {
      const token = parseRelayAuthToken(req);
      if (!token || token !== RELAY_AUTH_TOKEN) {
        return unauthorized(res);
      }
    }

    const agent = buildProxyAgent();
    const url = "https://geo.brdtest.com/welcome.txt?product=resi&method=native";
    const response = await axios.get(url, {
      httpsAgent: agent,
      proxy: false,
      timeout: TIMEOUT_MS,
      validateStatus: () => true
    });

    return res.status(response.status).json({
      ok: response.status >= 200 && response.status < 300,
      status: response.status,
      via: "proxy",
      body: String(response.data || "").slice(0, 2000)
    });
  } catch (err) {
    const message = err?.message ? String(err.message) : "proxy_check_error";
    return res.status(502).json({ error: "proxy_check_failed", message });
  }
});

app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`mf-relay listening on :${PORT}`);
});
