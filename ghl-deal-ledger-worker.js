/**
 * Leo Deal Ledger Worker — Google Sheets Edition
 *
 * Receives Leo's outgoing webhook when a deal goes under contract and inserts a
 * new row at row 4 (top of data) of the Summit Group Deal Ledger on Google Sheets.
 * Leo's webhook body = { event, delivered_at, lead, contact, phones, stage, board, owners, money, ... }
 * (the same body as GET /v1/leads/:id). Deals already in the ledger (matched on the Leo
 * deal id written into Notes, or on seller name + street) are skipped.
 *
 * ZERO external dependencies — deploys as a single file.
 *
 * Required env vars (unchanged from the GHL version):
 *   GOOGLE_CLIENT_EMAIL  — service account email
 *   GOOGLE_PRIVATE_KEY   — PEM private key (RS256)
 *   SPREADSHEET_ID       — Google Sheets spreadsheet ID
 *   SHEET_NAME           — worksheet tab name (default: "Deal Ledger")
 *   GHL_KV               — KV binding used to cache the Google token (name kept so no rebinding is needed)
 * Optional:
 *   LEO_WEBHOOK_SECRET   — if set, requests must carry it as ?secret=... or X-Webhook-Secret header
 */

// ─── Google Service Account Auth (JWT → Access Token) ───────────────────────

function base64url(buf) {
  const bytes = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function importPrivateKey(pem) {
  const pemBody = pem
    .replace(/-----BEGIN PRIVATE KEY-----/g, "")
    .replace(/-----END PRIVATE KEY-----/g, "")
    .replace(/\\n/g, "")
    .replace(/[\r\n\s]/g, "");
  const binary = atob(pemBody);
  const buf = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) buf[i] = binary.charCodeAt(i);
  return crypto.subtle.importKey(
    "pkcs8", buf.buffer,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false, ["sign"]
  );
}

async function getGoogleAccessToken(env) {
  const cached = await env.GHL_KV.get("google_access_token");
  if (cached) return cached;

  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss: env.GOOGLE_CLIENT_EMAIL,
    scope: "https://www.googleapis.com/auth/spreadsheets",
    aud: "https://oauth2.googleapis.com/token",
    iat: now,
    exp: now + 3600,
  };

  const enc = new TextEncoder();
  const headerB64 = base64url(enc.encode(JSON.stringify(header)));
  const payloadB64 = base64url(enc.encode(JSON.stringify(payload)));
  const unsignedToken = headerB64 + "." + payloadB64;

  const key = await importPrivateKey(env.GOOGLE_PRIVATE_KEY);
  const sig = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, enc.encode(unsignedToken));
  const jwt = unsignedToken + "." + base64url(sig);

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=" + jwt,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error("Google token request failed (" + res.status + "): " + text);
  }

  const data = await res.json();
  const ttl = Math.max((data.expires_in || 3600) - 120, 60);
  await env.GHL_KV.put("google_access_token", data.access_token, { expirationTtl: ttl });
  return data.access_token;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function clean(val) {
  if (val === null || val === undefined) return "";
  const s = String(val).trim();
  if (s === "null" || s === "undefined" || s === "") return "";
  return s;
}

function formatMoney(val) {
  if (!val) return "";
  const s = String(val).replace(/[^0-9.]/g, "");
  if (!s) return "";
  const num = parseFloat(s);
  if (isNaN(num)) return "";
  return "$" + num.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 2 });
}

function excelSerialToDate(serial) {
  return new Date((serial - 25569) * 86400 * 1000);
}

function parseAnyDate(val) {
  const s = clean(val);
  if (!s) return null;
  const num = Number(s);
  if (!isNaN(num) && num > 40000 && num < 60000) return excelSerialToDate(num);
  const d = new Date(s);
  if (!isNaN(d)) return d;
  return null;
}

function formatDate(val) {
  const d = parseAnyDate(val);
  if (!d) return clean(val) || "";
  return (d.getUTCMonth() + 1) + "/" + d.getUTCDate() + "/" + d.getUTCFullYear();
}

function formatMonthYear(val) {
  const d = parseAnyDate(val);
  if (!d) return "";
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const yy = String(d.getUTCFullYear()).slice(-2);
  return mm + "-" + yy;
}

// ─── Extract Deal from a Leo webhook body ───────────────────────────────────

const OWNER_FIRST = { aubrey: "Aubrey", luka: "Luka", brennen: "Brennen" };
function ownerName(o, fallback) {
  const full = clean(o && o.name);
  if (!full) return fallback;
  const first = full.split(/\s+/)[0];
  return OWNER_FIRST[first.toLowerCase()] || first;
}

function strategyFromLeo(lead) {
  const t = clean(lead.offer_type).toLowerCase();
  if (t.includes("novation")) return "Novation";
  if (t.includes("cash")) return "Cash";
  if (t.includes("sub") && t.includes("to")) return "Subject-to";
  if (t.includes("seller") && t.includes("finance")) return "Seller Finance";
  if (t.includes("list")) return "Listing Agent";
  if (t.includes("creative")) return "Creative";
  return t ? t.charAt(0).toUpperCase() + t.slice(1) : "Unknown";
}

function extractLeoDeal(body) {
  const lead = body.lead || {};
  const contact = body.contact || {};
  const owners = body.owners || {};
  const money = body.money || {};

  const sellerName = clean(contact.full_name) || [clean(contact.first_name), clean(contact.last_name)].filter(Boolean).join(" ") || "Unknown";
  const propertyAddress = [clean(lead.property_line1), clean(lead.property_city), clean(lead.property_state), clean(lead.property_zip)].filter(Boolean).join(", ");
  const strategy = strategyFromLeo(lead);

  const underContractDate = formatDate(lead.contract_date || body.delivered_at || new Date().toISOString());
  const closingDate = lead.closing_date ? formatDate(lead.closing_date) : "";

  // Columns that no longer exist on the simplified sheet (acq/dispo owner, list/buyer
  // price, rehab, projected fee, etc.) are preserved in Notes instead of being dropped.
  const notes = ["Auto-added from Leo webhook.", "Leo:" + clean(lead.id)];
  if (clean(lead.offer_type)) notes.push("Deal type: " + clean(lead.offer_type) + (lead.is_double_close ? " (double close)" : "") + (lead.is_flat_fee ? " (flat fee)" : ""));
  const projFee = formatMoney(lead.assignment_fee || lead.projected_fee || lead.target_fee || money.projected_profit);
  if (projFee) notes.push("Projected fee: " + projFee);
  if (clean(money.emd_amount)) notes.push("EMD: " + formatMoney(money.emd_amount));
  if (clean(lead.inspection_period_end)) notes.push("Inspection ends: " + formatDate(lead.inspection_period_end));
  if (clean(lead.emd_due)) notes.push("EMD due: " + formatDate(lead.emd_due));
  if (clean(lead.payoff_amount)) notes.push("Payoff: " + formatMoney(lead.payoff_amount));
  if (clean(lead.arv)) notes.push("ARV: " + formatMoney(lead.arv));
  if (clean(lead.rehab_costs)) notes.push("Rehab: " + formatMoney(lead.rehab_costs));
  if (clean(lead.list_price)) notes.push("List price: " + formatMoney(lead.list_price));
  const acq = ownerName(owners.acquisitions, "");
  const dispo = ownerName(owners.dispositions, "");
  if (acq) notes.push("Acq: " + acq);
  if (dispo) notes.push("Dispo: " + dispo);
  if (body.stage && body.stage.name) notes.push("Leo stage: " + body.stage.name);

  // Source: prefer a "Source: ..." tag (the marketing channel), else Leo's record source.
  let dealSource = "";
  if (Array.isArray(body.tags)) {
    const st = body.tags.find(function (t) { return /^\s*source\s*:/i.test(String(t)); });
    if (st) dealSource = String(st).replace(/^\s*source\s*:\s*/i, "").trim();
  }
  if (!dealSource) dealSource = clean(lead.source);

  return {
    leoId: clean(lead.id),               // kept for duplicate detection (also written into Notes)
    dealId: sellerName,                  // A: Deal ID
    propertyAddress,                     // B: Property Address
    market: clean(lead.property_state),  // C: Market
    dealStatus: "Under Contract",        // D: Deal Status
    reasonLost: "",                      // E: Reason Lost — filled in manually when a deal is lost
    source: dealSource,                  // F: Source — auto-filled from Leo (tag or record source)
    strategy,                            // G: Strategy
    underContractDate,                   // H: Under Contract Date
    closeDateActualEst: closingDate,     // I: Close Date (Actual/Est)
    contractPrice: formatMoney(lead.contract_price), // J: Contract Price ($)
    amountInEscrow: "",                  // K: Amount in Escrow ($) — filled in manually
    finalProfit: "",                     // L: Final Profit ($) — filled in manually
    notes: notes.join(" | "),            // M: Notes
  };
}

// Is this webhook a deal going under contract? (Leo's transaction board, or the Active board's pre-dispo UC stage)
function isUnderContractEvent(body) {
  const boardKind = clean(body.board && body.board.kind).toLowerCase();
  const stageSlug = clean(body.stage && body.stage.slug).toLowerCase();
  const stageName = clean(body.stage && body.stage.name).toLowerCase();
  if (boardKind === "transaction") return true;
  if (stageSlug.includes("under_contract") || stageName.includes("under contract")) return true;
  return false;
}

// ─── Duplicate check against the sheet (Leo id in Notes, or seller + street) ─

async function alreadyInLedger(env, deal) {
  const token = await getGoogleAccessToken(env);
  const sheetName = env.SHEET_NAME || "Deal Ledger";
  const res = await fetch("https://sheets.googleapis.com/v4/spreadsheets/" + env.SPREADSHEET_ID + "/values/" + encodeURIComponent("'" + sheetName + "'!A4:M"), { headers: { Authorization: "Bearer " + token } });
  if (!res.ok) return false;   // if we cannot read, do not block the insert
  const rows = (await res.json()).values || [];
  const norm = s => String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const street = norm((deal.propertyAddress || "").split(",")[0]);
  const name = norm(deal.dealId);
  for (const r of rows) {
    const notes = String(r[12] || "");   // Notes is column M (index 12)
    if (deal.leoId && notes.includes("Leo:" + deal.leoId)) return "leo id";
    if (name && norm(r[0]) === name && street && norm(r[1]).startsWith(street)) return "name + street";
  }
  return false;
}

// ─── Row Builder ─────────────────────────────────────────────────────────────

// Column order MUST match the sheet headers (A:M):
// Deal ID | Property Address | Market | Deal Status | Reason Lost | Source | Strategy |
// Under Contract Date | Close Date (Actual/Est) | Contract Price ($) |
// Amount in Escrow ($) | Final Profit ($) | Notes
function dealToRow(deal) {
  return [
    deal.dealId,             // A
    deal.propertyAddress,    // B
    deal.market,             // C
    deal.dealStatus,         // D
    deal.reasonLost,         // E
    deal.source,             // F
    deal.strategy,           // G
    deal.underContractDate,  // H
    deal.closeDateActualEst, // I
    deal.contractPrice,      // J
    deal.amountInEscrow,     // K
    deal.finalProfit,        // L
    deal.notes,              // M
  ];
}

// ─── Write to Google Sheets ─────────────────────────────────────────────────

async function writeToLedger(env, deal) {
  const token = await getGoogleAccessToken(env);
  const spreadsheetId = env.SPREADSHEET_ID;
  const sheetName = env.SHEET_NAME || "Deal Ledger";
  const baseUrl = "https://sheets.googleapis.com/v4/spreadsheets/" + spreadsheetId;
  const headers = {
    Authorization: "Bearer " + token,
    "Content-Type": "application/json",
  };

  // First data row (1-based). New deals insert here so the newest sits on top.
  // Override with the FIRST_DATA_ROW env var if the header rows change; defaults to 4.
  const dataRow = parseInt(env.FIRST_DATA_ROW, 10) || 4;

  // Step 1: Insert a blank row at the top of the data area to push existing rows down
  const insertRes = await fetch(baseUrl + ":batchUpdate", {
    method: "POST",
    headers,
    body: JSON.stringify({
      requests: [{
        insertDimension: {
          range: {
            sheetId: 0,
            dimension: "ROWS",
            startIndex: dataRow - 1,
            endIndex: dataRow,
          },
          inheritFromBefore: false,
        },
      }],
    }),
  });

  if (!insertRes.ok) {
    const text = await insertRes.text();
    throw new Error("Sheets insertDimension failed (" + insertRes.status + "): " + text);
  }

  // Step 2: Write the deal data into the new row (A:M = 13 columns)
  const rowValues = dealToRow(deal);
  const updateRes = await fetch(
    baseUrl + "/values/" + encodeURIComponent("'" + sheetName + "'!A" + dataRow + ":M" + dataRow) + "?valueInputOption=USER_ENTERED",
    {
      method: "PUT",
      headers,
      body: JSON.stringify({ values: [rowValues] }),
    }
  );

  if (!updateRes.ok) {
    const text = await updateRes.text();
    throw new Error("Sheets values update failed (" + updateRes.status + "): " + text);
  }

  console.log("Row inserted: " + deal.dealId + " | " + deal.strategy);
  return { success: true, deal: deal.dealId, address: deal.propertyAddress, strategy: deal.strategy };
}

// ─── Worker Entry Points ─────────────────────────────────────────────────────

function json(obj, status) {
  return new Response(JSON.stringify(obj, null, 2), { status: status || 200, headers: { "Content-Type": "application/json" } });
}

function authorized(request, url, env) {
  if (!env.LEO_WEBHOOK_SECRET) return true;
  return url.searchParams.get("secret") === env.LEO_WEBHOOK_SECRET || request.headers.get("X-Webhook-Secret") === env.LEO_WEBHOOK_SECRET;
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/health") return json({ status: "ok", crm: "leo", time: new Date().toISOString() });

    if ((url.pathname === "/webhook" || url.pathname === "/test") && request.method === "POST") {
      if (!authorized(request, url, env)) return json({ error: "unauthorized" }, 401);
      try {
        const body = await request.json();
        if (!body || !body.lead) return json({ error: "Expected a Leo webhook body with a lead object", keys: Object.keys(body || {}) }, 400);
        const deal = extractLeoDeal(body);
        const underContract = isUnderContractEvent(body);
        const force = url.searchParams.get("force") === "1";

        if (url.pathname === "/test") return json({ event: body.event, underContract, deal, row: dealToRow(deal) });

        if (!underContract && !force) return json({ skipped: true, reason: "not an under-contract stage", event: body.event, stage: body.stage && body.stage.name, board: body.board && body.board.kind });
        const dup = await alreadyInLedger(env, deal);
        if (dup && !force) return json({ skipped: true, reason: "already in ledger (" + dup + ")", deal: deal.dealId });

        const result = await writeToLedger(env, deal);
        return json(Object.assign({ event: body.event }, result));
      } catch (err) {
        console.error("Webhook error: " + err.message);
        return json({ error: err.message }, 500);
      }
    }

    return new Response("Leo Deal Ledger Worker. POST Leo's outgoing webhook to /webhook (use /test to preview the row without writing).", { status: 200 });
  },
};
