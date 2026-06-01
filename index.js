/**
 * GHL Deal Ledger Worker — Google Sheets Edition (v2: PandaDoc support)
 *
 * Endpoints:
 *   POST /webhook            — Original GHL native contract webhook
 *   POST /pandadoc-webhook   — NEW: PandaDoc signed-document webhook
 *   POST /test               — Test payload parsing without writing
 *   GET  /health             — Health check
 *
 * Required env vars:
 *   GOOGLE_CLIENT_EMAIL       — service account email
 *   GOOGLE_PRIVATE_KEY        — PEM private key (RS256)
 *   SPREADSHEET_ID            — Google Sheets spreadsheet ID
 *   SHEET_NAME                — worksheet tab name (default: "Deal Ledger")
 *   PANDADOC_WEBHOOK_SECRET   — NEW: shared secret from PandaDoc webhook subscription
 *   GHL_KV                    — KV namespace binding for Google access token cache
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

// ─── Deal Type Detection ────────────────────────────────────────────────────

function detectDealType(payload) {
  const explicit = (payload.deal_type || payload.dealType || "").toLowerCase();
  if (explicit.includes("novation")) return "Novation";
  if (explicit.includes("cash")) return "Cash";
  if (explicit.includes("sub to") || explicit.includes("sub_to") || explicit.includes("sub-to") || explicit.includes("subject to") || explicit.includes("subject-to")) return "Subject-to";
  if (explicit.includes("seller finance") || explicit.includes("seller_finance")) return "Seller Finance";

  const docName = (payload.document_name || payload.workflow_name || payload.name || "").toLowerCase();
  if (docName.includes("novation")) return "Novation";
  if (docName.includes("cash")) return "Cash";
  if (docName.includes("sub to") || docName.includes("sub_to") || docName.includes("sub-to") || docName.includes("subject to") || docName.includes("subject-to")) return "Subject-to";
  if (docName.includes("seller finance") || docName.includes("seller_finance")) return "Seller Finance";
  if (docName.includes("amendment")) return "Amendment";
  if (docName.includes("assignment")) return "Assignment";

  if (payload.purchase_price_novation || payload.closing_date_novation || payload.emd_novation) return "Novation";
  if (payload.purchase_price_cash || payload.closing_date_cash || payload.county_cash) return "Cash";
  if (payload.seller_finance_terms) return "Seller Finance";
  if (payload.existing_mortgage_balance || payload.monthly_mortgage_payment) return "Subject-to";

  return "Unknown";
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

// ─── Extract Deal from Webhook Payload ──────────────────────────────────────

function extractDeal(payload) {
  const dealType = detectDealType(payload);

  const firstName = clean(payload.first_name) || clean(payload.firstName) || clean(payload.contact_first_name);
  const lastName = clean(payload.last_name) || clean(payload.lastName) || clean(payload.contact_last_name);
  const sellerName = (firstName + " " + lastName).trim();

  const fullAddress = clean(payload.full_address_1);
  const street = clean(payload.address1) || clean(payload.street_address);
  const city = clean(payload.city);
  const state = clean(payload.state);
  const zip = clean(payload.postal_code) || clean(payload.zip);
  const propertyAddress = fullAddress || [street, city, state, zip].filter(Boolean).join(", ");

  let cleanMarket = state || "";
  if (!cleanMarket && fullAddress) {
    const parts = fullAddress.split(",").map(function(s) { return s.trim(); });
    if (parts.length >= 3) {
      const stateZip = parts[parts.length - 1];
      const stateMatch = stateZip.match(/^([A-Za-z\s]+)/);
      if (stateMatch) cleanMarket = stateMatch[1].trim();
    }
  }

  let contractPrice = "";
  let closingDate = "";
  let underContractDate = "";
  let earnestMoney = "";
  let existingMortgage = "";
  let balanceAtClosing = "";
  let notes = ["Auto-added from " + (payload._source || "GHL") + " webhook."];

  if (dealType === "Novation") {
    contractPrice = formatMoney(payload.purchase_price_novation);
    closingDate = formatDate(payload.closing_date_novation);
    underContractDate = formatDate(payload.date_completed_by_novation);
    earnestMoney = clean(payload.emd_novation);
    if (clean(payload.additional_terms)) notes.push("Terms: " + clean(payload.additional_terms));
  } else if (dealType === "Cash") {
    contractPrice = formatMoney(payload.purchase_price_cash);
    closingDate = formatDate(payload.closing_date_cash);
    underContractDate = formatDate(payload.date_completed_by_cash);
    balanceAtClosing = clean(payload.amt_due_at_closing_cash);
    if (clean(payload.due_diligence_cash)) notes.push("Due Diligence: " + clean(payload.due_diligence_cash) + " days");
  } else if (dealType === "Subject-to") {
    contractPrice = formatMoney(payload.total_purchase_price);
    closingDate = formatDate(payload.closing_date);
    underContractDate = formatDate(payload.date_and_time_completed_by);
    existingMortgage = formatMoney(payload.existing_mortgage_balance);
    if (clean(payload.monthly_mortgage_payment)) notes.push("Monthly Payment: " + formatMoney(payload.monthly_mortgage_payment));
    if (clean(payload.years_remaining_on_mortgage)) notes.push("Years Remaining: " + clean(payload.years_remaining_on_mortgage));
    if (clean(payload.months_remaining_on_mortgage)) notes.push("Months Remaining: " + clean(payload.months_remaining_on_mortgage));
    if (clean(payload.deposit)) earnestMoney = formatMoney(payload.deposit);
  } else if (dealType === "Seller Finance") {
    contractPrice = formatMoney(payload.total_purchase_price);
    closingDate = formatDate(payload.closing_date);
    underContractDate = formatDate(payload.date_and_time_completed_by);
    existingMortgage = formatMoney(payload.existing_mortgage_balance);
    if (clean(payload.seller_finance_terms)) notes.push("SF Terms: " + clean(payload.seller_finance_terms));
    if (clean(payload.monthly_mortgage_payment)) notes.push("Monthly Payment: " + formatMoney(payload.monthly_mortgage_payment));
    if (clean(payload.down_payment)) notes.push("Down Payment: " + formatMoney(payload.down_payment));
    if (clean(payload.deposit)) earnestMoney = formatMoney(payload.deposit);
  }

  if (clean(payload.amendment_purchase_price)) {
    contractPrice = formatMoney(payload.amendment_purchase_price);
    notes.push("Amendment applied");
  }
  if (clean(payload.amendment_closing_date)) {
    closingDate = formatDate(payload.amendment_closing_date);
  }
  if (clean(payload.amendment__other_notes)) {
    notes.push("Amendment Notes: " + clean(payload.amendment__other_notes));
  }

  if (earnestMoney) notes.push("EMD: " + earnestMoney);
  if (existingMortgage) notes.push("Existing Mortgage: " + existingMortgage);
  if (balanceAtClosing) notes.push("Balance at Closing: " + balanceAtClosing);

  let month = "";
  if (underContractDate) month = formatMonthYear(underContractDate);

  return {
    dealId: sellerName || "Unknown",
    propertyAddress,
    market: cleanMarket || state || "",
    acqOwner: "Brennen",
    dispositionOwner: "Aubrey",
    dealStatus: "Under Contract",
    strategy: dealType,
    exitType: "Assignment",
    underContractDate,
    closeDateActualEst: closingDate,
    month,
    contractPrice,
    listedPostedPrice: "",
    buyerPriceSalePrice: "",
    repairs: "",
    potentialProfit: "",
    buyerName: "",
    listingAgentName: "",
    finalProfit: "",
    notes: notes.join(" | "),
  };
}

// ─── Row Builder ─────────────────────────────────────────────────────────────

function dealToRow(deal) {
  return [
    deal.dealId, deal.propertyAddress, deal.market, deal.acqOwner,
    deal.dispositionOwner, deal.dealStatus, deal.strategy, deal.exitType,
    deal.underContractDate, deal.closeDateActualEst, deal.month, deal.contractPrice,
    deal.listedPostedPrice, deal.buyerPriceSalePrice, deal.repairs,
    deal.potentialProfit, deal.buyerName, deal.listingAgentName, deal.finalProfit, deal.notes,
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

  // Step 1: Insert a blank row at row 4 (0-indexed row 3) to push data down
  const insertRes = await fetch(baseUrl + ":batchUpdate", {
    method: "POST",
    headers,
    body: JSON.stringify({
      requests: [{
        insertDimension: {
          range: {
            sheetId: 0,
            dimension: "ROWS",
            startIndex: 3,
            endIndex: 4,
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

  // Step 2: Write the deal data into row 4 (A:T = 20 columns)
  const rowValues = dealToRow(deal);
  const updateRes = await fetch(
    baseUrl + "/values/" + encodeURIComponent("'" + sheetName + "'!A4:T4") + "?valueInputOption=USER_ENTERED",
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

// ─── Flatten nested GHL payload ─────────────────────────────────────────────

function flattenPayload(raw) {
  const flat = {};

  function merge(obj) {
    if (!obj || typeof obj !== "object") return;
    for (const [key, val] of Object.entries(obj)) {
      if (val && typeof val === "object" && !Array.isArray(val) && key !== "customData") {
        merge(val);
      } else {
        flat[key] = val;
      }
    }
  }

  merge(raw);

  if (Array.isArray(raw.customData)) {
    for (const item of raw.customData) {
      if (item.field_key && item.value !== undefined) flat[item.field_key] = item.value;
    }
  }
  if (raw.contact && Array.isArray(raw.contact.customData)) {
    for (const item of raw.contact.customData) {
      if (item.field_key && item.value !== undefined) flat[item.field_key] = item.value;
    }
  }

  return flat;
}

// ═══════════════════════════════════════════════════════════════════════════
// NEW: PandaDoc Webhook Handler
// ═══════════════════════════════════════════════════════════════════════════

// Verify PandaDoc HMAC signature
async function verifyPandaDocSignature(signatureHeader, body, secret) {
  if (!signatureHeader || !secret) return false;

  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw", enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false, ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(body));
  const sigHex = Array.from(new Uint8Array(sig))
    .map(function(b) { return b.toString(16).padStart(2, "0"); })
    .join("");

  // Allow signature header to be raw hex OR "sha256=hex" prefixed
  const provided = signatureHeader.replace(/^sha256=/i, "").trim();
  return provided === sigHex;
}

// Convert PandaDoc tokens array → flat object keyed by token name
function pandaDocTokensToMap(tokens) {
  const map = {};
  if (!Array.isArray(tokens)) return map;
  for (const t of tokens) {
    if (t && t.name) map[t.name] = t.value;
  }
  return map;
}

// Find the primary seller recipient (first signer that isn't the owner/buyer)
function findSellerRecipient(recipients) {
  if (!Array.isArray(recipients) || recipients.length === 0) return {};
  // Prefer role match
  const byRole = recipients.find(function(r) {
    return r && typeof r.role === "string" && /seller\s*1|seller$/i.test(r.role);
  });
  if (byRole) return byRole;
  // Otherwise first signer
  const signer = recipients.find(function(r) { return r && r.recipient_type === "signer"; });
  return signer || recipients[0] || {};
}

// Reshape PandaDoc data into the GHL-style payload that extractDeal() expects
function pandaDocToGHLShape(pdData) {
  const tokens = pandaDocTokensToMap(pdData.tokens || pdData.fields);
  const seller = findSellerRecipient(pdData.recipients);

  // Seller name: prefer token, fallback to recipient name fields
  let firstName = seller.first_name || "";
  let lastName = seller.last_name || "";
  if (tokens.seller_name && !(firstName || lastName)) {
    const parts = String(tokens.seller_name).trim().split(/\s+/);
    firstName = parts[0] || "";
    lastName = parts.slice(1).join(" ");
  }

  const shape = {
    _source: "PandaDoc",
    document_name: pdData.name || "",
    first_name: firstName,
    last_name: lastName,
    email: tokens.seller_email || seller.email || "",
    phone: tokens.seller_phone || seller.phone || "",
    full_address_1: tokens.property_address || tokens.seller_address || "",
  };

  // Detect deal type from document name
  const dealType = detectDealType({ document_name: pdData.name });

  // Map tokens to deal-type-specific field names
  if (dealType === "Novation") {
    shape.purchase_price_novation = tokens.purchase_price || "";
    shape.closing_date_novation = tokens.closing_date || "";
    shape.date_completed_by_novation = tokens.acceptance_date || pdData.date_completed || "";
    shape.time_completed_by_novation = tokens.acceptance_time || "";
    shape.emd_novation = tokens.emd_amount || "";
    shape.additional_terms = tokens.additional_terms || "";
  } else if (dealType === "Cash") {
    shape.purchase_price_cash = tokens.purchase_price || "";
    shape.closing_date_cash = tokens.closing_date || "";
    shape.date_completed_by_cash = tokens.acceptance_date || pdData.date_completed || "";
    shape.amt_due_at_closing_cash = tokens.balance_due_at_closing || "";
    shape.due_diligence_cash = tokens.due_diligence_days || "";
    shape.additional_terms = tokens.additional_terms || "";
  } else if (dealType === "Subject-to") {
    shape.total_purchase_price = tokens.purchase_price || "";
    shape.closing_date = tokens.closing_date || "";
    shape.date_and_time_completed_by = tokens.acceptance_date || pdData.date_completed || "";
    shape.existing_mortgage_balance = tokens.existing_mortgage_balance || "";
    shape.monthly_mortgage_payment = tokens.monthly_mortgage_payment || "";
    shape.deposit = tokens.emd_amount || tokens.deposit || "";
  } else if (dealType === "Seller Finance") {
    shape.total_purchase_price = tokens.purchase_price || "";
    shape.closing_date = tokens.closing_date || "";
    shape.date_and_time_completed_by = tokens.acceptance_date || pdData.date_completed || "";
    shape.seller_finance_terms = tokens.seller_finance_terms || tokens.additional_terms || "";
    shape.down_payment = tokens.down_payment || "";
    shape.deposit = tokens.emd_amount || tokens.deposit || "";
  } else if (dealType === "Amendment") {
    // Amendments update an existing row — for now, also create a row tagged so you can spot it.
    shape.amendment_purchase_price = tokens.purchase_price || "";
    shape.amendment_closing_date = tokens.closing_date || "";
    shape.amendment__other_notes = tokens.other || tokens.additional_terms || "";
  }

  return { shape, dealType };
}

async function handlePandaDocWebhook(request, env) {
  const body = await request.text();

  // Signature verification
  const signature = request.headers.get("x-pandadoc-signature")
                 || request.headers.get("X-PandaDoc-Signature")
                 || request.headers.get("signature");

  const sigValid = await verifyPandaDocSignature(signature, body, env.PANDADOC_WEBHOOK_SECRET);
  if (!sigValid) {
    console.warn("PandaDoc webhook signature invalid");
    return new Response(JSON.stringify({ error: "Invalid signature" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  // PandaDoc sends an array of events
  let events;
  try {
    const parsed = JSON.parse(body);
    events = Array.isArray(parsed) ? parsed : [parsed];
  } catch (err) {
    return new Response(JSON.stringify({ error: "Invalid JSON" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const results = [];
  for (const ev of events) {
    try {
      const eventType = ev.event || ev.event_type || "";
      const data = ev.data || ev;
      const status = (data.status || "").toLowerCase();

      // Only act on signed/completed documents
      const isCompleted = status === "document.completed"
                       || status === "completed"
                       || eventType === "document_state_changed" && status === "document.completed";

      if (!isCompleted) {
        results.push({ skipped: true, reason: "Not a completion event", event: eventType, status });
        continue;
      }

      const { shape, dealType } = pandaDocToGHLShape(data);

      // Amendment / Assignment: skip writing a new row (they modify existing deals)
      if (dealType === "Amendment" || dealType === "Assignment") {
        results.push({ skipped: true, reason: dealType + " — update logic not yet implemented", documentName: data.name });
        continue;
      }

      if (dealType === "Unknown") {
        results.push({ skipped: true, reason: "Unknown deal type from document name", documentName: data.name });
        continue;
      }

      const deal = extractDeal(shape);
      console.log("PandaDoc deal: " + deal.dealId + " | " + deal.strategy + " | " + deal.contractPrice);

      const result = await writeToLedger(env, deal);
      results.push(result);
    } catch (err) {
      console.error("PandaDoc event error: " + err.message);
      results.push({ error: err.message });
    }
  }

  return new Response(JSON.stringify({ results }), {
    headers: { "Content-Type": "application/json" },
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// END NEW PandaDoc handler
// ═══════════════════════════════════════════════════════════════════════════

// ─── Worker Entry Points ─────────────────────────────────────────────────────

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return new Response(JSON.stringify({ status: "ok", time: new Date().toISOString() }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // ── Existing GHL native contract webhook ──
    if (url.pathname === "/webhook" && request.method === "POST") {
      try {
        const rawPayload = await request.json();
        console.log("GHL Webhook received: " + Object.keys(rawPayload).join(", "));

        const payload = flattenPayload(rawPayload);
        const deal = extractDeal(payload);
        console.log("Deal: " + deal.dealId + " | " + deal.strategy + " | " + deal.contractPrice);

        const result = await writeToLedger(env, deal);

        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        console.error("Webhook error: " + err.message);
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    // ── NEW: PandaDoc webhook ──
    if (url.pathname === "/pandadoc-webhook" && request.method === "POST") {
      return await handlePandaDocWebhook(request, env);
    }

    // ── Test endpoint (no write) ──
    if (url.pathname === "/test" && request.method === "POST") {
      try {
        const rawPayload = await request.json();
        const payload = flattenPayload(rawPayload);
        const deal = extractDeal(payload);
        return new Response(JSON.stringify({ deal, row: dealToRow(deal) }, null, 2), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    return new Response("GHL Deal Ledger Worker v2. POST to /webhook (GHL) or /pandadoc-webhook (PandaDoc).", { status: 200 });
  },
};
