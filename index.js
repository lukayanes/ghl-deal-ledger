/**
 * GHL Deal Ledger Worker — Google Sheets Edition
 *
 * Receives a webhook POST from GoHighLevel when a contract is signed,
 * extracts deal data from the contact's custom fields, and inserts a
 * new row at row 4 (top of data) of the Summit Group Deal Ledger on Google Sheets.
 *
 * ZERO external dependencies — deploys as a single file.
 *
 * Required env vars:
 *   GOOGLE_CLIENT_EMAIL  — service account email
 *   GOOGLE_PRIVATE_KEY   — PEM private key (RS256)
 *   SPREADSHEET_ID       — Google Sheets spreadsheet ID
 *   SHEET_NAME           — worksheet tab name (default: "Deal Ledger")
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
  let notes = ["Auto-added from GHL webhook."];

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
    deal.potentialProfit, deal.finalProfit, deal.notes,
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

  // Step 2: Write the deal data into row 4
  const rowValues = dealToRow(deal);
  const updateRes = await fetch(
    baseUrl + "/values/" + encodeURIComponent(sheetName + "!A4:R4") + "?valueInputOption=USER_ENTERED",
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

// ─── Worker Entry Points ─────────────────────────────────────────────────────

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return new Response(JSON.stringify({ status: "ok", time: new Date().toISOString() }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    if (url.pathname === "/webhook" && request.method === "POST") {
      try {
        const rawPayload = await request.json();
        console.log("Webhook received: " + Object.keys(rawPayload).join(", "));

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

    return new Response("GHL Deal Ledger Worker. POST to /webhook to add a deal.", { status: 200 });
  },
};
