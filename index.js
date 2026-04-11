/**
 * GHL Deal Ledger Worker — Webhook Edition
 *
 * Receives a webhook POST from GoHighLevel when a contract is signed,
 * extracts deal data from the contact's custom fields, and inserts a
 * new row at row 4 (top of data) of the Summit Group Deal Ledger on SharePoint.
 *
 * ZERO external dependencies — deploys as a single file.
 */

// ─── Microsoft Graph Auth ────────────────────────────────────────────────────

async function getAccessToken(env) {
  const cached = await env.GHL_KV.get("ms_access_token");
  if (cached) return cached;

  const tokenUrl = `https://login.microsoftonline.com/${env.AZURE_TENANT_ID}/oauth2/v2.0/token`;

  const body = new URLSearchParams({
    client_id: env.AZURE_CLIENT_ID,
    client_secret: env.AZURE_CLIENT_SECRET,
    scope: "https://graph.microsoft.com/.default",
    grant_type: "client_credentials",
  });

  const res = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Token request failed (${res.status}): ${text}`);
  }

  const data = await res.json();
  const ttl = Math.max((data.expires_in || 3600) - 120, 60);
  await env.GHL_KV.put("ms_access_token", data.access_token, { expirationTtl: ttl });
  return data.access_token;
}

// ─── Graph API Helpers ───────────────────────────────────────────────────────

async function graphGet(token, url) {
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Graph GET failed (${res.status}): ${text}`);
  }
  return res.json();
}

async function graphPost(token, url, body, extraHeaders) {
  const headers = {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
  if (extraHeaders) Object.assign(headers, extraHeaders);
  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Graph POST failed (${res.status}): ${text}`);
  }
  return res.json();
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
  return mm + "/" + yy;
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
  return [[
    deal.dealId, deal.propertyAddress, deal.market, deal.acqOwner,
    deal.dispositionOwner, deal.dealStatus, deal.strategy, deal.exitType,
    deal.underContractDate, deal.closeDateActualEst, deal.month, deal.contractPrice,
    deal.listedPostedPrice, deal.buyerPriceSalePrice, deal.repairs,
    deal.potentialProfit, deal.finalProfit, deal.notes,
  ]];
}

// ─── Write to SharePoint ────────────────────────────────────────────────────

async function writeToLedger(env, deal) {
  const token = await getAccessToken(env);

  const siteUrl = "https://graph.microsoft.com/v1.0/sites/" + env.SHAREPOINT_SITE_ID;
  const workbookUrl = siteUrl + "/drive/root:/" + env.LEDGER_FILE_PATH + ":/workbook";
  const worksheetUrl = workbookUrl + "/worksheets('" + (env.LEDGER_SHEET_NAME || "Sheet1") + "')";
  const tableName = env.LEDGER_TABLE_NAME || "DealLedger";
  const insertUrl = worksheetUrl + "/tables('" + tableName + "')/rows";

  let sessionId = null;
  try {
    const session = await graphPost(token, workbookUrl + "/createSession", { persistChanges: true });
    sessionId = session.id;
  } catch (err) {
    console.error("Session failed: " + err.message);
  }
  const sessionHeaders = sessionId ? { "workbook-session-id": sessionId } : {};

  // Insert the row
  const rowValues = dealToRow(deal);
  await graphPost(token, insertUrl, { index: 0, values: rowValues }, sessionHeaders);
  console.log("Row inserted: " + deal.dealId + " | " + deal.strategy);

  // Format the row — white fill, blue font size 12
  try {
    const fmtHeaders = {
      Authorization: "Bearer " + token,
      "Content-Type": "application/json",
    };
    if (sessionId) fmtHeaders["workbook-session-id"] = sessionId;
    await fetch(worksheetUrl + "/range(address='A4:R4')/format/fill", {
      method: "PATCH", headers: fmtHeaders,
      body: JSON.stringify({ color: "FFFFFF" }),
    });
    await fetch(worksheetUrl + "/range(address='A4:R4')/format/font", {
      method: "PATCH", headers: fmtHeaders,
      body: JSON.stringify({ color: "0000FF", size: 12 }),
    });
  } catch (_) {}

  // Close session
  if (sessionId) {
    try {
      await fetch(workbookUrl + "/closeSession", {
        method: "POST",
        headers: {
          Authorization: "Bearer " + token,
          "Content-Type": "application/json",
          "workbook-session-id": sessionId,
        },
        body: "{}",
      });
    } catch (_) {}
  }

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
