/**
 * GHL Deal Ledger Worker — Webhook Edition
 *
 * Receives a webhook POST from GoHighLevel when a contract is signed,
 * extracts deal data from the contact's custom fields, and inserts a
 * new row at row 4 (top of data) of the Summit Group Deal Ledger on SharePoint.
 *
 * Supports four deal types:
 *   - Novation
 *   - Cash
 *   - Sub-To
 *   - Seller Finance
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
  // 1. Check explicit field if provided
  const explicit = (payload.deal_type || payload.dealType || "").toLowerCase();
  if (explicit.includes("novation")) return "Novation";
  if (explicit.includes("cash")) return "Cash";
  if (explicit.includes("sub to") || explicit.includes("sub_to") || explicit.includes("subject to")) return "Sub-To";
  if (explicit.includes("seller finance") || explicit.includes("seller_finance")) return "Seller Finance";

  // 2. Check document/workflow name if provided
  const docName = (payload.document_name || payload.workflow_name || payload.name || "").toLowerCase();
  if (docName.includes("novation")) return "Novation";
  if (docName.includes("cash")) return "Cash";
  if (docName.includes("sub to") || docName.includes("sub_to") || docName.includes("subject to")) return "Sub-To";
  if (docName.includes("seller finance") || docName.includes("seller_finance")) return "Seller Finance";

  // 3. Detect from which custom fields are filled
  if (payload.purchase_price_novation || payload.closing_date_novation || payload.emd_novation) {
    return "Novation";
  }
  if (payload.purchase_price_cash || payload.closing_date_cash || payload.county_cash) {
    return "Cash";
  }
  if (payload.seller_finance_terms) {
    return "Seller Finance";
  }
  if (payload.existing_mortgage_balance || payload.monthly_mortgage_payment) {
    return "Sub-To";
  }

  return "Unknown";
}

// ─── Extract Deal from Webhook Payload ──────────────────────────────────────

function extractDeal(payload) {
  const dealType = detectDealType(payload);

  // Seller name from standard contact fields
  const firstName = payload.first_name || payload.firstName || payload.contact_first_name || "";
  const lastName = payload.last_name || payload.lastName || payload.contact_last_name || "";
  const sellerName = (firstName + " " + lastName).trim();

  // Property address
  const fullAddress = payload.full_address_1 || "";
  const street = payload.address1 || payload.street_address || "";
  const city = payload.city || "";
  const state = payload.state || "";
  const zip = payload.postal_code || payload.zip || "";
  const propertyAddress = fullAddress || [street, city, state, zip].filter(Boolean).join(", ");

  // Market (county or state)
  const market = payload.county_cash || payload.county || state || "";

  // Deal-type-specific fields
  let contractPrice = "";
  let closingDate = "";
  let underContractDate = "";
  let earnestMoney = "";
  let existingMortgage = "";
  let balanceAtClosing = "";
  let notes = ["Auto-added from GHL webhook."];

  if (dealType === "Novation") {
    contractPrice = formatMoney(payload.purchase_price_novation);
    closingDate = payload.closing_date_novation || "";
    underContractDate = payload.date_completed_by_novation || "";
    earnestMoney = payload.emd_novation || "";
    if (payload.additional_terms) notes.push("Terms: " + payload.additional_terms);
  } else if (dealType === "Cash") {
    contractPrice = formatMoney(payload.purchase_price_cash);
    closingDate = payload.closing_date_cash || "";
    underContractDate = payload.date_completed_by_cash || "";
    balanceAtClosing = payload.amt_due_at_closing_cash || "";
    if (payload.due_diligence_cash) notes.push("Due Diligence: " + payload.due_diligence_cash + " days");
  } else if (dealType === "Sub-To") {
    contractPrice = formatMoney(payload.total_purchase_price);
    existingMortgage = formatMoney(payload.existing_mortgage_balance);
    if (payload.monthly_mortgage_payment) notes.push("Monthly Payment: " + formatMoney(payload.monthly_mortgage_payment));
    if (payload.years_remaining_on_mortgage) notes.push("Years Remaining: " + payload.years_remaining_on_mortgage);
    if (payload.months_remaining_on_mortgage) notes.push("Months Remaining: " + payload.months_remaining_on_mortgage);
    if (payload.deposit) earnestMoney = formatMoney(payload.deposit);
  } else if (dealType === "Seller Finance") {
    contractPrice = formatMoney(payload.total_purchase_price);
    existingMortgage = formatMoney(payload.existing_mortgage_balance);
    if (payload.seller_finance_terms) notes.push("SF Terms: " + payload.seller_finance_terms);
    if (payload.monthly_mortgage_payment) notes.push("Monthly Payment: " + formatMoney(payload.monthly_mortgage_payment));
    if (payload.down_payment) notes.push("Down Payment: " + formatMoney(payload.down_payment));
    if (payload.deposit) earnestMoney = formatMoney(payload.deposit);
  }

  // Amendment overrides
  if (payload.amendment_purchase_price) {
    contractPrice = formatMoney(payload.amendment_purchase_price);
    notes.push("Amendment applied");
  }
  if (payload.amendment_closing_date) {
    closingDate = payload.amendment_closing_date;
  }
  if (payload.amendment__other_notes) {
    notes.push("Amendment Notes: " + payload.amendment__other_notes);
  }

  // EMD formatting
  if (earnestMoney) notes.push("EMD: " + earnestMoney);
  if (existingMortgage) notes.push("Existing Mortgage: " + existingMortgage);
  if (balanceAtClosing) notes.push("Balance at Closing: " + balanceAtClosing);

  // Derive month from under-contract date
  let month = "";
  if (underContractDate) {
    try {
      const d = new Date(underContractDate);
      if (!isNaN(d)) month = d.toLocaleString("en-US", { month: "long" });
    } catch (_) {}
  }

  return {
    dealId: sellerName || "Unknown",
    propertyAddress,
    market,
    acqOwner: "Brennen",
    dispositionOwner: "",
    dealStatus: "Under Contract",
    strategy: dealType,
    exitType: "",
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

function formatMoney(val) {
  if (!val) return "";
  const s = String(val).replace(/[^0-9.]/g, "");
  if (!s) return "";
  const num = parseFloat(s);
  if (isNaN(num)) return "";
  return "$" + num.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 2 });
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

  // Create a workbook session to handle stale editing locks
  let sessionId = null;
  try {
    const session = await graphPost(token, workbookUrl + "/createSession", { persistChanges: true });
    sessionId = session.id;
    console.log("Workbook session created: " + sessionId);
  } catch (err) {
    console.error("Failed to create workbook session: " + err.message);
  }
  const sessionHeaders = sessionId ? { "workbook-session-id": sessionId } : {};

  const rowValues = dealToRow(deal);
  await graphPost(token, insertUrl, { index: 0, values: rowValues }, sessionHeaders);

  // Close the workbook session
  if (sessionId) {
    try {
      await fetch(workbookUrl + "/closeSession", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
          "workbook-session-id": sessionId,
        },
        body: "{}",
      });
      console.log("Workbook session closed.");
    } catch (_) {}
  }

  return { success: true, deal: deal.dealId, address: deal.propertyAddress, strategy: deal.strategy };
}

// ─── Flatten nested GHL payload ─────────────────────────────────────────────

function flattenPayload(raw) {
  // GHL webhooks sometimes nest contact data under a "contact" or "customData" key.
  // This flattener pulls everything to the top level so field access is simple.
  const flat = {};

  function merge(obj) {
    if (!obj || typeof obj !== "object") return;
    for (const [key, val] of Object.entries(obj)) {
      if (val && typeof val === "object" && !Array.isArray(val) && key !== "customData") {
        // Don't overwrite top-level keys with nested objects — recurse into them
        merge(val);
      } else {
        flat[key] = val;
      }
    }
  }

  merge(raw);

  // Also handle GHL's customData array format: [{ id, value, field_key }]
  if (Array.isArray(raw.customData)) {
    for (const item of raw.customData) {
      if (item.field_key && item.value !== undefined) {
        flat[item.field_key] = item.value;
      }
    }
  }
  if (raw.contact && Array.isArray(raw.contact.customData)) {
    for (const item of raw.contact.customData) {
      if (item.field_key && item.value !== undefined) {
        flat[item.field_key] = item.value;
      }
    }
  }

  return flat;
}

// ─── Worker Entry Points ─────────────────────────────────────────────────────

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({ status: "ok", time: new Date().toISOString() }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // Webhook endpoint — receives POST from GHL
    if (url.pathname === "/webhook" && request.method === "POST") {
      try {
        const rawPayload = await request.json();
        console.log("Webhook received. Keys: " + Object.keys(rawPayload).join(", "));

        // Deduplicate — skip if we already processed this event
        const eventId = rawPayload.id || rawPayload.event_id || rawPayload.contactId || rawPayload.contact_id || "";
        if (eventId) {
          const dedupeKey = "webhook_" + eventId;
          const already = await env.GHL_KV.get(dedupeKey);
          if (already) {
            console.log("Duplicate webhook, skipping: " + eventId);
            return new Response(JSON.stringify({ status: "duplicate", eventId }), {
              headers: { "Content-Type": "application/json" },
            });
          }
        }

        const payload = flattenPayload(rawPayload);
        console.log("Flattened keys: " + Object.keys(payload).join(", "));

        const deal = extractDeal(payload);
        console.log("Deal: " + deal.dealId + " | " + deal.propertyAddress + " | " + deal.strategy + " | " + deal.contractPrice);

        const result = await writeToLedger(env, deal);

        // Mark as processed
        if (eventId) {
          await env.GHL_KV.put("webhook_" + eventId, "done", { expirationTtl: 90 * 24 * 60 * 60 });
        }

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

    // Debug/test endpoint — send a test payload via POST to /test
    if (url.pathname === "/test" && request.method === "POST") {
      try {
        const rawPayload = await request.json();
        const payload = flattenPayload(rawPayload);
        const deal = extractDeal(payload);
        // Don't write to SharePoint — just return what would be inserted
        return new Response(JSON.stringify({
          detectedType: deal.strategy,
          deal,
          row: dealToRow(deal),
          flattenedKeys: Object.keys(payload),
        }, null, 2), {
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
