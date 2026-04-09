/**
 * GHL Deal Ledger Worker
 *
 * Polls Outlook inbox for GoHighLevel "Document signed successfully" emails,
 * downloads the attached signed PDF contract, parses deal data from it,
 * and appends a new row to the Summit Group Deal Ledger on SharePoint.
 *
 * Supports three contract types:
 *   - Novation  ("CONTRACT FOR THE SALE & PURCHASE OF REAL ESTATE")
 *   - Cash      ("Standard Purchase and Sales Agreement" — no existing mortgage)
 *   - Sub-To    ("Standard Purchase and Sales Agreement" — has existing mortgage)
 *
 * Runs on a cron schedule (every 5 minutes by default).
 */

import { getDocument, GlobalWorkerOptions } from "pdfjs-dist/legacy/build/pdf.mjs";

// Disable worker thread (not available in CF Workers)
GlobalWorkerOptions.workerSrc = "";

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
    throw new Error(`Graph GET ${url} failed (${res.status}): ${text}`);
  }
  return res.json();
}

async function graphPost(token, url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Graph POST ${url} failed (${res.status}): ${text}`);
  }
  return res.json();
}

// ─── PDF Text Extraction ─────────────────────────────────────────────────────

/**
 * Extracts all text from a PDF given as a base64-encoded string.
 * Returns a single string with all pages concatenated.
 */
async function extractPdfText(base64Content) {
  const binaryString = atob(base64Content);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }

  const pdf = await getDocument({ data: bytes, disableFontFace: true, useSystemFonts: false }).promise;
  const pages = [];

  for (let i = 1; i <= pdf.numPages; i++) {
    const page = await pdf.getPage(i);
    const content = await page.getTextContent();
    const pageText = content.items.map((item) => item.str).join(" ");
    pages.push(pageText);
  }

  return pages.join("\n\n");
}

// ─── Contract Type Detection ─────────────────────────────────────────────────

/**
 * Determines the contract type from the email subject line.
 *
 * GHL subjects follow the pattern:
 *   [Summit Group Acquisitions LLC] Purchase and Sale_<TYPE> signed
 *
 * Examples:
 *   "Purchase and Sale_Novation High Level Additional Terms signed"  → Novation
 *   "Purchase and Sale_Cash High Level 2 signed"                     → Cash
 *   "Purchase and Sale_High Level sub to signed"                     → Sub-To
 */
function detectContractType(subject) {
  const subjectLower = subject.toLowerCase();

  if (subjectLower.includes("novation")) return "Novation";
  if (subjectLower.includes("sub to") || subjectLower.includes("sub_to") || subjectLower.includes("subject to")) return "Sub-To";
  if (subjectLower.includes("cash")) return "Cash";

  // Fallback: try to detect from PDF content later
  return "Unknown";
}

// ─── Contract Parsers ────────────────────────────────────────────────────────

/**
 * Parses a Novation-style contract.
 * Template: "CONTRACT FOR THE SALE & PURCHASE OF REAL ESTATE"
 */
function parseNovationContract(text) {
  const deal = createEmptyDeal();
  deal.strategy = "Novation";

  const sellerMatch = text.match(/PARTIES:\s*(.+?)\s*\(Seller\)/i);
  if (sellerMatch) deal.sellerName = sellerMatch[1].trim();

  const propertyMatch = text.match(/SUBJECT PROPERTY:\s*(.+?)(?:\s*hereinafter|\s*$)/im);
  if (propertyMatch) {
    deal.propertyAddress = propertyMatch[1].trim().replace(/\s+/g, " ");
  }

  const priceMatch = text.match(/PURCHASE PRICE:\s*\$?([\d,]+(?:\.\d{2})?)/i);
  if (priceMatch) deal.contractPrice = "$" + priceMatch[1];

  const closingMatch = text.match(/closing will take place on or before:\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s*,?\s*\d{4})/i) ||
                        text.match(/on or before:\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s*,?\s*\d{4})/i);
  if (closingMatch) deal.closeDateActualEst = closingMatch[1].trim();

  const sigDateMatch = text.match(/(\d{2})\s*\/\s*(\d{2})\s*\/\s*(\d{2,4})/);
  if (sigDateMatch) {
    const month = sigDateMatch[1];
    const day = sigDateMatch[2];
    const year = sigDateMatch[3].length === 2 ? "20" + sigDateMatch[3] : sigDateMatch[3];
    deal.underContractDate = `${month}/${day}/${year}`;
  }

  const earnestMatch = text.match(/earnest money deposit of \$\s*([\d,]+)/i);
  if (earnestMatch) deal.earnestMoney = "$" + earnestMatch[1];

  deal.market = extractMarketFromAddress(deal.propertyAddress);

  const refMatch = text.match(/Document Ref:\s*([\w-]+)/i);
  if (refMatch) deal.dealId = refMatch[1];

  return deal;
}

/**
 * Parses a Standard Purchase and Sales Agreement (Cash or Sub-To).
 */
function parseStandardContract(text, fallbackType) {
  const deal = createEmptyDeal();

  const sellerMatch = text.match(/\(BUYER\)\s*and\s+(.+?)\s*\(SELLER\)/i);
  if (sellerMatch) deal.sellerName = sellerMatch[1].trim();

  const addressMatch = text.match(/Address\s+(.+?)(?:\s*Legal Description)/i) ||
                        text.match(/described as follows:\s*Address\s+(.+?)(?:\s*Legal)/i);
  if (addressMatch) {
    deal.propertyAddress = addressMatch[1].trim().replace(/\s+/g, " ");
  }

  const countyMatch = text.match(/Property is in\s+(.+?)\s+County/i);
  if (countyMatch) deal.market = countyMatch[1].trim();

  const totalPriceMatch = text.match(/H\.\s*\$?([\d,]+(?:\.\d{2})?)/i) ||
                           text.match(/Total Purchase Price.+?\$\s*([\d,]+(?:\.\d{2})?)/i);
  if (totalPriceMatch) deal.contractPrice = "$" + totalPriceMatch[1];

  const mortgageMatch = text.match(/G\.\s*\$?([\d,]+(?:\.\d{2})?)/i);
  const existingMortgage = mortgageMatch ? parseFloat(mortgageMatch[1].replace(/,/g, "")) : 0;

  if (existingMortgage > 0) {
    deal.strategy = "Sub-To";
    deal.existingMortgage = "$" + mortgageMatch[1];
  } else {
    deal.strategy = fallbackType === "Sub-To" ? "Sub-To" : "Cash";
  }

  const balanceMatch = text.match(/C\.\s*\$?([\d,]+(?:\.\d{2})?)/i);
  if (balanceMatch) deal.balanceAtClosing = "$" + balanceMatch[1];

  const binderMatch = text.match(/A\.\s*\$?([\d,]+(?:\.\d{2})?)/i);
  if (binderMatch) deal.earnestMoney = "$" + binderMatch[1];

  const closingMatch = text.match(/on or before\s+([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s*,?\s*\d{4})/i);
  if (closingMatch) deal.closeDateActualEst = closingMatch[1].trim();

  const offerDateMatch = text.match(/Date of Offer\s+(\d{2})\s*\/\s*(\d{2})\s*\/\s*(\d{4})/i);
  if (offerDateMatch) {
    deal.underContractDate = `${offerDateMatch[1]}/${offerDateMatch[2]}/${offerDateMatch[3]}`;
  } else {
    const sigDateMatch = text.match(/(\d{2})\s*\/\s*(\d{2})\s*\/\s*(\d{4})/);
    if (sigDateMatch) {
      deal.underContractDate = `${sigDateMatch[1]}/${sigDateMatch[2]}/${sigDateMatch[3]}`;
    }
  }

  const stateMatch = text.match(/construed under\s+([A-Z]{2})\s+Law/i);
  if (stateMatch && !deal.market) {
    deal.market = stateMatch[1];
  }

  const refMatch = text.match(/Document Ref:\s*([\w-]+)/i);
  if (refMatch) deal.dealId = refMatch[1];

  const termsMatch = text.match(/18\.\s*Additional Terms.+?(?:notice or consent)\s*(.*?)(?:\s*19\.|$)/is);
  if (termsMatch) deal.additionalTerms = termsMatch[1].trim().substring(0, 200);

  return deal;
}

// ─── Shared Helpers ──────────────────────────────────────────────────────────

function createEmptyDeal() {
  return {
    dealId: "",
    propertyAddress: "",
    market: "",
    acqOwner: "Brennen",
    dispositionOwner: "",
    dealStatus: "Under Contract",
    strategy: "",
    exitType: "",
    underContractDate: "",
    closeDateActualEst: "",
    month: "",
    contractPrice: "",
    listedPostedPrice: "",
    buyerPriceSalePrice: "",
    repairs: "",
    potentialProfit: "",
    finalProfit: "",
    notes: "",
    // Extra fields for notes (not separate ledger columns)
    sellerName: "",
    earnestMoney: "",
    existingMortgage: "",
    balanceAtClosing: "",
    additionalTerms: "",
  };
}

function extractMarketFromAddress(address) {
  if (!address) return "";
  const stateMatch = address.match(/,\s*([A-Z]{2})\s*\d{0,5}\s*$/);
  if (stateMatch) return stateMatch[1];
  const parts = address.split(",").map((s) => s.trim());
  if (parts.length >= 2) return parts[parts.length - 1].replace(/\d{5}/, "").trim();
  return "";
}

function dealToRow(deal) {
  // Compute month from under contract date
  if (deal.underContractDate && !deal.month) {
    try {
      const d = new Date(deal.underContractDate);
      if (!isNaN(d)) deal.month = d.toLocaleString("en-US", { month: "long" });
    } catch (_) {}
  }

  // Deal ID = seller name (from the contract)
  deal.dealId = deal.sellerName || deal.dealId;

  // Build notes
  const noteParts = [`Auto-added from GHL contract PDF.`];
  if (deal.earnestMoney) noteParts.push(`EMD: ${deal.earnestMoney}`);
  if (deal.existingMortgage) noteParts.push(`Existing Mortgage: ${deal.existingMortgage}`);
  if (deal.balanceAtClosing) noteParts.push(`Balance at Closing: ${deal.balanceAtClosing}`);
  deal.notes = noteParts.join(" | ");

  return [[
    deal.dealId,
    deal.propertyAddress,
    deal.market,
    deal.acqOwner,
    deal.dispositionOwner,
    deal.dealStatus,
    deal.strategy,
    deal.exitType,
    deal.underContractDate,
    deal.closeDateActualEst,
    deal.month,
    deal.contractPrice,
    deal.listedPostedPrice,
    deal.buyerPriceSalePrice,
    deal.repairs,
    deal.potentialProfit,
    deal.finalProfit,
    deal.notes,
  ]];
}

// ─── Main Logic ──────────────────────────────────────────────────────────────

async function processSigningEmails(env) {
  const token = await getAccessToken(env);
  const userEmail = env.TARGET_MAILBOX;

  let lastProcessed = await env.GHL_KV.get("last_processed_timestamp");
  if (!lastProcessed) {
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    lastProcessed = yesterday;
  }

  const filter = encodeURIComponent(
    `receivedDateTime ge ${lastProcessed} and contains(subject, 'signed') and hasAttachments eq true`
  );
  const messagesUrl =
    `https://graph.microsoft.com/v1.0/users/${userEmail}/messages?$filter=${filter}&$orderby=receivedDateTime asc&$top=50&$select=id,subject,body,receivedDateTime,from,hasAttachments`;

  const messages = await graphGet(token, messagesUrl);
  const emails = (messages.value || []).filter((e) => {
    const sender = e.from?.emailAddress?.address || "";
    const bodyPreview = e.body?.content || "";
    return sender.includes("msgsndr.net") && bodyPreview.toLowerCase().includes("document signed successfully");
  });

  if (emails.length === 0) {
    console.log("No new GHL signing emails with attachments found.");
    return { processed: 0 };
  }

  console.log(`Found ${emails.length} signing email(s) with attachments.`);

  const siteUrl = `https://graph.microsoft.com/v1.0/sites/${env.SHAREPOINT_SITE_ID}`;
  const worksheetUrl = `${siteUrl}/drive/root:/${env.LEDGER_FILE_PATH}:/workbook/worksheets('${env.LEDGER_SHEET_NAME || "Sheet1"}')`;

  let processedCount = 0;
  let latestTimestamp = lastProcessed;

  for (const email of emails) {
    const emailKey = `processed_email_${email.id}`;
    const alreadyDone = await env.GHL_KV.get(emailKey);
    if (alreadyDone) continue;

    try {
      // Step 1: Detect contract type from subject line
      const contractType = detectContractType(email.subject);
      console.log(`Processing: "${email.subject}" → type: ${contractType}`);

      // Step 2: Download the PDF attachment via Graph API
      const attachmentsUrl =
        `https://graph.microsoft.com/v1.0/users/${userEmail}/messages/${email.id}/attachments?$filter=contentType eq 'application/pdf'&$select=id,name,contentBytes,contentType`;
      const attachments = await graphGet(token, attachmentsUrl);
      const pdfAttachments = (attachments.value || []).filter(
        (a) => a.contentType === "application/pdf" || a.name?.toLowerCase().endsWith(".pdf")
      );

      if (pdfAttachments.length === 0) {
        const allAttUrl = `https://graph.microsoft.com/v1.0/users/${userEmail}/messages/${email.id}/attachments`;
        const allAtt = await graphGet(token, allAttUrl);
        const pdfFallback = (allAtt.value || []).filter(
          (a) => a.name?.toLowerCase().endsWith(".pdf") && a.contentBytes
        );
        if (pdfFallback.length === 0) {
          console.log(`No PDF attachment found for email ${email.id}, skipping.`);
          continue;
        }
        pdfAttachments.push(pdfFallback[0]);
      }

      const pdfBase64 = pdfAttachments[0].contentBytes;
      if (!pdfBase64) {
        console.log(`PDF attachment has no content for email ${email.id}, skipping.`);
        continue;
      }

      // Step 3: Extract text from the PDF
      const pdfText = await extractPdfText(pdfBase64);
      console.log(`Extracted ${pdfText.length} chars from PDF.`);

      // Step 4: Parse the contract based on type
      let deal;
      if (contractType === "Novation" || pdfText.includes("CONTRACT FOR THE SALE & PURCHASE")) {
        deal = parseNovationContract(pdfText);
      } else {
        deal = parseStandardContract(pdfText, contractType);
      }

      // Step 5: Convert to ledger row and insert at row 4 (top of data)
      const rowValues = dealToRow(deal);

      // index: 0 inserts at the first data row of the table (row 4 in the sheet,
      // assuming header is row 3). This keeps the newest deals at the top.
      const insertUrl = `${worksheetUrl}/tables('${env.LEDGER_TABLE_NAME || "DealLedger"}')/rows`;
      await graphPost(token, insertUrl, { index: 0, values: rowValues });

      console.log(`✓ Inserted at row 4: ${deal.propertyAddress} (${deal.strategy}) — ${deal.contractPrice}`);

      // Mark as processed
      await env.GHL_KV.put(emailKey, "done", { expirationTtl: 90 * 24 * 60 * 60 });
      processedCount++;

      if (email.receivedDateTime > latestTimestamp) {
        latestTimestamp = email.receivedDateTime;
      }
    } catch (err) {
      console.error(`Error processing email ${email.id}: ${err.message}`);
    }
  }

  if (latestTimestamp !== lastProcessed) {
    await env.GHL_KV.put("last_processed_timestamp", latestTimestamp);
  }

  return { processed: processedCount, total: emails.length };
}

// ─── Worker Entry Points ─────────────────────────────────────────────────────

export default {
  async scheduled(event, env, ctx) {
    console.log(`Cron triggered at ${new Date().toISOString()}`);
    const result = await processSigningEmails(env);
    console.log(`Done. Processed ${result.processed} of ${result.total || 0} emails.`);
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return new Response(JSON.stringify({ status: "ok", time: new Date().toISOString() }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    if (url.pathname === "/run") {
      const authHeader = request.headers.get("Authorization");
      if (env.WORKER_SECRET && authHeader !== `Bearer ${env.WORKER_SECRET}`) {
        return new Response("Unauthorized", { status: 401 });
      }
      try {
        const result = await processSigningEmails(env);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    return new Response("GHL Deal Ledger Worker. Use /health or /run.", { status: 200 });
  },
};
