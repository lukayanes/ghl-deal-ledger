/**
 * GHL Deal Ledger Worker
 *
 * Polls Outlook inbox for GoHighLevel "Document signed successfully" emails,
 * downloads the attached signed PDF contract, parses deal data from it,
 * and inserts a new row at row 4 of the Summit Group Deal Ledger on SharePoint.
 *
 * Supports three contract types:
 *   - Novation  ("CONTRACT FOR THE SALE & PURCHASE OF REAL ESTATE")
 *   - Cash      ("Standard Purchase and Sales Agreement" — no existing mortgage)
 *   - Sub-To    ("Standard Purchase and Sales Agreement" — has existing mortgage)
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
    throw new Error(`Graph POST failed (${res.status}): ${text}`);
  }
  return res.json();
}

// ─── PDF Text Extraction (no dependencies) ───────────────────────────────────

async function decompress(data) {
  for (const format of ["deflate", "raw"]) {
    try {
      const ds = new DecompressionStream(format);
      const writer = ds.writable.getWriter();
      const reader = ds.readable.getReader();
      writer.write(data);
      writer.close();
      const chunks = [];
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
      }
      let totalLen = 0;
      for (const c of chunks) totalLen += c.length;
      const result = new Uint8Array(totalLen);
      let offset = 0;
      for (const c of chunks) {
        result.set(c, offset);
        offset += c.length;
      }
      return result;
    } catch (_) {
      continue;
    }
  }
  return null;
}

function decodePdfString(s) {
  let result = "";
  let i = 0;
  while (i < s.length) {
    if (s[i] === "\\" && i + 1 < s.length) {
      i++;
      switch (s[i]) {
        case "n": result += "\n"; break;
        case "r": result += "\r"; break;
        case "t": result += "\t"; break;
        case "(": result += "("; break;
        case ")": result += ")"; break;
        case "\\": result += "\\"; break;
        default:
          if (s[i] >= "0" && s[i] <= "7") {
            let octal = s[i];
            if (i + 1 < s.length && s[i + 1] >= "0" && s[i + 1] <= "7") { octal += s[++i]; }
            if (i + 1 < s.length && s[i + 1] >= "0" && s[i + 1] <= "7") { octal += s[++i]; }
            result += String.fromCharCode(parseInt(octal, 8));
          } else {
            result += s[i];
          }
      }
    } else {
      result += s[i];
    }
    i++;
  }
  return result;
}

function extractTextFromStream(streamText) {
  const parts = [];
  let m;

  const tjRegex = /\(([^)]*(?:\\.[^)]*)*)\)\s*Tj/g;
  while ((m = tjRegex.exec(streamText)) !== null) {
    parts.push(decodePdfString(m[1]));
  }

  const tjArrayRegex = /\[([^\]]*)\]\s*TJ/gi;
  while ((m = tjArrayRegex.exec(streamText)) !== null) {
    const inner = m[1];
    const strRegex = /\(([^)]*(?:\\.[^)]*)*)\)/g;
    let s;
    while ((s = strRegex.exec(inner)) !== null) {
      parts.push(decodePdfString(s[1]));
    }
  }

  const quoteRegex = /\(([^)]*(?:\\.[^)]*)*)\)\s*'/g;
  while ((m = quoteRegex.exec(streamText)) !== null) {
    parts.push("\n" + decodePdfString(m[1]));
  }

  return parts.join("");
}

async function extractPdfText(base64Content) {
  // Only decode the first 180KB of the PDF — contract text is always in the first
  // 1-2 pages. The rest is embedded signature images that inflate the file but
  // contain no parseable text, and are what causes CPU limit errors.
  const safeBase64 = base64Content.slice(0, Math.floor(240000 / 4) * 4);
  const binaryString = atob(safeBase64);

  // Use Uint8Array.from instead of a manual loop — significantly faster
  const bytes = Uint8Array.from(binaryString, (c) => c.charCodeAt(0));
  const raw = new TextDecoder("latin1").decode(bytes);
  const allText = [];

  // Use indexOf to find stream boundaries — much faster than regex on binary data
  let pos = 0;
  while (pos < raw.length) {
    let streamMarker = raw.indexOf("stream\r\n", pos);
    let markerLen = 8;
    if (streamMarker === -1) {
      streamMarker = raw.indexOf("stream\n", pos);
      markerLen = 7;
    }
    if (streamMarker === -1) break;

    const dataStart = streamMarker + markerLen;

    let streamEnd = raw.indexOf("\r\nendstream", dataStart);
    let endLen = 12;
    if (streamEnd === -1) {
      streamEnd = raw.indexOf("\nendstream", dataStart);
      endLen = 10;
    }
    if (streamEnd === -1) break;

    pos = streamEnd + endLen;

    // Skip tiny streams (metadata) and huge ones (images)
    const streamLen = streamEnd - dataStart;
    if (streamLen < 10 || streamLen > 80000) continue;

    try {
      const streamData = raw.slice(dataStart, streamEnd);
      const streamBytes = Uint8Array.from(streamData, (c) => c.charCodeAt(0));
      const decompressed = await decompress(streamBytes);
      const streamText = decompressed
        ? new TextDecoder("latin1").decode(decompressed)
        : streamData;

      const text = extractTextFromStream(streamText);
      if (text.trim()) allText.push(text);
    } catch (_) {}
  }

  return allText.join(" ").replace(/\s+/g, " ").trim();
}

// ─── Contract Type Detection ─────────────────────────────────────────────────

function detectContractType(subject) {
  const s = subject.toLowerCase();
  if (s.includes("novation")) return "Novation";
  if (s.includes("sub to") || s.includes("sub_to") || s.includes("subject to")) return "Sub-To";
  if (s.includes("cash")) return "Cash";
  return "Unknown";
}

// ─── Contract Parsers ────────────────────────────────────────────────────────

function createEmptyDeal() {
  return {
    dealId: "", propertyAddress: "", market: "", acqOwner: "Brennen",
    dispositionOwner: "", dealStatus: "Under Contract", strategy: "", exitType: "",
    underContractDate: "", closeDateActualEst: "", month: "", contractPrice: "",
    listedPostedPrice: "", buyerPriceSalePrice: "", repairs: "", potentialProfit: "",
    finalProfit: "", notes: "", sellerName: "", earnestMoney: "",
    existingMortgage: "", balanceAtClosing: "",
  };
}

function extractMarketFromAddress(address) {
  if (!address) return "";
  const m = address.match(/,\s*([A-Z]{2})\s*\d{0,5}\s*$/);
  if (m) return m[1];
  const parts = address.split(",").map((s) => s.trim());
  if (parts.length >= 2) return parts[parts.length - 1].replace(/\d{5}/, "").trim();
  return "";
}

function parseNovationContract(text) {
  const deal = createEmptyDeal();
  deal.strategy = "Novation";

  const sellerMatch = text.match(/PARTIES:\s*(.+?)\s*\(Seller\)/i);
  if (sellerMatch) deal.sellerName = sellerMatch[1].trim();

  const propertyMatch = text.match(/SUBJECT PROPERTY:\s*(.+?)(?:\s*hereinafter|\s*$)/im);
  if (propertyMatch) deal.propertyAddress = propertyMatch[1].trim().replace(/\s+/g, " ");

  const priceMatch = text.match(/PURCHASE PRICE:\s*\$?([\d,]+(?:\.\d{2})?)/i);
  if (priceMatch) deal.contractPrice = "$" + priceMatch[1];

  const closingMatch =
    text.match(/closing will take place on or before:\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s*,?\s*\d{4})/i) ||
    text.match(/on or before:\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s*,?\s*\d{4})/i);
  if (closingMatch) deal.closeDateActualEst = closingMatch[1].trim();

  const sigDateMatch = text.match(/(\d{2})\s*\/\s*(\d{2})\s*\/\s*(\d{2,4})/);
  if (sigDateMatch) {
    const year = sigDateMatch[3].length === 2 ? "20" + sigDateMatch[3] : sigDateMatch[3];
    deal.underContractDate = sigDateMatch[1] + "/" + sigDateMatch[2] + "/" + year;
  }

  const earnestMatch = text.match(/earnest money deposit of \$\s*([\d,]+)/i);
  if (earnestMatch) deal.earnestMoney = "$" + earnestMatch[1];

  deal.market = extractMarketFromAddress(deal.propertyAddress);

  const refMatch = text.match(/Document Ref:\s*([\w-]+)/i);
  if (refMatch) deal.dealId = refMatch[1];

  return deal;
}

function parseStandardContract(text, fallbackType) {
  const deal = createEmptyDeal();

  const sellerMatch = text.match(/\(BUYER\)\s*and\s+(.+?)\s*\(SELLER\)/i);
  if (sellerMatch) deal.sellerName = sellerMatch[1].trim();

  const addressMatch =
    text.match(/Address\s+(.+?)(?:\s*Legal Description)/i) ||
    text.match(/described as follows:\s*Address\s+(.+?)(?:\s*Legal)/i);
  if (addressMatch) deal.propertyAddress = addressMatch[1].trim().replace(/\s+/g, " ");

  const countyMatch = text.match(/Property is in\s+(.+?)\s+County/i);
  if (countyMatch) deal.market = countyMatch[1].trim();

  const totalPriceMatch =
    text.match(/H\.\s*\$?([\d,]+(?:\.\d{2})?)/i) ||
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
    deal.underContractDate = offerDateMatch[1] + "/" + offerDateMatch[2] + "/" + offerDateMatch[3];
  } else {
    const sigDateMatch = text.match(/(\d{2})\s*\/\s*(\d{2})\s*\/\s*(\d{4})/);
    if (sigDateMatch) deal.underContractDate = sigDateMatch[1] + "/" + sigDateMatch[2] + "/" + sigDateMatch[3];
  }

  const stateMatch = text.match(/construed under\s+([A-Z]{2})\s+Law/i);
  if (stateMatch && !deal.market) deal.market = stateMatch[1];

  const refMatch = text.match(/Document Ref:\s*([\w-]+)/i);
  if (refMatch) deal.dealId = refMatch[1];

  return deal;
}

// ─── Row Builder ─────────────────────────────────────────────────────────────

function dealToRow(deal) {
  if (deal.underContractDate && !deal.month) {
    try {
      const d = new Date(deal.underContractDate);
      if (!isNaN(d)) deal.month = d.toLocaleString("en-US", { month: "long" });
    } catch (_) {}
  }

  deal.dealId = deal.sellerName || deal.dealId;

  const noteParts = ["Auto-added from GHL contract PDF."];
  if (deal.earnestMoney) noteParts.push("EMD: " + deal.earnestMoney);
  if (deal.existingMortgage) noteParts.push("Existing Mortgage: " + deal.existingMortgage);
  if (deal.balanceAtClosing) noteParts.push("Balance at Closing: " + deal.balanceAtClosing);
  deal.notes = noteParts.join(" | ");

  return [[
    deal.dealId, deal.propertyAddress, deal.market, deal.acqOwner,
    deal.dispositionOwner, deal.dealStatus, deal.strategy, deal.exitType,
    deal.underContractDate, deal.closeDateActualEst, deal.month, deal.contractPrice,
    deal.listedPostedPrice, deal.buyerPriceSalePrice, deal.repairs,
    deal.potentialProfit, deal.finalProfit, deal.notes,
  ]];
}

// ─── Main Logic ──────────────────────────────────────────────────────────────

async function processSigningEmails(env) {
  const token = await getAccessToken(env);
  const userEmail = env.TARGET_MAILBOX;

  let lastProcessed = await env.GHL_KV.get("last_processed_timestamp");
  if (!lastProcessed) {
    lastProcessed = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  }

  const filter = encodeURIComponent(
    "receivedDateTime ge " + lastProcessed + " and contains(subject, 'signed') and hasAttachments eq true"
  );
  const messagesUrl =
    "https://graph.microsoft.com/v1.0/users/" + userEmail + "/messages?$filter=" + filter + "&$orderby=receivedDateTime asc&$top=50&$select=id,subject,body,receivedDateTime,from,hasAttachments";

  const messages = await graphGet(token, messagesUrl);
  const emails = (messages.value || []).filter(function(e) {
    const sender = e.from?.emailAddress?.address || "";
    const body = e.body?.content || "";
    return sender.includes("msgsndr.net") && body.toLowerCase().includes("document signed successfully");
  });

  if (emails.length === 0) {
    console.log("No new GHL signing emails with attachments found.");
    return { processed: 0 };
  }

  console.log("Found " + emails.length + " signing email(s) with attachments.");

  const siteUrl = "https://graph.microsoft.com/v1.0/sites/" + env.SHAREPOINT_SITE_ID;
  const worksheetUrl = siteUrl + "/drive/root:/" + env.LEDGER_FILE_PATH + ":/workbook/worksheets('" + (env.LEDGER_SHEET_NAME || "Sheet1") + "')";

  let processedCount = 0;
  let latestTimestamp = lastProcessed;

  for (const email of emails) {
    const emailKey = "processed_email_" + email.id;
    const alreadyDone = await env.GHL_KV.get(emailKey);
    if (alreadyDone) continue;

    try {
      const contractType = detectContractType(email.subject);
      console.log("Processing: " + email.subject + " -> type: " + contractType);

      const attUrl = "https://graph.microsoft.com/v1.0/users/" + userEmail + "/messages/" + email.id + "/attachments";
      const attachments = await graphGet(token, attUrl);
      const pdfAtt = (attachments.value || []).find(function(a) {
        return (a.name || "").toLowerCase().endsWith(".pdf") && a.contentBytes;
      });

      if (!pdfAtt) {
        console.log("No PDF attachment for email " + email.id + ", skipping.");
        continue;
      }

      const pdfText = await extractPdfText(pdfAtt.contentBytes);
      console.log("Extracted " + pdfText.length + " chars from PDF.");

      let deal;
      if (contractType === "Novation" || pdfText.includes("CONTRACT FOR THE SALE & PURCHASE")) {
        deal = parseNovationContract(pdfText);
      } else {
        deal = parseStandardContract(pdfText, contractType);
      }

      const rowValues = dealToRow(deal);
      const insertUrl = worksheetUrl + "/tables('" + (env.LEDGER_TABLE_NAME || "DealLedger") + "')/rows";
      await graphPost(token, insertUrl, { index: 0, values: rowValues });

      console.log("Inserted: " + deal.propertyAddress + " (" + deal.strategy + ") - " + deal.contractPrice);

      await env.GHL_KV.put(emailKey, "done", { expirationTtl: 90 * 24 * 60 * 60 });
      processedCount++;

      if (email.receivedDateTime > latestTimestamp) {
        latestTimestamp = email.receivedDateTime;
      }
    } catch (err) {
      console.error("Error processing email " + email.id + ": " + err.message);
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
    console.log("Cron triggered at " + new Date().toISOString());
    const result = await processSigningEmails(env);
    console.log("Done. Processed " + result.processed + " of " + (result.total || 0) + " emails.");
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
      if (env.WORKER_SECRET && authHeader !== ("Bearer " + env.WORKER_SECRET)) {
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
