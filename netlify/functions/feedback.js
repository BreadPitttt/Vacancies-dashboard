"use strict";
const fetch = (...args) => import("node-fetch").then(({default: fetch}) => fetch(...args));

// Reusable CORS headers (allowing your GitHub Pages origin)
const ALLOW_ORIGIN = "https://breadpitttt.github.io"; // exact origin of your dashboard
const CORS_HEADERS = {
  "Access-Control-Allow-Origin": ALLOW_ORIGIN,
  "Access-Control-Allow-Methods": "OPTIONS, POST",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Max-Age": "86400",
  "Content-Type": "text/plain; charset=utf-8"
};

exports.handler = async (event) => {
  // Preflight
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS_HEADERS, body: "" };
  }

  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers: CORS_HEADERS, body: "Method Not Allowed" };
  }

  try {
    if (!process.env.FEEDBACK_TOKEN) {
      return { statusCode: 500, headers: CORS_HEADERS, body: "Missing FEEDBACK_TOKEN" };
    }

    const { type, payload } = JSON.parse(event.body || "{}");
    if (!type || !payload) {
      return { statusCode: 400, headers: CORS_HEADERS, body: "Bad Request" };
    }

    const filePath = (type === "missing") ? "submissions.jsonl" : "reports.jsonl";
    const record = normalizeRecord(type, payload);

    const owner = "BreadPitttt";               // exact username
    const repo  = "Vacancies-dashboard";       // repo name
    const gh    = "https://api.github.com";
    const headers = {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${process.env.FEEDBACK_TOKEN}`,
      "Content-Type": "application/json",
      "User-Agent": "netlify-fn-feedback"
    };

    // Read existing file (404 tolerated)
    const getUrl = `${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(filePath)}`;
    let sha, current = "";
    const getRes = await fetch(getUrl, { headers });
    if (getRes.status === 404) {
      sha = undefined; current = "";
    } else if (getRes.ok) {
      const fileJson = await getRes.json();
      sha = fileJson.sha;
      current = Buffer.from(fileJson.content || "", fileJson.encoding || "base64").toString("utf-8");
    } else {
      const txt = await getRes.text().catch(()=> "");
      return { statusCode: getRes.status, headers: CORS_HEADERS, body: `Read failed: ${txt}` };
    }

    // Append one JSONL line
    const line = JSON.stringify({ ...record, ts: new Date().toISOString() }) + "\n";
    const updated = Buffer.from(current + line).toString("base64");

    // Write back
    const putUrl = `${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(filePath)}`;
    const body = { message: `feedback: append to ${filePath}`, content: updated };
    if (sha) body.sha = sha;

    const putRes = await fetch(putUrl, { method: "PUT", headers, body: JSON.stringify(body) });
    if (!putRes.ok) {
      const t = await putRes.text().catch(()=> "");
      return { statusCode: putRes.status, headers: CORS_HEADERS, body: `Update failed: ${t}` };
    }

    return { statusCode: 200, headers: CORS_HEADERS, body: "OK" };
  } catch (e) {
    return { statusCode: 500, headers: CORS_HEADERS, body: `Error: ${e.message}` };
  }
};

function normalizeRecord(type, payload){
  if (payload && (payload.flag || payload.jobId)) {
    return {
      type: "report",
      flag: payload.flag || "not general vacancy",
      jobId: payload.jobId || "",
      title: payload.title || "",
      url: payload.url || "",
      note: payload.note || ""
    };
  }
  if (type === "report") {
    return {
      type: "report",
      flag: "not general vacancy",
      jobId: payload.listingId || "",
      title: payload.reason || "",
      url: payload.evidenceUrl || "",
      note: payload.note || ""
    };
  }
  return {
    type: "missing",
    title: payload.title || "",
    url: payload.url || "",
    note: payload.note || ""
  };
}
