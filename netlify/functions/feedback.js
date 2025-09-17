"use strict";

/*
  Netlify Function: feedback.js
  - Uses global fetch (Node 18+) — no node-fetch import required.
  - CORS enabled for GitHub Pages origin.
  - Accepts:
      type: 'report' with payload { listingId, reason, evidenceUrl, note }  // modal
      type: 'missing' with payload { title, url, note }                     // modal
      type: 'report' with payload { jobId, title, url, flag }               // on-card vote
  - Appends JSONL lines to reports.jsonl or submissions.jsonl in the repo.
*/

const ALLOW_ORIGIN = "https://breadpitttt.github.io"; // dashboard origin (no trailing slash)
const CORS_HEADERS = {
  "Access-Control-Allow-Origin": ALLOW_ORIGIN,
  "Access-Control-Allow-Methods": "OPTIONS, POST",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Max-Age": "86400",
  "Content-Type": "text/plain; charset=utf-8"
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS_HEADERS, body: "" };
  }
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers: CORS_HEADERS, body: "Method Not Allowed" };
  }

  try {
    const token = process.env.FEEDBACK_TOKEN;
    if (!token) {
      return { statusCode: 500, headers: CORS_HEADERS, body: "Missing FEEDBACK_TOKEN" };
    }

    const { type, payload } = JSON.parse(event.body || "{}");
    if (!type || !payload) {
      return { statusCode: 400, headers: CORS_HEADERS, body: "Bad Request" };
    }

    const filePath = (type === "missing") ? "submissions.jsonl" : "reports.jsonl";
    const record = normalizeRecord(type, payload);

    // GitHub repo details
    const owner = "BreadPitttt";            // exact username
    const repo  = "Vacancies-dashboard";    // repository name

    const ghBase = "https://api.github.com";
    const ghHeaders = {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "User-Agent": "netlify-fn-feedback"
    };

    // Fetch existing file (404 tolerated on first write)
    const getUrl = `${ghBase}/repos/${owner}/${repo}/contents/${encodeURIComponent(filePath)}`;
    let sha, current = "";
    const getRes = await fetch(getUrl, { headers: ghHeaders });
    if (getRes.status === 404) {
      sha = undefined;
      current = "";
    } else if (getRes.ok) {
      const fileJson = await getRes.json();
      sha = fileJson.sha;
      current = Buffer.from(fileJson.content || "", fileJson.encoding || "base64").toString("utf-8");
    } else {
      const txt = await getRes.text().catch(()=> "");
      return { statusCode: getRes.status, headers: CORS_HEADERS, body: `Read failed: ${txt}` };
    }

    // Append JSONL
    const line = JSON.stringify({ ...record, ts: new Date().toISOString() }) + "\n";
    const updated = Buffer.from(current + line).toString("base64");

    // Write file back
    const putUrl = `${ghBase}/repos/${owner}/${repo}/contents/${encodeURIComponent(filePath)}`;
    const body = { message: `feedback: append to ${filePath}`, content: updated };
    if (sha) body.sha = sha;

    const putRes = await fetch(putUrl, { method: "PUT", headers: ghHeaders, body: JSON.stringify(body) });
    if (!putRes.ok) {
      const t = await putRes.text().catch(()=> "");
      return { statusCode: 500, headers: CORS_HEADERS, body: `Update failed: ${t}` };
    }

    return { statusCode: 200, headers: CORS_HEADERS, body: "OK" };
  } catch (e) {
    return { statusCode: 500, headers: CORS_HEADERS, body: `Error: ${e.message}` };
  }
};

function normalizeRecord(type, payload){
  // On-card vote form
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
  // Report modal
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
  // Missing modal
  return {
    type: "missing",
    title: payload.title || "",
    url: payload.url || "",
    note: payload.note || ""
  };
}
