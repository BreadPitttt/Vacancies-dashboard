"use strict";
const fetch = (...args) => import("node-fetch").then(({default: fetch}) => fetch(...args));

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, body: "Method Not Allowed" };
  }
  try {
    // Guard: env must exist
    if (!process.env.FEEDBACK_TOKEN) {
      return { statusCode: 500, body: "Missing FEEDBACK_TOKEN" };
    }

    const { type, payload } = JSON.parse(event.body || "{}");
    if (!type || !payload) {
      return { statusCode: 400, body: "Bad Request" };
    }

    // Map to file
    const filePath = (type === "missing") ? "submissions.jsonl" : "reports.jsonl";

    // Normalize record shapes
    const record = normalizeRecord(type, payload);

    // GitHub config
    const owner = "BreadPitttt";               // exact username
    const repo  = "Vacancies-dashboard";       // repository
    const gh    = "https://api.github.com";
    const headers = {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${process.env.FEEDBACK_TOKEN}`,
      "Content-Type": "application/json",
      "User-Agent": "netlify-fn-feedback"
    };

    // Read file if exists, otherwise start fresh (404 tolerant)
    const getUrl = `${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(filePath)}`;
    let sha = undefined, current = "";
    let getRes = await fetch(getUrl, { headers });
    if (getRes.status === 404) {
      sha = undefined; current = "";
    } else if (getRes.ok) {
      const fileJson = await getRes.json();
      sha = fileJson.sha;
      current = Buffer.from(fileJson.content || "", fileJson.encoding || "base64").toString("utf-8");
    } else {
      const txt = await getRes.text().catch(()=> "");
      return { statusCode: getRes.status, body: `Read failed: ${txt}` };
    }

    // Append one JSONL line with server timestamp
    const line = JSON.stringify({ ...record, ts: new Date().toISOString() }) + "\n";
    const updated = Buffer.from(current + line).toString("base64");

    // Write back
    const putUrl = `${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(filePath)}`;
    const putBody = { message: `feedback: append to ${filePath}`, content: updated };
    if (sha) putBody.sha = sha;

    const putRes = await fetch(putUrl, { method: "PUT", headers, body: JSON.stringify(putBody) });
    if (!putRes.ok) {
      const t = await putRes.text().catch(()=> "");
      return { statusCode: putRes.status, body: `Update failed: ${t}` };
    }
    return { statusCode: 200, body: "OK" };
  } catch (e) {
    return { statusCode: 500, body: `Error: ${e.message}` };
  }
};

function normalizeRecord(type, payload){
  // On-card vote: {jobId,title,url,flag:'right'|'not general vacancy'}
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
  // Old report modal: {listingId, reason, evidenceUrl, note}
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
  // Missing modal: {title,url,note}
  return {
    type: "missing",
    title: payload.title || "",
    url: payload.url || "",
    note: payload.note || ""
  };
}
