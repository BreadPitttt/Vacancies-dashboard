"use strict";
const fetch = (...args) => import("node-fetch").then(({default: fetch}) => fetch(...args));

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, body: "Method Not Allowed" };
  }
  try {
    const { type, payload } = JSON.parse(event.body || "{}");
    if (!type || !payload) {
      return { statusCode: 400, body: "Bad Request" };
    }

    // Map friendly alias: 'vote' uses reports.jsonl
    const path =
      type === "missing"
        ? "submissions.jsonl"
        : "reports.jsonl";

    // Prepare record (normalize both forms and on‑card votes)
    const record = normalizeRecord(type, payload);

    // Configure your repo
    const owner = "<BreadPitttt>"; // keep as in your working setup
    const repo = "Vacancies-dashboard";
    const gh = "https://api.github.com";
    const headers = {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${process.env.FEEDBACK_TOKEN}`,
      "Content-Type": "application/json",
    };

    // Read current file
    const getRes = await fetch(
      `${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`,
      { headers }
    );
    if (!getRes.ok) {
      return { statusCode: getRes.status, body: `Failed to read ${path}` };
    }
    const fileJson = await getRes.json();
    const sha = fileJson.sha;
    const current = Buffer.from(
      fileJson.content || "",
      fileJson.encoding || "base64"
    ).toString("utf-8");

    // Append line
    const line = JSON.stringify({ ...record, ts: new Date().toISOString() }) + "\n";
    const updated = Buffer.from(current + line).toString("base64");

    // Write back
    const putRes = await fetch(
      `${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`,
      {
        method: "PUT",
        headers,
        body: JSON.stringify({
          message: `feedback: append to ${path}`,
          content: updated,
          sha,
        }),
      }
    );
    if (!putRes.ok) {
      const t = await putRes.text();
      return { statusCode: putRes.status, body: `Update failed: ${t}` };
    }
    return { statusCode: 200, body: "OK" };
  } catch (e) {
    return { statusCode: 500, body: `Error: ${e.message}` };
  }
};

function normalizeRecord(type, payload){
  // On‑card vote shape: {jobId, title, url, flag:'right'|'not general vacancy'}
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
  // Old report form: {listingId, reason, evidenceUrl, note}
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
  // Missing form: {title, url, note}
  return {
    type: "missing",
    title: payload.title || "",
    url: payload.url || "",
    note: payload.note || ""
  };
}
