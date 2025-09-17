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

    // TODO: replace with your GitHub username exactly
    const owner = "<BreadPitttt>";
    const repo = "Vacancies-dashboard";

    const path =
      type === "report"
        ? "reports.jsonl"
        : type === "missing"
        ? "submissions.jsonl"
        : null;

    if (!path) {
      return { statusCode: 400, body: "Unknown type" };
    }

    const gh = "https://api.github.com";
    const headers = {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${process.env.FEEDBACK_TOKEN}`,
      "Content-Type": "application/json",
    };

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

    const line =
      JSON.stringify({ ...payload, ts: new Date().toISOString() }) + "\n";
    const updated = Buffer.from(current + line).toString("base64");

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
