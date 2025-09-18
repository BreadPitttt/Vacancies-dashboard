// functions/v1/submit.js — Consolidated, robust function
const ALLOW_ORIGIN = "https://breadpitttt.github.io";

// Handles the browser's preflight OPTIONS request
export const onRequestOptions = () => {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": ALLOW_ORIGIN,
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Max-Age": "86400",
    },
  });
};

// Handles the actual POST request
export const onRequestPost = async ({ request, env }) => {
  const origin = request.headers.get("origin");
  if (origin !== ALLOW_ORIGIN) {
    return jsonRes({ error: "Origin not allowed" }, 403, origin);
  }

  const token = env.FEEDBACK_TOKEN;
  if (!token) {
    return jsonRes({ error: "Missing FEEDBACK_TOKEN" }, 500, origin);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return jsonRes({ error: "Invalid JSON" }, 400, origin);
  }

  const { type } = body;

  try {
    switch (type) {
      case "ping":
        return jsonRes({ ok: true, message: "pong" }, 200, origin);

      case "vote":
      case "report":
      case "missing":
      case "state":
        const r = await handleGitHubWrite(type, body, token);
        return jsonRes({ ok: r.ok }, r.status, origin);

      default:
        return jsonRes({ error: `Unknown type: ${type}` }, 400, origin);
    }
  } catch (e) {
    return jsonRes({ error: e.message }, 500, origin);
  }
};

// Helper to create a JSON response with correct CORS headers
function jsonRes(obj, status = 200, origin) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json;charset=UTF-8",
      "Access-Control-Allow-Origin": origin || ALLOW_ORIGIN,
    },
  });
}

// Central logic for writing to GitHub
async function handleGitHubWrite(type, body, token) {
  let path, record;

  switch (type) {
    case "vote":
      path = "votes.jsonl";
      record = { type, vote: body.vote, jobId: body.jobId, ts: new Date().toISOString() };
      break;
    case "report":
      path = "reports.jsonl";
      record = { type, jobId: body.jobId, title: body.title, url: body.url, note: body.note, ts: new Date().toISOString() };
      break;
    case "missing":
      path = "submissions.jsonl";
      record = { type, title: body.title, url: body.url, lastDate: body.lastDate, note: body.note, ts: new Date().toISOString() };
      break;
    case "state":
        const { jobId, action, ts } = body.payload;
        return await upsertJsonMap(token, "user_state.json", (state = {}) => {
            if (action === "undo") {
                delete state[jobId];
            } else {
                state[jobId] = { action, ts: ts || new Date().toISOString() };
            }
            return state;
        });
    default:
      return { ok: false, status: 400 };
  }

  return await appendJsonl(token, path, record);
}

// GitHub API helpers
async function appendJsonl(token, path, record) {
    const url = `https://api.github.com/repos/BreadPitttt/Vacancies-dashboard/contents/${path}`;
    const headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${token}`,
        "User-Agent": "Vacancy-Dashboard-Function",
    };

    let sha;
    let currentContent = "";

    const getRes = await fetch(url, { headers });
    if (getRes.status === 200) {
        const data = await getRes.json();
        sha = data.sha;
        currentContent = atob(data.content);
    }

    const newContent = btoa(currentContent + JSON.stringify(record) + "\n");
    const body = { message: `Append to ${path}`, content: newContent, sha };

    const putRes = await fetch(url, { method: "PUT", headers, body: JSON.stringify(body) });
    return { ok: putRes.ok, status: putRes.status };
}

async function upsertJsonMap(token, path, transform) {
    const url = `https://api.github.com/repos/BreadPitttt/Vacancies-dashboard/contents/${path}`;
    const headers = {
      "Accept": "application/vnd.github+json",
      "Authorization": `Bearer ${token}`,
      "User-Agent": "Vacancy-Dashboard-Function",
    };

    let sha;
    let currentData = {};

    const getRes = await fetch(url, { headers });
    if (getRes.status === 200) {
        const data = await getRes.json();
        sha = data.sha;
        currentData = JSON.parse(atob(data.content) || "{}");
    }

    const newData = transform(currentData);
    const newContent = btoa(JSON.stringify(newData, null, 2));
    const body = { message: `Update ${path}`, content: newContent, sha };

    const putRes = await fetch(url, { method: "PUT", headers, body: JSON.stringify(body) });
    return { ok: putRes.ok, status: putRes.status };
}
