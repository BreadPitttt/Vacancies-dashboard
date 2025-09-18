// functions/api.js — Final, simplified, single-file function
const ALLOW_ORIGIN = "https://breadpitttt.github.io";

// This handler responds to the browser's initial "handshake" request
const handleOptions = (request) => {
  if (
    request.headers.get("Origin") === ALLOW_ORIGIN &&
    request.headers.get("Access-Control-Request-Method") === "POST"
  ) {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": ALLOW_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
        "Access-Control-Max-Age": "86400",
      },
    });
  }
  return new Response(null, { status: 403 });
};

// This handler processes the actual data from your dashboard buttons
const handlePost = async ({ request, env }) => {
  const origin = request.headers.get("origin");
  if (origin !== ALLOW_ORIGIN) {
    return jsonRes({ error: "Origin not allowed" }, 403);
  }

  const token = env.FEEDBACK_TOKEN;
  if (!token) {
    return jsonRes({ error: "Server is missing FEEDBACK_TOKEN" }, 500);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return jsonRes({ error: "Invalid JSON body" }, 400);
  }

  try {
    const r = await handleGitHubWrite(body, token);
    return jsonRes({ ok: r.ok }, r.status);
  } catch (e) {
    return jsonRes({ error: e.message }, 500);
  }
};

export const onRequest = async (context) => {
  if (context.request.method === "OPTIONS") {
    return handleOptions(context.request);
  }
  if (context.request.method === "POST") {
    return await handlePost(context);
  }
  return new Response("Method Not Allowed", { status: 405 });
};

// --- Helper Functions ---

function jsonRes(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json;charset=UTF-8",
      "Access-Control-Allow-Origin": ALLOW_ORIGIN,
    },
  });
}

async function handleGitHubWrite(body, token) {
  const { type } = body;
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
        if (action === "undo") delete state[jobId];
        else state[jobId] = { action, ts: ts || new Date().toISOString() };
        return state;
      });
    default:
      return { ok: false, status: 400 };
  }

  return await appendJsonl(token, path, record);
}

async function appendJsonl(token, path, record) {
    const url = `https://api.github.com/repos/BreadPitttt/Vacancies-dashboard/contents/${path}`;
    const headers = { "Accept": "application/vnd.github+json", "Authorization": `Bearer ${token}`, "User-Agent": "Vacancy-Dashboard" };
    let sha, currentContent = "";

    const getRes = await fetch(url, { headers });
    if (getRes.status === 200) {
        const data = await getRes.json();
        sha = data.sha;
        currentContent = atob(data.content);
    }

    const newContent = btoa(currentContent + JSON.stringify(record) + "\n");
    const putBody = { message: `Append to ${path}`, content: newContent, sha };
    const putRes = await fetch(url, { method: "PUT", headers, body: JSON.stringify(putBody) });
    return { ok: putRes.ok, status: putRes.status };
}

async function upsertJsonMap(token, path, transform) {
  const url = `https://api.github.com/repos/BreadPitttt/Vacancies-dashboard/contents/${path}`;
  const headers = { "Accept": "application/vnd.github+json", "Authorization": `Bearer ${token}`, "User-Agent": "Vacancy-Dashboard" };
  let sha, currentData = {};

  const getRes = await fetch(url, { headers });
  if (getRes.status === 200) {
      const data = await getRes.json();
      sha = data.sha;
      currentData = JSON.parse(atob(data.content) || "{}");
  }

  const newData = transform(currentData);
  const newContent = btoa(JSON.stringify(newData, null, 2));
  const putBody = { message: `Update ${path}`, content: newContent, sha };
  const putRes = await fetch(url, { method: "PUT", headers, body: JSON.stringify(putBody) });
  return { ok: putRes.ok, status: putRes.status };
}
