// functions/feedback.js — Cloudflare Pages Function
const ALLOW_ORIGIN = "https://breadpitttt.github.io";

export const onRequestOptions = () => new Response(null, { status: 204, headers: corsHeaders() });

export const onRequestPost = async ({ request, env }) => {
  try {
    const originHdr = request.headers.get("origin") || "";
    const origin = new URL(originHdr, ALLOW_ORIGIN).origin;
    if (origin !== ALLOW_ORIGIN) return new Response("Origin not allowed", { status: 403, headers: corsHeaders() });

    const token = env.FEEDBACK_TOKEN;
    if (!token) return new Response("Missing FEEDBACK_TOKEN", { status: 500, headers: corsHeaders() });

    const { type, payload } = await request.json();
    if (!type || !payload) return new Response("Bad Request", { status: 400, headers: corsHeaders() });

    // Route: state (applied / not_interested / undo) -> user_state.json
    if (type === "state") {
      const { jobId, action, ts } = payload || {};
      if (!jobId || !action) return new Response("Bad Request", { status: 400, headers: corsHeaders() });
      const { ok, status, body } = await upsertJsonMap(token, "BreadPitttt", "Vacancies-dashboard", "user_state.json", (state) => {
        const copy = state && typeof state === "object" ? state : {};
        if (action === "undo") delete copy[jobId];
        else copy[jobId] = { action, ts: ts || new Date().toISOString() };
        return copy;
      });
      if (!ok) return new Response(body, { status, headers: corsHeaders() });
      return new Response("OK", { status: 200, headers: corsHeaders() });
    }

    // Route: soft vote (card buttons) -> votes.jsonl
    if (type === "report" && (payload.flag || payload.jobId)) {
      const vote = payload.flag === "right" ? "right" : "wrong";
      const record = {
        type: "vote",
        vote,
        jobId: payload.jobId || "",
        title: payload.title || "",
        url: payload.url || "",
        reason: quickReason(payload),
        ts: new Date().toISOString()
      };
      const r = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", "votes.jsonl", record);
      return new Response(r.body, { status: r.status, headers: corsHeaders() });
    }

    // Route: hard report (modal) -> reports.jsonl
    if (type === "report") {
      const record = {
        type: "report",
        flag: "not general vacancy",
        jobId: payload.listingId || "",
        title: payload.reason || "",
        url: payload.evidenceUrl || "",
        note: payload.note || "",
        ts: new Date().toISOString()
      };
      const r = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", "reports.jsonl", record);
      return new Response(r.body, { status: r.status, headers: corsHeaders() });
    }

    // Route: missing vacancy -> submissions.jsonl
    if (type === "missing") {
      const record = {
        type: "missing",
        title: payload.title || "",
        url: payload.url || "",
        note: payload.note || "",
        ts: new Date().toISOString()
      };
      const r = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", "submissions.jsonl", record);
      return new Response(r.body, { status: r.status, headers: corsHeaders() });
    }

    return new Response("Bad Request", { status: 400, headers: corsHeaders() });
  } catch (e) {
    return new Response(`Error: ${e.message}`, { status: 500, headers: corsHeaders() });
  }
};

function corsHeaders(){
  return {
    "Access-Control-Allow-Origin": ALLOW_ORIGIN,
    "Access-Control-Allow-Methods": "OPTIONS, POST",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Max-Age": "86400",
    "Content-Type": "text/plain; charset=utf-8"
  };
}

// For JSONL append files
async function appendJsonl(token, owner, repo, path, record){
  const gh = "https://api.github.com";
  const headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json",
    "User-Agent": "cf-pages-feedback"
  };

  // Read
  let get = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, { headers });
  let sha, current = "";
  if (get.status === 404) { sha = undefined; current = ""; }
  else if (get.ok) { const j = await get.json(); sha = j.sha; current = atob(j.content||""); }
  else { return { status: get.status, body: `Read failed: ${await get.text()}` }; }

  const updated = btoa(current + JSON.stringify(record) + "\n");
  const body = { message: `append ${path}`, content: updated };
  if (sha) body.sha = sha;

  const put = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, {
    method: "PUT", headers, body: JSON.stringify(body)
  });
  if (!put.ok) return { status: 500, body: `Update failed: ${await put.text()}` };
  return { status: 200, body: "OK" };
}

// For JSON map upserts (user_state.json)
async function upsertJsonMap(token, owner, repo, path, transform){
  const gh = "https://api.github.com";
  const headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json",
    "User-Agent": "cf-pages-feedback"
  };
  let get = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, { headers });
  let sha, obj = {};
  if (get.status === 404) { sha = undefined; obj = {}; }
  else if (get.ok) { const j = await get.json(); sha = j.sha; obj = JSON.parse(atob(j.content||"") || "{}"); }
  else { return { ok:false, status:get.status, body:`Read failed: ${await get.text()}` }; }

  const next = transform(obj) || {};
  const updated = btoa(JSON.stringify(next, null, 2));
  const body = { message: `upsert ${path}`, content: updated };
  if (sha) body.sha = sha;

  const put = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, {
    method: "PUT", headers, body: JSON.stringify(body)
  });
  if (!put.ok) return { ok:false, status:500, body:`Update failed: ${await put.text()}` };
  return { ok:true, status:200, body:"OK" };
}

// Quick heuristic; pipeline will refine
function quickReason(p){
  const t = (p.title || "").toLowerCase();
  if (/apply online|notification/.test(t)) return { code:"generic_title", details:t.slice(0,40) };
  return { code:"unknown", details:"" };
}
