// functions/feedback.js — explicit routes, CORS for GitHub Pages, JSON replies
const ALLOW_ORIGIN = "https://breadpitttt.github.io"; // your dashboard origin

export const onRequestOptions = () => new Response(null, { status: 204, headers: corsHeaders() });

export const onRequestPost = async ({ request, env }) => {
  try {
    const originHdr = request.headers.get("origin") || "";
    const referer = request.headers.get("referer") || "";
    const reqOrigin = safeOrigin(originHdr) || safeOrigin(referer) || "";
    if (reqOrigin !== ALLOW_ORIGIN) return jsonRes({ error: "Origin not allowed", origin: reqOrigin }, 403);

    const token = env.FEEDBACK_TOKEN;
    if (!token) return jsonRes({ error: "Missing FEEDBACK_TOKEN" }, 500);

    let body = {};
    try { body = await request.json(); } catch { return jsonRes({ error: "Invalid JSON" }, 400); }
    const type = body && body.type;

    // Vote: {type:"vote", vote:"right"|"wrong", jobId, title?, url?}
    if (type === "vote") {
      const v = (body.vote === "right" || body.vote === "wrong") ? body.vote : null;
      if (!v || !body.jobId) return jsonRes({ error: "Bad vote payload" }, 400);
      const rec = {
        type: "vote", vote: v,
        jobId: body.jobId || "", title: body.title || "", url: body.url || "",
        ts: new Date().toISOString()
      };
      const r = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", "votes.jsonl", rec);
      return jsonRes({ ok: r.status === 200 }, r.status);
    }

    // Report: {type:"report", jobId?, title?, url?, note?}
    if (type === "report") {
      if (!body.jobId && !body.title && !body.url) return jsonRes({ error: "Bad report payload" }, 400);
      const rec = {
        type: "report",
        jobId: body.jobId || "", title: body.title || "", url: body.url || "",
        note: body.note || "", ts: new Date().toISOString()
      };
      const r = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", "reports.jsonl", rec);
      return jsonRes({ ok: r.status === 200 }, r.status);
    }

    // Missing: {type:"missing", title, url, lastDate?, note?}
    if (type === "missing") {
      if (!body.title || !body.url) return jsonRes({ error: "Missing title/url" }, 400);
      const rec = {
        type: "missing", title: body.title || "", url: body.url || "",
        lastDate: body.lastDate || body.deadline || "", note: body.note || "",
        ts: new Date().toISOString()
      };
      const r = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", "submissions.jsonl", rec);
      return jsonRes({ ok: r.status === 200 }, r.status);
    }

    // State: {type:"state", payload:{jobId, action:"applied"|"not_interested"|"undo", ts?}}
    if (type === "state") {
      const p = body.payload || {};
      if (!p.jobId || !p.action) return jsonRes({ error: "Bad state payload" }, 400);
      const r = await upsertJsonMap(token, "BreadPitttt", "Vacancies-dashboard", "user_state.json", (state) => {
        const copy = state && typeof state === "object" ? state : {};
        if (p.action === "undo") delete copy[p.jobId];
        else copy[p.jobId] = { action: p.action, ts: p.ts || new Date().toISOString() };
        return copy;
      });
      return jsonRes({ ok: r.ok !== false });
    }

    return jsonRes({ error: "Unknown type" }, 400);
  } catch (e) {
    return jsonRes({ error: e.message }, 500);
  }
};

function corsHeaders(){
  return {
    "Access-Control-Allow-Origin": ALLOW_ORIGIN,
    "Access-Control-Allow-Methods": "OPTIONS, POST",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Max-Age": "86400",
    "Content-Type": "application/json; charset=utf-8"
  };
}
function jsonRes(obj, status=200){ return new Response(JSON.stringify(obj), { status, headers: corsHeaders() }); }
function safeOrigin(u){ try{ return new URL(u).origin; }catch{ return ""; } }

// JSONL helpers
async function appendJsonl(token, owner, repo, path, record){
  const gh = "https://api.github.com";
  const headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json",
    "User-Agent": "cf-pages-feedback"
  };
  const get = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, { headers });
  let sha, current = "";
  if (get.status === 404) { sha = undefined; current = ""; }
  else if (get.ok) { const j = await get.json(); sha = j.sha; current = atob(j.content||""); }
  else { return { status: get.status, body: `Read failed: ${await get.text()}` }; }
  const updated = btoa(current + JSON.stringify(record) + "\n");
  const body = { message: `append ${path}`, content: updated };
  if (sha) body.sha = sha;
  const put = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, { method: "PUT", headers, body: JSON.stringify(body) });
  if (!put.ok) return { status: 500, body: `Update failed: ${await put.text()}` };
  return { status: 200, body: "OK" };
}
async function upsertJsonMap(token, owner, repo, path, transform){
  const gh = "https://api.github.com";
  const headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json",
    "User-Agent": "cf-pages-feedback"
  };
  const get = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, { headers });
  let sha, obj = {};
  if (get.status === 404) { sha = undefined; obj = {}; }
  else if (get.ok) { const j = await get.json(); sha = j.sha; obj = JSON.parse(atob(j.content||"") || "{}"); }
  else { return { ok:false, status:get.status, body:`Read failed: ${await get.text()}` }; }
  const next = transform(obj) || {};
  const updated = btoa(JSON.stringify(next, null, 2));
  const body = { message: `upsert ${path}`, content: updated };
  if (sha) body.sha = sha;
  const put = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, { method: "PUT", headers, body: JSON.stringify(body) });
  if (!put.ok) return { ok:false, status:500, body:`Update failed: ${await put.text()}` };
  return { ok:true, status:200, body:"OK" };
}
