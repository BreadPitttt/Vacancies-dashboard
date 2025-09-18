// functions/feedback.js — robust routes, dual-shape support, ping, clear errors
const ALLOW_ORIGIN = "https://breadpitttt.github.io"; // must match your GitHub Pages origin

export const onRequestOptions = () => new Response(null, { status: 204, headers: corsHeaders() });

export const onRequestPost = async ({ request, env }) => {
  try {
    const originHdr = request.headers.get("origin") || "";
    const referer = request.headers.get("referer") || "";
    const reqOrigin = safeOrigin(originHdr) || safeOrigin(referer) || "";
    if (reqOrigin !== ALLOW_ORIGIN) return jsonRes({ error: "Origin not allowed", got: reqOrigin, want: ALLOW_ORIGIN }, 403);

    const token = env.FEEDBACK_TOKEN;
    if (!token) return jsonRes({ error: "Missing FEEDBACK_TOKEN" }, 500);

    let body = {};
    try { body = await request.json(); } catch { return jsonRes({ error: "Invalid JSON body" }, 400); }

    // Allow both new and old shapes:
    // New: { type, ...fields }
    // Old (from earlier client): { type, payload: {...} } for report/state
    const type = body.type;
    const p = body.payload && typeof body.payload === "object" ? body.payload : body;

    // Smoke test route
    if (type === "ping") return jsonRes({ ok: true, message: "pong" });

    if (type === "vote") {
      const v = (p.vote === "right" || p.vote === "wrong") ? p.vote : null;
      if (!v || !p.jobId) return jsonRes({ error: "Bad vote payload", need: "vote, jobId" }, 400);
      const rec = { type: "vote", vote: v, jobId: p.jobId || "", title: p.title || "", url: p.url || "", ts: nowIso() };
      const r = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", "votes.jsonl", rec);
      return jsonRes({ ok: r.status === 200 }, r.status);
    }

    if (type === "report") {
      const rec = { type: "report", jobId: (p.jobId || p.listingId || ""), title: (p.title || p.reason || ""), url: (p.url || p.evidenceUrl || ""), note: (p.note || ""), ts: nowIso() };
      if (!rec.jobId && !rec.title && !rec.url) return jsonRes({ error: "Bad report payload", need: "jobId or title or url" }, 400);
      const r = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", "reports.jsonl", rec);
      return jsonRes({ ok: r.status === 200 }, r.status);
    }

    if (type === "missing") {
      const title = p.title || "";
      const url = p.url || "";
      const lastDate = p.lastDate || p.deadline || "";
      if (!title || !url) return jsonRes({ error: "Missing title/url" }, 400);
      const rec = { type: "missing", title, url, lastDate, note: p.note || "", ts: nowIso() };
      const r = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", "submissions.jsonl", rec);
      return jsonRes({ ok: r.status === 200 }, r.status);
    }

    if (type === "state") {
      const jobId = p.jobId || "";
      const action = p.action || "";
      if (!jobId || !action) return jsonRes({ error: "Bad state payload", need: "jobId, action" }, 400);
      const r = await upsertJsonMap(env.FEEDBACK_TOKEN, "BreadPitttt", "Vacancies-dashboard", "user_state.json", (state) => {
        const copy = state && typeof state === "object" ? state : {};
        if (action === "undo") delete copy[jobId];
        else copy[jobId] = { action, ts: p.ts || nowIso() };
        return copy;
      });
      return jsonRes({ ok: r.ok !== false });
    }

    return jsonRes({ error: "Unknown type", got: type }, 400);
  } catch (e) {
    return jsonRes({ error: e.message }, 500);
  }
};

function nowIso(){ return new Date().toISOString(); }
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

// JSON helpers
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
