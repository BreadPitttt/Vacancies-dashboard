// functions/index.js — unified Worker
// - Keeps existing GitHub JSONL writes (votes/reports/missing/state)
// - Adds KV-backed user_state.json sync for cross-device persistence
// - Adds GET ?state=1 to read KV state
// - CORS locked to your Pages origin

const ALLOW_ORIGIN = "https://breadpitttt.github.io"; // your Pages origin
const OWNER = "BreadPitttt";
const REPO  = "Vacancies-dashboard";

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const origin = req.headers.get("Origin") || "";

    // Diagnostics
    if (url.pathname === "/diag" && req.method === "GET") {
      const hasToken = !!(env && env.FEEDBACK_TOKEN);
      const hasKV = !!(env && env.VAC_STATE);
      return json({ ok: true, hasToken, hasKV }, 200);
    }

    // CORS preflight
    if (req.method === "OPTIONS") {
      if (origin === ALLOW_ORIGIN) {
        return new Response(null, {
          status: 204,
          headers: {
            "Access-Control-Allow-Origin": ALLOW_ORIGIN,
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "86400"
          }
        });
      }
      return new Response(null, { status: 403 });
    }

    // KV: GET ?state=1 -> return merged/saved user_state.json from KV
    if (req.method === "GET" && url.searchParams.get("state") === "1") {
      try {
        const raw = await env.VAC_STATE.get("user_state.json");
        const body = raw ? JSON.parse(raw) : {};
        return withCORS(json({ ok: true, state: body }, 200), ALLOW_ORIGIN);
      } catch (e) {
        return withCORS(json({ ok: false, error: "read_failed" }, 500), ALLOW_ORIGIN);
      }
    }

    // Only POST after here
    if (req.method !== "POST") {
      return withCORS(new Response("Method Not Allowed", { status: 405 }), ALLOW_ORIGIN);
    }

    if (origin !== ALLOW_ORIGIN) {
      return withCORS(json({ error: "Origin not allowed" }, 403), ALLOW_ORIGIN);
    }

    let body;
    try { body = await req.json(); }
    catch { return withCORS(json({ error: "Invalid JSON" }, 400), ALLOW_ORIGIN); }

    const type = body && body.type;

    // New: KV user state sync (does not replace GitHub state; complements it)
    if (type === "user_state_sync") {
      try {
        let current = {};
        const raw = await env.VAC_STATE.get("user_state.json");
        if (raw) current = JSON.parse(raw);
        const merged = { ...current, ...(body.payload || {}) };
        await env.VAC_STATE.put("user_state.json", JSON.stringify(merged));
        return withCORS(json({ ok: true, saved: Object.keys(merged).length }, 200), ALLOW_ORIGIN);
      } catch {
        return withCORS(json({ ok: false, error: "kv_write_failed" }, 500), ALLOW_ORIGIN);
      }
    }

    // Existing GitHub-backed features (preserved)
    const token = env && env.FEEDBACK_TOKEN;
    if (!token) {
      return withCORS(json({ error:"Missing FEEDBACK_TOKEN secret on Worker (Production)" }, 500), ALLOW_ORIGIN);
    }

    if (type === "vote") {
      if (!body.vote || !body.jobId) return withCORS(json({ error:"Bad vote payload" }, 400), ALLOW_ORIGIN);
      const rec = { type, vote: body.vote, jobId: body.jobId, title: body.title||"", url: body.url||"", ts: new Date().toISOString() };
      const r = await appendJsonl(token, OWNER, REPO, "votes.jsonl", rec);
      return withCORS(json({ ok:r.ok }, r.ok?200:500), ALLOW_ORIGIN);
    }

    if (type === "report") {
      const rec = { type, jobId: body.jobId||"", title: body.title||"", url: body.url||"", note: body.note||"", ts: new Date().toISOString() };
      if (!rec.jobId && !rec.title && !rec.url) return withCORS(json({ error:"Bad report payload" }, 400), ALLOW_ORIGIN);
      const r = await appendJsonl(token, OWNER, REPO, "reports.jsonl", rec);
      return withCORS(json({ ok:r.ok }, r.ok?200:500), ALLOW_ORIGIN);
    }

    if (type === "missing") {
      if (!body.title || !body.url) return withCORS(json({ error:"Missing title/url" }, 400), ALLOW_ORIGIN);
      const rec = {
        type, title: body.title, url: body.url,
        lastDate: body.lastDate||"", note: body.note||"",
        officialSite: body.officialSite||"", posts: body.posts||null,
        ts: new Date().toISOString()
      };
      const r = await appendJsonl(token, OWNER, REPO, "submissions.jsonl", rec);
      return withCORS(json({ ok:r.ok }, r.ok?200:500), ALLOW_ORIGIN);
    }

    if (type === "state") {
      const p = body.payload||{};
      const jobId = p.jobId||"", action=p.action||"";
      if (!jobId || !action) return withCORS(json({ error:"Bad state payload" }, 400), ALLOW_ORIGIN);
      const r = await upsertJsonMap(token, OWNER, REPO, "user_state.json", (state = {}) => {
        if (action === "undo") delete state[jobId];
        else state[jobId] = { action, ts: p.ts || new Date().toISOString() };
        return state;
      });
      return withCORS(json({ ok:r.ok }, r.ok?200:500), ALLOW_ORIGIN);
    }

    // Fallback
    return withCORS(json({ ok:true, message:"pong" }, 200), ALLOW_ORIGIN);
  }
};

// helpers
function withCORS(res, origin){
  const h = new Headers(res.headers);
  h.set("Access-Control-Allow-Origin", origin);
  return new Response(res.body, { status: res.status, headers: h });
}
function json(obj, status=200){
  return new Response(JSON.stringify(obj), { status, headers: { "Content-Type":"application/json; charset=utf-8" }});
}

// GitHub API helpers (unchanged)
async function ghGet(token, owner, repo, path){
  return fetch(`https://api.github.com/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, {
    headers: { "Accept":"application/vnd.github+json", "Authorization":`Bearer ${token}`, "User-Agent":"vacancy-worker" }
  });
}
async function ghPut(token, owner, repo, path, contentB64, sha, message){
  return fetch(`https://api.github.com/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, {
    method:"PUT",
    headers: { "Accept":"application/vnd.github+json", "Authorization":`Bearer ${token}`, "User-Agent":"vacancy-worker" },
    body: JSON.stringify(sha ? { message, content: contentB64, sha } : { message, content: contentB64 })
  });
}
async function appendJsonl(token, owner, repo, path, record){
  let sha, current="";
  const g = await ghGet(token, owner, repo, path);
  if (g.status===200){
    const j = await g.json();
    sha = j.sha; current = atob(j.content||"");
  } else if (g.status!==404){
    return { ok:false, status:g.status };
  }
  const updated = btoa(current + JSON.stringify(record) + "\n");
  const p = await ghPut(token, owner, repo, path, updated, sha, `append ${path}`);
  return { ok:p.ok, status:p.status };
}
async function upsertJsonMap(token, owner, repo, path, transform){
  let sha, obj={};
  const g = await ghGet(token, owner, repo, path);
  if (g.status===200){
    const j = await g.json();
    sha = j.sha; obj = JSON.parse(atob(j.content||"")||"{}");
  } else if (g.status!==404){
    return { ok:false, status:g.status };
  }
  const next = transform(obj)||{};
  const updated = btoa(JSON.stringify(next,null,2));
  const p = await ghPut(token, owner, repo, path, updated, sha, `upsert ${path}`);
  return { ok:p.ok, status:p.status };
}
