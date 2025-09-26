// functions/index.js — enforce normalized report schema so lastDate/eligibility always persist
const ALLOW_ORIGIN = "https://breadpitttt.github.io";
const OWNER = "BreadPitttt";
const REPO  = "Vacancies-dashboard";
const STATE_KEY = "user_state_personal.json";

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const origin = req.headers.get("Origin") || "";

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

    // KV single-tenant read
    if (req.method === "GET" && url.searchParams.get("state") === "1") {
      try {
        const raw = await env.VAC_STATE.get(STATE_KEY);
        const body = raw ? JSON.parse(raw) : {};
        return cors(json({ ok: true, state: body }), ALLOW_ORIGIN);
      } catch {
        return cors(json({ ok: false, error: "read_failed" }, 500), ALLOW_ORIGIN);
      }
    }

    if (req.method !== "POST") {
      return cors(new Response("Method Not Allowed", { status: 405 }), ALLOW_ORIGIN);
    }
    if (origin !== ALLOW_ORIGIN) {
      return cors(json({ error: "Origin not allowed" }, 403), ALLOW_ORIGIN);
    }

    let body;
    try { body = await req.json(); } catch { return cors(json({ error: "Invalid JSON" }, 400), ALLOW_ORIGIN); }
    const type = body && body.type;
    const token = env && env.FEEDBACK_TOKEN;

    // Single-tenant state merge (unchanged)
    if (type === "user_state_sync") {
      try {
        let current = {};
        const raw = await env.VAC_STATE.get(STATE_KEY);
        if (raw) current = JSON.parse(raw);
        const merged = { ...current, ...(body.payload || {}) };
        await env.VAC_STATE.put(STATE_KEY, JSON.stringify(merged));
        return cors(json({ ok: true, saved: Object.keys(merged).length }), ALLOW_ORIGIN);
      } catch {
        return cors(json({ ok: false, error: "kv_write_failed" }, 500), ALLOW_ORIGIN);
      }
    }

    if (!token) return cors(json({ error:"Missing FEEDBACK_TOKEN secret" }, 500), ALLOW_ORIGIN);

    // Votes (unchanged)
    if (type === "vote") {
      if (!body.vote || !body.jobId) return cors(json({ error:"Bad vote payload" }, 400), ALLOW_ORIGIN);
      const rec = { type, vote: body.vote, jobId: body.jobId, title: body.title||"", url: body.url||"", ts: new Date().toISOString() };
      const r = await appendJsonl(token, OWNER, REPO, "votes.jsonl", rec);
      return cors(json({ ok:r.ok }, r.ok?200:500), ALLOW_ORIGIN);
    }

    // REPORTS — enforce normalized schema here
    if (type === "report") {
      const nd = normalizeDate(body.lastDate || "");
      const rec = {
        type: "report",
        jobId: String(body.jobId||"").trim(),
        title: String(body.title||"").trim(),
        url: normalizeUrl(String(body.url||"").trim()),
        reasonCode: String(body.reasonCode||"").trim(),        // critical
        evidenceUrl: String(body.evidenceUrl||"").trim(),
        posts: body.posts===null || body.posts===undefined || body.posts==="" ? null : String(body.posts).trim(),
        lastDate: nd || "",                                     // dd/mm/yyyy or ""
        eligibility: String(body.eligibility||"").trim(),
        note: String(body.note||"").trim(),
        ts: new Date().toISOString()
      };
      // Require enough identity
      if (!rec.jobId && !rec.title && !rec.url) {
        return cors(json({ error:"Bad report payload" }, 400), ALLOW_ORIGIN);
      }
      const r = await appendJsonl(token, OWNER, REPO, "reports.jsonl", rec);
      return cors(json({ ok:r.ok, normalizedLastDate: rec.lastDate }), r.ok?200:500);
    }

    // Missing submissions (unchanged, but normalize date)
    if (type === "missing") {
      if (!body.title || !body.url) return cors(json({ error:"Missing title/url" }, 400), ALLOW_ORIGIN);
      const rec = {
        type:"missing",
        title:String(body.title||"").trim(),
        url:normalizeUrl(String(body.url||"").trim()),
        lastDate: normalizeDate(body.lastDate||"") || String(body.lastDate||"").trim(),
        note:String(body.note||"").trim(),
        officialSite:String(body.officialSite||"").trim(),
        posts: body.posts===null || body.posts===undefined || body.posts==="" ? null : String(body.posts).trim(),
        ts:new Date().toISOString()
      };
      const r = await appendJsonl(token, OWNER, REPO, "submissions.jsonl", rec);
      return cors(json({ ok:r.ok }, r.ok?200:500), ALLOW_ORIGIN);
    }

    // State audit in repo (unchanged)
    if (type === "state") {
      const p = body.payload||{};
      const jobId = p.jobId||"", action=p.action||"";
      if (!jobId || !action) return cors(json({ error:"Bad state payload" }, 400), ALLOW_ORIGIN);
      const r = await upsertJsonMap(token, OWNER, REPO, "user_state.json", (state = {}) => {
        if (action === "undo") delete state[jobId];
        else state[jobId] = { action, ts: p.ts || new Date().toISOString() };
        return state;
      });
      return cors(json({ ok:r.ok }, r.ok?200:500), ALLOW_ORIGIN);
    }

    return cors(json({ ok:true, message:"pong" }), ALLOW_ORIGIN);
  }
};

function cors(res, origin){ const h = new Headers(res.headers); h.set("Access-Control-Allow-Origin", origin); return new Response(res.body, { status: res.status, headers: h }); }
function json(obj, status=200){ return new Response(JSON.stringify(obj), { status, headers: { "Content-Type":"application/json; charset=utf-8" }}); }

function normalizeUrl(u){
  try{ const p=new URL(u); p.hash=""; p.search=""; let s=p.toString(); if(s.endsWith("/")) s=s.slice(0,-1); return s; }
  catch{ return (u||"").replace(/[?#].*$/,"").replace(/\/$/,""); }
}
function normalizeDate(s){
  if(!s) return "";
  s=String(s).trim();
  let m=s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
  if(m){ const d=m[1].padStart(2,"0"), mo=m[2].padStart(2,"0"), y=m[3]; return `${d}/${mo}/${y}`; }
  m=s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{2})$/);
  if(m){ const d=m[1].padStart(2,"0"), mo=m[2].padStart(2,"0"), y=`20${m[3]}`; return `${d}/${mo}/${y}`; }
  return s;
}

// GitHub helpers (unchanged)
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
    const j = await g.json(); sha = j.sha; current = atob(j.content||"");
  } else if (g.status!==404){ return { ok:false, status:g.status }; }
  const updated = btoa(current + JSON.stringify(record) + "\n");
  const p = await ghPut(token, owner, repo, path, updated, sha, `append ${path}`);
  return { ok:p.ok, status:p.status };
}
async function upsertJsonMap(token, owner, repo, path, transform){
  let sha, obj={};
  const g = await ghGet(token, owner, repo, path);
  if (g.status===200){
    const j = await g.json(); sha = j.sha; obj = JSON.parse(atob(j.content||"")||"{}");
  } else if (g.status!==404){ return { ok:false, status:g.status }; }
  const next = transform(obj)||{};
  const updated = btoa(JSON.stringify(next,null,2));
  const p = await ghPut(token, owner, repo, path, updated, sha, `upsert ${path}`);
  return { ok:p.ok, status:p.status };
}
