// functions/feedback.js — Cloudflare Pages Function (no deps), CORS + JSONL writes

const ALLOW_ORIGIN = "https://breadpitttt.github.io"; // your site origin

export const onRequestOptions = () => new Response(null, {
  status: 204,
  headers: corsHeaders()
});

export const onRequestPost = async ({ request, env }) => {
  try {
    const origin = new URL(request.headers.get("origin") || "", ALLOW_ORIGIN).origin;
    if (origin !== ALLOW_ORIGIN) {
      return new Response("Origin not allowed", { status: 403, headers: corsHeaders() });
    }

    const token = env.FEEDBACK_TOKEN;
    if (!token) return new Response("Missing FEEDBACK_TOKEN", { status: 500, headers: corsHeaders() });

    const { type, payload } = await request.json();
    if (!type || !payload) return new Response("Bad Request", { status: 400, headers: corsHeaders() });

    // Detect soft votes (card buttons) vs hard report (modal)
    const isVote = !!(payload.flag || payload.jobId) && type === 'report';
    const filePath = isVote ? "votes.jsonl" : (type === "missing" ? "submissions.jsonl" : "reports.jsonl");

    // Build record
    let record;
    if (isVote) {
      // Soft vote with quick inline reason guess; the pipeline refines later
      const vote = (payload.flag === "right") ? "right" : "wrong";
      record = {
        type: "vote",
        vote,
        jobId: payload.jobId || "",
        title: payload.title || "",
        url: payload.url || "",
        reason: quickReason(payload), // basic guess; pipeline runs deeper checks
        ts: new Date().toISOString()
      };
    } else if (type === "report") {
      // Hard report (modal)
      record = {
        type: "report",
        flag: "not general vacancy",
        jobId: payload.listingId || "",
        title: payload.reason || "",
        url: payload.evidenceUrl || "",
        note: payload.note || "",
        ts: new Date().toISOString()
      };
    } else {
      // Missing modal
      record = {
        type: "missing",
        title: payload.title || "",
        url: payload.url || "",
        note: payload.note || "",
        ts: new Date().toISOString()
      };
    }

    // GitHub write
    const ok = await appendJsonl(token, "BreadPitttt", "Vacancies-dashboard", filePath, record);
    if (!ok.ok) return new Response(ok.body, { status: ok.status, headers: corsHeaders() });
    return new Response("OK", { status: 200, headers: corsHeaders() });
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

// Quick, conservative guess; refined in pipeline
function quickReason(p){
  const t = (p.title || "").toLowerCase();
  if (/aiims|cisf|rsmssb/.test(t)) return { code: "named_org", details: t.slice(0, 40) };
  if (/apply online|notification/.test(t)) return { code: "generic_title", details: t.slice(0, 40) };
  return { code: "unknown", details: "" };
}

async function appendJsonl(token, owner, repo, path, record){
  const gh = "https://api.github.com";
  const headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json",
    "User-Agent": "cf-pages-feedback"
  };

  // Get current file (404 tolerant)
  let get = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, { headers });
  let sha, current = "";
  if (get.status === 404) {
    sha = undefined; current = "";
  } else if (get.ok) {
    const j = await get.json();
    sha = j.sha;
    current = atob(j.content || "");
  } else {
    return { ok:false, status:get.status, body:`Read failed: ${await get.text()}` };
  }

  const line = JSON.stringify(record) + "\n";
  const updated = btoa(current + line);
  const body = { message: `feedback: append to ${path}`, content: updated };
  if (sha) body.sha = sha;

  const put = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, {
    method: "PUT", headers, body: JSON.stringify(body)
  });
  if (!put.ok) {
    return { ok:false, status:500, body:`Update failed: ${await put.text()}` };
  }
  return { ok:true, status:200, body:"OK" };
}
