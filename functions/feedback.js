// functions/feedback.js — Cloudflare Pages Function (no dependencies)

// CORS origin: your GitHub Pages site
const ALLOW_ORIGIN = "https://breadpitttt.github.io";

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

    const filePath = (type === "missing") ? "submissions.jsonl" : "reports.jsonl";
    const record = normalize(type, payload);

    const owner = "BreadPitttt";
    const repo  = "Vacancies-dashboard";
    const gh    = "https://api.github.com";
    const headers = {
      "Accept": "application/vnd.github+json",
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json",
      "User-Agent": "cf-pages-feedback"
    };

    // Read (404 tolerant)
    let sha, current = "";
    let r = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(filePath)}`, { headers });
    if (r.status === 404) { sha = undefined; current = ""; }
    else if (r.ok) {
      const j = await r.json();
      sha = j.sha;
      current = atob(j.content || "");
    } else {
      return new Response(`Read failed: ${await r.text()}`, { status: r.status, headers: corsHeaders() });
    }

    const line = JSON.stringify({ ...record, ts: new Date().toISOString() }) + "\n";
    const updated = btoa(current + line);
    const body = { message: `feedback: append to ${filePath}`, content: updated };
    if (sha) body.sha = sha;

    r = await fetch(`${gh}/repos/${owner}/${repo}/contents/${encodeURIComponent(filePath)}`, {
      method: "PUT", headers, body: JSON.stringify(body)
    });
    if (!r.ok) return new Response(`Update failed: ${await r.text()}`, { status: 500, headers: corsHeaders() });

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
function normalize(type, p){
  if (p && (p.flag || p.jobId)) {
    return { type:"report", flag:p.flag || "not general vacancy", jobId:p.jobId||"", title:p.title||"", url:p.url||"", note:p.note||"" };
  }
  if (type === "report") {
    return { type:"report", flag:"not general vacancy", jobId:p.listingId||"", title:p.reason||"", url:p.evidenceUrl||"", note:p.note||"" };
  }
  return { type:"missing", title:p.title||"", url:p.url||"", note:p.note||"" };
}
