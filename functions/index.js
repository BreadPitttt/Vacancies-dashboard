// functions/index.js — Guaranteed route at "/"
const ALLOW_ORIGIN = "https://breadpitttt.github.io";

export async function onRequest(context) {
  const req = context.request;

  // Preflight (OPTIONS)
  if (req.method === "OPTIONS") {
    const origin = req.headers.get("Origin") || "";
    if (origin === ALLOW_ORIGIN) {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": ALLOW_ORIGIN,
          "Access-Control-Allow-Methods": "POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization",
          "Access-Control-Max-Age": "86400"
        }
      });
    }
    return new Response(null, { status: 403 });
  }

  // Only accept POST for actual work
  if (req.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405, headers: { "Access-Control-Allow-Origin": ALLOW_ORIGIN } });
  }

  const origin = req.headers.get("Origin") || "";
  if (origin !== ALLOW_ORIGIN) {
    return new Response(JSON.stringify({ error: "Origin not allowed" }), {
      status: 403,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": ALLOW_ORIGIN }
    });
  }

  let body;
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  // Echo-only smoke test to confirm end-to-end works.
  // Replace with your GitHub write once this returns 200 from the page.
  return json({ ok: true, echo: body }, 200);
}

function json(obj, status=200){
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "https://breadpitttt.github.io"
    }
  });
}
