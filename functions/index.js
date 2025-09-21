// functions/index.js — route "/"
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

  // Only POST for the test
  if (req.method !== "POST") {
    return withCORS(new Response("Method Not Allowed", { status: 405 }));
  }

  const origin = req.headers.get("Origin") || "";
  if (origin !== ALLOW_ORIGIN) {
    return withCORS(new Response(JSON.stringify({ error: "Origin not allowed" }), {
      status: 403,
      headers: { "Content-Type": "application/json" }
    }));
  }

  let body;
  try { body = await req.json(); }
  catch { return withCORS(json({ error: "Invalid JSON" }, 400)); }

  // Echo back so you can see 200 OK
  return withCORS(json({ ok: true, echo: body }, 200));
}

function json(obj, status=200){
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" }
  });
}
function withCORS(res){
  const h = new Headers(res.headers);
  h.set("Access-Control-Allow-Origin", ALLOW_ORIGIN);
  return new Response(res.body, { status: res.status, headers: h });
}
