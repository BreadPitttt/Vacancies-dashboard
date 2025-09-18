// functions/v1/submit.js — Minimal version to debug CORS preflight
const ALLOW_ORIGIN = "https://breadpitttt.github.io";

export const onRequest = async (context) => {
  // Handle CORS preflight requests
  if (context.request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": ALLOW_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age": "86400",
      },
    });
  }

  // Handle POST requests
  if (context.request.method === "POST") {
    const origin = context.request.headers.get("origin");
    if (origin !== ALLOW_ORIGIN) {
      return new Response(JSON.stringify({ error: "Origin not allowed" }), {
        status: 403,
        headers: { "Content-Type": "application/json" }
      });
    }

    // For now, just return a simple success message for any POST
    return new Response(JSON.stringify({ ok: true, message: "Request received" }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": ALLOW_ORIGIN,
      },
    });
  }

  // For any other method, return method not allowed
  return new Response(null, { status: 405 });
};
