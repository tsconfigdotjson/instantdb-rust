// OAuth flow test with a mock OIDC provider (discovery/authorize/token).
// Simulates the browser: /oauth/start -> provider -> /oauth/callback -> app
// redirect -> /oauth/token exchange. Usage: node oauth-test.mjs <app-id>
import http from "node:http";
import crypto from "node:crypto";

const appId = process.argv[2];
if (!appId) throw new Error("usage: node oauth-test.mjs <app-id>");
const SERVER = "http://localhost:8888";
const PROVIDER_PORT = 9377;

const assert = (cond, msg) => {
  if (!cond) throw new Error("ASSERT FAILED: " + msg);
  console.log("ok:", msg);
};

// --- mock OIDC provider ---
const issued = { code: "mock-code-123" };
const provider = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${PROVIDER_PORT}`);
  if (url.pathname === "/.well-known/openid-configuration") {
    res.setHeader("content-type", "application/json");
    res.end(
      JSON.stringify({
        issuer: `http://localhost:${PROVIDER_PORT}`,
        authorization_endpoint: `http://localhost:${PROVIDER_PORT}/authorize`,
        token_endpoint: `http://localhost:${PROVIDER_PORT}/token`,
        jwks_uri: `http://localhost:${PROVIDER_PORT}/jwks`,
      }),
    );
  } else if (url.pathname === "/token") {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      const params = new URLSearchParams(body);
      assert(params.get("code") === issued.code, "provider got the code");
      assert(params.get("client_id") === "mock-client-id", "provider got client_id");
      assert(params.get("client_secret") === "mock-secret", "provider got secret");
      // unsigned id_token is fine: the code-exchange path only decodes the payload
      const payload = Buffer.from(
        JSON.stringify({
          iss: `http://localhost:${PROVIDER_PORT}`,
          sub: "mock-sub-42",
          email: "oauth-user@example.com",
          email_verified: true,
          picture: "https://example.com/pic.png",
          aud: "mock-client-id",
        }),
      ).toString("base64url");
      const idToken = `${Buffer.from(JSON.stringify({ alg: "RS256", typ: "JWT" })).toString("base64url")}.${payload}.sig`;
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ id_token: idToken, access_token: "at" }));
    });
  } else {
    res.statusCode = 404;
    res.end("nope");
  }
});
await new Promise((r) => provider.listen(PROVIDER_PORT, r));

// --- start flow ---
const startUrl = `${SERVER}/runtime/oauth/start?app_id=${appId}&client_name=mock&redirect_uri=${encodeURIComponent("http://localhost:5173/app")}&state=appstate123`;
const startRes = await fetch(startUrl, { redirect: "manual" });
assert(startRes.status === 307 || startRes.status === 302, "start redirects");
const authUrl = new URL(startRes.headers.get("location"));
assert(authUrl.pathname === "/authorize", "redirects to provider authorize");
assert(authUrl.searchParams.get("client_id") === "mock-client-id", "client_id in auth url");
const state = authUrl.searchParams.get("state");
assert(state.length === 72, "state is appid+uuid");
const cookie = startRes.headers.get("set-cookie").split(";")[0];
assert(cookie.startsWith("__session=instantdb_"), "session cookie set");

// --- provider "redirects back" with a code (simulate browser GET) ---
const cbUrl = `${SERVER}/runtime/oauth/callback?state=${state}&code=${issued.code}`;
const cbRes = await fetch(cbUrl, { redirect: "manual", headers: { cookie } });
assert(cbRes.status === 307 || cbRes.status === 302, "callback redirects to app");
const appRedirect = new URL(cbRes.headers.get("location"));
assert(appRedirect.origin + appRedirect.pathname === "http://localhost:5173/app", "back to app");
assert(appRedirect.searchParams.get("_instant_oauth_redirect") === "true", "oauth marker");
assert(appRedirect.searchParams.get("state") === "appstate123", "app state preserved");
const appCode = appRedirect.searchParams.get("code");
assert(appCode, "app-level code present");

// --- exchange the code ---
const tokenRes = await fetch(`${SERVER}/runtime/oauth/token`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ app_id: appId, code: appCode }),
});
const tokenBody = await tokenRes.json();
assert(tokenRes.status === 200, "token exchange 200");
assert(tokenBody.user.email === "oauth-user@example.com", "user email from provider");
assert(tokenBody.refresh_token, "top-level refresh_token");
assert(tokenBody.user.refresh_token === tokenBody.refresh_token, "token in both places");
assert(tokenBody.created === true, "user created");

// --- double exchange fails with the exact hint the client checks ---
const dblRes = await fetch(`${SERVER}/runtime/oauth/token`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ app_id: appId, code: appCode }),
});
const dblBody = await dblRes.json();
assert(dblRes.status === 400, "double exchange 400");
assert(dblBody.hint["record-type"] === "app-oauth-code", "double-exchange hint shape");

// --- second login links to the same user ---
const start2 = await fetch(startUrl, { redirect: "manual" });
const state2 = new URL(start2.headers.get("location")).searchParams.get("state");
const cookie2 = start2.headers.get("set-cookie").split(";")[0];
const cb2 = await fetch(`${SERVER}/runtime/oauth/callback?state=${state2}&code=${issued.code}`, {
  redirect: "manual",
  headers: { cookie: cookie2 },
});
const appCode2 = new URL(cb2.headers.get("location")).searchParams.get("code");
const token2 = await fetch(`${SERVER}/runtime/oauth/token`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ app_id: appId, code: appCode2 }),
}).then((r) => r.json());
assert(token2.created === false, "second login reuses user");
assert(token2.user.id === tokenBody.user.id, "same user id");

// --- verify the refresh token works on the runtime endpoint ---
const verify = await fetch(`${SERVER}/runtime/auth/verify_refresh_token`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ "app-id": appId, "refresh-token": tokenBody.refresh_token }),
}).then((r) => r.json());
assert(verify.user.email === "oauth-user@example.com", "oauth refresh token verifies");

provider.close();
console.log("OAUTH TEST PASSED");
process.exit(0);
