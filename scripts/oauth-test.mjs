// OAuth flow test with a mock OIDC provider (discovery/authorize/token).
// Simulates the browser: /oauth/start -> provider -> /oauth/callback -> app
// redirect -> /oauth/token exchange. Usage: node oauth-test.mjs <app-id>
import http from "node:http";
import crypto from "node:crypto";

const appId = process.argv[2];
const adminToken = process.argv[3]; // optional: enables the linkedPrimaryUser check
if (!appId) throw new Error("usage: node oauth-test.mjs <app-id> [<admin-token>]");
const SERVER = "http://localhost:8888";
const PROVIDER_PORT = 9377;

// RSA keypair for the signed id_token path (/runtime/oauth/id_token verifies
// signatures against the provider's JWKS; the code-exchange path only decodes)
const { publicKey, privateKey } = crypto.generateKeyPairSync("rsa", { modulusLength: 2048 });
const jwk = { ...publicKey.export({ format: "jwk" }), kid: "k1", use: "sig", alg: "RS256" };
const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
const signJwt = (claims, alg = "RS256") => {
  const input = `${b64({ alg, typ: "JWT", kid: "k1" })}.${b64(claims)}`;
  const sig = crypto.sign("sha256", Buffer.from(input), privateKey).toString("base64url");
  return `${input}.${sig}`;
};
const claimsFor = (sub, email) => ({
  iss: `http://localhost:${PROVIDER_PORT}`,
  sub,
  email,
  email_verified: true,
  aud: "mock-client-id",
  iat: Math.floor(Date.now() / 1000),
  exp: Math.floor(Date.now() / 1000) + 3600,
});

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
        id_token_signing_alg_values_supported: ["RS256"],
      }),
    );
  } else if (url.pathname === "/jwks") {
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ keys: [jwk] }));
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

// --- Origin must be an authorized redirect origin on the token exchange ---
const freshCode = async () => {
  const s = await fetch(startUrl, { redirect: "manual" });
  const st = new URL(s.headers.get("location")).searchParams.get("state");
  const ck = s.headers.get("set-cookie").split(";")[0];
  const cb = await fetch(`${SERVER}/runtime/oauth/callback?state=${st}&code=${issued.code}`, { redirect: "manual", headers: { cookie: ck } });
  return new URL(cb.headers.get("location")).searchParams.get("code");
};
const exchange = (code, headers = {}, extra = {}) =>
  fetch(`${SERVER}/runtime/oauth/token`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify({ app_id: appId, code, ...extra }),
  }).then(async (r) => ({ status: r.status, body: await r.json() }));
const evil = await exchange(await freshCode(), { origin: "https://evil.example" });
assert(evil.status === 400 && evil.body.type === "validation-failed", "token exchange from an unauthorized Origin is refused");
assert(evil.body.message === "Validation failed for origin: Unauthorized origin.", "legacy origin message");
const good = await exchange(await freshCode(), { origin: "http://localhost:5173" });
assert(good.status === 200 && good.body.user.id === tokenBody.user.id, "token exchange from the authorized Origin works");

// --- redirect_uri must be an authorized origin (no implicit localhost) ---
const badStart = await fetch(
  `${SERVER}/runtime/oauth/start?app_id=${appId}&client_name=mock&redirect_uri=${encodeURIComponent("http://localhost:9999/app")}`,
  { redirect: "manual" },
);
assert(badStart.status === 400, "an unlisted redirect_uri is refused, localhost included");

// --- id_token sign-in: signature verified against the JWKS, alg gated by discovery ---
const idTokenCall = (body, headers = {}) =>
  fetch(`${SERVER}/runtime/oauth/id_token`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify({ app_id: appId, client_name: "mock", ...body }),
  }).then(async (r) => ({ status: r.status, body: await r.json() }));
const es = await idTokenCall({ id_token: signJwt(claimsFor("mock-sub-42", "oauth-user@example.com"), "ES256") });
assert(es.status === 400 && es.body.type === "oauth-error", "an algorithm the discovery document doesn't list is refused");
assert(es.body.error === "The id_token used an unsupported algorithm.", "unsupported-alg message");
const forged = signJwt(claimsFor("mock-sub-42", "oauth-user@example.com")).replace(/\.[^.]+$/, ".AAAA");
const bad = await idTokenCall({ id_token: forged });
assert(bad.status === 400 && bad.body.type === "oauth-error", "a bad signature is refused");
const evilOrigin = await idTokenCall({ id_token: signJwt(claimsFor("mock-sub-42", "oauth-user@example.com")) }, { origin: "https://evil.example" });
assert(evilOrigin.status === 400 && evilOrigin.body.type === "validation-failed", "id_token sign-in from an unauthorized Origin is refused");
const signed = await idTokenCall({ id_token: signJwt(claimsFor("mock-sub-42", "oauth-user@example.com")) }, { origin: "http://localhost:5173" });
assert(signed.status === 200 && signed.body.user.id === tokenBody.user.id, "a signed id_token signs the linked user in");

// --- guests upgrade through OAuth: a fresh email keeps the guest id ---
const signInGuest = () =>
  fetch(`${SERVER}/runtime/auth/sign_in_guest`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ "app-id": appId }),
  }).then((r) => r.json());
const guest = (await signInGuest()).user;
const up = await idTokenCall({ id_token: signJwt(claimsFor("sub-guest-1", "guest-oauth@example.com")), refresh_token: guest.refresh_token });
assert(up.status === 200 && up.body.user.id === guest.id && up.body.created === true, "guest upgraded in place for a new email");
// ...and an existing account gets the guest linked to it
const guest2 = (await signInGuest()).user;
const linked = await exchange(await freshCode(), {}, { refresh_token: guest2.refresh_token });
assert(linked.status === 200 && linked.body.user.id === tokenBody.user.id, "guest signing into an existing account gets that account");
if (adminToken) {
  const users = await fetch(`${SERVER}/admin/query`, {
    method: "POST",
    headers: { "content-type": "application/json", "app-id": appId, authorization: `Bearer ${adminToken}` },
    body: JSON.stringify({ query: { $users: { $: { where: { id: guest2.id } } } } }),
  }).then((r) => r.json());
  assert(users.$users?.[0]?.linkedPrimaryUser === tokenBody.user.id, "guest row points at the primary user (linkedPrimaryUser)");
}

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
