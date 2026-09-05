// Runtime OAuth layer of the differential harness: the client-facing OAuth
// routes (`/runtime/oauth/start|callback|token|id_token`, the app-scoped
// `/runtime/:app_id/oauth/start|token` with PKCE, the openid configuration
// and `/runtime/signout`) driven end to end on both servers against one mock
// OpenID Connect provider (scripts/mock-oidc.mjs). Legacy reaches the mock
// through its SSRF guard, so the mock runs as a container on a
// documentation-range network (203.0.113.0/24, docker-compose.yml) and is
// `mock-oidc:9377` for legacy and `localhost:9377` for this server; every
// URL a probe records has both spellings normalized to `<provider>`.
//
// Usage: node oauth.mjs <app-id> <admin-token>
//   (provision.sh; the same app id and admin token on both servers)
// Env: LEGACY_URL, RUST_URL, MOCK_OIDC_URL (harness → mock, default
//      http://localhost:9377), LEGACY_MOCK_OIDC_URL (legacy → mock, default
//      http://mock-oidc:9377), DUMP=1

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { canon, uuid } from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken) throw new Error("usage: node oauth.mjs <app-id> <admin-token>");

const MOCK = process.env.MOCK_OIDC_URL || "http://localhost:9377";
const SERVERS = {
  legacy: { url: process.env.LEGACY_URL || "http://localhost:8891", provider: process.env.LEGACY_MOCK_OIDC_URL || "http://mock-oidc:9377" },
  rust: { url: process.env.RUST_URL || "http://localhost:8888", provider: MOCK },
};
const APP_REDIRECT = "http://localhost:5173/app";
const APP_ORIGIN = "http://localhost:5173";

// ---------------------------------------------------------------------------
// normalization
const DROP_KEYS = new Set(["trace-id", "debug-uri"]);
const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
function normText(s, ctx) {
  return s
    .replaceAll(ctx.provider, "<provider>")
    .replaceAll(ctx.url, "<server>")
    .replace(/[0-9a-f]{72}/gi, "<state>")
    .replace(UUID_RE, "<uuid>");
}
function norm(v, ctx, key = null) {
  if (typeof v === "string") {
    if (ISO_RE.test(v)) return "<ts>";
    return normText(v, ctx);
  }
  if (Array.isArray(v)) return v.map((x) => norm(x, ctx));
  if (v && typeof v === "object") {
    const out = {};
    for (const [k, val] of Object.entries(v)) {
      if (DROP_KEYS.has(k)) continue;
      out[k] = norm(val, ctx, k);
    }
    return out;
  }
  return v;
}
function errView(res, ctx) {
  const b = res.body && typeof res.body === "object" ? res.body : { "<non-json>": String(res.body).slice(0, 160) };
  return norm({ status: res.status, type: b.type, message: b.message ?? b.error, hint: b.hint ?? null, keys: Object.keys(b).sort() }, ctx);
}
function userView(res, ctx) {
  if (res.status !== 200) return errView(res, ctx);
  const u = res.body.user ?? {};
  return norm({ status: 200, keys: Object.keys(res.body).sort(), created: res.body.created, user: { ...u, keys: Object.keys(u).sort() }, tokenMatches: res.body.refresh_token === undefined ? null : res.body.refresh_token === u.refresh_token }, ctx);
}
// a redirect (302) or an error page / JSON error
async function redirectView(res, ctx) {
  const text = await res.text();
  const loc = res.headers.get("location");
  let u = null;
  try { u = loc ? new URL(loc) : null; } catch { u = null; }
  const cookie = res.headers.get("set-cookie") ?? "";
  const attrs = cookie.split(";").map((a) => a.trim().toLowerCase()).filter(Boolean).map((a) => (a.startsWith("__session=") ? "__session=<cookie>" : a.startsWith("expires=") ? "expires=<date>" : a)).filter((a) => a !== "secure").sort();
  let body = null;
  if (text) {
    try { body = errView({ status: res.status, body: JSON.parse(text) }, ctx); } catch { body = { "<non-json>": normText(text.slice(0, 200), ctx).includes("<!DOCTYPE") ? "<html>" : normText(text.slice(0, 120), ctx) }; }
  }
  return {
    status: res.status,
    contentType: (res.headers.get("content-type") ?? "").split(";")[0],
    location: u ? normText(u.origin + u.pathname, ctx) : loc ? normText(loc, ctx) : null,
    params: u ? Object.fromEntries([...u.searchParams.entries()].map(([k, v]) => [k, normText(v, ctx)])) : null,
    cookieAttrs: attrs,
    body,
  };
}

// ---------------------------------------------------------------------------
// helpers
async function call(base, method, p, { token = adminToken, body, form, headers = {} } = {}) {
  const h = {
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(body !== undefined ? { "content-type": "application/json" } : {}),
    ...(form !== undefined ? { "content-type": "application/x-www-form-urlencoded" } : {}),
    ...headers,
  };
  const res = await fetch(base + p, { method, headers: h, body: body !== undefined ? JSON.stringify(body) : form !== undefined ? new URLSearchParams(form).toString() : undefined, redirect: "manual" });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = text; }
  return { status: res.status, body: json, headers: res.headers };
}
const qs = (o) => new URLSearchParams(Object.entries(o).filter(([, v]) => v !== undefined)).toString();
const startUrl = (ctx, over = {}) => `${ctx.url}/runtime/oauth/start?${qs({ app_id: appId, client_name: "mock", redirect_uri: APP_REDIRECT, ...over })}`;
const fetchStart = async (ctx, over) => {
  const res = await fetch(startUrl(ctx, over), { redirect: "manual" });
  const loc = res.headers.get("location") ?? "";
  let state = null;
  try { state = new URL(loc).searchParams.get("state"); } catch { state = null; }
  const cookie = /__session=([^;]+)/.exec(res.headers.get("set-cookie") ?? "")?.[1] ?? null;
  return { res, state, cookie };
};
const fetchCallback = (ctx, params, cookie, { method = "GET" } = {}) => {
  const headers = cookie ? { cookie: `__session=${cookie}` } : {};
  if (method === "POST") {
    return fetch(`${ctx.url}/runtime/oauth/callback`, { method, redirect: "manual", headers: { ...headers, "content-type": "application/x-www-form-urlencoded" }, body: qs(params) });
  }
  return fetch(`${ctx.url}/runtime/oauth/callback?${qs(params)}`, { redirect: "manual", headers });
};
// a full start → callback round trip; returns the app-level code
async function appCode(ctx, { code = "mock-code", startOver = {} } = {}) {
  const s = await fetchStart(ctx, startOver);
  const cb = await fetchCallback(ctx, { state: s.state, code }, s.cookie);
  const loc = cb.headers.get("location");
  try { return new URL(loc).searchParams.get("code"); } catch { return null; }
}
const exchange = (ctx, body, headers = {}) => call(ctx.url, "POST", "/runtime/oauth/token", { token: null, body, headers });
async function mint(ctx, claims, { alg, forge } = {}) {
  const res = await fetch(`${MOCK}/__mint`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ claims: { iss: ctx.provider, sub: "mock-sub-42", email: "oauth-user@example.com", email_verified: true, aud: "mock-client-id", ...claims }, alg, forge }) });
  return (await res.json()).id_token;
}
const idToken = (ctx, body, headers = {}) => call(ctx.url, "POST", "/runtime/oauth/id_token", { token: null, body: { app_id: appId, client_name: "mock", ...body }, headers });
const sha256b64url = (s) => crypto.createHash("sha256").update(s).digest("base64url");

// ---------------------------------------------------------------------------
// setup: a provider, a confidential client, a public client, the app origin
async function setup(ctx) {
  const prov = await call(ctx.url, "POST", `/dash/apps/${appId}/oauth_service_providers`, { body: { provider_name: "mock" } });
  if (prov.status !== 200) throw new Error(`[${ctx.name}] provider create failed: ${JSON.stringify(prov.body)}`);
  const providerId = prov.body.provider.id;
  const discovery = `${ctx.provider}/.well-known/openid-configuration`;
  for (const client of [
    { client_name: "mock", client_id: "mock-client-id", client_secret: "mock-secret", discovery_endpoint: discovery, meta: {} },
    // no secret: the id_token path ignores the audience and may accept unverified emails
    { client_name: "mock-public", client_id: "mock-public-id", discovery_endpoint: discovery, meta: { allowUnverifiedEmail: true } },
  ]) {
    const c = await call(ctx.url, "POST", `/dash/apps/${appId}/oauth_clients`, { body: { provider_id: providerId, ...client } });
    if (c.status !== 200) throw new Error(`[${ctx.name}] client ${client.client_name} create failed: ${JSON.stringify(c.body)}`);
  }
  const origin = await call(ctx.url, "POST", `/dash/apps/${appId}/authorized_redirect_origins`, { body: { service: "generic", params: ["localhost:5173"] } });
  if (origin.status !== 200) throw new Error(`[${ctx.name}] origin create failed: ${JSON.stringify(origin.body)}`);
}

// ---------------------------------------------------------------------------
// probes
const probes = [];
const probe = (name, run) => probes.push({ name, run });

// start
probe("start/ok", async (ctx) => redirectView((await fetchStart(ctx)).res, ctx));
probe("start/client-id-alias", async (ctx) => redirectView((await fetch(`${ctx.url}/runtime/oauth/start?${qs({ app_id: appId, client_id: "mock", redirect_uri: APP_REDIRECT })}`, { redirect: "manual" })), ctx));
probe("start/with-state-and-hd", async (ctx) => redirectView((await fetchStart(ctx, { state: "app-state-1", hd: "example.com", ignored: "x" })).res, ctx));
probe("start/missing-app", async (ctx) => redirectView(await fetch(`${ctx.url}/runtime/oauth/start?${qs({ client_name: "mock", redirect_uri: APP_REDIRECT })}`, { redirect: "manual" }), ctx));
probe("start/bad-app", async (ctx) => redirectView(await fetch(`${ctx.url}/runtime/oauth/start?${qs({ app_id: "nope", client_name: "mock", redirect_uri: APP_REDIRECT })}`, { redirect: "manual" }), ctx));
probe("start/unknown-client", async (ctx) => redirectView((await fetchStart(ctx, { client_name: "nope" })).res, ctx));
probe("start/missing-client", async (ctx) => redirectView(await fetch(`${ctx.url}/runtime/oauth/start?${qs({ app_id: appId, redirect_uri: APP_REDIRECT })}`, { redirect: "manual" }), ctx));
probe("start/missing-redirect", async (ctx) => redirectView(await fetch(`${ctx.url}/runtime/oauth/start?${qs({ app_id: appId, client_name: "mock" })}`, { redirect: "manual" }), ctx));
probe("start/unauthorized-redirect", async (ctx) => redirectView((await fetchStart(ctx, { redirect_uri: "http://localhost:9999/app" })).res, ctx));
probe("start/app-scoped-pkce", async (ctx) => redirectView(await fetch(`${ctx.url}/runtime/${appId}/oauth/start?${qs({ client_id: "mock", redirect_uri: APP_REDIRECT, state: "s1", code_challenge: sha256b64url("verifier-1"), code_challenge_method: "S256" })}`, { redirect: "manual" }), ctx));
probe("start/app-scoped-bad-app", async (ctx) => redirectView(await fetch(`${ctx.url}/runtime/nope/oauth/start?${qs({ client_id: "mock", redirect_uri: APP_REDIRECT })}`, { redirect: "manual" }), ctx));

// callback
probe("callback/no-params", async (ctx) => redirectView(await fetchCallback(ctx, {}, null), ctx));
probe("callback/error-param", async (ctx) => redirectView(await fetchCallback(ctx, { error: "access_denied", state: "x" }, null), ctx));
probe("callback/bad-state", async (ctx) => redirectView(await fetchCallback(ctx, { state: "abc", code: "mock-code" }, uuid()), ctx));
probe("callback/no-cookie", async (ctx) => {
  const s = await fetchStart(ctx);
  return redirectView(await fetchCallback(ctx, { state: s.state, code: "mock-code" }, null), ctx);
});
probe("callback/bad-cookie", async (ctx) => {
  const s = await fetchStart(ctx);
  return redirectView(await fetchCallback(ctx, { state: s.state, code: "mock-code" }, "nope"), ctx);
});
probe("callback/unknown-state", async (ctx) => redirectView(await fetchCallback(ctx, { state: appId + uuid(), code: "mock-code" }, uuid()), ctx));
probe("callback/mismatch-cookie", async (ctx) => {
  const s = await fetchStart(ctx);
  return redirectView(await fetchCallback(ctx, { state: s.state, code: "mock-code" }, `instantdb_${uuid()}`), ctx);
});
probe("callback/consumed", async (ctx) => {
  const s = await fetchStart(ctx);
  await fetchCallback(ctx, { state: s.state, code: "mock-code" }, `instantdb_${uuid()}`);
  return redirectView(await fetchCallback(ctx, { state: s.state, code: "mock-code" }, s.cookie), ctx);
});
probe("callback/no-code", async (ctx) => {
  const s = await fetchStart(ctx);
  return redirectView(await fetchCallback(ctx, { state: s.state }, s.cookie), ctx);
});
probe("callback/provider-rejects-code", async (ctx) => {
  const s = await fetchStart(ctx, { state: "app-state-2" });
  return redirectView(await fetchCallback(ctx, { state: s.state, code: "bad-code" }, s.cookie), ctx);
});
probe("callback/unverified-email", async (ctx) => {
  const s = await fetchStart(ctx);
  return redirectView(await fetchCallback(ctx, { state: s.state, code: "mock-code-unverified" }, s.cookie), ctx);
});
probe("callback/ok", async (ctx) => {
  const s = await fetchStart(ctx, { state: "app-state-3" });
  return redirectView(await fetchCallback(ctx, { state: s.state, code: "mock-code" }, s.cookie), ctx);
});
probe("callback/ok-form-post", async (ctx) => {
  const s = await fetchStart(ctx);
  return redirectView(await fetchCallback(ctx, { state: s.state, code: "mock-code" }, s.cookie, { method: "POST" }), ctx);
});
probe("callback/test-redirect-page", async (ctx) => redirectView(await fetchCallback(ctx, { "test-redirect": "true" }, null), ctx));

// token
probe("token/ok", async (ctx) => userView(await exchange(ctx, { app_id: appId, code: await appCode(ctx) }), ctx));
probe("token/second-login-same-user", async (ctx) => {
  const first = await exchange(ctx, { app_id: appId, code: await appCode(ctx) });
  const second = await exchange(ctx, { app_id: appId, code: await appCode(ctx) });
  return { first: userView(first, ctx), second: userView(second, ctx), sameUser: first.body?.user?.id === second.body?.user?.id };
});
probe("token/double-exchange", async (ctx) => {
  const code = await appCode(ctx);
  await exchange(ctx, { app_id: appId, code });
  return errView(await exchange(ctx, { app_id: appId, code }), ctx);
});
probe("token/missing-code", async (ctx) => errView(await exchange(ctx, { app_id: appId }), ctx));
probe("token/bad-code", async (ctx) => errView(await exchange(ctx, { app_id: appId, code: "nope" }), ctx));
probe("token/unknown-code", async (ctx) => errView(await exchange(ctx, { app_id: appId, code: uuid() }), ctx));
probe("token/wrong-app", async (ctx) => errView(await exchange(ctx, { app_id: uuid(), code: await appCode(ctx) }), ctx));
probe("token/missing-app", async (ctx) => errView(await exchange(ctx, { code: await appCode(ctx) }), ctx));
probe("token/origin-unauthorized", async (ctx) => errView(await exchange(ctx, { app_id: appId, code: await appCode(ctx) }, { origin: "https://evil.example" }), ctx));
probe("token/origin-authorized", async (ctx) => userView(await exchange(ctx, { app_id: appId, code: await appCode(ctx) }, { origin: APP_ORIGIN }), ctx));
probe("token/form-encoded", async (ctx) => userView(await call(ctx.url, "POST", "/runtime/oauth/token", { token: null, form: { app_id: appId, code: await appCode(ctx) } }), ctx));
probe("token/query-params", async (ctx) => userView(await call(ctx.url, "POST", `/runtime/oauth/token?${qs({ app_id: appId, code: await appCode(ctx) })}`, { token: null }), ctx));
probe("token/guest-upgrade", async (ctx) => {
  const guest = await call(ctx.url, "POST", "/runtime/auth/sign_in_guest", { token: null, body: { "app-id": appId } });
  const res = await exchange(ctx, { app_id: appId, code: await appCode(ctx), refresh_token: guest.body?.refresh_token });
  return { view: userView(res, ctx), keptGuestId: res.body?.user?.id === guest.body?.user?.id };
});
probe("token/extra-fields-unknown", async (ctx) => errView(await exchange(ctx, { app_id: appId, code: await appCode(ctx), extra_fields: { nope: 1 } }), ctx));
// PKCE through the app-scoped routes (the openid-configuration flow)
async function pkceCode(ctx, verifier, method = "S256") {
  const challenge = method === "S256" ? sha256b64url(verifier) : verifier;
  const res = await fetch(`${ctx.url}/runtime/${appId}/oauth/start?${qs({ client_id: "mock", redirect_uri: APP_REDIRECT, state: "pk", code_challenge: challenge, code_challenge_method: method })}`, { redirect: "manual" });
  const state = new URL(res.headers.get("location") ?? "http://x/").searchParams.get("state");
  const cookie = /__session=([^;]+)/.exec(res.headers.get("set-cookie") ?? "")?.[1] ?? null;
  const cb = await fetchCallback(ctx, { state, code: "mock-code" }, cookie);
  try { return new URL(cb.headers.get("location")).searchParams.get("code"); } catch { return null; }
}
probe("token/pkce-ok", async (ctx) => userView(await call(ctx.url, "POST", `/runtime/${appId}/oauth/token`, { token: null, form: { code: await pkceCode(ctx, "verifier-ok"), code_verifier: "verifier-ok" } }), ctx));
probe("token/pkce-plain", async (ctx) => userView(await call(ctx.url, "POST", `/runtime/${appId}/oauth/token`, { token: null, form: { code: await pkceCode(ctx, "verifier-plain", "plain"), code_verifier: "verifier-plain" } }), ctx));
probe("token/pkce-wrong-verifier", async (ctx) => errView(await call(ctx.url, "POST", `/runtime/${appId}/oauth/token`, { token: null, form: { code: await pkceCode(ctx, "verifier-2"), code_verifier: "wrong" } }), ctx));
probe("token/pkce-missing-verifier", async (ctx) => errView(await call(ctx.url, "POST", `/runtime/${appId}/oauth/token`, { token: null, form: { code: await pkceCode(ctx, "verifier-3") } }), ctx));
probe("token/pkce-verifier-without-challenge", async (ctx) => userView(await exchange(ctx, { app_id: appId, code: await appCode(ctx), code_verifier: "unexpected" }), ctx));

// id_token
probe("id-token/ok", async (ctx) => userView(await idToken(ctx, { id_token: await mint(ctx, { nonce: "n1" }), nonce: "n1" }), ctx));
probe("id-token/ok-without-nonce", async (ctx) => userView(await idToken(ctx, { id_token: await mint(ctx, {}) }), ctx));
probe("id-token/hashed-nonce", async (ctx) => userView(await idToken(ctx, { id_token: await mint(ctx, { nonce: crypto.createHash("sha256").update("n2").digest("hex") }), nonce: "n2" }), ctx));
probe("id-token/nonce-missing-in-token", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, {}), nonce: "n3" }), ctx));
probe("id-token/nonce-missing-in-request", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, { nonce: "n4" }) }), ctx));
probe("id-token/nonce-mismatch", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, { nonce: "n5" }), nonce: "other" }), ctx));
probe("id-token/wrong-audience", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, { aud: "someone-else" }) }), ctx));
probe("id-token/wrong-issuer", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, { iss: "https://evil.example" }) }), ctx));
probe("id-token/no-subject", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, { sub: null }) }), ctx));
probe("id-token/unsupported-alg", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, {}, { alg: "RS512" }) }), ctx));
probe("id-token/forged-signature", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, {}, { forge: true }) }), ctx));
probe("id-token/garbage", async (ctx) => errView(await idToken(ctx, { id_token: "not.a.jwt" }), ctx));
probe("id-token/missing", async (ctx) => errView(await idToken(ctx, {}), ctx));
probe("id-token/unknown-client", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, {}), client_name: "nope" }), ctx));
probe("id-token/missing-app", async (ctx) => errView(await call(ctx.url, "POST", "/runtime/oauth/id_token", { token: null, body: { client_name: "mock", id_token: await mint(ctx, {}) } }), ctx));
probe("id-token/origin-unauthorized", async (ctx) => errView(await idToken(ctx, { id_token: await mint(ctx, {}) }, { origin: "https://evil.example" }), ctx));
probe("id-token/origin-authorized", async (ctx) => userView(await idToken(ctx, { id_token: await mint(ctx, {}) }, { origin: APP_ORIGIN }), ctx));
probe("id-token/unverified-email-confidential", async (ctx) => userView(await idToken(ctx, { id_token: await mint(ctx, { email_verified: false, sub: "mock-sub-unv", email: "unverified@example.com" }) }), ctx));
probe("id-token/public-client-unverified-email", async (ctx) => userView(await idToken(ctx, { client_name: "mock-public", id_token: await mint(ctx, { aud: "anything", email_verified: false, sub: "mock-sub-pub", email: "public@example.com" }) }), ctx));
probe("id-token/reuses-refresh-token", async (ctx) => {
  const first = await idToken(ctx, { id_token: await mint(ctx, {}) });
  const second = await idToken(ctx, { id_token: await mint(ctx, {}), refresh_token: first.body?.user?.refresh_token });
  return { first: userView(first, ctx), second: userView(second, ctx), sameToken: first.body?.user?.refresh_token === second.body?.user?.refresh_token };
});
probe("id-token/guest-upgrade", async (ctx) => {
  const guest = await call(ctx.url, "POST", "/runtime/auth/sign_in_guest", { token: null, body: { "app-id": appId } });
  const res = await idToken(ctx, { id_token: await mint(ctx, { sub: "mock-sub-guest", email: "guest-upgrade@example.com" }), refresh_token: guest.body?.refresh_token });
  return { view: userView(res, ctx), keptGuestId: res.body?.user?.id === guest.body?.user?.id };
});

// misc
probe("openid-configuration/ok", async (ctx) => {
  const res = await call(ctx.url, "GET", `/runtime/${appId}/.well-known/openid-configuration`, { token: null });
  return res.status === 200 ? norm({ status: 200, body: res.body }, ctx) : errView(res, ctx);
});
probe("openid-configuration/bad-app", async (ctx) => errView(await call(ctx.url, "GET", "/runtime/nope/.well-known/openid-configuration", { token: null }), ctx));
probe("signout/ok", async (ctx) => {
  const login = await exchange(ctx, { app_id: appId, code: await appCode(ctx) });
  const token = login.body?.refresh_token;
  const out = await call(ctx.url, "POST", "/runtime/signout", { token: null, body: { app_id: appId, refresh_token: token } });
  const after = await call(ctx.url, "POST", "/runtime/auth/verify_refresh_token", { token: null, body: { "app-id": appId, "refresh-token": token } });
  return { signout: out.status === 200 ? norm({ status: 200, body: out.body }, ctx) : errView(out, ctx), after: errView(after, ctx) };
});
probe("signout/unknown-token", async (ctx) => {
  const out = await call(ctx.url, "POST", "/runtime/signout", { token: null, body: { app_id: appId, refresh_token: uuid() } });
  return out.status === 200 ? norm({ status: 200, body: out.body }, ctx) : errView(out, ctx);
});
probe("signout/missing-token", async (ctx) => errView(await call(ctx.url, "POST", "/runtime/signout", { token: null, body: { app_id: appId } }), ctx));
probe("signout/bad-app", async (ctx) => errView(await call(ctx.url, "POST", "/runtime/signout", { token: null, body: { app_id: "nope", refresh_token: uuid() } }), ctx));

// ---------------------------------------------------------------------------
const allowed = fs.existsSync(path.join(here, "oauth-allowed.json"))
  ? JSON.parse(fs.readFileSync(path.join(here, "oauth-allowed.json"), "utf8"))
  : [];
const results = {};
for (const [name, cfg] of Object.entries(SERVERS)) {
  const ctx = { name, ...cfg };
  await setup(ctx);
  results[name] = {};
  for (const p of probes) {
    try {
      results[name][p.name] = await p.run(ctx);
    } catch (e) {
      results[name][p.name] = { threw: String(e.message ?? e) };
    }
    if (process.env.DUMP) console.log(`[${name}] ${p.name}: ${JSON.stringify(results[name][p.name])}`);
  }
}
let failures = 0;
let allowedHits = 0;
for (const p of probes) {
  const l = results.legacy[p.name];
  const r = results.rust[p.name];
  if (canon(l) === canon(r)) continue;
  const rule = allowed.find((a) => new RegExp(a.probe).test(p.name));
  if (rule) {
    allowedHits++;
    console.log(`allowed divergence ${p.name}: ${rule.reason.split(".")[0]}.`);
    continue;
  }
  failures++;
  console.error(`OAUTH MISMATCH ${p.name}\n  legacy: ${JSON.stringify(l)}\n  rust:   ${JSON.stringify(r)}`);
}
if (failures) {
  console.error(`OAUTH DIFFERENTIAL FAILED: ${failures} of ${probes.length} probes differ`);
  process.exit(1);
}
console.log(`OAUTH DIFFERENTIAL PASSED: ${probes.length} probes match (${allowedHits} allowed divergences)`);
