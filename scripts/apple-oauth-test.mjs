// Apple sign-in with a mock Apple (discovery / authorize / token / JWKS), the
// paths the differential harness can't reach without real Apple
// credentials: legacy's Apple client-secret JWT (auth/jwt.clj:129-147,
// auth/oauth.clj:139-147), the Apple issuer quirk and the hashed-nonce
// acceptance on /oauth/id_token (auth/oauth.clj:215-263).
// Apple-ness follows the discovery document's `issuer`, as in legacy, so the
// mock serves Apple's issuers from localhost (the server must run with
// INSTANT_OAUTH_ALLOW_PRIVATE=1).
// Usage: node apple-oauth-test.mjs <app-id> <admin-token>  (a fresh app: the
// test adds the `apple` provider, its clients and the localhost:5173 origin)
import http from "node:http";
import crypto from "node:crypto";

const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken) throw new Error("usage: node apple-oauth-test.mjs <app-id> <admin-token>");
const SERVER = process.env.SERVER_URL ?? "http://localhost:8888";
const PORT = 9380;
const MOCK = `http://localhost:${PORT}`;
const APP_URL = "http://localhost:5173/app";
const APPLEID = "https://appleid.apple.com";
const ACCOUNT = "https://account.apple.com";
const SERVICES_ID = "com.example.web";
const TEAM_ID = "TEAM123456";
const KEY_ID = "KEY1234567";
const SAFARI_UA = /Version\/17\.6 Safari/;

const assert = (cond, msg) => {
  if (!cond) throw new Error("ASSERT FAILED: " + msg);
  console.log("ok:", msg);
};

// the app's Sign in with Apple key (.p8 = PKCS#8 PEM, P-256)
const appleKey = crypto.generateKeyPairSync("ec", { namedCurve: "prime256v1" });
const p8 = appleKey.privateKey.export({ format: "pem", type: "pkcs8" });
// Apple's id_token signing keys; the EC one is listed but Apple's discovery
// document only allows RS256
const rsa = crypto.generateKeyPairSync("rsa", { modulusLength: 2048 });
const ec = crypto.generateKeyPairSync("ec", { namedCurve: "prime256v1" });
const jwks = {
  keys: [
    { ...rsa.publicKey.export({ format: "jwk" }), kid: "apple-rsa", use: "sig", alg: "RS256" },
    { ...ec.publicKey.export({ format: "jwk" }), kid: "apple-ec", use: "sig", alg: "ES256" },
  ],
};
const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
const unb64 = (s) => JSON.parse(Buffer.from(s, "base64url").toString());
const now = () => Math.floor(Date.now() / 1000);
const signRs256 = (claims, kid = "apple-rsa") => {
  const input = `${b64({ alg: "RS256", typ: "JWT", kid })}.${b64(claims)}`;
  return `${input}.${crypto.sign("sha256", Buffer.from(input), rsa.privateKey).toString("base64url")}`;
};
const signEs256 = (claims) => {
  const input = `${b64({ alg: "ES256", typ: "JWT", kid: "apple-ec" })}.${b64(claims)}`;
  return `${input}.${crypto.sign("sha256", Buffer.from(input), { key: ec.privateKey, dsaEncoding: "ieee-p1363" }).toString("base64url")}`;
};
const appleClaims = (over = {}) => ({
  iss: APPLEID,
  aud: SERVICES_ID,
  sub: "apple-sub-1",
  email: "apple-user@privaterelay.appleid.com",
  // Apple sends this as a string or a boolean
  email_verified: "true",
  iat: now(),
  exp: now() + 600,
  ...over,
});

// --- mock Apple ---
// /.well-known/... is appleid.apple.com's document, /account/... account.apple.com's
// (served to browsers only), /plain/... a non-Apple OIDC provider
const discovery = (prefix, issuer) => ({
  issuer,
  authorization_endpoint: `${MOCK}${prefix}/auth/authorize`,
  token_endpoint: `${MOCK}${prefix}/auth/token`,
  jwks_uri: `${MOCK}/auth/keys`,
  response_modes_supported: ["query", "fragment", "form_post"],
  id_token_signing_alg_values_supported: ["RS256"],
  token_endpoint_auth_methods_supported: ["client_secret_post"],
});
const seen = { secrets: [], redirectUri: null };
// what the token endpoint makes of an Apple client secret
const checkSecret = (secret) => {
  const parts = (secret ?? "").split(".");
  if (parts.length !== 3) return { ok: false, why: "not a JWT" };
  const header = unb64(parts[0]);
  const claims = unb64(parts[1]);
  const valid = crypto.verify(
    "sha256",
    Buffer.from(`${parts[0]}.${parts[1]}`),
    { key: appleKey.publicKey, dsaEncoding: "ieee-p1363" },
    Buffer.from(parts[2], "base64url"),
  );
  return { ok: valid && header.kid === KEY_ID && claims.iss === TEAM_ID, valid, header, claims };
};
const provider = http.createServer((req, res) => {
  const url = new URL(req.url, MOCK);
  const send = (status, body) => {
    res.statusCode = status;
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(body));
  };
  if (url.pathname === "/.well-known/openid-configuration") return send(200, discovery("", APPLEID));
  if (url.pathname === "/account/.well-known/openid-configuration") {
    // legacy fetch-discovery sends a browser User-Agent for this one
    if (!SAFARI_UA.test(req.headers["user-agent"] ?? "")) return send(403, { error: "browser required" });
    return send(200, discovery("/account", ACCOUNT));
  }
  if (url.pathname === "/plain/.well-known/openid-configuration") return send(200, discovery("/plain", "https://oidc.example.com"));
  if (url.pathname === "/auth/keys") return send(200, jwks);
  if (req.method === "POST" && url.pathname.endsWith("/auth/token")) {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      const p = new URLSearchParams(body);
      const code = p.get("code");
      if (url.pathname === "/plain/auth/token") {
        seen.secrets.push({ plain: p.get("client_secret") });
        if (p.get("client_secret") !== "plain-secret") return send(400, { error: "invalid_client" });
      } else {
        const check = checkSecret(p.get("client_secret"));
        seen.secrets.push(check);
        // Apple answers a bad client secret with a bare invalid_client
        if (!check.ok || check.claims.sub !== p.get("client_id")) return send(400, { error: "invalid_client" });
      }
      if (p.get("grant_type") !== "authorization_code") return send(400, { error: "unsupported_grant_type" });
      if (seen.redirectUri && p.get("redirect_uri") !== seen.redirectUri) return send(400, { error_description: `redirect_uri ${p.get("redirect_uri")}` });
      if (code === "code-expired") return send(400, { error: "invalid_grant", error_description: "The code has expired or has been revoked." });
      if (code === "code-unverified") return send(200, { id_token: signRs256(appleClaims({ sub: "apple-sub-2", email_verified: false })) });
      if (code === "code-noemail") return send(200, { id_token: signRs256(appleClaims({ sub: "apple-sub-3", email: undefined })) });
      if (code === "code-garbage") return send(200, { id_token: "not-a-jwt", access_token: "at" });
      send(200, {
        access_token: "at",
        token_type: "Bearer",
        expires_in: 3600,
        refresh_token: "rt",
        id_token: signRs256(appleClaims()),
      });
    });
    return;
  }
  send(404, { error: "not found" });
});
await new Promise((r) => provider.listen(PORT, r));

const json = async (r) => ({ status: r.status, body: await r.json().catch(() => null) });
const dash = (method, path, body) =>
  fetch(`${SERVER}${path}`, {
    method,
    headers: { authorization: `Bearer ${adminToken}`, "content-type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  }).then(json);

// --- provision through the dashboard routes: an `apple` provider is an OIDC
// provider with Apple's discovery endpoint plus meta teamId / keyId ---
const prov = await dash("POST", `/dash/apps/${appId}/oauth_service_providers`, { provider_name: "apple" });
assert(prov.status === 200 && prov.body.provider?.provider_name === "apple", "apple provider created");
const addClient = (name, extra) =>
  dash("POST", `/dash/apps/${appId}/oauth_clients`, {
    provider_id: prov.body.provider.id,
    client_name: name,
    client_id: SERVICES_ID,
    discovery_endpoint: `${MOCK}/.well-known/openid-configuration`,
    meta: { providerName: "apple", teamId: TEAM_ID, keyId: KEY_ID },
    ...extra,
  });
const clients = {
  web: await addClient("apple-web", { client_secret: p8 }),
  account: await addClient("apple-account", { client_secret: p8, discovery_endpoint: `${MOCK}/account/.well-known/openid-configuration` }),
  noKey: await addClient("apple-nokey", {}),
  badKey: await addClient("apple-badkey", { client_secret: "not a pem" }),
  rsaKey: await addClient("apple-rsakey", { client_secret: rsa.privateKey.export({ format: "pem", type: "pkcs8" }) }),
  noIds: await addClient("apple-noids", { client_secret: p8, meta: { providerName: "apple" } }),
  plain: await addClient("plain-oidc", { client_secret: "plain-secret", discovery_endpoint: `${MOCK}/plain/.well-known/openid-configuration` }),
  // a native (secret-less) client: no audience check, allowUnverifiedEmail honored
  native: await addClient("apple-native", { client_id: "com.example.ios", meta: { allowUnverifiedEmail: true } }),
};
for (const [k, c] of Object.entries(clients)) assert(c.status === 200, `client ${k} created (${c.status} ${JSON.stringify(c.body).slice(0, 200)})`);
assert(clients.account.body.client.discovery_endpoint.includes("/account/"), "account.apple.com's discovery document loads (browser User-Agent)");
const origin = await dash("POST", `/dash/apps/${appId}/authorized_redirect_origins`, { service: "generic", params: ["localhost:5173"] });
assert(origin.status === 200, "localhost:5173 is an authorized redirect origin");

// --- start: Apple's authorize URL with legacy's OIDC params ---
const start = async (client) => {
  const r = await fetch(`${SERVER}/runtime/oauth/start?app_id=${appId}&client_name=${client}&redirect_uri=${encodeURIComponent(APP_URL)}&state=appstate`, { redirect: "manual" });
  const loc = new URL(r.headers.get("location"));
  return { status: r.status, loc, state: loc.searchParams.get("state"), cookie: r.headers.get("set-cookie")?.split(";")[0] };
};
const s0 = await start("apple-web");
assert(s0.status === 302 && s0.loc.origin + s0.loc.pathname === `${MOCK}/auth/authorize`, "start redirects to Apple's authorize endpoint");
assert(s0.loc.searchParams.get("response_mode") === "form_post" && s0.loc.searchParams.get("scope") === "email openid", "form_post with the email scope (Apple requires form_post for it)");
assert(s0.loc.searchParams.get("client_id") === SERVICES_ID, "the Services ID is the client_id");
seen.redirectUri = s0.loc.searchParams.get("redirect_uri");

// Apple posts the callback (response_mode=form_post)
const callback = async (s, code) => {
  const r = await fetch(`${SERVER}/runtime/oauth/callback`, {
    method: "POST",
    redirect: "manual",
    headers: { "content-type": "application/x-www-form-urlencoded", cookie: s.cookie },
    body: new URLSearchParams({ state: s.state, code, user: JSON.stringify({ name: { firstName: "A", lastName: "B" } }) }),
  });
  const loc = r.headers.get("location") ? new URL(r.headers.get("location")) : null;
  return { status: r.status, loc, error: loc?.searchParams.get("error") ?? null, code: loc?.searchParams.get("code") ?? null, body: loc ? null : await r.text() };
};
const tokenExchange = (code) =>
  fetch(`${SERVER}/runtime/oauth/token`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ app_id: appId, code }),
  }).then(json);

// --- the code exchange signs a client-secret JWT with the app's key ---
const cb1 = await callback(s0, "code-ok");
assert(cb1.status === 302 && cb1.loc.origin + cb1.loc.pathname === APP_URL && cb1.code, `callback redirects back to the app with a code (${cb1.error ?? cb1.body ?? ""})`);
const secret = seen.secrets.at(-1);
assert(secret.valid, "the client secret is an ES256 JWT signed with the .p8 key");
assert(secret.header.alg === "ES256" && secret.header.kid === KEY_ID, "header: alg ES256, kid = meta keyId");
assert(secret.claims.iss === TEAM_ID && secret.claims.sub === SERVICES_ID, "claims: iss = meta teamId, sub = the client id");
assert(secret.claims.aud === APPLEID, "claims: aud = https://appleid.apple.com");
assert(secret.claims.exp - secret.claims.iat === 120 && Math.abs(secret.claims.iat - now()) < 60, "claims: valid for two minutes from now");
const first = await tokenExchange(cb1.code);
assert(first.status === 200 && first.body.created === true, "first Apple sign-in creates the user");
assert(first.body.user.email === "apple-user@privaterelay.appleid.com", "the email Apple marks verified (as the string \"true\") is the user's");

// account.apple.com's issuer gets the same client secret
const sAcc = await start("apple-account");
assert(sAcc.loc.origin + sAcc.loc.pathname === `${MOCK}/account/auth/authorize`, "account.apple.com client starts at its own authorize endpoint");
const cbAcc = await callback(sAcc, "code-ok");
assert(cbAcc.code && seen.secrets.at(-1).ok, "account.apple.com's token exchange also gets the signed client secret");
const again = await tokenExchange(cbAcc.code);
assert(again.status === 200 && again.body.created === false && again.body.user.id === first.body.user.id, "the same Apple sub signs the same user in");

// a non-Apple issuer still sends the stored secret verbatim
const cbPlain = await callback(await start("plain-oidc"), "code-ok");
assert(cbPlain.code && seen.secrets.at(-1).plain === "plain-secret", "a non-Apple OIDC client's client_secret is the stored secret");

// --- exchange failures redirect back with legacy's messages ---
const failWith = async (client, code, expected, label) => {
  const before = seen.secrets.length;
  const cb = await callback(await start(client), code);
  assert(cb.status === 302 && cb.loc.origin + cb.loc.pathname === APP_URL && cb.error === expected, `${label}: redirects back with error=${JSON.stringify(expected)} (got ${JSON.stringify(cb.error ?? cb.body)})`);
  assert(cb.loc.searchParams.get("_instant_oauth_redirect") === "true" && cb.loc.searchParams.get("state") === "appstate", `${label}: keeps the app's state and the oauth marker`);
  return seen.secrets.length - before;
};
await failWith("apple-web", "code-expired", "The code has expired or has been revoked.", "Apple's error_description");
await failWith("apple-web", "code-unverified", "Missing user info", "an unverified email");
await failWith("apple-web", "code-noemail", "Missing user info", "no email");
await failWith("apple-web", "code-garbage", "Invalid token exchanging code for token.", "an undecodable id_token and no userinfo endpoint");
// legacy sends a client secret without iss / kid and Apple refuses it
assert((await failWith("apple-noids", "code-ok", "Error exchanging code for token.", "no teamId / keyId")) === 1, "no teamId / keyId: the token request still goes out");
const noIds = seen.secrets.at(-1);
assert(noIds.valid && noIds.header.kid === undefined && noIds.claims.iss === undefined, "no teamId / keyId: the JWT carries neither (legacy)");
// the client secret can't be built: no request reaches Apple (legacy 500s; see docs/PARITY.md)
const missingKey = "Missing Apple private key: set the client secret to the contents of your .p8 key file.";
const invalidKey = "Invalid Apple private key: the client secret must be the contents of your .p8 key file.";
assert((await failWith("apple-nokey", "code-ok", missingKey, "no private key")) === 0, "no private key: nothing is sent to Apple");
assert((await failWith("apple-badkey", "code-ok", invalidKey, "a secret that isn't PEM")) === 0, "a non-PEM secret: nothing is sent to Apple");
assert((await failWith("apple-rsakey", "code-ok", invalidKey, "an RSA key")) === 0, "an RSA key: nothing is sent to Apple");

// --- /oauth/id_token (native Sign in with Apple) ---
const idToken = (body, client = "apple-web") =>
  fetch(`${SERVER}/runtime/oauth/id_token`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ app_id: appId, client_name: client, ...body }),
  }).then(json);
const refused = (r, msg, label, jwt) => {
  assert(r.status === 400 && r.body?.type === "validation-failed" && r.body.message === `Validation failed for id_token: ${msg}`, `${label} (got ${r.status} ${JSON.stringify(r.body?.message ?? r.body?.error)})`);
  if (jwt) assert(r.body.hint?.["data-type"] === "id_token" && r.body.hint.input === jwt && r.body.hint.errors?.[0]?.message === msg, `${label}: legacy hint {data-type, input, errors}`);
};
const oauthErr = (r, msg, label) => assert(r.status === 400 && r.body?.type === "oauth-error" && r.body.error === msg, `${label} (got ${r.status} ${JSON.stringify(r.body)})`);

// the issuer quirk: either Apple host matches the other
const viaAccount = await idToken({ id_token: signRs256(appleClaims({ iss: ACCOUNT })) });
assert(viaAccount.status === 200 && viaAccount.body.user.id === first.body.user.id, "an account.apple.com token signs in through an appleid.apple.com client");
const viaAppleid = await idToken({ id_token: signRs256(appleClaims()) }, "apple-account");
assert(viaAppleid.status === 200 && viaAppleid.body.user.id === first.body.user.id, "an appleid.apple.com token signs in through an account.apple.com client");
const evilJwt = signRs256(appleClaims({ iss: "https://evil.example.com" }));
refused(await idToken({ id_token: evilJwt }), `The id_token wasn't issued by ${APPLEID}.`, "another issuer is refused", evilJwt);
const plainJwt = signRs256(appleClaims({ iss: APPLEID, aud: SERVICES_ID }));
refused(await idToken({ id_token: plainJwt }, "plain-oidc"), "The id_token wasn't issued by https://oidc.example.com.", "the quirk is Apple-only");

// nonces: raw, or the SHA-256 hex of it (react-native-apple-authentication)
const sha = (s) => crypto.createHash("sha256").update(s).digest("hex");
assert((await idToken({ id_token: signRs256(appleClaims({ nonce: "n-raw" })), nonce: "n-raw" })).status === 200, "the request's nonce matches");
assert((await idToken({ id_token: signRs256(appleClaims({ nonce: sha("n-raw") })), nonce: "n-raw" })).status === 200, "the SHA-256 hex of the request's nonce matches");
refused(await idToken({ id_token: signRs256(appleClaims({ nonce: sha("other") })), nonce: "n-raw" }), "The nonces do not match.", "a different nonce");
refused(await idToken({ id_token: signRs256(appleClaims()), nonce: "n-raw" }), "The id_token is missing a nonce.", "no nonce in the token");
refused(await idToken({ id_token: signRs256(appleClaims({ nonce: "n-raw" })) }), "The nonce parameter was not provided in the request.", "no nonce in the request");
// legacy checks the nonce before the issuer
refused(await idToken({ id_token: signRs256(appleClaims({ iss: "https://evil.example.com", nonce: "x" })), nonce: "y" }), "The nonces do not match.", "the nonce error wins over the issuer's");

refused(await idToken({ id_token: signRs256(appleClaims({ aud: "com.example.other" })) }), "The id_token was generated for the wrong OAuth client.", "another audience");
refused(await idToken({ id_token: signRs256(appleClaims({ sub: undefined })) }), "The id_token had no subject.", "no subject");
// ES256 verifies against Apple's EC key, then fails the discovery document's RS256-only list
refused(await idToken({ id_token: signEs256(appleClaims()) }), "The id_token used an unsupported algorithm.", "an algorithm Apple's discovery document doesn't list");
oauthErr(await idToken({ id_token: signRs256(appleClaims({ exp: now() - 5 })) }), "Error validating JWT. Token is expired.", "an expired token");
oauthErr(await idToken({ id_token: signRs256(appleClaims(), "nope") }), "Error validating JWT. Could not find signing key.", "an unknown kid");
oauthErr(await idToken({ id_token: signRs256(appleClaims()).replace(/\.[^.]+$/, ".AAAA") }), "Error validating JWT. Signature is invalid.", "a bad signature");
oauthErr(await idToken({ id_token: "x.y.z" }), "Invalid JWT.", "a malformed token");
oauthErr(await idToken({ id_token: `${b64({ alg: "HS256", typ: "JWT" })}.${b64(appleClaims())}.sig` }), "Unsupported signing algorithm HS256", "HS256");

// a native client: no audience check; allowUnverifiedEmail keeps an unverified email
const native = await idToken({ id_token: signRs256(appleClaims({ aud: "com.example.ios", sub: "apple-sub-native", email: "native@example.com", email_verified: false })) }, "apple-native");
assert(native.status === 200 && native.body.user.email === "native@example.com", "a secret-less client honors allowUnverifiedEmail");
const webUnverified = await idToken({ id_token: signRs256(appleClaims({ sub: "apple-sub-web-unverified", email: "unverified@example.com", email_verified: false })) });
assert(webUnverified.status === 200 && !webUnverified.body.user.email, "an unverified email is dropped on a client with a secret");

provider.close();
console.log("apple oauth test passed");
