// Dashboard Google login e2e against this server with a mock Google token
// endpoint, plus the get-a-db app creation, track-import and the
// active-session stats.
//
// Usage: node scripts/dash-login-test.mjs
// Env: SERVER_URL (default http://localhost:8888), PROVIDER_PORT (9378),
//      DATABASE_URL (psql; seeds the get-a-db service user + its PAT).
//      The server must run with
//        INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_ID=mock-google-client
//        INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_SECRET=mock-google-secret
//        INSTANT_DASHBOARD_GOOGLE_OAUTH_AUTH_URL=http://localhost:9378/authorize
//        INSTANT_DASHBOARD_GOOGLE_OAUTH_TOKEN_URL=http://localhost:9378/token

import http from "node:http";
import { createHash, randomBytes } from "node:crypto";
import { execFileSync } from "node:child_process";

const base = process.env.SERVER_URL || "http://localhost:8888";
const port = Number(process.env.PROVIDER_PORT || 9378);
const dbUrl = process.env.DATABASE_URL || "postgres://instant:instant@localhost:5432/instant";

let failures = 0;
const check = (name, ok, detail) => {
  console.log(`${ok ? "ok  " : "FAIL"} ${name}${ok ? "" : ` ${JSON.stringify(detail)?.slice(0, 500)}`}`);
  if (!ok) failures++;
};

async function call(method, p, { token, body, headers = {} } = {}) {
  const res = await fetch(base + p, {
    method,
    headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}), "content-type": "application/json", ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = text; }
  return { status: res.status, body: json };
}

// --- the mock Google token endpoint: what it says is scripted per test ---
const google = { status: 200, claims: { sub: "google-sub-1", email: "Login.User@Example.com", email_verified: true }, lastForm: null };
const provider = http.createServer((req, res) => {
  if (req.url !== "/token") { res.writeHead(404); res.end(); return; }
  let data = "";
  req.on("data", (c) => (data += c));
  req.on("end", () => {
    google.lastForm = Object.fromEntries(new URLSearchParams(data));
    res.writeHead(google.status, { "content-type": "application/json" });
    if (google.status !== 200) { res.end(JSON.stringify({ error: "invalid_grant", error_description: "Malformed auth code" })); return; }
    const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
    res.end(JSON.stringify({ id_token: `${b64({ alg: "RS256" })}.${b64(google.claims)}.sig`, access_token: "at" }));
  });
});
await new Promise((r) => provider.listen(port, "127.0.0.1", r));

const cookieOf = (res) => /__session=([^;]+)/.exec(res.headers.get("set-cookie") ?? "")?.[1];
const start = async (qs = "") => {
  const res = await fetch(`${base}/dash/oauth/start${qs}`, { redirect: "manual" });
  const loc = new URL(res.headers.get("location") ?? "http://x/");
  return { res, loc, state: loc.searchParams.get("state"), cookie: cookieOf(res) };
};
const callback = async (qs, cookie) => {
  const res = await fetch(`${base}/dash/oauth/callback${qs}`, { redirect: "manual", headers: cookie ? { cookie: `__session=${cookie}` } : {} });
  const loc = res.headers.get("location") ? new URL(res.headers.get("location")) : null;
  return { status: res.status, loc, params: loc ? Object.fromEntries(loc.searchParams) : null, contentType: res.headers.get("content-type") };
};

// --- start (the ticket is a registered CLI login, as `instant-cli login` does) ---
const reg = await call("POST", "/dash/cli/auth/register");
check("cli register", reg.status === 200 && /^[0-9a-f-]{36}$/.test(reg.body.ticket) && /^[0-9a-f-]{36}$/.test(reg.body.secret), reg);
const ticket = reg.body.ticket;
check("cli check waits for a user", (await call("POST", "/dash/cli/auth/check", { body: { secret: reg.body.secret } })).body.hint?.errors?.[0]?.issue === "waiting-for-user");
const s1 = await start(`?redirect_path=apps&ticket=${ticket}`);
check("start redirects to the auth url", s1.res.status === 302 && s1.loc.origin + s1.loc.pathname === `http://localhost:${port}/authorize`, s1.loc.href);
check("start params", s1.loc.searchParams.get("scope") === "email" && s1.loc.searchParams.get("response_type") === "code" && s1.loc.searchParams.get("redirect_uri") === `${base}/dash/oauth/callback` && s1.loc.searchParams.get("client_id") === "mock-google-client" && /^[0-9a-f-]{36}$/.test(s1.state), s1.loc.search);
check("start param order like legacy", s1.loc.search.startsWith("?scope=email&response_type=code&state=") && s1.loc.search.includes("&redirect_uri=http%3A%2F%2F") && s1.loc.search.endsWith("&client_id=mock-google-client"), s1.loc.search);
const cookieHeader = s1.res.headers.get("set-cookie") ?? "";
check("start cookie", /^__session=[0-9a-f-]{36}; HttpOnly; Expires=.*GMT; Path=\/dash\/oauth; SameSite=Lax$/.test(cookieHeader), cookieHeader);
check("start has no body", (await s1.res.text()) === "" && !s1.res.headers.get("content-type"));

// --- callback error paths ---
check("callback without params", (await callback("")).params?.error === "Missing state param in OAuth redirect.");
check("callback error param", (await callback("?error=access_denied")).params?.error === "Error from Google: access_denied");
check("callback without cookie", (await callback(`?state=${s1.state}`)).params?.error === "Missing cookie.");
check("callback bad state", (await callback("?state=nope", s1.cookie)).params?.error === "Invalid state param in OAuth redirect.");
check("callback bad cookie", (await callback(`?state=${s1.state}`, "nope")).params?.error === "Invalid cookie.");
// a callback with a valid state + cookie consumes the redirect before the
// code check (legacy's side-effect order), so this one gets its own start
const s0 = await start();
check("callback without code", (await callback(`?state=${s0.state}`, s0.cookie)).params?.error === "Missing code param in OAuth redirect.");
check("that callback consumed the redirect", (await callback(`?state=${s0.state}&code=mock-code`, s0.cookie)).params?.error === "Could not find OAuth request.");
check("callback unknown state", (await callback(`?state=${crypto.randomUUID()}&code=x`, s1.cookie)).params?.error === "Could not find OAuth request.");
const cb1 = await callback(`?state=${s1.state}&code=mock-code`, s1.cookie);
check("callback redirects to the dashboard with a code + ticket", cb1.status === 302 && cb1.loc.origin + cb1.loc.pathname === "http://localhost:3000/dash/oauth/callback" && /^[0-9a-f-]{36}$/.test(cb1.params.code) && cb1.params.ticket === ticket, cb1);
check("callback has no content-type", cb1.contentType === null, cb1.contentType);
check("google got the form", google.lastForm?.client_id === "mock-google-client" && google.lastForm.client_secret === "mock-google-secret" && google.lastForm.code === "mock-code" && google.lastForm.grant_type === "authorization_code" && google.lastForm.redirect_uri === `${base}/dash/oauth/callback`, google.lastForm);
check("redirect is consumed", (await callback(`?state=${s1.state}&code=mock-code`, s1.cookie)).params?.error === "Could not find OAuth request.");

// --- token ---
const t1 = await call("POST", "/dash/oauth/token", { body: { code: cb1.params.code } });
check("token", t1.status === 200 && /^[0-9a-f-]{36}$/.test(t1.body.token) && t1.body.redirect_path === "/apps" && t1.body.user?.email === "login.user@example.com" && typeof t1.body.user.created_at === "string", t1);
check("token keys", Object.keys(t1.body).sort().join() === "redirect_path,token,user" && Object.keys(t1.body.user).sort().join() === "created_at,email,id", t1.body);
check("code is one-use", (await call("POST", "/dash/oauth/token", { body: { code: cb1.params.code } })).body.type === "record-not-found");
check("token missing code", (await call("POST", "/dash/oauth/token", { body: {} })).body.type === "param-missing");
check("token malformed code", (await call("POST", "/dash/oauth/token", { body: { code: "nope" } })).body.type === "param-malformed");
const me = await call("GET", "/dash", { token: t1.body.token });
check("refresh token works on /dash", me.status === 200 && me.body.user?.email === "login.user@example.com", me.body?.user);
const userId = t1.body.user.id;

// --- the CLI side: claim the ticket as the user, then check with the secret ---
check("cli claim needs a user", (await call("POST", "/dash/cli/auth/claim", { body: { ticket } })).status === 400);
check("cli claim needs a ticket", (await call("POST", "/dash/cli/auth/claim", { token: t1.body.token, body: {} })).body.type === "param-missing");
const claimed = await call("POST", "/dash/cli/auth/claim", { token: t1.body.token, body: { ticket } });
check("cli claim", claimed.status === 200 && claimed.body.ticket === ticket, claimed);
const checked = await call("POST", "/dash/cli/auth/check", { body: { secret: reg.body.secret } });
check("cli check hands out a token", checked.status === 200 && /^[0-9a-f-]{36}$/.test(checked.body.token) && checked.body.email === "login.user@example.com" && Object.keys(checked.body).sort().join() === "email,token", checked);
check("cli token works", (await call("GET", "/dash", { token: checked.body.token })).status === 200);
check("cli check is one-use", (await call("POST", "/dash/cli/auth/check", { body: { secret: reg.body.secret } })).body.hint?.errors?.[0]?.issue === "user-already-claimed");
check("cli check unknown secret", (await call("POST", "/dash/cli/auth/check", { body: { secret: crypto.randomUUID() } })).body.type === "record-not-found");
check("cli check malformed secret", (await call("POST", "/dash/cli/auth/check", { body: { secret: "nope" } })).body.type === "param-malformed");
const reg2 = await call("POST", "/dash/cli/auth/register");
check("cli void", (await call("POST", "/dash/cli/auth/void", { token: t1.body.token, body: { ticket: reg2.body.ticket } })).status === 200);
check("cli check after void", (await call("POST", "/dash/cli/auth/check", { body: { secret: reg2.body.secret } })).body.hint?.errors?.[0]?.issue === "user-voided-request");

// --- the same google sub with a new email updates the user ---
google.claims = { sub: "google-sub-1", email: "renamed@example.com", email_verified: true };
const s2 = await start();
const cb2 = await callback(`?state=${s2.state}&code=mock-code`, s2.cookie);
const t2 = await call("POST", "/dash/oauth/token", { body: { code: cb2.params?.code } });
check("same sub, new email → same user with the new email", t2.body.user?.id === userId && t2.body.user.email === "renamed@example.com" && t2.body.redirect_path === "/dash", t2.body);

// --- an existing magic-code user (no sub yet) gets the sub attached ---
const magicEmail = `magic-${randomBytes(4).toString("hex")}@example.com`;
await call("POST", "/dash/auth/send_magic_code", { body: { email: magicEmail } });
google.claims = { sub: "google-sub-2", email: magicEmail, email_verified: true };
const s3 = await start();
const cb3 = await callback(`?state=${s3.state}&code=mock-code`, s3.cookie);
const t3 = await call("POST", "/dash/oauth/token", { body: { code: cb3.params?.code } });
const subRow = execFileSync("psql", [dbUrl, "-tA", "-c", `SELECT google_sub FROM instant_users WHERE email = '${magicEmail}'`]).toString().trim();
check("existing email gets the google sub", t3.status === 200 && t3.body.user?.email === magicEmail && subRow === "google-sub-2", [t3.body, subRow]);

// --- google failures ---
google.claims = { sub: "google-sub-3", email: "unverified@example.com", email_verified: false };
const s4 = await start();
check("unverified email", (await callback(`?state=${s4.state}&code=mock-code`, s4.cookie)).params?.error === "Could not verify email.");
google.status = 400;
const s5 = await start();
check("google rejects the code", (await callback(`?state=${s5.state}&code=mock-code`, s5.cookie)).params?.error === "Error fetching user data from Google: Malformed auth code.");
google.status = 200;
google.claims = { sub: "google-sub-4", email: "not-an-email", email_verified: true };
const s6 = await start();
check("bad email claim", (await callback(`?state=${s6.state}&code=mock-code`, s6.cookie)).params?.error === "Could not determine email.");
const s7 = await start();
check("cookie mismatch", (await callback(`?state=${s7.state}&code=mock-code`, crypto.randomUUID())).params?.error === "Mismatch in OAuth request cookie.");

// --- get-a-db: the service user + a PAT seeded like production ---
const getadbId = crypto.randomUUID();
const pat = "per_" + randomBytes(32).toString("hex");
const lookup = createHash("sha256").update(pat).digest("hex");
execFileSync("psql", [dbUrl, "-q", "-v", "ON_ERROR_STOP=1", "-c", `
  INSERT INTO instant_users (id, email) SELECT '${getadbId}', 'hello+getadbapps@instantdb.com'
    WHERE NOT EXISTS (SELECT 1 FROM instant_users WHERE email = 'hello+getadbapps@instantdb.com');
  INSERT INTO instant_personal_access_tokens (id, name, user_id, lookup_key)
    SELECT '${crypto.randomUUID()}', 'getadb-pat', id, decode('${lookup}', 'hex') FROM instant_users WHERE email = 'hello+getadbapps@instantdb.com';
`]);
check("get-a-db needs the service user", (await call("POST", "/dash/apps/get_a_db", { token: t1.body.token, body: { title: "x" } })).status === 400);
const userPat = await call("POST", "/dash/personal_access_tokens", { token: t1.body.token, body: { name: "t" } });
const denied = await call("POST", "/dash/apps/get_a_db", { token: userPat.body.token, body: { title: "x" } });
check("get-a-db denies other users", denied.body.type === "permission-denied" && denied.body.hint?.expected === "get-a-db-user?", denied.body);
check("get-a-db needs a title", (await call("POST", "/dash/apps/get_a_db", { token: pat, body: {} })).body.type === "param-missing");
check("get-a-db validates rules", (await call("POST", "/dash/apps/get_a_db", { token: pat, body: { title: "x", rules: { code: { posts: { allow: { view: "nope(" } } } } } })).body.type === "validation-failed");
const created = await call("POST", "/dash/apps/get_a_db", {
  token: pat,
  body: { title: "get a db", rules: { code: { posts: { allow: { view: "true" } } } }, schema: { entities: { posts: { title: { valueType: "string", config: { indexed: true, unique: false } } } }, links: {} } },
});
const serviceUserId = execFileSync("psql", [dbUrl, "-tA", "-c", "SELECT id FROM instant_users WHERE email = 'hello+getadbapps@instantdb.com'"]).toString().trim();
check("get-a-db creates the app", created.status === 200 && created.body.app?.title === "get a db" && created.body.app.creator_id === serviceUserId && /^[0-9a-f-]{36}$/.test(created.body.app["admin-token"] ?? ""), created.body);
const appId = created.body.app?.id;
const adminToken = created.body.app?.["admin-token"];
const claimable = await call("GET", `/dash/apps/get_a_db/${appId}`);
check("the app is claimable", claimable.status === 200 && claimable.body.app?.id === appId, claimable.body);
const schema = await call("GET", "/admin/schema", { token: adminToken, headers: { "app-id": appId } });
check("schema applied", schema.body?.schema?.blobs?.posts?.title?.["index?"] === true || JSON.stringify(schema.body).includes('"posts"'), schema.body);
const perms = await call("GET", `/dash/apps/${appId}/perms/pull`, { token: adminToken });
check("rules applied", perms.body?.perms?.posts?.allow?.view === "true", perms.body);

// --- track-import + stats ---
check("track-import", (await call("POST", `/dash/apps/${appId}/track-import`)).body?.ok === true);
check("track-import bad id", (await call("POST", "/dash/apps/nope/track-import")).body?.type === "param-malformed");
const stats = await call("GET", "/dash/stats/active_sessions");
check("active sessions", stats.status === 200 && typeof stats.body["total-count"] === "number" && typeof stats.body["total-queries"] === "number", stats.body);

provider.close();
if (failures) {
  console.error(`DASH LOGIN TEST FAILED: ${failures} checks`);
  process.exit(1);
}
console.log("DASH LOGIN TEST PASSED");
