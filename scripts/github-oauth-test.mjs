// GitHub sign-in with a mock GitHub (authorize / access_token / API), the
// success paths the differential harness can't reach: legacy's
// GitHubOAuthClient (LEGACY/server/src/instant/auth/oauth.clj:31-113) talks
// to github.com only. The server must run with
//   INSTANT_GITHUB_OAUTH_AUTH_URL=http://localhost:9379/login/oauth/authorize
//   INSTANT_GITHUB_OAUTH_TOKEN_URL=http://localhost:9379/login/oauth/access_token
//   INSTANT_GITHUB_API_URL=http://localhost:9379/api
// Usage: node github-oauth-test.mjs <app-id> <admin-token>  (a fresh app: the
// test adds the `github` provider, its client and the localhost:5173 origin)
import http from "node:http";

const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken) throw new Error("usage: node github-oauth-test.mjs <app-id> <admin-token>");
const SERVER = process.env.SERVER_URL ?? "http://localhost:8888";
const GH_PORT = 9379;
const APP_URL = "http://localhost:5173/app";

const assert = (cond, msg) => {
  if (!cond) throw new Error("ASSERT FAILED: " + msg);
  console.log("ok:", msg);
};

// --- mock GitHub ---
// a code is `code-<token>`; the token picks the GitHub account below
const accounts = {
  "tok-1": { user: { id: 1001, login: "octo", avatar_url: "https://avatars.example.com/u/1001" }, emails: [{ email: "other@example.com", primary: false, verified: true }, { email: "  GH-User@Example.com", primary: true, verified: true }] },
  // the same account later: new primary email, new avatar
  "tok-1-moved": { user: { id: 1001, login: "octo", avatar_url: "https://avatars.example.com/u/1001?v=2" }, emails: [{ email: "gh-user-new@example.com", primary: true, verified: true }] },
  // a primary email GitHub hasn't verified is no email
  "tok-noemail": { user: { id: 1002, avatar_url: "https://avatars.example.com/u/1002" }, emails: [{ email: "unverified@example.com", primary: true, verified: false }, { email: "secondary@example.com", primary: false, verified: true }] },
  // /user/emails refused (token without user:email): no email either
  "tok-emails-403": { user: { id: 1003, avatar_url: null }, emailsStatus: 403 },
  "tok-user-401": { userStatus: 401, user: { message: "Bad credentials" } },
  "tok-user-500": { userStatus: 500, user: {} },
  "tok-noid": { user: { login: "ghost" }, emails: [] },
  // signs in as one user, then reports the email of another
  "tok-5-noemail": { user: { id: 1005 }, emails: [] },
  "tok-5-email": { user: { id: 1005 }, emails: [{ email: "move-target@example.com", primary: true, verified: true }] },
  "tok-6": { user: { id: 1006 }, emails: [{ email: "existing@example.com", primary: true, verified: true }] },
  "tok-6-noemail": { user: { id: 1006 }, emails: [] },
  "tok-7": { user: { id: 1007, avatar_url: "https://avatars.example.com/u/1007" }, emails: [{ email: "guest-gh@example.com", primary: true, verified: true }] },
};
const seen = { redirectUri: null, tokenRequests: 0 };
const provider = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${GH_PORT}`);
  const send = (status, body) => {
    res.statusCode = status;
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(body));
  };
  // legacy `(:headers github-config)` on every call
  if (req.headers.accept !== "application/json" || req.headers["user-agent"] !== "InstantDB OAuth") {
    return send(400, { message: `bad headers: ${req.headers.accept} / ${req.headers["user-agent"]}` });
  }
  if (req.method === "POST" && url.pathname === "/login/oauth/access_token") {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      seen.tokenRequests++;
      const p = new URLSearchParams(body);
      if (p.get("client_id") !== "gh-mock-client" || p.get("client_secret") !== "gh-mock-secret") return send(404, { error: "Not Found" });
      if (p.get("grant_type") !== "authorization_code") return send(400, { error_description: "bad grant_type" });
      if (seen.redirectUri && p.get("redirect_uri") !== seen.redirectUri) return send(400, { error_description: `redirect_uri ${p.get("redirect_uri")} != ${seen.redirectUri}` });
      const code = p.get("code");
      // GitHub answers a bad code with a 200 and no token
      if (code === "code-bad") return send(200, { error: "bad_verification_code", error_description: "The code passed is incorrect or expired." });
      if (code === "code-500") return send(500, { error_description: "GitHub is down." });
      if (code === "code-400") return send(400, {});
      if (code === "code-not-json") {
        res.statusCode = 200;
        return res.end("<html>nope</html>");
      }
      send(200, { access_token: code.replace(/^code-/, ""), token_type: "bearer", scope: "read:user,user:email" });
    });
    return;
  }
  const token = /^Bearer (.+)$/.exec(req.headers.authorization ?? "")?.[1];
  const account = accounts[token];
  if (req.method === "GET" && url.pathname === "/api/user") {
    if (!account) return send(401, { message: "Bad credentials" });
    return send(account.userStatus ?? 200, account.user);
  }
  if (req.method === "GET" && url.pathname === "/api/user/emails") {
    if (!account) return send(401, { message: "Bad credentials" });
    if (account.emailsStatus) return send(account.emailsStatus, { message: "Resource not accessible by integration" });
    return send(200, account.emails ?? []);
  }
  send(404, { message: "Not Found" });
});
await new Promise((r) => provider.listen(GH_PORT, r));

const json = async (r) => ({ status: r.status, body: await r.json().catch(() => null) });
const dash = (method, path, body) =>
  fetch(`${SERVER}${path}`, {
    method,
    headers: { authorization: `Bearer ${adminToken}`, "content-type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  }).then(json);
const admin = (path, body) =>
  fetch(`${SERVER}${path}`, {
    method: "POST",
    headers: { authorization: `Bearer ${adminToken}`, "app-id": appId, "content-type": "application/json" },
    body: JSON.stringify(body),
  }).then(json);

// --- provision through the dashboard routes: a github client needs no discovery endpoint ---
const prov = await dash("POST", `/dash/apps/${appId}/oauth_service_providers`, { provider_name: "github" });
assert(prov.status === 200 && prov.body.provider?.provider_name === "github", "github provider created");
const client = await dash("POST", `/dash/apps/${appId}/oauth_clients`, {
  provider_id: prov.body.provider.id,
  client_name: "github",
  client_id: "gh-mock-client",
  client_secret: "gh-mock-secret",
  meta: { providerName: "github" },
});
assert(client.status === 200 && client.body.client?.discovery_endpoint == null, "github client created without a discovery endpoint");
const origin = await dash("POST", `/dash/apps/${appId}/authorized_redirect_origins`, { service: "generic", params: ["localhost:5173"] });
assert(origin.status === 200, "localhost:5173 is an authorized redirect origin");

// --- start: GitHub's authorize URL, GitHub's scope, no response_mode / hd ---
const startUrl = (extra = "") =>
  `${SERVER}/runtime/oauth/start?app_id=${appId}&client_name=github&redirect_uri=${encodeURIComponent(APP_URL)}&state=appstate${extra}`;
const start = async (extra) => {
  const r = await fetch(startUrl(extra), { redirect: "manual" });
  const loc = new URL(r.headers.get("location"));
  return { status: r.status, loc, state: loc.searchParams.get("state"), cookie: r.headers.get("set-cookie")?.split(";")[0] };
};
const s0 = await start("&hd=example.com");
assert(s0.status === 302, "start is a 302");
assert(s0.loc.origin + s0.loc.pathname === `http://localhost:${GH_PORT}/login/oauth/authorize`, "start redirects to GitHub's authorize URL");
assert(
  JSON.stringify([...s0.loc.searchParams.keys()]) === JSON.stringify(["scope", "response_type", "state", "redirect_uri", "client_id"]),
  "authorize params are legacy's (no response_mode, hd dropped)",
);
assert(s0.loc.searchParams.get("scope") === "read:user user:email", "GitHub scope");
assert(s0.loc.searchParams.get("response_type") === "code", "response_type=code");
assert(s0.loc.searchParams.get("client_id") === "gh-mock-client", "client_id");
assert(s0.state.length === 72 && s0.state.startsWith(appId), "state is app id + uuid");
seen.redirectUri = s0.loc.searchParams.get("redirect_uri");
assert(seen.redirectUri === `${SERVER}/runtime/oauth/callback`, "redirect_uri is this server's callback");
assert(/^__session=instantdb_[0-9a-f-]{36}$/.test(s0.cookie), "oauth cookie set");

// --- the callback exchanges the code; the app gets its own code ---
const callback = async (token, extra) => {
  const s = await start(extra);
  const r = await fetch(`${SERVER}/runtime/oauth/callback?code=code-${token}&state=${s.state}`, { redirect: "manual", headers: { cookie: s.cookie } });
  const loc = r.headers.get("location") ? new URL(r.headers.get("location")) : null;
  return { status: r.status, loc, body: loc ? null : await r.text() };
};
const exchange = (code, extra = {}) =>
  fetch(`${SERVER}/runtime/oauth/token`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ app_id: appId, code, ...extra }),
  }).then(json);
const signIn = async (token, extra = {}) => {
  const cb = await callback(token);
  if (!cb.loc || !cb.loc.searchParams.get("code")) throw new Error(`no app code for ${token}: ${cb.status} ${cb.loc ?? cb.body}`);
  return exchange(cb.loc.searchParams.get("code"), extra);
};

const cb1 = await callback("tok-1");
assert(cb1.status === 302 && cb1.loc.origin + cb1.loc.pathname === APP_URL, "callback redirects back to the app");
assert(cb1.loc.searchParams.get("state") === "appstate" && cb1.loc.searchParams.get("_instant_oauth_redirect") === "true", "app state + oauth marker");
const first = await exchange(cb1.loc.searchParams.get("code"));
assert(first.status === 200 && first.body.created === true, "first GitHub sign-in creates the user");
assert(first.body.user.email === "gh-user@example.com", "the primary verified email, coerced (trimmed, lower-cased)");
assert(first.body.user.imageURL === "https://avatars.example.com/u/1001", "imageURL is the GitHub avatar");
assert(first.body.refresh_token && first.body.user.refresh_token === first.body.refresh_token, "refresh token in both places");
const verify = await fetch(`${SERVER}/runtime/auth/verify_refresh_token`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ "app-id": appId, "refresh-token": first.body.refresh_token }),
}).then(json);
assert(verify.status === 200 && verify.body.user.id === first.body.user.id, "the refresh token verifies");

const again = await signIn("tok-1");
assert(again.status === 200 && again.body.created === false && again.body.user.id === first.body.user.id, "second sign-in reuses the linked user");

// legacy update-email! / update-image-url!: the link wins, the profile follows GitHub
const moved = await signIn("tok-1-moved");
assert(moved.status === 200 && moved.body.user.id === first.body.user.id, "a changed primary email still signs the linked user in");
assert(moved.body.user.email === "gh-user-new@example.com", "the user's email follows GitHub's primary email");
assert(moved.body.user.imageURL === "https://avatars.example.com/u/1001?v=2", "the user's imageURL follows the avatar");

// --- no primary verified email: an email-less user, matched by the link alone ---
const noEmail = await signIn("tok-noemail");
assert(noEmail.status === 200 && noEmail.body.created === true, "an account without a verified primary email still signs up");
assert(!noEmail.body.user.email, "...with no email (unverified / non-primary addresses are ignored)");
assert(noEmail.body.user.imageURL === "https://avatars.example.com/u/1002", "...and the avatar");
const noEmailAgain = await signIn("tok-noemail");
assert(noEmailAgain.body.created === false && noEmailAgain.body.user.id === noEmail.body.user.id, "the email-less user is found again by the link");
const emails403 = await signIn("tok-emails-403");
assert(emails403.status === 200 && emails403.body.created === true && !emails403.body.user.email, "a refused /user/emails is no email, not an error");

// --- an existing user with the email gets the link ---
const existing = await admin("/admin/refresh_tokens", { email: "existing@example.com" });
assert(existing.status === 200, "a user with existing@example.com exists");
const existingId = existing.body.user.id;
const linkExisting = await signIn("tok-6");
assert(linkExisting.status === 200 && linkExisting.body.created === false && linkExisting.body.user.id === existingId, "GitHub sign-in with an existing email links that user");
const linkExistingAgain = await signIn("tok-6-noemail");
assert(linkExistingAgain.body.user.id === existingId, "...and the link finds them without the email");

// --- the sub's user and the email's user differ: the email wins, the link moves ---
const target = await admin("/admin/refresh_tokens", { email: "move-target@example.com" });
const before = await signIn("tok-5-noemail");
assert(before.body.created === true && before.body.user.id !== target.body.user.id, "GitHub account 1005 first signs up email-less");
const after = await signIn("tok-5-email");
assert(after.status === 200 && after.body.created === false && after.body.user.id === target.body.user.id, "once GitHub reports another user's email, that user signs in");
const afterAgain = await signIn("tok-5-noemail");
assert(afterAgain.body.user.id === target.body.user.id, "the link moved to the email's user");

// --- a guest upgrading through GitHub keeps its id ---
const guest = await fetch(`${SERVER}/runtime/auth/sign_in_guest`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ "app-id": appId }),
}).then((r) => r.json());
const upgraded = await signIn("tok-7", { refresh_token: guest.user.refresh_token });
assert(upgraded.status === 200 && upgraded.body.created === true && upgraded.body.user.id === guest.user.id, "a guest upgrades in place");
assert(upgraded.body.user.email === "guest-gh@example.com" && upgraded.body.user.type === "user", "...with the GitHub email, as a user");

// --- failed exchanges redirect back to the app with legacy's messages ---
const failure = async (token, message, what) => {
  const cb = await callback(token);
  assert(
    cb.status === 302 && cb.loc.origin + cb.loc.pathname === APP_URL && cb.loc.searchParams.get("error") === message && cb.loc.searchParams.get("_instant_oauth_redirect") === "true" && cb.loc.searchParams.get("state") === "appstate" && !cb.loc.searchParams.get("code"),
    `${what} redirects back with error=${JSON.stringify(message)}`,
  );
};
await failure("bad", "No access token received from GitHub.", "a bad code (GitHub's 200 without a token)");
await failure("500", "GitHub is down.", "a failed token exchange with an error_description");
await failure("400", "Error exchanging code for token.", "a failed token exchange without one");
await failure("not-json", "Error exchanging code for token.", "a token response that isn't JSON");
await failure("tok-user-401", "Bad credentials", "a /user failure with a message");
await failure("tok-user-500", "Failed to fetch user info from GitHub.", "a /user failure without one");
await failure("tok-noid", "Missing user ID from GitHub", "a /user without an id");
const wrongSecret = await dash("POST", `/dash/apps/${appId}/oauth_clients/${client.body.client.id}`, { client_secret: "wrong" });
assert(wrongSecret.status === 200, "client secret changed");
await failure("tok-1", "Error exchanging code for token.", "GitHub refusing the client credentials (404)");
await dash("POST", `/dash/apps/${appId}/oauth_clients/${client.body.client.id}`, { client_secret: "gh-mock-secret" });

// --- the error surfaces before the exchange stay 400 oauth-errors ---
{
  const s = await start();
  const denied = await fetch(`${SERVER}/runtime/oauth/callback?error=access_denied&error_description=denied&state=${s.state}`, { redirect: "manual", headers: { cookie: s.cookie } }).then(json);
  assert(denied.status === 400 && denied.body.type === "oauth-error" && denied.body.error === "access_denied", "GitHub's access_denied is a 400 oauth-error");
  const before = seen.tokenRequests;
  const mismatch = await fetch(`${SERVER}/runtime/oauth/callback?code=code-tok-1&state=${s.state}`, { redirect: "manual", headers: { cookie: "__session=instantdb_00000000-0000-0000-0000-000000000000" } }).then(json);
  assert(mismatch.status === 400 && mismatch.body.error === "Mismatch in OAuth request cookie.", "a cookie mismatch is refused before any GitHub call");
  assert(seen.tokenRequests === before, "...and GitHub was not asked");
}

// --- GitHub isn't OIDC: the id_token route refuses ---
const idToken = await fetch(`${SERVER}/runtime/oauth/id_token`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ app_id: appId, client_name: "github", id_token: "x.y.z" }),
}).then(json);
assert(idToken.status === 400 && idToken.body.type === "validation-failed", "id_token with a GitHub client is a validation error");
assert(idToken.body.message === "Validation failed for id_token: GitHub OAuth does not support ID tokens.", "legacy's id_token message");
assert(idToken.body.hint?.["data-type"] === "id_token" && idToken.body.hint?.input === null, "legacy's id_token hint");

// --- a client's custom redirect_to is the redirect_uri on both hops ---
const custom = "http://localhost:4001/custom/callback";
const upd = await dash("POST", `/dash/apps/${appId}/oauth_clients/${client.body.client.id}`, { redirect_to: custom });
assert(upd.status === 200, "redirect_to set");
const sc = await start();
assert(sc.loc.searchParams.get("redirect_uri") === custom, "start sends the client's redirect_to");
seen.redirectUri = custom;
const cbc = await fetch(`${SERVER}/runtime/oauth/callback?code=code-tok-1&state=${sc.state}`, { redirect: "manual", headers: { cookie: sc.cookie } });
const back = new URL(cbc.headers.get("location"));
assert(back.searchParams.get("code") && !back.searchParams.get("error"), "the exchange repeats the custom redirect_uri");

provider.close();
console.log("GITHUB OAUTH TEST PASSED");
process.exit(0);
