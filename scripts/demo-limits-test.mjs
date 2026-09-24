// Public-deployment limits e2e: the per-app size cap, the per-user app cap,
// disabled ephemeral apps, the hard-delete sweeper and the OAuth SSRF guard.
//
// Usage: node demo-limits-test.mjs
// Env: SERVER_URL (default http://localhost:8886): a node booted with
//        INSTANT_APP_SIZE_LIMIT_MB=1 INSTANT_SIZE_COLLECT_SECS=1
//        INSTANT_MAX_APPS_PER_USER=2 INSTANT_EPHEMERAL_APPS=off
//        INSTANT_HARD_DELETE_GRACE_HOURS=0 INSTANT_HARD_DELETE_SWEEP_SECS=2
//        and without INSTANT_OAUTH_ALLOW_PRIVATE
//      DATABASE_URL (psql: provisioning, magic codes, deletion marks)
import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";

const SERVER = process.env.SERVER_URL || "http://localhost:8886";
const DB = process.env.DATABASE_URL || "postgres://instant:instant@localhost:5432/instant";
const assert = (c, m, detail) => {
  if (!c) throw new Error("ASSERT FAILED: " + m + (detail === undefined ? "" : " " + JSON.stringify(detail)));
  console.log("ok:", m);
};
const psql = (sql) => execFileSync("psql", [DB, "-tA", "-v", "ON_ERROR_STOP=1", "-c", sql]).toString().trim();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const call = async (method, path, { headers = {}, body, raw } = {}) => {
  const res = await fetch(`${SERVER}${path}`, {
    method,
    headers: { ...(raw ? {} : { "content-type": "application/json" }), ...headers },
    body: raw ?? (body === undefined ? undefined : JSON.stringify(body)),
    redirect: "manual",
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}
  return { status: res.status, body: json, text, headers: res.headers };
};
const waitFor = async (what, pred, ms = 20000) => {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    if (await pred()) return;
    await sleep(500);
  }
  throw new Error("timed out waiting for " + what);
};
const newApp = (title) => {
  const out = execFileSync("./scripts/create-app.sh", [title], { env: { ...process.env, DATABASE_URL: DB } }).toString();
  return {
    appId: out.match(/^app_id=(.+)$/m)[1],
    token: out.match(/^admin_token=(.+)$/m)[1],
  };
};
const admin = ({ appId, token }) => ({ "app-id": appId, authorization: `Bearer ${token}` });
const id = () => crypto.randomUUID();

// ---------------------------------------------------------------------------
// size cap (1 MB)
{
  const app = newApp("size cap");
  // random base64 so TOAST compression can't shrink it under the cap
  const big = () => randomBytes(300 * 1024).toString("base64");
  const ids = [id(), id(), id(), id(), id()];
  const w = await call("POST", "/admin/transact", {
    headers: admin(app),
    body: { steps: ids.map((e) => ["update", "blobs", e, { text: big() }]) },
  });
  assert(w.status === 200, "an app under the cap writes", w.body);
  const blocked = async () =>
    (await call("POST", "/admin/transact", {
      headers: admin(app),
      body: { steps: [["update", "blobs", id(), { text: "more" }]] },
    }));
  await waitFor("the app to be over its cap", async () => (await blocked()).body?.type === "app-size-limit-exceeded");
  const b = await blocked();
  assert(b.status === 400 && /1 MB size limit/.test(b.body.message), "writes over the cap are refused", b.body);
  const up = await call("PUT", "/admin/storage/upload", {
    headers: { ...admin(app), path: "a.txt", "content-type": "text/plain" },
    raw: "hello",
  });
  assert(up.body?.type === "app-size-limit-exceeded", "uploads over the cap are refused", up.body);
  const del = await call("POST", "/admin/transact", {
    headers: admin(app),
    body: { steps: ids.map((e) => ["delete", "blobs", e]) },
  });
  assert(del.status === 200, "deletes still work over the cap", del.body);
  await waitFor("the app to drop under its cap", async () => (await blocked()).status === 200);
  assert(true, "writes resume once the app is back under the cap");

  const fresh = newApp("size cap upload");
  const tooBig = await call("PUT", "/admin/storage/upload", {
    headers: { ...admin(fresh), path: "big.bin", "content-type": "application/octet-stream" },
    raw: randomBytes(1100 * 1024),
  });
  assert(tooBig.body?.type === "app-size-limit-exceeded", "an upload that would cross the cap is refused", tooBig.body);
  const small = await call("PUT", "/admin/storage/upload", {
    headers: { ...admin(fresh), path: "small.txt", "content-type": "text/plain" },
    raw: "hi",
  });
  assert(small.status === 200, "a small upload fits", small.body);
}

// ---------------------------------------------------------------------------
// per-user app cap (2), ephemeral apps off
{
  const email = `limits-${randomBytes(4).toString("hex")}@example.com`;
  const sent = await call("POST", "/dash/auth/send_magic_code", { body: { email } });
  assert(sent.status === 200, "dashboard magic code sent", sent.body);
  const code = psql(
    `SELECT c.code FROM instant_user_magic_codes c JOIN instant_users u ON u.id = c.user_id WHERE u.email = '${email}' ORDER BY c.created_at DESC LIMIT 1`,
  );
  const v = await call("POST", "/dash/auth/verify_magic_code", { body: { email, code } });
  assert(v.status === 200 && v.body.token, "dashboard sign-in", v.body);
  const dash = { authorization: `Bearer ${v.body.token}` };
  const create = (title) =>
    call("POST", "/dash/apps", { headers: dash, body: { title, id: id(), admin_token: id() } });
  const a1 = await create("one");
  const a2 = await create("two");
  assert(a1.status === 200 && a2.status === 200, "a user creates apps up to the cap", [a1.body, a2.body]);
  const a3 = await create("three");
  assert(a3.body?.type === "app-limit-exceeded" && a3.body.hint?.limit === 2, "the next app is refused", a3.body);
  const d = await call("DELETE", `/dash/apps/${a2.body.app.id}`, { headers: dash });
  assert(d.status === 200, "deleting an app", d.body);
  // grace 0h: the sweeper (every 2s) purges it, which frees the slot
  await waitFor("the deleted app to be purged", async () => psql(`SELECT count(*) FROM apps WHERE id = '${a2.body.app.id}'`) === "0");
  const a4 = await create("four");
  assert(a4.status === 200, "a purged app frees its slot", a4.body);

  const eph = await call("POST", "/dash/apps/ephemeral", { body: { title: "temp" } });
  assert(eph.status === 400 && eph.body?.type === "permission-denied", "ephemeral apps are disabled", eph.body);
}

// ---------------------------------------------------------------------------
// hard delete: a marked app loses its triples, transactions and blobs; a
// marked attr loses its triples
{
  const app = newApp("hard delete");
  const e = id();
  const w = await call("POST", "/admin/transact", {
    headers: admin(app),
    body: { steps: [["update", "things", e, { name: "a", note: "b" }]] },
  });
  assert(w.status === 200, "seed data", w.body);
  const up = await call("PUT", "/admin/storage/upload", {
    headers: { ...admin(app), path: "f.txt", "content-type": "text/plain" },
    raw: "file",
  });
  assert(up.status === 200, "seed a file", up.body);
  const noteAttr = psql(`SELECT id FROM attrs WHERE app_id = '${app.appId}' AND etype = 'things' AND label = 'note'`);
  psql(`UPDATE attrs SET deletion_marked_at = now() WHERE id = '${noteAttr}'`);
  await waitFor("the marked attr to be purged", async () => psql(`SELECT count(*) FROM attrs WHERE id = '${noteAttr}'`) === "0");
  assert(psql(`SELECT count(*) FROM triples WHERE attr_id = '${noteAttr}'`) === "0", "a purged attr's triples are gone");
  assert(Number(psql(`SELECT count(*) FROM triples WHERE app_id = '${app.appId}'`)) > 0, "the app's other triples stay");

  psql(`UPDATE apps SET deletion_marked_at = now() WHERE id = '${app.appId}'`);
  await waitFor("the marked app to be purged", async () => psql(`SELECT count(*) FROM apps WHERE id = '${app.appId}'`) === "0");
  for (const t of ["triples", "transactions", "attrs", "rust_blobs"]) {
    assert(psql(`SELECT count(*) FROM ${t} WHERE app_id = '${app.appId}'`) === "0", `a purged app has no ${t}`);
  }
}

// ---------------------------------------------------------------------------
// OAuth SSRF guard: a discovery endpoint on a private address is refused
{
  const app = newApp("ssrf");
  execFileSync("./scripts/create-oauth-client.sh", [
    app.appId, "internal", "internal-provider", "cid", "secret",
    "http://127.0.0.1:5432/.well-known/openid-configuration", "localhost:5173",
  ], { env: { ...process.env, DATABASE_URL: DB } });
  const start = await call(
    "GET",
    `/runtime/oauth/start?app_id=${app.appId}&client_name=internal&redirect_uri=${encodeURIComponent("http://localhost:5173/app")}`,
  );
  const said = start.body?.message ?? start.headers.get("location") ?? start.text;
  assert(/public(\+|\s)address/.test(decodeURIComponent(said)), "a private discovery endpoint is not fetched", said);
}

console.log("DEMO LIMITS TEST PASSED");
process.exit(0);
