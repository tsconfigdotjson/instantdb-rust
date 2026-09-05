// Error-matrix layer of the differential harness: one probe per externally
// reachable legacy error type (util/exception.clj `::type`s, see surface.json
// `err:*`), over HTTP and over the ws session, run against both servers. The
// error envelope each server returns (status, type, message, hint) is
// normalized and must match; `errors-allowed.json` lists the accepted
// divergences with a citation.
//
// Usage: node errors.mjs <app-id> <admin-token> <user-refresh-token>
//   (provision.sh with the third argument; the same app id, admin token and
//    creator refresh token must exist on both servers)
// Env: LEGACY_URL, RUST_URL, LEGACY_DATABASE_URL, RUST_DATABASE_URL, DUMP=1

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { canon, connect, makeIdFactory, psql, uuid } from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appId = process.argv[2];
const adminToken = process.argv[3];
const userToken = process.argv[4];
if (!appId || !adminToken || !userToken) throw new Error("usage: node errors.mjs <app-id> <admin-token> <user-refresh-token>");

const SERVERS = {
  legacy: {
    url: process.env.LEGACY_URL || "http://localhost:8891",
    db: process.env.LEGACY_DATABASE_URL || "postgres://instant:instant@localhost:8890/instant",
  },
  rust: {
    url: process.env.RUST_URL || "http://localhost:8888",
    db: process.env.RUST_DATABASE_URL || "postgres://instant:instant@localhost:5432/instant",
  },
};

const mk = makeIdFactory(appId);
const FIXED = {
  probeId: mk(),
  probeName: mk(),
  probe2Id: mk(),
  probe2S: mk(),
  dupAttr: mk(),
  entity: mk(),
  entity2: mk(),
};
const isFixed = (s) => typeof s === "string" && s.includes("-0000-4000-8000-");
const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

// ---------------------------------------------------------------------------
// normalization: the envelope a client reads, minus server-chosen values

const DROP_KEYS = new Set(["trace-id", "session-id", "client-event-id", "original-event", "debug-uri"]);
function norm(v, key = null) {
  if (typeof v === "string") {
    if (ISO_RE.test(v)) return "<ts>";
    return v.replace(UUID_RE, (m) => (isFixed(m) ? m : "<uuid>"));
  }
  if (typeof v === "number") return v > 1e12 ? "<ts>" : v;
  if (Array.isArray(v)) return v.map((x) => norm(x));
  if (v && typeof v === "object") {
    const out = {};
    for (const [k, val] of Object.entries(v)) {
      if (DROP_KEYS.has(k)) continue;
      out[k] = norm(val, k);
    }
    return out;
  }
  return v;
}

function httpView(res) {
  const b = res.body && typeof res.body === "object" ? res.body : { "<non-json>": String(res.body).slice(0, 120) };
  return norm({ status: res.status, type: b.type, message: b.message ?? b.error, hint: b.hint ?? null, keys: Object.keys(b).sort() });
}
function wsView(frame) {
  if (!frame) return { status: null, type: "<no error frame>" };
  return norm({ status: frame.status, type: frame.type, message: frame.message, hint: frame.hint ?? null, keys: Object.keys(frame).sort() });
}

// ---------------------------------------------------------------------------
// helpers

async function call(base, method, p, { token = adminToken, body, headers = {} } = {}) {
  const h = {
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(body !== undefined ? { "content-type": "application/json" } : {}),
    ...headers,
  };
  const res = await fetch(base + p, { method, headers: h, body: body !== undefined ? JSON.stringify(body) : undefined });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = text;
  }
  return { status: res.status, body: json };
}

const admin = (ctx, p, body, headers) => call(ctx.url, "POST", p, { body, headers: { "app-id": appId, ...headers } });

async function session(ctx, { init = true, token } = {}) {
  const conn = connect(ctx.url, appId, `${ctx.name}:errors`);
  await conn.open;
  if (init) {
    conn.send({ "client-event-id": uuid(), op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.21.0" }, ...(token ? { "refresh-token": token } : {}) });
    await conn.waitFor((m) => m.op === "init-ok");
    conn.takeNewFrames();
  }
  return conn;
}
async function wsError(conn, msg, timeout = 8000) {
  const ceid = uuid();
  conn.takeNewFrames();
  conn.send({ "client-event-id": ceid, ...msg });
  try {
    return await conn.waitFor((m) => m.op === "error" || (m["client-event-id"] === ceid && m.op !== "error"), timeout);
  } catch {
    return null;
  }
}
async function wsOk(conn, msg) {
  const ceid = uuid();
  conn.takeNewFrames();
  conn.send({ "client-event-id": ceid, ...msg });
  return conn.waitFor((m) => m["client-event-id"] === ceid || m.op === "error");
}

// ---------------------------------------------------------------------------
// probes

const probes = [];
const probe = (name, run) => probes.push({ name, run });

// HTTP: record-not-found (admin token / app id), the dashboard 401 branch
probe("http/record-not-found/admin-token", async (ctx) =>
  httpView(await call(ctx.url, "POST", "/admin/query", { token: uuid(), body: { query: {} }, headers: { "app-id": appId } })));
probe("http/record-not-found/app-id", async (ctx) =>
  httpView(await call(ctx.url, "POST", "/admin/query", { body: { query: {} }, headers: { "app-id": uuid() } })));
probe("http/record-not-found/dash-app", async (ctx) =>
  httpView(await call(ctx.url, "GET", `/dash/apps/${uuid()}`, { token: userToken })));
probe("http/auth-401/dash-me", async (ctx) => httpView(await call(ctx.url, "GET", "/dash/me", { token: uuid() })));
probe("http/auth-401/no-bearer", async (ctx) => httpView(await call(ctx.url, "GET", "/dash/me", { token: null })));

// HTTP: param-missing / param-malformed
probe("http/param-missing/query", async (ctx) => httpView(await admin(ctx, "/admin/query", {})));
probe("http/param-missing/steps", async (ctx) => httpView(await admin(ctx, "/admin/transact", {})));
probe("http/param-missing/app-id", async (ctx) =>
  httpView(await call(ctx.url, "POST", "/admin/query", { body: { query: {} } })));
probe("http/param-malformed/app-id-path", async (ctx) =>
  httpView(await call(ctx.url, "GET", "/dash/apps/not-a-uuid", { token: userToken })));
probe("http/param-malformed/as-token", async (ctx) =>
  httpView(await admin(ctx, "/admin/query", { query: {} }, { "as-token": "nope" })));
probe("http/param-malformed/app-id-header", async (ctx) =>
  httpView(await call(ctx.url, "POST", "/admin/query", { body: { query: {} }, headers: { "app-id": "nope" } })));
probe("http/param-malformed/email", async (ctx) =>
  httpView(await call(ctx.url, "POST", "/runtime/auth/send_magic_code", { token: null, body: { "app-id": appId, email: "not an email" } })));

// HTTP: validation-failed
probe("http/validation-failed/bogus-step", async (ctx) =>
  httpView(await admin(ctx, "/admin/transact", { steps: [["frobnicate", "probe", FIXED.entity, {}]] })));
probe("http/validation-failed/bad-entity-id", async (ctx) =>
  httpView(await admin(ctx, "/admin/transact", { steps: [["update", "probe", "not-a-uuid", { name: "x" }]] })));
probe("http/validation-failed/steps-not-array", async (ctx) =>
  httpView(await admin(ctx, "/admin/transact", { steps: "nope" })));
probe("http/validation-failed/bad-lookup", async (ctx) =>
  httpView(await admin(ctx, "/admin/transact", { steps: [["update", "probe", "lookup__name__42", { name: "x" }]] })));
probe("http/validation-failed/query-bad-op", async (ctx) =>
  httpView(await admin(ctx, "/admin/query", { query: { probe: { $: { where: { name: { $frob: 1 } } } } } })));
probe("http/validation-failed/query-where-not-indexed-isnull", async (ctx) =>
  httpView(await admin(ctx, "/admin/query", { query: { probe: { $: { where: { name: { $isNull: true } } } } } })));
probe("http/validation-failed/query-like-on-number", async (ctx) =>
  httpView(await admin(ctx, "/admin/query", { query: { probe: { $: { where: { name: { $like: 3 } } } } } })));
probe("http/validation-failed/query-empty-or", async (ctx) =>
  httpView(await admin(ctx, "/admin/query", { query: { probe: { $: { where: { or: [] } } } } })));
probe("http/validation-failed/rules", async (ctx) =>
  httpView(await call(ctx.url, "POST", `/dash/apps/${appId}/rules`, { token: userToken, body: { code: { probe: { allow: { view: "this is not cel" } } } } })));
probe("http/validation-failed/rules-undeclared", async (ctx) =>
  httpView(await call(ctx.url, "POST", `/dash/apps/${appId}/rules`, { token: userToken, body: { code: { probe: { allow: { view: "newData.x == 1" } } } } })));

// HTTP: permission-denied / permission-evaluation-failed
// rules: `probe` can only be created by admins; `probe2` views throw at runtime
async function pushRules(ctx) {
  const res = await call(ctx.url, "POST", `/dash/apps/${appId}/rules`, {
    token: userToken,
    body: { code: { probe: { allow: { create: "false", view: "true" } }, probe2: { allow: { view: "int(data.s) > 0" } } } },
  });
  if (res.status !== 200) throw new Error(`[${ctx.name}] rules push failed: ${JSON.stringify(res.body)}`);
}
probe("http/permission-denied/as-guest-create", async (ctx) => {
  await pushRules(ctx);
  return httpView(await admin(ctx, "/admin/transact", { steps: [["update", "probe", FIXED.entity, { name: "x" }]] }, { "as-guest": "true" }));
});
probe("http/permission-evaluation-failed/query", async (ctx) => {
  const seed = await admin(ctx, "/admin/transact", { steps: [["update", "probe2", FIXED.entity2, { s: "abc" }]] });
  if (seed.status !== 200) return { seedFailed: httpView(seed) };
  return httpView(await admin(ctx, "/admin/query", { query: { probe2: {} } }, { "as-guest": "true" }));
});
probe("http/permission-denied/query-perms-check-as-admin", async (ctx) =>
  httpView(await admin(ctx, "/admin/query_perms_check", { query: { probe: {} } })));

// HTTP: oauth-error and record-expired
probe("http/oauth-error/callback", async (ctx) =>
  httpView(await call(ctx.url, "GET", "/runtime/oauth/callback?error=access_denied&state=x", { token: null })));
probe("http/record-expired/magic-code", async (ctx) => {
  const email = "expired@example.com";
  const gen = await admin(ctx, "/admin/magic_code", { email });
  if (gen.status !== 200) return { generateFailed: httpView(gen) };
  // magic codes are `$magicCodes` entities on both servers; the expiry clock
  // is the entity's created_at (model/app_user_magic_code.clj expired?)
  psql(
    ctx.db,
    `UPDATE triples SET created_at = created_at - 3 * 24 * 60 * 60 * 1000 WHERE app_id = '${appId}' AND entity_id IN (SELECT entity_id FROM triples WHERE app_id = '${appId}' AND value = '"${email}"'::jsonb);`,
  );
  const view = httpView(await call(ctx.url, "POST", "/runtime/auth/verify_magic_code", { token: null, body: { "app-id": appId, email, code: gen.body.code } }));
  // the code is random per server; the hint echoes it
  if (view.hint?.args?.[0]?.code) view.hint.args[0].code = "<code>";
  return view;
});
probe("http/record-not-found/magic-code", async (ctx) =>
  httpView(await call(ctx.url, "POST", "/runtime/auth/verify_magic_code", { token: null, body: { "app-id": appId, email: "nobody@example.com", code: "000000" } })));
probe("http/record-not-found/refresh-token", async (ctx) =>
  httpView(await call(ctx.url, "POST", "/runtime/auth/verify_refresh_token", { token: null, body: { "app-id": appId, "refresh-token": uuid() } })));

// HTTP: rate-limited (magic codes per email); records the first error seen
probe("http/rate-limited/magic-codes", async (ctx) => {
  const email = `flood-${ctx.name}@example.com`;
  for (let i = 0; i < 12; i++) {
    const res = await call(ctx.url, "POST", "/runtime/auth/send_magic_code", { token: null, body: { "app-id": appId, email } });
    if (res.status !== 200) return { after: i, ...httpView(res) };
  }
  return { after: null, type: "<none>" };
});

// HTTP: routing
probe("http/routing/not-found", async (ctx) => httpView(await call(ctx.url, "GET", "/no/such/route", { token: null })));
probe("http/routing/method-not-allowed", async (ctx) => httpView(await call(ctx.url, "DELETE", "/runtime/auth/send_magic_code", { token: null })));
probe("http/routing/malformed-json", async (ctx) => {
  const res = await fetch(ctx.url + "/admin/query", { method: "POST", headers: { Authorization: `Bearer ${adminToken}`, "app-id": appId, "content-type": "application/json" }, body: "{not json" });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = text; }
  return httpView({ status: res.status, body: json });
});

// ws: the session-level error frames
probe("ws/param-malformed/invalid-op", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "frobnicate" }));
  c.close();
  return v;
});
probe("ws/validation-failed/init-twice", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "init", "app-id": appId, versions: {} }));
  c.close();
  return v;
});
probe("ws/validation-failed/before-init", async (ctx) => {
  const c = await session(ctx, { init: false });
  const v = wsView(await wsError(c, { op: "add-query", q: { probe: {} } }));
  c.close();
  return v;
});
probe("ws/record-not-found/init-unknown-app", async (ctx) => {
  const c = await session(ctx, { init: false });
  const v = wsView(await wsError(c, { op: "init", "app-id": uuid(), versions: {} }));
  c.close();
  return v;
});
probe("ws/param-malformed/init-app-id", async (ctx) => {
  const c = await session(ctx, { init: false });
  const v = wsView(await wsError(c, { op: "init", "app-id": "nope", versions: {} }));
  c.close();
  return v;
});
probe("ws/record-not-found/init-bad-refresh-token", async (ctx) => {
  const c = await session(ctx, { init: false });
  const v = wsView(await wsError(c, { op: "init", "app-id": appId, "refresh-token": uuid(), versions: {} }));
  c.close();
  return v;
});
probe("ws/param-missing/join-room", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "join-room", "room-type": "chat" }));
  c.close();
  return v;
});
probe("ws/validation-failed/set-presence-not-joined", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "set-presence", "room-id": "never-joined", data: { x: 1 } }));
  c.close();
  return v;
});
probe("ws/validation-failed/add-query-null", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "add-query", q: null }));
  c.close();
  return v;
});
probe("ws/validation-failed/start-sync-non-admin", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "start-sync", q: { probe: {} } }));
  c.close();
  return v;
});
probe("ws/validation-failed/tx-steps-not-coll", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "transact", "tx-steps": "nope" }));
  c.close();
  return v;
});
probe("ws/param-missing/transact-no-steps", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "transact" }));
  c.close();
  return v;
});
probe("ws/validation-failed/append-stream-missing", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "append-stream", "stream-id": uuid(), offset: 0, chunks: ["x"] }));
  c.close();
  return v;
});
probe("ws/validation-failed/subscribe-stream-missing", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "subscribe-stream", "stream-id": uuid() }));
  c.close();
  return v;
});
probe("ws/validation-failed/subscribe-stream-no-id", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "subscribe-stream" }));
  c.close();
  return v;
});
probe("ws/validation-failed/unsubscribe-stream-missing", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "unsubscribe-stream", "subscribe-event-id": uuid() }));
  c.close();
  return v;
});
probe("ws/record-not-unique/attr-ident", async (ctx) => {
  const c = await session(ctx, { token: null });
  const attr = (id) => ["add-attr", { id, "forward-identity": [id, "dupns", "name"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }];
  const first = await wsOk(c, { op: "transact", "tx-steps": [attr(FIXED.dupAttr)] });
  if (first.op !== "transact-ok") {
    c.close();
    return { firstFailed: wsView(first) };
  }
  const v = wsView(await wsError(c, { op: "transact", "tx-steps": [attr(mk())] }));
  c.close();
  return v;
});
probe("ws/permission-denied/transact", async (ctx) => {
  await pushRules(ctx);
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "transact", "tx-steps": [["add-triple", FIXED.entity, FIXED.probeId, FIXED.entity], ["add-triple", FIXED.entity, FIXED.probeName, "x"]] }));
  c.close();
  return v;
});
probe("ws/permission-evaluation-failed/query", async (ctx) => {
  await pushRules(ctx);
  const seed = await admin(ctx, "/admin/transact", { steps: [["update", "probe2", FIXED.entity2, { s: "abc" }]] });
  if (seed.status !== 200) return { seedFailed: httpView(seed) };
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "add-query", q: { probe2: {} } }));
  c.close();
  return v;
});
probe("ws/validation-failed/query-bad-op", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "add-query", q: { probe: { $: { where: { name: { $frob: 1 } } } } } }));
  c.close();
  return v;
});
probe("ws/validation-failed/aggregate-non-admin", async (ctx) => {
  const c = await session(ctx);
  const v = wsView(await wsError(c, { op: "add-query", q: { probe: { $: { aggregate: "count" } } } }));
  c.close();
  return v;
});
probe("ws/validation-failed/mode-create-exists", async (ctx) => {
  const c = await session(ctx, { token: null });
  const eid = mk();
  const first = await wsOk(c, { op: "transact", "tx-steps": [["add-triple", eid, FIXED.probeId, eid]] });
  if (first.op !== "transact-ok") {
    c.close();
    return { firstFailed: wsView(first) };
  }
  const v = wsView(await wsError(c, { op: "transact", "tx-steps": [["add-triple", eid, FIXED.probeName, "x", { mode: "create" }]] }));
  c.close();
  return v;
});

// ---------------------------------------------------------------------------

async function setup(ctx) {
  // attrs the probes refer to by fixed id (probe.id / probe.name, probe2.id / probe2.s)
  const res = await admin(ctx, "/admin/transact", {
    steps: [
      ["add-attr", { id: FIXED.probeId, "forward-identity": [FIXED.probeId, "probe", "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": true }],
      ["add-attr", { id: FIXED.probeName, "forward-identity": [FIXED.probeName, "probe", "name"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }],
      ["add-attr", { id: FIXED.probe2Id, "forward-identity": [FIXED.probe2Id, "probe2", "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": true }],
      ["add-attr", { id: FIXED.probe2S, "forward-identity": [FIXED.probe2S, "probe2", "s"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }],
    ],
  });
  if (res.status !== 200) throw new Error(`[${ctx.name}] setup transact failed: ${JSON.stringify(res.body)}`);
}

const allowed = fs.existsSync(path.join(here, "errors-allowed.json"))
  ? JSON.parse(fs.readFileSync(path.join(here, "errors-allowed.json"), "utf8"))
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
  console.error(`ERROR MATRIX MISMATCH ${p.name}\n  legacy: ${JSON.stringify(l)}\n  rust:   ${JSON.stringify(r)}`);
}
if (failures) {
  console.error(`ERROR MATRIX FAILED: ${failures} of ${probes.length} probes differ`);
  process.exit(1);
}
console.log(`ERROR MATRIX PASSED: ${probes.length} probes match (${allowedHits} allowed divergences)`);
