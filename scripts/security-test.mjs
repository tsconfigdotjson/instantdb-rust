// Security e2e for the 2026-09-04 audit fixes that have no legacy
// counterpart to diff against: signed storage URLs and served-blob headers,
// dash bearer parsing, admin-route auth gates, system-namespace guards over
// the socket, cross-session sync-table removal, the per-op handler timeout,
// stream reconnect tokens, and the $users.allow.create signup gate.
//
// Usage: node security-test.mjs <app-id> <admin-token>
// Env: SERVER_URL (default http://localhost:8888)
//      TIMEOUT_NODE_URL: a node booted with INSTANT_HANDLE_RECEIVE_TIMEOUT_MS=1
//        (the timeout section is skipped when unset)
//      DATABASE_URL for the rules writes
import { execSync } from "node:child_process";

const [appId, adminToken] = process.argv.slice(2);
if (!appId || !adminToken) throw new Error("usage: node security-test.mjs <app-id> <admin-token>");
const SERVER = process.env.SERVER_URL || "http://localhost:8888";
const TIMEOUT_NODE = process.env.TIMEOUT_NODE_URL;
const DB = process.env.DATABASE_URL || "postgres://instant:instant@localhost:5432/instant";
const uuid = () => crypto.randomUUID();
const assert = (c, m) => {
  if (!c) throw new Error("ASSERT FAILED: " + m);
  console.log("ok:", m);
};
const psql = (sql) => execSync(`psql "${DB}" -q -v ON_ERROR_STOP=1 -f -`, { input: sql });

function connect(name, base = SERVER) {
  const ws = new WebSocket(`${base.replace(/^http/, "ws")}/runtime/session?app_id=${appId}`);
  const inbox = [];
  const waiters = [];
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    for (const m of Array.isArray(msg) ? msg : [msg]) {
      inbox.push(m);
      for (let i = waiters.length - 1; i >= 0; i--) {
        const [pred, resolve] = waiters[i];
        if (pred(m)) {
          waiters.splice(i, 1);
          resolve(m);
        }
      }
    }
  };
  const send = (msg) => {
    const withId = { "client-event-id": uuid(), ...msg };
    ws.send(JSON.stringify(withId));
    return withId["client-event-id"];
  };
  const waitFor = (pred, timeout = 8000) =>
    new Promise((resolve, reject) => {
      const existing = inbox.find(pred);
      if (existing) return resolve(existing);
      const t = setTimeout(() => reject(new Error(`timeout (${name})`)), timeout);
      waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
    });
  const expectErr = async (msg) => {
    const ceid = send(msg);
    return waitFor((m) => m.op === "error" && m["client-event-id"] === ceid);
  };
  return { ws, send, waitFor, expectErr, inbox, open: new Promise((r) => (ws.onopen = r)), close: () => ws.close() };
}

const adminHeaders = { "app-id": appId, authorization: `Bearer ${adminToken}`, "content-type": "application/json" };
const call = async (method, path, { headers = {}, body } = {}) => {
  const res = await fetch(`${SERVER}${path}`, {
    method,
    headers: { "content-type": "application/json", "app-id": appId, ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}
  return { status: res.status, body: json, text, headers: res.headers };
};

// ---------------------------------------------------------------------------
// 1. signed storage URLs + served-blob headers
{
  const html = "<script>alert(1)</script>";
  const up = await fetch(`${SERVER}/admin/storage/upload`, {
    method: "PUT",
    headers: { ...adminHeaders, "content-type": "text/html", "content-disposition": "inline", path: "sec/page.html" },
    body: html,
  });
  assert(up.status === 200, "html upload accepted");
  const files = await call("POST", "/admin/query", { headers: adminHeaders, body: { query: { $files: { $: { where: { path: "sec/page.html" } } } } } });
  const url = files.body?.$files?.[0]?.url;
  assert(url, "$files row carries a serve url");
  const served = await fetch(url);
  assert(served.status === 200 && (await served.text()) === html, "valid signature serves the bytes");
  assert(served.headers.get("content-type").startsWith("text/html"), "uploader content-type preserved");
  assert(served.headers.get("content-security-policy") === "sandbox; default-src 'none'", "served blobs are sandboxed");
  assert(served.headers.get("x-content-type-options") === "nosniff", "nosniff on served blobs");
  const u = new URL(url);
  const tampered = new URL(url);
  tampered.searchParams.set("sig", u.searchParams.get("sig").replace(/^./, (c) => (c === "0" ? "1" : "0")));
  assert((await fetch(tampered)).status === 403, "tampered signature is refused");
  const future = new URL(url);
  future.searchParams.set("d", String(Number(u.searchParams.get("d")) + 1));
  assert((await fetch(future)).status === 403, "future signing day is refused");
  const stale = new URL(url);
  stale.searchParams.set("d", String(Number(u.searchParams.get("d")) - 8));
  assert((await fetch(stale)).status === 403, "expired signing day is refused");
  const unsigned = new URL(url);
  unsigned.searchParams.delete("sig");
  assert((await fetch(unsigned)).status === 403, "missing signature is refused");
}

// ---------------------------------------------------------------------------
// 2. dash bearer parsing + admin-route gates
{
  const bare = await call("GET", `/dash/apps/${appId}/schema/pull`, { headers: { authorization: adminToken } });
  assert(bare.status === 400 && bare.body?.type === "param-malformed", "dash rejects a bare token");
  const ok = await call("GET", `/dash/apps/${appId}/schema/pull`, { headers: { authorization: `Bearer ${adminToken}` } });
  assert(ok.status === 200 && ok.body?.schema, "dash accepts the Bearer form");

  const guest = { "as-guest": "true" };
  for (const [method, path, body] of [
    ["POST", "/admin/refresh_tokens", { email: "sec@example.com" }],
    ["POST", "/admin/magic_code", { email: "sec@example.com" }],
    ["GET", "/admin/users?email=sec@example.com", undefined],
    ["DELETE", "/admin/users?email=sec@example.com", undefined],
    ["POST", "/admin/sign_out", { email: "sec@example.com" }],
    ["POST", "/admin/sign_in_guest", {}],
    ["GET", "/admin/rooms/presence?room-type=x&room-id=y", undefined],
    ["POST", "/admin/storage/signed-upload-url", { filename: "x.txt" }],
    ["GET", "/admin/storage/signed-download-url?filename=x.txt", undefined],
    ["GET", "/admin/storage/files", undefined],
    ["POST", "/admin/query_perms_check", { query: { $users: {} } }],
    ["POST", "/admin/transact_perms_check", { steps: [], "rules-override": { $default: { allow: { $default: "true" } } }, "dangerously-commit-tx": true }],
  ]) {
    const r = await call(method, path, { headers: guest, body });
    assert(r.status === 400 && r.body?.type === "param-missing", `${method} ${path.split("?")[0]} needs the admin token even with as-guest`);
  }
  const q = await call("POST", "/admin/query", { headers: guest, body: { query: { $users: {} } } });
  assert(q.status === 200 && Array.isArray(q.body?.$users) && q.body.$users.length === 0, "as-guest still works on /admin/query (rule-filtered)");
  const miss = await call("GET", "/admin/users?email=nobody-sec@example.com", { headers: adminHeaders });
  assert(miss.status === 200 && miss.body?.user === null, "unknown user is {user: null}");
}

// ---------------------------------------------------------------------------
// 3. socket guards: $stream/ paths, reserved reverse identities, reconnect tokens
const admin = connect("ADMIN");
await admin.open;
admin.send({ op: "init", "app-id": appId, "__admin-token": adminToken });
const initOk = await admin.waitFor((m) => m.op === "init-ok");
const attrId = (etype, label) => initOk.attrs.find((a) => a["forward-identity"][1] === etype && a["forward-identity"][2] === label).id;
{
  const f = uuid();
  const err = await admin.expectErr({ op: "transact", "tx-steps": [["add-triple", f, attrId("$files", "id"), f], ["add-triple", f, attrId("$files", "path"), "$stream/x"]] });
  assert(err.message.includes("The path for stream files can't be edited."), "$stream/ file paths are locked for admins too");

  const mkLink = (rev) => ["add-attr", { id: uuid(), "forward-identity": [uuid(), "secthings", "link"], "reverse-identity": [uuid(), ...rev], "value-type": "ref", cardinality: "many", "unique?": false, "index?": false }];
  const e1 = await admin.expectErr({ op: "transact", "tx-steps": [mkLink(["$magicCodes", "secthings"])] });
  assert(e1.message.includes("$ is reserved for system tables"), "reverse identity can't open a system namespace");
  const e2 = await admin.expectErr({ op: "transact", "tx-steps": [mkLink(["$users", "email"])] });
  assert(e2.message.includes("$users.email is a system column"), "reverse identity can't claim a catalog ident");
  const nameAttr = uuid();
  admin.send({ op: "transact", "tx-steps": [["add-attr", { id: nameAttr, "forward-identity": [uuid(), "secthings", "name"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }]] });
  await admin.waitFor((m) => m.op === "transact-ok");
  const e3 = await admin.expectErr({ op: "transact", "tx-steps": [["update-attr", { id: nameAttr, "forward-identity": [uuid(), "$users", "email"] }]] });
  assert(e3.message.includes("$users.email is a system column"), "renaming onto a catalog ident is refused");

  const e4 = await admin.expectErr({ op: "start-stream", "client-id": "sec-stream" });
  assert(e4.type === "param-missing" && e4.message === 'Missing parameter: ["reconnect-token"]', "start-stream requires a reconnect token");
  const e5 = await admin.expectErr({ op: "start-stream", "client-id": "sec-stream", "reconnect-token": "not-a-uuid" });
  assert(e5.type === "param-malformed", "reconnect token must be a uuid");
}

// ---------------------------------------------------------------------------
// 4. remove-sync from another session leaves the subscription alone
{
  const idAttr = uuid(), nameAttr = uuid();
  admin.send({
    op: "transact",
    "tx-steps": [
      ["add-attr", { id: idAttr, "forward-identity": [uuid(), "secdocs", "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": false }],
      ["add-attr", { id: nameAttr, "forward-identity": [uuid(), "secdocs", "name"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }],
    ],
  });
  await admin.waitFor((m) => m.op === "transact-ok" && admin.inbox.filter((f) => f.op === "transact-ok").length >= 2);
  admin.send({ op: "start-sync", q: { secdocs: {} } });
  const started = await admin.waitFor((m) => m.op === "start-sync-ok");
  await admin.waitFor((m) => m.op === "sync-init-finish");
  const other = connect("OTHER");
  await other.open;
  other.send({ op: "init", "app-id": appId, "__admin-token": adminToken });
  await other.waitFor((m) => m.op === "init-ok");
  other.send({ op: "remove-sync", "subscription-id": started["subscription-id"], "keep-subscription": false });
  await new Promise((r) => setTimeout(r, 500));
  assert(!other.inbox.some((m) => m.op === "error"), "foreign remove-sync is a silent no-op");
  const e = uuid();
  other.send({ op: "transact", "tx-steps": [["add-triple", e, idAttr, e], ["add-triple", e, nameAttr, "still synced"]] });
  await other.waitFor((m) => m.op === "transact-ok");
  const upd = await admin.waitFor((m) => m.op === "sync-update-triples" && JSON.stringify(m).includes("still synced"));
  assert(upd["subscription-id"] === started["subscription-id"], "the owner's subscription survived");
  other.close();
}

// ---------------------------------------------------------------------------
// 5. per-op handler timeout
if (TIMEOUT_NODE) {
  const slow = connect("SLOW", TIMEOUT_NODE);
  await slow.open;
  const err = await slow.expectErr({ op: "init", "app-id": appId });
  assert(err.type === "operation-timed-out" && err.status === 500, "overrunning handlers answer operation-timed-out");
  assert(err.message === "Operation timed out: handle-receive" && err.hint?.["timeout-ms"] === 1, "timeout error shape");
  slow.close();
} else {
  console.log("skip: TIMEOUT_NODE_URL unset");
}

// ---------------------------------------------------------------------------
// 6. $users.allow.create gates signups (and a denied check keeps the code)
{
  const rules = { $users: { allow: { create: "data.email != null && data.email.endsWith('@allowed.example')" } } };
  psql(`INSERT INTO rules (app_id, code) VALUES ('${appId}', $r$${JSON.stringify(rules)}$r$::jsonb)
        ON CONFLICT (app_id) DO UPDATE SET code = rules.code || EXCLUDED.code`);
  const guest = await call("POST", "/runtime/auth/sign_in_guest", { body: { "app-id": appId } });
  assert(guest.status === 400 && guest.body?.type === "permission-denied", "guest sign-in is denied by the create rule");
  const denied = "sec-denied@example.com";
  const code = (await call("POST", "/admin/magic_code", { headers: adminHeaders, body: { email: denied } })).body?.code;
  assert(code, "admin-issued magic code");
  const v1 = await call("POST", "/runtime/auth/verify_magic_code", { body: { "app-id": appId, email: denied, code } });
  assert(v1.status === 400 && v1.body?.type === "permission-denied", "magic-code signup is denied by the create rule");
  const allowed = "sec-ok@allowed.example";
  const code2 = (await call("POST", "/admin/magic_code", { headers: adminHeaders, body: { email: allowed } })).body?.code;
  const v2 = await call("POST", "/runtime/auth/verify_magic_code", { body: { "app-id": appId, email: allowed, code: code2 } });
  assert(v2.status === 200 && v2.body?.user?.email === allowed, "an allowed email signs up");
  psql(`UPDATE rules SET code = code - '$users' WHERE app_id = '${appId}'`);
  const v3 = await call("POST", "/runtime/auth/verify_magic_code", { body: { "app-id": appId, email: denied, code } });
  assert(v3.status === 200 && v3.body?.created === true, "the denied check did not burn the code");
  const guest2 = await call("POST", "/runtime/auth/sign_in_guest", { body: { "app-id": appId } });
  assert(guest2.status === 200 && guest2.body?.user?.refresh_token, "guest sign-in works once the rule is gone");
}

// CORS preflight: the self-hosted dashboard is a different origin than the
// API, so every JSON POST it makes is preflighted. The preflight must be
// answered by the CORS layer, not the method-not-allowed 404.
{
  const pre = await fetch(`${SERVER}/dash/auth/send_magic_code`, {
    method: "OPTIONS",
    headers: {
      Origin: "https://dash.example.com",
      "Access-Control-Request-Method": "POST",
      "Access-Control-Request-Headers": "content-type,authorization",
    },
  });
  assert(pre.ok && pre.headers.get("access-control-allow-origin") === "*", "preflight to a POST-only route is allowed");
  const miss = await fetch(`${SERVER}/no/such/route`, { headers: { Origin: "https://dash.example.com" } });
  assert(miss.status === 404 && miss.headers.get("access-control-allow-origin") === "*", "404s carry CORS headers");
}

admin.close();
console.log("SECURITY TEST PASSED");
process.exit(0);
