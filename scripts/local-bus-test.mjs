// INSTANT_BUS=local: one node, no LISTEN/NOTIFY. A transaction must still
// refresh other sessions' queries, and a dashboard status change must still
// reach live sessions as `app-status-changed` (there is no row trigger to
// announce it; the route publishes it), only when the status really changed.
// Run against a node started with INSTANT_BUS=local.
// Usage: SERVER_URL=http://localhost:8885 node scripts/local-bus-test.mjs
import { execFileSync } from "node:child_process";

const SERVER = process.env.SERVER_URL || "http://localhost:8888";
const DB = process.env.DATABASE_URL || "postgres://instant:instant@localhost:5432/instant";
const uuid = () => crypto.randomUUID();
const psql = (sql) => execFileSync("psql", [DB, "-q", "-v", "ON_ERROR_STOP=1", "-c", sql]);

let failed = 0;
const check = (ok, name) => {
  console.log(`${ok ? "ok  " : "FAIL"} ${name}`);
  if (!ok) failed++;
};

// an app owned by a dashboard user we hold a token for
const appId = uuid(), userId = uuid(), token = uuid();
psql(`INSERT INTO instant_users (id, email) VALUES ('${userId}', 'bus-${userId}@example.com');
      INSERT INTO instant_user_refresh_tokens (id, user_id) VALUES ('${token}', '${userId}');
      INSERT INTO apps (id, creator_id, title) VALUES ('${appId}', '${userId}', 'local bus');
      INSERT INTO app_admin_tokens (app_id, token) VALUES ('${appId}', '${uuid()}');`);
const dash = async (path, body) => {
  const res = await fetch(`${SERVER}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${path}: ${res.status} ${await res.text()}`);
  return res.json();
};

function connect() {
  const ws = new WebSocket(`${SERVER.replace(/^http/, "ws")}/runtime/session?app_id=${appId}`);
  const inbox = [], waiters = [];
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    for (const m of Array.isArray(msg) ? msg : [msg]) {
      inbox.push(m);
      for (let i = waiters.length - 1; i >= 0; i--) {
        if (waiters[i][0](m)) waiters.splice(i, 1)[0][1](m);
      }
    }
  };
  const send = (m) => {
    const id = uuid();
    ws.send(JSON.stringify({ "client-event-id": id, ...m }));
    return id;
  };
  const waitFor = (pred, timeout = 5000) =>
    new Promise((resolve, reject) => {
      const hit = inbox.find(pred);
      if (hit) return resolve(hit);
      const t = setTimeout(() => reject(new Error("timeout")), timeout);
      waiters.push([pred, (m) => (clearTimeout(t), resolve(m))]);
    });
  const open = new Promise((resolve) => (ws.onopen = resolve));
  return { ws, send, waitFor, open };
}
const session = async () => {
  const c = connect();
  await c.open;
  c.send({ op: "init", "app-id": appId });
  await c.waitFor((m) => m.op === "init-ok");
  return c;
};

const a = await session();
const todosId = uuid(), todosTitle = uuid(), e1 = uuid();
a.send({
  op: "transact",
  "tx-steps": [
    ["add-attr", { id: todosId, "forward-identity": [uuid(), "todos", "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": false }],
    ["add-attr", { id: todosTitle, "forward-identity": [uuid(), "todos", "title"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }],
    ["add-triple", e1, todosId, e1],
    ["add-triple", e1, todosTitle, "first"],
  ],
});
await a.waitFor((m) => m.op === "transact-ok");

// --- a transaction refreshes another session's query
const b = await session();
b.send({ op: "add-query", q: { todos: {} } });
await b.waitFor((m) => m.op === "add-query-ok");
a.send({ op: "transact", "tx-steps": [["add-triple", e1, todosTitle, "changed"]] });
const refreshed = await b
  .waitFor((m) => m.op === "refresh-ok" && JSON.stringify(m.computations).includes("changed"))
  .then(() => true, () => false);
check(refreshed, "refresh-ok after another session's transaction");

// --- status: a dashboard flip is pushed to live sessions
await dash(`/dash/apps/${appId}/status`, { status: "read-only" });
const pushed = await a
  .waitFor((m) => m.op === "app-status-changed" && m.status === "read-only")
  .then(() => true, () => false);
check(pushed, "app-status-changed pushed on a status change");
await dash(`/dash/apps/${appId}/status`, { status: "read-only" });
const again = await a
  .waitFor((m) => m !== undefined && false, 500)
  .then(() => true, () => false);
check(!again, "no push when the status did not change");
await dash(`/dash/apps/${appId}/status`, { status: "active" });
check(
  await a.waitFor((m) => m.op === "app-status-changed" && m.status === "active").then(() => true, () => false),
  "app-status-changed pushed on the way back",
);

a.ws.close();
b.ws.close();
if (failed) {
  console.error(`${failed} failed`);
  process.exit(1);
}
console.log("local bus ok");
