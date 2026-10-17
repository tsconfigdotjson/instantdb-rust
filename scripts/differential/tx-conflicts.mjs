// Concurrent transact conflicts (issue #53): rust-only. Sessions send
// transacts at once that touch overlapping entities in different orders —
// multi-entity updates in shuffled order, creates, links and delete-entity
// (forward and reverse-ref rows) — the way two clients editing the same rows
// do. Postgres aborts one side of a lock cycle with a deadlock (40P01);
// legacy surfaces that as a 500 sql-exception. The rust server orders its row
// locks and retries a conflicted tx, so every transact here must be answered
// with transact-ok or a 4xx error, never a 5xx.
//
// Usage: node tx-conflicts.mjs <app-id> <admin-token> [seed] [txs-per-session]
// Env: RUST_URL (default http://localhost:8888)

import { connect, makeIdFactory, uuid } from "./lib.mjs";

const appId = process.argv[2];
const adminToken = process.argv[3];
const seed = Number(process.argv[4] ?? 1);
const perSession = Number(process.argv[5] ?? 60);
if (!appId || !adminToken) throw new Error("usage: node tx-conflicts.mjs <app-id> <admin-token> [seed] [txs-per-session]");
const url = process.env.RUST_URL || "http://localhost:8888";

const SESSIONS = 8;
// a small pool, so concurrent txs overlap on most writes. Each tx touches 2-4
// entities: when every tx locked most of the pool, txs queued behind each
// other's locks (and Postgres' 1s deadlock_timeout for cycles across
// statements, which the server retries) past the 5s handler timeout.
const POOL = 12;

function prng(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rand = prng(seed * 7919 + 13);
const pick = (arr) => arr[Math.floor(rand() * arr.length)];
const shuffle = (arr) => {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
};

const mk = makeIdFactory(appId);
const NS = "conflict";
const attrs = { id: mk(), v: mk(), g: mk(), next: mk(), prev: mk() };
const blob = (id, label, extra = {}) => [
  "add-attr",
  { id, "forward-identity": [id, NS, label], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true, ...extra },
];
const schema = [
  blob(attrs.id, "id", { "unique?": true, "index?": true }),
  blob(attrs.v, "v", { "index?": true, "checked-data-type": "number" }),
  blob(attrs.g, "g", { "index?": true }),
  [
    "add-attr",
    {
      id: attrs.next,
      "forward-identity": [attrs.next, NS, "next"],
      "reverse-identity": [attrs.prev, NS, "prev"],
      "value-type": "ref",
      cardinality: "many",
      "unique?": false,
      "index?": false,
      isUnsynced: true,
    },
  ],
];
const pool = Array.from({ length: POOL }, () => mk());
const create = (e) => [
  ["add-triple", e, attrs.id, e],
  ["add-triple", e, attrs.v, Math.floor(rand() * 100)],
  ["add-triple", e, attrs.g, pick(["x", "y", "z", 7, true])],
];

const txFor = () => {
  const k = rand();
  const some = shuffle(pool).slice(0, 2 + Math.floor(rand() * 3));
  // shuffled multi-entity updates: two of these lock the same rows in
  // opposite orders unless the server orders them
  if (k < 0.35) return some.map((e) => ["add-triple", e, attrs.v, Math.floor(rand() * 100)]);
  if (k < 0.55) return some.flatMap(create);
  if (k < 0.7) return some.slice(1).map((e) => ["add-triple", some[0], attrs.next, e]);
  if (k < 0.85) return [...some.slice(1).map((e) => ["add-triple", e, attrs.v, 1]), ["delete-entity", some[0], NS]];
  return some.map((e) => ["delete-entity", e, NS]);
};

async function open(name) {
  const c = connect(url, appId, `conflict:${name}`);
  await c.open;
  c.send({ "client-event-id": uuid(), op: "init", "app-id": appId, "__admin-token": adminToken, versions: { "@instantdb/core": "v0.21.0" } });
  const ok = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
  if (ok.op !== "init-ok") throw new Error(`init ${name} failed: ${JSON.stringify(ok).slice(0, 400)}`);
  return c;
}

const transact = async (c, steps) => {
  const ceid = c.send({ "client-event-id": uuid(), op: "transact", "tx-steps": steps });
  const r = await c.waitFor((m) => (m.op === "transact-ok" || m.op === "error") && m["client-event-id"] === ceid, 60000);
  c.takeNewFrames();
  return r;
};

const conns = await Promise.all(Array.from({ length: SESSIONS }, (_, i) => open(String(i))));
const setup = await transact(conns[0], [...schema, ...pool.flatMap(create)]);
if (setup.op !== "transact-ok") throw new Error(`schema tx failed: ${JSON.stringify(setup).slice(0, 400)}`);

// every session's script is drawn up front so the seed alone fixes the run
const scripts = conns.map(() => Array.from({ length: perSession }, txFor));
let ok = 0;
// "status type: message" -> count
const errors = new Map();
const serverErrors = [];
await Promise.all(
  conns.map(async (c, i) => {
    for (const steps of scripts[i]) {
      const r = await transact(c, steps);
      if (r.op === "transact-ok") {
        ok++;
        continue;
      }
      const key = `${r.status} ${r.type}: ${r.message}`;
      errors.set(key, (errors.get(key) ?? 0) + 1);
      if (!(r.status < 500)) serverErrors.push({ session: i, steps, reply: r });
    }
  }),
);
for (const c of conns) c.close();

for (const [e, n] of errors) console.log(`note: ${n} transact(s) failed with ${e.slice(0, 200)}`);
if (serverErrors.length) {
  for (const { session, steps, reply } of serverErrors.slice(0, 5)) {
    console.error(`session ${session}: ${JSON.stringify(steps)}\n  -> ${JSON.stringify({ status: reply.status, type: reply.type, message: reply.message })}`);
  }
  console.error(`TX-CONFLICTS FAILED: ${serverErrors.length} of ${SESSIONS * perSession} transacts got a 5xx (seed ${seed}, ${url})`);
  process.exit(1);
}
console.log(`TX-CONFLICTS PASSED: ${ok} of ${SESSIONS * perSession} transacts ok, no 5xx (seed ${seed}, ${url})`);
