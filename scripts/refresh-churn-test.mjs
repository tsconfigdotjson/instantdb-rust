// Refresh under session churn (nightly fuzz-perms seed 9, run 45620798435).
//
// Legacy shares one in-flight datalog computation between every session that
// asks for the same query and cancels it when the session that started it
// disconnects, so a session that did nothing can get a 500 on its refresh
// and then never be refreshed again (docs/PARITY.md, "refresh failing because
// another session's shared query was cancelled"). This server must not: here
// stable subscribers hold nested subscriptions under a request.time view rule
// (the seed 9 rules) while other sessions open, add-query the same queries
// and disconnect with the reply possibly still in flight, and an admin
// session keeps writing. Asserted: the stable sessions get no error frame at
// all, and each one's folded subscription state (add-query-ok, then every
// refresh-ok) ends equal to what a brand-new session is answered.
//
// Usage: node scripts/refresh-churn-test.mjs <app-id> <admin-token> [seconds]
// Env:   API_URL (default http://localhost:8888), DATABASE_URL (rules setup;
//        default postgres://instant:instant@localhost:5432/instant)

import { execSync } from "node:child_process";

const [appId, adminToken, secs = "20"] = process.argv.slice(2);
if (!appId || !adminToken) throw new Error("usage: node refresh-churn-test.mjs <app-id> <admin-token> [seconds]");
const API = process.env.API_URL || "http://localhost:8888";
const DB = process.env.DATABASE_URL || "postgres://instant:instant@localhost:5432/instant";
const uuid = () => crypto.randomUUID();

function connect() {
  const ws = new WebSocket(`${API.replace(/^http/, "ws")}/runtime/session?app_id=${appId}`, {
    headers: { origin: "https://churn.example", "x-forwarded-for": "10.0.0.1, 10.0.0.2" },
  });
  const frames = [];
  const waiters = [];
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    for (const m of Array.isArray(msg) ? msg : [msg]) {
      frames.push(m);
      for (let i = waiters.length - 1; i >= 0; i--) {
        if (waiters[i][0](m)) waiters.splice(i, 1)[0][1](m);
      }
    }
  };
  let lastFrameAt = Date.now();
  ws.addEventListener("message", () => (lastFrameAt = Date.now()));
  const open = new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = reject;
  });
  const send = (m) => {
    const ceid = uuid();
    ws.send(JSON.stringify({ "client-event-id": ceid, ...m }));
    return ceid;
  };
  const waitFor = (pred, timeout = 10000) =>
    new Promise((resolve, reject) => {
      const hit = frames.find(pred);
      if (hit) return resolve(hit);
      const t = setTimeout(() => reject(new Error("timeout waiting for a frame")), timeout);
      waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
    });
  const reply = (ceid, timeout) => waitFor((m) => m["client-event-id"] === ceid, timeout);
  return { ws, frames, open, send, reply, quietFor: () => Date.now() - lastFrameAt };
}

async function session(admin) {
  const c = connect();
  await c.open;
  const init = { op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.21.0" } };
  if (admin) init["__admin-token"] = adminToken;
  const ok = await c.reply(c.send(init));
  if (ok.op !== "init-ok") throw new Error(`init failed: ${JSON.stringify(ok).slice(0, 300)}`);
  return c;
}

let passed = 0;
const assert = (cond, msg) => {
  if (!cond) throw new Error(`ASSERT FAILED: ${msg}`);
  passed++;
};

// what the client keeps from a result: its triples, as a set
const triplesOf = (result) => {
  const out = new Set();
  const walk = (node) => {
    for (const row of node?.data?.["datalog-result"]?.["join-rows"] ?? []) {
      for (const t of row) out.add(JSON.stringify(t.slice(0, 3)));
    }
    for (const child of node?.["child-nodes"] ?? []) walk(child);
  };
  for (const node of result ?? []) walk(node);
  return [...out].sort();
};
// add-query echoes q back as sent; key on a stable form of it
const keyOf = (q) => JSON.stringify(q, (_k, v) => (v && typeof v === "object" && !Array.isArray(v) ? Object.fromEntries(Object.entries(v).sort()) : v));

const ids = {
  id: uuid(), p1: uuid(), owner: uuid(), ownerRev: uuid(), oid: uuid(), oname: uuid(),
};
const blob = (id, ns, label, extra = {}) => [
  "add-attr",
  { id, "forward-identity": [uuid(), ns, label], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, ...extra },
];
const schema = [
  blob(ids.id, "pz", "id", { "unique?": true, "index?": true }),
  blob(ids.p1, "pz", "p1"),
  blob(ids.oid, "pzowner", "id", { "unique?": true, "index?": true }),
  blob(ids.oname, "pzowner", "name", { "index?": true }),
  ["add-attr", { id: ids.owner, "forward-identity": [uuid(), "pz", "owner"], "reverse-identity": [uuid(), "pzowner", "items"], "value-type": "ref", cardinality: "one", "unique?": false, "index?": false }],
];
const ents = Array.from({ length: 10 }, () => uuid());
const owners = Array.from({ length: 3 }, () => uuid());
const seed = [
  ...owners.flatMap((o, i) => [["add-triple", o, ids.oid, o], ["add-triple", o, ids.oname, ["ann", "bob", "cy"][i]]]),
  ...ents.flatMap((e, i) => [["add-triple", e, ids.id, e], ["add-triple", e, ids.p1, "a"], ["add-triple", e, ids.owner, owners[i % 3]]]),
];
// the seed 9 view rules: pz visible through request.time, pzowner hidden
const hidden = uuid();
const rules = {
  pz: { allow: { view: `request.time > timestamp('2000-01-01T00:00:00Z') || data.id == '${hidden}'`, create: "true", update: "true", delete: "true" } },
  pzowner: { allow: { view: `false || data.id == '${hidden}'`, create: "true", update: "true", delete: "true" } },
};
execSync(`psql "${DB}" -q -v ON_ERROR_STOP=1 -f -`, {
  input: `INSERT INTO rules (app_id, code) VALUES ('${appId}', $r$${JSON.stringify(rules)}$r$::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code;`,
});

const adm = await session(true);
for (const steps of [schema, seed]) {
  const r = await adm.reply(adm.send({ op: "transact", "tx-steps": steps }));
  assert(r.op === "transact-ok", `setup transact: ${JSON.stringify(r).slice(0, 300)}`);
}

console.log("== stable subscribers ==");
const QS = [{ pz: {} }, { pz: { owner: { $: { fields: ["name"] } } } }, { pz: { owner: {} } }];
const stable = [];
for (let i = 0; i < 3; i++) {
  const c = await session(false);
  for (const q of QS) {
    const r = await c.reply(c.send({ op: "add-query", q }));
    assert(r.op === "add-query-ok", `subscribe: ${JSON.stringify(r).slice(0, 300)}`);
  }
  stable.push(c);
}

console.log(`== ${secs}s of writes while sessions add-query and disconnect ==`);
const deadline = Date.now() + Number(secs) * 1000;
let writes = 0;
let churned = 0;
const pick = (xs) => xs[Math.floor(Math.random() * xs.length)];
const writer = (async () => {
  while (Date.now() < deadline) {
    const e = pick(ents);
    const steps = Math.random() < 0.5 ? [["add-triple", e, ids.p1, pick(["a", "b", "c"])]] : [["add-triple", e, ids.owner, pick(owners)]];
    const r = await adm.reply(adm.send({ op: "transact", "tx-steps": steps }));
    assert(r.op === "transact-ok", `write: ${JSON.stringify(r).slice(0, 300)}`);
    writes++;
    await new Promise((res) => setTimeout(res, 15));
  }
})();
const churner = async () => {
  while (Date.now() < deadline) {
    const c = await session(false);
    const ceid = c.send({ op: "add-query", q: pick(QS) });
    // half disconnect with the add-query (and any refresh it gets) in flight
    if (Math.random() < 0.5) await c.reply(ceid).catch(() => {});
    else await new Promise((res) => setTimeout(res, Math.random() * 30));
    c.ws.close();
    churned++;
  }
};
await Promise.all([writer, churner(), churner(), churner(), churner()]);
// let the last refreshes land
for (const start = Date.now(); Date.now() - start < 15000; ) {
  if (stable.every((c) => c.quietFor() > 1500)) break;
  await new Promise((res) => setTimeout(res, 100));
}
console.log(`${writes} writes, ${churned} churned sessions`);

console.log("== stable sessions: no errors, folded state is current ==");
const fresh = await session(false);
const want = {};
for (const q of QS) {
  const r = await fresh.reply(fresh.send({ op: "add-query", q }));
  assert(r.op === "add-query-ok", "fresh add-query");
  want[keyOf(q)] = triplesOf(r.result);
}
for (const [i, c] of stable.entries()) {
  const errors = c.frames.filter((m) => m.op === "error");
  assert(errors.length === 0, `stable session ${i} got error frames: ${JSON.stringify(errors).slice(0, 600)}`);
  const refreshes = c.frames.filter((m) => m.op === "refresh-ok").length;
  assert(refreshes > 0, `stable session ${i} was never refreshed`);
  const state = {};
  for (const m of c.frames) {
    if (m.op === "add-query-ok") state[keyOf(m.q)] = triplesOf(m.result);
    if (m.op === "refresh-ok") for (const x of m.computations ?? []) state[keyOf(x["instaql-query"])] = triplesOf(x["instaql-result"]);
  }
  for (const q of QS) {
    assert(JSON.stringify(state[keyOf(q)]) === JSON.stringify(want[keyOf(q)]), `stable session ${i} is stale for ${JSON.stringify(q)}`);
  }
}
for (const c of [...stable, adm, fresh]) c.ws.close();
console.log(`refresh-churn: ${passed} assertions passed`);
process.exit(0);
