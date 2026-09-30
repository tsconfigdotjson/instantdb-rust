// Subscription race fuzz (issue #51): rust-only invariants for queries
// registered while transactions are in flight. Seeded bursts send
// transacts and add-queries without waiting — on one session (a transact
// then an add-query of a page that tx changes, the way an infinite query
// opens its next page from an optimistic row) and across sessions — and
// after every burst, once the frame stream has settled, checks:
//   1. ordering: no refresh-ok for a query arrives before that query's
//      add-query-ok (the client would take the older add-query-ok as the
//      latest result);
//   2. convergence: each held subscription, folded in arrival order the way
//      the client folds it (the last add-query-ok / refresh-ok wins), equals
//      what a fresh add-query of the same query answers right then.
//
// Not a differential layer: legacy runs add-query and refresh on separate
// group keys (session.clj group-key) and shares paginated datalog results
// across sessions, so it fails both checks now and then. Point it at a rust
// server booted with INSTANT_CHAOS_DELAY_MS (random pauses at the points
// where add-query and a refresh interleave) so the windows are wide enough
// to hit on every run.
//
// Usage: node fuzz-race.mjs <app-id> <admin-token> [seed] [rounds]
// Env: RACE_URL (default RUST_URL, then http://localhost:8888)

import { canon, connect, makeIdFactory, projectResult, settle, uuid } from "./lib.mjs";

const appId = process.argv[2];
const adminToken = process.argv[3];
const seed = Number(process.argv[4] ?? 1);
const rounds = Number(process.argv[5] ?? 30);
if (!appId || !adminToken) throw new Error("usage: node fuzz-race.mjs <app-id> <admin-token> [seed] [rounds]");
const url = process.env.RACE_URL || process.env.RUST_URL || "http://localhost:8888";

// the frame stream counts as settled after this long without a frame
const QUIET_MS = 400;
// subscriptions a session holds at once; past this the oldest is removed
const MAX_HELD = 4;
// admin sessions share the query cache; the guest takes the rules path
const SESSIONS = { A: "admin", B: "admin", C: "admin", G: "guest" };

function prng(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rand = prng(seed * 104729 + 7);
const pick = (arr) => arr[Math.floor(rand() * arr.length)];
const chance = (p) => rand() < p;

const mk = makeIdFactory(appId);
const NS = "race";
const attrs = { id: mk(), v: mk(), g: mk() };
const GROUPS = ["x", "y", "z"];
const blob = (id, label, extra = {}) => [
  "add-attr",
  { id, "forward-identity": [id, NS, label], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true, ...extra },
];
const schema = [
  blob(attrs.id, "id", { "unique?": true, "index?": true }),
  blob(attrs.v, "v", { "index?": true, "checked-data-type": "number" }),
  blob(attrs.g, "g", { "index?": true, "checked-data-type": "string" }),
];

// entity ids come from a fixed pool so updates and deletes hit live rows
const pool = Array.from({ length: 40 }, () => mk());
const live = new Set();
const txFor = () => {
  const k = rand();
  const dead = pool.filter((e) => !live.has(e));
  if ((k < 0.55 || live.size < 3) && dead.length) {
    const e = pick(dead);
    live.add(e);
    return [
      ["add-triple", e, attrs.id, e],
      ["add-triple", e, attrs.v, Math.floor(rand() * 60) - 20],
      ["add-triple", e, attrs.g, pick(GROUPS)],
    ];
  }
  const e = pick([...live]);
  if (k < 0.85) return [["add-triple", e, attrs.v, Math.floor(rand() * 60) - 20]];
  live.delete(e);
  return [["delete-entity", e, NS]];
};

// each session subscribes to a query at most once per run, so frames for a
// removed registration can never be folded into a later one
const used = Object.fromEntries(Object.keys(SESSIONS).map((s) => [s, new Set()]));
const queryFor = (s) => {
  for (let tries = 0; tries < 20; tries++) {
    const $ = { order: { v: pick(["asc", "desc"]) }, limit: 1 + Math.floor(rand() * 5) };
    const w = rand();
    if (w < 0.25) $.where = { g: pick(GROUPS) };
    else if (w < 0.45) $.where = { v: { [pick(["$gt", "$lt"])]: Math.floor(rand() * 40) - 10 } };
    if (chance(0.15)) {
      delete $.order;
      delete $.limit;
    }
    const q = { [NS]: { $ } };
    if (!used[s].has(canon(q))) {
      used[s].add(canon(q));
      return q;
    }
  }
  return null;
};

async function open(name, kind) {
  const c = connect(url, appId, `race:${name}`);
  await c.open;
  const init = { "client-event-id": uuid(), op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.21.0" } };
  if (kind === "admin") init["__admin-token"] = adminToken;
  c.send(init);
  const ok = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
  if (ok.op !== "init-ok") throw new Error(`init ${name} failed: ${JSON.stringify(ok).slice(0, 400)}`);
  return c;
}

const conns = {};
for (const [s, kind] of Object.entries(SESSIONS)) conns[s] = await open(s, kind);
// fresh answers come from a separate connection per auth kind
const probes = { admin: await open("probe-admin", "admin"), guest: await open("probe-guest", "guest") };
const all = [...Object.values(conns), ...Object.values(probes)];

const request = (c, msg, ops) => {
  const ceid = c.send({ "client-event-id": uuid(), ...msg });
  return c.waitFor((m) => ops.includes(m.op) && m["client-event-id"] === ceid, 20000);
};
const transact = (s, steps) => request(conns[s], { op: "transact", "tx-steps": steps }, ["transact-ok", "error"]);
const fresh = async (kind, q) => {
  const r = await request(probes[kind], { op: "add-query", q }, ["add-query-ok", "error"]);
  if (r.op !== "add-query-ok") throw new Error(`probe failed: ${JSON.stringify(r).slice(0, 400)}`);
  await request(probes[kind], { op: "remove-query", q }, ["remove-query-ok"]);
  return projectResult(r.result);
};

const setup = await transact("A", schema);
if (setup.op !== "transact-ok") throw new Error(`schema tx failed: ${JSON.stringify(setup).slice(0, 400)}`);

// per session: canonical q -> the query, for the queries currently held
const held = Object.fromEntries(Object.keys(SESSIONS).map((s) => [s, new Map()]));
const failures = [];
let ops = 0;
let checks = 0;
// "type: message" -> count, printed at the end
const txErrors = new Map();

for (let round = 0; round < rounds; round++) {
  const pending = [];
  const burst = 3 + Math.floor(rand() * 6);
  for (let j = 0; j < burst; j++) {
    const s = pick(Object.keys(SESSIONS));
    const k = rand();
    if (k < 0.45) {
      // transact, then at once subscribe to a page it may change (the
      // infinite-query pattern), sometimes from another session
      pending.push(transact(s, txFor()));
      const s2 = chance(0.7) ? s : pick(Object.keys(SESSIONS));
      const q = queryFor(s2);
      if (q) {
        held[s2].set(canon(q), q);
        pending.push(request(conns[s2], { op: "add-query", q }, ["add-query-ok", "error"]));
      }
    } else if (k < 0.8) {
      pending.push(transact(s, txFor()));
    } else if (k < 0.93) {
      const q = queryFor(s);
      if (q) {
        held[s].set(canon(q), q);
        pending.push(request(conns[s], { op: "add-query", q }, ["add-query-ok", "error"]));
      }
    } else if (held[s].size) {
      const key = pick([...held[s].keys()]);
      const q = held[s].get(key);
      held[s].delete(key);
      pending.push(request(conns[s], { op: "remove-query", q }, ["remove-query-ok"]));
    }
    ops++;
  }
  for (const s of Object.keys(SESSIONS)) {
    while (held[s].size > MAX_HELD) {
      const [key, q] = held[s].entries().next().value;
      held[s].delete(key);
      pending.push(request(conns[s], { op: "remove-query", q }, ["remove-query-ok"]));
    }
  }
  const replies = await Promise.all(pending);
  for (const r of replies) {
    // this layer checks subscriptions, not transacts: a failed tx (one that
    // raced a delete of its entity, say) only changes less data
    if (r.op === "error" && r["original-event"]?.op === "transact") {
      txErrors.set(`${r.type}: ${r.message}`, (txErrors.get(`${r.type}: ${r.message}`) ?? 0) + 1);
    } else if (r.op === "error") {
      throw new Error(`round ${round}: unexpected error ${JSON.stringify(r).slice(0, 400)}`);
    }
  }
  await settle(all, QUIET_MS);

  for (const s of Object.keys(SESSIONS)) {
    // fold every frame of the run in arrival order, as the client does
    const state = new Map();
    const answered = new Set();
    for (const m of conns[s].frames) {
      if (m.op === "add-query-ok") {
        const key = canon(m.q);
        answered.add(key);
        state.set(key, m.result);
      } else if (m.op === "refresh-ok") {
        for (const c of m.computations ?? []) {
          const key = canon(c["instaql-query"]);
          if (!answered.has(key)) {
            // a held query is subscribed once per run, so this refresh came
            // before its add-query-ok
            if (held[s].has(key)) {
              failures.push({ round, session: s, q: key, kind: "refresh-ok before add-query-ok" });
            }
            continue;
          }
          state.set(key, c["instaql-result"]);
        }
      }
    }
    for (const [key, q] of held[s]) {
      if (!state.has(key)) continue; // the add-query failed
      const got = projectResult(state.get(key));
      const want = await fresh(SESSIONS[s], q);
      checks++;
      if (canon(got) !== canon(want)) {
        failures.push({ round, session: s, q: key, kind: "stale subscription", got, want });
      }
    }
  }
  // stop at the first bad round: later rounds would repeat its failures
  if (failures.length) break;
}

for (const c of all) c.close();
for (const [e, n] of txErrors) console.log(`note: ${n} transact(s) failed with ${e.slice(0, 200)}`);
const seen = new Set();
for (const f of failures) {
  const id = `${f.session} ${f.kind} ${f.q}`;
  if (seen.has(id)) continue;
  seen.add(id);
  console.error(`round ${f.round}, session ${f.session}: ${f.kind}\n  q: ${f.q}`);
  if (f.got) {
    const key = (t) => canon(t.slice(0, 3));
    const g = new Set(f.got.triples.map(key)), w = new Set(f.want.triples.map(key));
    console.error(`  only held:  ${JSON.stringify(f.got.triples.filter((t) => !w.has(key(t))).map((t) => t.slice(0, 3)))}`);
    console.error(`  only fresh: ${JSON.stringify(f.want.triples.filter((t) => !g.has(key(t))).map((t) => t.slice(0, 3)))}`);
    if (canon(f.got["page-info"]) !== canon(f.want["page-info"])) {
      console.error(`  page-info held ${JSON.stringify(f.got["page-info"])} fresh ${JSON.stringify(f.want["page-info"])}`);
    }
  }
}
if (failures.length) {
  console.error(`FUZZ-RACE FAILED: ${seen.size} problem(s) (seed ${seed}, ${url})`);
  process.exit(1);
}
console.log(`FUZZ-RACE PASSED: ${rounds} rounds, ${ops} ops, ${checks} settled subscriptions matched a fresh answer (seed ${seed}, ${url})`);
