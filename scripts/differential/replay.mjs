// Differential replay: runs an identical op script against the legacy server
// and the rust server, folds each server's frames into client-visible state
// (see lib.mjs), and diffs the results. Every remaining diff must be listed
// in allowed-divergences.json with a client-code citation.
//
// Usage: node replay.mjs <app-id-legacy> <app-id-rust> <admin-token>
//   (app ids may be equal; both apps must exist on their server, see provision.sh)
// Env: LEGACY_URL (default http://localhost:8891), RUST_URL (default http://localhost:8888),
//      LEGACY_DATABASE_URL, RUST_DATABASE_URL (for rules setup via psql)

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  connect,
  settle,
  foldFrames,
  newState,
  projectState,
  makeIdFactory,
  canon,
  psql,
  uuid,
} from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node replay.mjs <app-id-legacy> <app-id-rust> <admin-token>");

const SERVERS = {
  legacy: {
    url: process.env.LEGACY_URL || "http://localhost:8891",
    db: process.env.LEGACY_DATABASE_URL || "postgres://instant:instant@localhost:8890/instant",
    appId: appIdLegacy,
  },
  rust: {
    url: process.env.RUST_URL || "http://localhost:8888",
    db: process.env.RUST_DATABASE_URL || "postgres://instant:instant@localhost:5432/instant",
    appId: appIdRust,
  },
};

// ---------------------------------------------------------------------------
// scenario: identical client input on both servers (ids from the deterministic
// factory), breadth across queries, transactions, errors, rooms, sync tables
// and streams.

function buildScenario() {
  const mk = makeIdFactory(appIdLegacy);
  const ids = {
    todosId: mk(), todosTitle: mk(), todosDone: mk(), todosScore: mk(),
    ownerRef: mk(), ownersId: mk(), ownersName: mk(),
    e1: mk(), e2: mk(), e3: mk(), owner1: mk(), extraAttr: mk(),
    mergeAttr: mk(), lookupTitleEid: mk(),
  };
  const attr = (id, etype, label, { unique = false, fwd, rev } = {}) => [
    "add-attr",
    {
      id,
      "forward-identity": [fwd ?? mk(), etype, label],
      "value-type": "blob",
      cardinality: "one",
      "unique?": unique,
      "index?": unique,
      isUnsynced: true,
    },
  ];
  const ceids = (() => { let i = 0; return () => { i++; return `00000000-0000-4000-9000-${String(i).padStart(12, "0")}`; }; })();
  const msg = (conn, m) => conn.send({ "client-event-id": ceids(), ...m });

  const steps = [
    {
      name: "01-init",
      run: async (env) => {
        for (const name of ["A", "B"]) {
          env.conns[name] = connect(env.url, env.appId, `${env.serverName}:${name}`);
          await env.conns[name].open;
        }
        env.conns.ADMIN = connect(env.url, env.appId, `${env.serverName}:ADMIN`);
        await env.conns.ADMIN.open;
        msg(env.conns.A, { op: "init", "app-id": env.appId, versions: { "@instantdb/core": "v0.21.0" } });
        msg(env.conns.B, { op: "init", "app-id": env.appId, versions: { "@instantdb/core": "v0.21.0" } });
        msg(env.conns.ADMIN, { op: "init", "app-id": env.appId, versions: { "@instantdb/core": "v0.21.0" }, "__admin-token": adminToken });
        await env.conns.A.waitFor((m) => m.op === "init-ok");
        await env.conns.B.waitFor((m) => m.op === "init-ok");
        await env.conns.ADMIN.waitFor((m) => m.op === "init-ok");
      },
    },
    {
      name: "02-schema-and-seed",
      run: async (env) => {
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [
            attr(ids.todosId, "todos", "id", { unique: true, fwd: ids.todosId }),
            attr(ids.todosTitle, "todos", "title", { fwd: ids.todosTitle }),
            attr(ids.todosDone, "todos", "done", { fwd: ids.todosDone }),
            attr(ids.todosScore, "todos", "score", { unique: true, fwd: ids.todosScore }),
            attr(ids.ownersId, "owners", "id", { unique: true, fwd: ids.ownersId }),
            attr(ids.ownersName, "owners", "name", { fwd: ids.ownersName }),
            [
              "add-attr",
              {
                id: ids.ownerRef,
                "forward-identity": [ids.ownerRef, "todos", "owner"],
                "reverse-identity": [mk(), "owners", "todos"],
                "value-type": "ref",
                cardinality: "many",
                "unique?": false,
                "index?": false,
                isUnsynced: true,
              },
            ],
            ["add-triple", ids.e1, ids.todosId, ids.e1],
            ["add-triple", ids.e1, ids.todosTitle, "one"],
            ["add-triple", ids.e1, ids.todosDone, false],
            ["add-triple", ids.e1, ids.todosScore, 1],
            ["add-triple", ids.owner1, ids.ownersId, ids.owner1],
            ["add-triple", ids.owner1, ids.ownersName, "alice"],
            ["add-triple", ids.e1, ids.ownerRef, ids.owner1],
          ],
        });
        await env.conns.A.waitFor((m) => m.op === "transact-ok");
      },
    },
    {
      name: "03-add-query",
      run: async (env) => {
        msg(env.conns.B, { op: "add-query", q: { todos: {} } });
        await env.conns.B.waitFor((m) => m.op === "add-query-ok");
        msg(env.conns.B, { op: "add-query", q: { todos: {} } });
        await env.conns.B.waitFor((m) => m.op === "add-query-exists");
        msg(env.conns.B, { op: "add-query", q: { todos: { owner: {} } } });
        await env.conns.B.waitFor(
          (m) => m.op === "add-query-ok" && m.q?.todos?.owner,
        );
      },
    },
    {
      name: "04-refresh",
      run: async (env) => {
        msg(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.e1, ids.todosDone, true]] });
        await env.conns.A.waitFor((m) => m.op === "transact-ok");
        await env.conns.B.waitFor((m) => m.op === "refresh-ok");
      },
    },
    {
      name: "05-new-attr-refresh",
      run: async (env) => {
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [
            attr(ids.extraAttr, "todos", "extra", { fwd: ids.extraAttr }),
            ["add-triple", ids.e1, ids.extraAttr, "x"],
          ],
        });
        await env.conns.A.waitFor((m) => m.op === "transact-ok" && env.conns.A.frames.filter((f) => f.op === "transact-ok").length >= 3);
        await env.conns.B.waitFor((m) => m.op === "refresh-ok" && "attrs" in m);
      },
    },
    {
      name: "06-pagination-cursor-roundtrip",
      run: async (env) => {
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [
            ["add-triple", ids.e2, ids.todosId, ids.e2],
            ["add-triple", ids.e2, ids.todosTitle, "two"],
            ["add-triple", ids.e3, ids.todosId, ids.e3],
            ["add-triple", ids.e3, ids.todosTitle, "three"],
          ],
        });
        await env.conns.A.waitFor((m) => m.op === "transact-ok" && env.conns.A.frames.filter((f) => f.op === "transact-ok").length >= 4);
        msg(env.conns.A, { op: "add-query", q: { todos: { $: { limit: 2, order: { serverCreatedAt: "asc" } } } } });
        const page = await env.conns.A.waitFor((m) => m.op === "add-query-ok");
        const cursor = page.result?.[0]?.data?.["page-info"]?.todos?.["end-cursor"];
        // per-server cursor round-trip (cursor timestamps are server-local)
        msg(env.conns.A, { op: "add-query", q: { todos: { $: { limit: 2, order: { serverCreatedAt: "asc" }, after: cursor } } } });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.todos?.$?.after);
      },
    },
    {
      name: "07-mutation-breadth",
      run: async (env) => {
        // deep-merge, retract, lookup-ref write, delete-entity
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [
            attr(ids.mergeAttr, "todos", "meta", { fwd: ids.mergeAttr }),
            ["add-triple", ids.e2, ids.mergeAttr, { a: 1, keep: true }],
            ["deep-merge-triple", ids.e2, ids.mergeAttr, { a: 2, b: { c: 3 } }],
            ["retract-triple", ids.e1, ids.ownerRef, ids.owner1],
            ["add-triple", [ids.todosScore, 1], ids.todosTitle, "via-lookup"],
            ["delete-entity", ids.e3, "todos"],
          ],
        });
        await env.conns.A.waitFor((m) => m.op === "transact-ok" && env.conns.A.frames.filter((f) => f.op === "transact-ok").length >= 5);
        msg(env.conns.A, { op: "add-query", q: { todos: { $: { where: { done: true } } } } });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.todos?.$?.where);
      },
    },
    {
      name: "08-remove-query",
      run: async (env) => {
        msg(env.conns.B, { op: "remove-query", q: { todos: { owner: {} } } });
        await env.conns.B.waitFor((m) => m.op === "remove-query-ok");
      },
    },
    {
      name: "09-aggregate-admin",
      run: async (env) => {
        msg(env.conns.ADMIN, { op: "add-query", q: { todos: { $: { aggregate: "count" } } } });
        await env.conns.ADMIN.waitFor((m) => m.op === "add-query-ok");
      },
    },
    {
      name: "10-error-matrix",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        await expectErr(env.conns.A, { op: "add-query", q: { todos: { $: { order: { title: "asc" } } } } });
        await expectErr(env.conns.A, { op: "add-query", q: { todos: { $: { aggregate: "count" } } } });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["bogus-step"]] });
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", mk(), ids.todosTitle, "x", { mode: "update" }]],
        });
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", ids.e1, ids.todosTitle, "again", { mode: "create" }]],
        });
        await expectErr(env.conns.B, { op: "set-presence", "room-id": "never", data: {} });
        await expectErr(env.conns.B, { op: "start-sync", q: { todos: {} } });
        await expectErr(env.conns.B, { op: "subscribe-stream" });
      },
    },
    {
      name: "11-rooms",
      run: async (env) => {
        msg(env.conns.A, { op: "join-room", "room-type": "chat", "room-id": "r1", data: { who: "A" } });
        await env.conns.A.waitFor((m) => m.op === "join-room-ok");
        msg(env.conns.B, { op: "join-room", "room-type": "chat", "room-id": "r1", data: { who: "B" } });
        await env.conns.B.waitFor((m) => m.op === "join-room-ok");
        msg(env.conns.B, { op: "set-presence", "room-id": "r1", data: { who: "B", cursor: { x: 1 } } });
        await settle(Object.values(env.conns), 800);
        msg(env.conns.B, { op: "client-broadcast", "room-id": "r1", roomType: "chat", topic: "emoji", data: { e: "🔥" } });
        await env.conns.A.waitFor((m) => m.op === "server-broadcast");
        msg(env.conns.B, { op: "leave-room", "room-id": "r1" });
        await env.conns.B.waitFor((m) => m.op === "leave-room-ok");
      },
    },
    {
      name: "12-sync-table",
      run: async (env) => {
        msg(env.conns.ADMIN, { op: "start-sync", q: { todos: {} } });
        await env.conns.ADMIN.waitFor((m) => m.op === "sync-init-finish");
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", ids.e2, ids.todosTitle, "two-b"]],
        });
        await env.conns.ADMIN.waitFor((m) => m.op === "sync-update-triples");
      },
    },
    {
      name: "13-streams",
      run: async (env) => {
        psql(
          env.db,
          `INSERT INTO rules (app_id, code) VALUES ('${env.appId}', '{"$streams": {"allow": {"create": "true", "view": "true"}}}'::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`,
        );
        msg(env.conns.A, { op: "start-stream", "client-id": "diff-stream", "reconnect-token": "00000000-0000-4000-8000-00000000feed" });
        await env.conns.A.waitFor((m) => m.op === "start-stream-ok");
        const streamId = env.conns.A.frames.find((m) => m.op === "start-stream-ok")["stream-id"];
        // legacy buffers small appends and only flushes on close/threshold,
        // so intermediate stream-flushed frames are not awaited
        msg(env.conns.A, { op: "append-stream", "stream-id": streamId, chunks: ["hello"], offset: 0, done: false });
        msg(env.conns.B, { op: "subscribe-stream", "client-id": "diff-stream", offset: 0 });
        await env.conns.B.waitFor((m) => m.op === "stream-append");
        msg(env.conns.A, { op: "append-stream", "stream-id": streamId, chunks: [" world"], offset: 5, done: false });
        await env.conns.B.waitFor(
          (m) => m.op === "stream-append" && (m.content ?? "").includes("world"),
          20000,
        );
        msg(env.conns.A, { op: "append-stream", "stream-id": streamId, chunks: [], offset: 11, done: true });
        await env.conns.A.waitFor((m) => m.op === "stream-flushed" && m.done === true, 20000);
      },
    },
  ];
  return steps;
}

// ---------------------------------------------------------------------------
// runner

async function runAgainst(serverName) {
  const { url, db, appId } = SERVERS[serverName];
  const env = { serverName, url, db, appId, conns: {} };
  const steps = buildScenario();
  const stepResults = [];
  const keySets = {}; // op -> Set of keys
  const states = {}; // conn name -> folded state
  for (const step of steps) {
    try {
      await step.run(env);
    } catch (e) {
      console.error(`step ${step.name} failed on ${serverName}: ${e.message}`);
      for (const [name, conn] of Object.entries(env.conns)) {
        console.error(`  last frames on ${name}:`, JSON.stringify(conn.frames.slice(-3))?.slice(0, 1500));
      }
      throw e;
    }
    await settle(Object.values(env.conns), 900);
    const byConn = {};
    for (const [name, conn] of Object.entries(env.conns)) {
      const frames = conn.takeNewFrames();
      for (const f of frames) {
        (keySets[f.op] ??= new Set());
        for (const k of Object.keys(f)) keySets[f.op].add(k);
      }
      states[name] ??= newState();
      byConn[name] = await foldFrames(frames, states[name]);
    }
    stepResults.push({ name: step.name, byConn });
  }
  for (const conn of Object.values(env.conns)) conn.close();
  return {
    steps: stepResults,
    finalStates: Object.fromEntries(
      Object.entries(states).map(([k, v]) => [k, projectState(v)]),
    ),
    keySets: Object.fromEntries(
      Object.entries(keySets).map(([k, v]) => [k, [...v].sort()]),
    ),
  };
}

// ---------------------------------------------------------------------------
// diff + allowlist

const allowlist = JSON.parse(
  fs.readFileSync(path.join(here, "allowed-divergences.json"), "utf8"),
);
const usedAllows = new Set();
function allowed(p) {
  for (const entry of allowlist) {
    if (new RegExp(entry.path).test(p)) {
      usedAllows.add(entry.path);
      return true;
    }
  }
  return false;
}

const diffs = [];
function record(p, legacyVal, rustVal) {
  if (canon(legacyVal) === canon(rustVal)) return;
  diffs.push({ path: p, allowed: allowed(p), legacy: legacyVal, rust: rustVal });
}

console.log("replaying against legacy…");
const legacy = await runAgainst("legacy");
console.log("replaying against rust…");
const rust = await runAgainst("rust");

for (let i = 0; i < legacy.steps.length; i++) {
  const l = legacy.steps[i], r = rust.steps[i];
  for (const conn of new Set([...Object.keys(l.byConn), ...Object.keys(r.byConn)])) {
    record(`step:${l.name}/${conn}`, l.byConn[conn] ?? [], r.byConn[conn] ?? []);
  }
}
for (const conn of new Set([...Object.keys(legacy.finalStates), ...Object.keys(rust.finalStates)])) {
  const lf = legacy.finalStates[conn] ?? {};
  const rf = rust.finalStates[conn] ?? {};
  for (const section of new Set([...Object.keys(lf), ...Object.keys(rf)])) {
    record(`final/${conn}/${section}`, lf[section] ?? null, rf[section] ?? null);
  }
}
for (const op of new Set([...Object.keys(legacy.keySets), ...Object.keys(rust.keySets)])) {
  const lk = new Set(legacy.keySets[op] ?? []);
  const rk = new Set(rust.keySets[op] ?? []);
  for (const k of new Set([...lk, ...rk])) {
    if (lk.has(k) !== rk.has(k)) {
      record(`keyset/${op}/${k}`, lk.has(k) ? "present" : "absent", rk.has(k) ? "present" : "absent");
    }
  }
}

// descend into both values to the first differing sub-path for readable output
function firstDiff(a, b, p = "") {
  if (canon(a) === canon(b)) return null;
  const isObj = (v) => v !== null && typeof v === "object";
  if (isObj(a) && isObj(b) && Array.isArray(a) === Array.isArray(b)) {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    for (const k of keys) {
      const d = firstDiff(a?.[k], b?.[k], `${p}.${k}`);
      if (d) return d;
    }
  }
  return { p, a, b };
}

const blocking = diffs.filter((d) => !d.allowed);
for (const d of diffs) {
  const tag = d.allowed ? "ALLOWED" : "DIVERGENCE";
  console.log(`\n[${tag}] ${d.path}`);
  const fd = firstDiff(d.legacy, d.rust);
  if (fd && fd.p) {
    console.log(`  first differing sub-path: ${fd.p}`);
    console.log("  legacy:", JSON.stringify(fd.a)?.slice(0, 1200));
    console.log("  rust:  ", JSON.stringify(fd.b)?.slice(0, 1200));
  } else {
    console.log("  legacy:", JSON.stringify(d.legacy)?.slice(0, 1200));
    console.log("  rust:  ", JSON.stringify(d.rust)?.slice(0, 1200));
  }
}
for (const entry of allowlist) {
  if (!usedAllows.has(entry.path)) {
    console.log(`\n[STALE ALLOWLIST] ${entry.path} matched nothing (ok if flaky-path)`);
  }
}
if (blocking.length) {
  console.error(`\nDIFFERENTIAL REPLAY FAILED: ${blocking.length} unallowed divergences`);
  process.exit(1);
}
console.log(`\nDIFFERENTIAL REPLAY PASSED (${diffs.length} allowed divergences, ${legacy.steps.length} steps)`);
