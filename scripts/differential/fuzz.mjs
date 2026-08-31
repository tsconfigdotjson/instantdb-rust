// Property/fuzz layer of the differential harness: seeded random tx-steps and
// queries run against both servers; asserts client-visible invariants on each
// server and cross-server equality of final query results.
//
// Usage: node fuzz.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL (defaults as in replay.mjs)

import {
  connect,
  settle,
  makeIdFactory,
  projectResult,
  canon,
  uuid,
} from "./lib.mjs";

const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
const seed = Number(process.argv[5] ?? 42);
const rounds = Number(process.argv[6] ?? 40);
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node fuzz.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");

const SERVERS = {
  legacy: { url: process.env.LEGACY_URL || "http://localhost:8891", appId: appIdLegacy },
  rust: { url: process.env.RUST_URL || "http://localhost:8888", appId: appIdRust },
};

// mulberry32 PRNG — identical op sequence for both servers
function prng(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Build the deterministic op script once; replay it on each server.
function buildScript() {
  const rand = prng(seed);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const mk = makeIdFactory(appIdLegacy);
  const NS = "fuzz";
  const attrs = {
    id: mk(),
    labels: Object.fromEntries(["p1", "p2", "p3", "num"].map((l) => [l, mk()])),
  };
  const entities = Array.from({ length: 8 }, () => mk());
  const attrSteps = [
    ["add-attr", { id: attrs.id, "forward-identity": [attrs.id, NS, "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": true, isUnsynced: true }],
    ...Object.entries(attrs.labels).map(([l, id]) => [
      "add-attr",
      { id, "forward-identity": [id, NS, l], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true },
    ]),
  ];
  const script = [{ kind: "tx", steps: attrSteps }];
  const values = ["a", "b", "c", 1, 2, 3.5, true, false, null, { k: 1 }, [1, 2]];
  for (let i = 0; i < rounds; i++) {
    const r = rand();
    if (r < 0.7) {
      // random transaction: 1-4 steps
      const n = 1 + Math.floor(rand() * 4);
      const steps = [];
      for (let j = 0; j < n; j++) {
        const e = pick(entities);
        const kind = rand();
        if (kind < 0.55) {
          steps.push(["add-triple", e, attrs.id, e]);
          steps.push(["add-triple", e, pick(Object.values(attrs.labels)), pick(values)]);
        } else if (kind < 0.75) {
          steps.push(["add-triple", e, attrs.id, e]);
          steps.push(["deep-merge-triple", e, attrs.labels.p3, { [`k${Math.floor(rand() * 3)}`]: pick(values) }]);
        } else if (kind < 0.9) {
          steps.push(["retract-triple", e, pick(Object.values(attrs.labels)), pick(values)]);
        } else {
          steps.push(["delete-entity", e, NS]);
        }
      }
      script.push({ kind: "tx", steps });
    } else {
      // random query
      const opts = {};
      const q = rand();
      if (q < 0.3) opts.where = { [pick(["p1", "p2"])]: pick(["a", "b", "c"]) };
      if (rand() < 0.3) opts.limit = 1 + Math.floor(rand() * 5);
      if (opts.limit && rand() < 0.5) opts.order = { serverCreatedAt: pick(["asc", "desc"]) };
      const form = Object.keys(opts).length ? { $: opts } : {};
      script.push({ kind: "query", q: { [NS]: form } });
    }
  }
  // final full query on every namespace
  script.push({ kind: "query", q: { [NS]: {} } });
  return script;
}

async function runOn(serverName, script) {
  const { url, appId } = SERVERS[serverName];
  const conn = connect(url, appId, serverName);
  await conn.open;
  conn.send({ "client-event-id": uuid(), op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.21.0" } });
  await conn.waitFor((m) => m.op === "init-ok");

  const queryResults = [];
  let lastTxId = 0;
  let violations = 0;
  let watermarkLags = 0;
  for (const [i, op] of script.entries()) {
    // each op only ever matches frames that arrive after it was sent
    conn.takeNewFrames();
    const ceid = uuid();
    if (op.kind === "tx") {
      conn.send({ "client-event-id": ceid, op: "transact", "tx-steps": op.steps });
      const reply = await conn.waitFor(
        (m) => ["transact-ok", "error"].includes(m.op) && m["client-event-id"] === ceid,
      );
      if (reply.op === "transact-ok") {
        // invariant: tx-ids strictly increase per server
        if (!(reply["tx-id"] > lastTxId)) {
          console.error(`[${serverName}] tx-id not increasing at op ${i}: ${reply["tx-id"]} <= ${lastTxId}`);
          violations++;
        }
        lastTxId = reply["tx-id"];
      } else {
        queryResults.push({ i, error: { type: reply.type, status: reply.status } });
      }
    } else {
      conn.send({ "client-event-id": ceid, op: "add-query", q: op.q });
      const reply = await conn.waitFor(
        (m) =>
          ((m.op === "add-query-ok" || m.op === "add-query-exists") && canon(m.q) === canon(op.q)) ||
          (m.op === "error" && m["client-event-id"] === ceid),
      );
      if (reply.op === "add-query-ok") {
        // add-query-ok's processed-tx-id is the invalidator watermark, which
        // legacy legitimately reports behind the last confirmed tx; track as
        // informational only (the hard >= invariant applies to refresh-ok,
        // covered by the replay scenario)
        if (!(reply["processed-tx-id"] >= lastTxId)) watermarkLags++;
        queryResults.push({ i, q: op.q, result: stripT(projectResult(reply.result)) });
        conn.takeNewFrames();
        conn.send({ "client-event-id": uuid(), op: "remove-query", q: op.q });
        await conn.waitFor((m) => m.op === "remove-query-ok" && canon(m.q) === canon(op.q));
      } else if (reply.op === "error") {
        queryResults.push({ i, q: op.q, error: { type: reply.type, status: reply.status } });
      }
    }
  }
  if (watermarkLags) {
    console.log(`[${serverName}] add-query watermark lagged last tx ${watermarkLags} times (informational)`);
  }
  await settle([conn], 500);
  conn.close();
  return { queryResults, violations };
}

// timestamps differ between servers; compare triples without t and without
// per-server ordering (the client's store treats join-rows as a set)
function stripT(projected) {
  const triples = projected.triples
    .map(([e, a, v]) => [e, a, v])
    .sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
  return { triples, aggregate: projected.aggregate };
}

const script = buildScript();
console.log(`fuzz: seed=${seed} rounds=${rounds} ops=${script.length}`);
console.log("running against legacy…");
const legacy = await runOn("legacy", script);
console.log("running against rust…");
const rust = await runOn("rust", script);

let mismatches = 0;
const n = Math.max(legacy.queryResults.length, rust.queryResults.length);
for (let i = 0; i < n; i++) {
  const l = legacy.queryResults[i], r = rust.queryResults[i];
  if (canon(l) !== canon(r)) {
    // Paginated non-final queries may legitimately race concurrent refreshes;
    // report everything, fail only on the final full-table comparison below.
    console.error(`query mismatch at slot ${i}:\n  legacy: ${JSON.stringify(l)}\n  rust:   ${JSON.stringify(r)}`);
    mismatches++;
  }
}
const finalL = legacy.queryResults.at(-1);
const finalR = rust.queryResults.at(-1);
const finalMatch = canon(finalL?.result) === canon(finalR?.result);
if (!finalMatch) {
  console.error("FINAL full-table results differ between servers");
}
if (legacy.violations || rust.violations || !finalMatch || mismatches) {
  console.error(
    `FUZZ FAILED: ${mismatches} query mismatches, invariant violations legacy=${legacy.violations} rust=${rust.violations}, final match=${finalMatch}`,
  );
  process.exit(1);
}
console.log(`FUZZ PASSED: ${script.length} ops replayed identically (seed ${seed})`);
