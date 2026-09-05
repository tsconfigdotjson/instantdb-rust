// Property/fuzz layer of the differential harness: seeded random tx-steps and
// queries over the whole client grammar run against both servers; asserts
// client-visible invariants on each server and cross-server equality of every
// query result and every error type.
//
// Grammar covered (surface.json tx:* / iq:*): add-triple (with `mode`),
// retract-triple, deep-merge-triple, delete-entity, add-attr, update-attr,
// delete-attr, restore-attr, rule-params, links (ref attrs both directions),
// malformed steps; where ops = / $in / in / $ne / $not / $isNull / $gt / $gte /
// $lt / $lte / $like / $ilike / $entityIdStartsWith, or / and, dotted link
// paths, limit / offset / first / last / order / after / before (cursor
// walks), fields, nested links, $$ruleParams, and admin-only options that
// must be rejected alike.
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

const blob = (id, ns, label, extra = {}) => [
  "add-attr",
  { id, "forward-identity": [id, ns, label], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true, ...extra },
];

// Build the deterministic op script once; replay it on each server.
function buildScript() {
  const rand = prng(seed);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const mk = makeIdFactory(appIdLegacy);
  const NS = "fuzz";
  const OWNER = "owner";
  const attrs = {
    id: mk(),
    labels: Object.fromEntries(["p1", "p2", "p3", "num"].map((l) => [l, mk()])),
    // typed + indexed attrs: exercised by comparison/like/order query ops
    typedNum: mk(),
    typedStr: mk(),
    // link fuzz.owner -> owner (reverse owner.items)
    owner: mk(),
    ownerId: mk(),
    ownerName: mk(),
  };
  const entities = Array.from({ length: 8 }, () => mk());
  const owners = Array.from({ length: 3 }, () => mk());
  const names = ["ann", "bob", "cy"];
  const attrSteps = [
    blob(attrs.id, NS, "id", { "unique?": true, "index?": true }),
    ...Object.entries(attrs.labels).map(([l, id]) => blob(id, NS, l)),
    blob(attrs.typedNum, NS, "tnum", { "index?": true, "checked-data-type": "number" }),
    blob(attrs.typedStr, NS, "tstr", { "index?": true, "checked-data-type": "string" }),
    blob(attrs.ownerId, OWNER, "id", { "unique?": true, "index?": true }),
    blob(attrs.ownerName, OWNER, "name", { "index?": true, "checked-data-type": "string" }),
    [
      "add-attr",
      {
        id: attrs.owner,
        "forward-identity": [attrs.owner, NS, "owner"],
        "reverse-identity": [mk(), OWNER, "items"],
        "value-type": "ref",
        cardinality: "one",
        "unique?": false,
        "index?": false,
        isUnsynced: true,
      },
    ],
  ];
  const script = [{ kind: "tx", steps: attrSteps }];
  script.push({
    kind: "tx",
    steps: owners.flatMap((o, i) => [
      ["add-triple", o, attrs.ownerId, o],
      ["add-triple", o, attrs.ownerName, names[i]],
    ]),
  });
  const values = ["a", "b", "c", 1, 2, 3.5, true, false, null, { k: 1 }, [1, 2]];
  const nums = [0, 1, 2.5, 7, -3, 100];
  const strs = ["alpha", "beta", "gamma", "alphabet", "Zed"];
  const orderKeys = ["serverCreatedAt", "tnum", "tstr"];
  const malformed = (e) =>
    pick([
      ["add-triple", "not-a-uuid", attrs.id, 1],
      ["add-triple", e],
      ["frobnicate", e],
      ["delete-entity", e, 42],
      ["add-triple", e, uuid(), 1],
      ["retract-triple", e, attrs.labels.p1],
      ["add-triple", e, attrs.id, e, { mode: "bogus" }],
      ["rule-params", e, NS, "not-a-map"],
      ["add-triple", e, attrs.owner, "not-a-uuid"],
      ["update-attr", { id: uuid(), "index?": true }],
      ["delete-attr", uuid()],
    ]);
  let p2Deleted = false;
  for (let i = 0; i < rounds; i++) {
    const r = rand();
    if (r < 0.6) {
      // random transaction: 1-4 steps
      const n = 1 + Math.floor(rand() * 4);
      const steps = [];
      if (chance(0.05)) steps.push(["rule-params", pick(entities), NS, { k: Math.floor(rand() * 3) }]);
      for (let j = 0; j < n; j++) {
        const e = pick(entities);
        const kind = rand();
        if (kind < 0.3) {
          steps.push(["add-triple", e, attrs.id, e]);
          steps.push(["add-triple", e, pick(Object.values(attrs.labels)), pick(values)]);
        } else if (kind < 0.45) {
          steps.push(["add-triple", e, attrs.id, e]);
          steps.push(["add-triple", e, attrs.typedNum, pick(nums)]);
          steps.push(["add-triple", e, attrs.typedStr, pick(strs)]);
        } else if (kind < 0.55) {
          steps.push(["add-triple", e, attrs.id, e]);
          steps.push(["deep-merge-triple", e, attrs.labels.p3, { [`k${Math.floor(rand() * 3)}`]: pick(values) }]);
        } else if (kind < 0.62) {
          // checked-type violation: both servers must reject alike
          steps.push(["add-triple", e, attrs.id, e]);
          steps.push(["add-triple", e, attrs.typedNum, pick(["oops", true])]);
        } else if (kind < 0.7) {
          // link / unlink
          if (chance(0.7)) steps.push(["add-triple", e, attrs.owner, pick(owners)]);
          else steps.push(["retract-triple", e, attrs.owner, pick(owners)]);
        } else if (kind < 0.78) {
          // create / update mode: existence-dependent, must fail alike
          steps.push(["add-triple", e, attrs.id, e, { mode: pick(["create", "update"]) }]);
          steps.push(["add-triple", e, attrs.labels.p1, pick(values)]);
        } else if (kind < 0.83) {
          steps.push(malformed(e));
        } else if (kind < 0.93) {
          steps.push(["retract-triple", e, pick(Object.values(attrs.labels)), pick(values)]);
        } else {
          steps.push(["delete-entity", e, NS]);
        }
      }
      script.push({ kind: "tx", steps });
    } else if (r < 0.64) {
      // schema churn: delete-attr then restore-attr (p2), or an update-attr
      if (chance(0.5)) {
        if (!p2Deleted) {
          script.push({ kind: "tx", steps: [["delete-attr", attrs.labels.p2]] });
          p2Deleted = true;
        } else {
          script.push({ kind: "tx", steps: [["restore-attr", attrs.labels.p2]] });
          p2Deleted = false;
        }
      } else {
        script.push({ kind: "tx", steps: [["update-attr", { id: attrs.labels.num, "index?": chance(0.5), "checked-data-type": "number" }]] });
      }
    } else if (r < 0.7) {
      // cursor walk: page through the whole namespace by a typed order
      const dir = pick(["asc", "desc"]);
      const key = pick(orderKeys);
      const size = 1 + Math.floor(rand() * 3);
      script.push({ kind: "paginate", ns: NS, key, dir, size, backwards: chance(0.3) });
    } else {
      // random query over the where grammar, links, fields, pagination
      const opts = {};
      const q = rand();
      if (q < 0.12) opts.where = { [pick(["p1", "p2"])]: pick(["a", "b", "c"]) };
      else if (q < 0.22) opts.where = { tnum: { [pick(["$gt", "$gte", "$lt", "$lte"])]: pick(nums) } };
      else if (q < 0.3) opts.where = { tstr: { [pick(["$like", "$ilike"])]: pick(["%a%", "alph%", "%ed", "%ET%"]) } };
      else if (q < 0.36) opts.where = { [pick(["tnum", "owner"])]: { $isNull: pick([true, false]) } };
      else if (q < 0.44) opts.where = { [pick(["tstr", "p1"])]: { [pick(["$in", "in"])]: [pick(strs), pick(strs), "a"] } };
      else if (q < 0.5) opts.where = { [pick(["tstr", "tnum"])]: { [pick(["$ne", "$not"])]: pick([...strs, ...nums]) } };
      else if (q < 0.56) opts.where = { [pick(["owner.name", "owner.id"])]: pick([...names, ...owners]) };
      else if (q < 0.6) opts.where = { $entityIdStartsWith: pick(entities).slice(0, 8) };
      else if (q < 0.68) {
        opts.where = {
          [pick(["or", "and"])]: [
            { tnum: { $gt: pick(nums) } },
            { [pick(["tstr", "owner.name"])]: pick([...strs, ...names]) },
          ],
        };
      } else if (q < 0.72) {
        // invalid on both: comparison on an unindexed attr / $like on a number
        opts.where = pick([{ p1: { $gt: 1 } }, { tnum: { $like: "%1%" } }, { or: [] }]);
      }
      const paged = rand();
      if (paged < 0.25) opts.limit = 1 + Math.floor(rand() * 5);
      else if (paged < 0.4) opts.first = 1 + Math.floor(rand() * 4);
      else if (paged < 0.5) opts.last = 1 + Math.floor(rand() * 4);
      if ((opts.limit || opts.first || opts.last) && rand() < 0.75) {
        opts.order = { [pick(orderKeys)]: pick(["asc", "desc"]) };
      }
      if (opts.limit && rand() < 0.3) opts.offset = Math.floor(rand() * 3);
      if (chance(0.12)) opts.fields = pick([["p1"], ["tnum", "tstr"], ["id"], ["p3", "owner"]]);
      if (chance(0.04)) opts.aggregate = "count"; // admin-only: rejected alike
      const form = Object.keys(opts).length ? { $: opts } : {};
      if (chance(0.25)) form.owner = chance(0.5) ? {} : { $: { fields: ["name"] } };
      if (chance(0.08)) form.$$ruleParams = { k: 1 };
      const root = chance(0.15) ? { [OWNER]: { items: chance(0.5) ? {} : { $: { where: { tnum: { $gt: 0 } } } } } } : { [NS]: form };
      script.push({ kind: "query", q: root });
    }
  }
  // final full queries on every namespace, both directions of the link
  script.push({ kind: "query", q: { [NS]: { owner: {} } } });
  script.push({ kind: "query", q: { [OWNER]: { items: {} } } });
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

  const query = async (q) => {
    conn.takeNewFrames();
    const ceid = uuid();
    conn.send({ "client-event-id": ceid, op: "add-query", q });
    const reply = await conn.waitFor(
      (m) =>
        ((m.op === "add-query-ok" || m.op === "add-query-exists") && canon(m.q) === canon(q)) ||
        (m.op === "error" && m["client-event-id"] === ceid),
    );
    if (reply.op === "add-query-ok") {
      // add-query-ok's processed-tx-id is the invalidator watermark, which
      // legacy legitimately reports behind the last confirmed tx; track as
      // informational only (the hard >= invariant applies to refresh-ok,
      // covered by the replay scenario)
      if (!(reply["processed-tx-id"] >= lastTxId)) watermarkLags++;
      conn.takeNewFrames();
      conn.send({ "client-event-id": uuid(), op: "remove-query", q });
      await conn.waitFor((m) => m.op === "remove-query-ok" && canon(m.q) === canon(q));
      return { result: stripT(projectResult(reply.result)), raw: reply.result };
    }
    if (reply.op === "error") return { error: { type: reply.type, status: reply.status } };
    return { error: { type: "unexpected", op: reply.op } };
  };

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
    } else if (op.kind === "query") {
      const r = await query(op.q);
      queryResults.push({ i, q: op.q, ...(r.error ? { error: r.error } : { result: r.result }) });
    } else if (op.kind === "paginate") {
      // walk every page with each server's own cursors; page boundaries may
      // differ on ties, the union and the page count may not
      const { ns, key, dir, size, backwards } = op;
      const seen = new Map();
      let cursor = null;
      let pages = 0;
      let error = null;
      for (;;) {
        const $ = backwards ? { last: size, order: { [key]: dir } } : { first: size, order: { [key]: dir } };
        if (cursor) $[backwards ? "before" : "after"] = cursor;
        const r = await query({ [ns]: { $ } });
        if (r.error) {
          error = r.error;
          break;
        }
        pages++;
        for (const t of r.result.triples) seen.set(canon(t), t);
        const info = r.raw?.[0]?.data?.["page-info"]?.[ns] ?? r.raw?.[0]?.data?.["page-info"] ?? {};
        const more = backwards ? info["has-previous-page?"] : info["has-next-page?"];
        const next = backwards ? info["start-cursor"] : info["end-cursor"];
        if (!more || !next || pages > 50) break;
        cursor = next;
      }
      queryResults.push({ i, paginate: op, ...(error ? { error } : { pages, union: [...seen.values()].sort((x, y) => (canon(x) < canon(y) ? -1 : 1)) }) });
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
