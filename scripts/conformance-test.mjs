// Wire-protocol conformance suite (issue #12).
//
// Golden-shape assertions for every server→client op: exact key set, casing,
// and value types are checked against a live server. Divergences from the
// legacy server that are deliberate are documented in docs/PARITY.md
// ("Wire-level divergences") with client-code citations.
//
// Usage: node scripts/conformance-test.mjs <app-id> <admin-token>
// Env:   API_URL (default http://localhost:8888), DATABASE_URL (for psql rule
//        setup; default postgres://instant:instant@localhost:5432/instant)

import { execSync } from "node:child_process";

const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken)
  throw new Error("usage: node conformance-test.mjs <app-id> <admin-token>");
const API = process.env.API_URL || "http://localhost:8888";
const WS = API.replace(/^http/, "ws");
const DB = process.env.DATABASE_URL || "postgres://instant:instant@localhost:5432/instant";

const uuid = () => crypto.randomUUID();
let passed = 0;
const assert = (c, m) => {
  if (!c) throw new Error("ASSERT FAILED: " + m);
  passed++;
};
const group = (name) => console.log(`\n== ${name} ==`);

// ---------------------------------------------------------------------------
// type checkers

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const T = {
  string: (v) => typeof v === "string",
  number: (v) => typeof v === "number",
  boolean: (v) => typeof v === "boolean",
  object: (v) => v !== null && typeof v === "object" && !Array.isArray(v),
  array: (v) => Array.isArray(v),
  null: (v) => v === null,
  any: () => true,
  uuid: (v) => typeof v === "string" && UUID_RE.test(v),
  // legacy trace ids are hex strings (OTel); ours are 32-hex
  traceId: (v) => typeof v === "string" && /^[0-9a-f]+$/.test(v),
  eq: (want) => (v) => v === want,
  oneOf: (...opts) => (v) => opts.includes(v),
  or: (...fns) => (v) => fns.some((f) => f(v)),
  arrayOf: (f) => (v) => Array.isArray(v) && v.every(f),
  // marks a key that may be absent; checked when present
  maybe: (f) => Object.assign((v) => f(v), { optional: true }),
};

// Exact key set + per-key type assertions. Casing is implicitly checked by
// the exact key-set comparison.
function assertShape(msg, spec, label) {
  const specKeys = Object.keys(spec);
  const required = specKeys.filter((k) => !spec[k].optional);
  const allowed = new Set(specKeys);
  const got = Object.keys(msg).sort();
  const extra = got.filter((k) => !allowed.has(k));
  const missing = required.filter((k) => !(k in msg));
  if (extra.length || missing.length) {
    throw new Error(
      `SHAPE FAILED [${label}]: extra keys [${extra}] missing keys [${missing}]\n` +
        JSON.stringify(msg, null, 2),
    );
  }
  for (const k of specKeys) {
    if (!(k in msg)) continue;
    if (!spec[k](msg[k])) {
      throw new Error(
        `SHAPE FAILED [${label}]: key "${k}" has unexpected value ${JSON.stringify(msg[k])}`,
      );
    }
  }
  passed++;
}

// triple: [e, a, v, t]
const isTriple = (v) =>
  Array.isArray(v) && v.length === 4 && T.uuid(v[0]) && T.uuid(v[1]) && T.number(v[3]);
const isJoinRows = (v) => Array.isArray(v) && v.every(T.arrayOf(isTriple));
const isCursor = (v) =>
  v === null || (Array.isArray(v) && v.length === 4 && T.uuid(v[1]) && T.number(v[3]));

// instaql-result node tree
function assertResultNodes(result, label, { pageInfo = false, aggregate = false } = {}) {
  assert(Array.isArray(result) && result.length >= 1, `${label}: result is a node array`);
  for (const node of result) {
    const dataSpec = {
      "datalog-result": T.object,
    };
    if (pageInfo) dataSpec["page-info"] = T.object;
    if (aggregate) dataSpec["aggregate"] = T.object;
    assertShape(node, { data: T.object, "child-nodes": T.array }, `${label}: node`);
    assertShape(node.data, dataSpec, `${label}: node.data`);
    assertShape(
      node.data["datalog-result"],
      { "join-rows": isJoinRows },
      `${label}: datalog-result`,
    );
  }
}

// attr object
const ATTR_SPEC = {
  id: T.uuid,
  "value-type": T.oneOf("blob", "ref"),
  cardinality: T.oneOf("one", "many"),
  "forward-identity": (v) => Array.isArray(v) && v.length === 3 && T.uuid(v[0]),
  "reverse-identity": T.maybe((v) => Array.isArray(v) && v.length === 3 && T.uuid(v[0])),
  "unique?": T.boolean,
  "index?": T.boolean,
  "required?": T.boolean,
  "inferred-types": T.or(T.null, T.array),
  catalog: T.oneOf("user", "system"),
  "on-delete": T.maybe(T.eq("cascade")),
  "on-delete-reverse": T.maybe(T.eq("cascade")),
  "checked-data-type": T.maybe(T.oneOf("number", "string", "boolean", "date")),
};

// isn strings look like "0/<pg lsn>", e.g. "0/0/16B3760" (LEGACY isn.clj)
const isIsn = (v) => v === null || (typeof v === "string" && /^[0-9A-F]+\/[0-9A-F]+\/[0-9A-F]+$/i.test(v));

// ---------------------------------------------------------------------------
// golden shapes, one spec per server→client op

const SHAPES = {
  "init-ok": {
    op: T.eq("init-ok"),
    "session-id": T.uuid,
    "client-event-id": T.or(T.string, T.null),
    attrs: T.array,
    auth: T.object,
    "app-status": (v) =>
      T.object(v) && ["active", "read-only", "disabled"].includes(v.status),
    "trace-id": T.traceId,
  },
  "add-query-ok": {
    op: T.eq("add-query-ok"),
    q: T.object,
    result: T.array,
    "result-meta": T.null, // populated only for the tree return-type (legacy query.clj:143)
    "processed-tx-id": T.number,
    "processed-isn": isIsn,
    "client-event-id": T.or(T.string, T.null),
    "trace-id": T.traceId,
  },
  "add-query-exists": {
    op: T.eq("add-query-exists"),
    q: T.object,
    "client-event-id": T.or(T.string, T.null),
    "trace-id": T.traceId,
  },
  "remove-query-ok": {
    op: T.eq("remove-query-ok"),
    q: T.object,
    "client-event-id": T.or(T.string, T.null),
    "trace-id": T.traceId,
  },
  "transact-ok": {
    op: T.eq("transact-ok"),
    "client-event-id": T.string,
    "tx-id": T.number,
    isn: isIsn, // legacy session.clj:580-584
    "trace-id": T.traceId,
  },
  "refresh-ok": {
    op: T.eq("refresh-ok"),
    "processed-tx-id": T.number,
    "processed-isn": isIsn,
    computations: T.array,
    attrs: T.maybe(T.array), // skip-attrs gated (legacy session.clj:503-533)
    "trace-id": T.traceId,
  },
  computation: {
    // legacy recompute-instaql-query! (session.clj:459-465)
    "instaql-query": T.object,
    "instaql-query-hash": T.number,
    "instaql-result": T.array,
    "result-meta": T.null,
    "result-changed?": T.eq(true),
    "duration-ms": T.number,
    "instaql-topic?": T.boolean,
  },
  error: {
    // legacy handle-error! (session.clj:589-616): all keys always present
    op: T.eq("error"),
    status: T.number,
    type: T.string,
    message: T.string,
    hint: T.or(T.object, T.null),
    "original-event": T.object,
    "client-event-id": T.or(T.string, T.null),
    "trace-id": T.traceId,
  },
  "join-room-ok": {
    op: T.eq("join-room-ok"),
    "room-id": T.string,
    "client-event-id": T.or(T.string, T.null),
    "trace-id": T.traceId,
  },
  "leave-room-ok": {
    op: T.eq("leave-room-ok"),
    "room-id": T.string,
    "client-event-id": T.or(T.string, T.null),
    "trace-id": T.traceId,
  },
  "set-presence-ok": {
    op: T.eq("set-presence-ok"),
    "room-id": T.string,
    "client-event-id": T.or(T.string, T.null),
    "trace-id": T.traceId,
  },
  "client-broadcast-ok": {
    op: T.eq("client-broadcast-ok"),
    "room-id": T.string,
    topic: T.string,
    data: T.object,
    "client-event-id": T.or(T.string, T.null),
    "trace-id": T.traceId,
  },
  "refresh-presence": {
    op: T.eq("refresh-presence"),
    "room-id": T.string,
    data: T.object,
    "trace-id": T.traceId,
  },
  "patch-presence": {
    op: T.eq("patch-presence"),
    "room-id": T.string,
    edits: T.arrayOf(
      (e) =>
        Array.isArray(e) &&
        (e.length === 2 || e.length === 3) &&
        Array.isArray(e[0]) &&
        ["+", "r", "-"].includes(e[1]),
    ),
    "trace-id": T.traceId,
  },
  "server-broadcast": {
    op: T.eq("server-broadcast"),
    "room-id": T.string,
    topic: T.string,
    data: T.object,
    "trace-id": T.traceId,
  },
  "start-sync-ok": {
    op: T.eq("start-sync-ok"),
    "client-event-id": T.or(T.string, T.null),
    "subscription-id": T.uuid,
    q: T.object,
    token: T.uuid,
    "trace-id": T.traceId,
  },
  "sync-load-batch": {
    op: T.eq("sync-load-batch"),
    "subscription-id": T.uuid,
    "join-rows": isJoinRows,
    "trace-id": T.traceId,
  },
  "sync-init-finish": {
    op: T.eq("sync-init-finish"),
    "subscription-id": T.uuid,
    "tx-id": T.number,
    "trace-id": T.traceId,
  },
  "sync-update-triples": {
    op: T.eq("sync-update-triples"),
    "subscription-id": T.uuid,
    txes: T.arrayOf(
      (tx) =>
        T.object(tx) &&
        T.number(tx["tx-id"]) &&
        Array.isArray(tx.changes) &&
        tx.changes.every(
          (c) => ["added", "removed"].includes(c.action) && isTriple(c.triple),
        ),
    ),
    "trace-id": T.traceId,
  },
  "start-stream-ok": {
    op: T.eq("start-stream-ok"),
    "client-event-id": T.string,
    "client-id": T.string,
    "stream-id": T.uuid,
    offset: T.number,
    "trace-id": T.traceId,
  },
  "stream-flushed": {
    // legacy carries no client-event-id here (session.clj:855-858)
    op: T.eq("stream-flushed"),
    "stream-id": T.uuid,
    offset: T.number,
    done: T.boolean,
    "trace-id": T.traceId,
  },
  "stream-append": {
    op: T.eq("stream-append"),
    "client-event-id": T.string,
    "stream-id": T.uuid,
    "client-id": T.or(T.string, T.null),
    offset: T.number,
    content: T.string,
    done: T.boolean,
    "abort-reason": T.maybe(T.string), // present only for aborted streams
    "trace-id": T.traceId,
  },
  "sse-init": {
    op: T.eq("sse-init"),
    "machine-id": T.uuid,
    "session-id": T.uuid,
    "sse-token": T.uuid,
    "trace-id": T.traceId,
  },
};

// presence snapshot entry: {peer-id, instance-id, user, data} — client only
// reads .data (instance-id is the node the session lives on, ephemeral.clj:280-286)
const PRESENCE_ENTRY_SPEC = {
  "peer-id": T.uuid,
  "instance-id": T.or(T.uuid, T.string, T.null),
  user: T.or(T.object, T.null),
  data: T.any,
};

// ---------------------------------------------------------------------------
// ws client helper

function connect(name) {
  const ws = new WebSocket(`${WS}/runtime/session?app_id=${appId}`);
  const inbox = [];
  const waiters = [];
  let sawArrayFrame = false;
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (Array.isArray(msg)) sawArrayFrame = true;
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
      const t = setTimeout(
        () => reject(new Error(`timeout waiting in ${name}`)),
        timeout,
      );
      waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
    });
  const init = async (extra = {}) => {
    const ceid = send({ op: "init", "app-id": appId, ...extra });
    return waitFor((m) => m.op === "init-ok" || (m.op === "error" && m["client-event-id"] === ceid));
  };
  return {
    ws,
    send,
    waitFor,
    init,
    inbox,
    sawArray: () => sawArrayFrame,
    open: new Promise((r) => (ws.onopen = r)),
    close: () => ws.close(),
  };
}

// SQL via stdin: no shell interpolation of quotes or $-signs in JSON rules
const psql = (sql) => execSync(`psql "${DB}" -q -v ON_ERROR_STOP=1 -f -`, { input: sql });

// make sure this app has no rules to start
psql(`DELETE FROM rules WHERE app_id = '${appId}'`);

// ===========================================================================
group("init-ok / attrs");

const a = connect("A");
await a.open;
const initOk = await a.init({ versions: { "@instantdb/core": "v0.21.0" } });
assertShape(initOk, SHAPES["init-ok"], "init-ok");
for (const attr of initOk.attrs) assertShape(attr, ATTR_SPEC, "init-ok attr");
assert(initOk.attrs.some((at) => at.catalog === "system"), "system attrs present");

// ===========================================================================
group("transact-ok");

const ns = "c" + uuid().slice(0, 8); // per-run namespace
const idAttr = uuid(), titleAttr = uuid(), doneAttr = uuid(), scoreAttr = uuid();
const e1 = uuid(), e2 = uuid(), e3 = uuid();
const mkAttr = (id, label, unique = false) => [
  "add-attr",
  {
    id,
    "forward-identity": [uuid(), ns, label],
    "value-type": "blob",
    cardinality: "one",
    "unique?": unique,
    "index?": unique,
    isUnsynced: true,
  },
];
const sendTx = async (conn, steps) => {
  const ceid = conn.send({ op: "transact", "tx-steps": steps });
  return conn.waitFor((m) => m.op === "transact-ok" && m["client-event-id"] === ceid);
};
const tx1 = await sendTx(a, [
  mkAttr(idAttr, "id", true),
  mkAttr(titleAttr, "title"),
  mkAttr(doneAttr, "done"),
  mkAttr(scoreAttr, "score", true),
  ["add-triple", e1, idAttr, e1],
  ["add-triple", e1, titleAttr, "one"],
  ["add-triple", e1, doneAttr, false],
  ["add-triple", e1, scoreAttr, 1],
]);
assertShape(tx1, SHAPES["transact-ok"], "transact-ok");

// ===========================================================================
group("add-query-ok / add-query-exists / remove-query-ok");

const q1 = { [ns]: {} };
a.send({ op: "add-query", q: q1 });
const aq = await a.waitFor((m) => m.op === "add-query-ok");
assertShape(aq, SHAPES["add-query-ok"], "add-query-ok");
assert(JSON.stringify(aq.q) === JSON.stringify(q1), "add-query-ok echoes q verbatim");
assert(aq["processed-tx-id"] >= tx1["tx-id"], "processed-tx-id >= last tx-id");
assertResultNodes(aq.result, "add-query-ok");
const idTriple = aq.result[0].data["datalog-result"]["join-rows"]
  .flat()
  .find((t) => t[0] === e1 && t[1] === idAttr);
assert(idTriple && idTriple[2] === e1, "entity id triple present ([eid, id-attr, eid, t])");

a.send({ op: "add-query", q: q1 });
const exists = await a.waitFor((m) => m.op === "add-query-exists");
assertShape(exists, SHAPES["add-query-exists"], "add-query-exists (dedupe)");

a.send({ op: "remove-query", q: q1 });
const rq = await a.waitFor((m) => m.op === "remove-query-ok");
assertShape(rq, SHAPES["remove-query-ok"], "remove-query-ok");

// ===========================================================================
group("refresh-ok + computations + skip-attrs gating");

// modern client (skip-attrs capable): data-only tx must omit attrs
const b = connect("B");
await b.open;
await b.init({ versions: { "@instantdb/core": "v0.21.0" } });
b.send({ op: "add-query", q: q1 });
await b.waitFor((m) => m.op === "add-query-ok");
await sendTx(a, [["add-triple", e1, doneAttr, true]]);
const r1 = await b.waitFor((m) => m.op === "refresh-ok");
assertShape(r1, SHAPES["refresh-ok"], "refresh-ok");
assert(!("attrs" in r1), "skip-attrs: attrs omitted for core > 0.20.4 when unchanged");
for (const comp of r1.computations) assertShape(comp, SHAPES.computation, "computation");
assertResultNodes(r1.computations[0]["instaql-result"], "refresh computation result");

// attr-creating tx must include attrs even for modern clients
const lateAttr = uuid();
await sendTx(a, [mkAttr(lateAttr, "extra"), ["add-triple", e1, lateAttr, "x"]]);
const r2 = await b.waitFor((m) => m.op === "refresh-ok" && "attrs" in m);
assertShape(r2, SHAPES["refresh-ok"], "refresh-ok with attrs");
assert(r2.attrs.some((at) => at.id === lateAttr), "changed attrs are resent");

// legacy client (no skip-attrs): attrs always present
const old = connect("OLD");
await old.open;
await old.init({ versions: { "@instantdb/core": "v0.17.0" } });
old.send({ op: "add-query", q: q1 });
await old.waitFor((m) => m.op === "add-query-ok");
await sendTx(a, [["add-triple", e1, titleAttr, "one-v2"]]);
const r3 = await old.waitFor((m) => m.op === "refresh-ok");
assert("attrs" in r3, "attrs always sent to clients <= 0.20.4");

// ===========================================================================
group("page-info / aggregate / serverCreatedAt cursor round-trip");

await sendTx(a, [
  ["add-triple", e2, idAttr, e2],
  ["add-triple", e2, titleAttr, "two"],
  ["add-triple", e3, idAttr, e3],
  ["add-triple", e3, titleAttr, "three"],
]);

const qPage = { [ns]: { $: { limit: 2, order: { serverCreatedAt: "asc" } } } };
a.send({ op: "add-query", q: qPage });
const pageRes = await a.waitFor((m) => m.op === "add-query-ok" && m.q?.[ns]?.$?.limit === 2);
assertResultNodes(pageRes.result, "paginated result", { pageInfo: true });
const pi = pageRes.result[0].data["page-info"][ns];
assertShape(
  pi,
  {
    "start-cursor": isCursor,
    "end-cursor": isCursor,
    "has-next-page?": T.boolean,
    "has-previous-page?": T.boolean,
  },
  "page-info entry",
);
assert(pi["has-next-page?"] === true, "has-next-page? with limit 2 of 3");
const endCursor = pi["end-cursor"];
assert(
  endCursor[0] === endCursor[2] && endCursor[1] && T.number(endCursor[3]),
  "cursor is [eid, id-attr, eid, t]",
);
// round-trip: cursor sent back verbatim in $.after selects the next page,
// whose start-cursor must be a byte-identical continuation format
const qAfter = { [ns]: { $: { limit: 2, order: { serverCreatedAt: "asc" }, after: endCursor } } };
a.send({ op: "add-query", q: qAfter });
const pageRes2 = await a.waitFor((m) => m.op === "add-query-ok" && m.q?.[ns]?.$?.after);
assert(
  JSON.stringify(pageRes2.q[ns].$.after) === JSON.stringify(endCursor),
  "cursor round-trips byte-identically through q echo",
);
const page2Ids = new Set(
  pageRes2.result[0].data["datalog-result"]["join-rows"].flat().map((t) => t[0]),
);
assert(!page2Ids.has(endCursor[0]), "after-cursor excludes the cursor row");
assert(page2Ids.size === 1, "second page has the remaining entity");

// aggregate (admin-only)
const adminC = connect("ADMIN");
await adminC.open;
await adminC.init({ "__admin-token": adminToken });
adminC.send({ op: "add-query", q: { [ns]: { $: { aggregate: "count" } } } });
const aggRes = await adminC.waitFor((m) => m.op === "add-query-ok");
assertResultNodes(aggRes.result, "aggregate result", { aggregate: true });
assert(
  aggRes.result[0].data.aggregate[ns].count === 3,
  "aggregate count keyed by top-level query key",
);

// ===========================================================================
group("rooms / presence golden shapes + patch-presence edit matrix");

const roomId = "conf-room-" + uuid().slice(0, 8);
a.send({ op: "join-room", "room-type": "chat", "room-id": roomId, data: { who: "A" } });
const jr = await a.waitFor((m) => m.op === "join-room-ok");
assertShape(jr, SHAPES["join-room-ok"], "join-room-ok");
const rp = await a.waitFor((m) => m.op === "refresh-presence");
assertShape(rp, SHAPES["refresh-presence"], "refresh-presence");
for (const [sid, entry] of Object.entries(rp.data)) {
  assert(UUID_RE.test(sid), "presence keys are session ids");
  assertShape(entry, PRESENCE_ENTRY_SPEC, "presence entry");
}

// B joins -> A (modern client) gets a "+" edit
b.send({ op: "join-room", "room-type": "chat", "room-id": roomId, data: { who: "B" } });
await b.waitFor((m) => m.op === "join-room-ok");
const patchAdd = await a.waitFor((m) => m.op === "patch-presence");
assertShape(patchAdd, SHAPES["patch-presence"], "patch-presence (join)");
const addEdit = patchAdd.edits.find((e) => e[1] === "+");
assert(addEdit, "join produces a '+' edit");
assertShape(addEdit[2], PRESENCE_ENTRY_SPEC, "'+' edit value is a presence entry");

// B changes data -> "r" edit
b.send({ op: "set-presence", "room-id": roomId, data: { who: "B", x: 1 } });
const spOk = await b.waitFor((m) => m.op === "set-presence-ok");
assertShape(spOk, SHAPES["set-presence-ok"], "set-presence-ok");
const patchR = await a.waitFor(
  (m) => m.op === "patch-presence" && m.edits.some((e) => e[1] === "r"),
);
const rEdit = patchR.edits.find((e) => e[1] === "r");
assert(rEdit[0].length === 2 && rEdit[0][1] === "data", "'r' edit path is [sid, 'data']");
assert(rEdit[2].x === 1, "'r' edit carries the new presence data");

// old client fallback: gets refresh-presence, never patch-presence
old.send({ op: "join-room", "room-type": "chat", "room-id": roomId, data: { who: "OLD" } });
await old.waitFor((m) => m.op === "join-room-ok");
b.send({ op: "set-presence", "room-id": roomId, data: { who: "B", x: 2 } });
await old.waitFor(
  (m) =>
    m.op === "refresh-presence" &&
    m["room-id"] === roomId &&
    Object.values(m.data).some((v) => v.data?.x === 2),
);
assert(
  !old.inbox.some((m) => m.op === "patch-presence"),
  "core <= 0.17.5 never receives patch-presence",
);

// B leaves -> "-" edit for A
b.send({ op: "leave-room", "room-id": roomId });
const lr = await b.waitFor((m) => m.op === "leave-room-ok");
assertShape(lr, SHAPES["leave-room-ok"], "leave-room-ok");
const patchDel = await a.waitFor(
  (m) => m.op === "patch-presence" && m.edits.some((e) => e[1] === "-"),
);
const delEdit = patchDel.edits.find((e) => e[1] === "-");
assert(delEdit[0].length === 1 && UUID_RE.test(delEdit[0][0]), "'-' edit path is [sid]");

// broadcast envelope
old.send({ op: "client-broadcast", "room-id": roomId, roomType: "chat", topic: "emoji", data: { e: "🔥" } });
const cbOk = await old.waitFor((m) => m.op === "client-broadcast-ok");
assertShape(cbOk, SHAPES["client-broadcast-ok"], "client-broadcast-ok");
const sb = await a.waitFor((m) => m.op === "server-broadcast");
assertShape(sb, SHAPES["server-broadcast"], "server-broadcast");
assertShape(sb.data, { "peer-id": T.uuid, user: T.or(T.object, T.null), data: T.any }, "server-broadcast data envelope");
assert(sb.data.data.e === "🔥", "broadcast payload nested under data.data");

// ===========================================================================
group("sync tables");

const st = connect("SYNC");
await st.open;
await st.init({ "__admin-token": adminToken });
st.send({ op: "start-sync", q: { [ns]: {} } });
const sOk = await st.waitFor((m) => m.op === "start-sync-ok");
assertShape(sOk, SHAPES["start-sync-ok"], "start-sync-ok");
const sBatch = await st.waitFor((m) => m.op === "sync-load-batch");
assertShape(sBatch, SHAPES["sync-load-batch"], "sync-load-batch");
const sFin = await st.waitFor((m) => m.op === "sync-init-finish");
assertShape(sFin, SHAPES["sync-init-finish"], "sync-init-finish");

const e4 = uuid();
await sendTx(a, [["add-triple", e4, idAttr, e4], ["add-triple", e4, titleAttr, "four"]]);
const sUpd = await st.waitFor((m) => m.op === "sync-update-triples");
assertShape(sUpd, SHAPES["sync-update-triples"], "sync-update-triples");

// ===========================================================================
group("streams");

psql(
  `INSERT INTO rules (app_id, code) VALUES ('${appId}', '{"$streams": {"allow": {"create": "true", "view": "true"}}}'::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`,
);
const clientId = "conf-stream-" + uuid().slice(0, 8);
const reconnectToken = uuid();
a.send({ op: "start-stream", "client-id": clientId, "reconnect-token": reconnectToken });
const stOk = await a.waitFor((m) => m.op === "start-stream-ok");
assertShape(stOk, SHAPES["start-stream-ok"], "start-stream-ok");
const streamId = stOk["stream-id"];

a.send({ op: "append-stream", "stream-id": streamId, chunks: ["hello"], offset: 0, done: false });
const fl = await a.waitFor((m) => m.op === "stream-flushed");
assertShape(fl, SHAPES["stream-flushed"], "stream-flushed");

const subEvent = b.send({ op: "subscribe-stream", "stream-id": streamId, offset: 0 });
const sa = await b.waitFor((m) => m.op === "stream-append");
assertShape(sa, SHAPES["stream-append"], "stream-append (catch-up)");
assert(sa["client-event-id"] === subEvent, "stream-append correlated to subscribe event id");
assert(sa["client-id"] === clientId, "stream-append carries client-id");

a.send({ op: "append-stream", "stream-id": streamId, chunks: [" world"], offset: 5, done: false });
const live = await b.waitFor((m) => m.op === "stream-append" && m.content === " world");
assertShape(live, SHAPES["stream-append"], "stream-append (live)");

// abort surfaces as done + abort-reason (legacy session.clj:918-927; the
// client reader only reads error/retry for transport failures)
a.send({ op: "append-stream", "stream-id": streamId, chunks: [], offset: 11, done: true, "abort-reason": "user-cancelled" });
const aborted = await b.waitFor((m) => m.op === "stream-append" && m.done === true);
assertShape(aborted, SHAPES["stream-append"], "stream-append (abort)");
assert(aborted["abort-reason"] === "user-cancelled", "abort-reason propagated");

// catch-up on the aborted stream
const lateSub = adminC.send({ op: "subscribe-stream", "client-id": clientId, offset: 0 });
const lateApp = await adminC.waitFor((m) => m.op === "stream-append" && m["client-event-id"] === lateSub);
assertShape(lateApp, SHAPES["stream-append"], "stream-append (aborted catch-up)");
assert(lateApp.done === true && lateApp["abort-reason"] === "user-cancelled", "aborted catch-up: done + abort-reason");

// ===========================================================================
group("sse-init handshake");

const sseResp = await fetch(`${API}/runtime/sse?app_id=${appId}`);
const reader = sseResp.body.getReader();
const { value } = await reader.read();
const text = new TextDecoder().decode(value);
const dataLine = text.split("\n").find((l) => l.startsWith("data:"));
const sseInit = JSON.parse(dataLine.slice(5));
assertShape(sseInit, SHAPES["sse-init"], "sse-init");
reader.cancel().catch(() => {});

// ===========================================================================
group("error matrix: original-event.op routes × error types");

// Every case asserts the full legacy error frame shape plus the specific
// type / status / hint / original-event routing fields the client reads
// (Reactor.js:984-1067).
async function expectError(conn, msg, { type, status = 400, hint, originalKeys = [] }, label) {
  const ceid = conn.send(msg);
  const err = await conn.waitFor(
    (m) => m.op === "error" && m["client-event-id"] === ceid,
  );
  assertShape(err, SHAPES.error, `error frame [${label}]`);
  assert(err.type === type, `${label}: type=${type} (got ${err.type})`);
  assert(err.status === status, `${label}: status=${status} (got ${err.status})`);
  assert(err["original-event"].op === msg.op, `${label}: original-event.op echoed`);
  for (const k of originalKeys) {
    assert(
      JSON.stringify(err["original-event"][k]) === JSON.stringify(msg[k]),
      `${label}: original-event.${k} echoed verbatim`,
    );
  }
  if (hint) {
    for (const [k, v] of Object.entries(hint)) {
      assert(
        JSON.stringify(err.hint?.[k]) === JSON.stringify(v),
        `${label}: hint.${k}=${JSON.stringify(v)}`,
      );
    }
  }
  return err;
}

// --- init routes
const badApp = connect("BADAPP");
await badApp.open;
await expectError(
  badApp,
  { op: "init", "app-id": uuid() },
  { type: "record-not-found", hint: { "record-type": "app" } },
  "init: unknown app",
);
badApp.close();

// invalid refresh token → the logout convention (Reactor.js:1020-1029)
const badTok = connect("BADTOK");
await badTok.open;
await expectError(
  badTok,
  { op: "init", "app-id": appId, "refresh-token": uuid() },
  { type: "record-not-found", hint: { "record-type": "app-user" } },
  "init: invalid refresh-token (client logout path)",
);
badTok.close();

// double init
await expectError(
  a,
  { op: "init", "app-id": appId },
  { type: "validation-failed" },
  "init: double init rejected",
);

// ops before init
const cold = connect("COLD");
await cold.open;
await expectError(cold, { op: "add-query", q: { x: {} } }, { type: "validation-failed", message: "Validation failed for init: `init` has not run for this session." }, "add-query before init");
cold.close();

// --- add-query routes (errors route to subscribers via original-event.q)
await expectError(
  a,
  { op: "add-query", q: { [ns]: { $: { order: { title: "asc" } } } } },
  { type: "validation-failed", originalKeys: ["q"] },
  "add-query: unindexed order attr",
);
await expectError(
  a,
  { op: "add-query", q: { [ns]: { $: { aggregate: "count" } } } },
  { type: "validation-failed", originalKeys: ["q"] },
  "add-query: aggregate without admin",
);

// --- transact routes
await expectError(
  a,
  { op: "transact", "tx-steps": [["bogus-step"]] },
  { type: "validation-failed" },
  "transact: malformed step",
);
const dupe = uuid();
await expectError(
  a,
  {
    op: "transact",
    "tx-steps": [["add-triple", dupe, idAttr, dupe], ["add-triple", dupe, scoreAttr, 1]],
  },
  { type: "record-not-unique", hint: { "record-type": "triples" } },
  "transact: unique violation",
);
// legacy also raises validation-failed here (transaction.clj:349-358)
await expectError(
  a,
  { op: "transact", "tx-steps": [["add-triple", uuid(), titleAttr, "x", { mode: "update" }]] },
  { type: "validation-failed" },
  "transact: update mode on missing entity",
);

// permission-denied via rules
psql(
  `UPDATE rules SET code = code || '{"${ns}": {"allow": {"create": "false"}}}'::jsonb WHERE app_id = '${appId}'`,
);
const denied = uuid();
await expectError(
  b,
  { op: "transact", "tx-steps": [["add-triple", denied, idAttr, denied]] },
  { type: "permission-denied" },
  "transact: create denied by rules",
);
psql(`DELETE FROM rules WHERE app_id = '${appId}'`);
psql(
  `INSERT INTO rules (app_id, code) VALUES ('${appId}', '{"$streams": {"allow": {"create": "true", "view": "true"}}}'::jsonb)`,
);

// --- room routes
await expectError(
  b,
  { op: "set-presence", "room-id": "never-joined", data: {} },
  { type: "validation-failed" },
  "set-presence outside room",
);
await expectError(
  b,
  { op: "client-broadcast", "room-id": "never-joined", roomType: "r", topic: "t", data: {} },
  { type: "validation-failed" },
  "client-broadcast outside room",
);

// --- sync-table routes
await expectError(
  b,
  { op: "start-sync", q: { [ns]: {} } },
  { type: "validation-failed", originalKeys: ["q"] },
  "start-sync: non-admin rejected",
);
await expectError(
  st,
  { op: "start-sync", q: null },
  { type: "validation-failed" },
  "start-sync: null q",
);
await expectError(
  st,
  { op: "resync-table", "subscription-id": sOk["subscription-id"], "tx-id": sFin["tx-id"], token: uuid() },
  // legacy get-by-id-with-topics! (model/sync_sub.clj:178-183): a bad token
  // is a `subscription` validation error, not a missing record
  { type: "validation-failed", originalKeys: ["subscription-id"] },
  "resync-table: invalid token",
);

// resync after the change log lost coverage: an attr-only tx creates a
// transactions row with no triple changes, so the sub is "too far behind"
// and the client must restart with start-sync (SyncTable.ts:904-918)
await sendTx(a, [mkAttr(uuid(), "prune-marker")]);
psql(`DELETE FROM rust_tx_changes WHERE app_id = '${appId}'`);
const pruneErr = await expectError(
  st,
  { op: "resync-table", "subscription-id": sOk["subscription-id"], "tx-id": sFin["tx-id"], token: sOk.token },
  { type: "record-not-found", originalKeys: ["subscription-id"] },
  "resync-table: pruned log forces restart",
);
assert(/behind/.test(pruneErr.message), "prune error says the sub is behind");

// --- stream routes
await expectError(
  a,
  { op: "start-stream", "client-id": clientId, "reconnect-token": uuid() },
  { type: "validation-failed" },
  "start-stream: reconnect token mismatch",
);
await expectError(
  b,
  { op: "append-stream", "stream-id": uuid(), chunks: ["x"], offset: 0, done: false },
  { type: "validation-failed", originalKeys: ["stream-id"] },
  "append-stream: not the writer",
);
await expectError(
  b,
  { op: "subscribe-stream" },
  { type: "validation-failed" },
  "subscribe-stream: missing both ids",
);
await expectError(
  b,
  { op: "subscribe-stream", "client-id": "does-not-exist" },
  { type: "validation-failed" },
  "subscribe-stream: unknown stream",
);

// ===========================================================================
group("reconnect replay (re-init → re-add-query → unconfirmed transact resend)");

const rc1 = connect("RC1");
await rc1.open;
await rc1.init({ versions: { "@instantdb/core": "v0.21.0" } });
rc1.send({ op: "add-query", q: q1 });
await rc1.waitFor((m) => m.op === "add-query-ok");
// an "unconfirmed" mutation: build it, then simulate the socket dying and the
// client replaying its pending state on a fresh connection (Reactor.js:1635-1671)
const pendingEid = uuid();
const pendingTx = {
  "client-event-id": uuid(),
  op: "transact",
  "tx-steps": [["add-triple", pendingEid, idAttr, pendingEid], ["add-triple", pendingEid, titleAttr, "replayed"]],
  created: Date.now(),
  error: null,
  order: 1,
};
rc1.close();

const rc2 = connect("RC2");
await rc2.open;
const rcInit = await rc2.init({ versions: { "@instantdb/core": "v0.21.0" } });
assertShape(rcInit, SHAPES["init-ok"], "reconnect init-ok");
rc2.send({ op: "add-query", q: q1 });
const rcAq = await rc2.waitFor((m) => m.op === "add-query-ok");
assertShape(rcAq, SHAPES["add-query-ok"], "reconnect add-query-ok (fresh session, not -exists)");
rc2.ws.send(JSON.stringify(pendingTx)); // resend with the original event id
const rcTx = await rc2.waitFor(
  (m) => m.op === "transact-ok" && m["client-event-id"] === pendingTx["client-event-id"],
);
assertShape(rcTx, SHAPES["transact-ok"], "replayed transact-ok echoes original event id");
const rcRefresh = await rc2.waitFor(
  (m) =>
    m.op === "refresh-ok" &&
    m.computations.some((c) =>
      c["instaql-result"][0].data["datalog-result"]["join-rows"].flat().some((t) => t[2] === "replayed"),
    ),
);
assert(rcRefresh["processed-tx-id"] >= rcTx["tx-id"], "refresh watermark covers the replayed tx");
rc2.close();

// ===========================================================================
group("frame batching (core > 0.22.75)");

// clients above the gate may receive JSON-array frames; all helper clients
// here already handle both forms (like Reactor.js:1798-1804). Exercise a
// modern client through a burst and confirm nothing breaks; array frames are
// opportunistic so their appearance is informational.
const fast = connect("FAST");
await fast.open;
await fast.init({ versions: { "@instantdb/core": "v0.23.0" } });
fast.send({ op: "join-room", "room-type": "burst", "room-id": "burst-room", data: {} });
await fast.waitFor((m) => m.op === "join-room-ok");
await fast.waitFor((m) => m.op === "refresh-presence" && m["room-id"] === "burst-room");
console.log(
  fast.sawArray()
    ? "ok: observed a batched JSON-array frame"
    : "ok: no array frame observed this run (batching is opportunistic)",
);
passed++;

console.log(`\nCONFORMANCE SUITE PASSED (${passed} assertions)`);
process.exit(0);
