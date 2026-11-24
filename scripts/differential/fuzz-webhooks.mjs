// Seeded webhook event-production parity layer.
//
// Legacy queues `webhook_events` from its logical-replication feed: a WAL
// record's id-attr triple changes are matched against the app's active
// webhooks (`webhook-matches?`, model/webhook.clj), and the payload is rebuilt
// from the stored WAL record (`webhook-data-for-wal-record`). This server has
// no WAL consumer, so it produces the same rows on the transaction path
// (instant-server/src/webhooks.rs). The scripted dash layer covers the routes
// and one update/delete sequence; this layer compares event production itself
// over generated transactions against the live legacy server.
//
// We create webhooks over generated namespaces / action subsets, then run a
// deterministic mix of: creates, client-style updates, merges, id-only and
// no-op updates, raw non-id updates, deletes (existing and missing), multi-
// entity transactions, link / unlink steps (which must produce nothing),
// cascade deletes, writes to non-webhook namespaces, and id / unique lookup
// upserts. After every transaction we read each webhook's event list back (no
// public https receiver is needed: the sink URL only has to validate) and fetch
// each new event's payload through `/webhooks/payload/...`.
//
// Compared per transaction and per webhook:
//   - whether an event row was produced (0 or 1; one transaction cannot make
//     two rows for the same webhook because the key is (webhook, isn));
//   - every payload record's namespace, action, entity id, and before/after
//     label maps, in client order-independent form.
// Only ISNs / LSNs and values derived from them (payload idempotency keys) are
// normalized. Entity ids are generated identically on both servers; no
// timestamps are compared because event status / attempts are delivery
// concerns already covered by scripts/webhooks-test.mjs.
//
// Usage: node fuzz-webhooks.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL

import { canon, connect, makeIdFactory } from "./lib.mjs";

const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
const seed = Number(process.argv[5] ?? 501);
const rounds = Number(process.argv[6] ?? 18);
if (!appIdLegacy || !appIdRust || !adminToken) {
  throw new Error("usage: node fuzz-webhooks.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");
}

const SERVERS = {
  legacy: { url: process.env.LEGACY_URL || "http://localhost:8891", appId: appIdLegacy },
  rust: { url: process.env.RUST_URL || "http://localhost:8888", appId: appIdRust },
};
const ALL_ACTIONS = ["create", "update", "delete"];
const EVENT_SETTLE_MS = 650;
const EVENT_MAX_WAIT_MS = 6000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function prng(a) {
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const attr = (id, ns, label, extra = {}) => [
  "add-attr",
  {
    id,
    "forward-identity": [id, ns, label],
    "value-type": "blob",
    cardinality: "one",
    "unique?": false,
    "index?": false,
    isUnsynced: true,
    ...extra,
  },
];

const refAttr = (id, ns, label, revId, revNs, revLabel, extra = {}) => [
  "add-attr",
  {
    id,
    "forward-identity": [id, ns, label],
    "reverse-identity": [revId, revNs, revLabel],
    "value-type": "ref",
    cardinality: "many",
    "unique?": false,
    "index?": false,
    isUnsynced: true,
    ...extra,
  },
];

function buildPlan() {
  const rand = prng(seed * 104729 + 503);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const mk = makeIdFactory(appIdLegacy);

  const A = `whA_${seed}`;
  const B = `whB_${seed}`;
  const C = `whC_${seed}`;
  const N = `whN_${seed}`;
  const byLabel = {
    [A]: { id: mk(), name: mk(), rank: mk(), meta: mk(), flag: mk(), handle: mk(), peer: mk(), peerRev: mk(), owned: mk() },
    [B]: { id: mk(), title: mk(), count: mk(), owner: mk(), owned: mk(), peerRev: mk() },
    [C]: { id: mk(), text: mk() },
    [N]: { id: mk(), value: mk() },
  };

  const schema = [
    attr(byLabel[A].id, A, "id", { "unique?": true, "index?": true }),
    attr(byLabel[A].name, A, "name"),
    attr(byLabel[A].rank, A, "rank", { "checked-data-type": "number" }),
    attr(byLabel[A].meta, A, "meta"),
    attr(byLabel[A].flag, A, "flag", { "checked-data-type": "boolean" }),
    attr(byLabel[A].handle, A, "handle", { "unique?": true }),
    refAttr(byLabel[A].peer, A, "peer", byLabel[B].peerRev, B, "peerRev"),
    attr(byLabel[B].id, B, "id", { "unique?": true, "index?": true }),
    attr(byLabel[B].title, B, "title"),
    attr(byLabel[B].count, B, "count", { "checked-data-type": "number" }),
    // child B -> parent A; deleting A cascades to every B pointing at it
    refAttr(byLabel[B].owner, B, "owner", byLabel[A].owned, A, "owned", {
      cardinality: "one",
      "on-delete": "cascade",
    }),
    attr(byLabel[C].id, C, "id", { "unique?": true, "index?": true }),
    attr(byLabel[C].text, C, "text"),
    attr(byLabel[N].id, N, "id", { "unique?": true, "index?": true }),
    attr(byLabel[N].value, N, "value"),
  ];

  const state = {
    [A]: new Map(),
    [B]: new Map(),
    [C]: new Map(),
    [N]: new Map(),
  };
  const links = new Set();

  const fieldSteps = (ns, eid, fields) =>
    Object.entries(fields).flatMap(([label, value]) => {
      if (value === undefined) return [];
      const aid = byLabel[ns][label];
      const op = value && typeof value === "object" && value.__merge ? "deep-merge-triple" : "add-triple";
      const v = value && typeof value === "object" && value.__merge ? value.value : value;
      return [[op, eid, aid, v]];
    });

  const create = (ns, eid, fields) => {
    state[ns].set(eid, { ...fields, alive: true });
    return [["add-triple", eid, byLabel[ns].id, eid], ...fieldSteps(ns, eid, fields)];
  };
  const update = (ns, eid, fields, withId = true) => [
    ...(withId ? [["add-triple", eid, byLabel[ns].id, eid]] : []),
    ...fieldSteps(ns, eid, fields),
  ];
  const merge = (ns, eid, patches, withId = true) => [
    ...(withId ? [["add-triple", eid, byLabel[ns].id, eid]] : []),
    ...Object.entries(patches).map(([label, value]) => ["deep-merge-triple", eid, byLabel[ns][label], value]),
  ];
  const remove = (ns, eid) => {
    const cur = state[ns].get(eid);
    if (cur) cur.alive = false;
    return [["delete-entity", eid, ns]];
  };
  const markAlive = (ns, eid) => {
    const cur = state[ns].get(eid) ?? {};
    cur.alive = true;
    state[ns].set(eid, cur);
  };
  const alive = (ns) => [...state[ns].entries()].filter(([, v]) => v.alive).map(([id]) => id);
  const link = (a, b) => {
    links.add(`${a}|${b}`);
    return [["add-triple", a, byLabel[A].peer, b]];
  };
  const unlink = (a, b) => {
    links.delete(`${a}|${b}`);
    return [["retract-triple", a, byLabel[A].peer, b]];
  };

  const aIds = Array.from({ length: 5 }, () => mk());
  const bIds = Array.from({ length: 4 }, () => mk());
  const cIds = Array.from({ length: 3 }, () => mk());
  const nIds = Array.from({ length: 2 }, () => mk());
  const initialA = new Map();
  const seedSteps = [];
  for (const [i, eid] of aIds.entries()) {
    const fields = {
      name: `a${i + 1}`,
      rank: i + 1,
      meta: { i, nested: { n: i } },
      flag: i % 2 === 0,
      handle: `handle-${i + 1}-${seed}`,
    };
    initialA.set(eid, fields);
    seedSteps.push(...create(A, eid, fields));
  }
  for (const [i, eid] of bIds.entries()) {
    seedSteps.push(...create(B, eid, { title: `b${i + 1}`, count: i + 1 }));
  }
  for (const [i, eid] of cIds.entries()) {
    seedSteps.push(...create(C, eid, { text: `c${i + 1}` }));
  }
  for (const [i, eid] of nIds.entries()) {
    seedSteps.push(...create(N, eid, { value: `n${i + 1}` }));
  }
  seedSteps.push(["add-triple", bIds[0], byLabel[B].owner, aIds[0]]);
  seedSteps.push(["add-triple", bIds[1], byLabel[B].owner, aIds[1]]);
  seedSteps.push(...link(aIds[0], bIds[0]));
  seedSteps.push(...link(aIds[1], bIds[1]));

  const subsets = [
    ALL_ACTIONS,
    ["update"],
    ["delete"],
    ["create", "delete"],
    ["create"],
    ["update", "delete"],
  ];
  const hooks = [
    { namespaces: [A], actions: ALL_ACTIONS },
    { namespaces: [B], actions: pick(subsets) },
    { namespaces: [A, B], actions: pick(subsets) },
    { namespaces: [C], actions: pick(subsets) },
  ].map((h, i) => ({
    ...h,
    url: `https://example.com/webhooks/${seed}/${i}`,
    name: `hook-${i}`,
  }));

  const script = [];
  const tx = (name, steps) => script.push({ name, steps });

  // -- fixed edge cases, all on top of the seeded data --------------------
  tx("update-client-style", update(A, aIds[0], { name: "a1-updated", rank: 11 }));
  tx("update-noop", update(A, aIds[1], { name: initialA.get(aIds[1]).name }));
  // one id-marked update plus a raw non-id update on the same namespace: the
  // WAL entity log carries both entities, so the payload must too
  tx("update-plus-raw-non-id", [
    ...update(A, aIds[0], { name: "a1-multi" }),
    ["add-triple", aIds[1], byLabel[A].name, "a2-raw-only"],
  ]);
  tx("merge-client-style", merge(A, aIds[2], { meta: { merge: true, n: 2 } }));
  tx("merge-without-id", merge(A, aIds[3], { meta: { rawMerge: true } }, false));
  tx("id-only-noop", [["add-triple", aIds[3], byLabel[A].id, aIds[3]]]);
  tx("id-merge-marker", [
    ["add-triple", aIds[3], byLabel[A].id, aIds[3]],
    ["deep-merge-triple", aIds[3], byLabel[A].id, aIds[3]],
  ]);
  const a6 = mk();
  tx("create-entity", create(A, a6, { name: "a6", rank: 6, meta: { fresh: true }, flag: true, handle: `handle-6-${seed}` }));
  tx("update-b", update(B, bIds[3], { title: "b4-updated", count: 44 }));
  tx("delete-c", remove(C, cIds[0]));
  tx("delete-missing", remove(C, mk()));
  const c4 = mk();
  tx("multi-entity-mixed", [
    ...update(A, aIds[0], { rank: 12 }),
    ...update(B, bIds[1], { count: 22 }),
    ...create(C, c4, { text: "c4-created" }),
    ...update(N, nIds[0], { value: "n1-updated" }),
    ...link(aIds[1], bIds[2]),
  ]);
  tx("pure-link", link(aIds[2], bIds[3]));
  tx("pure-unlink", unlink(aIds[2], bIds[3]));
  const b5 = mk();
  tx("create-cascade-child", create(B, b5, { title: "b5-cascade", count: 5, owner: aIds[2] }));
  tx("cascade-delete", remove(A, aIds[2]));
  const a9 = mk();
  tx("lookup-create-by-id", [
    ["add-triple", [byLabel[A].id, a9], byLabel[A].id, [byLabel[A].id, a9]],
    ["add-triple", [byLabel[A].id, a9], byLabel[A].name, "lookup-created"],
  ]);
  markAlive(A, a9);
  tx("lookup-update-by-handle", [
    ["add-triple", [byLabel[A].handle, initialA.get(aIds[0]).handle], byLabel[A].id, [byLabel[A].handle, initialA.get(aIds[0]).handle]],
    ["add-triple", [byLabel[A].handle, initialA.get(aIds[0]).handle], byLabel[A].name, "lookup-updated"],
  ]);

  // -- seeded random transactions ----------------------------------------
  const randFields = (ns) => {
    if (ns === A) {
      return {
        name: `rnd-a-${Math.floor(rand() * 1e6)}`,
        rank: Math.floor(rand() * 50),
        meta: { r: Math.floor(rand() * 100), nested: { ok: chance(0.5) } },
        flag: chance(0.5),
      };
    }
    if (ns === B) return { title: `rnd-b-${Math.floor(rand() * 1e6)}`, count: Math.floor(rand() * 50) };
    if (ns === C) return { text: `rnd-c-${Math.floor(rand() * 1e6)}` };
    return { value: `rnd-n-${Math.floor(rand() * 1e6)}` };
  };

  for (let i = 0; i < rounds; i++) {
    const kind = pick([
      "create",
      "update",
      "merge",
      "delete",
      "link",
      "unlink",
      "noop",
      "raw-update",
      "multi",
      "lookup",
      "cascade",
    ]);
    if (kind === "create") {
      const ns = pick([A, B, C, N]);
      const eid = mk();
      const fields = randFields(ns);
      if (ns === A) fields.handle = `rnd-handle-${seed}-${i}`;
      if (ns === B && alive(A).length && chance(0.4)) fields.owner = pick(alive(A));
      tx(`rand-${i}-create-${ns}`, create(ns, eid, fields));
    } else if (kind === "delete") {
      const ns = pick([A, B, C, N]);
      const ids = alive(ns);
      if (ids.length) tx(`rand-${i}-delete-${ns}`, remove(ns, pick(ids)));
      else tx(`rand-${i}-delete-missing-${ns}`, remove(ns, mk()));
    } else if (kind === "link" || kind === "unlink") {
      const as = alive(A);
      const bs = alive(B);
      if (as.length && bs.length) {
        const a = pick(as);
        const b = pick(bs);
        if (kind === "link") tx(`rand-${i}-link`, link(a, b));
        else {
          const cur = [...links].filter((x) => x.startsWith(`${a}|`)).map((x) => x.split("|")[1]);
          tx(`rand-${i}-unlink`, unlink(a, cur.length ? pick(cur) : b));
        }
      } else tx(`rand-${i}-${kind}-empty`, [["add-triple", mk(), byLabel[N].id, mk()]]);
    } else if (kind === "cascade") {
      const as = alive(A);
      if (as.length) {
        const parent = pick(as);
        const child = mk();
        tx(`rand-${i}-cascade-seed`, create(B, child, { title: `cascade-${i}`, count: i, owner: parent }));
        tx(`rand-${i}-cascade-delete`, remove(A, parent));
      }
    } else if (kind === "lookup") {
      const as = alive(A);
      if (as.length) {
        const target = pick(as);
        const stateA = state[A].get(target);
        if (stateA?.handle) {
          tx(`rand-${i}-lookup-update`, [
            ["add-triple", [byLabel[A].handle, stateA.handle], byLabel[A].id, [byLabel[A].handle, stateA.handle]],
            ["add-triple", [byLabel[A].handle, stateA.handle], byLabel[A].name, `lookup-rnd-${i}`],
          ]);
        }
      }
    } else if (kind === "multi") {
      const parts = [];
      for (const ns of [A, B, C, N]) {
        const ids = alive(ns);
        if (ids.length && chance(0.65)) parts.push(...update(ns, pick(ids), randFields(ns)));
      }
      if (parts.length === 0) parts.push(...update(N, nIds[0], randFields(N)));
      tx(`rand-${i}-multi`, parts);
    } else {
      const ns = pick([A, B]);
      const ids = alive(ns);
      if (!ids.length) continue;
      const eid = pick(ids);
      if (kind === "update") tx(`rand-${i}-update`, update(ns, eid, randFields(ns)));
      else if (kind === "merge") {
        if (ns === A) tx(`rand-${i}-merge`, merge(A, eid, { meta: { rnd: i, nested: { i } } }));
        else tx(`rand-${i}-merge`, merge(B, eid, { title: `merge-b-${i}` }));
      } else if (kind === "noop") {
        const fields = ns === A ? { name: state[A].get(eid)?.name ?? "same" } : { title: state[B].get(eid)?.title ?? "same" };
        tx(`rand-${i}-noop`, update(ns, eid, fields));
      } else {
        const fields = ns === A ? { name: `raw-${i}` } : { title: `raw-${i}` };
        tx(`rand-${i}-raw-update`, update(ns, eid, fields, false));
      }
    }
  }

  return { A, B, C, N, byLabel, schema, seedSteps, warmupId: aIds[4], hooks, script };
}

async function call(server, method, path, { body, token = adminToken } = {}) {
  const res = await fetch(server.url + path, {
    method,
    headers: {
      ...(token ? { authorization: `Bearer ${token}` } : {}),
      "app-id": server.appId,
      "content-type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = text;
  }
  return { status: res.status, body: parsed };
}

async function openAdmin(server) {
  const c = connect(server.url, server.appId, `${server.url}:webhooks-admin`);
  await c.open;
  c.send({
    op: "init",
    "app-id": server.appId,
    versions: { "@instantdb/core": "v0.22.76" },
    "__admin-token": adminToken,
  });
  const reply = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
  if (reply.op !== "init-ok") throw new Error(`init failed on ${server.url}: ${JSON.stringify(reply).slice(0, 400)}`);
  return c;
}

async function sendTx(conn, tx) {
  const ceid = crypto.randomUUID();
  conn.send({ "client-event-id": ceid, op: "transact", "tx-steps": tx.steps });
  const reply = await conn.waitFor((m) => m["client-event-id"] === ceid && (m.op === "transact-ok" || m.op === "error"));
  if (reply.op !== "transact-ok") {
    const detail = { status: reply.status, type: reply.type, message: reply.message, hint: reply.hint };
    throw new Error(`transaction ${tx.name} failed: ${JSON.stringify(detail)}\nsteps: ${JSON.stringify(tx.steps)}`);
  }
  return reply;
}

async function createHook(server, hook) {
  const res = await call(server, "POST", `/dash/apps/${server.appId}/webhooks`, {
    body: { url: hook.url, namespaces: hook.namespaces, actions: hook.actions },
  });
  if (res.status !== 200 || !res.body?.webhook?.id) {
    throw new Error(`create webhook ${hook.name} on ${server.url} failed: ${res.status} ${JSON.stringify(res.body).slice(0, 500)}`);
  }
  return res.body.webhook.id;
}

async function listEvents(server, hookId) {
  const res = await call(server, "GET", `/dash/apps/${server.appId}/webhooks/${hookId}/events`);
  if (res.status !== 200 || !Array.isArray(res.body?.events)) {
    throw new Error(`list events on ${server.url} failed: ${res.status} ${JSON.stringify(res.body).slice(0, 500)}`);
  }
  return res.body.events;
}

async function fetchPayload(server, hookId, isn) {
  // An event row can be delivered and recorded just before the history read
  // catches up; retry briefly like a receiver polling the payload URL.
  for (let i = 0; i < 40; i++) {
    const res = await call(server, "GET", `/webhooks/payload/${server.appId}/${hookId}/${isn}`);
    if (res.status === 200) return res.body;
    if (i === 39) {
      throw new Error(`payload ${isn} on ${server.url} failed: ${res.status} ${JSON.stringify(res.body).slice(0, 500)}`);
    }
    await sleep(50);
  }
}

function sortValue(v) {
  if (Array.isArray(v)) return v.map(sortValue);
  if (v && typeof v === "object") {
    return Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([k, x]) => [k, sortValue(x)]));
  }
  return v;
}

function normalizePayload(payload) {
  const data = (payload?.data ?? []).map((r) => ({
    namespace: r.namespace,
    action: r.action,
    id: r.id,
    before: r.before == null ? null : sortValue(r.before),
    after: r.after == null ? null : sortValue(r.after),
    // derived from the server's ISN, which is intentionally different
    idempotencyKey: typeof r.idempotencyKey === "string" ? "<idempotency-key>" : r.idempotencyKey,
  }));
  data.sort((a, b) => (canon(a) < canon(b) ? -1 : canon(a) > canon(b) ? 1 : 0));
  return {
    data,
    idempotencyKey: typeof payload?.idempotencyKey === "string" ? "<idempotency-key>" : payload?.idempotencyKey,
  };
}

async function waitDeltas(server, hooks, seen, { quietMs = EVENT_SETTLE_MS, maxMs = EVENT_MAX_WAIT_MS } = {}) {
  const started = Date.now();
  let lastNew = Date.now();
  // Events already discovered by this call stay in `observed`; otherwise the
  // same new row would count as "new" on every poll and reset the quiet timer
  // forever.
  const observed = new Map(hooks.map((h) => [h.id, []]));
  for (;;) {
    const lists = await Promise.all(hooks.map((h) => listEvents(server, h.id)));
    let gotNew = false;
    for (const [i, h] of hooks.entries()) {
      const already = new Set(observed.get(h.id).map((e) => e.isn));
      for (const e of lists[i]) {
        if (seen.get(h.id).has(e.isn) || already.has(e.isn)) continue;
        observed.get(h.id).push(e);
        already.add(e.isn);
        gotNew = true;
      }
    }
    if (gotNew) lastNew = Date.now();
    if (Date.now() - lastNew >= quietMs) break;
    if (Date.now() - started > maxMs) {
      const pending = [...observed.entries()].map(([id, es]) => `${id}: ${es.length}`).join(", ");
      throw new Error(`timed out waiting for webhook events on ${server.url} (${pending})`);
    }
    await sleep(60);
  }
  const out = [];
  for (const h of hooks) {
    const events = observed.get(h.id) ?? [];
    const payloads = [];
    for (const e of events) payloads.push(normalizePayload(await fetchPayload(server, h.id, e.isn)));
    payloads.sort((a, b) => (canon(a) < canon(b) ? -1 : canon(a) > canon(b) ? 1 : 0));
    out.push({ name: h.name, count: events.length, payloads });
    for (const e of events) seen.get(h.id).add(e.isn);
  }
  return out;
}

async function setupServer(server, plan) {
  const conn = await openAdmin(server);
  await sendTx(conn, { name: "schema", steps: plan.schema });
  await sendTx(conn, { name: "seed-data", steps: plan.seedSteps });
  const hooks = [];
  for (const spec of plan.hooks) hooks.push({ ...spec, id: await createHook(server, spec) });
  return { conn, hooks };
}

async function main() {
  const plan = buildPlan();
  const started = Date.now();
  const legacy = await setupServer(SERVERS.legacy, plan);
  const rust = await setupServer(SERVERS.rust, plan);

  // Warm up the flag that makes legacy log full entities to its WAL. Creating
  // a webhook writes it into the config app; the server learns it through its
  // config-app subscription, so wait for the first event before taking the
  // baseline. This is also the feasibility check: if legacy never produces a
  // webhook event here, the rest of the comparison cannot be trusted.
  const seenLegacy = new Map(legacy.hooks.map((h) => [h.id, new Set()]));
  const seenRust = new Map(rust.hooks.map((h) => [h.id, new Set()]));
  let warmLegacy;
  let warmRust;
  let warmed = false;
  // Creating a webhook writes the enable-wal-entity-log flag into legacy's
  // config app; its subscription can take a few seconds to observe it. Keep
  // doing a real update (different value each time, so it is not a no-op) until
  // both servers queue the first event, then start comparing from there.
  for (let attempt = 0; attempt < 60 && !warmed; attempt++) {
    const steps = [
      ["add-triple", plan.warmupId, plan.byLabel[plan.A].id, plan.warmupId],
      ["add-triple", plan.warmupId, plan.byLabel[plan.A].name, `warmup-${attempt}`],
    ];
    const warmup = { name: `warmup-${attempt}`, steps };
    await Promise.all([sendTx(legacy.conn, warmup), sendTx(rust.conn, warmup)]);
    [warmLegacy, warmRust] = await Promise.all([
      waitDeltas(SERVERS.legacy, legacy.hooks, seenLegacy, { quietMs: 150, maxMs: 900 }),
      waitDeltas(SERVERS.rust, rust.hooks, seenRust, { quietMs: 150, maxMs: 900 }),
    ]);
    const legacyFirst = warmLegacy.find((h) => h.name === plan.hooks[0].name);
    const rustFirst = warmRust.find((h) => h.name === plan.hooks[0].name);
    warmed = Boolean(legacyFirst?.count && rustFirst?.count);
  }
  if (!warmed) {
    throw new Error(
      "legacy did not produce a webhook event for repeated warm-up updates: its WAL consumer / enable-wal-entity-log flag is not active in this harness",
    );
  }
  // Drain anything still in flight before taking the baseline, so a delayed
  // warm-up record cannot be attributed to the first real transaction.
  await Promise.all([
    waitDeltas(SERVERS.legacy, legacy.hooks, seenLegacy, { quietMs: EVENT_SETTLE_MS }),
    waitDeltas(SERVERS.rust, rust.hooks, seenRust, { quietMs: EVENT_SETTLE_MS }),
  ]);

  let checked = 0;
  for (const tx of plan.script) {
    await Promise.all([sendTx(legacy.conn, tx), sendTx(rust.conn, tx)]);
    const [legacyDelta, rustDelta] = await Promise.all([
      waitDeltas(SERVERS.legacy, legacy.hooks, seenLegacy),
      waitDeltas(SERVERS.rust, rust.hooks, seenRust),
    ]);
    if (canon(legacyDelta) !== canon(rustDelta)) {
      console.error(`webhook event mismatch at ${tx.name}\nsteps: ${JSON.stringify(tx.steps)}`);
      console.error(`legacy: ${JSON.stringify(legacyDelta)}`);
      console.error(`rust:   ${JSON.stringify(rustDelta)}`);
      throw new Error(`webhook event production mismatch at ${tx.name}`);
    }
    checked++;
  }

  legacy.conn.close();
  rust.conn.close();
  console.log(
    `fuzz-webhooks PASSED: seed=${seed} transactions=${checked} hooks=${plan.hooks.length} ` +
      `actions=${JSON.stringify(plan.hooks.map((h) => h.actions))} (${Math.round((Date.now() - started) / 1000)}s)`,
  );
}

main().catch((e) => {
  console.error(`FUZZ-WEBHOOKS FAILED for seed ${seed}: ${e.message}`);
  process.exit(1);
});
