// Differential layer for legacy's rule-where rewriter: the CEL overloads of
// cel.clj:729-1617 (`_eq_dynamic`, `_neq_dynamic`, `_in_dynamic`,
// `_not_datakey`, `_not_whereclause`, `_datakey_starts_with` /
// `_ends_with` / `_contains`, `_size_refpath_size`, `_checked_data_ref`,
// `_timestamp_from_datakey` and the three `type` overrides). Legacy only
// evaluates them when the operator's `rule-wheres` flag lists the app
// (flags.clj:383-392, read from the Instant Config app the self-hosted
// bootstrap creates, tasks.clj bootstrap-config-app!): a query's view rules
// are then compiled with the where-clause compiler and, where they convert,
// added to the query as InstaQL where clauses (instaql.clj get-rule-wheres /
// add-rule-wheres-to-query), and the matched entities skip the per-entity
// rule check (get-etype+eid-check-result! :checked-by-rule-where). A rule
// that doesn't convert (the overload throws) falls back to the per-entity
// check. The rewriter is an optimization: the rust server always checks per
// entity, like legacy with the flag off.
//
// This layer turns the flag on for its own app on the legacy server, writes
// one view + create rule per probe namespace, and compares, per probe, what a
// guest gets: socket add-query results (the triples of the attrs the query
// reads, plus whatever else the join rows carried), the admin SDK's
// `asUser({guest: true})` query (object tree), and the outcome of the
// guest's creates. On legacy only, `/admin/query_perms_check` (debugQuery)
// returns the `rule-wheres` the rewriter produced: each probe checks that its
// overload ran there (the where clause it pins, or no where clause for a
// rule whose overload throws, next to a probe that converted in the same
// query) before its surface ids are recorded as covered.
//
// Usage: node rule-wheres.mjs <app-id> <admin-token>
//   (the app must exist on both servers with the same admin token, see
//    provision.sh)
// Env: LEGACY_URL / RUST_URL, LEGACY_DATABASE_URL / RUST_DATABASE_URL (rules
//      and the config app's admin token via psql), DUMP=1 prints each
//      server's results

import { execSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { note } from "./coverage-hook.mjs";
import { canon, connect, DIVERGENCE_PREFIXES, describeAllowVerdict, firstDifference, loadAllowlist, makeIdFactory, matchPin, normalize, projectTree, psql, uuid } from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken) throw new Error("usage: node rule-wheres.mjs <app-id> <admin-token>");

const SERVERS = {
  legacy: {
    url: process.env.LEGACY_URL || "http://localhost:8891",
    db: process.env.LEGACY_DATABASE_URL || "postgres://instant:instant@localhost:8890/instant",
  },
  rust: {
    url: process.env.RUST_URL || "http://localhost:8888",
    db: process.env.RUST_DATABASE_URL || "postgres://instant:instant@localhost:5432/instant",
  },
};
// config.clj instant-config-app-id: the app whose `rule-wheres` entity the
// legacy server subscribes to for its flags (flags_impl.clj init)
const CONFIG_APP_ID = "24a4d71b-7bb2-4630-9aee-01146af26239";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// rules written straight to Postgres: legacy evicts its rule cache off the
// WAL feed, so give it a beat before the next op
const RULES_SETTLE_MS = 1200;

// ---------------------------------------------------------------------------
// probes: one namespace each, its view and create rule are `rule`. `where`
// is the where clause legacy's rewriter must produce for it (a pin, see
// lib.mjs matchPin; `$in` lists are compared sorted); `throws` marks a rule
// whose overload throws in the rewriter, so legacy falls back to the
// per-entity check and debugQuery lists no rule-where for it. `covers` are
// the surface ids the probe exercises once its witness holds.

// Rules that would read a missing field (`null.startsWith(...)`) are guarded
// with `data.x != null && ...`: a rule erroring on any entity fails the whole
// query on rust and on legacy with the flag off (permission-evaluation-
// failed), so without the guard there would be no result sets to compare.
// `endswith` is left unguarded on purpose: with the flag on, legacy's where
// clause filters the erroring row out before any rule runs.
const PROBES = [
  { name: "eq", rule: "data.title == 'alpha'", where: { title: "alpha" }, covers: ["_eq_dynamic"] },
  { name: "eqnull", rule: "data.tag == null", where: { tag: { $isNull: true } }, covers: ["_eq_dynamic"] },
  { name: "neq", rule: "data.tag != 'x'", where: { tag: { $not: "x" } }, covers: ["_neq_dynamic"] },
  { name: "neqnull", rule: "data.tag != null", where: { tag: { $isNull: false } }, covers: ["_neq_dynamic"] },
  // a list literal is a java List, neither an ArrayList nor `sequential?`,
  // so `_in_dynamic` throws "invalid inputs for in" (cel.clj:1229-1233)
  { name: "inlist", rule: "data.tag in ['x', 'z']", throws: true, covers: ["_in_dynamic"] },
  { name: "inref", rule: "'ann' in data.ref('owner.name')", where: { "owner.name": "ann" }, covers: ["_in_dynamic", "_checked_data_ref"] },
  { name: "notkey", rule: "data.flag != null && !data.flag", where: { and: [{ flag: { $isNull: false } }, { flag: false }] }, covers: ["_not_datakey"] },
  {
    name: "notwhere",
    rule: "!(data.title == 'alpha' || data.tag == 'x')",
    where: { and: [{ title: { $not: "alpha" } }, { tag: { $not: "x" } }] },
    covers: ["_not_whereclause"],
  },
  {
    name: "startswith",
    rule: "data.title != null && data.title.startsWith('al')",
    where: { and: [{ title: { $isNull: false } }, { title: { $like: "al%" } }] },
    covers: ["_datakey_starts_with"],
  },
  { name: "endswith", rule: "data.title.endsWith('a')", where: { title: { $like: "%a" } }, covers: ["_datakey_ends_with"] },
  // cel.clj:1356 builds the pattern as (str "%" y "%s"): a stray `s`
  {
    name: "contains",
    rule: "data.title != null && data.title.contains('lph')",
    where: { and: [{ title: { $isNull: false } }, { title: { $like: "%lph%s" } }] },
    covers: ["_datakey_contains"],
  },
  // the prefix goes into the LIKE pattern unescaped: `_` matches any character
  {
    name: "likewild",
    rule: "data.title != null && data.title.startsWith('a_')",
    where: { and: [{ title: { $isNull: false } }, { title: { $like: "a_%" } }] },
    covers: ["_datakey_starts_with"],
  },
  { name: "sizeempty", rule: "size(data.ref('owner.id')) == 0", where: { owner: { $isNull: true } }, covers: ["_size_refpath_size", "_checked_data_ref"] },
  { name: "sizenonempty", rule: "size(data.ref('owner.id')) != 0", where: { owner: { $isNull: false } }, covers: ["_size_refpath_size"] },
  // `_timestamp_from_datakey` hands the DataKey to `_>_datakey_timestamp`,
  // whose clause carries a java.time.Instant (cel.clj:1076-1083) that the
  // query validation rejects for a date attr (instaql.clj get-rule-wheres
  // rule-wheres->where-conds), so legacy drops the rule-where like a throw
  { name: "timestamp", rule: "data.due != null && timestamp(data.due) > request.time", throws: true, covers: ["_timestamp_from_datakey"] },
  { name: "typekey", rule: "type(data.tag) == string", throws: true, covers: ["_type_datakey_override"] },
  { name: "typeref", rule: "type(data.ref('owner.name')) == list", throws: true, covers: ["_type_refpath_override"] },
  { name: "typewhere", rule: "data.title != null && type(data.title.startsWith('al')) == bool", throws: true, covers: ["_type_whereclause_override"] },
  // a like clause can't be negated (negate-where-clauses throws)
  { name: "notlike", rule: "data.title != null && !data.title.startsWith('al')", throws: true, covers: ["_not_whereclause"] },
];
// the probe whose rule-where proves the flag is on next to a `throws` probe
const CONTROL = "eq";
const ns = (p) => `rw_${p.name}`;
const itemsLabel = (p) => `items_${p.name}`;

// deterministic ids, identical on both servers (fresh per provisioned app)
const mk = makeIdFactory(appId);
const ids = {
  oId: mk(),
  oName: mk(),
  o1: mk(),
  o2: mk(),
  o3: mk(),
  passOff: mk(),
  passOn: mk(),
  probes: Object.fromEntries(
    PROBES.map((p) => [
      p.name,
      {
        id: mk(), title: mk(), tag: mk(), flag: mk(), due: mk(), owner: mk(), ownerRev: mk(),
        rows: [mk(), mk(), mk(), mk(), mk()],
        creates: [mk(), mk()],
      },
    ]),
  ),
};

// Five rows per probe namespace: matching and non-matching values for every
// probe, and a row missing each attr (r3, r5). `title` and `due` are
// indexed and typed (the rewriter's string and comparison overloads need
// both, cel.clj:1047-1083, 1326-1356), `tag` and `flag` are plain blobs.
const ROWS = [
  { title: "alpha", tag: "x", flag: true, due: "2020-01-01T00:00:00.000Z", owner: "o1" },
  { title: "alphas", tag: "y", flag: false, due: "2099-01-01T00:00:00.000Z", owner: "o2" },
  { title: "beta" },
  { title: "a_c", tag: "x", flag: true, due: "2020-06-01T00:00:00.000Z", owner: "o1" },
  { tag: "z", flag: false, due: "2099-06-01T00:00:00.000Z" },
];
// what the guest creates per probe: like r1 and like r5
const CREATES = [ROWS[0], ROWS[4]];

// fixed ids carry the app id's prefix; recorded values name them instead
// (`contains.r1`, `attr:rw_contains.title`, `o1`), so allowlist pins hold
// from run to run
const ID_NAMES = new Map([
  [ids.oId, "attr:rwowner.id"],
  [ids.oName, "attr:rwowner.name"],
  [ids.o1, "o1"],
  [ids.o2, "o2"],
  [ids.o3, "o3"],
  ...PROBES.flatMap((p) => {
    const a = ids.probes[p.name];
    return [
      ...["id", "title", "tag", "flag", "due", "owner"].map((l) => [a[l], `attr:${ns(p)}.${l}`]),
      ...a.rows.map((e, i) => [e, `${p.name}.r${i + 1}`]),
      ...a.creates.map((e, i) => [e, `${p.name}.c${i + 1}`]),
    ];
  }),
]);
function relabel(v) {
  if (typeof v === "string") return ID_NAMES.get(v) ?? v;
  if (Array.isArray(v)) return v.map(relabel);
  if (v !== null && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, relabel(x)]));
  return v;
}

const ceids = (() => {
  let i = 0;
  return () => {
    i++;
    return `00000000-0000-4000-9000-${String(i).padStart(12, "0")}`;
  };
})();

function attrStep(id, etype, label, extra = {}) {
  return [
    "add-attr",
    { id, "forward-identity": [id, etype, label], "value-type": "blob", cardinality: "one", "unique?": label === "id", "index?": label === "id", isUnsynced: true, ...extra },
  ];
}

function rowSteps(p, e, row) {
  const a = ids.probes[p.name];
  const steps = [["add-triple", e, a.id, e]];
  if ("title" in row) steps.push(["add-triple", e, a.title, row.title]);
  if ("tag" in row) steps.push(["add-triple", e, a.tag, row.tag]);
  if ("flag" in row) steps.push(["add-triple", e, a.flag, row.flag]);
  if ("due" in row) steps.push(["add-triple", e, a.due, row.due]);
  if ("owner" in row) steps.push(["add-triple", e, a.owner, ids[row.owner]]);
  return steps;
}

// The queries a guest runs per probe, and the attrs each reads (the client
// computes its result from the triples of those attrs: the forms' etypes and
// the attrs on a where path; any other join-row triple is compared apart).
// `admin` queries also go through the admin SDK as guest. The nested ones
// don't: legacy's permission filter drops a nested link form left without
// viewable rows (instaql.clj:1945-1954), so an impersonated admin query has
// no key where rust answers `[]`; the `emptyLink` query below pins that once.
function queriesFor(p, pass) {
  const a = ids.probes[p.name];
  // legacy answers a socket add-query it has answered before from its
  // query cache, keyed by the query as sent (before the rule-wheres
  // rewrite): every pass's queries are made unique
  const unique = { id: { $ne: pass } };
  const probeAttrs = [a.id, a.title, a.tag, a.flag, a.due, a.owner];
  const ownerAttrs = [ids.oId, ids.oName];
  return [
    // the probe's rule-where becomes the form's where
    { key: "top", q: { [ns(p)]: { $: { where: unique } } }, reads: probeAttrs, admin: true },
    { key: "withOwner", q: { [ns(p)]: { $: { where: unique }, owner: {} } }, reads: [...probeAttrs, ...ownerAttrs] },
    // ... the nested form's where (add-rule-wheres-to-query)
    { key: "nested", q: { rwowner: { $: { where: unique }, [itemsLabel(p)]: {} } }, reads: [...ownerAttrs, ...probeAttrs] },
    // ... and is added to a where path that reaches the probe's etype
    // (extend-where-with-rule-refs)
    { key: "wherePath", q: { rwowner: { $: { where: { ...unique, [`${itemsLabel(p)}.tag`]: "x" } } } }, reads: [...ownerAttrs, a.owner, a.tag], admin: true },
  ];
}

const rules = {
  rwowner: { allow: { view: "true" } },
  ...Object.fromEntries(PROBES.map((p) => [ns(p), { allow: { view: p.rule, create: p.rule } }])),
};

// ---------------------------------------------------------------------------
// http helpers

async function call(base, method, p, { headers = {}, json } = {}) {
  const h = { ...headers };
  if (json !== undefined) h["content-type"] = "application/json";
  const res = await fetch(base + p, { method, headers: h, body: json !== undefined ? JSON.stringify(json) : undefined });
  const text = await res.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { "<non-json>": text.slice(0, 200) };
  }
  return { status: res.status, body };
}
const asGuest = { "app-id": appId, authorization: `Bearer ${adminToken}`, "as-guest": "true" };
const adminOnly = { "app-id": appId, authorization: `Bearer ${adminToken}` };
const errView = (status, b) => normalize({ status, type: b?.type ?? null, message: b?.message ?? null });

// the object tree, entities in a stable order (rows seeded in one tx share
// their serverCreatedAt, so neither server's order is meaningful)
function treeView(res) {
  if (res.status !== 200) return errView(res.status, res.body);
  const sortTop = (v) => {
    if (Array.isArray(v)) return v.map(sortTop).sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
    if (v !== null && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, sortTop(x)]));
    return v;
  };
  return { status: 200, result: sortTop(projectTree(res.body?.result ?? res.body)) };
}

function triplesOf(result) {
  const out = [];
  const walk = (nodes) => {
    for (const node of nodes ?? []) {
      for (const rows of node?.data?.["datalog-result"]?.["join-rows"] ?? []) for (const t of rows) out.push(t);
      walk(node?.["child-nodes"]);
    }
  };
  walk(result);
  const norm = [...new Map(out.map((t) => normalize(t)).map((t) => [canon(t), t])).values()];
  return norm.sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
}

// ---------------------------------------------------------------------------
// legacy only: the rule-wheres flag and the debugQuery witness

async function enableRuleWheres() {
  const { url, db } = SERVERS.legacy;
  // the bootstrap gave the config app its (one) admin token
  const token = execSync(`psql "${db}" -tA -v ON_ERROR_STOP=1 -c "SELECT token FROM app_admin_tokens WHERE app_id = '${CONFIG_APP_ID}'"`).toString().trim();
  if (!token) throw new Error(`legacy has no admin token for its config app ${CONFIG_APP_ID} (does the image bootstrap it?)`);
  const res = await call(url, "POST", "/admin/transact", {
    headers: { "app-id": CONFIG_APP_ID, authorization: `Bearer ${token}` },
    json: { steps: [["update", "rule-wheres", uuid(), { "app-ids": [appId] }]] },
  });
  if (res.status !== 200) {
    throw new Error(`could not set the rule-wheres flag in legacy's config app ${CONFIG_APP_ID} (does the image bootstrap it?): ${res.status} ${JSON.stringify(res.body).slice(0, 400)}`);
  }
  // the flag reaches the server through its config-app subscription
  const control = PROBES.find((p) => p.name === CONTROL);
  for (let i = 0; i < 60; i++) {
    const w = await ruleWheres({ [ns(control)]: {} });
    if (w?.[ns(control)]) return;
    await sleep(500);
  }
  throw new Error("legacy never used rule-wheres for the app after the flag was set (30 s)");
}

// debugQuery's `rule-wheres` (admin/routes.clj query-perms-check), or
// {error} when legacy fails to answer
async function ruleWheres(q) {
  const res = await call(SERVERS.legacy.url, "POST", "/admin/query_perms_check", { headers: asGuest, json: { query: q } });
  if (res.status !== 200) return { error: `debugQuery answered ${res.status} ${JSON.stringify(res.body).slice(0, 400)}` };
  return res.body?.["rule-wheres"] ?? {};
}

const sortIn = (v) => {
  if (Array.isArray(v)) return v.map(sortIn);
  if (v !== null && typeof v === "object") {
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, k === "$in" && Array.isArray(x) ? [...x].sort() : sortIn(x)]));
  }
  return v;
};

// Did the probe's overload run in legacy's rewriter? Returns null when it
// did, else why not.
async function witness(p) {
  const control = PROBES.find((c) => c.name === CONTROL);
  const w = await ruleWheres(p.throws ? { [ns(p)]: {}, [ns(control)]: {} } : { [ns(p)]: {} });
  if (w.error) return w.error;
  const got = w[ns(p)];
  if (p.throws) {
    if (!w[ns(control)]) return `the control ${ns(control)} has no rule-where in the same query: ${JSON.stringify(w).slice(0, 400)}`;
    return got ? `expected no rule-where (the overload throws), got ${JSON.stringify(got).slice(0, 400)}` : null;
  }
  if (!got) return `no rule-where: ${JSON.stringify(w).slice(0, 400)}`;
  if (got["short-circuit?"] !== false) return `expected a where clause, got ${JSON.stringify(got).slice(0, 400)}`;
  return matchPin(sortIn(p.where), sortIn(got["where-clauses"]));
}

// ---------------------------------------------------------------------------
// scenario

// Seeds the schema, rows and rules, runs the guest's creates and returns a
// `queries()` that runs every probe query (legacy runs it with the flag off
// and again with it on; creates go through the per-entity evaluator either
// way, so they run once).
async function setUp(name, out) {
  const { url, db } = SERVERS[name];
  const msg = (conn, m) => conn.send({ "client-event-id": ceids(), ...m });
  const reply = (conn, ceid) => conn.waitFor((m) => (m.op === "transact-ok" || m.op === "error") && m["client-event-id"] === ceid);
  const record = (k, v) => {
    v = relabel(v);
    out[k] = v;
    if (process.env.DUMP) console.log(`[${name}] ${k}`, JSON.stringify(v));
  };

  const admin = connect(url, appId, `${name}:ADMIN`);
  const guest = connect(url, appId, `${name}:GUEST`);
  await Promise.all([admin.open, guest.open]);
  msg(admin, { op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.22.0" }, "__admin-token": adminToken });
  msg(guest, { op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.22.0" } });
  await Promise.all([admin.waitFor((m) => m.op === "init-ok"), guest.waitFor((m) => m.op === "init-ok")]);

  const adminTx = async (steps, what) => {
    const r = await reply(admin, msg(admin, { op: "transact", "tx-steps": steps }));
    if (r.op === "error") throw new Error(`${what} failed on ${name}: ${JSON.stringify(r).slice(0, 500)}`);
  };
  // schema and rows: small transacts, one per namespace
  await adminTx(
    [attrStep(ids.oId, "rwowner", "id"), attrStep(ids.oName, "rwowner", "name"), ["add-triple", ids.o1, ids.oId, ids.o1], ["add-triple", ids.o1, ids.oName, "ann"], ["add-triple", ids.o2, ids.oId, ids.o2], ["add-triple", ids.o2, ids.oName, "bob"], ["add-triple", ids.o3, ids.oId, ids.o3], ["add-triple", ids.o3, ids.oName, "cy"]],
    "owner seed",
  );
  for (const p of PROBES) {
    const a = ids.probes[p.name];
    await adminTx(
      [
        attrStep(a.id, ns(p), "id"),
        attrStep(a.title, ns(p), "title", { "index?": true, "checked-data-type": "string" }),
        attrStep(a.tag, ns(p), "tag"),
        attrStep(a.flag, ns(p), "flag"),
        attrStep(a.due, ns(p), "due", { "index?": true, "checked-data-type": "date" }),
        ["add-attr", { id: a.owner, "forward-identity": [a.owner, ns(p), "owner"], "reverse-identity": [a.ownerRev, "rwowner", itemsLabel(p)], "value-type": "ref", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true }],
      ],
      `${ns(p)} attrs`,
    );
    await adminTx(ROWS.flatMap((row, i) => rowSteps(p, a.rows[i], row)), `${ns(p)} rows`);
  }
  admin.close();
  psql(db, `INSERT INTO rules (app_id, code) VALUES ('${appId}', $rules$${JSON.stringify(rules)}$rules$::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`);
  await sleep(RULES_SETTLE_MS);

  for (const p of PROBES) {
    const a = ids.probes[p.name];
    for (const [i, row] of CREATES.entries()) {
      const r = await reply(guest, msg(guest, { op: "transact", "tx-steps": rowSteps(p, a.creates[i], row) }));
      record(`${p.name}/create${i + 1}`, r.op === "error" ? errView(r.status, r) : { ok: true });
    }
  }

  return {
    async queries(prefix, pass) {
      for (const p of PROBES) {
        for (const { key, q, reads, admin } of queriesFor(p, pass)) {
          const k = `${prefix}${p.name}/${key}`;
          const ceid = msg(guest, { op: "add-query", q });
          const r = await guest.waitFor((m) => (m.op === "add-query-ok" && canon(m.q) === canon(q)) || (m.op === "error" && m["client-event-id"] === ceid));
          if (r.op === "error") {
            record(k, errView(r.status, r));
          } else {
            const readSet = new Set(reads);
            const triples = triplesOf(r.result);
            record(k, { visible: triples.filter((t) => readSet.has(t[1])), extra: triples.filter((t) => !readSet.has(t[1])) });
            msg(guest, { op: "remove-query", q });
            await guest.waitFor((m) => m.op === "remove-query-ok" && canon(m.q) === canon(q));
          }
          if (admin) record(`${k}.admin`, treeView(await call(url, "POST", "/admin/query", { headers: asGuest, json: { query: q } })));
        }
      }
      // an owner with no items: nested as guest and as admin
      const emptyLink = { rwowner: { $: { where: { and: [{ id: ids.o3 }, { id: { $ne: pass } }] } }, [itemsLabel(PROBES[0])]: {} } };
      record(`${prefix}emptyLink.admin`, treeView(await call(url, "POST", "/admin/query", { headers: asGuest, json: { query: emptyLink } })));
      record(`${prefix}emptyLink.asAdmin`, treeView(await call(url, "POST", "/admin/query", { headers: adminOnly, json: { query: emptyLink } })));
    },
    close: () => guest.close(),
  };
}

// ---------------------------------------------------------------------------
// run, witness, diff
//
// Compared paths: `rulewheres:<probe>/create<n>` (the guest's creates),
// `rulewheres:<probe>/<query>` (socket add-query: {visible, extra}) and
// `rulewheres:<probe>/<query>.admin` (admin SDK as guest) with legacy's flag
// on, and the same queries under `rulewheres:flag-off/` with it off.

const results = { legacy: {}, rust: {} };
const witnessFailures = [];
{
  const legacy = await setUp("legacy", results.legacy);
  console.log("legacy: rule-where probes with the flag off…");
  await legacy.queries("flag-off/", ids.passOff);
  await enableRuleWheres();
  console.log("legacy: rule-wheres is on for the app; the probes again…");
  await legacy.queries("", ids.passOn);
  legacy.close();
  for (const p of PROBES) {
    const why = await witness(p);
    if (why) witnessFailures.push(`${p.name} (${p.rule}): ${why}`);
    else for (const id of p.covers) note(`cel:${id}`);
  }
}
{
  console.log("rust: rule-where probes…");
  const rust = await setUp("rust", results.rust);
  await rust.queries("flag-off/", ids.passOff);
  await rust.queries("", ids.passOn);
  rust.close();
}

// strict entries (lib.mjs loadAllowlist): this layer owns the rulewheres:
// paths and fails on any of them that allowed nothing
const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), { prefixes: DIVERGENCE_PREFIXES.rulewheres });
const diffs = [];
for (const k of new Set([...Object.keys(results.legacy), ...Object.keys(results.rust)])) {
  const l = results.legacy[k];
  const r = results.rust[k];
  if (canon(l) === canon(r)) continue;
  const p = `rulewheres:${k}`;
  const verdict = allowlist.check(p, l, r);
  diffs.push({ path: p, allowed: !!verdict.entry, verdict, legacy: l, rust: r });
}
const blocking = diffs.filter((d) => !d.allowed);
for (const d of diffs) {
  console.log(`\n[${d.allowed ? "ALLOWED" : "DIVERGENCE"}] ${d.path}`);
  const fd = firstDifference(d.legacy, d.rust);
  if (fd && fd.p) console.log(`  first differing sub-path: ${fd.p}`);
  console.log("  legacy:", JSON.stringify(d.legacy)?.slice(0, 2000));
  console.log("  rust:  ", JSON.stringify(d.rust)?.slice(0, 2000));
  for (const line of describeAllowVerdict(d.verdict)) console.log(line);
  if (!d.allowed) console.log(`  UNALLOWED ${JSON.stringify({ path: d.path, legacy: d.legacy, rust: d.rust })}`);
}
for (const w of witnessFailures) console.log(`\n[WITNESS FAILED] ${w}`);
const staleAllows = allowlist.reportStale();
if (blocking.length || staleAllows || witnessFailures.length) {
  console.error(
    `\nRULE-WHERES DIFFERENTIAL FAILED: ${blocking.length} unallowed divergences, ${staleAllows} stale allowlist entries, ${witnessFailures.length} probes legacy's rewriter did not run as pinned`,
  );
  process.exit(1);
}
console.log(`\nRULE-WHERES DIFFERENTIAL PASSED: ${PROBES.length} probes, ${Object.keys(results.rust).length} compared results, ${diffs.length} allowed divergences`);
