// Seeded parity layer for the server-side schema planner: `POST
// /dash/apps/:id/schema/push/{plan,apply}` and the same routes under
// /superadmin, which @instantdb/platform's `schemaPush` / `planSchemaPush`
// call (platform/src/api.ts: the body is the JSON of an InstantSchemaDef plus
// `check_types: true, supports_background_updates: true`). dash.mjs step 11 /
// step 38 diff a few fixed schemas; this layer generates schema evolutions.
//
// Every round starts from the schema pulled back from the server, turned
// into client defs the way platform's apiSchemaToInstantSchemaDef does (an
// i.string() / i.number() / ... DataAttrDef per attr, `required: true` unless
// optional, links with `required` / `onDelete` only when set, system
// namespaces included), then mutates them: namespaces added / dropped, attrs
// added / dropped / renamed / retyped (string, number, boolean, date, json,
// any, and bad valueTypes), unique / indexed / required flips on populated
// and empty attrs, links added / dropped / renamed / changed over every
// cardinality pair, required and onDelete, client key order shuffled, plus
// invalid schemas: link collisions (backwards, duplicate idents, blob vs link
// labels), cascade on a `many` side, reserved system attrs ($users.email,
// $files.url), system links (exact and with extra keys), custom `$`
// namespaces, unknown `has` / `onDelete` values. check_types and
// supports_background_updates are both varied, and the plan is sent to the
// dash or the superadmin route. Seeded data writes (/admin/transact, the
// admin SDK's update / link / delete grammar) are interleaved, with
// duplicates, nulls, missing values, wrong-typed and oversized values, so the
// indexing jobs a plan creates run against real data and fail on it. Now and
// then an attr is deleted or renamed through `schema/steps/apply` (what
// instant-cli itself sends), so plans meet renamed and soft-deleted attrs.
//
// Compared per round, legacy vs rust:
//   - the plan: HTTP status, the full error envelope (type / message / hint),
//     `new-schema`, `current-schema`, `current-attrs` (whole attr maps) and the
//     step list, in order;
//   - the apply result: the plan keys plus `transaction`, the steps with their
//     job ids and `indexing-jobs`;
//   - the indexing jobs once settled: every job's type / status / stage /
//     error, invalid-triple samples (entity ids are generated identically on
//     both servers) and error data;
//   - the pulled schema and attrs at the start of the next round, and every
//     data write's status / error envelope.
// Only server-chosen values are normalized: an attr / ident id is replaced by
// the name of the attr it belongs to on that server (so a step that points at
// the wrong attr still differs), a new attr's id by <uuid>, job / group ids,
// tx ids and timestamps. Step order is compared exactly when every entity,
// attr and link map of the request has at most eight keys: legacy reads the
// body into Clojure array-maps, which keep the client's key order, and walks
// them to plan. Past eight keys Clojure switches to hash maps, so for those
// requests the steps are compared as a sorted list plus, per attr, the order
// of that attr's own steps (and plan-error lists as a sorted set).
//
// The first unallowed difference stops the seed (the two apps may differ from
// then on) and fails it. Documented divergences live in
// allowed-divergences.json under `schemapush:` and are exercised by the fixed
// prefix of every seed, so a stale entry fails too.
//
// Usage: node fuzz-schema-push.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL, DUMP=1 (print every compared pair)

import path from "node:path";
import { fileURLToPath } from "node:url";
import { canon, DIVERGENCE_PREFIXES, describeAllowVerdict, firstDifference, isFixedId, loadAllowlist, makeIdFactory } from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
const seed = Number(process.argv[5] ?? 601);
const rounds = Number(process.argv[6] ?? 20);
if (!appIdLegacy || !appIdRust || !adminToken) {
  throw new Error("usage: node fuzz-schema-push.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");
}
const SERVERS = {
  legacy: { url: process.env.LEGACY_URL || "http://localhost:8891", appId: appIdLegacy },
  rust: { url: process.env.RUST_URL || "http://localhost:8888", appId: appIdRust },
};
const NAMES = ["legacy", "rust"];
const DUMP = process.env.DUMP === "1";
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
const rand = prng(seed * 7919 + 601);
const pick = (arr) => arr[Math.floor(rand() * arr.length)];
const chance = (p) => rand() < p;
const shuffle = (arr) => {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
};
const mk = makeIdFactory(appIdLegacy);
const clone = (v) => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));

// ---------------------------------------------------------------------------
// http

async function call(name, method, p, body) {
  const s = SERVERS[name];
  const res = await fetch(s.url + p.replaceAll(":app", s.appId), {
    method,
    headers: {
      authorization: `Bearer ${adminToken}`,
      "app-id": s.appId,
      ...(body !== undefined ? { "content-type": "application/json" } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = { "<non-json>": text.slice(0, 200) };
  }
  return { status: res.status, body: parsed };
}
async function both(fn) {
  const [legacy, rust] = await Promise.all(NAMES.map((n) => fn(n)));
  return { legacy, rust };
}

// ---------------------------------------------------------------------------
// normalization: server-chosen ids become the name of what they identify

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const UUID_ANY = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
const ids = { legacy: new Map([[appIdLegacy, "<app>"]]), rust: new Map([[appIdRust, "<app>"]]) };

function learnAttr(name, a) {
  if (!a || typeof a !== "object") return;
  const fi = a["forward-identity"];
  const ri = a["reverse-identity"];
  if (!Array.isArray(fi)) return;
  const n = `${fi[1]}.${fi[2]}`;
  if (typeof a.id === "string") ids[name].set(a.id, `attr:${n}`);
  if (typeof fi[0] === "string") ids[name].set(fi[0], `fwd:${n}`);
  if (Array.isArray(ri) && typeof ri[0] === "string") ids[name].set(ri[0], `rev:${ri[1]}.${ri[2]}`);
}
function learnAttrs(name, attrs) {
  for (const a of attrs ?? []) learnAttr(name, a);
}
// an applied plan's add-attr steps carry the ids the new attrs now have
function learnSteps(name, steps) {
  for (const s of steps ?? []) if (Array.isArray(s) && s[0] === "add-attr") learnAttr(name, s[1]);
}

function norm(name, v, key = null) {
  if (typeof v === "string") {
    if (isFixedId(v)) return v;
    if (UUID_RE.test(v)) return ids[name].get(v) ?? "<uuid>";
    if (ISO_RE.test(v)) return "<ts>";
    return v.replace(UUID_ANY, (m) => (isFixedId(m) ? m : ids[name].get(m) ?? "<uuid>"));
  }
  if (Array.isArray(v)) return v.map((x) => norm(name, x));
  if (v && typeof v === "object") {
    const out = {};
    for (const [k, x] of Object.entries(v)) {
      if (k === "trace-id") continue;
      if (k === "debug-uri") {
        out[k] = "<debug-uri>";
        continue;
      }
      out[k] = norm(name, x, k);
    }
    return out;
  }
  return v;
}

const attrKey = (a) => `${a?.["forward-identity"]?.[1]}.${a?.["forward-identity"]?.[2]}`;
function attrsByName(name, attrs) {
  const out = {};
  for (const a of attrs ?? []) out[attrKey(a)] = norm(name, a);
  return out;
}

const PLAN_MSG = "Validation failed for schema: ";
function errView(name, res, unordered = false) {
  const b = res.body ?? {};
  const view = {
    status: res.status,
    type: b.type ?? null,
    message: b.message ?? null,
    hint: norm(name, b.hint ?? null),
    keys: Object.keys(b).sort(),
  };
  if (typeof b["<non-json>"] === "string") view.body = b["<non-json>"];
  view.message = norm(name, view.message);
  // validate-required! lists the missing (entity, attr) rows in
  // (entity_id, attr_id) order and words the message from them; attr ids are
  // server-chosen, so compare the rows and the message parts as sets
  if (Array.isArray(view.hint?.records) && typeof view.message === "string" && view.message.startsWith("Missing required attribute")) {
    view.hint = { ...view.hint, records: sortCanon(view.hint.records) };
    view.message = view.message.split(/\. (?=Missing required attribute)/).sort().join(". ");
  }
  // plan errors follow the step order, which is the client's key order only
  // up to eight keys per map (see the header)
  if (unordered && view.hint?.["data-type"] === "schema" && Array.isArray(view.hint.errors)) {
    const msgs = view.hint.errors.map((e) => e.message);
    view.messageMatchesErrors = view.message === PLAN_MSG + msgs.join(", ");
    view.hint = { ...view.hint, errors: [...view.hint.errors].sort((x, y) => (canon(x) < canon(y) ? -1 : 1)) };
    // the same message, rebuilt from the errors in sorted order
    view.message = PLAN_MSG + view.hint.errors.map((e) => e.message).join(", ");
  }
  return view;
}

function stepsView(name, steps, unordered) {
  const n = norm(name, steps ?? null);
  if (!Array.isArray(n) || !unordered) return n;
  const byAttr = {};
  for (const s of n) {
    const k = `${s?.[1]?.["forward-identity"]?.[1]}.${s?.[1]?.["forward-identity"]?.[2]}`;
    (byAttr[k] ??= []).push(s);
  }
  return { sorted: [...n].sort((x, y) => (canon(x) < canon(y) ? -1 : 1)), byAttr };
}

function jobView(name, j) {
  const v = norm(name, {
    ...j,
    // legacy fills these from its sketch-based estimator; the CLI only uses
    // them for a progress percentage (same normalization as dash.mjs)
    work_estimate: j.work_estimate == null ? null : "<n>",
    work_completed: "<n?>",
  });
  if (Array.isArray(v.invalid_triples_sample)) {
    v.invalid_triples_sample = [...v.invalid_triples_sample].sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
  }
  v.keys = Object.keys(j).sort();
  return v;
}
const sortCanon = (arr) => [...arr].sort((x, y) => (canon(x) < canon(y) ? -1 : 1));

function planView(name, res, unordered) {
  if (res.status !== 200) return errView(name, res, unordered);
  const b = res.body;
  const view = {
    status: 200,
    keys: Object.keys(b).sort(),
    "new-schema": norm(name, b["new-schema"]),
    "current-schema": norm(name, b["current-schema"]),
    "current-attrs": attrsByName(name, b["current-attrs"]),
    steps: stepsView(name, b.steps, unordered),
  };
  if ("transaction" in b) {
    const t = b.transaction ?? {};
    view.transaction = {
      keys: Object.keys(t).sort(),
      id: typeof t.id,
      app_id: norm(name, t.app_id ?? null),
      created_at: norm(name, t.created_at ?? null),
      results: norm(name, t.results ?? null),
    };
  }
  if ("indexing-jobs" in b) {
    const ij = b["indexing-jobs"];
    view["indexing-jobs"] = ij
      ? { keys: Object.keys(ij).sort(), "group-id": norm(name, ij["group-id"]), jobs: sortCanon((ij.jobs ?? []).map((j) => jobView(name, j))) }
      : ij ?? "<absent>";
  }
  return view;
}

function pullView(name, res) {
  if (res.status !== 200) return errView(name, res);
  return {
    status: 200,
    keys: Object.keys(res.body).sort(),
    schema: norm(name, res.body.schema),
    attrs: attrsByName(name, res.body.attrs),
    "app-title": res.body["app-title"],
  };
}

function txView(name, res) {
  if (res.status !== 200) return errView(name, res);
  return { status: 200, keys: Object.keys(res.body ?? {}).sort(), txId: typeof res.body?.["tx-id"] };
}

// ---------------------------------------------------------------------------
// comparison

const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), { prefixes: DIVERGENCE_PREFIXES.schemapush });
const stats = { compared: 0, allowed: 0, exactOrder: 0, unorderedPlans: 0, plans: 0, planErrors: 0, applies: 0, applyErrors: 0, jobGroups: 0, jobsErrored: 0, jobsCompleted: 0, writes: 0, writeErrors: 0, cliSteps: 0 };
class Divergence extends Error {}

function compare(label, views, raw) {
  const p = `schemapush:${label}`;
  stats.compared++;
  const { legacy, rust } = views;
  if (DUMP) console.log(`-- ${p}\n  legacy ${JSON.stringify(legacy)}\n  rust   ${JSON.stringify(rust)}`);
  if (canon(legacy) === canon(rust)) return;
  const verdict = allowlist.check(p, legacy, rust);
  if (verdict.entry) {
    stats.allowed++;
    console.log(`[ALLOWED] ${p}`);
    for (const line of describeAllowVerdict(verdict)) console.log(line);
    return;
  }
  const fd = firstDifference(legacy, rust);
  console.error(`\n[DIVERGENCE] ${p} (seed ${seed})`);
  console.error(`  first difference at ${fd.p || "(whole value)"}:`);
  console.error(`    legacy: ${JSON.stringify(fd.a)?.slice(0, 1500)}`);
  console.error(`    rust:   ${JSON.stringify(fd.b)?.slice(0, 1500)}`);
  for (const line of describeAllowVerdict(verdict)) console.error(line);
  if (raw !== undefined) console.error(`  request: ${JSON.stringify(raw).slice(0, 4000)}`);
  console.error(`  legacy view: ${JSON.stringify(legacy).slice(0, 6000)}`);
  console.error(`  rust view:   ${JSON.stringify(rust).slice(0, 6000)}`);
  throw new Divergence(p);
}

// ---------------------------------------------------------------------------
// client defs (platform's apiSchemaToInstantSchemaDef + JSON.stringify)

const TYPES = ["string", "number", "boolean", "date", "json"];
const BAD_TYPES = ["text", "String", "int", "any", 5, null, ""];
const cap = (s) => s.charAt(0).toUpperCase() + s.slice(1);

// what i.string() / i.number() / ... .unique().indexed().optional() serialize to
function attrDef(valueType, { unique = false, indexed = false, required = true } = {}) {
  return { valueType, required, isIndexed: indexed, config: { indexed, unique }, metadata: {} };
}
const RELS = { "many-false": ["many", "many"], "one-true": ["one", "one"], "many-true": ["many", "one"], "one-false": ["one", "many"] };

function pulledToDefs(schema) {
  const sorted = (o) => Object.entries(o ?? {}).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const entities = {};
  for (const [etype, attrs] of sorted(schema.blobs)) {
    const defs = {};
    for (const [label, a] of sorted(attrs)) {
      if (label === "id") continue;
      defs[label] = attrDef(a["checked-data-type"] ?? "json", { unique: !!a["unique?"], indexed: !!a["index?"], required: !!a["required?"] });
      defs[label].metadata = { derivedType: { type: a["checked-data-type"] ?? "any", origin: a["checked-data-type"] ? "checked" : "unknown" } };
    }
    entities[etype] = { attrs: defs, links: {} };
  }
  const links = {};
  for (const [, a] of sorted(schema.refs)) {
    const [, fe, fl] = a["forward-identity"];
    const [, re, rl] = a["reverse-identity"];
    const [fhas, rhas] = RELS[`${a.cardinality}-${a["unique?"]}`] ?? [a.cardinality, "many"];
    const forward = { on: fe, has: fhas, label: fl };
    if (a["required?"]) forward.required = true;
    if (a["on-delete"] === "cascade") forward.onDelete = "cascade";
    const reverse = { on: re, has: rhas, label: rl };
    if (a["on-delete-reverse"] === "cascade") reverse.onDelete = "cascade";
    links[`${fe}${cap(fl)}`] = { forward, reverse };
  }
  return { entities, links, rooms: {} };
}

// Over eight keys a Clojure map loses the client's key order (header).
function largeMaps(defs) {
  const n = (o) => (o && typeof o === "object" && !Array.isArray(o) ? Object.keys(o).length : 0);
  if (n(defs?.entities) > 8 || n(defs?.links) > 8) return true;
  for (const e of Object.values(defs?.entities ?? {})) if (n(e?.attrs) > 8) return true;
  return false;
}

// A client's schema may name only part of what the app has (the planner never
// deletes, so whatever it omits is left alone). Most rounds send a subset of
// at most five entities, five attrs per entity and five links, so with up to
// three mutations every map stays within array-map size and the steps keep
// the client's order; a full-schema round past eight keys is only planned.
function subsetDefs(defs) {
  const pickKeys = (o, n) => {
    const keys = Object.keys(o ?? {});
    if (keys.length <= n) return keys;
    const keep = new Set(shuffle(keys).slice(0, n));
    return keys.filter((k) => keep.has(k));
  };
  const entities = {};
  for (const e of pickKeys(defs.entities, 5)) {
    const def = defs.entities[e];
    entities[e] = { ...def, attrs: Object.fromEntries(pickKeys(def.attrs, 5).map((l) => [l, def.attrs[l]])) };
  }
  const links = Object.fromEntries(pickKeys(defs.links, 5).map((k) => [k, defs.links[k]]));
  return { ...defs, entities, links };
}

// a `has` the server stores verbatim on apply (legacy) / refuses (rust): such
// plans are only planned in random rounds, never applied (see the suffix)
function hasUnknownCardinality(defs) {
  return Object.values(defs?.links ?? {}).some((l) => typeof l?.forward?.has === "string" && !["one", "many"].includes(l.forward.has));
}

const USER_NS = ["posts", "tags", "notes", "teams", "items", "events", "profiles", "orders"];
const ATTR_LABELS = ["title", "body", "count", "score", "flag", "when", "meta", "slug", "rank", "handle", "note", "kind", "level", "price", "with-dash", "ünïcode", "with space", "_under", "CamelCase"];
const LINK_LABELS = ["author", "owner", "tagged", "children", "parent", "members", "teamOf", "related", "items", "posts", "notesOf", "watchers"];

const userNs = (defs) => Object.keys(defs.entities).filter((e) => !e.startsWith("$"));
const blobNs = (defs) => Object.keys(defs.entities).filter((e) => !e.startsWith("$") || e === "$users" || e === "$files");
const freeLabel = (pool, taken) => {
  const free = pool.filter((l) => !taken.has(l));
  return free.length ? pick(free) : `${pick(pool)}${Math.floor(rand() * 1000)}`;
};
// every label already used on a namespace (attrs and both link sides)
function labelsOn(defs, ns) {
  const s = new Set(Object.keys(defs.entities[ns]?.attrs ?? {}));
  s.add("id");
  for (const l of Object.values(defs.links)) {
    if (l?.forward?.on === ns) s.add(l.forward.label);
    if (l?.reverse?.on === ns) s.add(l.reverse.label);
  }
  return s;
}

function randomAttrDef() {
  const valueType = chance(0.06) ? pick(BAD_TYPES) : pick(TYPES);
  const def = attrDef(valueType, { unique: chance(0.15), indexed: chance(0.25), required: chance(0.3) });
  if (chance(0.05)) def.required = pick(["yes", 1, null]);
  if (chance(0.04)) def.config.unique = pick(["no", 0, "yes"]);
  if (chance(0.04)) def.config.indexed = pick([0, "", "true"]);
  return def;
}

function randomLink(defs, { invalid = false } = {}) {
  const pool = [...userNs(defs), "$users", ...(chance(0.15) ? ["$files"] : [])];
  const from = pick(pool.filter((n) => n !== "$files") ?? pool);
  const to = pick(pool);
  const fwdLabel = freeLabel(LINK_LABELS, labelsOn(defs, from));
  const taken = labelsOn(defs, to);
  if (from === to) taken.add(fwdLabel);
  const revLabel = freeLabel(LINK_LABELS, taken);
  const has = pick(["one", "many"]);
  const rhas = pick(["one", "many"]);
  const forward = { on: from, has, label: fwdLabel };
  const reverse = { on: to, has: rhas, label: revLabel };
  if (chance(0.15)) forward.required = true;
  if (has === "one" && chance(0.25)) forward.onDelete = "cascade";
  if (rhas === "one" && chance(0.2)) reverse.onDelete = "cascade";
  if (invalid) {
    const k = rand();
    if (k < 0.3) forward.has = pick(["few", "ONE", "many "]);
    else if (k < 0.55) delete forward.has;
    else if (k < 0.8) (chance(0.5) ? forward : reverse).onDelete = pick(["restrict", "CASCADE", "set-null"]);
    else forward.onDelete = pick([true, 1]);
  }
  return [`${from}${cap(fwdLabel)}${Math.floor(rand() * 100)}`, { forward, reverse }];
}

// One mutation of the client defs; returns a short description.
function mutate(defs) {
  const ns = userNs(defs);
  const k = rand();
  const anyAttr = () => {
    const cands = blobNs(defs).flatMap((e) => Object.keys(defs.entities[e]?.attrs ?? {}).map((l) => [e, l]));
    return cands.length ? pick(cands) : null;
  };
  if (k < 0.08 && ns.length < 6) {
    const name = freeLabel(USER_NS, new Set(Object.keys(defs.entities)));
    const attrs = {};
    const n = Math.floor(rand() * 4);
    for (let i = 0; i < n; i++) attrs[freeLabel(ATTR_LABELS, new Set(Object.keys(attrs)))] = randomAttrDef();
    defs.entities[name] = { attrs, links: {} };
    return `add-ns ${name} (${n} attrs)`;
  }
  if (k < 0.12 && ns.length) {
    const name = pick(ns);
    delete defs.entities[name];
    return `drop-ns ${name}`;
  }
  if (k < 0.3) {
    const e = pick(blobNs(defs).length ? blobNs(defs) : ["$users"]);
    defs.entities[e] ??= { attrs: {}, links: {} };
    defs.entities[e].attrs ??= {};
    const label = freeLabel(ATTR_LABELS, labelsOn(defs, e));
    defs.entities[e].attrs[label] = randomAttrDef();
    return `add-attr ${e}.${label}`;
  }
  const at = anyAttr();
  if (k < 0.36 && at) {
    delete defs.entities[at[0]].attrs[at[1]];
    return `drop-attr ${at.join(".")}`;
  }
  if (k < 0.42 && at) {
    const [e, l] = at;
    const to = freeLabel(ATTR_LABELS, labelsOn(defs, e));
    defs.entities[e].attrs[to] = defs.entities[e].attrs[l];
    delete defs.entities[e].attrs[l];
    return `rename-attr ${e}.${l} -> ${to}`;
  }
  if (k < 0.62 && at) {
    const d = defs.entities[at[0]].attrs[at[1]];
    if (!d || typeof d !== "object") return "noop";
    d.config ??= {};
    const f = pick(["unique", "indexed", "required", "required"]);
    if (f === "unique") d.config.unique = !d.config.unique;
    if (f === "indexed") d.isIndexed = d.config.indexed = !d.config.indexed;
    if (f === "required") d.required = d.required !== true;
    return `flip-${f} ${at.join(".")}`;
  }
  if (k < 0.7 && at) {
    const d = defs.entities[at[0]].attrs[at[1]];
    if (!d || typeof d !== "object") return "noop";
    d.valueType = chance(0.1) ? pick(BAD_TYPES) : pick(TYPES);
    return `retype ${at.join(".")} ${JSON.stringify(d.valueType)}`;
  }
  const linkNames = Object.keys(defs.links);
  if (k < 0.8) {
    const [name, link] = randomLink(defs);
    defs.links[name] = link;
    return `add-link ${name}`;
  }
  if (k < 0.84 && linkNames.length) {
    const name = pick(linkNames);
    delete defs.links[name];
    return `drop-link ${name}`;
  }
  if (k < 0.87 && linkNames.length) {
    const name = pick(linkNames);
    const l = defs.links[name];
    if (l?.forward?.on) l.forward.label = freeLabel(LINK_LABELS, labelsOn(defs, l.forward.on));
    return `rename-link ${name}`;
  }
  if (k < 0.93 && linkNames.length) {
    const l = defs.links[pick(linkNames)];
    if (!l?.forward || !l?.reverse) return "noop";
    const f = pick(["has", "rhas", "onDelete", "onDeleteReverse", "required"]);
    if (f === "has") l.forward.has = l.forward.has === "one" ? "many" : "one";
    if (f === "rhas") l.reverse.has = l.reverse.has === "one" ? "many" : "one";
    if (f === "onDelete") l.forward.onDelete ? delete l.forward.onDelete : (l.forward.onDelete = "cascade");
    if (f === "onDeleteReverse") l.reverse.onDelete ? delete l.reverse.onDelete : (l.reverse.onDelete = "cascade");
    if (f === "required") l.forward.required ? delete l.forward.required : (l.forward.required = true);
    return `change-link ${f} ${l.forward.on}.${l.forward.label}`;
  }
  if (k < 0.96) return collide(defs);
  if (k < 0.98) {
    const [name, link] = randomLink(defs, { invalid: true });
    defs.links[name] = link;
    return `invalid-link ${name} ${JSON.stringify(link)}`;
  }
  return systemMutation(defs);
}

// a schema the planner must refuse (plan-errors), or that merely collides
function collide(defs) {
  const links = Object.values(defs.links).filter((l) => l?.forward && l?.reverse);
  const k = rand();
  if (k < 0.3 && links.length) {
    const l = pick(links);
    defs.links[`backwards${Math.floor(rand() * 100)}`] = { forward: { ...l.reverse, has: "many" }, reverse: { ...l.forward, has: "many" } };
    return `collide backwards ${l.forward.on}.${l.forward.label}`;
  }
  if (k < 0.5 && links.length) {
    const l = pick(links);
    const from = pick(userNs(defs).length ? userNs(defs) : ["$users"]);
    defs.links[`dupRev${Math.floor(rand() * 100)}`] = {
      forward: { on: from, has: "many", label: freeLabel(LINK_LABELS, labelsOn(defs, from)) },
      reverse: { on: l.reverse.on, has: "many", label: l.reverse.label },
    };
    return `collide dup-reverse ${l.reverse.on}.${l.reverse.label}`;
  }
  const withAttrs = blobNs(defs).filter((e) => Object.keys(defs.entities[e]?.attrs ?? {}).length);
  if (k < 0.7 && withAttrs.length) {
    const e = pick(withAttrs);
    const label = pick(Object.keys(defs.entities[e].attrs));
    defs.links[`blobClash${Math.floor(rand() * 100)}`] = {
      forward: { on: e, has: "one", label },
      reverse: { on: pick([...userNs(defs), "$users"]), has: "many", label: freeLabel(LINK_LABELS, new Set()) },
    };
    return `collide link-over-attr ${e}.${label}`;
  }
  if (k < 0.85 && links.length) {
    const l = pick(links);
    if (defs.entities[l.forward.on]) {
      defs.entities[l.forward.on].attrs ??= {};
      defs.entities[l.forward.on].attrs[l.forward.label] = attrDef("string", { required: false });
      return `collide attr-over-link ${l.forward.on}.${l.forward.label}`;
    }
  }
  const [name, link] = randomLink(defs);
  if (chance(0.5)) {
    link.forward.has = "many";
    link.forward.onDelete = "cascade";
  } else {
    link.reverse.has = "many";
    link.reverse.onDelete = "cascade";
  }
  defs.links[name] = link;
  return `collide cascade-on-many ${name}`;
}

function systemMutation(defs) {
  const k = rand();
  if (k < 0.2) {
    defs.entities.$foo = { attrs: { bar: attrDef("string", { required: false }) }, links: {} };
    return "system custom-$-namespace";
  }
  if (k < 0.3 && userNs(defs).length) {
    const from = pick(userNs(defs));
    defs.links[`${from}Dollar${Math.floor(rand() * 100)}`] = {
      forward: { on: from, has: "many", label: freeLabel(LINK_LABELS, labelsOn(defs, from)) },
      reverse: { on: pick(["$foo", "$magicCodes"]), has: "many", label: from },
    };
    return "system link-to-$-namespace";
  }
  if (k < 0.45 && defs.entities.$users?.attrs?.email) {
    defs.entities.$users.attrs.email = attrDef(pick(["string", "number"]), { unique: chance(0.5), indexed: chance(0.5), required: chance(0.5) });
    return "system reserved $users.email";
  }
  if (k < 0.6 && defs.entities.$files?.attrs?.url) {
    defs.entities.$files.attrs.url = attrDef("number", { required: false, indexed: true });
    return "system reserved $files.url";
  }
  const sys = Object.entries(defs.links).filter(([, l]) => l?.forward?.on?.startsWith?.("$") && l?.reverse?.on?.startsWith?.("$"));
  if (k < 0.8 && sys.length) {
    const [name] = pick(sys);
    delete defs.links[name];
    return `system drop-link ${name}`;
  }
  if (sys.length) {
    const [name, l] = pick(sys);
    // exactly the catalog's shape: stripped before planning
    defs.links[name] = {
      forward: { on: l.forward.on, has: l.forward.has, label: l.forward.label },
      reverse: { on: l.reverse.on, has: l.reverse.has, label: l.reverse.label },
    };
    return `system exact-link ${name}`;
  }
  return "noop";
}

function shuffleKeys(defs) {
  const sh = (o) => (o && typeof o === "object" && !Array.isArray(o) ? Object.fromEntries(shuffle(Object.entries(o))) : o);
  defs.entities = sh(defs.entities);
  for (const e of Object.values(defs.entities)) if (e && typeof e === "object" && e.attrs) e.attrs = sh(e.attrs);
  defs.links = sh(defs.links);
}

// ---------------------------------------------------------------------------
// data writes (/admin/transact, the admin SDK grammar)

const entities = {}; // ns -> fixed entity ids (identical on both servers)
const VALUES = {
  string: ["a", "b", "dup", "dup", "Zed", "", "ünï", "2024-01-01"],
  number: [1, 2, 2, -3.5, 0, 1e6],
  boolean: [true, false, false],
  date: ["2024-01-01T00:00:00Z", 1700000000000, "2024-03-05", "2024-02-30", "yesterday"],
  json: [{ k: 1 }, [1, 2], "str", 3, { nested: { a: [1] } }],
};
function valueFor(type) {
  const r = rand();
  if (r < 0.06) return null;
  if (r < 0.08) return "x".repeat(pick([3000, 9000]));
  if (r < 0.3) return pick(VALUES[pick(TYPES)]);
  return pick(VALUES[type] ?? VALUES.json);
}

function dataSteps(schema) {
  const blobs = Object.entries(schema?.blobs ?? {}).filter(([e]) => !e.startsWith("$"));
  const refs = Object.values(schema?.refs ?? {}).filter((a) => !a["forward-identity"][1].startsWith("$") && !a["reverse-identity"][1].startsWith("$"));
  const k = rand();
  if (blobs.length && k < 0.65) {
    const [ns, attrs] = pick(blobs);
    const pool = (entities[ns] ??= []);
    const eid = pool.length < 8 && (pool.length === 0 || chance(0.35)) ? (pool.push(mk()), pool[pool.length - 1]) : pick(pool);
    // One attr per write: legacy inserts a write's triples sorted by
    // (entity_id, attr_id, value_md5), so when several of them are invalid
    // the one it reports follows server-chosen attr ids.
    const labels = Object.keys(attrs).filter((l) => l !== "id");
    const vals = {};
    if (!labels.length || chance(0.04)) vals[freeLabel(ATTR_LABELS, new Set(Object.keys(attrs)))] = pick(VALUES.string);
    else {
      const l = pick(labels);
      vals[l] = valueFor(attrs[l]["checked-data-type"] ?? pick(TYPES));
    }
    return [[chance(0.2) ? "create" : "update", ns, eid, vals]];
  }
  if (refs.length && k < 0.85) {
    const a = pick(refs);
    const [, fe, fl] = a["forward-identity"];
    const [, re] = a["reverse-identity"];
    const from = (entities[fe] ??= []);
    const to = (entities[re] ??= []);
    if (!from.length) from.push(mk());
    if (!to.length) to.push(mk());
    return [[chance(0.85) ? "link" : "unlink", fe, pick(from), { [fl]: pick(to) }]];
  }
  const withEnts = Object.entries(entities).filter(([, v]) => v.length);
  if (!withEnts.length) return null;
  const [ns, pool] = pick(withEnts);
  return [["delete", ns, pick(pool)]];
}

async function writeData(label, steps) {
  const views = {};
  const res = await both((n) => call(n, "POST", `/admin/transact?app_id=:app`, { steps }));
  for (const n of NAMES) views[n] = txView(n, res[n]);
  stats.writes++;
  if (res.legacy.status !== 200) stats.writeErrors++;
  compare(label, views, steps);
}

// ---------------------------------------------------------------------------
// plan / apply / jobs / pull

let lastPull = null;
const plainSchema = (n, r) => (r.status === 200 ? { status: 200, keys: Object.keys(r.body).sort(), schema: norm(n, r.body.schema) } : errView(n, r));

async function settleJobs(name, groupId) {
  let last;
  for (let i = 0; i < 600; i++) {
    last = await call(name, "GET", `/dash/apps/:app/indexing-jobs/group/${groupId}`);
    const jobs = last.body?.jobs ?? [];
    if (last.status !== 200 || !jobs.some((j) => j.job_status === "waiting" || j.job_status === "processing")) return last;
    await sleep(100);
  }
  return last;
}

// route: "dash" | "superadmin"; op: "plan" | "apply"
async function push(label, route, op, body) {
  const unordered = largeMaps(body?.schema);
  const base = route === "dash" ? `/dash/apps/:app/schema/push/${op}` : `/superadmin/apps/:app/schema/push/${op}`;
  const res = await both((n) => call(n, "POST", base, body));
  const views = { legacy: planView("legacy", res.legacy, unordered), rust: planView("rust", res.rust, unordered) };
  if (op === "plan") stats.plans++;
  else stats.applies++;
  if (res.legacy.status === 200) {
    if (unordered) stats.unorderedPlans++;
    else stats.exactOrder++;
  } else if (op === "plan") stats.planErrors++;
  else stats.applyErrors++;
  compare(`${label}/${op}`, views, body);
  if (op === "apply" && res.legacy.status === 200 && res.rust.status === 200) {
    for (const n of NAMES) learnSteps(n, res[n].body.steps);
    const gl = res.legacy.body["indexing-jobs"]?.["group-id"];
    const gr = res.rust.body["indexing-jobs"]?.["group-id"];
    if (gl && gr) {
      const settled = { legacy: await settleJobs("legacy", gl), rust: await settleJobs("rust", gr) };
      stats.jobGroups++;
      for (const j of settled.legacy.body?.jobs ?? []) {
        if (j.job_status === "errored") stats.jobsErrored++;
        if (j.job_status === "completed") stats.jobsCompleted++;
      }
      const jv = (n) => (settled[n].status === 200 ? { status: 200, jobs: sortCanon((settled[n].body.jobs ?? []).map((j) => jobView(n, j))) } : errView(n, settled[n]));
      compare(`${label}/jobs`, { legacy: jv("legacy"), rust: jv("rust") });
    }
  }
  return res;
}

// instant-cli's own path: delete / rename an attr with schema/steps/apply,
// each server addressing its own attr ids
async function cliStep(label, schema) {
  const cands = [];
  for (const [e, attrs] of Object.entries(schema?.blobs ?? {})) {
    if (e.startsWith("$")) continue;
    for (const [l, a] of Object.entries(attrs)) if (l !== "id") cands.push(a);
  }
  for (const a of Object.values(schema?.refs ?? {})) if (!a["forward-identity"][1].startsWith("$") && !a["reverse-identity"][1].startsWith("$")) cands.push(a);
  if (!cands.length) return;
  const target = pick(cands);
  const [, e, l] = target["forward-identity"];
  const rename = chance(0.35) && target["value-type"] === "blob";
  const newLabel = `${l}2`;
  const stepsFor = (n) => {
    const a = (lastPullBy[n]?.attrs ?? []).find((x) => attrKey(x) === `${e}.${l}`);
    if (!a) return [];
    return rename ? [["update-attr", { id: a.id, "forward-identity": [a["forward-identity"][0], e, newLabel] }]] : [["delete-attr", a.id]];
  };
  const res = await both((n) => call(n, "POST", `/dash/apps/:app/schema/steps/apply`, { steps: stepsFor(n) }));
  stats.cliSteps++;
  const view = (n) =>
    res[n].status === 200
      ? { status: 200, keys: Object.keys(res[n].body).sort(), steps: norm(n, res[n].body.steps), transactionKeys: Object.keys(res[n].body.transaction ?? {}).sort(), "indexing-jobs": res[n].body["indexing-jobs"] ?? null }
      : errView(n, res[n]);
  compare(`${label}/cli-${rename ? "rename" : "delete"}`, { legacy: view("legacy"), rust: view("rust") }, `${e}.${l}`);
}
const lastPullBy = { legacy: null, rust: null };
async function pullBoth(label) {
  const res = await both((n) => call(n, "GET", `/dash/apps/:app/schema/pull`));
  for (const n of NAMES) {
    if (res[n].status === 200) learnAttrs(n, res[n].body.attrs);
    lastPullBy[n] = res[n].body;
  }
  compare(label, { legacy: pullView("legacy", res.legacy), rust: pullView("rust", res.rust) });
  if (chance(0.2)) {
    const sa = await both((n) => call(n, "GET", `/superadmin/apps/:app/schema`));
    compare(`${label}/superadmin`, { legacy: plainSchema("legacy", sa.legacy), rust: plainSchema("rust", sa.rust) });
  }
  lastPull = res.legacy.body;
  return res.legacy.body;
}

// ---------------------------------------------------------------------------
// fixed prefix: every seed covers these shapes (and the allowlist entries)

const PLATFORM_OPTS = { check_types: true, supports_background_updates: true };

async function fixedPrefix() {
  let pulled = await pullBoth("fixed/pull-initial");
  // 1. a realistic first push: a pulled schema (system namespaces and their
  // links, $streams.$files with its reverse onDelete) plus user entities
  const defs = pulledToDefs(pulled.schema);
  defs.entities.posts = {
    attrs: {
      title: attrDef("string", { indexed: true, required: false }),
      body: attrDef("string", { required: false }),
      slug: attrDef("string", { required: false }),
      views: attrDef("json", { required: false }),
      published: attrDef("boolean", { required: false }),
      // untyped and unindexed for now, so step 2 can store garbage in them
      at: attrDef("json", { required: false }),
      big: attrDef("json", { required: false }),
    },
    links: {},
  };
  defs.entities.tags = { attrs: { name: attrDef("string", { unique: true, indexed: true, required: false }) }, links: {} };
  defs.entities.$users ??= { attrs: {}, links: {} };
  defs.entities.$users.attrs.nickname = attrDef("string", { required: false });
  defs.entities.$files ??= { attrs: {}, links: {} };
  defs.entities.$files.attrs.caption = attrDef("string", { required: false });
  defs.links.postsAuthor = { forward: { on: "posts", has: "one", label: "author" }, reverse: { on: "$users", has: "many", label: "posts" } };
  defs.links.postsTags = { forward: { on: "posts", has: "many", label: "tags" }, reverse: { on: "tags", has: "many", label: "posts" } };
  defs.links.postsCover = { forward: { on: "posts", has: "one", label: "cover", onDelete: "cascade" }, reverse: { on: "$files", has: "one", label: "post" } };
  await push("fixed/initial", "superadmin", "plan", { schema: defs, ...PLATFORM_OPTS });
  await push("fixed/initial", "superadmin", "apply", { schema: defs, ...PLATFORM_OPTS });
  // re-pushing the pulled schema plans nothing (system links with onDelete
  // stay in new-schema and match the catalog)
  pulled = await pullBoth("fixed/pull-after-initial");
  await push("fixed/repush", "superadmin", "plan", { schema: pulledToDefs(pulled.schema), ...PLATFORM_OPTS });

  // 2. data that breaks every job kind: duplicate slugs, a missing / null
  // body, a string in views (number check), an unparseable date, an
  // oversized value in an attr that will be indexed
  const p = (entities.posts = [mk(), mk(), mk(), mk()]);
  const t = (entities.tags = [mk(), mk()]);
  await writeData("fixed/data-0", [
    ["update", "posts", p[0], { title: "first", slug: "dup", views: 10, body: "x", at: "2024-01-01T00:00:00Z", big: "small" }],
    ["update", "posts", p[1], { title: "second", slug: "dup", views: "lots", at: "not a date" }],
    ["update", "posts", p[2], { title: "third", slug: "solo", views: null, body: null, big: "x".repeat(9000) }],
    ["update", "tags", t[0], { name: "red" }],
    ["update", "tags", t[1], { name: "blue" }],
    ["link", "posts", p[0], { tags: t[0] }],
  ]);
  pulled = await pullBoth("fixed/pull-data");
  // 3. background jobs over that data: unique on duplicates, number check on
  // a string, required with a missing / null value, date check on garbage,
  // index on an oversized value, a unique that holds, remove-unique, a
  // required link some posts lack
  let d = pulledToDefs(pulled.schema);
  d.entities.posts.attrs.slug.config.unique = true;
  d.entities.posts.attrs.views.valueType = "number";
  d.entities.posts.attrs.body.required = true;
  d.entities.posts.attrs.at.valueType = "date";
  d.entities.posts.attrs.big.config.indexed = d.entities.posts.attrs.big.isIndexed = true;
  d.entities.posts.attrs.title.config.unique = true;
  d.entities.tags.attrs.name.config.unique = false;
  d.links.postsTags.forward.required = true;
  await push("fixed/jobs", "dash", "apply", { schema: d, ...PLATFORM_OPTS });
  // 3b. an untyped unique attr: a duplicate array value (legacy cuts the
  // value in its message at the array's comma) and an oversized value
  // (legacy echoes the row's NULL checked_data_type as the string "null")
  pulled = await pullBoth("fixed/pull-jobs-pre-uniq");
  d = pulledToDefs(pulled.schema);
  d.entities.uniq = { attrs: { v: attrDef("json", { unique: true, required: false }) }, links: {} };
  await push("fixed/uniq", "dash", "apply", { schema: d, check_types: true, supports_background_updates: false });
  const u = (entities.uniq = [mk(), mk()]);
  await writeData("fixed/data-unique-array-0", [["update", "uniq", u[0], { v: [1, 2] }]]);
  await writeData("fixed/data-unique-array-1", [["update", "uniq", u[1], { v: [1, 2] }]]);
  await writeData("fixed/data-too-large", [["update", "uniq", u[1], { v: "x".repeat(9000) }]]);

  // 4. the same flips without background updates (update-attr steps) and
  // with check_types off
  pulled = await pullBoth("fixed/pull-jobs");
  d = pulledToDefs(pulled.schema);
  d.entities.posts.attrs.slug.config.unique = !d.entities.posts.attrs.slug.config.unique;
  d.entities.posts.attrs.body.required = !d.entities.posts.attrs.body.required;
  d.entities.posts.attrs.title.config.indexed = d.entities.posts.attrs.title.isIndexed = !d.entities.posts.attrs.title.config.indexed;
  d.entities.posts.attrs.views.valueType = "string";
  await push("fixed/sync", "dash", "plan", { schema: d, check_types: false, supports_background_updates: false });
  await push("fixed/sync", "dash", "apply", { schema: d, check_types: true, supports_background_updates: false });

  // 5. plan errors: a backwards link, duplicate idents, a link over an attr,
  // an attr over a link, cascade on a many side (forward and reverse)
  pulled = await pullBoth("fixed/pull-sync");
  d = pulledToDefs(pulled.schema);
  d.links.tagsPosts = { forward: { on: "tags", has: "many", label: "posts" }, reverse: { on: "posts", has: "many", label: "tags" } };
  d.links.notesPosts = { forward: { on: "notes", has: "many", label: "post" }, reverse: { on: "posts", has: "many", label: "tags" } };
  d.links.postsSlug = { forward: { on: "posts", has: "one", label: "slug" }, reverse: { on: "tags", has: "many", label: "slugOf" } };
  d.entities.posts.attrs.author = attrDef("string", { required: false });
  d.links.postsRelated = { forward: { on: "posts", has: "many", label: "related", onDelete: "cascade" }, reverse: { on: "posts", has: "many", label: "relatedBy" } };
  d.links.postsLiked = { forward: { on: "posts", has: "many", label: "liked" }, reverse: { on: "tags", has: "many", label: "likedBy", onDelete: "cascade" } };
  await push("fixed/plan-errors", "dash", "plan", { schema: d, ...PLATFORM_OPTS });
  await push("fixed/plan-errors", "superadmin", "apply", { schema: d, ...PLATFORM_OPTS });

  // 6. link values no attr can hold: planned the same way by both servers;
  // applying them is a documented divergence (allowed-divergences.json)
  d = pulledToDefs(pulled.schema);
  d.links.postsEditor = { forward: { on: "posts", has: "few", label: "editor" }, reverse: { on: "$users", has: "many", label: "edited" } };
  d.links.postsTags.forward = { on: "posts", label: "tags" }; // has missing on an existing link: nil is ignored
  d.links.postsSeen = { forward: { on: "posts", has: "one", label: "seen", onDelete: true }, reverse: { on: "tags", has: "one", label: "seenBy", onDelete: "restrict" } };
  await push("fixed/invalid-link", "dash", "plan", { schema: d, ...PLATFORM_OPTS });
  d = pulledToDefs(pulled.schema);
  d.links.postsTags.forward = { on: "posts", label: "tags" };
  await push("fixed/invalid-link/missing-has-existing", "dash", "apply", { schema: d, ...PLATFORM_OPTS });
  d = pulledToDefs(pulled.schema);
  d.links.postsSeen = { forward: { on: "posts", has: "one", label: "seen" }, reverse: { on: "tags", has: "one", label: "seenBy", onDelete: "restrict" } };
  await push("fixed/invalid-link/restrict", "dash", "apply", { schema: d, ...PLATFORM_OPTS });
  d = pulledToDefs(pulled.schema);
  d.links.postsDraft = { forward: { on: "posts", label: "draft" }, reverse: { on: "$users", has: "many", label: "drafts" } };
  await push("fixed/invalid-link/missing-has", "superadmin", "apply", { schema: d, ...PLATFORM_OPTS });

  // 7. schema shapes the planner cannot read
  const malformed = {
    "schema-string": "posts",
    "schema-array": [],
    "entities-array": { entities: [1], links: {} },
    "entity-string": { entities: { posts: "x" } },
    "attrs-string": { entities: { posts: { attrs: "x" } } },
    "links-array": { entities: {}, links: [1] },
    "link-string": { links: { a: "x" } },
    "link-forward-missing": { links: { a: { reverse: { on: "tags", has: "many", label: "x" } } } },
    "link-label-number": { links: { a: { forward: { on: "posts", has: "one", label: 3 }, reverse: { on: "tags", has: "many", label: "x" } } } },
  };
  for (const [name, schema] of Object.entries(malformed)) {
    await push(`fixed/malformed/${name}`, chance(0.5) ? "dash" : "superadmin", "plan", { schema, ...PLATFORM_OPTS });
  }
  // shapes both servers read the same way: null parts, non-map attr defs
  await push("fixed/lenient", "dash", "plan", { schema: { entities: { posts: null, tags: { attrs: null }, notes: { attrs: { a: "str", b: null } } }, links: null }, ...PLATFORM_OPTS });
  await push("fixed/no-schema", "dash", "plan", {});

  // 8. system namespaces: a custom $ namespace (dropped), reserved attrs
  // (dropped), custom $users / $files attrs, an exact catalog link (dropped)
  // and one with an extra key (kept, so it plans an update of a system attr)
  d = pulledToDefs(pulled.schema);
  d.entities.$foo = { attrs: { bar: attrDef("string") }, links: {} };
  d.entities.$users.attrs.email = attrDef("number", { unique: false, indexed: false });
  d.entities.$users.attrs.level = attrDef("number", { required: false, indexed: true });
  const sys = Object.entries(d.links).filter(([, l]) => l.forward.on.startsWith("$") && l.reverse.on.startsWith("$"));
  for (const [name, l] of sys) d.links[name] = { forward: { on: l.forward.on, has: l.forward.has, label: l.forward.label }, reverse: { on: l.reverse.on, has: l.reverse.has, label: l.reverse.label } };
  await push("fixed/system", "superadmin", "apply", { schema: d, ...PLATFORM_OPTS });
  // a link onto a custom `$` namespace is not a catalog link, so it is
  // planned; applying it hits the reserved-namespace check
  d = pulledToDefs(pulled.schema);
  d.links.postsFoos = { forward: { on: "posts", has: "many", label: "foos" }, reverse: { on: "$foo", has: "many", label: "posts" } };
  await push("fixed/system-dollar-link", "dash", "apply", { schema: d, ...PLATFORM_OPTS });
  // a required attr on a `$` namespace trips legacy's trigger (migration 92)
  d = pulledToDefs(pulled.schema);
  d.entities.$files ??= { attrs: {}, links: {} };
  d.entities.$files.attrs.tagline = attrDef("string", { required: true });
  await push("fixed/system-required", "superadmin", "apply", { schema: d, ...PLATFORM_OPTS });
  pulled = await pullBoth("fixed/pull-system");
  if (sys.length) {
    d = pulledToDefs(pulled.schema);
    const [name, l] = sys[0];
    d.links[name] = { forward: { on: l.forward.on, has: l.forward.has, label: l.forward.label }, reverse: { on: l.reverse.on, has: l.reverse.has, label: l.reverse.label }, extra: 1 };
    await push("fixed/system-link-extra-key", "superadmin", "plan", { schema: d, ...PLATFORM_OPTS });
    await push("fixed/system-link-extra-key", "superadmin", "apply", { schema: d, ...PLATFORM_OPTS });
  }

  // 9. client key order: the same additions written in two orders
  d = pulledToDefs(pulled.schema);
  d.entities = { zebras: { attrs: { stripes: attrDef("number", { required: false }), age: attrDef("number", { required: false }) }, links: {} }, ...d.entities, apes: { attrs: { name: attrDef("string", { required: false }) }, links: {} } };
  d.links = { zebrasKeeper: { forward: { on: "zebras", has: "one", label: "keeper" }, reverse: { on: "apes", has: "many", label: "zebras" } }, ...d.links };
  const small = { entities: Object.fromEntries(Object.entries(d.entities).filter(([e]) => ["zebras", "apes", "posts"].includes(e))), links: { zebrasKeeper: d.links.zebrasKeeper } };
  await push("fixed/order", "dash", "plan", { schema: small, ...PLATFORM_OPTS });
  await push("fixed/order-reversed", "dash", "plan", { schema: { entities: Object.fromEntries(Object.entries(small.entities).reverse()), links: small.links }, ...PLATFORM_OPTS });
}

// the very last probe: an unknown `has` is stored by legacy and refused here,
// after which the two schemas differ, so nothing is compared afterwards
async function fixedSuffix() {
  const pulled = await pullBoth("fixed/pull-final");
  const d = pulledToDefs(pulled.schema);
  d.links.postsReviewer = { forward: { on: "posts", has: "few", label: "reviewer" }, reverse: { on: "$users", has: "many", label: "reviewed" } };
  await push("fixed/invalid-link/unknown-has", "dash", "apply", { schema: d, ...PLATFORM_OPTS });
}

// ---------------------------------------------------------------------------

async function main() {
  const started = Date.now();
  let failed = null;
  let round = 0;
  try {
    await fixedPrefix();
    for (; round < rounds; round++) {
      const nWrites = Math.floor(rand() * 3);
      for (let i = 0; i < nWrites; i++) {
        const steps = dataSteps(lastPull?.schema);
        if (steps) await writeData(`round/${round}/data-${i}`, steps);
      }
      if (chance(0.08)) await cliStep(`round/${round}`, lastPull?.schema);
      const pulled = await pullBoth(`round/${round}/pull`);
      const full = chance(0.15);
      const defs = full ? pulledToDefs(pulled.schema) : subsetDefs(pulledToDefs(pulled.schema));
      const muts = [];
      const n = 1 + Math.floor(rand() * 3);
      for (let i = 0; i < n; i++) muts.push(mutate(defs));
      if (chance(0.5)) shuffleKeys(defs);
      const body = {
        schema: defs,
        check_types: chance(0.85),
        supports_background_updates: chance(0.8),
        ...(chance(0.2) ? { overwrite: false } : {}),
      };
      const route = chance(0.6) ? "superadmin" : "dash";
      console.log(`round ${round}: ${route}${full ? " full" : ""} ${muts.join("; ")} (check_types=${body.check_types} bg=${body.supports_background_updates})`);
      const k = rand();
      // past eight keys legacy plans in hash order, so which of several
      // failing steps an apply meets first is not the client's order either
      const noApply = hasUnknownCardinality(defs) || largeMaps(defs);
      if (k < 0.3 || noApply) {
        await push(`round/${round}`, route, "plan", body);
      } else if (k < 0.85) {
        const planned = await push(`round/${round}`, route, "plan", body);
        if (planned.legacy.status === 200) await push(`round/${round}`, route, "apply", body);
      } else {
        await push(`round/${round}`, route, "apply", body);
      }
    }
    await fixedSuffix();
  } catch (e) {
    if (!(e instanceof Divergence)) throw e;
    failed = e.message;
  }
  const staleCount = failed ? 0 : allowlist.reportStale(console.error);
  console.log(`schema-push stats: ${JSON.stringify(stats)}`);
  // every seed's fixed prefix creates errored and completed jobs and a plan
  // error; a seed that saw none of them did not exercise the planner
  const floor = [];
  if (!failed) {
    if (!stats.jobsErrored) floor.push("no indexing job errored");
    if (!stats.jobsCompleted) floor.push("no indexing job completed");
    if (!stats.planErrors) floor.push("no plan was refused");
    if (!stats.exactOrder) floor.push("no step list was compared in order");
  }
  if (failed || staleCount || floor.length) {
    console.error(
      `FUZZ-SCHEMA-PUSH FAILED for seed ${seed}` +
        (failed ? `: divergence at ${failed} (round ${round})` : "") +
        (staleCount ? `: ${staleCount} stale allowlist entries` : "") +
        (floor.length ? `: ${floor.join(", ")}` : ""),
    );
    process.exit(1);
  }
  console.log(`fuzz-schema-push PASSED: seed=${seed} rounds=${rounds} compared=${stats.compared} allowed=${stats.allowed} (${Math.round((Date.now() - started) / 1000)}s)`);
}

main().catch((e) => {
  console.error(`FUZZ-SCHEMA-PUSH FAILED for seed ${seed}: ${e.stack || e.message}`);
  process.exit(1);
});
