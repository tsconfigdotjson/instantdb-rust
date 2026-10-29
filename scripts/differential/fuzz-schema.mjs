// Schema-evolution fuzz layer: seeded inline add-attr / update-attr /
// delete-attr / restore-attr interleaved with data writes, one-shot queries
// and live subscriptions.
//
// The other fuzz layers exercise schemaless add-attr and the occasional
// delete/restore, but no layer drives the schema itself as a long-lived,
// mutable object while clients keep reading and subscribing. This one does:
// links are added with forward and reverse identities (cardinality one/many,
// unique, indexed, checked data types, required), flags are flipped and
// identities renamed, attrs are soft-deleted and restored, and every step is
// bracketed by writes and reads to the attrs before and after the change.
//
// Compared against the live legacy server:
//   - every transact outcome, including the complete error envelope
//     (status, type, message, hint);
//   - every one-shot query result (where / order / pagination) on the attrs
//     before, during and after the mutation, with each server's own page
//     cursors fed back page by page;
//   - each session's folded subscription state at settled checkpoints (the
//     result the client computes from add-query-ok / refresh-ok) and the
//     refresh errors it received;
//   - the attrs a client sees in init-ok and in refresh-ok, folded and
//     compared as the client's attr map.
//
// Admin and non-admin paths: schema mutations that legacy admin-gates
// (update-attr / delete-attr / restore-attr) run on the admin session, and
// a non-admin session's attempts are compared as permission-denied errors.
// Inline add-attr and data transacts run on both sessions where legacy
// allows them.
//
// Usage: node fuzz-schema.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL (defaults as in fuzz.mjs)

import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  canon,
  connect,
  describeAllowVerdict,
  DIVERGENCE_PREFIXES,
  firstDifference,
  loadAllowlist,
  makeIdFactory,
  normalize,
  projectAttrs,
  settle,
  uuid,
} from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
const seed = Number(process.argv[5] ?? 31);
const rounds = Number(process.argv[6] ?? 100);
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node fuzz-schema.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");

const SERVERS = {
  legacy: { url: process.env.LEGACY_URL || "http://localhost:8891", appId: appIdLegacy },
  rust: { url: process.env.RUST_URL || "http://localhost:8888", appId: appIdRust },
};
const SESSIONS = ["ADMIN", "USER"];
const CHECKPOINT_QUIET_MS = 2000;

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
  {
    id,
    "forward-identity": [id, ns, label],
    "value-type": "blob",
    cardinality: "one",
    "unique?": false,
    "index?": false,
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
    cardinality: "one",
    "unique?": false,
    "index?": false,
    ...extra,
  },
];
const updateAttr = (id, patch) => ["update-attr", { id, ...patch }];
const getAttr = (id, patch = {}) => ["update-attr", { id, ...patch }];

function buildScript() {
  const rand = prng(seed * 7919 + 31);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const shuffle = (arr) => {
    const out = [...arr];
    for (let i = out.length - 1; i > 0; i--) {
      const j = Math.floor(rand() * (i + 1));
      [out[i], out[j]] = [out[j], out[i]];
    }
    return out;
  };
  const mk = makeIdFactory(appIdLegacy);

  const NS = "sch";
  const OWN = "schowner";
  const TAG = "schtag";
  const PRO = "schprofile";
  const EMPTY = "schempty";

  const attrs = {
    id: mk(),
    slot: mk(),
    score: mk(),
    label: mk(),
    dup: mk(),
    extra: mk(),
    complete: mk(),
    when: mk(),
    ok: mk(),
    body: mk(),
    numMany: mk(),
    owner: mk(),
    ownerRev: mk(),
    tags: mk(),
    tagsRev: mk(),
    profile: mk(),
    profileRev: mk(),
    ownId: mk(),
    ownName: mk(),
    tagId: mk(),
    tagLabel: mk(),
    proId: mk(),
    proHandle: mk(),
    emptyId: mk(),
  };
  // dynamic schema-evolution attrs
  const dyn = {
    idx: mk(),
    uniq: mk(),
    many: mk(),
    link: mk(),
    linkRev: mk(),
    req: mk(),
    reqEmpty: mk(),
    cdt: mk(),
    rename: mk(),
  };
  const ghost = mk();

  const entities = Array.from({ length: 8 }, () => mk());
  const owners = Array.from({ length: 3 }, () => mk());
  const tags = Array.from({ length: 5 }, () => mk());
  const profiles = Array.from({ length: 4 }, () => mk());
  const emptyEntity = mk();
  const names = ["ann", "bob", "cy"];
  const tagLabels = ["red", "green", "blue", "gold", "nope"];
  const handles = ["h0", "h1", "h2", "h3"];
  const nums = [0, 1, 2.5, 7, -3, 100, 42, 9];
  const strs = ["alpha", "beta", "gamma", "Zed", "alphabet", "delta", "epsilon", "zeta"];
  const dates = [
    0, -86400000, 946684800000, 1704067200000, 1718445600000,
    "2024-01-01T00:00:00Z", "2024-06-15T12:00:00Z", "2024-06-15",
  ];

  const script = [];
  const tx = (session, steps, features = [], label = "") => script.push({ kind: "tx", session, steps, features, label });
  const query = (session, q, features = [], label = "") => script.push({ kind: "query", session, q, features, label });
  const paginate = (session, spec, features = [], label = "") => script.push({ kind: "paginate", session, spec, features, label });
  const subscribe = (session, q, features = [], label = "") => script.push({ kind: "subscribe", session, q, features, label });
  const unsubscribe = (session, q, features = [], label = "") => script.push({ kind: "unsubscribe", session, q, features, label });
  const initAttrs = (session, label = "") => script.push({ kind: "init-attrs", session, label });
  const checkpoint = (label = "") => script.push({ kind: "checkpoint", label });

  // ---- initial catalog -------------------------------------------------
  tx(
    "ADMIN",
    [
      blob(attrs.id, NS, "id", { "unique?": true, "index?": true }),
      blob(attrs.slot, NS, "slot", { "unique?": true, "index?": true, "checked-data-type": "string" }),
      blob(attrs.score, NS, "score", { "index?": true, "checked-data-type": "number" }),
      blob(attrs.label, NS, "label"),
      blob(attrs.dup, NS, "dup"),
      blob(attrs.extra, NS, "extra"),
      blob(attrs.complete, NS, "complete"),
      blob(attrs.when, NS, "when", { "index?": true, "checked-data-type": "date" }),
      blob(attrs.ok, NS, "ok", { "index?": true, "checked-data-type": "boolean" }),
      blob(attrs.body, NS, "body"),
      blob(attrs.numMany, NS, "numMany", { cardinality: "many", "index?": true, "checked-data-type": "number" }),
      refAttr(attrs.owner, NS, "owner", attrs.ownerRev, OWN, "owned"),
      refAttr(attrs.tags, NS, "tags", attrs.tagsRev, TAG, "items", { cardinality: "many" }),
      refAttr(attrs.profile, NS, "profile", attrs.profileRev, PRO, "ownerProfile", { "unique?": true }),
      blob(attrs.ownId, OWN, "id", { "unique?": true, "index?": true }),
      blob(attrs.ownName, OWN, "name", { "index?": true, "checked-data-type": "string" }),
      blob(attrs.tagId, TAG, "id", { "unique?": true, "index?": true }),
      blob(attrs.tagLabel, TAG, "label", { "unique?": true, "index?": true, "checked-data-type": "string" }),
      blob(attrs.proId, PRO, "id", { "unique?": true, "index?": true }),
      blob(attrs.proHandle, PRO, "handle", { "unique?": true, "index?": true, "checked-data-type": "string" }),
      blob(attrs.emptyId, EMPTY, "id", { "unique?": true, "index?": true }),
    ],
    ["add-attr"],
    "initial schema",
  );
  tx(
    "ADMIN",
    [
      ...owners.flatMap((o, i) => [
        ["add-triple", o, attrs.ownId, o],
        ["add-triple", o, attrs.ownName, names[i]],
      ]),
      ...tags.flatMap((t, i) => [
        ["add-triple", t, attrs.tagId, t],
        ["add-triple", t, attrs.tagLabel, tagLabels[i]],
      ]),
      ...profiles.flatMap((p, i) => [
        ["add-triple", p, attrs.proId, p],
        ["add-triple", p, attrs.proHandle, handles[i]],
      ]),
    ],
    [],
    "seed link targets",
  );
  const seedEntitySteps = entities.flatMap((e, i) => [
    ["add-triple", e, attrs.id, e],
    ["add-triple", e, attrs.slot, `slot-${i}`],
    ["add-triple", e, attrs.score, nums[i % nums.length]],
    // label values are all distinct, dup values are deliberately not
    ["add-triple", e, attrs.label, strs[i % strs.length]],
    ["add-triple", e, attrs.dup, i < 4 ? "same" : `dup-${i}`],
    ["add-triple", e, attrs.extra, `extra-${i}`],
    // complete is incomplete on purpose: entities 5-7 have no value yet
    ...(i < 5 ? [["add-triple", e, attrs.complete, `done-${i}`]] : []),
    ["add-triple", e, attrs.when, dates[i % dates.length]],
    ["add-triple", e, attrs.ok, i % 2 === 0],
    ["add-triple", e, attrs.body, { i, tag: i % 2 === 0 ? "even" : "odd" }],
    ["add-triple", e, attrs.numMany, i],
    ["add-triple", e, attrs.numMany, i + 100],
    ["add-triple", e, attrs.owner, owners[i % owners.length]],
    ["add-triple", e, attrs.tags, tags[i % tags.length]],
    ["add-triple", e, attrs.tags, tags[(i + 1) % tags.length]],
    ...(i < 4 ? [["add-triple", e, attrs.profile, profiles[i]]] : []),
  ]);
  tx("ADMIN", seedEntitySteps, [], "seed entities");
  initAttrs("ADMIN", "after setup");
  initAttrs("USER", "after setup");
  checkpoint("setup");

  // Long-lived subscriptions on a stable namespace and on a mutated attr.
  const stableQuery = { [NS]: {} };
  const scoreQuery = { [NS]: { $: { where: { score: { $gte: 0 } }, order: { score: "asc" }, limit: 3 } } };
  for (const s of SESSIONS) {
    subscribe(s, stableQuery, ["subscription"], "stable namespace");
    subscribe(s, scoreQuery, ["subscription"], "mutated score");
  }
  checkpoint("subscriptions");

  const allEntities = () => entities;
  const writeValues = (attrId, values, session = "ADMIN") =>
    entities.forEach((e, i) => {
      const v = Array.isArray(values) ? values[i % values.length] : values;
      if (v !== undefined) tx(session, [["add-triple", e, attrId, v]], ["data"], "write values");
    });

  // ---- add-attr: indexed / typed, non-admin create, then flag flips -----
  tx("USER", [blob(dyn.idx, NS, "dynIdx", { "index?": true, "checked-data-type": "number" })], ["add-attr", "index", "checked-type"], "add indexed attr as user");
  for (const [i, e] of entities.entries()) tx("USER", [["add-triple", e, dyn.idx, nums[i % nums.length]]], ["data"], "write dynIdx");
  checkpoint("after add indexed");
  query("USER", { [NS]: { $: { where: { dynIdx: { $gte: 0 } }, order: { dynIdx: "asc" }, limit: 3 } } }, ["where", "order", "pagination"], "query dynIdx");
  paginate("ADMIN", { ns: NS, key: "dynIdx", dir: "asc", size: 2, where: { dynIdx: { $gte: 0 } } }, ["pagination", "order", "where"], "page dynIdx");
  // a checked type violation on the newly typed attr
  tx("ADMIN", [["add-triple", entities[0], dyn.idx, "not-a-number"]], ["checked-type"], "violate dynIdx type");
  tx("ADMIN", [updateAttr(dyn.idx, { "index?": false })], ["update-attr", "index"], "dynIdx index off");
  checkpoint("after index off");
  query("ADMIN", { [NS]: { $: { order: { dynIdx: "asc" }, limit: 2 } } }, ["order", "pagination"], "order unindexed dynIdx");
  tx("ADMIN", [updateAttr(dyn.idx, { "index?": true })], ["update-attr", "index"], "dynIdx index on");
  checkpoint("after index on");
  query("ADMIN", { [NS]: { $: { where: { dynIdx: { $lt: 50 } }, order: { dynIdx: "desc" }, first: 2 } } }, ["where", "order", "pagination"], "query dynIdx again");

  // ---- add-attr: unique, duplicate values before/after flip ------------
  tx("ADMIN", [blob(dyn.uniq, NS, "dynUniq", { "unique?": true, "index?": true, "checked-data-type": "string" })], ["add-attr", "unique", "index", "checked-type"], "add unique attr");
  for (let i = 0; i < 5; i++) tx("USER", [["add-triple", entities[i], dyn.uniq, `u-${i}`]], ["data"], "write unique");
  tx("ADMIN", [["add-triple", entities[0], dyn.uniq, "u-dup"]], ["data"], "write unique candidate");
  tx("ADMIN", [["add-triple", entities[1], dyn.uniq, "u-dup"]], ["data"], "duplicate unique value (must fail)");
  checkpoint("after unique violation");
  tx("ADMIN", [updateAttr(dyn.uniq, { "unique?": false })], ["update-attr", "unique"], "unique off with no duplicate");
  tx("USER", [["add-triple", entities[1], dyn.uniq, "u-dup"]], ["data", "unique"], "duplicate now allowed");
  tx("ADMIN", [updateAttr(dyn.uniq, { "unique?": true })], ["update-attr", "unique"], "unique on with duplicates (must fail)");
  checkpoint("after unique duplicate flip");
  query("ADMIN", { [NS]: { $: { where: { dynUniq: "u-dup" }, order: { dynUniq: "asc" } } } }, ["where", "order"], "query duplicate unique");
  tx("ADMIN", [updateAttr(dyn.uniq, { "unique?": false })], ["update-attr", "unique"], "unique off again");
  tx("ADMIN", [["retract-triple", entities[1], dyn.uniq, "u-dup"]], ["data"], "remove one duplicate");
  tx("ADMIN", [updateAttr(dyn.uniq, { "unique?": true })], ["update-attr", "unique"], "unique on without duplicates");
  tx("USER", [["add-triple", entities[2], dyn.uniq, "u-dup"]], ["unique"], "unique violation after flip");
  tx("ADMIN", [updateAttr(dyn.uniq, { "unique?": false })], ["update-attr", "unique"], "unique off final");
  checkpoint("after unique cycle");

  // ---- add-attr: cardinality many --------------------------------------
  tx("ADMIN", [blob(dyn.many, NS, "dynMany", { cardinality: "many", "index?": true, "checked-data-type": "number" })], ["add-attr", "index", "checked-type"], "add many attr");
  for (const [i, e] of entities.slice(0, 4).entries()) {
    tx("USER", [
      ["add-triple", e, dyn.many, i],
      ["add-triple", e, dyn.many, i + 10],
    ], ["data"], "write many");
  }
  checkpoint("after add many");
  // A non-paginated equality on a has-many attr has one where row per match,
  // so the result is the entity's complete triple set on both servers; the
  // paginated form's DISTINCT-ON where row is plan-dependent (see
  // docs/PARITY.md) and is not used for result comparison here.
  query("ADMIN", { [NS]: { $: { where: { dynMany: 0 } } } }, ["where"], "query many equality");
  query("ADMIN", { [NS]: { $: { where: { dynMany: { $gte: 0 } }, order: { dynMany: "asc" }, limit: 4 } } }, ["where", "order", "pagination"], "query many order (must fail)");
  paginate("USER", { ns: NS, key: "dynMany", dir: "desc", size: 3, where: { dynMany: { $gte: 0 } } }, ["pagination", "order", "where"], "page many");
  tx("ADMIN", [updateAttr(dyn.many, { "index?": false })], ["update-attr", "index"], "many index off");
  checkpoint("many index off");
  tx("ADMIN", [updateAttr(dyn.many, { "index?": true })], ["update-attr", "index"], "many index on");

  // ---- update-attr: checked-data-type is a raw-update no-op in legacy --
  tx("ADMIN", [updateAttr(attrs.label, { "checked-data-type": "number" })], ["update-attr", "checked-type"], "checked type over existing strings");
  tx("USER", [["add-triple", entities[0], attrs.label, "still-a-string"]], ["checked-type", "data"], "write after checked type flip");
  query("ADMIN", { [NS]: { $: { where: { label: "still-a-string" } } } }, ["where"], "query after checked type flip");
  tx("ADMIN", [updateAttr(attrs.when, { "checked-data-type": "string" })], ["update-attr", "checked-type"], "date attr checked type flip");
  query("ADMIN", { [NS]: { $: { where: { when: { $gte: "2024-01-01T00:00:00Z" } }, order: { when: "asc" } } } }, ["where", "order"], "query date after checked type flip");
  tx("ADMIN", [blob(dyn.cdt, NS, "cdtSwitch", { "checked-data-type": "string" })], ["add-attr", "checked-type"], "add checked string attr");
  tx("USER", [["add-triple", entities[2], dyn.cdt, "string-value"]], ["data"], "write checked string");
  tx("ADMIN", [updateAttr(dyn.cdt, { "checked-data-type": "number" })], ["update-attr", "checked-type"], "checked type string->number over data");
  tx("USER", [["add-triple", entities[2], dyn.cdt, "still-string"]], ["checked-type", "data"], "write after string->number flip");
  checkpoint("after checked type cycle");

  // ---- update-attr: required on populated / incomplete namespaces ------
  tx("ADMIN", [blob(dyn.req, NS, "dynReq", { "required?": true })], ["add-attr", "required"], "required on populated namespace (must fail)");
  tx("USER", [blob(dyn.reqEmpty, EMPTY, "reqEmpty", { "required?": true })], ["add-attr", "required"], "required on empty namespace");
  tx("USER", [["add-triple", emptyEntity, attrs.emptyId, emptyEntity], ["add-triple", emptyEntity, dyn.reqEmpty, "present"]], ["data", "required"], "write required empty");
  tx("ADMIN", [updateAttr(dyn.reqEmpty, { "required?": false })], ["update-attr", "required"], "required off");
  tx("ADMIN", [updateAttr(dyn.reqEmpty, { "required?": true })], ["update-attr", "required"], "required on complete empty ns");
  tx("ADMIN", [updateAttr(attrs.complete, { "required?": true })], ["update-attr", "required"], "required on incomplete namespace (must fail)");
  for (let i = 5; i < 8; i++) tx("ADMIN", [["add-triple", entities[i], attrs.complete, `done-${i}`]], ["data"], "fill incomplete");
  tx("ADMIN", [updateAttr(attrs.complete, { "required?": true })], ["update-attr", "required"], "required on now complete namespace");
  tx("USER", [["retract-triple", entities[0], attrs.complete, "done-0"]], ["required"], "retract required (must fail)");
  tx("ADMIN", [updateAttr(attrs.complete, { "required?": false })], ["update-attr", "required"], "required off again");
  checkpoint("after required cycle");

  // ---- unique on an existing attr with duplicates ----------------------
  tx("ADMIN", [updateAttr(attrs.dup, { "unique?": true })], ["update-attr", "unique"], "unique on duplicate attr (must fail)");
  tx("ADMIN", [updateAttr(attrs.dup, { "index?": true })], ["update-attr", "index"], "index on duplicate attr");
  tx("ADMIN", [updateAttr(attrs.dup, { "unique?": false })], ["update-attr", "unique"], "unique off duplicate attr");
  for (const e of entities.slice(1, 4)) tx("ADMIN", [["retract-triple", e, attrs.dup, "same"]], ["data"], "remove duplicates");
  tx("ADMIN", [updateAttr(attrs.dup, { "unique?": true })], ["update-attr", "unique"], "unique on now unique attr");
  tx("USER", [["add-triple", entities[5], attrs.dup, "same"]], ["unique"], "unique duplicate after flip (must fail)");
  tx("ADMIN", [updateAttr(attrs.dup, { "unique?": false })], ["update-attr", "unique"], "unique off final");
  checkpoint("after dup unique cycle");

  // ---- index flips on an existing attr --------------------------------
  tx("ADMIN", [updateAttr(attrs.score, { "index?": false })], ["update-attr", "index"], "score index off");
  query("ADMIN", { [NS]: { $: { order: { score: "asc" }, limit: 2 } } }, ["order", "pagination"], "order score unindexed");
  query("USER", { [NS]: { $: { where: { score: { $gt: 0 } }, order: { score: "desc" } } } }, ["where", "order"], "where score unindexed");
  checkpoint("score index off");
  tx("ADMIN", [updateAttr(attrs.score, { "index?": true })], ["update-attr", "index"], "score index on");
  paginate("ADMIN", { ns: NS, key: "score", dir: "asc", size: 3, where: { score: { $gte: 0 } } }, ["pagination", "order", "where"], "page score");
  checkpoint("score index on");

  // ---- update-attr renames (forward and reverse identities) ------------
  const renamedLabel = `labelRenamed${seed % 97}`;
  tx("ADMIN", [updateAttr(attrs.label, { "forward-identity": [attrs.label, NS, renamedLabel] })], ["update-attr", "rename"], "rename blob attr");
  query("ADMIN", { [NS]: { $: { where: { [renamedLabel]: "still-a-string" } } } }, ["where", "rename"], "query renamed attr");
  query("USER", { [NS]: { $: { where: { label: "still-a-string" } } } }, ["where", "rename"], "query old attr label (must fail)");
  tx("USER", [["add-triple", entities[1], attrs.label, "after-rename"]], ["data"], "write renamed attr by id");
  tx("ADMIN", [updateAttr(attrs.label, { "forward-identity": [attrs.label, NS, "label"] })], ["update-attr", "rename"], "rename blob attr back");
  checkpoint("after blob rename");

  const ownerRenamed = `ownerRenamed${seed % 89}`;
  const ownedRenamed = `ownedRenamed${seed % 89}`;
  tx("ADMIN", [updateAttr(attrs.owner, {
    "forward-identity": [attrs.owner, NS, ownerRenamed],
    "reverse-identity": [attrs.ownerRev, OWN, ownedRenamed],
  })], ["update-attr", "rename"], "rename link both identities");
  query("ADMIN", { [NS]: { [ownerRenamed]: {} } }, ["rename", "where"], "nested renamed link");
  query("USER", { [OWN]: { [ownedRenamed]: {} } }, ["rename", "where"], "reverse renamed link");
  query("ADMIN", { [NS]: { owner: {} } }, ["rename"], "old link label (must fail)");
  tx("USER", [["add-triple", entities[6], attrs.owner, owners[0]]], ["data"], "write renamed link by id");
  tx("ADMIN", [updateAttr(attrs.owner, {
    "forward-identity": [attrs.owner, NS, "owner"],
    "reverse-identity": [attrs.ownerRev, OWN, "owned"],
  })], ["update-attr", "rename"], "rename link back");
  checkpoint("after link rename");

  // ---- add / mutate / delete / restore a link --------------------------
  tx("ADMIN", [refAttr(dyn.link, NS, "dynLink", dyn.linkRev, OWN, "dynBack")], ["add-attr", "link"], "add link attr");
  for (const [i, e] of entities.slice(0, 6).entries()) tx("USER", [["add-triple", e, dyn.link, owners[i % owners.length]]], ["data", "link"], "write link");
  checkpoint("after add link");
  query("ADMIN", { [NS]: { dynLink: {} } }, ["link", "where"], "nested added link");
  query("USER", { [OWN]: { dynBack: {} } }, ["link", "where"], "nested reverse added link");
  tx("ADMIN", [updateAttr(dyn.link, {
    "forward-identity": [dyn.link, NS, "dynLink2"],
    "reverse-identity": [dyn.linkRev, OWN, "dynBack2"],
  })], ["update-attr", "rename", "link"], "rename added link");
  query("ADMIN", { [NS]: { dynLink2: {} } }, ["link", "rename"], "nested renamed added link");
  tx("ADMIN", [["delete-attr", dyn.link]], ["delete-attr", "link"], "soft delete link");
  query("ADMIN", { [NS]: { dynLink2: {} } }, ["link", "delete-attr"], "query deleted link (must fail)");
  tx("USER", [["add-triple", entities[0], dyn.link, owners[1]]], ["link", "delete-attr"], "write deleted link (must fail)");
  checkpoint("after link delete");
  tx("ADMIN", [["restore-attr", dyn.link]], ["restore-attr", "link"], "restore link");
  query("ADMIN", { [NS]: { dynLink2: {} } }, ["link", "restore-attr"], "query restored link");
  tx("USER", [["add-triple", entities[0], dyn.link, owners[1]]], ["link", "data"], "write restored link");
  checkpoint("after link restore");

  // ---- soft delete / restore a blob attr with data ---------------------
  tx("ADMIN", [["delete-attr", attrs.extra]], ["delete-attr"], "soft delete extra");
  initAttrs("ADMIN", "after delete extra");
  query("ADMIN", { [NS]: { $: { where: { extra: "extra-0" } } } }, ["delete-attr", "where"], "query deleted extra (must fail)");
  tx("USER", [["add-triple", entities[0], attrs.extra, "after-delete"]], ["delete-attr"], "write deleted extra (must fail)");
  // non-admin schema mutations are denied, even for an already-deleted attr
  tx("USER", [["delete-attr", attrs.extra]], ["permission"], "user delete attr denied");
  tx("USER", [["restore-attr", attrs.extra]], ["permission"], "user restore attr denied");
  tx("USER", [updateAttr(attrs.extra, { "index?": true })], ["permission"], "user update attr denied");
  checkpoint("after extra delete");
  tx("ADMIN", [["restore-attr", attrs.extra]], ["restore-attr"], "restore extra");
  initAttrs("USER", "after restore extra");
  query("USER", { [NS]: { $: { where: { extra: "extra-0" } } } }, ["restore-attr", "where"], "query restored extra");
  tx("USER", [["add-triple", entities[0], attrs.extra, "after-restore"]], ["restore-attr", "data"], "write restored extra");

  // delete/restore an indexed attr: restore deliberately drops the index
  tx("ADMIN", [["delete-attr", dyn.idx]], ["delete-attr", "index"], "soft delete indexed attr");
  tx("ADMIN", [["restore-attr", dyn.idx]], ["restore-attr", "index"], "restore indexed attr");
  checkpoint("after indexed restore");
  query("ADMIN", { [NS]: { $: { order: { dynIdx: "asc" }, limit: 2 } } }, ["restore-attr", "order", "pagination"], "order restored attr (index dropped)");
  tx("ADMIN", [updateAttr(dyn.idx, { "index?": true })], ["update-attr", "index"], "reindex restored attr");
  checkpoint("after reindex");

  // ---- unknown ids are no-ops for legacy schema updates ----------------
  tx("ADMIN", [getAttr(ghost, { "index?": true })], ["update-attr"], "update unknown attr (no-op)");
  tx("ADMIN", [["delete-attr", ghost]], ["delete-attr"], "delete unknown attr (no-op)");
  tx("ADMIN", [["restore-attr", ghost]], ["restore-attr"], "restore unknown attr (no-op)");
  // duplicate forward identity: the same error on both servers
  tx("ADMIN", [blob(mk(), NS, "label")], ["add-attr"], "duplicate forward identity (must fail)");
  checkpoint("after schema no-ops");

  // ---- extra seeded rounds ---------------------------------------------
  const baseAttrs = [
    ["score", attrs.score, nums],
    ["label", attrs.label, strs],
    ["dup", attrs.dup, ["same", "d-1", "d-2", "d-3"]],
    ["when", attrs.when, dates],
    ["ok", attrs.ok, [true, false]],
    ["body", attrs.body, [{ k: 1 }, { k: 2 }, { even: true }]],
    ["numMany", attrs.numMany, nums],
    ["dynIdx", dyn.idx, nums],
    ["dynMany", dyn.many, nums],
    ["dynUniq", dyn.uniq, ["u-0", "u-1", "u-2", "u-3", "u-4"]],
    ["dynLink2", dyn.link, owners],
  ];
  // Extra rounds keep data/subscription churn, but query only card-one,
  // typed, indexed attrs the random flag flips never touch: the mandatory
  // scenarios above already cover reads across every schema mutation.
  const queryLabels = ["score", "when", "ok"];
  const queryAttrs = baseAttrs.filter(([label]) => queryLabels.includes(label));
  const mutableAttrs = baseAttrs.filter(([label]) => !queryLabels.includes(label));
  const randomQuery = () => {
    const [label, , values] = pick(queryAttrs);
    const opts = {};
    const mode = rand();
    if (mode < 0.35) opts.where = { [label]: pick(values) };
    else if (mode < 0.6) opts.where = { [label]: { [pick(["$gte", "$lte", "$ne"])]: pick(values) } };
    else if (mode < 0.75) opts.where = { [label]: { $in: [pick(values), pick(values)] } };
    if (chance(0.6)) opts.order = { [label]: pick(["asc", "desc"]) };
    if (chance(0.65)) opts[pick(["limit", "first", "last"])] = 1 + Math.floor(rand() * 4);
    if (opts.limit && chance(0.3)) opts.offset = Math.floor(rand() * 3);
    return { [NS]: Object.keys(opts).length ? { $: opts } : {} };
  };
  const extra = Math.max(0, Math.min(rounds, 600) - script.length);
  for (let i = 0; i < extra; i++) {
    const r = rand();
    if (r < 0.42) {
      const [label, id, values] = pick(baseAttrs);
      const e = pick(entities);
      // links need a target, everything else takes a raw value
      const value = label === "dynLink2" ? pick(values) : pick(values);
      tx(pick(SESSIONS), [["add-triple", e, id, value]], ["data"]);
    } else if (r < 0.68) {
      query(pick(SESSIONS), randomQuery(), ["where", "order", "pagination"]);
    } else if (r < 0.8) {
      const [label, id] = pick(mutableAttrs);
      tx("ADMIN", [updateAttr(id, { [pick(["index?", "unique?", "required?"])]: chance(0.5) })], ["update-attr", pick(["index", "unique", "required"])]);
    } else if (r < 0.9) {
      const [label] = pick(queryAttrs);
      // a live subscription on a current attr, removed again shortly after
      const q = { [NS]: { $: { where: { [label]: pick(queryAttrs.find(([l]) => l === label)[2]) }, order: { [label]: "asc" }, limit: 2 } } };
      subscribe(pick(SESSIONS), q, ["subscription"]);
      tx(pick(SESSIONS), [["add-triple", pick(entities), queryAttrs.find(([l]) => l === label)[1], pick(queryAttrs.find(([l]) => l === label)[2])]], ["data"], "subscription churn");
      unsubscribe(pick(SESSIONS), q, ["subscription"]);
    } else {
      checkpoint("extra");
    }
  }

  // final reads and a final settled state
  initAttrs("ADMIN", "final");
  initAttrs("USER", "final");
  for (const s of SESSIONS) query(s, { [NS]: { owner: {}, tags: {}, profile: {} } }, ["where"]);
  checkpoint("final");
  return { script, attrs, dyn, NS, OWN, TAG, PRO };
}

// The client flattens result join-rows into [e a v] triples; t and row order
// are server-local. Values stay raw (they can be epoch-ms dates this script
// wrote), except strings a server chose (uuids) are normalized.
function projectTriples(result) {
  const triples = [];
  const walk = (nodes) => {
    for (const node of nodes ?? []) {
      for (const rows of node?.data?.["datalog-result"]?.["join-rows"] ?? []) {
        for (const [e, a, v] of rows) {
          triples.push([normalize(e), normalize(a), typeof v === "string" ? normalize(v) : v]);
        }
      }
      walk(node?.["child-nodes"]);
    }
  };
  walk(result);
  const deduped = [...new Map(triples.map((t) => [canon(t), t])).values()];
  deduped.sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
  return deduped;
}

const rootKey = (q) => Object.keys(q ?? {}).find((k) => !k.startsWith("$$"));
const pageInfoOf = (result, ns) => result?.[0]?.data?.["page-info"]?.[ns] ?? null;

// A page cursor is [e, a, v, t]; only the server-local timestamp and chosen
// entity/attr ids normalize. The order value stays raw (it can be an epoch-ms
// date), except under serverCreatedAt where it is the entity id.
function normalizePageInfo(info, orderKey) {
  if (!info) return null;
  const out = {};
  for (const [k, v] of Object.entries(info)) {
    if (k === "start-cursor" || k === "end-cursor") {
      out[k] = Array.isArray(v)
        ? v.map((part, idx) => {
            if (idx === 0 || idx === 1 || idx === 3) return normalize(part);
            if (idx === 2 && orderKey === "serverCreatedAt") return normalize(part);
            return part;
          })
        : normalize(v);
    } else {
      out[k] = normalize(v);
    }
  }
  return out;
}

function projectQueryResult(result, q) {
  const ns = rootKey(q);
  const orderKey = Object.keys(q?.[ns]?.$?.order ?? {})[0] ?? null;
  return {
    triples: projectTriples(result),
    pageInfo: normalizePageInfo(pageInfoOf(result, ns), orderKey),
    aggregate: normalize(result?.[0]?.data?.aggregate ?? null),
  };
}

const errView = (m) => ({
  status: m.status,
  type: m.type,
  message: m.message,
  hint: normalize(m.hint ?? null),
});

let reregistered = 0;

async function runOn(serverName, plan) {
  const { url, appId } = SERVERS[serverName];
  const initMessage = (session) => {
    const msg = {
      "client-event-id": uuid(),
      op: "init",
      "app-id": appId,
      versions: { "@instantdb/core": "v0.21.0" },
    };
    if (session === "ADMIN") msg["__admin-token"] = adminToken;
    return msg;
  };
  const open = async (session, tag = "") => {
    const c = connect(url, appId, `${serverName}:schema:${session}${tag}`);
    await c.open;
    c.takeNewFrames();
    const ceid = c.send(initMessage(session));
    const reply = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
    if (reply.op !== "init-ok") throw new Error(`[${serverName}] init ${session}${tag} failed: ${JSON.stringify(reply).slice(0, 400)}`);
    c.takeNewFrames();
    return { conn: c, reply, ceid };
  };

  const conns = {};
  const outcomes = [];
  const freshCheckpoints = {};
  const state = Object.fromEntries(SESSIONS.map((s) => [s, { attrs: null }]));
  for (const s of SESSIONS) {
    const { conn, reply } = await open(s);
    conns[s] = conn;
    state[s].attrs = projectAttrs(reply.attrs ?? []);
    outcomes.push({ i: -1, kind: "init", session: s, attrs: state[s].attrs });
  }

  // queries used for subscriptions: canonical key -> q
  const subs = Object.fromEntries(SESSIONS.map((s) => [s, new Map()]));
  const subQ = Object.fromEntries(SESSIONS.map((s) => [s, new Map()]));
  const refreshErrors = Object.fromEntries(SESSIONS.map((s) => [s, []]));
  const cursors = Object.fromEntries(SESSIONS.map((s) => [s, conns[s].frames.length]));

  const fold = () => {
    for (const s of SESSIONS) {
      const frames = conns[s].frames.slice(cursors[s]);
      cursors[s] = conns[s].frames.length;
      for (const m of frames) {
        if (m.op === "add-query-ok") {
          const k = canon(normalize(m.q));
          if (subs[s].has(k)) subs[s].set(k, projectQueryResult(m.result, m.q));
        } else if (m.op === "refresh-ok") {
          for (const comp of m.computations ?? []) {
            const k = canon(normalize(comp["instaql-query"]));
            if (subs[s].has(k)) subs[s].set(k, projectQueryResult(comp["instaql-result"], comp["instaql-query"]));
          }
          if (m.attrs) state[s].attrs = projectAttrs(m.attrs);
        } else if (m.op === "error" && m["original-event"]?.op === "refresh") {
          refreshErrors[s].push(errView(m));
        }
      }
    }
  };
  const snapshot = () => {
    fold();
    const out = {};
    for (const s of SESSIONS) {
      out[s] = {
        attrs: state[s].attrs,
        subs: Object.fromEntries([...subs[s].entries()].sort(([a], [b]) => (a < b ? -1 : 1))),
        refreshErrors: refreshErrors[s].splice(0),
      };
    }
    return out;
  };

  // A one-shot query on its own session: the long-lived subscriptions keep
  // folding independently, and a fresh session cannot answer add-query with
  // add-query-exists for a query another session holds.
  const oneShot = async (session, q) => {
    const c = connect(url, appId, `${serverName}:schema:${session}:query`);
    await c.open;
    c.takeNewFrames();
    const ceid = c.send(initMessage(session));
    const init = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
    if (init.op !== "init-ok") {
      c.close();
      return { error: errView(init) };
    }
    const qid = c.send({ "client-event-id": uuid(), op: "add-query", q });
    const reply = await c.waitFor((m) => ["add-query-ok", "error"].includes(m.op) && m["client-event-id"] === qid, 20000);
    c.close();
    if (reply.op === "add-query-ok") {
      return { result: projectQueryResult(reply.result, q), rawInfo: pageInfoOf(reply.result, rootKey(q)) };
    }
    return { error: errView(reply) };
  };

  // A fresh query variant is unique on every probe (the same unique-id
  // predicate every entity satisfies), so legacy's shared datalog cache
  // cannot hand a new connection an answer it cached for an earlier probe.
  const variant = (q) =>
    Object.fromEntries(
      Object.entries(q).map(([k, form]) => {
        if (k.startsWith("$$")) return [k, form];
        const opts = { ...(form?.$ ?? {}) };
        const idCond = { and: [{ id: { $isNull: false } }, { id: { $ne: uuid() } }] };
        opts.where = opts.where ? { and: [opts.where, idCond] } : idCond;
        return [k, { ...form, $: opts }];
      }),
    );
  // One fresh session per checkpoint: its init attrs, then a fresh answer
  // for every held subscription. This is the server's current state, used to
  // distinguish a legacy cache/invalidation lag from a real difference.
  const probeFresh = async (session) => {
    const c = connect(url, appId, `${serverName}:schema:${session}:fresh`);
    await c.open;
    c.takeNewFrames();
    const ceid = c.send(initMessage(session));
    const init = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
    if (init.op !== "init-ok") {
      c.close();
      return { attrs: null, subs: {}, initError: errView(init) };
    }
    const subs = {};
    for (const [key, q] of subQ[session]) {
      const vq = variant(q);
      const qid = c.send({ "client-event-id": uuid(), op: "add-query", q: vq });
      const reply = await c.waitFor(
        (m) => ["add-query-ok", "error"].includes(m.op) && m["client-event-id"] === qid,
        20000,
      );
      if (reply.op === "add-query-ok") subs[key] = projectQueryResult(reply.result, q);
    }
    c.close();
    return { attrs: projectAttrs(init.attrs ?? []), subs };
  };

  // Queries each session holds as far as the script knows. Legacy's refresh
  // can re-register a query removed while it recomputes it (query.clj
  // instaql-query-reactive! -> bump-instaql-version!), and a later add-query
  // of it then answers add-query-exists. That is the same legacy race
  // fuzz-perms handles: remove the stray query and ask again.
  const held = Object.fromEntries(SESSIONS.map((s) => [s, new Set()]));
  const addQuery = async (session, q) => {
    const key = canon(normalize(q));
    const ask = async () => {
      fold();
      subs[session].set(key, null);
      subQ[session].set(key, q);
      const ceid = conns[session].send({ "client-event-id": uuid(), op: "add-query", q });
      const r = await conns[session].waitFor(
        (m) => ["add-query-ok", "add-query-exists", "error"].includes(m.op) && m["client-event-id"] === ceid,
        20000,
      );
      if (r.op === "add-query-ok") {
        subs[session].set(key, projectQueryResult(r.result, q));
        held[session].add(key);
        return { ok: r.op };
      }
      subs[session].delete(key);
      subQ[session].delete(key);
      return r.op === "error" ? { error: errView(r) } : { ok: r.op };
    };
    let r = await ask();
    if (r.ok === "add-query-exists" && serverName === "legacy" && !held[session].has(key)) {
      reregistered++;
      await removeQuery(session, q);
      r = await ask();
    }
    return r;
  };
  const removeQuery = async (session, q) => {
    fold();
    const key = canon(normalize(q));
    subs[session].delete(key);
    subQ[session].delete(key);
    held[session].delete(key);
    const ceid = conns[session].send({ "client-event-id": uuid(), op: "remove-query", q });
    const r = await conns[session].waitFor(
      (m) => ["remove-query-ok", "error"].includes(m.op) && m["client-event-id"] === ceid,
      20000,
    );
    return r.op === "remove-query-ok" ? { ok: true } : { error: errView(r) };
  };

  for (const [i, op] of plan.script.entries()) {
    if (op.kind === "tx") {
      const ceid = conns[op.session].send({ "client-event-id": uuid(), op: "transact", "tx-steps": op.steps });
      const r = await conns[op.session].waitFor(
        (m) => ["transact-ok", "error"].includes(m.op) && m["client-event-id"] === ceid,
        20000,
      );
      outcomes.push({ i, kind: op.kind, session: op.session, label: op.label, ...(r.op === "transact-ok" ? { ok: true } : { error: errView(r) }) });
    } else if (op.kind === "query") {
      const r = await oneShot(op.session, op.q);
      outcomes.push({ i, kind: op.kind, session: op.session, q: op.q, label: op.label, ...(r.error ? { error: r.error } : { result: r.result }) });
    } else if (op.kind === "paginate") {
      const { ns, key, dir, size, where } = op.spec;
      const pages = [];
      let cursor = null;
      for (let pageNo = 0; pageNo < 8; pageNo++) {
        const $ = { order: { [key]: dir }, limit: size };
        if (where) $.where = where;
        if (cursor) $.after = cursor;
        const q = { [ns]: { $ } };
        const r = await oneShot(op.session, q);
        if (r.error) {
          pages.push({ error: r.error });
          break;
        }
        pages.push({ result: r.result });
        const more = r.rawInfo?.["has-next-page?"];
        const next = r.rawInfo?.["end-cursor"];
        if (!more || !next) break;
        cursor = next;
      }
      outcomes.push({ i, kind: op.kind, session: op.session, spec: op.spec, label: op.label, pages });
    } else if (op.kind === "subscribe") {
      const r = await addQuery(op.session, op.q);
      outcomes.push({ i, kind: op.kind, session: op.session, q: op.q, label: op.label, ...r });
    } else if (op.kind === "unsubscribe") {
      const r = await removeQuery(op.session, op.q);
      outcomes.push({ i, kind: op.kind, session: op.session, q: op.q, label: op.label, ...r });
    } else if (op.kind === "init-attrs") {
      const { conn, reply } = await open(op.session, ":init-attrs");
      conn.close();
      outcomes.push({ i, kind: op.kind, session: op.session, label: op.label, attrs: projectAttrs(reply.attrs ?? []) });
    } else if (op.kind === "checkpoint") {
      await settle(Object.values(conns), CHECKPOINT_QUIET_MS);
      const checkpointState = snapshot();
      // A fresh init is the server's current catalog. It lets the comparison
      // tell a legacy session that has not received its refresh yet apart
      // from a real attr difference (the same fresh-probe rule fuzz-perms
      // uses for stale query results).
      const fresh = {};
      for (const s of SESSIONS) fresh[s] = await probeFresh(s);
      freshCheckpoints[i] = fresh;
      outcomes.push({ i, kind: op.kind, label: op.label, state: checkpointState });
    }
  }
  await settle(Object.values(conns), CHECKPOINT_QUIET_MS);
  outcomes.push({ i: plan.script.length, kind: "final-state", state: snapshot() });
  const finalFresh = {};
  for (const s of SESSIONS) finalFresh[s] = await probeFresh(s);
  freshCheckpoints[plan.script.length] = finalFresh;
  for (const c of Object.values(conns)) c.close();
  return { outcomes, freshCheckpoints };
}

const plan = buildScript();
console.log(`fuzz-schema: seed=${seed} rounds=${rounds} ops=${plan.script.length}`);
console.log("running against legacy…");
const legacy = await runOn("legacy", plan);
console.log("running against rust…");
const rust = await runOn("rust", plan);

const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), { prefixes: DIVERGENCE_PREFIXES.fuzzschema });
const slug = (s) => String(s ?? "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
const outcomePath = (op, fallbackIndex) => {
  if (!op) return `fuzzschema:final/${fallbackIndex}`;
  // checkpoints are named by their scenario label so a documented exception
  // does not depend on an op index that later script edits shift
  if (op.kind === "checkpoint") return `fuzzschema:checkpoint/${slug(op.label) || fallbackIndex}`;
  return `fuzzschema:${op.kind}/${op.i ?? fallbackIndex}${op.session ? `/${op.session}` : ""}`;
};
// Legacy's attr/query caches can keep a session's folded state behind the
// database until a later invalidation. Accept only when every differing part
// is stale on the legacy side and a fresh legacy probe equals both what
// rust's session holds and what rust's own fresh probe sees. A stale rust
// session, a different refresh-error list or any other field still fails.
const same = (a, b) => a !== undefined && b !== undefined && canon(a) === canon(b);
function legacyStaleState(l, r) {
  if (!l || !r || l.i !== r.i) return false;
  if (!["checkpoint", "final-state"].includes(l.kind) || l.kind !== r.kind) return false;
  const lf = legacy.freshCheckpoints[l.i];
  const rf = rust.freshCheckpoints[r.i];
  let stale = false;
  for (const s of SESSIONS) {
    const la = l.state?.[s]?.attrs;
    const ra = r.state?.[s]?.attrs;
    if (canon(la) !== canon(ra)) {
      stale = true;
      if (!same(lf?.[s]?.attrs, ra) || !same(rf?.[s]?.attrs, ra)) return false;
    }
    const ls = l.state?.[s]?.subs ?? {};
    const rs = r.state?.[s]?.subs ?? {};
    for (const key of new Set([...Object.keys(ls), ...Object.keys(rs)])) {
      if (canon(ls[key]) === canon(rs[key])) continue;
      stale = true;
      if (!same(lf?.[s]?.subs?.[key], rs[key]) || !same(rf?.[s]?.subs?.[key], rs[key])) return false;
    }
    if (canon(l.state?.[s]?.refreshErrors) !== canon(r.state?.[s]?.refreshErrors)) return false;
  }
  return stale;
}
let staleLegacy = 0;

// An update-attr unique? flip rewrites av for every triple of the attr in one
// UPDATE. With several already-duplicated values, Postgres reports whichever
// duplicate row it reaches first, and that order changes between runs on both
// servers (seed 34's body/label and seed 36/41's dynIdx picks vary). The
// winning value is not a client contract; keep the envelope and the attr
// identity, mask only the chosen value.
const isUniqueFlagFlip = (op) =>
  op?.kind === "tx" &&
  (op.steps ?? []).some(([kind, value]) => kind === "update-attr" && value?.["unique?"] === true);
const multiConflictUniqueView = (op, o) => {
  if (!isUniqueFlagFlip(op)) return o;
  const e = o?.error;
  if (e?.type !== "record-not-unique" || e.hint?.["record-type"] !== "triple") return o;
  return {
    ...o,
    error: {
      ...e,
      message: "<conflicting unique value>",
      hint: { ...e.hint, value: "<conflicting unique value>" },
    },
  };
};
let multiConflict = 0;

let allowed = 0;
let mismatches = 0;
const n = Math.max(legacy.outcomes.length, rust.outcomes.length);
for (let k = 0; k < n; k++) {
  const l = legacy.outcomes[k];
  const r = rust.outcomes[k];
  if (canon(l) === canon(r)) continue;
  const op = plan.script[l?.i ?? r?.i];
  const p = outcomePath(op, l?.i ?? r?.i);
  if (legacyStaleState(l, r)) {
    staleLegacy++;
    console.log(`legacy session state was stale at ${p}; a fresh legacy probe agreed with rust`);
    continue;
  }
  const verdict = allowlist.check(p, l, r);
  if (verdict.entry) {
    allowed++;
    console.log(`[ALLOWED] ${p}: ${verdict.differed.join(", ") || "(whole value)"}`);
    continue;
  }
  if (canon(multiConflictUniqueView(op, l)) === canon(multiConflictUniqueView(op, r))) {
    multiConflict++;
    console.log(
      `${p}: legacy and rust chose different conflicting unique values; compared type/attr-id/etype/label only`,
    );
    continue;
  }
  mismatches++;
  if (mismatches > 12) continue;
  console.error(`mismatch at ${p} (${op?.label ?? l?.kind ?? r?.kind}):`);
  const first = firstDifference(l, r);
  if (first) console.error(`  first differing sub-path: ${first.p || "(whole value)"} legacy=${JSON.stringify(first.a)?.slice(0, 800)} rust=${JSON.stringify(first.b)?.slice(0, 800)}`);
  console.error(`  legacy: ${JSON.stringify(l)?.slice(0, 3000)}`);
  console.error(`  rust:   ${JSON.stringify(r)?.slice(0, 3000)}`);
  for (const line of describeAllowVerdict(verdict)) console.error(line);
}

// A script with no successful schema mutations, or whose reads never return
// data, compares nothing meaningful: require the grammar's core paths to
// have committed on both servers and its queries to have returned rows.
const txTotal = plan.script.filter((op) => op.kind === "tx").length;
const txOk = (side) => side.outcomes.filter((o) => o.kind === "tx" && o.ok).length;
const txOkLegacy = txOk(legacy);
const txOkRust = txOk(rust);
const queryWithData = (side) =>
  side.outcomes.filter((o) => {
    if (o.kind === "query") return (o.result?.triples?.length ?? 0) > 0;
    if (o.kind === "paginate") return (o.pages ?? []).some((p) => (p.result?.triples?.length ?? 0) > 0);
    return false;
  }).length;
const dataLegacy = queryWithData(legacy);
const dataRust = queryWithData(rust);

const featureFloor = {};
for (const op of plan.script) {
  if (op.kind !== "tx") continue;
  for (const f of op.features ?? []) (featureFloor[f] ??= { tx: 0, okLegacy: 0, okRust: 0 }).tx += 1;
}
for (const side of ["legacy", "rust"]) {
  for (const o of (side === "legacy" ? legacy : rust).outcomes) {
    if (o.kind !== "tx" || !o.ok) continue;
    const op = plan.script[o.i];
    for (const f of op?.features ?? []) featureFloor[f][side === "legacy" ? "okLegacy" : "okRust"] += 1;
  }
}
const needed = ["add-attr", "update-attr", "delete-attr", "restore-attr", "rename", "unique", "index", "checked-type", "required", "data"];
let floorOk = txOkLegacy > txTotal / 2 && txOkRust > txTotal / 2 && dataLegacy > 0 && dataRust > 0;
for (const f of needed) {
  const c = featureFloor[f];
  if (!c || !c.okLegacy || !c.okRust) {
    console.error(`FUZZ-SCHEMA FLOOR NOT MET for ${f}: ${JSON.stringify(c ?? null)}`);
    floorOk = false;
  }
}
console.log(`grammar floor: ${JSON.stringify(featureFloor)}`);
console.log(`legacy: ${txOkLegacy}/${txTotal} transacts committed, ${dataLegacy} queries returned data; rust: ${txOkRust}/${txTotal}, ${dataRust}`);
const stale = allowlist.reportStale();
if (mismatches || stale || !floorOk) {
  console.error(`FUZZ-SCHEMA FAILED: ${mismatches} mismatches, ${stale} stale allowlist entries (seed ${seed})`);
  process.exit(1);
}
console.log(`FUZZ-SCHEMA PASSED: ${plan.script.length} ops, ${legacy.outcomes.filter((o) => o.kind === "checkpoint").length} checkpoints, ${allowed} allowed differences, ${multiConflict} multi-conflict unique values, ${reregistered} legacy re-registered removed queries, ${staleLegacy} stale legacy snapshots (seed ${seed})`);
