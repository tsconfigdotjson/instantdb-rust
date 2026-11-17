// Seeded sync-table fuzz layer of the differential harness.
//
// SyncTable.ts drives `start-sync` over one namespace (legacy only supports
// `$.order`, not where/link forms), then applies `sync-load-batch` rows and
// `sync-update-triples` change sets, persists a tx-id watermark and resumes
// with `resync-table` after a reconnect. No other layer generates that
// protocol beyond a few fixed replay steps. This one does:
//   - start-sync over every supported order (serverCreatedAt asc/desc, typed
//     number / date / string / boolean attrs in both directions), plus the
//     unsupported shapes (where, links, extra options, unknown / unindexed
//     order fields, multiple namespaces, null q, non-admin) compared by error;
//   - random creates / updates / retracts / deletes / link + unlink from
//     several sessions (admin and signed-in) interleaved with the sync;
//   - a view rule that hides some rows, to pin that admin sync tables bypass
//     view filtering like legacy (and non-admin start-sync stays denied);
//   - remove-sync (with and without keep-subscription), reconnect + resync
//     from the last tx the client saw, and a schema attr added after a sub
//     started (the sub's topic attrs stay frozen until a new start-sync,
//     including across resync, as legacy persists them in sync_sub_topics);
//   - a burst of >100 transactions, so sync-update-triples batching and the
//     tx-id watermark catch-up are exercised; frames must never carry more
//     than legacy's 100-tx cap.
//
// Every sync frame is folded the way SyncTable.ts reads it (load rows build
// the store, update txes are skipped at or below the watermark, grouped by
// entity, applied as added/removed triples, removed entities drop out), and
// both the normalized frame sequence and the folded store/entity projection
// are compared byte-for-byte. Only server-chosen ids, timestamps, tx ids and
// trace ids are normalized. Every transaction touching a synced namespace is
// awaited to its sync-update before the next op, so legacy's invalidator can
// not coalesce separate fuzz transactions into a different frame grouping.
//
// Usage: node fuzz-sync.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL, LEGACY_DATABASE_URL, RUST_DATABASE_URL

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
  psql,
  settle,
} from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
const seed = Number(process.argv[5] ?? 301);
const rounds = Number(process.argv[6] ?? 60);
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node fuzz-sync.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");

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

const DOCS = "syncDocs";
const NOTES = "syncNotes";
const OWNERS = "syncOwners";
const ROLES = ["S1", "S2", "M", "U"];
const SYNC_ROLES = ["S1", "S2"];
const QUIET_MS = 180;

function prng(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
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

const refAttr = (id, ns, label, revId, revNs, revLabel) => [
  "add-attr",
  {
    id,
    "forward-identity": [id, ns, label],
    "reverse-identity": [revId, revNs, revLabel],
    "value-type": "ref",
    cardinality: "one",
    "unique?": false,
    "index?": false,
    isUnsynced: true,
  },
];

const manyAttr = (id, ns, label) => [
  "add-attr",
  {
    id,
    "forward-identity": [id, ns, label],
    "value-type": "blob",
    cardinality: "many",
    "unique?": false,
    "index?": false,
    isUnsynced: true,
  },
];

function buildPlan() {
  const rand = prng(seed * 104729 + 97);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const mk = makeIdFactory(appIdLegacy);

  const attrs = {
    docsId: mk(),
    title: mk(),
    body: mk(),
    rank: mk(),
    due: mk(),
    flag: mk(),
    label: mk(),
    owner: mk(),
    ownerRev: mk(),
    tags: mk(),
    notesId: mk(),
    noteBody: mk(),
    noteRank: mk(),
    noteDue: mk(),
    ownersId: mk(),
    ownerName: mk(),
    dyn: mk(),
  };
  const attrDefs = {
    [attrs.docsId]: { etype: DOCS, label: "id", type: "string" },
    [attrs.title]: { etype: DOCS, label: "title", type: "string" },
    [attrs.body]: { etype: DOCS, label: "body", type: "json" },
    [attrs.rank]: { etype: DOCS, label: "rank", type: "number" },
    [attrs.due]: { etype: DOCS, label: "due", type: "date" },
    [attrs.flag]: { etype: DOCS, label: "flag", type: "boolean" },
    [attrs.label]: { etype: DOCS, label: "label", type: "string" },
    [attrs.owner]: { etype: DOCS, label: "owner", type: "string" },
    [attrs.tags]: { etype: DOCS, label: "tags", type: "json" },
    [attrs.notesId]: { etype: NOTES, label: "id", type: "string" },
    [attrs.noteBody]: { etype: NOTES, label: "body", type: "string" },
    [attrs.noteRank]: { etype: NOTES, label: "rank", type: "number" },
    [attrs.noteDue]: { etype: NOTES, label: "due", type: "date" },
    [attrs.ownersId]: { etype: OWNERS, label: "id", type: "string" },
    [attrs.ownerName]: { etype: OWNERS, label: "name", type: "string" },
    [attrs.dyn]: { etype: DOCS, label: "dyn", type: "boolean" },
  };
  const labelByAttr = Object.fromEntries(Object.entries(attrDefs).map(([id, d]) => [id, d.label]));
  const attrByLabel = (etype) =>
    Object.fromEntries(
      Object.entries(attrDefs)
        .filter(([, d]) => d.etype === etype)
        .map(([id, d]) => [d.label, id]),
    );
  const byLabel = { [DOCS]: attrByLabel(DOCS), [NOTES]: attrByLabel(NOTES), [OWNERS]: attrByLabel(OWNERS) };

  const schemaSteps = [
    attr(attrs.docsId, DOCS, "id", { "unique?": true, "index?": true }),
    attr(attrs.title, DOCS, "title"),
    attr(attrs.body, DOCS, "body"),
    attr(attrs.rank, DOCS, "rank", { "index?": true, "checked-data-type": "number" }),
    attr(attrs.due, DOCS, "due", { "index?": true, "checked-data-type": "date" }),
    attr(attrs.flag, DOCS, "flag", { "index?": true, "checked-data-type": "boolean" }),
    attr(attrs.label, DOCS, "label", { "index?": true, "checked-data-type": "string" }),
    refAttr(attrs.owner, DOCS, "owner", attrs.ownerRev, OWNERS, "docs"),
    manyAttr(attrs.tags, DOCS, "tags"),
    attr(attrs.notesId, NOTES, "id", { "unique?": true, "index?": true }),
    attr(attrs.noteBody, NOTES, "body"),
    attr(attrs.noteRank, NOTES, "rank", { "index?": true, "checked-data-type": "number" }),
    attr(attrs.noteDue, NOTES, "due", { "index?": true, "checked-data-type": "date" }),
    attr(attrs.ownersId, OWNERS, "id", { "unique?": true, "index?": true }),
    attr(attrs.ownerName, OWNERS, "name"),
  ];

  const docIds = Array.from({ length: 9 }, () => mk());
  const noteIds = Array.from({ length: 5 }, () => mk());
  const ownerIds = Array.from({ length: 3 }, () => mk());
  const titles = ["alpha", "beta", "hidden", "delta", "Echo", "foxtrot", "gamma", "Hotel", "iota"];
  const rankValues = [0, 1, 1, 2.5, 7, -3, 100, 42, 9];
  const dueValues = [
    "2024-01-01T00:00:00Z",
    "2024-06-15T12:00:00Z",
    "2023-12-31T23:59:59Z",
    "2025-02-01T00:00:00Z",
    0,
    1704067200000,
    "2022-05-05T05:05:05Z",
    "2026-01-01T00:00:00Z",
    "2024-02-29T00:00:00Z",
  ];
  const labelValues = ["aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii"];
  const docsState = new Map();
  const notesState = new Map();
  const ownerState = new Map();
  const seedSteps = [];
  for (const [i, eid] of docIds.entries()) {
    const fields = {
      title: titles[i],
      body: i % 3 === 0 ? { i, nested: { n: i } } : `body-${i}`,
      rank: rankValues[i],
      due: dueValues[i],
      flag: i % 2 === 0,
      label: labelValues[i],
      owner: ownerIds[i % ownerIds.length],
    };
    if (i === 4) delete fields.title;
    if (i === 6) delete fields.due;
    if (i === 8) delete fields.label;
    docsState.set(eid, fields);
    seedSteps.push(["add-triple", eid, attrs.docsId, eid]);
    for (const [label, value] of Object.entries(fields)) {
      const key = { title: "title", body: "body", rank: "rank", due: "due", flag: "flag", label: "label", owner: "owner" }[label];
      seedSteps.push(["add-triple", eid, attrs[key], value]);
    }
    seedSteps.push(["add-triple", eid, attrs.tags, `tag-${i}`]);
  }
  for (const [i, eid] of ownerIds.entries()) {
    const fields = { name: `owner-${i}` };
    ownerState.set(eid, fields);
    seedSteps.push(["add-triple", eid, attrs.ownersId, eid]);
    seedSteps.push(["add-triple", eid, attrs.ownerName, fields.name]);
  }
  for (const [i, eid] of noteIds.entries()) {
    const fields = {
      body: `note-${i}`,
      rank: [3, -1, 4, 4, 99][i],
      due: ["2024-03-01T00:00:00Z", "2024-04-01T00:00:00Z", "2024-05-01T00:00:00Z", "2024-06-01T00:00:00Z", "2024-07-01T00:00:00Z"][i],
    };
    notesState.set(eid, fields);
    seedSteps.push(["add-triple", eid, attrs.notesId, eid]);
    for (const [label, value] of Object.entries(fields)) {
      const key = { body: "noteBody", rank: "noteRank", due: "noteDue" }[label];
      seedSteps.push(["add-triple", eid, attrs[key], value]);
    }
  }

  const script = [];
  let seq = 0;
  const add = (op) => {
    op.name = op.name || `${op.kind}-${++seq}`;
    script.push(op);
    return op;
  };
  const tx = (session, steps, etypes, sync = true) =>
    add({ kind: "tx", session, steps, etypes, sync });
  const checkpoint = (label) => add({ kind: "checkpoint", name: label });
  const active = { S1: [], S2: [] };
  const subByLabel = new Map();
  const start = (session, subName, q) => {
    add({ kind: "start", session, subName, q });
    if (!active[session].includes(subName)) active[session].push(subName);
    subByLabel.set(subName, { q });
  };
  const remove = (session, subName, keep) => {
    add({ kind: "remove", session, subName, keep });
    active[session] = active[session].filter((x) => x !== subName);
  };
  const reconnect = (session) => add({ kind: "reconnect", session, subs: [...active[session]] });
  const resync = (session, subName) => {
    add({ kind: "resync", session, subName });
    if (!active[session].includes(subName)) active[session].push(subName);
  };
  const burst = (items) => add({ kind: "burst", items });
  const stateCheckpoint = (label) => add({ kind: "state-checkpoint", name: label });

  // Schema and data are in place before any sync starts (the layer's later
  // dynamic attr exercises the frozen topic set).
  tx("M", schemaSteps, [], false).name = "seed-schema";
  tx("M", seedSteps, [DOCS, NOTES, OWNERS], false).name = "seed-data";

  start("S1", "s1-docs-default", { [DOCS]: {} });
  start("S2", "s2-docs-rank-desc", { [DOCS]: { $: { order: { rank: "desc" } } } });
  start("S1", "s1-docs-label-asc", { [DOCS]: { $: { order: { label: "asc" } } } });
  start("S2", "s2-notes-default", { [NOTES]: {} });
  checkpoint("after-starts");

  // remove-sync + resync of the same persisted subscription id: legacy keeps
  // the sync_subs row (remove-sync's delete names a missing attr), so a later
  // resync from the last seen tx still replays the backlog.
  remove("S1", "s1-docs-label-asc", true);
  tx(
    "M",
    [
      ["add-triple", docIds[1], attrs.title, "after-remove"],
      ["add-triple", docIds[1], attrs.rank, 55],
    ],
    [DOCS],
  );
  checkpoint("after-removed-tx");
  resync("S1", "s1-docs-label-asc");
  checkpoint("after-resync-removed");

  // Add an indexed typed attr after the subs started, write it in the same tx,
  // then write it again. Legacy's persisted topics do not include it, so
  // neither the live subs nor a resync from the old watermark may deliver it.
  tx(
    "M",
    [
      ["add-attr", {
        id: attrs.dyn,
        "forward-identity": [attrs.dyn, DOCS, "dyn"],
        "value-type": "blob",
        cardinality: "one",
        "unique?": false,
        "index?": true,
        "checked-data-type": "boolean",
        isUnsynced: true,
      }],
      ["add-triple", docIds[2], attrs.dyn, true],
    ],
    [DOCS],
    false,
  ).name = "add-dyn-attr";
  tx("M", [["add-triple", docIds[2], attrs.dyn, false]], [DOCS], false).name = "write-dyn-attr";
  reconnect("S1");
  reconnect("S2");
  checkpoint("after-dyn-resync");

  const validQueries = [
    { q: { [DOCS]: {} }, label: "docs-default" },
    { q: { [DOCS]: { $: { order: { serverCreatedAt: "desc" } } } }, label: "docs-created-desc" },
    { q: { [DOCS]: { $: { order: { rank: "asc" } } } }, label: "docs-rank-asc" },
    { q: { [DOCS]: { $: { order: { rank: "desc" } } } }, label: "docs-rank-desc" },
    { q: { [DOCS]: { $: { order: { due: "asc" } } } }, label: "docs-due-asc" },
    { q: { [DOCS]: { $: { order: { due: "desc" } } } }, label: "docs-due-desc" },
    { q: { [DOCS]: { $: { order: { label: "asc" } } } }, label: "docs-label-asc" },
    { q: { [DOCS]: { $: { order: { label: "desc" } } } }, label: "docs-label-desc" },
    { q: { [DOCS]: { $: { order: { flag: "asc" } } } }, label: "docs-flag-asc" },
    { q: { [NOTES]: {} }, label: "notes-default" },
    { q: { [NOTES]: { $: { order: { rank: "asc" } } } }, label: "notes-rank-asc" },
    { q: { [NOTES]: { $: { order: { due: "desc" } } } }, label: "notes-due-desc" },
  ];
  const invalidQueries = [
    { q: null, label: "null-q" },
    { q: { [DOCS]: { $: { where: { rank: { $gt: 0 } } } } }, label: "where-unsupported" },
    { q: { [DOCS]: { $: { limit: 1 } } }, label: "limit-unsupported" },
    { q: { [DOCS]: { tags: {} } }, label: "link-unsupported" },
    { q: { [DOCS]: {}, [NOTES]: {} }, label: "two-namespaces" },
    { q: { missingTable: {} }, label: "unknown-namespace" },
    { q: { [DOCS]: { $: { order: { title: "asc" } } } }, label: "unindexed-order" },
    { q: { [DOCS]: { $: { order: { nope: "asc" } } } }, label: "unknown-order" },
    { q: { [DOCS]: { $: { order: { rank: "sideways" } } } }, label: "bad-direction" },
  ];

  const docValues = {
    title: () => pick(["one", "two", "three", "Zed", "alpha", "hidden"]),
    body: () => (chance(0.5) ? { n: Math.floor(rand() * 100) } : `body-${Math.floor(rand() * 100)}`),
    rank: () => pick([...rankValues, 11, -9, 3.25]),
    due: () => pick(dueValues),
    flag: () => chance(0.5),
    label: () => pick(labelValues),
    owner: () => pick(ownerIds),
  };
  const noteValues = {
    body: () => `note-${Math.floor(rand() * 100)}`,
    rank: () => pick([...rankValues, 5, 6]),
    due: () => pick(dueValues),
  };
  const attrsFor = (etype) =>
    etype === DOCS
      ? ["title", "body", "rank", "due", "flag", "label"]
      : ["body", "rank", "due"];

  const stepFor = (etype, eid, fields) => {
    const ids = byLabel[etype];
    if (Object.keys(fields).length === 0) {
      const label = etype === DOCS ? "title" : "body";
      const value = docValuesOrNote(etype, label)();
      fields[label] = value;
      return [["add-triple", eid, ids[label], value]];
    }
    const op = chance(0.75) ? "add-triple" : "retract-triple";
    const label = pick(Object.keys(fields));
    const value = op === "retract-triple" ? fields[label] : docValuesOrNote(etype, label)();
    const steps = [[op, eid, ids[label], value]];
    if (op === "add-triple") fields[label] = value;
    else delete fields[label];
    return steps;
  };
  const docValuesOrNote = (etype, label) => (etype === DOCS ? docValues[label] : noteValues[label]);
  const randomDocTx = () => {
    const mode = rand();
    if (mode < 0.24 || docsState.size < 3) {
      const eid = mk();
      const fields = { title: docValues.title(), rank: docValues.rank(), due: docValues.due() };
      if (chance(0.7)) fields.flag = docValues.flag();
      if (chance(0.6)) fields.label = docValues.label();
      const steps = [["add-triple", eid, attrs.docsId, eid]];
      for (const [label, value] of Object.entries(fields)) steps.push(["add-triple", eid, attrs[label], value]);
      if (chance(0.6)) steps.push(["add-triple", eid, attrs.owner, pick(ownerIds)]);
      docsState.set(eid, fields);
      return { steps, etypes: [DOCS] };
    }
    const eid = pick([...docsState.keys()]);
    const fields = docsState.get(eid);
    if (mode < 0.36) {
      docsState.delete(eid);
      return { steps: [["delete-entity", eid, DOCS]], etypes: [DOCS] };
    }
    if (mode < 0.52) {
      const steps = [];
      if (Object.keys(fields).length === 0) steps.push(["add-triple", eid, attrs.docsId, eid]);
      for (let i = 0; i < 1 + Math.floor(rand() * 3); i++) {
        steps.push(...stepFor(DOCS, eid, fields));
      }
      if (chance(0.25)) steps.push(["add-triple", eid, attrs.owner, pick(ownerIds)]);
      return { steps, etypes: [DOCS] };
    }
    if (mode < 0.68) {
      return {
        steps: [
          ["add-triple", eid, attrs.title, docValues.title()],
          ["add-triple", eid, attrs.rank, docValues.rank()],
          ...(fields.due === undefined ? [] : [["retract-triple", eid, attrs.due, fields.due]]),
        ],
        etypes: [DOCS],
      };
    }
    if (mode < 0.82) {
      const linked = chance(0.5);
      const cleanup = fields.owner ? [["retract-triple", eid, attrs.owner, fields.owner]] : [];
      if (linked) fields.owner = pick(ownerIds);
      else delete fields.owner;
      return {
        steps: [
          ["add-triple", eid, attrs.title, docValues.title()],
          ...cleanup,
          ...(linked ? [["add-triple", eid, attrs.owner, fields.owner]] : []),
        ],
        etypes: [DOCS],
      };
    }
    if (mode < 0.91) {
      return {
        steps: [
          ["add-triple", eid, attrs.title, docValues.title()],
          ["add-triple", eid, attrs.tags, `tag-${Math.floor(rand() * 8)}`],
          ["retract-triple", eid, attrs.rank, fields.rank ?? 0],
        ],
        etypes: [DOCS],
      };
    }
    return {
      steps: [["add-triple", eid, attrs.rank, pick(["not-a-number", true])]],
      etypes: [],
      sync: false,
    };
  };
  const randomNoteTx = () => {
    if (chance(0.3) || notesState.size < 2) {
      const eid = mk();
      const fields = { body: noteValues.body(), rank: noteValues.rank(), due: noteValues.due() };
      notesState.set(eid, fields);
      const steps = [["add-triple", eid, attrs.notesId, eid]];
      for (const [label, value] of Object.entries(fields)) {
        steps.push(["add-triple", eid, attrs[{ body: "noteBody", rank: "noteRank", due: "noteDue" }[label]], value]);
      }
      return { steps, etypes: [NOTES] };
    }
    const eid = pick([...notesState.keys()]);
    if (chance(0.15)) {
      notesState.delete(eid);
      return { steps: [["delete-entity", eid, NOTES]], etypes: [NOTES] };
    }
    const fields = notesState.get(eid);
    return { steps: stepFor(NOTES, eid, fields), etypes: [NOTES] };
  };

  for (let i = 0; i < rounds; i++) {
    const r = rand();
    if (r < 0.44) {
      const etypes = chance(0.72) ? [DOCS] : [NOTES];
      const session = pick(["M", "M", "M", "U"]);
      const picked = etypes[0] === DOCS ? randomDocTx() : randomNoteTx();
      tx(session, picked.steps, picked.etypes, picked.sync !== false);
    } else if (r < 0.62) {
      const v = pick(validQueries);
      if (i > 0 && i % 9 === 0 && chance(0.5)) {
        start(pick(["S1", "S2"]), `dyn-${i}`, { [DOCS]: { $: { order: { dyn: chance(0.5) ? "asc" : "desc" } } } });
      } else {
        start(pick(["S1", "S2"]), `q-${i}-${v.label}`, v.q);
      }
    } else if (r < 0.71) {
      const bad = pick(invalidQueries);
      add({ kind: "start", session: chance(0.5) ? "S1" : "S2", subName: `bad-${i}`, q: bad.q, expectError: true, label: bad.label });
    } else if (r < 0.77) {
      const bad = pick(invalidQueries);
      add({ kind: "start", session: "U", subName: `nonadmin-${i}`, q: bad.q, expectError: true, label: `nonadmin-${bad.label}` });
    } else if (r < 0.85) {
      const session = pick(SYNC_ROLES);
      if (active[session].length) {
        const victim = pick(active[session]);
        remove(session, victim, chance(0.5));
      } else {
        checkpoint(`no-active-${i}`);
      }
    } else if (r < 0.93) {
      const session = pick(SYNC_ROLES);
      if (active[session].length) reconnect(session);
      else checkpoint(`no-active-reconnect-${i}`);
    } else {
      checkpoint(`checkpoint-${i}`);
    }
    if (i % 11 === 10) checkpoint(`periodic-${i}`);
  }

  // Burst: >100 transactions without waiting for sync frames in between, so
  // the change log has a backlog. The final folded state must catch up to the
  // last write, and no sync-update frame may exceed legacy's 100-tx batch cap.
  const burstDoc = mk();
  docsState.set(burstDoc, { title: "burst-0", rank: 0 });
  tx("M", [
    ["add-triple", burstDoc, attrs.docsId, burstDoc],
    ["add-triple", burstDoc, attrs.title, "burst-0"],
    ["add-triple", burstDoc, attrs.rank, 0],
  ], [DOCS]);
  const burstItems = [];
  for (let i = 1; i <= 130; i++) {
    burstItems.push({
      session: "M",
      steps: [
        ["add-triple", burstDoc, attrs.title, `burst-${i}`],
        ["add-triple", burstDoc, attrs.rank, i],
      ],
      etypes: [DOCS],
    });
  }
  burst(burstItems);
  stateCheckpoint("after-burst");
  checkpoint("final-checkpoint");
  return {
    rules,
    script,
    attrDefs,
    labelByAttr,
    byLabel,
    user: { id: mk(), email: `sync-fuzz-${seed}@fuzz.example` },
  };
}

// A view rule hides some rows from client queries; admin sync tables bypass
// rules on both servers. create/update/delete stay allowed for the signed-in
// fuzzer.
const rules = {
  [DOCS]: {
    allow: {
      view: "data.title != 'hidden'",
      create: "true",
      update: "true",
      delete: "true",
    },
  },
};

// ---------------------------------------------------------------------------
// client-visible projection

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;

function norm(value, key = null) {
  if (typeof value === "string") {
    return value.replace(UUID_RE, (m) => (m.includes("-0000-4000-8000-") ? m : "<uuid>"));
  }
  if (typeof value === "number") {
    if (key === "tx-id" || key === "txId" || key === "processed-tx-id") return "<tx>";
    if (key === "created_at" || key === "createdAt") return "<ts>";
    return value > 1e12 ? "<ts>" : value;
  }
  if (Array.isArray(value)) return value.map((v) => norm(v));
  if (value && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (k === "trace-id") continue;
      out[k] = norm(v, k);
    }
    return out;
  }
  return value;
}

const typeName = (v) => (v === null ? "null" : Array.isArray(v) ? "array" : typeof v);

function errorView(m) {
  const oe = m["original-event"] ?? {};
  return {
    op: "error",
    status: m.status,
    type: m.type,
    message: m.message,
    "original-event": norm(
      Object.fromEntries(
        ["op", "q", "subscription-id", "stream-id", "tx-steps", "client-event-id"]
          .filter((k) => k in oe)
          .map((k) => [k, oe[k]]),
      ),
    ),
    hint: m.hint
      ? norm(
          Object.fromEntries(
            ["data-type", "record-type"].filter((k) => k in m.hint).map((k) => [k, m.hint[k]]),
          ),
        )
      : null,
  };
}

function frameView(m) {
  return m.op === "error" ? errorView(m) : norm(m);
}

function replyView(m) {
  return m.op === "error" ? errorView(m) : norm(m);
}

function orderOf(q) {
  const etype = Object.keys(q || {}).find((k) => k !== "$$ruleParams");
  const order = q?.[etype]?.$?.order;
  if (!order) return { etype, key: "serverCreatedAt", dir: "asc" };
  const [key, dir] = Object.entries(order)[0] ?? ["serverCreatedAt", "asc"];
  return { etype, key: key || "serverCreatedAt", dir: dir === "desc" ? "desc" : "asc" };
}

function hasTriple(store, eid) {
  return store.some((t) => t[0] === eid);
}

function applyChange(triples, change) {
  const t = change.triple;
  const key = canon(t);
  if (change.action === "added") {
    if (!triples.some((x) => canon(x) === key)) triples.push(t);
  } else {
    const i = triples.findIndex((x) => canon(x) === key);
    if (i >= 0) triples.splice(i, 1);
  }
}

function idOf(store) {
  for (const t of store) if (t[1] && t[2] === t[0]) return t[3];
  return undefined;
}

function valueFor(store, attrId) {
  const t = store.find((x) => x[1] === attrId);
  return t ? t[2] : undefined;
}

function compareOrder(idA, vA, idB, vB, dataType) {
  if (vA === vB || (vA == null && vB == null)) return idA < idB ? -1 : idA > idB ? 1 : 0;
  if (vB == null) return 1;
  if (vA == null) return -1;
  if (dataType === "string") return String(vA).localeCompare(String(vB), "en-US");
  return vA > vB ? 1 : -1;
}

function sortEntities(model) {
  const attrId = model.order.key === "serverCreatedAt" ? null : model.byLabel[model.order.key];
  const type = attrId ? model.attrDefs[attrId]?.type : "number";
  const dir = model.order.dir === "desc" ? -1 : 1;
  model.entities.sort((a, b) => {
    const va = attrId ? valueFor(a.triples, attrId) : a.serverCreatedAt;
    const vb = attrId ? valueFor(b.triples, attrId) : b.serverCreatedAt;
    const c = compareOrder(a.eid, va, b.eid, vb, type);
    return dir * c;
  });
}

function projectModel(model) {
  const entities = model.entities.map((e) => {
    const triples = e.triples.map((t) => norm(t)).sort((a, b) => (canon(a) < canon(b) ? -1 : 1));
    const entity = {};
    for (const t of e.triples) {
      const label = model.labelByAttr[t[1]] ?? t[1];
      entity[label] = norm(t[2]);
    }
    return {
      eid: e.eid,
      serverCreatedAt: e.serverCreatedAt == null ? null : "<ts>",
      triples,
      entity,
    };
  });
  return {
    q: norm(model.q),
    order: model.order,
    txId: model.txId == null ? null : "<tx>",
    entities,
  };
}

// ---------------------------------------------------------------------------
// one run of the plan against one server

async function mintUser(url, appId, user) {
  const res = await fetch(`${url}/admin/refresh_tokens`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "app-id": appId,
      authorization: `Bearer ${adminToken}`,
    },
    body: JSON.stringify({ id: user.id, email: user.email }),
  });
  const body = await res.json().catch(() => null);
  if (!res.ok || body?.user?.id !== user.id) {
    throw new Error(`mintUser failed: ${res.status} ${JSON.stringify(body).slice(0, 400)}`);
  }
  return body.user;
}

async function runOn(serverName, plan) {
  const { url, db, appId } = SERVERS[serverName];
  psql(
    db,
    `INSERT INTO rules (app_id, code) VALUES ('${appId}', $rules$${JSON.stringify(plan.rules)}$rules$::jsonb)
     ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`,
  );
  const user = await mintUser(url, appId, plan.user);

  const conns = {};
  const models = new Map();
  const subByName = new Map();
  const committed = [];
  const outcomes = [];
  let maxTxPerFrame = 0;

  const openRole = async (role) => {
    const c = connect(url, appId, `${serverName}:sync:${role}`);
    await c.open;
    const init = {
      op: "init",
      "app-id": appId,
      versions: { "@instantdb/core": "v0.22.76" },
    };
    if (role === "U") init["refresh-token"] = user.refresh_token;
    else init["__admin-token"] = adminToken;
    c.send(init);
    const ok = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
    if (ok.op !== "init-ok") throw new Error(`[${serverName}] init ${role}: ${JSON.stringify(ok).slice(0, 400)}`);
    c.takeNewFrames();
    conns[role] = c;
  };

  const foldFrame = (role, m) => {
    if (m.op === "start-sync-ok") {
      const order = orderOf(m.q);
      const etype = order.etype;
      models.set(m["subscription-id"], {
        subId: m["subscription-id"],
        role,
        q: m.q,
        token: m.token,
        order,
        etype,
        byLabel: plan.byLabel[etype] ?? {},
        attrDefs: plan.attrDefs,
        labelByAttr: plan.labelByAttr,
        txId: null,
        entities: [],
        active: true,
      });
      return;
    }
    const sub = models.get(m["subscription-id"]);
    if (!sub) return;
    if (m.op === "sync-load-batch") {
      for (const rows of m["join-rows"]) {
        const triples = rows.map((t) => [...t]);
        sub.entities.push({
          eid: triples[0]?.[0],
          triples,
          serverCreatedAt: idOf(triples),
        });
      }
      sortEntities(sub);
      return;
    }
    if (m.op === "sync-init-finish") {
      sub.txId = m["tx-id"];
      return;
    }
    if (m.op !== "sync-update-triples") return;
    for (const tx of m.txes) {
      if (sub.txId != null && sub.txId >= tx["tx-id"]) continue;
      sub.txId = tx["tx-id"];
      const byEid = new Map();
      for (const change of tx.changes) {
        const eid = change.triple[0];
        if (!byEid.has(eid)) byEid.set(eid, []);
        byEid.get(eid).push(change);
      }
      for (const [eid, changes] of byEid) {
        const idx = sub.entities.findIndex((e) => hasTriple(e.triples, eid));
        if (idx >= 0) {
          for (const c of changes) applyChange(sub.entities[idx].triples, c);
          if (!hasTriple(sub.entities[idx].triples, eid)) sub.entities.splice(idx, 1);
        } else {
          const triples = [];
          for (const c of changes) applyChange(triples, c);
          if (!triples.length) continue;
          sub.entities.push({ eid, triples, serverCreatedAt: idOf(triples) });
        }
      }
      sortEntities(sub);
    }
  };

  const flush = () => {
    const frames = {};
    for (const role of ROLES) {
      const c = conns[role];
      if (!c) continue;
      const raw = c.takeNewFrames();
      const views = [];
      for (const m of raw) {
        if (m.op === "error") {
          const oe = m["original-event"]?.op;
          if (["start-sync", "resync-table", "remove-sync"].includes(oe)) views.push(frameView(m));
          continue;
        }
        if (!["start-sync-ok", "sync-load-batch", "sync-init-finish", "sync-update-triples"].includes(m.op)) continue;
        if (m.op === "sync-update-triples") {
          maxTxPerFrame = Math.max(maxTxPerFrame, m.txes?.length ?? 0);
          if ((m.txes?.length ?? 0) > 100) {
            throw new Error(`[${serverName}] sync-update-triples carried ${m.txes.length} txes (legacy cap is 100)`);
          }
        }
        foldFrame(role, m);
        views.push(frameView(m));
      }
      frames[role] = views;
    }
    return frames;
  };

  const stateView = () => {
    const out = {};
    for (const role of SYNC_ROLES) {
      out[role] = [...models.values()]
        .filter((m) => m.active && m.role === role)
        .sort((a, b) => ((a.name ?? a.subId) < (b.name ?? b.subId) ? -1 : 1))
        .map(projectModel);
    }
    return out;
  };

  const waitReply = (role, ceid, timeout = 15000) =>
    conns[role].waitFor((m) => m["client-event-id"] === ceid, timeout);

  const syncFrameAfter = (frame, subId, target) => {
    if (frame.op !== "sync-update-triples" || frame["subscription-id"] !== subId) return false;
    const last = frame.txes?.at(-1)?.["tx-id"];
    return last != null && last >= target;
  };

  const targetsFor = (subs, etypes = null) => {
    const out = [];
    for (const sub of subs) {
      if (!sub.active) continue;
      const target = committed
        .filter((c) =>
          c.txId > (sub.txId ?? -1)
            ? etypes
              ? etypes.includes(c.etype)
              : c.etype === sub.etype
            : false,
        )
        .reduce((m, c) => Math.max(m, c.txId), 0);
      if (target) out.push({ sub, target });
    }
    return out;
  };

  const waitTargets = async (targets) => {
    try {
      await Promise.all(
        targets.map(({ sub, target }) => {
          const c = conns[sub.role];
          return c.waitFor((m) => syncFrameAfter(m, sub.subId, target), 20000);
        }),
      );
    } catch (e) {
      const waiting = targets
        .map(({ sub, target }) => `${sub.name ?? sub.subId}(${sub.etype})@tx${target}`)
        .join(", ");
      throw new Error(`${e.message}; waiting ${waiting}`);
    }
  };

  for (const role of ROLES) await openRole(role);

  for (const op of plan.script) {
    if (op.kind === "tx") {
      const ceid = crypto.randomUUID();
      conns[op.session].send({ "client-event-id": ceid, op: "transact", "tx-steps": op.steps });
      const reply = await waitReply(op.session, ceid);
      if (reply.op === "transact-ok" && op.sync) {
        committed.push({ txId: reply["tx-id"], etypes: op.etypes });
        const subs = [...models.values()].filter((m) => m.active && op.etypes.includes(m.etype));
        await waitTargets(targetsFor(subs, op.etypes));
      }
      outcomes.push({ name: op.name, kind: "tx", reply: replyView(reply), frames: flush() });
    } else if (op.kind === "start") {
      const ceid = crypto.randomUUID();
      const msg = { "client-event-id": ceid, op: "start-sync" };
      if (op.q !== null) msg.q = op.q;
      conns[op.session].send(msg);
      const reply = await waitReply(op.session, ceid);
      if (reply.op === "start-sync-ok") {
        subByName.set(op.subName, reply["subscription-id"]);
        await conns[op.session].waitFor(
          (m) => m.op === "sync-init-finish" && m["subscription-id"] === reply["subscription-id"],
        );
      }
      const frames = flush();
      const subId = subByName.get(op.subName);
      const model = subId ? models.get(subId) : null;
      if (model) model.name = op.subName;
      outcomes.push({ name: op.name, kind: "start", label: op.label ?? null, reply: replyView(reply), frames });
    } else if (op.kind === "remove") {
      const subId = subByName.get(op.subName);
      if (!subId) throw new Error(`remove: unknown sub ${op.subName}`);
      conns[op.session].send({
        "client-event-id": crypto.randomUUID(),
        op: "remove-sync",
        "subscription-id": subId,
        "keep-subscription": op.keep,
      });
      await new Promise((r) => setTimeout(r, 60));
      const frames = flush();
      const model = models.get(subId);
      if (model) model.active = false;
      outcomes.push({ name: op.name, kind: "remove", keep: op.keep, frames });
    } else if (op.kind === "resync") {
      const subId = subByName.get(op.subName);
      if (!subId) throw new Error(`resync: unknown sub ${op.subName}`);
      const model = models.get(subId);
      const ceid = subId;
      conns[op.session].send({
        "client-event-id": ceid,
        op: "resync-table",
        "subscription-id": subId,
        "tx-id": model.txId ?? 0,
        token: model.token,
      });
      model.active = true;
      model.role = op.session;
      const targets = targetsFor([model]);
      if (targets.length) await waitTargets(targets);
      else await new Promise((r) => setTimeout(r, 100));
      outcomes.push({ name: op.name, kind: "resync", frames: flush() });
    } else if (op.kind === "reconnect") {
      const role = op.session;
      const before = flush();
      for (const model of models.values()) {
        if (model.role === role) model.active = false;
      }
      conns[role].close();
      await openRole(role);
      const resumed = [];
      for (const subName of op.subs) {
        const subId = subByName.get(subName);
        const model = models.get(subId);
        if (!model) continue;
        model.role = role;
        model.active = true;
        conns[role].send({
          "client-event-id": subId,
          op: "resync-table",
          "subscription-id": subId,
          "tx-id": model.txId ?? 0,
          token: model.token,
        });
        resumed.push(model);
      }
      const targets = targetsFor(resumed);
      if (targets.length) await waitTargets(targets);
      else await new Promise((r) => setTimeout(r, 100));
      const after = flush();
      const frames = Object.fromEntries(ROLES.map((r) => [r, [...(before[r] ?? []), ...(after[r] ?? [])]]));
      outcomes.push({ name: op.name, kind: "reconnect", frames });
    } else if (op.kind === "checkpoint") {
      await new Promise((r) => setTimeout(r, 80));
      await settle(Object.values(conns), QUIET_MS);
      outcomes.push({ name: op.name, kind: "checkpoint", frames: flush(), states: stateView() });
    } else if (op.kind === "state-checkpoint") {
      await new Promise((r) => setTimeout(r, 80));
      await settle(Object.values(conns), QUIET_MS);
      flush();
      outcomes.push({ name: op.name, kind: "state-checkpoint", states: stateView() });
    } else if (op.kind === "burst") {
      const sends = op.items.map((item) => {
        const ceid = crypto.randomUUID();
        conns[item.session].send({ "client-event-id": ceid, op: "transact", "tx-steps": item.steps });
        return { ceid, item };
      });
      const replies = await Promise.all(sends.map(({ ceid, item }) => waitReply(item.session, ceid)));
      replies.forEach((reply, i) => {
        if (reply.op === "transact-ok") {
          committed.push({ txId: reply["tx-id"], etypes: sends[i].item.etypes });
        }
      });
      const subs = [...models.values()].filter((m) => m.active && m.etype === DOCS);
      const targets = targetsFor(subs, [DOCS]);
      if (targets.length) await waitTargets(targets);
      await settle(Object.values(conns), QUIET_MS);
      flush();
      outcomes.push({
        name: op.name,
        kind: "burst",
        replies: replies.map(replyView),
        states: stateView(),
      });
    } else {
      throw new Error(`unknown plan op ${op.kind}`);
    }
  }

  await new Promise((r) => setTimeout(r, 80));
  await settle(Object.values(conns), QUIET_MS);
  const finalFrames = flush();
  outcomes.push({ name: "final", kind: "final", frames: finalFrames, states: stateView() });
  for (const c of Object.values(conns)) c.close();
  return { outcomes, maxTxPerFrame };
}

// ---------------------------------------------------------------------------
// run both servers and compare

const plan = buildPlan();
console.log(`fuzz-sync: seed=${seed} rounds=${rounds} ops=${plan.script.length}`);
console.log("running against legacy…");
const legacy = await runOn("legacy", plan);
console.log("running against rust…");
const rust = await runOn("rust", plan);

const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), {
  prefixes: DIVERGENCE_PREFIXES.syncfuzz,
});
let mismatches = 0;
let allowed = 0;
const maxOutcomes = Math.max(legacy.outcomes.length, rust.outcomes.length);
for (let i = 0; i < maxOutcomes; i++) {
  const a = legacy.outcomes[i];
  const b = rust.outcomes[i];
  const name = a?.name ?? b?.name ?? `#${i}`;
  const p = `syncfuzz:${name}`;
  if (!a || !b || a.name !== b.name || a.kind !== b.kind) {
    console.error(`\nDIVERGENCE ${p}: outcome list differs`);
    console.error(" legacy:", JSON.stringify(a)?.slice(0, 600));
    console.error(" rust:  ", JSON.stringify(b)?.slice(0, 600));
    mismatches++;
    continue;
  }
  if (canon(a) === canon(b)) continue;
  const verdict = allowlist.check(p, a, b);
  const lines = describeAllowVerdict(verdict);
  if (verdict.entry) {
    allowed++;
    console.log(`allowed ${p}: ${lines.join("; ")}`);
    continue;
  }
  const fd = firstDifference(a, b);
  mismatches++;
  console.error(`\nDIVERGENCE ${p} first at ${fd.p || "(whole value)"}`);
  console.error(" legacy:", JSON.stringify(fd.a)?.slice(0, 1200));
  console.error(" rust:  ", JSON.stringify(fd.b)?.slice(0, 1200));
  for (const line of lines) console.error(line);
}

const stale = allowlist.reportStale();
console.log(
  `\nfuzz-sync seed ${seed}: ${legacy.outcomes.length} checkpoints, max txes/frame legacy=${legacy.maxTxPerFrame} rust=${rust.maxTxPerFrame}, ` +
    `${mismatches} mismatch(es), ${allowed} allowed`,
);
if (mismatches || stale) {
  console.error(`FUZZ SYNC FAILED for seed ${seed}`);
  process.exit(1);
}
console.log(`FUZZ SYNC PASSED for seed ${seed}`);
process.exit(0);
