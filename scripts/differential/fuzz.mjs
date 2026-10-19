// Property/fuzz layer of the differential harness: seeded random tx-steps and
// queries over the whole client grammar run against both servers; asserts
// client-visible invariants on each server and cross-server equality of every
// query result and every error type.
//
// Grammar covered (surface.json tx:* / iq:*): add-triple (with `mode`),
// retract-triple, deep-merge-triple, delete-entity, add-attr, update-attr,
// delete-attr, restore-attr, rule-params, lookup refs ([attr-id, value] on a
// unique attr) as a step's entity and as a link's value, matching or not,
// malformed steps; checked-data-type number / string / date / boolean attrs
// (dates as epoch ms and ISO-8601 strings, checked-type violations); links
// of cardinality one (fuzz.owner), many (fuzz.tags) and one-to-one
// (fuzz.profile, a unique ref), linked and unlinked, queried nested in both
// directions; where ops = / $in / in / $ne / $not / $isNull / $gt / $gte /
// $lt / $lte / $like / $ilike / $entityIdStartsWith, or / and, dotted link
// paths of one to three hops, limit / offset / first / last / order / after /
// before (cursor walks, also by date and boolean), fields, nested links,
// $$ruleParams, and admin-only options that must be rejected alike.
//
// Usage: node fuzz.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL (defaults as in replay.mjs)

import {
  connect,
  settle,
  makeIdFactory,
  normalize,
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

const blobRef = (mk) => (id, ns, label, rns, rlabel, extra = {}) => [
  "add-attr",
  {
    id,
    "forward-identity": [id, ns, label],
    "reverse-identity": [mk(), rns, rlabel],
    "value-type": "ref",
    cardinality: "one",
    "unique?": false,
    "index?": false,
    isUnsynced: true,
    ...extra,
  },
];

// Build the deterministic op script once; replay it on each server.
function buildScript() {
  const rand = prng(seed);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const mk = makeIdFactory(appIdLegacy);
  const ref = blobRef(mk);
  const NS = "fuzz";
  const OWNER = "owner";
  const TAG = "tag";
  const PROFILE = "profile";
  const attrs = {
    id: mk(),
    labels: Object.fromEntries(["p1", "p2", "p3", "num"].map((l) => [l, mk()])),
    // typed + indexed attrs: exercised by comparison/like/order query ops
    typedNum: mk(),
    typedStr: mk(),
    typedDate: mk(),
    typedBool: mk(),
    // link fuzz.owner -> owner (reverse owner.items), cardinality one
    owner: mk(),
    ownerId: mk(),
    ownerName: mk(),
    // link fuzz.tags -> tag (reverse tag.fuzzes), cardinality many
    tags: mk(),
    tagId: mk(),
    tagLabel: mk(),
    // link fuzz.profile -> profile (reverse profile.fuzz), one-to-one: a
    // unique cardinality-one ref
    profile: mk(),
    profileId: mk(),
    profileHandle: mk(),
    profileBio: mk(),
  };
  const entities = Array.from({ length: 8 }, () => mk());
  const owners = Array.from({ length: 3 }, () => mk());
  const tags = Array.from({ length: 4 }, () => mk());
  // the 4th profile isn't seeded: links to it dangle until an id lookup
  // or a churn step creates it
  const profiles = Array.from({ length: 4 }, () => mk());
  const names = ["ann", "bob", "cy"];
  // unique values for lookups; "nope", "h3" and "h4" match nothing at first
  const tagLabels = ["red", "green", "blue", "gold", "nope"];
  const handles = ["h0", "h1", "h2", "h3", "h4"];
  const attrSteps = [
    blob(attrs.id, NS, "id", { "unique?": true, "index?": true }),
    ...Object.entries(attrs.labels).map(([l, id]) => blob(id, NS, l)),
    blob(attrs.typedNum, NS, "tnum", { "index?": true, "checked-data-type": "number" }),
    blob(attrs.typedStr, NS, "tstr", { "index?": true, "checked-data-type": "string" }),
    blob(attrs.typedDate, NS, "tdate", { "index?": true, "checked-data-type": "date" }),
    blob(attrs.typedBool, NS, "tbool", { "index?": true, "checked-data-type": "boolean" }),
    blob(attrs.ownerId, OWNER, "id", { "unique?": true, "index?": true }),
    blob(attrs.ownerName, OWNER, "name", { "index?": true, "checked-data-type": "string" }),
    blob(attrs.tagId, TAG, "id", { "unique?": true, "index?": true }),
    blob(attrs.tagLabel, TAG, "label", { "unique?": true, "index?": true, "checked-data-type": "string" }),
    blob(attrs.profileId, PROFILE, "id", { "unique?": true, "index?": true }),
    blob(attrs.profileHandle, PROFILE, "handle", { "unique?": true, "index?": true, "checked-data-type": "string" }),
    blob(attrs.profileBio, PROFILE, "bio"),
    ref(attrs.owner, NS, "owner", OWNER, "items"),
    ref(attrs.tags, NS, "tags", TAG, "fuzzes", { cardinality: "many" }),
    ref(attrs.profile, NS, "profile", PROFILE, "fuzz", { "unique?": true }),
  ];
  const script = [{ kind: "tx", steps: attrSteps }];
  script.push({
    kind: "tx",
    steps: [
      ...owners.flatMap((o, i) => [
        ["add-triple", o, attrs.ownerId, o],
        ["add-triple", o, attrs.ownerName, names[i]],
      ]),
      ...tags.flatMap((t, i) => [
        ["add-triple", t, attrs.tagId, t],
        ["add-triple", t, attrs.tagLabel, tagLabels[i]],
      ]),
      ...profiles.slice(0, 3).flatMap((p, i) => [
        ["add-triple", p, attrs.profileId, p],
        ["add-triple", p, attrs.profileHandle, handles[i]],
      ]),
    ],
  });
  const values = ["a", "b", "c", 1, 2, 3.5, true, false, null, { k: 1 }, [1, 2]];
  const nums = [0, 1, 2.5, 7, -3, 100];
  const strs = ["alpha", "beta", "gamma", "alphabet", "Zed"];
  // the dates clients write: epoch ms (Date.getTime(), some past 1e12),
  // ISO-8601 with a zone, an offset, no zone, a space instead of `T`,
  // date-only, and Date.toUTCString(); several name the same instant
  const dates = [
    0, -86400000, 946684800000, 1704067200000, 1718445600000,
    "2024-01-01T00:00:00Z", "2024-01-01T00:00:00.000Z", "2024-06-15T12:00:00+02:00",
    "2024-06-15T10:00:00.500Z", "2024-06-15T12:00:00", "2024-06-15 12:00:00", "2024-06-15",
    "2000-01-01T00:00:00Z", "Sat, 15 Jun 2024 10:00:00 GMT",
  ];
  // where values for a date: the same shapes plus Date.toLocaleString() and
  // the relative keyword comparisons accept (every date written is past)
  const dateQs = [
    0, 946684800000, 1704067200000, 1718445600000, "2024-01-01T00:00:00Z", "2024-06-15T10:00:00Z",
    "2024-06-15T12:00:00+02:00", "2024-06-15", "2024-06-15T12:00:00", "2000-01-01 00:00:00",
    "Sat, 15 Jun 2024 10:00:00 GMT", "6/15/2024, 10:00:00 AM", "now",
  ];
  // not a date on either server
  const badDateQs = ["not-a-date", "2024-13-45", "", true];
  const pickDateQ = () => pick(chance(0.85) ? dateQs : badDateQs);
  const orderKeys = ["serverCreatedAt", "tnum", "tstr", "tdate", "tbool"];
  // where paths through the links: one hop, and two or three hops through
  // every link kind in both directions
  const linkPaths = ["owner.name", "owner.id", "tags.label", "tags.id", "profile.handle", "profile.id"];
  const hopPaths = [
    "owner.items.tstr", "tags.fuzzes.tnum", "profile.fuzz.tbool", "owner.items.tags.label",
    "tags.fuzzes.owner.name", "profile.fuzz.owner.name", "tags.fuzzes.profile.handle",
  ];
  const pathValue = (path) => {
    const segs = path.split(".");
    switch (segs.at(-1)) {
      case "name": return pick(names);
      case "label": return pick(tagLabels);
      case "handle": return pick(handles);
      case "tstr": return pick(strs);
      case "tnum": return chance(0.3) ? { [pick(["$gt", "$lte"])]: pick(nums) } : pick(nums);
      case "tbool": return pick([true, false]);
      case "id": return pick({ owner: owners, tags: tags, profile: profiles }[segs.at(-2)]);
    }
    throw new Error(`no value for ${path}`);
  };
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
  // lookup refs ([attr-id, value] on a unique attr) as the entity of a step.
  // One that matches nothing creates the entity, with a server-chosen id
  // unless the attr is `id`, so those creations stay in tag / profile,
  // which are never paged by creation time.
  const lookupStep = () =>
    pick([
      () => ["add-triple", [attrs.profileHandle, pick(handles)], attrs.profileBio, pick(values)],
      () => ["deep-merge-triple", [attrs.profileHandle, pick(handles)], attrs.profileBio, { [`k${Math.floor(rand() * 2)}`]: pick(values) }],
      () => ["retract-triple", [attrs.profileHandle, pick(handles)], attrs.profileBio, pick(values)],
      () => ["add-triple", [attrs.profileId, pick(profiles)], attrs.profileBio, pick(values)],
      () => ["add-triple", [attrs.id, pick(entities)], attrs.typedNum, pick(nums)],
      // a rename through the lookup (`tag[lookup('label', 'red')].update({label: ...})`)
      () => ["add-triple", [attrs.tagLabel, pick(tagLabels)], attrs.tagLabel, pick(tagLabels)],
      () => ["delete-entity", [attrs.profileHandle, pick(handles)], PROFILE],
      () => ["delete-entity", [attrs.tagLabel, pick(tagLabels)], TAG],
    ])();
  let p2Deleted = false;
  for (let i = 0; i < rounds; i++) {
    const r = rand();
    if (r < 0.6) {
      // random transaction: 1-4 steps
      const n = 1 + Math.floor(rand() * 4);
      const steps = [];
      // the new grammar a tx exercises (success floor)
      const features = new Set();
      if (chance(0.05)) steps.push(["rule-params", pick(entities), NS, { k: Math.floor(rand() * 3) }]);
      for (let j = 0; j < n; j++) {
        const e = pick(entities);
        const kind = rand();
        if (kind < 0.22) {
          steps.push(["add-triple", e, attrs.id, e]);
          steps.push(["add-triple", e, pick(Object.values(attrs.labels)), pick(values)]);
        } else if (kind < 0.32) {
          steps.push(["add-triple", e, attrs.id, e]);
          steps.push(["add-triple", e, attrs.typedNum, pick(nums)]);
          steps.push(["add-triple", e, attrs.typedStr, pick(strs)]);
        } else if (kind < 0.41) {
          steps.push(["add-triple", e, attrs.id, e]);
          if (chance(0.7)) {
            steps.push(["add-triple", e, attrs.typedDate, pick([...dates, null])]);
            features.add("date");
          }
          if (chance(0.6)) {
            steps.push(["add-triple", e, attrs.typedBool, pick([true, false, null])]);
            features.add("bool");
          }
        } else if (kind < 0.47) {
          steps.push(["add-triple", e, attrs.id, e]);
          steps.push(["deep-merge-triple", e, attrs.labels.p3, { [`k${Math.floor(rand() * 3)}`]: pick(values) }]);
        } else if (kind < 0.51) {
          // checked-type violation: both servers must reject alike
          steps.push(["add-triple", e, attrs.id, e]);
          const [a, bad] = pick([
            [attrs.typedNum, ["oops", true]],
            [attrs.typedDate, ["not-a-date", "2024-13-45", "", true, { d: 1 }]],
            [attrs.typedBool, ["true", 1, 0]],
          ]);
          steps.push(["add-triple", e, a, pick(bad)]);
        } else if (kind < 0.56) {
          // link / unlink, cardinality one
          if (chance(0.7)) steps.push(["add-triple", e, attrs.owner, pick(owners)]);
          else steps.push(["retract-triple", e, attrs.owner, pick(owners)]);
        } else if (kind < 0.62) {
          // has-many link / unlink, to an id or through a value lookup on
          // the unique label (one that matches nothing fails the tx)
          const t = chance(0.6) ? pick(tags) : [attrs.tagLabel, pick(tagLabels)];
          steps.push([chance(0.75) ? "add-triple" : "retract-triple", e, attrs.tags, t]);
          features.add("tags");
        } else if (kind < 0.66) {
          // one-to-one link / unlink: a profile already linked to another
          // entity is a unique violation
          const p = chance(0.6) ? pick(profiles) : [attrs.profileHandle, pick(handles)];
          steps.push([chance(0.75) ? "add-triple" : "retract-triple", e, attrs.profile, p]);
          features.add("profile");
        } else if (kind < 0.72) {
          steps.push(lookupStep());
          features.add("lookup");
        } else if (kind < 0.77) {
          // create / update mode: existence-dependent, must fail alike
          steps.push(["add-triple", e, attrs.id, e, { mode: pick(["create", "update"]) }]);
          steps.push(["add-triple", e, attrs.labels.p1, pick(values)]);
        } else if (kind < 0.81) {
          steps.push(malformed(e));
        } else if (kind < 0.9) {
          steps.push(["retract-triple", e, pick(Object.values(attrs.labels)), pick(values)]);
        } else if (kind < 0.95) {
          steps.push(["delete-entity", e, NS]);
        } else if (chance(0.5)) {
          // tag churn: delete one (its links go with it), or (re)create it
          // under a label that may be taken
          const t = pick(tags);
          if (chance(0.3)) steps.push(["delete-entity", t, TAG]);
          else steps.push(["add-triple", t, attrs.tagId, t], ["add-triple", t, attrs.tagLabel, pick(tagLabels)]);
        } else {
          const p = pick(profiles);
          if (chance(0.3)) steps.push(["delete-entity", p, PROFILE]);
          else steps.push(["add-triple", p, attrs.profileId, p], ["add-triple", p, attrs.profileHandle, pick(handles)]);
        }
      }
      script.push({ kind: "tx", steps, features: [...features] });
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
      // cursor walk: page through a namespace by a typed order (tags by
      // their unique label)
      const dir = pick(["asc", "desc"]);
      const [ns, key] = chance(0.85) ? [NS, pick(orderKeys)] : [TAG, "label"];
      const size = 1 + Math.floor(rand() * 3);
      script.push({ kind: "paginate", ns, key, dir, size, backwards: chance(0.3), inclusive: chance(0.3) });
    } else {
      // random query over the where grammar, links, fields, pagination
      const opts = {};
      const q = rand();
      if (q < 0.08) opts.where = { [pick(["p1", "p2"])]: pick(["a", "b", "c"]) };
      else if (q < 0.15) opts.where = { tnum: { [pick(["$gt", "$gte", "$lt", "$lte"])]: pick(nums) } };
      else if (q < 0.2) opts.where = { tstr: { [pick(["$like", "$ilike"])]: pick(["%a%", "alph%", "%ed", "%ET%"]) } };
      else if (q < 0.26) opts.where = { tdate: { [pick(["$gt", "$gte", "$lt", "$lte"])]: pickDateQ() } };
      else if (q < 0.3) {
        // date equality is by instant; the relative keywords are refused here
        const d = () => pick(chance(0.9) ? dateQs : badDateQs);
        opts.where = { tdate: pick([d(), { $in: [d(), d()] }, { $in: [d(), null] }, { [pick(["$ne", "$not"])]: d() }]) };
      } else if (q < 0.34) {
        const b = () => pick([true, false]);
        // a string, or $like, on a boolean is refused alike
        opts.where = { tbool: pick([b(), { $in: [b(), b()] }, { $in: [true, null] }, { [pick(["$ne", "$not"])]: b() }, { [pick(["$gt", "$lte"])]: b() }, "true", { $like: "t%" }]) };
      } else if (q < 0.38) opts.where = { [pick(["tnum", "owner", "tdate", "tbool", "tags", "profile"])]: { $isNull: pick([true, false]) } };
      else if (q < 0.43) opts.where = { [pick(["tstr", "p1"])]: { [pick(["$in", "in"])]: [pick(strs), pick(strs), "a"] } };
      else if (q < 0.47) opts.where = { [pick(["tstr", "tnum"])]: { [pick(["$ne", "$not"])]: pick([...strs, ...nums]) } };
      else if (q < 0.52) {
        const path = pick(linkPaths);
        opts.where = { [path]: pathValue(path) };
      } else if (q < 0.6) {
        const path = pick(hopPaths);
        opts.where = { [path]: pathValue(path) };
      } else if (q < 0.625) opts.where = { $entityIdStartsWith: pick(entities).slice(0, 8) };
      else if (q < 0.69) {
        const branch = () =>
          pick([
            () => ({ tnum: { $gt: pick(nums) } }),
            () => ({ [pick(["tstr", "owner.name"])]: pick([...strs, ...names]) }),
            () => ({ tdate: { [pick(["$lt", "$gte"])]: pick(dateQs) } }),
            () => ({ tbool: pick([true, false]) }),
            () => ({ "tags.label": pick(tagLabels) }),
            () => {
              const path = pick(hopPaths);
              return { [path]: pathValue(path) };
            },
          ])();
        opts.where = { [pick(["or", "and"])]: [branch(), branch()] };
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
      // the inclusive flags are validated as booleans whether or not a cursor
      // is given; null is "absent"
      if ((opts.first || opts.last) && chance(0.15)) opts[opts.first ? "afterInclusive" : "beforeInclusive"] = pick([true, false, null, "yes", 1]);
      if (chance(0.12)) opts.fields = pick([["p1"], ["tnum", "tstr"], ["id"], ["p3", "owner"], ["tdate", "tbool"], ["tdate", "tags", "profile"]]);
      if (chance(0.04)) opts.aggregate = "count"; // admin-only: rejected alike
      const form = Object.keys(opts).length ? { $: opts } : {};
      if (chance(0.25)) form.owner = chance(0.5) ? {} : { $: { fields: ["name"] } };
      if (chance(0.2)) form.tags = pick([{}, { $: { where: { label: { $in: [pick(tagLabels), pick(tagLabels)] } } } }, { fuzzes: {} }]);
      if (chance(0.15)) form.profile = pick([{}, { fuzz: { owner: {} } }]);
      if (chance(0.08)) form.$$ruleParams = { k: 1 };
      const rp = rand();
      let root;
      if (rp < 0.08) {
        // owner -> items (reverse of the cardinality-one link)
        const items = chance(0.4)
          ? {}
          : { $: { where: pick([{ tnum: { $gt: 0 } }, { "tags.label": pick(tagLabels) }, { tdate: { $lt: pick(dateQs) } }, { "profile.handle": pick(handles) }]) } };
        root = { [OWNER]: chance(0.3) ? { $: { where: pick([{ "items.tags.label": pick(tagLabels) }, { "items.tdate": { $gte: pick(dateQs) } }]) }, items } : { items } };
      } else if (rp < 0.13) {
        root = {
          [TAG]: pick([
            { fuzzes: {} },
            { $: { where: { "fuzzes.owner.name": pick(names) } }, fuzzes: {} },
            { $: { where: { "fuzzes.tbool": pick([true, false]) } } },
            { $: { where: { "fuzzes.profile.handle": pick(handles) } } },
            { $: { order: { label: pick(["asc", "desc"]) }, first: 1 + Math.floor(rand() * 3) } },
            { fuzzes: { $: { where: { tdate: { $gte: pick(dateQs) } } } } },
          ]),
        };
      } else if (rp < 0.17) {
        root = {
          [PROFILE]: pick([
            { fuzz: {} },
            { fuzz: { owner: {}, tags: {} } },
            { $: { where: { "fuzz.tdate": { $lt: pick(dateQs) } } } },
            { $: { where: { "fuzz.owner.name": pick(names) } }, fuzz: {} },
            { $: { where: { fuzz: { $isNull: pick([true, false]) } } } },
            { $: { where: { handle: { $in: [pick(handles), pick(handles)] } } } },
          ]),
        };
      } else {
        root = { [NS]: form };
      }
      script.push({ kind: "query", q: root });
    }
  }
  // final full queries on every namespace, both directions of every link
  script.push({ kind: "query", q: { [TAG]: { fuzzes: {} } } });
  script.push({ kind: "query", q: { [PROFILE]: { fuzz: {} } } });
  script.push({ kind: "query", q: { [NS]: { owner: {}, tags: {}, profile: {} } } });
  script.push({ kind: "query", q: { [OWNER]: { items: {} } } });
  // what each label of a namespace names: [attr id, namespace a link leads
  // to, whether it is the link's reverse] (pagedOrWhereOnly resolves where
  // paths with it)
  script.schema = {
    [NS]: {
      ...Object.fromEntries(Object.entries(attrs.labels).map(([l, id]) => [l, [id]])),
      id: [attrs.id],
      tnum: [attrs.typedNum],
      tstr: [attrs.typedStr],
      tdate: [attrs.typedDate],
      tbool: [attrs.typedBool],
      owner: [attrs.owner, OWNER],
      tags: [attrs.tags, TAG],
      profile: [attrs.profile, PROFILE],
    },
    [OWNER]: { id: [attrs.ownerId], name: [attrs.ownerName], items: [attrs.owner, NS, true] },
    [TAG]: { id: [attrs.tagId], label: [attrs.tagLabel], fuzzes: [attrs.tags, NS, true] },
    [PROFILE]: { id: [attrs.profileId], handle: [attrs.profileHandle], bio: [attrs.profileBio], fuzz: [attrs.profile, NS, true] },
  };
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
  let txOk = 0;
  // committed transacts per grammar feature (success floor)
  const txOkBy = {};
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
      return { result: projectTriples(reply.result), raw: reply.result };
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
        txOk++;
        for (const f of op.features ?? []) txOkBy[f] = (txOkBy[f] ?? 0) + 1;
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
      const { ns, key, dir, size, backwards, inclusive } = op;
      const seen = new Map();
      let cursor = null;
      let pages = 0;
      let error = null;
      let firstCursor = null;
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
        if (pages === 1) firstCursor = next ?? null;
        if (!more || !next || pages > 50) break;
        cursor = next;
      }
      // an inclusive page from the first page's boundary cursor: the boundary
      // row comes back on both servers (each with its own cursor); compared
      // as the set of entity ids since cursor values differ by server
      let inclusivePage;
      if (inclusive && !error && firstCursor) {
        const $ = backwards ? { last: size, order: { [key]: dir }, before: firstCursor, beforeInclusive: true } : { first: size, order: { [key]: dir }, after: firstCursor, afterInclusive: true };
        const r = await query({ [ns]: { $ } });
        inclusivePage = r.error ? { error: r.error } : { entities: [...new Set(r.result.triples.map((t) => t[0]))].sort(), boundaryIncluded: r.result.triples.some((t) => t[0] === firstCursor[0]) };
      }
      queryResults.push({ i, paginate: op, ...(error ? { error } : { pages, union: [...seen.values()].sort((x, y) => (canon(x) < canon(y) ? -1 : 1)), inclusivePage }) });
    }
  }
  if (watermarkLags) {
    console.log(`[${serverName}] add-query watermark lagged last tx ${watermarkLags} times (informational)`);
  }
  await settle([conn], 500);
  conn.close();
  return { queryResults, violations, txOk, txOkBy };
}

// The client flattens a result's join-rows into a set of [e a v] triples
// (model/instaqlResult.js:1-25); t is a server timestamp and the order is
// the server's, so neither is compared. Unlike lib.mjs projectResult, values
// stay raw: its normalize turns numbers past 1e12 into "<ts>", and here
// those are epoch-ms dates the script wrote. Only uuids a server chose
// (entities a lookup on a non-id attr created) become "<uuid>" on both.
function projectTriples(result) {
  const triples = [];
  const walk = (nodes) => {
    for (const node of nodes ?? []) {
      for (const rows of node?.data?.["datalog-result"]?.["join-rows"] ?? []) {
        for (const [e, a, v] of rows) triples.push([normalize(e), normalize(a), typeof v === "string" ? normalize(v) : v]);
      }
      walk(node?.["child-nodes"]);
    }
  };
  walk(result);
  const deduped = [...new Map(triples.map((t) => [canon(t), t])).values()];
  deduped.sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
  return { triples: deduped, aggregate: normalize(result?.[0]?.data?.aggregate ?? null) };
}

const script = buildScript();
console.log(`fuzz: seed=${seed} rounds=${rounds} ops=${script.length}`);
console.log("running against legacy…");
const legacy = await runOn("legacy", script);
console.log("running against rust…");
const rust = await runOn("rust", script);

// A paginated form keeps one where row per entity (legacy's page cte is a
// DISTINCT ON over its where ctes), and a top-level `or` gathers its
// branches as disjoint rows. For an entity matching several branches, which
// branch's row survives is up to legacy's query plan; rust keeps the first.
// So for those queries the triples only a where row carries (a branch attr
// the `fields` leave out, a link path's triples past the nested forms the
// query asks for) are compared on neither side. A path that walks a link
// back (tags.fuzzes) reuses its attr id, which then drops at every hop.
function pagedOrWhereOnly(q) {
  const form = q?.fuzz;
  const $ = form?.$;
  if (!$?.where?.or || !($.limit || $.first || $.last)) return null;
  const drop = new Set();
  for (const branch of $.where.or) {
    for (const path of Object.keys(branch)) {
      const segs = path.split(".");
      // does the query nest forms along the path down to depth d?
      const nested = (d) => segs.slice(0, d).reduce((node, seg) => node?.[seg], form) !== undefined;
      let ns = "fuzz";
      segs.forEach((seg, k) => {
        const [id, next, reverse] = script.schema[ns]?.[seg] ?? [];
        if (!id) return;
        // a hop's triple sits on the entity the hop starts from, or for a
        // reverse link on the one it reaches; it ships without the where row
        // when the query's own forms read that entity
        const kept = k === 0 ? !reverse && (!$.fields || $.fields.includes(seg)) : nested(reverse ? k + 1 : k);
        if (!kept) drop.add(id);
        ns = next;
      });
    }
  }
  return drop.size ? drop : null;
}
const comparable = (entry) => {
  const drop = entry?.result && pagedOrWhereOnly(entry.q);
  if (!drop) return entry;
  return { ...entry, result: { ...entry.result, triples: entry.result.triples.filter(([, a]) => !drop.has(a)) } };
};

// Named exceptions: a documented legacy defect this server doesn't copy.
// Each matches one script op shape and pins both servers' replies whole, so
// anything else that differs, or either side changing, still fails. Hits
// are counted and reported.
const whereEntries = (v, out = []) => {
  if (Array.isArray(v)) v.forEach((x) => whereEntries(x, out));
  else if (v && typeof v === "object") {
    for (const [k, x] of Object.entries(v)) {
      out.push([k, x]);
      whereEntries(x, out);
    }
  }
  return out;
};
const EXCEPTIONS = [
  {
    // attr_pat.clj:308-317 coerced-type-comparison-value!: for a `date`
    // attr it `case`s over the value's tag with only :number and :string
    // branches, so a boolean bound (which the ::comparator spec admits,
    // instaql.clj:56-58) throws IllegalArgumentException, and
    // handle-uncaught-err (session.clj:1101-1118) answers a typeless 500.
    // This server refuses it like any other non-date value: 400
    // validation-failed, the client's ordinary query error.
    name: "date-comparison-with-boolean",
    matches: (op) =>
      op?.kind === "query" &&
      whereEntries(op.q).some(
        ([k, v]) => (k === "tdate" || k.endsWith(".tdate")) && v && typeof v === "object" && ["$gt", "$gte", "$lt", "$lte"].some((c) => typeof v[c] === "boolean"),
      ),
    legacy: { status: 500 },
    rust: { type: "validation-failed", status: 400 },
  },
];
const exceptionHits = {};
const allowedByException = (l, r) => {
  const op = script[l?.i];
  if (l?.i !== r?.i || canon(l?.q) !== canon(r?.q)) return null;
  const ex = EXCEPTIONS.find(
    (x) => x.matches(op) && canon(l.error) === canon(x.legacy) && canon(r.error) === canon(x.rust),
  );
  if (ex) exceptionHits[ex.name] = (exceptionHits[ex.name] ?? 0) + 1;
  return ex;
};

let mismatches = 0;
const n = Math.max(legacy.queryResults.length, rust.queryResults.length);
for (let i = 0; i < n; i++) {
  const l = legacy.queryResults[i], r = rust.queryResults[i];
  if (canon(comparable(l)) !== canon(comparable(r)) && !allowedByException(l, r)) {
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
// success floor: identical failure on both sides is not parity. Most of the
// script's transacts must have committed and the final full-table read must
// hold data, or the comparison compared nothing.
const txTotal = script.filter((op) => op.kind === "tx").length;
let floorOk = legacy.txOk > txTotal / 2 && rust.txOk > txTotal / 2 && (finalL?.result?.triples?.length ?? 0) > 0;
if (!floorOk) {
  console.error(`FUZZ FLOOR NOT MET: transact-ok legacy=${legacy.txOk} rust=${rust.txOk} of ${txTotal}, final triples=${finalL?.result?.triples?.length ?? 0}`);
}
// The same per grammar feature, once a script exercises it a few times:
// some transact that wrote it committed on both servers, and some query
// that reads it answered with data (both servers' answers are equal when
// the comparison above passed).
const queryFeatures = (op) => {
  const text = JSON.stringify(op.kind === "paginate" ? { [op.ns]: op.key } : op.q ?? null);
  const f = [];
  if (text.includes("tdate")) f.push("date");
  if (text.includes("tbool")) f.push("bool");
  // the tag and profile namespaces and the links to them
  if (text.includes("tag")) f.push("tags");
  if (text.includes("profile")) f.push("profile");
  if (/"\w+\.\w+\.\w+/.test(text)) f.push("hops");
  return f;
};
const floor = {};
const tally = (feature, key) => ((floor[feature] ??= { tx: 0, txOkLegacy: 0, txOkRust: 0, queries: 0, withData: 0 })[key] += 1);
for (const op of script) for (const f of op.features ?? []) tally(f, "tx");
for (const [f, k] of Object.entries(legacy.txOkBy)) floor[f].txOkLegacy = k;
for (const [f, k] of Object.entries(rust.txOkBy)) floor[f].txOkRust = k;
for (const entry of legacy.queryResults) {
  const op = script[entry.i];
  if (op.kind !== "query" && op.kind !== "paginate") continue;
  const data = entry.result ? entry.result.triples.length > 0 : (entry.union?.length ?? 0) > 0;
  for (const f of queryFeatures(op)) {
    tally(f, "queries");
    if (data) tally(f, "withData");
  }
}
for (const [f, c] of Object.entries(floor)) {
  const txFloor = c.tx < 5 || (c.txOkLegacy > 0 && c.txOkRust > 0);
  const queryFloor = c.queries < 10 || c.withData > 0;
  if (!txFloor || !queryFloor) {
    console.error(`FUZZ FLOOR NOT MET for ${f}: ${JSON.stringify(c)}`);
    floorOk = false;
  }
}
console.log(`grammar floor: ${JSON.stringify(floor)}`);
for (const [name, hits] of Object.entries(exceptionHits)) {
  console.log(`exception ${name}: ${hits} result(s) allowed`);
}
if (legacy.violations || rust.violations || !finalMatch || mismatches || !floorOk) {
  console.error(
    `FUZZ FAILED: ${mismatches} query mismatches, invariant violations legacy=${legacy.violations} rust=${rust.violations}, final match=${finalMatch}`,
  );
  process.exit(1);
}
console.log(`FUZZ PASSED: ${script.length} ops replayed identically (seed ${seed})`);
