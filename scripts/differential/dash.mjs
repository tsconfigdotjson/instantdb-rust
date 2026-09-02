// Differential replay for the instant-cli dashboard routes (issue #6):
// schema pull / steps apply / indexing-job polling / push plan+apply, perms
// pull / rules push, and their error matrix. The exact HTTP sequence the CLI
// performs (cli/src/lib/{pushSchema,pullSchema,pushPerms,pullPerms}.ts +
// old.js waitForIndexingJobsToFinish) is replayed against the legacy server
// and the rust server, responses are folded to what the CLI reads (with
// server-chosen ids/timestamps normalized), and the two must match.
//
// Usage: node dash.mjs <app-id> <admin-token> [<second-app-id> <second-token>]
//   (the app must exist on both servers with the same admin token, see
//    provision.sh; the optional second app exercises admin-token-mismatch)
// Env: LEGACY_URL (default http://localhost:8891), RUST_URL (default
//      http://localhost:8888), DUMP=1 prints raw responses, ONLY=legacy|rust

import fs from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { canon, makeIdFactory } from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appId = process.argv[2];
const adminToken = process.argv[3];
const otherAppId = process.argv[4];
const otherToken = process.argv[5];
if (!appId || !adminToken) throw new Error("usage: node dash.mjs <app-id> <admin-token> [<app2> <token2>]");

const SERVERS = {
  legacy: process.env.LEGACY_URL || "http://localhost:8891",
  rust: process.env.RUST_URL || "http://localhost:8888",
};

// ---------------------------------------------------------------------------
// http helpers (exactly the headers the CLI sends: lib/http.ts)

async function call(base, method, p, { token = adminToken, body, headers = {} } = {}) {
  const h = {
    "X-Instant-Source": "instant-cli",
    "X-Instant-Version": "v0.22.0",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(body !== undefined ? { "content-type": "application/json" } : {}),
    ...headers,
  };
  const res = await fetch(base + p, { method, headers: h, body: body !== undefined ? JSON.stringify(body) : undefined });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { "<non-json>": text.slice(0, 200) };
  }
  return { status: res.status, body: json };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// poll like old.js waitForIndexingJobsToFinish: until no job is waiting/processing
async function waitForJobs(base, groupId) {
  let last;
  for (let i = 0; i < 600; i++) {
    last = await call(base, "GET", `/dash/apps/${appId}/indexing-jobs/group/${groupId}`);
    const jobs = last.body?.jobs ?? [];
    if (!jobs.some((j) => j.job_status === "waiting" || j.job_status === "processing")) return last;
    await sleep(100);
  }
  return last;
}

// ---------------------------------------------------------------------------
// normalization: what the CLI reads, minus server-chosen values

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
let fixedPrefix = "";
const isFixed = (s) => typeof s === "string" && fixedPrefix && s.startsWith(fixedPrefix) && s.includes("-0000-4000-8000-");

function norm(v, key = null) {
  if (typeof v === "string") {
    if (isFixed(v)) return v;
    if (UUID_RE.test(v)) return "<uuid>";
    if (ISO_RE.test(v)) return "<ts>";
    // CEL compile diagnostics come from different CEL implementations
    if (key === "message" && /Validation failed for rule: /.test(v)) return v.replace(/: .*$/, ": <cel-messages>");
    return v.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, (m) => (isFixed(m) ? m : "<uuid>"));
  }
  if (typeof v === "number") return v > 1e12 ? "<ts>" : v;
  if (Array.isArray(v)) return v.map((x) => norm(x));
  if (v && typeof v === "object") {
    const out = {};
    for (const [k, val] of Object.entries(v)) {
      // legacy stamps trace-id on error bodies
      if (k === "trace-id") continue;
      out[k] = norm(val, k);
    }
    return out;
  }
  return v;
}

// the parts of an attr map the CLI reads (platform/src/api.ts + schema.ts)
function attrView(a) {
  return norm({
    id: a.id,
    "value-type": a["value-type"],
    cardinality: a.cardinality,
    "forward-identity": a["forward-identity"],
    "reverse-identity": a["reverse-identity"] ?? null,
    "unique?": a["unique?"],
    "index?": a["index?"],
    "required?": a["required?"] ?? false,
    "checked-data-type": a["checked-data-type"] ?? null,
    "on-delete": a["on-delete"] ?? null,
    "on-delete-reverse": a["on-delete-reverse"] ?? null,
    catalog: a.catalog,
    keys: Object.keys(a).sort(),
  });
}

function schemaView(schema) {
  const blobs = {};
  for (const [etype, attrs] of Object.entries(schema?.blobs ?? {})) {
    blobs[etype] = Object.fromEntries(Object.entries(attrs).map(([l, a]) => [l, attrView(a)]));
  }
  const refs = {};
  for (const [k, a] of Object.entries(schema?.refs ?? {})) refs[k] = attrView(a);
  return { blobs, refs };
}

function pullView(res) {
  if (res.status !== 200) return errView(res);
  const attrs = {};
  for (const a of res.body.attrs ?? []) attrs[`${a["forward-identity"][1]}.${a["forward-identity"][2]}`] = attrView(a);
  return { status: 200, schema: schemaView(res.body.schema), attrs, "app-title": res.body["app-title"], keys: Object.keys(res.body).sort() };
}

// error bodies: type/message/hint (http.ts reads message, hint.message, type)
function errView(res) {
  const b = res.body ?? {};
  const view = norm({ status: res.status, type: b.type, message: b.message, hint: b.hint ?? null, keys: Object.keys(b).sort() });
  // rule validation walks the rules map in key order, which differs between
  // Clojure hash maps and ours for larger maps; the CLI prints the joined
  // message, so compare the error set instead
  if (view.hint?.["data-type"] === "rule" && Array.isArray(view.hint.errors)) {
    view.hint.errors = [...view.hint.errors].sort((a, c) => (canon(a) < canon(c) ? -1 : 1));
    view.message = "Validation failed for rule: <sorted-below>";
  }
  return view;
}

function jobView(j) {
  return norm({
    ...j,
    // legacy fills these from its sketch-based estimator; the CLI only
    // uses them for a progress percentage
    work_estimate: j.work_estimate == null ? null : "<n>",
    work_completed: "<n?>",
    invalid_triples_sample: Array.isArray(j.invalid_triples_sample)
      ? j.invalid_triples_sample.map((t) => ({ ...t, entity_id: "<uuid>" })).sort((a, b) => (canon(a) < canon(b) ? -1 : 1))
      : j.invalid_triples_sample ?? null,
    error_data: j.error_data ? { ...j.error_data, "entity-ids": (j.error_data["entity-ids"] ?? []).map(() => "<uuid>") } : null,
    keys: Object.keys(j).sort(),
  });
}

function applyView(res) {
  if (res.status !== 200) return errView(res);
  const b = res.body;
  return {
    status: 200,
    keys: Object.keys(b).sort(),
    transactionKeys: Object.keys(b.transaction ?? {}).sort(),
    steps: norm(b.steps),
    "indexing-jobs": b["indexing-jobs"]
      ? { "group-id": "<uuid>", jobs: b["indexing-jobs"].jobs.map(jobView) }
      : null,
  };
}

function jobsView(res) {
  if (res.status !== 200) return errView(res);
  return { status: 200, jobs: (res.body.jobs ?? []).map(jobView).sort((a, b) => (canon(a) < canon(b) ? -1 : 1)) };
}

function planView(res) {
  if (res.status !== 200) return errView(res);
  const b = res.body;
  const attrs = {};
  for (const a of b["current-attrs"] ?? []) attrs[`${a["forward-identity"][1]}.${a["forward-identity"][2]}`] = attrView(a);
  return {
    status: 200,
    keys: Object.keys(b).sort(),
    "new-schema": schemaView(b["new-schema"]),
    "current-schema": schemaView(b["current-schema"]),
    "current-attrs": attrs,
    steps: norm(b.steps).sort((a, c) => (canon(a) < canon(c) ? -1 : 1)),
    ...(b.transaction !== undefined ? { transactionKeys: Object.keys(b.transaction ?? {}).sort() } : {}),
    ...(b["indexing-jobs"] !== undefined
      ? { "indexing-jobs": b["indexing-jobs"] ? { "group-id": "<uuid>", jobs: b["indexing-jobs"].jobs.map(jobView) } : null }
      : {}),
  };
}

function plainView(res) {
  if (res.status !== 200) return errView(res);
  return norm({ status: 200, body: res.body });
}

// ---------------------------------------------------------------------------
// scenario (deterministic ids so both servers get byte-identical input)

async function runAgainst(name) {
  const base = SERVERS[name];
  const mk = makeIdFactory(appId);
  fixedPrefix = appId.replaceAll("-", "").slice(0, 8);
  const ids = {
    postsId: mk(), postsTitle: mk(), postsViews: mk(), postsAuthor: mk(), postsAuthorRev: mk(),
    tagsId: mk(), tagsName: mk(), postsTags: mk(), postsTagsRev: mk(), postsSlug: mk(),
    p1: mk(), p2: mk(), p3: mk(), t1: mk(),
    postsBig: mk(), postsWhen: mk(), postsBody: null,
  };
  const out = {};
  const record = (k, v) => {
    out[k] = v;
    if (process.env.DUMP) console.log(`\n### [${name}] ${k}\n${JSON.stringify(v, null, 1).slice(0, 4000)}`);
    if (process.env.DUMP_DIR) fs.writeFileSync(path.join(process.env.DUMP_DIR, `${name}-${k}.json`), JSON.stringify(v, null, 1));
  };
  const raw = (k, res) => {
    if (process.env.DUMP) console.log(`\n### [${name}] RAW ${k}\n${JSON.stringify(res, null, 1).slice(0, 6000)}`);
    if (process.env.DUMP_DIR) fs.writeFileSync(path.join(process.env.DUMP_DIR, `${name}-RAW-${k}.json`), JSON.stringify(res, null, 1));
  };

  // 01 empty pull
  let res = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  raw("01-pull-empty", res);
  record("01-pull-empty", pullView(res));

  // 02 push: exactly the steps @instantdb/platform convertTxSteps emits for a
  // new schema (add-attr with flags false + unique/index/required job steps)
  const addAttr = (id, etype, label, extra = {}) => [
    "add-attr",
    {
      "forward-identity": [id, etype, label],
      "reverse-identity": null,
      "inferred-types": null,
      "value-type": "blob",
      id,
      cardinality: "one",
      "index?": false,
      "required?": false,
      "unique?": false,
      catalog: "user",
      "on-delete": undefined,
      "on-delete-reverse": undefined,
      "checked-data-type": undefined,
      ...extra,
    },
  ];
  const job = (op, id, etype, label, extra = {}) => [op, { "attr-id": id, "forward-identity": [id, etype, label], ...extra }];
  const steps02 = [
    addAttr(ids.postsId, "posts", "id"),
    job("unique", ids.postsId, "posts", "id"),
    job("index", ids.postsId, "posts", "id"),
    addAttr(ids.postsTitle, "posts", "title", { "checked-data-type": "string" }),
    job("index", ids.postsTitle, "posts", "title"),
    addAttr(ids.postsViews, "posts", "views"),
    addAttr(ids.tagsId, "tags", "id"),
    job("unique", ids.tagsId, "tags", "id"),
    job("index", ids.tagsId, "tags", "id"),
    addAttr(ids.tagsName, "tags", "name", { "checked-data-type": "string" }),
    job("unique", ids.tagsName, "tags", "name"),
    job("index", ids.tagsName, "tags", "name"),
    [
      "add-attr",
      {
        "forward-identity": [ids.postsAuthor, "posts", "author"],
        "reverse-identity": [ids.postsAuthorRev, "$users", "posts"],
        "inferred-types": null,
        "value-type": "ref",
        id: ids.postsAuthor,
        cardinality: "one",
        "index?": false,
        "required?": false,
        "unique?": false,
        catalog: "user",
        "on-delete": undefined,
        "on-delete-reverse": undefined,
        "checked-data-type": undefined,
      },
    ],
    [
      "add-attr",
      {
        "forward-identity": [ids.postsTags, "posts", "tags"],
        "reverse-identity": [ids.postsTagsRev, "tags", "posts"],
        "inferred-types": null,
        "value-type": "ref",
        id: ids.postsTags,
        cardinality: "many",
        "index?": false,
        "required?": false,
        "unique?": false,
        catalog: "user",
        "on-delete": undefined,
        "on-delete-reverse": "cascade",
        "checked-data-type": undefined,
      },
    ],
  ];
  res = await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: steps02 } });
  raw("02-apply", res);
  record("02-apply", applyView(res));
  let groupId = res.body?.["indexing-jobs"]?.["group-id"];
  if (groupId) {
    res = await waitForJobs(base, groupId);
    raw("03-jobs", res);
    record("03-jobs-done", jobsView(res));
  } else {
    record("03-jobs-done", { missing: true });
  }

  // 04 pull after push
  res = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  raw("04-pull", res);
  record("04-pull", pullView(res));

  // 05 seed data through the admin API (both servers; admin steps format,
  // posts.slug is auto-created for p1 only), then apply constraint jobs that
  // must fail: unique on duplicate titles, number type on a string, required
  // on an attr some entities lack
  const adminSteps = [
    ["update", "posts", ids.p1, { title: "hello", views: 1, slug: "hello-1" }],
    ["update", "posts", ids.p2, { title: "hello" }],
    ["update", "posts", ids.p3, { title: "other", views: "not-a-number" }],
  ];
  res = await fetch(`${base}/admin/transact`, {
    method: "POST",
    headers: { "content-type": "application/json", "app-id": appId, authorization: `Bearer ${adminToken}` },
    body: JSON.stringify({ steps: adminSteps }),
  });
  const seed = { status: res.status, body: await res.json() };
  raw("05-seed", seed);
  record("05-seed", { status: seed.status });
  res = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  // the auto-created attr has a server-chosen id on each server
  ids.postsSlug = res.body?.schema?.blobs?.posts?.slug?.id ?? ids.postsSlug;
  record("05-pull-after-seed", pullView(res));

  const steps06 = [
    job("unique", ids.postsTitle, "posts", "title"),
    job("check-data-type", ids.postsViews, "posts", "views", { "checked-data-type": "number" }),
    job("required", ids.postsSlug, "posts", "slug"),
    job("index", ids.postsSlug, "posts", "slug"),
    job("remove-index", ids.postsTitle, "posts", "title"),
    job("required", ids.postsTitle, "posts", "title"),
    job("remove-data-type", ids.postsTitle, "posts", "title"),
  ];
  res = await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: steps06 } });
  raw("06-apply-constraints", res);
  record("06-apply-constraints", applyView(res));
  groupId = res.body?.["indexing-jobs"]?.["group-id"];
  res = await waitForJobs(base, groupId);
  raw("07-jobs", res);
  record("07-jobs-mixed", jobsView(res));
  res = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  record("08-pull-after-jobs", pullView(res));

  // 09 update-attr (rename) + delete-attr, as the CLI sends them
  const steps09 = [
    ["update-attr", { id: ids.postsViews, "forward-identity": [ids.postsViews, "posts", "viewCount"] }],
    ["delete-attr", ids.postsSlug],
  ];
  res = await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: steps09 } });
  raw("09-apply-rename-delete", res);
  record("09-apply-rename-delete", applyView(res));
  res = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  record("10-pull-after-rename", pullView(res));

  // 11 server-side planning (push/plan, push/apply)
  const defs = {
    entities: {
      posts: { attrs: { title: { valueType: "string", config: { unique: false, indexed: true }, required: true }, viewCount: { valueType: "number", config: { unique: false, indexed: false } }, body: { valueType: "string", config: { unique: false, indexed: false } } } },
      tags: { attrs: { name: { valueType: "string", config: { unique: true, indexed: true } } } },
      comments: { attrs: { text: { valueType: "string", config: { unique: false, indexed: false } } } },
      $users: { attrs: { email: { valueType: "string", config: { unique: true, indexed: true } }, nickname: { valueType: "string", config: { unique: false, indexed: false } } } },
    },
    links: {
      postsAuthor: { forward: { on: "posts", has: "one", label: "author" }, reverse: { on: "$users", has: "many", label: "posts" } },
      postsTags: { forward: { on: "posts", has: "many", label: "tags" }, reverse: { on: "tags", has: "many", label: "posts", onDelete: "cascade" } },
      postsComments: { forward: { on: "posts", has: "many", label: "comments" }, reverse: { on: "comments", has: "one", label: "post", onDelete: "cascade" } },
    },
  };
  res = await call(base, "POST", `/dash/apps/${appId}/schema/push/plan`, { body: { schema: defs, check_types: true, supports_background_updates: true } });
  raw("11-plan", res);
  record("11-plan", planView(res));
  res = await call(base, "POST", `/dash/apps/${appId}/schema/push/plan`, { body: { schema: defs, check_types: true, supports_background_updates: false } });
  record("12-plan-sync", planView(res));
  // plan errors: backwards link + cascade on many
  const badDefs = {
    entities: { posts: { attrs: { title: { valueType: "string", config: { unique: false, indexed: true }, required: true } } }, tags: { attrs: {} } },
    links: {
      tagsPosts: { forward: { on: "tags", has: "many", label: "posts" }, reverse: { on: "posts", has: "many", label: "tags" } },
      postsRelated: { forward: { on: "posts", has: "many", label: "related", onDelete: "cascade" }, reverse: { on: "posts", has: "many", label: "relatedBy" } },
    },
  };
  res = await call(base, "POST", `/dash/apps/${appId}/schema/push/plan`, { body: { schema: badDefs, check_types: true, supports_background_updates: true } });
  raw("13-plan-errors", res);
  record("13-plan-errors", errView(res));
  res = await call(base, "POST", `/dash/apps/${appId}/schema/push/apply`, { body: { schema: defs, check_types: true, supports_background_updates: true } });
  raw("14-push-apply", res);
  record("14-push-apply", planView(res));
  groupId = res.body?.["indexing-jobs"]?.["group-id"];
  if (groupId) {
    res = await waitForJobs(base, groupId);
    record("15-push-apply-jobs", jobsView(res));
  }
  res = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  record("16-pull-final", pullView(res));
  ids.postsBody = res.body?.schema?.blobs?.posts?.body?.id ?? ids.t1;

  // 16b async indexing jobs, the paths issue #5 adds on top of step 06:
  // values too large for an index (unique + index abort with
  // triple-too-large-error), remove-unique / re-unique, remove-required (no
  // work estimate) / re-required, a date type check failing then passing,
  // remove-data-type, and required with explicit nulls. After each group
  // the pulled attrs must carry no in-flight markers.
  // incompressible: TOAST would shrink a repeated character under the
  // 1024-byte index limit (legacy's test uses random uuids too)
  let bigValue = "";
  for (let h = appId; bigValue.length < 3000; ) {
    h = createHash("sha256").update(h).digest("hex");
    bigValue += h;
  }
  const seed2 = [
    addAttr(ids.postsBig, "posts", "big"),
    addAttr(ids.postsWhen, "posts", "when"),
  ];
  res = await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: seed2 } });
  record("16b-add-attrs", applyView(res));
  res = await fetch(`${base}/admin/transact`, {
    method: "POST",
    headers: { "content-type": "application/json", "app-id": appId, authorization: `Bearer ${adminToken}` },
    body: JSON.stringify({
      steps: [
        ["update", "posts", ids.p1, { big: bigValue, when: "2024-01-01T00:00:00Z", body: "x" }],
        ["update", "posts", ids.p2, { when: 1700000000000, body: null }],
        ["update", "posts", ids.p3, { when: "not a date" }],
      ],
    }),
  });
  record("16b-seed", { status: res.status });
  const steps16c = [
    job("unique", ids.postsBig, "posts", "big"),
    job("index", ids.postsBig, "posts", "big"),
    job("remove-unique", ids.tagsName, "tags", "name"),
    job("remove-required", ids.postsTitle, "posts", "title"),
    job("check-data-type", ids.postsWhen, "posts", "when", { "checked-data-type": "date" }),
    job("required", ids.postsBody, "posts", "body"),
  ];
  res = await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: steps16c } });
  raw("16c-apply", res);
  record("16c-apply", applyView(res));
  groupId = res.body?.["indexing-jobs"]?.["group-id"];
  res = await waitForJobs(base, groupId);
  raw("16d-jobs", res);
  record("16d-jobs-errors", jobsView(res));
  res = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  record("16e-pull-after-errors", pullView(res));
  // fix the bad date, then the second round must complete
  res = await fetch(`${base}/admin/transact`, {
    method: "POST",
    headers: { "content-type": "application/json", "app-id": appId, authorization: `Bearer ${adminToken}` },
    body: JSON.stringify({ steps: [["update", "posts", ids.p3, { when: "2024-03-03T00:00:00Z" }]] }),
  });
  record("16f-fix", { status: res.status });
  const steps16g = [
    job("unique", ids.tagsName, "tags", "name"),
    job("required", ids.postsTitle, "posts", "title"),
    job("check-data-type", ids.postsWhen, "posts", "when", { "checked-data-type": "date" }),
    job("index", ids.postsWhen, "posts", "when"),
  ];
  res = await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: steps16g } });
  record("16g-apply", applyView(res));
  groupId = res.body?.["indexing-jobs"]?.["group-id"];
  res = await waitForJobs(base, groupId);
  raw("16h-jobs", res);
  record("16h-jobs-completed", jobsView(res));
  res = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  record("16i-pull-after-completed", pullView(res));
  const steps16j = [job("remove-data-type", ids.postsWhen, "posts", "when"), job("remove-index", ids.postsWhen, "posts", "when")];
  res = await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: steps16j } });
  record("16j-apply", applyView(res));
  groupId = res.body?.["indexing-jobs"]?.["group-id"];
  res = await waitForJobs(base, groupId);
  record("16k-jobs-removed", jobsView(res));
  res = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  record("16l-pull-after-removed", pullView(res));

  // 17 perms
  res = await call(base, "GET", `/dash/apps/${appId}/perms/pull`);
  raw("17-perms-empty", res);
  record("17-perms-empty", plainView(res));
  const rules = {
    posts: { bind: ["isAuthor", "auth.id != null && auth.id in data.ref('author.id')"], allow: { view: "true", create: "isAuthor", update: "isAuthor", delete: "false" } },
    $users: { allow: { view: "auth.id == data.id", delete: "false" } },
    $default: { bind: { isAdmin: "auth.email == 'admin@example.com'" }, allow: { $default: "isAdmin" } },
    $rateLimits: { burst: { limits: [{ capacity: 10, refill: { period: "1 hour", amount: 10, type: "interval" } }] } },
  };
  res = await call(base, "POST", `/dash/apps/${appId}/rules`, { body: { code: rules } });
  raw("18-rules-push", res);
  record("18-rules-push", plainView(res));
  res = await call(base, "GET", `/dash/apps/${appId}/perms/pull`);
  record("19-perms-pull", plainView(res));
  res = await call(base, "POST", `/dash/apps/${appId}/rules`, { body: { code: rules } });
  raw("20-rules-push-same", res);
  record("20-rules-push-same", plainView(res));
  const badRules = {
    posts: { bind: ["a", "true", "b"], allow: { view: "true" } },
    docs: { bind: ["x", "true", "x", "false"], allow: { view: "auth.id ==", create: 42 }, fields: { id: "true" } },
    $magicCodes: { allow: { view: "true" } },
    $users: { allow: { delete: "auth.id == data.id" } },
    $rateLimits: { a: {}, b: { limits: [{ capacity: 0 }] }, c: { limits: [{ capacity: 5, refill: { period: "3 days" } }] } },
  };
  res = await call(base, "POST", `/dash/apps/${appId}/rules`, { body: { code: badRules } });
  raw("21-rules-invalid", res);
  record("21-rules-invalid", errView(res));
  res = await call(base, "POST", `/dash/apps/${appId}/rules`, { body: {} });
  raw("22-rules-missing-code", res);
  record("22-rules-missing-code", errView(res));

  // 23 error matrix
  const errs = {};
  errs.noAuth = errView(await call(base, "GET", `/dash/apps/${appId}/schema/pull`, { token: null }));
  errs.badBearer = errView(await call(base, "GET", `/dash/apps/${appId}/schema/pull`, { token: "not-a-token" }));
  errs.unknownUuid = errView(await call(base, "GET", `/dash/apps/${appId}/schema/pull`, { token: "11111111-2222-4333-8444-555555555555" }));
  errs.badAppId = errView(await call(base, "GET", `/dash/apps/nope/schema/pull`));
  if (otherAppId && otherToken) {
    errs.mismatch = errView(await call(base, "GET", `/dash/apps/${appId}/schema/pull`, { token: otherToken }));
  }
  errs.stepsMissing = errView(await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: {} }));
  errs.stepsNotColl = errView(await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: "nope" } }));
  errs.stepNotColl = errView(await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: [["add-attr", {}], 5] } }));
  errs.badAttr = errView(await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: [["add-attr", { id: "x" }]] } }));
  errs.deleteUnknown = errView(await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: [["delete-attr", ids.t1]] } }));
  errs.unknownGroup = jobsView(await call(base, "GET", `/dash/apps/${appId}/indexing-jobs/group/${ids.t1}`));
  errs.unknownJob = errView(await call(base, "GET", `/dash/apps/${appId}/indexing-jobs/${ids.t1}`));
  errs.cliVersion = plainView(await call(base, "GET", `/dash/cli/version`, { token: null }));
  errs.permsNoAuth = errView(await call(base, "GET", `/dash/apps/${appId}/perms/pull`, { token: null }));
  raw("23-errors", errs);
  record("23-errors", errs);

  // 24 admin transact ref lookups (issue #10): `lookup("owner.id", <uuid>)`
  // names the unique forward link `<etype>.owner` and matches on the linked
  // entity's id (admin/model.clj extract-lookup); a missing one is
  // auto-created as a unique cardinality-one link (add-attrs-for-ref-lookup)
  const adminCall = (method, p, body, extra = {}) =>
    call(base, method, p, { body, headers: { "app-id": appId, ...extra } });
  const lookup = (attr, value) => `lookup__${attr}__${JSON.stringify(value)}`;
  const txView = (res) => (res.status === 200 ? { status: 200, keys: Object.keys(res.body).sort() } : errView(res));
  const queryView = (res) => {
    if (res.status !== 200) return errView(res);
    const rows = (res.body.docs ?? []).map((d) => norm(d)).sort((a, c) => (canon(a) < canon(c) ? -1 : 1));
    return { status: 200, docs: rows };
  };
  const ids24 = { owner1: mk(), owner2: mk(), doc1: mk() };
  const r24 = {};
  // a plain (non-unique, cardinality-many) link and an owner to look up
  r24.seed = txView(await adminCall("POST", "/admin/transact", { steps: [
    ["update", "owners", ids24.owner1, { name: "ann" }],
    ["update", "owners", ids24.owner2, { name: "bo" }],
    ["update", "docs", ids24.doc1, { title: "d1" }],
    ["link", "docs", ids24.doc1, { owner: ids24.owner1 }],
  ] }));
  // ref lookup on a fresh label: auto-creates docs.primaryOwner (ref/one/unique) and upserts
  r24.createByRefLookup = txView(await adminCall("POST", "/admin/transact", { steps: [
    ["update", "docs", lookup("primaryOwner.id", ids24.owner1), { title: "d2" }],
  ] }));
  r24.updateByRefLookup = txView(await adminCall("POST", "/admin/transact", { steps: [
    ["update", "docs", lookup("primaryOwner.id", ids24.owner1), { title: "d2-renamed" }],
    ["merge", "docs", lookup("primaryOwner.id", ids24.owner1), { meta: { k: 1 } }],
  ] }));
  // link + delete through the same lookup
  r24.linkByRefLookup = txView(await adminCall("POST", "/admin/transact", { steps: [
    ["link", "docs", lookup("primaryOwner.id", ids24.owner1), { owner: ids24.owner2 }],
  ] }));
  r24.query = queryView(await adminCall("POST", "/admin/query", { query: { docs: { primaryOwner: {}, owner: {} } } }));
  // error matrix
  r24.notUnique = txView(await adminCall("POST", "/admin/transact", { steps: [
    ["update", "docs", lookup("owner.id", ids24.owner1), { title: "x" }],
  ] }));
  r24.badPath = txView(await adminCall("POST", "/admin/transact", { steps: [
    ["update", "docs", lookup("primaryOwner.name", ids24.owner1), { title: "x" }],
  ] }));
  r24.tooDeep = txView(await adminCall("POST", "/admin/transact", { steps: [
    ["update", "docs", lookup("primaryOwner.id.id", ids24.owner1), { title: "x" }],
  ] }));
  r24.missingStrict = txView(await adminCall("POST", "/admin/transact", { steps: [
    ["update", "docs", lookup("editor.id", ids24.owner1), { title: "x" }],
  ], "throw-on-missing-attrs?": true }));
  r24.deleteByRefLookup = txView(await adminCall("POST", "/admin/transact", { steps: [
    ["delete", "docs", lookup("primaryOwner.id", ids24.owner1)],
  ] }));
  r24.queryAfterDelete = queryView(await adminCall("POST", "/admin/query", { query: { docs: { primaryOwner: {} } } }));
  const pulled = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
  r24.primaryOwnerAttr = pulled.status === 200
    ? attrView(pulled.body.attrs.find((a) => a["forward-identity"][1] === "docs" && a["forward-identity"][2] === "primaryOwner") ?? {})
    : errView(pulled);
  raw("24-admin-ref-lookups", r24);
  record("24-admin-ref-lookups", r24);

  return out;
}

// ---------------------------------------------------------------------------
// diff + allowlist

const allowlist = JSON.parse(fs.readFileSync(path.join(here, "allowed-divergences.json"), "utf8"));
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

function firstDiff(a, b, p = "") {
  if (canon(a) === canon(b)) return null;
  const isObj = (v) => v !== null && typeof v === "object";
  if (isObj(a) && isObj(b) && Array.isArray(a) === Array.isArray(b)) {
    for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
      const d = firstDiff(a?.[k], b?.[k], `${p}.${k}`);
      if (d) return d;
    }
  }
  return { p, a, b };
}

const only = process.env.ONLY;
const results = {};
for (const name of only ? [only] : ["legacy", "rust"]) {
  console.log(`replaying dash routes against ${name}…`);
  results[name] = await runAgainst(name);
}
if (only) process.exit(0);

const diffs = [];
for (const step of new Set([...Object.keys(results.legacy), ...Object.keys(results.rust)])) {
  const l = results.legacy[step];
  const r = results.rust[step];
  // descend one level so allowlist paths can name a sub-key
  const keys = l && r && typeof l === "object" && !Array.isArray(l) ? new Set([...Object.keys(l), ...Object.keys(r ?? {})]) : null;
  if (keys) {
    for (const k of keys) {
      if (canon(l[k]) !== canon(r?.[k])) {
        const p = `dash:${step}/${k}`;
        diffs.push({ path: p, allowed: allowed(p), legacy: l[k], rust: r?.[k] });
      }
    }
  } else if (canon(l) !== canon(r)) {
    const p = `dash:${step}`;
    diffs.push({ path: p, allowed: allowed(p), legacy: l, rust: r });
  }
}
const blocking = diffs.filter((d) => !d.allowed);
for (const d of diffs) {
  console.log(`\n[${d.allowed ? "ALLOWED" : "DIVERGENCE"}] ${d.path}`);
  const fd = firstDiff(d.legacy, d.rust);
  if (fd && fd.p) {
    console.log(`  first differing sub-path: ${fd.p}`);
    console.log("  legacy:", JSON.stringify(fd.a)?.slice(0, 1500));
    console.log("  rust:  ", JSON.stringify(fd.b)?.slice(0, 1500));
  } else {
    console.log("  legacy:", JSON.stringify(d.legacy)?.slice(0, 1500));
    console.log("  rust:  ", JSON.stringify(d.rust)?.slice(0, 1500));
  }
}
if (blocking.length) {
  console.error(`\nDASH DIFFERENTIAL FAILED: ${blocking.length} unallowed divergences`);
  process.exit(1);
}
console.log(`\nDASH DIFFERENTIAL PASSED (${diffs.length} allowed divergences, ${Object.keys(results.legacy).length} steps)`);
