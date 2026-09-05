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
import { execSync } from "node:child_process";
import { createHash } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { canon, connectSse, makeIdFactory } from "./lib.mjs";

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
const DBS = {
  legacy: process.env.LEGACY_DATABASE_URL || "postgres://instant:instant@localhost:8890/instant",
  rust: process.env.RUST_DATABASE_URL || "postgres://instant:instant@localhost:5432/instant",
};
// the dashboard login code: neither server has a mail provider in the
// harness, so it is read back from instant_user_magic_codes
function readDashCode(name, email) {
  return execSync(
    `psql "${DBS[name]}" -At -c "SELECT c.code FROM instant_user_magic_codes c JOIN instant_users u ON u.id = c.user_id WHERE u.email = '${email}' ORDER BY c.created_at DESC LIMIT 1"`,
    { encoding: "utf8" },
  ).trim();
}

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

  // 25 admin SSE transports (issue #8): the HTTP side of subscribe-query /
  // sse / sse/push — auth and parameter errors before a stream opens, and
  // the push envelope checks (admin/routes.clj:160-210, session.clj:1250-1261).
  // Frames on an open stream are covered by replay.mjs steps 23-24.
  const r25 = {};
  const sseCall = (p, body, extra = {}) =>
    call(base, "POST", p, { body, headers: { "app-id": appId, ...extra } });
  r25.subscribeNoQuery = errView(await sseCall(`/admin/subscribe-query?local_connection_id=${ids.t1}`, { "inference?": true }));
  r25.subscribeQueryNotMap = errView(await sseCall(`/admin/subscribe-query?local_connection_id=${ids.t1}`, { query: [1] }));
  r25.subscribeBadToken = errView(await call(base, "POST", `/admin/subscribe-query?local_connection_id=${ids.t1}`, {
    token: ids.p1, body: { query: { posts: {} } }, headers: { "app-id": appId },
  }));
  r25.subscribeNoAppId = errView(await call(base, "POST", `/admin/subscribe-query?local_connection_id=${ids.t1}`, { body: { query: { posts: {} } } }));
  r25.sseBadToken = errView(await call(base, "POST", `/admin/sse?app_id=${appId}`, { token: ids.p1, body: { "inference?": false } }));
  r25.sseUnknownAsEmail = errView(await sseCall(`/admin/sse?app_id=${appId}`, { "inference?": false }, { "as-email": "nobody@example.com" }));
  // push envelope: every field is a required uuid; messages must be present
  const push = (body, qs = `?app_id=${appId}`) => call(base, "POST", `/admin/sse/push${qs}`, { token: null, body });
  r25.pushEmpty = errView(await push({}));
  r25.pushMalformedMachine = errView(await push({ machine_id: "nope" }));
  r25.pushNoAppId = errView(await push({ machine_id: ids.p1 }, ""));
  r25.pushNoSession = errView(await push({ machine_id: ids.p1 }));
  r25.pushNoToken = errView(await push({ machine_id: ids.p1, session_id: ids.p2 }));
  r25.pushNoMessages = errView(await push({ machine_id: ids.p1, session_id: ids.p2, sse_token: ids.p3 }));
  // a live session: wrong token and unknown session are both "session missing";
  // an unknown machine id is "member missing"
  const live = connectSse(base, appId, `${name}:dash-sse`, {
    path: `/admin/sse?app_id=${appId}`,
    headers: { "app-id": appId, authorization: `Bearer ${adminToken}` },
    body: { "inference?": false },
  });
  await live.open;
  const env = { machine_id: live.init["machine-id"], session_id: live.init["session-id"], sse_token: live.init["sse-token"] };
  r25.pushWrongToken = errView(await push({ ...env, sse_token: ids.p3, messages: [] }));
  r25.pushUnknownSession = errView(await push({ ...env, session_id: ids.p2, messages: [] }));
  r25.pushUnknownMachine = errView(await push({ ...env, machine_id: ids.p1, session_id: ids.p2, messages: [] }));
  r25.pushOk = plainView(await push({ ...env, messages: [] }));
  live.close();
  raw("25-admin-sse", r25);
  record("25-admin-sse", r25);

  // 26+ the CLI's app / info / claim / auth / email routes (issue #29 item
  // 6). Routes without an app id take the creator's dashboard refresh
  // token (DASH_USER_TOKEN, provisioned on both servers); per-app routes
  // accept it or the admin token.
  const userToken = process.env.DASH_USER_TOKEN;
  if (userToken) {
    const okKeys = (res, pick = (b) => b) => (res.status === 200 ? { status: 200, keys: Object.keys(pick(res.body) ?? {}).sort() } : errView(res));
    // 26 me + apps list
    const r26 = {};
    const me = await call(base, "GET", "/dash/me", { token: userToken });
    r26.me = me.status === 200 ? { status: 200, keys: Object.keys(me.body.user).sort(), email: me.body.user.email, id: norm(me.body.user.id) } : errView(me);
    r26.meAdminToken = errView(await call(base, "GET", "/dash/me"));
    r26.meNoAuth = errView(await call(base, "GET", "/dash/me", { token: null }));
    const dash = await call(base, "GET", "/dash", { token: userToken });
    // the app rows: what the CLI reads plus the members / rules shape; key
    // sets are not compared because the legacy image runs migrations newer
    // than the vendored source (extra apps columns)
    const appView = (a) => norm({ id: a.id, title: a.title, user_app_role: a.user_app_role, admin_token: a.admin_token, status: a.status, effective_status: a.effective_status, org: a.org, rules: a.rules, members: a.members, invites: a.invites, creator_id: a.creator_id, org_id: a.org_id, deletion_marked_at: a.deletion_marked_at });
    r26.dash = dash.status === 200
      ? { status: 200, user: norm(dash.body.user), apps: (dash.body.apps ?? []).map(appView).sort((a, c) => (canon(a) < canon(c) ? -1 : 1)) }
      : errView(dash);
    raw("26-me-and-dash", r26);
    record("26-me-and-dash", r26);

    // 27 create / get / delete an app the way `instant-cli init` does
    const r27 = {};
    const newApp = mk();
    const newToken = mk();
    const created = await call(base, "POST", "/dash/apps", { token: userToken, body: { id: newApp, title: "cli app", admin_token: newToken } });
    r27.create = created.status === 200 ? { status: 200, keys: Object.keys(created.body).sort(), app: norm({ id: created.body.app?.id, title: created.body.app?.title, "admin-token": created.body.app?.["admin-token"], status: created.body.app?.status, creator_id: created.body.app?.creator_id, org_id: created.body.app?.org_id }) } : errView(created);
    r27.createMissingTitle = errView(await call(base, "POST", "/dash/apps", { token: userToken, body: { id: mk(), admin_token: mk() } }));
    r27.createBlankTitle = errView(await call(base, "POST", "/dash/apps", { token: userToken, body: { id: mk(), title: "  ", admin_token: mk() } }));
    r27.createBadId = errView(await call(base, "POST", "/dash/apps", { token: userToken, body: { id: "nope", title: "x", admin_token: mk() } }));
    r27.createWithAdminToken = errView(await call(base, "POST", "/dash/apps", { body: { id: mk(), title: "x", admin_token: mk() } }));
    r27.createBadRules = errView(await call(base, "POST", "/dash/apps", { token: userToken, body: { id: mk(), title: "x", admin_token: mk(), rules: { code: { posts: { allow: { view: "auth.id ==" } } } } } }));
    const withRules = mk();
    const cw = await call(base, "POST", "/dash/apps", { token: userToken, body: { id: withRules, title: "with rules + schema", admin_token: mk(), rules: { code: { posts: { allow: { view: "true" } } } }, schema: { entities: { posts: { title: { valueType: "string", config: { indexed: false, unique: false } } } }, links: {} } } });
    r27.createWithRulesAndSchema = cw.status === 200 ? { status: 200, keys: Object.keys(cw.body).sort() } : errView(cw);
    r27.pullAfterCreate = pullView(await call(base, "GET", `/dash/apps/${withRules}/schema/pull`, { token: userToken }));
    r27.permsAfterCreate = plainView(await call(base, "GET", `/dash/apps/${withRules}/perms/pull`, { token: userToken }));
    const got = await call(base, "GET", `/dash/apps/${newApp}`, { token: newToken });
    r27.getWithAdminToken = got.status === 200 ? { status: 200, keys: Object.keys(got.body).sort(), app: norm({ id: got.body.app.id, title: got.body.app.title, status: got.body.app.status, creator_id: got.body.app.creator_id }) } : errView(got);
    const gotUser = await call(base, "GET", `/dash/apps/${newApp}`, { token: userToken });
    r27.getWithUserToken = gotUser.status === 200 ? { status: 200, title: gotUser.body.app.title } : errView(gotUser);
    r27.getWrongAdminToken = errView(await call(base, "GET", `/dash/apps/${newApp}`));
    r27.getUnknown = errView(await call(base, "GET", `/dash/apps/${mk()}`, { token: userToken }));
    r27.deleteWithAdminToken = errView(await call(base, "DELETE", `/dash/apps/${newApp}`, { token: newToken }));
    r27.delete = plainView(await call(base, "DELETE", `/dash/apps/${newApp}`, { token: userToken }));
    r27.getAfterDelete = errView(await call(base, "GET", `/dash/apps/${newApp}`, { token: userToken }));
    r27.deleteAgain = errView(await call(base, "DELETE", `/dash/apps/${newApp}`, { token: userToken }));
    const dash2 = await call(base, "GET", "/dash", { token: userToken });
    r27.listAfter = dash2.status === 200 ? { status: 200, titles: (dash2.body.apps ?? []).map((a) => a.title).sort() } : errView(dash2);
    raw("27-app-create-get-delete", r27);
    record("27-app-create-get-delete", r27);

    // 28 orgs (the CLI's app picker lists an org's apps)
    const r28 = {};
    const org = await call(base, "POST", "/dash/orgs", { token: userToken, body: { title: "cli org" } });
    r28.create = org.status === 200 ? { status: 200, keys: Object.keys(org.body).sort(), orgKeys: Object.keys(org.body.org ?? {}).sort(), title: org.body.org?.title } : errView(org);
    r28.createMissingTitle = errView(await call(base, "POST", "/dash/orgs", { token: userToken, body: {} }));
    const orgId = org.body?.org?.id;
    const orgGet = await call(base, "GET", `/dash/orgs/${orgId}`, { token: userToken });
    r28.get = orgGet.status === 200
      ? { status: 200, keys: Object.keys(orgGet.body).sort(), org: norm({ id: orgGet.body.org.id, title: orgGet.body.org.title, role: orgGet.body.org.role }), members: (orgGet.body.members ?? []).map((m) => ({ email: m.email, role: m.role })), apps: orgGet.body.apps ?? [] }
      : errView(orgGet);
    const orgApp = mk();
    const oa = await call(base, "POST", "/dash/apps", { token: userToken, body: { id: orgApp, title: "org app", admin_token: mk(), org_id: orgId } });
    r28.createOrgApp = oa.status === 200 ? { status: 200, app: norm({ id: oa.body.app?.id, org_id: oa.body.app?.org_id, creator_id: oa.body.app?.creator_id }) } : errView(oa);
    const orgGet2 = await call(base, "GET", `/dash/orgs/${orgId}`, { token: userToken });
    r28.getWithApp = orgGet2.status === 200 ? { status: 200, apps: (orgGet2.body.apps ?? []).map((a) => norm({ id: a.id, title: a.title, org: a.org, user_app_role: a.user_app_role })) } : errView(orgGet2);
    r28.getUnknown = errView(await call(base, "GET", `/dash/orgs/${mk()}`, { token: userToken }));
    r28.getWithAdminToken = errView(await call(base, "GET", `/dash/orgs/${orgId}`));
    r28.delete = plainView(await call(base, "DELETE", `/dash/orgs/${orgId}`, { token: userToken }));
    r28.getAfterDelete = errView(await call(base, "GET", `/dash/orgs/${orgId}`, { token: userToken }));
    raw("28-orgs", r28);
    record("28-orgs", r28);

    // 29 OAuth configuration: providers, clients (github needs no discovery
    // endpoint), authorized redirect origins, and the /auth summary
    const r29 = {};
    const authView = (res) => {
      if (res.status !== 200) return errView(res);
      const b = res.body;
      return {
        status: 200,
        keys: Object.keys(b).sort(),
        providers: (b.oauth_service_providers ?? []).map((p) => norm({ id: p.id, provider_name: p.provider_name, keys: Object.keys(p).sort() })),
        clients: (b.oauth_clients ?? []).map((c) => norm({ id: c.id, client_name: c.client_name, client_id: c.client_id, provider_id: c.provider_id, meta: c.meta, discovery_endpoint: c.discovery_endpoint, redirect_to: c.redirect_to, use_shared_credentials: c.use_shared_credentials, keys: Object.keys(c).sort() })),
        origins: (b.authorized_redirect_origins ?? []).map((o) => norm({ id: o.id, service: o.service, params: o.params, keys: Object.keys(o).sort() })),
        originsNull: b.authorized_redirect_origins === null,
      };
    };
    r29.authEmpty = authView(await call(base, "GET", `/dash/apps/${appId}/auth`));
    const prov = await call(base, "POST", `/dash/apps/${appId}/oauth_service_providers`, { body: { provider_name: "github" } });
    r29.providerCreate = prov.status === 200 ? { status: 200, keys: Object.keys(prov.body.provider ?? {}).sort(), provider_name: prov.body.provider?.provider_name } : errView(prov);
    r29.providerMissingName = errView(await call(base, "POST", `/dash/apps/${appId}/oauth_service_providers`, { body: {} }));
    const providerId = prov.body?.provider?.id;
    const client = await call(base, "POST", `/dash/apps/${appId}/oauth_clients`, { body: { provider_id: providerId, client_name: "gh", client_id: "gh-client", client_secret: "gh-secret", meta: { providerName: "github" } } });
    r29.clientCreate = client.status === 200 ? { status: 200, keys: Object.keys(client.body.client ?? {}).sort(), client: norm({ client_name: client.body.client?.client_name, client_id: client.body.client?.client_id, provider_id: client.body.client?.provider_id, meta: client.body.client?.meta, discovery_endpoint: client.body.client?.discovery_endpoint, use_shared_credentials: client.body.client?.use_shared_credentials }) } : errView(client);
    r29.clientUnknownProvider = errView(await call(base, "POST", `/dash/apps/${appId}/oauth_clients`, { body: { provider_id: mk(), client_name: "x", client_id: "a", client_secret: "b" } }));
    r29.clientMissingName = errView(await call(base, "POST", `/dash/apps/${appId}/oauth_clients`, { body: { provider_id: providerId, client_id: "a" } }));
    r29.clientBadRedirect = errView(await call(base, "POST", `/dash/apps/${appId}/oauth_clients`, { body: { provider_id: providerId, client_name: "y", client_id: "a", client_secret: "b", redirect_to: "http://example.com/#frag" } }));
    const clientId = client.body?.client?.id;
    const upd = await call(base, "POST", `/dash/apps/${appId}/oauth_clients/${clientId}`, { body: { meta: { extra: 1 }, redirect_to: "https://example.com/cb", client_id: "gh-client-2" } });
    r29.clientUpdate = upd.status === 200 ? { status: 200, keys: Object.keys(upd.body.client ?? {}).sort(), client: norm({ client_id: upd.body.client?.client_id, meta: upd.body.client?.meta, redirect_to: upd.body.client?.redirect_to }) } : errView(upd);
    r29.clientUpdateBadRedirect = errView(await call(base, "POST", `/dash/apps/${appId}/oauth_clients/${clientId}`, { body: { redirect_to: "https://user:pw@example.com/cb" } }));
    const origin = await call(base, "POST", `/dash/apps/${appId}/authorized_redirect_origins`, { body: { service: "generic", params: ["example.com"] } });
    r29.originCreate = origin.status === 200 ? { status: 200, keys: Object.keys(origin.body.origin ?? {}).sort(), origin: norm({ service: origin.body.origin?.service, params: origin.body.origin?.params }) } : errView(origin);
    r29.originBadArity = errView(await call(base, "POST", `/dash/apps/${appId}/authorized_redirect_origins`, { body: { service: "vercel", params: ["only-one"] } }));
    r29.originBadService = errView(await call(base, "POST", `/dash/apps/${appId}/authorized_redirect_origins`, { body: { service: "gopher", params: ["x"] } }));
    r29.originReservedScheme = errView(await call(base, "POST", `/dash/apps/${appId}/authorized_redirect_origins`, { body: { service: "custom-scheme", params: ["https"] } }));
    r29.originMissingParams = errView(await call(base, "POST", `/dash/apps/${appId}/authorized_redirect_origins`, { body: { service: "generic" } }));
    r29.auth = authView(await call(base, "GET", `/dash/apps/${appId}/auth`, { token: userToken }));
    r29.originDelete = okKeys(await call(base, "DELETE", `/dash/apps/${appId}/authorized_redirect_origins/${origin.body?.origin?.id}`), (b) => b.origin);
    r29.originDeleteUnknown = errView(await call(base, "DELETE", `/dash/apps/${appId}/authorized_redirect_origins/${mk()}`));
    r29.clientDelete = okKeys(await call(base, "DELETE", `/dash/apps/${appId}/oauth_clients/${clientId}`), (b) => b.client);
    r29.clientDeleteUnknown = errView(await call(base, "DELETE", `/dash/apps/${appId}/oauth_clients/${clientId}`));
    r29.authAfter = authView(await call(base, "GET", `/dash/apps/${appId}/auth`));
    raw("29-oauth-config", r29);
    record("29-oauth-config", r29);

    // 30 email templates (`instant-cli auth email status|push|reset`)
    const r30 = {};
    const statusView = (res) => (res.status === 200 ? { status: 200, info: res.body.info === null ? null : norm({ ...res.body.info, keys: Object.keys(res.body.info).sort() }) } : errView(res));
    r30.statusEmpty = statusView(await call(base, "GET", `/dash/apps/${appId}/email_status`));
    r30.pushNoCodeInSubject = errView(await call(base, "POST", `/dash/apps/${appId}/email_templates`, { body: { "email-type": "magic-code", subject: "hi", body: "use {code}" } }));
    r30.pushNoCodeInBody = errView(await call(base, "POST", `/dash/apps/${appId}/email_templates`, { body: { "email-type": "magic-code", subject: "{code} hi", body: "nope" } }));
    r30.pushMissingBody = errView(await call(base, "POST", `/dash/apps/${appId}/email_templates`, { body: { "email-type": "magic-code", subject: "{code} hi" } }));
    const pushed = await call(base, "POST", `/dash/apps/${appId}/email_templates`, { body: { "email-type": "magic-code", subject: "{code} for {app_title}", body: "<p>{code}</p>", "sender-name": "Diff Sender" } });
    r30.push = pushed.status === 200 ? { status: 200, keys: Object.keys(pushed.body).sort() } : errView(pushed);
    r30.status = statusView(await call(base, "GET", `/dash/apps/${appId}/email_status`));
    r30.reset = plainView(await call(base, "DELETE", `/dash/apps/${appId}/email_templates/${pushed.body?.id}`));
    r30.statusAfterReset = statusView(await call(base, "GET", `/dash/apps/${appId}/email_status`));
    const def = await call(base, "GET", "/dash/default-email-template", { token: null });
    r30.defaultTemplate = def.status === 200 ? { status: 200, keys: Object.keys(def.body).sort(), "email-type": def.body["email-type"], subject: def.body.subject, hasCode: String(def.body.body).includes("{code}") } : errView(def);
    raw("30-email-templates", r30);
    record("30-email-templates", r30);

    // 31 direct indexing-job creation (POST /dash/apps/:id/indexing-jobs)
    const r31 = {};
    const pulled = await call(base, "GET", `/dash/apps/${appId}/schema/pull`);
    const titleAttr = (pulled.body?.attrs ?? []).find((a) => a["forward-identity"][1] === "posts" && a["forward-identity"][2] === "title");
    r31.badJobType = errView(await call(base, "POST", `/dash/apps/${appId}/indexing-jobs`, { body: { "attr-id": titleAttr?.id, "job-type": "frobnicate" } }));
    r31.missingJobType = errView(await call(base, "POST", `/dash/apps/${appId}/indexing-jobs`, { body: { "attr-id": titleAttr?.id } }));
    r31.missingAttr = errView(await call(base, "POST", `/dash/apps/${appId}/indexing-jobs`, { body: { "job-type": "index" } }));
    r31.unknownAttr = errView(await call(base, "POST", `/dash/apps/${appId}/indexing-jobs`, { body: { "attr-id": mk(), "job-type": "index" } }));
    r31.otherAppAttr = otherAppId ? errView(await call(base, "POST", `/dash/apps/${otherAppId}/indexing-jobs`, { token: otherToken, body: { "attr-id": titleAttr?.id, "job-type": "index" } })) : null;
    r31.checkTypeMissingType = errView(await call(base, "POST", `/dash/apps/${appId}/indexing-jobs`, { body: { "attr-id": titleAttr?.id, "job-type": "check-data-type" } }));
    const job = await call(base, "POST", `/dash/apps/${appId}/indexing-jobs`, { body: { "attr-id": titleAttr?.id, "job-type": "index", "checked-data-type": "string" } });
    r31.create = job.status === 200 ? { status: 200, keys: Object.keys(job.body).sort(), job: jobView({ ...job.body.job, job_status: "<status>", job_stage: "<stage>" }) } : errView(job);
    let last;
    for (let i = 0; i < 600; i++) {
      last = await call(base, "GET", `/dash/apps/${appId}/indexing-jobs/${job.body?.job?.id}`);
      const st = last.body?.job?.job_status;
      if (st !== "waiting" && st !== "processing") break;
      await sleep(100);
    }
    r31.done = last?.status === 200 ? { status: 200, job: jobView(last.body.job) } : errView(last);
    const removed = await call(base, "POST", `/dash/apps/${appId}/indexing-jobs`, { body: { "attr-id": titleAttr?.id, "job-type": "remove-index" } });
    r31.removeCreate = removed.status === 200 ? { status: 200, job: jobView({ ...removed.body.job, job_status: "<status>", job_stage: "<stage>" }) } : errView(removed);
    for (let i = 0; i < 600; i++) {
      last = await call(base, "GET", `/dash/apps/${appId}/indexing-jobs/${removed.body?.job?.id}`);
      const st = last.body?.job?.job_status;
      if (st !== "waiting" && st !== "processing") break;
      await sleep(100);
    }
    r31.removeDone = last?.status === 200 ? { status: 200, job: jobView(last.body.job) } : errView(last);
    raw("31-indexing-job-post", r31);
    record("31-indexing-job-post", r31);

    // 32 ephemeral apps + claim
    const r32 = {};
    r32.claimRegularApp = errView(await call(base, "POST", `/dash/apps/${appId}/claim`, { token: userToken, body: { token: adminToken } }));
    r32.claimNoAuth = errView(await call(base, "POST", `/dash/apps/${appId}/claim`, { token: null, body: { token: adminToken } }));
    const eph = await call(base, "POST", "/dash/apps/ephemeral", { token: null, body: { title: "ephemeral cli app" } });
    r32.ephemeralCreate = eph.status === 200 ? { status: 200, keys: Object.keys(eph.body).sort(), hasAdminToken: typeof eph.body.app?.["admin-token"] === "string", expiresLater: eph.body.expires_ms > Date.now() } : errView(eph);
    const ephId = eph.body?.app?.id;
    const ephGet = await call(base, "GET", `/dash/apps/ephemeral/${ephId}`, { token: null });
    r32.ephemeralGet = ephGet.status === 200 ? { status: 200, keys: Object.keys(ephGet.body).sort(), title: ephGet.body.app?.title } : errView(ephGet);
    r32.ephemeralGetRegular = errView(await call(base, "GET", `/dash/apps/ephemeral/${appId}`, { token: null }));
    r32.claimWrongToken = errView(await call(base, "POST", `/dash/apps/ephemeral/${ephId}/claim`, { token: userToken, body: { app_id: ephId, token: mk() } }));
    r32.claim = plainView(await call(base, "POST", `/dash/apps/ephemeral/${ephId}/claim`, { token: userToken, body: { app_id: ephId, token: eph.body?.app?.["admin-token"] } }));
    const claimed = await call(base, "GET", `/dash/apps/${ephId}`, { token: userToken });
    r32.getClaimed = claimed.status === 200 ? { status: 200, creatorIsUser: claimed.body.app?.creator_id === me.body?.user?.id, title: claimed.body.app?.title } : errView(claimed);
    r32.claimAgain = errView(await call(base, "POST", `/dash/apps/${ephId}/claim`, { token: userToken, body: { token: eph.body?.app?.["admin-token"] } }));
    raw("32-ephemeral-and-claim", r32);
    record("32-ephemeral-and-claim", r32);

    // 33 app management (dashboard-only routes: rename / status / admin-token
    // rotation / magic-code expiry / rule versions / soft-deleted attrs /
    // test users / stats, plus /admin/schema + /admin/soft_deleted_attrs)
    const r33 = {};
    const appHdr = { headers: { "app-id": appId } };
    r33.renameAdminToken = errView(await call(base, "POST", `/dash/apps/${appId}/rename`, { body: { title: "renamed" } }));
    r33.renameMissingTitle = errView(await call(base, "POST", `/dash/apps/${appId}/rename`, { token: userToken, body: {} }));
    r33.rename = plainView(await call(base, "POST", `/dash/apps/${appId}/rename`, { token: userToken, body: { title: "renamed app" } }));
    r33.titleAfterRename = (await call(base, "GET", `/dash/apps/${appId}`, { token: userToken })).body?.app?.title ?? null;
    r33.statusBad = errView(await call(base, "POST", `/dash/apps/${appId}/status`, { token: userToken, body: { status: "paused" } }));
    r33.statusMissing = errView(await call(base, "POST", `/dash/apps/${appId}/status`, { token: userToken, body: {} }));
    r33.statusAdminToken = errView(await call(base, "POST", `/dash/apps/${appId}/status`, { body: { status: "active" } }));
    r33.statusReadOnly = plainView(await call(base, "POST", `/dash/apps/${appId}/status`, { token: userToken, body: { status: "read-only" } }));
    r33.transactReadOnly = errView(await call(base, "POST", "/admin/transact", { ...appHdr, body: { steps: [["update", "posts", mk(), { title: "ro" }]] } }));
    r33.queryReadOnly = okKeys(await call(base, "POST", "/admin/query", { ...appHdr, body: { query: { posts: {} } } }));
    r33.statusDisabled = plainView(await call(base, "POST", `/dash/apps/${appId}/status`, { token: userToken, body: { status: "disabled" } }));
    r33.queryDisabled = errView(await call(base, "POST", "/admin/query", { ...appHdr, body: { query: { posts: {} } } }));
    r33.transactDisabled = errView(await call(base, "POST", "/admin/transact", { ...appHdr, body: { steps: [["update", "posts", mk(), { title: "dis" }]] } }));
    r33.statusActive = plainView(await call(base, "POST", `/dash/apps/${appId}/status`, { token: userToken, body: { status: "active" } }));
    r33.queryActive = okKeys(await call(base, "POST", "/admin/query", { ...appHdr, body: { query: { posts: {} } } }));
    r33.statusInAppRow = (await call(base, "GET", `/dash/apps/${appId}`, { token: userToken })).body?.app?.status ?? null;
    // admin-token rotation: the old token stops working, the new one works
    const rotated = mk();
    r33.tokensAdminToken = errView(await call(base, "POST", `/dash/apps/${appId}/tokens`, { body: { "admin-token": rotated } }));
    r33.tokensMissing = errView(await call(base, "POST", `/dash/apps/${appId}/tokens`, { token: userToken, body: {} }));
    r33.tokensBad = errView(await call(base, "POST", `/dash/apps/${appId}/tokens`, { token: userToken, body: { "admin-token": "nope" } }));
    r33.tokens = plainView(await call(base, "POST", `/dash/apps/${appId}/tokens`, { token: userToken, body: { "admin-token": rotated } }));
    r33.oldTokenAfterRotate = errView(await call(base, "GET", `/dash/apps/${appId}/schema/pull`));
    r33.newTokenAfterRotate = okKeys(await call(base, "GET", `/dash/apps/${appId}/schema/pull`, { token: rotated }));
    r33.rotateBack = plainView(await call(base, "POST", `/dash/apps/${appId}/tokens`, { token: userToken, body: { "admin-token": adminToken } }));
    // magic-code expiry
    r33.expiryMissing = errView(await call(base, "POST", `/dash/apps/${appId}/set-magic-code-expiry`, { token: userToken, body: {} }));
    r33.expiryString = errView(await call(base, "POST", `/dash/apps/${appId}/set-magic-code-expiry`, { token: userToken, body: { expiry: "x" } }));
    r33.expiryZero = errView(await call(base, "POST", `/dash/apps/${appId}/set-magic-code-expiry`, { token: userToken, body: { expiry: 0 } }));
    r33.expiryTooLong = errView(await call(base, "POST", `/dash/apps/${appId}/set-magic-code-expiry`, { token: userToken, body: { expiry: 2000 } }));
    r33.expiryAdminToken = errView(await call(base, "POST", `/dash/apps/${appId}/set-magic-code-expiry`, { body: { expiry: 30 } }));
    r33.expiry = plainView(await call(base, "POST", `/dash/apps/${appId}/set-magic-code-expiry`, { token: userToken, body: { expiry: 30.7 } }));
    r33.expiryAfter = (await call(base, "GET", `/dash/apps/${appId}`, { token: userToken })).body?.app?.magic_code_expiry_minutes ?? null;
    // rule versions (rules were pushed by earlier sections)
    const rv = await call(base, "GET", `/dash/apps/${appId}/rule-versions`);
    r33.ruleVersions = rv.status === 200 ? { status: 200, keys: Object.keys(rv.body).sort(), versions: norm(rv.body.versions) } : errView(rv);
    r33.ruleVersionsUser = okKeys(await call(base, "GET", `/dash/apps/${appId}/rule-versions`, { token: userToken }));
    r33.ruleVersionsNoAuth = errView(await call(base, "GET", `/dash/apps/${appId}/rule-versions`, { token: null }));
    // soft-deleted attrs: add one, delete it, list it (dash + admin)
    const tmpAttr = mk();
    await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: [addAttr(tmpAttr, "posts", "tmpdel")] } });
    await call(base, "POST", `/dash/apps/${appId}/schema/steps/apply`, { body: { steps: [["delete-attr", tmpAttr]] } });
    const softView = (res) => (res.status === 200 ? { status: 200, keys: Object.keys(res.body).sort(), grace: res.body["grace-period-days"], attrs: (res.body.attrs ?? []).map(attrView) } : errView(res));
    r33.softDeleted = softView(await call(base, "GET", `/dash/apps/${appId}/soft_deleted_attrs`));
    r33.softDeletedUser = softView(await call(base, "GET", `/dash/apps/${appId}/soft_deleted_attrs`, { token: userToken }));
    r33.softDeletedAdmin = softView(await call(base, "GET", "/admin/soft_deleted_attrs", appHdr));
    r33.softDeletedAdminNoToken = errView(await call(base, "GET", "/admin/soft_deleted_attrs", { token: null, ...appHdr }));
    const adminSchema = await call(base, "GET", "/admin/schema", appHdr);
    r33.adminSchema = adminSchema.status === 200 ? { status: 200, keys: Object.keys(adminSchema.body).sort(), schema: schemaView(adminSchema.body.schema) } : errView(adminSchema);
    r33.adminSchemaNoToken = errView(await call(base, "GET", "/admin/schema", { token: null, ...appHdr }));
    r33.adminSchemaNoApp = errView(await call(base, "GET", "/admin/schema"));
    // test users
    const tuView = (u) => (u ? norm({ email: u.email, code: u.code, app_id: u.app_id, keys: Object.keys(u).sort() }) : null);
    r33.testUsersEmpty = plainView(await call(base, "GET", `/dash/apps/${appId}/test_users`, { token: userToken }));
    r33.testUserBadCode = errView(await call(base, "POST", `/dash/apps/${appId}/test_users`, { token: userToken, body: { email: "tester@example.com", code: "12" } }));
    r33.testUserBadEmail = errView(await call(base, "POST", `/dash/apps/${appId}/test_users`, { token: userToken, body: { email: "nope", code: "123456" } }));
    r33.testUserMissingCode = errView(await call(base, "POST", `/dash/apps/${appId}/test_users`, { token: userToken, body: { email: "tester@example.com" } }));
    const tu = await call(base, "POST", `/dash/apps/${appId}/test_users`, { token: userToken, body: { email: "Tester@Example.com", code: "123456" } });
    r33.testUserCreate = tu.status === 200 ? { status: 200, keys: Object.keys(tu.body).sort(), user: tuView(tu.body["test-user"]) } : errView(tu);
    r33.testUserDuplicate = errView(await call(base, "POST", `/dash/apps/${appId}/test_users`, { token: userToken, body: { email: "tester@example.com", code: "654321" } }));
    const tus = await call(base, "GET", `/dash/apps/${appId}/test_users`, { token: userToken });
    r33.testUsersList = tus.status === 200 ? { status: 200, users: (tus.body["test-users"] ?? []).map(tuView) } : errView(tus);
    r33.testUserDeleteMissingId = errView(await call(base, "DELETE", `/dash/apps/${appId}/test_users`, { token: userToken, body: {} }));
    r33.testUserDelete = plainView(await call(base, "DELETE", `/dash/apps/${appId}/test_users`, { token: userToken, body: { id: tu.body?.["test-user"]?.id } }));
    r33.testUserDeleteAgain = plainView(await call(base, "DELETE", `/dash/apps/${appId}/test_users`, { token: userToken, body: { id: tu.body?.["test-user"]?.id } }));
    // stats: the shape only (legacy sums cached per-machine session reports)
    const stats = await call(base, "GET", `/dash/apps/${appId}/stats`, { token: userToken });
    r33.stats = stats.status === 200 ? { status: 200, keys: Object.keys(stats.body).sort(), countIsNumber: typeof stats.body.count === "number", originsIsObject: typeof stats.body.origins === "object" } : errView(stats);
    r33.statsAdminToken = errView(await call(base, "GET", `/dash/apps/${appId}/stats`));
    raw("33-app-management", r33);
    record("33-app-management", r33);

    // 34 account routes: profiles, check-admin, personal access tokens,
    // dashboard magic-code login, signout
    const r34 = {};
    const patNorm = (v) => JSON.parse(JSON.stringify(norm(v)).replace(/per_[0-9a-f]{64}/g, "<pat>"));
    r34.profile = plainView(await call(base, "POST", "/dash/profiles", { token: userToken, body: { meta: { role: "dev" } } }));
    r34.profileMissingMeta = errView(await call(base, "POST", "/dash/profiles", { token: userToken, body: {} }));
    r34.profileNoAuth = errView(await call(base, "POST", "/dash/profiles", { token: null, body: { meta: {} } }));
    r34.checkAdmin = errView(await call(base, "GET", "/dash/check-admin", { token: userToken }));
    r34.checkAdminNoAuth = errView(await call(base, "GET", "/dash/check-admin", { token: null }));
    r34.patsEmpty = plainView(await call(base, "GET", "/dash/personal_access_tokens", { token: userToken }));
    r34.patCreateMissingName = errView(await call(base, "POST", "/dash/personal_access_tokens", { token: userToken, body: {} }));
    const pat = await call(base, "POST", "/dash/personal_access_tokens", { token: userToken, body: { name: "ci token" } });
    r34.patCreate = pat.status === 200 ? { status: 200, keys: Object.keys(pat.body).sort(), data: patNorm(pat.body.data) } : errView(pat);
    const pats = await call(base, "GET", "/dash/personal_access_tokens", { token: userToken });
    r34.pats = pats.status === 200 ? { status: 200, data: (pats.body.data ?? []).map((t) => patNorm({ ...t, keys: Object.keys(t).sort() })) } : errView(pats);
    r34.patDeleteBadId = errView(await call(base, "DELETE", "/dash/personal_access_tokens/nope", { token: userToken }));
    r34.patDeleteOther = plainView(await call(base, "DELETE", `/dash/personal_access_tokens/${mk()}`, { token: userToken }));
    r34.patDelete = plainView(await call(base, "DELETE", `/dash/personal_access_tokens/${pats.body?.data?.[0]?.id}`, { token: userToken }));
    r34.patsAfterDelete = plainView(await call(base, "GET", "/dash/personal_access_tokens", { token: userToken }));
    const loginEmail = `login-${fixedPrefix}@example.com`;
    r34.sendCodeBadEmail = errView(await call(base, "POST", "/dash/auth/send_magic_code", { token: null, body: { email: "nope" } }));
    r34.sendCodeMissingEmail = errView(await call(base, "POST", "/dash/auth/send_magic_code", { token: null, body: {} }));
    r34.sendCode = plainView(await call(base, "POST", "/dash/auth/send_magic_code", { token: null, body: { email: loginEmail } }));
    const loginCode = readDashCode(name, loginEmail);
    r34.codeShape = /^\d{6}$/.test(loginCode);
    r34.verifyWrongCode = errView(await call(base, "POST", "/dash/auth/verify_magic_code", { token: null, body: { email: loginEmail, code: "000000" } }));
    r34.verifyMissingCode = errView(await call(base, "POST", "/dash/auth/verify_magic_code", { token: null, body: { email: loginEmail } }));
    const verified = await call(base, "POST", "/dash/auth/verify_magic_code", { token: null, body: { email: loginEmail, code: ` ${loginCode} ` } });
    r34.verify = verified.status === 200 ? { status: 200, keys: Object.keys(verified.body).sort(), userKeys: Object.keys(verified.body.user ?? {}).sort(), email: verified.body.user?.email } : errView(verified);
    r34.verifyReuse = errView(await call(base, "POST", "/dash/auth/verify_magic_code", { token: null, body: { email: loginEmail, code: loginCode } }));
    const loginToken = verified.body?.token;
    r34.meWithLoginToken = okKeys(await call(base, "GET", "/dash/me", { token: loginToken }), (b) => b.user);
    r34.signoutNoAuth = errView(await call(base, "POST", "/dash/signout", { token: null }));
    r34.signout = plainView(await call(base, "POST", "/dash/signout", { token: loginToken }));
    r34.meAfterSignout = errView(await call(base, "GET", "/dash/me", { token: loginToken }));
    raw("34-account-routes", r34);
    record("34-account-routes", r34);

    // 35 dashboard storage + test email
    const r35 = {};
    const dashUpload = async (token, headers, body = "hello") => {
      const res = await fetch(base + `/dash/apps/${appId}/storage/upload`, { method: "PUT", headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...headers }, body });
      const text = await res.text();
      let json;
      try { json = JSON.parse(text); } catch { json = { "<non-json>": text.slice(0, 200) }; }
      return { status: res.status, body: json };
    };
    const upView = (res) => (res.status === 200 ? { status: 200, keys: Object.keys(res.body).sort(), data: norm({ ...res.body.data, keys: Object.keys(res.body.data ?? {}).sort() }) } : errView(res));
    r35.uploadNoAuth = errView(await dashUpload(null, { path: "dash/hello.txt", "content-type": "text/plain" }));
    r35.uploadMissingPath = errView(await dashUpload(userToken, { "content-type": "text/plain" }));
    r35.upload = upView(await dashUpload(userToken, { path: "dash/hello.txt", "content-type": "text/plain" }));
    r35.uploadAdminToken = upView(await dashUpload(adminToken, { path: "dash/hello2.txt", "content-type": "text/plain" }, "hello again"));
    r35.filesAfterUpload = plainView(await call(base, "POST", "/admin/query", { ...appHdr, body: { query: { $files: { $: { fields: ["path"] } } } } }));
    r35.filesDeleteMissing = errView(await call(base, "POST", `/dash/apps/${appId}/storage/files/delete`, { token: userToken, body: {} }));
    r35.filesDeleteNotArray = errView(await call(base, "POST", `/dash/apps/${appId}/storage/files/delete`, { token: userToken, body: { filenames: "x" } }));
    r35.filesDeleteNoAuth = errView(await call(base, "POST", `/dash/apps/${appId}/storage/files/delete`, { token: null, body: { filenames: ["dash/hello.txt"] } }));
    r35.filesDelete = plainView(await call(base, "POST", `/dash/apps/${appId}/storage/files/delete`, { token: userToken, body: { filenames: ["dash/hello.txt", "nope.txt"] } }));
    r35.filesDeleteAdminToken = plainView(await call(base, "POST", `/dash/apps/${appId}/storage/files/delete`, { body: { filenames: ["dash/hello2.txt"] } }));
    r35.filesAfterDelete = plainView(await call(base, "POST", "/admin/query", { ...appHdr, body: { query: { $files: { $: { fields: ["path"] } } } } }));
    r35.testEmailMissingTo = errView(await call(base, "POST", `/dash/apps/${appId}/send-test-email`, { token: userToken, body: { subject: "s {code}", body: "b {code}" } }));
    r35.testEmailNonMember = errView(await call(base, "POST", `/dash/apps/${appId}/send-test-email`, { token: userToken, body: { subject: "s {code}", body: "b {code}", to: "stranger@example.com" } }));
    r35.testEmailAdminToken = errView(await call(base, "POST", `/dash/apps/${appId}/send-test-email`, { body: { subject: "s {code}", body: "b {code}", to: me.body?.user?.email } }));
    r35.testEmail = plainView(await call(base, "POST", `/dash/apps/${appId}/send-test-email`, { token: userToken, body: { subject: "s {code}", body: "b {code}", to: me.body?.user?.email } }));
    raw("35-dash-storage-and-test-email", r35);
    record("35-dash-storage-and-test-email", r35);

    // 36 clear: every user attr soft-deleted, rules reset (last: it empties the app)
    const r36 = {};
    r36.clearAdminToken = errView(await call(base, "POST", `/dash/apps/${appId}/clear`));
    r36.clear = plainView(await call(base, "POST", `/dash/apps/${appId}/clear`, { token: userToken }));
    r36.pullAfterClear = pullView(await call(base, "GET", `/dash/apps/${appId}/schema/pull`));
    r36.permsAfterClear = plainView(await call(base, "GET", `/dash/apps/${appId}/perms/pull`));
    r36.softDeletedAfterClear = softView(await call(base, "GET", `/dash/apps/${appId}/soft_deleted_attrs`));
    r36.queryAfterClear = plainView(await call(base, "POST", "/admin/query", { ...appHdr, body: { query: { posts: {} } } }));
    raw("36-clear", r36);
    record("36-clear", r36);

    // 37 teams: app + org invites, member roles, org rename, transfer to
    // org, ephemeral status, get-a-db lookup. A second dashboard user (the
    // invitee) is signed in through the dashboard magic-code flow.
    const r37 = {};
    const inviteeEmail = `invitee-${fixedPrefix}@example.com`;
    await call(base, "POST", "/dash/auth/send_magic_code", { token: null, body: { email: inviteeEmail } });
    const inviteeLogin = await call(base, "POST", "/dash/auth/verify_magic_code", { token: null, body: { email: inviteeEmail, code: readDashCode(name, inviteeEmail) } });
    const inviteeToken = inviteeLogin.body?.token;
    r37.inviteeLogin = inviteeLogin.status === 200 ? { status: 200 } : errView(inviteeLogin);
    const invitesOf = async (token) => {
      const d = await call(base, "GET", "/dash", { token });
      return (d.body?.apps ?? []).find((a) => a.id === appId) ?? {};
    };
    r37.inviteSendAdminToken = errView(await call(base, "POST", `/dash/apps/${appId}/invite/send`, { body: { "invitee-email": inviteeEmail, role: "admin" } }));
    r37.inviteSendBadRole = errView(await call(base, "POST", `/dash/apps/${appId}/invite/send`, { token: userToken, body: { "invitee-email": inviteeEmail, role: "creator" } }));
    r37.inviteSendBadEmail = errView(await call(base, "POST", `/dash/apps/${appId}/invite/send`, { token: userToken, body: { "invitee-email": "nope", role: "admin" } }));
    r37.inviteSendMissingRole = errView(await call(base, "POST", `/dash/apps/${appId}/invite/send`, { token: userToken, body: { "invitee-email": inviteeEmail } }));
    r37.inviteSendByStranger = errView(await call(base, "POST", `/dash/apps/${appId}/invite/send`, { token: inviteeToken, body: { "invitee-email": "x@example.com", role: "admin" } }));
    r37.inviteSend = plainView(await call(base, "POST", `/dash/apps/${appId}/invite/send`, { token: userToken, body: { "invitee-email": inviteeEmail, role: "admin" } }));
    r37.inviteSendAgain = plainView(await call(base, "POST", `/dash/apps/${appId}/invite/send`, { token: userToken, body: { "invitee-email": inviteeEmail, role: "collaborator" } }));
    let appRow = await invitesOf(userToken);
    r37.invitesOnApp = norm((appRow.invites ?? []).map((i) => ({ email: i.email, role: i.role, status: i.status, expired: i.expired })));
    const inviteId = appRow.invites?.[0]?.id;
    r37.acceptWrongUser = errView(await call(base, "POST", "/dash/invites/accept", { token: userToken, body: { "invite-id": inviteId } }));
    r37.acceptUnknown = errView(await call(base, "POST", "/dash/invites/accept", { token: inviteeToken, body: { "invite-id": mk() } }));
    r37.acceptMissingId = errView(await call(base, "POST", "/dash/invites/accept", { token: inviteeToken, body: {} }));
    r37.acceptNoAuth = errView(await call(base, "POST", "/dash/invites/accept", { token: null, body: { "invite-id": inviteId } }));
    r37.declineWrongUser = errView(await call(base, "POST", "/dash/invites/decline", { token: userToken, body: { "invite-id": inviteId } }));
    r37.accept = plainView(await call(base, "POST", "/dash/invites/accept", { token: inviteeToken, body: { "invite-id": inviteId } }));
    r37.acceptAgain = errView(await call(base, "POST", "/dash/invites/accept", { token: inviteeToken, body: { "invite-id": inviteId } }));
    appRow = await invitesOf(userToken);
    r37.membersAfterAccept = norm((appRow.members ?? []).map((m) => ({ email: m.email, role: m.role })));
    r37.inviteStatusAfterAccept = norm((appRow.invites ?? []).map((i) => ({ email: i.email, status: i.status })));
    const memberId = (appRow.members ?? []).find((m) => m.email === inviteeEmail)?.id;
    r37.inviteeSeesApp = { titles: ((await call(base, "GET", "/dash", { token: inviteeToken })).body?.apps ?? []).map((a) => a.title).sort() };
    r37.memberUpdateBadRole = errView(await call(base, "POST", `/dash/apps/${appId}/members/update`, { token: userToken, body: { id: memberId, role: "boss" } }));
    r37.memberUpdateUnknown = errView(await call(base, "POST", `/dash/apps/${appId}/members/update`, { token: userToken, body: { id: mk(), role: "admin" } }));
    r37.memberUpdateMissingId = errView(await call(base, "POST", `/dash/apps/${appId}/members/update`, { token: userToken, body: { role: "admin" } }));
    r37.memberUpdateByCollaborator = errView(await call(base, "POST", `/dash/apps/${appId}/members/update`, { token: inviteeToken, body: { id: memberId, role: "admin" } }));
    r37.memberUpdate = plainView(await call(base, "POST", `/dash/apps/${appId}/members/update`, { token: userToken, body: { id: memberId, role: "admin" } }));
    r37.memberUpdateAboveSelf = errView(await call(base, "POST", `/dash/apps/${appId}/members/update`, { token: inviteeToken, body: { id: memberId, role: "owner" } }));
    r37.memberRemoveMissingId = errView(await call(base, "DELETE", `/dash/apps/${appId}/members/remove`, { token: userToken, body: {} }));
    r37.memberRemoveUnknown = errView(await call(base, "DELETE", `/dash/apps/${appId}/members/remove`, { token: userToken, body: { id: mk() } }));
    r37.memberRemove = plainView(await call(base, "DELETE", `/dash/apps/${appId}/members/remove`, { token: userToken, body: { id: memberId } }));
    appRow = await invitesOf(userToken);
    r37.membersAfterRemove = (appRow.members ?? []).length;
    r37.inviteeSeesAppAfterRemove = { titles: ((await call(base, "GET", "/dash", { token: inviteeToken })).body?.apps ?? []).map((a) => a.title).sort() };
    // revoke a fresh invite; decline another; a declined invite can't be accepted
    await call(base, "POST", `/dash/apps/${appId}/invite/send`, { token: userToken, body: { "invitee-email": "third@example.com", role: "collaborator" } });
    appRow = await invitesOf(userToken);
    const thirdInvite = (appRow.invites ?? []).find((i) => i.email === "third@example.com")?.id;
    r37.revokeMissingId = errView(await call(base, "DELETE", `/dash/apps/${appId}/invite/revoke`, { token: userToken, body: {} }));
    r37.revokeByStranger = errView(await call(base, "DELETE", `/dash/apps/${appId}/invite/revoke`, { token: inviteeToken, body: { "invite-id": thirdInvite } }));
    r37.revoke = plainView(await call(base, "DELETE", `/dash/apps/${appId}/invite/revoke`, { token: userToken, body: { "invite-id": thirdInvite } }));
    appRow = await invitesOf(userToken);
    r37.invitesAfterRevoke = norm((appRow.invites ?? []).map((i) => ({ email: i.email, status: i.status })).sort((a, b) => (a.email < b.email ? -1 : 1)));
    await call(base, "POST", `/dash/apps/${appId}/invite/send`, { token: userToken, body: { "invitee-email": inviteeEmail, role: "collaborator" } });
    appRow = await invitesOf(userToken);
    const declineId = (appRow.invites ?? []).find((i) => i.email === inviteeEmail)?.id;
    r37.decline = plainView(await call(base, "POST", "/dash/invites/decline", { token: inviteeToken, body: { "invite-id": declineId } }));
    r37.acceptDeclined = errView(await call(base, "POST", "/dash/invites/accept", { token: inviteeToken, body: { "invite-id": declineId } }));
    // org side
    const teamOrg = await call(base, "POST", "/dash/orgs", { token: userToken, body: { title: "team org" } });
    const teamOrgId = teamOrg.body?.org?.id;
    const orgView = async (token = userToken) => {
      const g = await call(base, "GET", `/dash/orgs/${teamOrgId}`, { token });
      return g.status === 200 ? { status: 200, title: g.body.org?.title, members: (g.body.members ?? []).map((m) => ({ email: m.email, role: m.role })).sort((a, b) => (a.email < b.email ? -1 : 1)), invites: (g.body.invites ?? []).map((i) => ({ email: i.email, role: i.role, status: i.status })), apps: (g.body.apps ?? []).map((a) => a.title).sort() } : errView(g);
    };
    r37.orgInviteSendByStranger = errView(await call(base, "POST", `/dash/orgs/${teamOrgId}/invite/send`, { token: inviteeToken, body: { "invitee-email": "x@example.com", role: "admin" } }));
    r37.orgInviteSend = plainView(await call(base, "POST", `/dash/orgs/${teamOrgId}/invite/send`, { token: userToken, body: { "invitee-email": inviteeEmail, role: "admin" } }));
    let orgGot = await call(base, "GET", `/dash/orgs/${teamOrgId}`, { token: userToken });
    const orgInviteId = (orgGot.body?.invites ?? []).find((i) => i.email === inviteeEmail)?.id;
    r37.orgAccept = plainView(await call(base, "POST", "/dash/invites/accept", { token: inviteeToken, body: { "invite-id": orgInviteId } }));
    r37.orgAfterAccept = await orgView();
    r37.orgRenameMissingTitle = errView(await call(base, "POST", `/dash/orgs/${teamOrgId}/rename`, { token: userToken, body: {} }));
    r37.orgRenameTooLong = errView(await call(base, "POST", `/dash/orgs/${teamOrgId}/rename`, { token: userToken, body: { title: "x".repeat(141) } }));
    r37.orgRename = plainView(await call(base, "POST", `/dash/orgs/${teamOrgId}/rename`, { token: userToken, body: { title: "renamed org" } }));
    r37.orgRenameByAdmin = plainView(await call(base, "POST", `/dash/orgs/${teamOrgId}/rename`, { token: inviteeToken, body: { title: "renamed by admin" } }));
    orgGot = await call(base, "GET", `/dash/orgs/${teamOrgId}`, { token: userToken });
    const orgMemberId = (orgGot.body?.members ?? []).find((m) => m.email === inviteeEmail)?.id;
    const orgOwnerId = (orgGot.body?.members ?? []).find((m) => m.email !== inviteeEmail)?.id;
    r37.orgMemberUpdateAboveSelf = errView(await call(base, "POST", `/dash/orgs/${teamOrgId}/members/update`, { token: inviteeToken, body: { id: orgMemberId, role: "owner" } }));
    r37.orgMemberUpdate = plainView(await call(base, "POST", `/dash/orgs/${teamOrgId}/members/update`, { token: userToken, body: { id: orgMemberId, role: "collaborator" } }));
    r37.orgRenameByCollaborator = errView(await call(base, "POST", `/dash/orgs/${teamOrgId}/rename`, { token: inviteeToken, body: { title: "nope" } }));
    r37.orgRemoveLastOwner = errView(await call(base, "DELETE", `/dash/orgs/${teamOrgId}/members/remove`, { token: userToken, body: { id: orgOwnerId } }));
    r37.orgRemoveOwnerByCollaborator = errView(await call(base, "DELETE", `/dash/orgs/${teamOrgId}/members/remove`, { token: inviteeToken, body: { id: orgOwnerId } }));
    r37.orgMemberRemove = plainView(await call(base, "DELETE", `/dash/orgs/${teamOrgId}/members/remove`, { token: userToken, body: { id: orgMemberId } }));
    r37.orgAfterRemove = await orgView();
    // transfer an app into the org
    const tApp = mk();
    await call(base, "POST", "/dash/apps", { token: userToken, body: { id: tApp, title: "to transfer", admin_token: mk() } });
    r37.transferUnknownOrg = errView(await call(base, "POST", `/dash/apps/${tApp}/transfer_to_org/${mk()}`, { token: userToken }));
    r37.transferNotOwner = errView(await call(base, "POST", `/dash/apps/${tApp}/transfer_to_org/${teamOrgId}`, { token: inviteeToken }));
    r37.transfer = plainView(await call(base, "POST", `/dash/apps/${tApp}/transfer_to_org/${teamOrgId}`, { token: userToken }));
    const tGot = await call(base, "GET", `/dash/apps/${tApp}`, { token: userToken });
    r37.transferred = { orgMatches: tGot.body?.app?.org_id === teamOrgId, creator: tGot.body?.app?.creator_id ?? null };
    r37.orgAfterTransfer = await orgView();
    // ephemeral status toggle + get-a-db lookup
    const eph2 = await call(base, "POST", "/dash/apps/ephemeral", { token: null, body: { title: "eph status" } });
    const eph2Id = eph2.body?.app?.id;
    r37.ephStatusBadToken = errView(await call(base, "POST", `/dash/apps/ephemeral/${eph2Id}/status`, { token: null, body: { "admin-token": mk(), status: "read-only" } }));
    r37.ephStatusBadStatus = errView(await call(base, "POST", `/dash/apps/ephemeral/${eph2Id}/status`, { token: null, body: { "admin-token": eph2.body?.app?.["admin-token"], status: "paused" } }));
    r37.ephStatus = plainView(await call(base, "POST", `/dash/apps/ephemeral/${eph2Id}/status`, { token: null, body: { "admin-token": eph2.body?.app?.["admin-token"], status: "read-only" } }));
    r37.ephStatusRegularApp = errView(await call(base, "POST", `/dash/apps/ephemeral/${appId}/status`, { token: null, body: { "admin-token": adminToken, status: "active" } }));
    r37.getADbRegular = errView(await call(base, "GET", `/dash/apps/get_a_db/${appId}`, { token: null }));
    r37.getADbUnknown = errView(await call(base, "GET", `/dash/apps/get_a_db/${mk()}`, { token: null }));
    raw("37-teams", r37);
    record("37-teams", r37);
  }

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
    console.log("  full legacy:", JSON.stringify(d.legacy)?.slice(0, 700));
    console.log("  full rust:  ", JSON.stringify(d.rust)?.slice(0, 700));
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
