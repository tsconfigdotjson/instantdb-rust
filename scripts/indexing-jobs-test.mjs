// End-to-end check of async indexing jobs (issue #5) against the rust server:
// batched rewrites with progress, in-flight attr markers on the wire
// (`indexing?` / `setting-unique?` / `checking-data-type?`), the query
// planner ignoring an index whose job is still running, every job type's
// success and error path, and the sweep that resumes orphaned jobs.
//
// Usage: node scripts/indexing-jobs-test.mjs <app-id> <admin-token>
// Env: RUST_URL (default http://localhost:8888), DATABASE_URL (for the
//      orphaned-job scenario; default postgres://instant:instant@localhost:5432/instant)
// Run the server with a small batch so the scans really are batched, e.g.
//   INSTANT_INDEXING_BATCH_SIZE=25 INSTANT_INDEXING_SWEEP_SECS=5 INSTANT_INDEXING_STALE_SECS=30
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { connect, psql } from "./differential/lib.mjs";

const appId = process.argv[2];
const token = process.argv[3];
if (!appId || !token) throw new Error("usage: node indexing-jobs-test.mjs <app-id> <admin-token>");
const BASE = process.env.RUST_URL || "http://localhost:8888";
const DB = process.env.DATABASE_URL || "postgres://instant:instant@localhost:5432/instant";
const uuid = () => crypto.randomUUID();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function dash(method, p, body) {
  const res = await fetch(`${BASE}${p}`, {
    method,
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json", "X-Instant-Source": "instant-cli" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}
async function admin(p, body) {
  const res = await fetch(`${BASE}/admin/${p}`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "app-id": appId, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}
async function pullAttrs() {
  const res = await dash("GET", `/dash/apps/${appId}/schema/pull`);
  assert.equal(res.status, 200);
  const out = {};
  for (const a of res.body.attrs) out[`${a["forward-identity"][1]}.${a["forward-identity"][2]}`] = a;
  return out;
}
const jobStep = (op, id, etype, label, extra = {}) => [op, { "attr-id": id, "forward-identity": [id, etype, label], ...extra }];
async function apply(steps) {
  const res = await dash("POST", `/dash/apps/${appId}/schema/steps/apply`, { steps });
  assert.equal(res.status, 200, JSON.stringify(res.body));
  return res.body;
}
async function waitJobs(groupId, { onPoll } = {}) {
  for (let i = 0; i < 3000; i++) {
    const res = await dash("GET", `/dash/apps/${appId}/indexing-jobs/group/${groupId}`);
    assert.equal(res.status, 200);
    if (onPoll) await onPoll(res.body.jobs);
    if (!res.body.jobs.some((j) => j.job_status === "waiting" || j.job_status === "processing")) return res.body.jobs;
    await sleep(5);
  }
  throw new Error("jobs did not finish");
}
async function runJobs(steps, opts) {
  const applied = await apply(steps);
  const groupId = applied["indexing-jobs"]["group-id"];
  for (const j of applied["indexing-jobs"].jobs) assert.equal(j.job_status, "waiting");
  const jobs = await waitJobs(groupId, opts);
  const byType = {};
  for (const j of jobs) byType[`${j.job_type}:${j.attr_id}`] = j;
  return { jobs, byType, groupId };
}
function checkEstimate(job) {
  // legacy check-estimate: completed <= estimate <= 1.05 * completed + 1
  assert.ok(job.work_estimate > 0, `estimate ${job.work_estimate}`);
  assert.ok(job.work_completed <= job.work_estimate, `completed ${job.work_completed} > estimate ${job.work_estimate}`);
  assert.ok(job.work_estimate <= 1.05 * job.work_completed + 1, `estimate ${job.work_estimate} not within 5% of ${job.work_completed}`);
}
/// Hold a row lock in a psql session so a job's `FOR UPDATE` batch blocks
/// there: a deterministic mid-flight window. `release()` commits.
async function lockRow(sql) {
  const p = spawn("psql", [DB, "-q", "-v", "ON_ERROR_STOP=1"], { stdio: ["pipe", "pipe", "inherit"] });
  let out = "";
  const locked = new Promise((resolve) => p.stdout.on("data", (d) => { out += d; if (out.includes("LOCKED")) resolve(); }));
  p.stdin.write(`BEGIN;\n${sql};\n\\echo LOCKED\n`);
  await locked;
  return { release: () => new Promise((r) => { p.on("exit", r); p.stdin.end("COMMIT;\n"); }) };
}
async function waitUntil(pred, what, timeout = 20000) {
  const start = Date.now();
  for (;;) {
    const v = await pred();
    if (v) return v;
    if (Date.now() - start > timeout) throw new Error(`timeout waiting for ${what}`);
    await sleep(10);
  }
}
// incompressible (TOAST would shrink a repeated character below the 1024-byte index limit)
const bigValue = (() => { let h = appId, out = ""; while (out.length < 3000) { h = createHash("sha256").update(h).digest("hex"); out += h; } return out; })();
let passed = 0;
const ok = (msg) => {
  passed++;
  console.log("  ok -", msg);
};

// ---------------------------------------------------------------------------
// schema + data: N items with title (unique strings), n (numbers, 3 bad
// strings), tag ("x" for all), big (one 2KB value)
const N = 3000;
const ids = { id: uuid(), title: uuid(), n: uuid(), tag: uuid(), big: uuid() };
const addAttr = (id, label) => [
  "add-attr",
  { id, "forward-identity": [id, "items", label], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, "required?": false },
];
await apply([addAttr(ids.id, "id"), jobStep("unique", ids.id, "items", "id"), jobStep("index", ids.id, "items", "id"), addAttr(ids.title, "title"), addAttr(ids.n, "n"), addAttr(ids.tag, "tag"), addAttr(ids.big, "big")]);
const items = Array.from({ length: N }, () => uuid());
const badN = new Set([7, 1500, N - 1]);
for (let i = 0; i < N; i += 500) {
  const steps = [];
  for (let j = i; j < Math.min(N, i + 500); j++) {
    steps.push(["update", "items", items[j], { title: `t-${j}`, n: badN.has(j) ? `not-${j}` : j, tag: "x" }]);
  }
  const res = await admin("transact", { steps });
  assert.equal(res.status, 200, JSON.stringify(res.body));
}
const bigItem = uuid();
assert.equal((await admin("transact", { steps: [["update", "items", bigItem, { big: bigValue, title: "big", tag: "x" }]] })).status, 200);
console.log(`seeded ${N + 1} items`);

// a websocket session with a query: attrs changes arrive as refresh-ok
const ws = connect(BASE, appId, "watcher");
await ws.open;
ws.send({ op: "init", "client-event-id": uuid(), "app-id": appId, versions: { "@instantdb/core": "v0.21.0" } });
await ws.waitFor((m) => m.op === "init-ok");
ws.send({ op: "add-query", "client-event-id": uuid(), q: { items: { $: { where: { n: 5 } } } } });
await ws.waitFor((m) => m.op === "add-query-ok");
ws.takeNewFrames();

// ---------------------------------------------------------------------------
console.log("1. index + check-data-type (invalid values) on items.n");
{
  // hold one of items.n's rows: the index job flags everything else, then
  // blocks in update-triples on the batch holding this row
  const lock = await lockRow(`SELECT 1 FROM triples WHERE app_id = '${appId}' AND attr_id = '${ids.n}' AND entity_id = '${items[N - 1]}' FOR UPDATE`);
  const applied = await apply([jobStep("index", ids.n, "items", "n"), jobStep("check-data-type", ids.n, "items", "n", { "checked-data-type": "number" })]);
  const groupId = applied["indexing-jobs"]["group-id"];
  const nAttr = (m) => m.attrs.find((a) => a.id === ids.n);

  // update-attr-start: index? flips on together with indexing?
  const midAttr = await waitUntil(async () => { const a = (await pullAttrs())["items.n"]; return a["indexing?"] ? a : null; }, "indexing?");
  assert.equal(midAttr["index?"], true);
  // the job is really stuck mid-rewrite with progress recorded
  const midJob = await waitUntil(async () => {
    const res = await dash("GET", `/dash/apps/${appId}/indexing-jobs/group/${groupId}`);
    const j = res.body.jobs.find((x) => x.job_type === "index");
    return j.job_stage === "update-triples" && j.work_completed > 0 ? j : null;
  }, "index job in update-triples with progress");
  assert.equal(midJob.job_status, "processing");
  // ...and blocked: progress stops once the batch holding the locked row
  // is reached, while the job stays in processing
  let stuck = midJob;
  await waitUntil(async () => {
    await sleep(150);
    const j = (await dash("GET", `/dash/apps/${appId}/indexing-jobs/group/${groupId}`)).body.jobs.find((x) => x.job_type === "index");
    assert.equal(j.job_status, "processing", "must not finish while the row is locked");
    const plateau = j.work_completed === stuck.work_completed && j.job_stage === "update-triples";
    stuck = j;
    return plateau;
  }, "index job to block on the locked row");
  assert.ok(stuck.work_completed < stuck.work_estimate, `${stuck.work_completed} < ${stuck.work_estimate}`);
  // the planner must not use the half-built index
  const q = await admin("query", { query: { items: { $: { where: { n: 5 } } } } });
  assert.equal(q.status, 200);
  assert.equal(q.body.items.length, 1, "in-flight index must not hide rows");
  assert.equal(q.body.items[0].id, items[5]);
  // ordering by it is refused while the job runs
  const o = await admin("query", { query: { items: { $: { order: { n: "asc" }, limit: 1 } } } });
  assert.equal(o.status, 400);
  assert.match(o.body.message, /still in the process of indexing/);
  // the websocket session already got attrs with indexing?
  await ws.waitFor((m) => m.op === "refresh-ok" && Array.isArray(m.attrs) && nAttr(m)?.["indexing?"] === true, 30000);
  ok(`indexing? visible mid-flight (blocked at ${stuck.work_completed}/${stuck.work_estimate}); queries ignore the index; order by refused; session refreshed`);

  await lock.release();
  const jobs = await waitJobs(groupId);
  const idx = jobs.find((j) => j.job_type === "index");
  const chk = jobs.find((j) => j.job_type === "check-data-type");
  assert.equal(idx.job_status, "completed", JSON.stringify(idx));
  assert.equal(idx.job_stage, "update-attr-done");
  checkEstimate(idx);
  // N rows flagged + N+1 entities scanned for nulls
  assert.ok(idx.work_completed >= 2 * N, `index work ${idx.work_completed}`);
  assert.equal(chk.job_status, "errored");
  assert.equal(chk.error, "invalid-triple-error");
  assert.equal(chk.job_stage, "validate");
  assert.equal(chk.invalid_triples_sample.length, 3);
  for (const s of chk.invalid_triples_sample) assert.equal(s.json_type, "string");
  const attrs = await pullAttrs();
  assert.equal(attrs["items.n"]["index?"], true);
  assert.equal(attrs["items.n"]["indexing?"], undefined);
  assert.equal(attrs["items.n"]["checked-data-type"], undefined, "failed check leaves the attr untyped");
  assert.equal(attrs["items.n"]["checking-data-type?"], undefined);
  // the big item had no n: the null backfill gives it one (visible as isNull)
  const nulls = await admin("query", { query: { items: { $: { where: { n: { $isNull: true } } } } } });
  assert.equal(nulls.body.items.length, 1);
  assert.equal(nulls.body.items[0].id, bigItem);
  ok(`index completed in batches (work ${idx.work_completed}/${idx.work_estimate}); invalid type check errored with samples`);

  // refreshes are queued behind the query recomputation; wait for the one
  // that carries the final attr state
  await ws.waitFor((m) => m.op === "refresh-ok" && Array.isArray(m.attrs) && nAttr(m)?.["index?"] === true && !nAttr(m)?.["indexing?"], 30000);
  const frames = ws.takeNewFrames().filter((m) => m.op === "refresh-ok" && Array.isArray(m.attrs));
  assert.ok(frames.some((m) => nAttr(m)?.["indexing?"] === true), "a refresh carried indexing?");
  const last = nAttr(frames[frames.length - 1]);
  assert.equal(last["index?"], true);
  assert.equal(last["indexing?"], undefined);
  ok("websocket session received attrs with indexing? during the job and without it after");
}

// ---------------------------------------------------------------------------
console.log("2. unique on a duplicated value rolls back");
{
  const { byType } = await runJobs([jobStep("unique", ids.tag, "items", "tag")]);
  const j = byType[`unique:${ids.tag}`];
  assert.equal(j.job_status, "errored");
  assert.equal(j.error, "triple-not-unique-error");
  assert.equal(j.job_stage, "update-triples");
  assert.equal(j.invalid_unique_value, "x");
  assert.ok(j.invalid_triples_sample.length > 1);
  for (const s of j.invalid_triples_sample) assert.equal(s.value, "x");
  const attrs = await pullAttrs();
  assert.equal(attrs["items.tag"]["unique?"], false);
  assert.equal(attrs["items.tag"]["setting-unique?"], undefined);
  ok("unique job errored with invalid_unique_value and the attr is back to non-unique");
}

// ---------------------------------------------------------------------------
console.log("3. check-data-type after fixing the data; order by; remove-data-type; remove-index");
{
  const fixes = [...badN].map((i) => ["update", "items", items[i], { n: i }]);
  assert.equal((await admin("transact", { steps: fixes })).status, 200);
  const lock = await lockRow(`SELECT 1 FROM triples WHERE app_id = '${appId}' AND attr_id = '${ids.n}' AND entity_id = '${items[N - 1]}' FOR UPDATE`);
  const applied = await apply([jobStep("check-data-type", ids.n, "items", "n", { "checked-data-type": "number" })]);
  const groupId = applied["indexing-jobs"]["group-id"];
  // validate passes (reads only), update-attr-start marks the attr, the
  // update-triples batch holding the locked row blocks
  const midAttr = await waitUntil(async () => { const a = (await pullAttrs())["items.n"]; return a["checking-data-type?"] ? a : null; }, "checking-data-type?");
  assert.equal(midAttr["checked-data-type"], "number");
  await waitUntil(async () => {
    const j = (await dash("GET", `/dash/apps/${appId}/indexing-jobs/group/${groupId}`)).body.jobs[0];
    return j.job_stage === "update-triples" ? j : null;
  }, "check-data-type in update-triples");
  const o = await admin("query", { query: { items: { $: { order: { n: "asc" }, limit: 1 } } } });
  assert.equal(o.status, 400);
  assert.match(o.body.message, /still in the process of validating its type/);
  const c = await admin("query", { query: { items: { $: { where: { n: { $gt: 5 } } } } } });
  assert.equal(c.status, 400);
  assert.match(c.body.message, /still in the process of checking its data type/);
  const q = await admin("query", { query: { items: { $: { where: { n: 1500 } } } } });
  assert.equal(q.body.items.length, 1, "typed lookup must not miss unstamped rows mid-check");
  ok("checking-data-type? visible mid-flight; order/comparators refused; equality still finds unstamped rows");
  await lock.release();
  const jobs = await waitJobs(groupId);
  const byType = { [`check-data-type:${ids.n}`]: jobs[0] };
  const j = byType[`check-data-type:${ids.n}`];
  assert.equal(j.job_status, "completed", JSON.stringify(j));
  assert.equal(j.job_stage, "update-attr-done");
  checkEstimate(j);
  assert.ok(j.work_completed >= 3 * N, `check work ${j.work_completed}`);
  let attrs = await pullAttrs();
  assert.equal(attrs["items.n"]["checked-data-type"], "number");
  assert.equal(attrs["items.n"]["checking-data-type?"], undefined);
  const desc = await admin("query", { query: { items: { $: { order: { n: "desc" }, limit: 3 } } } });
  assert.equal(desc.status, 200, JSON.stringify(desc.body));
  assert.deepEqual(desc.body.items.map((i) => i.n), [N - 1, N - 2, N - 3]);
  const gt = await admin("query", { query: { items: { $: { where: { n: { $gt: N - 3 } } } } } });
  assert.equal(gt.body.items.length, 2);
  ok("check-data-type completed in batches; order/comparators work afterwards");

  const r = await runJobs([jobStep("remove-data-type", ids.n, "items", "n")]);
  const rj = r.byType[`remove-data-type:${ids.n}`];
  assert.equal(rj.job_status, "completed");
  checkEstimate(rj);
  attrs = await pullAttrs();
  assert.equal(attrs["items.n"]["checked-data-type"], undefined);
  const untyped = await admin("query", { query: { items: { $: { order: { n: "asc" } } } } });
  assert.equal(untyped.status, 400);
  assert.match(untyped.body.message, /not typed/);
  ok("remove-data-type completed; order by refused as untyped");

  const ri = await runJobs([jobStep("remove-index", ids.n, "items", "n")]);
  const rij = ri.byType[`remove-index:${ids.n}`];
  assert.equal(rij.job_status, "completed");
  checkEstimate(rij);
  attrs = await pullAttrs();
  assert.equal(attrs["items.n"]["index?"], false);
  assert.equal(attrs["items.n"]["indexing?"], undefined);
  const plain = await admin("query", { query: { items: { $: { where: { n: 5 } } } } });
  assert.equal(plain.body.items.length, 1);
  ok("remove-index completed in batches");
}

// ---------------------------------------------------------------------------
console.log("4. required / remove-required / required with missing values");
{
  const r1 = await runJobs([jobStep("required", ids.title, "items", "title")]);
  const j1 = r1.byType[`required:${ids.title}`];
  assert.equal(j1.job_status, "completed", JSON.stringify(j1));
  assert.equal(j1.job_stage, "revalidate");
  checkEstimate(j1);
  assert.ok(j1.work_completed >= 2 * N);
  assert.equal((await pullAttrs())["items.title"]["required?"], true);
  const r2 = await runJobs([jobStep("remove-required", ids.title, "items", "title")]);
  const j2 = r2.byType[`remove-required:${ids.title}`];
  assert.equal(j2.job_status, "completed");
  assert.equal(j2.work_estimate, null, "remove-required has no estimate stage");
  assert.equal((await pullAttrs())["items.title"]["required?"], false);
  const missing = items.slice(0, 5);
  assert.equal((await admin("transact", { steps: missing.map((id) => ["update", "items", id, { title: null }]) })).status, 200);
  const r3 = await runJobs([jobStep("required", ids.title, "items", "title")]);
  const j3 = r3.byType[`required:${ids.title}`];
  assert.equal(j3.job_status, "errored");
  assert.equal(j3.error, "missing-required-error");
  assert.equal(j3.job_stage, "validate");
  // legacy counts the offenders of the batch that failed validation; at
  // this test's tiny batch size the five may straddle two batches
  assert.ok(j3.error_data.count >= 1 && j3.error_data.count <= 5, `count ${j3.error_data.count}`);
  assert.equal(j3.error_data.etype, "items");
  assert.equal(j3.error_data.label, "title");
  assert.equal(j3.error_data["entity-ids"].length, j3.error_data.count);
  for (const id of j3.error_data["entity-ids"]) assert.ok(missing.includes(id), `${id} is not one of the broken items`);
  assert.equal(j3.invalid_triples_sample.length, j3.error_data.count);
  assert.equal((await pullAttrs())["items.title"]["required?"], false);
  ok("required completed / remove-required / required errored with error_data");
}

// ---------------------------------------------------------------------------
console.log("5. values too large for an index");
{
  const { byType } = await runJobs([jobStep("unique", ids.big, "items", "big"), jobStep("index", ids.big, "items", "big")]);
  for (const t of ["unique", "index"]) {
    const j = byType[`${t}:${ids.big}`];
    assert.equal(j.job_status, "errored", JSON.stringify(j));
    assert.equal(j.error, "triple-too-large-error");
    assert.equal(j.job_stage, "update-triples");
    assert.deepEqual(j.invalid_triples_sample.map((s) => s.entity_id), [bigItem]);
  }
  const a = (await pullAttrs())["items.big"];
  assert.equal(a["unique?"], false);
  assert.equal(a["index?"], false);
  assert.equal(a["setting-unique?"], undefined);
  assert.equal(a["indexing?"], undefined);
  ok("unique + index on a 2KB value errored with triple-too-large-error and rolled back");
}

// ---------------------------------------------------------------------------
console.log("6. orphaned jobs are swept up and resumed from their cursor");
{
  // A job left mid-way by a node that died: processing, owned by a dead
  // worker, stale. The sweep reclaims it and continues from its stage +
  // cursor (here: update-triples for `index` on items.tag).
  const jobId = uuid();
  const groupId = uuid();
  psql(
    DB,
    `UPDATE attrs SET is_indexed = true, indexing = true WHERE id = '${ids.tag}';
     INSERT INTO indexing_jobs (id, group_id, app_id, attr_id, job_serial_key, job_type, job_stage, job_status, worker_id, work_estimate, created_at, updated_at)
     VALUES ('${jobId}', '${groupId}', '${appId}', '${ids.tag}', 'index', 'index', 'update-triples', 'processing', 'dead-node', ${Math.floor(1.05 * N)}, now() - interval '1 hour', now() - interval '1 hour');`,
  );
  // plus one a node created and died before starting
  const waitingId = uuid();
  psql(
    DB,
    `INSERT INTO indexing_jobs (id, group_id, app_id, attr_id, job_serial_key, job_type, job_stage, job_status)
     VALUES ('${waitingId}', '${groupId}', '${appId}', '${ids.title}', 'unique', 'unique', 'update-attr-start', 'waiting');`,
  );
  const before = Date.now();
  const jobs = await waitJobs(groupId);
  const byId = Object.fromEntries(jobs.map((j) => [j.id, j]));
  assert.equal(byId[jobId].job_status, "completed", JSON.stringify(byId[jobId]));
  assert.equal(byId[jobId].job_stage, "update-attr-done");
  assert.ok(byId[jobId].work_completed >= N, `resumed job did the rewrite (${byId[jobId].work_completed})`);
  assert.equal(byId[waitingId].job_status, "completed", JSON.stringify(byId[waitingId]));
  const attrs = await pullAttrs();
  assert.equal(attrs["items.tag"]["index?"], true);
  assert.equal(attrs["items.tag"]["indexing?"], undefined);
  assert.equal(attrs["items.title"]["unique?"], true);
  assert.equal(attrs["items.title"]["setting-unique?"], undefined);
  ok(`sweep resumed a stale processing job and a forgotten waiting job (${Date.now() - before}ms)`);
}

ws.close();
console.log(`\nINDEXING JOBS TEST PASSED (${passed} checks)`);
process.exit(0);
