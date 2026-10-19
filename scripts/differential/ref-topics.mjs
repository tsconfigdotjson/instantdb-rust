// Differential layer for reactive invalidation of queries whose permission
// rules read linked entities (issue #59). Legacy runs a rule's `data.ref` /
// `auth.ref` prefetch through the reactive datalog-query-fn (cel.clj:1752-1800
// prefetch-data-refs, :116-137 get-ref-many), which in a subscription is
// datalog-query-reactive! (reactive/query.clj:35-50): every lookup a check
// makes is recorded against the subscription with its topics, so a write
// refreshes the subscriptions whose own data or ref targets it touched and
// no others. Rust records the topics of the same lookups
// (perms::resolve_ref_path) next to the query's own.
//
// Three signed-in / guest sessions hold subscriptions whose view and field
// rules use `data.ref` through one hop, two hops and a reverse link, and
// `auth.ref` through a reverse link from `$users`; nested link forms and a
// where path reach the same rules. Then an admin writes, one tx per phase:
//   a*  the ref targets (memberships added / removed / moved, an org
//       renamed), so visibility flips;
//   b*  unrelated data (another namespace, an org nobody's docs are in);
//   c*  the queried entities themselves.
// Compared per phase and session: the queries each session got a
// `refresh-ok` computation for, with the results (what the client applies,
// lib.mjs projectAnyResult), and refresh errors; at the end, every session's
// folded subscription state. Paths: `reftopics:<phase>/<session>` and
// `reftopics:final/<session>`.
//
// Rust only: the `/metrics` refresh counters must show that a write to an
// unrelated namespace recomputes none of these subscriptions (they used to be
// catch-all for the whole app) and a ref-target write recomputes some.
//
// Usage: node ref-topics.mjs <app-id> <admin-token>
//   (the app must exist on both servers with the same admin token, see
//    provision.sh)
// Env: LEGACY_URL / RUST_URL, LEGACY_DATABASE_URL / RUST_DATABASE_URL
//      (rules via psql), DUMP=1 prints each server's recorded values

import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  canon,
  connect,
  DIVERGENCE_PREFIXES,
  describeAllowVerdict,
  firstDifference,
  loadAllowlist,
  makeIdFactory,
  normalize,
  projectAnyResult,
  psql,
  settle,
} from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken) throw new Error("usage: node ref-topics.mjs <app-id> <admin-token>");

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
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// rules written straight to Postgres: legacy evicts its rule cache off the
// WAL feed, so give it a beat before the next op
const RULES_SETTLE_MS = 1500;
// a phase is over once every session has been frame-silent this long
const PHASE_QUIET_MS = 1200;

// ---------------------------------------------------------------------------
// schema, rows, rules

const mk = makeIdFactory(appId);
const ids = {
  orgId: mk(), orgName: mk(),
  memId: mk(), memEmail: mk(), memOrg: mk(), memOrgRev: mk(), memUser: mk(), memUserRev: mk(),
  docId: mk(), docTitle: mk(), docBody: mk(), docOrg: mk(), docOrgRev: mk(),
  taskId: mk(), taskTitle: mk(), taskDoc: mk(), taskDocRev: mk(),
  boardId: mk(), boardName: mk(),
  noiseId: mk(), noiseVal: mk(),
  acme: mk(), beta: mk(), gamma: mk(), delta: mk(),
  m1: mk(), m2: mk(), m3: mk(), m4: mk(),
  d1: mk(), d2: mk(), d3: mk(), d4: mk(), d5: mk(),
  k1: mk(), k2: mk(), k3: mk(),
  b1: mk(), b2: mk(),
  n1: mk(), n2: mk(),
};
const U1_EMAIL = "reftopics-u1@example.com";
const U2_EMAIL = "reftopics-u2@example.com";

const attrStep = (id, etype, label) => [
  "add-attr",
  { id, "forward-identity": [id, etype, label], "value-type": "blob", cardinality: "one", "unique?": label === "id", "index?": label === "id", isUnsynced: true },
];
const linkStep = (id, etype, label, revId, revEtype, revLabel) => [
  "add-attr",
  { id, "forward-identity": [id, etype, label], "reverse-identity": [revId, revEtype, revLabel], "value-type": "ref", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true },
];
const ent = (e, idAttr, fields) => [["add-triple", e, idAttr, e], ...fields.map(([a, v]) => ["add-triple", e, a, v])];
const org = (e, name) => ent(e, ids.orgId, [[ids.orgName, name]]);
const member = (e, email, orgE, userId) => ent(e, ids.memId, [[ids.memEmail, email], [ids.memOrg, orgE], ...(userId ? [[ids.memUser, userId]] : [])]);
const doc = (e, title, orgE) => ent(e, ids.docId, [[ids.docTitle, title], [ids.docBody, `${title} body`], [ids.docOrg, orgE]]);
const task = (e, title, docE) => ent(e, ids.taskId, [[ids.taskTitle, title], [ids.taskDoc, docE]]);

const schemaSteps = () => [
  attrStep(ids.orgId, "rtOrgs", "id"), attrStep(ids.orgName, "rtOrgs", "name"),
  attrStep(ids.memId, "rtMembers", "id"), attrStep(ids.memEmail, "rtMembers", "email"),
  linkStep(ids.memOrg, "rtMembers", "org", ids.memOrgRev, "rtOrgs", "members"),
  linkStep(ids.memUser, "rtMembers", "user", ids.memUserRev, "$users", "rtMembers"),
  attrStep(ids.docId, "rtDocs", "id"), attrStep(ids.docTitle, "rtDocs", "title"), attrStep(ids.docBody, "rtDocs", "body"),
  linkStep(ids.docOrg, "rtDocs", "org", ids.docOrgRev, "rtOrgs", "docs"),
  attrStep(ids.taskId, "rtTasks", "id"), attrStep(ids.taskTitle, "rtTasks", "title"),
  linkStep(ids.taskDoc, "rtTasks", "doc", ids.taskDocRev, "rtDocs", "tasks"),
  attrStep(ids.boardId, "rtBoards", "id"), attrStep(ids.boardName, "rtBoards", "name"),
  attrStep(ids.noiseId, "rtNoise", "id"), attrStep(ids.noiseVal, "rtNoise", "val"),
];
// acme: d1, d2 (U1 is a member); beta: d3 (no members); gamma: d4 (no
// members); tasks k1 on d1, k2 on d3
const seedSteps = (users) => [
  ...org(ids.acme, "acme"), ...org(ids.beta, "beta"), ...org(ids.gamma, "gamma"),
  ...member(ids.m1, U1_EMAIL, ids.acme, users.U1.id),
  ...doc(ids.d1, "d1", ids.acme), ...doc(ids.d2, "d2", ids.acme), ...doc(ids.d3, "d3", ids.beta), ...doc(ids.d4, "d4", ids.gamma),
  ...task(ids.k1, "k1", ids.d1), ...task(ids.k2, "k2", ids.d3),
  ...ent(ids.b1, ids.boardId, [[ids.boardName, "b1"]]), ...ent(ids.b2, ids.boardId, [[ids.boardName, "b2"]]),
  ...ent(ids.n1, ids.noiseId, [[ids.noiseVal, 1]]),
];

const rules = {
  rtOrgs: { allow: { view: "true" } },
  rtMembers: { allow: { view: "true" } },
  // forward hop, reverse hop, terminal attr; a field rule with a one-hop ref
  rtDocs: {
    allow: { view: "auth.email != null && auth.email in data.ref('org.members.email')" },
    fields: { body: "'acme' in data.ref('org.name')" },
  },
  // two forward hops, visible to guests too
  rtTasks: { allow: { view: "'acme' in data.ref('doc.org.name')" } },
  // auth.ref through the reverse link from $users
  rtBoards: { allow: { view: "'acme' in auth.ref('$user.rtMembers.org.name')" } },
  rtNoise: { allow: { view: "true" } },
};

// the subscriptions, per session
const QUERIES = {
  docs: { rtDocs: {} },
  docsTasks: { rtDocs: { tasks: {} } },
  tasks: { rtTasks: {} },
  tasksByDoc: { rtTasks: { $: { where: { "doc.title": "d1" } } } },
  boards: { rtBoards: {} },
  orgsDocs: { rtOrgs: { docs: {} } },
};
const SESSIONS = {
  U1: ["docs", "docsTasks", "tasks", "tasksByDoc", "boards", "orgsDocs"],
  U2: ["docs", "docsTasks", "tasks", "boards", "orgsDocs"],
  G: ["docs", "tasks", "orgsDocs"],
};
const queryName = new Map(Object.entries(QUERIES).map(([k, q]) => [canon(normalize(q)), k]));

// One admin tx per phase. `rust` pins what the rust server's refresh
// counters must show for it: "none" recomputed, or "some".
const PHASES = [
  { name: "a1-member-added", rust: "some", steps: (u) => member(ids.m2, U2_EMAIL, ids.acme, u.U2.id) },
  { name: "a2-member-removed", steps: () => [["delete-entity", ids.m1, "rtMembers"]] },
  { name: "a3-org-renamed", steps: () => [["add-triple", ids.acme, ids.orgName, "acme2"]] },
  { name: "a4-org-renamed-back", steps: () => [["add-triple", ids.acme, ids.orgName, "acme"]] },
  // U2 leaves acme for beta, whose doc had no member chain before
  { name: "a5-member-moved", steps: () => [["add-triple", ids.m2, ids.memOrg, ids.beta]] },
  // U1 joins gamma, whose doc d4 had no member chain: the only lookup
  // that finds the new membership is the reverse hop from gamma
  { name: "a6-member-joins-empty-org", steps: (u) => member(ids.m3, U1_EMAIL, ids.gamma, u.U1.id) },
  // the second hop of tasks' rule: k2's doc moves to acme
  { name: "a7-task-doc-moved", steps: () => [["add-triple", ids.d3, ids.docOrg, ids.acme]] },
  { name: "b1-other-namespace", rust: "none", steps: () => [...ent(ids.n2, ids.noiseId, [[ids.noiseVal, 2]]), ["add-triple", ids.n1, ids.noiseVal, 3]] },
  { name: "b2-org-without-docs", steps: () => [...org(ids.delta, "delta"), ...member(ids.m4, "nobody@example.com", ids.delta)] },
  { name: "b3-member-of-org-without-docs", rust: "none", steps: () => [["add-triple", ids.m4, ids.memEmail, "still-nobody@example.com"]] },
  { name: "c1-doc-retitled", steps: () => [["add-triple", ids.d1, ids.docTitle, "d1 renamed"]] },
  { name: "c2-doc-created", steps: () => [...doc(ids.d5, "d5", ids.beta), ...task(ids.k3, "k3", ids.d5)] },
  { name: "c3-task-deleted", steps: () => [["delete-entity", ids.k1, "rtTasks"]] },
  { name: "c4-doc-relinked", steps: () => [["add-triple", ids.d2, ids.docOrg, ids.beta]] },
];

// ---------------------------------------------------------------------------
// helpers

const ceids = (() => {
  let i = 0;
  return () => {
    i++;
    return `00000000-0000-4000-9000-${String(i).padStart(12, "0")}`;
  };
})();

async function mintUser(url, email) {
  const resp = await fetch(`${url}/admin/refresh_tokens`, {
    method: "POST",
    headers: { "content-type": "application/json", "app-id": appId, authorization: `Bearer ${adminToken}` },
    body: JSON.stringify({ email }),
  });
  const body = await resp.json();
  const user = body?.user;
  if (!user?.refresh_token || !user?.id) throw new Error(`no refresh token for ${email} from ${url}: ${JSON.stringify(body).slice(0, 300)}`);
  return { id: user.id, token: user.refresh_token };
}

// rust's /metrics counters (instant-server metrics.rs)
async function rustCounters() {
  const text = await (await fetch(`${SERVERS.rust.url}/metrics`)).text();
  const get = (name) => {
    const m = text.match(new RegExp(`^${name} (\\d+)`, "m"));
    if (!m) throw new Error(`rust /metrics has no ${name}`);
    return Number(m[1]);
  };
  return {
    batches: get("instant_refresh_batch_seconds_count"),
    computed: get("instant_refresh_queries_computed_total"),
    skipped: get("instant_refresh_queries_skipped_total"),
  };
}

const errView = (m) => normalize({ status: m.status, type: m.type, message: m.message });

// What a session's frames since `from` did to its subscriptions: the
// queries it got a computation for (the client applies each one,
// Reactor.js handleRefreshOk), with the result, and any error.
function phaseView(conn, from) {
  const refreshed = {};
  const errors = [];
  for (const f of conn.frames.slice(from)) {
    if (f.op === "refresh-ok") {
      for (const c of f.computations ?? []) {
        const k = queryName.get(canon(normalize(c["instaql-query"]))) ?? canon(normalize(c["instaql-query"]));
        refreshed[k] = projectAnyResult(c["instaql-result"]);
      }
    } else if (f.op === "error") {
      errors.push(errView(f));
    }
  }
  return { refreshed, errors };
}

// ---------------------------------------------------------------------------
// scenario

async function run(name, out, pinFailures) {
  const { url, db } = SERVERS[name];
  const record = (k, v) => {
    out[k] = v;
    if (process.env.DUMP) console.log(`[${name}] ${k}`, JSON.stringify(v));
  };
  const msg = (conn, m) => conn.send({ "client-event-id": ceids(), ...m });

  const admin = connect(url, appId, `${name}:ADMIN`);
  await admin.open;
  msg(admin, { op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.22.0" }, "__admin-token": adminToken });
  await admin.waitFor((m) => m.op === "init-ok");
  const adminTx = async (steps, what) => {
    const ceid = msg(admin, { op: "transact", "tx-steps": steps });
    const r = await admin.waitFor((m) => (m.op === "transact-ok" || m.op === "error") && m["client-event-id"] === ceid);
    if (r.op === "error") throw new Error(`${what} failed on ${name}: ${JSON.stringify(r).slice(0, 500)}`);
  };

  const users = { U1: await mintUser(url, U1_EMAIL), U2: await mintUser(url, U2_EMAIL) };
  await adminTx(schemaSteps(), "schema");
  await adminTx(seedSteps(users), "seed");
  psql(db, `INSERT INTO rules (app_id, code) VALUES ('${appId}', $rules$${JSON.stringify(rules)}$rules$::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`);
  await sleep(RULES_SETTLE_MS);

  // sessions open after the schema exists, so no attrs refresh is pending
  const conns = {};
  for (const s of Object.keys(SESSIONS)) {
    const c = (conns[s] = connect(url, appId, `${name}:${s}`));
    await c.open;
    const token = users[s]?.token;
    msg(c, { op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.22.0" }, ...(token ? { "refresh-token": token } : {}) });
    await c.waitFor((m) => m.op === "init-ok");
  }
  let subscriptions = 0;
  for (const [s, names] of Object.entries(SESSIONS)) {
    const initial = {};
    for (const qn of names) {
      const q = QUERIES[qn];
      const ceid = msg(conns[s], { op: "add-query", q });
      const r = await conns[s].waitFor((m) => (m.op === "add-query-ok" && canon(m.q) === canon(q)) || (m.op === "error" && m["client-event-id"] === ceid));
      initial[qn] = r.op === "error" ? errView(r) : projectAnyResult(r.result);
      subscriptions++;
    }
    record(`00-subscribed/${s}`, initial);
  }
  const sessionConns = Object.values(conns);
  await settle(sessionConns, PHASE_QUIET_MS);

  for (const phase of PHASES) {
    const marks = Object.fromEntries(Object.entries(conns).map(([s, c]) => [s, c.frames.length]));
    const before = name === "rust" && phase.rust ? await rustCounters() : null;
    await adminTx(phase.steps(users), phase.name);
    if (before) {
      // the tx's refresh batch has run once its duration is observed
      for (let i = 0; i < 200; i++) {
        if ((await rustCounters()).batches > before.batches) break;
        await sleep(50);
      }
    }
    await settle(sessionConns, PHASE_QUIET_MS);
    for (const [s, c] of Object.entries(conns)) record(`${phase.name}/${s}`, phaseView(c, marks[s]));
    if (before) {
      const after = await rustCounters();
      const computed = after.computed - before.computed;
      const skipped = after.skipped - before.skipped;
      const line = `rust ${phase.name}: ${computed} recomputed, ${skipped} skipped by topics (${subscriptions} subscriptions)`;
      console.log(line);
      if (after.batches === before.batches) pinFailures.push(`${line}: no refresh batch ran for the tx`);
      else if (phase.rust === "none" && (computed !== 0 || skipped < subscriptions)) pinFailures.push(`${line}: expected none recomputed, every subscription skipped`);
      else if (phase.rust === "some" && computed === 0) pinFailures.push(`${line}: expected the ref-target write to recompute some`);
    }
  }

  // the client's folded state: initial results, then every computation
  for (const [s, c] of Object.entries(conns)) {
    const state = {};
    for (const f of c.frames) {
      if (f.op === "add-query-ok") {
        const k = queryName.get(canon(normalize(f.q)));
        if (k) state[k] = projectAnyResult(f.result);
      }
    }
    Object.assign(state, phaseView(c, 0).refreshed);
    record(`final/${s}`, state);
  }
  for (const c of [admin, ...sessionConns]) c.close();
}

// ---------------------------------------------------------------------------
// run + diff

const results = { legacy: {}, rust: {} };
const pinFailures = [];
console.log("ref-topics: legacy…");
await run("legacy", results.legacy, pinFailures);
console.log("ref-topics: rust…");
await run("rust", results.rust, pinFailures);

// strict entries (lib.mjs loadAllowlist): this layer owns the reftopics:
// paths and fails on any of them that allowed nothing
const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), { prefixes: DIVERGENCE_PREFIXES.reftopics });
const diffs = [];
for (const k of new Set([...Object.keys(results.legacy), ...Object.keys(results.rust)])) {
  const l = results.legacy[k] ?? null;
  const r = results.rust[k] ?? null;
  if (canon(l) === canon(r)) continue;
  const p = `reftopics:${k}`;
  const verdict = allowlist.check(p, l, r);
  diffs.push({ path: p, allowed: !!verdict.entry, verdict, legacy: l, rust: r });
}
const blocking = diffs.filter((d) => !d.allowed);
for (const d of diffs) {
  console.log(`\n[${d.allowed ? "ALLOWED" : "DIVERGENCE"}] ${d.path}`);
  const fd = firstDifference(d.legacy, d.rust);
  if (fd && fd.p) console.log(`  first differing sub-path: ${fd.p}`);
  console.log("  legacy:", JSON.stringify(d.legacy)?.slice(0, 4000));
  console.log("  rust:  ", JSON.stringify(d.rust)?.slice(0, 4000));
  for (const line of describeAllowVerdict(d.verdict)) console.log(line);
}
for (const f of pinFailures) console.log(`\n[RUST PIN FAILED] ${f}`);
const staleAllows = allowlist.reportStale();
if (blocking.length || staleAllows || pinFailures.length) {
  console.error(`\nREF-TOPICS DIFFERENTIAL FAILED: ${blocking.length} unallowed divergences, ${staleAllows} stale allowlist entries, ${pinFailures.length} rust narrowing pins failed`);
  process.exit(1);
}
console.log(`\nREF-TOPICS DIFFERENTIAL PASSED: ${PHASES.length} phases, ${Object.keys(results.rust).length} compared values, ${diffs.length} allowed divergences`);
