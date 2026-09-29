// Permissions + concurrency fuzz layer (issue #45): the seeded tx / query
// grammar of fuzz.mjs, run under generated permission rules by several
// sessions at once — a guest, two signed-in users and an admin — with
// long-lived subscriptions. Compared across servers:
//   - every transact outcome (ok, or the error type) and every one-shot
//     query result / error type, per session;
//   - at checkpoints (after the stream settles), each session's folded
//     subscription state, i.e. what the client computes from its
//     add-query-ok / refresh-ok frames, plus the refresh errors it got;
//   - bursts: transacts from several sessions (disjoint entities, so the
//     outcome can't depend on their order) and transact / add-query /
//     transact on one session sent without waiting, which exercises the
//     per-session scheduler (#44); compared by outcome and settled state.
//
// Rules are generated from the seed (view / create / update / delete per
// namespace, a bind, field rules, ruleParams) and written to both
// databases before the script runs.
//
// Usage: node fuzz-perms.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL, LEGACY_DATABASE_URL, RUST_DATABASE_URL

import {
  connect,
  settle,
  makeIdFactory,
  projectAnyResult,
  canon,
  normalize,
  psql,
  uuid,
} from "./lib.mjs";

const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
const seed = Number(process.argv[5] ?? 7);
const rounds = Number(process.argv[6] ?? 120);
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node fuzz-perms.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");

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

// legacy evicts its rule cache off the WAL feed
const RULES_SETTLE_MS = 1500;
// a checkpoint waits for the frame stream to go quiet this long
const CHECKPOINT_QUIET_MS = 900;
const SESSIONS = ["G", "U1", "U2", "ADM"];
const EMAILS = { U1: "u1@fuzz.example", U2: "u2@fuzz.example" };

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

function buildScript() {
  const rand = prng(seed * 7919 + 13);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const mk = makeIdFactory(appIdLegacy);
  const NS = "pz";
  const OWNER = "pzowner";
  const attrs = {
    id: mk(),
    p1: mk(), p2: mk(), p3: mk(),
    tnum: mk(), tstr: mk(),
    owner: mk(), ownerRev: mk(),
    ownerId: mk(), ownerName: mk(),
  };
  const entities = Array.from({ length: 10 }, () => mk());
  const owners = Array.from({ length: 3 }, () => mk());
  const names = ["ann", "bob", "cy"];
  const values = ["a", "b", "c", 1, true, null, EMAILS.U1, EMAILS.U2, "x"];
  const nums = [0, 1, 7, 49, 51, -3];
  const strs = ["alpha", "beta", "gamma", "Zed"];

  // ---- rules
  const bind = ["isMine", "auth.email != null && data.p2 == auth.email"];
  const rules = {
    [NS]: {
      bind,
      allow: {
        view: pick([
          "true",
          "auth.id != null",
          "data.p1 != 'b'",
          "data.tnum == null || data.tnum < 50",
          "isMine || data.tstr == 'alpha'",
          "data.p2 == auth.email",
          "ruleParams.k == 1 || auth.id != null",
          "isMine || data.owner != null",
        ]),
        create: pick(["true", "auth.id != null", "newData.p1 != 'c'", "false", "newData.p2 == auth.email"]),
        update: pick(["true", "auth.id != null", "isMine", "newData.p1 != 'c'", "false", "data.tnum == null || newData.tnum == null || newData.tnum >= data.tnum"]),
        delete: pick(["true", "auth.id != null", "isMine", "false", "data.tstr != 'Zed'"]),
      },
    },
    [OWNER]: {
      allow: {
        view: pick(["true", "data.name != 'cy'", "auth.id != null", "false"]),
        create: "true",
        update: "true",
        delete: "true",
      },
    },
  };
  const fields = {};
  if (chance(0.5)) fields.p3 = pick(["auth.id != null", "false", "data.p1 == 'a'"]);
  if (chance(0.4)) fields.tstr = pick(["data.tnum == null || data.tnum > 0", "isMine"]);
  if (Object.keys(fields).length) rules[NS].fields = fields;

  const schema = [
    blob(attrs.id, NS, "id", { "unique?": true, "index?": true }),
    blob(attrs.p1, NS, "p1"),
    blob(attrs.p2, NS, "p2"),
    blob(attrs.p3, NS, "p3"),
    blob(attrs.tnum, NS, "tnum", { "index?": true, "checked-data-type": "number" }),
    blob(attrs.tstr, NS, "tstr", { "index?": true, "checked-data-type": "string" }),
    blob(attrs.ownerId, OWNER, "id", { "unique?": true, "index?": true }),
    blob(attrs.ownerName, OWNER, "name", { "index?": true, "checked-data-type": "string" }),
    [
      "add-attr",
      {
        id: attrs.owner,
        "forward-identity": [attrs.owner, NS, "owner"],
        "reverse-identity": [attrs.ownerRev, OWNER, "items"],
        "value-type": "ref",
        cardinality: "one",
        "unique?": false,
        "index?": false,
        isUnsynced: true,
      },
    ],
  ];
  const seedData = [
    ...owners.flatMap((o, i) => [
      ["add-triple", o, attrs.ownerId, o],
      ["add-triple", o, attrs.ownerName, names[i]],
    ]),
    ...entities.slice(0, 6).flatMap((e) => [
      ["add-triple", e, attrs.id, e],
      ["add-triple", e, attrs.p1, pick(["a", "b", "c"])],
      ["add-triple", e, attrs.p2, pick([EMAILS.U1, EMAILS.U2, "x"])],
      ["add-triple", e, attrs.tnum, pick(nums)],
      ["add-triple", e, attrs.tstr, pick(strs)],
      ...(chance(0.6) ? [["add-triple", e, attrs.owner, pick(owners)]] : []),
    ]),
  ];

  const txSteps = (e) => {
    const n = 1 + Math.floor(rand() * 3);
    const steps = [];
    for (let j = 0; j < n; j++) {
      const k = rand();
      if (k < 0.35) {
        steps.push(["add-triple", e, attrs.id, e]);
        steps.push(["add-triple", e, pick([attrs.p1, attrs.p2, attrs.p3]), pick(values)]);
      } else if (k < 0.55) {
        steps.push(["add-triple", e, attrs.id, e]);
        steps.push(["add-triple", e, attrs.tnum, pick(nums)]);
        if (chance(0.5)) steps.push(["add-triple", e, attrs.tstr, pick(strs)]);
      } else if (k < 0.65) {
        steps.push(["add-triple", e, attrs.id, e]);
        steps.push(["deep-merge-triple", e, attrs.p3, { [`k${Math.floor(rand() * 2)}`]: pick(values) }]);
      } else if (k < 0.75) {
        steps.push(chance(0.7) ? ["add-triple", e, attrs.owner, pick(owners)] : ["retract-triple", e, attrs.owner, pick(owners)]);
      } else if (k < 0.82) {
        steps.push(["add-triple", e, attrs.id, e, { mode: pick(["create", "update"]) }]);
        steps.push(["add-triple", e, attrs.p1, pick(values)]);
      } else if (k < 0.9) {
        steps.push(["retract-triple", e, pick([attrs.p1, attrs.p2, attrs.p3]), pick(values)]);
      } else {
        steps.push(["delete-entity", e, NS]);
      }
    }
    if (chance(0.08)) steps.push(["rule-params", e, NS, { k: Math.floor(rand() * 2) }]);
    return steps;
  };

  const randomQuery = () => {
    const opts = {};
    const q = rand();
    if (q < 0.15) opts.where = { p1: pick(["a", "b", "c"]) };
    else if (q < 0.27) opts.where = { tnum: { [pick(["$gt", "$lt", "$gte"])]: pick(nums) } };
    else if (q < 0.35) opts.where = { tstr: { $in: [pick(strs), pick(strs)] } };
    else if (q < 0.43) opts.where = { "owner.name": pick(names) };
    else if (q < 0.5) opts.where = { or: [{ tnum: { $gt: pick(nums) } }, { "owner.name": pick(names) }] };
    else if (q < 0.55) opts.where = { p2: { $not: EMAILS.U1 } };
    else if (q < 0.6) opts.where = { owner: { $isNull: pick([true, false]) } };
    if (chance(0.3)) opts.fields = pick([["p1"], ["tnum", "tstr"], ["p3", "owner"], ["p2"]]);
    if (chance(0.25)) {
      opts.order = { [pick(["tnum", "tstr", "serverCreatedAt"])]: pick(["asc", "desc"]) };
      opts.limit = 1 + Math.floor(rand() * 4);
    }
    const form = Object.keys(opts).length ? { $: opts } : {};
    if (chance(0.3)) form.owner = chance(0.5) ? {} : { $: { fields: ["name"] } };
    const root = chance(0.15)
      ? { [OWNER]: { items: chance(0.5) ? {} : { $: { where: { tnum: { $gt: 0 } } } } } }
      : { [NS]: form };
    if (chance(0.1)) root.$$ruleParams = { k: 1 };
    return root;
  };

  const script = [];
  const subs = Object.fromEntries(SESSIONS.map((s) => [s, []]));
  const userSession = () => pick(["G", "U1", "U2", "G", "U1", "U2", "ADM"]);
  for (let i = 0; i < rounds; i++) {
    const r = rand();
    if (r < 0.42) {
      script.push({ kind: "tx", session: userSession(), steps: txSteps(pick(entities)) });
    } else if (r < 0.64) {
      script.push({ kind: "query", session: userSession(), q: randomQuery() });
    } else if (r < 0.76) {
      const s = userSession();
      if (subs[s].length < 4) {
        const q = randomQuery();
        if (!subs[s].some((x) => canon(x) === canon(q))) {
          subs[s].push(q);
          script.push({ kind: "subscribe", session: s, q });
        }
      }
    } else if (r < 0.8) {
      const s = pick(SESSIONS);
      if (subs[s].length) {
        const q = subs[s].splice(Math.floor(rand() * subs[s].length), 1)[0];
        script.push({ kind: "unsubscribe", session: s, q });
      }
    } else if (r < 0.9) {
      // several sessions transact at once, on disjoint entities
      const pool = [...entities];
      const ops = ["G", "U1", "U2"].filter(() => chance(0.8)).map((s) => {
        const e = pool.splice(Math.floor(rand() * pool.length), 1)[0];
        return { session: s, steps: txSteps(e) };
      });
      if (ops.length) script.push({ kind: "burst", ops });
    } else if (r < 0.95) {
      // transact / add-query / transact on one session, unawaited
      const s = pick(["U1", "U2"]);
      const [e1, e2] = [pick(entities.slice(0, 5)), pick(entities.slice(5))];
      const q = randomQuery();
      const subscribe = subs[s].length < 4 && !subs[s].some((x) => canon(x) === canon(q));
      if (subscribe) subs[s].push(q);
      script.push({ kind: "session-burst", session: s, first: txSteps(e1), q, subscribe, second: txSteps(e2) });
    } else {
      script.push({ kind: "checkpoint" });
    }
    if (i % 6 === 5) script.push({ kind: "checkpoint" });
  }
  script.push({ kind: "checkpoint" });
  for (const s of SESSIONS) script.push({ kind: "query", session: s, q: { [NS]: { owner: {} } } });
  return { rules, schema, seedData, script };
}

async function mintRefreshToken(url, appId, email) {
  const resp = await fetch(`${url}/admin/refresh_tokens`, {
    method: "POST",
    headers: { "content-type": "application/json", "app-id": appId, authorization: `Bearer ${adminToken}` },
    body: JSON.stringify({ email }),
  });
  const body = await resp.json();
  const token = body?.user?.refresh_token;
  if (!token) throw new Error(`no refresh token for ${email}: ${JSON.stringify(body).slice(0, 300)}`);
  return token;
}

const errView = (m) => ({ type: m.type, status: m.status });

async function runOn(serverName, plan) {
  const { url, db, appId } = SERVERS[serverName];
  psql(db, `INSERT INTO rules (app_id, code) VALUES ('${appId}', $rules$${JSON.stringify(plan.rules)}$rules$::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`);
  await new Promise((r) => setTimeout(r, RULES_SETTLE_MS));

  const conns = {};
  // a second connection per session, same auth, that holds no
  // subscriptions: at every one-shot query and checkpoint it asks the same
  // question afresh, so a result the session was served can be told apart
  // from what the server holds at that moment
  const probes = {};
  const open = async (s, tag) => {
    const c = connect(url, appId, `${serverName}:pz:${s}${tag}`);
    await c.open;
    const init = { "client-event-id": uuid(), op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.21.0" } };
    if (s === "ADM") init["__admin-token"] = adminToken;
    if (EMAILS[s]) init["refresh-token"] = await mintRefreshToken(url, appId, EMAILS[s]);
    c.send(init);
    const ok = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
    if (ok.op !== "init-ok") throw new Error(`[${serverName}] init ${s}${tag} failed: ${JSON.stringify(ok).slice(0, 400)}`);
    return c;
  };
  for (const s of SESSIONS) {
    conns[s] = await open(s, "");
    probes[s] = await open(s, ":probe");
  }
  const all = Object.values(conns);
  const probe = async (s, q) => {
    const c = probes[s];
    const ceid = c.send({ "client-event-id": uuid(), op: "add-query", q });
    const r = await c.waitFor(
      (m) => (m.op === "add-query-ok" && canon(m.q) === canon(q)) || (m.op === "error" && m["client-event-id"] === ceid),
      20000,
    );
    if (r.op !== "add-query-ok") return errView(r);
    c.send({ "client-event-id": uuid(), op: "remove-query", q });
    await c.waitFor((m) => m.op === "remove-query-ok" && canon(m.q) === canon(q));
    return projectAnyResult(r.result);
  };
  // op index -> the probe's answers (not part of the compared outcomes)
  const fresh = {};
  const send = (s, m) => conns[s].send({ "client-event-id": uuid(), ...m });
  const txReply = (s, ceid) =>
    conns[s].waitFor((m) => ["transact-ok", "error"].includes(m.op) && m["client-event-id"] === ceid, 20000);
  const queryReply = (s, ceid, q) =>
    conns[s].waitFor(
      (m) => ((m.op === "add-query-ok" || m.op === "add-query-exists") && canon(m.q) === canon(q)) || (m.op === "error" && m["client-event-id"] === ceid),
      20000,
    );
  const txOutcome = (r) => (r.op === "transact-ok" ? { ok: true } : errView(r));

  for (const steps of [plan.schema, plan.seedData]) {
    const ceid = send("ADM", { op: "transact", "tx-steps": steps });
    const r = await txReply("ADM", ceid);
    if (r.op !== "transact-ok") throw new Error(`[${serverName}] setup tx failed: ${JSON.stringify(r).slice(0, 500)}`);
  }
  await settle(all, CHECKPOINT_QUIET_MS);

  // folded client state per session: subscriptions (by canonical q) and the
  // refresh errors seen since the last checkpoint. Frames are consumed in
  // arrival order at every checkpoint.
  const subs = Object.fromEntries(SESSIONS.map((s) => [s, new Map()]));
  // canonical key -> the query as sent, per session (for the probe)
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
          if (subs[s].has(k)) subs[s].set(k, projectAnyResult(m.result));
        } else if (m.op === "refresh-ok") {
          for (const c of m.computations ?? []) {
            const k = canon(normalize(c["instaql-query"]));
            if (subs[s].has(k)) subs[s].set(k, projectAnyResult(c["instaql-result"]));
          }
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
        subs: Object.fromEntries([...subs[s].entries()].sort(([a], [b]) => (a < b ? -1 : 1))),
        refreshErrors: refreshErrors[s].splice(0),
      };
    }
    return out;
  };

  const outcomes = [];
  for (const [i, op] of plan.script.entries()) {
    if (op.kind === "tx") {
      const r = await txReply(op.session, send(op.session, { op: "transact", "tx-steps": op.steps }));
      outcomes.push({ i, kind: op.kind, session: op.session, ...txOutcome(r) });
    } else if (op.kind === "query") {
      const ceid = send(op.session, { op: "add-query", q: op.q });
      const r = await queryReply(op.session, ceid, op.q);
      if (r.op === "add-query-ok") {
        send(op.session, { op: "remove-query", q: op.q });
        await conns[op.session].waitFor((m) => m.op === "remove-query-ok" && canon(m.q) === canon(op.q));
      }
      outcomes.push({ i, kind: op.kind, session: op.session, q: op.q, ...(r.op === "add-query-ok" ? { result: projectAnyResult(r.result) } : r.op === "error" ? errView(r) : { op: r.op }) });
      if (r.op === "add-query-ok") fresh[i] = await probe(op.session, op.q);
    } else if (op.kind === "subscribe") {
      fold();
      subs[op.session].set(canon(normalize(op.q)), null);
      subQ[op.session].set(canon(normalize(op.q)), op.q);
      const ceid = send(op.session, { op: "add-query", q: op.q });
      const r = await queryReply(op.session, ceid, op.q);
      if (r.op !== "add-query-ok") subs[op.session].delete(canon(normalize(op.q)));
      outcomes.push({ i, kind: op.kind, session: op.session, ...(r.op === "error" ? errView(r) : { ok: r.op }) });
    } else if (op.kind === "unsubscribe") {
      fold();
      subs[op.session].delete(canon(normalize(op.q)));
      send(op.session, { op: "remove-query", q: op.q });
      await conns[op.session].waitFor((m) => m.op === "remove-query-ok" && canon(m.q) === canon(op.q));
      outcomes.push({ i, kind: op.kind, session: op.session });
    } else if (op.kind === "burst") {
      const pending = op.ops.map((o) => txReply(o.session, send(o.session, { op: "transact", "tx-steps": o.steps })));
      const replies = await Promise.all(pending);
      outcomes.push({ i, kind: op.kind, replies: replies.map(txOutcome) });
    } else if (op.kind === "session-burst") {
      const s = op.session;
      if (op.subscribe) {
        fold();
        subs[s].set(canon(normalize(op.q)), null);
        subQ[s].set(canon(normalize(op.q)), op.q);
      }
      const c1 = send(s, { op: "transact", "tx-steps": op.first });
      const cq = send(s, { op: "add-query", q: op.q });
      const c2 = send(s, { op: "transact", "tx-steps": op.second });
      const [r1, rq, r2] = await Promise.all([txReply(s, c1), queryReply(s, cq, op.q), txReply(s, c2)]);
      if (!op.subscribe && rq.op === "add-query-ok") {
        send(s, { op: "remove-query", q: op.q });
        await conns[s].waitFor((m) => m.op === "remove-query-ok" && canon(m.q) === canon(op.q));
      }
      if (op.subscribe && rq.op !== "add-query-ok") subs[s].delete(canon(normalize(op.q)));
      // the query's own result depends on where it lands between the two
      // transacts; only its success is compared (the subscription state is
      // compared once settled)
      outcomes.push({ i, kind: op.kind, session: s, first: txOutcome(r1), query: rq.op === "error" ? errView(rq) : { ok: true }, second: txOutcome(r2) });
    } else if (op.kind === "checkpoint") {
      await settle(all, CHECKPOINT_QUIET_MS);
      const state = snapshot();
      outcomes.push({ i, kind: "checkpoint", state });
      fresh[i] = {};
      for (const s of SESSIONS) {
        fresh[i][s] = {};
        for (const key of Object.keys(state[s].subs)) {
          const q = subQ[s].get(key);
          if (q) fresh[i][s][key] = await probe(s, q);
        }
      }
    }
  }
  for (const c of [...all, ...Object.values(probes)]) c.close();
  return { outcomes, fresh };
}

const plan = buildScript();
console.log(`fuzz-perms: seed=${seed} rounds=${rounds} ops=${plan.script.length}`);
console.log(`rules: ${JSON.stringify(plan.rules)}`);
console.log("running against legacy…");
const { outcomes: legacy, fresh: legacyFresh } = await runOn("legacy", plan);
console.log("running against rust…");
const { outcomes: rust, fresh: rustFresh } = await runOn("rust", plan);

// A mismatch is legacy serving a stale result when, at that moment, a fresh
// legacy session with the same auth answered exactly what rust served, and
// rust's own fresh answer agreed. Legacy caches query results per session
// and misses some invalidations; the server's data still matched.
const same = (a, b) => a !== undefined && b !== undefined && canon(a) === canon(b);
function legacyStale(l, r) {
  if (!l || !r || l.i !== r.i) return false;
  const lf = legacyFresh[l.i], rf = rustFresh[r.i];
  if (l.kind === "query") {
    if (canon({ ...l, result: undefined }) !== canon({ ...r, result: undefined })) return false;
    return same(lf, r.result) && same(rf, r.result);
  }
  if (l.kind !== "checkpoint") return false;
  for (const s of SESSIONS) {
    const ls = l.state?.[s], rs = r.state?.[s];
    if (canon(ls?.refreshErrors) !== canon(rs?.refreshErrors)) return false;
    for (const key of new Set([...Object.keys(ls?.subs ?? {}), ...Object.keys(rs?.subs ?? {})])) {
      const lv = ls?.subs?.[key], rv = rs?.subs?.[key];
      if (canon(lv) === canon(rv)) continue;
      if (!(same(lf?.[s]?.[key], rv) && same(rf?.[s]?.[key], rv))) return false;
    }
  }
  return true;
}
let staleLegacy = 0;

let mismatches = 0;
// what differs between two projected results, small enough for a CI log:
// the triples only one side has, and the page-info of each side
function resultDiff(lv, rv, indent) {
  const lt = lv?.triples ?? lv?.result?.triples, rt = rv?.triples ?? rv?.result?.triples;
  if (!Array.isArray(lt) || !Array.isArray(rt)) return;
  const key = (t) => canon(t.slice(0, 3));
  const ls = new Set(lt.map(key)), rs = new Set(rt.map(key));
  console.error(`${indent}only legacy: ${JSON.stringify(lt.filter((t) => !rs.has(key(t))).map((t) => t.slice(0, 3)))}`);
  console.error(`${indent}only rust:   ${JSON.stringify(rt.filter((t) => !ls.has(key(t))).map((t) => t.slice(0, 3)))}`);
  const lp = lv?.["page-info"] ?? lv?.result?.["page-info"], rp = rv?.["page-info"] ?? rv?.result?.["page-info"];
  if (canon(lp) !== canon(rp)) console.error(`${indent}page-info legacy ${JSON.stringify(lp)} rust ${JSON.stringify(rp)}`);
}

const n = Math.max(legacy.length, rust.length);
for (let k = 0; k < n; k++) {
  const l = legacy[k], r = rust[k];
  if (canon(l) === canon(r)) continue;
  if (legacyStale(l, r)) {
    staleLegacy++;
    console.log(`legacy served a stale result at op ${l.i} (${l.kind}); a fresh legacy session agreed with rust`);
    continue;
  }
  mismatches++;
  if (mismatches > 12) continue;
  const op = plan.script[l?.i ?? r?.i];
  console.error(`mismatch at op ${l?.i ?? r?.i} (${op?.kind}${op?.session ? ` on ${op.session}` : ""}):`);
  if (op && op.kind !== "checkpoint") console.error(`  op:     ${JSON.stringify(op).slice(0, 1500)}`);
  if (op?.kind === "checkpoint") {
    // name the differing session / subscription
    for (const s of SESSIONS) {
      for (const key of new Set([...Object.keys(l?.state?.[s]?.subs ?? {}), ...Object.keys(r?.state?.[s]?.subs ?? {})])) {
        const lv = l?.state?.[s]?.subs?.[key], rv = r?.state?.[s]?.subs?.[key];
        if (canon(lv) !== canon(rv)) {
          console.error(`  ${s} subscription ${key}:`);
          console.error(`    legacy: ${JSON.stringify(lv)?.slice(0, 2500)}`);
          console.error(`    rust:   ${JSON.stringify(rv)?.slice(0, 2500)}`);
          resultDiff(lv, rv, "    ");
        }
      }
      const le = l?.state?.[s]?.refreshErrors, re = r?.state?.[s]?.refreshErrors;
      if (canon(le) !== canon(re)) console.error(`  ${s} refresh errors: legacy ${JSON.stringify(le)} rust ${JSON.stringify(re)}`);
    }
  } else {
    console.error(`  legacy: ${JSON.stringify(l)?.slice(0, 2500)}`);
    console.error(`  rust:   ${JSON.stringify(r)?.slice(0, 2500)}`);
    resultDiff(l, r, "  ");
  }
}
const txTotal = legacy.filter((o) => o.kind === "tx").length;
const txOkLegacy = legacy.filter((o) => o.kind === "tx" && o.ok).length;
const viewedLegacy = legacy.filter((o) => o.kind === "query" && o.result?.triples?.length).length;
console.log(`legacy: ${txOkLegacy}/${txTotal} user transacts committed, ${viewedLegacy} one-shot queries returned data`);
// a script whose rules deny everything compares nothing but errors; that is
// still a valid comparison (rules must deny alike), but say so
if (!txOkLegacy && !viewedLegacy) console.log("note: the generated rules denied every user transact and hid every result");
if (mismatches) {
  console.error(`FUZZ-PERMS FAILED: ${mismatches} mismatches (seed ${seed})`);
  process.exit(1);
}
console.log(`FUZZ-PERMS PASSED: ${plan.script.length} ops, ${legacy.filter((o) => o.kind === "checkpoint").length} checkpoints identical, ${staleLegacy} stale legacy results (seed ${seed})`);
