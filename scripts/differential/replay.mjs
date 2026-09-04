// Differential replay: runs an identical op script against the legacy server
// and the rust server, folds each server's frames into client-visible state
// (see lib.mjs), and diffs the results. Every remaining diff must be listed
// in allowed-divergences.json with a client-code citation.
//
// Usage: node replay.mjs <app-id-legacy> <app-id-rust> <admin-token>
//   (app ids may be equal; both apps must exist on their server, see provision.sh)
// Env: LEGACY_URL (default http://localhost:8891), RUST_URL (default http://localhost:8888),
//      DUMP_STEPS=<step-name>[,...] prints the folded frames of those steps per server,
//      DUMP_OPS=1 prints the raw op sequence of every step per server and connection,
//      LEGACY_DATABASE_URL, RUST_DATABASE_URL (for rules setup via psql)

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  connect,
  connectSse,
  httpConn,
  projectResult,
  projectAttrs,
  settle,
  foldFrames,
  newState,
  projectState,
  makeIdFactory,
  canon,
  normalize,
  psql,
  uuid,
} from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node replay.mjs <app-id-legacy> <app-id-rust> <admin-token>");

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

// ---------------------------------------------------------------------------
// scenario: identical client input on both servers (ids from the deterministic
// factory), breadth across queries, transactions, errors, rooms, sync tables
// and streams.

function buildScenario() {
  const mk = makeIdFactory(appIdLegacy);
  const ids = {
    todosId: mk(), todosTitle: mk(), todosDone: mk(), todosScore: mk(),
    ownerRef: mk(), ownersId: mk(), ownersName: mk(),
    e1: mk(), e2: mk(), e3: mk(), owner1: mk(), extraAttr: mk(),
    mergeAttr: mk(), lookupTitleEid: mk(),
    // typed-query namespace (step 14)
    typedId: mk(), typedScore: mk(), typedName: mk(), typedWhen: mk(), typedFlag: mk(),
    t1: mk(), t2: mk(), t3: mk(), t4: mk(), t5: mk(),
    // authed perms namespace (step 15)
    secretsId: mk(), secretsOwner: mk(), secretsTitle: mk(),
    s1: mk(), s2: mk(), s3: mk(), s4: mk(),
    // issue #10 parity polish (steps 17+)
    nicknameAttr: mk(),
    reqsId: mk(), reqsTitle: mk(), reqsSecret: mk(), r1: mk(), r2: mk(), r3: mk(),
    limitedId: mk(), limitedTitle: mk(), l1: mk(), l2: mk(), l3: mk(),
    // admin SSE transports (issue #8, steps 23-24)
    sseTodo: mk(), sseSecret: mk(), sseTodo2: mk(),
    // audit follow-ups (steps 25-28)
    dupTitleAttr: mk(), reqAttr: mk(), laterReqAttr: mk(), ownersHandle: mk(),
    articlesId: mk(), remarksId: mk(), remarksArticle: mk(), a1: mk(), r1: mk(),
    gatedId: mk(), gatedTitle: mk(), g1: mk(), g2: mk(), g3: mk(), fakeUser: mk(),
    // issue #29 parity (steps 29-34)
    celId: mk(), celTitle: mk(), celScore: mk(), celWhen: mk(), c1: mk(), c2: mk(), c3: mk(), c4: mk(),
    vfId: mk(), vfTitle: mk(), vfOwner: mk(), v1: mk(), v2: mk(),
    projectsId: mk(), projectsName: mk(), tasksId: mk(), tasksTitle: mk(), tasksProject: mk(),
    p1: mk(), p2: mk(), k1: mk(), k2: mk(), k3: mk(),
    dynAttrOk: mk(), dynAttrDenied: mk(), m1: mk(), m2: mk(),
    // stream ids must be v4-shaped uuids on both servers
    stream2Token: "00000000-0000-4000-8000-00000000a5ee",
  };
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  // rules written straight to Postgres: legacy evicts its rule cache off the
  // WAL feed, so give it a beat before the next op
  const RULES_SETTLE_MS = 1200;
  const attr = (id, etype, label, { unique = false, fwd, rev } = {}) => [
    "add-attr",
    {
      id,
      "forward-identity": [fwd ?? mk(), etype, label],
      "value-type": "blob",
      cardinality: "one",
      "unique?": unique,
      "index?": unique,
      isUnsynced: true,
    },
  ];
  const ceids = (() => { let i = 0; return () => { i++; return `00000000-0000-4000-9000-${String(i).padStart(12, "0")}`; }; })();
  const msg = (conn, m) => conn.send({ "client-event-id": ceids(), ...m });

  const steps = [
    {
      name: "01-init",
      run: async (env) => {
        for (const name of ["A", "B"]) {
          env.conns[name] = connect(env.url, env.appId, `${env.serverName}:${name}`);
          await env.conns[name].open;
        }
        env.conns.ADMIN = connect(env.url, env.appId, `${env.serverName}:ADMIN`);
        await env.conns.ADMIN.open;
        msg(env.conns.A, { op: "init", "app-id": env.appId, versions: { "@instantdb/core": "v0.21.0" } });
        msg(env.conns.B, { op: "init", "app-id": env.appId, versions: { "@instantdb/core": "v0.21.0" } });
        msg(env.conns.ADMIN, { op: "init", "app-id": env.appId, versions: { "@instantdb/core": "v0.21.0" }, "__admin-token": adminToken });
        await env.conns.A.waitFor((m) => m.op === "init-ok");
        await env.conns.B.waitFor((m) => m.op === "init-ok");
        await env.conns.ADMIN.waitFor((m) => m.op === "init-ok");
      },
    },
    {
      name: "02-schema-and-seed",
      run: async (env) => {
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [
            attr(ids.todosId, "todos", "id", { unique: true, fwd: ids.todosId }),
            attr(ids.todosTitle, "todos", "title", { fwd: ids.todosTitle }),
            attr(ids.todosDone, "todos", "done", { fwd: ids.todosDone }),
            attr(ids.todosScore, "todos", "score", { unique: true, fwd: ids.todosScore }),
            attr(ids.ownersId, "owners", "id", { unique: true, fwd: ids.ownersId }),
            attr(ids.ownersName, "owners", "name", { fwd: ids.ownersName }),
            attr(ids.ownersHandle, "owners", "handle", { unique: true, fwd: ids.ownersHandle }),
            [
              "add-attr",
              {
                id: ids.ownerRef,
                "forward-identity": [ids.ownerRef, "todos", "owner"],
                "reverse-identity": [mk(), "owners", "todos"],
                "value-type": "ref",
                cardinality: "many",
                "unique?": false,
                "index?": false,
                isUnsynced: true,
              },
            ],
            ["add-triple", ids.e1, ids.todosId, ids.e1],
            ["add-triple", ids.e1, ids.todosTitle, "one"],
            ["add-triple", ids.e1, ids.todosDone, false],
            ["add-triple", ids.e1, ids.todosScore, 1],
            ["add-triple", ids.owner1, ids.ownersId, ids.owner1],
            ["add-triple", ids.owner1, ids.ownersName, "alice"],
            ["add-triple", ids.e1, ids.ownerRef, ids.owner1],
          ],
        });
        await env.conns.A.waitFor((m) => m.op === "transact-ok");
      },
    },
    {
      name: "03-add-query",
      run: async (env) => {
        msg(env.conns.B, { op: "add-query", q: { todos: {} } });
        await env.conns.B.waitFor((m) => m.op === "add-query-ok");
        msg(env.conns.B, { op: "add-query", q: { todos: {} } });
        await env.conns.B.waitFor((m) => m.op === "add-query-exists");
        msg(env.conns.B, { op: "add-query", q: { todos: { owner: {} } } });
        await env.conns.B.waitFor(
          (m) => m.op === "add-query-ok" && m.q?.todos?.owner,
        );
      },
    },
    {
      name: "04-refresh",
      run: async (env) => {
        msg(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.e1, ids.todosDone, true]] });
        await env.conns.A.waitFor((m) => m.op === "transact-ok");
        await env.conns.B.waitFor((m) => m.op === "refresh-ok");
      },
    },
    {
      name: "05-new-attr-refresh",
      run: async (env) => {
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [
            attr(ids.extraAttr, "todos", "extra", { fwd: ids.extraAttr }),
            ["add-triple", ids.e1, ids.extraAttr, "x"],
          ],
        });
        await env.conns.A.waitFor((m) => m.op === "transact-ok" && env.conns.A.frames.filter((f) => f.op === "transact-ok").length >= 3);
        await env.conns.B.waitFor((m) => m.op === "refresh-ok" && "attrs" in m);
      },
    },
    {
      name: "06-pagination-cursor-roundtrip",
      run: async (env) => {
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [
            ["add-triple", ids.e2, ids.todosId, ids.e2],
            ["add-triple", ids.e2, ids.todosTitle, "two"],
            ["add-triple", ids.e3, ids.todosId, ids.e3],
            ["add-triple", ids.e3, ids.todosTitle, "three"],
          ],
        });
        await env.conns.A.waitFor((m) => m.op === "transact-ok" && env.conns.A.frames.filter((f) => f.op === "transact-ok").length >= 4);
        msg(env.conns.A, { op: "add-query", q: { todos: { $: { limit: 2, order: { serverCreatedAt: "asc" } } } } });
        const page = await env.conns.A.waitFor((m) => m.op === "add-query-ok");
        const cursor = page.result?.[0]?.data?.["page-info"]?.todos?.["end-cursor"];
        // per-server cursor round-trip (cursor timestamps are server-local)
        msg(env.conns.A, { op: "add-query", q: { todos: { $: { limit: 2, order: { serverCreatedAt: "asc" }, after: cursor } } } });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.todos?.$?.after);
      },
    },
    {
      name: "07-mutation-breadth",
      run: async (env) => {
        // deep-merge, retract, lookup-ref write, delete-entity
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [
            attr(ids.mergeAttr, "todos", "meta", { fwd: ids.mergeAttr }),
            ["add-triple", ids.e2, ids.mergeAttr, { a: 1, keep: true }],
            ["deep-merge-triple", ids.e2, ids.mergeAttr, { a: 2, b: { c: 3 } }],
            ["retract-triple", ids.e1, ids.ownerRef, ids.owner1],
            ["add-triple", [ids.todosScore, 1], ids.todosTitle, "via-lookup"],
            ["delete-entity", ids.e3, "todos"],
          ],
        });
        await env.conns.A.waitFor((m) => m.op === "transact-ok" && env.conns.A.frames.filter((f) => f.op === "transact-ok").length >= 5);
        msg(env.conns.A, { op: "add-query", q: { todos: { $: { where: { done: true } } } } });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.todos?.$?.where);
      },
    },
    {
      name: "08-remove-query",
      run: async (env) => {
        msg(env.conns.B, { op: "remove-query", q: { todos: { owner: {} } } });
        await env.conns.B.waitFor((m) => m.op === "remove-query-ok");
      },
    },
    {
      name: "09-aggregate-admin",
      run: async (env) => {
        msg(env.conns.ADMIN, { op: "add-query", q: { todos: { $: { aggregate: "count" } } } });
        await env.conns.ADMIN.waitFor((m) => m.op === "add-query-ok");
      },
    },
    {
      name: "10-error-matrix",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        await expectErr(env.conns.A, { op: "add-query", q: { todos: { $: { order: { title: "asc" } } } } });
        await expectErr(env.conns.A, { op: "add-query", q: { todos: { $: { aggregate: "count" } } } });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["bogus-step"]] });
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", mk(), ids.todosTitle, "x", { mode: "update" }]],
        });
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", ids.e1, ids.todosTitle, "again", { mode: "create" }]],
        });
        await expectErr(env.conns.B, { op: "set-presence", "room-id": "never", data: {} });
        await expectErr(env.conns.B, { op: "start-sync", q: { todos: {} } });
        await expectErr(env.conns.B, { op: "subscribe-stream" });
      },
    },
    {
      name: "11-rooms",
      run: async (env) => {
        msg(env.conns.A, { op: "join-room", "room-type": "chat", "room-id": "r1", data: { who: "A" } });
        await env.conns.A.waitFor((m) => m.op === "join-room-ok");
        msg(env.conns.B, { op: "join-room", "room-type": "chat", "room-id": "r1", data: { who: "B" } });
        await env.conns.B.waitFor((m) => m.op === "join-room-ok");
        msg(env.conns.B, { op: "set-presence", "room-id": "r1", data: { who: "B", cursor: { x: 1 } } });
        await settle(Object.values(env.conns), 800);
        msg(env.conns.B, { op: "client-broadcast", "room-id": "r1", roomType: "chat", topic: "emoji", data: { e: "🔥" } });
        await env.conns.A.waitFor((m) => m.op === "server-broadcast");
        msg(env.conns.B, { op: "leave-room", "room-id": "r1" });
        await env.conns.B.waitFor((m) => m.op === "leave-room-ok");
      },
    },
    {
      name: "12-sync-table",
      run: async (env) => {
        msg(env.conns.ADMIN, { op: "start-sync", q: { todos: {} } });
        await env.conns.ADMIN.waitFor((m) => m.op === "sync-init-finish");
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", ids.e2, ids.todosTitle, "two-b"]],
        });
        await env.conns.ADMIN.waitFor((m) => m.op === "sync-update-triples");
      },
    },
    {
      name: "13-streams",
      run: async (env) => {
        psql(
          env.db,
          `INSERT INTO rules (app_id, code) VALUES ('${env.appId}', '{"$streams": {"allow": {"create": "true", "view": "true"}}}'::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`,
        );
        msg(env.conns.A, { op: "start-stream", "client-id": "diff-stream", "reconnect-token": "00000000-0000-4000-8000-00000000feed" });
        await env.conns.A.waitFor((m) => m.op === "start-stream-ok");
        const streamId = env.conns.A.frames.find((m) => m.op === "start-stream-ok")["stream-id"];
        // legacy buffers small appends and only flushes on close/threshold,
        // so intermediate stream-flushed frames are not awaited
        msg(env.conns.A, { op: "append-stream", "stream-id": streamId, chunks: ["hello"], offset: 0, done: false });
        msg(env.conns.B, { op: "subscribe-stream", "client-id": "diff-stream", offset: 0 });
        await env.conns.B.waitFor((m) => m.op === "stream-append");
        msg(env.conns.A, { op: "append-stream", "stream-id": streamId, chunks: [" world"], offset: 5, done: false });
        await env.conns.B.waitFor(
          (m) => m.op === "stream-append" && (m.content ?? "").includes("world"),
          20000,
        );
        msg(env.conns.A, { op: "append-stream", "stream-id": streamId, chunks: [], offset: 11, done: true });
        await env.conns.A.waitFor((m) => m.op === "stream-flushed" && m.done === true, 20000);
      },
    },
    {
      name: "14-typed-query-breadth",
      run: async (env) => {
        // typed + indexed attrs: number / string / date, plus an untyped flag
        const typedAttr = (id, label, cdt) => [
          "add-attr",
          {
            id,
            "forward-identity": [id, "typed", label],
            "value-type": "blob",
            cardinality: "one",
            "unique?": label === "id",
            "index?": true,
            ...(cdt ? { "checked-data-type": cdt } : {}),
            isUnsynced: true,
          },
        ];
        const row = (e, score, name, when, flag) => [
          ["add-triple", e, ids.typedId, e],
          ...(score === undefined ? [] : [["add-triple", e, ids.typedScore, score]]),
          ...(name === undefined ? [] : [["add-triple", e, ids.typedName, name]]),
          ...(when === undefined ? [] : [["add-triple", e, ids.typedWhen, when]]),
          ...(flag === undefined ? [] : [["add-triple", e, ids.typedFlag, flag]]),
        ];
        msg(env.conns.A, {
          op: "transact",
          "tx-steps": [
            typedAttr(ids.typedId, "id"),
            typedAttr(ids.typedScore, "score", "number"),
            typedAttr(ids.typedName, "name", "string"),
            typedAttr(ids.typedWhen, "when", "date"),
            typedAttr(ids.typedFlag, "flag"),
            ...row(ids.t1, 1, "alice", "2024-01-01T00:00:00Z", true),
            ...row(ids.t2, 2.5, "bob", "2024-02-01T00:00:00Z", false),
            ...row(ids.t3, 3, "carol", "2024-03-01T00:00:00Z", null),
            ...row(ids.t4, -1, "malice", undefined, undefined),
            ...row(ids.t5, undefined, undefined, "2024-04-01T00:00:00Z", true),
          ],
        });
        await env.conns.A.waitFor(
          (m) => m.op === "transact-ok" && env.conns.A.frames.filter((f) => f.op === "transact-ok").length >= 6,
        );
        const queries = [
          { typed: { $: { where: { score: { $gt: 2 } } } } },
          { typed: { $: { where: { score: { $lte: 1 } } } } },
          { typed: { $: { where: { name: { $like: "%li%" } } } } },
          { typed: { $: { where: { name: { $ilike: "%A%" } } } } },
          { typed: { $: { where: { name: { $in: ["alice", "carol", "nobody"] } } } } },
          { typed: { $: { where: { name: { $not: "bob" } } } } },
          { typed: { $: { where: { score: { $isNull: true } } } } },
          { typed: { $: { where: { or: [{ score: { $gt: 2.6 } }, { name: "alice" }] } } } },
          { typed: { $: { where: { and: [{ score: { $gt: 0 } }, { when: { $lt: "2024-02-15T00:00:00Z" } }] } } } },
          { typed: { $: { where: { flag: true } } } },
          { typed: { $: { order: { score: "desc" }, limit: 2 } } },
          { typed: { $: { order: { name: "asc" }, limit: 2, offset: 1 } } },
          { typed: { $: { order: { when: "asc" }, last: 2 } } },
          { typed: { $: { fields: ["name", "score"] } } },
          { todos: { $: { where: { "owner.name": "alice" } } } }, // dot-path (link severed in 07 -> empty)
        ];
        for (const q of queries) {
          msg(env.conns.A, { op: "add-query", q });
          await env.conns.A.waitFor(
            (m) => m.op === "add-query-ok" && JSON.stringify(m.q) === JSON.stringify(q),
          );
          msg(env.conns.A, { op: "remove-query", q });
          await env.conns.A.waitFor(
            (m) => m.op === "remove-query-ok" && JSON.stringify(m.q) === JSON.stringify(q),
          );
        }
        // typed-order cursor page: capture per-server for step 16's mismatch case
        const qOrd = { typed: { $: { order: { score: "asc" }, limit: 2 } } };
        msg(env.conns.A, { op: "add-query", q: qOrd });
        const page = await env.conns.A.waitFor(
          (m) => m.op === "add-query-ok" && JSON.stringify(m.q) === JSON.stringify(qOrd),
        );
        env.scratch = env.scratch ?? {};
        env.scratch.scoreCursor = page.result?.[0]?.data?.["page-info"]?.typed?.["end-cursor"];
        // and a serverCreatedAt cursor for the mismatch case
        const qCreated = { typed: { $: { order: { serverCreatedAt: "asc" }, limit: 2 } } };
        msg(env.conns.A, { op: "add-query", q: qCreated });
        const page2 = await env.conns.A.waitFor(
          (m) => m.op === "add-query-ok" && JSON.stringify(m.q) === JSON.stringify(qCreated),
        );
        env.scratch.createdCursor = page2.result?.[0]?.data?.["page-info"]?.typed?.["end-cursor"];
        // typed-order after-cursor round trip
        const qAfter = { typed: { $: { order: { score: "asc" }, limit: 2, after: env.scratch.scoreCursor } } };
        msg(env.conns.A, { op: "add-query", q: qAfter });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.typed?.$?.after);
      },
    },
    {
      name: "15-authed-perms",
      run: async (env) => {
        // mint a real refresh token via the admin API (both servers implement
        // POST /admin/refresh_tokens with app-id + bearer admin token)
        const resp = await fetch(`${env.url}/admin/refresh_tokens`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "app-id": env.appId,
            authorization: `Bearer ${adminToken}`,
          },
          body: JSON.stringify({ email: "authuser@example.com" }),
        });
        const tokenBody = await resp.json();
        const refreshToken = tokenBody?.user?.refresh_token;
        if (!refreshToken) throw new Error(`no refresh token from ${env.serverName}: ${JSON.stringify(tokenBody).slice(0, 300)}`);
        env.scratch = env.scratch ?? {};
        env.scratch.refreshToken = refreshToken;

        // rules: owner-only view via bind, owner-only create, no updates
        psql(
          env.db,
          `UPDATE rules SET code = code || '{"secrets": {"bind": ["isOwner", "auth.email != null && data.owner == auth.email"], "allow": {"view": "isOwner", "create": "isOwner", "update": "false"}}}'::jsonb WHERE app_id = '${env.appId}'`,
        );

        // seed as admin: one owned secret, one foreign secret
        const secretsAttr = (id, label) => [
          "add-attr",
          {
            id,
            "forward-identity": [id, "secrets", label],
            "value-type": "blob",
            cardinality: "one",
            "unique?": label === "id",
            "index?": label === "id",
            isUnsynced: true,
          },
        ];
        msg(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [
            secretsAttr(ids.secretsId, "id"),
            secretsAttr(ids.secretsOwner, "owner"),
            secretsAttr(ids.secretsTitle, "title"),
            ["add-triple", ids.s1, ids.secretsId, ids.s1],
            ["add-triple", ids.s1, ids.secretsOwner, "authuser@example.com"],
            ["add-triple", ids.s1, ids.secretsTitle, "mine"],
            ["add-triple", ids.s2, ids.secretsId, ids.s2],
            ["add-triple", ids.s2, ids.secretsOwner, "other@example.com"],
            ["add-triple", ids.s2, ids.secretsTitle, "theirs"],
          ],
        });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok");

        // authed session: happy-path init with the refresh token
        env.conns.AUTH = connect(env.url, env.appId, `${env.serverName}:AUTH`);
        await env.conns.AUTH.open;
        msg(env.conns.AUTH, {
          op: "init",
          "app-id": env.appId,
          "refresh-token": refreshToken,
          versions: { "@instantdb/core": "v0.21.0" },
        });
        await env.conns.AUTH.waitFor((m) => m.op === "init-ok");

        // view rule filters to owned entities only
        msg(env.conns.AUTH, { op: "add-query", q: { secrets: {} } });
        await env.conns.AUTH.waitFor((m) => m.op === "add-query-ok");
        // $users default rules: authed user sees only self
        msg(env.conns.AUTH, { op: "add-query", q: { $users: {} } });
        await env.conns.AUTH.waitFor((m) => m.op === "add-query-ok" && m.q?.$users);

        // create own secret allowed; foreign create + any update denied
        msg(env.conns.AUTH, {
          op: "transact",
          "tx-steps": [
            ["add-triple", ids.s3, ids.secretsId, ids.s3],
            ["add-triple", ids.s3, ids.secretsOwner, "authuser@example.com"],
            ["add-triple", ids.s3, ids.secretsTitle, "also mine"],
          ],
        });
        await env.conns.AUTH.waitFor((m) => m.op === "transact-ok");
        const expectErr = async (m) => {
          const ceid = msg(env.conns.AUTH, m);
          await env.conns.AUTH.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        await expectErr({
          op: "transact",
          "tx-steps": [
            ["add-triple", ids.s4, ids.secretsId, ids.s4],
            ["add-triple", ids.s4, ids.secretsOwner, "other@example.com"],
          ],
        });
        await expectErr({
          op: "transact",
          "tx-steps": [["add-triple", ids.s1, ids.secretsTitle, "renamed"]],
        });
      },
    },
    {
      name: "16-error-breadth",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        await expectErr(env.conns.A, { op: "add-query", q: { typed: { $: { order: { nope: "asc" } } } } });
        await expectErr(env.conns.A, {
          op: "add-query",
          q: { typed: { $: { order: { score: "asc" }, limit: 2, after: env.scratch.createdCursor } } },
        });
        await expectErr(env.conns.ADMIN, {
          op: "add-query",
          q: { todos: { $: { aggregate: "count" }, owner: {} } },
        });
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [["deep-merge-triple", ids.e1, ids.ownerRef, { x: 1 }]],
        });
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", [ids.todosTitle, "one"], ids.todosDone, true]],
        });
      },
    },
    {
      // apps.status flips reach live sessions as `app-status-changed` (legacy
      // cache_evict.clj off the WAL feed); read-only rejects writes, disabled
      // rejects reads too, with the app-read-only / app-disabled error types
      name: "17-app-status",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        psql(env.db, `UPDATE apps SET status = 'read-only' WHERE id = '${env.appId}'`);
        await env.conns.A.waitFor((m) => m.op === "app-status-changed" && m.status === "read-only");
        await env.conns.B.waitFor((m) => m.op === "app-status-changed" && m.status === "read-only");
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", ids.e1, ids.todosTitle, "blocked"]],
        });
        msg(env.conns.A, { op: "add-query", q: { todos: { $: { where: { title: "reads-still-ok" } } } } });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.todos?.$?.where?.title === "reads-still-ok");
        psql(env.db, `UPDATE apps SET status = 'disabled' WHERE id = '${env.appId}'`);
        await env.conns.A.waitFor((m) => m.op === "app-status-changed" && m.status === "disabled");
        await expectErr(env.conns.A, { op: "add-query", q: { todos: { $: { where: { title: "reads-blocked" } } } } });
        await expectErr(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [["add-triple", ids.e1, ids.todosTitle, "blocked-admin"]],
        });
        psql(env.db, `UPDATE apps SET status = 'active' WHERE id = '${env.appId}'`);
        await env.conns.A.waitFor((m) => m.op === "app-status-changed" && m.status === "active");
        await env.conns.ADMIN.waitFor((m) => m.op === "app-status-changed" && m.status === "active");
      },
    },
    {
      // a guest upgraded with an existing user's email is linked to it
      // ($users.linkedPrimaryUser); the default $users rules let the primary
      // user view and update that guest row (rule.clj:198-210)
      name: "18-users-linked-guest",
      run: async (env) => {
        const hdr = { "content-type": "application/json", "app-id": env.appId, authorization: `Bearer ${adminToken}` };
        const post = async (p, body) => (await fetch(`${env.url}${p}`, { method: "POST", headers: hdr, body: JSON.stringify(body) })).json();
        const email = "primary-diff@example.com";
        const primary = (await post("/admin/refresh_tokens", { email })).user;
        const guest = (await post("/admin/sign_in_guest", {})).user;
        const code = (await post("/admin/magic_code", { email })).code;
        const upgraded = await post("/runtime/auth/verify_magic_code", { "app-id": env.appId, email, code, "refresh-token": guest.refresh_token });
        if (upgraded?.user?.id !== primary.id) throw new Error(`guest upgrade did not return the primary user on ${env.serverName}: ${JSON.stringify(upgraded).slice(0, 200)}`);
        // a fresh email upgrades the guest row in place (same id)
        const guest2 = (await post("/admin/sign_in_guest", {})).user;
        const email2 = "fresh-diff@example.com";
        const code2 = (await post("/admin/magic_code", { email: email2 })).code;
        const upgraded2 = await post("/runtime/auth/verify_magic_code", { "app-id": env.appId, email: email2, code: code2, "refresh-token": guest2.refresh_token });
        if (upgraded2?.user?.id !== guest2.id) throw new Error(`fresh-email upgrade did not keep the guest id on ${env.serverName}`);
        env.scratch.guestId = guest.id;
        env.scratch.strangerId = guest2.id;
        // a user-editable $users column (system columns are locked, see step 22)
        msg(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [[
            "add-attr",
            { id: ids.nicknameAttr, "forward-identity": [ids.nicknameAttr, "$users", "nickname"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true },
          ]],
        });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok" && env.conns.ADMIN.frames.filter((f) => f.op === "transact-ok").length >= 2);
        env.conns.PRIMARY = connect(env.url, env.appId, `${env.serverName}:PRIMARY`);
        await env.conns.PRIMARY.open;
        msg(env.conns.PRIMARY, { op: "init", "app-id": env.appId, "refresh-token": upgraded.user.refresh_token, versions: { "@instantdb/core": "v0.21.0" } });
        await env.conns.PRIMARY.waitFor((m) => m.op === "init-ok");
        // sees self + linked guest, not the stranger
        msg(env.conns.PRIMARY, { op: "add-query", q: { $users: {} } });
        await env.conns.PRIMARY.waitFor((m) => m.op === "add-query-ok");
        // may update the linked guest row, not the stranger's
        msg(env.conns.PRIMARY, { op: "transact", "tx-steps": [["add-triple", guest.id, ids.nicknameAttr, "my-guest"]] });
        await env.conns.PRIMARY.waitFor((m) => m.op === "transact-ok");
        const ceid = msg(env.conns.PRIMARY, { op: "transact", "tx-steps": [["add-triple", guest2.id, ids.nicknameAttr, "not-mine"]] });
        await env.conns.PRIMARY.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
      },
    },
    {
      // delete-attr soft-deletes (brands names, keeps triples); restore-attr
      // brings the attr and its triples back, un-indexed; admin-only
      name: "19-restore-attr",
      run: async (env) => {
        const ceid = msg(env.conns.A, { op: "transact", "tx-steps": [["restore-attr", ids.extraAttr]] });
        await env.conns.A.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        msg(env.conns.ADMIN, { op: "transact", "tx-steps": [["delete-attr", ids.extraAttr]] });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok" && env.conns.ADMIN.frames.filter((f) => f.op === "transact-ok").length >= 3);
        msg(env.conns.B, { op: "add-query", q: { todos: { $: { where: { title: "one" } } } } });
        await env.conns.B.waitFor((m) => m.op === "add-query-ok" && m.q?.todos?.$?.where?.title === "one");
        // unknown id: no-op like delete-attr
        msg(env.conns.ADMIN, { op: "transact", "tx-steps": [["restore-attr", ids.r3], ["restore-attr", ids.extraAttr]] });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok" && env.conns.ADMIN.frames.filter((f) => f.op === "transact-ok").length >= 4);
        msg(env.conns.B, { op: "add-query", q: { todos: { $: { where: { title: "one" }, fields: ["title", "extra"] } } } });
        await env.conns.B.waitFor((m) => m.op === "add-query-ok" && m.q?.todos?.$?.fields);
      },
    },
    {
      // request.origin / request.ip come from the upgrade headers,
      // request.modifiedFields from the tx, request.time is a timestamp
      name: "20-request-bindings",
      run: async (env) => {
        const attr = (id, label) => [
          "add-attr",
          { id, "forward-identity": [id, "reqs", label], "value-type": "blob", cardinality: "one", "unique?": label === "id", "index?": label === "id", isUnsynced: true },
        ];
        msg(env.conns.ADMIN, { op: "transact", "tx-steps": [attr(ids.reqsId, "id"), attr(ids.reqsTitle, "title"), attr(ids.reqsSecret, "secret"), ["add-triple", ids.r1, ids.reqsId, ids.r1], ["add-triple", ids.r1, ids.reqsTitle, "seeded"]] });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok" && env.conns.ADMIN.frames.filter((f) => f.op === "transact-ok").length >= 5);
        const rules = {
          reqs: {
            allow: {
              view: "request.origin == 'https://app.example' && request.ip == '203.0.113.9' && size(request.modifiedFields) == 0 && request.time > timestamp('2020-01-01T00:00:00Z')",
              create: "'title' in request.modifiedFields && !('secret' in request.modifiedFields) && request.origin.startsWith('https://')",
              update: "request.modifiedFields == ['title'] && has(request.ip) && has(request.origin)",
              delete: "request.modifiedFields.size() == 0 && request.ip == ''",
            },
          },
        };
        psql(env.db, `UPDATE rules SET code = code || $rules$${JSON.stringify(rules)}$rules$::jsonb WHERE app_id = '${env.appId}'`);
        await sleep(RULES_SETTLE_MS);
        env.conns.HDR = connect(env.url, env.appId, `${env.serverName}:HDR`, {
          origin: "https://app.example",
          // legacy takes the second-to-last hop (the last one is the load balancer's)
          "x-forwarded-for": "10.0.0.1, 203.0.113.9, 172.16.0.1",
        });
        await env.conns.HDR.open;
        msg(env.conns.HDR, { op: "init", "app-id": env.appId, versions: { "@instantdb/core": "v0.21.0" } });
        await env.conns.HDR.waitFor((m) => m.op === "init-ok");
        // view: only the headers session sees reqs
        msg(env.conns.HDR, { op: "add-query", q: { reqs: {} } });
        await env.conns.HDR.waitFor((m) => m.op === "add-query-ok");
        msg(env.conns.A, { op: "add-query", q: { reqs: {} } });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.reqs);
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        // create: title only passes, title+secret fails, no-origin session fails
        msg(env.conns.HDR, { op: "transact", "tx-steps": [["add-triple", ids.r2, ids.reqsId, ids.r2], ["add-triple", ids.r2, ids.reqsTitle, "created"]] });
        await env.conns.HDR.waitFor((m) => m.op === "transact-ok");
        await expectErr(env.conns.HDR, { op: "transact", "tx-steps": [["add-triple", ids.r3, ids.reqsId, ids.r3], ["add-triple", ids.r3, ids.reqsTitle, "t"], ["add-triple", ids.r3, ids.reqsSecret, "s"]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.r3, ids.reqsId, ids.r3], ["add-triple", ids.r3, ids.reqsTitle, "t"]] });
        // update: exactly [title] passes (id excluded, deep-merge counts), secret fails
        msg(env.conns.HDR, { op: "transact", "tx-steps": [["add-triple", ids.r2, ids.reqsId, ids.r2], ["deep-merge-triple", ids.r2, ids.reqsTitle, "renamed"]] });
        await env.conns.HDR.waitFor((m) => m.op === "transact-ok" && env.conns.HDR.frames.filter((f) => f.op === "transact-ok").length >= 2);
        await expectErr(env.conns.HDR, { op: "transact", "tx-steps": [["add-triple", ids.r2, ids.reqsSecret, "s"]] });
        // retractions are not modified fields; the no-header session has ip ''
        await expectErr(env.conns.HDR, { op: "transact", "tx-steps": [["delete-entity", ids.r2, "reqs"]] });
        msg(env.conns.A, { op: "transact", "tx-steps": [["delete-entity", ids.r2, "reqs"]] });
        await env.conns.A.waitFor((m) => m.op === "transact-ok" && env.conns.A.frames.filter((f) => f.op === "transact-ok").length >= 7);
      },
    },
    {
      // $rateLimits buckets via rateLimit.<name>.limit(key[, tokens]):
      // token bucket per (app, bucket, config, key); exhaustion is a
      // rate-limited error, charged once per checked entity
      name: "21-rate-limits",
      run: async (env) => {
        const attr = (id, label) => [
          "add-attr",
          { id, "forward-identity": [id, "limited", label], "value-type": "blob", cardinality: "one", "unique?": label === "id", "index?": label === "id", isUnsynced: true },
        ];
        msg(env.conns.ADMIN, { op: "transact", "tx-steps": [attr(ids.limitedId, "id"), attr(ids.limitedTitle, "title")] });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok" && env.conns.ADMIN.frames.filter((f) => f.op === "transact-ok").length >= 6);
        const rules = {
          $rateLimits: {
            creates: { limits: [{ capacity: 2, refill: { amount: 2, period: "1 hour", type: "interval" } }] },
            views: { limits: [{ capacity: 5 }] },
          },
          limited: {
            allow: {
              create: "rateLimit.creates.limit('shared')",
              update: "rateLimit['creates'].limit('shared', 5)",
              view: "rateLimit.views.limit('viewer')",
            },
          },
        };
        psql(env.db, `UPDATE rules SET code = code || $rules$${JSON.stringify(rules)}$rules$::jsonb WHERE app_id = '${env.appId}'`);
        await sleep(RULES_SETTLE_MS);
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        const create = (e, t) => ({ op: "transact", "tx-steps": [["add-triple", e, ids.limitedId, e], ["add-triple", e, ids.limitedTitle, t]] });
        msg(env.conns.B, create(ids.l1, "one"));
        await env.conns.B.waitFor((m) => m.op === "transact-ok");
        msg(env.conns.B, create(ids.l2, "two"));
        await env.conns.B.waitFor((m) => m.op === "transact-ok" && env.conns.B.frames.filter((f) => f.op === "transact-ok").length >= 2);
        await expectErr(env.conns.B, create(ids.l3, "three"));
        // 5 tokens on a capacity-2 bucket can never be granted
        await expectErr(env.conns.B, { op: "transact", "tx-steps": [["add-triple", ids.l1, ids.limitedTitle, "renamed"]] });
        // view charges one token per entity: 2 entities fit twice into 5, not thrice
        msg(env.conns.B, { op: "add-query", q: { limited: {} } });
        await env.conns.B.waitFor((m) => m.op === "add-query-ok" && m.q?.limited && !m.q.limited.$);
        msg(env.conns.B, { op: "add-query", q: { limited: { $: { where: { title: "one" } } } } });
        await env.conns.B.waitFor((m) => m.op === "add-query-ok" && m.q?.limited?.$?.where?.title === "one");
        msg(env.conns.B, { op: "add-query", q: { limited: { $: { where: { title: "two" } } } } });
        await env.conns.B.waitFor((m) => m.op === "add-query-ok" && m.q?.limited?.$?.where?.title === "two");
        await expectErr(env.conns.B, { op: "add-query", q: { limited: { $: { order: { serverCreatedAt: "desc" } } } } });
      },
    },
    {
      // system-catalog guards (permissioned_transaction.clj:44-71): users
      // can't write system columns or delete system entities, admins can
      name: "22-system-guards",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        const initOk = env.conns.AUTH.frames.find((f) => f.op === "init-ok");
        const selfId = initOk?.auth?.user?.id;
        if (!selfId) throw new Error(`no user id in init-ok on ${env.serverName}`);
        const imageUrl = initOk.attrs.find((a) => a["forward-identity"][1] === "$users" && a["forward-identity"][2] === "imageURL").id;
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", selfId, imageUrl, "https://x/me.png"]] });
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["delete-entity", selfId, "$users"]] });
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["delete-entity", env.scratch.strangerId, "$users"]] });
        // admins may write system columns (not $files / $streams) and delete system entities
        msg(env.conns.ADMIN, { op: "transact", "tx-steps": [["add-triple", env.scratch.strangerId, imageUrl, "https://x/stranger.png"]] });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok" && env.conns.ADMIN.frames.filter((f) => f.op === "transact-ok").length >= 7);
        msg(env.conns.ADMIN, { op: "transact", "tx-steps": [["delete-entity", env.scratch.strangerId, "$users"]] });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok" && env.conns.ADMIN.frames.filter((f) => f.op === "transact-ok").length >= 8);
      },
    },
    {
      // @instantdb/admin subscribeQuery (issue #8): POST /admin/subscribe-query
      // opens an admin session over SSE that registers the body's query with
      // the `tree` return-type — sse-init, then add-query-ok with the object
      // tree + result-meta page-info, then refresh-ok computations with the
      // same shape (admin/src/subscribe.ts:300-345). One admin subscription
      // (paginated, nested link, inference on) and one impersonated with
      // `as-token` (secrets view rule filters to the user's own rows).
      name: "23-admin-sse-subscribe-query",
      run: async (env) => {
        const versions = { "@instantdb/admin": "v0.22.0", "@instantdb/core": "v0.22.0" };
        env.conns.SSEQ = connectSse(env.url, env.appId, `${env.serverName}:SSEQ`, {
          path: `/admin/subscribe-query?local_connection_id=${ids.sseTodo}`,
          headers: { "app-id": env.appId, authorization: `Bearer ${adminToken}` },
          body: {
            query: { todos: { $: { limit: 2, order: { serverCreatedAt: "desc" } }, owner: {} } },
            "inference?": true,
            versions,
          },
        });
        await env.conns.SSEQ.open;
        await env.conns.SSEQ.waitFor((m) => m.op === "add-query-ok");
        env.conns.SSEU = connectSse(env.url, env.appId, `${env.serverName}:SSEU`, {
          path: `/admin/subscribe-query?local_connection_id=${ids.sseSecret}`,
          headers: { "app-id": env.appId, "as-token": env.scratch.refreshToken },
          body: {
            query: { secrets: { $: { order: { serverCreatedAt: "asc" } } } },
            "inference?": false,
            versions,
          },
        });
        await env.conns.SSEU.open;
        await env.conns.SSEU.waitFor((m) => m.op === "add-query-ok");

        // a new todo (linked to an owner) lands at the top of the desc page
        msg(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [
            ["add-triple", ids.sseTodo, ids.todosId, ids.sseTodo],
            ["add-triple", ids.sseTodo, ids.todosTitle, "over sse"],
            ["add-triple", ids.sseTodo, ids.todosDone, true],
            ["add-triple", ids.sseTodo, ids.ownerRef, ids.owner1],
          ],
        });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok" && env.conns.ADMIN.frames.filter((f) => f.op === "transact-ok").length >= 9);
        await env.conns.SSEQ.waitFor((m) => m.op === "refresh-ok");
        // a secret owned by the impersonated user refreshes only its subscription
        msg(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [
            ["add-triple", ids.sseSecret, ids.secretsId, ids.sseSecret],
            ["add-triple", ids.sseSecret, ids.secretsOwner, "authuser@example.com"],
            ["add-triple", ids.sseSecret, ids.secretsTitle, "mine too"],
          ],
        });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok" && env.conns.ADMIN.frames.filter((f) => f.op === "transact-ok").length >= 10);
        await env.conns.SSEU.waitFor((m) => m.op === "refresh-ok");
      },
    },
    {
      // the generic admin session (db.streams): POST /admin/sse opens it,
      // POST /admin/sse/push feeds it the same ops the socket takes —
      // join-rows queries, add-query-exists, transacts, and a stream a ws
      // subscriber tails (core/src/Connection.ts SSEConnection.postMessages).
      name: "24-admin-sse-generic",
      run: async (env) => {
        env.conns.SSEG = connectSse(env.url, env.appId, `${env.serverName}:SSEG`, {
          path: `/admin/sse?app_id=${env.appId}`,
          headers: { "app-id": env.appId, authorization: `Bearer ${adminToken}` },
          body: { "inference?": false, versions: { "@instantdb/admin": "v0.22.0", "@instantdb/core": "v0.22.0" } },
        });
        await env.conns.SSEG.open;
        const q = { todos: { $: { where: { title: "over sse" } } } };
        msg(env.conns.SSEG, { op: "add-query", q });
        await env.conns.SSEG.waitFor((m) => m.op === "add-query-ok");
        msg(env.conns.SSEG, { op: "add-query", q });
        await env.conns.SSEG.waitFor((m) => m.op === "add-query-exists");
        msg(env.conns.SSEG, {
          op: "transact",
          "tx-steps": [
            ["add-triple", ids.sseTodo2, ids.todosId, ids.sseTodo2],
            ["add-triple", ids.sseTodo2, ids.todosTitle, "over sse"],
          ],
        });
        await env.conns.SSEG.waitFor((m) => m.op === "transact-ok");
        await env.conns.SSEG.waitFor((m) => m.op === "refresh-ok");
        // stream written over the admin session, tailed over the socket
        msg(env.conns.SSEG, { op: "start-stream", "client-id": "sse-stream", "reconnect-token": "00000000-0000-4000-8000-0000000055ee" });
        const started = await env.conns.SSEG.waitFor((m) => m.op === "start-stream-ok");
        msg(env.conns.SSEG, { op: "append-stream", "stream-id": started["stream-id"], chunks: ["from sse"], offset: 0, done: false });
        msg(env.conns.B, { op: "subscribe-stream", "client-id": "sse-stream", offset: 0 });
        await env.conns.B.waitFor((m) => m.op === "stream-append" && (m.content ?? "").includes("from sse"), 20000);
        msg(env.conns.SSEG, { op: "append-stream", "stream-id": started["stream-id"], chunks: [], offset: 8, done: true });
        await env.conns.SSEG.waitFor((m) => m.op === "stream-flushed" && m.done === true, 20000);
      },
    },

    {
      // lookup / attr / query validation the audit found unenforced:
      // value-position lookups never create (triple.clj:885-899), lookup
      // namespaces must match (transaction.clj:532-556,
      // permissioned_transaction.clj:124-140), a second attr under an
      // existing name is record-not-unique (exception.clj:227-240),
      // required attrs can't land on populated namespaces
      // (attr.clj:334-350, :533-580), typed where values are validated even
      // on unindexed attrs (attr_pat.clj:228-295), `$not` on a link needs a
      // uuid (attr_pat.clj:410-419), only the first operator of an args map
      // applies (instaql.clj:669-675), `order: {}` is a no-op and a form on
      // missing attrs carries no page-info (instaql.clj:313-315, :1171-1172)
      name: "25-lookup-attr-query-validation",
      run: async (env) => {
        const okTx = async (conn, m) => {
          const before = conn.frames.filter((f) => f.op === "transact-ok").length;
          msg(conn, m);
          await conn.waitFor((x) => x.op === "transact-ok" && conn.frames.filter((f) => f.op === "transact-ok").length > before);
        };
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        const okQuery = async (conn, q) => {
          msg(conn, { op: "add-query", q });
          await conn.waitFor((m) => m.op === "add-query-ok" && JSON.stringify(m.q) === JSON.stringify(q));
          msg(conn, { op: "remove-query", q });
          await conn.waitFor((m) => m.op === "remove-query-ok" && JSON.stringify(m.q) === JSON.stringify(q));
        };
        // value-position lookup on a missing owner (unique attr): no phantom entity
        await expectErr(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [["add-triple", ids.e1, ids.ownerRef, [ids.ownersHandle, "nobody"]]],
        });
        // lookup namespace validation is legacy's non-admin pre-processing
        // (permissioned_transaction.clj:683-687): eid lookup on todos.score
        // writing an owners attr, and a value lookup outside the link's
        // reverse namespace; the admin path falls through to the lookup miss
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", [ids.todosScore, 7], ids.ownersName, "x"]],
        });
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [["add-triple", ids.e1, ids.ownerRef, [ids.todosScore, 7]]],
        });
        await expectErr(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [["add-triple", ids.e1, ids.ownerRef, [ids.todosScore, 7]]],
        });
        // a second attr under todos.title
        await expectErr(env.conns.A, {
          op: "transact",
          "tx-steps": [attr(ids.dupTitleAttr, "todos", "title", { fwd: ids.dupTitleAttr })],
        });
        // required attr on a namespace that already has entities
        await expectErr(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [[
            "add-attr",
            { id: ids.reqAttr, "forward-identity": [ids.reqAttr, "todos", "mustHave"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, "required?": true, isUnsynced: true },
          ]],
        });
        // flipping an existing attr to required when entities lack it
        await okTx(env.conns.ADMIN, { op: "transact", "tx-steps": [attr(ids.laterReqAttr, "todos", "laterRequired", { fwd: ids.laterReqAttr })] });
        await expectErr(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [["update-attr", { id: ids.laterReqAttr, "required?": true }]],
        });
        // typed where values: wrong types are 400s, incl. on the unindexed flag
        await expectErr(env.conns.A, { op: "add-query", q: { typed: { $: { where: { name: { $gt: 5 } } } } } });
        await expectErr(env.conns.A, { op: "add-query", q: { typed: { $: { where: { score: "2" } } } } });
        await expectErr(env.conns.A, { op: "add-query", q: { typed: { $: { where: { name: 5 } } } } });
        // equality refuses relative keywords; comparison parses them (rows are all in 2024, so the result is stable)
        await expectErr(env.conns.A, { op: "add-query", q: { typed: { $: { where: { when: "now" } } } } });
        await okQuery(env.conns.A, { typed: { $: { where: { when: { $gt: "now" } } } } });
        await expectErr(env.conns.A, { op: "add-query", q: { typed: { $: { where: { when: { $gt: "not-a-date" } } } } } });
        await expectErr(env.conns.A, { op: "add-query", q: { todos: { $: { where: { owner: { $not: "nope" } } } } } });
        await expectErr(env.conns.A, { op: "add-query", q: { typed: { $: { where: { name: { $like: 5 } } } } } });
        // deleting a link target re-validates required links on its referrers
        // (transaction.clj:617-623 feeds the deleted reverse rows to validate-required!)
        await okTx(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [
            attr(ids.articlesId, "articles", "id", { unique: true, fwd: ids.articlesId }),
            attr(ids.remarksId, "remarks", "id", { unique: true, fwd: ids.remarksId }),
            ["add-attr", { id: ids.remarksArticle, "forward-identity": [ids.remarksArticle, "remarks", "article"], "reverse-identity": [mk(), "articles", "remarks"], "value-type": "ref", cardinality: "one", "unique?": false, "index?": false, "required?": true, isUnsynced: true }],
            ["add-triple", ids.a1, ids.articlesId, ids.a1],
            ["add-triple", ids.r1, ids.remarksId, ids.r1],
            ["add-triple", ids.r1, ids.remarksArticle, ids.a1],
          ],
        });
        await expectErr(env.conns.ADMIN, { op: "transact", "tx-steps": [["delete-entity", ids.a1, "articles"]] });
        await okTx(env.conns.ADMIN, { op: "transact", "tx-steps": [["delete-entity", ids.r1, "remarks"], ["delete-entity", ids.a1, "articles"]] });
        // a row that predates the indexed-null backfill (datalog.clj:2946-2959
        // synthesizes a null for it): drop t5's score triple on both servers
        psql(env.db, `DELETE FROM triples WHERE app_id = '${env.appId}' AND attr_id = '${ids.typedScore}' AND entity_id = '${ids.t5}'`);
        // a write on `typed` makes both servers recompute the registered
        // typed queries (the direct delete notifies neither invalidator)
        await okTx(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.t4, ids.typedFlag, false]] });
        await okQuery(env.conns.A, { typed: { $: { order: { score: "asc" } } } });
        await okQuery(env.conns.A, { typed: { $: { order: { score: "asc" }, limit: 3 } } });
        await okQuery(env.conns.A, { typed: { $: { order: { score: "desc" }, limit: 3 } } });
        // first operator wins; empty order is a no-op; missing attrs carry no page-info
        await okQuery(env.conns.A, { typed: { $: { where: { score: { $gt: 0, $lt: 3 } } } } });
        await okQuery(env.conns.A, { typed: { $: { order: {} } } });
        await okQuery(env.conns.A, { nothing: { $: { limit: 2 } } });
        await okQuery(env.conns.A, { typed: { $: { where: { missingAttr: "x" }, limit: 2 } } });
      },
    },
    {
      // protocol edge cases: pre-init ops (session.clj:198-202), `init`
      // param shapes (session.clj:154-172, exception.clj:410-428), unknown
      // ops (session.clj:1017-1024), `q: null` (session.clj:243-246),
      // `remove-query` always acks, a missing `tx-steps` (transaction.clj:128-134),
      // a missing `room-id` (session.clj:638-641), re-joining a room keeps
      // the presence data (hazelcast.clj:123-134), and stream append /
      // reconnect-token / unsubscribe validation (session.clj:767-769,
      // :832-843, :958-970; app_stream.clj:444-457)
      name: "26-protocol-edges",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        env.conns.COLD = connect(env.url, env.appId, `${env.serverName}:COLD`);
        await env.conns.COLD.open;
        await expectErr(env.conns.COLD, { op: "add-query", q: { todos: {} } });
        await expectErr(env.conns.COLD, { op: "remove-query", q: { todos: {} } });
        await expectErr(env.conns.COLD, { op: "transact", "tx-steps": [] });
        await expectErr(env.conns.COLD, { op: "init", versions: { "@instantdb/core": "v0.21.0" } });
        await expectErr(env.conns.COLD, { op: "init", "app-id": "not-a-uuid", versions: { "@instantdb/core": "v0.21.0" } });
        await expectErr(env.conns.COLD, { op: "init", "app-id": env.appId, "__admin-token": "00000000-0000-4000-8000-00000000bad0", versions: { "@instantdb/core": "v0.21.0" } });
        await expectErr(env.conns.A, { op: "no-such-op", anything: 1 });
        await expectErr(env.conns.A, { op: "add-query", q: null });
        await expectErr(env.conns.A, { op: "transact" });
        await expectErr(env.conns.A, { op: "join-room" });
        msg(env.conns.A, { op: "remove-query", q: { never: { registered: {} } } });
        await env.conns.A.waitFor((m) => m.op === "remove-query-ok" && m.q?.never);
        msg(env.conns.A, { op: "remove-query" });
        await env.conns.A.waitFor((m) => m.op === "remove-query-ok" && m.q === null);
        // re-join keeps the presence data a peer already saw
        msg(env.conns.A, { op: "join-room", "room-type": "diff", "room-id": "rejoin", data: { who: "A" } });
        await env.conns.A.waitFor((m) => m.op === "join-room-ok" && m["room-id"] === "rejoin");
        msg(env.conns.B, { op: "join-room", "room-type": "diff", "room-id": "rejoin", data: { who: "B" } });
        await env.conns.B.waitFor((m) => m.op === "join-room-ok" && m["room-id"] === "rejoin");
        msg(env.conns.B, { op: "set-presence", "room-id": "rejoin", data: { who: "B", x: 1 } });
        await env.conns.A.waitFor((m) => (m.op === "refresh-presence" || m.op === "patch-presence") && m["room-id"] === "rejoin" && JSON.stringify(m).includes('"x"'));
        msg(env.conns.B, { op: "join-room", "room-type": "diff", "room-id": "rejoin" });
        await env.conns.B.waitFor((m) => m.op === "join-room-ok" && m["room-id"] === "rejoin" && env.conns.B.frames.filter((f) => f.op === "join-room-ok" && f["room-id"] === "rejoin").length >= 2);
        // streams
        await expectErr(env.conns.A, { op: "start-stream", "client-id": "diff-stream-2" });
        msg(env.conns.A, { op: "start-stream", "client-id": "diff-stream-2", "reconnect-token": ids.stream2Token });
        const started = await env.conns.A.waitFor((m) => m.op === "start-stream-ok" && m["client-id"] === "diff-stream-2");
        const sid = started["stream-id"];
        await expectErr(env.conns.A, { op: "append-stream", "stream-id": sid, chunks: ["x"], offset: 5, done: false });
        await expectErr(env.conns.A, { op: "append-stream", "stream-id": sid, offset: 0, done: false });
        await expectErr(env.conns.A, { op: "append-stream", "stream-id": sid, chunks: ["x"], done: false });
        msg(env.conns.A, { op: "append-stream", "stream-id": sid, chunks: ["fin"], offset: 0, done: true });
        await env.conns.A.waitFor((m) => m.op === "stream-flushed" && m.done === true && m["stream-id"] === sid, 20000);
        await expectErr(env.conns.A, { op: "append-stream", "stream-id": sid, chunks: ["more"], offset: 3, done: false });
        await expectErr(env.conns.B, { op: "unsubscribe-stream", "subscribe-event-id": "never-subscribed" });
      },
    },
    {
      // rule evaluation semantics: `$default.bind` + object-form binds
      // (rule.clj:100-111, :139-142), Clojure truthiness of a non-boolean
      // result (exception.clj:291-297), evaluation errors as
      // permission-evaluation-failed (exception.clj:299-323), non-admins
      // can't create $users rows (permissioned_transaction.clj:589-596),
      // and `$users.allow.create` gates signups over HTTP (app_user.clj:51-85,
      // magic_code_auth.clj:277-288, runtime/routes.clj:131-139)
      name: "27-rule-evaluation",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        const rules = {
          $default: { bind: { isAuthUser: "auth.email == 'authuser@example.com'" } },
          gated: {
            bind: ["hasTitle", "data.title != null"],
            allow: { view: "isAuthUser && hasTitle", create: "isAuthUser", update: "data.title", delete: "data.title.foo" },
          },
          $users: { allow: { create: "data.email != null && data.email.endsWith('@allowed.example')" } },
        };
        psql(env.db, `UPDATE rules SET code = code || $rules$${JSON.stringify(rules)}$rules$::jsonb WHERE app_id = '${env.appId}'`);
        await sleep(RULES_SETTLE_MS);
        const gatedAttr = (id, label) => [
          "add-attr",
          { id, "forward-identity": [id, "gated", label], "value-type": "blob", cardinality: "one", "unique?": label === "id", "index?": label === "id", isUnsynced: true },
        ];
        const okTx = async (conn, m) => {
          const before = conn.frames.filter((f) => f.op === "transact-ok").length;
          msg(conn, m);
          await conn.waitFor((x) => x.op === "transact-ok" && conn.frames.filter((f) => f.op === "transact-ok").length > before);
        };
        await okTx(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [gatedAttr(ids.gatedId, "id"), gatedAttr(ids.gatedTitle, "title"), ["add-triple", ids.g1, ids.gatedId, ids.g1], ["add-triple", ids.g1, ids.gatedTitle, "seeded"]],
        });
        // the $default bind resolves for the etype rule; anon sees nothing
        msg(env.conns.AUTH, { op: "add-query", q: { gated: {} } });
        await env.conns.AUTH.waitFor((m) => m.op === "add-query-ok" && m.q?.gated);
        msg(env.conns.A, { op: "add-query", q: { gated: {} } });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.gated);
        await okTx(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.g2, ids.gatedId, ids.g2], ["add-triple", ids.g2, ids.gatedTitle, "mine"]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.g3, ids.gatedId, ids.g3], ["add-triple", ids.g3, ids.gatedTitle, "not mine"]] });
        // a string result is truthy; a broken rule is an evaluation error
        await okTx(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.g2, ids.gatedTitle, "renamed"]] });
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["delete-entity", ids.g2, "gated"]] });
        // non-admins can't mint $users rows
        const usersId = env.conns.AUTH.frames.find((f) => f.op === "init-ok").attrs.find((a) => a["forward-identity"][1] === "$users" && a["forward-identity"][2] === "id").id;
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.fakeUser, usersId, ids.fakeUser]] });
        // signups over HTTP go through $users.allow.create
        env.conns.HTTP = env.conns.HTTP ?? httpConn(`${env.serverName}:HTTP`);
        const hdr = { "content-type": "application/json", "app-id": env.appId, authorization: `Bearer ${adminToken}` };
        const post = async (p, body, headers = hdr) => {
          const res = await fetch(`${env.url}${p}`, { method: "POST", headers, body: JSON.stringify(body) });
          return { status: res.status, body: await res.json().catch(() => null) };
        };
        const view = (name, r, pick = []) => ({
          name,
          status: r.status,
          type: r.body?.type ?? null,
          message: r.body?.message ?? null,
          ...Object.fromEntries(pick.map((k) => [k, r.body?.[k] ?? null])),
        });
        env.conns.HTTP.record(view("guestSignupDenied", await post("/runtime/auth/sign_in_guest", { "app-id": env.appId })));
        const denied = "denied-diff@example.com";
        const code = (await post("/admin/magic_code", { email: denied })).body?.code;
        env.conns.HTTP.record(view("magicSignupDenied", await post("/runtime/auth/verify_magic_code", { "app-id": env.appId, email: denied, code })));
        // the failed check must not burn the code: the same code verifies once the rule allows it
        psql(env.db, `UPDATE rules SET code = code - '$users' WHERE app_id = '${env.appId}'`);
        await sleep(RULES_SETTLE_MS);
        env.conns.HTTP.record(view("magicSignupAfterRuleRemoved", await post("/runtime/auth/verify_magic_code", { "app-id": env.appId, email: denied, code }), ["created"]));
        env.conns.HTTP.record(view("guestSignupAllowed", await post("/runtime/auth/sign_in_guest", { "app-id": env.appId })));
      },
    },
    {
      // admin HTTP gates and shapes: impersonation headers only stand in for
      // the admin token on the get-perms! routes (admin/routes.clj:59-110,
      // :396-772), the perms-check routes need both (:220-221, :325-326),
      // /admin/users misses are 200 nulls (:490-499), unknown routes and
      // wrong methods are a JSON 404 (core.clj:189-190), and
      // /runtime/framework/query serves SSR clients (runtime/routes.clj:728-743)
      name: "28-admin-http-gates",
      run: async (env) => {
        env.conns.HTTP = env.conns.HTTP ?? httpConn(`${env.serverName}:HTTP`);
        const call = async (method, p, { headers = {}, body } = {}) => {
          const res = await fetch(`${env.url}${p}`, {
            method,
            headers: { "content-type": "application/json", "app-id": env.appId, ...headers },
            body: body === undefined ? undefined : JSON.stringify(body),
          });
          const raw = await res.text();
          let parsed = null;
          try { parsed = JSON.parse(raw); } catch {}
          return { status: res.status, body: parsed, raw };
        };
        const view = (name, r, extra = {}) => ({
          name,
          status: r.status,
          type: r.body?.type ?? null,
          message: r.body?.message ?? null,
          // non-JSON bodies are kept verbatim (key always present so the
          // per-op key sets match)
          raw: r.body ? null : String(r.raw ?? "").slice(0, 200),
          ...extra,
        });
        const auth = { authorization: `Bearer ${adminToken}` };
        const guest = { "as-guest": "true" };
        const asToken = { "as-token": env.scratch.refreshToken };
        const rec = (f) => env.conns.HTTP.record(f);
        // token-only routes with impersonation headers and no token
        rec(view("refreshTokensAsGuest", await call("POST", "/admin/refresh_tokens", { headers: guest, body: { email: "authuser@example.com" } })));
        rec(view("magicCodeAsToken", await call("POST", "/admin/magic_code", { headers: asToken, body: { email: "authuser@example.com" } })));
        rec(view("usersGetAsGuest", await call("GET", "/admin/users?email=authuser@example.com", { headers: guest })));
        rec(view("usersDeleteAsGuest", await call("DELETE", "/admin/users?email=authuser@example.com", { headers: guest })));
        rec(view("signOutAsGuest", await call("POST", "/admin/sign_out", { headers: guest, body: { email: "authuser@example.com" } })));
        rec(view("signInGuestAsGuest", await call("POST", "/admin/sign_in_guest", { headers: guest, body: {} })));
        rec(view("presenceAsGuest", await call("GET", "/admin/rooms/presence?room-type=diff&room-id=rejoin", { headers: guest })));
        rec(view("signedUploadUrlAsGuest", await call("POST", "/admin/storage/signed-upload-url", { headers: guest, body: { filename: "x.txt" } })));
        rec(view("signedDownloadUrlAsGuest", await call("GET", "/admin/storage/signed-download-url?filename=x.txt", { headers: guest })));
        rec(view("queryPermsCheckAsGuest", await call("POST", "/admin/query_perms_check", { headers: guest, body: { query: { todos: {} } } })));
        rec(view("transactPermsCheckAsToken", await call("POST", "/admin/transact_perms_check", { headers: asToken, body: { steps: [["update", "todos", ids.e1, { title: "x" }]] } })));
        // with the token the perms-check routes accept impersonation
        const qpc = await call("POST", "/admin/query_perms_check", { headers: { ...auth, ...guest }, body: { query: { todos: {} } } });
        rec(view("queryPermsCheckAdminAsGuest", qpc, { checks: Array.isArray(qpc.body?.["check-results"]) }));
        const tpc = await call("POST", "/admin/transact_perms_check", { headers: { ...auth, ...asToken }, body: { steps: [["update", "todos", ids.e1, { title: "x" }]] } });
        rec(view("transactPermsCheckAdminAsToken", tpc, { keys: Object.keys(tpc.body ?? {}).sort() }));
        // impersonation still works where legacy allows it
        const qAs = await call("POST", "/admin/query", { headers: guest, body: { query: { todos: {} } } });
        rec(view("queryAsGuest", qAs, { keys: Object.keys(qAs.body ?? {}).sort() }));
        // misses are 200 nulls
        const miss = await call("GET", "/admin/users?email=nobody-here@example.com", { headers: auth });
        rec(view("usersGetMiss", miss, { user: miss.body?.user ?? "<absent>" }));
        const missDel = await call("DELETE", "/admin/users?email=nobody-here@example.com", { headers: auth });
        rec(view("usersDeleteMiss", missDel, { deleted: missDel.body?.deleted ?? "<absent>" }));
        // routing fallbacks: compared on their own pseudo-connection because
        // the self-hosted legacy image answers these with a 200 non-JSON body
        // (allowed-divergences.json)
        env.conns.HTTP_ROUTING = env.conns.HTTP_ROUTING ?? httpConn(`${env.serverName}:HTTP_ROUTING`);
        env.conns.HTTP_ROUTING.record(view("unknownRoute", await call("GET", "/admin/no-such-route", { headers: auth })));
        env.conns.HTTP_ROUTING.record(view("wrongMethod", await call("GET", "/admin/query", { headers: auth })));
        // SSR framework query: anonymous and with a refresh token
        const fq = await call("POST", "/runtime/framework/query", { body: { query: { secrets: {} } } });
        rec(view("frameworkQueryAnon", fq, { result: projectResult(fq.body?.result), attrCount: fq.body?.attrs ? Object.keys(projectAttrs(fq.body.attrs)).length : null }));
        const fqAuth = await call("POST", "/runtime/framework/query", { headers: { authorization: `Bearer ${env.scratch.refreshToken}` }, body: { query: { secrets: {} } } });
        rec(view("frameworkQueryAuthed", fqAuth, { result: projectResult(fqAuth.body?.result) }));
      },
    },
    {
      // cel-java's strings + math extensions and Instant's getTime /
      // timestamp(int|string) overloads (cel.clj:387-454, :479-488), inside
      // binds and every rule kind; an unknown function is an evaluation
      // error on both servers
      name: "29-cel-extensions",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        const okTx = async (conn, m) => {
          const before = conn.frames.filter((f) => f.op === "transact-ok").length;
          msg(conn, m);
          await conn.waitFor((x) => x.op === "transact-ok" && conn.frames.filter((f) => f.op === "transact-ok").length > before);
        };
        const rules = {
          cel: {
            bind: ["emailOk", "auth.email != null && auth.email.upperAscii().lowerAscii().endsWith('@example.com') && auth.email.indexOf('@') > 0"],
            allow: {
              view: "emailOk && data.title.trim().lowerAscii().startsWith('ok')",
              // every extension function, evaluated per entity on the
              // created row (legacy rewrites view rules into where clauses,
              // so the breadth lives on the create rule)
              create: [
                "emailOk",
                "newData.title.lowerAscii() == newData.title",
                "newData.title.charAt(0) == 'o'",
                "newData.title.substring(0, 2) == 'ok'",
                "newData.title.substring(3) == 'three'",
                "newData.title.indexOf('t') == 3",
                "newData.title.lastIndexOf('e') > newData.title.indexOf('e')",
                "newData.title.replace('o', '0').split(' ').size() == 2",
                "newData.title.replace('e', '3', 1) == 'ok thr3e'",
                "newData.title.upperAscii() == 'OK THREE'",
                "['a', 'b'].join('-') == 'a-b'",
                "['a', 'b'].join() == 'ab'",
                "math.greatest(newData.score, 0) == newData.score",
                "math.least(newData.score, 0) == 0",
                "math.greatest([0, newData.score, 1]) == newData.score",
                "math.abs(-1) == 1",
                "math.floor(2.5) == 2.0",
                "math.ceil(2.5) == 3.0",
                "math.round(2.5) == 3.0",
                "math.trunc(-2.5) == -2.0",
                "math.sign(-3) == -1",
                "math.isNaN(0.0 / 0.0)",
                "math.isFinite(1.0) && !math.isInf(1.0)",
                "math.bitAnd(6, 3) == 2 && math.bitOr(6, 3) == 7 && math.bitXor(6, 3) == 5 && math.bitShiftLeft(1, 3) == 8 && math.bitShiftRight(8, 3) == 1",
                "timestamp(newData.when) < request.time",
                "timestamp(newData.when).getTime() == 1577836800000",
                "timestamp(1577836800000).getFullYear() == 2020",
                "timestamp(1577836800000) == timestamp(newData.when)",
                "timestamp('2020-01-01T00:00:00Z').getTime() == timestamp(newData.when).getTime()",
                "request.time.getTime() > timestamp(newData.when).getTime()",
                "timestamp('01/02/2020').getDate() == 2",
              ].join(" && "),
              // an unknown function is a compile-time undeclared reference
              update: "data.title.frobnicate() == 'x'",
              delete: "auth.email.trim() == 'nobody@example.com'",
            },
          },
        };
        psql(env.db, `UPDATE rules SET code = code || $rules$${JSON.stringify(rules)}$rules$::jsonb WHERE app_id = '${env.appId}'`);
        await sleep(RULES_SETTLE_MS);
        const celAttr = (id, label) => [
          "add-attr",
          { id, "forward-identity": [id, "cel", label], "value-type": "blob", cardinality: "one", "unique?": label === "id", "index?": label === "id", isUnsynced: true },
        ];
        await okTx(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [
            celAttr(ids.celId, "id"), celAttr(ids.celTitle, "title"), celAttr(ids.celScore, "score"), celAttr(ids.celWhen, "when"),
            ["add-triple", ids.c1, ids.celId, ids.c1], ["add-triple", ids.c1, ids.celTitle, "ok one"], ["add-triple", ids.c1, ids.celScore, 5], ["add-triple", ids.c1, ids.celWhen, "2020-01-01"],
            ["add-triple", ids.c2, ids.celId, ids.c2], ["add-triple", ids.c2, ids.celTitle, "nope"], ["add-triple", ids.c2, ids.celScore, 5], ["add-triple", ids.c2, ids.celWhen, "2020-01-01"],
          ],
        });
        // only c1 passes the view rule for the authed user; anonymous sees nothing
        msg(env.conns.AUTH, { op: "add-query", q: { cel: {} } });
        await env.conns.AUTH.waitFor((m) => m.op === "add-query-ok" && m.q?.cel);
        msg(env.conns.A, { op: "add-query", q: { cel: {} } });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.cel);
        // create: lowerAscii on newData
        await okTx(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.c3, ids.celId, ids.c3], ["add-triple", ids.c3, ids.celTitle, "ok three"], ["add-triple", ids.c3, ids.celScore, 1], ["add-triple", ids.c3, ids.celWhen, "2020-01-01"]] });
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.c4, ids.celId, ids.c4], ["add-triple", ids.c4, ids.celTitle, "OK four"]] });
        // an unknown function fails to compile; the delete rule denies
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.c1, ids.celTitle, "ok one"]] });
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["delete-entity", ids.c3, "cel"]] });
      },
    },
    {
      // view + field rules evaluate on the whole entity even when the query
      // projects `fields` (instaql.clj:1956-2007 preload-entity-maps)
      name: "30-view-fields-projection",
      run: async (env) => {
        const rules = {
          vf: { allow: { view: "auth.email != null && data.owner == auth.email" }, fields: { title: "data.owner == auth.email" } },
        };
        psql(env.db, `UPDATE rules SET code = code || $rules$${JSON.stringify(rules)}$rules$::jsonb WHERE app_id = '${env.appId}'`);
        await sleep(RULES_SETTLE_MS);
        const vfAttr = (id, label) => [
          "add-attr",
          { id, "forward-identity": [id, "vf", label], "value-type": "blob", cardinality: "one", "unique?": label === "id", "index?": label === "id", isUnsynced: true },
        ];
        msg(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [
            vfAttr(ids.vfId, "id"), vfAttr(ids.vfTitle, "title"), vfAttr(ids.vfOwner, "owner"),
            ["add-triple", ids.v1, ids.vfId, ids.v1], ["add-triple", ids.v1, ids.vfTitle, "mine"], ["add-triple", ids.v1, ids.vfOwner, "authuser@example.com"],
            ["add-triple", ids.v2, ids.vfId, ids.v2], ["add-triple", ids.v2, ids.vfTitle, "theirs"], ["add-triple", ids.v2, ids.vfOwner, "other@example.com"],
          ],
        });
        await env.conns.ADMIN.waitFor((m) => m.op === "transact-ok");
        msg(env.conns.AUTH, { op: "add-query", q: { vf: { $: { fields: ["title"] } } } });
        await env.conns.AUTH.waitFor((m) => m.op === "add-query-ok" && m.q?.vf);
        msg(env.conns.AUTH, { op: "add-query", q: { vf: { $: { fields: ["owner"] } } } });
        await env.conns.AUTH.waitFor((m) => m.op === "add-query-ok" && m.q?.vf?.$?.fields?.[0] === "owner");
        // the same query with a `$isNull` inside `or` on an indexed attr folds
        // to `{in [nil]}` (instaql.clj:204-230)
        msg(env.conns.A, { op: "add-query", q: { typed: { $: { where: { or: [{ score: { $isNull: true } }, { name: "alice" }] } } } } });
        await env.conns.A.waitFor((m) => m.op === "add-query-ok" && m.q?.typed?.$?.where?.or);
      },
    },
    {
      // link rules see `actions` and `linkedData.ref`, a link that creates
      // the entity runs the link rule with actions.data == "create"
      // (permissioned_transaction.clj:528-560), `unlink` rules never see
      // `actions`, and update / delete rules read `data.ref` against the
      // pre-tx graph (:697-715)
      name: "31-link-bindings-and-pre-tx-refs",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        const okTx = async (conn, m) => {
          const before = conn.frames.filter((f) => f.op === "transact-ok").length;
          msg(conn, m);
          await conn.waitFor((x) => x.op === "transact-ok" && conn.frames.filter((f) => f.op === "transact-ok").length > before);
        };
        const rules = {
          tasks: {
            allow: {
              create: "false",
              link: { project: "actions.data == 'create' && actions.linkedData == 'update' && linkedData.name == 'main' && 'main' in linkedData.ref('name')" },
            },
          },
          projects: { allow: { delete: "size(data.ref('tasks.id')) > 0", update: "size(data.ref('tasks.id')) > 0" } },
        };
        const setRules = async (r) => {
          psql(env.db, `UPDATE rules SET code = code || $rules$${JSON.stringify(r)}$rules$::jsonb WHERE app_id = '${env.appId}'`);
          await sleep(RULES_SETTLE_MS);
        };
        await setRules(rules);
        const blob = (id, etype, label) => [
          "add-attr",
          { id, "forward-identity": [id, etype, label], "value-type": "blob", cardinality: "one", "unique?": label === "id", "index?": label === "id", isUnsynced: true },
        ];
        await okTx(env.conns.ADMIN, {
          op: "transact",
          "tx-steps": [
            blob(ids.projectsId, "projects", "id"), blob(ids.projectsName, "projects", "name"),
            blob(ids.tasksId, "tasks", "id"), blob(ids.tasksTitle, "tasks", "title"),
            ["add-attr", { id: ids.tasksProject, "forward-identity": [ids.tasksProject, "tasks", "project"], "reverse-identity": [mk(), "projects", "tasks"], "value-type": "ref", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true }],
            ["add-triple", ids.p1, ids.projectsId, ids.p1], ["add-triple", ids.p1, ids.projectsName, "main"],
            ["add-triple", ids.p2, ids.projectsId, ids.p2], ["add-triple", ids.p2, ids.projectsName, "side"],
            ["add-triple", ids.k2, ids.tasksId, ids.k2], ["add-triple", ids.k2, ids.tasksTitle, "existing"],
          ],
        });
        // a task brought into being by the link step alone: link rule, not create
        await okTx(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.k1, ids.tasksProject, ids.p1]] });
        // the same against the side project: linkedData.name != 'main'
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.k3, ids.tasksProject, ids.p2]] });
        // an existing task linking: actions.data == 'update' -> denied
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.k2, ids.tasksProject, ids.p1]] });
        // unlink programs don't declare `actions`: legacy fails to compile
        // the rule (`validation-failed` for permission with the rule's path)
        // and, since every ref step loads both sides' link and unlink
        // programs, a link step trips over it too
        await setRules({ tasks: { allow: { ...rules.tasks.allow, unlink: { project: "actions.data == 'update'" } } } });
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["retract-triple", ids.k1, ids.tasksProject, ids.p1]] });
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.k3, ids.tasksProject, ids.p1]] });
        // a view rule mentioning newData / a typo'd identifier fail the same way
        await setRules({ tasks: { allow: { ...rules.tasks.allow, view: "newData.title == 'x'" } } });
        await expectErr(env.conns.AUTH, { op: "add-query", q: { tasks: {} } });
        await setRules({ tasks: { allow: { ...rules.tasks.allow, view: "dta.title == 'x'" } } });
        await expectErr(env.conns.AUTH, { op: "add-query", q: { tasks: {} } });
        await setRules(rules);
        // `data.ref` only sees entities with an id triple (cel.clj:88-117
        // build-query anchors on it), so give the link-created task one
        await okTx(env.conns.ADMIN, { op: "transact", "tx-steps": [["add-triple", ids.k1, ids.tasksId, ids.k1]] });
        // update rule on projects reads data.ref pre-tx: p1 has a task, p2 none
        await okTx(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.p1, ids.projectsName, "main"]] });
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["add-triple", ids.p2, ids.projectsName, "side!"]] });
        // delete rule: the pre-tx link graph still shows p1's task
        await okTx(env.conns.AUTH, { op: "transact", "tx-steps": [["delete-entity", ids.p1, "projects"]] });
        await expectErr(env.conns.AUTH, { op: "transact", "tx-steps": [["delete-entity", ids.p2, "projects"]] });
        msg(env.conns.ADMIN, { op: "add-query", q: { projects: { tasks: {} } } });
        await env.conns.ADMIN.waitFor((m) => m.op === "add-query-ok" && m.q?.projects);
      },
    },
    {
      // `attrs.allow.create` gates inline add-attr steps
      // (permissioned_transaction.clj:519-527); `mode` is validated in one
      // pre-pass against the pre-tx state with legacy's messages
      // (transaction.clj:283-358); step shapes follow the specs (:25-69)
      name: "32-attrs-create-mode-and-step-shapes",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        const okTx = async (conn, m) => {
          const before = conn.frames.filter((f) => f.op === "transact-ok").length;
          msg(conn, m);
          await conn.waitFor((x) => x.op === "transact-ok" && conn.frames.filter((f) => f.op === "transact-ok").length > before);
        };
        const rules = { attrs: { allow: { create: "auth.email == 'authuser@example.com'" } } };
        psql(env.db, `UPDATE rules SET code = code || $rules$${JSON.stringify(rules)}$rules$::jsonb WHERE app_id = '${env.appId}'`);
        await sleep(RULES_SETTLE_MS);
        const dyn = (id, label) => [
          "add-attr",
          { id, "forward-identity": [id, "todos", label], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true },
        ];
        await okTx(env.conns.AUTH, { op: "transact", "tx-steps": [dyn(ids.dynAttrOk, "dynOk"), ["add-triple", ids.e1, ids.dynAttrOk, 1]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [dyn(ids.dynAttrDenied, "dynDenied"), ["add-triple", ids.e1, ids.dynAttrDenied, 1]] });
        psql(env.db, `UPDATE rules SET code = code - 'attrs' WHERE app_id = '${env.appId}'`);
        await sleep(RULES_SETTLE_MS);
        // mode pre-pass: existence by any triple of the etype, all offenders
        // in one message, lookups checked as written
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.e1, ids.todosTitle, "again", { mode: "create" }], ["add-triple", ids.e2, ids.todosDone, true, { mode: "create" }]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.m1, ids.todosTitle, "a", { mode: "update" }], ["add-triple", ids.m1, ids.todosDone, true, { mode: "update" }]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.m1, ids.todosId, ids.m1, { mode: "create" }], ["add-triple", ids.m1, ids.todosTitle, "x", { mode: "update" }]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["delete-entity", ids.e1, "todos"], ["add-triple", ids.e1, ids.todosTitle, "back", { mode: "create" }]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", [ids.ownersHandle, "nobody-mode"], ids.ownersName, "nobody-here", { mode: "update" }]] });
        await okTx(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.m2, ids.todosId, ids.m2, { mode: "create" }], ["add-triple", ids.e1, ids.todosTitle, "one-c", { mode: "update" }], ["add-triple", ids.m2, ids.todosDone, false, { mode: "upsert" }]] });
        // step-shape specs
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.e1, ids.todosTitle, "x", { mode: "replace" }]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.e1, ids.todosTitle, "x", "create"]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", ids.e1, ids.todosTitle]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["retract-triple", ids.e1, ids.todosTitle, "x", { mode: "create" }]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["delete-entity", ids.e1, 42]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["rule-params", ids.e1, "todos", "not-a-map"]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["add-triple", "not-an-id", ids.todosTitle, "x"]] });
        await expectErr(env.conns.A, { op: "transact", "tx-steps": [["delete-attr", ids.todosDone, "extra"]] });
      },
    },
    {
      // /admin/rooms/presence requires room-type and re-fetches each peer's
      // $users row (admin/routes.clj:739-765), presence entries carry the
      // node's instance-id (ephemeral.clj:280-286), and resync-table checks
      // the session's admin-ness / user against the subscription
      // (model/sync_sub.clj:170-195)
      name: "33-presence-admin-and-resync-checks",
      run: async (env) => {
        const expectErr = async (conn, m) => {
          const ceid = msg(conn, m);
          await conn.waitFor((x) => x.op === "error" && x["client-event-id"] === ceid);
        };
        msg(env.conns.AUTH, { op: "join-room", "room-type": "diff", "room-id": "adminroom", data: { who: "auth" } });
        await env.conns.AUTH.waitFor((m) => m.op === "join-room-ok" && m["room-id"] === "adminroom");
        msg(env.conns.A, { op: "join-room", "room-type": "diff", "room-id": "adminroom", data: { who: "anon" } });
        await env.conns.A.waitFor((m) => m.op === "join-room-ok" && m["room-id"] === "adminroom");
        await settle(Object.values(env.conns), 800);
        env.conns.HTTP = env.conns.HTTP ?? httpConn(`${env.serverName}:HTTP`);
        const call = async (p) => {
          const res = await fetch(`${env.url}${p}`, { headers: { "app-id": env.appId, authorization: `Bearer ${adminToken}` } });
          return { status: res.status, body: await res.json().catch(() => null) };
        };
        const errView = (name, r) => ({ name, status: r.status, type: r.body?.type ?? null, message: r.body?.message ?? null });
        const pres = await call("/admin/rooms/presence?room-type=diff&room-id=adminroom");
        const sessions = Object.values(pres.body?.sessions ?? {})
          .map((e) => ({
            keys: Object.keys(e).sort(),
            instanceIdKind: typeof e["instance-id"],
            data: e.data,
            user: e.user ? { id: normalize(e.user.id), email: e.user.email ?? null, keys: Object.keys(e.user).sort() } : null,
          }))
          .sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
        env.conns.HTTP.record({ name: "presenceAdmin", status: pres.status, sessions });
        env.conns.HTTP.record(errView("presenceNoRoomType", await call("/admin/rooms/presence?room-id=adminroom")));
        env.conns.HTTP.record(errView("presenceNoRoomId", await call("/admin/rooms/presence?room-type=diff")));
        // resync-table from a non-admin session on the admin's subscription
        const sub = env.conns.ADMIN.frames.find((f) => f.op === "start-sync-ok");
        await expectErr(env.conns.A, { op: "resync-table", "subscription-id": sub["subscription-id"], "tx-id": 1, token: sub.token });
        await expectErr(env.conns.ADMIN, { op: "resync-table", "subscription-id": sub["subscription-id"], "tx-id": 1, token: ids.m1 });
        await expectErr(env.conns.ADMIN, { op: "resync-table", "subscription-id": ids.m2, "tx-id": 1, token: sub.token });
        msg(env.conns.AUTH, { op: "leave-room", "room-id": "adminroom" });
        await env.conns.AUTH.waitFor((m) => m.op === "leave-room-ok" && m["room-id"] === "adminroom");
        msg(env.conns.A, { op: "leave-room", "room-id": "adminroom" });
        await env.conns.A.waitFor((m) => m.op === "leave-room-ok" && m["room-id"] === "adminroom");
      },
    },
    {
      // the OAuth callback's error surfaces up to the client lookup are 400
      // oauth-errors, never redirects (runtime/routes.clj:506-601), and
      // ?test-redirect renders the landing page (:379-432)
      name: "34-oauth-callback-http",
      run: async (env) => {
        env.conns.HTTP = env.conns.HTTP ?? httpConn(`${env.serverName}:HTTP`);
        const call = async (qs, headers = {}) => {
          const res = await fetch(`${env.url}/runtime/oauth/callback${qs}`, { redirect: "manual", headers });
          const raw = await res.text();
          let body = null;
          try { body = JSON.parse(raw); } catch {}
          return { status: res.status, body, raw, ct: (res.headers.get("content-type") ?? "").split(";")[0], location: res.headers.get("location") };
        };
        const view = (name, r) => ({
          name,
          status: r.status,
          type: r.body?.type ?? null,
          // legacy renders oauth-error bodies as {type, message}; the text is what a developer reads
          message: r.body?.message ?? r.body?.error ?? null,
          redirected: r.location != null,
        });
        const rec = (f) => env.conns.HTTP.record(f);
        rec(view("cbProviderError", await call("?error=access_denied&state=whatever")));
        rec(view("cbMissingState", await call("")));
        rec(view("cbInvalidState", await call("?state=nope&code=x")));
        rec(view("cbMissingCookie", await call(`?state=${env.appId}${ids.m1}&code=x`)));
        // the cookie value is `instantdb_<uuid>`; a bare uuid is no cookie
        rec(view("cbBareCookie", await call(`?state=${env.appId}${ids.m1}&code=x`, { cookie: `__session=${ids.m2}` })));
        rec(view("cbUnknownRequest", await call(`?state=${env.appId}${ids.m1}&code=x`, { cookie: `__session=instantdb_${ids.m2}` })));
        const landing = await call("?test-redirect=1");
        rec({ name: "cbTestRedirect", status: landing.status, ct: landing.ct, ok: landing.raw.includes("Your OAuth redirect looks good!") });
      },
    },
  ];
  return steps;
}

// ---------------------------------------------------------------------------
// runner

async function runAgainst(serverName) {
  const { url, db, appId } = SERVERS[serverName];
  const env = { serverName, url, db, appId, conns: {} };
  const steps = buildScenario();
  const stepResults = [];
  const keySets = {}; // op -> Set of keys
  const states = {}; // conn name -> folded state
  for (const step of steps) {
    try {
      await step.run(env);
    } catch (e) {
      console.error(`step ${step.name} failed on ${serverName}: ${e.message}`);
      for (const [name, conn] of Object.entries(env.conns)) {
        const brief = conn.frames.slice(-3).map((f) => ({ ...f, attrs: f.attrs ? `<${f.attrs.length} attrs>` : undefined }));
        console.error(`  last frames on ${name}:`, JSON.stringify(brief)?.slice(0, 1500));
        // the newest frame in full: it is usually the error that stalled the step
        const last = brief[brief.length - 1];
        if (last) console.error(`  newest frame on ${name}:`, JSON.stringify(last)?.slice(0, 6000));
      }
      throw e;
    }
    await settle(Object.values(env.conns), 900);
    const byConn = {};
    for (const [name, conn] of Object.entries(env.conns)) {
      const frames = conn.takeNewFrames();
      for (const f of frames) {
        (keySets[f.op] ??= new Set());
        for (const k of Object.keys(f)) keySets[f.op].add(k);
      }
      if (process.env.DUMP_OPS) {
        // raw op trace (refresh-ok: which queries recomputed, attrs carried?)
        const ops = frames.map((f) =>
          f.op === "refresh-ok"
            ? `refresh-ok(${(f.computations ?? []).map((c) => JSON.stringify(c["instaql-query"])).join("|")}${f.attrs ? " +attrs" : ""})`
            : f.op,
        );
        if (ops.length) console.log(`--- [${serverName}] ${step.name} ${name}: ${ops.join(", ")}`);
      }
      states[name] ??= newState();
      byConn[name] = await foldFrames(frames, states[name]);
    }
    stepResults.push({ name: step.name, byConn });
    if (process.env.DUMP_STEPS && process.env.DUMP_STEPS.split(",").includes(step.name)) {
      console.log(`### [${serverName}] ${step.name}\n${JSON.stringify(byConn, null, 1).slice(0, 20000)}`);
    }
  }
  for (const conn of Object.values(env.conns)) conn.close();
  return {
    steps: stepResults,
    finalStates: Object.fromEntries(
      Object.entries(states).map(([k, v]) => [k, projectState(v)]),
    ),
    keySets: Object.fromEntries(
      Object.entries(keySets).map(([k, v]) => [k, [...v].sort()]),
    ),
  };
}

// ---------------------------------------------------------------------------
// diff + allowlist

const allowlist = JSON.parse(
  fs.readFileSync(path.join(here, "allowed-divergences.json"), "utf8"),
);
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

const diffs = [];
function record(p, legacyVal, rustVal) {
  if (canon(legacyVal) === canon(rustVal)) return;
  diffs.push({ path: p, allowed: allowed(p), legacy: legacyVal, rust: rustVal });
}

console.log("replaying against legacy…");
const legacy = await runAgainst("legacy");
console.log("replaying against rust…");
const rust = await runAgainst("rust");

for (let i = 0; i < legacy.steps.length; i++) {
  const l = legacy.steps[i], r = rust.steps[i];
  for (const conn of new Set([...Object.keys(l.byConn), ...Object.keys(r.byConn)])) {
    record(`step:${l.name}/${conn}`, l.byConn[conn] ?? [], r.byConn[conn] ?? []);
  }
}
for (const conn of new Set([...Object.keys(legacy.finalStates), ...Object.keys(rust.finalStates)])) {
  const lf = legacy.finalStates[conn] ?? {};
  const rf = rust.finalStates[conn] ?? {};
  for (const section of new Set([...Object.keys(lf), ...Object.keys(rf)])) {
    record(`final/${conn}/${section}`, lf[section] ?? null, rf[section] ?? null);
  }
}
for (const op of new Set([...Object.keys(legacy.keySets), ...Object.keys(rust.keySets)])) {
  const lk = new Set(legacy.keySets[op] ?? []);
  const rk = new Set(rust.keySets[op] ?? []);
  for (const k of new Set([...lk, ...rk])) {
    if (lk.has(k) !== rk.has(k)) {
      record(`keyset/${op}/${k}`, lk.has(k) ? "present" : "absent", rk.has(k) ? "present" : "absent");
    }
  }
}

// descend into both values to the first differing sub-path for readable output
function firstDiff(a, b, p = "") {
  if (canon(a) === canon(b)) return null;
  const isObj = (v) => v !== null && typeof v === "object";
  if (isObj(a) && isObj(b) && Array.isArray(a) === Array.isArray(b)) {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    for (const k of keys) {
      const d = firstDiff(a?.[k], b?.[k], `${p}.${k}`);
      if (d) return d;
    }
  }
  return { p, a, b };
}

const blocking = diffs.filter((d) => !d.allowed);
for (const d of diffs) {
  const tag = d.allowed ? "ALLOWED" : "DIVERGENCE";
  console.log(`\n[${tag}] ${d.path}`);
  const fd = firstDiff(d.legacy, d.rust);
  if (fd && fd.p) {
    console.log(`  first differing sub-path: ${fd.p}`);
    console.log("  legacy:", JSON.stringify(fd.a)?.slice(0, 1200));
    console.log("  rust:  ", JSON.stringify(fd.b)?.slice(0, 1200));
    if (!d.allowed && d.path.startsWith("step:")) {
      // the folded frames are sorted, so one differing frame shifts every
      // index after it; print both sides whole for the CI log
      console.log("  legacy (whole):", JSON.stringify(d.legacy)?.slice(0, 20000));
      console.log("  rust   (whole):", JSON.stringify(d.rust)?.slice(0, 20000));
    }
  } else {
    console.log("  legacy:", JSON.stringify(d.legacy)?.slice(0, 1200));
    console.log("  rust:  ", JSON.stringify(d.rust)?.slice(0, 1200));
  }
}
for (const entry of allowlist) {
  if (!usedAllows.has(entry.path)) {
    console.log(`\n[STALE ALLOWLIST] ${entry.path} matched nothing (ok if flaky-path)`);
  }
}
if (blocking.length) {
  console.error(`\nDIFFERENTIAL REPLAY FAILED: ${blocking.length} unallowed divergences`);
  process.exit(1);
}
console.log(`\nDIFFERENTIAL REPLAY PASSED (${diffs.length} allowed divergences, ${legacy.steps.length} steps)`);
