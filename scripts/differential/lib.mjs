// Shared machinery for the differential harness: ws capture clients,
// client-visible normalization, and diffing.
//
// Normalization philosophy: two servers are wire-equivalent when a client
// computes identical state from their frames. Frames are normalized down to
// exactly what the client reads (per the client source's Reactor.js et al.),
// with volatile server-chosen values (ids, timestamps, isns, hashes,
// trace ids) replaced by placeholders. Anything left must match byte-for-byte
// unless an entry in allowed-divergences.json (with a client-code citation)
// says otherwise.

import { execSync } from "node:child_process";
import fs from "node:fs";
import { noteHttp, noteCel, noteWsFrame, noteWsMessage } from "./coverage-hook.mjs";

export const uuid = () => crypto.randomUUID();

// Named harness exception for the rust-only contention checks (tx-conflicts,
// fuzz-race), which otherwise fail on any 5xx transact reply. A transact that
// waits on row locks past the 5s handle-receive timeout gets a 500
// operation-timed-out from both servers: legacy session.clj:58
// handle-receive-timeout-ms and :1160-1178 (it cancels the in-progress
// statements and answers with the 500), rust ws.rs handle_receive_timeout_ms.
// That is legacy behavior, not a bug, so these checks count and report it
// but don't fail on it. Only that exact reply matches: a deadlock /
// sql-exception, an internal-error or any other 5xx still fails.
export const HANDLER_TIMEOUT_EXCEPTION = "handler-timeout";
export const isHandlerTimeout = (reply) =>
  reply?.op === "error" &&
  reply.status === 500 &&
  reply.type === "operation-timed-out" &&
  reply.message === "Operation timed out: handle-receive";

// Deterministic uuids so both servers receive byte-identical client input.
// Derived from the app id (attr ids are globally unique in the legacy schema,
// so ids must be fresh per provisioned app) — but identical between the two
// servers, which share the app id.
let fixedPrefix = "00000000";
export function makeIdFactory(appId) {
  if (appId) fixedPrefix = appId.replaceAll("-", "").slice(0, 8);
  let n = 0;
  return () => {
    n++;
    return `${fixedPrefix}-0000-4000-8000-${String(n).padStart(12, "0")}`;
  };
}

export const isFixedId = (s) =>
  typeof s === "string" && s.includes("-0000-4000-8000-") && s.startsWith(fixedPrefix);

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function psql(url, sql) {
  noteCel(sql);
  execSync(`psql "${url}" -q -v ON_ERROR_STOP=1 -f -`, { input: sql });
}

// Latest committed tx a sync subscription still needs to receive. Committed
// entries carry an `etypes` array (one transaction can touch several
// namespaces) and the attr ids its steps actually changed. A transaction that
// changed nothing, or only attrs outside the subscription's frozen sync attr
// set, produces no frame for that sub and must not be waited on.
export function syncTargets(committed, subs, etypes = null) {
  const out = [];
  for (const sub of subs) {
    if (!sub.active) continue;
    if (etypes && !etypes.includes(sub.etype)) continue;
    const target = committed
      .filter((c) => {
        if (c.txId <= (sub.txId ?? -1)) return false;
        if (etypes ? !etypes.some((etype) => c.etypes.includes(etype)) : !c.etypes.includes(sub.etype)) {
          return false;
        }
        if (c.attrs && c.attrs.length === 0) return false;
        if (c.attrs && sub.syncAttrs) {
          return c.attrs.some((attrId) => sub.syncAttrs.includes(attrId));
        }
        return true;
      })
      .reduce((max, c) => Math.max(max, c.txId), 0);
    if (target) out.push({ sub, target });
  }
  return out;
}

// Annotate a fuzz plan with the attr ids each transaction actually changes in
// a simple triple store (add triples, retract exact triples, delete an
// entity). Start ops get the card-one attrs available at that point, matching
// the sync-table topic freeze. This lets the harness wait only for txs that
// can produce a sync frame: seed 308 tx-51 repeated a retraction that was
// already gone and set the same title/rank, so it emitted no frame at all.
export function annotateSyncChanges(script, manyAttrs = []) {
  const many = new Set(manyAttrs);
  const triples = new Map(); // key -> { e, a }
  const byEa = new Map(); // "e|a" -> Set<key>
  const available = new Set();

  const keyOf = (e, a, v) => `${e}|${a}|${canon(v)}`;
  const remove = (key) => {
    const t = triples.get(key);
    if (!t) return false;
    triples.delete(key);
    const ea = `${t.e}|${t.a}`;
    const keys = byEa.get(ea);
    if (keys) {
      keys.delete(key);
      if (!keys.size) byEa.delete(ea);
    }
    return true;
  };

  for (const op of script) {
    if (op.kind === "start") {
      op.syncAttrs = [...available].filter((attrId) => !many.has(attrId));
      continue;
    }
    if (op.kind !== "tx") continue;

    const changed = new Set();
    const appliesTriples = op.etypes.length > 0;
    for (const step of op.steps) {
      const [kind] = step;
      if (kind === "add-attr") {
        available.add(step[1].id);
        continue;
      }
      if (!appliesTriples) continue;

      if (kind === "add-triple") {
        const [, e, a, v] = step;
        const key = keyOf(e, a, v);
        if (triples.has(key)) continue;
        if (!many.has(a)) {
          const ea = `${e}|${a}`;
          for (const oldKey of [...(byEa.get(ea) ?? [])]) remove(oldKey);
        }
        triples.set(key, { e, a });
        const ea = `${e}|${a}`;
        if (!byEa.has(ea)) byEa.set(ea, new Set());
        byEa.get(ea).add(key);
        changed.add(a);
      } else if (kind === "retract-triple") {
        const [, e, a, v] = step;
        if (remove(keyOf(e, a, v))) changed.add(a);
      } else if (kind === "delete-entity") {
        const [, e] = step;
        for (const [key, t] of [...triples]) {
          if (t.e === e && remove(key)) changed.add(t.a);
        }
      }
    }
    op.changedAttrs = [...changed];
  }
}

// Frames for independent client objects (one sync/stream subscription each)
// may arrive in any order. Only a single key's own frame sequence is
// client-visible, so canonicalize across keys while keeping each key's
// arrival order stable.
export function sortFramesByKey(frames, keyOf) {
  return frames
    .map((frame, i) => ({ frame, key: String(keyOf(frame)), i }))
    .sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : a.i - b.i))
    .map((x) => x.frame);
}

// ---------------------------------------------------------------------------
// capture client

// First frame at or after `from` that matches `pred`. Waiting from a frame
// mark (rather than from the connection's cursor) is what lets a caller wait
// for a *new* frame: a room-presence refresh from before a leave/re-join must
// not satisfy the wait for that join's snapshot.
export function firstFrameAfter(frames, from, pred) {
  for (let i = Math.max(0, from); i < frames.length; i++) {
    if (pred(frames[i])) return frames[i];
  }
  return undefined;
}

// `headers` (Origin, X-Forwarded-For) ride on the upgrade request like a
// browser's would; both servers read them into request.origin / request.ip
// for rule evaluation (node's WebSocket accepts them as an undici option).
export function connect(serverUrl, appId, name, headers) {
  noteHttp("GET", `${serverUrl}/runtime/session`);
  const ws = new WebSocket(
    `${serverUrl.replace(/^http/, "ws")}/runtime/session?app_id=${appId}`,
    headers ? { headers } : undefined,
  );
  const frames = []; // all frames ever, in arrival order
  let cursorMark = 0; // frames before this index belong to earlier steps
  const waiters = [];
  let lastFrameAt = Date.now();
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    for (const m of Array.isArray(msg) ? msg : [msg]) {
      frames.push(m);
      noteWsFrame(m);
      lastFrameAt = Date.now();
      for (let i = waiters.length - 1; i >= 0; i--) {
        const [pred, resolve] = waiters[i];
        if (pred(m)) {
          waiters.splice(i, 1);
          resolve(m);
        }
      }
    }
  };
  const send = (msg) => {
    noteWsMessage(msg);
    ws.send(JSON.stringify(msg));
    return msg["client-event-id"];
  };
  const waitFor = (pred, timeout = 15000, from = cursorMark) =>
    new Promise((resolve, reject) => {
      const existing = firstFrameAfter(frames, from, pred);
      if (existing) return resolve(existing);
      const t = setTimeout(
        () => reject(new Error(`timeout waiting on ${name}`)),
        timeout,
      );
      waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
    });
  return {
    name,
    ws,
    send,
    waitFor,
    frames,
    open: new Promise((r, j) => {
      ws.onopen = r;
      ws.onerror = (e) => j(new Error(`ws error on ${name}: ${e.message ?? e}`));
    }),
    close: () => ws.close(),
    takeNewFrames() {
      const out = frames.slice(cursorMark);
      cursorMark = frames.length;
      return out;
    },
    quietSince: () => Date.now() - lastFrameAt,
  };
}

// SSE capture client with the same surface as `connect`: the admin SDK's
// transports (admin/src/subscribe.ts subscribe(), core/src/Connection.ts
// SSEConnection). `path` is opened with a POST carrying `headers` + JSON
// `body`; the stream is open once `sse-init` arrives, and `send` POSTs the
// message envelope (machine_id / session_id / sse_token / messages) to
// `pushPath` like SSEConnection.postMessages.
export function connectSse(serverUrl, appId, name, { path, pushPath, headers = {}, body, method = "POST" }) {
  const frames = [];
  let cursorMark = 0;
  const waiters = [];
  let lastFrameAt = Date.now();
  let init = null;
  let httpStatus = null;
  const deliver = (m) => {
    frames.push(m);
    noteWsFrame(m);
    lastFrameAt = Date.now();
    if (m.op === "sse-init") init = m;
    for (let i = waiters.length - 1; i >= 0; i--) {
      const [pred, resolve] = waiters[i];
      if (pred(m)) {
        waiters.splice(i, 1);
        resolve(m);
      }
    }
  };
  const controller = new AbortController();
  const open = (async () => {
    // the browser transport opens with GET (core/src/Connection.ts
    // SSEConnection), the admin SDK's with a POST body
    const res = await fetch(serverUrl + path, {
      method,
      headers: method === "GET" ? { accept: "text/event-stream", ...headers } : { "content-type": "application/json", accept: "text/event-stream", ...headers },
      body: method === "GET" ? undefined : JSON.stringify(body ?? {}),
      signal: controller.signal,
    });
    httpStatus = res.status;
    if (!res.ok) throw new Error(`sse open failed on ${name}: ${res.status} ${(await res.text()).slice(0, 300)}`);
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    (async () => {
      let buf = "";
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          let idx;
          while ((idx = buf.indexOf("\n\n")) >= 0) {
            const chunk = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            const data = chunk
              .split("\n")
              .filter((l) => l.startsWith("data:"))
              .map((l) => l.slice(5).trim())
              .join("\n");
            if (!data) continue;
            let parsed;
            try {
              parsed = JSON.parse(data);
            } catch {
              continue;
            }
            for (const m of Array.isArray(parsed) ? parsed : [parsed]) deliver(m);
          }
        }
      } catch {
        // stream closed
      }
    })();
    await new Promise((resolve, reject) => {
      if (init) return resolve();
      const t = setTimeout(() => reject(new Error(`timeout waiting for sse-init on ${name}`)), 15000);
      waiters.push([(m) => m.op === "sse-init", () => { clearTimeout(t); resolve(); }]);
    });
  })();
  const send = (msg) => {
    if (!init) throw new Error(`sse ${name} not open`);
    noteWsMessage(msg);
    const envelope = {
      machine_id: init["machine-id"],
      session_id: init["session-id"],
      sse_token: init["sse-token"],
      messages: [msg],
    };
    fetch(serverUrl + (pushPath ?? `/admin/sse/push?app_id=${appId}`), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(envelope),
    }).then(async (r) => {
      if (!r.ok) console.error(`push failed on ${name}: ${r.status} ${(await r.text()).slice(0, 300)}`);
    });
    return msg["client-event-id"];
  };
  const waitFor = (pred, timeout = 15000, from = cursorMark) =>
    new Promise((resolve, reject) => {
      const existing = firstFrameAfter(frames, from, pred);
      if (existing) return resolve(existing);
      const t = setTimeout(() => reject(new Error(`timeout waiting on ${name}`)), timeout);
      waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
    });
  return {
    name,
    send,
    waitFor,
    frames,
    open,
    get httpStatus() { return httpStatus; },
    get init() { return init; },
    close: () => controller.abort(),
    takeNewFrames() {
      const out = frames.slice(cursorMark);
      cursorMark = frames.length;
      return out;
    },
    quietSince: () => Date.now() - lastFrameAt,
  };
}

// wait until every connection has been frame-silent for quietMs
// A pseudo-connection for HTTP calls: `record(frame)` queues a frame that
// folds like any other (normalized whole), so a step can diff HTTP
// responses (status + the fields a client reads) next to socket frames.
export function httpConn(name) {
  const frames = [];
  let cursorMark = 0;
  return {
    name,
    frames,
    record(frame) {
      frames.push({ op: "http", ...frame });
    },
    takeNewFrames() {
      const out = frames.slice(cursorMark);
      cursorMark = frames.length;
      return out;
    },
    quietSince: () => Number.MAX_SAFE_INTEGER,
    close() {},
  };
}

export async function settle(conns, quietMs = 700, maxMs = 15000) {
  const start = Date.now();
  for (;;) {
    if (conns.every((c) => c.quietSince() >= quietMs)) return;
    if (Date.now() - start > maxMs) return;
    await new Promise((r) => setTimeout(r, 50));
  }
}

// ---------------------------------------------------------------------------
// client-visible projection + volatile-value normalization

const VOLATILE_KEYS = {
  "trace-id": "<trace>",
  "duration-ms": "<ms>",
  isn: "<isn>",
  "processed-isn": "<isn>",
  "instaql-query-hash": "<hash>",
  "tx-id": "<tx>",
  "processed-tx-id": "<tx>",
  token: "<uuid>",
  "session-id": "<uuid>",
  "subscription-id": "<uuid>",
  "stream-id": "<uuid>",
  "machine-id": "<uuid>",
  "sse-token": "<uuid>",
  "client-event-id": "<ceid>", // fixed ids stay recognizable below
};

export function normalize(value, key = null) {
  if (typeof value === "string") {
    if (isFixedId(value)) return value; // scenario-chosen id: identical on both
    if (UUID_RE.test(value)) return "<uuid>";
    return value;
  }
  if (typeof value === "number") {
    // epoch-ms timestamps (triple t values, created)
    if (value > 1e12) return "<ts>";
    return value;
  }
  if (Array.isArray(value)) return value.map((v) => normalize(v));
  if (value !== null && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (k in VOLATILE_KEYS) {
        out[k] = isFixedId(v) ? v : VOLATILE_KEYS[k];
      } else {
        out[k] = normalize(v, k);
      }
    }
    return out;
  }
  return value;
}

export const canon = (v) => JSON.stringify(sortDeep(v));
function sortDeep(v) {
  if (Array.isArray(v)) return v.map(sortDeep);
  if (v !== null && typeof v === "object") {
    const out = {};
    for (const k of Object.keys(v).sort()) out[k] = sortDeep(v[k]);
    return out;
  }
  return v;
}

// The client's only consumption of an instaql-result tree: flatten all
// join-rows to triples (model/instaqlResult.js:1-25) and read
// page-info/aggregate off result[0].data (Reactor.js:671-672).
export function projectResult(result) {
  const triples = [];
  const walk = (nodes) => {
    for (const node of nodes ?? []) {
      for (const rows of node?.data?.["datalog-result"]?.["join-rows"] ?? []) {
        for (const t of rows) triples.push(t);
      }
      walk(node?.["child-nodes"]);
    }
  };
  walk(result);
  const norm = triples.map((t) => normalize(t));
  norm.sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
  // dedupe: grouping differences can repeat triples; the client's store dedupes
  const deduped = [...new Map(norm.map((t) => [canon(t), t])).values()];
  return {
    triples: deduped,
    "page-info": normalize(result?.[0]?.data?.["page-info"] ?? null),
    aggregate: normalize(result?.[0]?.data?.aggregate ?? null),
  };
}

// Tree-shaped results (admin SSE `return-type: tree`): the admin SDK hands
// the object tree to the app as-is (subscribe.ts:305-320), so compare it
// whole. Top-level order is what the query asked for; nested link arrays
// are sorted by id (both servers order them by server-created-at with an
// id tie-break, and entities seeded in one tx share a timestamp).
export const isTreeResult = (result) => result !== null && typeof result === "object" && !Array.isArray(result);
export function projectTree(result, top = true) {
  if (Array.isArray(result)) {
    const items = result.map((v) => projectTree(v, false));
    if (!top) items.sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
    return items;
  }
  if (result !== null && typeof result === "object") {
    const out = {};
    for (const [k, v] of Object.entries(result)) out[k] = projectTree(v, false);
    return out;
  }
  return normalize(result);
}
export const projectAnyResult = (result) =>
  isTreeResult(result) ? { tree: projectTree(result) } : projectResult(result);

// Fold a step's frames into (a) directly comparable normalized frames,
// (b) per-query latest results, (c) per-room presence state — mirroring
// client state application. Async because stream readers fetch `files` URLs
// exactly like the client does (Stream.ts:1077-1084).
export async function foldFrames(frames, state) {
  const direct = [];
  for (const m of frames) {
    switch (m.op) {
      case "refresh-presence": {
        if (state.leftRooms?.has(m["room-id"])) break; // straggler after leave
        // client keeps only entry.data per session (Reactor.js:2699-2710)
        state.rooms[m["room-id"]] = Object.fromEntries(
          Object.entries(m.data).map(([sid, v]) => [sid, v.data]),
        );
        break;
      }
      case "patch-presence": {
        if (state.leftRooms?.has(m["room-id"])) break; // straggler after leave
        // Reactor.js:2672-2697 applies editscript edits generically: a path
        // is [sid] (whole entry), [sid, "data"] (whole data) or deeper
        // (legacy diffs inside data, e.g. [sid, "data", "x"] "+" 1)
        const room = (state.rooms[m["room-id"]] ??= {});
        for (const [path, op, val] of m.edits) {
          const [sid, ...rest] = path;
          if (rest.length === 0) {
            if (op === "-") delete room[sid];
            else room[sid] = val.data;
          } else if (rest[0] === "data") {
            const inner = rest.slice(1);
            if (inner.length === 0) {
              if (op === "-") delete room[sid];
              else room[sid] = val;
            } else {
              let node = (room[sid] ??= {});
              for (const k of inner.slice(0, -1)) node = node[k] ??= {};
              if (op === "-") delete node[inner[inner.length - 1]];
              else node[inner[inner.length - 1]] = val;
            }
          }
        }
        break;
      }
      case "leave-room-ok": {
        // the client drops a room's presence state on leave (Reactor.js:894-898
        // marks the room disconnected); anything a server pushes for that room
        // around the leave is not client-visible, and delivery timing of a
        // final straggler frame legitimately differs between servers
        delete state.rooms[m["room-id"]];
        state.leftRooms = state.leftRooms ?? new Set();
        state.leftRooms.add(m["room-id"]);
        direct.push(normalize(m));
        break;
      }
      case "refresh-ok": {
        for (const comp of m.computations ?? []) {
          const key = canon(normalize(comp["instaql-query"]));
          state.queries[key] = projectAnyResult(comp["instaql-result"]);
          // admin SSE reads result-meta.page-info per computation (subscribe.ts:322-327)
          if (isTreeResult(comp["instaql-result"])) {
            state.queries[key]["result-meta"] = normalize(comp["result-meta"] ?? null);
          }
        }
        if (m.attrs) state.attrs = projectAttrs(m.attrs);
        break;
      }
      case "add-query-ok": {
        const projected = projectAnyResult(m.result);
        if (isTreeResult(m.result)) projected["result-meta"] = normalize(m["result-meta"] ?? null);
        state.queries[canon(normalize(m.q))] = projected;
        direct.push({
          op: m.op,
          q: normalize(m.q),
          result: projected,
          "processed-tx-id": "<tx>",
        });
        break;
      }
      case "error": {
        // client-read projection: type/status/message drive app-visible
        // errors (Reactor.js:513-548); original-event routing reads only
        // op/q/subscription-id/stream-id (Reactor.js:1009-1046); of hint,
        // only record-type is pattern-matched (Reactor.js:1020-1029) and
        // data-type names the failing input — the rest is debugging detail
        // whose shape differs (documented in docs/PARITY.md).
        const oe = m["original-event"] ?? {};
        direct.push({
          op: "error",
          status: m.status,
          type: m.type,
          message: m.message,
          "original-event": normalize(
            Object.fromEntries(
              ["op", "q", "subscription-id", "stream-id", "tx-steps", "client-event-id"]
                .filter((k) => k in oe)
                .map((k) => [k, oe[k]]),
            ),
          ),
          hint: m.hint
            ? normalize(
                Object.fromEntries(
                  ["data-type", "record-type"]
                    .filter((k) => k in m.hint)
                    .map((k) => [k, m.hint[k]]),
                ),
              )
            : null,
        });
        break;
      }
      case "app-status-changed": {
        // the client handler is idempotent (Reactor.js:899-920 sets the
        // status; the disabled branch fires only on a transition), and
        // legacy delivers every flip twice because both of its WAL consumers
        // run cache eviction (jdbc/wal.clj:692 and :766 -> cache_evict.clj
        // notify-app-status-changed), so fold repeats of the same status
        if (state.appStatus === m.status) break;
        state.appStatus = m.status;
        direct.push(normalize(m));
        break;
      }
      case "init-ok": {
        state.attrs = projectAttrs(m.attrs);
        state.selfSid = m["session-id"];
        direct.push({
          op: m.op,
          "app-status": m["app-status"],
          // auth contents are unread by the client (Reactor.js:644-660)
        });
        break;
      }
      case "sync-load-batch": {
        const sub = (state.sync ??= { entities: {} });
        for (const rows of m["join-rows"]) {
          for (const t of rows) {
            (sub.entities[t[0]] ??= []).push(normalize(t));
          }
        }
        break;
      }
      case "sync-update-triples": {
        const sub = (state.sync ??= { entities: {} });
        for (const tx of m.txes) {
          for (const c of tx.changes) {
            const t = normalize(c.triple);
            const list = (sub.entities[c.triple[0]] ??= []);
            if (c.action === "added") list.push(t);
            else {
              const i = list.findIndex((x) => canon(x) === canon(t));
              if (i >= 0) list.splice(i, 1);
            }
          }
        }
        break;
      }
      case "stream-append": {
        // reader applies files then content at offset (Stream.ts:1077-1090)
        const st = (state.streams ??= {});
        const s = (st[m["client-id"] ?? "?"] ??= { content: "", done: false });
        if (typeof m.offset === "number") {
          let payload = "";
          for (const f of m.files ?? []) {
            try {
              payload += await (await fetch(f.url)).text();
            } catch {
              payload += `<unfetchable:${f.size}>`;
            }
          }
          if (typeof m.content === "string") payload += m.content;
          s.content = s.content.slice(0, m.offset) + payload;
        }
        if (m.done) s.done = true;
        if (m["abort-reason"]) s.abortReason = m["abort-reason"];
        break;
      }
      case "stream-flushed": {
        // writer only trims its buffer on flush (Stream.ts:994 onFlush);
        // flush timing/chunking is server-internal, so fold to high-water mark
        const wf = (state.writerFlushed ??= { offset: 0, done: false });
        wf.offset = Math.max(wf.offset, m.offset ?? 0);
        if (m.done) wf.done = true;
        break;
      }
      default:
        direct.push(normalize(m));
    }
  }
  direct.sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
  return direct;
}

// attrs array → stable client-visible projection, keyed by etype.label
export function projectAttrs(attrs) {
  const out = {};
  for (const at of attrs) {
    const key = `${at["forward-identity"]?.[1]}.${at["forward-identity"]?.[2]}`;
    out[key] = normalize(at);
  }
  return out;
}

export function newState() {
  return { rooms: {}, queries: {}, attrs: null, selfSid: null };
}

// Comparable projection of a connection's folded state: presence rooms become
// sorted multisets of peer presence data (the client strips its own session
// and peers' session ids are server-chosen).
export function projectState(state) {
  const rooms = {};
  for (const [roomId, peers] of Object.entries(state.rooms)) {
    rooms[roomId] = Object.entries(peers)
      .filter(([sid]) => sid !== state.selfSid)
      .map(([, data]) => normalize(data))
      .sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
  }
  const out = { rooms, queries: state.queries, attrs: state.attrs };
  if (state.writerFlushed) out.writerFlushed = state.writerFlushed;
  if (state.sync) {
    const entities = {};
    for (const [eid, triples] of Object.entries(state.sync.entities)) {
      const sorted = [...triples].sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
      if (sorted.length) entities[eid] = sorted;
    }
    out.sync = entities;
  }
  if (state.streams) out.streams = state.streams;
  return out;
}

// ---------------------------------------------------------------------------
// Strict allowlists (issue #45, blind spot 5).
//
// An allowlist entry names exactly what it allows, so a new difference that
// happens to land on an allowlisted path still fails:
//
//   {
//     "path":    "^dash:38-platform/transferRevoke$",   ("probe" in errors-allowed.json)
//     "differs": ["^\\.(status|type|message|hint|keys|body)$"],
//     "legacy":  { "status": 500, "type": "unknown" },  (optional pins)
//     "rust":    { "status": 200, "body": { "count": 1 } },
//     "reason":  "...", "citation": "...",
//     "flaky": true, "flakyReason": "..."               (optional)
//   }
//
// `differs` is a list of anchored regexes over sub-paths of the compared
// value: `.status`, `.hint.debug-uri`, `.0.status` (array index), or the
// empty string (`^$`: the whole value, for scalars such as keyset
// "present"/"absent"). Every sub-path matching one of them is masked on both
// sides (present on one side only counts as a difference there too); what is
// left must be identical. `legacy` / `rust` pin each side's documented
// behavior as a deep subset (see matchPin), so the entry stops applying when
// either server changes. An owned entry that allowed nothing in a run fails
// its layer unless it is `flaky` (with a `flakyReason`).

const ALLOW_MASK = "<allowed-divergence>";

// Every compared-path prefix of allowed-divergences.json and the layer that
// owns it: each layer stale-checks exactly its own entries, and an entry no
// layer owns is rejected on load.
export const DIVERGENCE_PREFIXES = {
  replay: ["step:", "final/", "keyset/"],
  dash: ["dash:"],
  storage: ["storage:"],
  storagefuzz: ["storagefuzz:"],
  rulewheres: ["rulewheres:"],
  reftopics: ["reftopics:"],
  fuzzperms: ["fuzzperms:"],
  fuzzschema: ["fuzzschema:"],
  presence: ["presence:"],
  syncfuzz: ["syncfuzz:"],
  streamfuzz: ["streamfuzz:"],
  authfuzz: ["authfuzz:"],
  schemapush: ["schemapush:"],
};

function anchoredRegex(src, what) {
  if (typeof src !== "string" || !src.startsWith("^") || !src.endsWith("$") || src.endsWith("\\$")) {
    throw new Error(`${what}: ${JSON.stringify(src)} must be an anchored regex (^...$)`);
  }
  return new RegExp(src);
}

// Validate and compile one raw entry. `keyField` is "path" (allowed-
// divergences.json) or "probe" (errors-allowed.json).
export function compileAllowEntry(raw, keyField = "path") {
  const label = raw?.[keyField];
  const where = `allowlist entry ${JSON.stringify(label)}`;
  const key = anchoredRegex(label, where);
  if (!Array.isArray(raw.differs) || raw.differs.length === 0) {
    throw new Error(`${where}: needs a non-empty "differs" list of sub-path regexes`);
  }
  const differs = raw.differs.map((d) => anchoredRegex(d, `${where} differs`));
  if (typeof raw.reason !== "string" || !raw.reason) throw new Error(`${where}: needs a "reason"`);
  if (raw.flaky !== undefined && raw.flaky !== true) throw new Error(`${where}: "flaky" must be true when set`);
  if (raw.flaky && (typeof raw.flakyReason !== "string" || !raw.flakyReason)) {
    throw new Error(`${where}: a flaky entry needs a "flakyReason"`);
  }
  const known = new Set([keyField, "differs", "legacy", "rust", "reason", "citation", "flaky", "flakyReason"]);
  for (const k of Object.keys(raw)) if (!known.has(k)) throw new Error(`${where}: unknown field ${JSON.stringify(k)}`);
  return { raw, label, key, differs, used: 0 };
}

const isPlainObj = (v) => v !== null && typeof v === "object" && !Array.isArray(v);
const typeName = (v) => (v === null ? "null" : Array.isArray(v) ? "array" : typeof v);

// Deep-subset match of `value` against a pin. A pin object's keys must match
// the value's (extra keys in the value are fine); a pin array must match an
// array of the same length element-wise; scalars compare exactly. Operators:
// {"$regex": "..."} (a string matching it), {"$type": "string" | "number" |
// "boolean" | "null" | "array" | "object"}, {"$absent": true} (key not
// present). Returns null on a match, else a description of the first mismatch.
export function matchPin(pin, value, p = "") {
  const at = p || "(whole value)";
  if (isPlainObj(pin)) {
    if ("$regex" in pin) {
      return typeof value === "string" && new RegExp(pin.$regex).test(value)
        ? null
        : `${at}: ${JSON.stringify(value)} does not match /${pin.$regex}/`;
    }
    if ("$type" in pin) {
      return typeName(value) === pin.$type ? null : `${at}: expected a ${pin.$type}, got ${JSON.stringify(value)}`;
    }
    if ("$absent" in pin) return value === undefined ? null : `${at}: expected absent, got ${JSON.stringify(value)}`;
    if (!isPlainObj(value)) return `${at}: expected an object, got ${JSON.stringify(value)}`;
    for (const [k, sub] of Object.entries(pin)) {
      const m = matchPin(sub, value[k], `${p}.${k}`);
      if (m) return m;
    }
    return null;
  }
  if (Array.isArray(pin)) {
    if (!Array.isArray(value) || value.length !== pin.length) {
      return `${at}: expected an array of ${pin.length}, got ${JSON.stringify(value)}`;
    }
    for (let i = 0; i < pin.length; i++) {
      const m = matchPin(pin[i], value[i], `${p}.${i}`);
      if (m) return m;
    }
    return null;
  }
  return canon(pin) === canon(value) ? null : `${at}: expected ${JSON.stringify(pin)}, got ${JSON.stringify(value)}`;
}

// Mask every sub-path matching `differs` on both sides; collect the masked
// sub-paths whose values actually differed.
function maskAllowed(a, b, differs, p, hits) {
  if (differs.some((re) => re.test(p))) {
    if (canon(a) !== canon(b)) hits.push(p);
    return [ALLOW_MASK, ALLOW_MASK];
  }
  const isObj = (v) => v !== null && typeof v === "object";
  if (!isObj(a) || !isObj(b) || Array.isArray(a) !== Array.isArray(b)) return [a, b];
  if (Array.isArray(a)) {
    const oa = [];
    const ob = [];
    for (let i = 0; i < Math.max(a.length, b.length); i++) {
      const [x, y] = maskAllowed(a[i], b[i], differs, `${p}.${i}`, hits);
      if (x === ALLOW_MASK && y === ALLOW_MASK) {
        oa.push(x);
        ob.push(y);
      } else {
        if (i < a.length) oa.push(x);
        if (i < b.length) ob.push(y);
      }
    }
    return [oa, ob];
  }
  const oa = {};
  const ob = {};
  for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
    const [x, y] = maskAllowed(a[k], b[k], differs, `${p}.${k}`, hits);
    if (x === ALLOW_MASK && y === ALLOW_MASK) {
      oa[k] = x;
      ob[k] = y;
    } else {
      if (k in a) oa[k] = x;
      if (k in b) ob[k] = y;
    }
  }
  return [oa, ob];
}

// The first sub-path (`.a.0.b`; the empty string is the whole value) where
// two values differ.
export function firstDifference(a, b, p = "") {
  if (canon(a) === canon(b)) return null;
  const isObj = (v) => v !== null && typeof v === "object";
  if (isObj(a) && isObj(b) && Array.isArray(a) === Array.isArray(b)) {
    for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
      const d = firstDifference(a[k], b[k], `${p}.${k}`);
      if (d) return d;
    }
  }
  return { p, a, b };
}

// Does `entry` (compiled) allow this legacy/rust pair? The caller has already
// matched the entry's path. Returns { ok: true, differed: [sub-paths] } or
// { ok: false, why }.
export function allowDivergence(entry, legacyVal, rustVal) {
  for (const [side, val] of [["legacy", legacyVal], ["rust", rustVal]]) {
    if (entry.raw[side] === undefined) continue;
    const m = matchPin(entry.raw[side], val);
    if (m) return { ok: false, why: `${side} no longer matches the pinned behavior: ${m}` };
  }
  const differed = [];
  const [ma, mb] = maskAllowed(legacyVal, rustVal, entry.differs, "", differed);
  if (canon(ma) !== canon(mb)) {
    const fd = firstDifference(ma, mb);
    return {
      ok: false,
      why: `differs outside the allowed sub-paths, first at ${fd.p || "(whole value)"}: legacy ${JSON.stringify(fd.a)?.slice(0, 300)} vs rust ${JSON.stringify(fd.b)?.slice(0, 300)}`,
    };
  }
  return { ok: true, differed };
}

// Load an allowlist file for one layer. `prefixes` (for allowed-
// divergences.json) keeps the entries whose path starts with `^<prefix>` for
// one of them, after rejecting entries no layer owns; null keeps every entry
// (errors-allowed.json, keyed by "probe").
export function loadAllowlist(file, { keyField = "path", prefixes = null } = {}) {
  const all = JSON.parse(fs.readFileSync(file, "utf8")).map((raw) => compileAllowEntry(raw, keyField));
  if (!prefixes) return makeAllowlist(all);
  const owned = (e, ps) => ps.some((pre) => e.label.startsWith(`^${pre}`));
  const everyPrefix = Object.values(DIVERGENCE_PREFIXES).flat();
  for (const e of all) {
    if (!owned(e, everyPrefix)) {
      throw new Error(`allowlist entry ${JSON.stringify(e.label)}: path must start with ^ and one of ${everyPrefix.join(" ")}`);
    }
  }
  return makeAllowlist(all.filter((e) => owned(e, prefixes)));
}

export function makeAllowlist(entries) {
  return {
    entries,
    // { entry, differed, rejected } when an entry allows the pair, else
    // { entry: null, rejected }; `rejected` lists the entries whose path
    // matched but whose pins or sub-paths did not, with why
    check(p, legacyVal, rustVal) {
      const rejected = [];
      for (const entry of entries) {
        if (!entry.key.test(p)) continue;
        const v = allowDivergence(entry, legacyVal, rustVal);
        if (v.ok) {
          entry.used++;
          return { entry, differed: v.differed, rejected };
        }
        rejected.push({ entry, why: v.why });
      }
      return { entry: null, rejected };
    },
    // print the entries that allowed nothing; returns how many of them are
    // not flaky (each one fails the layer)
    reportStale(log = console.log) {
      let failing = 0;
      for (const e of entries) {
        if (e.used) continue;
        if (e.raw.flaky) {
          log(`\n[STALE ALLOWLIST, flaky] ${e.label} allowed nothing this run: ${e.raw.flakyReason}`);
        } else {
          failing++;
          log(`\n[STALE ALLOWLIST] ${e.label} allowed nothing this run: drop it, fix its pins, or mark it flaky with a flakyReason`);
        }
      }
      return failing;
    },
  };
}

// Log lines for one compared difference's allowlist verdict (from check()).
export function describeAllowVerdict(verdict) {
  const lines = [];
  if (verdict.entry) {
    const subs = verdict.differed.map((d) => d || "(whole value)").join(", ");
    lines.push(`  allowed by ${verdict.entry.label}; differing sub-paths: ${subs}`);
  }
  for (const { entry, why } of verdict.rejected ?? []) lines.push(`  allowlist entry ${entry.label} does not apply: ${why}`);
  return lines;
}


// ---------------------------------------------------------------------------
// legacy refresh failures caused by another session's cancelled computation
//
// Legacy runs one datalog computation per (app, datalog query) and hands the
// same CompletableFuture to every session that asks for it while it runs
// (reactive/store.clj swap-datalog-cache!). The computation runs under the
// statement tracker and child-vfuture set of the session that started it.
// When that session goes away (on-close cancels its pending handlers) or
// times out, and it was the only registered watcher at that instant, legacy
// cancels the in-flight SQL and child futures (cancel-in-progress-datalog-
// query). A session that joined the same future meanwhile then fails its
// *refresh* although it did nothing: the cancelled statement surfaces as a
// typed `sql-exception` (connection-failure) or `timeout` (57014 query
// canceled), and a cancelled child future / interrupted thread as an untyped
// exception, which handle-uncaught-err answers with a typeless 500 "Yikes".
// instaql-query-reactive! then removes the failed query from the session
// (reactive/query.clj catch -> remove-query!), so that session never refreshes
// it again: every later checkpoint shows it stale while a fresh legacy session
// agrees with rust. Nightly fuzz-perms seed 9 op 73 (2026-11-25, run
// 45620798435) caught the typeless flavour; the typed flavours reproduce
// within a minute against the legacy image with stable subscribers and
// sessions that open, add-query and disconnect while writes land.
//
// Only these exact (type, status, message) shapes, on a *refresh* error, can
// be excused, and only by legacyCancelledRefresh below.
export const LEGACY_CANCELLED_REFRESH_ERRORS = [
  { type: null, status: 500, messagePrefix: "Yikes, something broke on our end!" },
  { type: "sql-exception", status: 500, messagePrefix: "SQL Exception: connection-failure" },
  { type: "timeout", status: 500, messagePrefix: "The query took too long to complete." },
];
export const isLegacyCancelledRefreshError = (err, message) =>
  !!err &&
  typeof message === "string" &&
  LEGACY_CANCELLED_REFRESH_ERRORS.some(
    (k) => (err.type ?? null) === k.type && err.status === k.status && message.startsWith(k.messagePrefix),
  );

// A checkpoint where the only difference is legacy refresh errors of the
// shapes above (plus subscriptions legacy left stale), and rust provably
// missed nothing. Every condition must hold:
//   - rust's refresh errors for each session equal legacy's once the
//     excusable legacy errors are dropped; rust's own errors are never
//     dropped, so a rust 5xx / timeout / deadlock reply still fails;
//   - for a session that got an excused legacy error, every one of rust's
//     subscriptions equals rust's fresh probe of it (the refresh legacy lost
//     reached the rust client);
//   - every subscription where the two sessions differ is one legacy left
//     stale: legacy's fresh probe and rust's fresh probe both equal rust's
//     session (the legacyStale rule);
//   - both sides hold the same subscriptions, and at least one excused error
//     exists (otherwise this is not the shape this rule is for).
// l / r: the checkpoint outcomes ({ i, kind, state: { [s]: { subs,
// refreshErrors } } }); lf / rf: fresh probes ({ [s]: { [key]: answer } });
// messageOf: the error frame's message for a refreshErrors entry.
export function legacyCancelledRefresh(l, r, lf, rf, messageOf, sessions) {
  if (!l || !r || l.i !== r.i || l.kind !== "checkpoint" || r.kind !== "checkpoint") return false;
  const same = (a, b) => a !== undefined && b !== undefined && canon(a) === canon(b);
  let excused = 0;
  for (const s of sessions) {
    const ls = l.state?.[s], rs = r.state?.[s];
    if (!ls || !rs) return false;
    const le = ls.refreshErrors ?? [], re = rs.refreshErrors ?? [];
    const kept = le.filter((e) => !isLegacyCancelledRefreshError(e, messageOf(e)));
    if (canon(kept) !== canon(re)) return false;
    const crashed = kept.length !== le.length;
    excused += le.length - kept.length;
    const lkeys = Object.keys(ls.subs ?? {}).sort(), rkeys = Object.keys(rs.subs ?? {}).sort();
    if (canon(lkeys) !== canon(rkeys)) return false;
    for (const key of rkeys) {
      const lv = ls.subs[key], rv = rs.subs[key];
      if (crashed && !same(rf?.[s]?.[key], rv)) return false;
      if (canon(lv) === canon(rv)) continue;
      if (!(same(lf?.[s]?.[key], rv) && same(rf?.[s]?.[key], rv))) return false;
    }
  }
  return excused > 0;
}
