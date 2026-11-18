// Seeded stream fuzz layer of the differential harness.
//
// Stream.ts drives create-stream (`start-stream`), `append-stream`,
// `subscribe-stream` / `unsubscribe-stream` and a read stream that fetches
// flushed `files` then applies inline `content` at `offset`. The replay layer
// only has a few fixed stream steps, so this layer generates:
//   - client, signed-in and admin write streams under generated $streams
//     create / view rules (guest create is limited to guest-/public- ids,
//     guest view to public- ids, user/admin bypass; a ruleParams exception
//     lets a guest create a private id);
//   - appends of varied sizes and chunk boundaries (empty chunks, unicode,
//     multi-chunk, an over-1MB flush), done and abort, appends after done,
//     and the >1MB file-flush path so resume start-offsets are comparable;
//   - late subscribers, subscribers from 0 / the current end / a flushed-file
//     boundary, multiple concurrent readers, reader reconnects that resume
//     every live subscription from its seen byte offset, and unsubscribes;
//   - invalid ops and missing / malformed parameters for all four stream ops,
//     including append by a non-writer and a guest subscribing to a stream
//     its view rule denies, with the complete error envelope compared.
//
// A subscriber's frames are folded the way Stream.ts reads them: files are
// fetched, the frame's offset is used to discard already-seen bytes, content
// bytes are decoded per frame, done / abort-reason close the reader, and
// frames that arrive for an unsubscribed or finished iterator are ignored.
// Legacy splits one live append into a content frame followed by a done
// frame, this server combines them; both are one client delivery, so frames
// of one op are compared as the delivery they produce (first offset, bytes,
// decoded content hash, done, abort-reason). Flush timing is server-internal
// too: stream-flushed is folded to its high-water mark and compared on the
// done / forced-flush ops. Only server-chosen ids, timestamps and trace ids
// are normalized; the settled per-subscriber byte stream and every ack /
// error key set are compared.
//
// Usage: node fuzz-streams.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL, LEGACY_DATABASE_URL, RUST_DATABASE_URL

import { createHash } from "node:crypto";
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
const seed = Number(process.argv[5] ?? 401);
const rounds = Number(process.argv[6] ?? 30);
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node fuzz-streams.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");

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

const QUIET_MS = 120;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sha256 = (s) => createHash("sha256").update(s).digest("hex");

function prng(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const rules = {
  $streams: {
    allow: {
      // The create check binds data = {clientId}; view binds the stream row.
      create:
        "auth.id != null || data.clientId.startsWith('guest-') || data.clientId.startsWith('public-') || (has(ruleParams.allow) && ruleParams.allow == true)",
      view: "auth.id != null || data.clientId.startsWith('public-')",
    },
  },
};

function buildPlan() {
  const rand = prng(seed * 104729 + 401);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const mk = makeIdFactory(appIdLegacy);
  const user = { id: mk(), email: `stream-fuzz-${seed}@fuzz.example` };
  // Builders are evaluated once per server; ids they send must be chosen at
  // plan time so the original-event echo is byte-identical.
  const rawIds = {
    missingClientToken: mk(),
    blankClientToken: mk(),
    wrongPubToken: mk(),
    guestPrivateToken: mk(),
    unsubscribeUnknown: mk(),
    resumeWrongToken: mk(),
  };
  const randomBadOffset = pick([-1, "0", 1.5, true]);

  const readerRoles = ["RG", "RU", "RA"];
  const roleAuth = { RG: "guest", RU: "user", RA: "admin" };
  const chunksPool = ["", "a", "hello", "héllo", "🚀", "𝄞 music", " line\n", "x".repeat(17), "  spaces  "];

  const specs = [
    { label: "pub", writerRole: "W0", clientId: `public-${seed}-pub`, expectCreate: true },
    { label: "guest", writerRole: "W1", clientId: `guest-${seed}-guest`, expectCreate: true },
    { label: "user", writerRole: "W2", clientId: `private-${seed}-user`, expectCreate: true },
    { label: "admin", writerRole: "W3", clientId: `private-admin-${seed}`, expectCreate: true },
    { label: "denied", writerRole: "W4", clientId: `private-${seed}-denied`, expectCreate: false },
    {
      label: "rp",
      writerRole: "W5",
      clientId: `private-rp-${seed}`,
      expectCreate: true,
      ruleParams: { allow: true },
    },
    { label: "numeric", writerRole: "W6", clientId: 12345, expectCreate: true },
  ];
  const streams = new Map();
  for (const s of specs) {
    streams.set(s.label, { ...s, token: mk() });
    roleAuth[s.writerRole] = s.label === "admin" || s.label === "numeric" ? "admin" : s.label === "user" ? "user" : "guest";
  }
  const roles = [...readerRoles, ...specs.map((s) => s.writerRole)];
  const roleKind = (role) => roleAuth[role];
  const canView = (role, stream) =>
    roleKind(role) === "admin" || roleKind(role) === "user" || String(stream.clientId).startsWith("public-");

  const script = [];
  let seq = 0;
  const add = (op) => {
    op.name ||= `${op.kind}-${++seq}`;
    script.push(op);
    return op;
  };
  const streamOf = (label) => streams.get(label);
  const liveStreams = () => specs.filter((s) => s.expectCreate);
  const checkpoint = (label) => add({ kind: "checkpoint", name: label });

  const plannedActive = [];
  let subSeq = 0;
  const addSubscribe = (role, stream, offsetKind, extra = {}) => {
    const op = add({
      kind: "subscribe",
      role,
      stream: stream.label,
      by: extra.by ?? (chance(0.5) ? "stream-id" : "client-id"),
      offsetKind: offsetKind ?? pick(["zero", "total", "lastBoundary"]),
      ruleParams: extra.ruleParams,
      label: `sub-${++subSeq}`,
    });
    if (stream.expectCreate && canView(role, stream)) {
      op.active = true;
      plannedActive.push(op);
    }
    return op;
  };

  const raw = (role, name, build) =>
    add({ kind: "raw", role, name, build });

  // Create every stream first; the denied one pins the create-rule path.
  for (const s of specs) add({ kind: "create", stream: s.label, name: `create-${s.label}` });
  checkpoint("created");

  // Force a legacy flush before the first resume / late subscription, so
  // both servers report the same persisted offset (see the header).
  add({ kind: "flush", stream: "pub", name: "flush-pub" });
  add({ kind: "resume", stream: "pub", name: "resume-pub" });
  add({ kind: "append", stream: "pub", chunks: ["after-resume"], name: "append-after-resume" });
  checkpoint("after-resume");

  // Several concurrent readers on one public stream, plus guest / user /
  // admin readers on streams their view rules allow and deny.
  addSubscribe("RG", streamOf("pub"), "zero");
  addSubscribe("RU", streamOf("pub"), "zero");
  addSubscribe("RA", streamOf("pub"), "zero");
  addSubscribe("RU", streamOf("guest"), "zero");
  addSubscribe("RG", streamOf("guest"), "zero");
  addSubscribe("RU", streamOf("user"), "zero");
  addSubscribe("RG", streamOf("user"), "zero");
  addSubscribe("RA", streamOf("admin"), "zero");
  addSubscribe("RU", streamOf("rp"), "zero");
  addSubscribe("RG", streamOf("rp"), "zero");
  checkpoint("subscribed");

  add({ kind: "append", stream: "pub", chunks: ["live-1"], name: "append-live-pub" });
  add({ kind: "append", stream: "user", chunks: ["u-1", "u-2"], name: "append-live-user" });
  add({ kind: "append", stream: "guest", chunks: ["g-🚀"], name: "append-live-guest" });
  checkpoint("live");

  // Random churn over the not-yet-done streams.
  for (let i = 0; i < rounds; i++) {
    const r = rand();
    if (r < 0.43) {
      const st = streamOf(pick(liveStreams()).label);
      if (st.label === "pub" && st.plannedFlushes === undefined) st.plannedFlushes = 1;
      if (chance(0.05) && (st.plannedFlushes ?? 0) < 2) {
        st.plannedFlushes = (st.plannedFlushes ?? 0) + 1;
        add({ kind: "flush", stream: st.label });
      } else {
        const chunks = chance(0.12)
          ? []
          : Array.from({ length: 1 + Math.floor(rand() * 3) }, () => pick(chunksPool));
        add({ kind: "append", stream: st.label, chunks });
      }
    } else if (r < 0.61) {
      const role = pick(readerRoles);
      const candidates = liveStreams().filter(
        (st) => !plannedActive.some((op) => op.active && op.role === role && op.stream === st.label),
      );
      if (candidates.length) addSubscribe(role, streamOf(pick(candidates).label));
      else checkpoint(`no-sub-candidate-${i}`);
    } else if (r < 0.73) {
      const active = plannedActive.filter((op) => op.active);
      if (active.length) {
        const victim = pick(active);
        victim.active = false;
        add({ kind: "unsubscribe", role: victim.role, subLabel: victim.label });
      } else {
        checkpoint(`no-unsubscribe-${i}`);
      }
    } else if (r < 0.82) {
      const role = pick(readerRoles);
      if (plannedActive.some((op) => op.active && op.role === role)) add({ kind: "reconnectReader", role });
      else checkpoint(`no-reconnect-${i}`);
    } else if (r < 0.94) {
      checkpoint(`periodic-${i}`);
    } else {
      // Mix one of the parameter probes back into the random sequence.
      const st = streamOf(pick(liveStreams()).label);
      raw(pick(readerRoles), `random-bad-subscribe-${i}`, () => ({
        op: "subscribe-stream",
        "client-id": st.clientId,
        offset: randomBadOffset,
      }));
    }
  }

  // Invalid ops / parameter shapes while the streams are still live.
  raw("RA", "start-missing-client", () => ({ op: "start-stream", "reconnect-token": rawIds.missingClientToken }));
  raw("RA", "start-blank-client", () => ({ op: "start-stream", "client-id": "   ", "reconnect-token": rawIds.blankClientToken }));
  raw("W3", "start-missing-token", () => ({ op: "start-stream", "client-id": `missing-token-${seed}` }));
  raw("W3", "start-bad-token", () => ({
    op: "start-stream",
    "client-id": `bad-token-${seed}`,
    "reconnect-token": "not-a-uuid",
  }));
  raw("W0", "start-wrong-token", () => ({
    op: "start-stream",
    "client-id": streamOf("pub").clientId,
    "reconnect-token": rawIds.wrongPubToken,
  }));
  raw("RG", "start-guest-private-denied", () => ({
    op: "start-stream",
    "client-id": `private-denied-${seed}`,
    "reconnect-token": rawIds.guestPrivateToken,
  }));
  raw("W2", "append-missing-stream-id", () => ({ op: "append-stream", chunks: ["x"], offset: 0 }));
  raw("W2", "append-bad-stream-id", () => ({
    op: "append-stream",
    "stream-id": "not-a-uuid",
    chunks: ["x"],
    offset: 0,
  }));
  raw("W2", "append-missing-chunks", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    offset: 0,
  }));
  raw("W2", "append-bad-chunks", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    chunks: "x",
    offset: 0,
  }));
  raw("W2", "append-bad-chunk-element", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    chunks: [1],
    offset: 0,
  }));
  raw("W2", "append-missing-offset", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    chunks: ["x"],
  }));
  raw("W2", "append-negative-offset", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    chunks: ["x"],
    offset: -1,
  }));
  raw("W2", "append-wrong-offset", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    chunks: ["x"],
    offset: S.get("user").total + 7,
  }));
  raw("W2", "append-done-string", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    chunks: [],
    offset: 0,
    done: "yes",
  }));
  raw("W2", "append-blank-abort", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    chunks: [],
    offset: 0,
    done: true,
    "abort-reason": "   ",
  }));
  raw("RU", "append-not-writer", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    chunks: ["x"],
    offset: 0,
  }));
  raw("RA", "subscribe-no-ids", () => ({ op: "subscribe-stream" }));
  raw("RA", "subscribe-bad-stream-id", () => ({ op: "subscribe-stream", "stream-id": "not-a-uuid" }));
  raw("RA", "subscribe-negative-offset", (S) => ({
    op: "subscribe-stream",
    "stream-id": S.get("user").streamId,
    offset: -1,
  }));
  raw("RA", "subscribe-string-offset", (S) => ({
    op: "subscribe-stream",
    "stream-id": S.get("user").streamId,
    offset: "0",
  }));
  raw("RA", "subscribe-blank-client-id", () => ({ op: "subscribe-stream", "client-id": "   " }));
  raw("RA", "subscribe-unknown-client-id", () => ({
    op: "subscribe-stream",
    "client-id": `no-such-stream-${seed}`,
  }));
  raw("RG", "subscribe-private-denied", () => ({
    op: "subscribe-stream",
    "client-id": streamOf("user").clientId,
  }));
  raw("RA", "unsubscribe-missing", () => ({ op: "unsubscribe-stream" }));
  raw("RA", "unsubscribe-blank", () => ({ op: "unsubscribe-stream", "subscribe-event-id": "   " }));
  raw("RA", "unsubscribe-unknown", () => ({ op: "unsubscribe-stream", "subscribe-event-id": rawIds.unsubscribeUnknown }));
  checkpoint("invalid-ops");

  // Finish the streams with live readers, then probe the closed / completed
  // paths and late catch-up on a done stream (files on legacy, inline content
  // here; the reader projection compares the delivered bytes).
  add({ kind: "done", stream: "pub", chunks: [" final-pub"], abortReason: `cancelled-${seed}` });
  add({ kind: "done", stream: "user", chunks: [] });
  add({ kind: "done", stream: "guest", chunks: ["guest-final"], abortReason: "guest-abort" });
  checkpoint("done");
  addSubscribe("RA", streamOf("pub"), "zero");
  addSubscribe("RU", streamOf("pub"), "lastBoundary");
  addSubscribe("RU", streamOf("user"), "zero");
  addSubscribe("RU", streamOf("guest"), "zero");
  raw("W0", "append-after-done", (S) => ({
    op: "append-stream",
    "stream-id": S.get("pub").streamId,
    chunks: ["late"],
    offset: 0,
    done: false,
  }));
  raw("W2", "append-after-done-empty", (S) => ({
    op: "append-stream",
    "stream-id": S.get("user").streamId,
    chunks: [],
    offset: 0,
    done: true,
  }));
  raw("W0", "resume-done", () => ({
    op: "start-stream",
    "client-id": streamOf("pub").clientId,
    "reconnect-token": streamOf("pub").token,
  }));
  raw("W2", "resume-done-wrong-token", () => ({
    op: "start-stream",
    "client-id": streamOf("user").clientId,
    "reconnect-token": rawIds.resumeWrongToken,
  }));
  checkpoint("final-checkpoint");
  return {
    rules,
    script,
    roles,
    roleAuth,
    user,
    streamDefinitions: specs.map((s) => streams.get(s.label)),
  };
}

// ---------------------------------------------------------------------------
// client-visible projection

function errorView(m) {
  // Full request-scoped error envelope (status / type / message / hint). The
  // original event is projected to the keys the client routes on; legacy also
  // injects a session-id into it, which no handler reads (Reactor.js:1009-1046).
  const oe = m["original-event"] ?? {};
  return {
    op: "error",
    status: m.status,
    type: m.type,
    message: m.message,
    hint: normalize(m.hint),
    "original-event": normalize(
      Object.fromEntries(
        ["op", "q", "subscription-id", "stream-id", "client-event-id", "tx-steps"]
          .filter((k) => k in oe)
          .map((k) => [k, oe[k]]),
      ),
    ),
    "client-event-id": normalize(m["client-event-id"] ?? null),
  };
}

async function fetchBytes(url, cache) {
  if (cache.has(url)) return cache.get(url);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`stream file fetch failed: ${res.status} ${url}`);
  const bytes = Buffer.from(await res.arrayBuffer());
  cache.set(url, bytes);
  return bytes;
}

// Stream.ts discards `seen - frame.offset` bytes from the file bodies then
// inline content, and decodes each delivered piece with a TextDecoder. Returns
// the decoded text and byte count this frame actually delivers.
function consumeFrame(reader, m, fileCache) {
  let discard = Math.max(0, reader.seenBytes - (typeof m.offset === "number" ? m.offset : reader.seenBytes));
  let text = "";
  let bytes = 0;
  const decoder = new TextDecoder();
  const consume = (buf) => {
    if (discard > 0) {
      const drop = Math.min(discard, buf.length);
      discard -= drop;
      buf = buf.subarray(drop);
    }
    if (!buf.length) return;
    bytes += buf.length;
    text += decoder.decode(buf);
  };
  for (const f of m.files ?? []) {
    const buf = fileCache.get(f.url);
    if (!buf) throw new Error(`missing cached stream file ${f.url}`);
    consume(buf);
  }
  if (typeof m.content === "string") consume(Buffer.from(m.content, "utf8"));
  reader.seenBytes += bytes;
  reader.content += text;
  if (m.done) {
    reader.done = true;
    reader.active = false;
  }
  if ("abort-reason" in m) reader.abortReason = m["abort-reason"];
  return { text, bytes };
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
  const definitions = plan.streamDefinitions;
  const streams = new Map(
    definitions.map((def) => [
      def.label,
      {
        ...def,
        streamId: null,
        total: 0,
        boundaries: [0],
        done: false,
        flushOffset: 0,
        flushDone: false,
      },
    ]),
  );

  const readers = new Map();
  const readersByEvent = new Map();
  let outcomes = [];

  const openRole = async (role) => {
    const c = connect(url, appId, `${serverName}:${role}`);
    await c.open;
    const init = { op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.22.76" } };
    const kind = plan.roleAuth[role];
    if (kind === "user") init["refresh-token"] = user.refresh_token;
    if (kind === "admin") init["__admin-token"] = adminToken;
    c.send(init);
    const ok = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
    if (ok.op !== "init-ok") throw new Error(`[${serverName}] init ${role}: ${JSON.stringify(ok).slice(0, 400)}`);
    c.takeNewFrames();
    conns[role] = c;
  };

  const fileCache = new Map();
  const streamsById = () => {
    const out = new Map();
    for (const st of streams.values()) if (st.streamId) out.set(st.streamId, st);
    return out;
  };

  const processFrames = async (role, rawFrames) => {
    const events = [];
    const deliveries = new Map();
    const byStreamId = streamsById();
    for (const m of rawFrames) {
      if (m.op === "error") {
        events.push(errorView(m));
        continue;
      }
      if (m.op === "start-stream-ok") {
        events.push(normalize(m));
        continue;
      }
      if (m.op === "stream-flushed") {
        const st = byStreamId.get(m["stream-id"]);
        if (st) {
          st.flushOffset = Math.max(st.flushOffset, m.offset ?? 0);
          if (m.done) st.flushDone = true;
        }
        continue;
      }
      if (m.op !== "stream-append") continue;
      const reader = readersByEvent.get(`${role}:${m["client-event-id"]}`);
      if (!reader || !reader.active) continue; // finished / unsubscribed iterator
      let delivery = deliveries.get(reader.label);
      if (!delivery) {
        delivery = {
          event: {
            op: "stream-append",
            sub: reader.label,
            stream: reader.stream,
            offset: null,
            bytes: 0,
            content: null,
            done: false,
            "abort-reason": null,
          },
          parts: [],
          textBytes: 0,
        };
        deliveries.set(reader.label, delivery);
        events.push(delivery.event);
      }
      if (delivery.event.offset === null && typeof m.offset === "number") delivery.event.offset = m.offset;
      if (m.error) {
        delivery.event.error = normalize(m.error);
        delivery.event.retry = !!m.retry;
        reader.error = "stream-append-error";
        reader.active = false;
        continue;
      }
      for (const f of m.files ?? []) await fetchBytes(f.url, fileCache);
      const consumed = consumeFrame(reader, m, fileCache);
      delivery.parts.push(consumed.text);
      delivery.textBytes += consumed.bytes;
      delivery.event.bytes = delivery.textBytes;
      if (m.done) delivery.event.done = true;
      if ("abort-reason" in m) delivery.event["abort-reason"] = normalize(m["abort-reason"]);
    }
    for (const d of deliveries.values()) d.event.content = sha256(d.parts.join(""));
    return events;
  };

  const drain = async () => {
    await settle(Object.values(conns).filter(Boolean), QUIET_MS);
    const events = {};
    for (const role of plan.roles) {
      const raw = conns[role]?.takeNewFrames() ?? [];
      events[role] = await processFrames(role, raw);
    }
    return events;
  };

  const readerState = () =>
    [...readers.values()]
      .sort((a, b) => (a.label < b.label ? -1 : a.label > b.label ? 1 : 0))
      .map((r) => ({
        label: r.label,
        role: r.role,
        stream: r.stream,
        active: r.active,
        seenBytes: r.seenBytes,
        content: sha256(r.content),
        done: r.done,
        "abort-reason": normalize(r.abortReason ?? null),
        error: r.error ?? null,
      }));

  const addReader = (role, streamLabel, eventId, by, ruleParams) => {
    const label = `reader-${readers.size + 1}`;
    const reader = {
      label,
      role,
      stream: streamLabel,
      eventId,
      by,
      ruleParams,
      seenBytes: 0,
      content: "",
      active: true,
      done: false,
      abortReason: null,
      error: null,
    };
    readers.set(label, reader);
    readersByEvent.set(`${role}:${eventId}`, reader);
    return reader;
  };

  const offsetFor = (st, kind) => {
    if (kind === "zero") return 0;
    if (kind === "total") return st.total;
    if (kind === "lastBoundary") return st.boundaries.at(-1) ?? st.total;
    return 0;
  };

  const activeReadersFor = (streamLabel) =>
    [...readers.values()].filter((r) => r.active && r.stream === streamLabel);

  // A live append reaches every active subscriber as a frame at the append's
  // start offset; a completing append also reaches them with done=true (legacy
  // may split that into a content frame plus a done frame). Waiting for those
  // frames keeps a slow delivery from being attributed to the next op.
  const waitAppendDeliveries = async (st, done, startOffset) => {
    const active = activeReadersFor(st.label);
    await Promise.all(
      active.map((reader) =>
        conns[reader.role]
          .waitFor(
            (m) =>
              m.op === "stream-append" &&
              m["client-event-id"] === reader.eventId &&
              (done ? m.done === true : m.offset === startOffset),
            20000,
          )
          .catch((e) => {
            throw new Error(`${e.message} waiting for ${reader.label} on ${st.label} at ${startOffset}`);
          }),
      ),
    );
    return active.length;
  };

  const waitReply = (role, ceid, op, timeout = 15000) =>
    conns[role].waitFor((m) => m["client-event-id"] === ceid && m.op === op, timeout);

  const waitErrorOr = async (role, ceid, op, timeout = 15000) => {
    const m = await conns[role].waitFor(
      (x) => x["client-event-id"] === ceid && (x.op === "error" || x.op === op),
      timeout,
    );
    return m;
  };

  for (const role of plan.roles) await openRole(role);

  for (const op of plan.script) {
    if (op.kind === "create") {
      const def = definitions.find((d) => d.label === op.stream);
      const st = streams.get(op.stream);
      const ceid = crypto.randomUUID();
      const msg = { op: "start-stream", "client-id": def.clientId, "reconnect-token": def.token };
      if (def.ruleParams) msg["rule-params"] = def.ruleParams;
      conns[def.writerRole].send({ "client-event-id": ceid, ...msg });
      const reply = await waitErrorOr(def.writerRole, ceid, "start-stream-ok");
      if (reply.op === "start-stream-ok") {
        st.streamId = reply["stream-id"];
        st.total = 0;
        st.writerRole = def.writerRole;
        st.clientId = reply["client-id"];
      }
      await sleep(30);
      outcomes.push({ name: op.name, kind: op.kind, events: await drain() });
    } else if (op.kind === "resume") {
      const st = streams.get(op.stream);
      if (!st?.streamId) {
        outcomes.push({ name: op.name, kind: op.kind, skipped: true });
        continue;
      }
      const def = definitions.find((d) => d.label === op.stream);
      const ceid = crypto.randomUUID();
      conns[st.writerRole].send({
        "client-event-id": ceid,
        op: "start-stream",
        "client-id": def.clientId,
        "reconnect-token": def.token,
      });
      await waitErrorOr(st.writerRole, ceid, "start-stream-ok");
      await sleep(30);
      outcomes.push({ name: op.name, kind: op.kind, events: await drain() });
    } else if (op.kind === "append" || op.kind === "flush" || op.kind === "done") {
      const st = streams.get(op.stream);
      if (!st?.streamId) {
        outcomes.push({ name: op.name, kind: op.kind, skipped: true });
        continue;
      }
      const chunks =
        op.kind === "flush" ? ["x".repeat(1_100_000)] : op.chunks ?? [];
      const byteLen = chunks.reduce((n, c) => n + Buffer.byteLength(c, "utf8"), 0);
      const offset = st.total;
      const ceid = crypto.randomUUID();
      const done = op.kind === "done";
      const msg = {
        "client-event-id": ceid,
        op: "append-stream",
        "stream-id": st.streamId,
        chunks,
        offset,
        done,
      };
      if (op.abortReason) msg["abort-reason"] = op.abortReason;
      conns[st.writerRole].send(msg);
      st.total += byteLen;
      if (done) {
        st.done = true;
        st.boundaries.push(st.total);
      }
      if (op.kind === "flush") st.boundaries.push(st.total);
      if (op.kind === "flush" || done) {
        await conns[st.writerRole].waitFor(
          (m) =>
            (m.op === "error" && m["client-event-id"] === ceid) ||
            (m.op === "stream-flushed" &&
              m["stream-id"] === st.streamId &&
              (m.offset ?? 0) >= st.total &&
              (!done || m.done === true)),
          20000,
        );
        await waitAppendDeliveries(st, done, offset);
        await sleep(30);
      } else {
        const delivered = await waitAppendDeliveries(st, false, offset);
        await sleep(delivered ? 30 : 120);
      }
      const events = await drain();
      const outcome = { name: op.name, kind: op.kind, events };
      if (op.kind === "flush" || done) {
        outcome["writer-flushed"] = { offset: st.flushOffset, done: st.flushDone };
      }
      outcomes.push(outcome);
    } else if (op.kind === "subscribe") {
      const st = streams.get(op.stream);
      if (!st?.streamId) {
        outcomes.push({ name: op.name, kind: op.kind, skipped: true });
        continue;
      }
      const ceid = crypto.randomUUID();
      const msg = { "client-event-id": ceid, op: "subscribe-stream" };
      if (op.by === "stream-id") msg["stream-id"] = st.streamId;
      else msg["client-id"] = st.clientId;
      msg.offset = offsetFor(st, op.offsetKind);
      if (op.ruleParams) msg["rule-params"] = op.ruleParams;
      const reader = addReader(op.role, op.stream, ceid, op.by, op.ruleParams);
      // Stream.ts starts a read stream at byteOffset, so the first frame's
      // offset is allowed to equal it rather than only exceed 0.
      reader.seenBytes = msg.offset;
      reader.opLabel = op.label;
      conns[op.role].send(msg);
      const reply = await waitErrorOr(op.role, ceid, "stream-append");
      if (reply.op === "error") {
        reader.error = errorView(reply);
        reader.active = false;
      }
      await sleep(30);
      outcomes.push({ name: op.name, kind: op.kind, events: await drain() });
    } else if (op.kind === "unsubscribe") {
      const reader = [...readers.values()].find((r) => r.opLabel === op.subLabel);
      if (!reader || !reader.active) {
        outcomes.push({ name: op.name, kind: op.kind, skipped: true });
        continue;
      }
      conns[op.role].send({
        "client-event-id": crypto.randomUUID(),
        op: "unsubscribe-stream",
        "subscribe-event-id": reader.eventId,
      });
      // Stream.ts deletes the iterator immediately; late frames are ignored.
      reader.active = false;
      await sleep(100);
      outcomes.push({ name: op.name, kind: op.kind, events: await drain() });
    } else if (op.kind === "reconnectReader") {
      const active = [...readers.values()].filter((r) => r.role === op.role && r.active);
      conns[op.role].close();
      delete conns[op.role];
      await openRole(op.role);
      // Resume one subscription at a time: legacy runs each subscribe under
      // its own group key, so catch-up frames for several would interleave in
      // server-chosen order.
      const events = Object.fromEntries(plan.roles.map((r) => [r, []]));
      for (const reader of active.sort((a, b) => (a.label < b.label ? -1 : 1))) {
        const ceid = crypto.randomUUID();
        const oldKey = `${reader.role}:${reader.eventId}`;
        readersByEvent.delete(oldKey);
        reader.eventId = ceid;
        reader.active = true;
        readersByEvent.set(`${reader.role}:${ceid}`, reader);
        const msg = { "client-event-id": ceid, op: "subscribe-stream", offset: reader.seenBytes };
        if (reader.by === "stream-id") msg["stream-id"] = streams.get(reader.stream).streamId;
        else msg["client-id"] = streams.get(reader.stream).clientId;
        if (reader.ruleParams) msg["rule-params"] = reader.ruleParams;
        conns[op.role].send(msg);
        const reply = await waitErrorOr(op.role, ceid, "stream-append");
        if (reply.op === "error") {
          reader.error = errorView(reply);
          reader.active = false;
        }
        await sleep(30);
        const stepEvents = await drain();
        for (const role of plan.roles) events[role].push(...stepEvents[role]);
      }
      outcomes.push({ name: op.name, kind: op.kind, events });
    } else if (op.kind === "raw") {
      const msg = op.build(streams, definitions);
      const ceid = crypto.randomUUID();
      conns[op.role].send({ "client-event-id": ceid, ...msg });
      await waitReply(op.role, ceid, "error", 15000);
      await sleep(30);
      outcomes.push({ name: op.name, kind: op.kind, events: await drain() });
    } else if (op.kind === "checkpoint") {
      await sleep(120);
      await settle(Object.values(conns).filter(Boolean), 180);
      const events = await drain();
      outcomes.push({ name: op.name, kind: op.kind, events, readers: readerState() });
    } else {
      throw new Error(`unknown plan op ${op.kind}`);
    }
  }

  await sleep(120);
  await settle(Object.values(conns).filter(Boolean), 180);
  outcomes.push({ name: "final", kind: "final", events: await drain(), readers: readerState() });
  for (const c of Object.values(conns)) c.close();
  return { outcomes };
}

// ---------------------------------------------------------------------------
// run both servers and compare

const plan = buildPlan();
console.log(`fuzz-streams: seed=${seed} rounds=${rounds} ops=${plan.script.length}`);
console.log("running against legacy…");
const legacy = await runOn("legacy", plan);
console.log("running against rust…");
const rust = await runOn("rust", plan);

const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), {
  prefixes: DIVERGENCE_PREFIXES.streamfuzz,
});
let mismatches = 0;
let allowed = 0;
const maxOutcomes = Math.max(legacy.outcomes.length, rust.outcomes.length);
for (let i = 0; i < maxOutcomes; i++) {
  const a = legacy.outcomes[i];
  const b = rust.outcomes[i];
  const name = a?.name ?? b?.name ?? `#${i}`;
  const p = `streamfuzz:${name}`;
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
  console.error(" legacy:", JSON.stringify(fd.a)?.slice(0, 1600));
  console.error(" rust:  ", JSON.stringify(fd.b)?.slice(0, 1600));
  for (const line of lines) console.error(line);
}

const stale = allowlist.reportStale();
console.log(
  `\nfuzz-streams seed ${seed}: ${legacy.outcomes.length} checkpoints, ${mismatches} mismatch(es), ${allowed} allowed`,
);
if (mismatches || stale) {
  console.error(`FUZZ STREAMS FAILED for seed ${seed}`);
  process.exit(1);
}
console.log(`FUZZ STREAMS PASSED for seed ${seed}`);
process.exit(0);
