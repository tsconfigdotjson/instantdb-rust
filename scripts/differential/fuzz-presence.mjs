// Seeded rooms / presence fuzz layer of the differential harness.
//
// Several concurrent sessions (old and new @instantdb/core versions, guests,
// signed-in users and an admin) run random join-room / leave-room /
// set-presence / client-broadcast sequences over a few rooms and room types,
// including re-joins, invalid ops and /admin/rooms/presence reads. Both
// servers run the same script and these are compared:
//   - every ack (op + key set) and every error (status, type, message, hint);
//   - at settled checkpoints, each session's folded presence state per room
//     (what Reactor.js computes from refresh-presence / patch-presence);
//   - every client-broadcast's recipients and envelope (wrong recipients,
//     missing/duplicate deliveries and payloads all fail);
//   - /admin/rooms/presence: status, error envelope, and each entry's key set,
//     peer, instance id shape, user entity and data.
//
// The client versions exercise legacy's own feature gate: @instantdb/core
// <= 0.17.5 only gets full refresh-presence snapshots, newer clients may get
// patch-presence edits. The layer asserts the old-client side of that gate and
// that a new client got at least one patch, then folds both frame shapes the
// way the client does.
//
// A session's first join into a room waits for the full refresh-presence
// snapshot before the next op is sent. Legacy's receive queue can combine that
// snapshot with a following patch and deliver only the patch, which leaves a
// modern client without the peers that did not change in between; that is a
// legacy lost-snapshot defect, not a contract (see docs/PARITY.md).
//
// Only genuinely nondeterministic values are normalized: session ids are
// mapped back to their role, instance ids and timestamps are placeholders.
// Room ids, presence data, user ids (seeded deterministically on both
// servers), topics and broadcast payloads stay raw.
//
// Usage: node fuzz-presence.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL

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
  settle,
} from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));

const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
const seed = Number(process.argv[5] ?? 101);
const rounds = Number(process.argv[6] ?? 100);
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node fuzz-presence.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");

const SERVERS = {
  legacy: { url: process.env.LEGACY_URL || "http://localhost:8891", appId: appIdLegacy },
  rust: { url: process.env.RUST_URL || "http://localhost:8888", appId: appIdRust },
};

// A checkpoint waits for all sockets to go quiet this long; a broadcast is
// isolated by settling before and after it, so its deliveries can be attributed.
const SETTLE_MS = 500;
const BROADCAST_SETTLE_MS = 450;

const ROLES = [
  { name: "A", version: "v0.21.0" },
  { name: "B", version: "v0.17.5" }, // old client: full snapshots only
  { name: "C", version: "v0.22.76", user: "U1" }, // array frames + patches
  { name: "D", version: "v0.17.5", user: "U2" }, // old client: full snapshots only
  { name: "E", version: "v0.21.0", admin: true },
];
const OLD_ROLES = ["B", "D"];
const ROOMS = [
  { id: "presence-chat", type: "chat" },
  { id: "presence-cursor", type: "cursor" },
  { id: "presence-edge", type: "presence-fuzz" },
  { id: "", type: "chat" }, // an empty room id is a valid string in legacy
];

function prng(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const roomName = (room) => (room.id === "" ? "<empty>" : room.id);

function buildPlan() {
  const rand = prng(seed * 104729 + 37);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const shuffle = (arr) => {
    const out = [...arr];
    for (let i = out.length - 1; i > 0; i--) {
      const j = Math.floor(rand() * (i + 1));
      [out[i], out[j]] = [out[j], out[i]];
    }
    return out;
  };
  const mk = makeIdFactory(appIdLegacy);
  // Deterministic on both servers (the admin refresh-token route accepts an
  // explicit id), so admin presence user entities compare without masking ids.
  const users = {
    U1: { id: mk(), email: `presence-u1-${seed}@fuzz.example` },
    U2: { id: mk(), email: `presence-u2-${seed}@fuzz.example` },
  };

  const script = [];
  const joined = Object.fromEntries(ROLES.map((r) => [r.name, new Set()]));
  const dataFor = (role, i) => {
    const r = rand();
    if (r < 0.09) return { has: false };
    if (r < 0.13) return { has: true, value: null };
    if (r < 0.17) return { has: true, value: false };
    if (r < 0.20) return { has: true, value: 0 };
    if (r < 0.23) return { has: true, value: `raw-${role}-${i}` };
    return {
      has: true,
      value: pick([
        { who: role, seq: i, cursor: { x: i % 7, y: i % 3 } },
        { who: role, seq: i, nested: { a: { b: i }, list: [1, 2, i % 2] } },
        { who: role, seq: i, tags: ["a", i % 2 ? "b" : "c"] },
        {},
      ]),
    };
  };
  const add = (name, session, msg, opts = {}) => {
    script.push({ kind: "send", name, session, msg, ceid: mk(), ...opts });
  };
  const join = (role, room, data, name, opts = {}) => {
    const msg = { op: "join-room", "room-type": room.type, "room-id": room.id };
    if (data.has) msg.data = data.value;
    // A session that was not already in the room needs the full snapshot the
    // server sends for a join. The runner waits for it before the next op:
    // legacy's receive queue can combine that snapshot with a following patch
    // and deliver only the patch (see the comment in runOn).
    const newJoin = !joined[role].has(room.id);
    add(name, role, msg, newJoin ? { ...opts, newJoin: true } : opts);
    joined[role].add(room.id);
  };
  const leave = (role, room, name, opts = {}) => {
    add(name, role, { op: "leave-room", "room-id": room.id }, opts);
    joined[role].delete(room.id);
  };
  const setData = (role, room, data, name, opts = {}) => {
    const msg = { op: "set-presence", "room-id": room.id };
    if (data.has) msg.data = data.value;
    add(name, role, msg, opts);
  };
  const broadcast = (role, room, topic, data, name, opts = {}) => {
    const msg = { op: "client-broadcast", "room-id": room.id, roomType: room.type, topic };
    if (data.has) msg.data = data.value;
    add(name, role, msg, opts);
  };
  const adminRead = (name, room, { roomType = room.type, roomId = room.id } = {}) =>
    script.push({ kind: "admin", name, roomType, roomId });

  // A deterministic prefix that guarantees both presence frame shapes are
  // produced: A and C are modern clients in the room before later joins and
  // updates, B and D are old clients that must only ever see full snapshots.
  const seedRoom = ROOMS[0];
  join("A", seedRoom, { has: true, value: { who: "A", seq: 0 } }, "seed-join-A", { settle: true });
  join("B", seedRoom, { has: true, value: { who: "B", seq: 0 } }, "seed-join-B", { settle: true });
  join("C", seedRoom, { has: true, value: { who: "C", seq: 0 } }, "seed-join-C", { settle: true });
  setData("A", seedRoom, { has: true, value: { who: "A", seq: 1 } }, "seed-set-A", { settle: true });
  join("D", seedRoom, { has: true, value: { who: "D", seq: 0 } }, "seed-join-D", { settle: true });
  join("E", seedRoom, { has: true, value: { who: "E", seq: 0 } }, "seed-join-E", { settle: true });
  setData("C", seedRoom, { has: true, value: { who: "C", seq: 1, profile: { title: "C" } } }, "seed-set-C", {
    settle: true,
  });
  script.push({ kind: "checkpoint", name: "seed" });
  adminRead("seed-admin", seedRoom);

  const invalidOp = (i) => {
    const role = pick(ROLES).name;
    const templates = [
      ["invalid-join-missing-room", { op: "join-room", "room-type": "chat" }],
      ["invalid-leave-missing-room", { op: "leave-room" }],
      ["invalid-set-missing-room", { op: "set-presence", data: { x: 1 } }],
      ["invalid-broadcast-missing-room", { op: "client-broadcast", topic: "t", data: { x: 1 } }],
      ["invalid-join-null-room", { op: "join-room", "room-type": "chat", "room-id": null }],
      ["invalid-join-false-room", { op: "join-room", "room-type": "chat", "room-id": false }],
      ["invalid-join-number-room", { op: "join-room", "room-type": "chat", "room-id": 7 }],
      ["invalid-join-object-room", { op: "join-room", "room-type": "chat", "room-id": { x: 1 } }],
      ["invalid-unknown-op", { op: "no-such-room-op", "room-id": seedRoom.id }],
    ];
    const notJoined = ROOMS.filter((r) => !joined[role].has(r.id));
    if (notJoined.length && chance(0.5)) {
      const room = pick(notJoined);
      if (chance(0.5)) {
        return add(`invalid-set-not-joined-${i}`, role, { op: "set-presence", "room-id": room.id, data: { x: 1 } });
      }
      return add(`invalid-broadcast-not-joined-${i}`, role, {
        op: "client-broadcast",
        "room-id": room.id,
        topic: "t",
        data: { x: 1 },
      });
    }
    const [name, msg] = pick(templates);
    return add(`${name}-${i}`, role, msg);
  };

  for (let i = 0; i < rounds; i++) {
    // Concurrent presence updates on one room from two or three sessions.
    // Each session writes only its own entry, so the settled state is ordered.
    // Joins are awaited first: legacy's coalesced room events can lose a
    // concurrent joiner's first snapshot, which is a legacy defect this
    // server does not reproduce (so the fuzz doesn't make it the baseline).
    if (i > 0 && i % 18 === 0) {
      const room = pick(ROOMS);
      const roles = shuffle(ROLES).slice(0, 2 + Math.floor(rand() * 2));
      for (const [k, r] of roles.entries()) {
        if (!joined[r.name].has(room.id)) {
          join(r.name, room, dataFor(r.name, i * 10 + k), `burst-join-${r.name}-${i}`, { settle: true });
        }
      }
      const ops = roles.map((r, k) => {
        const data = dataFor(r.name, i * 10 + k);
        const msg = { op: "set-presence", "room-id": room.id };
        if (data.has) msg.data = data.value;
        return { session: r.name, msg, ceid: mk(), name: `burst-set-${r.name}-${i}` };
      });
      script.push({ kind: "burst", name: `burst-${i}`, ops });
      continue;
    }

    const r = rand();
    if (r < 0.22) {
      const role = pick(ROLES).name;
      const room = pick(ROOMS);
      join(role, room, dataFor(role, i));
    } else if (r < 0.36) {
      const role = pick(ROLES).name;
      const fromJoined = ROOMS.filter((x) => joined[role].has(x.id));
      const room = fromJoined.length && chance(0.75) ? pick(fromJoined) : pick(ROOMS);
      leave(role, room);
    } else if (r < 0.58) {
      const role = pick(ROLES).name;
      const fromJoined = ROOMS.filter((x) => joined[role].has(x.id));
      const room = fromJoined.length && chance(0.78) ? pick(fromJoined) : pick(ROOMS);
      setData(role, room, dataFor(role, i));
    } else if (r < 0.70) {
      const role = pick(ROLES).name;
      const fromJoined = ROOMS.filter((x) => joined[role].has(x.id));
      const room = fromJoined.length && chance(0.8) ? pick(fromJoined) : pick(ROOMS);
      const topic = pick(["cursor", "emoji", "chat", "empty-topic"]);
      broadcast(role, room, topic, dataFor(role, i));
    } else if (r < 0.76) {
      const role = pick(ROLES).name;
      const fromJoined = ROOMS.filter((x) => joined[role].has(x.id));
      const room = fromJoined.length ? pick(fromJoined) : seedRoom;
      join(role, room, dataFor(role, i), `rejoin-${i}`);
    } else if (r < 0.86) {
      invalidOp(i);
    } else if (r < 0.93) {
      const room = pick(ROOMS);
      adminRead(`admin-${i}`, room);
    } else {
      script.push({ kind: "checkpoint", name: `checkpoint-${i}` });
    }

    if (i % 13 === 12) script.push({ kind: "checkpoint", name: `periodic-${i}` });
  }

  // Named edge probes: empty room ids are valid strings, malformed room ids
  // keep legacy's missing/malformed distinction, and admin reads cover every
  // room plus the route's parameter errors.
  const empty = ROOMS[3];
  join("A", empty, { has: true, value: { who: "A", empty: true } }, "edge-join-empty-room", { settle: true });
  join("B", empty, { has: false }, "edge-join-empty-missing-data", { settle: true });
  setData("A", empty, { has: true, value: { who: "A", empty: 1 } }, "edge-set-empty-room", { settle: true });
  broadcast("A", empty, "empty-room", { has: true, value: { n: 1 } }, "edge-broadcast-empty-room", {
    settle: true,
  });
  leave("A", empty, "edge-leave-empty-room", { settle: true });
  leave("B", empty, "edge-leave-empty-room-B", { settle: true });
  add("edge-set-not-joined", "D", { op: "set-presence", "room-id": "never-joined", data: { x: 1 } });
  add("edge-broadcast-not-joined", "E", {
    op: "client-broadcast",
    "room-id": "never-joined",
    roomType: "chat",
    topic: "t",
    data: { x: 1 },
  });
  add("edge-leave-not-joined", "C", { op: "leave-room", "room-id": "never-joined" });
  add("edge-unknown-op", "A", { op: "no-such-room-op", "room-id": seedRoom.id });
  for (const [name, value] of [
    ["edge-invalid-room-null", null],
    ["edge-invalid-room-false", false],
    ["edge-invalid-room-number", 7],
    ["edge-invalid-room-object", { x: 1 }],
    ["edge-invalid-room-array", []],
  ]) {
    add(name, "A", { op: "join-room", "room-type": "chat", "room-id": value });
  }
  join("A", seedRoom, { has: true, value: { who: "A", edge: true } }, "edge-join-before-set-missing-data", {
    settle: true,
  });
  add("edge-set-missing-data", "A", { op: "set-presence", "room-id": seedRoom.id });
  add("edge-join-missing-data", "C", { op: "join-room", "room-type": "chat", "room-id": "missing-data-room" });
  joined.C.add("missing-data-room");
  script.push({ kind: "checkpoint", name: "edge" });

  for (const [i, room] of ROOMS.entries()) adminRead(`final-admin-room-${i}`, room);
  adminRead("final-admin-missing-room", { id: "never-populated", type: "chat" });
  adminRead("final-admin-missing-type", seedRoom, { roomType: null });
  adminRead("final-admin-missing-id", seedRoom, { roomId: null });
  adminRead("final-admin-blank-type", seedRoom, { roomType: "" });
  adminRead("final-admin-blank-id", seedRoom, { roomId: "" });

  return { users, script };
}

// ---------------------------------------------------------------------------
// comparison helpers

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
const DROP_KEYS = new Set([
  "trace-id",
  "original-event",
  "debug-uri",
  "total-delay-ms",
  "ws-ping-latency-ms",
]);

function isFixed(id) {
  return typeof id === "string" && id.includes("-0000-4000-8000-");
}

// Session ids are mapped by the caller; any remaining UUID is server-chosen.
function norm(value, key = null) {
  if (typeof value === "string") {
    if (ISO_RE.test(value)) return "<ts>";
    return value.replace(UUID_RE, (m) => (isFixed(m) ? m : "<uuid>"));
  }
  if (typeof value === "number") {
    if (key === "created_at" || key === "createdAt") return "<ts>";
    return value > 1e12 ? "<ts>" : value;
  }
  if (Array.isArray(value)) return value.map((v) => norm(v));
  if (value && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (DROP_KEYS.has(k) || k === "session-id") continue;
      out[k] = norm(v, k);
    }
    return out;
  }
  return value;
}

const typeName = (v) => (v === null ? "null" : Array.isArray(v) ? "array" : typeof v);

function ackView(m) {
  if (m.op === "error") {
    return {
      kind: "error",
      keys: Object.keys(m).sort(),
      status: m.status,
      type: m.type,
      message: m.message,
      hint: norm(m.hint ?? null),
    };
  }
  return { kind: "ack", op: m.op, keys: Object.keys(m).sort(), value: norm(m) };
}

function applyEdit(room, edit, label) {
  const [path, op, value] = edit;
  if (!Array.isArray(path) || !path.length) return;
  const sid = label(path[0]);
  const rest = path.slice(1);
  if (rest.length === 0) {
    if (op === "-") delete room[sid];
    else room[sid] = value?.data ?? null;
    return;
  }
  if (rest[0] !== "data") return; // the client keeps only each peer's data
  const inner = rest.slice(1);
  if (inner.length === 0) {
    if (op === "-") delete room[sid];
    else room[sid] = value;
    return;
  }
  if (room[sid] === null || room[sid] === undefined || typeof room[sid] !== "object") {
    if (op === "-") return;
    room[sid] = {};
  }
  let node = room[sid];
  for (const k of inner.slice(0, -1)) {
    if (!node[k] || typeof node[k] !== "object") node[k] = {};
    node = node[k];
  }
  const last = inner[inner.length - 1];
  if (op === "-") delete node[last];
  else node[last] = value;
}

function projectPresence(state) {
  const rooms = {};
  for (const [room, peers] of Object.entries(state.rooms)) {
    rooms[room] = Object.entries(peers)
      .map(([peer, data]) => ({ peer, data: norm(data) }))
      .sort((a, b) => (a.peer < b.peer ? -1 : 1));
  }
  return { rooms };
}

function adminUserView(user) {
  if (user === null || user === undefined) return null;
  if (typeof user !== "object" || Array.isArray(user)) return norm(user);
  const out = {};
  for (const [k, v] of Object.entries(user)) out[k] = k === "created_at" ? "<ts>" : norm(v);
  return out;
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
    throw new Error(`mintUser ${user.email} failed: ${res.status} ${JSON.stringify(body).slice(0, 400)}`);
  }
  return body.user;
}

async function runOn(serverName, plan) {
  const { url, appId } = SERVERS[serverName];
  const users = {};
  for (const [name, user] of Object.entries(plan.users)) users[name] = await mintUser(url, appId, user);

  const conns = {};
  const sessionRole = {};
  const sessionUser = {};
  const label = (sid) => sessionRole[sid] ?? `<sid:${sid}>`;
  for (const role of ROLES) {
    const c = connect(url, appId, `${serverName}:presence:${role.name}`);
    await c.open;
    const init = {
      "client-event-id": plan.script[0]?.ceid ?? `init-${role.name}`,
      op: "init",
      "app-id": appId,
      versions: { "@instantdb/core": role.version },
    };
    if (role.user) init["refresh-token"] = users[role.user].refresh_token;
    if (role.admin) init["__admin-token"] = adminToken;
    c.send(init);
    const ok = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
    if (ok.op !== "init-ok") throw new Error(`[${serverName}] init ${role.name} failed: ${JSON.stringify(ok).slice(0, 400)}`);
    sessionRole[ok["session-id"]] = role.name;
    sessionUser[role.name] = role.user ? plan.users[role.user].id : null;
    c.takeNewFrames();
    conns[role.name] = c;
  }

  const states = Object.fromEntries(
    ROLES.map((r) => [
      r.name,
      {
        rooms: {},
        leftRooms: new Set(),
        joining: new Set(),
        kinds: { refresh: 0, patch: 0 },
      },
    ]),
  );
  const cursors = Object.fromEntries(ROLES.map((r) => [r.name, conns[r.name].frames.length]));

  const fold = (role) => {
    const st = states[role];
    const c = conns[role];
    const frames = c.frames.slice(cursors[role]);
    cursors[role] = c.frames.length;
    for (const m of frames) {
      const room = m["room-id"];
      if (m.op === "join-room-ok") {
        if (typeof room === "string") st.leftRooms.delete(room);
      } else if (m.op === "leave-room-ok") {
        if (typeof room === "string") {
          delete st.rooms[room];
          st.leftRooms.add(room);
        }
      } else if (m.op === "refresh-presence") {
        if (typeof room !== "string" || (st.leftRooms.has(room) && !st.joining.has(room))) continue;
        st.kinds.refresh++;
        st.rooms[room] = Object.fromEntries(
          Object.entries(m.data ?? {}).map(([sid, entry]) => [label(sid), entry?.data ?? null]),
        );
      } else if (m.op === "patch-presence") {
        if (typeof room !== "string" || (st.leftRooms.has(room) && !st.joining.has(room))) continue;
        st.kinds.patch++;
        const peers = (st.rooms[room] ??= {});
        for (const edit of m.edits ?? []) applyEdit(peers, edit, label);
      }
    }
  };
  const foldAll = () => ROLES.forEach((r) => fold(r.name));

  const callAdmin = async (op) => {
    const params = new URLSearchParams();
    if (op.roomType !== null) params.set("room-type", op.roomType);
    if (op.roomId !== null) params.set("room-id", op.roomId);
    const res = await fetch(`${url}/admin/rooms/presence?${params}`, {
      headers: { "app-id": appId, authorization: `Bearer ${adminToken}` },
    });
    const body = await res.json().catch(() => null);
    if (!res.ok) {
      return {
        kind: "admin-error",
        status: res.status,
        keys: body && typeof body === "object" ? Object.keys(body).sort() : [],
        type: body?.type ?? null,
        message: body?.message ?? body?.error ?? null,
        hint: norm(body?.hint ?? null),
      };
    }
    const sessions = Object.entries(body?.sessions ?? {})
      .map(([sid, entry]) => ({
        peer: label(sid),
        keys: Object.keys(entry).sort(),
        instance: entry["instance-id"] === null ? null : "<instance>",
        instanceType: typeName(entry["instance-id"]),
        user: adminUserView(entry.user),
        data: norm(entry.data),
      }))
      .sort((a, b) => (a.peer < b.peer ? -1 : 1));
    return { kind: "admin-ok", keys: Object.keys(body ?? {}).sort(), sessions };
  };

  const broadcastDeliveries = (room, before) => {
    const out = {};
    for (const role of ROLES) {
      const c = conns[role.name];
      out[role.name] = c.frames
        .slice(before[role.name])
        .filter((m) => m.op === "server-broadcast" && m["room-id"] === room)
        .map((m) => ({
          kind: "server-broadcast",
          keys: Object.keys(m).sort(),
          topic: m.topic,
          peer: label(m.data?.["peer-id"]),
          user: norm(m.data?.user ?? null),
          data: norm(m.data?.data),
        }));
    }
    return out;
  };

  const waitReply = (role, ceid, timeout = 15000) =>
    conns[role].waitFor(
      (m) => m["client-event-id"] === ceid && (m.op === "error" || m.op !== "error"),
      timeout,
    );

  const outcomes = [];
  for (const op of plan.script) {
    if (op.kind === "send") {
      const isBroadcast = op.msg.op === "client-broadcast";
      const isJoin = op.msg.op === "join-room" && typeof op.msg["room-id"] === "string";
      if (isBroadcast) await settle(Object.values(conns), SETTLE_MS);
      const before = Object.fromEntries(ROLES.map((r) => [r.name, conns[r.name].frames.length]));
      const ceid = op.ceid;
      const joinRoom = isJoin ? op.msg["room-id"] : null;
      const joinFrameMark = isJoin ? conns[op.session].frames.length : 0;
      // a re-join snapshot may reach the client before join-room-ok; apply it
      // just like Reactor does, but keep ignoring stragglers after a leave
      if (isJoin) states[op.session].joining.add(joinRoom);
      conns[op.session].send({ "client-event-id": ceid, ...op.msg });
      const reply = await waitReply(op.session, ceid);
      if (isJoin) {
        fold(op.session);
        if (op.newJoin && reply.op === "join-room-ok") {
          // Wait for the joiner's full refresh-presence before sending another
          // op. Legacy's grouped receive queue combines a just-joined full
          // snapshot (edits nil) with a later patch by keeping only the patch
          // (grouped_queue.clj:37-52, session.clj:1526-1528); a modern client
          // that applies that patch against no snapshot then misses every peer
          // that did not change between the two events. Rust always sends the
          // joiner a full snapshot; waiting here keeps the fuzz from making
          // legacy's lost-snapshot defect the baseline (docs/PARITY.md).
          const c = conns[op.session];
          const isFullSnapshot = (m) =>
            m.op === "refresh-presence" && m["room-id"] === joinRoom;
          if (!c.frames.slice(joinFrameMark).some(isFullSnapshot)) {
            await c.waitFor(isFullSnapshot);
          }
          fold(op.session);
        }
        states[op.session].joining.delete(joinRoom);
      }
      if (isBroadcast) {
        await settle(Object.values(conns), BROADCAST_SETTLE_MS);
        outcomes.push({
          name: op.name,
          ...ackView(reply),
          deliveries: broadcastDeliveries(op.msg["room-id"], before),
        });
      } else {
        if (op.settle) await settle(Object.values(conns), SETTLE_MS);
        outcomes.push({ name: op.name, ...ackView(reply) });
      }
    } else if (op.kind === "burst") {
      for (const o of op.ops) {
        if (o.msg.op === "join-room" && typeof o.msg["room-id"] === "string") {
          states[o.session].joining.add(o.msg["room-id"]);
        }
      }
      const pending = op.ops.map((o) => {
        conns[o.session].send({ "client-event-id": o.ceid, ...o.msg });
        return waitReply(o.session, o.ceid);
      });
      const replies = await Promise.all(pending);
      for (const o of op.ops) {
        if (!states[o.session].joining.size) continue;
        fold(o.session);
        for (const room of states[o.session].joining) states[o.session].joining.delete(room);
      }
      await settle(Object.values(conns), SETTLE_MS);
      outcomes.push({
        name: op.name,
        kind: "burst",
        replies: replies.map((r, i) => ({ session: op.ops[i].session, ...ackView(r) })),
      });
    } else if (op.kind === "checkpoint") {
      await settle(Object.values(conns), SETTLE_MS);
      foldAll();
      outcomes.push({
        name: op.name,
        kind: "checkpoint",
        states: Object.fromEntries(ROLES.map((r) => [r.name, projectPresence(states[r.name])])),
      });
    } else if (op.kind === "admin") {
      await settle(Object.values(conns), SETTLE_MS);
      outcomes.push({ name: op.name, ...(await callAdmin(op)) });
    } else {
      throw new Error(`unknown plan op ${op.kind}`);
    }
  }
  await settle(Object.values(conns), SETTLE_MS);
  foldAll();
  const finalStates = Object.fromEntries(ROLES.map((r) => [r.name, projectPresence(states[r.name])]));
  const frameKinds = Object.fromEntries(ROLES.map((r) => [r.name, { ...states[r.name].kinds }]));
  for (const c of Object.values(conns)) c.close();
  return { outcomes, finalStates, frameKinds };
}

// ---------------------------------------------------------------------------
// run both servers and compare

const plan = buildPlan();
console.log(`fuzz-presence: seed=${seed} rounds=${rounds} ops=${plan.script.length}`);
console.log("running against legacy…");
const legacy = await runOn("legacy", plan);
console.log("running against rust…");
const rust = await runOn("rust", plan);

for (const server of [
  ["legacy", legacy],
  ["rust", rust],
]) {
  const [name, result] = server;
  const oldPatches = OLD_ROLES.map((r) => result.frameKinds[r].patch);
  if (oldPatches.some((n) => n > 0)) {
    throw new Error(`[${name}] old clients received patch-presence: ${JSON.stringify(result.frameKinds)}`);
  }
  const newPatches = ROLES.filter((r) => !OLD_ROLES.includes(r.name)).reduce(
    (n, r) => n + result.frameKinds[r.name].patch,
    0,
  );
  if (newPatches === 0) {
    throw new Error(`[${name}] no modern client received patch-presence: ${JSON.stringify(result.frameKinds)}`);
  }
}

const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), {
  prefixes: DIVERGENCE_PREFIXES.presence,
});
let mismatches = 0;
let allowed = 0;

const outcomePath = (op, i) => {
  if (!op) return `presence:outcome/${i}`;
  if (op.kind === "admin") return `presence:${op.name ?? `admin-${i}`}/admin`;
  if (op.kind === "checkpoint") return `presence:${op.name ?? `checkpoint-${i}`}`;
  if (op.kind === "burst") return `presence:${op.name ?? `burst-${i}`}`;
  return `presence:${op.name ?? `op-${i}`}/${op.session ?? ""}`;
};

function compare(p, legacyValue, rustValue) {
  if (canon(legacyValue) === canon(rustValue)) return;
  const verdict = allowlist.check(p, legacyValue, rustValue);
  if (verdict.entry) {
    allowed++;
    console.log(`[ALLOWED] ${p}: ${verdict.differed.join(", ") || "(whole value)"}`);
    return;
  }
  mismatches++;
  const fd = firstDifference(legacyValue, rustValue);
  console.error(`DIVERGENCE ${p}`);
  if (verdict.rejected.length) for (const line of describeAllowVerdict(verdict)) console.error(line);
  console.error(`  at ${fd?.p || "(whole value)"}:`);
  console.error(`    legacy ${JSON.stringify(fd?.a)?.slice(0, 1200)}`);
  console.error(`    rust   ${JSON.stringify(fd?.b)?.slice(0, 1200)}`);
}

const n = Math.max(legacy.outcomes.length, rust.outcomes.length);
for (let i = 0; i < n; i++) {
  const l = legacy.outcomes[i];
  const r = rust.outcomes[i];
  const op = plan.script[i];
  if (l?.name !== r?.name) {
    mismatches++;
    console.error(`OUTCOME NAME MISMATCH at ${i}: legacy ${JSON.stringify(l?.name)} rust ${JSON.stringify(r?.name)}`);
    continue;
  }
  compare(outcomePath(op, i), l, r);
}
for (const role of ROLES) {
  compare(`presence:final/${role.name}`, legacy.finalStates[role.name], rust.finalStates[role.name]);
}

const checkpoints = legacy.outcomes.filter((o) => o.kind === "checkpoint").length;
const broadcasts = plan.script.filter((o) => o.kind === "send" && o.msg.op === "client-broadcast").length;
const adminReads = plan.script.filter((o) => o.kind === "admin").length;
const stale = allowlist.reportStale();
if (mismatches || stale) {
  console.error(`FUZZ-PRESENCE FAILED: ${mismatches} mismatches, ${stale} stale allowlist entries (seed ${seed})`);
  process.exit(1);
}
console.log(
  `FUZZ-PRESENCE PASSED: ${plan.script.length} ops, ${checkpoints} checkpoints, ${broadcasts} broadcasts, ${adminReads} admin reads, ${allowed} allowed divergences, old clients stayed on full snapshots (seed ${seed})`,
);
console.log(`  frame kinds: legacy ${JSON.stringify(legacy.frameKinds)} rust ${JSON.stringify(rust.frameKinds)}`);
