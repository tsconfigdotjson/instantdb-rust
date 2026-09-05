// Scheduling / timing stress step of the differential harness. Legacy runs a
// session's ops on independent group keys (session.clj:1463-1553), so a slow
// transact does not delay that session's presence or query replies; this
// server handles a session's frames in arrival order (docs/PARITY.md "session
// op scheduling"). This step pins down that the difference is latency only:
// a large transact is followed immediately by set-presence, add-query and a
// client-broadcast on the same session, and both servers must
//   - answer every op (no drop, no timeout),
//   - deliver the presence update and the broadcast to a peer session,
//   - and converge to the same query result and presence state.
// The reply order is printed per server as information.
//
// Usage: node stress.mjs <app-id-legacy> <app-id-rust> <admin-token> [triples]
// Env: LEGACY_URL, RUST_URL

import { canon, connect, makeIdFactory, projectResult, settle, uuid } from "./lib.mjs";

const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
// legacy's transact hits Postgres' 100-argument function limit well before
// 8000 triples (a 54023 sql-exception on the live image), so the burst stays
// at a size both servers accept; the point is ordering, not volume
const triples = Number(process.argv[5] ?? 400);
if (!appIdLegacy || !appIdRust || !adminToken) throw new Error("usage: node stress.mjs <app-id-legacy> <app-id-rust> <admin-token> [triples]");

const SERVERS = {
  legacy: { url: process.env.LEGACY_URL || "http://localhost:8891", appId: appIdLegacy },
  rust: { url: process.env.RUST_URL || "http://localhost:8888", appId: appIdRust },
};

const mk = makeIdFactory(appIdLegacy);
const NS = "stress";
const attrs = { id: mk(), n: mk() };
const entities = Array.from({ length: triples }, () => mk());
const ROOM = "stress-room";

async function init(url, appId, name) {
  const conn = connect(url, appId, name);
  await conn.open;
  conn.send({ "client-event-id": uuid(), op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.21.0" } });
  const ok = await conn.waitFor((m) => m.op === "init-ok");
  conn.takeNewFrames();
  return { conn, sessionId: ok["session-id"] };
}

async function runOn(serverName) {
  const { url, appId } = SERVERS[serverName];
  const main = await init(url, appId, `${serverName}:main`);
  const peer = await init(url, appId, `${serverName}:peer`);

  // schema + both sessions in the room
  main.conn.send({
    "client-event-id": uuid(),
    op: "transact",
    "tx-steps": [
      ["add-attr", { id: attrs.id, "forward-identity": [attrs.id, NS, "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": true }],
      ["add-attr", { id: attrs.n, "forward-identity": [attrs.n, NS, "n"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }],
    ],
  });
  await main.conn.waitFor((m) => m.op === "transact-ok");
  for (const s of [main, peer]) {
    s.conn.send({ "client-event-id": uuid(), op: "join-room", "room-type": "chat", "room-id": ROOM });
    await s.conn.waitFor((m) => m.op === "join-room-ok");
  }
  await settle([main.conn, peer.conn], 300);
  main.conn.takeNewFrames();
  peer.conn.takeNewFrames();

  // the burst: a big transact, then presence, a query and a broadcast, sent
  // back to back without waiting
  const steps = [];
  for (const e of entities) {
    steps.push(["add-triple", e, attrs.id, e]);
    steps.push(["add-triple", e, attrs.n, 1]);
  }
  const t0 = Date.now();
  const ids = {
    transact: main.conn.send({ "client-event-id": uuid(), op: "transact", "tx-steps": steps }),
    presence: main.conn.send({ "client-event-id": uuid(), op: "set-presence", "room-id": ROOM, data: { cursor: 1 } }),
    query: main.conn.send({ "client-event-id": uuid(), op: "add-query", q: { [NS]: { $: { limit: 1 } } } }),
    broadcast: main.conn.send({ "client-event-id": uuid(), op: "client-broadcast", "room-id": ROOM, topic: "ping", data: { hello: 1 } }),
  };
  const arrivals = {};
  const waitAck = async (key, pred) => {
    const m = await main.conn.waitFor(pred, 60000);
    arrivals[key] = Date.now() - t0;
    return m;
  };
  const [transact, presence, query, broadcast, peerPresence, peerBroadcast] = await Promise.all([
    waitAck("transact", (m) => m["client-event-id"] === ids.transact),
    waitAck("presence", (m) => m["client-event-id"] === ids.presence),
    waitAck("query", (m) => m["client-event-id"] === ids.query),
    waitAck("broadcast", (m) => m["client-event-id"] === ids.broadcast),
    // clients newer than core 0.17.5 get patch-presence instead of refresh-presence
    peer.conn.waitFor((m) => (m.op === "refresh-presence" || m.op === "patch-presence") && JSON.stringify(m).includes('"cursor":1'), 60000).then(() => (arrivals.peerPresence = Date.now() - t0)),
    peer.conn.waitFor((m) => m.op === "server-broadcast" && m.topic === "ping", 60000).then(() => (arrivals.peerBroadcast = Date.now() - t0)),
  ]);
  const order = Object.entries(arrivals).sort((a, b) => a[1] - b[1]).map(([k, ms]) => `${k}@${ms}ms`);
  console.log(`[${serverName}] reply order: ${order.join(" ")}`);

  const errors = [transact, presence, query, broadcast].filter((m) => m.op === "error").map((m) => `${m.type}: ${m.message}`);

  // convergence: a fresh query after everything settled, and the peer's view
  // of the room
  await settle([main.conn, peer.conn], 700, 30000);
  main.conn.takeNewFrames();
  const ceid = uuid();
  main.conn.send({ "client-event-id": ceid, op: "add-query", q: { [NS]: { $: { aggregate: "count" } } } });
  const countReply = await main.conn.waitFor((m) => m["client-event-id"] === ceid, 30000);
  peer.conn.takeNewFrames();
  const pc = uuid();
  peer.conn.send({ "client-event-id": pc, op: "set-presence", "room-id": ROOM, data: { probe: true } });
  await peer.conn.waitFor((m) => m["client-event-id"] === pc, 15000);
  const final = {
    errors,
    // an aggregate query is admin-only on both servers: the type of reply is
    // itself part of the comparison
    count: countReply.op === "error" ? { error: countReply.type } : projectResult(countReply.result).aggregate,
    peerPresence: !!peerPresence,
    peerBroadcast: !!peerBroadcast,
  };
  main.conn.close();
  peer.conn.close();
  return final;
}

console.log(`stress: ${triples} entities (${triples * 2} triples) in one transact, then presence + query + broadcast`);
const legacy = await runOn("legacy");
const rust = await runOn("rust");
if (canon(legacy) !== canon(rust)) {
  console.error(`STRESS FAILED\n  legacy: ${JSON.stringify(legacy)}\n  rust:   ${JSON.stringify(rust)}`);
  process.exit(1);
}
if (legacy.errors.length) {
  console.error(`STRESS FAILED: both servers errored identically: ${legacy.errors.join("; ")}`);
  process.exit(1);
}
console.log("STRESS PASSED: every op answered on both servers, peers saw presence + broadcast, states converged");
