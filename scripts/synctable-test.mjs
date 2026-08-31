// Sync-table protocol test: start-sync / load batches / incremental updates /
// resync / remove-sync. Usage: node synctable-test.mjs <app-id> <admin-token>
// (start-sync is admin-only, matching legacy session.clj:281-284)
const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken)
  throw new Error("usage: node synctable-test.mjs <app-id> <admin-token>");
const uuid = () => crypto.randomUUID();
const assert = (c, m) => {
  if (!c) throw new Error("ASSERT FAILED: " + m);
  console.log("ok:", m);
};

function connect(name) {
  const ws = new WebSocket(`ws://localhost:8888/runtime/session?app_id=${appId}`);
  const inbox = [];
  const waiters = [];
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    for (const m of Array.isArray(msg) ? msg : [msg]) {
      console.log(`[${name}] <-`, m.op);
      inbox.push(m);
      for (let i = waiters.length - 1; i >= 0; i--) {
        const [pred, resolve] = waiters[i];
        if (pred(m)) {
          waiters.splice(i, 1);
          resolve(m);
        }
      }
    }
  };
  const send = (msg) => ws.send(JSON.stringify({ "client-event-id": uuid(), ...msg }));
  const waitFor = (pred, timeout = 8000) =>
    new Promise((resolve, reject) => {
      const existing = inbox.find(pred);
      if (existing) return resolve(existing);
      const t = setTimeout(() => reject(new Error(`timeout (${name})`)), timeout);
      waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
    });
  return { ws, send, waitFor, open: new Promise((r) => (ws.onopen = r)), inbox };
}

const a = connect("A");
await a.open;
a.send({ op: "init", "app-id": appId, "__admin-token": adminToken });
await a.waitFor((m) => m.op === "init-ok");

// seed schema + two rows
const idAttr = uuid(), nameAttr = uuid();
const e1 = uuid(), e2 = uuid();
a.send({
  op: "transact",
  "tx-steps": [
    ["add-attr", { id: idAttr, "forward-identity": [uuid(), "docs", "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": false }],
    ["add-attr", { id: nameAttr, "forward-identity": [uuid(), "docs", "name"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }],
    ["add-triple", e1, idAttr, e1],
    ["add-triple", e1, nameAttr, "doc one"],
    ["add-triple", e2, idAttr, e2],
    ["add-triple", e2, nameAttr, "doc two"],
  ],
});
await a.waitFor((m) => m.op === "transact-ok");

// start-sync
a.send({ op: "start-sync", q: { docs: {} } });
const startOk = await a.waitFor((m) => m.op === "start-sync-ok");
assert(startOk["subscription-id"] && startOk.token, "start-sync-ok has sub + token");
const subId = startOk["subscription-id"];
const batch = await a.waitFor((m) => m.op === "sync-load-batch");
assert(batch["join-rows"].length === 2, "load batch has both entities");
assert(
  batch["join-rows"].every((rows) => rows.some((t) => t[1] === idAttr)),
  "each entity carries its id triple",
);
const initFinish = await a.waitFor((m) => m.op === "sync-init-finish");
assert(initFinish["tx-id"] > 0, "init-finish tx watermark");

// incremental update: add a row
const e3 = uuid();
a.send({
  op: "transact",
  "tx-steps": [
    ["add-triple", e3, idAttr, e3],
    ["add-triple", e3, nameAttr, "doc three"],
  ],
});
await a.waitFor((m) => m.op === "transact-ok");
const upd = await a.waitFor((m) => m.op === "sync-update-triples");
assert(upd["subscription-id"] === subId, "update targets the sub");
const changes = upd.txes.flatMap((t) => t.changes);
assert(
  changes.some((c) => c.action === "added" && c.triple[2] === "doc three"),
  "added change captured",
);

// update a value -> removed + added
a.send({ op: "transact", "tx-steps": [["add-triple", e1, nameAttr, "doc one v2"]] });
await a.waitFor((m) => m.op === "transact-ok");
const upd2 = await a.waitFor(
  (m) =>
    m.op === "sync-update-triples" &&
    m.txes.some((t) => t.changes.some((c) => c.triple[2] === "doc one v2")),
);
const ch2 = upd2.txes.flatMap((t) => t.changes);
assert(ch2.some((c) => c.action === "removed" && c.triple[2] === "doc one"), "old value removed");
assert(ch2.some((c) => c.action === "added" && c.triple[2] === "doc one v2"), "new value added");

// delete -> removed changes
a.send({ op: "transact", "tx-steps": [["delete-entity", e2, "docs"]] });
await a.waitFor((m) => m.op === "transact-ok");
const upd3 = await a.waitFor(
  (m) =>
    m.op === "sync-update-triples" &&
    m.txes.some((t) => t.changes.some((c) => c.action === "removed" && c.triple[0] === e2)),
);
assert(upd3, "delete produces removed changes");
const lastTx = upd3.txes[upd3.txes.length - 1]["tx-id"];

// resync from a new session replays txes after the given watermark
const b = connect("B");
await b.open;
b.send({ op: "init", "app-id": appId, "__admin-token": adminToken });
await b.waitFor((m) => m.op === "init-ok");
b.send({
  op: "resync-table",
  "subscription-id": subId,
  "tx-id": initFinish["tx-id"],
  token: startOk.token,
});
const replay = await b.waitFor((m) => m.op === "sync-update-triples");
const replayChanges = replay.txes.flatMap((t) => t.changes);
assert(
  replayChanges.some((c) => c.action === "added" && c.triple[2] === "doc three"),
  "resync replays the backlog",
);
assert(replay.txes.every((t) => t["tx-id"] > initFinish["tx-id"]), "replay starts after watermark");

// bad token -> error, client restarts
b.send({ op: "resync-table", "subscription-id": subId, "tx-id": lastTx, token: uuid() });
const err = await b.waitFor(
  (m) => m.op === "error" && m["original-event"]?.op === "resync-table",
);
assert(err["original-event"]["subscription-id"] === subId, "resync error echoes sub id");

// remove-sync deletes the subscription (no reply, matching legacy); after it,
// new txes must not produce sync-update-triples for this sub anymore
a.send({ op: "remove-sync", "subscription-id": subId, "keep-subscription": false });
await new Promise((r) => setTimeout(r, 300));
const inboxLen = a.inbox.length;
const e4 = uuid();
a.send({ op: "transact", "tx-steps": [["add-triple", e4, idAttr, e4], ["add-triple", e4, nameAttr, "doc four"]] });
await a.waitFor((m) => m.op === "transact-ok" && a.inbox.indexOf(m) >= inboxLen);
await new Promise((r) => setTimeout(r, 500));
assert(
  !a.inbox.slice(inboxLen).some((m) => m.op === "sync-update-triples"),
  "no sync updates after remove-sync",
);
console.log("SYNC TABLE TEST PASSED");
process.exit(0);
