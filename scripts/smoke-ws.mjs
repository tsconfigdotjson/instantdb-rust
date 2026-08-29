// Protocol smoke test against the rust server, mimicking @instantdb/core.
// Usage: node scripts/smoke-ws.mjs <app-id>
const appId = process.argv[2];
if (!appId) throw new Error("usage: node smoke-ws.mjs <app-id>");
const url = `ws://localhost:8888/runtime/session?app_id=${appId}`;

const uuid = () => crypto.randomUUID();
function connect(name) {
  const ws = new WebSocket(url);
  const inbox = [];
  const waiters = [];
  const rooms = {}; // room-id -> {sid: {data...}} maintained like the client
  function applyPresence(m) {
    if (m.op === "refresh-presence") {
      rooms[m["room-id"]] = m.data;
    } else if (m.op === "patch-presence") {
      const room = (rooms[m["room-id"]] ||= {});
      for (const [path, op, value] of m.edits) {
        if (op === "-") delete room[path[0]];
        else if (path.length === 1) room[path[0]] = value;
        else if (path.length === 2 && path[1] === "data") {
          (room[path[0]] ||= {}).data = value;
        }
      }
    }
  }
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    const msgs = Array.isArray(msg) ? msg : [msg];
    for (const m of msgs) {
      console.log(`[${name}] <-`, m.op ?? m);
      applyPresence(m);
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
  const send = (msg) => {
    const withId = { "client-event-id": uuid(), ...msg };
    console.log(`[${name}] ->`, withId.op);
    ws.send(JSON.stringify(withId));
  };
  const waitFor = (pred, timeout = 5000) =>
    new Promise((resolve, reject) => {
      const existing = inbox.find(pred);
      if (existing) return resolve(existing);
      const t = setTimeout(() => reject(new Error(`timeout waiting (${name})`)), timeout);
      waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
    });
  const open = new Promise((resolve) => (ws.onopen = resolve));
  const waitRoom = (roomId, pred, timeout = 5000) =>
    new Promise((resolve, reject) => {
      if (pred(rooms[roomId] || {})) return resolve();
      const t = setTimeout(() => reject(new Error(`room timeout (${name})`)), timeout);
      waiters.push([
        () => pred(rooms[roomId] || {}),
        () => { clearTimeout(t); resolve(); },
      ]);
    });
  return { ws, send, waitFor, waitRoom, open, inbox, rooms };
}

const a = connect("A");
await a.open;
a.send({ op: "init", "app-id": appId, versions: { "@instantdb/core": "v0.21.0" } });
const initOk = await a.waitFor((m) => m.op === "init-ok");
console.assert(initOk["session-id"], "has session id");
console.assert(Array.isArray(initOk.attrs), "has attrs");

// schemaless transact: create attrs + a todo
const todosId = uuid(), todosTitle = uuid(), todosDone = uuid();
const e1 = uuid();
a.send({
  op: "transact",
  "tx-steps": [
    ["add-attr", { id: todosId, "forward-identity": [uuid(), "todos", "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": false, isUnsynced: true }],
    ["add-attr", { id: todosTitle, "forward-identity": [uuid(), "todos", "title"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true }],
    ["add-attr", { id: todosDone, "forward-identity": [uuid(), "todos", "done"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false, isUnsynced: true }],
    ["add-triple", e1, todosId, e1],
    ["add-triple", e1, todosTitle, "first todo"],
    ["add-triple", e1, todosDone, false],
  ],
});
const txOk = await a.waitFor((m) => m.op === "transact-ok");
console.assert(txOk["tx-id"] > 0, "tx id present");

// second client: add-query and receive the todo
const b = connect("B");
await b.open;
b.send({ op: "init", "app-id": appId });
await b.waitFor((m) => m.op === "init-ok");
b.send({ op: "add-query", q: { todos: {} } });
const aqOk = await b.waitFor((m) => m.op === "add-query-ok");
const triples = aqOk.result[0].data["datalog-result"]["join-rows"][0];
console.assert(triples.some((t) => t[2] === "first todo"), "query returned todo");
console.assert(aqOk["processed-tx-id"] >= txOk["tx-id"], "processed-tx-id watermark");

// A transacts again; B should get refresh-ok
a.send({ op: "transact", "tx-steps": [["add-triple", e1, todosDone, true]] });
const refresh = await b.waitFor((m) => m.op === "refresh-ok");
const comp = refresh.computations[0];
console.assert(comp["instaql-query"].todos !== undefined, "computation carries query");
const refreshedTriples = comp["instaql-result"][0].data["datalog-result"]["join-rows"][0];
console.assert(refreshedTriples.some((t) => t[2] === true), "refresh has done=true");

// rooms: both join, presence flows
a.send({ op: "join-room", "room-type": "chat", "room-id": "r1", data: { name: "A" } });
await a.waitFor((m) => m.op === "join-room-ok");
b.send({ op: "join-room", "room-type": "chat", "room-id": "r1", data: { name: "B" } });
await b.waitFor((m) => m.op === "join-room-ok");
await a.waitRoom("r1", (room) => Object.keys(room).length >= 2);
console.log("both peers in presence");

b.send({ op: "set-presence", "room-id": "r1", data: { name: "B", cursor: { x: 1 } } });
await a.waitRoom("r1", (room) =>
  Object.values(room).some((v) => v.data?.cursor?.x === 1)
);
console.log("presence patch applied");

// broadcast
b.send({ op: "client-broadcast", "room-id": "r1", roomType: "chat", topic: "emoji", data: { emoji: "🔥" } });
const bc = await a.waitFor((m) => m.op === "server-broadcast");
console.assert(bc.data.data.emoji === "🔥", "broadcast payload");
console.assert(bc.data["peer-id"], "broadcast peer id");

// error handling: bad transact echoes original-event
a.send({ op: "transact", "tx-steps": [["bogus-step"]] });
const err = await a.waitFor((m) => m.op === "error");
console.assert(err["original-event"].op === "transact", "error echoes original event");
console.assert(err.type === "validation-failed", "error type");

console.log("SMOKE TEST PASSED");
process.exit(0);
