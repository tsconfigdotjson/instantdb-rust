// Multi-node test: client A on node :8888, client B on node :8889.
// Verifies cross-node invalidation, presence, and broadcast through Postgres.
const appId = process.argv[2];
if (!appId) throw new Error("usage: node multinode-ws.mjs <app-id>");

const uuid = () => crypto.randomUUID();
function connect(name, port) {
  const ws = new WebSocket(`ws://localhost:${port}/runtime/session?app_id=${appId}`);
  const inbox = [];
  const waiters = [];
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    for (const m of Array.isArray(msg) ? msg : [msg]) {
      console.log(`[${name}:${port}] <-`, m.op);
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
  return { ws, send, waitFor, open: new Promise((r) => (ws.onopen = r)) };
}

const a = connect("A", 8888);
const b = connect("B", 8889);
await Promise.all([a.open, b.open]);
a.send({ op: "init", "app-id": appId });
b.send({ op: "init", "app-id": appId });
await a.waitFor((m) => m.op === "init-ok");
await b.waitFor((m) => m.op === "init-ok");

// B subscribes on node 2
b.send({ op: "add-query", q: { items: {} } });
await b.waitFor((m) => m.op === "add-query-ok");

// A transacts on node 1
const itemsId = uuid(), itemsName = uuid();
const e1 = uuid();
a.send({
  op: "transact",
  "tx-steps": [
    ["add-attr", { id: itemsId, "forward-identity": [uuid(), "items", "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": false }],
    ["add-attr", { id: itemsName, "forward-identity": [uuid(), "items", "name"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }],
    ["add-triple", e1, itemsId, e1],
    ["add-triple", e1, itemsName, "cross-node item"],
  ],
});
await a.waitFor((m) => m.op === "transact-ok");

// B must receive refresh-ok through PG NOTIFY (different process!)
const refresh = await b.waitFor((m) => m.op === "refresh-ok");
const triples = refresh.computations[0]["instaql-result"][0].data["datalog-result"]["join-rows"][0];
console.assert(triples.some((t) => t[2] === "cross-node item"), "cross-node refresh carries data");

// presence across nodes
a.send({ op: "join-room", "room-type": "r", "room-id": "xnode", data: { who: "A" } });
await a.waitFor((m) => m.op === "join-room-ok");
b.send({ op: "join-room", "room-type": "r", "room-id": "xnode", data: { who: "B" } });
await b.waitFor((m) => m.op === "join-room-ok");
await a.waitFor(
  (m) => m.op === "refresh-presence" && Object.values(m.data).some((v) => v.data?.who === "B")
);
console.log("presence crossed nodes");

// broadcast across nodes
b.send({ op: "client-broadcast", "room-id": "xnode", roomType: "r", topic: "hi", data: { n: 42 } });
const bc = await a.waitFor((m) => m.op === "server-broadcast");
console.assert(bc.data.data.n === 42, "broadcast crossed nodes");

console.log("MULTINODE TEST PASSED");
process.exit(0);
