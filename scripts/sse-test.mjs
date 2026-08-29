// SSE fallback transport test: open the stream, init, add-query, transact
// over HTTP POSTs, receive refresh-ok as SSE events.
const appId = process.argv[2];
if (!appId) throw new Error("usage: node sse-test.mjs <app-id>");
const BASE = "http://localhost:8888";
const uuid = () => crypto.randomUUID();
const assert = (c, m) => {
  if (!c) throw new Error("ASSERT FAILED: " + m);
  console.log("ok:", m);
};

const inbox = [];
const waiters = [];
function deliver(m) {
  console.log("<-", m.op);
  inbox.push(m);
  for (let i = waiters.length - 1; i >= 0; i--) {
    const [pred, resolve] = waiters[i];
    if (pred(m)) {
      waiters.splice(i, 1);
      resolve(m);
    }
  }
}
const waitFor = (pred, timeout = 8000) =>
  new Promise((resolve, reject) => {
    const existing = inbox.find(pred);
    if (existing) return resolve(existing);
    const t = setTimeout(() => reject(new Error("timeout")), timeout);
    waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
  });

// open SSE stream
const res = await fetch(`${BASE}/runtime/sse?app_id=${appId}`, {
  headers: { accept: "text/event-stream" },
});
assert(res.ok, "sse stream opens");
const reader = res.body.getReader();
const decoder = new TextDecoder();
let buf = "";
(async () => {
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf("\n\n")) >= 0) {
      const chunk = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      for (const line of chunk.split("\n")) {
        if (line.startsWith("data:")) {
          try {
            const parsed = JSON.parse(line.slice(5).trim());
            for (const m of Array.isArray(parsed) ? parsed : [parsed]) deliver(m);
          } catch {}
        }
      }
    }
  }
})();

const init = await waitFor((m) => m.op === "sse-init");
assert(init["sse-token"] && init["session-id"], "sse-init has token + session");

const envelope = {
  machine_id: init["machine-id"],
  session_id: init["session-id"],
  sse_token: init["sse-token"],
};
const push = (messages) =>
  fetch(`${BASE}/runtime/sse?app_id=${appId}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ...envelope, messages }),
  });

await push([{ "client-event-id": uuid(), op: "init", "app-id": appId }]);
await waitFor((m) => m.op === "init-ok");
console.log("ok: init over sse");

await push([{ "client-event-id": uuid(), op: "add-query", q: { sseitems: {} } }]);
await waitFor((m) => m.op === "add-query-ok");
console.log("ok: add-query over sse");

const attrId = uuid(), nameAttr = uuid(), e1 = uuid();
await push([
  {
    "client-event-id": uuid(),
    op: "transact",
    "tx-steps": [
      ["add-attr", { id: attrId, "forward-identity": [uuid(), "sseitems", "id"], "value-type": "blob", cardinality: "one", "unique?": true, "index?": false }],
      ["add-attr", { id: nameAttr, "forward-identity": [uuid(), "sseitems", "name"], "value-type": "blob", cardinality: "one", "unique?": false, "index?": false }],
      ["add-triple", e1, attrId, e1],
      ["add-triple", e1, nameAttr, "over sse"],
    ],
  },
]);
await waitFor((m) => m.op === "transact-ok");
const refresh = await waitFor((m) => m.op === "refresh-ok");
const triples = refresh.computations[0]["instaql-result"][0].data["datalog-result"]["join-rows"][0];
assert(triples.some((t) => t[2] === "over sse"), "refresh-ok over sse");

// bad token rejected
const bad = await fetch(`${BASE}/runtime/sse?app_id=${appId}`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ ...envelope, sse_token: uuid(), messages: [] }),
});
assert(bad.status === 400, "bad sse token rejected");

console.log("SSE TEST PASSED");
process.exit(0);
