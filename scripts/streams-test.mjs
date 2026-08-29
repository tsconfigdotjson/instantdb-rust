// Streams protocol test: writer appends, reader tails live + catches up.
// Usage: node streams-test.mjs <app-id>
const appId = process.argv[2];
if (!appId) throw new Error("usage: node streams-test.mjs <app-id>");
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
  const send = (msg) => {
    const withId = { "client-event-id": uuid(), ...msg };
    ws.send(JSON.stringify(withId));
    return withId["client-event-id"];
  };
  const waitFor = (pred, timeout = 8000) =>
    new Promise((resolve, reject) => {
      const existing = inbox.find(pred);
      if (existing) return resolve(existing);
      const t = setTimeout(() => reject(new Error(`timeout (${name})`)), timeout);
      waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
    });
  return { ws, send, waitFor, open: new Promise((r) => (ws.onopen = r)) };
}

// $streams default-denies; open up perms for the test app
import { execSync } from "node:child_process";
execSync(
  `psql postgres://instant:instant@localhost:5432/instant -q -c "INSERT INTO rules (app_id, code) VALUES ('${appId}', '{\\"\\$streams\\": {\\"allow\\": {\\"create\\": \\"true\\", \\"view\\": \\"true\\"}}}'::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code"`,
);

const w = connect("W");
const r = connect("R");
await Promise.all([w.open, r.open]);
w.send({ op: "init", "app-id": appId });
r.send({ op: "init", "app-id": appId });
await w.waitFor((m) => m.op === "init-ok");
await r.waitFor((m) => m.op === "init-ok");

// writer starts a stream
const clientId = "stream-" + uuid();
const reconnectToken = uuid();
w.send({ op: "start-stream", "client-id": clientId, "reconnect-token": reconnectToken });
const startOk = await w.waitFor((m) => m.op === "start-stream-ok");
assert(startOk.offset === 0, "new stream starts at 0");
const streamId = startOk["stream-id"];

// append two chunks
w.send({ op: "append-stream", "stream-id": streamId, chunks: ["hello ", "world"], offset: 0, done: false });
const flushed = await w.waitFor((m) => m.op === "stream-flushed");
assert(flushed.offset === 11, "flushed offset counts bytes");

// reader subscribes from 0: gets catch-up content
const subEvent = r.send({ op: "subscribe-stream", "stream-id": streamId, offset: 0 });
const catchup = await r.waitFor((m) => m.op === "stream-append");
assert(catchup["client-event-id"] === subEvent, "append correlated to subscribe event");
assert(catchup.content === "hello world", "catch-up content");

// live append reaches the reader
w.send({ op: "append-stream", "stream-id": streamId, chunks: ["! more"], offset: 11, done: false });
await w.waitFor((m) => m.op === "stream-flushed" && m.offset === 17);
const live = await r.waitFor((m) => m.op === "stream-append" && m.content === "! more");
assert(live.offset === 11, "live append carries its offset");

// done closes the stream for readers
w.send({ op: "append-stream", "stream-id": streamId, chunks: [], offset: 17, done: true });
await w.waitFor((m) => m.op === "stream-flushed" && m.done === true);
await r.waitFor((m) => m.op === "stream-append" && m.done === true);
console.log("ok: done propagated");

// writer resume: same client-id + token resumes at the flushed offset
w.send({ op: "start-stream", "client-id": clientId, "reconnect-token": reconnectToken });
const resume = await w.waitFor((m) => m.op === "start-stream-ok" && m.offset === 17);
assert(resume["stream-id"] === streamId, "resume returns the same stream");

// wrong reconnect token is rejected
w.send({ op: "start-stream", "client-id": clientId, "reconnect-token": uuid() });
const err = await w.waitFor((m) => m.op === "error" && m["original-event"]?.op === "start-stream");
assert(err.type === "validation-failed", "bad reconnect token rejected");

// late subscriber catches up fully on a finished stream
const r2 = connect("R2");
await r2.open;
r2.send({ op: "init", "app-id": appId });
await r2.waitFor((m) => m.op === "init-ok");
r2.send({ op: "subscribe-stream", "client-id": clientId, offset: 0 });
const full = await r2.waitFor((m) => m.op === "stream-append");
assert(full.content === "hello world! more" && full.done === true, "late reader gets full content + done");

console.log("STREAMS TEST PASSED");
process.exit(0);
