// Webhook delivery e2e against this server: a local receiver gets the signed
// POST, verifies `Instant-Signature` with the served JWK set, fetches the
// payload with the delivered token, and the dashboard routes show the event
// succeeding. Then a 410 from the receiver disables the webhook, and a
// resend requeues an event.
//
// Usage: node scripts/webhooks-test.mjs <app-id> <admin-token>
// Env: SERVER_URL (default http://localhost:8888), RECEIVER_PORT (9911).
//      The server must run with INSTANT_WEBHOOK_ALLOW_INSECURE=1 so an
//      http://localhost receiver passes the url validation.

import http from "node:http";
import { createPublicKey, verify as cryptoVerify } from "node:crypto";

const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken) throw new Error("usage: node webhooks-test.mjs <app-id> <admin-token>");
const base = process.env.SERVER_URL || "http://localhost:8888";
const port = Number(process.env.RECEIVER_PORT || 9911);

let failures = 0;
const check = (name, ok, detail) => {
  console.log(`${ok ? "ok  " : "FAIL"} ${name}${ok ? "" : ` ${JSON.stringify(detail)?.slice(0, 400)}`}`);
  if (!ok) failures++;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function call(method, p, body, token = adminToken) {
  const res = await fetch(base + p, {
    method,
    headers: { Authorization: `Bearer ${token}`, "app-id": appId, "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = text; }
  return { status: res.status, body: json };
}

// the receiver: records every delivery; `mode` picks the reply
const received = [];
let mode = "ok";
const server = http.createServer((req, res) => {
  let data = "";
  req.on("data", (c) => (data += c));
  req.on("end", () => {
    received.push({ headers: req.headers, body: data });
    if (mode === "gone") {
      res.writeHead(410);
      res.end("gone");
    } else {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    }
  });
});
await new Promise((r) => server.listen(port, "127.0.0.1", r));
const receiverUrl = `http://localhost:${port}/hook`;

// schema: a namespace to hook
const seed = await call("POST", "/admin/transact", { steps: [["update", "orders", crypto.randomUUID(), { total: 1 }]] });
check("seed namespace", seed.status === 200, seed);

// jwks
const jwks = await (await fetch(base + "/.well-known/webhooks/jwks.json")).json();
check("jwks shape", jwks.keys?.[0]?.kty === "OKP" && jwks.keys[0].crv === "Ed25519" && typeof jwks.keys[0].kid === "string" && typeof jwks.keys[0].x === "string", jwks);
const keyObject = createPublicKey({ key: { kty: "OKP", crv: "Ed25519", x: jwks.keys[0].x }, format: "jwk" });

// create
const created = await call("POST", `/dash/apps/${appId}/webhooks`, { url: receiverUrl, namespaces: ["orders"], actions: ["create", "update", "delete"] });
check("create webhook", created.status === 200 && created.body.webhook?.status === "active" && created.body.webhook.namespaces?.[0] === "orders", created);
const webhookId = created.body.webhook?.id;
check("create requires https unless insecure allowed", (await call("POST", `/dash/apps/${appId}/webhooks`, { url: "ftp://x", namespaces: ["orders"], actions: ["create"] })).status === 400);
check("unknown namespace rejected", (await call("POST", `/dash/apps/${appId}/webhooks`, { url: receiverUrl, namespaces: ["nope"], actions: ["create"] })).body.message?.includes("Could not find matching table"));
check("duplicate rejected", (await call("POST", `/dash/apps/${appId}/webhooks`, { url: receiverUrl, namespaces: ["orders"], actions: ["create", "update", "delete"] })).body.message?.includes("already exists"));

// a transaction → delivery
const orderId = crypto.randomUUID();
const tx = await call("POST", "/admin/transact", { steps: [["update", "orders", orderId, { total: 42, note: "first" }]] });
check("transact", tx.status === 200, tx);
for (let i = 0; i < 100 && received.length === 0; i++) await sleep(100);
check("receiver got a delivery", received.length >= 1, received.length);
const first = received[0];
if (first) {
  const sig = Object.fromEntries((first.headers["instant-signature"] ?? "").split(",").map((kv) => kv.split("=")));
  check("signature header parts", sig.t && sig.kid === jwks.keys[0].kid && /^[0-9a-f]{128}$/.test(sig.v1), first.headers["instant-signature"]);
  const okSig = cryptoVerify(null, Buffer.from(`${sig.t}.${first.body}`), keyObject, Buffer.from(sig.v1, "hex"));
  check("signature verifies with the JWK", okSig);
  check("idempotency key header is a uuid", /^[0-9a-f-]{36}$/.test(first.headers["idempotency-key"] ?? ""), first.headers);
  check("user agent", first.headers["user-agent"] === "InstantDB Webhook Sender", first.headers["user-agent"]);
  const body = JSON.parse(first.body);
  check("body shape", typeof body.payloadUrl === "string" && typeof body.token === "string", body);
  const payloadRes = await fetch(body.payloadUrl, { headers: { Authorization: `Bearer ${body.token}` } });
  const payload = await payloadRes.json();
  check("payload with the delivered token", payloadRes.status === 200 && Array.isArray(payload.data), payload);
  const rec = payload.data?.find((r) => r.id === orderId);
  check("payload record", rec && rec.namespace === "orders" && rec.action === "create" && rec.before === null && rec.after?.total === 42 && typeof rec.idempotencyKey === "string", payload.data);
  check("payload idempotency key matches the header", payload.idempotencyKey === first.headers["idempotency-key"], [payload.idempotencyKey, first.headers["idempotency-key"]]);
  check("payload with the admin token", (await fetch(body.payloadUrl, { headers: { Authorization: `Bearer ${adminToken}` } })).status === 200);
  check("payload with a bad token", (await fetch(body.payloadUrl, { headers: { Authorization: "Bearer eyJ.nope.x" } })).status === 400);
  // the CORS layer appends its own Vary members, so only the Authorization member is pinned
  check("payload cache headers", payloadRes.headers.get("cache-control") === "no-store, private" && payloadRes.headers.get("pragma") === "no-cache" && (payloadRes.headers.get("vary") ?? "").split(",").map((s) => s.trim()).includes("Authorization"), Object.fromEntries(payloadRes.headers));
}

// update event: before/after
received.length = 0;
await call("POST", "/admin/transact", { steps: [["update", "orders", orderId, { total: 43 }]] });
for (let i = 0; i < 100 && received.length === 0; i++) await sleep(100);
if (received[0]) {
  const body = JSON.parse(received[0].body);
  const payload = await (await fetch(body.payloadUrl, { headers: { Authorization: `Bearer ${body.token}` } })).json();
  const rec = payload.data?.find((r) => r.id === orderId);
  check("update record has before/after", rec && rec.action === "update" && rec.before?.total === 42 && rec.after?.total === 43, payload.data);
}

// delete event
received.length = 0;
await call("POST", "/admin/transact", { steps: [["delete", "orders", orderId]] });
for (let i = 0; i < 100 && received.length === 0; i++) await sleep(100);
if (received[0]) {
  const body = JSON.parse(received[0].body);
  const payload = await (await fetch(body.payloadUrl, { headers: { Authorization: `Bearer ${body.token}` } })).json();
  const rec = payload.data?.find((r) => r.id === orderId);
  check("delete record", rec && rec.action === "delete" && rec.after === null && rec.before?.total === 43, payload.data);
}

// events list shows successes
await sleep(300);
const events = await call("GET", `/dash/apps/${appId}/webhooks/${webhookId}/events`);
check("events listed", events.status === 200 && events.body.events?.length >= 3 && events.body.events.every((e) => e.status === "success"), events.body);
const ev = events.body.events?.[0];
check("event shape", ev && typeof ev.isn === "string" && Array.isArray(ev.attempts) && ev.attempts[0]?.["success?"] === true && ev.attempts[0]["status-code"] === 200, ev);
const one = await call("GET", `/dash/apps/${appId}/webhooks/${webhookId}/events/${ev?.isn}`);
check("event by isn", one.status === 200 && one.body.event?.isn === ev?.isn, one);
check("unknown event", (await call("GET", `/dash/apps/${appId}/webhooks/${webhookId}/events/0/0/1`)).body.type === "record-not-found");
received.length = 0;
const resent = await call("POST", `/dash/apps/${appId}/webhooks/${webhookId}/events/${ev?.isn}`);
check("resend requeues", resent.status === 200 && resent.body.event?.status === "pending", resent);
for (let i = 0; i < 100 && received.length === 0; i++) await sleep(100);
check("resend delivered again", received.length >= 1);

// 410 disables the webhook
mode = "gone";
received.length = 0;
await call("POST", "/admin/transact", { steps: [["update", "orders", crypto.randomUUID(), { total: 7 }]] });
for (let i = 0; i < 100 && received.length === 0; i++) await sleep(100);
await sleep(500);
const after410 = await call("GET", `/dash/apps/${appId}/webhooks`);
const hook = after410.body.webhooks?.find((w) => w.id === webhookId);
check("410 disables the webhook", hook?.status === "disabled" && hook?.disabled_reason === "Endpoint returned 410 status code.", hook);
const failed = await call("GET", `/dash/apps/${appId}/webhooks/${webhookId}/events`);
check("410 event is failed", failed.body.events?.some((e) => e.status === "failed" && e.attempts?.[0]?.["status-code"] === 410), failed.body.events?.map((e) => e.status));
mode = "ok";
check("enable", (await call("POST", `/dash/apps/${appId}/webhooks/${webhookId}/enable`)).body.webhook?.status === "active");
check("disable with reason", (await call("POST", `/dash/apps/${appId}/webhooks/${webhookId}/disable`, { reason: "paused" })).body.webhook?.disabled_reason === "paused");
check("update url", (await call("POST", `/dash/apps/${appId}/webhooks/${webhookId}`, { url: receiverUrl + "2" })).body.webhook?.sink?.url === receiverUrl + "2");
check("delete", (await call("DELETE", `/dash/apps/${appId}/webhooks/${webhookId}`)).body.webhook?.id === webhookId);
check("gone after delete", !(await call("GET", `/dash/apps/${appId}/webhooks`)).body.webhooks?.some((w) => w.id === webhookId));
// legacy's webhook_events has no foreign key to webhooks (migration 109:
// "deletes only happen through truncate"), so the events outlive the webhook
// and the events route, which never checks the webhook, still pages them;
// resend requeues one as well
const goneEvents = await call("GET", `/dash/apps/${appId}/webhooks/${webhookId}/events`);
check("events outlive the deleted webhook", goneEvents.status === 200 && goneEvents.body.events?.length >= 3, goneEvents);
check("resend on a deleted webhook still requeues", (await call("POST", `/dash/apps/${appId}/webhooks/${webhookId}/events/${ev?.isn}`)).body.event?.status === "pending");

server.close();
if (failures) {
  console.error(`WEBHOOKS TEST FAILED: ${failures} checks`);
  process.exit(1);
}
console.log("WEBHOOKS TEST PASSED");
