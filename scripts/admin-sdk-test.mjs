// Validates the official @instantdb/admin SDK against the rust server.
// Usage: node scripts/admin-sdk-test.mjs <app-id> <admin-token>
import { init, id, tx, lookup } from "../LEGACY/client/packages/admin/dist/esm/index.js";

const [appId, adminToken] = process.argv.slice(2);
if (!appId || !adminToken) throw new Error("usage: node admin-sdk-test.mjs <app-id> <token>");

const db = init({
  appId,
  adminToken,
  apiURI: "http://localhost:8888",
});

const assert = (cond, msg) => {
  if (!cond) throw new Error("ASSERT FAILED: " + msg);
  console.log("ok:", msg);
};

// 1. transact via SDK tx builder
const gid = id();
const tid = id();
await db.transact([
  tx.goals[gid].update({ title: "sdk goal", level: 3 }),
  tx.sdktodos[tid].update({ title: "sdk todo", done: false }),
  tx.goals[gid].link({ sdktodos: tid }),
]);
console.log("ok: transact");

// 2. query with nesting
const res = await db.query({ goals: { sdktodos: {} } });
const goal = res.goals.find((g) => g.id === gid);
assert(goal, "goal returned");
assert(goal.title === "sdk goal", "goal title");
assert(goal.sdktodos.length === 1 && goal.sdktodos[0].title === "sdk todo", "nested todo");

// 3. where + order + limit
await db.transact([
  tx.goals[id()].update({ title: "g2", level: 1 }),
  tx.goals[id()].update({ title: "g3", level: 2 }),
]);
const res2 = await db.query({
  goals: { $: { limit: 2, order: { serverCreatedAt: "desc" } } },
});
assert(res2.goals.length === 2, "limit works");
// g2/g3 share a tx timestamp; desc-limit-2 must exclude the older "sdk goal"
assert(!res2.goals.some((g) => g.title === "sdk goal"), "desc order excludes oldest");

// 4. lookup refs
await db.transact([
  tx.profiles[lookup("handle", "sdk-user")].update({ nickname: "sdk" }),
]);
const res3 = await db.query({ profiles: { $: { where: { handle: "sdk-user" } } } });
assert(res3.profiles.length === 1 && res3.profiles[0].nickname === "sdk", "lookup upsert");

// 5. auth: createToken + verifyToken + getUser
const token = await db.auth.createToken("sdk-test@example.com");
assert(typeof token === "string" && token.length > 10, "createToken");
const user = await db.auth.verifyToken(token);
assert(user.email === "sdk-test@example.com", "verifyToken");
const fetched = await db.auth.getUser({ email: "sdk-test@example.com" });
assert(fetched.id === user.id, "getUser");

// 6. impersonation: user without perms rules can read (default allow)
const asUser = db.asUser({ token });
const res4 = await asUser.query({ goals: {} });
assert(res4.goals.length >= 3, "impersonated query");

// 7. delete entity
await db.transact([tx.goals[gid].delete()]);
const res5 = await db.query({ goals: {} });
assert(!res5.goals.find((g) => g.id === gid), "delete");

// 8. storage upload + $files query
const buf = Buffer.from("hello from the rust instant server");
const up = await db.storage.uploadFile("test/hello.txt", buf, { contentType: "text/plain" });
assert(up.data.id, "upload returns id");
const files = await db.query({ $files: {} });
assert(files.$files.length >= 1, "$files query");
const f = files.$files.find((f) => f.path === "test/hello.txt");
assert(f && f.url, "$files has url");
const fetched2 = await fetch(f.url).then((r) => r.text());
assert(fetched2 === "hello from the rust instant server", "file served via signed url");

// 9. signOut
await db.auth.signOut({ email: "sdk-test@example.com" });
try {
  await db.auth.verifyToken(token);
  assert(false, "token should be revoked");
} catch (e) {
  assert(true, "signOut revokes tokens");
}

// 10. subscribeQuery over SSE (/admin/subscribe-query): first payload is the
// current object tree with page-info, later payloads follow transacts
const withTimeout = (p, ms, what) =>
  Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error(`timeout: ${what}`)), ms))]);
const subGoal = id();
await db.transact([tx.goals[subGoal].update({ title: "sub goal", level: 9 })]);
const sub = db.subscribeQuery({ goals: { $: { where: { title: "sub goal" }, limit: 5 }, sdktodos: {} } });
const iter = sub[Symbol.asyncIterator]();
const first = (await withTimeout(iter.next(), 10000, "first subscribeQuery payload")).value;
assert(first.type === "ok", "subscribeQuery first payload ok: " + JSON.stringify(first.error?.body ?? null));
assert(first.data.goals.length === 1 && first.data.goals[0].id === subGoal, "subscribeQuery initial tree");
assert(Array.isArray(first.data.goals[0].sdktodos), "nested link is an array without a schema");
assert(first.pageInfo?.goals?.hasNextPage === false, "subscribeQuery page-info formatted");
assert(sub.sessionInfo?.sessionId && sub.sessionInfo?.machineId, "sse-init session info");
await db.transact([tx.goals[subGoal].update({ level: 10 })]);
const second = (await withTimeout(iter.next(), 10000, "refresh payload")).value;
assert(second.type === "ok" && second.data.goals[0].level === 10, "subscribeQuery refresh delivers the new tree");
sub.close();
assert(sub.isClosed, "subscribeQuery close");

// 11. streams over the generic admin SSE session (/admin/sse + /admin/sse/push)
const clientId = "sdk-stream-" + id();
const writer = db.streams.createWriteStream({ clientId }).getWriter();
await writer.write("hello ");
await writer.write("streams");
await writer.close();
const reader = db.streams.createReadStream({ clientId });
let received = "";
await withTimeout(
  (async () => {
    for await (const chunk of reader) received += chunk;
  })(),
  15000,
  "read stream to completion",
);
assert(received === "hello streams", "stream written and read back over admin SSE: " + JSON.stringify(received));

console.log("ADMIN SDK TEST PASSED");
// the SDK keeps its generic /admin/sse EventSource open (no shutdown API),
// which would keep the process alive forever
process.exit(0);
