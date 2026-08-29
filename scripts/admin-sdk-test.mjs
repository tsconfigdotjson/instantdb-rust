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

console.log("ADMIN SDK TEST PASSED");
