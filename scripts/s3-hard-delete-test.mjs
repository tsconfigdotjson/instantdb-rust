// S3 hard-delete cleanup: hard-deleting an app removes its triples, which
// fires migration 52's trigger and queues every live $files location-id in
// app_files_to_sweep. The storage sweeper must then remove the S3 object(s)
// and the queue rows (legacy storage/sweeper.clj). The S3 nodes are booted
// with INSTANT_HARD_DELETE_GRACE_HOURS=0, INSTANT_HARD_DELETE_SWEEP_SECS=1,
// INSTANT_STORAGE_SWEEP_GRACE_HOURS=0 and INSTANT_STORAGE_SWEEP_SECS=1 in CI.
//
// Usage: node s3-hard-delete-test.mjs <app-id> <admin-token>
// Env:   SERVER_URL (default http://localhost:8898), DATABASE_URL (psql)
import { execFileSync } from "node:child_process";

const [appId, adminToken] = process.argv.slice(2);
if (!appId || !adminToken) throw new Error("usage: node s3-hard-delete-test.mjs <app-id> <admin-token>");
const SERVER = process.env.SERVER_URL || "http://localhost:8898";
const DB = process.env.DATABASE_URL || "postgres://instant:instant@localhost:5432/instant";
const assert = (c, m) => {
  if (!c) throw new Error("ASSERT FAILED: " + m);
  console.log("ok:", m);
};
const psql = (sql) => execFileSync("psql", [DB, "-tA", "-v", "ON_ERROR_STOP=1", "-c", sql]).toString().trim();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const waitFor = async (what, pred, ms = 20000) => {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    if (await pred()) return;
    await sleep(500);
  }
  throw new Error("timed out waiting for " + what);
};
const adminHeaders = { "app-id": appId, authorization: `Bearer ${adminToken}` };
const path = `hard-delete/${crypto.randomUUID()}.txt`;

const up = await fetch(`${SERVER}/admin/storage/upload?app_id=${appId}`, {
  method: "PUT",
  headers: { ...adminHeaders, path, "content-type": "text/plain" },
  body: "hard delete me",
});
const upBody = await up.json();
assert(up.status === 200 && upBody.data.id, "upload before hard delete");
const row = (
  await (
    await fetch(`${SERVER}/admin/query?app_id=${appId}`, {
      method: "POST",
      headers: { ...adminHeaders, "content-type": "application/json" },
      body: JSON.stringify({ query: { $files: {} } }),
    })
  ).json()
).$files.find((f) => f.path === path);
assert(row && row.url, "the file has a presigned S3 url");
assert((await fetch(row.url)).status === 200, "the object is in the bucket before the purge");

psql(`UPDATE apps SET deletion_marked_at = now() WHERE id = '${appId}'`);
await waitFor("the hard-delete sweeper to purge the app", async () => psql(`SELECT count(*) FROM apps WHERE id = '${appId}'`) === "0");
assert(true, "hard delete purged the app");

await waitFor("the storage sweeper to delete the purged app's S3 object", async () => (await fetch(row.url)).status === 404);
assert(true, "the purged app's object is gone from the bucket");

await waitFor("the purged app's sweep rows to drain", async () => psql(`SELECT count(*) FROM app_files_to_sweep WHERE app_id = '${appId}'`) === "0");
assert(true, "the purged app left no app_files_to_sweep rows");

console.log("S3 HARD DELETE TEST PASSED");
