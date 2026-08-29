// Multi-node storage test: file uploaded through node :8888 must be servable
// by node :8889 (blobs live in Postgres, not on node-local disk).
// Usage: node multinode-storage-test.mjs <app-id> <admin-token>
const [appId, adminToken] = process.argv.slice(2);
if (!appId || !adminToken) throw new Error("usage: node multinode-storage-test.mjs <app-id> <token>");
const assert = (c, m) => {
  if (!c) throw new Error("ASSERT FAILED: " + m);
  console.log("ok:", m);
};

const body = `multi-node blob ${crypto.randomUUID()}`;

// upload via node 1
const up = await fetch("http://localhost:8888/admin/storage/upload?app_id=" + appId, {
  method: "PUT",
  headers: {
    "app-id": appId,
    authorization: `Bearer ${adminToken}`,
    path: "xnode/test.txt",
    "content-type": "text/plain",
  },
  body,
});
const upBody = await up.json();
assert(up.status === 200 && upBody.data.id, "upload via node 1");

// query $files via node 2 to get the signed url
const q = await fetch("http://localhost:8889/admin/query?app_id=" + appId, {
  method: "POST",
  headers: {
    "app-id": appId,
    authorization: `Bearer ${adminToken}`,
    "content-type": "application/json",
  },
  body: JSON.stringify({ query: { $files: {} } }),
}).then((r) => r.json());
const file = q.$files.find((f) => f.path === "xnode/test.txt");
assert(file && file.url, "node 2 sees the $files row with a url");

// the url points at node 2's BASE_URL (localhost:8888 by default config);
// rewrite the origin to node 2 to prove the blob is readable from any node
const nodeTwoUrl = file.url.replace("localhost:8888", "localhost:8889");
const served = await fetch(nodeTwoUrl);
assert(served.status === 200, "node 2 serves the blob");
assert((await served.text()) === body, "content matches across nodes");
assert(served.headers.get("content-type").startsWith("text/plain"), "content-type preserved");

// delete via node 2, then node 1 must 404
const del = await fetch(
  `http://localhost:8889/admin/storage/files?app_id=${appId}&filename=${encodeURIComponent("xnode/test.txt")}`,
  { method: "DELETE", headers: { "app-id": appId, authorization: `Bearer ${adminToken}` } },
).then((r) => r.json());
assert(del.data.id, "delete via node 2");
const gone = await fetch(file.url);
assert(gone.status === 404, "blob gone on node 1 after delete");

console.log("MULTINODE STORAGE TEST PASSED");
