// S3 storage backend test (issue #9): the server must be booted with
// STORAGE_BACKEND=s3 against an S3-compatible store (MinIO in dev/CI). Talks
// to the bucket directly with its own SigV4 signer to prove what the server
// actually wrote — legacy's object-key layout (`app-id/bin/location-id`,
// Java-hashCode bin), object metadata, deletes, and the stream spool → S3
// handoff — and checks the presigned `$files.url` shape the SDKs consume.
//
// Usage: node s3-storage-test.mjs <app-id> <admin-token>
// Env:   NODE1_URL (default http://localhost:8888), NODE2_URL (default
//        http://localhost:8889), PROXY_NODE_URL (optional: a node booted
//        with S3_PRESIGN=0), S3_ENDPOINT (default http://localhost:9000),
//        S3_BUCKET (default instant-rust-bucket), AWS_REGION (us-east-1),
//        AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (minioadmin),
//        DATABASE_URL (rust postgres, for the stream-spool check via psql)
import { createHash, createHmac } from "node:crypto";
import { execSync } from "node:child_process";

const [appId, adminToken] = process.argv.slice(2);
if (!appId || !adminToken) throw new Error("usage: node s3-storage-test.mjs <app-id> <admin-token>");
const NODE1 = process.env.NODE1_URL || "http://localhost:8888";
const NODE2 = process.env.NODE2_URL || "http://localhost:8889";
const PROXY_NODE = process.env.PROXY_NODE_URL;
const S3_ENDPOINT = (process.env.S3_ENDPOINT || "http://localhost:9000").replace(/\/$/, "");
const BUCKET = process.env.S3_BUCKET || "instant-rust-bucket";
const REGION = process.env.AWS_REGION || "us-east-1";
const AK = process.env.AWS_ACCESS_KEY_ID || "minioadmin";
const SK = process.env.AWS_SECRET_ACCESS_KEY || "minioadmin";
const DB = process.env.DATABASE_URL || "postgres://instant:instant@localhost:5432/instant";

const assert = (c, m) => {
  if (!c) throw new Error("ASSERT FAILED: " + m);
  console.log("ok:", m);
};
const uuid = () => crypto.randomUUID();

// ---------------------------------------------------------------------------
// independent SigV4 signer (header auth) for talking to the bucket directly

const sha256 = (d) => createHash("sha256").update(d).digest("hex");
const hmac = (k, d) => createHmac("sha256", k).update(d).digest();
function amzDate(d = new Date()) {
  return d.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
}
async function s3(method, key, { body, headers = {} } = {}) {
  const url = new URL(`${S3_ENDPOINT}/${BUCKET}/${key}`);
  const now = amzDate();
  const short = now.slice(0, 8);
  const payloadHash = sha256(body ?? "");
  const h = {
    host: url.host,
    "x-amz-content-sha256": payloadHash,
    "x-amz-date": now,
    ...Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v])),
  };
  const sortedKeys = Object.keys(h).sort();
  const canonical = [
    method,
    url.pathname,
    "",
    ...sortedKeys.map((k) => `${k}:${h[k]}`),
    "",
    sortedKeys.join(";"),
    payloadHash,
  ].join("\n");
  const scope = `${short}/${REGION}/s3/aws4_request`;
  const sts = ["AWS4-HMAC-SHA256", now, scope, sha256(canonical)].join("\n");
  const kSign = hmac(hmac(hmac(hmac(`AWS4${SK}`, short), REGION), "s3"), "aws4_request");
  const sig = createHmac("sha256", kSign).update(sts).digest("hex");
  const { host: _h, ...sendHeaders } = h;
  const res = await fetch(url, {
    method,
    headers: { ...sendHeaders, authorization: `AWS4-HMAC-SHA256 Credential=${AK}/${scope}, SignedHeaders=${sortedKeys.join(";")}, Signature=${sig}` },
    body,
  });
  return res;
}

// legacy location-id->bin: (mod (Math/abs (.hashCode s)) 10)
function javaHashBin(s) {
  let h = 0;
  for (const c of s) h = (Math.imul(h, 31) + c.charCodeAt(0)) | 0;
  const abs = h === -2147483648 ? h : Math.abs(h);
  return ((abs % 10) + 10) % 10;
}
const objectKey = (loc) => `${appId}/${javaHashBin(loc)}/${loc}`;

// ---------------------------------------------------------------------------
// server helpers

const adminHeaders = { "app-id": appId, authorization: `Bearer ${adminToken}` };
async function upload(base, path, body, extra = {}) {
  const res = await fetch(`${base}/admin/storage/upload?app_id=${appId}`, {
    method: "PUT",
    headers: { ...adminHeaders, path, ...extra },
    body,
  });
  return { status: res.status, body: await res.json() };
}
async function query(base, q) {
  return (
    await fetch(`${base}/admin/query?app_id=${appId}`, {
      method: "POST",
      headers: { ...adminHeaders, "content-type": "application/json" },
      body: JSON.stringify({ query: q }),
    })
  ).json();
}
const fileRow = async (base, path) => (await query(base, { $files: {} })).$files.find((f) => f.path === path);

// ---------------------------------------------------------------------------
// 1. upload lands in the bucket under the legacy key layout with metadata

const alpha = `alpha ${uuid()}`;
const up = await upload(NODE1, "s3/alpha.txt", alpha, { "content-type": "text/plain", "content-disposition": 'attachment; filename="alpha.txt"' });
assert(up.status === 200 && up.body.data.id && up.body.data["location-id"], "upload via node 1");
const loc = up.body.data["location-id"];
const head = await s3("HEAD", objectKey(loc));
assert(head.status === 200, `object exists at legacy key ${objectKey(loc)}`);
assert(head.headers.get("content-type") === "text/plain", "object content-type stored");
assert(head.headers.get("content-disposition") === 'attachment; filename="alpha.txt"', "object content-disposition stored");
assert(Number(head.headers.get("content-length")) === Buffer.byteLength(alpha), "object size matches");
const wrongBin = [...Array(10).keys()].filter((b) => b !== javaHashBin(loc))[0];
assert((await s3("HEAD", `${appId}/${wrongBin}/${loc}`)).status === 404, "no object under any other bin");

// 2. $files row from the other node: metadata + presigned url
const row = await fileRow(NODE2, "s3/alpha.txt");
assert(row && row.size === Buffer.byteLength(alpha), "node 2 sees the $files row");
assert(row["content-type"] === "text/plain" && row["content-disposition"] === 'attachment; filename="alpha.txt"', "row carries content-type + disposition");
assert(row["location-id"] === loc, "row keeps location-id (legacy transform-$files-result)");
const url = new URL(row.url);
assert(url.origin === new URL(S3_ENDPOINT).origin, "url points at the S3 public endpoint");
assert(url.pathname === `/${BUCKET}/${objectKey(loc)}`, "url is path-style bucket/app/bin/location-id");
const qp = Object.fromEntries(url.searchParams);
assert(qp["X-Amz-Algorithm"] === "AWS4-HMAC-SHA256" && qp["X-Amz-SignedHeaders"] === "host", "SigV4 presigned url");
assert(qp["X-Amz-Expires"] === String(7 * 86400), "7-day expiry like legacy");
assert(/T000000Z$/.test(qp["X-Amz-Date"]), "signing instant is day-bucketed");
assert(qp["response-cache-control"] === "public, max-age=86400, immutable", "legacy response-cache-control param");
assert(qp["X-Amz-Credential"].startsWith(`${AK}/`) && qp["X-Amz-Signature"]?.length === 64, "credential + signature present");
const again = await fileRow(NODE1, "s3/alpha.txt");
assert(again.url === row.url, "url is stable across nodes and calls (browser-cacheable)");

// 3. the presigned url works for a browser
const got = await fetch(row.url);
assert(got.status === 200 && (await got.text()) === alpha, "presigned GET serves the bytes");
assert(got.headers.get("content-type") === "text/plain", "GET content-type");
assert(got.headers.get("content-disposition") === 'attachment; filename="alpha.txt"', "GET content-disposition");
assert(got.headers.get("cache-control") === "public, max-age=86400, immutable", "GET cache-control from the signed param");
const tampered = row.url.replace(/X-Amz-Signature=[0-9a-f]{8}/, "X-Amz-Signature=00000000");
assert((await fetch(tampered)).status === 403, "tampered signature rejected by the store");

// 4. defaults when the client sends nothing
const bare = await upload(NODE1, "s3/bare.bin", Buffer.from([0, 1, 2, 3]));
const bareRow = await fileRow(NODE2, "s3/bare.bin");
assert(bareRow["content-type"] === "application/octet-stream" && bareRow["content-disposition"] === "inline", "row defaults: octet-stream + inline");
const bareHead = await s3("HEAD", objectKey(bare.body.data["location-id"]));
assert(bareHead.headers.get("content-type") === "application/octet-stream" && bareHead.headers.get("content-disposition") === "inline", "object defaults: octet-stream + inline");

// 5. replacing a path re-keys the blob and removes the old object
const beta = `beta ${uuid()}`;
const up2 = await upload(NODE2, "s3/alpha.txt", beta, { "content-type": "text/plain" });
const loc2 = up2.body.data["location-id"];
assert(loc2 !== loc, "replacement gets a new location-id");
assert((await s3("HEAD", objectKey(loc2))).status === 200, "new object present");
assert((await s3("HEAD", objectKey(loc))).status === 404, "old object removed");
const rows = (await query(NODE1, { $files: {} })).$files.filter((f) => f.path === "s3/alpha.txt");
assert(rows.length === 1 && (await (await fetch(rows[0].url)).text()) === beta, "one row per path, new content served");

// 6. delete routes remove the objects
const del = await (
  await fetch(`${NODE1}/admin/storage/files?app_id=${appId}&filename=${encodeURIComponent("s3/alpha.txt")}`, { method: "DELETE", headers: adminHeaders })
).json();
assert(del.data.id === rows[0].id, "delete returns the file id");
assert((await s3("HEAD", objectKey(loc2))).status === 404, "deleted object gone from the bucket");
assert((await fetch(rows[0].url)).status === 404, "old presigned url now 404s");
const many = await (
  await fetch(`${NODE2}/admin/storage/files/delete?app_id=${appId}`, {
    method: "POST",
    headers: { ...adminHeaders, "content-type": "application/json" },
    body: JSON.stringify({ filenames: ["s3/bare.bin", "s3/never-existed"] }),
  })
).json();
assert(many.data.ids.length === 1 && many.data.ids[0] === bareRow.id, "deleteMany returns only existing ids");
assert((await s3("HEAD", objectKey(bare.body.data["location-id"]))).status === 404, "deleteMany removed the object");

// 7. deprecated list + signed-url routes expose the same key layout
const gamma = await upload(NODE1, "s3/gamma.txt", "gamma", { "content-type": "text/plain" });
const list = await (await fetch(`${NODE2}/admin/storage/files?app_id=${appId}`, { headers: adminHeaders })).json();
const listed = list.data.find((f) => f.name === "s3/gamma.txt");
assert(listed && listed.key === objectKey(gamma.body.data["location-id"]) && listed.size === 5 && listed.etag === null && listed.last_modified === null, "list route: legacy StorageFile shape with the S3 object key");
const signed = await (await fetch(`${NODE1}/admin/storage/signed-download-url?app_id=${appId}&filename=${encodeURIComponent("s3/gamma.txt")}`, { headers: adminHeaders })).json();
assert(typeof signed.data === "string" && (await (await fetch(signed.data)).text()) === "gamma", "admin signed-download-url is a working presigned url");
const unknownSigned = await (await fetch(`${NODE1}/admin/storage/signed-download-url?app_id=${appId}&filename=nope`, { headers: adminHeaders })).json();
assert(unknownSigned.data === null, "signed-download-url for an unknown path is null");
const uploadUrl = await (
  await fetch(`${NODE1}/admin/storage/signed-upload-url?app_id=${appId}`, {
    method: "POST",
    headers: { ...adminHeaders, "content-type": "application/json" },
    body: JSON.stringify({ app_id: appId, filename: "s3/delta.txt" }),
  })
).json();
assert(/\/storage\/[0-9a-f-]{36}\/consume-upload-url$/.test(uploadUrl.data), "signed-upload-url hands out a consume url");
const consumed = await (await fetch(uploadUrl.data.replace(NODE1, NODE2), { method: "PUT", headers: { "content-type": "text/plain" }, body: "delta" })).json();
assert(consumed.data?.id && consumed.data.size === 5, "consume-upload-url on the other node stores the file");
assert((await s3("HEAD", objectKey(consumed.data["location-id"]))).status === 200, "consumed upload landed in the bucket");
const reconsume = await fetch(uploadUrl.data, { method: "PUT", body: "again" });
assert(reconsume.status === 400 && (await reconsume.json()).type === "validation-failed", "consume url is single-use");

// 8. streams: live bytes spool through postgres, finished streams move to S3
execSync(
  `psql "${DB}" -q -c "INSERT INTO rules (app_id, code) VALUES ('${appId}', '{\\"\\$streams\\": {\\"allow\\": {\\"create\\": \\"true\\", \\"view\\": \\"true\\"}}}'::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code"`,
);
function connect(base, name) {
  const ws = new WebSocket(`${base.replace(/^http/, "ws")}/runtime/session?app_id=${appId}`);
  const inbox = [];
  const waiters = [];
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    for (const m of Array.isArray(msg) ? msg : [msg]) {
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
  const send = (m) => ws.send(JSON.stringify({ "client-event-id": uuid(), ...m }));
  const waitFor = (pred, timeout = 8000) =>
    new Promise((resolve, reject) => {
      const existing = inbox.find(pred);
      if (existing) return resolve(existing);
      const t = setTimeout(() => reject(new Error(`timeout (${name})`)), timeout);
      waiters.push([pred, (m) => { clearTimeout(t); resolve(m); }]);
    });
  return { ws, send, waitFor, open: new Promise((r) => (ws.onopen = r)), close: () => ws.close() };
}
const w = connect(NODE1, "W");
await w.open;
w.send({ op: "init", "app-id": appId });
await w.waitFor((m) => m.op === "init-ok");
const clientId = "s3-stream-" + uuid();
w.send({ op: "start-stream", "client-id": clientId, "reconnect-token": uuid() });
const started = await w.waitFor((m) => m.op === "start-stream-ok");
const streamId = started["stream-id"];
const streamKey = `stream-${streamId}`;
w.send({ op: "append-stream", "stream-id": streamId, chunks: ["hello "], offset: 0, done: false });
await w.waitFor((m) => m.op === "stream-flushed" && m.offset === 6);
const spooled = execSync(`psql "${DB}" -tA -c "SELECT length(data) FROM rust_blobs WHERE app_id = '${appId}' AND location_id = '${streamKey}'"`).toString().trim();
assert(spooled === "6", "live stream bytes are spooled in postgres");
assert((await s3("HEAD", objectKey(streamKey))).status === 404, "nothing in the bucket while the stream is live");
w.send({ op: "append-stream", "stream-id": streamId, chunks: ["bucket"], offset: 6, done: true });
await w.waitFor((m) => m.op === "stream-flushed" && m.done === true);
let moved = false;
for (let i = 0; i < 40 && !moved; i++) {
  const inPg = execSync(`psql "${DB}" -tA -c "SELECT count(*) FROM rust_blobs WHERE app_id = '${appId}' AND location_id = '${streamKey}'"`).toString().trim();
  const inS3 = (await s3("HEAD", objectKey(streamKey))).status;
  if (inPg === "0" && inS3 === 200) moved = true;
  else await new Promise((r) => setTimeout(r, 250));
}
assert(moved, "finished stream moved from the spool to the bucket");
const obj = await s3("GET", objectKey(streamKey));
assert((await obj.text()) === "hello bucket", "bucket object holds the whole stream");
// a late reader on the other node catches up from S3
const r = connect(NODE2, "R");
await r.open;
r.send({ op: "init", "app-id": appId });
await r.waitFor((m) => m.op === "init-ok");
r.send({ op: "subscribe-stream", "client-id": clientId, offset: 0 });
const catchup = await r.waitFor((m) => m.op === "stream-append");
assert(catchup.content === "hello bucket" && catchup.done === true, "late reader on node 2 gets the full stream from the bucket");
w.close();
r.close();

// 9. optional: a node booted with S3_PRESIGN=0 proxies through /storage/serve
if (PROXY_NODE) {
  const prow = await fileRow(PROXY_NODE, "s3/gamma.txt");
  assert(prow.url.startsWith(`${PROXY_NODE}/storage/serve/${appId}/`), "proxy node hands out /storage/serve urls");
  const pres = await fetch(prow.url);
  assert(pres.status === 200 && (await pres.text()) === "gamma", "proxy serves bytes read from the bucket");
  assert(pres.headers.get("content-type") === "text/plain" && pres.headers.get("content-disposition") === "inline", "proxy sets content-type + disposition from the $files row");
  assert(pres.headers.get("cache-control") === "public, max-age=86400, immutable", "proxy sets legacy's cache-control");
}

console.log("S3 STORAGE TEST PASSED");
