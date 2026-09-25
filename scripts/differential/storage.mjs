// Differential replay for the storage surface (issue #9): every route the
// admin SDK (`db.storage.*`, admin/src/index.ts:849-1035) and the browser SDK
// (core/src/StorageAPI.ts) call, plus what a `$files` query returns over
// /admin/query and over the socket, replayed against the legacy server and
// the rust server. Responses are folded to what the SDKs read; every `url`
// is fetched and compared by what a browser gets back (status, bytes,
// content-type, content-disposition, cache-control) since the URL text
// itself is server-specific (legacy: presigned S3 GET; rust: presigned S3
// GET with STORAGE_BACKEND=s3, else an HMAC-signed /storage/serve URL).
//
// Usage: node storage.mjs <app-id> <admin-token>
//   (the app must exist on both servers with the same admin token, see
//    provision.sh)
// Env: LEGACY_URL (default http://localhost:8891), RUST_URL (default
//      http://localhost:8888), LEGACY_DATABASE_URL / RUST_DATABASE_URL (rules
//      via psql), DUMP=1 prints each server's folded results, ONLY=legacy|rust

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { canon, connect, DIVERGENCE_PREFIXES, describeAllowVerdict, firstDifference, foldFrames, loadAllowlist, newState, normalize, projectState, psql, settle } from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appId = process.argv[2];
const adminToken = process.argv[3];
if (!appId || !adminToken) throw new Error("usage: node storage.mjs <app-id> <admin-token>");

const SERVERS = {
  legacy: {
    url: process.env.LEGACY_URL || "http://localhost:8891",
    db: process.env.LEGACY_DATABASE_URL || "postgres://instant:instant@localhost:8890/instant",
  },
  rust: {
    url: process.env.RUST_URL || "http://localhost:8888",
    db: process.env.RUST_DATABASE_URL || "postgres://instant:instant@localhost:5432/instant",
  },
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// rules written straight to Postgres: legacy evicts its rule cache off the
// WAL feed, so give it a beat before the next op
const RULES_SETTLE_MS = 1200;

// ---------------------------------------------------------------------------
// http helpers (headers exactly as the SDKs send them)

async function call(base, method, p, { headers = {}, body, json } = {}) {
  const h = { ...headers };
  if (json !== undefined) h["content-type"] = "application/json";
  const res = await fetch(base + p, { method, headers: h, body: json !== undefined ? JSON.stringify(json) : body });
  const text = await res.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = { "<non-json>": text.slice(0, 200) };
  }
  return { status: res.status, body: parsed };
}

const adminHeaders = { "app-id": appId, authorization: `Bearer ${adminToken}` };

function errView(res) {
  const b = res.body ?? {};
  return normalize({ status: res.status, type: b.type, message: b.message, hint: b.hint ?? null, keys: Object.keys(b).sort() });
}
function plainView(res) {
  if (res.status !== 200) return errView(res);
  return normalize({ status: 200, body: res.body });
}

// What a browser gets from a download url. The URL text is server-specific;
// its shape is recorded separately (see urlShape).
async function fetched(url) {
  if (typeof url !== "string") return { url };
  const res = await fetch(url);
  const body = await res.text();
  return {
    status: res.status,
    body: res.status === 200 ? body : "<error>",
    contentType: res.headers.get("content-type"),
    contentDisposition: res.headers.get("content-disposition"),
    cacheControl: res.headers.get("cache-control"),
  };
}

const URL_SHAPES = new Set();
function urlShape(url) {
  if (typeof url !== "string") return url;
  const u = new URL(url);
  const presigned = u.searchParams.has("X-Amz-Signature");
  URL_SHAPES.add(presigned ? "presigned" : "proxy");
  if (!presigned) return "<proxy:/storage/serve>";
  // legacy presigns app-id/bin/location-id under the bucket (path-style
  // against a custom endpoint); everything volatile is masked
  const segs = u.pathname.split("/").filter(Boolean);
  const key = segs.slice(-3).map((s, i) => (i === 1 ? "<bin>" : "<uuid>")).join("/");
  const params = {};
  for (const [k, v] of u.searchParams) {
    params[k] =
      k === "X-Amz-Signature" ? "<sig>" : k === "X-Amz-Date" ? (/^\d{8}T000000Z$/.test(v) ? "<day-bucketed>" : v) : k === "X-Amz-Credential" ? v.replace(/^[^/]+\/\d{8}\//, "<key>/<day>/") : v;
  }
  return { pathTail: key, params };
}

async function fileView(f) {
  if (!f || typeof f !== "object") return f;
  const { url, ...rest } = f;
  const out = normalize(rest);
  if ("url" in f) {
    out.fetched = await fetched(url);
    out.urlShape = urlShape(url);
  }
  return out;
}
const byPath = (files) => [...(files ?? [])].sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
async function filesView(res) {
  if (res.status !== 200) return errView(res);
  return { status: 200, files: await Promise.all(byPath(res.body.$files).map(fileView)) };
}

// ---------------------------------------------------------------------------
// scenario

async function runAgainst(name) {
  const { url: base, db } = SERVERS[name];
  const out = {};
  const record = (step, v) => {
    out[step] = v;
    if (process.env.DUMP) console.log(`[${name}] ${step}`, JSON.stringify(v, null, 1));
  };
  const upload = (p, body, extra = {}, headers = adminHeaders) =>
    call(base, "PUT", `/admin/storage/upload?app_id=${appId}`, { headers: { ...headers, path: p, ...extra }, body });
  const adminQuery = (q) => call(base, "POST", `/admin/query?app_id=${appId}`, { headers: adminHeaders, json: { query: q } });

  // 01: admin uploads (db.storage.uploadFile): metadata headers present,
  // absent, and the "null"/"undefined" strings the SDK can leak
  const r1 = {};
  r1.withMeta = plainView(await upload("docs/a.txt", "alpha", { "content-type": "text/plain", "content-disposition": 'attachment; filename="a.txt"' }));
  // distinct sizes so the `order: {size}` query below has no ties
  r1.noMeta = plainView(await upload("docs/b.bin", Buffer.from([0, 1, 2, 3, 4, 5])));
  r1.nullType = plainView(await upload("docs/c.txt", "charlie", { "content-type": "null" }));
  r1.undefinedType = plainView(await upload("docs/d.txt", "delta", { "content-type": "undefined", "content-disposition": "" }));
  r1.missingPath = errView(await call(base, "PUT", `/admin/storage/upload?app_id=${appId}`, { headers: adminHeaders, body: "x" }));
  r1.badToken = errView(await upload("docs/z.txt", "z", {}, { "app-id": appId, authorization: "Bearer 00000000-0000-4000-8000-000000000bad" }));
  r1.noAppId = errView(await call(base, "PUT", `/admin/storage/upload`, { headers: { authorization: `Bearer ${adminToken}`, path: "docs/z.txt" }, body: "x" }));
  record("01-admin-upload", r1);

  // 02: $files over /admin/query: whole rows, then fields projections
  const r2 = {};
  r2.all = await filesView(await adminQuery({ $files: {} }));
  r2.fieldsPathUrl = await filesView(await adminQuery({ $files: { $: { fields: ["path", "url"] } } }));
  r2.fieldsPathLoc = await filesView(await adminQuery({ $files: { $: { fields: ["path", "location-id"] } } }));
  r2.fieldsPathOnly = await filesView(await adminQuery({ $files: { $: { fields: ["path", "size"] } } }));
  r2.wherePath = await filesView(await adminQuery({ $files: { $: { where: { path: "docs/a.txt" } } } }));
  r2.orderSize = await (async () => {
    const res = await adminQuery({ $files: { $: { order: { size: "desc" } }, } });
    if (res.status !== 200) return errView(res);
    return { status: 200, files: await Promise.all((res.body.$files ?? []).map(fileView)) };
  })();
  record("02-admin-query-files", r2);

  // 03: $files over the socket (admin socket + a plain socket under a view
  // rule): join-rows fold; url triple values are fetched like above
  const r3 = {};
  {
    psql(db, `INSERT INTO rules (app_id, code) VALUES ('${appId}', '{"$files": {"allow": {"view": "data.path != ''docs/c.txt''", "create": "data.path.startsWith(''u/'')", "delete": "data.path.startsWith(''u/'')"}}}'::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`);
    await sleep(RULES_SETTLE_MS);
    const admin = connect(base, appId, `${name}:ADMIN`);
    const plain = connect(base, appId, `${name}:PLAIN`);
    await admin.open;
    await plain.open;
    admin.send({ op: "init", "app-id": appId, "client-event-id": "00000000-0000-4000-9000-000000000301", "__admin-token": adminToken });
    plain.send({ op: "init", "app-id": appId, "client-event-id": "00000000-0000-4000-9000-000000000302" });
    await admin.waitFor((m) => m.op === "init-ok");
    await plain.waitFor((m) => m.op === "init-ok");
    const attrs = admin.frames.find((m) => m.op === "init-ok").attrs;
    const urlAttr = attrs.find((a) => a["forward-identity"][1] === "$files" && a["forward-identity"][2] === "url")?.id;
    admin.takeNewFrames();
    plain.takeNewFrames();
    admin.send({ op: "add-query", q: { $files: {} }, "client-event-id": "00000000-0000-4000-9000-000000000303" });
    plain.send({ op: "add-query", q: { $files: {} }, "client-event-id": "00000000-0000-4000-9000-000000000304" });
    admin.send({ op: "add-query", q: { $files: { $: { fields: ["path"] } } }, "client-event-id": "00000000-0000-4000-9000-000000000305" });
    await admin.waitFor((m) => m.op === "add-query-ok" && m["client-event-id"] === "00000000-0000-4000-9000-000000000305");
    await plain.waitFor((m) => m.op === "add-query-ok" || m.op === "error");
    await settle([admin, plain]);
    for (const [k, conn] of [["admin", admin], ["plain", plain]]) {
      const frames = conn.takeNewFrames();
      const st = newState();
      const direct = await foldFrames(frames, st);
      const projected = projectState(st);
      // url triples: fetch what the browser would get (the value text is
      // server-specific), keyed by the attr id so the fold stays comparable
      const queries = {};
      for (const [qk, qv] of Object.entries(projected.queries ?? {})) {
        const triples = [];
        for (const t of qv.triples ?? qv.result ?? []) {
          if (Array.isArray(t) && (t[1] === urlAttr || /^https?:\/\//.test(t[2]))) {
            triples.push([t[0], t[1], { fetched: await fetched(t[2]), urlShape: urlShape(t[2]) }, t[3]]);
          } else triples.push(t);
        }
        // re-sort: the fold sorted by the raw url text, which is server-specific
        triples.sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
        queries[qk] = { ...qv, ...(qv.triples ? { triples } : { result: triples }) };
      }
      r3[k] = { queries, direct: direct.filter((m) => m.op !== "add-query-ok") };
    }
    admin.close();
    plain.close();
  }
  record("03-socket-query-files", r3);

  // 04: browser SDK routes (core/src/StorageAPI.ts) with a refresh token
  // under the create/delete/view rules above
  const r4 = {};
  {
    const tok = await call(base, "POST", "/admin/refresh_tokens", { headers: adminHeaders, json: { email: "storage-diff@example.com" } });
    const refreshToken = tok.body?.user?.refresh_token;
    if (!refreshToken) throw new Error(`no refresh token from ${name}: ${JSON.stringify(tok.body).slice(0, 300)}`);
    const userHeaders = { "app-id": appId, app_id: appId, authorization: `Bearer ${refreshToken}` };
    const clientUpload = (p, body, extra = {}) => call(base, "PUT", `/storage/upload`, { headers: { ...userHeaders, path: p, ...extra }, body });
    r4.allowed = plainView(await clientUpload("u/x.txt", "x-ray", { "content-type": "text/plain", "content-disposition": "inline" }));
    r4.allowedFilenameHeader = plainView(await call(base, "PUT", `/storage/upload`, { headers: { ...userHeaders, filename: "u/y.txt", "content-type": "image/png" }, body: "yankee" }));
    r4.denied = errView(await clientUpload("docs/nope.txt", "nope", { "content-type": "text/plain" }));
    r4.noToken = errView(await call(base, "PUT", `/storage/upload`, { headers: { "app-id": appId, path: "u/anon.txt", "content-type": "text/plain" }, body: "anon" }));
    r4.missingPath = errView(await call(base, "PUT", `/storage/upload`, { headers: { ...userHeaders, "content-type": "text/plain" }, body: "x" }));
    r4.missingAppId = errView(await call(base, "PUT", `/storage/upload`, { headers: { authorization: `Bearer ${refreshToken}`, path: "u/z.txt" }, body: "x" }));
    const dl = await call(base, "GET", `/storage/signed-download-url?app_id=${appId}&filename=${encodeURIComponent("u/x.txt")}`, { headers: { "content-type": "application/json", authorization: `Bearer ${refreshToken}` } });
    r4.downloadUrl = dl.status === 200 ? { status: 200, fetched: await fetched(dl.body.data), urlShape: urlShape(dl.body.data) } : errView(dl);
    r4.downloadUrlUnknown = plainView(await call(base, "GET", `/storage/signed-download-url?app_id=${appId}&filename=nope`, { headers: { authorization: `Bearer ${refreshToken}` } }));
    r4.downloadUrlDeniedView = errView(await call(base, "GET", `/storage/signed-download-url?app_id=${appId}&filename=${encodeURIComponent("docs/c.txt")}`, { headers: { authorization: `Bearer ${refreshToken}` } }));
    const anonDl = await call(base, "GET", `/storage/signed-download-url?app_id=${appId}&filename=${encodeURIComponent("docs/a.txt")}`, { headers: { "content-type": "application/json" } });
    r4.downloadUrlAnon = anonDl.status === 200 ? { status: 200, fetched: await fetched(anonDl.body.data) } : errView(anonDl);
    r4.downloadUrlMissingFilename = errView(await call(base, "GET", `/storage/signed-download-url?app_id=${appId}`, { headers: { authorization: `Bearer ${refreshToken}` } }));
    // deprecated upload-url flow (getSignedUploadUrl + upload)
    const su = await call(base, "POST", `/storage/signed-upload-url`, { headers: { authorization: `Bearer ${refreshToken}` }, json: { app_id: appId, filename: "u/presigned.txt" } });
    r4.signedUploadUrl = su.status === 200 ? { status: 200, urlTail: String(su.body.data).replace(/^.*\/storage\//, "/storage/").replace(/[0-9a-f-]{36}/, "<uuid>") } : errView(su);
    if (su.status === 200) {
      const consume = await call(base, "PUT", new URL(su.body.data).pathname, { headers: { "content-type": "text/plain" }, body: "presigned body" });
      r4.consumeUpload = plainView(consume);
      r4.consumeAgain = errView(await call(base, "PUT", new URL(su.body.data).pathname, { headers: { "content-type": "text/plain" }, body: "again" }));
    }
    r4.signedUploadUrlDenied = errView(await call(base, "POST", `/storage/signed-upload-url`, { headers: { authorization: `Bearer ${refreshToken}` }, json: { app_id: appId, filename: "docs/denied.txt" } }));
    r4.consumeUnknown = errView(await call(base, "PUT", `/storage/00000000-0000-4000-8000-00000000dead/consume-upload-url`, { headers: { "content-type": "text/plain" }, body: "x" }));
    r4.deleteAllowed = plainView(await call(base, "DELETE", `/storage/files?app_id=${appId}&filename=${encodeURIComponent("u/y.txt")}`, { headers: { "content-type": "application/json", authorization: `Bearer ${refreshToken}` } }));
    r4.deleteUnknown = plainView(await call(base, "DELETE", `/storage/files?app_id=${appId}&filename=nope`, { headers: { "content-type": "application/json", authorization: `Bearer ${refreshToken}` } }));
    r4.deleteDenied = errView(await call(base, "DELETE", `/storage/files?app_id=${appId}&filename=${encodeURIComponent("docs/a.txt")}`, { headers: { "content-type": "application/json", authorization: `Bearer ${refreshToken}` } }));
    r4.deleteMissingFilename = errView(await call(base, "DELETE", `/storage/files?app_id=${appId}`, { headers: { authorization: `Bearer ${refreshToken}` } }));
    r4.filesAfter = await filesView(await adminQuery({ $files: { $: { where: { path: { $like: "u/%" } } } } }));
    // impersonated admin calls go through the rules too (as-token)
    r4.asTokenUploadDenied = errView(await upload("docs/as-token.txt", "x", { "content-type": "text/plain" }, { ...adminHeaders, "as-token": refreshToken }));
    r4.asTokenUploadAllowed = plainView(await upload("u/as-token.txt", "as token", { "content-type": "text/plain" }, { ...adminHeaders, "as-token": refreshToken }));
    r4.asGuestUploadDenied = errView(await upload("docs/as-guest.txt", "x", { "content-type": "text/plain" }, { ...adminHeaders, "as-guest": "true" }));
  }
  record("04-client-routes", r4);

  // 05: replacing a path keeps one row and serves the new bytes
  const r5 = {};
  r5.replace = plainView(await upload("docs/a.txt", "alpha v2", { "content-type": "text/markdown" }));
  r5.rows = await filesView(await adminQuery({ $files: { $: { where: { path: "docs/a.txt" } } } }));
  record("05-replace", r5);

  // 06: admin deletes (db.storage.delete / deleteMany) + error matrix
  const r6 = {};
  r6.deleteOne = plainView(await call(base, "DELETE", `/admin/storage/files?app_id=${appId}&filename=${encodeURIComponent("docs/c.txt")}`, { headers: adminHeaders }));
  r6.deleteUnknown = plainView(await call(base, "DELETE", `/admin/storage/files?app_id=${appId}&filename=nope`, { headers: adminHeaders }));
  r6.deleteMissingFilename = errView(await call(base, "DELETE", `/admin/storage/files?app_id=${appId}`, { headers: adminHeaders }));
  r6.deleteMany = plainView(await call(base, "POST", `/admin/storage/files/delete?app_id=${appId}`, { headers: adminHeaders, json: { filenames: ["docs/b.bin", "docs/never"] } }));
  r6.deleteManyMissing = errView(await call(base, "POST", `/admin/storage/files/delete?app_id=${appId}`, { headers: adminHeaders, json: {} }));
  r6.deleteManyBadToken = errView(await call(base, "POST", `/admin/storage/files/delete?app_id=${appId}`, { headers: { "app-id": appId, authorization: "Bearer 00000000-0000-4000-8000-000000000bad" }, json: { filenames: ["docs/d.txt"] } }));
  r6.deleteAsTokenDenied = errView(await call(base, "DELETE", `/admin/storage/files?app_id=${appId}&filename=${encodeURIComponent("docs/d.txt")}`, { headers: { ...adminHeaders, "as-guest": "true" } }));
  r6.filesAfter = await filesView(await adminQuery({ $files: {} }));
  record("06-admin-delete", r6);

  // 07: deprecated admin routes (list / signed-download-url / upload + consume)
  const r7 = {};
  const list = await call(base, "GET", `/admin/storage/files?app_id=${appId}`, { headers: adminHeaders });
  r7.list = list.status === 200 ? { status: 200, data: [...list.body.data].sort((a, b) => (a.name < b.name ? -1 : 1)).map((f) => normalize({ ...f, key: String(f.key).replace(/[0-9a-f-]{36}/g, "<uuid>").replace(/\/\d\//, "/<bin>/") })) } : errView(list);
  const sd = await call(base, "GET", `/admin/storage/signed-download-url?app_id=${appId}&filename=${encodeURIComponent("docs/d.txt")}`, { headers: adminHeaders });
  r7.signedDownload = sd.status === 200 ? { status: 200, fetched: await fetched(sd.body.data), urlShape: urlShape(sd.body.data) } : errView(sd);
  r7.signedDownloadUnknown = plainView(await call(base, "GET", `/admin/storage/signed-download-url?app_id=${appId}&filename=nope`, { headers: adminHeaders }));
  r7.signedDownloadMissing = errView(await call(base, "GET", `/admin/storage/signed-download-url?app_id=${appId}`, { headers: adminHeaders }));
  const su = await call(base, "POST", `/admin/storage/signed-upload-url?app_id=${appId}`, { headers: adminHeaders, json: { app_id: appId, filename: "docs/e.txt" } });
  r7.signedUpload = su.status === 200 ? { status: 200, urlTail: String(su.body.data).replace(/^.*\/storage\//, "/storage/").replace(/[0-9a-f-]{36}/, "<uuid>") } : errView(su);
  if (su.status === 200) {
    r7.consume = plainView(await call(base, "PUT", new URL(su.body.data).pathname, { headers: { "content-type": "text/html" }, body: "<b>echo</b>" }));
    r7.consumeAgain = errView(await call(base, "PUT", new URL(su.body.data).pathname, { headers: { "content-type": "text/html" }, body: "again" }));
  }
  r7.signedUploadMissing = errView(await call(base, "POST", `/admin/storage/signed-upload-url?app_id=${appId}`, { headers: adminHeaders, json: {} }));
  r7.filesAfter = await filesView(await adminQuery({ $files: { $: { where: { path: "docs/e.txt" } } } }));
  record("07-admin-deprecated-routes", r7);

  // 08: $files rows written through transact: allowed updates vs guarded
  // system columns (admin steps grammar, like the SDK's `db.tx.$files[...]`)
  const r8 = {};
  const fileRow = r7.filesAfter.files?.[0];
  const transact = (steps) => call(base, "POST", `/admin/transact?app_id=${appId}`, { headers: adminHeaders, json: { steps } });
  if (fileRow?.id) {
    const rowsNow = await adminQuery({ $files: { $: { where: { path: "docs/e.txt" } } } });
    const fid = rowsNow.body.$files[0].id;
    r8.updatePath = plainView(await transact([["update", "$files", fid, { path: "docs/e-renamed.txt" }]]));
    r8.updateSize = errView(await transact([["update", "$files", fid, { size: 1 }]]));
    r8.updateLocation = errView(await transact([["update", "$files", fid, { "location-id": "hacked" }]]));
    r8.updateContentType = plainView(await transact([["update", "$files", fid, { "content-type": "text/plain" }]]));
    r8.afterUpdates = await filesView(await adminQuery({ $files: { $: { where: { path: { $like: "docs/e%" } } } } }));
    r8.deleteByLookup = plainView(await transact([["delete", "$files", { path: "docs/e-renamed.txt" }]]));
    r8.afterDelete = await filesView(await adminQuery({ $files: { $: { where: { path: { $like: "docs/e%" } } } } }));
  }
  record("08-files-transact", r8);

  return out;
}

// ---------------------------------------------------------------------------
// diff + allowlist

// strict entries (lib.mjs loadAllowlist): this layer owns the storage:
// paths and fails on any of them that allowed nothing
const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), { prefixes: DIVERGENCE_PREFIXES.storage });
function pushDiff(p, legacyVal, rustVal) {
  const verdict = allowlist.check(p, legacyVal, rustVal);
  diffs.push({ path: p, allowed: !!verdict.entry, verdict, legacy: legacyVal, rust: rustVal });
}

// url shapes are only comparable when both servers presign S3 URLs (rust
// with STORAGE_BACKEND=s3); a rust server proxying downloads through
// /storage/serve is a documented difference (docs/PARITY.md), so the shape
// keys are dropped from both sides and only the fetched content compares
function stripUrlShapes(v) {
  if (Array.isArray(v)) return v.map(stripUrlShapes);
  if (v !== null && typeof v === "object") {
    const out = {};
    for (const [k, x] of Object.entries(v)) if (k !== "urlShape") out[k] = stripUrlShapes(x);
    return out;
  }
  return v;
}

const only = process.env.ONLY;
const results = {};
for (const name of only ? [only] : ["legacy", "rust"]) {
  console.log(`replaying storage routes against ${name}…`);
  URL_SHAPES.clear();
  results[name] = await runAgainst(name);
  if (name === "rust") results.rustPresigns = URL_SHAPES.has("presigned") && !URL_SHAPES.has("proxy");
}
if (only) process.exit(0);

console.log(results.rustPresigns ? "rust presigns S3 urls: presigned url shapes are compared" : "rust proxies downloads through /storage/serve: url shapes are not compared (documented divergence)");
const diffs = [];
for (const step of new Set([...Object.keys(results.legacy), ...Object.keys(results.rust)])) {
  const l = results.rustPresigns ? results.legacy[step] : stripUrlShapes(results.legacy[step]);
  const r = results.rustPresigns ? results.rust[step] : stripUrlShapes(results.rust[step]);
  const keys = l && r && typeof l === "object" && !Array.isArray(l) ? new Set([...Object.keys(l), ...Object.keys(r ?? {})]) : null;
  if (keys) {
    for (const k of keys) {
      if (canon(l[k]) !== canon(r?.[k])) {
        pushDiff(`storage:${step}/${k}`, l[k], r?.[k]);
      }
    }
  } else if (canon(l) !== canon(r)) {
    pushDiff(`storage:${step}`, l, r);
  }
}
const blocking = diffs.filter((d) => !d.allowed);
for (const d of diffs) {
  console.log(`\n[${d.allowed ? "ALLOWED" : "DIVERGENCE"}] ${d.path}`);
  const fd = firstDifference(d.legacy, d.rust);
  if (fd && fd.p) {
    console.log(`  first differing sub-path: ${fd.p}`);
    console.log("  legacy:", JSON.stringify(fd.a)?.slice(0, 1500));
    console.log("  rust:  ", JSON.stringify(fd.b)?.slice(0, 1500));
    console.log("  full legacy:", JSON.stringify(d.legacy)?.slice(0, 700));
    console.log("  full rust:  ", JSON.stringify(d.rust)?.slice(0, 700));
  } else {
    console.log("  legacy:", JSON.stringify(d.legacy)?.slice(0, 1500));
    console.log("  rust:  ", JSON.stringify(d.rust)?.slice(0, 1500));
  }
  for (const line of describeAllowVerdict(d.verdict)) console.log(line);
}
const staleAllows = allowlist.reportStale();
if (blocking.length || staleAllows) {
  console.error(`\nSTORAGE DIFFERENTIAL FAILED: ${blocking.length} unallowed divergences, ${staleAllows} stale allowlist entries`);
  process.exit(1);
}
const steps = Object.keys(results.legacy).length;
console.log(`\nSTORAGE DIFFERENTIAL PASSED: ${steps} steps, ${diffs.length} allowed divergences`);
