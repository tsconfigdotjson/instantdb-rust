// Seeded storage / $files fuzz layer of the differential harness.
//
// Storage used to be covered only by the eight scripted steps in storage.mjs.
// This layer drives both servers with a seeded sequence of:
//   - admin and client uploads (PUT /admin/storage/upload, PUT /storage/upload,
//     the deprecated signed-upload-url + consume-upload-url flow), overwrites
//     of the same path, odd / unicode / nested / whitespace-padded paths, and
//     missing / blank path and filename parameters;
//   - content-type and content-disposition variants (missing, blank, null /
//     undefined strings, inline / attachment);
//   - single, bulk and missing-file deletes, on the admin, client and
//     impersonated admin routes;
//   - signed download URLs, with the URL fetched and compared by what a
//     browser receives (status, bytes, content-type, content-disposition,
//     cache-control);
//   - $files queries over the socket and /admin/query: whole rows, where,
//     order, fields projections, limit/offset and cursor pagination, plus a
//     custom entity linked to $files (query results are + links);
//   - generated $files create / view / delete rules, applied to client routes,
//     socket subscriptions and impersonated admin calls (as-guest / as-email /
//     as-token);
//   - settled subscription state at checkpoints, with a fresh-probe rule so a
//     legacy cache lag is not mistaken for a data difference.
//
// Compared against the live legacy server:
//   - every HTTP response (status, type, message, hint and key set) and every
//     successful upload / delete / signed-url response;
//   - every $files / linked query result and page-info, every settled folded
//     subscription, and every download fetch.
//
// Only genuinely nondeterministic values are normalized: server-chosen ids
// and timestamps, downloaded URL text (legacy always presigns S3; rust serves
// /storage/serve unless STORAGE_BACKEND=s3), and signature/upload ids in URL
// shapes. File ids are mapped back to their path so links between a custom
// entity and $files stay comparable. The documented URL-text difference is
// handled the same way storage.mjs does it: if rust presigns, URL shapes are
// compared too; otherwise only the fetched bytes/headers are compared.
//
// Usage: node fuzz-storage.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]
// Env: LEGACY_URL, RUST_URL, LEGACY_DATABASE_URL, RUST_DATABASE_URL,
//      DUMP=1 prints each server's outcomes.

import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  canon,
  connect,
  describeAllowVerdict,
  DIVERGENCE_PREFIXES,
  firstDifference,
  loadAllowlist,
  makeIdFactory,
  normalize,
  psql,
  settle,
  uuid,
} from "./lib.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const appIdLegacy = process.argv[2];
const appIdRust = process.argv[3];
const adminToken = process.argv[4];
const seed = Number(process.argv[5] ?? 201);
const rounds = Number(process.argv[6] ?? 80);
if (!appIdLegacy || !appIdRust || !adminToken)
  throw new Error("usage: node fuzz-storage.mjs <app-id-legacy> <app-id-rust> <admin-token> [seed] [rounds]");

const SERVERS = {
  legacy: {
    url: process.env.LEGACY_URL || "http://localhost:8891",
    db: process.env.LEGACY_DATABASE_URL || "postgres://instant:instant@localhost:8890/instant",
    appId: appIdLegacy,
  },
  rust: {
    url: process.env.RUST_URL || "http://localhost:8888",
    db: process.env.RUST_DATABASE_URL || "postgres://instant:instant@localhost:5432/instant",
    appId: appIdRust,
  },
};

// legacy evicts its rule cache off the WAL feed; a checkpoint waits for the
// frame stream to go quiet. A legacy probe that disagrees with the session is
// asked once more after RESETTLE_MS.
const RULES_SETTLE_MS = 1500;
const CHECKPOINT_QUIET_MS = 3000;
const CHECKPOINT_RESETTLE_MS = 2500;
const LEGACY_RESETTLE_MS = 1500;

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
const URL_RE = /^https?:\/\//;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function prng(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function buildPlan() {
  const rand = prng(seed * 104729 + 211);
  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const chance = (p) => rand() < p;
  const mk = makeIdFactory(appIdLegacy);
  const users = {
    U1: { id: mk(), email: `storage-u1-${seed}@fuzz.example` },
    U2: { id: mk(), email: `storage-u2-${seed}@fuzz.example` },
  };

  const NS = "sf";
  const attrs = {
    id: mk(),
    file: mk(),
    fileRev: mk(),
    label: mk(),
  };
  const assets = Array.from({ length: 6 }, () => mk());
  const userPrefix = pick(["u/", "user/", "up/"]);
  const guestPrefix = "guest/";

  // Generated $files rules. pub/ and guest/ are always visible so a guest can
  // subscribe; userPrefix is the client user's writable area. Variants add
  // auth-dependent view/create/delete paths without changing that floor.
  const view = pick([
    `data.path.startsWith('pub/') || data.path.startsWith('${userPrefix}') || data.path.startsWith('${guestPrefix}')`,
    `data.path.startsWith('pub/') || (auth.id != null && data.path.startsWith('${userPrefix}')) || data.path.startsWith('${guestPrefix}')`,
    `(auth.id != null || data.path.startsWith('pub/')) && !data.path.startsWith('secret/')`,
  ]);
  const create = pick([
    `data.path.startsWith('${userPrefix}') || data.path.startsWith('${guestPrefix}')`,
    `(auth.id != null && data.path.startsWith('${userPrefix}')) || data.path.startsWith('${guestPrefix}')`,
  ]);
  const remove = pick([
    `data.path.startsWith('${userPrefix}') || data.path.startsWith('${guestPrefix}')`,
    `(auth.id != null && data.path.startsWith('${userPrefix}')) || data.path.startsWith('${guestPrefix}')`,
  ]);
  const rules = { $files: { allow: { view, create, delete: remove } } };

  const script = [];
  const op = (kind, props = {}) => {
    script.push({ kind, ...props });
    return script[script.length - 1];
  };
  const upload = (route, p, body, meta = {}, label = "", omitAppId = false) =>
    op("upload", { route, path: p, body: Buffer.from(body), meta, label, omitAppId });
  const deleteOp = (route, paths, opts = {}) => op("delete", { route, paths, ...opts });
  const download = (route, session, p, opts = {}) => op("download", { route, session, path: p, ...opts });
  const signedUpload = (route, session, p, body, meta = {}, opts = {}) =>
    op("signed-upload", { route, session, path: p, body: Buffer.from(body), meta, ...opts });
  const adminQuery = (q, opts = {}) => op("admin-query", { q, session: "ADMIN", ...opts });
  const socketQuery = (session, q, label = "") => op("socket-query", { session, q, label });
  const paginate = (session, q, label = "") => op("paginate", { session, q, label });
  const subscribe = (session, key, q) => op("subscribe", { session, key, q });
  const unsubscribe = (session, key) => op("unsubscribe", { session, key });
  const checkpoint = (label) => op("checkpoint", { label });
  const link = (links, label = "") => op("link", { links, label });

  const qAll = { $files: { $: { order: { path: "asc" } } } };
  const qAllDesc = { $files: { $: { order: { path: "desc" } } } };
  const qPub = { $files: { $: { where: { path: { $like: "pub/%" } }, fields: ["path", "size", "url"] } } };
  const qUser = { $files: { $: { where: { path: { $like: `${userPrefix}%` } }, fields: ["path", "size", "content-type"] } } };
  const qAsset = { [NS]: { file: {} } };
  const qTemp = { $files: { $: { where: { path: { $like: "tmp/%" } }, order: { path: "asc" } } } };
  const qPage = { $files: { $: { order: { path: "asc" }, limit: 2 } } };
  const qWhereIn = { $files: { $: { where: { path: { $in: ["pub/base.txt", `${userPrefix}initial.txt`] } } } } };

  const subscriptions = {
    ADMIN: [
      { key: "admin-all", q: qAll },
      { key: "admin-asset", q: qAsset },
      { key: "admin-temp", q: qTemp },
      { key: "admin-page", q: qPage },
    ],
    GUEST: [
      { key: "guest-pub", q: qPub },
      { key: "guest-all", q: qAll },
    ],
    USER: [
      { key: "user-files", q: qUser },
      { key: "user-all", q: qAllDesc },
    ],
  };
  const held = { ADMIN: new Set(), GUEST: new Set(), USER: new Set() };

  const initial = [
    { p: "pub/base.txt", body: "public base v1\n", ct: "text/plain", cd: "inline" },
    {
      p: `${userPrefix}initial.txt`,
      body: Buffer.from([0, 1, 2, 3, 4, 5, 6, 7]),
      ct: "application/octet-stream",
      cd: 'attachment; filename="initial.bin"',
    },
    { p: `odd/with space-${seed}.txt`, body: "a path with spaces\n", ct: "text/plain; charset=utf-8", cd: "inline" },
    { p: `odd/odd name-${seed} #?.txt`, body: "odd ascii path\n", ct: "text/plain; charset=utf-8" },
    { p: `nested/a/b/c-${seed}.txt`, body: '{"nested":true}\n', ct: "application/json" },
    { p: `assets/file-${seed}.dat`, body: "linked file bytes\n", ct: "text/plain", cd: 'attachment; filename="linked.dat"' },
    { p: `delete/one-${seed}.txt`, body: "delete me\n", ct: "text/plain" },
    { p: `delete/two-${seed}.txt`, body: "delete me too\n", ct: "text/plain" },
  ];
  // A Unicode path travels in a JSON body (signed-upload-url), not an HTTP
  // header: the browser Headers API is ByteString-only and this server reads
  // header params as ASCII. Query and consume paths URL-encode it normally.
  // Keep the Unicode path in a prefix of its own: database collations can
  // order it differently against another odd/ path, which is an environment
  // difference, not a server contract.
  const unicodePath = `zz/ünïcode/文件-${seed}.txt`;

  const mutationPaths = Array.from({ length: 10 }, (_, i) =>
    pick([
      `${userPrefix}rand-${seed}-${i}.txt`,
      `tmp/rand-${seed}-${i}.bin`,
      `odd/rand ${seed} ${i} #.txt`,
      `nested/rand/${seed}/${i}.txt`,
      `${guestPrefix}rand-${seed}-${i}.txt`,
    ]),
  );
  const protectedPaths = new Set([
    "pub/base.txt",
    `${userPrefix}initial.txt`,
    `odd/with space-${seed}.txt`,
    unicodePath,
    `nested/a/b/c-${seed}.txt`,
    `assets/file-${seed}.dat`,
  ]);

  // ---- deterministic prefix -------------------------------------------
  op("schema", { attrs, label: "schema" });
  upload("admin", initial[0].p, initial[0].body, initial[0], "admin-upload-base");
  upload("client-user", initial[1].p, initial[1].body, initial[1], "client-upload-binary");
  upload("admin", initial[2].p, initial[2].body, initial[2], "admin-upload-space");
  upload("admin", initial[3].p, initial[3].body, initial[3], "admin-upload-odd-ascii");
  signedUpload("admin", "ADMIN", unicodePath, "unicode path body ✓\n", { ct: "text/plain; charset=utf-8" }, { label: "signed-unicode-path" });
  upload("admin", initial[4].p, initial[4].body, initial[4], "admin-upload-nested");
  upload("admin", initial[5].p, initial[5].body, initial[5], "admin-upload-linked");
  upload("admin", initial[6].p, initial[6].body, initial[6], "admin-upload-delete-one");
  upload("admin", initial[7].p, initial[7].body, initial[7], "admin-upload-delete-two");
  link(
    [
      { asset: assets[0], path: initial[0].p, label: "asset-base" },
      { asset: assets[1], path: initial[1].p, label: "asset-user" },
      { asset: assets[2], path: unicodePath, label: "asset-unicode" },
      { asset: assets[3], path: initial[5].p, label: "asset-linked" },
    ],
    "initial-links",
  );
  for (const s of ["ADMIN", "GUEST", "USER"]) {
    for (const sub of subscriptions[s]) {
      subscribe(s, sub.key, sub.q);
      held[s].add(sub.key);
    }
  }
  checkpoint("initial-subscriptions");
  adminQuery(qAll, { label: "admin-query-all" });
  socketQuery("ADMIN", qAll, "socket-query-all");
  adminQuery(qPub, { label: "admin-query-pub-fields" });
  socketQuery("GUEST", qPub, "socket-query-pub-fields");
  adminQuery(qAsset, { label: "admin-query-linked" });
  socketQuery("ADMIN", qAsset, "socket-query-linked");
  adminQuery(qWhereIn, { label: "admin-query-where-in" });
  adminQuery({ $files: { $: { order: { path: "desc" }, limit: 3, offset: 1 } } }, { label: "admin-query-page" });
  paginate("ADMIN", qPage, "paginate-all");
  download("admin", "ADMIN", initial[0].p, { label: "download-admin" });
  download("client", "USER", initial[1].p, { label: "download-client" });
  signedUpload("admin", "ADMIN", `signed/admin-${seed}.txt`, "signed admin body\n", {
    ct: "text/plain",
    cd: 'attachment; filename="signed-admin.txt"',
  }, { label: "signed-admin" });
  signedUpload("client", "USER", `${userPrefix}signed-${seed}.txt`, "signed client body\n", {
    ct: "text/markdown",
  }, { label: "signed-client" });
  upload("admin", initial[0].p, "public base v2 replaced\n", { ct: "text/markdown", cd: "inline" }, "overwrite-base");
  checkpoint("after-overwrite");
  upload("as-token", `${userPrefix}as-token.txt`, "as token\n", { ct: "text/plain" }, "as-token-allowed");
  upload("as-token", "denied/as-token.txt", "as token denied\n", { ct: "text/plain" }, "as-token-denied");
  upload("as-email", `${userPrefix}as-email.txt`, "as email\n", { ct: "text/plain" }, "as-email-allowed");
  upload("as-guest", `${guestPrefix}as-guest.txt`, "as guest\n", { ct: "text/plain" }, "as-guest-allowed");
  upload("as-guest", "denied/as-guest.txt", "as guest denied\n", { ct: "text/plain" }, "as-guest-denied");
  deleteOp("admin-one", [initial[6].p], { label: "delete-admin-one" });
  deleteOp("admin-one", [`missing/never-${seed}.txt`], { label: "delete-admin-missing" });
  deleteOp("admin-multi", [initial[7].p, `missing/bulk-${seed}.txt`], { label: "delete-admin-multi" });
  deleteOp("client", [`missing/client-${seed}.txt`], { session: "USER", label: "delete-client-missing" });
  download("admin", "ADMIN", initial[6].p, { label: "download-deleted-admin" });
  download("admin", "ADMIN", `missing/never-${seed}.txt`, { label: "download-missing-admin" });
  download("client", "USER", `missing/client-${seed}.txt`, { label: "download-missing-client" });
  op("consume-unknown", { id: "00000000-0000-4000-8000-00000000dead", label: "consume-unknown" });
  op("consume-unknown", { id: "not-a-uuid", label: "consume-malformed" });

  // Missing / blank parameter probes. These are the shapes the SDKs never
  // send but the routes define; both servers must agree on missing vs
  // malformed and on the hint.
  upload("admin", null, "no path", {}, "bad-admin-upload-missing-path");
  upload("admin", "", "blank path", {}, "bad-admin-upload-blank-path");
  upload("admin", "   ", "blank path spaces", {}, "bad-admin-upload-blank-path-spaces");
  upload("client-user", null, "no path", {}, "bad-client-upload-missing-path");
  upload("client-user", "   ", "blank path", {}, "bad-client-upload-blank-path");
  upload("client-user", null, "no app", {}, "bad-client-upload-missing-app-id", true);
  upload("admin", "of/missing-app.txt", "no app", {}, "bad-admin-upload-missing-app-id", true);
  upload("client-user", `${userPrefix}blank-disposition.txt`, "blank cd", { ct: "text/plain", cd: "" }, "bad-client-upload-blank-disposition");
  upload("admin", "denied/blank-disposition.txt", "blank cd denied", { ct: "text/plain", cd: "" }, "bad-admin-upload-blank-disposition-denied");
  deleteOp("admin-one", undefined, { label: "bad-admin-delete-missing-filename" });
  deleteOp("admin-one", ["   "], { label: "bad-admin-delete-blank-filename" });
  deleteOp("admin-multi", undefined, { label: "bad-admin-delete-multi-missing" });
  deleteOp("client", undefined, { session: "USER", label: "bad-client-delete-missing-filename" });
  deleteOp("client", ["   "], { session: "USER", label: "bad-client-delete-blank-filename" });
  signedUpload("admin", "ADMIN", null, "x", {}, { label: "bad-admin-signed-upload-missing" });
  signedUpload("admin", "ADMIN", "   ", "x", {}, { label: "bad-admin-signed-upload-blank" });
  signedUpload("client", "USER", "   ", "x", {}, { label: "bad-client-signed-upload-blank" });
  download("admin", "ADMIN", null, { label: "bad-admin-download-missing" });
  download("admin", "ADMIN", "   ", { label: "bad-admin-download-blank" });
  download("client", "USER", "   ", { label: "bad-client-download-blank" });
  checkpoint("after-prefix");

  // ---- seeded random rounds -------------------------------------------
  const contentTypeChoices = [undefined, "", "text/plain", "text/plain; charset=utf-8", "application/json", "null", "undefined", "image/svg+xml"];
  const dispositionChoices = [undefined, "", "inline", 'attachment; filename="a b.txt"', "attachment; filename*=UTF-8''unicode.txt"];
  const bodyFor = (i) => pick([`body ${i}\n`, `ünïcode ${i} ✓\n`, `{"i":${i}}\n`, Buffer.from([i % 256, (i * 7) % 256, 0, 255])]);
  const randomMeta = () => ({ ct: pick(contentTypeChoices), cd: pick(dispositionChoices) });

  for (let i = 0; i < rounds; i++) {
    const r = rand();
    if (r < 0.20) {
      const p = pick(mutationPaths);
      const route = pick(["admin", "client-user", "client-guest", "as-token", "as-email", "as-guest", "admin", "client-user"]);
      upload(route, p, bodyFor(i), randomMeta(), `upload-${i}`);
    } else if (r < 0.34) {
      const route = pick(["admin-one", "admin-multi", "client", "as-guest-token"]);
      const paths = Array.from({ length: route === "admin-multi" ? 2 : 1 }, () =>
        chance(0.25) ? `missing/rand-${seed}-${i}-${Math.floor(rand() * 100)}` : pick(mutationPaths),
      );
      deleteOp(route === "as-guest-token" ? "admin-one" : route, paths, {
        session: route === "client" ? pick(["USER", "GUEST"]) : undefined,
        impersonate: route === "as-guest-token" ? "guest" : undefined,
        label: `delete-${i}`,
      });
    } else if (r < 0.46) {
      const p = pick([...mutationPaths, "pub/base.txt", `missing/nope-${seed}-${i}`]);
      download(chance(0.5) ? "admin" : "client", pick(["ADMIN", "USER", "GUEST"]), p, { label: `download-${i}` });
    } else if (r < 0.56) {
      const q = pick([
        qAll,
        qAllDesc,
        qPub,
        qUser,
        qAsset,
        qTemp,
        qWhereIn,
        { $files: { $: { where: { path: { $like: "odd/%" } }, order: { path: "asc" } } } },
        { $files: { $: { fields: ["path", "url"] } } },
        { $files: { $: { fields: ["path", "location-id"] } } },
        { $files: { $: { where: { path: { $isNull: false } }, limit: 2, offset: i % 3 } } },
      ]);
      if (chance(0.5)) adminQuery(q, { label: `admin-query-${i}` });
      else socketQuery(pick(["ADMIN", "GUEST", "USER"]), q, `socket-query-${i}`);
    } else if (r < 0.64) {
      const q = pick([qAll, qAllDesc, qTemp, { $files: { $: { order: { path: "asc" }, limit: 1 + (i % 3) } } }]);
      paginate(pick(["ADMIN", "GUEST", "USER"]), q, `paginate-${i}`);
    } else if (r < 0.72) {
      const s = pick(["ADMIN", "GUEST", "USER"]);
      const choices = subscriptions[s].filter((sub) => !held[s].has(sub.key));
      if (choices.length && chance(0.7)) {
        const sub = pick(choices);
        subscribe(s, sub.key, sub.q);
        held[s].add(sub.key);
      } else {
        const keys = [...held[s]];
        if (keys.length) {
          const key = pick(keys);
          unsubscribe(s, key);
          held[s].delete(key);
        }
      }
    } else if (r < 0.80) {
      const p = pick([...protectedPaths]);
      const asset = assets[Math.floor(rand() * assets.length)];
      link([{ asset, path: p, label: `link-${i}` }], `link-${i}`);
    } else if (r < 0.89) {
      const kind = Math.floor(rand() * 6);
      if (kind === 0) upload("admin", "   ", "x", {}, `bad-random-blank-path-${i}`);
      else if (kind === 1) deleteOp("admin-one", ["   "], { label: `bad-random-blank-filename-${i}` });
      else if (kind === 2) download("client", "USER", null, { label: `bad-random-missing-download-${i}` });
      else if (kind === 3) signedUpload("client", "GUEST", "", "x", {}, { label: `bad-random-signed-blank-${i}` });
      else if (kind === 4) upload("client-guest", null, "x", {}, `bad-random-client-no-path-${i}`);
      else op("consume-unknown", { id: chance(0.5) ? "00000000-0000-4000-8000-00000000beef" : "nope", label: `consume-${i}` });
    } else {
      checkpoint(`random-${i}`);
    }
    if (i % 17 === 16) checkpoint(`periodic-${i}`);
  }

  // Final reads for every query shape and a last settled state.
  adminQuery(qAllDesc, { label: "final-admin-all" });
  socketQuery("ADMIN", qAll, "final-socket-all");
  adminQuery(qAsset, { label: "final-admin-linked" });
  socketQuery("USER", qUser, "final-socket-user");
  checkpoint("final");

  return { attrs, assets, users, rules, script, userPrefix, guestPrefix };
}

// ---------------------------------------------------------------------------
// response folding / normalization

const URL_SHAPES = new Set();
const errView = (res) => {
  const b = res.body ?? {};
  return normalize({
    status: res.status,
    type: b.type,
    message: b.message,
    hint: b.hint ?? null,
    keys: Object.keys(b).sort(),
  });
};
const plainView = (res) => (res.status === 200 ? normalize({ status: 200, body: res.body }) : errView(res));

function urlShape(url) {
  if (typeof url !== "string") return url;
  try {
    const u = new URL(url);
    const presigned = u.searchParams.has("X-Amz-Signature");
    URL_SHAPES.add(presigned ? "presigned" : "proxy");
    if (!presigned) return "<proxy:/storage/serve>";
    const segs = u.pathname.split("/").filter(Boolean);
    const key = segs.slice(-3).map((s, i) => (i === 1 ? "<bin>" : "<uuid>")).join("/");
    const params = {};
    for (const [k, v] of u.searchParams) {
      params[k] =
        k === "X-Amz-Signature"
          ? "<sig>"
          : k === "X-Amz-Date"
            ? /^\d{8}T000000Z$/.test(v)
              ? "<day-bucketed>"
              : v
            : k === "X-Amz-Credential"
              ? v.replace(/^[^/]+\/\d{8}\//, "<key>/<day>/")
              : v;
    }
    return { pathTail: key, params };
  } catch {
    return normalize(url);
  }
}

function uploadUrlShape(url) {
  if (typeof url !== "string") return url;
  try {
    return new URL(url).pathname.replace(UUID_RE, "<upload-id>");
  } catch {
    return normalize(url);
  }
}

async function fetched(url) {
  if (typeof url !== "string") return { url };
  const res = await fetch(url);
  const body = Buffer.from(await res.arrayBuffer()).toString("base64");
  return {
    status: res.status,
    body: res.status === 200 ? body : "<error>",
    contentType: res.headers.get("content-type"),
    contentDisposition: res.headers.get("content-disposition"),
    cacheControl: res.headers.get("cache-control"),
  };
}

function stripUrlShapes(v) {
  if (Array.isArray(v)) return v.map(stripUrlShapes);
  if (v !== null && typeof v === "object") {
    const out = {};
    for (const [k, x] of Object.entries(v)) if (k !== "urlShape") out[k] = stripUrlShapes(x);
    return out;
  }
  return v;
}

const rootKey = (q) => Object.keys(q ?? {}).find((k) => !k.startsWith("$$"));
const hasOrder = (q) => Object.values(q ?? {}).some((form) => Object.keys(form?.$?.order ?? {}).length > 0);

function fileKey(files, value) {
  if (typeof value !== "string") return null;
  return files.byId.get(value) ?? null;
}

function normScalar(files, v, key = null) {
  const f = fileKey(files, v);
  if (f) return `<file:${f}>`;
  if (typeof v === "string" && ISO_RE.test(v)) return "<ts>";
  if (typeof v === "number" && (key === "created_at" || key === "createdAt" || v > 1e12)) return "<ts>";
  return normalize(v, key);
}

async function normEntityValue(files, value, key = null) {
  if (typeof value === "string") {
    const f = fileKey(files, value);
    if (f) return `<file:${f}>`;
    if (URL_RE.test(value) && key === "url") {
      return { fetched: await fetched(value), urlShape: urlShape(value) };
    }
    return normScalar(files, value, key);
  }
  if (Array.isArray(value)) {
    const out = [];
    for (const v of value) out.push(await normEntityValue(files, v, key));
    return out;
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = await normEntityValue(files, v, k);
    return out;
  }
  return normScalar(files, value, key);
}

async function projectAdminTree(value, files, sortArray = false, depth = 0) {
  if (Array.isArray(value)) {
    const out = [];
    for (const v of value) out.push(await normEntityValue(files, v));
    if (sortArray) out.sort((a, b) => (canon(a) < canon(b) ? -1 : 1));
    return out;
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (URL_RE.test(String(v ?? ""))) out[k] = await normEntityValue(files, v, k);
      else out[k] = await projectAdminTree(v, files, depth === 0 ? sortArray : true, depth + 1);
    }
    return out;
  }
  return normEntityValue(files, value);
}

async function adminTreeView(res, q, files) {
  if (res.status !== 200) return errView(res);
  return { status: 200, body: await projectAdminTree(res.body, files, !hasOrder(q)) };
}

function normalizeCursor(files, cursor) {
  if (!Array.isArray(cursor)) return normScalar(files, cursor);
  return cursor.map((part, i) => {
    if (i === 0) return normScalar(files, part);
    if (i === 1) return normalize(part);
    if (i === 2) return normScalar(files, part);
    return normScalar(files, part);
  });
}

async function projectSocketResult(result, q, files) {
  const triples = [];
  const walk = async (nodes) => {
    for (const node of nodes ?? []) {
      for (const rows of node?.data?.["datalog-result"]?.["join-rows"] ?? []) {
        for (const t of rows) {
          const val =
            typeof t[2] === "string" && URL_RE.test(t[2])
              ? { fetched: await fetched(t[2]), urlShape: urlShape(t[2]) }
              : await normEntityValue(files, t[2]);
          triples.push([normScalar(files, t[0]), normalize(t[1]), val, normScalar(files, t[3])]);
        }
      }
      await walk(node?.["child-nodes"]);
    }
  };
  await walk(result);
  const deduped = [...new Map(triples.map((t) => [canon(t), t])).values()];
  deduped.sort((x, y) => (canon(x) < canon(y) ? -1 : 1));
  const ns = rootKey(q);
  const info = result?.[0]?.data?.["page-info"]?.[ns] ?? null;
  const pageInfo = info
    ? Object.fromEntries(
        Object.entries(info).map(([k, v]) => [
          k,
          k === "start-cursor" || k === "end-cursor" ? normalizeCursor(files, v) : normScalar(files, v),
        ]),
      )
    : null;
  return {
    triples: deduped,
    pageInfo,
    aggregate: normalize(result?.[0]?.data?.aggregate ?? null),
  };
}

function rawPageInfo(result, q) {
  return result?.[0]?.data?.["page-info"]?.[rootKey(q)] ?? null;
}

const errSocket = (m) => ({ status: m.status, type: m.type, message: m.message, hint: normalize(m.hint ?? null) });

// ---------------------------------------------------------------------------
// one run of the plan against one server

async function call(base, method, p, { headers = {}, body, json } = {}) {
  const h = { ...headers };
  if (json !== undefined) h["content-type"] = "application/json";
  const res = await fetch(base + p, {
    method,
    headers: h,
    body: json !== undefined ? JSON.stringify(json) : body,
  });
  const text = await res.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = { "<non-json>": text.slice(0, 200) };
  }
  return { status: res.status, body: parsed };
}

async function runOn(serverName, plan) {
  const { url, db, appId } = SERVERS[serverName];
  URL_SHAPES.clear();
  psql(
    db,
    `INSERT INTO rules (app_id, code) VALUES ('${appId}', $rules$${JSON.stringify(plan.rules)}$rules$::jsonb) ON CONFLICT (app_id) DO UPDATE SET code = EXCLUDED.code`,
  );
  await sleep(RULES_SETTLE_MS);

  const adminHeaders = { "app-id": appId, authorization: `Bearer ${adminToken}` };
  const mintUser = async (user) => {
    const res = await call(url, "POST", "/admin/refresh_tokens", {
      headers: { "content-type": "application/json", ...adminHeaders },
      json: { id: user.id, email: user.email },
    });
    if (res.status !== 200 || res.body?.user?.id !== user.id || !res.body?.user?.refresh_token) {
      throw new Error(`[${serverName}] mint ${user.email} failed: ${res.status} ${JSON.stringify(res.body).slice(0, 400)}`);
    }
    return { id: user.id, email: user.email, token: res.body.user.refresh_token };
  };
  const users = {};
  for (const [name, user] of Object.entries(plan.users)) users[name] = await mintUser(user);
  const userHeaders = (name) => ({ "app-id": appId, app_id: appId, authorization: `Bearer ${users[name].token}` });
  const guestHeaders = { "app-id": appId, app_id: appId };
  const impersonationHeaders = (which) => {
    if (which === "guest") return { "as-guest": "true" };
    if (which === "email") return { "as-email": users.U1.email };
    if (which === "token") return { "as-token": users.U1.token };
    return {};
  };

  const files = { byId: new Map(), byPath: new Map() };
  const remember = (p, body) => {
    const id = body?.data?.id;
    if (typeof id !== "string") return;
    const key = String(p ?? "").trim();
    const prev = files.byPath.get(key);
    if (prev && prev !== id) files.byId.delete(prev);
    files.byPath.set(key, id);
    files.byId.set(id, key);
  };
  const forget = (p) => {
    const key = String(p ?? "").trim();
    const id = files.byPath.get(key);
    if (!id) return;
    files.byPath.delete(key);
    files.byId.delete(id);
  };

  const uploadHeaders = (route, p, meta, omitAppId = false) => {
    const headers = {};
    if (route === "admin") Object.assign(headers, adminHeaders);
    else if (route === "client-user") Object.assign(headers, userHeaders("U1"));
    else if (route === "client-guest") Object.assign(headers, guestHeaders);
    else if (route === "as-token") Object.assign(headers, adminHeaders, impersonationHeaders("token"));
    else if (route === "as-email") Object.assign(headers, adminHeaders, impersonationHeaders("email"));
    else if (route === "as-guest") Object.assign(headers, adminHeaders, impersonationHeaders("guest"));
    if (omitAppId) {
      delete headers["app-id"];
      delete headers["app_id"];
    }
    if (p !== null && p !== undefined) headers.path = p;
    if (meta.ct !== undefined) headers["content-type"] = meta.ct;
    if (meta.cd !== undefined) headers["content-disposition"] = meta.cd;
    return headers;
  };

  const doUpload = async (op) => {
    const route = op.route;
    const qs = op.omitAppId ? "" : `?app_id=${appId}`;
    const headers = uploadHeaders(route, op.path, op.meta, op.omitAppId);
    const res = await call(url, "PUT", `/admin/storage/upload${qs}`, { headers, body: op.body });
    if (res.status === 200) remember(op.path, res.body);
    return { kind: "upload", route, path: op.path, ...plainView(res) };
  };

  const doClientUpload = async (op) => {
    const qs = op.omitAppId ? "" : `?app_id=${appId}`;
    const headers = uploadHeaders(op.route, op.path, op.meta, op.omitAppId);
    const res = await call(url, "PUT", `/storage/upload${qs}`, { headers, body: op.body });
    if (res.status === 200) remember(op.path, res.body);
    return { kind: "upload", route: op.route, path: op.path, ...plainView(res) };
  };

  const doDelete = async (op) => {
    const route = op.route;
    const session = op.session ?? "USER";
    const imp = op.impersonate ? impersonationHeaders(op.impersonate) : {};
    let res;
    if (route === "admin-one") {
      const p = op.paths?.[0];
      const qs = new URLSearchParams();
      qs.set("app_id", appId);
      if (p !== undefined && p !== null) qs.set("filename", p);
      res = await call(url, "DELETE", `/admin/storage/files?${qs}`, { headers: { ...adminHeaders, ...imp } });
    } else if (route === "admin-multi") {
      const json = op.paths === undefined ? {} : { filenames: op.paths };
      res = await call(url, "POST", `/admin/storage/files/delete?app_id=${appId}`, {
        headers: { ...adminHeaders, ...imp },
        json,
      });
    } else {
      const p = op.paths?.[0];
      const qs = new URLSearchParams();
      qs.set("app_id", appId);
      if (p !== undefined && p !== null) qs.set("filename", p);
      const headers = session === "USER" ? userHeaders("U1") : guestHeaders;
      res = await call(url, "DELETE", `/storage/files?${qs}`, { headers: { ...headers, ...imp } });
    }
    if (res.status === 200) {
      const ids = res.body?.data?.ids;
      if (Array.isArray(ids)) {
        for (const p of op.paths ?? []) if (ids.length) forget(p);
      } else if (op.paths?.length) {
        forget(op.paths[0]);
      }
    }
    return { kind: "delete", route, paths: op.paths ?? null, ...plainView(res) };
  };

  const doSignedUpload = async (op) => {
    const route = op.route;
    let create;
    if (route === "admin") {
      create = await call(url, "POST", `/admin/storage/signed-upload-url?app_id=${appId}`, {
        headers: adminHeaders,
        json: op.path === null || op.path === undefined ? {} : { filename: op.path },
      });
    } else {
      const body = { app_id: appId };
      if (op.path !== null && op.path !== undefined) body[op.pathKey ?? "path"] = op.path;
      const headers = op.session === "GUEST" ? guestHeaders : userHeaders("U1");
      create = await call(url, "POST", `/storage/signed-upload-url`, { headers, json: body });
    }
    const out = { kind: "signed-upload", route, path: op.path, create: { status: create.status } };
    if (create.status !== 200) {
      out.create = errView(create);
      return out;
    }
    out.create = { status: 200, urlPath: uploadUrlShape(create.body.data) };
    const consumeUrl = create.body.data;
    const headers = {};
    if (op.meta.ct !== undefined) headers["content-type"] = op.meta.ct;
    const consume = await call(url, "PUT", new URL(consumeUrl).pathname, { headers, body: op.body });
    if (consume.status === 200) remember(op.path, consume.body);
    const again = await call(url, "PUT", new URL(consumeUrl).pathname, { headers, body: op.body });
    out.consume = plainView(consume);
    out.consumeAgain = errView(again);
    return out;
  };

  const doDownload = async (op) => {
    let res;
    if (op.route === "admin") {
      const qs = new URLSearchParams();
      qs.set("app_id", appId);
      if (op.path !== null && op.path !== undefined) qs.set("filename", op.path);
      res = await call(url, "GET", `/admin/storage/signed-download-url?${qs}`, { headers: adminHeaders });
    } else {
      const qs = new URLSearchParams();
      qs.set("app_id", appId);
      if (op.path !== null && op.path !== undefined) qs.set("filename", op.path);
      const headers = op.session === "USER" ? userHeaders("U1") : guestHeaders;
      res = await call(url, "GET", `/storage/signed-download-url?${qs}`, { headers });
    }
    if (res.status !== 200) return { kind: "download", route: op.route, path: op.path, ...errView(res) };
    const dl = res.body?.data;
    if (dl === null || dl === undefined) return { kind: "download", route: op.route, path: op.path, status: 200, url: null };
    return {
      kind: "download",
      route: op.route,
      path: op.path,
      status: 200,
      url: { fetched: await fetched(dl), urlShape: urlShape(dl) },
    };
  };

  const doConsumeUnknown = async (op) => {
    const res = await call(url, "PUT", `/storage/${op.id}/consume-upload-url`, { body: Buffer.from("x") });
    return { kind: "consume-unknown", id: op.id, ...plainView(res) };
  };

  const initMessage = (session, ceid = uuid()) => {
    const msg = {
      "client-event-id": ceid,
      op: "init",
      "app-id": appId,
      versions: { "@instantdb/core": "v0.22.76" },
    };
    if (session === "ADMIN") msg["__admin-token"] = adminToken;
    if (session === "USER") msg["refresh-token"] = users.U1.token;
    return msg;
  };
  const openConn = async (session, tag = "") => {
    const c = connect(url, appId, `${serverName}:storage:${session}${tag}`);
    await c.open;
    c.takeNewFrames();
    c.send(initMessage(session));
    const reply = await c.waitFor((m) => m.op === "init-ok" || m.op === "error");
    if (reply.op !== "init-ok") throw new Error(`[${serverName}] init ${session}${tag} failed: ${JSON.stringify(reply).slice(0, 400)}`);
    c.takeNewFrames();
    return c;
  };

  const conns = {};
  for (const session of ["ADMIN", "GUEST", "USER"]) conns[session] = await openConn(session);
  const allConns = Object.values(conns);
  const subs = Object.fromEntries(["ADMIN", "GUEST", "USER"].map((s) => [s, new Map()]));
  const subQ = Object.fromEntries(["ADMIN", "GUEST", "USER"].map((s) => [s, new Map()]));
  // plan subscription key (e.g. "user-all") -> query, so unsubscribe removes
  // the query the plan named rather than looking a plan key up in subQ (whose
  // keys are canonical query strings).
  const planSubQ = Object.fromEntries(["ADMIN", "GUEST", "USER"].map((s) => [s, new Map()]));
  const refreshErrors = Object.fromEntries(["ADMIN", "GUEST", "USER"].map((s) => [s, []]));
  const cursors = Object.fromEntries(["ADMIN", "GUEST", "USER"].map((s) => [s, conns[s].frames.length]));
  const held = Object.fromEntries(["ADMIN", "GUEST", "USER"].map((s) => [s, new Set()]));
  let reregistered = 0;

  const fold = async () => {
    for (const s of ["ADMIN", "GUEST", "USER"]) {
      const frames = conns[s].frames.slice(cursors[s]);
      cursors[s] = conns[s].frames.length;
      for (const m of frames) {
        if (m.op === "add-query-ok") {
          const k = canon(normalize(m.q));
          if (subs[s].has(k)) subs[s].set(k, await projectSocketResult(m.result, m.q, files));
        } else if (m.op === "refresh-ok") {
          for (const comp of m.computations ?? []) {
            const k = canon(normalize(comp["instaql-query"]));
            if (subs[s].has(k)) subs[s].set(k, await projectSocketResult(comp["instaql-result"], comp["instaql-query"], files));
          }
        } else if (m.op === "error" && m["original-event"]?.op === "refresh") {
          refreshErrors[s].push(errSocket(m));
        }
      }
    }
  };

  const snapshot = async () => {
    await fold();
    const out = {};
    for (const s of ["ADMIN", "GUEST", "USER"]) {
      out[s] = {
        subs: Object.fromEntries([...subs[s].entries()].sort(([a], [b]) => (a < b ? -1 : 1))),
        refreshErrors: refreshErrors[s].splice(0),
      };
    }
    return out;
  };

  const oneShot = async (session, q) => {
    const c = await openConn(session, ":query");
    try {
      const ceid = c.send({ "client-event-id": uuid(), op: "add-query", q });
      const r = await c.waitFor((m) => ["add-query-ok", "error"].includes(m.op) && m["client-event-id"] === ceid, 20000);
      if (r.op === "add-query-ok") {
        return {
          result: await projectSocketResult(r.result, q, files),
          rawInfo: rawPageInfo(r.result, q),
        };
      }
      return { error: errSocket(r) };
    } finally {
      c.close();
    }
  };

  const variant = (q) =>
    Object.fromEntries(
      Object.entries(q).map(([k, form]) => {
        if (k.startsWith("$$")) return [k, form];
        const opts = { ...(form?.$ ?? {}) };
        let cond = { and: [{ id: { $isNull: false } }, { id: { $ne: uuid() } }] };
        // A fresh $files answer also carries an impossible path: legacy
        // shares datalog results across sessions, and this makes the probe a
        // different query even when only the id condition was cached.
        if (k === "$files") cond = { and: [cond, { path: { $ne: uuid() } }] };
        opts.where = opts.where ? { and: [opts.where, cond] } : cond;
        return [k, { ...form, $: opts }];
      }),
    );

  const probeFresh = async (session, q) => {
    const r = await oneShot(session, variant(q));
    return r.error ?? r.result;
  };

  const addQuery = async (session, q) => {
    const key = canon(normalize(q));
    const ask = async () => {
      await fold();
      subs[session].set(key, null);
      subQ[session].set(key, q);
      const ceid = conns[session].send({ "client-event-id": uuid(), op: "add-query", q });
      const r = await conns[session].waitFor(
        (m) => ["add-query-ok", "add-query-exists", "error"].includes(m.op) && m["client-event-id"] === ceid,
        20000,
      );
      if (r.op === "add-query-ok") {
        subs[session].set(key, await projectSocketResult(r.result, q, files));
        held[session].add(key);
        return { ok: r.op };
      }
      subs[session].delete(key);
      subQ[session].delete(key);
      return r.op === "error" ? { error: errSocket(r) } : { ok: r.op };
    };
    let r = await ask();
    for (let attempt = 0; attempt < 3 && r.ok === "add-query-exists" && serverName === "legacy" && !held[session].has(key); attempt++) {
      reregistered++;
      await removeQuery(session, q);
      r = await ask();
    }
    return r;
  };
  const removeQuery = async (session, q) => {
    await fold();
    const key = canon(normalize(q));
    subs[session].delete(key);
    subQ[session].delete(key);
    held[session].delete(key);
    const ceid = conns[session].send({ "client-event-id": uuid(), op: "remove-query", q });
    const r = await conns[session].waitFor(
      (m) => ["remove-query-ok", "error"].includes(m.op) && m["client-event-id"] === ceid,
      20000,
    );
    return r.op === "remove-query-ok" ? { ok: true } : { error: errSocket(r) };
  };
  const removeKey = async (session, key) => {
    const q = planSubQ[session].get(key);
    if (!q) return { ok: false };
    planSubQ[session].delete(key);
    return removeQuery(session, q);
  };

  const adminCall = async (route, body, which = null) => {
    const headers = { "content-type": "application/json", ...adminHeaders, ...impersonationHeaders(which) };
    return call(url, "POST", `${route}?app_id=${appId}`, { headers, json: body });
  };

  const outcomes = [];
  const fresh = {};
  const doLink = async (op) => {
    const steps = [];
    for (const l of op.links) {
      const id = files.byPath.get(String(l.path).trim());
      const fields = { id: l.asset, label: l.label };
      if (id) fields.file = id;
      steps.push(["update", "sf", l.asset, fields]);
    }
    const res = await adminCall("/admin/transact", { steps });
    return res.status === 200 ? { kind: "link", status: 200, ok: true } : { kind: "link", status: res.status, ...errView(res) };
  };

  for (const [i, op] of plan.script.entries()) {
    let out;
    if (op.kind === "schema") {
      const steps = [
        [
          "add-attr",
          {
            id: op.attrs.id,
            "forward-identity": [op.attrs.id, "sf", "id"],
            "value-type": "blob",
            cardinality: "one",
            "unique?": true,
            "index?": true,
          },
        ],
        [
          "add-attr",
          {
            id: op.attrs.label,
            "forward-identity": [op.attrs.label, "sf", "label"],
            "value-type": "blob",
            cardinality: "one",
            "unique?": false,
            "index?": true,
            "checked-data-type": "string",
          },
        ],
        [
          "add-attr",
          {
            id: op.attrs.file,
            "forward-identity": [op.attrs.file, "sf", "file"],
            "reverse-identity": [op.attrs.fileRev, "$files", "sfAssets"],
            "value-type": "ref",
            cardinality: "one",
            "unique?": false,
            "index?": true,
          },
        ],
      ];
      const res = await adminCall("/admin/transact", { steps });
      if (res.status !== 200) throw new Error(`[${serverName}] schema setup failed: ${JSON.stringify(res.body).slice(0, 500)}`);
      out = { kind: "schema", status: 200, ok: true };
    } else if (op.kind === "upload") {
      out = op.route === "admin" || op.route?.startsWith("as-") ? await doUpload(op) : await doClientUpload(op);
    } else if (op.kind === "delete") {
      out = await doDelete(op);
    } else if (op.kind === "signed-upload") {
      out = await doSignedUpload(op);
    } else if (op.kind === "download") {
      out = await doDownload(op);
    } else if (op.kind === "consume-unknown") {
      out = await doConsumeUnknown(op);
    } else if (op.kind === "admin-query") {
      const which = op.impersonate ?? null;
      const res = await adminCall("/admin/query", { query: op.q }, which);
      out = { kind: "admin-query", q: op.q, label: op.label, ...(await adminTreeView(res, op.q, files)) };
    } else if (op.kind === "socket-query") {
      const r = await oneShot(op.session, op.q);
      out = {
        kind: "socket-query",
        session: op.session,
        q: op.q,
        label: op.label,
        ...(r.error ? { error: r.error } : { result: r.result }),
      };
    } else if (op.kind === "paginate") {
      const pages = [];
      let cursor = null;
      for (let pageNo = 0; pageNo < 8; pageNo++) {
        const q = JSON.parse(JSON.stringify(op.q));
        const form = q[rootKey(q)];
        form.$ = { ...(form.$ ?? {}) };
        if (cursor) form.$.after = cursor;
        const r = await oneShot(op.session, q);
        if (r.error) {
          pages.push({ error: r.error });
          break;
        }
        pages.push({ result: r.result });
        if (!r.rawInfo?.["has-next-page?"] || !r.rawInfo?.["end-cursor"]) break;
        cursor = r.rawInfo["end-cursor"];
      }
      out = { kind: "paginate", session: op.session, q: op.q, label: op.label, pages };
    } else if (op.kind === "subscribe") {
      planSubQ[op.session].set(op.key, op.q);
      const r = await addQuery(op.session, op.q);
      out = { kind: "subscribe", session: op.session, key: op.key, ...r };
    } else if (op.kind === "unsubscribe") {
      const r = await removeKey(op.session, op.key);
      out = { kind: "unsubscribe", session: op.session, key: op.key, ...r };
    } else if (op.kind === "checkpoint") {
      await settle(allConns, CHECKPOINT_QUIET_MS);
      let state = await snapshot();
      const probe = async () => {
        const answers = {};
        let moved = false;
        for (const s of ["ADMIN", "GUEST", "USER"]) {
          answers[s] = {};
          for (const [key, q] of subQ[s]) {
            if (!subs[s].has(key)) continue;
            let answer = await probeFresh(s, q);
            if (serverName === "legacy" && canon(answer) !== canon(state[s].subs[key])) {
              await sleep(LEGACY_RESETTLE_MS);
              answer = await probeFresh(s, q);
            }
            if (canon(answer) !== canon(state[s].subs[key])) moved = true;
            answers[s][key] = answer;
          }
        }
        return { answers, moved };
      };
      let probed = await probe();
      for (let attempt = 0; attempt < 3 && probed.moved; attempt++) {
        // A refresh landed while the probes were running, or the server is
        // still invalidating under load. Let the sessions catch up, then
        // compare the settled state (the same rule fuzz-schema uses).
        await settle(allConns, CHECKPOINT_RESETTLE_MS);
        state = await snapshot();
        probed = await probe();
      }
      fresh[i] = probed.answers;
      out = { kind: "checkpoint", label: op.label, state };
    } else if (op.kind === "link") {
      out = await doLink(op);
    } else {
      throw new Error(`unknown plan op ${op.kind}`);
    }
    outcomes.push({ i, ...out });
    if (process.env.DUMP) console.log(`[${serverName}] ${i} ${op.kind}`, JSON.stringify(out).slice(0, 1500));
  }
  await settle(allConns, CHECKPOINT_QUIET_MS);
  await fold();
  for (const c of allConns) c.close();
  console.log(`[${serverName}] ${reregistered} legacy re-registered removed queries, ${URL_SHAPES.size} download url shapes`);
  return { outcomes, fresh };
}

// ---------------------------------------------------------------------------
// run both servers and compare

const plan = buildPlan();
console.log(`fuzz-storage: seed=${seed} rounds=${rounds} ops=${plan.script.length}`);
console.log(`rules: ${JSON.stringify(plan.rules)}`);
console.log("running against legacy…");
const legacy = await runOn("legacy", plan);
console.log("running against rust…");
const rust = await runOn("rust", plan);
const rustPresigns = URL_SHAPES.has("presigned") && !URL_SHAPES.has("proxy");
console.log(rustPresigns ? "rust presigns S3 urls: url shapes are compared" : "rust proxies downloads through /storage/serve: url shapes are not compared (documented divergence)");

const allowlist = loadAllowlist(path.join(here, "allowed-divergences.json"), { prefixes: DIVERGENCE_PREFIXES.storagefuzz });
const same = (a, b) => a !== undefined && b !== undefined && canon(a) === canon(b);
function legacyStale(l, r, index) {
  if (!l || !r || l.i !== r.i || l.kind !== "checkpoint" || r.kind !== "checkpoint") return false;
  const lf = legacy.fresh[index];
  const rf = rust.fresh[index];
  let stale = false;
  for (const s of ["ADMIN", "GUEST", "USER"]) {
    const ls = l.state?.[s]?.subs ?? {};
    const rs = r.state?.[s]?.subs ?? {};
    for (const key of new Set([...Object.keys(ls), ...Object.keys(rs)])) {
      if (canon(ls[key]) === canon(rs[key])) continue;
      stale = true;
      if (!same(lf?.[s]?.[key], rs[key]) || !same(rf?.[s]?.[key], rs[key])) return false;
    }
    if (canon(l.state?.[s]?.refreshErrors) !== canon(r.state?.[s]?.refreshErrors)) return false;
  }
  return stale;
}

const outcomePath = (op, fallback) => {
  const label = op?.label ?? op?.key ?? op?.session ?? "";
  return `storagefuzz:${op?.kind ?? "unknown"}/${label || fallback}`;
};
// The full checkpoint states are too large for the generic first-difference
// dump (the ADMIN subscriptions alone fill it), so name the rows that differ
// per subscription: usually one side's refresh is missing a just-uploaded
// file.
function printCheckpointDiff(lv, rv) {
  for (const s of ["ADMIN", "GUEST", "USER"]) {
    const ls = lv.state?.[s]?.subs ?? {};
    const rs = rv.state?.[s]?.subs ?? {};
    for (const key of new Set([...Object.keys(ls), ...Object.keys(rs)])) {
      const left = ls[key];
      const right = rs[key];
      if (canon(left) === canon(right)) continue;
      const lt = left?.triples ?? [];
      const rt = right?.triples ?? [];
      const lset = new Map(lt.map((t) => [canon(t), t]));
      const rset = new Map(rt.map((t) => [canon(t), t]));
      const onlyL = [...lset].filter(([k]) => !rset.has(k)).map(([, t]) => t);
      const onlyR = [...rset].filter(([k]) => !lset.has(k)).map(([, t]) => t);
      console.error(
        `  ${s} ${key}: triples legacy=${lt.length} rust=${rt.length}; only legacy=${onlyL.length} only rust=${onlyR.length}`,
      );
      if (onlyL.length) console.error(`    only legacy: ${JSON.stringify(onlyL.slice(0, 24))}`);
      if (onlyR.length) console.error(`    only rust:   ${JSON.stringify(onlyR.slice(0, 24))}`);
      if (canon(left?.pageInfo) !== canon(right?.pageInfo)) {
        console.error(`    pageInfo legacy=${JSON.stringify(left?.pageInfo)} rust=${JSON.stringify(right?.pageInfo)}`);
      }
      if (canon(left?.aggregate) !== canon(right?.aggregate)) {
        console.error(`    aggregate legacy=${JSON.stringify(left?.aggregate)} rust=${JSON.stringify(right?.aggregate)}`);
      }
    }
    if (canon(lv.state?.[s]?.refreshErrors) !== canon(rv.state?.[s]?.refreshErrors)) {
      console.error(
        `  ${s} refreshErrors legacy=${JSON.stringify(lv.state?.[s]?.refreshErrors)} rust=${JSON.stringify(rv.state?.[s]?.refreshErrors)}`,
      );
    }
  }
}
let allowed = 0;
let mismatches = 0;
let staleLegacy = 0;
const n = Math.max(legacy.outcomes.length, rust.outcomes.length);
for (let i = 0; i < n; i++) {
  const l = legacy.outcomes[i];
  const r = rust.outcomes[i];
  if (l?.i !== r?.i || l?.kind !== r?.kind) {
    mismatches++;
    console.error(`OUTCOME MISMATCH at ${i}: legacy ${JSON.stringify(l?.kind)} rust ${JSON.stringify(r?.kind)}`);
    continue;
  }
  const lv = rustPresigns ? l : stripUrlShapes(l);
  const rv = rustPresigns ? r : stripUrlShapes(r);
  if (canon(lv) === canon(rv)) continue;
  const op = plan.script[l.i];
  const p = outcomePath(op, l.i);
  if (legacyStale(l, r, l.i)) {
    staleLegacy++;
    console.log(`legacy session state was stale at ${p}; a fresh legacy probe agreed with rust`);
    continue;
  }
  const verdict = allowlist.check(p, lv, rv);
  if (verdict.entry) {
    allowed++;
    console.log(`[ALLOWED] ${p}: ${verdict.differed.join(", ") || "(whole value)"}`);
    continue;
  }
  mismatches++;
  if (mismatches <= 6 && op?.kind === "checkpoint") printCheckpointDiff(lv, rv);
  if (mismatches > 12) continue;
  const fd = firstDifference(lv, rv);
  console.error(`DIVERGENCE ${p} (${op?.kind}/${op?.label ?? op?.path ?? ""})`);
  console.error(`  at ${fd?.p || "(whole value)"}:`);
  console.error(`    legacy ${JSON.stringify(fd?.a)?.slice(0, 1500)}`);
  console.error(`    rust   ${JSON.stringify(fd?.b)?.slice(0, 1500)}`);
  console.error(`  legacy: ${JSON.stringify(lv)?.slice(0, 2500)}`);
  console.error(`  rust:   ${JSON.stringify(rv)?.slice(0, 2500)}`);
  for (const line of describeAllowVerdict(verdict)) console.error(line);
}

// A seed that never uploaded, deleted, queried data, or checked a
// subscription would compare nothing meaningful.
const count = (side, pred) => side.outcomes.filter(pred).length;
const uploadsOk = (side) => count(side, (o) => o.kind === "upload" && o.status === 200);
const deletesOk = (side) => count(side, (o) => o.kind === "delete" && o.status === 200);
const downloadsOk = (side) => count(side, (o) => o.kind === "download" && o.status === 200 && o.url?.fetched?.status === 200);
const queriesWithData = (side) =>
  count(
    side,
    (o) =>
      (o.kind === "admin-query" && Array.isArray(o.body?.$files) && o.body.$files.length > 0) ||
      (o.kind === "socket-query" && (o.result?.triples?.length ?? 0) > 0) ||
      (o.kind === "paginate" && (o.pages ?? []).some((p) => (p.result?.triples?.length ?? 0) > 0)),
  );
const checkpoints = count(legacy, (o) => o.kind === "checkpoint");
const floorOk =
  uploadsOk(legacy) > 5 &&
  uploadsOk(rust) > 5 &&
  deletesOk(legacy) > 0 &&
  deletesOk(rust) > 0 &&
  downloadsOk(legacy) > 0 &&
  downloadsOk(rust) > 0 &&
  queriesWithData(legacy) > 0 &&
  queriesWithData(rust) > 0 &&
  checkpoints > 0;
console.log(
  `floor: legacy uploads=${uploadsOk(legacy)} deletes=${deletesOk(legacy)} downloads=${downloadsOk(legacy)} queries-with-data=${queriesWithData(legacy)}; rust uploads=${uploadsOk(rust)} deletes=${deletesOk(rust)} downloads=${downloadsOk(rust)} queries-with-data=${queriesWithData(rust)}; checkpoints=${checkpoints}`,
);
if (!floorOk) console.error("FUZZ-STORAGE FLOOR NOT MET");

const stale = allowlist.reportStale();
if (mismatches || stale || !floorOk) {
  console.error(`FUZZ-STORAGE FAILED: ${mismatches} mismatches, ${stale} stale allowlist entries, floor ${floorOk ? "ok" : "not met"} (seed ${seed})`);
  process.exit(1);
}
console.log(
  `FUZZ-STORAGE PASSED: ${plan.script.length} ops, ${allowed} allowed differences, ${staleLegacy} stale legacy sessions, ${uploadsOk(legacy)} uploads, ${queriesWithData(legacy)} data queries (seed ${seed})`,
);
