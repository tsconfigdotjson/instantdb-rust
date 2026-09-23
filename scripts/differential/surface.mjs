// The legacy server's public surface, derived mechanically from the vendored
// source so parity coverage can be counted against it instead of against a
// hand-written table:
//
//   http:<METHOD> <path>   every route in a `defroutes` (compojure) table
//   ws:<op>                every op `handle-event` dispatches (reactive/session.clj)
//   tx:<op>                every tx-step op of the `::tx-step` spec (db/transaction.clj)
//   iq:<option>            every `$` option / where operator InstaQL accepts (db/instaql.clj)
//   cel:<overload>         every custom CEL overload legacy registers (db/cel.clj)
//   err:<type>             every `::type` the exception namespace can throw (util/exception.clj)
//
// `node surface.mjs`          prints the manifest
// `node surface.mjs --write`  rewrites surface.json
// `node surface.mjs --check`  fails when surface.json is stale (CI: the vendored
//                             legacy source moved without the harness noticing)

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const legacy = path.resolve(here, "../../LEGACY/server/src/instant");
const read = (rel) => fs.readFileSync(path.join(legacy, rel), "utf8");

// routes: every file with a `defroutes` table that core.clj mounts (core.clj:196-208);
// demo/mma/stripe/health are dev or hosted-billing routes and are listed but
// tagged so coverage can exclude them; core.clj's own three routes (`GET /`,
// the Stripe and Honeycomb receivers) are tagged `core` for the same reason
const ROUTE_FILES = {
  "core.clj": "core",
  "dash/routes.clj": "dash",
  "runtime/routes.clj": "runtime",
  "admin/routes.clj": "admin",
  "superadmin/routes.clj": "superadmin",
  "storage/routes.clj": "storage",
  "oauth_apps/routes.clj": "platform",
  "webhook_routes.clj": "webhooks",
  "health.clj": "health",
  "demo_routes.clj": "demo",
  "mma_example.clj": "demo",
};

export function legacySurface() {
  const items = [];
  for (const [file, group] of Object.entries(ROUTE_FILES)) {
    const src = read(file);
    for (const m of src.matchAll(/\((GET|POST|PUT|DELETE|PATCH|ANY) "([^"]+)"/g)) {
      items.push({ id: `http:${m[1]} ${m[2]}`, group, source: file });
    }
  }
  // ws ops: the `(case op ...)` of handle-event
  const session = read("reactive/session.clj");
  const caseStart = session.indexOf("(case op");
  const caseBody = session.slice(caseStart, caseStart + 4000);
  // refresh / refresh-presence / refresh-sync-table / server-broadcast /
  // error are enqueued by the server itself, never sent by a client, and
  // sse-init is the frame the server emits to open an SSE session
  // (`POST /admin/sse` is the client-side act, counted as its route)
  const SERVER_INTERNAL_OPS = new Set(["refresh", "refresh-presence", "refresh-sync-table", "server-broadcast", "error", "sse-init"]);
  for (const m of caseBody.matchAll(/^\s+:([a-z-]+)\s+\(handle-/gm)) {
    items.push({ id: `ws:${m[1]}`, group: SERVER_INTERNAL_OPS.has(m[1]) ? "ws-internal" : "ws", source: "reactive/session.clj" });
  }
  // tx-step ops: `(s/def ::tx-step (s/or :add-triple ::add-triple-step ...))`
  const tx = read("db/transaction.clj");
  const txSpec = tx.slice(tx.indexOf("(s/def ::tx-step "), tx.indexOf("(s/def ::tx-steps"));
  for (const m of txSpec.matchAll(/:([a-z-]+) ::[a-z-]+-step/g)) {
    items.push({ id: `tx:${m[1]}`, group: "tx", source: "db/transaction.clj" });
  }
  items.push({ id: "tx:mode", group: "tx", source: "db/transaction.clj ::opts" });
  // InstaQL: the `$` options `->forms` dissocs, and every `$` where operator
  const iq = read("db/instaql.clj");
  // `:$expected` / `:$query` / `:$root` (and `:expected` / `:in` / `:message`
  // in the error maps next to the dissoc) are error-map keys, not query syntax
  const NOT_OPTIONS = new Set(["$", "$expected", "$query", "$root", "expected", "in", "message", "query", "root"]);
  const optBlock = iq.slice(iq.indexOf("x (dissoc x"), iq.indexOf("x (dissoc x") + 400);
  for (const m of new Set([...optBlock.matchAll(/:([a-zA-Z]+)/g)].map((x) => x[1]))) {
    if (NOT_OPTIONS.has(m)) continue;
    items.push({ id: `iq:$${m}`, group: "instaql", source: "db/instaql.clj options" });
  }
  for (const m of new Set([...iq.matchAll(/:(\$[a-zA-Z]+)/g)].map((x) => x[1]))) {
    if (NOT_OPTIONS.has(m)) continue;
    items.push({ id: `iq:${m}`, group: "instaql", source: "db/instaql.clj where" });
  }
  // CEL: custom overload ids. The user-facing ones are `ref`, `getTime`, the
  // `timestamp` widenings and `rateLimit.limit`; the rest are the rule-where
  // rewriter's internal overloads (cel.clj:1400+), listed but not counted.
  // The standard library and cel-java's strings / math extensions are
  // covered by the differential's per-clause probes (replay step 29).
  const USER_FACING_CEL = new Set([
    "_ref",
    "_getTime",
    "_timestamp_from_string",
    "_timestamp_from_number",
    "_rateLimit_limit",
    "_rateLimit_limit_tokens",
  ]);
  const cel = read("db/cel.clj");
  for (const m of new Set([...cel.matchAll(/:overload-id "([^"]+)"/g)].map((x) => x[1]))) {
    items.push({ id: `cel:${m}`, group: USER_FACING_CEL.has(m) ? "cel" : "cel-internal", source: "db/cel.clj" });
  }
  // error types: every `::type ::x` thrown by util/exception.clj
  const ex = read("util/exception.clj");
  // `(s/keys :req [::type ::message ...])` mentions ::message next to ::type
  const NOT_TYPES = new Set(["message"]);
  for (const m of new Set([...ex.matchAll(/::type ::([a-z-]+)/g)].map((x) => x[1]))) {
    if (NOT_TYPES.has(m)) continue;
    items.push({ id: `err:${m}`, group: "errors", source: "util/exception.clj" });
  }
  // dedupe, stable order
  const seen = new Map();
  for (const it of items) if (!seen.has(it.id)) seen.set(it.id, it);
  return [...seen.values()].sort((a, b) => (a.id < b.id ? -1 : 1));
}

const manifestPath = path.join(here, "surface.json");
const mode = process.argv[2];
if (import.meta.url === `file://${process.argv[1]}`) {
  const items = legacySurface();
  const out = JSON.stringify({ generatedFrom: "LEGACY/server/src/instant", count: items.length, items }, null, 1) + "\n";
  if (mode === "--write") {
    fs.writeFileSync(manifestPath, out);
    console.log(`wrote ${items.length} surface items to surface.json`);
  } else if (mode === "--check") {
    const committed = fs.existsSync(manifestPath) ? fs.readFileSync(manifestPath, "utf8") : "";
    if (committed !== out) {
      const old = new Set((JSON.parse(committed || '{"items":[]}').items ?? []).map((i) => i.id));
      const now = new Set(items.map((i) => i.id));
      for (const id of now) if (!old.has(id)) console.error(`  + ${id}`);
      for (const id of old) if (!now.has(id)) console.error(`  - ${id}`);
      console.error("surface.json is stale: the vendored legacy source changed. Run `node scripts/differential/surface.mjs --write` and update coverage.");
      process.exit(1);
    }
    console.log(`surface.json is current (${items.length} items)`);
  } else {
    process.stdout.write(out);
  }
}
