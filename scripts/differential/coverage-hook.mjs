// Runtime coverage of the legacy surface (surface.json) for the differential
// harness. Imported by lib.mjs, so every script that uses the capture clients
// records what it exercised without changes of its own:
//   http:  global fetch is wrapped; the request's method + path is matched
//          against the manifest's route patterns (`:param` / `*` segments)
//   ws/tx/iq: `noteWsMessage` is called by connect().send with every op
//          (tx-step ops, `mode`, InstaQL options and where operators are
//          read off the message)
//   err:   every error frame / JSON error body's `type`
//   cel:   custom overloads mentioned in rules pushed through `psql` or
//          `POST /dash/apps/:id/rules`
// When COVERAGE_FILE is set the covered ids are appended (one JSON line per
// process) on exit; coverage.mjs aggregates them.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(fs.readFileSync(path.join(here, "surface.json"), "utf8"));
const known = new Set(manifest.items.map((i) => i.id));
const covered = new Set();
const routes = manifest.items
  .filter((i) => i.id.startsWith("http:"))
  .map((i) => {
    const [method, p] = i.id.slice(5).split(" ");
    const re = new RegExp(
      "^" + p.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/:[a-zA-Z_-]+/g, "[^/]+").replace(/\*/g, ".*") + "$",
    );
    return { id: i.id, method, re, literal: !p.includes(":") && !p.includes("*") };
  });

export function note(id) {
  if (known.has(id)) covered.add(id);
}

export function noteHttp(method, urlString) {
  let pathname;
  try {
    pathname = new URL(urlString).pathname;
  } catch {
    return;
  }
  // prefer literal routes, then the most specific pattern (fewest wildcards)
  const hits = routes.filter((r) => r.method === method && r.re.test(pathname));
  hits.sort((a, b) => Number(b.literal) - Number(a.literal));
  if (hits[0]) covered.add(hits[0].id);
}

export function noteError(type) {
  if (typeof type === "string") note(`err:${type}`);
}

function walkQuery(q) {
  if (!q || typeof q !== "object") return;
  for (const [k, v] of Object.entries(q)) {
    if (k === "$" && v && typeof v === "object") {
      for (const opt of Object.keys(v)) note(`iq:$${opt}`);
      if (v.where) walkWhere(v.where);
    } else if (k !== "$$ruleParams") {
      walkQuery(v);
    }
  }
}
function walkWhere(w) {
  if (Array.isArray(w)) return w.forEach(walkWhere);
  if (!w || typeof w !== "object") return;
  for (const [k, v] of Object.entries(w)) {
    if (k === "or" || k === "and") walkWhere(v);
    else if (k === "$entityIdStartsWith" || k === "$entityId") note(`iq:${k}`);
    else if (v && typeof v === "object" && !Array.isArray(v)) for (const op of Object.keys(v)) note(`iq:${op}`);
  }
}

export function noteCel(text) {
  if (typeof text !== "string") return;
  if (/\.ref\(/.test(text)) note("cel:_ref");
  if (/\.getTime\(\)/.test(text)) note("cel:_getTime");
  if (/timestamp\(\s*['"]/.test(text) || /timestamp\(\s*(data|newData|linkedData|auth|ruleParams)\.[a-zA-Z]+\s*\)/.test(text)) note("cel:_timestamp_from_string");
  if (/timestamp\(\s*\d/.test(text)) note("cel:_timestamp_from_number");
  if (/\.limit\(\s*[^,()]+\s*\)/.test(text)) note("cel:_rateLimit_limit");
  if (/\.limit\(\s*[^,()]+\s*,\s*[^()]+\)/.test(text)) note("cel:_rateLimit_limit_tokens");
}

export function noteWsMessage(msg) {
  if (!msg || typeof msg !== "object") return;
  if (typeof msg.op === "string") note(`ws:${msg.op}`);
  if (Array.isArray(msg["tx-steps"])) {
    for (const step of msg["tx-steps"]) {
      if (!Array.isArray(step)) continue;
      note(`tx:${step[0]}`);
      if (step[4] && typeof step[4] === "object" && step[4].mode) note("tx:mode");
    }
  }
  if (msg.q) walkQuery(msg.q);
}

export function noteWsFrame(frame) {
  if (frame && frame.op === "error") noteError(frame.type);
}

// wrap fetch once per process
if (!globalThis.__instantCoverageFetch) {
  const orig = globalThis.fetch;
  globalThis.__instantCoverageFetch = true;
  globalThis.fetch = async function coverageFetch(input, init) {
    const url = typeof input === "string" ? input : input?.url;
    const method = (init?.method ?? (typeof input === "object" && input?.method) ?? "GET").toUpperCase();
    if (url) noteHttp(method, url);
    if (init?.body && typeof init.body === "string") {
      try {
        const body = JSON.parse(init.body);
        if (body?.code) noteCel(JSON.stringify(body.code));
        if (body?.query) walkQuery(body.query);
        if (body?.rules?.code) noteCel(JSON.stringify(body.rules.code));
      } catch {}
    }
    const res = await orig(input, init);
    if (url && !res.ok) {
      try {
        const body = JSON.parse(await res.clone().text());
        noteError(body?.type);
      } catch {}
    }
    return res;
  };
}

export function coveredIds() {
  return [...covered].sort();
}

const file = process.env.COVERAGE_FILE;
if (file) {
  process.on("exit", () => {
    try {
      fs.appendFileSync(file, JSON.stringify({ script: path.basename(process.argv[1] ?? "?"), covered: coveredIds() }) + "\n");
    } catch {}
  });
}
