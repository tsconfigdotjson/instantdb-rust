#!/usr/bin/env node
// Load-test harness for the sync layer (issue #11).
//
//   node scripts/loadtest.mjs --apps <id[,id...]> [options]
//
// Opens N websocket clients (spread across the given apps), registers M
// queries each, runs W closed-loop writers for D seconds, and reports:
//   - connect/init latency + rate
//   - add-query latency
//   - transact latency + throughput
//   - refresh fan-out latency (transact sent -> refresh-ok observed by each
//     subscriber), delivery completeness
//   - server-side counters from /metrics (RSS, CPU, pool, topic skips, ...)
//
// Options (defaults in brackets):
//   --url ws://host:port      server base [ws://localhost:8888]
//   --clients N               websocket clients [200]
//   --queries M               queries per client, 1..4 [3]
//   --writers W               concurrent writers [4]
//   --inflight K              transacts in flight per writer [1]
//   --duration S              write phase seconds [15]
//   --seed E                  seeded todos/notes per app [50]
//   --connect-concurrency C   clients connecting at once [100]
//   --out FILE                write the JSON report here
//   --smoke                   assert delivery/latency thresholds (CI)
//   --max-p99-ms MS           fail if fan-out p99 exceeds this (with --smoke)
//   --cleanup                 delete seeded entities at the end
//
// Query mix per client (first M of): counters, todos, notes, todos-by-owner.
// Writers bump their own counter and rewrite one todo per tx; notes are never
// written, so with topic narrowing the `notes` query is never recomputed.

import { performance } from "node:perf_hooks";
import fs from "node:fs";

const args = parseArgs(process.argv.slice(2));
const URL_BASE = (args.url ?? "ws://localhost:8888").replace(/\/$/, "");
const HTTP_BASE = URL_BASE.replace(/^ws/, "http");
const APPS = (args.apps ?? "").split(",").filter(Boolean);
if (APPS.length === 0) die("usage: loadtest.mjs --apps <app-id[,app-id...]> [options]");
const N_CLIENTS = num("clients", 200);
const N_QUERIES = Math.min(4, Math.max(1, num("queries", 3)));
const N_WRITERS = num("writers", 4);
const INFLIGHT = num("inflight", 1);
const DURATION = num("duration", 15);
const SEED = num("seed", 50);
const CONNECT_CONC = num("connect-concurrency", 100);
const SMOKE = !!args.smoke;
const MAX_P99 = args["max-p99-ms"] ? Number(args["max-p99-ms"]) : null;
const CLEANUP = !!args.cleanup;
const CORE_VERSION = "v0.22.80"; // skip-attrs + batched frames, like a current SDK

// ---------------------------------------------------------------------------
// helpers

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) out[key] = true;
    else {
      out[key] = next;
      i++;
    }
  }
  return out;
}
function num(k, d) {
  return args[k] === undefined ? d : Number(args[k]);
}
function die(msg) {
  console.error(msg);
  process.exit(2);
}
const uuid = () => crypto.randomUUID();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/// Fixed-width latency histogram: 0.25ms bins up to 30s, overflow bin after.
class Hist {
  constructor(binMs = 0.25, maxMs = 30000) {
    this.binMs = binMs;
    this.bins = new Uint32Array(Math.ceil(maxMs / binMs) + 1);
    this.count = 0;
    this.sum = 0;
    this.max = 0;
  }
  add(ms) {
    let i = Math.floor(ms / this.binMs);
    if (i >= this.bins.length) i = this.bins.length - 1;
    this.bins[i]++;
    this.count++;
    this.sum += ms;
    if (ms > this.max) this.max = ms;
  }
  pct(p) {
    if (this.count === 0) return null;
    const target = Math.ceil((p / 100) * this.count);
    let acc = 0;
    for (let i = 0; i < this.bins.length; i++) {
      acc += this.bins[i];
      if (acc >= target) return (i + 1) * this.binMs;
    }
    return this.max;
  }
  summary() {
    if (this.count === 0) return { count: 0 };
    return {
      count: this.count,
      mean: round(this.sum / this.count),
      p50: round(this.pct(50)),
      p95: round(this.pct(95)),
      p99: round(this.pct(99)),
      max: round(this.max),
    };
  }
}
const round = (x) => (x == null ? null : Math.round(x * 100) / 100);

async function scrapeMetrics() {
  try {
    const res = await fetch(`${HTTP_BASE}/metrics`);
    if (!res.ok) return null;
    const text = await res.text();
    const out = {};
    for (const line of text.split("\n")) {
      if (!line || line.startsWith("#")) continue;
      const sp = line.lastIndexOf(" ");
      const key = line.slice(0, sp);
      const val = Number(line.slice(sp + 1));
      if (!key.includes("{") || key.includes("_bucket")) out[key] = val;
    }
    return out;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// client

class Client {
  constructor(name, appId) {
    this.name = name;
    this.appId = appId;
    this.ws = null;
    this.waiters = [];
    this.lastSeen = new Map(); // counter eid -> last seq observed
    this.refreshCount = 0;
    this.bytes = 0;
    this.closed = false;
  }
  connect() {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(`${URL_BASE}/runtime/session?app_id=${this.appId}`);
      this.ws = ws;
      ws.onopen = () => resolve();
      ws.onerror = (e) => reject(new Error(`ws error (${this.name}): ${e.message ?? e}`));
      ws.onclose = () => {
        this.closed = true;
        for (const [, , rej] of this.waiters.splice(0)) rej(new Error(`closed (${this.name})`));
      };
      ws.onmessage = (e) => {
        this.bytes += e.data.length;
        const msg = JSON.parse(e.data);
        const msgs = Array.isArray(msg) ? msg : [msg];
        for (const m of msgs) this.onMessage(m);
      };
    });
  }
  onMessage(m) {
    if (m.op === "refresh-ok") {
      this.refreshCount++;
      stats.refreshOk++;
      this.observeRefresh(m);
    } else if (m.op === "error") {
      stats.errors++;
      if (stats.errors <= 5) console.error(`[${this.name}] error:`, m.type, m.message);
    }
    for (let i = this.waiters.length - 1; i >= 0; i--) {
      const [pred, resolve] = this.waiters[i];
      if (pred(m)) {
        this.waiters.splice(i, 1);
        resolve(m);
      }
    }
  }
  /// Fan-out latency: each writer's counter carries a seq; every seq the
  /// client has not yet seen is attributed to its own send time.
  observeRefresh(m) {
    const now = performance.now();
    for (const c of m.computations ?? []) {
      const rows = c["instaql-result"]?.[0]?.data?.["datalog-result"]?.["join-rows"]?.[0];
      if (!rows) continue;
      for (const [e, a, v] of rows) {
        if (a !== schema.counters.seq) continue;
        const writer = writersByCounter.get(e);
        if (!writer) continue;
        const prev = this.lastSeen.get(e) ?? 0;
        if (v <= prev) continue;
        for (let s = prev + 1; s <= v; s++) {
          const t0 = writer.sendTimes.get(s);
          if (t0 !== undefined) fanout.add(now - t0);
        }
        this.lastSeen.set(e, v);
      }
    }
  }
  send(msg) {
    const withId = { "client-event-id": uuid(), ...msg };
    this.ws.send(JSON.stringify(withId));
    return withId["client-event-id"];
  }
  waitFor(pred, timeout = 30000) {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => {
        const i = this.waiters.findIndex((w) => w[1] === res);
        if (i >= 0) this.waiters.splice(i, 1);
        reject(new Error(`timeout (${this.name})`));
      }, timeout);
      const res = (m) => {
        clearTimeout(t);
        resolve(m);
      };
      this.waiters.push([pred, res, reject]);
    });
  }
  async request(msg, okOp, timeout) {
    const id = this.send(msg);
    return this.waitFor((m) => m["client-event-id"] === id && (m.op === okOp || m.op === "error"), timeout);
  }
  close() {
    try {
      this.ws?.close();
    } catch {}
  }
}

// ---------------------------------------------------------------------------
// schema + seed (per app)

const schema = {
  todos: { id: uuid(), title: uuid(), done: uuid(), owner: uuid() },
  notes: { id: uuid(), body: uuid() },
  counters: { id: uuid(), seq: uuid(), writer: uuid() },
};
const attrStep = (etype, label, id) => [
  "add-attr",
  {
    id,
    "forward-identity": [uuid(), etype, label],
    "value-type": "blob",
    cardinality: "one",
    "unique?": label === "id",
    "index?": false,
    isUnsynced: true,
  },
];
const seeded = new Map(); // appId -> { todos: [eids], notes: [eids] }
const writersByCounter = new Map(); // counter eid -> writer
const fanout = new Hist();
const connectHist = new Hist();
const addQueryHist = new Hist();
const txHist = new Hist();
const stats = { refreshOk: 0, errors: 0, txOk: 0, txErr: 0 };

async function setupApp(appId) {
  const c = new Client(`setup-${appId.slice(0, 8)}`, appId);
  await c.connect();
  const init = await c.request({ op: "init", "app-id": appId, versions: { "@instantdb/core": CORE_VERSION } }, "init-ok");
  if (init.op === "error") die(`init failed for app ${appId}: ${init.message}`);
  const steps = [];
  for (const [etype, attrs] of Object.entries(schema)) {
    for (const [label, id] of Object.entries(attrs)) steps.push(attrStep(etype, label, id));
  }
  const todos = [];
  const notes = [];
  for (let i = 0; i < SEED; i++) {
    const t = uuid();
    todos.push(t);
    steps.push(["add-triple", t, schema.todos.id, t]);
    steps.push(["add-triple", t, schema.todos.title, `todo ${i}`]);
    steps.push(["add-triple", t, schema.todos.done, i % 2 === 0]);
    steps.push(["add-triple", t, schema.todos.owner, `user-${i % 10}`]);
    const n = uuid();
    notes.push(n);
    steps.push(["add-triple", n, schema.notes.id, n]);
    steps.push(["add-triple", n, schema.notes.body, `note ${i}`]);
  }
  const r = await c.request({ op: "transact", "tx-steps": steps }, "transact-ok", 60000);
  if (r.op === "error") die(`seed transact failed: ${r.message}`);
  seeded.set(appId, { todos, notes, client: c });
}

async function cleanupApp(appId) {
  const { todos, notes, client } = seeded.get(appId);
  const steps = [];
  for (const t of todos) steps.push(["delete-entity", t, "todos"]);
  for (const n of notes) steps.push(["delete-entity", n, "notes"]);
  for (const w of writers) if (w.appId === appId) steps.push(["delete-entity", w.counter, "counters"]);
  for (const etype of Object.keys(schema)) {
    for (const id of Object.values(schema[etype])) steps.push(["delete-attr", id]);
  }
  const r = await client.request({ op: "transact", "tx-steps": steps }, "transact-ok", 60000);
  if (r.op === "error") console.error(`cleanup failed for ${appId}: ${r.message}`);
}

// ---------------------------------------------------------------------------
// writers

const writers = [];
class Writer {
  constructor(i, appId) {
    this.i = i;
    this.appId = appId;
    this.counter = uuid();
    this.seq = 0;
    this.sendTimes = new Map();
    this.client = new Client(`writer-${i}`, appId);
    writersByCounter.set(this.counter, this);
  }
  async start() {
    await this.client.connect();
    const init = await this.client.request({ op: "init", "app-id": this.appId, versions: { "@instantdb/core": CORE_VERSION } }, "init-ok");
    if (init.op === "error") die(`writer init failed: ${init.message}`);
    const r = await this.client.request(
      {
        op: "transact",
        "tx-steps": [
          ["add-triple", this.counter, schema.counters.id, this.counter],
          ["add-triple", this.counter, schema.counters.writer, this.i],
          ["add-triple", this.counter, schema.counters.seq, 0],
        ],
      },
      "transact-ok",
    );
    if (r.op === "error") die(`writer counter create failed: ${r.message}`);
  }
  async run(untilMs) {
    const { todos } = seeded.get(this.appId);
    const loop = async () => {
      while (performance.now() < untilMs && !this.client.closed) {
        const seq = ++this.seq;
        const todo = todos[seq % todos.length];
        const t0 = performance.now();
        this.sendTimes.set(seq, t0);
        const r = await this.client.request(
          {
            op: "transact",
            "tx-steps": [
              ["add-triple", this.counter, schema.counters.seq, seq],
              ["add-triple", todo, schema.todos.title, `todo ${seq}`],
            ],
          },
          "transact-ok",
          60000,
        );
        txHist.add(performance.now() - t0);
        if (r.op === "error") stats.txErr++;
        else stats.txOk++;
      }
    };
    await Promise.all(Array.from({ length: INFLIGHT }, loop));
  }
}

// ---------------------------------------------------------------------------
// main

const t0 = performance.now();
console.log(
  `loadtest: ${N_CLIENTS} clients × ${N_QUERIES} queries, ${N_WRITERS} writers × ${INFLIGHT} in flight, ${DURATION}s, ${APPS.length} app(s), server ${URL_BASE}`,
);

for (const app of APPS) await setupApp(app);
console.log(`seeded ${SEED} todos + ${SEED} notes per app in ${elapsed()}s`);

for (let i = 0; i < N_WRITERS; i++) writers.push(new Writer(i, APPS[i % APPS.length]));
for (const w of writers) await w.start();

// clients
const clients = [];
const metricsBefore = await scrapeMetrics();
const connectStart = performance.now();
let connectFailures = 0;
for (let base = 0; base < N_CLIENTS; base += CONNECT_CONC) {
  const batch = [];
  for (let i = base; i < Math.min(N_CLIENTS, base + CONNECT_CONC); i++) {
    const c = new Client(`c${i}`, APPS[i % APPS.length]);
    batch.push(
      (async () => {
        const s = performance.now();
        await c.connect();
        const init = await c.request({ op: "init", "app-id": c.appId, versions: { "@instantdb/core": CORE_VERSION } }, "init-ok");
        if (init.op === "error") throw new Error(init.message);
        connectHist.add(performance.now() - s);
        const mix = [
          { counters: {} },
          { todos: {} },
          { notes: {} },
          { todos: { $: { where: { owner: `user-${i % 10}` } } } },
        ];
        for (const q of mix.slice(0, N_QUERIES)) {
          const qs = performance.now();
          const r = await c.request({ op: "add-query", q }, "add-query-ok");
          if (r.op === "error") throw new Error(r.message);
          addQueryHist.add(performance.now() - qs);
        }
        clients.push(c);
      })().catch((e) => {
        connectFailures++;
        if (connectFailures <= 5) console.error(`client ${i} failed: ${e.message}`);
        c.close();
      }),
    );
  }
  await Promise.all(batch);
}
const connectSecs = (performance.now() - connectStart) / 1000;
console.log(`connected ${clients.length}/${N_CLIENTS} clients in ${connectSecs.toFixed(1)}s (${connectFailures} failures)`);

// write phase
const metricsStart = await scrapeMetrics();
const writeStart = performance.now();
await Promise.all(writers.map((w) => w.run(writeStart + DURATION * 1000)));
const writeSecs = (performance.now() - writeStart) / 1000;
console.log(`write phase done: ${stats.txOk} tx ok, ${stats.txErr} errors in ${writeSecs.toFixed(1)}s`);

// drain: wait for every client to see every writer's final seq
const drainStart = performance.now();
let caughtUp = 0;
while (performance.now() - drainStart < 20000) {
  caughtUp = clients.filter((c) => writers.every((w) => (c.lastSeen.get(w.counter) ?? 0) >= w.seq)).length;
  if (caughtUp === clients.length) break;
  await sleep(200);
}
const drainSecs = (performance.now() - drainStart) / 1000;
const metricsEnd = await scrapeMetrics();

// expected fan-out samples: every client observes every seq (writers count
// only their own app's clients)
let expectedSamples = 0;
for (const w of writers) expectedSamples += w.seq * clients.filter((c) => c.appId === w.appId).length;

const report = {
  config: { url: URL_BASE, apps: APPS.length, clients: N_CLIENTS, queries: N_QUERIES, writers: N_WRITERS, inflight: INFLIGHT, duration: DURATION, seed: SEED },
  connect: { ok: clients.length, failed: connectFailures, seconds: round(connectSecs), per_sec: round(clients.length / connectSecs), latency_ms: connectHist.summary() },
  add_query: { latency_ms: addQueryHist.summary() },
  transact: { ok: stats.txOk, errors: stats.txErr, seconds: round(writeSecs), per_sec: round(stats.txOk / writeSecs), latency_ms: txHist.summary() },
  fanout: {
    latency_ms: fanout.summary(),
    samples: fanout.count,
    expected_samples: expectedSamples,
    delivered_pct: expectedSamples ? round((100 * fanout.count) / expectedSamples) : null,
    clients_caught_up: caughtUp,
    drain_seconds: round(drainSecs),
    refresh_ok_received: stats.refreshOk,
    bytes_received: clients.reduce((a, c) => a + c.bytes, 0),
  },
  errors: stats.errors,
  server: serverDelta(metricsStart, metricsEnd, writeSecs, metricsBefore),
};

console.log("\n" + markdown(report));
if (args.out) fs.writeFileSync(args.out, JSON.stringify(report, null, 2));

// cleanup
if (CLEANUP) for (const app of APPS) await cleanupApp(app);
for (const c of clients) c.close();
for (const w of writers) w.client.close();
for (const { client } of seeded.values()) client.close();

let failed = false;
if (SMOKE) {
  const checks = [
    [clients.length === N_CLIENTS, `all ${N_CLIENTS} clients connected (${clients.length})`],
    [stats.txErr === 0 && stats.errors === 0, `no errors (tx ${stats.txErr}, ws ${stats.errors})`],
    [stats.txOk > 0, `transactions committed (${stats.txOk})`],
    [caughtUp === clients.length, `every client caught up (${caughtUp}/${clients.length})`],
    [report.fanout.delivered_pct >= 99.9, `fan-out delivered ${report.fanout.delivered_pct}% (>= 99.9)`],
  ];
  if (MAX_P99 != null) checks.push([report.fanout.latency_ms.p99 <= MAX_P99, `fan-out p99 ${report.fanout.latency_ms.p99}ms <= ${MAX_P99}ms`]);
  for (const [ok, label] of checks) {
    console.log(`${ok ? "PASS" : "FAIL"} ${label}`);
    if (!ok) failed = true;
  }
}
console.log(`total ${elapsed()}s`);
process.exit(failed ? 1 : 0);

// ---------------------------------------------------------------------------

function elapsed() {
  return ((performance.now() - t0) / 1000).toFixed(1);
}

function serverDelta(a, b, secs, before) {
  if (!a || !b) return null;
  const d = (k) => (b[k] ?? 0) - (a[k] ?? 0);
  const out = {
    rss_mb: round((b.process_resident_memory_bytes ?? 0) / 1048576),
    rss_mb_before_clients: before ? round((before.process_resident_memory_bytes ?? 0) / 1048576) : null,
    cpu_seconds: round(d("process_cpu_seconds")),
    cpu_cores_avg: round(d("process_cpu_seconds") / secs),
    sessions: b.instant_sessions,
    pg_pool_size: b.instant_pg_pool_size,
    pg_pool_max: b.instant_pg_pool_max,
    refresh_batches: d("instant_refresh_batches_total"),
    refresh_txs: d("instant_refresh_txs_total"),
    queries_skipped_by_topic: d("instant_refresh_queries_skipped_total"),
    queries_computed: d("instant_refresh_queries_computed_total"),
    queries_deduped: d("instant_refresh_queries_deduped_total"),
    queries_changed: d("instant_refresh_queries_changed_total"),
    attr_cache_hits: d("instant_attr_cache_hits_total"),
    attr_cache_misses: d("instant_attr_cache_misses_total"),
    sessions_overflowed: d("instant_ws_sessions_overflowed_total"),
  };
  for (const [name, key] of [
    ["notify_lag_ms", "instant_notify_lag_seconds"],
    ["refresh_batch_ms", "instant_refresh_batch_seconds"],
    ["query_ms", "instant_query_seconds"],
    ["transact_ms", "instant_transact_seconds"],
  ]) {
    const c = d(`${key}_count`);
    out[`${name}_mean`] = c ? round((1000 * d(`${key}_sum`)) / c) : null;
  }
  return out;
}

function fmtLat(l) {
  if (!l || !l.count) return "n/a";
  return `p50 ${l.p50} / p95 ${l.p95} / p99 ${l.p99} / max ${l.max}`;
}

function markdown(r) {
  const s = r.server ?? {};
  const rows = [
    ["clients (ok/failed)", `${r.connect.ok} / ${r.connect.failed}`],
    ["connect+init rate", `${r.connect.per_sec}/s (${r.connect.seconds}s)`],
    ["connect+init latency ms", fmtLat(r.connect.latency_ms)],
    ["add-query latency ms", fmtLat(r.add_query.latency_ms)],
    ["tx throughput", `${r.transact.per_sec} tx/s (${r.transact.ok} ok, ${r.transact.errors} err)`],
    ["tx latency ms", fmtLat(r.transact.latency_ms)],
    ["fan-out latency ms", fmtLat(r.fanout.latency_ms)],
    ["fan-out delivered", `${r.fanout.delivered_pct}% (${r.fanout.samples}/${r.fanout.expected_samples}), ${r.fanout.clients_caught_up} clients caught up`],
    ["refresh-ok received", `${r.fanout.refresh_ok_received} (${round(r.fanout.bytes_received / 1048576)} MB)`],
    ["server RSS", s.rss_mb != null ? `${s.rss_mb} MB (before clients: ${s.rss_mb_before_clients} MB)` : "n/a (no /metrics)"],
    ["server CPU", s.cpu_seconds != null ? `${s.cpu_seconds}s = ${s.cpu_cores_avg} cores avg during writes` : "n/a"],
    ["refresh batches / txs", s.refresh_batches != null ? `${s.refresh_batches} / ${s.refresh_txs}` : "n/a"],
    ["queries skipped / computed / deduped / changed", s.queries_computed != null ? `${s.queries_skipped_by_topic} / ${s.queries_computed} / ${s.queries_deduped} / ${s.queries_changed}` : "n/a"],
    ["attr cache hits / misses", s.attr_cache_hits != null ? `${s.attr_cache_hits} / ${s.attr_cache_misses}` : "n/a"],
    ["server means ms (notify lag / batch / query / transact)", s.query_ms_mean != null ? `${s.notify_lag_ms_mean} / ${s.refresh_batch_ms_mean} / ${s.query_ms_mean} / ${s.transact_ms_mean}` : "n/a"],
    ["pg pool", s.pg_pool_size != null ? `${s.pg_pool_size}/${s.pg_pool_max}` : "n/a"],
  ];
  const cfg = r.config;
  let md = `### loadtest: ${cfg.clients} clients × ${cfg.queries} queries, ${cfg.writers}×${cfg.inflight} writers, ${cfg.duration}s, ${cfg.apps} app(s)\n\n| metric | value |\n|---|---|\n`;
  for (const [k, v] of rows) md += `| ${k} | ${v} |\n`;
  return md;
}
