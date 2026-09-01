# Performance & load (issue #11, #4)

Target from the issue: **5–10k concurrent websocket connections and ~1k tx/s
fan-out per 2-vCPU node**, Postgres as the only scaling bottleneck.

## Harness

`instant-loadtest` (`crates/instant-loadtest`, a plain tokio/tungstenite
binary) drives one or more server nodes the way a fleet of SDK clients would:

- **N clients** connect over websocket, `init` as a current core version
  (skip-attrs + batched frames), and register **M queries** from a fixed mix:
  `{counters: {}}`, `{todos: {}}`, `{notes: {}}`, `{todos: {$: {where:
  {owner: "user-k"}}}}`. Clients are spread round-robin across **A apps** and
  across the server urls given (`--url ws://a:8888,ws://b:8888`).
- **W writers** run closed-loop transactions (`--inflight` per writer,
  optional `--think-ms` pause): each tx bumps the writer's own `counters.seq`
  and rewrites one seeded todo's title. `notes` are never written, so a
  correct topic matcher never recomputes the `notes` query.
- **Fan-out latency** is measured end to end: the writer stamps the send time
  of each seq; every subscriber attributes each newly observed seq in a
  `refresh-ok` to that stamp. Coalesced refreshes still account for every
  seq, so the delivery percentage must be 100% for a correct server.
- Server-side numbers come from `GET /metrics` on every node (RSS, CPU,
  pool, topic skip/recompute/dedupe counters, NOTIFY lag, batch latency).

A client costs ~35 KB in the harness, so a 2-vCPU / 1 GB box drives 10k
connections (a node/ws client costs ~200 KB each and one node process tops
out around 2k clients).

The `loadtest` GitHub workflow runs a smoke variant on every push (300
clients, asserts no errors, 100% delivery, fan-out p99 ≤ 5s) and the full
sizing when the head commit message contains `[loadtest-full]` or via
workflow_dispatch. Runners are 4-vCPU; the server is pinned to two cores with
`taskset` to approximate the 2-vCPU target. Postgres 17 runs on the same
runner, unpinned. Rate limits are disabled for the harness
(`INSTANT_RATE_LIMITS=off`) — the free-tier buckets (issue #1) cap a single
app at ~400 add-queries/s and ~400 tx/s, far below what the engine can do.

Reproduce locally:

```sh
./scripts/apply-migrations.sh
cargo build --release -p instant-server -p instant-loadtest
INSTANT_RATE_LIMITS=off ./target/release/instant-server &
ids=$(for i in 1 2 3 4; do ./scripts/create-app.sh "lt $i" | grep '^app_id=' | cut -d= -f2; done | paste -sd,)
./target/release/instant-loadtest --apps "$ids" \
  --clients 5000 --queries 3 --writers 8 --think-ms 1000 --duration 30 --seed 5 --cleanup
```

## Results

Setup (2026-09-01): server + Postgres 17 on an Apple M1 (8 cores, 8 GB);
load generator on a 2-vCPU / 4 GB VPS ~15 ms away over Tailscale (the Mac is
on WiFi, which caps the fan-out stream at roughly 25–30 MB/s — see
"reading the numbers"). `before` = `main` at 7aa2ec6, `after` = this branch,
both `--release`. Every run uses `--queries 3 --seed 5`; fan-out numbers are
end-to-end from the writer's send to each subscriber's refresh-ok.

### Connection capacity — remote clients (VPS → Mac)

8 writers paced so each tx fans out to every client of its app; 4 apps.

| run | tx/s | tx p50 / p99 ms | fan-out p50 / p95 / p99 ms | delivered | server RSS | server CPU (cores) | Postgres CPU |
|---|---|---|---|---|---|---|---|
| 1 000 clients, before | 9.8 | 301 / 772 | 568 / 1 162 / 1 404 | 100% | 440 MB | 2.3 peak | 3.7 peak |
| 1 000 clients, after | 15.3 | 22 / 85 | 42 / 90 / 159 | 100% | 72 MB | 0.32 avg / 0.63 peak | idle |
| 5 000 clients, before | 2.1 | 2 605 / 6 881 | 3 908 / 10 365 / 12 741 | 100% | 1 158 MB | 2.7 peak | 2.7 peak |
| 5 000 clients, after | 7.7 | 38 / 286 | 153 / 629 / 893 | 100% | 244 MB | 0.36 avg / 1.2 peak | idle |
| 2 × 5 000 clients (two nodes), after | 2.1 | 88 / 644 | 475 / 1 529 / 2 170 | 100% | 231 + 232 MB | 0.37 avg (both) / 0.7 peak | 1.4 peak |

The writers are closed-loop with a 0.5 s (1 000 clients) / 1 s (5 000) / 4 s
(10 000) pause, so "tx/s" is what the server let them achieve within that
pacing: before, a single transact on a 5 000-session app takes 2.6 s because
the server recomputes 15 000 queries per tx; after, the same tx costs 341
recomputes for 296 250 deliveries and the round trip is RTT plus ~11 ms.

### Fan-out ceiling — loopback (harness on the same M1, no network)

5 000 clients, 4 apps, 8 unpaced writers, 30 s:

| build | tx/s | refresh-ok/s delivered | fan-out p50 / p99 ms | delivered | server CPU (cores avg) | batch mean |
|---|---|---|---|---|---|---|
| after, before pre-serialization | 118 | 65 000 | 114 / 274 | 100% | 4.0 | 130 ms |
| after | 142 | 133 000 | 87 / 173 | 100% | 2.1 | 33 ms |

Serializing each shared result once (`RawValue` splice per session) halved
CPU while doubling deliveries; at 133k refresh-ok/s the harness itself (3.4
cores) is the limit, not the server.

### Transaction throughput — 64 apps, 512 clients, 64 writers × 2 in flight

Each tx fans out to the 8 clients of its app; this is the "1k tx/s per node"
target with a realistic per-tx audience.

| run | tx/s | tx p50 / p99 ms | fan-out p50 / p99 ms | delivered | server CPU | Postgres CPU |
|---|---|---|---|---|---|---|
| before (remote) | 246 | 508 / 775 | 621 / 1 021 | 100% | 2.3 peak | 3.2 peak |
| after (remote) | 993 | 97 / 472 | 120 / 511 | 100% | 1.3 avg / 1.5 peak | 3.3 peak |
| after (loopback) | 1 175 | 76 / 452 | 94 / 485 | 100% | 1.8 avg | 3.7 peak |

Postgres is the bottleneck at ~1.2k tx/s (3.7 cores for the per-tx
statements), which is the shape the issue asks for: the sync tier is cheap,
scale the database.

### Reading the numbers

- **Per connection**: ~48 KB of server RSS at 5 000 sessions (was ~230 KB;
  tungstenite's default 128 KiB read buffer per socket accounted for most of
  it). 10 000 sessions on a 4 GB box is comfortable.
- **Per delivered refresh-ok**: ~16 µs of server CPU at the loopback ceiling
  (2.1 cores for 133k/s), i.e. a 2-vCPU node can push on the order of 100k
  refresh-ok/s before the query work matters.
- **The remote fan-out tails are the network.** At 5 000 clients each tx
  produces ~1 250 × 2.4 KB refresh-ok; above ~10 tx/s that is 30 MB/s, the
  WiFi link saturates, TCP queues, and p99 climbs into seconds while server
  CPU stays under one core (a 10 000-client two-node run at 5 tx/s: 99.7%
  delivered inside the 20 s drain window, servers at 0.8 cores, Postgres
  statements stalling behind the saturated host). Pacing the writers below
  the link's capacity gives the numbers above; the loopback rows show the
  same server without that limit.
- **Postgres CPU "idle" vs "peak"** is the difference between recomputing
  341 queries and recomputing 15 000 for the same transactions; before,
  Postgres was the busiest process on the host at every connection count.

## What changed (and why it matters)

| change | effect on the hot path |
|---|---|
| **Attr cache** (`service::load_attrs`) | the attr catalog was a Postgres round trip on every init, add-query, transact and every session refresh; now one load per app, invalidated by the `attrs_changed` flag carried in the tx NOTIFY (all nodes), with a generation check so a load racing an invalidation is never cached |
| **Topic narrowing** (`instant_core::topics`, issue #4) | each registered query stores coarse topics (`[e-part, attr-set, v-part]`, QUERY.md §6.2 shapes with result substitution on the entity fetch); a tx's `rust_tx_changes` rows are matched against them, so queries on untouched namespaces/entities cost zero SQL. Unresolvable shapes and rules using `.ref(` degrade to catch-all |
| **Per-app refresh batches** (`invalidator::RefreshQueue`) | one worker per app drains queued tx notifications; bursts become one batch (one tx-changes load, one rules load, one `processed-isn`) instead of one refresh task per session per tx |
| **Cross-session dedupe** | identical (query, auth) pairs across sessions are recomputed once and the result fanned out; with N sessions on the same query the SQL cost goes from N to 1 |
| **Bounded concurrency + backpressure** | recomputations per batch are capped (`INSTANT_REFRESH_CONCURRENCY`) so one busy app can't drain the pool; a session that stops reading is disconnected at `INSTANT_MAX_QUEUED_MESSAGES` instead of growing an unbounded queue |
| **Pre-serialized fan-out** (`invalidator::RefreshOkWire`) | each shared result is serialized once and spliced into every subscriber's `refresh-ok` as raw bytes; the transport writer batches raw frames without re-parsing. Halves fan-out CPU |
| **Small socket buffers** (`ws::handler`) | tungstenite's 128 KiB read buffer per connection was ~75% of per-session memory; 8 KiB read / unbuffered write (frames are flushed as sent) brings a session to ~48 KB |
| **Release profile / allocator** | fat LTO, `codegen-units = 1`, mimalloc |
| **`/metrics`** | the harness (and any Prometheus) reads counters instead of scraping logs |

## Not done yet / next

- **Batched SQL in transact**: triple inserts, lookup resolution and mode
  checks are still one statement each; multi-row `UNNEST` inserts would cut
  the per-tx round trips (issue #11 list item).
- **Presence read amplification**: every presence change still re-reads the
  room from Postgres per node.
- **Refined topics** (legacy `instaql_topic.clj` CEL programs) and per-value
  narrowing for numbers/dates (only strings/booleans narrow on `v` today).
- **Per-session frame assembly**: the result bytes are shared, but the
  `refresh-ok` envelope (query, hash, isn, trace-id) is still built and
  copied per session; a per-(query, auth) frame with a shared trace-id would
  make fan-out a pure write.
