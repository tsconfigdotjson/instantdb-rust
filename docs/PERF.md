# Performance

The design target is **5–10k concurrent websocket connections and ~1k
transactions/s of fan-out per 2-vCPU node**, with Postgres as the only thing
you have to scale.

## Headline numbers

One server node with Postgres 17 on an 8-core Apple M1:

| | |
|---|---|
| **~48 KB** | server memory per live websocket session, at 5,000 sessions |
| **133k/s** | `refresh-ok` query updates pushed to clients by one node, on ~2.1 cores |
| **1,362 tx/s** | transactions on one node, where Postgres (3.6 cores) was the bottleneck |
| **69** | queries computed when 5,000 clients with 15,000 subscriptions reconnect at once |

Every run delivered 100% of updates.

## How the numbers were measured

`instant-loadtest` ([`crates/instant-loadtest`](../crates/instant-loadtest))
drives one or more server nodes the way a fleet of SDK clients would:

- **N clients** connect over websocket, `init` like a current
  `@instantdb/core`, and each registers **M queries** from a fixed mix:
  `{counters: {}}`, `{todos: {}}`, `{notes: {}}`, and
  `{todos: {$: {where: {owner: "user-k"}}}}`. Clients are spread round-robin
  across **A apps** and across the server URLs given
  (`--url ws://a:8888,ws://b:8888`).
- **W writers** run closed-loop transactions (`--inflight` per writer,
  optional `--think-ms` pause). Each transaction bumps the writer's own
  `counters.seq` and rewrites one seeded todo's title. `notes` is never
  written, so a correct server never recomputes the `notes` query.
- **Fan-out latency** is measured end to end: the writer stamps the send time
  of each `seq`, and every subscriber attributes each newly seen `seq` in a
  `refresh-ok` to that stamp. Coalesced refreshes still account for every
  `seq`, so delivery must be 100% for a correct server.
- Server-side numbers come from `GET /metrics` on every node.

A simulated client costs ~35 KB in the harness, so a 2-vCPU / 1 GB machine
can drive 10k connections.

Unless noted, the server and Postgres 17 ran on an Apple M1 (8 cores, 8 GB),
and the load generator on a 2-vCPU VPS ~15 ms away. "Loopback" runs put the
load generator on the same machine to take the network out. All runs use
`--queries 3 --seed 5` and a `--release` build with rate limits off.

## Results

### Connection capacity (remote clients)

8 writers, paced so each transaction fans out to every client of its app;
4 apps.

| clients | tx/s | tx p50 / p99 ms | fan-out p50 / p95 / p99 ms | delivered | server RSS | server CPU (cores) | Postgres CPU |
|---|---|---|---|---|---|---|---|
| 1,000 | 15.3 | 22 / 85 | 42 / 90 / 159 | 100% | 72 MB | 0.32 avg / 0.63 peak | idle |
| 5,000 | 7.7 | 35 / 329 | 129 / 636 / 999 | 100% | 277 MB | 0.32 avg / 0.9 peak | idle |
| 2 × 5,000 (two nodes) | 2.1 | 88 / 644 | 475 / 1,529 / 2,170 | 100% | 231 + 232 MB | 0.37 avg / 0.7 peak | 1.4 peak |

Writers pause 0.5 s (1,000 clients), 1 s (5,000) or 4 s (10,000) between
transactions, so tx/s here is set by that pacing, not by the server. At 5,000
clients, one transaction costs 341 query recomputations for 296,250
deliveries, and the round trip is network RTT plus ~11 ms.

### Fan-out ceiling (loopback)

5,000 clients, 4 apps, 8 unpaced writers, 30 s:

| tx/s | refresh-ok/s delivered | fan-out p50 / p99 ms | delivered | server CPU (cores avg) | batch mean |
|---|---|---|---|---|---|
| 142 | 133,000 | 87 / 173 | 100% | 2.1 | 33 ms |

At this rate the load generator itself (3.4 cores) is the limit, not the
server. That works out to ~16 µs of server CPU per delivered update, so a
2-vCPU node can push on the order of 100k updates/s before query work
matters.

### Transaction throughput

64 apps, 512 clients, 64 writers × 2 in flight:

| | tx/s | tx p50 / p99 ms | fan-out p50 / p99 ms | delivered | server CPU | Postgres CPU |
|---|---|---|---|---|---|---|
| remote | 922 | 106 / 474 | 132 / 498 | 100% | 1.2 avg / 1.3 peak | 2.7 peak |
| loopback | 1,362 | 73 / 336 | 90 / 369 | 100% | 1.5 avg | 3.6 peak |

Postgres is the bottleneck at ~1.4k tx/s, which is the intended shape: the
sync tier is cheap, and you scale the database. What remains per transaction
is index maintenance on `triples` and WAL. The remote figure is bound by the
writers' round trip (128 in flight over a 15 ms link), not by the server.

### Reconnect storm

5,000 clients × 3 queries reconnecting at once, as after a node restart:

| | connect + init rate | add-query p50 / p99 ms | queries computed |
|---|---|---|---|
| loopback, concurrency 500 | 2,409/s | 44 / 112 | 69 |
| remote, concurrency 200 | 806/s | 31 / 81 | 69 |

Sessions that register the same query with the same auth at the same point
in time share one computation, so the storm barely touches the query engine.

### Reading the numbers

- **Remote fan-out tails are the network.** At 5,000 clients each
  transaction produces ~1,250 updates of ~2.4 KB. Above ~10 tx/s that's
  30 MB/s, which saturated the WiFi link in these runs: TCP queues and p99
  climbs into seconds while server CPU stays under one core. The writers
  were paced below the link's capacity for the remote tables; the loopback
  rows show the same server without that limit.
- **Postgres "idle"** means the server recomputed only the queries a
  transaction could affect. Recomputing every subscribed query on every
  write instead (15,000 per transaction at 5,000 clients) makes Postgres the
  busiest process on the host at every connection count.

## How it stays cheap

The expensive part of a sync engine is re-running queries when data changes.
The server avoids doing that work, and avoids repeating it:

- **Topic matching.** Each subscribed query records which attributes,
  entities and values it depends on. A transaction's changes are matched
  against those topics, so queries on untouched data cost nothing.
- **Per-app batching.** Bursts of transactions on one app coalesce into a
  single refresh batch: one read of the changes, one rules load.
- **Compute once, send many.** Identical (query, auth) pairs across sessions
  are recomputed once. The result is serialized once and spliced into every
  subscriber's `refresh-ok` as raw bytes.
- **Shared add-query results.** The same query registered by many sessions
  at the same point in time is computed once, which is what keeps reconnect
  storms cheap.
- **Small sessions.** 8 KiB socket read buffers and unbuffered writes keep a
  live session near 48 KB. The open-file limit is raised at boot so a node
  isn't capped at ~1k sockets.
- **Batched writes.** Triples are inserted with one multi-row statement per
  group, and change capture is a statement-level trigger.
- **Backpressure.** Recomputations per batch are capped
  (`INSTANT_REFRESH_CONCURRENCY`) so one busy app can't drain the pool, and a
  client that stops reading is disconnected at `INSTANT_MAX_QUEUED_MESSAGES`
  instead of growing an unbounded queue.
- **Release build.** Fat LTO, one codegen unit, and mimalloc.

## Limits

- **`NOTIFY` is the cluster-wide ceiling.** Every transaction is one
  notification that every node receives, through one Postgres queue. That's
  fine up to a few thousand tx/s per cluster. Beyond that, shard apps across
  Postgres instances rather than adding sync nodes.
- **No websocket compression.** `refresh-ok` payloads are verbose JSON and the
  network is the first wall for large fan-outs, but the websocket stack
  doesn't support permessage-deflate.

## Reproduce

```sh
./scripts/apply-migrations.sh
cargo build --release -p instant-server -p instant-loadtest
INSTANT_RATE_LIMITS=off ./target/release/instant-server &
ids=$(for i in 1 2 3 4; do ./scripts/create-app.sh "lt $i" | grep '^app_id=' | cut -d= -f2; done | paste -sd,)
./target/release/instant-loadtest --apps "$ids" \
  --clients 5000 --queries 3 --writers 8 --think-ms 1000 --duration 30 --seed 5 --cleanup
```

Rate limits are off because the default per-app buckets cap a single app at
~400 add-queries/s and ~400 tx/s, well below what the engine can do. Point
`--url` at any server the machine can reach to test a remote deployment.

The `loadtest` GitHub workflow runs a smoke variant on every push (300
clients; asserts no errors, 100% delivery and fan-out p99 ≤ 5 s) and the
full sizing on demand. CI runners are 4-vCPU with the server pinned to two
cores to approximate the 2-vCPU target.
