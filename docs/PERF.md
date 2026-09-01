# Performance & load (issue #11, #4)

Target from the issue: **5–10k concurrent websocket connections and ~1k tx/s
fan-out per 2-vCPU node**, Postgres as the only scaling bottleneck.

## Harness

`scripts/loadtest.mjs` drives a server the way a fleet of SDK clients would:

- **N clients** connect over websocket, `init` as a current core version
  (skip-attrs + batched frames), and register **M queries** from a fixed mix:
  `{counters: {}}`, `{todos: {}}`, `{notes: {}}`, `{todos: {$: {where:
  {owner: "user-k"}}}}`. Clients are spread round-robin across **A apps**.
- **W writers** run closed-loop transactions (`--inflight` per writer): each
  tx bumps the writer's own `counters.seq` and rewrites one seeded todo's
  title. `notes` are never written, so a correct topic matcher never
  recomputes the `notes` query.
- **Fan-out latency** is measured end to end: the writer stamps the send time
  of each seq; every subscriber attributes each newly observed seq in a
  `refresh-ok` to that stamp. Coalesced refreshes still account for every
  seq, so the delivery percentage must be 100% for a correct server.
- Server-side numbers come from `GET /metrics` (RSS, CPU, pool, topic
  skip/recompute/dedupe counters, NOTIFY lag, batch latency).

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
cargo build --release -p instant-server
INSTANT_RATE_LIMITS=off ./target/release/instant-server &
ids=$(for i in 1 2 3 4; do ./scripts/create-app.sh "lt $i" | grep '^app_id=' | cut -d= -f2; done | paste -sd,)
node --max-old-space-size=4096 scripts/loadtest.mjs --apps "$ids" \
  --clients 5000 --queries 3 --writers 16 --duration 60 --out results.json --cleanup
```

## Results

RESULTS_PLACEHOLDER

## What changed (and why it matters)

| change | effect on the hot path |
|---|---|
| **Attr cache** (`service::load_attrs`) | the attr catalog was a Postgres round trip on every init, add-query, transact and every session refresh; now one load per app, invalidated by the `attrs_changed` flag carried in the tx NOTIFY (all nodes), with a generation check so a load racing an invalidation is never cached |
| **Topic narrowing** (`instant_core::topics`, issue #4) | each registered query stores coarse topics (`[e-part, attr-set, v-part]`, QUERY.md §6.2 shapes with result substitution on the entity fetch); a tx's `rust_tx_changes` rows are matched against them, so queries on untouched namespaces/entities cost zero SQL. Unresolvable shapes and rules using `.ref(` degrade to catch-all |
| **Per-app refresh batches** (`invalidator::RefreshQueue`) | one worker per app drains queued tx notifications; bursts become one batch (one tx-changes load, one rules load, one `processed-isn`) instead of one refresh task per session per tx |
| **Cross-session dedupe** | identical (query, auth) pairs across sessions are recomputed once and the result fanned out; with N sessions on the same query the SQL cost goes from N to 1 |
| **Bounded concurrency + backpressure** | recomputations per batch are capped (`INSTANT_REFRESH_CONCURRENCY`) so one busy app can't drain the pool; a session that stops reading is disconnected at `INSTANT_MAX_QUEUED_MESSAGES` instead of growing an unbounded queue |
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
- **Pre-serialized fan-out**: the shared computation JSON is still cloned
  into each session's `refresh-ok`; serializing once per (query, auth) and
  writing bytes per socket would remove the last O(sessions) serialization.
