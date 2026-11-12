# instant-worker: second deployment (PR head 662a928)

Date: 2026-11-12, 01:35–01:45 UTC. Times are epoch ms on the clock named in each row.

**What was deployed**
- Commit `662a928`, which adds repeat-boot skip, fewer statements per operation, `NODE_LOCATION_HINT`, and random-start statement numbering.
- Worker version `6de2ad66`. Startup time 71 ms; upload 12638.89 KiB, gzip 3994.20 KiB.
- New Hyperdrive config `d2210fa0f5ca4bdbbc6b5ea414101df1`, created with `--caching-disabled`.
- Same Neon database as the first run: AWS `us-east-2`, PostgreSQL 18.6.

**Vars:**
- `NODE_NAME = "node-8"` (a fresh object)
- `NODE_LOCATION_HINT = "enam"`
- `RUST_LOG = "info,sqlx=debug"`
- `INSTANT_HANDLE_RECEIVE_TIMEOUT_MS = "30000"`

**Method.** Data comes from `wrangler tail --format json`, written outside the repo; `tail-sanitized.jsonl` is the committed copy (no IPs, geolocation or URLs; UUIDs replaced). The method is the same as [LATENCY.md](../latency/LATENCY.md) (first run, version `b6805425`, PR head before these changes), and so are its limits:
- `BEGIN` is not logged.
- Timestamps advance only at I/O, so CPU time between statements is not visible.
- Statements are attributed to operations by time window, and background-loop statements are excluded by table name.
- HTTP tail events are sampled.

As agreed, per-step logs are not committed.

## 0. Protocol suites on Cloudflare

Each suite ran on a fresh app.

| suite | result | time (first run) |
|---|---|---|
| smoke-ws | pass | 3 s (7 s) |
| sse-test | pass | 1 s (3 s) |
| synctable-test | pass | 4 s (7 s) |
| conformance-test | `CONFORMANCE SUITE PASSED (222 assertions)` | 15 s (51 s) |
| local-bus-test | `local bus ok` | 3 s (8 s) |
| streams-test against the Neon DB | `STREAMS TEST PASSED` | 3 s (14 s) |

For the whole run, the tail for version `6de2ad66` has **0 WARN/ERROR lines and 0 exceptions**.

## 1. Summary table

The columns are as defined in LATENCY.md. Each operation kind has 20 samples. Values in brackets are the first run.

| operation | client median | statements (min–max) | sum of statement time | remainder |
|---|---|---|---|---|
| `/health`, warm, reused connection (TTFB − pretransfer) | 47.0 ms (48.1) | 0 | 0 | 47.0 ms |
| a. `init` → `init-ok`, new websocket each time | 44.4 ms (63.3) | 0 (0) | 0 | 44.4 ms |
| b. `add-query`, distinct query | **121.5 ms (242.5)** | **5 (5–6)** (8) | 92.0 ms (189.0) | 29.5 ms (53.5) |
| c. `add-query`, same query | 22.3 ms (27.0) | 0; 5 on the first (8) | 0 | 22.3 ms |
| d. `transact` → `transact-ok` | **220.9 ms (295.1)** | 10 (10–10) (10) | 184.0 ms (254.5) | 36.9 ms (40.6) |
| e. `transact` → `refresh-ok` on another session | **338.7 ms (482.4)** | **15 (15–16)** (16) | 281.0 ms (406.0) | 57.7 ms (76.4) |
| first boot of this version (1 sample, TTFB) | 4472 ms (median 4620) | 98 (96) | 3518 ms (3672) | 954 ms |
| **repeat boot** (2 samples, TTFB) | **1010 ms and 444 ms** | **1** | 153 ms and 48 ms | 857 ms and 396 ms |

Other measurements:
- **Statement time:** median sqlx `elapsed` over 607 operation statements was **18.0 ms** (min 15.0, max 52.0). The first run had 25.0 ms over 690 statements.
- **Websocket open** before `init`: median 255.0 ms (328.5 in the first run).
- **e:** `transact-ok` reached session 1 after a median of 216.5 ms (297.8 in the first run).
- **Statements after the reply** (not counted in the table): 5 after d #20 and 1 after c #20. Both were followed by setup for the next phase, as in the first run.
- **Warm `/health`:** 47.0 ms is the median of all 20 requests. The first of them took 452.6 ms, coming 1 s after a repeat boot; without it, the median is 42.8 ms (min 33.9, max 117.1).

## 2. Boots

There were three boots of `node-8`. The first was a full bootstrap: the database had no `bootstrap_marker` yet, and statement 98 writes it. The other two were repeat boots.

| boot | trigger | client TTFB | Worker → DO | statements | sum of elapsed | DO start → `storage sweeper started` | DO event cpuTime |
|---|---|---|---|---|---|---|---|
| 1 | first request after the deploy | 4472.5 ms | 620 ms | 98 | 3518 ms | 3671 ms | 368 ms (event runs on past the response) |
| 2 | request after 100 s with no requests | 1009.7 ms | 659 ms | 1 | 153 ms | 158 ms | 159 ms |
| 3 | A6 eviction check, after >2 min with no clients | 444.1 ms | 213 ms | 1 | 48 ms | 52 ms | 224 ms (event runs on past the response) |

**Repeat boot, the whole list (boots 2 and 3):**

| # | elapsed | SQL |
|---|---|---|
| 1 | 153 ms / 48 ms | `SELECT key, value FROM rust_server_config WHERE key IN ('bootstrap_marker', 'server_secret', 'webhook_signing_key', 'webhook_kid')` |

**First boot (98 statements):**
- **#1** is the marker read above. It took **735 ms**, and is the first statement after the pool connects.
- **#2–97** are the 96 bootstrap statements from the first run, in the same order:
  - #2–3: two advisory locks;
  - #4–5: catalog rows;
  - #6–62: 57 `idents` inserts, 1158 ms in total (17–73 ms each);
  - #63–66: `set_config`, `UPDATE`, `COMMIT`;
  - #67–97: DDL, server config and triggers, 34–126 ms each.
- **#98** is `INSERT INTO rust_server_config (key, value) VALUES ('bootstrap_marker', …)`.

The 735 ms is not broken down: connect, TLS and authentication through Hyperdrive are not logged separately.

## 3. Baselines

| | this run | first run |
|---|---|---|
| `/health` over one reused connection (TTFB − pretransfer) | min 33.9, median 47.0, max 452.6 ms (without #1: median 42.8) | min 35.3, median 48.1, max 78.4 ms |
| client edge | `colo=YYZ` | `colo=YYZ` |
| `select 1` × 10 via psql from this machine | min 23.0, median **23.5**, max 24.3 ms | min 19.8, median 20.5, max 22.0 ms |
| median sqlx `elapsed`, Durable Object → Hyperdrive → Neon | **18.0 ms** | 25.0 ms |

`psql` runs in Docker Desktop on macOS, as in the first run.

## 4. Representative operations (#10 of each kind)

The gap column is this statement's start minus the previous statement's end; a negative gap means the statements overlapped on different connections.

### b. `add-query`, distinct: client 118.9 ms, 5 statements, 92 ms

The first run's 8 statements are now 5. Its separate `rules`, `max(id)` and `pg_current_wal_lsn()` reads became statement 1, and the `SET TRANSACTION` statement is gone.

| # | elapsed ms | gap ms | SQL |
|---|---|---|---|
| 1 | 18.0 | | `SELECT coalesce(max(id), 0) AS n, '0/' \|\| pg_current_wal_lsn()::text AS isn, (SELECT code FROM rules WHERE app_id = $1) AS code FROM transactions WHERE app_id = $1` |
| 2 | 19.0 | 19 | `SELECT idt.entity_id AS eid, idt.value AS order_v, idt.created_at AS order_t, idt.created_at AS sort_t, NULL::float8 AS sort_n, NULL::text AS sort_s, NULL::boolean AS sort_b, NULL::timestamptz AS sort` |
| 3 | 18.0 | 0 | `SELECT DISTINCT c.eid FROM (SELECT a.entity_id AS eid FROM triples a WHERE a.app_id = $1 AND a.attr_id = $2) c WHERE c.eid IS NOT NULL AND NOT EXISTS (SELECT 1 FROM triples i WHERE i.app_id = $3 AND i` |
| 4 | 20.0 | 0 | `COMMIT` |
| 5 | 17.0 | 0 | `SELECT coalesce(max(id), 0) AS n FROM transactions WHERE app_id = $1` |

### d. `transact`: client 226.2 ms, 10 statements, 193 ms

The same 10 statements as the first run, in the same order.

| # | elapsed ms | gap ms | SQL |
|---|---|---|---|
| 1 | 20.0 | | `SELECT pg_backend_pid() AS pid, txid_current() AS xid, set_config('statement_timeout', $1, true)` |
| 2 | 21.0 | 0 | `SELECT status FROM apps WHERE id = $1` |
| 3 | 17.0 | 0 | `SELECT code FROM rules WHERE app_id = $1` |
| 4 | 17.0 | 0 | `SELECT attr_id, value FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = ANY($3)` |
| 5 | 19.0 | 0 | `WITH t AS (INSERT INTO transactions (app_id) VALUES ($1) RETURNING id) SELECT id, set_config('instant.rust_tx_id', id::text, true) AS tag FROM t` |
| 6 | 20.0 | 0 | `INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5, ea, eav, av, ave, vae, checked_data_type) SELECT $1, e, a, v::jsonb, md5((v::jsonb)::text), ea, eav, av, ave, vae, cdt::checked_data_` |
| 7 | 19.0 | 0 | `SELECT 1 AS x FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3` |
| 8 | 23.0 | 0 | `COMMIT` |
| 9 | 18.0 | 0 | `SELECT '0/' \|\| pg_current_wal_lsn()::text AS isn` |
| 10 | 19.0 | −18 | `SELECT entity_id, attr_id, value FROM rust_tx_changes WHERE app_id = $1 AND tx_id = ANY($2)` |

### e. `transact` → `refresh-ok`: client 335.9 ms, 15 statements, 272 ms

Statements 1–10 are the transact; 11–15 are the refresh. The first run's refresh had 6 statements, including `SET TRANSACTION`.

| # | elapsed ms | gap ms | SQL |
|---|---|---|---|
| 1 | 18.0 | | `SELECT pg_backend_pid() AS pid, txid_current() AS xid, set_config('statement_timeout', $1, true)` |
| 2 | 17.0 | 0 | `SELECT status FROM apps WHERE id = $1` |
| 3 | 18.0 | 0 | `SELECT code FROM rules WHERE app_id = $1` |
| 4 | 19.0 | 0 | `SELECT attr_id, value FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = ANY($3)` |
| 5 | 17.0 | 0 | `WITH t AS (INSERT INTO transactions (app_id) VALUES ($1) RETURNING id) SELECT id, set_config('instant.rust_tx_id', id::text, true) AS tag FROM t` |
| 6 | 21.0 | 0 | `INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5, ea, eav, av, ave, vae, checked_data_type) SELECT $1, e, a, v::jsonb, md5((v::jsonb)::text), ea, eav, av, ave, vae, cdt::checked_data_` |
| 7 | 19.0 | 0 | `SELECT 1 AS x FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3` |
| 8 | 20.0 | 0 | `COMMIT` |
| 9 | 17.0 | 0 | `SELECT entity_id, attr_id, value FROM rust_tx_changes WHERE app_id = $1 AND tx_id = ANY($2)` |
| 10 | 18.0 | −17 | `SELECT '0/' \|\| pg_current_wal_lsn()::text AS isn` |
| 11 | 16.0 | −1 | `SELECT code FROM rules WHERE app_id = $1` |
| 12 | 18.0 | 19 | `SELECT idt.entity_id AS eid, idt.value AS order_v, idt.created_at AS order_t, idt.created_at AS sort_t, NULL::float8 AS sort_n, NULL::text AS sort_s, NULL::boolean AS sort_b, NULL::timestamptz AS sort` |
| 13 | 18.0 | 0 | `SELECT entity_id, attr_id, value, created_at FROM triples WHERE app_id = $1 AND entity_id = ANY($2) AND attr_id = ANY($3) AND ea` |
| 14 | 18.0 | 0 | `COMMIT` |
| 15 | 18.0 | 0 | `SELECT '0/' \|\| pg_current_wal_lsn()::text AS isn` |

The unlogged gaps of 19 ms before b #2 and e #12 are where read transactions start. No `BEGIN` is logged.

## 5. Idle statements

**Window:** 1794447759850 – 1794447879850, 120 s with no client connected.

**Totals:**
- **44 statements**, i.e. **22 per minute** over the window. The first run had 38 (19 per minute).
- All 44 fall between 1794447762484 and 1794447827633, a span of 65 s. There were none after that.
- The next request (boot 3) was a boot, so the object had been evicted.

**By source file** (matched by SQL text, as in the first run):

| source | statements | SQL |
|---|---|---|
| `presence.rs` | 35 | heartbeat `INSERT INTO rust_nodes`, `DELETE FROM rust_presence`, `rust_nodes`, `rust_spill`, `rust_tx_changes` — 7 each, every ~10 s |
| `indexing_jobs.rs` | 4 | `UPDATE indexing_jobs …`, `SELECT id FROM indexing_jobs …` — twice each, 60 s apart |
| `usage.rs` | 2 | `triples_size_updates` — twice, 60 s apart |
| `webhooks.rs` | 3 | `webhook_events` reclaim, select, retry — once each |

## 6. Not measured, or not working

- **Where the Durable Object runs.** The `NODE_LOCATION_HINT = "enam"` effect is not visible in any response or tail field. What was measured:
  - median statement `elapsed`: 18.0 ms here, 25.0 ms in the first run (no hint, object created from a Toronto client);
  - `select 1` from this machine to Neon: 23.5 ms here, 20.5 ms then.

  Whether the object moved, and how much of the 7 ms difference is due to the hint, was not measured. The two runs also differ in code and in Hyperdrive config.
- **The 735 ms first statement of boot 1**, and the 153 ms and 48 ms single statements of the repeat boots, include an unlogged connection setup through Hyperdrive. That part was not separated.
- **Repeat-boot client time** varied: 1010 ms and 444 ms over 2 samples. Most of each is outside the 1 statement: Worker → DO took 659 ms and 213 ms, and was not broken down. The eviction threshold was not measured either; this run saw a boot after 100 s and after more than 2 min idle.
- **The prepared-statement leak**, listed as open in the PR, was not measured in this run.
- **Same limits as LATENCY.md section 6:**
  - `BEGIN` is not logged (0 lines here);
  - the timestamps hide CPU time;
  - the clock offset between this machine and Cloudflare is unknown (attribution allowance −100/+150 ms);
  - HTTP tail events are sampled.
- **No tail event was marked `truncated`.**

## Teardown

Done 2026-11-12, about 02:00 UTC.

### Removed from Cloudflare

| What | Command | Result |
|---|---|---|
| Worker `instant-worker`, with its Durable Object `node-8` | `npx wrangler delete --force` | `Successfully deleted instant-worker`, exit 0 |
| Hyperdrive config `instant` (`d2210fa0f5ca4bdbbc6b5ea414101df1`) | `npx wrangler hyperdrive delete d2210fa0f5ca4bdbbc6b5ea414101df1` | `✅ Deleted`, exit 0 |

- `npx wrangler hyperdrive list` now lists no configs.
- `curl -i https://instant-worker.leerosen686.workers.dev/health` now returns `HTTP/2 404`, body `error code: 1042`.

### Removed locally

- **`crates/instant-worker/wrangler.toml`:** restored to the committed version. This run's local copy held the Hyperdrive id and the test vars, not the connection string.
- **Scratchpad data:** the raw tail, the raw A5 timings, the test app id, the copies of the test scripts and the analysis scripts.
- **Final scan:** `/usr/bin/grep -rIl` over the working tree, including gitignored files, and `git grep` on `HEAD` find no Neon host and no `npg_` password.

### Still existing

These are the same items listed under Teardown in LATENCY.md:
- **The Neon project.** It now also holds this run's test apps. Delete it in the Neon console.
- **`~/.iw-database-url`:** a file holding the connection string (`rm ~/.iw-database-url`).
- **`wrangler` OAuth login.** It was already logged in before the test (`npx wrangler logout`).
- **Docker image `postgres:17`**, pulled for the `psql` shim.
- **Build toolchain:** the Rust beta toolchain, `worker-build`, and the Emscripten SDK cache.
- **Build output:** `crates/instant-worker/target` and `build`.
- **Per-step logs** for this run (`R*`, `T1` in `deploy-test-logs/latency2/`) exist only on the test machine, as agreed.
