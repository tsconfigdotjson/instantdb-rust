# instant-worker: where the latency goes

Date: 2026-11-11. Times are epoch ms on the clock named in each row; local is EST.

**Setup**
- Deployment: `instant-worker` version `b6805425` (deploy output in `A1-deploy.log`). The code is the same as the previous deploy `1e82065f`; only the vars changed (`A1-config.log`):
  - `RUST_LOG = "info,sqlx=debug"`
  - `NODE_NAME = "node-7"`
- Network path: client in Toronto (Cloudflare `colo=YYZ`) → Worker → Durable Object `node-7` → Hyperdrive (caching off) → Neon Postgres in AWS `us-east-2`.
- Data source: `wrangler tail --format json`, written outside the repo. `tail-sanitized.jsonl` is a copy without IPs, geolocation or URLs, with every UUID replaced by `<uuid>`.
- Analysis: the files in this directory were produced from the raw tail by the scripts `/tmp/iw-lat-parse.py`, `/tmp/iw-a5-analyze.py` and `/tmp/iw-a5.mjs`.

**sqlx statement logging works.** With `sqlx=debug`, every statement appears as a `DEBUG sqlx::query` line carrying `elapsed`. The tail has 1370 statement lines.

Two properties of these lines limit what can be measured (details in section 6):
- They contain **0 `BEGIN` lines**, against 92 `COMMIT` lines. Transaction starts are therefore not visible.
- **Timestamps advance only at I/O.** The time between two consecutive statements on one connection always reads 0 ms, so CPU time cannot be seen in the timestamps.

## 1. Summary table

**Columns:**
- **Client median:** measured on this machine.
- **Statements:** the `sqlx::query` lines for that operation that complete before the client receives the reply (median, with min–max).
- **Sum:** the median, over the 20 operations, of each operation's summed `elapsed`.
- **Remainder:** client median minus that sum.

**Sources:** A3, A5 and A2. Each A5 kind has 20 samples. Background-loop statements are excluded from the counts (see section 5).

| operation | client median | statements | sum of statement time | remainder |
|---|---|---|---|---|
| `/health`, warm, reused HTTP connection (A3, TTFB − pretransfer) | 48.1 ms | 0 | 0 | 48.1 ms |
| a. `init` → `init-ok`, new websocket each time | 63.3 ms | 0 (0–0) | 0 | 63.3 ms |
| b. `add-query`, distinct query each time | 242.5 ms | 8 (8–9) | 189.0 ms | 53.5 ms |
| c. `add-query`, same query each time | 27.0 ms | 0 (0–8) | 0 | 27.0 ms |
| d. `transact` → `transact-ok`, one `add-triple` | 295.1 ms | 10 (10–10) | 254.5 ms | 40.6 ms |
| e. `transact` on session 1 → `refresh-ok` on session 2 | 482.4 ms | 16 (16–17) | 406.0 ms | 76.4 ms |
| cold start: first `/health` to a fresh object (3 timed samples, TTFB) | 4620 ms | 96 (96–96) | 3672 ms | 948 ms |

Notes on the table:
- **a:** the websocket open before each `init` took a median of **328.5 ms**. It is not included in the 63.3 ms.
- **c:** only the first of the 20 was a new query (223.3 ms, 8 statements). The other 19 ran 0 statements and took 22.2–92.2 ms.
- **e:** `transact-ok` reached session 1 after a median of 297.8 ms. Of the 16 statements, 10 belong to the transact (the same as d), and 6 to the refresh.
- **Per-statement time:** across all 690 A5 operation statements, `elapsed` was min 17.0, **median 25.0**, max 55.0 ms.
- **Cold start:** the three timed samples were 5390, 4620 and 3647 ms. Their statement sums were 3672, 3712 and 3290 ms.

## 2. Cold start

**Boots:** the object booted four times during this test (`A2-boots.txt`). Every boot ran the same **96 statements** before the `storage sweeper started` line.

**Columns:**
- **Worker → DO:** from the Worker event start to the Durable Object event start, both on Cloudflare's clock.
- **Before first statement:** from the Durable Object event start to the first statement start.

| boot | trigger | client TTFB | Worker → DO | before first statement | statements | sum of elapsed | DO start → sweeper started |
|---|---|---|---|---|---|---|---|
| 1 | A2: first request after the deploy | 5389.6 ms | 615 ms | 757 ms | 96 | 3672 ms | 4576 ms |
| 2 | A3 run 1, request 1 | 4620.4 ms | 547 ms | 38 ms | 96 | 3712 ms | 3908 ms |
| 3 | A5 setup `init` | not recorded | 323 ms | 33 ms | 96 | 3661 ms | 3868 ms |
| 4 | A6 eviction check | 3647.4 ms | 155 ms | 34 ms | 96 | 3290 ms | 3352 ms |

**Gaps between statements.** Apart from the gap before statement 1, every boot has one more gap of 25–171 ms (145, 156, 171 and 25 ms in boots 1–4), before statement 2. All other gaps are 0–3 ms.

**CPU time.** The Durable Object event's `cpuTime` was 352, 352, 442 and 302 ms. These events stay open past the response, so the figures include background loops. The Worker events' `cpuTime` was 2–27 ms.

### Boot 1, in order

The full per-statement list for boot 1 is in `A2-cold-statements.txt`. Here, `+ms` is the time since the Durable Object event start at which the statement (or group's first statement) completed.

| # | +ms | count | elapsed ms | SQL |
|---|---|---|---|---|
| 1 | +788 | 1 | 31 | `SELECT pg_advisory_xact_lock(772677321)` |
| 2 | +985 | 1 | 52 | `SELECT pg_advisory_xact_lock(772677322)` |
| 3 | +1058 | 1 | 73 | `INSERT INTO instant_users (id, email) VALUES ($1, 'system-catalog@instantdb.com') ON CONFLICT (id) DO NOTHING` |
| 4 | +1123 | 1 | 65 | `INSERT INTO apps (id, creator_id, title) VALUES ($1, $2, 'System catalog') ON CONFLICT (id) DO NOTHING` |
| 5–61 | +1214 | 57 | 1553 total (24–91 each) | `WITH fwd AS ( INSERT INTO idents (id, app_id, attr_id, etype, label) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING ), rev AS ( INSERT INTO idents (id, app_id, attr_id, etype, label) SELECT $6, $2` |
| 62 | +2728 | 1 | 52 | `SELECT set_config('instant.allow_system_catalog_app_attr_update', 'true', false)` |
| 63 | +2782 | 1 | 54 | `UPDATE attrs SET inferred_types = (CASE WHEN label = 'meta' THEN 8 ELSE 2 END)::bit(32) WHERE app_id = $1 AND inferred_types IS NULL` |
| 64 | +2832 | 1 | 50 | `SELECT set_config('instant.allow_system_catalog_app_attr_update', 'false', false)` |
| 65 | +2858 | 1 | 26 | `COMMIT` |
| 66 | +3023 | 1 | 163 | `CREATE UNLOGGED TABLE IF NOT EXISTS rust_presence ( … )` |
| 67 | +3075 | 1 | 52 | `CREATE UNLOGGED TABLE IF NOT EXISTS rust_nodes ( … )` |
| 68 | +3120 | 1 | 45 | `CREATE UNLOGGED TABLE IF NOT EXISTS rust_spill ( … )` |
| 69 | +3171 | 1 | 51 | `CREATE TABLE IF NOT EXISTS rust_tx_changes ( … )` |
| 70 | +3224 | 1 | 53 | `CREATE INDEX IF NOT EXISTS rust_tx_changes_app_tx_idx ON rust_tx_changes (app_id, tx_id)` |
| 71 | +3289 | 1 | 65 | `CREATE OR REPLACE FUNCTION rust_capture_triple_change() RETURNS trigger AS $fn$ …` |
| 72 | +3336 | 1 | 47 | `CREATE TABLE IF NOT EXISTS rust_rate_limit_buckets ( … )` |
| 73 | +3388 | 1 | 52 | `CREATE OR REPLACE FUNCTION rust_notify_rules_changed() …` |
| 74 | +3448 | 1 | 60 | `DROP TRIGGER IF EXISTS rust_rules_trigger ON rules` |
| 75 | +3504 | 1 | 56 | `CREATE TRIGGER rust_rules_trigger AFTER INSERT OR UPDATE OR DELETE ON rules …` |
| 76 | +3550 | 1 | 46 | `CREATE OR REPLACE FUNCTION rust_notify_app_status() …` |
| 77 | +3604 | 1 | 54 | `DROP TRIGGER IF EXISTS rust_app_status_trigger ON apps` |
| 78 | +3654 | 1 | 50 | `CREATE TRIGGER rust_app_status_trigger AFTER UPDATE OF status ON apps …` |
| 79 | +3710 | 1 | 56 | `DROP TRIGGER IF EXISTS rust_capture_trigger ON triples` |
| 80 | +3764 | 1 | 54 | `DROP TRIGGER IF EXISTS rust_capture_ins ON triples` |
| 81 | +3817 | 1 | 53 | `CREATE TRIGGER rust_capture_ins AFTER INSERT ON triples …` |
| 82 | +3863 | 1 | 46 | `DROP TRIGGER IF EXISTS rust_capture_upd ON triples` |
| 83 | +3914 | 1 | 51 | `CREATE TRIGGER rust_capture_upd AFTER UPDATE ON triples …` |
| 84 | +3960 | 1 | 46 | `DROP TRIGGER IF EXISTS rust_capture_del ON triples` |
| 85 | +4010 | 1 | 50 | `CREATE TRIGGER rust_capture_del AFTER DELETE ON triples …` |
| 86 | +4054 | 1 | 44 | `CREATE TABLE IF NOT EXISTS rust_blobs ( … )` |
| 87 | +4108 | 1 | 54 | `CREATE TABLE IF NOT EXISTS rust_server_config ( … )` |
| 88 | +4163 | 1 | 55 | `INSERT INTO rust_server_config (key, value) VALUES ('server_secret', $1) ON CONFLICT (key) DO NOTHING` |
| 89 | +4216 | 1 | 53 | `SELECT value FROM rust_server_config WHERE key = 'server_secret'` |
| 90 | +4245 | 1 | 29 | `COMMIT` |
| 91 | +4294 | 1 | 49 | `CREATE OR REPLACE FUNCTION rust_notify_webhooks_changed() …` |
| 92 | +4353 | 1 | 59 | `DROP TRIGGER IF EXISTS rust_webhooks_changed_trigger ON webhooks` |
| 93 | +4413 | 1 | 60 | `CREATE TRIGGER rust_webhooks_changed_trigger AFTER INSERT OR UPDATE OR DELETE ON webhooks …` |
| 94 | +4459 | 1 | 46 | `CREATE TABLE IF NOT EXISTS rust_webhook_history ( … )` |
| 95 | +4514 | 1 | 55 | `INSERT INTO rust_server_config (key, value) VALUES ('webhook_signing_key', $1), ('webhook_kid', $2) ON CONFLICT (key) DO NOTHING` |
| 96 | +4576 | 1 | 62 | `SELECT key, value FROM rust_server_config WHERE key IN ('webhook_signing_key', 'webhook_kid')` |

**Boot 1 totals:**
- 96 statements, 3672 ms of `elapsed`.
- By group:

| statements | what | count | time |
|---|---|---|---|
| #1–4 | locks and catalog rows | 4 | 221 ms |
| #5–61 | `idents` inserts | 57 | 1553 ms, 27 ms median each |
| #62–65 | `set_config`, `UPDATE`, `COMMIT` | 4 | 182 ms |
| #66–96 | DDL, server config, triggers | 31 | 1716 ms |

- In #66–96, statements with no bind parameters took 44–65 ms each; the exception is #66 at 163 ms.

**Boot 1 wall time:**
- Client TTFB 5390 ms. The request reached the Worker about 134 ms after curl started; that includes DNS, connect and TLS (60 ms) plus the clock offset.
- Worker event start → Durable Object event start: 615 ms.
- Durable Object start → first statement start: 757 ms.
- Statements, including the 145 ms gap before #2: about 3.8 s.
- Durable Object start → `storage sweeper started`: 4576 ms.

## 3. Baselines

### Network, no database (A3, `A3-network.log`)

- **Method:** 20 × `/health` in one `curl` invocation, which reuses one connection (`num_connects` = 1 in total). The figure is TTFB minus pretransfer, i.e. excluding DNS, connect and TLS.
- **Run 2, warm:** min **35.3**, median **48.1**, max **78.4** ms.
- **Run 1:** its first request was a cold start (4591.8 ms). The other 19 had a median of 62.2 ms.
- **Server side:** for the run 2 requests the tail recorded, Worker event `wallTime` was 28–46 ms and Durable Object event `wallTime` 40–81 ms. The tail recorded only 11 of the 20 requests (see section 6).
- **Client edge:** `colo=YYZ`.

### Database from this machine (A4, `A4-database.log`)

- **Method:** `psql` with `\timing on`, `select 1` × 10 on one connection.
- **Result:** min 19.8, **median 20.5**, max 22.0 ms.
- **Caveat:** `psql` runs in Docker Desktop on macOS (`postgres:17` image), which adds its own network layer.
- **Database:** Neon, host suffix `c-8.us-east-2.aws.neon.tech` (AWS us-east-2), PostgreSQL 18.6.

### Database from the Durable Object, through Hyperdrive

- Median sqlx `elapsed` over 690 statements in A5: **25.0 ms** (min 17.0, max 55.0).
- In the cold boots, statements without bind parameters (DDL, `set_config`) took 44–65 ms each.

## 4. Representative operations (A5, operation #10 of each kind)

Full lists for all 100 operations are in `A5-attribution.txt`, and the per-statement detail with gaps is in `A5-representative.txt`.
- **Gap:** this statement's start minus the previous statement's end. Negative means two statements overlapped on different connections.
- SQL is truncated to 200 characters.

### b. `add-query`, distinct query: client 214.6 ms, 8 statements, 186 ms

| # | elapsed ms | gap ms | SQL |
|---|---|---|---|
| 1 | 25.0 | | `SELECT code FROM rules WHERE app_id = $1` |
| 2 | 22.0 | 0 | `SELECT coalesce(max(id), 0) AS n FROM transactions WHERE app_id = $1` |
| 3 | 23.0 | 0 | `SELECT '0/' \|\| pg_current_wal_lsn()::text AS isn` |
| 4 | 22.0 | 23 | `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY` |
| 5 | 23.0 | 0 | `SELECT idt.entity_id AS eid, idt.value AS order_v, idt.created_at AS order_t, idt.created_at AS sort_t, NULL::float8 AS sort_n, NULL::text AS sort_s, NULL::boolean AS sort_b, NULL::timestamptz AS sort` |
| 6 | 23.0 | 0 | `SELECT DISTINCT c.eid FROM (SELECT a.entity_id AS eid FROM triples a WHERE a.app_id = $1 AND a.attr_id = $2) c WHERE c.eid IS NOT NULL AND NOT EXISTS (SELECT 1 FROM triples i WHERE i.app_id = $3 AND i` |
| 7 | 23.0 | 0 | `COMMIT` |
| 8 | 25.0 | 0 | `SELECT coalesce(max(id), 0) AS n FROM transactions WHERE app_id = $1` |

### d. `transact`, one `add-triple`: client 275.4 ms, 10 statements, 241 ms

| # | elapsed ms | gap ms | SQL |
|---|---|---|---|
| 1 | 26.0 | | `SELECT pg_backend_pid() AS pid, txid_current() AS xid, set_config('statement_timeout', $1, true)` |
| 2 | 23.0 | 0 | `SELECT status FROM apps WHERE id = $1` |
| 3 | 23.0 | 0 | `SELECT code FROM rules WHERE app_id = $1` |
| 4 | 25.0 | 0 | `SELECT attr_id, value FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = ANY($3)` |
| 5 | 23.0 | 0 | `WITH t AS (INSERT INTO transactions (app_id) VALUES ($1) RETURNING id) SELECT id, set_config('instant.rust_tx_id', id::text, true) AS tag FROM t` |
| 6 | 24.0 | 0 | `INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5, ea, eav, av, ave, vae, checked_data_type) SELECT $1, e, a, v::jsonb, md5((v::jsonb)::text), ea, eav, av, ave, vae, cdt::checked_data_` |
| 7 | 23.0 | 0 | `SELECT 1 AS x FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3` |
| 8 | 23.0 | 0 | `COMMIT` |
| 9 | 24.0 | 0 | `SELECT '0/' \|\| pg_current_wal_lsn()::text AS isn` |
| 10 | 27.0 | −24 | `SELECT entity_id, attr_id, value FROM rust_tx_changes WHERE app_id = $1 AND tx_id = ANY($2)` |

### e. `transact` on session 1 → `refresh-ok` on session 2: client 498.6 ms, 16 statements, 406 ms

| # | elapsed ms | gap ms | SQL |
|---|---|---|---|
| 1 | 25.0 | | `SELECT pg_backend_pid() AS pid, txid_current() AS xid, set_config('statement_timeout', $1, true)` |
| 2 | 25.0 | 0 | `SELECT status FROM apps WHERE id = $1` |
| 3 | 26.0 | 0 | `SELECT code FROM rules WHERE app_id = $1` |
| 4 | 26.0 | 0 | `SELECT attr_id, value FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = ANY($3)` |
| 5 | 25.0 | 0 | `WITH t AS (INSERT INTO transactions (app_id) VALUES ($1) RETURNING id) SELECT id, set_config('instant.rust_tx_id', id::text, true) AS tag FROM t` |
| 6 | 27.0 | 0 | `INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5, ea, eav, av, ave, vae, checked_data_type) SELECT $1, e, a, v::jsonb, md5((v::jsonb)::text), ea, eav, av, ave, vae, cdt::checked_data_` |
| 7 | 26.0 | 0 | `SELECT 1 AS x FROM triples WHERE app_id = $1 AND entity_id = $2 AND attr_id = $3` |
| 8 | 26.0 | 0 | `COMMIT` |
| 9 | 21.0 | 0 | `SELECT entity_id, attr_id, value FROM rust_tx_changes WHERE app_id = $1 AND tx_id = ANY($2)` |
| 10 | 25.0 | −21 | `SELECT '0/' \|\| pg_current_wal_lsn()::text AS isn` |
| 11 | 24.0 | −4 | `SELECT code FROM rules WHERE app_id = $1` |
| 12 | 27.0 | 27 | `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY` |
| 13 | 27.0 | 0 | `SELECT idt.entity_id AS eid, idt.value AS order_v, idt.created_at AS order_t, idt.created_at AS sort_t, NULL::float8 AS sort_n, NULL::text AS sort_s, NULL::boolean AS sort_b, NULL::timestamptz AS sort` |
| 14 | 26.0 | 0 | `SELECT entity_id, attr_id, value, created_at FROM triples WHERE app_id = $1 AND entity_id = ANY($2) AND attr_id = ANY($3) AND ea` |
| 15 | 26.0 | 0 | `COMMIT` |
| 16 | 24.0 | 0 | `SELECT '0/' \|\| pg_current_wal_lsn()::text AS isn` |

Where the gaps are:
- Transaction starts: the 23 ms gap before b#4 and the 27 ms gap before e#12. No `BEGIN` line is logged.
- The first statement of each operation started 37 ms (b), 61 ms (d) and 68 ms (e) after the client sent, but the two clocks are not synchronised.

## 5. Idle statements (A6, `A6-idle.txt`)

**Window:** 1794442446019 – 1794442566019, Cloudflare clock. It runs 120 s, starting 5 s after the last A5 client closed. No client was connected.

**Totals:**
- **38 statements** in 120 s, i.e. **19 per minute** over the window.
- All 38 fall between 1794442453062 and 1794442513920, a span of 61 s (about 37 per minute). There were none in the last 52 s of the window.
- The next request (`A6-eviction-check.log`) took 3647 ms and booted the object again (boot 4 in section 2).

**Statements by loop.** Every statement logs under target `sqlx::query`, so the target does not name the loop. Instead, each SQL text was matched to the file in `crates/instant-server/src` that contains it.

| source | statements in window | SQL | interval seen |
|---|---|---|---|
| `presence.rs` | 35 | `INSERT INTO rust_nodes … heartbeat`, `DELETE FROM rust_presence …`, `DELETE FROM rust_nodes …`, `DELETE FROM rust_spill …`, `DELETE FROM rust_tx_changes …` (7 each) | 5 statements every ~10 s |
| `indexing_jobs.rs` | 2 | `UPDATE indexing_jobs SET worker_id = NULL …`, `SELECT id FROM indexing_jobs …` | once |
| `usage.rs` | 1 | `WITH ids AS ( SELECT id FROM triples_size_updates … )` | once |

**Other loops.** In the 71 s after boot 1, the following also ran (visible in `tail-sanitized.jsonl`):
- webhooks (`webhook_events`);
- the storage sweeper (`app_files_to_sweep`);
- hard delete (`pg_try_advisory_xact_lock`, `deletion_marked_at`).

These did not run inside the A6 window.

## 6. What did not work or was not measured

**Not visible in the logs:**
- **Time between statements.** On Workers, `Date.now()` and the timers sqlx uses advance only when I/O completes. So:
  - log timestamps between consecutive statements on one connection differ by exactly the next statement's `elapsed`;
  - CPU work between statements is not visible;
  - the "remainder" column mixes network transfer, unlogged round trips, and CPU.

  The only CPU figure available is each event's `cpuTime` (section 2).
- **`BEGIN`.** It is not logged (0 lines out of 1370 statements). Unlogged gaps of 23–27 ms appear where transactions start (section 4), and 25–171 ms before boot statement #2. What happens in those gaps was not measured.
- **The split inside a statement's `elapsed`.** How much is Durable Object → Hyperdrive, Hyperdrive → Neon, or Postgres execution was not measured.

**Not measured:**
- **Where the Durable Object runs.** Neither the response nor the tail says.
- **The clock offset** between this machine and Cloudflare. Statements were attributed to an operation if they completed between 100 ms before the client sent and 150 ms after the client received.
  - In A5 that window held 690 statements.
  - Another 10 statements fell between an operation's reply and the next operation's start: 8 after d #20 and 1 after c #20 (setup for the next phase ran there), and 1 after a #11. They are not counted in the table.
  - Background-loop statements were excluded by table name: `rust_nodes`, `rust_presence`, `rust_spill`, `DELETE FROM rust_tx_changes`, `triples_size_updates`, `indexing_jobs`, `webhook_events`, `app_files_to_sweep`, `deletion_marked_at`, `pg_try_advisory_xact_lock`.
- **The 328.5 ms websocket open** (a) was measured but not broken down.
- **The eviction threshold.** The object booted again after 69 s, 72 s and 105 s without requests. 69 s is the shortest observed; the exact threshold was not measured.
- **Boot 3's client time.** It happened inside the A5 setup `init`, which was not timed.

**Deviations from the plan:**
- **Item c** was sent sequentially with the 2 s attribution gap, not back to back without waiting.
- **Item d** ran on a fresh session with no subscriptions. The session used for b and c was closed first, so its subscriptions would not add refresh statements.
- **Item e** used two new sessions, with session 2 subscribed to `{"todos":{}}`.
- **A3 run 1** started with a cold start, so run 2 (warm) is the baseline.
- **The A2 tail** had sqlx statement lines on the first try. `RUST_LOG = "debug"` was not needed.

**The tail samples HTTP events.** In each A3 run, the tail recorded 11 of the 20 `/health` requests (Worker and Durable Object events alike). The A5 websocket operations show consistent statement counts (b 8–9, d 10, e 16–17 in every one of the 20), so none looks to be missing statements, but sampling of log lines was not tested directly.

**Other:** no tail event was marked `truncated`, and no exceptions were logged.

## Teardown

Pending: waiting for the go-ahead.
