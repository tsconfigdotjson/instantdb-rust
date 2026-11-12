# instant-worker on Cloudflare, round 4: diagnostics

Date: 2026-11-12, 03:45–08:20 UTC. Branch head `862a8b5b9abb3fd9e5783163dae0cebee096ec62`. The code is the same as round 3; only docs changed since.

**Scope.** Measurements only: no Rust source changes. The database is the same Neon Postgres (AWS us-east-2) used in rounds 1–3; it already had the migrations.

**Settings common to every deploy:**
- Hyperdrive `6697852637ab43e388b37d1dcdc0993b`, created with `--caching-disabled`.
- `BASE_URL = "https://instant-worker.leerosen686.workers.dev"`, `INSTANT_HANDLE_RECEIVE_TIMEOUT_MS = "30000"`, `NODE_LOCATION_HINT = "enam"`, `RUST_LOG = "info,sqlx=debug"`.
- `INSTANT_TRACE_REFRESH = "1"`, a server config var. It makes the server log `refresh session … stale=N` and `refresh send … recomputed=N` for every refresh.

**Tail.** `npx wrangler tail --format json` ran to a file outside the repo, in a restart loop that records every exit. It started once (1794455267139) and **never disconnected** before teardown. `tail-sanitized.jsonl.gz` is the committed copy: no IPs, geolocation or URLs; UUIDs, app ids and tokens replaced. It is gzipped because it is 38.8 MB uncompressed (100046 log lines, mostly `sqlx::query` statements); `gunzip -k` restores it.

## 1. Summary

| section | status | headline |
|---|---|---|
| A. Where the time goes | **ran**: all 5 configurations, 5 min each, 0 failed operations | **Only 1 subscription query** ran per transact whether 1, 5 or 20 sessions were subscribed; the trace logs 1/5/20 recomputations. Medians (median statement elapsed): **A1, A3, A5 transact 264–281 ms (23–25 ms)**; **A2, A4 transact 541–548 ms (49–50 ms)**. No `slow`/`acquire`/`pool`/`timed out`/`exceeded` lines. PG_POOL_MAX=10 (A4) ran without errors. |
| B. Dropped database connections | **ran**: B1, B2, B3 | **B1:** 1 failed op after the kill, first success 1.4 s after it, no drops, no reboot. **B2:** 1 failed op, then **no subscription refreshed for the rest of the run** (99 of 99 refresh waits timed out at 60 s; every later `refresh session` logged `stale=0`) while every transact succeeded. **B3:** 12 min idle, then all four operations succeeded first time. |
| C. 3-hour soak | **ran**: full 3 h (05:10:55Z–08:11:37Z), version `ede99d51` | **No drift:** transact median per 30 min 511–549 ms, refresh 797–879 ms, statement elapsed median 44–48 ms per checkpoint. 1079/1079 refreshes on time, 0 late or missing, 0 boots. **1 websocket drop** (session 4, code 1006, at 08:09:06Z), reconnected in 1.7 s; 1 transact skipped while it was down. Only tail warning: 91 × `webhook delivery failed`. |
| D. Connection capacity | **ran**: all stages up to 2000 | **2000 idle sessions held**: 0 drops, `/health` 200 (81–365 ms), transact → refresh-ok 0.97–1.42 s at every stage. The stop condition never triggered. |

## Setup

- **S1.** `git rev-parse HEAD` → `862a8b5b9abb3fd9e5783163dae0cebee096ec62`.
- **S2.** Created the Hyperdrive config: `npx wrangler hyperdrive create instant --connection-string=<REDACTED> --caching-disabled` printed `✅ Created new Hyperdrive PostgreSQL config: 6697852637ab43e388b37d1dcdc0993b`. Every configuration below was a separate deploy with a new `NODE_NAME`. Each deploy printed `Worker Startup Time` 65–69 ms.
- **S3.** The load script is `/tmp/iw-r4/load.mjs`, round 3's workload with these changes:
  - every transact writes `title = "w-<counter>-<epoch ms>"`, always a new value, and logs that value;
  - each refresh is matched to its transact by `processed-tx-id >= tx-id`;
  - besides the target session, the time until every subscribed session has refreshed is recorded.

## 2. A: where the time goes under load

**Method.**
- **Deploy and boot:** each configuration was a deploy with a new `NODE_NAME`, then `/health` every 5 s until the tail showed that version's `storage sweeper started` line (11–12 s each).
- **Load:** on a fresh app, 5 min at one transact and one new-shape add-query (plus its remove-query) every 2 s, offset by 1 s. Every session subscribed to `{"todos":{}}`.
- **Tail log sink.** The tail caps logs at 256 KB per request:
  ```
  Log size limit exceeded: More than 256KB of data (across console.log statements, exception, request metadata and headers) was logged during a single request. Subsequent data for this request will not be recorded in logs, appear when tailing this Worker's logs, or in Tail Workers.
  ```
  A probe (P0) showed that the object's logs are charged to its newest request, not to the session doing the work. So each run kept one extra idle websocket (connected, never initialised) and replaced it every 5 s (every 3 s in A5). **No event in A1–A5 was truncated.**

**How statements were attributed.**
- **Transact:** the statements matching the transact's own SQL (`pg_backend_pid`, `apps.status`, the triples read, `INSERT INTO transactions`, `INSERT INTO triples`, `SELECT 1 AS x`, `rust_tx_changes`), plus its rules read, `COMMIT` and LSN read, inside [send − 100 ms, reply + 150 ms].
- **Add-query:** the new-shape query statements and their `COMMIT`, plus the `max(id)` read logged just after the reply.
- **Refresh:** the refresh-type statements (the subscription query, its triples read, rules, `COMMIT`, LSN) between the transact's `COMMIT` and the target session's `refresh-ok`.
- **Remove-query:** any query statements in its window. That figure is an upper bound; the A2 run shows 0.

**Unaccounted** = client median − median per-op statement sum.

### A table

| config | sessions | PG_POOL_MAX | kind | ok / failed | client min / median / p95 / max (ms) | statements per op, median (range) | statement sum per op, median (ms) | unaccounted (ms) |
|---|---|---|---|---|---|---|---|---|
| A1 | 1 | 5 | transact | 150 / 0 | 236 / 267 / 392 / 802 | 10 (9–11) | 230 | 37 |
| A1 | 1 | 5 | refresh (target session; with 1 session it is the same session) | 150 / 0 | 386 / 425 / 567 / 958 | 6 (6–6) | 151 | 274 |
| A1 | 1 | 5 | add-query | 150 / 0 | 150 / 374 / 527 / 641 | 5 (3–10) | 146 | 228 |
| A1 | 1 | 5 | remove-query | 150 / 0 | 29 / 31 / 35 / 61 | 1 (0–2) | 23 | 8 |
| A2 | 5 | 5 | transact | 150 / 0 | 478 / 541 / 640 / 1221 | 9 (9–10) | 436 | 105 |
| A2 | 5 | 5 | refresh (target session) | 150 / 0 | 770 / 873 / 998 / 1336 | 6 (6–8) | 308 | 565 |
| A2 | 5 | 5 | refresh, last of all 5 sessions | 150 | 772 / 876 / 1005 / 1459 | | | |
| A2 | 5 | 5 | add-query | 150 / 0 | 284 / 692 / 1017 / 1147 | 5 (3–10) | 290 | 402 |
| A2 | 5 | 5 | remove-query | 150 / 0 | 50 / 52 / 57 / 228 | 0 (0–1) | 0 | 52 |
| A3 | 5 | 2 | transact | 150 / 0 | 241 / 264 / 342 / 577 | 10 (9–10) | 233 | 31 |
| A3 | 5 | 2 | refresh (target session) | 150 / 0 | 397 / 431 / 492 / 776 | 6 (6–6) | 152 | 279 |
| A3 | 5 | 2 | refresh, last of all 5 sessions | 150 | 409 / 438 / 518 / 776 | | | |
| A3 | 5 | 2 | add-query | 150 / 0 | 147 / 356 / 508 / 589 | 5 (3–10) | 136 | 220 |
| A3 | 5 | 2 | remove-query | 150 / 0 | 20 / 25 / 31 / 67 | 2 (0–3) | 46 | −21 |
| A4 | 5 | 10 | transact | 150 / 0 | 485 / 548 / 660 / 984 | 9 (9–9) | 438 | 110 |
| A4 | 5 | 10 | refresh (target session) | 150 / 0 | 787 / 888 / 1018 / 1389 | 6 (6–8) | 308 | 580 |
| A4 | 5 | 10 | refresh, last of all 5 sessions | 150 | 795 / 892 / 1020 / 1396 | | | |
| A4 | 5 | 10 | add-query | 150 / 0 | 297 / 704 / 1041 / 1173 | 5 (3–10) | 289 | 414 |
| A4 | 5 | 10 | remove-query | 150 / 0 | 51 / 59 / 62 / 66 | 1 (0–1) | 47 | 12 |
| A5 | 20 | 5 | transact | 150 / 0 | 246 / 281 / 377 / 475 | 10 (8–10) | 244 | 37 |
| A5 | 20 | 5 | refresh (target session) | 150 / 0 | 398 / 456 / 583 / 728 | 6 (6–6) | 160 | 296 |
| A5 | 20 | 5 | refresh, last of all 20 sessions | 150 | 408 / 468 / 602 / 750 | | | |
| A5 | 20 | 5 | add-query | 150 / 0 | 145 / 375 / 541 / 678 | 5 (3–10) | 150 | 225 |
| A5 | 20 | 5 | remove-query | 150 / 0 | 20 / 26 / 31 / 38 | 2 (0–4) | 65 | −39 |

### Per run

| config | node / version | statement elapsed, all statements in the run (excl. background loops): median / p95 (ms) | sessions refreshed per transact (trace `refresh session` lines carrying its tx id) | recomputations per transact (sum of `stale=`) | subscription-query statements (`SELECT idt.…`) from the transact's COMMIT until the last session's refresh-ok, median (range) | refresh-type statements over the whole run |
|---|---|---|---|---|---|---|
| A1 | node-12 / bad87356 | 23.0 / 59.0 | 1 | 1 | 1 (1–1) | 654 |
| A2 | node-13 / 94fc3f0e | 49.0 / 114.0 | 5 | 5 | 1 (1–2) | 652 |
| A3 | node-14 / 145564be | 23.0 / 61.0 | 5 | 5 | 1 (1–1) | 652 |
| A4 | node-15 / 6b010d40 | 50.0 / 111.0 | 5 | 5 | 1 (1–2) | 652 |
| A5 | node-16 / 942d82fd | 25.0 / 63.0 | 20 | 20 | 1 (1–1) | 652 |

**Refreshes with N sessions.** For one transact, the server logs a `refresh session … stale=1` line for each subscribed session (1, 5 or 20), and a `refresh send … recomputed=1` for each. Yet only one subscription-query statement runs between the COMMIT and the last session's refresh-ok. The refresh-type statement count over a run is 652–654 whether 1, 5 or 20 sessions are subscribed. So the SQL does not grow with N here; the N "recomputations" in the trace are not N queries.

**Statements between the transact's COMMIT and the target session's refresh-ok:** median 6 refresh-type statements in every run (range 6–8). In order: LSN read, rules read, subscription query, its triples read, COMMIT, LSN read. The transact's own `rust_tx_changes` read also runs in this span, but is counted with the transact.

**A2 and A4 against the others.** Both have about twice the median statement elapsed (49–50 ms against 23–25 ms), about twice the transact and refresh latency, and about twice the remove-query time, an operation with ~0 statements (52–59 ms against 25–31 ms). Neither pool size nor session count matches this split: A3 (pool 2) and A5 (20 sessions) are in the fast group. Where each object ran was not measured; the location is not exposed.

**Tail lines** containing `slow`, `acquire`, `pool`, `timed out` or `exceeded`: **none** in A1–A5. Of the patterns, only the SQL text of some statements matched, and none was a message.

**Errors, drops, late refreshes:** none in A1–A5. No refresh took over 5 s; every transact's refresh reached every subscribed session.

### pg_stat_activity during each run (3 samples, rows `count|state`)

| config | sample 1 | sample 2 | sample 3 |
|---|---|---|---|
| A1 | 1 active, 8 idle | 1 active, 8 idle | 1 active, 8 idle |
| A2 | 1 active, 7 idle, 1 idle in transaction | 1 active, 8 idle | 1 active, 8 idle |
| A3 | 1 active, 7 idle | 1 active, 6 idle, 1 idle in transaction | 1 active, 5 idle, 1 idle in transaction |
| A4 | 1 active, 7 idle | 1 active, 7 idle | 1 active, 7 idle, 1 idle in transaction |
| A5 | 1 active, 10 idle | 1 active, 10 idle | 1 active, 9 idle, 1 idle in transaction |

The `active` row is the sampling `psql` itself. A breakdown taken during B showed the others as `application_name = 'Cloudflare Hyperdrive'`. These are Hyperdrive's origin connections, not the Worker's pool, so the Worker-side connection count (relevant to the 6-outbound-connection limit with PG_POOL_MAX=10) was not measured. A4 showed no errors and no pool-related log lines.

## 3. B: a dropped database connection

**Setup.** Version `b7e780b7`, `node-17`, PG_POOL_MAX=5. Each of B1, B2 and B3 ran 5 sessions on a fresh app, each subscribed to `{"todos":{}}`. B1 and B2 sent one transact and one new-shape add-query (with remove-query) per second.

**The kill:**
```
select pg_terminate_backend(pid) from pg_stat_activity where datname = current_database() and pid <> pg_backend_pid() and application_name not like 'psql%';
```

### B1 (kill at 1794458302879)

- **Before the kill:** `application_name, state, count` was `Cloudflare Hyperdrive|idle|9`, `Cloudflare Hyperdrive|idle in transaction|1`, `psql|active|1`.
- **Terminated: 10** (10 × `t`, exit 0).
- **Failed operations:**
  - 1794458302559 (320 ms before the kill was issued; still in flight) add-query, session 3: `{"op":"error","type":"internal-error","message":"database error: error returned from database: Connection to the origin database was lost."}`
  - 1794458302879 + 179 ms, transact, session 2: `{"op":"error","type":"internal-error","message":"database error: error returned from database: Connection to the origin database was lost."}`
- **Failures before the first success after the kill:** 1.
- **First success:** an add-query sent at +682 ms, done at **+1402 ms**.
- **First latencies after the kill (ms):**

  | kind | first 5 |
  |---|---|
  | transact | 426, 479, 662, 413, 413 |
  | refresh | 718, 686, 1004, 661, 657 |
  | add-query | 720, 832, 270, 636, 411 |

- **Totals:** 551 operations after the kill, 1 failed.
- **Refresh delivery:** every one of the 137 transacts after the kill reached all 5 sessions.
- **Drops: none. Reboots: none.**
- **Tail:**
  - `2 × WARN sqlx_core::pool::connection: error occurred while testing the connection on-release error=error communicating with database: expected to read 5 bytes, got 0 bytes at EOF` (first at +351 ms);
  - `2 × WARN instant_server::webhooks: webhook delivery failed: Record not found: webhook`, also present outside B.

### B2 (kill at 1794458564381, same node and load, fresh app)

- **Before the kill:** `Cloudflare Hyperdrive|idle|4`, `Cloudflare Hyperdrive|idle in transaction|1`, `psql|active|1`.
- **Terminated: 5.**
- **Failed operations:**
  - 1794458564654 (+273 ms) add-query, session 3: `{"op":"error","type":"internal-error","message":"database error: error returned from database: Connection to the origin database was lost."}`
  - At 1794458564874, each of the 5 sessions received an unsolicited error frame (no client-event-id): `{"type":"internal-error","message":"database error: error returned from database: Connection to the origin database was lost."}`
  - **Refresh: 99 failures**, each `timeout 60000 ms` on the target session. They are for the transacts sent from 238 ms before the kill to +98.988 s, i.e. every transact whose 60 s wait ended before the script exited. Transacts in the last ~40 s had their waits cut off at exit.
- **Failures before the first success after the kill:** 1.
- **First success:** a transact sent at +764 ms, done at +1430 ms.
- **Transacts after the kill: 138 of 138 succeeded** (first five: 665, 442, 471, 701, 420 ms). Add-query and remove-query kept succeeding.
- **Refresh delivery: none of the 5 sessions received a refresh** for any of the 99 transacts recorded after the kill.
- **Drops: none. Reboots: none.**
- **Tail, verbatim, in order:**
  ```
  1794458564571  5 × INFO refresh_trace: refresh session app_id=<uuid> session=<uuid> admin=false tx_ids=[1881] files_changed=false stale=1 files=[]
  1794458564758  INFO refresh_trace: refresh query failed app_id=<uuid> query={"todos":{}} error=database error: error returned from database: Connection to the origin database was lost.
  1794458564758  2 × WARN sqlx_core::pool::connection: error occurred while testing the connection on-release error=error communicating with database: expected to read 5 bytes, got 0 bytes at EOF
  1794458565712  5 × INFO refresh_trace: refresh session app_id=<uuid> session=<uuid> admin=false tx_ids=[1882] files_changed=false stale=0 files=[]
  1794458566596  5 × INFO refresh_trace: refresh session app_id=<uuid> session=<uuid> admin=false tx_ids=[1883] files_changed=false stale=0 files=[]
  … (the same with stale=0 for every later transaction)
  ```
- **Trace counts:**

  | | `tx notify` | `refresh session` | `stale=` values | `refresh send` | `refresh query failed` |
  |---|---|---|---|---|---|
  | before the kill | 61 | 310 | 305 × 1, 5 × 0 | 305 | — |
  | after the kill | 139 | 725 | 6 × 1, 719 × 0 | **0** | 1 |
  | B1 after its kill, for comparison | 137 | 705 | 685 × 1, 20 × 0 | 685 | — |

### B3 (12 min with no operations, then one of each kind)

- **Setup:** 5 sessions connected and subscribed on a fresh app, then 720 s with no operations (1794458763336 → 1794459483346).
- **pg_stat_activity after 10 min idle:** 1 active, 5 idle.
- **Then:**

  | kind | result |
  |---|---|
  | transact | ok, 474 ms |
  | refresh | ok, 854 ms |
  | add-query | ok, 282 ms |
  | remove-query | ok, 40 ms |

- **Failures: 0. Drops: none. Reboots: none.**
- **Tail:** only `1 × WARN instant_server::webhooks: webhook delivery failed: Record not found: webhook`. No connection errors were logged during or after the idle period.

## 4. C: 3-hour soak

**Setup.** Version `ede99d51`, `node-18`, PG_POOL_MAX=5, a fresh app. 5 sessions, all subscribed to `{"todos":{}}`. Every 10 s, alternately: one transact (sessions take turns), or one new-shape add-query followed by its remove-query (offset by 5 s). Log sink replaced every 30 s. Duration 10800 s; the load script exited 0.

**Run window:** setup done 1794460255486 (05:10:55Z), end 1794471097780 (08:11:37Z), 3.01 h.

### Totals

| kind | attempted | succeeded | failed |
|---|---|---|---|
| transact | 1080 | 1079 | 1 |
| refresh (target session) | 1079 | 1079 | 0 |
| add-query | 1080 | 1080 | 0 |
| remove-query | 1080 | 1080 | 0 |
| init | 6 | 6 | 0 |
| reconnect | 1 | 1 | 0 |

**Distinct errors:** 1.
- `transact × 1: skipped: session 4 not ready`, at 1794470947753 (08:09:07Z). The load script skipped this transact because session 4 was reconnecting; it was never sent to the server.

### Drift: per 30-minute window (client ms, n / min / median / p95 / max)

| kind | 05:10:55–05:40:55 | 05:40:55–06:10:55 | 06:10:55–06:40:55 | 06:40:55–07:10:55 | 07:10:55–07:40:55 | 07:40:55–08:10:55 |
|---|---|---|---|---|---|---|
| transact | 180 / 475 / 549 / 662 / 1096 | 180 / 463 / 534 / 644 / 1027 | 180 / 463 / 543 / 657 / 1013 | 180 / 476 / 511 / 613 / 861 | 180 / 471 / 528 / 615 / 998 | 179 / 483 / 531 / 636 / 1107 |
| refresh | 180 / 762 / 879 / 1048 / 1395 | 180 / 746 / 847 / 968 / 1434 | 180 / 749 / 864 / 978 / 1324 | 180 / 762 / 797 / 885 / 1344 | 180 / 763 / 834 / 934 / 1354 | 179 / 785 / 845 / 936 / 1437 |
| add-query | 180 / 264 / 735 / 1020 / 1113 | 180 / 274 / 699 / 997 / 1749 | 180 / 271 / 717 / 997 / 1190 | 180 / 266 / 626 / 912 / 1056 | 180 / 240 / 682 / 919 / 1309 | 180 / 284 / 691 / 954 / 1123 |
| remove-query | 180 / 50 / 54 / 63 / 108 | 180 / 50 / 53 / 62 / 392 | 180 / 50 / 53 / 62 / 197 | 180 / 50 / 58 / 63 / 90 | 180 / 50 / 55 / 63 / 102 | 180 / 50 / 54 / 62 / 100 |

### Drift: 30-minute checkpoints

| checkpoint | transact, last-10-min median (n) | refresh | add-query | remove-query | statement elapsed median (n), tail window [−11, −1 min] | shapes so far | pg_stat_activity (`count|state`) | `/health` |
|---|---|---|---|---|---|---|---|---|
| 0 (before start) | — | — | — | — | 106 ms (2) | 0 | 1 active, 8 idle | 200, 0.097 s |
| 30 min | 543 ms (60) | 870 ms (60) | 748 ms (60) | 54 ms (60) | 48 ms (1427) | 177 | 1 active, 3 idle, 1 idle in transaction | 200, 0.124 s |
| 60 min | 546 ms (60) | 863 ms (60) | 741 ms (59) | 53 ms (59) | 47 ms (1424) | 357 | 1 active, 5 idle | 200, 0.130 s |
| 90 min | 540 ms (60) | 835 ms (60) | 720 ms (60) | 54 ms (60) | 46 ms (1482) | 538 | 1 active, 4 idle | 200, 0.108 s |
| 120 min | 531 ms (60) | 816 ms (60) | 688 ms (60) | 60 ms (60) | 44 ms (1436) | 718 | 1 active, 5 idle | 200, 0.121 s |
| 150 min | 531 ms (60) | 846 ms (60) | 697 ms (60) | 53 ms (60) | 47 ms (1446) | 898 | 1 active, 4 idle | 200, 0.127 s |
| 180 min | 535 ms (59) | 851 ms (59) | 699 ms (60) | 54 ms (60) | 47 ms (1382) | 1078 | 1 active, 5 idle | 200, 0.116 s |
| end | 536 ms (48) | 851 ms (48) | 690 ms (50) | 54 ms (50) | 47 ms (1302) | 1080 | 1 active, 5 idle | 200, 0.119 s |

- **Statement elapsed over the whole soak** (excluding the background loops): n = 25748, median 47.0 ms, p95 104.0 ms.
- **Provider (Neon console) figures:** not recorded.
- This run was in the slow group from section 2 (transact about 540 ms, statements about 47 ms), from its first checkpoint to its last.

### End of soak

- **Late or missing refreshes (> 5 s) on the target session: 0.** Every transact reached all 5 sessions within 60 s, and the last session never took more than 5 s.
- **Drops and reconnects:**
  - 1794470946719 (08:09:06Z): session 4 closed, code 1006, empty reason.
  - 1794470948135: session 4 `init` ok (112 ms); 1794470948452 (08:09:08Z): reconnect ok on attempt 1.
  - Session 4 then completed its add-query (733 ms) and refresh (816 ms) on schedule. The end record shows drops per session `[0, 0, 0, 0, 1]`.
  - **Tail around the drop:**
    - A Durable Object event at 1794470942299 (wallTime 31425 ms) ended `responseStreamDisconnected`, as do most session events in this tail.
    - Two `canceled` events at 1794470947835 (stateless) and 1794470948040 (Durable Object, wallTime 149832 ms).
    - No WARN/ERROR line, exception or boot in [drop − 15 s, drop + 5 s].
    - Which request the closed socket belonged to was not determined.
- **Boots (`storage sweeper started`) during the soak: 0.**
- **Tail warnings, errors and exceptions** in the soak window:
  - 91 × `WARN instant_server::webhooks: webhook delivery failed: Record not found: webhook` (first at 1794460300793).
  - Nothing else: no exceptions, no ERROR, no `exceeded`, no panics.
  - The same warning appears under every version deployed this round. Counts over the whole tail: `ede99d51` 94, `b7e780b7` 12, and 3–4 for each of the A versions.
- **Tail events in the soak window:**
  - 731 in total, 0 marked truncated.
  - Outcomes: 358 Durable Object `responseStreamDisconnected`, 344 stateless `responseStreamDisconnected`, 6 + 5 `ok`, 2 + 1 `canceled`.
- **Tail gaps and restarts:**
  - The tail loop recorded one start (1794455267139) and no exit during the soak.
  - Largest gaps between consecutive tail timestamps while load ran: 5227 ms (05:52:36Z), 4844 ms, 4832 ms, 4828 ms.
  - Gaps of 6.0–10.0 s at 08:10:56–08:11:33Z fall in the drain after the last operation.

## 5. D: connection capacity

**Setup.** Version `555dba8c`, `node-19`, PG_POOL_MAX=5, on a fresh app. Script `/tmp/iw-r4/dstage.mjs`: each session connects, sends `init`, subscribes to `{"todos":{}}`, then stays open. Sessions were opened 50 at a time.

**At each stage:**
1. wait 30 s;
2. count sessions still open;
3. `/health` from the same process;
4. one transact on an open session, timed to `refresh-ok` on another.

| stage (total sessions) | time to open the stage | open failures | still open after 30 s | drops | `/health` | transact → transact-ok | transact → refresh-ok on another session |
|---|---|---|---|---|---|---|---|
| 100 | 3.6 s | 0 | 100 | 0 | 200, 117 ms | 694 ms | 1114 ms |
| 250 | 4.1 s | 0 | 250 | 0 | 200, 105 ms | 571 ms | 967 ms |
| 500 | 7.3 s | 0 | 500 | 0 | 200, 365 ms | 731 ms | 1143 ms |
| 1000 | 14.8 s | 0 | 1000 | 0 | 200, 81 ms | 501 ms | 1039 ms |
| 2000 | 30.1 s | 0 | 2000 | 0 | 200, 106 ms | 534 ms | 1417 ms |

- **Stop condition:** none was met (no drops, health always 200, refresh always under 10 s). All 2000 sessions were closed at the end.
- **Tail for this version:**
  - no exceptions, no `exceeded` lines (memory or CPU), no WARN/ERROR, no events marked truncated;
  - one boot, at 1794459555153, which was the boot before the stages started;
  - the highest Durable Object event `cpuTime` recorded was 152 ms.
- **Coverage caveat:** the tail recorded 621 Worker events but only 17 Durable Object events for this version, while 2000+ session requests were made. **The tail did not record most of the sessions' Durable Object events**, so a restart or limit message inside an unrecorded event would not have been seen. The sessions themselves stayed open, and the object kept answering.
- **pg_stat_activity:** before, 1 active, 8 idle; after closing, 1 active, 7 idle.

## 6. Not measured, and what did not go to plan

- **The 256 KB per-request log cap.** The first A1 run (node-11, version `1c285d8c`) lost most of its statements: its session's event was truncated after 26 s. I rewrote the method to use the log sink (above) and reran A1 on node-12. The first run's client numbers are in `A1-firstrun-truncated.log` (not committed) and are not used. Two short probes on node-11, P0 (5 sessions, no sink) and P1 (with the sink), established how logs are attributed.
- **The C node's boot line.** After deploying `ede99d51` (node-18), the boot wait saw no `storage sweeper started` line within 300 s. Meanwhile `/health` returned 200 sixty times, and the object's background loop logged under that version. The orchestration stopped there, and I restarted C on the same node without waiting for a boot line (`C-run.log`). Why the boot line was not recorded was not determined.
- **Object location.** Where each Durable Object ran is not exposed, so the A2/A4 split is reported, not explained.
- **Worker-side connections.** The number of the Worker's own outbound connections (relevant to A4's PG_POOL_MAX=10 against the 6-connection limit) is not visible; `pg_stat_activity` shows Hyperdrive's origin connections.
- **Statement attribution under concurrency** is by SQL class and time window (section 2), not by connection. Remove-query counts are upper bounds.
- **Clock offset:** the offset between this machine and Cloudflare is unknown. Attribution windows allow −100/+150 ms.
- **Neon console figures:** not recorded in C, as in round 3.
- **D coverage:** the tail recorded few Durable Object events (section 5).

## Teardown

TEARDOWN_SECTION
