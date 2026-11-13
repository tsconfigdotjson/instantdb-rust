# instant-worker on Cloudflare, round 5: confirming two fixes

Date: 2026-11-12, 19:08–19:58 UTC. Branch head `549cb17c55519333046ea5b5e397fbe8a1e31446`, which includes:
- `c48c484` and `549cb17`: a refresh that fails with an internal error is retried;
- `549cb17`: an event of a deleted webhook ends as failed.

**Rules followed:**
- no Rust source changes;
- connection string, tokens and client IPs redacted (one exception for two test tokens, see section 6);
- every command and its exit code is in the per-step logs under `deploy-test-logs/round5/`, which are not committed.

## 1. Summary

| section | headline |
|---|---|
| A. Dropped database connections | **The refresh fix holds.** Of the transacts sent after the first kill, **A1 123/123, A2 124/124 and A3 178/178 reached all 5 sessions within 10 s** (round 4 B2: 0 of 99). No refresh gave up (`refresh query failed` at info: 0), no unsolicited error frames, no drops, no reboots. Failed operations: 1, 1 and 3, all `Connection to the origin database was lost.` A3's 12 kills took 93 s, not 60 s. A **supplementary run (A4)** with debug logging for the invalidator repeated A3: **4 refresh queries failed and were retried**, none gave up, and **179/179** transacts reached all 5 sessions. |
| B. Events of a deleted webhook | **B1: exit 0, `WEBHOOKS TEST PASSED`, 38 checks**, including `ok   an event of a deleted webhook ends as failed`. **B2: 0 `Record not found: webhook` lines** in the 10-minute idle window. The tail recorded no events at all in that window, though. The same line also appears 0 times in the whole round, across about 14 minutes of load. In the database, the event of a deleted webhook left over from round 3 was marked `failed` ("Webhook was deleted.") 0.4 s after the node first booted. No event of a deleted webhook is still pending or processing. |
| C. Regression check | **All six suites passed, exit 0:** smoke-ws, sse, synctable, streams, conformance (`222 assertions`) and local-bus. |

## Setup

- **S1.** `git rev-parse HEAD` → `549cb17c55519333046ea5b5e397fbe8a1e31446`.
- **S2, database:** the same Neon Postgres (AWS us-east-2) as rounds 1–4. No migration files changed since round 4, so the migrations were already applied.
- **S2, Hyperdrive:** `npx wrangler hyperdrive create instant --connection-string=<REDACTED> --caching-disabled` → `✅ Created new Hyperdrive PostgreSQL config: abe731625aa24a20a467dcf698410b2a`.
- **S2, vars:**
  - `BASE_URL = "https://instant-worker.leerosen686.workers.dev"`;
  - `INSTANT_HANDLE_RECEIVE_TIMEOUT_MS = "30000"` and `PG_POOL_MAX = "5"`, both as committed;
  - `NODE_NAME = "node-20"` (new), `NODE_LOCATION_HINT = "enam"`;
  - `RUST_LOG = "info,sqlx=warn"`, `INSTANT_TRACE_REFRESH = "1"`.
- **S2, deploy:**
  - The first `npx wrangler deploy` failed in the custom build with `/bin/sh: cargo: command not found` (exit 127), because `~/.cargo/bin` was not on this shell's PATH.
  - The retry deployed version **`05c51f00`**, `Worker Startup Time: 116 ms`. Its boot line (`storage sweeper started`) was at 1794511474624.
- **Tail:** `npx wrangler tail --format json` ran to a file outside the repo, in a restart loop that records every exit.
  - The loop started before the Worker existed. Its first 143 starts exited 1 with `This Worker does not exist on your account. [code: 10007]`.
  - The 144th start (1794511450705), right after the deploy, connected and **never exited** until teardown.
  - **0 of 517 events were marked truncated.**
  - `tail-sanitized.jsonl` is the committed copy (2.5 MB): no IPs, geolocation, URLs or emails; UUIDs other than the Worker version ids, tokens and tunnel hosts replaced.
- **cloudflared (for B):** release `2026.10.7`, `cloudflared-darwin-arm64.tgz`, downloaded into the session scratchpad. The checks gave the same results as round 3:
  - the tarball matches GitHub's asset digest `sha256:3586ccf3…f831bd`;
  - the binary inside is `2136431d…d5bb12`;
  - the signature is `Developer ID Application: Cloudflare Inc. (68WVV388M8)`.

## 2. A: dropped database connections

**Workload** (`/tmp/iw-r5/load.mjs`):
- 5 websocket sessions on a fresh app, each subscribed to `{"todos":{}}`.
- One transact per second, sessions taking turns. Each transact sets the todo's title to a new value `w-<counter>-<epoch ms>` and logs it.
- One new-shape add-query per second (offset 0.5 s), then its remove-query.
- An idle log-sink websocket, replaced every 5 s, as in round 4.

**Kills.** The kill was the given query, run through `psql` (postgres:17 in Docker) against the database:

```
select pg_terminate_backend(pid) from pg_stat_activity
 where datname = current_database() and pid <> pg_backend_pid()
   and application_name not like 'psql%';
```

"Terminated" is the number of `t` rows it returned.

**Refresh delivery** was measured as follows:
- A transact reached a session when that session received a `refresh-ok` with `processed-tx-id` ≥ the transact's `tx-id`, after the transact was sent.
- A transact counts as delivered within 10 s when all 5 sessions got such a frame within 10 s of sending.
- Failed transacts have no `tx-id` and are listed separately.

A1, A2 and A3 ran on `node-20` (version `05c51f00`), each on a fresh app.

### Overview

| run | kills | terminated per kill | ops after first kill: ok / failed | **transacts after first kill reaching all 5 sessions ≤ 10 s** | not reaching | time to last session, median / max | unsolicited errors | drops | boots |
|---|---|---|---|---|---|---|---|---|---|
| A1 | 1 | 5 | 373 / 1 | **123 of 123** | 0 | 614 / 778 ms | 0 | 0 | 0 |
| A2 | 1 | 5 | 372 / 1 | **124 of 124** | 0 | 634 / 871 ms | 0 | 0 | 0 |
| A3 | 12 | 5, 4, 4, 3, 3, 3, 4, 4, 4, 4, 4, 4 | 534 / 3 | **178 of 178** | 0 | 642 / 1388 ms | 0 | 0 | 0 |
| A4 (supplementary, debug log) | 12 | 7, 4, 3, 3, 5, 4, 4, 3, 3, 4, 4, 3 | 533 / 3 | **179 of 179** | 0 | 670 / 1924 ms | 0 | 0 | 0 |

Before the first kill, every run delivered 61 of 61 transacts to all 5 sessions within 10 s. The time to the last session was median 620–799 ms, max 1029–1281 ms. **No transact in A1–A4 failed to reach a session within 10 s**, so there is none to list.

### A1 (first kill at 1794511555182, 19:25:55.182Z)

- **Kill:** 5 terminated. psql returned after 2638 ms.
- **pg_stat_activity** (`count|state`):
  - before the kill: `1|idle in transaction`, `3|idle`;
  - right after: `1|idle in transaction`, `3|idle`;
  - at the end: `5|idle`.
- **Operations:**

  | kind | before the kill: ok / failed | after: ok / failed |
  |---|---|---|
  | transact | 61 / 0 | 123 / 1 |
  | add-query | 60 / 0 | 125 / 0 |
  | remove-query | 60 / 0 | 125 / 0 |

- **Failed operations:**
  - 1794511556887 (+1705 ms after the kill), transact, session 2: `{"op":"error","status":500,"type":"internal-error","message":"database error: error returned from database: Connection to the origin database was lost.","hint":null}`
- **Unsolicited error frames:** none. **Drops:** none. **Boots:** none.
- **Tail, refresh trace:**

  | | `refresh query failed` | `refresh send` (`recomputed=`) | `refresh session` (`stale=`) |
  |---|---|---|---|
  | before the kill | 0 | 300 (1: 300) | 315 (0: 15, 1: 300) |
  | after | 0 | 620 (1: 620) | 620 (1: 620) |

- **WARN/ERROR:** `1 × WARN sqlx_core::pool::connection: error occurred while testing the connection on-release error=error communicating with database: expected to read 5 bytes, got 0 bytes at EOF`

### A2 (same node, fresh app; kill at 1794511778241, 19:29:38.241Z)

- **Kill:** 5 terminated. psql returned after 544 ms.
- **pg_stat_activity:**
  - before the kill: `5|idle`;
  - right after: `1|idle in transaction`;
  - at the end: `5|idle`.
- **Operations:**

  | kind | before the kill: ok / failed | after: ok / failed |
  |---|---|---|
  | transact | 61 / 0 | 124 / 0 |
  | add-query | 60 / 0 | 124 / 1 |
  | remove-query | 60 / 0 | 124 / 0 |

- **Failed operations:**
  - 1794511778678 (+437 ms after the kill), add-query, session 1: `{"op":"error","status":500,"type":"internal-error","message":"database error: error returned from database: Connection to the origin database was lost.","hint":null}`
- **Unsolicited error frames:** none. **Drops:** none. **Boots:** none.
- **Tail, refresh trace:**

  | | `refresh query failed` | `refresh send` (`recomputed=`) | `refresh session` (`stale=`) |
  |---|---|---|---|
  | before the kill | 0 | 300 (1: 300) | 305 (0: 5, 1: 300) |
  | after | 0 | 625 (1: 625) | 625 (1: 625) |

- **WARN/ERROR:** `2 × WARN sqlx_core::pool::connection: error occurred while testing the connection on-release error=error communicating with database: expected to read 5 bytes, got 0 bytes at EOF`

### A3 (same node, fresh app; 12 kills from 1794511990309, 19:33:10.309Z)

- **Kills:** each was scheduled 5 s after the previous one, or later if the previous psql had not yet returned.

  | kill | sent (ms after kill 1) | psql returned after | terminated |
  |---|---|---|---|
  | 1 | 0 | 486 ms | 5 |
  | 2 | 4976 | 418 ms | 4 |
  | 3 | 9969 | 391 ms | 4 |
  | 4 | 14944 | 428 ms | 3 |
  | 5 | 19945 | 585 ms | 3 |
  | 6 | 24950 | 1054 ms | 3 |
  | 7 | 29985 | 999 ms | 4 |
  | 8 | 34980 | 780 ms | 4 |
  | 9 | 39958 | **15755 ms** | 4 |
  | 10 | 55787 | **11667 ms** | 4 |
  | 11 | 67537 | **25054 ms** | 4 |
  | 12 | 92671 | 582 ms | 4 |

  **The 12 kills spanned 93 s instead of 60 s**, because kills 9–11 took 11.7–25.1 s to return. The last transact was sent **86 s after the last kill returned**, not 120 s; the load duration was fixed in advance.
- **pg_stat_activity:**
  - before the first kill: `5|idle`;
  - after the last: `1|idle`;
  - at the end: `5|idle`.
- **Operations:**

  | kind | before the kills: ok / failed | after the first: ok / failed |
  |---|---|---|
  | transact | 61 / 0 | 178 / 1 |
  | add-query | 60 / 0 | 178 / 2 |
  | remove-query | 60 / 0 | 178 / 0 |

- **Failed operations**, all with the same text, `{"op":"error","status":500,"type":"internal-error","message":"database error: error returned from database: Connection to the origin database was lost.","hint":null}`:
  - 1794512054707 (+64398 ms from the first kill), add-query, session 0;
  - 1794512076706 (+86397 ms), add-query, session 2;
  - 1794512083206 (+92897 ms), transact, session 3.
- **Unsolicited error frames:** none. **Drops:** none. **Boots:** none.
- **Tail, refresh trace:**

  | | `refresh query failed` | `refresh send` (`recomputed=`) | `refresh session` (`stale=`) |
  |---|---|---|---|
  | before the first kill | 0 | 300 (1: 300) | 305 (0: 5, 1: 300) |
  | after | 0 | 895 (1: 895) | 905 (0: 10, 1: 895) |

- **WARN/ERROR:** `6 × WARN sqlx_core::pool::connection: error occurred while testing the connection on-release error=error communicating with database: expected to read 5 bytes, got 0 bytes at EOF`

### A4, supplementary: A3 again with the retries visible

**Why it ran.** The retry added by the fix logs `refresh query failed, retrying: …` at **debug** level. Under the specified `RUST_LOG = "info,sqlx=warn"`, a retried refresh therefore leaves no line. The info-level `refresh query failed` trace line is written only after the retries run out. So A1–A3 show that no refresh gave up, but not whether any refresh failed in the first place.

**Setup.** A4 was a redeploy with `RUST_LOG = "info,sqlx=warn,instant_server::invalidator=debug"` and `NODE_NAME = "node-21"`.
- The redeploy gave version **`257df472`**; its boot was at 1794512954589.
- The load started 30 s after the boot, on a fresh app. The schedule was A3's: 12 kills every 5 s.
- This run is outside the brief. It changed only the log level, through a config var.

**Kills.** Terminated per kill: 7, 4, 3, 3, 5, 4, 4, 3, 3, 4, 4, 3. psql returned in 0.4–0.9 s each time, so the kills spanned 55 s, and the last transact was sent 123 s after the last kill returned.

**Operations after the first kill:**

| kind | ok / failed |
|---|---|
| transact | 179 / 0 |
| add-query | 177 / 3 |
| remove-query | 177 / 0 |

**Failed operations:** 3, at +10329, +25331 and +50330 ms, all add-query on session 1, all `{"op":"error","status":500,"type":"internal-error","message":"database error: error returned from database: Connection to the origin database was lost.","hint":null}`.

**Retries.** 4 × `DEBUG refresh_batch{app_id=<uuid> txs=1}: instant_server::invalidator: refresh query failed, retrying: database error: error returned from database: Connection to the origin database was lost.`

| retry logged at | after kill |
|---|---|
| 1794513064127 | 590 ms after kill 2 |
| 1794513079345 | 790 ms after kill 5 |
| 1794513089118 | 581 ms after kill 7 |
| 1794513114150 | 595 ms after kill 12 |

- **No refresh gave up:** 0 info-level `refresh query failed` trace lines, and 0 debug `refresh query failed: …` lines.
- `refresh: failed to load rules, retrying` was logged 0 times.

**Refresh delivery:** 179 of 179 transacts after the first kill reached all 5 sessions within 10 s. Time to the last session: median 670 ms, max 1924 ms.

**Tail, refresh trace:**

| | `refresh send` (`recomputed=`) | `refresh session` (`stale=`) |
|---|---|---|
| before the first kill | 300 (1: 300) | 310 (0: 10, 1: 300) |
| after | 895 (1: 895) | 955 (0: 55, 1: 900) |

**WARN:** `10 × WARN sqlx_core::pool::connection: … expected to read 5 bytes, got 0 bytes at EOF`.

**Unsolicited error frames, drops, boots during the run:** none.

## 3. B: events of a deleted webhook

### B1. webhooks-test, no `INSTANT_WEBHOOK_ALLOW_INSECURE`

**Setup:**
- **Tunnel:** `cloudflared tunnel --no-autoupdate --url http://127.0.0.1:9911`. The tunnel was up 5 s after start, and its host resolved via 1.1.1.1 2 s later.
- **Script copy:** `/tmp/iw-r5/webhooks-test.mjs`, with these lines changed:
  - `const receiverUrl = …` → `"https://<tunnel-9911>/hook"`;
  - `const base = …`: the default URL replaced by the workers.dev URL, and the same in the header comment.
- **App:** a fresh one, on `node-20` (`05c51f00`).

**Result:** exit 0, 17.3 s, 38 `ok` lines. The last 20 lines:

```
ok   update record has before/after
ok   link / unlink steps deliver no event
ok   delete record
ok   events listed
ok   event shape
ok   event by isn
ok   unknown event
ok   resend requeues
ok   resend delivered again
ok   410 disables the webhook
ok   410 event is failed
ok   enable
ok   disable with reason
ok   update url
ok   delete
ok   gone after delete
ok   events outlive the deleted webhook
ok   resend on a deleted webhook still requeues
ok   an event of a deleted webhook ends as failed
WEBHOOKS TEST PASSED
```

### B2. 10 minutes idle

- **Window:** the deployment was left idle from the end of B1, 1794512234469 (19:37:14.469Z), to 1794512834666 (19:47:14.666Z). The tunnel stayed up; nothing else ran.
- **`Record not found: webhook` lines in the window: 0.** Timestamps: none.
- **Coverage caveat.** The tail recorded **0 events** of any kind in the idle window, so it recorded no logs at all there. The next request, the first C suite at 1794512881608, was followed by a **new boot of `05c51f00`** at 1794512882744 (19:48:02.744Z). The idle window alone therefore does not show whether the line would have been logged.
- **Same line over the whole round: 0.** Across both versions (`05c51f00` and `257df472`) the tail has 0 `Record not found: webhook` lines, and no other WARN/ERROR line mentions webhooks. That covers 10 509 log lines, including A1–A4 (about 14 minutes of load), B1 and C. In round 4 this line appeared about every two minutes under load (91 in the 3-hour soak).
- **Database check** (read-only `select` on `webhook_events` joined to `webhooks`, in `B3-db-events.log`). Events whose webhook no longer exists:

  | created | status | attempts | last attempt |
  |---|---|---|---|
  | 2026-11-12 02:16:18Z | success | 1 | 200 |
  | 2026-11-12 02:16:20Z | success | 1 | 200 |
  | **2026-11-12 02:16:23Z** (round 3) | **failed**, updated **19:24:35Z** | 3 | `disabled`, `"Webhook was deleted."`, at 19:24:35.025Z |
  | 2026-11-12 02:16:25Z | failed | 1 | 410 `gone` |
  | 2026-11-12 19:37:00Z | success | 1 | 200 |
  | 2026-11-12 19:37:02Z | success | 1 | 200 |
  | 2026-11-12 19:37:07Z (B1) | failed, updated 19:37:14Z | 3 | `disabled`, `"Webhook was deleted."` |
  | 2026-11-12 19:37:09Z | failed | 1 | 410 `gone` |

  - The round 3 event was marked failed 0.4 s after `node-20`'s first boot (19:24:34.624Z).
  - **No event of a deleted webhook is pending or processing.**
  - Events of existing webhooks: 2 success, 1 failed.

## 4. C: regression check

Each suite ran on a fresh app on `node-20` (`05c51f00`), from `/tmp/iw-r5/` copies, with these changes:
- the `localhost:8888` URLs replaced by the workers.dev URL (`wss://` / `https://`);
- in `streams-test`, `psql postgres://instant:instant@localhost:5432/instant` replaced by `psql "$DATABASE_URL"`, so the copy holds no connection string.

`local-bus-test` creates its own app.

| suite | exit | time | last line |
|---|---|---|---|
| smoke-ws | 0 | 5.5 s | `SMOKE TEST PASSED` |
| sse-test | 0 | 3.5 s | `SSE TEST PASSED` |
| synctable-test | 0 | 5.8 s | `SYNC TABLE TEST PASSED` |
| streams-test | 0 | 5.8 s | `STREAMS TEST PASSED` |
| conformance-test | 0 | 21.4 s | `CONFORMANCE SUITE PASSED (222 assertions)` |
| local-bus-test | 0 | 6.6 s | `local bus ok` |

**Tail during C:**
- The node booted at 1794512882744, 1.1 s after smoke-ws started; this is the boot after B2's idle.
- One warning, during local-bus-test, at 1794512932156:
  ```
  WARN sqlx::query: slow statement: execution time exceeded alert threshold summary="SELECT u.id, u.email, u.created_at, …" … elapsed=1.66s elapsed_secs=1.6600000000000001 slow_threshold=1s
  ```
  The statement reads `instant_refresh_tokens` joined to `instant_users`.
- No exceptions and no other WARN/ERROR.

## 5. Whole-round tail

| version | log lines | boots | WARN/ERROR | exceptions | truncated events |
|---|---|---|---|---|---|
| `05c51f00` (node-20) | 7542 | 2: 1794511474624, 1794512882744 | 9 × the `on-release … got 0 bytes at EOF` warning (A1–A3); 1 × `slow statement` (C) | 0 | 0 |
| `257df472` (node-21, A4) | 2967 | 1: 1794512954589 | 10 × the `on-release` warning; plus the 4 DEBUG retry lines (A4) | 0 | 0 |

## 6. Not measured, and what did not go to plan

- **Retries under the specified log level.** Under `info,sqlx=warn`, a refresh that failed and then succeeded on retry leaves no trace. A1–A3 show the outcome (every refresh delivered, none gave up), but not how many retries happened. A4 measured the retries with one extra config var; that run is outside the brief.
- **A3's schedule.** The kills spanned 93 s, not 60 s, and the last transact was sent 86 s after the last kill, not 120 s later (see the A3 kill table).
- **B2's coverage.** The tail recorded no events during the idle window, and the node booted again at the next request. The idle count of 0 is therefore backed up only by the whole-round count and the database check above.
- **Admin tokens in a local log.** My C runner wrote the full command lines into `C-suites.log`, and two of them carried the admin tokens of the two fresh apps (synctable, conformance). One tool output in my session also showed them. The log was not committed. I redacted the file and **revoked both tokens** (`DELETE FROM app_admin_tokens` for those two tokens: 2 rows).
- **Provider figures** (Neon, Hyperdrive dashboards): not recorded.
- **Where the Durable Objects ran:** not exposed.
- **Clock offset** between this machine and Cloudflare: not measured.
- **What a refresh carried.** Refresh delivery is judged by `processed-tx-id`. Whether each `refresh-ok` held the written value was logged but not analysed.

## Teardown

Done after the report was pushed (`3375749`), 19:57:37Z–19:58:04Z. Every command and its exit code is in `T-teardown.log` (not committed).

**Removed:**
- **Processes:**
  - touched the tail loop's stop file;
  - `pkill -f 'wrangler tail --format json'`: exit 0 (the loop exited 144);
  - `pkill -f 'cloudflared tunnel'`: exit 0;
  - `pkill -f 'iw-r5/load.mjs'`: exit 1, nothing was running.

  `pgrep -fl 'wrangler tail|load.mjs|cloudflared|runA.sh|runB.sh|runC.sh'` then returned nothing (exit 1).
- **Worker:** `npx wrangler delete --force` → `Successfully deleted instant-worker` (exit 0). This removes the Worker, its Durable Objects (`node-20`, `node-21`) and its secrets.
- **Hyperdrive:** `npx wrangler hyperdrive delete abe731625aa24a20a467dcf698410b2a` → `✅ Deleted` (exit 0).
- **Verification:**
  - `npx wrangler hyperdrive list` shows only the header `📋 Listing Hyperdrive configs`, with no configs.
  - `curl $URL/health` → `error code: 1042`, HTTP 404.
- **Local files:**
  - `git checkout -- crates/instant-worker/wrangler.toml`; `git status` shows it clean.
  - Deleted from the session scratchpad:
    - the raw tail (6.0 MB);
    - the raw per-operation logs and kill records;
    - the analysis scripts and helper library;
    - the psql shim;
    - the `cloudflared` binary, its tarball and the tunnel log;
    - the uncompressed sanitized tail.
  - Deleted `/tmp/iw-r5` (load script and suite copies, including the webhooks-test copy with the tunnel URL).
  - The scratchpad is empty and `/tmp/iw-r5` does not exist.
- **Secret scan:**
  - The database host and password were read from `~/.iw-database-url` into a temporary pattern file, never printed; the file was deleted afterwards. The scan also searched for `npg_[A-Za-z0-9]{4}` and the two revoked admin tokens.
  - Working tree, gitignored files included (`/usr/bin/grep -rIl`, excluding `.git`, `target`, `build`, `node_modules`): **0 files**.
  - `target/` and `build/`: **0 files**.
  - `git grep` on HEAD: **0 files**.
  - `git log --all -S`:
    - **host:** `19238da` and `e61700a`, the round 1 commits reported in round 4. This round added nothing.
    - **password:** none.

**Still exists:**
- **The Neon database**, with this round's 11 apps (10 from `create-app.sh`, 1 created by `local-bus-test`). The two revoked admin tokens no longer exist. The user deletes the database.
- **`~/.iw-database-url`**, which holds the connection string.
- **The database host name** in the branch history (`e61700a`), as in round 4.
- **Other local state:**
  - the wrangler OAuth login;
  - the `postgres:17` Docker image;
  - the Rust toolchains;
  - `~/Library/Caches/worker-build`;
  - `crates/instant-worker/target` and `build`.
- **Uncommitted per-step logs** in `deploy-test-logs/round5/` (gitignored). They contain no connection string or token, per the scan above.
