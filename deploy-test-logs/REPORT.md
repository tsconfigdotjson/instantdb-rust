# instant-worker: first Cloudflare deployment test

Date: 2026-11-11 (times are UTC unless marked local; local is EST, UTC-5).
Branch `claude/beautiful-brahmagupta-t7bogg` at `0a37e7a` (PR #105), plus uncommitted local edits listed below.
The database connection string and all tokens are redacted here and in every log in this directory.

## 1. Summary

- **As committed, the Worker does not run on Cloudflare.** It deploys cleanly, but every request returns `500 INTERNAL SERVER ERROR`. The cause: bootstrap fails with `Protocol Error: Unexpected protocol code: C`, because Hyperdrive rejects the Postgres `Close` message that sqlx sends.
- **`wrangler dev` hid the problem.** Under `wrangler dev` everything passed, but the local Hyperdrive binding connects straight to Postgres and so never exercises Hyperdrive's pooler.
- **After fixes, every protocol script passes on Cloudflare.** The test was then changed from "deploy and report" to "make it work" (Rust source edits allowed). The working deploy is version `1e82065f`; results are in section 7. The fixes:
  - four patches to sqlx-postgres 0.8.6, vendored into the worker crate only;
  - Hyperdrive query caching turned off;
  - the session handler timeout raised to 30 s;
  - console tracing, so the server's logs reach `wrangler tail`.
- **Hyperdrive/sqlx incompatibilities found**, all fixed in `crates/instant-worker/vendor/sqlx-postgres`:
  1. Hyperdrive rejects frontend `Close` ('C') messages. sqlx sends one after every extended-protocol query.
  2. Hyperdrive's own ErrorResponse has a quoted severity (`"FATAL"`), which sqlx cannot parse. This hides the real error.
  3. Hyperdrive confuses named prepared statements across the backends it multiplexes. sqlx names them `sqlx_s_1`, `sqlx_s_2`, … on every connection, so two connections reuse the same names. Errors seen:
     - `bind message supplies 1 parameters, but prepared statement "sqlx_s_5" requires 0`
     - `expected ParseComplete, got ParameterDescription`
  4. sqlx's on-release `ping()` writes a bare `Sync` behind the `ROLLBACK` that a dropped transaction queues. Hyperdrive never answers it, so each failed transaction leaked one pooled connection. After 5 (`PG_POOL_MAX`) the node stalled with `pool timed out while waiting for an open connection`.
- **Hyperdrive config change.** Query caching (on by default) returned stale reads straight after writes, so I disabled it on the config.
- **Latency is high.** Median add-query round trip is about 364–383 ms from Toronto, with Postgres on Neon in AWS us-east-2.
- **Not done:**
  - Capacity test (skipped at the user's request).
  - Test scripts that need localhost mock servers (skipped per the instructions): oauth, webhooks, dash-login, security.

## 2. Environment

Log: `01-environment.log`.

| Item | Value |
|---|---|
| Machine | Apple M1 MacBook Air, macOS 26.6.2 (25G83), arm64 |
| Disk free at start | 18 GiB (92% used) |
| Node | v24.22.1 |
| wrangler | 4.160.1 (via `npx`, installed on first use) |
| Rust | rustup 1.29.2. `~/.cargo/bin` was not on `PATH` (`rustup: command not found`), so it was added per command. The crate's `rust-toolchain.toml` auto-installed `beta-aarch64-apple-darwin`; it is now `rustc 1.101.0-beta.1 (bbcdfd90a 2026-11-10)` |
| Emscripten | emsdk 6.0.10, downloaded by worker-build on first build into `~/Library/Caches/worker-build` |
| worker-build | `cargo install --git workers-rs --rev f6c8d1da…` (from `wrangler.toml` `[build]`) |
| psql | Not installed. Used a shim that runs `docker run --rm -i postgres:17 psql` (psql 17.11, Docker 29.9.3) |
| Cloudflare | `wrangler whoami`: OAuth login, `workers (write)` and `workers_tail (read)` scopes. Account name, id and email are not reproduced here |
| Worker URL | `https://instant-worker.leerosen686.workers.dev` |
| Client edge | `colo=YYZ` (Toronto), from `/cdn-cgi/trace` |

**Local config edits** (uncommitted, not pushed). In `crates/instant-worker/wrangler.toml`:
- `[[hyperdrive]] id = "373fc0afe8a7431b88970c9f530f6435"`
- `localConnectionString = "<REDACTED>"`
- `BASE_URL = "https://instant-worker.leerosen686.workers.dev"`. The first deploy still had `http://localhost:8787`.
- New vars: `INSTANT_HANDLE_RECEIVE_TIMEOUT_MS = "30000"`, `NODE_NAME = "node-6"`, `RUST_LOG = "info,sqlx=info"`

Code changes are listed in section 11 (*The fixes, as code*).

## 3. Database

Log: `02-database.log`.

- **Database:** a throwaway Neon project, PostgreSQL 18.6 (aarch64), host region `us-east-2` (AWS). The user created it for this test.
- **Starting state:** empty (0 tables).
- **Migrations:** `scripts/apply-migrations.sh` applied all 126, exit 0, in 277 s. The post-check counted 126 applied migrations and 110 tables.
- **Test apps:** created with `scripts/create-app.sh`, a fresh app per script per run (`08-create-apps.log` and the `create-app.sh` lines in each `08-protocol*.log`). Admin tokens are redacted.

## 4. Build

Log: `03-wrangler-dev.log`. The build started at 12:57:22 local (`03-build-start.txt`).

- **First build:** `worker-build --emscripten --release` via `npx wrangler dev`.
  - Installed the beta toolchain and emsdk 6.0.10.
  - `Finished release profile [optimized] target(s) in 13m 27s`, producing `index.js 88.6kb`.
  - Warnings: unused dependencies inside worker-build itself, plus one linker warning (`emcc: MODULARIZE=instance is still experimental`, `-sSOURCE_PHASE_IMPORTS is experimental`, `-sWASM_BINDGEN is experimental`).
- **`wrangler dev`:** `Ready on http://localhost:8787`. `/health` returned `200 ok`: 5.79 s on the first call (boot), then 0.023 s (`03-health-local.log`).
- **Rebuilds:**
  - A change to `main.rs` or `wrangler.toml` took 5m 36s – 6m 08s.
  - Rebuilds that recompiled the vendored sqlx-postgres took 8m 36s – 20m 19s. The last one ran while other work was going on.
  - With no change, the build took about 0.3 s.
- The vendored sqlx-postgres compiles with 10 warnings: dead code left over from the removed `Close` and `Flush` messages, and one upstream deprecation.

## 5. Deploy

Every deploy was `npx wrangler deploy` from `crates/instant-worker` and exited 0. Full outputs are in `05-deploy-*.log`.

| # | Version | Change | Build | Upload / gzip | Startup |
|---|---|---|---|---|---|
| 1 | 700218ff | as committed (BASE_URL still localhost) | 0.38 s (cached) | 12337.50 / 3883.45 KiB | 68 ms |
| 2 | 5d1f4d65 | BASE_URL = workers.dev URL | 0.29 s | 12337.50 / 3883.45 KiB | 73 ms |
| 3 | cfd8a6c2 | vendored sqlx-postgres without `Close` | 10m 55s | 12329.07 / 3880.33 KiB | 60 ms |
| 4 | 2485f4b5 | + accept quoted severity | 8m 36s | 12329.79 / 3881.85 KiB | 63 ms |
| 5 | 0f1a5cf4 | + console tracing, pool `acquire_timeout(10s)` | 6m 08s | 12470.58 / 3940.36 KiB | 103 ms |
| 6 | 6c6aefa5 | redeploy (tried to reset the Durable Object; it did not) | 0.31 s | same | 92 ms |
| 7 | cf14d2b7 | `RUST_LOG=info,sqlx=info` | 0.32 s | same | 80 ms |
| 8 | f9d82698 | `NODE_NAME` var (`node-2`), handler timeout 30 s | 5m 41s | 12472.17 / 3940.81 KiB | 86 ms |
| 9 | 2f04039d | statement names from a SQL hash (`node-3`). Worse; reverted | 5m 36s | 12471.57 / 3940.70 KiB | 106 ms |
| 10 | f90dcc23 | process-wide statement counter (`node-4`) | 5m 43s | 12473.76 / 3942.02 KiB | 70 ms |
| 11 | 5973171e | `node-5` (escape a node wedged by the pool leak) | 2.72 s | same | 116 ms |
| 12 | **1e82065f** | `ping()` without bare `Sync` (`node-6`). **Current** | 20m 19s | 12473.62 / 3941.81 KiB | 161 ms |

**Startup line, current deploy:** `Worker Startup Time: 161 ms`. Across all 12 deploys it ranged from 60 to 161 ms.

No Durable Object migration line was printed in any deploy output. The `v1` migration (`new_sqlite_classes = ["Node"]`) was applied silently on the first deploy.

### Deploy 1 output, verbatim

```
$ npx wrangler deploy   (first deploy, BASE_URL still http://localhost:8787)

 ⛅️ wrangler 4.160.1
────────────────────
[custom build] Running: cargo install -q --locked --git https://github.com/cloudflare/workers-rs --rev f6c8d1da189cdb91d8bd9576e19cd1dcfd917460 worker-build && worker-build --emscripten --release
[custom build] [INFO]: 🎯  Checking for the Wasm target...
[custom build] 
[custom build] [INFO]: 🎯  Checking the Emscripten toolchain...
[custom build] 
[custom build] [INFO]: 🌀  Compiling to Wasm (emscripten)...
[custom build] 
[custom build] warning: linker stderr: cache:INFO: generating system headers: sysroot_install.stamp... (this will be cached in "/Users/lrosen/Library/Caches/worker-build/emsdk-6.0.10/upstream/emscripten/cache/sysroot_install.stamp" for subsequent builds)
[custom build]          cache:INFO:  - ok
[custom build]          emcc: MODULARIZE=instance is still experimental. Many features may not work or will change. [-Wexperimental]
[custom build]          emcc: -sSOURCE_PHASE_IMPORTS is experimental and not yet supported in browsers [-Wexperimental]
[custom build]          emcc: -sWASM_BINDGEN is experimental and subject to change [-Wexperimental]
[custom build]   |
[custom build]   = note: `#[warn(linker_messages)]` on by default
[custom build] 
[custom build] 
[custom build] warning
[custom build] : `instant-worker` (bin "instant-worker") generated 1 warning
[custom build] 
[custom build]     Finished `release` profile [optimized] target(s) in 0.38s
[custom build] 
[custom build] [INFO]: ✨  Done in 0.46s
[custom build] [INFO]: 📦   Your wasm pkg is ready to publish at /Users/lrosen/Workspace/instantdb-rust/crates/instant-worker/build/.tmp.
[custom build] 
[custom build] 
[custom build]   index.js  88.6kb
[custom build] 
[custom build] ⚡ Done in 23ms
[custom build] 
Total Upload: 12337.50 KiB / gzip: 3883.45 KiB
Worker Startup Time: 68 ms
Your Worker has access to the following bindings:
Binding                                                 Resource                  
env.NODE (Node)                                         Durable Object            
env.HYPERDRIVE (373fc0afe8a7431b88970c9f530f6435)       Hyperdrive Config         
env.BASE_URL ("http://localhost:8787")                  Environment Variable      
env.PG_POOL_MAX ("5")                                   Environment Variable      
env.PG_POOL_MIN ("0")                                   Environment Variable      

Uploaded instant-worker (2.85 sec)
Deployed instant-worker triggers (0.78 sec)
  https://instant-worker.leerosen686.workers.dev
Current Version ID: 700218ff-af23-4846-ac8d-43c2a5efd910
[exit 0] [8s]
```

### Deploy 12 (current) output, tail verbatim

The full output, including the sqlx-postgres compile warnings, is in `05-deploy-12-ping.log`.

```
[custom build]     Finished `release` profile [optimized] target(s) in 20m 19s
[custom build] 
[custom build] [INFO]: ✨  Done in 20m 19s
[custom build] 
[custom build] [INFO]: 📦   Your wasm pkg is ready to publish at /Users/lrosen/Workspace/instantdb-rust/crates/instant-worker/build/.tmp.
[custom build] 
[custom build] 
[custom build]   index.js  88.6kb
[custom build] 
[custom build] ⚡ Done in 21ms
[custom build] 
Total Upload: 12473.62 KiB / gzip: 3941.81 KiB
Worker Startup Time: 161 ms
Your Worker has access to the following bindings:
Binding                                                                                 Resource                  
env.NODE (Node)                                                                         Durable Object            
env.HYPERDRIVE (373fc0afe8a7431b88970c9f530f6435)                                       Hyperdrive Config         
env.BASE_URL ("https://instant-worker.leerosen686.wo...")                               Environment Variable      
env.PG_POOL_MAX ("5")                                                                   Environment Variable      
env.PG_POOL_MIN ("0")                                                                   Environment Variable      
env.INSTANT_HANDLE_RECEIVE_TIMEOUT_MS ("30000")                                         Environment Variable      
env.NODE_NAME ("node-6")                                                                Environment Variable      
env.RUST_LOG ("info,sqlx=info")                                                         Environment Variable      

Uploaded instant-worker (4.83 sec)
Deployed instant-worker triggers (0.59 sec)
  https://instant-worker.leerosen686.workers.dev
Current Version ID: 1e82065f-39cc-4a77-8374-b05f39c676ad
[exit 0] [1248s]
```

### Hyperdrive

Logs: `04-hyperdrive.log`, `04b-hyperdrive-caching.log`.

- **Created:** `npx wrangler hyperdrive create instant --connection-string=<REDACTED>` returned config `373fc0afe8a7431b88970c9f530f6435` with `origin_connection_limit: 60` and `caching.disabled: false`.
- **Changed:** at 18:57 UTC, `npx wrangler hyperdrive update 373fc0afe8a7431b88970c9f530f6435 --caching-disabled` set `caching.disabled: true` (see section 11).

## 6. Health / cold start

Logs: `07-health.log`, `07b-health-after-patch.log`, `07c-health-final.log`. Times are curl `time_total` from Toronto.

| When | Version | Request | Status | Time |
|---|---|---|---|---|
| deploy 2, first call | 5d1f4d65 | `/health` | 500 `INTERNAL SERVER ERROR` | 1.10 s |
| deploy 2, calls 2–5 | 5d1f4d65 | `/health` | 500 (each retries boot and fails) | 0.78, 0.50, 0.56, 0.61 s |
| deploy 3, cold | cfd8a6c2 | `/health` | 200 `ok` | **8.44 s** |
| deploy 3, warm 2–5 | cfd8a6c2 | `/health` | 200 | 0.36, 0.13, 0.15, 0.21 s |
| final, 1–5 (23:47 UTC, warm) | 1e82065f | `/health` | 200 `ok` ×5 | 0.115, 0.098, 0.110, 0.074, 0.095 s |
| final | 1e82065f | `/` | 200 `instant-server` | 0.081 s |
| final | 1e82065f | `/no-such-route` | 404 `{"message":"Oops! We couldn't match this route."}` | 0.125 s |
| after 5 min idle | 1e82065f | `/health` | still running when this report was pushed; see `07c-health-final.log` | |
| after a further 10 min idle (15 min total) | 1e82065f | `/health` | still running when this report was pushed | |

- **Cold start:** about 7–9 s on every fresh Durable Object, i.e. after a deploy that changes `NODE_NAME` or after an eviction. That covers:
  - the pool connect over Hyperdrive;
  - the system catalog bootstrap, which runs about a dozen `CREATE … IF NOT EXISTS` statements;
  - loading `SERVER_SECRET` from Postgres;
  - starting the background loops.
- **Warm:** 0.1–0.4 s.
- **The Durable Object outlives deploys.** It stayed resident across redeploys: a redeploy without a `NODE_NAME` change kept the old in-memory node and its wedged pool (deploy 6).
- **It was evicted after about 2.5 hours idle.** The tail shows node `node-6` booting at 21:08:28 (deploy 12) and again at 23:45:57, which is the first request after about 2.5 hours without traffic.

## 7. Protocol tests

**Setup:**
- Scripts were copied to `/tmp/iw-scripts/` with `localhost:8888` replaced by the workers.dev URL (`wss://` / `https://`).
- `conformance-test.mjs` and `local-bus-test.mjs` ran from the repo with `API_URL` / `SERVER_URL` set.
- Each script got a fresh app.
- Each script had a 300 s alarm.

**Skipped per the instructions:** oauth, webhooks, dash-login, security. They need localhost mock servers.

### Final results, version 1e82065f (`08-protocol-final4.log`, `08-protocol-final5-streams.log`)

| Script | Exit | Time | Result |
|---|---|---|---|
| smoke-ws | 0 | 7 s | two peers: transact, refresh, presence, broadcast |
| sse-test | 0 | 3 s | pass |
| synctable-test | 0 | 7 s | pass |
| conformance-test | 0 | 51 s | `CONFORMANCE SUITE PASSED (222 assertions)` |
| local-bus-test | 0 | 8 s | 4/4 `ok`, `local bus ok` |
| streams-test (as instructed) | 1 | 1 s | fails in the script itself: it runs `psql postgres://instant:instant@localhost:5432/instant`, which does not exist here. Not a Worker result |
| streams-test against the Neon DB (copy with `$DATABASE_URL`) | 0 | 14 s | 13 `ok` checks, `STREAMS TEST PASSED` |

### History: how each run failed before the fixes

| Run (local time) | Version | smoke | sse | synctable | conformance | local-bus | streams (Neon) |
|---|---|---|---|---|---|---|---|
| `08-protocol.log` 13:33 | cfd8a6c2 | ✗ | ✗ | ✗ | – | – | – |
| `08-protocol-after-fix.log` 13:44 | 2485f4b5 | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| `08-protocol-final.log` 13:55 | 0f1a5cf4 | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| `08-protocol-nocache.log` 13:58 | 0f1a5cf4 | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| `08-protocol-fixed.log` 14:10 | f9d82698 | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ |
| `08-protocol-final2.log` 15:30 | 2f04039d | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |
| `08-protocol-final3.log` 15:41 | f90dcc23 | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ |
| `08-protocol-final4.log` 16:12 | **1e82065f** | ✓ | ✓ | ✓ | ✓ | ✓ | (cut off) |
| `08-protocol-final5-streams.log` 18:46 | **1e82065f** | | | | | | ✓ |

The scripts log only that an `error` frame or a timeout arrived; the error text was found by rerunning with copies that print whole frames (`/tmp/iw-smoke-debug.mjs`, `/tmp/iw-streams-debug.mjs`) and from the tail. In the order they were found, the causes were:
1. transact errors with `unknown severity: "\"FATAL\""`, which hid the real error;
2. empty reads straight after writes (`Cannot read properties of undefined (reading 'instaql-result')` in `08-protocol.log` and `08-protocol-final.log`): Hyperdrive caching;
3. session `init` timing out and `pool timed out` in the tail, left by handler timeouts and by a node still wedged from the previous run;
4. `bind message supplies 1 parameters, but prepared statement "sqlx_s_5" requires 0`: statement-name collisions;
5. with hash-based names, `expected ParseComplete, got ParameterDescription` (`invalidator: refresh: failed to load rules` in the tail at 15:28 local);
6. the conformance error matrix draining the pool, one connection per failed transaction.

`08-protocol-fixed-rerun.log` (15:21, f9d82698, after a network outage on the client side) had local-bus pass and conformance reach "sync tables" before failing.

The pool-leak fix was checked separately before the final run, with a probe (`/tmp/iw-probe.mjs`) that repeats each error case 8 times:
- init with an unknown app;
- init with a bad refresh token;
- a malformed transact;
- a transact that hits a unique violation;
- an update of a missing entity.

The pool stayed healthy throughout, and a fresh session `init` answered in 66–91 ms. Before the fix, the 6th erroring transaction exhausted the 5-connection pool.

## 8. Latency

Log: `09-latency.log`.

- **Test:** `node /tmp/iw-latency.mjs wss://instant-worker.leerosen686.workers.dev <smoke app>`. It opens one websocket, sends `init`, then sends 20 sequential `add-query` messages. Each query is distinct (`todos where title = latency-<i>-<ts>`), so none is answered from the session's query cache. Each round trip is measured to its `add-query-ok`.
- **Client:** Toronto, entering Cloudflare at `colo=YYZ`.
- **Database:** Neon, AWS `us-east-2` (Ohio).
- **Unknown:** where the Durable Object runs. Neither the response nor the tail events say.

| Run | init-ok | min | median | max |
|---|---|---|---|---|
| 1 | 236.5 ms | 363.5 ms | 382.5 ms | 637.1 ms (#1) |
| 2 | 107.6 ms | 347.9 ms | 364.3 ms | 614.1 ms (#1) |

- The first query of each session is about 250 ms slower than the rest.
- About 370 ms per new query is high. For comparison, warm `/health`, which does not touch Postgres, takes 0.13–0.36 s from the same client.
- An `add-query` runs several statements: rules, attrs, then the query itself. With caching off, each statement is a separate trip from the Durable Object through Hyperdrive to Neon in us-east-2.
- I did not measure where that time goes.

## 9. Capacity

Skipped at the user's request. A script was written (`/tmp/iw-capacity.mjs`) but not run.

## 10. Runtime errors from tail

**Sources:**
- `tail.log`: `wrangler tail --format pretty`, from 13:18 local.
- `tail-json.log` (`--format json`, from 18:58 UTC): not committed, because each event carries the client's IP address and geolocation. `tail-json-sanitized.jsonl` keeps timestamp, version, outcome, method, URL (app id redacted), logs and exceptions for all 316 events.

The pretty format did not show the Durable Object's `console.log` output, so the JSON tail was added. Per-version counts are in `12-tail-errors.txt`, produced by `/tmp/iw-errors.py`.

| Version | Errors and warnings (count) |
|---|---|
| 5d1f4d65 (deploy 2) | `GET /health - Exception Thrown` on all 5 requests. 4 of them: `Error: bootstrap: system catalog bootstrap failed: database error: error returned from database: Protocol Error: Unexpected protocol code: C`. 1 of them (18:18:42): `Error: bootstrap: system catalog bootstrap failed: database error: encountered unexpected or invalid data: Postgres protocol error: expected ParseComplete, got ParameterDescription` |
| cfd8a6c2 – 2485f4b5 (deploys 3–4) | none logged; the server had no log output until deploy 5 |
| 0f1a5cf4 (deploy 5) | background loops: `pool timed out while waiting for an open connection` (usage 3, indexing_jobs 3, webhooks 3) |
| f9d82698 (deploy 8) | `sqlx_core::pool::connection: error occurred while testing the connection on-release error=… Idle connection closed by Hyperdrive.` (1) |
| 2f04039d (deploy 9) | `invalidator: refresh: failed to load rules: … expected ParseComplete, got ParameterDescription` (1), then `pool timed out` in the background loops (6) |
| f90dcc23 (deploy 10) | `pool timed out` in background loops (12), after conformance's error matrix |
| 5973171e (deploy 11) | `pool timed out` in background loops (10), including hard_delete and storage_sweeper |
| **1e82065f (current)** | **No warnings, errors or exceptions.** Only INFO lines: the bootstrap's Postgres notices (`relation … already exists, skipping`) and `SERVER_SECRET not set; using a generated secret persisted in Postgres`, each twice (two boots) |

Other notes:
- **Event outcomes** on the current version: 77 `ok`, 24 `responseStreamDisconnected`, 18 `canceled`. The last two are websocket sessions that the client closed.
- **The tails dropped several times:**
  - `tail.log`: `✘ [ERROR] Unable to reconnect to the tail for instant-worker after 5 attempts`
  - `tail-json.log`: `ECONNRESET`
  - Both were restarted in append mode. Events during the gaps, including part of the pool-leak probe, are missing.

## 11. Surprises

1. **`wrangler dev` does not test Hyperdrive.** Locally, the binding's `localConnectionString` connects straight to Postgres. Everything passed under `wrangler dev`, but nothing worked through the real Hyperdrive pooler.
2. **sqlx 0.8 is not compatible with Hyperdrive as shipped.** Four separate protocol problems, listed in section 1. They are fixed only in the worker's vendored sqlx-postgres; the native server still uses crates.io sqlx.
3. **Hyperdrive caching is on by default and breaks read-after-write.** After a transact, the next query read cached, pre-write results. Instant needs caching off.
4. **Durable Objects survive deploys.** Timers and websockets keep the old instance resident with its old in-memory state, including a wedged pool. A fresh node needed a new object name, so `NODE_NAME` was added. Separately, the object was evicted after about 2.5 h idle.
5. **The 5 s handler timeout is too short over Hyperdrive.** Every statement is an extra network hop, and multi-statement transactions were cancelled midway. Raised to 30 s with `INSTANT_HANDLE_RECEIVE_TIMEOUT_MS`.
6. **No server logs at first.** The worker installed no `tracing` subscriber, so the server's warnings went nowhere. The pretty `wrangler tail` did not show the Durable Object's console output either; the JSON tail does.
7. **A 500 hides the cause.** The HTTP body is only `INTERNAL SERVER ERROR`, and the reason is visible only in the tail. Every request retries boot (`OnceCell::get_or_try_init`), so a broken deploy fails on each request rather than once.
8. **A pool timeout wedges the whole node.** After the pool leaked dry, every session and background loop failed with `pool timed out` until the object was replaced.
9. **Builds are slow.**
   - First build: 13m 27s, including the toolchain and emsdk downloads.
   - Rebuilds touching sqlx: 8–20 min.
   - The Rust beta toolchain is auto-installed by `rust-toolchain.toml`.
10. **`streams-test.mjs` hardcodes a local database URL** (`postgres://instant:instant@localhost:5432/instant`), so it cannot pass against any deployed server as written.
11. **`create-app.sh` prints the admin token to stdout,** so it lands in any log of the run. The tokens were redacted from these logs afterwards.
12. **Rust tooling was not on `PATH`** in the non-interactive shell, and psql was missing. I used a Docker-based psql shim.

### The fixes, as code

- **`crates/instant-worker/vendor/sqlx-postgres`:** a copy of sqlx-postgres 0.8.6, wired in through the worker crate's own `[patch.crates-io]`. The patch against crates.io is in `sqlx-postgres-patch.diff`, 173 lines over four files:
  - `executor.rs`: no `Close(Portal)` after execute; no `Close(Statement)` on cache eviction; persistent statements are named from a process-wide counter seeded from the clock.
  - `io/mod.rs`: `StatementId::from_number`.
  - `connection/mod.rs`: `clear_cached_statements` forgets statements locally instead of closing them; `ping()` only flushes and drains, with no bare `Sync`.
  - `message/response.rs`: the severity parser trims surrounding quotes.
- **`crates/instant-worker/src/main.rs`** and **`Cargo.toml`** (`worker-src.diff`):
  - `NODE_NAME` picks the Durable Object;
  - `init_tracing()` sends `tracing` output to `console_log!`, filtered by `RUST_LOG`;
  - the pool gets `acquire_timeout(10s)`;
  - adds the `tracing-subscriber` dependency and the sqlx-postgres patch entry.
- **`crates/instant-worker/wrangler.toml`:** see section 2.
- **Hyperdrive config `373fc0afe8a7431b88970c9f530f6435`:** caching disabled.

Untouched: the Instant server crates (`instant-server`, `instant-core`, `sqlx-shim`) and the native server's dependencies.

## 12. What was left deployed / cleaned up

As of this report, nothing has been cleaned up. The deployment-or-delete decision is still with the user.

Still on Cloudflare:
- **Worker:** `instant-worker` at `https://instant-worker.leerosen686.workers.dev`, version `1e82065f`. It has Durable Object class `Node`, with instances `node` and `node-2` … `node-6`; only `node-6` is in use.
- **Hyperdrive config:** `instant` (`373fc0afe8a7431b88970c9f530f6435`), caching disabled.

Elsewhere:
- **Neon database:** throwaway; the user will delete it. It holds the test apps created by `create-app.sh`.
- **Committed:**
  - the code fixes: `vendor/sqlx-postgres` with `VENDORED.md`, `src/main.rs`, `Cargo.toml`, `Cargo.lock`;
  - `wrangler.toml` with only the new `INSTANT_HANDLE_RECEIVE_TIMEOUT_MS` var and a commented `NODE_NAME`;
  - updated docs (`docs/CLOUDFLARE.md`, the crate README);
  - this directory, minus the raw `tail-json.log` (see section 10).
- **Kept local, not committed:** the deployment's `wrangler.toml`, which holds the Neon connection string, the Hyperdrive id, the workers.dev `BASE_URL` and `NODE_NAME = "node-6"`.

To remove the Cloudflare resources:
```
npx wrangler delete                      # from crates/instant-worker
npx wrangler hyperdrive delete 373fc0afe8a7431b88970c9f530f6435
```
