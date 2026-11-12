# Cloudflare Workers (experimental)

Status: **deployed once, experimental.** `crates/instant-worker` builds for
Workers and, under `wrangler dev` against a local Postgres, passes the same
e2e suites as the native server. It has been deployed to Cloudflare once,
with Hyperdrive in front of a Neon Postgres, and the protocol suites passed
there after the fixes described under
[Hyperdrive and sqlx](#hyperdrive-and-sqlx). The report is
[deploy-test-logs/REPORT.md](../deploy-test-logs/REPORT.md). It depends on a
workers-rs preview toolchain. The native server is unchanged by default; this
is a second way to run the same code.

## The shape

```
client ── Worker ── Durable Object "node" ── Hyperdrive ── your Postgres
                    (the whole server:
                     router, sessions,
                     refresh fan-out)
```

- **Rust on Workers, with Tokio.** The server is built on Tokio, axum and
  sqlx. The stable Workers Rust target (`wasm32-unknown-unknown`) has no Tokio
  runtime and no sockets for sqlx, so that route means rewriting the
  data layer. workers-rs has a preview that builds for
  `wasm32-unknown-emscripten` instead (`worker-build --emscripten`, feature
  `experimental_tokio`), where `tokio::spawn`, timers and `tokio::net` work
  and sqlx connects through Hyperdrive. Its
  [`emscripten-sqlx` example](https://github.com/cloudflare/workers-rs/tree/main/examples/emscripten-sqlx)
  is this project's stack in miniature. `instant-worker` targets that preview.
- **One Durable Object is one node.** A Worker invocation ends with its
  request; the server keeps sessions, subscriptions and caches in memory and
  pushes to websockets when data changes. A Durable Object is the Workers
  primitive that lives across requests, and inside one all handlers share a
  Tokio runtime, so the connection pool and background loops keep working.
- **Postgres through Hyperdrive.** The schema and all ~390 queries stay as
  they are.

## What Hyperdrive rules out, and what changed for it

Hyperdrive pools in transaction mode. It does not support `LISTEN`/`NOTIFY`
or session-level advisory locks
([supported features](https://developers.cloudflare.com/hyperdrive/reference/supported-databases-and-features/)).
The server used both:

| was | now |
|---|---|
| nodes coordinate over `LISTEN/NOTIFY` | `INSTANT_BUS=local` delivers the same messages in-process, for a single node. The default is unchanged |
| `rules`, `apps.status` and `webhooks` changes announced by row triggers | with the local bus, the routes that write those rows publish the message themselves. An edit made directly in SQL is not pushed to live sessions; the server picks it up when its caches expire |
| boot and hard-delete sweeps take session-level advisory locks | with the local bus, transaction-level locks, held by a transaction kept open for the duration. The default is unchanged |

A node on the local bus may also be far from its database, where every
statement is a network round trip (about 25 ms each in the deployment
below), and it may start often (an idle Durable Object is evicted after
about a minute). So with the local bus, and only then:

| | default | local bus |
|---|---|---|
| boot | runs the bootstrap (about 96 statements) | skips it when `rust_server_config` holds this build's marker: one statement |
| a query's rules and watermark (tx id, ISN) | three statements | one |
| a query's snapshot transaction | `BEGIN`, then `SET TRANSACTION` | one `BEGIN ISOLATION LEVEL ...` |

Counted locally from the server's statement log (excluding `BEGIN`, which
sqlx does not log): a new query went from 8 statements to 5, a transaction
from 11 to 10, and a transaction through to another session's refresh from
17 to 15. A second deployment confirmed the counts and the effect (below).

Nothing is cached to get there: every operation still reads the current
rules. (A first attempt cached rules for 5 s; the conformance suite, which
edits rules directly in SQL, caught the staleness.)

### Hyperdrive and sqlx

`wrangler dev` connects straight to Postgres, so it does not exercise
Hyperdrive's pooler. The first real deployment failed every request until
these were fixed:

- **sqlx-postgres 0.8.6 is patched for the Worker only**
  (`crates/instant-worker/vendor/sqlx-postgres`, via that crate's
  `[patch.crates-io]`):
  - It sends no `Close` messages, which Hyperdrive rejects.
  - It accepts Hyperdrive's quoted error severity (`"FATAL"`).
  - Prepared statements are named from a process-wide counter, because
    Hyperdrive mixes up names that repeat across connections.
  - `ping()` on release sends no bare `Sync`, which Hyperdrive leaves
    unanswered after a rolled-back transaction. Each one leaked a pooled
    connection.
- **Turn Hyperdrive's query caching off**
  (`wrangler hyperdrive update <id> --caching-disabled`). With caching on, a
  read straight after a write returns the old result.
- **`INSTANT_HANDLE_RECEIVE_TIMEOUT_MS = "30000"`.** Every statement crosses
  Hyperdrive, and the native 5 s limit cancelled transactions midway.

Two more changes make the server embeddable: `instant-server` is a library
(`bootstrap`, `spawn_background`, `router`) with a thin `main.rs`, and the
websocket session runs over any frame source and sink (`ws::serve_channels`),
not only an axum socket.

## What differs in the Workers build

The same crates compile for `wasm32-unknown-emscripten` with these
differences, all selected by target, none by a feature you set:

| | native | Workers |
|---|---|---|
| outbound HTTP (webhooks, OAuth, S3, email APIs) | reqwest | the runtime's `fetch`, through `src/http.rs`. No client timeout, no address pinning; redirects are never followed |
| TLS to Postgres | rustls | none; Hyperdrive's connection is plain |
| JWT verification | jsonwebtoken 9 (ring) | supplied by `instant-worker`: jsonwebtoken 10 on its pure-Rust backend, in that crate's own lockfile |
| SMTP email | yes | no; use `EMAIL_PROVIDER=cloudflare` or `postmark` |
| S3 credentials from instance or task roles | yes | no; static keys only |
| Tokio | multi-threaded | single-threaded, driven by the Workers event loop |

rustls and jsonwebtoken 9 are out because `ring` has no random source on
this target.

## What has been verified

- **Native, default bus:** the unit and integration tests and the
  single-node e2e suites.
- **Native, `INSTANT_BUS=local` behind PgBouncer 1.22 in
  `pool_mode = transaction`** (the closest local stand-in for Hyperdrive):
  the same e2e suites.
- **The Worker under `wrangler dev`**, with local Postgres through the
  Hyperdrive binding's local connection string: ws smoke, wire conformance,
  SSE, sync tables, streams, the local-bus test, OAuth, security, webhooks
  and dashboard login all pass, with no panics in the runtime log. The
  release build is a 12.5 MB wasm module (the limit is 64 MiB).
- **One deployment to Cloudflare** (workers.dev, Hyperdrive, Neon in AWS
  us-east-2; [report](../deploy-test-logs/REPORT.md)):
  - **Startup:** Cloudflare accepted the Emscripten build. Startup time was
    60–161 ms.
  - **Passed:** ws smoke, wire conformance (222 assertions), SSE, sync
    tables, the local-bus test and streams all pass. The tail shows no
    errors.
  - **Cold start:** 4.6 s median, of which 3.7 s was the bootstrap's 96
    statements ([latency report](../deploy-test-logs/latency/LATENCY.md)).
    An object with no requests was evicted after 69 to 105 s.
  - **Latency:** from Toronto, a new `add-query` took 243 ms median, a
    transaction 295 ms, and a transaction through to another session's
    refresh 482 ms. Nearly all of it is statement round trips at about
    25 ms each.

- **A second deployment, with the boot fast path and the statement
  savings** (same setup, `NODE_LOCATION_HINT=enam`;
  [report](../deploy-test-logs/latency2/LATENCY-2.md)):
  - **Passed:** the same six suites; conformance took 15 s against 51 s.
  - **Latency:** from Toronto, a new `add-query` took 122 ms median (5
    statements), a transaction 221 ms (10), and a transaction through to
    another session's refresh 339 ms (15). A statement took 18 ms median,
    down from 25; the cause was not isolated.
  - **Boot:** a repeat boot ran 1 statement and answered in 0.44 s and
    1.01 s (two samples), most of it between the Worker and the Durable
    Object starting. The first boot of a build still runs the bootstrap
    (4.5 s).
  - **Idle:** with no clients, the background loops ran 22 statements a
    minute until the object was evicted after about 65 s.

- **A third round on Cloudflare**
  ([report](../deploy-test-logs/round3/ROUND-3.md)), with the local mock
  servers reached through public tunnels:
  - **Passed:** the security suite; webhooks (37 checks) and OAuth (111
    checks, including signed id_tokens) without the private-address flags,
    so URL vetting and its DNS lookup work on Workers; magic-code sign-in;
    dashboard login except two checks that assume an `http://localhost`
    server.
  - **Soak, 48 minutes:** 5 sessions, 1148 operations, 287 distinct query
    shapes; no failures, no dropped sessions, no reboots, flat latency.
    Under that load a transaction took about 430 ms median and a refresh on
    another session about 700 ms, roughly twice the single-client figures
    above; where the extra time goes was not measured.
  - **One unexplained failure:** the first webhook run failed with
    `database error: ... shutting down` as its connections closed; the
    database had not restarted, and a rerun 30 s later passed.

- **A fourth round, diagnostic**
  ([report](../deploy-test-logs/round4/ROUND-4.md)):
  - **Subscribers share one recomputation.** One transaction ran one
    subscription query whether 1, 5 or 20 sessions were subscribed.
  - **Capacity:** 2000 idle subscribed sessions held on one object, with no
    drops and refreshes still arriving in about a second. The tail recorded
    few of the object's events during that test, so a limit warning could
    have gone unseen.
  - **Soak, 3 hours:** 5 sessions, no failed operations, every one of 1079
    refreshes on time, no restarts, no latency drift.
  - **Speed varies by node, not by load.** Of five fresh objects, two were
    about twice as slow as the rest for every operation, including ones
    that run no SQL (a statement took 49 ms against 23 ms). Session count
    and pool size did not predict which. The likely cause is where
    Cloudflare placed each object, which nothing reports. Round 3's slower
    figures match the slow group.
  - **A dropped database connection could end live updates.** With the
    server's Postgres connections killed mid-refresh, the failing refresh
    unsubscribed the query (as the original server does on any failed
    refresh) and no client was updated again, while writes kept succeeding.
    Fixed after this round, on every bus: a refresh that fails with an
    internal error is retried up to three times before the query is
    unsubscribed. The fix is verified against a local reproduction, not yet
    on Cloudflare.

## Not verified

- **On a deployment:** the limit on sessions per object (2000 idle ones
  held; active load at that scale is untested), anything longer than a
  3 hour soak, and the refresh retry above.
- **`STORAGE_BACKEND=s3` on Workers.** It compiles; the S3 suites were not
  run against the Worker. `disk` has no place there; `postgres` is the
  default and was the one tested.
- **The multinode, admin SDK and differential suites** against the Worker.
- **CI coverage is a smoke test.** The `worker` job builds the Worker and
  runs the ws smoke and local-bus tests against it under `wrangler dev`. It
  is allowed to fail, because the preview toolchain can break it without a
  change here, and `wrangler dev` does not exercise Hyperdrive.

## Limits of this shape

- **One node.** A Durable Object is single-threaded with 128 MB of memory.
  At about 48 KB per live connection ([PERF.md](PERF.md)) that is a small
  deployment, not a large one. The next step is one object per app: every
  message on the bus is keyed by app id, so the local bus still holds if all
  of an app's requests reach its object, and only the cross-app dashboard
  routes need forwarding.
- **No scale to zero while clients are connected.** Sessions and their
  subscriptions live in memory, so the object uses standard websockets, not
  the Hibernation API. With no clients it is evicted after about a minute,
  and the next request pays a cold start.
  Hibernating would mean persisting session state to the object's storage.
- **Database round trips.** Every statement is a round trip from the object
  to Postgres, and an operation runs several in sequence. Cloudflare creates
  a Durable Object near the first client that reaches it, which may be far
  from the database: set `NODE_LOCATION_HINT` to the database's region (with
  a new `NODE_NAME`, since the hint applies only at creation). It was set
  on the second deployment, but nothing reports where the object runs, so
  its effect is unconfirmed.
- **Prepared statements are never closed** in the Worker's patched sqlx,
  because Hyperdrive rejects `Close`. Whether they pile up on Postgres
  backends over a long uptime has not been measured.
- **Preview toolchain.** Rust beta, patched Tokio and mio, and a patched
  Emscripten, all pinned by workers-rs and expected to change.

## Why not D1

D1 is SQLite. The data layer is Postgres-specific throughout (`jsonb`,
arrays with `= ANY($1)`, `uuid`, `ON CONFLICT ... RETURNING`,
`FOR UPDATE SKIP LOCKED`, advisory locks, plpgsql triggers, and the original
Instant migrations). D1 also has no interactive transactions (only
`batch()`), caps a query at 100 bound parameters and a database at 10 GB.
InstaML transactions read, check permissions and write inside one
transaction. A port would be a second storage engine with its own parity
work, not an adaptation. If that is ever wanted, SQLite inside a Durable
Object is the better target than D1: it has synchronous transactions and
sits next to the sessions.

## Trying it

`wrangler dev` runs locally against a local Postgres. The first build
installs Rust beta, `worker-build` and the Emscripten SDK (about 1 GB), and
takes several minutes.

```bash
./scripts/apply-migrations.sh           # schema, against your Postgres
cd crates/instant-worker
npx wrangler dev                        # builds, serves :8787
```

Then point a client at `http://localhost:8787` as in the README. Settings go
in `wrangler.toml` `[vars]` (and `wrangler secret put`); every string binding
reaches the server as the environment variable of the same name
([CONFIGURATION.md](CONFIGURATION.md)).

To deploy:
1. Create a Hyperdrive config for your database with caching off:
   `npx wrangler hyperdrive create instant --connection-string=postgres://... --caching-disabled`.
2. Put its id in `wrangler.toml`.
3. Set `BASE_URL` to the Worker's public URL.
4. Run `npx wrangler deploy`.

A redeploy does not restart a running Durable Object: its timers and
websockets keep the old instance resident. Set `NODE_NAME` (a var) to a new
value to start a fresh node. Server logs reach `wrangler tail --format json`,
filtered by `RUST_LOG`.
