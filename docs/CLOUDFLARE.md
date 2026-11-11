# Cloudflare Workers (experimental)

Status: **not yet running on Workers.** The server-side changes it needs are
in and tested natively. The Worker host in `crates/instant-worker` is written
but has not been compiled or run; it depends on a workers-rs preview toolchain.
This page records what was checked, what was not, and what is left.

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
| boot and hard-delete sweeps took session-level advisory locks | transaction-level locks, held by a transaction kept open for the duration (all modes) |

Verified natively: with `INSTANT_BUS=local` and the server connected through
PgBouncer 1.22 in `pool_mode = transaction` (the closest local stand-in for
Hyperdrive), the single-node e2e suites pass: ws smoke, wire conformance, SSE,
sync tables, streams, OAuth, security, webhooks and dashboard login.
`scripts/local-bus-test.mjs` covers the local bus in CI.

Two more changes make the server embeddable: `instant-server` is now a
library (`bootstrap`, `spawn_background`, `router`) with a thin `main.rs`,
and the websocket session runs over any frame source and sink
(`ws::serve_channels`), not only an axum socket.

## Not verified

`crates/instant-worker` has never been compiled. Expect to fix things in
this order:

1. **Outbound HTTP.** On any `wasm32` target `reqwest` switches to its
   `fetch` backend, whose futures are not `Send`; axum handlers and
   `tokio::spawn` require `Send`. The 11 call sites (webhook delivery, OAuth
   token exchange, S3, email APIs, the SSRF-guarded client) need a wrapper
   that reads the whole response inside a `Send` shim, and `ssrf.rs` uses
   client options the `fetch` backend lacks.
2. **C dependencies.** `ring` (via rustls, jsonwebtoken) must build with
   emcc. Hyperdrive's connection string needs no TLS, so sqlx's TLS feature
   can be dropped from the worker build if it gets in the way.
3. **`STORAGE_BACKEND=disk` and SMTP email** have no place on Workers; use
   `postgres` or `s3` (R2 works) and `EMAIL_PROVIDER=cloudflare` or
   `postmark`.
4. **axum's `ws` feature** pulls in hyper's server upgrade path. The worker
   never uses that route (it accepts the socket itself), but it still has to
   compile.

## Limits of this shape

- **One node.** A Durable Object is single-threaded with 128 MB of memory.
  At about 48 KB per live connection ([PERF.md](PERF.md)) that is a small
  deployment, not a large one. The next step is one object per app: every
  message on the bus is keyed by app id, so the local bus still holds if all
  of an app's requests reach its object, and only the cross-app dashboard
  routes need forwarding.
- **No scale to zero while clients are connected.** Sessions and their
  subscriptions live in memory, so the object uses standard websockets, not
  the Hibernation API, and the background loops' timers keep it resident.
  Hibernating would mean persisting session state to the object's storage.
- **Database round trips.** The object runs in one location and Hyperdrive
  connects from near it; put Postgres in a nearby region.
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

Needs a Cloudflare account for deployment; `wrangler dev` runs locally
against a local Postgres.

```bash
./scripts/apply-migrations.sh           # schema, against your Postgres
cd crates/instant-worker
npx wrangler dev                        # installs worker-build + Emscripten, builds, serves :8787
```

Then point a client at `http://localhost:8787` as in the README. Settings go
in `wrangler.toml` `[vars]` (and `wrangler secret put`); every string binding
reaches the server as the environment variable of the same name
([CONFIGURATION.md](CONFIGURATION.md)).
