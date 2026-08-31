# Instant Sync Engine — Rust

[![CI](https://github.com/GratefulWorkspace/instantdb-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/GratefulWorkspace/instantdb-rust/actions/workflows/ci.yml)

A from-scratch, wire-compatible reimplementation of the
[InstantDB](https://instantdb.com) sync engine in Rust. It plugs into a
**standard Postgres database**, runs **stateless**, and **scales horizontally**
— any node can serve any client; nodes coordinate purely through Postgres
(LISTEN/NOTIFY). Existing `@instantdb/react`, `@instantdb/core`, and
`@instantdb/admin` clients work unmodified: point them at this server and go.

Built as a migration path for apps on the sunsetting hosted Instant service.

## What's inside

- **The full client sync protocol** over `/runtime/session` websockets:
  `init`, `add-query`, `remove-query`, `transact`, `refresh-ok` push
  invalidation, rooms/presence (`join-room`, `set-presence`,
  `client-broadcast`), and Instant's exact error shapes.
- **InstaQL** query engine over the triple store: dot-path joins, `or`/`and`,
  `$in`, `$not`/`$ne`, `$isNull`, `$like`/`$ilike`, typed comparators
  (`$gt`…`$lte` over string/number/boolean/date), ordering, pagination with
  cursors (`limit`/`first`/`last`/`offset`/`before`/`after`), nested link
  loading, field projection, and `count` aggregates.
- **InstaML** transactions: `add-triple`, `deep-merge-triple`,
  `retract-triple`, `delete-entity` (with `on-delete` cascades), lookup refs,
  create/update modes, schemaless inline `add-attr`, attr updates and soft
  deletes, unique/index enforcement, required validation.
- **Permissions**: Instant's CEL rules engine (`view`/`create`/`update`/
  `delete`, `bind`, `data.ref`, `auth.ref`, `ruleParams`, `$default`
  fallbacks, `$users` system defaults) enforced on queries and transactions.
- **Auth**: magic codes, guest users, refresh tokens, sign-out, and
  OAuth/OIDC sign-in (Google works out of the box via its discovery document)
  including PKCE, `signInWithIdToken`, and the full redirect flow.
- **Admin API**: `@instantdb/admin`-compatible (`/admin/query` object trees,
  `/admin/transact` with the admin steps grammar, `refresh_tokens`, users,
  magic codes, impersonation headers, presence, storage).
- **Storage**: `$files` namespace with HMAC-signed download URLs. Blobs live
  in Postgres by default so every node can serve every file; a local-disk
  backend is available and an S3 adapter can slot in beside them.
- **The legacy database schema**: the exact Postgres schema from the original
  server (its migrations replay cleanly onto stock Postgres 16/17/18), so
  existing exported data drops straight in. System-catalog attr UUIDs are
  byte-for-byte identical.

## Quick start

```bash
docker compose up --build
# or, on bare metal:
#   1. createdb + create role (see below)
#   2. ./scripts/apply-migrations.sh
#   3. cargo run --release -p instant-server
```

Provision an app:

```bash
./scripts/create-app.sh "my app"     # prints app_id + admin_token
```

Point your client at the server:

```js
import { init } from "@instantdb/react";

const db = init({
  appId: "<app_id>",
  apiURI: "http://localhost:8888",
  websocketURI: "ws://localhost:8888/runtime/session",
});
```

Everything else — `useQuery`, `transact`, presence, `db.auth` magic codes —
works exactly as with the hosted service. A complete example lives in
`examples/react-todo`.

### Environment

| var | default | |
|---|---|---|
| `DATABASE_URL` | `postgres://instant:instant@localhost:5432/instant` | any standard Postgres |
| `PORT` | `8888` | |
| `BASE_URL` | `http://localhost:$PORT` | public URL (oauth redirects, file URLs) |
| `SERVER_SECRET` | `dev-secret` | signs storage URLs — set in production |
| `STORAGE_BACKEND` | `postgres` | blob store: `postgres` (multi-node correct) or `disk` |
| `STORAGE_DIR` | `./storage-data` | blob directory for the `disk` backend |

### Horizontal scaling

Nodes hold no cross-request state: queries are computed from Postgres,
invalidation and presence/broadcast fan out over `pg_notify`, presence lives
in an unlogged Postgres table with node heartbeats. Run as many replicas as
you like behind any websocket-capable load balancer
(`docker compose up --scale server=3`). `scripts/multinode-ws.mjs`
demonstrates two clients on two different nodes syncing live.

## Migrating from hosted Instant

See [docs/MIGRATION.md](docs/MIGRATION.md). Because this server uses the
legacy schema and deterministic system-catalog IDs, a data export
(`apps`, `attrs`, `idents`, `triples`, `rules`) restores directly, and
existing clients keep working after a URL switch.

## Development

```bash
./scripts/apply-migrations.sh          # one-time schema setup
cargo test                             # 50+ unit/integration tests (needs Postgres)
cargo run -p instant-server            # dev server on :8888
node scripts/smoke-ws.mjs <app-id>     # ws protocol smoke test
node scripts/admin-sdk-test.mjs <app-id> <token>   # official admin SDK suite
node scripts/oauth-test.mjs <app-id>   # oauth flow against a mock OIDC provider
node scripts/multinode-ws.mjs <app-id> # two-node coordination test
```

Layout:

- `crates/instant-core` — attrs, triples, InstaML transactions, InstaQL,
  CEL permissions, system catalog. Storage-agnostic logic lives here.
- `crates/instant-server` — axum HTTP/WS server, session handling,
  invalidator, presence, auth routes, admin API, storage adapter.
- `docs/` — protocol and subsystem specs extracted from the legacy codebase
  (`PROTOCOL.md`, `QUERY.md`, `DATAMODEL.md`, `PERMS.md`, `AUTH.md`,
  `ADMIN.md`, `SERVER-SYNC.md`) plus `PARITY.md` for coverage status.
- `LEGACY/` — the original Clojure/TypeScript codebase, kept as reference.

## Parity status

The core product surface — sync protocol, queries, transactions, permissions,
auth, presence, admin API, storage — is implemented and exercised against the
unmodified official clients (browser-validated React app, admin SDK suite).
Known gaps (experimental/rarely-used legacy features) are tracked in
[docs/PARITY.md](docs/PARITY.md).
