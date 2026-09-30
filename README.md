<p align="center">
  <a href="https://instantdbrust.com">
    <img src="web/public/og.jpg" alt="instantdb rust: Instant, after the sunset. Your @instantdb apps, unmodified, on your own Postgres." width="100%" />
  </a>
</p>

<p align="center">
  <b>A drop-in, self-hosted InstantDB sync engine, rewritten in Rust.</b><br />
  Your existing <code>@instantdb</code> apps, unmodified, on your own Postgres.
</p>

<p align="center">
  <a href="https://instantdbrust.com"><b>Website</b></a> ·
  <a href="https://dash.instantdbrust.com/dash">Demo dashboard</a> ·
  <a href="docs/PARITY.md">Parity</a> ·
  <a href="docs/PERF.md">Performance</a>
</p>

<p align="center">
  <a href="https://github.com/tsconfigdotjson/instantdb-rust/actions/workflows/ci.yml"><img src="https://github.com/tsconfigdotjson/instantdb-rust/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <img src="https://img.shields.io/badge/postgres-16%20%7C%2017%20%7C%2018-336791?logo=postgresql&logoColor=white" alt="Postgres 16 | 17 | 18" />
  <img src="https://img.shields.io/badge/license-MIT-orange" alt="MIT license" />
</p>

---

[Instant Cloud](https://instantdb.com) shuts down on August 31, 2027. This project is a
from-scratch, wire-compatible reimplementation of its sync engine that you run
yourself. Point `@instantdb/react`, `@instantdb/core`, `@instantdb/admin` or
`instant-cli` at it and they keep working. There's no fork and no client patch.

- **Just Postgres.** It runs on stock Postgres 16–18, using the original
  Instant server's database schema and system-catalog IDs byte for byte.
- **Stateless and horizontal.** Nodes coordinate only through Postgres
  (`LISTEN/NOTIFY`). Any node can serve any client, so you scale by adding
  replicas behind a load balancer.
- **Checked against the original, not just ported.** A differential harness
  runs every counted API surface item against the official legacy server and
  compares the responses. [Parity report →](docs/PARITY.md)
- **Cheap to run.** About 48 KB per live connection, results computed once per
  distinct query and shared with every subscriber, and Postgres is the
  bottleneck before the server is. [Numbers →](docs/PERF.md)

## Quick start

```bash
git clone https://github.com/tsconfigdotjson/instantdb-rust && cd instantdb-rust
docker compose up --build              # Postgres + the server on :8888
./scripts/create-app.sh "my app"       # prints app_id + admin_token (needs psql)
```

Point your client at it:

```js
import { init } from "@instantdb/react";

const db = init({
  appId: "<app_id>",
  apiURI: "http://localhost:8888",
  websocketURI: "ws://localhost:8888/runtime/session",
});
```

Everything else (`useQuery`, `transact`, presence, `db.auth`, storage) works
the same as on the hosted service. [`examples/react-todo`](examples/react-todo)
is a complete app.

`instant-cli` works too. Its `push` / `pull` of schema and permissions go
through the same routes as on hosted Instant:

```bash
export INSTANT_CLI_API_URI=http://localhost:8888
export INSTANT_APP_ID=<app_id>
export INSTANT_APP_ADMIN_TOKEN=<admin_token>
npx instant-cli@latest pull            # writes instant.schema.ts / instant.perms.ts
npx instant-cli@latest push
```

<details>
<summary>Running without Docker</summary>

```bash
createdb instant                       # and a role the server can connect as
./scripts/apply-migrations.sh          # the original Instant schema
cargo run --release -p instant-server  # listens on :8888
```

Set `DATABASE_URL` if you're not using the default
`postgres://instant:instant@localhost:5432/instant`.
</details>

## What works

Everything the official clients use is implemented:

| | |
|---|---|
| **Sync protocol** | Websocket and SSE sessions, live query invalidation, optimistic transactions, rooms, presence and broadcast, sync tables and streams |
| **InstaQL** | Nested link queries; `where` with `and`/`or`, `$in`, `$not`, `$isNull`, `$like`, comparators; ordering, cursor pagination, field projection and `count` aggregates |
| **InstaML** | Create/update/merge/link/unlink/delete, lookup refs, cascading deletes, unique/indexed/required/typed attributes, async indexing jobs |
| **Permissions** | Instant's CEL rules engine (`view`/`create`/`update`/`delete`, `bind`, `data.ref`, `auth.ref`, `ruleParams`, `rateLimit`, field rules) |
| **Auth** | Magic codes, guest users, OAuth/OIDC (Google works out of the box), `signInWithIdToken`, refresh tokens |
| **Admin & tooling** | The `@instantdb/admin` API, `instant-cli` push/pull/login, the Platform API and OAuth apps, webhooks, and the self-hosted Instant dashboard |
| **Storage** | `$files` via Postgres (default), local disk, or any S3-compatible bucket; the S3 layout matches the hosted service's, so an existing bucket is served as-is |

The few remaining differences from the original server are listed, with
reasons, in [docs/PARITY.md](docs/PARITY.md).

## Docs

- **[Configuration](docs/CONFIGURATION.md)**: every environment variable.
- **[Deploying](docs/DEPLOY.md)**: production checklist, scaling,
  monitoring, and a full Docker Compose + Caddy stack with the dashboard.
- **[Parity](docs/PARITY.md)**: feature-by-feature status against the legacy
  server and how it's verified.
- **[Performance](docs/PERF.md)**: load-test method, results, and how to
  reproduce them.
- **[Migrating from Instant Cloud](docs/MIGRATION.md)**: coming soon.

## Development

```bash
./scripts/apply-migrations.sh          # one-time schema setup
cargo test                             # unit + integration tests (needs Postgres)
cargo run -p instant-server            # dev server on :8888
node scripts/smoke-ws.mjs <app-id>     # websocket protocol smoke test
```

`scripts/` also has end-to-end suites that drive the official SDKs and CLI
against a running server (`admin-sdk-test.mjs`, `oauth-test.mjs`,
`multinode-ws.mjs`, `cli-test.mjs`, …), and the
[differential harness](scripts/differential/) that compares this server with
the legacy one. CI runs all of them.

| path | |
|---|---|
| `crates/instant-core` | Attributes, triples, InstaML, InstaQL, CEL permissions, system catalog |
| `crates/instant-server` | axum HTTP/websocket server: sessions, invalidation, presence, auth, admin and dashboard APIs, storage |
| `crates/instant-loadtest` | The load generator behind [docs/PERF.md](docs/PERF.md) |
| `web/` | The [instantdbrust.com](https://instantdbrust.com) site |
| `LEGACY/` | The original Instant codebase (Clojure + TypeScript), kept for reference and for the differential harness |

---

<sub>An independent open-source project, not affiliated with or endorsed by
Instant. Background on the shutdown is in
[Instant's announcement](https://www.instantdb.com/essays/instant_team_joins_openai).</sub>
