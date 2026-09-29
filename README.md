# Instant Sync Engine — Rust

[![CI](https://github.com/tsconfigdotjson/instantdb-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/tsconfigdotjson/instantdb-rust/actions/workflows/ci.yml)

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
- **Storage**: `$files` namespace with the full admin + client route set
  (`uploadFile`, `delete`/`deleteMany`, the deprecated signed-upload-url /
  signed-download-url / list flows). Blobs live in Postgres by default so
  every node can serve every file; a local-disk backend and an
  S3-compatible backend (AWS S3, Cloudflare R2, MinIO) are built in. The S3
  backend uses the legacy server's exact bucket layout, so an existing bucket
  is served as-is, and hands out presigned URLs exactly like legacy.
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
| `SERVER_SECRET` | generated & persisted | signs storage download URLs; auto-generated on first boot and stored in Postgres when unset — set explicitly to control rotation |
| `STORAGE_BACKEND` | `postgres` | blob store: `postgres` (multi-node correct), `disk`, or `s3` (any S3-compatible store) |
| `STORAGE_DIR` | `./storage-data` | blob directory for the `disk` backend |
| `S3_BUCKET` | — | bucket for `STORAGE_BACKEND=s3` (required in that mode) |
| `AWS_REGION` | `us-east-1` | bucket region (`AWS_DEFAULT_REGION` / `S3_REGION` also read) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | — | static keys (+ `AWS_SESSION_TOKEN`); when unset, ECS/EKS container credentials or EC2 IMDSv2 are used and refreshed in the background |
| `S3_ENDPOINT` | — | custom endpoint for MinIO / R2 / etc.; setting it selects path-style addressing (`S3_FORCE_PATH_STYLE=0|1` overrides) |
| `S3_PUBLIC_ENDPOINT` | `S3_ENDPOINT` | endpoint written into presigned URLs (what browsers can reach) |
| `S3_PRESIGN` | `1` | `$files.url` is a 7-day presigned GET like legacy; `0` proxies downloads through `/storage/serve` instead (private buckets, temporary credentials) |
| `S3_PRESIGN_ACCESS_KEY_ID` / `S3_PRESIGN_SECRET_ACCESS_KEY` | — | optional long-lived keys used only for presigning (temporary role credentials cap a presigned URL's life at the credential's) |
| `EMAIL_PROVIDER` | `log` | magic-code email delivery: `log` (print code to server log) or `cloudflare` ([Email Service](https://developers.cloudflare.com/email-service/) REST API) |
| `CLOUDFLARE_ACCOUNT_ID` | — | required for `EMAIL_PROVIDER=cloudflare` |
| `CLOUDFLARE_API_TOKEN` | — | API token with Email Sending permission |
| `INSTANT_APP_EMAIL_SENDER_EMAIL` | `verify@auth-pm.instantdb.com` | default From address — set to a sender on your verified Cloudflare domain |
| `PG_POOL_MAX` / `PG_POOL_MIN` | `20` / `2` | Postgres pool per node |
| `INSTANT_REFRESH_CONCURRENCY` | `8` | concurrent query recomputations per app refresh batch |
| `INSTANT_MAX_QUEUED_MESSAGES` | `10000` | outgoing messages a session may queue before it is disconnected as a slow consumer |
| `INSTANT_RATE_LIMITS` | on | `off` disables the per-app token buckets (load testing) |
| `INSTANT_SUPERUSER_EMAIL` | — | the dashboard user `GET /dash/check-admin` treats as the operator (legacy's superuser flag) |
| `INSTANT_DASHBOARD_SIGNUP_MODE` | `open` | dashboard signup policy for magic-code / Google login: `open` (anyone), `restricted` (existing users plus the allow-list), `closed` (existing users only); legacy's dashboard-signup flag |
| `INSTANT_DASHBOARD_ALLOWED_EMAILS` | — | comma-separated emails admitted under `restricted` |
| `INSTANT_PAID_FEATURES_FREE` | `false` | `true` admits every app / org member without a Pro or Startup row in `instant_subscriptions` (legacy's `paid-features-free` flag; members who joined before the free-teams cutoff and owners are always admitted) |
| `INSTANT_DASHBOARD_URL` | — | the self-hosted dashboard's origin: the platform OAuth consent screen and the Google login redirect target |
| `INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_ID` / `_SECRET` | — | Google OAuth client for the dashboard's Google login (legacy's names); unset leaves the login unconfigured like legacy |
| `INSTANT_WEBHOOK_ALLOW_INSECURE` | `false` | lets webhooks target `http://` and private hosts (test receivers only; never in production) |
| `INSTANT_INDEXING_BATCH_SIZE` | `1000` | triples an indexing job (index / unique / type check / required) rewrites per step; the job is released between steps so large attrs never hold a long lock |
| `INSTANT_INDEXING_SWEEP_SECS` | `60` | how often each node picks up indexing jobs nobody is driving (created by a node that died, or blocked by a conflicting job) |
| `INSTANT_INDEXING_STALE_SECS` | `600` | a processing indexing job with no progress for this long is treated as orphaned and reclaimed |
| `INSTANT_APP_SIZE_LIMIT_MB` | — | per-app cap on stored data (triples + files). An app over it can still delete, but writes, uploads and stream appends are refused until it is back under. Unset: no cap |
| `INSTANT_SIZE_COLLECT_SECS` | `60` | how often per-app sizes are rolled up (legacy's `triples_size_aggregate`) and the cap re-checked |
| `INSTANT_MAX_APPS_PER_USER` | — | apps a dashboard user may own, directly or through their orgs; deleted apps count until purged. Unset: no cap |
| `INSTANT_EPHEMERAL_APPS` | on | `off` refuses the unauthenticated `POST /dash/apps/ephemeral` (temporary apps); turn it off on public deployments |
| `INSTANT_HARD_DELETE_GRACE_HOURS` / `_SWEEP_SECS` | `48` / `3600` | deleted apps and attrs can be restored for this long, then the sweeper purges their data |
| `INSTANT_OAUTH_ALLOW_PRIVATE` | `false` | lets OIDC discovery / token / userinfo / JWKS fetches reach private addresses (test providers on localhost only; never in production) |

### Horizontal scaling

Nodes hold no cross-request state: queries are computed from Postgres,
invalidation and presence/broadcast fan out over `pg_notify`, presence lives
in an unlogged Postgres table with node heartbeats. Run as many replicas as
you like behind any websocket-capable load balancer
(`docker compose up --scale server=3`). `scripts/multinode-ws.mjs`
demonstrates two clients on two different nodes syncing live.

### Observability

`GET /metrics` serves Prometheus text format: live sessions (total and per
app), message counters, query/transact/add-query duration histograms,
NOTIFY lag, refresh batch latency, topic-skip / recompute / dedupe counters,
attr-cache hits, pool utilization and process RSS/CPU. It is unauthenticated
and cheap to scrape; keep it behind your proxy's network boundary.

### Performance

The refresh path is where a sync engine spends its CPU. Per app transaction
this server loads the tx's triple changes once, matches them against the
topics of every registered query (`crates/instant-core/src/topics.rs`, see
`docs/QUERY.md` §6), recomputes only the matching queries — once per distinct
(query, auth) across all sessions — and pushes `refresh-ok` to the sessions
whose result changed. Bursts of transactions coalesce into one batch per app.
`docs/PERF.md` has the load-test methodology and before/after numbers; the
`instant-loadtest` binary (`crates/instant-loadtest`) reproduces them against
any server, from any machine that can reach it:

```sh
./scripts/create-app.sh "loadtest" | tee /tmp/app.txt   # app id + token
INSTANT_RATE_LIMITS=off ./target/release/instant-server &
cargo run --release -p instant-loadtest -- --apps "$(grep '^app_id=' /tmp/app.txt | cut -d= -f2)" \
  --clients 2000 --queries 3 --writers 8 --duration 30 --cleanup
```

## Production deployment

The server speaks plain HTTP/WS and expects to sit behind a TLS-terminating
reverse proxy:

- **TLS / websockets**: terminate TLS at a proxy (Caddy, nginx, a cloud load
  balancer) and forward to the server port. The proxy must support websocket
  upgrades on `/runtime/session` and SSE on `/runtime/sse`. Point clients at
  `https://…` / `wss://…` and set `BASE_URL` to the public https URL so oauth
  redirects and file URLs are generated correctly.
- **Secrets**: `SERVER_SECRET` signs storage download URLs. If unset, a random
  secret is generated on first boot and persisted in Postgres (all nodes share
  it automatically). Set it explicitly only if you want to manage rotation
  yourself — never ship a guessable value.
- **Postgres**: use a strong password (the `instant:instant` credentials in
  the examples are dev-only), enable TLS to the database where it crosses a
  network, and keep Postgres unreachable from the public internet — only the
  server nodes need to talk to it.
- **Network isolation**: expose only the proxy. The server has no separate
  management port; `/admin/*` is part of the public API surface and is
  protected by per-app admin tokens (accepted via the `Authorization: Bearer`
  header only — tokens never appear in URLs, and the server does not log
  request URLs or headers).
- **Request caps**: JSON endpoints accept bodies up to 10MB; storage uploads
  up to 100MB; per-app rate limits apply to all route groups (see
  `crates/instant-server/src/rate_limit.rs`). Size any proxy body limits at or
  above these.
- **CORS**: responses use wildcard CORS without credentials — auth is
  bearer-token based, so browser cookies are never accepted cross-origin.
- **Dependencies**: CI runs `cargo audit` (RustSec advisory database) on every
  push alongside the test suite.

## Migrating from hosted Instant

See [docs/MIGRATION.md](docs/MIGRATION.md). Because this server uses the
legacy schema and deterministic system-catalog IDs, a data export
(`apps`, `attrs`, `idents`, `triples`, `rules`) restores directly, and
existing clients keep working after a URL switch.

### Using `instant-cli`

The official CLI's `push`/`pull` (schema and perms) work against this server
through the same `/dash/...` routes it uses with hosted Instant. `instant-cli
login` works too (magic-code dashboard login, or Google when
`INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_ID` / `_SECRET` are set); the quickest
path is still the app's admin token:

```bash
export INSTANT_CLI_API_URI=http://localhost:8888   # this server
export INSTANT_APP_ID=<app-id>
export INSTANT_APP_ADMIN_TOKEN=<admin-token>        # or: instant-cli --token <admin-token>
npx instant-cli@latest pull                        # writes instant.schema.ts / instant.perms.ts
npx instant-cli@latest push                        # diffs, applies, waits for indexing jobs
```

`push schema` runs index/unique/required/type changes as indexing jobs and
reports invalid data exactly like hosted Instant (duplicate values, missing
required values, wrong types). See `docs/ADMIN.md` §6.

## Development

```bash
./scripts/apply-migrations.sh          # one-time schema setup
cargo test                             # 50+ unit/integration tests (needs Postgres)
cargo run -p instant-server            # dev server on :8888
node scripts/smoke-ws.mjs <app-id>     # ws protocol smoke test
node scripts/admin-sdk-test.mjs <app-id> <token>   # official admin SDK suite
node scripts/oauth-test.mjs <app-id>   # oauth flow against a mock OIDC provider
node scripts/multinode-ws.mjs <app-id> # two-node coordination test
node scripts/cli-test.mjs <app-id> <token>         # official instant-cli push/pull (needs the CLI built, see script header)
```

Layout:

- `crates/instant-core` — attrs, triples, InstaML transactions, InstaQL,
  CEL permissions, system catalog. Storage-agnostic logic lives here.
- `crates/instant-server` — axum HTTP/WS server, session handling,
  invalidator, presence, auth routes, admin API, storage adapter.
- `docs/` — protocol and subsystem specs extracted from the legacy codebase
  (`PROTOCOL.md`, `QUERY.md`, `DATAMODEL.md`, `PERMS.md`, `AUTH.md`,
  `ADMIN.md`, `SERVER-SYNC.md`) plus `PARITY.md` for coverage status.
- `web/` — the project's marketing site (React + Vite, static build), live at
  https://instantdbrust.com.
- `LEGACY/` — the original Clojure/TypeScript codebase, kept as reference.

## Parity status

The core product surface — sync protocol, queries, transactions, permissions,
auth, presence, admin API, storage — is implemented and exercised against the
unmodified official clients (browser-validated React app, admin SDK suite).
Known gaps (experimental/rarely-used legacy features) are tracked in
[docs/PARITY.md](docs/PARITY.md). The differential harness
(`scripts/differential/`) runs every counted legacy surface item against the
official legacy server; that proves every surface item matches on the
scripted and probed paths plus what the seeded fuzz layers reach, not that
every possible input matches — see "What the harness proves" in PARITY.md.
