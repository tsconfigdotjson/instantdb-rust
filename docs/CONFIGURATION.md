# Configuration

The server is configured entirely through environment variables. Everything
has a default, so the server runs with none of them set against a local
Postgres at `postgres://instant:instant@localhost:5432/instant`.

## Core

| var | default | |
|---|---|---|
| `DATABASE_URL` | `postgres://instant:instant@localhost:5432/instant` | any standard Postgres 16–18 with the schema from `scripts/apply-migrations.sh` |
| `PORT` | `8888` | |
| `BASE_URL` | `http://localhost:$PORT` | the public URL; OAuth redirects and file URLs are built from it |
| `SERVER_SECRET` | generated & persisted | signs storage download URLs. When unset, a random secret is generated on first boot and stored in Postgres, so every node shares it. Set it explicitly to control rotation |
| `PG_POOL_MAX` / `PG_POOL_MIN` | `20` / `2` | Postgres connection pool per node |

## Storage

| var | default | |
|---|---|---|
| `STORAGE_BACKEND` | `postgres` | where `$files` blobs live: `postgres` (correct across nodes with no extra setup), `disk`, or `s3` (any S3-compatible store) |
| `STORAGE_DIR` | `./storage-data` | blob directory for the `disk` backend |
| `S3_BUCKET` | — | bucket for `STORAGE_BACKEND=s3` (required in that mode) |
| `AWS_REGION` | `us-east-1` | bucket region (`AWS_DEFAULT_REGION` / `S3_REGION` are also read) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | — | static keys (+ `AWS_SESSION_TOKEN`). When unset, ECS/EKS container credentials or EC2 IMDSv2 are used and refreshed in the background |
| `S3_ENDPOINT` | — | custom endpoint for MinIO, R2 and similar; setting it selects path-style addressing (`S3_FORCE_PATH_STYLE=0\|1` overrides) |
| `S3_PUBLIC_ENDPOINT` | `S3_ENDPOINT` | the endpoint written into presigned URLs (what browsers can reach) |
| `S3_PRESIGN` | `1` | `$files.url` is a 7-day presigned GET, like hosted Instant. `0` proxies downloads through `/storage/serve` instead (private buckets, temporary credentials) |
| `S3_PRESIGN_ACCESS_KEY_ID` / `S3_PRESIGN_SECRET_ACCESS_KEY` | — | optional long-lived keys used only for presigning (temporary role credentials cap a presigned URL's lifetime at the credential's) |

## Email

Magic-code and invite emails.

| var | default | |
|---|---|---|
| `EMAIL_PROVIDER` | `log` | `log` prints codes to the server log; `cloudflare` sends through the [Cloudflare Email Service](https://developers.cloudflare.com/email-service/) REST API |
| `CLOUDFLARE_ACCOUNT_ID` | — | required for `EMAIL_PROVIDER=cloudflare` |
| `CLOUDFLARE_API_TOKEN` | — | API token with Email Sending permission |
| `INSTANT_APP_EMAIL_SENDER_EMAIL` | `verify@auth-pm.instantdb.com` | default From address; set it to a sender on your verified domain |

## Dashboard and accounts

These only matter if you run the self-hosted Instant dashboard or use
`instant-cli login`.

| var | default | |
|---|---|---|
| `INSTANT_DASHBOARD_URL` | `http://localhost:3000` | the dashboard's origin: the Platform OAuth consent screen and the Google login redirect target |
| `INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_ID` / `_SECRET` | — | Google OAuth client for dashboard login. Unset leaves Google login unconfigured (magic codes still work) |
| `INSTANT_DASHBOARD_SIGNUP_MODE` | `open` | who can sign up: `open` (anyone), `restricted` (existing users plus the allow-list), `closed` (existing users only) |
| `INSTANT_DASHBOARD_ALLOWED_EMAILS` | — | comma-separated emails admitted under `restricted` |
| `INSTANT_SUPERUSER_EMAIL` | — | the dashboard user treated as the operator (`GET /dash/check-admin`) |
| `INSTANT_PAID_FEATURES_FREE` | `false` | like hosted Instant, app and org members added after the free-teams cutoff need a paid plan, which doesn't exist here; `true` admits every member. Owners and earlier members are always admitted |
| `INSTANT_EPHEMERAL_APPS` | on | `off` refuses the unauthenticated `POST /dash/apps/ephemeral` (temporary apps). Turn it off on public deployments |
| `INSTANT_MAX_APPS_PER_USER` | — | apps a dashboard user may own, directly or through their orgs; deleted apps count until purged. Unset: no cap |

## Limits and tuning

| var | default | |
|---|---|---|
| `INSTANT_RATE_LIMITS` | on | `off` disables the per-app token buckets (for load testing) |
| `INSTANT_APP_SIZE_LIMIT_MB` | — | per-app cap on stored data (triples + files). An app over it can still delete, but writes, uploads and stream appends are refused until it's back under. Unset: no cap |
| `INSTANT_QUERY_TIMEOUT_SECS` | `30` | statement timeout for transactions and admin queries |
| `INSTANT_REFRESH_CONCURRENCY` | `8` | concurrent query recomputations per app refresh batch |
| `INSTANT_MAX_QUEUED_MESSAGES` | `10000` | outgoing messages a session may queue before it's disconnected as a slow consumer |

## Background jobs

| var | default | |
|---|---|---|
| `INSTANT_INDEXING_BATCH_SIZE` | `1000` | triples an indexing job (index / unique / type check / required) rewrites per step; the job is released between steps so large attributes never hold a long lock |
| `INSTANT_INDEXING_SWEEP_SECS` | `60` | how often each node picks up indexing jobs nobody is driving (from a node that died, or blocked by a conflicting job) |
| `INSTANT_INDEXING_STALE_SECS` | `600` | an indexing job with no progress for this long is treated as orphaned and reclaimed |
| `INSTANT_SIZE_COLLECT_SECS` | `60` | how often per-app sizes are rolled up and the size cap re-checked |
| `INSTANT_HARD_DELETE_GRACE_HOURS` / `_SWEEP_SECS` | `48` / `3600` | deleted apps and attributes can be restored for this long; then the sweeper purges their data |

## Testing only

Never set these in production.

| var | default | |
|---|---|---|
| `INSTANT_WEBHOOK_ALLOW_INSECURE` | `false` | lets webhooks target `http://` and private hosts |
| `INSTANT_OAUTH_ALLOW_PRIVATE` | `false` | lets OIDC discovery / token / userinfo / JWKS fetches reach private addresses (mock providers on localhost) |
