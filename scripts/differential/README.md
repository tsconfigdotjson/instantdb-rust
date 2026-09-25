# Differential harness: legacy server vs rust server

Proves wire parity instead of assuming it (issue #12). Boots the **official
legacy Instant server** (self-hosting images) beside this repo's rust server,
replays byte-identical op scripts against both, and diffs what a client would
compute from each server's frames.

```
./run.sh          # boots the legacy stack, provisions, replays, fuzzes, tears down
KEEP=1 ./run.sh   # keep the legacy stack running afterwards
```

Prerequisites: docker, node ≥ 20, psql, and the rust server already running on
`RUST_URL` (default `http://localhost:8888`) against `RUST_DATABASE_URL`.

## Pieces

- `docker-compose.yml` — legacy postgres (host :8890), minio (host :9000,
  buckets `instant-bucket` for legacy and `instant-rust-bucket` so the rust
  server can run `STORAGE_BACKEND=s3 S3_ENDPOINT=http://localhost:9000`
  against the same store), and the legacy server (host :8891) from
  `ghcr.io/instantdb`.
- `provision.sh` — creates the same app id + admin token in both servers'
  databases (both run the same legacy schema).
- `replay.mjs` — 34-step scenario across init, schemaless transacts, queries
  (nested/paginated/cursor round-trip/aggregate), typed-attr query breadth
  ($gt/$lt/$like/$ilike/$in/$not/$isNull/or/and, typed ordering, offset,
  last, fields projection, dot-paths), authed sessions + permissions (real
  refresh-token init, bind rules, view-rule filtering, $users defaults,
  allowed/denied writes), the error matrix, rooms and presence, sync tables,
  streams, and the issue #10 polish: `app-status-changed` pushes with the
  read-only / disabled gates, linked-guest `$users` access after a magic-code
  upgrade, `delete-attr` / `restore-attr` round trips, `request.*` rule
  bindings (origin / x-forwarded-for ride on the upgrade request), `$rateLimits`
  buckets, the system-column / system-entity guards, and the admin SSE
  transports (issue #8): `POST /admin/subscribe-query` sessions (admin and
  `as-token`-impersonated, object-tree `add-query-ok` / `refresh-ok` with
  `result-meta` page-info compared whole) and the generic `POST /admin/sse` +
  `/admin/sse/push` session driving join-rows queries, transacts and a stream a
  socket subscriber tails (`connectSse` in lib.mjs mirrors the SDK's
  transports); and the issue #29 items (steps 29-34): the cel-java strings /
  math extensions and `getTime` / `timestamp` overloads in binds and every
  rule kind, view + field rules under a `fields` projection, `$isNull` inside
  `or`, link-rule `actions` / `linkedData.ref` / link-on-create and pre-tx
  `data.ref` in update / delete rules, `attrs.allow.create`, the `mode`
  pre-pass messages and tx-step shape specs, the admin presence route with
  re-fetched users and `instance-id`, `resync-table` mismatch checks, and the
  OAuth callback's 400 surfaces + `?test-redirect` page; step 35 the
  browser's SSE fallback transport (`GET` / `POST /runtime/sse`) and
  `POST /runtime/signout`. `inferred-types` on
  attrs is compared for real (it used to be normalized away). Frames are folded into the
  **client-visible projection** (exactly what `Reactor.js`/`SyncTable.ts`/
  `Stream.ts` read, with volatile server-chosen values normalized) and must
  match byte-for-byte. Key sets per op are compared raw. Remaining diffs must
  be listed in `allowed-divergences.json` with a client-code citation, and the
  run fails on anything unlisted. `DUMP_STEPS=<name,...>` prints the folded
  frames of a step per server; `DUMP_OPS=1` prints the raw op sequence per
  step and connection (which queries a `refresh-ok` recomputed, whether it
  carried attrs) — the quickest way to see why one server refreshed a session
  the other did not.
- `dash.mjs` — the `/dash/*` routes `instant-cli` uses (issue #6): schema
  pull, `schema/steps/apply` with the exact add-attr + unique/index/required/
  check-data-type job steps `@instantdb/platform` emits, indexing-job polling
  (issue #5: every job type's success and error path — too-large values,
  duplicate values, invalid / date type checks, required with nulls, the
  `remove-*` jobs — with job stages, error codes, samples and estimates
  compared, and the pulled attrs checked for lingering in-flight markers)
  (completed and errored jobs with their invalid-data samples), server-side
  `schema/push/{plan,apply}`, perms pull, rules push (valid, unchanged and
  invalid rules), the auth/param error matrix, and the HTTP side of the admin
  SSE routes (auth / query / push-envelope errors, `session-missing` and
  `member-missing` against a live session), and — with `DASH_USER_TOKEN`
  (a dashboard refresh token `provision.sh` seeds on both servers for the
  app's creator) — the CLI's app / info / claim / auth / email routes
  (issue #29): `/dash/me`, `/dash`, app create / get / delete, orgs, OAuth
  providers / clients / redirect origins and the `/auth` summary, email
  templates and status, direct indexing-job creation with its validation
  matrix, ephemeral apps and `claim`. Responses are folded to
  what the CLI reads (server-chosen ids, timestamps and CEL diagnostics
  normalized) and must match. `node dash.mjs <app> <token> [<app2> <token2>]`.
- `storage.mjs` — the storage surface (issue #9): every `db.storage.*`
  admin route and browser `StorageAPI` route (uploads with / without
  metadata headers, the `create`/`delete`/`view` rules for refresh-token
  and impersonated callers, single and bulk deletes, the deprecated
  signed-upload-url / consume / signed-download-url / list flows), `$files`
  over `/admin/query` (fields projections, where, order) and over the socket
  (admin + rule-filtered), path replacement, and `$files` writes through
  transact (system-column guards with legacy's vectorized `input`). Every
  download URL is fetched and compared by what a browser receives (status,
  bytes, content-type, content-disposition, cache-control). When the rust
  server runs `STORAGE_BACKEND=s3` the presigned URL shape (key layout,
  SigV4 params, day-bucketed date, 7-day expiry, `response-cache-control`)
  is compared with legacy's too; on the other backends rust proxies through
  `/storage/serve` and only the fetched content is compared.
  `node storage.mjs <app> <token>`.
- `fuzz.mjs` — seeded random tx-steps + queries over the whole client
  grammar (every tx-step op incl. `mode`, links, schema churn, malformed
  steps; every where operator, dotted link paths, first/last/after/before
  cursor walks, fields, nested links, `$$ruleParams`) replayed on both
  servers; asserts per-server invariants (monotonic tx-ids) and cross-server
  equality of every query result and error type.
  `node fuzz.mjs <app> <app> <token> [seed] [rounds]`. CI runs two seeds per
  PR and ten longer seeds nightly (`schedule` in ci.yml).
- `errors.mjs` / `errors-allowed.json` — the error matrix: one probe per
  externally reachable legacy error type (`err:*` in surface.json) over HTTP
  and the ws session; the normalized envelope (status, type, message, hint)
  must match. The timeout probes hold `LOCK TABLE triples IN SHARE MODE` on
  each server's database so a transact outlives the 5s handler timeout
  (ws, `operation-timed-out`) or the 30s statement timeout (HTTP, `timeout`).
  `unreachable.json` lists the error types no request can produce, with the
  reason. `node errors.mjs <app> <token> <user-refresh-token>`.
- `schema.mjs` / `schema-allowed.json` — diffs the two live database
  catalogs (tables, columns, constraints, indexes, enums, functions,
  triggers): an upstream migration the vendored copy lacks fails CI here
  instead of at runtime. `node schema.mjs`.
- `stress.mjs` — scheduling stress: a large transact followed immediately by
  presence, a query and a broadcast on the same session; every op must be
  answered on both servers, peers must see the presence/broadcast, and the
  states must converge (the reply order is printed; both servers run a
  session's ops on legacy's group keys).
- `dash.mjs` step 39 drives the webhook management routes, the events a
  transaction queues and the payload for them on both servers.
- `dash.mjs` step 40 drives the dashboard's Google login (`/dash/oauth/start`
  with legacy's unconfigured client, every callback error path, a real Google
  rejection, the token errors), the get-a-db creation gates, `track-import`
  and the active-session stats; step 41 the admin magic-code routes
  (`send_magic_code` hands the code back, `verify_magic_code` signs in);
  step 42 the runtime OAuth routes (the `start` redirect through Google's
  real discovery document, the callback / token / id_token error surfaces,
  `openid-configuration`).
  `scripts/dash-login-test.mjs` (the `cargo test + e2e` job) runs the full
  Google round trip against a mock token endpoint.
- `lib.mjs` — capture clients, normalization, folding.
- `surface.mjs` / `surface.json` — the legacy server's public surface derived
  mechanically from the vendored source (every route table, ws op, tx-step
  op, InstaQL option and where operator, custom CEL overload, error type);
  `--check` runs in CI so the manifest can't drift from `LEGACY/`.
- `coverage-hook.mjs` / `coverage.mjs` / `coverage-baseline.json` — the
  capture clients and a wrapped `fetch` record which surface items a run
  exercised. "Covered" means the harness *sent* that route / op / option /
  operator to a server, or *saw* that error type from one, during a run whose
  comparisons all passed; it is a reachability count, not a per-item proof
  that both servers' responses were compared (the replay, dash, storage,
  error-matrix and fuzz layers are what compare, and every mismatch they find
  fails the run). `run.sh` prints per-group coverage with the uncovered list and
  fails if an item in the committed baseline is no longer exercised
  (`node coverage.mjs --write <file>` updates the baseline after adding
  coverage). `out-of-scope.json` names the hosted-only items (billing,
  backups / restores, sunset stages, Postmark sender verification, the
  operators' reports) and `unreachable.json` the error types no request can
  produce, with a reason each; they leave the counted total and are
  reported on their own line, so the percentage measures what a
  self-hosted server can serve. Groups `demo`, `health`, `ws-internal`, `cel-internal` are
  listed but not counted.

This harness found (and pinned as regression coverage) real divergences during
development: deep-merge null semantics, system-catalog attr visibility,
`tx-step` vs `tx-steps` error wording, and subscribe-stream validation order.

The official SDK suites (`@instantdb/core`'s browser e2e, `instant-cli`'s e2e)
run against this same legacy stack and the rust server in the `sdk-suites`
CI job; see `scripts/sdk-suites/README.md`.
