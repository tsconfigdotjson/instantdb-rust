# Differential harness: legacy server vs rust server

Proves wire parity instead of assuming it. Boots the **official
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
- `replay.mjs` — 37-step scenario across init, schemaless transacts, queries
  (nested/paginated/cursor round-trip/aggregate), typed-attr query breadth
  ($gt/$lt/$like/$ilike/$in/$not/$isNull/or/and, typed ordering, offset,
  last, fields projection, dot-paths), authed sessions + permissions (real
  refresh-token init, bind rules, view-rule filtering, $users defaults,
  allowed/denied writes), the error matrix, rooms and presence, sync tables,
  streams, `app-status-changed` pushes with the
  read-only / disabled gates, linked-guest `$users` access after a magic-code
  upgrade, `delete-attr` / `restore-attr` round trips, `request.*` rule
  bindings (origin / x-forwarded-for ride on the upgrade request), `$rateLimits`
  buckets, the system-column / system-entity guards, and the admin SSE
  transports: `POST /admin/subscribe-query` sessions (admin and
  `as-token`-impersonated, object-tree `add-query-ok` / `refresh-ok` with
  `result-meta` page-info compared whole) and the generic `POST /admin/sse` +
  `/admin/sse/push` session driving join-rows queries, transacts and a stream a
  socket subscriber tails (`connectSse` in lib.mjs mirrors the SDK's
  transports); steps 29-34 the cel-java strings /
  math extensions and `getTime` / `timestamp` overloads in binds and every
  rule kind, view + field rules under a `fields` projection, `$isNull` inside
  `or`, link-rule `actions` / `linkedData.ref` / link-on-create and pre-tx
  `data.ref` in update / delete rules, `attrs.allow.create`, the `mode`
  pre-pass messages and tx-step shape specs, the admin presence route with
  re-fetched users and `instance-id`, `resync-table` mismatch checks, and the
  OAuth callback's 400 surfaces + `?test-redirect` page; step 35 the
  browser's SSE fallback transport (`GET` / `POST /runtime/sse`) and
  `POST /runtime/signout`; step 36 the triples a where clause matched under
  `fields` projections, link paths, `or` / `and` / `$not` / `$isNull` and
  view / field rules, step 37 a refresh whose view rule's
  `rateLimit` runs dry (the session's error frame, the unsubscribed query).
  `inferred-types` on
  attrs is compared for real (it used to be normalized away). Frames are folded into the
  **client-visible projection** (exactly what `Reactor.js`/`SyncTable.ts`/
  `Stream.ts` read, with volatile server-chosen values normalized) and must
  match byte-for-byte. Key sets per op are compared raw. Remaining diffs must
  be listed in `allowed-divergences.json` with a client-code citation and the
  exact sub-paths and pinned values they allow (see Allowlists below), and the
  run fails on anything unlisted. `DUMP_STEPS=<name,...>` prints the folded
  frames of a step per server; `DUMP_OPS=1` prints the raw op sequence per
  step and connection (which queries a `refresh-ok` recomputed, whether it
  carried attrs) — the quickest way to see why one server refreshed a session
  the other did not.
- `dash.mjs` — the `/dash/*` routes `instant-cli` uses: schema
  pull, `schema/steps/apply` with the exact add-attr + unique/index/required/
  check-data-type job steps `@instantdb/platform` emits, indexing-job polling
  (every job type's success and error path — too-large values,
  duplicate values, invalid / date type checks, required with nulls, the
  `remove-*` jobs — with job stages, error codes, samples and estimates
  compared, and the pulled attrs checked for lingering in-flight markers)
  (completed and errored jobs with their invalid-data samples), server-side
  `schema/push/{plan,apply}`, perms pull, rules push (valid, unchanged and
  invalid rules), the auth/param error matrix, and the HTTP side of the admin
  SSE routes (auth / query / push-envelope errors, `session-missing` and
  `member-missing` against a live session), and — with `DASH_USER_TOKEN`
  (a dashboard refresh token `provision.sh` seeds on both servers for the
  app's creator) — the CLI's app / info / claim / auth / email routes:
  `/dash/me`, `/dash`, app create / get / delete, orgs, OAuth
  providers / clients / redirect origins and the `/auth` summary, email
  templates and status, direct indexing-job creation with its validation
  matrix, ephemeral apps and `claim`. Responses are folded to
  what the CLI reads (server-chosen ids, timestamps and CEL diagnostics
  normalized) and must match. `node dash.mjs <app> <token> [<app2> <token2>]`.
- `storage.mjs` — the storage surface: every `db.storage.*`
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
  `node fuzz.mjs <app> <app> <token> [seed] [rounds]`. Every PR and push
  runs twelve seeds at 250 rounds (42, 99 and the nightly's original 1-10);
  the nightly (`schedule` in ci.yml, or a manual run with the `nightly`
  input) runs them at 400 rounds plus a window of 40 fresh seeds that moves
  every day (`FUZZ_SEED_WINDOW`). `run.sh` runs every seed and lists the
  failing ones at the end.
- `fuzz-perms.mjs` — the same grammar under **generated permission rules**
  (view / create / update / delete per namespace, a bind, field rules,
  `ruleParams`) driven by four concurrent sessions: a guest, two signed-in
  users and an admin. Compared: every transact outcome and one-shot query
  result per session; at settled checkpoints, each session's folded
  subscription state (what the client computes from its `add-query-ok` /
  `refresh-ok` frames) and the refresh errors it received; bursts of
  transacts from several sessions at once (disjoint entities, so the outcome
  can't depend on their order; queries don't order by `serverCreatedAt`,
  whose order across concurrent transacts is arbitrary) and transact /
  add-query / transact on one session sent without waiting (the per-session
  scheduler). Legacy's known slips are told apart from mismatches: a stale
  result that a fresh legacy session corrects, and `add-query-exists` for a
  query the session removed (legacy's refresh re-registers it), which is
  removed and asked again.
  `node fuzz-perms.mjs <app> <app> <token> [seed] [rounds]`; seeds 7-9 at 120
  rounds per PR (`FUZZ_PERMS_SEEDS` / `FUZZ_PERMS_ROUNDS`), 16 seeds at 300
  nightly, `FUZZ_PERMS_JOBS` (4 nightly) at a time.
- `fuzz-race.mjs` — **rust-only** subscription race invariants (issue #51):
  seeded bursts of unawaited transacts and add-queries of order/limit pages
  (a transact then a subscription to the page it changes, as an infinite
  query does), on one session and across sessions. After every settled
  burst: no `refresh-ok` for a query may arrive before its `add-query-ok`,
  and each held subscription, folded in arrival order the way the client
  folds it, must equal a fresh add-query of the same query. Legacy runs
  add-query and refresh on separate group keys and shares paginated results
  across sessions, so it fails these now and then; this layer is not
  compared against it. It runs against a rust server booted with
  `INSTANT_CHAOS_DELAY_MS=30` (`RACE_URL`, :8885 in CI), which pauses at
  random where add-query and a refresh interleave, so windows that are
  microseconds wide get hit every run. Transact failures (a tx racing a
  delete of its entity) are printed as notes, not failures.
  `node fuzz-race.mjs <app> <token> [seed] [rounds]`; seeds 1-3 at 30 rounds
  per PR (`FUZZ_RACE_SEEDS` / `FUZZ_RACE_ROUNDS`), 10 nightly.
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

## Allowlists

`allowed-divergences.json` (replay, dash and storage layers, keyed by
`path`) and `errors-allowed.json` (error matrix, keyed by `probe`) share one
strict entry format, checked by `loadAllowlist` / `allowDivergence` in
`lib.mjs`. An entry allows one documented difference, not whatever else later
shows up at the same path:

```json
{
  "path": "^dash:38-platform/transferRevoke$",
  "differs": ["^\\.(status|type|message|hint|keys|body)$"],
  "legacy": { "status": 500, "type": "unknown", "message": "Something went wrong. Sorry about this!" },
  "rust": { "status": 200, "body": { "count": 1 } },
  "reason": "why the difference is safe / decided",
  "citation": "LEGACY/...:line"
}
```

- `path` / `probe`: an anchored (`^...$`) regex over the compared path.
  Replay paths are `step:<step>/<conn>`, `final/<conn>/<section>` and
  `keyset/<op>/<key>`; dash paths `dash:<step>/<key>`; storage paths
  `storage:<step>/<key>`; errors-allowed.json uses probe names.
- `differs` (required): anchored regexes over sub-paths of the compared value.
  Object keys and array indexes join with dots, as in the log's "first
  differing sub-path": `.status`, `.hint.debug-uri`, `.0.status`; `^$` is
  the whole value, for scalars such as the keyset "present" / "absent". The
  matcher masks every sub-path that matches on both sides (a key present on
  only one side counts as differing there), and what is left must be
  identical.
- `legacy` / `rust` (optional, but every current entry has both): each side
  must still match its documented behavior, as a deep subset. Object keys
  listed must match and extra keys are fine. Arrays must have the same length
  and match element by element. Scalars compare exactly. The operators are
  `{"$regex": "..."}`, `{"$type": "string" | "number" | "boolean" | "null" |
  "array" | "object"}` and `{"$absent": true}`. If either server changes
  behavior, the entry stops applying and the difference fails.
- Each layer loads only the entries whose path prefix it owns (replay:
  `step:` / `final/` / `keyset/`; dash: `dash:`; storage: `storage:`). An
  entry no layer owns is a load error, and so is a malformed entry or an
  unknown field. If an owned entry allowed nothing in a run, the layer fails
  (`[STALE ALLOWLIST]`). The exception is `"flaky": true` with a
  `"flakyReason"`, which only logs `[STALE ALLOWLIST, flaky]`.
- Every allowed difference logs `allowed by <entry>; differing sub-paths:
  ...`. When an entry's path matched but its pins or sub-paths did not, the
  log says why next to the `DIVERGENCE` / `MISMATCH`.

`schema-allowed.json` is a separate format: `{category, pattern, side?,
reason}`. `pattern` is a start-anchored regex over a catalog row, and `side`
(`"legacy"` / `"rust"`) limits the entry to rows that only that database
has. An entry that allowed nothing fails `schema.mjs`.

This harness found (and pinned as regression coverage) real divergences during
development: deep-merge null semantics, system-catalog attr visibility,
`tx-step` vs `tx-steps` error wording, and subscribe-stream validation order.

The official SDK suites (`@instantdb/core`'s browser e2e, `instant-cli`'s e2e)
run against this same legacy stack and the rust server in the `sdk-suites`
CI job; see `scripts/sdk-suites/README.md`.
