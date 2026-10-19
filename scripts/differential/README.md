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

### Parallel legs in CI

CI's `differential` job runs `run.sh` as a five-leg matrix. Each leg boots its
own legacy stack and rust servers and runs part of the harness:

```
LAYERS="schema replay stress" ./run.sh   # only these layers
LAYERS="fuzz perms race" SHARD=2/3 ./run.sh   # every 3rd seed of each, from the 2nd
```

A seeded layer makes one run per seed (`fuzz:42`, `perms:7`, `race:1`, …) and
an unseeded layer makes one run, which lands in shard 1. With `RUNS_DIR` set,
a leg lists every run of the full harness in `$RUNS_DIR/planned` and the runs
it made in `$RUNS_DIR/ran`. A partial run (`LAYERS` or `SHARD` set) skips the
coverage check. `differential-merge` (check name `differential vs legacy
server`) then runs `legs.sh inventory`, which fails unless every planned run
was made by exactly one leg. That catches a new layer that no leg lists. When
every leg passed, `legs.sh coverage` runs `coverage.mjs --check` on the union
of the legs' coverage records. Without `LAYERS` and `SHARD`, `run.sh` runs
everything and checks coverage itself, as before.

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
  grammar replayed on both servers; asserts per-server invariants
  (monotonic tx-ids) and cross-server equality of every query result and
  error type. The grammar:
  - tx-steps: every op incl. `mode`, schema churn and malformed steps;
    lookup refs (`[attr-id, value]` on a unique attr) as a step's entity
    (add / deep-merge / retract / delete, a rename through the lookup, on
    the `id` attr and on others, matching an entity or not) and as a
    link's value (one that matches nothing fails the tx);
  - typed, indexed attrs of every checked data type: `number`, `string`,
    `date` (epoch ms, ISO-8601 with `Z`, an offset, no zone, a space for the
    `T`, date-only, `Date.toUTCString()`) and `boolean`, plus checked-type
    violations of each;
  - links of cardinality one (`fuzz.owner`), many (`fuzz.tags`) and
    one-to-one (`fuzz.profile`, a unique ref), linked and unlinked by id or
    lookup, their targets deleted and recreated, queried nested in both
    directions and from every namespace;
  - every where operator (on dates: comparisons with the same input shapes
    plus `Date.toLocaleString()`, `now` and strings neither server parses;
    equality, `$in` (with `null`), `$ne` / `$not`, `$isNull`; on booleans the
    same plus refused strings and `$like`), dotted link paths of one to
    three hops through every link kind, `or` / `and`;
  - first / last / after / before cursor walks (also by date, boolean and a
    unique label, with the inclusive flags), limit / offset / order, fields,
    nested links, `$$ruleParams`.
  An entity a lookup creates on a non-`id` attr gets an id each server picks
  (`<uuid>` in the comparison), so those creations only happen in the
  `tag` / `profile` namespaces, which are never paged by creation time.
  Besides most transacts committing and the final read holding data, the
  success floor asks each new grammar feature (date, bool, tags, profile,
  lookup, hops) used often enough for at least one committed transact and
  one query answered with data (`grammar floor:` in the log).
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
  scheduler). Legacy's known slips are told apart from mismatches:
  - a stale result that a fresh legacy session corrects. Each probe query is
    unique (an `id $ne <random uuid>` clause), so legacy's shared datalog
    cache can't answer it. When the probe still disagrees with the session,
    legacy is asked again 2 s later, because its view rules read cached
    entity maps right after a write.
  - `add-query-exists` for a query the session removed (legacy's refresh
    re-registers it): the query is removed and asked again.
  For a paginated top-level `or`, the triples only a where row carries are
  left out of the comparison in both fuzz layers. Legacy keeps one where row
  per entity (DISTINCT ON), and which branch survives for an entity matching
  several branches depends on its query plan.
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
  microseconds wide get hit every run. 4xx transact failures (a tx racing a
  delete of its entity) are printed as notes, not failures; a 5xx fails it,
  except the named `handler-timeout` exception (below).
  `node fuzz-race.mjs <app> <token> [seed] [rounds]`; seeds 1-3 at 30 rounds
  per PR (`FUZZ_RACE_SEEDS` / `FUZZ_RACE_ROUNDS`), 10 nightly.
- `tx-conflicts.mjs` — **rust-only** concurrent transact conflicts (issue
  #53): 8 sessions transact at once on a pool of 12 entities, 2-4 per tx
  (multi-entity updates in shuffled order, creates, links, delete-entity).
  Every transact must get `transact-ok` or a 4xx. Postgres deadlocks between txs surface
  as a 500 `sql-exception` in legacy; rust orders its row locks like legacy
  and retries a deadlocked or unserializable tx (`service::run_transact`).
  **Named exception `handler-timeout`** (`isHandlerTimeout` in lib.mjs, used
  here and by fuzz-race): a reply of exactly status 500, type
  `operation-timed-out`, message `Operation timed out: handle-receive` is
  counted and reported (`exception handler-timeout: N transact(s) ...`) but
  doesn't fail the check. Under this contention a few txs wait on row locks
  past the 5s handle-receive timeout, and legacy answers those with the same
  500 (session.clj:58, :1160-1178). Any other 5xx, including a deadlock
  `sql-exception` or an `internal-error`, still fails.
  `node tx-conflicts.mjs <app> <token> [seed] [txs-per-session]`; seeds 1-3
  at 60 per session (`TX_CONFLICT_SEEDS` / `TX_CONFLICT_TXS`).
- `rule-wheres.mjs` — legacy's rule-where rewriter (cel.clj:729-1617, the
  `cel-where` group of surface.json). With the operator's `rule-wheres` flag
  listing an app (set in the Instant Config app the self-hosted bootstrap
  creates), legacy compiles view rules a second time with a where-clause
  compiler whose overloads (`==` / `!=` / `in` on `data.*`, `!`,
  `startsWith` / `endsWith` / `contains`, `size(data.ref(...))`,
  `timestamp(data.x)`, `data.ref`, and `type()` overrides that throw) turn
  the rule into InstaQL where clauses, adds them to the query and skips the
  per-entity check for the entities they match. A rule that doesn't
  convert falls back to the per-entity check. The layer seeds one namespace
  per probe rule (as its view and create rule), runs the guest's creates and
  four queries per probe (top level, with a nested link, as a nested form,
  through a where path) over the socket and `/admin/query` as guest: on
  legacy with the flag off, then on (it sets the flag through the config
  app's admin API and waits for legacy to pick it up), and on rust, which
  always checks per entity. Both legacy passes are compared with rust
  (`rulewheres:flag-off/...` and `rulewheres:...`). Socket results are split
  into the triples of the attrs the query reads (`visible`) and the other
  join-row triples (`extra`). On legacy, debugQuery's `rule-wheres` must show
  each probe's overload ran (its pinned where clause, or none for a probe
  whose overload throws next to one that converted) before the probe's
  `cel:*` ids are recorded as covered. `node rule-wheres.mjs <app> <token>`.
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
  self-hosted server can serve. Groups `demo`, `health`, `core` and
  `ws-internal` are listed but not counted.

## Allowlists

`allowed-divergences.json` (replay, dash, storage and rule-wheres layers, keyed by
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
