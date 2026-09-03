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
- `replay.mjs` — 24-step scenario across init, schemaless transacts, queries
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
  transports). `inferred-types` on
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
  `member-missing` against a live session). Responses are folded to
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
- `fuzz.mjs` — seeded random tx-steps + queries replayed on both servers;
  asserts per-server invariants (monotonic tx-ids) and cross-server equality
  of every query result. `node fuzz.mjs <app> <app> <token> [seed] [rounds]`.
- `lib.mjs` — capture clients, normalization, folding.

This harness found (and pinned as regression coverage) real divergences during
development: deep-merge null semantics, system-catalog attr visibility,
`tx-step` vs `tx-steps` error wording, and subscribe-stream validation order.
