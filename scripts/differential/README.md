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

- `docker-compose.yml` — legacy postgres (host :8890), minio, and the legacy
  server (host :8891) from `ghcr.io/instantdb`.
- `provision.sh` — creates the same app id + admin token in both servers'
  databases (both run the same legacy schema).
- `replay.mjs` — 22-step scenario across init, schemaless transacts, queries
  (nested/paginated/cursor round-trip/aggregate), typed-attr query breadth
  ($gt/$lt/$like/$ilike/$in/$not/$isNull/or/and, typed ordering, offset,
  last, fields projection, dot-paths), authed sessions + permissions (real
  refresh-token init, bind rules, view-rule filtering, $users defaults,
  allowed/denied writes), the error matrix, rooms and presence, sync tables,
  streams, and the issue #10 polish: `app-status-changed` pushes with the
  read-only / disabled gates, linked-guest `$users` access after a magic-code
  upgrade, `delete-attr` / `restore-attr` round trips, `request.*` rule
  bindings (origin / x-forwarded-for ride on the upgrade request), `$rateLimits`
  buckets, and the system-column / system-entity guards. `inferred-types` on
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
  invalid rules), and the auth/param error matrix. Responses are folded to
  what the CLI reads (server-chosen ids, timestamps and CEL diagnostics
  normalized) and must match. `node dash.mjs <app> <token> [<app2> <token2>]`.
- `fuzz.mjs` — seeded random tx-steps + queries replayed on both servers;
  asserts per-server invariants (monotonic tx-ids) and cross-server equality
  of every query result. `node fuzz.mjs <app> <app> <token> [seed] [rounds]`.
- `lib.mjs` — capture clients, normalization, folding.

This harness found (and pinned as regression coverage) real divergences during
development: deep-merge null semantics, system-catalog attr visibility,
`tx-step` vs `tx-steps` error wording, and subscribe-stream validation order.
