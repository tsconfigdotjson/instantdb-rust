# Parity status vs the legacy Instant server

Legend: ✅ implemented + tested · 🟡 implemented, partial/simplified · ❌ not implemented

## Sync protocol (websocket `/runtime/session`)

| Feature | Status | Notes |
|---|---|---|
| `init` / `init-ok` (attrs, session-id, auth, app-status) | ✅ | validated with official clients |
| refresh-token auth on init, invalid-token logout error | ✅ | |
| `__admin-token` perms bypass | ✅ | |
| `add-query` / `add-query-ok` / `add-query-exists` | ✅ | result join-rows + page-info + aggregate |
| `remove-query` | ✅ | |
| `transact` / `transact-ok` (tx-id watermark) | ✅ | |
| `refresh-ok` push with computations + attrs | ✅ | result-hash suppression; per-app coalesced refresh batches, identical (query, auth) recomputed once across sessions |
| topic-based invalidation narrowing | ✅ | coarse topics per registered query (`instant_core::topics`, QUERY.md §6.2 shapes with result substitution on the entity fetch) matched against the tx's `rust_tx_changes` rows; unresolvable shapes and `.ref(` rules fall back to catch-all; result-hash suppression remains the backstop |
| error shapes (`type`, `hint`, `original-event` echo) | ✅ | full legacy key set incl. null `hint`/`client-event-id` (conformance error matrix) |
| rooms: join/leave/set-presence/refresh-presence | ✅ | cross-node via Postgres; in-room asserts + `set-presence-ok`/`client-broadcast-ok` acks like legacy |
| `patch-presence` incremental edits | ✅ | diff-based patches for core > 0.17.5; full snapshots for older clients and fresh joiners |
| `client-broadcast` / `server-broadcast` | ✅ | |
| message batching (JSON array frames) | ✅ | queued messages coalesce into one array frame for core > 0.22.75 (see divergence table) |
| legacy field superset (`processed-isn`, `isn`, `result-meta`, computation metadata, `trace-id`) | ✅ | see "Wire-level divergences" below |
| `skip-attrs` version gating | ✅ | refresh-ok omits `attrs` for core > 0.20.4 unless attrs changed (legacy session.clj:503-533) |
| SSE fallback transport (`/runtime/sse`) | ✅ | sse-init handshake + POST envelope (scripts/sse-test.mjs) |
| sync tables (`start-sync`, `sync-load-batch`, `sync-update-triples`, `resync-table`, `remove-sync`) | ✅ | admin-only like legacy (session.clj:281-284); trigger-based per-tx change log replaces the WAL feed; cross-session resync + pruned-log forced-restart path (scripts/synctable-test.mjs, scripts/conformance-test.mjs) |
| streams (`start-stream`, `append-stream`, `subscribe-stream`, live tailing, resume) | ✅ | disk-backed bytes + NOTIFY fan-out; $streams perms enforced (scripts/streams-test.mjs) |

## InstaQL

| Feature | Status | Notes |
|---|---|---|
| flat + nested queries, forward/reverse links | ✅ | |
| where: eq, dot-paths, `$in`/`in`, `$not`/`$ne`, `$isNull`, `or`, `and` | ✅ | including missing-attr null-prefix expansion |
| `$like` / `$ilike` / `$gt` `$gte` `$lt` `$lte` (typed) | ✅ | index+type guards with legacy error messages |
| `$entityIdStartsWith` | ✅ | |
| order by serverCreatedAt / typed attrs, asc/desc, nulls placement | ✅ | |
| pagination: limit/first/last/offset/before/after/inclusive + page-info | ✅ | top-level only, like legacy |
| fields projection | ✅ | |
| aggregate `count` (admin-only) | ✅ | |
| missing attrs → empty results (no error) | ✅ | |
| `$files` url synthesis / location-id hiding | ✅ | |
| BYOP (bring-your-own-postgres proxy tables) | ❌ | separate legacy subsystem |
| query modifiers by hash (ops flag) | ❌ | internal ops tooling |

## InstaML (transactions)

| Feature | Status | Notes |
|---|---|---|
| add-triple / deep-merge-triple / retract-triple | ✅ | |
| delete-entity + on-delete / on-delete-reverse cascades | ✅ | |
| lookup refs (eid + value position, id-attr lookups) | ✅ | |
| create/update/upsert modes | ✅ | |
| inline add-attr (schemaless), update-attr (flag rewrite), delete-attr (soft) | ✅ | restore-attr is a no-op |
| unique constraint errors (`record-not-unique`) | ✅ | |
| required-attr validation | ✅ | |
| indexed-null backfill | ✅ | |
| checked-data-type validation, oversized-value errors | ✅ | enforced by the schema's check constraints |
| inferred-types tracking | ❌ | attrs report `inferred-types: null` |

## Permissions

| Feature | Status | Notes |
|---|---|---|
| view/create/update/delete rules, `$default` fallbacks | ✅ | |
| `bind` | ✅ | evaluated as pre-bound variables rather than cel.bind textual expansion |
| `data.ref` / `auth.ref` | ✅ | literal-path prefetch |
| `ruleParams` (query `$$ruleParams` + `rule-params` steps) | ✅ | |
| `$users` default rules (view/update self, create true, delete false) | 🟡 | linkedPrimaryUser guest clause simplified to `auth.id == data.id` |
| link/unlink rules per label | ✅ | explicit `allow.link`/`allow.unlink` label maps with `linkedData` binding; update+view fallback otherwise |
| attr-level (field) rules | ✅ | `[etype].fields.[field]` view programs filter triples per entity |
| `request.*` / `rateLimit.*` CEL bindings | ❌ | |
| rule-where query rewriting | ❌ | optimization only; per-entity evaluation gives the same results |
| CEL null-safety (`missing key -> null`) | ✅ | every key a rule statically mentions (select fields + string literals) is pre-inserted as null into `data`/`newData`/`auth`/`ruleParams`/`linkedData` and their nested maps; `has()`/`in` answer true like legacy; anonymous `auth` is an empty map; only keys computed at runtime (`data[someVar]`) still error → deny |

## Auth

| Feature | Status | Notes |
|---|---|---|
| magic codes (send/verify, expiry, one-time) | ✅ | email delivery stubbed: code is logged |
| guest sign-in + guest→user linking | ✅ | |
| refresh tokens (sha256-hashed, no expiry) | ✅ | |
| signout | ✅ | |
| OAuth redirect flow (start/callback/token, PKCE, state+cookie hashes) | ✅ | mock-OIDC integration test; Google = discovery OIDC |
| `signInWithIdToken` (JWKS verification, nonce, audience) | ✅ | Google nonce skip honored |
| `.well-known/openid-configuration` | ✅ | |
| authorized redirect origins (generic/custom-scheme/netlify/vercel) | ✅ | localhost always allowed |
| shared oauth credentials / Apple secret-JWT / GitHub non-OIDC client | ❌ | bring your own provider credentials; generic OIDC only |
| custom email templates/senders | ❌ | |

## Admin API

| Feature | Status | Notes |
|---|---|---|
| /admin/query (object tree, `inference?` singular links) | ✅ | |
| /admin/transact (create/update/merge/link/unlink/delete/ruleParams, lookup strings, attr auto-create, `throw-on-missing-attrs?`) | ✅ | ref-lookup attr names (`owner.id`) not supported |
| impersonation headers (`as-token`/`as-email`/`as-guest`) | ✅ | |
| refresh_tokens / sign_out / users GET+DELETE | ✅ | |
| magic_code / send_magic_code / verify_magic_code / sign_in_guest | ✅ | |
| rooms/presence | ✅ | |
| storage upload/delete (admin + client routes) + signed download URLs | ✅ | Postgres-backed blobs by default (multi-node correct; scripts/multinode-storage-test.mjs); STORAGE_BACKEND=disk optional; S3 would slot in beside them |
| query_perms_check / transact_perms_check (debugQuery/debugTransact) | ✅ | check-results with programs; dry-run/commit semantics |
| /admin/subscribe-query + /admin/sse (SSE transports) | ❌ | |
| schema endpoints (`/admin/schema`, `/dash/.../schema/*` CLI push) | ❌ | schema changes go through /admin/transact attr steps |
| platform tokens (`per_`/`pat_`), dashboard routes | ❌ | out of scope (dashboard is a separate product) |

## Operations

| Feature | Status | Notes |
|---|---|---|
| stateless horizontal scaling | ✅ | pg LISTEN/NOTIFY coordination; multinode test |
| legacy Postgres schema, migrations replay on stock PG | ✅ | tested on Postgres 18 |
| deterministic system-catalog UUIDs | ✅ | verified against hardcoded legacy values |
| app status gates (read-only / disabled) | 🟡 | write gate enforced; `app-status-changed` push not sent |
| rate limiting | ❌ | |
| backups/restore tooling, indexing jobs, sketches | ❌ | ops tooling of the hosted service |

## Wire-level divergences

Every remaining field-level divergence from the legacy server, with the
client-code citation proving it is unread (client paths relative to
`LEGACY/client/packages/core/src/`, legacy server paths relative to
`LEGACY/server/src/instant/`). Machine-checked by
`scripts/conformance-test.mjs` (golden shapes, in CI) and
`scripts/differential/` (side-by-side replay against the legacy server).

| Field / behavior | Legacy | This server | Why it's safe |
|---|---|---|---|
| `processed-isn` (add-query-ok, refresh-ok), `isn` (transact-ok) | `"{slotHex}/{LSN}"` from the logical-replication feed (isn.clj) | same format, `'0/' \|\| pg_current_wal_lsn()` read at emission; monotonic, so `refresh isn >= transact isn` holds | no read of `isn`/`processed-isn` anywhere in `Reactor.js` / `Connection.ts` / `SyncTable.ts` / `Stream.ts` |
| `instaql-query-hash` in refresh-ok computations | Clojure `hash` of normalized query forms (reactive/session.clj:460) | 32-bit hash of the query JSON | client reads only `instaql-query` and `instaql-result` from computations (`Reactor.js:759-788`) |
| `instaql-topic?` in computations | `true` when a refined CEL topic program compiled (reactive/query.clj:147) | always `false` (coarse topic matching only, no refined programs) | unread by the client (`Reactor.js:759-788`); observable results identical |
| `result-meta` | populated only for the `tree` return-type (admin SSE, reactive/query.clj:143-144) | always `null` (ws clients always use `join-rows`) | client never reads `result-meta` (`Reactor.js:665-700`) |
| `trace-id` on every frame | OTel trace id of the handling span (reactive/store.clj:1601) | random 32-hex id per frame (no tracing backend) | only concatenated into debug logs (`Reactor.js:998-1002`) |
| `init-ok.auth` contents | full app + user + creator rows | trimmed `{app: {id, title}, user, admin?}` | client reads only `attrs`, `session-id`, `app-status` from init-ok (`Reactor.js:644-660`) |
| `init-ok` `server-hostname`/`server-port` | present in dev mode only (reactive/session.clj:192-196) | never sent | dev-only tooling keys; unknown keys ignored |
| query result node tree | one node per datalog pattern with `child-nodes` nesting | single node, all triples in one join-row | client flattens all join-rows via `extractTriples` and re-runs InstaQL locally (`model/instaqlResult.js:1-25`, `Reactor.js:673-680`); `page-info`/`aggregate` stay on `result[0].data` as required (`Reactor.js:671-672`) |
| `processed-tx-id` before any tx | `null` (no store entry yet) | `0` | compared with `>=` against pending mutation tx-ids (`Reactor.js:1676-1691`); `0 >= n` and `null` behave identically for an empty app |
| batching scope | only server-broadcast fan-out is batched, 500/frame, core > 0.22.75 (reactive/session.clj:747-757) | any queued frames coalesce (≤100/frame) for core > 0.22.75 | array frames are handled for every op (`Reactor.js:1798-1804`); single-vs-array framing is transport-level |
| `stream-append` payload | file URLs for flushed segments + inline `content` for the tail | always inline `content` | reader consumes `files` (if any) then `content` (`Stream.ts:1077-1084`); bytes delivered are identical |
| `client-broadcast-ok` payload | includes fanned-out envelope | same | no client handler for the op at all (unknown ops ignored, `Reactor.js:932-934`) |
| refresh-ok with zero changed computations | old (≤0.20.4) clients still get an empty `refresh-ok` whenever their queries were stale (session.clj:523) | not sent (nothing to say) | an empty `computations` array with unchanged `attrs` is a no-op client-side (`Reactor.js:733-814`) |
| rate limiting (`rate-limited` errors) | per-app flag-driven (reactive/session.clj:983-987) | not implemented | client has no special handling for `rate-limited`; it surfaces like any other event error |
| error `hint` detail | spec explain data / `input` echo / `expected`+`in` paths per error (util/exception.clj) | `{data-type, errors: [{message}]}` (+ `record-type` where applicable) | the client treats `hint` as opaque debugging JSON; the only pattern-matched key is `hint.record-type` (`Reactor.js:1020-1029`), which matches byte-for-byte |
| `add-query-ok.processed-tx-id` freshness | the invalidator's processed watermark, which can lag the latest confirmed tx | exact `max(tx-id)` at query time | client compares with `>=` to GC optimistic mutations (`Reactor.js:1676-1691`); our value is never ahead of what the result reflects, so behavior is identical with earlier GC |
| `join-room-error` op | defined in the client (`Reactor.js:921-927`) but never emitted by the legacy server either | never emitted; join failures use the generic `error` op | matches legacy behavior (no emitter in LEGACY/server) |
