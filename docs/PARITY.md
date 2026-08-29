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
| `refresh-ok` push with computations + attrs | ✅ | result-hash suppression; recompute-per-app strategy |
| topic-based invalidation narrowing | 🟡 | legacy matches WAL topics against query topics as an optimization; this server recomputes registered queries per app tx and suppresses unchanged results (identical observable behavior, more compute per tx) |
| error shapes (`type`, `hint`, `original-event` echo) | ✅ | |
| rooms: join/leave/set-presence/refresh-presence | ✅ | cross-node via Postgres |
| `patch-presence` incremental edits | 🟡 | server always sends full `refresh-presence` snapshots (protocol-valid for all client versions; less bandwidth-efficient) |
| `client-broadcast` / `server-broadcast` | ✅ | |
| message batching (JSON array frames) | 🟡 | server sends single frames (clients accept both) |
| SSE fallback transport (`/runtime/sse`) | ❌ | ws-only; SSE used when websockets are blocked |
| sync tables (`start-sync`, `sync-load-batch`, …) | ❌ | experimental client feature |
| streams (`start-stream`, `stream-append`, …) | ❌ | experimental client feature |

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
| link/unlink rules per label | 🟡 | uses legacy default fallback (update on forward entity + view on linked entity); explicit `allow.link` maps not read |
| attr-level (field) rules | ❌ | |
| `request.*` / `rateLimit.*` CEL bindings | ❌ | |
| rule-where query rewriting | ❌ | optimization only; per-entity evaluation gives the same results |
| CEL null-safety (`missing key -> null`) | 🟡 | entity maps pre-populate all schema fields as null; unknown ad-hoc keys still error inside CEL |

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
| storage upload/delete (admin + client routes) + signed download URLs | ✅ | local-disk adapter |
| query_perms_check / transact_perms_check (debugQuery/debugTransact) | ❌ | |
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
