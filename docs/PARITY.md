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
| `refresh-ok` push with computations + attrs | ✅ | result-hash suppression; per-app coalesced refresh batches, identical (query, auth, request.ip/origin) recomputed once across sessions. Fan-out follows legacy's invalidator: only sessions with a topic-stale query are refreshed (triple topics plus a wildcard per changed attrs row, `topics-for-attr-upsert`), except attr inserts / deletes and ident changes (`schema-changes-require-refreshing-sessions?`), which refresh every session; flag flips and inferred-types writes therefore reach only sessions whose queries mention the attr |
| topic-based invalidation narrowing | ✅ | coarse topics per registered query (`instant_core::topics`, QUERY.md §6.2 shapes with result substitution on the entity fetch) matched against the tx's `rust_tx_changes` rows; unresolvable shapes and `.ref(` rules fall back to catch-all; result-hash suppression remains the backstop |
| error shapes (`type`, `hint`, `original-event` echo) | ✅ | full legacy key set incl. null `hint`/`client-event-id` (conformance error matrix); unknown ops are `param-malformed` "Invalid op", pre-init ops `validation-failed` "`init` has not run for this session.", `ex/get-param!` shapes (`Missing parameter: ["app-id"]` / `Malformed parameter: [...]` with `hint.in`) for `app-id`, `room-id`, stream `chunks`/`offset`/`reconnect-token`, `q: null` is "Query can not be null.", a missing `tx-steps` is `validation-failed` for `tx-steps`, a bad `__admin-token` is `record-not-found` (differential step 26) |
| per-op handler timeout and op scheduling (`operation-timed-out`) | ✅ | legacy cancels a handler after `handle-receive-timeout-ms` (5000, session.clj:58, :1137-1207) and answers `operation-timed-out` status 500; same here (`INSTANT_HANDLE_RECEIVE_TIMEOUT_MS`). A session's ops run on legacy's group keys (`scheduler.rs`, session.clj:1463-1553): same-key ops in arrival order, different keys concurrently; while an op waits, a newer `set-presence` replaces it, `append-stream`s merge, and a transact of the same cardinality-one `add-triple`s takes it over (legacy's default-on `combine-transacts?`, every client-event-id answered with the one result); an `add-query` re-reads the tx watermark after registering and recomputes if a tx landed in between. Differential error matrix (`ws/operation-timed-out/transact-blocked`), stress.mjs |
| rooms: join/leave/set-presence/refresh-presence | ✅ | cross-node via Postgres; in-room asserts + `set-presence-ok`/`client-broadcast-ok` acks like legacy; re-joining a room keeps the presence data already set (hazelcast.clj:123-134, the client re-joins on every reconnect; differential step 26) |
| `patch-presence` incremental edits | ✅ | diff-based patches for core > 0.17.5; full snapshots for older clients and fresh joiners |
| `client-broadcast` / `server-broadcast` | ✅ | |
| message batching (JSON array frames) | ✅ | queued messages coalesce into one array frame for core > 0.22.75 (see divergence table) |
| legacy field superset (`processed-isn`, `isn`, `result-meta`, computation metadata, `trace-id`) | ✅ | see "Wire-level divergences" below |
| `skip-attrs` version gating | ✅ | refresh-ok omits `attrs` for core > 0.20.4 unless attrs changed (legacy session.clj:503-533) |
| SSE fallback transport (`/runtime/sse`) | ✅ | sse-init handshake + POST envelope, ops through the same scheduler as the socket (scripts/sse-test.mjs, differential replay step 35) |
| `add-query` `return-type: tree` | ✅ | legacy session.clj:249 / query.clj:139-144: `result` is the admin object tree and `result-meta` carries `{page-info, aggregate}` keyed by top-level form; refresh-ok computations keep the shape. Used by the admin SSE session; a ws client may ask for it too |
| sync tables (`start-sync`, `sync-load-batch`, `sync-update-triples`, `resync-table`, `remove-sync`) | ✅ | admin-only like legacy (session.clj:281-284); trigger-based per-tx change log replaces the WAL feed; cross-session resync + pruned-log forced-restart path (scripts/synctable-test.mjs, scripts/conformance-test.mjs) |
| streams (`start-stream`, `append-stream`, `subscribe-stream`, live tailing, resume) | ✅ | disk-backed bytes + NOTIFY fan-out; $streams perms enforced with the stream row as `data` for view checks (scripts/streams-test.mjs); `reconnect-token` is required (session.clj:767-769, so a client-id can't be taken over), appends need `chunks` + `offset`, "Stream is completed." / "Invalid offset for stream." (app_stream.clj:444-457), an unknown `unsubscribe-stream` is "Stream subscription is missing." (differential step 26); `remove-sync` deletes only a sub this session holds, scoped to its app (session.clj:373-381) |

## InstaQL

| Feature | Status | Notes |
|---|---|---|
| flat + nested queries, forward/reverse links | ✅ | |
| where: eq, dot-paths, `$in`/`in`, `$not`/`$ne`, `$isNull`, `or`, `and` | ✅ | including missing-attr null-prefix expansion; an args map with several operators applies only its first key like legacy `(first v-value)` (instaql.clj:669-675); `$not` on a link needs a uuid ("Expected owner to be a uuid, got ..."); a form on missing attrs / unknown namespace carries no `page-info` / `aggregate` key (differential step 25) |
| `$like` / `$ilike` / `$gt` `$gte` `$lt` `$lte` (typed) | ✅ | index+type guards with legacy error messages; every checked-type attr, indexed or not, validates the query value with legacy's "The data type of `x.y` is `t`, but the query got the value `v` of type `T`." (attr_pat.clj:228-295, :366-372); date strings reject `now`/`today`/`tomorrow`/`yesterday` and unparseable text (a cheap pre-parse stands in for `parse-date-value`) instead of a 500 |
| `$entityIdStartsWith` | ✅ | |
| order by serverCreatedAt / typed attrs, asc/desc, nulls placement | ✅ | `order: {}` is a no-op (instaql.clj:313-315); a row with no triple at all for the order attr (data predating the indexed-null backfill) is left out of an ordered query like legacy, instead of failing the query (differential step 25) |
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
| lookup refs (eid + value position, id-attr lookups) | ✅ | a value-position lookup only resolves existing entities (plus eid lookups earlier in the tx) and is otherwise "The entity for the lookup does not exist." (triple.clj:885-899 `missing-lookup-value`), never a phantom row; lookup namespaces are validated on both positions for non-admin transacts, legacy's pre-processing branch (transaction.clj:532-556, permissioned_transaction.clj:124-140, :683-687); admins fall through to the lookup miss (differential step 25) |
| create/update/upsert modes | ✅ | |
| inline add-attr (schemaless), update-attr (flag rewrite), delete-attr (soft), restore-attr | ✅ | add-attr under an existing `etype.label` is `record-not-unique` "`label` already exists on `etype`" with `hint.record-type` `ident` (legacy `app_ident_uq`, exception.clj:227-240); forward *and* reverse identities are checked against the catalog and the `$` namespaces on add and rename (attr.clj:313-332, :585); `required?` refuses a populated namespace on add ("Can't create attribute ... already have entities", attr.clj:334-350) and an incomplete one on update (attr.clj:533-580); deleting an entity re-validates required links on its referrers (transaction.clj:617-623). | delete-attr soft-deletes like legacy `soft-delete-multi!` (brands names with `{id}_deleted$`, drops index/required, stores `soft_delete_snapshot` in `attrs.metadata`, keeps triples); restore-attr is legacy `restore-multi!` (un-brands, clears the snapshot, leaves the attr un-indexed / not required; unknown or live ids are a no-op); admin-only, users get the attr-scope `permission-denied` (differential step 19-restore-attr) |
| unique constraint errors (`record-not-unique`) | ✅ | |
| required-attr validation | ✅ | see add-attr / update-attr guards above |
| indexed-null backfill | ✅ | |
| checked-data-type validation, oversized-value errors | ✅ | enforced by the schema's check constraints |
| inferred-types tracking | ✅ | `attrs.inferred_types` bitset OR'd per add-triple / deep-merge-triple (patch values, not the merged result) like legacy `insert-attr-inferred-types-cte`; system-catalog attrs seeded string/json like legacy; wire order `number, string, json, boolean` as legacy renders its keyword set (differential harness no longer normalizes `inferred-types`) |

## Permissions

| Feature | Status | Notes |
|---|---|---|
| view/create/update/delete rules, `$default` fallbacks | ✅ | |
| `bind` | ✅ | evaluated as pre-bound variables rather than cel.bind textual expansion; `$default.bind` precedes the etype's binds for every rule and both array and object forms count (rule.clj:100-111, :139-142; differential step 27) |
| `data.ref` / `auth.ref` | ✅ | literal-path prefetch |
| `ruleParams` (query `$$ruleParams` + `rule-params` steps) | ✅ | |
| `$users` default rules (view/update self + linked guests, create true, delete false) | ✅ | `auth.id == data.id \|\| (data.linkedPrimaryUser != null && auth.id == data.linkedPrimaryUser)` (rule.clj:198-210); a guest upgrading with an existing user's email gets `linkedPrimaryUser`, a guest upgrading with a fresh email keeps its id (differential step 18-users-linked-guest) |
| link/unlink rules per label | ✅ | explicit `allow.link`/`allow.unlink` label maps with `linkedData` binding; update+view fallback otherwise |
| attr-level (field) rules | ✅ | `[etype].fields.[field]` view programs filter triples per entity |
| `request.*` / `rateLimit.*` CEL bindings | ✅ | `request.modifiedFields` (labels written to the checked entity, create/update checks only, `id` excluded, retractions ignored), `request.time` (CEL timestamp), `request.ip` (second-to-last `x-forwarded-for` hop, like util/http.clj) and `request.origin` (Origin header) from the ws upgrade / HTTP request; `has(request.ip)` follows proto semantics, unknown fields fail like legacy's `undefined field 'x'`; `ip-override` / `origin-override` on the perms-check debug routes. `rateLimit.<name>.limit(key[, tokens])` charges token buckets configured in `$rateLimits` (greedy / interval refill, bucket identity covers the config); exhaustion is `rate-limited` with `retry-at` / `retry-after` / `remaining-tokens` (status 400 on the socket like every request error, session.clj:1040-1060). Bucket state lives in `rust_rate_limit_buckets` so every node shares it (legacy: in-memory bucket4j per machine). Query caches / refresh dedupe are keyed on ip+origin. Differential steps 20-request-bindings, 21-rate-limits |
| rule-where query rewriting | ❌ | optimization only; per-entity evaluation gives the same results |
| `$isNull: true` inside `or` on an indexed attr | ✅ | folds into `{in [nil]}` like `combine-or-where-conds` (instaql.clj:204-230), i.e. matches the entity's null triple; only rows predating the null backfill would differ. Differential step 30 |
| `mode: create|update` | ✅ | one pre-pass over the pre-tx state like `validate-mode` (transaction.clj:283-358): existence is any triple of the step's etype, lookups are checked as written, every offender is reported in one `tx-step` error (in step order, with the offending steps as `input`), and create-then-update / create-after-delete inside one tx fail like legacy. Differential step 32 |
| tx-step shapes | ✅ | the `::tx-steps` specs (transaction.clj:25-69): fixed arity per op, `opts` nil or a map with `mode` in create/update/upsert, string etypes on `delete-entity` / `rule-params`, map `rule-params`, a value on every triple; `check-for-invalid-entity-ids!`'s friendly message wins for a bad entity id. Differential step 32 |
| `resync-table` subscription checks | ✅ | token hash, admin-ness and user must match the subscription (model/sync_sub.clj:170-195) with legacy's `subscription` validation messages; a missing subscription is `record-not-found: subscription`. Differential step 33 |
| OAuth callback error surfaces, PKCE | ✅ | `error=`, a missing / invalid `state`, a missing or mismatched cookie, an unknown or expired request, a missing code or client are 400 `oauth-error`s; only a failed user-info exchange or a missing `sub` redirects back with `?error=` (runtime/routes.clj:506-601); success is a 302, or the "Finish Sign In" landing page for non-http redirect targets (:434-504); `?test-redirect` renders the test page (:379-432). PKCE follows `verify-pkce!` (auth/oauth.clj:364-415): verifier without challenge / challenge without verifier / unknown method / undecodable S256 challenge each have their message under `app-oauth-code`. scripts/oauth-test.mjs, differential step 34 |
| `coerce_email` | ✅ | legacy's `email/coerce` pattern (util/email.clj:7-21): non-empty dotted local part of RFC atoms, two or more hyphen-safe labels; `@b.com` is malformed |
| admin `link` / `unlink` by lookup | ✅ | a lookup entity gets its `id` triple first (`with-id-attr-for-lookup`, admin/model.clj:95-103), so the entity is created — and its create rule runs — like `update` / `merge` |
| `$files` / `$streams` rule `auth` | ✅ | bound through the same `AuthCelMap` path, so `auth.ref('$user...')` resolves (coordinator.clj:19-38, app_stream.clj:38-90) |
| rule result semantics | ✅ | Clojure truthiness: only `false`/`null` deny, a string or number passes (exception.clj:291-297); an evaluation error is `permission-evaluation-failed` "Could not evaluate permission rule for `etype.action`. You may have a typo. ..." with `hint.rule` (exception.clj:299-323) rather than a silent deny (differential step 27) |
| `$users.allow.create` on signup, `$users` create guard | ✅ | legacy `assert-signup!` (app_user.clj:51-85): when that exact rule path exists, magic-code signups (checked before the code is consumed, magic_code_auth.clj:277-288), guest sign-ins and OAuth signups must pass it with the prospective `{id, email}` as `data`/`auth`; admin flows skip it. Non-admin transacts can't create `$users` rows ("$users is a system entity. You aren't allowed to create this directly.", permissioned_transaction.clj:589-596). Differential step 27 |
| CEL extension functions (`lowerAscii`, `upperAscii`, `charAt`, `indexOf`, `lastIndexOf`, `replace`, `split`, `substring`, `trim`, `join`, `math.*`), `cel.bind`, `getTime()`, `timestamp(int|string)` | ✅ | `instant_core::cel_ext` registers cel-java's strings + math extensions (greatest/least/abs/sign/ceil/floor/round/trunc/isInf/isNaN/isFinite/bit ops) and Instant's overloads (cel.clj:387-414, :439-454, :479-488): `math.round` rounds half to even like cel-java, `getTime()` is epoch milliseconds, `timestamp(int)` reads epoch milliseconds, `timestamp(string)` goes through the same lenient date parser as the `date` checked type (the call is renamed in the AST before evaluation because the crate's strict RFC 3339 overload would otherwise shadow it); `cel.bind(var, init, body)` (the bindings extension, cel.clj:489-491) is expanded into the same empty-range comprehension cel-java's macro produces. The newer cel-java string functions (`reverse`, `strings.quote`, `format`) are compile-time `undeclared reference`s on the legacy server (cel-java 0.11) and here, reported one issue per reference like cel-java. Differential step 29 |
| `data.ref(...)` path anchoring | ✅ | like legacy `build-query` (cel.clj:88-117) the walk is anchored on the root entity's `id` triple and a terminal `id` is read as a triple, so an entity that only exists through a link (no `id` triple) is invisible to `ref` in both positions. Differential step 31 |
| CEL compile-time references | ✅ | each action's compiler declares its variables (cel.clj:459-497: `newData` from create / update on, `linkedData` for link / unlink, `actions` for link only); an identifier or function the compiler doesn't know is `validation-failed` for `permission` ("undeclared reference to 'x' (in container '')") with the rule's path as `input`, raised when the program is loaded — for link / unlink programs on every ref step — and by the rules push validation. Differential steps 29, 31 |
| `data.ref(...)` in `update` / `delete` / `link` / `unlink` rules | ✅ | every ref path those rules can read is resolved before the steps run and kept on the pre-tx snapshot (legacy pre-checks, permissioned_transaction.clj:697-715); `create` and linked-`view` checks read the post-tx graph like legacy's post-create checks. Differential step 31 |
| link-rule bindings (`actions`, `linkedData.ref`, link rules on create) | ✅ | `link` programs see `actions` (`{data: create|update, linkedData: create|update}`), `unlink` programs don't (cel.clj:459-497); `linkedData.ref(...)` resolves (pre-tx for an existing linked entity); an entity brought into being by a link step alone runs the `link` rule with `actions.data == "create"` instead of `create` (permissioned_transaction.clj:528-560). Differential step 31 |
| `view` rules with a `fields` projection | ✅ | `PermsFilter::filter_with_forms` re-fetches a projected entity before its `view` / field rules run (instaql.clj:1956-2007 `preload-entity-maps`); the projected result is still what the client gets. Differential step 30 |
| `attrs` create permission for schemaless add-attr | ✅ | every inline `add-attr` step of a non-admin transact is checked post-tx against `attrs.allow.create` → `attrs.allow.$default` → `$default.allow.create` → `$default.allow.$default` (else allow) with the attr map as `data` (permissioned_transaction.clj:519-527, rule.clj:278-290); denial is `["attrs" "attr"]`. Differential step 32 |
| CEL null-safety (`missing key -> null`) | ✅ | every key a rule statically mentions (select fields + string literals) is pre-inserted as null into `data`/`newData`/`auth`/`ruleParams`/`linkedData` and their nested maps; `has()`/`in` answer true like legacy; anonymous `auth` is an empty map; only keys computed at runtime (`data[someVar]`) still error → deny |

## Auth

| Feature | Status | Notes |
|---|---|---|
| magic codes (send/verify, expiry, one-time) | ✅ | delivery is a provider seam (`email.rs`): `EMAIL_PROVIDER=log` (default) prints the code to the server log, `EMAIL_PROVIDER=cloudflare` sends through the Cloudflare Email Service API; legacy uses Postmark, so a Postmark provider is one env-selected arm away |
| guest sign-in + guest→user linking | ✅ | |
| refresh tokens (sha256-hashed, no expiry) | ✅ | |
| signout | ✅ | |
| OAuth redirect flow (start/callback/token, PKCE, state+cookie hashes) | ✅ | mock-OIDC integration test (`scripts/oauth-test.mjs`), single-server: no differential step, since legacy fetches and vets the provider's discovery document through its SSRF guard, which rejects a container-local mock provider; Google = discovery OIDC; `/oauth/token` and `/oauth/id_token` assert the request `Origin` against the authorized origins like legacy (runtime/routes.clj:622-623, :670-671); `/oauth/start` and `/:app_id/oauth/token` are rate limited (routes.clj:754-768); a guest's `refresh_token` on either path upgrades the guest in place or links it via `linkedPrimaryUser` (routes.clj:305-333, :625-637, :690-696) |
| `signInWithIdToken` (JWKS verification, nonce, audience) | ✅ | Google nonce skip honored |
| `.well-known/openid-configuration` | ✅ | |
| `POST /runtime/framework/query` (SSR `FrameworkClient`) | ✅ | legacy `framework-query-triples` (runtime/routes.clj:728-743): optional bearer refresh token, `app-id` header, body `{query, versions}` → `{result, attrs}` (differential step 28) |
| `extra_fields` on signup (`validate-extra-fields!`) | ✅ | `extra-fields` (`extra_fields` on the OAuth routes) is validated against the `$users` schema (unknown / system field → `validation-failed` for `extra-fields`), bound into `data` / `newData` / `auth` for the `$users.allow.create` check, denied when supplied without an explicit create rule, and written with the new row; admin flows validate without the rule check (app_user.clj:22-109; magic code, guest, OAuth, `/admin/refresh_tokens`, `/admin/sign_in_guest`) |
| authorized redirect origins (generic/custom-scheme/netlify/vercel) | ✅ | nothing is allowed by default: legacy's localhost / `exp://` defaults apply only to shared-credential clients (app_authorized_redirect_origin.clj:77-116), which this server doesn't have; `scripts/create-oauth-client.sh` takes an origin host as its optional 7th argument |
| shared oauth credentials / Apple secret-JWT / GitHub non-OIDC client | ❌ | bring your own provider credentials; generic OIDC only |
| custom email templates/senders | 🟡 | `app_email_templates` (`magic-code` type, `{code}` `{app_title}` `{user_email}` `{expiration}` placeholders) and `app_email_senders` are honored on the send path like legacy `magic_code_auth.clj`, and the `/dash/apps/:id/email_templates*` + `send-test-email` routes manage them; a custom sender is used only when its row is marked verified, and the Postmark sender-verification routes that set that flag are hosted-only (out-of-scope.json), so a self-hosted operator verifies a sender by setting the row directly |

## Admin API

| Feature | Status | Notes |
|---|---|---|
| /admin/query (object tree, `inference?` singular links) | ✅ | |
| /admin/transact (create/update/merge/link/unlink/delete/ruleParams, lookup strings, ref lookups, attr auto-create, `throw-on-missing-attrs?`) | ✅ | `lookup("owner.id", <uuid>)` names the unique forward link `<etype>.owner` (legacy `extract-lookup`); a missing one is auto-created as a unique cardinality-one link, `throw-on-missing-attrs?` reports `<etype>.owner`; `x.name` / `x.id.id` / non-unique links fail with legacy's `lookup` validation messages (differential dash step 24-admin-ref-lookups) |
| impersonation headers (`as-token`/`as-email`/`as-guest`) | ✅ | honored only where legacy calls `get-perms!` (`/admin/query`, `/admin/transact`, the SSE routes, storage upload/delete); every other `/admin/*` route requires the bearer admin token and ignores them (`req->app-id-authed!`, admin/routes.clj:396-772), the perms-check routes need both (routes.clj:220-221, :325-326). Differential step 28 |
| refresh_tokens / sign_out / users GET+DELETE | ✅ | a miss on `/admin/users` is `{"user": null}` / `{"deleted": null}` (admin/routes.clj:490-499) |
| magic_code / send_magic_code / verify_magic_code / sign_in_guest | ✅ | `/admin/send_magic_code` delivers the email (admin/routes.clj:506-511); `/admin/magic_code` returns the code |
| unknown routes / wrong methods | 🟡 | JSON 404 `{"message": "Oops! We couldn't match this route."}` like core.clj:189-190 (the CLI parses error bodies); the self-hosted legacy image itself answers these with a 200 non-JSON body (differential step 28, allow-listed) |
| `/admin/rooms/presence` shape | ✅ | `room-type` and `room-id` are required (`param-missing` with legacy's `["params" ...]` names) and each peer's `{id}` user is replaced with its current `$users` entity (admin/routes.clj:739-765); entries carry `instance-id` (the node id, ephemeral.clj:280-286). Differential step 33 |
| rooms/presence | ✅ | |
| storage: `PUT /admin/storage/upload`, `DELETE /admin/storage/files`, `POST /admin/storage/files/delete`, `GET /admin/storage/files`, `/admin/storage/signed-{upload,download}-url`, client `PUT /storage/upload`, `DELETE /storage/files`, `GET /storage/signed-download-url`, `POST /storage/signed-upload-url`, `PUT /storage/:id/consume-upload-url` | ✅ | `$files` rows carry legacy's S3 metadata defaults (`content-type: application/octet-stream`, `content-disposition: inline`), keep `location-id` unless a `fields` projection drops it, and get the synthetic `url`; blank `content-disposition` is `param-malformed`, `create`/`delete` rules apply to impersonated admin calls, errors mirror legacy's `["path"]`/`["params" "filename"]` param names, `has-storage-permission?`, `app-upload-url` shapes. Backends: Postgres blobs by default (multi-node correct; scripts/multinode-storage-test.mjs), `disk`, and `s3` (issue #9: any S3-compatible store via hand-rolled SigV4, legacy's `app-id/bin/location-id` key layout with the Java-hashCode bin so a legacy bucket serves as-is, object content-type/disposition metadata, presigned `$files.url` byte-compatible with legacy's `presign-s3-url` — day-bucketed signing instant, 7-day expiry, `response-cache-control` — or `S3_PRESIGN=0` to proxy; live stream bytes spool through Postgres and move to the bucket when the stream is done). Download URL text differs by design: legacy always presigns S3, this server presigns only on the `s3` backend and otherwise serves `/storage/serve/...` (HMAC-signed, day-bucketed, same `Cache-Control`). Legacy's deprecated `GET /admin/storage/files` 500s on the self-hosted image; this server returns the documented list. Verified: scripts/differential/storage.mjs (8 steps vs live legacy in both presign and proxy modes), scripts/s3-storage-test.mjs (bucket contents inspected with an independent signer) |
| query_perms_check / transact_perms_check (debugQuery/debugTransact) | ✅ | check-results with programs; dry-run/commit semantics |
| `/admin/subscribe-query`, `/admin/sse`, `/admin/sse/push` (SSE transports, issue #8) | ✅ | `@instantdb/admin` `subscribeQuery` and `db.streams`: an admin-authed reactive session over `text/event-stream` (`retry: 500` hint, `sse-init`, then `add-query-ok` with the object tree + `result-meta` for subscribe-query, `refresh-ok` computations in the same shape, `error` frames); the generic session takes every socket op via the push envelope (machine_id / session_id / sse_token / messages). Impersonation headers work like every /admin route (`as-token` sessions see rule-filtered trees). Sessions are pre-initialized like legacy `admin-init!` (no `init`, no feature flags: refresh-ok always carries attrs, frames are never batched). Push errors mirror legacy (`session-missing` with its printed-map message, `member-missing`, `param-missing` with `possible-ins`). Multi-node: the session lives on the node that holds the stream and pushes must reach it (session affinity), where legacy forwards over hazelcast. Differential replay steps 23-24 + dash step 25, scripts/admin-sdk-test.mjs (real SDK subscribeQuery + streams) |
| `/dash/apps/:id/schema/pull`, `schema/steps/apply`, `schema/push/{plan,apply}` (instant-cli push/pull schema) | ✅ | `{schema: {blobs, refs}, attrs, app-title}` incl. legacy's Clojure-printed ref keys; add/update/delete-attr steps transact, `index`/`unique`/`required`/`check-data-type` (+ `remove-*`) become indexing jobs; server-side planning (`schemas->ops`, plan errors) ported to `instant_core::schema`; pulled `instant.schema.ts` is byte-identical to legacy's (scripts/cli-test.mjs, scripts/differential/dash.mjs) |
| indexing jobs (`/dash/apps/:id/indexing-jobs/*`) | ✅ | port of legacy's stage machine (issue #5): `indexing?` / `setting-unique?` / `checking-data-type?` on the attr wire while a job runs, query planning treats such attrs as unindexed / non-unique / untyped (with legacy's order-by and comparator messages), triples rewritten in `INSTANT_INDEXING_BATCH_SIZE` batches resumed from a cursor in the job row, `work_estimate` / `work_completed` progress, legacy statuses, stages and error codes (`triple-not-unique-error` + `invalid_unique_value`, `triple-too-large-error` + sample, `invalid-triple-error` samples, `missing-required-error` + `error_data`); jobs are released between steps so any node continues them and orphaned ones are swept up (legacy only warns about stuck jobs). A raw `update-attr` tx-step still rewrites flags synchronously like legacy `update-multi!`. Verified by scripts/differential/dash.mjs (36 steps vs live legacy) + scripts/indexing-jobs-test.mjs |
| `/dash/apps/:id/perms/pull`, `POST /dash/apps/:id/rules` (instant-cli push/pull perms) | ✅ | full rule validation port (binds, reserved namespaces, `$users.delete`, CEL compile errors with the same ANTLR messages, field rules, `$rateLimits` configs); version bump + `rules: null` on unchanged code like legacy |
| `/dash/cli/version`, `/dash/cli/auth/{register,claim,check,void}` | ✅ | `instant-cli login`: `register` hands out a ticket + secret (`instant_cli_logins`, secret stored as sha256), the dashboard login (`/dash/oauth/start?ticket=` or the magic-code flow) claims or voids the ticket for the user, `check` polls with the secret and answers legacy's `issue` codes (`waiting-for-user`, `user-voided-request`, `user-already-claimed`, 2-minute expiry) before handing out a refresh token. Differential dash step 40, `scripts/dash-login-test.mjs` |
| dashboard-route auth | ✅ | app admin token (the CLI's `INSTANT_APP_ADMIN_TOKEN`) with legacy's admin-token-mismatch error; dashboard refresh tokens for creators/members with the least-privilege roles; personal access tokens (`per_`) and scoped platform access tokens (`pat_`) through the superadmin path (`req->app-accepting-superadmin-or-ref-token!` with each route's scope, missing-scope `permission-denied`) |
| `GET /admin/schema`, `GET /admin/soft_deleted_attrs` | ✅ | `attrs->schema` with `-`-joined ref keys (admin/routes.clj:754-761); soft-deleted attrs in wire shape with `grace-period-days` (:771-776). Differential dash step 33 |
| dashboard app management (`rename`, `clear`, `status`, `tokens`, `set-magic-code-expiry`, `rule-versions`, `soft_deleted_attrs`, `test_users`, `stats`, `storage/upload`, `storage/files/delete`, `send-test-email`) | ✅ | `routes/dash_manage.rs`: each route uses the legacy auth helper it has (`req->app-and-user!` = refresh token only with the role, `req->app-accepting-superadmin-or-ref-token!` = admin token too), the same param order (expiry / filenames parsed before auth), legacy's raw statement results (`{"next.jdbc/update-count": 1}`) where legacy returns them, `clear` soft-deletes every user attr in one transaction and resets rules, `status` flips the read/write gates. `stats` counts this node's sessions (legacy sums cached per-machine reports). Differential dash steps 33, 35, 36 |
| dashboard account routes (`profiles`, `signout`, `check-admin`, `auth/send_magic_code`, `auth/verify_magic_code`, `personal_access_tokens`) | ✅ | dashboard login via `instant_user_magic_codes` (10-minute expiry, `dashboard-login-disabled` user flag, signup policy `INSTANT_DASHBOARD_SIGNUP_MODE` for legacy's flag), `check-admin` passes for `INSTANT_SUPERUSER_EMAIL`; personal access tokens are `per_` + 32 random bytes hex stored as `sha256` lookup keys, and the create response is the inserted row (`id`, `user_id`, `name`, `created_at`) plus the plaintext `token`, legacy's `:return-keys` row through `format-token-for-api`. Differential dash step 34 |
| teams (`invite/send`, `invite/revoke`, `/dash/invites/accept`, `/dash/invites/decline`, `members/update`, `members/remove` for apps and orgs, `orgs/:id/rename`, `transfer_to_org`) | ✅ | `routes/dash_members.rs`: legacy's role gates (`assert-valid-member-role!` bare-string error, the two least-privilege checks on update, collaborator-level remove), invite upsert per (fk, email) and legacy's invite email (`team-member-invite-email`) through the configured `EMAIL_PROVIDER`, accept after the 3-day window is `record-not-found` like legacy (`accept-by-id!` asserts the `RETURNING` row of its windowed UPDATE), `creator` invites change the app's creator, the org_members triggers' `remove_last_org_owner` / `modify_org_id_on_org_member` raises and `orgs_title_check` become legacy's `validation-failed` messages, transfer runs legacy's CTE (paid-org member dedupe) with `credit: null` since there is no billing. Differential dash step 37 |
| `POST /dash/apps/ephemeral/:id/status`, `GET /dash/apps/get_a_db/:id`, `POST /dash/apps/get_a_db` | ✅ | ephemeral-creator + admin-token gated status toggle (ephemeral_app.clj:104-119); the unauthenticated get-a-db lookup (get_a_db.clj:60-67); creation by the get-a-db service user's PAT (`apps-write`, `permission-denied: not get-a-db-user?` for anyone else) with optional validated rules and a checked-types schema plan (get_a_db.clj:26-58). `scripts/dash-login-test.mjs` seeds the service user like production |
| dashboard Google login (`GET /dash/oauth/start`, `GET /dash/oauth/callback`, `POST /dash/oauth/token`) | ✅ | `routes/dash_login.rs` on `instant_oauth_redirects` / `instant_oauth_codes` (sha256 lookup keys): legacy's param order and `URLEncoder` encoding on the Google redirect, the `__session` cookie for `/dash/oauth`, the callback's side-effect order and error precedence (dash/routes.clj:990-1073), `upsert-user-from-google-sub!` (email or sub brought up to date, several matches fail), 10-minute redirects / 5-minute one-use codes, the signup policy, `dashboard-login-disabled`. Client from `INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_ID` / `_SECRET` (legacy's names; unset = legacy's unconfigured client); `_AUTH_URL` / `_TOKEN_URL` override Google for tests. `redirect_to_dev` goes to the same `INSTANT_DASHBOARD_URL` since both origins read it first. Differential dash step 40 (start + error paths + token), `scripts/dash-login-test.mjs` (mock Google round trip) |
| `POST /dash/apps/:id/track-import`, `GET /dash/stats/active_sessions` | ✅ | track-import is legacy's posthog event + `{ok: true}` (no auth); active sessions counts this node's sessions and their subscribed queries where legacy sums cached per-machine reports (`GET /dash/session_counts` is legacy's internal websocket feed for that and is not served) |
| platform API (`/superadmin/apps[/:id[/schema[/push/{plan,apply}]|/perms|/transfers/{send,revoke}]]`, `/superadmin/orgs[/:id/apps]`) | ✅ | `routes/superadmin.rs`: `@instantdb/platform`'s PlatformApi; `?include=schema,perms`; create with `perms` (validated as `perms`) and `schema`; transfers are PAT-only (legacy's unused `apps-transfer` scope) and answer `{id: null}` like legacy's row-less upsert; personal apps need their owner to delete. Differential dash step 38 |
| platform OAuth provider (`/platform/oauth/{start,claim,grant,deny,token,token-info,revoke}`) and OAuth-app management (`/dash/apps/:id/oauth-apps*`, `oauth-app-clients*`, `oauth-app-client-secrets/:id`, `/dash/user/oauth_apps[/revoke_access]`) | ✅ | `routes/platform_oauth.rs`: the HTML error page and `__session=instantdb_<uuid>` cookie on `start` (`INSTANT_DASHBOARD_URL` is the consent screen), claim/grant/deny with the grant token and cookie checks, PKCE and confidential token exchange (`scopes` vs `scope` key quirk kept), 10-minute redirects/codes, 2-week access tokens, at most 5 refresh tokens per client+user, secrets/codes/tokens stored as sha256 lookup keys, logos in legacy's 4-byte-mime encoding. Differential dash step 38 (full round trip on both servers) |
| webhooks (`/dash/apps/:id/webhooks*`, `/webhooks/payload/*`, `/.well-known/webhooks/jwks.json`) | ✅ | `webhooks.rs` + `routes/webhooks.rs` on the legacy tables (migration 109): create/update/enable/disable/delete with legacy's validations (https + public host, namespaces → id attrs, ≥1 table/action, 100 active, duplicate detection), events with cursors, event by ISN, resend, the EdDSA-signed delivery (`Instant-Signature: t=,kid=,v1=`, payload JWT, `Idempotency-Key`), Stripe's retry schedule, 410 disabling. Events are produced on the transaction path instead of the WAL feed (see the wire-level table); the payload rebuilds legacy's records from a per-tx snapshot. Delivery pins the connection to the addresses the URL validation vetted (legacy wires its filtering resolver into the client; a DNS answer that changes after validation can't reach a private network), reads at most 256 chars of the reply, runs retries concurrently under a one-minute batch bound, and frees stuck events from a ticker of its own. Differential dash step 39, scripts/webhooks-test.mjs (local receiver, signature verified against the JWK set) |
| CLI app-management / OAuth-config / email / org routes (`GET /dash`, `GET /dash/me`, `POST /dash/apps`, `GET|DELETE /dash/apps/:id`, `/dash/apps/ephemeral[/:id[/claim]]`, `/dash/apps/:id/claim`, `/dash/apps/:id/auth`, `oauth_service_providers`, `oauth_clients[/:id]`, `authorized_redirect_origins[/:id]`, `email_status`, `email_templates[/:id]`, `/dash/default-email-template`, `/dash/orgs[/:id]`) | ✅ | `routes/dash_apps.rs`: routes without an app id take a dashboard refresh token (`req->auth-user!`), per-app routes the admin token or a member token with legacy's least-privilege roles (`get-app-with-role!`); providers / clients live in the `$oauthProviders` / `$oauthClients` triples with legacy's key translation (client secrets stored as given, like `create-oauth-client.sh`), origins in `app_authorized_redirect_origins` with the per-service param validation, templates in `app_email_templates` (a `sender-email` is recorded but never Postmark-verified, so the default sender keeps delivering); ephemeral apps belong to the seeded ephemeral creator and expire after 14 days; shared Instant OAuth credentials and `sender-verification` are hosted-service features and answer `record-not-found`. Differential dash steps 26-32 |
| `POST /dash/apps/:id/indexing-jobs` validation, `invalid-attr-state-error` | ✅ | direct creation validates like legacy (dash/routes.clj:1875-1906: unknown job type → `validation-failed` for `job-type`, attr must belong to the app → `record-not-found: attrs`, `checked-data-type` read only for check-data-type), stores no group, and answers `job->client-format`; a guarded `update-attr-done` (or abort) that matches no row errors the job with `invalid-attr-state-error` (indexing_jobs.clj:469-486). Differential dash step 31 |

## Operations

| Feature | Status | Notes |
|---|---|---|
| stateless horizontal scaling | ✅ | pg LISTEN/NOTIFY coordination; multinode test |
| legacy Postgres schema, migrations replay on stock PG | ✅ | tested on Postgres 18 |
| deterministic system-catalog UUIDs | ✅ | verified against hardcoded legacy values |
| app status gates (read-only / disabled) | ✅ | read-only rejects writes (`app-read-only`), disabled rejects reads too (`app-disabled`), legacy messages; an `apps.status` flip (dashboard, psql) pushes `app-status-changed {status}` to every live session of the app on every node (row trigger + NOTIFY standing in for legacy's WAL-fed cache_evict), and a per-app status cache with a TTL safety net backs the gates (differential step 17-app-status) |
| security hardening from the 2026-09-04 audit (scripts/security-test.mjs, scripts/oauth-test.mjs, crates/instant-core/tests/audit_test.rs) | ✅ | `/storage/serve` URLs are HMAC-SHA256 signed, compared in constant time, never dated in the future, and served with `Content-Security-Policy: sandbox` + `nosniff` (legacy serves blobs from the S3 origin, so uploader-chosen `text/html; inline` can't script against the API host here either); `/dash/*` bearer parsing requires the `Bearer ` prefix; JWT algorithms must be listed in the provider's discovery document (auth/oauth.clj:223-224); `$files.path` values under `$stream/` are rejected for everyone (permissioned_transaction.clj:72-79) |
| rate limiting | ✅ | per-app + per-email token buckets (issue #1, `rate_limit.rs`, per node like legacy bucket4j; `INSTANT_RATE_LIMITS=off` disables) and rule-level `rateLimit.*` buckets shared through Postgres (issue #10) |
| backups/restore tooling, attr sketches | ❌ | ops tooling of the hosted service |
| hosted-service dashboard routes: billing / Stripe (`checkout_session`, `portal_session`, `billing`, `/dash/stripe/*`), backups and restores (`/dash/apps/:id/backups*`, `restore*`), sunset stages, Postmark sender verification (`sender-verification*`), `/dash/session_counts` (internal websocket feed), `/dash/admin/*` overview and top-app reports, `ws_playground`, `/dash/apps/:id/track-*` posthog analytics beyond `track-import` | ❌ | These exist only for instantdb.com's operators and paid plans (Stripe, S3 backup buckets, Postmark, posthog, CloudWatch); a self-hosted server has no counterpart, so they are not served. Everything else under `/dash`, `/superadmin`, `/platform` and `/admin` is mounted; `scripts/differential/out-of-scope.json` names these items with a reason each, and the coverage report counts the rest of `surface.json` against what a differential run exercised |

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
| `result-meta` | populated only for the `tree` return-type (admin SSE, reactive/query.clj:143-144) | same: `{page-info, aggregate}` for `tree`, `null` for join-rows | the admin SDK reads `result-meta.page-info` (`admin/src/subscribe.ts:312-327`), compared by differential step 23; ws clients never read it (`Reactor.js:665-700`) |
| `trace-id` on every frame | OTel trace id of the handling span (reactive/store.clj:1601) | random 32-hex id per frame (no tracing backend) | only concatenated into debug logs (`Reactor.js:998-1002`) |
| `init-ok.auth` contents | full app + user + creator rows | trimmed `{app: {id, title}, user, admin?}` | client reads only `attrs`, `session-id`, `app-status` from init-ok (`Reactor.js:644-660`) |
| `init-ok` `server-hostname`/`server-port` | present in dev mode only (reactive/session.clj:192-196) | never sent | dev-only tooling keys; unknown keys ignored |
| query result node tree | one node per datalog pattern with `child-nodes` nesting | single node, all triples in one join-row | client flattens all join-rows via `extractTriples` and re-runs InstaQL locally (`model/instaqlResult.js:1-25`, `Reactor.js:673-680`); `page-info`/`aggregate` stay on `result[0].data` as required (`Reactor.js:671-672`) |
| `processed-tx-id` before any tx | `null` (no store entry yet) | `0` | compared with `>=` against pending mutation tx-ids (`Reactor.js:1676-1691`); `0 >= n` and `null` behave identically for an empty app |
| `update-attr` without `on-delete` | legacy's update set-list assigns `on-delete` unconditionally (attr.clj:606-607), so a raw `update-attr` that omits it silently clears cascade config | cascade config is kept unless the step names `on-delete` | the CLI always sends the full attr; wiping a cascade on a flag flip is the legacy defect, so this server keeps it |
| `init` rate limiting | `:init` / `:sse-init` are exempt from the per-app ws bucket, which is keyed by the session's established app (session.clj:974-984) | same: `init` is exempt and the bucket is keyed by the session's app, never by the frame's `app-id` | — |
| `permission-evaluation-failed` cause text | CEL message for `show-cel-errors?` sessions (plain admin HTTP), "You may have a typo" otherwise | always "You may have a typo" (the CEL detail is logged at debug) | plain admin calls never evaluate rules, so no legacy caller sees the CEL text either |
| `timestamp(string)` that no date format accepts | the raw parse exception escapes as a 500 "Yikes, something broke on our end!" | 400 `permission-evaluation-failed` like any other rule evaluation error | the legacy reply is an uncaught exception, not a wire contract; the client surfaces both as an event error |
| unknown where operator | `validation-failed` for `coerced-query`: the hint's input is legacy's internal coerced form and `expected` a clojure.spec predicate name (instaql.clj:563) | `validation-failed` for `query` naming the operator | no client reads the coerced form; every other query validation error carries the legacy `input` (whole query) and `in` path (error matrix) |
| `permission-evaluation-failed` `hint.error.type` | the cel-java `CelErrorCode` name (`BAD_FORMAT`, `ATTRIBUTE_NOT_FOUND`, `DIVIDE_BY_ZERO`, ...) | the same names, approximated from the cel crate's error (`cel_error_code`) | conversion failures map to `BAD_FORMAT` exactly; the rarer codes are best-effort |
| ws validation hints echoing the event | `hint.input` is the whole event including grouped-queue bookkeeping keys (`instant.grouped-queue/put-at`, `total-delay-ms`, `ws-ping-latency-ms`) | the event plus those keys with `put-at` = now and the delays 0 | byte-identical shape; the values are scheduler internals |
| sync-table attr set | derived from the query when the subscription starts (or resyncs) and kept with the session, so an attr added to the etype later is not synced until a resync | same (`SyncSub.attr_ids` frozen at start-sync / resync) | — |
| `permission-denied` hint `input` | `[etype scope]` — `["posts" "object"]` for entity checks, `["attrs" "attr"]` for attr checks (`run-checks!`, permissioned_transaction.clj:613-631); `["$users" "create"]` on signup; `["$files" action]` / `["$streams" action]` with `has-storage-permission?` / `has-streams-permission?` | same | — |
| `debugQuery` / `debugTransact` check-results | `bindings` (`data`, `new-data`, `linked-data`, `linked-etype`, `actions`, `rule-params`, `modified-fields`) per transact check; query checks keyed by `[etype id label]` with `rule-wheres` derived from convertible view rules | `bindings` carried on every transact check; query checks carry `label: null` (entity-level) and `rule-wheres: {}` because rules are evaluated per entity, never rewritten into where clauses | the admin SDK's debug helpers print these maps; nothing pattern-matches `rule-wheres` |
| batching scope | only server-broadcast fan-out is batched, 500/frame, core > 0.22.75 (reactive/session.clj:747-757) | any queued frames coalesce (≤100/frame) for core > 0.22.75 | array frames are handled for every op (`Reactor.js:1798-1804`); single-vs-array framing is transport-level |
| `stream-append` payload | file URLs for flushed segments + inline `content` for the tail | always inline `content` | reader consumes `files` (if any) then `content` (`Stream.ts:1077-1084`); bytes delivered are identical |
| `client-broadcast-ok` payload | includes fanned-out envelope | same | no client handler for the op at all (unknown ops ignored, `Reactor.js:932-934`) |
| refresh-ok with zero changed computations | old (≤0.20.4) clients still get an empty `refresh-ok` whenever their queries were stale (session.clj:523) | sent, with `attrs`, only when the schema changed (legacy refreshes every session then, so a non-skip-attrs client's attrs stay current; verified by the differential final attrs state); not sent for a merely stale query whose result is unchanged | an empty `computations` array with unchanged `attrs` is a no-op client-side (`Reactor.js:733-814`) |
| rate limiting (`rate-limited` errors) | per-app limits are flag-driven (reactive/session.clj:983-987); `rateLimit.*` rule buckets are in-memory bucket4j per machine | per-app limits always on (sized in `rate_limit.rs`, `INSTANT_RATE_LIMITS=off` to disable); rule buckets are Postgres rows shared by every node | same error type, message and hint fields; the client has no special handling for `rate-limited`, it surfaces like any other event error |
| error `hint` detail | spec explain data / `input` echo / `expected`+`in` paths per error (util/exception.clj) | `{data-type, errors: [{message}]}` (+ `record-type` where applicable) | the client treats `hint` as opaque debugging JSON; the only pattern-matched key is `hint.record-type` (`Reactor.js:1020-1029`), which matches byte-for-byte |
| `add-query-ok.processed-tx-id` freshness | the invalidator's processed watermark, which can lag the latest confirmed tx | exact `max(tx-id)` at query time | client compares with `>=` to GC optimistic mutations (`Reactor.js:1676-1691`); our value is never ahead of what the result reflects, so behavior is identical with earlier GC |
| admin `delete` by ref lookup (`lookup("owner.id", <uuid>)`) | silently dropped: `resolve-lookups-for-delete-entity` keeps only deletes whose lookup resolved, and the ref-attr lookup resolves to nothing on the live server (transaction.clj:229-250, :360-378) | the doc is deleted (one resolver for every op) | admin SDK reads only `tx-id`; legacy's behavior loses the caller's intent, so this server keeps the delete (differential dash step 24, allowlisted) |
| invite revoke scope | `reject-by-id-and-foreign-key` formats the id-only query (member_invites.clj:166-172), so an app admin can revoke any pending invite by id | the revoke is scoped to the app / org in the path | the legacy behavior is a tenant-isolation defect, not a contract; the dashboard only ever revokes its own app's invites |
| webhook event production | the logical-replication feed: every WAL record's attr ids are bloom-matched against `webhooks.topics`, then `webhook-matches?` inspects the triple changes; the payload is rebuilt from the stored WAL record (`history`) | inside each transaction, before commit: an entity of a webhook namespace produces `create` when its id-attr triple was inserted, `update` when it was written again, `delete` when it was deleted (the same signal `webhook-matches?` reads off the WAL: ref / value attrs alone, i.e. link / unlink steps and the referrers of a deleted entity, produce nothing); a per-tx snapshot (`rust_webhook_history`: cardinality-one triples as this tx leaves them + the tx's triple changes) and the `webhook_events` rows commit with the data; the ISN is `0/<tx id as lsn>`. Active webhooks are cached per node and evicted by a `webhooks` row trigger's NOTIFY | same events for the same transactions (an "update" needs an entity write, which InstaML always accompanies with the id triple legacy keys on); ISN values differ (tx ids vs Postgres LSNs), so cursors and payload URLs are only comparable in shape |
| `$entityId` where key | present in `where-value-valid-keys?` (a dashboard hack, instaql.clj:69-75) but with no spec entry and no SQL branch, so a `{$entityId: v}` where value passes validation and yields a degenerate query result rather than an error | rejected as an unsupported where operator | undefined legacy behavior with no client that relies on it; the real, spec’d `$entityIdStartsWith` operator is implemented and covered by the fuzz layer |
| `remove-sync` without `keep-subscription` | drops the in-memory sync query and calls `sync-sub-model/delete!` with `(:sync/subscription-id sync-ent)`, an attribute nothing sets, so the `sync_subs` row is never deleted (reactive/session.clj:373-381) | the same: the row is kept, `keep-subscription` changes nothing | a client that removes and later `resync-table`s the same subscription id resumes on both servers (differential replay steps 12 and 33) |
| transaction with more than ~32k distinct lookup refs | `parameter-limit-exceeded`: lookups resolve through one `VALUES` list of two bind parameters each (transaction.clj resolve-lookups), past pgjdbc's 65,535 | the transaction runs | an implementation limit, not a contract; `$in` sets are one array parameter on both servers (error matrix `query-in-70k-values`) |
| `/dash/cli/auth/claim` on a ticket another user claimed | the ticket is re-pointed at the caller (instant_cli_login.clj `claim!`) | refused: `validation-failed`, issue `user-already-claimed`; a same-user re-claim still succeeds | hardening (issue #38); the CLI only polls `check`, which signs in the first claimant (differential dash step 40, allowlisted) |
| `/platform/oauth/deny` checks | deletes the redirect first, then checks the cookie; the grant token is never compared (oauth_apps/routes.clj:318-338) | grant token (`record-not-found`, like `grant`) and cookie checked before the redirect is deleted | hardening (issue #38): a bad deny can no longer burn the user's pending consent (differential dash step 38, allowlisted) |
| `join-room-error` op | defined in the client (`Reactor.js:921-927`) but never emitted by the legacy server either | never emitted; join failures use the generic `error` op | matches legacy behavior (no emitter in LEGACY/server) |

## Open items from the 2026-09-04 audit

Three read-only audits (security, sync/InstaQL/InstaML parity, perms/auth/admin
parity) were run against the legacy source. Everything they found is fixed
and folded into the tables above: the first batch in #28 (differential replay
steps 25-28), the rest in the follow-up issue's implementation (replay steps
29-34, dash steps 26-32, `scripts/oauth-test.mjs`, the perms / tx integration
tests). The two deliberate divergences are documented in the wire-level table
(`update-attr` keeps cascade config; the `permission-evaluation-failed` cause
text), and the per-session op scheduling difference is described there too.

## Open decisions on the way to 100%

Everything a shipped client (`@instantdb/core`, `react`, `admin`, `platform`,
`instant-cli`, the self-hosted dashboard) sends is served and compared against
the legacy server by the differential harness. What is left is a set of
decisions rather than a backlog: each item below is either a legacy subsystem
with no self-hosting counterpart, a deliberate divergence, or a gap only a
particular deployment hits. Decide each one and the ledger is complete.

### Real gaps a self-hosted deployment can hit

| Gap | Legacy | Here | Decision needed |
|---|---|---|---|
| `app_files_to_sweep` on the `s3` backend | `storage/sweeper.clj` drains the table migration 52's trigger fills on every `$files` delete and removes the objects | `storage.rs` deletes objects synchronously; the trigger still fills the table and nothing reads it | port the sweeper (recommended) or drop the trigger in a `rust_*` migration step |
| Apple and GitHub sign-in | Apple client-secret JWT + issuer quirk (auth/oauth.clj:141-147, :215), GitHub non-OIDC client (:31-113), Instant's shared credentials | generic OIDC discovery only; the dashboard route accepts a `github` provider that the runtime can't sign anyone in with | implement both (a day each) or refuse those provider types on the dash route so the failure is at setup, not at sign-in |
| email delivery provider | Postmark (with Sendgrid behind a flag) and per-app verified senders | `EMAIL_PROVIDER=log` or `cloudflare`; custom senders only via a hand-set `verified` row | add a Postmark / SMTP arm; decide whether sender verification (a Postmark feature) stays out of scope |

### Resolved

- **Per-session op scheduling** (session.clj:1463-1553): ported as
  `scheduler.rs` (issue #38 item 9). Ops with different group keys run
  concurrently, so a slow transact no longer delays that session's queries or
  presence; the concurrency needed one fix legacy gets from its store, an
  `add-query` that re-checks the tx watermark after it registers.
- **Statement timeout** (jdbc/sql.clj `*query-timeout-seconds*`): user
  transacts run with `statement_timeout` = `INSTANT_QUERY_TIMEOUT_SECS`
  (default 30) and `/admin/query` is cut off at the same bound; a cancelled
  statement (57014) is legacy's `timeout` "The query took too long to
  complete." (error matrix `http/timeout/transact-blocked`).
- **`$in` binding**: a `$in` set is one array parameter, like legacy's
  `in-any`; it used to be one bind parameter per value, which failed past
  65,535 values where legacy answers.
- **Hard deletion of apps and attrs** (`hard_deletion_sweeper.clj` +
  `custodian.clj`): `hard_delete.rs` purges apps and attrs whose
  `deletion_marked_at` is older than `INSTANT_HARD_DELETE_GRACE_HOURS`
  (default 48, legacy's 2 days). One node at a time drains triples and
  transactions in 1000-row statements that re-check the mark, then deletes
  the app row (the rest cascades) and its `rust_*` rows. On the `s3` backend
  the objects stay (see `app_files_to_sweep` above).
- **SSRF guard on OIDC fetches** (`smokescreen.clj`,
  `assert-safe-discovery-endpoints!`): discovery, token, userinfo and JWKS
  fetches, plus the dashboard's discovery-endpoint check, go through
  `ssrf.rs`: private addresses refused, connections pinned to the vetted
  addresses, no redirects, 1 MB response cap. `INSTANT_OAUTH_ALLOW_PRIVATE=1`
  admits test providers on localhost.
- **Triple size roll-up** (`triples_size_updates.clj`): the migration 114
  triggers log every write's size delta to `triples_size_updates`; nothing
  aggregated it, so the table grew without bound. `usage.rs` runs legacy's
  collect query every `INSTANT_SIZE_COLLECT_SECS`, which also backs the
  optional per-app size cap (`INSTANT_APP_SIZE_LIMIT_MB`).

### Deliberate divergences (decided 2026-09-24: keep)

Each is documented in the wire-level table above with the client-code citation
that makes it safe; none is observable by a shipped client. Issue #38 item 10
settled every one as a keep; the differential harness pins the observable ones
(`allowed-divergences.json`, `errors-allowed.json`, including `$entityId` and
the out-of-scope invite revoke): `update-attr` keeps cascade
config; admin `delete` by ref lookup deletes the doc; invite revoke is scoped
to the app / org; `$entityId` is rejected; the `permission-evaluation-failed`
cause text is always "You may have a typo"; `timestamp(string)` parse failures
are 400s, not 500s; unknown where operators report `query`, not
`coerced-query`; `processed-tx-id` is `0` before any tx and exact on
`add-query-ok`; empty `refresh-ok`s are not sent to ≤0.20.4 clients for an
unchanged result; any queued frames coalesce; `stream-append` always inlines
`content`; rate limits are always on and rule buckets are shared through
Postgres; webhook ISNs are tx ids, not WAL LSNs; `remove-sync` keeps the
`sync_subs` row (as legacy does by accident); nothing is allowed as a redirect
origin by default; per-node session stats.

### Cosmetic differences

Five server-emitted ws ops (`refresh`, `refresh-presence`,
`refresh-sync-table`, `server-broadcast`, `error`) are executed by legacy when a
client sends them and rejected as `Invalid op` here; the generic 500 body is
`internal-error` with a per-site message rather than "Yikes, something broke
on our end!"; idle sockets are not force-closed (pings drop them); `GET /`
answers `instant-server` rather than the welcome HTML; `GET /health/system`
(`{wal: ok}`) is not served because there is no WAL consumer.

### Not needed for self-hosting (accepted as out of scope unless decided otherwise)

`scripts/differential/out-of-scope.json` lists every hosted-only route with a
reason: billing / Stripe, backups and restores, sunset stages, Postmark sender
verification, the operators' reports and the session-counter feed. The legacy
namespaces with no counterpart (WAL consumer and `cache_evict`, `aggregator` /
attr sketches, app proxy / fail-over / Hazelcast, the flags app, CloudWatch /
Honeycomb / Discord / posthog, BYOP, the admin transact queue) are replaced by
triggers + NOTIFY, exact counts, env vars and `/metrics`, or are the hosted
service's own machinery.

### Validation still to decide

- The self-hosted legacy dashboard (`LEGACY/client/www` with
  `NEXT_PUBLIC_SELF_HOSTED=true`) has every route it calls mounted here, but
  it has not been driven in a browser against this server end to end. Doing so
  (build `www`, point `INSTANT_API_URI` at a node, sign in, manage an app,
  push a schema, invite a member, create a webhook) is the last validation
  layer the dashboard-route PRs lack.
- Differential coverage (issue #38 item 8): every counted surface item is
  exercised on both servers. The runtime OAuth routes are compared in dash
  step 42 (the `start` redirect against Google's real discovery document, the
  callback / token / id_token error surfaces, `openid-configuration`), the
  runtime SSE transport and `POST /runtime/signout` in replay step 35, org
  invite revoke in dash step 37, `operation-timed-out` and `timeout` in the
  error matrix (a transact held behind a table lock), and `$entityId` as an
  allowlisted divergence. The OAuth code exchange's success path needs a
  provider round trip that legacy's DNS-over-HTTPS resolver can't make to a
  local double, so it stays single-server in `scripts/oauth-test.mjs`. Five
  error types no request can produce (`socket-missing`, `socket-error`,
  `connection-closed`, `parameter-limit-exceeded`, `record-check-violation`)
  are listed with a reason each in `scripts/differential/unreachable.json`
  and counted apart.
