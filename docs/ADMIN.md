# InstantDB Admin HTTP API & Storage Endpoints

Reference for reimplementing the Admin HTTP API in Rust, wire-compatible with
`@instantdb/admin` (and the parts of the CLI that matter). All file references are into
`LEGACY/` at the repo root:

- Server routes: `LEGACY/server/src/instant/admin/routes.clj` (route table at `routes.clj:778-822`)
- Steps translation: `LEGACY/server/src/instant/admin/model.clj`
- Storage: `LEGACY/server/src/instant/storage/{routes,coordinator,s3}.clj`,
  `LEGACY/server/src/instant/model/{app_file,app_upload_url}.clj`
- Error handling: `LEGACY/server/src/instant/util/{exception,http}.clj`
- Admin SDK client: `LEGACY/client/packages/admin/src/index.ts`, `subscribe.ts`
- CLI client: `LEGACY/client/packages/cli/src/lib/*.ts`

---

## 1. Authentication & headers

### 1.1 App identification

Every admin endpoint resolves the app id from, in order
(`admin/routes.clj:53-57`, `ex/get-some-param!`):

1. Header `app-id` (exactly this, lowercase, with a dash)
2. Query param `app_id` (underscore)

Both must coerce to a UUID. The admin SDK sends **both**: header `app-id` plus
`?app_id=<uuid>` on the URL (`admin/src/index.ts:241-260`, e.g. `index.ts:1213`).

### 1.2 Bearer token

`authorization: Bearer <token>` (`util/http.clj:15-25`). The token string is coerced
(`util/token.clj:44-55`):

- starts with `pat_` → platform OAuth access token
- starts with `per_` → personal access token
- starts with `eyJ` → JWT
- otherwise → must parse as a UUID → **app admin token** (the normal case for `@instantdb/admin`)

For a plain admin token, auth = row lookup in `app_admin_tokens` where
`token = ? AND app_id = ?` (`model/app_admin_token.clj:7-16`). Failure throws
`record-not-found` with hint message *"This admin token may be expired or invalid. Or you
may have provided an incorrect app ID."* → **HTTP 400** (see §3).

Platform/personal tokens route through `req->superadmin-user!`
(`superadmin/routes.clj:34-63`): scope check (`data/read`, `data/write`, `storage/read`,
`storage/write`, `apps/read`, `apps/write`) then the app must belong to that user
(`admin/routes.clj:59-76`). For a first Rust implementation only the UUID admin-token
path is required for `@instantdb/admin` parity.

### 1.3 Impersonation headers

Exact header names (`admin/routes.clj:78-111`; SDK side `admin/src/index.ts:202-214`):

| Header     | Value                          | Behavior |
|------------|--------------------------------|----------|
| `as-token` | app-user refresh token (UUID)  | `admin? = false`; `current-user` looked up by refresh token. **Does not validate the admin token at all** (app id taken from the untrusted header). Malformed UUID → 400 `param-malformed` on `[:asUser :token]`; unknown token → 400 `record-not-found` (`app-user`). |
| `as-email` | user email                     | **Requires a valid admin token** (calls `req->app-id-authed!`); `admin? = false`; user looked up by email (`get-by-email!` → 400 `record-not-found` if missing). |
| `as-guest` | any value (SDK sends `"true"`) | `admin? = false`, `current-user = nil`. No admin-token validation. |
| *(none)*   |                                | Requires valid admin token; `admin? = true` (perms skipped), `show-cel-errors? = true`. |

Precedence: `as-token` > `as-email` > `as-guest` > admin.

The SDK also always sends `content-type: application/json`, plus
`Instant-Admin-Version` and `Instant-Core-Version` (e.g. `v0.21.x`) on every request
(`index.ts:299-316`). The server only logs these (`util/http.clj:92-115`); don't require
them.

### 1.4 JSON conventions

- Request bodies are parsed with `{:keywords? true}` (`core.clj:89-96`), so JSON keys map
  1:1 onto Clojure keywords — keys like `"inference?"`, `"throw-on-missing-attrs?"`,
  `"rules-override"`, `"extra-fields"` are literal (kebab-case, `?` suffix included).
- Responses are serialized with Cheshire: keyword keys render as their name, so
  `:tx-id` → `"tx-id"`, `:check-results` → `"check-results"`,
  `:has-next-page?` → `"has-next-page?"`. `java.util.Date` values (`created_at`)
  serialize as `"yyyy-MM-dd'T'HH:mm:ss'Z'"`; `java.time.Instant` as ISO-8601 string.
- Raw-body exception: `PUT /admin/storage/upload`, `PUT /storage/upload`,
  `PUT /dash/apps/:app_id/storage/upload` skip JSON body parsing (`core.clj:217-220`).
- Successful responses are always **200** (`response/ok`). The SDK treats anything
  other than 200 as an error (`index.ts:299-316`).

---

## 2. Error shape & status codes

Middleware `wrap-errors` (`util/http.clj:148-218`) converts internal
`instant-exception`s (`util/exception.clj:22-53`):

**400 Bad Request** — for types in `ex/bad-request-types` (`exception.clj:59-85`):

```json
{
  "type": "record-not-found",       // keyword name of the ex type
  "message": "Record not found: app-admin-token",
  "hint": { "record-type": "app-admin-token", "args": [ ... ], "message": "..." },
  "trace-id": "<hex>"               // present when tracing produced one
}
```

Type names seen on the wire: `record-not-found`, `record-expired`, `record-not-unique`,
`record-foreign-key-invalid`, `record-check-violation`, `sql-raise`, `permission-denied`,
`permission-evaluation-failed`, `param-missing`, `param-malformed`, `validation-failed`,
`parameter-limit-exceeded`, `app-read-only`, `app-disabled`, etc.

- `param-missing`: `hint: {"in": ["body","query"]}` (path segments), message
  `"Missing parameter: [\"body\" \"query\"]"` (`exception.clj:410-418`).
- `param-malformed`: `hint: {"in": [...], "original-input": ...}`.
- `validation-failed`: `hint: {"data-type": ..., "input": ..., "errors": [{"message": ...,
  "hint"?/"expected"?/"in"?}]}`; top-level message is
  `"Validation failed for <input-type>: <joined error messages>"` (`exception.clj:366-383`).
- `permission-denied`: message `"Permission denied: not <perm>"`,
  `hint: {"input": ..., "expected": "<perm>"}` (`exception.clj:291-297`).

**401 Unauthorized** — same body, only when `hint.args[0].auth?` is truthy
(`http.clj:188-190`); this happens for dashboard user auth
(`instant-user get-by-refresh-token!`), not for admin-token failures. Admin-token
failures are **400**.

**429 Too Many Requests** — two cases (`http.clj:192-201`):
- `type: "timeout"` (`message: "The query took too long to complete."`)
- `type: "rate-limited"` — most `/admin/*` routes are wrapped in `with-rate-limiting`
  (`admin/routes.clj:691-696`, `util/http.clj:32-56`), which checks a per-app flag.

**500** — `{"type": "<type>"|"unknown", "message": "...", "hint": {..., "debug-uri": ...},
"trace-id"?}` (`http.clj:207-218`).

The SDK wraps non-200 bodies in `InstantAPIError({status, body})`; if the body isn't
JSON it uses `{type: undefined, message: <raw text>}` (`index.ts:284-297`).

---

## 3. Endpoints used by `@instantdb/admin` (priority for parity)

Route table: `admin/routes.clj:778-822`.

| Endpoint | SDK method | Priority |
|---|---|---|
| `POST /admin/query` | `db.query` | **P0** |
| `POST /admin/transact` | `db.transact` | **P0** |
| `POST /admin/refresh_tokens` | `db.auth.createToken` | **P0** |
| `POST /runtime/auth/verify_refresh_token` | `db.auth.verifyToken` | **P0** |
| `GET /admin/users` | `db.auth.getUser` | P1 |
| `DELETE /admin/users` | `db.auth.deleteUser` | P1 |
| `POST /admin/sign_out` | `db.auth.signOut` | P1 |
| `POST /admin/magic_code` | `db.auth.generateMagicCode` | P1 |
| `POST /admin/send_magic_code` | `db.auth.sendMagicCode` | P1 |
| `POST /admin/verify_magic_code` | `db.auth.verifyMagicCode` / `checkMagicCode` | P1 |
| `POST /admin/sign_in_guest` | (server route exists; SDK exposes guest impersonation) | P2 |
| `PUT /admin/storage/upload` | `db.storage.uploadFile` | **P0** (storage) |
| `DELETE /admin/storage/files` | `db.storage.delete` (deprecated) | P2 |
| `POST /admin/storage/files/delete` | `db.storage.deleteMany` (deprecated) | P2 |
| `POST /admin/storage/signed-upload-url` | `db.storage.upload` (deprecated) | P3 |
| `GET /admin/storage/signed-download-url` | `db.storage.getDownloadUrl` (deprecated) | P3 |
| `GET /admin/storage/files` | `db.storage.list` (deprecated) | P3 |
| `POST /admin/query_perms_check` | `db.debugQuery` | P2 |
| `POST /admin/transact_perms_check` | `db.debugTransact` | P2 |
| `GET /admin/rooms/presence` | `db.rooms.getPresence` | P2 |
| `POST /admin/subscribe-query` (SSE) | `db.subscribeQuery` | P2 |
| `POST /admin/sse`, `POST /admin/sse/push` | `db.streams` (SSE transport) | P3 |
| `GET /admin/schema` | not used by SDK (Kosmik-experimental) | P3 |
| `GET /admin/soft_deleted_attrs` | not used by SDK | P3 |

CLI (`instant-cli`) uses `/dash/...` routes with a *dashboard user* bearer token — see §6.

---

## 4. Endpoint details

### 4.1 `POST /admin/query` (`admin/routes.clj:143-158`)

Request body:

```json
{
  "query": { "goals": { "todos": {} , "$": { "where": {...}, "limit": 10, ... } } },
  "inference?": true,          // optional; SDK sends !!schema
  "versions": { "@instantdb/admin": "v...", "@instantdb/core": "v..." }  // optional
}
```

- `query` must be a JSON object (else 400 `param-missing`/`param-malformed` on
  `["body","query"]`).
- `ruleParams`: the SDK merges them into the query as a top-level `$$ruleParams` key
  (`index.ts:1202-1204`), not a separate body field.
- Perms context from §1.3; runs `iq/permissioned-query` then
  `instaql-nodes->object-tree` (`util/instaql.clj:99-102`).

Response **200** — the object tree directly (no wrapper):

```json
{
  "goals": [
    { "id": "<uuid>", "title": "Get fit", "todos": [ { "id": "...", ... } ] },
    ...
  ]
}
```

Formatting rules (vs the WS protocol, which returns raw instaql nodes / datalog
results — see `docs/PROTOCOL.md`):

- Top-level keys are the query's namespace strings; values are **arrays** of entity
  objects with string keys (`util/instaql.clj:86-102`).
- Every entity includes `"id"`; nested link keys are merged into the entity object.
- Sorting: entries are sorted server-side by the `$` `order` field (default
  `$serverCreatedAt` ascending), ties broken by id using Postgres uuid comparison; the
  internal `"$serverCreatedAt"` field is stripped from output (`util/instaql.clj:66-84`).
- **`inference?` singular refs**: when `inference?` is true and a link is
  forward-cardinality-one (or reverse-unique), the nested value is a single object (or
  absent/`null`) instead of an array (`util/instaql.clj:39-42,93-95`). With
  `inference?` false everything is an array. This is the only effect of `inference?`.
- `$files` entities get a synthetic `"url"` field (signed download URL) injected, and
  `location-id` is hidden unless explicitly selected via `$.fields`
  (`db/instaql.clj:1050-1081`).
- No pageInfo in this response (pageInfo only exists on the SSE subscribe path and WS).

### 4.2 `POST /admin/transact` (`admin/routes.clj:266-322`)

Request body:

```json
{
  "steps": [ ... ],
  "throw-on-missing-attrs?": true   // optional; SDK sends !!schema
}
```

Response **200**: `{"tx-id": 4711}` (integer transaction id).

#### Steps format (`admin/model.clj`)

Each step is an array `[action, ...args]`. Spec at `admin/model.clj:429-492`:

| Step | Shape |
|---|---|
| `["create", etype, eid, obj]` | insert-only (`:mode :create`) |
| `["update", etype, eid, obj, opts?]` | upsert by default; `opts = {"upsert": false}` → strict update, `{"upsert": true}` → explicit upsert (`model.clj:164-186`) |
| `["merge", etype, eid, obj, opts?]` | deep-merge JSON values (`:deep-merge-triple`), `id` key still `add-triple` |
| `["link", etype, eid, {label: eidOrEids, ...}]` | values may be a single lookup or array |
| `["unlink", etype, eid, {label: eidOrEids}]` | retracts triples |
| `["delete", etype, eid, ...ignored]` | `[:delete-entity lookup etype]` |
| `["ruleParams", etype, eid, paramsObj]` | attaches rule params |
| `["add-attr", attrObj]` | attr map: `{id, forward-identity: [id, etype, label], reverse-identity?, value-type: "blob"|"ref", cardinality: "one"|"many", "unique?", "index?", ...}` |
| `["update-attr", attrObj]` | |
| `["delete-attr", attrId]` | |

`eid` may be:
- UUID string;
- **lookup string** `"lookup__<attr>__<json-value>"` (JSON value after second `__`;
  parse: split on `__`, rejoin remainder, JSON-parse — `model.clj:31-45`);
- 2-tuple `[attrName, value]`;
- single-entry object `{attrName: value}` (`model.clj:47-63`).

Lookup attr must be `unique?`, else 400 validation error `"<attr> is not a unique
attribute on <etype>"` (`model.clj:81-93`). Ref lookups like `"owner.id"` are supported
(`model.clj:65-79`).

**Attr auto-creation**: unknown attrs referenced by `create/update/merge/link/unlink`
or lookups are auto-created (object attrs `value-type: blob, cardinality: one`; link
attrs `ref/many`; lookup attrs get `unique?+index?`) and the generated `[:add-attr ...]`
steps are prepended to the transaction (`model.clj:238-397`). If
`throw-on-missing-attrs?` is true, missing attrs instead throw 400 `validation-failed`
with `hint.errors[0].message = "Attributes are missing in your schema"` and
`hint.errors[0].hint.attributes = ["etype.label", ...]` (`model.clj:382-397`).

Invalid entity ids produce 400 with message `"Invalid entity ID '<x>'. Entity IDs must
be UUIDs or lookup references."` and `errors[0].in = [stepIdx, 2]` (`model.clj:414-427`).

Perms: with plain admin auth, permission checks are skipped; with impersonation the
transaction runs the `$default`/etype rules like a client transaction
(`get-perms!` merged into ctx; rules loaded at `routes.clj:277`).

### 4.3 `POST /admin/query_perms_check` (`admin/routes.clj:219-249`)

Requires a **valid admin token** *and* an impersonation header (admin context is
rejected: 400 `validation-failed` "Cannot test perms as admin").

Body: `{"query": {...}, "rules-override"?: {...}, "inference?"?: bool,
"ip-override"?: "1.2.3.4", "origin-override"?: "https://..."}` (SDK:
`index.ts:1357-1399`).

Response **200**:

```json
{
  "check-results": [
    { "id": "<uuid>", "entity": "goals", "label"?: ...,
      "record": { ...entity map... },
      "program": { "code": ..., "display-code": ..., "etype": ..., "action": ... },
      "check": true|false|<cel value> }
  ],
  "result": { ...same object tree as /admin/query... },
  "rule-wheres": [...]
}
```

(`db/instaql.clj:2270-2307`.) SDK reads `response.result` and
`response["check-results"]`.

### 4.4 `POST /admin/transact_perms_check` (`admin/routes.clj:324-375`)

Same auth constraints as 4.3. Body: `{"steps": [...], "rules-override"?: {...},
"dangerously-commit-tx"?: bool, "ip-override"?, "origin-override"?,
"throw-on-missing-attrs?"?}`. `dangerously-commit-tx` false/absent → dry run
(transaction rolled back).

Response **200**:

```json
{
  "tx-id": 123,
  "all-checks-ok?": true,
  "committed?": false,
  "check-results": [
    { "scope": "object"|"attr", "etype": ..., "action": "create|update|delete|view|link|unlink|...",
      "eid": ..., "check-result": ..., "check-pass?": true,
      "program": { "etype": ..., "action": ..., "code": ..., "display-code": ... },
      "bindings"?: { "data": {...}, "new-data": {...}, ... } }
      // program filtered to those 4 keys; internal ":check" key removed
      // (routes.clj:364-374; item construction permissioned_transaction.clj:326-632)
  ]
}
```

(`routes.clj:364-375`, result assembly `db/permissioned_transaction.clj:613-744`.)

### 4.5 `POST /admin/refresh_tokens` (`admin/routes.clj:396-435`) — createToken

Body: `{"email": "a@b.com"}` **or** `{"id": "<uuid>"}`, optional
`{"extra-fields": {field: value}}`. Neither email nor id → 400 validation
`"Please provide an `email` or `id`"`.

Semantics: find user by email/id; if missing, create it (with given id or random,
running `$users` schema validation of extra-fields but skipping perms). Always creates a
fresh refresh token (random UUID) for the user.

Response **200**:

```json
{
  "user": {
    "id": "<uuid>", "email": "a@b.com", "app_id": "<uuid>",
    "created_at": "2026-08-29T12:00:00Z",
    "type"?: "user"|"guest", ...extraFields,
    "refresh_token": "<uuid>"
  },
  "created": false
}
```

User objects come from `system_catalog_ops.clj:32-81` (`triples->db-format`): all
`$users` attrs plus `app_id` and `created_at`. SDK returns `ret.user.refresh_token`
(`index.ts:629-642`).

### 4.6 `POST /runtime/auth/verify_refresh_token` (`runtime/routes.clj:154-160`, mounted at `:750`) — verifyToken

Not an `/admin` route but used by `db.auth.verifyToken` (`index.ts:660-673`). No admin
token; body only:

```json
{ "app-id": "<uuid>", "refresh-token": "<uuid>" }
```

(SDK also passes `?app_id=` in the URL; the handler reads the body keys `:app-id` /
`:refresh-token`.) Response **200** `{"user": { ...user fields..., "refresh_token":
"<the same token>" }}`. Unknown token → 400 `record-not-found` (`app-user`).

### 4.7 `POST /admin/sign_out` (`admin/routes.clj:437-463`)

Admin token required. Body: one of `{"id": uuid}`, `{"email": str}`,
`{"refresh_token": uuid}` (checked in that order). `id`/`email` delete **all** refresh
tokens for the user (email 404s → 400 if user missing); `refresh_token` deletes just
that token. None given → 400 validation `"Please provide an `id`, `email`, or
`refresh_token`"`. Response **200** `{"ok": true}`.

### 4.8 `GET /admin/users` / `DELETE /admin/users` (`admin/routes.clj:465-498`)

Admin token required. Query params (precedence): `email`, then `refresh_token`, then
`id`. None given → 400 validation `"Please provide a user id, email, or refresh_token"`;
malformed value → 400 `param-malformed`.

- GET → **200** `{"user": {...user fields...} | null}` (no refresh_token field).
- DELETE → deletes the user entity (not its data) → **200**
  `{"deleted": {...deleted user...} | null}`.

### 4.9 Magic codes & guests

- `POST /admin/magic_code` (`routes.clj:500-504`): body `{"email": ...}` → creates a
  code without sending email. **200** `{"code": "123456"}` (6 random digits,
  `model/app_user_magic_code.clj:16-43`; stored as sha256 hash on a `$magicCodes`
  entity).
- `POST /admin/send_magic_code` (`routes.clj:506-510`): same, but also emails the code
  via the configured provider. **200** `{"code": "123456"}`.
- `POST /admin/verify_magic_code` (`routes.clj:512-532`): body `{"email", "code",
  "extra-fields"?, "refresh-token"?}` (`refresh-token` = an existing **guest** user's
  token to upgrade/link). Consumes the code (wrong/expired → 400 `record-not-found`
  `app-user-magic-code`), upserts the user, mints a refresh token. **200**:
  `{"user": {..., "refresh_token": "<uuid>"}, "created": true|false}`
  (`runtime/magic_code_auth.clj:269-314`).
- `POST /admin/sign_in_guest` (`routes.clj:534-554`): body `{"extra-fields"?}` →
  creates a `type: "guest"` user + refresh token. **200**
  `{"user": {..., "type": "guest", "refresh_token": "<uuid>"}}`.

### 4.10 `GET /admin/rooms/presence` (`admin/routes.clj:739-766`)

Admin token required. Query params: `app_id`, `room-type` (required but unused beyond
validation), `room-id`. SDK call
(`index.ts:475-488`): `GET /admin/rooms/presence?app_id=..&room-type=..&room-id=..`.

Response **200**:

```json
{
  "sessions": {
    "<session-uuid>": {
      "peer-id": "<session-uuid>",
      "data": { ...user presence data... },
      "user": { ...full app user record... } | null
    }
  }
}
```

Room data comes from the ephemeral (Hazelcast) room map; the stored `user {id}` stub is
replaced with the full user row. SDK returns `res.sessions || {}`.

### 4.11 `GET /admin/schema` (`admin/routes.clj:682-689`)

Experimental (only external consumer noted is “Kosmik”; the SDK/CLI don't use it).
Requires admin token (scope `apps/read` for platform tokens). Returns
**200** `{"schema": {"blobs": {etype: {label: attr}}, "refs": {"comments-post-posts-comments": attr}}}`
— i.e. `schema-model/attrs->schema` with ref keys (4-tuples) joined by `-`.

### 4.12 `GET /admin/soft_deleted_attrs` (`admin/routes.clj:771-776`)

**200** `{"attrs": [...], "grace-period-days": N}`. Not used by SDK/CLI; skip.

### 4.13 SSE endpoints (subscribeQuery / streams)

All are POST requests that hold open a `text/event-stream` response:

- `POST /admin/subscribe-query?local_connection_id=<uuid>` (`routes.clj:160-179`) —
  body `{"query": {...}, "inference?": bool, "versions": {...}}`, same auth headers.
  Server messages (JSON in SSE `data:` lines), consumed by
  `admin/src/subscribe.ts:300-345`:
  - `{"op": "sse-init", "machine-id": ..., "session-id": ..., "sse-token": "<uuid>"}`
    (`reactive/session.clj:1358-1396,204-209`)
  - `{"op": "add-query-ok", "result": <object tree>, "result-meta": {"page-info": {etype: {"start-cursor": [...], "end-cursor": [...], "has-next-page?": bool, "has-previous-page?": bool}}}}`
  - `{"op": "refresh-ok", "computations": [{"instaql-result": ..., "result-meta": ...}]}`
  - `{"op": "error", "status": ..., ...}`
- `POST /admin/sse` (`routes.clj:184-201`) — generic session (used by `db.streams`);
  client sends events back over HTTP via
- `POST /admin/sse/push?app_id=...` (`routes.clj:203-210`) — body
  `{"machine_id", "session_id", "sse_token", "messages": [...]}`; token is checked
  against the sha256 hash captured at SSE open. **200** `{}`.

These reuse the WS reactive-session machinery (see `docs/PROTOCOL.md`); implement last.

---

## 5. Storage

### 5.1 Data model

Files are rows in the system namespace **`$files`** (`model/app_file.clj`), attrs:
`id`, `path` (unique lookup), `size`, `content-type`, `content-disposition`,
`location-id` (random UUID naming the blob), `key-version` (=1), plus a **virtual
`url`** attr injected at query time. The blob itself lives in S3 under object key
`"<app-id>/<bin>/<location-id>"` where `bin = abs(javaStringHashCode(location-id)) % 10`
(`storage/s3.clj:176-185`). A local-disk adapter only needs: write blob, head (size),
delete, and produce a download URL for a `(app-id, location-id)` pair.

Storage perms use the `$files` rules namespace with actions `create`, `delete`, `view`
evaluated with CEL data `{"path": <path>}`; **default-deny when no rule exists**;
admin (`admin? = true`) skips checks entirely (`storage/coordinator.clj:19-38`).
In prod there's a per-app "storage disabled" flag (`storage/beta.clj`) — irrelevant
locally.

### 5.2 `PUT /admin/storage/upload` (`admin/routes.clj:628-643`) — **the current upload flow**

- Auth: admin token or impersonation headers (perms action `create` when impersonating).
- Metadata comes from **headers**: `path` (required), `content-type` (optional; literal
  strings `"null"`/`"undefined"`/blank are treated as absent —
  `coordinator.clj:148-151`), `content-disposition` (optional), `content-length`
  (required by the SDK for streams; server passes it to S3).
- Body: the raw file bytes (this route is excluded from JSON parsing,
  `core.clj:217-220`).
- Server: generates `location-id = randomUUID`, uploads to S3
  (`s3.clj:226-244`; content-type defaults to `application/octet-stream` at the S3
  layer when absent), reads back object metadata (size/content-type/etag/last-modified),
  then creates/updates the `$files` entity by `path` lookup
  (`app_file.clj:13-40` — upsert semantics: re-uploading a path replaces the row's
  metadata and location).
- Response **200**:

```json
{ "data": { "id": "<file entity uuid>", "location-id": "<uuid>", "size": 12345 } }
```

SDK type only requires `data.id` (`core/src/StorageAPI.ts:3-7`).

Note: re-upload orphan cleanup and S3 deletion are done by a background sweeper, not
inline.

### 5.3 Deletes

- `DELETE /admin/storage/files?filename=<url-encoded path>` (`routes.clj:645-652`) —
  single delete by path. **200** `{"data": {"id": "<uuid>" | null}}`.
- `POST /admin/storage/files/delete` (`routes.clj:654-661`) — body
  `{"filenames": ["a.png", ...]}`. **200** `{"data": {"ids": ["<uuid>", ...] | null}}`.

Both only delete the `$files` rows (`coordinator.clj:83-103`); blob cleanup is the
sweeper's job. Files can also be deleted via `/admin/transact` on `$files` (the
recommended path per SDK deprecation notes, `index.ts:916-960`).

### 5.4 Download URLs / file serving

There is **no server file-serving route**: queries on `$files` return a presigned S3
GET URL as the `url` field (`db/instaql.clj:1050-1063` → `s3.clj:311-328`).
URLs are signed with a signing instant bucketed to start-of-day (stable for ~24h for
browser caching) and a 7-day expiry. For local disk: either implement a
`/storage/serve/...` route with your own HMAC-signed URLs, or serve unsigned since
you control the deployment — but the `url` field **must** appear on `$files` query
results for client compatibility.

Deprecated (Jan 2025) but still live admin routes:

- `POST /admin/storage/signed-upload-url` (`routes.clj:702-708`): body
  `{"app_id": ..., "filename": ...}` → **200**
  `{"data": "<server-origin>/storage/<upload-id>/consume-upload-url"}` — *not* an S3
  URL: it creates a row in `app_upload_urls` (`model/app_upload_url.clj:8-17`) and
  returns an Instant URL. The client then does a raw
  `PUT /storage/:upload-id/consume-upload-url` with the file body
  (`storage/routes.clj:54-62`), which consumes the row (single-use; expiry check) and
  runs the same upload pipeline → **200** `{"data": {"id", "location-id", "size"}}`.
- `GET /admin/storage/signed-download-url?filename=...` (`routes.clj:710-716`) →
  **200** `{"data": "<presigned S3 url>"}`; an unknown path is not an error —
  `get-by-path` returns nil so the response is `{"data": null}`.
- `GET /admin/storage/files` (`routes.clj:719-737`) → **200**
  `{"data": [{"key": "<app-id>/<bin>/<location-id>", "name": "<path>", "size": N,
  "etag": null, "last_modified": null}]}` (implemented by querying `$files`
  internally).

### 5.5 Client-facing storage routes (`storage/routes.clj:71-76`)

Used by browser SDKs with a **user refresh token** as bearer (may be absent → guest):

- `PUT /storage/upload` — headers `app-id` (or `app_id`), `path` (or `filename`),
  `content-type?`, `content-disposition?`; raw body → `{"data": {...}}` (perms `create`
  enforced).
- `DELETE /storage/files?app_id=..&filename=..` → `{"data": {"id"}}` (perms `delete`).
- `POST /storage/signed-upload-url` — body `{app_id, filename}` (perms `create`).
- `PUT /storage/:upload-id/consume-upload-url` — raw body, no auth (the upload-id is
  the credential).
- `GET /storage/signed-download-url?app_id=..&filename=..` (perms `view`) →
  `{"data": "<url>"}`.

Dashboard variants also exist (`PUT /dash/apps/:app_id/storage/upload`,
`POST /dash/apps/:app_id/storage/files/delete`, `dash/routes.clj:~1760-1790`) using
dashboard auth; low priority.

---

## 6. Schema & perms endpoints (CLI parity)

The CLI authenticates with `Authorization: Bearer <token>` where the token is a
dashboard-user token obtained via the CLI login flow
(`POST /dash/cli/auth/register` → user visits dashboard → `POST /dash/cli/auth/check`
returns the token; `dash/routes.clj:2789-2792`), or `INSTANT_CLI_AUTH_TOKEN`. These
routes accept dashboard refresh tokens, personal access tokens (`per_...`), platform
tokens, and (for the `req->app-accepting-superadmin-or-ref-token!` variants) also an
**app admin token**.

Used by the CLI (`cli/src/lib/*.ts`):

- `GET /dash/apps/:app_id/schema/pull` (`dash/routes.clj:1836-1845`, route `:2843`) →
  **200** `{"schema": {"blobs": ..., "refs": ...}, "attrs": [ ...raw attr maps... ],
  "app-title": "..."}`. Attr maps: `{"id", "forward-identity": [id, etype, label],
  "reverse-identity"?, "value-type", "cardinality", "unique?", "index?",
  "required?"?, "checked-data-type"?, "catalog"?, "inferred-types"?, ...}`.
- `POST /dash/apps/:app_id/schema/steps/apply` (`dash/routes.clj:1808-1818`, route
  `:2842`) — **this is how `instant-cli push schema` applies changes**
  (`cli/src/lib/pushSchema.ts:166-174`): the CLI pulls, diffs locally, and posts
  `{"steps": [ ["add-attr", {...}], ["update-attr", {...}], ["delete-attr", id],
  ["index", ...], ["unique", ...], ["check-data-type", ...], ... ]}` (tx-step format,
  coerced by `tx/coerce!`). Response **200**
  `{"transaction": {...}, "steps": [...], "indexing-jobs": {...} | null}`
  (`model/schema.clj:581-596`); the CLI polls indexing jobs via
  `GET /dash/apps/:app_id/indexing-jobs/group/:group_id`.
- `POST /dash/apps/:app_id/schema/push/plan` (`:2839`) — body `{"schema": <client
  defs>, "check_types"?: bool, "supports_background_updates"?: bool}` → **200**
  `{"new-schema", "current-schema", "current-attrs", "steps"}`
  (`model/schema.clj:495-513`). (Server-side planning; current CLI mostly diffs
  client-side but this remains supported.)
- `POST /dash/apps/:app_id/schema/push/apply` (`:2840`) — plan+apply in one call.
- `GET /dash/apps/:app_id/perms/pull` (`dash/routes.clj:1847-1856`, route `:2844`) →
  **200** `{"perms": { ...rules code... } | null}`.
- `POST /dash/apps/:app_id/rules` (`dash/routes.clj:688-698`, route `:2758`) — push
  perms; body `{"code": { ...rules json... }}` → **200** `{"rules": {...}}`
  (validation errors → 400 `validation-failed`).
- `GET /dash/cli/version` (`:2794`) → `{"min-version": "..."}`.

### 6.1 What this server implements

All of the routes above except `/dash/cli/auth/*` are served (`crates/instant-server/src/routes/dash.rs`,
planning in `crates/instant-core/src/schema.rs`, jobs in `crates/instant-server/src/indexing_jobs.rs`).
Verified against the live legacy server by `scripts/differential/dash.mjs` and with the real
CLI by `scripts/cli-test.mjs`.

- **Auth.** There is no dashboard, so the CLI authenticates with the **app admin token**:
  `INSTANT_APP_ADMIN_TOKEN=<token> INSTANT_APP_ID=<id> INSTANT_CLI_API_URI=<server> instant-cli push`
  (or `--token`). The legacy `admin-token-mismatch` error (400 `validation-failed`,
  `hint.reason`) is raised when the token belongs to another app. Dashboard refresh tokens
  present in a migrated `instant_user_refresh_tokens` table are honored for app creators and
  `app_members` (collaborator+); `per_`/`pat_` platform tokens get a 401. `/dash/cli/auth/*`
  answers 400 with a message pointing at the admin token instead of the browser login flow.
- **Jobs.** `index`/`remove-index`/`unique`/`remove-unique`/`required`/`remove-required`/
  `check-data-type`/`remove-data-type` steps create rows in the legacy `indexing_jobs` table
  (`waiting` → `processing` → `completed`/`errored`, same `job_stage` names, `worker_id` = node id)
  and the node that accepted the request runs them, one transaction per job. Error reporting
  matches the CLI's expectations: `triple-not-unique-error` with `invalid_unique_value`,
  `invalid-triple-error` with `invalid_triples_sample`, `missing-required-error` with
  `error_data` (`count`, `etype`, `label`, `entity-ids`); errored jobs keep `done_at` null.
  Each job also inserts a `transactions` row so connected clients get refreshed attrs.
  Batched/resumable rewrites for very large attrs are tracked in issue #5.
- **Shapes worth knowing.** `schema/steps/apply` echoes the input steps with a `job-id` added
  to job steps and returns the raw `indexing_jobs` rows (all columns) under `indexing-jobs.jobs`,
  while the group/poll endpoints return the client format (`attr_name`,
  `invalid_triples_sample`, no `job_serial_key`/`worker_id`/...). `delete-attr` of an unknown
  attr id is a no-op (legacy's soft delete is a plain `UPDATE`). `GET .../indexing-jobs/:id`
  for an unknown id returns `{"job": {}}`. Rules pushes that don't change the code return
  `{"rules": null}`. Error bodies carry a `trace-id` like legacy's `wrap-errors`.

`/superadmin/*` equivalents exist for the OAuth-platform API
(`superadmin/routes.clj:333-351`: `GET/POST /superadmin/apps`,
`GET /superadmin/apps/:app_id/schema`, `POST .../schema/push/{plan,apply}`,
`GET/POST /superadmin/apps/:app_id/perms`) authenticated with `per_`/`pat_` tokens —
**not used by the CLI**; implement only if you want platform-API parity.

---

## 7. Implementation notes for the Rust server

1. Only 200 counts as success to clients; error bodies must carry `type`, `message`,
   `hint` with the casings above (kebab-case keys, `?`-suffixed booleans).
2. `app-id` header + `app_id` query param must both be accepted; header wins.
3. Admin-token auth failures are **400 record-not-found**, not 401/403.
4. `as-token` / `as-guest` must work **without** any admin token.
5. `/admin/query` responses: plain object tree, string keys, arrays unless
   `inference?`+singular link; entities always include `id`; `$files` rows get `url`.
6. `/admin/transact` must accept the full steps grammar of `admin/model.clj`
   (lookup strings with `lookup__` prefix included) and auto-create attrs unless
   `throw-on-missing-attrs?`.
7. Upload metadata rides in headers (`path`, `content-type`, `content-disposition`);
   body is raw bytes; response nests under `"data"`.
8. Local-disk storage adapter surface needed: `put(app_id, location_id, stream,
   content_type, content_disposition) -> metadata{size,...}`, `delete`, `head`,
   `download_url(app_id, location_id) -> String` (stable ≥24h if you want cache
   parity).
