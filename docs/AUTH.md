# InstantDB App-Level Auth (Runtime Auth)

End-user authentication for apps built on InstantDB. This documents the wire
protocol implemented by the legacy Clojure server so the Rust server can be
wire-compatible with existing clients (`@instantdb/core` et al.).

Primary sources:

- Routes: `LEGACY/server/src/instant/runtime/routes.clj`
- Magic-code logic: `LEGACY/server/src/instant/runtime/magic_code_auth.clj`
- OAuth provider logic: `LEGACY/server/src/instant/auth/oauth.clj`, `LEGACY/server/src/instant/auth/jwt.clj`
- Models: `LEGACY/server/src/instant/model/app_user.clj`, `app_user_refresh_token.clj`,
  `app_user_magic_code.clj`, `app_oauth_client.clj`, `app_oauth_code.clj`,
  `app_oauth_redirect.clj`, `app_user_oauth_link.clj`, `app_oauth_service_provider.clj`,
  `app_authorized_redirect_origin.clj`, `shared_oauth_client.clj`,
  `app_email_template.clj`, `app_email_sender.clj`
- System catalog: `LEGACY/server/src/instant/system_catalog.clj`, `system_catalog_ops.clj`
- Client: `LEGACY/client/packages/core/src/authAPI.ts`, `Reactor.js`, `clientTypes.ts`, `utils/fetch.ts`

---

## 0. General conventions

- All request bodies are JSON, parsed with keyword keys (`core.clj:96`,
  `wrap-json-body {:keywords? true}`). Responses are JSON via ring
  `wrap-json-response` (cheshire): keyword keys become strings verbatim,
  `java.util.Date` values serialize with cheshire's default date format
  `yyyy-MM-dd'T'HH:mm:ss'Z'` (UTC).
- **Casing is inconsistent and must be preserved.** The magic-code/refresh-token
  endpoints take kebab-case body keys (`app-id`, `refresh-token`,
  `extra-fields`); the oauth/signout endpoints take snake_case (`app_id`,
  `refresh_token`, `extra_fields`). See per-endpoint tables below;
  `authAPI.ts` is the ground truth for what clients send.
- Success status is always `200` with a JSON body. The client treats any
  non-200 as an error and parses the JSON error body (`utils/fetch.ts:117-126`).
- IDs and tokens are UUIDs rendered as lowercase strings.

### Error JSON shape

Built in `util/http.clj:148-218` (`instant-ex->bad-request`, `wrap-errors`)
from exceptions defined in `util/exception.clj`.

Standard error body (status 400, or 401/429/500 as below):

```json
{
  "type": "record-not-found",          // keyword name, see list below
  "message": "Record not found: app-user",
  "hint": { "record-type": "app-user", "args": [ { ... } ] },
  "trace-id": "..."                     // present when a trace is active
}
```

- `type` values seen by auth clients (`utils/fetch.ts:3-77` documents what the
  client expects): `param-missing` (hint `{"in": [path...]}`),
  `param-malformed` (hint `{"in": [...], "message"?}`), `record-not-found`
  (hint `{"record-type": "...", ...}`), `record-expired`,
  `record-not-unique`, `validation-failed` (hint
  `{"data-type": "...", "errors": [{"message": "..."}, ...]}`, message is
  `"Validation failed for <type>: <joined messages>"` per `exception.clj:375-381`),
  `permission-denied` (hint `{"input": ..., "expected": "..."}`),
  `rate-limited` (hint may contain `retry-at`, `retry-after`,
  `remaining-tokens`), `app-read-only`, `app-disabled`, `signups-closed`.
- **`oauth-error` is special** (`http.clj:157-160`): the body is
  `{"type": "oauth-error", "error": "<message>"}` — an `error` key, no
  `message`/`hint`. Status 400.
- Status mapping (`http.clj:187-218`): `bad-request-types`
  (`exception.clj:59-83`) → **400**; if the first entry of `hint.args` has
  `auth? true` → **401** (not used by runtime auth routes; only dash routes);
  `timeout`/`rate-limited` → **429**; anything else → **500** with
  `{"type": ..., "message": ..., "hint": {..., "debug-uri": ...}}` or
  `{"type": "unknown", "message": "Something went wrong. Sorry about this!"}`.
- Missing/invalid params: `ex/get-param!` throws `param-missing` (400,
  `hint.in` = key path, e.g. `["body", "email"]`) or `param-malformed`
  when the coercer returns nil (e.g. invalid email — `util/email.clj:15-21`
  lowercases, trims, then regex-validates).
- Route-level per-app rate limiting: `http-util/with-rate-limiting`
  (`http.clj:52-56`) throws `rate-limited` (429) when the app-id (from
  header/params/body) is flagged.

### The `user` JSON object

All auth endpoints return the user as serialized triples of the `$users`
entity (`system_catalog_ops.clj:32-82`, `triples->db-format` +
`add-computed-columns`). Keys:

```json
{
  "id": "9f0f8e57-...",              // uuid string
  "app_id": "counted-app-uuid",      // injected at system_catalog_ops.clj:80
  "email": "user@example.com",       // absent for guests
  "type": "user",                    // "user" | "guest" (may be absent on old rows)
  "imageURL": "https://...",         // only if set (OAuth picture)
  "created_at": "2026-08-29T12:00:00Z", // txn timestamp of the id triple
  "isGuest": false,                  // computed: type == "guest" (system_catalog_ops.clj:22-24)
  "refresh_token": "uuid-token",     // added by route handlers before responding
  "...extraFields": "..."            // any custom $users attrs set at signup
}
```

The client type (`clientTypes.ts:1-8`) requires `id`, `refresh_token`,
`isGuest`; treats `email`, `imageURL`, `type` as optional.

---

## 1. Runtime auth HTTP endpoints

Route table: `runtime/routes.clj:745-776`.

| Method | Path | Notes |
|---|---|---|
| POST | `/runtime/auth/send_magic_code` | rate-limited wrapper |
| POST | `/runtime/auth/verify_magic_code` | rate-limited wrapper |
| POST | `/runtime/auth/verify_refresh_token` | rate-limited wrapper |
| POST | `/runtime/auth/sign_in_guest` | rate-limited wrapper |
| GET | `/runtime/oauth/start` | cookie-wrapped |
| GET | `/runtime/:app_id/oauth/start` | same handler (path app_id for OIDC clients) |
| GET | `/runtime/oauth/callback` | cookie-wrapped |
| POST | `/runtime/oauth/callback` | for `response_mode=form_post` providers (Apple) |
| POST | `/runtime/oauth/token` | code → refresh token exchange |
| POST | `/runtime/:app_id/oauth/token` | same handler, rate-limited |
| POST | `/runtime/oauth/id_token` | sign in with a provider-issued id_token |
| POST | `/runtime/signout` | |
| GET | `/runtime/:app_id/.well-known/openid-configuration` | |
| GET | `/runtime/session` | WebSocket upgrade (sync protocol) |

### 1.1 POST /runtime/auth/send_magic_code

Handler: `routes.clj:76-81`. Client: `authAPI.ts:14-24`.

Request body (kebab-case):

```json
{ "app-id": "<uuid>", "email": "user@example.com" }
```

- `email` is coerced (lowercase/trim/regex). Invalid → 400 `param-malformed`.
- Rate limit per (app-id, email): flag-controlled, default path bucket4j;
  exceeded → 429 `rate-limited` with message
  `"Too many verification codes requested for this email. Please try again later."`
  (`magic_code_auth.clj:32-59`, `exception.clj:480-482`).

Response 200:

```json
{ "sent": true }
```

### 1.2 POST /runtime/auth/verify_magic_code

Handler: `routes.clj:94-111`, logic `magic_code_auth.clj:269-313`.
Client: `authAPI.ts:39-89` (both `verifyMagicCode` and `checkMagicCode` hit
this endpoint).

Request body (kebab-case):

```json
{
  "app-id": "<uuid>",
  "email": "user@example.com",
  "code": "123456",
  "refresh-token": "<uuid>",          // optional; only honored if it belongs to a guest user
  "extra-fields": { "field": "val" }  // optional; custom $users fields on first signup
}
```

- `code` is trimmed. Lookup is by `sha256(code)` + email within the app.
- If `refresh-token` resolves to a user of `type == "guest"`, that guest is
  upgraded/linked (see §4). If the token is invalid,
  `get-by-refresh-token!` throws `record-not-found`/`app-user` (400) —
  note: the guest lookup throws rather than silently ignoring a bad token
  (`routes.clj:99-104`).
- Errors: unknown/wrong code → 400
  `{"type":"record-not-found","message":"Record not found: app-user-magic-code","hint":{"record-type":"app-user-magic-code","args":[...]}}`;
  expired code → 400 `{"type":"record-expired","message":"Record expired: app-user-magic-code", ...}`;
  `$users` create-permission denial → 400 `permission-denied`; unknown
  extra field → 400 `validation-failed`.

Response 200:

```json
{
  "user": { ...user object..., "refresh_token": "<new uuid>" },
  "created": true            // true iff a new $users row was created
}
```

(`routes.clj:110-111` — `created` is stripped from the user map and hoisted
to the top level; the user object keeps `refresh_token`.)

### 1.3 POST /runtime/auth/verify_refresh_token

Handler: `routes.clj:154-159`. Client: `authAPI.ts:92-106`.

Request body (kebab-case):

```json
{ "app-id": "<uuid>", "refresh-token": "<uuid>" }
```

Response 200 (no `created` key):

```json
{ "user": { ...user object..., "refresh_token": "<same token echoed back>" } }
```

Invalid token → 400 `record-not-found` / `"record-type": "app-user"`.

### 1.4 POST /runtime/auth/sign_in_guest

Handler: `routes.clj:131-149`. Client: `authAPI.ts:111-125`.

Request body (kebab-case):

```json
{ "app-id": "<uuid>", "extra-fields": { ... } }   // extra-fields optional
```

Creates a `$users` row with `type = "guest"` and **no email**, plus a fresh
refresh token.

Response 200:

```json
{ "user": { ...user object (isGuest: true, no email)..., "refresh_token": "<uuid>" } }
```

### 1.5 POST /runtime/signout

Handler: `routes.clj:161-165`. Client: `authAPI.ts:189-205`.

Request body (**snake_case**, unlike the other auth routes):

```json
{ "app_id": "<uuid>", "refresh_token": "<uuid>" }
```

Deletes the refresh-token entity (lookup by `hashedToken`). Idempotent-ish:
delete of a missing token does not error. Sign-out works even when the app is
read-only/disabled (`app_user_refresh_token.clj:66-76`,
`:skip-app-status-write-check?`).

Response 200: `{}`

### 1.6 GET /runtime/oauth/start (and /runtime/:app_id/oauth/start)

Handler: `routes.clj:213-274`. Client builds this URL in
`Reactor.js:2428-2446`:
`/runtime/oauth/start?app_id=<uuid>&client_name=<name>&redirect_uri=<url-encoded>`.

Query params:

| param | required | notes |
|---|---|---|
| `app_id` | yes | uuid |
| `client_name` or `client_id` | yes | `client_id` is an alias for OAuth-SDK compatibility (`routes.clj:216-220`) |
| `redirect_uri` | yes | must match an authorized redirect origin (§2.4) |
| `state` | no | opaque; if present it is appended to `redirect_uri` as `?state=` before storing (`routes.clj:238-241`) |
| `code_challenge`, `code_challenge_method` | no | PKCE, stored for later verification (`plain` or `S256`) |
| `hd` | no | forwarded to the provider; Google-only "hosted domain" hint (`routes.clj:172-174`, `oauth.clj:23`) |

Behavior:

1. Look up `$oauthClients` row by name; 400 `record-not-found` if missing.
2. Validate `redirect_uri` against authorized origins → 400
   `validation-failed` with message `"Invalid redirect_uri. If you're the developer, make sure to add your website to the list of approved domains."`.
3. Generate `cookie-uuid` and `state-uuid`. Persist a `$oauthRedirects`
   entity (hashes only, §2.5).
4. Redirect **302** to the provider authorization endpoint with
   `state = str(app_id) + str(state-uuid)` (72 chars, two concatenated
   uuid strings — `routes.clj:254`), and set cookie:
   - name `__session`, value `instantdb_<cookie-uuid>` (prefix stripped on
     parse, `routes.clj:184-190`),
   - `HttpOnly`, `Secure` (except dev), `Expires` = now + 1h,
   - `Path=/runtime/oauth`.

Provider authorization URL params (generic OIDC, `oauth.clj:127-137`):
`scope=email openid`, `response_type=code`, `response_mode=form_post`,
`state`, `redirect_uri`, `client_id`, plus allowed extra params (`hd`).
GitHub variant (`oauth.clj:60-66`): `scope=read:user user:email`,
`response_type=code`, no `response_mode`.

The provider `redirect_uri` is `"<server-origin>/runtime/oauth/callback"`
(`routes.clj:170`) unless the oauth client has a custom `redirectTo`
(`routes.clj:249-250`).

### 1.7 GET|POST /runtime/oauth/callback

Handler: `routes.clj:506-600`. Not called by JS clients directly — the
provider redirects the browser here. `?test-redirect` returns a static test
page (`routes.clj:597-600`).

Steps (each failure short-circuits):

1. `error` param present → treated as OAuth error.
2. `state` param must be 72 chars; first 36 = app-id, last 36 = state uuid.
3. `__session` cookie must be present and its sha256 must equal the stored
   `cookieHash` (constant-time compare, `routes.clj:541-543`).
4. `$oauthRedirects` entity consumed (get+delete, one-time) by
   `sha256(state)`; must exist and be **≤ 10 minutes old**
   (`app_oauth_redirect.clj:37-60`).
5. `code` param exchanged with the provider (`get-user-info`, §2.2);
   extracts `email` (only if `email_verified`), `sub`, `imageURL`
   (from `picture` claim).
6. A one-time app-level code (uuid) is created in `$oauthCodes` storing
   `{email, sub, imageURL}` as `userInfo`, plus the PKCE challenge carried
   over from the redirect entity (`routes.clj:567-576`).
7. **302** to the app's stored `redirectUrl` with
   `?code=<uuid>&_instant_oauth_redirect=true` appended. For non-http(s)
   schemes (native apps), renders an HTML landing page that opens the app
   (`routes.clj:583-585`, `434-504`).

On error with a known redirect: **302** to
`redirectUrl?error=<message>&_instant_oauth_redirect=true`
(`routes.clj:587-595`). On error before the redirect entity was found:
400 `{"type":"oauth-error","error":"<message>"}`.

The client SPA (`Reactor.js:2020-2080`) detects `_instant_oauth_redirect` in
its URL, reads `code` (or `error`), strips the params from the URL, and calls
`/runtime/oauth/token`.

### 1.8 POST /runtime/oauth/token (and /runtime/:app_id/oauth/token)

Handler: `routes.clj:607-659`. Client: `authAPI.ts:134-154`
(`exchangeCodeForToken`).

Request params (**snake_case**). Accepted from JSON body, query params, or
form params, under both keyword and string keys (`routes.clj:602-611`
`param-paths` — this makes it usable as a standard OAuth token endpoint):

```json
{
  "app_id": "<uuid>",
  "code": "<uuid from callback redirect>",
  "code_verifier": "...",            // required iff code_challenge was given at /start
  "refresh_token": "<uuid>",         // optional; guest-user upgrade (body only)
  "extra_fields": { ... }            // optional (body only)
}
```

Behavior:

1. `$oauthCodes` consumed (get+delete). Missing → 400 `record-not-found`
   `"record-type": "app-oauth-code"` (the client specifically checks
   `body.hint["record-type"] === "app-oauth-code"` to ignore double
   exchanges, `Reactor.js:2068-2076`). Older than **5 minutes** → 400
   `record-expired` (`app_oauth_code.clj:39-60`).
2. PKCE verified (`oauth.clj:364-414`): challenge XOR verifier mismatch →
   400 `validation-failed`; `S256` = sha256(verifier) vs
   base64url-decoded challenge; `plain` = constant-time string equality.
3. If an `Origin` header is present it must be an authorized origin → else
   400 `validation-failed` `"Unauthorized origin."` (`routes.clj:621-622`, `204-211`).
4. User upsert/link via `$oauthUserLinks` (§2.3).
5. New refresh token created.

Response 200 (note `refresh_token` appears **both** inside `user` and at the
top level — `routes.clj:657-659`):

```json
{
  "user": { ...user object..., "refresh_token": "<uuid>" },
  "created": false,
  "refresh_token": "<uuid>"
}
```

### 1.9 POST /runtime/oauth/id_token (signInWithIdToken)

Handler: `routes.clj:661-716`. Client: `authAPI.ts:164-186`.

Request body (**snake_case**):

```json
{
  "app_id": "<uuid>",
  "id_token": "<JWT from native provider SDK>",
  "client_name": "<oauth client name>",
  "nonce": "...",                 // optional
  "refresh_token": "<uuid>",      // optional: reuse token / guest upgrade
  "extra_fields": { ... }         // optional
}
```

Behavior:

1. Client looked up by name; `Origin` header (if present) must be
   authorized.
2. JWT verified against the provider's JWKS (§2.6). If the oauth client has
   **no client_secret** configured, verification runs with
   `ignore-audience? true` and honors the client's
   `meta.allowUnverifiedEmail` flag (`routes.clj:673-681`).
3. User upsert/link as in §2.3 (email only used when `email_verified`, or
   `allowUnverifiedEmail`).
4. Refresh token: if the supplied `refresh_token` exists **and** belongs to
   the same user that logged in, it is reused; otherwise a new one is
   created (`routes.clj:706-712`). If the supplied token belongs to a guest
   user, the guest is linked/upgraded.

Response 200 (no top-level `refresh_token`, unlike /oauth/token):

```json
{ "user": { ...user object..., "refresh_token": "<uuid>" }, "created": false }
```

Errors: bad JWT → 400 `oauth-error` (`{"type":"oauth-error","error":"..."}`)
for signature/JWKS problems, or 400 `validation-failed` on `:id_token` for
nonce/issuer/audience/subject problems (messages listed in §2.6).

### 1.10 GET /runtime/:app_id/.well-known/openid-configuration

Handler: `routes.clj:718-724`. Response 200:

```json
{
  "authorization_endpoint": "<server-origin>/runtime/<app_id>/oauth/start",
  "token_endpoint": "<server-origin>/runtime/<app_id>/oauth/token"
}
```

---

## 2. OAuth flow details

### 2.1 Client / provider storage

Providers and clients are stored **as triples** in the system catalog
namespaces (they are entities in the app's own triple store, not SQL tables):

- `$oauthProviders` (`system_catalog.clj:213-220`,
  `app_oauth_service_provider.clj`): `id`, `name` (unique; e.g. `"google"`,
  `"github"`, `"apple"`). A provider is the issuer of unique `sub` values;
  one provider may have several clients (web + native).
- `$oauthClients` (`system_catalog.clj:247-269`, `app_oauth_client.clj`):
  `id`, `$oauthProvider` (ref), `name` (unique; the `client_name` clients
  pass), `clientId`, `encryptedClientSecret`, `discoveryEndpoint`, `meta`
  (JSON blob), `redirectTo`, `useSharedCredentials`.
  - The secret is AEAD-encrypted with the entity id as associated data and
    stored hex-encoded (`app_oauth_client.clj:47-50`, decrypt at 138-142).
  - When reading, `triples->db-format` renames keys:
    `clientId → client_id`, `encryptedClientSecret → client_secret`
    (decoded to bytes), `discoveryEndpoint → discovery_endpoint`,
    `$oauthProvider → provider_id`, `name → client_name`
    (`system_catalog_ops.clj:53-76`).
- `->OAuthClient` (`app_oauth_client.clj:144-179`) dispatch:
  - has `discovery_endpoint` → generic OIDC client built from the discovery
    document;
  - provider name `"github"` → hard-coded GitHub OAuth2 client
    (`oauth.clj:31-113`; no OIDC, email fetched from
    `api.github.com/user/emails`, `sub` = numeric GitHub user id as string);
  - otherwise → error.
- **Shared credentials** (`shared_oauth_client.clj`): when
  `useSharedCredentials` is true, `client_id`/`client_secret` come from
  server config (`config/shared-oauth-clients`, per-env EDN) keyed by
  provider name — lets devs test Google OAuth without their own client.
  Limited to 100 users per app; exceeding → 400 `validation-failed`
  (`shared_oauth_client.clj:45-61`, checked on signup in
  `routes.clj:306-307`).

### 2.2 Google specifics vs generic OIDC

Google is just a generic OIDC client with
`discoveryEndpoint = "https://accounts.google.com/.well-known/openid-configuration"`.
Google-specific behavior:

- `hd` extra param forwarded on the authorization URL (`oauth.clj:23`,
  `routes.clj:174`).
- **Nonce checks are skipped entirely when issuer is
  `https://accounts.google.com`** (`oauth.clj:242-247`) because native
  Google sign-in libraries handle nonces inconsistently.
- Discovery + Google JWKS are pre-warmed at startup
  (`oauth.clj:421-427`, `jwt.clj:155-158`).

Generic OIDC mechanics:

- Discovery doc fetched (SSRF-guarded), cached (max 32 endpoints), refreshed
  hourly (`oauth.clj:296-334, 419-442`). Fields used:
  `authorization_endpoint`, `token_endpoint`, `jwks_uri`, `issuer`,
  `id_token_signing_alg_values_supported` (default `#{RS256 HS256}` when
  absent), `userinfo_endpoint`.
- Code exchange (`oauth.clj:139-199`): form POST to `token_endpoint` with
  `client_id`, `client_secret`, `code`, `grant_type=authorization_code`,
  `redirect_uri`. The returned `id_token` is **not signature-verified** in
  this path (server-to-server exchange); its payload is base64-decoded for
  `email`/`email_verified`/`sub`/`picture`. Fallback: if no id_token,
  hit `userinfo_endpoint` with the access token. Email is used **only when
  `email_verified` is truthy**.
- Apple quirk: client secret is a self-signed ES256 JWT built from
  `meta.teamId`/`meta.keyId` + the stored private key
  (`oauth.clj:140-147`, `jwt.clj:129-148`); Apple posts the callback
  (hence the POST route) and has two issuer spellings
  (`account.apple.com` / `appleid.apple.com`) treated as equivalent
  (`oauth.clj:215-222`).

### 2.3 User creation / linking — `$oauthUserLinks`

`upsert-oauth-link!` (`routes.clj:276-377`), model
`app_user_oauth_link.clj`, attrs `system_catalog.clj:222-245`.

Link entity fields: `id`, `sub`, `$user` (ref), `$oauthProvider` (ref), and
the composite unique key **`sub+$oauthProvider` = `"<sub>+<provider-id-uuid>"`**
(`format "%s+%s" sub provider-id`, `app_user_oauth_link.clj:19-21`) — this
string is how uniqueness of (sub, provider) is enforced.

Upsert algorithm:

1. Query users matching `email == e` OR `oauthUserLinks.sub+$oauthProvider ==
   "<sub>+<provider-id>"` (`app_user.clj:225-260`).
2. 0 matches → create user (`type: "user"`, with `email`, `imageURL`,
   extra fields; signup permission + shared-credential cap checked) + create
   link. `created: true`. If a guest refresh token was supplied, the guest's
   user-id is reused as the new user id.
3. 1 match → reuse; update `imageURL` if changed; update email if the link
   matched but email changed; create the link if the user matched by email
   only. Guest upgrade: `linkedPrimaryUser` triple added on the guest user.
4. \>1 matches → disambiguate by exact email match; if ambiguous → 400
   `oauth-error` `"Could not disambiguate between multiple users for this account."`;
   may re-point an existing link's `$user` to the selected user.

### 2.4 Authorized redirect origins

SQL table `app_authorized_redirect_origins` (one of the few real SQL tables
here): `id`, `app_id`, `service`, `params text[]`
(`app_authorized_redirect_origin.clj:14-43`). Matching
(`app_authorized_redirect_origin.clj:95-116`) by service type:

- `"generic"` — params `[host]`, exact host (incl. `:port`) match.
- `"netlify"` — params `[site-name]`, matches `*--<site>.netlify.app|live`
  and `<site>.netlify.app`.
- `"vercel"` — params `[deployment-suffix, project-name]`, host starts with
  project name and ends with suffix.
- `"custom-scheme"` — params `[scheme]`, matches URL scheme (native apps),
  reserved schemes (http, https, ...) rejected.
- Shared-credential apps additionally always allow localhost/127.0.0.1/[::1]/
  0.0.0.0 (http/https) and `exp://` (`app_authorized_redirect_origin.clj:71-93`).
  This server has no shared credentials, so nothing is allowed by default: add
  a `generic` origin for local development (`scripts/create-oauth-client.sh`
  takes it as its optional 7th argument, e.g. `localhost:5173`).

Used for the `redirect_uri` at `/oauth/start` and the `Origin` header at
`/oauth/token` and `/oauth/id_token`.

### 2.5 State, cookie, PKCE — `$oauthRedirects` and `$oauthCodes`

`$oauthRedirects` (`app_oauth_redirect.clj`, attrs
`system_catalog.clj:289-316`): `id`, `stateHash` (sha256-hex of state uuid,
unique), `cookieHash` (sha256-hex of cookie uuid), `redirectUrl` (the app's
final redirect), `redirectTo` (provider callback used), `$oauthClient` (ref),
`codeChallengeMethod`, `codeChallenge`. Consumed once (delete-on-read) by
stateHash; expiry 10 minutes from `created_at`. Lookups are by hash to
prevent timing attacks (`app_oauth_redirect.clj:54-55`).

`$oauthCodes` (`app_oauth_code.clj`, attrs `system_catalog.clj:271-287`):
`id`, `codeHash` (sha256-hex of the code uuid, unique), `codeChallengeMethod`,
`codeChallenge`, `userInfo` (blob `{"email","sub","imageURL"}`),
`$oauthClient` (ref). Consumed once; expiry 5 minutes.

PKCE is optional and app-level (between the JS/native client and Instant's
`/oauth/start` + `/oauth/token`), **not** between Instant and Google.
Instant→provider exchange uses the client secret, no PKCE.

### 2.6 id_token verification (`oauth.clj:201-294`, `jwt.clj:109-127`)

- JWKS fetched from the discovery doc's `jwks_uri`; cached per URI with
  `Expires`/`max-age` honoring, hourly refresh; RS256 and ES256 supported
  (HS256 in the alg-allowlist default but no verifier — effectively RSA/EC
  only). Signature/decode failures → 400 `oauth-error` with messages like
  `"Error validating JWT. Signature is invalid."`.
- Post-signature claim checks (each failure → 400 `validation-failed` on
  `:id_token` with one message):
  - nonce: must equal the request's `nonce` (or sha256-hex of it, for the
    invertase Apple lib); skipped for Google; messages:
    `"The id_token is missing a nonce."`,
    `"The nonce parameter was not provided in the request."`,
    `"The nonces do not match."`.
  - issuer must equal the discovery `issuer` (Apple dual-issuer exception) —
    `"The id_token wasn't issued by <issuer>."`.
  - alg must be in `id_token_signing_alg_values_supported` —
    `"The id_token used an unsupported algorithm."`.
  - audience must contain `client_id` unless `ignore-audience?`
    (secret-less clients) — `"The id_token was generated for the wrong OAuth client."`.
  - `sub` required — `"The id_token had no subject."`.
- Output: `{email (nil unless email_verified or allowUnverifiedEmail), sub,
  imageURL (from "picture")}`.

---

## 3. Refresh tokens

Model `app_user_refresh_token.clj`; attrs `system_catalog.clj:199-211`
(`$userRefreshTokens`).

- The token handed to clients is a **random UUID**. It is never stored in
  plaintext: the entity stores `hashedToken = hex(sha256(uuid-bytes))`
  (`app_user_refresh_token.clj:12-16` — sha256 over the uuid's 16 raw
  bytes, then lowercase hex).
- Entity fields: `id` (a *separate* random uuid — not the token),
  `hashedToken` (unique, indexed), `$user` (ref to `$users`, cascade
  delete). API-level `id` is back-filled with the plaintext token for
  compatibility (`app_user_refresh_token.clj:40-48`).
- **No expiry.** Tokens live until deleted (signout, user deletion cascade).
- Lookup: user-by-token is an instaql query
  `{$users: {$: {where: {"$userRefreshTokens.hashedToken": <hash>}}}}`
  (`app_user.clj:155-176`).
- **WS `init`**: the sync client sends
  `{"op":"init","app-id":...,"refresh-token":"<uuid>", ...}`
  (`Reactor.js:1762-1773`); the server resolves the user via the same hash
  lookup and errors the init if the token doesn't resolve
  (`reactive/session.clj:154-196`, `get-by-refresh-token!`). The resulting
  user becomes `auth.user` for permissions and is echoed in the `init-ok`
  payload.
- Admin/HTTP APIs use the refresh token as a bearer token
  (e.g. `routes.clj:726-743` framework query uses
  `Authorization: Bearer <refresh token>` to resolve `current-user`).

Rust note: sha256 input is the 16-byte big-endian encoding of the UUID
(`crypt-util/uuid->sha256`), not the string form.

## 4. Magic codes

Model `app_user_magic_code.clj`; attrs `system_catalog.clj:186-197`
(`$magicCodes`); logic `magic_code_auth.clj`.

- **Generation**: 6-char numeric string via `rand-num-str 6`
  (`util/string.clj:14-17`). Quirk: `(rand-nth (range 0 9))` yields digits
  **0–8 only** (the digit 9 never appears). Codes are not unique per se;
  lookup is by `(codeHash, email)` within the app.
- **Storage**: entity `{id, codeHash: hex(sha256(utf8(code))), email}`.
  The magic code is keyed by email, not user — the user row is created only
  on verify.
- **Expiry**: `created_at + app.magic_code_expiry_minutes` (per-app SQL
  column, `app.clj:429-433`), default **1440 minutes (24h)** from the
  `default-magic-code-expiry-minutes` flag (`flags.clj:593-595`).
- **Consume** (`app_user_magic_code.clj:45-65`): get by `(codeHash,email)`,
  assert exists (`record-not-found` / `app-user-magic-code`), delete, then
  check expiry (`record-expired` if past). One-time use. A per-app "test
  user" with a static code bypasses consumption
  (`magic_code_auth.clj:250-267`).
- **Verify flow** (`magic_code_auth.clj:269-313`): permission check
  (`$users` `create` rule + extra-fields validation) runs *before* consuming
  so a denial doesn't burn the code; then user upsert (`type: "user"`),
  guest linking (`linkedPrimaryUser`), and refresh-token creation. Returns
  the user map + `refresh_token` + `created`.
- **Email sending** (`magic_code_auth.clj:188-239`): rendered from the
  per-app template (`app_email_templates` SQL table, `email-type =
  "magic-code"`, placeholders `{code}`, `{app_title}`, `{user_email}`,
  `{expiration}`; custom sender only if verified) or the default:
  subject `"<code> is your verification code for <title>"`. Sent via
  Postmark with fallback to the default sender.
  **Rust implementation** (`crates/instant-server/src/email.rs`): a
  delivery-provider seam selected by `EMAIL_PROVIDER` — `log` (default)
  prints the code to the server log; `cloudflare` sends via the Cloudflare
  Email Service REST API (see README env vars). Templates/senders are
  honored as in legacy, with one retry falling back to the default sender.
  Unlike legacy, delivery is fully fire-and-forget: the response is always
  `{"sent": true}` and send failures are only logged.

## 5. `$users` integration (system catalog)

Auth data lives **inside the normal triple store** of each app, in reserved
`$`-prefixed namespaces. There are no SQL user tables; the models transact
triples through `system_catalog_ops.clj` (`update-op`/`query-op`).

- Namespaces: `$users`, `$magicCodes`, `$userRefreshTokens`,
  `$oauthProviders`, `$oauthUserLinks`, `$oauthClients`, `$oauthCodes`,
  `$oauthRedirects` (`system_catalog.clj:33-43`).
- **Deterministic attr ids**: every system attribute has a globally fixed
  UUID computed by encoding `("system" + type-shortcode)` and
  `"<etype-shortcode>/<label-shortcode>"` into the two 64-bit halves of the
  UUID, using a 5-bit alphabet (`system_catalog.clj:22-123`,
  `encode-system-uuid`). E.g. `$users` = `"us"`, label `id` = `"id"`.
  The Rust server must reproduce these exact UUIDs for wire/DB
  compatibility (`get-attr-id etype label`).
- `$users` attrs (`system_catalog.clj:172-184`): `id` (unique, indexed),
  `email` (unique, indexed, string), `type` (string), `imageURL` (string),
  `linkedPrimaryUser` (ref `$users`↔`linkedGuestUsers`, cascade).
- `$userRefreshTokens.$user` has reverse identity `$users.$userRefreshTokens`
  and `$oauthUserLinks.$user` reverse `$users.$oauthUserLinks` — these are
  what queries/permission rules can traverse.
- Users may **extend** `$users` (and `$files`/`$streams`) with custom attrs
  (`editable-etypes`, `system_catalog.clj:422-430`); those become the
  `extra-fields` accepted at signup, validated against the schema and
  forbidden from touching system attrs (`app_user.clj:24-41`).
- **Permissions**: the authenticated user (resolved from the refresh token
  at WS `init` or via bearer token) is set as `current-user` in the query
  context; CEL rules reference it as `auth` (`auth.id`, `auth.email`, custom
  fields). The `$users.allow.create` rule gates signup
  (`app_user.clj:51-85`); when extra fields are supplied and no rule exists,
  signup is denied.
- Creating a user = transacting triples
  `[:add-triple uid $users/id uid]`, `[... $users/email email]`,
  `[... $users/type "user"|"guest"]`, etc. (`app_user.clj:87-109`), so
  `$users` rows are queryable by instaql like any other namespace (subject
  to permissions).

## 6. Endpoint/response quick reference

| Endpoint | Request keys | 200 response |
|---|---|---|
| POST `/runtime/auth/send_magic_code` | `app-id`, `email` | `{"sent": true}` |
| POST `/runtime/auth/verify_magic_code` | `app-id`, `email`, `code`, `refresh-token?`, `extra-fields?` | `{"user": {..., "refresh_token"}, "created": bool}` |
| POST `/runtime/auth/verify_refresh_token` | `app-id`, `refresh-token` | `{"user": {..., "refresh_token"}}` |
| POST `/runtime/auth/sign_in_guest` | `app-id`, `extra-fields?` | `{"user": {..., "refresh_token"}}` |
| POST `/runtime/signout` | `app_id`, `refresh_token` | `{}` |
| GET `/runtime/oauth/start` | query: `app_id`, `client_name`/`client_id`, `redirect_uri`, `state?`, `code_challenge?`, `code_challenge_method?`, `hd?` | 302 + `__session` cookie |
| GET/POST `/runtime/oauth/callback` | query/form: `state`, `code`/`error`; cookie | 302 to app with `code` & `_instant_oauth_redirect=true` (or `error`) |
| POST `/runtime/oauth/token` | `app_id`, `code`, `code_verifier?`, `refresh_token?`, `extra_fields?` | `{"user": {..., "refresh_token"}, "created": bool, "refresh_token": "..."}` |
| POST `/runtime/oauth/id_token` | `app_id`, `id_token`, `client_name`, `nonce?`, `refresh_token?`, `extra_fields?` | `{"user": {..., "refresh_token"}, "created": bool}` |
| GET `/runtime/:app_id/.well-known/openid-configuration` | — | `{"authorization_endpoint", "token_endpoint"}` |

Error bodies: §0. All errors are JSON; clients switch on `body.type`,
`body.hint["record-type"]`, and `body.message`.
