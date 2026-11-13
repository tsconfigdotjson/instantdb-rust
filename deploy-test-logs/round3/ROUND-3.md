# instant-worker on Cloudflare, round 3: skipped suites and a soak

Date: 2026-11-12, 02:12–03:12 UTC. Branch head `9908d5fb255f9a0e836b29220bd40460d309d101`.

## Summary

| item | result |
|---|---|
| A1 security-test | **pass** (`SECURITY TEST PASSED`; the TIMEOUT_NODE_URL section skipped itself) |
| A2 webhooks-test, no `INSTANT_WEBHOOK_ALLOW_INSECURE` | **First attempt failed** on a database error (`shutting down`), not on URL vetting. **Repeat passed**, 37/37 checks, with no insecure flag. The fallback run with the flag was not needed and was not run. |
| A3 oauth-test, no `INSTANT_OAUTH_ALLOW_PRIVATE` | **pass**, 111 checks (`OAUTH TEST PASSED`), including signed-id_token verification. The fallback run was not needed and was not run. |
| A4 dash-login-test | **Run 1 invalid**: during the deploy rollout, requests were served by the previous version, which had no dashboard OAuth config. **Run 2: 51 ok, 2 FAIL.** Both failures are localhost assumptions in the script (quoted below). |
| A5 magic-code sign-in, log provider | **pass**: send 200, code found in the tail, verify 200, refresh-token check 200 |
| B soak | **ran 47 min 47 s of the planned 1 h**, stopped early at the user's request. 1148 operations, 0 failed, 0 session drops. 28 of 287 refresh waits took 10.6–20.7 s (see B.2). |

## Setup

- **S1.** `git rev-parse HEAD` → `9908d5fb255f9a0e836b29220bd40460d309d101`.
- **S2.** Last round's Worker and Hyperdrive config were gone. With the user's go-ahead:
  - **Hyperdrive:** `npx wrangler hyperdrive create instant --connection-string=<REDACTED> --caching-disabled` → `101dc1c6db37417f80eead618c01e005`.
  - **Database:** the same Neon database as rounds 1–2 (migrations already applied; AWS us-east-2).
  - **Vars:** `BASE_URL = "https://instant-worker.leerosen686.workers.dev"`, `INSTANT_HANDLE_RECEIVE_TIMEOUT_MS = "30000"`, `RUST_LOG = "info,sqlx=warn"`, `NODE_LOCATION_HINT = "enam"`.
  - **Deploy:** `npx wrangler deploy` → version `a41c0d5a`, `NODE_NAME = "node-9"`, `Worker Startup Time: 65 ms`.
  - **Tail:** `npx wrangler tail --format json` ran to a file outside the repo for the whole round, in a restart loop that logs every exit. It started once (1794449646774) and **never disconnected** before teardown.
- **S3.** `cloudflared` was not installed and there is no Homebrew. With the user's go-ahead, I downloaded release `2026.10.7`, `cloudflared-darwin-arm64.tgz`, from GitHub into the session scratchpad.
  - The tarball matches GitHub's asset digest (`sha256:3586ccf3…f831bd`).
  - The binary inside matches the sha256 in the release notes (`2136431d…d5bb12`).
  - Its code signature is `Developer ID Application: Cloudflare Inc. (68WVV388M8)`.
  - The release notes list the binary's hash, not the tarball's, so my first check reported a mismatch. I had run `cloudflared --version` before finishing that check.
- **Tunnels:** each was `cloudflared tunnel --no-autoupdate --url http://127.0.0.1:<port>`, to ports **9911** (A2 receiver), **9377** (A3 mock provider) and **9378** (A4 mock Google).

## A1. security-test: pass

`SERVER_URL=$URL DATABASE_URL=<REDACTED> node scripts/security-test.mjs <app> <admin-token>` on a fresh app, against `node-9`. It took 16 s and exited 0. The last 40 lines all read `ok:`, apart from `skip: TIMEOUT_NODE_URL unset` and the final `SECURITY TEST PASSED`.

## A2. webhooks-test, without `INSTANT_WEBHOOK_ALLOW_INSECURE`

**Script copy** `/tmp/iw-r3/webhooks-test.mjs`. One line changed:
```
58c58
< const receiverUrl = `http://localhost:${port}/hook`;
---
> const receiverUrl = "https://<tunnel-9911>/hook";
```
The local listener stays on `127.0.0.1:9911`. The run used `SERVER_URL=$URL` against `node-9`, with the insecure flag unset.

### Attempt 1: exit 1, 32 s, `WEBHOOKS TEST FAILED: 20 checks`

The failures began with the very first database writes:
```
FAIL create webhook {"status":500,"body":{"type":"internal-error","message":"database error: error returned from database: shutting down","hint":null,"trace-id":"bc88d486bb154c6bbb1a714d5bf76578"}}
FAIL transact {"status":500,"body":{"type":"internal-error","message":"database error: error returned from database: shutting down","hint":null,"trace-id":"5ecc43466a974a9aad622845e876d830"}}
```
The remaining failures follow from those two: there was no webhook id, so later calls returned `param-malformed … webhook_id … "undefined"`.

The tail, on version `a41c0d5a`, shows the database connections closing at the same moment. This line appeared five times between 1794449715010 and 1794449715420, and once more at 1794449754694:
```
WARN sqlx_core::pool::connection: error occurred while testing the connection on-release error=error communicating with database: expected to read 5 bytes, got 0 bytes at EOF
```
Afterwards, `select pg_postmaster_start_time()` on the database returned 02:12:04 UTC. That is the compute start caused by my first `psql` of the round, which was before this failure. So **the Neon compute had not restarted**. Where `shutting down` came from was not determined.

### Attempt 1b: exit 0, 14 s, `WEBHOOKS TEST PASSED`

This was the same server, tunnel and script copy, on a fresh app, started 30 s after attempt 1 ended. All 37 checks passed, including:
- `create webhook` and `create requires https unless insecure allowed`;
- `receiver got a delivery`;
- `signature verifies with the JWK`;
- `payload with the delivered token`;
- `410 disables the webhook`;
- `resend delivered again`.

So the URL vetting (https required, DNS lookup, public-address check) accepted the tunnel's https URL on Workers, and delivery worked.

**Leftover delivery warnings.** Later in the round, the tail logged this 4 times in part A and 24 times during the soak:
```
WARN instant_server::webhooks: webhook delivery failed: Record not found: webhook
```
Both runs delete their webhook at the end, and attempt 1's create had failed. These look like retries for webhooks that no longer exist; I didn't check further.

## A3. oauth-test, without `INSTANT_OAUTH_ALLOW_PRIVATE`: pass

**Script copy** `/tmp/iw-r3/oauth-test.mjs`. `SERVER` changed, plus every provider URL the mock advertises. The `http.createServer` listener (all interfaces, port 9377) and the URL-parsing base on line 41 are unchanged.
```
10c10  const SERVER = "http://localhost:8888";  ->  "https://instant-worker.leerosen686.workers.dev"
24c24  iss: `http://localhost:${PROVIDER_PORT}`,  ->  `https://<tunnel-9377>`
46-49  issuer / authorization_endpoint / token_endpoint / jwks_uri: `http://localhost:${PROVIDER_PORT}…`  ->  `https://<tunnel-9377>…`
71c71  iss: `http://localhost:${PROVIDER_PORT}`,  ->  `https://<tunnel-9377>`
```

**Client registration:**
```
./scripts/create-oauth-client.sh <app> mock mock-provider mock-client-id mock-secret "https://<tunnel-9377>/.well-known/openid-configuration" localhost:5173
```
It exited 0.

**Run:** `node /tmp/iw-r3/oauth-test.mjs <app> <admin-token>` against `node-9`: exit 0, 35 s, `OAUTH TEST PASSED`, 111 `ok:` lines. These include the signed-id_token checks:
```
ok: an algorithm the discovery document doesn't list is refused
ok: unsupported-alg message
ok: a bad signature is refused
ok: id_token sign-in from an unauthorized Origin is refused
ok: a signed id_token signs the linked user in
```
The server fetched discovery, token and JWKS over the tunnel, and the private-address vetting did not reject it.

## A4. dash-login-test

**Server config.** I redeployed with `NODE_NAME = "node-10"` and these vars:
- `INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_ID = "mock-google-client"`
- `INSTANT_DASHBOARD_GOOGLE_OAUTH_AUTH_URL = "https://<tunnel-9378>/authorize"`
- `INSTANT_DASHBOARD_GOOGLE_OAUTH_TOKEN_URL = "https://<tunnel-9378>/token"`

The deploy produced version `a0199a4e`. Then `npx wrangler secret put INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_SECRET` (value on stdin) printed `✨ Success!` and produced version `f09d1394`.

**Script copy** `/tmp/iw-r3/dash-login-test.mjs`. One line changed:
```
79c79
< ... s1.loc.origin + s1.loc.pathname === `http://localhost:${port}/authorize` ...
---
> ... s1.loc.origin + s1.loc.pathname === `https://<tunnel-9378>/authorize` ...
```
The run used `SERVER_URL=$URL DATABASE_URL=<REDACTED>`.

### Run 1, started 4 s after `secret put`: invalid

It exited 1 after 5 s: 15 ok, 10 FAIL, then `TypeError: Cannot read properties of undefined (reading 'id')` at line 113. The start redirect went to Google's default URL with an empty client id:
```
FAIL start redirects to the auth url "https://accounts.google.com/o/oauth2/v2/auth?scope=email&response_type=code&state=…&redirect_uri=https%3A%2F%2Finstant-worker.leerosen686.workers.dev%2Fdash%2Foauth%2Fcallback&client_id="
FAIL callback redirects to the dashboard with a code + ticket {"status":302,"loc":"http://localhost:3000/dash/oauth/callback?error=Error+fetching+user+data+from+Google%3A+Could+not+determine+client+ID+from+request..", …}
```
In the tail, the run's requests came from three versions: `a41c0d5a` (node-9, no dashboard vars), `a0199a4e` and `f09d1394`. Every Durable Object event ran `a41c0d5a`, including a boot (`storage sweeper started` at 1794449904394). So the run hit the deploy rollout, and the server answered with no dashboard config.

### Run 2: exit 1, 14 s, 51 ok, `DASH LOGIN TEST FAILED: 2 checks`

Before the rerun:
- I waited 130 s with no requests, so the object would be evicted;
- three `/health` calls returned 200 (0.63, 0.08, 0.08 s);
- every tail event in the run was version `f09d1394`, and the object booted fresh (1794450065410).

The two failures:
```
FAIL start param order like legacy "?scope=email&response_type=code&state=…&redirect_uri=https%3A%2F%2Finstant-worker.leerosen686.workers.dev%2Fdash%2Foauth%2Fcallback&client_id=mock-google-client"
FAIL start cookie "__session=…; HttpOnly; Secure; Expires=Thu, 12 Nov 2026 03:21:06 GMT; Path=/dash/oauth; SameSite=Lax"
```
- The first check expects `&redirect_uri=http%3A%2F%2F`, but `BASE_URL` here is https.
- The second check's pattern has no `Secure` attribute, and the server adds `Secure` when served over https.

Both are assumptions in the script about a localhost http server. I didn't change these checks, because the instructions allowed only the auth-URL change.

Everything else passed, including:
- `start redirects to the auth url` (to the tunnel);
- `google got the form` (client id, secret, code and redirect_uri all reached the mock token endpoint);
- `callback redirects to the dashboard with a code + ticket`;
- `token`;
- the CLI claim/check flow.

## A5. Magic-code sign-in (log provider): pass

`EMAIL_PROVIDER` was unset; the server was `node-10`, version `f09d1394`, on a fresh app.

| step | status | time |
|---|---|---|
| `POST /runtime/auth/send_magic_code` `{"app-id","email":"soak@example.com"}` | 200, body `{"sent":true}` | 0.68 s |
| code in the tail | **yes**: `MAGIC CODE for soak@example.com: <CODE>` and `INFO instant_server::email: magic code for soak@example.com (app <app>): <CODE>` | |
| `POST /runtime/auth/verify_magic_code` `{"app-id","email","code"}` | 200, `{"user": {…, "refresh_token": "<REDACTED>"}, "created": true}` | 2.41 s |
| `POST /runtime/auth/verify_refresh_token` `{"app-id","refresh-token"}` | 200, `{"user": {…, "email": "soak@example.com", "isGuest": false, …}}` | 0.26 s |

## B. Soak

**Script:** `/tmp/iw-r3/soak.mjs`, against one fresh app on `node-10` (version `f09d1394`, the dashboard vars still set).

**Setup:** one transact created the attrs:
- `todos`: id, title (string), done (boolean), prio (number), due (date), all indexed; and note (untyped);
- `owners.name` and `tags.label`;
- links `todos.owner` (one) and `todos.tags` (many).

A second transact added 5 owners, 5 tags and 40 todos.

**Sessions:** 5 websocket sessions were opened, each subscribed to `{"todos":{}}` after `init`.

**Every 10 s**, a transact on a rotating session. Each transact was one `add-triple` that set either a todo's title (always a new value) or its prio (a random 0–6, which can equal the current value). The script timed transact → `transact-ok`, and transact → the next `refresh-ok` on the following session.

**Every 10 s, offset by 5 s**, an `add-query` with a new shape, followed by a `remove-query`. A shape is a unique combination of:
- 1–3 conditions from 22 types: `=`, `$like`, `$ilike`, `$in`, `$not`, `$gt`, `$gte`, `$lt`, `$lte` and `$isNull` on typed attrs; dotted link paths `owner.name` and `tags.label`; and `owner $isNull`;
- flat, `and` or `or`;
- one of 7 orders;
- limit or none;
- nested `owner`, `tags`, both or neither.

**Run:** started at 02:23:23 UTC, with setup done at 02:23:46. Planned for 1 h; stopped at 03:11:41 at the user's request (47 min 47 s of operations). The script was killed, so it wrote no `end` record; drops are logged per event, and there were none.

### B.1 Operations

| kind | attempted | succeeded | failed |
|---|---|---|---|
| transact | 287 | 287 | 0 |
| refresh (on another session) | 287 | 287 | 0 |
| add-query | 287 | 287 | 0 |
| remove-query | 287 | 287 | 0 |
| init (start) | 5 | 5 | 0 |

**Distinct error texts: none.** No error frames, timeouts or unsolicited errors were recorded.

### B.2 Latency per 30-minute window (ms: n / min / median / p95 / max)

| kind | 02:23:46–02:53:46 | 02:53:46–03:11:33 (partial) |
|---|---|---|
| transact → transact-ok | 180 / 379 / 424 / 530 / 950 | 107 / 391 / 437 / 511 / 706 |
| transact → refresh-ok on another session | 180 / 636 / 701 / 10670 / 20691 | 107 / 641 / 718 / 10757 / 20715 |
| add-query (new shape) → add-query-ok | 180 / 217 / 641 / 850 / 1060 | 107 / 214 / 605 / 872 / 1027 |
| remove-query → reply | 180 / 20 / 38 / 45 / 101 | 107 / 19 / 37 / 45 / 283 |

**Drift.** Medians per 10 minutes were flat:

| kind | 10-minute medians (ms) |
|---|---|
| transact | 430, 418, 421, 440, 432 |
| refresh | 707, 699, 693, 724, 703 |
| add-query | 677, 644, 585, 609, 596 |

**Refresh outliers.** Refresh times split into four groups:

| range | count |
|---|---|
| < 1 s | 256 |
| 1–5 s | 3 (1.03–1.18 s, all at 02:53:18–02:53:38, around the 30-minute checkpoint's psql and curl) |
| 5–15 s | 26 (all 10.64–10.88 s) |
| ≥ 15 s | 2 (20.69 and 20.72 s) |

The 28 slow values sit one or two 10-second ticks above the normal ~0.7 s. In those cases, the watched session received no `refresh-ok` within 10 s of that transact, and the wait ended with the next transact's refresh. They are spread over all 5 sessions (session 4: 9, 0: 7, 1: 7, 3: 5, 2: 3) and over the whole run.

**Not measured:** whether those transacts changed any value. A `prio` update can write the value already there, and the script did not record the old value. So whether the server skips a refresh for a write that changes nothing, or misses one, was not determined.

### B.3 Session drops and reconnects

None. All 5 sessions stayed open from setup to stop.

### B.4 Tail during the soak (02:23:22 – 03:13:33)

- **Boots: 0.** The object had booted at 1794450187578 (02:23:07, version `f09d1394`), before the soak, and did not boot again during it.
- **Warnings / errors / exceptions / "exceeded":** only `24 × WARN instant_server::webhooks: webhook delivery failed: Record not found: webhook` (see A2). No exceptions, no `exceeded` lines.
- **Event outcomes:**
  - the 5 session websockets each produced a Worker event and a Durable Object event; in both cases 4 ended `responseStreamDisconnected` and 1 ended `canceled`, when the soak was killed;
  - the 3 `/health` checkpoints produced 3 Worker events and 3 Durable Object events, all `ok`.
- **Tail gaps:** none. The restart loop recorded a single start and no exit until teardown.

### B.5 Database and `/health` over time

| checkpoint | pg_stat_activity (this database) | `/health` connect / TLS / TTFB / total |
|---|---|---|
| 0, 02:23:22 (before the soak) | 1 active, 9 idle | 0.009 / 0.023 / 0.424 / 0.424 s |
| 30 min, 02:53:23 | 1 active, 5 idle | 0.033 / 0.048 / 0.323 / 0.323 s |
| stop, 03:11:46 | 1 active, 6 idle | 0.119 / 0.143 / 0.457 / 0.457 s |

The `active` row includes the checkpoint's own `psql` query. The Neon console's memory and connection figures were not recorded (agreed: psql only).

### B.6 Query shapes

**287 distinct shapes were sent**, one per add-query; no shape repeated, and all 287 add-queries succeeded.

## What was not measured or not run

- **A2 and A3 fallbacks** (`INSTANT_WEBHOOK_ALLOW_INSECURE=1`, `INSTANT_OAUTH_ALLOW_PRIVATE=1`): not run, because the runs without the flags passed.
- **A2 attempt 1:** the source of `database error: … shutting down` and of the connection EOFs. The Neon compute did not restart.
- **A4:** the two failing checks were not adapted to https. Run 1's failure is a rollout artefact, not a server result.
- **Soak length:** the soak ran 48 min of the agreed 1 h, which the original request had at 3 h. It gives 2 windows, the second partial.
- **Refresh outliers (B.2):** whether the 28 delayed refreshes followed no-op writes.
- **Neon console figures:** not recorded.
- **Where the Durable Object runs:** not exposed, as in earlier rounds.
- **Per-step logs:** kept only on the test machine (`deploy-test-logs/round3/*.log`), as in round 2. The tunnel hostnames in them are throwaway.

## Teardown

Done 2026-11-12, about 03:19 UTC.

### Removed from Cloudflare

| What | Command | Result |
|---|---|---|
| Worker `instant-worker`, with its Durable Objects `node-9` and `node-10` and the secret `INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_SECRET` | `npx wrangler delete --force` | `Successfully deleted instant-worker`, exit 0 |
| Hyperdrive config `instant` (`101dc1c6db37417f80eead618c01e005`) | `npx wrangler hyperdrive delete 101dc1c6db37417f80eead618c01e005` | `✅ Deleted`, exit 0 |

Checks afterwards:
- `npx wrangler hyperdrive list` lists no configs.
- `npx wrangler secret list` exits 1, because the Worker no longer exists.
- `curl -i https://instant-worker.leerosen686.workers.dev/health` returns `HTTP/2 404`, body `error code: 1042`.

### Stopped

- The `wrangler tail` restart loop.
- The three quick tunnels (`cloudflared`, ports 9911, 9377 and 9378). `pgrep` finds no `cloudflared`, `wrangler tail` or soak process.

### Removed locally

- **`crates/instant-worker/wrangler.toml`:** restored to the committed version. The local copy held the Hyperdrive id, the test vars and the tunnel URLs, but not the connection string.
- **Raw data:** the raw tail and its restart markers, and the soak's per-operation log.
- **Tools:** the round's helper scripts, the `cloudflared` binary and its tunnel logs.
- **Script copies:** all of `/tmp/iw-r3` (the four script copies and their diffs).
- **Final scan:** `/usr/bin/grep -rIl` over the working tree, including gitignored files, and `git grep` on `HEAD` find no Neon host and no `npg_` password.

### Still existing

These are the same items as after rounds 1 and 2:
- **The Neon project**, which now also holds this round's test apps. Delete it in the Neon console.
- **`~/.iw-database-url`**, which holds the connection string (`rm ~/.iw-database-url`).
- **The `wrangler` OAuth login.**
- **The `postgres:17` Docker image.**
- **The Rust beta toolchain and the Emscripten cache.**
- **The worker's `target/` and `build/` directories.**
- **This round's per-step logs**, in `deploy-test-logs/round3/*.log` on the test machine only.
