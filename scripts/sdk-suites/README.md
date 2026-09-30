# Official SDK suites vs legacy + rust

The `sdk-suites` CI job runs the vendored Instant SDKs' own
runtime test suites, unchanged, against **both** the legacy server (the
official self-hosting images from `scripts/differential/docker-compose.yml`,
host :8891) and this repo's rust server (:8888), then compares the outcomes
test by test.

| suite | tests | pointed at a server via |
|---|---|---|
| `core` | `LEGACY/client/packages/core/__tests__/src/*.e2e.test.ts` (vitest browser mode, playwright chromium) | `core.vitest.config.ts` defines `__DEV_LOCAL_PORT__` from `SDK_SERVER_PORT` |
| `cli` | `LEGACY/client/packages/cli/__tests__/e2e` (instant-cli's `e2e` project) | `INSTANT_CLI_API_URI` |

Both suites create their apps through `POST /dash/apps/ephemeral`.

Both suites fall back to **production** (`https://api.instantdb.com`) when not
pointed elsewhere — core's upstream config does so whenever `CI` is set.
`core.vitest.config.ts` throws when `SDK_SERVER_PORT` is missing or invalid,
and `run-suite.sh` refuses any URL other than `http://localhost:<port>` /
`http://127.0.0.1:<port>`.

## Pieces

- `run-suite.sh <core|cli> <server-label> <base-url>` — runs one suite against
  one server with the default reporter plus a JSON report at
  `$SDK_REPORT_DIR/<suite>-<server>.json` (default `./sdk-suite-reports`) and
  vitest's exit status beside it (`.exit`). For `core` it copies
  `core.vitest.config.ts` into the core package for the run (vitest resolves
  the config's imports from its own directory) and removes it afterwards.
- `compare.mjs [--dir DIR] [--suites core,cli] [--allowed FILE]` — lists
  every test with its outcome on each server and fails when a report is
  missing or empty, a test's outcome differs between legacy and rust
  (including a test only one side ran), a test fails on both servers, or
  vitest exited non-zero without a failing test in its report (unhandled
  errors). Module-level errors appear as a `<file> > (file)` row. Writes a
  Markdown table to `$GITHUB_STEP_SUMMARY` when set.
- `allowed.json` — empty by default. A failure shared with legacy is still a
  broken SDK path, so exemptions must be explicit:
  `{"suite": "core", "test": "<id exactly as compare.mjs prints it>", "reason": "..."}`.

In CI each suite run is `continue-on-error` so all four runs happen; the
comparator step decides, and the reports are uploaded as the
`sdk-suite-reports` artifact.

## Running locally

With the rust server on :8888, the legacy stack up
(`docker compose -f scripts/differential/docker-compose.yml up -d`), and the
SDKs installed and built under `LEGACY/client`
(`pnpm install --filter instant-local-monorepo --filter "@instantdb/core..." --filter "instant-cli..."`,
then `pnpm --filter "@instantdb/core..." --filter "instant-cli..." run build`,
and `pnpm exec playwright install chromium` in `packages/core`):

```
for s in core cli; do
  scripts/sdk-suites/run-suite.sh $s legacy http://localhost:8891
  scripts/sdk-suites/run-suite.sh $s rust   http://localhost:8888
done
node scripts/sdk-suites/compare.mjs
```

## Not covered

- `@instantdb/react` and `@instantdb/admin` ship no runtime test suites (only
  `tsc` type tests); the admin SDK is exercised by `scripts/admin-sdk-test.mjs`
  in the `cargo test + e2e` job.
- `@instantdb/platform`'s `schema.push` e2e and `@instantdb/mcp`'s e2e build
  their clients through `PlatformApi` with production URLs hardcoded; they
  cannot target a local server without editing vendored code, so they are
  not run.
- core's `node` project (unit tests, no server) is not a server test.
