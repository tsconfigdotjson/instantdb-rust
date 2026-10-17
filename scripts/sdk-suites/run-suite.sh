#!/usr/bin/env bash
# Runs one official SDK test suite against one LOCAL server and writes a
# vitest JSON report to $SDK_REPORT_DIR/<suite>-<server>.json.
#
#   run-suite.sh core  legacy http://localhost:8891
#   run-suite.sh cli   rust   http://localhost:8888
#
# Suites:
#   core — LEGACY/client/packages/core/__tests__/src/*.e2e.test.ts (vitest
#          browser mode, chromium) via core.vitest.config.ts
#   cli  — LEGACY/client/packages/cli/__tests__/e2e (instant-cli's e2e project)
#
# Exits with vitest's status; CI runs this with continue-on-error and lets
# compare.mjs decide. Assumes the SDK workspace is installed and built
# (pnpm install/build under LEGACY/client) and, for core, playwright chromium.
set -uo pipefail

SUITE="${1:?usage: run-suite.sh <core|cli> <server-label> <base-url>}"
SERVER="${2:?usage: run-suite.sh <core|cli> <server-label> <base-url>}"
URL="${3:?usage: run-suite.sh <core|cli> <server-label> <base-url>}"

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="${SDK_REPORT_DIR:-$ROOT/sdk-suite-reports}"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
REPORT="$OUT/$SUITE-$SERVER.json"
# vitest's exit status: the JSON report does not record unhandled errors
EXIT_FILE="$OUT/$SUITE-$SERVER.exit"
rm -f "$REPORT" "$EXIT_FILE"

# Both suites silently fall back to https://api.instantdb.com when not
# pointed elsewhere; only ever run them against a local server.
if ! [[ "$URL" =~ ^http://(localhost|127\.0\.0\.1):([0-9]+)$ ]]; then
  echo "refusing to run the $SUITE suite against '$URL': only http://localhost:<port> is allowed" >&2
  exit 2
fi
PORT="${BASH_REMATCH[2]}"

echo "== $SUITE suite vs $SERVER ($URL) -> $REPORT =="
case "$SUITE" in
  core)
    PKG="$ROOT/LEGACY/client/packages/core"
    # vitest resolves the config's imports from the config's own directory
    cp "$HERE/core.vitest.config.ts" "$PKG/vitest.sdk-suites.config.ts"
    cd "$PKG"
    SDK_SERVER_PORT="$PORT" pnpm exec vitest run \
      --config vitest.sdk-suites.config.ts \
      --reporter=default --reporter=json --outputFile.json="$REPORT"
    status=$?
    rm -f "$PKG/vitest.sdk-suites.config.ts"
    ;;
  cli)
    cd "$ROOT/LEGACY/client/packages/cli"
    INSTANT_CLI_API_URI="$URL" pnpm exec vitest run --project e2e \
      --reporter=default --reporter=json --outputFile.json="$REPORT"
    status=$?
    ;;
  *)
    echo "unknown suite '$SUITE' (expected core or cli)" >&2
    exit 2
    ;;
esac

echo "$status" > "$EXIT_FILE"
if [ ! -s "$REPORT" ]; then
  echo "no report written for $SUITE vs $SERVER (vitest exit $status)" >&2
fi
exit "$status"
