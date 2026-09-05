#!/usr/bin/env bash
# Boots the legacy server stack, provisions matching apps on the legacy and
# rust servers, and runs the differential replay + fuzz layers.
# Assumes the rust server is already running (RUST_URL, default :8888) against
# RUST_DATABASE_URL.
set -euo pipefail
cd "$(dirname "$0")"

RUST_URL="${RUST_URL:-http://localhost:8888}"
LEGACY_URL="${LEGACY_URL:-http://localhost:8891}"
KEEP="${KEEP:-0}"

echo "== legacy surface manifest =="
node surface.mjs --check
export COVERAGE_FILE="${COVERAGE_FILE:-$(pwd)/.coverage.jsonl}"
rm -f "$COVERAGE_FILE"

echo "== booting legacy stack =="
docker compose up -d --quiet-pull

cleanup() {
  if [ "$KEEP" != "1" ]; then
    docker compose down -v > /dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "== waiting for legacy server =="
for i in $(seq 1 180); do
  if curl -sf "$LEGACY_URL/" > /dev/null 2>&1; then
    echo "legacy server is up"
    break
  fi
  if [ "$i" = "180" ]; then
    echo "legacy server failed to come up"; docker compose logs legacy-server | tail -50; exit 1
  fi
  sleep 2
done

echo "== provisioning =="
APP_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$APP_ID" "$TOKEN"

echo "== schema parity =="
node schema.mjs

echo "== differential replay =="
node replay.mjs "$APP_ID" "$APP_ID" "$TOKEN"

echo "== dashboard/CLI routes =="
DASH_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
DASH_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
DASH_APP2=$(python3 -c "import uuid; print(uuid.uuid4())")
DASH_TOKEN2=$(python3 -c "import uuid; print(uuid.uuid4())")
# a dashboard refresh token for the first app's creator: the CLI's app /
# info / claim / auth routes take one instead of the admin token
DASH_USER_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$DASH_APP" "$DASH_TOKEN" "$DASH_USER_TOKEN"
./provision.sh "$DASH_APP2" "$DASH_TOKEN2"
DASH_USER_TOKEN="$DASH_USER_TOKEN" node dash.mjs "$DASH_APP" "$DASH_TOKEN" "$DASH_APP2" "$DASH_TOKEN2"

echo "== storage routes =="
STORAGE_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
STORAGE_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$STORAGE_APP" "$STORAGE_TOKEN"
node storage.mjs "$STORAGE_APP" "$STORAGE_TOKEN"

echo "== error matrix =="
ERR_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
ERR_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
ERR_USER_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$ERR_APP" "$ERR_TOKEN" "$ERR_USER_TOKEN"
node errors.mjs "$ERR_APP" "$ERR_TOKEN" "$ERR_USER_TOKEN"

echo "== scheduling stress =="
STRESS_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
STRESS_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$STRESS_APP" "$STRESS_TOKEN"
node stress.mjs "$STRESS_APP" "$STRESS_APP" "$STRESS_TOKEN" "${STRESS_TRIPLES:-400}"

echo "== fuzz layer =="
for SEED in ${FUZZ_SEEDS:-42 99}; do
  FUZZ_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
  FUZZ_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
  ./provision.sh "$FUZZ_APP" "$FUZZ_TOKEN"
  node fuzz.mjs "$FUZZ_APP" "$FUZZ_APP" "$FUZZ_TOKEN" "$SEED" "${FUZZ_ROUNDS:-60}"
done

echo "== legacy surface coverage =="
node coverage.mjs --check "$COVERAGE_FILE"

echo "DIFFERENTIAL HARNESS PASSED"
