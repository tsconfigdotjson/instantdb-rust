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

echo "== differential replay =="
node replay.mjs "$APP_ID" "$APP_ID" "$TOKEN"

echo "== fuzz layer =="
for SEED in ${FUZZ_SEEDS:-42 99}; do
  FUZZ_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
  FUZZ_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
  ./provision.sh "$FUZZ_APP" "$FUZZ_TOKEN"
  node fuzz.mjs "$FUZZ_APP" "$FUZZ_APP" "$FUZZ_TOKEN" "$SEED" "${FUZZ_ROUNDS:-60}"
done

echo "DIFFERENTIAL HARNESS PASSED"
