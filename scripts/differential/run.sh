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

# Every layer runs even when an earlier one fails, so one CI round reports
# them all; the script fails at the end if any did.
FAILED_LAYERS=""
layer() {
  local name="$1"; shift
  if ! "$@"; then
    FAILED_LAYERS="$FAILED_LAYERS $name"
    echo "LAYER FAILED: $name"
  fi
}

echo "== schema parity =="
layer schema node schema.mjs

echo "== differential replay =="
layer replay node replay.mjs "$APP_ID" "$APP_ID" "$TOKEN"

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
layer dash env DASH_USER_TOKEN="$DASH_USER_TOKEN" node dash.mjs "$DASH_APP" "$DASH_TOKEN" "$DASH_APP2" "$DASH_TOKEN2"

echo "== storage routes =="
STORAGE_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
STORAGE_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$STORAGE_APP" "$STORAGE_TOKEN"
layer storage node storage.mjs "$STORAGE_APP" "$STORAGE_TOKEN"

echo "== error matrix =="
ERR_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
ERR_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
ERR_USER_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$ERR_APP" "$ERR_TOKEN" "$ERR_USER_TOKEN"
layer errors node errors.mjs "$ERR_APP" "$ERR_TOKEN" "$ERR_USER_TOKEN"

echo "== scheduling stress =="
STRESS_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
STRESS_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$STRESS_APP" "$STRESS_TOKEN"
layer stress node stress.mjs "$STRESS_APP" "$STRESS_APP" "$STRESS_TOKEN" "${STRESS_TRIPLES:-400}"

echo "== fuzz layer =="
# FUZZ_SEEDS: fixed seeds (every run); FUZZ_SEED_WINDOW=N adds N more seeds
# from a window that moves every day (seeds 1000 + day*N ...), so the
# nightly keeps exploring new scripts while any failure stays reproducible
# from the seed it prints. Every seed runs; failures are listed at the end.
SEEDS="${FUZZ_SEEDS:-42 99}"
if [ "${FUZZ_SEED_WINDOW:-0}" -gt 0 ]; then
  DAY=$(( $(date -u +%s) / 86400 ))
  BASE=$(( 1000 + (DAY % 1000) * FUZZ_SEED_WINDOW ))
  SEEDS="$SEEDS $(seq -s ' ' "$BASE" $(( BASE + FUZZ_SEED_WINDOW - 1 )))"
fi
FUZZ_FAILED=""
for SEED in $SEEDS; do
  FUZZ_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
  FUZZ_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
  ./provision.sh "$FUZZ_APP" "$FUZZ_TOKEN" > /dev/null
  if ! node fuzz.mjs "$FUZZ_APP" "$FUZZ_APP" "$FUZZ_TOKEN" "$SEED" "${FUZZ_ROUNDS:-60}"; then
    FUZZ_FAILED="$FUZZ_FAILED $SEED"
  fi
done
# permissions + concurrency fuzz: generated rules, four concurrent sessions,
# subscriptions compared at settled checkpoints (fuzz-perms.mjs)
for SEED in ${FUZZ_PERMS_SEEDS:-7 8 9}; do
  PZ_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
  PZ_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
  ./provision.sh "$PZ_APP" "$PZ_TOKEN" > /dev/null
  if ! node fuzz-perms.mjs "$PZ_APP" "$PZ_APP" "$PZ_TOKEN" "$SEED" "${FUZZ_PERMS_ROUNDS:-120}"; then
    FUZZ_FAILED="$FUZZ_FAILED perms:$SEED"
  fi
done
if [ -n "$FUZZ_FAILED" ]; then
  echo "FUZZ FAILED for seeds:$FUZZ_FAILED (rerun one with: FUZZ_SEEDS=<seed> FUZZ_ROUNDS=${FUZZ_ROUNDS:-60} ./run.sh)"
  FAILED_LAYERS="$FAILED_LAYERS fuzz"
fi

if [ -n "$FAILED_LAYERS" ]; then
  echo "DIFFERENTIAL HARNESS FAILED:$FAILED_LAYERS"
  exit 1
fi

echo "== legacy surface coverage =="
node coverage.mjs --check "$COVERAGE_FILE"

echo "DIFFERENTIAL HARNESS PASSED"
