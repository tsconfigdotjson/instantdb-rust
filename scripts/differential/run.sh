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

# CI spreads the layers over parallel legs (the `differential` matrix in
# ci.yml). LAYERS="replay dash ..." runs only the named layers (default: all);
# SHARD=k/n runs only every n-th run of each layer, starting at the k-th (a
# seeded layer runs once per seed, an unseeded one once, so in shard 1).
# LAYER_SHARDS="presence:1/3 race:2/3" overrides the shard for individual
# layers, so one leg can spread expensive seeds of different layers apart (CI
# uses this to keep every seeded leg near the same measured cost). With
# RUNS_DIR set, every run the full script would make is appended to
# $RUNS_DIR/planned and every run this invocation makes to $RUNS_DIR/ran;
# legs.sh checks that the legs together made each planned run exactly once.
LAYERS="${LAYERS:-}"
SHARD="${SHARD:-1/1}"
SHARD_K="${SHARD%/*}"
SHARD_N="${SHARD#*/}"
if ! [[ "$SHARD" =~ ^[0-9]+/[0-9]+$ ]] || [ "$SHARD_K" -lt 1 ] || [ "$SHARD_K" -gt "$SHARD_N" ]; then
  echo "SHARD must be k/n with 1 <= k <= n, got: $SHARD"; exit 1
fi
LAYER_SHARDS="${LAYER_SHARDS:-}"
for spec in $LAYER_SHARDS; do
  if [[ "$spec" != *:* ]]; then
    echo "LAYER_SHARDS entries must be <layer>:k/n, got: $spec"; exit 1
  fi
  lk="${spec#*:}"; lk="${lk%/*}"; ln="${spec##*/}"
  if ! [[ "${spec#*:}" =~ ^[0-9]+/[0-9]+$ ]] || [ "$lk" -lt 1 ] || [ "$lk" -gt "$ln" ]; then
    echo "LAYER_SHARDS entries must be <layer>:k/n with 1 <= k <= n, got: $spec"; exit 1
  fi
done
RUNS_DIR="${RUNS_DIR:-}"
if [ -n "$RUNS_DIR" ]; then
  mkdir -p "$RUNS_DIR"
  rm -f "$RUNS_DIR/planned" "$RUNS_DIR/ran"
fi
declare -A RUN_COUNT=()
# want <layer>[:<seed>]: whether this invocation makes that run
want() {
  local base="${1%%:*}" i k="$SHARD_K" n="$SHARD_N" spec override
  i="${RUN_COUNT[$base]:-0}"
  RUN_COUNT[$base]=$((i + 1))
  if [ -n "$RUNS_DIR" ]; then echo "$1" >> "$RUNS_DIR/planned"; fi
  if [ -n "$LAYERS" ] && [[ " $LAYERS " != *" $base "* ]]; then return 1; fi
  for spec in $LAYER_SHARDS; do
    if [ "${spec%%:*}" = "$base" ]; then
      override="${spec#*:}"
      k="${override%/*}"
      n="${override#*/}"
    fi
  done
  if [ $((i % n)) -ne $((k - 1)) ]; then return 1; fi
  if [ -n "$RUNS_DIR" ]; then echo "$1" >> "$RUNS_DIR/ran"; fi
}

echo "== legacy surface manifest =="
node surface.mjs --check
echo "== differential frame-wait self-test =="
node wait-frames.test.mjs
echo "== differential cancelled-refresh self-test =="
node cancelled-refresh.test.mjs
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
  if ! want "$name"; then
    echo "(not in this leg: $name)"
    return 0
  fi
  if ! "$@"; then
    FAILED_LAYERS="$FAILED_LAYERS $name"
    echo "LAYER FAILED: $name"
  fi
}

echo "== schema parity =="
layer schema node schema.mjs

echo "== differential replay =="
layer replay node replay.mjs "$APP_ID" "$APP_ID" "$TOKEN"

echo "== ref-rule invalidation =="
# subscriptions under data.ref / auth.ref rules, refreshed by ref-target
# writes only (issue #59)
RT_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
RT_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$RT_APP" "$RT_TOKEN"
layer reftopics node ref-topics.mjs "$RT_APP" "$RT_TOKEN"

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
echo "== rule-where rewriter =="
# legacy's rule-wheres flag on for this app only (rule-wheres.mjs)
RW_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
RW_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
./provision.sh "$RW_APP" "$RW_TOKEN"
layer rulewheres node rule-wheres.mjs "$RW_APP" "$RW_TOKEN"

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

echo "== concurrent transact conflicts =="
# rust-only (issue #53): overlapping concurrent transacts never get a 5xx
for SEED in ${TX_CONFLICT_SEEDS:-1 2 3}; do
  want "conflicts:$SEED" || continue
  TC_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
  TC_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
  ./provision.sh "$TC_APP" "$TC_TOKEN" > /dev/null
  if ! node tx-conflicts.mjs "$TC_APP" "$TC_TOKEN" "$SEED" "${TX_CONFLICT_TXS:-60}"; then
    FAILED_LAYERS="$FAILED_LAYERS conflicts:$SEED"
    echo "LAYER FAILED: conflicts:$SEED"
  fi
done

echo "== fuzz layer =="
# FUZZ_SEEDS: fixed seeds (every run); FUZZ_SEED_WINDOW=N adds N more seeds
# from a window that moves every day (seeds 1000 + day*N ...), so the
# nightly keeps exploring new scripts while any failure stays reproducible
# from the seed it prints. Every seed runs; failures are listed at the end.
# FUZZ_SEED_DAY pins the day (CI legs use the workflow run's creation day, so
# legs that straddle midnight still split one window).
SEEDS="${FUZZ_SEEDS:-42 99}"
if [ "${FUZZ_SEED_WINDOW:-0}" -gt 0 ]; then
  DAY="${FUZZ_SEED_DAY:-$(( $(date -u +%s) / 86400 ))}"
  BASE=$(( 1000 + (DAY % 1000) * FUZZ_SEED_WINDOW ))
  SEEDS="$SEEDS $(seq -s ' ' "$BASE" $(( BASE + FUZZ_SEED_WINDOW - 1 )))"
fi
FUZZ_FAILED=""
for SEED in $SEEDS; do
  want "fuzz:$SEED" || continue
  FUZZ_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
  FUZZ_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
  ./provision.sh "$FUZZ_APP" "$FUZZ_TOKEN" > /dev/null
  if ! node fuzz.mjs "$FUZZ_APP" "$FUZZ_APP" "$FUZZ_TOKEN" "$SEED" "${FUZZ_ROUNDS:-60}"; then
    FUZZ_FAILED="$FUZZ_FAILED $SEED"
  fi
done
# Seeded layers run on their own apps, so several seeds can run at once. Each
# *_JOBS knob caps how many seeds of that layer run concurrently (default 1:
# unchanged serial behaviour). Their logs are buffered and printed in seed
# order so a failure stays readable.
seeded_seed() {
  local layer="$1" seed="$2" app token
  app=$(python3 -c "import uuid; print(uuid.uuid4())")
  token=$(python3 -c "import uuid; print(uuid.uuid4())")
  ./provision.sh "$app" "$token" > /dev/null || return 1
  case "$layer" in
    fuzzschema) node fuzz-schema.mjs "$app" "$app" "$token" "$seed" "${FUZZ_SCHEMA_ROUNDS:-200}" ;;
    perms) node fuzz-perms.mjs "$app" "$app" "$token" "$seed" "${FUZZ_PERMS_ROUNDS:-120}" ;;
    presence) node fuzz-presence.mjs "$app" "$app" "$token" "$seed" "${FUZZ_PRESENCE_ROUNDS:-100}" ;;
    storagefuzz) node fuzz-storage.mjs "$app" "$app" "$token" "$seed" "${FUZZ_STORAGE_ROUNDS:-80}" ;;
    syncfuzz) node fuzz-sync.mjs "$app" "$app" "$token" "$seed" "${FUZZ_SYNC_ROUNDS:-60}" ;;
    streamfuzz) node fuzz-streams.mjs "$app" "$app" "$token" "$seed" "${FUZZ_STREAMS_ROUNDS:-30}" ;;
    webhookfuzz) node fuzz-webhooks.mjs "$app" "$app" "$token" "$seed" "${FUZZ_WEBHOOKS_ROUNDS:-18}" ;;
    authfuzz) node fuzz-auth.mjs "$app" "$app" "$token" "$seed" "${FUZZ_AUTH_ROUNDS:-40}" ;;
    schemapush) node fuzz-schema-push.mjs "$app" "$app" "$token" "$seed" "${FUZZ_SCHEMAPUSH_ROUNDS:-30}" ;;
    *) echo "unknown seeded layer: $layer"; return 1 ;;
  esac
}
run_seeded_layer() {
  local layer="$1" jobs="$2" seed logs run=""
  shift 2
  logs=$(mktemp -d)
  for seed in "$@"; do
    want "$layer:$seed" || continue
    run="$run $seed"
    # Log a start marker now: the seed's own output is buffered until it
    # finishes, so this is what per-seed CI timing is read from.
    echo "$layer: seed=$seed starting"
    (
      if seeded_seed "$layer" "$seed" > "$logs/$seed.log" 2>&1; then
        echo 0 > "$logs/$seed.rc"
      else
        echo 1 > "$logs/$seed.rc"
      fi
    ) &
    while [ "$(jobs -rp | wc -l)" -ge "$jobs" ]; do wait -n || true; done
  done
  wait
  for seed in $run; do
    cat "$logs/$seed.log"
    if [ "$(cat "$logs/$seed.rc" 2>/dev/null)" != "0" ]; then
      FUZZ_FAILED="$FUZZ_FAILED $layer:$seed"
    fi
  done
  rm -rf "$logs"
}
# schema-evolution fuzz: inline add-attr / update-attr / delete-attr /
# restore-attr interleaved with data writes, one-shot queries (where / order /
# pagination) and live subscriptions, with admin and non-admin sessions
# (fuzz-schema.mjs). Every transact outcome (including the full error envelope),
# query result, settled subscription state and the attrs seen in init-ok /
# refresh-ok are compared; a legacy session whose cache has not caught up is
# accepted only when a fresh legacy probe agrees with rust.
SCHEMA_SEEDS="${FUZZ_SCHEMA_SEEDS:-31 32}"
run_seeded_layer fuzzschema "${FUZZ_SCHEMA_JOBS:-1}" $SCHEMA_SEEDS
# permissions + concurrency fuzz: generated rules, four concurrent sessions,
# subscriptions compared at settled checkpoints (fuzz-perms.mjs). A seed is
# mostly waiting for streams to go quiet, so its seeds are the most parallel
# layer: one per CI leg at PR depth and four at a time at nightly depth.
PERMS_SEEDS="${FUZZ_PERMS_SEEDS:-7 8 9}"
run_seeded_layer perms "${FUZZ_PERMS_JOBS:-1}" $PERMS_SEEDS
# rooms / presence fuzz: concurrent old and new clients join / leave / set
# presence / broadcast over several rooms, with re-joins, invalid ops and
# /admin/rooms/presence reads (fuzz-presence.mjs). Each seed folds the settled
# client state and compares every ack / error / broadcast / admin shape.
PRESENCE_SEEDS="${FUZZ_PRESENCE_SEEDS:-101 102 103}"
run_seeded_layer presence "${FUZZ_PRESENCE_JOBS:-1}" $PRESENCE_SEEDS
# storage / $files fuzz: admin and client uploads, signed upload consumption,
# overwrites, odd / blank paths, content-type / disposition variants, deletes,
# signed downloads, $files queries / pagination / links, impersonated admin
# calls and generated $files rules (fuzz-storage.mjs). Every HTTP response,
# query result, download fetch and settled subscription state is compared.
# A seed is mostly waiting on uploads/downloads, so run several at once.
STORAGE_SEEDS="${FUZZ_STORAGE_SEEDS:-201 202}"
run_seeded_layer storagefuzz "${FUZZ_STORAGE_JOBS:-1}" $STORAGE_SEEDS
# sync-table fuzz: seeded start-sync over every supported order, random
# creates / updates / deletes / links from several sessions, view rules,
# remove-sync, reconnects with resync-table from the last seen tx, an attr
# added after a sub started, and a >100-tx burst (fuzz-sync.mjs). Every sync
# frame, its batching and tx-id watermarks, and the folded client store are
# compared. The three PR seeds (and all ten nightly seeds) ride the replay leg.
SYNC_SEEDS="${FUZZ_SYNC_SEEDS:-301 302 308}"
run_seeded_layer syncfuzz "${FUZZ_SYNC_JOBS:-1}" $SYNC_SEEDS
# stream fuzz: seeded create / append / done / abort / subscribe from an
# offset / reconnect / unsubscribe sequences over client, signed-in and admin
# writers under generated $streams rules, with late and concurrent readers,
# unicode / empty / multi-chunk / >1MB appends, a writer resume after a flush
# and every invalid parameter envelope (fuzz-streams.mjs). The folded reader
# byte stream, delivery projections, writer flush watermarks and acks are
# compared. The two cheap PR seeds (and all eight nightly seeds) ride the
# replay leg with the sync seeds.
STREAM_SEEDS="${FUZZ_STREAMS_SEEDS:-401 402}"
run_seeded_layer streamfuzz "${FUZZ_STREAMS_JOBS:-1}" $STREAM_SEEDS
# webhook event production: generated namespaces / action subsets, then
# creates, updates, merges, deletes (including missing and cascades), link /
# unlink, multi-entity and non-webhook namespaces, and lookup upserts. Every
# transaction compares the set of events each webhook queued on legacy vs rust
# and each new event's decoded payload records (fuzz-webhooks.mjs). The two
# cheap PR seeds ride the seed legs; nightly spreads one per seed leg.
WEBHOOK_SEEDS="${FUZZ_WEBHOOKS_SEEDS:-501 502}"
run_seeded_layer webhookfuzz "${FUZZ_WEBHOOKS_JOBS:-1}" $WEBHOOK_SEEDS
# auth-flow fuzz: guest / magic-code / refresh-token / signout / admin user
# sequences under generated $users create / view rules (fuzz-auth.mjs). The
# fixed prefix always covers wrong / reused / expired codes, guest upgrades,
# signout by email / id / token, deleted-user tokens and malformed parameters.
AUTH_SEEDS="${FUZZ_AUTH_SEEDS:-501 502 503}"
run_seeded_layer authfuzz "${FUZZ_AUTH_JOBS:-1}" $AUTH_SEEDS
# server-side schema planner fuzz (`schema/push/{plan,apply}`, what
# @instantdb/platform's schemaPush calls): generated schema evolutions from the
# pulled schema each round, valid and invalid, both check_types /
# supports_background_updates values, interleaved data writes so the indexing
# jobs run against real data (fuzz-schema-push.mjs). The plan steps, error
# envelopes, apply results, settled jobs and pulled schema are compared.
SCHEMAPUSH_SEEDS="${FUZZ_SCHEMAPUSH_SEEDS:-601 602 603}"
run_seeded_layer schemapush "${FUZZ_SCHEMAPUSH_JOBS:-1}" $SCHEMAPUSH_SEEDS
# subscription race fuzz (issue #51): rust-only invariants for queries
# registered while transactions are in flight, against a rust server booted
# with INSTANT_CHAOS_DELAY_MS (RACE_URL; defaults to RUST_URL)
for SEED in ${FUZZ_RACE_SEEDS:-1 2 3}; do
  want "race:$SEED" || continue
  RZ_APP=$(python3 -c "import uuid; print(uuid.uuid4())")
  RZ_TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
  ./provision.sh "$RZ_APP" "$RZ_TOKEN" > /dev/null
  if ! RACE_URL="${RACE_URL:-$RUST_URL}" node fuzz-race.mjs "$RZ_APP" "$RZ_TOKEN" "$SEED" "${FUZZ_RACE_ROUNDS:-30}"; then
    FUZZ_FAILED="$FUZZ_FAILED race:$SEED"
  fi
done
if [ -n "$FUZZ_FAILED" ]; then
  echo "FUZZ FAILED for seeds:$FUZZ_FAILED"
  echo "  (rerun a fuzz seed with FUZZ_SEEDS=<seed> FUZZ_ROUNDS=${FUZZ_ROUNDS:-60}, a presence seed with FUZZ_PRESENCE_SEEDS=<seed> FUZZ_PRESENCE_ROUNDS=${FUZZ_PRESENCE_ROUNDS:-100}, a schema seed with FUZZ_SCHEMA_SEEDS=<seed> FUZZ_SCHEMA_ROUNDS=${FUZZ_SCHEMA_ROUNDS:-200}, a storage seed with FUZZ_STORAGE_SEEDS=<seed> FUZZ_STORAGE_ROUNDS=${FUZZ_STORAGE_ROUNDS:-80}, a sync seed with FUZZ_SYNC_SEEDS=<seed> FUZZ_SYNC_ROUNDS=${FUZZ_SYNC_ROUNDS:-60}, a stream seed with FUZZ_STREAMS_SEEDS=<seed> FUZZ_STREAMS_ROUNDS=${FUZZ_STREAMS_ROUNDS:-30}, a webhook seed with FUZZ_WEBHOOKS_SEEDS=<seed> FUZZ_WEBHOOKS_ROUNDS=${FUZZ_WEBHOOKS_ROUNDS:-18}, an auth seed with FUZZ_AUTH_SEEDS=<seed> FUZZ_AUTH_ROUNDS=${FUZZ_AUTH_ROUNDS:-40}, a schema-push seed with FUZZ_SCHEMAPUSH_SEEDS=<seed> FUZZ_SCHEMAPUSH_ROUNDS=${FUZZ_SCHEMAPUSH_ROUNDS:-30}; LAYERS narrows the run)"
  FAILED_LAYERS="$FAILED_LAYERS fuzz"
fi

if [ -n "$FAILED_LAYERS" ]; then
  echo "DIFFERENTIAL HARNESS FAILED:$FAILED_LAYERS"
  exit 1
fi

if [ -n "$LAYERS" ] || [ "$SHARD" != "1/1" ] || [ -n "$LAYER_SHARDS" ]; then
  # a part of the harness covers part of the surface: CI checks the baseline
  # against the union of every leg's records (legs.sh)
  echo "DIFFERENTIAL HARNESS PASSED (LAYERS=${LAYERS:-all} SHARD=$SHARD LAYER_SHARDS=${LAYER_SHARDS:-none}; coverage is checked across legs)"
  exit 0
fi

echo "== legacy surface coverage =="
node coverage.mjs --check "$COVERAGE_FILE"

echo "DIFFERENTIAL HARNESS PASSED"
