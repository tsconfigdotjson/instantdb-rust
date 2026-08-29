#!/usr/bin/env bash
# Replays the legacy InstantDB migrations onto a plain Postgres database.
# Usage: DATABASE_URL=postgres://instant:instant@localhost:5432/instant ./scripts/apply-migrations.sh
set -euo pipefail
DATABASE_URL="${DATABASE_URL:-postgres://instant:instant@localhost:5432/instant}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
MIG="$DIR/LEGACY/server/resources/migrations"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -c \
  "CREATE TABLE IF NOT EXISTS schema_migrations (version int PRIMARY KEY, applied_at timestamptz DEFAULT now());"

for base in $(ls "$MIG" | grep '\.up\.sql$' | sort -n -t_ -k1); do
  f="$MIG/$base"

  ver=${base%%_*}
  applied=$(psql "$DATABASE_URL" -tA -c "SELECT 1 FROM schema_migrations WHERE version=$ver")
  if [ "$applied" = "1" ]; then continue; fi
  echo "applying $base"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -f "$f"
  psql "$DATABASE_URL" -q -c "INSERT INTO schema_migrations (version) VALUES ($ver)"
done
echo "done"
