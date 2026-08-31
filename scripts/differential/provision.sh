#!/usr/bin/env bash
# Provisions the SAME app id + admin token on both servers' databases so the
# differential harness can replay identical op scripts against each.
# Usage: ./provision.sh <app-id> <admin-token>
# Env: RUST_DATABASE_URL (default postgres://instant:instant@localhost:5432/instant)
#      LEGACY_DATABASE_URL (default postgres://instant:instant@localhost:8890/instant)
set -euo pipefail
APP_ID="$1"
TOKEN="$2"
RUST_DATABASE_URL="${RUST_DATABASE_URL:-postgres://instant:instant@localhost:5432/instant}"
LEGACY_DATABASE_URL="${LEGACY_DATABASE_URL:-postgres://instant:instant@localhost:8890/instant}"

for url in "$RUST_DATABASE_URL" "$LEGACY_DATABASE_URL"; do
  USER_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
  psql "$url" -q -v ON_ERROR_STOP=1 <<SQL
INSERT INTO instant_users (id, email) VALUES ('$USER_ID', 'diff-$USER_ID@example.com');
INSERT INTO apps (id, creator_id, title) VALUES ('$APP_ID', '$USER_ID', 'differential app');
INSERT INTO app_admin_tokens (app_id, token) VALUES ('$APP_ID', '$TOKEN');
SQL
done
echo "provisioned app $APP_ID on both servers"
