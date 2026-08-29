#!/usr/bin/env bash
# Creates an app + admin token for local development.
# Usage: ./scripts/create-app.sh [title] ; prints app id and admin token.
set -euo pipefail
DATABASE_URL="${DATABASE_URL:-postgres://instant:instant@localhost:5432/instant}"
TITLE="${1:-dev app}"
APP_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
TOKEN=$(python3 -c "import uuid; print(uuid.uuid4())")
USER_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
psql "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<SQL
INSERT INTO instant_users (id, email) VALUES ('$USER_ID', 'dev-$USER_ID@example.com');
INSERT INTO apps (id, creator_id, title) VALUES ('$APP_ID', '$USER_ID', '$TITLE');
INSERT INTO app_admin_tokens (app_id, token) VALUES ('$APP_ID', '$TOKEN');
SQL
echo "app_id=$APP_ID"
echo "admin_token=$TOKEN"
