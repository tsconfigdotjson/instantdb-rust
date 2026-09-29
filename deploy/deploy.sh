#!/usr/bin/env bash
# Rolls the stack to IMAGE_TAG. Runs on the VPS from /opt/instant; the deploy
# workflow invokes it over ssh after copying this directory up.
#   IMAGE_TAG=<sha> ./deploy.sh      # or with no tag: whatever .env says
set -euo pipefail
cd "$(dirname "$0")"

if [ -n "${IMAGE_TAG:-}" ]; then
  sed -i "s/^IMAGE_TAG=.*/IMAGE_TAG=${IMAGE_TAG}/" .env
fi

mkdir -p site

dc() { docker compose --env-file .env "$@"; }

dc pull --quiet server dashboard caddy postgres
dc up -d postgres
dc --profile migrate run --rm migrate
dc up -d --remove-orphans server dashboard caddy
# The Caddyfile is bind-mounted, so an edit alone doesn't restart Caddy.
dc exec -T caddy caddy reload --config /etc/caddy/Caddyfile

# Wait for the server to report healthy before calling the deploy good.
for _ in $(seq 1 30); do
  status=$(docker inspect -f '{{.State.Health.Status}}' "$(dc ps -q server)")
  [ "$status" = healthy ] && break
  sleep 2
done
if [ "$status" != healthy ]; then
  echo "server did not become healthy" >&2
  dc logs --tail 100 server >&2
  exit 1
fi

docker image prune -af --filter "until=168h" >/dev/null
echo "deployed $(grep '^IMAGE_TAG=' .env | cut -d= -f2)"
