#!/usr/bin/env bash
# Nightly logical backup (cron, installed by bootstrap.sh). Keeps 7 days on
# the box — this protects against bad deploys / operator error, not disk
# loss. Copy /opt/instant/backups off-box for that.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p backups
out="backups/instant-$(date -u +%Y%m%dT%H%M%SZ).dump"
docker compose --env-file .env exec -T postgres pg_dump -U instant -Fc instant > "$out.tmp"
mv "$out.tmp" "$out"
find backups -name 'instant-*.dump' -mtime +7 -delete
