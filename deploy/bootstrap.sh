#!/usr/bin/env bash
# One-time setup of a fresh Ubuntu (24.04+) VPS for the demo stack. Run as root:
#   curl -fsSL <raw url>/deploy/bootstrap.sh | bash -s -- '<deploy ssh public key>'
# Idempotent: safe to re-run.
set -euo pipefail
DEPLOY_KEY="${1:?usage: bootstrap.sh '<ssh public key for the deploy user>'}"
APP_DIR=/opt/instant

export DEBIAN_FRONTEND=noninteractive
apt-get update -q
# Docker from Ubuntu's archive: covered by unattended security upgrades and
# available on new releases before Docker's own repo supports them.
apt-get install -yq ca-certificates curl openssl ufw unattended-upgrades \
  docker.io docker-compose-v2
systemctl enable --now docker

# 2 GB swap: the memory backstop for a 4 GB box.
if ! swapon --show | grep -q /swapfile; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
cat > /etc/sysctl.d/99-instant.conf <<'SYSCTL'
vm.swappiness = 10
SYSCTL
sysctl --system -q

# Firewall: ssh + http(s) only. Postgres is never published by compose.
ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw allow 443/udp
ufw --force enable

# Deploy user: CI connects as this user; docker group lets it run compose.
id deploy >/dev/null 2>&1 || useradd -m -s /bin/bash deploy
usermod -aG docker deploy
install -d -m 700 -o deploy -g deploy /home/deploy/.ssh
touch /home/deploy/.ssh/authorized_keys
grep -qF "$DEPLOY_KEY" /home/deploy/.ssh/authorized_keys || echo "$DEPLOY_KEY" >> /home/deploy/.ssh/authorized_keys
chown deploy:deploy /home/deploy/.ssh/authorized_keys
chmod 600 /home/deploy/.ssh/authorized_keys

install -d -o deploy -g deploy "$APP_DIR" "$APP_DIR/backups"
if [ ! -f "$APP_DIR/.env" ]; then
  pw=$(openssl rand -hex 24)
  cat > "$APP_DIR/.env" <<ENV
SITE_DOMAIN=
DASH_DOMAIN=
API_DOMAIN=
ACME_EMAIL=
POSTGRES_PASSWORD=$pw
IMAGE_TAG=latest
IMAGE_REPO=ghcr.io/tsconfigdotjson/instantdb-rust
INSTANT_SUPERUSER_EMAIL=
INSTANT_DASHBOARD_SIGNUP_MODE=restricted
INSTANT_DASHBOARD_ALLOWED_EMAILS=
EMAIL_PROVIDER=log
INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_ID=
INSTANT_DASHBOARD_GOOGLE_OAUTH_CLIENT_SECRET=
ENV
  chown deploy:deploy "$APP_DIR/.env"
  chmod 600 "$APP_DIR/.env"
  echo ">> wrote $APP_DIR/.env with a generated POSTGRES_PASSWORD; fill in the domains/emails"
fi

# Nightly backup at 03:30 UTC.
echo "30 3 * * * deploy $APP_DIR/backup.sh >> $APP_DIR/backups/backup.log 2>&1" > /etc/cron.d/instant-backup

echo ">> bootstrap done. Next: fill $APP_DIR/.env, then run the Deploy workflow."
