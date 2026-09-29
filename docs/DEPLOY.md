# Deploying the public demo

The demo is one VPS (4 GB RAM) running the whole stack under Docker Compose:

```
Caddy (TLS) ─┬─ $SITE_DOMAIN → marketing site (web/, static files in /opt/instant/site)
             ├─ $DASH_DOMAIN → dashboard  (LEGACY/client/www, self-hosted Next.js)
             └─ $API_DOMAIN  → instant-server
                                  │
                              Postgres 17 (deploy/postgresql.conf)
```

The dashboard and the API need separate hostnames: the dashboard's own pages
live under `/dash`, which is also the server's dashboard API prefix.

Everything in `deploy/` is what runs on the box (`/opt/instant`):

| file | |
|---|---|
| `compose.yml` | the stack; images come from GHCR, tagged by commit sha |
| `postgresql.conf` | Postgres tuned for the 4 GB box (memory budget in the header) |
| `Caddyfile` | automatic Let's Encrypt TLS, websocket/SSE proxying, `/metrics` hidden |
| `deploy.sh` | pull → migrate → restart → wait for health; run by CI |
| `backup.sh` | nightly `pg_dump`, 7 days kept in `/opt/instant/backups` |
| `bootstrap.sh` | one-time host setup (Docker, swap, firewall, `deploy` user, cron) |
| `.env.example` | the settings `/opt/instant/.env` holds (secrets never enter the repo) |

## Pipeline

`.github/workflows/deploy.yml` runs after **CI passes on `main`** (or by hand
from the Actions tab). It builds `instantdb-rust-server` and
`instantdb-rust-dashboard` images for `linux/amd64`, pushes them to GHCR as
`:<sha>` and `:latest`, builds the marketing site (`web/`), copies `deploy/`
and the site to the VPS and runs `deploy.sh` with that sha. Rolling back is
re-running the workflow on an older commit, or on the box:
`IMAGE_TAG=<old sha> /opt/instant/deploy.sh` (that rolls the images; the site
only changes on a workflow run).

The dashboard is never built on the VPS: a Next.js build needs more memory
than the box can spare.

## First-time setup

1. **DNS**: A/AAAA records for `$SITE_DOMAIN`, `www.$SITE_DOMAIN`,
   `$DASH_DOMAIN` and `$API_DOMAIN` → the VPS. On Cloudflare, leave them
   DNS-only (grey cloud): Caddy gets its own certificates.
2. **Deploy key**: `ssh-keygen -t ed25519 -f instant-deploy -N ''`.
3. **Bootstrap** (as root on a fresh Ubuntu 24.04 or newer):
   ```sh
   bash bootstrap.sh "$(cat instant-deploy.pub)"
   ```
   It writes `/opt/instant/.env` with a generated `POSTGRES_PASSWORD`.
   Fill in the domains, `ACME_EMAIL`, `INSTANT_SUPERUSER_EMAIL` and
   `INSTANT_DASHBOARD_ALLOWED_EMAILS`.
4. **GitHub** (Settings → Environments → `production`):
   - variables `VPS_HOST` (ip or hostname), `API_DOMAIN`
   - secrets `VPS_SSH_KEY` (contents of `instant-deploy`),
     `VPS_KNOWN_HOSTS` (`ssh-keyscan <host>` output)
5. Run **Actions → Deploy → Run workflow**.

## Operating

```sh
cd /opt/instant
docker compose ps
docker compose logs -f server
docker compose logs server | grep -i code      # magic codes (EMAIL_PROVIDER=log)
docker compose exec postgres psql -U instant   # database shell
```

There is no email provider yet. Magic codes are printed to the server log,
which is fine while signup is `restricted` to the operator. Before signup goes
`open`, configure dashboard Google login (`INSTANT_DASHBOARD_GOOGLE_OAUTH_*`,
redirect URI `https://$API_DOMAIN/dash/oauth/callback`).

Changing `.env` takes effect on the next deploy, or immediately with
`docker compose up -d`.

## Manual deploy without CI

The GHCR packages are private, so the box needs a token with
`read:packages` to pull by hand:

```sh
echo <token> | docker login ghcr.io -u <github user> --password-stdin
IMAGE_TAG=<sha> /opt/instant/deploy.sh
```

## Trying the stack locally

```sh
docker build -t ghcr.io/tsconfigdotjson/instantdb-rust-server:local .
docker build -f LEGACY/client/Dockerfile.www -t ghcr.io/tsconfigdotjson/instantdb-rust-dashboard:local LEGACY/client
cd deploy && cp .env.example .env    # set DASH_DOMAIN=dash.localhost, API_DOMAIN=api.localhost,
                                     # POSTGRES_PASSWORD=x, IMAGE_TAG=local
docker compose --profile migrate run --rm migrate && docker compose up -d
curl -k https://api.localhost/health
```

Caddy issues certificates from its internal CA for `*.localhost`.
