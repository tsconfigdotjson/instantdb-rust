# Deploying

The server is one stateless binary that speaks plain HTTP and websockets, and
Postgres holds all of its state. A production setup needs three things: a
Postgres database, one or more server nodes, and a TLS-terminating reverse
proxy in front of them.

## Production checklist

- **TLS and websockets.** Terminate TLS at a proxy (Caddy, nginx, a cloud
  load balancer) and forward to the server port. The proxy has to support
  websocket upgrades on `/runtime/session` and server-sent events on
  `/runtime/sse`. Point clients at `https://…` / `wss://…` and set `BASE_URL`
  to the public https URL so OAuth redirects and file URLs come out right.
- **Secrets.** `SERVER_SECRET` signs storage download URLs. If you leave it
  unset, a random secret is generated on first boot and stored in Postgres,
  and every node picks it up. Set it yourself only if you want to manage
  rotation, and never use a guessable value.
- **Postgres.** Use a strong password (`instant:instant` in the examples is
  for development), enable TLS to the database when it crosses a network, and
  keep Postgres off the public internet. Only the server nodes need to reach
  it.
- **Network exposure.** Expose only the proxy. There's no separate
  management port: `/admin/*` is part of the public API and is protected by
  per-app admin tokens. Tokens are accepted only in the
  `Authorization: Bearer` header, never in URLs, and the server doesn't log
  request URLs or headers.
- **Body limits.** JSON endpoints accept bodies up to 10 MB and storage
  uploads up to 100 MB. Set your proxy's limits at or above these.
- **CORS.** Responses use wildcard CORS without credentials. Auth is
  bearer-token based, so cookies are never accepted cross-origin.
- **Public signups.** If you run the dashboard on the open internet, set
  `INSTANT_EPHEMERAL_APPS=off` and pick an `INSTANT_DASHBOARD_SIGNUP_MODE`
  (see [CONFIGURATION.md](CONFIGURATION.md)).

## Scaling

Nodes hold no state between requests. Queries are computed from Postgres,
invalidations, presence and broadcasts fan out over `pg_notify`, and presence
lives in an unlogged Postgres table with node heartbeats. Run as many
replicas as you like behind any websocket-capable load balancer:

```bash
docker compose up --scale server=3
```

`scripts/multinode-ws.mjs` shows two clients on two different nodes syncing
live. The one exception to "any node, any request" is the server-sent-events
transports: the browser client's fallback when websockets are unavailable
(`/runtime/sse`) and `@instantdb/admin`'s `subscribeQuery` / `db.streams`
(`/admin/subscribe-query`, `/admin/sse*`). Their follow-up POSTs have to reach
the node holding the stream, so give those paths session affinity when you
run more than one node. Websocket clients need none.

Each transaction is one `NOTIFY` that every node receives. That comfortably
handles a few thousand transactions per second per cluster. Past that, shard
apps across Postgres instances rather than adding sync nodes. See
[PERF.md](PERF.md) for measured numbers.

## Monitoring

`GET /metrics` serves Prometheus text format: live sessions (total and per
app), message counters, query / transact / add-query duration histograms,
NOTIFY lag, refresh batch latency, invalidation counters, attribute-cache
hits, pool utilization, and process RSS and CPU. It's unauthenticated and
cheap to scrape, so keep it behind your proxy.

## A full stack with Docker Compose

`deploy/` holds a complete single-host stack: Postgres, the server, the
self-hosted Instant dashboard, and the marketing site, behind Caddy with
automatic Let's Encrypt certificates. It's what runs
[instantdbrust.com](https://instantdbrust.com) on one 4 GB VPS, and it's a
reasonable starting point for your own deployment.

```
Caddy (TLS) ─┬─ $SITE_DOMAIN → marketing site (web/, static files in /opt/instant/site)
             ├─ $DASH_DOMAIN → dashboard  (LEGACY/client/www, self-hosted Next.js)
             └─ $API_DOMAIN  → instant-server
                                  │
                              Postgres 17 (deploy/postgresql.conf)
```

The dashboard and the API need separate hostnames, because the dashboard's
own pages live under `/dash`, which is also the server's dashboard API
prefix. Drop the site block from the `Caddyfile` if you don't need it.

| file | |
|---|---|
| `compose.yml` | the stack; images come from GHCR, tagged by commit sha |
| `postgresql.conf` | Postgres tuned for a 4 GB box (memory budget in the header) |
| `Caddyfile` | automatic TLS, websocket/SSE proxying, `/metrics` hidden |
| `deploy.sh` | pull → migrate → restart → wait for health |
| `backup.sh` | nightly `pg_dump`, 7 days kept in `/opt/instant/backups` |
| `bootstrap.sh` | one-time host setup (Docker, swap, firewall, `deploy` user, cron) |
| `.env.example` | the settings `/opt/instant/.env` holds (secrets never enter the repo) |

### Try it locally

```sh
docker build -t ghcr.io/tsconfigdotjson/instantdb-rust-server:local .
docker build -f LEGACY/client/Dockerfile.www -t ghcr.io/tsconfigdotjson/instantdb-rust-dashboard:local LEGACY/client
cd deploy && cp .env.example .env    # set DASH_DOMAIN=dash.localhost, API_DOMAIN=api.localhost,
                                     # POSTGRES_PASSWORD=x, IMAGE_TAG=local
docker compose --profile migrate run --rm migrate && docker compose up -d
curl -k https://api.localhost/health
```

Caddy issues certificates from its internal CA for `*.localhost`.

### First-time setup on a server

1. **DNS.** A/AAAA records for `$SITE_DOMAIN`, `www.$SITE_DOMAIN`,
   `$DASH_DOMAIN` and `$API_DOMAIN` pointing at the host. On Cloudflare,
   leave them DNS-only (grey cloud) so Caddy can get its own certificates.
2. **Deploy key.** `ssh-keygen -t ed25519 -f instant-deploy -N ''`.
3. **Bootstrap** (as root on a fresh Ubuntu 24.04 or newer):
   ```sh
   bash bootstrap.sh "$(cat instant-deploy.pub)"
   ```
   This writes `/opt/instant/.env` with a generated `POSTGRES_PASSWORD`.
   Fill in the domains, `ACME_EMAIL`, `INSTANT_SUPERUSER_EMAIL` and
   `INSTANT_DASHBOARD_ALLOWED_EMAILS`.
4. **Email.** Magic codes are printed to the server log until you configure
   an email provider (`EMAIL_PROVIDER=cloudflare`) or dashboard Google login
   (`INSTANT_DASHBOARD_GOOGLE_OAUTH_*`, redirect URI
   `https://$API_DOMAIN/dash/oauth/callback`). The log is fine while signup
   is `restricted` to you; set one up before opening signups.

### Deploying from GitHub Actions

`.github/workflows/deploy.yml` runs after CI passes on `main`, or by hand
from the Actions tab. It builds the server and dashboard images for
`linux/amd64`, pushes them to GHCR as `:<sha>` and `:latest`, builds the site,
copies `deploy/` and the site to the host, and runs `deploy.sh` with that
sha. The dashboard is never built on the host, since a Next.js build needs
more memory than a small box can spare.

To use it from a fork, create a `production` environment under
Settings → Environments with:

- variables `VPS_HOST` (IP or hostname) and `API_DOMAIN`
- secrets `VPS_SSH_KEY` (the contents of `instant-deploy`) and
  `VPS_KNOWN_HOSTS` (`ssh-keyscan <host>` output)

To roll back, re-run the workflow on an older commit, or on the box run
`IMAGE_TAG=<old sha> /opt/instant/deploy.sh`. That rolls the images; the site
only changes on a workflow run. If your GHCR packages are private, the host
needs a token with `read:packages` to pull by hand:

```sh
echo <token> | docker login ghcr.io -u <github user> --password-stdin
IMAGE_TAG=<sha> /opt/instant/deploy.sh
```

### Operating

```sh
cd /opt/instant
docker compose ps
docker compose logs -f server
docker compose logs server | grep -i code      # magic codes (EMAIL_PROVIDER=log)
docker compose exec postgres psql -U instant   # database shell
```

Changes to `.env` take effect on the next deploy, or immediately with
`docker compose up -d`.
