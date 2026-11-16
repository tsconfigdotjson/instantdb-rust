# instant-worker (experimental)

The Instant sync server hosted in a Cloudflare Durable Object, with Postgres
reached through Hyperdrive.

It builds and passes the project's e2e suites locally under `wrangler dev`.
It has been deployed to Cloudflare once and passed the protocol suites
there, with a patched sqlx-postgres (`vendor/`) and Hyperdrive caching off;
see [deploy-test-logs/REPORT.md](../../deploy-test-logs/REPORT.md). It
targets workers-rs' Emscripten +
Tokio preview and sits outside the main workspace. Read
[docs/CLOUDFLARE.md](../../docs/CLOUDFLARE.md) for the design, what has been
verified, and what is left.

```bash
../../scripts/apply-migrations.sh   # schema, against your Postgres
npx wrangler dev                    # first run installs the toolchain; serves :8787
```
