# instant-worker (experimental)

The Instant sync server hosted in a Cloudflare Durable Object, with Postgres
reached through Hyperdrive.

**This crate has not been compiled or run yet.** It targets workers-rs'
Emscripten + Tokio preview and sits outside the main workspace. Read
[docs/CLOUDFLARE.md](../../docs/CLOUDFLARE.md) for the design, what has been
verified, and the known work left.

```bash
npx wrangler dev
```
