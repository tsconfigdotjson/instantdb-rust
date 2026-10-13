// Every word on /dashboard (the page the old "demo dashboard" links now land
// on). Edit freely; the layout lives in Sunset.tsx.
export const SUNSET = {
  kicker: "lights out",
  title: "The demo server has",
  accent: "gone dark.",
  body: [
    "We ran a public InstantDB Rust server and dashboard so you could try things before moving anything real. We didn't have the resources to keep it running, so we've shut it down.",
    "The good news: it was only ever the same open-source server you can run yourself, and it takes a few minutes on your own machine.",
  ],
  promptKicker: "hand this to your coding agent",
  promptHint: "Paste it into Claude Code, Cursor, or whichever agent you use, from an empty folder.",
  copy: "Copy prompt",
  copied: "Copied!",
  home: "Back to the homepage",
  github: "Read the source",
};

// The prompt a visitor pastes into their coding agent. The commands come
// straight from README.md ("Quick start", "Running without Docker") and
// docs/DEPLOY.md ("Try it locally"); keep them in sync with those files.
export const SELF_HOST_PROMPT = `Set up a self-hosted instantdb-rust server on this machine for me.
instantdb-rust is an open-source, wire-compatible Rust rewrite of the
InstantDB sync engine: https://github.com/tsconfigdotjson/instantdb-rust
Read its README.md and docs/DEPLOY.md before you start, and stop to ask me
if anything below doesn't match them.

1. Clone it and start Postgres plus the server on port 8888:

   git clone https://github.com/tsconfigdotjson/instantdb-rust && cd instantdb-rust
   docker compose up --build

   If I don't have Docker, use Postgres 16-18 instead:

   createdb instant                       # and a role the server can connect as
   ./scripts/apply-migrations.sh          # the original Instant schema
   cargo run --release -p instant-server  # listens on :8888

   (Set DATABASE_URL if it isn't postgres://instant:instant@localhost:5432/instant.)

2. Create an app and give me its credentials (needs psql):

   ./scripts/create-app.sh "my app"       # prints app_id + admin_token

3. Show me how to point an @instantdb client at it:

   init({
     appId: "<app_id>",
     apiURI: "http://localhost:8888",
     websocketURI: "ws://localhost:8888/runtime/session",
   })

   and instant-cli, via INSTANT_CLI_API_URI=http://localhost:8888,
   INSTANT_APP_ID and INSTANT_APP_ADMIN_TOKEN.

4. Only if I ask for the web dashboard too: follow "A full stack with Docker
   Compose" > "Try it locally" in docs/DEPLOY.md:

   docker build -t ghcr.io/tsconfigdotjson/instantdb-rust-server:local .
   docker build -f LEGACY/client/Dockerfile.www -t ghcr.io/tsconfigdotjson/instantdb-rust-dashboard:local LEGACY/client
   cd deploy && cp .env.example .env
   # in .env: DASH_DOMAIN=dash.localhost, API_DOMAIN=api.localhost,
   # POSTGRES_PASSWORD=<anything>, IMAGE_TAG=local, and my email in
   # INSTANT_SUPERUSER_EMAIL and INSTANT_DASHBOARD_ALLOWED_EMAILS
   docker compose --profile migrate run --rm migrate && docker compose up -d
   curl -k https://api.localhost/health

   The dashboard is then at https://dash.localhost/dash. Sign-in magic codes
   are printed to the server log: docker compose logs server | grep -i code

When it's running, check http://localhost:8888/health (or the curl above)
and tell me exactly what you ran.`;
