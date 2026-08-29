# Rust Instant Sync Engine — Working Plan

Goal (GOAL.md): stateless, adapter-driven, horizontally scalable InstantDB-compatible
sync engine in Rust on plain Postgres. Wire-compatible with existing @instantdb clients.
Full permissions (CEL rules), Google OAuth, refresh tokens, presence/rooms, admin API.
Full unit + scenario testing. React todo example validated in browser.

## Legacy map (LEGACY/server, Clojure)
- Schema: resources/migrations/*.up.sql (124 migrations, applied in order)
- Core: src/instant/db/{instaql,datalog,transaction,permissioned_transaction,cel}.clj
- Models: src/instant/db/model/{attr,attr_pat,triple,entity,transaction}.clj
- Sync: src/instant/reactive/{session,invalidator,ephemeral,query,receive_queue}.clj
- Auth models: src/instant/model/{app_user,app_user_refresh_token,app_oauth_*,rule}.clj
- HTTP routes: src/instant/routes/*; admin API: src/instant/admin/*
- Client protocol reference: LEGACY/client/packages/core/src (Reactor.js)
- Tests: LEGACY/server/test (68 files) — port scenarios

## Architecture decisions
- Cargo workspace at repo root: crates/
  - `instant-core`: attrs, triples, instaql, instaml tx, cel perms (storage adapter trait)
  - `instant-server`: axum HTTP + WS, session actor, invalidation via pg LISTEN/NOTIFY
- Stateless: no cross-node in-memory state; invalidation + presence broadcast via
  Postgres LISTEN/NOTIFY; horizontally scalable.
- CEL: `cel-interpreter` crate; port instant's bindings (data.ref, auth, newData, ...)

## Milestones / status (update as work progresses!)
- [x] M0 survey legacy layout
- [x] M0.5 postgres 18 (apt, port 5432, db=instant user=instant pw=instant), legacy migrations replayed via scripts/apply-migrations.sh (all 124 clean on stock PG18)
- [x] M1 protocol specs extracted -> docs/{PROTOCOL,SERVER-SYNC,DATAMODEL,AUTH,ADMIN,PERMS,QUERY}.md
- [x] M2 attrs+triples+transact in Rust + 14 tests green (tx_test.rs)
- [x] M3 instaql query engine + 14 tests green (query_test.rs); topics deferred (recompute-all + result-hash suppression is the invalidation baseline)
- [x] M4 WS session + refresh/invalidation via LISTEN/NOTIFY (smoke-ws.mjs passes: init/add-query/transact/refresh-ok/errors)
- [x] M5 CEL perms wired into query+transact paths (cel crate; binds, data.ref/auth.ref prefetch; needs dedicated tests)
- [ ] M6 auth: refresh tokens, magic codes, google OAuth
- [x] M7 presence/rooms/broadcast via PG table + NOTIFY (refresh-presence snapshots; patch-presence TODO optional)
- [x] M8 admin API: query (object tree, inference), transact (admin steps grammar incl lookups/links/auto-attrs), refresh_tokens, sign_out, users, magic codes, presence, storage upload/delete + local-disk blob serve
- [x] M9 react-todo example (examples/react-todo, official @instantdb/react built from LEGACY) browser-validated: two tabs live sync, presence=2, toggle/add/delete-completed sync, magic-code auth in-browser
- [x] M10 scenario tests: 50 core tests (tx/query/rules/scenarios) + 5 e2e node suites (smoke-ws, multinode, admin-sdk, oauth, sse) + browser validation

## Validation loop
- cargo test (unit+integration against dockerized PG)
- differential option: run legacy server via self-hosting docker-compose; compare
- browser: react todo app + chrome plugin

## Status log
- 2026-08-29: M0 survey done. 124 migrations found. Client packages enumerated. Nothing built yet.
- 2026-08-29: M2 done: attr/triple/tx modules, system catalog encoder verified, 14 tests green against local PG.
- 2026-08-29: M3 done: instaql.rs (where ops incl $not/$isNull/comparators/or/and/paths, pagination+cursors+nulls, children, fields, aggregate, ws result shape).
- 2026-08-29: M4/M7 smoke-tested end-to-end via scripts/smoke-ws.mjs (two clients, presence, broadcast). Server: crates/instant-server (state/service/ws/invalidator/presence/auth).
- 2026-08-29: M6+M8 tested via curl: magic code lifecycle, admin query/transact/link/refresh_tokens all green. OAuth implemented but needs a mock-provider test.
- 2026-08-29: M9 browser validation complete. Fixed cursor-direction bug found by scenario tests (forward=is_after). 50 core tests green.
- 2026-08-29: patch-presence, SSE fallback, perms-check endpoints, client storage routes added; full regression green (5 node suites + 50 rust tests + browser).
- 2026-08-29: ALL MILESTONES COMPLETE. GOAL.md satisfied: stateless multi-node sync engine on plain Postgres, wire-compatible with official clients (browser + admin SDK validated), full perms/auth/oauth/presence/storage, packaged with Docker + migration docs. Remaining known gaps tracked honestly in docs/PARITY.md (sync-tables/streams experimental features, BYOP, dashboard routes).
- 2026-08-29 (final): sync tables + streams implemented and tested (trigger change-log, disk-backed streams, cross-node fan-out). Every wire-protocol feature the clients speak is now implemented. Full regression: 53 rust tests + 7 e2e node suites all green.
