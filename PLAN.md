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
- [ ] M4 WS session + refresh/invalidation loop
- [ ] M5 permissions (CEL) + permissioned transact + perms-checked queries
- [ ] M6 auth: refresh tokens, magic codes, google OAuth
- [ ] M7 presence/rooms via NOTIFY
- [ ] M8 admin API
- [ ] M9 react todo example wired to local server, browser-validated
- [ ] M10 scenario tests matching legacy test suite level

## Validation loop
- cargo test (unit+integration against dockerized PG)
- differential option: run legacy server via self-hosting docker-compose; compare
- browser: react todo app + chrome plugin

## Status log
- 2026-08-29: M0 survey done. 124 migrations found. Client packages enumerated. Nothing built yet.
- 2026-08-29: M2 done: attr/triple/tx modules, system catalog encoder verified, 14 tests green against local PG.
- 2026-08-29: M3 done: instaql.rs (where ops incl $not/$isNull/comparators/or/and/paths, pagination+cursors+nulls, children, fields, aggregate, ws result shape).
