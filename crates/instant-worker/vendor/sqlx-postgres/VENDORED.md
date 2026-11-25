# sqlx-postgres 0.8.6, patched for Hyperdrive

This directory holds sqlx-postgres 0.8.6 from crates.io. It is used only by
`instant-worker`, through that crate's `[patch.crates-io]`; the native server
builds against the unpatched crate. Every change is marked
`instant-worker patch`, and
`deploy-test-logs/sqlx-postgres-patch.diff` has the full diff against the
crates.io release.

| Change | File | Why |
|---|---|---|
| no `Close(Portal)` after execute; no `Close(Statement)` on cache eviction or `clear_cached_statements` | `connection/executor.rs`, `connection/mod.rs` | Hyperdrive rejects `Close` ("Unexpected protocol code: C") |
| persistent statements are named from a process-wide counter with a random start (`StatementId::from_number`) | `connection/executor.rs`, `io/mod.rs` | per-connection names (`sqlx_s_1`, …) collide across the backends Hyperdrive multiplexes |
| `ping()` only flushes and drains; no bare `Sync` | `connection/mod.rs` | after a dropped transaction's queued `ROLLBACK`, Hyperdrive never answers that `Sync`, so the connection hangs and the pool drains |
| ErrorResponse severity may be quoted (`"FATAL"`) | `message/response.rs` | Hyperdrive's own errors quote it, and sqlx then fails to parse the error |

The leftover `Close` and `Flush` code is now unused, which accounts for the
compiler warnings.
