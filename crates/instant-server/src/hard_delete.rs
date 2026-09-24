//! Purges apps and attrs whose deletion mark is older than the grace period
//! (legacy hard_deletion_sweeper.clj + custodian.clj). Until then a delete
//! only sets `deletion_marked_at` and can be undone; afterwards the rows go.
//!
//! Legacy plans each purge as a chain of custodian rows; here one node at a
//! time (a Postgres advisory lock) drains the big tables in bounded batches,
//! each its own statement, so no single transaction holds a huge lock or
//! bloats the WAL. Every batch re-checks the mark, so a restore mid-purge
//! stops it. The final `DELETE FROM apps` cascades the remaining app tables.

use std::sync::Arc;

use instant_core::error::Result;
use sqlx::Row;
use uuid::Uuid;

use crate::state::AppState;

/// Rows per delete statement (legacy custodian-batch-size).
const BATCH: i64 = 1000;
/// Arbitrary key for the sweeper's session advisory lock.
const LOCK_KEY: i64 = 0x1a5d_e1e7_0000_0001;
/// Server tables keyed by app without a foreign key to `apps`.
const RUST_APP_TABLES: &[&str] = &[
    "rust_blobs",
    "rust_tx_changes",
    "rust_webhook_history",
    "rust_presence",
];

pub async fn run(state: Arc<AppState>) {
    let every = std::time::Duration::from_secs(state.cfg.hard_delete_sweep_secs);
    tokio::time::sleep(every.min(std::time::Duration::from_secs(30))).await;
    loop {
        if let Err(e) = sweep(&state).await {
            tracing::warn!(error = %e, "hard-delete sweep failed");
        }
        tokio::time::sleep(every).await;
    }
}

pub async fn sweep(state: &AppState) -> Result<()> {
    let mut lock_conn = state.pool.acquire().await?;
    let locked: bool = sqlx::query("SELECT pg_try_advisory_lock($1) AS ok")
        .bind(LOCK_KEY)
        .fetch_one(&mut *lock_conn)
        .await?
        .get("ok");
    if !locked {
        return Ok(());
    }
    let result = sweep_locked(state).await;
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(LOCK_KEY)
        .execute(&mut *lock_conn)
        .await;
    result
}

async fn sweep_locked(state: &AppState) -> Result<()> {
    let grace_secs = (state.cfg.hard_delete_grace_hours * 3600) as f64;
    let attrs = sqlx::query(
        "SELECT id, app_id FROM attrs
          WHERE deletion_marked_at IS NOT NULL
            AND deletion_marked_at < now() - make_interval(secs => $1)",
    )
    .bind(grace_secs)
    .fetch_all(&state.pool)
    .await?;
    for r in &attrs {
        let (attr_id, app_id): (Uuid, Uuid) = (r.get("id"), r.get("app_id"));
        purge_attr(state, app_id, attr_id).await?;
    }
    let apps = sqlx::query(
        "SELECT id FROM apps
          WHERE deletion_marked_at IS NOT NULL
            AND deletion_marked_at < now() - make_interval(secs => $1)",
    )
    .bind(grace_secs)
    .fetch_all(&state.pool)
    .await?;
    for r in &apps {
        purge_app(state, r.get("id")).await?;
    }
    if !attrs.is_empty() || !apps.is_empty() {
        tracing::info!(
            attrs = attrs.len(),
            apps = apps.len(),
            "hard-deleted marked apps and attrs"
        );
    }
    Ok(())
}

/// Delete in batches until a statement removes fewer than `BATCH` rows.
/// `sql` binds $1 = app id, $2 = batch size, $3 = attr id (optional).
async fn drain(state: &AppState, sql: &str, app_id: Uuid, attr_id: Option<Uuid>) -> Result<()> {
    loop {
        let mut q = sqlx::query(sql).bind(app_id).bind(BATCH);
        if let Some(a) = attr_id {
            q = q.bind(a);
        }
        let n = q.execute(&state.pool).await?.rows_affected();
        if (n as i64) < BATCH {
            return Ok(());
        }
    }
}

async fn purge_attr(state: &AppState, app_id: Uuid, attr_id: Uuid) -> Result<()> {
    drain(
        state,
        "DELETE FROM triples WHERE ctid IN (
           SELECT ctid FROM triples WHERE app_id = $1 AND attr_id = $3 LIMIT $2)
           AND EXISTS (SELECT 1 FROM attrs WHERE id = $3 AND deletion_marked_at IS NOT NULL)",
        app_id,
        Some(attr_id),
    )
    .await?;
    sqlx::query("DELETE FROM attrs WHERE id = $1 AND deletion_marked_at IS NOT NULL")
        .bind(attr_id)
        .execute(&state.pool)
        .await?;
    Ok(())
}

async fn purge_app(state: &AppState, app_id: Uuid) -> Result<()> {
    const MARKED: &str =
        "EXISTS (SELECT 1 FROM apps WHERE id = $1 AND deletion_marked_at IS NOT NULL)";
    drain(
        state,
        &format!(
            "DELETE FROM triples WHERE ctid IN (
               SELECT ctid FROM triples WHERE app_id = $1 LIMIT $2) AND {MARKED}"
        ),
        app_id,
        None,
    )
    .await?;
    drain(
        state,
        &format!(
            "DELETE FROM transactions WHERE ctid IN (
               SELECT ctid FROM transactions WHERE app_id = $1 LIMIT $2) AND {MARKED}"
        ),
        app_id,
        None,
    )
    .await?;
    let deleted = sqlx::query("DELETE FROM apps WHERE id = $1 AND deletion_marked_at IS NOT NULL")
        .bind(app_id)
        .execute(&state.pool)
        .await?
        .rows_affected();
    if deleted == 0 {
        // restored mid-purge: keep its server-side rows
        return Ok(());
    }
    for table in RUST_APP_TABLES {
        if let Err(e) = sqlx::query(&format!("DELETE FROM {table} WHERE app_id = $1"))
            .bind(app_id)
            .execute(&state.pool)
            .await
        {
            tracing::warn!(%app_id, table, error = %e, "hard-delete: server table cleanup failed");
        }
    }
    if crate::storage::backend() == crate::storage::Backend::Disk {
        crate::storage::delete_app_dir(app_id).await;
    }
    Ok(())
}
