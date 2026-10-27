//! Drains `app_files_to_sweep`, the queue migration 52's triggers fill when a
//! `$files` location-id triple is deleted or replaced (port of
//! LEGACY/server/src/instant/storage/sweeper.clj).
//!
//! Synchronous deletes already remove most objects, but the trigger still
//! queues every deleted key and hard-deleting an app queues the keys of all its
//! live files while it removes their triples. This job is what makes those
//! rows (and any object a failed synchronous delete missed) eventually go
//! away. Rows become available after the legacy 32-day backup grace; a batch is
//! claimed with `FOR UPDATE SKIP LOCKED` so every node can work concurrently,
//! and a claim whose process died is reclaimed after 5 minutes.
//!
//! The queue drains on every storage backend so it cannot grow forever on a
//! non-S3 deployment; only the S3 backend has an object to delete.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use instant_core::error::{InstantError, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::state::AppState;
use crate::storage::Backend;

/// Files per claim/delete loop (legacy `batch-size`).
const BATCH: i64 = 1000;
/// Max claims in one sweep before logging that the queue may be backed up
/// (legacy `default-max-loops`).
const MAX_LOOPS: usize = 1000;
/// A claimed row whose process stopped updating it is reclaimable after this
/// long (legacy's "5 minutes").
const RECLAIM_SECS: f64 = 5.0 * 60.0;
/// Object deletes in flight per claimed batch. Legacy sends one batch
/// DeleteObjects request; concurrent idempotent DeleteObject calls are the
/// same wire behavior without adding a second S3 request shape here.
const DELETE_CONCURRENCY: usize = 16;

#[derive(Debug)]
struct SweepFile {
    id: Uuid,
    app_id: Uuid,
    location_id: String,
}

pub async fn run(state: Arc<AppState>) {
    let every = Duration::from_secs(state.cfg.storage_sweep_secs.max(1));
    tracing::info!(
        every_secs = every.as_secs(),
        grace_hours = state.cfg.storage_sweep_grace_hours,
        "storage sweeper started"
    );
    // First pass soon after boot (legacy chime starts at now); the cap avoids
    // a thundering herd of hourly nodes all sweeping at once after a deploy.
    tokio::time::sleep(every.min(Duration::from_secs(30))).await;
    loop {
        match sweep_once(&state).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(files = n, "storage sweeper removed files"),
            Err(e) => tracing::warn!(error = %e, "storage sweep failed"),
        }
        tokio::time::sleep(every).await;
    }
}

/// Drain available rows in batches. Returns how many rows were removed.
pub async fn sweep_once(state: &AppState) -> Result<usize> {
    let grace_secs = (state.cfg.storage_sweep_grace_hours * 3600) as f64;
    let mut total = 0usize;
    for loop_num in 0..MAX_LOOPS {
        let files = claim(state, grace_secs).await?;
        if files.is_empty() {
            return Ok(total);
        }
        let n = files.len();
        delete_objects(&files).await?;
        delete_rows(state, &files).await?;
        total += n;
        if n < BATCH as usize {
            return Ok(total);
        }
        if loop_num + 1 == MAX_LOOPS {
            tracing::warn!(
                max_loops = MAX_LOOPS,
                "storage sweeper hit its loop limit; the queue may be backed up"
            );
        }
    }
    Ok(total)
}

async fn claim(state: &AppState, grace_secs: f64) -> Result<Vec<SweepFile>> {
    // Legacy files-available-wheres + claim-files!: oldest unclaimed rows, or
    // claims from a process that stopped heartbeating more than 5 minutes ago.
    let rows = sqlx::query(
        r#"WITH to_update AS (
             SELECT id
               FROM app_files_to_sweep
              WHERE created_at < now() - make_interval(secs => $1)
                AND (process_id IS NULL
                     OR (process_id IS NOT NULL
                         AND updated_at < now() - make_interval(secs => $2)))
              ORDER BY created_at
              FOR UPDATE SKIP LOCKED
              LIMIT $3
           )
           UPDATE app_files_to_sweep f
              SET process_id = $4, updated_at = now()
             FROM to_update
            WHERE f.id = to_update.id
           RETURNING f.id, f.app_id, f.location_id"#,
    )
    .bind(grace_secs)
    .bind(RECLAIM_SECS)
    .bind(BATCH)
    .bind(state.node_id.to_string())
    .fetch_all(&state.pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| SweepFile {
            id: r.get("id"),
            app_id: r.get("app_id"),
            location_id: r.get("location_id"),
        })
        .collect())
}

async fn delete_objects(files: &[SweepFile]) -> Result<()> {
    if crate::storage::backend() != Backend::S3 {
        return Ok(());
    }
    let client = crate::storage::s3()
        .cloned()
        .ok_or_else(|| InstantError::internal("s3 storage backend is not initialized"))?;
    let keys: Vec<(Uuid, String)> = files
        .iter()
        .filter_map(|f| {
            if f.location_id.is_empty()
                || f.location_id.contains('/')
                || f.location_id.contains("..")
            {
                // Never let a malformed location-id turn into a key outside
                // the app's blob prefix. The row still goes away below.
                tracing::warn!(
                    app_id = %f.app_id,
                    location_id = %f.location_id,
                    "storage sweeper skipped a malformed location id"
                );
                None
            } else {
                Some((
                    f.app_id,
                    crate::storage::object_key(f.app_id, &f.location_id),
                ))
            }
        })
        .collect();
    let results: Vec<(Uuid, String, Result<()>)> = futures::stream::iter(keys)
        .map(|(app_id, key)| {
            let client = client.clone();
            async move {
                let result = client.delete(&key).await;
                (app_id, key, result)
            }
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .collect()
        .await;
    let mut first_error = None;
    let mut failures = 0usize;
    for (app_id, key, result) in results {
        if let Err(e) = result {
            failures += 1;
            tracing::warn!(%app_id, %key, error = %e, "storage sweeper could not delete an object");
            first_error.get_or_insert(e);
        }
    }
    if let Some(e) = first_error {
        tracing::warn!(
            failures,
            "storage sweeper left the batch claimed for a retry after 5 minutes"
        );
        return Err(e);
    }
    Ok(())
}

async fn delete_rows(state: &AppState, files: &[SweepFile]) -> Result<()> {
    let ids: Vec<Uuid> = files.iter().map(|f| f.id).collect();
    sqlx::query("DELETE FROM app_files_to_sweep WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&state.pool)
        .await?;
    Ok(())
}
