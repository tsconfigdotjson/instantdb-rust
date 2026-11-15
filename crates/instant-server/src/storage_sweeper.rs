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
//! non-S3 deployment; only the S3 backend has an object to delete. Nodes in
//! one deployment may use different backends (or buckets) while sharing the
//! database, so a claim is scoped to rows whose `rust_file_backends` marker
//! matches this node: otherwise a postgres/disk node would delete an S3 row
//! (and the `$files` bookkeeping) while the object stayed in the bucket.

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

#[derive(Debug, Clone, Copy)]
struct SweepScope<'a> {
    backend: Backend,
    bucket: &'a str,
}

fn current_scope() -> Result<SweepScope<'static>> {
    let backend = crate::storage::backend();
    let bucket = if backend == Backend::S3 {
        crate::storage::s3()
            .ok_or_else(|| InstantError::internal("s3 storage backend is not initialized"))?
            .cfg
            .bucket
            .as_str()
    } else {
        ""
    };
    Ok(SweepScope { backend, bucket })
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
    let scope = current_scope()?;
    let mut total = 0usize;
    for loop_num in 0..MAX_LOOPS {
        let files = claim(state, grace_secs, scope).await?;
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

async fn claim(state: &AppState, grace_secs: f64, scope: SweepScope<'_>) -> Result<Vec<SweepFile>> {
    // Legacy files-available-wheres + claim-files!: oldest unclaimed rows, or
    // claims from a process that stopped heartbeating more than 5 minutes ago.
    // The owner join keeps each backend/bucket's rows for its own sweeper.
    // Rows queued before owner markers existed have no join row: legacy wrote
    // objects to S3, so an S3 sweeper adopts them; a non-S3 sweeper only does
    // so when this database has never recorded an S3 write (a pure non-S3
    // deployment upgraded from before the marker table).
    let rows = sqlx::query(
        r#"WITH to_update AS (
             SELECT f.id
               FROM app_files_to_sweep f
               LEFT JOIN rust_file_backends o
                 ON o.app_id = f.app_id AND o.location_id = f.location_id
              WHERE f.created_at < now() - make_interval(secs => $1)
                AND (f.process_id IS NULL
                     OR (f.process_id IS NOT NULL
                         AND f.updated_at < now() - make_interval(secs => $2)))
                AND ((o.backend = $5 AND ($5 <> 's3' OR o.bucket = $6))
                     OR (o.backend IS NULL
                         AND ($5 = 's3'
                              OR NOT EXISTS (SELECT 1 FROM rust_file_backends s3
                                              WHERE s3.backend = 's3'))))
              ORDER BY f.created_at
              FOR UPDATE OF f SKIP LOCKED
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
    .bind(scope.backend.as_str())
    .bind(scope.bucket)
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
    let app_ids: Vec<Uuid> = files.iter().map(|f| f.app_id).collect();
    let location_ids: Vec<String> = files.iter().map(|f| f.location_id.clone()).collect();
    // Keep the row deletion and owner-marker cleanup together: a crash after
    // the queue row is gone would otherwise leak a marker forever.
    let mut tx = state.pool.begin().await.map_err(InstantError::from)?;
    sqlx::query("DELETE FROM app_files_to_sweep WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "DELETE FROM rust_file_backends o
          USING unnest($1::uuid[], $2::text[]) AS x(app_id, location_id)
          WHERE o.app_id = x.app_id AND o.location_id = x.location_id",
    )
    .bind(&app_ids)
    .bind(&location_ids)
    .execute(&mut *tx)
    .await?;
    tx.commit().await.map_err(InstantError::from)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://instant:instant@localhost:5432/instant".into());
        sqlx::PgPool::connect(&url).await.expect("connect postgres")
    }

    /// Regression for the mixed-backend CI flake: a postgres/disk node sharing
    /// the database must not claim an S3 row (nor an S3 row from another
    /// bucket); only the matching S3 bucket may claim it.
    #[tokio::test]
    async fn claim_is_scoped_to_backend_and_bucket() {
        let pool = pool().await;
        crate::storage::ensure_blob_table(&pool)
            .await
            .expect("ensure storage tables");
        let state = AppState::new(crate::state::Config::from_env(), pool.clone());
        let app_id = Uuid::new_v4();
        let location_id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO app_files_to_sweep (app_id, location_id, created_at)
             VALUES ($1, $2, now() - interval '33 days')",
        )
        .bind(app_id)
        .bind(&location_id)
        .execute(&pool)
        .await
        .expect("insert sweep row");
        crate::storage::record_blob_owner(
            &state,
            app_id,
            &location_id,
            Backend::S3.as_str(),
            "test-bucket",
        )
        .await
        .expect("record owner");

        let postgres = SweepScope {
            backend: Backend::Postgres,
            bucket: "",
        };
        assert!(claim(&state, 0.0, postgres)
            .await
            .expect("claim as postgres")
            .is_empty());

        let other_bucket = SweepScope {
            backend: Backend::S3,
            bucket: "other-bucket",
        };
        assert!(claim(&state, 0.0, other_bucket)
            .await
            .expect("claim as another bucket")
            .is_empty());

        let s3 = SweepScope {
            backend: Backend::S3,
            bucket: "test-bucket",
        };
        let files = claim(&state, 0.0, s3).await.expect("claim as s3");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].app_id, app_id);
        assert_eq!(files[0].location_id, location_id);

        delete_rows(&state, &files)
            .await
            .expect("delete claimed rows");
        let left: i64 =
            sqlx::query_scalar("SELECT count(*) FROM app_files_to_sweep WHERE app_id = $1")
                .bind(app_id)
                .fetch_one(&pool)
                .await
                .expect("count sweep rows");
        assert_eq!(left, 0);
        let markers: i64 =
            sqlx::query_scalar("SELECT count(*) FROM rust_file_backends WHERE app_id = $1")
                .bind(app_id)
                .fetch_one(&pool)
                .await
                .expect("count owner markers");
        assert_eq!(markers, 0);

        // Legacy queue rows have no marker. With no S3 writes recorded, the
        // local backend adopts them; once an S3 write is recorded, it leaves
        // them for the S3 sweeper instead.
        let legacy_loc = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO app_files_to_sweep (app_id, location_id, created_at)
             VALUES ($1, $2, now() - interval '33 days')",
        )
        .bind(app_id)
        .bind(&legacy_loc)
        .execute(&pool)
        .await
        .expect("insert legacy sweep row");
        let files = claim(&state, 0.0, postgres)
            .await
            .expect("claim unmarked row as postgres");
        assert_eq!(files.len(), 1);
        delete_rows(&state, &files)
            .await
            .expect("delete legacy row");

        let s3_loc = Uuid::new_v4().to_string();
        crate::storage::record_blob_owner(
            &state,
            app_id,
            &s3_loc,
            Backend::S3.as_str(),
            "test-bucket",
        )
        .await
        .expect("record an S3 write");
        sqlx::query(
            "INSERT INTO app_files_to_sweep (app_id, location_id, created_at)
             VALUES ($1, $2, now() - interval '33 days')",
        )
        .bind(app_id)
        .bind(&legacy_loc)
        .execute(&pool)
        .await
        .expect("insert another unmarked sweep row");
        assert!(claim(&state, 0.0, postgres)
            .await
            .expect("claim unmarked row with S3 present")
            .is_empty());
        sqlx::query("DELETE FROM app_files_to_sweep WHERE app_id = $1")
            .bind(app_id)
            .execute(&pool)
            .await
            .expect("clean sweep rows");
        sqlx::query("DELETE FROM rust_file_backends WHERE app_id = $1")
            .bind(app_id)
            .execute(&pool)
            .await
            .expect("clean owner markers");
    }
}
