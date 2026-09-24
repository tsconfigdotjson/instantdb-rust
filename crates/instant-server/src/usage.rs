//! Per-app size accounting and the optional size cap.
//!
//! The legacy triples triggers (migration 114) append every insert / update /
//! delete's size delta to `triples_size_updates` — row bytes plus `$files`
//! sizes. Legacy's `triples_size_updates.clj` rolls those into
//! `triples_size_aggregate` every few minutes; [`collect_sizes`] is that
//! roll-up. Without it the updates table grows forever.
//!
//! With `INSTANT_APP_SIZE_LIMIT_MB` set, each pass also reloads the set of
//! apps over the cap. Those apps can still delete (to get back under), but
//! transactions that add data, uploads and stream appends are refused. The
//! check runs against the last roll-up, so an app can overshoot by whatever
//! it writes within one collection interval.

use std::sync::Arc;

use instant_core::error::{InstantError, Result};
use instant_core::tx::TxStep;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::state::AppState;

/// Rows rolled up per statement (legacy triples-size-collection-batch-size).
const COLLECT_BATCH: i64 = 10_000;
/// Batches per pass before yielding to the next tick.
const COLLECT_MAX_LOOPS: usize = 100;

/// Legacy `collect-batch-q`: move a batch of deltas into the aggregate.
/// `SKIP LOCKED` lets every node run it concurrently.
const COLLECT_BATCH_Q: &str = r#"
WITH ids AS (
  SELECT id FROM triples_size_updates ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED
), deletes AS (
  DELETE FROM triples_size_updates USING ids
   WHERE triples_size_updates.id = ids.id
  RETURNING triples_size_updates.app_id, triples_size_updates.attr_id,
            triples_size_updates.pg_size, triples_size_updates.files_size
), aggregate AS (
  INSERT INTO triples_size_aggregate (app_id, attr_id, pg_size, files_size)
  SELECT d.app_id, d.attr_id, sum(d.pg_size), sum(d.files_size)
    FROM deletes d
    -- drops deltas whose app / attr was deleted mid-batch
    JOIN apps ON apps.id = d.app_id
    JOIN attrs ON attrs.id = d.attr_id
   GROUP BY d.app_id, d.attr_id
   ORDER BY d.app_id, d.attr_id
  ON CONFLICT ON CONSTRAINT triples_size_aggregate_pkey DO UPDATE SET
    pg_size = triples_size_aggregate.pg_size + excluded.pg_size,
    files_size = coalesce(triples_size_aggregate.files_size + excluded.files_size,
                          triples_size_aggregate.files_size,
                          excluded.files_size)
  RETURNING 1
)
SELECT (SELECT count(*) FROM deletes) AS deleted,
       (SELECT count(*) FROM aggregate) AS aggregated
"#;

pub async fn collect_sizes(state: &AppState) -> Result<u64> {
    let mut total = 0u64;
    for _ in 0..COLLECT_MAX_LOOPS {
        let row = sqlx::query(COLLECT_BATCH_Q)
            .bind(COLLECT_BATCH)
            .fetch_one(&state.pool)
            .await?;
        let deleted: i64 = row.get("deleted");
        total += deleted as u64;
        if deleted < COLLECT_BATCH {
            break;
        }
    }
    Ok(total)
}

/// Bytes an app uses as of the last roll-up.
pub async fn app_bytes(state: &AppState, app_id: Uuid) -> Result<i64> {
    let row = sqlx::query(
        "SELECT coalesce(sum(pg_size + coalesce(files_size, 0)), 0)::bigint AS n
           FROM triples_size_aggregate WHERE app_id = $1",
    )
    .bind(app_id)
    .fetch_one(&state.pool)
    .await?;
    Ok(row.get("n"))
}

async fn refresh_over_limit(state: &AppState, limit: i64) -> Result<()> {
    let rows = sqlx::query(
        "SELECT app_id FROM triples_size_aggregate
          GROUP BY app_id
         HAVING sum(pg_size + coalesce(files_size, 0)) > $1",
    )
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    let over: std::collections::HashSet<Uuid> = rows.iter().map(|r| r.get("app_id")).collect();
    state.over_size_limit.retain(|id| over.contains(id));
    for id in over {
        state.over_size_limit.insert(id);
    }
    Ok(())
}

pub async fn run(state: Arc<AppState>) {
    let every = std::time::Duration::from_secs(state.cfg.size_collect_secs);
    loop {
        match collect_sizes(&state).await {
            Ok(n) if n > 0 => tracing::debug!(rows = n, "collected triple sizes"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "triple size collection failed"),
        }
        if let Some(limit) = state.cfg.app_size_limit_bytes {
            if let Err(e) = refresh_over_limit(&state, limit).await {
                tracing::warn!(error = %e, "app size limit check failed");
            }
        }
        tokio::time::sleep(every).await;
    }
}

fn limit_mb(limit: i64) -> i64 {
    limit / (1024 * 1024)
}

pub fn size_limit_error(limit: i64) -> InstantError {
    InstantError::new(
        "app-size-limit-exceeded",
        400,
        format!(
            "This app has reached its {} MB size limit. Delete data to keep writing.",
            limit_mb(limit)
        ),
        Some(json!({"limit-bytes": limit})),
    )
}

fn adds_data(step: &TxStep) -> bool {
    matches!(
        step,
        TxStep::AddAttr(_) | TxStep::AddTriple { .. } | TxStep::DeepMergeTriple { .. }
    )
}

/// Transactions of an app over the cap may only remove data.
pub fn check_transact(state: &AppState, app_id: Uuid, steps: &[TxStep]) -> Result<()> {
    match state.cfg.app_size_limit_bytes {
        Some(limit) if state.over_size_limit.contains(&app_id) && steps.iter().any(adds_data) => {
            Err(size_limit_error(limit))
        }
        _ => Ok(()),
    }
}

/// An upload of `incoming` bytes must fit under the cap.
pub async fn check_upload(state: &AppState, app_id: Uuid, incoming: usize) -> Result<()> {
    let Some(limit) = state.cfg.app_size_limit_bytes else {
        return Ok(());
    };
    if state.over_size_limit.contains(&app_id)
        || app_bytes(state, app_id).await? + incoming as i64 > limit
    {
        return Err(size_limit_error(limit));
    }
    Ok(())
}

/// Stream appends stop once the app is over the cap.
pub fn check_append(state: &AppState, app_id: Uuid) -> Result<()> {
    match state.cfg.app_size_limit_bytes {
        Some(limit) if state.over_size_limit.contains(&app_id) => Err(size_limit_error(limit)),
        _ => Ok(()),
    }
}
