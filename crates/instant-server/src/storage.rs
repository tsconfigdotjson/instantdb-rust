//! Blob storage behind a backend seam + HMAC-signed download URLs.
//!
//! Backends (STORAGE_BACKEND env):
//! - "postgres" (default): blobs in the rust_blobs table — multi-node correct
//!   with zero extra infrastructure; any node can serve any file.
//! - "disk": local filesystem under STORAGE_DIR — single-node setups, or a
//!   shared/NFS mount. (An S3 backend would slot in beside these.)
//!
//! Legacy stores blobs in S3 and presigns URLs; here /storage/serve/... URLs
//! are HMAC-signed with SERVER_SECRET and day-bucketed like legacy so browser
//! caches can reuse them.

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use instant_core::error::{InstantError, Result};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::state::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Postgres,
    Disk,
}

pub fn backend() -> Backend {
    match std::env::var("STORAGE_BACKEND").as_deref() {
        Ok("disk") => Backend::Disk,
        _ => Backend::Postgres,
    }
}

pub fn storage_dir() -> PathBuf {
    std::env::var("STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./storage-data"))
}

fn blob_path(app_id: Uuid, location_id: &str) -> Result<PathBuf> {
    if location_id.contains('/') || location_id.contains("..") {
        return Err(InstantError::param_malformed("Malformed location id"));
    }
    Ok(storage_dir().join(app_id.to_string()).join(location_id))
}

/// One-time bootstrap for the postgres backend.
pub async fn ensure_blob_table(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rust_blobs (
           app_id uuid NOT NULL,
           location_id text NOT NULL,
           data bytea NOT NULL,
           created_at timestamptz NOT NULL DEFAULT now(),
           PRIMARY KEY (app_id, location_id))",
    )
    .execute(pool)
    .await
    .map_err(InstantError::from)?;
    Ok(())
}

pub async fn put_blob(
    state: &AppState,
    app_id: Uuid,
    location_id: &str,
    bytes: &[u8],
) -> Result<u64> {
    match backend() {
        Backend::Postgres => {
            sqlx::query(
                "INSERT INTO rust_blobs (app_id, location_id, data) VALUES ($1, $2, $3)
                 ON CONFLICT (app_id, location_id) DO UPDATE SET data = $3",
            )
            .bind(app_id)
            .bind(location_id)
            .bind(bytes)
            .execute(&state.pool)
            .await
            .map_err(InstantError::from)?;
        }
        Backend::Disk => {
            let path = blob_path(app_id, location_id)?;
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| InstantError::internal(format!("storage mkdir: {e}")))?;
            }
            tokio::fs::write(&path, bytes)
                .await
                .map_err(|e| InstantError::internal(format!("storage write: {e}")))?;
        }
    }
    Ok(bytes.len() as u64)
}

pub async fn read_blob(state: &AppState, app_id: Uuid, location_id: &str) -> Option<Vec<u8>> {
    match backend() {
        Backend::Postgres => {
            let row =
                sqlx::query("SELECT data FROM rust_blobs WHERE app_id = $1 AND location_id = $2")
                    .bind(app_id)
                    .bind(location_id)
                    .fetch_optional(&state.pool)
                    .await
                    .ok()??;
            Some(row.get::<Vec<u8>, _>("data"))
        }
        Backend::Disk => {
            let path = blob_path(app_id, location_id).ok()?;
            tokio::fs::read(&path).await.ok()
        }
    }
}

pub async fn blob_size(state: &AppState, app_id: Uuid, location_id: &str) -> i64 {
    match backend() {
        Backend::Postgres => sqlx::query(
            "SELECT length(data) AS n FROM rust_blobs WHERE app_id = $1 AND location_id = $2",
        )
        .bind(app_id)
        .bind(location_id)
        .fetch_optional(&state.pool)
        .await
        .ok()
        .flatten()
        .map(|r| r.get::<i32, _>("n") as i64)
        .unwrap_or(0),
        Backend::Disk => match blob_path(app_id, location_id) {
            Ok(path) => tokio::fs::metadata(&path)
                .await
                .map(|m| m.len() as i64)
                .unwrap_or(0),
            Err(_) => 0,
        },
    }
}

/// Append bytes at `offset` (must be <= current size; overlap is skipped).
/// Returns the new total size. Used by streams.
pub async fn append_blob(
    state: &AppState,
    app_id: Uuid,
    location_id: &str,
    offset: i64,
    bytes: &[u8],
) -> Result<i64> {
    match backend() {
        Backend::Postgres => {
            // serialize concurrent appends on the row
            let mut tx = state.pool.begin().await.map_err(InstantError::from)?;
            let row = sqlx::query(
                "SELECT length(data) AS n FROM rust_blobs
                 WHERE app_id = $1 AND location_id = $2 FOR UPDATE",
            )
            .bind(app_id)
            .bind(location_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(InstantError::from)?;
            let current = row.map(|r| r.get::<i32, _>("n") as i64).unwrap_or(0);
            if offset > current {
                return Err(InstantError::validation_failed(
                    "stream",
                    "Append offset is past the end of the stream.",
                    serde_json::json!([]),
                ));
            }
            let skip = (current - offset) as usize;
            let new_bytes = if skip < bytes.len() {
                &bytes[skip..]
            } else {
                &[]
            };
            if !new_bytes.is_empty() {
                sqlx::query(
                    "INSERT INTO rust_blobs (app_id, location_id, data) VALUES ($1, $2, $3)
                     ON CONFLICT (app_id, location_id) DO UPDATE
                     SET data = rust_blobs.data || EXCLUDED.data",
                )
                .bind(app_id)
                .bind(location_id)
                .bind(new_bytes)
                .execute(&mut *tx)
                .await
                .map_err(InstantError::from)?;
            }
            tx.commit().await.map_err(InstantError::from)?;
            Ok(current + new_bytes.len() as i64)
        }
        Backend::Disk => {
            let path = blob_path(app_id, location_id)?;
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| InstantError::internal(format!("storage mkdir: {e}")))?;
            }
            let current = tokio::fs::metadata(&path)
                .await
                .map(|m| m.len() as i64)
                .unwrap_or(0);
            if offset > current {
                return Err(InstantError::validation_failed(
                    "stream",
                    "Append offset is past the end of the stream.",
                    serde_json::json!([]),
                ));
            }
            let skip = (current - offset) as usize;
            let new_bytes = if skip < bytes.len() {
                &bytes[skip..]
            } else {
                &[]
            };
            if !new_bytes.is_empty() {
                use tokio::io::AsyncWriteExt;
                let mut f = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .await
                    .map_err(|e| InstantError::internal(format!("stream open: {e}")))?;
                f.write_all(new_bytes)
                    .await
                    .map_err(|e| InstantError::internal(format!("stream write: {e}")))?;
                f.flush().await.ok();
            }
            Ok(current + new_bytes.len() as i64)
        }
    }
}

pub async fn delete_blob(state: &AppState, app_id: Uuid, location_id: &str) {
    match backend() {
        Backend::Postgres => {
            let _ = sqlx::query("DELETE FROM rust_blobs WHERE app_id = $1 AND location_id = $2")
                .bind(app_id)
                .bind(location_id)
                .execute(&state.pool)
                .await;
        }
        Backend::Disk => {
            if let Ok(path) = blob_path(app_id, location_id) {
                let _ = tokio::fs::remove_file(path).await;
            }
        }
    }
}

/// Signature stable within a day (legacy signs with day-bucketed instants so
/// browser caches can reuse URLs).
fn sign(secret: &str, app_id: Uuid, location_id: &str, day: i64) -> String {
    let mut h = Sha256::new();
    h.update(secret.as_bytes());
    h.update(app_id.as_bytes());
    h.update(location_id.as_bytes());
    h.update(day.to_be_bytes());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

pub fn download_url(state: &AppState, app_id: Uuid, location_id: &str) -> String {
    let day = chrono::Utc::now().timestamp() / 86_400;
    let sig = sign(&state.cfg.secret, app_id, location_id, day);
    format!(
        "{}/storage/serve/{}/{}?d={}&sig={}",
        state.cfg.base_url, app_id, location_id, day, sig
    )
}

pub async fn serve(
    State(state): State<Arc<AppState>>,
    Path((app_id, location_id)): Path<(Uuid, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let day: i64 = params.get("d").and_then(|d| d.parse().ok()).unwrap_or(0);
    let sig = params.get("sig").cloned().unwrap_or_default();
    let now_day = chrono::Utc::now().timestamp() / 86_400;
    // valid for 7 days
    if now_day - day > 7 || sign(&state.cfg.secret, app_id, &location_id, day) != sig {
        return (StatusCode::FORBIDDEN, "invalid signature").into_response();
    }
    // After the signature check so scanners without valid urls (already a
    // cheap 403) can't drain an app's download budget.
    if let Err(retry) = state.limiters.storage_serve.check(app_id, 1.0) {
        return crate::routes::runtime::err_response(&crate::rate_limit::rate_limited_err(retry));
    }
    match read_blob(&state, app_id, &location_id).await {
        Some(bytes) => {
            let content_type = file_content_type(&state, app_id, &location_id)
                .await
                .unwrap_or_else(|| "application/octet-stream".to_string());
            ([(header::CONTENT_TYPE, content_type)], bytes).into_response()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

async fn file_content_type(state: &AppState, app_id: Uuid, location_id: &str) -> Option<String> {
    let loc_attr = instant_core::system_catalog::attr_id("$files", "location-id");
    let ct_attr = instant_core::system_catalog::attr_id("$files", "content-type");
    let row = sqlx::query(
        "SELECT ct.value AS ct FROM triples t
         JOIN triples ct ON ct.app_id = t.app_id AND ct.entity_id = t.entity_id AND ct.attr_id = $3
         WHERE t.app_id = $1 AND t.attr_id = $2 AND t.value = to_jsonb($4::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(loc_attr)
    .bind(ct_attr)
    .bind(location_id)
    .fetch_optional(&state.pool)
    .await
    .ok()??;
    row.get::<serde_json::Value, _>("ct")
        .as_str()
        .map(|s| s.to_string())
}
