//! Blob storage behind a backend seam + signed download URLs.
//!
//! Backends (STORAGE_BACKEND env):
//! - "postgres" (default): blobs in the rust_blobs table — multi-node correct
//!   with zero extra infrastructure; any node can serve any file.
//! - "disk": local filesystem under STORAGE_DIR — single-node setups, or a
//!   shared/NFS mount.
//! - "s3": any S3-compatible store (AWS, R2, MinIO) via `crate::s3`, using
//!   legacy's exact object-key layout (`app-id/bin/location-id`) so a bucket
//!   written by the legacy server is served as-is. `$files.url` is a
//!   presigned GET like legacy (day-bucketed signing instant, 7-day ttl);
//!   `S3_PRESIGN=0` proxies through `/storage/serve` instead. S3 has no
//!   append, so live stream bytes are spooled through the postgres table and
//!   moved to S3 once the stream is done (legacy likewise flushes finished
//!   streams to S3).
//!
//! `/storage/serve/...` URLs are HMAC-signed with SERVER_SECRET and
//! day-bucketed like legacy's presigned URLs so browser caches can reuse them.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use instant_core::error::{InstantError, Result};
use sha2::Sha256;
use sqlx::Row;
use uuid::Uuid;

use crate::s3::S3Client;
use crate::state::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Postgres,
    Disk,
    S3,
}

pub fn backend() -> Backend {
    match std::env::var("STORAGE_BACKEND").as_deref() {
        Ok("disk") => Backend::Disk,
        Ok("s3") => Backend::S3,
        _ => Backend::Postgres,
    }
}

/// Legacy S3 object metadata defaults (`instant.util.s3`): every stored blob
/// carries a content type and disposition, and so does its `$files` row.
pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
pub const DEFAULT_CONTENT_DISPOSITION: &str = "inline";

/// Headers stored alongside a blob (S3 object metadata; the other backends
/// keep them on the `$files` row only).
#[derive(Debug, Clone)]
pub struct BlobMeta {
    pub content_type: String,
    pub content_disposition: String,
}

impl Default for BlobMeta {
    fn default() -> Self {
        BlobMeta {
            content_type: DEFAULT_CONTENT_TYPE.into(),
            content_disposition: DEFAULT_CONTENT_DISPOSITION.into(),
        }
    }
}

static S3: OnceLock<Arc<S3Client>> = OnceLock::new();

/// Validate the backend configuration at boot (fail fast on a missing bucket
/// or credentials) and warm role-based S3 credentials.
pub fn init_from_env() -> anyhow::Result<()> {
    if backend() == Backend::S3 {
        let cfg = crate::s3::S3Config::from_env()?;
        tracing::info!(
            bucket = %cfg.bucket,
            region = %cfg.region,
            endpoint = ?cfg.endpoint.as_ref().map(|u| u.as_str()),
            presign = cfg.presign,
            "storage backend: s3"
        );
        let client = Arc::new(S3Client::from_env(cfg));
        tokio::spawn(client.clone().refresh_loop());
        let _ = S3.set(client);
    }
    Ok(())
}

pub fn s3() -> Option<&'static Arc<S3Client>> {
    S3.get()
}

fn s3_client() -> Result<&'static Arc<S3Client>> {
    s3().ok_or_else(|| InstantError::internal("s3 storage backend is not initialized"))
}

pub fn storage_dir() -> PathBuf {
    std::env::var("STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./storage-data"))
}

fn check_location_id(location_id: &str) -> Result<()> {
    if location_id.is_empty() || location_id.contains('/') || location_id.contains("..") {
        return Err(InstantError::param_malformed("Malformed location id"));
    }
    Ok(())
}

fn blob_path(app_id: Uuid, location_id: &str) -> Result<PathBuf> {
    check_location_id(location_id)?;
    Ok(storage_dir().join(app_id.to_string()).join(location_id))
}

/// Stream bytes live under this prefix (crate::streams::stream_key); in S3
/// mode they are spooled in postgres while the stream is live.
fn is_stream_key(location_id: &str) -> bool {
    location_id.starts_with("stream-")
}

/// Legacy S3 object key for a blob (`app-id/bin/location-id`).
pub fn object_key(app_id: Uuid, location_id: &str) -> String {
    crate::s3::object_key(app_id, location_id)
}

/// One-time bootstrap for the postgres backend (and the S3 stream spool).
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

// ---------------------------------------------------------------------------
// postgres primitives (the default backend and the S3 stream spool)

async fn pg_put(state: &AppState, app_id: Uuid, location_id: &str, bytes: &[u8]) -> Result<()> {
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
    Ok(())
}

async fn pg_get(state: &AppState, app_id: Uuid, location_id: &str) -> Option<Vec<u8>> {
    let row = sqlx::query("SELECT data FROM rust_blobs WHERE app_id = $1 AND location_id = $2")
        .bind(app_id)
        .bind(location_id)
        .fetch_optional(&state.pool)
        .await
        .ok()??;
    Some(row.get::<Vec<u8>, _>("data"))
}

async fn pg_size(state: &AppState, app_id: Uuid, location_id: &str) -> Option<i64> {
    sqlx::query("SELECT length(data) AS n FROM rust_blobs WHERE app_id = $1 AND location_id = $2")
        .bind(app_id)
        .bind(location_id)
        .fetch_optional(&state.pool)
        .await
        .ok()
        .flatten()
        .map(|r| r.get::<i32, _>("n") as i64)
}

async fn pg_delete(state: &AppState, app_id: Uuid, location_id: &str) {
    let _ = sqlx::query("DELETE FROM rust_blobs WHERE app_id = $1 AND location_id = $2")
        .bind(app_id)
        .bind(location_id)
        .execute(&state.pool)
        .await;
}

fn append_offset_error() -> InstantError {
    InstantError::validation_failed(
        "stream",
        "Append offset is past the end of the stream.",
        serde_json::json!([]),
    )
}

/// Bytes of `bytes` that land past `current` when appending at `offset`.
fn new_tail(current: i64, offset: i64, bytes: &[u8]) -> Result<&[u8]> {
    if offset > current {
        return Err(append_offset_error());
    }
    let skip = (current - offset) as usize;
    Ok(if skip < bytes.len() {
        &bytes[skip..]
    } else {
        &[]
    })
}

async fn pg_append(
    state: &AppState,
    app_id: Uuid,
    location_id: &str,
    offset: i64,
    bytes: &[u8],
) -> Result<i64> {
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
    let new_bytes = new_tail(current, offset, bytes)?;
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

// ---------------------------------------------------------------------------
// public API

pub async fn put_blob(
    state: &AppState,
    app_id: Uuid,
    location_id: &str,
    bytes: &[u8],
    meta: &BlobMeta,
) -> Result<u64> {
    check_location_id(location_id)?;
    match backend() {
        Backend::Postgres => pg_put(state, app_id, location_id, bytes).await?,
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
        Backend::S3 => {
            s3_client()?
                .put(
                    &object_key(app_id, location_id),
                    bytes.to_vec(),
                    &meta.content_type,
                    &meta.content_disposition,
                )
                .await?;
        }
    }
    Ok(bytes.len() as u64)
}

pub async fn read_blob(state: &AppState, app_id: Uuid, location_id: &str) -> Option<Vec<u8>> {
    check_location_id(location_id).ok()?;
    match backend() {
        Backend::Postgres => pg_get(state, app_id, location_id).await,
        Backend::Disk => {
            let path = blob_path(app_id, location_id).ok()?;
            tokio::fs::read(&path).await.ok()
        }
        Backend::S3 => {
            if is_stream_key(location_id) {
                if let Some(bytes) = pg_get(state, app_id, location_id).await {
                    return Some(bytes);
                }
            }
            match s3_client()
                .ok()?
                .get(&object_key(app_id, location_id))
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!("s3 read {location_id}: {e}");
                    None
                }
            }
        }
    }
}

pub async fn blob_size(state: &AppState, app_id: Uuid, location_id: &str) -> i64 {
    if check_location_id(location_id).is_err() {
        return 0;
    }
    match backend() {
        Backend::Postgres => pg_size(state, app_id, location_id).await.unwrap_or(0),
        Backend::Disk => match blob_path(app_id, location_id) {
            Ok(path) => tokio::fs::metadata(&path)
                .await
                .map(|m| m.len() as i64)
                .unwrap_or(0),
            Err(_) => 0,
        },
        Backend::S3 => {
            if is_stream_key(location_id) {
                if let Some(n) = pg_size(state, app_id, location_id).await {
                    return n;
                }
            }
            match s3_client() {
                Ok(c) => c
                    .head(&object_key(app_id, location_id))
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0),
                Err(_) => 0,
            }
        }
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
    check_location_id(location_id)?;
    match backend() {
        // S3 objects are immutable: spool live appends through postgres and
        // move the finished stream over in finalize_blob.
        Backend::Postgres | Backend::S3 => {
            pg_append(state, app_id, location_id, offset, bytes).await
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
            let new_bytes = new_tail(current, offset, bytes)?;
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

/// A stream is done: on the S3 backend, move its spooled bytes from
/// postgres to the bucket (write S3 first, then drop the spool row, so a
/// reader always finds the bytes in one place or the other). No-op for the
/// other backends.
pub async fn finalize_blob(state: &AppState, app_id: Uuid, location_id: &str) -> Result<()> {
    if backend() != Backend::S3 {
        return Ok(());
    }
    check_location_id(location_id)?;
    let Some(bytes) = pg_get(state, app_id, location_id).await else {
        return Ok(());
    };
    s3_client()?
        .put(
            &object_key(app_id, location_id),
            bytes,
            "text/plain; charset=utf-8",
            DEFAULT_CONTENT_DISPOSITION,
        )
        .await?;
    pg_delete(state, app_id, location_id).await;
    Ok(())
}

pub async fn delete_blob(state: &AppState, app_id: Uuid, location_id: &str) {
    if check_location_id(location_id).is_err() {
        return;
    }
    match backend() {
        Backend::Postgres => pg_delete(state, app_id, location_id).await,
        Backend::Disk => {
            if let Ok(path) = blob_path(app_id, location_id) {
                let _ = tokio::fs::remove_file(path).await;
            }
        }
        Backend::S3 => {
            if is_stream_key(location_id) {
                pg_delete(state, app_id, location_id).await;
            }
            if let Ok(c) = s3_client() {
                if let Err(e) = c.delete(&object_key(app_id, location_id)).await {
                    tracing::warn!("s3 delete {location_id}: {e}");
                }
            }
        }
    }
}

/// Signature stable within a day (legacy signs with day-bucketed instants so
/// browser caches can reuse URLs).
fn sign(secret: &str, app_id: Uuid, location_id: &str, day: i64) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("hmac accepts any key length");
    mac.update(app_id.as_bytes());
    mac.update(location_id.as_bytes());
    mac.update(&day.to_be_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Constant-time equality for the URL signature.
fn sig_eq(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Start of the current UTC day: legacy `bucketed-signing-instant`.
fn day_bucket() -> (i64, chrono::DateTime<chrono::Utc>) {
    let day = chrono::Utc::now().timestamp() / 86_400;
    let instant = chrono::DateTime::from_timestamp(day * 86_400, 0).unwrap_or_default();
    (day, instant)
}

pub fn download_url(state: &AppState, app_id: Uuid, location_id: &str) -> String {
    let (day, instant) = day_bucket();
    if backend() == Backend::S3 {
        if let Some(c) = s3() {
            if c.cfg.presign {
                if let Some(url) = c.presigned_get(&object_key(app_id, location_id), instant) {
                    return url;
                }
            }
        }
    }
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
    // valid for 7 days from its signing day; a day in the future is not a
    // signing day we ever produced
    if day > now_day
        || now_day - day > 7
        || !sig_eq(&sign(&state.cfg.secret, app_id, &location_id, day), &sig)
    {
        return (StatusCode::FORBIDDEN, "invalid signature").into_response();
    }
    // After the signature check so scanners without valid urls (already a
    // cheap 403) can't drain an app's download budget.
    if let Err(retry) = state.limiters.storage_serve.check(app_id, 1.0) {
        return crate::routes::runtime::err_response(&crate::rate_limit::rate_limited_err(retry));
    }
    match read_blob(&state, app_id, &location_id).await {
        Some(bytes) => {
            let meta = file_meta(&state, app_id, &location_id)
                .await
                .unwrap_or_default();
            (
                [
                    (header::CONTENT_TYPE, meta.content_type),
                    (header::CONTENT_DISPOSITION, meta.content_disposition),
                    // blobs are immutable per location-id (a replaced file
                    // gets a new one), matching legacy's presigned
                    // response-cache-control
                    (
                        header::CACHE_CONTROL,
                        crate::s3::RESPONSE_CACHE_CONTROL.to_string(),
                    ),
                    // Legacy serves blobs from the S3 origin; here they come
                    // off the API origin with uploader-chosen content-type /
                    // disposition, so an `inline` text/html upload would be
                    // stored XSS against the API host. The sandbox CSP keeps
                    // such a document scriptless and origin-less without
                    // changing the bytes or headers a browser reads.
                    (
                        header::CONTENT_SECURITY_POLICY,
                        "sandbox; default-src 'none'".to_string(),
                    ),
                    (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
                ],
                bytes,
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

async fn file_meta(state: &AppState, app_id: Uuid, location_id: &str) -> Option<BlobMeta> {
    let loc_attr = instant_core::system_catalog::attr_id("$files", "location-id");
    let ct_attr = instant_core::system_catalog::attr_id("$files", "content-type");
    let cd_attr = instant_core::system_catalog::attr_id("$files", "content-disposition");
    let row = sqlx::query(
        "SELECT ct.value AS ct, cd.value AS cd FROM triples t
         LEFT JOIN triples ct ON ct.app_id = t.app_id AND ct.entity_id = t.entity_id AND ct.attr_id = $3
         LEFT JOIN triples cd ON cd.app_id = t.app_id AND cd.entity_id = t.entity_id AND cd.attr_id = $5
         WHERE t.app_id = $1 AND t.attr_id = $2 AND t.value = to_jsonb($4::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(loc_attr)
    .bind(ct_attr)
    .bind(location_id)
    .bind(cd_attr)
    .fetch_optional(&state.pool)
    .await
    .ok()??;
    let pick = |col: &str, default: &str| {
        row.try_get::<Option<serde_json::Value>, _>(col)
            .ok()
            .flatten()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| default.to_string())
    };
    Some(BlobMeta {
        content_type: pick("ct", DEFAULT_CONTENT_TYPE),
        content_disposition: pick("cd", DEFAULT_CONTENT_DISPOSITION),
    })
}
