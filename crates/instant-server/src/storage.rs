//! Local-disk storage adapter + HMAC-signed download URLs.
//! Legacy stores blobs in S3 and signs URLs; we serve from disk at
//! /storage/serve/:app_id/:location_id with an HMAC signature.

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use instant_core::error::{InstantError, Result};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::state::AppState;

pub fn storage_dir() -> PathBuf {
    std::env::var("STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./storage-data"))
}

fn blob_path(app_id: Uuid, location_id: &str) -> PathBuf {
    storage_dir().join(app_id.to_string()).join(location_id)
}

pub async fn put_blob(app_id: Uuid, location_id: &str, bytes: &[u8]) -> Result<u64> {
    let path = blob_path(app_id, location_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| InstantError::internal(format!("storage mkdir: {e}")))?;
    }
    tokio::fs::write(&path, bytes)
        .await
        .map_err(|e| InstantError::internal(format!("storage write: {e}")))?;
    Ok(bytes.len() as u64)
}

pub async fn delete_blob(app_id: Uuid, location_id: &str) {
    let _ = tokio::fs::remove_file(blob_path(app_id, location_id)).await;
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
    if location_id.contains('/') || location_id.contains("..") {
        return (StatusCode::BAD_REQUEST, "bad location").into_response();
    }
    match tokio::fs::read(blob_path(app_id, &location_id)).await {
        Ok(bytes) => {
            // find content-type from the $files row
            let content_type = file_content_type(&state, app_id, &location_id)
                .await
                .unwrap_or_else(|| "application/octet-stream".to_string());
            ([(header::CONTENT_TYPE, content_type)], bytes).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

async fn file_content_type(state: &AppState, app_id: Uuid, location_id: &str) -> Option<String> {
    use sqlx::Row;
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
    row.get::<serde_json::Value, _>("ct").as_str().map(|s| s.to_string())
}
