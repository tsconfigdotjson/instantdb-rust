//! App-level end-user auth: refresh tokens, users (stored as triples in the
//! $users / $userRefreshTokens system namespaces).

use instant_core::error::{InstantError, Result};
use instant_core::system_catalog as sc;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::state::AppState;

/// hex(sha256(uuid-16-bytes)) — matches legacy refresh-token hashing.
pub fn hash_token(token: Uuid) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

/// hex(sha256(utf8 string)) — for magic codes and oauth state hashes.
pub fn hash_string(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[derive(Debug, Clone)]
pub struct AppUser {
    pub id: Uuid,
    pub email: Option<String>,
}

impl AppUser {
    pub fn to_json(&self) -> Value {
        json!({"id": self.id, "email": self.email})
    }
}

/// Look up the user for a refresh token (token = plaintext uuid).
pub async fn user_by_refresh_token(
    state: &AppState,
    app_id: Uuid,
    token: &str,
) -> Result<Option<AppUser>> {
    let Ok(token) = Uuid::parse_str(token) else {
        return Ok(None);
    };
    let hashed = hash_token(token);
    let hashed_attr = sc::attr_id("$userRefreshTokens", "hashedToken");
    let user_attr = sc::attr_id("$userRefreshTokens", "$user");
    let email_attr = sc::attr_id("$users", "email");
    let row = sqlx::query(
        r#"
        SELECT json_uuid_to_uuid(u.value) AS user_id, e.value AS email
        FROM triples t
        JOIN triples u ON u.app_id = t.app_id AND u.entity_id = t.entity_id AND u.attr_id = $3 AND u.eav
        LEFT JOIN triples e ON e.app_id = t.app_id
              AND e.entity_id = json_uuid_to_uuid(u.value) AND e.attr_id = $4
        WHERE t.app_id = $1 AND t.attr_id = $2 AND t.av AND t.value = to_jsonb($5::text)
        LIMIT 1
        "#,
    )
    .bind(app_id)
    .bind(hashed_attr)
    .bind(user_attr)
    .bind(email_attr)
    .bind(&hashed)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    Ok(row.and_then(|r| {
        let user_id: Option<Uuid> = r.get("user_id");
        let email: Option<Value> = r.try_get("email").ok();
        user_id.map(|id| AppUser {
            id,
            email: email.and_then(|v| v.as_str().map(|s| s.to_string())),
        })
    }))
}

/// Find a user by email.
pub async fn user_by_email(state: &AppState, app_id: Uuid, email: &str) -> Result<Option<AppUser>> {
    let email_attr = sc::attr_id("$users", "email");
    let row = sqlx::query(
        "SELECT entity_id FROM triples
         WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text) LIMIT 1",
    )
    .bind(app_id)
    .bind(email_attr)
    .bind(email)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    Ok(row.map(|r| AppUser { id: r.get("entity_id"), email: Some(email.to_string()) }))
}

pub async fn user_by_id(state: &AppState, app_id: Uuid, id: Uuid) -> Result<Option<AppUser>> {
    let id_attr = sc::attr_id("$users", "id");
    let email_attr = sc::attr_id("$users", "email");
    let row = sqlx::query(
        "SELECT e.value AS email FROM triples t
         LEFT JOIN triples e ON e.app_id = t.app_id AND e.entity_id = t.entity_id AND e.attr_id = $3
         WHERE t.app_id = $1 AND t.attr_id = $2 AND t.entity_id = $4 LIMIT 1",
    )
    .bind(app_id)
    .bind(id_attr)
    .bind(email_attr)
    .bind(id)
    .fetch_optional(&state.pool)
    .await
    .map_err(InstantError::from)?;
    Ok(row.map(|r| {
        let email: Option<Value> = r.try_get("email").ok();
        AppUser { id, email: email.and_then(|v| v.as_str().map(|s| s.to_string())) }
    }))
}

/// Create a user (if needed) and mint a refresh token. Returns (user, token).
pub async fn create_user_and_token(
    state: &AppState,
    app_id: Uuid,
    email: &str,
) -> Result<(AppUser, Uuid)> {
    let user = match user_by_email(state, app_id, email).await? {
        Some(u) => u,
        None => {
            let uid = Uuid::new_v4();
            let steps = json!([
                ["add-triple", uid, sc::attr_id("$users", "id"), uid],
                ["add-triple", uid, sc::attr_id("$users", "email"), email]
            ]);
            crate::service::run_system_transact(state, app_id, &steps).await?;
            AppUser { id: uid, email: Some(email.to_string()) }
        }
    };
    let token = mint_refresh_token(state, app_id, user.id).await?;
    Ok((user, token))
}

pub async fn mint_refresh_token(state: &AppState, app_id: Uuid, user_id: Uuid) -> Result<Uuid> {
    let token = Uuid::new_v4();
    let entity = Uuid::new_v4();
    let steps = json!([
        ["add-triple", entity, sc::attr_id("$userRefreshTokens", "id"), entity],
        ["add-triple", entity, sc::attr_id("$userRefreshTokens", "hashedToken"), hash_token(token)],
        ["add-triple", entity, sc::attr_id("$userRefreshTokens", "$user"), user_id]
    ]);
    crate::service::run_system_transact(state, app_id, &steps).await?;
    Ok(token)
}

/// Delete all refresh tokens for a user (sign out) or one token.
pub async fn sign_out(
    state: &AppState,
    app_id: Uuid,
    user_id: Option<Uuid>,
    token: Option<&str>,
) -> Result<()> {
    let mut entity_ids: Vec<Uuid> = vec![];
    if let Some(token) = token.and_then(|t| Uuid::parse_str(t).ok()) {
        let hashed = hash_token(token);
        let rows = sqlx::query(
            "SELECT entity_id FROM triples
             WHERE app_id = $1 AND attr_id = $2 AND av AND value = to_jsonb($3::text)",
        )
        .bind(app_id)
        .bind(sc::attr_id("$userRefreshTokens", "hashedToken"))
        .bind(&hashed)
        .fetch_all(&state.pool)
        .await
        .map_err(InstantError::from)?;
        entity_ids.extend(rows.iter().map(|r| r.get::<Uuid, _>("entity_id")));
    }
    if let Some(uid) = user_id {
        let rows = sqlx::query(
            "SELECT entity_id FROM triples
             WHERE app_id = $1 AND attr_id = $2 AND eav AND json_uuid_to_uuid(value) = $3",
        )
        .bind(app_id)
        .bind(sc::attr_id("$userRefreshTokens", "$user"))
        .bind(uid)
        .fetch_all(&state.pool)
        .await
        .map_err(InstantError::from)?;
        entity_ids.extend(rows.iter().map(|r| r.get::<Uuid, _>("entity_id")));
    }
    if entity_ids.is_empty() {
        return Ok(());
    }
    let steps: Vec<Value> = entity_ids
        .iter()
        .map(|e| json!(["delete-entity", e, "$userRefreshTokens"]))
        .collect();
    crate::service::run_system_transact(state, app_id, &Value::Array(steps)).await?;
    Ok(())
}

/// Verify an admin token for an app.
pub async fn check_admin_token(state: &AppState, app_id: Uuid, token: &str) -> Result<bool> {
    let Ok(token) = Uuid::parse_str(token) else { return Ok(false) };
    let row = sqlx::query("SELECT 1 AS x FROM app_admin_tokens WHERE app_id = $1 AND token = $2")
        .bind(app_id)
        .bind(token)
        .fetch_optional(&state.pool)
        .await
        .map_err(InstantError::from)?;
    Ok(row.is_some())
}
