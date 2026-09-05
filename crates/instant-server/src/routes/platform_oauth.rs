//! The platform OAuth provider (`/platform/oauth/*`, LEGACY
//! oauth_apps/routes.clj) and the dashboard's OAuth-app management routes
//! (`/dash/apps/:app_id/oauth-apps*`, `/dash/user/oauth_apps*`, LEGACY
//! dash/routes.clj:2461-2723). Tables: migration 58
//! (instant_oauth_apps / _clients / _client_secrets / _redirects / _codes,
//! instant_user_oauth_{refresh,access}_tokens). Secrets, codes, redirect ids
//! and tokens are stored as sha256 lookup keys (model/oauth_app.clj).

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use instant_core::error::{InstantError, Result};
use serde_json::{json, Map, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::routes::dash::{dash_user, param_malformed, param_missing, parse_body, DashRole};
use crate::routes::dash_apps::{
    app_role_for_user, live_app_row, path_uuid, record_not_found,
    redirect_url_validation_errors_opt,
};
use crate::routes::dash_manage::{app_and_user, token_lookup_key};
use crate::routes::runtime::{err_response, json_or_err};
use crate::routes::superadmin::{
    access_token_by_value, expired, ALL_SCOPES, PLATFORM_ACCESS_TOKEN_PREFIX,
    PLATFORM_REFRESH_TOKEN_PREFIX,
};
use crate::state::AppState;

const COOKIE_NAME: &str = "__session";
/// `default-expires-at`: redirects and codes live 10 minutes
const CODE_TTL_MINUTES: i64 = 10;
/// access tokens live 2 weeks (oauth_app.clj:731)
const ACCESS_TOKEN_TTL_DAYS: i64 = 14;
/// at most 5 refresh tokens per (client, user) (oauth_app.clj:666)
const REFRESH_TOKEN_LIMIT: i64 = 5;

fn ts(t: Option<chrono::DateTime<chrono::Utc>>) -> Value {
    match t {
        Some(t) => json!(t.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        None => Value::Null,
    }
}

fn random_hex(n: usize) -> String {
    use rand::RngCore;
    let mut bytes = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn uuid_sha256(id: Uuid) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(id.as_bytes()).to_vec()
}

// ---------------------------------------------------------------------------
// logo encoding (oauth_app.clj:55-90): 4 bytes of space-padded mime type
// then the image bytes

fn base64_image_url_to_bytes(s: &str) -> std::result::Result<Vec<u8>, &'static str> {
    let rest = s.strip_prefix("data:image/").ok_or("Invalid image url")?;
    let (mime, b64) = rest.split_once(";base64,").ok_or("Invalid image url")?;
    if mime.is_empty() || !mime.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err("Invalid image url");
    }
    if mime.len() > 4 {
        return Err("Invalid image type");
    }
    if !["jpg", "jpeg", "png", "svg", "webp"].contains(&mime) {
        return Err("Invalid image type");
    }
    let image = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|_| "Invalid image")?;
    if image.len() > 1024 * 1024 {
        return Err("Image is too large");
    }
    let mut out = format!("{mime:<4}").into_bytes();
    out.extend_from_slice(&image);
    Ok(out)
}

fn bytes_to_base64_image_url(b: &[u8]) -> Value {
    if b.len() < 4 {
        return Value::Null;
    }
    let mime = String::from_utf8_lossy(&b[..4]).trim().to_string();
    let data = base64::engine::general_purpose::STANDARD.encode(&b[4..]);
    json!(format!("data:image/{mime};base64,{data}"))
}

fn logo_param(body: &Value, key: &str) -> Result<Option<Vec<u8>>> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let s = v
                .as_str()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| param_malformed(&["body", key], v.clone()))?;
            base64_image_url_to_bytes(s)
                .map(Some)
                .map_err(|m| InstantError::new("param-malformed", 400, m, None))
        }
    }
}

fn opt_str(body: &Value, key: &str) -> Result<Option<String>> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_str()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(Some)
            .ok_or_else(|| param_malformed(&["body", key], v.clone())),
    }
}

/// `url-util/coerce-web-url`: http(s) with a host
fn opt_web_url(body: &Value, key: &str) -> Result<Option<String>> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let ok = v
                .as_str()
                .and_then(|s| url::Url::parse(s).ok())
                .filter(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some())
                .is_some();
            if ok {
                Ok(v.as_str().map(|s| s.to_string()))
            } else {
                Err(param_malformed(&["body", key], v.clone()))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// api shapes (format-*-for-api)

fn oauth_app_json(r: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": r.get::<Uuid, _>("id"),
        "appId": r.get::<Uuid, _>("app_id"),
        "appName": r.get::<String, _>("app_name"),
        "appLogo": r.get::<Option<Vec<u8>>, _>("app_logo").map(|b| bytes_to_base64_image_url(&b)).unwrap_or(Value::Null),
        "grantedScopes": r.get::<Option<Vec<String>>, _>("granted_scopes").unwrap_or_default(),
        "isPublic": r.get::<bool, _>("is_public"),
        "supportEmail": r.get::<Option<String>, _>("support_email"),
        "appHomePage": r.get::<Option<String>, _>("app_home_page"),
        "appPrivacyPolicyLink": r.get::<Option<String>, _>("app_privacy_policy_link"),
        "appTosLink": r.get::<Option<String>, _>("app_tos_link"),
        "createdAt": ts(r.try_get("created_at").ok().flatten()),
        "updatedAt": ts(r.try_get("updated_at").ok().flatten()),
    })
}

fn client_json(r: &sqlx::postgres::PgRow) -> Value {
    json!({
        "clientId": r.get::<Uuid, _>("client_id"),
        "oauthAppId": r.get::<Uuid, _>("oauth_app_id"),
        "clientName": r.get::<String, _>("client_name"),
        "authorizedRedirectUrls": r.get::<Option<Vec<String>>, _>("authorized_redirect_urls").unwrap_or_default(),
        "createdAt": ts(r.try_get("created_at").ok().flatten()),
        "updatedAt": ts(r.try_get("updated_at").ok().flatten()),
    })
}

fn secret_json(r: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": r.get::<Uuid, _>("id"),
        "clientId": r.get::<Uuid, _>("client_id"),
        "firstFour": r.get::<String, _>("first_four"),
        "createdAt": ts(r.try_get("created_at").ok().flatten()),
    })
}

const OAUTH_APP_COLUMNS: &str = "id, app_id, app_name, granted_scopes, is_public, support_email, app_home_page, app_privacy_policy_link, app_tos_link, app_logo, created_at, updated_at";
const CLIENT_COLUMNS: &str =
    "client_id, oauth_app_id, client_name, authorized_redirect_urls, created_at, updated_at";
const SECRET_COLUMNS: &str = "id, client_id, first_four, created_at";

// ---------------------------------------------------------------------------
// dashboard: OAuth-app management

/// GET /dash/apps/:app_id/oauth-apps — legacy oauth-apps-get +
/// `get-for-dash` (oauth_app.clj:151-215): apps with clients with secrets,
/// newest first.
pub async fn oauth_apps_get(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let id = app_uuid(&app);
        let apps = sqlx::query(&format!("SELECT {OAUTH_APP_COLUMNS} FROM instant_oauth_apps WHERE app_id = $1 ORDER BY created_at DESC"))
            .bind(id)
            .fetch_all(&state.pool)
            .await?;
        let mut out = vec![];
        for a in &apps {
            let mut app_json = oauth_app_json(a);
            let oauth_app_id: Uuid = a.get("id");
            let clients = sqlx::query(&format!("SELECT {CLIENT_COLUMNS} FROM instant_oauth_app_clients WHERE oauth_app_id = $1 ORDER BY created_at DESC"))
                .bind(oauth_app_id)
                .fetch_all(&state.pool)
                .await?;
            let mut client_list = vec![];
            for c in &clients {
                let mut cj = client_json(c);
                let client_id: Uuid = c.get("client_id");
                let secrets = sqlx::query(&format!("SELECT {SECRET_COLUMNS} FROM instant_oauth_app_client_secrets WHERE client_id = $1 ORDER BY created_at DESC"))
                    .bind(client_id)
                    .fetch_all(&state.pool)
                    .await?;
                cj["clientSecrets"] = Value::Array(secrets.iter().map(secret_json).collect());
                client_list.push(cj);
            }
            app_json["clients"] = Value::Array(client_list);
            out.push(app_json);
        }
        Ok(json!({"apps": out}))
    }
    .await;
    json_or_err(r)
}

fn app_uuid(app: &Value) -> Uuid {
    app.get("id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_default()
}

/// POST /dash/apps/:app_id/oauth-apps — legacy oauth-apps-post (:2465-2508)
pub async fn oauth_apps_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let body = parse_body(&body)?;
        let app_name = opt_str(&body, "app_name")?.ok_or_else(|| param_missing(&["body", "app_name"]))?;
        let logo = logo_param(&body, "app_logo")?;
        let support_email = opt_str(&body, "support_email")?;
        let home = opt_web_url(&body, "app_home_page")?;
        let privacy = opt_web_url(&body, "app_privacy_policy_link")?;
        let tos = opt_web_url(&body, "app_tos_link")?;
        let row = sqlx::query(&format!(
            "INSERT INTO instant_oauth_apps (id, app_id, app_name, granted_scopes, is_public, support_email, app_home_page, app_privacy_policy_link, app_tos_link, app_logo)
             VALUES ($1, $2, $3, '{{}}'::text[], false, $4, $5, $6, $7, $8) RETURNING {OAUTH_APP_COLUMNS}"
        ))
        .bind(Uuid::new_v4())
        .bind(app_uuid(&app))
        .bind(&app_name)
        .bind(support_email)
        .bind(home)
        .bind(privacy)
        .bind(tos)
        .bind(logo)
        .fetch_one(&state.pool)
        .await
        .map_err(unique_err("instant-oauth-app"))?;
        Ok(json!({"app": oauth_app_json(&row)}))
    }
    .await;
    json_or_err(r)
}

fn unique_err(record: &'static str) -> impl Fn(sqlx::Error) -> InstantError {
    move |e| match &e {
        sqlx::Error::Database(db) if db.code().as_deref() == Some("23505") => InstantError::new(
            "record-not-unique",
            400,
            format!("Record not unique: {record}"),
            Some(json!({"record-type": record})),
        ),
        _ => InstantError::from(e),
    }
}

/// POST /dash/apps/:app_id/oauth-apps/:oauth_app_id — legacy oauth-app-post (:2510-2559)
pub async fn oauth_app_update(
    State(state): State<Arc<AppState>>,
    Path((app_id, oauth_app_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let oauth_app_id = path_uuid(&oauth_app_id, "oauth_app_id")?;
        let body = parse_body(&body)?;
        let app_name = opt_str(&body, "app_name")?;
        let logo = logo_param(&body, "app_logo")?;
        let support_email = opt_str(&body, "support_email")?;
        let home = opt_web_url(&body, "app_home_page")?;
        let privacy = opt_web_url(&body, "app_privacy_policy_link")?;
        let tos = opt_web_url(&body, "app_tos_link")?;
        let id = app_uuid(&app);
        let row = sqlx::query(&format!(
            "UPDATE instant_oauth_apps SET
                app_name = coalesce($3, app_name),
                support_email = coalesce($4, support_email),
                app_home_page = coalesce($5, app_home_page),
                app_privacy_policy_link = coalesce($6, app_privacy_policy_link),
                app_tos_link = coalesce($7, app_tos_link),
                app_logo = coalesce($8, app_logo)
              WHERE app_id = $1 AND id = $2 RETURNING {OAUTH_APP_COLUMNS}"
        ))
        .bind(id)
        .bind(oauth_app_id)
        .bind(app_name)
        .bind(support_email)
        .bind(home)
        .bind(privacy)
        .bind(tos)
        .bind(logo)
        .fetch_optional(&state.pool)
        .await
        .map_err(unique_err("instant-oauth-app"))?
        .ok_or_else(|| {
            record_not_found(
                "oauth-app",
                json!({"args": [{"app-id": id, "oauth-app-id": oauth_app_id}]}),
            )
        })?;
        Ok(json!({"app": oauth_app_json(&row)}))
    }
    .await;
    json_or_err(r)
}

/// DELETE /dash/apps/:app_id/oauth-apps/:oauth_app_id — legacy oauth-app-delete (:2561-2573), admin
pub async fn oauth_app_delete(
    State(state): State<Arc<AppState>>,
    Path((app_id, oauth_app_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Admin).await?;
        let oauth_app_id = path_uuid(&oauth_app_id, "oauth_app_id")?;
        let id = app_uuid(&app);
        let row = sqlx::query(&format!("DELETE FROM instant_oauth_apps WHERE app_id = $1 AND id = $2 RETURNING {OAUTH_APP_COLUMNS}"))
            .bind(id)
            .bind(oauth_app_id)
            .fetch_optional(&state.pool)
            .await?
            .ok_or_else(|| record_not_found("oauth-app", json!({"args": [{"app-id": id, "oauth-app-id": oauth_app_id}]})))?;
        Ok(json!({"app": oauth_app_json(&row)}))
    }
    .await;
    json_or_err(r)
}

async fn oauth_app_is_public_by_id(
    state: &AppState,
    app_id: Uuid,
    oauth_app_id: Uuid,
) -> Result<bool> {
    let row = sqlx::query("SELECT is_public FROM instant_oauth_apps WHERE app_id = $1 AND id = $2")
        .bind(app_id)
        .bind(oauth_app_id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| {
            record_not_found(
                "oauth-app",
                json!({"args": [{"app-id": app_id, "oauth-app-id": oauth_app_id}]}),
            )
        })?;
    Ok(row.get("is_public"))
}

fn assert_redirect_urls(urls: &[String], allow_localhost: bool) -> Result<()> {
    for u in urls {
        let errors = redirect_url_validation_errors_opt(u, allow_localhost);
        if !errors.is_empty() {
            return Err(InstantError::new(
                "validation-failed",
                400,
                "Validation failed for authorized_redirect_urls",
                Some(
                    json!({"data-type": "authorized_redirect_urls", "input": u, "errors": errors}),
                ),
            ));
        }
    }
    Ok(())
}

/// `gen-client-secret`: 34 random bytes as hex, the first four shown
fn gen_client_secret() -> String {
    random_hex(34)
}

/// POST /dash/apps/:app_id/oauth-apps/:oauth_app_id/clients — legacy oauth-app-clients-post (:2589-2624)
pub async fn oauth_clients_post(
    State(state): State<Arc<AppState>>,
    Path((app_id, oauth_app_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let oauth_app_id = path_uuid(&oauth_app_id, "oauth_app_id")?;
        let id = app_uuid(&app);
        let is_public = oauth_app_is_public_by_id(&state, id, oauth_app_id).await?;
        let body = parse_body(&body)?;
        let client_name = opt_str(&body, "client_name")?.ok_or_else(|| param_missing(&["body", "client_name"]))?;
        let urls: Vec<String> = match body.get("authorized_redirect_urls") {
            None | Some(Value::Null) => vec![],
            Some(v) => v
                .as_array()
                .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
                .ok_or_else(|| param_malformed(&["body", "authorized_redirect_urls"], v.clone()))?,
        };
        assert_redirect_urls(&urls, !is_public)?;
        let client_id = Uuid::new_v4();
        let secret = gen_client_secret();
        let mut dbtx = state.pool.begin().await?;
        let client = sqlx::query(&format!(
            "INSERT INTO instant_oauth_app_clients (client_id, oauth_app_id, client_name, authorized_redirect_urls)
             VALUES ($1, $2, $3, $4) RETURNING {CLIENT_COLUMNS}"
        ))
        .bind(client_id)
        .bind(oauth_app_id)
        .bind(&client_name)
        .bind(&urls)
        .fetch_one(&mut *dbtx)
        .await
        .map_err(unique_err("instant-oauth-app-client"))?;
        let secret_row = sqlx::query(&format!(
            "INSERT INTO instant_oauth_app_client_secrets (id, client_id, hashed_secret, first_four)
             VALUES ($1, $2, $3, $4) RETURNING {SECRET_COLUMNS}"
        ))
        .bind(Uuid::new_v4())
        .bind(client_id)
        .bind(token_lookup_key(&secret))
        .bind(&secret[..4])
        .fetch_one(&mut *dbtx)
        .await?;
        dbtx.commit().await?;
        Ok(json!({"client": client_json(&client), "clientSecret": secret_json(&secret_row), "secretValue": secret}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/oauth-app-clients/:client_id — legacy oauth-app-client-post (:2626-2660)
pub async fn oauth_client_update(
    State(state): State<Arc<AppState>>,
    Path((app_id, client_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let client_id = path_uuid(&client_id, "client_id")?;
        let id = app_uuid(&app);
        let is_public: bool = sqlx::query(
            "SELECT oa.is_public FROM instant_oauth_apps oa JOIN instant_oauth_app_clients c ON c.oauth_app_id = oa.id
              WHERE oa.app_id = $1 AND c.client_id = $2",
        )
        .bind(id)
        .bind(client_id)
        .fetch_optional(&state.pool)
        .await?
        .map(|r| r.get("is_public"))
        .ok_or_else(|| record_not_found("oauth-app", json!({"args": [{"app-id": id, "client-id": client_id}]})))?;
        let body = parse_body(&body)?;
        let client_name = opt_str(&body, "client_name")?;
        let add = opt_str(&body, "add_redirect_url")?;
        if let Some(u) = &add {
            assert_redirect_urls(std::slice::from_ref(u), !is_public)?;
        }
        let remove = opt_str(&body, "remove_redirect_url")?;
        let row = sqlx::query(&format!(
            "UPDATE instant_oauth_app_clients c SET
                client_name = coalesce($3, c.client_name),
                authorized_redirect_urls = (
                  SELECT coalesce(array_agg(url ORDER BY ord), '{{}}'::text[]) FROM (
                    SELECT DISTINCT ON (url) url, ord FROM unnest(
                      array_remove(array_append(c.authorized_redirect_urls, $4), $5)
                    ) WITH ORDINALITY AS t(url, ord)
                    WHERE url IS NOT NULL ORDER BY url, ord
                  ) x)
              WHERE c.client_id = (
                SELECT cl.client_id FROM instant_oauth_app_clients cl
                  JOIN instant_oauth_apps oa ON oa.id = cl.oauth_app_id
                 WHERE oa.app_id = $1 AND cl.client_id = $2)
              RETURNING {CLIENT_COLUMNS}"
        ))
        .bind(id)
        .bind(client_id)
        .bind(client_name)
        .bind(add)
        .bind(remove)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| record_not_found("oauth-client", json!({"args": [{"app-id": id, "client-id": client_id}]})))?;
        Ok(json!({"client": client_json(&row)}))
    }
    .await;
    json_or_err(r)
}

/// DELETE /dash/apps/:app_id/oauth-app-clients/:client_id — legacy oauth-app-client-delete (:2575-2587), admin
pub async fn oauth_client_delete(
    State(state): State<Arc<AppState>>,
    Path((app_id, client_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Admin).await?;
        let client_id = path_uuid(&client_id, "client_id")?;
        let id = app_uuid(&app);
        let row = sqlx::query(&format!(
            "DELETE FROM instant_oauth_app_clients WHERE client_id = (
               SELECT cl.client_id FROM instant_oauth_app_clients cl
                 JOIN instant_oauth_apps oa ON oa.id = cl.oauth_app_id
                WHERE oa.app_id = $1 AND cl.client_id = $2) RETURNING {CLIENT_COLUMNS}"
        ))
        .bind(id)
        .bind(client_id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| {
            record_not_found(
                "oauth-client",
                json!({"args": [{"app-id": id, "client-id": client_id}]}),
            )
        })?;
        Ok(json!({"client": client_json(&row)}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/oauth-app-clients/:client_id/client-secrets — legacy oauth-app-client-secrets (:2662-2677)
pub async fn oauth_client_secrets_post(
    State(state): State<Arc<AppState>>,
    Path((app_id, client_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let client_id = path_uuid(&client_id, "client_id")?;
        let id = app_uuid(&app);
        let secret = gen_client_secret();
        let row = sqlx::query(&format!(
            "INSERT INTO instant_oauth_app_client_secrets (id, hashed_secret, first_four, client_id)
             SELECT $1, $2, $3, cl.client_id FROM instant_oauth_app_clients cl
               JOIN instant_oauth_apps oa ON oa.id = cl.oauth_app_id
              WHERE cl.client_id = $4 AND oa.app_id = $5
             RETURNING {SECRET_COLUMNS}"
        ))
        .bind(Uuid::new_v4())
        .bind(token_lookup_key(&secret))
        .bind(&secret[..4])
        .bind(client_id)
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| {
            record_not_found(
                "oauth-app-client-secrets",
                json!({"args": [{"app-id": id, "client-id": client_id}]}),
            )
        })?;
        Ok(json!({"clientSecret": secret_json(&row), "secretValue": secret}))
    }
    .await;
    json_or_err(r)
}

/// DELETE /dash/apps/:app_id/oauth-app-client-secrets/:client_secret_id — legacy oauth-app-client-secret-delete (:2679-2693)
pub async fn oauth_client_secret_delete(
    State(state): State<Arc<AppState>>,
    Path((app_id, client_secret_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let secret_id = path_uuid(&client_secret_id, "client_secret_id")?;
        let id = app_uuid(&app);
        let row = sqlx::query(&format!(
            "DELETE FROM instant_oauth_app_client_secrets WHERE id = (
               SELECT s.id FROM instant_oauth_app_client_secrets s
                 JOIN instant_oauth_app_clients cl ON cl.client_id = s.client_id
                 JOIN instant_oauth_apps oa ON oa.id = cl.oauth_app_id
                WHERE s.id = $1 AND oa.app_id = $2) RETURNING {SECRET_COLUMNS}"
        ))
        .bind(secret_id)
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| {
            record_not_found(
                "oauth-app-client-secrets",
                json!({"args": [{"app-id": id, "client-secret-id": secret_id}]}),
            )
        })?;
        Ok(json!({"clientSecret": secret_json(&row)}))
    }
    .await;
    json_or_err(r)
}

/// legacy `authorized-oauth-apps` (:2695-2707): the OAuth apps holding a
/// token for the user, sorted by name.
async fn authorized_oauth_apps(state: &AppState, user_id: Uuid) -> Result<Vec<Value>> {
    let rows = sqlx::query(
        "SELECT id, app_name, app_logo, app_home_page, app_privacy_policy_link, app_tos_link
           FROM instant_oauth_apps WHERE id IN (
             SELECT oauth_app_id FROM instant_oauth_app_clients WHERE client_id IN (
               SELECT client_id FROM instant_user_oauth_access_tokens WHERE user_id = $1
               UNION ALL
               SELECT client_id FROM instant_user_oauth_refresh_tokens WHERE user_id = $1))",
    )
    .bind(user_id)
    .fetch_all(&state.pool)
    .await?;
    let mut apps: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "id": r.get::<Uuid, _>("id"),
                "name": r.get::<String, _>("app_name"),
                "logo": r.get::<Option<Vec<u8>>, _>("app_logo").map(|b| bytes_to_base64_image_url(&b)).unwrap_or(Value::Null),
                "homePage": r.get::<Option<String>, _>("app_home_page"),
                "privacyPolicyLink": r.get::<Option<String>, _>("app_privacy_policy_link"),
                "tosLink": r.get::<Option<String>, _>("app_tos_link"),
            })
        })
        .collect();
    apps.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Ok(apps)
}

/// GET /dash/user/oauth_apps
pub async fn user_oauth_apps_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        Ok(json!({"oauthApps": authorized_oauth_apps(&state, user.id).await?}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/user/oauth_apps/revoke_access — legacy user-oauth-apps-revoke-access (:2713-2723)
pub async fn user_oauth_apps_revoke(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let body = parse_body(&body)?;
        let raw = body.get("oauthAppId").filter(|v| !v.is_null()).ok_or_else(|| param_missing(&["body", "oauthAppId"]))?;
        let oauth_app_id = raw
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| param_malformed(&["body", "oauthAppId"], raw.clone()))?;
        sqlx::query(
            "WITH client_ids AS (SELECT client_id FROM instant_oauth_app_clients WHERE oauth_app_id = $1),
                  r AS (DELETE FROM instant_user_oauth_refresh_tokens WHERE user_id = $2 AND client_id = ANY(SELECT client_id FROM client_ids)),
                  a AS (DELETE FROM instant_user_oauth_access_tokens WHERE user_id = $2 AND client_id = ANY(SELECT client_id FROM client_ids))
             SELECT 1",
        )
        .bind(oauth_app_id)
        .bind(user.id)
        .execute(&state.pool)
        .await?;
        Ok(json!({"oauthApps": authorized_oauth_apps(&state, user.id).await?}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// the OAuth flow

/// `oauth-error-page` (oauth_apps/routes.clj:26-70): a 400 HTML page.
fn oauth_error_page(error: &str) -> Response {
    let html = format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"><title>OAuth error</title><style>body {{ margin: 0; height: 100vh; display: flex; justify-content: center; align-items: center; background-color: white; flex-direction: column; font-family: sans-serif; }} a.button {{ text-decoration: none; padding: 15px 30px; font-size: 18px; border-radius: 5px; font-family: sans-serif; text-align: center; }} a {{ cursor: pointer; }} @media (prefers-color-scheme: dark) {{ body {{ background-color: black; }} a.button {{ color: black; background-color: white; }} }} @media (prefers-color-scheme: light) {{ a.button {{ color: white; background-color: black; }} }}</style></head><body><p>There was an error with your OAuth request.</p><p>{}</p></body></html>",
        html_escape(error)
    );
    (
        StatusCode::BAD_REQUEST,
        [(header::CONTENT_TYPE, "text/html")],
        html,
    )
        .into_response()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn found(url: &str) -> Response {
    (StatusCode::FOUND, [(header::LOCATION, url)], "").into_response()
}

fn add_query_params(url: &str, params: &[(&str, &str)]) -> String {
    let mut u =
        url::Url::parse(url).unwrap_or_else(|_| url::Url::parse("http://invalid/").unwrap());
    {
        let mut q = u.query_pairs_mut();
        for (k, v) in params {
            q.append_pair(k, v);
        }
    }
    u.to_string()
}

fn qp<'a>(params: &'a HashMap<String, String>, key: &str) -> Result<&'a str> {
    params
        .get(key)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| param_missing(&["params", key]))
}
fn qp_uuid(params: &HashMap<String, String>, key: &str) -> Result<Uuid> {
    let raw = params
        .get(key)
        .ok_or_else(|| param_missing(&["params", key]))?;
    Uuid::parse_str(raw.trim()).map_err(|_| param_malformed(&["params", key], json!(raw)))
}

struct ClientAndApp {
    client_id: Uuid,
    authorized_redirect_urls: Vec<String>,
    oauth_app_id: Uuid,
    app_id: Uuid,
    app_name: String,
    granted_scopes: Vec<String>,
    is_public: bool,
    support_email: Option<String>,
    app_home_page: Option<String>,
    app_privacy_policy_link: Option<String>,
    app_tos_link: Option<String>,
    app_logo: Option<Vec<u8>>,
}

/// `get-client-and-app-by-client-id!` (oauth_app.clj:254-276)
async fn client_and_app(state: &AppState, client_id: Uuid) -> Result<ClientAndApp> {
    let r = sqlx::query(
        "SELECT c.client_id, c.authorized_redirect_urls, a.id AS oauth_app_id, a.app_id, a.app_name,
                a.granted_scopes, a.is_public, a.support_email, a.app_home_page,
                a.app_privacy_policy_link, a.app_tos_link, a.app_logo
           FROM instant_oauth_app_clients c JOIN instant_oauth_apps a ON a.id = c.oauth_app_id
          WHERE c.client_id = $1",
    )
    .bind(client_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| record_not_found("oauth-app-client", json!({"args": [{"client-id": client_id}]})))?;
    Ok(ClientAndApp {
        client_id: r.get("client_id"),
        authorized_redirect_urls: r
            .get::<Option<Vec<String>>, _>("authorized_redirect_urls")
            .unwrap_or_default(),
        oauth_app_id: r.get("oauth_app_id"),
        app_id: r.get("app_id"),
        app_name: r.get("app_name"),
        granted_scopes: r
            .get::<Option<Vec<String>>, _>("granted_scopes")
            .unwrap_or_default(),
        is_public: r.get("is_public"),
        support_email: r.get("support_email"),
        app_home_page: r.get("app_home_page"),
        app_privacy_policy_link: r.get("app_privacy_policy_link"),
        app_tos_link: r.get("app_tos_link"),
        app_logo: r.get("app_logo"),
    })
}

fn assert_valid_bare(data_type: &str, input: Value, errors: Vec<String>) -> Result<()> {
    if errors.is_empty() {
        return Ok(());
    }
    Err(InstantError::new(
        "validation-failed",
        400,
        format!("Validation failed for {data_type}"),
        Some(json!({"data-type": data_type, "input": input, "errors": errors})),
    ))
}

/// legacy `string-util/join-in-sentence`
fn join_in_sentence(items: &[String]) -> String {
    match items.len() {
        0 => String::new(),
        1 => items[0].clone(),
        2 => format!("{} and {}", items[0], items[1]),
        n => format!("{}, and {}", items[..n - 1].join(", "), items[n - 1]),
    }
}

fn cookie_value(headers: &HeaderMap) -> Option<Uuid> {
    headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| {
            raw.split(';').map(|p| p.trim()).find_map(|p| {
                let (k, v) = p.split_once('=')?;
                if k.trim() != COOKIE_NAME {
                    return None;
                }
                // legacy parse-cookie reads `instantdb_<uuid>`
                v.trim()
                    .strip_prefix("instantdb_")
                    .and_then(|s| Uuid::parse_str(s).ok())
            })
        })
}

/// GET /platform/oauth/start — legacy oauth-start (:72-213). Every
/// validation error renders the HTML error page.
pub async fn start(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    match start_impl(&state, &params).await {
        Ok(resp) => resp,
        Err(e) => oauth_error_page(&e.message),
    }
}

async fn start_impl(state: &AppState, params: &HashMap<String, String>) -> Result<Response> {
    let client_id = qp_uuid(params, "client_id")?;
    let redirect_uri = qp(params, "redirect_uri")?.to_string();
    let response_type = qp(params, "response_type")?;
    if response_type != "code" {
        return Err(InstantError::new(
            "param-malformed",
            400,
            "`response_type` parameter must have value `code`",
            None,
        ));
    }
    let scope_input = qp(params, "scope")?.to_string();
    let requested: Vec<String> = scope_input.split(' ').map(|s| s.to_string()).collect();
    if requested.is_empty() {
        return Err(InstantError::new(
            "param-malformed",
            400,
            "The scope param must specify at least one scope",
            None,
        ));
    }
    for s in &requested {
        if !ALL_SCOPES.contains(&s.as_str()) {
            return Err(InstantError::new(
                "param-malformed",
                400,
                format!("Invalid scope {s}"),
                Some(json!({"scope-input": scope_input, "invalid-scope": s})),
            ));
        }
    }
    let state_param = qp(params, "state")?.to_string();
    let code_challenge = params
        .get("code_challenge")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let code_challenge_method = match params.get("code_challenge_method") {
        None => None,
        Some(m) if m == "S256" || m == "plain" => Some(m.clone()),
        Some(m) => {
            return Err(param_malformed(
                &["params", "code_challenge_method"],
                json!(m),
            ))
        }
    };
    assert_valid_bare(
        "code-challenge",
        json!(code_challenge),
        if code_challenge_method.is_some() && code_challenge.is_none() {
            vec![
                "The code_challenge param must be provided when code_challenge_method is provided"
                    .to_string(),
            ]
        } else {
            vec![]
        },
    )?;
    assert_valid_bare(
        "code-challenge-method",
        json!(code_challenge_method),
        if code_challenge.is_some() && code_challenge_method.is_none() {
            vec![
                "The code_challenge_method param must be provided when code_challenge is provided"
                    .to_string(),
            ]
        } else {
            vec![]
        },
    )?;
    let ca = client_and_app(state, client_id).await?;
    if ca.is_public {
        let invalid: Vec<String> = requested
            .iter()
            .filter(|s| !ca.granted_scopes.contains(s))
            .cloned()
            .collect();
        assert_valid_bare(
            "scope",
            json!(scope_input),
            if invalid.is_empty() {
                vec![]
            } else {
                vec![format!(
                    "this OAuth app has not been granted the {} scopes",
                    join_in_sentence(&invalid)
                )]
            },
        )?;
    }
    assert_valid_bare(
        "redirect_uri",
        json!(redirect_uri),
        if ca
            .authorized_redirect_urls
            .iter()
            .any(|u| u == &redirect_uri)
        {
            vec![]
        } else {
            vec!["The redirect_uri does not appear in the set of authorized redirect uri for the OAuth client.".to_string()]
        },
    )?;
    assert_valid_bare(
        "redirect_uri",
        json!(redirect_uri),
        redirect_url_validation_errors_opt(&redirect_uri, !ca.is_public),
    )?;
    let cookie = Uuid::new_v4();
    let redirect_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO instant_oauth_app_redirects
           (lookup_key, client_id, state, cookie, redirect_uri, scopes, code_challenge_method, code_challenge, status, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'init', now() + make_interval(mins => $9))",
    )
    .bind(uuid_sha256(redirect_id))
    .bind(ca.client_id)
    .bind(&state_param)
    .bind(cookie)
    .bind(&redirect_uri)
    .bind(&requested)
    .bind(&code_challenge_method)
    .bind(&code_challenge)
    .bind(CODE_TTL_MINUTES as i32)
    .execute(&state.pool)
    .await?;
    let dash_url = add_query_params(
        &format!("{}/platform/oauth/start", state.cfg.dashboard_origin),
        &[("redirect-id", &redirect_id.to_string())],
    );
    let expires =
        (chrono::Utc::now() + chrono::Duration::hours(1)).format("%a, %d %b %Y %H:%M:%S GMT");
    let secure = if state.cfg.base_url.starts_with("https://") {
        "; Secure"
    } else {
        ""
    };
    let cookie_header = format!(
        "{COOKIE_NAME}=instantdb_{cookie}; HttpOnly{secure}; Expires={expires}; Path=/platform/oauth; SameSite=Lax"
    );
    Ok((
        StatusCode::FOUND,
        [
            (header::LOCATION, dash_url),
            (header::SET_COOKIE, cookie_header),
        ],
        "",
    )
        .into_response())
}

struct Redirect {
    client_id: Uuid,
    state: String,
    cookie: Uuid,
    redirect_uri: String,
    scopes: Vec<String>,
    code_challenge_method: Option<String>,
    code_challenge: Option<String>,
    user_id: Option<Uuid>,
    grant_token: Option<Uuid>,
    expires_at: chrono::DateTime<chrono::Utc>,
}

fn redirect_from_row(r: &sqlx::postgres::PgRow) -> Redirect {
    Redirect {
        client_id: r.get("client_id"),
        state: r.get("state"),
        cookie: r.get("cookie"),
        redirect_uri: r.get("redirect_uri"),
        scopes: r
            .get::<Option<Vec<String>>, _>("scopes")
            .unwrap_or_default(),
        code_challenge_method: r.get("code_challenge_method"),
        code_challenge: r.get("code_challenge"),
        user_id: r.get("user_id"),
        grant_token: r.get("grant_token"),
        expires_at: r.get("expires_at"),
    }
}

const REDIRECT_COLUMNS: &str = "client_id, state, cookie, redirect_uri, scopes, code_challenge_method, code_challenge, user_id, grant_token, expires_at";

/// POST /platform/oauth/claim — legacy claim-oauth-redirect (:215-259): the
/// dashboard (signed-in user) claims the redirect and gets the consent data.
pub async fn claim(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let body = parse_body(&body)?;
        let raw = body.get("redirect").filter(|v| !v.is_null()).ok_or_else(|| param_missing(&["body", "redirect"]))?;
        let redirect_id = raw
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| param_malformed(&["body", "redirect"], raw.clone()))?;
        let row = sqlx::query(&format!(
            "UPDATE instant_oauth_app_redirects SET user_id = $2, status = 'claimed', grant_token = gen_random_uuid()
              WHERE lookup_key = $1 AND user_id IS NULL AND status = 'init' RETURNING {REDIRECT_COLUMNS}"
        ))
        .bind(uuid_sha256(redirect_id))
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| record_not_found("oauth-app-redirect", json!({"args": [{"redirect-id": redirect_id}]})))?;
        let redirect = redirect_from_row(&row);
        if redirect.expires_at <= chrono::Utc::now() {
            return Err(expired("oauth-app-redirect", redirect.expires_at));
        }
        let ca = client_and_app(&state, redirect.client_id).await?;
        let host = url::Url::parse(&redirect.redirect_uri).ok().and_then(|u| u.host_str().map(|h| h.to_string()));
        let localhost = host.as_deref() == Some("localhost");
        if !ca.is_public || localhost {
            let app = live_app_row(&state, ca.app_id).await?;
            let member = app_role_for_user(&state, &app, user.id)
                .await?
                .any_role()
                .is_some();
            if !member {
                sqlx::query("DELETE FROM instant_oauth_app_redirects WHERE lookup_key = $1")
                    .bind(uuid_sha256(redirect_id))
                    .execute(&state.pool)
                    .await?;
                let message = if !ca.is_public {
                    "This OAuth app is not public, only members of the app may use it."
                } else {
                    "Redirects to localhost can only be used by members of the app."
                };
                return Err(InstantError::new("permission-denied", 400, message, None));
            }
        }
        Ok(json!({
            "appName": ca.app_name,
            "userEmail": user.email,
            "supportEmail": ca.support_email,
            "appPrivacyPolicyLink": ca.app_privacy_policy_link,
            "appLogo": ca.app_logo.map(|b| bytes_to_base64_image_url(&b)).unwrap_or(Value::Null),
            "appTosLink": ca.app_tos_link,
            "appHomePage": ca.app_home_page,
            "redirectOrigin": host,
            "scopes": redirect.scopes,
            "grantToken": redirect.grant_token,
        }))
    }
    .await;
    json_or_err(r)
}

/// Form (`application/x-www-form-urlencoded`) or JSON body, or the query
/// string: legacy reads `:params`, which ring fills from all three.
fn merged_params(
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    body: &Bytes,
) -> HashMap<String, String> {
    let mut out = query.clone();
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ct.starts_with("application/x-www-form-urlencoded") {
        for (k, v) in url::form_urlencoded::parse(body) {
            out.insert(k.to_string(), v.to_string());
        }
    } else if ct.starts_with("application/json") {
        if let Ok(Value::Object(m)) = serde_json::from_slice::<Value>(body) {
            for (k, v) in m {
                let s = match v {
                    Value::String(s) => s,
                    Value::Null => continue,
                    other => other.to_string(),
                };
                out.insert(k, s);
            }
        }
    }
    out
}

/// POST /platform/oauth/grant — legacy oauth-grant-access (:261-316)
pub async fn grant(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let params = merged_params(&headers, &query, &body);
    // outer errors (bad params, unknown redirect) → error page
    let redirect = match grant_outer(&state, &params).await {
        Ok(r) => r,
        Err(e) => return oauth_error_page(&e.message),
    };
    // inner errors → redirect back with an OAuth error
    match grant_inner(&state, &headers, &redirect).await {
        Ok(resp) => resp,
        Err(e) => {
            let code = match e.error_type.as_str() {
                "param-missing" | "param-malformed" => "invalid_request",
                _ => "server_error",
            };
            found(&add_query_params(
                &redirect.redirect_uri,
                &[
                    ("error", code),
                    ("error_description", &e.message),
                    ("state", &redirect.state),
                ],
            ))
        }
    }
}

async fn grant_outer(state: &AppState, params: &HashMap<String, String>) -> Result<Redirect> {
    let redirect_id = qp_uuid(params, "redirect_id")?;
    let grant_token = qp_uuid(params, "grant_token")?;
    // grant-redirect!: delete and return; a wrong grant token is "not found"
    let row = sqlx::query(&format!(
        "DELETE FROM instant_oauth_app_redirects WHERE lookup_key = $1 RETURNING {REDIRECT_COLUMNS}"
    ))
    .bind(uuid_sha256(redirect_id))
    .fetch_optional(&state.pool)
    .await?;
    let not_found = || {
        record_not_found(
            "oauth-app-redirect",
            json!({"args": [{"redirect-id": redirect_id}]}),
        )
    };
    let Some(row) = row else {
        return Err(not_found());
    };
    let redirect = redirect_from_row(&row);
    if redirect.grant_token != Some(grant_token) {
        return Err(not_found());
    }
    if redirect.expires_at <= chrono::Utc::now() {
        return Err(expired("oauth-app-redirect", redirect.expires_at));
    }
    Ok(redirect)
}

async fn grant_inner(
    state: &AppState,
    headers: &HeaderMap,
    redirect: &Redirect,
) -> Result<Response> {
    client_and_app(state, redirect.client_id).await?;
    let cookie = cookie_value(headers)
        .ok_or_else(|| InstantError::new("param-missing", 400, "Missing cookie.", None))?;
    if cookie != redirect.cookie {
        return Err(InstantError::new(
            "param-missing",
            400,
            "Invalid cookie.",
            None,
        ));
    }
    let code = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO instant_oauth_app_codes
           (hashed_code, client_id, redirect_uri, user_id, scopes, code_challenge, code_challenge_method, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, now() + make_interval(mins => $8))",
    )
    .bind(uuid_sha256(code))
    .bind(redirect.client_id)
    .bind(&redirect.redirect_uri)
    .bind(redirect.user_id)
    .bind(&redirect.scopes)
    .bind(&redirect.code_challenge)
    .bind(&redirect.code_challenge_method)
    .bind(CODE_TTL_MINUTES as i32)
    .execute(&state.pool)
    .await?;
    Ok(found(&add_query_params(
        &redirect.redirect_uri,
        &[
            ("code", &code.to_string()),
            ("state", &redirect.state),
            ("scope", &redirect.scopes.join(" ")),
        ],
    )))
}

/// POST /platform/oauth/deny — legacy oauth-deny-access (:318-338)
pub async fn deny(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let params = merged_params(&headers, &query, &body);
    let r = async {
        let redirect_id = qp_uuid(&params, "redirect_id")?;
        let _grant_token = qp_uuid(&params, "grant_token")?;
        let row = sqlx::query(&format!(
            "DELETE FROM instant_oauth_app_redirects WHERE lookup_key = $1 RETURNING {REDIRECT_COLUMNS}"
        ))
        .bind(uuid_sha256(redirect_id))
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| record_not_found("oauth-app-redirect", json!({"args": [{"redirect-id": redirect_id}]})))?;
        let redirect = redirect_from_row(&row);
        let cookie = cookie_value(&headers).ok_or_else(|| InstantError::new("param-missing", 400, "Missing cookie.", None))?;
        if cookie != redirect.cookie {
            return Err(InstantError::new("param-missing", 400, "Invalid cookie.", None));
        }
        Ok(found(&add_query_params(
            &redirect.redirect_uri,
            &[("error", "access_denied"), ("state", &redirect.state)],
        )))
    }
    .await;
    match r {
        Ok(resp) => resp,
        Err(e) => err_response(&e),
    }
}

struct Code {
    client_id: Uuid,
    redirect_uri: String,
    user_id: Uuid,
    scopes: Vec<String>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
}

/// `claim-code!` (oauth_app.clj:653-664)
async fn claim_code(state: &AppState, code: Uuid) -> Result<Code> {
    let row = sqlx::query(
        "DELETE FROM instant_oauth_app_codes WHERE hashed_code = $1
         RETURNING client_id, redirect_uri, user_id, scopes, code_challenge, code_challenge_method, expires_at",
    )
    .bind(uuid_sha256(code))
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| record_not_found("oauth-code", json!({"args": [{"code": code}]})))?;
    let expires_at: chrono::DateTime<chrono::Utc> = row.get("expires_at");
    if expires_at <= chrono::Utc::now() {
        return Err(expired("oauth-code", expires_at));
    }
    Ok(Code {
        client_id: row.get("client_id"),
        redirect_uri: row.get("redirect_uri"),
        user_id: row.get("user_id"),
        scopes: row
            .get::<Option<Vec<String>>, _>("scopes")
            .unwrap_or_default(),
        code_challenge: row.get("code_challenge"),
        code_challenge_method: row.get("code_challenge_method"),
    })
}

struct IssuedToken {
    value: String,
    scopes: Vec<String>,
    expires_in: i64,
}

async fn create_access_token(
    exec: &mut sqlx::PgConnection,
    client_id: Uuid,
    user_id: Uuid,
    scopes: &[String],
    refresh_lookup_key: Option<Vec<u8>>,
) -> Result<IssuedToken> {
    let value = format!("{PLATFORM_ACCESS_TOKEN_PREFIX}{}", random_hex(32));
    let row = sqlx::query(
        "INSERT INTO instant_user_oauth_access_tokens (lookup_key, refresh_token_lookup_key, client_id, user_id, scopes, expires_at)
         VALUES ($1, $2, $3, $4, $5, now() + make_interval(days => $6)) RETURNING scopes, expires_at",
    )
    .bind(token_lookup_key(&value))
    .bind(refresh_lookup_key)
    .bind(client_id)
    .bind(user_id)
    .bind(scopes)
    .bind(ACCESS_TOKEN_TTL_DAYS as i32)
    .fetch_one(&mut *exec)
    .await?;
    let expires_at: chrono::DateTime<chrono::Utc> = row.get("expires_at");
    Ok(IssuedToken {
        value,
        scopes: row
            .get::<Option<Vec<String>>, _>("scopes")
            .unwrap_or_default(),
        expires_in: (expires_at - chrono::Utc::now()).num_seconds(),
    })
}

fn pm(params: &HashMap<String, String>, key: &str) -> Result<String> {
    params
        .get(key)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| param_missing(&[key]))
}
fn pm_uuid(params: &HashMap<String, String>, key: &str) -> Result<Uuid> {
    let raw = params.get(key).ok_or_else(|| param_missing(&[key]))?;
    Uuid::parse_str(raw.trim()).map_err(|_| param_malformed(&[key], json!(raw)))
}

/// POST /platform/oauth/token — legacy oauth-token (:456-499): PKCE
/// (no client secret) or confidential (`authorization_code` /
/// `refresh_token`).
pub async fn token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let params = if ct.starts_with("application/x-www-form-urlencoded")
        || ct.starts_with("application/json")
    {
        merged_params(&headers, &HashMap::new(), &body)
    } else {
        query
    };
    json_or_err(token_impl(&state, &params).await)
}

async fn token_impl(state: &AppState, params: &HashMap<String, String>) -> Result<Value> {
    let grant_type = pm(params, "grant_type")?;
    let client_id = pm_uuid(params, "client_id")?;
    let client_secret = params
        .get("client_secret")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let Some(client_secret) = client_secret else {
        if grant_type != "authorization_code" {
            return Err(InstantError::new(
                "param-malformed",
                400,
                "Unrecognized `grant_type` parameter only `authorization_code` is allowed for the PKCE flow.",
                Some(json!({"input": grant_type})),
            ));
        }
        // PKCE
        let redirect_uri = pm(params, "redirect_uri")?;
        let code_param = pm_uuid(params, "code")?;
        let code = claim_code(state, code_param).await?;
        if code.client_id != client_id {
            return Err(InstantError::new(
                "param-malformed",
                400,
                "Invalid client_id parameter",
                Some(json!({"input": client_id})),
            ));
        }
        if !constant_eq(redirect_uri.as_bytes(), code.redirect_uri.as_bytes()) {
            return Err(InstantError::new(
                "param-malformed",
                400,
                "Invalid redirect_uri parameter",
                Some(json!({"input": redirect_uri})),
            ));
        }
        if code.code_challenge.is_none() || code.code_challenge_method.is_none() {
            return Err(InstantError::new(
                "param-missing",
                400,
                "You must provide the client_secret from a secure server or use the client-side PKCE flow to exchange the OAuth code for a token.",
                None,
            ));
        }
        let verifier = pm(params, "code_verifier")?;
        crate::routes::oauth::verify_pkce_for(
            "oauth-code",
            code.code_challenge.as_deref(),
            code.code_challenge_method.as_deref(),
            Some(&verifier),
        )?;
        let mut conn = state.pool.acquire().await?;
        let access =
            create_access_token(&mut conn, code.client_id, code.user_id, &code.scopes, None)
                .await?;
        return Ok(json!({
            "access_token": access.value,
            "expires_in": access.expires_in,
            "token_type": "Bearer",
            "scopes": access.scopes.join(" "),
        }));
    };
    // confidential client: the secret must belong to a client
    let client_row = sqlx::query(
        "SELECT c.client_id FROM instant_oauth_app_clients c
           JOIN instant_oauth_app_client_secrets s ON s.client_id = c.client_id
          WHERE s.hashed_secret = $1",
    )
    .bind(token_lookup_key(&client_secret))
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| {
        record_not_found(
            "oauth-app-client",
            json!({"args": [{"client-id": client_id}]}),
        )
    })?;
    let secret_client: Uuid = client_row.get("client_id");
    match grant_type.as_str() {
        "authorization_code" => {
            let redirect_uri = pm(params, "redirect_uri")?;
            let code_param = pm_uuid(params, "code")?;
            let code = claim_code(state, code_param).await?;
            if code.client_id != secret_client {
                return Err(InstantError::new("param-malformed", 400, "Invalid client_id parameter", Some(json!({"input": secret_client}))));
            }
            if !constant_eq(redirect_uri.as_bytes(), code.redirect_uri.as_bytes()) {
                return Err(InstantError::new("param-malformed", 400, "Invalid redirect_uri parameter", Some(json!({"input": redirect_uri}))));
            }
            let mut dbtx = state.pool.begin().await?;
            let refresh_value = format!("{PLATFORM_REFRESH_TOKEN_PREFIX}{}", random_hex(32));
            let refresh_key = token_lookup_key(&refresh_value);
            sqlx::query(
                "INSERT INTO instant_user_oauth_refresh_tokens (lookup_key, client_id, user_id, scopes) VALUES ($1, $2, $3, $4)",
            )
            .bind(&refresh_key)
            .bind(code.client_id)
            .bind(code.user_id)
            .bind(&code.scopes)
            .execute(&mut *dbtx)
            .await?;
            let access = create_access_token(&mut dbtx, code.client_id, code.user_id, &code.scopes, Some(refresh_key)).await?;
            sqlx::query(
                "DELETE FROM instant_user_oauth_refresh_tokens WHERE lookup_key IN (
                   SELECT lookup_key FROM instant_user_oauth_refresh_tokens
                    WHERE client_id = $1 AND user_id = $2 ORDER BY created_at DESC OFFSET $3)",
            )
            .bind(code.client_id)
            .bind(code.user_id)
            .bind(REFRESH_TOKEN_LIMIT)
            .execute(&mut *dbtx)
            .await?;
            dbtx.commit().await?;
            Ok(json!({
                "access_token": access.value,
                "expires_in": access.expires_in,
                "token_type": "Bearer",
                "refresh_token": refresh_value,
                "scopes": access.scopes.join(" "),
            }))
        }
        "refresh_token" => {
            let refresh = pm(params, "refresh_token")?;
            let row = sqlx::query(
                "SELECT lookup_key, user_id, scopes FROM instant_user_oauth_refresh_tokens WHERE lookup_key = $1 AND client_id = $2",
            )
            .bind(token_lookup_key(&refresh))
            .bind(secret_client)
            .fetch_optional(&state.pool)
            .await?
            .ok_or_else(|| {
                InstantError::new(
                    "record-not-found",
                    400,
                    "Record not found: oauth-refresh-token",
                    Some(json!({"record-type": "oauth-refresh-token"})),
                )
            })?;
            let user_id: Uuid = row.get("user_id");
            let scopes: Vec<String> = row.get::<Option<Vec<String>>, _>("scopes").unwrap_or_default();
            let key: Vec<u8> = row.get("lookup_key");
            let mut conn = state.pool.acquire().await?;
            let access = create_access_token(&mut conn, secret_client, user_id, &scopes, Some(key)).await?;
            Ok(json!({
                "access_token": access.value,
                "expires_in": access.expires_in,
                "token_type": "Bearer",
                // legacy's refresh path says `scope`, the code paths `scopes`
                "scope": access.scopes.join(" "),
            }))
        }
        other => Err(InstantError::new(
            "param-malformed",
            400,
            "Unrecognized `grant_type` parameter, expected either `authorization_code` or `refresh_token`",
            Some(json!({"input": other})),
        )),
    }
}

fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// GET /platform/oauth/token-info — legacy get-token-info (:501-517)
pub async fn token_info(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let r = async {
        let token = pm(&params, "access_token")?;
        if !token.starts_with(PLATFORM_ACCESS_TOKEN_PREFIX) {
            return Err(InstantError::validation_failed_input(
                "access_token",
                json!({}),
                json!([{"message": "The access_token is not a valid platform OAuth access token."}]),
            ));
        }
        let record = access_token_by_value(&state, &token).await?;
        Ok(json!({
            "expires_in": (record.expires_at - chrono::Utc::now()).num_seconds(),
            "token_type": "Bearer",
            "scopes": record.scopes.join(" "),
        }))
    }
    .await;
    json_or_err(r)
}

/// POST /platform/oauth/revoke — legacy revoke-oauth-token (:519-532)
pub async fn revoke(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let params = merged_params(&headers, &query, &body);
    let r = async {
        let token = qp(&params, "token")?.to_string();
        if token.starts_with(PLATFORM_REFRESH_TOKEN_PREFIX) {
            sqlx::query("DELETE FROM instant_user_oauth_refresh_tokens WHERE lookup_key = $1")
                .bind(token_lookup_key(&token))
                .execute(&state.pool)
                .await?;
        } else if token.starts_with(PLATFORM_ACCESS_TOKEN_PREFIX) {
            sqlx::query("DELETE FROM instant_user_oauth_access_tokens WHERE lookup_key = $1")
                .bind(token_lookup_key(&token))
                .execute(&state.pool)
                .await?;
        } else {
            return Err(InstantError::new(
                "param-malformed",
                400,
                "Token is not a access token or a refresh token.",
                None,
            ));
        }
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}

#[allow(dead_code)]
fn _unused(_: &Map<String, Value>) {}
