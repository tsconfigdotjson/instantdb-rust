//! `/superadmin/*` — the platform API (`@instantdb/platform` PlatformApi):
//! apps and orgs of a user identified by a personal access token (`per_`)
//! or a platform OAuth access token (`pat_`, scoped). Port of LEGACY
//! superadmin/routes.clj; the token model is util/token.clj +
//! model/instant_personal_access_token.clj + model/oauth_app.clj.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use instant_core::error::{InstantError, Result};
use instant_core::schema;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::routes::dash::{param_malformed, param_missing, parse_body, DashRole, DashUser};
use crate::routes::dash_apps::{
    app_role_for_user, app_row, apps_for_org, assert_app_access, body_str, create_app,
    live_app_row, org_role_for_user, orgs_for_user, path_uuid, record_not_found,
};
use crate::routes::dash_manage::{app_and_user, token_lookup_key};
use crate::routes::runtime::{coerce_email_pub, json_or_err};
use crate::service;
use crate::state::AppState;

// ---------------------------------------------------------------------------
// tokens (util/token.clj)

pub(crate) const PLATFORM_REFRESH_TOKEN_PREFIX: &str = "prt_";
pub(crate) const PLATFORM_ACCESS_TOKEN_PREFIX: &str = "pat_";
pub(crate) const PERSONAL_ACCESS_TOKEN_PREFIX: &str = "per_";

/// OAuth scopes (model/oauth_app.clj:18-46); `*-write` satisfies `*-read`.
/// The data / storage scopes gate the admin-style routes (webhooks, storage)
/// that accept platform tokens.
#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    AppsRead,
    AppsWrite,
    DataRead,
    DataWrite,
    StorageRead,
    StorageWrite,
    /// no OAuth app can hold this one: transfers are PAT-only
    AppsTransfer,
}

impl Scope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Scope::AppsRead => "apps-read",
            Scope::AppsWrite => "apps-write",
            Scope::DataRead => "data-read",
            Scope::DataWrite => "data-write",
            Scope::StorageRead => "storage-read",
            Scope::StorageWrite => "storage-write",
            Scope::AppsTransfer => "apps-transfer",
        }
    }
    pub(crate) fn satisfied_by(self, scopes: &[String]) -> bool {
        let has = |s: &str| scopes.iter().any(|x| x == s);
        match self {
            Scope::AppsRead => has("apps-read") || has("apps-write"),
            Scope::AppsWrite => has("apps-write"),
            Scope::DataRead => has("data-read") || has("data-write"),
            Scope::DataWrite => has("data-write"),
            Scope::StorageRead => has("storage-read") || has("storage-write"),
            Scope::StorageWrite => has("storage-write"),
            Scope::AppsTransfer => false,
        }
    }
}

pub(crate) const ALL_SCOPES: [&str; 6] = [
    "apps-read",
    "apps-write",
    "data-read",
    "data-write",
    "storage-read",
    "storage-write",
];

/// `req->bearer-token!` (util/http.clj:15-25): the raw bearer string.
pub(crate) fn bearer_string(headers: &HeaderMap) -> Result<String> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| param_missing(&["headers", "authorization"]))?;
    auth.strip_prefix("Bearer ")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| param_malformed(&["headers", "authorization"], json!(auth)))
}

pub(crate) fn is_platform_access_token(s: &str) -> bool {
    s.starts_with(PLATFORM_ACCESS_TOKEN_PREFIX)
}
pub(crate) fn is_personal_access_token(s: &str) -> bool {
    s.starts_with(PERSONAL_ACCESS_TOKEN_PREFIX)
}

fn user_from_row(r: &sqlx::postgres::PgRow) -> DashUser {
    DashUser {
        id: r.get("id"),
        email: r.get("email"),
        created_at: r.try_get("created_at").ok(),
        google_sub: r.try_get("google_sub").ok(),
    }
}

/// `instant-user-model/get-by-personal-access-token` (instant_user.clj:127-138)
pub(crate) async fn user_by_personal_access_token(
    state: &AppState,
    token: &str,
) -> Result<Option<DashUser>> {
    let row = sqlx::query(
        "SELECT u.id, u.email, u.created_at, u.google_sub FROM instant_users u
           JOIN instant_personal_access_tokens t ON u.id = t.user_id
          WHERE t.lookup_key = $1",
    )
    .bind(token_lookup_key(token))
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.map(|r| user_from_row(&r)))
}

pub(crate) struct AccessToken {
    pub user_id: Uuid,
    pub scopes: Vec<String>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// `oauth-app-model/access-token-by-token-value!` (oauth_app.clj:774-787):
/// record-not-found (no hint args) then the expiry check.
pub(crate) async fn access_token_by_value(state: &AppState, token: &str) -> Result<AccessToken> {
    let row = sqlx::query(
        "SELECT user_id, scopes, expires_at FROM instant_user_oauth_access_tokens WHERE lookup_key = $1",
    )
    .bind(token_lookup_key(token))
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| {
        InstantError::new(
            "record-not-found",
            400,
            "Record not found: oauth-access-token",
            Some(json!({"record-type": "oauth-access-token"})),
        )
    })?;
    let expires_at: chrono::DateTime<chrono::Utc> = row.get("expires_at");
    if expires_at <= chrono::Utc::now() {
        return Err(expired("oauth-access-token", expires_at));
    }
    Ok(AccessToken {
        user_id: row.get("user_id"),
        scopes: row.get::<Vec<String>, _>("scopes"),
        expires_at,
    })
}

/// `assert-not-expired!` (oauth_app.clj:551-556)
pub(crate) fn expired(
    record_type: &str,
    expires_at: chrono::DateTime<chrono::Utc>,
) -> InstantError {
    InstantError::new(
        "record-expired",
        400,
        format!("Record expired: {record_type}"),
        Some(json!({"expired_at": expires_at.format("%Y-%m-%dT%H:%M:%SZ").to_string()})),
    )
}

fn missing_scope(scope: Scope) -> InstantError {
    InstantError::new(
        "permission-denied",
        400,
        format!("You are missing the {} scope", scope.as_str()),
        Some(json!({"required-scope": scope.as_str()})),
    )
}

/// `req->superadmin-user!` (superadmin/routes.clj:34-64): a platform access
/// token with the scope, else the bearer as a personal access token (any
/// string counts, for pre-May-2025 tokens without the `per_` prefix).
pub(crate) async fn superadmin_user(
    state: &AppState,
    headers: &HeaderMap,
    scope: Scope,
) -> Result<DashUser> {
    let token = bearer_string(headers)?;
    superadmin_user_for_token(state, &token, scope).await
}

pub(crate) async fn superadmin_user_for_token(
    state: &AppState,
    token: &str,
    scope: Scope,
) -> Result<DashUser> {
    if is_platform_access_token(token) {
        let record = access_token_by_value(state, token).await?;
        if !scope.satisfied_by(&record.scopes) {
            return Err(missing_scope(scope));
        }
        let row = sqlx::query(
            "SELECT id, email, created_at, google_sub FROM instant_users WHERE id = $1",
        )
        .bind(record.user_id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| {
            record_not_found("instant-user", json!({"args": [{"id": record.user_id}]}))
        })?;
        return Ok(user_from_row(&row));
    }
    user_by_personal_access_token(state, token)
        .await?
        .ok_or_else(|| record_not_found("instant-user", json!({})))
}

/// `req->superadmin-user-and-app!`: the user, then `get-app-with-role!`.
pub(crate) async fn superadmin_user_and_app(
    state: &AppState,
    headers: &HeaderMap,
    scope: Scope,
    role: DashRole,
    app_id_raw: &str,
) -> Result<(DashUser, Value, DashRole)> {
    let user = superadmin_user(state, headers, scope).await?;
    let app_id = path_uuid(app_id_raw, "app_id")?;
    let app = live_app_row(state, app_id).await?;
    let access = app_role_for_user(state, &app, user.id).await?;
    let user_role = assert_app_access(role, access)?;
    Ok((user, app, user_role))
}

/// Does this bearer string authenticate through the superadmin path
/// (`req->superadmin-app!`'s first branch): a `pat_` / `per_` token, or any
/// string that resolves as a personal access token.
pub(crate) async fn is_superadmin_token(state: &AppState, token: &str) -> Result<bool> {
    if is_platform_access_token(token) || is_personal_access_token(token) {
        return Ok(true);
    }
    Ok(user_by_personal_access_token(state, token).await?.is_some())
}

pub(crate) fn admin_token_mismatch(requested: Uuid) -> InstantError {
    InstantError::new(
        "validation-failed",
        400,
        format!(
            "This admin token does not belong to app {requested}. Admin tokens are bound to a single app. Use a personal access token to manage other apps."
        ),
        Some(json!({"reason": "admin-token-mismatch"})),
    )
}

/// `req->superadmin-app!` (superadmin/routes.clj:72-95): superadmin token →
/// user + role on the app; else the app's admin token (with the mismatch
/// error when the path names another app); else a dashboard refresh token
/// with the role.
pub(crate) async fn superadmin_app(
    state: &AppState,
    headers: &HeaderMap,
    scope: Scope,
    role: DashRole,
    app_id_raw: &str,
) -> Result<Value> {
    let token = bearer_string(headers)?;
    if is_superadmin_token(state, &token).await? {
        return Ok(
            superadmin_user_and_app(state, headers, scope, role, app_id_raw)
                .await?
                .1,
        );
    }
    if let Ok(token_uuid) = Uuid::parse_str(&token) {
        let admin_app: Option<Uuid> =
            sqlx::query("SELECT a.id FROM app_admin_tokens t JOIN apps a ON a.id = t.app_id WHERE t.token = $1 AND a.deletion_marked_at IS NULL")
                .bind(token_uuid)
                .fetch_optional(&state.pool)
                .await?
                .map(|r| r.get("id"));
        if let Some(admin_app) = admin_app {
            let requested = path_uuid(app_id_raw, "app_id")?;
            if requested != admin_app {
                return Err(admin_token_mismatch(requested));
            }
            return live_app_row(state, admin_app).await;
        }
    }
    let (_, app, _) = app_and_user(state, headers, app_id_raw, role).await?;
    Ok(app)
}

// ---------------------------------------------------------------------------
// enhance-apps: ?include=schema,perms

async fn enhance_apps(
    state: &AppState,
    params: &HashMap<String, String>,
    apps: Vec<Value>,
) -> Result<Vec<Value>> {
    let includes: Vec<&str> = params
        .get("include")
        .map(|s| {
            s.split(',')
                .map(|x| x.trim())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let want_schema = includes.contains(&"schema");
    let want_perms = includes.contains(&"perms");
    if !want_schema && !want_perms {
        return Ok(apps);
    }
    let mut out = vec![];
    for mut app in apps {
        let app_id = app
            .get("id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or_default();
        if let Some(m) = app.as_object_mut() {
            if want_schema {
                let attrs = service::load_attrs(state, app_id).await?;
                m.insert("schema".into(), schema::attrs_to_schema(&attrs).to_wire());
            }
            if want_perms {
                let code: Option<Value> = sqlx::query("SELECT code FROM rules WHERE app_id = $1")
                    .bind(app_id)
                    .fetch_optional(&state.pool)
                    .await?
                    .map(|r| r.get("code"));
                m.insert("perms".into(), code.unwrap_or(Value::Null));
            }
        }
        out.push(app);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// orgs

/// GET /superadmin/orgs — legacy orgs-list-get (:108-111)
pub async fn orgs_list(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let r = async {
        let user = superadmin_user(&state, &headers, Scope::AppsRead).await?;
        Ok(json!({"orgs": orgs_for_user(&state, user.id).await?}))
    }
    .await;
    json_or_err(r)
}

/// GET /superadmin/orgs/:org_id/apps — legacy orgs-list-apps-get (:113-119)
pub async fn org_apps_list(
    State(state): State<Arc<AppState>>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let r = async {
        let user = superadmin_user(&state, &headers, Scope::AppsRead).await?;
        let org_id = path_uuid(&org_id, "org_id")?;
        let org = org_role_for_user(&state, org_id, user.id, DashRole::Collaborator).await?;
        let apps = apps_for_org(&state, org_id, user.id, &org).await?;
        Ok(json!({"apps": enhance_apps(&state, &params, apps).await?}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// apps

/// GET /superadmin/apps — legacy apps-list-get (:123-126): the apps the
/// user created (`apps.*`).
pub async fn apps_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let r = async {
        let user = superadmin_user(&state, &headers, Scope::AppsRead).await?;
        let rows = sqlx::query(
            "SELECT id, creator_id, org_id, title, created_at, status, deletion_marked_at,
                    subscription_id, magic_code_expiry_minutes, connection_string
               FROM apps a WHERE a.creator_id = $1 AND a.deletion_marked_at IS NULL",
        )
        .bind(user.id)
        .fetch_all(&state.pool)
        .await?;
        let apps: Vec<Value> = rows
            .iter()
            .map(crate::routes::dash_apps::app_row_json)
            .collect();
        Ok(json!({"apps": enhance_apps(&state, &params, apps).await?}))
    }
    .await;
    json_or_err(r)
}

/// POST /superadmin/apps — legacy apps-create-post (:128-166): `title`,
/// optional `org_id` (admin of the org), `schema`, `perms`.
pub async fn apps_create(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = superadmin_user(&state, &headers, Scope::AppsWrite).await?;
        let body = parse_body(&body)?;
        let title = body_str(&body, "title")?;
        let org_id = match body.get("org_id") {
            None | Some(Value::Null) => None,
            Some(v) => Some(
                v.as_str()
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .ok_or_else(|| param_malformed(&["body", "org_id"], v.clone()))?,
            ),
        };
        if let Some(org_id) = org_id {
            org_role_for_user(&state, org_id, user.id, DashRole::Admin).await?;
        }
        let perms = match body.get("perms") {
            None | Some(Value::Null) => None,
            Some(code) => {
                let errors = instant_core::perms::validation_errors(code);
                if !errors.is_empty() {
                    let msgs: Vec<&str> = errors
                        .iter()
                        .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                        .collect();
                    return Err(InstantError::new(
                        "validation-failed",
                        400,
                        format!("Validation failed for perms: {}", msgs.join(", ")),
                        Some(json!({"data-type": "perms", "input": code, "errors": errors})),
                    ));
                }
                Some(code.clone())
            }
        };
        let id = Uuid::new_v4();
        let (creator, org) = match org_id {
            Some(o) => (None, Some(o)),
            None => (Some(user.id), None),
        };
        crate::routes::dash_apps::assert_app_limit(&state, user.id).await?;
        let mut app = create_app(&state, id, &title, creator, org, Uuid::new_v4()).await?;
        let perms_code = match &perms {
            Some(code) => {
                let row = sqlx::query(
                    "INSERT INTO rules (app_id, code) VALUES ($1, $2)
                     ON CONFLICT (app_id) DO UPDATE SET code = excluded.code, version = rules.version + 1
                     WHERE rules.code IS DISTINCT FROM excluded.code
                     RETURNING code",
                )
                .bind(id)
                .bind(code)
                .fetch_optional(&state.pool)
                .await?;
                row.map(|r| r.get::<Value, _>("code")).unwrap_or(Value::Null)
            }
            None => Value::Null,
        };
        if let Some(schema_defs) = body.get("schema").filter(|s| !s.is_null()) {
            let plan_body =
                json!({"schema": schema_defs, "check_types": true, "supports_background_updates": false});
            crate::routes::dash::plan_and_apply(&state, id, &plan_body).await?;
        }
        let attrs = service::load_attrs(&state, id).await?;
        if let Some(m) = app.as_object_mut() {
            m.insert("perms".into(), perms_code);
            m.insert("schema".into(), schema::attrs_to_schema(&attrs).to_wire());
        }
        Ok(json!({"app": app}))
    }
    .await;
    json_or_err(r)
}

/// GET /superadmin/apps/:app_id — legacy app-details-get (:168-170)
pub async fn app_details(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = superadmin_app(
            &state,
            &headers,
            Scope::AppsRead,
            DashRole::Collaborator,
            &app_id,
        )
        .await?;
        Ok(json!({"app": app}))
    }
    .await;
    json_or_err(r)
}

/// POST /superadmin/apps/:app_id — legacy app-update-post (:172-176): rename
pub async fn app_update(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app =
            superadmin_app(&state, &headers, Scope::AppsWrite, DashRole::Admin, &app_id).await?;
        let body = parse_body(&body)?;
        let title = body_str(&body, "title")?;
        let id = app
            .get("id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or_default();
        sqlx::query("UPDATE apps SET title = $1 WHERE id = $2")
            .bind(title)
            .bind(id)
            .execute(&state.pool)
            .await?;
        // next.jdbc execute-one! returns the updated row
        Ok(json!({"app": app_row(&state, id).await?}))
    }
    .await;
    json_or_err(r)
}

/// DELETE /superadmin/apps/:app_id — legacy app-delete (:178-192): admin,
/// and the owner for a personal app.
pub async fn app_delete(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let (user, app, _) =
            superadmin_user_and_app(&state, &headers, Scope::AppsWrite, DashRole::Admin, &app_id)
                .await?;
        let creator = app
            .get("creator_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok());
        if let Some(creator) = creator {
            if creator != user.id {
                return Err(InstantError::new(
                    "permission-denied",
                    400,
                    "Permission denied: not allowed-member-role?",
                    Some(json!({"input": "owner", "expected": "allowed-member-role?"})),
                ));
            }
        }
        let id = app
            .get("id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or_default();
        sqlx::query("UPDATE apps SET deletion_marked_at = NOW() WHERE id = $1")
            .bind(id)
            .execute(&state.pool)
            .await?;
        service::invalidate_attrs(&state, id);
        Ok(json!({"app": app_row(&state, id).await?}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// transfers

fn body_email(body: &Value, key: &str) -> Result<String> {
    let raw = body
        .get(key)
        .filter(|v| !v.is_null())
        .ok_or_else(|| param_missing(&["body", key]))?;
    raw.as_str()
        .and_then(|s| coerce_email_pub(s).ok())
        .ok_or_else(|| param_malformed(&["body", key], raw.clone()))
}

/// POST /superadmin/apps/:app_id/transfers/send — legacy
/// app-transfer-send-invite-post (:218-233): PAT only (the `apps-transfer`
/// scope exists for no OAuth app); a `creator` invite is upserted, the
/// transfer email goes out through the configured provider, and the
/// upserted invite's id is returned.
pub async fn transfer_send(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (user, app, _) =
            superadmin_user_and_app(&state, &headers, Scope::AppsTransfer, DashRole::Owner, &app_id).await?;
        let body = parse_body(&body)?;
        let email = body_email(&body, "dest_email")?;
        let id = app.get("id").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok()).unwrap_or_default();
        let row = sqlx::query(
            "INSERT INTO app_member_invites (id, app_id, inviter_id, invitee_email, invitee_role, status, sent_at)
             VALUES ($1, $2, $3, $4, 'creator', 'pending', now())
             ON CONFLICT (app_id, invitee_email)
             DO UPDATE SET status = 'pending', sent_at = now(), invitee_role = excluded.invitee_role
             RETURNING id",
        )
        .bind(Uuid::new_v4())
        .bind(id)
        .bind(user.id)
        .bind(&email)
        .fetch_one(&state.pool)
        .await?;
        crate::email::deliver_transfer_invite(
            &state,
            &email,
            &user.email,
            app.get("title").and_then(|v| v.as_str()).unwrap_or_default(),
        );
        Ok(json!({"id": row.get::<Uuid, _>("id")}))
    }
    .await;
    json_or_err(r)
}

/// POST /superadmin/apps/:app_id/transfers/revoke — legacy
/// app-transfer-revoke-post (:235-245)
pub async fn transfer_revoke(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (user, app, _) = superadmin_user_and_app(
            &state,
            &headers,
            Scope::AppsTransfer,
            DashRole::Owner,
            &app_id,
        )
        .await?;
        let body = parse_body(&body)?;
        let email = body_email(&body, "dest_email")?;
        let id = app
            .get("id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or_default();
        let res = sqlx::query(
            "UPDATE app_member_invites SET status = 'revoked'
              WHERE inviter_id = $1 AND app_id = $2 AND invitee_email = $3
                AND invitee_role = 'creator' AND status = 'pending'",
        )
        .bind(user.id)
        .bind(id)
        .bind(&email)
        .execute(&state.pool)
        .await?;
        Ok(json!({"count": res.rows_affected()}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// rules + schema

fn app_uuid(app: &Value) -> Uuid {
    app.get("id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_default()
}

/// GET /superadmin/apps/:app_id/perms — legacy app-rules-get (:249-252)
pub async fn perms_get(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = superadmin_app(
            &state,
            &headers,
            Scope::AppsRead,
            DashRole::Collaborator,
            &app_id,
        )
        .await?;
        let code: Option<Value> = sqlx::query("SELECT code FROM rules WHERE app_id = $1")
            .bind(app_uuid(&app))
            .fetch_optional(&state.pool)
            .await?
            .map(|r| r.get("code"));
        Ok(json!({"perms": code.unwrap_or(Value::Null)}))
    }
    .await;
    json_or_err(r)
}

/// POST /superadmin/apps/:app_id/perms — legacy app-rules-post (:254-262)
pub async fn perms_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = superadmin_app(
            &state,
            &headers,
            Scope::AppsWrite,
            DashRole::Collaborator,
            &app_id,
        )
        .await?;
        let body = parse_body(&body)?;
        let code = body
            .get("code")
            .filter(|v| !v.is_null())
            .cloned()
            .ok_or_else(|| param_missing(&["body", "code"]))?;
        let errors = instant_core::perms::validation_errors(&code);
        if !errors.is_empty() {
            let msgs: Vec<&str> = errors
                .iter()
                .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                .collect();
            return Err(InstantError::new(
                "validation-failed",
                400,
                format!("Validation failed for rule: {}", msgs.join(", ")),
                Some(json!({"data-type": "rule", "input": code, "errors": errors})),
            ));
        }
        let row = sqlx::query(
            "INSERT INTO rules (app_id, code) VALUES ($1, $2)
             ON CONFLICT (app_id) DO UPDATE SET code = excluded.code, version = rules.version + 1
             WHERE rules.code IS DISTINCT FROM excluded.code
             RETURNING app_id, code, version",
        )
        .bind(app_uuid(&app))
        .bind(&code)
        .fetch_optional(&state.pool)
        .await?;
        let rules = row.map(|r| {
            json!({
                "app_id": r.get::<Uuid, _>("app_id"),
                "code": r.get::<Value, _>("code"),
                "version": r.get::<i32, _>("version"),
            })
        });
        Ok(json!({"rules": rules}))
    }
    .await;
    json_or_err(r)
}

/// GET /superadmin/apps/:app_id/schema — legacy app-schema-get (:266-270)
pub async fn schema_get(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = superadmin_app(
            &state,
            &headers,
            Scope::AppsRead,
            DashRole::Collaborator,
            &app_id,
        )
        .await?;
        let attrs = service::load_attrs(&state, app_uuid(&app)).await?;
        Ok(json!({"schema": schema::attrs_to_schema(&attrs).to_wire()}))
    }
    .await;
    json_or_err(r)
}

/// POST /superadmin/apps/:app_id/schema/push/plan — legacy app-schema-plan-post (:272-281)
pub async fn schema_plan(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = superadmin_app(
            &state,
            &headers,
            Scope::AppsRead,
            DashRole::Collaborator,
            &app_id,
        )
        .await?;
        let body = parse_body(&body)?;
        Ok(crate::routes::dash::plan(&state, app_uuid(&app), &body)
            .await?
            .wire)
    }
    .await;
    json_or_err(r)
}

/// POST /superadmin/apps/:app_id/schema/push/apply — legacy app-schema-apply-post (:283-296)
pub async fn schema_apply(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = superadmin_app(
            &state,
            &headers,
            Scope::AppsWrite,
            DashRole::Collaborator,
            &app_id,
        )
        .await?;
        let body = parse_body(&body)?;
        let id = app_uuid(&app);
        let plan = crate::routes::dash::plan(&state, id, &body).await?;
        let applied = crate::routes::dash::apply_steps(&state, id, plan.steps, false).await?;
        let mut out = plan.wire;
        if let (Some(o), Some(a)) = (out.as_object_mut(), applied.as_object()) {
            for (k, v) in a {
                o.insert(k.clone(), v.clone());
            }
        }
        Ok(out)
    }
    .await;
    json_or_err(r)
}
