//! Dashboard app-management and account routes (the part of LEGACY
//! dash/routes.clj the CLI does not use but the dashboard and the platform
//! SDK do): rename / clear / status / admin-token rotation / magic-code
//! expiry / rule versions / soft-deleted attrs / test users / stats /
//! dashboard storage / test emails, and the account routes (profiles,
//! signout, dashboard magic-code login, personal access tokens, check-admin),
//! plus the experimental `/admin/schema` + `/admin/soft_deleted_attrs`.
//!
//! Auth follows the legacy helper each handler uses:
//! * `req->app-and-user!` (dashboard refresh token only, role checked) —
//!   [`app_and_user`]
//! * `req->app-accepting-superadmin-or-ref-token!` (admin token or refresh
//!   token) — `dash_authed_with_role`
//! * `req->auth-user!` (refresh token only) — `dash_user`
//! * `req->app-id-authed!` (`/admin/*`: admin token) — `admin::authed_admin`

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use instant_core::attr;
use instant_core::error::{InstantError, Result};
use instant_core::schema;
use serde_json::{json, Map, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::routes::admin;
use crate::routes::dash::{
    coerce_non_blank_str, dash_authed_with_role, dash_user, param_malformed, param_missing,
    parse_body, DashRole, DashUser,
};
use crate::routes::dash_apps::{
    app_role_for_user, app_row, assert_app_access, body_str, body_uuid, live_app_row, path_uuid,
    record_not_found, ts_naive, ts_tz,
};
use crate::routes::runtime::{coerce_email_pub, json_or_err};
use crate::service;
use crate::state::AppState;

/// legacy `hard-deletion-sweeper/grace-period-days`
const GRACE_PERIOD_DAYS: i64 = 2;

/// Legacy `req->app-and-user!` (util/http.clj:71-79): the app id param,
/// then the dashboard user, then `get-app-with-role!`.
pub(crate) async fn app_and_user(
    state: &AppState,
    headers: &HeaderMap,
    app_id_raw: &str,
    least: DashRole,
) -> Result<(DashUser, Value, DashRole)> {
    let app_id = path_uuid(app_id_raw, "app_id")?;
    let user = dash_user(state, headers).await?;
    let app = live_app_row(state, app_id).await?;
    let access = app_role_for_user(state, &app, user.id).await?;
    let role = assert_app_access(least, access)?;
    Ok((user, app, role))
}

fn app_uuid(app: &Value) -> Uuid {
    app.get("id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_default()
}

fn validation_err(data_type: &str, input: Value, message: &str) -> InstantError {
    InstantError::new(
        "validation-failed",
        400,
        format!("Validation failed for {data_type}: {message}"),
        Some(json!({"data-type": data_type, "input": input, "errors": [{"message": message}]})),
    )
}

/// A timestamp column that may be `timestamp` or `timestamptz` depending on
/// the migration that created it.
fn ts_col(row: &sqlx::postgres::PgRow, col: &str) -> Value {
    if let Ok(t) = row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(col) {
        return ts_tz(t);
    }
    ts_naive(
        row.try_get::<Option<chrono::NaiveDateTime>, _>(col)
            .ok()
            .flatten(),
    )
}

// ---------------------------------------------------------------------------
// /admin/schema, /admin/soft_deleted_attrs

/// GET /admin/schema — legacy schema-get (admin/routes.clj:754-761):
/// `attrs->schema` with the refs keys joined by `-`.
pub async fn admin_schema(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let r = async {
        let ctx = admin::authed_admin(&state, &headers, &params).await?;
        let attrs = service::load_attrs(&state, ctx.app_id).await?;
        let s = schema::attrs_to_schema(&attrs);
        let mut wire = s.to_wire();
        let mut refs = Map::new();
        for (k, a) in &s.refs {
            refs.insert(k.join("-"), a.wire.clone());
        }
        if let Some(m) = wire.as_object_mut() {
            m.insert("refs".into(), Value::Object(refs));
        }
        Ok(json!({"schema": wire}))
    }
    .await;
    json_or_err(r)
}

async fn soft_deleted_attrs(state: &AppState, app_id: Uuid) -> Result<Value> {
    let attrs = attr::get_soft_deleted_by_app_id(&state.pool, app_id).await?;
    // legacy row->attr keeps `deletion-marked-at` on soft-deleted rows
    let stamps: std::collections::HashMap<Uuid, Value> = sqlx::query(
        "SELECT id, deletion_marked_at FROM attrs WHERE app_id = $1 AND deletion_marked_at IS NOT NULL",
    )
    .bind(app_id)
    .fetch_all(&state.pool)
    .await?
    .iter()
    .map(|r| (r.get::<Uuid, _>("id"), ts_col(r, "deletion_marked_at")))
    .collect();
    let wire: Vec<Value> = attrs
        .iter()
        .map(|a| {
            let mut w = a.to_wire();
            if let Some(m) = w.as_object_mut() {
                m.insert(
                    "deletion-marked-at".into(),
                    stamps.get(&a.id).cloned().unwrap_or(Value::Null),
                );
            }
            w
        })
        .collect();
    Ok(json!({
        "attrs": wire,
        "grace-period-days": GRACE_PERIOD_DAYS,
    }))
}

/// GET /admin/soft_deleted_attrs — legacy soft-deleted-attrs-get
/// (admin/routes.clj:771-776).
pub async fn admin_soft_deleted_attrs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let r = async {
        let ctx = admin::authed_admin(&state, &headers, &params).await?;
        soft_deleted_attrs(&state, ctx.app_id).await
    }
    .await;
    json_or_err(r)
}

/// GET /dash/apps/:app_id/soft_deleted_attrs — legacy soft-deleted-attrs-get
/// (dash/routes.clj:676-683), collaborator.
pub async fn dash_soft_deleted_attrs(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator).await?;
        soft_deleted_attrs(&state, app.id).await
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// app management (req->app-and-user!)

/// POST /dash/apps/:app_id/rename — legacy app-rename-post (:1694-1700), owner.
pub async fn app_rename(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Owner).await?;
        let body = parse_body(&body)?;
        let title = body_str(&body, "title")?;
        sqlx::query("UPDATE apps SET title = $1 WHERE id = $2")
            .bind(title)
            .bind(app_uuid(&app))
            .execute(&state.pool)
            .await?;
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/clear — legacy apps-clear (:651-657) +
/// `app-model/clear-by-id!` (model/app.clj:435-449): soft-delete every user
/// attr in one transaction and reset the rules to `{}`. Owner.
pub async fn app_clear(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Owner).await?;
        let app_id = app_uuid(&app);
        let attrs = service::load_attrs(&state, app_id).await?;
        let steps: Vec<Value> = attrs
            .iter()
            .filter(|a| !a.is_system)
            .map(|a| json!(["delete-attr", a.id]))
            .collect();
        // legacy always writes a transactions row, even for an empty app
        if steps.is_empty() {
            sqlx::query("INSERT INTO transactions (app_id) VALUES ($1)")
                .bind(app_id)
                .execute(&state.pool)
                .await?;
        } else {
            service::run_system_transact(&state, app_id, &Value::Array(steps)).await?;
        }
        sqlx::query(
            "INSERT INTO rules (app_id, code) VALUES ($1, '{}'::jsonb)
             ON CONFLICT (app_id) DO UPDATE SET code = excluded.code, version = rules.version + 1
             WHERE rules.code IS DISTINCT FROM excluded.code",
        )
        .bind(app_id)
        .execute(&state.pool)
        .await?;
        Ok(json!({"ok": true}))
    }
    .await;
    json_or_err(r)
}

/// legacy `app-model/coerce-status` (model/app.clj:341-347)
fn coerce_status(v: &Value) -> Option<&'static str> {
    match v.as_str()? {
        "active" => Some("active"),
        "read-only" => Some("read-only"),
        "disabled" => Some("disabled"),
        _ => None,
    }
}

/// POST /dash/apps/:app_id/status — legacy app-status-post (:1687-1692), admin.
pub async fn app_status_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Admin).await?;
        let body = parse_body(&body)?;
        let raw = body
            .get("status")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "status"]))?;
        let status =
            coerce_status(raw).ok_or_else(|| param_malformed(&["body", "status"], raw.clone()))?;
        let app_id = app_uuid(&app);
        sqlx::query("UPDATE apps SET status = $1 WHERE id = $2")
            .bind(status)
            .bind(app_id)
            .execute(&state.pool)
            .await?;
        service::set_app_status(&state, app_id, status.to_string());
        Ok(json!({"status": status}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/tokens — legacy admin-tokens-regenerate
/// (:670-674) + `app-admin-token-model/recreate!`: admin; the body key is
/// `admin-token`; the raw insert result is the response.
pub async fn app_tokens_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Admin).await?;
        let body = parse_body(&body)?;
        let token = body_uuid(&body, "admin-token")?;
        let app_id = app_uuid(&app);
        let mut dbtx = state.pool.begin().await?;
        sqlx::query("DELETE FROM app_admin_tokens WHERE app_id = $1")
            .bind(app_id)
            .execute(&mut *dbtx)
            .await?;
        sqlx::query("INSERT INTO app_admin_tokens (token, app_id) VALUES ($1, $2)")
            .bind(token)
            .bind(app_id)
            .execute(&mut *dbtx)
            .await?;
        dbtx.commit().await?;
        // next.jdbc execute-one! returns the inserted row
        Ok(json!({"token": token, "app_id": app_id}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/set-magic-code-expiry — legacy
/// app-set-magic-code-expiry (:1713-1735): the `expiry` param is coerced
/// with `(int v)` before auth; admin; 1..=1440 minutes.
pub async fn app_set_magic_code_expiry(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let body = parse_body(&body)?;
        let raw = body
            .get("expiry")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "expiry"]))?;
        // `(int v)`: numbers truncate, anything else throws → nil
        let expiry: i64 = raw
            .as_f64()
            .map(|f| f.trunc() as i64)
            .ok_or_else(|| param_malformed(&["body", "expiry"], raw.clone()))?;
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Admin).await?;
        let input = json!({"magic-token-expiry-minutes": expiry});
        if expiry <= 0 {
            return Err(validation_err(
                "app",
                input,
                "The magic token expiry must be positive.",
            ));
        }
        if expiry > 24 * 60 {
            return Err(validation_err(
                "app",
                input,
                "The magic token expiry must be under 1,440 minutes (24 hours).",
            ));
        }
        let app_id = app_uuid(&app);
        sqlx::query("UPDATE apps SET magic_code_expiry_minutes = $1 WHERE id = $2")
            .bind(expiry as i32)
            .bind(app_id)
            .execute(&state.pool)
            .await?;
        // next.jdbc execute-one! returns the updated row
        Ok(json!({"app": app_row(&state, app_id).await?}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash/apps/:app_id/rule-versions — legacy rule-versions-get
/// (:710-714), collaborator or admin token; newest 50.
pub async fn rule_versions_get(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let rows = sqlx::query(
            "SELECT version, edits, created_at FROM rule_versions
              WHERE app_id = $1 ORDER BY version DESC LIMIT 50",
        )
        .bind(app.id)
        .fetch_all(&state.pool)
        .await?;
        let versions: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "version": r.get::<i32, _>("version"),
                    "edits": r.get::<Option<Value>, _>("edits").unwrap_or(Value::Null),
                    "created_at": ts_col(r, "created_at"),
                })
            })
            .collect();
        Ok(json!({"versions": versions}))
    }
    .await;
    json_or_err(r)
}

fn test_user_json(r: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": r.get::<Uuid, _>("id"),
        "app_id": r.get::<Uuid, _>("app_id"),
        "email": r.get::<String, _>("email"),
        "code": r.get::<String, _>("code"),
        "created_at": ts_col(r, "created_at"),
    })
}

/// GET /dash/apps/:app_id/test_users — legacy test-users-get, collaborator.
pub async fn test_users_get(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let rows = sqlx::query(
            "SELECT id, app_id, email, code, created_at FROM app_test_users WHERE app_id = $1",
        )
        .bind(app_uuid(&app))
        .fetch_all(&state.pool)
        .await?;
        Ok(json!({"test-users": rows.iter().map(test_user_json).collect::<Vec<_>>()}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/test_users — legacy test-users-post (:1742-1752):
/// `email` (coerced), `code` a 6-digit string; unique per (app, email).
pub async fn test_users_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let body = parse_body(&body)?;
        let raw_email = body
            .get("email")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "email"]))?;
        let email = raw_email
            .as_str()
            .and_then(|s| coerce_email_pub(s).ok())
            .ok_or_else(|| param_malformed(&["body", "email"], raw_email.clone()))?;
        let code = body_str(&body, "code")?;
        if code.len() != 6 || !code.bytes().all(|b| b.is_ascii_digit()) {
            return Err(validation_err(
                "code",
                json!(code),
                "Code must be a 6-digit number.",
            ));
        }
        let row = sqlx::query(
            "INSERT INTO app_test_users (id, app_id, email, code) VALUES ($1, $2, $3, $4)
             RETURNING id, app_id, email, code, created_at",
        )
        .bind(Uuid::new_v4())
        .bind(app_uuid(&app))
        .bind(&email)
        .bind(&code)
        .fetch_one(&state.pool)
        .await
        .map_err(|e| match &e {
            sqlx::Error::Database(db) if db.code().as_deref() == Some("23505") => {
                InstantError::new(
                    "record-not-unique",
                    400,
                    "Record not unique: app-test-user",
                    Some(json!({"record-type": "app-test-user"})),
                )
            }
            _ => InstantError::from(e),
        })?;
        Ok(json!({"test-user": test_user_json(&row)}))
    }
    .await;
    json_or_err(r)
}

/// DELETE /dash/apps/:app_id/test_users — legacy test-users-delete: the id
/// comes in the JSON body; the deleted row (or null) is returned.
pub async fn test_users_delete(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let body = parse_body(&body)?;
        let id = body_uuid(&body, "id")?;
        let row = sqlx::query(
            "DELETE FROM app_test_users WHERE app_id = $1 AND id = $2
             RETURNING id, app_id, email, code, created_at",
        )
        .bind(app_uuid(&app))
        .bind(id)
        .fetch_optional(&state.pool)
        .await?;
        Ok(json!({"test-user": row.as_ref().map(test_user_json).unwrap_or(Value::Null)}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash/apps/:app_id/stats — legacy app-stats-get (:427-434): live
/// session count and per-origin counts (this node only; legacy sums the
/// cached per-machine session reports).
pub async fn app_stats_get(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let (_, app, _) = app_and_user(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let sessions = state.sessions_for_app(app_uuid(&app));
        let mut origins: std::collections::BTreeMap<String, u64> = Default::default();
        for s in &sessions {
            if let Some(origin) = s.state.lock().await.origin.clone() {
                *origins.entry(origin).or_default() += 1;
            }
        }
        Ok(json!({"count": sessions.len(), "origins": origins}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// dashboard storage (req->app-accepting-superadmin-or-ref-token!)

/// PUT /dash/apps/:app_id/storage/upload — legacy upload-put (:1763-1779):
/// collaborator, `path` header, storage rules skipped.
pub async fn storage_upload(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let path = headers
            .get("path")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| coerce_non_blank_str(&json!(s)))
            .ok_or_else(|| param_missing(&["path"]))?;
        if body.is_empty() {
            return Err(param_missing(&["body"]));
        }
        let meta = admin::blob_meta_from_headers(&headers)?;
        admin::store_file(&state, app.id, &path, &body, &meta).await
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/storage/files/delete — legacy files-delete
/// (:1781-1789): `filenames` is read before auth; collaborator; storage
/// rules skipped; `{data: {ids}}`.
pub async fn storage_files_delete(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let body = parse_body(&body)?;
        let raw = body
            .get("filenames")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "filenames"]))?;
        // legacy coerces with `vec`: an array as-is, a string into its
        // characters, anything else is malformed
        let filenames: Vec<String> = match raw {
            Value::Array(a) => a
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect(),
            Value::String(s) => s.chars().map(|c| c.to_string()).collect(),
            _ => return Err(param_malformed(&["body", "filenames"], raw.clone())),
        };
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Collaborator).await?;
        let mut ids = vec![];
        for f in &filenames {
            if let Some(id) = admin::delete_file_by_path(&state, app.id, f).await? {
                ids.push(id);
            }
        }
        Ok(json!({"data": {"ids": ids}}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/send-test-email — legacy send-test-email-post
/// (:1663-1685): admin; the recipient must be a member of the app.
pub async fn send_test_email(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed_with_role(&state, &headers, &app_id, DashRole::Admin).await?;
        let body = parse_body(&body)?;
        let subject = body_str(&body, "subject")?;
        let html = body_str(&body, "body")?;
        let sender_email = body
            .get("sender-email")
            .and_then(|v| v.as_str())
            .and_then(|s| coerce_email_pub(s).ok());
        let sender_name = body.get("sender-name").and_then(coerce_non_blank_str);
        let raw_to = body
            .get("to")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "to"]))?;
        let to = raw_to
            .as_str()
            .and_then(|s| coerce_email_pub(s).ok())
            .ok_or_else(|| param_malformed(&["body", "to"], raw_to.clone()))?;
        // model/app.clj authorized-users: creator, app members, org members
        let allowed: Vec<String> = sqlx::query(
            "SELECT u.email FROM instant_users u JOIN apps a ON a.creator_id = u.id WHERE a.id = $1
             UNION
             SELECT u.email FROM instant_users u JOIN app_members m ON m.user_id = u.id WHERE m.app_id = $1
             UNION
             SELECT u.email FROM instant_users u JOIN org_members m ON m.user_id = u.id
               JOIN apps a ON a.org_id = m.org_id WHERE a.id = $1",
        )
        .bind(app.id)
        .fetch_all(&state.pool)
        .await?
        .iter()
        .map(|r| r.get::<String, _>("email"))
        .collect();
        if !allowed.contains(&to) {
            return Err(validation_err(
                "to",
                json!(to),
                "You can only send a test email to a member of this app.",
            ));
        }
        state
            .limiters
            .magic_code_send
            .check((app.id, to.clone()), 1.0)
            .map_err(crate::rate_limit::email_rate_limited_err)?;
        crate::email::send_test_email(
            &state,
            app.id,
            &app.title,
            &to,
            &subject,
            &html,
            sender_email,
            sender_name,
        )
        .await
        .map_err(|e| {
            InstantError::new("email-send-failed", 500, format!("Failed to send email: {e}"), None)
        })?;
        Ok(json!({"sent-to": to}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// account routes (req->auth-user!)

/// POST /dash/profiles — legacy profiles-post (:571-575): upsert the
/// caller's `instant_profiles.meta`; the raw statement result is returned.
pub async fn profiles_post(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let body = parse_body(&body)?;
        let meta = body
            .get("meta")
            .filter(|v| !v.is_null())
            .cloned()
            .ok_or_else(|| param_missing(&["body", "meta"]))?;
        let row = sqlx::query(
            "INSERT INTO instant_profiles (id, meta) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET meta = excluded.meta
             RETURNING id, meta, created_at",
        )
        .bind(user.id)
        .bind(meta)
        .fetch_one(&state.pool)
        .await?;
        Ok(json!({"profile": {
            "id": row.get::<Uuid, _>("id"),
            "meta": row.get::<Value, _>("meta"),
            "created_at": ts_col(&row, "created_at"),
        }}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/signout — legacy signout (:2451-2455): delete the bearer
/// refresh token.
pub async fn signout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let r = async {
        dash_user(&state, &headers).await?;
        let token = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .and_then(|s| Uuid::parse_str(s.trim()).ok());
        if let Some(token) = token {
            sqlx::query("DELETE FROM instant_user_refresh_tokens WHERE id = $1")
                .bind(token)
                .execute(&state.pool)
                .await?;
        }
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash/check-admin — legacy admin-check-get (:343-346): the caller
/// must be the deployment superuser (`INSTANT_SUPERUSER_EMAIL`).
pub async fn check_admin(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let su = state.cfg.superuser_email.as_deref();
        let ok = su
            .map(|su| su == coerce_email_pub(&user.email).unwrap_or_default())
            .unwrap_or(false);
        if !ok {
            return Err(InstantError::new(
                "permission-denied",
                400,
                "Permission denied: not admin?",
                Some(json!({"input": user.email, "expected": "admin?"})),
            ));
        }
        Ok(json!({"ok": true}))
    }
    .await;
    json_or_err(r)
}

fn body_email(body: &Value) -> Result<String> {
    let raw = body
        .get("email")
        .filter(|v| !v.is_null())
        .ok_or_else(|| param_missing(&["body", "email"]))?;
    raw.as_str()
        .and_then(|s| coerce_email_pub(s).ok())
        .ok_or_else(|| param_malformed(&["body", "email"], raw.clone()))
}

/// legacy `rand-num-str` (util/string.clj:14-17): digits 0-8
fn rand_code() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..6)
        .map(|_| char::from(b'0' + rng.gen_range(0..9)))
        .collect()
}

/// legacy `flags/dashboard-signup-allowed?` (flags.clj:332-338) with the
/// mode from `INSTANT_DASHBOARD_SIGNUP_MODE` (open | restricted | closed).
fn signup_allowed(state: &AppState, email: &str) -> bool {
    match state.cfg.dashboard_signup_mode.as_str() {
        "closed" => false,
        "restricted" => state
            .cfg
            .dashboard_allowed_emails
            .iter()
            .any(|e| e == &email.trim().to_lowercase()),
        _ => true,
    }
}

/// The legacy config app id: the rate-limit bucket dashboard logins share
/// with that app's magic codes (dash/routes.clj:259-261).
const CONFIG_APP_ID: Uuid = Uuid::from_u128(0x24a4d71b_7bb2_4630_9aee_01146af26239);

/// POST /dash/auth/send_magic_code — legacy send-magic-code-post (:257-287).
pub async fn auth_send_magic_code(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    let r = async {
        let body = parse_body(&body)?;
        let email = body_email(&body)?;
        state
            .limiters
            .magic_code_send
            .check((CONFIG_APP_ID, email.clone()), 1.0)
            .map_err(crate::rate_limit::email_rate_limited_err)?;
        let existing: Option<Uuid> = sqlx::query("SELECT id FROM instant_users WHERE email = $1")
            .bind(&email)
            .fetch_optional(&state.pool)
            .await?
            .map(|r| r.get("id"));
        let user_id = match existing {
            Some(id) => id,
            None => {
                if !signup_allowed(&state, &email) {
                    return Err(validation_err(
                        "email",
                        json!(email),
                        "This email is not allowed to sign up for this Instant deployment.",
                    ));
                }
                let id = Uuid::new_v4();
                sqlx::query("INSERT INTO instant_users (id, email) VALUES ($1, $2)")
                    .bind(id)
                    .bind(&email)
                    .execute(&state.pool)
                    .await?;
                id
            }
        };
        let code = rand_code();
        sqlx::query("INSERT INTO instant_user_magic_codes (id, code, user_id) VALUES ($1, $2, $3)")
            .bind(Uuid::new_v4())
            .bind(&code)
            .bind(user_id)
            .execute(&state.pool)
            .await?;
        crate::email::deliver_dashboard_magic_code(&state, &email, &code);
        Ok(json!({"sent": true}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/auth/verify_magic_code — legacy verify-magic-code-post
/// (:296-314) + `instant-user-magic-code-model/consume!` (10-minute expiry)
/// and `instant-user-refresh-token-model/create!` (the dashboard-login-disabled
/// user flag).
pub async fn auth_verify_magic_code(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    let r = async {
        let body = parse_body(&body)?;
        let email = body_email(&body)?;
        state
            .limiters
            .magic_code_verify
            .check((CONFIG_APP_ID, email.clone()), 1.0)
            .map_err(crate::rate_limit::email_rate_limited_err)?;
        let raw_code = body
            .get("code")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "code"]))?;
        let code = raw_code
            .as_str()
            .map(|s| s.trim().to_string())
            .ok_or_else(|| param_malformed(&["body", "code"], raw_code.clone()))?;
        let args = json!({"code": code, "email": email});
        let row = sqlx::query(
            "DELETE FROM instant_user_magic_codes USING instant_users
              WHERE instant_user_magic_codes.user_id = instant_users.id
                AND instant_user_magic_codes.code = $1 AND instant_users.email = $2
              RETURNING instant_user_magic_codes.user_id, instant_user_magic_codes.created_at",
        )
        .bind(&code)
        .bind(&email)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| {
            record_not_found("instant-user-magic-code", json!({"args": [args.clone()]}))
        })?;
        let user_id: Uuid = row.get("user_id");
        let created: chrono::NaiveDateTime = row.get("created_at");
        let age = chrono::Utc::now().naive_utc() - created;
        if age > chrono::Duration::minutes(10) {
            return Err(InstantError::new(
                "record-expired",
                400,
                "Record expired: instant-user-magic-code",
                Some(json!({"args": [args]})),
            ));
        }
        let disabled = sqlx::query(
            "SELECT 1 AS x FROM user_flags WHERE user_id = $1 AND flag_name = 'dashboard-login-disabled'",
        )
        .bind(user_id)
        .fetch_optional(&state.pool)
        .await?
        .is_some();
        if disabled {
            return Err(InstantError::new(
                "permission-denied",
                400,
                "Permission denied: not dashboard-login-enabled",
                Some(json!({"input": user_id, "expected": "dashboard-login-enabled"})),
            ));
        }
        let token = Uuid::new_v4();
        sqlx::query("INSERT INTO instant_user_refresh_tokens (id, user_id) VALUES ($1, $2)")
            .bind(token)
            .bind(user_id)
            .execute(&state.pool)
            .await?;
        let user = sqlx::query("SELECT id, email, created_at FROM instant_users WHERE id = $1")
            .bind(user_id)
            .fetch_one(&state.pool)
            .await?;
        Ok(json!({
            "token": token,
            "user": {
                "id": user.get::<Uuid, _>("id"),
                "email": user.get::<String, _>("email"),
                "created_at": ts_col(&user, "created_at"),
            }
        }))
    }
    .await;
    json_or_err(r)
}

fn pat_json(r: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": r.get::<Uuid, _>("id"),
        "user_id": r.get::<Uuid, _>("user_id"),
        "name": r.get::<String, _>("name"),
        "created_at": ts_col(r, "created_at"),
    })
}

/// GET /dash/personal_access_tokens — legacy personal-access-tokens-get:
/// the caller's tokens without their secrets.
pub async fn personal_access_tokens_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let rows = sqlx::query(
            "SELECT id, user_id, name, created_at FROM instant_personal_access_tokens WHERE user_id = $1",
        )
        .bind(user.id)
        .fetch_all(&state.pool)
        .await?;
        Ok(json!({"data": rows.iter().map(pat_json).collect::<Vec<_>>()}))
    }
    .await;
    json_or_err(r)
}

/// legacy `token-util/generate-personal-access-token`: `per_` + 32 random
/// bytes as hex.
pub(crate) fn generate_personal_access_token() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!(
        "per_{}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}

/// sha256 of the token string, the `lookup_key` bytea.
pub(crate) fn token_lookup_key(token: &str) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(token.as_bytes()).to_vec()
}

/// POST /dash/personal_access_tokens — legacy personal-access-tokens-post:
/// the inserted row plus the plaintext token, returned once.
pub async fn personal_access_tokens_post(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let body = parse_body(&body)?;
        let name = body_str(&body, "name")?;
        let token = generate_personal_access_token();
        let row = sqlx::query(
            "INSERT INTO instant_personal_access_tokens (id, user_id, name, lookup_key)
             VALUES ($1, $2, $3, $4) RETURNING id, user_id, name, created_at",
        )
        .bind(Uuid::new_v4())
        .bind(user.id)
        .bind(&name)
        .bind(token_lookup_key(&token))
        .fetch_one(&state.pool)
        .await?;
        let mut data = pat_json(&row);
        data["token"] = json!(token);
        Ok(json!({"data": data}))
    }
    .await;
    json_or_err(r)
}

/// DELETE /dash/personal_access_tokens/:id — legacy
/// personal-access-tokens-delete: scoped to the caller; a silent no-op for
/// anyone else's id.
pub async fn personal_access_tokens_delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let user = dash_user(&state, &headers).await?;
        let id = path_uuid(&id, "id")?;
        sqlx::query("DELETE FROM instant_personal_access_tokens WHERE id = $1 AND user_id = $2")
            .bind(id)
            .bind(user.id)
            .execute(&state.pool)
            .await?;
        Ok(json!({}))
    }
    .await;
    json_or_err(r)
}
