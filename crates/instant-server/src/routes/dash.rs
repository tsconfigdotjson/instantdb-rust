//! `/dash/*` routes used by `instant-cli` (schema pull/push, perms pull/push,
//! indexing-job polling; docs/ADMIN.md §6). Port of the corresponding
//! handlers in LEGACY dash/routes.clj + model/schema.clj.
//!
//! Auth: the CLI sends `Authorization: Bearer <token>`. Legacy accepts
//! dashboard-user tokens, platform tokens and the app's admin token
//! (`req->app-accepting-superadmin-or-ref-token!`). There is no dashboard
//! here, so the admin token is the normal credential (`INSTANT_APP_ADMIN_TOKEN`
//! / `--token` in the CLI); dashboard refresh tokens already present in a
//! migrated `instant_user_refresh_tokens` table keep working for app creators
//! and members.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use instant_core::error::{InstantError, Result};
use instant_core::schema;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::indexing_jobs::{self, NewJob};
use crate::routes::runtime::{err_response, json_or_err};
use crate::service::{self, AppRow, PermsCtx};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// auth

fn param_missing(ks: &[&str]) -> InstantError {
    InstantError::new(
        "param-missing",
        400,
        format!("Missing parameter: {}", clj_vec(ks)),
        Some(json!({"in": ks})),
    )
}

fn param_malformed(ks: &[&str], original: Value) -> InstantError {
    InstantError::new(
        "param-malformed",
        400,
        format!("Malformed parameter: {}", clj_vec(ks)),
        Some(json!({"in": ks, "original-input": original})),
    )
}

/// `(format "%s" ["headers" "authorization"])` → `["headers" "authorization"]`
fn clj_vec(ks: &[&str]) -> String {
    format!(
        "[{}]",
        ks.iter()
            .map(|k| format!("{k:?}"))
            .collect::<Vec<_>>()
            .join(" ")
    )
}

fn unauthorized() -> InstantError {
    InstantError::new(
        "record-not-found",
        401,
        "Record not found: instant-user",
        Some(json!({"args": [{"auth?": true}], "record-type": "instant-user"})),
    )
}

fn parse_app_id(raw: &str) -> Result<Uuid> {
    Uuid::parse_str(raw).map_err(|_| param_malformed(&["params", "app_id"], json!(raw)))
}

/// Resolve the app for a `/dash/apps/:app_id/*` request.
async fn dash_authed(state: &AppState, headers: &HeaderMap, app_id_raw: &str) -> Result<AppRow> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| param_missing(&["headers", "authorization"]))?;
    let token = auth
        .rsplit("Bearer ")
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();
    if token.starts_with("per_") || token.starts_with("pat_") || token.starts_with("eyJ") {
        // platform / personal access tokens belong to the hosted dashboard
        return Err(unauthorized());
    }
    let token = Uuid::parse_str(&token)
        .map_err(|_| param_malformed(&["headers", "authorization"], json!(auth)))?;

    // app admin token (superadmin path)
    let admin_app: Option<Uuid> =
        sqlx::query("SELECT app_id FROM app_admin_tokens WHERE token = $1")
            .bind(token)
            .fetch_optional(&state.pool)
            .await?
            .map(|r| r.get("app_id"));
    if let Some(admin_app) = admin_app {
        let requested = parse_app_id(app_id_raw)?;
        if requested != admin_app {
            return Err(InstantError::new(
                "validation-failed",
                400,
                format!(
                    "This admin token does not belong to app {requested}. Admin tokens are bound to a single app. Use a personal access token to manage other apps."
                ),
                Some(json!({"reason": "admin-token-mismatch"})),
            ));
        }
        return service::get_app(state, admin_app).await;
    }

    // dashboard user refresh token (creator or member of the app)
    let app_id = parse_app_id(app_id_raw)?;
    let user_id: Option<Uuid> = sqlx::query(
        "SELECT u.id FROM instant_user_refresh_tokens t
           JOIN instant_users u ON u.id = t.user_id
          WHERE t.id = $1",
    )
    .bind(token)
    .fetch_optional(&state.pool)
    .await?
    .map(|r| r.get("id"));
    let Some(user_id) = user_id else {
        return Err(unauthorized());
    };
    let app = service::get_app(state, app_id).await?;
    let role: Option<String> = sqlx::query(
        "SELECT CASE WHEN a.creator_id = $2 THEN 'owner' ELSE m.member_role END AS role
           FROM apps a
           LEFT JOIN app_members m ON m.app_id = a.id AND m.user_id = $2
          WHERE a.id = $1",
    )
    .bind(app_id)
    .bind(user_id)
    .fetch_optional(&state.pool)
    .await?
    .and_then(|r| r.get::<Option<String>, _>("role"));
    match role.as_deref() {
        Some("owner") | Some("admin") | Some("collaborator") => Ok(app),
        _ => Err(InstantError::validation_failed(
            "user-role",
            "User is missing role collaborator.",
            json!([{"message": "User is missing role collaborator."}]),
        )),
    }
}

fn parse_body(body: &Bytes) -> Result<Value> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(body).map_err(|e| {
        InstantError::new(
            "param-malformed",
            400,
            format!("Malformed JSON body: {e}"),
            None,
        )
    })
}

// ---------------------------------------------------------------------------
// schema

/// GET /dash/apps/:app_id/schema/pull
pub async fn schema_pull(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    json_or_err(schema_pull_impl(&state, &headers, &app_id).await)
}

async fn schema_pull_impl(state: &AppState, headers: &HeaderMap, app_id: &str) -> Result<Value> {
    let app = dash_authed(state, headers, app_id).await?;
    let attrs = service::load_attrs(state, app.id).await?;
    Ok(json!({
        "schema": schema::attrs_to_schema(&attrs).to_wire(),
        "attrs": attrs.to_wire_visible(),
        "app-title": app.title,
    }))
}

/// Result of applying planned steps (model/schema.clj apply-plan!).
async fn apply_steps(
    state: &Arc<AppState>,
    app_id: Uuid,
    steps: Vec<Value>,
    with_deletes: bool,
) -> Result<Value> {
    let is_tx_step = |s: &Value| {
        matches!(
            s.get(0).and_then(|v| v.as_str()),
            Some("add-attr") | Some("update-attr")
        ) || (with_deletes && s.get(0).and_then(|v| v.as_str()) == Some("delete-attr"))
    };
    let tx_steps: Vec<Value> = steps.iter().filter(|s| is_tx_step(s)).cloned().collect();
    let transaction = if tx_steps.is_empty() {
        json!({})
    } else {
        let perms = PermsCtx {
            admin: true,
            ..Default::default()
        };
        let report = service::run_transact(state, app_id, &perms, &Value::Array(tx_steps)).await?;
        // transactions.created_at is a timestamp without time zone
        let created_at: Option<chrono::NaiveDateTime> =
            sqlx::query("SELECT created_at FROM transactions WHERE id = $1")
                .bind(report.tx_id)
                .fetch_optional(&state.pool)
                .await?
                .and_then(|r| r.try_get("created_at").ok());
        json!({
            "id": report.tx_id,
            "app_id": app_id,
            "created_at": created_at.map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
            "results": {},
        })
    };

    // indexing jobs for the remaining step kinds; unknown ops pass through
    let group_id = Uuid::new_v4();
    let mut out_steps = Vec::with_capacity(steps.len());
    let mut jobs = vec![];
    let mut conn = state.pool.acquire().await?;
    for mut step in steps {
        let op = step
            .get(0)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if schema::job_serial_key(&op).is_some() {
            let args = step.get(1).cloned().unwrap_or(Value::Null);
            let attr_id = args
                .get("attr-id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or_else(|| {
                    InstantError::validation_failed(
                        "tx-steps",
                        format!("{op} step is missing a valid attr-id"),
                        json!([]),
                    )
                })?;
            let job = NewJob {
                attr_id,
                job_type: op.clone(),
                checked_data_type: args
                    .get("checked-data-type")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            };
            let job_id = indexing_jobs::create_job(&mut conn, app_id, group_id, &job).await?;
            jobs.push(indexing_jobs::get_basic(&mut conn, job_id).await?);
            if let Some(obj) = step.get_mut(1).and_then(|v| v.as_object_mut()) {
                obj.insert("job-id".into(), json!(job_id));
            }
        }
        out_steps.push(step);
    }
    drop(conn);
    let indexing_jobs = if jobs.is_empty() {
        Value::Null
    } else {
        indexing_jobs::spawn_group(state.clone(), app_id, group_id);
        json!({"group-id": group_id, "jobs": jobs})
    };
    Ok(json!({
        "transaction": transaction,
        "steps": out_steps,
        "indexing-jobs": indexing_jobs,
    }))
}

/// tx/coerce! shape check: steps and each step must be collections.
fn coerce_steps(steps: &Value) -> Result<Vec<Value>> {
    let arr = steps
        .as_array()
        .ok_or_else(|| param_malformed(&["body", "steps"], steps.clone()))?;
    for (idx, step) in arr.iter().enumerate() {
        if !(step.is_array() || step.is_object()) {
            return Err(InstantError::new(
                "validation-failed",
                400,
                "Validation failed for tx-steps",
                Some(json!({
                    "data-type": "tx-steps",
                    "input": steps,
                    "errors": [{"expected": "coll?", "in": [idx]}],
                })),
            ));
        }
    }
    Ok(arr.clone())
}

/// POST /dash/apps/:app_id/schema/steps/apply — how `instant-cli push schema`
/// applies its diff.
pub async fn schema_steps_apply(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    json_or_err(schema_steps_apply_impl(&state, &headers, &app_id, &body).await)
}

async fn schema_steps_apply_impl(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    app_id: &str,
    body: &Bytes,
) -> Result<Value> {
    let app = dash_authed(state, headers, app_id).await?;
    let body = parse_body(body)?;
    let steps = body
        .get("steps")
        .filter(|s| !s.is_null())
        .ok_or_else(|| param_missing(&["body", "steps"]))?;
    if !(steps.is_array() || steps.is_object()) {
        return Err(param_malformed(&["body", "steps"], steps.clone()));
    }
    let steps = coerce_steps(steps)?;
    apply_steps(state, app.id, steps, true).await
}

struct Plan {
    steps: Vec<Value>,
    wire: Value,
}

async fn plan(state: &AppState, app_id: Uuid, body: &Value) -> Result<Plan> {
    let client_defs = body.get("schema").cloned().unwrap_or(Value::Null);
    let opts = schema::PlanOpts {
        check_types: truthy(body.get("check_types")),
        background_updates: truthy(body.get("supports_background_updates")),
    };
    let new_schema = schema::defs_to_schema(&schema::remove_system_namespaces(&client_defs));
    let current_attrs = service::load_attrs(state, app_id).await?;
    let current_schema = schema::attrs_to_schema(&current_attrs);
    let steps = schema::schemas_to_ops(opts, &current_schema, &new_schema);
    let errors = schema::plan_errors(&current_attrs, &steps);
    if !errors.is_empty() {
        let msgs: Vec<&str> = errors
            .iter()
            .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
            .collect();
        return Err(InstantError::new(
            "validation-failed",
            400,
            format!("Validation failed for schema: {}", msgs.join(", ")),
            Some(json!({"data-type": "schema", "input": "plan", "errors": errors})),
        ));
    }
    Ok(Plan {
        wire: json!({
            "new-schema": new_schema.to_wire(),
            "current-schema": current_schema.to_wire(),
            "current-attrs": current_attrs.to_wire_visible(),
            "steps": steps,
        }),
        steps,
    })
}

fn truthy(v: Option<&Value>) -> bool {
    !matches!(v, None | Some(Value::Null) | Some(Value::Bool(false)))
}

/// POST /dash/apps/:app_id/schema/push/plan
pub async fn schema_push_plan(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed(&state, &headers, &app_id).await?;
        let body = parse_body(&body)?;
        Ok(plan(&state, app.id, &body).await?.wire)
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/schema/push/apply — plan + apply in one call.
pub async fn schema_push_apply(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed(&state, &headers, &app_id).await?;
        let body = parse_body(&body)?;
        let plan = plan(&state, app.id, &body).await?;
        let applied = apply_steps(&state, app.id, plan.steps, false).await?;
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

// ---------------------------------------------------------------------------
// indexing jobs

/// GET /dash/apps/:app_id/indexing-jobs/group/:group_id
pub async fn indexing_jobs_group(
    State(state): State<Arc<AppState>>,
    Path((app_id, group_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed(&state, &headers, &app_id).await?;
        let group_id = Uuid::parse_str(&group_id)
            .map_err(|_| param_malformed(&["params", "group_id"], json!(group_id)))?;
        let jobs = indexing_jobs::get_by_group_for_client(&state, app.id, group_id).await?;
        Ok(json!({"jobs": jobs}))
    }
    .await;
    json_or_err(r)
}

/// GET /dash/apps/:app_id/indexing-jobs/:job_id
pub async fn indexing_job_get(
    State(state): State<Arc<AppState>>,
    Path((app_id, job_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed(&state, &headers, &app_id).await?;
        let job_id = Uuid::parse_str(&job_id)
            .map_err(|_| param_malformed(&["params", "job_id"], json!(job_id)))?;
        // legacy: (job->client-format nil) => {} — no 404 for unknown ids
        let job = indexing_jobs::get_by_id_for_client(&state, app.id, job_id)
            .await?
            .unwrap_or_else(|| json!({}));
        Ok(json!({"job": job}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/indexing-jobs — body `{attr-id, job-type,
/// checked-data-type?}`
pub async fn indexing_job_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed(&state, &headers, &app_id).await?;
        let body = parse_body(&body)?;
        let attr_id = body
            .get("attr-id")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "attr-id"]))?;
        let attr_id = attr_id
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| param_malformed(&["body", "attr-id"], attr_id.clone()))?;
        let job_type = body
            .get("job-type")
            .filter(|v| !v.is_null())
            .ok_or_else(|| param_missing(&["body", "job-type"]))?;
        let job_type = job_type
            .as_str()
            .filter(|t| indexing_jobs::job_spec(t).is_some())
            .ok_or_else(|| param_malformed(&["body", "job-type"], job_type.clone()))?
            .to_string();
        let checked_data_type = body
            .get("checked-data-type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let group_id = Uuid::new_v4();
        let mut conn = state.pool.acquire().await?;
        let job_id = indexing_jobs::create_job(
            &mut conn,
            app.id,
            group_id,
            &NewJob {
                attr_id,
                job_type,
                checked_data_type,
            },
        )
        .await?;
        let job = indexing_jobs::get_basic(&mut conn, job_id).await?;
        drop(conn);
        indexing_jobs::spawn_group(state.clone(), app.id, group_id);
        Ok(json!({"job": job}))
    }
    .await;
    json_or_err(r)
}

// ---------------------------------------------------------------------------
// perms

/// GET /dash/apps/:app_id/perms/pull
pub async fn perms_pull(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let r = async {
        let app = dash_authed(&state, &headers, &app_id).await?;
        let code: Option<Value> = sqlx::query("SELECT code FROM rules WHERE app_id = $1")
            .bind(app.id)
            .fetch_optional(&state.pool)
            .await?
            .map(|r| r.get("code"));
        Ok(json!({"perms": code}))
    }
    .await;
    json_or_err(r)
}

/// POST /dash/apps/:app_id/rules — body `{code: {...rules...}}`
pub async fn rules_post(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let r = async {
        let app = dash_authed(&state, &headers, &app_id).await?;
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
        // rule.clj put!: version bumps only when the code actually changes,
        // and legacy returns nil (no row) in the unchanged case.
        let row = sqlx::query(
            "INSERT INTO rules (app_id, code) VALUES ($1, $2)
             ON CONFLICT (app_id) DO UPDATE SET code = excluded.code, version = rules.version + 1
             WHERE rules.code IS DISTINCT FROM excluded.code
             RETURNING app_id, code, version",
        )
        .bind(app.id)
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

// ---------------------------------------------------------------------------
// cli

/// GET /dash/cli/version
pub async fn cli_version() -> Response {
    json_or_err(Ok(json!({
        "min-version": {"major": 0, "minor": 19, "patch": 0, "dev?": false}
    })))
}

/// POST /dash/cli/auth/{register,check,claim,void}: the browser login flow
/// needs the hosted dashboard. Point users at the admin token instead of a
/// bare 404.
pub async fn cli_auth_unsupported() -> Response {
    err_response(&InstantError::new(
        "validation-failed",
        400,
        "Dashboard login is not available on this server. Authenticate instant-cli with your app's admin token instead: set INSTANT_APP_ADMIN_TOKEN (or pass --token) alongside INSTANT_APP_ID.",
        Some(json!({"data-type": "instant-cli-login", "errors": [{"issue": "unsupported", "message": "Dashboard login is not available on this server."}]})),
    ))
}
